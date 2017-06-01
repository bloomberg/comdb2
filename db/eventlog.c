#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stddef.h>
#include <sys/time.h>
#include <inttypes.h>

#include <zlib.h>

#include "reqlog_int.h"
#include "eventlog.h"
#include "util.h"
#include "plhash.h"

#include "cson_amalgamation_core.h"

static char* eventlog_fname(const char *dbname);
static int eventlog_nkeep = 10;
static int eventlog_rollat = 1024*1024*1024;
static int eventlog_enabled = 0;
static int eventlog_detailed = 0;
static int64_t bytes_written = 0;

static gzFile eventlog = NULL;
static pthread_mutex_t eventlog_lk = PTHREAD_MUTEX_INITIALIZER;
static gzFile eventlog_open(const char *fname);
int eventlog_every_n = 1;
int64_t eventlog_count = 0;

static void eventlog_roll(void);

struct sqltrack {
    char fingerprint[16];
    char *sql;
    LINKC_T(struct sqltrack) lnk;
};

LISTC_T(struct sqltrack) sql_statements;

static hash_t *seen_sql;

void eventlog_init(const char *dbname) {
    seen_sql = hash_init_o(offsetof(struct sqltrack, fingerprint), 16);
    listc_init(&sql_statements, offsetof(struct sqltrack, lnk));
}

static gzFile eventlog_open(const char *fname) {
    gzFile f = gzopen(fname, "2w");
    if (f == NULL) {
        eventlog_enabled = 0;
        return NULL;
    }
    return f;
}

static void eventlog_close(void) {
    if (eventlog == NULL)
        return;
    gzclose(eventlog);
    eventlog = NULL;
    bytes_written = 0;
    struct sqltrack *t = listc_rtl(&sql_statements);
    while (t) {
        hash_del(seen_sql, t);
        free(t->sql);
        free(t);
        t = listc_rtl(&sql_statements);
    }
}

static char* eventlog_fname(const char *dbname) {
    return comdb2_location("logs", "%s.events.%"PRId64"", dbname, time_epochus());
}

static cson_output_opt opt = { .indentation = 0, .maxDepth = 4096, .addNewline = 1, .addSpaceAfterColon = 1, .indentSingleMemberValues = 0, .escapeForwardSlashes = 1};

void eventlog_params(cson_object *obj, const struct reqlogger *logger) {
    if (logger->request && logger->request->n_bindvars > 0) {
        cson_object_set(obj, "le", cson_value_new_bool(logger->request->little_endian));
        cson_value *bindings = cson_value_new_array();
        cson_array *arr = cson_value_get_array(bindings);
        cson_array_reserve(arr, logger->request->n_bindvars);

        for (int i = 0; i < logger->request->n_bindvars; i++) {
            cson_value *binding = cson_value_new_object();
            cson_object *bobj = cson_value_get_object(binding);

            CDB2SQLQUERY__Bindvalue *val = logger->request->bindvars[i];
            if (val->varname)
                cson_object_set(bobj, "name", cson_value_new_string(val->varname, strlen(val->varname)));
            else
                cson_object_set(bobj, "index", cson_new_int(val->index));
            if (!val->has_isnull && val->isnull) {
                /* null, omit value */
                cson_object_set(bobj, "value", cson_value_null());
            }
            else {
                uint8_t *value = malloc(val->value.len);

                cson_object_set(bobj, "type", cson_new_int(val->type));
                cson_object_set(bobj, "len", cson_new_int(val->value.len));

                for (int i = 0; i < val->value.len; i++) {
                    static const char *hexchars = "0123456789abcdef";

                    value[i*2] = hexchars[((val->value.data[i] & 0xf0) >> 4)];
                    value[i*2+1] = hexchars[val->value.data[i] & 0x0f];
                }
                cson_object_set(bobj, "value", cson_value_new_string(value, val->value.len*2));
                free(value);
            }
            cson_array_append(arr, binding);
        }

        cson_object_set(obj, "bindings", bindings);
    }
}

void eventlog_perfdata(cson_object *obj, const struct reqlogger *logger) {
    const struct bdb_thread_stats *thread_stats = bdb_get_thread_stats();
    int64_t start, end;

    start = logger->startus;
    end = time_epochus();

    cson_value *perfval = cson_value_new_object();
    cson_object *perfobj = cson_value_get_object(perfval);

    cson_object_set(perfobj, "runtime", cson_new_int(end-start));

    if (thread_stats->n_lock_waits || thread_stats->n_preads || thread_stats->n_pwrites || thread_stats->pread_time_ms || thread_stats->pwrite_time_ms || thread_stats->lock_wait_time_ms) {
        if (thread_stats->n_lock_waits) {
            cson_object_set(perfobj, "lockwaits", cson_new_int(thread_stats->n_lock_waits));
            cson_object_set(perfobj, "lockwaittime", cson_new_int(thread_stats->lock_wait_time_ms));
        }
        if (thread_stats->n_preads) {
            cson_object_set(perfobj, "reads", cson_new_int(thread_stats->n_preads));
            cson_object_set(perfobj, "readtimetime", cson_new_int(thread_stats->pread_time_ms));
        }
        if (thread_stats->n_pwrites) {
            cson_object_set(perfobj, "writes", cson_new_int(thread_stats->n_pwrites));
            cson_object_set(perfobj, "writetime", cson_new_int(thread_stats->pwrite_time_ms));
        }
    }
    cson_object_set(obj, "perf", perfval);
}

int write_json( void * state, void const * src, unsigned int n ) {
    int rc = gzwrite((gzFile) state, src, n);
    bytes_written += rc;
    return rc != n;
}

static void eventlog_add_int(cson_object *obj, const struct reqlogger *logger) {
    static const char *hexchars = "0123456789abcdef";

    int detailed = eventlog_detailed;

    pthread_mutex_lock(&eventlog_lk);
    if (logger->event_type && strcmp(logger->event_type, "sql") == 0 && !hash_find(seen_sql, logger->fingerprint)) {
        struct sqltrack *st;
        st = malloc(sizeof(struct sqltrack));
        memcpy(st->fingerprint, logger->fingerprint, sizeof(logger->fingerprint));
        st->sql = strdup(logger->stmt);
        hash_add(seen_sql, st);
        listc_abl(&sql_statements, st);

        cson_value *newval;
        cson_object *newobj;
        newval = cson_value_new_object();
        newobj = cson_value_get_object(newval);

        cson_object_set(newobj, "time", cson_new_int(logger->startus));
        cson_object_set(newobj, "type", cson_value_new_string("newsql", sizeof("newsql")));
        cson_object_set(newobj, "sql", cson_value_new_string(logger->stmt, strlen(logger->stmt)));

        char fingerprint[32];
        for (int i = 0; i < 16; i++) {
             fingerprint[i*2] = hexchars[((logger->fingerprint[i] & 0xf0) >> 4)];
             fingerprint[i*2+1] = hexchars[logger->fingerprint[i] & 0x0f];
        }
        cson_object_set(newobj, "fingerprint", cson_value_new_string(fingerprint, sizeof(fingerprint)));

        /* yes, this can spill the file to beyond the configured size - we need this
           event to be in the same file as the event its being logged for */
        cson_output(newval, write_json, eventlog, &opt);
        cson_value_free(newval);
    }
    pthread_mutex_unlock(&eventlog_lk);

    cson_object_set(obj, "time", cson_new_int(logger->startus));
    if (logger->event_type)
        cson_object_set(obj, "type", cson_value_new_string(logger->event_type, strlen(logger->event_type)));

    if (logger->stmt && detailed)
        cson_object_set(obj, "sql", cson_value_new_string(logger->stmt, strlen(logger->stmt)));

    if (logger->have_id)
        cson_object_set(obj, "id", cson_value_new_string(logger->id, sizeof(logger->id)));
    if (logger->sqlcost)
        cson_object_set(obj, "cost", cson_new_double(logger->sqlcost));
    if (logger->sqlrows)
        cson_object_set(obj, "rows", cson_new_int(logger->sqlrows));
    if (logger->vreplays)
        cson_object_set(obj, "replays", cson_new_int(logger->vreplays));
    if (logger->have_fingerprint) {
        uint8_t fingerprint[32];
        for (int i = 0; i < 15; i++) {
            fingerprint[i*2] = hexchars[((logger->fingerprint[i] & 0xf0) >> 4)];
            fingerprint[i*2+1] = hexchars[logger->fingerprint[i] & 0x0f];
        }
        cson_object_set(obj, "fingerprint", cson_value_new_string(fingerprint, 32));
    }

    cson_object_set(obj, "duration", cson_new_int(logger->durationms));
    if (logger->queuetimems)
        cson_object_set(obj, "qtime", cson_new_int(logger->queuetimems));

    if (detailed)
        eventlog_params(obj, logger);

    eventlog_perfdata(obj, logger);
}


void eventlog_add(const struct reqlogger *logger) {
    int sz = 0;
    char *fname;
    cson_value *val;
    cson_object *obj;

    if (eventlog == NULL)
        return;
    if (!eventlog_enabled)
        return;

    pthread_mutex_lock(&eventlog_lk);
    eventlog_count++;
    if (eventlog_every_n > 1 && eventlog_count % eventlog_every_n != 0) {
        pthread_mutex_unlock(&eventlog_lk);
        return;
    }
    if (bytes_written > eventlog_rollat) {
        eventlog_roll();
    }
    pthread_mutex_unlock(&eventlog_lk);

    val = cson_value_new_object();
    obj = cson_value_get_object(val);
    eventlog_add_int(obj, logger);
    
    pthread_mutex_lock(&eventlog_lk);
    cson_output(val, write_json, eventlog, &opt);
    pthread_mutex_unlock(&eventlog_lk);

    cson_value_free(val);
}

static void eventlog_roll(void) {
    char *fname = eventlog_fname(thedb->envname);
    eventlog_close();
    eventlog = eventlog_open(fname);
    free(fname);
}

static void eventlog_enable(void) {
    eventlog_enabled = 1;
    eventlog_roll();
}

static void eventlog_disable(void) {
    eventlog_close();
    eventlog = NULL;
    eventlog_enabled = 0;
    bytes_written = 0;
}

void eventlog_stop(void) {
    eventlog_disable();
}

void eventlog_process_message_locked(char *line, int lline, int *toff) {
    char *tok;
    int ltok;

    tok = segtok(line, lline, toff, &ltok);
    if (ltok == 0) {
        logmsg(LOGMSG_ERROR, "Expected option for reql events\n");
        return;
    }
    if (tokcmp(tok, ltok, "on") == 0)
        eventlog_enable();
    else if (tokcmp(tok, ltok, "off") == 0)
        eventlog_disable();
    else if (tokcmp(tok, ltok, "roll") == 0) {
        eventlog_roll();
    } else if (tokcmp(tok, ltok, "keep") == 0) {
        int nfiles;
        tok = segtok(line, lline, toff, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected number of files to keep\n");
            return;
        }
        nfiles = toknum(tok, ltok);
        if (nfiles < 1 || nfiles > 100) {
            logmsg(LOGMSG_ERROR, "Invalid #files to keep for \"sqllogger keep (must "
                    "be between 1 and 100\"\n");
            return;
        }
        eventlog_nkeep = nfiles;
        logmsg(LOGMSG_USER, "Keeping %d logs\n", eventlog_nkeep);
    } else if (tokcmp(tok, ltok, "detailed") == 0) {
        tok = segtok(line, lline, toff, &ltok);
        if (tokcmp(tok, ltok, "on") == 0)
            eventlog_detailed = 1;
        else if (tokcmp(tok, ltok, "off") == 0)
            eventlog_detailed = 0;
    } else if (tokcmp(tok, ltok, "rollat") == 0) {
        off_t rollat;
        char *s;
        tok = segtok(line, lline, toff, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected number of files to keep\n");
            return;
        }
        s = tokdup(tok, ltok);
        if (s == NULL) {
            logmsg(LOGMSG_ERROR, "can't allocate memory\n");
            return;
        }
        rollat = strtol(s, NULL, 10);
        free(s);
        if (rollat < 0) {
            return;
        }
        if (rollat == 0)
            logmsg(LOGMSG_USER, "Turned off rolling\n");
        else {
            logmsg(LOGMSG_USER, "Rolling logs after %d bytes\n", rollat);
        }
        eventlog_nkeep = rollat;
    } else if (tokcmp(tok, ltok, "every") == 0) {
        int every;
        tok = segtok(line, lline, toff, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected a count for 'every'\n");
            return;
        }
        every = toknum(tok, ltok);
        if (every == 0) {
            logmsg(LOGMSG_USER, "Logging all events\n");
        } else if (every < 0) {
            logmsg(LOGMSG_ERROR, "Invalid count for 'every'\n");
            return;
        } else
            logmsg(LOGMSG_USER, "Logging every %d queries\n", eventlog_every_n);
        eventlog_every_n = every;
    } else if (tokcmp(tok, ltok, "flush") == 0) {
        gzflush(eventlog, 1);
    } else {
        logmsg(LOGMSG_ERROR, "Unknown eventlog command\n");
        return;
    }
}

void eventlog_process_message(char *line, int lline, int *toff) {
    pthread_mutex_lock(&eventlog_lk);
    eventlog_process_message_locked(line, lline, toff);
    pthread_mutex_unlock(&eventlog_lk);
}
