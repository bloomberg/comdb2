/*
  Copyright 2017, Bloomberg Finance L.P.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stddef.h>
#include <sys/time.h>
#include <inttypes.h>

#include <comdb2.h>
#if defined(_IBM_SOURCE)
#include <openssl/objects.h>
#include <openssl/ec.h>
#endif

#include <zlib.h>

#include "reqlog_int.h"
#include "eventlog.h"
#include "util.h"
#include "plhash.h"
#include "logmsg.h"
#include "dbinc/locker_info.h"

#include "cson_amalgamation_core.h"

static char *gbl_eventlog_fname = NULL;
static char *eventlog_fname(const char *dbname);
static int eventlog_nkeep = 10;
static int eventlog_rollat = 1024 * 1024 * 1024;
static int eventlog_enabled = 1;
static int eventlog_detailed = 0;
static int64_t bytes_written = 0;
static int eventlog_verbose = 0;

static gzFile eventlog = NULL;
static pthread_mutex_t eventlog_lk = PTHREAD_MUTEX_INITIALIZER;
static gzFile eventlog_open(void);
int eventlog_every_n = 1;
int64_t eventlog_count = 0;

static void eventlog_roll(void);
#define min(x, y) ((x) < (y) ? (x) : (y))

struct sqltrack {
    char fingerprint[FINGERPRINTSZ];
    char *sql;
    LINKC_T(struct sqltrack) lnk;
};

LISTC_T(struct sqltrack) sql_statements;

static hash_t *seen_sql;

void eventlog_init()
{
    seen_sql =
        hash_init_o(offsetof(struct sqltrack, fingerprint), FINGERPRINTSZ);
    listc_init(&sql_statements, offsetof(struct sqltrack, lnk));
    if (eventlog_enabled) eventlog = eventlog_open();
}

static inline void free_gbl_eventlog_fname()
{
    if (gbl_eventlog_fname == NULL)
        return;
    free(gbl_eventlog_fname);
    gbl_eventlog_fname = NULL;
}

static gzFile eventlog_open()
{
    char *fname = eventlog_fname(thedb->envname);
    gbl_eventlog_fname = fname;
    gzFile f = gzopen(fname, "2w");
    if (f == NULL) {
        eventlog_enabled = 0;
        free(fname);
        return NULL;
    }
    gbl_eventlog_fname = fname;
    return f;
}

static void eventlog_close(void)
{
    if (eventlog == NULL) return;
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
    free_gbl_eventlog_fname();
}

static char *eventlog_fname(const char *dbname)
{
    return comdb2_location("logs", "%s.events.%" PRId64 "", dbname,
                           comdb2_time_epochus());
}

static cson_output_opt opt = {.indentation = 0,
                              .maxDepth = 4096,
                              .addNewline = 1,
                              .addSpaceAfterColon = 1,
                              .indentSingleMemberValues = 0,
                              .escapeForwardSlashes = 1};

cson_array *get_bind_array(struct reqlogger *logger, int nfields)
{
    if (eventlog == NULL || !eventlog_enabled || !eventlog_detailed)
        return NULL;
    cson_value *bind_list = cson_value_new_array();
    logger->bound_param_cson = bind_list;

    cson_array *arr = cson_value_get_array(bind_list);

    cson_array_reserve(arr, nfields);
    return arr;
}

inline static cson_object *
eventlog_append_name(cson_array *arr, const char *name, const char *type)
{
    if (!arr)
        return NULL;
    cson_value *binding = cson_value_new_object();
    cson_array_append(arr, binding);
    cson_object *bobj = cson_value_get_object(binding);
    cson_object_set(bobj, "name", cson_value_new_string(name, strlen(name)));
    cson_object_set(bobj, "type", cson_value_new_string(type, strlen(type)));
    return bobj;
}

void eventlog_bind_null(cson_array *arr, const char *name)
{
    /* log null values as int for simplicity */
    cson_object *bobj = eventlog_append_name(arr, name, "int");
    if (!bobj)
        return;
    cson_object_set(bobj, "value", cson_value_null());
    return;
}

void eventlog_bind_int64(cson_array *arr, const char *name, int64_t val,
                         int dlen)
{
    const char *type;
    switch (dlen) {
    case 2: type = "smallint"; break;
    case 4: type = "int"; break;
    case 8: type = "largeint"; break;
    default: return;
    }
    cson_object *bobj = eventlog_append_name(arr, name, type);
    if (!bobj)
        return;
    cson_object_set(bobj, "value", cson_value_new_integer(val));
}

void eventlog_bind_text(cson_array *arr, const char *name, const char *val,
                        int dlen)
{
    cson_object *bobj = eventlog_append_name(arr, name, "char");
    if (!bobj)
        return;
    cson_object_set(bobj, "value", cson_value_new_string(val, dlen));
}

void eventlog_bind_double(cson_array *arr, const char *name, double val,
                          int dlen)
{
    const char *type;
    switch (dlen) {
    case 4: type = "float"; break;
    case 8: type = "doublefloat"; break;
    default: return;
    }
    cson_object *bobj = eventlog_append_name(arr, name, type);
    if (!bobj)
        return;
    cson_object_set(bobj, "value", cson_value_new_double(val));
}

static void eventlog_bind_blob_int(cson_array *arr, const char *name,
                                   const char *type, const void *val, int dlen)
{
    cson_object *bobj = eventlog_append_name(arr, name, type);
    if (!bobj)
        return;
    int datalen = min(dlen, 1024);         /* cap the datalen logged */
    const int exp_len = (2 * datalen) + 4; /* x' ... '/0  */
    char *expanded_buf = malloc(exp_len);
    expanded_buf[0] = 'x';
    expanded_buf[1] = '\'';
    util_tohex(&expanded_buf[2], val, datalen);
    expanded_buf[2 + datalen * 2] = '\'';
    expanded_buf[3 + datalen * 2] = '\0';
    cson_object_set(bobj, "value",
                    cson_value_new_string(expanded_buf, exp_len));
    free(expanded_buf);
}

void eventlog_bind_blob(cson_array *a, const char *n, const void *v, int l)
{
    eventlog_bind_blob_int(a, n, "blob", v, l);
}

void eventlog_bind_varchar(cson_array *a, const char *n, const void *v, int l)
{
    eventlog_bind_blob_int(a, n, "varchar", v, l);
}

void eventlog_bind_datetime(cson_array *arr, const char *name, dttz_t *dt,
                            const char *tz)
{
    const char *type =
        dt->dttz_prec == DTTZ_PREC_MSEC ? "datetime" : "datetimeus";
    cson_object *bobj = eventlog_append_name(arr, name, type);
    if (!bobj)
        return;
    char str[256];
    int used;
    dttz_to_str(dt, str, sizeof(str), &used, tz);
    cson_object_set(bobj, "value", cson_value_new_string(str, used));
}

void eventlog_bind_interval(cson_array *arr, const char *name, intv_t *tv)
{
    const char *type;
    switch (tv->type) {
    case INTV_YM_TYPE: type = "interval month"; break;
    case INTV_DS_TYPE: type = "interval sec"; break;
    case INTV_DSUS_TYPE: type = "interval usec"; break;
    default: return;
    }
    cson_object *bobj = eventlog_append_name(arr, name, type);
    if (!bobj)
        return;
    char str[256];
    int n;
    intv_to_str(tv, str, sizeof(str), &n);
    cson_object_set(bobj, "value", cson_value_new_string(str, n));
}

void eventlog_tables(cson_object *obj, const struct reqlogger *logger)
{
    if (logger->ntables == 0) return;

    cson_value *tables = cson_value_new_array();
    cson_array *arr = cson_value_get_array(tables);
    cson_array_reserve(arr, logger->ntables);

    for (int i = 0; i < logger->ntables; i++) {
        cson_value *v = cson_value_new_string(logger->sqltables[i],
                                              strlen(logger->sqltables[i]));
        cson_array_append(arr, v);
    }

    cson_object_set(obj, "tables", tables);
}

void eventlog_perfdata(cson_object *obj, const struct reqlogger *logger)
{
    const struct bdb_thread_stats *thread_stats = bdb_get_thread_stats();
    int64_t start = logger->startus;
    int64_t end = comdb2_time_epochus();

    cson_value *perfval = cson_value_new_object();
    cson_object *perfobj = cson_value_get_object(perfval);

    // runtime is in microseconds
    cson_object_set(perfobj, "runtime", cson_new_int(end - start));

    if (thread_stats->n_lock_waits || thread_stats->n_preads ||
        thread_stats->n_pwrites || thread_stats->pread_time_us ||
        thread_stats->pwrite_time_us || thread_stats->lock_wait_time_us) {
        if (thread_stats->n_lock_waits) {
            cson_object_set(perfobj, "lockwaits",
                            cson_new_int(thread_stats->n_lock_waits));
            cson_object_set(perfobj, "lockwaittime",
                            cson_new_int(thread_stats->lock_wait_time_us));
        }
        if (thread_stats->n_preads) {
            cson_object_set(perfobj, "reads",
                            cson_new_int(thread_stats->n_preads));
            cson_object_set(perfobj, "readtimetime",
                            cson_new_int(thread_stats->pread_time_us));
        }
        if (thread_stats->n_pwrites) {
            cson_object_set(perfobj, "writes",
                            cson_new_int(thread_stats->n_pwrites));
            cson_object_set(perfobj, "writetime",
                            cson_new_int(thread_stats->pwrite_time_us));
        }
    }
    cson_object_set(obj, "perf", perfval);
}

int write_json(void *state, const void *src, unsigned int n)
{
    int rc = gzwrite((gzFile)state, src, n);
    bytes_written += rc;
    return rc != n;
}

int write_logmsg(void *state, const void *src, unsigned int n)
{
    logmsg(LOGMSG_USER, "%.*s", n, (const char *)src);
    return 0;
}

static void eventlog_context(cson_object *obj, const struct reqlogger *logger)
{
    if (logger->ncontext > 0) {
        cson_value *contexts = cson_value_new_array();
        cson_array *arr = cson_value_get_array(contexts);
        cson_array_reserve(arr, logger->ncontext);
        for (int i = 0; i < logger->ncontext; i++) {
            cson_value *v = cson_value_new_string(logger->context[i],
                                                  strlen(logger->context[i]));
            cson_array_append(arr, v);
        }
        cson_object_set(obj, "context", contexts);
    }
}

static void eventlog_path(cson_object *obj, const struct reqlogger *logger)
{
    if (eventlog == NULL || !eventlog_enabled)
        return;

    if (!logger->path || logger->path->n_components == 0) return;

    cson_value *components = cson_value_new_array();
    cson_array *arr = cson_value_get_array(components);
    cson_array_reserve(arr, logger->path->n_components);

    for (int i = 0; i < logger->path->n_components; i++) {
        cson_value *component;
        component = cson_value_new_object();
        cson_object *obj = cson_value_get_object(component);
        struct client_query_path_component *c;
        c = &logger->path->path_stats[i];
        if (c->table[0])
            cson_object_set(obj, "table",
                            cson_value_new_string(c->table, strlen(c->table)));
        if (c->ix != -1) cson_object_set(obj, "index", cson_new_int(c->ix));
        if (c->nfind) cson_object_set(obj, "find", cson_new_int(c->nfind));
        if (c->nnext) cson_object_set(obj, "next", cson_new_int(c->nnext));
        if (c->nwrite) cson_object_set(obj, "write", cson_new_int(c->nwrite));
        cson_array_append(arr, component);
    }
    cson_object_set(obj, "path", components);
}

static void eventlog_add_int(cson_object *obj, const struct reqlogger *logger)
{
    if (eventlog == NULL || !eventlog_enabled)
        return;

    bool isSql = logger->event_type && (strcmp(logger->event_type, "sql") == 0);
    bool isSqlErr = logger->error && logger->stmt;

    pthread_mutex_lock(&eventlog_lk);
    if ((isSql || isSqlErr) && !hash_find(seen_sql, logger->fingerprint)) {
        /* add never seen before "newsql" query, also print it to log */
        struct sqltrack *st;
        st = malloc(sizeof(struct sqltrack));
        memcpy(st->fingerprint, logger->fingerprint,
               sizeof(logger->fingerprint));
        st->sql = strdup(logger->stmt);
        hash_add(seen_sql, st);
        listc_abl(&sql_statements, st);

        cson_value *newval;
        cson_object *newobj;
        newval = cson_value_new_object();
        newobj = cson_value_get_object(newval);

        cson_object_set(newobj, "time", cson_new_int(logger->startus));
        cson_object_set(newobj, "type",
                        cson_value_new_string("newsql", sizeof("newsql")));
        cson_object_set(newobj, "sql", cson_value_new_string(
                                           logger->stmt, strlen(logger->stmt)));

        char expanded_fp[2 * FINGERPRINTSZ + 1];
        util_tohex(expanded_fp, logger->fingerprint, FINGERPRINTSZ);
        cson_object_set(newobj, "fingerprint",
                        cson_value_new_string(expanded_fp, FINGERPRINTSZ * 2));

        /* yes, this can spill the file to beyond the configured size - we need
           this
           event to be in the same file as the event its being logged for */
        cson_output(newval, write_json, eventlog, &opt);
        if (eventlog_verbose) cson_output(newval, write_logmsg, stdout, &opt);
        cson_value_free(newval);
    }
    pthread_mutex_unlock(&eventlog_lk);

    cson_object_set(obj, "time", cson_new_int(logger->startus));
    if (logger->event_type)
        cson_object_set(obj, "type",
                        cson_value_new_string(logger->event_type,
                                              strlen(logger->event_type)));

    if (logger->stmt && eventlog_detailed) {
        cson_object_set(obj, "sql", cson_value_new_string(
                                        logger->stmt, strlen(logger->stmt)));
        cson_object_set(obj, "bound_parameters", logger->bound_param_cson);
    }

    snap_uid_t snap, *p = NULL;
    if (logger->iq && logger->iq->have_snap_info) /* for txn type */
        p = &logger->iq->snap_info;
    else if (logger->clnt && get_cnonce(logger->clnt, &snap) == 0)
        p = &snap;
    if (p)
        cson_object_set(obj, "cnonce", cson_value_new_string(p->key, p->keylen));

    if (logger->have_id)
        cson_object_set(obj, "id",
                        cson_value_new_string(logger->id, sizeof(logger->id)));
    if (logger->sqlcost)
        cson_object_set(obj, "cost", cson_new_double(logger->sqlcost));
    if (logger->sqlrows)
        cson_object_set(obj, "rows", cson_new_int(logger->sqlrows));
    if (logger->vreplays)
        cson_object_set(obj, "replays", cson_new_int(logger->vreplays));

    if (logger->error) {
        cson_object_set(obj, "error_code", cson_new_int(logger->error_code));
        cson_object_set(
            obj, "error",
            cson_value_new_string(logger->error, strlen(logger->error)));
    }

    cson_object_set(obj, "host",
                    cson_value_new_string(gbl_mynode, strlen(gbl_mynode)));

    if (logger->have_fingerprint) {
        char expanded_fp[2 * FINGERPRINTSZ + 1];
        util_tohex(expanded_fp, logger->fingerprint, FINGERPRINTSZ);
        cson_object_set(obj, "fingerprint",
                        cson_value_new_string(expanded_fp, FINGERPRINTSZ * 2));
    }

    if (logger->queuetimeus)
        cson_object_set(obj, "qtime", cson_new_int(logger->queuetimeus));
    if (logger->clnt) {
        uint64_t clientstarttime = get_client_starttime(logger->clnt);
        if (clientstarttime && logger->startus > clientstarttime)
            cson_object_set(obj, "startlag", /* in microseconds */
                            cson_new_int(logger->startus - clientstarttime));
        int clientretries = get_client_retries(logger->clnt);
        if (clientretries > 0) {
            cson_object_set(obj, "clientretries",
                    cson_new_int(clientretries));
        }
    }

    eventlog_context(obj, logger);
    eventlog_perfdata(obj, logger);
    eventlog_tables(obj, logger);
    eventlog_path(obj, logger);
}

void eventlog_add(const struct reqlogger *logger)
{
    if (eventlog == NULL || !eventlog_enabled)
        return;

    int sz = 0;
    char *fname;
    cson_value *val;
    cson_object *obj;

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

    if (eventlog_verbose) cson_output(val, write_logmsg, stdout, &opt);

    cson_value_free(val);
}

void cson_snap_info_key(cson_object *obj, snap_uid_t *snap_info)
{
    if (obj && snap_info)
        cson_object_set(obj, "cnonce", cson_value_new_string(
                                           snap_info->key, snap_info->keylen));
}

void eventlog_status(void)
{
    if (eventlog_enabled == 1)
        logmsg(LOGMSG_USER, "Eventlog enabled, file:%s\n", gbl_eventlog_fname);
    else
        logmsg(LOGMSG_USER, "Eventlog disabled\n");
}

static void eventlog_roll(void)
{
    eventlog_close();
    eventlog = eventlog_open();
}

static void eventlog_enable(void)
{
    eventlog_enabled = 1;
    eventlog_roll();
}

static void eventlog_disable(void)
{
    eventlog_close();
    eventlog = NULL;
    eventlog_enabled = 0;
    bytes_written = 0;
}

void eventlog_stop(void)
{
    eventlog_disable();
}

static void eventlog_process_message_locked(char *line, int lline, int *toff)
{
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
            logmsg(LOGMSG_ERROR,
                   "Invalid #files to keep for \"sqllogger keep (must "
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
            logmsg(LOGMSG_ERROR, "Expected number of bytes in a log file\n");
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
            logmsg(LOGMSG_USER, "Rolling logs after %zd bytes\n", rollat);
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
    } else if (tokcmp(tok, ltok, "verbose") == 0) {
        tok = segtok(line, lline, toff, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected on/off for 'verbose'\n");
            return;
        }
        if (tokcmp(tok, ltok, "on") == 0)
            eventlog_verbose = 1;
        else if (tokcmp(tok, ltok, "off") == 0)
            eventlog_verbose = 0;
        else {
            logmsg(LOGMSG_ERROR, "Expected on/off for 'verbose'\n");
            return;
        }
    } else if (tokcmp(tok, ltok, "flush") == 0) {
        gzflush(eventlog, 1);
    } else {
        logmsg(LOGMSG_ERROR, "Unknown eventlog command\n");
        return;
    }
}

void eventlog_process_message(char *line, int lline, int *toff)
{
    pthread_mutex_lock(&eventlog_lk);
    eventlog_process_message_locked(line, lline, toff);
    pthread_mutex_unlock(&eventlog_lk);
}

void log_deadlock_cycle(locker_info *idmap, u_int32_t *deadmap,
                        u_int32_t nlockers, u_int32_t victim)
{
    if (eventlog == NULL)
        return;
    if (!eventlog_enabled)
        return;

    cson_value *dval = cson_value_new_object();
    cson_object *obj = cson_value_get_object(dval);

    cson_value *dd_list = cson_value_new_array();
    int64_t comdb2_time_epochus(void);
    uint64_t startus = comdb2_time_epochus();
    cson_object_set(obj, "time", cson_new_int(startus));
    extern char *gbl_mynode;
    cson_object_set(obj, "host",
                    cson_value_new_string(gbl_mynode, strlen(gbl_mynode)));
    cson_object_set(obj, "deadlock_cycle", dd_list);
    cson_array *arr = cson_value_get_array(dd_list);
    cson_array_reserve(arr, nlockers);

    for (int j = 0; j < nlockers; j++) {
        if (!ISSET_MAP(deadmap, j))
            continue;

        cson_value *lobj = cson_value_new_object();
        cson_object *vobj = cson_value_get_object(lobj);

        void cson_snap_info_key(cson_object * obj, snap_uid_t * snap_info);
        cson_snap_info_key(vobj, idmap[j].snap_info);
        char hex[11];
        sprintf(hex, "0x%x", idmap[j].id);
        cson_object_set(vobj, "lid", cson_value_new_string(hex, strlen(hex)));
        cson_object_set(vobj, "lcount", cson_value_new_integer(idmap[j].count));
        if (j == victim)
            cson_object_set(vobj, "victim", cson_value_new_bool(1));
        cson_array_append(arr, lobj);
    }
    logmsg(LOGMSG_USER, "\n");

    pthread_mutex_lock(&eventlog_lk);
    cson_output(dval, write_json, eventlog, &opt);
    pthread_mutex_unlock(&eventlog_lk);
}
