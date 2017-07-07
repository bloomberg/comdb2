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

#include <zlib.h>

#include "reqlog_int.h"
#include "eventlog.h"
#include "util.h"
#include "plhash.h"
#include "logmsg.h"

#include "cson_amalgamation_core.h"

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

struct sqltrack {
    char fingerprint[16];
    char *sql;
    LINKC_T(struct sqltrack) lnk;
};

LISTC_T(struct sqltrack) sql_statements;

static hash_t *seen_sql;

void eventlog_init(const char *dbname)
{
    seen_sql = hash_init_o(offsetof(struct sqltrack, fingerprint), 16);
    listc_init(&sql_statements, offsetof(struct sqltrack, lnk));
    if (eventlog_enabled) eventlog = eventlog_open();
}

static gzFile eventlog_open()
{
    char *fname = eventlog_fname(thedb->envname);
    gzFile f = gzopen(fname, "2w");
    free(fname);
    if (f == NULL) {
        eventlog_enabled = 0;
        return NULL;
    }
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
}

static char *eventlog_fname(const char *dbname)
{
    return comdb2_location("logs", "%s.events.%" PRId64 "", dbname,
                           time_epochus());
}

static cson_output_opt opt = {.indentation = 0,
                              .maxDepth = 4096,
                              .addNewline = 1,
                              .addSpaceAfterColon = 1,
                              .indentSingleMemberValues = 0,
                              .escapeForwardSlashes = 1};

void eventlog_params(struct reqlogger *logger, sqlite3_stmt *stmt,
                     struct schema *params, struct sqlclntstate *clnt)
{
    if (eventlog == NULL) return;
    if (!eventlog_enabled) return;
    if (!eventlog_detailed) return;

    cson_value *bind_list = cson_value_new_array();
    logger->bound_param_cson = bind_list;

    cson_array *arr = cson_value_get_array(bind_list);

    int nfields = params ? params->nmembers : clnt->sql_query->n_bindvars;
    cson_array_reserve(arr, nfields);

    int blobno = 0;
    for (int i = 0; i < nfields; i++) {
        cson_value *binding = cson_value_new_object();
        cson_object *bobj = cson_value_get_object(binding);

        struct field c_fld;
        struct field *f;
        char *buf;

        if (params) {
            f = &params->member[i];
            buf = (char *)clnt->tagbuf;
        } else {
            f = convert_client_field(clnt->sql_query->bindvars[i], &c_fld);
            buf = clnt->sql_query->bindvars[i]->value.data;
        }

        /* name of bound parameter */
        cson_object_set(bobj, "name",
                        cson_value_new_string(f->name, strlen(f->name)));

        /* bind binding to array of bindings */
        cson_array_append(arr, binding);

        /* mostly taken from sqltype() and bind_parameters() */
        int dlen = f->datalen;
        const char *strtype = "";
        switch (f->type) {
        case CLIENT_UINT:
        case CLIENT_INT:
            /* set type */
            switch (dlen) {
            case 2: strtype = "smallint"; break;
            case 4: strtype = "int"; break;
            case 8: strtype = "largeint"; break;
            }
            cson_object_set(bobj, "type",
                            cson_value_new_string(strtype, strlen(strtype)));

            /* set value */
            if (f->type == CLIENT_UINT) {
                uint64_t uival = *(uint64_t *)(buf + f->offset);
                cson_object_set(bobj, "value", cson_value_new_integer(uival));
            } else {
                int64_t ival = *(int64_t *)(buf + f->offset);
                cson_object_set(bobj, "value", cson_value_new_integer(ival));
            }
            break;
        case CLIENT_REAL: {
            /* set type */
            switch (dlen) {
            case 4: strtype = "float"; break;
            case 8: strtype = "doublefloat"; break;
            }
            cson_object_set(bobj, "type",
                            cson_value_new_string(strtype, strlen(strtype)));

            double dval = *(double *)(buf + f->offset);
            cson_object_set(bobj, "value", cson_value_new_double(dval));
            break;
        }
        case CLIENT_CSTR:
        case CLIENT_PSTR:
        case CLIENT_PSTR2: {
            char *str;
            int datalen;

            /* set type */
            strtype = "char";
            cson_object_set(bobj, "type",
                            cson_value_new_string(strtype, strlen(strtype)));

            /* set value */
            if (get_str_field(f, buf, &str, &datalen) == 0)
                cson_object_set(bobj, "value",
                                cson_value_new_string(str, datalen));
            break;
        }
        case CLIENT_BYTEARRAY:
        case CLIENT_BLOB:
        case CLIENT_VUTF8: {
            void *byteval;
            int datalen;
            int rc = 0;

            /* set type */
            strtype = "blob";
            if (f->type == CLIENT_VUTF8) strtype = "varchar";
            cson_object_set(bobj, "type",
                            cson_value_new_string(strtype, strlen(strtype)));

            /* set value */
            if (f->type == CLIENT_BYTEARRAY) {
                rc = get_byte_field(f, buf, &byteval, &datalen);
            } else {
                if (params) {
                    rc = get_blob_field(blobno, clnt, &byteval, &datalen);
                    blobno++;
                }
            }
            if (rc == 0)
                cson_object_set(bobj, "value",
                                cson_value_new_string(byteval, datalen));
            break;
        }
        case CLIENT_DATETIME: {
            char strtime[62];

            /* set type */
            strtype = "datetime";
            cson_object_set(bobj, "type",
                            cson_value_new_string(strtype, strlen(strtype)));

            /* set value */
            if (structdatetime2string_ISO((void *)buf, strtime,
                                          sizeof(strtime)) == 0)
                cson_object_set(bobj, "value", cson_value_new_string(
                                                   strtime, sizeof(strtime)));
            break;
        }
        case CLIENT_DATETIMEUS: {
            char strtime[65];

            /* set type */
            strtype = "datetimeus";
            cson_object_set(bobj, "type",
                            cson_value_new_string(strtype, strlen(strtype)));

            /* set value */
            if (structdatetime2string_ISO((void *)buf, strtime,
                                          sizeof(strtime)) == 0)
                cson_object_set(bobj, "value", cson_value_new_string(
                                                   strtime, sizeof(strtime)));
            break;
        }
        case CLIENT_INTVYM:
            strtype = "interval month";
            cson_object_set(bobj, "type",
                            cson_value_new_string(strtype, strlen(strtype)));
            break;
        case CLIENT_INTVDS:
            strtype = "interval sec";
            cson_object_set(bobj, "type",
                            cson_value_new_string(strtype, strlen(strtype)));
            break;
        case CLIENT_INTVDSUS:
            strtype = "interval usec";
            cson_object_set(bobj, "type",
                            cson_value_new_string(strtype, strlen(strtype)));
            break;
        }
    }
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
    int64_t start, end;

    start = logger->startus;
    end = time_epochus();

    cson_value *perfval = cson_value_new_object();
    cson_object *perfobj = cson_value_get_object(perfval);

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
    logmsg(LOGMSG_USER, "%.*s", n, src);
    return 0;
}

static void eventlog_context(cson_object *obj, const struct reqlogger *logger)
{
    cson_value *contexts = cson_value_new_array();
    if (logger->ncontext > 0) {
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
    if (eventlog == NULL) return;
    if (!eventlog_enabled) return;

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
    if (eventlog == NULL) return;
    if (!eventlog_enabled) return;

    static const char *hexchars = "0123456789abcdef";
    pthread_mutex_lock(&eventlog_lk);
    if (logger->event_type && strcmp(logger->event_type, "sql") == 0 &&
        !hash_find(seen_sql, logger->fingerprint)) {
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

        char fingerprint[32];
        for (int i = 0; i < 16; i++) {
            fingerprint[i * 2] =
                hexchars[((logger->fingerprint[i] & 0xf0) >> 4)];
            fingerprint[i * 2 + 1] = hexchars[logger->fingerprint[i] & 0x0f];
        }
        cson_object_set(
            newobj, "fingerprint",
            cson_value_new_string(fingerprint, sizeof(fingerprint)));

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

    if (logger->stmt && eventlog_detailed)
        cson_object_set(obj, "sql", cson_value_new_string(
                                        logger->stmt, strlen(logger->stmt)));

    if (logger->have_id)
        cson_object_set(obj, "id",
                        cson_value_new_string(logger->id, sizeof(logger->id)));
    if (logger->sqlcost)
        cson_object_set(obj, "cost", cson_new_double(logger->sqlcost));
    if (logger->sqlrows)
        cson_object_set(obj, "rows", cson_new_int(logger->sqlrows));
    if (logger->vreplays)
        cson_object_set(obj, "replays", cson_new_int(logger->vreplays));

    if (logger->error)
        cson_object_set(
            obj, "error",
            cson_value_new_string(logger->error, strlen(logger->error)));

    cson_object_set(obj, "host",
                    cson_value_new_string(gbl_mynode, strlen(gbl_mynode)));

    if (logger->have_fingerprint) {
        char fingerprint[32];
        for (int i = 0; i < 16; i++) {
            fingerprint[i * 2] =
                hexchars[((logger->fingerprint[i] & 0xf0) >> 4)];
            fingerprint[i * 2 + 1] = hexchars[logger->fingerprint[i] & 0x0f];
        }
        cson_object_set(obj, "fingerprint",
                        cson_value_new_string(fingerprint, 32));
        // printf("%s -> %.*s\n", logger->stmt, sizeof(fingerprint),
        // fingerprint);
    }

    if (logger->queuetimeus)
        cson_object_set(obj, "qtime", cson_new_int(logger->queuetimeus));

    eventlog_context(obj, logger);
    if (eventlog_detailed)
        cson_object_set(obj, "bound_parameters", logger->bound_param_cson);
    eventlog_perfdata(obj, logger);
    eventlog_tables(obj, logger);
    eventlog_path(obj, logger);
}

void eventlog_add(const struct reqlogger *logger)
{
    int sz = 0;
    char *fname;
    cson_value *val;
    cson_object *obj;

    if (eventlog == NULL) return;
    if (!eventlog_enabled) return;

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
