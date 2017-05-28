#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stddef.h>

#include "reqlog_int.h"
#include "eventlog.h"
#include "roll_file.h"
#include "util.h"
#include "plhash.h"

static char* eventlog_fname(const char *dbname);
static int eventlog_nkeep = 10;
static int eventlog_rollat = 16*1024*1024;
static int eventlog_enabled = 1;
static int eventlog_detailed = 0;

static FILE *eventlog = NULL;
static pthread_mutex_t eventlog_lk = PTHREAD_MUTEX_INITIALIZER;
static FILE *eventlog_open(const char *fname);
int eventlog_every_n = 1;
int64_t eventlog_count = 0;

void eventlog_init(const char *dbname) {
    char *filename = eventlog_fname(dbname);
    eventlog = eventlog_open(filename);
    free(filename);
}

static FILE *eventlog_open(const char *fname) {
    FILE *f = fopen(fname, "a");
    off_t sz;
    if (f == NULL)
        return NULL;
    sz = ftello(f);
    if (sz != 0) {
        fclose(f);
        roll_file(fname, eventlog_nkeep);
    }
    f = fopen(fname, "a");
    fprintf(f, "[\n");
    return f;
}

static void eventlog_close(void) {
    if (eventlog == NULL)
        return;
    fprintf(eventlog, "{ \"type\" : \"end\" }\n]\n");
    fflush(eventlog);
    fclose(eventlog);
    eventlog = NULL;
}

static char* eventlog_fname(const char *dbname) {
    return comdb2_location("logs", "%s.events", dbname);
}

static void eventlog_string(FILE *log, char *in, int *sz) {
    char *s = in;
    *sz += fprintf(log, "\"");
    if (in) {
        while (*s) {
            switch (*s) {
                case '\"':
                    *sz += fprintf(log, "\\\"");
                    break;
                case '\n':
                    *sz += fprintf(log, "\\n");
                    break;
                case '\b':
                    *sz += fprintf(log, "\\b");
                    break;
                case '/':
                    *sz += fprintf(log, "\\/");
                    break;
                case '\f':
                    *sz += fprintf(log, "\\f");
                    break;
                case '\r':
                    *sz += fprintf(log, "\\r");
                    break;
                case '\t':
                    *sz += fprintf(log, "\\t");
                    break;
                default:
                    putc(*s, log);
                    *sz += 1;
                    break;
            }
            s++;
        }
    }
    *sz += fprintf(log, "\"");
}

void eventlog_params(FILE *log, const struct reqlogger *logger, int *sz) {
    if (logger->request && logger->request->n_bindvars > 0) {
        *sz += fprintf(log, ", \"le\" : %s", logger->request->little_endian ? "true" : "false");
        *sz += fprintf(log, ", \"bindings\" : [ ");
        for (int i = 0; i < logger->request->n_bindvars; i++) {
            *sz += fprintf(log, " { ");
            CDB2SQLQUERY__Bindvalue *val = logger->request->bindvars[i];
            if (val->varname)
                *sz += fprintf(log, "\"name\" : \"%s\", ", val->varname); 
            else
                *sz += fprintf(log, "\"index\" : %d,  ", val->index); 
            if (!val->has_isnull && val->isnull) {
                /* null, omit value */
                *sz += fprintf(log, "\"value\" : null ");
            }
            else {
                *sz += fprintf(log, "\"type\" : %d, ", val->type);
                *sz += fprintf(log, "\"len\" : %d, ", val->value.len);
                *sz += fprintf(log, "\"value\" : \"");
                for (int i = 0; i < val->value.len; i++) {
                    static const char *hexchars = "0123456789abcdef";

                    putc(hexchars[((val->value.data[i] & 0xf0) >> 4)], log);
                    putc(hexchars[val->value.data[i] & 0x0f], log);
                }
                *sz += val->value.len * 2;
                *sz += fprintf(log, "\"");
            }
            *sz += fprintf(log, " } ");
            if (i != logger->request->n_bindvars-1)
                *sz += fprintf(log, ", ");
        }
        *sz += fprintf(log, " ]");
    }
}

void eventlog_perfdata(FILE *log, const struct reqlogger *logger, int *sz) {
    const struct bdb_thread_stats *thread_stats = bdb_get_thread_stats();
    int first = 1;

    if (thread_stats->n_lock_waits || thread_stats->n_preads || thread_stats->n_pwrites || thread_stats->pread_time_ms || thread_stats->pwrite_time_ms || thread_stats->lock_wait_time_ms) {
        *sz += fprintf(log, ", \"perf\" : { ");

        if (thread_stats->n_lock_waits) {
            *sz += fprintf(log,  "%c \"lockwaits\" :  %d", first ? ' ': ',', thread_stats->n_lock_waits);
            *sz += fprintf(log,  ", \"lockwaittime\" :  %d", first ? ' ': ',', thread_stats->lock_wait_time_ms);
            first = 0;
        }
        if (thread_stats->n_preads) {
            *sz += fprintf(log,  "%c \"reads\" :  %d", first ? ' ': ',', thread_stats->n_preads);
            *sz += fprintf(log,  ", \"readtime\" :  %d", first ? ' ': ',', thread_stats->pread_time_ms);
            first = 0;
        }
        if (thread_stats->n_pwrites) {
            *sz += fprintf(log,  "%c \"reads\" :  %d", first ? ' ': ',', thread_stats->n_pwrites);
            *sz += fprintf(log,  ", \"readtime\" :  %d", first ? ' ': ',', thread_stats->pwrite_time_ms);
            first = 0;
        }

        *sz += fprintf(log, "} ");
    }
}

static void eventlog_locked(FILE *log, const struct reqlogger *logger, int *sz) {
    static const char *hexchars = "0123456789abcdef";

    int detailed = eventlog_detailed;
    *sz += fprintf(log, "{  \"type\" : \"%s\" ", logger->event_type);

    if (logger->stmt && detailed) {
        *sz += fprintf(log, ", \"sql\" : ");
        eventlog_string(log, logger->stmt, sz);
    }

    if (logger->have_id)
        *sz += fprintf(log, ", \"id\" : \"%s\"", logger->id);
    if (logger->sqlcost)
        *sz += fprintf(log, ", \"cost\" : %f", logger->sqlcost);
    if (logger->sqlrows)
        *sz += fprintf(log, ", \"rows\" : %d", logger->sqlrows);
    if (logger->vreplays)
        *sz += fprintf(log, ", \"replays\" : %d", logger->vreplays);
    if (logger->have_fingerprint) {
        *sz += fprintf(log, ", \"fingerprint\" : \"");
        for (int i = 0; i < 15; i++) {
            putc(hexchars[((logger->fingerprint[i] & 0xf0) >> 4)], log);
            putc(hexchars[logger->fingerprint[i] & 0x0f], log);
        }
        fprintf(log, "\"");
    }
    *sz += 32;
    *sz += fprintf(log, ", \"duration\" : %d", logger->durationms);
    if (logger->queuetimems)
        *sz += fprintf(log, ", \"qtime\" : %d", logger->queuetimems);

    if (detailed)
        eventlog_params(log, logger, sz);

    eventlog_perfdata(log, logger, sz);

    *sz += fprintf(log, "}, \n");
}

void eventlog_add(const struct reqlogger *logger) {
    int sz = 0;
    char *fname;

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
    eventlog_locked(eventlog, logger, &sz);
    if (sz > eventlog_rollat) {
        eventlog_close();
        char *fname = eventlog_fname(thedb->envname);
        eventlog = eventlog_open(fname);
        free(fname);
    }
    pthread_mutex_unlock(&eventlog_lk);
}

static void eventlog_roll(void) {
    char *fname = eventlog_fname(thedb->envname);
    pthread_mutex_lock(&eventlog_lk);
    eventlog_close();
    eventlog = eventlog_open(fname);
    pthread_mutex_unlock(&eventlog_lk);
    free(fname);
}

static void eventlog_enable(void) {
    pthread_mutex_lock(&eventlog_lk);
    eventlog_enabled = 1;
    eventlog_roll();
    pthread_mutex_unlock(&eventlog_lk);
}

static void eventlog_disable(void) {
    pthread_mutex_lock(&eventlog_lk);
    eventlog_enabled = 0;
    eventlog_close();
    eventlog = NULL;
    pthread_mutex_unlock(&eventlog_lk);
}

void eventlog_stop(void) {
    eventlog_disable();
}

void eventlog_process_message(char *line, int lline, int *toff) {
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
    } else {
        logmsg(LOGMSG_ERROR, "Unknown eventlog command\n");
        return;
    }
} 
