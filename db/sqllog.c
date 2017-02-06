/*
   Copyright 2015 Bloomberg Finance L.P.

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
#include <stddef.h>
#include <sys/time.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/uio.h>

#include "list.h"
#include "segstring.h"
#include "comdb2.h"
#include "sql.h"
#include "pool.h"
#include "roll_file.h"
#include "util.h"
#include "logmsg.h"
#include "log.pb-c.h"

static pthread_mutex_t sql_log_lk = PTHREAD_MUTEX_INITIALIZER;
int gbl_log_all_sql = 0;
static FILE *sqllog = NULL;

static const int32_t sqllog_version = 5;
static void sqllog_run_roll(int nkeep);
static FILE *sqllog_open(int quiet);
static off_t sqllog_rollat_size = 0;
static int sqllog_keep_files = 2;
/* Apparently this is a "feature" that "standard" products have. */
static int sqllog_every_n = 0;
static int sqllog_every_counter = 0;
static int sqllog_use_async = 1;
#define DEFAULT_ASYNC_MAXSIZE 1024 * 1024 * 4
static int sqllog_async_maxsize = DEFAULT_ASYNC_MAXSIZE;

static pthread_cond_t async_writer_wait = PTHREAD_COND_INITIALIZER;
static pool_t *event_pool;

/* TODO: mspace for event buffers? */
struct log_event {
    size_t bufsz;
    uint8_t *buf;
    LINKC_T(struct log_event) lnk;
};

static LISTC_T(struct log_event) sqllog_events;

static int async_size = 0;
static int async_maxsize = 0;
static int64_t async_nmessages = 0;
static int64_t async_ndrops = 0;
static int async_have_thread = 0;

static int sqllog_roll_locked(int nkeep, int quiet);
static void sqllog_changesync(int async);

static void free_event(struct log_event *e)
{
    free(e->buf);
    pool_relablk(event_pool, e);
}

static void *async_logthd(void *unused)
{
    struct log_event *e = NULL;
    int rc;

    for (;;) {
        pthread_mutex_lock(&sql_log_lk);
        if (e) {
            async_size -= e->bufsz;
            free_event(e);
        }

        e = listc_rtl(&sqllog_events);
        while (e == NULL) {
            pthread_cond_wait(&async_writer_wait, &sql_log_lk);
            e = listc_rtl(&sqllog_events);
        }
        /* don't hold lock while possibly doing IO */
        pthread_mutex_unlock(&sql_log_lk);

        /* this is our cue to stop */
        if (e->buf == 0 && e->bufsz == 0) {
            free_event(e);
            return NULL;
        }

        rc = fwrite(e->buf, e->bufsz, 1, sqllog);
        if (rc != 1) {
            /* do what? disable logging? */
        }
        async_nmessages++;

        if (sqllog_rollat_size && ftell(sqllog) > sqllog_rollat_size)
            sqllog_roll_locked(sqllog_keep_files, 1);
    }
}

static void async_enqueue(void *buf, int bufsz)
{
    struct log_event *e;

    if (!async_have_thread)
        sqllog_changesync(1);

    pthread_mutex_lock(&sql_log_lk);

    if (sqllog_async_maxsize && async_size + bufsz > sqllog_async_maxsize ||
        gbl_log_all_sql == 0) {
        async_ndrops++;
        pthread_mutex_unlock(&sql_log_lk);
        return;
    }

    async_size += bufsz;
    if (async_size > async_maxsize)
        async_maxsize = async_size;
    e = pool_getablk(event_pool);
    e->buf = buf;
    e->bufsz = bufsz;
    listc_abl(&sqllog_events, e);

    pthread_cond_signal(&async_writer_wait);
    pthread_mutex_unlock(&sql_log_lk);
}

static pthread_t sqllog_threadid;

static void sqllog_async_init_once(void)
{
    int rc;
    listc_init(&sqllog_events, offsetof(struct log_event, lnk));
    event_pool = pool_setalloc_init(sizeof(struct log_event), 20, malloc, free);
    if (event_pool == NULL) {
        logmsg(LOGMSG_FATAL, "can't create memory pool for sql log events\n");
        abort();
    }
}

static pthread_once_t async_init_once = PTHREAD_ONCE_INIT;

static void sqllog_changesync(int async)
{
    struct log_event *e;
    void *thread_rc;
    int rc;

    pthread_once(&async_init_once, sqllog_async_init_once);

    pthread_mutex_lock(&sql_log_lk);
    if (async == 0) {
        /* changing to sync? eat everything on the async queue  - treat them as
         * drops */
        if (async_have_thread) {
            e = listc_rtl(&sqllog_events);
            while (e) {
                async_ndrops++;
                async_size -= e->bufsz;
                free_event(e);
                e = listc_rtl(&sqllog_events);
            }
            /* add an event that tells the writer thread to exit */
            e = pool_getablk(event_pool);
            if (e == NULL) {
                logmsg(LOGMSG_ERROR, "%s:%d out of memory\n", __FILE__, __LINE__);
                pthread_mutex_unlock(&sql_log_lk);
                return;
            }
            e->bufsz = 0;
            e->buf = NULL;
            listc_atl(&sqllog_events, e);
            pthread_cond_signal(&async_writer_wait);
            pthread_mutex_unlock(&sql_log_lk);

            rc = pthread_join(sqllog_threadid, &thread_rc);
            if (rc)
                logmsg(LOGMSG_WARN, "rc %d waiting for sqllog thread\n", rc);

            pthread_mutex_lock(&sql_log_lk);
        }
        async_have_thread = 0;
        sqllog_use_async = 0;
    } else {
        rc = pthread_create(&sqllog_threadid, NULL, async_logthd, NULL);
        if (rc)
            logmsg(LOGMSG_ERROR, 
                    "can't create sql logger thread, logging disabled\n");
        sqllog_use_async = 1;
        async_have_thread = 1;
    }
    pthread_mutex_unlock(&sql_log_lk);
}

static FILE *sqllog_open(int quiet)
{
    char fname[255];
    int rc;
    int32_t version;
    FILE *f;

again:
    snprintf(fname, sizeof(fname), "%s/%s.sqllog", thedb->basedir,
             thedb->envname);

    f = fopen(fname, "r+");
    if (f == NULL) {
        f = fopen(fname, "w");
        if (f == NULL) {
            logmsg(LOGMSG_ERROR, "Can't open %s: %d %s\n", fname, errno,
                    strerror(errno));
            return NULL;
        }
    }

    rc = fseek(f, 0, SEEK_END);
    if (rc) {
        fclose(f);
        logmsg(LOGMSG_ERROR, "Failed to seek to end of SQL log: %d %s\n", errno,
                strerror(errno));
        return NULL;
    }
    /* if it's not a new file, write version number */
    if (ftell(f) == 0) {
        int32_t ver = htonl(sqllog_version);
        printf("ver %d\n", sqllog_version);
        rc = fwrite(&ver, sizeof(int32_t), 1, f);
        if (rc != 1) {
            logmsg(LOGMSG_ERROR, "Can't write version to SQL log file\n");
            fclose(f);
            return NULL;
        }
    } else {
        /* read the version number - if our version is different, roll the log
         */
        rc = fseek(f, 0, SEEK_SET);
        if (rc) {
            fclose(f);
            logmsg(LOGMSG_ERROR, "Failed to seek to start of SQL log: %d %s\n",
                    errno, strerror(errno));
            return NULL;
        }
        rc = fread(&version, sizeof(int32_t), 1, f);
        if (rc != 1) {
            fclose(f);
            logmsg(LOGMSG_ERROR, "Failed to read version number of SQL log: %d %s\n",
                    errno, strerror(errno));
            return NULL;
        }
        version = ntohl(version);
        if (version != sqllog_version) {
            struct stat st;
            logmsg(LOGMSG_INFO, "log version %d, my version %d, rolling log\n", version,
                   sqllog_version);
            fclose(f);
            /* roll, leave as many files as possible */
            sqllog_run_roll(100);
            if (stat(fname, &st) != 0 || st.st_size != 0) {
                logmsg(LOGMSG_ERROR, "Failed to roll logs, and I have a different "
                                "log version, disabled logging\n");
                return NULL;
            }
            goto again;
        }
        rc = fseek(f, 0, SEEK_END);
        if (rc) {
            fclose(f);
            logmsg(LOGMSG_ERROR, "Failed to seek to end of SQL log: %d %s\n", errno,
                    strerror(errno));
            return NULL;
        }
    }

    fflush(f);
    if (!quiet)
        logmsg(LOGMSG_INFO, "Opened SQL log\n");

    return f;
}

static int sqllog_enable(void)
{
    pthread_mutex_lock(&sql_log_lk);
    if (gbl_log_all_sql == 1) {
        pthread_mutex_unlock(&sql_log_lk);
        logmsg(LOGMSG_ERROR, "SQL logging already enabled\n");
        return 1;
    }

    if (sqllog == NULL) {
        sqllog = sqllog_open(0);
        if (sqllog == NULL) {
            pthread_mutex_unlock(&sql_log_lk);
            return 1;
        }
    }

    gbl_log_all_sql = 1;
    pthread_mutex_unlock(&sql_log_lk);
    logmsg(LOGMSG_USER, "SQL logging enabled\n");

    return 0;
}

static int sqllog_disable(void)
{
    pthread_mutex_lock(&sql_log_lk);
    if (gbl_log_all_sql == 0) {
        pthread_mutex_unlock(&sql_log_lk);
        logmsg(LOGMSG_ERROR, "SQL logging already disabled\n");
        return 1;
    }
    gbl_log_all_sql = 0;
    fflush(sqllog);
    pthread_mutex_unlock(&sql_log_lk);
    sqllog_changesync(0);
    logmsg(LOGMSG_USER, "SQL logging disabled\n");

    return 0;
}

static int sqllog_flush(void)
{
    pthread_mutex_lock(&sql_log_lk);
    if (sqllog == NULL) {
        logmsg(LOGMSG_ERROR, "SQL log not open (logging %senabled)\n",
                gbl_log_all_sql ? "" : "not ");
        pthread_mutex_unlock(&sql_log_lk);
        return 1;
    }
    fflush(sqllog);
    pthread_mutex_unlock(&sql_log_lk);
    logmsg(LOGMSG_USER, "Flushed SQL log\n");

    return 0;
}

static void sqllog_run_roll(int nkeep)
{
    char *name;
    name = comdb2_asprintf("%s/%s.sqllog", thedb->basedir, thedb->envname);
    roll_file(name, nkeep);
    free(name);
}

static int sqllog_roll_locked(int nkeep, int quiet)
{
    if (sqllog == NULL) {
        logmsg(LOGMSG_ERROR, "SQL log not open (logging %senabled)\n",
                gbl_log_all_sql ? "" : "not ");
        pthread_mutex_unlock(&sql_log_lk);
        return 1;
    }
    fflush(sqllog);
    fclose(sqllog);
    sqllog = NULL;

    sqllog_run_roll(nkeep);
    sqllog = sqllog_open(1);
    /* Can't open, disable (open will print trace on error) */
    if (sqllog == NULL)
        gbl_log_all_sql = 0;
    if (!quiet)
        logmsg(LOGMSG_USER, "Rolled SQL log\n");
    return 0;
}

static int sqllog_roll(int nkeep)
{
    int rc;
    pthread_mutex_lock(&sql_log_lk);
    rc = sqllog_roll_locked(nkeep, 0);
    pthread_mutex_unlock(&sql_log_lk);
    return rc;
}

static int log_async(struct sqlclntstate *clnt, int cost, int nrows, int timems)
{
    int32_t sz;
    CDB2Event ev;
    CDB2SQLEvent sql;
    struct timeval tv;
    uint8_t *buf;
    uuidstr_t fingerprint;

    cdb2__event__init(&ev);
    cdb2__sqlevent__init(&sql);
    gettimeofday(&tv, NULL);
    ev.seconds = tv.tv_sec;
    ev.useconds = tv.tv_usec;
    ev.sql = &sql;
    ev.sql->cost = cost;
    ev.sql->nrows = nrows;
    ev.sql->timems = timems;
    ev.sql->query = clnt->sql_query;
    comdb2uuidstr(clnt->fingerprint, fingerprint);
    ev.sql->fingerprint = fingerprint;
    sz = cdb2__event__get_packed_size(&ev);
    sz += sizeof(int32_t);
    buf = malloc(sz);
    int32_t szf = htonl(sz);
    memcpy(buf, &szf, sizeof(int32_t));
    cdb2__event__pack(&ev, buf + sizeof(int32_t));
    async_enqueue(buf, sz);
    return 0;
}

void sqllog_log_statement(struct sqlclntstate *clnt, int cost, int nrows,
                          int timems)
{
    struct timeval t;
    int32_t ival;
    int sqllen;
    int rc;

/* For each statement, log the following:
 *
 * int threadid
 * int tv_sec
 * int tv_usec
 * int sqllen
 * char sql[sqllen]
 * int tagstrlen
 * char tag[tagstrlen]
 * int tagbuflen
 * char tagbuf[tagbuflen]
 * int nullbits
 * char [nullbits]
 * int numblobs
 * int bloblens[numblobs]
 * int noverrides
 * int overrides[noverrides]
 * int cost
 * int time
 * int64_t tranid (v2)
 */
#define WRITE_INT(v)                                                           \
    do {                                                                       \
        ival = v;                                                              \
        rc = fwrite(&ival, sizeof(int32_t), 1, sqllog);                        \
        if (rc != 1)                                                           \
            goto log_err;                                                      \
    } while (0)

    /* TODO: why isn't this being set? */
    sqllen = strlen(clnt->sql) + 1;

    if (sqllog_use_async) {
        log_async(clnt, cost, nrows, timems);
        return;
    }

    rc = gettimeofday(&t, NULL);
    if (rc)
        goto log_err;

    pthread_mutex_lock(&sql_log_lk);
    if (sqllog_every_n) {
        sqllog_every_counter++;
        if (sqllog_every_counter >= sqllog_every_n)
            sqllog_every_counter = 0;
        else {
            pthread_mutex_unlock(&sql_log_lk);
            return;
        }
    }
    if (sqllog == NULL || gbl_log_all_sql == 0) {
        pthread_mutex_unlock(&sql_log_lk);
        return;
    }

    WRITE_INT((int32_t)pthread_self());
    WRITE_INT(t.tv_sec);
    WRITE_INT(t.tv_usec);
    WRITE_INT(sqllen);
    rc = fwrite(clnt->sql, sqllen, 1, sqllog);
    if (rc != 1)
        goto log_err;
    if (clnt->tag) {
        WRITE_INT(strlen(clnt->tag) + 1);
        rc = fwrite(clnt->tag, strlen(clnt->tag) + 1, 1, sqllog);
        if (rc != 1)
            goto log_err;
        WRITE_INT(clnt->tagbufsz);
        rc = fwrite(clnt->tagbuf, clnt->tagbufsz, 1, sqllog);
        if (rc != 1)
            goto log_err;
        WRITE_INT(clnt->numnullbits);
        if (clnt->numnullbits > 0) {
            rc = fwrite(clnt->nullbits, clnt->numnullbits, 1, sqllog);
            if (rc != 1)
                goto log_err;
        }
        WRITE_INT(clnt->numblobs);
        for (int i = 0; i < clnt->numblobs; i++) {
            WRITE_INT(clnt->bloblens[i]);
            if (clnt->bloblens[i] > 0) {
                rc = fwrite(clnt->blobs[i], clnt->bloblens[i], 1, sqllog);
                if (rc != 1)
                    goto log_err;
            }
        }
    } else {
        WRITE_INT(-1);
    }
    /* TODO: is this value valid at this point? */
    WRITE_INT(clnt->req.parm);
    if (clnt->req.parm > 0) {
        rc = fwrite(clnt->type_overrides, sizeof(int), clnt->req.parm, sqllog);
        if (rc != clnt->req.parm)
            goto log_err;
    }
    WRITE_INT(cost);
    WRITE_INT(nrows);
    WRITE_INT(timems);
    rc = fwrite(&clnt->osql.rqid, sizeof(unsigned long long), 1, sqllog);
    if (rc != 1)
        goto log_err;

    if (0) {
    log_err:
        logmsg(LOGMSG_ERROR, "Had an error logging an SQL statement?\n");
    }

    if (sqllog_rollat_size && ftell(sqllog) > sqllog_rollat_size)
        sqllog_roll_locked(sqllog_keep_files, 1);

    pthread_mutex_unlock(&sql_log_lk);
#undef WRITE_INT
}

void sqllogger_process_message(char *line, int lline)
{
    int st = 0;
    int ltok = 0;
    char *tok;

    tok = segtok(line, lline, &st, &ltok);
    if (ltok == 0) {
        logmsg(LOGMSG_ERROR, "Expected option for sqllogger\n");
        return;
    }
    if (tokcmp(tok, ltok, "on") == 0)
        sqllog_enable();
    else if (tokcmp(tok, ltok, "off") == 0)
        sqllog_disable();
    else if (tokcmp(tok, ltok, "flush") == 0)
        sqllog_flush();
    else if (tokcmp(tok, ltok, "roll") == 0) {
        int nfiles = sqllog_keep_files;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok)
            nfiles = toknum(tok, ltok);
        if (ltok == 0) {
            if (nfiles < 1 || nfiles > 100) {
                logmsg(LOGMSG_ERROR, "Invalid #files to keep for \"sqllogger roll "
                                "(must be between 1 and 100\"\n");
                return;
            }
        }
        logmsg(LOGMSG_DEBUG, "roll nfiles %d\n", nfiles);
        sqllog_roll(nfiles);
    } else if (tokcmp(tok, ltok, "keep") == 0) {
        int nfiles;
        tok = segtok(line, lline, &st, &ltok);
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
        sqllog_keep_files = nfiles;
        logmsg(LOGMSG_USER, "Keeping %d logs\n", sqllog_keep_files);
    } else if (tokcmp(tok, ltok, "rollat") == 0) {
        off_t rollat;
        char *s;
        tok = segtok(line, lline, &st, &ltok);
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
        sqllog_rollat_size = rollat;
    } else if (tokcmp(tok, ltok, "every") == 0) {
        int every;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected a count for 'every'\n");
            return;
        }
        every = toknum(tok, ltok);
        if (every == 0) {
            logmsg(LOGMSG_USER, "Logging all queries\n");
        } else if (every < 0) {
            logmsg(LOGMSG_ERROR, "Invalid count for 'every'\n");
            return;
        } else
            logmsg(LOGMSG_USER, "Logging every %d queries\n", sqllog_every_n);
        sqllog_every_n = every;
    } else if (tokcmp(tok, ltok, "async") == 0) {
        if (sqllog_use_async)
            logmsg(LOGMSG_USER, "already using async\n");
        else {
            logmsg(LOGMSG_USER, "sqllogger using async\n");
            sqllog_changesync(1);
        }
    } else if (tokcmp(tok, ltok, "sync") == 0) {
        if (!sqllog_use_async)
            logmsg(LOGMSG_USER, "already using sync\n");
        else {
            logmsg(LOGMSG_USER, "sqllogger using sync\n");
            sqllog_changesync(0);
        }
    } else if (tokcmp(tok, ltok, "asyncsize") == 0) {
        int size;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected value for asyncsize, or -1 for unbounded "
                            "(default %d current max %d, current %d)",
                    DEFAULT_ASYNC_MAXSIZE, sqllog_async_maxsize, async_size);
            return;
        }
        size = toknum(tok, ltok);
        if (size == 0 || size < -1) {
            logmsg(LOGMSG_ERROR, "Invalid value for asyncsize\n");
            return;
        }
        sqllog_async_maxsize = size;
    } else if (tokcmp(tok, ltok, "stat") == 0) {
        logmsg(LOGMSG_USER, "async logged %d dropped %d size %d max %d\n", async_nmessages,
               async_ndrops, async_size, async_maxsize);
    } else {
        logmsg(LOGMSG_ERROR, "Unknown sqllogger command\n");
        return;
    }
}

void sqllog_save_event(struct sqlclntstate *clnt, char *p, int bytes) {
}

