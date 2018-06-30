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

#include "bdb_api.h"
#include "bdb_int.h"

#include <build/db_int.h>
#include "llog_auto.h"
#include "llog_ext.h"
#include "printformats.h"

#include <dbinc/db_swap.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctrace.h>
#include <string.h>
#include <alloca.h>

#include <logmsg.h>
#include "util.h"

static int bdb_blkseq_update_lsn_locked(bdb_state_type *bdb_state,
                                        int timestamp, DB_LSN lsn, int stripe);

static DB *create_blkseq(bdb_state_type *bdb_state, int stripe, int num)
{
    char fname[1024];
    DB *db;
    DB_ENV *env;
    int fd;
    int rc;

    env = bdb_state->blkseq_env[stripe];

    rc = db_create(&db, env, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "blkseq create rc %d\n", rc);
        return NULL;
    }
    snprintf(fname, sizeof(fname), "%s/_blkseq.%d.%d.XXXXXX", bdb_state->tmpdir,
             stripe, num);
    fd = mkstemp(fname);
    if (fd == -1) {
        logmsg(LOGMSG_ERROR, "mkstemp rc %d %s\n", errno, strerror(errno));
        return NULL;
    }
    rc = fchmod(fd, 0666);
    if (rc) {
        close(fd);
        logmsg(LOGMSG_ERROR, "fchmod rc %d %s\n", errno, strerror(errno));
        return NULL;
    }
    ctrace("blkseq stripe %d file %d %s\n", stripe, num, fname);
    rc = db->open(db, NULL, fname, NULL, DB_BTREE, DB_CREATE | DB_TRUNCATE,
                  0666);
    /* we don't need the descriptor mkstemp creates, just need a unique name */
    close(fd);
    if (rc) {
        logmsg(LOGMSG_ERROR, "blkseq->open rc %d\n", rc);
        return NULL;
    }
    return db;
}

void bdb_cleanup_private_blkseq(bdb_state_type *bdb_state)
{
    if (!bdb_state) 
        return;
    for (int stripe = 0; stripe < bdb_state->attr->private_blkseq_stripes;
         stripe++) {
        DB_ENV *env = bdb_state->blkseq_env[stripe];
        if (env) {
            pthread_mutex_destroy(&bdb_state->blkseq_lk[stripe]);
            for (int i = 0; i < 2; i++) {
                DB *to_be_deleted = bdb_state->blkseq[i][stripe];
                to_be_deleted->close(to_be_deleted, DB_NOSYNC);
            }

            env->close(env, 0);
            bdb_state->blkseq_env[stripe] = NULL;
        }
    }

    if (bdb_state->blkseq_env) {
        free(bdb_state->blkseq_env);
        bdb_state->blkseq_env = NULL;
    }

    if (bdb_state->blkseq_lk) {
        free(bdb_state->blkseq_lk);
        bdb_state->blkseq_lk = NULL;
    }
    if (bdb_state->blkseq[0]) {
        free(bdb_state->blkseq[0]);
        bdb_state->blkseq[0] = NULL;
    }
    if (bdb_state->blkseq[1]) {
        free(bdb_state->blkseq[1]);
        bdb_state->blkseq[1] = NULL;
    }
    if (bdb_state->blkseq_last_lsn[0]) {
        free(bdb_state->blkseq_last_lsn[0]);
        bdb_state->blkseq_last_lsn[0] = NULL;
    }
    if (bdb_state->blkseq_last_lsn[1]) {
        free(bdb_state->blkseq_last_lsn[1]);
        bdb_state->blkseq_last_lsn[1] = NULL;
    }
}

int bdb_create_private_blkseq(bdb_state_type *bdb_state)
{
    DB_ENV *env;
    DB *db[2];
    int rc;

    bdb_state->blkseq_env =
        malloc(bdb_state->attr->private_blkseq_stripes * sizeof(DB_ENV *));
    bdb_state->blkseq_lk = malloc(bdb_state->attr->private_blkseq_stripes *
                                  sizeof(pthread_mutex_t));
    bdb_state->blkseq[0] =
        malloc(bdb_state->attr->private_blkseq_stripes * sizeof(DB *));
    bdb_state->blkseq[1] =
        malloc(bdb_state->attr->private_blkseq_stripes * sizeof(DB *));
    bdb_state->blkseq_last_lsn[0] =
        malloc(bdb_state->attr->private_blkseq_stripes * sizeof(DB_LSN));
    bdb_state->blkseq_last_lsn[1] =
        malloc(bdb_state->attr->private_blkseq_stripes * sizeof(DB_LSN));

    listc_init(&bdb_state->blkseq_log_list[0],
               offsetof(struct seen_blkseq, lnk));
    listc_init(&bdb_state->blkseq_log_list[1],
               offsetof(struct seen_blkseq, lnk));

    for (int stripe = 0; stripe < bdb_state->attr->private_blkseq_stripes;
         stripe++) {
        rc = db_env_create(&env, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "db_env_create rc %d\n", rc);
            return rc;
        }

        bdb_state->blkseq_env[stripe] = env;
        env->set_errfile(env, stderr);

        rc = env->set_cachesize(env, 0, bdb_state->attr->private_blkseq_cachesz,
                                1);
        if (rc) {
            logmsg(LOGMSG_ERROR, "blkseq->set_cachesize rc %d\n", rc);
            return rc;
        }
        /* no locking around blkseq - we'll lock around them ourselves */
        rc = env->open(env, bdb_state->tmpdir,
                       DB_PRIVATE | DB_INIT_MPOOL | DB_CREATE, 0666);
        if (rc) {
            logmsg(LOGMSG_ERROR, "blkseq->open rc %d\n", rc);
            return rc;
        }
        rc = pthread_mutex_init(&bdb_state->blkseq_lk[stripe], NULL);
        if (rc) {
            logmsg(LOGMSG_ERROR, "pthread_mutex_init init rc %d %s\n", rc,
                    strerror(rc));
            return rc;
        }

        for (int i = 0; i < 2; i++) {
            bdb_state->blkseq[i][stripe] = create_blkseq(bdb_state, stripe, i);
            if (bdb_state->blkseq[i][stripe] == NULL)
                return -1;
            bzero(&bdb_state->blkseq_last_lsn[i][stripe], sizeof(DB_LSN));
        }
    }
    bdb_state->blkseq_last_roll_time = comdb2_time_epoch();

    return 0;
}

static uint8_t get_stripe(bdb_state_type *bdb_state, uint8_t *bytes, int len)
{
    uint8_t stripe = 0;

    /* TODO: */
    for (int i = 0; i < len; i++) {
        stripe ^= *bytes;
        bytes++;
    }
    return stripe % bdb_state->attr->private_blkseq_stripes;
}

/* recovery callback from berkeley (through bdb_apprec) */
int bdb_blkseq_recover(DB_ENV *dbenv, u_int32_t rectype, llog_blkseq_args *args,
                       DB_LSN *lsn, db_recops op)
{
    int rc = 0;
    DBT key = {0}, data = {0};
    bdb_state_type *bdb_state;
    int now;
    uint8_t stripe;

    bdb_state = dbenv->app_private;

    // printf("at "PR_LSN", blkseq\n", PARM_LSNP(lsn));
    if (op == DB_TXN_PRINT) {
        printf("[%u][%u] CUSTOM: add_blkseq: rec: %u txnid %x"
               " prevlsn[" PR_LSN "]\n",
               lsn->file, lsn->offset, rectype, args->txnid->txnid,
               PARM_LSN(args->prev_lsn));
        printf("\ttime:     %" PRId64 "\n", args->time);
        printf("\tkey:      ");
        hexdumpdbt(&args->key);
        printf("\n");
        printf("\tdata:     ");
        hexdumpdbt(&args->data);
        printf("\n");
        printf("\n");
        *lsn = args->prev_lsn;
        return (0);
    }

    if (op == DB_TXN_APPLY || op == DB_TXN_FORWARD_ROLL) {
        stripe =
            get_stripe(bdb_state, (uint8_t *)args->key.data, args->key.size);
        int *p = (int *)args->key.data;

        now = comdb2_time_epoch();

        // printf("%d seconds old %x %x %x ", now - args->time, p[0], p[1],
        // p[2]);
        pthread_mutex_lock(&bdb_state->blkseq_lk[stripe]);
        rc = bdb_state->blkseq[0][stripe]->put(bdb_state->blkseq[0][stripe],
                NULL, &args->key,
                &args->data, DB_NOOVERWRITE);
        if (rc == 0) {
            bdb_state->blkseq_last_lsn[0][stripe] = *lsn;
            rc = bdb_blkseq_update_lsn_locked(bdb_state, args->time, *lsn,
                    stripe);
        }
        pthread_mutex_unlock(&bdb_state->blkseq_lk[stripe]);
        if (rc == DB_KEYEXIST)
            rc = 0;
        if (rc)
            return rc;
            // printf("applied ");
    }
    /* turns out we do need to back these out after all, since otherwise parent
     * transaction aborts look like replays, and we silently drop updates. */
    else if (op == DB_TXN_BACKWARD_ROLL || op == DB_TXN_ABORT) {
        stripe =
            get_stripe(bdb_state, (uint8_t *)args->key.data, args->key.size);
        pthread_mutex_lock(&bdb_state->blkseq_lk[stripe]);
        rc = bdb_state->blkseq[0][stripe]->del(bdb_state->blkseq[0][stripe],
                                               NULL, &args->key, 0);
        if (rc == 0 || rc == DB_NOTFOUND) {
            rc = bdb_state->blkseq[1][stripe]->del(bdb_state->blkseq[1][stripe],
                                                   NULL, &args->key, 0);
            if (rc == DB_NOTFOUND)
                rc = 0;
        }
        pthread_mutex_unlock(&bdb_state->blkseq_lk[stripe]);
    }
    // printf("\n");
    *lsn = args->prev_lsn;

    return rc;
}

static void dump_logseq(bdb_state_type *bdb_state, struct seen_blkseq *logseq)
{
    int now = comdb2_time_epoch();
    logmsg(LOGMSG_USER, "log %d timestamp %d age %d ", logseq->logfile, logseq->timestamp,
           now - logseq->timestamp);
    if ((now - logseq->timestamp) > bdb_state->attr->private_blkseq_maxage)
        logmsg(LOGMSG_USER, "(deleteable)");
    logmsg(LOGMSG_USER, "\n");
}

static int bdb_blkseq_update_lsn_locked(bdb_state_type *bdb_state,
                                        int timestamp, DB_LSN lsn, int stripe)
{
    struct seen_blkseq *logseq;

    /* In recovery, we're running backwards, we add to the head of the list.
     * Otherwise, we add to the
     * tail of the list.  Rollback can be treated like recovery, but it really
     * doesn't matter. */
    logseq = bdb_state->blkseq_log_list[stripe].bot;

    if (logseq == NULL || logseq->logfile != lsn.file) {
        logseq = malloc(sizeof(struct seen_blkseq));
        if (logseq == NULL)
            return ENOMEM;
        logseq->logfile = lsn.file;
        logseq->timestamp = timestamp;

        listc_abl(&bdb_state->blkseq_log_list[stripe], logseq);
        // printf("%s: stripe %d new log %d timestamp %d age %d\n", __func__,
        // stripe, lsn.file, timestamp, time(NULL) - timestamp);
    }

    return 0;
}

int bdb_blkseq_find(bdb_state_type *bdb_state, tran_type *tran, void *key,
                    int klen, void **dtaout, int *lenout)
{
    DBT dkey = {0}, ddata = {0};
    int rc;
    uint8_t stripe;
    ddata.flags = DB_DBT_REALLOC;
    if (!bdb_state->attr->private_blkseq_enabled)
        return IX_EMPTY;
    stripe = get_stripe(bdb_state, (uint8_t *)key, klen);
    pthread_mutex_lock(&bdb_state->blkseq_lk[stripe]);
    dkey.data = key;
    dkey.size = klen;
    for (int i = 0; i < 2; i++) {
        rc = bdb_state->blkseq[i][stripe]->get(bdb_state->blkseq[i][stripe],
                                               NULL, &dkey, &ddata, 0);
        if (rc == 0) {
            if (dtaout)
                *dtaout = ddata.data;
            if (lenout)
                *lenout = ddata.size;
            pthread_mutex_unlock(&bdb_state->blkseq_lk[stripe]);
            return IX_FND;
        } else if (rc != DB_NOTFOUND) {
            pthread_mutex_unlock(&bdb_state->blkseq_lk[stripe]);
            return IX_ACCESS;
        }
    }
    pthread_mutex_unlock(&bdb_state->blkseq_lk[stripe]);
    return IX_NOTFND;
}

int bdb_blkseq_insert(bdb_state_type *bdb_state, tran_type *tran, void *key,
                      int klen, void *data, int datalen, void **dtaout,
                      int *lenout)
{
    DBT dkey = {0}, ddata = {0};
    DB_LSN lsn;
    int now;
    // int *k;
    int rc;
    uint8_t stripe;

    ddata.flags = DB_DBT_REALLOC;

    // k = (int*) key;
    // printf("inserting %x %x %x\n", k[0], k[1], k[2]);
    stripe = get_stripe(bdb_state, (uint8_t *)key, klen);

    pthread_mutex_lock(&bdb_state->blkseq_lk[stripe]);
    dkey.data = key;
    dkey.size = klen;
    ddata.data = data;
    ddata.size = datalen;

    now = comdb2_time_epoch();

    for (int i = 0; i < 2; i++) {
        rc = bdb_state->blkseq[i][stripe]->get(bdb_state->blkseq[i][stripe],
                                               NULL, &dkey, &ddata, 0);
        if (rc == 0) {
            if (dtaout)
                *dtaout = ddata.data;
            if (lenout)
                *lenout = ddata.size;
            pthread_mutex_unlock(&bdb_state->blkseq_lk[stripe]);
            return IX_DUP;
        } else if (rc != DB_NOTFOUND) {
            logmsg(LOGMSG_ERROR, "bdb_blkseq_insert stripe %d num %d rc %d\n",
                    stripe, i, rc);
            pthread_mutex_unlock(&bdb_state->blkseq_lk[stripe]);
            return BDBERR_MISC; /* change this??? IX_DUP == 2 == BDBERR_MISC */
        }
    }

    /* not found in either tree - put it in the first */

    rc = bdb_state->blkseq[0][stripe]->put(bdb_state->blkseq[0][stripe], NULL,
                                           &dkey, &ddata, DB_NOOVERWRITE);
    if (rc) {
        logmsg(LOGMSG_ERROR, "blkseq put stripe %d error %d\n", stripe, rc);
        pthread_mutex_unlock(&bdb_state->blkseq_lk[stripe]);
        return BDBERR_MISC;
    }

    /* succeded in updating local table, log the update if transactional
     * (recovery isn't) */
    if (tran) {
        rc = llog_blkseq_log(bdb_state->dbenv, tran->tid, &lsn, 0, now, &dkey,
                             &ddata);

        /* Don't bother with these during recovery since we'll run
         * bdb_blkseq_recover to
         * take care of that shortly. */
        if (rc == 0) {
            rc = bdb_blkseq_update_lsn_locked(bdb_state, now, lsn, stripe);
            bdb_state->blkseq_last_lsn[0][stripe] = lsn;
        }
    }

    pthread_mutex_unlock(&bdb_state->blkseq_lk[stripe]);
    return rc;
}

/* Every N seconds, we shift all the trees down and delete the oldest */
int bdb_blkseq_clean(bdb_state_type *bdb_state, uint8_t stripe)
{
    time_t now, last;
    DB *to_be_deleted;
    DB *newdb;
    char *oldname = NULL;
    int rc = 0;
    DB_ENV *env;
    int start, end;

    start = comdb2_time_epochms();
    now = comdb2_time_epoch();

    pthread_mutex_lock(&bdb_state->blkseq_lk[stripe]);

    last = bdb_state->blkseq_last_roll_time;

    /* Not yet time?  Do nothing. */
    if ((now - last) < bdb_state->attr->private_blkseq_maxage)
        goto done;

    /* Is anything here still referenced?  Do nothing. */
    if (bdb_state->blkseq_last_lsn[1][stripe].file) {
        DB_LSN lsn = {0};
        DBT logdta = {0};
        DB_LOGC *logc = NULL;

        if ((rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0)) != 0) {
            logmsg(LOGMSG_ERROR, "%s: couldn't create log-cursor, rc=%d\n",
                    __func__, rc);
            rc = BDBERR_MISC;
            goto done;
        }

        logdta.flags = DB_DBT_MALLOC;
        rc = logc->get(logc, &lsn, &logdta, DB_FIRST);
        logc->close(logc, 0);

        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: couldn't retrieve first log-record, rc=%d\n",
                    __func__, rc);
            rc = BDBERR_MISC;
            goto done;
        }

        if (logdta.data)
            free(logdta.data);

        if (lsn.file <= bdb_state->blkseq_last_lsn[1][stripe].file)
            goto done;
    }

    /* create a new db first */
    newdb = create_blkseq(bdb_state, stripe, 0);
    if (newdb == NULL) {
        rc = BDBERR_MISC;
        goto done;
    }

    /* swap pointers */
    to_be_deleted = bdb_state->blkseq[1][stripe];
    bdb_state->blkseq[1][stripe] = bdb_state->blkseq[0][stripe];
    bdb_state->blkseq[0][stripe] = newdb;
    bdb_state->blkseq_last_lsn[1][stripe] = bdb_state->blkseq_last_lsn[0][stripe];

    bdb_state->blkseq_last_roll_time = now;

    /* Clean up the old blkseq file. Get its name, close it, delete it. */
    rc =
        to_be_deleted->get_dbname(to_be_deleted, (const char **)&oldname, NULL);
    if (rc) {
        /* warn, but proceed - we leak a file, which will be cleaned up when
         * the database next turns. */
        logmsg(LOGMSG_ERROR, "can't find filename of blkseq table\n");
    } else
        oldname = strdup(oldname);

    to_be_deleted->close(to_be_deleted, DB_NOSYNC);

    if (oldname) {
        DB *db;
        env = bdb_state->blkseq_env[stripe];

        rc = db_create(&db, env, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "blkseq create rc %d\n", rc);
            rc = BDBERR_MISC;
            goto done;
        }

        rc = db->remove(db, oldname, NULL, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "db->remove %s rc %d\n", oldname, rc);
            rc = BDBERR_MISC;
            goto done;
        }
    }
    if (bdb_state->attr->private_blkseq_close_warn_time) {
        end = comdb2_time_epochms();
        if ((end - start) > bdb_state->attr->private_blkseq_close_warn_time) {
            logmsg(LOGMSG_WARN, "blkseq close took %dms\n", end - start);
        }
    }

done:
    pthread_mutex_unlock(&bdb_state->blkseq_lk[stripe]);
    if (oldname)
        free(oldname);

    return rc;
}

int bdb_blkseq_dumpall(bdb_state_type *bdb_state, uint8_t stripe)
{
    DBC *dbc = NULL;
    DBT dkey = {0}, ddata = {0};
    int now;
    int rc;

    dkey.flags = ddata.flags = DB_DBT_REALLOC;
    pthread_mutex_lock(&bdb_state->blkseq_lk[stripe]);

    for (int i = 0; i < 2; i++) {
        rc = bdb_state->blkseq[i][stripe]->cursor(bdb_state->blkseq[i][stripe],
                                                  NULL, &dbc, 0);
        logmsg(LOGMSG_USER, "stripe %d idx %d last_lsn=[%d][%d]\n", stripe, i, 
                bdb_state->blkseq_last_lsn[i][stripe].file,
                bdb_state->blkseq_last_lsn[i][stripe].offset);
        if (rc)
            goto done;
        rc = dbc->c_get(dbc, &dkey, &ddata, DB_FIRST);
        now = comdb2_time_epoch();
        while (rc == 0) {
            int *k;
            k = (int *)dkey.data;
            if (ddata.size < sizeof(int)) {
                logmsg(LOGMSG_ERROR, "%x %x %x invalid sz %d\n", k[0], k[1], k[2],
                       ddata.size);
            } else {
                int timestamp;
                int age;
                memcpy(&timestamp, (uint8_t *)ddata.data + (ddata.size - 4), 4);
                age = now - timestamp;
                // this is a cnonce 
                if (dkey.size > 12) {
                    char *p = alloca(dkey.size + 1);
                    memcpy(p, dkey.data, dkey.size);
                    p[dkey.size] = '\0';
                    logmsg(LOGMSG_USER, "stripe %d idx %d : %s sz %d age %d\n", stripe, i, 
                            p, ddata.size, age);
                }
                else {
                    logmsg(LOGMSG_USER, "stripe %d idx %d : %x %x %x sz %d age %d\n", stripe,
                            i, k[0], k[1], k[2], ddata.size, age);
                }
            }

            rc = dbc->c_get(dbc, &dkey, &ddata, DB_NEXT);
        }
        if (rc) {
            if (rc != DB_NOTFOUND)
                logmsg(LOGMSG_ERROR, "fstblk c_get rc %d\n", rc);
            else
                rc = 0;
        }
        dbc->c_close(dbc);
        dbc = NULL;
    }

done:
    pthread_mutex_unlock(&bdb_state->blkseq_lk[stripe]);
    if (dkey.data)
        free(dkey.data);
    if (ddata.data)
        free(ddata.data);
    if (dbc)
        dbc->c_close(dbc);

    return rc;
}

/* Note: this code runs for recovery on startup only - replicated recovery
 * processes
 * blkseq events along with everything else.  The only reason we need this
 * routine is
 * that blkseqs may live earlier in the log stream then the recovery point.  I'd
 * rather
 * not move recovery - so this is an extra pass at startup. */
int bdb_recover_blkseq(bdb_state_type *bdb_state)
{
    DB_LOGC *logc = NULL;
    int rc;
    DBT logdta = {0};
    DB_LSN lsn, last_lsn;
    llog_blkseq_args *blkseq = NULL;
    int now = comdb2_time_epoch();
    int nblkseq = 0;
    int ndupes = 0;
    int last_log_filenum;
    int stripe;
    int oldest_blkseq;
    struct seen_blkseq *logseq;

    /* Walk the log file list backwards, stop when we get to blkseqs older than
     * we care about. */
    logdta.flags = DB_DBT_REALLOC;
    oldest_blkseq = time(NULL);

    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0);
    if (rc)
        goto err;

    rc = logc->get(logc, &lsn, &logdta, DB_LAST);
    if (rc == 0)
        last_lsn = lsn;
    last_log_filenum = lsn.file;
    while (rc == 0) {
        u_int32_t rectype;
        if (logdta.size > sizeof(u_int32_t)) {
            LOGCOPY_32(&rectype, logdta.data);
            if (rectype == DB_llog_blkseq) {
                rc = llog_blkseq_read(bdb_state->dbenv, logdta.data, &blkseq);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "at " PR_LSN " llog_blkseq_read rc %d\n",
                            PARM_LSN(lsn), rc);
                    goto err;
                }
                int *k;
                k = (int *)blkseq->key.data;
                if ((now - blkseq->time) >
                    bdb_state->attr->private_blkseq_maxage) {
                    logmsg(LOGMSG_INFO,
                           "Stopping at " PR_LSN ", blkseq age %ld > max %d\n",
                           PARM_LSN(lsn), now - blkseq->time,
                           bdb_state->attr->private_blkseq_maxage);
                    break;
                }

                /* TODO: these should always run back in time, so the if may be
                 * unnecessary? Right? */
                if (blkseq->time < oldest_blkseq)
                    oldest_blkseq = blkseq->time;

                stripe =
                    get_stripe(bdb_state, blkseq->key.data, blkseq->key.size);
                if (lsn.file != last_log_filenum && oldest_blkseq != INT_MAX) {
                    /* if we just switched a file, remember the oldest blkseq we
                     * saw in the current file */
                    for (int i = 0; i < 2; i++) {
                        logseq = malloc(sizeof(struct seen_blkseq));
                        logseq->logfile = lsn.file;
                        logseq->timestamp = oldest_blkseq;

                        /* we are running backwards here, so add to the start of
                         * the list */
                        listc_atl(&bdb_state->blkseq_log_list[i], logseq);
                    }

                    oldest_blkseq = INT_MAX;
                    last_log_filenum = lsn.file;
                }

                rc = bdb_blkseq_insert(bdb_state, NULL, blkseq->key.data,
                                       blkseq->key.size, blkseq->data.data,
                                       blkseq->data.size, NULL, NULL);
                if (rc == IX_DUP)
                    ndupes++;
                else if (rc) {
                    logmsg(LOGMSG_ERROR, "at " PR_LSN " bdb_blkseq_insert %x %x %x rc %d\n",
                           PARM_LSN(lsn), k[0], k[1], k[2], rc);
                    goto err;
                } else
                    nblkseq++;
                free(blkseq);
                blkseq = NULL;
            }
        }

        rc = logc->get(logc, &lsn, &logdta, DB_PREV);
    }

    /* If we only had one log and didn't hit the "did the log file change"
     * code above, add the information about the only log we saw - it'll
     * eventually be not the only log, and we need information about its
     * blkseq ages */
    if (nblkseq > 0) {
        for (int i = 0; i < 2; i++) {
            if (bdb_state->blkseq_log_list[i].count == 0) {
                logseq = malloc(sizeof(struct seen_blkseq));
                logseq->logfile = lsn.file;
                logseq->timestamp = oldest_blkseq;
                listc_atl(&bdb_state->blkseq_log_list[i], logseq);
            }
        }
    }

    if (rc == DB_NOTFOUND)
        rc = 0;

    if (rc == 0) {
        logmsg(LOGMSG_INFO, "blkseq recovery " PR_LSN " <- " PR_LSN
               ", %d blkseqs, %d during recovery\n",
               PARM_LSN(lsn), PARM_LSN(last_lsn), nblkseq, ndupes);
    }

err:
    if (logc)
        logc->close(logc, 0);
    if (logdta.data)
        free(logdta.data);
    if (blkseq)
        free(blkseq);
    return rc;
}

int bdb_blkseq_dumplogs(bdb_state_type *bdb_state)
{
    struct seen_blkseq *logseq;
    int now = comdb2_time_epoch();

    for (int stripe = 0; stripe < bdb_state->attr->private_blkseq_stripes;
         stripe++) {
        pthread_mutex_lock(&bdb_state->blkseq_lk[stripe]);
        LISTC_FOR_EACH(&bdb_state->blkseq_log_list[stripe], logseq, lnk)
        {
            dump_logseq(bdb_state, logseq);
        }
        pthread_mutex_unlock(&bdb_state->blkseq_lk[stripe]);
    }
    return 0;
}

int bdb_blkseq_can_delete_log(bdb_state_type *bdb_state, int lognum)
{
    struct seen_blkseq *logseq, *logseqtmp;
    int num_ok = 0;
    int found = 0;
    int now = comdb2_time_epoch();

    for (int stripe = 0; stripe < bdb_state->attr->private_blkseq_stripes;
         stripe++) {
        pthread_mutex_lock(&bdb_state->blkseq_lk[stripe]);
        LISTC_FOR_EACH(&bdb_state->blkseq_log_list[stripe], logseq, lnk)
        {
            if (logseq->logfile == lognum) {
                found++;
                if ((now - logseq->timestamp) >
                    bdb_state->attr->private_blkseq_maxage)
                    num_ok++;
            }
        }
        pthread_mutex_unlock(&bdb_state->blkseq_lk[stripe]);
    }

    /* Get rid of these entries if we're about to tell the caller that we
     * can delete logs.  If they fail to delete logs, the next call to
     * bdb_blkseq_can_delete_log will still succeed (because no
     * entries will be found) */
    if (found == num_ok) {
        for (int stripe = 0; stripe < bdb_state->attr->private_blkseq_stripes;
             stripe++) {
            pthread_mutex_lock(&bdb_state->blkseq_lk[stripe]);
            LISTC_FOR_EACH_SAFE(&bdb_state->blkseq_log_list[stripe], logseq,
                                logseqtmp, lnk)
            {
                if (logseq->logfile == lognum) {
                    listc_rfl(&bdb_state->blkseq_log_list[stripe], logseq);
                    free(logseq);
                }
            }
            pthread_mutex_unlock(&bdb_state->blkseq_lk[stripe]);
        }
        return 1;
    }
    return 0;
}
