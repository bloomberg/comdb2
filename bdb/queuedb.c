#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <strings.h>
#include <stddef.h>
#include <sys/stat.h>

#include <pthread.h>
#include <bb_stdint.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <inttypes.h>
#include <flibc.h>
#include "endian_core.h"
#include <build/db.h>
#include "bdb_cursor.h"
#include "bdb_int.h"
#include "locks.h"
#include "str0.h"

#include "bdb_queue.h"
#include "bdb_queuedb.h"
#include "plbitlib.h"
#include "logmsg.h"
#include "cron.h"
#include "schemachange.h"
#include "comdb2.h"

extern int gbl_queuedb_file_threshold;
extern int gbl_queuedb_file_interval;

/* Another implementation of queues.  Don't really "trust" berkeley queues.
 * We've had some issues with
 * them that have proven difficult to reproduce (unreclaimed extents, queue
 * head/tail mismatching between
 * nodes, to name a couple).  So just use a plain old btree, which we know and
 * love. Implements the same API
 * as bdb/queue.c. */

/* These are the public APIs exposed by bdb/queue.c - implement the same
 * interface. */

static cron_sched_t *gbl_queuedb_cron = NULL;

/* TODO: this is in db-land, not bdb.... */
#define MAXCONSUMERS 32

#define BDB_QUEUEDB_GET_DBP_ZERO(a)  ((a)->dbp_data[0][0])
#define BDB_QUEUEDB_GET_DBP_ONE(a)   ((a)->dbp_data[1][0])

struct bdb_queue_priv {
    uint64_t genid;
    struct bdb_queue_stats stats;
};

struct queuedb_key {
    int consumer;
    uint64_t genid;
};

enum { QUEUEDB_KEY_LEN = 4 + 8 };

int gbl_debug_queuedb = 0;

static uint8_t *queuedb_key_get(struct queuedb_key *p_queuedb_key,
                                uint8_t *p_buf, uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || (p_buf_end - p_buf) < QUEUEDB_KEY_LEN)
        return NULL;
    p_buf =
        (uint8_t *)buf_get(&p_queuedb_key->consumer,
                           sizeof(p_queuedb_key->consumer), p_buf, p_buf_end);
    p_buf = (uint8_t *)buf_no_net_get(
        &p_queuedb_key->genid, sizeof(p_queuedb_key->genid), p_buf, p_buf_end);
    return p_buf;
}

static uint8_t *queuedb_key_put(struct queuedb_key *p_queuedb_key,
                                uint8_t *p_buf, uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || (p_buf_end - p_buf) < QUEUEDB_KEY_LEN)
        return NULL;
    p_buf = buf_put(&p_queuedb_key->consumer, sizeof(p_queuedb_key->consumer),
                    p_buf, p_buf_end);
    p_buf = buf_no_net_put(&p_queuedb_key->genid, sizeof(p_queuedb_key->genid),
                           p_buf, p_buf_end);
    return p_buf;
}

static int bdb_queuedb_is_db_empty(DB *db, tran_type *tran)
{
    int rc;
    DBC *dbcp = NULL;
    DBT dbt_key = {0}, dbt_data = {0};

    rc = db->cursor(db, tran ? tran->tid : NULL, &dbcp, 0);
    if (rc != 0) {
        rc = 0; /* TODO: Safe failure choice here is non-empty? */
        goto done;
    }
    dbt_data.flags = dbt_key.flags = DB_DBT_MALLOC;
    rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_FIRST);
    if (rc == DB_NOTFOUND) {
        rc = 1; /* NOTE: Confirmed empty. */
    } else if (rc) {
        logmsg(LOGMSG_ERROR, "%s: c_get berk rc %d\n", __func__, rc);
        rc = 0; /* TODO: Safe failure choice here is non-empty? */
    } else {
        rc = 0; /* NOTE: Confirmed non-empty. */
    }
done:
    if (dbcp) {
        int crc = dbcp->c_close(dbcp);
        if (crc) {
            logmsg(LOGMSG_ERROR, "%s: c_close berk rc %d\n", __func__, crc);
        }
    }
    if (dbt_key.data)
        free(dbt_key.data);
    if (dbt_data.data)
        free(dbt_data.data);
    return rc;
}

static int bdb_queuedb_is_db_full(DB *db)
{
    if (gbl_queuedb_file_threshold <= 0) return 0; /* never full? */
    char new[PATH_MAX];
    struct stat sb;
    if (stat(bdb_trans(db->fname, new), &sb) != 0) {
        return 0; /* cannot detect, assume no? */
    }
    return ((sb.st_size / 1048576) >= gbl_queuedb_file_threshold);
}

static void *queuedb_cron_event(struct cron_event *evt, struct errstat *err)
{
    if (db_is_stopped()) return NULL;
    struct dbenv *dbenv = NULL;
    if (evt != NULL) dbenv = evt->arg4;
    if (gbl_queuedb_file_interval > 0) {
        int tm = comdb2_time_epoch() + (gbl_queuedb_file_interval / 1000);
        void *p = cron_add_event(gbl_queuedb_cron, NULL, tm,
                                 (FCRON)queuedb_cron_event, NULL,
                                 NULL, NULL, dbenv, NULL, err, NULL);
        if (p == NULL) {
            logmsg(LOGMSG_ERROR, "Failed to schedule next queuedb event. "
                            "rc = %d, errstr = %s\n",
                    err->errval, err->errstr);
        }
    }
    if (gbl_queuedb_file_threshold <= 0) return NULL;
    if (dbenv == NULL) return NULL;
    bdb_state_type *bdb_state = dbenv->bdb_env;
    bdb_thread_event(bdb_state, BDBTHR_EVENT_START_RDONLY);
    BDB_READLOCK("queuedb cron thread");
    if (dbenv->master != gbl_mynode) {
        BDB_RELLOCK();
        bdb_thread_event(bdb_state, BDBTHR_EVENT_DONE_RDONLY);
        return NULL; 
    }
    rdlock_schema_lk();
    for (int i = 0; i < dbenv->num_qdbs; i++) {
        dbtable *tbl = dbenv->qdbs[i];
        if (tbl == NULL) continue;
        bdb_state_type *tbl_bdb_state = tbl->handle;
        if (tbl_bdb_state == NULL) continue;
        DB *db1 = BDB_QUEUEDB_GET_DBP_ZERO(tbl_bdb_state);
        DB *db2 = BDB_QUEUEDB_GET_DBP_ONE(tbl_bdb_state);
        struct schema_change_type *sc = NULL;
        int rc;
        if (db2 != NULL) {
            if (bdb_queuedb_is_db_empty(db1, NULL)) {
                logmsg(LOGMSG_DEBUG,
                    "%s: queuedb '%s' has two files and old file is empty, "
                    "attempting to delete it...\n", __func__, tbl_bdb_state->name);
                sc = new_schemachange_type();
                if (sc == NULL) {
                    continue;
                }
                strncpy0(
                    sc->tablename, tbl_bdb_state->name, sizeof(sc->tablename)
                );
                sc->type = DBTYPE_QUEUEDB;
                sc->del_qdb_file = 1;
                sc->nothrevent = 1;
                sc->finalize = 1;
                sc->already_locked = 1;
                sc->db = tbl;
                rc = start_schema_change(sc);
                if ((rc != SC_OK) && (rc != SC_ASYNC)) {
                    logmsg(LOGMSG_ERROR,
                           "%s: failed to start schema change to delete "
                           "old file for queuedb '%s'\n",
                           __func__, sc->tablename);
                    free_schema_change_type(sc);
                }
            } else {
                logmsg(LOGMSG_DEBUG,
                    "%s: queuedb '%s' has two files and old file is not "
                    "empty, doing nothing...\n", __func__, tbl_bdb_state->name);
            }
        } else if (bdb_queuedb_is_db_full(db1)) {
            logmsg(LOGMSG_DEBUG,
                "%s: queuedb '%s' has one file and old file is full, "
                "attempting to add new file...\n", __func__, tbl_bdb_state->name);
            sc = new_schemachange_type();
            if (sc == NULL) {
                continue;
            }
            strncpy0(
                sc->tablename, tbl_bdb_state->name, sizeof(sc->tablename)
            );
            sc->type = DBTYPE_QUEUEDB;
            sc->add_qdb_file = 1;
            sc->qdb_file_ver = flibc_htonll(bdb_get_cmp_context(tbl_bdb_state));
            sc->nothrevent = 1;
            sc->finalize = 1;
            sc->already_locked = 1;
            sc->db = tbl;
            rc = start_schema_change(sc);
            if ((rc != SC_OK) && (rc != SC_ASYNC)) {
                logmsg(LOGMSG_ERROR,
                       "%s: failed to start schema change to add "
                       "new file for queuedb '%s'\n",
                           __func__, sc->tablename);
                free_schema_change_type(sc);
            }
        } else {
            logmsg(LOGMSG_DEBUG,
                "%s: queuedb '%s' has one file and old file is not "
                "full, doing nothing...\n", __func__, tbl_bdb_state->name);
        }
    }
    unlock_schema_lk();
    BDB_RELLOCK();
    bdb_thread_event(bdb_state, BDBTHR_EVENT_DONE_RDONLY);
    return NULL;
}

static void *queuedb_cron_kickoff(struct cron_event *evt, struct errstat *err)
{
    if (db_is_stopped()) return NULL;
    if (gbl_queuedb_file_interval > 0) {
        logmsg(LOGMSG_INFO, "Starting queuedb cron job. "
                        "Will check queuedb usage every %d seconds.\n",
                gbl_queuedb_file_interval / 1000);
        return queuedb_cron_event(evt, err);
    }
    return NULL;
}

static char *queuedb_cron_describe(sched_if_t *impl)
{
    return strdup("QueueDB cron scheduler");
}

static char *queuedb_cron_event_describe(sched_if_t *impl, cron_event_t *event)
{
    const char *name;
    if (event->func == (FCRON)queuedb_cron_event)
        name = "QueueDB usage update";
    else if (event->func == (FCRON)queuedb_cron_kickoff)
        name = "QueueDB usage kickoff";
    else
        name = "Unknown";

    return strdup(name);
}

int bdb_queuedb_create_cron(void *arg)
{
    struct errstat xerr = {0};

    if (gbl_queuedb_file_interval > 0) {
        if (!gbl_queuedb_cron) {
            sched_if_t impl = {0};
            time_cron_create(
                &impl, queuedb_cron_describe, queuedb_cron_event_describe
            );
            gbl_queuedb_cron = cron_add_event(NULL, "QueueDB Job Scheduler",
                                      INT_MIN, (FCRON)queuedb_cron_kickoff,
                                      NULL, NULL, NULL, arg, NULL, &xerr,
                                      &impl);
        } else {
            gbl_queuedb_cron = cron_add_event(gbl_queuedb_cron, NULL, INT_MIN,
                                      (FCRON)queuedb_cron_kickoff, NULL, NULL,
                                      NULL, arg, NULL, &xerr, NULL);
        }
        if (gbl_queuedb_cron == NULL) {
            logmsg(LOGMSG_ERROR, "Failed to schedule queuedb cron job. "
                            "rc = %d, errstr = %s\n",
                    xerr.errval, xerr.errstr);
        }
    }
    return xerr.errval;
}

void bdb_queuedb_init_priv(bdb_state_type *bdb_state)
{
    if (gbl_debug_queuedb)
        logmsg(LOGMSG_USER, ">>> bdb_queuedb_init_priv %s\n", bdb_state->name);
    bdb_state->qpriv = calloc(1, sizeof(struct bdb_queue_priv));
    /* read max, use? use genids, with guarantee they'll never decrement? */
    // bdb_state->qstate->genid = 0; /* CALLOC'd */
}

/* btree, so rely on our usual page size suggester */
int bdb_queuedb_best_pagesize(int avg_item_sz)
{
    return calc_pagesize(avg_item_sz);
}

/* add to queue */
int bdb_queuedb_add(bdb_state_type *bdb_state, tran_type *tran, const void *dta,
                    size_t dtalen, int *bdberr, unsigned long long *out_genid)
{
    DB *db = BDB_QUEUEDB_GET_DBP_ONE(bdb_state);
    if (db == NULL) db = BDB_QUEUEDB_GET_DBP_ZERO(bdb_state);
    struct queuedb_key k;
    int rc;
    DBT dbt_key = {0}, dbt_data = {0};
    uint8_t *p_buf, *p_buf_end;
    struct bdb_queue_found qfnd;
    void *databuf = NULL;
    struct bdb_queue_priv *qstate = bdb_state->qpriv;

    if (gbl_debug_queuedb)
        logmsg(LOGMSG_USER, ">>> bdb_queuedb_add %s\n", bdb_state->name);

    qstate = (struct bdb_queue_priv *)bdb_state->qpriv;
    databuf = malloc(dtalen + sizeof(struct bdb_queue_found));
    qfnd.genid = get_genid(bdb_state, 0);
    qfnd.data_len = dtalen;
    qfnd.data_offset = sizeof(struct bdb_queue_found);
    qfnd.trans.tid = tran->tid->txnid;
    if (tran->trigger_epoch) {
        qfnd.epoch = tran->trigger_epoch;
    } else {
        qfnd.epoch = tran->trigger_epoch = comdb2_time_epoch();
    }
    p_buf = databuf;
    p_buf_end = p_buf + dtalen + sizeof(struct bdb_queue_found);
    p_buf = queue_found_put(&qfnd, p_buf, p_buf_end);
    if (p_buf == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed to encode queue header for %s\n", __func__,
                bdb_state->name);
        *bdberr = BDBERR_MISC;
        rc = -1;
        goto done;
    }
    p_buf = buf_no_net_put(dta, dtalen, p_buf, p_buf_end);
    if (p_buf == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed to encode queue entry for %s\n", __func__,
                bdb_state->name);
        *bdberr = BDBERR_MISC;
        rc = -1;
        goto done;
    }

    *bdberr = BDBERR_NOERROR;
    for (int i = 0; i < MAXCONSUMERS; i++) {
        if (btst(&bdb_state->active_consumers, i)) {
            uint8_t key[QUEUEDB_KEY_LEN];
            uint8_t *p_buf, *p_buf_end;
            p_buf = key;
            p_buf_end = key + sizeof(key);
            k.consumer = i;
            k.genid = qfnd.genid;
            p_buf = queuedb_key_put(&k, p_buf, p_buf_end);
            if (p_buf == NULL) {
                logmsg(LOGMSG_ERROR, "%s: failed to encode key for %s consumer %d\n",
                        __func__, bdb_state->name, i);
                *bdberr = BDBERR_MISC;
                rc = -1;
                goto done;
            }

            dbt_key.data = key;
            dbt_key.size = QUEUEDB_KEY_LEN;
            if (gbl_debug_queuedb) {
               logmsg(LOGMSG_USER, "adding key:\n");
                fsnapf(stdout, key, QUEUEDB_KEY_LEN);
            }
            dbt_data.data = (void *)databuf;
            dbt_data.size = dtalen + sizeof(struct bdb_queue_found);

            if (gbl_debug_queuedb) {
                logmsg(LOGMSG_USER, "inserted:\n");
                fsnapf(stdout, dbt_data.data, dbt_data.size);
            }

            /* TODO: rowlocks? */
            rc = db->put(db, tran->tid, &dbt_key, &dbt_data, 0);
            if (rc == DB_LOCK_DEADLOCK) {
                *bdberr = BDBERR_DEADLOCK;
                qstate->stats.n_add_deadlocks++;
                rc = -1;
                goto done;
            } else if (rc) {
                logmsg(LOGMSG_ERROR,
                       "queuedb %s consumer %d genid %" PRIx64 " put rc %d\n",
                       bdb_state->name, i, k.genid, rc);
                *bdberr = BDBERR_MISC;
                rc = -1;
                goto done;
            }
        }
    }
    bdb_state->qdb_adds++;
    rc = 0;

done:
    if (databuf)
        free(databuf);
    return rc;
}

int bdb_queuedb_walk(bdb_state_type *bdb_state, int flags, void *lastitem,
                     bdb_queue_walk_callback_t callback, void *userptr,
                     int *bdberr)
{
    DB *dbs[2] = {
      BDB_QUEUEDB_GET_DBP_ZERO(bdb_state),
      BDB_QUEUEDB_GET_DBP_ONE(bdb_state)
    };
    DBT dbt_key = {0}, dbt_data = {0};
    DBC *dbcp = NULL;
    int rc;

    if (gbl_debug_queuedb)
        logmsg(LOGMSG_USER, ">>> bdb_queuedb_walk %s\n", bdb_state->name);

    for (int i = 0; i < sizeof(dbs); i++) {
        if (dbcp) {
            int crc2;
            crc2 = dbcp->c_close(dbcp);
            dbcp = NULL;
            if (crc2 == DB_LOCK_DEADLOCK) {
                logmsg(LOGMSG_ERROR, "%s: c_close berk rc %d\n", __func__, crc2);
                *bdberr = BDBERR_DEADLOCK;
                rc = -1;
                goto done;
            } else if (crc2) {
                logmsg(LOGMSG_ERROR, "%s: c_close berk rc %d\n", __func__, crc2);
                *bdberr = BDBERR_MISC;
                rc = -1;
                goto done;
            }
        }

        DB *db = dbs[i]; if (db == NULL) continue;
        dbt_key.flags = dbt_data.flags = DB_DBT_REALLOC;

        /* this API is a little nutty... */
        rc = db->cursor(db, NULL, &dbcp, 0);
        if (rc != 0) {
            *bdberr = BDBERR_MISC;
            return -1;
        }

        if (flags & BDB_QUEUE_WALK_RESTART) {
            /* this is a restart, and lastitem containts a copy of the last key when
             * we stopped */
            dbt_key.data = lastitem;
            dbt_key.size = sizeof(struct queuedb_key);
            rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_FIRST);
        } else {
            rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_SET_RANGE);
        }
        while (rc == 0) {
            struct bdb_queue_found qfnd;
            int consumern = 0;
            uint8_t *p_buf, *p_buf_end;

            lastitem = (void *)dbt_key.data;

            p_buf = dbt_data.data;
            p_buf_end = p_buf + dbt_data.size;
            p_buf = (uint8_t *)queue_found_get(&qfnd, p_buf, p_buf_end);
            if (p_buf == NULL) {
                logmsg(LOGMSG_ERROR, "%s failed to decode queue header for %s\n",
                        __func__, bdb_state->name);
                *bdberr = BDBERR_MISC;
                rc = -1;
                goto done;
            }

            rc = callback(consumern, dbt_data.size, qfnd.epoch, userptr);
            if (rc) {
                rc = 0;
                break;
            }
            rc = dbcp->c_get(dbcp, &dbt_key, &dbt_key, DB_NEXT);
        }
        if (rc) {
            if (rc == DB_LOCK_DEADLOCK) {
                *bdberr = BDBERR_DEADLOCK;
                rc = -1;
                goto done;
            } else if (rc == DB_NOTFOUND) {
                /* EOF */
                rc = 0;
                goto done;
            } else {
                logmsg(LOGMSG_ERROR, "%s find/next berk rc %d\n", __func__, rc);
                *bdberr = BDBERR_MISC;
                assert(rc != 0);
                goto done;
            }
        }
    }
done:
    if (dbcp) {
        int crc;
        crc = dbcp->c_close(dbcp);
        if (crc == DB_LOCK_DEADLOCK) {
            logmsg(LOGMSG_ERROR, "%s: c_close berk rc %d\n", __func__, crc);
            *bdberr = BDBERR_DEADLOCK;
            rc = -1;
        } else if (crc) {
            logmsg(LOGMSG_ERROR, "%s: c_close berk rc %d\n", __func__, crc);
            *bdberr = BDBERR_MISC;
            rc = -1;
        }
    }
    if (dbt_key.data)
        free(dbt_key.data);
    if (dbt_data.data)
        free(dbt_data.data);

    return rc;
}

int bdb_queuedb_dump(bdb_state_type *bdb_state, FILE *out, int *bdberr)
{
    /* TODO */
    *bdberr = BDBERR_NOERROR;
    return 0;
}

int bdb_queuedb_get(bdb_state_type *bdb_state, int consumer,
                    const struct bdb_queue_cursor *prevcursor, void **fnd,
                    size_t *fnddtalen, size_t *fnddtaoff,
                    struct bdb_queue_cursor *fndcursor, unsigned int *epoch,
                    int *bdberr)
{
    DB *db = BDB_QUEUEDB_GET_DBP_ZERO(bdb_state);
    if (db == NULL) { // trigger dropped?
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    struct queuedb_key k;
    DBT dbt_key = {0}, dbt_data = {0};
    DBC *dbcp = NULL;
    int rc;
    struct bdb_queue_found qfnd;
    uint8_t *p_buf, *p_buf_end;
    uint8_t key[QUEUEDB_KEY_LEN] = {0};
    struct queuedb_key fndk;
    uint8_t fndkey[QUEUEDB_KEY_LEN] = {0};
    struct bdb_queue_priv *qstate = bdb_state->qpriv;

    if (gbl_debug_queuedb)
       logmsg(LOGMSG_USER, ">> bdb_queuedb_get %s\n", bdb_state->name);

    dbt_key.flags = dbt_data.flags = DB_DBT_REALLOC;

    rc = db->cursor(db, NULL, &dbcp, 0);
    if (rc) {
        *bdberr = BDBERR_MISC;
        goto done;
    }

    k.consumer = consumer;
    if (prevcursor)
        memcpy(&k.genid, prevcursor->genid, sizeof(uint64_t));
    else
        k.genid = 0;

    p_buf = key;
    p_buf_end = p_buf + QUEUEDB_KEY_LEN;
    p_buf = queuedb_key_put(&k, p_buf, p_buf_end);
    if (p_buf == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d failed to encode key for queue %s consumer %d\n",
                __func__, __LINE__, bdb_state->name, consumer);
        *bdberr = BDBERR_DEADLOCK;
        qstate->stats.n_get_deadlocks++;
        rc = -1;
        goto done;
    }

    dbt_key.data = key;
    dbt_key.size = QUEUEDB_KEY_LEN;
    if (gbl_debug_queuedb) {
        logmsg(LOGMSG_USER, "was looking for:\n");
        fsnapf(stdout, dbt_key.data, dbt_key.size);
    }

    qstate->stats.n_physical_gets++;
    rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_SET_RANGE);
    if (rc) {
        if (rc == DB_LOCK_DEADLOCK) {
            *bdberr = BDBERR_DEADLOCK;
            rc = -1;
            goto done;
        } else if (rc == DB_NOTFOUND) {
            /* empty or past eof? we're done */
            *bdberr = BDBERR_FETCH_DTA;
            rc = -1;
            goto done;
        } else {
            logmsg(LOGMSG_ERROR, "%s %s get rc %d\n", __func__, bdb_state->name, rc);
            *bdberr = BDBERR_MISC;
            goto done;
        }
    }

    if (gbl_debug_queuedb) {
        logmsg(LOGMSG_USER, "found:\n");
        fsnapf(stdout, dbt_key.data, dbt_key.size);
    }

    if (prevcursor && prevcursor->genid[0] != 0 && prevcursor->genid[1] != 0) {
        /* We found something!  It may however be:
         * (1) the previous record that wasn't consumed yet
         * (2) a record for another consumer
         *
         * For (1) we step forward (and need to check for (2) again)
         * For (2) we pretend we're at EOF, since there's nothing else for this
         *consumer.
         */
        fndk.consumer = consumer;
        memcpy(&fndk.genid, prevcursor->genid, sizeof(prevcursor->genid));
        p_buf = fndkey;
        p_buf_end = p_buf + QUEUEDB_KEY_LEN;
        p_buf = queuedb_key_put(&fndk, p_buf, p_buf_end);
        if (p_buf == NULL) {
            logmsg(LOGMSG_ERROR, 
                "%s:%d failed to encode found key for queue %s consumer %d\n",
                __func__, __LINE__, bdb_state->name, consumer);
            *bdberr = BDBERR_DEADLOCK;
            rc = -1;
            goto done;
        }
        if (gbl_debug_queuedb) {
            logmsg(LOGMSG_USER, "was looking for key:\n");
            fsnapf(stdout, key, QUEUEDB_KEY_LEN);
            logmsg(LOGMSG_USER, "found key:\n");
            fsnapf(stdout, fndkey, QUEUEDB_KEY_LEN);
        }

        /* case (1) - we found a key that matches the prev key - unconsumed
         * record */
        if (memcmp(&fndkey, key, QUEUEDB_KEY_LEN) == 0) {
            rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_NEXT);
            if (rc) {
                if (rc == DB_LOCK_DEADLOCK) {
                    *bdberr = BDBERR_DEADLOCK;
                    rc = -1;
                    goto done;
                } else if (rc == DB_NOTFOUND) {
                    /* empty or eof */
                    qstate->stats.n_get_not_founds++;
                    *bdberr = BDBERR_FETCH_DTA;
                    rc = -1;
                    goto done;
                } else {
                    logmsg(LOGMSG_ERROR, "%s %s next rc %d\n", __func__,
                            bdb_state->name, rc);
                    *bdberr = BDBERR_MISC;
                    goto done;
                }
            }
        }
    }

    /* see if the thing we ended up on is for a different consumer - case (2) */
    p_buf = dbt_key.data;
    p_buf_end = p_buf + dbt_key.size;
    p_buf = queuedb_key_get(&fndk, p_buf, p_buf_end);
    if (p_buf == NULL) {
        logmsg(LOGMSG_USER, 
                "%s:%d failed to decode found key for queue %s consumer %d\n",
                __func__, __LINE__, bdb_state->name, consumer);
        *bdberr = BDBERR_DEADLOCK;
        rc = -1;
        goto done;
    }
    if (gbl_debug_queuedb)
        logmsg(LOGMSG_USER, "next key is consumer %d genid %016" PRIx64 "\n",
               fndk.consumer, fndk.genid);
    if (fndk.consumer != consumer) {
        /* pretend we didn't find anything - the next record is meant for
         * a different consumer, our "queue" is empty */
        if (gbl_debug_queuedb)
            logmsg(LOGMSG_USER, "found record for consumer %d but I am %d\n", fndk.consumer,
                   consumer);
        *bdberr = BDBERR_FETCH_DTA;
        rc = -1;
        goto done;
    }

    /* made this far? massage the data and return it. */
    p_buf = dbt_data.data;
    p_buf_end = p_buf + dbt_data.size;
    if (dbt_data.size < sizeof(struct bdb_queue_found)) {
        logmsg(LOGMSG_ERROR, "%s: invalid queue entry size %d in queue %s\n",
                __func__, dbt_data.size, bdb_state->name);
        *bdberr = BDBERR_MISC; /* ... */
        rc = -1;
        goto done;
    }

    p_buf = (uint8_t *)queue_found_get(&qfnd, p_buf, p_buf_end);
    if (p_buf == NULL) {
        logmsg(LOGMSG_ERROR, "%s: can't decode header size %u in queue %s\n",
               __func__, dbt_data.size, bdb_state->name);
        *bdberr = BDBERR_MISC; /* ... */
        rc = -1;
        goto done;
    }

    /* what endianness is this? */
    *fnd = dbt_data.data;
    if (fnddtalen)
        *fnddtalen = dbt_data.size;
    if (fnddtaoff)
        *fnddtaoff =
            qfnd.data_offset; /* This length will be used to check version. */
    if (fndcursor) {
        memcpy(fndcursor->genid, &fndk.genid, sizeof(fndk.genid));
        fndcursor->recno = 0;
        fndcursor->reserved = 0;
    }
    dbt_data.data = NULL;
    *bdberr = BDBERR_NOERROR;
    rc = 0;

done:
    if (dbcp) {
        int crc;
        crc = dbcp->c_close(dbcp);
        if (crc) {
            if (crc == DB_LOCK_DEADLOCK) {
                logmsg(LOGMSG_ERROR, "%s: c_close berk rc %d\n", __func__, crc);
                *bdberr = DB_LOCK_DEADLOCK;
                rc = -1;
            } else {
                logmsg(LOGMSG_ERROR, "%s: c_close berk rc %d\n", __func__, crc);
                *bdberr = BDBERR_MISC;
                rc = -1;
            }
        }
    }
    if (dbt_key.data && dbt_key.data != key /*this puppy isn't malloced*/)
        free(dbt_key.data);
    if (dbt_data.data)
        free(dbt_data.data);

    return rc;
}

int bdb_queuedb_consume(bdb_state_type *bdb_state, tran_type *tran,
                        int consumer, const void *prevfnd, int *bdberr)
{
    DB *db = BDB_QUEUEDB_GET_DBP_ZERO(bdb_state);
    struct bdb_queue_found qfnd;
    uint8_t *p_buf, *p_buf_end;
    struct queuedb_key k;
    uint8_t key[QUEUEDB_KEY_LEN];
    int rc = 0;
    struct bdb_queue_priv *qstate = bdb_state->qpriv;
    struct queuedb_key fndk = {0};

    DBT dbt_key = {0}, dbt_data = {0};
    DBC *dbcp = NULL;

    if (gbl_debug_queuedb) {
        logmsg(LOGMSG_USER, ">> bdb_queuedb_consume %s\n", bdb_state->name);
        logmsg(LOGMSG_USER, "prevfnd:\n");
        fsnapf(stdout, (void *)prevfnd, sizeof(struct bdb_queue_found));
    }

    p_buf = (uint8_t *)prevfnd;
    p_buf_end = p_buf + sizeof(struct bdb_queue_found);
    p_buf = (uint8_t *)queue_found_get(&qfnd, p_buf, p_buf_end);
    if (p_buf == NULL) {
        logmsg(LOGMSG_ERROR, 
                "%s: can't decode queue header for queue %s consumer %d\n",
                __func__, bdb_state->name, consumer);
        *bdberr = BDBERR_MISC;
        rc = -1;
        goto done;
    }
    k.consumer = consumer;
    k.genid = 0;
    if (gbl_debug_queuedb)
        logmsg(LOGMSG_USER, "consumer %d genid %016llx\n", consumer,
               qfnd.genid);
    p_buf = (uint8_t *)key;
    p_buf_end = p_buf + QUEUEDB_KEY_LEN;
    p_buf = queuedb_key_put(&k, p_buf, p_buf_end);
    if (p_buf == NULL) {
        logmsg(LOGMSG_ERROR, "%s: can't decode queue key for queue %s consumer %d\n",
                __func__, bdb_state->name, consumer);
        *bdberr = BDBERR_MISC;
        rc = -1;
        goto done;
    }
    if (gbl_debug_queuedb) {
        logmsg(LOGMSG_USER, "trying to consume:\n");
        fsnapf(stdout, key, QUEUEDB_KEY_LEN);
    }

    /* we don't want to actually fetch the data, just position the cursor */
    dbt_key.flags = dbt_data.flags = DB_DBT_REALLOC;
    dbt_key.data = key;
    dbt_key.size = QUEUEDB_KEY_LEN;

    rc = db->cursor(db, tran->tid, &dbcp, 0);
    if (rc != 0) {
        *bdberr = BDBERR_MISC;
        goto done;
    }
    rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_FIRST);
    if (rc == DB_NOTFOUND) {
        if (gbl_debug_queuedb) {
            logmsg(LOGMSG_USER, "not found on consume:\n");
            fsnapf(stdout, dbt_key.data, QUEUEDB_KEY_LEN);
        }
        *bdberr = BDBERR_DELNOTFOUND;
        rc = -1;
        goto done;
    } else if (rc == DB_LOCK_DEADLOCK) {
        *bdberr = BDBERR_DEADLOCK;
        rc = -1;
        qstate->stats.n_consume_deadlocks++;
        goto done;
    } else if (rc) {
        logmsg(LOGMSG_ERROR, "%s: find queue %s consumer %d berk rc %d\n", __func__,
                bdb_state->name, consumer, rc);
        *bdberr = BDBERR_MISC;
        rc = -1;
        goto done;
    }

    p_buf = dbt_key.data;
    p_buf_end = p_buf + dbt_key.size;
    p_buf = queuedb_key_get(&fndk, p_buf, p_buf_end);

    if ((fndk.genid != qfnd.genid) &&
        (fndk.genid != flibc_ntohll(qfnd.genid))) {
        logmsg(LOGMSG_ERROR,
               "%s: Trying to consume non-first item of queue %s genid: "
               "%016llx first: %016" PRIx64 " consumer %d\n",
               __func__, bdb_state->name, qfnd.genid, fndk.genid, consumer);
        *bdberr = BDBERR_MISC;
        rc = -1;
        goto done;
    }

    /* we found it, delete */
    rc = dbcp->c_del(dbcp, 0);
    if (rc) {
        if (rc == DB_LOCK_DEADLOCK) {
            *bdberr = BDBERR_DEADLOCK;
            rc = -1;
            goto done;
        } else if (rc) {
            logmsg(LOGMSG_ERROR, "%s: del queue %s consumer %d berk rc %d\n",
                    __func__, bdb_state->name, consumer, rc);
            *bdberr = BDBERR_DEADLOCK;
            rc = -1;
            goto done;
        }
    }
    bdb_state->qdb_cons++;
    if (gbl_debug_queuedb)
        logmsg(LOGMSG_USER, ">> CONSUMED!\n");

/* and we consumed successfully */
done:
    if (dbcp) {
        int crc;
        crc = dbcp->c_close(dbcp);
        if (crc == DB_LOCK_DEADLOCK) {
            logmsg(LOGMSG_ERROR, "%s: c_close berk rc %d\n", __func__, crc);
            *bdberr = BDBERR_DEADLOCK;
            rc = -1;
        } else if (crc) {
            logmsg(LOGMSG_ERROR, "%s: c_close berk rc %d\n", __func__, crc);
            *bdberr = BDBERR_MISC;
            rc = -1;
        }
    }
    if (dbt_key.data && dbt_key.data != key)
        free(dbt_key.data);
    if (dbt_data.data)
        free(dbt_data.data);
    return rc;
}

const struct bdb_queue_stats *bdb_queuedb_get_stats(bdb_state_type *bdb_state)
{
    struct bdb_queue_priv *qstate = bdb_state->qpriv;

    return &qstate->stats;
}

int bdb_trigger_subscribe(bdb_state_type *bdb_state, pthread_cond_t **cond,
                          pthread_mutex_t **lock, const uint8_t **status)
{
    DB_ENV *dbenv = bdb_state->dbenv;
    return dbenv->trigger_subscribe(dbenv, bdb_state->name, cond, lock, status);
}

int bdb_trigger_unsubscribe(bdb_state_type *bdb_state)
{
    DB_ENV *dbenv = bdb_state->dbenv;
    return dbenv->trigger_unsubscribe(dbenv, bdb_state->name);
}

int bdb_trigger_open(bdb_state_type *bdb_state)
{
    DB_ENV *dbenv = bdb_state->dbenv;
    return dbenv->trigger_open(dbenv, bdb_state->name);
}

int bdb_trigger_close(bdb_state_type *bdb_state)
{
    DB_ENV *dbenv = bdb_state->dbenv;
    return dbenv->trigger_close(dbenv, bdb_state->name);
}

int bdb_trigger_ispaused(bdb_state_type *bdb_state)
{
    DB_ENV *dbenv = bdb_state->dbenv;
    return dbenv->trigger_ispaused(dbenv, bdb_state->name);
}

int bdb_trigger_pause(bdb_state_type *bdb_state)
{
    DB_ENV *dbenv = bdb_state->dbenv;
    return dbenv->trigger_pause(dbenv, bdb_state->name);
}

int bdb_trigger_unpause(bdb_state_type *bdb_state)
{
    DB_ENV *dbenv = bdb_state->dbenv;
    return dbenv->trigger_unpause(dbenv, bdb_state->name);
}
