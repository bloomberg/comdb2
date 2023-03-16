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
#include "translistener.h"

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

int put_queue_sequence(const char *name, tran_type *, long long seq);
int get_queue_sequence_tran(const char *name, long long *seq, tran_type *);

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

    rc = db->cursor(db, tran ? tran->tid : NULL, &dbcp, 0);
    if (rc != 0) {
        rc = 0; /* TODO: Safe failure choice here is non-empty? */
        goto done;
    }
    DBT dbt_key = {0}, dbt_data = {0};
    dbt_key.flags = dbt_data.flags = DB_DBT_PARTIAL;
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
    return rc;
}

static int bdb_queuedb_is_db_full(DB *db)
{
    if (gbl_queuedb_file_threshold <= 0) return 0; /* never full? */
    char new[PATH_MAX];
    struct stat sb;
    if (stat(bdb_trans(db->fname, new), &sb) != 0) {
        logmsg(LOGMSG_ERROR, "%s: stat rc %d\n", __func__, errno);
        return 0; /* cannot detect, assume no? */
    }
    return ((sb.st_size / 1048576) >= gbl_queuedb_file_threshold);
}

static int start_qdb_schemachange(struct schema_change_type *sc)
{
    javasp_splock_wrlock();
    int rc = start_schema_change(sc);
    javasp_splock_unlock();
    return rc;
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
    bdb_thread_event(bdb_state, BDBTHR_EVENT_START_RDWR);
    BDB_READLOCK("queuedb cron thread");
    if (dbenv->master != gbl_myhostname) {
        BDB_RELLOCK();
        bdb_thread_event(bdb_state, BDBTHR_EVENT_DONE_RDWR);
        return NULL; 
    }
    wrlock_schema_lk();
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
                sc->kind = SC_DEL_QDB_FILE;
                sc->nothrevent = 1;
                sc->finalize = 1;
                sc->already_locked = 1;
                sc->keep_locked = 1;
                sc->db = tbl;
                rc = start_qdb_schemachange(sc);
                if ((rc != SC_OK) && (rc != SC_ASYNC)) {
                    logmsg(LOGMSG_ERROR,
                           "%s: failed to start schema change to delete "
                           "old file for queuedb '%s'\n",
                           __func__, sc->tablename);
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
            sc->kind = SC_ADD_QDB_FILE;
            sc->qdb_file_ver = flibc_htonll(bdb_get_cmp_context(tbl_bdb_state));
            sc->nothrevent = 1;
            sc->finalize = 1;
            sc->already_locked = 1;
            sc->keep_locked = 1;
            sc->db = tbl;
            rc = start_qdb_schemachange(sc);
            if ((rc != SC_OK) && (rc != SC_ASYNC)) {
                logmsg(LOGMSG_ERROR,
                       "%s: failed to start schema change to add "
                       "new file for queuedb '%s'\n",
                           __func__, sc->tablename);
            }
        } else {
            logmsg(LOGMSG_DEBUG,
                "%s: queuedb '%s' has one file and old file is not "
                "full, doing nothing...\n", __func__, tbl_bdb_state->name);
        }
    }
    unlock_schema_lk();
    BDB_RELLOCK();
    bdb_thread_event(bdb_state, BDBTHR_EVENT_DONE_RDWR);
    return NULL;
}

static void *queuedb_cron_kickoff(struct cron_event *evt, struct errstat *err)
{
    if (db_is_stopped()) return NULL;
    if (gbl_queuedb_file_interval > 0) {
        logmsg(LOGMSG_INFO, "Starting queuedb cron job. Will check queuedb usage every %d seconds.\n",
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
    return calc_pagesize(4096, avg_item_sz);
}

/* add to queue */
int bdb_queuedb_add(bdb_state_type *bdb_state, tran_type *tran, const void *dta,
                    size_t dtalen, int *bdberr, unsigned long long *out_genid)
{
    struct bdb_queue_priv *qstate = (struct bdb_queue_priv *)bdb_state->qpriv;

    /* TODO: rather than grabbing inline, minimize the time we hold this lock by
     * deferring queue-writes until after everything else in toblock is done.
     * Also force all transactions to acquire these locks in the same order so
     * that we can avoid deadlocks from two transactions which have both made
     * it to that point. */
    int rc = bdb_lock_table_read(bdb_state, tran);
    if (rc == DB_LOCK_DEADLOCK) {
        *bdberr = BDBERR_DEADLOCK;
        qstate->stats.n_add_deadlocks++;
        return -1;
    } else if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: queuedb %s error getting tablelock %d\n",
               __func__, bdb_state->name, rc);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    struct queuedb_key k;
    unsigned long long genid;
    uint8_t ver = 0;
    DBT dbt_key = {0}, dbt_data = {0};
    DBC *dbcp1 = NULL;
    DBC *dbcp2 = NULL;
    uint8_t key[QUEUEDB_KEY_LEN];
    uint8_t *p_buf, *p_buf_end;
    struct bdb_queue_found qfnd;
    struct bdb_queue_found_seq qfnd_odh, prev_seq;
    void *databuf = NULL;
    void *freeme1 = NULL;
    void *freeme2 = NULL;

    if (gbl_debug_queuedb)
        logmsg(LOGMSG_USER, ">>> bdb_queuedb_add %s\n", bdb_state->name);

    databuf = malloc(dtalen + sizeof(struct bdb_queue_found_seq));

    int usingDbpOne = 0;
    DB *db1 = BDB_QUEUEDB_GET_DBP_ZERO(bdb_state);
    DB *db2 = BDB_QUEUEDB_GET_DBP_ONE(bdb_state);
    DB *db = db2;
    if (db != NULL) {
        usingDbpOne = 1; /* second file is being used */
    } else {
        db = db1;
    }
    rc = db->cursor(db, tran->tid, &dbcp1, 0);
    if (rc != 0) {
        *bdberr = BDBERR_MISC;
        rc = -1;
        goto done;
    }

    dbt_key.data = key;
    dbt_key.ulen = QUEUEDB_KEY_LEN;
    dbt_key.flags = DB_DBT_USERMEM;

    dbt_data.data = NULL;
    dbt_data.flags = DB_DBT_MALLOC;

    /* Lock last page */
    rc = bdb_cget_unpack(bdb_state, dbcp1, &dbt_key, &dbt_data, &ver,
                         DB_LAST | DB_RMW);

    if (rc == 0) {
        freeme1 = dbt_data.data;
    } else if (rc == DB_LOCK_DEADLOCK) {
        *bdberr = BDBERR_DEADLOCK;
        rc = -1;
        goto done;
    } else if (rc != DB_NOTFOUND) {
        logmsg(LOGMSG_ERROR, "%s bad rc %d retrieving seq for %s\n", __func__,
               rc, bdb_state->name);
        *bdberr = BDBERR_MISC;
        rc = -1;
        goto done;
    }

    /* DB_RMW holds a writelock on rightmost btree page */
    if (bdb_state->ondisk_header) {
        genid = qfnd_odh.genid = get_genid(bdb_state, 0);
        qfnd_odh.data_len = dtalen;
        qfnd_odh.data_offset = sizeof(struct bdb_queue_found_seq);
        qfnd_odh.trans.tid = tran->tid->txnid;
        if (tran->trigger_epoch) {
            qfnd_odh.epoch = tran->trigger_epoch;
        } else {
            qfnd_odh.epoch = tran->trigger_epoch = comdb2_time_epoch();
        }

        prev_seq.seq = 0;

        if (rc == 0) {
            p_buf = dbt_data.data;
            p_buf_end = p_buf + dbt_data.size;
            p_buf = (uint8_t *)queue_found_seq_get(&prev_seq, p_buf, p_buf_end);
            if (p_buf == NULL) {
                logmsg(LOGMSG_ERROR, "%s failed to decode prev seq for %s\n",
                       __func__, bdb_state->name);
                *bdberr = BDBERR_MISC;
                rc = -1;
                goto done;
            }
        } else if (usingDbpOne) {
            int rc2;
            assert(db == db2);

            rc2 = db1->cursor(db1, tran->tid, &dbcp2, 0);
            if (rc2 != 0) {
                *bdberr = BDBERR_MISC;
                rc = -1;
                goto done;
            }

            dbt_key.data = key;
            dbt_key.ulen = QUEUEDB_KEY_LEN;
            dbt_key.flags = DB_DBT_USERMEM;

            dbt_data.data = NULL;
            dbt_data.flags = DB_DBT_MALLOC;

            rc2 = bdb_cget_unpack(bdb_state, dbcp2, &dbt_key, &dbt_data, &ver,
                                  DB_LAST);

            if (rc2 == 0) {
                freeme2 = dbt_data.data;
                p_buf = dbt_data.data;
                p_buf_end = p_buf + dbt_data.size;
                p_buf = (uint8_t *)queue_found_seq_get(&prev_seq, p_buf,
                                                       p_buf_end);
                if (p_buf == NULL) {
                    logmsg(LOGMSG_ERROR,
                           "%s failed to decode prev seq for %s\n",
                           __func__, bdb_state->name);
                    *bdberr = BDBERR_MISC;
                    rc = -1;
                    goto done;
                }
            } else if (rc2 == DB_LOCK_DEADLOCK) {
                *bdberr = BDBERR_DEADLOCK;
                rc = -1;
                goto done;
            } else if (rc2 != DB_NOTFOUND) {
                logmsg(LOGMSG_ERROR, "%s bad rc2 %d retrieving seq for %s\n",
                       __func__, rc2, bdb_state->name);
                *bdberr = BDBERR_MISC;
                rc = -1;
                goto done;
            } else if (bdb_state->persistent_seq) {
                get_queue_sequence_tran(bdb_state->name, &prev_seq.seq, tran);
            }
        } else if (bdb_state->persistent_seq) {
            get_queue_sequence_tran(bdb_state->name, &prev_seq.seq, tran);
        }
        qfnd_odh.seq = (prev_seq.seq + 1);

        dbt_key.flags = dbt_data.flags = 0;
        dbt_key.ulen = dbt_data.ulen = 0;

        p_buf = databuf;
        p_buf_end = p_buf + dtalen + sizeof(struct bdb_queue_found_seq);
        p_buf = queue_found_seq_put(&qfnd_odh, p_buf, p_buf_end);
    } else {
        genid = qfnd.genid = get_genid(bdb_state, 0);
        qfnd.data_len = dtalen;
        qfnd.data_offset = sizeof(struct bdb_queue_found);
        qfnd.trans.tid = tran->tid->txnid;
        if (tran->trigger_epoch) {
            qfnd.epoch = tran->trigger_epoch;
        } else {
            qfnd.epoch = tran->trigger_epoch = comdb2_time_epoch();
        }

        p_buf = databuf;
        p_buf_end = p_buf + dtalen + sizeof(struct bdb_queue_found_seq);
        p_buf = queue_found_put(&qfnd, p_buf, p_buf_end);
    }
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
            k.genid = genid;
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
            if (bdb_state->ondisk_header)
                dbt_data.size = dtalen + sizeof(struct bdb_queue_found_seq);
            else
                dbt_data.size = dtalen + sizeof(struct bdb_queue_found);

            if (gbl_debug_queuedb) {
                logmsg(LOGMSG_USER, "inserted:\n");
                fsnapf(stdout, dbt_data.data, dbt_data.size);
            }

            /* TODO: rowlocks? */
            rc = bdb_cput_pack(bdb_state, 0, dbcp1, &dbt_key, &dbt_data,
                               DB_KEYLAST);
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
    if (dbcp2) {
        int crc;
        crc = dbcp2->c_close(dbcp2);
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
        dbcp2 = NULL;
    }
    if (dbcp1) {
        int crc;
        crc = dbcp1->c_close(dbcp1);
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
        dbcp1 = NULL;
    }
    if (databuf)
        free(databuf);
    if (freeme2)
        free(freeme2);
    if (freeme1)
        free(freeme1);
    return rc;
}

int bdb_queuedb_stats(bdb_state_type *bdb_state,
                      bdb_queue_stats_callback_t callback, tran_type *tran,
                      void *userptr, int *bdberr)
{
    int rc = bdb_lock_table_read(bdb_state, tran);
    if (rc == DB_LOCK_DEADLOCK) {
        *bdberr = BDBERR_DEADLOCK;
        return -1;
    } else if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: queuedb %s error getting tablelock %d\n",
               __func__, bdb_state->name, rc);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    DBT dbt_key = {0}, dbt_data = {0};
    DBC *dbcp1 = NULL;
    DBC *dbcp2 = NULL;
    uint8_t ver = 0;
    size_t item_length = 0;
    unsigned int epoch = 0, first_seq = 0, last_seq = 0;
    struct bdb_queue_found_seq qfnd_odh;
    uint8_t *p_buf, *p_buf_end;
    int consumern = 0;

    assert(bdb_state->ondisk_header);
    if (gbl_debug_queuedb)
        logmsg(LOGMSG_USER, ">>> bdb_queuedb_stats %s\n", bdb_state->name);

    dbt_key.flags = dbt_data.flags = DB_DBT_REALLOC;

    DB *db1 = BDB_QUEUEDB_GET_DBP_ZERO(bdb_state);
    rc = db1->cursor(db1, tran ? tran->tid : NULL, &dbcp1, 0);

    if (rc != 0) {
        *bdberr = BDBERR_MISC;
        goto done;
    }

    DB *db2 = BDB_QUEUEDB_GET_DBP_ONE(bdb_state);
    if (db2 != NULL) {
        rc = db2->cursor(db2, tran ? tran->tid : NULL, &dbcp2, 0);

        if (rc != 0) {
            *bdberr = BDBERR_MISC;
            goto done;
        }
    } else {
        dbcp2 = dbcp1; /* no second file, use same cursor */
    }

    rc = bdb_cget_unpack(bdb_state, dbcp1, &dbt_key, &dbt_data, &ver, DB_FIRST);

    int did_dbcp2_first = 0;
chk_db_first_rc:
    if (rc) {
        if (rc == DB_LOCK_DEADLOCK) {
            *bdberr = BDBERR_DEADLOCK;
            rc = -1;
            goto done;
        } else if (rc == DB_NOTFOUND) {
            if (!did_dbcp2_first && (dbcp2 != dbcp1)) {
                rc = bdb_cget_unpack(bdb_state, dbcp2, &dbt_key, &dbt_data, &ver,
                                     DB_FIRST);
                did_dbcp2_first = 1; goto chk_db_first_rc;
            } else {
                /* EOF */
                rc = 0;
                goto done;
            }
        } else {
            logmsg(LOGMSG_ERROR, "%s first berk rc %d\n", __func__, rc);
            *bdberr = BDBERR_MISC;
            assert(rc != 0);
            goto done;
        }
    }

    p_buf = dbt_data.data;
    p_buf_end = p_buf + dbt_data.size;
    p_buf = (uint8_t *)queue_found_seq_get(&qfnd_odh, p_buf, p_buf_end);
    first_seq = qfnd_odh.seq;
    epoch = qfnd_odh.epoch;
    item_length = dbt_data.size;

    rc = bdb_cget_unpack(bdb_state, dbcp2, &dbt_key, &dbt_data, &ver, DB_LAST);

    int did_dbcp2_last = 0;
chk_db_last_rc:
    if (rc) {
        if (rc == DB_LOCK_DEADLOCK) {
            *bdberr = BDBERR_DEADLOCK;
            rc = -1;
            goto done;
        } else if (rc == DB_NOTFOUND) {
            if (!did_dbcp2_last && (dbcp2 != dbcp1)) {
                rc = bdb_cget_unpack(bdb_state, dbcp1, &dbt_key, &dbt_data, &ver,
                                     DB_LAST);
                did_dbcp2_last = 1; goto chk_db_last_rc;
            } else {
                /* EOF */
                rc = 0;
                goto done;
            }
        } else {
            logmsg(LOGMSG_ERROR, "%s last berk rc %d\n", __func__, rc);
            *bdberr = BDBERR_MISC;
            assert(rc != 0);
            goto done;
        }
    }

    p_buf = dbt_data.data;
    p_buf_end = p_buf + dbt_data.size;
    p_buf = (uint8_t *)queue_found_seq_get(&qfnd_odh, p_buf, p_buf_end);
    last_seq = qfnd_odh.seq;

    if (last_seq >= first_seq)
        callback(consumern, item_length, epoch, (last_seq - first_seq) + 1,
                 userptr);

done:
    if (dbcp2 && (dbcp2 != dbcp1)) {
        int crc;
        crc = dbcp2->c_close(dbcp2);
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
    if (dbcp1) {
        int crc;
        crc = dbcp1->c_close(dbcp1);
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

int bdb_queuedb_walk(bdb_state_type *bdb_state, int flags, void *lastitem,
                     bdb_queue_walk_callback_t callback, tran_type *tran,
                     void *userptr, int *bdberr)
{
    int rc = bdb_lock_table_read(bdb_state, tran);
    if (rc == DB_LOCK_DEADLOCK) {
        *bdberr = BDBERR_DEADLOCK;
        return -1;
    } else if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: queuedb %s error getting tablelock %d\n",
               __func__, bdb_state->name, rc);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    DB *dbs[2] = {
        BDB_QUEUEDB_GET_DBP_ZERO(bdb_state),
        BDB_QUEUEDB_GET_DBP_ONE(bdb_state)
    };
    DBT dbt_key = {0}, dbt_data = {0};
    DBC *dbcp = NULL;
    uint8_t ver = 0;

    if (gbl_debug_queuedb)
        logmsg(LOGMSG_USER, ">>> bdb_queuedb_walk %s\n", bdb_state->name);

    for (int i = 0; i < sizeof(dbs)/sizeof(dbs[0]); i++) {
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
        rc = db->cursor(db, tran ? tran->tid : NULL, &dbcp, 0);
        if (rc != 0) {
            *bdberr = BDBERR_MISC;
            return -1;
        }

        if (flags & BDB_QUEUE_WALK_RESTART) {
            /* this is a restart, and lastitem containts a copy of the last key when
             * we stopped */
            dbt_key.data = lastitem;
            dbt_key.size = sizeof(struct queuedb_key);
            rc = bdb_cget_unpack(bdb_state, dbcp, &dbt_key, &dbt_data, &ver,
                                 DB_FIRST);

            /* TODO: It seems the BDB_QUEUE_WALK_RESTART flag should only be
             *       honored on the first get operation? */
            flags &= ~BDB_QUEUE_WALK_RESTART;
        } else {
            rc = bdb_cget_unpack(bdb_state, dbcp, &dbt_key, &dbt_data, &ver,
                                 DB_SET_RANGE);
        }
        while (rc == 0) {
            struct bdb_queue_found qfnd;
            struct bdb_queue_found_seq qfnd_odh;
            unsigned int epoch;
            int consumern = 0;
            uint8_t *p_buf, *p_buf_end;

            lastitem = (void *)dbt_key.data;

            p_buf = dbt_data.data;
            p_buf_end = p_buf + dbt_data.size;

            if (bdb_state->ondisk_header) {
                p_buf = (uint8_t *)queue_found_seq_get(&qfnd_odh, p_buf, p_buf_end);
                epoch = qfnd_odh.epoch;
            } else {
                p_buf = (uint8_t *)queue_found_get(&qfnd, p_buf, p_buf_end);
                epoch = qfnd.epoch;
            }
            if (p_buf == NULL) {
                logmsg(LOGMSG_ERROR, "%s failed to decode queue header for %s\n",
                        __func__, bdb_state->name);
                *bdberr = BDBERR_MISC;
                rc = -1;
                goto done;
            }

            rc = callback(consumern, dbt_data.size, epoch, userptr);
            if (rc) {
                rc = 0;
                break;
            }
            rc = bdb_cget_unpack(bdb_state, dbcp, &dbt_key, &dbt_data, &ver,
                                 DB_NEXT);
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

static int bdb_queuedb_get_int(bdb_state_type *bdb_state, tran_type *tran, DB *db, int consumer,
                               const struct bdb_queue_cursor *prevcursor, struct bdb_queue_found **fnd,
                               size_t *fnddtalen, size_t *fnddtaoff, struct bdb_queue_cursor *fndcursor,
                               long long *seq, int *bdberr)
{
    if (db == NULL) { // trigger dropped?
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    struct queuedb_key k;
    DBT dbt_key = {0}, dbt_data = {0};
    DBC *dbcp = NULL;
    uint8_t ver = 0;
    size_t data_offset;
    int rc;
    long long sequence = 0;

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

    if (tran) {
        dbcp->c_replace_lockid(dbcp, tran->tid->txnid);
    }

    k.consumer = consumer;
    if (prevcursor) {
        memcpy(&k.genid, &prevcursor->genid, sizeof(uint64_t));
    }
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
    rc = bdb_cget_unpack(bdb_state, dbcp, &dbt_key, &dbt_data, &ver,
                         DB_SET_RANGE);
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

    if (prevcursor && prevcursor->genid != 0) {
        /* We found something!  It may however be:
         * (1) the previous record that wasn't consumed yet
         * (2) a record for another consumer
         *
         * For (1) we step forward (and need to check for (2) again)
         * For (2) we pretend we're at EOF, since there's nothing else for this
         *consumer.
         */
        fndk.consumer = consumer;
        memcpy(&fndk.genid, &prevcursor->genid, sizeof(prevcursor->genid));
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
            rc = bdb_cget_unpack(bdb_state, dbcp, &dbt_key, &dbt_data, &ver,
                                 DB_NEXT);
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

    if (bdb_state->ondisk_header) {
        struct bdb_queue_found_seq qfnd_odh;
        p_buf = (uint8_t *)queue_found_seq_get(&qfnd_odh, p_buf, p_buf_end);
        memcpy(dbt_data.data, &qfnd_odh, sizeof(qfnd_odh));
        sequence = qfnd_odh.seq;
        data_offset = qfnd_odh.data_offset;
    } else {
        struct bdb_queue_found qfnd;
        p_buf = (uint8_t *)queue_found_get(&qfnd, p_buf, p_buf_end);
        memcpy(dbt_data.data, &qfnd, sizeof(qfnd));
        data_offset = qfnd.data_offset;
    }
    if (p_buf == NULL) {
        logmsg(LOGMSG_ERROR, "%s: can't decode header size %u in queue %s\n",
               __func__, dbt_data.size, bdb_state->name);
        *bdberr = BDBERR_MISC; /* ... */
        rc = -1;
        goto done;
    }

    *fnd = dbt_data.data;
    if (fnddtalen)
        *fnddtalen = dbt_data.size;
    if (fnddtaoff)
        *fnddtaoff =
            data_offset; /* This length will be used to check version. */
    if (seq)
        *seq = sequence;
    if (fndcursor) {
        memcpy(&fndcursor->genid, &fndk.genid, sizeof(fndk.genid));
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

int bdb_queuedb_get(bdb_state_type *bdb_state, tran_type *tran, int consumer,
                    const struct bdb_queue_cursor *prevcursor,
                    struct bdb_queue_found **fnd, size_t *fnddtalen,
                    size_t *fnddtaoff, struct bdb_queue_cursor *fndcursor,
                    long long *seq, int *bdberr)
{
    int rc = bdb_lock_table_read(bdb_state, tran);
    if (rc == DB_LOCK_DEADLOCK) {
        *bdberr = BDBERR_DEADLOCK;
        struct bdb_queue_priv *qstate = bdb_state->qpriv;
        qstate->stats.n_get_deadlocks++;
        return -1;
    } else if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: queuedb %s error getting tablelock %d\n",
               __func__, bdb_state->name, rc);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    DB *db = BDB_QUEUEDB_GET_DBP_ZERO(bdb_state);
    assert(db != NULL);
    *bdberr = 0;

    rc = bdb_queuedb_get_int(bdb_state, tran, db, consumer, prevcursor,
                             fnd, fnddtalen, fnddtaoff, fndcursor,
                             seq, bdberr);
    if ((rc == -1) && (*bdberr == BDBERR_FETCH_DTA)) { /* EMPTY FILE #0? */
        db = BDB_QUEUEDB_GET_DBP_ONE(bdb_state);

        if (db != NULL) {
            *bdberr = 0;

            rc = bdb_queuedb_get_int(bdb_state, tran, db, consumer, prevcursor,
                                     fnd, fnddtalen, fnddtaoff, fndcursor,
                                     seq, bdberr);
        }
    }
    return rc;
}

static int bdb_queuedb_consume_int(bdb_state_type *bdb_state, DB *db,
                                   tran_type *tran, int consumer,
                                   const struct bdb_queue_found *fnd,
                                   int put_seq, int *bdberr)
{
    struct queuedb_key k = {
        .consumer = consumer,
        .genid = fnd->genid
    };
    uint8_t ver = 0;
    uint8_t search[QUEUEDB_KEY_LEN];
    queuedb_key_put(&k, search, search + sizeof(search));

    DBT key = {0};
    key.flags = DB_DBT_USERMEM;
    key.data = search;
    key.size = sizeof(search);

    DBT val = {0};
    if (bdb_state->persistent_seq) {
        val.flags = DB_DBT_MALLOC;
    } else {
        val.flags = DB_DBT_PARTIAL;
    }

    DBC *dbcp = NULL;
    int rc = db->cursor(db, tran->tid, &dbcp, 0);
    if (rc != 0) {
        *bdberr = BDBERR_MISC;
        goto done;
    }

    if (bdb_state->persistent_seq)
        rc = bdb_cget_unpack(bdb_state, dbcp, &key, &val, &ver, DB_SET);
    else
        rc = dbcp->c_get(dbcp, &key, &val, DB_SET);
    if (rc == DB_NOTFOUND) {
        *bdberr = BDBERR_DELNOTFOUND;
        rc = -1;
        goto done;
    } else if (rc == DB_LOCK_DEADLOCK) {
        *bdberr = BDBERR_DEADLOCK;
        rc = -1;
        struct bdb_queue_priv *qstate = bdb_state->qpriv;
        qstate->stats.n_consume_deadlocks++;
        goto done;
    } else if (rc) {
        logmsg(LOGMSG_ERROR, "%s: find queue %s consumer %d berk rc %d\n", __func__,
                bdb_state->name, consumer, rc);
        *bdberr = BDBERR_MISC;
        rc = -1;
        goto done;
    }

    rc = dbcp->c_del(dbcp, 0);
    if (rc) {
        if (rc == DB_LOCK_DEADLOCK) {
            *bdberr = BDBERR_DEADLOCK;
            rc = -1;
            goto done;
        } else if (rc) {
            logmsg(LOGMSG_ERROR, "%s: del queue %s consumer %d berk rc %d\n",
                    __func__, bdb_state->name, consumer, rc);
            *bdberr = BDBERR_MISC;
            rc = -1;
            goto done;
        }
    }

    if (put_seq && bdb_state->persistent_seq) {
        DBT next_key = {0}, next_data = {0};
        next_key.flags = next_data.flags = DB_DBT_PARTIAL;

        /* Probe for another record */
        rc = dbcp->c_get(dbcp, &next_key, &next_data, DB_NEXT);

        /* If none add this sequence to meta */
        if (rc == DB_NOTFOUND) {
            struct bdb_queue_found_seq qfnd;
            uint8_t *p_buf = (uint8_t *)val.data;
            uint8_t *p_buf_end = p_buf + sizeof(struct bdb_queue_found_seq);
            p_buf = (uint8_t *)queue_found_seq_get(&qfnd, p_buf, p_buf_end);
            rc = put_queue_sequence(bdb_state->name, tran, qfnd.seq);
            if (rc == DB_LOCK_DEADLOCK) {
                *bdberr = BDBERR_DEADLOCK;
                rc = -1;
                goto done;
            } else if (rc) {
                logmsg(LOGMSG_ERROR,
                       "%s: del queue %s put-seq %lld berk rc "
                       "%d\n",
                       __func__, bdb_state->name, qfnd.seq, rc);
                *bdberr = BDBERR_MISC;
                rc = -1;
                goto done;
            }
        } else if (rc == DB_LOCK_DEADLOCK) {
            *bdberr = BDBERR_DEADLOCK;
            rc = -1;
            goto done;
        } else if (rc) {
            logmsg(LOGMSG_ERROR, "%s: del queue %s probe rc %d\n", __func__,
                   bdb_state->name, rc);
            *bdberr = BDBERR_MISC;
            rc = -1;
            goto done;
        }
    }
    bdb_state->qdb_cons++;

done:
    if (put_seq && bdb_state->persistent_seq && val.data)
        free(val.data);
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
    return rc;
}

int bdb_queuedb_consume(bdb_state_type *bdb_state, tran_type *tran,
                        int consumer, const struct bdb_queue_found *fnd,
                        int *bdberr)
{
    int rc = bdb_lock_table_read(bdb_state, tran);
    if (rc == DB_LOCK_DEADLOCK) {
        *bdberr = BDBERR_DEADLOCK;
        struct bdb_queue_priv *qstate = bdb_state->qpriv;
        qstate->stats.n_consume_deadlocks++;
        return -1;
    } else if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: queuedb %s error getting tablelock %d\n",
               __func__, bdb_state->name, rc);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    DB *db1 = BDB_QUEUEDB_GET_DBP_ZERO(bdb_state);
    DB *db2 = BDB_QUEUEDB_GET_DBP_ONE(bdb_state);
    int put_seq = (db2 != NULL) ? bdb_queuedb_is_db_empty(db2, tran) : 1;

    *bdberr = 0;

    rc = bdb_queuedb_consume_int(bdb_state, db1, tran, consumer,
                                 fnd, put_seq, bdberr);
    if ((rc == -1) && (*bdberr == BDBERR_DELNOTFOUND)) { /* EMPTY FILE #0? */
        if (db2 != NULL) {
            *bdberr = 0;

            rc = bdb_queuedb_consume_int(bdb_state, db2, tran, consumer,
                                         fnd, 1, bdberr);
        }
    }
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
