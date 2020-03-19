#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <strings.h>
#include <stddef.h>

#include <pthread.h>
#include <bb_stdint.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <inttypes.h>
#include <flibc.h>
#include "endian_core.h"
#include "bdb_cursor.h"
#include "bdb_int.h"
#include "locks.h"

#include "bdb_queue.h"
#include "bdb_queuedb.h"
#include "plbitlib.h"
#include "logmsg.h"

/* Another implementation of queues.  Don't really "trust" berkeley queues.
 * We've had some issues with
 * them that have proven difficult to reproduce (unreclaimed extents, queue
 * head/tail mismatching between
 * nodes, to name a couple).  So just use a plain old btree, which we know and
 * love. Implements the same API
 * as bdb/queue.c. */

/* These are the public APIs exposed by bdb/queue.c - implement the same
 * interface. */

/* TODO: this is in db-land, not bdb.... */
#define MAXCONSUMERS 32

extern void fsnapf(FILE *, void *, int);

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

void bdb_queuedb_init_priv(bdb_state_type *bdb_state)
{
    struct bdb_queue_priv *qstate;
    if (gbl_debug_queuedb)
        logmsg(LOGMSG_USER, ">>> bdb_queuedb_init_priv %s\n", bdb_state->name);
    bdb_state->qpriv = calloc(1, sizeof(struct bdb_queue_priv));
    qstate = bdb_state->qpriv;
    /* read max, use? use genids, with guarantee they'll never decrement? */
    qstate->genid = 0;
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
    struct queuedb_key k;
    unsigned long long genid;
    int rc;
    uint8_t ver = 0;
    DBT dbt_key = {0}, dbt_data = {0};
    DBC *dbcp = NULL;
    uint8_t key[QUEUEDB_KEY_LEN];
    uint8_t *p_buf, *p_buf_end;
    struct bdb_queue_found qfnd;
    struct bdb_queue_found_seq qfnd_seq, qfnd_fnd;
    void *databuf = NULL;
    void *freeme = NULL;
    struct bdb_queue_priv *qstate = bdb_state->qpriv;

    if (gbl_debug_queuedb)
        logmsg(LOGMSG_USER, ">>> bdb_queuedb_add %s\n", bdb_state->name);

    qstate = (struct bdb_queue_priv *)bdb_state->qpriv;
    databuf = malloc(dtalen + sizeof(struct bdb_queue_found_seq));

    /* TODO: rather than grabbing inline, minimize the time we hold this lock by
     * deferring queue-writes until after everything else in toblock is done.
     * Also force all transactions to acquire these locks in the same order so
     * that we can avoid deadlocks from two transactions which have both made
     * it to that point. */
    rc = bdb_lock_table_read(bdb_state, tran);
    if (rc == DB_LOCK_DEADLOCK) {
        *bdberr = BDBERR_DEADLOCK;
        qstate->stats.n_add_deadlocks++;
        rc = -1;
        goto done;
    } else if (rc != 0) {
        logmsg(LOGMSG_ERROR, "queuedb %s error getting tablelock %d\n",
               bdb_state->name, rc);
        *bdberr = BDBERR_MISC;
        rc = -1;
        goto done;
    }
    rc = bdb_state->dbp_data[0][0]->cursor(bdb_state->dbp_data[0][0], tran->tid,
                                           &dbcp, 0);
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
    rc = bdb_cget_unpack(bdb_state, dbcp, &dbt_key, &dbt_data, &ver,
                         DB_LAST | DB_RMW);

    if (rc == 0) {
        freeme = dbt_data.data;
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
        genid = qfnd_seq.genid = get_genid(bdb_state, 0);
        qfnd_seq.data_len = dtalen;
        qfnd_seq.data_offset = sizeof(struct bdb_queue_found_seq);
        qfnd_seq.trans.tid = tran->tid->txnid;
        qfnd_seq.unused = 0;
        if (tran->trigger_epoch) {
            qfnd_seq.epoch = tran->trigger_epoch;
        } else {
            qfnd_seq.epoch = tran->trigger_epoch = comdb2_time_epoch();
        }

        qfnd_fnd.seq = 0;

        if (rc == 0) {
            p_buf = dbt_data.data;
            p_buf_end = p_buf + dbt_data.size;
            p_buf = (uint8_t *)queue_found_seq_get(&qfnd_fnd, p_buf, p_buf_end);
            if (p_buf == NULL) {
                logmsg(LOGMSG_ERROR, "%s failed to decode prev seq for %s\n",
                       __func__, bdb_state->name);
                *bdberr = BDBERR_MISC;
                rc = -1;
                goto done;
            }
        }
        qfnd_seq.seq = (qfnd_fnd.seq + 1);

        dbt_key.flags = dbt_data.flags = 0;
        dbt_key.ulen = dbt_data.ulen = 0;

        p_buf = databuf;
        p_buf_end = p_buf + dtalen + sizeof(struct bdb_queue_found_seq);
        p_buf = queue_found_seq_put(&qfnd_seq, p_buf, p_buf_end);
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
            rc = bdb_cput_pack(bdb_state, 0, dbcp, &dbt_key, &dbt_data,
                               DB_KEYLAST);
            if (rc == DB_LOCK_DEADLOCK) {
                *bdberr = BDBERR_DEADLOCK;
                qstate->stats.n_add_deadlocks++;
                rc = -1;
                goto done;
            } else if (rc) {
                logmsg(LOGMSG_ERROR,
                       "queuedb %s consumer %d genid %lx put rc %d\n",
                       bdb_state->name, i, k.genid, rc);
                *bdberr = BDBERR_MISC;
                rc = -1;
                goto done;
            }
        }
    }
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
        dbcp = NULL;
    }
    if (databuf)
        free(databuf);
    if (freeme)
        free(freeme);
    return rc;
}

int bdb_queuedb_stats(bdb_state_type *bdb_state,
                      bdb_queue_stats_callback_t callback, void *userptr,
                      int *bdberr)
{
    DBT dbt_key = {0}, dbt_data = {0};
    DBC *dbcp = NULL;
    uint8_t ver = 0;
    size_t item_length = 0;
    unsigned int epoch = 0, first_seq = 0, last_seq = 0;
    struct bdb_queue_found_seq qfnd_seq;
    uint8_t *p_buf, *p_buf_end;
    int rc, consumern = 0;

    assert(bdb_state->ondisk_header);
    if (gbl_debug_queuedb)
        logmsg(LOGMSG_USER, ">>> bdb_queuedb_stats %s\n", bdb_state->name);

    dbt_key.flags = dbt_data.flags = DB_DBT_REALLOC;

    rc = bdb_state->dbp_data[0][0]->cursor(bdb_state->dbp_data[0][0], NULL,
                                           &dbcp, 0);

    if (rc != 0) {
        *bdberr = BDBERR_MISC;
        return -1;
    }

    rc = bdb_cget_unpack(bdb_state, dbcp, &dbt_key, &dbt_data, &ver, DB_FIRST);

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
            logmsg(LOGMSG_ERROR, "%s first berk rc %d\n", __func__, rc);
            *bdberr = BDBERR_MISC;
            assert(rc != 0);
            goto done;
        }
    }

    p_buf = dbt_data.data;
    p_buf_end = p_buf + dbt_data.size;
    p_buf = (uint8_t *)queue_found_seq_get(&qfnd_seq, p_buf, p_buf_end);
    first_seq = qfnd_seq.seq;
    epoch = qfnd_seq.epoch;
    item_length = dbt_data.size;

    rc = bdb_cget_unpack(bdb_state, dbcp, &dbt_key, &dbt_data, &ver, DB_LAST);

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
            logmsg(LOGMSG_ERROR, "%s last berk rc %d\n", __func__, rc);
            *bdberr = BDBERR_MISC;
            assert(rc != 0);
            goto done;
        }
    }

    p_buf = dbt_data.data;
    p_buf_end = p_buf + dbt_data.size;
    p_buf = (uint8_t *)queue_found_seq_get(&qfnd_seq, p_buf, p_buf_end);
    last_seq = qfnd_seq.seq;

    if (last_seq >= first_seq)
        callback(consumern, item_length, epoch, (last_seq - first_seq) + 1,
                 userptr);

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

int bdb_queuedb_walk(bdb_state_type *bdb_state, int flags, void *lastitem,
                     bdb_queue_walk_callback_t callback, void *userptr,
                     int *bdberr)
{
    DBT dbt_key = {0}, dbt_data = {0};
    DBC *dbcp = NULL;
    uint8_t ver = 0;
    int rc;

    if (gbl_debug_queuedb)
        logmsg(LOGMSG_USER, ">>> bdb_queuedb_walk %s\n", bdb_state->name);

    dbt_key.flags = dbt_data.flags = DB_DBT_REALLOC;

    /* this API is a little nutty... */
    rc = bdb_state->dbp_data[0][0]->cursor(bdb_state->dbp_data[0][0], NULL,
                                           &dbcp, 0);
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
    } else {
        rc = bdb_cget_unpack(bdb_state, dbcp, &dbt_key, &dbt_data, &ver,
                             DB_SET_RANGE);
    }
    while (rc == 0) {
        struct bdb_queue_found qfnd;
        struct bdb_queue_found_seq qfnd_seq;
        unsigned int epoch;
        int consumern = 0;
        uint8_t *p_buf, *p_buf_end;

        lastitem = (void *)dbt_key.data;

        p_buf = dbt_data.data;
        p_buf_end = p_buf + dbt_data.size;

        if (bdb_state->ondisk_header) {
            p_buf = (uint8_t *)queue_found_seq_get(&qfnd_seq, p_buf, p_buf_end);
            epoch = qfnd_seq.epoch;
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
            return -1;
        }
    }
done:
    if (dbcp) {
        int crc;
        crc = dbcp->c_close(dbcp);
        if (crc == DB_LOCK_DEADLOCK) {
            *bdberr = BDBERR_DEADLOCK;
            rc = -1;
            goto done;
        } else if (crc) {
            logmsg(LOGMSG_ERROR, "%s: c_close berk rc %d\n", __func__, crc);
            *bdberr = BDBERR_MISC;
            rc = -1;
            goto done;
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

    if (bdb_state->dbp_data[0][0] == NULL) { // trigger dropped?
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    struct queuedb_key k;
    DBT dbt_key = {0}, dbt_data = {0};
    DBC *dbcp = NULL;
    uint8_t ver = 0;
    size_t data_offset;
    int rc;
    struct bdb_queue_found qfnd;
    struct bdb_queue_found_seq qfnd_seq;
    uint8_t *p_buf, *p_buf_end;
    uint8_t key[QUEUEDB_KEY_LEN] = {0};
    struct queuedb_key fndk;
    uint8_t fndkey[QUEUEDB_KEY_LEN] = {0};
    struct bdb_queue_priv *qstate = bdb_state->qpriv;

    if (gbl_debug_queuedb)
       logmsg(LOGMSG_USER, ">> bdb_queuedb_get %s\n", bdb_state->name);

    dbt_key.flags = dbt_data.flags = DB_DBT_REALLOC;

    rc = bdb_state->dbp_data[0][0]->cursor(bdb_state->dbp_data[0][0], NULL,
                                           &dbcp, 0);
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
        logmsg(LOGMSG_USER, "next key is consumer %d genid %016lx\n",
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
        p_buf = (uint8_t *)queue_found_seq_get(&qfnd_seq, p_buf, p_buf_end);
        data_offset = qfnd_seq.data_offset;
    } else {
        p_buf = (uint8_t *)queue_found_get(&qfnd, p_buf, p_buf_end);
        data_offset = qfnd.data_offset;
    }
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
            data_offset; /* This length will be used to check version. */
    if (fndcursor) {
        memcpy(fndcursor->genid, &fndk.genid, sizeof(fndk.genid));
        fndcursor->recno = 0;
        fndcursor->reserved = 0;
    }
    dbt_data.data = NULL;
    *bdberr = BDBERR_NOERROR;
    rc = 0;

done:
    if (dbt_key.data && dbt_key.data != key /*this puppy isn't malloced*/)
        free(dbt_key.data);
    if (dbt_data.data)
        free(dbt_data.data);
    if (dbcp) {
        int crc;
        crc = dbcp->c_close(dbcp);
        if (crc) {
            if (crc == DB_LOCK_DEADLOCK) {
                *bdberr = DB_LOCK_DEADLOCK;
                rc = -1;
            }
            /* TODO: log any other error, shouldn't happen */
        }
    }

    return rc;
}

int bdb_queuedb_consume(bdb_state_type *bdb_state, tran_type *tran,
                        int consumer, const void *prevfnd, int *bdberr)
{
    struct bdb_queue_found qfnd;
    struct bdb_queue_found_seq qfnd_seq;
    unsigned long long genid = 0;
    uint8_t *p_buf, *p_buf_end;
    uint8_t ver = 0;
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

    if (bdb_state->ondisk_header) {
        p_buf = (uint8_t *)prevfnd;
        p_buf_end = p_buf + sizeof(struct bdb_queue_found_seq);
        p_buf = (uint8_t *)queue_found_seq_get(&qfnd_seq, p_buf, p_buf_end);
        genid = qfnd_seq.genid;

    } else {
        p_buf = (uint8_t *)prevfnd;
        p_buf_end = p_buf + sizeof(struct bdb_queue_found);
        p_buf = (uint8_t *)queue_found_get(&qfnd, p_buf, p_buf_end);
        genid = qfnd.genid;
    }
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
        logmsg(LOGMSG_USER, "consumer %d genid %016llx\n", consumer, genid);
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

    rc = bdb_state->dbp_data[0][0]->cursor(bdb_state->dbp_data[0][0], tran->tid,
                                           &dbcp, 0);
    if (rc != 0) {
        *bdberr = BDBERR_MISC;
        goto done;
    }
    rc = bdb_cget_unpack(bdb_state, dbcp, &dbt_key, &dbt_data, &ver, DB_FIRST);
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

    if ((fndk.genid != genid) && (fndk.genid != flibc_ntohll(genid))) {
        logmsg(LOGMSG_ERROR,
               "%s: Trying to consume non-first item of queue %s genid: "
               "%016llx first: %016lx consumer %d\n",
               __func__, bdb_state->name, genid, fndk.genid, consumer);
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
    if (gbl_debug_queuedb)
        logmsg(LOGMSG_USER, ">> CONSUMED!\n");

/* and we consumed successfully */
done:
    if (dbcp) {
        int crc;
        crc = dbcp->c_close(dbcp);
        if (crc == DB_LOCK_DEADLOCK) {
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
                          pthread_mutex_t **lock, const uint8_t **open)
{
    DB_ENV *dbenv = bdb_state->dbenv;
    return dbenv->trigger_subscribe(dbenv, bdb_state->name, cond, lock, open);
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
