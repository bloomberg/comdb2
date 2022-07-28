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

/*
 * Transaction code. 1172 1325 1372 1440 1581 1659 1698
 */

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stddef.h>

#include <build/db.h>
#include <epochlib.h>
#include <lockmacros.h>

#include <ctrace.h>

#include <net.h>
#include "bdb_cursor.h"
#include "bdb_int.h"
#include "locks.h"
#include "locks_wrap.h"
#include "bdb_osqltrn.h"
#include "bdb_osqlcur.h"

#include "llog_auto.h"
#include "llog_ext.h"
#include "missing.h"
#include <alloca.h>

#include <assert.h>

#include "nodemap.h"
#include "logmsg.h"
#include "txn_properties.h"
#include <build/db.h>

static unsigned int curtran_counter = 0;
extern int gbl_debug_txn_sleep;
extern int __txn_getpriority(DB_TXN *txnp, int *priority);

#if 0
int __lock_dump_region_lockerid __P((DB_ENV *, const char *, FILE *, u_int32_t lockerid));
#endif

int bdb_tran_clear_request_ack(tran_type *tran)
{
    tran->request_ack = 0;
    return 0;
}

int bdb_tran_set_request_ack(tran_type *tran)
{
    tran->request_ack = 1;
    return 0;
}

tran_type *bdb_tran_begin_logical_norowlocks_int(bdb_state_type *bdb_state,
                                                 unsigned long long tranid,
                                                 int trak, int *bdberr)
{
    tran_type *tran;
    int rc;
    unsigned int flags = 0;

    /* One day this will change.  One day.
       We really want a bdb_env_type and a bdb_table_type.
       bdb_state_type is making me feel all stabby. */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    *bdberr = BDBERR_NOERROR;

    tran = mymalloc(sizeof(tran_type));
    bzero(tran, sizeof(tran_type));

    tran->tranclass = TRANCLASS_LOGICAL_NOROWLOCKS;
    tran->threadid = pthread_self();
    tran->wrote_begin_record = 0;
    tran->committed_begin_record = 0;
    tran->get_schema_lock = 0;

    if (tranid == 0)
        tran->logical_tranid = get_id(bdb_state);
    else {
        tran->logical_tranid = tranid;
    }

    /* LSN_NOT_LOGGED */
    tran->last_logical_lsn.file = 0;
    tran->last_logical_lsn.offset = 1;

    if (!bdb_state->attr->synctransactions)
        flags |= DB_TXN_NOSYNC;

    rc = bdb_state->dbenv->txn_begin(bdb_state->dbenv, NULL, &tran->tid, flags);
    if (rc) {
        logmsg(LOGMSG_ERROR, "can't start locker transaction, tid %016llx\n",
                tran->logical_tranid);
        /* we can retry this from the top level if need be */
        *bdberr = BDBERR_DEADLOCK;
        myfree(tran);
        return NULL;
    }

    return tran;
}

extern int gbl_update_startlsn_printstep;
int maxstep = 0;

void comdb2_cheapstack(FILE *f);

/* Update the startlsn of an outstanding logical transaction - you are holding
 * the lock*/
int bdb_update_startlsn_lk(bdb_state_type *bdb_state, struct tran_tag *intran,
                           DB_LSN *firstlsn)
{
    static int lastpr = 0;
    int countstep = 0, now;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    struct tran_tag *ltran = NULL, *tmp = NULL;
    int inserted = 0;

    /* Remove from the transactions list */
    listc_rfl(&bdb_state->logical_transactions_list, intran);

    /* Update the startlsn */
    intran->startlsn = *firstlsn;

    /* Add this in startlsn order.  This is cheap: in the non-recovery case
     * we're guaranteed that these will be added in order, so they'll break
     * out of this loop after the first comparison.  */
    LISTC_FOR_EACH_SAFE(&bdb_state->logical_transactions_list, ltran, tmp,
                        tranlist_lnk)
    {
        countstep++;
        if (log_compare(&intran->startlsn, &ltran->startlsn) <= 0 ||
            ltran->startlsn.file == 0) {
            listc_add_before(&bdb_state->logical_transactions_list, intran,
                             ltran);
            inserted = 1;
            break;
        }
    }

    if (gbl_update_startlsn_printstep) {
        if (countstep > maxstep) {
            maxstep = countstep;

            if ((now = comdb2_time_epoch()) - lastpr) {
                logmsg(LOGMSG_USER, "Maxstep was %d\n", maxstep);
                comdb2_cheapstack(stderr);
                maxstep = 0;
                lastpr = now;
            }
        }
    }

    /* Make this the last element */
    if (!inserted) {
        listc_abl(&bdb_state->logical_transactions_list, intran);
    }

    /* Grab top lsn */
    DB_LSN *top = &bdb_state->logical_transactions_list.top->startlsn;

    /* Assert that this didn't go backwards */
    if (bdb_state->lwm.file && 0 == bdb_state->in_recovery) {
        /* This can happen on the replicant if the commit records are processed
         * out of order.  I don't think this matters .. */
        /*
        assert(log_compare(top, &bdb_state->lwm) >= 0);
        */
    }

    /* Update lwm while under lock. */
    if (top->file) {
        bdb_state->lwm.file = top->file;
        bdb_state->lwm.offset = top->offset;
    }

    return 0;
}

/* This allows the scdone 'rep-transaction' to gather locks using gbl_rep_lockid
 */
void bdb_set_tran_lockerid(tran_type *tran, uint32_t lockerid)
{
    if (tran->tranclass == TRANCLASS_LOGICAL) {
        tran->logical_lid = lockerid;
    } else {
        tran->tid->txnid = lockerid;
    }
}

void bdb_get_tran_lockerid(tran_type *tran, uint32_t *lockerid)
{
    if (tran->tranclass == TRANCLASS_LOGICAL) {
        (*lockerid) = tran->logical_lid;
    } else {
        (*lockerid) = tran->tid->txnid;
    }
}

void *bdb_get_physical_tran(tran_type *ltran)
{
    assert(ltran->tranclass == TRANCLASS_LOGICAL);
    return ltran->physical_tran;
}

void bdb_reset_physical_tran(tran_type *ltran)
{
    assert(ltran->tranclass == TRANCLASS_LOGICAL);
    ltran->physical_tran = NULL;
}

void *bdb_get_sc_parent_tran(tran_type *ltran)
{
    assert(ltran->tranclass == TRANCLASS_LOGICAL);
    return ltran->sc_parent_tran;
}

void bdb_ltran_get_schema_lock(tran_type *ltran)
{
    ltran->get_schema_lock = 1;
    ltran->single_physical_transaction = 1;
    ltran->schema_change_txn = 1;
}

void bdb_ltran_put_schema_lock(tran_type *ltran)
{
    ltran->get_schema_lock = 0;
}

/* Create a txn and add to the txn list.  Called directly from the replication
 * stream.  */
static tran_type *bdb_start_ltran(bdb_state_type *bdb_state,
                                  unsigned long long ltranid, void *firstlsn)
{
    tran_type *tran;
    int bdberr = 0;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

#if DEBUG_TXN_LIST
    LOCK(&bdb_state->translist_lk)
    {

        /* Should not be in the hash */
        assert(NULL ==
               hash_find(bdb_state->logical_transactions_hash, &ltranid));
    }
    UNLOCK(&bdb_state->translist_lk);
#endif

    tran = bdb_tran_start_logical(bdb_state, ltranid, 0, &bdberr);

    if (firstlsn)
        bdb_update_startlsn(tran, firstlsn);

    return tran;
}

/* Logical start function to register with berkley replication */
int berkdb_start_logical(DB_ENV *dbenv, void *state, uint64_t ltranid,
                         DB_LSN *lsn)
{
    tran_type *txn;
    bdb_state_type *bdb_state;

    bdb_state = (bdb_state_type *)state;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

#ifdef DEBUG_TXN_LIST
    LOCK(&bdb_state->translist_lk)
    {
        txn = hash_find(bdb_state->logical_transactions_hash, &ltranid);
    }
    UNLOCK(&bdb_state->translist_lk);

    assert(!txn);
#endif

    txn = bdb_start_ltran(bdb_state, ltranid, lsn);
    return txn ? 0 : -1;
}

int logical_commit_replicant(bdb_state_type *bdb_state,
                             unsigned long long ltranid);

/* Logical commit function to register with berkley replication */
int berkdb_commit_logical(DB_ENV *dbenv, void *state, uint64_t ltranid,
                          DB_LSN *lsn)
{
    int rc;
    bdb_state_type *bdb_state;

    bdb_state = (bdb_state_type *)state;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

#ifdef DEBUG_TXN_LIST
    tran_type *txn;
    LOCK(&bdb_state->translist_lk)
    {
        txn = hash_find(bdb_state->logical_transactions_hash, &ltranid);
    }
    UNLOCK(&bdb_state->translist_lk);

    assert(txn);
#endif

    rc = logical_commit_replicant(bdb_state, ltranid);

    return rc;
}

/* Release all of the locks held by this logical transaction */
int bdb_release_ltran_locks(bdb_state_type *bdb_state, struct tran_tag *ltran,
                            int lockerid)
{
    DB_LOCKREQ rq = {0};
    int rc;

    if (!lockerid) {
        assert(bdb_state->in_recovery);
        return 0;
    }

    /* Release all rowlocks */
    rq.op = DB_LOCK_PUT_ALL;

    /* Release all the locks */
    rc =
        bdb_state->dbenv->lock_vec(bdb_state->dbenv, lockerid, 0, &rq, 1, NULL);

    /* Shouldn't fail */
    assert(0 == rc);
    if (rc)
        logmsg(LOGMSG_WARN, "%s:%d rc = %d\n", __func__, __LINE__, rc);

    /* Make sure this is NULL */
    assert(NULL == ltran->tid);

    /* Release my locker id */
    rc = bdb_state->dbenv->lock_id_free(bdb_state->dbenv, lockerid);

    /* Shouldn't fail */
    assert(0 == rc);
    if (rc)
        logmsg(LOGMSG_WARN, "%s:%d rc = %d\n", __func__, __LINE__, rc);

    /* Invalid lockerid */
    ltran->logical_lid = 0;

    return 0;
}

/* Aborts anything waiting on locks held by the logical transactions.  Used
 * before blocking on the bdb writelock so that the replication thread doesn't
 * deadlock on a reader which is blocked on a lock that it holds. */
int bdb_abort_logical_waiters(bdb_state_type *bdb_state)
{
    struct tran_tag *ltran = NULL, *tmp = NULL;
    int rc;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    LOCK(&bdb_state->translist_lk)
    {

        LISTC_FOR_EACH_SAFE(&bdb_state->logical_transactions_list, ltran, tmp,
                            tranlist_lnk)
        {
            if (ltran->logical_lid) {
                rc = bdb_state->dbenv->lock_abort_waiters(bdb_state->dbenv, ltran->logical_lid, DB_LOCK_ABORT_LOGICAL);
                if (rc)
                    abort();
            }
        }
    }
    UNLOCK(&bdb_state->translist_lk);
    return 0;
}

/* Purge the logical txn list of any startlsn's equal to or greater than
 * trunclsn.
 * This is for the rep_verify truncate case.  */
int bdb_purge_logical_transactions(void *statearg, DB_LSN *trunclsn)
{
    /* Release rowlocks for each aborted logical transaction.  The pages
     * should be rolled back already. */
    bdb_state_type *bdb_state = (bdb_state_type *)statearg;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    LOCK(&bdb_state->translist_lk)
    {

        struct tran_tag *ltran = NULL, *tmp = NULL;

        LISTC_FOR_EACH_SAFE(&bdb_state->logical_transactions_list, ltran, tmp,
                            tranlist_lnk)
        {
            if (ltran->startlsn.file > 0 &&
                log_compare(trunclsn, &ltran->startlsn) < 0) {
                /* Remove from hash */
                hash_del(bdb_state->logical_transactions_hash, ltran);

                /* Remove from list */
                listc_rfl(&bdb_state->logical_transactions_list, ltran);

                pool_free(ltran->rc_pool);
                myfree(ltran->rc_list);
                myfree(ltran->rc_locks);

#if DEBUG_TXN_LIST
                ctrace("Removing logical_tranid 0x%llx func %s line %d - "
                       "ltran->startlsn is %d:%d, trunclsn is %d:%d\n",
                       ltran->logical_tranid, __func__, __LINE__,
                       ltran->startlsn.file, ltran->startlsn.offset,
                       trunclsn->file, trunclsn->offset);
#endif

                /* Release locks */
                bdb_release_ltran_locks(bdb_state, ltran, ltran->logical_lid);

                /* Free this */
                free(ltran);
            }
        }

        /* Don't have to touch the lwm: if it was removed the list is empty */
    }
    UNLOCK(&bdb_state->translist_lk);

    return 0;
}

void print_logical_commits_starts(FILE *f);

int bdb_dump_logical_tranlist(void *state, FILE *f)
{
    bdb_state_type *bdb_state = (bdb_state_type *)state;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    LOCK(&bdb_state->translist_lk)
    {

        struct tran_tag *ltran = NULL, *tmp = NULL;

        LISTC_FOR_EACH_SAFE(&bdb_state->logical_transactions_list, ltran, tmp,
                            tranlist_lnk)
        {
            logmsg(LOGMSG_USER, "ltranid: %llx startlsn: %d:%d lllsn: %d:%d\n",
                    ltran->logical_tranid, ltran->startlsn.file,
                    ltran->startlsn.offset, ltran->last_logical_lsn.file,
                    ltran->last_logical_lsn.offset);
        }
    }
    UNLOCK(&bdb_state->translist_lk);

    print_logical_commits_starts(f);

    return 0;
}

/* Update the startlsn from berkeley */
int bdb_update_startlwm_berk(void *statearg, unsigned long long ltranid,
                             DB_LSN *firstlsn)
{
    bdb_state_type *bdb_state = (bdb_state_type *)statearg;
    tran_type *ltran;
    int rc;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    LOCK(&bdb_state->translist_lk)
    {

        /* Find it in my hash */
        ltran = hash_find(bdb_state->logical_transactions_hash, &ltranid);

        /* Should always find this */
        assert(ltran);

        /* Now reorder */
        rc = bdb_update_startlsn_lk(bdb_state, ltran, firstlsn);
    }
    UNLOCK(&bdb_state->translist_lk);

    return rc;
}

/* Made to be called in berkeley land */
int bdb_update_startlsn(struct tran_tag *intran, DB_LSN *firstlsn)
{
    bdb_state_type *bdb_state = intran->parent_state;
    int rc;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    LOCK(&bdb_state->translist_lk)
    {
        rc = bdb_update_startlsn_lk(bdb_state, intran, firstlsn);
    }
    UNLOCK(&bdb_state->translist_lk);
    return rc;
}

int tran_reset_rowlist(tran_type *tran)
{
    pool_relall(tran->rc_pool);
    tran->rc_count = 0;
    return (0);
}

/* We don't need the top 'N' locks that we've just allocated */
int tran_deallocate_pop(tran_type *tran, int count)
{
    assert(tran->rc_count >= count);

    while (count--) {
        pool_relablk(tran->rc_pool, tran->rc_list[--tran->rc_count]);
    }
    return (0);
}

int tran_allocate_rlptr(tran_type *tran, DBT **rlptr, DB_LOCK **rlock)
{
    int ret = 0, step = tran->parent_state->attr->rllist_step;
    step = step > 0 ? step : 10;
    if (tran->rc_count >= tran->rc_max) {
        void *new_rc_list, *new_rc_locks;

        new_rc_list =
            myrealloc(tran->rc_list, sizeof(DBT *) * (tran->rc_max + step));

        if (new_rc_list == NULL) {
            logmsg(LOGMSG_FATAL, "%s line %d out of memory.\n", __func__, __LINE__);
            abort();
        }

        new_rc_locks =
            myrealloc(tran->rc_locks, sizeof(DB_LOCK) * (tran->rc_max + step));

        if (new_rc_locks == NULL) {
            logmsg(LOGMSG_FATAL, "%s line %d out of memory.\n", __func__, __LINE__);
            abort();
        }

        tran->rc_list = new_rc_list;
        tran->rc_locks = new_rc_locks;
        tran->rc_max += step;
    }

    if (!(tran->rc_list[tran->rc_count] = pool_getablk(tran->rc_pool))) {
        logmsg(LOGMSG_FATAL, "%s line %d out of memory.\n", __func__, __LINE__);
        abort();
    }

    tran->rc_list[tran->rc_count]->data =
        ((u_int8_t *)tran->rc_list[tran->rc_count]) + sizeof(DBT);
    tran->rc_list[tran->rc_count]->size = 0;

    (*rlock) = &tran->rc_locks[tran->rc_count];
    (*rlptr) = tran->rc_list[tran->rc_count++];

    return (ret);
}

static tran_type *bdb_tran_begin_logical_int(bdb_state_type *bdb_state,
                                             unsigned long long logical_tranid,
                                             int trak, int got_bdb_lock,
                                             int reptxn, uint32_t logical_lid,
                                             int *bdberr)
{
    tran_type *tran;
    int step;
    int ismaster;

    /* One day this will change.  One day.
       We really want a bdb_env_type and a bdb_table_type.
       bdb_state_type is making me feel all stabby. */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    *bdberr = BDBERR_NOERROR;

    tran = mymalloc(sizeof(tran_type));
    bzero(tran, sizeof(tran_type));

    tran->tranclass = TRANCLASS_LOGICAL;
    tran->threadid = pthread_self();
    tran->wrote_begin_record = 0;
    tran->committed_begin_record = 0;
    tran->get_schema_lock = 0;
    tran->is_about_to_commit = 0;
    tran->micro_commit =
        gbl_rowlocks ? bdb_state->attr->rowlocks_micro_commit : 0;

    tran->logical_tranid = logical_tranid;

    /* LSN_NOT_LOGGED */
    tran->last_logical_lsn.file = 0;
    tran->last_logical_lsn.offset = 1;

    tran->last_physical_commit_lsn.file = 0;
    tran->last_physical_commit_lsn.offset = 1;

    tran->last_regop_lsn.file = 0;
    tran->last_regop_lsn.offset = 1;

    /* Start at 0 */
    tran->startlsn.file = 0;
    tran->startlsn.offset = 1;

    tran->reptxn = reptxn;

    tran->logical_lid = logical_lid; /* 0 for replication */

    tran->is_rowlocks_trans = 1;
    tran->is_about_to_commit = 0;
    tran->master = 1; /* logical transactions don't have subtransactions */
    tran->trak = trak;

    tran->last_logical_lsn.file = 0;
    tran->last_logical_lsn.offset = 1;
    tran->parent_state = bdb_state;

    /* Put this tran in seqnum_info->key */
    Pthread_setspecific(bdb_state->seqnum_info->key, tran);

    tran->got_bdb_lock = got_bdb_lock;

    ismaster = (bdb_state->repinfo->myhost == bdb_state->repinfo->master_host);

    if (ismaster) {
        step = bdb_state->attr->rllist_step > 0 ? bdb_state->attr->rllist_step
                                                : 10;
        tran->rc_pool = pool_setalloc_init(sizeof(DBT) + MINMAX_KEY_SIZE, step,
                                           malloc, free);
        tran->rc_list = mymalloc(sizeof(DBT *) * step);
        tran->rc_locks = mymalloc(sizeof(DB_LOCK) * step);
        tran->rc_max = step;
        tran->rc_count = 0;
    }

    /* Unset startlsn's are added to the back of this list */
    LOCK(&bdb_state->translist_lk)
    {
        listc_abl(&bdb_state->logical_transactions_list, tran);
        hash_add(bdb_state->logical_transactions_hash, tran);
#if defined DEBUG_TXN_LIST
        ctrace("Adding logical_tranid 0x%llx func %s line %d\n",
               tran->logical_tranid, __func__, __LINE__);
#endif
    }
    UNLOCK(&bdb_state->translist_lk);

    if (tran->trak)
        logmsg(LOGMSG_USER, "TRK_TRAN: began transaction %p (logical, class=%d)\n",
                tran, tran->tranclass);

    return tran;
}

#define GET_TRANID(tranid) (tranid) ? (tranid) : get_id(bdb_state)

extern int gbl_extended_sql_debug_trace;

int bdb_tran_get_start_file_offset(bdb_state_type *bdb_state, tran_type *tran,
                                   int *file, int *offset)
{
    if (gbl_new_snapisol_asof) {
        if (tran && tran->asof_lsn.file) {
            *file = tran->asof_lsn.file;
            *offset = tran->asof_lsn.offset;
            if (gbl_extended_sql_debug_trace) {
                logmsg(LOGMSG_USER, "%s line %d using asof lsn [%d][%d]\n",
                       __func__, __LINE__, *file, *offset);
            }
            return 0;
        } else if (tran && tran->birth_lsn.file) {
            *file = tran->birth_lsn.file;
            *offset = tran->birth_lsn.offset;
            if (gbl_extended_sql_debug_trace) {
                logmsg(LOGMSG_USER, "%s line %d using birth lsn [%d][%d]\n",
                       __func__, __LINE__, *file, *offset);
            }
            return 0;
        } else
            return -1;
    } else {
        if (tran && tran->snapy_commit_lsn.file) {
            *file = tran->snapy_commit_lsn.file;
            *offset = tran->snapy_commit_lsn.offset;
            if (gbl_extended_sql_debug_trace) {
                logmsg(LOGMSG_USER,
                       "%s line %d using snappy-commit lsn [%d][%d]\n",
                       __func__, __LINE__, *file, *offset);
            }
            return 0;
        } 
        else
        {
            // If we are not in a transaction, return nothing
            if (tran) {
                bdb_get_current_lsn(bdb_state, (unsigned int *)file,
                                    (unsigned int *)offset);
                if (gbl_extended_sql_debug_trace) {
                    logmsg(LOGMSG_USER, "%s line %d using current lsn[%d][%d], tran is %p\n", 
                            __func__, __LINE__, *file, *offset, tran);
                }
            }
            else {
                *file = 0;
                *offset = 0;
                if (gbl_extended_sql_debug_trace) {
                    logmsg(LOGMSG_USER, "%s line %d using ZERO lsn[%d][%d], tran is NULL\n", 
                            __func__, __LINE__, *file, *offset);
                }


            }
            return 0;
        }
    }
    return -1;
}

/* only should be called from phys.c */
tran_type *bdb_tran_begin_phys(bdb_state_type *bdb_state,
                               tran_type *logical_tran)
{
    int rc, flags = 0;
    extern int gbl_locks_check_waiters, gbl_rowlocks_commit_on_waiters;
    tran_type *tran;

    if (logical_tran->tranclass != TRANCLASS_LOGICAL) {
        logmsg(LOGMSG_FATAL, "Tried to start a physical transaction with a "
                        "non-logical parent\n");
        abort();
    }
    if (logical_tran->physical_tran) {
        logmsg(LOGMSG_FATAL, "Tried to start a physical transaction with another "
                        "physical transaction active\n");
        abort();
    }

    tran = mymalloc(sizeof(tran_type));
    bzero(tran, sizeof(tran_type));

    logical_tran->physical_tran = tran;
    tran->tranclass = TRANCLASS_PHYSICAL;
    tran->threadid = pthread_self();
    tran->logical_tran = logical_tran;

    if (!bdb_state->attr->synctransactions)
        flags |= DB_TXN_NOSYNC;

    rc = bdb_state->dbenv->txn_begin(bdb_state->dbenv, NULL, &tran->tid, flags);
    if (rc) {
        logmsg(LOGMSG_ERROR, 
                "Can't start physical transaction for ltranid %016llx: rc %d\n",
                logical_tran->logical_tranid, rc);
        free(tran);
        return NULL;
    }

    /* Make the logical tran the 'parent' for deadlock detection purposes (but
     * this will commit independantly). */
    if (!logical_tran->micro_commit ||
        (gbl_locks_check_waiters && gbl_rowlocks_commit_on_waiters)) {
        rc = bdb_state->dbenv->lock_add_child_locker(
            bdb_state->dbenv, logical_tran->logical_lid, tran->tid->txnid);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Error setting physical child locker for logical txn\n");
            tran->tid->abort(tran->tid);
            free(tran);
            return NULL;
        }
    }

    /* Set the 'logical_abort' flag in the physical transactions lockerid */
    /* XXX the set_abort_flag_in_locker check is for debugging.  This code
     * should always be run */
    if (!bdb_state->in_recovery && logical_tran->aborted &&
        bdb_state->attr->set_abort_flag_in_locker) {
        rc = bdb_state->dbenv->lock_id_set_logical_abort(
            bdb_state->dbenv, logical_tran->logical_lid);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Error setting logical-abort flag in the physical "
                            "transaction\n");
            tran->tid->abort(tran->tid);
            free(tran);
            return NULL;
        }
    }

    if (tran->trak)
        logmsg(LOGMSG_USER, "TRK_TRAN: began transaction %p (physical class=%d) "
                        "for %p (logical, class=%d)\n",
                tran, tran->tranclass, logical_tran, logical_tran->tranclass);

    return tran;
}

static inline unsigned long long lag_bytes(bdb_state_type *bdb_state)
{
    const char *connlist[REPMAX];
    int count, i;
    char *master_host = bdb_state->repinfo->master_host;
    uint64_t lagbytes;
    DB_LSN minlsn, masterlsn;

    /* MAX_LSN */
    minlsn.file = minlsn.offset = 0xffffffff;

    count = net_get_all_nodes_connected(bdb_state->repinfo->netinfo, connlist);

    if (count == 0)
        return 0;

    for (i = 0; i < count; i++) {
        if (!is_incoherent(bdb_state, connlist[i]) &&
            log_compare(
                &bdb_state->seqnum_info->seqnums[nodeix(connlist[i])].lsn,
                &minlsn) < 0) {
            minlsn = bdb_state->seqnum_info->seqnums[nodeix(connlist[i])].lsn;
        }
    }

    masterlsn = bdb_state->seqnum_info->seqnums[nodeix(master_host)].lsn;
    if (log_compare(&minlsn, &masterlsn) >= 0)
        return 0;

    lagbytes = subtract_lsn(bdb_state, &masterlsn, &minlsn);

    return lagbytes;
}

static int bdb_tran_commit_phys_getlsn_flags(bdb_state_type *bdb_state,
                                             tran_type *tran, DB_LSN *inlsn,
                                             int flags)
{
    extern int gbl_replicate_rowlocks;
    DB_LSN llsn, *lsn = (NULL == inlsn ? &llsn : inlsn);
    DBT rldbt = {0};
    int rc_count, rc, iirc;

    if (tran->logical_tran->physical_tran != tran) {
        logmsg(LOGMSG_FATAL, "Transaction mismatch: expected 0x%p got 0x%p\n", tran,
                tran->logical_tran->physical_tran);
        abort();
    }

    if (!tran->logical_tran->committed_begin_record)
        flags |= DB_TXN_LOGICAL_BEGIN;

    if (tran->logical_tran->is_about_to_commit)
        flags |= DB_TXN_LOGICAL_COMMIT;

    if (tran->logical_tran->get_schema_lock)
        flags |= DB_TXN_SCHEMA_LOCK;

    tran->logical_tran->committed_begin_record = 1;
    tran->logical_tran->physical_tran = NULL;

    if (!bdb_state->attr->synctransactions)
        flags |= DB_TXN_NOSYNC;

    if (!gbl_replicate_rowlocks) {
        rldbt.data = NULL;
        rldbt.size = 0;
        rc_count = 0;
    } else {
        /* This is only called for a logical commit or abort.  Pass in the
         * logical
         * transaction so that the replication code knows to flush it. */
        rldbt.data = tran->logical_tran->rc_list;
        rldbt.size = sizeof(DBT *) * tran->logical_tran->rc_count;
        rc_count = tran->logical_tran->rc_count;
    }

    bdb_osql_trn_repo_lock();
    iirc = update_shadows_beforecommit(
        bdb_state, &tran->logical_tran->last_logical_lsn, NULL, 1);
    bdb_osql_trn_repo_unlock();
    if (iirc) {
        logmsg(LOGMSG_ERROR, "%s:update_shadows_beforecommit returns %d\n", __func__,
                iirc);
        return -1;
    }

    /* Update last committed logical lsn */
    tran->logical_tran->last_physical_commit_lsn =
        tran->logical_tran->last_logical_lsn;

    assert(lsn == &tran->logical_tran->last_regop_lsn);

    /* A little terrible - it serializes commits on this lock ..
     * maybe pass in a flag instead?  There's a race here if we
     * don't lock: this can attempt to update-pagelogs for a
     * shadow-trans that is exiting .. */
    /* XXX This doesn't seem to be locking .. XXX */
    u_int64_t logbytes = 0;
    rc = tran->tid->commit_rowlocks(tran->tid, flags, tran->logical_tran->logical_tranid,
                                    tran->logical_tran->logical_lid, &tran->logical_tran->last_regop_lsn, &rldbt,
                                    tran->logical_tran->rc_locks, rc_count, &logbytes, &tran->logical_tran->begin_lsn,
                                    lsn, tran->logical_tran);

    tran_reset_rowlist(tran->logical_tran);

    if (lsn->file && flags & DB_TXN_REP_ACK) {
        int timeoutms = -1;
        seqnum_type seqnum = {{0}};
        memcpy(&seqnum.lsn, lsn, sizeof(*lsn));
        bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &seqnum.generation);
        bdb_wait_for_seqnum_from_all_adaptive_newcoh(bdb_state, &seqnum, 0,
                                                     &timeoutms);
    }

    tran->logbytes += logbytes;

    if (tran->trak)
        logmsg(LOGMSG_USER, "TRK_TRAN: committed transaction %p (physical "
                        "class=%d) for %p (logical, class=%d)\n",
                tran, tran->tranclass, tran->logical_tran,
                tran->logical_tran->tranclass);

    if (tran->table_version_cache)
        free(tran->table_version_cache);
    tran->table_version_cache = NULL;

    free(tran);
    return rc;
}

int bdb_tran_commit_phys_getlsn(bdb_state_type *bdb_state, tran_type *tran,
                                DB_LSN *lsn)
{
    return bdb_tran_commit_phys_getlsn_flags(bdb_state, tran, lsn,
                                             DB_TXN_REP_ACK);
}

int bdb_tran_commit_phys(bdb_state_type *bdb_state, tran_type *tran)
{
    return bdb_tran_commit_phys_getlsn_flags(
        bdb_state, tran, &tran->logical_tran->last_regop_lsn, 0);
}

static int bdb_tran_abort_phys_int(bdb_state_type *bdb_state, tran_type *tran,
                                   int reset_rowlist)
{
    int rc;
    if (tran->logical_tran->physical_tran != tran) {
        logmsg(LOGMSG_FATAL, "Transaction mismatch: expected 0x%p got 0x%p\n", tran,
                tran->logical_tran->physical_tran);
        abort();
    }
    tran->logical_tran->physical_tran = NULL;

    tran->logical_tran->last_logical_lsn =
        tran->logical_tran->last_physical_commit_lsn;
    rc = tran->tid->abort(tran->tid);

    if (tran->trak)
        logmsg(LOGMSG_USER, "TRK_TRAN: aborted transaction %p (physical class=%d) "
                        "for %p (logical, class=%d)\n",
                tran, tran->tranclass, tran->logical_tran,
                tran->logical_tran->tranclass);

    if (reset_rowlist)
        tran_reset_rowlist(tran->logical_tran);

    if (tran->table_version_cache)
        free(tran->table_version_cache);
    tran->table_version_cache = NULL;
    free(tran);
    return rc;
}

int bdb_tran_abort_phys_retry(bdb_state_type *bdb_state, tran_type *tran)
{
    return bdb_tran_abort_phys_int(bdb_state, tran, 0);
}

int bdb_tran_abort_phys(bdb_state_type *bdb_state, tran_type *tran)
{
    return bdb_tran_abort_phys_int(bdb_state, tran, 1);
}

static tran_type *bdb_tran_begin_ll_int(bdb_state_type *bdb_state,
                                        tran_type *parent, struct txn_properties *prop, 
                                        int tranclass, int *bdberr,
                                        u_int32_t inflags)
{
    if (gbl_debug_txn_sleep)
        sleep(gbl_debug_txn_sleep);

    tran_type *tran;
    int rc;
    DB_TXN *parent_tid;
    unsigned int flags;

    /*fprintf(stderr, "calling bdb_tran_begin_ll_int\n");*/

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    *bdberr = BDBERR_NOERROR;

    tran = mymalloc(sizeof(tran_type));
    bzero(tran, sizeof(tran_type));

    tran->tranclass = tranclass;
    tran->threadid = pthread_self();

    tran->usrptr = 0;
    int setThdTran = 0; /* was tran saved into thread local data? */

    /* comdb2 coding style:  "if parent" means "i am a child" */
    if (parent) {
        parent_tid = parent->tid;
        tran->master = 0;
        tran->parent = parent;

        parent->committed_child = 0; /* reset this, there are children */
    } else {
        parent_tid = NULL;

        tran->committed_child =
            1; /* set this by default for the parent trans */
        tran->master = 1;

        Pthread_setspecific(bdb_state->seqnum_info->key, tran);
        setThdTran = 1;

        /*fprintf(stderr, "Pthread_setspecific %x to %x\n", bdb_state, tran);*/
        tran->startlsn.file = 0;
        tran->startlsn.offset = 1;
    }

    /*fprintf(stderr, "beginning transaction\n");*/

    /* begin transaction */
    flags = 0;
    if (!bdb_state->attr->synctransactions)
        flags |= DB_TXN_NOSYNC;

    if (inflags & BDB_TRAN_RECOVERY)
        flags |= DB_TXN_RECOVERY;

    tran->flags |= (inflags & (BDB_TRAN_NOLOG | BDB_TRAN_RECOVERY));

    switch (tran->tranclass) {
    case TRANCLASS_SNAPISOL:
    case TRANCLASS_SERIALIZABLE:
        /* THIS HAS TO BE DONE AT THE MOMENT OF REGISTRATION 
           tran->startgenid = bdb_get_commit_genid(bdb_state); */ /*get_gblcontext(
                                                                 bdb_state);*/
        break;

    case TRANCLASS_SOSQL:
    case TRANCLASS_READCOMMITTED:
        break;

    case TRANCLASS_PHYSICAL:
    case TRANCLASS_BERK:
        /*fprintf(stderr, "calling txn_begin_with_prop\n");*/

        rc = bdb_state->dbenv->txn_begin_with_prop(
            bdb_state->dbenv, parent_tid, &(tran->tid), flags, prop);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "begin transaction failed\n");
            *bdberr = BDBERR_DEADLOCK;
            myfree(tran);
            if (setThdTran){
                Pthread_setspecific(bdb_state->seqnum_info->key, NULL);
                setThdTran = 0;
            }
            return NULL;
        } else {
            if (!parent) {
                tran->startlsn = tran->tid->we_start_at_this_lsn;
            }
        }

        break;

    default:
        abort();
        break;
    }

    tran->aborted = 0;
    tran->last_logical_lsn.offset = 1;

    if (tran->trak)
        logmsg(LOGMSG_USER, "TRK_TRAN: began transaction %p n", tran);

    return tran;
}

static tran_type *bdb_tran_begin_berk_int(bdb_state_type *bdb_state,
                                          tran_type *parent, struct txn_properties *prop,
                                          int *bdberr, u_int32_t flags)
{
    tran_type *tran;

    tran = bdb_tran_begin_ll_int(bdb_state, parent, prop, TRANCLASS_BERK,
                                 bdberr, flags);

    return tran;
}

tran_type *bdb_tran_begin_shadow_int(bdb_state_type *bdb_state, int tranclass,
                                     int trak, int *bdberr, int epoch, int file,
                                     int offset, int is_ha_retry)
{
    tran_type *tran;
    int rc = 0;

#if 0
   SINCE SHADOW TRANSACTIONS (READ COMMITTED/SNAPSHOT/SERIALIZABLE)
   OWN NO BERKDB TRANSACTION, IT IS SAFE TO AVOID GETTING
   AN ADDITIONAL BDB READ LOCK HERE.
   EACH INDIVIDUAL SQL QUERY REMAINS PROTECTED BY A BDB 
   READ LOCK IN CURTRAN.
   THIS ALSO MAKE USER TRANSACTION SURVIVE MASTER SWINGS
   WITH NO IMPACT.
   THE DOWNSIDE IS THAT WE WILL HAVE TO BE CAREFULL TO ROLLBACK
   THE SHADOW TRANSACTION IF NODE GOES DOWN (NO WAIT AFTER IT?)
#endif

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    tran = bdb_tran_begin_ll_int(bdb_state, NULL, NULL, tranclass, bdberr, 0);

    /* we do this for query isolation in socksql with row caching;
       snapshot/serializable will set this again while holding the trn_repo
       mutex when session is registered. */
    tran->startgenid = bdb_get_commit_genid_generation(
        bdb_state, &tran->snapy_commit_lsn, &tran->snapy_commit_generation);
    if (tran->snapy_commit_lsn.file == 0) {
        tran->startgenid = bdb_get_current_lsn(
            bdb_state, &(tran->birth_lsn.file), &(tran->birth_lsn.offset));
    } else {
        tran->birth_lsn = tran->snapy_commit_lsn;
    }

    tran->pglogs_hashtbl = hash_init_o(PGLOGS_KEY_OFFSET, PAGE_KEY_SIZE);

    tran->relinks_hashtbl =
        hash_init_o(PGLOGS_RELINK_KEY_OFFSET, PAGE_KEY_SIZE);

    Pthread_mutex_init(&tran->pglogs_mutex, NULL);

    tran->asof_lsn.file = 0;
    tran->asof_lsn.offset = 1;
    tran->asof_ref_lsn.file = 0;
    tran->asof_ref_lsn.offset = 1;
    tran->asof_hashtbl = NULL;

    if (tran) {
        tran->trak = trak;

        if (tran->tranclass == TRANCLASS_SNAPISOL ||
            tran->tranclass == TRANCLASS_SERIALIZABLE) {
            rc = bdb_osql_cache_table_versions(bdb_state, tran, trak, bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR,
                       "%s failed to cache table versions rc=%d bdberr=%d\n",
                       __func__, rc, *bdberr);
            }

            /* register transaction so we start receiving log undos */
            tran->osql =
                bdb_osql_trn_register(bdb_state, tran, trak, bdberr, epoch,
                                      file, offset, is_ha_retry);
            if (!tran->osql) {
                if (*bdberr != BDBERR_NOT_DURABLE)
                    logmsg(LOGMSG_ERROR, "%s %d\n", __func__, *bdberr);

                myfree(tran);
                return NULL;
            }

            listc_init(&tran->open_cursors,
                       offsetof(struct bdb_cursor_ifn, lnk));
        }
    }

    return tran;
}

tran_type *bdb_tran_begin_logical(bdb_state_type *bdb_state, int trak,
                                  int *bdberr)
{
    uint32_t logical_lid = 0;
    int tranid = 0;
    unsigned long long logical_tranid = GET_TRANID(tranid);
    int rc;

    /* Note: don't get the bdb read lock here.  The only berkeley
       resources we hold are locks */

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    rc = bdb_state->dbenv->lock_id(bdb_state->dbenv, &logical_lid);
    if (rc) {
        logmsg(LOGMSG_ERROR, "can't acquire a locker id, tid %016llx\n",
               logical_tranid);
        *bdberr = BDBERR_DEADLOCK;
        return NULL;
    }

    BDB_READLOCK("trans_start_logical");

    int ismaster =
        (bdb_state->repinfo->myhost == bdb_state->repinfo->master_host);

    /* If we're getting the lock, this has to be the master
     * NOTE: we don't release this lock until commit/rollback time
     */
    if (!ismaster) {
        BDB_RELLOCK();
        logmsg(LOGMSG_ERROR, "Master change while getting logical tran.\n");
        bdb_state->dbenv->lock_id_free(bdb_state->dbenv, logical_lid);
        *bdberr = BDBERR_READONLY;
        return NULL;
    }

    return bdb_tran_begin_logical_int(bdb_state, logical_tranid, trak, 1, 0,
                                      logical_lid, bdberr);
}

static tran_type *bdb_tran_begin_pp(bdb_state_type *bdb_state,
                                    tran_type *parent,
                                    struct txn_properties *prop, int *bdberr,
                                    u_int32_t flags)
{
    tran_type *tran;

    BDB_READLOCK("bdb_tran_begin");

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    tran = bdb_tran_begin_berk_int(bdb_state, parent, prop, bdberr, flags);
    /* NOTE: we don't release this lock until commit/rollback time */
    if (!tran) {
        BDB_RELLOCK();
    }

    return tran;
}

tran_type *bdb_tran_start_logical(bdb_state_type *bdb_state,
                                  unsigned long long tranid, int trak,
                                  int *bdberr)
{
    tran_type *tran;
    unsigned long long logical_tranid = GET_TRANID(tranid);
    tran = bdb_tran_begin_logical_int(bdb_state, logical_tranid, trak, 0, 1, 0,
                                      bdberr);
    return tran;
}

/* This routine is called only on the master, and only during recovery. It's
   needed to undo logical transactions, so that any compensating log records
   are written using the same transaction id as the original transaction. */
tran_type *bdb_tran_continue_logical(bdb_state_type *bdb_state,
                                     unsigned long long tranid, int trak,
                                     int *bdberr)
{
    tran_type *tran;
    unsigned long long logical_tranid = GET_TRANID(tranid);
    tran = bdb_tran_begin_logical_int(bdb_state, logical_tranid, trak, 0, 0, 0,
                                      bdberr);
    return tran;
}

tran_type *bdb_tran_begin_internal(bdb_state_type *bdb_state, tran_type *parent,
                          int *bdberr, const char *func, int line)
{
    tran_type *tran;
#if DEBUG_RECOVERY_LOCK
    logmsg(LOGMSG_USER, "%s called from %s:%d\n", __func__, func, line);
#endif
    tran = bdb_tran_begin_pp(bdb_state, parent, NULL, bdberr, 0);
    return tran;
}

tran_type *bdb_tran_begin_flags(bdb_state_type *bdb_state, tran_type *parent,
                                int *bdberr, u_int32_t flags)
{
    tran_type *tran;
    tran = bdb_tran_begin_pp(bdb_state, parent, NULL, bdberr, flags);
    return tran;
}

tran_type *bdb_tran_begin_set_prop(bdb_state_type *bdb_state,
                                      tran_type *parent, struct txn_properties *prop,
                                      int *bdberr)
{
    tran_type *tran;
    tran = bdb_tran_begin_pp(bdb_state, parent, prop, bdberr, 0);
    return tran;
}

tran_type *bdb_tran_begin_readcommitted(bdb_state_type *bdb_state, int trak,
                                        int *bdberr)
{
    return bdb_tran_begin_shadow_int(bdb_state, TRANCLASS_READCOMMITTED, trak,
                                     bdberr, 0, 0, 0, 0);
}

tran_type *bdb_tran_begin_socksql(bdb_state_type *bdb_state, int trak,
                                  int *bdberr)
{
    return bdb_tran_begin_shadow_int(bdb_state, TRANCLASS_SOSQL, trak, bdberr,
                                     0, 0, 0, 0);
}

tran_type *bdb_tran_begin_snapisol(bdb_state_type *bdb_state, int trak,
                                   int *bdberr, int epoch, int file, int offset,
                                   int is_ha_retry)
{
    return bdb_tran_begin_shadow_int(bdb_state, TRANCLASS_SNAPISOL, trak,
                                     bdberr, epoch, file, offset, is_ha_retry);
}

tran_type *bdb_tran_begin_serializable(bdb_state_type *bdb_state, int trak,
                                       int *bdberr, int epoch, int file,
                                       int offset, int is_ha_retry)
{
    return bdb_tran_begin_shadow_int(bdb_state, TRANCLASS_SERIALIZABLE, trak,
                                     bdberr, epoch, file, offset, is_ha_retry);
}

/*
  this function is interwoven with replication stuff.
  i wish it werent.
*/
extern int gbl_rowlocks;

int llog_ltran_commit_log_wrap(DB_ENV *dbenv, DB_TXN *txnid, DB_LSN *ret_lsnp,
                               u_int32_t flags, u_int64_t ltranid,
                               DB_LSN *prevllsn, u_int64_t gblcontext,
                               short isabort);

void return_pglogs_queue_cursor(struct pglogs_queue_cursor *c);

int free_pglogs_queue_cursors(void *obj, void *arg)
{
    return_pglogs_queue_cursor(obj);
    return 0;
}

void abort_at_exit(void) 
{
    abort();
}

static int update_logical_redo_lsn(void *obj, void *arg)
{
    bdb_state_type *bdb_state = (bdb_state_type *)obj;
    tran_type *tran = (tran_type *)arg;
    if (bdb_state->logical_live_sc == 0)
        return 0;
    struct sc_redo_lsn *last = NULL;
    struct sc_redo_lsn *redo = malloc(sizeof(struct sc_redo_lsn));
    if (redo == NULL) {
        logmsg(LOGMSG_FATAL, "%s: failed to malloc sc redo\n", __func__);
        abort();
    }
    redo->lsn = tran->last_logical_lsn;
    redo->txnid = tran->tid->txnid;
    /* We must have table lock here and so the list will not go away */
    Pthread_mutex_lock(&bdb_state->sc_redo_lk);

    last = LISTC_BOT(&bdb_state->sc_redo_list);
    /* Add in order */
    /* 'Logical commits' can be out-of-order for prepared txns */
    if (!last || (log_compare(&last->lsn, &redo->lsn) <= 0) || tran->is_prepared)
        listc_abl(&bdb_state->sc_redo_list, redo);
    else {
        logmsg(LOGMSG_FATAL, "%s: logical commit lsn should be in order\n",
               __func__);
        abort();
    }

    Pthread_cond_signal(&bdb_state->sc_redo_wait);
    Pthread_mutex_unlock(&bdb_state->sc_redo_lk);
    return 0;
}

int bdb_tran_prepare(bdb_state_type *bdb_state, tran_type *tran, const char *dist_txnid, const char *coordinator_name,
                     const char *coordinator_tier, uint32_t coordinator_gen, void *blkseq_key, int blkseq_key_len,
                     int *bdberr)
{
    u_int32_t flags = (DB_TXN_DONT_GET_REPO_MTX | (tran->request_ack) ? DB_TXN_REP_ACK : 0);
    *bdberr = BDBERR_NOERROR;
    DBT blkseq = {.data = blkseq_key, .size = blkseq_key_len};
    extern int gbl_utxnid_log;

    if (gbl_utxnid_log == 0) {
        logmsg(LOGMSG_DEBUG, "%s requires utxnid_log\n", __func__);
        return -1;
    }

    if (tran->tranclass != TRANCLASS_BERK) {
        logmsg(LOGMSG_FATAL, "%s preparing incorrect tranclass: %d\n", __func__, tran->tranclass);
        abort();
    }

    if (tran->parent != NULL) {
        logmsg(LOGMSG_FATAL, "%s cannot prepare child txns\n", __func__);
        abort();
    }

    if (tran->tid->parent) {
        logmsg(LOGMSG_FATAL, "%s preparing child transaction\n", __func__);
        abort();
    }

    if (tran->is_prepared) {
        logmsg(LOGMSG_FATAL, "%s re-preparing an already prepared txn\n", __func__);
        abort();
    }

    if ((add_snapisol_logging(bdb_state, tran) || tran->force_logical_commit) && !(tran->flags & BDB_TRAN_NOLOG)) {
        unsigned long long ctx = get_gblcontext(bdb_state);
        int rc, isabort = (tran->committed_child) ? 0 : 1;
        rc = llog_ltran_commit_log_wrap(bdb_state->dbenv, tran->tid, &tran->commit_lsn, 0, tran->logical_tranid,
                                        &tran->last_logical_lsn, ctx, isabort);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "%s error writing logical commit, %d\n", __func__, rc);
            abort();
        }
        memcpy(&tran->last_logical_lsn, &tran->commit_lsn, sizeof(DB_LSN));
        flags |= DB_TXN_DIST_UPD_SHADOWS;
    }

    int prepare_rc = tran->tid->dist_prepare(tran->tid, dist_txnid, coordinator_name, coordinator_tier, coordinator_gen,
                                             &blkseq, flags);

    if (prepare_rc != 0) {
        logmsg(LOGMSG_INFO, "%s error preparing txn: %d\n", __func__, prepare_rc);
    }
    tran->is_prepared = 1;

    return prepare_rc;
}

int bdb_tran_commit_with_seqnum_int(bdb_state_type *bdb_state, tran_type *tran,
                                    seqnum_type *seqnum, int *bdberr,
                                    int getseqnum, uint64_t *out_txnsize,
                                    void *blkseq, int blklen, void *blkkey,
                                    int blkkeylen)
{
    int rc = 0, outrc = 0;
    unsigned int flags;
    int needed_to_abort = 0;
    int set_seqnum = 0;
    uint32_t generation = 0;
    tran_type *physical_tran = NULL;
    DB_LSN lsn;
    DB_LSN old_lsn;

    bzero(&lsn, sizeof(DB_LSN));
    bzero(&old_lsn, sizeof(DB_LSN));

    if (seqnum)
        bzero(seqnum, sizeof(seqnum_type));

    if (out_txnsize)
        *out_txnsize = 0;

    *bdberr = BDBERR_NOERROR;

    bdb_state->repinfo->repstats.commits++;

#if 0
   if(tran->tranclass == TRANCLASS_LOGICAL)
   {
        /* Dump all of my locks */
        fprintf(stderr, "Tid %d func %s locks:\n", pthread_self(), __func__);
        char parm[2] = {0};
        parm[0] = 'L';
        __lock_dump_region_lockerid(bdb_state->dbenv, parm, stderr, tran->tid->txnid);
        fprintf(stderr, "\n");

   }
#endif

    /*fprintf(stderr, "commiting\n");*/

    bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &generation);

    switch (tran->tranclass) {

    case TRANCLASS_LOGICAL_NOROWLOCKS:
        flags = (tran->request_ack) ? DB_TXN_REP_ACK : 0;
        rc = tran->tid->commit_getlsn(tran->tid, flags, out_txnsize, &lsn, tran);
        if (rc != 0) {
            *bdberr = BDBERR_MISC;
            outrc = -1;
            goto cleanup;
        }
        break;

    case TRANCLASS_SNAPISOL:
    case TRANCLASS_SERIALIZABLE:
        rc = bdb_osql_trn_unregister(tran->osql, bdberr);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s %d %d\n", __func__, rc, *bdberr);
    /* fallthrough */
    case TRANCLASS_SOSQL:
    case TRANCLASS_READCOMMITTED:
        bdb_tran_free_shadows(bdb_state, tran);
        break;

    case TRANCLASS_PHYSICAL:
    case TRANCLASS_BERK:

        /* XXX it's rumoured that berk screws up and does sync
           despite telling it to open the env with nosync,
           unless you tell it once again to do
           nosync here.  easy enough to be triple explicit about it
           either way.*/
        flags = 0;
        if (!bdb_state->attr->synctransactions)
            flags |= DB_TXN_NOSYNC;

        bdb_osql_trn_repo_lock();

        /* only generate a log for PARENT transactions */
        if (tran->parent == NULL &&
            (add_snapisol_logging(bdb_state, tran) ||
             tran->force_logical_commit) &&
            !(tran->flags & BDB_TRAN_NOLOG)) {
            tran_type *parent = (tran->parent) ? tran->parent : tran; /*nop*/
            int iirc = 0;
            int isabort;
            unsigned long long ctx = get_gblcontext(bdb_state);

            /* If the child didn't commit, this is a logical abort. */
            isabort = (tran->committed_child) ? 0 : 1;

            if (!tran->is_prepared) {
                iirc = llog_ltran_commit_log_wrap(bdb_state->dbenv, tran->tid, &parent->commit_lsn, 0,
                                                  parent->logical_tranid, &parent->last_logical_lsn, ctx, isabort);
                memcpy(&parent->last_logical_lsn, &parent->commit_lsn, sizeof(DB_LSN));
            }

            if (iirc) {
                tran->tid->abort(tran->tid);
                bdb_osql_trn_repo_unlock();
                logmsg(LOGMSG_ERROR, "%s:%d td %p failed to log logical commit, rc %d\n", __func__, __LINE__,
                       (void *)pthread_self(), iirc);
                *bdberr = BDBERR_MISC;
                outrc = -1;
                goto cleanup;
            }

            if (!isabort) {
                /* hookup locals one too */
                iirc = update_shadows_beforecommit(
                    bdb_state, &tran->last_logical_lsn, NULL, 1);
            }

            if (iirc) {
                tran->tid->abort(tran->tid);
                bdb_osql_trn_repo_unlock();
                logmsg(LOGMSG_ERROR, 
                        "%s:update_shadows_beforecommit nonblocking rc %d\n",
                        __func__, rc);
                *bdberr = rc;
                outrc = -1;
                goto cleanup;
            }

            if (!isabort && tran->committed_child &&
                tran->force_logical_commit && tran->dirty_table_hash) {
                hash_for(tran->dirty_table_hash, update_logical_redo_lsn, tran);
                hash_clear(tran->dirty_table_hash);
                hash_free(tran->dirty_table_hash);
                tran->dirty_table_hash = NULL;
            }
        }

        /* "normal" case for physical transactions. just commit */
        flags = DB_TXN_DONT_GET_REPO_MTX;
        flags |= (tran->request_ack) ? DB_TXN_REP_ACK : 0;
        rc = tran->tid->commit_getlsn(tran->tid, flags, out_txnsize, &lsn, tran);
        bdb_osql_trn_repo_unlock();
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, 
                   "%s:%d failed commit_getlsn, rc %d\n", __func__,
                   __LINE__, rc);
            *bdberr = BDBERR_MISC;
            outrc = -1;
            goto cleanup;
        } else {
            /* successful physical commit, lets increment our seqnum */
            Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));
            /* dont let our global lsn go backwards */
            memcpy(&old_lsn,
                   &(bdb_state->seqnum_info
                         ->seqnums[nodeix(bdb_state->repinfo->myhost)]),
                   sizeof(DB_LSN));

            if (log_compare(&lsn, &old_lsn) > 0) {
                /*fprintf(stderr, "%s:%d 2 updating my seqnum to %d:%d\n",
                  __func__, __LINE__, lsn.file, lsn.offset);*/

                // TODO not sure if this is necessary anymore 
                // I should be setting this from a hook in log-put
                memcpy(&(bdb_state->seqnum_info
                             ->seqnums[nodeix(bdb_state->repinfo->myhost)]),
                       &lsn, sizeof(DB_LSN));
                bdb_state->seqnum_info
                    ->seqnums[nodeix(bdb_state->repinfo->myhost)]
                    .generation = generation;
            }
            Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));
        }

        /* Set the 'committed-child' flag if this is not the parent. */
        if (tran->parent != NULL) {
            tran->parent->committed_child = 1;
        }

        break;

    case TRANCLASS_LOGICAL:

        /* if we were called by abort code, abort the transaction first.
           remember that we aborted it, since the commit case may try to
           abort it again, and we don't want that (though it won't do any
           harm) */

        if (tran->aborted) {
            needed_to_abort = 1;

            if (tran->schema_change_txn && tran->sc_parent_tran) {
                tran->physical_tran = tran->sc_parent_tran;
                tran->sc_parent_tran = NULL;
                tran->get_schema_lock = 0;
                tran->schema_change_txn = 0; // done
            }

            if (tran->physical_tran) {
                bdb_tran_abort_phys(bdb_state, tran->physical_tran);
            }

            /* lock-repo here */
            rc = abort_logical_transaction(
                bdb_state, tran, &tran->last_regop_lsn, blkseq ? 0 : 1);
            /* unlock-repo here */
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s:%d abort_logical_transaction rc %d\n", __FILE__,
                       __LINE__, rc);
                outrc = -1;
                goto cleanup;
            }
            lsn = tran->last_regop_lsn;
        } else if (tran->schema_change_txn && tran->sc_parent_tran) {
            rc = bdb_tran_commit(bdb_state, tran->physical_tran, bdberr);
            if (rc) {
                logmsg(
                    LOGMSG_ERROR,
                    "%s:%d failed to commit physical tran rc %d, bdberr %d\n",
                    __func__, __LINE__, rc, *bdberr);
                outrc = -1;
                goto cleanup;
            }
            tran->physical_tran = tran->sc_parent_tran;
            tran->sc_parent_tran = NULL;
            tran->schema_change_txn = 0; // done
        }

        /* we need to start a transaction, write a commit log record,
           and commit the transaction. we may also need to write a fstblk
           record if we were given one. */

        if (!needed_to_abort || blkseq) {
            rc = get_physical_transaction(bdb_state, tran, &physical_tran, 0);
            if (!physical_tran) {
                logmsg(LOGMSG_FATAL, 
                        "%s %d error getting physical transaction, %d\n",
                        __FILE__, __LINE__, rc);
                abort();
            }
        }

        if (blkseq) {
            *bdberr = 0;
            rc = bdb_blkseq_insert(bdb_state, physical_tran, blkkey, blkkeylen, blkseq, blklen, NULL, NULL, 0);
            *bdberr = (rc == IX_DUP) ? BDBERR_ADD_DUPE : rc;

            /*
               I am in bdb_tran_commit_logical_with_seqnum_size.
               I just tried to add a fstblk record under a brand new
               transaction, and may have failed.  I need to commit no-matter
               what.  If I try to add the blkseq record and get a dupe,
               abort the transaction (thus writing an abort record, also
               known as a commit record).  If I get a deadlock here,
               I can just abort the tran->tid transaction, and try again.
               Any other error and I am seriously screwed.
               If the record adds successfully, fall through and commit.
               Any of these steps can get me a deadlock, including the abort.
               Any deadlocks that can happen during the commit/abort can be
               retried in the lower layer routines, since I am not holding
               a berkeley transaction open.
               As an extra bonus feature we could have been called by abort
               code.  The only thing that happens differently in that case
               is that we need abort the logical transaction, even if the
               above is not a dupe.

               I may have seriously lost some brain cells processing all this.
            */
            if (*bdberr == BDBERR_ADD_DUPE) {
                /* why do we abort this transaction?
                   because abort_logical_transaction will need to start
                   more transactions (in the same thread) to undo
                   this transaction and we can self-deadlock.
                */
                bdb_tran_abort_phys(bdb_state, physical_tran);
                physical_tran = NULL;
                outrc = IX_DUP;

                /* abort if we haven't already */
                if (!needed_to_abort) {
                    rc = abort_logical_transaction(bdb_state, tran,
                                                   &tran->last_regop_lsn, 1);
                    if (rc) {
                       logmsg(LOGMSG_FATAL, "%s:%d abort_logical_transaction rc %d\n",
                               __FILE__, __LINE__, rc);
                        abort();
                    }
                    lsn = tran->last_regop_lsn;

                    bdb_release_ltran_locks(bdb_state, tran, tran->logical_lid);

                    /* Do i really need to do anything else?
                     * I've already aborted the logical txn (& written
                     * the abort record). */
                    needed_to_abort = 1;
                    goto cleanup;
                }

                /* We've already aborted but failed with blkseq.  Get a physical
                 * transaction to write the commit record. */
                rc = get_physical_transaction(bdb_state, tran, &physical_tran,
                                              0);
                if (!physical_tran) {
                    logmsg(LOGMSG_FATAL, 
                            "%s %d error getting physical transaction, %d\n",
                            __FILE__, __LINE__, rc);
                    abort();
                }
            } else if (*bdberr == BDBERR_DEADLOCK) {
                /* shouldn't happen for private blkseq */

                bdb_tran_abort_phys(bdb_state, physical_tran);
                physical_tran = NULL;
                /* I need to return internal_retry */

                if (!needed_to_abort) {
                    rc = abort_logical_transaction(bdb_state, tran,
                                                   &tran->last_regop_lsn, 1);
                    if (rc) {
                       logmsg(LOGMSG_ERROR, "%s:%d abort_logical_transaction rc %d\n",
                               __FILE__, __LINE__, rc);
                    }
                    lsn = tran->last_regop_lsn;
                }
                outrc = -1;
                needed_to_abort = 1;
            } else if (*bdberr) {
                /* really shouldn't get here */
                bdb_tran_abort_phys(bdb_state, physical_tran);
                bdb_release_ltran_locks(bdb_state, tran, tran->logical_lid);
                logmsg(LOGMSG_ERROR, "%s:%d bdb_lite_add rc %d bdberr=%d\n", __FILE__,
                       __LINE__, rc, *bdberr);
                abort();
                outrc = -1;
                goto cleanup;
            }
            /*
              if we get here we added the blkseq record successfully
              or got a dupe and successfully aborted. in either case,
              just need to write a final commit record.
            */
        }

        /* log end of transaction */
        if (physical_tran != NULL && tran->wrote_begin_record &&
            bdb_state->repinfo->myhost == bdb_state->repinfo->master_host) {
            /* set flag telling the send routine to flush this lsn immediately
             */

            assert(physical_tran != NULL);

            physical_tran->last_logical_lsn = tran->last_logical_lsn;

            // and here ..
            tran->is_about_to_commit = 1;
            rc = bdb_llog_commit(bdb_state, physical_tran, needed_to_abort);
            if (rc) {
                bdb_tran_abort_phys(bdb_state, physical_tran);
                rc =
                    bdb_release_ltran_locks(bdb_state, tran, tran->logical_lid);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "%s:%d abort rc %d\n", __FILE__, __LINE__, rc);
                    *bdberr = BDBERR_DEADLOCK;
                    outrc = -1;
                    goto cleanup;
                }
                tran->tid = NULL;
                *bdberr = BDBERR_DEADLOCK;
                outrc = -1;
                goto cleanup;
            }
        }

/* Don't think I need this - can just abort the logical transaction
   instead of commiting it.  The only problem is that I need to make
   sure the final physical transaction that writes the log record
   is physical.  I think I just need to set the master flags on it. */
#if 0
        tran->tid->dont_log_locks(tran->tid);
#endif
        /*
           We are about to commit a logical transaction;
           This will release a bunch of logical locks and
           will make visible changes to snapshot/serial transaction
           Maintain the snapshot/serializable isolation by updating
           the shadow data for each snapshot/serializable sql session
         */
        if (!needed_to_abort && tran->wrote_begin_record &&
            bdb_state->repinfo->myhost == bdb_state->repinfo->master_host &&
            !bdb_state->attr->shadows_nonblocking) {

            bdb_osql_trn_repo_lock();

            /* printf("update_shadows_beforecommit tranid %016llx lsn %u:%u\n",
                     tran->logical_tranid, tran->last_logical_lsn.file,
               tran->last_logical_lsn.offset); */
            rc = update_shadows_beforecommit(bdb_state, &tran->last_logical_lsn,
                                             NULL, 1);

            bdb_osql_trn_repo_unlock();

            if (rc) {
                /* TODO: to we need to abort here??? */
                logmsg(LOGMSG_ERROR, "%s:update_shadows_beforecommit rc %d\n",
                        __func__, rc);
                *bdberr = rc;
                outrc = -1;
                goto cleanup;
            }
        }

        if (physical_tran) {
            physical_tran->master = 1;
            tran->is_about_to_commit = 1;
            rc = bdb_tran_commit_phys_getlsn(
                bdb_state, physical_tran,
                &physical_tran->logical_tran->last_regop_lsn);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s:%d bdb_tran_commit_phys rc %d\n", __FILE__,
                        __LINE__, rc);
                *bdberr = rc;
                outrc = -1;
                goto cleanup;
            }
            lsn = tran->last_regop_lsn;
        }

        assert(NULL == tran->tid);

        /* Release the locks, free this lockerid */
        rc = bdb_release_ltran_locks(bdb_state, tran, tran->logical_lid);
        assert(!rc);

        if (seqnum) {
            memcpy(seqnum, &lsn, sizeof(DB_LSN));
            seqnum->generation = generation;
            set_seqnum = 1;

            Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));
            /* dont let our global lsn go backwards */
            memcpy(&old_lsn,
                   &(bdb_state->seqnum_info
                         ->seqnums[nodeix(bdb_state->repinfo->myhost)]),
                   sizeof(DB_LSN));

            if (log_compare(&lsn, &old_lsn) > 0) {
                /*fprintf(stderr, "%s:%d 2 updating my seqnum to %d:%d\n",
                  __func__, __LINE__, lsn.file, lsn.offset);*/
                memcpy(&(bdb_state->seqnum_info
                             ->seqnums[nodeix(bdb_state->repinfo->myhost)]),
                       &lsn, sizeof(DB_LSN));
                bdb_state->seqnum_info
                    ->seqnums[nodeix(bdb_state->repinfo->myhost)]
                    .generation = generation;
            }
            Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));
        }

        break;

    /* Huh? */
    default:
        abort();
    }

    /* we're done if we werent told to get the seqnum */
    if (!getseqnum) {
        outrc = 0;
        goto cleanup;
    }

    if (seqnum && !set_seqnum) {
        bzero(seqnum, sizeof(seqnum_type));
    }

    /* if we have no cluster, get the seqnum from the logfile */
    if (!is_real_netinfo(bdb_state->repinfo->netinfo)) {
        DB_LSN our_lsn;
        DB_LOG_STAT *log_stats;

        /* XXX this continues to be retarted.  there has to be a lighter weight
           way to get the lsn */
        bdb_state->dbenv->log_stat(bdb_state->dbenv, &log_stats, 0);
        make_lsn(&our_lsn, log_stats->st_cur_file, log_stats->st_cur_offset);
        free(log_stats);

        if (seqnum) {
            memcpy(seqnum, &our_lsn, sizeof(DB_LSN));
            seqnum->generation = generation;
        }

        outrc = 0;
        goto cleanup;
    }

    if (tran->master) {
        /* form the seqnum for the caller */
        if (set_seqnum) {

            /*
            if(log_compare(&lsn, &tran->savelsn) != 0)
            {
                fprintf(stderr, "Commit lsn is %d:%d but tran->savelsn is
            %d:%d?\n",
                        lsn.file, lsn.offset, tran->savelsn.file,
            tran->savelsn.offset);
            }
            */
        }

        else if (seqnum) {
            bzero(seqnum, sizeof(seqnum_type));
            // TODO: NC: copy lsn instead of tran->savelsn instead?
            memcpy(seqnum, &(tran->savelsn), sizeof(DB_LSN));
            seqnum->generation = generation;
        }

        if (out_txnsize && gbl_rowlocks) {
            *out_txnsize = tran->logbytes;
        }
    }

    outrc = 0;

cleanup:

    if (TRANCLASS_LOGICAL == tran->tranclass) {
        LOCK(&bdb_state->translist_lk)
        {
            listc_rfl(&bdb_state->logical_transactions_list, tran);

            if (listc_size(&bdb_state->logical_transactions_list) > 0) {
                DB_LSN *top =
                    &bdb_state->logical_transactions_list.top->startlsn;

                if (top->file > 0) {
                    bdb_state->lwm.file =
                        bdb_state->logical_transactions_list.top->startlsn.file;
                    bdb_state->lwm.offset = bdb_state->logical_transactions_list
                                                .top->startlsn.offset;
                }
            }

            hash_del(bdb_state->logical_transactions_hash, tran);

#if defined DEBUG_TXN_LIST
            ctrace("Removing logical_tranid 0x%llx func %s line %d\n",
                   tran->logical_tranid, __func__, __LINE__);
#endif
        }
        UNLOCK(&bdb_state->translist_lk);

        /* Master can change - just release the lock if I have it */
        if (tran->got_bdb_lock) {
            BDB_RELLOCK();
        }
    }

    /* if we are the master and we free tran we need to get rid of the stored
     * reference so functions like berkdb_send_rtn don't try to use it */
    if (tran->master) {
        Pthread_setspecific(bdb_state->seqnum_info->key, NULL);
    }

    if (tran->trak)
        logmsg(LOGMSG_USER, "TRK_TRAN: committed %p (type=%d)\n", tran,
                tran->tranclass);

    if (tran->table_version_cache)
        free(tran->table_version_cache);
    tran->table_version_cache = NULL;

    pool_free(tran->rc_pool);
    myfree(tran->rc_list);
    myfree(tran->rc_locks);

    if (tran->pglogs_queue_hash) {
        hash_for(tran->pglogs_queue_hash, free_pglogs_queue_cursors, NULL);
        hash_free(tran->pglogs_queue_hash);
        tran->pglogs_queue_hash = NULL;
    }

    if (tran->bkfill_txn_list)
        free(tran->bkfill_txn_list);

    free(tran);

    return outrc;
}

int bdb_tran_rep_handle_dead(bdb_state_type *bdb_state)
{
    tran_type *tran;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    tran = pthread_getspecific(bdb_state->seqnum_info->key);
    if (tran) {
        logmsg(LOGMSG_WARN, "bdb_tran_rep_handle_dead: flagging txn %p\n", tran);
        tran->rep_handle_dead = 1;
        return 1;
    }
    return 0;
}

unsigned long long check_waiters_skip_count = 0;
unsigned long long check_waiters_commit_count = 0;

int bdb_rowlocks_check_commit_physical(bdb_state_type *bdb_state,
                                       tran_type *tran, int blockop_count)
{
    extern int gbl_rowlocks_commit_on_waiters;
    extern int gbl_locks_check_waiters;
    int commit_interval = bdb_state->attr->physical_commit_interval;
    int ack_interval = bdb_state->attr->physical_ack_interval;
    int check_waiters =
        (gbl_rowlocks_commit_on_waiters && gbl_locks_check_waiters);
    int replag = bdb_state->attr->ack_on_replag_threshold;
    int do_commit = 0;
    u_int32_t flags = 0;
    int ret;

    if (tran->physical_tran == NULL)
        return 0;

    if (replag && lag_bytes(bdb_state) >= replag) {
        do_commit = 1;
        flags |= DB_TXN_REP_ACK;
    }

    if (commit_interval > 0 && (blockop_count % commit_interval) == 0) {
        if (!check_waiters)
            do_commit = 1;
        else {
            ret = bdb_state->dbenv->lock_id_has_waiters(
                bdb_state->dbenv, tran->physical_tran->tid->txnid);

            if (ret == 0)
                check_waiters_skip_count++;
            else if (ret == 1) {
                check_waiters_commit_count++;
                do_commit = 1;
            } else
                logmsg(LOGMSG_ERROR, "%s:lock_id_has_waiters returns %d\n", __func__,
                        ret);
        }
    }

    /* Ideally ack_interval should be some (commit_interval * N) */
    if (ack_interval > 0 && (blockop_count % ack_interval) == 0) {
        do_commit = 1;
        flags |= DB_TXN_REP_ACK;
    }

    if (!do_commit)
        return 0;

    return bdb_tran_commit_phys_getlsn_flags(bdb_state, tran->physical_tran,
                                             &tran->last_regop_lsn, flags);
}

int bdb_is_rowlocks_transaction(tran_type *tran)
{
    return tran->is_rowlocks_trans;
}

int bdb_tran_commit_with_seqnum_size(bdb_state_type *bdb_state, tran_type *tran,
                                     seqnum_type *seqnum, uint64_t *out_txnsize,
                                     int *bdberr)
{
    int rc;
    int is_rowlocks_trans = tran->is_rowlocks_trans;

    /* lock was acquired in bdb_tran_begin */
    /* BDB_READLOCK(); */

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    rc = bdb_tran_commit_with_seqnum_int(bdb_state, tran, seqnum, bdberr, 1,
                                         out_txnsize, NULL, 0, NULL, 0);

    if (!is_rowlocks_trans)
        BDB_RELLOCK();

    return rc;
}

int bdb_tran_commit(bdb_state_type *bdb_state, tran_type *tran, int *bdberr)
{
    int rc;
    int has_bdblock;

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    /*
    if(tran->is_rowlocks_trans)
       has_bdblock = 0;
    else */
    if (tran->tranclass == TRANCLASS_SOSQL)
        has_bdblock = 0;
    else if (tran->tranclass == TRANCLASS_READCOMMITTED)
        has_bdblock = 0;
    else if (tran->tranclass == TRANCLASS_SNAPISOL)
        has_bdblock = 0;
    else if (tran->tranclass == TRANCLASS_SERIALIZABLE)
        has_bdblock = 0;
    else
        has_bdblock = 1;

    rc = bdb_tran_commit_with_seqnum_int(bdb_state, tran, NULL, bdberr, 0, NULL,
                                         NULL, 0, NULL, 0);

    if (has_bdblock)
        BDB_RELLOCK();

    return rc;
}

unsigned long long bdb_logical_tranid(void *intran)
{
    tran_type *tran = (tran_type *)intran;
    return tran->logical_tranid;
}

int bdb_tran_commit_logical_with_seqnum_size(bdb_state_type *bdb_state,
                                             tran_type *tran, void *blkseq,
                                             int blklen, void *blkkey,
                                             int blkkeylen, seqnum_type *seqnum,
                                             uint64_t *out_txnsize, int *bdberr)
{
    int rc;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    rc = bdb_tran_commit_with_seqnum_int(bdb_state, tran, seqnum, bdberr, 1,
                                         out_txnsize, blkseq, blklen, blkkey,
                                         blkkeylen);

    /*BDB_RELLOCK();*/

    return rc;
}

uint64_t bdb_tran_logbytes(tran_type *tran)
{
    u_int64_t logbytes = 0;
    if (tran->tid)
        tran->tid->getlogbytes(tran->tid, &logbytes);
    return logbytes;
}

int bdb_tran_abort_int_int(bdb_state_type *bdb_state, tran_type *tran,
                           int *bdberr, void *blkseq, int blklen, void *blkkey,
                           int blkkeylen, seqnum_type *seqnum, int *priority)
{
    int rc = 0;
    int outrc = 0;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    *bdberr = BDBERR_NOERROR;

    switch (tran->tranclass) {
    case TRANCLASS_LOGICAL_NOROWLOCKS:
        rc = tran->tid->abort(tran->tid);
        if (rc != 0) {
            *bdberr = BDBERR_MISC;
            outrc = -1;
            goto cleanup;
        }
        break;

    case TRANCLASS_SNAPISOL:
    case TRANCLASS_SERIALIZABLE:
        /* Clean up shadow indices if any. */
        rc = bdb_osql_trn_unregister(tran->osql, bdberr);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s %d %d\n", __func__, rc, *bdberr);
    /* fallthrough */
    case TRANCLASS_SOSQL:
    case TRANCLASS_READCOMMITTED:
        bdb_tran_free_shadows(bdb_state, tran);
        break;

    case TRANCLASS_PHYSICAL:
    case TRANCLASS_BERK:
        outrc = 0;

        if (tran->tid) {
            /* THIS IS ALWAYS A CHILDLESS TRANSACTION OR A CHILD TRANSACTION */
            if (priority) {
                __txn_getpriority(tran->tid, priority);
#if 0           
              int priority_p = 0;
              int priority2 = 0;


              if (tran->parent)
                 __txn_getpriority(tran->parent->tid, &priority_p);
               
              __txn_getpriority(tran->tid, &priority2);

                printf("%d %s:%d %p ABORT got priority %d parent %d %s\n", 
                      pthread_self(), __FILE__, __LINE__, tran->tid, priority2, priority_p, (!tran->parent)?"NO PARENT":"");
              *priority = priority2;
#endif
            }

            rc = tran->tid->abort(tran->tid);
            if (rc) {
                *bdberr = BDBERR_MISC;
                outrc = -1;
                goto cleanup;
            }
        }
        break;

    case TRANCLASS_LOGICAL:
        /* abort of a logical transaction is pretty close to a commit, so
           call the commit code to do the dirty work */
        tran->aborted = 1;
        rc = bdb_tran_commit_with_seqnum_int(bdb_state, tran, seqnum, bdberr, 1,
                                             NULL, blkseq, blklen, blkkey,
                                             blkkeylen);
        return rc;
/*
        if(rc)
        {
            *bdberr = BDBERR_MISC;
            outrc = -1;
            goto cleanup;
        }
        */
#if 0
        return rc; /*dh 03152012: isn't this leaking tran object freed lower?*/
        return rc; /*mh 06042013: no - it's freed inside of bdb_tran_commit_with_seqnum_int */
#endif
    }

cleanup:

    switch (tran->tranclass) {
    case TRANCLASS_LOGICAL:
    case TRANCLASS_PHYSICAL:
    case TRANCLASS_BERK:
        bdb_tran_free_shadows(bdb_state, tran);
        break;
    default:
        break;
    }

    /* if we are the master and we free tran we need to get rid of the stored
     * reference so functions like berkdb_send_rtn don't try to use it */
    if (tran->master) {
        Pthread_setspecific(bdb_state->seqnum_info->key, NULL);
    }

    if (tran->trak)
        logmsg(LOGMSG_USER, "TRK_TRAN: aborted %p (type=%d)\n", tran,
                tran->tranclass);

    if (tran->table_version_cache)
        free(tran->table_version_cache);
    tran->table_version_cache = NULL;

    if (tran->pglogs_queue_hash) {
        hash_for(tran->pglogs_queue_hash, free_pglogs_queue_cursors, NULL);
        hash_free(tran->pglogs_queue_hash);
        tran->pglogs_queue_hash = NULL;
    }

    if (tran->bkfill_txn_list)
        free(tran->bkfill_txn_list);

    free(tran);
    return outrc;
}

int bdb_tran_abort_int(bdb_state_type *bdb_state, tran_type *tran, int *bdberr,
                       void *blkseq, int blklen, void *blkkey, int blkkeylen,
                       int *priority)
{
    return bdb_tran_abort_int_int(bdb_state, tran, bdberr, blkseq, blklen,
                                  blkkey, blkkeylen, NULL, priority);
}

int bdb_tran_abort_wrap(bdb_state_type *bdb_state, tran_type *tran, int *bdberr,
                        int *priority)
{
    int rc;
    int has_bdblock;

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    /* Logical locks are released elsewhere */
    if (tran->is_rowlocks_trans)
        has_bdblock = 0;
    else if (tran->tranclass == TRANCLASS_SOSQL)
        has_bdblock = 0;
    else if (tran->tranclass == TRANCLASS_READCOMMITTED)
        has_bdblock = 0;
    else if (tran->tranclass == TRANCLASS_SNAPISOL)
        has_bdblock = 0;
    else if (tran->tranclass == TRANCLASS_SERIALIZABLE)
        has_bdblock = 0;
    else
        has_bdblock = 1;

    rc =
        bdb_tran_abort_int(bdb_state, tran, bdberr, NULL, 0, NULL, 0, priority);

    if (has_bdblock)
        BDB_RELLOCK();

    return rc;
}

int bdb_tran_abort_priority(bdb_state_type *bdb_state, tran_type *tran,
                            int *bdberr, int *priority)
{
    return bdb_tran_abort_wrap(bdb_state, tran, bdberr, priority);
}

int bdb_tran_abort(bdb_state_type *bdb_state, tran_type *tran, int *bdberr)
{
    return bdb_tran_abort_wrap(bdb_state, tran, bdberr, NULL);
}

int bdb_tran_abort_logical(bdb_state_type *bdb_state, tran_type *tran,
                           int *bdberr, void *blkseq, int blklen, void *blkkey,
                           int blkkeylen, seqnum_type *seqnum)
{
    int rc;
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    rc = bdb_tran_abort_int_int(bdb_state, tran, bdberr, blkseq, blklen, blkkey,
                                blkkeylen, seqnum, NULL);

    return rc;
}

/*
    CURSORTRAN give sql users a locker id to use for the locking
    in the absence of a transaction.
    because of this we make sure we get a bdb read lock, even though
    the upper layer will reaquire it again
 */
cursor_tran_t *bdb_get_cursortran(bdb_state_type *bdb_state, uint32_t flags,
                                  int *bdberr)
{
    cursor_tran_t *curtran = NULL;
    int lowpri = (flags & BDB_CURTRAN_LOW_PRIORITY);
    int rc = 0;

    if (bdb_state->parent) {
        logmsg(LOGMSG_ERROR, "%s: error, should be called for master bdb_state\n",
                __func__);
    }

    BDB_READLOCK("bdb_get_cursortran");

    curtran = calloc(sizeof(cursor_tran_t), 1);
    if (curtran) {
        unsigned int loc_flags = DB_LOCK_ID_READONLY;
        extern int gbl_track_curtran_locks;

        if (lowpri)
            loc_flags |= DB_LOCK_ID_LOWPRI;
        if (gbl_track_curtran_locks)
            loc_flags |= DB_LOCK_ID_TRACK;

        rc = bdb_state->dbenv->lock_id_flags(bdb_state->dbenv,
                                             &curtran->lockerid, loc_flags);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: fail to get lock_id rc=%d\n", __func__, rc);
            *bdberr = (rc == DB_LOCK_DEADLOCK) ? BDBERR_DEADLOCK : rc;
            free(curtran);
            BDB_RELLOCK();
            return NULL;
        }
        curtran->id = curtran_counter++;
    } else {
        logmsg(LOGMSG_ERROR, "%s: error allocating %zu bytes\n", __func__,
               sizeof(cursor_tran_t));
        *bdberr = BDBERR_MALLOC;
        BDB_RELLOCK();
    }

    if (bdb_state->attr->dbgberkdbcursor && curtran)
        logmsg(LOGMSG_USER, "BERKDBLOG=%d %p curtran lockerid=%u bdberr=%d GET\n",
                curtran->id, curtran, (curtran) ? curtran->lockerid : -1U,
                *bdberr);

    return curtran;
}

int bdb_curtran_has_waiters(bdb_state_type *bdb_state, cursor_tran_t *curtran)
{
    if (!curtran || curtran->lockerid == 0) return 0;
    return bdb_state->dbenv->lock_id_has_waiters(bdb_state->dbenv, curtran->lockerid);
}

unsigned int bdb_curtran_get_lockerid(cursor_tran_t *curtran)
{
    return curtran->lockerid;
}

int bdb_free_curtran_locks(bdb_state_type *bdb_state, cursor_tran_t *curtran,
                           int *bdberr)
{
    DB_LOCKREQ rq = {0};
    int rc;

    *bdberr = 0;

    rq.op = DB_LOCK_PUT_ALL;
    rc = bdb_state->dbenv->lock_vec(bdb_state->dbenv, curtran->lockerid, 0, &rq,
                                    1, NULL);
    if (rc) {
        *bdberr = rc;
        logmsg(LOGMSG_ERROR, "release rc %d\n", rc);
        return -1;
    }

    return 0;
}

extern void javasp_splock_unlock(void);

int bdb_put_cursortran(bdb_state_type *bdb_state, cursor_tran_t *curtran,
                       uint32_t flags, int *bdberr)
{
    int haslocks = 0;
    int rc = 0;

    *bdberr = 0;

    if (bdb_state->parent) {
        logmsg(LOGMSG_ERROR, "%s: error, should be called for master bdb_state\n",
                __func__);
    }

    if (!curtran) {
        logmsg(LOGMSG_DEBUG, "bdb_put_cursortran called with null curtran\n");
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    rc = bdb_free_curtran_locks(bdb_state, curtran, bdberr);
    if (rc)
        return rc;

    /* before closing what is meant to be the last cursor with this lockerid
       we check there are no pending locks!
       another test is that the cursor is its own "master"
     */
    if (bdb_state->attr->check_locker_locks) {
        /* This check is expensive.  It makes things about 3 times slower. */
        haslocks = __lock_locker_haslocks(bdb_state->dbenv, curtran->lockerid);
        if (haslocks) {
            logmsg(LOGMSG_ERROR, "%s: curtran lockerid=%d has still LOCKS!\n",
                    __func__, curtran->lockerid);
            *bdberr = BDBERR_BUG_KILLME;
            rc = -1;
        }
    }

    rc = bdb_state->dbenv->lock_id_free(bdb_state->dbenv, curtran->lockerid);
    if (rc) {
        logmsg(LOGMSG_ERROR, 
                "%s:%d:%s: fail returned by lock_id_free lid=%x rc=%d\n",
                __FILE__, __LINE__, __func__, curtran->lockerid, rc);
        *bdberr = BDBERR_BUG_KILLME;
        rc = -1;
    }

    BDB_RELLOCK();

    if (bdb_state->attr->dbgberkdbcursor)
        logmsg(LOGMSG_ERROR, "BERKDBLOG=%d %p curtran bdberr=%d PUT\n", curtran->id,
                curtran, *bdberr);

    /* nullify */
    free(curtran);

    if (*bdberr)
        return -1;

    return rc;
}

/* The tranlist is kept in order of startlsn - transactions which have been
 * created, but have not yet written are at the end of the list, and have a
 * startlsn of (0, 1). */
int bdb_get_lsn_lwm(bdb_state_type *bdb_state, DB_LSN *lsnout)
{
    int rc = BDBERR_INVALID_LSN;
    bdb_state_type *parent;

    if (bdb_state->parent)
        parent = bdb_state->parent;
    else
        parent = bdb_state;

    LOCK(&parent->translist_lk)
    {

        /* Just copy lwm */
        *lsnout = parent->lwm;

        /* Good rc if are in-progress transactions */
        if (listc_size(&parent->logical_transactions_list)) {
            DB_LSN *lsnp;

            lsnp = &parent->logical_transactions_list.top->startlsn;

            if (lsnp->file > 0) {
                rc = 0;
            }

#if 0
            /* Keep this part in until you're sure that the bulk-cursor code works */
            DB_LSN *prev = NULL;
            struct tran_tag *tran = NULL, *tmp = NULL;
            int cmp;

            if(0 != lsnp->file)
            {
                assert(0 == (cmp = log_compare(&parent->lwm, lsnp)));
            }

            /* Make sure these are in the correct order */
            LISTC_FOR_EACH_SAFE(&parent->logical_transactions_list, tran, tmp, tranlist_lnk)
            {
                lsnp = &tran->startlsn;
                if(prev) 
                {
                    assert(log_compare(prev, lsnp) <= 0 || 0 == lsnp->file);
                }
                prev = lsnp;
            }
#endif
        }
    }
    UNLOCK(&parent->translist_lk);
    return rc;
}

uint32_t bdb_get_lid_from_cursortran(cursor_tran_t *curtran)
{
    return curtran->lockerid;
}

int bdb_add_rep_blob(bdb_state_type *bdb_state, tran_type *tran, int session,
                     int seqno, void *blob, int sz, int *bdberr)
{
    int rc;
    DB_LSN lsn;
    DB_ENV *dbenv;
    dbenv = (DB_ENV *)bdb_state->dbenv;
    DBT dbt = {0};

    rc = llog_repblob_log(dbenv, tran->tid, &lsn, 0, session, seqno, &dbt);
    if (rc) {
        *bdberr = BDBERR_MISC;
        rc = -1;
    }
    return rc;
}

void bdb_upgrade_all_prepared(bdb_state_type *bdb_state)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;
    bdb_state->dbenv->txn_upgrade_all_prepared(bdb_state->dbenv);
}

unsigned long long bdb_get_current_lsn(bdb_state_type *bdb_state,
                                       unsigned int *file, unsigned int *offset)
{
    DB_LSN outlsn;
    unsigned long long current_context = 0;
    current_context = bdb_get_commit_genid(bdb_state, &outlsn);
    if (outlsn.file == 0 || outlsn.offset == 1) {
        __log_txn_lsn(bdb_state->dbenv, &outlsn, NULL, NULL);
    }
    if (file)
        *file = outlsn.file;
    if (offset)
        *offset = outlsn.offset;
    return current_context;
}

void bdb_set_tran_verify_updateid(tran_type *tran)
{
    tran->verify_updateid = 1;
}
