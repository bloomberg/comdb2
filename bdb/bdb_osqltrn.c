/*
   Copyright 2015, 2017, Bloomberg Finance L.P.

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

/**
 *  Snapshot/Serial sql transaction support;
 *
 */
#include "limit_fortify.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <limits.h>

#include <list.h>
#include <ctrace.h>
#include <alloca.h>

#include <build/db.h>

#include "bdb_int.h"
#include "bdb_cursor.h"
#include "bdb_api.h"
#include "bdb_osqltrn.h"
#include "bdb_osqllog.h"
#include "bdb_osqlbkfill.h"
#include "bdb_osqlcur.h"
#include "locks.h"

#include <list.h>
#include <ctrace.h>
#include <logmsg.h>

#ifdef NEWSI_STAT
#include <time.h>
#include <sys/time.h>
#include <util.h>
extern struct timeval logical_undo_time;
#endif

/**
 * Each snapshot/serializable transaction registers one bdb_osql_trn
 *
 */
struct bdb_osql_trn {
    tran_type *shadow_tran;              /* shadow transaction */
    bdb_osql_log_t *first;               /* first log to process */
    pthread_mutex_t log_mtx;             /* mutex for first */
    int trak;                            /* trak this transaction */
    LINKC_T(struct bdb_osql_trn) lnk;    /* link */
    LISTC_T(bdb_osql_log_t) bkfill_list; /* backfill list */
    int cancelled; /* set if resource limitation forced this to abort */
    bdb_osql_log_t *last_reg; /* last registered log */
};

/**
 * Snapshot/Serializable transaction repository
 *
 */
typedef struct bdb_osql_trn_repo {
    LISTC_T(bdb_osql_trn_t) trns; /* shadow_transactions */
    int trak;                     /* global debug */
} bdb_osql_trn_repo_t;

/* snapshot/serializable session repository */
bdb_osql_trn_repo_t *trn_repo;
/* for creatn/deltn of trns */
pthread_mutex_t trn_repo_mtx = PTHREAD_MUTEX_INITIALIZER;

static int bdb_osql_trn_create_backfill(bdb_state_type *bdb_state,
                                        bdb_osql_trn_t *trn, int *bdberr,
                                        int epoch, int file, int offset,
                                        struct bfillhndl *bkfill_hndl);
static int
bdb_osql_trn_create_backfill_active_trans(bdb_state_type *bdb_state,
                                          bdb_osql_trn_t *trn, int *bdberr,
                                          struct bfillhndl **ret_bkfill_hndl);
int bdb_prepare_newsi_bkfill(bdb_state_type *bdb_state, uint64_t **list,
                             int *count, DB_LSN *oldest);

/**
 * Create the snapshot/serializable transaction repository
 *
 */
int bdb_osql_trn_repo_init(int *bdberr)
{
    bdb_osql_trn_repo_t *tmp = NULL;

    tmp = (bdb_osql_trn_repo_t *)malloc(sizeof(*tmp));
    if (!tmp) {
        *bdberr = BDBERR_MALLOC;
        return -1;
    }

    listc_init(&tmp->trns, offsetof(bdb_osql_trn_t, lnk));

    tmp->trak = 0;

    trn_repo = tmp;

    return 0;
}

/**
 * Enable/disable tracking for trn repo
 *
 */
void bdb_osql_trn_trak(int flag)
{
    if (!trn_repo)
        return;

    trn_repo->trak = flag;
}

#ifdef _LINUX_SOURCE
#include <unistd.h>
#include <sys/syscall.h>

void verify_pthread_mutex(pthread_mutex_t *lock)
{
    return;
    // WTF?  The world is turning upside-down .. valgrind is hitting this ..
    /*
    pid_t tid = syscall(__NR_gettid);
    assert(tid == lock->__data.__owner);
    */
}
#else

void verify_pthread_mutex(pthread_mutex_t *lock) { return; }

#endif

void bdb_verify_repo_lock() { verify_pthread_mutex(&trn_repo_mtx); }

/**
 * lock the snapshot/serializable transaction repository
 *
 */
int bdb_osql_trn_repo_lock() { return pthread_mutex_lock(&trn_repo_mtx); }

/**
 * unlock the snapshot/serializable transaction repository
 *
 */
int bdb_osql_trn_repo_unlock() { return pthread_mutex_unlock(&trn_repo_mtx); }

/**
 * Destroy the snapshot/serializable transaction repository
 *
 */

int bdb_osql_trn_repo_destroy(int *bdberr)
{
    bdb_osql_trn_repo_t *tmp = trn_repo;
    bdb_osql_trn_t *trn = NULL, *trn2 = NULL;
    int rc = 0;

    if (!tmp)
        return 0;

    rc = pthread_mutex_lock(&trn_repo_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
        *bdberr = BDBERR_BADARGS;
    }

    trn_repo = NULL;

    rc = pthread_mutex_unlock(&trn_repo_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
        *bdberr = BDBERR_BADARGS;
    }

    /* this should be clean */
    LISTC_FOR_EACH_SAFE(&tmp->trns, trn, trn2, lnk)
    {
        /* this is possible if there are pending cancelled
           transactions */
        ctrace("%s: pending transaction\n", __func__);
        bdb_osql_trn_unregister(trn, bdberr);
    }

    free(tmp);

    return 0;
}

extern int gbl_extended_sql_debug_trace;
extern int gbl_new_snapisol;
DB_LSN bdb_gbl_recoverable_lsn;
int32_t bdb_gbl_recoverable_timestamp = 0;
pthread_mutex_t bdb_gbl_recoverable_lsn_mutex;

DB_LSN bdb_asof_current_lsn = {0};
DB_LSN bdb_latest_commit_lsn = {0};
uint32_t bdb_latest_commit_gen = 0;
pthread_mutex_t bdb_asof_current_lsn_mutex;
pthread_cond_t bdb_asof_current_lsn_cond;

void bdb_set_gbl_recoverable_lsn(void *lsn, int32_t timestamp)
{
    if (!gbl_new_snapisol_asof)
        return;
    Pthread_mutex_lock(&bdb_gbl_recoverable_lsn_mutex);
    bdb_gbl_recoverable_timestamp = timestamp;
    bdb_gbl_recoverable_lsn = *(DB_LSN *)lsn;
    Pthread_mutex_unlock(&bdb_gbl_recoverable_lsn_mutex);
}

int bdb_is_timestamp_recoverable(bdb_state_type *bdb_state, int32_t timestamp)
{
    Pthread_mutex_lock(&bdb_gbl_recoverable_lsn_mutex);
    if (timestamp < bdb_gbl_recoverable_timestamp) {
        Pthread_mutex_unlock(&bdb_gbl_recoverable_lsn_mutex);
        return 0;
    }
    Pthread_mutex_unlock(&bdb_gbl_recoverable_lsn_mutex);

    return 1;
}

unsigned int bdb_osql_trn_count = 0;

int request_durable_lsn_from_master(bdb_state_type *bdb_state, 
        uint32_t *durable_file, uint32_t *durable_offset, uint32_t *durable_gen);



/**
 *  Register a shadow transaction with the repository
 *  Called upon transaction begin/start
 *
 */
bdb_osql_trn_t *bdb_osql_trn_register(bdb_state_type *bdb_state,
                                      tran_type *shadow_tran, int trak,
                                      int *bdberr, int epoch, int file,
                                      int offset, int is_ha_retry)
{
    bdb_osql_trn_t *trn = NULL;
    DB_LSN lsn;
    int rc = 0, durable_lsns = bdb_state->attr->durable_lsns;
    bdb_state_type *parent;
    DB_LSN durable_lsn = {0};
    uint32_t durable_gen = 0;
    int backfill_required = 0;
    struct bfillhndl *bkfill_hndl = NULL;

    if (gbl_extended_sql_debug_trace) {
        logmsg(LOGMSG_USER,
               "%s line %d called with epoch=%d lsn=[%d][%d] "
               "is_retry=%d\n",
               __func__, __LINE__, epoch, file, offset, is_ha_retry);
    }

    if (bdb_state->parent)
        parent = bdb_state->parent;
    else
        parent = bdb_state;

    if (!is_ha_retry)
        file = 0;

    if ((shadow_tran->tranclass == TRANCLASS_SNAPISOL ||
         shadow_tran->tranclass == TRANCLASS_SERIALIZABLE)) {
        int behind = 0;

        /* Assert that we have an lsn for ha-retries */
        if (is_ha_retry)
            assert(file);

        /* Don't request an LSN if we're given one */
        if (epoch || file)
            backfill_required = 1;

        /* Request our startpoint from the master */
        if (durable_lsns || file) {
            DB_LSN my_lsn, arg_lsn;
            uint32_t my_gen;

            /* Request my startpoint */
            if (!file && (rc = request_durable_lsn_from_master(
                              bdb_state, &file, &offset, &durable_gen)) != 0) {
                *bdberr = BDBERR_NOT_DURABLE;
                return NULL;
            }

            /* Get our current lsn */
            BDB_READLOCK("bdb_current_lsn");
            bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &my_gen);
            __log_txn_lsn(bdb_state->dbenv, &my_lsn, NULL, NULL);
            BDB_RELLOCK();

            arg_lsn.file = file;
            arg_lsn.offset = offset;

            /* Verify generation numbers for durable-lsn case */
            if (durable_gen && my_gen != durable_gen)
                behind = 1;

            /* Verify lsns for both cases */
            else if (log_compare(&my_lsn, &arg_lsn) < 0)
                behind = 1;

            /* Error out if we are too far behind */
            if (behind) {
                if (gbl_extended_sql_debug_trace) {
                    logmsg(LOGMSG_USER,
                           "%s line %d: returning not-durable, durable_gen=%u,"
                           " my_gen=%u req_lsn=[%d][%d] my_lsn=[%d][%d]\n",
                           __func__, __LINE__, durable_gen, my_gen, file,
                           offset, my_lsn.file, my_lsn.offset);
                }
                *bdberr = BDBERR_NOT_DURABLE;
                return NULL;
            }

            /* Otherwise we'll do backfill */
            backfill_required = 1;
        }
    }

    rc = pthread_mutex_lock(&trn_repo_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
        *bdberr = BDBERR_BADARGS;
        abort();
    }

    if (!trn_repo)
        goto done;

    trn = (bdb_osql_trn_t *)calloc(1, sizeof(bdb_osql_trn_t));
    if (!trn) {
        *bdberr = BDBERR_MALLOC;
        goto done;
    }

    rc = pthread_mutex_init(&trn->log_mtx, NULL);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_init %d %d\n", rc, errno);
        *bdberr = BDBERR_BADARGS;
        free(trn);
        trn = NULL;
        goto done;
    }

    trn->shadow_tran = shadow_tran;
    trn->trak = (trak & SQL_DBG_BDBTRN) || (trn_repo->trak);

    listc_init(&trn->bkfill_list, offsetof(bdb_osql_log_t, lnk));

    /* Set while holding the trn_repo lock */
    shadow_tran->startgenid = bdb_get_commit_genid_generation(
        bdb_state, &shadow_tran->snapy_commit_lsn,
        &shadow_tran->snapy_commit_generation);
    if (shadow_tran->snapy_commit_lsn.file == 0) {
        shadow_tran->startgenid =
            bdb_get_current_lsn(bdb_state, &(shadow_tran->birth_lsn.file),
                                &(shadow_tran->birth_lsn.offset));
    } else {
        shadow_tran->birth_lsn = shadow_tran->snapy_commit_lsn;
    }

    /* lock the translist and create a backfill handler here
     * if we need backfill to avoid active logical
     * transactions exiting the system
     * after we set our startgenid and birthlsn
     */
    if (backfill_required) {
        if (gbl_new_snapisol)
            rc = bdb_prepare_newsi_bkfill(bdb_state,
                                          &shadow_tran->bkfill_txn_list,
                                          &shadow_tran->bkfill_txn_count,
                                          &shadow_tran->oldest_txn_at_start);
        else
            rc = bdb_osql_trn_create_backfill_active_trans(
                bdb_state, trn, bdberr, &bkfill_hndl);
        if (rc) {
            logmsg(LOGMSG_ERROR, 
                    "%s:%d failed to create backfill active trans, rc %d\n",
                    __func__, __LINE__, rc);
            free(trn);
            trn = NULL;
            goto done;
        }
    }

    listc_abl(&trn_repo->trns, trn);
    ++bdb_osql_trn_count;

    if (trn->trak)
        logmsg(LOGMSG_USER, "TRK_TRN: registered %p rc=%d for shadow=%p genid=%llx "
                        "birthlsn[%d][%d]\n",
                trn, rc, shadow_tran, shadow_tran->startgenid,
                shadow_tran->birth_lsn.file, shadow_tran->birth_lsn.offset);

done:
    if (pthread_mutex_unlock(&trn_repo_mtx)) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_unlock %d %d\n", rc, errno);
        *bdberr = BDBERR_BADARGS;
    }

    if (gbl_new_snapisol) {
        shadow_tran->pglogs_queue_hash =
            hash_init_o(offsetof(struct pglogs_queue_cursor, fileid),
                        DB_FILE_ID_LEN * sizeof(unsigned char));
    }

    if (!rc && backfill_required) {
        if (gbl_new_snapisol) {
            if (epoch && gbl_new_snapisol_asof) {
                rc = bdb_get_lsn_context_from_timestamp(
                    bdb_state, epoch, &shadow_tran->asof_lsn,
                    &shadow_tran->startgenid, bdberr);
                if (gbl_extended_sql_debug_trace) {
                    logmsg(LOGMSG_USER, "%s line %d get_lsn_context from "
                            "epoch=%d lsn=[%d][%d] rc=%d\n", __func__, 
                            __LINE__, epoch, file, offset, rc);
                }
                Pthread_mutex_lock(&bdb_gbl_recoverable_lsn_mutex);
                if (!rc && ((bdb_gbl_recoverable_lsn.file == 0 &&
                             bdb_gbl_recoverable_lsn.offset == 1) ||
                            (log_compare(&shadow_tran->asof_lsn,
                                         &bdb_gbl_recoverable_lsn) < 0))) {
                    shadow_tran->asof_lsn.file = 0;
                    shadow_tran->asof_lsn.offset = 1;
                    rc = -1;
                } else if (rc) {
                    logmsg(LOGMSG_ERROR, "%s failed to get lsn from timestamp\n",
                            __func__);
                    if (epoch >= bdb_gbl_recoverable_timestamp) {
                        shadow_tran->asof_lsn = bdb_gbl_recoverable_lsn;
                        rc = 0;
                    }
                }
                Pthread_mutex_unlock(&bdb_gbl_recoverable_lsn_mutex);
            } else if (file && gbl_new_snapisol_asof) {
                if (gbl_extended_sql_debug_trace) {
                    logmsg(LOGMSG_USER, "%s line %d creating asof backfill lsn=[%d][%d]\n",
                            __func__, __LINE__, file, offset);
                }
                shadow_tran->asof_lsn.file = file;
                shadow_tran->asof_lsn.offset = offset;
                Pthread_mutex_lock(&bdb_gbl_recoverable_lsn_mutex);
                if ((bdb_gbl_recoverable_lsn.file == 0 &&
                     bdb_gbl_recoverable_lsn.offset == 1) ||
                    (log_compare(&shadow_tran->asof_lsn,
                                 &bdb_gbl_recoverable_lsn) < 0)) {
                    shadow_tran->asof_lsn.file = 0;
                    shadow_tran->asof_lsn.offset = 1;
                    rc = -1;
                }
                Pthread_mutex_unlock(&bdb_gbl_recoverable_lsn_mutex);
                if (!rc) {
                    bdb_get_context_from_lsn(bdb_state, &shadow_tran->asof_lsn,
                                             &shadow_tran->startgenid, bdberr);
                }
            }

            if (shadow_tran->asof_lsn.file != 0 &&
                shadow_tran->asof_lsn.offset != 1) {
                int counter = 0;
                DB_LSN latest_commit_lsn;

                pthread_mutex_lock(&bdb_asof_current_lsn_mutex);
                assert(bdb_latest_commit_lsn.file);
                latest_commit_lsn = bdb_latest_commit_lsn;

                // Make sure we have everything copied into pglogs
                while (log_compare(&bdb_asof_current_lsn, &latest_commit_lsn) <
                       0) {
                    struct timespec now;

                    if (counter++ > 3) {
                        logmsg(LOGMSG_ERROR, 
                                "%s: waiting for asof_lsn %d:%d to become "
                                "current with req %d:%d, count=%d\n",
                                __func__, bdb_asof_current_lsn.file,
                            bdb_asof_current_lsn.offset, latest_commit_lsn.file,
                            latest_commit_lsn.offset, counter);
                    }

                    rc = clock_gettime(CLOCK_REALTIME, &now);
                    now.tv_sec += 1;
                    pthread_cond_timedwait(&bdb_asof_current_lsn_cond,
                                           &bdb_asof_current_lsn_mutex, &now);
                }
                pthread_mutex_unlock(&bdb_asof_current_lsn_mutex);

                shadow_tran->asof_hashtbl =
                    hash_init_o(PGLOGS_KEY_OFFSET, PAGE_KEY_SIZE);
                bdb_checkpoint_list_get_ckplsn_before_lsn(
                    shadow_tran->asof_lsn, &shadow_tran->asof_ref_lsn);
#ifdef ASOF_TRACE
                printf("snapshot shadow_tran %p began as of lsn[%u][%u], "
                       "ref_lsn[%u][%u], startgenid %llx\n",
                       shadow_tran, shadow_tran->asof_lsn.file,
                       shadow_tran->asof_lsn.offset,
                       shadow_tran->asof_ref_lsn.file,
                       shadow_tran->asof_ref_lsn.offset,
                       shadow_tran->startgenid);
#endif
            } else if (rc) {
                // not able to recover back to that point
                logmsg(LOGMSG_ERROR, "%s:%d unable to recover db to epoch %d, "
                                     "file %d, offset %d\n",
                       __func__, __LINE__, epoch, file, offset);
                *bdberr = BDBERR_NO_LOG;
            }
        }

        if (!gbl_new_snapisol || !gbl_new_snapisol_asof) {
            if (gbl_extended_sql_debug_trace) {
                logmsg(LOGMSG_USER, "%s line %d creating old-style backfill epoch=%d lsn=[%d][%d]\n",
                        __func__, __LINE__, epoch, file, offset);
            }
            rc = bdb_osql_trn_create_backfill(bdb_state, trn, bdberr, epoch,
                                              file, offset, bkfill_hndl);
        }

        if (rc) {
            logmsg(LOGMSG_ERROR, "fail to backfill %d %d\n", rc, *bdberr);
            pthread_mutex_lock(&trn_repo_mtx);
            listc_rfl(&trn_repo->trns, trn);
            pthread_mutex_destroy(&trn->log_mtx);
            free(trn);
            pthread_mutex_unlock(&trn_repo_mtx);
            return NULL;
        }
    }

    return trn;
}

int free_pglogs_queue_cursors(void *obj, void *arg);
/**
 *  Unregister a shadow transaction with the repository
 *  Called upon transaction commit/rollback
 *
 */
int bdb_osql_trn_unregister(bdb_osql_trn_t *trn, int *bdberr)
{

    int rc = 0;
    int exit = 0;

    rc = pthread_mutex_lock(&trn_repo_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
        *bdberr = BDBERR_BADARGS;
    }

    if (trn_repo)
        listc_rfl(&trn_repo->trns, trn);
    else
        exit = 1;

    /*
    fprintf( stderr, "%d %s:%d UNregistered %p\n",
          pthread_self(), __FILE__, __LINE__, trn->shadow_tran);
     */

    rc = pthread_mutex_unlock(&trn_repo_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
        *bdberr = BDBERR_BADARGS;
    }
    if (exit)
        return 0;

    /* clean the logs if we point to anything */
    if (trn->first) {
        /* at this point trn is unlinked, no more updates for trn */
        rc = bdb_osql_log_unregister(trn->shadow_tran, trn->first,
                                     trn->last_reg, trn->trak);
        if (rc < 0)
            logmsg(LOGMSG_WARN, "%s %d bdb_osql_log_unregister LOG LEAK POSSIBLE\n",
                    __func__, rc);
    } else {
        if (trn->trak)
            logmsg(LOGMSG_USER, "TRK_TRN: %p has not logs  shadow=%p\n", trn,
                    trn->shadow_tran);
    }

    /* now clean this structure */
    rc = pthread_mutex_destroy(&trn->log_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_init %d %d\n", rc, errno);
        *bdberr = BDBERR_BADARGS;
    }

    if (trn->trak)
        logmsg(LOGMSG_USER, "TRK_TRN: unregistered %p rc=%d for shadow=%p\n", trn,
                rc, trn->shadow_tran);

    free(trn);

    return rc;
}

/**
 * Mark "aborted" all snapisol transactions that point to "log"
 * as first argument
 * (Upon updating the shadows, each transaction will actually abort)
 *
 */
int bdb_osql_trn_cancel_clients(bdb_osql_log_t *log, int lock_repo, int *bdberr)
{
    bdb_osql_trn_t *trn = NULL;
    int rc = 0;

    if (lock_repo) {
        /* lock the repository so no transactions will move in/out */
        rc = pthread_mutex_lock(&trn_repo_mtx);
        if (rc) {
            logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
            *bdberr = BDBERR_BADARGS;
            return -1;
        }
    }

    if (trn_repo) {
        LISTC_FOR_EACH(&trn_repo->trns, trn, lnk)
        {
            assert(trn->shadow_tran->tranclass == TRANCLASS_SNAPISOL ||
                   trn->shadow_tran->tranclass == TRANCLASS_SERIALIZABLE);

            /* lock transaction mutex while registering the snapshot update */
            rc = pthread_mutex_lock(&trn->log_mtx);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s mutex_lock %d %d\n", __func__, rc, errno);
                goto done;
            }

            if (trn->first == log) {
                trn->cancelled = 1;

                if (trn->trak)
                    logmsg(LOGMSG_USER, "TRK_LOG: cancel transaction %p start "
                                    "genid=%llx due to vlog forced cleanup\n",
                            trn, trn->shadow_tran->startgenid);
            } else {
                if (trn->trak)
                    logmsg(LOGMSG_USER, "TRK_LOG: not affected transaction %p "
                                    "start genid=%llx\n",
                            trn, trn->shadow_tran->startgenid);
            }

            rc = pthread_mutex_unlock(&trn->log_mtx);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s mutex_unlock %d %d\n", __func__, rc, errno);
                goto done;
            }
        }
    } else {
        logmsg(LOGMSG_WARN, "%s: No trn repo???\n", __func__);
    }

done:
    if (lock_repo) {
        rc = pthread_mutex_unlock(&trn_repo_mtx);
        if (rc) {
            logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
            *bdberr = BDBERR_BADARGS;
        }
    }

    return rc;
}

int bdb_osql_trn_count_clients(int *count, int lock_repo, int *bdberr)
{
    int rc = 0;

    *count = 0;

    if (lock_repo) {
        rc = pthread_mutex_lock(&trn_repo_mtx);
        if (rc) {
            logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
            *bdberr = BDBERR_BADARGS;
            return -1;
        }
    }

    bdb_verify_repo_lock();

    if (trn_repo) {
        *count = listc_size(&trn_repo->trns);
    }

    if (lock_repo) {
        rc = pthread_mutex_unlock(&trn_repo_mtx);
        if (rc) {
            logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
            *bdberr = BDBERR_BADARGS;
            return -1;
        }
    }

    return 0;
}

void bdb_osql_trn_clients_status()
{
    int rc, bdberr;
    int count;
    rc = bdberr = 0;

    rc = pthread_mutex_lock(&trn_repo_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
        return;
    }

    rc = bdb_osql_trn_count_clients(&count, 0, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d error counting clients, rc %d\n", __FILE__, __LINE__, rc);
    } else {
        logmsg(LOGMSG_USER, "snapshot registered: %u\n", bdb_osql_trn_count);
        logmsg(LOGMSG_USER, "active snapshot: %u\n", count);
    }

    rc = pthread_mutex_unlock(&trn_repo_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
        return;
    }
}

/**
 * Check all the shadow transactions and register them
 * with the log if any are viable (oldest_genid<birth_genid)
 * If there are no clients for this transaction, set empty
 *
 */
int bdb_osql_trn_check_clients(bdb_osql_log_t *log, int *empty, int lock_repo,
                               int *bdberr)
{
    bdb_osql_trn_t *trn = NULL;
    unsigned long long genid = bdb_osql_log_get_genid(log); /* debug only */
    int rc = 0;
    int clients = 0;

    if (lock_repo) {
        /* lock the repository so no transactions will move in/out */
        rc = pthread_mutex_lock(&trn_repo_mtx);
        if (rc) {
            logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
            *bdberr = BDBERR_BADARGS;
            return -1;
        }
    }

    if (trn_repo) {
        /* this is also run during restart recovery;
           there is no sql session/client here, and
           the trn repo is not initialized yet
         */

        LISTC_FOR_EACH(&trn_repo->trns, trn, lnk)
        {
            assert(trn->shadow_tran->tranclass == TRANCLASS_SNAPISOL ||
                   trn->shadow_tran->tranclass == TRANCLASS_SERIALIZABLE);

            if (bdb_osql_log_undo_required(trn->shadow_tran, log)) {
                /* printf("XXX %p LOG %p Found %llx vs %llx\n", pthread_self(),
                   log,
                      trn->shadow_tran->startgenid, genid); */
                /* replication is changing genid-s committed before
                   the start of our transaction, need to update its snapshot
                 */
                clients++;

                /*
                fprintf(stderr, "%d %s:%d session %llx found, commit genid
                =%llx, clients=%d\n",
                      pthread_self(), __FILE__, __LINE__,
                trn->shadow_tran->startgenid, genid, clients);
                 */

                /* lock transaction mutex while registering the snapshot update
                 */
                rc = pthread_mutex_lock(&trn->log_mtx);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "%s mutex_lock %d %d\n", __func__, rc,
                            errno);
                    goto done;
                }

                if (!trn->first) {
                    trn->first = log;
                }

                trn->last_reg = log;

                if (trn->trak)
                    logmsg(LOGMSG_USER, "TRK_LOG: marking transaction %p first=%p "
                                    "(%llx < %llx)\n",
                            trn, trn->first, trn->shadow_tran->startgenid,
                            genid);

                rc = pthread_mutex_unlock(&trn->log_mtx);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "%s mutex_unlock %d %d\n", __func__, rc,
                            errno);
                    goto done;
                }
            } else {
                /* printf ( "XXX %d LOG %p Skipping transaction %llx<= %llx\n",
                   pthread_self(), log,
                      trn->shadow_tran->startgenid, genid); */
                if (trn->trak)
                    logmsg(LOGMSG_USER, 
                            "TRK_LOG: skipping transaction %p (%llx<= %llx)\n",
                            trn, trn->shadow_tran->startgenid, genid);
            }
        }
    } else {
        logmsg(LOGMSG_WARN, "%s: No trn repo???\n", __func__);
    }

    if (clients) {
        rc = bdb_osql_log_register_clients(log, clients, bdberr);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s: register_clients %d %d\n", __func__, rc,
                    *bdberr);
        *empty = 0;
    } else
        *empty = 1;

done:
    if (lock_repo) {
        rc = pthread_mutex_unlock(&trn_repo_mtx);
        if (rc) {
            logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
            *bdberr = BDBERR_BADARGS;
        }
    }

    return rc;
}


struct clients_update_req {
    bdb_state_type *bdb_state;
    tran_type *shadow_tran;
    struct page_logical_lsn_key *keylist;
    unsigned int nkeys;
};

struct clients_relink_req {
    bdb_state_type *bdb_state;
    tran_type *shadow_tran;
    unsigned char *fileid;
    db_pgno_t pgno;
    db_pgno_t prev_pgno;
    db_pgno_t next_pgno;
    DB_LSN lsn;
};

int bdb_insert_pglogs_int(hash_t *pglogs_hashtbl, unsigned char *fileid,
                          db_pgno_t pgno, DB_LSN lsn);

static pthread_mutex_t clients_update_req_lk = PTHREAD_MUTEX_INITIALIZER;
static pool_t *clients_update_req_pool = NULL;
struct clients_update_req *allocate_clients_update_req(void)
{
    struct clients_update_req *rq = NULL;
    pthread_mutex_lock(&clients_update_req_lk);
    if (clients_update_req_pool == NULL)
        clients_update_req_pool = pool_setalloc_init(
            sizeof(struct clients_update_req), 0, malloc, free);
    rq = pool_getablk(clients_update_req_pool);
    pthread_mutex_unlock(&clients_update_req_lk);
    return rq;
}

void free_clients_update_req(struct clients_update_req *rq)
{
    pthread_mutex_lock(&clients_update_req_lk);
    pool_relablk(clients_update_req_pool, rq);
    pthread_mutex_unlock(&clients_update_req_lk);
}

static pthread_mutex_t clients_relink_req_lk = PTHREAD_MUTEX_INITIALIZER;
static pool_t *clients_relink_req_pool = NULL;
struct clients_relink_req *allocate_clients_relink_req(void)
{
    struct clients_relink_req *rq = NULL;
    pthread_mutex_lock(&clients_relink_req_lk);
    if (clients_relink_req_pool == NULL)
        clients_relink_req_pool = pool_setalloc_init(
            sizeof(struct clients_relink_req), 0, malloc, free);
    rq = pool_getablk(clients_relink_req_pool);
    pthread_mutex_unlock(&clients_relink_req_lk);
    return rq;
}

void free_clients_relink_req(struct clients_relink_req *rq)
{
    pthread_mutex_lock(&clients_relink_req_lk);
    pool_relablk(clients_relink_req_pool, rq);
    pthread_mutex_unlock(&clients_relink_req_lk);
}

/**
 * Check all the shadow transactions and
 * get the oldest asof_lsn among all begin-as-of transactions
 *
 */
int bdb_osql_trn_get_oldest_asof_reflsn(DB_LSN *lsnout)
{
    bdb_osql_trn_t *trn = NULL;
    int rc = 0;
    tran_type *shadow_tran = NULL;

    lsnout->file = 0;
    lsnout->offset = 1;

    rc = pthread_mutex_lock(&trn_repo_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
        return -1;
    }

    if (trn_repo) {
        LISTC_FOR_EACH(&trn_repo->trns, trn, lnk)
        {
            assert(trn->shadow_tran->tranclass == TRANCLASS_SNAPISOL ||
                   trn->shadow_tran->tranclass == TRANCLASS_SERIALIZABLE);
            shadow_tran = trn->shadow_tran;
            assert(shadow_tran);
            if (shadow_tran->asof_ref_lsn.file != 0 &&
                shadow_tran->asof_ref_lsn.offset != 1) {
                if (lsnout->file == 0)
                    *lsnout = shadow_tran->asof_ref_lsn;
                else if (log_compare(&shadow_tran->asof_ref_lsn, lsnout) < 0)
                    *lsnout = shadow_tran->asof_ref_lsn;
            }
        }
    } else {
        logmsg(LOGMSG_WARN, "%s: No trn repo???\n", __func__);
    }

done:
    if (pthread_mutex_unlock(&trn_repo_mtx)) {
        logmsg(LOGMSG_ERROR, "%s:%d pthread_mutex_unlock failed\n", __func__,
                __LINE__);
    }

    return rc;
}

/**
 * Check all the shadow transactions and
 * see if deleting log file will affect active begin-as-of transaction
 *
 */
int bdb_osql_trn_asof_ok_to_delete_log(int filenum)
{
    int rc = 0;
    DB_LSN oldest_reference;

    rc = bdb_osql_trn_get_oldest_asof_reflsn(&oldest_reference);
    if (rc) {
        logmsg(LOGMSG_FATAL, "%s: failed to get oldest asof lsn\n", __func__);
        abort();
    }

    if (oldest_reference.file != 0 && oldest_reference.offset != 1) {
        if (oldest_reference.file <= filenum)
            return 0;
    }

    return 1;
}

/**
 * NOTE: log retrieval for update
 *  - lock trn
 *  - if there is no log (first == NULL) return
 *  - get a copy of the tail of the log (get last)
 *  - unlock trn
 *  - process each log from first to last log
 *  - unlock trn
 */

/**
 * Returns the first log
 *
 */
bdb_osql_log_t *bdb_osql_trn_first_log(bdb_osql_trn_t *trn, int *bdberr)
{
    bdb_osql_log_t *log = NULL;
    int rc = 0;

    *bdberr = 0;

    /* lock transaction mutex while registering the snapshot update */
    rc = pthread_mutex_lock(&trn->log_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s mutex_lock %d %d\n", __func__, rc, errno);
        *bdberr = BDBERR_BUG_KILLME;
        return NULL;
    }

    log = trn->first;

    rc = pthread_mutex_unlock(&trn->log_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s mutex_unlock %d %d\n", __func__, rc, errno);
        *bdberr = BDBERR_BUG_KILLME;
        return NULL;
    }

    return log;
}

/**
 * Returns the next log
 *
 */
bdb_osql_log_t *bdb_osql_trn_next_log(bdb_osql_trn_t *trn, bdb_osql_log_t *log,
                                      int *bdberr)
{
    bdb_osql_log_t *next = NULL;
    unsigned long long genid;
    int rc = 0;

    *bdberr = 0;

    /*TODO: no need for this locking! */
    /* lock transaction mutex while registering the snapshot update */
    rc = pthread_mutex_lock(&trn->log_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s mutex_lock %d %d\n", __func__, rc, errno);
        *bdberr = BDBERR_BUG_KILLME;
        return NULL;
    }

    next = bdb_osql_log_next(log, &genid);

    rc = pthread_mutex_unlock(&trn->log_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s mutex_unlock %d %d\n", __func__, rc, errno);
        *bdberr = BDBERR_BUG_KILLME;
        return NULL;
    }

    return next;
}

/**
 * Returns the shadow_tran
 *
 */
struct tran_tag *bdb_osql_trn_get_shadow_tran(bdb_osql_trn_t *trn)
{
    return trn->shadow_tran;
}

extern int gbl_rowlocks;

/**
 * Create a backfill handler for a snapshot/serializable transaction
 * from the outstanding active logical transactions
 */
static int
bdb_osql_trn_create_backfill_active_trans(bdb_state_type *bdb_state,
                                          bdb_osql_trn_t *trn, int *bdberr,
                                          struct bfillhndl **ret_bkfill_hndl)
{
    int rc;
    /* get the list of lsn-s */
    /* only care about active transactions for now */
    rc = bdb_osqlbkfill_create(bdb_state, ret_bkfill_hndl, bdberr, 0, 0, 0,
                               trn->shadow_tran);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d error retrieving backfill handler %d %d\n",
                __func__, __LINE__, rc, *bdberr);
        return rc;
    }
    return 0;
}

static int bdb_osql_trn_process_bfillhndl(bdb_state_type *bdb_state,
                                          bdb_osql_trn_t *trn, int *bdberr,
                                          struct bfillhndl *bkfill_hndl,
                                          int bkfill_active_trans)
{
    bdb_osql_log_t *log;
    DB_LOGC *cur;
    DB_LSN lsn;
    int rc;

    cur = NULL;
    log = NULL;
    rc = 0;

    if (bkfill_hndl == NULL)
        return 0;

    /* get a log cursor */
    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &cur, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d log_cursor rc %d\n", __FILE__, __LINE__, rc);
        return rc;
    }

    rc = bdb_osqlbkfill_firstlsn(bkfill_hndl, &lsn, bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed getting first lsn %d %d\n", __func__, rc,
                *bdberr);
    } else {
        do {
            if ((trn->shadow_tran->tranclass == TRANCLASS_SNAPISOL ||
                 trn->shadow_tran->tranclass == TRANCLASS_SERIALIZABLE) &&
                (!gbl_rowlocks || !bkfill_active_trans))
                log = parse_log_for_snapisol(bdb_state, cur, &lsn,
                                             (!bkfill_active_trans) ? 2 : 1,
                                             bdberr);
            else
                log = parse_log_for_shadows(bdb_state, cur, &lsn,
                                            1 /* backfill */, bdberr);
            if (*bdberr) goto done;

            if (log) {
                listc_abl(&trn->bkfill_list, log);
            }

            rc = bdb_osqlbkfill_next_lsn(bkfill_hndl, &lsn, bdberr);
            if (rc < 0) {
                logmsg(LOGMSG_ERROR, "%s: failure retrieving next lsn %d %d\n",
                        __func__, rc, *bdberr);
            }
        } while (!rc);
        rc = 0;
    }

done:
    /* close backfill */
    if (bdb_osqlbkfill_close(bkfill_hndl, bdberr))
        logmsg(LOGMSG_ERROR, "%s: failure closing backfill %d %d\n", __func__, rc,
                *bdberr);
    /* close log cursor */
    if (cur->close(cur, 0))
        logmsg(LOGMSG_ERROR, "%s fail to close curlog\n", __func__);

    return rc;
}

/**
 * Create a backfill log list (or page-base lsn info for page-base backfill)
 * for a snapshot/serializable transaction
 */
static int bdb_osql_trn_create_backfill(bdb_state_type *bdb_state,
                                        bdb_osql_trn_t *trn, int *bdberr,
                                        int epoch, int file, int offset,
                                        struct bfillhndl *bkfill_hndl)
{
    int rc;
    rc = 0;

    if (bkfill_hndl != NULL) {
        /* process the lsn list created from the active logical transactions */

        /* if we are backfilling active transactions,
         * we skip those that have already been committed.
         */
        rc = bdb_osql_trn_process_bfillhndl(bdb_state, trn, bdberr, bkfill_hndl,
                                            1);
        if (rc) {
            logmsg(LOGMSG_ERROR, 
                    "%s:%d failed to process backfill handler, rc = %d\n",
                    __func__, __LINE__, rc);
            return rc;
        }
        bkfill_hndl = NULL;
    }

    /* not a "begin as of" transaction, we are done */
    if (epoch == 0 && file == 0)
        return 0;

    /* get the list of lsn-s */
    rc = bdb_osqlbkfill_create(bdb_state, &bkfill_hndl, bdberr, epoch, file,
                               offset, trn->shadow_tran);
    if (rc) {
        logmsg(LOGMSG_ERROR, 
                "%s:%d error retrieving backfill handler %d %d\n",
                __func__, __LINE__, rc, *bdberr);
        return rc;
    }

    if (bkfill_hndl != NULL) {

        DB_LSN start_lsn;

        rc = bdb_osqlbkfill_lastlsn(bkfill_hndl, &start_lsn, bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:%d error retrieving the lowest lsn, %d %d\n",
                    __func__, __LINE__, rc, *bdberr);
            return rc;
        }

        if (epoch) {
            trn->shadow_tran->snapy_commit_lsn = start_lsn;
        }
        else {
            trn->shadow_tran->snapy_commit_lsn.file = file;
            trn->shadow_tran->snapy_commit_lsn.offset = offset;
        }

#ifdef NEWSI_STAT
        struct timeval before, after, diff;
        gettimeofday(&before, NULL);
#endif
        rc = bdb_osql_trn_process_bfillhndl(bdb_state, trn, bdberr, bkfill_hndl,
                                            0 /* dont skip committed trans */);
#ifdef NEWSI_STAT
        gettimeofday(&after, NULL);
        timeval_diff(&before, &after, &diff);
        timeval_add(&logical_undo_time, &diff, &logical_undo_time);
#endif
        if (rc) {
            logmsg(LOGMSG_ERROR, 
                    "%s:%d failed to process backfill handler, rc = %d\n",
                    __func__, __LINE__, rc);
            return rc;
        }
        bkfill_hndl = NULL;
    }

    return rc;
}

/**
 * Backfill shadows needed by cursor cur
 *
 */
tmpcursor_t *bdb_osql_open_backfilled_shadows(bdb_cursor_impl_t *cur,
                                              bdb_osql_trn_t *trn, int type,
                                              int *bdberr)
{
    bdb_osql_log_t *log = NULL;
    tmpcursor_t *shadcur = NULL;
    DB_LOGC *curlog = NULL;
    int dirty = 0;
    int rc = 0;

    shadcur =
        bdb_tran_open_shadow(cur->state, cur->dbnum, cur->shadow_tran, cur->idx,
                             cur->type, (type == BERKDB_SHAD_CREATE), bdberr);

#ifdef NEWSI_STAT
    struct timeval before, after, diff;
    gettimeofday(&before, NULL);
#endif
    /* backfill only snapshot/serializable for now */
    if ((cur->shadow_tran->tranclass == TRANCLASS_SERIALIZABLE ||
         cur->shadow_tran->tranclass == TRANCLASS_SNAPISOL) &&
        (shadcur == NULL ||
         bdb_osql_shadow_is_bkfilled(cur->ifn, bdberr) == 0)) {
        if (trn->bkfill_list.top) {
            /* retrieve a log cursor */
            rc = cur->state->dbenv->log_cursor(cur->state->dbenv, &curlog, 0);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s:%d error get log cursor rc %d\n", __FILE__,
                        __LINE__, rc);
                *bdberr = rc;
                return NULL;
            }

            if (cur->trak)
                logmsg(LOGMSG_USER, "TRN_TRK: backfilling cur %p begin\n", cur);

            /* apply the logs */
            LISTC_FOR_EACH(&trn->bkfill_list, log, lnk)
            {
                if (cur->trak)
                    logmsg(LOGMSG_USER, "TRN_TRK: backfill %p log %p\n", cur, log);

                rc = bdb_osql_log_apply_log(cur, curlog, log,
                                            cur->shadow_tran->osql, &dirty,
                                            LOG_BACKFILL, cur->trak, bdberr);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "%s failure applying log %d %d\n", __func__,
                            rc, *bdberr);
                    return shadcur;
                }
            }

            if (cur->trak)
                logmsg(LOGMSG_USER, "TRN_TRK: backfilling cur %p end dirty=%d\n",
                        cur, dirty);

            /* close log cursor */
            rc = curlog->close(curlog, 0);
            if (rc) {
                *bdberr = rc;
                return NULL;
            }
        }

        rc = bdb_osql_shadow_set_bkfilled(cur->ifn, bdberr);
        if (rc)
            return NULL;
    }

#ifdef NEWSI_STAT
    gettimeofday(&after, NULL);
    timeval_diff(&before, &after, &diff);
    timeval_add(&logical_undo_time, &diff, &logical_undo_time);
#endif

    if (dirty && !shadcur) {
        /* retrieve now a cursor for the newly created shadow */
        shadcur = bdb_tran_open_shadow(cur->state, cur->dbnum, cur->shadow_tran,
                                       cur->idx, cur->type, 0, bdberr);
    }

    return shadcur;
}

/**
 * Returns 1 if the transaction was cancelled due to resource limitations
 *
 */
int bdb_osql_trn_is_cancelled(bdb_osql_trn_t *trn) { return trn->cancelled; }

int bdb_oldest_active_lsn(bdb_state_type *bdb_state, void *inlsn)
{
    DB_LSN lsn, log_lsn;
    bdb_osql_trn_t *trn = NULL;
    int rc;

    lsn.file = INT_MAX;
    __log_txn_lsn(bdb_state->dbenv, &log_lsn, NULL, NULL);

    rc = pthread_mutex_lock(&trn_repo_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
        return -1;
    }

    if (trn_repo) {
        LISTC_FOR_EACH(&trn_repo->trns, trn, lnk)
        {
            assert(trn->shadow_tran->tranclass == TRANCLASS_SNAPISOL ||
                   trn->shadow_tran->tranclass == TRANCLASS_SERIALIZABLE);

            if (trn->cancelled == 0) {
                DB_LSN chklsn = trn->shadow_tran->oldest_txn_at_start.file > 0
                                    ? trn->shadow_tran->oldest_txn_at_start
                                    : trn->shadow_tran->birth_lsn;
                if (log_compare(&chklsn, &lsn) < 0)
                    lsn = chklsn;
            }
        }
    }

    rc = pthread_mutex_unlock(&trn_repo_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
    }

    // plsn = (DB_LSN *)inlsn;

    if (lsn.file == INT_MAX)
        *((DB_LSN *)inlsn) = log_lsn;
    else
        *((DB_LSN *)inlsn) = lsn;

    return 0;
}

int bdb_osql_trn_get_lwm(bdb_state_type *bdb_state, void *plsn)
{
    DB_LSN lsn;
    DB_LSN crtlsn;
    bdb_osql_trn_t *trn = NULL;
    int rc = 0;
    int trak = 0;

    lsn.file = INT_MAX;
    lsn.offset = INT_MAX;

    /* lock the repository so no transactions will move in/out */
    rc = pthread_mutex_lock(&trn_repo_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
        return -1;
    }

    if (trn_repo) {
        LISTC_FOR_EACH(&trn_repo->trns, trn, lnk)
        {
            assert(trn->shadow_tran->tranclass == TRANCLASS_SNAPISOL ||
                   trn->shadow_tran->tranclass == TRANCLASS_SERIALIZABLE);

            if (trn->cancelled == 0 &&
                log_compare(&trn->shadow_tran->birth_lsn, &lsn) < 0) {
                trak |= trn->trak;
                lsn = trn->shadow_tran->birth_lsn;
            }
        }
    } else {
        logmsg(LOGMSG_WARN, "%s: No trn repo???\n", __func__);
    }

done:
    *(DB_LSN *)plsn = lsn;

    rc = pthread_mutex_unlock(&trn_repo_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
    }

    return rc;
}
