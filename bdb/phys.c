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
  phys : physical layer

  this file will contain every physical primitive.
  each physical primitive is a physical transaction consisting of
  3 things:  1) a logical UNDO record.  2) a call to a berk primitive.
  3) locks necessary to prevent dirty reads

  the logical undo record in a physical transaction must be able to stand
  alone, with no context.  it must be able to completely undo the physical
  transaction.  it must also contain enough information for locks to be
  aquired based on genid.

  More on this below.


  Control flow here is confusing, so here's an overview.  add.c/upd.c/del.c
  routines call ll.c to do some underlying low level operation (like a put
  or a del).  The ll.c code either does the operation directly, or calls
  phys.c to wrap the operation in a new (physical) transaction.  So there's
  three levels of operations:
  Logical         - Multiple operations like adding records and their
                    corresponding keys are done under a logical
                    transaction.
  Physical        - A single ("real") operation is wrapped in a
                    physical transaction.  A physical transaction contains
                    a berkdb operation (see below) wrapped in a transaction
                    along with a log record that contains enough information
                    to undo the operation.
  Real berkdb)    - A real call to berkeley to do something.

  So for a transaction with row locks that inserts a record, the call
  sequence is:

  Description                                   Calls
  -----------                                   -----

  Begin logical transaction (tran.c)
  Add dta (add.c)                               bdb_prim_allocdta_int
  Add a record (ll.c)                        ll_dta_add
  Begin physical transaction (phys.c)     phys_dta_add
  Add the record (ll.c)               ll_dta_add  !!!!
  (called with different transaction level)
  Add log record (custom_recover.c)   bdb_llog_add_dta
  Commit physical transaction (phys.c)
  Add key (add.c)                               bdb_prim_addk_int
  Add key (ll.c)                             ll_key_add
  Begin physical transaction (phys.c)     phys_key_add
  Add the key (ll.c)                  ll_key_add    (see above)
  Add log key (phys.c)                bdb_llog_add_ix
  Commit physical transaction (phys.c)
  Add blob (add.c)
  Blobs are records, same case as for record (same code)
  Write logical commit record (tran.c)

  For a transaction without row locks, the flow is much like before,
  with an additional ll layer instead of direct berkeley calls.
  It's likely better this way in any case.

  Begin real transaction (tran.c)
  Add dta (add.c)                               bdb_prim_allocdta_int
    Add a record (ll.c)                        ll_dta_add
Add the record (ll.c)                   ll_dta_add (same call)
    Add key (ll.c)                                bdb_prim_addk_int
    Add key (ll.c)                             ll_key_add
Add the key                             ll_key_add (same call)
    */

#include <errno.h>
#include <assert.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/poll.h>
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
#include <fsnapf.h>

#include <ctrace.h>

#include "flibc.h"
#include "net.h"
#include "bdb_int.h"
#include "locks.h"
#include <dbinc/db_swap.h>
#include "logmsg.h"

/* There are two problems that berkeley solves for us that we need to
   re-solve with rowlocks:
   1) applications seeing records that are written by transactions
      that have not yet committed.
   2) applications not seeing records that were deleted by transactions
      that have not yet committed.
 */

static int start_physical_transaction(bdb_state_type *bdb_state,
                                      tran_type *logical_tran,
                                      tran_type **outtran)
{
    tran_type *physical_tran;
    int rc;

    physical_tran = bdb_tran_begin_phys(bdb_state, logical_tran);
    if (physical_tran == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d begin trans failed\n", __FILE__, __LINE__);
        return DB_LOCK_DEADLOCK;
    }

    if (!logical_tran->committed_begin_record &&
        (bdb_state->repinfo->myhost == bdb_state->repinfo->master_host)) {
        rc = bdb_llog_start(bdb_state, logical_tran, physical_tran->tid);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:%d begin bdb_llog_start rc %d\n", __FILE__,
                    __LINE__, rc);
            bdb_tran_abort_phys(bdb_state, physical_tran);
            return rc;
        }
        logical_tran->wrote_begin_record = 1;
    }

    *outtran = physical_tran;

    return 0;
}

#include <epochlib.h>

extern int gbl_rowlocks_commit_on_waiters;
extern int gbl_locks_check_waiters;

int get_physical_transaction(bdb_state_type *bdb_state, tran_type *logical_tran,
                             tran_type **outtran, int force_commit)
{
    extern unsigned long long check_waiters_skip_count;
    extern unsigned long long check_waiters_commit_count;
    int rc = 0;

    if (!logical_tran->single_physical_transaction &&
        logical_tran->physical_tran) {
        int do_commit = 0;

        if (force_commit || logical_tran->micro_commit)
            do_commit = 1;
        else if (gbl_locks_check_waiters && gbl_rowlocks_commit_on_waiters) {
            rc = bdb_state->dbenv->lock_id_has_waiters(
                bdb_state->dbenv, logical_tran->physical_tran->tid->txnid);

            if (rc == 0)
                check_waiters_skip_count++;
            else if (rc == 1) {
                check_waiters_commit_count++;
                do_commit = 1;
            } else
                logmsg(LOGMSG_ERROR, "%s: lock_id_has_waiters returns %d\n",
                        __func__, rc);
        }

        if (do_commit) {
            bdb_tran_commit_phys(bdb_state, logical_tran->physical_tran);
            assert(!logical_tran->physical_tran);
        }
    }
    if (!logical_tran->physical_tran) {
        rc = start_physical_transaction(bdb_state, logical_tran, outtran);
        if (rc != 0) {
            int ismaster =
                (bdb_state->repinfo->myhost == bdb_state->repinfo->master_host);
            if (!ismaster && !bdb_state->in_recovery) {
                logmsg(LOGMSG_ERROR,
                       "Master change while getting physical tran.\n");
                return BDBERR_READONLY;
            }
            return rc;
        }
        if (logical_tran->single_physical_transaction &&
            logical_tran->schema_change_txn && gbl_rowlocks) {
            int bdberr = 0;
            logical_tran->sc_parent_tran = logical_tran->physical_tran;
            logical_tran->physical_tran = bdb_tran_begin(
                bdb_state, logical_tran->sc_parent_tran, &bdberr);
            logical_tran->physical_tran->logical_tran = logical_tran;
            logical_tran->physical_tran->tranclass = TRANCLASS_PHYSICAL;
            if (logical_tran->physical_tran == NULL) {
                logmsg(LOGMSG_ERROR,
                       "%s:%d failed to start child tran for sc, bdberr=%d\n",
                       __func__, __LINE__, bdberr);
                return -1;
            }
        }
    }

    *outtran = logical_tran->physical_tran;
    return 0;
}

static inline int micro_retry_check(bdb_state_type *bdb_state, tran_type *tran)
{
    extern int gbl_micro_retry_on_deadlock;

    if (gbl_rowlocks && gbl_micro_retry_on_deadlock && tran->micro_commit &&
        !tran->single_physical_transaction &&
        !(gbl_locks_check_waiters && gbl_rowlocks_commit_on_waiters))
        return 1;
    else
        return 0;
}

extern unsigned long long gbl_rowlocks_deadlock_retries;

static inline int is_deadlock(int rc)
{
    switch (rc) {
    case BDBERR_DEADLOCK_ROWLOCK:
    case BDBERR_DEADLOCK:
    case DB_LOCK_DEADLOCK:
        return 1;
        break;
    default:
        return 0;
        break;
    }
}

static inline void deadlock_trace(const char *func, tran_type *tran, int rc)
{
    extern int gbl_rowlocks_deadlock_trace;
    if (is_deadlock(rc) && gbl_rowlocks_deadlock_trace) {
        logmsg(LOGMSG_ERROR, "ltranid %llu %s returning deadlock %d\n",
               tran->logical_tranid, func, rc);
    }
}

int phys_dta_add(bdb_state_type *bdb_state, tran_type *logical_tran,
                 unsigned long long genid, DB *dbp, int dtafile, int dtastripe,
                 DBT *dbt_key, DBT *dbt_data, int odhready)
{
    int rc, micro_retry, retry = 0;
    int retry_count = bdb_state->attr->pagedeadlock_retries;
    int max_poll = bdb_state->attr->pagedeadlock_maxpoll;
    tran_type *physical_tran = NULL;

    DBT *addlkptr = NULL;
    DB_LOCK *addrowlk = NULL;
    DB_LSN last_regop_lsn = logical_tran->last_regop_lsn;

    /* Start transaction */
    rc = get_physical_transaction(bdb_state, logical_tran, &physical_tran, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s failed get physical tran rc %d\n", __func__,
               rc);
        goto done;
    }

    if (gbl_rowlocks && dtafile == 0) {
        int did_commit = 0;
        rc = tran_allocate_rlptr(logical_tran, &addlkptr, &addrowlk);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s failed to allocate rlptr rc %d\n",
                   __func__, rc);
            goto done;
        }

        if (log_compare(&last_regop_lsn, &logical_tran->last_regop_lsn))
            did_commit = 1;

        rc = bdb_lock_row_write_getlock(bdb_state, logical_tran, -1, genid,
                                        addrowlk, addlkptr, !did_commit);
        if (rc == DB_LOCK_NOTGRANTED && !did_commit) {
            /* trylock failed, let's commit here and wait again */
            tran_deallocate_pop(logical_tran, 1);
            rc = get_physical_transaction(bdb_state, logical_tran,
                                          &physical_tran, 1);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s failed get physical tran rc %d\n",
                       __func__, rc);
                goto done;
            }
            tran_allocate_rlptr(logical_tran, &addlkptr, &addrowlk);
            rc = bdb_lock_row_write_getlock(bdb_state, logical_tran, -1, genid,
                                            addrowlk, addlkptr, 0);
        }
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s failed to lock row write genid %llx rc %d\n", __func__,
                   genid, rc);
            goto done;
        }
    }

    micro_retry = micro_retry_check(bdb_state, logical_tran);

    do {
        /* Insert row */
        rc = ll_dta_add(bdb_state, genid, dbp, physical_tran, dtafile,
                        dtastripe, dbt_key, dbt_data, DB_NOOVERWRITE, odhready);

        if (micro_retry && --retry_count > 0 &&
            (rc == BDBERR_DEADLOCK || rc == DB_LOCK_DEADLOCK)) {
            retry = 1;
            bdb_tran_abort_phys_retry(bdb_state, physical_tran);
            rc = get_physical_transaction(bdb_state, logical_tran,
                                          &physical_tran, 0);
            if (rc)
                goto done;
            if (max_poll > 0)
                poll(NULL, 0, rand() % max_poll);
            gbl_rowlocks_deadlock_retries++;
        } else {
            retry = 0;
        }

    } while (retry);
    deadlock_trace(__func__, logical_tran, rc);

    if (rc)
        goto done;

    if (gbl_rowlocks) {
        /* Logical log on success */
        rc = bdb_llog_add_dta_lk(bdb_state, physical_tran, genid, dtafile,
                                 dtastripe);
    }

done:

    /* Normalize deadlock rcode */
    if (rc == BDBERR_DEADLOCK_ROWLOCK || rc == BDBERR_DEADLOCK)
        rc = DB_LOCK_DEADLOCK;

    return rc;
}

int phys_dta_del(bdb_state_type *bdb_state, tran_type *logical_tran, int rrn,
                 unsigned long long genid, DB *dbp, int dtafile, int dtastripe)
{
    int rc, micro_retry, retry = 0;
    int retry_count = bdb_state->attr->pagedeadlock_retries;
    int max_poll = bdb_state->attr->pagedeadlock_maxpoll;
    tran_type *physical_tran = NULL;
    DBT dbt_dta;

    /* Delete row */
    DBT *dellkptr = NULL;
    DB_LOCK *delrowlk = NULL;
    DB_LSN last_regop_lsn = logical_tran->last_regop_lsn;

    /* Start my transaction */
    rc = get_physical_transaction(bdb_state, logical_tran, &physical_tran, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s failed get physical tran rc %d\n", __func__,
               rc);
        goto done;
    }

    if (gbl_rowlocks && dtafile == 0) {
        int did_commit = 0;
        rc = tran_allocate_rlptr(logical_tran, &dellkptr, &delrowlk);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s failed to allocate rlptr rc %d\n",
                   __func__, rc);
            goto done;
        }

        if (log_compare(&last_regop_lsn, &logical_tran->last_regop_lsn))
            did_commit = 1;

        rc = bdb_lock_row_write_getlock(bdb_state, logical_tran, -1, genid,
                                        delrowlk, dellkptr, !did_commit);
        if (rc == DB_LOCK_NOTGRANTED && !did_commit) {
            /* trylock failed, let's commit here and wait again */
            tran_deallocate_pop(logical_tran, 1);
            rc = get_physical_transaction(bdb_state, logical_tran,
                                          &physical_tran, 1);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s failed get physical tran rc %d\n",
                       __func__, rc);
                goto done;
            }
            tran_allocate_rlptr(logical_tran, &dellkptr, &delrowlk);
            rc = bdb_lock_row_write_getlock(bdb_state, logical_tran, -1, genid,
                                            delrowlk, dellkptr, 0);
        }
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s failed to lock row write genid %llx rc %d\n", __func__,
                   genid, rc);
            goto done;
        }
    }

    micro_retry = micro_retry_check(bdb_state, logical_tran);

    do {
        /* Call dta-del */
        rc = ll_dta_del(bdb_state, physical_tran, rrn, genid, dbp, dtafile,
                        dtastripe, &dbt_dta);

        if (micro_retry && --retry_count > 0 &&
            (rc == BDBERR_DEADLOCK || rc == DB_LOCK_DEADLOCK)) {
            retry = 1;
            bdb_tran_abort_phys_retry(bdb_state, physical_tran);
            rc = get_physical_transaction(bdb_state, logical_tran,
                                          &physical_tran, 0);
            if (rc)
                goto done;
            if (max_poll > 0)
                poll(NULL, 0, rand() % max_poll);
            gbl_rowlocks_deadlock_retries++;
        } else {
            retry = 0;
        }
    } while (retry);
    deadlock_trace(__func__, logical_tran, rc);

    if (rc)
        goto done;

    if (gbl_rowlocks) {
        /* Logical log successful delete */
        rc = bdb_llog_del_dta_lk(bdb_state, physical_tran, genid, &dbt_dta,
                                 dtafile, dtastripe);
    }
    if (dbt_dta.size)
        free(dbt_dta.data);

done:
    /* Normalize deadlock rcode */
    if (rc == BDBERR_DEADLOCK_ROWLOCK || rc == BDBERR_DEADLOCK)
        rc = DB_LOCK_DEADLOCK;

    return rc;
}

int phys_dta_upd(bdb_state_type *bdb_state, int rrn,
                 unsigned long long oldgenid, unsigned long long *newgenid,
                 DB *dbp, tran_type *logical_tran, int dtafile, int dtastripe,
                 DBT *verify_dta, DBT *dta, int odhready)
{
    int rc, micro_retry, retry = 0;
    int retry_count = bdb_state->attr->pagedeadlock_retries;
    int max_poll = bdb_state->attr->pagedeadlock_maxpoll;
    tran_type *physical_tran = NULL;
    DBT old_dta;

    /* Old row */
    DBT *oldlkptr = NULL;
    DB_LOCK *oldrowlk = NULL;

    /* Masked genids */
    unsigned long long orignew = *newgenid;
    DB_LSN last_regop_lsn = logical_tran->last_regop_lsn;

    /* Start my transaction */
    rc = get_physical_transaction(bdb_state, logical_tran, &physical_tran, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s failed get physical tran rc %d\n", __func__,
               rc);
        goto done;
    }

    if (gbl_rowlocks && dtafile == 0) {
        int did_commit = 0;
        rc = tran_allocate_rlptr(logical_tran, &oldlkptr, &oldrowlk);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s failed to allocate rlptr rc %d\n",
                   __func__, rc);
            goto done;
        }

        if (log_compare(&last_regop_lsn, &logical_tran->last_regop_lsn))
            did_commit = 1;

        /* trylock first if physical tran wasn't committed before */
        rc = bdb_lock_row_write_getlock(bdb_state, logical_tran, -1, oldgenid,
                                        oldrowlk, oldlkptr, !did_commit);
        if (rc == DB_LOCK_NOTGRANTED && !did_commit) {
            /* trylock failed, let's commit here and wait again */
            tran_deallocate_pop(logical_tran, 1);
            rc = get_physical_transaction(bdb_state, logical_tran,
                                          &physical_tran, 1);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s failed get physical tran rc %d\n",
                       __func__, rc);
                goto done;
            }
            tran_allocate_rlptr(logical_tran, &oldlkptr, &oldrowlk);
            rc = bdb_lock_row_write_getlock(bdb_state, logical_tran, -1,
                                            oldgenid, oldrowlk, oldlkptr, 0);
        }
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s failed to lock row write genid %llx rc %d\n", __func__,
                   oldgenid, rc);
            goto done;
        }
    }

    /*
     * The only rowlock I need is for the original row:  any new changes will be
     * hidden from the sql sessions: they won't see the new row, and they'll
     * never need to be blocked by a wall lock in the old row.  Writes (other
     * updates or * deletes) should block on the old row until a logical
     * transaction commits.
     */

    micro_retry = micro_retry_check(bdb_state, logical_tran);

    do {
        *newgenid = orignew;

        /* Returns wall genid, wall rowlock, new genid & new rowlock */
        rc = ll_dta_upd(bdb_state, rrn, oldgenid, newgenid, dbp, physical_tran,
                        dtafile, dtastripe, 0 /*participantstripid*/,
                        0 /*use_new_genid*/, verify_dta, dta, &old_dta,
                        odhready);

        if (micro_retry && --retry_count > 0 &&
            (rc == BDBERR_DEADLOCK || rc == DB_LOCK_DEADLOCK)) {
            retry = 1;
            bdb_tran_abort_phys_retry(bdb_state, physical_tran);
            rc = get_physical_transaction(bdb_state, logical_tran,
                                          &physical_tran, 0);
            if (rc)
                goto done;
            if (max_poll > 0)
                poll(NULL, 0, rand() % max_poll);
            gbl_rowlocks_deadlock_retries++;
        } else {
            retry = 0;
        }
    } while (retry);
    deadlock_trace(__func__, logical_tran, rc);

    if (rc)
        goto done;

    if (gbl_rowlocks) {
        /* Write the logical log for this update */
        rc = bdb_llog_upd_dta_lk(bdb_state, physical_tran, oldgenid, *newgenid,
                                 dtafile, dtastripe, &old_dta);
    }

    if (old_dta.data)
        free(old_dta.data);

done:
    /* Normalize deadlock rcode */
    if (rc == BDBERR_DEADLOCK_ROWLOCK || rc == BDBERR_DEADLOCK)
        rc = DB_LOCK_DEADLOCK;

    return rc;
}

int phys_key_add(bdb_state_type *bdb_state, tran_type *logical_tran,
                 unsigned long long genid, int ixnum, DBT *dbt_key,
                 DBT *dbt_data)
{
    int rc, micro_retry, retry = 0;
    int retry_count = bdb_state->attr->pagedeadlock_retries;
    int max_poll = bdb_state->attr->pagedeadlock_maxpoll;
    tran_type *physical_tran = NULL;

    /* New row */
    DBT *newlkptr = NULL;
    DB_LOCK *newrowlk = NULL;
    DB_LSN last_regop_lsn = logical_tran->last_regop_lsn;

    /* Physical tran */
    rc = get_physical_transaction(bdb_state, logical_tran, &physical_tran, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s failed get physical tran rc %d\n", __func__,
               rc);
        goto done;
    }

    /* Master-only locks unique ix value to ensure we aren't colliding with a
     * delete */
    if (gbl_rowlocks && !bdb_state->ixdups[ixnum]) {
        int did_commit = 0;
        rc = tran_allocate_rlptr(logical_tran, &newlkptr, &newrowlk);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s failed to allocate rlptr rc %d\n",
                   __func__, rc);
            goto done;
        }

        if (log_compare(&last_regop_lsn, &logical_tran->last_regop_lsn))
            did_commit = 1;

        rc = bdb_lock_ix_value_write(bdb_state, logical_tran, ixnum, dbt_key,
                                     newrowlk, newlkptr, !did_commit);
        if (rc == DB_LOCK_NOTGRANTED && !did_commit) {
            /* trylock failed, let's commit here and wait again */
            tran_deallocate_pop(logical_tran, 1);
            rc = get_physical_transaction(bdb_state, logical_tran,
                                          &physical_tran, 1);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s failed get physical tran rc %d\n",
                       __func__, rc);
                goto done;
            }
            tran_allocate_rlptr(logical_tran, &newlkptr, &newrowlk);
            rc = bdb_lock_ix_value_write(bdb_state, logical_tran, ixnum,
                                         dbt_key, newrowlk, newlkptr, 0);
        }
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s failed to lock row index write genid %llx rc %d\n",
                   __func__, genid, rc);
            goto done;
        }
    }

    micro_retry = micro_retry_check(bdb_state, logical_tran);

    /* Add key */
    do {
        rc = ll_key_add(bdb_state, genid, physical_tran, ixnum, dbt_key,
                        dbt_data);

        if (micro_retry && --retry_count > 0 &&
            (rc == BDBERR_DEADLOCK || rc == DB_LOCK_DEADLOCK)) {
            retry = 1;
            bdb_tran_abort_phys_retry(bdb_state, physical_tran);
            rc = get_physical_transaction(bdb_state, logical_tran,
                                          &physical_tran, 0);
            if (rc)
                goto done;
            if (max_poll > 0)
                poll(NULL, 0, rand() % max_poll);
            gbl_rowlocks_deadlock_retries++;
        } else {
            retry = 0;
        }
    } while (retry);
    deadlock_trace(__func__, logical_tran, rc);

    if (rc)
        goto done;

    if (gbl_rowlocks) {
        /* Logical log on success */
        rc = bdb_llog_add_ix_lk(bdb_state, physical_tran, ixnum, genid, dbt_key,
                                dbt_data->size);
    }

done:
    /* The value-lock for master-only rowlocks is txn duration:
     * Otherwise, we could return incorrect dups for subsequent
     * inserts. */

    /* Normalize deadlock rcode */
    if (rc == BDBERR_DEADLOCK_ROWLOCK || rc == BDBERR_DEADLOCK)
        rc = DB_LOCK_DEADLOCK;

    return rc;
}

int phys_key_del(bdb_state_type *bdb_state, tran_type *logical_tran,
                 unsigned long long genid, int ixnum, DBT *key)
{
    int rc, micro_retry, retry = 0;
    int retry_count = bdb_state->attr->pagedeadlock_retries;
    int max_poll = bdb_state->attr->pagedeadlock_maxpoll;
    tran_type *physical_tran = NULL;
    int payloadsz;

    /* Delete row */
    DBT *dellkptr = NULL;
    DB_LOCK *delrowlk;
    DB_LSN last_regop_lsn = logical_tran->last_regop_lsn;

    /* Start my transaction */
    rc = get_physical_transaction(bdb_state, logical_tran, &physical_tran, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s failed get physical tran rc %d\n", __func__,
               rc);
        goto done;
    }

    /* Master-only locks unique ix values to prevent colliding inserts from
     * making this delete un-abortable */
    if (gbl_rowlocks && !bdb_state->ixdups[ixnum]) {
        int did_commit = 0;
        rc = tran_allocate_rlptr(logical_tran, &dellkptr, &delrowlk);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s failed to allocate rlptr rc %d\n",
                   __func__, rc);
            goto done;
        }

        if (log_compare(&last_regop_lsn, &logical_tran->last_regop_lsn))
            did_commit = 1;

        rc = bdb_lock_ix_value_write(bdb_state, logical_tran, ixnum, key,
                                     delrowlk, dellkptr, !did_commit);
        if (rc == DB_LOCK_NOTGRANTED && !did_commit) {
            /* trylock failed, let's commit here and wait again */
            tran_deallocate_pop(logical_tran, 1);
            rc = get_physical_transaction(bdb_state, logical_tran,
                                          &physical_tran, 1);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s failed get physical tran rc %d\n",
                       __func__, rc);
                goto done;
            }
            tran_allocate_rlptr(logical_tran, &dellkptr, &delrowlk);
            rc = bdb_lock_ix_value_write(bdb_state, logical_tran, ixnum, key,
                                         delrowlk, dellkptr, 0);
        }
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s failed to lock row index write genid %llx rc %d\n",
                   __func__, genid, rc);
            goto done;
        }
    }

    micro_retry = micro_retry_check(bdb_state, logical_tran);

    /* Call key-delete */
    do {
        rc = ll_key_del(bdb_state, physical_tran, ixnum, key->data, key->size,
                        2, genid, &payloadsz);
        if (micro_retry && --retry_count > 0 &&
            (rc == BDBERR_DEADLOCK || rc == DB_LOCK_DEADLOCK)) {
            retry = 1;
            bdb_tran_abort_phys_retry(bdb_state, physical_tran);
            rc = get_physical_transaction(bdb_state, logical_tran,
                                          &physical_tran, 0);
            if (rc)
                goto done;
            if (max_poll > 0)
                poll(NULL, 0, rand() % max_poll);
            gbl_rowlocks_deadlock_retries++;
        } else {
            retry = 0;
        }
    } while (retry);
    deadlock_trace(__func__, logical_tran, rc);

    if (rc)
        goto done;

    if (gbl_rowlocks) {
        /* Logical log successful delete */
        rc = bdb_llog_del_ix_lk(bdb_state, physical_tran, ixnum, genid, key,
                                payloadsz);
    }

done:
    /* Normalize deadlock rcode */
    if (rc == BDBERR_DEADLOCK_ROWLOCK || rc == BDBERR_DEADLOCK)
        rc = DB_LOCK_DEADLOCK;

    return rc;
}

/* This doesn't need a lock wall because it doesn't move in the btree. */
int phys_key_upd(bdb_state_type *bdb_state, tran_type *logical_tran,
                 char *table_name, unsigned long long oldgenid,
                 unsigned long long newgenid, void *key, int ix, int keylen,
                 void *dta, int dtalen, int llog_payload_len)
{

    int rc, micro_retry, retry = 0;
    int retry_count = bdb_state->attr->pagedeadlock_retries;
    int max_poll = bdb_state->attr->pagedeadlock_maxpoll;
    tran_type *physical_tran = NULL;

    if (flibc_ntohll(oldgenid) >= flibc_ntohll(newgenid))
        abort();

    /* Start my transaction */
    rc = get_physical_transaction(bdb_state, logical_tran, &physical_tran, 0);
    if (rc)
        goto done;

    /* No rowlocks for master only: the key value hasn't changed
     * I have to do this to prevent it from being deleted, don't i?
     * No .. the code will block trying to delete the data table row first */

    micro_retry = micro_retry_check(bdb_state, logical_tran);

    do {
        /* Call into ll */
        rc = ll_key_upd(bdb_state, physical_tran, table_name, oldgenid,
                        newgenid, key, ix, keylen, dta, dtalen);

        if (micro_retry && --retry_count > 0 &&
            (rc == BDBERR_DEADLOCK || rc == DB_LOCK_DEADLOCK)) {
            retry = 1;
            bdb_tran_abort_phys_retry(bdb_state, physical_tran);
            rc = get_physical_transaction(bdb_state, logical_tran,
                                          &physical_tran, 0);
            if (rc)
                goto done;
            if (max_poll > 0)
                poll(NULL, 0, rand() % max_poll);
            gbl_rowlocks_deadlock_retries++;
        } else {
            retry = 0;
        }
    } while (retry);
    deadlock_trace(__func__, logical_tran, rc);
    if (rc)
        goto done;

    if (gbl_rowlocks) {
        /* Rowlocks logical logging */
        rc = bdb_llog_upd_ix_lk(bdb_state, physical_tran, table_name, key,
                                keylen, ix, llog_payload_len, oldgenid,
                                newgenid);
    }

done:
    /* Normalize deadlock rcode */
    if (rc == BDBERR_DEADLOCK_ROWLOCK || rc == BDBERR_DEADLOCK)
        rc = DB_LOCK_DEADLOCK;

    return rc;
}

int ll_undo_add_ix_lk(bdb_state_type *bdb_state, tran_type *tran,
                      char *table_name, int ixnum, void *key, int keylen,
                      DB_LSN *undolsn)
{
    int rc;
    DB *dbp;
    DBT dbt_key = {0};
    bdb_state_type *table;
    tran_type *physical_tran = NULL;

    table = bdb_get_table_by_name(bdb_state, table_name);
    if (table == NULL) {
        logmsg(LOGMSG_ERROR, "%s unknown table %s\n", __func__, table_name);
        return -1;
    }

    dbt_key.data = key;
    dbt_key.size = keylen;

    rc = get_physical_transaction(bdb_state, tran, &physical_tran, 0);
    if (rc)
        goto done;
    dbp = table->dbp_ix[ixnum];
    rc = dbp->del(dbp, physical_tran->tid, &dbt_key, 0);
    if (rc)
        goto done;
    rc = bdb_llog_comprec(bdb_state, physical_tran, undolsn);
done:

    return rc;
}

/* undolsn is the LSN of the record we are undoing with this call */
int ll_undo_add_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                       char *table_name, unsigned long long genid,
                       DB_LSN *undolsn, int dtafile, int dtastripe)
{
    int rc;
    unsigned long long search_genid;
    DB *dbp;
    DBT dbt_genid = {0};
    bdb_state_type *table;
    tran_type *physical_tran = NULL;

    table = bdb_get_table_by_name(bdb_state, table_name);
    if (table == NULL) {
        logmsg(LOGMSG_ERROR, "%s unknown table %s\n", __func__, table_name);
        return -1;
    }

    search_genid = get_search_genid(table, genid);
    if (search_genid != genid)
        assert(dtafile != 0);

    dbt_genid.data = &search_genid;
    dbt_genid.size = sizeof(unsigned long long);

    rc = get_physical_transaction(bdb_state, tran, &physical_tran, 0);
    if (rc)
        goto done;

    /* TODO: should this call ll.c? */
    dbp = table->dbp_data[dtafile][dtastripe];
    rc = dbp->del(dbp, physical_tran->tid, &dbt_genid, 0);
    if (rc)
        goto done;
    rc = bdb_llog_comprec(bdb_state, physical_tran, undolsn);
done:

    return rc;
}

int ll_undo_del_ix_lk(bdb_state_type *bdb_state, tran_type *tran,
                      char *table_name, unsigned long long genid, int ixnum,
                      DB_LSN *undolsn, void *key, int keylen, void *dta,
                      int dtalen)
{
    int rc;
    DBT dbt_key = {0};
    DBT dbt_data = {0};
    bdb_state_type *table;
    tran_type *physical_tran = NULL;
    DB *dbp;

    table = bdb_get_table_by_name(bdb_state, table_name);
    if (table == NULL) {
        logmsg(LOGMSG_ERROR, "%s unknown table %s at lsn %u:%u\n", __func__,
                table_name, undolsn->file, undolsn->offset);
        return BDBERR_BADARGS;
    }

    dbt_key.data = key;
    dbt_key.size = keylen;
    dbt_data.data = dta;
    dbt_data.size = dtalen;

    rc = get_physical_transaction(bdb_state, tran, &physical_tran, 0);
    if (rc)
        goto done;

    /* call ll.c? */
    dbp = table->dbp_ix[ixnum];
    rc = dbp->put(dbp, physical_tran->tid, &dbt_key, &dbt_data, DB_NOOVERWRITE);
    if (rc)
        goto done;
    rc = bdb_llog_comprec(bdb_state, physical_tran, undolsn);
done:

    return rc;
}

int ll_undo_del_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                       char *table_name, unsigned long long genid,
                       DB_LSN *undolsn, int dtafile, int dtastripe, void *dta,
                       int dtalen)
{
    int rc;
    DB *dbp;
    DBT dbt_genid = {0};
    DBT dbt_dta = {0};
    bdb_state_type *table;
    tran_type *physical_tran = NULL;

    table = bdb_get_table_by_name(bdb_state, table_name);
    if (table == NULL) {
        logmsg(LOGMSG_ERROR, "%s unknown table %s at lsn %u:%u\n", __func__,
                table_name, undolsn->file, undolsn->offset);
        return BDBERR_BADARGS;
    }
    dbt_genid.data = &genid;
    dbt_genid.size = sizeof(unsigned long long);
    dbt_dta.data = dta;
    dbt_dta.size = dtalen;
    dbp = table->dbp_data[dtafile][dtastripe];

    rc = get_physical_transaction(bdb_state, tran, &physical_tran, 0);
    if (rc)
        goto done;
    rc =
        dbp->put(dbp, physical_tran->tid, &dbt_genid, &dbt_dta, DB_NOOVERWRITE);
    if (rc)
        goto done;
    rc = bdb_llog_comprec(bdb_state, physical_tran, undolsn);
done:
    return rc;
}

int ll_undo_inplace_upd_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                               char *table_name, unsigned long long oldgenid,
                               unsigned long long newgenid, void *olddta,
                               int olddta_len, int dtafile, int dtastripe,
                               DB_LSN *undolsn)
{
    DBT dbt_key = {0};
    DBT dbt_data = {0};
    DB *dbp;
    unsigned long long search_genid;
    DBC *cur = NULL;
    tran_type *physical_tran = NULL;
    bdb_state_type *table;
    int rc;

    table = bdb_get_table_by_name(bdb_state, table_name);
    search_genid = get_search_genid(table, oldgenid);

    if (table == NULL) {
        logmsg(LOGMSG_FATAL, "%s unknown table %s\n", __func__, table_name);
        abort();
    }

    rc = get_physical_transaction(bdb_state, tran, &physical_tran, 0);
    if (rc)
        goto done;
    dbp = table->dbp_data[dtafile][dtastripe];

    rc = dbp->cursor(dbp, physical_tran->tid, &cur, 0);
    if (rc)
        goto done;

    dbt_key.data = &search_genid;
    dbt_key.size = sizeof(unsigned long long);
    dbt_key.flags = dbt_data.flags = DB_DBT_MALLOC;

    rc = cur->c_get(cur, &dbt_key, &dbt_data, DB_SET);
    if (rc)
        goto done;

    if (olddta_len > dbt_data.size) {
        dbt_data.data = realloc(dbt_data.data, olddta_len);
    }
    memcpy(dbt_data.data, olddta, olddta_len);
    dbt_data.size = olddta_len;

    rc = cur->c_put(cur, &dbt_key, &dbt_data, DB_CURRENT);
    if (rc)
        goto done;

    /* close cursor */
    rc = cur->c_close(cur);
    cur = NULL;
    if (rc)
        goto done;

    /* write compensation record and commit */
    rc = bdb_llog_comprec(bdb_state, physical_tran, undolsn);
done:
    if (cur) {
        int crc;
        crc = cur->c_close(cur);
        if (crc == DB_LOCK_DEADLOCK)
            rc = DB_LOCK_DEADLOCK;
    }

    if (dbt_data.data)
        free(dbt_data.data);

    return rc;
}

int ll_undo_upd_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                       char *table_name, unsigned long long oldgenid,
                       unsigned long long newgenid, void *olddta,
                       int olddta_len, int dtafile, int dtastripe,
                       DB_LSN *undolsn)
{
    DBT dbt_key = {0};
    DBT dbt_data = {0};
    DB *dbp;
    unsigned long long search_genid;
    tran_type *physical_tran = NULL;
    bdb_state_type *table;
    int rc;

    table = bdb_get_table_by_name(bdb_state, table_name);
    search_genid = get_search_genid(table, newgenid);

    if (table == NULL) {
        logmsg(LOGMSG_ERROR, "%s unknown table %s\n", __func__, table_name);
        return -1;
    }

    rc = get_physical_transaction(bdb_state, tran, &physical_tran, 0);
    if (rc)
        goto done;
    dbp = table->dbp_data[dtafile][dtastripe];

    dbt_key.data = &search_genid;
    dbt_key.size = sizeof(unsigned long long);

    /* undo the update */
    /* delete new genid */
    rc = dbp->del(dbp, physical_tran->tid, &dbt_key, 0);
    if (rc)
        goto done;

    if (olddta_len > 0) {
        search_genid = get_search_genid(table, oldgenid);

        /* add old genid with old data */
        dbt_key.data = &search_genid;
        dbt_key.size = sizeof(unsigned long long);

        dbt_data.data = olddta;
        dbt_data.size = olddta_len;

        rc = dbp->put(dbp, physical_tran->tid, &dbt_key, &dbt_data, 0);
        if (rc)
            goto done;
    } else {
        assert(dtafile != 0);
        rc = 0;
    }

    /* write compensation record */
    rc = bdb_llog_comprec(bdb_state, physical_tran, undolsn);
done:
    return rc;
}

int ll_undo_upd_ix_lk(bdb_state_type *bdb_state, tran_type *tran,
                      char *table_name, int ixnum, void *key, int keylen,
                      void *dta, int dtalen, DB_LSN *undolsn, void *diff,
                      int offset, int difflen)
{
    DBT dbt_key = {0};
    DBT dbt_data = {0};
    DB *dbp;
    tran_type *physical_tran = NULL;
    bdb_state_type *table;
    int rc;
    DBC *cur = NULL;

    table = bdb_get_table_by_name(bdb_state, table_name);
    if (table == NULL) {
        logmsg(LOGMSG_ERROR, "%u:%u %s unknown table %s\n", undolsn->file,
                undolsn->offset, __func__, table_name);
        return -1;
    }

    rc = get_physical_transaction(bdb_state, tran, &physical_tran, 0);
    if (rc)
        goto done;
    dbp = table->dbp_ix[ixnum];

    rc = dbp->cursor(dbp, physical_tran->tid, &cur, 0);
    if (rc)
        goto done;

    dbt_key.data = key;
    dbt_key.size = keylen;
    dbt_key.flags = dbt_data.flags = DB_DBT_MALLOC;
    rc = cur->c_get(cur, &dbt_key, &dbt_data, DB_SET);
    if (rc)
        goto done;

    if (dbt_key.size != keylen) {
        logmsg(LOGMSG_ERROR, "%u:%u %s unexpected key size expected %d got %d\n",
                undolsn->file, undolsn->offset, __func__, keylen, dbt_key.size);
        rc = -1;
        goto done;
    }
    if (dbt_data.size != dtalen) {
        logmsg(LOGMSG_ERROR, "%u:%u %s unexpected data size expected %d got %d\n",
                undolsn->file, undolsn->offset, __func__, dtalen,
                dbt_data.size);
        rc = -1;
        goto done;
    }

    /* Expand this now if necessary */
    if (offset + difflen > dbt_data.size) {
        dbt_data.data = realloc(dbt_data.data, offset + difflen);
    }

    dbt_data.size = offset + difflen;

    /* replace with our data */
    dbt_key.data = key;
    dbt_key.size = keylen;

    /* New data. */
    memcpy((char *)dbt_data.data + offset, diff, difflen);

    rc = cur->c_put(cur, &dbt_key, &dbt_data, DB_CURRENT);
    if (rc)
        goto done;

    /* close cursor */
    rc = cur->c_close(cur);
    cur = NULL;
    if (rc)
        goto done;

    /* write compensation record and commit */
    rc = bdb_llog_comprec(bdb_state, physical_tran, undolsn);
done:
    if (cur) {
        int crc;
        crc = cur->c_close(cur);
        if (crc == DB_LOCK_DEADLOCK)
            rc = DB_LOCK_DEADLOCK;
    }
    if (dbt_data.data)
        free(dbt_data.data);

    return rc;
}

int phys_rowlocks_log_bench_lk(bdb_state_type *bdb_state,
                               tran_type *logical_tran, int op, int arg1,
                               int arg2, void *payload, int paylen)
{
    bdb_state_type *llmeta_bdb_state = bdb_llmeta_bdb_state();
    tran_type *physical_tran = NULL;
    unsigned long long genid1, genid2, ullarg1, ullarg2;
    DB_LOCK rowlk1 = {0}, rowlk2 = {0};
    DBT lk1 = {0}, lk2 = {0};
    char mem1[ROWLOCK_KEY_SIZE], mem2[ROWLOCK_KEY_SIZE];
    int gotrowlock1 = 0, gotrowlock2 = 0, rc = 0;

    lk1.data = mem1;
    lk2.data = mem2;

    /* TODO: Grab locks, etc, for different ops as necessary.  For now, just
     * log it.*/
    switch (op) {
    case 0:
        /* Basecase: (do nothing) - case 0 is the commit benchmark */
        abort();
        break;

    case 1:
        /* Basecase for rowlocks - don't grab any locks */
        break;

    case 3:
        /* Grab 2 rowlocks */
        ullarg1 = arg1;
        ullarg2 = arg2;
        genid2 = (unsigned long long)~(ullarg1 << 32 | ullarg2);
        rc = bdb_lock_row_write_getlock(llmeta_bdb_state, logical_tran, -1,
                                        genid2, &rowlk2, &lk2, 0);
        if (rc)
            goto done;
        gotrowlock2 = 1;
    /* Fall through */

    case 2:
        /* Grab a single rowlock */
        ullarg1 = arg1;
        ullarg2 = arg2;
        genid1 = (unsigned long long)(ullarg1 << 32 | ullarg2);
        rc = bdb_lock_row_write_getlock(llmeta_bdb_state, logical_tran, -1,
                                        genid1, &rowlk1, &lk1, 0);
        if (rc)
            goto done;
        gotrowlock1 = 1;
        break;

    default:
        logmsg(LOGMSG_FATAL, "%s - unhandled op, %d\n", __func__, op);
        abort();
        break;
    }

    rc = get_physical_transaction(bdb_state, logical_tran, &physical_tran, 0);
    if (rc)
        goto done;

    rc = ll_rowlocks_bench(bdb_state, physical_tran, op, arg1, arg2, payload,
                           paylen);
    if (rc)
        goto done;

    /* Logical log on success */
    rc = bdb_llog_rowlocks_bench(bdb_state, physical_tran, op, arg1, arg2, &lk1,
                                 &lk2, payload, paylen);
done:
    /* Release rowlock1 */
    if (gotrowlock1 && rc)
        bdb_release_row_lock(bdb_state, &rowlk1);

    /* Release rowlock2 */
    if (gotrowlock2 && rc)
        bdb_release_row_lock(bdb_state, &rowlk2);

    /* Normalize deadlock rcode */
    if (rc == BDBERR_DEADLOCK_ROWLOCK || rc == BDBERR_DEADLOCK)
        rc = DB_LOCK_DEADLOCK;

    return rc;
}
