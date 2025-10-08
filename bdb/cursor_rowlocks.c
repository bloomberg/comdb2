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

/**
 *  Similar to cursor_ll.c, but the cursors defined here have two additional
 *  properties:
 *   1) They only hold transient page locks - just enough to position the
 *      cursor.  A cursor that's not in the middle of a find/next/prev/
 *      last call does not hold a page lock.
 *   2) They hold row locks for the duration of a cursor.  When a cursor moves
 *      to a different record, gets a row lock on the next row and relinquishes
 *      the lock on the previous row.
 *
 *  In rowlocks mode, the replication thread has 2 types of transactions active
 *  at a time: (1) a long duration transaction that only holds row locks, and
 *  (2) a *  short duration transaction that only holds page locks.  If our
 *  cursors hold both page and row locks we can get into a non-detectable
 *  cycle with the replication thread.  To avoid this we use strict ordering:
 *  we never unconditionally wait for a row locks while holding a page lock. A
 *  positioned cursor remembers the page_lsn, page number, slot number, key
 *  value and row lock name of the last record it was on.
 *
 *  Can I use bulk if I'll be releasing the pagelock every time?  Nope - don't
 *  release the pagelock when you use bulk.
 *
 *  TODO (priority order):
 *
 *  odh                                                         (X)
 *  should work on linux                                        (X)
 *  all tag apis should fail against a rowlocks database        ( )
 *  pause/restore instead of close/open/refind                  (X)
 *  bulk                                                        ( )
 *  replace key->lastkey memcpy with switching buffers?         ( )
 *  retry on page operation deadlocks                           (X)
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <alloca.h>
#include <string.h>
#include <assert.h>
#include <strings.h>

#include <build/db.h>

#include "bdb_cursor.h"
#include "bdb_int.h"
#include "cursor_ll.h"
#include "bdb_osqlcur.h"
#include "bdb_osqllog.h"
#include "locks.h"
#include "tohex.h"

/* For pthread self debug trace */
#include <pthread.h>
#include <logmsg.h>

/* Switch into and out-of the no-rowlocks mode */
enum { CURSOR_MODE_ROWLOCKS = 0, CURSOR_MODE_NO_ROWLOCKS = 1 };

/* Debug variable for this module */
static int rowlocks_debug = 0;

/* Enable debug trace */
static inline int debug_trace(bdb_berkdb_t *pberkdb)
{
    return (pberkdb->impl->trak || rowlocks_debug);
}

/* Print nice looking debug trace */
static inline const char *curtypetostr(int type)
{
    switch (type) {
    default:
    case BDBC_UN:
        return "unknown?????";
    case BDBC_IX:
        return "index";
    case BDBC_DT:
        return "dta";
    case BDBC_SK:
        return "shadow";
    case BDBC_BL:
        return "blob";
    }
}

/* Consolidated lock & cursor positioning */
static inline int close_pagelock_cursor(bdb_berkdb_t *berkdb)
{
    bdb_rowlocks_tag_t *r;
    int rc;

    /* Pointer to last row structure */
    r = &berkdb->impl->u.row;

    /* Return good rcode if cursor is already closed */
    if (!r->pagelock_cursor)
        return 0;

    /* Close my cursor */
    rc = r->pagelock_cursor->c_close(r->pagelock_cursor);

    /* Heavy handed debug assert */
    assert(0 == rc || DB_LOCK_DEADLOCK == rc);

    /* Nilify cursor */
    r->pagelock_cursor = NULL;

    /* This is no longer paused */
    r->paused = 0;

    return rc;
}


/* Nullify cursor after a failed unpause */
static inline int stale_pagelock_cursor(bdb_berkdb_t *berkdb)
{
    bdb_rowlocks_tag_t *r;

    /* Pointer to last row structure */
    r = &berkdb->impl->u.row;

    /* Zero nexts */
    berkdb->impl->num_nexts_since_stale = 0;

    /* Return good rcode if cursor is already null */
    if (!r->pagelock_cursor)
        return 0;

    /* Print debug trace */
    if (debug_trace(berkdb)) {
        logmsg(LOGMSG_ERROR, "Cur %p nullify stale pagelock cursor\n", berkdb);
    }

    /* Nilify cursor */
    r->pagelock_cursor = NULL;

    return 0;
}

/* Get the pagelsn */
static inline int cur_pagelsn(bdb_berkdb_t *berkdb, DB_LSN *lsn)
{
    bdb_rowlocks_tag_t *r = &berkdb->impl->u.row;

    /* Retrieve if unset */
    if (0 == r->pagelsn.file && r->pagelock_cursor) {
        r->pagelock_cursor->c_get_pagelsn(r->pagelock_cursor, &r->pagelsn);
    }

    /* Copy */
    *lsn = r->pagelsn;

    return 0;
}

/* Unpack data from either a datafile or an index.  Switch to bulk if
 * appropriate */
static inline int cur_cget(bdb_berkdb_t *berkdb, DBT *dbtkey, DBT *dbtdta,
                           int flags)
{
    bdb_rowlocks_tag_t *r = &berkdb->impl->u.row;
    bdb_cursor_impl_t *cur = berkdb->impl->cur;
    bdb_state_type *bdb_state = berkdb->impl->bdb_state;
    int rc;

    /* Logic for accessing data file */
    if (BDBC_DT == cur->type && !r->use_bulk) {
        rc = bdb_cget_unpack(bdb_state, r->pagelock_cursor, dbtkey, dbtdta,
                             &r->ver, flags);
        if (0 == rc)
            cur->ver = r->ver;
    }

    /* Logic for accessing index file */
    else {
        rc = r->pagelock_cursor->c_get(r->pagelock_cursor, dbtkey, dbtdta,
                                       flags);
    }

    /* Unset pagelsn */
    r->pagelsn.file = 0;
    r->pagelsn.offset = 1;

    return rc;
}

/* Snap active cursor back to its original position.  Returns cursor
   to it's steady state suitable for repositioning. */
static inline int return_cursor(bdb_berkdb_t *berkdb)
{
    bdb_rowlocks_tag_t *r;
    bdb_cursor_impl_t *cur;
    bdb_state_type *bdb_state;
    int rc;
    DBT dbtkey = {0};
    DBT dbtdta = {0};
    int bdberr;
    DB *db = NULL;

    r = &berkdb->impl->u.row;
    cur = berkdb->impl->cur;
    bdb_state = berkdb->impl->bdb_state;

    if (cur->type == BDBC_DT)
        db = bdb_state->dbp_data[0][cur->idx];
    else
        db = cur->state->dbp_ix[cur->idx];

    if (!r->positioned) {
        if (debug_trace(berkdb)) {
            logmsg(LOGMSG_ERROR, "Cur %p return_cursor on unpositioned cursor?\n",
                    berkdb);
        }
        return 0;
    }

    /* Set pageorder options. */
    if (r->pagelock_cursor == NULL) {
        /* All of these cursors must be pausible */
        const u_int32_t curflags = get_cursor_flags(cur, /* cursor_is_pausible */ 1);

        r->pagelock_cursor = get_cursor_for_cursortran_flags(
            berkdb->impl->cur->curtran, db, curflags, &bdberr);
    }

    if (r->pagelock_cursor == NULL) {
        if (debug_trace(berkdb)) {
            logmsg(LOGMSG_ERROR, "Cur %p return_cursor error creating cursor\n",
                    cur);
        }
        return -1;
    }

    dbtkey.data = r->lastkey;
    dbtkey.size = r->keylen;
    dbtdta.data = NULL;
    dbtdta.size = 0;
    dbtdta.doff = 0;
    dbtdta.dlen = 0;
    dbtdta.flags |= DB_DBT_MALLOC;

    if (debug_trace(berkdb)) {
        char *srcmemp;
        char *srckeyp;

        srcmemp = alloca((2 * r->keylen) + 2);
        srckeyp = util_tohex(srcmemp, r->lastkey, r->keylen);

        logmsg(LOGMSG_USER, "Cur %p %s tbl %s %s return_key='%s'\n", berkdb,
                __func__, bdb_state->name, curtypetostr(cur->type), srckeyp);
    }

    rc = cur_cget(berkdb, &dbtkey, &dbtdta, DB_SET | DB_DBT_PARTIAL);

    if (dbtdta.data)
        free(dbtdta.data);

    if (rc == DB_REP_HANDLE_DEAD)
        rc = DB_LOCK_DEADLOCK;

    if (debug_trace(berkdb)) {
        logmsg(LOGMSG_USER, "Cur %p return-cursor cursor rc=%d\n", cur, rc);
    }
    return rc;
}

/* Wrapper for getting a rowlock */
static inline int get_row_lock(bdb_berkdb_t *berkdb, bdb_state_type *bdb_state,
                               int rowlock_lid, int idx, DBC *dbcp,
                               unsigned long long genid, DB_LOCK *rlk,
                               DBT *lkname, int how)
{
    bdb_cursor_impl_t *cur = berkdb->impl->cur;

    /* Get the rowlock & pause all pagelock cursors if this blocks. */
    return bdb_get_row_lock_pfunc(bdb_state, rowlock_lid, idx, dbcp,
                                  cur->ifn->pauseall, cur->ifn->pausearg, genid,
                                  rlk, lkname, how);
}

/* Wrapper for getting a minmax lock */
static inline int get_row_lock_minmaxlk(bdb_berkdb_t *berkdb,
                                        bdb_state_type *bdb_state,
                                        int rowlock_lid, DBC *dbcp, int ixnum,
                                        int stripe, int minmax, DB_LOCK *rlk,
                                        DBT *lkname, int how)
{
    bdb_cursor_impl_t *cur = berkdb->impl->cur;

    /* Get the endlock & pause all pagelock cursors if this blocks. */
    return bdb_get_row_lock_minmaxlk_pfunc(
        bdb_state, rowlock_lid, dbcp, cur->ifn->pauseall, cur->ifn->pausearg,
        ixnum, stripe, minmax, rlk, lkname, how);
}

/* Open a cursor */
static inline int open_pagelock_cursor(bdb_berkdb_t *berkdb)
{
    bdb_rowlocks_tag_t *r;
    bdb_cursor_impl_t *cur;
    int bdberr = 0;
    DB *db = NULL;

    /* Pointer to last row structure */
    r = &berkdb->impl->u.row;

    /* Cursor */
    cur = berkdb->impl->cur;

    /* Shouldn't be here if we got a cursor */
    assert(NULL == r->pagelock_cursor);

    /* Resolve db object */
    db = (BDBC_DT == cur->type) ? cur->state->dbp_data[0][cur->idx]
                                : cur->state->dbp_ix[cur->idx];

    /* All of these cursors must be pausible */
    const u_int32_t curflags = get_cursor_flags(cur, /* cursor_is_pausible */ 1);

    /* Get a cursor */
    r->pagelock_cursor = get_cursor_for_cursortran_flags(
        berkdb->impl->cur->curtran, db, curflags, &bdberr);

    /* Check for error */
    if (NULL == r->pagelock_cursor) {
        logmsg(LOGMSG_ERROR, "%s line %d: error getting cursor, bdberr is %d.\n",
                __func__, __LINE__, bdberr);
        return bdberr;
    }

    /* No nexts */
    berkdb->impl->num_nexts_since_open = 0;

    /* This is not paused */
    r->paused = 0;

    /* Cursors are opened in rowlocks mode */
    r->lock_mode = CURSOR_MODE_ROWLOCKS;

    return 0;
}

/* Unlock routine for rowlocks */
int bdb_berkdb_rowlocks_unlock(bdb_berkdb_t *pberkdb, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_state_type *bdb_state = berkdb->bdb_state;
    bdb_rowlocks_tag_t *r = &pberkdb->impl->u.row;
    int rc = 0;

    /* Reset bdberr */
    *bdberr = 0;

    /* If you're not rowlocks you shouldn't be here */
    assert(BERKDB_REAL_ROWLOCKS == berkdb->type);

    /* Close your cursor if you have it */
    if (r->pagelock_cursor) {
        rc = close_pagelock_cursor(pberkdb);
        if (rc) {
            if (DB_LOCK_DEADLOCK == rc || DB_REP_HANDLE_DEAD == rc)
                *bdberr = BDBERR_DEADLOCK;
            else
                *bdberr = rc;
            rc = -1;
        }
    }

    /* Release rowlock if I have it */
    if (r->have_lock) {
        bdb_release_row_lock(bdb_state, &r->lk);
        r->have_lock = 0;
    }

    /* Debug trace */
    if (debug_trace(pberkdb)) {
        bdb_cursor_impl_t *cur = pberkdb->impl->cur;
        logmsg(LOGMSG_USER, "Cur %p rowlocks unlock rc=%d", cur, rc);
    }

    return rc;
}

/* Recreate a cursor */
int bdb_berkdb_rowlocks_lock(bdb_berkdb_t *pberkdb, struct cursor_tran *curtran,
                             int *bdberr)
{
    int rc = 0;

#ifndef NDEBUG
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    /* If you're not rowlocks you shouldn't be here */
    assert(BERKDB_REAL_ROWLOCKS == berkdb->type);

    bdb_rowlocks_tag_t *r = &pberkdb->impl->u.row;
    /* Cursor better be null */
    assert(NULL == r->pagelock_cursor);
#endif

    /* Open a cursor */
    rc = open_pagelock_cursor(pberkdb);

    /* Debug trace */
    if (debug_trace(pberkdb)) {
        bdb_cursor_impl_t *cur = pberkdb->impl->cur;
        logmsg(LOGMSG_USER, "Cur %p rowlocks lock rc=%d", cur, rc);
    }

    return rc;
}

/* Find the first or last record of a table or index */
static inline int bdb_berkdb_rowlocks_firstlast_int(bdb_berkdb_t *berkdb,
                                                    int how, int *bdberr)
{
    /* Rcode */
    int rc;

    /* Page index */
    int pg, idx;

    /* New lock */
    DB_LOCK newlk;

    /* End lock */
    DB_LOCK endlk;

    /* Light a flag if we got a record */
    int gotrecord = 0;

    /* bdb_state */
    bdb_state_type *bdb_state;

    /* Pointer to genid */
    unsigned long long *fgenid;

    /* Genid copy */
    unsigned long long cgenid = 0;

    /* Locker id */
    int cursor_lid;

    /* Key and Data */
    DBT key = {0};
    DBT data = {0};

    /* Got the endlock flag */
    int got_endlk = 0;

    /* Work on both data and idx cursors */
    int curidx;
    int curstp;

    /* Pointers to cursors */
    bdb_rowlocks_tag_t *r;
    bdb_cursor_impl_t *cur;

    /* Local reference to bdb_state */
    bdb_state = berkdb->impl->bdb_state;

    /* Reference to cursor info */
    r = &berkdb->impl->u.row;
    cur = berkdb->impl->cur;

    /* Get lockerid */
    cursor_lid = bdb_get_lid_from_cursortran(cur->curtran);

    /* This should not be holding any locks */
    assert(!r->have_lock);

    /* Index & stripe settings for index file */
    if (BDBC_IX == cur->type) {
        curidx = cur->idx;
        curstp = -1;
    }

    /* Index & stripe settings for data file */
    else {
        curidx = -1;
        curstp = cur->idx;
    }

    /* Set key */
    key.data = r->keymem;
    key.size = key.ulen = r->keylen;
    key.flags = DB_DBT_USERMEM;
    r->key = r->keymem;

    /* Set data */
    data.data = r->dtamem;
    data.size =
        r->dtalen + (BDBC_DT == cur->type && bdb_state->ondisk_header ? 7 : 0);
    data.ulen = data.size;
    data.flags = DB_DBT_USERMEM;
    r->dta = r->dtamem;

    /* Grab the endlock */
    if (!got_endlk) {
        /* Get minimum or maximum lock */
        rc = get_row_lock_minmaxlk(berkdb, bdb_state, cursor_lid, NULL, curidx,
                                   curstp, DB_FIRST == how ? 0 : 1, &endlk,
                                   NULL, BDB_LOCK_READ);

        /* Break out if this failed */
        if (rc) {
            goto err_rc;
        }

        /* Endlock flag */
        got_endlk = 1;
    }

    /* Clear the eof flag */
    r->eof = 0;

again:
    /* If this is null then we are in a retry */
    if (NULL == r->pagelock_cursor) {
        rc = open_pagelock_cursor(berkdb);
        if (0 != rc) {
            goto err_rc;
        }
    }

    /* Genid pointer */
    if (BDBC_DT == cur->type) {
        fgenid = (unsigned long long *)r->key;
    } else {
        fgenid = (unsigned long long *)r->dta;
    }

    /* Fetch the record */
    rc = cur_cget(berkdb, &key, &data, how);

    /* Not found case */
    if (DB_NOTFOUND == rc) {
        rc = IX_EMPTY;
        r->eof = 1;
        goto done;
    }

    /* Found a row */
    else if (0 == rc) {
        r->lastdtasize = data.size;
        r->lastkeysize = key.size;

        /* Grab rowlock */
        rc = get_row_lock(berkdb, bdb_state, cursor_lid, curidx,
                          r->pagelock_cursor, *fgenid, &newlk, NULL,
                          BDB_LOCK_READ);

        /* Success */
        if (0 == rc) {
            /* Light the gotrecord flag */
            gotrecord = 1;

            /* Copy the genid */
            cgenid = *fgenid;

            /* Grab page of current record */
            rc = r->pagelock_cursor->c_get_pageindex(r->pagelock_cursor, &pg,
                                                     &idx);
            if (rc)
                goto err_rc;

            /* Stash page and index in cursor */
            r->page = pg;
            r->index = idx;

            goto done;
        }
        /* Retry on stale cursor */
        else if (DB_CUR_STALE == rc) {
            /* Cursor is closed automatically by unpause */
            stale_pagelock_cursor(berkdb);

            /* Try again */
            goto again;
        } else {
            if (BDBERR_DEADLOCK_ROWLOCK != rc) {
                logmsg(LOGMSG_ERROR, "%s:%d Bad rcode from get_row_lock_ix %d\n",
                        __func__, __LINE__, rc);
            }
            goto err_rc;
        }
    }

    /* Error */
    else {
        goto err_rc;
    }

err_rc:
    if (DB_LOCK_DEADLOCK == rc || BDBERR_DEADLOCK_ROWLOCK == rc ||
        BDBERR_DEADLOCK == rc || DB_REP_HANDLE_DEAD == rc) {
        *bdberr = BDBERR_DEADLOCK;
        rc = DB_LOCK_DEADLOCK;
    } else {
        *bdberr = rc;
        rc = -1;
    }

done:
    /* Release endlock if I got it */
    if (got_endlk) {
        bdb_release_row_lock(bdb_state, &endlk);
    }

    /* Cursor is in 'gather-rowlocks' mode */
    r->lock_mode = CURSOR_MODE_ROWLOCKS;

    /* Got a record.  Update cursor internals */
    if (gotrecord) {
        /* Copy new lock */
        r->lk = newlk;

        /* We have a new genid */
        r->genid = cgenid;

        /* We have a lock */
        r->have_lock = 1;

        /* Copy lastkey */
        memcpy(r->lastkey, r->key, r->keylen);

        /* Positioned after lastkey copy */
        r->positioned = 1;
    }

    return rc;
}

/* Test to enable bulk mode */
static inline int switch_bulk_test(bdb_berkdb_t *berkdb, int dir)
{
    /* bdb_state */
    bdb_state_type *bdb_state;

    /* Cursors */
    bdb_rowlocks_tag_t *r;

    /* Bulk size */
    int bulksz;

    /* Convenience vars */
    bdb_state = berkdb->impl->bdb_state;
    r = &berkdb->impl->u.row;

    /* Already enabled */
    if (r->use_bulk)
        return 0;

    /* Not enabled */
    if (!bdb_state->attr->bulk_sql_mode)
        return 0;

    if (!bdb_state->attr->bulk_sql_rowlocks)
        return 0;

    /* Wrong mode */
    if (CURSOR_MODE_NO_ROWLOCKS != r->lock_mode)
        return 0;

    /* Wrong direction */
    if (DB_NEXT != dir)
        return 0;

    /* Didn't pass threshold */
    if (berkdb->impl->num_nexts < bdb_state->attr->bulk_sql_threshold)
        return 0;

    /* Not worth it */
    if ((bulksz = bdb_state->attr->sqlbulksz) < (bdb_state->lrl << 1))
        return 0;

    /* Good to enable */
    return bulksz;
}

/* Return 1 if there are no outstanding logical transactions */
static inline int update_cursor_lwm(bdb_berkdb_t *pberkdb,
                                    bdb_state_type *bdb_state,
                                    bdb_rowlocks_tag_t *r, int lineno)
{
    DB_LSN lwmlsn;
    int lwmrc, cmp;
    bdb_cursor_impl_t *cur;

    lwmrc = bdb_get_lsn_lwm(bdb_state, &lwmlsn);

    /* There is an outstanding logical transaction */
    if (0 == lwmrc) {
        if ((cmp = log_compare(&lwmlsn, &r->lwmlsn)) > 0) {
            /* Print trace */
            if (debug_trace(pberkdb)) {
                cur = pberkdb->impl->cur;
                logmsg(LOGMSG_ERROR, "Cur %p %s line %d tbl %s %s update lwm from "
                                "%d:%d to %d:%d\n",
                        pberkdb, __FILE__, lineno, bdb_state->name,
                        curtypetostr(cur->type), r->lwmlsn.file,
                        r->lwmlsn.offset, lwmlsn.file, lwmlsn.offset);
            }

            /* Copy into the cursor */
            r->lwmlsn = lwmlsn;
        } else if (cmp < 0) {
            /* This can happen: the replicant's log records are processed in the
             * order of physical commit.  You could have a log-stream which
             * looks like this:
             *
             * t1: logical-start
             * t2: logical-start
             * t2: physical-commit
             * Open a read cursor which is tagged with t2's logical start
             * t1: physical-commit
             *
             * We don't have to care if the cursor is marked with the later lsn
             * because the first physical write from either must have occurred
             * after the lsn that we have anyway.
             */

            if (debug_trace(pberkdb)) {
                cur = pberkdb->impl->cur;
                logmsg(LOGMSG_ERROR, "Cur %p %s line %d tbl %s %s not updating "
                                "out-of-order lwm %d:%d vs cursor %d:%d\n",
                        pberkdb, __FILE__, lineno, bdb_state->name,
                        curtypetostr(cur->type), lwmlsn.file, lwmlsn.offset,
                        r->lwmlsn.file, r->lwmlsn.offset);
            }
        }
    }

    return lwmrc;
}

int __lock_dump_region_lockerid
__P((DB_ENV *, const char *, FILE *, u_int32_t lockerid));

/* Here's the algorithm:
 * -
 * 1 Call next-or-prev, and get the rowlock.
 * 2 If the cursor is stale, then reposition on the original record and retry.
 * 3 Release the previous rowlock.
 *
 * Holding the previous rowlock will keep the row from disappearing.  It will
 * also protect the next record from disappearing (by holding it, we're
 * preventing a lock-wall from being formed).
 *
 * If we get a deadlock and have to release our locks all bets are off.  The
 * next 'move' should turn into a find, which can grab the next row and lock.
 * The upper layer handles this correctly.
 *
 * I was worried about missing deleted records which are rolled back.  The wall
 * locks protect against that.  I was also worried about the last-record case.
 * We shouldn't have to acquire the endlock for that as the deleted/updated
 * record would have been protected by a wall-lock which we've passed.
 */
static inline int bdb_berkdb_rowlocks_nextprev_int(bdb_berkdb_t *berkdb,
                                                   int dir, int *bdberr)
{
    /* Rcode */
    int rc = 0;

    /* Bulk sz */
    int bulksz;

    /* Compare variable */
    int cmp;

    /* Rcode from get_lsn_lwm.  Non-0 means there are no logical txns */
    int lwmrc;

    /* Switch to no-rowlocks */
    int nrlsw = 0;

    /* For unpacking bulk data */
    struct odh odh;

    /* Holds the pagelsn of a page */
    DB_LSN pagelsn;

    /* New lock */
    DB_LOCK newlk;

    /* Verify */
    int pg, idx;

    /* Got record flag */
    int gotrecord = 0;

    /* Have lock flag */
    int have_lock = 0;

    /* Current index */
    int curidx;

    /* Lid for my cursor */
    int cursor_lid;

    /* Pointer to genid */
    unsigned long long *fgenid = NULL;

    /* Genid copy */
    unsigned long long cgenid = 0;

    /* Reposition-on-deadlock key */
    char *repokey;

    /* Reposition-on-deadlock key pointer */
    char *repokeyptr;

    /* bdb_state */
    bdb_state_type *bdb_state;

    /* Key and Data */
    DBT key = {0};
    DBT data = {0};

    /* Pointers to cursors */
    bdb_rowlocks_tag_t *r;
    bdb_cursor_impl_t *cur;

    /* Local reference to bdb_state */
    bdb_state = berkdb->impl->bdb_state;

    /* Reference to cursor info */
    r = &berkdb->impl->u.row;
    cur = berkdb->impl->cur;

    /* Get lockerid */
    cursor_lid = bdb_get_lid_from_cursortran(cur->curtran);

    /* This should be holding a lock for the current row */
    assert(r->have_lock || CURSOR_MODE_NO_ROWLOCKS == r->lock_mode);

    /* Allocate memory for the reposition key */
    repokey = alloca(r->keylen);

    /* Copy the repokey */
    memcpy(repokey, r->key, r->keylen);

    /* A reposition in cursor.c read from here */
    repokeyptr = r->key;

    /* Bulk test */
    if ((bulksz = switch_bulk_test(berkdb, dir)) != 0) {
        /* Switching to bulk mode */
        r->use_bulk = 1;

        /* Reset pointers */
        r->bulkptr = r->bulkdata = NULL;

        /* Reset other bulk vars */
        r->bulkdatasz = r->bulkkeysz = 0;

        /* Allocate data */
        if (!r->bulk.data) {
            r->bulk.data = malloc(bulksz);
            r->bulk.ulen = bulksz;
            r->bulk.flags = DB_DBT_USERMEM;
        }

        /* Allocate space for unpacking */
        if (BDBC_DT == cur->type && !r->odh_tmp) {
            r->odh_tmp = malloc(MAXRECSZ);
        }
    }

    /* Set key */
    key.data = r->keymem;
    key.size = key.ulen = r->keylen;
    key.flags = DB_DBT_USERMEM;
    r->key = r->keymem;

    /* Setup for normal find */
    if (!r->use_bulk) {
        /* Set data */
        data.data = r->dtamem;
        data.size = r->dtalen +
                    (BDBC_DT == cur->type && bdb_state->ondisk_header ? 7 : 0);
        data.ulen = data.size;
        data.flags = DB_DBT_USERMEM;
        r->dta = r->dtamem;

        /* Genid pointer */
        if (BDBC_DT == cur->type) {
            fgenid = (unsigned long long *)r->key;
        } else {
            fgenid = (unsigned long long *)r->dta;
        }
    }

    /* Index & stripe settings for index file */
    if (BDBC_IX == cur->type) {
        curidx = cur->idx;
    }

    /* Index & stripe settings for data file */
    else {
        curidx = -1;
    }

again:
    /* Return the cursor to the original position */
    if (NULL == r->pagelock_cursor) {
        /* Sanity */
        assert(!r->use_bulk);

        /* Open the cursor */
        rc = return_cursor(berkdb);
        if (rc) {
            /* Shouldn't happen: we have a lock on the row */
            if (rc == DB_NOTFOUND)
                abort();
            goto err_rc;
        } else if (!r->pagelock_cursor) {
            logmsg(LOGMSG_FATAL, 
                   "Return_cursor has a 0 rc and didn't create a cursor.\n");
            abort();
        }
    }

    /* Unset no-rowlocks switch */
    nrlsw = 0;

    /* Switch into pagelocks if I can */
    if (bdb_state->attr->rowlocks_pagelock_optimization &&
        CURSOR_MODE_ROWLOCKS == r->lock_mode && !r->force_rowlocks) {
        /* Get the current pagelsn */
        cur_pagelsn(berkdb, &pagelsn);

        /* Reset lwmrc - it will be set if I check the list and there's nothing
         * in it. */
        lwmrc = 0;

        /* Only hit the lock (and refresh cursor lwmlsn) if I have to. */
        if (log_compare(&pagelsn, &r->lwmlsn) >= 0) {
            lwmrc = update_cursor_lwm(berkdb, bdb_state, r, __LINE__);
        }

        /* To page-locking if there are no txns or the pagelsn is low enough */
        if (lwmrc || (cmp = log_compare(&pagelsn, &r->lwmlsn)) < 0) {
            /* 'Switch to no-rowlocks' flag */
            nrlsw = 1;
        }
    }

    /* Bulk code */
    if (r->use_bulk) {
        /* Consume from last */
        if (r->bulkptr) {
            DB_MULTIPLE_KEY_NEXT(r->bulkptr, &r->bulk, r->bulkkey, r->bulkkeysz,
                                 r->bulkdata, r->bulkdatasz);
            if (r->bulkptr) {
                r->lastdtasize = r->bulkdatasz;
                r->lastkeysize = r->bulkkeysz;
            }
        }

        /* Retrieve new data */
        if (!r->bulkptr) {
            /* Get the next bulk data */
            rc = r->pagelock_cursor->c_get(r->pagelock_cursor, &key, &r->bulk,
                                           dir | DB_MULTIPLE_KEY);

            /* Unset pagelsn to force re-query by cur_pagelsn */
            r->pagelsn.file = 0;
            r->pagelsn.offset = 1;

            /* Check rc */
            if (!rc) {
                DB_MULTIPLE_INIT(r->bulkptr, &r->bulk);
                DB_MULTIPLE_KEY_NEXT(r->bulkptr, &r->bulk, r->bulkkey,
                                     r->bulkkeysz, r->bulkdata, r->bulkdatasz);
                if (r->bulkptr) {
                    r->lastdtasize = r->bulkdatasz;
                    r->lastkeysize = r->bulkkeysz;
                }
            }
        }

        /* Tweak a bulk record */
        if (r->bulkptr) {
            /* Set the key */
            r->key = r->bulkkey;

            if (BDBC_DT == cur->type) {
                /* Unpack */
                rc = bdb_unpack(bdb_state, r->bulkdata, r->bulkdatasz,
                                r->odh_tmp, MAXRECSZ, &odh, NULL);
                if (rc != 0) {
                    *bdberr = BDBERR_UNPACK;
                    goto err_rc;
                }

                /* Copy so conversion doesn't overlap next */
                if (odh.length < bdb_state->lrl) {
                    memcpy(r->dtamem, odh.recptr, odh.length);
                    r->dta = r->odh.data = r->dtamem;
                } else {
                    r->dta = r->odh.data = odh.recptr;
                }

                /* Size & Version */
                r->lastdtasize = r->odh.size = odh.length;
                cur->ver = r->ver = odh.csc2vers;

                /* Genptr */
                fgenid = (unsigned long long *)r->bulkkey;

                /* In-place */
                if (ip_updates_enabled(bdb_state)) {
                    *fgenid = set_updateid(bdb_state, odh.updateid, *fgenid);
                }
            } else {
                fgenid = (unsigned long long *)r->bulkdata;
                r->dta = r->bulkdata;
            }
        }
    }
    /* Get the data */
    else {
        rc = cur_cget(berkdb, &key, &data, dir);
        if (!rc) {
            r->lastdtasize = data.size;
            r->lastkeysize = key.size;
        }
    }

    /* If we're in NO_ROWLOCKS mode and need to start gathering rowlocks */
    if (CURSOR_MODE_NO_ROWLOCKS == r->lock_mode) {
        /* Get the current pagelsn */
        cur_pagelsn(berkdb, &pagelsn);

        lwmrc = 0;

        /* Only hit the lsn_lwm list if I have to */
        if (log_compare(&pagelsn, &r->lwmlsn) > 0) {
            lwmrc = update_cursor_lwm(berkdb, bdb_state, r, __LINE__);

            /* Use rowlocks if we're on a potentially damaged page */
            if (0 == lwmrc && (cmp = log_compare(&pagelsn, &r->lwmlsn)) >= 0) {
                /* Reposition to correct key */
                memcpy(repokeyptr, repokey, r->keylen);

                /* Tell caller to reposition */
                *bdberr = BDBERR_NEED_REPOSITION;

                /* Print useful trace */
                if (debug_trace(berkdb)) {
                    logmsg(LOGMSG_USER, 
                            "Cur %p tbl %s %s switching to rowlocks mode: "
                            "curlsn=%d:%d pagelsn=%d:%d\n",
                            berkdb, bdb_state->name, curtypetostr(cur->type),
                            r->lwmlsn.file, r->lwmlsn.offset, pagelsn.file,
                            pagelsn.offset);
                }
                return -1;
            } else {
                if (debug_trace(berkdb)) {
                    logmsg(LOGMSG_USER, 
                            "Cur %p tbl %s %s staying in pagelock mode: "
                            "curlsn=%d:%d pagelsn=%d:%d (2)\n",
                            berkdb, bdb_state->name, curtypetostr(cur->type),
                            r->lwmlsn.file, r->lwmlsn.offset, pagelsn.file,
                            pagelsn.offset);
                }
            }
        } else {
            if (debug_trace(berkdb)) {
                logmsg(LOGMSG_USER, 
                        "Cur %p tbl %s %s staying in pagelock mode: "
                        "curlsn=%d:%d pagelsn=%d:%d (1)\n",
                        berkdb, bdb_state->name, curtypetostr(cur->type),
                        r->lwmlsn.file, r->lwmlsn.offset, pagelsn.file,
                        pagelsn.offset);
            }
        }
    }

    else if (nrlsw) {
        /* Get the new pagelsn */
        cur_pagelsn(berkdb, &pagelsn);

        /* Reset have-txn variable */
        lwmrc = 0;

        if (log_compare(&pagelsn, &r->lwmlsn) >= 0) {
            lwmrc = update_cursor_lwm(berkdb, bdb_state, r, __LINE__);

            /* Switch into lockless if the pagelsn is low enough  */
            if (lwmrc || (cmp = log_compare(&pagelsn, &r->lwmlsn)) < 0) {
                /* Light the 'switch to no-rowlocks' flag */
                r->lock_mode = CURSOR_MODE_NO_ROWLOCKS;

                /* Debug trace */
                if (debug_trace(berkdb)) {
                    logmsg(LOGMSG_USER, 
                            "Cur %p tbl %s %s switching to non-rowlocks mode: "
                            "curlsn=%d:%d pagelsn=%d:%d\n",
                            berkdb, bdb_state->name, curtypetostr(cur->type),
                            r->lwmlsn.file, r->lwmlsn.offset, pagelsn.file,
                            pagelsn.offset);
                }
            }
        }
        /* No need to lock - we're already in range */
        else {
            r->lock_mode = CURSOR_MODE_NO_ROWLOCKS;

            /* Debug trace */
            if (debug_trace(berkdb)) {
                logmsg(LOGMSG_USER, 
                        "Cur %p tbl %s %s switching to non-rowlocks mode: "
                        "curlsn=%d:%d pagelsn=%d:%d\n",
                        berkdb, bdb_state->name, curtypetostr(cur->type),
                        r->lwmlsn.file, r->lwmlsn.offset, pagelsn.file,
                        pagelsn.offset);
            }
        }
    }

    /* Not found case: don't need the endlock, as this would have blocked on a
     * lockwall before getting here. */
    if (DB_NOTFOUND == rc) {
        /* Light end-of-file flag */
        r->eof = 1;

        /* Release a lock if we're holding it */
        if (r->have_lock) {
            bdb_release_row_lock(bdb_state, &r->lk);
            r->have_lock = 0;
        }

        /* Convert rc */
        rc = IX_NOTFND;

        /* Goto done */
        goto done;
    }

    /* Found a record */
    else if (0 == rc) {
        /* Clear the eof flag */
        r->eof = 0;

        if (CURSOR_MODE_ROWLOCKS == r->lock_mode) {
            /* Grab rowlock */
            rc = get_row_lock(berkdb, bdb_state, cursor_lid, curidx,
                              r->pagelock_cursor, *fgenid, &newlk, NULL,
                              BDB_LOCK_READ);

            /* Set flag */
            if (0 == rc)
                have_lock = 1;
        }

        /* Got rowlock */
        if (CURSOR_MODE_NO_ROWLOCKS == r->lock_mode || 0 == rc) {
            /* Copy the genid. */
            cgenid = *fgenid;

            /* Light the gotrecord flag */
            gotrecord = 1;

            /* Grab page of current record */
            rc = r->pagelock_cursor->c_get_pageindex(r->pagelock_cursor, &pg,
                                                     &idx);
            if (rc) {
                goto err_rc;
            }

            /* Stash in cursor */
            r->page = pg;
            r->index = idx;

            /* Success */
            goto done;
        }

        /* Retry if the page lsn changed */
        else if (DB_CUR_STALE == rc) {
            /* Cursor is already closed by unpause */
            stale_pagelock_cursor(berkdb);

            if (debug_trace(berkdb)) {
                logmsg(LOGMSG_USER, 
                        "Cur %p %s tbl %s %s retrying on changed pagelsn.\n",
                        berkdb, __func__, bdb_state->name,
                        curtypetostr(cur->type));
            }

            /* Try again */
            goto again;
        } else {
            if (BDBERR_DEADLOCK_ROWLOCK != rc) {
                logmsg(LOGMSG_ERROR, "%s:%d Bad rcode from get_row_lock_ix %d\n",
                        __func__, __LINE__, rc);
            }
            goto err_rc;
        }
    }

    else {
        goto err_rc;
    }

err_rc:
    /* All flavors of deadlock set bdberr to BDBERR_DEADLOCK */
    if (DB_LOCK_DEADLOCK == rc || BDBERR_DEADLOCK_ROWLOCK == rc ||
        BDBERR_DEADLOCK == rc || DB_REP_HANDLE_DEAD == rc) {
        memcpy(repokeyptr, repokey, r->keylen);
        *bdberr = BDBERR_DEADLOCK;
        rc = DB_LOCK_DEADLOCK;
    } else {
        *bdberr = rc;
        rc = -1;
    }

done:
    if (gotrecord) {
        /* Release the lock from the previous row */
        if (r->have_lock) {
            bdb_release_row_lock(bdb_state, &r->lk);
        }

        /* Just got a rowlock */
        if (have_lock) {
            /* Copy new lock */
            r->lk = newlk;

            /* We have a lock */
            r->have_lock = 1;
        }
        /* Pagelock mode */
        else {
            r->have_lock = 0;
        }

        /* Copy into lastkey */
        memcpy(r->lastkey, r->key, r->keylen);

        /* Positioned after lastkey copy */
        r->positioned = 1;

        /* We have a new genid */
        r->genid = cgenid;
    }

    return rc;
}

/* Finds
 * -
 * 1 Do the find
 * 2 Get a rowlock on the row you've found
 *
 * For an index btree:
 * 3 Do a single prev
 * 4 Success if the prev key is less than what you were originally looking for
 * 5 Retry if the prev key is equal to or more than what you were looking for
 *
 * Rationale:
 * If you are holding a rowlock for a record, nothing can be inserted directly
 * before it.  If a prev returns something equal to or greater than the original
 * probe something was inserted.
 *
 * Optimization:
 * 2.5 No need for prev if the original btree descent lands on the same page
 * as the returned row provided that we didn't have to release the pagelock
 * when we were acquiring the rowlock.
 *
 */

/* Find a row on an index */
static inline int bdb_berkdb_rowlocks_find_idx_int(bdb_berkdb_t *berkdb,
                                                   void *inkey, int keylen,
                                                   int how, int *bdberr)
{
    /* Rcode */
    int rc;
    int frc;

    /* New lock */
    DB_LOCK idxlk;

    /* End lock */
    DB_LOCK endlk;

    /* Verify find */
    int pg, idx;

    /* First leaf */
    unsigned int firstleaf;

    /* Light a flag if we got a record */
    int gotrecord = 0;

    /* bdb_state */
    bdb_state_type *bdb_state;

    /* Pointer to genid */
    unsigned long long *fgenid;

    /* Genid copy */
    unsigned long long cgenid = 0;

    /* Locker id */
    int cursor_lid;

    /* Key and Data */
    DBT key = {0};
    DBT data = {0};

    /* Pointers to cursors */
    bdb_rowlocks_tag_t *r;
    bdb_cursor_impl_t *cur;

    /* Local reference to bdb_state */
    bdb_state = berkdb->impl->bdb_state;

    /* Reference to cursor info */
    r = &berkdb->impl->u.row;
    cur = berkdb->impl->cur;

    /* Get lockerid */
    cursor_lid = bdb_get_lid_from_cursortran(cur->curtran);

    /* This should not be holding any locks */
    assert(0 == r->have_lock);

again:
    /* If this is null then we are in a retry */
    if (NULL == r->pagelock_cursor) {
        rc = open_pagelock_cursor(berkdb);

        if (0 != rc)
            goto err_rc;
    }

    /* Set key */
    key.data = r->keymem;
    key.size = keylen;
    key.ulen = key.dlen = r->keylen;
    key.flags = DB_DBT_USERMEM;
    r->key = r->keymem;

    /* Set data */
    data.data = r->dtamem;
    data.size =
        r->dtalen + (BDBC_DT == cur->type && bdb_state->ondisk_header ? 7 : 0);
    data.ulen = data.size;
    data.flags = DB_DBT_USERMEM;
    r->dta = r->dtamem;

    /* Copy key */
    memcpy(key.data, inkey, keylen);

    /* Set genid */
    fgenid = (unsigned long long *)r->dta;

    /* Set to DB_SET_RANGE and lock what we find to make sure we don't miss a
     * physically deleted record which gets rolled back */
    rc = cur_cget(berkdb, &key, &data, DB_SET_RANGE);

    /* Not found case */
    if (DB_NOTFOUND == rc) {
        /* Maxlock: find was set_range so there should be no other records */
        rc = get_row_lock_minmaxlk(berkdb, bdb_state, cursor_lid,
                                   r->pagelock_cursor, cur->idx, -1, 1, &endlk,
                                   NULL, BDB_LOCK_READ);

        /* 0 means the lsn didn't change- so we have rowlock and pagelock */
        if (0 == rc) {
            /* Return IX_NOTFND */
            frc = IX_NOTFND;

            /* Release immediately */
            bdb_release_row_lock(bdb_state, &endlk);

            /* Light eof flag */
            r->eof = 1;

            /* Success */
            goto done;
        }

        /* Retry if the page lsn changed */
        else if (DB_CUR_STALE == rc) {
            /* Cursor is closed by unpause */
            stale_pagelock_cursor(berkdb);

            /* Try again */
            goto again;
        } else {
            if (BDBERR_DEADLOCK_ROWLOCK != rc) {
                logmsg(LOGMSG_ERROR, "%s:%d Bad rcode from get_row_lock_ix %d\n",
                        __func__, __LINE__, rc);
            }
            goto err_rc;
        }
    }

    /* Found a row */
    else if (0 == rc) {
        r->lastdtasize = data.size;
        r->lastkeysize = key.size;

        /* Grab original leaf page */
        rc = r->pagelock_cursor->c_firstleaf(r->pagelock_cursor, &firstleaf);
        if (rc) {
            goto err_rc;
        }

        /* Grab page of current record */
        rc = r->pagelock_cursor->c_get_pageindex(r->pagelock_cursor, &pg, &idx);
        if (rc) {
            goto err_rc;
        }

        /* Get a rowlock */
        rc = get_row_lock(berkdb, bdb_state, cursor_lid, cur->idx,
                          r->pagelock_cursor, *fgenid, &idxlk, NULL,
                          BDB_LOCK_READ);

        /* Clear the eof flag */
        r->eof = 0;

        /* Got rowlock */
        if (0 == rc) {
            /* Verify found key against search key */
            if (DB_SET == how) {
                /* Compare keys */
                int cmp = memcmp(key.data, inkey, keylen);

                if (0 == cmp) {
                    frc = 0;
                } else {
                    frc = IX_NOTFND;
                }
            }

            /* Not found case */
            else {
                frc = rc;
            }

            /* Copy the genid before prev'ing */
            cgenid = *fgenid;

            /* If the page we found the record on is different from the first-
             * leaf (the page we landed on for the initial descent down the
             * btree), then a record matching this key could have been inserted
             * to the left of this one before I acquired the row-lock. */
            if (firstleaf != pg) {
                DBT pvkey = {0};
                DBT pvdata = {0};

                /* Set pvkey */
                pvkey.data = malloc(r->keylen);
                pvkey.size = keylen;
                pvkey.ulen = pvkey.dlen = r->keylen;
                pvkey.flags = DB_DBT_USERMEM;

                /* Set pvdata */
                pvdata.data = NULL;
                pvdata.size = 0;
                pvdata.doff = 0;
                pvdata.dlen = 0;
                pvdata.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

                /* Grab value of the previous record without moving cursor */
                rc = cur_cget(berkdb, &pvkey, &pvdata, DB_PREV_VALUE);

                /* The cursor remains positioned */
                if (DB_NOTFOUND == rc) {
                    /* Free memory */
                    free(pvkey.data);
                }

                /* Verify key */
                else if (0 == rc) {
                    int cmp = memcmp(pvkey.data, inkey, keylen);

                    free(pvkey.data);

                    /* Retry if we'd find something else */
                    if (cmp >= 0) {
                        /* Release my lock */
                        bdb_release_row_lock(bdb_state, &idxlk);

                        /* Close my cursor */
                        rc = close_pagelock_cursor(berkdb);

                        /* Error out on close cursor error */
                        if (rc)
                            goto err_rc;

                        /* Retry */
                        goto again;
                    }
                }

                /* Error */
                else {
                    /* Release my lock */
                    bdb_release_row_lock(bdb_state, &idxlk);

                    /* Free memory */
                    free(pvkey.data);

                    /* Punt */
                    goto err_rc;
                }
            }

            /* Grab the page index and page */
            rc = r->pagelock_cursor->c_get_pageindex(r->pagelock_cursor, &pg,
                                                     &idx);

            /* Stash in cursor */
            r->page = pg;
            r->index = idx;

            /* Light the gotrecord flag */
            gotrecord = 1;

            /* Success */
            goto done;
        }

        /* Retry if the page lsn changed */
        else if (DB_CUR_STALE == rc) {
            /* Cursor is closed by the failed unpause */
            stale_pagelock_cursor(berkdb);

            /* Try again */
            goto again;
        } else {
            if (BDBERR_DEADLOCK_ROWLOCK != rc) {
                logmsg(LOGMSG_ERROR, "%s:%d Bad rcode from get_row_lock_ix %d\n",
                        __func__, __LINE__, rc);
            }
            goto err_rc;
        }
    }

    /* Bad rcode from cur_cget */
    else {
        if (DB_LOCK_DEADLOCK != rc) {
            logmsg(LOGMSG_ERROR, "%s:%d Bad rcode from cur_cget %d\n", __func__,
                    __LINE__, rc);
        }
        goto err_rc;
    }

err_rc:
    /* All flavors of deadlock set bdberr to BDBERR_DEADLOCK */
    if (DB_LOCK_DEADLOCK == rc || BDBERR_DEADLOCK_ROWLOCK == rc ||
        BDBERR_DEADLOCK == rc || DB_REP_HANDLE_DEAD == rc) {
        *bdberr = BDBERR_DEADLOCK;
        frc = DB_LOCK_DEADLOCK;
    } else {
        *bdberr = rc;
        frc = -1;
    }

done:
    /* Cursor is in 'gather-rowlocks' mode */
    r->lock_mode = CURSOR_MODE_ROWLOCKS;

    /* Got a new record */
    if (gotrecord) {
        /* Should have released the lock previously */
        assert(!r->have_lock);

        /* Copy new lock */
        r->lk = idxlk;

        /* We have a lock */
        r->have_lock = 1;

        /* Copy into lastkey */
        memcpy(r->lastkey, r->key, r->keylen);

        /* positioned after lastkey copy */
        r->positioned = 1;

        /* We have a new genid */
        r->genid = cgenid;
    }

    return frc;
}

/* Find a row in a data file */
static inline int bdb_berkdb_rowlocks_find_dta_int(bdb_berkdb_t *berkdb,
                                                   void *inkey, int keylen,
                                                   int how, int *bdberr)
{
    /* Rcode */
    int rc;
    int frc;

    /* Data row locks */
    DB_LOCK dtalk;
    DB_LOCK nxtlk;

    /* Verify find */
    int pg, idx;

    /* Light a flag if we got a record */
    int gotrecord = 0;

    /* bdb_state */
    bdb_state_type *bdb_state;

    /* Pointer to genid */
    unsigned long long *fgenid;

    /* Genid copy */
    unsigned long long cgenid = 0;

    /* Locker id */
    int cursor_lid;

    /* Key and Data */
    DBT key = {0};
    DBT data = {0};

    /* Pointers to cursors */
    bdb_rowlocks_tag_t *r;
    bdb_cursor_impl_t *cur;

    /* Local reference to bdb_state */
    bdb_state = berkdb->impl->bdb_state;

    /* Reference to cursor info */
    r = &berkdb->impl->u.row;
    cur = berkdb->impl->cur;

    /* Get lockerid */
    cursor_lid = bdb_get_lid_from_cursortran(cur->curtran);

    /* This should not be holding any locks */
    assert(0 == r->have_lock);

    /* Set key */
    key.data = r->keymem;
    key.size = keylen;
    key.ulen = key.dlen = r->keylen;
    key.flags = DB_DBT_USERMEM;
    r->key = r->keymem;

    /* Set data */
    data.data = r->dtamem;
    data.size =
        r->dtalen + (BDBC_DT == cur->type && bdb_state->ondisk_header ? 7 : 0);
    data.ulen = data.size;
    data.flags = DB_DBT_USERMEM;
    r->dta = r->dtamem;

again:
    /* Copy key */
    memcpy(key.data, inkey, keylen);

    /* Genid is the key */
    fgenid = (unsigned long long *)r->key;

    /* Always lock what was asked for */
    rc = get_row_lock(berkdb, bdb_state, cursor_lid, -1, r->pagelock_cursor,
                      *fgenid, &dtalk, NULL, BDB_LOCK_READ);

    /* Cursor is closed by failed pause/unpause */
    if (DB_CUR_STALE == rc) {
        stale_pagelock_cursor(berkdb);
        goto again;
    }

    /* Punt on error */
    if (rc) {
        goto err_rc;
    }

    /* Open if needed */
    if (NULL == r->pagelock_cursor) {
        /* Open cursor */
        rc = open_pagelock_cursor(berkdb);

        /* Punt on failure */
        if (0 != rc) {
            goto err_rc;
        }
    }

    /* Set to DB_SET_RANGE and lock what we find to make sure we don't miss a
     * physically deleted record which gets rolled back */
    rc = cur_cget(berkdb, &key, &data, DB_SET_RANGE);

    /* Not found case */
    if (DB_NOTFOUND == rc) {
        /* Return IX_NOTFND */
        frc = IX_NOTFND;

        /* Release the lock immediately */
        bdb_release_row_lock(bdb_state, &dtalk);

        /* Light eof flag */
        r->eof = 1;

        /* Success */
        goto done;
    }

    /* Found a row while holding a rowlock */
    else if (0 == rc) {
        unsigned long long *g1 = (unsigned long long *)key.data;
        unsigned long long *g2 = (unsigned long long *)inkey;
        int cmp;

        r->lastdtasize = data.size;
        r->lastkeysize = key.size;

        /* Clear the eof flag */
        r->eof = 0;

        /* Get the page number and index */
        rc = r->pagelock_cursor->c_get_pageindex(r->pagelock_cursor, &pg, &idx);

        /* Stash in cursor */
        r->page = pg;
        r->index = idx;

        /* Compare actual or masked genid? */
        cmp = bdb_cmp_genids(*g1, *g2);

        /* Found it */
        if (0 == cmp) {
            /* We got a record */
            frc = 0;

            /* Set the gotrecord flag */
            gotrecord = 1;
        } else if (DB_SET_RANGE == how) {
            /* Get lock on what we actually found */
            rc = get_row_lock(berkdb, bdb_state, cursor_lid, -1,
                              r->pagelock_cursor, *g1, &nxtlk, NULL,
                              BDB_LOCK_READ);

            /* Release original row lock */
            bdb_release_row_lock(bdb_state, &dtalk);

            /* Cursor is closed by failed pause/unpause */
            if (DB_CUR_STALE == rc) {
                stale_pagelock_cursor(berkdb);
                goto again;
            }

            /* Punt on error */
            if (rc) {
                goto err_rc;
            }

            memcpy(&dtalk, &nxtlk, sizeof(dtalk));

            frc = rc;

            /* Light the gotrecord flag */
            gotrecord = 1;

        } else {
            assert(DB_SET == how);

            /* Not found */
            frc = IX_NOTFND;

            /* Release immediately */
            bdb_release_row_lock(bdb_state, &dtalk);

            /* Do not have the record */
            gotrecord = 0;

            goto done;
        }

        /* Copy the genid before prev'ing */
        cgenid = *fgenid;

        /* Success */
        goto done;
    }

    /* Bad rcode from cur_cget */
    else {
        if (DB_LOCK_DEADLOCK != rc) {
            logmsg(LOGMSG_ERROR, "%s:%d Bad rcode from cur_cget %d\n", __func__,
                    __LINE__, rc);
        }
        goto err_rc;
    }

err_rc:
    /* All flavors of deadlock set bdberr to BDBERR_DEADLOCK */
    if (DB_LOCK_DEADLOCK == rc || BDBERR_DEADLOCK_ROWLOCK == rc ||
        BDBERR_DEADLOCK == rc || DB_REP_HANDLE_DEAD == rc) {
        *bdberr = BDBERR_DEADLOCK;
        frc = DB_LOCK_DEADLOCK;
    } else {
        *bdberr = rc;
        frc = -1;
    }

done:
    /* Cursor is in rowlocks mode */
    r->lock_mode = CURSOR_MODE_ROWLOCKS;

    /* Got a new record */
    if (gotrecord) {
        /* Should have released the lock previously */
        assert(!r->have_lock);

        /* Copy new lock */
        r->lk = dtalk;

        /* We have a lock */
        r->have_lock = 1;

        /* Copy into lastkey */
        memcpy(r->lastkey, r->key, r->keylen);

        /* positioned after lastkey copy */
        r->positioned = 1;

        /* We have a new genid */
        r->genid = cgenid;
    }

    return frc;
}

/* Find a row on an index */
static inline int bdb_berkdb_rowlocks_find_int(bdb_berkdb_t *berkdb,
                                               void *inkey, int keylen, int how,
                                               int *bdberr)
{
    bdb_cursor_impl_t *cur = berkdb->impl->cur;

    /* Find on an index */
    if (BDBC_IX == cur->type) {
        return bdb_berkdb_rowlocks_find_idx_int(berkdb, inkey, keylen, how,
                                                bdberr);
    }

    /* Find on data */
    else {
        return bdb_berkdb_rowlocks_find_dta_int(berkdb, inkey, keylen, how,
                                                bdberr);
    }
}

/* Return 1 for absolute moves, 0 for relative */
static inline int absolute_move(int how)
{
    switch (how) {
    case DB_FIRST:
    case DB_LAST:
    case DB_SET:
    case DB_SET_RANGE:
        return 1;
        break;

    case DB_NEXT:
    case DB_PREV:
        return 0;
        break;

    default:
        logmsg(LOGMSG_FATAL, "Unexpected move type, %d\n", how);
        abort();
    }
}

/* Process entrance and exit lockcounts */
static inline int lockcount_trace(bdb_berkdb_t *berkdb, const char *func,
                                  int how, int enter_lkcount, int exit_lkcount)
{
    bdb_state_type *bdb_state = berkdb->impl->bdb_state;
    int cursor_count;
    u_int32_t page_lock_count;
    bdb_cursor_impl_t *cur = berkdb->impl->cur;

    /* I'm just looking for which function to instrument */
    if (debug_trace(berkdb) && exit_lkcount > 100 &&
        exit_lkcount > enter_lkcount) {
        logmsg(LOGMSG_USER,
               "thd %p function %s %s %d lock-count incremented from "
               "%d to %d\n",
               (void *)pthread_self(), func, cur->type == BDBC_IX ? "index" : "stripe", cur->idx, enter_lkcount,
               exit_lkcount);

        /* Grab the number of cursors */
        cursor_count = cur->ifn->count(cur->ifn->countarg);

        /* Grab the number of pagelocks */
        bdb_state->dbenv->lock_locker_pagelockcount(
            bdb_state->dbenv, cur->curtran->lockerid, &page_lock_count);

        if (page_lock_count > cursor_count) {
            logmsg(LOGMSG_USER,
                   "thd %p function %s %s %d pagelock-count is "
                   "%d cursor count is  %d\n",
                   (void *)pthread_self(), func, cur->type == BDBC_IX ? "index" : "stripe", cur->idx, page_lock_count,
                   cursor_count);
        }

        if (cur->max_page_locks < page_lock_count) {
            logmsg(LOGMSG_USER,
                   "thd %p function %s %s %d incrementing max "
                   "pagelock count from %d to %d\n",
                   (void *)pthread_self(), func, cur->type == BDBC_IX ? "index" : "stripe", cur->idx,
                   cur->max_page_locks, page_lock_count);

            cur->max_page_locks = page_lock_count;
        }
    }

    return 0;
}

/* Entering rowlocks */
static inline int bdb_berkdb_rowlocks_enter(bdb_berkdb_t *berkdb,
                                            const char *func, int how,
                                            const char *srckey, int keylen,
                                            int *lkcount, int *bdberr)
{
    /* Determine if this is an absolute or relative move */
    int absolute = absolute_move(how), rc, ret = 0;

    /* Reset bdberr */
    *bdberr = 0;

    /* Pointers to cursors */
    bdb_rowlocks_tag_t *r = &berkdb->impl->u.row;
    bdb_cursor_impl_t *cur = berkdb->impl->cur;

    /* Print trace showing we are here */
    if (debug_trace(berkdb)) {
        bdb_state_type *bdb_state = berkdb->impl->bdb_state;

        if (keylen > 0) {
            /* Search key stack variables */
            char *srcmemp;
            char *srckeyp = "(NULL)";

            /* Search key to hex */
            if (srckey) {
                srcmemp = alloca((2 * keylen) + 2);
                srckeyp = util_tohex(srcmemp, srckey, keylen);
            }

            /* Print */
            logmsg(LOGMSG_USER, "Cur %p thd %p %s tbl %s %s how=%d srch='%s'\n", berkdb, (void *)pthread_self(), func,
                   bdb_state->name, curtypetostr(cur->type), how, srckeyp);
        } else {
            /* Print */
            logmsg(LOGMSG_USER, "Cur %p thd %p %s tbl %s %s how=%d\n", berkdb, (void *)pthread_self(), func,
                   bdb_state->name, curtypetostr(cur->type), how);
        }

        /* Grab the lock count if debugging */
        bdb_state->dbenv->lock_locker_lockcount(
            bdb_state->dbenv, cur->curtran->lockerid, (u_int32_t *)lkcount);
    }

    /* Mark cursor as active */
    r->active = 1;

    /* Increment num_nexts */
    if (DB_NEXT == how) {
        berkdb->impl->num_nexts++;

        /* XXX Remove these */
        berkdb->impl->num_nexts_since_deadlock++;
        berkdb->impl->num_nexts_since_open++;
        berkdb->impl->num_nexts_since_stale++;
    }

    /* Reset use_bulk */
    else {
        r->use_bulk = 0;
        berkdb->impl->num_nexts = 0;
    }

    /* Release lock for absolute moves */
    if (absolute) {
        /* Release current rowlock. */
        if (r->have_lock) {
            r->have_lock = 0;
            bdb_release_row_lock(berkdb->impl->bdb_state, &r->lk);
        }

        /* Don't allow repositioning */
        r->positioned = 0;

        /* Close rather than unpause */
        close_pagelock_cursor(berkdb);
    }

    /* Return immediately if there's no cursor */
    if (!r->pagelock_cursor || !r->paused) {
        return 0;
    }

    if (r->paused) {
        if ((rc = (r->pagelock_cursor->c_unpause(r->pagelock_cursor,
                                                 &r->pagelock_pause))) != 0) {
            /* Error on unpause trace */
            if (debug_trace(berkdb)) {
                logmsg(LOGMSG_USER, 
                        "Cur %p %s nullifying unpaused cursor on lsn change.\n",
                        berkdb, func);
            }

            /* This closes on failure so just set to null */
            r->pagelock_cursor = NULL;

            /* Closed is unpaused */
            r->paused = 0;

            /* Reposition immediately for failed unpause on pagelock mode */
            if (!absolute && CURSOR_MODE_NO_ROWLOCKS == r->lock_mode) {
                *bdberr = BDBERR_NEED_REPOSITION;
                ret = -1;
            }
        } else {
            if (debug_trace(berkdb)) {
                /* Pagelock pause is an lsn. */
                DB_LSN *lsn = (DB_LSN *)&r->pagelock_pause, chklsn;

                logmsg(LOGMSG_USER, "Cur %p %s unpaused cursor lsn='%d:%d'.\n",
                        berkdb, func, lsn->file, lsn->offset);

                /* To be completely sane, lets get the lsn again immediately &
                 * compare */
                r->pagelock_cursor->c_get_pagelsn(r->pagelock_cursor, &chklsn);

                /* If it's not the same, abort */
                assert(chklsn.file == lsn->file &&
                       chklsn.offset == lsn->offset);
            }

            r->paused = 0;
        }
    }

    /* Inactive if there were errors */
    if (ret)
        r->active = 0;

    /* Ready */
    return ret;
}

/* Exiting rowlocks */
static inline int bdb_berkdb_rowlocks_exit(bdb_berkdb_t *berkdb,
                                           const char *func, int how,
                                           const char *srckey,
                                           const char *fndkey, int keylen,
                                           int rcode, int *lkcount, int *bdberr)
{
    int rc;
    /* Pointers to cursors */
    bdb_rowlocks_tag_t *r = &berkdb->impl->u.row;
    bdb_cursor_impl_t *cur = berkdb->impl->cur;

    /* Print lots of trace here if enabled */
    if (debug_trace(berkdb)) {
        bdb_state_type *bdb_state = berkdb->impl->bdb_state;
        DB_LSN lsn;

        cur_pagelsn(berkdb, &lsn);

        /* Grab the lock count */
        bdb_state->dbenv->lock_locker_lockcount(
            bdb_state->dbenv, cur->curtran->lockerid, (u_int32_t *)lkcount);

        if (keylen > 0) {
            /* Search key stack variables */
            char *srcmemp;
            char *srckeyp = "(NULL)";

            /* Found key stack variables */
            char *fndmemp;
            char *fndkeyp = "(NULL)";

            /* Search key to hex */
            if (srckey) {
                srcmemp = alloca((2 * keylen) + 2);
                srckeyp = util_tohex(srcmemp, srckey, keylen);
            }

            /* Found key to hex */
            if (fndkey) {
                fndmemp = alloca((2 * keylen) + 2);
                fndkeyp = util_tohex(fndmemp, fndkey, keylen);
            }

            /* Print */
            logmsg(LOGMSG_USER, 
                    "Cur %p %s tbl %s %s how=%d srch='%s' fnd='%s' returns %d"
                    " pg=%d idx=%d lsn='%d:%d' bdberr=%d\n",
                    berkdb, func, bdb_state->name, curtypetostr(cur->type), how,
                    srckeyp, fndkeyp, rcode, r->page, r->index, lsn.file,
                    lsn.offset, *bdberr);
        } else {
            /* Print */
            logmsg(LOGMSG_USER, "Cur %p %s tbl %s type %s how=%d page %d index "
                                "%d returns %d lsn='%d:%d' bdberr=%d\n",
                   berkdb, func, bdb_state->name, curtypetostr(cur->type), how,
                   rcode, r->page, r->index, lsn.file, lsn.offset, *bdberr);
        }
    }

    /* No longer active */
    r->active = 0;

    /* Collect rcodes and ops */
    r->last_op = how;
    r->last_rc = rcode;
    r->last_bdberr = *bdberr;

    /* Return immediately if I'm not positioned */
    if (!r->pagelock_cursor) {
        return 0;
    }

    /* Close the cursor on deadlock */
    if (rcode && BDBERR_DEADLOCK == *bdberr) {
        if (debug_trace(berkdb)) {
            logmsg(LOGMSG_USER, "Cur %p closing on deadlock.\n", berkdb);
        }

        /* Close the cursor */
        close_pagelock_cursor(berkdb);

        /* Clear nexts */
        berkdb->impl->num_nexts_since_deadlock = 0;
    }

    /* Don't release the pagelock if we're not gathering rowlocks */
    else if (!r->paused && CURSOR_MODE_ROWLOCKS == r->lock_mode) {
        /* Pause cursor.  You have to even in non-rowlocks mode. */
        rc =
            r->pagelock_cursor->c_pause(r->pagelock_cursor, &r->pagelock_pause);

        if (0 == rc) {
            if (debug_trace(berkdb)) {
                /* Pagelock pause is an lsn - dump it here */
                DB_LSN *lsn = (DB_LSN *)&r->pagelock_pause;

                logmsg(LOGMSG_USER, "Cur %p paused, pause-lsn='%d:%d'\n", berkdb,
                        lsn->file, lsn->offset);
            }

            /* Set paused flag */
            r->paused = 1;
        } else {
            if (debug_trace(berkdb)) {
                logmsg(LOGMSG_USER, "Cur %p got %d on pause, closing.\n", berkdb,
                        rc);
            }

            /* Close the cursor */
            close_pagelock_cursor(berkdb);
        }
    }

    /* Light a flag.  We have to release all pagelocks before getting any
       rowlocks. */
    else if (!r->paused && CURSOR_MODE_NO_ROWLOCKS == r->lock_mode) {
        *cur->pagelockflag = 1;
    }

    return 0;
}

/* Rowlocks enter & exit */
static inline int bdb_berkdb_rowlocks_firstlast(bdb_berkdb_t *berkdb, int how,
                                                int *bdberr)
{
    int enter_lkcnt, exit_lkcnt;
    int rc;

    /* Pointers to cursors */
    bdb_rowlocks_tag_t *r = &berkdb->impl->u.row;

    /* We need these */
    assert(DB_FIRST == how || DB_LAST == how);

    /* Set state upon entering */
    rc = bdb_berkdb_rowlocks_enter(berkdb, __func__, how, NULL, 0, &enter_lkcnt,
                                   bdberr);

    /* Return immediately if this failed */
    if (rc)
        return rc;

    /* Call internal first or last */
    rc = bdb_berkdb_rowlocks_firstlast_int(berkdb, how, bdberr);

    /* Pause cursor on exit */
    bdb_berkdb_rowlocks_exit(berkdb, __func__, how, NULL, r->lastkey, r->keylen,
                             rc, &exit_lkcnt, bdberr);

    /* Print trace if the lock-count went up */
    lockcount_trace(berkdb, __func__, how, enter_lkcnt, exit_lkcnt);

    /* Return */
    return rc;
}

/* Rowlocks enter & exit */
static inline int bdb_berkdb_rowlocks_nextprev(bdb_berkdb_t *berkdb, int how,
                                               int *bdberr)
{
    int enter_lkcnt, exit_lkcnt;
    int rc;

    /* Pointers to cursors */
    bdb_rowlocks_tag_t *r = &berkdb->impl->u.row;

    /* Set state upon entering */
    rc = bdb_berkdb_rowlocks_enter(berkdb, __func__, how, NULL, 0, &enter_lkcnt,
                                   bdberr);

    /* Return immediately if this failed */
    if (rc)
        return rc;

    /* Call internal nextprev */
    rc = bdb_berkdb_rowlocks_nextprev_int(berkdb, how, bdberr);

    /* Pause cursor on exit*/
    bdb_berkdb_rowlocks_exit(berkdb, __func__, how, NULL, r->lastkey, r->keylen,
                             rc, &exit_lkcnt, bdberr);

    lockcount_trace(berkdb, __func__, how, enter_lkcnt, exit_lkcnt);

    /* Return */
    return rc;
}

/* Prototype this */
int bdb_berkdb_rowlocks_move(bdb_berkdb_t *pberkdb, int dir, int *bdberr);

/* Rowlocks enter & exit */
int bdb_berkdb_rowlocks_find(bdb_berkdb_t *berkdb, void *key, int keylen,
                             int how, int *bdberr)
{
    int enter_lkcnt, exit_lkcnt;
    int rc;

    /* Pointers to cursors */
    bdb_rowlocks_tag_t *r = &berkdb->impl->u.row;

    /* Find-and-skip uses find for move. */
    if (how != DB_SET && how != DB_SET_RANGE) {
        return bdb_berkdb_rowlocks_move(berkdb, how, bdberr);
    }

    /* Set state upon entering */
    rc = bdb_berkdb_rowlocks_enter(berkdb, __func__, how, key, keylen,
                                   &enter_lkcnt, bdberr);

    /* Return immediately if this failed */
    if (rc)
        return rc;

    /* Call internal find */
    rc = bdb_berkdb_rowlocks_find_int(berkdb, key, keylen, how, bdberr);

    /* Pause cursor on exit */
    bdb_berkdb_rowlocks_exit(berkdb, __func__, how, key, r->lastkey, keylen, rc,
                             &exit_lkcnt, bdberr);

    lockcount_trace(berkdb, __func__, how, enter_lkcnt, exit_lkcnt);

    /* Return */
    return rc;
}

/* Wrapper for firstlast */
int bdb_berkdb_rowlocks_first(bdb_berkdb_t *berkdb, int *bdberr)
{
    return bdb_berkdb_rowlocks_firstlast(berkdb, DB_FIRST, bdberr);
}

/* Wrapper for firstlast */
int bdb_berkdb_rowlocks_last(bdb_berkdb_t *berkdb, int *bdberr)
{
    return bdb_berkdb_rowlocks_firstlast(berkdb, DB_LAST, bdberr);
}

/* Wraper for nextprev */
int bdb_berkdb_rowlocks_next(bdb_berkdb_t *berkdb, int *bdberr)
{
    return bdb_berkdb_rowlocks_nextprev(berkdb, DB_NEXT, bdberr);
}

/* Wraper for nextprev */
int bdb_berkdb_rowlocks_prev(bdb_berkdb_t *berkdb, int *bdberr)
{
    return bdb_berkdb_rowlocks_nextprev(berkdb, DB_PREV, bdberr);
}

/* Not sure we need this for rowlocks */
int bdb_berkdb_rowlocks_get_pageindex(bdb_berkdb_t *pberkdb, int *page,
                                      int *index, int *bdberr)
{
    bdb_rowlocks_tag_t *r;

    /* Cursor */
    r = &pberkdb->impl->u.row;

    /* Copy information */
    if (page)
        *page = r->page;
    if (index)
        *index = r->index;

    return 0;
}

int bdb_berkdb_rowlocks_get_fileid(bdb_berkdb_t *pberkdb, void *pfileid,
                                   int *bdberr)
{
    bdb_rowlocks_tag_t *r;

    /* Cursor */
    r = &pberkdb->impl->u.row;

    memcpy(pfileid, r->fileid, DB_FILE_ID_LEN);

    return 0;
}

/* Wrapper */
int bdb_berkdb_rowlocks_move(bdb_berkdb_t *pberkdb, int dir, int *bdberr)
{
    int rc = 0;
    int debugtrc = debug_trace(pberkdb);
    bdb_cursor_impl_t *cur = pberkdb->impl->cur;

    switch (dir) {
    case DB_FIRST:
        rc = pberkdb->first(pberkdb, bdberr);
        if (debugtrc) {
            if (cur->type == BDBC_DT) {
                logmsg(LOGMSG_USER, "Cur %p <first> stripe=%d rc=%d bdberr=%d\n",
                        pberkdb, cur->idx, rc, *bdberr);
            } else {
                logmsg(LOGMSG_USER, "Cur %p <first> index=%d rc=%d bdberr=%d\n",
                        pberkdb, cur->idx, rc, *bdberr);
            }
        }
        return rc;
        break;

    case DB_LAST:
        rc = pberkdb->last(pberkdb, bdberr);
        if (debugtrc) {
            if (cur->type == BDBC_DT) {
                logmsg(LOGMSG_USER, "Cur %p <last> stripe=%d rc=%d bdberr=%d\n",
                        pberkdb, cur->idx, rc, *bdberr);
            } else {
                logmsg(LOGMSG_USER, "Cur %p <last> index=%d rc=%d bdberr=%d\n",
                        pberkdb, cur->idx, rc, *bdberr);
            }
        }
        return rc;
        break;

    case DB_PREV:
        rc = pberkdb->prev(pberkdb, bdberr);
        if (debugtrc) {
            if (cur->type == BDBC_DT) {
                logmsg(LOGMSG_USER, "Cur %p <prev> stripe=%d rc=%d bdberr=%d\n",
                        pberkdb, cur->idx, rc, *bdberr);
            } else {
                logmsg(LOGMSG_USER, "Cur %p <prev> index=%d rc=%d bdberr=%d\n",
                        pberkdb, cur->idx, rc, *bdberr);
            }
        }
        return rc;
        break;

    case DB_NEXT:
        rc = pberkdb->next(pberkdb, bdberr);
        if (debugtrc) {
            if (cur->type == BDBC_DT) {
                logmsg(LOGMSG_USER, "Cur %p <next> stripe=%d rc=%d bdberr=%d\n",
                        pberkdb, cur->idx, rc, *bdberr);
            } else {
                logmsg(LOGMSG_FATAL, "Cur %p <next> index=%d rc=%d bdberr=%d\n",
                        pberkdb, cur->idx, rc, *bdberr);
            }
        }
        return rc;
        break;

    default:
        logmsg(LOGMSG_FATAL, "%s: unknown dir %d\n", __func__, dir);
        abort();
        *bdberr = BDBERR_BADARGS;
        return -1;
    }
}

int bdb_berkdb_rowlocks_defer_update_shadows(bdb_berkdb_t *berkdb) { return 0; }

/* Release the pagelock cursor if we're holding it */
int bdb_berkdb_rowlocks_pause(bdb_berkdb_t *berkdb, int *bdberr)
{
    int rc;

    /* Pointers to cursors */
    bdb_rowlocks_tag_t *r = &berkdb->impl->u.row;

    *bdberr = 0;

    /* Return if there's no cursor */
    if (!r->pagelock_cursor)
        return 0;

    /* Return if this is paused */
    if (r->paused) {
        return 0;
    }

    /* Return if this is the active cursor */
    if (r->active)
        return 0;

    /* Must be in NO_ROWLOCKS mode */
    if (CURSOR_MODE_NO_ROWLOCKS != r->lock_mode)
        return 0;

    /* Pause this cursor */
    rc = r->pagelock_cursor->c_pause(r->pagelock_cursor, &r->pagelock_pause);

    if (!rc) {
        /* Mark as paused */
        r->paused = 1;
    } else {
        close_pagelock_cursor(berkdb);
    }

    return 0;
}

int bdb_berkdb_rowlocks_get_pagelsn(bdb_berkdb_t *berkdb, DB_LSN *lsn,
                                    int *bdberr)
{
    cur_pagelsn(berkdb, lsn);
    return 0;
}

int bdb_berkdb_rowlocks_dta(bdb_berkdb_t *berkdb, char **dta, int *bdberr)
{
    bdb_rowlocks_tag_t *r;

    r = &berkdb->impl->u.row;
    *dta = (char *)r->dta;

    if (debug_trace(berkdb)) {
        bdb_state_type *bdb_state = berkdb->impl->bdb_state;
        bdb_cursor_impl_t *cur = berkdb->impl->cur;
        char *srcmemp;
        char *srcdtap;

        srcmemp = alloca((2 * r->dtalen) + 2);
        srcdtap = util_tohex(srcmemp, r->dta, r->dtalen);

        logmsg(LOGMSG_USER, "Cur %p %s tbl %s %s dta='%s'\n", berkdb, __func__,
                bdb_state->name, curtypetostr(cur->type), srcdtap);
    }

    return 0;
}

int bdb_berkdb_rowlocks_key(bdb_berkdb_t *berkdb, char **key, int *bdberr)
{
    bdb_rowlocks_tag_t *r;

    r = &berkdb->impl->u.row;
    *key = (char *)r->key;

    if (debug_trace(berkdb)) {
        bdb_state_type *bdb_state = berkdb->impl->bdb_state;
        bdb_cursor_impl_t *cur = berkdb->impl->cur;
        char *srcmemp;
        char *srckeyp;

        srcmemp = alloca((2 * r->keylen) + 2);
        srckeyp = util_tohex(srcmemp, r->key, r->keylen);

        logmsg(LOGMSG_USER, "Cur %p %s tbl %s %s key='%s'\n", berkdb, __func__,
                bdb_state->name, curtypetostr(cur->type), srckeyp);
    }

    return 0;
}

int bdb_berkdb_rowlocks_keysize(bdb_berkdb_t *berkdb, int *keysize, int *bdberr)
{
    bdb_rowlocks_tag_t *r;

    r = &berkdb->impl->u.row;
    *keysize = r->lastkeysize;
    return 0;
}

int bdb_berkdb_rowlocks_dtasize(bdb_berkdb_t *berkdb, int *dtasize, int *bdberr)
{
    bdb_rowlocks_tag_t *r;

    r = &berkdb->impl->u.row;
    *dtasize = r->lastdtasize;
    return 0;
}

int bdb_berkdb_rowlocks_allow_optimized(bdb_berkdb_t *berkdb)
{
    bdb_rowlocks_tag_t *r;

    r = &berkdb->impl->u.row;
    r->force_rowlocks = 0;
    return 0;
}

int bdb_berkdb_rowlocks_get_skip_stat(struct bdb_berkdb *berkdb,
                                      u_int64_t *nextcount,
                                      u_int64_t *skipcount)
{
    bdb_rowlocks_tag_t *r;
    bdb_cursor_impl_t *cur;

    r = &berkdb->impl->u.row;
    cur = berkdb->impl->cur;

    if (!r->pagelock_cursor || cur->type != BDBC_DT) {
        return -1;
    }

    return r->pagelock_cursor->c_skip_stat(r->pagelock_cursor, nextcount,
                                           skipcount);
}

int bdb_berkdb_rowlocks_prevent_optimized(bdb_berkdb_t *berkdb)
{
    bdb_rowlocks_tag_t *r;

    r = &berkdb->impl->u.row;
    r->force_rowlocks = 1;
    return 0;
}

int bdb_berkdb_rowlocks_init(bdb_berkdb_t *berkdb, DB *db, int *bdberr)
{
    bdb_state_type *bdb_state;
    bdb_rowlocks_tag_t *r;
    bdb_cursor_impl_t *cur;

    cur = berkdb->impl->cur;
    r = &berkdb->impl->u.row;
    bdb_state = berkdb->impl->bdb_state;

    bzero(&r->lk, sizeof(DB_LOCK));

    r->positioned = 0;
    r->last_op = -1;
    r->last_rc = 0;

    if (berkdb->impl->cur->type == BDBC_IX) {
        /* index */
        r->keylen = bdb_state->ixlen[cur->idx];
        if (bdb_state->ixdups[cur->idx])
            r->keylen += sizeof(unsigned long long);
        r->dtalen = sizeof(unsigned long long);
        if (bdb_state->ixdta[cur->idx]) {
            int datacopy_size = bdb_state->ixdtalen[cur->idx] > 0 ? bdb_state->ixdtalen[cur->idx] : bdb_state->lrl;
            r->dtalen += datacopy_size + (bdb_state->ondisk_header ? 7 : 0);
        }
        r->keymem = malloc(r->keylen + 1);
        r->key = r->keymem;
        r->lastkey = malloc(r->keylen);
        r->dtamem = malloc(r->dtalen);
        r->dta = r->dtamem;
    } else if (berkdb->impl->cur->type == BDBC_DT) {
        /* data */
        r->keylen = 8;
        r->keymem = malloc(8);
        r->key = r->keymem;
        r->lastkey = malloc(8);
        r->dtalen = bdb_state->lrl;
        r->dtamem = malloc(r->dtalen + (bdb_state->ondisk_header ? 7 : 0));
        r->dta = r->dtamem;
    }

    r->have_lock = 0;

    if (debug_trace(berkdb)) {
        logmsg(LOGMSG_USER, "Cur %p cursor inited at %p.\n", berkdb, cur);
    }

    return 0;
}

int bdb_berkdb_rowlocks_close(bdb_berkdb_t *berkdb, int *bdberr)
{
    bdb_rowlocks_tag_t *r;
    int rc;
    bdb_state_type *bdb_state;
    bdb_cursor_impl_t *cur;

    bdb_state = berkdb->impl->bdb_state;
    cur = berkdb->impl->cur;

    if (debug_trace(berkdb)) {
        if (cur->type == BDBC_DT) {
            logmsg(LOGMSG_USER, "Cur %p stripe=%d close\n", cur, cur->idx);
        } else {
            logmsg(LOGMSG_USER, "Cur %p index=%d close\n", cur, cur->idx);
        }
    }

    r = &berkdb->impl->u.row;
    if (r->dtamem)
        free(r->dtamem);
    if (r->keymem)
        free(r->keymem);
    if (r->lastkey)
        free(r->lastkey);
    if (r->bulk.data)
        free(r->bulk.data);
    if (r->odh_tmp)
        free(r->odh_tmp);
    if (r->pagelock_cursor) {
        rc = close_pagelock_cursor(berkdb);
        if (rc) {
            if (rc == DB_LOCK_DEADLOCK || rc == DB_REP_HANDLE_DEAD)
                *bdberr = BDBERR_DEADLOCK;
            else
                *bdberr = rc;

            if (debug_trace(berkdb)) {
                logmsg(LOGMSG_USER, "Cur %p close error rc=%d bdberr=%d\n", cur, rc,
                        *bdberr);
            }

            return -1;
        }
    }

    if (r->have_lock) {
        bdb_release_row_lock(bdb_state, &r->lk);
        r->have_lock = 0;
    }

    /* Release cursor */
    free(berkdb);

    return 0;
}
