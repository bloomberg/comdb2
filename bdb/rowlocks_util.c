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

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include "bdb_int.h"
#include <assert.h>
#include "locks.h"
#include "logmsg.h"

/* Get a rowlock.  If the caller has provided a cursor, verify the pagelsn and
 * return a good rcode only if it hasn't changed. */
int bdb_get_row_lock_pfunc(bdb_state_type *bdb_state, int rowlock_lid, int idx,
                           DBC *dbcp, int (*pfunc)(void *), void *parg,
                           unsigned long long genid, DB_LOCK *rlk, DBT *lkname,
                           int how)
{
    DB_LOCK lk = {0}, *lkptr;
    int rc;

    /* Return the lock if the caller wants it */
    if (rlk) {
        lkptr = rlk;
    } else {
        lkptr = &lk;
    }

    /* Zero */
    bzero(lkptr, sizeof(*lkptr));

    /* Attempt to get the rowlock, block if cursor is null */
    rc = bdb_lock_row_fromlid(bdb_state, rowlock_lid, idx, genid, how, lkptr,
                              lkname, dbcp || pfunc ? 1 : 0);

    /* Return now if cursor is null */
    if (!dbcp && !pfunc) {
        if (rc && (DB_LOCK_DEADLOCK != rc && BDBERR_DEADLOCK_ROWLOCK != rc)) {
            logmsg(LOGMSG_ERROR, "%s:%d rowlock lock error, rc=%d\n", __FILE__,
                    __LINE__, rc);
        }
        return rc;
    }

    /* Would have blocked */
    if (rc == DB_LOCK_NOTGRANTED) {
        /* Stash the lsn here */
        DBCPS plsn = {{0}};

        /* Pause all pagelock cursors in this txn */
        if (pfunc) {
            rc = pfunc(parg);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s:%d error running pausefunc, rc=%d\n",
                        __FILE__, __LINE__, rc);
                return rc;
            }
        }

        /* Release pagelock */
        if (dbcp) {
            rc = dbcp->c_pause(dbcp, &plsn);
            if (rc) {
                /* Print a message for anything that's not a deadlock */
                if (DB_LOCK_DEADLOCK != rc) {
                    logmsg(LOGMSG_ERROR, "%s:%d error pausing cursor, rc=%d\n",
                            __FILE__, __LINE__, rc);
                }
                /* Return CURSOR_STALE so the caller knows the cursor was
                 * closed. */
                return DB_CUR_STALE;
            }
        }

#if defined DEBUG_ROWLOCKS
        int pagelockcount = 0;
        /*
         * It turns out that there are (apparently) cases where a txn will hold
         * onto a pagelock even after a cursor has been closed.  I'm not sure
         * what conditions cause this, but it seems like a bug.  It happens
         * rarely, so I'm going to code around it for now.
         */
        if ((rc = bdb_state->dbenv->lock_locker_pagelockcount(
                 bdb_state->dbenv, rowlock_lid, &pagelockcount)) ||
            pagelockcount > 0) {
            logmsg(LOGMSG_FATAL, 
                   "Error: blocking on rowlock when I'm holding pages!\n");
            abort();
        }
#endif

        /* Blocking call - the LOCK_NOPAGELK means that this will return
         * immediately with DEADLOCK if this lockerid is holding any pagelocks.
         */
        rc = bdb_lock_row_fromlid_int(bdb_state, rowlock_lid, idx, genid, how,
                                      lkptr, lkname, 0, DB_LOCK_NOPAGELK);
        if (rc) {
            if (BDBERR_DEADLOCK_ROWLOCK != rc) {
                logmsg(LOGMSG_ERROR, "%s:%d rowlock lock error, rc=%d\n", __FILE__,
                        __LINE__, rc);
            }
            return rc;
        }

        /* Unpause */
        if (dbcp) {
            rc = dbcp->c_unpause(dbcp, &plsn);

            /* Page changed from under us */
            if (0 != rc) {
                /* Remove this */
                assert(DB_LOCK_DEADLOCK == rc || DB_CUR_STALE == rc);

                /* Unlock row */
                bdb_release_row_lock(bdb_state, lkptr);

                /* Print errors we don't expect */
                if (DB_LOCK_DEADLOCK != rc && DB_CUR_STALE != rc) {
                    logmsg(LOGMSG_ERROR, "%s:%d rowlock lock error, rc=%d\n",
                            __FILE__, __LINE__, rc);
                }

                /* Return CURSOR_STALE so the caller knows the cursor was
                 * closed. */
                return DB_CUR_STALE;
            }
        }

        /* Success */
        return 0;
    }
    /* Another error */
    else if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d lock_row on data returned %d.\n", __FILE__,
                __LINE__, rc);
        return rc;
    }

    /* Success */
    return 0;
}

int bdb_get_row_lock(bdb_state_type *bdb_state, int rowlock_lid, int idx,
                     DBC *dbcp, unsigned long long genid, DB_LOCK *rlk,
                     DBT *lkname, int how)
{
    return bdb_get_row_lock_pfunc(bdb_state, rowlock_lid, idx, dbcp, NULL, NULL,
                                  genid, rlk, lkname, how);
}

/* Get a minmax lock: return a good rcode if the pagelsn has not changed.
 * If the caller has provided a cursor, return a DB_CUR_STALE if cursor's
 * page lsn has changed, so the caller can retry the search. */
int bdb_get_row_lock_minmaxlk_pfunc(bdb_state_type *bdb_state, int rowlock_lid,
                                    DBC *dbcp, int (*pfunc)(void *), void *parg,
                                    int ixnum, int stripe, int minmax,
                                    DB_LOCK *rlk, DBT *lkname, int how)
{
    DB_LOCK lk, *lkptr;
    int rc;

    /* Return the lock if the caller wants it */
    if (rlk) {
        lkptr = rlk;
    } else {
        lkptr = &lk;
    }

    /* Zero */
    bzero(lkptr, sizeof(*lkptr));

    /* Acquire min or max lock, block if cursor is null */
    rc = bdb_lock_minmax(bdb_state, ixnum, stripe, minmax, how, lkptr, lkname,
                         rowlock_lid, dbcp || pfunc ? 1 : 0);

    /* Return now if cursor is null */
    if (!dbcp && !pfunc) {
        if (rc && DB_LOCK_DEADLOCK != rc && BDBERR_DEADLOCK_ROWLOCK != rc) {
            logmsg(LOGMSG_ERROR, "%s:%d minmax lock error, rc=%d\n", __FILE__,
                    __LINE__, rc);
        }
        return rc;
    }

    /* Would block */
    if (rc == DB_LOCK_NOTGRANTED) {
        DBCPS plsn = {{0}};

        if (pfunc) {
            rc = pfunc(parg);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s:%d error running pausefunc, rc=%d\n",
                        __FILE__, __LINE__, rc);
                return rc;
            }
        }

        /* Release pagelock */
        if (dbcp) {
            rc = dbcp->c_pause(dbcp, &plsn);
            if (0 != rc) {
                /* Print a message for anything that's not a deadlock */
                if (DB_LOCK_DEADLOCK != rc) {
                    logmsg(LOGMSG_ERROR, "%s:%d error pausing cursor, rc=%d\n",
                            __FILE__, __LINE__, rc);
                }
                return rc;
            }
        }

        /* Blocking call */
        rc = bdb_lock_minmax(bdb_state, ixnum, stripe, minmax, how, lkptr,
                             lkname, rowlock_lid, 0);
        if (rc) {
            if (BDBERR_DEADLOCK_ROWLOCK != rc) {
                logmsg(LOGMSG_ERROR, "%s:%d rowlock lock error, rc=%d\n", __FILE__,
                        __LINE__, rc);
            }
            return rc;
        }

        /* Unpause */
        if (dbcp) {
            rc = dbcp->c_unpause(dbcp, &plsn);

            /* Page changed from under us */
            if (0 != rc) {
                /* Remove this */
                assert(DB_LOCK_DEADLOCK == rc || DB_CUR_STALE == rc);

                /* Unlock row */
                bdb_release_row_lock(bdb_state, lkptr);

                /* Print errors we don't expect */
                if (DB_LOCK_DEADLOCK != rc && DB_CUR_STALE != rc) {
                    logmsg(LOGMSG_ERROR, "%s:%d rowlock lock error, rc=%d\n",
                            __FILE__, __LINE__, rc);
                }
                return rc;
            }
        }

        /* Success */
        return 0;
    }

    /* Error */
    else if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d lock_%s on index returned %d.\n", __FILE__,
                __LINE__, (0 == how) ? "min" : "max", rc);
        return rc;
    }

    /* Success */
    return 0;
}

int bdb_get_row_lock_minmaxlk(bdb_state_type *bdb_state, int rowlock_lid,
                              DBC *dbcp, int ixnum, int stripe, int minmax,
                              DB_LOCK *rlk, DBT *lkname, int how)
{
    return bdb_get_row_lock_minmaxlk_pfunc(bdb_state, rowlock_lid, dbcp, NULL,
                                           NULL, ixnum, stripe, minmax, rlk,
                                           lkname, how);
}
