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
 * Common error handling routines.
 *
 * DB_REP_HANDLE_DEAD
 *  "replication recovery unrolled committed transactions"
 *  We don't expect to get this on a write since we expect to be the master
 *  and therefore not in a replication recovery mess.  On the other hand -
 *  we've seen it happen to the master node on beta.  All these functions
 *  take a tid so that we can correctly determine if we are the master.  For
 *  now, on REP_HANDLE_DEAD we will reopen our handles whether we are master
 *  or not.
 *
 * DB_LOCK_DEADLOCK
 *  Set bdberr to deadlock.  Higher levels of the db will retry the request.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include "bdb_int.h"
#include "locks.h"

#include <logmsg.h>

/* Close a Berkeley cursor, set the pointer to NULL and
 * handle deadlock correctly. */
int bdb_dbcp_close(DBC **dbcp_ptr, int *bdberr, const char *context_str)
{
    int rc;
    DBC *dbcp = *dbcp_ptr;
    *dbcp_ptr = NULL;
    rc = dbcp->c_close(dbcp);
    if (rc != 0) {
        switch (rc) {
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;

        default:
            logmsg(LOGMSG_ERROR, "bdb_dbcp_close(%s): "
                            "unexpected rcode %d %s\n",
                    context_str, rc, bdb_strerror(rc));
            *bdberr = BDBERR_MISC;
            break;
        }
        return -1;
    }
    return 0;
}

static void rep_handle_dead(bdb_state_type *bdb_state, DB_TXN *tid,
                            const char *context_str)
{
    static int last;
    int now;

    now = time(NULL);
    if (now != last) {
        /* want to know when we do this */
        logmsg(LOGMSG_ERROR, "rep_handle_dead: "
                        "%s DB_REP_HANDLE_DEAD from %s!\n",
                tid ? "transactional" : "untransactional", context_str);
        last = now;
    }

    if (bdb_state->read_write) {
        /* We might be in a transaction.  Find out and flag the transaction
         * as having failed due to a rep handle dead. */
        if (bdb_tran_rep_handle_dead(bdb_state))
            return;
    }
}

void bdb_cursor_error(bdb_state_type *bdb_state, DB_TXN *tid, int rc,
                      int *bdberr, const char *context_str)
{
    switch (rc) {
    case DB_REP_HANDLE_DEAD:
        rep_handle_dead(bdb_state, tid, context_str);
        *bdberr = BDBERR_DEADLOCK;
        break;

    case DB_LOCK_DEADLOCK:
        /* This DOES happen on cursor open even though docs claim otherwise */
        *bdberr = BDBERR_DEADLOCK;
        break;

    default:
        logmsg(LOGMSG_ERROR, "%s (%s): "
                        "unexpected cursor open rcode %d %s\n",
                __func__, context_str, rc, bdb_strerror(rc));
        *bdberr = BDBERR_MISC;
        break;
    }
}

void bdb_get_error(bdb_state_type *bdb_state, DB_TXN *tid, int rc,
                   int not_found_rc, int *bdberr, const char *context_str)
{
    switch (rc) {
    case DB_REP_HANDLE_DEAD:
        rep_handle_dead(bdb_state, tid, context_str);
        *bdberr = BDBERR_DEADLOCK;
        break;

    case DB_LOCK_DEADLOCK:
        *bdberr = BDBERR_DEADLOCK;
        break;

    case DB_NOTFOUND:
        *bdberr = not_found_rc;
        break;

    default:
        logmsg(LOGMSG_ERROR, "bdb_get_error(%s): "
                        "unexpected get rcode %d %s\n",
                context_str, rc, bdb_strerror(rc));
        *bdberr = BDBERR_MISC;
        break;
    }
}

/* We can pass in the cursor that failed here.  This is to encourage us to
 * close it before attempting any downgrades.  If the caller has multiple
 * cursors open, then he should close them all before calling this. */
void bdb_c_get_error(bdb_state_type *bdb_state, DB_TXN *tid, DBC **dbcp, int rc,
                     int not_found_rc, int *bdberr, const char *context_str)
{
    int closerc = 0;
    if (*dbcp != NULL) {
        closerc = (*dbcp)->c_close(*dbcp);
        *dbcp = NULL;
    }
    switch (rc) {
    case DB_REP_HANDLE_DEAD:
        rep_handle_dead(bdb_state, tid, context_str);
        *bdberr = BDBERR_DEADLOCK;
        break;

    case DB_LOCK_DEADLOCK:
        *bdberr = BDBERR_DEADLOCK;
        break;

    case DB_NOTFOUND:
        /* Make sure that a deadlock on cursor close gets bubbled back.
         * This will be important for transactional read code.  (Probably
         * irrelevant for non transaction reads). */
        switch (closerc) {
        case DB_REP_HANDLE_DEAD:
            rep_handle_dead(bdb_state, tid, context_str);
        /* fallthrough */
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;
        default:
            *bdberr = not_found_rc;
            break;
        }
        break;

    default:
        logmsg(LOGMSG_ERROR, "bdb_c_get_error(%s): "
                        "unexpected cursor get rcode %d %s\n",
                context_str, rc, bdb_strerror(rc));
        *bdberr = BDBERR_MISC;
        break;
    }
}
