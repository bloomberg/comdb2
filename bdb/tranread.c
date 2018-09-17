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

#include <ctrace.h>

#include <net.h>
#include "bdb_int.h"
#include "locks.h"
#include <logmsg.h>

/* this is used only for comdbg2 stuff, will not work with rowlocks for now */

static int bdb_fetch_last_key_tran_int(bdb_state_type *bdb_state,
                                       tran_type *tran, int write, int idx,
                                       int keylen, void *fndkey, int *fndlen,
                                       int *bdberr)
{
    DBC *dbc;
    DB_TXN *tid;
    DBT key, data;
    int rc = 0;
    int flags = 0;
    char *buf;

    *bdberr = 0;

    if (tran) {
        tid = tran->tid;
        rc = bdb_lock_table_read(bdb_state, tran);
        if (rc != 0)
        {
            *bdberr = BDBERR_MISC;
            return -1;
        }
    } else {
        tid = NULL;
    }

    if (idx < 0 || idx >= bdb_state->numix) {
        *bdberr = BDBERR_FETCH_IX;
        return -1;
    }

    if (keylen < bdb_state->ixlen[idx]) {
        *bdberr = BDBERR_BUFSMALL;
        return -1;
    }

    if ((rc = bdb_state->dbp_ix[idx]->cursor(bdb_state->dbp_ix[idx], tid, &dbc,
                                             0)) != 0) {
        logmsg(LOGMSG_ERROR, ":%d" ":bdb_fetch_by_key_tran_int:\n"
                                 "cursor failed: rc %d %s\n",
                __LINE__, rc, db_strerror(rc));

        switch (rc) {
        case DB_REP_HANDLE_DEAD:
            *bdberr = BDBERR_DEADLOCK;
            break;

        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;

        default:
            *bdberr = BDBERR_MISC;
            break;
        }
        return -1;
    }

    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    key.flags = DB_DBT_USERMEM;
    key.size = key.ulen = keylen;
    key.data = fndkey;
    data.flags = DB_DBT_MALLOC;

    if (write)
        flags |= DB_RMW;

    rc = dbc->c_get(dbc, &key, &data, DB_LAST | flags);
    if (rc) {
        switch (rc) {
        case DB_NOTFOUND:
            *fndlen = 0;
            rc = 0;
            break;

        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            rc = -1;
            break;

        default:
            *bdberr = BDBERR_FETCH_IX;
            rc = -1;
            break;
        }
    } else {
        *fndlen = key.size;
        rc = 0;
    }
    if (data.data)
        free(data.data);
    dbc->c_close(dbc);

    return rc;
}

int bdb_fetch_last_key_tran(bdb_state_type *bdb_state, tran_type *tran,
                            int write, int idx, int keylen, void *fndkey,
                            int *fndlen, int *bdberr)
{
    int rc;
    BDB_READLOCK("bdb_fetch_last");
    rc = bdb_fetch_last_key_tran_int(bdb_state, tran, write, idx, keylen,
                                     fndkey, fndlen, bdberr);
    BDB_RELLOCK();
    return rc;
}
