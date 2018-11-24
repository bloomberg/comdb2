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
#include "genid.h"
#include "bdb_api.h"

static int bdb_fetch_next_genids_int(bdb_state_type *bdb_state, int ixnum,
                                     int ixlen, unsigned char *ix,
                                     unsigned long long *genids, int numgenids,
                                     int *num_genids_gotten, int *bdberr)
{
    DBT dbt_key, dbt_data;
    int rc;
    DBC *dbcp = NULL;
    unsigned long long found_genid;
    int outrc;
    int cursor_flags;
    int num;
    int i;
    unsigned char buf[512];

    outrc = 0;
    cursor_flags = 0;

    cursor_flags |= DB_DIRTY_READ;

    *num_genids_gotten = 0;
    num = 0;

    memset(&dbt_key, 0, sizeof(dbt_key));
    memset(&dbt_data, 0, sizeof(dbt_data));

    memcpy(buf, ix, ixlen);

    dbt_key.data = buf;
    dbt_key.size = ixlen;
    dbt_key.ulen = 512;

    dbt_data.data = &found_genid;
    dbt_data.size = sizeof(unsigned long long);
    dbt_data.ulen = sizeof(unsigned long long);

    dbt_key.flags |= DB_DBT_USERMEM;
    dbt_data.flags |= DB_DBT_USERMEM;

    rc = bdb_state->dbp_ix[ixnum]->cursor(bdb_state->dbp_ix[ixnum], 0, &dbcp,
                                          cursor_flags);

    if (rc != 0) {
        return -2;
    }

    rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_SET_RANGE);
    if (rc != 0) {
        dbcp->c_close(dbcp);
        return -1;
    }

    memcpy(genids + 0, dbt_data.data, sizeof(unsigned long long));
    num++;

    for (i = 1; i < numgenids; i++) {
        rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_NEXT);
        if (rc != 0) {
            dbcp->c_close(dbcp);
            outrc = 0;
            return 0;
        }

        memcpy(genids + num, dbt_data.data, sizeof(unsigned long long));
        num++;
    }

    dbcp->c_close(dbcp);
    outrc = 0;

    if (outrc == 0) {
        *num_genids_gotten = num;
    }

    return outrc;
}

int bdb_fetch_next_genids(bdb_state_type *bdb_state, int ixnum, int ixlen,
                          unsigned char *ix, unsigned long long *genids,
                          int numgenids, int *num_genids_gotten, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_fetch_next_genids");

    rc = bdb_fetch_next_genids_int(bdb_state, ixnum, ixlen, ix, genids,
                                   numgenids, num_genids_gotten, bdberr);

    BDB_RELLOCK();

    return rc;
}

/* finds the oldest or newest genid, if direction is 0 it finds the oldest, if
 * it is anything else it finds the newest. */
static int bdb_find_edge_genid_int(bdb_state_type *bdb_state, tran_type *tran,
                                   int direction, int stripe, void *rec,
                                   int *reclen, int maxlen,
                                   unsigned long long *genid, uint8_t *ver,
                                   int *bdberr)
{
    DBT dbt_key = {0}, dbt_data = {0};
    DBC *cur;
    int rc, ixrc;

    rc = bdb_state->dbp_data[0][stripe]->cursor(
        bdb_state->dbp_data[0][stripe], tran ? tran->tid : NULL, &cur, 0);
    if (rc) {
        if (rc == DB_LOCK_DEADLOCK) {
            *bdberr = BDBERR_DEADLOCK;
            return -1;
        } else {
            *bdberr = rc;
            return -1;
        }
    }
    dbt_key.data = genid;
    dbt_key.ulen = sizeof(unsigned long long);
    dbt_key.flags = DB_DBT_USERMEM;
    dbt_data.data = rec;
    dbt_data.ulen = maxlen;
    dbt_data.flags = DB_DBT_USERMEM;

    ixrc = bdb_cget_unpack(bdb_state, cur, &dbt_key, &dbt_data, ver,
                           (direction) ? DB_LAST : DB_FIRST);
    if (ixrc == 0)
        *reclen = dbt_data.size;

    rc = cur->c_close(cur);
    if (rc) {
        if (rc == DB_LOCK_DEADLOCK) {
            *bdberr = BDBERR_DEADLOCK;
            return -1;
        } else {
            *bdberr = rc;
            return -1;
        }
    }

    if (ixrc == 0)
        return 0;
    else if (ixrc == DB_LOCK_DEADLOCK) {
        *bdberr = BDBERR_DEADLOCK;
        return -1;
    } else if (ixrc == DB_NOTFOUND)
        return 1;
    else {
        *bdberr = ixrc;
        return -1;
    }
}

int bdb_find_oldest_genid(bdb_state_type *bdb_state, tran_type *tran,
                          int stripe, void *rec, int *reclen, int maxlen,
                          unsigned long long *genid, uint8_t *ver, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_find_oldest_genid");
    *bdberr = BDBERR_NOERROR;
    rc = bdb_find_edge_genid_int(bdb_state, tran, 0 /*direction*/, stripe, rec,
                                 reclen, maxlen, genid, ver, bdberr);
    BDB_RELLOCK();
    return rc;
}

int bdb_find_newest_genid(bdb_state_type *bdb_state, tran_type *tran,
                          int stripe, void *rec, int *reclen, int maxlen,
                          unsigned long long *genid, uint8_t *ver, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_find_newest_genid");
    *bdberr = BDBERR_NOERROR;
    rc = bdb_find_edge_genid_int(bdb_state, tran, 1 /*direction*/, stripe, rec,
                                 reclen, maxlen, genid, ver, bdberr);
    BDB_RELLOCK();
    return rc;
}
