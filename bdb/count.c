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

/* returns -1 on error, 0 on empy table, >0 for number of recs found */
int bdb_count_int(bdb_state_type *bdb_state, int *bdberr)
{
    int i;
    int rc;
    DBT dbt_key, dbt_data;
    DBC *dbcp;
    int lowrecnum;
    int highrecnum;
    int numrecs;
    int smallest_index = 0;

    size_t buffer_length;
    void *buffer;
    int count = 0;
    unsigned char keymax[BDB_KEY_MAX + sizeof(unsigned long long)];

    /* see if we can find an index with recnums.  if we can, we can
       get the number of rrns in constant time by getting the recnum of
       the first and the recnum of the last and subtracting the high
       from the low and adding 1. */
    if (bdb_state->have_recnums) {
        for (i = 0; i < bdb_state->numix; i++) {
            if (bdb_state->ixrecnum[i]) {
                int recnum;

                rc = bdb_state->dbp_ix[i]->cursor(bdb_state->dbp_ix[i], 0,
                                                  &dbcp, 0);
                if (0 != rc) {
                    switch (rc) {
                    case DB_REP_HANDLE_DEAD:
                        *bdberr = BDBERR_DEADLOCK;
                        break;

                    case DB_LOCK_DEADLOCK:
                        /* This DOES happen even though docs claim otherwise */
                        *bdberr = BDBERR_DEADLOCK;
                        break;

                    default:
                        fprintf(stderr, "bdb_count_int: cursor fail %d %s\n",
                                rc, db_strerror(rc));
                        *bdberr = BDBERR_MISC;
                        break;
                    }
                    return -1;
                }

                memset(&dbt_key, 0, sizeof(dbt_key));
                memset(&dbt_data, 0, sizeof(dbt_data));
                dbt_key.flags |= DB_DBT_MALLOC;
                dbt_data.flags |= DB_DBT_MALLOC;
                rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_FIRST);
                if (rc == 0) {
                    if (dbt_key.data)
                        free(dbt_key.data);
                    if (dbt_data.data)
                        free(dbt_data.data);
                } else if (rc == DB_NOTFOUND) {
                    dbcp->c_close(dbcp);
                    return 0;
                } else {
                    dbcp->c_close(dbcp);
                    if (rc == DB_LOCK_DEADLOCK || rc == DB_REP_HANDLE_DEAD)
                        *bdberr = BDBERR_DEADLOCK;
                    else {
                        fprintf(stderr,
                                "bdb_count_int: c_get first failed %d %s\n", rc,
                                db_strerror(rc));
                    }
                    return -1;
                }

                memset(&dbt_data, 0, sizeof(dbt_data));
                dbt_data.data = &recnum;
                dbt_data.ulen = sizeof(int);
                dbt_data.flags |= DB_DBT_USERMEM;
                rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_GET_RECNO);
                lowrecnum = recnum;
                if (rc != 0) {
                    dbcp->c_close(dbcp);
                    switch (rc) {
                    case DB_LOCK_DEADLOCK:
                    case DB_REP_HANDLE_DEAD:
                        *bdberr = BDBERR_DEADLOCK;
                        break;
                    default:
                        fprintf(stderr,
                                "bdb_count_int: get first recno %d %s\n", rc,
                                db_strerror(rc));
                        break;
                    }
                    return -1;
                }

                memset(&dbt_key, 0, sizeof(dbt_key));
                memset(&dbt_data, 0, sizeof(dbt_data));
                dbt_key.flags |= DB_DBT_MALLOC;
                dbt_data.flags |= DB_DBT_MALLOC;
                rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_LAST);
                if (rc == 0) {
                    if (dbt_key.data)
                        free(dbt_key.data);
                    if (dbt_data.data)
                        free(dbt_data.data);
                } else if (rc == DB_NOTFOUND) {
                    fprintf(stderr, "bdb_count_int: strangely occurence!\n");
                    dbcp->c_close(dbcp);
                    return 0;
                } else {
                    dbcp->c_close(dbcp);
                    if (rc == DB_LOCK_DEADLOCK || rc == DB_REP_HANDLE_DEAD)
                        *bdberr = BDBERR_DEADLOCK;
                    else {
                        fprintf(stderr,
                                "bdb_count_int: c_get first failed %d %s\n", rc,
                                db_strerror(rc));
                    }
                    return -1;
                }

                memset(&dbt_data, 0, sizeof(dbt_data));
                dbt_data.data = &recnum;
                dbt_data.ulen = sizeof(int);
                dbt_data.flags |= DB_DBT_USERMEM;
                rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_GET_RECNO);
                highrecnum = recnum;
                if (rc != 0) {
                    dbcp->c_close(dbcp);
                    switch (rc) {
                    case DB_LOCK_DEADLOCK:
                    case DB_REP_HANDLE_DEAD:
                        *bdberr = BDBERR_DEADLOCK;
                        break;
                    default:
                        fprintf(stderr, "bdb_count_int: get last recno %d %s\n",
                                rc, db_strerror(rc));
                        break;
                    }
                    return -1;
                }

                dbcp->c_close(dbcp);

                numrecs = highrecnum - lowrecnum + 1;

                return numrecs;
            }

            if (bdb_state->ixlen[i] < bdb_state->ixlen[smallest_index])
                smallest_index = i;
        }
    }

    /* Bulk extract and walk the smallest index - probably not good
     * for cache. */

    buffer_length = 1024 * 1024;
    buffer = mymalloc(buffer_length);
    if (!buffer) {
        fprintf(stderr, "bdb_count: mymalloc %u failed "
                        "(dtastripe index walk)\n",
                (unsigned)buffer_length);
        return -1;
    }

    rc = bdb_state->dbp_ix[0]->cursor(bdb_state->dbp_ix[0], 0, &dbcp, 0);
    if (rc != 0) {
        myfree(buffer);
        fprintf(stderr, "bdb_count: ix0->cursor open failed %d %s\n", rc,
                db_strerror(rc));
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
            *bdberr = BDBERR_DEADLOCK;
            break;

        case DB_LOCK_DEADLOCK:
            /* This DOES happen even though docs claim otherwise */
            *bdberr = BDBERR_DEADLOCK;
            break;

        default:
            *bdberr = BDBERR_MISC;
            break;
        }
        return -1;
    }

    bzero(&dbt_key, sizeof(dbt_key));
    bzero(&dbt_data, sizeof(dbt_data));

    dbt_key.data = keymax;
    dbt_key.ulen = sizeof(keymax);
    dbt_key.flags = DB_DBT_USERMEM;

    dbt_data.data = buffer;
    dbt_data.ulen = buffer_length;
    dbt_data.flags = DB_DBT_USERMEM;

    while (1) {
        void *ptr;

        rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_MULTIPLE_KEY | DB_NEXT);
        if (rc == DB_NOTFOUND) {
            break;
        } else if (rc != 0) {
            switch (rc) {
            case DB_REP_HANDLE_DEAD:
            case DB_LOCK_DEADLOCK:
                *bdberr = BDBERR_DEADLOCK;
                break;

            default:
                fprintf(stderr, "bdb_count_int: index walk c_get %d %s\n", rc,
                        db_strerror(rc));
                *bdberr = BDBERR_MISC;
                break;
            }
            count = -1;
            break;
        }

        for (DB_MULTIPLE_INIT(ptr, &dbt_data);;) {
            void *dataptr;
            size_t datalen;
            void *keyptr;
            size_t keylen;
            (void) datalen;
            (void) keyptr;
            (void) keylen;
            DB_MULTIPLE_KEY_NEXT(ptr, &dbt_data, keyptr, keylen, dataptr,
                                 datalen);
            if (!dataptr)
                break;
            count++;
        }

        /* stop temporarely if we do require the read lock */
        if (bdb_lock_desired(bdb_state)) {
            fprintf(stderr,
                    "count thread: aborting due to write lock desired\n");
            count = -1;
            *bdberr = BDBERR_MISC;
            break;
        }
    }

    dbcp->c_close(dbcp);
    myfree(buffer);

    return count;
}

int bdb_count(bdb_state_type *bdb_state, int *bdberr)
{
    int ret;
    BDB_READLOCK("bdb_count");

    ret = bdb_count_int(bdb_state, bdberr);
    BDB_RELLOCK();
    return ret;
}
