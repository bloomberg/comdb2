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

#ifdef BERKDB_46

#define DEADLOCK_RETRIES 100

#define DEFAULT_TIMEOUT 1000

static int compact_this_db(bdb_state_type *bdb_state, DB *db, DBT *end,
                           int *bdberr, int timeout, int freefs);
static int bdb_compact_table_int(bdb_state_type *bdb_state, int *bdberr,
                                 int timeout, int freefs);
/*static void*    bdb_compact_waitfor_all_nodes(void *state);*/

/* return 0 on success */
int bdb_compact_table(bdb_state_type *bdb_state, int *bdberr, int timeout,
                      int freefs)
{
    int ret;
    BDB_READLOCK("bdb_compact_table");
    ret = bdb_compact_table_int(bdb_state, bdberr, timeout, freefs);
    BDB_RELLOCK();
    return ret;
}

/* returns -1 on error, 0 on success*/
static int bdb_compact_table_int(bdb_state_type *bdb_state, int *bdberr,
                                 int timeout, int freefs)
{

    DBT end;
    int rc = 0;
    int i = 0, j = 0;

    /* check if we're the master*/
    if (bdb_state->repinfo->master_host !=
        bdb_state->repinfo->netinfo->mynode) {
        logmsg(LOGMSG_ERROR, "bdb_compact_table_int: btree compaction is only "
                        "allowed on MASTER node.\n");
        return -1;
    }

    /* we're provide this only for stripped data */
    if (!bdb_state->attr->dtastripe /*|| !bdb_state->attr->blobstripe*/) {
        logmsg(LOGMSG_ERROR, "bdb_compact_table_int: btree compaction is only "
                        "allowed for stripped data.\n");
        return -1;
    }

    if (timeout <= 0)
        timeout = DEFAULT_TIMEOUT;

    /* compact the data and the blobs*/
    for (j = 0; j < bdb_state->numdtafiles; j++) {

        int limit =
            (j == 0) ? bdb_state->attr->dtastripe : bdb_state->attr->blobstripe;

        for (i = 0; i < limit; i++) {

            bzero(&end, sizeof(end));
            if (compact_this_db(bdb_state, bdb_state->dbp_data[j][i], &end,
                                bdberr, timeout, freefs))
                return -1;
        }
    }

    /* compact the indexes*/
    for (i = 0; i < bdb_state->numix; i++) {

        bzero(&end, sizeof(end));
        if (compact_this_db(bdb_state, bdb_state->dbp_ix[i], &end, bdberr,
                            timeout, freefs))
            return -1;
    }

    return 0;
}

static int compact_this_db(bdb_state_type *bdb_state, DB *db, DBT *end,
                           int *bdberr, int timeout, int freefs)
{

    DB_COMPACT c_data;
    int rc = DB_LOCK_DEADLOCK;
    int retries = 0;
    /*    tran_type   *tran = NULL;               */ /* dummy tran to get a lsn
                                                        number */

    print(bdb_state, "compacting %s\n", db->fname);
    logmsg(LOGMSG_USER, "compacting %s\n", db->fname);
    /*
        tran = mymalloc(sizeof(tran_type));
        bzero(tran, sizeof(tran_type));
        tran->master = 1;

        rc = pthread_setspecific(bdb_state->seqnum_info->key, tran);
        if( rc != 0)
            fprintf( stderr, "compact_this_db: pthread_specific failed\n");

        rc = DB_LOCK_DEADLOCK;
    */
    while (rc == DB_LOCK_DEADLOCK && retries < DEADLOCK_RETRIES) {

        /* initialize c_data */
        bzero(&c_data, sizeof(c_data));
        c_data.compact_timeout = timeout;
        /*
                c_data.sync = bdb_compact_waitfor_all_nodes;
                c_data.state = bdb_state;
         */

        rc = db->compact(db, NULL, NULL, NULL, &c_data,
                         (freefs) ? DB_FREE_SPACE : 0, end);
        if (rc) {
            switch (rc) {

            case DB_REP_HANDLE_DEAD:

                logmsg(LOGMSG_ERROR, "bdb_compact_table_int: handle dead due to "
                                "replication rollout while processing %s\n",
                        db->fname);
                bdb_reopen(bdb_state);

                *bdberr = BDBERR_DEADLOCK;
                break;

            case DB_LOCK_DEADLOCK:

                retries++;
                if (retries < DEADLOCK_RETRIES) {

                    logmsg(LOGMSG_ERROR, "bdb_compact_table_int: retrying, %d "
                                    "deadlocks so far, while processing %s\n",
                            c_data.compact_deadlock, db->fname);
                    continue;
                }

                logmsg(LOGMSG_ERROR, "bdb_compact_table_int: too many deadlocks "
                                "while processing %s, please run the command "
                                "again\n",
                        db->fname);
                *bdberr = BDBERR_DEADLOCK;
                break;

            case DB_REP_LOCKOUT:

                /* this is a new election; fail here, as we probably lost
                 * mastership*/
                logmsg(LOGMSG_ERROR, "bdb_compact_table_int: election in progress "
                                "while processing %s, please run this on "
                                "master node\n",
                        db->fname);
                *bdberr = BDBERR_DEADLOCK;
                break;

            case DB_LOCK_NOTGRANTED:

                logmsg(LOGMSG_ERROR, "bdb_compact_table_int: transaction timeout "
                                "while processing %s, please try again\n",
                        db->fname);
                *bdberr = BDBERR_TIMEOUT;
                break;

            case EACCES:

                logmsg(LOGMSG_ERROR, "bdb_compact_table_int: error trying to "
                                "compact a readonly file %s",
                        db->fname);
                *bdberr = BDBERR_READONLY;
                break;

            default:

                logmsg(LOGMSG_ERROR, "bdb_compact_table_int: unknown error code %d "
                                "when processing %s\n",
                        rc, db->fname);
                *bdberr = BDBERR_MISC;
                break;
            }

            return -1;
        }
    }
    /*
        pthread_setspecific(bdb_state->seqnum_info->key, NULL);

        myfree(tran);
    */
    print(bdb_state, "compaction %s successful\n", db->fname);
    print(
        bdb_state,
        "    %u ddlcks %u seen pgs %u freed pgs %u rmvd lvls %u fs freed pgs\n",
        c_data.compact_deadlock, c_data.compact_pages_examine,
        c_data.compact_pages_free, c_data.compact_levels,
        c_data.compact_pages_truncated);
    logmsg(LOGMSG_USER, "compaction %s successful\n", db->fname);
    logmsg(LOGMSG_USER, "    %u ddlcks %u seen pgs %u freed pgs %u rmvd lvls %u fs freed pgs\n",
        c_data.compact_deadlock, c_data.compact_pages_examine,
        c_data.compact_pages_free, c_data.compact_levels,
        c_data.compact_pages_truncated);

    return 0;
}
/*
static void*    bdb_compact_waitfor_all_nodes(void * state) {

    tran_type   *tran = NULL;

    if(state) {

        tran = pthread_getspecific( ((bdb_state_type*)state)->seqnum_info->key);
        if( !tran) {
            fprintf(stderr, "bdb_compact_waitfor_all_nodes: error returning
tran\n");
            return NULL;
        }

        fprintf(stderr, "bdb_compact_waitfor_all_nodes: waiting for %d:%d\n",
                ((DB_LSN*)(&tran->savelsn))->file,
((DB_LSN*)(&tran->savelsn))->offset);
        print((bdb_state_type*)state, "bdb_compact_waitfor_all_nodes: waiting
for %d:%d\n",
                ((DB_LSN*)(&tran->savelsn))->file,
((DB_LSN*)(&tran->savelsn))->offset);
        bdb_wait_for_seqnum_from_all( (bdb_state_type*)state,
(seqnum_type*)(&tran->savelsn));
    }
    return NULL;
}
*/
#endif
