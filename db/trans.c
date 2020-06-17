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

#include <alloca.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <time.h>

#include "comdb2.h"
#include "comdb2_trn_intrl.h"
#include "timer.h"
#include <plhash.h>

static int check_trn_purge(void *trnarg, void *purgebuf)
{
    struct purge_trn_type *pbuf = (struct purge_trn_type *)purgebuf;
    time_t curtime = 0;
    longblk_trans_type *trn = (longblk_trans_type *)trnarg;

    if (pbuf->count >= CDB2_TRN_MAXPURGE)
        return 1;

    curtime = time(NULL);

    /* transaction is not blocking on malloc() and is older than 10 seconds! */
    if (!trn->blocking &&
        (curtime - trn->timestamp) > gbl_longblk_trans_purge_interval) {
        pbuf->purgearr[pbuf->count] = trn;
        pbuf->count++;
    }
    return 0;
}

int purge_expired_long_transactions(struct dbenv *dbenv)
{
    /*hash_for*/
    int rc = 0, i = 0;
    struct purge_trn_type purgebuf;
    memset(&purgebuf, 0, sizeof(struct purge_trn_type));
    Pthread_mutex_lock(&dbenv->long_trn_mtx);
    /*fprintf(stderr, "purging transactions\n");*/

    rc = hash_for(dbenv->long_trn_table, check_trn_purge, &purgebuf);
    if (rc == 1 || purgebuf.count > 0) {
        dbenv->long_trn_stats.ltrn_npurges++;
        dbenv->long_trn_stats.ltrn_npurged += purgebuf.count;
        dbenv->long_trn_stats.ltrn_fulltrans -= purgebuf.count;
        if (dbenv->long_trn_stats.ltrn_npurges <= 1)
            dbenv->long_trn_stats.ltrn_avgpurge = purgebuf.count;
        else {
            dbenv->long_trn_stats.ltrn_avgpurge =
                ((dbenv->long_trn_stats.ltrn_avgpurge *
                      (dbenv->long_trn_stats.ltrn_npurges - 1) +
                  purgebuf.count) /
                 dbenv->long_trn_stats.ltrn_npurges);
        }
        for (i = 0; i < purgebuf.count; i++) {
            rc = hash_del(dbenv->long_trn_table, purgebuf.purgearr[i]);
            if (rc != 0) {
                fprintf(stderr, "Error purging long transaction\n");
            }
            free(purgebuf.purgearr[i]->trn_data);
            free(purgebuf.purgearr[i]);
        }
    }
    /*fprintf(stderr, "purged %d transactions\n",purgebuf.count);*/

    Pthread_mutex_unlock(&dbenv->long_trn_mtx);
    return 0;
}

int add_new_transaction_entry(struct dbenv *dbenv, void *entry)
{
    int rc = 0;
    Pthread_mutex_lock(&dbenv->long_trn_mtx);
    rc = hash_add(dbenv->long_trn_table, entry);
    if (rc != 0) {
        rc = ERR_INTERNAL;
    }
    /*listc_abl(&dbenv->long_trn_list, entry);*/
    Pthread_mutex_unlock(&dbenv->long_trn_mtx);
    return rc;
}

void tran_dump(struct long_trn_stat *tstats)
{
    fprintf(stdout, "LONG TRANSACTION STATS\n");
    fprintf(stdout, "----------------------\n");
    fprintf(stdout, "num full trans     %10d\n", tstats->ltrn_fulltrans);
    fprintf(stdout, "num purges         %10d\n", tstats->ltrn_npurges);
    fprintf(stdout, "num tran purged    %10d\n", tstats->ltrn_npurged);
    fprintf(stdout, "avg purges         %10.1lf\n", tstats->ltrn_avgpurge);
    fprintf(stdout, "num max trn segs   %10d\n", tstats->ltrn_maxnseg);
    fprintf(stdout, "num avg trn segs   %10d\n", tstats->ltrn_avgnseg);
    fprintf(stdout, "num min trn segs   %10d\n", tstats->ltrn_minnseg);
    fprintf(stdout, "max trn size       %10d\n", tstats->ltrn_maxsize);
    fprintf(stdout, "avg trn size       %10d\n", tstats->ltrn_avgsize);
    fprintf(stdout, "min trn size       %10d\n", tstats->ltrn_minsize);
    fprintf(stdout, "max trn reqs       %10d\n", tstats->ltrn_maxnreq);
    fprintf(stdout, "avg trn reqs       %10d\n", tstats->ltrn_avgnreq);
    fprintf(stdout, "min trn reqs       %10d\n", tstats->ltrn_minnreq);
}
