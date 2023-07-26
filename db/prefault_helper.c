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

/* prefault_helper - aka buffer_buddy */

/*
  buffer buddy - you're the one...
  you make swapin so much fun..
  buffer buddy you're my very best friend it's true...
*/

#include <alloca.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <memory_sync.h>

#include "comdb2.h"
#include "tag.h"
#include "types.h"
#include "translistener.h"
#include "block_internal.h"
#include "prefault.h"
#include "logmsg.h"

int gbl_prefault_toblock_bcast = 1;
int gbl_prefault_toblock_local = 1;

extern pthread_t gbl_invalid_tid;
pthread_t gbl_invalid_tid2;

int prefault_check_enabled(void) { return gbl_prefaulthelperthreads; }

void clear_pfk(struct dbenv *dbenv, int i, int numops)
{

    /*fprintf(stderr, "clearing pfk for helper %d\n", i);*/
    bzero(dbenv->prefault_helper.threads[i].pfk_bitmap, 8 * gbl_maxblockops);
}

static void *prefault_helper_thread(void *arg)
{
    comdb2_name_thread(__func__);
    prefault_helper_thread_arg_type prefault_helper_thread_arg;
    struct dbenv *dbenv;
    int rc;
    int i;
    struct ireq *iq = NULL;
    block_state_t *blkstate = NULL;
    struct thread_info *thdinfo;
    unsigned char key[512];
    int keylen = 0;
    int ixnum = 0;
    struct dbtable *db = NULL;
    int numreadahead = 0;
    struct thr_handle *thr_self;
    pthread_t working_for;

    thr_self = thrman_register(THRTYPE_PREFAULT);
    thread_started("prefault helper");

    memcpy(&prefault_helper_thread_arg, arg,
           sizeof(prefault_helper_thread_arg_type));

    free(arg);

    dbenv = prefault_helper_thread_arg.dbenv;
    i = prefault_helper_thread_arg.instance;

    logmsg(LOGMSG_INFO, "prefault_helper_thread instance %d started as tid %p\n", i, (void *)pthread_self());

    backend_thread_event(dbenv, COMDB2_THR_EVENT_START_RDWR);

    /* thdinfo is assigned to thread specific variable thd_info_key which
     * will automatically free it when the thread exits. */
    thdinfo = malloc(sizeof(struct thread_info));
    if (thdinfo == NULL) {
        logmsg(LOGMSG_FATAL, "**aborting due malloc failure thd %p\n", (void *)pthread_self());
        abort();
    }
    thdinfo->uniquetag = 0;
    thdinfo->ct_id_key = 0LL;
    thdinfo->ct_add_table = NULL;
    thdinfo->ct_del_table = NULL;
    thdinfo->ct_add_table = NULL;
    thdinfo->ct_del_table = NULL;
    Pthread_setspecific(thd_info_key, thdinfo);

    while (1) {
        Pthread_mutex_lock(&(dbenv->prefault_helper.mutex));

        /*fprintf(stderr, "setting working_for(%d) to invalid\n", i);*/

        /*fprintf(stderr, "setting working_for to invalid for helper %d\n",
         * i);*/
        /* clear who we're working for - we're idle and available now */
        dbenv->prefault_helper.threads[i].working_for = gbl_invalid_tid;
        MEMORY_SYNC;

        Pthread_cond_wait(&(dbenv->prefault_helper.threads[i].cond),
                          &(dbenv->prefault_helper.mutex));

        /* bogus wakeup? */
        if (dbenv->prefault_helper.threads[i].working_for == gbl_invalid_tid) {
            Pthread_mutex_unlock(&(dbenv->prefault_helper.mutex));
            continue;
        }

        working_for = dbenv->prefault_helper.threads[i].working_for;

        /* pick off the info */

        /*fprintf(stderr, "prefault_helper_thread woke up\n");*/

        switch (dbenv->prefault_helper.threads[i].type) {
        case PREFAULT_TOBLOCK:
            /*fprintf(stderr, "prefault_helper: got PREFAULT_TOBLOCK\n");*/
            iq = dbenv->prefault_helper.threads[i].iq;
            blkstate = dbenv->prefault_helper.threads[i].blkstate;

            /* record the current pfk seqnum in the block state */
            blkstate->pfkseq = dbenv->prefault_helper.threads[i].seqnum;

            MEMORY_SYNC;

            break;

        case PREFAULT_READAHEAD:
            /*fprintf(stderr, "prefault_helper: got PREFAULT_READAHEAD\n");*/
            db = dbenv->prefault_helper.threads[i].db;
            ixnum = dbenv->prefault_helper.threads[i].ixnum;
            keylen = dbenv->prefault_helper.threads[i].keylen;
            memcpy(key, dbenv->prefault_helper.threads[i].key,
                   dbenv->prefault_helper.threads[i].keylen);
            numreadahead = dbenv->prefault_helper.threads[i].numreadahead;

            break;
        }

        Pthread_mutex_unlock(&(dbenv->prefault_helper.mutex));

        /*poll(NULL, 0, 10);*/

        /*fprintf(stderr, "prefault_helper %d running\n", i);*/

        switch (dbenv->prefault_helper.threads[i].type) {
        case PREFAULT_TOBLOCK:
            thrman_where(thr_self, "prefault_toblock");

            /*fprintf(stderr, "running prefault_toblock, helper id %d\n", i);*/

            /*fprintf(stderr, "clearing pfk for helper %d\n", i);
            bzero(dbenv->prefault_helper.threads[i].pfk_bitmap,
               8 * gbl_maxblockops);
            MEMORY_SYNC;
            */

            if (dbenv->prefault_helper.threads[i].working_for == gbl_invalid_tid) {
                fprintf(stderr, "PREFAULT ERROR!  working_for was %p\n", (void *)working_for);
                break;
            }

            rc = prefault_toblock(iq, blkstate, i,
                                  dbenv->prefault_helper.threads[i].seqnum,
                                  &(dbenv->prefault_helper.threads[i].abort));
            if (rc)
                logmsg(LOGMSG_ERROR, "%s:%d rc=%d\n", __func__, __LINE__, rc);

            /*fprintf(stderr, "helper %d working for invalid2\n", i);*/
            thrman_where(thr_self, NULL);
            break;

        case PREFAULT_READAHEAD:
            thrman_where(thr_self, "prefault_readahead");
            rc = prefault_readahead(db, ixnum, key, keylen, numreadahead);
            if (rc)
                logmsg(LOGMSG_ERROR, "%s:%d rc=%d\n", __func__, __LINE__, rc);
            thrman_where(thr_self, NULL);
            break;
        }
    }
#if 0
   /* we never actually get here */
   backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
   return NULL;
#endif
}

#if 0
static void *prefault_helper_thread(void *arg)
{
   prefault_helper_thread_arg_type prefault_helper_thread_arg;
   struct dbenv *dbenv;
   int rc;
   int i;
   struct ireq *iq;
   block_state_t *blkstate;
   struct thread_info *thdinfo;
   unsigned char key[512];
   int keylen;
   int ixnum;
   struct dbtable *db;
   int numreadahead;
   
   memcpy(&prefault_helper_thread_arg, arg,
      sizeof(prefault_helper_thread_arg_type));
   
   free(arg);

   dbenv = prefault_helper_thread_arg.dbenv;
   i = prefault_helper_thread_arg.instance;
   
   fprintf(stderr, "prefault_helper_thread instance %d started as tid %p\n",
      i, (void *)pthread_self());

   backend_thread_event(dbenv, COMDB2_THR_EVENT_START_RDWR);


   while(1)
   {
     again:

      Pthread_mutex_lock(&(dbenv->prefault_helper.threads[i].mutex));
      
      /* clear who we're working for - we're idle and available now */
      /*fprintf(stderr, "helper %d working for invalid\n", i);*/
      
      dbenv->prefault_helper.threads[i].working_for = gbl_invalid_tid;
      MEMORY_SYNC;
      
      Pthread_cond_wait(&(dbenv->prefault_helper.threads[i].cond),
         &(dbenv->prefault_helper.threads[i].mutex));

      MEMORY_SYNC;

      /* bogus wakeup? */
      if (dbenv->prefault_helper.threads[i].working_for == gbl_invalid_tid)
      {
         Pthread_mutex_unlock(&(dbenv->prefault_helper.threads[i].mutex));
         
         goto again;
      }
      /*fprintf(stderr, "prefault_helper_thread %d woke up\n", i);*/

      /*sleep(1);*/
      
      Pthread_mutex_unlock(&(dbenv->prefault_helper.threads[i].mutex));
   }
   
   backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
}
#endif

int create_prefault_helper_threads(struct dbenv *dbenv, int nthreads)
{
    int i;
    static int started = 0;
    prefault_helper_thread_arg_type *prefault_helper_thread_arg;
    pthread_attr_t attr;

    if (started != 0)
        return 0;

    dbenv->prefault_helper.numthreads = 0;

    Pthread_attr_init(&attr);

    Pthread_attr_setstacksize(&attr, 512 * 1024);

    Pthread_mutex_init(&(dbenv->prefault_helper.mutex), NULL);

    for (i = 0; i < nthreads; i++) {
        prefault_helper_thread_arg =
            malloc(sizeof(prefault_helper_thread_arg_type));
        prefault_helper_thread_arg->dbenv = dbenv;
        prefault_helper_thread_arg->instance = i;

        Pthread_mutex_init(&(dbenv->prefault_helper.threads[i].mutex), NULL);

        Pthread_cond_init(&(dbenv->prefault_helper.threads[i].cond), NULL);

        dbenv->prefault_helper.threads[i].pfk_bitmap =
            malloc(8 * gbl_maxblockops);

        bzero(dbenv->prefault_helper.threads[i].pfk_bitmap,
              8 * gbl_maxblockops);

        dbenv->prefault_helper.threads[i].seqnum = 1;

        MEMORY_SYNC;

        Pthread_create(&(dbenv->prefault_helper.threads[i].tid), &attr,
                       prefault_helper_thread, prefault_helper_thread_arg);

        /* latch the first helper threads tid as our second invalid tid */
        if (i == 0)
            gbl_invalid_tid2 = dbenv->prefault_helper.threads[i].tid;

        dbenv->prefault_helper.numthreads++;
    }
    started = 1;

    Pthread_attr_destroy(&attr);

    return 0;
}

int readaheadpf(struct ireq *iq, struct dbtable *db, int ixnum, unsigned char *key,
                int keylen, int num)
{
    pthread_t my_tid;
    int i;

    if (!prefault_check_enabled())
        return 0;

    my_tid = pthread_self();

    Pthread_mutex_lock(&(iq->dbenv->prefault_helper.mutex));

    for (i = 0; i < iq->dbenv->prefault_helper.numthreads; i++) {
        if (iq->dbenv->prefault_helper.threads[i].working_for == gbl_invalid_tid) {
            /* found an idle thread to give this to! */
            /*fprintf(stderr, "dispatching to processor %d\n", i);*/
            Pthread_mutex_lock(&(iq->dbenv->prefault_helper.threads[i].mutex));

            /* he's working for me! */
            iq->dbenv->prefault_helper.threads[i].working_for = my_tid;

            iq->dbenv->prefault_helper.threads[i].type = PREFAULT_READAHEAD;
            iq->dbenv->prefault_helper.threads[i].iq = NULL;
            iq->dbenv->prefault_helper.threads[i].db = db;
            iq->dbenv->prefault_helper.threads[i].ixnum = ixnum;
            iq->dbenv->prefault_helper.threads[i].keylen = keylen;
            memcpy(iq->dbenv->prefault_helper.threads[i].key, key,
                   iq->dbenv->prefault_helper.threads[i].keylen);
            iq->dbenv->prefault_helper.threads[i].numreadahead = num;

            /*fprintf(stderr, "readahead signaling helper %d\n", i);*/

            Pthread_cond_signal(&(iq->dbenv->prefault_helper.threads[i].cond));
            Pthread_mutex_unlock(
                &(iq->dbenv->prefault_helper.threads[i].mutex));

            /* Found someone to work for me, break out of loop */
            break;
        }
    }

    Pthread_mutex_unlock(&(iq->dbenv->prefault_helper.mutex));

    return 0;
}
