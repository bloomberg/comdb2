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

#include <plbitlib.h>
#include <memory_sync.h>

#include "comdb2.h"
#include "tag.h"
#include "types.h"
#include "translistener.h"
#include "block_internal.h"
#include "prefault.h"
#include "logmsg.h"

int gbl_prefault_readahead = 1;
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
    struct thr_handle *thr_self;
    int retrys;
    int working_for;

    thr_self = thrman_register(THRTYPE_PREFAULT);
    thread_started("prefault helper");

    memcpy(&prefault_helper_thread_arg, arg,
           sizeof(prefault_helper_thread_arg_type));

    free(arg);

    dbenv = prefault_helper_thread_arg.dbenv;
    i = prefault_helper_thread_arg.instance;

    logmsg(LOGMSG_INFO, "prefault_helper_thread instance %d started as tid %d\n", i,
            pthread_self());

    backend_thread_event(dbenv, COMDB2_THR_EVENT_START_RDWR);

    /* thdinfo is assigned to thread specific variable unique_tag_key which
     * will automatically free it when the thread exits. */
    thdinfo = malloc(sizeof(struct thread_info));
    if (thdinfo == NULL) {
        logmsg(LOGMSG_FATAL, "**aborting due malloc failure thd %d\n",
                pthread_self());
        abort();
    }
    thdinfo->uniquetag = 0;
    thdinfo->ct_id_key = 0LL;
    thdinfo->ct_add_table = NULL;
    thdinfo->ct_del_table = NULL;
    thdinfo->ct_add_table = NULL;
    thdinfo->ct_del_table = NULL;
    pthread_setspecific(unique_tag_key, thdinfo);

    while (1) {
    again:

        rc = pthread_mutex_lock(&(dbenv->prefault_helper.mutex));
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "prefault_helper couldnt lock\n");
            exit(1);
        }

        /*fprintf(stderr, "setting working_for(%d) to invalid\n", i);*/

        /*fprintf(stderr, "setting working_for to invalid for helper %d\n",
         * i);*/
        /* clear who we're working for - we're idle and available now */
        dbenv->prefault_helper.threads[i].working_for = gbl_invalid_tid;
        MEMORY_SYNC;

        rc = pthread_cond_wait(&(dbenv->prefault_helper.threads[i].cond),
                               &(dbenv->prefault_helper.mutex));
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "prefault_helper couldnt unlock\n");
            exit(1);
        }

        /* bogus wakeup? */
        if (dbenv->prefault_helper.threads[i].working_for == gbl_invalid_tid) {
            rc = pthread_mutex_unlock(&(dbenv->prefault_helper.mutex));

            goto again;
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

        rc = pthread_mutex_unlock(&(dbenv->prefault_helper.mutex));
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "prefault_helper couldnt unlock\n");
            exit(1);
        }

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

            if (dbenv->prefault_helper.threads[i].working_for ==
                gbl_invalid_tid) {
                fprintf(stderr, "PREFAULT ERROR!  working_for was %d\n",
                        working_for);
                break;
            }

            rc = prefault_toblock(iq, blkstate, i,
                                  dbenv->prefault_helper.threads[i].seqnum,
                                  &(dbenv->prefault_helper.threads[i].abort));

            /*fprintf(stderr, "helper %d working for invalid2\n", i);*/
            thrman_where(thr_self, NULL);
            break;

        case PREFAULT_READAHEAD:
            thrman_where(thr_self, "prefault_readahead");
            rc = prefault_readahead(db, ixnum, key, keylen, numreadahead);
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
   
   fprintf(stderr, "prefault_helper_thread instance %d started as tid %d\n",
      i, pthread_self());

   backend_thread_event(dbenv, COMDB2_THR_EVENT_START_RDWR);


   while(1)
   {
     again:

      rc = pthread_mutex_lock(&(dbenv->prefault_helper.threads[i].mutex));
      if (rc != 0)
      {
         fprintf(stderr, "prefault_helper couldnt lock\n");
         exit(1);
      }
      
      /* clear who we're working for - we're idle and available now */
      /*fprintf(stderr, "helper %d working for invalid\n", i);*/
      
      dbenv->prefault_helper.threads[i].working_for = gbl_invalid_tid;
      MEMORY_SYNC;
      
      rc = pthread_cond_wait(&(dbenv->prefault_helper.threads[i].cond),
         &(dbenv->prefault_helper.threads[i].mutex));
      if (rc != 0)
      {
         fprintf(stderr, "prefault_helper couldnt unlock\n");
         exit(1);
      }

      MEMORY_SYNC;

      /* bogus wakeup? */
      if (dbenv->prefault_helper.threads[i].working_for == gbl_invalid_tid)
      {
         rc = pthread_mutex_unlock(&(dbenv->prefault_helper.threads[i].mutex));
         if (rc != 0)
         {
            fprintf(stderr, "prefault_helper couldnt unlock\n");
            exit(1);
         }
         
         goto again;
      }
      /*fprintf(stderr, "prefault_helper_thread %d woke up\n", i);*/

      /*sleep(1);*/
      
      rc = pthread_mutex_unlock(&(dbenv->prefault_helper.threads[i].mutex));
      if (rc != 0)
      {
         fprintf(stderr, "prefault_helper couldnt unlock\n");
         exit(1);
      }
   }
   
   backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
}
#endif

int create_prefault_helper_threads(struct dbenv *dbenv, int nthreads)
{
    int i;
    int rc;
    prefault_helper_thread_arg_type *prefault_helper_thread_arg;
    pthread_attr_t attr;

    dbenv->prefault_helper.numthreads = 0;

    rc = pthread_attr_init(&attr);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "create_prefault_helper_threads: pthread_attr_init\n");
        exit(1);
    }

    rc = pthread_attr_setstacksize(&attr, 512 * 1024);
    if (rc) {
        logmsg(LOGMSG_FATAL, 
                "create_prefault_helper_threads: pthread_attr_setstacksize\n");
        exit(1);
    }

    rc = pthread_mutex_init(&(dbenv->prefault_helper.mutex), NULL);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "could not initialize pre-fault data mutex %d\n", rc);
        exit(1);
    }

    for (i = 0; i < nthreads; i++) {
        prefault_helper_thread_arg =
            malloc(sizeof(prefault_helper_thread_arg_type));
        prefault_helper_thread_arg->dbenv = dbenv;
        prefault_helper_thread_arg->instance = i;

        rc = pthread_mutex_init(&(dbenv->prefault_helper.threads[i].mutex),
                                NULL);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "could not initialize pre-fault data mutex %d\n",
                    i);
            exit(1);
        }

        rc = pthread_cond_init(&(dbenv->prefault_helper.threads[i].cond), NULL);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "could not initialize pre-fault data mutex %d\n",
                    i);
            exit(1);
        }

        dbenv->prefault_helper.threads[i].pfk_bitmap =
            malloc(8 * gbl_maxblockops);

        bzero(dbenv->prefault_helper.threads[i].pfk_bitmap,
              8 * gbl_maxblockops);

        dbenv->prefault_helper.threads[i].seqnum = 1;

        MEMORY_SYNC;

        rc = pthread_create(&(dbenv->prefault_helper.threads[i].tid), &attr,
                            prefault_helper_thread, prefault_helper_thread_arg);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "couldnt create prefault_helper_thread\n");
            exit(1);
        }

        /* latch the first helper threads tid as our second invalid tid */
        if (i == 0)
            gbl_invalid_tid2 = dbenv->prefault_helper.threads[i].tid;

        dbenv->prefault_helper.numthreads++;
    }

    rc = pthread_attr_destroy(&attr);
    if (rc)
        /* we don't return an error here, what would be the point? */
        perror_errnum("create_prefault_helper_threads:pthread_attr_destroy",
                      rc);

    return 0;
}

int readaheadpf(struct ireq *iq, struct dbtable *db, int ixnum, unsigned char *key,
                int keylen, int num)
{
    pthread_t my_tid;
    pthread_t working_for;
    int rc;
    int i;

    if (!prefault_check_enabled())
        return 0;

    my_tid = pthread_self();

    rc = pthread_mutex_lock(&(iq->dbenv->prefault_helper.mutex));
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "readahead: couldnt lock main mutex\n");
        return -1;
    }

    for (i = 0; i < iq->dbenv->prefault_helper.numthreads; i++) {
        if (iq->dbenv->prefault_helper.threads[i].working_for ==
            gbl_invalid_tid) {
            /* found an idle thread to give this to! */
            /*fprintf(stderr, "dispatching to processor %d\n", i);*/
            rc = pthread_mutex_lock(
                &(iq->dbenv->prefault_helper.threads[i].mutex));
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, "readahead: couldnt lock thread %d mutex\n", i);
                exit(1);
            }

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

            rc = pthread_cond_signal(
                &(iq->dbenv->prefault_helper.threads[i].cond));
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, "readahead: couldnt cond signal thrd %d\n", i);
                exit(1);
            }

            rc = pthread_mutex_unlock(
                &(iq->dbenv->prefault_helper.threads[i].mutex));
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, "readahead: couldnt unlock thrd %d\n", i);
                exit(1);
            }

            /* Found someone to work for me, break out of loop */
            break;
        }
    }

    rc = pthread_mutex_unlock(&(iq->dbenv->prefault_helper.mutex));
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "readahead: couldnt unlock main mutex\n");
        exit(1);
    }

    return 0;
}
