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
 * Comdb2 mtrap module.
 *
 * Previously mtraps were done on the main thread.  This meant that if an
 * mtrap took a while the database would be unresponsive to qtraps and
 * we would get losing request alarms and all kinds of hell.  So we queue
 * up mtraps for processing on a separate thread.  If an mtrap takes a really
 * long time we can have its thread "detach" and execute separately.
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include <epochlib.h>
#include <list.h>
#include <lockmacro.h>

#include "comdb2.h"
#include "debug_switches.h"

struct mtrap_item {
    LINKC_T(struct mtrap_item) linkv;
    int lline;
    int st;
    char *msg;
};

static pthread_mutex_t mutex;
static pthread_cond_t cond;
static pthread_attr_t attr;
static LISTC_T(struct mtrap_item) queue;
static int have_mtrap_thread = 0;
static pthread_t mtrap_tid;
static int mtrap_busy = 0;
static int mtrap_start_time = 0;

long n_mtrap = 0;
long n_mtrap_inline = 0;
long n_mtrap_detached = 0;
long n_mtrap_async = 0;

static void run_mtrap(struct thr_handle *thr_self, struct mtrap_item *item)
{
    /* strip the msg of trailing whitespace and set the thread's where
     * status. */
    int eol, len;
    for (eol = item->lline; eol > item->st; eol--) {
        if (!isspace(item->msg[eol - 1]))
            break;
    }
    len = eol - item->st;
    thrman_wheref(thr_self, "<%*.*s>", len, len, item->msg + item->st);
    process_command(thedb, item->msg, item->lline, item->st);
    thrman_where(thr_self, NULL);
}

static void *mtrap_thd(void *voidarg)
{
    struct mtrap_item *item = voidarg;
    pthread_t me = pthread_self();
    struct thr_handle *thr_self = thrman_register(THRTYPE_MTRAP);
    int rc;

    thread_started("mtrap");

    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);

    /* Special case: we got started up to do a specific message trap
     * and then die. */
    if (item) {
        run_mtrap(thr_self, item);
        free(item);
    } else {
        pthread_mutex_lock(&mutex);

        /* If I am no longer the official mtrap thread then I will exit. */
        while (have_mtrap_thread && pthread_equal(me, mtrap_tid)) {

            mtrap_busy = 0;

            /* pull work off the queue */
            while ((item = listc_rtl(&queue)) == NULL) {
                /* no work.. wait for work.  I can't become detached while
                 * waiting for work. */
                rc = pthread_cond_wait(&cond, &mutex);
                if (rc != 0) {
                    fprintf(stderr, "%s:pthread_cond_wait: %d %s\n", __func__,
                            rc, strerror(rc));
                    /* not sure what we can sensibly do now.. */
                }
            }

            /* work to do.  do work. */
            mtrap_busy = 1;
            mtrap_start_time = time_epoch();
            pthread_mutex_unlock(&mutex);
            run_mtrap(thr_self, item);
            free(item);
            pthread_mutex_lock(&mutex);
            /* we may have been detached (no longer the offical mtrap
             * thread) so don't clear mtrap_busy just yet. */
        }
        pthread_mutex_unlock(&mutex);
    }

    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);

    return NULL;
}

static int ensure_thread_ll(void)
{
    /* If there is no mtrap thread currently then spawn one. */
    if (!have_mtrap_thread) {
        int rc = pthread_create(&mtrap_tid, &attr, mtrap_thd, NULL);
        if (rc == 0) {
            have_mtrap_thread = 1;
        } else {
            fprintf(stderr, "%s:pthread_create failed: %d %s\n", __func__, rc,
                    strerror(rc));
            return -1;
        }
    }

    /* wake up the mtrap thread */
    pthread_cond_signal(&cond);

    return 0;
}

static int enqueue_mtrap(const char *msg, int lline, int st)
{
    struct mtrap_item *item;
    int rc;
    int outrc;

    item = malloc(sizeof(struct mtrap_item) + lline + 1);
    if (!item) {
        fprintf(stderr, "%s: malloc failed\n", __func__);
        return -1;
    }

    item->lline = lline;
    item->st = st;
    item->msg = (char *)(item + 1);
    memcpy(item->msg, msg, lline);

    /* We don't have to \0 terminate this.. but we do it anyway as a safety
     * feature.  Also this allows us to pass it directly to thrman_where()
     * later on. */
    item->msg[lline] = 0;

    LOCK(&mutex)
    {
        listc_abl(&queue, item);
        outrc = ensure_thread_ll();
        if (outrc == -1) {
            listc_rfl(&queue, item);
            free(item);
        }

        if (mtrap_busy && time_epoch() - mtrap_start_time > 5) {
            printf("The current mtrap seems to be taking a long time\n");
            printf("to execute.  Use \"send %s detach_mtrap\" to detach\n",
                   thedb->envname);
            printf("the current mtrap thread so that othr traps can run.\n");
        }
    }
    UNLOCK(&mutex);

    return outrc;
}

void mtrap_detach(void)
{
    LOCK(&mutex)
    {
        if (have_mtrap_thread && mtrap_busy) {
            /* Detach it.  Next time we get a trap we will create a new
             * thread for it. */
            have_mtrap_thread = 0;
            mtrap_busy = 0;
            n_mtrap_detached++;

            /* If we have traps queued up immediately make a new thread */
            ensure_thread_ll();
        } else {
            printf(
                "No mtrap is currently executing on the main mtrap thread\n");
        }
    }
    UNLOCK(&mutex);
}

int mtrap_async(char *msg, int lline, int st)
{
    struct mtrap_item *item;
    int rc;
    pthread_t tid;

    item = malloc(sizeof(struct mtrap_item) + lline + 1);
    if (!item) {
        fprintf(stderr, "%s: malloc failed\n", __func__);
        return -1;
    }

    item->lline = lline;
    item->st = st;
    item->msg = (char *)(item + 1);
    memcpy(item->msg, msg, lline);

    /* We don't have to \0 terminate this.. but we do it anyway as a safety
     * feature.  Also this allows us to pass it directly to thrman_where()
     * later on. */
    item->msg[lline] = 0;

    rc = pthread_create(&tid, &attr, mtrap_thd, item);
    if (rc != 0) {
        fprintf(stderr, "%s:pthread_create failed: %d %s\n", __func__, rc,
                strerror(rc));
        free(item);
        return -1;
    }

    n_mtrap_async++;

    return 0;
}

void mtrap_init(void)
{
    listc_init(&queue, offsetof(struct mtrap_item, linkv));
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&cond, NULL);
    pthread_attr_init(&attr); /* TODO use gbl_pthread_attr or
                               * gbl_pthread_attr_detached instead? */
    pthread_attr_setstacksize(&attr, DEFAULT_THD_STACKSZ);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
}

void mtrap(char *msg)
{
    int lline, st, ltok;
    char *tok;

    n_mtrap++;

    lline = 64;
    st = 8;

    tok = segtok(msg, lline, &st, &ltok);
    ;
    if (tokcmp(tok, ltok, "detach_mtrap") == 0) {
        /* detach the currently executing mtrap thread */
        mtrap_detach();
        return;
    } else if (tokcmp(tok, ltok, "async") == 0) {
        /* execute this mtrap asynchronously */
        mtrap_async(msg, lline, st);
        return;
    } else if (tokcmp(tok, ltok, "inline") == 0) {
        /* Force this trap inline to the waitft thread */
        n_mtrap_inline++;
        process_command(thedb, msg, 64 /*lline*/, 8 /*st*/);
        return;
    } else if (tokcmp(tok, ltok, "stat") == 0) {
        tok = segtok(msg, lline, &st, &ltok);
        ;
        if (tokcmp(tok, ltok, "mtrap") == 0) {
            LOCK(&mutex)
            {
                struct mtrap_item *item;
                printf("Num mtraps                :  %ld\n", n_mtrap);
                printf("Num mtraps inline         :  %ld\n", n_mtrap_inline);
                printf("Num mtraps detached       :  %ld\n", n_mtrap_detached);
                printf("Num mtraps async          :  %ld\n", n_mtrap_async);
                printf("Have mtrap thread         :  %s\n",
                       have_mtrap_thread ? "YES" : "NO");
                printf("Currently running an mtrap:  %s\n",
                       mtrap_busy ? "YES" : "NO");
                LISTC_FOR_EACH(&queue, item, linkv)
                {
                    int eol, len;
                    for (eol = item->lline; eol > item->st; eol--) {
                        if (!isspace(item->msg[eol - 1]))
                            break;
                    }
                    len = eol - item->st;
                    printf("Enqueued: <%*.*s>\n", len, len,
                           item->msg + item->st);
                }
            }
            UNLOCK(&mutex);
            return;
        }
    }

    if (debug_switch_inline_mtraps() || enqueue_mtrap(msg, 64, 8) != 0) {
        /* Fallback to inline mtraps */
        n_mtrap_inline++;
        process_command(thedb, msg, 64 /*lline*/, 8 /*st*/);
    }
}
