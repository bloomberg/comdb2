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

/*
 * Generic thread pool for comdb2.  This implementation will grow the pool
 * as much as it needs to in order to meet demand.
 *
 * Shamelessly based on Peter Martin's bigsnd thread pool.
 */

#include <assert.h>
#include <alloca.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <strings.h>
#include "ctrace.h"
#include <epochlib.h>
#include <segstr.h>
#include "lockmacros.h"
#include "list.h"
#include "pool.h"
#include "mem_util.h"
#include "mem_override.h"
#include "thdpool.h"
#include "thread_util.h"
#include "thread_malloc.h"
#include "sys_wrap.h"
#include "debug_switches.h"
#include "logmsg.h"
#include "comdb2_atomic.h"
#include "string_ref.h"

#ifdef MONITOR_STACK
#include "comdb2_pthread_create.h"
#endif

int gbl_random_thdpool_work_timeout = 0;

extern int gbl_throttle_sql_overload_dump_sec;
extern int thdpool_alarm_on_queing(int len);
extern int gbl_disable_exit_on_thread_error;
extern comdb2bma blobmem;


struct thd {
    pthread_t tid;
    arch_tid archtid;
    struct thdpool *pool;

    /* Work item that we need to do. */
    struct workitem work;

    /* This is the description of work item in progress.
     * It is not owned by the thread (thd) structure, do
     * not free this. */
    const char *persistent_info;

    /* To signal thread if there is work for it. */
    pthread_cond_t cond;

    int on_freelist;

    LINKC_T(struct thd) thdlist_linkv;
    LINKC_T(struct thd) freelist_linkv;
};

struct thdpool {
    char *name;

    size_t per_thread_data_sz; /* size of thread specific data */

    int stopped;

    /* Work queue protected by mutex */
    pthread_mutex_t mutex;

    /* List of all threads */
    LISTC_T(struct thd) thdlist;

    /* List of all free threads */
    LISTC_T(struct thd) freelist;

    pthread_attr_t attrs;
    size_t stack_sz;

    thdpool_thdinit_fn init_fn;
    thdpool_thddelt_fn delt_fn;
    thdpool_thddque_fn dque_fn;

    unsigned minnthd;   /* desired number of threads */
    unsigned maxnthd;   /* max threads - queue after this point */
    unsigned nactthd;   /* current number of active threads */
    unsigned nwrkthd;   /* current number of working threads */
    unsigned nwaitthd;  /* current number of wait/consumer threads */
    unsigned peaknthd;  /* maximum num threads ever */
    unsigned maxqueue;  /* maximum work items to queue */
    unsigned peakqueue; /* peak queue size */
    unsigned lingersecs;
    unsigned longwaitms;       /* threshold for long wait alarm */
    unsigned maxqueueoverride; /* determine how many queueing exceptions (client
                                  driven) we allow */
    unsigned maxqueueagems;    /* maximum age in a queue */

    unsigned num_passed;
    unsigned num_enqueued;
    unsigned num_dequeued;
    unsigned num_timeout;
    unsigned num_completed;
    unsigned num_creates;
    unsigned num_exits;
    unsigned num_failed_dispatches;

    /* Keep a histogram of how many times we had n threads busy */
    unsigned *busy_hist;
    unsigned busy_hist_len;
    unsigned busy_hist_maxlen;

    /* Work queue, and pool for allocating queued work items.  We only start
     * queueing if all threads are busy and we've hit max threads. */
    pool_t *pool;
    LISTC_T(struct workitem) queue;

    int exit_on_create_fail;

    /* slow enqueue request to block until we have an available thread */
    int wait;
    int waiting_for_thread;
    pthread_cond_t wait_for_thread;

    int dump_on_full;

    int mem_sz;

    LINKC_T(struct thdpool) lnk;

    int last_queue_alarm;
    int last_alarm_max;

#ifdef MONITOR_STACK
    comdb2ma stack_alloc;
#endif
    void (*queued_callback)(void*);
};

pthread_mutex_t pool_list_lk = PTHREAD_MUTEX_INITIALIZER;
LISTC_T(struct thdpool) threadpools;
pthread_once_t init_pool_list_once = PTHREAD_ONCE_INIT;

static void init_pool_list(void)
{
    listc_init(&threadpools, offsetof(struct thdpool, lnk));
}

#include "tunables.h"

#define REGISTER_THDPOOL_TUNABLE(POOL, NAME, DESCR, TYPE, VAR_PTR, FLAGS,      \
                                 VALUE_FN, VERIFY_FN, UPDATE_FN, DESTROY_FN)   \
    snprintf(buf, sizeof(buf), "%s.%s", POOL, #NAME);                          \
    REGISTER_TUNABLE(buf, DESCR, TYPE, VAR_PTR, FLAGS, VALUE_FN, VERIFY_FN,    \
                     UPDATE_FN, DESTROY_FN)

static void register_thdpool_tunables(char *name, struct thdpool *pool)
{
    char buf[100];

    REGISTER_TUNABLE(name, NULL, TUNABLE_COMPOSITE, NULL, INTERNAL, NULL, NULL,
                     NULL, NULL);
    REGISTER_THDPOOL_TUNABLE(
        name, mint, "Minimum number of threads in the pool.", TUNABLE_INTEGER,
        &pool->minnthd, SIGNED, NULL, NULL, NULL, NULL);
    REGISTER_THDPOOL_TUNABLE(
        name, maxt, "Maximum number of threads in the pool.", TUNABLE_INTEGER,
        &pool->maxnthd, SIGNED, NULL, NULL, NULL, NULL);
    REGISTER_THDPOOL_TUNABLE(name, maxq, "Maximum size of queue.",
                             TUNABLE_INTEGER, &pool->maxqueue, SIGNED, NULL,
                             NULL, NULL, NULL);
    REGISTER_THDPOOL_TUNABLE(
        name, longwait, "Long wait alarm threshold (in milliseconds).",
        TUNABLE_INTEGER, &pool->longwaitms, SIGNED, NULL, NULL, NULL, NULL);
    REGISTER_THDPOOL_TUNABLE(name, linger, "Thread linger time (in seconds).",
                             TUNABLE_INTEGER, &pool->lingersecs, SIGNED, NULL,
                             NULL, NULL, NULL);
    REGISTER_THDPOOL_TUNABLE(name, stacksz, "Thread stack size.",
                             TUNABLE_INTEGER, &pool->stack_sz, SIGNED, NULL,
                             NULL, NULL, NULL);
    REGISTER_THDPOOL_TUNABLE(name, maxqover,
                             "Maximum client forced queued items above maxq.",
                             TUNABLE_INTEGER, &pool->maxqueueoverride, SIGNED,
                             NULL, NULL, NULL, NULL);
    REGISTER_THDPOOL_TUNABLE(
        name, maxagems, "Maximum age for in-queue time (in milliseconds).",
        TUNABLE_INTEGER, &pool->maxqueueagems, SIGNED, NULL, NULL, NULL, NULL);
    REGISTER_THDPOOL_TUNABLE(name, exit_on_error, "Exit on pthread error.",
                             TUNABLE_BOOLEAN, &pool->exit_on_create_fail, NOARG,
                             NULL, NULL, NULL, NULL);
    REGISTER_THDPOOL_TUNABLE(name, dump_on_full, "Dump status on full queue.",
                             TUNABLE_BOOLEAN, &pool->dump_on_full, NOARG, NULL,
                             NULL, NULL, NULL);
    return;
}

struct thdpool *thdpool_create(const char *name, size_t per_thread_data_sz)
{
    struct thdpool *pool;

    pool = calloc(1, sizeof(struct thdpool));
    if (!pool) {
        logmsg(LOGMSG_ERROR, "%s: out of memory\n", __func__);
        return NULL;
    }
    pool->name = strdup(name);
    if (!pool->name) {
        logmsg(LOGMSG_ERROR, "%s: strdup out of memory\n", __func__);
        free(pool);
        return NULL;
    }
    pool->pool = pool_init(sizeof(struct workitem), 0);
    if (!pool->pool) {
        logmsg(LOGMSG_ERROR, "%s: pool_init failed\n", __func__);
        free(pool->name);
        free(pool);
        return NULL;
    }
#ifdef MONITOR_STACK
    pool->stack_alloc =
        comdb2ma_create_with_scope(0, 0, "stack", pool->name, 1);
    if (pool->stack_alloc == NULL) {
        logmsg(LOGMSG_ERROR, "%s: comdb2ma_create failed\n", __func__);
        free(pool->name);
        free(pool);
        return NULL;
    }
#endif
    listc_init(&pool->thdlist, offsetof(struct thd, thdlist_linkv));
    listc_init(&pool->freelist, offsetof(struct thd, freelist_linkv));
    listc_init(&pool->queue, offsetof(struct workitem, linkv));

    Pthread_mutex_init(&pool->mutex, NULL);
    Pthread_attr_init(&pool->attrs);
    Pthread_attr_setstacksize(&pool->attrs, DEFAULT_THD_STACKSZ);
    Pthread_attr_setdetachstate(&pool->attrs, PTHREAD_CREATE_DETACHED);

    /* 1 meg default mem per thread for thread_malloc() */
    pool->mem_sz = 1 * 1024 * 1024;

    pool->per_thread_data_sz = per_thread_data_sz;
    pool->longwaitms = 500;
    pool->wait = 0;
    pool->exit_on_create_fail = 1;
    pool->dump_on_full = 0;
    pool->stack_sz = DEFAULT_THD_STACKSZ;

    Pthread_cond_init(&pool->wait_for_thread, NULL);

    Pthread_mutex_lock(&pool_list_lk);
    pthread_once(&init_pool_list_once, init_pool_list);
    listc_abl(&threadpools, pool);
    Pthread_mutex_unlock(&pool_list_lk);

    /* Register all tunables. */
    register_thdpool_tunables((char *)name, pool);

    return pool;
}

int thdpool_destroy(struct thdpool **pool_p, int coopWaitUs)
{
    struct thdpool *pool = pool_p ? *pool_p : NULL;

    if (!pool) {
        logmsg(LOGMSG_ERROR, "%s: invalid thread pool.\n", __func__);
        return -1;
    }

    if (coopWaitUs != 0) {
        const unsigned int waitUs = 50000; /* 50 milliseconds */
        unsigned int elapsedUs = 0;
        thdpool_stop(pool);
        while (ATOMIC_LOAD32(pool->nactthd) > 0) {
            usleep(waitUs);
            elapsedUs += waitUs;
            if ((coopWaitUs > 0) && (elapsedUs >= (unsigned int)coopWaitUs)) {
                logmsg(LOGMSG_ERROR,
                       "%s: pool %s wait timeout (%d microseconds)\n",
                       __func__, pool->name, elapsedUs);
                return -2;
            }
        }
        logmsg(LOGMSG_INFO,
               "%s: pool %s wait done (%d microseconds)\n", __func__,
               pool->name, elapsedUs);
    }

    *pool_p = NULL; /* OUT: Invalidate reference in caller. */

    Pthread_mutex_lock(&pool_list_lk);
    listc_rfl(&threadpools, pool);
    Pthread_mutex_unlock(&pool_list_lk);

    Pthread_cond_destroy(&pool->wait_for_thread);
    Pthread_mutex_destroy(&pool->mutex);
    Pthread_attr_destroy(&pool->attrs);

    struct workitem *tmp, *iter;

    LISTC_FOR_EACH_SAFE(&pool->queue, iter, tmp, linkv)
    {
      listc_rfl(&pool->queue, iter);
      free(iter);
    }

    free(pool->busy_hist);
    pool_free(pool->pool);
    free(pool->name);
    free(pool);
    return 0;
}

void thdpool_foreach(struct thdpool *pool, thdpool_foreach_fn foreach_fn,
                     void *user)
{
    LOCK(&pool->mutex)
    {
        struct workitem *item;
        LISTC_FOR_EACH(&pool->queue, item, linkv)
        {
            (foreach_fn)(pool, item, user);
        }
    }
    UNLOCK(&pool->mutex);
}

void thdpool_unset_exit(struct thdpool *pool) { pool->exit_on_create_fail = 0; }

void thdpool_set_linger(struct thdpool *pool, unsigned lingersecs)
{
    pool->lingersecs = lingersecs;
}

void thdpool_set_minthds(struct thdpool *pool, unsigned minnthd)
{
    pool->minnthd = minnthd;
}

void thdpool_set_maxthds(struct thdpool *pool, unsigned maxnthd)
{
    pool->maxnthd = maxnthd;
}

void thdpool_set_maxqueue(struct thdpool *pool, unsigned maxqueue)
{
    pool->maxqueue = maxqueue;
}

void thdpool_set_longwaitms(struct thdpool *pool, unsigned longwaitms)
{
    pool->longwaitms = longwaitms;
}

void thdpool_set_maxqueueagems(struct thdpool *pool, unsigned maxqueueagems)
{
    pool->maxqueueagems = maxqueueagems;
}

void thdpool_set_maxqueueoverride(struct thdpool *pool,
                                  unsigned maxqueueoverride)
{
    pool->maxqueueoverride = maxqueueoverride;
}

void thdpool_set_stack_size(struct thdpool *pool, size_t sz_bytes)
{
    LOCK(&pool->mutex)
    {
        pool->stack_sz = sz_bytes;
        Pthread_attr_setstacksize(&pool->attrs, pool->stack_sz);
    }
    UNLOCK(&pool->mutex);
}

void thdpool_set_mem_size(struct thdpool *pool, size_t sz_bytes)
{
    pool->mem_sz = sz_bytes;
}

void thdpool_set_init_fn(struct thdpool *pool, thdpool_thdinit_fn init_fn)
{
    pool->init_fn = init_fn;
}

void thdpool_set_delt_fn(struct thdpool *pool, thdpool_thddelt_fn delt_fn)
{
    pool->delt_fn = delt_fn;
}

void thdpool_set_dque_fn(struct thdpool *pool, thdpool_thddque_fn dque_fn)
{
    pool->dque_fn = dque_fn;
}

void thdpool_set_wait(struct thdpool *pool, int wait) { pool->wait = wait; }

void thdpool_set_dump_on_full(struct thdpool *pool, int onoff)
{
    pool->dump_on_full = onoff;
}

void thdpool_print_stats(FILE *fh, struct thdpool *pool)
{
    LOCK(&pool->mutex)
    {
        unsigned ii;
        logmsgf(LOGMSG_USER, fh, "Thread pool [%s] stats\n", pool->name);
        logmsgf(LOGMSG_USER, fh, "  Status                    : %s\n",
                pool->stopped ? "STOPPED" : "running");
        logmsgf(LOGMSG_USER, fh, "  Current num threads       : %u\n",
                listc_size(&pool->thdlist));
        logmsgf(LOGMSG_USER, fh, "  Num free threads          : %u\n",
                listc_size(&pool->freelist));
        logmsgf(LOGMSG_USER, fh, "  Peak num threads          : %u\n", pool->peaknthd);
        logmsgf(LOGMSG_USER, fh, "  Num thread creates        : %u\n", pool->num_creates);
        logmsgf(LOGMSG_USER, fh, "  Num thread exits          : %u\n", pool->num_exits);
        logmsgf(LOGMSG_USER, fh, "  Work items done immediate : %u\n", pool->num_passed);
        logmsgf(LOGMSG_USER, fh, "  Num work items enqueued   : %u\n", pool->num_enqueued);
        logmsgf(LOGMSG_USER, fh, "  Num work items dequeued   : %u\n", pool->num_dequeued);
        logmsgf(LOGMSG_USER, fh, "  Num work items timeout    : %u\n", pool->num_timeout);
        logmsgf(LOGMSG_USER, fh, "  Num work items completed  : %u\n", pool->num_completed);
        logmsgf(LOGMSG_USER, fh, "  Num failed dispatches     : %u\n",
                pool->num_failed_dispatches);
        logmsgf(LOGMSG_USER, fh, "  Desired num threads       : %u\n", pool->minnthd);
        logmsgf(LOGMSG_USER, fh, "  Maximum num threads       : %u\n", pool->maxnthd);
        logmsgf(LOGMSG_USER, fh, "  Num active threads        : %u\n", pool->nactthd);
        logmsgf(LOGMSG_USER, fh, "  Num working threads       : %u\n", pool->nwrkthd);
        logmsgf(LOGMSG_USER, fh, "  Num waiting threads       : %u\n", pool->nwaitthd);
        logmsgf(LOGMSG_USER, fh, "  Work queue peak size      : %u\n", pool->peakqueue);
        logmsgf(LOGMSG_USER, fh, "  Work queue maximum size   : %u\n", pool->maxqueue);
        logmsgf(LOGMSG_USER, fh, "  Work queue current size   : %u\n",
                listc_size(&pool->queue));
        logmsgf(LOGMSG_USER, fh, "  Long wait alarm threshold : %u ms\n", pool->longwaitms);
        logmsgf(LOGMSG_USER, fh, "  Thread linger time        : %u seconds\n",
                pool->lingersecs);
        logmsgf(LOGMSG_USER, fh, "  Thread stack size         : %zu bytes\n",
                pool->stack_sz);
        logmsgf(LOGMSG_USER, fh, "  Maximum queue override    : %u\n",
                pool->maxqueueoverride);
        logmsgf(LOGMSG_USER, fh, "  Maximum queue age         : %u ms\n",
                pool->maxqueueagems);
        logmsgf(LOGMSG_USER, fh, "  Exit on thread errors     : %s\n",
                pool->exit_on_create_fail ? "yes" : "no");
        logmsgf(LOGMSG_USER, fh, "  Dump on queue full        : %s\n",
                pool->dump_on_full ? "yes" : "no");
        for (ii = 0; ii < pool->busy_hist_len; ii++) {
            if ((ii & 3) == 0) {
                logmsgf(LOGMSG_USER, fh, "  Busy threads histogram    : ");
            } else {
                logmsgf(LOGMSG_USER, fh, ", ");
            }
            logmsgf(LOGMSG_USER, fh, "%2u:%8u", ii, pool->busy_hist[ii]);
            if ((ii & 3) == 3) {
                logmsgf(LOGMSG_USER, fh, "\n");
            }
        }
        if ((ii & 3) > 0 && (ii & 3) <= 3) {
            logmsgf(LOGMSG_USER, fh, "\n");
        }
    }
    UNLOCK(&pool->mutex);
}

void thdpool_list_pools(void)
{
    struct thdpool *pool;
    logmsg(LOGMSG_USER, "thread pools:\n");
    Pthread_mutex_lock(&pool_list_lk);
    LISTC_FOR_EACH(&threadpools, pool, lnk) { logmsg(LOGMSG_USER, "  %s\n", pool->name); }
    Pthread_mutex_unlock(&pool_list_lk);
}

void thdpool_command_to_all(char *line, int lline, int st)
{
    struct thdpool *pool;
    Pthread_mutex_lock(&pool_list_lk);
    LISTC_FOR_EACH(&threadpools, pool, lnk)
    {
        thdpool_process_message(pool, line, lline, st);
    }
    Pthread_mutex_unlock(&pool_list_lk);
}

void thdpool_process_message(struct thdpool *pool, char *line, int lline,
                             int st)
{
    char *tok;
    int ltok;
    tok = segtok(line, lline, &st, &ltok);
    if (tokcmp(tok, ltok, "stat") == 0) {
        thdpool_print_stats(stdout, pool);
    } else if (tokcmp(tok, ltok, "stop") == 0) {
        thdpool_stop(pool);
        logmsg(LOGMSG_USER, "Pool [%s] stopped\n", pool->name);
    } else if (tokcmp(tok, ltok, "resume") == 0) {
        thdpool_resume(pool);
        logmsg(LOGMSG_USER, "Pool [%s] resumed\n", pool->name);
    } else if (tokcmp(tok, ltok, "mint") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            thdpool_set_minthds(pool, toknum(tok, ltok));
        }
        logmsg(LOGMSG_USER, "Pool [%s] min threads set to %u\n", pool->name, pool->minnthd);
    } else if (tokcmp(tok, ltok, "maxt") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            thdpool_set_maxthds(pool, toknum(tok, ltok));
        }
        logmsg(LOGMSG_USER, "Pool [%s] max threads set to %u\n", pool->name, pool->maxnthd);
    } else if (tokcmp(tok, ltok, "maxq") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            thdpool_set_maxqueue(pool, toknum(tok, ltok));
        }
        logmsg(LOGMSG_USER, "Pool [%s] max queue set to %u\n", pool->name, pool->maxqueue);
    } else if (tokcmp(tok, ltok, "longwait") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            thdpool_set_longwaitms(pool, toknum(tok, ltok));
        }
        logmsg(LOGMSG_USER, "Pool [%s] long wait alarm threshold set to %u ms\n", pool->name,
               pool->longwaitms);
    } else if (tokcmp(tok, ltok, "linger") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            thdpool_set_linger(pool, toknum(tok, ltok));
        }
        logmsg(LOGMSG_USER, "Pool [%s] thread linger time set to %u secs\n", pool->name,
               pool->lingersecs);
    } else if (tokcmp(tok, ltok, "stacksz") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            thdpool_set_stack_size(pool, toknum(tok, ltok));
        }
        logmsg(LOGMSG_USER, "Pool [%s] thread stack size set to %zu bytes\n",
               pool->name, pool->stack_sz);
    } else if (tokcmp(tok, ltok, "maxqover") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            thdpool_set_maxqueueoverride(pool, toknum(tok, ltok));
        }
        logmsg(LOGMSG_USER, "Pool [%s] thread maximum queued items above limit %u entries\n",
               pool->name, pool->maxqueueoverride);
    } else if (tokcmp(tok, ltok, "maxagems") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            thdpool_set_maxqueueagems(pool, toknum(tok, ltok));
        }
        logmsg(LOGMSG_USER, "Pool [%s] thread maximum queued time %u ms\n", pool->name,
               pool->maxqueueagems);
    } else if (tokcmp(tok, ltok, "exit_on_error") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0)
            return;
        if (tokcmp(tok, ltok, "on") == 0) {
            pool->exit_on_create_fail = 1;
            logmsg(LOGMSG_USER, "%s will exit on pthread errors\n", pool->name);
        } else if (tokcmp(tok, ltok, "off") == 0) {
            pool->exit_on_create_fail = 0;
            logmsg(LOGMSG_USER, "%s won't exit on pthread errors\n", pool->name);
        }
    } else if (tokcmp(tok, ltok, "dump_on_full") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0)
            return;
        if (tokcmp(tok, ltok, "on") == 0) {
            pool->dump_on_full = 1;
            logmsg(LOGMSG_USER, "%s will dump status on full queue\n", pool->name);
        } else if (tokcmp(tok, ltok, "off") == 0) {
            pool->dump_on_full = 0;
            logmsg(LOGMSG_USER, "%s won't dump status on full queue\n", pool->name);
        }

    } else if (tokcmp(tok, ltok, "help") == 0) {
        logmsg(LOGMSG_USER, "Pool [%s] commands:-\n", pool->name);
        logmsg(LOGMSG_USER, "  stop      -            stop all threads\n");
        logmsg(LOGMSG_USER, "  resume    -            resume all threads\n");
        logmsg(LOGMSG_USER, "  mint #    -            set desired minimum number of threads\n");
        logmsg(LOGMSG_USER, "  maxt #    -            set maximum number of threads\n");
        logmsg(LOGMSG_USER, "  maxq #    -            set maximum allowed queue length\n");
        logmsg(LOGMSG_USER, "  longwait #-            set long wait alarm threshold in ms\n");
        logmsg(LOGMSG_USER, "  linger #  -            set linger time in seconds\n");
        logmsg(LOGMSG_USER, "  stacksz # -            set thread stack size in bytes\n");
        logmsg(LOGMSG_USER, "  maxqover #-            set maximum client forced queued items above maxq\n");
        logmsg(LOGMSG_USER, "  maxagems #-            set maximum age in ms for in-queue time\n");
        logmsg(LOGMSG_USER, "  exit_on_error on/off - enable/disable exit on thread errors \n");
        logmsg(LOGMSG_USER, "  dump_on_full on/off -  enable/disable dumping status on full queue\n");
    }
}

void thdpool_stop(struct thdpool *pool)
{
    LOCK(&pool->mutex)
    {
        struct thd *thd;
        pool->stopped = 1;
        LISTC_FOR_EACH(&pool->thdlist, thd, thdlist_linkv)
        {
            Pthread_cond_signal(&thd->cond);
        }
    }
    UNLOCK(&pool->mutex);
}

void thdpool_resume(struct thdpool *pool)
{
    LOCK(&pool->mutex) { pool->stopped = 0; }
    UNLOCK(&pool->mutex);
}

/* Get the next item of work for this thread to do.  Returns 0 if there
 * is no work. */
static int get_work_ll(struct thd *thd, struct workitem *work)
{
    if (thd->work.available) {
        memcpy(work, &thd->work, sizeof(struct workitem));
        memset(&thd->work, 0, sizeof(struct workitem));
        return 1;
    } else {
        struct thdpool *pool = thd->pool;
        struct workitem *next;
        while ((next = listc_rtl(&pool->queue)) != NULL) {
            int force_timeout = 0;
            if ((thd->pool->maxqueueagems > 0) &&
                gbl_random_thdpool_work_timeout &&
                !(rand() % gbl_random_thdpool_work_timeout)) {
                force_timeout = 1;
                logmsg(LOGMSG_WARN,
                       "%s: forcing a random work item timeout\n",
                       __func__);
            }
            if (force_timeout || (thd->pool->maxqueueagems > 0 &&
                comdb2_time_epochms() - next->queue_time_ms >
                    pool->maxqueueagems)) {
                if (pool->dque_fn)
                    pool->dque_fn(thd->pool, next, 1);
                if (next->ref_persistent_info) {
                    put_ref(&next->ref_persistent_info);
                }
                next->work_fn(pool, next->work, NULL, THD_FREE);
                pool_relablk(pool->pool, next);
                thd->pool->num_timeout++;
                continue;
            }

            if (pool->dque_fn)
                pool->dque_fn(pool, next, 0);
            memcpy(work, next, sizeof(*work));
            pool_relablk(pool->pool, next);
            pool->num_dequeued++;
            return 1;
        }
        return 0;
    }
}

static void *thdpool_thd(void *voidarg)
{
    struct thd *thd = voidarg;
    struct thdpool *pool = thd->pool;

    comdb2_name_thread(pool->name);
    ATOMIC_ADD32(pool->nactthd, 1);
#   ifndef NDEBUG
    logmsg(LOGMSG_DEBUG, "%s(%s): thread going active: %u active\n",
           __func__, pool->name, ATOMIC_LOAD32(pool->nactthd));
#   endif
    int check_exit = 0;
    void *thddata = NULL;

    thdpool_thdinit_fn init_fn;
    thdpool_thddelt_fn delt_fn;

    thread_started("thdpool");

    ENABLE_PER_THREAD_MALLOC(pool->name);
    thd->archtid = getarchtid();

    if (pool->per_thread_data_sz > 0) {
        thddata = alloca(pool->per_thread_data_sz);
        assert(thddata != NULL);
        memset(thddata, 0, pool->per_thread_data_sz);
    }

    init_fn = pool->init_fn;
    if (init_fn)
        init_fn(pool, thddata);
    thread_memcreate(pool->mem_sz);
    struct workitem work = {0};

    while (1) {
        int diffms;

        LOCK(&pool->mutex)
        {
            thd->persistent_info = "looking for work...";

            struct timespec timeout;
            struct timespec *ts = NULL;
            int thr_exit = 0;

            if (pool->maxnthd > 0 &&
                listc_size(&pool->thdlist) > (pool->maxnthd + pool->nwaitthd))
                check_exit = 1;
            else
                check_exit = 0;

            if (pool->waiting_for_thread)
                Pthread_cond_signal(&pool->wait_for_thread);

            /* Get work.  If there is no work then place us on the free
             * list and wait for work. */
            memset(&work, 0, sizeof(struct workitem)); /* work is output, zero first */
            while (!get_work_ll(thd, &work)) {
                int rc = 0;
                if (listc_size(&pool->thdlist) > pool->minnthd && !ts) {
                    /* we have more threads than we want - wait for a bit then
                     * timeout */
                    if (pool->lingersecs > 0) {
                        struct timeval tp;
                        gettimeofday(&tp, NULL);
                        timeout.tv_sec = tp.tv_sec + pool->lingersecs;
                        timeout.tv_nsec = tp.tv_usec * 1000;
                        ts = &timeout;
                    } else {
                        /* no linger, die now */
                        thr_exit = 1;
                    }
                }
                if (pool->stopped || thr_exit) {
                    /* Thread exiting - remove from pools lists */
                    listc_rfl(&pool->thdlist, thd);
                    if (thd->on_freelist) {
                        listc_rfl(&pool->freelist, thd);
                        thd->on_freelist = 0;
                    }
                    pool->num_exits++;
                    errUNLOCK(&pool->mutex);

                    goto thread_exit;
                }
                /* Go to the head of the free list so we get work sooner.  This
                 * way the same thread keeps busy most of the time so we get
                 * better cache localities etc and most significantly of all
                 * excess threads can timeout and die.  We explicitly don't
                 * want to round robin our work distribution as that spoils
                 * the timeout logic. */
                if (!thd->on_freelist) {
                    listc_atl(&pool->freelist, thd);
                    thd->on_freelist = 1;
                }
                if (ts) {
                    rc = pthread_cond_timedwait(&thd->cond, &pool->mutex, ts);
                } else {
                    Pthread_cond_wait(&thd->cond, &pool->mutex);
                }
                if (rc == ETIMEDOUT) {
                    /* Make sure we don't get into a hot loop. */
                    ts = NULL;
                    /* If there's still no work we'll die. */
                    thr_exit = 1;
                } else if (rc != 0 && rc != EINTR) {
                    logmsg(LOGMSG_ERROR,
                           "%s(%s):pthread_cond_timedwait: %d %s\n", __func__,
                           pool->name, rc, strerror(rc));
                }
            }

            /* We have work.  We will already have been removed from the
             * free list by the enqueue function so just take our work
             * parameters, release lock and do it. */

            /* Since there is (now) no escape from this code path without
             * actually performing the work, set the thread state for the
             * current work in progress, obtained from get_work_ll, while
             * still holding the pool lock. */

            if (work.ref_persistent_info)
                thd->persistent_info = string_ref_cstr(work.ref_persistent_info); // will reset this before put_ref() below
            else
                thd->persistent_info = "working on unknown";
        }
        UNLOCK(&pool->mutex);

        diffms = comdb2_time_epochms() - work.queue_time_ms;
        if (diffms > pool->longwaitms) {
            logmsg(LOGMSG_WARN, "%s(%s): long wait %d ms\n", __func__, pool->name,
                    diffms);
        }

        ATOMIC_ADD32(pool->nwrkthd, 1);
        work.work_fn(pool, work.work, thddata, THD_RUN);
        ATOMIC_ADD32(pool->nwrkthd, -1);
        ATOMIC_ADD32(pool->num_completed, 1);

        /* work is no longer pending, reset thread state for
         * the current work in progress before doing anything
         * else.  this should make it as accurate as possible
         * from the perspective of other threads that may need
         * to examine it. */
        LOCK(&pool->mutex) {
            thd->persistent_info = "work completed.";
            if (work.ref_persistent_info) {
                put_ref(&work.ref_persistent_info);
            }
        }
        UNLOCK(&pool->mutex);

        /* might this is set at a certain point by work_fn */
        thread_util_donework();
        if (check_exit) {
            LOCK(&pool->mutex)
            {
                if (pool->maxnthd > 0 && listc_size(&pool->thdlist) >
                                             (pool->maxnthd + pool->nwaitthd)) {
                    listc_rfl(&pool->thdlist, thd);
                    if (thd->on_freelist) {
                        listc_rfl(&pool->freelist, thd);
                        thd->on_freelist = 0;
                    }
                    pool->num_exits++;
                    errUNLOCK(&pool->mutex);
                    goto thread_exit;
                }
            }
            UNLOCK(&pool->mutex);
        }

        // ready to perform yield operation, update thread info again
        LOCK(&pool->mutex) {
            thd->persistent_info = "yielding...";
        }
        UNLOCK(&pool->mutex);

        // before acquiring next request, yield
        comdb2bma_yield_all();
    }
thread_exit:

    delt_fn = pool->delt_fn;
    if (delt_fn)
        delt_fn(pool, thddata);

    Pthread_cond_destroy(&thd->cond);

    thread_memdestroy();

    free(thd);

#   ifndef NDEBUG
    logmsg(LOGMSG_DEBUG, "%s(%s): thread going inactive: %u active\n",
           __func__, pool->name, ATOMIC_LOAD32(pool->nactthd));
#   endif
    ATOMIC_ADD32(pool->nactthd, -1);
    return NULL;
}

int thdpool_enqueue(struct thdpool *pool, thdpool_work_fn work_fn, void *work,
                    int queue_override, struct string_ref *ref_persistent_info,
                    uint32_t flags)
{
    static time_t last_dump = 0;
    int enqueue_front = (flags & THDPOOL_ENQUEUE_FRONT);
    int force_dispatch = (flags & THDPOOL_FORCE_DISPATCH);
    int queue_only = (flags & THDPOOL_QUEUE_ONLY);

    /* If queue_override is true, try to enqueue unless hitting maxqoverride;
       If force_queue is true, enqueue regardless. */
    int force_queue = (flags & THDPOOL_FORCE_QUEUE);

    time_t crt_dump;

    LOCK(&pool->mutex)
    {
        struct thd *thd;
        struct workitem *item = NULL;
        unsigned nbusy;
        int did_create = 0;

        if (pool->stopped) {
            pool->num_failed_dispatches++;
            errUNLOCK(&pool->mutex);
            logmsg(LOGMSG_ERROR, "%s(%s): cannot enque to a stopped pool\n",
                    __func__, pool->name);
            return -1;
        }

        /* Keep our histogram of how often n threads were busy when we entered
         * enqueue. */
        nbusy = listc_size(&pool->thdlist) - listc_size(&pool->freelist);
        if (nbusy >= pool->busy_hist_maxlen) {
            unsigned *newp;
            pool->busy_hist_maxlen = nbusy + 1;
            newp = realloc(pool->busy_hist,
                           sizeof(unsigned) * pool->busy_hist_maxlen);
            if (!newp) {
                pool->num_failed_dispatches++;
                errUNLOCK(&pool->mutex);
                logmsg(LOGMSG_ERROR, "%s(%s): realloc of histogram failed\n",
                        __func__, pool->name);
                return -1;
            }
            pool->busy_hist = newp;
            bzero(pool->busy_hist + pool->busy_hist_len,
                  sizeof(unsigned) *
                      (pool->busy_hist_maxlen - pool->busy_hist_len));
        }
        if (nbusy >= pool->busy_hist_len) {
            pool->busy_hist_len = nbusy + 1;
        }
        pool->busy_hist[nbusy]++;

    /* Get a free thread, creating one if necessary and if we're allowed
     * more threads.  Note that the thread cannot enter its work loop
     * until the lock is released, which gives us a window to assign the
     * work item to the new thread. */
    again:
        thd = listc_rtl(&pool->freelist);
        if (thd) {
            assert(thd->on_freelist);
            thd->on_freelist = 0;
        }
        if (!thd &&
            (force_dispatch || pool->maxnthd == 0 ||
             listc_size(&pool->thdlist) < (pool->maxnthd + pool->nwaitthd))) {
            int rc;

            thd = calloc(1, sizeof(struct thd));
            if (!thd) {
                pool->num_failed_dispatches++;
                errUNLOCK(&pool->mutex);
                logmsg(LOGMSG_ERROR, "%s(%s):malloc %u failed\n", __func__,
                        pool->name, (unsigned)sizeof(struct thd));
                return -1;
            }

            Pthread_cond_init(&thd->cond, NULL);
            thd->pool = pool;
            listc_atl(&pool->thdlist, thd);

#ifdef MONITOR_STACK
            rc = comdb2_pthread_create(&thd->tid, &pool->attrs, thdpool_thd,
                                       thd, pool->stack_alloc, pool->stack_sz);
#else
            rc = pthread_create(&thd->tid, &pool->attrs, thdpool_thd, thd);
#endif
            if (rc != 0) {

                if (pool->exit_on_create_fail) {
                    logmsg(LOGMSG_ERROR, "pthread_create rc %d, exiting\n", rc);
                    if (!gbl_disable_exit_on_thread_error)
                        exit(1);
                }

                logmsg(LOGMSG_DEBUG, "CREATED %p\n", (void *)thd->tid);

                listc_rfl(&pool->thdlist, thd);
                pool->num_failed_dispatches++;
                errUNLOCK(&pool->mutex);
                logmsg(LOGMSG_ERROR, "%s(%s):pthread_create: %d %s\n", __func__,
                        pool->name, rc, strerror(rc));
                Pthread_cond_destroy(&thd->cond);
                free(thd);
                return -1;
            }
            if (listc_size(&pool->thdlist) > pool->peaknthd) {
                pool->peaknthd = listc_size(&pool->thdlist);
            }
            did_create = 1;
            pool->num_creates++;
        }

        if ((queue_only && did_create) || (thd == NULL && pool->wait)) {

            pool->waiting_for_thread = 1;
            Pthread_cond_wait(&pool->wait_for_thread, &pool->mutex);
            pool->waiting_for_thread = 0;

            if (!queue_only) goto again;
        }

        if (!queue_only && thd) {
            item = &thd->work;
            pool->num_passed++;
        } else {
#ifndef NDEBUG
            /* TODO: Carefully evaluate this code for non-debug builds. */
            /* if there are no active threads (i.e. we did not start one?),
             * there may not be much point in queueing an event that may
             * never be processed? */
            if (ATOMIC_LOAD32(pool->nactthd) == 0) {
                // TODO (NC): study
                logmsg(LOGMSG_INFO, "%s(%s): queue with no threads active?\n",
                       __func__, pool->name);
            }
#endif
            /* queue work */
            int queue_count = listc_size(&pool->queue);

            if (queue_count >= pool->maxqueue) {
                if (force_queue ||
                    (queue_override &&
                     (enqueue_front || !pool->maxqueueoverride ||
                      queue_count <
                          (pool->maxqueue + pool->maxqueueoverride)))) {
                    if (thdpool_alarm_on_queing(queue_count)) {
                        int now = comdb2_time_epoch();

                        if (now > pool->last_queue_alarm ||
                            queue_count > pool->last_alarm_max) {
                            logmsg(LOGMSG_USER, "%d Queing sql, queue size=%d. "
                                            "max_queue=%d "
                                            "max_queue_override=%d\n",
                                    __LINE__, queue_count,
                                    pool->maxqueue, pool->maxqueueoverride);

                            pool->last_queue_alarm = now;
                            pool->last_alarm_max = queue_count;
                        }
                    }
                } else {
                    if (queue_override) {
                        logmsg(LOGMSG_USER, "%d FAILED to queue sql, queue "
                                        "size=%d. max_queue=%d "
                                        "max_queue_override=%d\n",
                                __LINE__, queue_count,
                                pool->maxqueue, pool->maxqueueoverride);
                    }

                    /* go through all the threads and print them */
                    if (debug_switch_dump_pool_on_full()) {
                        int crt = 0;

                        crt_dump = time(NULL);

                        if (pool->dump_on_full &&
                            ((last_dump == 0) ||
                             (crt_dump >=
                              (last_dump +
                               gbl_throttle_sql_overload_dump_sec)))) {

                            ctrace(" === Dumping current pool \"%s\" users:\n",
                                   pool->name);


                            LISTC_FOR_EACH(&pool->thdlist, thd, thdlist_linkv)
                            {
                                crt++;
                                ctrace("%d. %s\n", crt,
                                       (thd->persistent_info)
                                           ? thd->persistent_info
                                           : "NULL");
                            }
                            ctrace(" === Done (%d sql queries)\n", crt);
                            last_dump = time(
                                NULL); /* grab the time at the end of logging */
                        }
                    }
                    pool->num_failed_dispatches++;
                    errUNLOCK(&pool->mutex);
                    /* this is a ctrace now */
                    if (pool->dump_on_full)
                        ctrace("%s(%s):all threads busy and queue full, see "
                               "trace file\n",
                               __func__, pool->name);

                    return -1;
                }
            }
            item = pool_getablk(pool->pool);
            if (!item) {
                pool->num_failed_dispatches++;
                errUNLOCK(&pool->mutex);
                logmsg(LOGMSG_ERROR, "%s(%s):pool_getablk failed\n", __func__,
                        pool->name);
                return -1;
            }

            if (enqueue_front)
                listc_atl(&pool->queue, item);
            else
                listc_abl(&pool->queue, item);
            pool->num_enqueued++;

            if (pool->queued_callback)
                pool->queued_callback(work);

            if (queue_count > pool->peakqueue) {
                pool->peakqueue = queue_count;
            }
        }

        item->work = work;
        item->work_fn = work_fn;
        transfer_ref(&ref_persistent_info, &item->ref_persistent_info); // item gets ownership of reference
        item->queue_time_ms = comdb2_time_epochms();
        item->available = 1;

        /* Now wake up the thread with work to do. */
        if (!thd) {
            comdb2bma_yield_all();
        } else {
            comdb2bma_transfer_priority(blobmem, thd->tid);
            Pthread_cond_signal(&thd->cond);
        }
    }
    UNLOCK(&pool->mutex);

    return 0;
}

/* No locks, so not 100% accurate */
char *thdpool_get_name(struct thdpool *pool)
{
    return pool->name;
}

int thdpool_get_status(struct thdpool *pool)
{
    return pool->stopped;
}

int thdpool_get_nthds(struct thdpool *pool)
{
    return pool->thdlist.count;
}

int thdpool_get_nfreethds(struct thdpool *pool)
{
    return pool->freelist.count;
}

int thdpool_get_nbusythds(struct thdpool *pool)
{
    return pool->thdlist.count - pool->freelist.count;
}

void thdpool_add_waitthd(struct thdpool *pool)
{
    pool->nwaitthd++;
}

void thdpool_remove_waitthd(struct thdpool *pool)
{
    pool->nwaitthd--;
}

int thdpool_get_maxthds(struct thdpool *pool)
{
    return pool->maxnthd;
}

int thdpool_get_peaknthds(struct thdpool *pool)
{
    return pool->peaknthd;
}

int thdpool_get_creates(struct thdpool *pool)
{
    return pool->num_creates;
}

int thdpool_get_exits(struct thdpool *pool)
{
    return pool->num_exits;
}

int thdpool_get_passed(struct thdpool *pool)
{
    return pool->num_passed;
}

int thdpool_get_enqueued(struct thdpool *pool)
{
    return pool->num_enqueued;
}

int thdpool_get_dequeued(struct thdpool *pool)
{
    return pool->num_dequeued;
}

int thdpool_get_timeouts(struct thdpool *pool)
{
    return pool->num_timeout;
}

int thdpool_get_failed_dispatches(struct thdpool *pool)
{
    return pool->num_failed_dispatches;
}

int thdpool_get_minnthd(struct thdpool *pool)
{
    return pool->minnthd;
}

int thdpool_get_maxnthd(struct thdpool *pool)
{
    return pool->maxnthd;
}

int thdpool_get_peakqueue(struct thdpool *pool)
{
    return pool->peakqueue;
}

int thdpool_get_maxqueue(struct thdpool *pool)
{
    return pool->maxqueue;
}

int thdpool_get_nqueuedworks(struct thdpool *pool)
{
    return listc_size(&pool->queue);
}

int thdpool_get_longwaitms(struct thdpool *pool)
{
    return pool->longwaitms;
}

int thdpool_get_lingersecs(struct thdpool *pool)
{
    return pool->lingersecs;
}

int thdpool_get_stacksz(struct thdpool *pool)
{
    return pool->stack_sz;
}

int thdpool_get_maxqueueoverride(struct thdpool *pool)
{
    return pool->maxqueueoverride;
}

int thdpool_get_maxqueueagems(struct thdpool *pool)
{
    return pool->maxqueueagems;
}

int thdpool_get_exit_on_create_fail(struct thdpool *pool)
{
    return pool->exit_on_create_fail;
}

int thdpool_get_dump_on_full(struct thdpool *pool)
{
    return pool->dump_on_full;
}

int thdpool_lock(struct thdpool *pool)
{
    Pthread_mutex_lock(&pool->mutex);
    return 0;
}

int thdpool_unlock(struct thdpool *pool)
{
    Pthread_mutex_unlock(&pool->mutex);
    return 0;
}

struct thdpool *thdpool_next_pool(struct thdpool *pool)
{
    return (pool) ? pool->lnk.next : 0;
}

int thdpool_get_queue_depth(struct thdpool *pool)
{
    return listc_size(&pool->queue);
}

void thdpool_set_queued_callback(struct thdpool *pool, void(*callback)(void*)) 
{
    pool->queued_callback = callback;
}

void comdb2_name_thread(const char *name) {
#ifdef __linux
    pthread_setname_np(pthread_self(), name);
#elif defined __APPLE__
    pthread_setname_np(name);
#endif
}
