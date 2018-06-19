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

#include "limit_fortify.h"
#include <alloca.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/time.h>
#include "ctrace.h"

#include <unistd.h>

#include <epochlib.h>
#include <lockmacro.h>
#include <segstring.h>

#include "list.h"
#include "pool.h"
#include "mem_util.h"
#include "mem_override.h"
#include "thdpool.h"
#include "thread_util.h"
#include "thread_malloc.h"

#include "debug_switches.h"

#ifdef MONITOR_STACK
#include "comdb2_pthread_create.h"
#endif
#include "logmsg.h"

extern int gbl_throttle_sql_overload_dump_sec;
extern int thdpool_alarm_on_queing(int len);
extern int gbl_disable_exit_on_thread_error;

struct workitem {
    void *work;
    thdpool_work_fn work_fn;
    int queue_time_ms;
    LINKC_T(struct workitem) linkv;
    int available;
    char *persistent_info;
};

struct thd {
    pthread_t tid;
    arch_tid archtid;
    struct thdpool *pool;

    /* Work item that we need to do. */
    struct workitem work;

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

    unsigned minnthd;   /* desired number of threads */
    unsigned maxnthd;   /* max threads - queue after this point */
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

    pthread_mutex_init(&pool->mutex, NULL);
    pthread_attr_init(&pool->attrs);
    pthread_attr_setstacksize(&pool->attrs, DEFAULT_THD_STACKSZ);
    pthread_attr_setdetachstate(&pool->attrs, PTHREAD_CREATE_DETACHED);

    /* 1 meg default mem per thread for thread_malloc() */
    pool->mem_sz = 1 * 1024 * 1024;

    pool->per_thread_data_sz = per_thread_data_sz;
    pool->longwaitms = 500;
    pool->wait = 0;
    pool->exit_on_create_fail = 1;
    pool->dump_on_full = 0;
    pool->stack_sz = DEFAULT_THD_STACKSZ;

    pthread_cond_init(&pool->wait_for_thread, NULL);

    pthread_mutex_lock(&pool_list_lk);
    pthread_once(&init_pool_list_once, init_pool_list);
    listc_abl(&threadpools, pool);
    pthread_mutex_unlock(&pool_list_lk);

    /* Register all tunables. */
    register_thdpool_tunables((char *)name, pool);

    return pool;
}

void thdpool_set_exit(struct thdpool *pool) { pool->exit_on_create_fail = 1; }

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
        pthread_attr_setstacksize(&pool->attrs, pool->stack_sz);
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
        logmsgf(LOGMSG_USER, fh, "  Num failed dispatches     : %u\n",
                pool->num_failed_dispatches);
        logmsgf(LOGMSG_USER, fh, "  Desired num threads       : %u\n", pool->minnthd);
        logmsgf(LOGMSG_USER, fh, "  Maximum num threads       : %u\n", pool->maxnthd);
        logmsgf(LOGMSG_USER, fh, "  Work queue peak size      : %u\n", pool->peakqueue);
        logmsgf(LOGMSG_USER, fh, "  Work queue maximum size   : %u\n", pool->maxqueue);
        logmsgf(LOGMSG_USER, fh, "  Work queue current size   : %u\n",
                listc_size(&pool->queue));
        logmsgf(LOGMSG_USER, fh, "  Long wait alarm threshold : %u ms\n", pool->longwaitms);
        logmsgf(LOGMSG_USER, fh, "  Thread linger time        : %u seconds\n",
                pool->lingersecs);
        logmsgf(LOGMSG_USER, fh, "  Thread stack size         : %zu bytes\n",
                pool->stack_sz);
        logmsgf(LOGMSG_USER, fh, "  Maximum queue overload    : %u\n",
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
    pthread_mutex_lock(&pool_list_lk);
    LISTC_FOR_EACH(&threadpools, pool, lnk) { logmsg(LOGMSG_USER, "  %s\n", pool->name); }
    pthread_mutex_unlock(&pool_list_lk);
}

void thdpool_command_to_all(char *line, int lline, int st)
{
    struct thdpool *pool;
    pthread_mutex_lock(&pool_list_lk);
    LISTC_FOR_EACH(&threadpools, pool, lnk)
    {
        thdpool_process_message(pool, line, lline, st);
    }
    pthread_mutex_unlock(&pool_list_lk);
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
            pthread_cond_signal(&thd->cond);
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
    struct workitem *next;

    if (thd->work.available) {
        memcpy(work, &thd->work, sizeof(*work));
        thd->work.available = 0;
        return 1;
    } else {
        while ((next = listc_rtl(&thd->pool->queue)) != NULL) {
            if (thd->pool->maxqueueagems > 0 &&
                comdb2_time_epochms() - work->queue_time_ms >
                    thd->pool->maxqueueagems) {
                if (next->persistent_info) {
                    free(next->persistent_info);
                    next->persistent_info = NULL;
                }
                thd->work.work_fn(thd->pool, next->work, NULL, THD_FREE);
                pool_relablk(thd->pool->pool, next);
                thd->pool->num_timeout++;
                continue;
            }

            memcpy(work, next, sizeof(*work));
            pool_relablk(thd->pool->pool, next);
            thd->pool->num_dequeued++;
            thd->work.persistent_info = next->persistent_info;
            return 1;
        }

        return 0;
    }
}

// call after obtaining pool lock
static inline void free_work_persistent_info(struct thd *thd,
                                             struct workitem *work)
{
    free(work->persistent_info);
    if (thd->work.persistent_info == work->persistent_info)
        thd->work.persistent_info = NULL;
    work->persistent_info = NULL;
}

static void *thdpool_thd(void *voidarg)
{
    struct thd *thd = voidarg;
    struct thdpool *pool = thd->pool;
    void *thddata = NULL;
    extern comdb2bma blobmem;

    thdpool_thdinit_fn init_fn;
    thdpool_thddelt_fn delt_fn;

    thread_started("thdpool");

#ifdef PER_THREAD_MALLOC
    thread_type_key = pool->name;
#endif
    thd->archtid = getarchtid();

    if (pool->per_thread_data_sz > 0) {
        thddata = alloca(pool->per_thread_data_sz);
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
            if (work.persistent_info) {
                free_work_persistent_info(thd, &work);
            }
            struct timespec timeout;
            struct timespec *ts = NULL;
            int thr_exit = 0;

            if (pool->wait && pool->waiting_for_thread)
                pthread_cond_signal(&pool->wait_for_thread);

            /* Get work.  If there is no work then place us on the free
             * list and wait for work. */
            while (!get_work_ll(thd, &work)) {
                int rc;
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
                    if (thd->on_freelist)
                        listc_rfl(&pool->freelist, thd);
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
                    rc = pthread_cond_wait(&thd->cond, &pool->mutex);
                }
                if (rc == ETIMEDOUT) {
                    /* Make sure we don't get into a hot loop. */
                    ts = NULL;
                    /* If there's still no work we'll die. */
                    thr_exit = 1;
                } else if (rc != 0 && rc != EINTR) {
                    logmsg(LOGMSG_ERROR, "%s(%s):pthread_cond_wait: %d %s\n",
                            __func__, pool->name, rc, strerror(rc));
                }
            }

            /* We have work.  We will already have been removed from the
             * free list by the enqueue function so just take our work
             * parameters, release lock and do it. */
        }
        UNLOCK(&pool->mutex);

        diffms = comdb2_time_epochms() - work.queue_time_ms;
        if (diffms > pool->longwaitms) {
            logmsg(LOGMSG_WARN, "%s(%s): long wait %d ms\n", __func__, pool->name,
                    diffms);
        }

        work.work_fn(pool, work.work, thddata, THD_RUN);

        /* might this is set at a certain point by work_fn */
        thread_util_donework();

        // before acquiring next request, yield
        comdb2bma_yield_all();
    }
thread_exit:

    if (work.persistent_info) {
        LOCK(&pool->mutex) { free_work_persistent_info(thd, &work); }
        UNLOCK(&pool->mutex);
    }

    delt_fn = pool->delt_fn;
    if (delt_fn)
        delt_fn(pool, thddata);

    pthread_cond_destroy(&thd->cond);

    thread_memdestroy();

    free(thd);

    return NULL;
}

int thdpool_enqueue(struct thdpool *pool, thdpool_work_fn work_fn, void *work,
                    int queue_override, char *persistent_info)
{
    static time_t last_dump = 0;
    time_t crt_dump;
    size_t mem_sz;
    extern comdb2bma blobmem;

    LOCK(&pool->mutex)
    {
        struct thd *thd;
        struct workitem *item = NULL;
        unsigned nbusy;

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
        if (!thd && (pool->maxnthd == 0 ||
                     listc_size(&pool->thdlist) < pool->maxnthd)) {
            int rc;

            thd = calloc(1, sizeof(struct thd));
            if (!thd) {
                pool->num_failed_dispatches++;
                errUNLOCK(&pool->mutex);
                logmsg(LOGMSG_ERROR, "%s(%s):malloc %u failed\n", __func__,
                        pool->name, (unsigned)sizeof(struct thd));
                return -1;
            }

            pthread_cond_init(&thd->cond, NULL);
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

                logmsg(LOGMSG_DEBUG, "CREATED %lu\n", thd->tid);

                listc_rfl(&pool->thdlist, thd);
                pool->num_failed_dispatches++;
                errUNLOCK(&pool->mutex);
                logmsg(LOGMSG_ERROR, "%s(%s):pthread_create: %d %s\n", __func__,
                        pool->name, rc, strerror(rc));
                pthread_cond_destroy(&thd->cond);
                free(thd);
                return -1;
            }
            if (listc_size(&pool->thdlist) > pool->peaknthd) {
                pool->peaknthd = listc_size(&pool->thdlist);
            }
            pool->num_creates++;
        }

        if (thd == NULL && pool->wait) {
            int rc;

            pool->waiting_for_thread = 1;
            rc = pthread_cond_wait(&pool->wait_for_thread, &pool->mutex);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s:%d pthread_cond_wait rc %d %s\n", __FILE__,
                        __LINE__, rc, strerror(rc));
                pool->num_failed_dispatches++;
                return -1; /* not sure what happens to my mutex here */
            }

            pool->waiting_for_thread = 0;

            goto again;
        }

        if (thd) {
            item = &thd->work;
            thd->on_freelist = 0;
            pool->num_passed++;
        } else {
            /* queue work */
            if (listc_size(&pool->queue) >= pool->maxqueue) {
                if (queue_override &&
                    (!pool->maxqueueoverride ||
                     listc_size(&pool->queue) <
                         (pool->maxqueue + pool->maxqueueoverride))) {
                    if (thdpool_alarm_on_queing(listc_size(&pool->queue))) {
                        int now = comdb2_time_epoch();

                        if (now > pool->last_queue_alarm ||
                            listc_size(&pool->queue) > pool->last_alarm_max) {
                            logmsg(LOGMSG_USER, "%d Queing sql, queue size=%d. "
                                            "max_queue=%d "
                                            "max_queue_override=%d\n",
                                    __LINE__, listc_size(&pool->queue),
                                    pool->maxqueue, pool->maxqueueoverride);

                            pool->last_queue_alarm = now;
                            pool->last_alarm_max = listc_size(&pool->queue);
                        }
                    }
                } else {
                    if (queue_override) {
                        logmsg(LOGMSG_USER, "%d FAILED to queue sql, queue "
                                        "size=%d. max_queue=%d "
                                        "max_queue_override=%d\n",
                                __LINE__, listc_size(&pool->queue),
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
                                       (thd->work.persistent_info)
                                           ? thd->work.persistent_info
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
            pool->num_enqueued++;
            listc_abl(&pool->queue, item);

            if (listc_size(&pool->queue) > pool->peakqueue) {
                pool->peakqueue = listc_size(&pool->queue);
            }
        }

        item->work = work;
        item->work_fn = work_fn;
        item->persistent_info = persistent_info;
        item->queue_time_ms = comdb2_time_epochms();
        item->available = 1;

        /* Now wake up the thread with work to do. */
        if (!thd) {
            comdb2bma_yield_all();
        } else {
            comdb2bma_transfer_priority(blobmem, thd->tid);
            pthread_cond_signal(&thd->cond);
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
    return pthread_mutex_lock(&pool->mutex);
}

int thdpool_unlock(struct thdpool *pool)
{
    return pthread_mutex_unlock(&pool->mutex);
}

struct thdpool *thdpool_next_pool(struct thdpool *pool)
{
    return (pool) ? pool->lnk.next : 0;
}
