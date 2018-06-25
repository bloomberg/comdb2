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

/* The bdb lock module. */

#include <poll.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <pthread.h>
#include <unistd.h>

#include <list.h>
#include <walkback.h>

#include "bdb_int.h"
#include "locks.h"
#include "logmsg.h"

/* XXX stupid chicken/egg.  this variable cannot live in the bdb_state
   cause it needs to get set before we have a bdb_state */
extern bdb_state_type *gbl_bdb_state;
extern int gbl_rowlocks;
extern pthread_t gbl_invalid_tid;

int gbl_bdblock_debug = 0;

void comdb2_cheap_stack_trace_file(FILE *f);

/* Defined in tran.c */
int bdb_abort_logical_waiters(bdb_state_type *bdb_state);

enum bdb_lock_type { NOLOCK = 0, READLOCK = 1, WRITELOCK = 2 };

static int bdb_bdblock_debug_init_called = 0;

void bdb_bdblock_debug_init(bdb_state_type *bdb_state)
{
    char buf[80];
    int rc;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    sprintf(buf, "%s.lock.log", bdb_state->name);
    unlink(buf);
    bdb_state->bdblock_debug_fp = fopen(buf, "w");
    setvbuf(bdb_state->bdblock_debug_fp, 0, _IOLBF, 0);

    rc = pthread_mutex_init(&(bdb_state->bdblock_debug_lock), NULL);
    logmsg(LOGMSG_USER, "initialized bdblock debug\n");

    bdb_bdblock_debug_init_called = 1;
}

void bdb_bdblock_print(bdb_state_type *bdb_state, char *str)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (!bdb_bdblock_debug_init_called)
        return;

    Pthread_mutex_lock(&(bdb_state->bdblock_debug_lock));

    logmsg(LOGMSG_USER, "%s\n", str);

    Pthread_mutex_unlock(&(bdb_state->bdblock_debug_lock));
}

static void get_read_lock_log(bdb_state_type *bdb_state)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (!bdb_bdblock_debug_init_called)
        return;

    Pthread_mutex_lock(&(bdb_state->bdblock_debug_lock));

    logmsgf(LOGMSG_USER, bdb_state->bdblock_debug_fp,
            "GOT THE READ LOCK AT BDB_STATE=%p THREAD=%lu: ", bdb_state,
            pthread_self());
    comdb2_cheap_stack_trace_file(bdb_state->bdblock_debug_fp);

    Pthread_mutex_unlock(&(bdb_state->bdblock_debug_lock));
}

static void get_read_lock_ref_log(bdb_state_type *bdb_state, int ref)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (!bdb_bdblock_debug_init_called)
        return;

    Pthread_mutex_lock(&(bdb_state->bdblock_debug_lock));

    logmsgf(LOGMSG_USER, bdb_state->bdblock_debug_fp,
            "INCREMENTING READ LOCK TO %d AT BDB_STATE=%p THREAD=%lu: ", ref,
            bdb_state, pthread_self());
    comdb2_cheap_stack_trace_file(bdb_state->bdblock_debug_fp);

    Pthread_mutex_unlock(&(bdb_state->bdblock_debug_lock));
}

static void get_write_lock_log(bdb_state_type *bdb_state)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (!bdb_bdblock_debug_init_called)
        return;

    Pthread_mutex_lock(&(bdb_state->bdblock_debug_lock));

    logmsgf(LOGMSG_USER, bdb_state->bdblock_debug_fp,
            "GOT THE WRITE LOCK AT BDB_STATE=%p THREAD=%lu: ", bdb_state,
            pthread_self());
    comdb2_cheap_stack_trace_file(bdb_state->bdblock_debug_fp);

    Pthread_mutex_unlock(&(bdb_state->bdblock_debug_lock));
}

static void get_write_lock_try_log(bdb_state_type *bdb_state)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (!bdb_bdblock_debug_init_called)
        return;

    Pthread_mutex_lock(&(bdb_state->bdblock_debug_lock));

    logmsgf(LOGMSG_USER, bdb_state->bdblock_debug_fp,
            "TRY GET THE WRITE LOCK AT BDB_STATE=%p THREAD=%lu: ", bdb_state,
            pthread_self());
    comdb2_cheap_stack_trace_file(bdb_state->bdblock_debug_fp);

    Pthread_mutex_unlock(&(bdb_state->bdblock_debug_lock));
}

static void rel_lock_log(bdb_state_type *bdb_state)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (!bdb_bdblock_debug_init_called)
        return;

    Pthread_mutex_lock(&(bdb_state->bdblock_debug_lock));

    logmsgf(LOGMSG_USER, bdb_state->bdblock_debug_fp,
            "RELEASED THE LOCK AT BDB_STATE=%p THREAD=%lu: ", bdb_state,
            pthread_self());
    comdb2_cheap_stack_trace_file(bdb_state->bdblock_debug_fp);

    Pthread_mutex_unlock(&(bdb_state->bdblock_debug_lock));
}

static void rel_lock_ref_log(bdb_state_type *bdb_state, int ref)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (!bdb_bdblock_debug_init_called)
        return;

    Pthread_mutex_lock(&(bdb_state->bdblock_debug_lock));

    logmsgf(LOGMSG_USER, bdb_state->bdblock_debug_fp,
            "DECREMENTING LOCK TO %d AT BDB_STATE=%p THREAD=%lu: ", ref,
            bdb_state, pthread_self());
    comdb2_cheap_stack_trace_file(bdb_state->bdblock_debug_fp);

    Pthread_mutex_unlock(&(bdb_state->bdblock_debug_lock));
}

#define BDB_DEBUG_STACK 32

struct thread_lock_info_tag {
    pthread_t threadid; /* which thread this is for */

    unsigned lockref;            /* how many people have this lock already */
    const char *ident;           /* who in this thread locked it */
    enum bdb_lock_type locktype; /* type of lock currently held */

    /* If we hold the write lock, this records whether or not we previously
     * held the read lock.  If this is non-zero then when we release the
     * write lock we must reacquire the read lock with the same reference
     * count. */
    unsigned readlockref;
    const char *readident;

    int line;

#ifdef _SUN_SOURCE
    /* set this if a calling thread's priority has not been set */
    int initpri;

    /* set if this thread lowered it's priority before getting a writelock */
    int loweredpri;
#endif

    /* Keep all these lock states chained together from the parent
     * bdb_state.  This can be useful when debugging core files. */
    LINKC_T(thread_lock_info_type) linkv;

    void **stack;
    unsigned int nstack;
};

void abort_lk(thread_lock_info_type *lk)
{
    if (lk && lk->stack && lk->nstack > 0)
        cheap_stack_trace();
    abort();
}

int bdb_the_lock_desired(void) { return gbl_bdb_state->bdb_lock_desired; }

int bdb_lock_desired(bdb_state_type *bdb_state)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    return bdb_state->bdb_lock_desired;
}

void bdb_lock_init(bdb_state_type *bdb_state)
{
    listc_init(&bdb_state->thread_lock_info_list,
               offsetof(thread_lock_info_type, linkv));
    pthread_mutex_init(&bdb_state->thread_lock_info_list_mutex, NULL);
}

void bdb_lock_destructor(void *ptr)
{
    thread_lock_info_type *lk = ptr;
    if (lk) {
        logmsg(LOGMSG_ERROR, 
            "%s: thread has not called bdb_thread_event to destroy locks!\n",
            __func__);

        if (lk->lockref != 0) {
            logmsg(LOGMSG_FATAL, "%s: exiting thread holding lock!\n", __func__);
            abort_lk(lk);
        }
        if (lk->stack && lk->nstack > 0)
            cheap_stack_trace();

        if (lk->stack)
            free(lk->stack);
        free(lk);
    }
}

static const char *locktype2str(enum bdb_lock_type locktype)
{
    switch (locktype) {
    case NOLOCK:
        return "NOLOCK";
    case READLOCK:
        return "READLOCK";
    case WRITELOCK:
        return "WRITELOCK";
    default:
        return "???";
    }
}

#ifdef _SUN_SOURCE

/* On Sun if a wrlock is held by one or more reader-threads and a writer thread
 * attempts to attain the lock then the writer blocks until the current readers
 * release the lock, and new readers of equal or lower priority are enqueued
 * behind the writer.  Rarely, this can cause deadlocks (i.e. between a
 * osql_pblog_commit thread which has the lock & a local worker which is
 * enqueued behind the schemachange thread).  A workaround is to lower the
 * priority of the writer-threads before grabbing the writelock. */

static int init_thd_priority(void)
{
    return pthread_setschedprio(pthread_self(), 10);
}

static int lower_thd_priority(void)
{
    int prio, policy, rc;
    struct sched_param sparam = {0};

    rc = pthread_getschedparam(pthread_self(), &policy, &sparam);
    if (0 != rc) {
        logmsg(LOGMSG_ERROR, "%s:pthread_getschedparam returns %d for thd %d\n",
                __func__, rc, pthread_self());
        return rc;
    }
    prio = (sparam.sched_priority - 1);
    rc = pthread_setschedprio(pthread_self(), prio);
    if (0 != rc) {
        logmsg(LOGMSG_ERROR, "%s:pthread_setschedprio returns %d for thd %d\n",
                __func__, rc, pthread_self());
        return rc;
    }
    return 0;
}

static int raise_thd_priority(void)
{
    int prio, policy, rc;
    struct sched_param sparam = {0};

    rc = pthread_getschedparam(pthread_self(), &policy, &sparam);
    if (0 != rc) {
        logmsg(LOGMSG_ERROR, "%s:pthread_getschedparam returns %d for thd %d\n",
                __func__, rc, pthread_self());
        return rc;
    }
    prio = (sparam.sched_priority + 1);
    rc = pthread_setschedprio(pthread_self(), prio);
    if (0 != rc) {
        logmsg(LOGMSG_ERROR, "%s:pthread_setschedprio returns %d for thd %d\n",
                __func__, rc, pthread_self());
        return rc;
    }
    return 0;
}

#endif

int __rep_block_on_inflight_transactions(DB_ENV *dbenv);

int gbl_force_serial_on_writelock = 1;

/* Acquire the write lock.  If the current thread already holds the bdb read
 * lock then it is upgraded to a write lock.  If it already holds the write
 * lock then we just increase our reference count. */
static inline void bdb_get_writelock_int(bdb_state_type *bdb_state,
                                         const char *idstr,
                                         const char *funcname, int line,
                                         int abort_waiters)
{
    thread_lock_info_type *lk = pthread_getspecific(lock_key);
    static pthread_mutex_t lk_desired_lock = PTHREAD_MUTEX_INITIALIZER;
    bdb_state_type *lock_handle = bdb_state;
    int rc;

    if (lock_handle->parent)
        lock_handle = lock_handle->parent;

    if (lk == NULL) {
        logmsg(LOGMSG_FATAL, "%s/%s(%s): bdb lock not inited in this thread\n",
                idstr, funcname, __func__);
        abort();
    }

    /* If we're upgrading from read lock to write lock, remember the
     * ref count for our old read lock. */
    if (lk->locktype == READLOCK && lk->lockref > 0) {
        /*
           printf("Thread %d upgrading read lock '%s' cnt %u to write lock
           '%s'\n",
                 (int)lk->threadid, lk->ident ? lk->ident : "",
                 lk->lockref,
                 idstr ? idstr : "");
           */
        lk->readlockref = lk->lockref;
        lk->readident = lk->ident;
        rc = pthread_rwlock_unlock(lock_handle->bdb_lock);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s(%s): pthread_rwlock_unlock error %d %s\n",
                    funcname, __func__, rc, strerror(rc));
            abort();
        }

        if (gbl_bdblock_debug)
            rel_lock_log(bdb_state);

        lk->locktype = NOLOCK;
        lk->ident = NULL;
        lk->lockref = 0;
    }

    if (lk->lockref == 0) {
        pthread_mutex_lock(&lk_desired_lock);
        lock_handle->bdb_lock_desired++;
        pthread_mutex_unlock(&lk_desired_lock);

        if (gbl_bdblock_debug)
            get_write_lock_try_log(bdb_state);

#ifdef _SUN_SOURCE
        if (!lk->initpri) {
            init_thd_priority();
            lk->initpri = 1;
        }

        rc = lower_thd_priority();
        if (0 == rc)
            lk->loweredpri = 1;
#endif

        rc = pthread_rwlock_trywrlock(lock_handle->bdb_lock);
        if (rc == EBUSY) {
            logmsg(LOGMSG_ERROR,
                   "trying writelock (%s %lu), last writelock is %s %lu\n",
                   idstr, pthread_self(), lock_handle->bdb_lock_write_idstr,
                   lock_handle->bdb_lock_write_holder);

            /*
             * Abort threads waiting on logical locks.
             * This only looks racy: while bdb_lock_desired is set, the lock
             * code
             * returns 'deadlock' for any thread attempting to get a rowlock.
             */
            if (gbl_rowlocks &&
                lock_handle->repinfo->master_host !=
                    lock_handle->repinfo->myhost &&
                abort_waiters) {
                bdb_abort_logical_waiters(lock_handle);
            }

            rc = pthread_rwlock_wrlock(lock_handle->bdb_lock);
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, 
                        "%s/%s(%s): pthread_rwlock_wrlock error %d %s\n", idstr,
                        funcname, __func__, rc, strerror(rc));
                abort();
            }
        } else if (rc != 0) {
            logmsg(LOGMSG_FATAL, "%s/%s(%s): pthread_rwlock_wrlock error %d %s\n",
                    idstr, funcname, __func__, rc, strerror(rc));
            abort();
        }

        /* Wait on rep_processor threads while we have the writelock lock */
        if (gbl_force_serial_on_writelock)
            __rep_block_on_inflight_transactions(lock_handle->dbenv);

        strcpy(lock_handle->bdb_lock_write_idstr, idstr);

        if (gbl_bdblock_debug)
            get_write_lock_log(bdb_state);

        pthread_mutex_lock(&lk_desired_lock);
        lock_handle->bdb_lock_desired--;
        pthread_mutex_unlock(&lk_desired_lock);

        lock_handle->bdb_lock_write_holder = pthread_self();
        lock_handle->bdb_lock_write_holder_ptr = lk;
        lk->locktype = WRITELOCK;
        lk->ident = (idstr);
    }
    if (lk->lockref == 0) {
        lk->line = line;
        if (lk->stack) {
            rc =
                stack_pc_getlist(NULL, lk->stack, BDB_DEBUG_STACK, &lk->nstack);
            if (rc) {
/* stack_pc_getlist isn't implemented yet on linux */
#ifndef _LINUX_SOURCE
                logmsg(LOGMSG_WARN, "%s: failed to get stack %d\n", __func__, rc);
#endif
                lk->nstack = 0;
            }
        }
    }
    lk->lockref++;
}

/* This flavor of get_writelock should be called from anything which cannot
 * be holding locks */
void bdb_get_writelock(bdb_state_type *bdb_state, const char *idstr,
                       const char *funcname, int line)
{
    bdb_get_writelock_int(bdb_state, idstr, funcname, line, 0);
}

/* This flavor of get_writelock should be called from anything which cannot
 * be holding rowlocks */
void bdb_get_writelock_abort_waiters(bdb_state_type *bdb_state,
                                     const char *idstr, const char *funcname,
                                     int line)
{
    bdb_get_writelock_int(bdb_state, idstr, funcname, line, 1);
}

/* Acquire the read lock.  Multiple threads can hold the read lock
 * simultaneously.  If a thread acquires the read lock twice it is reference
 * counted.  If a thread that holds the write lock calls this then it
 * continues to hold the write lock but with a higher reference count. */
void bdb_get_readlock(bdb_state_type *bdb_state, const char *idstr,
                      const char *funcname, int line)
{
    thread_lock_info_type *lk = pthread_getspecific(lock_key);
    bdb_state_type *lock_handle = bdb_state;
    int rc;
    int cnt;

    if (lock_handle->parent)
        lock_handle = lock_handle->parent;

    if (lk == NULL) {
        logmsg(LOGMSG_FATAL, "%s/%s(%s): bdb lock not inited in this thread\n",
                idstr, funcname, __func__);
        abort();
    }

    if (lk->lockref == 0) {
#ifdef _SUN_SOURCE
        if (!lk->initpri) {
            init_thd_priority();
            lk->initpri = 1;
        }
#endif

        rc = pthread_rwlock_tryrdlock(lock_handle->bdb_lock);
        if (rc == EBUSY) {
            logmsg(LOGMSG_INFO,
                   "trying readlock (%s %lu), last writelock is %s %lu\n",
                   idstr, pthread_self(), lock_handle->bdb_lock_write_idstr,
                   lock_handle->bdb_lock_write_holder);

            rc = pthread_rwlock_rdlock(lock_handle->bdb_lock);
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, 
                        "%s/%s(%s): pthread_rwlock_rdlock error %d %s\n", idstr,
                        funcname, __func__, rc, strerror(rc));
                abort();
            }
        } else if (rc != 0) {
            logmsg(LOGMSG_FATAL, "%s/%s(%s): pthread_rwlock_rdlock error %d %s\n",
                    idstr, funcname, __func__, rc, strerror(rc));
            abort();
        }

        if (gbl_bdblock_debug)
            get_read_lock_log(bdb_state);

        lk->locktype = READLOCK;
        lk->ident = (idstr);
    } else {
        if (gbl_bdblock_debug)
            get_read_lock_ref_log(bdb_state, lk->lockref);
    }

    /*printf("get [%2d] %*s%s %s %d\n", lk->lockref, lk->lockref*2, "",
     * funcname, lk->ident, line); */
    if (lk->lockref == 0) {
        lk->line = line;
        if (lk->stack) {
            rc =
                stack_pc_getlist(NULL, lk->stack, BDB_DEBUG_STACK, &lk->nstack);
            if (rc) {
#ifndef _LINUX_SOURCE
                logmsg(LOGMSG_INFO, "%s: failed to get stack %d\n", __func__, rc);
#endif
                lk->nstack = 0;
            }
        }
    }
    lk->lockref++;
}

void bdb_get_the_readlock(const char *idstr, const char *function, int line)
{
    bdb_get_readlock(gbl_bdb_state, idstr, function, line);
}

/* Release the lock of either type (decrements reference count, releases
 * actual lock if reference count hits zero). */
void bdb_rellock(bdb_state_type *bdb_state, const char *funcname, int line)
{
    thread_lock_info_type *lk = pthread_getspecific(lock_key);
    bdb_state_type *lock_handle = bdb_state;
    int rc;

    /*printf("rel [%2d] %*s%s %s %d\n", lk->lockref, (lk->lockref-1)*2, "",
     * funcname, lk->ident, line);*/

    if (lock_handle->parent)
        lock_handle = lock_handle->parent;

    if (lk == NULL) {
        logmsg(LOGMSG_FATAL, "%s(%s): bdb lock not inited in this thread\n",
                funcname, __func__);
        abort();
    }
    if (lk->lockref == 0) {
        logmsg(LOGMSG_FATAL, "%s(%s): I do not hold the lock!\n", funcname,
                __func__);
        abort_lk(lk);
    }
    lk->lockref--;
    if (lk->lockref == 0) {
        if (lk->locktype == WRITELOCK) {
            lock_handle->bdb_lock_write_holder_ptr = NULL;
            lock_handle->bdb_lock_write_holder = 0;
        }

        rc = pthread_rwlock_unlock(lock_handle->bdb_lock);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "%s(%s): pthread_rwlock_unlock error %d %s\n",
                    funcname, __func__, rc, strerror(rc));
            abort();
        }

        if (gbl_bdblock_debug)
            rel_lock_log(bdb_state);

#ifdef _SUN_SOURCE
        if (lk->locktype == WRITELOCK && lk->loweredpri) {
            rc = raise_thd_priority();
            if (0 == rc)
                lk->loweredpri = 0;
        }
#endif

        lk->locktype = NOLOCK;
        lk->ident = NULL;

        /* If we previously held the read lock before we acquired the write
         * lock, reacquire the read lock. */
        if (lk->readlockref > 0) {
            /*
               printf("Thread %d reacquiring read lock '%s' with refcount %u\n",
                     (int)lk->threadid, lk->readident ? lk->readident : "",
                     lk->readlockref);
               */
            while (lk->readlockref > 0) {
                bdb_get_readlock(bdb_state, lk->readident, funcname, line);
                lk->readlockref--;
            }
            lk->readident = NULL;
        }
    } else {
        if (gbl_bdblock_debug)
            rel_lock_ref_log(bdb_state, lk->lockref);
    }
}

void bdb_relthelock(const char *funcname, int line)
{
    bdb_rellock(gbl_bdb_state, funcname, line);
}

/* Check that all the locks are released; this ensures that no
 * bdb lock is lost after a request; thread might be reused and
 * trigger an abort much later
 * Returns -1 if locks are still present
 */
void bdb_checklock(bdb_state_type *bdb_state)
{
    thread_lock_info_type *lk = pthread_getspecific(lock_key);
    bdb_state_type *lock_handle = bdb_state;
    int rc;

    if (lock_handle->parent)
        lock_handle = lock_handle->parent;

    if (lk == NULL || lk->lockref == 0)
        return; /* all good */

    logmsg(LOGMSG_FATAL,
           "%lu %s: request terminated but thread is holding bdb lock!\n",
           pthread_self(), __func__);
    logmsg(LOGMSG_FATAL, "%lu %s: %s %s lockref=%u\n", pthread_self(), __func__,
           locktype2str(lk->locktype), lk->ident ? lk->ident : "?",
           lk->lockref);
    abort_lk(lk);
}

static int get_threadid(bdb_state_type *bdb_state)
{
    int i;
    int highest;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    highest = 16;

    if (!bdb_state->attr->dtastripe)
        return 0;

    if (bdb_state->attr->dtastripe == 1)
        return 0;

    Pthread_mutex_lock(&(bdb_state->numthreads_lock));

    for (i = bdb_state->stripe_pool_start; i < bdb_state->attr->dtastripe;
         i++) {
        if (bdb_state->stripe_pool[i] > bdb_state->stripe_pool[highest])
            highest = i;
    }

    for (i = 0; i < bdb_state->stripe_pool_start; i++) {
        if (bdb_state->stripe_pool[i] > bdb_state->stripe_pool[highest])
            highest = i;
    }

    bdb_state->stripe_pool_start++;
    if (bdb_state->stripe_pool_start == bdb_state->attr->dtastripe)
        bdb_state->stripe_pool_start = 0;

    bdb_state->stripe_pool[highest]--;

    Pthread_mutex_unlock(&(bdb_state->numthreads_lock));

    if ((highest < 0) || (highest >= bdb_state->attr->dtastripe))
        logmsg(LOGMSG_WARN, "get_threadid: highest = %d\n", highest);

    return highest;
}

static void return_threadid(bdb_state_type *bdb_state, int id)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (!bdb_state->attr->dtastripe)
        return;

    if (bdb_state->attr->dtastripe == 1)
        return;

    Pthread_mutex_lock(&(bdb_state->numthreads_lock));

    bdb_state->stripe_pool[id]++;

    Pthread_mutex_unlock(&(bdb_state->numthreads_lock));
}

/* Allocate a new lock info struct for the current thread and associate it
 * with our thread specific key.  Contains sanity checks to ensure this
 * hasn't already been done. */
static void new_thread_lock_info(bdb_state_type *bdb_state)
{
    thread_lock_info_type *lk = pthread_getspecific(lock_key);
    int rc = 0;

    if (lk) {
        logmsg(LOGMSG_WARN, "%s: redundant thread start!\n", __func__);
        return;
    }

    lk = calloc(1, sizeof(thread_lock_info_type));
    if (!lk) {
        logmsg(LOGMSG_FATAL, "%s: out of memory\n", __func__);
        exit(1);
    }

    lk->threadid = pthread_self();

    if (bdb_state->attr && bdb_state->attr->debug_bdb_lock_stack) {
        lk->stack = (void **)malloc(sizeof(void *) * BDB_DEBUG_STACK);
        if (!lk->stack) {
            logmsg(LOGMSG_FATAL, "%s: out of memory\n", __func__);
            free(lk);
            exit(1);
        }
        rc = stack_pc_getlist(NULL, lk->stack, BDB_DEBUG_STACK, &lk->nstack);
        if (rc) {
#ifndef _LINUX_SOURCE
            logmsg(LOGMSG_WARN, "%s: failed to get stack %d\n", __func__, rc);
#endif
            free(lk->stack);
            lk->stack = NULL;
        }
    }

    pthread_setspecific(lock_key, lk);

    Pthread_mutex_lock(&bdb_state->thread_lock_info_list_mutex);
    listc_atl(&bdb_state->thread_lock_info_list, lk);
    Pthread_mutex_unlock(&bdb_state->thread_lock_info_list_mutex);
}

/* Free the current thread's lock info struct. */
static void delete_thread_lock_info(bdb_state_type *bdb_state)
{
    thread_lock_info_type *lk = pthread_getspecific(lock_key);
    if (!lk) {
        logmsg(LOGMSG_WARN, "%s: thread stop before init!!\n", __func__);
        return;
    }

    if (lk->lockref != 0) {
        logmsg(LOGMSG_FATAL, "%s: exiting thread holding lock!\n", __func__);
        logmsg(LOGMSG_FATAL, "%s: %s %s lockref=%u\n", __func__,
               locktype2str(lk->locktype), lk->ident ? lk->ident : "?",
               lk->lockref);
        abort_lk(lk);
    }

    Pthread_mutex_lock(&bdb_state->thread_lock_info_list_mutex);
    listc_rfl(&bdb_state->thread_lock_info_list, lk);
    Pthread_mutex_unlock(&bdb_state->thread_lock_info_list_mutex);

    if (lk->stack)
        free(lk->stack);
    free(lk);
    pthread_setspecific(lock_key, NULL);
}

void bdb_stripe_get(bdb_state_type *bdb_state)
{
    size_t id;
    bdb_state_type *parent;

    if (bdb_state->parent)
        parent = bdb_state->parent;
    else
        parent = bdb_state;

    id = get_threadid(parent);

    pthread_setspecific(parent->tid_key, (void *)id);
}

void bdb_stripe_done(bdb_state_type *bdb_state)
{
    size_t id;
    bdb_state_type *parent;

    if (bdb_state->parent)
        parent = bdb_state->parent;
    else
        parent = bdb_state;

    id = (size_t)pthread_getspecific(parent->tid_key);

    return_threadid(parent, id);
}

void bdb_thread_event(bdb_state_type *bdb_state, int event)
{
    bdb_state_type *parent;

    if (bdb_state->parent)
        parent = bdb_state->parent;
    else
        parent = bdb_state;

    switch (event) {
    case BDBTHR_EVENT_DONE_RDONLY: /* thread done */
        delete_thread_lock_info(parent);
        break;

    case BDBTHR_EVENT_START_RDONLY: /* thread start */
        new_thread_lock_info(parent);
        break;

    case BDBTHR_EVENT_DONE_RDWR: /* thread done */
        delete_thread_lock_info(parent);
        break;

    case BDBTHR_EVENT_START_RDWR: /* thread start */
        new_thread_lock_info(parent);
        break;

    default:
        logmsg(LOGMSG_ERROR, "bdb_thread_event: event=%d?!\n", event);
        break;
    }
}

void bdb_thread_done_rw(void)
{
    bdb_thread_event(gbl_bdb_state, BDBTHR_EVENT_DONE_RDWR);
}

void bdb_thread_start_rw(void)
{
    bdb_thread_event(gbl_bdb_state, BDBTHR_EVENT_START_RDWR);
}

static void dump_int(thread_lock_info_type *lk, FILE *out)
{

#ifdef _LINUX_SOURCE
    logmsgf(LOGMSG_USER, out, "thr %lu (0x%lx)  lk %9s %u", lk->threadid,
            lk->threadid, locktype2str(lk->locktype), lk->lockref);
#else
    logmsgf(LOGMSG_USER, out, "thr %u (0x%x)  lk %9s %u", (int)lk->threadid,
            (int)lk->threadid, locktype2str(lk->locktype), lk->lockref);
#endif
    if (lk->lockref > 0) {
        logmsgf(LOGMSG_USER, out, " locker:'%s'", lk->ident ? lk->ident : "?");
        if (lk->readlockref) {
            logmsgf(LOGMSG_USER, out, " [upgr from readlock '%s' %u]",
                    lk->readident ? lk->readident : "?", lk->readlockref);
        }
    }
    logmsgf(LOGMSG_USER, out, "\n");
}

void bdb_dump_my_lock_state(FILE *out)
{
    thread_lock_info_type *lk = pthread_getspecific(lock_key);
    if (!lk)
        logmsgf(LOGMSG_USER, out, "no lock state defined\n");
    else
        dump_int(lk, out);
}

void bdb_locks_dump(bdb_state_type *bdb_state, FILE *out)
{
    thread_lock_info_type *lk;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    Pthread_mutex_lock(&bdb_state->thread_lock_info_list_mutex);

    LISTC_FOR_EACH(&bdb_state->thread_lock_info_list, lk, linkv)
    {
        dump_int(lk, out);
    }

    Pthread_mutex_unlock(&bdb_state->thread_lock_info_list_mutex);
}
