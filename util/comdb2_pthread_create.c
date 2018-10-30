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
#ifdef __sun
   /* for PTHREAD_STACK_MIN on Solaris */
#  define __EXTENSIONS__
#endif

#include <errno.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#ifndef __APPLE__
#include <malloc.h>
#endif
#include <sys/mman.h>

#include "comdb2_pthread_create.h"
#include "mem_util.h"
#include "list.h"
#include "logmsg.h"

#define RW (PROT_READ | PROT_WRITE)
#define STACK_FREE_DELAY 5
#define FREE_THR_INTV 5
#define MIN_MMAP_THRESH 128 * 1024

extern int db_is_stopped();

typedef struct thr_arg {
    void *(*func)(void *); /* actual pthread routine */
    void *arg;             /* argument for the pthread routine */
    void *memptr;          /* memory address */
    void *stack;           /* real stack address */
    size_t stacksz;        /* stack size */
    comdb2ma alloc;        /* allocator - for trimming */
    LINKC_T(struct thr_arg) lnk;
} thr_arg_t;

static size_t __page_size;
static pthread_key_t memptr;
static LISTC_T(struct thr_arg) stack_list;
static pthread_mutex_t pthr_mutex;
static size_t pthr_mmap_threshold;
static pthread_t free_thr;
static pthread_attr_t free_thr_attrs;
static pthread_once_t init_key_once = PTHREAD_ONCE_INIT;

/* free associated stack memory */
static void free_memptr(void *inarg)
{
    thr_arg_t *arg = (thr_arg_t *)inarg;
    if (pthread_mutex_lock(&pthr_mutex) == 0) {
        listc_abl(&stack_list, arg);
        pthread_mutex_unlock(&pthr_mutex);
    }
}

static void *free_stack_thr(void *unused)
{
    int rc;
    thr_arg_t *arg;
    int signal_count;
    size_t stacksz;

    /*
    ** [1] get # of elements on list.
    ** [2] if # == 0, sleep for a few seconds and goto [1]. otherwise go to [3].
    ** [3] sleep for a few seconds and free # elems from stack_list
    */

    while (!db_is_stopped()) {
        //![1]
        rc = pthread_mutex_lock(&pthr_mutex);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "%s:%d error %d upon pthread_mutex_lock.\n",
                    __func__, __LINE__, rc);
            abort();
        }
        signal_count = listc_size(&stack_list);
        rc = pthread_mutex_unlock(&pthr_mutex);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "%s:%d error %d upon pthread_mutex_unlock.\n",
                    __func__, __LINE__, rc);
            abort();
        }

        //![2]
        if (signal_count == 0) {
            sleep(FREE_THR_INTV);
            continue;
        }

        //![3]
        rc = sleep(STACK_FREE_DELAY);
        if (rc != 0) {
            logmsg(LOGMSG_INFO,
                   "%s:%d interrupted with rc %d while sleeping.\n", __func__,
                   __LINE__, rc);
            continue;
        }

        rc = pthread_mutex_lock(&pthr_mutex);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "%s:%d error %d upon pthread_mutex_lock.\n",
                    __func__, __LINE__, rc);
            abort();
        }

        //![4]
        while (signal_count != 0) {
            arg = listc_rtl(&stack_list);
            if (arg == NULL)
                logmsg(LOGMSG_FATAL, 
                        "%s:%d WARNING stack_list corrupted. could be a bug.\n",
                        __func__, __LINE__);
            else {
                stacksz = arg->stacksz;
                comdb2_free(arg->memptr);
                comdb2_free(arg);
                // calling trim() at this point should always succeed
                comdb2_malloc_trim(arg->alloc, 0);
#ifdef M_MMAP_THRESHOLD
                if (comdb2ma_niceness() && /* Be nice. */
                    comdb2ma_mmap_threshold() == 0 && /* no user mmap thresh */
                    stacksz > MIN_MMAP_THRESH &&
                    stacksz > pthr_mmap_threshold) {
                    /*
                    * Glibc, by default, dynamically determines
                    * MMAP_THRESHOLD. If MONITOR_STACK is enabled
                    * and MMAP_THRESHOLD becomes larger than
                    * pthread stack size, we may end up having
                    * a pthread stack which is sitting on heap.
                    * In this case, the pthread stack
                    * can't be released back to system.
                    * Therefore MMAP_THRESHOLD is set to a fixed value
                    * (4MB) to have glibc use mmap() for
                    * pthread stack allocation so that the stack can be
                    * independently released back to system.
                    * This setting is not required for correctness but for
                    * saving memory.
                    */
                    mallopt(M_MMAP_THRESHOLD, stacksz);
                    pthr_mmap_threshold = stacksz;
                }
#endif /* MONITOR_STACK && M_MMAP_THRESHOLD */
            }
            --signal_count;
        }
        rc = pthread_mutex_unlock(&pthr_mutex);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "%s:%d error %d upon pthread_mutex_unlock.\n",
                    __func__, __LINE__, rc);
            abort();
        }
    }

    /* should never reach this point */
    return (void *)0;
}

/* initialize memptr key */
static void init_memptr_key(void)
{
    (void)pthread_key_create(&memptr, free_memptr);
    __page_size = sysconf(_SC_PAGESIZE);

    if (pthread_mutex_init(&pthr_mutex, NULL) != 0) {
        logmsg(LOGMSG_FATAL, "%s:%d failed to init pthread mutex.\n", __func__,
                __LINE__);
        abort();
    }

    if (pthread_attr_init(&free_thr_attrs) != 0) {
        logmsg(LOGMSG_FATAL, "%s:%d failed to init pthread attrs.\n", __func__,
                __LINE__);
        abort();
    }

#ifdef PTHREAD_STACK_MIN
    if (pthread_attr_setstacksize(&free_thr_attrs,
                                  (PTHREAD_STACK_MIN + 0x4000)) != 0) {
        logmsg(LOGMSG_FATAL, "%s:%d failed to set thr stack size.\n", __func__,
                __LINE__);
        abort();
    }
#endif

    if (pthread_attr_setdetachstate(&free_thr_attrs, PTHREAD_CREATE_DETACHED) !=
        0) {
        logmsg(LOGMSG_FATAL, "%s:%d failed to set detach state.\n", __func__,
                __LINE__);
        abort();
    }

    if (pthread_create(&free_thr, &free_thr_attrs, free_stack_thr, NULL) != 0) {
        logmsg(LOGMSG_FATAL, "%s:%d failed to create stack free thread.\n", __func__,
                __LINE__);
        abort();
    }

    listc_init(&stack_list, offsetof(struct thr_arg, lnk));
}

/*
** pthread routine wrapper.
** set per-pthread base memory and execute the actual routine.
*/
static void *thr_func(void *arg)
{
    void *ret;
    thr_arg_t *thr_arg = (thr_arg_t *)arg;

#if (defined(_LINUX_SOURCE) || defined(_SUN_SOURCE))
    void *stack = thr_arg->stack;
    size_t stacksz = thr_arg->stacksz;
#endif

    pthread_setspecific(memptr, thr_arg);

    ret = thr_arg->func(thr_arg->arg);

#if (defined(_LINUX_SOURCE) || defined(_SUN_SOURCE))
    if (mprotect((void *)((char *)stack - __page_size),
        __page_size, RW) == -1) {
        logmsg(LOGMSG_FATAL, 
                "failed to restore thread stack guard page access: %d\n",
                errno);
        abort();
    }

    if (mprotect((void *)((char *)stack + stacksz),
        __page_size, RW) == -1) {
        logmsg(LOGMSG_FATAL, 
                "failed to restore thread stack guard page access: %d\n",
                errno);
        abort();
    }
#endif
    return ret;
}

/*
** thread stack is protected by 2 guard pages.
** one is right below lowest stack address. the other is above highest
** stack address (POSIX doesn't specify the direction of thread stack growth).
*/
int comdb2_pthread_create(pthread_t *thread, pthread_attr_t *attr,
                          void *(*start_routine)(void *), void *targ,
                          comdb2ma allocator, size_t stacksz)
{
    int rc;
    thr_arg_t *arg;
    char *stack;

    rc = pthread_once(&init_key_once, init_memptr_key);

    if (rc != 0)
        return rc;

    arg = comdb2_malloc(allocator, sizeof(thr_arg_t));
    if (arg == NULL)
        return ENOMEM;

    /* make stack size a multiple of pagesize */
    stacksz =
        (size_t)(((uint64_t)stacksz + __page_size - 1) & ~(__page_size - 1));

#if (defined(_LINUX_SOURCE) || defined(_SUN_SOURCE))
    arg->memptr = comdb2_malloc(allocator, (__page_size * 3) + stacksz);
#else
    arg->memptr = comdb2_malloc(allocator, __page_size + stacksz);
#endif
    if (arg->memptr == NULL) {
        comdb2_free(arg);
        return ENOMEM;
    }

    /* align to page boundary */
    stack = (char *)(((uintptr_t)arg->memptr + __page_size - 1) &
                     ~(__page_size - 1));

/*
** Linux allows mprotect calls on any address in address space.
** However ibm only permits mprotect on address obtained by a mmap/shmat call.
** Leave ibm for now till I come up with a solution.
*/
#if (defined(_LINUX_SOURCE) || defined(_SUN_SOURCE))

    arg->stack = (void *)(stack + __page_size);

    if (mprotect((void *)stack, __page_size, PROT_NONE) == -1) {
        logmsg(LOGMSG_FATAL, "failed to mprotect thread stack guard page.\n");
        abort();
    }

    if (mprotect((void *)(stack + __page_size + stacksz), __page_size,
                 PROT_NONE) == -1) {
        logmsg(LOGMSG_FATAL, "failed to mprotect thread stack guard page.\n");
        abort();
    }

#elif defined(_IBM_SOURCE)
    /*
     * POSIX states that sockaddr should point to the lowest
     * address regardless of stack growth direction.
     * However, it isn't honoured by libpthreads on AIX.
     */
    arg->stack = (void *)(stack + stacksz);
#endif

    arg->func = start_routine;
    arg->arg = targ;
    arg->stacksz = stacksz;
    arg->alloc = allocator;

    rc = pthread_attr_setstack(attr, arg->stack, stacksz);

    if (rc != 0) {
        comdb2_free(arg->memptr);
        comdb2_free(arg);
        return rc;
    }

    rc = pthread_create(thread, attr, thr_func, arg);
    if (rc != 0) {
        comdb2_free(arg->memptr);
        comdb2_free(arg);
    }

    return rc;
}
