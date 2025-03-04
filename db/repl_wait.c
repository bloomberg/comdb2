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
 * This module maintains a list of genids that are associated with inprogress
 * transactions, and allows you to block until a given genid is no longer
 * "in prgress" (i.e. replicating).
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include <plhash_glue.h>
#include <pool.h>
#include "lockmacros.h"
#include "repl_wait.h"
#include "switches.h"
#include "logmsg.h"
#include <sys_wrap.h>

#include <mem_uncategorized.h>
#include <mem_override.h>

static pool_t *pool;
static hash_t *hash;
static pthread_mutex_t lock;

static unsigned n_allocs = 0;
static unsigned n_deallocs = 0;
static unsigned n_blocks = 0;

static int enabled = 1;

struct waiter {
    pthread_mutex_t mutex;
    struct waiter *next;
};

/* A replication object that we might want to wait for. */
struct repl_object {

    /* The genid that identifies it.  Must be first thing in the struct
     * as this is also our hash table key. */
    unsigned long long genid;

    /* Singly linked list of threads waiting for this to be committed */
    struct waiter *waiter_list;

    /* Next node in singly linked list of objects that form part of
     * this transaction. */
    struct repl_object *next;
};

void repl_list_init(void)
{
    pool = pool_setalloc_init(sizeof(struct repl_object), 0, malloc, free);
    hash = hash_init(sizeof(unsigned long long));
    Pthread_mutex_init(&lock, NULL);
    register_int_switch("repl_wait",
                        "Replication wait system enabled for queues", &enabled);
}

void repl_wait_stats(void)
{
    logmsg(LOGMSG_USER, "Replication wait stats\n");
    logmsg(LOGMSG_USER, "n_allocs   = %8u\n", n_allocs);
    logmsg(LOGMSG_USER, "n_deallocs = %8u\n", n_deallocs);
    logmsg(LOGMSG_USER, "n_blocks   = %8u\n", n_blocks);
}

struct repl_object *add_genid_to_repl_list(unsigned long long genid,
                                           struct repl_object *head)
{
    struct repl_object *obj;

    if (!enabled)
        return head;

    LOCK(&lock)
    {

        obj = pool_getablk(pool);
        if (!obj) {
            logmsg(LOGMSG_ERROR, "%s: out of memory in pool for genid %llx\n",
                    __func__, genid);
            errUNLOCK(&lock);
            return head;
        }

        obj->genid = genid;
        obj->next = head;
        obj->waiter_list = NULL;

        if (hash_add(hash, obj) != 0) {
            logmsg(LOGMSG_ERROR, "%s: error hashing genid %llx\n", __func__, genid);
            pool_relablk(pool, obj);
            errUNLOCK(&lock);
            return head;
        }

        n_allocs++;
    }
    UNLOCK(&lock);

    return obj;
}

void wait_for_genid_repl(unsigned long long genid)
{
    LOCK(&lock)
    {

        struct repl_object *obj;

        while ((obj = hash_find(hash, &genid)) != NULL) {
            /* It's still there.. wait for it to go away. */
            struct waiter me;
            pthread_mutexattr_t attr;

            /* Create a locked mutex and chain it on to the genids notify
             * list */
            pthread_mutexattr_init(&attr);
            pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_NORMAL);

            Pthread_mutex_init(&me.mutex, &attr);
            Pthread_mutex_lock(&me.mutex);
            me.next = obj->waiter_list;
            obj->waiter_list = &me;

            n_blocks++;

            xUNLOCK(&lock)
            {
                /* Try to double lock the mutex - this will block until
                 * the mutex is unlocked by clearing the genid from the
                 * list. */
                Pthread_mutex_lock(&me.mutex);
            }
            xLOCK(&lock);

            Pthread_mutex_unlock(&me.mutex);
            Pthread_mutex_destroy(&me.mutex);
            pthread_mutexattr_destroy(&attr);
        }
    }
    UNLOCK(&lock);
}

void clear_trans_from_repl_list(struct repl_object *head)
{
    if (!head)
        return;

    LOCK(&lock)
    {
        while (head) {
            struct repl_object *next = head->next;
            struct waiter *waiter, *next_waiter;

            /* Unblock all the waiters.  The waiter structs are automatic
             * stack variables on the blocked thread's stacks, so don't access
             * them after they've been unblocked. */
            for (waiter = head->waiter_list; waiter != NULL;
                 waiter = next_waiter) {
                next_waiter = waiter->next;
                Pthread_mutex_unlock(&waiter->mutex);
            }

            hash_del(hash, head);

            pool_relablk(pool, head);
            head = next;

            n_deallocs++;
        }
    }
    UNLOCK(&lock);
}
