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
** Generic object pool.
**
**
**
** Example
**
**      static int seed = 0;
**
**      int dummy_new(void **dst, void *arg)
**      {
**          seed++;
**          *dst = (void *)seed;
**          return 0;
**      }
**
**      int dummy_free(void *arg1, void *arg2)
**      {
**          return 0;
**      }
**
**      int main()
**      {
**
**          void *ent;
**          seed = 0;
**
**          // create a lifo pool
**          comdb2_objpool_t lifo;
**          comdb2_objpool_create_lifo(&lifo, "lifo", 3,
**          dummy_new, NULL, dummy_free, NULL);
**
**          comdb2_objpool_borrow(lifo, &ent);
**          // ent would == 1
**          comdb2_objpool_borrow(lifo, &ent);
**          // ent would == 2
**          comdb2_objpool_borrow(lifo, &ent);
**          // ent would == 3
**
**          comdb2_objpool_return(lifo, (void *)1);
**          comdb2_objpool_return(lifo, (void *)2);
**          comdb2_objpool_return(lifo, (void *)3);
**
**          comdb2_objpool_borrow(lifo, &ent);
**          // ent would == 3
**          comdb2_objpool_borrow(lifo, &ent);
**          // ent would == 2
**          comdb2_objpool_borrow(lifo, &ent);
**          // ent would == 1
**
**          comdb2_objpool_return(lifo, (void *)1);
**          comdb2_objpool_return(lifo, (void *)2);
**          comdb2_objpool_return(lifo, (void *)3);
**          comdb2_objpool_destroy(lifo);
**      }
**
**
**
** A generic object pool can be created to behave as a LIFO/FIFO/RANDOM queue.
** A LIFO pool returns the most recently returned object from the pool.
** A FIFO pool returns the least recently returned object from the pool.
** A RANDOM pool randomly returns an object from the pool.
**
** A generic object pool can be configured to evict objects due to idle time.
** However caution should be used when enabling auto-eviction. If the eviction
** thread runs too frequently, performance issues may result. When the eviction
** thread is running, all borrow/return requests will be blocked.
**
** A generic object pool does not prevent resource leak. Each borrow call should
** be paired with a return call. Borrowing without returning will exhaust
** the pool, eventually. However, never return an object that was not borrowed
** from the pool. The behavior is undefined and very dangerous.
*/

#ifndef _INCLUDED_OBJECT_POOL_H_
#define _INCLUDED_OBJECT_POOL_H_

#define OPT_DISABLE (-1)

/*
** Object pool structure.
*/
typedef struct comdb2_objpool *comdb2_objpool_t;

/*
** Object creation/deletion function.
*/
typedef int (*obj_new_fn)(void **, void *);
typedef int (*obj_del_fn)(void *, void *);

/*
** Object pool tunables.
**
** OP_CAPACITY       - maximum number of objects that can be allocated
**
** OP_MAX_IDLES      - maximum number of idle objects. no effect if
**                     OP_MAX_IDLE_RATIO is enabled. disabled by default
**
** OP_MAX_IDLE_RATIO - maximum ratio of # of idle objects to # of all objects.
**                     disabled by default
**
** OP_EVICT_INTERVAL - how long the eviction thread sleeps before the next run
*of
**                     examining idle objects. Caution should be used when
*setting
**                     the feature. If the thread runs too frequently,
*performance
**                     issues may result. disabled by default
**
** OP_EVICT_RATIO    - eviction thread waking-up threshold
**                     (# of idle objects / # of all objects). Caution should be
**                     used when setting the feature. disabled by default
**
*****************************************************************************
** The following tunables have no effect unless either OP_EVICT_INTERVAL or *
** OP_EVICT_RATIO is enabled.                                               *
*****************************************************************************
**
** OP_MIN_IDLES      - minimum number of idle objects. no effect if
**                     OP_MIN_IDLE_RATIO is enabled.
**                     the default setting is 8
**
** OP_MIN_IDLE_RATIO - minimum ratio of # of idle objects to # of all objects.
**                     disabled by default
**
** OP_IDLE_TIME      - minimum amout of time an object can sit idle in the pool.
**                     the default setting is 300000 ms (5 min)
*/
enum comdb2_objpool_option {
    OP_CAPACITY,
    OP_MAX_IDLES,
    OP_MAX_IDLE_RATIO,
    OP_EVICT_INTERVAL,
    OP_EVICT_RATIO,
    OP_MIN_IDLES,
    OP_MIN_IDLE_RATIO,
    OP_IDLE_TIME
};

/*
** Create a lifo object pool.
**
** Parameters
** opp     - object pool pointer
** name    - name of the pool
** cap     - maximum size of elements
** new_fn  - function to create an object
** new_arg - argument of new_fn
** del_fn  - function to delete an object
** del_arg - argument of del_fn
**
** Return Value
** 0      - success
** EINVAL - opp or new_fn is NULL, or cap <= 0
** ENOMEM - failed to malloc space for the structure
*/
int comdb2_objpool_create_lifo(comdb2_objpool_t *opp, const char *name,
                               unsigned int cap, obj_new_fn new_fn,
                               void *new_arg, obj_del_fn del_fn, void *del_arg);

/*
** Create a fifo object pool.
**
** Parameters
** opp     - object pool pointer
** name    - name of the pool
** cap     - maximum size of elements
** new_fn  - function to create an object
** new_arg - argument of new_fn
** del_fn  - function to delete an object
** del_arg - argument of del_fn
**
** Return Value
** 0      - success
** EINVAL - opp or new_fn is NULL, or cap <= 0
** ENOMEM - failed to malloc space for the structure
*/
int comdb2_objpool_create_fifo(comdb2_objpool_t *opp, const char *name,
                               unsigned int cap, obj_new_fn new_fn,
                               void *new_arg, obj_del_fn del_fn, void *del_arg);

/*
** Create a rand object pool.
**
** Parameters
** opp     - object pool pointer
** name    - name of the pool
** cap     - maximum size of elements
** new_fn  - function to create an object
** new_arg - argument of new_fn
** del_fn  - function to delete an object
** del_arg - argument of del_fn
**
** Return Value
** 0      - success
** EINVAL - opp or new_fn is NULL, or cap <= 0
** ENOMEM - failed to malloc space for the structure
*/
int comdb2_objpool_create_rand(comdb2_objpool_t *opp, const char *name,
                               unsigned int cap, obj_new_fn new_fn,
                               void *new_arg, obj_del_fn del_fn, void *del_arg);

/*
** Stop an object pool.
**
** Parameters
** op - an object pool
**
** Return Value
** 0      - success
** non-0  - failed to lock/unlock mutex
*/
int comdb2_objpool_stop(comdb2_objpool_t op);

/*
** Resume an object pool.
**
** Parameters
** op - an object pool
**
** Return Value
** 0      - success
** Other  - failed to lock/unlock mutex
*/
int comdb2_objpool_resume(comdb2_objpool_t op);

/*
** Destroy an object pool.
**
** Parameters
** op - an object pool
**
** Return Value
** 0      - success
** EBUSY  - some objects are not returned to the pool
** Other  - failed to lock/unlock mutex
*/
int comdb2_objpool_destroy(comdb2_objpool_t op);

/*
** Configure an object pool.
**
** Parameters
** op     - an object pool
** option - a comdb2_objpool_option
** value  - set `option' to `value'
**
** Return Value
** 0      - success
** EINVAL - either option or value is invalid
** ENOMEM - failed to malloc space for capacity change
** Other  - failed to lock/unlock mutex
*/
int comdb2_objpool_setopt(comdb2_objpool_t op,
                          enum comdb2_objpool_option option, int value);

/*
** Return an object to a pool.
**
** Parameters
** op  - an object pool
** obj - object
**
** Return Value
** 0      - success
** ENOSPC - obbject is not pooled.
**          it is deleted instead of being placed in the pool.
** EPERM  - the pool is stopped
** EEXIST - the object has been returned already
** Other  - failed to lock/unlock mutex
*/
int comdb2_objpool_return(comdb2_objpool_t op, void *obj);

/*
** Borrow an object from a pool.
** Block if the pool has no available object.
**
** Parameters
** op   - an object pool
** objp - address to be written upon success
**
** Return Value
** 0      - success
** EPERM  - the pool is stopped
** EEXIST - object returned by creation function already exists
** Other  - failed to lock/unlock mutex
*/
int comdb2_objpool_borrow(comdb2_objpool_t op, void **objp);

/*
** Borrow an object from a pool.
** If the pool has idle objects, return one of them.
** Otherwise, create a new one.
**
** The function is logically equivalent to the code snippet below.
** if (comdb2_objpool_tryborrow() == EAGAIN)
**     return new_fn();
**
** Parameters
** op   - an object pool
** objp - address to be written upon success
**
** Return Value
** 0      - success
** EPERM  - the pool is stopped
** Other  - failed to lock/unlock mutex
*/
int comdb2_objpool_forcedborrow(comdb2_objpool_t op, void **objp);

/*
** Borrow an object from a pool.
** If the pool has an available object, return it.
** Otherwise, create a new one.
**
** Parameters
** op   - an object pool
** objp - address to be written upon success
**
** Return Value
** 0      - success
** EPERM  - the pool is stopped
** EEXIST - object returned by creation function already exists
** Other  - failed to lock/unlock mutex
*/
int comdb2_objpool_available_borrow(comdb2_objpool_t op, void **objp);

/*
** Borrow an object from a pool. Return immediately
** if the pool has no available object.
**
** Parameters
** op   - an object pool
** objp - address to be written upon success
**
** Return Value
** 0      - success
** EAGAIN - no available object
** EPERM  - the pool is stopped
** Other  - failed to lock/unlock mutex
*/
int comdb2_objpool_tryborrow(comdb2_objpool_t op, void **objp);

/*
** Borrow an object from a pool. Wait a period of time
** before returning, if the pool has no available object.
**
** Parameters
** op       - an object pool
** objp     - address to be written upon success
** nanosecs - waiting time
**
** Return Value
** 0      - success
** ETIMEDOUT - the time specified by nanosecs has passed
**             yet still no available object
** EPERM  - the pool is stopped
** Other  - failed to lock/unlock mutex
*/
int comdb2_objpool_timedborrow(comdb2_objpool_t op, void **objp, long nanosecs);

/*
** Return the number of elements in a pool in a lockless manner.
** The value returned may not be accurate.
**
** Parameters
** op - an object pool
*/
int comdb2_objpool_size(comdb2_objpool_t op);

/*
** Print statistics of a pool.
**
** Parameters
** op - an object pool
**
** Return Value
** 0      - success
** Other  - failed to lock/unlock mutex
*/
int comdb2_objpool_stats(comdb2_objpool_t op);

#endif
