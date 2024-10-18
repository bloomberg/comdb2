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

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <time.h>
#include <sys/time.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "plhash.h"
#include "object_pool.h"
#include "logmsg.h"
#include "sys_wrap.h"
#include "math_util.h" /* min */

#include <mem_util.h>
#include <mem_override.h>

// macros
//#define OBJ_POOL_DEBUG
#define full(op) ((op)->nobjs == (op)->capacity)
#define empty(op) ((op)->nobjs == 0)
#define exhausted(op) ((op)->nactiveobjs == (op)->nobjs)
#define nidles(op) ((op)->nobjs - (op)->nactiveobjs)
#define idle_rate(op)                                                          \
    (((op)->nobjs == 0) ? 0 : (nidles(op) * 100.0 / (op)->nobjs))
#define idle_minus_1_rate(op)                                                  \
    (((op)->nobjs == 0) ? 0 : ((nidles(op) - 1) * 100.0 / (op)->nobjs))
#define idle_plus_1_rate(op)                                                   \
    (((op)->nobjs == 0) ? 0 : ((nidles(op) + 1) * 100.0 / (op)->nobjs))

#define reached_max_idle_criteria(op)                                          \
    (((op)->max_idle_ratio == OPT_DISABLE)                                     \
         ? reached_max_idles(op)                                               \
         : (idle_plus_1_rate(op) > (op)->max_idle_ratio))

#define reached_max_idles(op)                                                  \
    (((op)->max_idles != OPT_DISABLE) ? (nidles(op) >= (op)->max_idles) : 0)

#define reached_min_idle_criteria(op)                                          \
    (((op)->min_idle_ratio == OPT_DISABLE)                                     \
         ? reached_min_idles(op)                                               \
         : (idle_minus_1_rate(op) < (op)->min_idle_ratio))

#define reached_min_idles(op)                                                  \
    (((op)->min_idles != OPT_DISABLE) ? (nidles(op) <= (op)->min_idles) : 0)

#define eviction_disabled(op)                                                  \
    (op->evict_intv_ms == OPT_DISABLE && op->evict_ratio == OPT_DISABLE)

#define difftimems(begin, end)                                                 \
    (((end).tv_sec * 1000L + (end).tv_nsec / 1000000L) -                       \
     ((begin).tv_sec * 1000L + (begin).tv_nsec / 1000000L))

#ifdef OBJ_POOL_DEBUG
#define OP_DBG(op, msg)                                                        \
    fprintf(stderr, "[object pool: %s] thd %x, addr %p, "                      \
                    "cap %u, # %u, # active %u, # forced %u, waits %u, in "    \
                    "%d, out %d, %s\n",                                        \
            op->name, (unsigned)pthread_self(), op, op->capacity, op->nobjs,   \
            op->nactiveobjs, op->nforcedobjs, op->nborrowwaits, op->in,        \
            op->out, msg);
#else
#define OP_DBG(op, msg)
#endif
// ^^^^^^macros

// typedef
typedef void (*typed_put_fn)(comdb2_objpool_t, void *);
typedef void (*typed_get_fn)(comdb2_objpool_t, void **);
typedef void (*typed_evict_fn)(comdb2_objpool_t);
typedef void (*typed_clear_fn)(comdb2_objpool_t);

enum objpool_type { OP_LIFO, OP_FIFO, OP_RAND };

typedef struct pooled_object {
    void *object;
    unsigned int nreturns;
    unsigned int nborrows;
    struct timespec tm;
    int active;
    pthread_t tid;
} pooled_object;

typedef struct comdb2_objpool {
    enum objpool_type type;

    /* mutexes and conditions */
    pthread_mutex_t data_mutex;
    pthread_cond_t unexhausted;
    pthread_mutex_t evict_mutex;
    pthread_cond_t evict_cond;

    /* object creation/deletion callbacks */
    obj_new_fn new_fn;
    void *new_arg;
    obj_del_fn del_fn;
    void *del_arg;
    obj_not_fn not_fn;
    void *not_arg;

    /* eviction thread */
    int evict_thd_run;
    pthread_t evict_thd;

    /* status */
    int stopped;
    unsigned int nobjs;
    unsigned int nactiveobjs;
    unsigned int nforcedobjs;
    unsigned int npeakobjs;
    unsigned int nreturns;
    unsigned int nborrows;
    unsigned int nborrowwaits;
    unsigned int npeakborrowwaits;

    /* conf */
    int capacity;
    int max_idles;
    int max_idle_ratio;
    int evict_intv_ms;
    int evict_ratio;
    int min_idles;
    int min_idle_ratio;
    int idle_time_ms;

    /*
    ** hashtable for storing access history
    ** of an object
    */
    hash_t *history;

    /*
    ** put/get impl
    */
    typed_put_fn put_impl;
    typed_get_fn get_impl;
    typed_evict_fn evict_impl;
    typed_clear_fn clear_impl;

    /*
    ** ring-buffer queue
    */
    int in;
    int out;
    void **objs;

    size_t namesz;
    char name[1];
} comdb2_objpool;
// ^^^^^^typedef

// static funcs
/***********************************************
** static pool/object administration functions *
************************************************/
static int comdb2_objpool_create_int(comdb2_objpool_t *opp, const char *name,
                                     unsigned int cap, obj_new_fn new_fn,
                                     void *new_arg, obj_del_fn del_fn,
                                     void *del_arg, obj_not_fn not_fn,
                                     void *not_arg, enum objpool_type type);

static int object_create(comdb2_objpool_t op, void **objp);
static int hash_elem_free_wrapper(void *, void *);
static void *eviction_thread(void *);

/**********************
** pool configuration *
***********************/
static int opt_capacity(comdb2_objpool_t op, int value);
static int opt_max_idles(comdb2_objpool_t op, int value);
static int opt_max_idle_ratio(comdb2_objpool_t op, int value);
static int opt_evict_intv_ms(comdb2_objpool_t op, int value);
static int opt_evict_ratio(comdb2_objpool_t op, int value);
static int opt_min_idles(comdb2_objpool_t op, int value);
static int opt_min_idle_ratio(comdb2_objpool_t op, int value);
static int opt_idle_time_ms(comdb2_objpool_t op, int value);

/**********************************
** static return/borrow functions *
***********************************/
static int objpool_return_int(comdb2_objpool_t op, void *obj);
static int objpool_borrow_int(comdb2_objpool_t op, void **objp, long nanosecs,
                              int force);

static void objpool_lifo_put(comdb2_objpool_t op, void *obj);
static void objpool_lifo_get(comdb2_objpool_t op, void **objp);
static void objpool_lifo_evict(comdb2_objpool_t op);
static void objpool_lifo_clear(comdb2_objpool_t op);
static void objpool_fifo_put(comdb2_objpool_t op, void *obj);
static void objpool_fifo_get(comdb2_objpool_t op, void **objp);
static void objpool_fifo_evict(comdb2_objpool_t op);
static void objpool_fifo_clear(comdb2_objpool_t op);
static void objpool_rand_put(comdb2_objpool_t op, void *obj);
static void objpool_rand_get(comdb2_objpool_t op, void **objp);
static void objpool_rand_evict(comdb2_objpool_t op);
static void objpool_rand_clear(comdb2_objpool_t op);

static void objpool_evict_all_int(comdb2_objpool_t op);

/*************************
** stats-display helpers *
**************************/
static const char *objpool_type_name(comdb2_objpool_t op);
// ^^^^^^static funcs

int comdb2_objpool_create_lifo(comdb2_objpool_t *opp, const char *name,
                               unsigned int cap, obj_new_fn new_fn,
                               void *new_arg, obj_del_fn del_fn,
                               void *del_arg, obj_not_fn not_fn,
                               void *not_arg)
{
    return comdb2_objpool_create_int(opp, name, cap, new_fn, new_arg, del_fn,
                                     del_arg, not_fn, not_arg, OP_LIFO);
}

int comdb2_objpool_create_fifo(comdb2_objpool_t *opp, const char *name,
                               unsigned int cap, obj_new_fn new_fn,
                               void *new_arg, obj_del_fn del_fn,
                               void *del_arg, obj_not_fn not_fn,
                               void *not_arg)
{
    return comdb2_objpool_create_int(opp, name, cap, new_fn, new_arg, del_fn,
                                     del_arg, not_fn, not_arg, OP_FIFO);
}

int comdb2_objpool_create_rand(comdb2_objpool_t *opp, const char *name,
                               unsigned int cap, obj_new_fn new_fn,
                               void *new_arg, obj_del_fn del_fn,
                               void *del_arg, obj_not_fn not_fn,
                               void *not_arg)
{
    return comdb2_objpool_create_int(opp, name, cap, new_fn, new_arg, del_fn,
                                     del_arg, not_fn, not_arg, OP_RAND);
}

int comdb2_objpool_stop(comdb2_objpool_t op)
{
    OP_DBG(op, "stop pool");
    Pthread_mutex_lock(&op->data_mutex);
    op->stopped = 1;
    Pthread_cond_broadcast(&op->unexhausted);
    Pthread_mutex_unlock(&op->data_mutex);
    return 0;
}

int comdb2_objpool_resume(comdb2_objpool_t op)
{
    OP_DBG(op, "resume pool");
    Pthread_mutex_lock(&op->data_mutex);
    op->stopped = 0;
    Pthread_mutex_unlock(&op->data_mutex);
    return 0;
}

int comdb2_objpool_clear(comdb2_objpool_t op)
{
    objpool_evict_all_int(op);
    return 0;
}

int comdb2_objpool_destroy(comdb2_objpool_t op)
{
    {
        Pthread_mutex_lock(&op->data_mutex);
        if (op->nactiveobjs > 0) {
            /* active objects out there, can't proceed */
            Pthread_mutex_unlock(&op->data_mutex);
            return EBUSY;
        }

        op->stopped = 1;

        if (op->evict_thd_run) {
            /* terminate eviction thread */
            Pthread_mutex_lock(&op->evict_mutex);
            op->evict_thd_run = 0;

            Pthread_cond_signal(&op->evict_cond);

            Pthread_mutex_unlock(&op->evict_mutex);

            int rc = pthread_join(op->evict_thd, NULL);
            if (rc != 0) {
                Pthread_mutex_unlock(&op->data_mutex);
                return rc;
            }
        }

        /* clear all objects in the pool */
        op->clear_impl(op);

        /* clear access history */
        hash_for(op->history, hash_elem_free_wrapper, NULL);
        hash_free(op->history);
        free(op->objs);

        Pthread_mutex_unlock(&op->data_mutex);
    }

    /* clean up mutexes and conditions */
    Pthread_mutex_destroy(&op->data_mutex);
    Pthread_mutex_destroy(&op->evict_mutex);
    Pthread_cond_destroy(&op->unexhausted);
    Pthread_cond_destroy(&op->evict_cond);

    free(op);

    return 0;
}

int comdb2_objpool_setopt(comdb2_objpool_t op,
                          enum comdb2_objpool_option option, int value)
{
    int rc = 0;
    Pthread_mutex_lock(&op->data_mutex);
    {
        switch (option) {
        case OP_CAPACITY:
            rc = opt_capacity(op, value);
            break;
        case OP_MAX_IDLES:
            rc = opt_max_idles(op, value);
            break;
        case OP_MAX_IDLE_RATIO:
            rc = opt_max_idle_ratio(op, value);
            break;
        case OP_EVICT_INTERVAL:
            rc = opt_evict_intv_ms(op, value);
            break;
        case OP_EVICT_RATIO:
            rc = opt_evict_ratio(op, value);
            break;
        case OP_MIN_IDLES:
            rc = opt_min_idles(op, value);
            break;
        case OP_MIN_IDLE_RATIO:
            rc = opt_min_idle_ratio(op, value);
            break;
        case OP_IDLE_TIME:
            rc = opt_idle_time_ms(op, value);
            break;
        default:
            rc = EINVAL;
            break;
        }
    }
    Pthread_mutex_unlock(&op->data_mutex);
    return rc;
}

int comdb2_objpool_return(comdb2_objpool_t op, void *obj)
{
    return objpool_return_int(op, obj);
}

int comdb2_objpool_notify(comdb2_objpool_t op, int force)
{
    int rc;
    assert(op != NULL);
    OP_DBG(op, "notify pool");
    Pthread_mutex_lock(&op->data_mutex);
    if (force || (op->nborrowwaits != 0)) {
        Pthread_cond_broadcast(&op->unexhausted);
        rc = 0;
    } else {
        rc = EPERM;
    }
    Pthread_mutex_unlock(&op->data_mutex);
    return rc;
}

int comdb2_objpool_borrow(comdb2_objpool_t op, void **objp)
{
    return objpool_borrow_int(op, objp, -1, 0);
}

int comdb2_objpool_forcedborrow(comdb2_objpool_t op, void **objp)
{
    return objpool_borrow_int(op, objp, 0, 1);
}

int comdb2_objpool_tryborrow(comdb2_objpool_t op, void **objp)
{
    return objpool_borrow_int(op, objp, 0, 0);
}

int comdb2_objpool_timedborrow(comdb2_objpool_t op, void **objp, long nanosecs)
{
    return objpool_borrow_int(op, objp, nanosecs, 0);
}

int comdb2_objpool_size(comdb2_objpool_t op) { return op->nobjs; }

int comdb2_objpool_stats(comdb2_objpool_t op)
{
    Pthread_mutex_lock(&op->data_mutex);

    /* status */
    logmsg(LOGMSG_USER, "Object pool [%s] stats\n", op->name);
    logmsg(LOGMSG_USER, "  Type                 : %s\n", objpool_type_name(op));
    logmsg(LOGMSG_USER, "  Status               : %s\n",
           op->stopped ? "STOPPED" : "running");
    logmsg(LOGMSG_USER, "  Current load         : %.f%%\n",
           (op->nobjs == 0) ? 0 : 100.0 * (op->nborrowwaits + op->nactiveobjs) /
                                      op->nobjs);
    logmsg(LOGMSG_USER, "  # total objects      : %u\n", op->nforcedobjs + op->nobjs);
    logmsg(LOGMSG_USER, "  # peak               : %u\n",
           op->npeakobjs == 0 ? op->nobjs : op->npeakobjs);
    logmsg(LOGMSG_USER, "  # active             : %u\n", op->nactiveobjs + op->nforcedobjs);
    logmsg(LOGMSG_USER, "  # idle               : %u\n", nidles(op));
    logmsg(LOGMSG_USER, "  # pooled objects     : %u\n", op->nobjs);
    logmsg(LOGMSG_USER, "  # unpooled objects   : %u\n", op->nforcedobjs);
    logmsg(LOGMSG_USER, "  # returns            : %u\n", op->nreturns);
    logmsg(LOGMSG_USER, "  # borrows            : %u\n", op->nborrows);
    logmsg(LOGMSG_USER, "  # borrow waits       : %u\n", op->nborrowwaits);
    logmsg(LOGMSG_USER, "  # peak borrow waits  : %u\n", op->npeakborrowwaits);
    logmsg(LOGMSG_USER, "  Capacity             : %u\n", op->capacity);

    /* configuration */
    if (op->max_idles == OPT_DISABLE || op->max_idle_ratio != OPT_DISABLE)
        logmsg(LOGMSG_USER, "  # max idles          : DISABLED\n");
    else
        logmsg(LOGMSG_USER, "  # max idles          : %d\n", op->max_idles);

    if (op->max_idle_ratio == OPT_DISABLE)
        logmsg(LOGMSG_USER, "  Max idle rate        : DISABLED\n");
    else
        logmsg(LOGMSG_USER, "  Max idle rate        : %d%%\n", op->max_idle_ratio);

    if (op->evict_intv_ms == OPT_DISABLE)
        logmsg(LOGMSG_USER, "  Eviction interval    : DISABLED\n");
    else
        logmsg(LOGMSG_USER, "  Eviction interval    : %d ms\n", op->evict_intv_ms);

    if (op->evict_ratio == OPT_DISABLE)
        logmsg(LOGMSG_USER, "  Eviction threshold   : DISABLED\n");
    else
        logmsg(LOGMSG_USER, "  Eviction threshold   : %d%%\n", op->evict_ratio);

    if (op->min_idles == OPT_DISABLE)
        logmsg(LOGMSG_USER, "  # min idles          : DISABLED\n");
    else
        logmsg(LOGMSG_USER, "  # min idles          : %d\n", op->min_idles);

    if (op->min_idle_ratio == OPT_DISABLE)
        logmsg(LOGMSG_USER, "  # min idle rate      : DISABLED\n");
    else
        logmsg(LOGMSG_USER, "  # min idle rate      : %d%%\n", op->min_idle_ratio);

    if (op->idle_time_ms == OPT_DISABLE)
        logmsg(LOGMSG_USER, "  Max idle time        : DISABLED\n");
    else
        logmsg(LOGMSG_USER, "  Max idle time        : %d ms\n", op->idle_time_ms);

    Pthread_mutex_unlock(&op->data_mutex);

    return 0;
}

// static function impl
static int comdb2_objpool_create_int(comdb2_objpool_t *opp, const char *name,
                                     unsigned int cap, obj_new_fn new_fn,
                                     void *new_arg, obj_del_fn del_fn,
                                     void *del_arg, obj_not_fn not_fn,
                                     void *not_arg, enum objpool_type type)
{
    comdb2_objpool_t op;
    const char *pname;
    size_t sz;

    if (opp == NULL || cap <= 0 || new_fn == NULL)
        return EINVAL;

    pname = (name == NULL) ? "Anonymous" : name;
    sz = strlen(pname);

    /* malloc structure */
    op = malloc(sizeof(comdb2_objpool) + sizeof(char) * sz);
    if (op == NULL)
        return ENOMEM;

    /* create mutex and condition */
    Pthread_mutex_init(&op->data_mutex, NULL);

    Pthread_mutex_init(&op->evict_mutex, NULL);

    Pthread_cond_init(&op->unexhausted, NULL);
    Pthread_cond_init(&op->evict_cond, NULL);

    /* initialize attributes */
    op->capacity = cap;
    op->in = 0;
    op->out = 0;
    op->objs = malloc(cap * sizeof(void *));
    if (op->objs == NULL) {
        Pthread_mutex_destroy(&op->data_mutex);
        Pthread_mutex_destroy(&op->evict_mutex);
        Pthread_cond_destroy(&op->unexhausted);
        Pthread_cond_destroy(&op->evict_cond);
        free(op);
        return ENOMEM;
    }

    op->type = type;
    switch (type) {
    case OP_LIFO:
        op->put_impl = objpool_lifo_put;
        op->get_impl = objpool_lifo_get;
        op->evict_impl = objpool_lifo_evict;
        op->clear_impl = objpool_lifo_clear;
        break;
    case OP_FIFO:
        op->put_impl = objpool_fifo_put;
        op->get_impl = objpool_fifo_get;
        op->evict_impl = objpool_fifo_evict;
        op->clear_impl = objpool_fifo_clear;
        break;
    case OP_RAND:
        op->put_impl = objpool_rand_put;
        op->get_impl = objpool_rand_get;
        op->evict_impl = objpool_rand_evict;
        op->clear_impl = objpool_rand_clear;
        break;
    default:
        Pthread_mutex_destroy(&op->data_mutex);
        Pthread_mutex_destroy(&op->evict_mutex);
        Pthread_cond_destroy(&op->unexhausted);
        Pthread_cond_destroy(&op->evict_cond);
        free(op);
        return EINVAL;
    }

    op->new_fn = new_fn;
    op->new_arg = new_arg;
    op->del_fn = del_fn;
    op->del_arg = del_arg;
    op->not_fn = not_fn;
    op->not_arg = not_arg;

    op->namesz = sz;
    strcpy(op->name, pname);
    op->history = hash_init(sizeof(void *));
    op->max_idles = OPT_DISABLE;
    op->max_idle_ratio = OPT_DISABLE;
    op->evict_intv_ms = OPT_DISABLE;
    op->evict_ratio = OPT_DISABLE;
    op->min_idles = 8;
    op->min_idle_ratio = OPT_DISABLE;
    op->idle_time_ms = 300000;

    op->evict_thd_run = 0;
    op->stopped = 0;
    op->nobjs = 0;
    op->nactiveobjs = 0;
    op->nforcedobjs = 0;
    op->npeakobjs = 0;
    op->nreturns = 0;
    op->nborrows = 0;
    op->nborrowwaits = 0;
    op->npeakborrowwaits = 0;

    OP_DBG(op, "object pool created");
    *opp = op;
    return 0;
}

static int object_create(comdb2_objpool_t op, void **objp)
{
    pooled_object *rec;
    int rc;

    rc = op->new_fn(objp, op->new_arg);
    if (rc != 0)
        return rc;

    rec = malloc(sizeof(pooled_object));
    if (rec == NULL) {
        if (op->del_fn != NULL)
            op->del_fn(*objp, op->del_arg);
        return ENOMEM;
    }

    rec->object = *objp;
    rec->nreturns = 0;
    rec->nborrows = 1;
    rec->tm = (struct timespec){0};
    rec->active = 1;
    rec->tid = pthread_self();

    rc = hash_add(op->history, rec);
    if (rc == 0) {
        ++op->nobjs;
        if (op->nobjs > op->npeakobjs)
            op->npeakobjs = op->nobjs;
        ++op->nactiveobjs;
        logmsg(LOGMSG_INFO, "created a pool %s object %p\n", op->name, *objp);
        OP_DBG(op, "create object done");
    } else {
        if (op->del_fn != NULL)
            op->del_fn(*objp, op->del_arg);
        free(rec);
    }

    return rc;
}

static int hash_elem_free_wrapper(void *elem, void *unused)
{
    free(elem);
    return 0;
}

static void *eviction_thread(void *arg)
{
    comdb2_name_thread(__func__);
    comdb2_objpool_t op;
    intptr_t rc;
    struct timespec tm;

    op = (comdb2_objpool_t)arg;
    OP_DBG(op, "start eviction thr");

    Pthread_mutex_lock(&op->evict_mutex);

    op->evict_thd_run = 1;

    while (1) {
        if (eviction_disabled(op)) { /* just in case */
            OP_DBG(op, "eviction thr disabled");
            break;
        }

        rc = 0;
        if (op->evict_intv_ms == OPT_DISABLE)
            Pthread_cond_wait(&op->evict_cond, &op->evict_mutex);
        else {
            clock_gettime(CLOCK_REALTIME, &tm);
            tm.tv_nsec += 1000000L * op->evict_intv_ms;
            while (tm.tv_nsec >= 1000000000) {
                tm.tv_nsec -= 1000000000;
                tm.tv_sec += 1;
            }
            rc = pthread_cond_timedwait(&op->evict_cond, &op->evict_mutex, &tm);
        }

        if (rc != 0 && rc != ETIMEDOUT)
            break;

        if (op->evict_thd_run == 0) {
            /* interrupted */
            OP_DBG(op, "eviction thr intr");
            break;
        }

        OP_DBG(op, "start type-specific eviction routine");
        op->evict_impl(op);
    }

    op->evict_thd_run = 0;
    Pthread_mutex_unlock(&op->evict_mutex);
    return NULL;
}

static int objpool_return_int(comdb2_objpool_t op, void *obj)
{
    int rc = 0;
    pooled_object *rec;

    if (op->stopped)
        return EPERM;

    Pthread_mutex_lock(&op->data_mutex);

    if (op->stopped) {
        Pthread_mutex_unlock(&op->data_mutex);
        return EPERM;
    }

    rec = (pooled_object *)hash_find_readonly(op->history, &obj);
    if (rec == NULL) {
        --op->nforcedobjs;
        ++op->nreturns;
        /* obj was forcefully-created, free it and return */
        rc = 0;
        if (op->del_fn != NULL)
            rc = op->del_fn(obj, op->del_arg);
        logmsg(LOGMSG_INFO, "destroyed a forced pool %s object %p (%d)\n",
               op->name, obj, rc);
        OP_DBG(op, "object deleted");
    } else if (rec->active == 0) {
        OP_DBG(op, "error- double return");
        rc = EEXIST;
    } else if (reached_max_idle_criteria(op)) {
        OP_DBG(op, "reached maximum number of idle objects");
        /*
         ** reached maximum, delete the object
         ** and its access history, decrement
         ** both nobjs and nactive.
         */
        rc = hash_del(op->history, rec);
        free(rec);
        if (op->del_fn != NULL)
            rc = op->del_fn(obj, op->del_arg);
        --op->nactiveobjs;
        ++op->nreturns;
        --op->nobjs;
        logmsg(LOGMSG_INFO, "destroyed a pool %s object %p\n", op->name, obj);
        OP_DBG(op, "evicted due to max idle");
    } else {
        clock_gettime(CLOCK_REALTIME, &rec->tm);
        ++rec->nreturns;
        rec->active = 0;

        op->put_impl(op, obj);
        --op->nactiveobjs;
        ++op->nreturns;
        OP_DBG(op, "returned to pool");

        if (op->evict_ratio != OPT_DISABLE &&
            idle_rate(op) >= op->evict_ratio) {
            Pthread_mutex_lock(&op->evict_mutex);
            Pthread_mutex_unlock(&op->data_mutex);
            OP_DBG(op, "signal evict_cond");
            Pthread_cond_signal(&op->evict_cond);
            Pthread_mutex_unlock(&op->evict_mutex);
            return 0;
        }

        if (op->nborrowwaits != 0) {
            OP_DBG(op, "signal unexhausted");
            Pthread_cond_signal(&op->unexhausted);
        }
    }

    Pthread_mutex_unlock(&op->data_mutex);

    return rc;
}

static int objpool_borrow_int(comdb2_objpool_t op, void **objp, long nanosecs,
                              int force)
{
    int rc = 0;
    struct timespec tm;
    pooled_object *rec;

    if (op->stopped)
        return EPERM;

    Pthread_mutex_lock(&op->data_mutex);

    if (op->stopped) {
        Pthread_mutex_unlock(&op->data_mutex);
        return EPERM;
    }

retry:

    if (exhausted(op)) {
        if (!full(op)) {
            /*
             ** if pool is not full, create a new object and an access
             ** history record, and then return the object in objp
             */
            OP_DBG(op, "pool exhausted but not full");
            rc = object_create(op, objp);
            if (rc == 0)
                ++op->nborrows;
            Pthread_mutex_unlock(&op->data_mutex);
            return rc;
        }

        if (force) {
            OP_DBG(op, "pool exhausted and full, force to get one");
            /*
             ** if pool is full and caller specified `force',
             ** create a new object and directly return it in objp
             */
            rc = op->new_fn(objp, op->new_arg);
            if (rc == 0) {
                ++op->nforcedobjs;
                if (op->nforcedobjs + op->nobjs > op->npeakobjs)
                    op->npeakobjs = op->nforcedobjs + op->nobjs;
                ++op->nborrows;
            }
            logmsg(LOGMSG_INFO, "created a forced pool %s object %p (%d)\n",
                   op->name, *objp, rc);
            OP_DBG(op, "forced create object done");
            Pthread_mutex_unlock(&op->data_mutex);
            return rc;
        }

        if (nanosecs == 0) {
            /*
             ** if pool is full and caller doesn't want to wait,
             ** return EAGAIN
             */
            Pthread_mutex_unlock(&op->data_mutex);
            return EAGAIN;
        }

        while (1) {
            /*
             ** first, issue a notification to the pool and
             ** give it a chance to do something to prepare
             ** for waiting -OR- to force object creation
             ** -OR- cancel waiting and fail the request.
             */
            if (op->not_fn != NULL) {
                int not_rc = op->not_fn(objp, op->not_arg);
                if (not_rc == OP_FAIL_NOW) {
                    OP_DBG(op, "canceled by not_fn()");
                    Pthread_mutex_unlock(&op->data_mutex);
                    return ECANCELED;
                } else if (not_rc == OP_FORCE_NOW) {
                    OP_DBG(op, "forced by not_fn()");
                    force = 1;
                    goto retry; /* mutex still held */
                }
            }

            /*
             ** if pool is full and caller is willing to wait,
             ** make a condition wait on unexhausted
             */
            ++op->nborrowwaits;
            OP_DBG(op, "pool exhausted and full, wait");
            if (op->nborrowwaits > op->npeakborrowwaits)
                op->npeakborrowwaits = op->nborrowwaits;

            rc = 0;
            if (nanosecs < 0)
                Pthread_cond_wait(&op->unexhausted, &op->data_mutex);
            else {
                clock_gettime(CLOCK_REALTIME, &tm);
                tm.tv_nsec += nanosecs;
                while (tm.tv_nsec >= 1000000000) {
                    tm.tv_nsec -= 1000000000;
                    tm.tv_sec += 1;
                }
                rc = pthread_cond_timedwait(&op->unexhausted, &op->data_mutex,
                                            &tm);
            }
            --op->nborrowwaits;
            OP_DBG(op, "thr wake up");

            if (rc != 0) {
                Pthread_mutex_unlock(&op->data_mutex);
                return rc;
            }

            if (op->stopped) {
                /* interrupted by stop() */
                OP_DBG(op, "intr by stop()");
                Pthread_mutex_unlock(&op->data_mutex);
                return EPERM;
            }

            if (!exhausted(op))
                break;

            /* interrupted by changing capactiy to a larger value */
            OP_DBG(op, "intr or spurious wakeup");
            if (full(op)) {
                if (nanosecs > 0) {
                    Pthread_mutex_unlock(&op->data_mutex);
                    return ETIMEDOUT;
                }
                /* if caller didn't specify waiting time, cond_wait again */
            } else {
                OP_DBG(op, "pool exhausted but no longer full");
                rc = object_create(op, objp);
                if (rc == 0)
                    ++op->nborrows;
                Pthread_mutex_unlock(&op->data_mutex);
                return rc;
            }
        }
    }

    op->get_impl(op, objp);
    ++op->nactiveobjs;
    ++op->nborrows;

    /* update access history */
    rec = (pooled_object *)hash_find(op->history, objp);
    Pthread_mutex_unlock(&op->data_mutex);

    rec->active = 1;
    ++rec->nborrows;
    rec->tid = pthread_self();


    OP_DBG(op, "borrowed from pool");
    return rc;
}

// get/put/evict impl
static void objpool_lifo_get(comdb2_objpool_t op, void **objp)
{
    *objp = op->objs[op->out];
    op->in = op->out;
    --op->out;
}

static void objpool_lifo_put(comdb2_objpool_t op, void *obj)
{
    op->objs[op->in] = obj;
    op->out = op->in;
    ++op->in;
}

static void objpool_lifo_evict(comdb2_objpool_t op)
{
    int indx;
    pooled_object *rec;
    void *object;
    struct timespec tm;

    Pthread_mutex_lock(&op->data_mutex);

    clock_gettime(CLOCK_REALTIME, &tm);

    int nidles = nidles(op);

    for (indx = 0; indx < nidles; ++indx) {

        if (reached_min_idle_criteria(op))
            break;

        object = op->objs[indx];
        rec = (pooled_object *)hash_find(op->history, &object);

        if (op->idle_time_ms != OPT_DISABLE &&
            difftimems(rec->tm, tm) <= op->idle_time_ms)
            break;

        hash_del(op->history, rec);
        free(rec);
        if (op->del_fn != NULL)
            op->del_fn(object, op->del_arg);
        --op->nobjs;

        logmsg(LOGMSG_INFO, "destroyed a pool %s object %p\n", op->name, object);
        OP_DBG(op, "idle object evicted");
    }

    if (indx != 0) {
        memmove(op->objs, op->objs + indx, sizeof(void *) * (op->in - indx));

        op->in -= indx;
        op->out = op->in - 1;
    }

    Pthread_mutex_unlock(&op->data_mutex);
}

static void objpool_lifo_clear(comdb2_objpool_t op)
{
    int indx;

    if (op->del_fn != NULL)
        for (indx = 0; indx != op->in; ++indx)
            op->del_fn(op->objs[indx], op->del_arg);
}

static void objpool_fifo_get(comdb2_objpool_t op, void **objp)
{
    *objp = op->objs[op->out];
    ++op->out;
    if (op->out >= op->capacity)
        op->out -= op->capacity;
}

static void objpool_fifo_put(comdb2_objpool_t op, void *obj)
{
    op->objs[op->in] = obj;
    ++op->in;
    if (op->in >= op->capacity)
        op->in -= op->capacity;
}

static void objpool_fifo_evict(comdb2_objpool_t op)
{
    int indx, cnt;
    pooled_object *rec;
    void *object;
    struct timespec tm;

    Pthread_mutex_lock(&op->data_mutex);

    clock_gettime(CLOCK_REALTIME, &tm);

    int nidles = nidles(op);

    for (cnt = 0; cnt < nidles; ++cnt) {
        if (reached_min_idle_criteria(op))
            break;

        indx = op->out + cnt;
        if (indx >= op->capacity)
            indx -= op->capacity;

        object = op->objs[indx];
        rec = (pooled_object *)hash_find(op->history, &object);

        if (op->idle_time_ms != OPT_DISABLE &&
            difftimems(rec->tm, tm) <= op->idle_time_ms)
            break;

        hash_del(op->history, rec);
        free(rec);
        if (op->del_fn != NULL)
            op->del_fn(object, op->del_arg);
        --op->nobjs;

        logmsg(LOGMSG_INFO, "destroyed a pool %s object %p\n", op->name, object);
        OP_DBG(op, "idle object evicted");
    }

    op->out += cnt;

    Pthread_mutex_unlock(&op->data_mutex);
}

static void objpool_fifo_clear(comdb2_objpool_t op)
{
    int indx;

    if (op->del_fn != NULL)
        for (indx = op->out; indx != op->in; ++indx) {
            if (indx >= op->capacity)
                indx -= op->capacity;

            op->del_fn(op->objs[indx], op->del_arg);
        }
}

static void objpool_rand_get(comdb2_objpool_t op, void **objp)
{
    int out = random() % nidles(op);
    *objp = op->objs[out];
    --op->in;
    op->objs[out] = op->objs[op->in];
}

static void objpool_rand_put(comdb2_objpool_t op, void *obj)
{
    op->objs[op->in] = obj;
    ++op->in;
}

static void objpool_rand_evict(comdb2_objpool_t op)
{
    int indx;
    pooled_object *rec;
    void *object;
    struct timespec tm;

    Pthread_mutex_lock(&op->data_mutex);

    clock_gettime(CLOCK_REALTIME, &tm);

    int nidles = nidles(op);

    for (indx = 0; indx < nidles && op->objs[indx] != NULL;) {
        if (reached_min_idle_criteria(op))
            break;

        object = op->objs[indx];
        rec = (pooled_object *)hash_find(op->history, &object);

        if (op->idle_time_ms != OPT_DISABLE &&
            difftimems(rec->tm, tm) <= op->idle_time_ms)
            ++indx;
        else {
            hash_del(op->history, rec);
            free(rec);
            if (op->del_fn != NULL)
                op->del_fn(object, op->del_arg);
            --op->nobjs;

            /* swap */

            --op->in;
            op->objs[indx] = op->objs[op->in];

            logmsg(LOGMSG_INFO, "destroyed a pool %s object %p\n", op->name, object);
            OP_DBG(op, "idle object evicted");
        }
    }

    Pthread_mutex_unlock(&op->data_mutex);
}

static void objpool_rand_clear(comdb2_objpool_t op)
{
    int indx;

    if (op->del_fn != NULL)
        for (indx = 0; indx != op->in; ++indx)
            op->del_fn(op->objs[indx], op->del_arg);
}
// ^^^^^^get/put impl

static void objpool_evict_all_int(comdb2_objpool_t op)
{
    int indx;
    pooled_object *rec;
    void *object;

    Pthread_mutex_lock(&op->data_mutex);

    int nidles = nidles(op);

    for (indx = 0; indx < nidles; ++indx) {
        object = op->objs[indx];
        rec = (pooled_object *)hash_find(op->history, &object);

        hash_del(op->history, rec);
        free(rec);
        if (op->del_fn != NULL)
            op->del_fn(object, op->del_arg);
        --op->nobjs;

        logmsg(LOGMSG_INFO, "destroyed a pool %s object %p\n", op->name, object);
        OP_DBG(op, "object evicted");
    }

    if (indx != 0) {
        memmove(op->objs, op->objs + indx, sizeof(void *) * (op->in - indx));

        op->in -= indx;
        op->out = op->in - 1;
    }

    Pthread_mutex_unlock(&op->data_mutex);
}

static int opt_capacity(comdb2_objpool_t op, int value)
{
    void **resized;
    int bottom;
    pooled_object *rec;
    /* # of a single memcpy call */
    size_t nszcpy;
    /* total # of all memcpy calls */
    size_t ntotalcpy = 0;
    /* the maximum number of idle objects
       that can be copied to the new ring buffer */
    size_t nidleobjscpy;

    if (value <= 0 || value < op->nactiveobjs)
        return EINVAL;

    nidleobjscpy = min(value, op->nobjs) - op->nactiveobjs;

    /*
    ** we can't simply realloc(objs, new size).
    ** if changing capacity to a smaller value,
    ** idle objects outside the new boundary need
    ** to be cleared. however, realloc may overrun
    ** the address space outside the new boundary.
    ** so instead we make a malloc and then memcpy
    */
    resized = malloc(sizeof(void *) * value);
    if (resized == NULL)
        return ENOMEM;

    OP_DBG(op, "reorganize pool, before");

    bottom = (op->type == OP_FIFO) ? op->out : 0;

    if (op->in > bottom || (op->in == bottom && exhausted(op))) {
        ntotalcpy = nszcpy = min(nidleobjscpy, (op->in - bottom));
        memcpy(resized, op->objs + bottom, sizeof(void *) * nszcpy);
    } else {
        nszcpy = min(nidleobjscpy, (op->capacity - bottom));
        ntotalcpy += nszcpy;
        memcpy(resized, op->objs + bottom, sizeof(void *) * nszcpy);
        nszcpy = min(nidleobjscpy - nszcpy, op->in);
        ntotalcpy += nszcpy;
        memcpy(resized, op->objs, sizeof(void *) * nszcpy);
    }

    for (nszcpy = ntotalcpy; nszcpy < nidles(op); ++nszcpy) {
        --op->in;
        rec = (pooled_object *)hash_find(op->history, &op->objs[op->in]);
        hash_del(op->history, rec);
        free(rec);
        if (op->del_fn)
            op->del_fn(op->objs[op->in], op->del_arg);
    }

    free(op->objs);
    op->objs = resized;
    op->in = ntotalcpy;
    op->out = (op->type == OP_LIFO) ? (op->in - 1) : 0;
    op->nobjs = ntotalcpy + op->nactiveobjs;
    op->capacity = value;

    OP_DBG(op, "reorganize pool, after");

    if (op->nborrowwaits > 0 && !full(op))
        Pthread_cond_broadcast(&op->unexhausted);

    return 0;
}

static int opt_max_idles(comdb2_objpool_t op, int value)
{
    if (value <= 0 && value != OPT_DISABLE)
        return EINVAL;
    op->max_idles = value;
    return 0;
}

static int opt_max_idle_ratio(comdb2_objpool_t op, int value)
{
    if ((value < 0 || value > 100) && value != OPT_DISABLE)
        return EINVAL;
    op->max_idle_ratio = value;
    return 0;
}

static int opt_evict_intv_ms(comdb2_objpool_t op, int value)
{
    int rc = 0;

    if (value < 0 && value != OPT_DISABLE)
        return EINVAL;

    if (op->evict_thd_run && value == OPT_DISABLE &&
        op->evict_ratio == OPT_DISABLE) {
        /* eviction thr is running. caller wants to terminate it */
        {
            Pthread_mutex_lock(&op->evict_mutex);
            op->evict_thd_run = 0;
            Pthread_cond_signal(&op->evict_cond);

            Pthread_mutex_unlock(&op->evict_mutex);
            rc = pthread_join(op->evict_thd, NULL);
        }

    } else if (!op->evict_thd_run && value != OPT_DISABLE) {
        /* eviction thr isn't running and caller wants to start it */
        Pthread_mutex_lock(&op->evict_mutex);

        rc = pthread_create(&op->evict_thd, NULL, eviction_thread, op);
        Pthread_mutex_unlock(&op->evict_mutex);
    }

    if (rc == 0)
        op->evict_intv_ms = value;
    return rc;
}

static int opt_evict_ratio(comdb2_objpool_t op, int value)
{
    int rc = 0;

    if ((value < 0 || value > 100) && value != OPT_DISABLE)
        return EINVAL;

    if (op->evict_thd_run && value == OPT_DISABLE &&
        op->evict_intv_ms == OPT_DISABLE) {
        /* eviction thr is running. caller wants to terminate it */
        {
            Pthread_mutex_lock(&op->evict_mutex);
            op->evict_thd_run = 0;
            Pthread_cond_signal(&op->evict_cond);

            Pthread_mutex_unlock(&op->evict_mutex);
            rc = pthread_join(op->evict_thd, NULL);
        }

    } else if (!op->evict_thd_run && value != OPT_DISABLE) {
        /* eviction thr isn't running and caller wants to start it */
        Pthread_mutex_lock(&op->evict_mutex);

        rc = pthread_create(&op->evict_thd, NULL, eviction_thread, op);
        Pthread_mutex_unlock(&op->evict_mutex);
    }

    if (rc == 0)
        op->evict_ratio = value;
    return rc;
}

static int opt_min_idles(comdb2_objpool_t op, int value)
{
    if (value < 0 && value != OPT_DISABLE)
        return EINVAL;
    op->min_idles = value;
    return 0;
}

static int opt_min_idle_ratio(comdb2_objpool_t op, int value)
{
    if ((value < 0 || value > 100) && value != OPT_DISABLE)
        return EINVAL;
    op->min_idle_ratio = value;
    return 0;
}

static int opt_idle_time_ms(comdb2_objpool_t op, int value)
{
    if (value < 0 && value != OPT_DISABLE)
        return EINVAL;
    op->idle_time_ms = value;
    return 0;
}

static const char *objpool_type_name(comdb2_objpool_t op)
{
    switch (op->type) {
    case OP_LIFO:
        return "LIFO";
    case OP_FIFO:
        return "FIFO";
    case OP_RAND:
        return "RAND";
    }
    return "UNKNOWN";
}
// ^^^^^^static function impl
