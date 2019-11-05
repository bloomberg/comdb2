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
#ifdef NDEBUG
#undef NDEBUG
#endif
#include "limit_fortify.h"

#ifndef MYBDB5
#include "bdb_api.h"
#include "bdb_int.h"
FILE *io_override_get_std(void);
int io_override_set_std(FILE *);
#define BDB5RET
#else
typedef unsigned long u_long;
typedef unsigned int u_int;
#define io_override_get_std() NULL
#define io_override_set_std(x)
#define BDB5RET 0
#endif

#include <build/db.h>
#include <unistd.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/time.h>
#include <assert.h>
#include <pthread.h>
#include <gettimeofday_ms.h>
#include <errno.h>
#include <logmsg.h>
#include <locks_wrap.h>

extern void berkdb_dump_lockers_summary(DB_ENV *);

static pthread_attr_t locktest_attr;
static DB_ENV *dbenv = NULL;
extern uint64_t detect_skip;
extern uint64_t detect_run;
static uint64_t counter;
static uint64_t deadlock;

static const char *counter_print(void)
{
    static char msg[1024];
    sprintf(msg, "counter:%-5" PRIu64 " deadlock:%-5" PRIu64
                 " detect_skip:%-5" PRIu64 " detect_run:%-5" PRIu64,
            counter, deadlock, detect_skip, detect_run);
    return msg;
}

#define PRINT_HEADER(x) logmsg(LOGMSG_USER, "%15s:: ", x)
#define PRINT_COUNTER()                                                        \
    logmsg(LOGMSG_USER, "time:%5" PRIu64 "ms %s", end - begin, counter_print())
#define TIME_IT(x)                                                             \
    do {                                                                       \
        reset_counters();                                                      \
        uint64_t begin = gettimeofday_ms();                                    \
        x();                                                                   \
        uint64_t end = gettimeofday_ms();                                      \
        PRINT_HEADER(#x);                                                      \
        PRINT_COUNTER();                                                       \
        logmsg(LOGMSG_USER, "\n");                                                 \
    } while (0)

#ifndef MYBDB5
static void *detect(void *_)
{
    int aborted = 0;
    u_int32_t policy;
    int rc;

    dbenv->get_lk_detect(dbenv, &policy);
    rc = dbenv->lock_detect(dbenv, 0, policy, &aborted);
    if (rc) {
        logmsg(LOGMSG_ERROR, "lock_detect rc:%d\n", rc);
    }

    return NULL;
}

void bdb_detect(void *_bdb_state)
{
    bdb_state_type *bdb_state = _bdb_state;
    dbenv = bdb_state->dbenv;
    pthread_t t;
    extern pthread_attr_t gbl_pthread_attr_detached;
    pthread_create(&t, &gbl_pthread_attr_detached, detect, NULL);
}

void bdb_locker_summary(void *_bdb_state)
{
    bdb_state_type *bdb_state = _bdb_state;
    berkdb_dump_lockers_summary(bdb_state->dbenv);
}
#endif

#define THDS 100
#define ITER 1000
#define DBTs 100000
#define THD_GRPS 10
#define NLOCKS 500

uint64_t diffs[THDS];

typedef struct {
    uint8_t fluff[28];
} Block28;

typedef struct {
    int tid;
    DBT *obj;
    int num_obj;
    db_lockmode_t mode;
} LockMgrArg;

static void *test_lockmgr(void *_arg)
{
    LockMgrArg *arg = _arg;
    uint64_t start, end;
    int i, j;
    u_int32_t locker;
    DB_LOCK lock[DBTs];
    ssize_t rc, rc1 = 0;
    DB_LOCKREQ put_all = {0};

    start = gettimeofday_ms();

    rc = dbenv->lock_id(dbenv, &locker);
    if (rc) {
        logmsg(LOGMSG_ERROR, "lock_id rc: %zu\n", rc);
        goto out;
    }

    for (i = 0; i < ITER; ++i) {
    again:
        for (j = 0; j < arg->num_obj; ++j) {
            rc = dbenv->lock_get(dbenv, locker, 0, &arg->obj[j], arg->mode,
                                 &lock[j]);
            if (rc) {
                put_all.op = DB_LOCK_PUT_ALL;
                int rc_ = dbenv->lock_vec(dbenv, locker, 0, &put_all, 1, NULL);
                if (rc_) {
                    logmsg(LOGMSG_ERROR, "%s lock_vec rc:%d\n", __func__, rc_);
                    goto id_free;
                }
                if (rc != DB_LOCK_DEADLOCK)
                    goto id_free;
                else
                    ++deadlock;
                goto again;
            }
        }
        ++counter;
        put_all.op = DB_LOCK_PUT_ALL;
        rc = dbenv->lock_vec(dbenv, locker, 0, &put_all, 1, NULL);
        if (rc) {
            goto id_free;
        }
    }

id_free:
    rc1 = dbenv->lock_id_free(dbenv, locker);
    if (rc1) {
        logmsg(LOGMSG_ERROR, "lock_id_free rc: %zu\n", rc1);
    }

out:
    end = gettimeofday_ms();
    diffs[arg->tid] = end - start;
    rc |= rc1;
    return (void *)rc;
}

/* All banging on the same lock */
static void test_same_lock(db_lockmode_t mode)
{
    int fail;
    ssize_t rc;
    size_t i;
    pthread_t t[THDS];
    Block28 data;
    DBT obj = {.data = &data, .size = sizeof(data)};
    LockMgrArg arg[THDS];

    for (i = 0; i < THDS; ++i) {
        arg[i].obj = &obj;
        arg[i].num_obj = 1;
        arg[i].mode = mode;
        arg[i].tid = i;
        pthread_create(&t[i], &locktest_attr, test_lockmgr, &arg[i]);
    }
    fail = 0;
    for (i = 0; i < THDS; ++i) {
        pthread_join(t[i], (void **)&rc);
        if (rc)
            ++fail;
    }
    if (fail) {
        logmsg(LOGMSG_ERROR, "fail: %d ", fail);
    } else {
        logmsg(LOGMSG_USER, "pass ");
    }
}

/* Every thread gets n write locks */
static void test_n_locks(const int n, db_lockmode_t mode)
{
    int fail;
    ssize_t rc;
    size_t i, j;
    pthread_t t[THDS];
    Block28 datas[THDS * n];
    DBT objs[THDS * n]; // each thread gets n locks
    LockMgrArg args[THDS];

    Block28 *data;
    DBT *obj;
    LockMgrArg *arg;

    assert(THDS % THD_GRPS == 0);
    assert((THDS / THD_GRPS) * n <= DBTs);
    bzero(datas, sizeof(datas));

    i = 0;
    while (i < THDS) {
        for (j = 0; j < n; ++j) {
            size_t p = (i * n) + j;
            data = &datas[p];
            *(int *)data = p;

            obj = &objs[p];
            obj->data = data;
            obj->size = sizeof(*data);
        }
        arg = &args[i];
        arg->obj = &objs[i * n];
        arg->num_obj = n;
        arg->mode = mode;
        arg->tid = i;

        for (j = 0; j < THD_GRPS; ++j) {
            pthread_create(&t[i++], &locktest_attr, test_lockmgr, arg);
        }
    }
    fail = 0;
    for (i = 0; i < THDS; ++i) {
        pthread_join(t[i], (void **)&rc);
        if (rc)
            ++fail;
    }
    if (fail) {
        logmsg(LOGMSG_ERROR, "fail: %d ", fail);
    } else {
        logmsg(LOGMSG_USER, "pass ");
    }
}

static void test_diff_lock(db_lockmode_t mode)
{
    int fail;
    ssize_t rc;
    size_t i;
    pthread_t t[THDS];

    Block28 datas[THDS];
    DBT objs[THDS];
    LockMgrArg args[THDS];

    Block28 *data;
    DBT *obj;
    LockMgrArg *arg;

    for (i = 0; i < THDS; ++i) {
        data = &datas[i];
        obj = &objs[i];
        arg = &args[i];
        *(size_t *)data = i;
        obj->data = data;
        obj->size = sizeof(datas[i]);
        arg->obj = obj;
        arg->mode = mode;
        arg->num_obj = 1;
        arg->tid = i;
        pthread_create(&t[i], &locktest_attr, test_lockmgr, arg);
    }
    fail = 0;
    for (i = 0; i < THDS; ++i) {
        pthread_join(t[i], (void **)&rc);
        if (rc)
            ++fail;
    }
    if (fail) {
        logmsg(LOGMSG_ERROR, "fail: %d ", fail);
    } else {
        logmsg(LOGMSG_USER, "pass ");
    }
}

static int sleep_sec = 5;
static int stop;
static void *test_get_put(void *_mode)
{
    db_lockmode_t mode = (db_lockmode_t)_mode;
    uint64_t start, end;
    u_int32_t locker;
    DB_LOCK lock;
    uint8_t data[28];
    size_t rc, rc1;
    DBT obj = {.data = &data, .size = sizeof(data)};

    rc = dbenv->lock_id(dbenv, &locker);
    if (rc) {
        logmsg(LOGMSG_ERROR, "lock_id rc: %zu\n", rc);
        return (void *)rc;
    }

    start = gettimeofday_ms();
    while (!stop) {
        rc = dbenv->lock_get(dbenv, locker, 0, &obj, mode, &lock);
        if (rc) {
            logmsg(LOGMSG_ERROR, "lock_get rc: %zu\n", rc);
            break;
        }
        ++counter;
        rc = dbenv->lock_put(dbenv, &lock);
        if (rc) {
            logmsg(LOGMSG_ERROR, "lock_get rc: %zu\n", rc);
            break;
        }
    }
    end = gettimeofday_ms();
    logmsg(LOGMSG_USER, "diff: %" PRIu64 "ms (%.2fs) ", end - start,
           (end - start) / 1000.0);
    rc1 = dbenv->lock_id_free(dbenv, locker);
    if (rc1) {
        logmsg(LOGMSG_ERROR, "lock_id_free rc: %zu\n", rc1);
    }
    rc |= rc1;
    return (void *)rc;
}

static void test_lock_per_sec(db_lockmode_t mode)
{
    pthread_t t;
    ssize_t *rc;
    stop = 0;
    pthread_create(&t, &locktest_attr, test_get_put, (void *)mode);
    sleep(sleep_sec);
    stop = 1;
    pthread_join(t, (void **)&rc);
    if (rc)
        logmsg(LOGMSG_ERROR, "fail: %zu ", (ssize_t)rc);
    else
        logmsg(LOGMSG_USER, "pass ");
}

static void reset_counters()
{
    deadlock = detect_skip = detect_run = counter = 0;
    for (int i = 0; i < THDS; ++i) {
        diffs[i] = 0;
    }
}

static void print_counters()
{
    uint64_t diff = 0;
    for (int i = 0; i < THDS; ++i) {
        diff += diffs[i];
    }
    logmsg(LOGMSG_USER,
           "detect_skip:%" PRIu64 " detect_run:%" PRIu64 " counter:%" PRIu64
           " deadlock:%" PRIu64 " time:%.2fms\n",
           detect_skip, detect_run, counter, deadlock, (double)diff / THDS);
}

static void test_n_locks_rd(void)
{
    test_n_locks(NLOCKS, DB_LOCK_READ);
    print_counters();
}

static void test_n_locks_wr(void)
{
    test_n_locks(NLOCKS, DB_LOCK_WRITE);
    print_counters();
}

static void mytester()
{
    /*
    reset_counters();
    fprintf(stderr,"test_lock_per_sec DB_LOCK_READ ");
    test_lock_per_sec(DB_LOCK_READ);
    fprintf(stderr,"locks/sec: %lu ", counter / sleep_sec);
    print_counters();

    reset_counters();
    fprintf(stderr,"test_lock_per_sec DB_LOCK_WRITE ");
    test_lock_per_sec(DB_LOCK_WRITE);
    fprintf(stderr,"locks/sec: %lu ", counter / sleep_sec);
    print_counters();
    */

    reset_counters();
    logmsg(LOGMSG_USER, "test_n_locks locks: %d DB_LOCK_READ ", NLOCKS);
    TIME_IT(test_n_locks_rd);

    reset_counters();
    logmsg(LOGMSG_USER, "test_n_locks locks: %d DB_LOCK_WRITE ", NLOCKS);
    TIME_IT(test_n_locks_wr);

    reset_counters();
    logmsg(LOGMSG_USER, "test_same_lock DB_LOCK_READ ");
    test_same_lock(DB_LOCK_READ);
    print_counters();

    reset_counters();
    logmsg(LOGMSG_USER, "test_diff_lock DB_LOCK_READ ");
    test_diff_lock(DB_LOCK_READ);
    print_counters();

    reset_counters();
    logmsg(LOGMSG_USER, "test_same_lock DB_LOCK_WRITE ");
    test_same_lock(DB_LOCK_WRITE);
    print_counters();

    reset_counters();
    logmsg(LOGMSG_USER, "test_diff_lock DB_LOCK_WRITE ");
    test_diff_lock(DB_LOCK_WRITE);
    print_counters();
}

static int lock_id(u_int32_t *locker_id)
{
    int ret;
    ret = dbenv->lock_id(dbenv, locker_id);
#ifdef PRINT_NON_ZERO_RET
    if (ret) {
        logmsg(LOGMSG_ERROR, "%s ret: %d\n", __func__, ret);
    }
#endif
    return ret;
}

static int lock_get(u_int32_t locker_id, int obj, db_lockmode_t mode,
                    DB_LOCK *lock)
{
    int ret;
    DBT o = {.data = &obj, .size = sizeof(obj)};
    ret = dbenv->lock_get(dbenv, locker_id, 0, &o, mode, lock);
#ifdef PRINT_NON_ZERO_RET
    if (ret) {
        logmsg(LOGMSG_ERROR, "%s locker_id: %d ret: %d\n", __func__, locker_id, ret);
    }
#endif
    return ret;
}

static int lock_put(DB_LOCK *lock)
{
    int ret;
    ret = dbenv->lock_put(dbenv, lock);
#ifdef PRINT_NON_ZERO_RET
    if (ret) {
        logmsg(LOGMSG_ERROR, "%s ret: %d\n", __func__, ret);
    }
#endif
    return ret;
}

static int lock_id_free(u_int32_t locker_id)
{
    int ret;
    ret = dbenv->lock_id_free(dbenv, locker_id);
#ifdef PRINT_NON_ZERO_RET
    if (ret) {
        logmsg(LOGMSG_ERROR, "%s ret: %d\n", __func__, ret);
    }
#endif
    return ret;
}

static int lock_vec(u_int32_t locker, DB_LOCKREQ list[], int nlist,
                    DB_LOCKREQ **elistp)
{
    int ret;
    ret = dbenv->lock_vec(dbenv, locker, 0, list, nlist, elistp);
#ifdef PRINT_NON_ZERO_RET
    if (ret) {
        logmsg(LOGMSG_ERROR, "%s ret: %d\n", __func__, ret);
    }
#endif
    return ret;
}

typedef void *(tester_routine)(void *);

typedef struct {
    int num;
    int id;
} tester_arg;

static void *clump_tester(void *_)
{
    u_int32_t locker_id;
    DB_LOCK lock1;
    DB_LOCK lock2;

    if (lock_id(&locker_id))
        return (void *)199;
    if (lock_get(locker_id, 10, DB_LOCK_READ, &lock1))
        return (void *)299;

    sleep(3);

    switch (lock_get(locker_id, 10, DB_LOCK_WRITE, &lock2)) {
    case 0:
        ++counter;
        break;
    case DB_LOCK_DEADLOCK:
        ++deadlock;
        assert(lock_put(&lock1) == 0);
        assert(lock_id_free(locker_id) == 0);
        return (void *)1;
        break;
    default:
        return (void *)399;
        break;
    }
    assert(lock_put(&lock1) == 0);
    assert(lock_put(&lock2) == 0);
    assert(lock_id_free(locker_id) == 0);
    return (void *)0;
}

static void *ring_tester(void *arg_)
{
    tester_arg *arg = arg_;
    u_int32_t locker_id;
    DB_LOCK lock1;
    DB_LOCK lock2;

    if (lock_id(&locker_id))
        return (void *)199;

    int obj = arg->id;
    if (lock_get(locker_id, obj, DB_LOCK_WRITE, &lock1))
        return (void *)299;

    sleep(3);

    obj = (obj + 1) % arg->num;
    switch (lock_get(locker_id, obj, DB_LOCK_WRITE, &lock2)) {
    case 0:
        ++counter;
        break;
    case DB_LOCK_DEADLOCK:
        ++deadlock;
        assert(lock_put(&lock1) == 0);
        assert(lock_id_free(locker_id) == 0);
        return (void *)1;
        break;
    default:
        return (void *)399;
        break;
    }
    assert(lock_put(&lock1) == 0);
    assert(lock_put(&lock2) == 0);
    assert(lock_id_free(locker_id) == 0);
    return (void *)0;
}

static ssize_t tester(const char *name, size_t num, tester_routine *routine)
{
    tester_arg arg[num];
    pthread_t t[num];
    intptr_t ret[num];
    ssize_t s = 0;
    int i;

    logmsg(LOGMSG_USER, "%s threads:%zu\n", name, num);
    for (i = 0; i < num; ++i) {
        arg[i].id = i;
        arg[i].num = num;
        pthread_create(&t[i], &locktest_attr, routine, &arg[i]);
    }
    for (i = 0; i < num; ++i) {
        pthread_join(t[i], (void **)&ret[i]);
    }
    for (i = 0; i < num; ++i) {
        s += (ssize_t)ret[i];
    }
    return s;
}

#define arraylen(x) (sizeof((x)) / sizeof((x)[0]))

static void berkdb_tester(void)
{
    int i;
    int rc;
    size_t num[] = {2, 4, 10, 50, 100, 200};
    size_t len = arraylen(num);
    for (i = 0; i < len; ++i) {
        rc = tester("clump", num[i], clump_tester);
        assert(rc == num[i] - 1); // all but one should ddlk
    }
    for (i = 0; i < len; ++i) {
        rc = tester("ring", num[i], ring_tester);
        assert(rc == 1); // just one should ddlk
    }
    return;
}

static void locks_per_sec(void)
{
    u_int32_t locker_id;
    DB_LOCK lock;
    int obj = 0;
    size_t n, num;
    n = num = 1000000;

    assert(lock_id(&locker_id) == 0);
    PRINT_HEADER(__func__);
    uint64_t begin = gettimeofday_ms();
    while (--num) {
        assert(lock_get(locker_id, obj, DB_LOCK_WRITE, &lock) == 0);
        assert(lock_put(&lock) == 0);
    }
    uint64_t end = gettimeofday_ms();
    assert(lock_id_free(locker_id) == 0);
    double rate = n / ((end - begin) / 1000.0);
    PRINT_COUNTER();
    logmsg(LOGMSG_USER, "rate:%.2f\n", rate);
}

typedef struct {
    int nlocks;
    int start;
    int step;
    int retry; /* out param */
} GetLocksArg;

static void *get_locks(void *arg_)
{
    GetLocksArg *arg = (GetLocksArg *)arg_;
    int nlocks = arg->nlocks;
    int start = arg->start;
    int step = arg->step;
    int o;
    int i;
    int rc;
    unsigned retry = 1;
    u_int32_t locker_id;
    DB_LOCK lock[nlocks];

    if (lock_id(&locker_id))
        return (void *)199;
again:
    for (i = 0, o = start; i < nlocks; ++i, o += step) {
        rc = lock_get(locker_id, o, DB_LOCK_WRITE, &lock[i]);
        if (rc) {
            if (rc != DB_LOCK_DEADLOCK)
                logmsg(LOGMSG_ERROR, "%s locker:%u lock_get rc:%d\n", __func__,
                        locker_id, rc);
            else
                ++deadlock;
            goto put;
        }
    }
    ++counter;
    arg->retry = retry - 1;
    retry = 0;

put:
    rc = 0;
    DB_LOCKREQ put_all = {0};
    put_all.op = DB_LOCK_PUT_ALL;
    rc = lock_vec(locker_id, &put_all, 1, NULL);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s lock_vec rc:%d\n", __func__, rc);
        retry = 0;
    }
    if (retry++)
        goto again;

    rc = lock_id_free(locker_id);
    if (rc)
        return (void *)299;
    return (void *)0;
}

static void get_locks_wrapper(int argc, GetLocksArg argv[])
{
    pthread_t t[argc];
    intptr_t rc[argc];
    int ret = 0;
    int i;
    int retry = 0;

    for (i = 0; i < argc; ++i) {
        pthread_create(&t[i], &locktest_attr, get_locks, &argv[i]);
    }
    for (i = 0; i < argc; ++i) {
        pthread_join(t[i], (void **)&rc[i]);
        ret += rc[i];
        retry += argv[i].retry;
    }
    assert(ret == 0);
}

static void sprint(void)
{
    GetLocksArg arg[100];
    const size_t n = arraylen(arg);
    size_t i;
    for (i = 0; i < n; ++i) {
        arg[i].start = 0;
        arg[i].nlocks = 10000;
        arg[i].step = 1;
    }
    get_locks_wrapper(n, arg);
}

static void collisionA(void)
{
    GetLocksArg arg[] = {{.start = 0, .nlocks = 50000, .step = 1},
                         {.start = 50000, .nlocks = 50000, .step = -1}};
    get_locks_wrapper(arraylen(arg), arg);
}

static void collisionB(void)
{
    GetLocksArg arg[] = {{.start = 0, .nlocks = 10000, .step = 1},
                         {.start = 10000, .nlocks = 10000, .step = -1}

                         ,
                         {.start = 10000, .nlocks = 10000, .step = 1},
                         {.start = 20000, .nlocks = 10000, .step = -1}

                         ,
                         {.start = 20000, .nlocks = 10000, .step = 1},
                         {.start = 30000, .nlocks = 10000, .step = -1}

                         ,
                         {.start = 30000, .nlocks = 10000, .step = 1},
                         {.start = 40000, .nlocks = 10000, .step = -1}

                         ,
                         {.start = 40000, .nlocks = 10000, .step = 1},
                         {.start = 50000, .nlocks = 10000, .step = -1}};
    get_locks_wrapper(arraylen(arg), arg);
}

static void collisionC(void)
{
    GetLocksArg arg[] = {{.start = 0, .nlocks = 5000, .step = 1},
                         {.start = 5000, .nlocks = 5000, .step = -1}

                         ,
                         {.start = 5000, .nlocks = 5000, .step = 1},
                         {.start = 10000, .nlocks = 5000, .step = -1}

                         ,
                         {.start = 10000, .nlocks = 5000, .step = 1},
                         {.start = 15000, .nlocks = 5000, .step = -1}

                         ,
                         {.start = 15000, .nlocks = 5000, .step = 1},
                         {.start = 20000, .nlocks = 5000, .step = -1}

                         ,
                         {.start = 20000, .nlocks = 5000, .step = 1},
                         {.start = 25000, .nlocks = 5000, .step = -1}

                         ,
                         {.start = 25000, .nlocks = 5000, .step = 1},
                         {.start = 30000, .nlocks = 5000, .step = -1}

                         ,
                         {.start = 30000, .nlocks = 5000, .step = 1},
                         {.start = 35000, .nlocks = 5000, .step = -1}

                         ,
                         {.start = 35000, .nlocks = 5000, .step = 1},
                         {.start = 40000, .nlocks = 5000, .step = -1}

                         ,
                         {.start = 40000, .nlocks = 5000, .step = 1},
                         {.start = 45000, .nlocks = 5000, .step = -1}

                         ,
                         {.start = 45000, .nlocks = 5000, .step = 1},
                         {.start = 50000, .nlocks = 5000, .step = -1}};
    get_locks_wrapper(arraylen(arg), arg);
}

static void stripe(void)
{
    GetLocksArg arg[] = {{.start = 0, .nlocks = 10000, .step = 1},
                         {.start = 10000, .nlocks = 10000, .step = 1},
                         {.start = 20000, .nlocks = 10000, .step = 1},
                         {.start = 30000, .nlocks = 10000, .step = 1},
                         {.start = 40000, .nlocks = 10000, .step = 1}};
    get_locks_wrapper(arraylen(arg), arg);
}

#define shuffle(x)                                                             \
    do {                                                                       \
        int i;                                                                 \
        unsigned int seed = pthread_self();                                    \
        for (i = 0; i < arraylen(x) - 1; ++i) {                                \
            int j = i + (rand_r(&seed) % (arraylen(x) - i));                   \
            uint8_t tmp[sizeof(x[0])];                                         \
            memcpy(tmp, &x[i], sizeof(tmp));                                   \
            memcpy(&x[i], &x[j], sizeof(tmp));                                 \
            memcpy(&x[j], tmp, sizeof(tmp));                                   \
        }                                                                      \
    } while (0)
#define NUMLOCKS 5
static uint8_t p_lockdesc[NUMLOCKS][28];
static uint8_t c_lockdesc[NUMLOCKS][28];
static int lockvec_as(void *lockdesc, uint32_t id)
{
    int i;
    uint8_t mylock[arraylen(p_lockdesc)][sizeof(p_lockdesc[0])];
    memcpy(mylock, lockdesc, sizeof(mylock));
    shuffle(mylock);

    DBT mydbt[arraylen(p_lockdesc)];
    DB_LOCKREQ list[arraylen(mylock)];
    for (i = 0; i < arraylen(mydbt); ++i) {
        mydbt[i].data = &mylock[i];
        mydbt[i].size = sizeof(mylock[i]);
        list[i].op = DB_LOCK_GET;
        list[i].obj = &mydbt[i];
        list[i].mode = (id % 5) ? DB_LOCK_READ : DB_LOCK_WRITE;
    }
    return dbenv->lock_vec(dbenv, id, 0, list, i, NULL);
}
static void *lockvec(void *_)
{
    int num = 0;
    while (num++ < 20000) {
        int prc, crc;
        DB_TXN *parent, *child;
        dbenv->txn_begin(dbenv, NULL, &parent, 0);
        dbenv->txn_begin(dbenv, parent, &child, 0);
        if ((crc = lockvec_as(c_lockdesc, child->txnid)) == 0) {
            ++counter;
            child->commit(child, DB_TXN_NOSYNC);
        } else {
            if (crc == DB_LOCK_DEADLOCK) {
                ++deadlock;
            } else {
                logmsg(LOGMSG_USER, "crc:%d\n", crc);
            }
            child->abort(child);
        }
        if ((prc = lockvec_as(p_lockdesc, parent->txnid)) == 0) {
            ++counter;
            parent->commit(parent, DB_TXN_NOSYNC);
        } else {
            if (prc == DB_LOCK_DEADLOCK) {
                ++deadlock;
            } else {
                logmsg(LOGMSG_USER, "prc:%d\n", prc);
            }
            parent->abort(parent);
        }
    }
    return NULL;
}
static void genlockdesc(void)
{
    int rc;
    memset(p_lockdesc, 0, sizeof(p_lockdesc));
    memset(c_lockdesc, 0, sizeof(c_lockdesc));
    FILE *urandom;
    if ((urandom = fopen("/dev/urandom", "r")) == NULL) {
        logmsgperror("fopen");
        return;
    }
    if ((rc = fread(c_lockdesc, sizeof(c_lockdesc[0]), arraylen(c_lockdesc),
                    urandom)) != arraylen(c_lockdesc) &&
        ferror(urandom)) {
        logmsgperror("fread");
    }
    if ((rc = fread(p_lockdesc, sizeof(p_lockdesc[0]), arraylen(p_lockdesc),
                    urandom)) != arraylen(p_lockdesc) &&
        ferror(urandom)) {
        logmsgperror("fread");
    }
    fclose(urandom);
}
static void locvec_test()
{
    // printf("%s START\n", __func__);
    genlockdesc();
    int i;
    pthread_t t[20];
    for (i = 0; i < arraylen(t); ++i) {
        pthread_create(&t[i], NULL, lockvec, NULL);
    }
    void *ret;
    for (i = 0; i < arraylen(t); ++i) {
        pthread_join(t[i], &ret);
    }
    // printf("%s FINISH\n", __func__);
}

static void *locktest(void *f)
{
    io_override_set_std(f);
    TIME_IT(berkdb_tester);
    TIME_IT(sprint);
    TIME_IT(stripe);
    TIME_IT(collisionA);
    TIME_IT(collisionB);
    TIME_IT(collisionC);
    // locks_per_sec();
    // mytester();
    TIME_IT(locvec_test);
    return NULL;
}

#ifdef MYBDB5
int main(int argc, char *argv[])
{
    int rc;
    u_int32_t flags;

    /* New Env */
    flags = 0;
    rc = db_env_create(&dbenv, flags);
    if (rc) {
        fprintf(stderr, "db_env_create failed\n");
        return rc;
    }

    rc = dbenv->set_lk_partitions(dbenv, 521);
    if (rc) {
        fprintf(stderr, "set_lk_partitions failed\n");
        return rc;
    }

    rc = dbenv->set_lk_max_lockers(dbenv, 50000);
    if (rc) {
        fprintf(stderr, "set_lk_max_lockers failed\n");
        return rc;
    }

    rc = dbenv->set_lk_max_objects(dbenv, 60000);
    if (rc) {
        fprintf(stderr, "set_lk_max_objects failed\n");
        return rc;
    }

    rc = dbenv->set_lk_detect(dbenv, DB_LOCK_YOUNGEST);
    if (rc) {
        fprintf(stderr, "set_lk_detect failed\n");
        return rc;
    }

    /* Open */
    char *db_home = "/tmp/locktest";
    flags = DB_INIT_LOCK | DB_CREATE | DB_PRIVATE | DB_THREAD;
    int mode = 0;
    rc = dbenv->open(dbenv, db_home, flags, mode);
    if (rc) {
        fprintf(stderr, "open rc: %d %s\n", rc, strerror(rc));
        return rc;
    }
#else
void bdb_locktest(void *_bdb_state)
{
    bdb_state_type *bdb_state = _bdb_state;
    dbenv = bdb_state->dbenv;
#endif
    Pthread_attr_init(&locktest_attr);
    Pthread_attr_setstacksize(&locktest_attr, 3 * 1024 * 1024);
    void *ret;
    pthread_t t;
    uint64_t before = gettimeofday_ms();
    pthread_create(&t, NULL, locktest, io_override_get_std());
    pthread_join(t, &ret);
    Pthread_attr_destroy(&locktest_attr);
    uint64_t after = gettimeofday_ms();
    uint64_t diff = after - before;
    fprintf(stderr, "total runtime: %" PRIu64 "ms (%0.2fs)\n", diff,
            (double)diff / 1000.0);
    return BDB5RET;
}
