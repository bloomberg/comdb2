#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>

#include "errstat.h"
#include "comdb2uuid.c"

/* stand-ins for the server symbols cron.c uses */
#define CRON_UNIT_TEST
#define logmsg(lvl, ...) fprintf(stderr, __VA_ARGS__)
#define Pthread_cond_destroy pthread_cond_destroy
#define Pthread_cond_init pthread_cond_init
#define Pthread_cond_signal pthread_cond_signal
#define Pthread_create pthread_create
#define Pthread_mutex_destroy pthread_mutex_destroy
#define Pthread_mutex_init pthread_mutex_init
#define Pthread_mutex_lock pthread_mutex_lock
#define Pthread_mutex_unlock pthread_mutex_unlock
#define Pthread_rwlock_init pthread_rwlock_init
#define Pthread_rwlock_rdlock pthread_rwlock_rdlock
#define Pthread_rwlock_unlock pthread_rwlock_unlock
#define Pthread_rwlock_wrlock pthread_rwlock_wrlock

void errstat_set_rc(errstat_t *err, unsigned int rc)
{
    err->errval = rc;
}

void errstat_set_strf(errstat_t *err, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(err->errstr, sizeof(err->errstr), fmt, ap);
    va_end(ap);
}

#define BDB_ATTR_CRON_IDLE_SECS 0
struct dbenv {
    void *bdb_attr;
};
static struct dbenv dbenv;
static struct dbenv *thedb = &dbenv;
static int gbl_exit;
static pthread_attr_t gbl_pthread_attr_detached;

static int db_is_exiting(void)
{
    return __atomic_load_n(&gbl_exit, __ATOMIC_SEQ_CST);
}

static int bdb_attr_get(void *attr, int which)
{
    return 1;
}

#include "cron.c"

static int fake_now;

static void set_now(int now)
{
    __atomic_store_n(&fake_now, now, __ATOMIC_SEQ_CST);
    cron_signal_all();
}

static int fake_is_exec_time(sched_if_t *impl, cron_event_t *event)
{
    return event->epoch <= __atomic_load_n(&fake_now, __ATOMIC_SEQ_CST);
}

static int fake_wait_next_event(sched_if_t *impl, cron_event_t *event)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 20 * 1000 * 1000;
    if (ts.tv_nsec >= 1000000000) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000;
    }
    return cron_timedwait(impl->sched, &ts);
}

#define FAIL(...)                                                                                                      \
    do {                                                                                                               \
        fprintf(stderr, "FAIL %s:%d: ", __func__, __LINE__);                                                           \
        fprintf(stderr, __VA_ARGS__);                                                                                  \
        fprintf(stderr, "\n");                                                                                         \
        exit(1);                                                                                                       \
    } while (0)

#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c))                                                                                                      \
            FAIL("%s", #c);                                                                                            \
    } while (0)

/* each event's arg4 (not owned by cron) points at one of these */
typedef struct {
    int id;
    int block;    /* wait on the gate before returning */
    int requeue;  /* re-add itself at epoch+1 this many times */
} ev_t;

static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cd = PTHREAD_COND_INITIALIZER;
static int ran[1024];
static int nran;
static int blocked;
static int gate_open;

static void reset(void)
{
    pthread_mutex_lock(&lk);
    nran = 0;
    blocked = 0;
    gate_open = 0;
    pthread_mutex_unlock(&lk);
}

static void *cb(cron_event_t *event, struct errstat *err)
{
    ev_t *ev = event->arg4;
    pthread_mutex_lock(&lk);
    if (ev->block) {
        blocked = 1;
        pthread_cond_broadcast(&cd);
        while (!gate_open)
            pthread_cond_wait(&cd, &lk);
    }
    if (nran < (int)(sizeof(ran) / sizeof(ran[0])))
        ran[nran] = ev->id;
    nran++;
    pthread_cond_broadcast(&cd);
    pthread_mutex_unlock(&lk);

    if (ev->requeue > 0) {
        ev->requeue--;
        struct errstat xerr = {0};
        CHECK(cron_add_event(event->schedif->sched, NULL, event->epoch + 1, cb, NULL, NULL, NULL, ev, NULL, &xerr,
                             NULL) != NULL);
    }
    return NULL;
}

static void wait_ran(int n)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 10;
    pthread_mutex_lock(&lk);
    while (nran < n)
        if (pthread_cond_timedwait(&cd, &lk, &ts))
            FAIL("timed out waiting for %d events, ran %d", n, nran);
    pthread_mutex_unlock(&lk);
}

static void wait_blocked(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 10;
    pthread_mutex_lock(&lk);
    while (!blocked)
        if (pthread_cond_timedwait(&cd, &lk, &ts))
            FAIL("timed out waiting for a blocked event");
    pthread_mutex_unlock(&lk);
}

static void open_gate(void)
{
    pthread_mutex_lock(&lk);
    gate_open = 1;
    pthread_cond_broadcast(&cd);
    pthread_mutex_unlock(&lk);
}

static int get_nran(void)
{
    pthread_mutex_lock(&lk);
    int n = nran;
    pthread_mutex_unlock(&lk);
    return n;
}

static void settle(void)
{
    usleep(150 * 1000);
}

static void check_ran(const int *ids, int n)
{
    settle();
    pthread_mutex_lock(&lk);
    if (nran != n)
        FAIL("ran %d events, expected %d", nran, n);
    for (int i = 0; i < n; i++)
        if (ran[i] != ids[i])
            FAIL("event %d was id %d, expected %d", i, ran[i], ids[i]);
    pthread_mutex_unlock(&lk);
}

static void check_queued(cron_sched_t *sched, const int *epochs, int n)
{
    systable_cron_events_t *arr = NULL;
    int narr = 0, nsize = 0;
    CHECK(cron_systable_sched_events_collect(sched, &arr, &narr, &nsize) == 0);
    if (narr != n)
        FAIL("%d events queued, expected %d", narr, n);
    for (int i = 0; i < n; i++)
        if ((int)arr[i].epoch != epochs[i])
            FAIL("queued event %d has epoch %d, expected %d", i, (int)arr[i].epoch, epochs[i]);
    cron_systable_events_free(arr, narr);
}

static cron_sched_t *new_sched(const char *name, enum cron_type type)
{
    sched_if_t impl = {0};
    impl.type = type;
    impl.is_exec_time = fake_is_exec_time;
    impl.wait_next_event = fake_wait_next_event;

    /* create with an event that never runs, then drop it */
    static ev_t never = {.id = -1};
    struct errstat err = {0};
    cron_sched_t *sched = cron_add_event(NULL, name, 1 << 30, cb, NULL, NULL, NULL, &never, NULL, &err, &impl);
    CHECK(sched);
    cron_clear_queue(sched);
    CHECK(cron_sched_byname(name) == sched);
    return sched;
}

static void add(cron_sched_t *sched, int epoch, ev_t *ev, uuid_t *source_id)
{
    struct errstat err = {0};
    CHECK(cron_add_event(sched, NULL, epoch, cb, strdup("a1"), NULL, NULL, ev, source_id, &err, NULL) == sched);
    CHECK(err.errval == CRON_NOERR);
}

static void test_order(void)
{
    reset();
    set_now(0);
    cron_sched_t *s = new_sched("order", CRON_TIMEPART);
    ev_t e[5] = {{.id = 0}, {.id = 1}, {.id = 2}, {.id = 3}, {.id = 4}};
    add(s, 30, &e[0], NULL);
    add(s, 10, &e[1], NULL);
    add(s, 20, &e[2], NULL);
    add(s, 10, &e[3], NULL); /* equal epochs run in insertion order */
    add(s, 40, &e[4], NULL);
    check_queued(s, (int[]){10, 10, 20, 30, 40}, 5);

    set_now(9);
    check_ran(NULL, 0);
    set_now(20);
    wait_ran(3);
    check_ran((int[]){1, 3, 2}, 3);
    set_now(100);
    wait_ran(5);
    check_ran((int[]){1, 3, 2, 0, 4}, 5);
    check_queued(s, NULL, 0);
}

static void test_clear(void)
{
    reset();
    set_now(0);
    cron_sched_t *s = new_sched("clear", CRON_TIMEPART);
    ev_t e[3] = {{.id = 0}, {.id = 1}, {.id = 2}};
    for (int i = 0; i < 3; i++)
        add(s, 10 + i, &e[i], NULL);
    cron_clear_queue(s);
    check_queued(s, NULL, 0);
    set_now(100);
    check_ran(NULL, 0);
}

static void test_clear_sourceid(void)
{
    reset();
    set_now(0);
    cron_sched_t *s = new_sched("sourceid", CRON_TIMEPART);
    uuid_t u1, u2;
    memset(u1, 1, sizeof(u1));
    memset(u2, 2, sizeof(u2));
    ev_t e[5] = {{.id = 0}, {.id = 1}, {.id = 2}, {.id = 3}, {.id = 4}};
    add(s, 10, &e[0], &u1);
    add(s, 11, &e[1], &u1);
    add(s, 12, &e[2], &u2);
    add(s, 13, &e[3], &u1);
    add(s, 14, &e[4], NULL);
    cron_clear_queue_for_sourceid(s, &u1);
    check_queued(s, (int[]){12, 14}, 2);
    set_now(100);
    wait_ran(2);
    check_ran((int[]){2, 4}, 2);
}

static void test_update(void)
{
    reset();
    set_now(0);
    cron_sched_t *s = new_sched("update", CRON_TIMEPART);
    uuid_t u1, u2, u3;
    memset(u1, 1, sizeof(u1));
    memset(u2, 2, sizeof(u2));
    memset(u3, 3, sizeof(u3));
    ev_t e[3] = {{.id = 0}, {.id = 1}, {.id = 2}};
    struct errstat err = {0};
    /* no arg1: the update overwrites it without freeing */
    CHECK(cron_add_event(s, NULL, 10, cb, NULL, NULL, NULL, &e[0], &u1, &err, NULL) == s);
    add(s, 20, &e[1], &u2);
    CHECK(cron_update_event(s, 30, cb, NULL, NULL, NULL, &e[2], u1, &err) == CRON_NOERR);
    CHECK(cron_update_event(s, 30, cb, NULL, NULL, NULL, &e[2], u3, &err) == CRON_ERR_EXIST);
    check_queued(s, (int[]){20, 30}, 2);
    set_now(100);
    wait_ran(2);
    check_ran((int[]){1, 2}, 2);
}

static void test_requeue_from_callback(void)
{
    reset();
    set_now(0);
    cron_sched_t *s = new_sched("requeue", CRON_TIMEPART);
    ev_t e = {.id = 7, .requeue = 4};
    add(s, 10, &e, NULL);
    set_now(100);
    wait_ran(5);
    check_ran((int[]){7, 7, 7, 7, 7}, 5);
    check_queued(s, NULL, 0);
}

static int locked;

static void *lock_thd(void *arg)
{
    cron_lock(arg);
    __atomic_store_n(&locked, 1, __ATOMIC_SEQ_CST);
    cron_unlock(arg);
    return NULL;
}

static void test_lock_waits_for_running(void)
{
    reset();
    set_now(0);
    cron_sched_t *s = new_sched("lockwait", CRON_TIMEPART);
    ev_t e = {.id = 0, .block = 1};
    add(s, 10, &e, NULL);
    set_now(100);
    wait_blocked();

    pthread_t t;
    locked = 0;
    pthread_create(&t, NULL, lock_thd, s);
    settle();
    CHECK(!__atomic_load_n(&locked, __ATOMIC_SEQ_CST));
    open_gate();
    pthread_join(t, NULL);
    CHECK(locked);
    check_ran((int[]){0}, 1);
}

static void test_clear_nowait(void)
{
    reset();
    set_now(0);
    cron_sched_t *s = new_sched("nowait", CRON_TIMEPART);
    ev_t e[3] = {{.id = 0, .block = 1}, {.id = 1}, {.id = 2}};
    add(s, 10, &e[0], NULL);
    add(s, 200, &e[1], NULL);
    add(s, 300, &e[2], NULL);
    set_now(100);
    wait_blocked();

    cron_clear_queue_nowait(s);
    open_gate();
    wait_ran(1);
    set_now(1000);
    check_ran((int[]){0}, 1);
    check_queued(s, NULL, 0);
}

/* An event queued with an earlier epoch while another runs goes ahead of the
 * running one; clearing without waiting must still leave only the running one. */
static void test_clear_nowait_running_not_top(void)
{
    reset();
    set_now(0);
    cron_sched_t *s = new_sched("nowait_not_top", CRON_TIMEPART);
    ev_t e[3] = {{.id = 0, .block = 1}, {.id = 1}, {.id = 2}};
    add(s, 10, &e[0], NULL);
    set_now(100);
    wait_blocked();

    add(s, 5, &e[1], NULL);
    add(s, 50, &e[2], NULL);
    cron_clear_queue_nowait(s);
    open_gate();
    wait_ran(1);
    check_ran((int[]){0}, 1);
    check_queued(s, NULL, 0);
}

static void test_clear_queue_type(void)
{
    reset();
    set_now(0);
    cron_sched_t *t = new_sched("type_time", CRON_TIMEPART);
    cron_sched_t *l = new_sched("type_logical", CRON_LOGICAL);
    ev_t e[4] = {{.id = 0}, {.id = 1}, {.id = 2}, {.id = 3}};
    add(t, 10, &e[0], NULL);
    add(l, 11, &e[1], NULL);
    cron_clear_queue_type(CRON_LOGICAL, 0);
    check_queued(t, (int[]){10}, 1);
    check_queued(l, NULL, 0);

    add(l, 12, &e[2], NULL);
    add(t, 13, &e[3], NULL);
    cron_clear_queue_type(CRON_TIMEPART, 1);
    check_queued(t, NULL, 0);
    check_queued(l, (int[]){12}, 1);
    set_now(100);
    wait_ran(1);
    check_ran((int[]){2}, 1);
}

static cron_sched_t *stress_sched;
static int stress_done;

static void *stress_cb(cron_event_t *event, struct errstat *err)
{
    if (rand() % 4 == 0)
        usleep(rand() % 500);
    return NULL;
}

static void *stress_thd(void *arg)
{
    unsigned seed = (unsigned)(uintptr_t)arg;
    uuid_t u;
    for (int op = 0; op < 3000; op++) {
        int now = __atomic_load_n(&fake_now, __ATOMIC_SEQ_CST);
        int epoch = now - 5 + (int)(rand_r(&seed) % 20);
        struct errstat err = {0};
        memset(u, rand_r(&seed) % 4, sizeof(u));
        switch (rand_r(&seed) % 10) {
        case 0:
            cron_clear_queue_nowait(stress_sched);
            break;
        case 1:
            cron_clear_queue_for_sourceid(stress_sched, &u);
            break;
        case 2:
            /* cron_update_event gives every event of a source the same args, so
             * only update a source that has at most one event */
            memset(u, 0xff, sizeof(u));
            u[0] = (unsigned char)(seed & 0xff);
            u[1] = (unsigned char)(uintptr_t)arg;
            cron_clear_queue_for_sourceid(stress_sched, &u);
            /* no arg1: the update overwrites it without freeing */
            cron_add_event(stress_sched, NULL, epoch, stress_cb, NULL, NULL, NULL, NULL, &u, &err, NULL);
            cron_update_event(stress_sched, epoch + 1, stress_cb, strdup("u"), NULL, NULL, NULL, u, &err);
            break;
        case 3:
            if (rand_r(&seed) % 10 == 0)
                cron_clear_queue(stress_sched);
            break;
        default:
            cron_add_event(stress_sched, NULL, epoch, stress_cb, strdup("s"), NULL, NULL, NULL, &u, &err, NULL);
        }
    }
    __atomic_add_fetch(&stress_done, 1, __ATOMIC_SEQ_CST);
    return NULL;
}

static void test_stress(void)
{
    set_now(0);
    stress_sched = new_sched("stress", CRON_TIMEPART);
    pthread_t t[4];
    for (int i = 0; i < 4; i++)
        pthread_create(&t[i], NULL, stress_thd, (void *)(uintptr_t)(i + 1));
    for (int now = 1; __atomic_load_n(&stress_done, __ATOMIC_SEQ_CST) < 4; now++) {
        set_now(now);
        usleep(1000);
    }
    for (int i = 0; i < 4; i++)
        pthread_join(t[i], NULL);
    cron_clear_queue(stress_sched);
    check_queued(stress_sched, NULL, 0);
}

int main(int argc, char **argv)
{
    pthread_attr_init(&gbl_pthread_attr_detached);
    pthread_attr_setdetachstate(&gbl_pthread_attr_detached, PTHREAD_CREATE_DETACHED);
    init_cron();

    struct {
        const char *name;
        void (*fn)(void);
    } tests[] = {
        {"order", test_order},
        {"clear", test_clear},
        {"clear_sourceid", test_clear_sourceid},
        {"update", test_update},
        {"requeue_from_callback", test_requeue_from_callback},
        {"lock_waits_for_running", test_lock_waits_for_running},
        {"clear_nowait", test_clear_nowait},
        {"clear_nowait_running_not_top", test_clear_nowait_running_not_top},
        {"clear_queue_type", test_clear_queue_type},
        {"stress", test_stress},
    };
    for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
        if (argc > 1 && strcmp(argv[1], tests[i].name))
            continue;
        printf("%s\n", tests[i].name);
        fflush(stdout);
        tests[i].fn();
    }

    __atomic_store_n(&gbl_exit, 1, __ATOMIC_SEQ_CST);
    cron_signal_all();
    settle();
    printf("Success\n");
    return 0;
}
