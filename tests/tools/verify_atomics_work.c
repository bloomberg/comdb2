/* Dumb test to verify that the atomics function calls that we use
 * actually do work
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <comdb2_atomic.h>

/* assert() is compiled out in release builds */
#define CHECK(cond)                                                                                                    \
    do {                                                                                                               \
        if (!(cond)) {                                                                                                 \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond);                                   \
            abort();                                                                                                   \
        }                                                                                                              \
    } while (0)

#define FORL(ii,s,e) for(int ii = s; ii < e; ii++)
#define THREADNUM 10
#define NUMTOADD 1000000

void *add_a_million(void *arg)
{
    uint32_t *a = arg;
    FORL(i, 0, NUMTOADD) {
        ATOMIC_ADD32(*a, 1);
    }
    return NULL;
}

uint32_t gbl_max;

#define MAX 1000000
void *get_max_rand(void *arg)
{
    uint32_t old;
    unsigned int seed = (unsigned int)time(NULL) ^ (unsigned int)(uintptr_t)&old;
    uint32_t mymax = 0;
    FORL(i, 0, MAX) {
        uint32_t new = rand_r(&seed);
        if (mymax < new)
            mymax = new;
        while((old = ATOMIC_LOAD32(gbl_max)) < new && !CAS32(gbl_max, old, new))
            ; // keep trying

        /* the above while loop is equivalent to the more friendly:
         int res = 0;
         while (!res && (old = ATOMIC_LOAD32(gbl_max)) < new)
            res = CAS32(gbl_max, old, new);
         */
    }
    CHECK(mymax <= ATOMIC_LOAD32(gbl_max));
    return NULL;
}

static void test_cas_semantics(void)
{
    uint32_t m32 = 5, o32 = 3;
    CHECK(!CAS32(m32, o32, 9));
    CHECK(m32 == 5);
    CHECK(o32 == 5); /* failed CAS reports the current value */
    CHECK(CAS32(m32, o32, 9));
    CHECK(m32 == 9);
    CHECK(o32 == 5);

    int si = -1, so = -1;
    CHECK(CAS32(si, so, -2));
    CHECK(si == -2);
    CHECK(!CAS32(si, so, 0));
    CHECK(so == -2);

    /* both halves must take part in the comparison */
    uint64_t m64 = 0x100000005ULL, o64 = 0x5ULL;
    CHECK(!CAS64(m64, o64, 1));
    CHECK(m64 == 0x100000005ULL);
    CHECK(o64 == 0x100000005ULL);
    CHECK(CAS64(m64, o64, 0xffffffff00000001ULL));
    CHECK(m64 == 0xffffffff00000001ULL);

    int64_t s64 = -5, so64 = -5;
    CHECK(CAS64(s64, so64, INT64_MIN));
    CHECK(s64 == INT64_MIN);

    volatile uint32_t vm = 1;
    uint32_t vo = 1;
    CHECK(CAS32(vm, vo, 2));
    CHECK(vm == 2);
}

static void test_rmw_semantics(void)
{
    uint32_t u = 7;
    CHECK(XCHANGE32(u, 11) == 7);
    CHECK(u == 11);
    CHECK(ATOMIC_ADD32(u, 4) == 15); /* returns the new value */
    CHECK(ATOMIC_LOAD32(u) == 15);

    /* results keep the operand's signedness */
    int s = 0;
    CHECK(ATOMIC_ADD32(s, -1) < 0);
    CHECK(ATOMIC_LOAD32(s) < 0);
    CHECK(XCHANGE32(s, 3) < 0);
    CHECK(ATOMIC_LOAD32_ACQ(s) == 3);
    CHECK(ATOMIC_ADD32_PTR(&s, -4) < 0);

    uint64_t w = 0xffffffffULL;
    CHECK(ATOMIC_ADD64(w, 1) == 0x100000000ULL);
    CHECK(XCHANGE64(w, 0x123456789abcdef0ULL) == 0x100000000ULL);
    CHECK(ATOMIC_LOAD64(w) == 0x123456789abcdef0ULL);

    int64_t sw = 0;
    CHECK(ATOMIC_ADD64(sw, -1) < 0);

    volatile uint32_t vu = 0;
    ATOMIC_STORE32_REL(vu, 42);
    CHECK(ATOMIC_LOAD32_ACQ(vu) == 42);
    CHECK(ATOMIC_ADD32(vu, 1) == 43);

    /* each operand evaluated exactly once */
    uint32_t arr[4] = {0, 0, 0, 0};
    int i = 0;
    ATOMIC_ADD32(arr[i++], 1);
    CHECK(i == 1 && arr[0] == 1);
    uint32_t exp = 0;
    CHECK(CAS32(arr[i++], exp, 5));
    CHECK(i == 2 && arr[1] == 5);
    CHECK(ATOMIC_LOAD32(arr[i++]) == 0);
    CHECK(i == 3);
}

#define CAS_ITERS 200000
static uint32_t cas_ctr32;
static uint64_t cas_ctr64;

/* relies on CAS refreshing the expected value on failure */
static void *cas_counter(void *arg)
{
    FORL(i, 0, CAS_ITERS) {
        uint32_t o32 = ATOMIC_LOAD32(cas_ctr32);
        while (!CAS32(cas_ctr32, o32, o32 + 1))
            ;
        uint64_t o64 = ATOMIC_LOAD64(cas_ctr64);
        while (!CAS64(cas_ctr64, o64, o64 + 0x100000001ULL))
            ;
    }
    return NULL;
}

static uint32_t xchg_lock;
static uint64_t xchg_protected;
static void *xchg_spinlock(void *arg)
{
    FORL(i, 0, CAS_ITERS) {
        while (XCHANGE32(xchg_lock, 1))
            ;
        xchg_protected++;
        ATOMIC_STORE32_REL(xchg_lock, 0);
    }
    return NULL;
}

#define MSGS 200000
static uint32_t mp_flag;
static uint64_t mp_payload;
static void *mp_producer(void *arg)
{
    FORL(i, 1, MSGS + 1) {
        while (ATOMIC_LOAD32_ACQ(mp_flag))
            ;
        mp_payload = (uint64_t)i * 0x100000001ULL;
        ATOMIC_STORE32_REL(mp_flag, 1);
    }
    return NULL;
}

static void *mp_consumer(void *arg)
{
    FORL(i, 1, MSGS + 1) {
        while (!ATOMIC_LOAD32_ACQ(mp_flag))
            ;
        CHECK(mp_payload == (uint64_t)i * 0x100000001ULL);
        ATOMIC_STORE32_REL(mp_flag, 0);
    }
    return NULL;
}

/* store buffering: a seq_cst RMW followed by a seq_cst load of another word
 * must not let both threads miss each other's write */
#define SB_ROUNDS 200000
static uint32_t sb_x, sb_y, sb_go, sb_done;
static uint32_t sb_r[2];
static void *sb_thread(void *arg)
{
    int me = (int)(intptr_t)arg;
    FORL(round, 1, SB_ROUNDS + 1) {
        while (ATOMIC_LOAD32(sb_go) != (uint32_t)round)
            ;
        if (me == 0) {
            XCHANGE32(sb_x, round);
            sb_r[0] = ATOMIC_LOAD32(sb_y);
        } else {
            XCHANGE32(sb_y, round);
            sb_r[1] = ATOMIC_LOAD32(sb_x);
        }
        ATOMIC_ADD32(sb_done, 1);
    }
    return NULL;
}

static void test_store_buffering(void)
{
    pthread_t t0, t1;
    pthread_create(&t0, NULL, sb_thread, (void *)(intptr_t)0);
    pthread_create(&t1, NULL, sb_thread, (void *)(intptr_t)1);
    FORL(round, 1, SB_ROUNDS + 1) {
        ATOMIC_STORE32_REL(sb_go, round);
        while (ATOMIC_LOAD32(sb_done) != 2 * (uint32_t)round)
            ;
        CHECK(sb_r[0] == (uint32_t)round || sb_r[1] == (uint32_t)round);
    }
    pthread_join(t0, NULL);
    pthread_join(t1, NULL);
}

static void run_threads(void *(*fn)(void *), int n)
{
    pthread_t thv[THREADNUM];
    FORL(i, 0, n) {
        pthread_create(&thv[i], NULL, fn, NULL);
    }
    FORL(i, 0, n) {
        pthread_join(thv[i], NULL);
    }
}

int main()
{
    test_cas_semantics();
    test_rmw_semantics();

    //call 10 threads to increase variable passed in
    int a = 0;
    pthread_t thv[THREADNUM];
    FORL(i, 0, THREADNUM) {
        pthread_create(&thv[i], NULL, add_a_million, &a);
    }

    FORL(i, 0, THREADNUM) {
        void *val;
        pthread_join(thv[i], &val);
    }
    CHECK(a == THREADNUM * NUMTOADD);

    FORL(i, 0, THREADNUM) {
        pthread_create(&thv[i], NULL, get_max_rand, &a);
    }

    FORL(i, 0, THREADNUM) {
        void *val;
        pthread_join(thv[i], &val);
    }

    run_threads(cas_counter, THREADNUM);
    CHECK(cas_ctr32 == THREADNUM * CAS_ITERS);
    CHECK(cas_ctr64 == (uint64_t)THREADNUM * CAS_ITERS * 0x100000001ULL);

    run_threads(xchg_spinlock, THREADNUM);
    CHECK(xchg_protected == (uint64_t)THREADNUM * CAS_ITERS);

    pthread_t prod, cons;
    pthread_create(&prod, NULL, mp_producer, NULL);
    pthread_create(&cons, NULL, mp_consumer, NULL);
    pthread_join(prod, NULL);
    pthread_join(cons, NULL);

    test_store_buffering();

    printf("ok\n");
    return 0;
}
