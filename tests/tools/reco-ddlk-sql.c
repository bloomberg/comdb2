#include <inttypes.h>
#include <stddef.h>
#include <stdlib.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <cdb2api.h>

static int done;
static char *dbname, *tier;
static const int A2 = 10; // number or rows in a
static const int B2 = 100; // for each row in a, number of rows in b
static const int C2 = 1000; // for each row in b, number of rows in c
static char *master_node;
static int total_incoherent;

#define NREADERS 16

/* Perf mode (-p): emit per-phase METRIC lines, and count data mismatches
   instead of exiting on the first one.  Both matter for an A/B run: the
   numbers are the point, and an arm that aborts part way yields nothing to
   compare against.  Without -p the tool behaves exactly as before. */
static int perf_mode;
static int data_errors;

/* Lock stats (-l): also sample the server's lock_instrumentation counters
   around each phase and add the lk_ fields to the METRIC line.  Separate from
   -p because it is not free -- the counters need a tunable the server keeps
   off by default, so measuring them changes what is being measured.  Without
   -l the METRIC line simply stops after data_errors. */
static int lockstats_mode;

static pthread_mutex_t printlk = PTHREAD_MUTEX_INITIALIZER;
#define Printf(...)                         \
({                                          \
    pthread_mutex_lock(&printlk);           \
    int printf_rc = printf(__VA_ARGS__);    \
    pthread_mutex_unlock(&printlk);         \
    (void)printf_rc;                        \
})

/* Exit on a soft error, unless we are measuring -- see perf_mode.  Callers use
   this both for a failed statement (often just a deadlock) and for a real data
   mismatch, so a non-zero data_errors does not on its own mean bad data. */
#define data_error_exit()                                                   \
    do {                                                                    \
        if (!perf_mode) exit(1);                                            \
        __sync_fetch_and_add(&data_errors, 1);                              \
    } while (0)

/*
 * Phase metrics.
 *
 * The tool's pre-existing timings are all whole seconds, which rounds every
 * sub-second write to 0 -- useless for comparing write latency, which is the
 * main thing an A/B of the lock-release feature wants to see.
 *
 * No locking on either path:
 *   writes -- test_stmt() is only reached from update()/delete(), both strictly
 *             sequential on the main thread, so these are single-writer.
 *   reads  -- each reader thread owns its own slot, indexed by the thread id it
 *             is already passed.  metrics_report() runs after pthread_join(),
 *             which is a memory barrier, so every slot write is visible.
 * Per-thread read slots also give the spread across readers, not just an
 * average: if releasing locks hurts reads, it may hurt them unevenly.
 */
struct lockstats {
    long long rd_wait_us, rd_waits;
    long long wr_wait_us, wr_waits;
    long long other_wait_us, other_waits;
    long long rel_total_us, rel_unlock_us, rel_sleep_us, rel_reacquire_us, rel_revalidate_us, rel_count;
    long long sync_dta_us, sync_dta_count;
};

struct rd_slot {
    long long scans;
    long long rows;
    long long total_ms;
    long long min_ms;
    long long max_ms;
};
static struct rd_slot rd_slots[NREADERS + 1]; /* +1: the standalone reader */

static long long wr_count, wr_total_ms, wr_max_ms;

static long long ms_since(const struct timeval *start)
{
    struct timeval now, d;
    gettimeofday(&now, NULL);
    timersub(&now, start, &d);
    return (long long)d.tv_sec * 1000 + d.tv_usec / 1000;
}

static void metrics_reset(void)
{
    memset(rd_slots, 0, sizeof(rd_slots));
    wr_count = wr_total_ms = wr_max_ms = 0;
}

static void metrics_add_write(long long ms)
{
    wr_count++;
    wr_total_ms += ms;
    if (ms > wr_max_ms) wr_max_ms = ms;
}

static void metrics_add_read(int idx, long long ms, int rows)
{
    struct rd_slot *s = &rd_slots[(idx >= 0 && idx < NREADERS) ? idx : NREADERS];
    if (!s->scans || ms < s->min_ms) s->min_ms = ms;
    if (ms > s->max_ms) s->max_ms = ms;
    s->scans++;
    s->rows += rows;
    s->total_ms += ms;
}

static void metrics_report(const char *phase, long long phase_ms, const struct lockstats *lk)
{
    long long scans = 0, rows = 0, total = 0, mn = 0, mx = 0;

    for (int i = 0; i <= NREADERS; ++i) {
        struct rd_slot *s = &rd_slots[i];
        if (!s->scans) continue;
        if (!scans || s->min_ms < mn) mn = s->min_ms;
        if (s->max_ms > mx) mx = s->max_ms;
        scans += s->scans;
        rows += s->rows;
        total += s->total_ms;
    }

    /* Appended only when the counters were actually sampled (-l), so a run
       without them reports a short line rather than a row of zeros that reads
       like a measurement. */
    char lkbuf[768]; /* 14 fields at their int64 widest come to 492 */
    lkbuf[0] = 0;
    if (lk)
        snprintf(lkbuf, sizeof(lkbuf),
                 " lk_rd_wait_us=%lld lk_rd_waits=%lld lk_wr_wait_us=%lld lk_wr_waits=%lld"
                 " lk_other_wait_us=%lld lk_other_waits=%lld"
                 " lk_rel_us=%lld lk_rel_unlock_us=%lld lk_rel_sleep_us=%lld"
                 " lk_rel_reacq_us=%lld lk_rel_reval_us=%lld lk_rel_count=%lld"
                 " lk_sync_us=%lld lk_sync_count=%lld",
                 lk->rd_wait_us, lk->rd_waits, lk->wr_wait_us, lk->wr_waits,
                 lk->other_wait_us, lk->other_waits,
                 lk->rel_total_us, lk->rel_unlock_us, lk->rel_sleep_us,
                 lk->rel_reacquire_us, lk->rel_revalidate_us, lk->rel_count,
                 lk->sync_dta_us, lk->sync_dta_count);

    Printf("METRIC phase=%s phase_ms=%lld "
           "wr_count=%lld wr_avg_ms=%.1f wr_max_ms=%lld "
           "rd_scans=%lld rd_rows=%lld rd_avg_ms=%.1f rd_min_ms=%lld rd_max_ms=%lld "
           "incoherent=%d data_errors=%d%s\n",
           phase, phase_ms, wr_count, wr_count ? (double)wr_total_ms / wr_count : 0.0, wr_max_ms, scans, rows,
           scans ? (double)total / scans : 0.0, mn, mx, total_incoherent, data_errors, lkbuf);
}

static char *tohex(char *in, int len, char *out)
{
    char *beginning = out;
    char hex[] = "0123456789abcdef";
    const char *end = in + len;
    while (in != end) {
        char i = *(in++);
        *(out++) = hex[(i & 0xf0) >> 4];
        *(out++) = hex[i & 0x0f];
    }
    *out = 0;
    return beginning;
}
static cdb2_hndl_tp *hndl(char *host)
{
    int flags;
    if (!host) {
        host = tier;
        flags = 0;
    } else {
        flags = CDB2_DIRECT_CPU;
    }
    int rc;
    cdb2_hndl_tp *db;
    if ((rc = cdb2_open(&db, dbname, host, flags)) == 0) return db;
    Printf("%s: cdb2_open db:%s host:%s rc:%d %s\n", __func__, dbname, host, rc, cdb2_errstr(db));
    exit(1);
    return NULL;
}
static void who_master(void)
{
    int rc;
    cdb2_hndl_tp *db = hndl(NULL);
    if ((rc = cdb2_run_statement(db, "SELECT host FROM comdb2_cluster WHERE is_master = 'Y'")) != 0) {
        Printf("%s cdb2_run_statement rc:%d %s\n", __func__, rc, cdb2_errstr(db));
        exit(1);
    }
    if ((rc = cdb2_next_record(db)) != CDB2_OK) {
        Printf("%s cdb2_next_record rc:%d %s\n", __func__, rc, cdb2_errstr(db));
        exit(1);
    }
    master_node = strdup(cdb2_column_value(db, 0));
    if ((rc = cdb2_next_record(db)) != CDB2_OK_DONE) {
        Printf("%s cdb2_next_record not done rc:%d %s\n", __func__, rc, cdb2_errstr(db));
        exit(1);
    }
    cdb2_close(db);
}
/*
 * Server-side lock instrumentation (the 'lock_instrumentation' tunable).
 * Reached only under -l; with the tunable off every counter below reads zero.
 *
 * These counters are per node and cumulative, so a phase's cost is the delta
 * across every node: on a cluster the readers run on the replicants while the
 * writes commit on the master, so reading one node would see only half of it.
 */
static void lockstats_add_host(struct lockstats *acc, const char *host)
{
    static const struct {
        const char *name;
        size_t off;
    } map[] = {
        {"lockwait_reader_time", offsetof(struct lockstats, rd_wait_us)},
        {"lockwait_reader_count", offsetof(struct lockstats, rd_waits)},
        {"lockwait_writer_time", offsetof(struct lockstats, wr_wait_us)},
        {"lockwait_writer_count", offsetof(struct lockstats, wr_waits)},
        {"lockwait_other_time", offsetof(struct lockstats, other_wait_us)},
        {"lockwait_other_count", offsetof(struct lockstats, other_waits)},
        {"release_locks_time", offsetof(struct lockstats, rel_total_us)},
        {"release_locks_unlock_time", offsetof(struct lockstats, rel_unlock_us)},
        {"release_locks_sleep_time", offsetof(struct lockstats, rel_sleep_us)},
        {"release_locks_reacquire_time", offsetof(struct lockstats, rel_reacquire_us)},
        {"release_locks_revalidate_time", offsetof(struct lockstats, rel_revalidate_us)},
        {"release_locks_count", offsetof(struct lockstats, rel_count)},
        {"sync_dta_time", offsetof(struct lockstats, sync_dta_us)},
        {"sync_dta_count", offsetof(struct lockstats, sync_dta_count)},
    };
    cdb2_hndl_tp *db = hndl((char *)host);
    int rc = cdb2_run_statement(db, "SELECT name, value FROM comdb2_metrics");
    if (rc != 0) {
        /* Not fatal: an older server simply will not have these. */
        Printf("%s cdb2_run_statement rc:%d %s\n", __func__, rc, cdb2_errstr(db));
        cdb2_close(db);
        return;
    }
    while ((rc = cdb2_next_record(db)) == CDB2_OK) {
        const char *name = cdb2_column_value(db, 0);
        void *val = cdb2_column_value(db, 1);
        if (!name || !val) continue;
        /* the value column comes back typed, not as text */
        long long v;
        switch (cdb2_column_type(db, 1)) {
        case CDB2_INTEGER: v = *(int64_t *)val; break;
        case CDB2_REAL: v = (long long)*(double *)val; break;
        default: continue;
        }
        for (size_t i = 0; i < sizeof(map) / sizeof(map[0]); ++i) {
            if (strcmp(name, map[i].name) == 0) {
                *(long long *)((char *)acc + map[i].off) += v;
                break;
            }
        }
    }
    cdb2_close(db);
}

static void lockstats_snapshot(struct lockstats *out)
{
    memset(out, 0, sizeof(*out));
    cdb2_hndl_tp *db = hndl(NULL);
    if (cdb2_run_statement(db, "SELECT host FROM comdb2_cluster") != 0) {
        cdb2_close(db);
        lockstats_add_host(out, NULL); /* single node */
        return;
    }
    char *hosts[16];
    int nhosts = 0, rc;
    while ((rc = cdb2_next_record(db)) == CDB2_OK && nhosts < 16) {
        const char *h = cdb2_column_value(db, 0);
        if (h) hosts[nhosts++] = strdup(h);
    }
    cdb2_close(db);
    if (nhosts == 0) {
        lockstats_add_host(out, NULL);
        return;
    }
    for (int i = 0; i < nhosts; ++i) {
        lockstats_add_host(out, hosts[i]);
        free(hosts[i]);
    }
}

static void lockstats_diff(struct lockstats *d, const struct lockstats *a, const struct lockstats *b)
{
    long long *pd = (long long *)d;
    const long long *pa = (const long long *)a, *pb = (const long long *)b;
    for (size_t i = 0; i < sizeof(*d) / sizeof(long long); ++i)
        pd[i] = pb[i] - pa[i];
}

static int num_incoherent(void)
{
    cdb2_hndl_tp *master = hndl(master_node);
    int rc = cdb2_run_statement(master, "SELECT count(*) FROM comdb2_cluster where coherent_state != 'coherent'");
    if (rc != 0) {
        Printf("%s cdb2_run_statement rc:%d %s\n", __func__, rc, cdb2_errstr(master));
        exit(1);
    }
    if ((rc = cdb2_next_record(master)) != CDB2_OK) {
        Printf("%s cdb2_next_record rc:%d %s\n", __func__, rc, cdb2_errstr(master));
        exit(1);
    }
    if (cdb2_column_type(master, 0) != CDB2_INTEGER) {
        Printf("%s unexpected column-type:%d (wanted:%d)\n", __func__, cdb2_column_type(master, 0), CDB2_INTEGER);
        exit(1);
    }
    int count = *(int64_t *)cdb2_column_value(master, 0);
    if ((rc = cdb2_next_record(master)) != CDB2_OK_DONE) {
        Printf("%s cdb2_next_record done rc:%d %s\n", __func__, rc, cdb2_errstr(master));
        exit(1);
    }
    cdb2_close(master);
    return count;
}
/* Returns 0 on success.  On failure: if !soft, exits (matching the tool's
   original, always-fatal behaviour); if soft, counts a soft error (or exits,
   if perf_mode is off -- see data_error_exit()) and returns the failing rc,
   leaving db open for the caller to close.  soft is for test_stmt(): under
   perf_mode a failing UPDATE should be counted and measured, not kill the arm
   before any numbers come out of it.  The common failure here is not corrupt
   data but CDB2ERR_DEADLOCK (203): the multi-row UPDATEs on c hold write locks
   on 1000 rows plus their INCLUDE ALL index entries until commit, and with 16
   readers churning the same index the block transaction can exhaust its 500
   retries and be abandoned. */
static int run_stmt_(const char *where, cdb2_hndl_tp *db_hndl, const char *sql, int soft)
{
    cdb2_hndl_tp *db = db_hndl ? db_hndl : hndl(NULL);
    int rc = cdb2_run_statement(db, sql);
    if (rc != 0) {
        Printf("%s: cdb2_run_statement rc:%d err:%s sql:%s\n", where, rc, cdb2_errstr(db), sql);
        if (!soft) exit(1);
        data_error_exit();
        if (!db_hndl) cdb2_close(db);
        return rc;
    }
    do {
        rc = cdb2_next_record(db);
    } while (rc == CDB2_OK);
    if (rc != CDB2_OK_DONE) {
        Printf("%s: cdb2_run_statement rc:%d err:%s sql:%s\n", where, rc, cdb2_errstr(db), sql);
        if (!soft) exit(1);
        data_error_exit();
        if (!db_hndl) cdb2_close(db);
        return rc;
    }
    if (!db_hndl) cdb2_close(db);
    return 0;
}
#define run_stmt(...) run_stmt_(__func__, __VA_ARGS__, 0)
#define run_stmt_soft(...) run_stmt_(__func__, __VA_ARGS__, 1)

static void insert_a(void)
{
    cdb2_hndl_tp *db = hndl(NULL);
    cdb2_bind_param(db, "a2", CDB2_INTEGER, &A2, sizeof(A2));
    run_stmt(db, "INSERT INTO a(a1, a2, a3, a33, a4, a44) SELECT value, value * 10, hex(value * 10), hex(value * 10), x'600d', x'600d' from generate_series(1, @a2)");
    cdb2_clearbindings(db);
    cdb2_close(db);
}
static void *insert_b_(void *data)
{
    int b1 = (intptr_t) data;
    cdb2_hndl_tp *db = hndl(NULL);
    cdb2_bind_param(db, "b1", CDB2_INTEGER, &b1, sizeof(b1));
    cdb2_bind_param(db, "b2", CDB2_INTEGER, &B2, sizeof(B2));
    run_stmt(db, "INSERT INTO b(b1, b2, b3, b33, b4, b44, b5) SELECT @b1, @b1 * 100, hex(@b1 * 100), hex(@b1 * 100), x'f0000f', x'f0000f', value from generate_series(1, @b2)");
    cdb2_clearbindings(db);
    cdb2_close(db);
    return NULL;
}
static void insert_b(void)
{
    int t = 0;
    pthread_t thds[10];
    for (intptr_t b1 = 1; b1 <= A2; ++b1) {
        pthread_create(&thds[t++], NULL, insert_b_, (void *)b1);
        if (t == (sizeof(thds) / sizeof(thds[0]))) {
            for (int i = 0; i < t; ++i) pthread_join(thds[i], NULL);
            t = 0;
        }
    }
    for (int i = 0; i < t; ++i) pthread_join(thds[i], NULL);
}
static void *insert_c_(void *data)
{
    int c1 = (intptr_t)data;
    cdb2_hndl_tp *db = hndl(NULL);
    cdb2_bind_param(db, "c1", CDB2_INTEGER, &c1, sizeof(c1));
    cdb2_bind_param(db, "c2", CDB2_INTEGER, &C2, sizeof(C2));
    run_stmt(db, "INSERT INTO c(c1, c2, c3, c33, c4, c44, c5) SELECT @c1, @c1 * 1000, hex(@c1 * 1000), hex(@c1 * 1000), x'600df00f', x'600df00f', value from generate_series(1, @c2)");
    cdb2_clearbindings(db);
    cdb2_close(db);
    return NULL;
}
static void insert_c(void)
{
    int t = 0;
    pthread_t thds[10];
    for (intptr_t c1 = 1; c1 <= B2; ++c1) {
        pthread_create(&thds[t++], NULL, insert_c_, (void *)c1);
        if (t == (sizeof(thds) / sizeof(thds[0]))) {
            for (int i = 0; i < t; ++i) pthread_join(thds[i], NULL);
            t = 0;
        }
    }
    for (int i = 0; i < t; ++i) pthread_join(thds[i], NULL);
}
static void wait_for_coherent(void)
{
    while (num_incoherent() != 0) poll(NULL, 0, 100);
}
static void insert(void)
{
    Printf("%s started\n", __func__);
    struct timeval start, finish, elapsed;
    gettimeofday(&start, NULL);
    insert_a();
    insert_b();
    insert_c();
    gettimeofday(&finish, NULL);
    timersub(&finish, &start, &elapsed);
    Printf("%s finished time:%ldsec\n", __func__, elapsed.tv_sec);
}
static void *reader(void *data)
{
    cdb2_hndl_tp *db = hndl(NULL);
    int i = (intptr_t)data;
    char *order = i % 2 == 0 ? "ASC" : "DESC";
    char buf[1024];
    struct timeval scan_start;
    gettimeofday(&scan_start, NULL);
    snprintf(buf, sizeof(buf),
             "SELECT "
             "a1, a2, a3, a33, a4, a44, hex(a2), "
             "b1, b2, b3, b33, b4, b44, hex(b2), "
             "c1, c2, c3, c33, c4, c44, hex(c2), "
             "b5, c5 "
             "FROM "
             "a, b, c "
             "WHERE "
             "a1 = b1 AND "
             "b1 = c1 "
             "ORDER BY a1 %s", order);
    int rc = cdb2_run_statement(db, buf);
    if (rc != 0) {
        Printf("%s: cdb2_run_statement rc:%d %s\n", __func__, rc, cdb2_errstr(db));
        exit(1);
    }
    int counter = 0;
    while ((rc = cdb2_next_record(db)) == CDB2_OK) {
        int do_exit = 0;
        ++counter;
        int col = 0;
        int a1, a2, b1, b2, c1, c2;
        void *a3, *a33, *a4, *a44, *ah, *b3, *b33, *b4, *b44, *bh, *c3, *c33, *c4, *c44, *ch;
        int a3l, a33l, a4l, a44l, ahl, b3l, b33l, b4l, b44l, bhl, c3l, c33l, c4l, c44l, chl;
        int b5, c5;
        
        a1 = *(int64_t*)cdb2_column_value(db, col++);
        a2 = *(int64_t*)cdb2_column_value(db, col++);
        a3 = cdb2_column_value(db, col);
        a3l = cdb2_column_size(db, col++);
        a33 = cdb2_column_value(db, col);
        a33l = cdb2_column_size(db, col++);
        a4 = cdb2_column_value(db, col);
        a4l = cdb2_column_size(db, col++);
        a44 = cdb2_column_value(db, col);
        a44l = cdb2_column_size(db, col++);
        ah = cdb2_column_value(db, col);
        ahl = cdb2_column_size(db, col++);

        b1 = *(int64_t*)cdb2_column_value(db, col++);
        b2 = *(int64_t*)cdb2_column_value(db, col++);
        b3 = cdb2_column_value(db, col);
        b3l = cdb2_column_size(db, col++);
        b33 = cdb2_column_value(db, col);
        b33l = cdb2_column_size(db, col++);
        b4 = cdb2_column_value(db, col);
        b4l = cdb2_column_size(db, col++);
        b44 = cdb2_column_value(db, col);
        b44l = cdb2_column_size(db, col++);
        bh = cdb2_column_value(db, col);
        bhl = cdb2_column_size(db, col++);

        c1 = *(int64_t*)cdb2_column_value(db, col++);
        c2 = *(int64_t*)cdb2_column_value(db, col++);
        c3 = cdb2_column_value(db, col);
        c3l = cdb2_column_size(db, col++);
        c33 = cdb2_column_value(db, col);
        c33l = cdb2_column_size(db, col++);
        c4 = cdb2_column_value(db, col);
        c4l = cdb2_column_size(db, col++);
        c44 = cdb2_column_value(db, col);
        c44l = cdb2_column_size(db, col++);
        ch = cdb2_column_value(db, col);
        chl = cdb2_column_size(db, col++);

        b5 = *(int64_t*)cdb2_column_value(db, col++);
        c5 = *(int64_t*)cdb2_column_value(db, col++);

        char g00d[] = {0x60, 0x0d};
        char f0000f[] = {0xf0, 0x00, 0x0f};
        char g00df00f[] = {0x60, 0x0d, 0xf0, 0x0f};
        uint8_t aout[512], hout[512];

        if (a1 != b1 ||
            b1 != c1 ||
            a1 * 10 != a2 ||
            b1 * 100 != b2 ||
            c1 * 1000 != c2
        ){
            Printf("%s %2d: cdb2_next_record UNEXPECTED integers  =>  "
                    "a1:%d a2:%d  "
                    "b1:%d b2:%d b5:%d  "
                    "c1:%d c2:%d c5:%d  "
                    "\n", __func__, i,
                    a1, a2,
                    b1, b2, b5,
                    c1, c2, c5);
            do_exit = 1;
        } else if (
            a3l != ahl ||
            a3l != a33l ||
            b3l != bhl ||
            b3l != b33l ||
            c3l != chl ||
            c3l != c33l ||
            a4l != sizeof(g00d) ||
            a4l != a44l ||
            b4l != sizeof(f0000f) ||
            b4l != b44l ||
            c4l != sizeof(g00df00f) ||
            c4l != c44l
        ){
            Printf("%s %2d: cdb2_next_record UNEXPECTED lengths  =>  "
                    "a1:%d b1:%d c1:%d  "
                    "a3l:%d ahl:%d a33l:%d  "
                    "b3l:%d bhl:%d b33l:%d  "
                    "c3l:%d chl:%d c33l:%d  "
                    "a4l:%d(want %zu) a44l:%d  "
                    "b4l:%d(want %zu) b44l:%d  "
                    "c4l:%d(want %zu) c44l:%d  "
                    "counter:%d\n", __func__, i,
                    a1, b1, c1,
                    a3l, ahl, a33l,
                    b3l, bhl, b33l,
                    c3l, chl, c33l,
                    a4l, sizeof(g00d), a44l,
                    b4l, sizeof(f0000f), b44l,
                    c4l, sizeof(g00df00f), c44l,
                    counter);
            do_exit = 1;
        } else if (memcmp(a3, ah, a3l)) {
            Printf("%s %2d: cdb2_next_record UNEXPECTED a3 payload =>  ", __func__, i);
            Printf("a3 len:%d payload:%s  vs       hex len:%d payload:%s\n", a3l, tohex(a3, a3l, (char *)aout), ahl, tohex(ah, ahl, (char *)hout));
            do_exit = 1;
        } else if (memcmp(a33, ah, a33l)) {
            Printf("%s %2d: cdb2_next_record UNEXPECTED a33 payload =>  ", __func__, i);
            Printf("a33 len:%d payload:%s  vs       hex len:%d payload:%s\n", a33l, tohex(a33, a33l, (char *)aout), ahl, tohex(ah, ahl, (char *)hout));
            do_exit = 1;
        } else if (memcmp(b3, bh, b3l)) {
            Printf("%s %2d: cdb2_next_record UNEXPECTED b3 payload =>  ", __func__, i);
            Printf("b3 len:%d payload:%s  vs      bh len:%d payload:%s\n", b3l, tohex(b3, b3l, (char *)aout), bhl, tohex(bh, bhl, (char *)hout));
            do_exit = 1;
        } else if (memcmp(b33, bh, b33l)) {
            Printf("%s %2d: cdb2_next_record UNEXPECTED b33 payload =>  ", __func__, i);
            Printf("b33 len:%d payload:%s  vs      bh len:%d payload:%s\n", b33l, tohex(b33, b33l, (char *)aout), bhl, tohex(bh, bhl, (char *)hout));
            do_exit = 1;
        } else if (memcmp(c3, ch, c3l)) {
            Printf("%s %2d: cdb2_next_record UNEXPECTED c3 payload =>  ", __func__, i);
            Printf("c3 len:%d payload:%s  vs      ch len:%d payload:%s\n", c3l, tohex(c3, c3l, (char *)aout), chl, tohex(ch, chl, (char *)hout));
            do_exit = 1;
        } else if (memcmp(c33, ch, c33l)) {
            Printf("%s %2d: cdb2_next_record UNEXPECTED c33 payload =>  ", __func__, i);
            Printf("c33 len:%d payload:%s  vs      ch len:%d payload:%s\n", c33l, tohex(c33, c33l, (char *)aout), chl, tohex(ch, chl, (char *)hout));
            do_exit = 1;
        } else if (memcmp(a4, g00d, a4l)) {
            Printf("%s %2d: cdb2_next_record UNEXPECTED a4 payload =>  ", __func__, i);
            Printf("a4 len:%d payload:%s  vs      g00d len:%zu payload:%s\n", a4l, tohex(a4, a4l, (char *)aout), sizeof(g00d), tohex(g00d, sizeof(g00d), (char *)hout));
            do_exit = 1;
        } else if (memcmp(a44, g00d, a44l)) {
            Printf("%s %2d: cdb2_next_record UNEXPECTED a44 payload =>  ", __func__, i);
            Printf("a44 len:%d payload:%s  vs      g00d len:%zu payload:%s\n", a44l, tohex(a44, a44l, (char *)aout), sizeof(g00d), tohex(g00d, sizeof(g00d), (char *)hout));
            do_exit = 1;
        } else if (memcmp(b4, f0000f, b4l)) {
            Printf("%s %2d: cdb2_next_record UNEXPECTED b4 payload =>  ", __func__, i);
            Printf("b4 len:%d payload:%s  vs    f0000f len:%zu payload:%s\n", b4l, tohex(b4, b4l, (char *)aout), sizeof(f0000f), tohex(f0000f, sizeof(f0000f), (char *)hout));
            do_exit = 1;
        } else if (memcmp(b44, f0000f, b44l)) {
            Printf("%s %2d: cdb2_next_record UNEXPECTED b44 payload =>  ", __func__, i);
            Printf("b44 len:%d payload:%s  vs    f0000f len:%zu payload:%s\n", b44l, tohex(b44, b44l, (char *)aout), sizeof(f0000f), tohex(f0000f, sizeof(f0000f), (char *)hout));
            do_exit = 1;
        } else if (memcmp(c4, g00df00f, c4l)) {
            Printf("%s %2d: cdb2_next_record UNEXPECTED c4 payload =>  ", __func__, i);
            Printf("c4 len:%d payload:%s  vs  g00df00f len:%zu payload:%s\n", c4l, tohex(c4, c4l, (char *)aout), sizeof(g00df00f), tohex(g00df00f, sizeof(g00df00f), (char *)hout));
            do_exit = 1;
        } else if (memcmp(c44, g00df00f, c44l)) {
            Printf("%s %2d: cdb2_next_record UNEXPECTED c44 payload =>  ", __func__, i);
            Printf("c44 len:%d payload:%s  vs  g00df00f len:%zu payload:%s\n", c44l, tohex(c44, c44l, (char *)aout), sizeof(g00df00f), tohex(g00df00f, sizeof(g00df00f), (char *)hout));
            do_exit = 1;
        }
        if (do_exit) {
            data_error_exit();
        }
    }
    if (rc != CDB2_OK_DONE) {
        Printf("%s: cdb2_next_record rc:%d %s\n", __func__, rc, cdb2_errstr(db));
        data_error_exit();
    }
    cdb2_close(db);
    metrics_add_read(i, ms_since(&scan_start), counter);
    if (counter) Printf("%s %2d: rows:%d\n", __func__, i, counter);
    return NULL;
}
static void *readers(void *data)
{
    Printf("%s started\n", __func__);
    pthread_t thds[16];
    struct timeval start, finish, elapsed;
    gettimeofday(&start, NULL);
    int count = 0;
    do {
        ++count;
        struct timeval start, finish, elapsed;
        gettimeofday(&start, NULL);
        for (intptr_t i = 0; i < sizeof(thds) / sizeof(thds[0]); ++i) pthread_create(&thds[i], NULL, reader, (void *)i);
        for (intptr_t i = 0; i < sizeof(thds) / sizeof(thds[0]); ++i) pthread_join(thds[i], NULL);
        gettimeofday(&finish, NULL);
        timersub(&finish, &start, &elapsed);
        if (count < 10) Printf("%s  iteration:%d  time:%ldsec\n", __func__, count, elapsed.tv_sec);
    } while (!done);
    gettimeofday(&finish, NULL);
    timersub(&finish, &start, &elapsed);
    Printf("%s  finished  iterations:%d  total-time:%ldsec\n", __func__, count, elapsed.tv_sec);
    return NULL;
}
static void test_stmt(const char *sql, int upd, int del)
{
    cdb2_hndl_tp *db;
    cdb2_effects_tp effects = {0};
    struct timeval start, finish, elapsed;
    gettimeofday(&start, NULL);
    db = hndl(NULL);
    int rc = run_stmt_soft(db, sql);
    /* record the statement latency before the checks below, which can bail */
    metrics_add_write(ms_since(&start));
    gettimeofday(&finish, NULL);
    timersub(&finish, &start, &elapsed);
    if (rc == 0) {
        cdb2_get_effects(db, &effects);
        if (upd) {
            if (effects.num_updated != upd) {
                Printf("%s: cdb2_get_effects unexpected num_updated:%d (wanted:%d)\n", sql, effects.num_updated, upd);
                data_error_exit();
            }
        }
        if (del) {
            if (effects.num_deleted != del) {
                Printf("%s: cdb2_get_effects unexpected num_deleted:%d (wanted:%d)\n", sql, effects.num_deleted, del);
                data_error_exit();
            }
        }
    }
    cdb2_close(db);
    int inco = num_incoherent();
    Printf("%s:  %s  time:%ldsec  num_updated:%d  num_deleted:%d  num-incoherent:%d\n",
            sql, upd ? "update" : "delete", elapsed.tv_sec, effects.num_updated, effects.num_deleted, inco);
    total_incoherent += inco;
}
static void update(void)
{
    Printf("%s started\n", __func__);
    struct timeval start, finish, elapsed;
    gettimeofday(&start, NULL);
    int counter = 0;
    char buf[1024];
    do {
        ++counter;
        for (int i = 1, j = A2; i < j; ++i, --j) {
            snprintf(buf, sizeof(buf), "UPDATE c SET c1 = c2 / 1000, c3 = c3, c4 = c4 where c1 = %d", i);
            test_stmt(buf, C2, 0);
            snprintf(buf, sizeof(buf), "UPDATE c SET c2 = c1 * 1000, c3 = c3, c4 = c4 where c1 = %d", j);
            test_stmt(buf, C2, 0);

            snprintf(buf, sizeof(buf), "UPDATE b SET b1 = b2 / 100, b3 = b3, b4 = b4 where b1 = %d", i);
            test_stmt(buf, B2, 0);
            snprintf(buf, sizeof(buf), "UPDATE b SET b2 = b1 * 100, b3 = b3, b4 = b4 where b1 = %d", j);
            test_stmt(buf, B2, 0);

            snprintf(buf, sizeof(buf), "UPDATE a SET a1 = a2 / 10, a3 = a3, a4 = a4 where a1 = %d", i);
            test_stmt(buf, 1, 0);
            snprintf(buf, sizeof(buf), "UPDATE a SET a2 = a1 * 10, a3 = a3, a4 = a4 where a1 = %d", j);
            test_stmt(buf, 1, 0);
        }
        gettimeofday(&finish, NULL);
        timersub(&finish, &start, &elapsed);
        Printf("%s  iteration:%d  time:%ldsec\n", __func__, counter, elapsed.tv_sec);
    } while (elapsed.tv_sec < 60);
    Printf("%s  finished  iterations:%d  total-time:%ldsec\n", __func__, counter, elapsed.tv_sec);
}
static void delete(void)
{
    Printf("%s started\n", __func__);
    struct timeval start, finish, elapsed;
    gettimeofday(&start, NULL);
    char buf[1024];
    for (int i = 1, j = A2; i < j; ++i, --j) {
        snprintf(buf, sizeof(buf), "DELETE FROM c WHERE c1 = %d", i);
        test_stmt(buf, 0, C2);
        snprintf(buf, sizeof(buf), "DELETE FROM c WHERE c1 = %d", j);
        test_stmt(buf, 0, C2);

        snprintf(buf, sizeof(buf), "DELETE FROM b WHERE b1 = %d", i);
        test_stmt(buf, 0, B2);
        snprintf(buf, sizeof(buf), "DELETE FROM b WHERE b1 = %d", j);
        test_stmt(buf, 0, B2);

        snprintf(buf, sizeof(buf), "DELETE FROM a WHERE a1 = %d", i);
        test_stmt(buf, 0, 1);
        snprintf(buf, sizeof(buf), "DELETE FROM a WHERE a1 = %d", j);
        test_stmt(buf, 0, 1);
    }
    gettimeofday(&finish, NULL);
    timersub(&finish, &start, &elapsed);
    Printf("%s  finished  total-time:%ldsec\n", __func__, elapsed.tv_sec);
}
/* Run one phase: readers in the background, `body` (the writer) in front.
   Metrics are reset before the readers start and reported after they are
   joined, so reads and writes are scoped to the same window and are directly
   comparable across an A/B run. */
static void run_phase(const char *name, void (*body)(void), int settle)
{
    pthread_t thd;
    struct timeval start;
    struct lockstats before, after, delta;

    int lk = perf_mode && lockstats_mode;

    metrics_reset();
    memset(&delta, 0, sizeof(delta));
    if (lk) lockstats_snapshot(&before);
    gettimeofday(&start, NULL);

    done = 0;
    pthread_create(&thd, NULL, readers, NULL);
    if (settle) sleep(1); /* give time for readers to start */
    body();
    wait_for_coherent();
    done = 1;
    pthread_join(thd, NULL);

    if (perf_mode) {
        long long ms = ms_since(&start);
        if (lk) {
            lockstats_snapshot(&after);
            lockstats_diff(&delta, &before, &after);
        }
        metrics_report(name, ms, lk ? &delta : NULL);
    }
}

static int runit(void)
{
    run_phase("insert", insert, 0);
    reader((void *)(intptr_t)99);

    run_phase("update", update, 1);
    run_phase("delete", delete, 1);

    return total_incoherent;
}
static void setup(void)
{
    cdb2_hndl_tp *db = hndl(NULL);
    //run_stmt(db, "DROP TABLE IF EXISTS sqlite_stat1");
    //run_stmt(db, "DROP TABLE IF EXISTS sqlite_stat4");
    run_stmt(db, "DROP TABLE IF EXISTS a");
    run_stmt(db, "DROP TABLE IF EXISTS b");
    run_stmt(db, "DROP TABLE IF EXISTS c");
    run_stmt(db, "CREATE TABLE a(a1 INTEGER, a2 INTEGER, a3 VUTF8, a33 VUTF8(32), a4 BLOB, a44 BLOB(32))");
    run_stmt(db, "CREATE TABLE b(b1 INTEGER, b2 INTEGER, b3 VUTF8, b33 VUTF8(32), b4 BLOB, b44 BLOB(32), b5 INTEGER)");
    run_stmt(db, "CREATE TABLE c(c1 INTEGER, c2 INTEGER, c3 VUTF8, c33 VUTF8(32), c4 BLOB, c44 BLOB(32), c5 INTEGER)");
    run_stmt(db, "CREATE UNIQUE INDEX a0 on a(a1) INCLUDE ALL");
    run_stmt(db, "CREATE INDEX b0 on b(b1) INCLUDE ALL");
    run_stmt(db, "CREATE INDEX c0 on c(c1) INCLUDE ALL");
    cdb2_close(db);
}
int main(int argc, char **argv)
{
    char *conf = getenv("CDB2_CONFIG");
    if (conf) cdb2_set_comdb2db_config(conf);

    /* usage: reco-ddlk-sql [-p] [-l] dbname [tier] */
    int npos = 0;
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-p") == 0) {
            perf_mode = 1;
        } else if (strcmp(argv[i], "-l") == 0) {
            lockstats_mode = 1;
        } else if (npos == 0) {
            dbname = argv[i];
            npos = 1;
        } else if (npos == 1) {
            tier = argv[i];
            npos = 2;
        }
    }
    if (!dbname) {
        fprintf(stderr, "usage: %s [-p] [-l] dbname [tier]\n", argv[0]);
        return 1;
    }
    if (!tier) tier = "default";

    who_master();
    setup();

    struct timeval start, finish, elapsed;
    gettimeofday(&start, NULL);
    int rc = runit();
    gettimeofday(&finish, NULL);
    timersub(&finish, &start, &elapsed);

    if (rc) {
        Printf("failed  =>  time:%ldsec  num-incoherent:%d\n", elapsed.tv_sec, total_incoherent);
    } else {
        Printf("passed  =>  time:%ldsec\n", elapsed.tv_sec);
    }
    /* In perf mode a data error is counted rather than fatal, so surface it
       here -- the A/B driver reports it alongside the timings. */
    if (perf_mode) Printf("METRIC total data_errors=%d incoherent=%d\n", data_errors, total_incoherent);
    return rc;
}
