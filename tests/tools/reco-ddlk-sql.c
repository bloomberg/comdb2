#include <inttypes.h>
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

static pthread_mutex_t printlk = PTHREAD_MUTEX_INITIALIZER;
#define Printf(...)                         \
({                                          \
    pthread_mutex_lock(&printlk);           \
    int printf_rc = printf(__VA_ARGS__);    \
    pthread_mutex_unlock(&printlk);         \
    (void)printf_rc;                        \
})

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
static void run_stmt_(const char *where, cdb2_hndl_tp *db_hndl, const char *sql)
{
    cdb2_hndl_tp *db = db_hndl ? db_hndl : hndl(NULL);
    int rc = cdb2_run_statement(db, sql);
    if (rc != 0) {
        Printf("%s: cdb2_run_statement rc:%d err:%s sql:%s\n", where, rc, cdb2_errstr(db), sql);
        exit(1);
    }
    do {
        rc = cdb2_next_record(db);
    } while (rc == CDB2_OK);
    if (rc != CDB2_OK_DONE) {
        Printf("%s: cdb2_run_statement rc:%d err:%s sql:%s\n", where, rc, cdb2_errstr(db), sql);
        exit(1);
    }
    if (!db_hndl) cdb2_close(db);
}
#define run_stmt(...) run_stmt_(__func__, __VA_ARGS__)

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
            Printf("%s %2d: cdb2_next_record UNEXPECTED lengths  =>  ", __func__, i);
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
            exit(1);
        }
    }
    if (rc != CDB2_OK_DONE) {
        Printf("%s: cdb2_next_record rc:%d %s\n", __func__, rc, cdb2_errstr(db));
        exit(1);
    }
    cdb2_close(db);
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
    cdb2_effects_tp effects;
    struct timeval start, finish, elapsed;
    gettimeofday(&start, NULL);
    db = hndl(NULL);
    run_stmt(db, sql);
    cdb2_get_effects(db, &effects);
    gettimeofday(&finish, NULL);
    timersub(&finish, &start, &elapsed);
    if (upd) {
        if (effects.num_updated != upd) {
            Printf("%s: cdb2_get_effects unexpected num_updated:%d (wanted:%d)\n", sql, effects.num_updated, upd);
            exit(1);
        }
    }
    if (del) {
        if (effects.num_deleted != del) {
            Printf("%s: cdb2_get_effects unexpected num_deleted:%d (wanted:%d)\n", sql, effects.num_deleted, del);
            exit(1);
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
static int runit(void)
{
    pthread_t thd;

    done = 0;
    pthread_create(&thd, NULL, readers, NULL);
    insert();
    wait_for_coherent();
    done = 1;
    pthread_join(thd, NULL);
    reader((void *)(intptr_t)99);

    done = 0;
    pthread_create(&thd, NULL, readers, NULL);
    sleep(1); /* give time for readers to start */
    update();
    wait_for_coherent();
    done = 1;
    pthread_join(thd, NULL);

    done = 0;
    pthread_create(&thd, NULL, readers, NULL);
    sleep(1); /* give time for readers to start */
    delete();
    wait_for_coherent();
    done = 1;
    pthread_join(thd, NULL);

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
    dbname = argv[1];
    tier = argc >= 3 ? argv[2] : "default";

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
    return rc;
}
