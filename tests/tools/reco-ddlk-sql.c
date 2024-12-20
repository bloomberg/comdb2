#include <inttypes.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <cdb2api.h>

static int done, failed_upd, failed_del;
static char *dbname;
static const int a2 = 10; // number or rows in a
static const int b2 = 100; // for each row in a, number of rows in b
static const int c2 = 1000; // for each row in b, number of rows in c
static char *master_node;
static int total_incoherent;

static cdb2_hndl_tp *hndl(char *host)
{
    int flags;
    if (!host) {
        host = "default";
        flags = 0;
    } else {
        flags = CDB2_DIRECT_CPU;
    }
    int rc;
    cdb2_hndl_tp *db;
    if ((rc = cdb2_open(&db, dbname, host, flags)) == 0) return db;
    fprintf(stderr, "%s: cdb2_open db:%s host:%s rc:%d %s\n", __func__, dbname, host, rc, cdb2_errstr(db));
    exit(1);
    return NULL;
}
static void who_master(void)
{
    int rc;
    cdb2_hndl_tp *db = hndl(NULL);
    if ((rc = cdb2_run_statement(db, "SELECT host FROM comdb2_cluster WHERE is_master = 'Y'")) != 0) {
        fprintf(stderr, "%s cdb2_run_statement rc:%d %s\n", __func__, rc, cdb2_errstr(db));
        exit(1);
    }
    if ((rc = cdb2_next_record(db)) != CDB2_OK) {
        fprintf(stderr, "%s cdb2_next_record rc:%d %s\n", __func__, rc, cdb2_errstr(db));
        exit(1);
    }
    master_node = strdup(cdb2_column_value(db, 0));
    if ((rc = cdb2_next_record(db)) != CDB2_OK_DONE) {
        fprintf(stderr, "%s cdb2_next_record not done rc:%d %s\n", __func__, rc, cdb2_errstr(db));
        exit(1);
    }
    cdb2_close(db);
}
static int num_incoherent(void)
{
    cdb2_hndl_tp *master = hndl(master_node);
    int rc = cdb2_run_statement(master, "SELECT count(*) FROM comdb2_cluster where coherent_state != 'coherent'");
    if (rc != 0) {
        fprintf(stderr, "%s cdb2_run_statement rc:%d %s\n", __func__, rc, cdb2_errstr(master));
        exit(1);
    }
    if ((rc = cdb2_next_record(master)) != CDB2_OK) {
        fprintf(stderr, "%s cdb2_next_record rc:%d %s\n", __func__, rc, cdb2_errstr(master));
        exit(1);
    }
    if (cdb2_column_type(master, 0) != CDB2_INTEGER) {
        fprintf(stderr, "%s unexpected column-type:%d (wanted:%d)\n", __func__, cdb2_column_type(master, 0), CDB2_INTEGER);
        exit(1);
    }
    int count = *(int64_t *)cdb2_column_value(master, 0);
    if ((rc = cdb2_next_record(master)) != CDB2_OK_DONE) {
        fprintf(stderr, "%s cdb2_next_record done rc:%d %s\n", __func__, rc, cdb2_errstr(master));
        exit(1);
    }
    cdb2_close(master);
    return count;
}
static void run_stmt_(const char *where, cdb2_hndl_tp *db, const char *sql)
{
    int rc;
    rc = cdb2_run_statement(db, sql);
    if (rc != 0) {
        fprintf(stderr, "%s: cdb2_run_statement rc:%d %s\n", where, rc, cdb2_errstr(db));
        exit(1);
    }
    do {
        rc = cdb2_next_record(db);
    } while (rc == CDB2_OK);
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "%s: cdb2_run_statement rc:%d %s\n", where, rc, cdb2_errstr(db));
        exit(1);
    }
}
#define run_stmt(...) run_stmt_(__func__, __VA_ARGS__)

static void insert_a(void)
{
    cdb2_hndl_tp *db = hndl(NULL);
    cdb2_bind_param(db, "a2", CDB2_INTEGER, &a2, sizeof(a2));
    run_stmt(db, "INSERT INTO a(a1, a2, a3, a4) SELECT value, value * 10, hex(value * 10), x'600d' from generate_series(1, @a2)");
    cdb2_clearbindings(db);
    cdb2_close(db);
}
static void *insert_b_(void *data)
{
    int b1 = (intptr_t) data;
    cdb2_hndl_tp *db = hndl(NULL);
    cdb2_bind_param(db, "b1", CDB2_INTEGER, &b1, sizeof(b1));
    cdb2_bind_param(db, "b2", CDB2_INTEGER, &b2, sizeof(b2));
    run_stmt(db, "INSERT INTO b(b1, b2, b3, b4, b5) SELECT @b1, @b1 * 100, hex(@b1 * 100), x'f0000f', value from generate_series(1, @b2)");
    cdb2_clearbindings(db);
    cdb2_close(db);
    return NULL;
}
static void insert_b(void)
{
    int t = 0;
    pthread_t thds[10];
    for (intptr_t b1 = 1; b1 <= a2; ++b1) {
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
    cdb2_bind_param(db, "c2", CDB2_INTEGER, &c2, sizeof(c2));
    run_stmt(db, "INSERT INTO c(c1, c2, c3, c4, c5) SELECT @c1, @c1 * 1000, hex(@c1 * 1000), x'600df00f', value from generate_series(1, @c2)");
    cdb2_clearbindings(db);
    cdb2_close(db);
    return NULL;
}
static void insert_c(void)
{
    int t = 0;
    pthread_t thds[10];
    for (intptr_t c1 = 1; c1 <= b2; ++c1) {
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
static void setup(void)
{
    cdb2_hndl_tp *db = hndl(NULL);
    run_stmt(db, "DROP TABLE IF EXISTS sqlite_stat1");
    run_stmt(db, "DROP TABLE IF EXISTS sqlite_stat4");
    run_stmt(db, "DROP TABLE IF EXISTS a");
    run_stmt(db, "DROP TABLE IF EXISTS b");
    run_stmt(db, "DROP TABLE IF EXISTS c");
    run_stmt(db, "CREATE TABLE a(a1 INTEGER INDEX, a2 INTEGER, a3 VUTF8, a4 BLOB)");
    run_stmt(db, "CREATE TABLE b(b1 INTEGER INDEX, b2 INTEGER, b3 VUTF8, b4 BLOB, b5 INTEGER)");
    run_stmt(db, "CREATE TABLE c(c1 INTEGER INDEX, c2 INTEGER, c3 VUTF8, c4 BLOB, c5 INTEGER)");
    cdb2_close(db);
}
static void insert(void)
{
    printf("%s started\n", __func__);
    struct timeval start, finish, elapsed;
    gettimeofday(&start, NULL);
    insert_a();
    insert_b();
    insert_c();
    gettimeofday(&finish, NULL);
    timersub(&finish, &start, &elapsed);
    printf("%s finished time:%ldsec\n", __func__, elapsed.tv_sec);
}
static void *reader(void *data)
{
    cdb2_hndl_tp *db = hndl(NULL);
    int i = (intptr_t)data;
    char *order = i % 2 == 0 ? "ASC" : "DESC";
    char buf[1024];
    snprintf(buf, sizeof(buf),
             "SELECT "
             "a1, a2, a3, a4, hex(a2), "
             "b1, b2, b3, b4, hex(b2), "
             "c1, c2, c3, c4, hex(c2), "
             "b5, c5 "
             "FROM "
             "a, b, c "
             "WHERE "
             "a1 = b1 AND "
             "b1 = c1 "
             "ORDER BY a1 %s, b1, c1", order);
    int rc = cdb2_run_statement(db, buf);
    if (rc != 0) {
        fprintf(stderr, "%s: cdb2_run_statement rc:%d %s\n", __func__, rc, cdb2_errstr(db));
        exit(1);
    }
    int counter = 0;
    while ((rc = cdb2_next_record(db)) == CDB2_OK) {
        ++counter;
        int col = 0;
        int a1, a2, b1, b2, c1, c2;
        void *a3, *a4, *ah, *b3, *b4, *bh, *c3, *c4, *ch;
        int a3l, a4l, ahl, b3l, b4l, bhl, c3l, c4l, chl;
        int b5, c5;
        
        a1 = *(int64_t*)cdb2_column_value(db, col++);
        a2 = *(int64_t*)cdb2_column_value(db, col++);
        a3 = cdb2_column_value(db, col);
        a3l = cdb2_column_size(db, col++);
        a4 = cdb2_column_value(db, col);
        a4l = cdb2_column_size(db, col++);
        ah = cdb2_column_value(db, col);
        ahl = cdb2_column_size(db, col++);

        b1 = *(int64_t*)cdb2_column_value(db, col++);
        b2 = *(int64_t*)cdb2_column_value(db, col++);
        b3 = cdb2_column_value(db, col);
        b3l = cdb2_column_size(db, col++);
        b4 = cdb2_column_value(db, col);
        b4l = cdb2_column_size(db, col++);
        bh = cdb2_column_value(db, col);
        bhl = cdb2_column_size(db, col++);

        c1 = *(int64_t*)cdb2_column_value(db, col++);
        c2 = *(int64_t*)cdb2_column_value(db, col++);
        c3 = cdb2_column_value(db, col);
        c3l = cdb2_column_size(db, col++);
        c4 = cdb2_column_value(db, col);
        c4l = cdb2_column_size(db, col++);
        ch = cdb2_column_value(db, col);
        chl = cdb2_column_size(db, col++);

        b5 = *(int64_t*)cdb2_column_value(db, col++);
        c5 = *(int64_t*)cdb2_column_value(db, col++);

        char g00d[] = {0x60, 0x0d};
        char f0000f[] = {0xf0, 0x00, 0x0f};
        char g00df00f[] = {0x60, 0x0d, 0xf0, 0x0f};

        if (a1 != b1 ||
            b1 != c1 ||
            a1 * 10 != a2 ||
            b1 * 100 != b2 ||
            c1 * 1000 != c2 ||
            a3l != ahl ||
            b3l != bhl ||
            c3l != chl ||
            memcmp(a3, ah, a3l) ||
            memcmp(b3, bh, b3l) ||
            memcmp(c3, ch, c3l) ||
            a4l != sizeof(g00d) ||
            b4l != sizeof(f0000f) ||
            c4l != sizeof(g00df00f) ||
            memcmp(a4, g00d, a4l) ||
            memcmp(b4, f0000f, b4l) ||
            memcmp(c4, g00df00f, c4l)
        ){
            fprintf(stderr, "%s: cdb2_next_record !unexpected! values  =>  "
                    "a1:%d a2:%d a3:%d a4:%d a-hex:%d  "
                    "b1:%d b2:%d b3:%d b4:%d b-hex:%d  "
                    "c1:%d c2:%d c3:%d c4:%d c-hex:%d  "
                    "\n", __func__,
                    a1, a2, a3l, a4l, ahl,
                    b1, b2, b3l, b4l, bhl,
                    c1, c2, c3l, c4l, chl);
            exit(1);
        } else if (c5 == 1) {
            fprintf(stdout, "%s %2d: cdb2_next_record expected values  =>  "
                    "a1:%d a2:%d a3:%d a4:%d a-hex:%d  "
                    "b1:%d b2:%d b3:%d b4:%d b-hex:%d  "
                    "c1:%d c2:%d c3:%d c4:%d c-hex:%d  "
                    "b5:%d c5:%d  "
                    "rows:%d  "
                    "\n", __func__, i,
                    a1, a2, a3l, a4l, ahl,
                    b1, b2, b3l, b4l, bhl,
                    c1, c2, c3l, c4l, chl,
                    b5, c5, counter);
        }
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "%s: cdb2_next_record rc:%d %s\n", __func__, rc, cdb2_errstr(db));
        exit(1);
    }
    cdb2_close(db);
    printf("%s %2d: rows:%d\n", __func__, i, counter);
    return NULL;
}
static void *readers(void *data)
{
    printf("%s started\n", __func__);
    pthread_t thds[16];
    int count = 0;
    while (!done) {
        ++count;
        struct timeval start, finish, elapsed;
        gettimeofday(&start, NULL);
        for (intptr_t i = 0; i < sizeof(thds) / sizeof(thds[0]); ++i) pthread_create(&thds[i], NULL, reader, (void *)i);
        for (intptr_t i = 0; i < sizeof(thds) / sizeof(thds[0]); ++i) pthread_join(thds[i], NULL);
        gettimeofday(&finish, NULL);
        timersub(&finish, &start, &elapsed);
        printf("%s  count:%d  time:%ldsec\n", __func__, count, elapsed.tv_sec);
    }
    printf("%s finished\n", __func__);
    return NULL;
}
static void test_stmt(const char *sql, int upd, int del, int max_sec)
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
            fprintf(stderr, "%s: cdb2_get_effects unexpected num_updated:%d (wanted:%d)\n", sql, effects.num_updated, upd);
            exit(1);
        }
        if (elapsed.tv_sec > max_sec) {
            ++failed_upd;
            fprintf(stderr, "%s: update took:%ldsec\n", sql, elapsed.tv_sec);
        }
    }
    if (del) {
        if (effects.num_deleted != del) {
            fprintf(stderr, "%s: cdb2_get_effects unexpected num_deleted:%d (wanted:%d)\n", sql, effects.num_deleted, del);
            exit(1);
        }
        if (elapsed.tv_sec > max_sec) {
            ++failed_del;
            fprintf(stderr, "%s: delete took:%ldsec\n", sql, elapsed.tv_sec);
        }
    }
    cdb2_close(db);
    int inco = num_incoherent();
    total_incoherent += inco;
    if (inco) fprintf(stderr, "%s incoherent:%d time:%ldsec\n", sql, inco, elapsed.tv_sec);
}
static void update(void)
{
    printf("%s started\n", __func__);
    struct timeval start, finish, elapsed;
    gettimeofday(&start, NULL);
    char buf[1024];
    for (int i = 1, j = a2; i < j; ++i, --j) {
        snprintf(buf, sizeof(buf), "UPDATE c SET c1 = c2 / 1000 where c1 = %d", i);
        test_stmt(buf, c2, 0, 2);
        snprintf(buf, sizeof(buf), "UPDATE c SET c2 = c1 * 1000 where c1 = %d", j);
        test_stmt(buf, c2, 0, 2);

        snprintf(buf, sizeof(buf), "UPDATE b SET b1 = b2 / 100 where b1 = %d", i);
        test_stmt(buf, b2, 0, 2);
        snprintf(buf, sizeof(buf), "UPDATE b SET b2 = b1 * 100 where b1 = %d", j);
        test_stmt(buf, b2, 0, 2);

        snprintf(buf, sizeof(buf), "UPDATE a SET a1 = a2 / 10 where a1 = %d", i);
        test_stmt(buf, 1, 0, 1);
        snprintf(buf, sizeof(buf), "UPDATE a SET a2 = a1 * 10 where a1 = %d", j);
        test_stmt(buf, 1, 0, 1);
    }
    gettimeofday(&finish, NULL);
    timersub(&finish, &start, &elapsed);
    printf("%s finished time:%ldsec\n", __func__, elapsed.tv_sec);
}
static void delete(void)
{
    printf("%s started\n", __func__);
    struct timeval start, finish, elapsed;
    gettimeofday(&start, NULL);
    char buf[1024];
    for (int i = 1, j = a2; i < j; ++i, --j) {
        snprintf(buf, sizeof(buf), "DELETE FROM c WHERE c1 = %d", i);
        test_stmt(buf, 0, c2, 2);
        snprintf(buf, sizeof(buf), "DELETE FROM c WHERE c1 = %d", j);
        test_stmt(buf, 0, c2, 2);

        snprintf(buf, sizeof(buf), "DELETE FROM b WHERE b1 = %d", i);
        test_stmt(buf, 0, b2, 2);
        snprintf(buf, sizeof(buf), "DELETE FROM b WHERE b1 = %d", j);
        test_stmt(buf, 0, b2, 2);

        snprintf(buf, sizeof(buf), "DELETE FROM a WHERE a1 = %d", i);
        test_stmt(buf, 0, 1, 1);
        snprintf(buf, sizeof(buf), "DELETE FROM a WHERE a1 = %d", j);
        test_stmt(buf, 0, 1, 1);
    }
    gettimeofday(&finish, NULL);
    timersub(&finish, &start, &elapsed);
    printf("%s finished time:%ldsec\n", __func__, elapsed.tv_sec);
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

    done = 0;
    pthread_create(&thd, NULL, readers, NULL);
    sleep(1);
    /* update twice */
    update();
    wait_for_coherent();
    update();
    wait_for_coherent();
    done = 1;
    pthread_join(thd, NULL);

    done = 0;
    pthread_create(&thd, NULL, readers, NULL);
    sleep(1);
    delete();
    wait_for_coherent();
    done = 1;
    pthread_join(thd, NULL);

    return total_incoherent || failed_upd || failed_del;
}
int main(int argc, char **argv)
{
    char *conf = getenv("CDB2_CONFIG");
    if (conf) cdb2_set_comdb2db_config(conf);
    dbname = argv[1];

    who_master();
    setup();

    struct timeval start, finish, elapsed;
    gettimeofday(&start, NULL);
    int rc = runit();
    gettimeofday(&finish, NULL);
    timersub(&finish, &start, &elapsed);

    if (rc) {
        fprintf(stderr, "failed  =>  num-incoherent:%d  upd-fail:%d  del-fail:%d  time:%ldsec\n",
                total_incoherent, failed_upd, failed_del, elapsed.tv_sec);
    } else {
        printf("passed  =>  time:%ldsec\n", elapsed.tv_sec);
    }
    return rc;
}
