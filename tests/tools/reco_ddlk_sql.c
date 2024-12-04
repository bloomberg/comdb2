#include <inttypes.h>
#include <pthread.h>
//#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

#include <cdb2api.h>

#define THDS 10 
static pthread_t thds[THDS];
static const int rows = 1000; /* number of rows in outer table */
static const int slice = rows / THDS;
static const char *dbname;

static int check_no_incoherent(cdb2_hndl_tp *db)
{
    cdb2_run_statement(db, "SELECT host FROM comdb2_cluster WHERE is_master='Y'");
    char *host = cdb2_column_value(db, 0);
    cdb2_hndl_tp *master;
    cdb2_open(&master, dbname, host, CDB2_DIRECT_CPU);
    cdb2_next_record(db); db = NULL;
    int rc = cdb2_run_statement(master, "SELECT count(*) from comdb2_cluster where coherent_state != 'coherent'");
    if (rc != 0) {
        fprintf(stderr, "%s cdb2_run_statement rc:%d => %s\n", __func__, rc, cdb2_errstr(master));
        exit(1);
    }
    int64_t count = *(int64_t *)cdb2_column_value(master, 0);
    cdb2_close(master);
    return count;

}

static void *updater(void *data)
{
    int id = (intptr_t)data;
    cdb2_effects_tp effects;
    int rc, num_updated = 0, retries = 0, a = id * (rows / THDS);
    printf("%s id:%d seed:%d\n", __func__, id, a);
    cdb2_hndl_tp *db;
    cdb2_open(&db, dbname, "default", 0);
    while (num_updated < 100000) {
        if (++a > rows) a = 0;
        char upd[1024];
again:  rc = cdb2_run_statement(db, "begin");
        if (rc != 0) {
            fprintf(stderr, "%s cdb2_run_statement 'begin' rc:%d -- %s\n", __func__, rc, cdb2_errstr(db));
            exit(1);
        }

        snprintf(upd, sizeof(upd), "UPDATE t SET a = a where a = %d", a);
        rc = cdb2_run_statement(db, upd);
        if (rc != 0) {
            fprintf(stderr, "%s cdb2_run_statement 't' rc:%d -- %s\n", __func__, rc, cdb2_errstr(db));
            exit(1);
        }

        snprintf(upd, sizeof(upd), "UPDATE u SET y = y where x = %d", a);
        rc = cdb2_run_statement(db, upd);
        if (rc != 0) {
            fprintf(stderr, "%s cdb2_run_statement 'u' rc:%d -- %s\n", __func__, rc, cdb2_errstr(db));
            exit(1);
        }

        rc = cdb2_run_statement(db, "commit");
        if (rc == CDB2ERR_VERIFY_ERROR) {
            ++retries;
            goto again;
        }
        if (rc != 0) {
            fprintf(stderr, "%s cdb2_run_statement 'commit' rc:%d -- %s\n", __func__, rc, cdb2_errstr(db));
            exit(1);
        }

        cdb2_get_effects(db, &effects);
        num_updated += effects.num_updated;
    }
    rc = check_no_incoherent(db);
    cdb2_close(db);
    printf("%s  thd:%d  num_updated:%d  retries:%d  incoherent:%d\n", __func__, id, num_updated, retries, rc);
    return NULL;
}

struct ins_data {
    int from;
    int to;
};

static void *inserter(void *data)
{
    cdb2_hndl_tp *db;
    cdb2_open(&db, dbname, "default", 0);
    char ins[1024];
    struct ins_data *ins_data = data;
    printf("%s from:%d -> to:%d\n", __func__, ins_data->from, ins_data->to);
    for (int i = ins_data->from; i <= ins_data->to; ++i) {
        snprintf(ins, sizeof(ins), "INSERT INTO u SELECT %d, VALUE FROM generate_series(1, 1000);", i);
        cdb2_run_statement(db, ins);
    }
    cdb2_close(db);
    return NULL;
}

static void setup(cdb2_hndl_tp *db)
{
    cdb2_run_statement(db, "DROP TABLE IF EXISTS t;");
    cdb2_run_statement(db, "DROP TABLE IF EXISTS u;");
    cdb2_run_statement(db, "CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT);");
    cdb2_run_statement(db, "CREATE TABLE IF NOT EXISTS u (x INT INDEX, y INT);");
}

static void insert(cdb2_hndl_tp *db)
{
    struct timeval a, b, c;
    gettimeofday(&a, NULL);
    void *ret;
    struct ins_data ins_data[THDS];
    for (int i = 0; i < THDS; ++i) {
        ins_data[i].from = i * slice + 1;
        ins_data[i].to = i * slice + slice;
        pthread_create(&thds[i], NULL, inserter, &ins_data[i]);
    }
    char ins[1024];
    snprintf(ins, sizeof(ins), "INSERT INTO t SELECT VALUE, VALUE * 10 FROM generate_series(1, %d);", rows);
    cdb2_run_statement(db, ins);
    for (int i = 0; i < THDS; ++i) pthread_join(thds[i], &ret);
    gettimeofday(&b, NULL);
    timersub(&b, &a, &c);
    printf("%s took:%ldsec\n", __func__, c.tv_sec);
}

static void *updaters(void *data)
{
    void *ret;
    struct timeval a, b, c;
    gettimeofday(&a, NULL);
    for (intptr_t i = 0; i < THDS; ++i) pthread_create(&thds[i], NULL, updater, (void *)i);
    for (int i = 0; i < THDS; ++i) pthread_join(thds[i], &ret);
    gettimeofday(&b, NULL);
    timersub(&b, &a, &c);
    printf("%s took:%ldsec\n", __func__, c.tv_sec);
    return NULL;
}

static void *reader(void *data)
{
    int rc;
    cdb2_hndl_tp *db;
    cdb2_open(&db, dbname, "default", 0);

    int counter = 0;
    struct timeval start, now, diff;
    cdb2_run_statement(db, "SELECT a, b, x, y from t, u where t.a = u.x order by 1");
    gettimeofday(&start, NULL);
    while ((rc = cdb2_next_record(db)) == CDB2_OK) {
        ++counter;
        gettimeofday(&now, NULL);
        timersub(&now, &start, &diff);
        if (diff.tv_sec) {
            start = now;
            int64_t a, b, x, y;
            a = *(int64_t *)cdb2_column_value(db, 0);
            b = *(int64_t *)cdb2_column_value(db, 1);
            x = *(int64_t *)cdb2_column_value(db, 2);
            y = *(int64_t *)cdb2_column_value(db, 3);
            printf("SELECT a:%"PRId64" b:%"PRId64" x:%"PRId64" y:%"PRId64" counter:%d\n", a, b, x, y, counter);
        }
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "%s: cdb2_next_record rc:%d -- %s\n", __func__, rc, cdb2_errstr(db));
        exit(1);
    }
    return NULL;
}

int main(int argc,char *argv[])
{
    void *ret;
    pid_t pid = getpid();
    srandom(pid);

    int rc;
    dbname = argv[1];

    cdb2_hndl_tp *db;
    cdb2_open(&db, dbname, "default", 0);

    //setup(db);
    //insert(db);

    pthread_t t;
    pthread_create(&t, NULL, updaters, NULL);

    struct timeval a, b, c;
    gettimeofday(&a, NULL);

    pthread_t r;
    pthread_create(&r, NULL, reader, NULL);
    pthread_join(r, &ret);

    gettimeofday(&b, NULL);
    timersub(&b, &a, &c);
    printf("reader took:%ldsec\n", c.tv_sec);

    rc = check_no_incoherent(db);
    cdb2_close(db);

    pthread_join(t, &ret);
    return rc;
}
