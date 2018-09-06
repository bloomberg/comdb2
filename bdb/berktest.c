#include <build/db.h>
#include "bdb_int.h"
#include <unistd.h>
#include <stdlib.h>
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

static DB_ENV *dbenv = NULL;
static u_int32_t commit_delay_ms = 2;

/* Common among tables */
typedef struct globalopts {
    u_int8_t checksums;
    u_int8_t encrypt;
    u_int8_t lorder;
    u_int8_t nosync;
} global_t;

global_t globals = {1, 0, 1, 1};

/* Table specific options */
typedef struct berktable {
    char name[64];
    u_int32_t pagesize;
    int keysize;
    int datasize;
    DB *dbp;
} berktable_t;

static inline int set_global_flags(DB *dbp)
{
    if (globals.checksums)
        if (dbp->set_flags(dbp, DB_CHKSUM) != 0)
            abort();

    if (globals.encrypt)
        if (dbp->set_flags(dbp, DB_ENCRYPT) != 0)
            abort();

    if (globals.lorder)
        if (dbp->set_lorder(dbp, 4321) != 0)
            abort();

    return 0;
}

static int create_and_open(berktable_t *table)
{
    int rc;
    DB_TXN *tid;

    if ((rc = db_create(&table->dbp, dbenv, 0)) != 0) {
        logmsg(LOGMSG_ERROR, "%s db_create error: %d\n", __func__, rc);
        return rc;
    }

    set_global_flags(table->dbp);
    table->dbp->set_pagesize(table->dbp, table->pagesize);

    if ((rc = dbenv->txn_begin(dbenv, NULL, &tid, 0)) != 0) {
        logmsg(LOGMSG_ERROR, "%s txn_begin error: %d\n", __func__, rc);
        return rc;
    }

    if ((rc = table->dbp->open(table->dbp, tid, table->name, NULL, DB_BTREE,
                               DB_THREAD | DB_CREATE | DB_DATAFILE, 0666)) !=
        0) {
        logmsg(LOGMSG_ERROR, "%s open error: %d\n", __func__, rc);
        return rc;
    }

    if ((rc = tid->commit(tid, 0)) != 0) {
        logmsg(LOGMSG_ERROR, "%s txn commit error: %d\n", __func__, rc);
        return rc;
    }

    return 0;
}

static void close_tables(berktable_t *tables, int tablecount)
{
    int i, rc;
    char new[PATH_MAX];
    berktable_t *table;

    for (i = 0; i < tablecount; i++) {
        table = &tables[i];
        if (table->dbp) {
            if ((rc = table->dbp->close(table->dbp, 0)) != 0) {
                logmsg(LOGMSG_ERROR, "%s close error: %d\n", __func__, rc);
                continue;
            }
            unlink(bdb_trans(table->name, new));
        }
    }
    free(tables);
}

static berktable_t *create_tables(int *tablecount)
{
    berktable_t *tables = calloc(sizeof(berktable_t), 2), *table;
    u_int32_t uniq = getpid() * random();
    char new[PATH_MAX];
    int count, rc;

    /* Index */
    count = 0;
    table = &tables[count];
    snprintf(table->name, sizeof(table->name), "XXX._berktest%d.index", uniq);
    unlink(bdb_trans(table->name, new));
    table->pagesize = 4096;
    table->keysize = 13;
    table->datasize = 8;
    if ((rc = create_and_open(table)) != 0) {
        close_tables(tables, count);
        free(tables);
        return NULL;
    }

    /* Data file */
    count++;
    table = &tables[count];
    snprintf(table->name, sizeof(table->name), "XXX._berktest%d.data", uniq);
    unlink(bdb_trans(table->name, new));
    table->pagesize = 4096;
    table->keysize = 8;
    table->datasize = 12;
    if ((rc = create_and_open(table)) != 0) {
        close_tables(tables, count);
        free(tables);
        return NULL;
    }

    *tablecount = count + 1;
    return tables;
}

static u_int32_t maxkey(berktable_t *tables, int tablecount)
{
    u_int32_t max = 0;
    int i;
    for (i = 0; i < tablecount; i++) {
        berktable_t *table = &tables[i];
        if (table->keysize > max)
            max = table->keysize;
    }
    return max;
}

static u_int32_t maxdata(berktable_t *tables, int tablecount)
{
    u_int32_t max = 0;
    int i;
    for (i = 0; i < tablecount; i++) {
        berktable_t *table = &tables[i];
        if (table->datasize > max)
            max = table->datasize;
    }
    return max;
}

static void run_test(berktable_t *tables, int tablecount, int txnsize,
                     int iterations)
{
    uint64_t start, end, totalrecs = (txnsize * iterations), persecond,
                         persecnorm;
    DB_TXN *tid;
    DBT key = {0}, data = {0};
    u_int32_t commit_flags, mkey, mdata, *keyptr;
    int i, j, k, rc;

    mkey = maxkey(tables, tablecount);
    mdata = maxdata(tables, tablecount);

    key.data = calloc(mkey, 1);
    data.data = calloc(mdata, 1);

    keyptr = (u_int32_t *)key.data;
    *keyptr = 0;

    start = gettimeofday_ms();
    commit_flags = globals.nosync ? DB_TXN_NOSYNC : 0;

    for (i = 0; i < iterations; i++) {
        if ((rc = dbenv->txn_begin(dbenv, NULL, &tid, 0)) != 0) {
            logmsg(LOGMSG_ERROR, "%s txn_begin error: %d iteration %d\n", __func__,
                    rc, i);
            return;
        }

        for (j = 0; j < txnsize; j++) {
            for (k = 0; k < tablecount; k++) {
                berktable_t *table = &tables[k];
                key.size = table->keysize;
                data.size = table->datasize;
                if ((rc = table->dbp->put(table->dbp, tid, &key, &data, 0)) !=
                    0) {
                    logmsg(LOGMSG_ERROR, "%s error putting record, %d\n", __func__,
                            rc);
                    tid->abort(tid);
                    return;
                }
            }
            (*keyptr)++;
        }

        if ((rc = tid->commit(tid, commit_flags)) != 0) {
            logmsg(LOGMSG_ERROR, "%s txn commit error: %d iteration %d\n", __func__,
                    rc, i);
            return;
        }

        if (commit_delay_ms) {
            usleep(commit_delay_ms * 1000);
        }
    }

    end = gettimeofday_ms();

    persecond = (1000 * totalrecs) / (end - start);
    logmsg(LOGMSG_USER,
           "%lu records per second for txnsize %d, commit-delay %d "
           "iterations: %d total time %lu\n",
           persecond, txnsize, commit_delay_ms, iterations, end - start);
    fflush(stdout);
}

static void berktest(int txnsize, int iterations)
{
    berktable_t *tables;
    int tablecount;

    if ((tables = create_tables(&tablecount)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s couldn't create tables\n", __func__);
        return;
    }
    run_test(tables, tablecount, txnsize, iterations);
    close_tables(tables, tablecount);
}

void bdb_berktest_multi(void *_bdb_state)
{
    bdb_state_type *bdb_state = _bdb_state;
    dbenv = bdb_state->dbenv;
    berktable_t *tables;
    int tablecount, iterations = 50;

    if ((tables = create_tables(&tablecount)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s couldn't create tables\n", __func__);
        return;
    }

    run_test(tables, tablecount, 500000, iterations);
    run_test(tables, tablecount, 200000, iterations);
    run_test(tables, tablecount, 100000, iterations);
    run_test(tables, tablecount, 50000, iterations);
    run_test(tables, tablecount, 20000, iterations);
    run_test(tables, tablecount, 10000, iterations);
    run_test(tables, tablecount, 5000, iterations);
    run_test(tables, tablecount, 2000, iterations);
    run_test(tables, tablecount, 1000, iterations);
    run_test(tables, tablecount, 500, iterations);
    run_test(tables, tablecount, 200, iterations);
    run_test(tables, tablecount, 100, iterations);
    run_test(tables, tablecount, 50, iterations);
    run_test(tables, tablecount, 20, iterations);
    run_test(tables, tablecount, 10, iterations);
    run_test(tables, tablecount, 5, iterations);
    run_test(tables, tablecount, 2, iterations);
    run_test(tables, tablecount, 1, iterations);
    close_tables(tables, tablecount);
}

void bdb_berktest_commit_delay(u_int32_t delayms) { commit_delay_ms = delayms; }

void bdb_berktest(void *_bdb_state, u_int32_t txnsize)
{
    bdb_state_type *bdb_state = _bdb_state;
    dbenv = bdb_state->dbenv;
    berktest(txnsize, 50);
}
