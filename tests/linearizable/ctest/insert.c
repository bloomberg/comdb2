#include <alloca.h>
#include <stdarg.h>
#include <strings.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stddef.h>
#include <poll.h>
#include <stdint.h>
#include <sys/time.h>
#include <string.h>
#include <ctype.h>
#include <signal.h>
#include <assert.h>
#include <testutil.h>
#include <nemesis.h>
#include <cdb2api.h>

enum eventtypes {
    PARTITION_EVENT = 0x00000001,
    SIGSTOP_EVENT = 0x00000002,
    CLOCK_EVENT = 0x00000004
};

int nthreads = 5;
pthread_t *threads;
int id = 0, value = 0;
int runtime = 60;
int max_clock_skew = 60;
int largest = -1;
int allocated = 10000000;
int max_retries = 1000000;
int *state;
uint32_t which_events = 0;
char *dbname = NULL;
char *cltype = "dev";
char *argv0 = NULL;
int fixtest_after = 0;
int partition_master = 1;
int is_hasql = 1;
int inserts_per_txn = 1;
int exit_at_failure = 0;
int debug_trace = 0;
int select_test = 0;
int select_test_prepare_only = 0;
int select_test_bug = 0;
int select_records = 10000;
int test_dup = 0;

/* Override exit */
void exit(int status)
{
    fflush(stdout);
    fflush(stderr);
    _exit(status);
}

void usage(FILE *f)
{
    fprintf(f, "Usage: %s [ opts ]\n", argv0);
    fprintf(f, "        -d <dbname>         - set name of the test database\n");
    fprintf(f, "        -G <event>          - add event type ('partition', "
               "'sigstop', or 'clock')\n");
    fprintf(f, "        -T <numthd>         - set the number of threads\n");
    fprintf(
        f, "        -t <cltype>         - 'dev', 'alpha', 'beta', or 'prod'\n");
    fprintf(f, "        -i <count>          - insert this many records per "
               "transaction\n");
    fprintf(f, "        -M                  - partition the master\n");
    fprintf(f, "        -m <max-retries>    - set max-retries in the api\n");
    fprintf(f, "        -D                  - enable debug trace\n");
    fprintf(f, "        -o                  - run only main thread\n");
    fprintf(f, "        -F                  - fix things after test\n");
    fprintf(f, "        -e                  - exit at insert failure\n");
    fprintf(
        f, "        -x                  - test blkseq handling of dup error\n");
    fprintf(f, "        -s                  - select between inserts\n");
    fprintf(f, "        -S <records>        - set number of select records\n");
    fprintf(f, "        -r <runtime>        - set runtime in seconds\n");
    fprintf(f, "        -Y                  - prepare select test and exit\n");
    fprintf(f, "        -B                  - prepare select test BUG\n");
}

enum { OK = 1, FAILED = 2, CHECKED = 3, RECOVERED = 4, LOST = 5, UNKNOWN = 6 };

int insert(cdb2_hndl_tp **indb, const char *readnode)
{
    static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
    int i, val;
    cdb2_hndl_tp *db = (*indb);
    char cnonce_begin[100];
    int rc;
    char sql[100];

    pthread_mutex_lock(&lk);
    i = id;
    val = value;

    id += inserts_per_txn;
    value += inserts_per_txn;

    if (value > largest) largest = value;
    if (value >= allocated) {
        fprintf(stderr, "Exceeded allocated amount: exiting\n");
        myexit(__func__, __LINE__, 1);
    }
    pthread_mutex_unlock(&lk);

    /* turning this off you can use this test to detect the error where we
     * return
     * IX_DUP rather than "SERIALIZABLE_ERROR" */
    if (is_hasql) {
        rc = cdb2_run_statement(db, "set hasql on");
        if (rc) {
            fprintf(stderr, "set 2: %d run rc %d %s\n", val, rc,
                    cdb2_errstr(db));
            for (int jj = 0; jj < inserts_per_txn; jj++)
                state[val + jj] = FAILED;
            if (exit_at_failure) myexit(__func__, __LINE__, 1);
            return rc;
        }
    }

    rc = cdb2_run_statement(db, "set transaction serializable");
    if (rc) {
        fprintf(stderr, "set 1: %d run rc %d %s\n", val, rc, cdb2_errstr(db));
        for (int jj = 0; jj < inserts_per_txn; jj++)
            state[val + jj] = FAILED;
        if (exit_at_failure) myexit(__func__, __LINE__, 1);
        return rc;
    }

    rc = cdb2_run_statement(db, "begin");

    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__,
                 "begin: %d run rc %d %s read node %s\n", val, rc,
                 cdb2_errstr(db), readnode);
        for (int jj = 0; jj < inserts_per_txn; jj++)
            state[val + jj] = FAILED;

        if (exit_at_failure) myexit(__func__, __LINE__, 1);
        return rc;
    } else {
        if (debug_trace) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "begin: %d read node %s (success)\n", val, readnode);
        }
    }

    strncpy(cnonce_begin, cdb2_cnonce(db), sizeof(cnonce_begin));

    for (int j = 0; j < inserts_per_txn; j++) {
        snprintf(sql, sizeof(sql),
                 "insert into jepsen(id, value) values(%d, %d)", i + j,
                 val + j);
        rc = cdb2_run_statement(db, sql);
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX insert: %d run rc %d %s read node %s : %s\n", val + j,
                     rc, cdb2_errstr(db), readnode,
                     rc == 299 ? "FAIL THIS TEST" : "");

            if (exit_at_failure) myexit(__func__, __LINE__, 1);

            for (int jj = 0; jj < inserts_per_txn; jj++) {
                if (rc == -109)
                    state[val + jj] = UNKNOWN;
                else
                    state[val + jj] = FAILED;
            }

            return rc;
        }
    }

    // Verify that we see all of the records in t1 in the correct order ..
    if (select_test) {
        int v;
        if (debug_trace)
            tdprintf(stderr, db, __func__, __LINE__, "select test\n");

        cdb2_run_statement(db, "select a from t1 order by a");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX select line %d: error %s\n", __LINE__,
                     cdb2_errstr(db));
            if (exit_at_failure) myexit(__func__, __LINE__, 1);
        }

        int expected = 0;

        rc = cdb2_next_record(db);
        while (rc == CDB2_OK) {
            v = (int)*(long long *)cdb2_column_value(db, 0);
            if (v != expected) {
                tdprintf(stderr, db, __func__, __LINE__,
                         "XXX select line %d: error %s expected value %d but "
                         "got %d\n",
                         __LINE__, cdb2_errstr(db), expected, v);
                if (exit_at_failure) myexit(__func__, __LINE__, 1);
            }
            expected = (v + 1);

            rc = cdb2_next_record(db);
        }

        if (rc != CDB2_OK_DONE) {
            tdprintf(
                stderr, db, __func__, __LINE__,
                "XXX select line %d: error %s expected ok-done but got %d\n",
                __LINE__, cdb2_errstr(db), rc);
        }
        if (expected != select_records) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX select line %d: error %s expected max record of %d "
                     "but got %d\n",
                     __LINE__, cdb2_errstr(db), select_records - 1,
                     expected - 1);
        }
    }

    rc = cdb2_run_statement(db, "commit");
    if (rc) {

        tdprintf(stderr, db, __func__, __LINE__,
                 "XXX commit: %d to %d run rc %d %s read node %s\n", val,
                 val + inserts_per_txn, rc, cdb2_errstr(db), readnode);

        if (rc == 299) exit(1);
        for (int j = 0; j < inserts_per_txn; j++) {
            if (rc == -109)
                state[val + j] = UNKNOWN;
            else
                state[val + j] = FAILED;
        }
        if (exit_at_failure) myexit(__func__, __LINE__, 1);
        return rc;
    }

    rc = cdb2_next_record(db);
    while (rc == CDB2_OK) {
        rc = cdb2_next_record(db);
    }

    for (int jj = 0; jj < inserts_per_txn; jj++) {
        if (rc == CDB2_OK_DONE)
            state[val + jj] = OK;
        else
            state[val + jj] = FAILED;
    }

    if (rc != CDB2_OK_DONE) {
        tdprintf(stderr, db, __func__, __LINE__,
                 "insert: %d next rc %d %s read node %s\n", val, rc,
                 cdb2_errstr(db), readnode);
        if (exit_at_failure) myexit(__func__, __LINE__, 1);
    }

    if (rc == CDB2_OK_DONE && test_dup) {

        rc = cdb2_run_statement(db, "begin");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "begin: %d run rc %d %s read node %s\n", val, rc,
                     cdb2_errstr(db), readnode);
            state[val] = FAILED;
            if (exit_at_failure) myexit(__func__, __LINE__, 1);
            return rc;
        }

        strncpy(cnonce_begin, cdb2_cnonce(db), sizeof(cnonce_begin));

        snprintf(sql, sizeof(sql),
                 "insert into jepsen(id, value) values(%d, %d)", i, val);
        rc = cdb2_run_statement(db, sql);
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX insert: %d run rc %d %s read node %s\n", val, rc,
                     cdb2_errstr(db), readnode);
        }

        rc = cdb2_run_statement(db, "commit");
        if (rc != CDB2ERR_DUPLICATE) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX commit: %d run rc %d %s read node %s THIS SHOULD "
                     "HAVE RETURNED DUP\n",
                     val, rc, cdb2_errstr(db), readnode);
            if (exit_at_failure) myexit(__func__, __LINE__, 1);
            return rc;
        } else {
            if (debug_trace)
                tdprintf(stderr, db, __func__, __LINE__,
                         "commit: %d run rc %d %s read node %s SUCCESSFULLY "
                         "RETURNED DUP\n",
                         val, rc, cdb2_errstr(db), readnode);
        }
    }

    return rc;
}

void *thd(void *arg)
{
    int64_t now = timems(), end = now + runtime * 1000;
    char *readnode;
    cdb2_hndl_tp *db;
    int rc;

    rc = cdb2_open(&db, dbname, cltype, CDB2_RANDOM);
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "thd: open rc %d %s\n", rc,
                 cdb2_errstr(db));
        cdb2_close(db);
        myexit(__func__, __LINE__, 1);
    }

    if (is_hasql) {
        rc = cdb2_run_statement(db, "set hasql on");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "thd: set hasql on rc %d, %s\n", rc, cdb2_errstr(db));
            cdb2_close(db);
            myexit(__func__, __LINE__, 1);
        }
    }

    cdb2_set_max_retries(max_retries);

    if (debug_trace) {
        cdb2_set_debug_trace(db);
    }

    if (!(readnode = read_node(db)))
        tdprintf(stderr, db, __func__, __LINE__,
                 "Couldn't determine read node for thread\n");
    else {
        if (debug_trace)
            tdprintf(stderr, db, __func__, __LINE__, "read node is %s\n",
                     readnode);
    }

    while (now < end) {
        insert(&db, readnode);
        now = timems();
    }
    cdb2_close(db);
    if (readnode) free(readnode);
    return NULL;
}

void check(void)
{
    cdb2_hndl_tp *db;
    int rc, reopen_count = 0;

reopen:
    rc = cdb2_open(&db, dbname, cltype, 0);
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "check: open rc %d %s\n", rc,
                 cdb2_errstr(db));
        cdb2_close(db);
        myexit(__func__, __LINE__, 1);
    }
    cdb2_set_max_retries(max_retries);
    if (debug_trace) {
        cdb2_set_debug_trace(db);
    }

    rc = cdb2_run_statement(db, "set hasql on");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__,
                 "set serializable: run rc %d %s\n", rc, cdb2_errstr(db));
        cdb2_close(db);
        myexit(__func__, __LINE__, 1);
    }

    rc = cdb2_run_statement(db, "set transaction serializable");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__,
                 "set serializable: run rc %d %s\n", rc, cdb2_errstr(db));
        cdb2_close(db);
        myexit(__func__, __LINE__, 1);
    }

    char *host = read_node(db);
    ;
    tdprintf(stderr, db, __func__, __LINE__, "check results run on %s\n", host);

    rc = cdb2_run_statement(db, "select value from jepsen order by value");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "check: run rc %d %s\n", rc,
                 cdb2_errstr(db));
        cdb2_close(db);
        if (rc == 402) {
            sleep(1);
            reopen_count++;
            tdprintf(stderr, db, __func__, __LINE__, "%s reopen count is %d\n",
                     __func__, reopen_count);
            goto reopen;
        }
        myexit(__func__, __LINE__, 1);
    }
    rc = cdb2_next_record(db);
    while (rc == CDB2_OK) {
        int v = (int)*(long long *)cdb2_column_value(db, 0);
        if (v < 0 || v > largest) {
            fprintf(stderr, "unexpected value %d\n", v);
            goto next;
        }
        if (state[v] != OK) {
            fprintf(stderr, "insert of %d failed, but exists in results\n", v);
            state[v] = RECOVERED;
            goto next;
        }
        state[v] = CHECKED;
    next:
        rc = cdb2_next_record(db);
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "check: next rc %d %s\n", rc, cdb2_errstr(db));
        myexit(__func__, __LINE__, 1);
    }
    for (int i = 0; i <= largest; i++) {
        if (state[i] == OK) {
            fprintf(stderr, "lost value %d\n", i);
            state[i] = LOST;
        }
    }
    cdb2_close(db);
}

#define INSERTS_FOR_T1_BUGGED 100000

int prepare_select_bug(void)
{
    uint8_t *select_array;
    cdb2_hndl_tp *db;
    int rc;

    select_array = (uint8_t *)calloc(sizeof(uint8_t), select_records + 1);
    rc = cdb2_open(&db, dbname, cltype, 0);
    cdb2_set_max_retries(max_retries);
    if (debug_trace) {
        cdb2_set_debug_trace(db);
    }

    rc = cdb2_run_statement(db, "set hasql on");

    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "%s line %d: run rc %d %s\n",
                 __func__, __LINE__, rc, cdb2_errstr(db));
        if (exit_at_failure) myexit(__func__, __LINE__, 1);
        return rc;
    }

    rc = cdb2_run_statement(db, "set transaction serializable");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "%s line %d: run rc %d %s\n",
                 __func__, __LINE__, rc, cdb2_errstr(db));
        if (exit_at_failure) myexit(__func__, __LINE__, 1);
        return rc;
    }

    int iteration = 0;
    cdb2_effects_tp effects;
    char sql[64];

    // First erase everything larger
    do {
        iteration++;
        rc = cdb2_run_statement(db, "begin");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__, "XXX %s: begin rc %d %s\n",
                     __func__, rc, cdb2_errstr(db));
            exit(1);
        }

        snprintf(sql, sizeof(sql), "delete from t1 where a >= %u limit 1000",
                 select_records);

        rc = cdb2_run_statement(db, sql);
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX %s line %d: run rc %d %s\n", __func__, __LINE__, rc,
                     cdb2_errstr(db));
            exit(1);
        }

        rc = cdb2_run_statement(db, "commit");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX %s: commit rc %d %s\n", __func__, rc,
                     cdb2_errstr(db));
            exit(1);
        }

        rc = cdb2_get_effects(db, &effects);
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX %s: get_effects rc %d %s\n", __func__, rc,
                     cdb2_errstr(db));
            exit(1);
        }
    } while (effects.num_deleted);

    // Select everything.. use the character array to mark the records that we
    // find
    rc = cdb2_run_statement(db, "select a from t1 order by a");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__,
                 "XXX %s line %d: select rc %d %s\n", __func__, __LINE__, rc,
                 cdb2_errstr(db));
        exit(1);
    }

    do {
        rc = cdb2_next_record(db);
        if (rc == CDB2_OK) {
            int a = (int)*(long long *)cdb2_column_value(db, 0);
            assert(a >= 0 && a <= select_records);
            select_array[a] = 1;
        }
    } while (rc == CDB2_OK);

    /* Check the array */
    int need_insert = 0;
    for (int i = 0; i < select_records; i++) {
        if (select_array[i] == 0) {
            need_insert = 1;
            break;
        }
    }

    if (need_insert == 0) goto done;

    int inserted_this_txn = 0;

    for (int i = 0; i < select_records; i++) {
        if (select_array[i] == 0) {
            if (inserted_this_txn == 0) {
                rc = cdb2_run_statement(db, "begin");
                if (rc) {
                    tdprintf(stderr, db, __func__, __LINE__,
                             "XXX %s line %d: begin rc %d %s\n", __func__,
                             __LINE__, rc, cdb2_errstr(db));
                    exit(1);
                }
            }
            snprintf(sql, sizeof(sql), "insert into t1(a) values(%d)", i);
            rc = cdb2_run_statement(db, sql);
            if (rc) {
                tdprintf(stderr, db, __func__, __LINE__,
                         "XXX %s line %d: run rc %d %s\n", __func__, __LINE__,
                         rc, cdb2_errstr(db));
                exit(1);
            }
            inserted_this_txn++;

            if (inserted_this_txn >= 100000) {
                rc = cdb2_run_statement(db, "commit");
                if (rc) {
                    tdprintf(stderr, db, __func__, __LINE__,
                             "XXX %s line %d: commit rc %d %s\n", __func__,
                             __LINE__, rc, cdb2_errstr(db));
                    exit(1);
                }

                inserted_this_txn = 0;

                rc = cdb2_get_effects(db, &effects);
                if (rc) {
                    tdprintf(stderr, db, __func__, __LINE__,
                             "XXX %s line %d: get_effects rc %d %s\n", __func__,
                             __LINE__, rc, cdb2_errstr(db));
                    exit(1);
                }
                assert(effects.num_inserted == INSERTS_FOR_T1_BUGGED);
            }
        }
    }
    if (inserted_this_txn > 0) {
        rc = cdb2_run_statement(db, "commit");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX %s line %d: commit rc %d %s\n", __func__, __LINE__,
                     rc, cdb2_errstr(db));
            exit(1);
        }

        rc = cdb2_get_effects(db, &effects);
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX %s line %d: get_effects rc %d %s\n", __func__,
                     __LINE__, rc, cdb2_errstr(db));
            exit(1);
        }
        assert(effects.num_inserted == inserted_this_txn);
    }

done:
    cdb2_close(db);
    free(select_array);
    return 0;
}

#define INSERTS_FOR_T1 1000

int prepare_select_test(void)
{
    uint8_t *select_array;
    cdb2_hndl_tp *db;
    int rc;

    select_array = (uint8_t *)calloc(sizeof(uint8_t), select_records + 1);
    rc = cdb2_open(&db, dbname, cltype, 0);
    cdb2_set_max_retries(max_retries);
    if (debug_trace) {
        cdb2_set_debug_trace(db);
    }

    rc = cdb2_run_statement(db, "set hasql on");

    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "%s line %d: run rc %d %s\n",
                 __func__, __LINE__, rc, cdb2_errstr(db));
        if (exit_at_failure) myexit(__func__, __LINE__, 1);
        return rc;
    }

    rc = cdb2_run_statement(db, "set transaction serializable");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "%s line %d: run rc %d %s\n",
                 __func__, __LINE__, rc, cdb2_errstr(db));
        if (exit_at_failure) myexit(__func__, __LINE__, 1);
        return rc;
    }

    int iteration = 0;
    cdb2_effects_tp effects;
    char sql[64];

    // First erase everything larger
    do {
        iteration++;
        rc = cdb2_run_statement(db, "begin");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__, "XXX %s: begin rc %d %s\n",
                     __func__, rc, cdb2_errstr(db));
            exit(1);
        }

        snprintf(sql, sizeof(sql), "delete from t1 where a >= %u limit 1000",
                 select_records);

        rc = cdb2_run_statement(db, sql);
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX %s line %d: run rc %d %s\n", __func__, __LINE__, rc,
                     cdb2_errstr(db));
            exit(1);
        }

        rc = cdb2_run_statement(db, "commit");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX %s: commit rc %d %s\n", __func__, rc,
                     cdb2_errstr(db));
            exit(1);
        }

        rc = cdb2_get_effects(db, &effects);
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX %s: get_effects rc %d %s\n", __func__, rc,
                     cdb2_errstr(db));
            exit(1);
        }
        fprintf(stderr, "Deleted %d records from t1\n", effects.num_deleted);
    } while (effects.num_deleted);

    // Select everything.. use the character array to mark the records that we
    // find
    rc = cdb2_run_statement(db, "select a from t1 order by a");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__,
                 "XXX %s line %d: select rc %d %s\n", __func__, __LINE__, rc,
                 cdb2_errstr(db));
        exit(1);
    }

    do {
        rc = cdb2_next_record(db);
        if (rc == CDB2_OK) {
            int a = (int)*(long long *)cdb2_column_value(db, 0);
            assert(a >= 0 && a <= select_records);
            select_array[a] = 1;
        }
    } while (rc == CDB2_OK);

    /* Check the array */
    int need_insert = 0;
    for (int i = 0; i < select_records; i++) {
        if (select_array[i] == 0) {
            need_insert = 1;
            break;
        }
    }

    if (need_insert == 0) goto done;

    int inserted_this_txn = 0;

    for (int i = 0; i < select_records; i++) {
        if (select_array[i] == 0) {
            if (inserted_this_txn == 0) {
                rc = cdb2_run_statement(db, "begin");
                if (rc) {
                    tdprintf(stderr, db, __func__, __LINE__,
                             "XXX %s line %d: begin rc %d %s\n", __func__,
                             __LINE__, rc, cdb2_errstr(db));
                    exit(1);
                }
            }
            snprintf(sql, sizeof(sql), "insert into t1(a) values(%d)", i);
            rc = cdb2_run_statement(db, sql);
            if (rc) {
                tdprintf(stderr, db, __func__, __LINE__,
                         "XXX %s line %d: run rc %d %s\n", __func__, __LINE__,
                         rc, cdb2_errstr(db));
                exit(1);
            }
            inserted_this_txn++;

            if (inserted_this_txn >= INSERTS_FOR_T1) {
                rc = cdb2_run_statement(db, "commit");
                if (rc) {
                    tdprintf(stderr, db, __func__, __LINE__,
                             "XXX %s line %d: commit rc %d %s\n", __func__,
                             __LINE__, rc, cdb2_errstr(db));
                    exit(1);
                }

                inserted_this_txn = 0;

                rc = cdb2_get_effects(db, &effects);
                if (rc) {
                    tdprintf(stderr, db, __func__, __LINE__,
                             "XXX %s line %d: get_effects rc %d %s\n", __func__,
                             __LINE__, rc, cdb2_errstr(db));
                    exit(1);
                }
                assert(effects.num_inserted == INSERTS_FOR_T1);
            }
        }
    }
    if (inserted_this_txn > 0) {
        rc = cdb2_run_statement(db, "commit");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX %s line %d: commit rc %d %s\n", __func__, __LINE__,
                     rc, cdb2_errstr(db));
            exit(1);
        }

        rc = cdb2_get_effects(db, &effects);
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX %s line %d: get_effects rc %d %s\n", __func__,
                     __LINE__, rc, cdb2_errstr(db));
            exit(1);
        }
        assert(effects.num_inserted == inserted_this_txn);
    }

done:
    cdb2_close(db);
    free(select_array);
    return 0;
}

int clear(void)
{
    cdb2_hndl_tp *db;
    int rc;

    rc = cdb2_open(&db, dbname, cltype, 0);
    if (rc) {
        fprintf(stderr, "clear: open rc %d %s\n", rc, cdb2_errstr(db));
        cdb2_close(db);
        myexit(__func__, __LINE__, 1);
    }
    cdb2_set_max_retries(max_retries);
    if (debug_trace) {
        cdb2_set_debug_trace(db);
    }

    rc = cdb2_run_statement(db, "set hasql on");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "%s line %d: run rc %d %s\n",
                 __func__, __LINE__, rc, cdb2_errstr(db));
        if (exit_at_failure) myexit(__func__, __LINE__, 1);
        return rc;
    }

    rc = cdb2_run_statement(db, "set transaction serializable");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "%s line %d: run rc %d %s\n",
                 __func__, __LINE__, rc, cdb2_errstr(db));
        if (exit_at_failure) myexit(__func__, __LINE__, 1);
        return rc;
    }

    int iteration = 0;
    cdb2_effects_tp effects;
    do {
        iteration++;
        rc = cdb2_run_statement(db, "begin");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX clear: begin rc %d %s\n", rc, cdb2_errstr(db));
            exit(1);
        }

        rc = cdb2_run_statement(db, "delete from jepsen where 1 limit 100000");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX clear: run rc %d %s\n", rc, cdb2_errstr(db));
            exit(1);
        }

        rc = cdb2_run_statement(db, "commit");
        if (rc == 230) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "clear: commit rc %d %s - retrying\n", rc,
                     cdb2_errstr(db));
            continue;
        } else if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX clear: commit rc %d %s\n", rc, cdb2_errstr(db));
            exit(1);
        }

        rc = cdb2_get_effects(db, &effects);
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX get_effects rc %d %s\n", rc, cdb2_errstr(db));
            exit(1);
        }
    } while (effects.num_deleted);

    cdb2_close(db);
    return 0;
}

char *statestr(int num)
{
    switch (num) {
    case OK:
    case CHECKED: return "ok";
    case FAILED: return "failed";
    case RECOVERED: return "recovered";
    case LOST: return "lost";
    case UNKNOWN: return "unknown";
    default: return "???";
    }
}

int main(int argc, char *argv[])
{

    int rc, c, errors = 0;
    argv0 = argv[0];

    /* This is dumb.. i can ask the api */
    // setcluster("m1,m2,m3,m4,m5");

    while ((c = getopt(argc, argv, "G:d:t:c:T:i:hMFeoxDsS:YBm:")) != EOF) {
        switch (c) {
        case 'd': dbname = optarg; break;
        case 'G':
            if (0 == strcasecmp(optarg, "partition")) {
                which_events |= PARTITION_EVENT;
            } else if (0 == strcasecmp(optarg, "sigstop")) {
                which_events |= SIGSTOP_EVENT;
            } else if (0 == strcasecmp(optarg, "clock")) {
                which_events |= CLOCK_EVENT;
            } else {
                fprintf(stderr, "Unknown test: %s\n", optarg);
                errors++;
            }
            break;
        case 'm': max_retries = atoi(optarg); break;
        case 'e': exit_at_failure = 1; break;
        case 's': select_test = 1; break;
        case 'S': select_records = atoi(optarg); break;
        case 'Y': select_test_prepare_only = 1; break;
        case 'B': select_test_bug = 1; break;
        case 't': cltype = optarg; break;
        case 'i': inserts_per_txn = atoi(optarg); break;
        case 'h': is_hasql = 1; break;
        case 'F': fixtest_after = 1; break;
        case 'x': test_dup = 1; break;
        case 'o': nthreads = 1; break;
        case 'D': debug_trace = 1; break;
        case 'M': partition_master = 1; break;
        case 'T': nthreads = atoi(optarg); break;
        case 'r': runtime = atoi(optarg); break;
        default:
            fprintf(stderr, "Unknown option, '%c'\n", c);
            myexit(__func__, __LINE__, 1);
            break;
        }
    }

    if (errors) {
        fprintf(stderr, "there were errors, exiting\n");
        usage(stdout);
        exit(1);
    }

    if (!dbname) {
        fprintf(stderr, "dbname is not specified\n");
        usage(stdout);
        exit(1);
    }

    if (which_events == 0) {
        fprintf(stderr, "NO TESTS SPECIFIED .. THIS SHOULD BE AN EASY RUN..\n");
    }

    uint32_t flags = 0;
    if (partition_master) flags |= NEMESIS_PARTITION_MASTER;
    if (debug_trace) flags |= NEMESIS_VERBOSE;
    struct nemesis *n = nemesis_open(dbname, cltype, flags);
    fixall(n);

    char *master_node = master(dbname, cltype);
    if (master_node)
        printf("master is %s\n", master_node);
    else
        printf("could not find master\n");
    srand(time(NULL) ^ getpid());
    setbuf(stdout, NULL);
    state = calloc(1, sizeof(int) * allocated);
    clear();

    if (select_test || select_test_prepare_only) prepare_select_test();

    if (select_test_bug) prepare_select_bug();

    if (select_test_prepare_only || select_test_bug) exit(0);

    threads = malloc(sizeof(pthread_t) * nthreads);
    for (int i = 0; i < nthreads; i++) {
        int *mali = (int *)malloc(sizeof(int));
        (*mali) = i;
        rc = pthread_create(&threads[i], NULL, thd, mali);
    }
    sleep(runtime / 2 - 1);

    if (which_events & PARTITION_EVENT) {
        breaknet(n);
    }
    if (which_events & SIGSTOP_EVENT) {
        signaldb(n, SIGSTOP, 0);
    }
    if (which_events & CLOCK_EVENT) {
        breakclocks(n, max_clock_skew);
    }

    sleep(runtime / 2 - 1);

    if (!fixtest_after) {
        if (which_events & PARTITION_EVENT) {
            fixnet(n);
        }
        if (which_events & SIGSTOP_EVENT) {
            signaldb(n, SIGCONT, 1);
        }
        if (which_events & CLOCK_EVENT) {
            fixclocks(n);
        }
    }

    sleep(2);
    printf("done\n");
    for (int i = 0; i < nthreads; i++) {
        void *p;
        rc = pthread_join(threads[i], &p);
        if (rc) {
            fprintf(stderr, "join %d rc %d %s\n", i, rc, strerror(rc));
            return 1;
        }
    }
    if (fixtest_after) {
        if (which_events & PARTITION_EVENT) {
            fixnet(n);
        }
        if (which_events & SIGSTOP_EVENT) {
            signaldb(n, SIGCONT, 1);
        }
    }
    sleep(2);
    check();
    printf("largest %d\n", largest);
    int start = 0;
    for (int i = 0; i <= largest; i++) {
        if (state[i] != state[start]) {
            printf("%d .. %d %s\n", start, i - 1, statestr(state[start]));
            start = i;
        }
    }
    if (start != largest)
        printf("%d .. %d %s\n", start, largest, statestr(state[start]));
    printf("\n");

    return 0;
}
