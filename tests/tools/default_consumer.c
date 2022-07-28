#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include <cdb2api.h>

/* order of columns emitted from default consumers */
enum {
    COMDB2_EVENT,
    COMDB2_ID,
    COMDB2_TID,
    COMDB2_SEQ,
    COMDB2_EPOCH,
};

static const char *dbname, *tier = "default";

static int run_stmt(cdb2_hndl_tp *db, const char *stmt)
{
    int rc = cdb2_run_statement(db, stmt);
    if (rc != CDB2_OK) {
        fprintf(stderr, "cdb2_run_statment sql:%s err:%d errstr:%s\n", stmt, rc, cdb2_errstr(db));
        return EXIT_FAILURE;
    }
    do {
        rc = cdb2_next_record(db);
    } while (rc == CDB2_OK);
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "cdb2_next_record sql:%s err:%d errstr:%s\n", stmt, rc, cdb2_errstr(db));
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

static void should_pass(cdb2_hndl_tp *db, const char *stmt)
{
    if (run_stmt(db, stmt) != EXIT_SUCCESS) abort();
}

static void should_fail(cdb2_hndl_tp *db, const char *stmt)
{
    if (run_stmt(db, stmt) != EXIT_FAILURE) abort();
}

static cdb2_hndl_tp *db_handle(void)
{
    cdb2_hndl_tp *hndl;
    int rc = cdb2_open(&hndl, dbname, tier, 0);
    if (rc != 0) {
        fprintf(stderr, "cdb2_open err:%d errstr:%s\n", rc, cdb2_errstr(hndl));
        abort();
    }
    return hndl;
}

static void test_register_timeout(void)
{
    cdb2_hndl_tp *db = db_handle();
    int rc = cdb2_run_statement(db, "EXEC PROCEDURE batch_consume('{\"register_timeout\":1000}')");
    if (rc != 0) {
        fprintf(stderr, "cdb2_run_statement err:%d errstr:%s\n", rc, cdb2_errstr(db));
        abort();
    }
    rc = cdb2_next_record(db);
    if (rc != CDB2_OK) {
        fprintf(stderr, "cdb2_next_record err:%d errstr:%s\n", rc, cdb2_errstr(db));
        abort();
    }
    if (strcmp("register_timeout", cdb2_column_value(db, COMDB2_EVENT)) != 0) {
        fprintf(stderr, "did not receive register_timeout\n");
        abort();
    }
    cdb2_close(db);
}

int main(int argc, char **argv)
{
    switch (argc) {
    case 4: cdb2_set_comdb2db_config(argv[3]);
    case 3: tier = argv[2];
    case 2: dbname = argv[1];
    }

    cdb2_hndl_tp *hndl_0 = db_handle();

    run_stmt(hndl_0, "DROP LUA CONSUMER batch_consume"); /* might fail, if does not exist */
    should_pass(hndl_0, "DROP TABLE IF EXISTS t");

    should_pass(hndl_0, "CREATE TABLE t(i INTEGER)");
    should_pass(hndl_0, "CREATE DEFAULT LUA CONSUMER batch_consume WITH SEQUENCE ON (TABLE t FOR INSERT)");

    should_pass(hndl_0, "SET MAXTRANSIZE 10");
    should_fail(hndl_0, "INSERT INTO T SELECT value FROM generate_series(1, 100)");

    should_pass(hndl_0, "SET MAXTRANSIZE 100");
    should_pass(hndl_0, "INSERT INTO T SELECT value FROM generate_series(1, 100)");
    should_pass(hndl_0, "INSERT INTO T SELECT value FROM generate_series(101, 200)");

    should_fail(hndl_0, "EXEC PROCEDURE batch_consume('{\"foo\":246}')");

    cdb2_hndl_tp *hndl_1 = db_handle();

    const char *sp = "EXEC PROCEDURE batch_consume('{\"with_epoch\":true, \"with_tid\":true, \"with_sequence\":true, \"batch_consume\":true, \"poll_timeout\":0 }')";
    cdb2_run_statement(hndl_1, sp);
    for (int j = 0; j < 50; ++j) {
        if (cdb2_next_record(hndl_1) != CDB2_OK) {
            fprintf(stderr, "cdb2_next_record errstr:%s\n", cdb2_errstr(hndl_1));
            abort();
        }
    }
    cdb2_clear_ack(hndl_1);
    cdb2_close(hndl_1);

    /* Did't consume entire transaction, so events should restart.. */
    hndl_1 = db_handle();
    cdb2_run_statement(hndl_1, sp);
    const char *cols[] = {
        "comdb2_event",
        "comdb2_id",
        "comdb2_tid",
        "comdb2_sequence",
        "comdb2_epoch",
        "i",
    };
    int rc, num_columns = sizeof(cols) / sizeof(cols[0]);
    int64_t comdb2_id, comdb2_tid;
    for (int j = 0; j < 200; ++j) {
        rc = cdb2_next_record(hndl_1);
        if (rc != CDB2_OK) {
            fprintf(stderr, "cdb2_next_record err:%d errstr:%s\n", rc, cdb2_errstr(hndl_1));
            abort();
        }
        if (j == 0) {
            if ((rc = cdb2_numcolumns(hndl_1)) != num_columns) {
                fprintf(stderr, "cdb2_num_columns expected:%d got:%d\n", num_columns, rc);
                abort();
            }
            for (int i = 0; i < num_columns; ++i) {
                if (strcmp(cdb2_column_name(hndl_1, i), cols[i]) != 0) {
                    fprintf(stderr, "cdb2_column_name[%d] expected:%s got:%s\n", i, cols[i], cdb2_column_name(hndl_1, i));
                    abort();
                }
            }
        }
        if (j == 0 || j == 100) {
            if (cdb2_column_size(hndl_1, 2) != sizeof(comdb2_id)) {
                fprintf(stderr, "bad size for comdb2_id expected:%zu got:%d\n", sizeof(comdb2_id), cdb2_column_size(hndl_1, 2));
                abort();
            }
            comdb2_id = *(int64_t *)cdb2_column_value(hndl_1, COMDB2_ID);
            comdb2_tid = *(int64_t *)cdb2_column_value(hndl_1, COMDB2_TID);
        } else {
            int64_t id = *(int64_t *)cdb2_column_value(hndl_1, COMDB2_ID);
            if (comdb2_id == id) {
                fprintf(stderr, "comdb2_id did not change j:%d id:%" PRId64 "\n", j, id);
                abort();
            }
            comdb2_id = id;

            int64_t tid = *(int64_t *)cdb2_column_value(hndl_1, COMDB2_TID);
            if (comdb2_tid != tid) {
                fprintf(stderr, "comdb2_tid changed j:%d expected:%" PRId64 " got:%" PRId64 "\n", j, comdb2_tid, tid);
                abort();
            }
        }
        int seq = *(int64_t *)cdb2_column_value(hndl_1, COMDB2_SEQ);
        int i = *(int64_t *)cdb2_column_value(hndl_1, 5);
        if (seq != i) {
            fprintf(stderr, "cdb2_column_value j:%d expected:%d got:%d\n", j, seq, i);
            abort();
        }
    }
    rc = cdb2_next_record(hndl_1);
    if (rc != CDB2_OK) {
        fprintf(stderr, "cdb2_next_record err:%d errstr:%s\n", rc, cdb2_errstr(hndl_1));
        abort();
    }
    if (strcmp("poll_timeout", cdb2_column_value(hndl_1, COMDB2_EVENT)) != 0) {
        fprintf(stderr, "did not receive poll_timeout\n");
        abort();
    }

    test_register_timeout(); /* while hndl_1 is active consumer */

    should_pass(hndl_0, "DROP LUA CONSUMER batch_consume");

    rc = cdb2_next_record(hndl_1); /* should fail */
    if (rc == CDB2_OK || rc == CDB2_OK_DONE) {
        fprintf(stderr, "should have failed because consumer was dropped\n");
        abort();
    }

    cdb2_close(hndl_1);
    cdb2_close(hndl_0);

    puts("passed default-consumer");

    return EXIT_SUCCESS;
}
