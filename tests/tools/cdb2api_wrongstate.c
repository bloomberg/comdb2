#undef NDEBUG
#include <assert.h>
#include <libgen.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cdb2api.h>
#include <cdb2api_test.h>

static int select_n(cdb2_hndl_tp *hndl, int n)
{
    char sql[32];
    snprintf(sql, sizeof(sql), "select %d", n);
    int rc = cdb2_run_statement(hndl, sql);
    if (rc != 0) {
        printf("follow-up %s rc %d (%s)\n", sql, rc, cdb2_errstr(hndl));
        return 1;
    }
    int failed = 0;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        long long got = *(long long *)cdb2_column_value(hndl, 0);
        if (got != n) {
            printf("follow-up %s returned %lld\n", sql, got);
            failed = 1;
        }
    }
    if (rc != CDB2_OK_DONE) {
        printf("follow-up %s next_record rc %d (%s)\n", sql, rc, cdb2_errstr(hndl));
        failed = 1;
    }
    return failed;
}

/* Lose the connection mid-transaction so cdb2api reconnects, fails the
 * statement with TRAN_IO_ERROR, and leaves the handle "in_trans" on a
 * connection the server knows nothing about. Then end the transaction. */
static int run(const char *db, const char *endtxn, int expected_rc)
{
    cdb2_hndl_tp *hndl = NULL;
    int rc = cdb2_open(&hndl, db, "default", 0);
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "begin");
    assert(rc == 0);

    int connects = get_num_sql_connects();
    set_fail_read(1);
    rc = cdb2_run_statement(hndl, "select 1");
    printf("%s: select rc %d (%s), new connects %d\n", endtxn, rc, cdb2_errstr(hndl),
           get_num_sql_connects() - connects);
    assert(rc == CDB2ERR_TRAN_IO_ERROR);
    assert(get_num_sql_connects() > connects);

    rc = cdb2_run_statement(hndl, endtxn);
    printf("%s: rc %d (%s)\n", endtxn, rc, cdb2_errstr(hndl));
    assert(rc == expected_rc);

    /* If the server's reply to endtxn went unread, each statement below gets
     * the reply to the one before it. Distinct values make the shift visible. */
    int failed = 0;
    for (int n = 101; n <= 103; ++n)
        failed |= select_n(hndl, n);
    printf("%s: handle replies %s\n", endtxn, failed ? "out of sync" : "in sync");

    rc = cdb2_close(hndl);
    assert(rc == 0);
    return failed;
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    if (argc < 3) {
        fprintf(stderr, "usage: %s <dbname> rollback|commit\n", argv[0]);
        return 1;
    }

    int failed;
    if (strcasecmp(argv[2], "rollback") == 0)
        failed = run(argv[1], "rollback", 0);
    else
        failed = run(argv[1], "commit", CDB2ERR_TRAN_IO_ERROR);

    printf("%s %s - %s\n", basename(argv[0]), argv[2], failed ? "fail" : "pass");
    return failed;
}
