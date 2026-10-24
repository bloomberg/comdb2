#undef NDEBUG
#include <assert.h>
#include <libgen.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

#include <cdb2api.h>
#include <cdb2api_test.h>

static int run(cdb2_hndl_tp *hndl, const char *sql)
{
    int rc = cdb2_run_statement(hndl, sql);
    if (rc != 0) {
        printf("%s: rc %d (%s)\n", sql, rc, cdb2_errstr(hndl));
        return rc;
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        ;
    if (rc != CDB2_OK_DONE) {
        printf("%s: next_record rc %d (%s)\n", sql, rc, cdb2_errstr(hndl));
        return rc;
    }
    return 0;
}

static long long select_v(cdb2_hndl_tp *hndl)
{
    int rc = cdb2_run_statement(hndl, "select v from chunk_set_replay where id = 1");
    assert(rc == 0);
    rc = cdb2_next_record(hndl);
    assert(rc == CDB2_OK);
    long long v = *(long long *)cdb2_column_value(hndl, 0);
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        ;
    assert(rc == CDB2_OK_DONE);
    return v;
}

/* "set transaction chunk N" lasts one transaction. A handle that ran a chunked
 * transaction and then switched to read committed must still work after a
 * reconnect, which resends the handle's set commands to the new connection. */
int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    if (argc < 2) {
        fprintf(stderr, "usage: %s <dbname>\n", argv[0]);
        return 1;
    }
    const char *db = argv[1];

    cdb2_hndl_tp *setup = NULL;
    int rc = cdb2_open(&setup, db, "default", 0);
    assert(rc == 0);
    assert(run(setup, "drop table if exists chunk_set_replay") == 0);
    assert(run(setup, "create table chunk_set_replay (id int primary key, v int)") == 0);
    assert(run(setup, "insert into chunk_set_replay values (1, 0), (2, 0)") == 0);

    cdb2_hndl_tp *hndl = NULL;
    rc = cdb2_open(&hndl, db, "default", 0);
    assert(rc == 0);

    assert(run(hndl, "set transaction blocksql") == 0);
    assert(run(hndl, "set transaction chunk 500") == 0);
    assert(run(hndl, "begin") == 0);
    assert(run(hndl, "update chunk_set_replay set v = v + 1 where id > 0") == 0);
    assert(run(hndl, "commit") == 0);

    assert(run(hndl, "set transaction read committed") == 0);
    assert(run(hndl, "begin") == 0);
    assert(run(hndl, "update chunk_set_replay set v = 10 where id = 1") == 0);
    assert(run(hndl, "update chunk_set_replay set v = 11 where id = 1") == 0);
    assert(run(hndl, "commit") == 0);

    int connects = get_num_sql_connects();
    set_fail_read(1);
    rc = run(hndl, "select 1");
    printf("select after reconnect: rc %d, new connects %d\n", rc, get_num_sql_connects() - connects);
    assert(get_num_sql_connects() > connects);

    int failed = rc != 0;
    if (!failed) {
        /* Read committed sees its own uncommitted write; blocksql would not. */
        failed |= run(hndl, "begin") != 0;
        failed |= run(hndl, "update chunk_set_replay set v = 12 where id = 1") != 0;
        long long v = select_v(hndl);
        printf("read in transaction after reconnect: v %lld\n", v);
        failed |= v != 12;
        failed |= run(hndl, "commit") != 0;
    }

    cdb2_close(hndl);
    assert(run(setup, "drop table chunk_set_replay") == 0);
    cdb2_close(setup);

    printf("%s - %s\n", basename(argv[0]), failed ? "fail" : "pass");
    return failed;
}
