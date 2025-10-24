#undef NDEBUG
#include <assert.h>
#include <cstdio>
#include <libgen.h>
#include <signal.h>
#include <unistd.h>

#include <cdb2api.h>
#include <cdb2api_test.h>

void run(const char *dbname, void (*func)(cdb2_hndl_tp*))
{
    cdb2_hndl_tp *hndl = NULL;

    int rc = cdb2_open(&hndl, dbname, "default", 0);
    assert(rc == 0);
   
    func(hndl);

    rc = cdb2_close(hndl);
    assert(rc == 0);
}

void run1(cdb2_hndl_tp *hndl)
{
    int rc = cdb2_run_statement(hndl, "SELECT 1");
    assert(rc == 0);
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    assert(rc == CDB2_OK_DONE);

    rc = cdb2_run_statement(hndl, "create table if not exists setoptionstable(i int)");
    assert(rc == 0);
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    assert(rc == CDB2_OK_DONE);

    cdb2_effects_type ef;
    do {
        rc = cdb2_run_statement(hndl, "delete from setoptionstable where 1 limit 1000");
        assert(rc == 0);
        rc = cdb2_get_effects(hndl, &ef);
        assert(rc == 0);
    } while (ef.num_deleted > 0);
}

void run2(cdb2_hndl_tp *hndl)
{
    int rc;
    rc = cdb2_run_statement(hndl, "set transaction blocksql");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "set transaction chunk 1000");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "begin");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "insert into setoptionstable select value from generate_series(1, 100001)");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "commit");
    if (rc)
        fprintf(stderr, "Failed with rc: %d: err:%s\n", rc, cdb2_errstr(hndl));
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "set transaction snapshot");
    assert(rc == 0);
}

void run3(cdb2_hndl_tp *hndl)
{
    int rc;
    rc = cdb2_run_statement(hndl, "set transaction blocksql");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "set transaction chunk 1000");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "begin");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "delete from setoptionstable where 1");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "commit");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "set transaction blocksql");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "set transaction chunk 1000");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "begin");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "insert into setoptionstable select value from generate_series(1, 100001)");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "commit");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "set transaction snapshot");
    assert(rc == 0);
}

void run4(cdb2_hndl_tp *hndl)
{
    int rc;
    rc = cdb2_run_statement(hndl, "set transaction blocksql");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "set transaction chunk 1000");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "begin");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "delete from setoptionstable where 1");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "commit");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "set transaction blocksql");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "set transaction chunk 1000");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "begin");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "insert into setoptionstable select value from generate_series(1, 100001)");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "commit");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "set transaction snapshot");
    assert(rc == 0);
}

void run5(cdb2_hndl_tp *hndl)
{
    int rc;
    rc = cdb2_run_statement(hndl, "set transaction chunk 1000");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "begin");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "delete from setoptionstable where 1");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "commit");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "set transaction chunk 1000");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "begin");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "insert into setoptionstable select value from generate_series(1, 100001)");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "commit");
    assert(rc != 0);
}

void run6(cdb2_hndl_tp *hndl)
{
    int rc;

    rc = cdb2_run_statement(hndl, "drop table if exists maxcost");
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "create table maxcost(i int)");
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "insert into maxcost select value from generate_series(1, 2)");
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "set querylimit maxcost 10"); /* find-first: 10 */
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "select * from maxcost");
    assert(rc == 0);

    rc = cdb2_next_record(hndl);
    assert(rc == 0);

    rc = cdb2_next_record(hndl);
    assert(rc == CDB2ERR_QUERYLIMIT);
}

int main(int argc, char **argv)
{ 
    const char *dbname;
    signal(SIGPIPE, SIG_IGN);
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    if (argc != 2) {
        fprintf(stderr, "Usage: %s dbname\n", argv[0]);
        exit(1);
    }

    dbname = argv[1];

    run(dbname, run1);
    run(dbname, run2);
    run(dbname, run3);
    run(dbname, run4);

#   if 0
    /* disable fix test */
    const char *info = "comdb2_feature:disable_fix_last_set=on\n";
    cdb2_set_comdb2db_info((char*)info);
    run(dbname, run5);
    info = "comdb2_feature:disable_fix_last_set=off\n";
    cdb2_set_comdb2db_info((char*)info);
#   endif

    run(dbname, run6);

    printf("%s - pass\n", basename(argv[0]));

    return 0;

}
