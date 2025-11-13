#undef NDEBUG
#include <assert.h>
#include <cstdio>
#include <libgen.h>
#include <signal.h>

#include <cdb2api.h>
#include <cdb2api_test.h>

static void txn(const char *db)
{
    cdb2_hndl_tp *hndl = NULL;
    int rc = cdb2_open(&hndl, db, "default", 0);
    assert(rc == 0);


    /*** handle rejection - default dbinfo setting in library */
    set_fail_reject(1);
    rc = cdb2_run_statement(hndl, "SELECT 1");
    assert(rc == 0);
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        ;
    assert(rc == CDB2_OK_DONE);


    /*** handle rejection - second run will test dbinfo from config */
    set_fail_reject(1);
    rc = cdb2_run_statement(hndl, "SELECT 1");
    assert(rc == 0);
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        ;
    assert(rc == CDB2_OK_DONE);


    /******************/
    /*** txn states ***/
    /******************/

    rc = cdb2_run_statement(hndl, "commit");
    assert(rc != 0);

    rc = cdb2_run_statement(hndl, "rollback");
    assert(rc != 0);

    rc = cdb2_run_statement(hndl, "begin");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "begin");
    assert(rc != 0);
    rc = cdb2_run_statement(hndl, "commit");
    assert(rc == 0); /* Questionable; Sould return non-zero */

    rc = cdb2_run_statement(hndl, "begin");
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "begin");
    assert(rc != 0);
    rc = cdb2_run_statement(hndl, "rollback");
    assert(rc == 0);


    rc = cdb2_run_statement(hndl, "SELECT 1");
    assert(rc == 0);
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        ;
    assert(rc == CDB2_OK_DONE);
    rc = cdb2_close(hndl);
    assert(rc == 0);
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    char *dbname = argv[1];

    txn(dbname);

    printf("%s - pass\n", basename(argv[0]));
}
