#undef NDEBUG
#include <assert.h>
#include <libgen.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>

#include <cdb2api.h>
#include <cdb2api_test.h>

static void hasql(const char *db)
{
    int rc;
    cdb2_effects_tp effects;
    cdb2_hndl_tp *hndl = NULL;

    rc = cdb2_open(&hndl, db, "default", 0);
    assert(rc == 0);

    /**********************************/
    
    rc = cdb2_run_statement(hndl, "create table get_effects(i int unique);");
    assert(rc == CDB2_OK);

    /**********************************/

    rc = cdb2_run_statement(hndl, "hi");
    assert(rc == CDB2ERR_PREPARE_ERROR);

    /**********************************/

    rc = cdb2_run_statement(hndl, "with foo as (select * from generate_series) select * from foo limit 5");
    assert(rc == 0);

    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        ;
    assert(rc == CDB2_OK_DONE);

    memset(&effects, 0xdb, sizeof(effects));
    rc = cdb2_get_effects(hndl, &effects);
    assert(rc == 0);
    assert(effects.num_selected == 5);

    /**********************************/

    rc = cdb2_run_statement(hndl, "set verifyretry off");
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "begin");
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "select value from generate_series(1, 100)");
    assert(rc == 0);

    memset(&effects, 0xdb, sizeof(effects));
    rc = cdb2_get_effects(hndl, &effects);
    assert(rc == 0);
    assert(effects.num_selected == 100);

    rc = cdb2_run_statement(hndl, "insert into get_effects select 1 from generate_series(1, 100)");
    assert(rc == 0);

    memset(&effects, 0xdb, sizeof(effects));
    rc = cdb2_get_effects(hndl, &effects);
    assert(rc == 0);
    assert(effects.num_selected == 100);
    assert(effects.num_inserted == 100);

    rc = cdb2_run_statement(hndl, "commit");
    assert(rc == CDB2ERR_DUPLICATE);

    rc = cdb2_close(hndl);
    assert(rc == 0);

    /******** hasql ********/

    rc = cdb2_open(&hndl, db, "default", 0);
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "set hasql on");
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "set transaction snapshot");
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "begin");
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "select value from generate_series(1, 100)");

    int i = 0;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        const char *name = cdb2_column_name(hndl, 0);
        assert(strcmp(name, "value") == 0);
        int64_t value = *(int64_t *)cdb2_column_value(hndl, 0);
        assert(value == ++i);
        if (i % 20 == 0) set_fail_next(1);
    }
    assert(rc == CDB2_OK_DONE);
    assert(i == 100);

    memset(&effects, 0xdb, sizeof(effects));
    rc = cdb2_get_effects(hndl, &effects);
    assert(rc == 0);
    assert(effects.num_selected == 100);

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
    hasql(dbname);
}
