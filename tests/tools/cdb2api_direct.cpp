#undef NDEBUG
#include <assert.h>
#include <cstdio>
#include <signal.h>
#include <string.h>

#include <cdb2api.h>

static void test(int argc, char **argv, int flag)
{
    int rc;
    cdb2_hndl_tp *hndl = NULL;

    rc = cdb2_open(&hndl, argv[1], argv[2], flag);
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "select dbname(), comdb2_host(), cluster()");
    assert(rc == 0);

    rc = cdb2_next_record(hndl);
    assert(rc == CDB2_OK);

    const char *dbname = (char *)cdb2_column_value(hndl, 0);
    assert(strcmp(dbname, argv[1]) == 0);

    if (flag == CDB2_DIRECT_CPU) {
        const char *host = (char *)cdb2_column_value(hndl, 1);
        assert(strcmp(host, argv[2]) == 0);
    }

    const char *cluster = (char *)cdb2_column_value(hndl, 2);
    assert(strcmp(cluster, argv[3]) == 0);

    rc = cdb2_next_record(hndl);
    assert(rc == CDB2_OK_DONE);

    rc = cdb2_close(hndl);
    assert(rc == 0);
}

static void load_cfg(char *db, char *tier)
{
    cdb2_hndl_tp *hndl = NULL;
    int rc = cdb2_open(&hndl, db, tier, 0);
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "select 1");
    assert(rc == 0);
    rc = cdb2_next_record(hndl);
    assert(rc == CDB2_OK);
    rc = cdb2_next_record(hndl);
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

    load_cfg(argv[1], argv[2]);
    test(argc, argv, CDB2_DIRECT_CPU);
    test(argc, argv, CDB2_RANDOM);
    test(argc, argv, CDB2_RANDOMROOM);
    test(argc, argv, CDB2_ROOM);
}
