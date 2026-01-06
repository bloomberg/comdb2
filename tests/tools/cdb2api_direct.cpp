#undef NDEBUG
#include <assert.h>
#include <cstdio>
#include <signal.h>
#include <string.h>

#include <cdb2api.h>

#include <iostream>

static void test(const char *dbname, const char *type, int flag)
{
    int rc;
    cdb2_hndl_tp *hndl = NULL;

    rc = cdb2_open(&hndl, dbname, type, flag);
    if (rc != 0) {
        std::cerr << "cdb2_open failed rc=" << rc << " " << cdb2_errstr(hndl) << std::endl;
        assert(rc == 0);
    }

    rc = cdb2_run_statement(hndl, "select comdb2_dbname(), comdb2_host(),  comdb2_sysinfo('class')");
    assert(rc == 0);

    rc = cdb2_next_record(hndl);
    assert(rc == CDB2_OK);

    const char *col_dbname = (char *)cdb2_column_value(hndl, 0);
    assert(strcmp(col_dbname, dbname) == 0);

    if (flag == CDB2_DIRECT_CPU) {
        const char *host = (char *)cdb2_column_value(hndl, 1);
        std::cout << "Host: " << host << std::endl;
        assert(strcmp(host, type) == 0);
    } else {
        const char *cluster = (char *)cdb2_column_value(hndl, 2);
        assert(strcmp(cluster, type) == 0);
    }

    rc = cdb2_next_record(hndl);
    assert(rc == CDB2_OK_DONE);

    rc = cdb2_close(hndl);
    assert(rc == 0);
}

static void load_cfg(const char *db, const char *tier)
{
    cdb2_hndl_tp *hndl = NULL;
    int rc = cdb2_open(&hndl, db, tier, CDB2_RANDOM);
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
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <dbname> <tier> <hostname>" << std::endl;
        return 1;
    }

    signal(SIGPIPE, SIG_IGN);

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    const char *dbname = argv[1];
    const char *tier = argv[2];
    const char *hostname = argv[3];

    // Load config using default tier
    load_cfg(dbname, tier);

    // Test CDB2_DIRECT_CPU with hostname
    test(dbname, hostname, CDB2_DIRECT_CPU);

    // Test other flags with tier
    test(dbname, tier, CDB2_RANDOM);
    test(dbname, tier, CDB2_RANDOMROOM);
    test(dbname, tier, CDB2_ROOM);

    return 0;
}
