#include <string.h>
#include <assert.h>
#include <signal.h>

#include "cdb2api.h"
#include "cdb2api_test.h"

int run(const char *dbname, const char *tier, const char *query) {
    cdb2_hndl_tp *db;
    int rc = cdb2_open(&db, dbname, tier, 0);
    if (rc) {
        fprintf(stderr, "open %d %s\n", rc, cdb2_errstr(db));
        return 1;
    }
    rc = cdb2_run_statement(db, query);
    if (rc) {
        fprintf(stderr, "run %d %s\n", rc, cdb2_errstr(db));
        return 1;
    }
    rc = cdb2_next_record(db);
    while (rc == CDB2_OK_DONE) {
        rc = cdb2_next_record(db);
    }

    if (rc) {
        fprintf(stderr, "next %d %s\n", rc, cdb2_errstr(db));
        return 1;
    }

    cdb2_close(db);
    
    return 0;
}

int main(int argc, char *argv[]) {
    cdb2_disable_sockpool();
    setenv("COMDB2_CONFIG_MAX_LOCAL_CONNECTION_CACHE_ENTRIES", "0", 1);
    test_process_env_vars();

    signal(SIGPIPE, SIG_IGN);
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    char *dbname = argv[1];
    int nconns = 1;
    if (getenv("CLUSTER"))
        nconns = 2;

    if (argc == 3 && strcmp(argv[2],"onlyrte") == 0) {
        set_allow_pmux_route(0);
        int prev = get_num_tcp_connects();
        run(dbname, "default", "select 1");
        int nconn = get_num_tcp_connects();
        // assert(nconn - prev >= 6);
        assert(nconn - prev >= nconns + 2);
    }

    set_allow_pmux_route(1);
    int prev = get_num_tcp_connects();
    run(dbname, "default", "select 1");
    int nconn = get_num_tcp_connects();
    fprintf(stderr, "prev=%d nconn=%d\n", prev, nconn);
    // assert(nconn - prev == 4);
    assert(nconn - prev == nconns);

    return 0;
}
