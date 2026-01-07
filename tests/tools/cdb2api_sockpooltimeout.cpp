#undef NDEBUG
#include <assert.h>
#include <libgen.h>
#include <signal.h>
#include <stdio.h>
#include <time.h>

#include <cdb2api.h>
#include <cdb2api_test.h>

static void sockpool_prod_timeout_test(const char *db)
{
    int rc;
    cdb2_effects_tp effects;
    cdb2_hndl_tp *hndl = NULL;

    // Should fail "quickly"
    time_t start_time = time(NULL);
    rc = cdb2_open(&hndl, db, "prod", 0);
    time_t end_time = time(NULL);
    printf("Total open time=%ld\n", end_time - start_time);
    assert(rc != 0 && (end_time - start_time) < 10);
}

// Just a simple select 1
static void sockpool_test(const char *db, int line)
{
    int rc;
    cdb2_effects_tp effects;
    cdb2_hndl_tp *hndl = NULL;

    rc = cdb2_open(&hndl, db, "default", 0);
    if (rc)
        fprintf(stderr, "Line %d: open rc=%d errstr=%s\n", line, rc,
                cdb2_errstr(hndl));
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "select 1");
    if (rc)
        fprintf(stderr, "Line %d: run_statement rc=%d errstr=%s\n", line, rc,
                cdb2_errstr(hndl));
    assert(rc == 0);

    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        ;
    assert(rc == CDB2_OK_DONE);

    rc = cdb2_close(hndl);
    assert(rc == 0);
}

int main(int argc, char **argv)
{
    const char *dbname;
    signal(SIGPIPE, SIG_IGN);

    /* bypass local cache code for this test needs to talk to sockpool */
    setenv("COMDB2_CONFIG_MAX_LOCAL_CONNECTION_CACHE_ENTRIES", "0", 1);
    test_process_env_vars();

    if (argc != 2) {
        fprintf(stderr, "Usage: %s dbname\n", argv[0]);
        exit(1);
    }
    dbname = argv[1];
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    cdb2_enable_sockpool();

    setenv("COMDB2_CONFIG_SOCKPOOL_SEND_TIMEOUTMS", "1", 1);
    setenv("COMDB2_CONFIG_SOCKPOOL_RECV_TIMEOUTMS", "1", 1);
    setenv("COMDB2_CONFIG_SOCKET_TIMEOUT", "1", 1);
    setenv("COMDB2_CONFIG_CONNECT_TIMEOUT", "1", 1);

    assert(0 == get_num_sockpool_send_timeouts());
    assert(0 == get_num_sockpool_recv_timeouts());

    // sockpool_prod_timeout_test("comdb2db");

    // prod test causes lots of actual sockpool recv timeouts
    int recv_timeouts = get_num_sockpool_recv_timeouts();

    setenv("COMDB2_CONFIG_SOCKPOOL_SEND_TIMEOUTMS", "1000", 1);
    setenv("COMDB2_CONFIG_SOCKPOOL_RECV_TIMEOUTMS", "1000", 1);
    setenv("COMDB2_CONFIG_SOCKET_TIMEOUT", "100", 1);
    setenv("COMDB2_CONFIG_CONNECT_TIMEOUT", "100", 1);

    sockpool_test(dbname, __LINE__);

    set_fail_timeout_sockpool_send(1);
    sockpool_test(dbname, __LINE__);

    assert(1 == get_num_sockpool_send_timeouts());
    assert(recv_timeouts == get_num_sockpool_recv_timeouts());

    set_fail_timeout_sockpool_recv(1);
    sockpool_test(dbname, __LINE__);
    assert(1 == get_num_sockpool_send_timeouts());
    assert((recv_timeouts + 1) == get_num_sockpool_recv_timeouts());

    set_fail_timeout_sockpool_send(2);
    set_fail_timeout_sockpool_recv(2);
    sockpool_test(dbname, __LINE__);
    assert(3 == get_num_sockpool_send_timeouts());
    int expected_recv_timeouts = getenv("CLUSTER") ? recv_timeouts + 2 : recv_timeouts + 1;
    // int expected_recv_timeouts = recv_timeouts + 3;
    fprintf(stderr, "Expected recv timeouts=%d actual=%d\n", expected_recv_timeouts,
            get_num_sockpool_recv_timeouts());
    assert((expected_recv_timeouts)== get_num_sockpool_recv_timeouts());

    printf("%s - pass\n", basename(argv[0]));
}
