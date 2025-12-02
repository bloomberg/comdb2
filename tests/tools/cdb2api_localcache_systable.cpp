#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <netdb.h>

#include <cdb2api.h>
#include <cdb2api_test.h>

/*
 * This test verifies that in the comdb2_connections systable,
 * a locally cached connection shows as state=reset and in_localcache=1, and
 * a sockpool cached connection shows as state=reset and in_localcache=0
*/
void open_select_close_cycle(char *dbname, char *host)
{
    cdb2_hndl_tp *hndl = NULL;
    int rc = cdb2_open(&hndl, dbname, host, CDB2_DIRECT_CPU);
    assert(rc == CDB2_OK);

    rc = cdb2_run_statement(hndl, "SELECT 1");
    assert(rc == CDB2_OK);
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    assert(rc == CDB2_OK_DONE);
    rc = cdb2_close(hndl);
    assert(rc == CDB2_OK);
}

int main(int argc, char **argv)
{
    int rc;
    long cnt;
    char *host = NULL;

    int mypid;
    char myhost[256];
    char query[1024];
    const char *host_to_check = getenv("CLUSTER") ? myhost : "localhost";

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);
        
    char *dbname = argv[1];


    /* who I am ??? */
    mypid = (int)getpid();
    gethostname(myhost, sizeof(myhost));

    signal(SIGPIPE, SIG_IGN);

    /* Use this connection to query the systable */
    cdb2_hndl_tp *hndl = NULL;
    rc = cdb2_open(&hndl, dbname, "default", 0);
    assert(rc == CDB2_OK);

    /* Get the host */
    rc = cdb2_run_statement(hndl, "SELECT comdb2_host()");
    assert(rc == CDB2_OK);
    host = strdup((char *)cdb2_column_value(hndl, 0));

    printf("direct-cpu host %s\n", host);

    snprintf(query, sizeof(query),
            "SELECT COUNT(*) FROM comdb2_connections WHERE state='reset' AND host LIKE '%s%%' AND pid=%d",
             host_to_check, mypid);
    puts(query);

    /* Still need to check for this? Because PID can be reused? */
    for (cnt = 999; cnt != 0; puts("Waiting for cached connections to go away..."), sleep(1)) {
        rc = cdb2_run_statement(hndl, query);
        assert(rc == CDB2_OK);
        cnt = *(long *)cdb2_column_value(hndl, 0);
    }

    /* Enabling local-caching. This connection would show as 'RESET' and in_local_cache=1 */
    puts("verifying locally cached connections...");
    setenv("COMDB2_CONFIG_MAX_LOCAL_CONNECTION_CACHE_ENTRIES", " 10", 1);
    test_process_env_vars();
    open_select_close_cycle(dbname, host);

    snprintf(query, sizeof(query),
            "SELECT COUNT(*) FROM comdb2_connections WHERE state='reset' AND host LIKE '%s%%' AND pid=%d AND in_local_cache=1",
             host_to_check, mypid);
    puts(query);

    rc = cdb2_run_statement(hndl, query);
    cnt = *(long *)cdb2_column_value(hndl, 0);
    if (cnt != 1) {
        fprintf(stderr, "expected one -locally- cached connection in the reset state, got %ld\n", cnt);
        return -1;
    }
    puts("ok");

    snprintf(query, sizeof(query),
            "SELECT COUNT(*) FROM comdb2_connections WHERE state='reset' AND host LIKE '%s%%' AND pid=%d AND in_local_cache=0",
             host_to_check, mypid);
    puts(query);
    /* Disabling local-caching. This connection would show as 'RESET' and in_local_cache=0 */
    puts("verifying sockpool cached connections...");
    setenv("COMDB2_CONFIG_MAX_LOCAL_CONNECTION_CACHE_ENTRIES", " 0", 1);
    test_process_env_vars();
    open_select_close_cycle(dbname, host);

    /* give sockpool a bit time to send its reset */
    sleep(2);

    rc = cdb2_run_statement(hndl, query);
    cnt = *(long *)cdb2_column_value(hndl, 0);
    if (cnt != 1) {
        fprintf(stderr, "expected one -sockpool- cached connection in the reset state, got %ld\n", cnt);
        return -1;
    }
    puts("ok");

    /* clean up */
    free(host);
    rc = cdb2_close(hndl);
    assert(rc == CDB2_OK);
    return 0;
}
