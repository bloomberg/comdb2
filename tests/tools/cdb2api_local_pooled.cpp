/*
 * Verify that a "local" handle keeps localhost as its only host when it
 * discovers hosts while a pooled local connection is available.
 *
 * Discovery asks a pooled connection for dbinfo before it gets to the "local"
 * check. If that succeeds, the handle takes every node in the cluster from the
 * reply instead of just localhost, and can then run queries on other nodes.
 *
 * Two ways to get there:
 *   - retry: cdb2_open takes a pooled connection, the query fails and the
 *     retry discovers hosts while another local connection is pooled.
 *   - get_dbinfo: cdb2_open skips the pool and discovers hosts right away.
 *
 * Don't call cdb2_set_comdb2db_config(): an explicit config skips both the
 * pooled open and the dbinfo through the pool.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

#include <string>
#include <vector>

#include <cdb2api.h>
#include <cdb2api_test.h>

#define MAX_HOSTS 32

static const char *dbname;

static int run(cdb2_hndl_tp *hndl, const char *sql, std::string *out = NULL)
{
    int rc = cdb2_run_statement(hndl, sql);
    if (rc != CDB2_OK)
        return rc;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        if (out)
            *out = (char *)cdb2_column_value(hndl, 0);
    }
    return rc == CDB2_OK_DONE ? CDB2_OK : rc;
}

/* Leave n local connections in the pool */
static int fill_pool(int n)
{
    std::vector<cdb2_hndl_tp *> hndls;
    int rc = 0;
    for (int i = 0; i < n && rc == 0; i++) {
        cdb2_hndl_tp *hndl = NULL;
        rc = cdb2_open(&hndl, dbname, "local", 0);
        if (rc == CDB2_OK)
            rc = run(hndl, "select 1");
        if (rc != CDB2_OK)
            fprintf(stderr, "open/select rc %d %s\n", rc, cdb2_errstr(hndl));
        hndls.push_back(hndl);
    }
    for (auto hndl : hndls)
        cdb2_close(hndl);
    if (rc)
        return rc;
    int pooled = get_num_cached_connections_for(dbname);
    if (pooled < n) {
        fprintf(stderr, "expected %d pooled connections, got %d\n", n, pooled);
        return 1;
    }
    return 0;
}

/* The handle must know of localhost only, and the query must have run there */
static int check(const char *test, cdb2_hndl_tp *hndl, int rc, const std::string &host, const std::string &localhost)
{
    char *hosts[MAX_HOSTS] = {0};
    int count = 0;
    cdb2_cluster_info(hndl, hosts, NULL, MAX_HOSTS, &count);
    printf("%s: rc %d host %s hosts", test, rc, host.c_str());
    for (int i = 0; i < count && i < MAX_HOSTS; i++)
        printf(" %s", hosts[i]);
    printf("\n");

    int failed = 0;
    if (rc != CDB2_OK) {
        fprintf(stderr, "%s: query failed rc %d %s\n", test, rc, cdb2_errstr(hndl));
        failed = 1;
    } else if (host != localhost) {
        fprintf(stderr, "%s: query ran on %s, not %s\n", test, host.c_str(), localhost.c_str());
        failed = 1;
    }
    if (count != 1 || strcmp(hosts[0], "localhost") != 0) {
        fprintf(stderr, "%s: local handle has %d hosts, expected only localhost\n", test, count);
        failed = 1;
    }
    for (int i = 0; i < count && i < MAX_HOSTS; i++)
        free(hosts[i]);
    return failed;
}

static int test_retry(const std::string &localhost)
{
    if (fill_pool(2))
        return 1;
    int skipped = get_num_skip_dbinfo();
    cdb2_hndl_tp *hndl = NULL;
    std::string host;
    int rc = cdb2_open(&hndl, dbname, "local", 0);
    if (rc == CDB2_OK) {
        if (get_num_skip_dbinfo() == skipped) {
            fprintf(stderr, "retry: cdb2_open didn't take a pooled connection\n");
            cdb2_close(hndl);
            return 1;
        }
        /* Drop the pooled connection so the retry discovers hosts */
        set_fail_send(1);
        rc = run(hndl, "select comdb2_host()", &host);
        set_fail_send(0);
    }
    int failed = check("retry", hndl, rc, host, localhost);
    cdb2_close(hndl);
    return failed;
}

static int test_get_dbinfo(const std::string &localhost)
{
    if (fill_pool(1))
        return 1;
    set_dbinfo_state(1);
    cdb2_hndl_tp *hndl = NULL;
    std::string host;
    int rc = cdb2_open(&hndl, dbname, "local", 0);
    if (rc == CDB2_OK)
        rc = run(hndl, "select comdb2_host()", &host);
    set_dbinfo_state(0);
    int failed = check("get_dbinfo", hndl, rc, host, localhost);
    cdb2_close(hndl);
    return failed;
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <dbname> <config>\n", argv[0]);
        return 1;
    }
    dbname = argv[1];
    signal(SIGPIPE, SIG_IGN);
    /* Pool in-process so the test doesn't depend on cdb2sockpool */
    setenv("COMDB2_CONFIG_MAX_LOCAL_CONNECTION_CACHE_ENTRIES", "10", 1);
    unsetenv("COMDB2_CONFIG_DEFAULT_TYPE");
    set_cdb2api_test_comdb2db_cfg(argv[2]);

    /* The connection is to localhost whether or not it came from the pool */
    cdb2_hndl_tp *hndl = NULL;
    std::string localhost;
    int rc = cdb2_open(&hndl, dbname, "local", 0);
    if (rc == CDB2_OK)
        rc = run(hndl, "select comdb2_host()", &localhost);
    if (rc != CDB2_OK) {
        fprintf(stderr, "local open/select rc %d %s\n", rc, cdb2_errstr(hndl));
        cdb2_close(hndl);
        return 1;
    }
    printf("local host %s\n", localhost.c_str());
    cdb2_close(hndl);

    int failed = test_retry(localhost);
    local_connection_cache_clear(1);
    failed |= test_get_dbinfo(localhost);

    if (failed)
        return 1;
    printf("Success\n");
    return 0;
}
