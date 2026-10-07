/*
 * Verify that a retry after a dropped query does not keep reusing pooled
 * connections to the node that dropped it.
 *
 * Fill sockpool with connections to one replicant, mark that replicant
 * rtcpu'd so the leader makes it incoherent (an incoherent node silently drops
 * queries), then run a query. The retries must reach another node instead of
 * failing with "Maximum number of retries done".
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>

#include <string>
#include <vector>

#include <cdb2api.h>
#include <cdb2api_test.h>

#define NUM_POOLED 30
#define MAX_HANDLES 500
/* Pooled connection to the incoherent node, maybe one direct connection to
 * it, then another node */
#define MAX_SENDS 3

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

static int run_on(const char *host, int flags, const char *sql, std::string *out = NULL)
{
    cdb2_hndl_tp *hndl = NULL;
    int rc = cdb2_open(&hndl, dbname, host, flags);
    if (rc == CDB2_OK)
        rc = run(hndl, sql, out);
    if (rc != CDB2_OK)
        fprintf(stderr, "%s on %s: rc %d %s\n", sql, host, rc, cdb2_errstr(hndl));
    cdb2_close(hndl);
    return rc;
}

static void *count_send(cdb2_hndl_tp *hndl, void *user_arg, int argc, void **argv)
{
    ++*(int *)user_arg;
    return NULL;
}

static int routecpu(const char *master, const char *node)
{
    char sql[256];
    snprintf(sql, sizeof(sql), "exec procedure sys.cmd.send('debug tcmtest routecpu %s')", node);
    return run_on(master, CDB2_ADMIN, sql);
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <dbname>\n", argv[0]);
        return 1;
    }
    dbname = argv[1];
    signal(SIGPIPE, SIG_IGN);
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    std::string master;
    if (run_on("default", 0, "select host from comdb2_cluster where is_master='Y'", &master) || master.empty()) {
        fprintf(stderr, "couldn't find master\n");
        return 1;
    }

    /* Collect connections to one replicant; hold the others open so only
     * connections to that replicant end up in the pool */
    std::string node;
    std::vector<cdb2_hndl_tp *> on_node, others;
    for (int i = 0; i < MAX_HANDLES && on_node.size() < NUM_POOLED; i++) {
        cdb2_hndl_tp *hndl = NULL;
        std::string host;
        int rc = cdb2_open(&hndl, dbname, "default", 0);
        if (rc == CDB2_OK)
            rc = run(hndl, "select comdb2_host()", &host);
        if (rc != CDB2_OK) {
            fprintf(stderr, "open/select rc %d %s\n", rc, cdb2_errstr(hndl));
            return 1;
        }
        if (node.empty() && host != master)
            node = host;
        if (host == node)
            on_node.push_back(hndl);
        else
            others.push_back(hndl);
    }
    if (on_node.size() < NUM_POOLED) {
        fprintf(stderr, "only got %zu connections to %s\n", on_node.size(), node.c_str());
        return 1;
    }
    printf("master %s, pooling %zu connections to %s\n", master.c_str(), on_node.size(), node.c_str());

    /* No writes yet, so the node stays coherent until the next commit */
    if (routecpu(master.c_str(), node.c_str()))
        return 1;

    for (auto hndl : on_node)
        cdb2_close(hndl);

    /* The commit makes the leader mark the rtcpu'd node incoherent */
    if (run_on(master.c_str(), CDB2_DIRECT_CPU, "insert into t1 values(1)"))
        return 1;
    std::string state;
    char sql[256];
    snprintf(sql, sizeof(sql), "select coherent_state from comdb2_cluster where host='%s'", node.c_str());
    run_on(master.c_str(), CDB2_DIRECT_CPU, sql, &state);
    printf("%s is %s\n", node.c_str(), state.c_str());
    /* Let the node's coherency lease expire */
    sleep(2);

    /* Pooled connections expire after 10 seconds, before the retries would
     * run out, so count the attempts rather than wait for the query to fail */
    cdb2_hndl_tp *hndl = NULL;
    std::string host;
    int sends = 0;
    int rc = cdb2_open(&hndl, dbname, "default", 0);
    if (rc == CDB2_OK) {
        cdb2_set_debug_trace(hndl);
        cdb2_register_event(hndl, CDB2_BEFORE_SEND_QUERY, (cdb2_event_ctrl)0, count_send, &sends, 0);
        rc = run(hndl, "select comdb2_host()", &host);
    }
    printf("query rc %d host %s sends %d err %s\n", rc, host.c_str(), sends, rc ? cdb2_errstr(hndl) : "");
    cdb2_close(hndl);

    routecpu(master.c_str(), "");
    for (auto h : others)
        cdb2_close(h);

    if (state != "INCOHERENT") {
        fprintf(stderr, "expected %s to be INCOHERENT\n", node.c_str());
        return 1;
    }
    if (rc != CDB2_OK || host == node) {
        fprintf(stderr, "query failed or ran on incoherent node %s\n", node.c_str());
        return 1;
    }
    if (sends > MAX_SENDS) {
        fprintf(stderr, "query was sent %d times, retries kept reusing pooled connections to %s\n", sends,
                node.c_str());
        return 1;
    }
    printf("Success\n");
    return 0;
}
