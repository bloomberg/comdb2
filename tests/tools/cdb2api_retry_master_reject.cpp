/*
 * Fill sockpool with connections to the master, then run a query on a regular
 * handle. The master rejects it with dbinfo; the retry must go to a replicant
 * rather than to another pooled connection to the master.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

#include <string>
#include <vector>

#include <cdb2api.h>
#include <cdb2api_test.h>

#define NUM_POOLED 30
/* Pooled connection to the master, then a replicant */
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

static void *count_send(cdb2_hndl_tp *hndl, void *user_arg, int argc, void **argv)
{
    ++*(int *)user_arg;
    return NULL;
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

    /* CDB2_RANDOMROOM keeps the policy, and so the sockpool typestr, the same as a default handle's */
    std::string master;
    std::vector<cdb2_hndl_tp *> on_master;
    for (int i = 0; i < NUM_POOLED; i++) {
        cdb2_hndl_tp *hndl = NULL;
        std::string host;
        int rc = cdb2_open(&hndl, dbname, "default", CDB2_MASTER | CDB2_RANDOMROOM);
        if (rc == CDB2_OK)
            rc = run(hndl, "select comdb2_host()", &host);
        if (rc != CDB2_OK) {
            fprintf(stderr, "master open/select rc %d %s\n", rc, cdb2_errstr(hndl));
            return 1;
        }
        if (master.empty())
            master = host;
        if (host != master) {
            fprintf(stderr, "CDB2_MASTER handle ran on %s, expected %s\n", host.c_str(), master.c_str());
            return 1;
        }
        on_master.push_back(hndl);
    }
    for (auto hndl : on_master)
        cdb2_close(hndl);
    printf("pooled %d connections to master %s\n", NUM_POOLED, master.c_str());

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

    if (rc != CDB2_OK || host == master) {
        fprintf(stderr, "query failed or ran on master %s\n", master.c_str());
        return 1;
    }
    if (sends < 2) {
        fprintf(stderr, "query was sent %d time(s), the master never rejected it\n", sends);
        return 1;
    }
    if (sends > MAX_SENDS) {
        fprintf(stderr, "query was sent %d times, retries kept reusing pooled connections to master %s\n", sends,
                master.c_str());
        return 1;
    }
    printf("Success\n");
    return 0;
}
