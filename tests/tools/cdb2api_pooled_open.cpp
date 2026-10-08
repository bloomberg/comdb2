/*
 * Verify that a handle which takes its connection from sockpool in cdb2_open
 * (before dbinfo) doesn't treat it as hosts[0]. The host isn't known yet, so
 * the API must report the pooled connection's own host rather than hosts[0].
 *
 * Don't call cdb2_set_comdb2db_config(): an explicit config disables taking a
 * pooled connection in cdb2_open. The config is found through COMDB2_ROOT.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <netdb.h>
#include <arpa/inet.h>

#include <string>

#include <cdb2api.h>
#include <cdb2api_test.h>

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

/* The API reports a pooled connection by reverse lookup of its peer address,
 * which gives a name (possibly domain-qualified) or, without reverse DNS, an
 * address. Accept either for host. */
static int same_host(const std::string &reported, const std::string &host)
{
    if (reported.empty())
        return 0;
    if (reported.substr(0, reported.find('.')) == host)
        return 1;
    struct addrinfo hints, *res, *ai;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    if (getaddrinfo(host.c_str(), NULL, &hints, &res) != 0)
        return 0;
    int found = 0;
    for (ai = res; ai && !found; ai = ai->ai_next) {
        char addr[INET_ADDRSTRLEN];
        struct sockaddr_in *sin = (struct sockaddr_in *)ai->ai_addr;
        if (inet_ntop(AF_INET, &sin->sin_addr, addr, sizeof(addr)) && reported == addr)
            found = 1;
    }
    freeaddrinfo(res);
    return found;
}

static void *record_host(cdb2_hndl_tp *hndl, void *user_arg, int argc, void **argv)
{
    std::string *host = (std::string *)user_arg;
    *host = argv[0] ? (char *)argv[0] : "(null)";
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
    /* Record the host of connections taken from sockpool */
    setenv("COMDB2_CONFIG_GET_HOSTNAME_FROM_SOCKPOOL_FD", "on", 1);

    /* Put some connections in sockpool */
    for (int i = 0; i < 3; i++) {
        cdb2_hndl_tp *hndl = NULL;
        int rc = cdb2_open(&hndl, dbname, "default", 0);
        if (rc == CDB2_OK)
            rc = run(hndl, "select 1");
        if (rc != CDB2_OK) {
            fprintf(stderr, "open/select rc %d %s\n", rc, cdb2_errstr(hndl));
            return 1;
        }
        cdb2_close(hndl);
    }

    int pooled = get_num_skip_dbinfo();
    cdb2_hndl_tp *hndl = NULL;
    std::string reported, host;
    int rc = cdb2_open(&hndl, dbname, "default", 0);
    if (rc == CDB2_OK) {
        cdb2_register_event(hndl, CDB2_BEFORE_SEND_QUERY, (cdb2_event_ctrl)0, record_host, &reported, 1,
                            CDB2_HOSTNAME);
        rc = run(hndl, "select comdb2_host()", &host);
    }
    pooled = get_num_skip_dbinfo() - pooled;
    printf("rc %d pooled %d host %s reported %s err %s\n", rc, pooled, host.c_str(), reported.c_str(),
           rc ? cdb2_errstr(hndl) : "");
    cdb2_close(hndl);

    if (rc != CDB2_OK)
        return 1;
    if (pooled != 1) {
        fprintf(stderr, "cdb2_open didn't take a connection from sockpool\n");
        return 1;
    }
    if (!same_host(reported, host)) {
        fprintf(stderr, "API reported host '%s' for a pooled connection to %s\n", reported.c_str(), host.c_str());
        return 1;
    }
    printf("Success\n");
    return 0;
}
