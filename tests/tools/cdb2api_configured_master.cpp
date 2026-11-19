/*
 * CDB2_MASTER on a database whose hosts come with cluster type "configured".
 *
 * Usage: cdb2api_configured_master <dbname> [host ...]
 *
 * Without hosts, the config named by CDB2_CONFIG lists them. With hosts, they
 * are supplied on CDB2_BEFORE_DISCOVERY the way a proxy such as prox2 does:
 * the hook sets the cluster type to "configured" and fills the handle's host
 * list. CDB2_MASTER handles skip get_connection_int(), where the "configured"
 * shortcut is, and go through cdb2_get_dbhosts(), which used to discard those
 * hosts and ask comdb2db for a "configured" cluster.
 *
 * Either way a plain handle must run on one of the cluster's hosts, and a
 * CDB2_MASTER handle on a host that comdb2_cluster, read on that same handle,
 * marks as the master.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <string>
#include <vector>

#include <cdb2api.h>
#include <cdb2api_hndl.h>

static std::vector<std::string> proxy_hosts;

static void *supply_hosts(cdb2_hndl_tp *hndl, void *user_arg, int argc, void **argv)
{
    snprintf(hndl->type, sizeof(hndl->type), "configured");
    hndl->num_hosts = 0;
    for (size_t i = 0; i < proxy_hosts.size() && i < MAX_NODES; i++)
        snprintf(hndl->hosts[hndl->num_hosts++], sizeof(hndl->hosts[0]), "%s", proxy_hosts[i].c_str());
    return NULL;
}

static int query(cdb2_hndl_tp *hndl, const char *sql, std::vector<std::string> &rows)
{
    rows.clear();
    int rc = cdb2_run_statement(hndl, sql);
    if (rc != CDB2_OK)
        return rc;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        rows.push_back((const char *)cdb2_column_value(hndl, 0));
    return rc == CDB2_OK_DONE ? CDB2_OK : rc;
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <dbname> [host ...]\n", argv[0]);
        return 1;
    }
    const char *dbname = argv[1];
    for (int i = 2; i < argc; i++)
        proxy_hosts.push_back(argv[i]);
    const char *mode = proxy_hosts.empty() ? "config" : "proxy";

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);
    if (!proxy_hosts.empty()) {
        /* Naming a config sends every open past get_connection_int() unless the tier comes from the per-database
         * variable. Set it, so that only the CDB2_MASTER open skips it, as for a client using its installed config. */
        std::string var = std::string("COMDB2_CONFIG_DEFAULT_TYPE_") + dbname;
        setenv(var.c_str(), "configured", 1);
        cdb2_register_event(NULL, CDB2_BEFORE_DISCOVERY, (cdb2_event_ctrl)0, supply_hosts, NULL, 1, CDB2_DBTYPE);
    }

    std::vector<std::string> host, cluster, is_master;
    cdb2_hndl_tp *hndl = NULL;
    int rc = cdb2_open(&hndl, dbname, "default", 0);
    if (rc == CDB2_OK)
        rc = query(hndl, "select comdb2_host()", host);
    if (rc == CDB2_OK)
        rc = query(hndl, "select host from comdb2_cluster", cluster);
    if (rc != CDB2_OK) {
        fprintf(stderr, "%s: open rc %d %s\n", mode, rc, cdb2_errstr(hndl));
        return 1;
    }
    cdb2_close(hndl);
    if (host.size() != 1 || std::find(cluster.begin(), cluster.end(), host[0]) == cluster.end()) {
        fprintf(stderr, "%s: plain handle ran on '%s', which isn't in the cluster\n", mode,
                host.empty() ? "" : host[0].c_str());
        return 1;
    }
    printf("%s: plain handle ran on %s\n", mode, host[0].c_str());

    /* Without the hosts, cdb2_get_dbhosts() first asks sockpool for a connection to the "configured" tier. One pooled
     * by an earlier open, in this process or another, would answer with the master and hide a lost host list. */
    if (!proxy_hosts.empty())
        cdb2_disable_sockpool();

    hndl = NULL;
    rc = cdb2_open(&hndl, dbname, "default", CDB2_MASTER);
    if (rc == CDB2_OK)
        rc = query(hndl, "select comdb2_host()", host);
    /* One statement, so the master can't change between finding it and checking where this ran. */
    if (rc == CDB2_OK)
        rc = query(hndl, "select host from comdb2_cluster where host = comdb2_host() and is_master = 'Y'", is_master);
    if (rc != CDB2_OK) {
        fprintf(stderr, "%s: CDB2_MASTER open rc %d %s\n", mode, rc, cdb2_errstr(hndl));
        return 1;
    }
    cdb2_close(hndl);
    if (host.size() != 1 || is_master.size() != 1 || is_master[0] != host[0]) {
        fprintf(stderr, "%s: CDB2_MASTER handle ran on '%s', which comdb2_cluster doesn't mark as the master\n", mode,
                host.empty() ? "" : host[0].c_str());
        return 1;
    }
    printf("%s: CDB2_MASTER handle ran on the master, %s\n", mode, host[0].c_str());
    return 0;
}
