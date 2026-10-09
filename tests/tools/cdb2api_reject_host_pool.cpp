/*
 * Verify that a retry after a reject gets its connection to the chosen host
 * from sockpool.
 *
 * After a reject, the api asks sockpool for a connection to a specific host.
 * Direct-cpu handles donate their connections under that host's typestr,
 * comdb2/<db>/<host>/newsql/dc, so pool a connection to every host that way,
 * force a reject, and check that the retry made no tcp connections of its own.
 */

#undef NDEBUG
#include <assert.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cdb2api.h>
#include <cdb2api_test.h>

#define MAX_HOSTS 32
#define NUM_POOLED 3

static int run(cdb2_hndl_tp *hndl, const char *sql)
{
    int rc = cdb2_run_statement(hndl, sql);
    if (rc != CDB2_OK)
        return rc;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        ;
    return rc == CDB2_OK_DONE ? CDB2_OK : rc;
}

static cdb2_hndl_tp *open_and_run(const char *db, const char *type, int flags)
{
    cdb2_hndl_tp *hndl = NULL;
    int rc = cdb2_open(&hndl, db, type, flags);
    if (rc == CDB2_OK)
        rc = run(hndl, "select 1");
    if (rc != CDB2_OK) {
        fprintf(stderr, "%s@%s: rc %d %s\n", db, type, rc, cdb2_errstr(hndl));
        exit(1);
    }
    return hndl;
}

/* Hold several handles open at once so that each gets its own connection,
 * then close them all so sockpool ends up with that many */
static void pool_connections(const char *db, const char *type, int flags)
{
    cdb2_hndl_tp *hndls[NUM_POOLED];
    for (int i = 0; i < NUM_POOLED; i++)
        hndls[i] = open_and_run(db, type, flags);
    for (int i = 0; i < NUM_POOLED; i++)
        cdb2_close(hndls[i]);
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <dbname>\n", argv[0]);
        return 1;
    }
    const char *db = argv[1];
    signal(SIGPIPE, SIG_IGN);

    /* bypass local cache code for this test needs to talk to sockpool */
    setenv("COMDB2_CONFIG_MAX_LOCAL_CONNECTION_CACHE_ENTRIES", "0", 1);
    test_process_env_vars();

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    char *hosts[MAX_HOSTS];
    int num_hosts = 0;
    cdb2_hndl_tp *hndl = open_and_run(db, "default", 0);
    cdb2_cluster_info(hndl, hosts, NULL, MAX_HOSTS, &num_hosts);
    cdb2_close(hndl);
    assert(num_hosts > 0 && num_hosts <= MAX_HOSTS);

    /* The first connection and any dbinfo query come from the tier pool */
    pool_connections(db, "default", 0);
    /* The retry can go to any host, so pool connections to all of them */
    for (int i = 0; i < num_hosts; i++) {
        printf("pooling connections to %s\n", hosts[i]);
        pool_connections(db, hosts[i], CDB2_DIRECT_CPU);
    }

    /* Whether the first connection comes from sockpool depends on the tier, so
     * compare against the same query without a reject */
    int tcp_connects = get_num_tcp_connects();
    int sockpool_fds = get_num_sockpool_fd();
    hndl = open_and_run(db, "default", 0);
    cdb2_close(hndl);
    int base_tcp_connects = get_num_tcp_connects() - tcp_connects;
    int base_sockpool_fds = get_num_sockpool_fd() - sockpool_fds;
    printf("without reject: %d tcp connects, %d fds from sockpool\n", base_tcp_connects, base_sockpool_fds);

    tcp_connects = get_num_tcp_connects();
    sockpool_fds = get_num_sockpool_fd();
    set_fail_reject(1);
    hndl = open_and_run(db, "default", 0);
    cdb2_close(hndl);
    int reject_tcp_connects = get_num_tcp_connects() - tcp_connects;
    int reject_sockpool_fds = get_num_sockpool_fd() - sockpool_fds;
    printf("with reject: %d tcp connects, %d fds from sockpool\n", reject_tcp_connects, reject_sockpool_fds);

    /* the retry's connection came from sockpool, not a tcp connect */
    assert(reject_sockpool_fds >= base_sockpool_fds + 1);
    assert(reject_tcp_connects <= base_tcp_connects);

    for (int i = 0; i < num_hosts; i++)
        free(hosts[i]);
    printf("pass\n");
    return 0;
}
