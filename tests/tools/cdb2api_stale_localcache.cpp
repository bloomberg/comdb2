/* Verify that stale connections in the local cache are detected and discarded.
 *
 * fail_local_cache_peek simulates recv() returning 0 (EOF from server), which
 * is the same code path as real staleness.  The test verifies that:
 *   - num_stale_cache_rejects increments on each stale detect
 *   - a new TCP connection is made when the cached one is discarded
 *   - normal cache hits resume once the injection is cleared
 */
#include <assert.h>
#include <signal.h>
#include <stdio.h>

#include <cdb2api.h>
#include <cdb2api_test.h>

static void run_query(const char *dbname, const char *type)
{
    cdb2_hndl_tp *hndl = NULL;
    int rc = cdb2_open(&hndl, dbname, type, 0);
    assert(rc == CDB2_OK);
    rc = cdb2_run_statement(hndl, "SELECT 1");
    assert(rc == CDB2_OK);
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        ;
    assert(rc == CDB2_OK_DONE);
    rc = cdb2_close(hndl);
    assert(rc == CDB2_OK);
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <dbname>\n", argv[0]);
        return 1;
    }
    const char *dbname = argv[1];
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    set_fail_sockpool(-1); /* keep connections out of the external sockpool */
    set_local_connections_limit(10);

    /* First query: cache miss, connection goes into local cache on close */
    run_query(dbname, "default");
    assert(get_num_cache_misses() == 1);
    assert(get_num_cache_hits() == 0);
    assert(get_num_stale_cache_rejects() == 0);

    int connects_before = get_num_tcp_connects();

    /* Simulate a stale cached socket (recv returns 0 = EOF) */
    set_fail_local_cache_peek(1);

    /* This query hits the cache, finds the socket stale, discards it, and
     * opens a fresh TCP connection to the database */
    run_query(dbname, "default");
    assert(get_num_stale_cache_rejects() == 1);
    assert(get_num_tcp_connects() > connects_before);

    /* Clear the injection; the fresh connection is now in the cache */
    set_fail_local_cache_peek(0);

    int hits_before = get_num_cache_hits();
    run_query(dbname, "default");
    assert(get_num_cache_hits() == hits_before + 1);
    assert(get_num_stale_cache_rejects() == 1); /* no new stale rejects */

    return 0;
}
