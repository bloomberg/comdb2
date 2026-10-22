#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <cdb2api.h>

/*
 * A connection that is handed from one handle to the next (local cache or
 * sockpool) must not keep the previous client's in-transaction results mode.
 */

static const char *dbname;
static char *host = "default";
static int direct;

static void die(cdb2_hndl_tp *hndl, const char *what, int rc)
{
    fprintf(stderr, "%s: rc %d: %s\n", what, rc, hndl ? cdb2_errstr(hndl) : "");
    exit(1);
}

static void run(cdb2_hndl_tp *hndl, const char *sql)
{
    int rc = cdb2_run_statement(hndl, sql);
    if (rc != CDB2_OK)
        die(hndl, sql, rc);
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        ;
    if (rc != CDB2_OK_DONE)
        die(hndl, sql, rc);
}

static cdb2_hndl_tp *open_hndl(int flags)
{
    cdb2_hndl_tp *hndl = NULL;
    int rc = cdb2_open(&hndl, dbname, host, direct | flags);
    if (rc != CDB2_OK)
        die(hndl, "cdb2_open", rc);
    return hndl;
}

/* Returns the server's id for the connection this handle is on */
static long long connection_id(cdb2_hndl_tp *hndl, long long *num_resets)
{
    char sql[256];
    long long id = -1;
    int rc;

    snprintf(sql, sizeof(sql),
             "SELECT connection_id, num_resets FROM comdb2_connections "
             "WHERE pid = %d AND sql LIKE 'SELECT connection_id, num_resets FROM comdb2_connections%%'",
             (int)getpid());
    rc = cdb2_run_statement(hndl, sql);
    if (rc != CDB2_OK)
        die(hndl, sql, rc);
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        if (id != -1) {
            fprintf(stderr, "more than one connection matches\n");
            exit(1);
        }
        id = *(long long *)cdb2_column_value(hndl, 0);
        *num_resets = *(long long *)cdb2_column_value(hndl, 1);
    }
    if (rc != CDB2_OK_DONE)
        die(hndl, sql, rc);
    if (id == -1) {
        fprintf(stderr, "can't find my connection\n");
        exit(1);
    }
    return id;
}

/* Fails unless hndl got the connection that prev_id was on */
static void check_reused(cdb2_hndl_tp *hndl, long long prev_id, long long prev_resets)
{
    long long resets;
    long long id = connection_id(hndl, &resets);
    if (id != prev_id || resets <= prev_resets) {
        fprintf(stderr, "connection not reused: id %lld resets %lld, previous id %lld resets %lld\n", id, resets,
                prev_id, prev_resets);
        exit(1);
    }
}

/* A client that skips in-transaction results, then one that reads them */
static void skip_then_read(void)
{
    long long id, resets;
    int rc;

    cdb2_hndl_tp *skip = open_hndl(0);
    run(skip, "BEGIN");
    run(skip, "SELECT 1");
    run(skip, "ROLLBACK");
    id = connection_id(skip, &resets);
    cdb2_close(skip);

    cdb2_hndl_tp *read = open_hndl(CDB2_READ_INTRANS_RESULTS);
    check_reused(read, id, resets);
    run(read, "BEGIN");
    rc = cdb2_run_statement(read, "selec 1");
    if (rc != CDB2ERR_PREPARE_ERROR)
        die(read, "expected a prepare error from 'selec 1' right away", rc);
    cdb2_run_statement(read, "ROLLBACK");
    cdb2_close(read);
}

/* A client that sets INTRANSRESULTS ON, then one that skips results */
static void set_on_then_skip(void)
{
    long long id, resets;
    int rc;

    cdb2_hndl_tp *on = open_hndl(0);
    run(on, "SET INTRANSRESULTS ON");
    run(on, "BEGIN");
    run(on, "SELECT 1");
    run(on, "ROLLBACK");
    id = connection_id(on, &resets);
    cdb2_close(on);

    cdb2_hndl_tp *skip = open_hndl(0);
    check_reused(skip, id, resets);
    run(skip, "BEGIN");
    rc = cdb2_run_statement(skip, "selec 1");
    if (rc != CDB2_OK)
        die(skip, "expected 'selec 1' to be skipped", rc);
    rc = cdb2_run_statement(skip, "ROLLBACK");
    if (rc != CDB2ERR_PREPARE_ERROR)
        die(skip, "expected the prepare error on ROLLBACK", rc);
    cdb2_close(skip);
}

int main(int argc, char **argv)
{
    cdb2_hndl_tp *hndl = NULL;
    char *conf = getenv("CDB2_CONFIG");
    int rc;

    if (argc < 2)
        return 1;
    dbname = argv[1];

    /* Hand connections between handles in this process. cdb2api reads this
       on its first call. */
    setenv("COMDB2_CONFIG_MAX_LOCAL_CONNECTION_CACHE_ENTRIES", "10", 1);

    if (conf != NULL)
        cdb2_set_comdb2db_config(conf);

    /* Both handles of a pair must land on the same node */
    if (getenv("CLUSTER")) {
        rc = cdb2_open(&hndl, dbname, "default", 0);
        if (rc != CDB2_OK)
            die(hndl, "cdb2_open", rc);
        rc = cdb2_run_statement(hndl, "SELECT comdb2_host()");
        if (rc != CDB2_OK || cdb2_next_record(hndl) != CDB2_OK)
            die(hndl, "SELECT comdb2_host()", rc);
        host = strdup((char *)cdb2_column_value(hndl, 0));
        direct = CDB2_DIRECT_CPU;
        cdb2_close(hndl);
    }

    skip_then_read();
    set_on_then_skip();

    return 0;
}
