#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <libgen.h>
#include <unistd.h>
#include <errno.h>

#include <cdb2api.h>

void free_conns(cdb2_hndl_tp **dbs, int freq) {
    cdb2_hndl_tp *db;
    for (int i = 0; i < freq; i++) {
        db = dbs[i];
        if (!db)
            continue;
        cdb2_run_statement(db, "rollback"); // may or may not fail
        cdb2_close(db);
    }
    free(dbs);
}

// Just check that when using CDB2_DIRECT_CPU you get max retry error or connect error instead of appsock error
// In a cluster when attempting to connect to all nodes, these errors mean that the api has retried on different nodes 
int appsock_test(cdb2_hndl_tp **dbs, int freq, char *dbname, char *host) {
    cdb2_hndl_tp **db;
    int rc;
    int i;
    for (i = 0; i < freq; i++) {
        db = &dbs[i];
        rc = cdb2_open(db, dbname, host, CDB2_DIRECT_CPU);
        if (rc != CDB2_OK) {
            fprintf(stderr, "%s: Error opening %s %d %s\n", __func__, dbname, rc, cdb2_errstr(*db));
            return -1;
        }

        rc = cdb2_run_statement(*db, "begin");
        if (rc == CDB2ERR_CONNECT_ERROR && i == 5) // this is expected on the last try
            return 0;
        if (rc) {
            fprintf(stderr, "%s: Error running begin on %s %d %s\n", __func__, dbname, rc, cdb2_errstr(*db));
            return -1;
        }
        rc = cdb2_run_statement(*db, "select comdb2_host()");
        if (rc) {
            fprintf(stderr, "%s: Error running query on %s %d %s\n", __func__, dbname, rc, cdb2_errstr(*db));
            return -1;
        }
        rc = cdb2_next_record(*db);
        if (rc != CDB2_OK) {
            fprintf(stderr, "%s: Error reading query on %s %d %s\n", __func__, dbname, rc, cdb2_errstr(*db));
            return -1;
        }
    }

    fprintf(stderr, "%s: Didn't get connect error\n", __func__);
    return -1;
}

static int change_appsock_limit(cdb2_hndl_tp *hndl)
{
    char query[500];
    int value = 5;
    sprintf(query, "exec procedure sys.cmd.send('bdb setattr MAXAPPSOCKSLIMIT %d')", value);
    int rc = cdb2_run_statement(hndl, query);
    if (rc) {
        fprintf(stderr, "Error setting MAXAPPSOCKSLIMIT to %d %d %s\n", value, rc, cdb2_errstr(hndl));
        return -1;
    }

    // make sure tunable is on/off
    rc = cdb2_run_statement(hndl, "select value from comdb2_tunables where name = 'maxappsockslimit'");
    if (rc) {
        fprintf(stderr, "Error running query %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }
    rc = cdb2_next_record(hndl);
    if (rc != CDB2_OK) {
        fprintf(stderr, "%s: Expected record %d %s\n", __func__, rc, cdb2_errstr(hndl));
        return -1;
    }
    char *returned = (char *)cdb2_column_value(hndl, 0);
    char expected[5];
    sprintf(expected, "%d", value);
    if (strcmp(returned, expected)) {
        fprintf(stderr, "Expected tunable %s, got %s\n", expected, returned);
        return -1;
    }
    rc = cdb2_next_record(hndl);
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "Expected done %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }

    return 0;
}

static int tunable_driver(char *dbname, char **rhost)
{
    cdb2_hndl_tp *hndl;
    int rc = cdb2_open(&hndl, dbname, "default", 0);
    if (rc) {
        fprintf(stderr, "%s: Error opening %s %d %s\n", __func__, dbname, rc, cdb2_errstr(hndl));
        return -1;
    }
    rc = cdb2_run_statement(hndl, "select host from comdb2_cluster order by is_master limit 1");
    if (rc) {
        fprintf(stderr, "Error running %s %d %s\n", __func__, rc, cdb2_errstr(hndl));
        return -1;
    }
    cdb2_hndl_tp *hndl2;
    char *host;
    *rhost = NULL;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        if (*rhost) {
            fprintf(stderr, "Did not expect another record\n");
            return -1;
        }
        host = (char *)cdb2_column_value(hndl, 0);
        *rhost = strdup(host);
        rc = cdb2_open(&hndl2, dbname, host, CDB2_DIRECT_CPU);
        if (rc) {
            fprintf(stderr, "Error opening %s on host %s %d %s\n", dbname, host, rc, cdb2_errstr(hndl2));
            return -1;
        }
        rc = cdb2_run_statement(hndl2, "select comdb2_host()");
        if (rc) {
            fprintf(stderr, "can't run select comdb2_host on host %s %d %s\n", host, rc, cdb2_errstr(hndl2));
            return -1;
        }
        rc = cdb2_next_record(hndl2);
        if (rc != CDB2_OK) {
            fprintf(stderr, "can't read\n");
            return -1;
        }
        char *host2 = (char *)cdb2_column_value(hndl2, 0);
        if (strcmp(host, host2) != 0) {
            fprintf(stderr, "Should be connected to %s, actually connected to %s\n", host, host2);
            return -1;
        }
        rc = cdb2_next_record(hndl2);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "can't finish reading\n");
            return -1;
        }
        rc = change_appsock_limit(hndl2);
        if (rc)
            return -1;
        cdb2_close(hndl2);
    }
    if (!*rhost) {
        fprintf(stderr, "Could not find a host\n");
        return -1;
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "Error reading %s %d %s\n", __func__, rc, cdb2_errstr(hndl));
        return -1;
    }
    rc = cdb2_close(hndl);
    if (rc) {
        fprintf(stderr, "%s: Error closing %s %d %s\n", __func__, dbname, rc, cdb2_errstr(hndl));
        return -1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    cdb2_disable_sockpool();
    setenv("COMDB2_CONFIG_MAX_LOCAL_CONNECTION_CACHE_ENTRIES", "0", 1);

    char *dbname = argv[1];
    char *conf = getenv("CDB2_CONFIG");
    if (conf) cdb2_set_comdb2db_config(conf);
    char *host = NULL;
    if (tunable_driver(dbname, &host))
        abort();

    int freq = 6;
    cdb2_hndl_tp **dbs = (cdb2_hndl_tp **)calloc(freq, sizeof(cdb2_hndl_tp *));
    if (appsock_test(dbs, freq, dbname, host) != 0) {
        free_conns(dbs, freq);
        abort();
    }
    free_conns(dbs, freq);

    printf("%s - pass\n", basename(argv[0]));
    return 0;
}
