#include <stdio.h>
#include <stdlib.h>
#include <strings.h>

#include <cdb2api.h>

int main(int argc, char **argv)
{
    cdb2_hndl_tp *hndl = NULL;
    const char *conf = getenv("CDB2_CONFIG");
    const char *db, *tier;
    int rc;

    if (argc < 2)
        return 1;

    db = argv[1];

    if (argc > 2)
        tier = argv[2];
    else
        tier = "default";

    if (conf != NULL)
        cdb2_set_comdb2db_config(conf);

    rc = cdb2_open(&hndl, db, tier, CDB2_READ_INTRANS_RESULTS);
    if (rc != 0) {
        fprintf(stderr, "Error opening a handle: %d: %s.\n",
                rc, cdb2_errstr(hndl));
        return 1;
    }

    rc = cdb2_run_statement(hndl, "BEGIN");
    if (rc != 0) {
        fprintf(stderr, "Error running query: %d: %s.\n",
                rc, cdb2_errstr(hndl));
        return 1;
    }

    /* Should get error immediately if in READ_INTRANS_RESULTS mode. */
    rc = cdb2_run_statement(hndl, "qwerty");
    if (rc != CDB2ERR_PREPARE_ERROR)
        return 1;

    rc = cdb2_run_statement(hndl, "ROLLBACK");
    if (rc != 0) {
        fprintf(stderr, "Error running query: %d: %s.\n",
                rc, cdb2_errstr(hndl));
        return 1;
    }

    rc = cdb2_run_statement(hndl, "DROP TABLE IF EXISTS read_intrans_results_test");
    if (rc != 0) {
        fprintf(stderr, "Error running query: %d: %s.\n",
                rc, cdb2_errstr(hndl));
        return 1;
    }

    rc = cdb2_run_statement(hndl, "CREATE TABLE read_intrans_results_test (i INTEGER)");
    if (rc != 0) {
        fprintf(stderr, "Error running query: %d: %s.\n",
                rc, cdb2_errstr(hndl));
        return 1;
    }

    rc = cdb2_run_statement(hndl, "BEGIN");
    if (rc != 0) {
        fprintf(stderr, "Error running query: %d: %s.\n",
                rc, cdb2_errstr(hndl));
        return 1;
    }

    rc = cdb2_run_statement(hndl, "INSERT INTO read_intrans_results_test VALUES (1)");
    if (rc != 0) {
        fprintf(stderr, "Error running query: %d: %s.\n",
                rc, cdb2_errstr(hndl));
        return 1;
    }

    while (cdb2_next_record(hndl) != CDB2_OK_DONE) {
        if (strcasecmp(cdb2_column_name(hndl, 0), "rows inserted") == 0 &&
            *(unsigned long long *)cdb2_column_value(hndl, 0) == 1)
            return 0;
    }

    return 1;
}
