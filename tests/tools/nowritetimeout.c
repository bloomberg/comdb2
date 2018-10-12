#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <cdb2api.h>

int main(int argc, char **argv)
{
    cdb2_hndl_tp *hndl = NULL;
    char *conf = getenv("CDB2_CONFIG");
    char *tier = "local";
    void *blob;
    int i, rc;

    if (conf != NULL) {
        cdb2_set_comdb2db_config(conf);
        tier = "default";
    }

    puts("CREATING TABLE...");
    cdb2_open(&hndl, argv[1], tier, 0);
    cdb2_run_statement(hndl, "DROP TABLE IF EXISTS t");
    cdb2_run_statement(hndl, "CREATE TABLE t (b blob)");

    puts("INSERTING DATA...");
    blob = malloc(1 << 20);
    cdb2_bind_param(hndl, "b", CDB2_BLOB, blob, 1 << 20);
    for (i = 0; i != 8; ++i)
        cdb2_run_statement(hndl, "INSERT INTO t VALUES(@b)");

    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    if (rc != CDB2_OK_DONE) {
        puts("FAILED");
        puts(cdb2_errstr(hndl));
        return rc;
    }

    puts("DONATING FD TO SOCK POOL...");
    cdb2_close(hndl);

    puts("REOPENING FROM SOCK POOL...");
    hndl = NULL;
    cdb2_open(&hndl, argv[1], tier, 0);

    puts("RUNNING QUERY TO FILL UP TCP SND BUFFER ON SERVER...");
    cdb2_run_statement(hndl, "SELECT * FROM t a, t b, t c");

    puts("SLEEPING 20 SECONDS...");
    sleep(20);

    puts("I'M AWAKE. FETCHING ROWS...");
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    if (rc != CDB2_OK_DONE) {
        puts("FAILED");
        puts(cdb2_errstr(hndl));
        return rc;
    }
    return 0;
}
