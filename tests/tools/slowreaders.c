#include <unistd.h>
#include <cdb2api.h>

int main(int argc, char **argv)
{
    char *db = argv[1];
    char *host = argv[2];
    cdb2_hndl_tp *hndl = NULL;

    cdb2_open(&hndl, db, host, CDB2_DIRECT_CPU);

    /* should show in longreqs, but should not have a netwaittime */
    cdb2_run_statement(hndl, "SELECT SLEEP(5)");
    while (cdb2_next_record(hndl) == CDB2_OK);

    /* should show in longreqs, and should have a netwaittime of around 5 seconds */
    cdb2_run_statement(hndl, "SELECT zeroblob(134217728) FROM generate_series(1, 5)");
    do {
        sleep(1);
    } while (cdb2_next_record(hndl) == CDB2_OK);

    cdb2_close(hndl);
    return 0;
}
