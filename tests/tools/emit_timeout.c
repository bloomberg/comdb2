#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <cdb2api.h>

int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr, "Missing args: <dbname> <expected> <timeout> <tier> [cfg-file]\n");
        return EXIT_FAILURE;
    }

    char *db = argv[1];
    char *expected = argv[2];
    int timeout = atoi(argv[3]);
    char *tier = argv[4];
    char *cfg = argc == 6 ? argv[5] : NULL;

    int rc;
    cdb2_hndl_tp *hndl = NULL;
    if (cfg) {
        cdb2_set_comdb2db_config(cfg);
    }
    rc = cdb2_open(&hndl, db, tier, 0);
    if (rc) {
        fprintf(stderr, "cdb2open rc:%d\n", rc);
        return EXIT_FAILURE;
    }
    char exec_procedure[512];
    snprintf(exec_procedure, sizeof(exec_procedure), "exec procedure emit_test(%s)", expected);
    rc = cdb2_run_statement(hndl, exec_procedure);
    if (rc) {
        fprintf(stderr, "cdb2_run_statement rc:%d err:'%s'\n", rc,
                cdb2_errstr(hndl));
        cdb2_close(hndl);
        return EXIT_FAILURE;
    }
    if (timeout) {
        sleep(timeout);
    }
    do {
        rc = cdb2_next_record(hndl);
    } while (rc == CDB2_OK);
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "cdb2_next_record rc:%d err:'%s'\n", rc,
                cdb2_errstr(hndl));
        cdb2_close(hndl);
        return EXIT_FAILURE;
    }
    cdb2_close(hndl);
    return EXIT_SUCCESS;
}
