#include <cdb2api.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <errno.h>
#include <sys/wait.h>
#include <stdlib.h>


int main(int argc, char **argv)
{
    char *data;
    int rc, i;

    pid_t child;

    if (argc < 3) {
        fprintf(stderr, "Usage: <executable> <# inserts> <dbname> [ [node] | [type cfg_file] ]\n");
        exit(1);
    }

    cdb2_init_ssl(1, 1);

    for (i = 0; i != atoi(argv[1]); ++i) {
        child = fork();
        if (child < 0)
            return 1;

        if (child == 0) {
            cdb2_hndl_tp *hndl = NULL;
            if (argc == 3)
                rc = cdb2_open(&hndl, argv[2], "local", 0);
            else if(argc == 5) {
                cdb2_set_comdb2db_config(argv[4]);
                rc = cdb2_open(&hndl, argv[2], argv[3], 0);
            }
            else
                rc = cdb2_open(&hndl, argv[2], argv[3], CDB2_DIRECT_CPU);

            if (rc) {
                fprintf(stderr, "failed to open handle\n");
                exit(1);
            }

            const int SZ = 1024 * 1024 * 4;
            data = malloc(SZ);

            rc = cdb2_run_statement(hndl, "begin");
            if (rc) {
                fprintf(stderr, "%d BEGIN failed rc = %d, error = %s\n", i, rc, cdb2_errstr(hndl));
                exit(1);
            }
            cdb2_bind_param(hndl, "data", CDB2_BLOB, data, SZ);
            rc = cdb2_run_statement(hndl, "insert into tbl values (@data)");
            if (rc) {
                fprintf(stderr, "%d INSERT failed rc = %d, error = %s\n", i, rc, cdb2_errstr(hndl));
                exit(1);
            }
            cdb2_clearbindings(hndl);
            rc = cdb2_run_statement(hndl, "commit");
            if (rc) {
                fprintf(stderr, "%d COMMIT failed rc = %d, error = %s\n", i, rc, cdb2_errstr(hndl));
                exit(1);
            } else {
                fprintf(stdout, "inserted\n");
            }

            cdb2_close(hndl);
            return 0;
        }
    }

    for (i = 0; i != atoi(argv[1]); ++i)
        wait(NULL);

    return 0;
}
