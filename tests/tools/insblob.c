#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <limits.h>

#include <cdb2api.h>

int main(int argc, char **argv)
{
    int rc;
    int i;

    int st_idx = atoi(argv[1]);
    int end_idx = atoi(argv[2]);
    int size = atoi(argv[3]);
    void *buf = malloc(size);
    char *dbname = "dorintdb";
    char *stage = "dev";

    if (argc >= 4 && argc <= 6) {
        st_idx = atoi(argv[1]);
        end_idx = atoi(argv[2]);
        size = atoi(argv[3]);
        buf = realloc(buf, size);
        if (argc > 4) {
            dbname = argv[4];
        }
        if (argc > 5) {
            stage = argv[5];
        }
    } else {
        fprintf(stderr, "Usage: %s start_col end_col size_blob [dbname stage]\n", argv[0]);
        fprintf(stderr, "       default dbname=dorintdb stage=dev\n");
        return -1;
    }

    cdb2_hndl_tp *hndl = NULL;

    /*cdb2_set_comdb2db_config("dhconfig.cfg");*/
 
    rc = cdb2_open(&hndl, dbname, stage, 0);
    if (rc != 0) {
        puts("cdb2_open");
        goto done;
    }
    cdb2_bind_param(hndl, "data", CDB2_BLOB, buf, size);
    cdb2_bind_param(hndl, "i", CDB2_INTEGER, &i, sizeof(i));
    for (i = st_idx; i <= end_idx; i++) {
        //rc = cdb2_run_statement(hndl, "INSERT INTO '$2_C429139B' (a, b) VALUES (@i, @data)");
        //rc = cdb2_run_statement(hndl, "INSERT INTO '$1_A2620AE4' (a, b) VALUES (@i, @data)");
        //rc = cdb2_run_statement(hndl, "INSERT INTO '$0_F64CD191' (a, b) VALUES (@i, @data)");
        rc = cdb2_run_statement(hndl, "INSERT INTO t (a, b) VALUES (@i, @data)");
        if (rc != 0) {
            fprintf(stderr, "insert failed with rc %d \"%s\"\n", rc, cdb2_errstr(hndl));
            goto done;
        }
    }

    cdb2_clearbindings(hndl);

done:
    free(buf);
    cdb2_close(hndl);
    return rc;
}
