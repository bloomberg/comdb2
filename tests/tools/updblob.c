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

    int ix;
    int st_idx;
    int end_idx;
    int size;
    void *buf;
    char *dbname = "dorintdb";
    char *stage = "dev";

    if (argc >= 5 && argc <= 7) {
        ix = atoi(argv[1]);
        st_idx = atoi(argv[2]);
        end_idx = atoi(argv[3]);
        size = atoi(argv[4]);
        buf = calloc(1, size);
        if (argc > 5) {
            dbname = argv[5];
        }
        if (argc > 6) {
            stage = argv[6];
        }
    } else {
        fprintf(stderr, "Usage: %s index start_col end_col size_blob [dbname stage]\n", argv[0]);
        fprintf(stderr, "       default dbname=dorintdb stage=dev\n");
        return -1;
    }

    cdb2_hndl_tp *hndl = NULL;

    /*cdb2_set_comdb2db_config("dhconfig.cfg");*/
    srand(ix);

    int *ibuf = (int*)buf;
    int rows = 0;

    for (i=0; i<size/sizeof(int); i++)
        ibuf[i] = rand();
 
    rc = cdb2_open(&hndl, dbname, stage, 0);
    if (rc != 0) {
        puts("cdb2_open");
        goto done;
    }
    cdb2_bind_param(hndl, "data", CDB2_BLOB, buf, size);
    cdb2_bind_param(hndl, "i", CDB2_INTEGER, &i, sizeof(i));
    for (i = st_idx; i <= end_idx; i++) {
        if (rand() % 3 != 0) continue;
        rows++;
        rc = cdb2_run_statement(hndl, "UPDATE t SET a=@i, b=@data where a=@i");
        if (rc != 0) {
            fprintf(stderr, "update failed with rc %d \"%s\"\n", rc, cdb2_errstr(hndl));
            goto done;
        }
    }

    cdb2_clearbindings(hndl);

done:
    fprintf(stdout, "Updated %d rows\n", rows);
    free(buf);
    cdb2_close(hndl);
    return rc;
}
