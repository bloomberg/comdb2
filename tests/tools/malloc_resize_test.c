#include <cdb2api.h>
#include <stdlib.h>

int main(int argc, char **argv)
{
    cdb2_hndl_tp *hndl = NULL;
    void *dummy = NULL;
    size_t i, len;
    int rc, r, w;
    char *conf = getenv("CDB2_CONFIG");

    if (argc < 2)
        exit(1);

    rc = 0;
    r = w = 1;

    if (argc > 2)
        w = !!atoi(argv[2]);
    if (argc > 3)
        r = !!atoi(argv[3]);

    if (conf != NULL)
        cdb2_set_comdb2db_config(conf);

    rc = cdb2_open(&hndl, argv[1], "default", 0);
    if (rc) {
        fprintf(stderr, "line %d rc %d reason %s\n", __LINE__, rc, cdb2_errstr(hndl));
        goto err;
    }

    /* Write blobs */
    if (w) {
        (void)cdb2_run_statement(hndl, "DROP TABLE t");
        rc = cdb2_run_statement(hndl, "CREATE TABLE t (b blob)");
        if (rc) {
            fprintf(stderr, "line %d rc %d reason %s\n", __LINE__, rc, cdb2_errstr(hndl));
            goto err;
        }

        for (i = 0, len = 1024; i != 2048; ++i, len += 1024)
        {
            dummy = calloc(1, len);

            rc = cdb2_bind_param(hndl, "blob", CDB2_BLOB, dummy, len);
            if (rc) {
                fprintf(stderr, "line %d rc %d reason %s\n", __LINE__, rc, cdb2_errstr(hndl));
                goto err;
            }

            rc = cdb2_run_statement(hndl, "INSERT INTO t VALUES(@blob)");
            if (rc) {
                fprintf(stderr, "line %d rc %d reason %s\n", __LINE__, rc, cdb2_errstr(hndl));
                goto err;
            }

            cdb2_clearbindings(hndl);
            free(dummy);
        }
    }

    /* Read blobs */
    if (r) {
        rc = cdb2_run_statement(hndl, "SELECT * FROM t");
        if (rc) {
            fprintf(stderr, "line %d rc %d reason %s\n", __LINE__, rc, cdb2_errstr(hndl));
            goto err;
        }

        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "line %d rc %d reason %s\n", __LINE__, rc, cdb2_errstr(hndl));
            goto err;
        } else {
            rc = 0;
        }
    }
err:
    if (hndl)
        cdb2_close(hndl);
    return rc;
}
