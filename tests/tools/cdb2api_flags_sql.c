#include <stdio.h>
#include <stdlib.h>

#include <cdb2api.h>

int main(int argc, char **argv)
{
    cdb2_hndl_tp *hndl = NULL;
    char *conf = getenv("CDB2_CONFIG");
    int rc;

    if (argc < 5) {
        fprintf(stderr, "Usage: %s <dbname> <tier> <cdb2_open flags> <sql> [<sql> ...]\n", argv[0]);
        return 1;
    }

    if (conf != NULL)
        cdb2_set_comdb2db_config(conf);

    /* All statements share one handle so they run on the same node */
    rc = cdb2_open(&hndl, argv[1], argv[2], (int)strtol(argv[3], NULL, 0));
    if (rc != 0) {
        fprintf(stderr, "Error opening a handle: %d: %s.\n", rc, cdb2_errstr(hndl));
        return 1;
    }

    for (int i = 4; i < argc; ++i) {
        rc = cdb2_run_statement(hndl, argv[i]);
        if (rc != 0) {
            fprintf(stderr, "Error running '%s': %d: %s.\n", argv[i], rc, cdb2_errstr(hndl));
            return 1;
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
            int ncols = cdb2_numcolumns(hndl);
            printf("(");
            for (int col = 0; col < ncols; ++col) {
                void *val = cdb2_column_value(hndl, col);
                printf("%s%s=", col ? ", " : "", cdb2_column_name(hndl, col));
                if (val == NULL) {
                    printf("NULL");
                    continue;
                }
                switch (cdb2_column_type(hndl, col)) {
                case CDB2_INTEGER: printf("%lld", *(long long *)val); break;
                case CDB2_REAL: printf("%f", *(double *)val); break;
                case CDB2_CSTRING: printf("'%s'", (char *)val); break;
                default: printf("<type %d>", cdb2_column_type(hndl, col)); break;
                }
            }
            printf(")\n");
        }
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "Error reading '%s': %d: %s.\n", argv[i], rc, cdb2_errstr(hndl));
            return 1;
        }
    }

    cdb2_close(hndl);
    return 0;
}
