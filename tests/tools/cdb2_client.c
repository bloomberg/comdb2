#include <cdb2api.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>

void _abort(cdb2_hndl_tp *hndl, const char *method)
{
    printf("%s: %s. Exiting\n", method, cdb2_errstr(hndl));
    cdb2_close(hndl);
    exit(1);
}

/* Can't use open as function name due to naming conflicts with libssl. */
void _open(cdb2_hndl_tp **hndl, const char *db, const char *type)
{
    int ret;

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    ret = cdb2_open(hndl, db, type, 0);

    if (ret != 0)
    {
        _abort(*hndl, "cdb2_open");
    }
}

void _exec(cdb2_hndl_tp *hndl, const char *sql)
{
    int ret;
    int coltype;

    printf("%s\n", sql);

    ret= cdb2_run_statement(hndl, sql);

    if (ret != 0)
    {
        _abort(hndl, "cdb2_run_statement");
    }

    while (1)
    {
        ret= cdb2_next_record(hndl);
        switch (ret)
        {
        case CDB2_OK:
            printf("  ");
            coltype= cdb2_column_type(hndl, 0);
            switch (coltype)
            {
                case CDB2_INTEGER:
                    printf("%d\n", *(int *) cdb2_column_value(hndl, 0));
                    break;
                case CDB2_CSTRING:
                    printf("%s\n", (const char *) cdb2_column_value(hndl, 0));
                    break;
                default:
                    _abort(hndl, "cdb2_run_statement");
            }
            break;
        case CDB2_OK_DONE:
            return;
        default:
            _abort(hndl, "cdb2_run_statement");
        }
    }
    return;
}

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        printf("Invalid number of args\n");
        exit(1);
    }

    cdb2_hndl_tp *hndl= NULL;
    _open(&hndl, argv[1], "default");
    _exec(hndl, "SELECT 1");
    cdb2_close(hndl);

    return 0;
}
