#undef NDEBUG
#include <assert.h>
#include <signal.h>
#include <stdio.h>
#include <cdb2api.h>

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    int rc;
    cdb2_hndl_tp *hndl = NULL;

    rc = cdb2_open(&hndl, argv[1], argv[2], 0);
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, argv[3]);
    if (rc != CDB2_OK) {
        fprintf(stderr, "cdb2_run_statement failed: %s\n", cdb2_errstr(hndl));
        return 1;
    }

    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        const char *comma = "";
        int columns = cdb2_numcolumns(hndl);
        for (int i = 0; i < columns; ++i) {
            switch (cdb2_column_type(hndl, i)) {
            case CDB2_CSTRING: printf("%s%s", comma, (char *)cdb2_column_value(hndl, i)); break;
            default: abort();
            }
            comma = ",";
        }
    }
    assert(rc == CDB2_OK_DONE);

    rc = cdb2_close(hndl);
    assert(rc == 0);

    return 0;
}
