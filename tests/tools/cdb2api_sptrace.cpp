#undef NDEBUG
#include <assert.h>
#include <cstdio>
#include <libgen.h>
#include <signal.h>

#include <cdb2api.h>

static void load_config(const char *db, const char *tier)
{
    int rc;
    cdb2_hndl_tp *hndl = NULL;
    rc = cdb2_open(&hndl, db, tier, 0);
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "SELECT 1");
    assert(rc == 0);
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        ;
    assert(rc == CDB2_OK_DONE);
    rc = cdb2_close(hndl);
    assert(rc == 0);
}

static void sp_debug(const char *db, const char *tier)
{
#   ifdef _LINUX_SOURCE
    cdb2_hndl_tp *hndl = NULL;
    int rc = cdb2_open(&hndl, db, tier, 0);
    assert(rc == 0);
    rc = cdb2_run_statement(hndl, "SET SPDEBUG ON");
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        ;
    assert(rc == CDB2_OK_DONE);

    char buf[] = "next\nnext\nnext";
    fclose(stdin);
    stdin = fmemopen(buf, sizeof(buf), "r");

    rc = cdb2_run_statement(hndl, "EXEC PROCEDURE spdebug()");
    assert(rc == 0);

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

    /* interactive debugging should have consumed stdin buffer */
    assert(feof(stdin) != 0);

    rc = cdb2_close(hndl);
    assert(rc == 0);
#   endif
}

static void sp_trace(const char *db, const char *tier)
{
    cdb2_hndl_tp *hndl = NULL;
    int rc = cdb2_open(&hndl, db, tier, 0);
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "SET SPTRACE ON");
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        ;
    assert(rc == CDB2_OK_DONE);

    rc = cdb2_run_statement(hndl, "EXEC PROCEDURE sptrace()");
    assert(rc == 0);

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

}

static void sp(const char *db, const char *tier)
{
    load_config(db, tier);
    sp_debug(db, tier);
    sp_trace(db, tier);
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    char *dbname = argv[1];

    sp(dbname, "default");
    return 0;
}
