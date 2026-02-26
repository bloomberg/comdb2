/*
 * simpleauth_test - run a SQL query with a given principal identity
 *
 * Usage: simpleauth_test <dbname> <principal> <sql>
 *
 * Constructs a dummy IdentityBlob with the given principal string,
 * sets it on the cdb2api handle, and runs the query. Prints results
 * to stdout; exits 0 on success, 1 on failure.
 */
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <csignal>

extern "C" {
#include <cdb2api.h>
#include <cdb2api_hndl.h>
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);

    if (argc < 4) {
        fprintf(stderr, "Usage: %s <dbname> <principal> <sql>\n", argv[0]);
        return 1;
    }

    const char *dbname = argv[1];
    const char *principal = argv[2];
    const char *sql = argv[3];

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    cdb2_hndl_tp *hndl = nullptr;
    int rc = cdb2_open(&hndl, dbname, "default", 0);
    if (rc) {
        fprintf(stderr, "cdb2_open failed: %s\n", cdb2_errstr(hndl));
        return 1;
    }

    /* Build a dummy IdentityBlob with the given principal */
    auto *id = static_cast<CDB2SQLQUERY__IdentityBlob *>(calloc(1, sizeof(CDB2SQLQUERY__IdentityBlob)));
    cdb2__sqlquery__identity_blob__init(id);
    id->principal = strdup(principal);
    id->majorversion = 1;
    id->minorversion = 0;
    id->data.data = reinterpret_cast<uint8_t *>(strdup("dummy"));
    id->data.len = 5;
    hndl->id_blob = id;

    rc = cdb2_run_statement(hndl, sql);
    if (rc) {
        fprintf(stderr, "[%s] failed with rc %d %s\n", sql, rc, cdb2_errstr(hndl));
        free(id->principal);
        free(id->data.data);
        free(id);
        hndl->id_blob = nullptr;
        cdb2_close(hndl);
        return 1;
    }

    int ncols = cdb2_numcolumns(hndl);
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        printf("(");
        for (int i = 0; i < ncols; i++) {
            if (i > 0)
                printf(", ");
            const char *name = cdb2_column_name(hndl, i);
            void *val = cdb2_column_value(hndl, i);
            int type = cdb2_column_type(hndl, i);
            if (val == nullptr) {
                printf("%s=NULL", name);
            } else if (type == CDB2_INTEGER) {
                printf("%s=%lld", name, *static_cast<long long *>(val));
            } else if (type == CDB2_REAL) {
                printf("%s=%f", name, *static_cast<double *>(val));
            } else if (type == CDB2_CSTRING) {
                printf("%s='%s'", name, static_cast<char *>(val));
            } else {
                printf("%s=<binary>", name);
            }
        }
        printf(")\n");
    }

    free(id->principal);
    free(id->data.data);
    free(id);
    hndl->id_blob = nullptr;
    cdb2_close(hndl);
    return (rc == CDB2_OK_DONE) ? 0 : 1;
}
