#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stddef.h>

#include <cdb2api.h>

int main(int argc, char *argv[]) {
    char *dbname = argv[1];
    int64_t id = atoi(argv[2]);
    off_t sz = atoi(argv[3]);
    void *buf;

    if (argc != 4) {
        printf("Usage: dbname id size\n");
        return 1;
    }

    char *config;
    config = getenv("CDB2_CONFIG");
    if (config)
        cdb2_set_comdb2db_config(config);

    buf = calloc(1, sz);
    if (buf == NULL) {
        printf("can't allocate %zd bytes for buffer\n", (size_t) sz);
        return 1;
    }
    cdb2_hndl_tp *db;
    int rc = cdb2_open(&db, dbname, "default", 0);
    if (rc) {
        printf("open %d %s\n", rc, cdb2_errstr(db));
        return 1;
    }

    cdb2_bind_param(db, "a", CDB2_INTEGER, &id, sizeof(int64_t));
    cdb2_bind_param(db, "b", CDB2_BLOB, buf, sz);
    rc = cdb2_run_statement(db, "insert into t(a, b) values(@a, @b)");
    if (rc) {
        printf("insert %d %s\n", rc, cdb2_errstr(db));
    }

    cdb2_clearbindings(db);
    cdb2_bind_param(db, "a", CDB2_INTEGER, &id, sizeof(int64_t));
    rc = cdb2_run_statement(db, "select * from t where a=@a");
    if (rc) {
        printf("run select %d %s\n", rc, cdb2_errstr(db));
        return 1;
    }
    rc = cdb2_next_record(db);
    while (rc == CDB2_OK) {
        rc = cdb2_next_record(db);
    }
    if (rc != CDB2_OK_DONE) {
        printf("next %d %s\n", rc, cdb2_errstr(db));
    }

    cdb2_clearbindings(db);
    cdb2_bind_param(db, "a", CDB2_INTEGER, &id, sizeof(int64_t));
    rc = cdb2_run_statement(db, "delete from t where a = @a");
    if (rc) {
        printf("run delete %d %s\n", rc, cdb2_errstr(db));
    }

    cdb2_clearbindings(db);
    cdb2_bind_param(db, "a", CDB2_INTEGER, &id, sizeof(int64_t));
    rc = cdb2_run_statement(db, "insert into t(a, b) values(@a, x'')");
    if (rc) {
        printf("run b %d %s\n", rc, cdb2_errstr(db));
    }

    cdb2_clearbindings(db);
    cdb2_bind_param(db, "a", CDB2_INTEGER, &id, sizeof(int64_t));
    cdb2_bind_param(db, "b", CDB2_BLOB, buf, sz);
    rc = cdb2_run_statement(db, "update t set b=@b where a=@a");
    if (rc) {
        printf("run b %d %s\n", rc, cdb2_errstr(db));
    }

    cdb2_clearbindings(db);
    cdb2_bind_param(db, "a", CDB2_INTEGER, &id, sizeof(int64_t));
    rc = cdb2_run_statement(db, "select * from t where a=@a");
    if (rc) {
        printf("run select %d %s\n", rc, cdb2_errstr(db));
        return 1;
    }
    rc = cdb2_next_record(db);
    while (rc == CDB2_OK) {
        rc = cdb2_next_record(db);
    }
    if (rc != CDB2_OK_DONE) {
        printf("next %d %s\n", rc, cdb2_errstr(db));
    }

    cdb2_close(db);
    return 0;
}
