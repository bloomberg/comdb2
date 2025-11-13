#include <stdio.h>
#include <string.h>
#include <cdb2api.h>
#include <signal.h>

static int Cdb2_open(cdb2_hndl_tp **db, char *dbname)
{
    int rc = cdb2_open(db, dbname, "default", 0);
    if (rc != 0) {
        fprintf(stderr, "cdb2_open rc:%d err:%s\n", rc, cdb2_errstr(*db));
        return -1;
    }
    return 0;
}

static int Cdb2_run_statement(cdb2_hndl_tp *db, const char *sql)
{
    int rc = cdb2_run_statement(db, sql);
    if (rc != 0) {
        fprintf(stderr, "cdb2_run_statement rc:%d err:%s\n", rc, cdb2_errstr(db));
        return -1;
    }
    while ((rc = cdb2_next_record(db)) == CDB2_OK) {
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "cdb2_next_record rc:%d err:%s\n", rc, cdb2_errstr(db));
        return -1;
    }
    return 0;
}

static int test_all_types(char *dbname)
{
    printf("TEST: %s\n", __func__);
    struct {
        cdb2_coltype type;
        const char *typestr;
    } types[] = {
        {CDB2_INTEGER, "INTEGER"},
        {CDB2_REAL, "REAL"},
        {CDB2_CSTRING, "CSTRING"},
        {CDB2_BLOB, "BLOB"},
        {CDB2_DATETIME, "DATETIME"},
        {CDB2_INTERVALYM, "INTERVALYM"},
        {CDB2_INTERVALDS, "INTERVALDS"},
        {CDB2_DATETIMEUS, "DATETIMEUS"},
        {CDB2_INTERVALDSUS, "INTERVALDSUS"}
    };
    const int n_types = sizeof(types) / sizeof(types[0]);
    char set_sql[512];
    char select_sql[512];
    const char *comma = "";
    snprintf(set_sql, sizeof(set_sql), "set statement return types");
    snprintf(select_sql, sizeof(select_sql), "select");
    for (int i = 0; i < n_types; ++i) {
        strncat(set_sql, " ", sizeof(set_sql) - 1);
        strncat(set_sql, types[i].typestr, sizeof(set_sql) - 1);

        strncat(select_sql, comma, sizeof(select_sql) - 1);
        strncat(select_sql, " NULL", sizeof(select_sql) - 1);
        comma = ",";
    }

    cdb2_hndl_tp *db;
    if (Cdb2_open(&db, dbname) != 0) return -1;
    if (Cdb2_run_statement(db, set_sql) != 0) return -1;
    if (Cdb2_run_statement(db, select_sql) != 0) return -1;
    int rc;
    while ((rc = cdb2_next_record(db)) == CDB2_OK) {
        for (int i = 0; i < n_types; ++i) {
            if (cdb2_column_type(db, i) != types[i].type) {
                fprintf(stderr, "%s: expected type:%d received type:%d\n",
                        __func__, types[i].type, cdb2_column_type(db, i));
                return -1;
            } else {
                printf("col:%d type:%s\n", i, types[i].typestr);
            }
        }
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "cdb2_next_record rc:%d err:%s\n", rc, cdb2_errstr(db));
        return -1;
    }
    return cdb2_close(db);
}

static int test_leading_space(char *dbname)
{
    printf("TEST: %s\n", __func__);
    cdb2_hndl_tp *db;
    if (Cdb2_open(&db, dbname) != 0) return -1;
    if (Cdb2_run_statement(db, "   set statement return types integer integer") != 0) return -1;
    if (cdb2_run_statement(db, "   select '100', NULL") != 0) return -1;
    int64_t *val;
    int type;
    int rc = cdb2_next_record(db);
    if (rc != CDB2_OK) {
        fprintf(stderr, "cdb2_next_record rc:%d err:%s\n", rc, cdb2_errstr(db));
        return -1;
    }
    type = cdb2_column_type(db, 0);
    if (type != CDB2_INTEGER) {
        fprintf(stderr, "%s failed. col:0 expected CDB2_INTEGER:%d received:%d\n", __func__, CDB2_INTEGER, type);
        return -1;
    }
    type = cdb2_column_type(db, 1);
    if (type != CDB2_INTEGER) {
        fprintf(stderr, "%s failed. col:1 expected CDB2_INTEGER:%d received:%d\n", __func__, CDB2_INTEGER, type);
        return -1;
    }
    val = (int64_t *)cdb2_column_value(db, 0);
    if (*val != 100) {
        fprintf(stderr, "%s failed. expected 100 received:%ld\n", __func__, *val);
        return -1;
    }
    val = (int64_t *)cdb2_column_value(db, 1);
    if (val != NULL) {
        fprintf(stderr, "%s failed. expected NULL received:%p val:%ld\n", __func__, val, *val);
        return -1;
    }
    rc = cdb2_next_record(db);
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "cdb2_next_record rc:%d err:%s\n", rc, cdb2_errstr(db));
        return -1;
    }
    return cdb2_close(db);
}

static int test_multiple_set(char *dbname)
{
    printf("TEST: %s\n", __func__);
    cdb2_hndl_tp *db;
    if (Cdb2_open(&db, dbname) != 0) return -1;
    if (Cdb2_run_statement(db, "set statement return types integer") != 0) return -1;
    if (Cdb2_run_statement(db, "set statement return types integer") == 0) return -1;
    return cdb2_close(db);
}

static int test_set_with_run_typed(char *dbname)
{
    printf("TEST: %s\n", __func__);
    cdb2_hndl_tp *db;
    int types[] = {CDB2_INTEGER};
    if (Cdb2_open(&db, dbname) != 0) return -1;
    if (Cdb2_run_statement(db, "set statement return types integer") != 0) return -1;
    if (cdb2_run_statement_typed(db, "select 1", 1, types) == 0) return -1;
    fprintf(stderr, "cdb2_run_statement_typed err:%s\n", cdb2_errstr(db));
    return cdb2_close(db);
}

static int test_n_columns(int n, char *dbname)
{
    printf("%s cols:%d\n", __func__, n);
    cdb2_hndl_tp *db;
    if (Cdb2_open(&db, dbname) != 0) return -1;
    const size_t sz = 20 * 1024;
    char *set_sql = (char *)malloc(sz);
    snprintf(set_sql, sz, "set statement return types");
    for (int i = 0; i < n; ++i) strncat(set_sql, " BLOB", sz);
    int rc = Cdb2_run_statement(db, set_sql);
    free(set_sql);
    cdb2_close(db);
    return rc;
}

static int test_too_many_columns(char *dbname)
{
    printf("TEST: %s\n", __func__);
    if (test_n_columns(0, dbname) == 0) return -1;
    if (test_n_columns(1, dbname) != 0) return -1;
    if (test_n_columns(1023, dbname) != 0) return -1;
    if (test_n_columns(1024, dbname) != 0) return -1;
    if (test_n_columns(1025, dbname) == 0) return -1;
    return 0;
}

static int test_bad_column_type(char *dbname)
{
    printf("TEST: %s\n", __func__);
    cdb2_hndl_tp *db;
    if (Cdb2_open(&db, dbname) != 0) return -1;

    if (Cdb2_run_statement(db, "set ") != 0) return -1;
    if (Cdb2_run_statement(db, "select 1") == 0) return -1;

    if (Cdb2_run_statement(db, "set  statement") != 0) return -1;
    if (Cdb2_run_statement(db, "select 1") == 0) return -1;

    if (Cdb2_run_statement(db, "set statement  return") != 0) return -1;
    if (Cdb2_run_statement(db, "select 1") == 0) return -1;

    if (Cdb2_run_statement(db, "set statement return  type") != 0) return -1;
    if (Cdb2_run_statement(db, "select 1") == 0) return -1;

    if (Cdb2_run_statement(db, "set statement return  types") == 0) return -1;
    if (Cdb2_run_statement(db, "set statement return types    ") == 0) return -1;
    if (Cdb2_run_statement(db, "set statement return types int") == 0) return -1;
    if (Cdb2_run_statement(db, "set statement return types datetimex") == 0) return -1;
    if (Cdb2_run_statement(db, "set statement return types vutf8") == 0) return -1;
    if (Cdb2_run_statement(db, "set statement return types decimal") == 0) return -1;

    return cdb2_close(db);
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    char *dbname = argv[1];

    if (test_all_types(dbname) != 0) return EXIT_FAILURE;
    if (test_leading_space(dbname) != 0) return EXIT_FAILURE;
    if (test_multiple_set(dbname)) return EXIT_FAILURE;
    if (test_set_with_run_typed(dbname)) return EXIT_FAILURE;
    if (test_too_many_columns(dbname)) return EXIT_FAILURE;
    if (test_bad_column_type(dbname)) return EXIT_FAILURE;
    return EXIT_SUCCESS;
}
