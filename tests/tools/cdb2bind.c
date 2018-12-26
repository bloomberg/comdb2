#include <stdlib.h>
#include <cdb2api.h>
#include <string.h>

const char *db;

/* ========================== Helper functions  =========================== */

/* Open a handle */
void test_open(cdb2_hndl_tp **hndl, const char *db)
{
    int rc = 0;

    if (!db) {
        fprintf(stderr, "db is not set!\n");
        exit(1);
    }

    /* Create a handle */
    char *dest = "local";
    char *conf = getenv("CDB2_CONFIG");
    if (conf) {
        cdb2_set_comdb2db_config(conf);
        dest = "default";
    }

    rc = cdb2_open(hndl, db, dest, 0);
    if (rc) {
        {
            fprintf(stderr, "error opening connection handle for %s (rc: %d)\n",
                    db, rc);
            exit(1);
        }
    }
}

/* Bind a parameter. */
void test_bind_param(cdb2_hndl_tp *hndl, const char *name, int type,
                     const void *varaddr, int length)
{
    int rc;
    rc = cdb2_bind_param(hndl, name, type, varaddr, length);
    if (rc != 0) {
        fprintf(stderr, "error binding parameter %s (err: %s)\n", name,
                cdb2_errstr(hndl));
        exit(1);
    }
}

/* Bind a parameter at the specified index. */
void test_bind_index(cdb2_hndl_tp *hndl, int index, int type,
                     const void *varaddr, int length)
{
    int rc;
    rc = cdb2_bind_index(hndl, index, type, varaddr, length);
    if (rc != 0) {
        fprintf(stderr, "error binding parameter %d (err: %s)\n", index,
                cdb2_errstr(hndl));
        exit(1);
    }
}

/* Execute a SQL query. */
void test_exec(cdb2_hndl_tp *hndl, const char *cmd)
{
    int rc;
    rc = cdb2_run_statement(hndl, cmd);
    if (rc != 0) {
        fprintf(stderr, "error execing %s (err: %s)\n", cmd, cdb2_errstr(hndl));
        exit(1);
    }
}

/* Move to next record in the result set. */
void test_next_record(cdb2_hndl_tp *hndl)
{
    int rc;
    rc = cdb2_next_record(hndl);
    if (rc != 0) {
        fprintf(stderr, "error in cdb2_next_record (err: %s)\n",
                cdb2_errstr(hndl));
        exit(1);
    }
}

/* Close the handle */
void test_close(cdb2_hndl_tp *hndl)
{
    int rc;
    rc = cdb2_close(hndl);
    if (rc != 0) {
        fprintf(stderr, "error closing handle (err: %s, rc: %d)\n",
                cdb2_errstr(hndl), rc);
        exit(1);
    }
}

/* ========================== Tests  =========================== */

void test_01()
{
    cdb2_hndl_tp *hndl = NULL;
    long long int id = 0;

    test_open(&hndl, db);
    test_bind_param(hndl, "id", CDB2_INTEGER, &id, sizeof(long long int));

    for (id = 0; id < 100; id++) {
        long long *val;

        test_exec(hndl, "select @id");
        test_next_record(hndl);
        val = cdb2_column_value(hndl, 0);
        if (*val != id) {
            fprintf(stderr, "error got:%lld expected:%lld\n", *val, id);
            exit(1);
        }
    }
    cdb2_clearbindings(hndl);

    test_bind_index(hndl, 1, CDB2_INTEGER, &id, sizeof(long long int));

    for (id = 0; id < 100; id++) {
        long long *val;

        test_exec(hndl, "select ?");
        test_next_record(hndl);
        val = cdb2_column_value(hndl, 0);
        if (*val != id) {
            fprintf(stderr, "error got:%lld expected:%lld\n", *val, id);
            exit(1);
        }
    }
    cdb2_clearbindings(hndl);

    test_close(hndl);
}

// https://github.com/bloomberg/comdb2/issues/963
void test_02()
{
    cdb2_hndl_tp *hndl = NULL;
    char str_buf[] = {'h', 'e', 'l', 'l', 'o', '\0', 'f', 'o', 'o'};
    const char *hello = "hello";
    const char *drop_table = "DROP TABLE IF EXISTS t1";
    const char *create_table_cstring = "CREATE TABLE t1 {schema {cstring a[20]}}";
    const char *create_table_vutf8 = "CREATE TABLE t1 {schema {vutf8 a}}";
    const char *insert_cmd = "INSERT INTO t1 (a) VALUES (@val);";
    const char *select_cmd = "SELECT * FROM t1 WHERE a=@val;";
    size_t value_size;
    void *buffer;

    test_open(&hndl, db);

    /* Test with CSTRING */
    test_exec(hndl, drop_table);
    test_exec(hndl, create_table_cstring);
    test_bind_param(hndl, "val", CDB2_CSTRING, str_buf, sizeof(str_buf));
    test_exec(hndl, insert_cmd);
    test_exec(hndl, select_cmd);
    test_next_record(hndl);

    /* Check the column value */
    value_size = cdb2_column_size(hndl, 0);
    buffer = malloc(value_size);
    memcpy(buffer, cdb2_column_value(hndl, 0), value_size);
    if ((strncmp((char *)buffer, hello, value_size) != 0)) {
        fprintf(stderr, "column value didn't match got:%s expected:%s\n",
                (char *)buffer, hello);
        exit(1);
    }
    free(buffer);
    cdb2_clearbindings(hndl);
    test_exec(hndl, drop_table);

    /* Test with VUTF */
    test_exec(hndl, create_table_vutf8);
    test_bind_param(hndl, "val", CDB2_CSTRING, str_buf, sizeof(str_buf));
    test_exec(hndl, insert_cmd);
    test_exec(hndl, select_cmd);
    test_next_record(hndl);

    /* Check the column value */
    value_size = cdb2_column_size(hndl, 0);
    buffer = malloc(value_size);
    memcpy(buffer, cdb2_column_value(hndl, 0), value_size);
    if ((strncmp((char *)buffer, hello, value_size) != 0)) {
        fprintf(stderr, "column value didn't match got:%s expected:%s\n",
                (char *)buffer, hello);
        exit(1);
    }
    free(buffer);
    cdb2_clearbindings(hndl);
    test_exec(hndl, drop_table);

    /* Close the handle */
    test_close(hndl);
}

int main(int argc, char *argv[])
{
    db = argv[1];

    test_01();
    test_02();

    return 0;
}
