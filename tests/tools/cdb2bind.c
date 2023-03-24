#include <stdlib.h>
#include <cdb2api.h>
#include <string.h>
#include <inttypes.h>

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
    printf("SQL: %s\n", cmd);
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
        fprintf(stderr, "error in cdb2_next_record (err: %s, rc: %d)\n",
                cdb2_errstr(hndl), rc);
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

// Test to verify empty and NULL values bind as expected.
void test_03()
{
    cdb2_hndl_tp *hndl = NULL;

    char c1[] = {0};
    const char *c2 = "";
    int row_count;
    int expected_row_count = 2;

    const char *drop_table = "DROP TABLE IF EXISTS t1";
    const char *create_table_cmd =
        "CREATE TABLE t1 {schema {blob c1 null=yes cstring c2[10] null=yes}}";
    const char *insert_cmd = "INSERT INTO t1 VALUES (@c1, @c2)";
    const char *insert_index_cmd = "INSERT INTO t1 VALUES (?, ?)";
    const char *select_empty_cmd =
        "SELECT COUNT(*) FROM t1 WHERE c1 = x'' AND c2 = ''";
    const char *select_null_cmd =
        "SELECT COUNT(*) FROM t1 WHERE c1 IS NULL AND c2 IS NULL";

    test_open(&hndl, db);

    test_exec(hndl, drop_table);
    test_exec(hndl, create_table_cmd);

    // Test empty values using bind_param
    test_bind_param(hndl, "c1", CDB2_BLOB, (void *)c1, 0);
    test_bind_param(hndl, "c2", CDB2_CSTRING, c2, strlen(c2));

    test_exec(hndl, insert_cmd);
    cdb2_clearbindings(hndl);

    // Test empty values using bind_index
    test_bind_index(hndl, 1, CDB2_BLOB, (void *)c1, 0);
    test_bind_index(hndl, 2, CDB2_CSTRING, c2, strlen(c2));
    test_exec(hndl, insert_index_cmd);
    cdb2_clearbindings(hndl);

    test_exec(hndl, select_empty_cmd);
    test_next_record(hndl);
    // Check row count
    row_count = (int)*((long long *)cdb2_column_value(hndl, 0));
    if (row_count != expected_row_count) {
        fprintf(stderr, "invalid row count, got: %d, expected: %d", row_count,
                expected_row_count);
        exit(1);
    }

    // Test NULL values
    test_bind_param(hndl, "c1", CDB2_BLOB, (void *)0, 0);
    test_bind_param(hndl, "c2", CDB2_CSTRING, 0, 0);

    test_exec(hndl, insert_cmd);
    cdb2_clearbindings(hndl);

    // Test NULL values using bind_index
    test_bind_index(hndl, 1, CDB2_BLOB, NULL, 0);
    test_bind_index(hndl, 2, CDB2_CSTRING, NULL, 0);
    test_exec(hndl, insert_index_cmd);
    cdb2_clearbindings(hndl);

    test_exec(hndl, select_null_cmd);
    test_next_record(hndl);
    // Check row count
    row_count = (int)*((long long *)cdb2_column_value(hndl, 0));
    if (row_count != expected_row_count) {
        fprintf(stderr, "invalid row count, got: %d, expected: %d", row_count,
                expected_row_count);
        exit(1);
    }

    // Cleanup
    test_exec(hndl, drop_table);

    // Close the handle
    test_close(hndl);
}

void test_04()
{
    cdb2_hndl_tp *hndl = NULL;
    char str_buf[] = {'h', 'e', 'l', 'l', 'o', '\0', 'f', 'o', 'o'};
    const char *hello = "hello";
    const char *drop_table = "DROP TABLE IF EXISTS t1";
    const char *create_table_cstring = "CREATE TABLE t1 {schema {cstring a[20]}}";
    const char *insert_cmd = "INSERT INTO t1 (a) VALUES (@1);";
    const char *select_cmd = "SELECT * FROM t1 WHERE a=@1;";
    size_t value_size;
    void *buffer;

    test_open(&hndl, db);

    /* Test with bind name "1" */
    test_exec(hndl, drop_table);
    test_exec(hndl, create_table_cstring);
    test_bind_param(hndl, "1", CDB2_CSTRING, str_buf, sizeof(str_buf));
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

void test_bind_array()
{
    cdb2_hndl_tp *hndl = NULL;
    test_open(&hndl, db);

    {   // bind int32 array
        int N = 2048;
        int values[N];
        for (int i = 0; i < N; i++) {
            values[i] = i + 123;
        }
        cdb2_bind_array(hndl, "myintarr", CDB2_INTEGER, (const void *)values, N, sizeof(values[0]));
        test_exec(hndl, "select * from carray(@myintarr) ");
        for (int i = 0; i < N; i++) {
            test_next_record(hndl);
            int64_t *vala = cdb2_column_value(hndl, 0);
            if (*vala != values[i]) {
                fprintf(stderr, "%s:%d:error got:%"PRId64" expected:%d\n", __func__, __LINE__, *vala, values[i]);
                exit(1);
            }

            //printf("TEST 05 RESP %"PRId64"\n", *vala);
        }

        cdb2_clearbindings(hndl);
    }
    {   // bind int64 array
        int N = 2048;
        int64_t values[N];
        for (int i = 0; i < N; i++) {
            values[i] = i + 123;
        }
        cdb2_bind_array(hndl, "mylongintarr", CDB2_INTEGER, (const void *)values, N, sizeof(values[0]));
        test_exec(hndl, "select * from carray(@mylongintarr) ");
        for (int i = 0; i < N; i++) {
            test_next_record(hndl);
            int64_t *vala = cdb2_column_value(hndl, 0);
            if (*vala != values[i]) {
                fprintf(stderr, "%s:%d:error got:%"PRId64" expected:%"PRId64"\n", __func__, __LINE__, *vala, values[i]);
                exit(1);
            }

            //printf("TEST 05 RESP %"PRId64"\n", *vala);
        }

        cdb2_clearbindings(hndl);
    } 

    {   // bind double array
        int N = 2048;
        double values[N];
        for (int i = 0; i < N; i++) {
            values[i] = i + 123;
        }
        cdb2_bind_array(hndl, "mydblarr", CDB2_REAL, (const void *)values, N, sizeof(values[0]));
        test_exec(hndl, "select * from carray(@mydblarr) ");
        for (int i = 0; i < N; i++) {
            test_next_record(hndl);
            double *vala = cdb2_column_value(hndl, 0);
            if (*vala != values[i]) {
                fprintf(stderr, "%s:%d:error got:%lf expected:%lf\n", __func__, __LINE__, *vala, values[i]);
                exit(1);
            }

            //printf("TEST 05 RESP %lf\n", *vala);
        }

        cdb2_clearbindings(hndl);
    }
    {   // bind char* array
        int N = 2048;
        char* values[N];
        for (int i = 0; i < N; i++) {
            values[i] = malloc(30);
            snprintf(values[i], 29, "Hello %d", i + 123);
        }
        cdb2_bind_array(hndl, "mytxtarr", CDB2_CSTRING, (const void *)values, N, 0);
        test_exec(hndl, "select * from carray(@mytxtarr) ");
        for (int i = 0; i < N; i++) {
            test_next_record(hndl);
            char *vala = cdb2_column_value(hndl, 0);
            if (strcmp(vala, values[i]) != 0) {
                fprintf(stderr, "%s:%d:error got:%s expected:%s\n", __func__, __LINE__, vala, values[i]);
                exit(1);
            }

            //printf("TEST 05 RESP %s\n", vala);
        }

        cdb2_clearbindings(hndl);
        for (int i = 0; i < N; i++)
            free(values[i]);
    }

    /* Close the handle */
    test_close(hndl);
}

void test_bind_array2()
{
    cdb2_hndl_tp *hndl = NULL;
    test_open(&hndl, db);

    test_exec(hndl, "drop table if exists t2");
    test_exec(hndl, "create table t2 (a int, b char(1))");
    test_exec(hndl, "insert into t2 select value, cast(value%10 as char) from generate_series(0,2999)");

    
    {
        int N = 2048;
        int values[N];
        for (int i = 0; i < N; i++) {
            values[i] = i;
        }
        cdb2_bind_array(hndl, "myarray", CDB2_INTEGER, (const void *)values, N, sizeof(values[0]));
        test_exec(hndl, "select a,b from t2 where a in carray(@myarray) order by a");
        for (int i = 0; i < N; i++) {
            test_next_record(hndl);
            int64_t *vala = cdb2_column_value(hndl, 0);
            if (*vala != values[i]) {
                fprintf(stderr, "%s:%d:error got:%"PRId64" expected:%d\n", __func__, __LINE__, *vala, values[i]);
                exit(1);
            }

            char *valb = cdb2_column_value(hndl, 1);
            if (*valb != ('0'+values[i]%10)) {
                fprintf(stderr, "%s:%d:error got:'%c' expected:'%d'\n", __func__, __LINE__, *valb, values[i]);
                exit(1);
            }

            //printf("TEST 06 RESP %"PRId64" %c\n", *vala, *valb);
        }

        cdb2_clearbindings(hndl);
    }

    {  // use bind to insert 
        test_exec(hndl, "drop table if exists t3");
        test_exec(hndl, "create table t3 (a int)");

        int N = 2048;
        int values[N];
        for (int i = 0; i < N; i++) {
            values[i] = i + 123;
        }
        cdb2_bind_array(hndl, "myintarr", CDB2_INTEGER, (const void *)values, N, sizeof(values[0]));
        test_exec(hndl, "insert into t3 select * from carray(@myintarr)");
        cdb2_clearbindings(hndl);

        // verify insert succeeded
        test_exec(hndl, "select a from t3 order by a");
        for (int i = 0; i < N; i++) {
            test_next_record(hndl);
            int64_t *vala = cdb2_column_value(hndl, 0);
            if (*vala != values[i]) {
                fprintf(stderr, "%s:%d:error got:%"PRId64" expected:%d\n", __func__, __LINE__, *vala, values[i]);
                exit(1);
            }
        }
    }
    /* Close the handle */
    test_close(hndl);
}

static void test_bind_blob_array()
{
    cdb2_hndl_tp *hndl = NULL;
    test_open(&hndl, db);
    const int N = INT16_MAX;
    struct {
        size_t len;
        void *data;
    } blobs[N];
    uint8_t db[N];
    memset(db, 0xdb, sizeof(db));
    for (int i = 0, j = N - 1; i < N; ++i, --j) {
        /* desc order so first blob is not zero-length (which server return as string) */
        blobs[i].len = j;
        blobs[i].data = db;
    }
    cdb2_bind_array(hndl, "blobs", CDB2_BLOB, blobs, N, 0);
    test_exec(hndl, "select * from carray(@blobs)");
    int rc , i = -1, j = N;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        ++i;
        --j;
        if (cdb2_column_type(hndl, 0) != CDB2_BLOB) {
            fprintf(stderr, "row:%d bad cdb2_column_type:%d expected:%d\n", i, cdb2_column_type(hndl, i), CDB2_BLOB);
            abort();
        }
        if (cdb2_column_size(hndl, 0) != j) {
            fprintf(stderr, "row:%d bad cdb2_column_size:%d expected:%d\n", i, cdb2_column_size(hndl, i), i);
            abort();
        }
        if (memcmp(db, cdb2_column_value(hndl, 0), j) != 0) {
            fprintf(stderr, "row:%d bad cdb2_column_value of size:%d\n", i, i);
            abort();
        }
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "cdb2_next_record rc:%d err:%s\n", rc, cdb2_errstr(hndl));
        abort();
    }
    cdb2_clearbindings(hndl);
    test_close(hndl);
}

static void create_procedure_sp_array(void)
{
    int rc;
    cdb2_hndl_tp *hndl = NULL;
    test_open(&hndl, db);
    char *sp_array = "\
create procedure sp_array version 'test' {    \
local function main(i32s, i64s, ds, ss, bs) \n\
    if #i32s ~= #i64s then return -201 end  \n\
    if #i32s ~= #ds then return -202 end    \n\
    if #i32s ~= #ss then return -203 end    \n\
    if #i32s ~= #bs then return -204 end    \n\
                                            \n\
    db:num_columns(5)                       \n\
                                            \n\
    db:column_name('i32s', 1)               \n\
    db:column_name('i64s', 2)               \n\
    db:column_name('ds', 3)                 \n\
    db:column_name('ss', 4)                 \n\
    db:column_name('bs', 5)                 \n\
                                            \n\
    db:column_type('int', 1)                \n\
    db:column_type('int', 2)                \n\
    db:column_type('double', 3)             \n\
    db:column_type('text', 4)               \n\
    db:column_type('blob', 5)               \n\
                                            \n\
    db:setmaxinstructions(#i32s * 10)       \n\
                                            \n\
    for i = 1, #i32s do                     \n\
     db:emit(                               \n\
      i32s[i], i64s[i], ds[i], ss[i], bs[i] \n\
     )                                      \n\
    end                                     \n\
end}";
    if ((rc = cdb2_run_statement(hndl, sp_array)) != 0) {
        fprintf(stderr, "cdb2_run_statement failed rc:%d err:%s\n", rc, cdb2_errstr(hndl));
        abort();
    }
    test_close(hndl);
}

static void test_pass_array_to_sp()
{
    create_procedure_sp_array();

    cdb2_hndl_tp *hndl = NULL;
    test_open(&hndl, db);


    const int N = 10240;
    int32_t i32s[N];
    int64_t i64s[N];
    double ds[N];
    char *ss[N];
    struct {
        size_t len;
        void *data;
    } bs[N];
    uint8_t blob[N];
    memset(blob, 0xdb, sizeof(blob));
    for (int i = 0; i < N; ++i) {
        i32s[i] = i;
        i64s[i] = i * 10;
        ds[i] = i * 100;
        char s[64]; sprintf(s, "%zd", i64s[i] * 100);
        ss[i] = strdup(s);
        bs[i].len = i;
        bs[i].data = blob;
    }
    cdb2_bind_array(hndl, "i32s", CDB2_INTEGER, i32s, N, sizeof(i32s[0]));
    cdb2_bind_array(hndl, "i64s", CDB2_INTEGER, i64s, N, sizeof(i64s[0]));
    cdb2_bind_array(hndl, "ds", CDB2_REAL, ds, N, 0);
    cdb2_bind_array(hndl, "ss", CDB2_CSTRING, ss, N, 0);
    cdb2_bind_array(hndl, "bs", CDB2_BLOB, bs, N, 0);
    test_exec(hndl, "exec procedure sp_array(@i32s, @i64s, @ds, @ss, @bs)");
    int rc, n = 0;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        for (int i = 0; i < cdb2_numcolumns(hndl); ++i) {
            switch (cdb2_column_type(hndl, i)) {
            case CDB2_INTEGER: {
                int64_t val = *(int64_t *)cdb2_column_value(hndl, i);
                if (i == 0) {
                    if (val != i32s[n]) {
                        fprintf(stderr, "row:%d i32 col:%d val:%zd vs exp:%d\n", n, i, val, i32s[n]);
                        abort();
                    }
                } else if (i == 1) {
                    if (val != i64s[n]) {
                        fprintf(stderr, "row:%d i64 col:%d %zd vs exp:%zd\n", n, i, val, i64s[n]);
                        abort();
                    }
                } else {
                    fprintf(stderr, "row:%d col:%d should not be integer\n", n, i);
                    abort();
                }
            }
            break;
            case CDB2_REAL: {
                double val = *(double *)cdb2_column_value(hndl, i);
                if (val != ds[n]) {
                    fprintf(stderr, "row:%d dbl col:%d val:%f vs exp:%f\n", n, i, val, ds[n]);
                    abort();
                }
            }
            break;
            case CDB2_CSTRING: {
                char *val = (char *)cdb2_column_value(hndl, i);
                if (strcmp(val, ss[n]) != 0) {
                    fprintf(stderr, "row:%d str col:%d val:'%s' vs exp:'%s'\n", n, i, val, ss[n]);
                    abort();
                }
            }
            break;
            case CDB2_BLOB: {
                void *val = cdb2_column_value(hndl, i);
                if (cdb2_column_size(hndl, i) != bs[n].len) {
                    fprintf(stderr, "row:%d blob col:%d len:%d vs exp:%zu\n", n, i, cdb2_column_size(hndl, i), bs[n].len);
                    abort();
                }
                if ((rc = memcmp(val, bs[n].data, bs[n].len)) != 0) {
                    fprintf(stderr, "row:%d blob col:%d len:%d memcmp:%d\n", n, i, cdb2_column_size(hndl, i), rc);
                    abort();
                }
            }
            break;
            default:
            fprintf(stderr, "row:%d col:%d unexpected type:%d\n", n, i, cdb2_column_type(hndl, i));
            abort();
            }
        }
        ++n;
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "cdb2_next_record failed rc:%d err:%s\n", rc, cdb2_errstr(hndl));
        abort();
    }
    if (n != N) {
        fprintf(stderr, "received rows:%d exp:%d\n", n, N);
        abort();
    }
    cdb2_clearbindings(hndl);
    test_close(hndl);
}

static void create_procedure_sp_carray(void)
{
    int rc;
    cdb2_hndl_tp *hndl = NULL;
    test_open(&hndl, db);
    char *sp_carray = "\
create procedure sp_carray version 'test' {                   \
local function main()                                       \n\
    local numbers = {0,1,2,3,4};                            \n\
    local integers = {}                                     \n\
    for i = 5, 10 do                                        \n\
        table.insert(integers, db:cast(i, 'int'))           \n\
    end                                                     \n\
    local strings = {'hello', 'world'}                      \n\
    local blobs = {x'436f6d64623200', x'35393200'}          \n\
                                                            \n\
    db:column_type('text', 1)                               \n\
    local stmt = db:prepare('select * from carray(@arr)')   \n\
                                                            \n\
    stmt:bind('arr', numbers)                               \n\
    stmt:emit()                                             \n\
                                                            \n\
    stmt:bind('arr', integers)                              \n\
    stmt:emit()                                             \n\
                                                            \n\
    stmt:bind('arr', strings)                               \n\
    stmt:emit()                                             \n\
                                                            \n\
    stmt:bind('arr', blobs)                                 \n\
    stmt:emit()                                             \n\
end}";
    if ((rc = cdb2_run_statement(hndl, sp_carray)) != 0) {
        fprintf(stderr, "cdb2_run_statement failed rc:%d err:%s\n", rc, cdb2_errstr(hndl));
        abort();
    }
    test_close(hndl);
}

static void test_bind_array_in_sp(void)
{
    create_procedure_sp_carray();

    cdb2_hndl_tp *hndl = NULL;
    test_open(&hndl, db);

    test_exec(hndl, "exec procedure sp_carray()");
    char *exp[] = {
        "0.0", "1.0", "2.0", "3.0", "4.0", //array of doubles
        "5","6","7","8","9", "10",         //array of integers
        "hello", "world",                  //array of strings
        "Comdb2", "592"                    //array of blobs
    };
    int rc, n = 0;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        char *val = cdb2_column_value(hndl, 0);
        if (strcmp(val, exp[n]) != 0) {
            fprintf(stderr, "cdb2_next_record exp:%s val:%s\n", exp[n], val);
            abort();
        }
        ++n;
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "cdb2_next_record failed rc:%d err:%s\n", rc, cdb2_errstr(hndl));
        abort();
    }
}

int main(int argc, char *argv[])
{
    db = argv[1];

    test_01();
    test_02();
    test_03();
    test_04();
    test_bind_array();
    test_bind_array2();
    test_bind_blob_array();
    test_pass_array_to_sp();
    test_bind_array_in_sp();

    return 0;
}
