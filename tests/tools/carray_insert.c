#include <stdlib.h>
#include <libgen.h>
#include <string.h>

#include <cdb2api.h>

void post_run(cdb2_hndl_tp *db, char *file, int index)
{
    int rc;
    while ((rc = cdb2_next_record(db)) == CDB2_OK)
        ;
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "%s:%d cdb2_next_record rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    if ((rc = cdb2_clearbindings(db)) != 0) {
        fprintf(stderr, "%s:%d cdb2_clearbindings rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    cdb2_effects_tp effects;
    if ((rc = cdb2_get_effects(db, &effects)) != 0) {
        fprintf(stderr, "%s:%d cdb2_get_effects rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    char *sql = index ? "select count(*) cnt from carray2" : "select count(*) cnt from carray1";
    if ((rc = cdb2_run_statement(db, sql)) != 0) {
        fprintf(stderr, "%s:%d cdb2_run_statement rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    if ((rc = cdb2_next_record(db)) != CDB2_OK) {
        fprintf(stderr, "%s:%d cdb2_next_record rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    int cnt = *(int64_t *)cdb2_column_value(db, 0);

    if ((rc = cdb2_next_record(db)) != CDB2_OK_DONE) {
        fprintf(stderr, "%s:%d cdb2_next_record rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    printf("%s:%d num_inserted:%d, count is:%d\n", file, __LINE__, effects.num_inserted, cnt);

    if (effects.num_inserted == 5 && cnt == 5) {
        printf("%s:%d pass%s\n", file, __LINE__, index ? " index" : "");
    } else {
        fprintf(stderr, "%s:%d failed%s\n", file, __LINE__, index ? " index" : "");
        abort();
    }
}

int main(int argc, char *argv[])
{
    char *file = basename(__FILE__);
    cdb2_hndl_tp *db;
    char *conf = getenv("CDB2_CONFIG");
    if (conf) cdb2_set_comdb2db_config(conf);
    char *host = getenv("CDB2_HOST");
    int rc = host ? cdb2_open(&db, argv[1], host, CDB2_DIRECT_CPU)
                  : cdb2_open(&db, argv[1], "default", 0);
    if (rc) {
        fprintf(stderr, "%s:%d cdb2_open rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    int a[] = {0, 1, 2, 3, 4};
    if ((rc = cdb2_bind_array(db, "a", CDB2_INTEGER, a, sizeof(a) / sizeof(a[0]), sizeof(a[0]))) != 0) {
        fprintf(stderr, "%s:%d cdb2_bind_array rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    double b[] = {0, 10, 20, 30, 40};
    if ((rc = cdb2_bind_array(db, "b", CDB2_REAL, b, sizeof(b) / sizeof(b[0]), 0)) != 0) {
        fprintf(stderr, "%s:%d cdb2_bind_array rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    char *c[] = {"592", "first", "second", "hello", "world"};
    if ((rc = cdb2_bind_array(db, "c", CDB2_CSTRING, c, sizeof(c) / sizeof(c[0]), 0)) != 0) {
        fprintf(stderr, "%s:%d cdb2_bind_array rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    int64_t d[] = {0, 1000, 2000, 3000, 4000};
    if ((rc = cdb2_bind_array(db, "d", CDB2_INTEGER, d, sizeof(d) / sizeof(d[0]), sizeof(d[0]))) != 0) {
        fprintf(stderr, "%s:%d cdb2_bind_array rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    struct {
        size_t len;
        void * data;
    } e[sizeof(c) / sizeof(c[0])];
    for (int i = 0; i < (sizeof(e) / sizeof(e[0])); ++i) {
        e[i].data = c[i];
        e[i].len = strlen(c[i]) + 1;
    }
    if ((rc = cdb2_bind_array(db, "e", CDB2_BLOB, e, sizeof(e) / sizeof(e[0]), 0)) != 0) {
        fprintf(stderr, "%s:%d cdb2_bind_array rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    rc = cdb2_run_statement(db, "with a as (select rowid, value aa from carray(@a)), "
                                   "b as (select rowid, value bb from carray(@b)), "
                                   "c as (select rowid, value cc from carray(@c)), "
                                   "d as (select rowid, value dd from carray(@d)), "
                                   "e as (select rowid, value ee from carray(@e)) "
                                "insert into carray1 select aa, bb, cc, dd, ee "
                                   "from a "
                                   "join b on b.rowid = a.rowid "
                                   "join c on c.rowid = a.rowid "
                                   "join d on d.rowid = a.rowid "
                                   "join e on e.rowid = a.rowid "
    );

    if (rc != 0) {
        fprintf(stderr, "%s:%d cdb2_run_statement rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    post_run(db, file, 0);

    if ((rc = cdb2_bind_array_index(db, 1, CDB2_INTEGER, a, sizeof(a) / sizeof(a[0]), sizeof(a[0]))) != 0) {
        fprintf(stderr, "%s:%d cdb2_bind_array_index rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    if ((rc = cdb2_bind_array_index(db, 2, CDB2_REAL, b, sizeof(b) / sizeof(b[0]), 0)) != 0) {
        fprintf(stderr, "%s:%d cdb2_bind_array_index rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    if ((rc = cdb2_bind_array_index(db, 3, CDB2_CSTRING, c, sizeof(c) / sizeof(c[0]), 0)) != 0) {
        fprintf(stderr, "%s:%d cdb2_bind_array_index rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    if ((rc = cdb2_bind_array_index(db, 4, CDB2_INTEGER, d, sizeof(d) / sizeof(d[0]), sizeof(d[0]))) != 0) {
        fprintf(stderr, "%s:%d cdb2_bind_array_index rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    if ((rc = cdb2_bind_array_index(db, 5, CDB2_BLOB, e, sizeof(e) / sizeof(e[0]), 0)) != 0) {
        fprintf(stderr, "%s:%d cdb2_bind_array_index rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    rc = cdb2_run_statement(db, "with a as (select rowid, value aa from carray(?)), "
                                   "b as (select rowid, value bb from carray(?)), "
                                   "c as (select rowid, value cc from carray(?)), "
                                   "d as (select rowid, value dd from carray(?)), "
                                   "e as (select rowid, value ee from carray(?)) "
                                "insert into carray2 select aa, bb, cc, dd, ee "
                                   "from a "
                                   "join b on b.rowid = a.rowid "
                                   "join c on c.rowid = a.rowid "
                                   "join d on d.rowid = a.rowid "
                                   "join e on e.rowid = a.rowid "
    );

    if (rc != 0) {
        fprintf(stderr, "%s:%d cdb2_run_statement rc=%d:%s\n", file, __LINE__, rc, cdb2_errstr(db));
        abort();
    }

    post_run(db, file, 1);

    cdb2_close(db);
    return 0;
}
