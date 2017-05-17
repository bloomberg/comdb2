#include <string.h>
#include <cdb2api.h>
static cdb2_hndl_tp *db = NULL;
static void run(const char *sql)
{
    int rc;
    rc = cdb2_run_statement(db, sql);
    if (rc)
        fprintf(stderr, "run %s failed with rc %d errmsg %s\n", sql, rc, cdb2_errstr(db));
    cdb2_next_record(db);
}
int main(int argc, char *argv[])
{
    char c[] = {'a', 'b', 'c', 'd', 'e', 0xff, 'f', 'g', 'h', 'i', 'j', 0 };
    cdb2_set_comdb2db_config(argv[1]);
    cdb2_open(&db, argv[2], "default", 0);
    run("drop table if exists t");
    run("create table t {schema{cstring c[32]}}");
    cdb2_bind_param(db, "c", CDB2_CSTRING, c, strlen(c));
    run("insert into t values(@c)");
    cdb2_close(db);
    return 0;
}
