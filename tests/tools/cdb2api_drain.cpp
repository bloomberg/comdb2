#include <signal.h>
#include <stdio.h>
#include <cdb2api.h>

const char *create_proc = "\
create procedure foo version 'bar' {\
local function main()\n\
    local c = db:consumer()\n\
    local e = c:poll(0)\n\
    while e do\n\
        c:emit(e.new.i)\n\
        c:consume()\n\
        e = c:poll(0)\n\
    end\n\
end}";

static cdb2_hndl_tp *hndl;

static void run(const char *stmt, int fatal)
{
    int rc = cdb2_run_statement(hndl, stmt);
    if (rc) {
        fprintf(stderr, "cdb2_run_statement rc:%d err:%s stmt:%s\n", rc, cdb2_errstr(hndl), stmt);
        if (fatal) abort();
        return;
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        ;
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "cdb2_next_record rc:%d err:%s stmt:%s\n", rc, cdb2_errstr(hndl), stmt);
        if (fatal) abort();
    }
}

#define run_stmt(stmt) run(stmt, 1)
#define try_stmt(stmt) run(stmt, 0)

static void setup(void)
{
    run_stmt("drop table if exists t");
    run_stmt("create table t(i int)");
    run_stmt(create_proc);
    run_stmt("put default procedure foo 'bar'");
    try_stmt("drop lua consumer foo");
    run_stmt("create lua consumer foo on (table t for insert)");
    run_stmt("insert into t values(111)");
    run_stmt("insert into t values(222)");
    run_stmt("insert into t values(333)");
}

#define have_event(expected)                                                   \
    {                                                                          \
        int rc;                                                                \
        if ((rc = cdb2_next_record(hndl)) != CDB2_OK) {                        \
            fprintf(stderr, "%s:%d cdb2_next_record rc:%d err:%s\n", __func__, \
                    __LINE__, rc, cdb2_errstr(hndl));                          \
            abort();                                                           \
        }                                                                      \
        int have = *(int64_t *)cdb2_column_value(hndl, 0);                     \
        if (have != expected) {                                                \
            fprintf(stderr, "%s:%d missing:%d got:%d\n", __func__, __LINE__,   \
                    expected, have);                                           \
            abort();                                                           \
        }                                                                      \
    }

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    char *db = argv[1];
    char *tier = argv[2];
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    cdb2_open(&hndl, db, tier, 0);
    setup();

    // Should not consume event
    cdb2_run_statement(hndl, "exec procedure foo()");
    have_event(111);

    cdb2_run_statement(hndl, "exec procedure foo()");
    have_event(111);

    cdb2_run_statement(hndl, "exec procedure foo()");
    cdb2_run_statement(hndl, "exec procedure foo()");
    have_event(111);

    cdb2_run_statement(hndl, "exec procedure foo()");
    cdb2_clear_ack(hndl); // Should not consume event
    cdb2_close(hndl);

    // Check event is still there
    cdb2_open(&hndl, db, tier, 0);
    cdb2_run_statement(hndl, "exec procedure foo()");
    have_event(111);
    cdb2_clear_ack(hndl); // Should not consume event
    cdb2_close(hndl);

    // Check event is still there
    cdb2_open(&hndl, db, tier, 0);
    cdb2_run_statement(hndl, "exec procedure foo()");
    have_event(111);
    cdb2_close(hndl); // Consume 111 on close

    cdb2_open(&hndl, db, tier, 0);
    cdb2_run_statement(hndl, "exec procedure foo()");
    have_event(222);
    have_event(333);
    int rc  = cdb2_next_record(hndl); //Consume last event 333
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "%s:%d did not consume all events\n", __func__, __LINE__);
        abort();
    }
    cdb2_close(hndl);
    return 0;
}
