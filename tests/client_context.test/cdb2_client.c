#include <cdb2api.h>
#include <stdio.h>
#include <string.h>

void fail(const char *msg)
{
    fprintf(stderr, "%s\n", msg);
    exit(1);
}

void likely(int rc, int expected, char *msg)
{
    if (rc != expected) fail(msg);
}

void opendb(cdb2_hndl_tp **hndl, const char *db, const char *type)
{
    int ret;

    char *conf = getenv("CDB2_CONFIG");
    if (conf) cdb2_set_comdb2db_config(conf);

    ret = cdb2_open(hndl, db, type, 0);

    if (ret != 0) {
        fail("cdb2_open");
    }
}

void exec(cdb2_hndl_tp *hndl, const char *sql)
{
    int ret;
    int coltype;

    printf("%s\n", sql);

    ret = cdb2_run_statement(hndl, sql);

    if (ret != 0) {
        fail("cdb2_run_statement");
    }

    while (1) {
        ret = cdb2_next_record(hndl);
        switch (ret) {
        case CDB2_OK:
            printf("  ");
            coltype = cdb2_column_type(hndl, 0);
            switch (coltype) {
            case CDB2_INTEGER:
                printf("%d\n", *(int *)cdb2_column_value(hndl, 0));
                break;
            case CDB2_CSTRING:
                printf("%s\n", (const char *)cdb2_column_value(hndl, 0));
                break;
            default: fail("cdb2_run_statement");
            }
            break;
        case CDB2_OK_DONE: return;
        default: fail("cdb2_run_statement");
        }
    }

    return;
}

int main(int argc, char **argv)
{
    int i;
    char buf[120];

    if (argc != 2) {
        printf("Invalid number of args\n");
        exit(1);
    }

    cdb2_hndl_tp *hndl = NULL;

    opendb(&hndl, argv[1], "local");

    /* Run a command without any client context message. */
    exec(hndl, "SELECT 1");

    /* Push a context message. */
    likely(cdb2_push_context(hndl, "1"), 0, "can't push context");
    exec(hndl, "SELECT 1");

    /* Fill the stack buffer. */
    for (i = 2; i <= 10; i++) {
        sprintf(buf, "%d", i);
        likely(cdb2_push_context(hndl, buf), 0, "can't push context (2)");
    }
    exec(hndl, "SELECT 1");

    /* Check stack buffer overflow. */
    likely(cdb2_push_context(hndl, "MUST NOT BE LOGGED!"), 1, "push didn't fail");
    exec(hndl, "SELECT 1");

    /* Remove the message at the top. */
    likely(cdb2_pop_context(hndl), 0, "can't pop");
    exec(hndl, "SELECT 1");

    /* Clear all context messages. */
    likely(cdb2_clear_contexts(hndl), 0, "can't clear");

    /* Check stack buffer underflow. */
    likely(cdb2_push_context(hndl, "1"), 0, "can't push contest (3)");
    likely(cdb2_pop_context(hndl), 0, "can't pop (2)");
    likely(cdb2_pop_context(hndl), 1, "pop didn't fail");
    exec(hndl, "SELECT 1");

    /* Test a long message. */
    memset(buf, '#', sizeof(buf));
    buf[sizeof(buf) - 1] = 0;
    likely(cdb2_push_context(hndl, buf), 0, "can't push (4)");
    exec(hndl, "SELECT 1");

    cdb2_close(hndl);
    return 0;
}
