#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <cdb2api.h>

static void *my_simple_hook(cdb2_hndl_tp *hndl, void *user_arg, int argc, void **argv)
{
    puts((char *)user_arg);
    return NULL;
}

static void *my_arg_hook(cdb2_hndl_tp *hndl, void *user_arg, int argc, void **argv)
{
    printf("SQL is %s\n", (char *)argv[0]);
    return NULL;
}

static void *my_overwrite_rc_hook(cdb2_hndl_tp *hndl, void *user_arg, int argc, void **argv)
{
    return (void*)(intptr_t)(-1);
}

static cdb2_event *init_once_event;

static void register_once(void)
{
    init_once_event = cdb2_register_event(NULL, CDB2_BEFORE_SEND_QUERY, 0, my_simple_hook, "HELLO", 0);
}

static int TEST_init_once_registration(const char *db, const char *tier)
{
    extern void (*cdb2_init)(void);
    int rc;
    cdb2_hndl_tp *hndl = NULL;

    cdb2_init = register_once;
    cdb2_open(&hndl, db, tier, 0);

    cdb2_run_statement(hndl, "SELECT 1");
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    cdb2_unregister_event(NULL, init_once_event);
    cdb2_close(hndl);
    if (rc != CDB2_OK_DONE)
        return 1;
    return 0;
}

static int TEST_simple_register_unregister(const char *db, const char *tier)
{
    int rc;
    cdb2_event *e1, *e2, *e3, *e4, *e5, *e6, *e7, *e8;
    cdb2_hndl_tp *hndl = NULL;

    e1 = cdb2_register_event(NULL, CDB2_BEFORE_SEND_QUERY, 0, my_simple_hook, "1", 0);
    e2 = cdb2_register_event(NULL, CDB2_BEFORE_SEND_QUERY, 0, my_simple_hook, "2", 0);
    e3 = cdb2_register_event(NULL, CDB2_BEFORE_SEND_QUERY, 0, my_simple_hook, "3", 0);
    e4 = cdb2_register_event(NULL, CDB2_BEFORE_SEND_QUERY, 0, my_simple_hook, "4", 0);
    cdb2_open(&hndl, db, tier, 0);
    e5 = cdb2_register_event(hndl, CDB2_BEFORE_SEND_QUERY, 0, my_simple_hook, "5", 0);
    e6 = cdb2_register_event(hndl, CDB2_BEFORE_SEND_QUERY, 0, my_simple_hook, "6", 0);
    e7 = cdb2_register_event(hndl, CDB2_BEFORE_SEND_QUERY, 0, my_simple_hook, "7", 0);
    e8 = cdb2_register_event(hndl, CDB2_BEFORE_SEND_QUERY, 0, my_simple_hook, "8", 0);

    cdb2_run_statement(hndl, "SELECT 1");
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    if (rc != CDB2_OK_DONE)
        return 1;

    puts("------");

    cdb2_unregister_event(NULL, e3);
    cdb2_run_statement(hndl, "SELECT 1");
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    if (rc != CDB2_OK_DONE)
        return 1;

    puts("------");

    cdb2_unregister_event(NULL, e1);
    cdb2_unregister_event(NULL, e2);
    cdb2_unregister_event(NULL, e4);
    cdb2_unregister_event(hndl, e6);
    cdb2_run_statement(hndl, "SELECT 1");
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    if (rc != CDB2_OK_DONE)
        return 1;

    puts("------");

    cdb2_unregister_event(hndl, e5);
    cdb2_unregister_event(hndl, e6);
    cdb2_unregister_event(hndl, e8);
    cdb2_run_statement(hndl, "SELECT 1");
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    if (rc != CDB2_OK_DONE)
        return 1;
    cdb2_close(hndl);
    return 0;
}

static int TEST_arg_events(const char *db, const char *tier)
{
    int rc;
    cdb2_event *e;
    cdb2_hndl_tp *hndl = NULL;

    cdb2_open(&hndl, db, tier, 0);
    e = cdb2_register_event(hndl, CDB2_BEFORE_SEND_QUERY, 0, my_arg_hook, NULL, 1, CDB2_SQL);
    cdb2_run_statement(hndl, "SELECT 1");
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    cdb2_close(hndl);
    if (rc != CDB2_OK_DONE)
        return 1;
    return 0;
}

static int TEST_modify_rc_event(const char *db, const char *tier)
{
    int rc;
    cdb2_event *e;
    cdb2_hndl_tp *hndl = NULL;

    e = cdb2_register_event(NULL, CDB2_BEFORE_PMUX, CDB2_OVERWRITE_RETURN_VALUE, my_overwrite_rc_hook, NULL, 0, NULL);
    cdb2_open(&hndl, db, tier, 0);
    rc = cdb2_run_statement(hndl, "SELECT 1");
    if (rc == 0)
        return 1;
    puts(cdb2_errstr(hndl));
    cdb2_unregister_event(NULL, e);
    rc = cdb2_run_statement(hndl, "SELECT 1");
    cdb2_register_event(hndl, CDB2_AFTER_READ_RECORD, CDB2_OVERWRITE_RETURN_VALUE, my_overwrite_rc_hook, NULL, 0, NULL);
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    if (rc == CDB2_OK_DONE)
        return 1;
    puts(cdb2_errstr(hndl));
    cdb2_close(hndl);
    return 0;
}

int main(int argc, char **argv)
{
    char *conf = getenv("CDB2_CONFIG");
    char *tier = "local";
    int rc;
    char *db = argv[1];

    puts("====== INIT ONCE REGISTRATION ======");
    rc = TEST_init_once_registration(db, tier);
    if (rc != 0)
        return rc;

    if (conf != NULL) {
        cdb2_set_comdb2db_config(conf);
        tier = "default";
    }

    puts("====== SIMPLE REGISTRATION AND UNREGISTRATION ======");
    rc = TEST_simple_register_unregister(db, tier);
    if (rc != 0)
        return rc;

    puts("====== EVENT WITH ADDITIONAL INFORMATION ======");
    rc = TEST_arg_events(db, tier);
    if (rc != 0)
        return rc;

    puts("====== EVENT THAT INTERCEPTS AND OVERWRITES THE RETURN VALUE ======");
    rc = TEST_modify_rc_event(db, tier);
    if (rc != 0)
        return rc;
    return 0;
}
