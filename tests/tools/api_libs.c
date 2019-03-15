#include <stdio.h>

#define CDB2_INSTALL_LIBS gbl_init_once
#define CDB2_UNINSTALL_LIBS gbl_uninit
#define WITH_DL_LIBS 1
#include <cdb2api.c>

static void *my_open_hook(cdb2_hndl_tp *hndl, void *user_arg, int argc, void **argv)
{
    puts("CLOSING A HANDLE");
    return NULL;
}

static cdb2_event *e;

void gbl_init_once(void)
{
    puts("HELLO WORLD");
    e = cdb2_register_event(NULL, CDB2_AT_CLOSE, 0, my_open_hook, NULL, 0);
}

void gbl_uninit(void)
{
    puts("UNINSTALLING LIBS");
    cdb2_unregister_event(NULL, e);
}

int main(int argc, char **argv)
{
    char *conf = getenv("CDB2_CONFIG");
    char *tier = "local";
    char *db = argv[1];
    cdb2_hndl_tp *hndl = NULL;

    if (conf != NULL) {
        cdb2_set_comdb2db_config(conf);
        tier = "default";
    }
    cdb2_open(&hndl, db, tier, 0);
    cdb2_run_statement(hndl, "SELECT 1");
    cdb2_open(&hndl, db, tier, 0);
    cdb2_run_statement(hndl, "SELECT 1");

    cdb2_open(&hndl, "dummy", "localhost", CDB2_DIRECT_CPU);
    cdb2_close(hndl);
    cdb2_set_comdb2db_info("comdb2_config:uninstall_static_libs");
    cdb2_open(&hndl, "dummy", "localhost", CDB2_DIRECT_CPU);
    cdb2_close(hndl);
    return 0;
}
