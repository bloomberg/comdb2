#include <stdio.h>
#include <stdlib.h>
#include <cdb2api.h>

typedef void (lib_cb)(void);
void cdb2_set_install_libs(lib_cb *);
void cdb2_set_uninstall_libs(lib_cb *);

static void *my_open_hook(cdb2_hndl_tp *hndl, void *user_arg, int argc, void **argv)
{
    puts("CLOSING A HANDLE");
    return NULL;
}

static cdb2_event *e;

static int inited = 0;
void gbl_init_once(void)
{
    if (inited)
        return;

    puts("HELLO WORLD");
    e = cdb2_register_event(NULL, CDB2_AT_CLOSE, 0, my_open_hook, NULL, 0);
    inited = 1;
}

void gbl_uninit(void)
{
    if (!inited)
        return;

    puts("UNINSTALLING LIBS");
    cdb2_unregister_event(NULL, e);

    inited = 0;
}

int main(int argc, char **argv)
{
    cdb2_set_install_libs(gbl_init_once);
    cdb2_set_uninstall_libs(gbl_uninit);

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
    /* Uninstall */
    cdb2_set_comdb2db_info("comdb2_config:disable_static_libs");
    cdb2_open(&hndl, "dummy", "localhost", CDB2_DIRECT_CPU);
    cdb2_close(hndl);
    /* Install again */
    cdb2_set_comdb2db_info("comdb2_config:enable_static_libs");
    cdb2_open(&hndl, "dummy", "localhost", CDB2_DIRECT_CPU);
    cdb2_close(hndl);
    /* Uninstall again */
    cdb2_set_comdb2db_info("comdb2_config:disable_static_libs");
    cdb2_open(&hndl, "dummy", "localhost", CDB2_DIRECT_CPU);
    cdb2_close(hndl);
    return 0;
}
