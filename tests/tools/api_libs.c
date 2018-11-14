#include <stdio.h>

#define CDB2_INIT gbl_init_once
#define WITH_DL_LIBS 1
#include <cdb2api.c>

static void gbl_init_once(void)
{
    puts("HELLO WORLD");
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
    return 0;
}
