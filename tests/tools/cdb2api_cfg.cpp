#undef NDEBUG
#include <assert.h>
#include <string.h>

#include <cdb2api.h>
#include <cdb2api_test.h>

int main(int argc, char **argv)
{
    if (argc != 5) {
        fprintf(stderr, "usage: cfg <dbname> <tier> <comdb2db.cfg> <dbname.cfg>\n");
        return -1;
    }

    set_fail_sockpool(-1);
    char *dbname = argv[1];
    char *type = argv[2];
    set_cdb2api_test_comdb2db_cfg(argv[3]);
    set_cdb2api_test_dbname_cfg(argv[4]);

    cdb2_hndl_tp *hndl = NULL;
    cdb2_open(&hndl, dbname, type, 0);
    printf("%s:%s\n", get_default_cluster(), get_default_cluster_hndl(hndl));
    cdb2_close(hndl);
    return 0;
}
