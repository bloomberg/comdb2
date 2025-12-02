#undef NDEBUG
#include <assert.h>
#include <cstdio>
#include <signal.h>
#include <string.h>

#include <cdb2api.h>
#include <iostream>
static void test(int argc, char **argv, int flag)
{
    int rc;
    cdb2_hndl_tp *hndl = NULL;

    rc = cdb2_open(&hndl, argv[1], argv[2], flag);
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "select comdb2_dbname()");
    if(strcmp(argv[2],"localhost")==0){
        if (rc != 0) {
            fprintf(stderr, "cdb2_run_statement failed rc=%d %s\n", rc, cdb2_errstr(hndl));
            abort();
        }
        rc = cdb2_next_record(hndl);
        assert(rc == CDB2_OK);

        const char *dbname = (char *)cdb2_column_value(hndl, 0);
        assert(strcmp(dbname, argv[1]) == 0);

        rc = cdb2_next_record(hndl);
        assert(rc == CDB2_OK_DONE);
    }
    else
        assert(rc != 0);

    rc = cdb2_close(hndl);
    assert(rc == 0);
}


int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);
        
    test(argc, argv, CDB2_ADMIN|CDB2_DIRECT_CPU); 
}
