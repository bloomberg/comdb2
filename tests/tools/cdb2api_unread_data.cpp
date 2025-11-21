#include <signal.h>
#include <stdio.h>
#include <cdb2api.h>
#include <cdb2api_test.h>

int test_unread_record(char *dbname, char *type)
{
    int rc = 0;

    cdb2_hndl_tp *cdb2h = NULL;
    int num_connects = get_num_tcp_connects();
    num_connects++; // Expect atleast 1 connect

    for (int i =0; i < 1000; i++) {
        rc = cdb2_open(&cdb2h, dbname, type, 0);
        if (rc != CDB2_OK) {
            fprintf(stderr, "%s Failed to open db %s rc=%d errstr=%s\n", __func__, dbname, rc, cdb2_errstr(cdb2h));
            return -1;
        }
        rc = cdb2_run_statement(cdb2h, "select 1");
        if (rc != CDB2_OK) {
            fprintf(stderr, "%s Failed to run statement rc=%d errstr=%s\n", __func__, rc, cdb2_errstr(cdb2h));
            return -1;
        }
        rc = cdb2_next_record (cdb2h);
        if (rc != CDB2_OK) {
            fprintf(stderr, "%s Failed to get record rc=%d errstr=%s\n", __func__, rc, cdb2_errstr(cdb2h));
            return -1;
        }
        rc = cdb2_close(cdb2h);
        if (rc != CDB2_OK) {
            fprintf(stderr, "%s Failed to close db rc=%d errstr=%s\n", __func__, rc, cdb2_errstr(cdb2h));
            return -1;
        }
    }

    if (get_num_tcp_connects() > num_connects + 2) {
        fprintf(stderr, "%s Failed: expected connects: %d got: %d\n", __func__, num_connects, get_num_tcp_connects());
        return -1;
    }

    return 0;
}


int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    char *db = argv[1];
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    cdb2_enable_sockpool();

    int rc = test_unread_record(db, "default");
    
    return rc;
}
