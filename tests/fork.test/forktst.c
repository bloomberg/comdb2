#include <cdb2api.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <errno.h>
#include <sys/wait.h>
#include <stdlib.h>


int main(int argc, char **argv)
{
    char *data;
    int rc, i;

    pid_t child;
    cdb2_hndl_tp *hndl = NULL;

    if (argc < 2) {
        fprintf(stderr, "Usage: <executable> <# inserts> <dbname> [ [node] | [type cfg_file] ]\n");
        exit(1);
    }

    cdb2_init_ssl(1, 1);

    int invalidate_parent = 0;
    if (argc == 5)
        invalidate_parent = CDB2_INVALIDATE_PARENT_FORK;

    if (argc == 2)
        rc = cdb2_open(&hndl, argv[1], "local", invalidate_parent);
    else if(argc <= 5) {
        cdb2_set_comdb2db_config(argv[3]);
        rc = cdb2_open(&hndl, argv[1], argv[2], invalidate_parent);
    }
    else
        rc = cdb2_open(&hndl, argv[1], argv[2], CDB2_DIRECT_CPU | invalidate_parent);

    if (rc) {
        fprintf(stderr, "failed to open handle\n");
        exit(1);
    }

    rc = cdb2_run_statement(hndl, "insert into t1 values (1)");
    if (rc) {
        fprintf(stderr, "INSERT failed rc = %d, error = %s\n", rc, cdb2_errstr(hndl));
        exit(1);
    }

    const char *sql = "select * from t1 order by i";
    rc = cdb2_run_statement(hndl, sql);
    if (rc) {
        fprintf(stderr, "sql failed rc = %d, error = %s\n", rc, cdb2_errstr(hndl));
        exit(1);
    }


    child = fork();
    if (child < 0)
        return 1;

    const char *process_str;
    if (child == 0) { //it is child process
        process_str = "CHILD";
    }
    else
        process_str = "PARENT";

    //next for select *
    rc = cdb2_next_record(hndl);
    while (rc == CDB2_OK) {
        int n = cdb2_numcolumns(hndl);
        int i;
        const char *c = process_str;
        for (i = 0; i < n; ++i) {
            printf("%s %d", c, *(int *)cdb2_column_value(hndl, i));
            c = ", ";
        }
        puts("");
        rc = cdb2_next_record(hndl);
    }

    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "cdb2_next_record failed for %s: rc = %d\n", process_str, rc);
    }

    rc = cdb2_run_statement(hndl, "insert into t1 values (2)");
    if (rc) {
        fprintf(stderr, "INSERT failed for %s: rc = %d\n", process_str, rc);
    }
    cdb2_close(hndl);

    return 0;
}
