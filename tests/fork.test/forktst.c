#include <cdb2api.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <errno.h>
#include <sys/wait.h>
#include <stdlib.h>

#define PARENTPARENT 0x0000
#define CHILDPARENT 0x0010
#define PARENTCHILD 0x0001
#define CHILDCHILD 0x0011

void usage() {
    fprintf(stderr, "Usage: <executable> <dbname> [ [node] | [type cfg_file] invalidation ]\n");
    exit(1);
}

int main(int argc, char **argv)
{
    char *data;
    int rc, i;

    if (argc < 2) {
        usage();
    }

    cdb2_init_ssl(1, 1);

    /* if fifth parameter is set we will transfer ownership of the db 
     * handle to child process thus in effect invalidate hndl on parent */
    int invalidation = 0;
    if (argc >= 5) {
        invalidation = atoi(argv[4]); //has to be one of 0-4
        //fprintf(stderr, "invalidation of %s, %d\n", argv[4], invalidation);
        if (invalidation < 0 || invalidation > 4) {
            fprintf(stderr, "bad invalidation %s\n", argv[4]);
            usage();
        }
    }

    cdb2_hndl_tp *hndl = NULL;
    cdb2_hndl_tp *hndl2 = NULL;
    const char *process_str = NULL;

    int fork_generation = 0;
    {
        pid_t child;

        int invalidate_parent = (invalidation & (1 << fork_generation) ) ? CDB2_INVALIDATE_PARENT_FORK : 0;
        //fprintf("will invalidate %d\n", invalidate_parent);
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
            fprintf(stderr, "1: INSERT failed rc = %d, error = %s\n", rc, cdb2_errstr(hndl));
            exit(1);
        }

        const char *sql = "select * from t1 order by i";
        rc = cdb2_run_statement(hndl, sql);
        if (rc) {
            fprintf(stderr, "1: sql failed rc = %d, error = %s\n", rc, cdb2_errstr(hndl));
            exit(1);
        }


        child = fork();
        if (child < 0)
            return 1;
        fork_generation++;

        if (child == 0) { //it is child process
            process_str = "CHILD";
        }
        else
            process_str = "PARENT";

        //next for select *
        rc = cdb2_next_record(hndl);
        while (rc == CDB2_OK) {
            /*
            int n = cdb2_numcolumns(hndl);
            int i;
            printf("1 %s:", process_str);
            const char *c = "";
            for (i = 0; i < n; ++i) {
                printf("%s %d", c, *(int *)cdb2_column_value(hndl, i));
                c = ", ";
            }
            puts("");
            */
            rc = cdb2_next_record(hndl);
        }

        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "1 %s: cdb2_next_record failed rc = %d\n", process_str, rc);
        }

        rc = cdb2_run_statement(hndl, "insert into t1 values (2)");
        if (rc) {
            fprintf(stderr, "1 %s: INSERT 2 failed rc = %d\n", process_str, rc);
        }
        else
            fprintf(stderr, "1 %s: INSERT 2 success\n", process_str);

    }

    {
        int invalidate_parent = (invalidation & (1 << fork_generation) ) ? CDB2_INVALIDATE_PARENT_FORK : 0;
        //printf("will invalidate %d\n", invalidate_parent);
        if (argc == 2)
            rc = cdb2_open(&hndl2, argv[1], "local", invalidate_parent);
        else if(argc <= 5) {
            cdb2_set_comdb2db_config(argv[3]);
            rc = cdb2_open(&hndl2, argv[1], argv[2], invalidate_parent);
        }
        else
            rc = cdb2_open(&hndl2, argv[1], argv[2], CDB2_DIRECT_CPU | invalidate_parent);

        if (rc) {
            fprintf(stderr, "failed to open handle\n");
            exit(1);
        }


        rc = cdb2_run_statement(hndl2, "insert into t2 values (3)");
        if (rc) {
            fprintf(stderr, "2: INSERT failed rc = %d, error = %s\n", rc, cdb2_errstr(hndl2));
            exit(1);
        }

        const char *sql = "select * from t2 order by j";
        rc = cdb2_run_statement(hndl2, sql);
        if (rc) {
            fprintf(stderr, "2: sql failed rc = %d, error = %s\n", rc, cdb2_errstr(hndl2));
            exit(1);
        }

        pid_t child2 = fork();
        if (child2 < 0)
            return 1;

        fork_generation++;
        char process_str2[128];
        if (child2 == 0) { //it is child process
            snprintf(process_str2, sizeof(process_str2), "CHILD of %s", process_str);
        }
        else
            snprintf(process_str2, sizeof(process_str2), "PARENT of %s", process_str);

        //next for select *
        rc = cdb2_next_record(hndl2);
        while (rc == CDB2_OK) {
            /*
            int n = cdb2_numcolumns(hndl2);
            int i;
            printf("2 %s:", process_str2);
            const char *c = "";
            for (i = 0; i < n; ++i) {
                printf("%s %d", c, *(int *)cdb2_column_value(hndl2, i));
                c = ", ";
            }
            puts("");
            */
            rc = cdb2_next_record(hndl2);
        }

        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "2 %s: cdb2_next_record failed rc = %d\n", process_str2, rc);
        }

        rc = cdb2_run_statement(hndl2, "insert into t2 values (4)");
        if (rc) {
            fprintf(stderr, "2 %s: INSERT 4 failed rc = %d\n", process_str2, rc);
        }
        else
            fprintf(stderr, "2 %s: INSERT 4 success\n", process_str2);

        cdb2_close(hndl2);


        // work with first handle again
        rc = cdb2_run_statement(hndl, "insert into t1 values (5)");
        if (rc) {
            fprintf(stderr, "3 %s: INSERT 5 failed rc = %d\n", process_str2, rc);
        }
        else
            fprintf(stderr, "3 %s: INSERT 5 success\n", process_str2);
    }
    cdb2_close(hndl);

    return 0;
}
