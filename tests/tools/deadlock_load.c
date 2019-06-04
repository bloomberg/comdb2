#include <alloca.h>
#include <stdarg.h>
#include <strings.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stddef.h>
#include <poll.h>
#include <stdint.h>
#include <sys/time.h>
#include <string.h>
#include <ctype.h>
#include <signal.h>
#include <assert.h>
#include "testutil.h"
#include "nemesis.h"
#include <cdb2api.h>

static char *argv0;
char *dbname = NULL;
char *cltype = "default";
int iterations = 1000;
int numthds = 10;
int records = 600;
int debug_trace = 0;

void usage(FILE *f)
{
    fprintf(f, "Usage: %s [opts]\n", argv0);
    fprintf(f, "        -d <dbname>         - set name of the test database\n");
    fprintf(f, "        -T <numthd>         - set the number of threads\n");
    fprintf(f, "        -t <cltype>         - 'dev', 'alpha', 'beta', or 'prod'\n");
    fprintf(f, "        -i <iterations>     - run this many iterations\n");
    fprintf(f, "        -r <records>        - records per iteration\n");
    fprintf(f, "        -c <cdb2cfg>        - set cdb2cfg file\n");
    fprintf(f, "        -h                  - this menu\n");
    fprintf(f, "        -D                  - enable debug trace\n");
}

void *run_test(void *x)
{
    cdb2_hndl_tp *insert_hndl, *update_hndl;
    int64_t td = (int64_t)x;
    int rc;

    if ((rc = cdb2_open(&insert_hndl, dbname, cltype, 0)) != 0) {
        fprintf(stderr, "%s error opening insert handle to %s stage %s\n",
                __func__, dbname, cltype);
        exit(1);
    }

    if ((rc = cdb2_open(&update_hndl, dbname, cltype, 0)) != 0) {
        fprintf(stderr, "%s error opening update handle to %s stage %s\n",
                __func__, dbname, cltype);
        exit(1);
    }

    for (int i = 0; i < iterations; i++) {
        int64_t val = ((i * numthds) + td), upval = ((i * numthds) + td);
        for (int j = 0; j < records; j++) {
            cdb2_clearbindings(insert_hndl);
            rc = cdb2_bind_param(insert_hndl, "a", CDB2_INTEGER, &val, sizeof(val));
            assert(rc == 0);
            if ((rc = cdb2_run_statement(insert_hndl, "INSERT INTO T1 (a, b) "
                            "VALUES(@a, 0)")) != 0) {
                fprintf(stderr, "%s error inserting record, %d\n", __func__,
                        rc);
                exit(1);
            }
            while ((rc = cdb2_next_record(insert_hndl)) == CDB2_OK);
            assert(rc == CDB2_OK_DONE);

            cdb2_clearbindings(update_hndl);
            rc = cdb2_bind_param(update_hndl, "a", CDB2_INTEGER, &val, sizeof(val));
            assert(rc == 0);
            rc = cdb2_bind_param(update_hndl, "b", CDB2_INTEGER, &upval, sizeof(upval));
            assert(rc == 0);
            if ((rc = cdb2_run_statement(update_hndl, "UPDATE T1 SET b=@b WHERE a=@a")) != 0) {
                fprintf(stderr, "%s error updating record %"PRId64", %d\n", __func__,
                        val, rc);
                exit(1);
            }
            while ((rc = cdb2_next_record(update_hndl)) == CDB2_OK);
            assert(rc == CDB2_OK_DONE);
        }
    }

    cdb2_close(insert_hndl);
    cdb2_close(update_hndl);
    return NULL;
}

void drop_table(void)
{
    cdb2_hndl_tp *hndl;
    int rc;
    if ((rc = cdb2_open(&hndl, dbname, cltype, 0)) != 0) {
        fprintf(stderr, "%s error opening handle to %s stage %s\n",
                __func__, dbname, cltype);
        exit(1);
    }

    cdb2_run_statement(hndl, "DROP TABLE t1");
    cdb2_close(hndl);
}

void create_table(void)
{
    cdb2_hndl_tp *hndl;
    int rc;
    if ((rc = cdb2_open(&hndl, dbname, cltype, 0)) != 0) {
        fprintf(stderr, "%s error opening handle to %s stage %s\n",
                __func__, dbname, cltype);
        exit(1);
    }

    if ((rc = cdb2_run_statement(hndl, "CREATE TABLE t1(a INTEGER NULL, b INTEGER NULL)")) 
            != 0) {
        fprintf(stderr, "%s error creating table, %d\n",
                __func__, rc);
        exit(1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    if ((rc = cdb2_run_statement(hndl, "CREATE INDEX ix1 on t1(a)")) 
            != 0) {
        fprintf(stderr, "%s error creating index, %d\n",
                __func__, rc);
        exit(1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    if ((rc = cdb2_run_statement(hndl, "CREATE INDEX ix2 on t1(b)")) 
            != 0) {
        fprintf(stderr, "%s error creating index, %d\n",
                __func__, rc);
        exit(1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    cdb2_close(hndl);
}

int main(int argc, char *argv[])
{
    int c, err = 0, rc;
    pthread_t *thds = NULL;
    argv0 = argv[0];

    while ((c = getopt(argc, argv, "d:T:t:i:r:c:hD")) != EOF) {
        switch(c) {
            case 'd':
                dbname = optarg;
                break;
            case 'T':
                numthds = atoi(optarg);
                break;
            case 't':
                cltype = optarg;
                break;
            case 'r':
                records = atoi(optarg);
                break;
            case 'i':
                iterations = atoi(optarg);
                break;
            case 'c':
                cdb2_set_comdb2db_config(optarg);
                break;
            case 'h':
                usage(stdout);
                exit(0);
                break;
            case 'D':
                debug_trace = 1;
                break;
            default:
                fprintf(stderr, "Unknown option, '%c'\n", optopt);
                err++;
                break;
        }
    }

    if (err) {
        usage(stderr);
        exit(1);
    }

    srand(time(NULL));
    drop_table();
    create_table();

    thds = (pthread_t *)calloc(numthds, sizeof(pthread_t));
    for(int i = 0; i < numthds; i++) {
        int64_t x = (int64_t)i;
        rc = pthread_create(&thds[i], NULL, run_test, (void *)x);
        if (rc != 0) {
            fprintf(stderr, "pthread_create error, rc=%d errno=%d\n", rc,
                    errno);
            exit(1);
        }
    }

    for(int i = 0; i < numthds; i++) {
        pthread_join(thds[i], NULL);
    }
}
