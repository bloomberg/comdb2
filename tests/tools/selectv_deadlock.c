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
int numthds = 10;
int verbose = 0;
int debug_trace = 0;
int limit = 2000;
int timelimit = 600;
int numrecs = 100000;
int contention = 0;
int stop_flag = 0;
int contention_interval = 60;
int do_insert = 0;
int do_create = 0;
int64_t updates_this_segment;
FILE *errlog = NULL;

void usage(FILE *f)
{
    fprintf(f, "Usage: %s [opts]\n", argv0);
    fprintf(f, "        -d <dbname>         - set name of the test database\n");
    fprintf(f, "        -T <numthd>         - set the number of threads\n");
    fprintf(f, "        -t <cltype>         - 'dev', 'alpha', 'beta', or 'prod'\n");
    fprintf(f, "        -I                  - do initial insert\n");
    fprintf(f, "        -M                  - create initial tables\n");
    fprintf(f, "        -m <timelimit>      - run test for this amount of time\n");
    fprintf(f, "        -l <limit>          - set selectv limit\n");
    fprintf(f, "        -R <initial-recs>   - initial database record count\n");
    fprintf(f, "        -c <cdb2cfg>        - set cdb2cfg file\n");
    fprintf(f, "        -C <seconds>        - set contention interval\n");
    fprintf(f, "        -h                  - this menu\n");
    fprintf(f, "        -D                  - enable debug trace\n");
}

void EXIT(const char *func, int line, int rc)
{
    fprintf(stderr, "%s line %d calling exit with rc %d\n", func, line, rc);
    fflush(stdout);
    fflush(stderr);
    exit(rc);
}

int64_t count_updates(void)
{
    int rc;
    cdb2_hndl_tp *hndl;
    int64_t rtn;
    if ((rc = cdb2_open(&hndl, dbname, cltype, 0)) != 0) {
        fprintf(stderr, "%s error opening handle to %s stage %s\n",
                __func__, dbname, cltype);
        EXIT(__func__, __LINE__, 1);
    }

    if ((rc = cdb2_run_statement(hndl, "select sum(state) from schedule")) != 0) {
        fprintf(stderr, "%s error finding number of updates: %d, %s\n", __func__,
                rc, cdb2_errstr(hndl));
        EXIT(__func__, __LINE__, 1);
    }

    rtn = *((int64_t *)cdb2_column_value(hndl, 0));

    cdb2_close(hndl);
    return rtn;
}


void *run_test(void *x)
{
    cdb2_hndl_tp *hndl;
    int64_t td = (int64_t)x;
    int64_t *instids = (int64_t *)calloc(limit, sizeof(int64_t));
    int count = 0;
    int success = 0;
    int total = 0;
    int rc;

    if ((rc = cdb2_open(&hndl, dbname, cltype, 0)) != 0) {
        fprintf(stderr, "%s error opening handle to %s stage %s\n",
                __func__, dbname, cltype);
        EXIT(__func__, __LINE__, 1);
    }

    if ((rc = cdb2_run_statement(hndl, "SET TRANSACTION READ COMMITTED")) != 0) {
            fprintf(stderr, "%s error running SET TRANSACION: %d\n", __func__,
                    rc);
            EXIT(__func__, __LINE__, 1);
    }

    while(stop_flag == 0) {

        int do_contention = contention;
        if ((rc = cdb2_run_statement(hndl, "BEGIN")) != 0) {
            fprintf(stderr, "%s error running BEGIN: %d\n", __func__,
                    rc);
            EXIT(__func__, __LINE__, 1);
        }

        cdb2_clearbindings(hndl);
        cdb2_bind_param(hndl, "limit", CDB2_INTEGER, &limit, sizeof(limit));
        rc = cdb2_run_statement(hndl, "selectv instid from schedule "
                "order by instid limit @limit");

        if (rc != 0) {
            fprintf(stderr, "%s error running selectv: %d, %s\n", __func__,
                    rc, cdb2_errstr(hndl));
            EXIT(__func__, __LINE__, 1);
        }
        count = 0;
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
            instids[count++] = *((int64_t *)cdb2_column_value(hndl, 0));
        }
        assert(count == limit);
        assert(rc == CDB2_OK_DONE);

        int64_t instid;
        cdb2_clearbindings(hndl);
        cdb2_bind_param(hndl, "instid", CDB2_INTEGER, &instid, sizeof(instid));
        for (int i = 0; i < (do_contention ? limit : 1); i++) {
            instid = instids[i];
            if ((rc = cdb2_run_statement(hndl, "update schedule set state = state + 1 where "
                    "instid = @instid")) != 0) {
                fprintf(stderr, "%s error running UPDATE: %d, %s\n", __func__,
                        rc, cdb2_errstr(hndl));
                EXIT(__func__, __LINE__, 1);
            }
        }

        cdb2_clearbindings(hndl);
        if ((rc = cdb2_run_statement(hndl, "COMMIT")) != 0) {
            if (rc != -103 && rc != 4 && rc != 2 && rc != 210 && rc != -1 && rc != 203) { 
                fprintf(stderr, "%s error running COMMIT: %d, %s\n", __func__,
                        rc, cdb2_errstr(hndl));
                EXIT(__func__, __LINE__, 1);
            }
            fprintf(errlog, "%s error running COMMIT line %d: %d, %s - CONTINUING\n",
                    __func__, __LINE__, rc, cdb2_errstr(hndl));
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
            ;
        if (rc != CDB2_OK_DONE) {
            if (rc != -103 && rc != 4 && rc != 2 && rc != 210 && rc != -1 && rc != 203) { 
                fprintf(stderr, "%s error running COMMIT line %d: %d, %s\n",
                        __func__, __LINE__, rc, cdb2_errstr(hndl));
                EXIT(__func__, __LINE__, 1);
            }
        } else {
            success++;
            updates_this_segment += limit;
        }
        total++;
    }

    cdb2_close(hndl);
    printf("Thread %"PRId64" exiting, %d success, %d total\n", td, success,
            total);

    free(instids);

    return NULL;
}

void drop_tables(void)
{
    cdb2_hndl_tp *hndl;
    int rc;
    if ((rc = cdb2_open(&hndl, dbname, cltype, 0)) != 0) {
        fprintf(stderr, "%s error opening handle to %s stage %s\n",
                __func__, dbname, cltype);
        EXIT(__func__, __LINE__, 1);
    }

    cdb2_run_statement(hndl, "DROP TABLE schedule");
    cdb2_run_statement(hndl, "DROP TABLE jobinstance");
    cdb2_run_statement(hndl, "DROP TABLE request");
    cdb2_close(hndl);
}

void create_schedule_table(void)
{
    cdb2_hndl_tp *hndl;
    int rc;
    if ((rc = cdb2_open(&hndl, dbname, cltype, 0)) != 0) {
        fprintf(stderr, "%s error opening handle to %s stage %s\n",
                __func__, dbname, cltype);
        EXIT(__func__, __LINE__, 1);
    }

    if ((rc = cdb2_run_statement(hndl,
                    "CREATE TABLE schedule("
                    "rqstid INTEGER NULL, "         /* (NUMRECS/100) different values */
                    "instid INTEGER, "              /* (NUMRECS) different values */
                    "start DATETIME, "              /* (NUMRECS / 36) - JUST USE now() */
                    "tztag CSTRING(128) NULL, "     /* SAME VALUE PROLLY NULL */
                    "state INTEGER)"))              /* (4) different values */
            != 0) {
        fprintf(stderr, "%s error creating table, %d, %s\n",
                __func__, rc, cdb2_errstr(hndl));
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    /* stat='1300000 5 5 5' */
    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE UNIQUE INDEX schdid on schedule("
                    "'instid', 'start' DESC, 'tztag') OPTION DATACOPY")) 
            != 0) {
        fprintf(stderr, "%s line %d error creating index, %d, %s\n",
                __func__, __LINE__, rc, cdb2_errstr(hndl));
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    /* stat='1300000 330000 30' */
    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE INDEX state on schedule("
                    "'state', 'start')")) 
            != 0) {
        fprintf(stderr, "%s line %d error creating index, %d, %s\n",
                __func__, __LINE__, rc, cdb2_errstr(hndl));

        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    /* stat='1300000 40' */
    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE INDEX start on schedule("
                    "'start' DESC)")) 
            != 0) {

        fprintf(stderr, "%s line %d error creating index, %d, %s\n",
                __func__, __LINE__, rc, cdb2_errstr(hndl));
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    /* stat='1300000 5 5 5' */
    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE INDEX instdata on schedule("
                    "'instid', 'state', 'start')")) 
            != 0) {

        fprintf(stderr, "%s line %d error creating index, %d, %s\n",
                __func__, __LINE__, rc, cdb2_errstr(hndl));
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    /* stat='1300000 100 90 5' */
    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE INDEX rqststate on schedule("
                    "'rqstid', 'state', 'start')")) 
            != 0) {
        fprintf(stderr, "%s line %d error creating index, %d, %s\n",
                __func__, __LINE__, rc, cdb2_errstr(hndl));
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    cdb2_close(hndl);
}

void populate_tables(void)
{
    cdb2_hndl_tp *hndl;
    int rc, rowcount = 0, chunksize = 2000;
    if ((rc = cdb2_open(&hndl, dbname, cltype, 0)) != 0) {
        fprintf(stderr, "%s error opening handle to %s stage %s\n",
                __func__, dbname, cltype);
        EXIT(__func__, __LINE__, 1);
    }

    while (rowcount < numrecs) {

        if ((rc = cdb2_run_statement(hndl, "BEGIN")) != 0) {
            fprintf(stderr, "%s error running begin, %d\n", __func__,
                    rc);
            EXIT(__func__, __LINE__, 1);
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        assert(rc == CDB2_OK_DONE);

        for (int j = 0 ; (j < chunksize) && (rowcount < numrecs) ;
                j++, rowcount++) {

            /* Insert into the jobinstance and schedule tables */
            int64_t instid = rowcount;
            int64_t rqstid = rowcount / 600;
            int64_t state = rowcount % 5;

            cdb2_clearbindings(hndl);

            rc = cdb2_bind_param(hndl, "instid", CDB2_INTEGER, &instid, sizeof(instid));
            assert(rc == 0);

            rc = cdb2_bind_param(hndl, "rqstid", CDB2_INTEGER, &rqstid, sizeof(rqstid));
            assert(rc == 0);

            rc = cdb2_bind_param(hndl, "state", CDB2_INTEGER, &state, sizeof(state));
            assert(rc == 0);

            if ((rc = cdb2_run_statement(hndl, "INSERT INTO schedule "
                            "(rqstid, instid, start, state) "
                            "VALUES (@rqstid, @instid, now(), @state)")) != 0) {
                fprintf(stderr, "%s error running insert into schedule, %d\n", __func__,
                        rc);
                EXIT(__func__, __LINE__, 1);
            }
            while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
            assert(rc == CDB2_OK_DONE);
        }

        if ((rc = cdb2_run_statement(hndl, "COMMIT")) != 0) {
            fprintf(stderr, "%s error running commit, %d, %s\n",
                    __func__, rc, cdb2_errstr(hndl));
            EXIT(__func__, __LINE__, 1);
        }
        
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        assert(rc == CDB2_OK_DONE);
    }

    cdb2_close(hndl);
}

void create_tables(void)
{
    create_schedule_table();
}

int main(int argc, char *argv[])
{
    int c, err = 0, rc;
    pthread_t *thds = NULL;
    argv0 = argv[0];
    setvbuf(stdout, NULL, _IOLBF, 0);
    while ((c = getopt(argc, argv, "d:T:t:Ic:hR:Dvm:l:M")) != EOF) {
        switch(c) {
            case 'd':
                dbname = optarg;
                break;
            case 'M':
                do_create = 1;
                break;
            case 'I':
                do_insert = 1;
                break;
            case 'v':
                verbose = 1;
                break;
            case 'T':
                numthds = atoi(optarg);
                break;
            case 'm':
                timelimit = atoi(optarg);
                break;
            case 't':
                cltype = optarg;
                break;
            case 'R':
                numrecs = atoi(optarg);
                break;
            case 'l':
                limit = atoi(optarg);
       	        break;
            case 'c':
                cdb2_set_comdb2db_config(optarg);
                break;
            case 'h':
                usage(stdout);
                EXIT(__func__, __LINE__, 0);
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
        EXIT(__func__, __LINE__, 1);
    }

    errlog = fopen("err.log", "w");
    assert(errlog);
    srand(time(NULL));

    if (do_create) {
        drop_tables();
        create_tables();
    }

    if (do_insert) {
        populate_tables();
    }
    thds = (pthread_t *)calloc(numthds, sizeof(pthread_t));
    for(int i = 0; i < numthds; i++) {
        int64_t x = (int64_t)i;
        rc = pthread_create(&thds[i], NULL, run_test, (void *)x);
        if (rc != 0) {
            fprintf(stderr, "pthread_create error, rc=%d errno=%d\n", rc,
                    errno);
            EXIT(__func__, __LINE__, 1);
        }
    }

    time_t now, start = time(NULL);
    int64_t updates, cupdates;

    printf("Running with %d threads for %d seconds\n", numthds,
            timelimit);
    while (((now = time(NULL)) - start) < timelimit) {
        sleep(contention_interval);
        updates = updates_this_segment;
        updates_this_segment = 0;
        contention = !contention;
        printf("%"PRId64" updates, %s contention\n", updates,
                contention ? "enabling" : "disabling");
    }

    stop_flag = 1;

    for(int i = 0; i < numthds; i++) {
        pthread_join(thds[i], NULL);
    }
    cupdates = count_updates();
    printf("%"PRId64" total updates\n", cupdates);
    return 0;
}
