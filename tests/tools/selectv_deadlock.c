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
int iterations = 1000000;
int update_density = 600; 
int numthds = 10;
int records = 600;
int verbose = 0;
int debug_trace = 0;
int numrecs = 100000;

void usage(FILE *f)
{
    fprintf(f, "Usage: %s [opts]\n", argv0);
    fprintf(f, "        -d <dbname>         - set name of the test database\n");
    fprintf(f, "        -T <numthd>         - set the number of threads\n");
    fprintf(f, "        -t <cltype>         - 'dev', 'alpha', 'beta', or 'prod'\n");
    fprintf(f, "        -i <iterations>     - run this many iterations\n");
    fprintf(f, "        -r <records>        - records per iteration\n");
    fprintf(f, "        -d <update-density> - update 1/x of every rqstid\n");
    fprintf(f, "        -R <initial-recs>   - initial database record count\n");
    fprintf(f, "        -c <cdb2cfg>        - set cdb2cfg file\n");
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


/* 
 * We saw per minute:
 *  30000 selectv
 *     50 updates
 * Against schedule, jobinstance, and request tables
 * I think this was divided between 20 requests .. so that would mean
 * each request does 1500 selectvs and 2 updates
 *
 * More information from David:
 * They are doing this:
 * 
 * BEGIN
 * SELECTV reqid from schedule where completed != NULL and status == not-running and start-time < now() limit 15
 * for (each record 'x' selected) {
 *  i think rqstids are the job-ids, and multiple instids can fall under a single jobid
 *  SELECTV from schedule where state=notrunning and this-is-same-rqstid(jobid)
 *  UPDATE each of these record's states
 *  INSERT a record into execution
 *  SELECTV from schedule left join execution where schedule record is not 'x' group by some schedule-column-grouping (multiple records possibly)
 *  UPDATE the selected schedule to some other state
 * }
 * UPDATE 15 SCHEDULE from first SELECTV
 * COMMIT
 */
void *run_test(void *x)
{
    cdb2_hndl_tp *hndl;
    int64_t td = (int64_t)x;
    int count = 0;
    int success = 0;
    int total = 0;
    int rc;
    int64_t machine = td;

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

    for (int j = 0; j < iterations; j++) {

        /*
        BEGIN

        selectv i.rqstid from schedule s 
        join jobinstance i on i.instid = s.instid
        join request r on r.rqstid = i.rqstid where s.start < (now() + cast(200 as second))
        order by s.start limit 15

        for each of these records {
            selectv from schedule where rqstid == this-request-id join jobinstance on s.instid

            for each of these records {
                        statreq shows that during the course of a minute, there were 30000 records
                        which were selectv'd against schedule, jobinstance, and request, and that 
                        50 records were updated against schedule.
                        So only the schedule table got any updates- and of every 600 records that
                        were selectv'd, only 1 was updated.  I've added an 'update density'
                        variable .. we'll update only a single one of these records- update
                        density controls the number of varying instid's per rqstid.

                update state set state = state in schedule limit 1
            }
        }
        */ 

        if ((rc = cdb2_run_statement(hndl, "BEGIN")) != 0) {
            fprintf(stderr, "%s error running BEGIN: %d\n", __func__,
                    rc);
            EXIT(__func__, __LINE__, 1);
        }

        cdb2_clearbindings(hndl);
        cdb2_bind_param(hndl, "machine", CDB2_INTEGER, &machine, sizeof(machine));
        if ((rc = cdb2_run_statement(hndl, "selectv i.rqstid from schedule s "
                        "join jobinstance i on i.instid = s.instid "
                        "join request r on r.instid = i.instid "
                        "where r.machine = @machine "
                        "order by s.start limit 15")) != 0) {
            fprintf(stderr, "%s error running BEGIN: %d\n", __func__,
                    rc);
            EXIT(__func__, __LINE__, 1);
        }

        int64_t rqstid[15] = {0};
        int cnt = 0;
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
            assert(cnt<15);
            (rqstid[cnt++]) = *((int64_t *)cdb2_column_value(hndl, 0));
        }
        assert(rc == CDB2_OK_DONE);
        assert(cnt == 15);

        int64_t r;
        for (int i = 1; i < 15; i++) {
            r = rqstid[i];
            if (verbose)
                printf("checking rqstid %"PRId64"\n", r);
            cdb2_clearbindings(hndl);
            cdb2_bind_param(hndl, "rqst", CDB2_INTEGER, &r, sizeof(r));
            if ((rc = cdb2_run_statement(hndl, "selectv j.instid from jobinstance j "
                            "join schedule s on j.instid = s.instid "
                            "where j.rqstid = @rqst")) != 0) {
                fprintf(stderr, "%s error selectv on jobinstance: %d, %s\n",
                        __func__, rc, cdb2_errstr(hndl));
                EXIT(__func__, __LINE__, 1);
            }
            count = 0;
            while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
                count++;
            assert(rc == CDB2_OK_DONE);
            if (verbose)
                printf("inner select returns %d records\n", count);


            if ((rc = cdb2_run_statement(hndl, "update schedule "
                            "set start = now() where rqstid = @rqst")) != 0) {
                fprintf(stderr, "%s error update schedule: %d\n", __func__,
                        rc);
                EXIT(__func__, __LINE__, 1);
            }
            rc = cdb2_next_record(hndl);
            while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
                ;
            assert(rc == CDB2_OK_DONE);
        }

        /* Update a single one of these in the schedule table randomly */
        cdb2_clearbindings(hndl);
        if ((rc = cdb2_run_statement(hndl, "COMMIT")) != 0) {
            if (rc != -103 && rc != 4 && rc != 2 && rc != 210) { 
                fprintf(stderr, "%s error running COMMIT: %d, %s\n", __func__,
                        rc, cdb2_errstr(hndl));
                EXIT(__func__, __LINE__, 1);
            }
            fprintf(stderr, "%s error running COMMIT line %d: %d, %s - CONTINUING\n",
                    __func__, __LINE__, rc, cdb2_errstr(hndl));
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            if (rc != -103 && rc != 4 && rc != 2 && rc != 210) { 
                fprintf(stderr, "%s error running COMMIT line %d: %d, %s\n",
                        __func__, __LINE__, rc, cdb2_errstr(hndl));
                EXIT(__func__, __LINE__, 1);
            }
        } else {
            success++;
        }
        total++;
    }

    cdb2_close(hndl);
    printf("Thread %"PRId64" exiting, %d success, %d total\n", td, success,
            total);

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
        fprintf(stderr, "%s error creating table, %d\n",
                __func__, rc);
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    /* stat='1300000 5 5 5' */
    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE UNIQUE INDEX schdid on schedule("
                    "'instid', 'start' DESC, 'tztag') OPTION DATACOPY")) 
            != 0) {
        fprintf(stderr, "%s error creating index, %d\n",
                __func__, rc);
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    /* stat='1300000 330000 30' */
    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE INDEX state on schedule("
                    "'state', 'start')")) 
            != 0) {
        fprintf(stderr, "%s error creating index, %d\n",
                __func__, rc);
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    /* stat='1300000 40' */
    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE INDEX start on schedule("
                    "'start' DESC)")) 
            != 0) {
        fprintf(stderr, "%s error creating index, %d\n",
                __func__, rc);
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    /* stat='1300000 5 5 5' */
    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE INDEX instdata on schedule("
                    "'instid', 'state', 'start')")) 
            != 0) {
        fprintf(stderr, "%s error creating index, %d\n",
                __func__, rc);
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    /* stat='1300000 100 90 5' */
    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE INDEX rqststate on schedule("
                    "'rqstid', 'state', 'start')")) 
            != 0) {
        fprintf(stderr, "%s error creating index, %d\n",
                __func__, rc);
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    cdb2_close(hndl);
}

/*
 * SELECTV s.instid,i.rqstid,s.start,s.tztag
 * FROM schedule s 
 * JOIN jobinstance i on i.instid=s.instid
 * WHERE s.state = @schdstate and s.start < (now() + cast(@lookahd as second))
 * ORDER BY s.start
 * LIMIT @chunk
 *
 * UPDATE schedule SET state = @schdcomp, updatehost = @localhost, end = now()
 * WHERE instid = @instid
 */


void create_jobinstance_table(void)
{
    cdb2_hndl_tp *hndl;
    int rc;
    if ((rc = cdb2_open(&hndl, dbname, cltype, 0)) != 0) {
        fprintf(stderr, "%s error opening handle to %s stage %s\n",
                __func__, dbname, cltype);
        EXIT(__func__, __LINE__, 1);
    }

    if ((rc = cdb2_run_statement(hndl,
                    "CREATE TABLE jobinstance("
                    "rqstid INTEGER, "              /* (NUMRECS / 100) different values */
                    "instid INTEGER, "              /* (NUMRECS) different values */
                    "state INTEGER, "               /* (4) different values */
                    "began DATETIME, "              /* (NUMRECS / 32) different values */
                    "completed DATETIME NULL, "
                    "updatehost INTEGER NULL)"))
            != 0) {
        fprintf(stderr, "%s error creating table, %d\n",
                __func__, rc);
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    /* stat='1300000 5' */
    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE UNIQUE INDEX instid on jobinstance('instid')"))
            != 0) {
        fprintf(stderr, "%s error creating index, %d\n",
                __func__, rc);
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    /* stat='1300000 80 5' */
    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE INDEX rqstidbegan on jobinstance("
                    "'rqstid' DESC, 'began' DESC) OPTION DATACOPY")) 
            != 0) {
        fprintf(stderr, "%s error creating index, %d\n",
                __func__, rc);
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    /* stat='1300000 500000 30' */
    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE INDEX statebegan on jobinstance("
                    "'state', 'began' DESC)")) 
            != 0) {
        fprintf(stderr, "%s error creating index, %d\n",
                __func__, rc);
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    /* stat='1300000 30' */
    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE INDEX began on jobinstance('began' DESC)"))
            != 0) {
        fprintf(stderr, "%s error creating index, %d\n",
                __func__, rc);
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    cdb2_close(hndl);
}

void create_request_table(void)
{
    cdb2_hndl_tp *hndl;

    int rc;
    if ((rc = cdb2_open(&hndl, dbname, cltype, 0)) != 0) {
        fprintf(stderr, "%s error opening handle to %s stage %s\n",
                __func__, dbname, cltype);
        EXIT(__func__, __LINE__, 1);
    }

    if ((rc = cdb2_run_statement(hndl,
                    "CREATE TABLE request("
                    "instid INTEGER, machine INTEGER)")) != 0) {
        fprintf(stderr, "%s error creating table, %d\n",
                __func__, rc);
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE INDEX jobrqstid on request("
                    "'instid')")) 
            != 0) {
        fprintf(stderr, "%s error creating index, %d\n",
                __func__, rc);
        EXIT(__func__, __LINE__, 1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) ;
    assert(rc == CDB2_OK_DONE);

    if ((rc = cdb2_run_statement(hndl, 
                    "CREATE INDEX machine on request("
                    "'machine')")) 
            != 0) {
        fprintf(stderr, "%s error creating index, %d\n",
                __func__, rc);
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

        /* We are shooting for each txn to have 
         * 9000 selectv's and 15 updates for the schedule table
         * 9000 selectv's for the instid table
         * 9000 selectv's for the request table */
        for (int j = 0 ; (j < chunksize) && (rowcount < numrecs) ;
                j++, rowcount++) {

            /* Insert into the jobinstance and schedule tables */
            int64_t instid = rowcount;
            int64_t rqstid = rowcount / update_density;
            int64_t state = rowcount % 5;
            int64_t machine = rowcount % numthds;

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

            cdb2_clearbindings(hndl);

            instid = rowcount;
            rqstid = rowcount / update_density;
            state = rowcount % 5;
            machine = rowcount % numthds;

            rc = cdb2_bind_param(hndl, "instid", CDB2_INTEGER, &instid, sizeof(instid));
            assert(rc == 0);

            rc = cdb2_bind_param(hndl, "rqstid", CDB2_INTEGER, &rqstid, sizeof(rqstid));
            assert(rc == 0);

            rc = cdb2_bind_param(hndl, "state", CDB2_INTEGER, &state, sizeof(state));
            assert(rc == 0);

            if ((rc = cdb2_run_statement(hndl, "INSERT INTO jobinstance "
                            "(rqstid, instid, state, began) "
                            "VALUES (@rqstid, @instid, @state, now())")) != 0) {
                fprintf(stderr, "%s error running insert into schedule, %d\n", __func__,
                        rc);
                EXIT(__func__, __LINE__, 1);
            }
            while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
            assert(rc == CDB2_OK_DONE);

            cdb2_clearbindings(hndl);

            rc = cdb2_bind_param(hndl, "instid", CDB2_INTEGER, &instid, sizeof(instid));
            assert(rc == 0);

            rc = cdb2_bind_param(hndl, "machine", CDB2_INTEGER, &machine, sizeof(machine));
            assert(rc == 0);

            if ((rc = cdb2_run_statement(hndl, "INSERT INTO request "
                            "(instid, machine) VALUES (@instid, @machine)")) != 0) {
                fprintf(stderr, "%s error running insert into request, %d, %s\n",
                        __func__, rc, cdb2_errstr(hndl));
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
    create_jobinstance_table();
    create_request_table();
}

int main(int argc, char *argv[])
{
    int c, err = 0, rc;
    pthread_t *thds = NULL;
    argv0 = argv[0];

    while ((c = getopt(argc, argv, "d:T:t:i:r:c:hR:Dv:")) != EOF) {
        switch(c) {
            case 'd':
                dbname = optarg;
                break;
            case 'v':
                verbose = 1;
                break;
            case 'T':
                numthds = atoi(optarg);
                break;
            case 't':
                cltype = optarg;
                break;
            case 'R':
                numrecs = atoi(optarg);
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

    srand(time(NULL));
    drop_tables();
    create_tables();
    populate_tables();

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

    for(int i = 0; i < numthds; i++) {
        pthread_join(thds[i], NULL);
    }
}
