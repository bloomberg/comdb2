#include <pthread.h>
#include <signal.h>
#include <assert.h>

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <time.h>

#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <cdb2api.h>

char *dbname = NULL;
char *stage = "default";
int order = 0;
int forkmode = 0;

enum {
    SOCKSQL             = 1,
    READ_COMMITTED       = 2,
    SNAPSHOT            = 3,
    SERIALIZABLE        = 4
};

int selectv_updaters = 10;
int updaters = 10;
int selectvers = 10;
int isolation = SOCKSQL;
int time_is_up = 0;

void usage(FILE *f)
{
    fprintf(f, "Usage:\n");
    fprintf(f, "    -d <dbname>                  - set dbname\n");
    fprintf(f, "    -s <stage>                   - set stage\n");
    fprintf(f, "    -c <config>                  - set config file\n");
    fprintf(f, "    -i <isolation>               - set isolation level\n");
    fprintf(f, "    -v <cnt>                     - number of selectv-updaters\n");
    fprintf(f, "    -u <cnt>                     - number of updaters\n");
    fprintf(f, "    -V <cnt>                     - number of selectv-ers\n");
    fprintf(f, "    -t <test-time>               - let test run for this many seconds\n");
    fprintf(f, "    -h                           - this menu\n");
}

enum {
    SELECTV_THREAD              = 1,
    UPDATER_THREAD              = 2,
    SELECTV_UPDATER_THREAD      = 3
};

struct thread_num_type
{
    int tdnum;
    int tdtype;
};

static char *type_to_str(int type)
{
    switch(type) {
        case SELECTV_THREAD:
            return "selectv";
        case UPDATER_THREAD:
            return "updater";
        case SELECTV_UPDATER_THREAD:
            return "selectv_updater";
        default:
            abort();
            break;
    }
}

/*
 * SELECTV THREAD SQL:
 * SHOULD GET CONSTRAINTS ERRORS, BUT NOT VERIFY ERRORS
 *
 * BEGIN
 *
 * SELECTV i.rqstid,instid,began from jobinstance i 
 *          LEFT JOIN dbgopts d ON d.rqstid=i.rqstid 
 *          WHERE state=1 AND began <= now() limit 15
 *
 * COMMIT
 *
 */

int selectv_query(cdb2_hndl_tp *db)
{
    int rc;
    int got_error = 0;

    cdb2_clearbindings(db);
    if ((rc = cdb2_run_statement(db, "begin")) != CDB2_OK) {
        fprintf(stderr, "line %d error running begin, %d\n", __LINE__, rc);
        exit(1);
    }
    if ((rc = cdb2_run_statement(db, "selectv i.rqstid,instid,began from jobinstance "
            "i left join dbgopts d on d.rqstid=i.rqstid where state=1 and began "
            "<= now() limit 15")) != CDB2_OK) {
        fprintf(stderr, "line %d error running select, %s\n", __LINE__, cdb2_errstr(db));
        exit(1);
    }
    while ((rc = cdb2_next_record(db)) != CDB2_OK_DONE) 
        ;
    assert(rc == CDB2_OK_DONE);

    /* SELECTV: only allowed constraints violation in any isolation level */
    if ((rc = cdb2_run_statement(db, "commit")) != CDB2_OK) {
        if (rc != -103) {
            fprintf(stderr, "line %d error running commit: %d %s\n", __LINE__, rc, cdb2_errstr(db));
            exit(1);
        } else
            got_error = 1;
    }

    if ((rc = cdb2_next_record(db)) != CDB2_OK_DONE) {
        if (rc != -103) {
            fprintf(stderr, "line %d error running commit: %d %s\n", __LINE__, rc, cdb2_errstr(db));
            exit(1);
        } else
            got_error = 1;
    }
    return got_error;
}


/*
 * SELECTV-UPDATER THREAD SQL
 * SHOULD GET CONSTRAINTS ERRORS
 *
 * BEGIN
 *
 * SELECTV i.rqstid,instid,began from jobinstance i 
 *          LEFT JOIN dbgopts d ON d.rqstid=i.rqstid 
 *          WHERE state=1 AND began <= now() limit 15
 *
 * UPDATE jobinstance SET instid = instdid WHERE instid = @x 
 *
 * COMMIT
 *
 */


int selectv_update_query(cdb2_hndl_tp *db)
{
    int rc;
    int64_t *instids = NULL;
    int cnt = 0;
    int got_error = 0;

    cdb2_clearbindings(db);
    if ((rc = cdb2_run_statement(db, "begin")) != CDB2_OK) {
        fprintf(stderr, "line %d error running begin, %d\n", __LINE__, rc);
        exit(1);
    }
    if ((rc = cdb2_run_statement(db, "selectv i.rqstid,instid,began from jobinstance "
            "i left join dbgopts d on d.rqstid=i.rqstid where state=1 and began "
            "<= now() limit 15")) != CDB2_OK) {
        fprintf(stderr, "line %d error running selectv, %s\n", __LINE__, cdb2_errstr(db));
        exit(1);
    }
    while ((rc = cdb2_next_record(db)) != CDB2_OK_DONE) {
        instids = realloc(instids, ((cnt+1) * sizeof(int64_t)));
        instids[cnt++] = *(int64_t *)cdb2_column_value(db, 1);
    }
    assert(rc == CDB2_OK_DONE);
    for (int i = 0; i < cnt; i++) {
        int64_t instid = instids[i];
        cdb2_clearbindings(db);
        if ((rc = cdb2_bind_param(db, "instid", CDB2_INTEGER, &instid, sizeof(instid))) != 0) {
            fprintf(stderr, "line %d error binding, %s\n", __LINE__, cdb2_errstr(db));
            exit(1);
        }
        if ((rc = cdb2_run_statement(db, "update jobinstance set instid = instid where "
                        "instid = @instid")) != CDB2_OK) {
            fprintf(stderr, "line %d run_statement error, %s\n", __LINE__, cdb2_errstr(db));
            exit(1);
        }
    }
    if ((rc = cdb2_run_statement(db, "commit")) != CDB2_OK) {
        /* SELECTV + UPDATE: only allowed constraints violation in any isolation level */
        if (rc != -103) {
            fprintf(stderr, "line %d error running commit: %d %s\n", __LINE__, rc, cdb2_errstr(db));
            exit(1);
        } else
            got_error = 1;
    }

    if ((rc = cdb2_next_record(db)) != CDB2_OK_DONE) {
        if (rc != -103) {
            fprintf(stderr, "line %d error running commit: %d %s\n", __LINE__, rc, cdb2_errstr(db));
            exit(1);
        } else
            got_error = 1;
    }
    return got_error;
}

/*
 * UPDATER THREAD SQL
 *
 * BEGIN
 *
 * SELECT i.rqstid,instid,began from jobinstance i 
 *          LEFT JOIN dbgopts d ON d.rqstid=i.rqstid 
 *          WHERE state=1 AND began <= now() limit 15
 *
 * UPDATE jobinstance SET instid = instdid WHERE instid = @x 
 *
 * COMMIT
 *
 */

int update_query(cdb2_hndl_tp *db)
{
    int rc;
    int64_t *instids = NULL;
    int cnt = 0;
    int got_error = 0;

    cdb2_clearbindings(db);
    if ((rc = cdb2_run_statement(db, "begin")) != CDB2_OK) {
        fprintf(stderr, "line %d error running begin, %d\n", __LINE__, rc);
        exit(1);
    }
    if ((rc = cdb2_run_statement(db, "select i.rqstid,instid,began from jobinstance "
            "i left join dbgopts d on d.rqstid=i.rqstid where state=1 and began "
            "<= now() limit 15")) != CDB2_OK) {
        fprintf(stderr, "line %d error running select, %s\n", __LINE__, cdb2_errstr(db));
        exit(1);
    }
    while ((rc = cdb2_next_record(db)) != CDB2_OK_DONE) {
        instids = realloc(instids, ((cnt+1) * sizeof(int64_t)));
        instids[cnt++] = *(int64_t *)cdb2_column_value(db, 1);
    }
    assert(rc == CDB2_OK_DONE);
    for (int i = 0; i < cnt; i++) {
        int64_t instid = instids[i];
        cdb2_clearbindings(db);
        if ((rc = cdb2_bind_param(db, "instid", CDB2_INTEGER, &instid, sizeof(instid))) != 0) {
            fprintf(stderr, "line %d error binding, %s\n", __LINE__, cdb2_errstr(db));
            exit(1);
        }
        if ((rc = cdb2_run_statement(db, "update jobinstance set instid = instid where "
                        "instid = @instid")) != CDB2_OK) {
            fprintf(stderr, "line %d run_statement error, %s\n", __LINE__, cdb2_errstr(db));
            exit(1);
        }
    }
    /* Allow verify error for < SERIALIZABLE, serializable error for == SERIALIZABLE */
    if ((rc = cdb2_run_statement(db, "commit")) != CDB2_OK) {
        if (isolation == SERIALIZABLE) {
            if (rc != 230) {
                fprintf(stderr, "line %d error running commit: %d %s\n", __LINE__, rc, cdb2_errstr(db));
                exit(1);
            } else
                got_error = 1;
        } else {
            if (rc != 2) {
                fprintf(stderr, "line %d error running commit: %d %s\n", __LINE__, rc, cdb2_errstr(db));
                exit(1);
            } else
                got_error = 1;
        }
    }

    if ((rc = cdb2_next_record(db)) != CDB2_OK_DONE) {
        if (isolation == SERIALIZABLE) {
            if (rc != 230) {
                fprintf(stderr, "line %d error running commit: %d %s\n", __LINE__, rc, cdb2_errstr(db));
                exit(1);
            } else
                got_error = 1;
        } else {
            if (rc != 2) {
                fprintf(stderr, "line %d error running commit: %d %s\n", __LINE__, rc, cdb2_errstr(db));
                exit(1);
            } else
                got_error = 1;
        }
    }
    return got_error;
}

void set_isolation(cdb2_hndl_tp *db)
{
    int rc;
    switch(isolation) {
        case SOCKSQL:
            rc = cdb2_run_statement(db, "set transaction blocksql");
            break;
        case READ_COMMITTED:
            rc = cdb2_run_statement(db, "set transaction read committed");
            break;
        case SNAPSHOT:
            rc = cdb2_run_statement(db, "set transaction snapshot");
            break;
        case SERIALIZABLE:
            rc = cdb2_run_statement(db, "set transaction serialzable");
            break;
        default:
            abort();
            break;
    }
    if (rc != 0) {
        fprintf(stderr, "set transaction failed, %d %s\n", rc, cdb2_errstr(db));
        exit(1);
    }

    while ((rc = cdb2_next_record(db)) == CDB2_OK)
        ;
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "Set transaction line %d bad rcode from next record, %d\n",
                __LINE__, rc);
        exit(1);
    }
}

void *thd(void *arg) {
    int rc;
    struct thread_num_type *tnum = (struct thread_num_type *)arg;
    int num = tnum->tdnum;
    int type = tnum->tdtype;
    int iterations = 0;
    int numerrs = 0;
    free(tnum);
    cdb2_hndl_tp *db;

    printf("%s thread num %d starting\n", type_to_str(type), num);
    rc = cdb2_open(&db, dbname, stage, 0);
    if (rc) {
        fprintf(stderr, "connect rc %d %s\n", rc, cdb2_errstr(db));
        exit(1);
    }

    if ((rc = cdb2_run_statement(db, "set verifyretry off")) != CDB2_OK) {
        fprintf(stderr, "line %d run_statement error, %d, %s\n", __LINE__,
                rc, cdb2_errstr(db));
        exit(1);
    }

    set_isolation(db);

    while(!time_is_up) {
        switch(type) {
            case SELECTV_THREAD:
                numerrs += selectv_query(db);
                break;
            case UPDATER_THREAD:
                numerrs += update_query(db);
                break;
            case SELECTV_UPDATER_THREAD:
                numerrs += selectv_update_query(db);
                break;
        }
        iterations++;
    }

    cdb2_close(db);
    printf("%s thread num %d exiting, iterations %d, permissible-errors: %d\n",
            type_to_str(type), num, iterations, numerrs);
    return NULL;
}

int main(int argc, char *argv[]) {
    char opt, err = 0;
    int test_time = 60;
    int nthreads;

    signal(SIGPIPE, SIG_IGN);
    setvbuf(stdout, NULL, _IOLBF, 0);
    srand(time(NULL) * getpid());

    while((opt = getopt(argc, argv, "d:s:c:v:u:V:i:t:h")) != -1) {
        switch (opt) {
            case 'd':
                dbname = optarg;
                break;
            case 'i':
                if (!strcasecmp(optarg, "socksql"))
                    isolation = SOCKSQL;
                else if (!strcasecmp(optarg, "read committed") || !strcasecmp(optarg, "reccom"))
                    isolation = READ_COMMITTED;
                else if (!strcasecmp(optarg, "snapshot") || !strcasecmp(optarg, "snapisol"))
                    isolation = SNAPSHOT;
                else if (!strcasecmp(optarg, "serial") || !strcasecmp(optarg, "serializable"))
                    isolation = SERIALIZABLE;
                else {
                    fprintf(stderr, "unknown isolation level, %s\n", optarg);
                    err++;
                }
                break;
            case 's':
                stage = optarg;
                break;
            case 'c':
                cdb2_set_comdb2db_config(optarg);
                break;
            case 'v':
                selectv_updaters = atoi(optarg);
                break;
            case 'u':
                updaters = atoi(optarg);
                break;
            case 'V':
                selectvers = atoi(optarg);
                break;
            case 't':
                test_time = atoi(optarg);
                break;
            case 'h':
                usage(stdout);
                exit(0);
                break;
            default:
                fprintf(stderr, "unknown option '%c'\n", opt);
                return 1;
        }
    }

    if (err) {
        usage(stdout);
        fprintf(stderr, "Invalid arguments\n");
        return 1;
    }

    if (dbname == NULL) {
        usage(stdout);
        fprintf(stderr, "Invalid dbname\n");
        return 1;
    }

    if (test_time <= 0) {
        usage(stdout);
        fprintf(stderr, "Invalid test-time\n");
        return 1;
    }

    pthread_t *threads;
    nthreads = selectv_updaters + updaters + selectvers;
    threads = malloc(sizeof(pthread_t) * nthreads);
    if (threads == NULL) {
        fprintf(stderr, "Out of memory?\n");
        return 1;
    }

    for (int i = 0 ; i < updaters; i++) {
        struct thread_num_type *t = (struct thread_num_type *)malloc(sizeof(*t));
        t->tdnum = i;
        t->tdtype = UPDATER_THREAD;
        int rc = pthread_create(&threads[i], NULL, thd, (void *)t);
        if (rc) {
            fprintf(stderr, "Can't create thread: %d %s\n", rc, strerror(rc));
            return 1;
        }
    }
    for (int i = 0 ; i < selectv_updaters; i++) {
        struct thread_num_type *t = (struct thread_num_type *)malloc(sizeof(*t));
        t->tdnum = i;
        t->tdtype = SELECTV_UPDATER_THREAD;
        int rc = pthread_create(&threads[updaters+i], NULL, thd, (void *)t);
        if (rc) {
            fprintf(stderr, "Can't create thread: %d %s\n", rc, strerror(rc));
            return 1;
        }
    }
    for (int i = 0 ; i < selectvers; i++) {
        struct thread_num_type *t = (struct thread_num_type *)malloc(sizeof(*t));
        t->tdnum = i;
        t->tdtype = SELECTV_THREAD;
        int rc = pthread_create(&threads[updaters+selectv_updaters+i], NULL, thd, (void *)t);
        if (rc) {
            fprintf(stderr, "Can't create thread: %d %s\n", rc, strerror(rc));
            return 1;
        }
    }

    sleep(test_time);
    time_is_up = 1;

    for (int i = 0; i < nthreads; i++) {
        void *p;
        int rc = pthread_join(threads[i], &p);
        if (rc) {
            fprintf(stderr, "Can't join thread: %d %s\n", rc, strerror(rc));
            return 1;
        }
    }

    return 0;
}

