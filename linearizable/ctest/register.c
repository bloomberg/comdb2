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
#include <testutil.h>
#include <nemesis.h>
#include <cdb2api.h>

enum eventtypes {
    PARTITION_EVENT = 0x00000001,
    SIGSTOP_EVENT = 0x00000002,
    CLOCK_EVENT = 0x00000004
};

int nthreads = 5;
pthread_t *threads;
int runtime = 60;
int max_clock_skew = 60;

char *dbname = NULL;
char *cltype = "dev";
char *argv0 = NULL;

uint32_t which_events = 0;
int is_hasql = 1;
int partition_master = 1;
int max_retries = 1000000;
int debug_trace = 0;
FILE *c_file;

void usage(FILE *f)
{
    fprintf(f, "Usage: %s [ opts ]\n", argv0);
    fprintf(f, "        -d <dbname>         - set name of the test database\n");
    fprintf(f, "        -G <event>          - add event type ('partition', "
               "'sigstop', or 'clock')\n");
    fprintf(f, "        -T <numthd>         - set the number of threads\n");
    fprintf(
        f, "        -t <cltype>         - 'dev', 'alpha', 'beta', or 'prod'\n");
    fprintf(f, "        -M                  - partition the master\n");
    fprintf(f, "        -m <max-retries>    - set max-retries in the api\n");
    fprintf(f, "        -D                  - enable debug trace\n");
    fprintf(f, "        -o                  - run only main thread\n");
    fprintf(f, "        -r <runtime>        - set runtime in seconds\n");
    fprintf(f, "        -j <output>         - set clojure-output file\n");
}

static int update_int(cdb2_hndl_tp *db, char *readnode, int threadnum, int op,
                      int curval, int newval, int newuid, int *foundval,
                      int *founduid)
{
    int rc, count = 0;
    cdb2_effects_tp effects;
    char sql[128];

    /* If there are no records, these will remain -1 */
    (*foundval) = (*founduid) = -1;

    rc = cdb2_run_statement(db, "begin");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "begin failed, rc %d, %s\n",
                 rc, cdb2_errstr(db));
        return rc;
    } else {
        if (debug_trace) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "begin: read node %s (success)\n", readnode);
        }
    }

    rc = cdb2_run_statement(db, "select * from register where id = 1");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__,
                 "XXX select line %d: error %s\n", __LINE__, cdb2_errstr(db));
        goto out;
    }

    rc = cdb2_next_record(db);
    while (rc == CDB2_OK) {
        count++;
        int v = (int)*(long long *)cdb2_column_value(db, 0);
        if (v != 1) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "Unexpected value for column 0: %d\n", v);
            myexit(__func__, __LINE__, 1);
        }

        *foundval = (int)*(long long *)cdb2_column_value(db, 1);
        *founduid = (int)*(long long *)cdb2_column_value(db, 2);

        rc = cdb2_next_record(db);
    }

    if (rc != CDB2_OK_DONE) {
        cdb2_run_statement(db, "commit");
        tdprintf(stderr, db, __func__, __LINE__,
                 "Unexpected error from cdb2_next_record, %d\n", rc);
        goto out;
    }

    if (count > 1) {
        tdprintf(stderr, db, __func__, __LINE__,
                 "XXX invalid number of records: %d\n", count);
        myexit(__func__, __LINE__, 1);
    }

    /* Read */
    if (op == 0) {
        rc = cdb2_run_statement(db, "commit");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__, "Failure committing after "
                                                     "a single select "
                                                     "statement?  rc=%d %s\n",
                     rc, cdb2_errstr(db));
        }

        /* This can't really fail - I've already read it */
        rc = 0;
    }

    /* Write */
    if (op == 1) {
        if (count == 0) {
            snprintf(sql, sizeof(sql),
                     "insert into register (id, val, uid) values (1, %d, %d)",
                     newval, newuid);
            rc = cdb2_run_statement(db, sql);
            if (rc) {
                tdprintf(stderr, db, __func__, __LINE__,
                         "XXX insert returns %d, %s\n", rc, cdb2_errstr(db));
                goto out;
            }

            rc = cdb2_run_statement(db, "commit");
            if (rc) {
                if (debug_trace)
                    tdprintf(stderr, db, __func__, __LINE__,
                             "XXX commit: insert into register(id, val, uid) "
                             "values (1, %d, %d) rc = %d %s\n",
                             newval, newuid, rc, cdb2_errstr(db));
                goto out;
            }

            rc = cdb2_get_effects(db, &effects);
            if (rc) {
                tdprintf(stderr, db, __func__, __LINE__,
                         "XXX get_effects rc %d %s\n", rc, cdb2_errstr(db));
                myexit(__func__, __LINE__, 1);
            }

            if (effects.num_inserted != 1) {
                if (debug_trace)
                    tdprintf(stderr, db, __func__, __LINE__,
                             "XXX get_effects num_inserted = %d\n",
                             effects.num_inserted);
                rc = -1;
                goto out;
            }
        } else {
            snprintf(sql, sizeof(sql), "update register set val = %d, uid = %d "
                                       "where 1 -- cur uid is %d",
                     newval, newuid, *founduid);
            rc = cdb2_run_statement(db, sql);
            if (rc) {
                if (debug_trace)
                    tdprintf(stderr, db, __func__, __LINE__,
                             "update returns %d, %s\n", rc, cdb2_errstr(db));
                goto out;
            }

            rc = cdb2_run_statement(db, "commit");
            if (rc) {
                if (debug_trace)
                    tdprintf(stderr, db, __func__, __LINE__,
                             "commit: update register set val = %d, uid = %d "
                             "where 1 -- cur uid is %d : rc = %d %s\n",
                             newval, newuid, *founduid, rc, cdb2_errstr(db));
                goto out;
            }

            rc = cdb2_get_effects(db, &effects);
            if (rc) {
                tdprintf(stderr, db, __func__, __LINE__,
                         "XXX get_effects rc %d %s\n", rc, cdb2_errstr(db));
                myexit(__func__, __LINE__, 1);
            }

            if (effects.num_updated != 1) {
                rc = -1;
                goto out;
            }
        }
    }

    /* Compare and Set */
    if (op == 2) {
        snprintf(sql, sizeof(sql), "update register set val = %d, uid = %d "
                                   "where val = %d -- cur uid is %d",
                 newval, newuid, curval, *founduid);
        rc = cdb2_run_statement(db, sql);

        if (rc) {
            if (debug_trace)
                tdprintf(stderr, db, __func__, __LINE__,
                         "XXX update returns %d, %s\n", rc, cdb2_errstr(db));
            goto out;
        }

        rc = cdb2_run_statement(db, "commit");
        if (rc) {
            if (debug_trace)
                tdprintf(stderr, db, __func__, __LINE__,
                         "XXX commit: update register set val = %d, uid = %d "
                         "where val = %d -- "
                         "cur uid is %d : rc = %d %s\n",
                         newval, newuid, curval, *founduid, rc,
                         cdb2_errstr(db));
            goto out;
        }

        rc = cdb2_get_effects(db, &effects);
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "XXX get_effects rc %d %s\n", rc, cdb2_errstr(db));
            myexit(__func__, __LINE__, 1);
        }

        if (effects.num_updated != 1) {
            rc = -1;
            goto out;
        }
    }

out:

    return rc;
}

void update(cdb2_hndl_tp **indb, char *readnode, int threadnum)
{
    cdb2_hndl_tp *db = (*indb);
    int rc, op, newval = (rand() % 5), curval = (rand() % 5),
                newuid = (rand() % 100000);
    int foundval, founduid;
    unsigned long long begintm, endtm;
    static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;

    op = (rand() % 3);

    if (is_hasql) {
        rc = cdb2_run_statement(db, "set hasql on");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "thd: set hasql on rc %d, %s\n", rc, cdb2_errstr(db));
            return;
        }
    }

    rc = cdb2_run_statement(db, "set transaction serializable");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__,
                 "set transaction serializable, rc %d, %s\n", rc,
                 cdb2_errstr(db));
        return;
    }

    begintm = timeus();

    if (c_file) {
        pthread_mutex_lock(&lk);

        /* Read */
        if (op == 0) {
            fprintf(
                c_file,
                "{:type :invoke :f :read :value nil :process %d :time %llu}\n",
                threadnum, begintm);
        }

        /* Write */
        if (op == 1) {
            fprintf(c_file, "{:type :invoke :f :write :value %d :process %d "
                            ":uid %d :time %llu}\n",
                    newval, threadnum, newuid, begintm);
        }

        /* CAS */
        if (op == 2) {
            fprintf(c_file, "{:type :invoke :f :cas :value [%d %d] :process %d "
                            ":uid %d :time %llu}\n",
                    curval, newval, threadnum, newuid, begintm);
        }
        pthread_mutex_unlock(&lk);
    }

    rc = update_int(db, readnode, threadnum, op, curval, newval, newuid,
                    &foundval, &founduid);

    endtm = timeus();

    if (c_file) {
        pthread_mutex_lock(&lk);

        /* Read */
        if (op == 0) {
            if (rc == 0) {
                if (foundval == -1) {
                    fprintf(c_file, "{:type :ok :f :read :process %d :value "
                                    "nil :uid nil :time %llu}\n",
                            threadnum, endtm);
                } else {
                    fprintf(c_file, "{:type :ok :f :read :process %d :value %d "
                                    ":uid %d :time %llu}\n",
                            threadnum, foundval, founduid, endtm);
                }
            } else if (rc == -105) {
                tdprintf(stderr, db, __func__, __LINE__,
                         "Don't know the outcome here with rcode -105\n");
                myexit(__func__, __LINE__, 1);
            } else {
                fprintf(c_file, "{:type :fail :f :read :process %d :value %d "
                                ":uid %d :time %llu}\n",
                        threadnum, foundval, founduid, endtm);
            }
        }

        /* Write */
        if (op == 1) {
            if (rc == 0) {
                fprintf(c_file, "{:type :ok :f :write :process %d :value %d "
                                ":uid %d :time %llu}\n",
                        threadnum, newval, newuid, endtm);
            } else if (rc == -105) {
                tdprintf(stderr, db, __func__, __LINE__,
                         "Don't know the outcome here with rcode -105\n");
                myexit(__func__, __LINE__, 1);
            } else {
                fprintf(c_file, "{:type :fail :f :write :process %d :value %d "
                                ":uid %d :time %llu}\n",
                        threadnum, newval, newuid, endtm);
            }
        }

        /* CAS */
        if (op == 2) {
            if (rc == 0) {
                fprintf(c_file, "{:type :ok :f :cas :process %d :value [%d %d] "
                                ":uid %d :time %llu}\n",
                        threadnum, curval, newval, newuid, endtm);
            } else if (rc == -105) {
                tdprintf(stderr, db, __func__, __LINE__,
                         "Don't know the outcome here with rcode -105\n");
                myexit(__func__, __LINE__, 1);
            } else {
                fprintf(c_file, "{:type :fail :f :cas :process %d :value [%d "
                                "%d] :uid %d :time %llu}\n",
                        threadnum, curval, newval, newuid, endtm);
            }
        }

        pthread_mutex_unlock(&lk);
    }
}

void *thd(void *arg)
{
    int64_t now = timems(), end = now + runtime * 1000;
    char *readnode;
    cdb2_hndl_tp *db;
    int rc, iter = 0;
    int threadnum = (unsigned long long)arg;
    fprintf(stdout, "Thread %d starting\n", threadnum);

    rc = cdb2_open(&db, dbname, cltype, CDB2_RANDOM);
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "thd: open rc %d %s\n", rc,
                 cdb2_errstr(db));
        cdb2_close(db);
        myexit(__func__, __LINE__, 1);
    }

    if (is_hasql) {
        rc = cdb2_run_statement(db, "set hasql on");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "thd: set hasql on rc %d, %s\n", rc, cdb2_errstr(db));
            cdb2_close(db);
            myexit(__func__, __LINE__, 1);
        }
    }

    rc = cdb2_run_statement(db, "set transaction serializable");
    if (rc) {
        fprintf(stderr, "Set transaction serializable failed, rc=%d %s\n", rc,
                cdb2_errstr(db));
        exit(-1);
    }

    cdb2_set_max_retries(max_retries);

    if (debug_trace) {
        cdb2_set_debug_trace(db);
    }

    if (!(readnode = read_node(db))) {
        tdprintf(stderr, db, __func__, __LINE__,
                 "Couldn't determine read node for thread\n");
    } else {
        if (debug_trace)
            tdprintf(stderr, db, __func__, __LINE__, "read node is %s\n",
                     readnode);
    }

    while (iter == 0 || now < end) {
        iter++;
        update(&db, readnode, threadnum);
        now = timems();
    }
    cdb2_close(db);
    if (readnode) free(readnode);
    return NULL;
}

static int empty_register_table(void)
{
    int rc;
    cdb2_hndl_tp *db;
    rc = cdb2_open(&db, dbname, cltype, CDB2_RANDOM);
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "thd: open rc %d %s\n", rc,
                 cdb2_errstr(db));
        cdb2_close(db);
        myexit(__func__, __LINE__, 1);
    }

    if (is_hasql) {
        rc = cdb2_run_statement(db, "set hasql on");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__,
                     "thd: set hasql on rc %d, %s\n", rc, cdb2_errstr(db));
            cdb2_close(db);
            myexit(__func__, __LINE__, 1);
        }
    }

    rc = cdb2_run_statement(db, "set transaction serializable");
    if (rc) {
        fprintf(stderr, "Set transaction serializable failed, rc=%d %s\n", rc,
                cdb2_errstr(db));
        exit(-1);
    }

    cdb2_set_max_retries(max_retries);

    if (debug_trace) {
        cdb2_set_debug_trace(db);
    }

    rc = cdb2_run_statement(db, "delete from register where 1");
    if (rc) {
        if (debug_trace)
            tdprintf(stderr, db, __func__, __LINE__,
                    "delete from register where 1 failed: %d %s\n", rc,
                    cdb2_errstr(db));
    }

    cdb2_close(db);
    return rc;
}

int main(int argc, char *argv[])
{
    int rc, c, errors = 0, cnt;
    argv0 = argv[0];

    while ((c = getopt(argc, argv, "d:G:T:t:PMm:Doi:r:j:")) != EOF) {
        switch (c) {
        case 'd': dbname = optarg; break;
        case 'G':
            if (0 == strcasecmp(optarg, "partition")) {
                which_events |= PARTITION_EVENT;
            } else if (0 == strcasecmp(optarg, "sigstop")) {
                which_events |= SIGSTOP_EVENT;
            } else if (0 == strcasecmp(optarg, "clock")) {
                which_events |= CLOCK_EVENT;
            } else {
                fprintf(stderr, "Unknown test: %s\n", optarg);
                errors++;
            }
            break;
        case 'T': nthreads = atoi(optarg); break;
        case 't': cltype = optarg; break;
        case 'M': partition_master = 1; break;
        case 'm': max_retries = atoi(optarg); break;
        case 'D': debug_trace = 1; break;
        case 'o': nthreads = 1; break;
        case 'r': runtime = atoi(optarg); break;
        case 'j':
            if ((c_file = fopen(optarg, "w")) == NULL) {
                fprintf(stderr, "Error opening clojure output file: %s\n",
                        strerror(errno));
            }
            break;
        default:
            fprintf(stderr, "Unknown option, '%c'\n", c);
            errors++;
            break;
        }
    }

    if (errors) {
        fprintf(stderr, "there were errors, exiting\n");
        usage(stdout);
        exit(1);
    }

    if (!dbname) {
        fprintf(stderr, "dbname is not specified\n");
        usage(stdout);
        exit(1);
    }

    if (which_events == 0) {
        fprintf(stderr, "NO TESTS SPECIFIED .. THIS SHOULD BE AN EASY RUN..\n");
    }

    srand(time(NULL) ^ getpid());

    uint32_t flags = 0;
    if (partition_master) flags |= NEMESIS_PARTITION_MASTER;
    if (debug_trace) flags |= NEMESIS_VERBOSE;
    struct nemesis *n = NULL;
    for (cnt = 0; cnt < 1000 && n == NULL; cnt++) {
        n = nemesis_open(dbname, cltype, flags);
        if (!n) sleep(1);
    }

    if (!n) {
        fprintf(stderr, "Could not open nemesis for %s, exiting\n", dbname);
        myexit(__func__, __LINE__, 1);
    }

    fixall(n);

    if (c_file) {
        fprintf(c_file, "[\n");
    }

    cnt = 0;
    while ((rc = empty_register_table())) {
        cnt++;
        fprintf(stderr, "Error emptying register table count %d retrying\n", 
                cnt);
        sleep(1);
    }

    threads = malloc(sizeof(pthread_t) * nthreads);
    for (unsigned long long i = 0; i < nthreads; i++) {
        rc = pthread_create(&threads[i], NULL, thd, (void *)i);
    }

    int sleeptime = (runtime / 2 - 1);
    if (sleeptime > 0) sleep(sleeptime);

    if (which_events & PARTITION_EVENT) {
        breaknet(n);
    }
    if (which_events & SIGSTOP_EVENT) {
        signaldb(n, SIGSTOP, 0);
    }
    if (which_events & CLOCK_EVENT) {
        breakclocks(n, max_clock_skew);
    }

    if (sleeptime > 0) sleep(sleeptime);

    if (which_events & PARTITION_EVENT) {
        fixnet(n);
    }
    if (which_events & SIGSTOP_EVENT) {
        signaldb(n, SIGCONT, 1);
    }
    if (which_events & CLOCK_EVENT) {
        fixclocks(n);
    }

    sleep(2);
    printf("done\n");

    for (int i = 0; i < nthreads; i++) {
        void *p;
        rc = pthread_join(threads[i], &p);
        if (rc) {
            fprintf(stderr, "join %d rc %d %s\n", i, rc, strerror(rc));
            return 1;
        }
    }

    if (c_file) {
        fprintf(c_file, "]\n");
    }
    return 0;
}
