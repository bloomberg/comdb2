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
    PARTITION_EVENT         = 0x00000001
   ,SIGSTOP_EVENT           = 0x00000002
   ,CLOCK_EVENT             = 0x00000004
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
int debug_trace = 0;
FILE *c_file;

void usage(FILE *f)
{
    fprintf(f, "Usage: %s [ opts ]\n", argv0);
    fprintf(f, "        -d <dbname>         - set name of the test database\n");
    fprintf(f, "        -G <event>          - add event type ('partition', 'sigstop', or 'clock')\n");
    fprintf(f, "        -T <numthd>         - set the number of threads\n");
    fprintf(f, "        -t <cltype>         - 'dev', 'alpha', 'beta', or 'prod'\n");
    fprintf(f, "        -M                  - partition the master\n");
    fprintf(f, "        -m <max-retries>    - set max-retries in the api\n");
    fprintf(f, "        -D                  - enable debug trace\n");
    fprintf(f, "        -o                  - run only main thread\n");
    fprintf(f, "        -r <runtime>        - set runtime in seconds\n");
    fprintf(f, "        -j <output>         - set clojure-output file\n");
}

static int update_int(cdb2_hndl_tp *db, char *readnode, int threadnum, int op, int curval, int newval, int newuid, int *foundval, int *founduid) 
{
    int rc;

    /* If there are no records, these will remain -1 */
    (*foundval) = (*founduid) = -1;

    rc = cdb2_run_statement(db, "begin");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "begin failed, rc %d, %s\n", rc, cdb2_errstr(db));
        return rc;
    }
    else {
        if (debug_trace) {
            tdprintf(stderr, db, __func__, __LINE__, "begin: read node %s (success)\n", readnode);
        }
    }


    rc = cdb2_run_statement(db, "select * from register where id = 1");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "XXX select line %d: error %s\n",
                __LINE__, cdb2_errstr(db));
        return rc;
    }

    rc = cdb2_next_record(db);
    while (rc == CDB2_OK) {
        count++;
        v = (int)*(long long*)cdb2_column_value(db, 0);
        if (v != 1) {
            fprintf(stderr, "Unexpected value for column 0: %d\n", v);
            myexit(__func__, __LINE__, 1);
        }

        *foundval = curval = (int)*(long long*)cdb2_column_value(db, 1);
        *founduid = curuid = (int)*(long long*)cdb2_column_value(db, 2);
    }

    if (count > 1) {
        tdprintf(stderr, db, __func__, __LINE__, "XXX invalid number of records: %d\n", count);
        myexit(__func__, __LINE__, 1);
    }

    if (op == 0) 
        return 0;

    /* Write .. if there are no records, this is an insert, otherwise this is an update */
    if (op == 1) {
        if (count == 0) {
            rc = cdb2_run_statement(db, "insert into register (id, val, uid) values (1, %d, %d)\n", 
                    newval, newuid);
        }
        else {
            rc = cdb2_run_statement(db, "update register set val = %d, uid = %d where 1\n", 
                    newval, newuid);
        }
    }

    /* curval is only used for CAS as the guessed original value */
}

void update(cdb2_hndl_tp **indb, char *readnode, int threadnum) {
    cdb2_hndl_tp *db = (*indb);
    int rc, op, newval = (rand() % 5), curval = (rand() % 5), curuid, newuid = (rand() % 100000), v, count=0;
    int foundval, founduid;
    uint64_t beginms, endms;
    static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;

    op = (rand() % 3);

    if (is_hasql)
    {
        rc = cdb2_run_statement(db, "set hasql on");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__, "thd: set hasql on rc %d, %s\n", rc, cdb2_errstr(db));
            return rc;
        }
    }

    rc = cdb2_run_statement(db, "set transaction serializable");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "set transaction serializable, rc %d, %s\n", rc, cdb2_errstr(db));
        return rc;
    }

    beginms = timems();

    if (c_file) {
        pthread_mutex_lock(&lk);

        /* Read */
        if (op == 0) {
            fprintf(c_file, "{:type :invoke :f :read :process %d :time %llu}\n", threadnum, beginms);
        }

        /* Write */
        if (op == 1) {
            fprintf(c_file, "{:type :invoke :f :write :value %d :process %d :time %llu :uid %d :time %llu}\n", 
                    newval, threadnum, newuid, beginms);
        }

        /* CAS */
        if (op == 2) {
            fprintf(c_file, "{:type :invoke :f :cas :value [%d %d] :process %d :time %llu :uid %d :time %llu}\n", 
                    curval, newval, threadnum, newuid, beginms);
        }
        pthread_mutex_unlock(&lk);
    }

    /*
    rc = cdb2_run_statement(db, "begin");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "begin failed, rc %d, %s\n", rc, cdb2_errstr(db));
        return rc;
    }
    else {
        if (debug_trace) {
            tdprintf(stderr, db, __func__, __LINE__, "begin: read node %s (success)\n", readnode);
        }
    }

    rc = update_int(db, readnode, threadnum, op, newval, newuid, &foundval, &founduid);
    */

    endms = timems();

    if (c_file) {
        pthread_mutex_lock(&lk);

        /* Read */
        if (op == 0) {
            if (rc == 0) {
                fprintf(c_file, "{:type :ok :f :read :process %d :value %d :uid %d :time %llu}\n", 
                        threadnum, foundval, founduid, endms);
            } else {
                fprintf(c_file, "{:type :fail :f :read :process %d :time %llu}\n", 
                        threadnum, endms);
            }
        }

        /* Write */
        if (op == 1) {
            if (rc == 0) {
                fprintf(c_file, "{:type :ok :f :write :process %d :value %d :uid %d :time %llu}\n", 
                        threadnum, newval, newuid, endms);
            } else {
                fprintf(c_file, "{:type :fail :f :write :process %d :time %llu}\n", 
                        threadnum, endms);
            }
        }

        /* CAS */
        if (op == 2) {
            if (rc == 0) {
                fprintf(c_file, "{:type :ok :f :cas :value [%d %d] :process %d :time %llu :uid %d :time %llu}\n", 
                        curval, newval, threadnum, newuid, beginms);
            }
            else {
            }
        }


        pthread_mutex_unlock(&lk);
    }




}

void* thd(void *arg) 
{
    int64_t now = timems(), end = now + runtime * 1000;
    char *readnode;
    cdb2_hndl_tp *db;
    int rc;
    int threadnum = (int)arg;

    rc = cdb2_open(&db, dbname, cltype, CDB2_RANDOM);
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "thd: open rc %d %s\n", rc, cdb2_errstr(db));
        cdb2_close(db);
        myexit(__func__, __LINE__, 1);
    }

    if (is_hasql)
    {
        rc = cdb2_run_statement(db, "set hasql on");
        if (rc) {
            tdprintf(stderr, db, __func__, __LINE__, "thd: set hasql on rc %d, %s\n", rc, cdb2_errstr(db));
            cdb2_close(db);
            myexit(__func__, __LINE__, 1);
        }
    }

    rc = cdb2_run_statement(db, "set transaction serializable");
    if (rc) {
        fprintf(stderr, "Set transaction serializable failed, rc=%d %s\n", rc, cdb2_errstr(db));
        exit(-1);
    }

    cdb2_set_max_retries(max_retries);

    if (debug_trace) {
        cdb2_set_debug_trace(db);
    }

    if (!(readnode = read_node(db))) {
        tdprintf(stderr, db, __func__, __LINE__, "Couldn't determine read node for thread\n");
    }
    else {
        if (debug_trace)
            tdprintf(stderr, db, __func__, __LINE__, "read node is %s\n", readnode);
    }

    while (now < end) {
        update(&db, readnode, threadnum);
        now = timems();
    }
    cdb2_close(db);
    if (readnode) free(readnode);
    return NULL;
}

int main (int argc, char *argv[])
{
    int rc, c, errors=0;
    argv0 = argv[0];

    while ((c = getopt(argc, argv, "d:G:T:t:PMm:Doi:r:j:"))!=EOF) {
            case 'd':
                dbname = optarg;
                break;
            case 'G':
                if (0 == strcasecmp(optarg, "partition")) {
                    which_events |= PARTITION_EVENT;
                }
                else if (0 == strcasecmp(optarg, "sigstop")) {
                    which_events |= SIGSTOP_EVENT;
                }
                else if (0 == strcasecmp(optarg, "clock")) {
                    which_events |= CLOCK_EVENT;
                }
                else {
                    fprintf(stderr, "Unknown test: %s\n", optarg);
                    errors++;
                }
                break;
            case 'T':
                nthreads = atoi(optarg);
                break;
            case 't':
                cltype = optarg;
                break;
            case 'M':
                partition_master = 1;
                break;
            case 'm':
                max_retries = atoi(optarg);
                break;
            case 'D':
                debug_trace = 1;
                break;
            case 'o':
                nthreads = 1;
                break;
            case 'r':
                runtime = atoi(optarg);
                break;
            case 'j':
                if ((c_file = fopen(optarg, "w")) == NULL) {
                    fprintf(stderr, "Error opening clojure output file: %s\n",
                            perror(errno));
                }
                break;
            default:
                fprintf(stderr, "Unknown option, '%c'\n", c);
                errors++;
                break;
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

    if (which_events == 0){
        fprintf(stderr, "NO TESTS SPECIFIED .. THIS SHOULD BE AN EASY RUN..\n");
    }

    srand(time(NULL) ^ getpid());

    struct nemesis *n = nemesis_open(dbname, cltype, partition_master ? 
            PARTITION_MASTER : 0);

    fixall(n);

    if (c_file) {
        fprintf(c_file, "[\n");
    }

    threads = malloc(sizeof(pthread_t) * nthreads);
    for (int i = 0; i < nthreads; i++) {
        rc = pthread_create(&threads[i], NULL, thd, i);
    }

    sleep(runtime / 2 - 1);

    if (which_events & PARTITION_EVENT) {
        breaknet(n);
    }
    if (which_events & SIGSTOP_EVENT) {
        signaldb(n, SIGSTOP, 0);
    }
    if (which_events & CLOCK_EVENT) {
        breakclocks(n, max_clock_skew); 
    } 

    sleep(runtime / 2 - 1);

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
        fprintf(c_file, "\n]");
    }

}
