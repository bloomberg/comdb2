#include <string>
#include <vector>
#include <limits.h>
#include <string.h>
#include <arpa/inet.h>
#include <cdb2api.h>
#include <unistd.h>
#include <time.h>
#include <sstream>
#include <iostream>
#include <getopt.h>

static std::string quote("");
static std::string comma(",");
static std::string space(" ");
static std::string at("@");
static std::string begin("begin");
static std::string commit("commit");
static std::string rollback("rollback");
static std::string sethasql("set hasql on");

int runsql(cdb2_hndl_tp *h, std::string &sql)
{
    int rc = cdb2_run_statement(h, sql.c_str());
    //printf("Here: cdb2_run_statement: %s\n", sql.c_str());

    if (rc != 0) {
        fprintf(stderr, "Error: cdb2_run_statement failed: %d %s (%s)\n", rc,
                cdb2_errstr(h), sql.c_str());
        exit(1);
    }

    int cnt = 0;
    rc = cdb2_next_record(h);
    while (rc == CDB2_OK) {
        int n = cdb2_numcolumns(h);
        const char *c = "";
        for (int i = 0; i < n; ++i) {
            if(cdb2_column_type(h, i) == CDB2_CSTRING) {
                char * v = (char *)cdb2_column_value(h, i);
                printf("%s%s", c, v);
                c = ", ";
            }
            else if(cdb2_column_type(h, i) == CDB2_INTEGER) {
                cnt = *(int *)cdb2_column_value(h, i);
            }
        }
        if(*c) printf("\n");
        rc = cdb2_next_record(h);
    }

    if (rc == CDB2_OK_DONE)
        rc = 0;
    else {
        fprintf(stderr, "Error: cdb2_next_record failed: %d %s\n", rc, cdb2_errstr(h));
        exit(1);
    }
    return cnt;
}


char * dbname;

typedef struct {
    unsigned int thrid;
    unsigned int count;
} thr_info_t;

void create_tbls(int N)
{
    cdb2_hndl_tp *db;
    int rc = cdb2_open(&db, dbname, "default", 0);
    if (rc != 0)
        rc = cdb2_open(&db, dbname, "local", 0);
    if (rc != 0) {
        fprintf(stderr, "Error: cdb2_open failed: %d\n", rc);
        exit(1);
    }
    cdb2_set_max_retries(999999);

    for(int i = 0 ; i < N; i++) {
        usleep(500);
        {
            std::ostringstream ss;
            ss << "drop table if exists tt" << i;
            std::string s = ss.str();
            runsql(db, s);
        }
        usleep(500);
        {
            std::ostringstream ss;
            ss << "create table tt" << i << "(i int )";
            std::string s = ss.str();
            runsql(db, s);
        }
    }
    cdb2_close(db);
}

void *thr(void *arg)
{
    cdb2_hndl_tp *db;
    int rc = cdb2_open(&db, dbname, "default", 0);
    if (rc != 0)
        rc = cdb2_open(&db, dbname, "local", 0);
    if (rc != 0) {
        fprintf(stderr, "Error: cdb2_open failed: %d\n", rc);
        exit(1);
    }
    cdb2_set_max_retries(999999);

    thr_info_t *tinfo = (thr_info_t *)arg;
    int i = tinfo->thrid;

    runsql(db, sethasql);
    int N = 100;

    std::string ins;
    std::string counts;
    std::string del;
    {
        std::ostringstream ss;
        ss << "insert into tt" << i << " select * from generate_series limit " << N;
        ins = ss.str();
    }
    {
        std::ostringstream ss;
        ss << "select count(*) from tt" << i;
        counts = ss.str();
    }
    {
        std::ostringstream ss;
        ss << "delete from tt" << i;
        del = ss.str();
    }

    for (unsigned int j = 0; j < tinfo->count; j++) {
        usleep(10); // slow down just a bit
        runsql(db, ins);
        int cnt = runsql(db, counts);
        if (cnt != N) {
            fprintf(stderr, "Error: count is %d but should be %d\n", cnt, N);
            exit(1);
        }
        runsql(db, del);
        cnt = runsql(db, counts);
        if (cnt != 0) {
            fprintf(stderr, "Error: count is %d but should be 0\n", cnt);
            exit(1);
        }

    }

    cdb2_close(db);
    std::cout << "Done thr " << i << std::endl;
    return NULL;
}

void usage(const char *p, const char *err) {
    fprintf(stderr, "%s\n", err);
    fprintf(stderr, "Usage %s --dbname DBNAME --numthreads NUMTHREADS --iterations ITERATIONS \n", p);
    exit(1);
}

int main(int argc, char *argv[])
{
    int numthreads = 0;
    int iterations = 0;

    if(argc < 5)
        usage(argv[0], "Required parameters were NOT provided"); //exit too

    static struct option long_options[] =
    {
        //{char *name; int has_arg; int *flag; int val;}
        {"dbname", required_argument, NULL, 'd'},
        {"numthreads", required_argument, NULL, 'n'},
        {"iterations", required_argument, NULL, 'i'},
        {NULL, 0, NULL, 0}
    };

    int c;
    int index;
    while ((c = getopt_long(argc, argv, "d:n:c:i:t:rd?", long_options, &index)) != -1) {
        //printf("c '%c' %d index %d optarg '%s'\n", c, c, index, optarg);
        switch(c) {
            case 'd': dbname = strdup(optarg); break;
            case 'n': numthreads = atoi(optarg); break;
            case 'i': iterations = atoi(optarg); break;
            case '?':  break;
            default: break;
        }
    }

    if (optind < argc) {
        dbname = strdup(argv[optind]);
    }

    if (!dbname)
        usage(argv[0], "Parameter dbname is not set"); //exit too
    if (numthreads < 1)
        usage(argv[0], "Parameter numthreads is not set"); //exit too
    if (iterations < 1)
        usage(argv[0], "Parameter iterations is not set"); //exit too

    //printf("%s %d %d\n", dbname, numthreads, iterations);//

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);
    else
        fprintf(stderr, "Warning: no config was set from getenv(\"CDB2_CONFIG\")\n");

    pthread_t *t = (pthread_t *) malloc(sizeof(pthread_t) * numthreads);
    thr_info_t *tinfo = (thr_info_t *) malloc(sizeof(thr_info_t) * numthreads);

    //create_tbls(numthreads);

    fprintf(stderr, "starting %d threads\n", numthreads);

    /* create threads */
    for (unsigned long long i = 0; i < numthreads; ++i) {
        tinfo[i].thrid = i;
        tinfo[i].count = iterations;
        pthread_create(&t[i], NULL, thr, (void *)&tinfo[i]);
    }

    void *r;
    for (unsigned int i = 0; i < numthreads; ++i)
        pthread_join(t[i], &r);

    std::cout << "Done Main" << std::endl;
    return 0;
}
