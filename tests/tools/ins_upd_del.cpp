#include <string>
#include <vector>
#include <limits.h>
#include <string.h>
#include <arpa/inet.h>
#include <cdb2api.h>
#include <cdb2api_test.h>
#include <unistd.h>
#include <time.h>
#include <sstream>
#include <iostream>
#include <getopt.h>
#include <assert.h>
#include <signal.h>

int gbl_iterations;
const char * gbl_tbl = "ttt";
static std::string quote("");
static std::string comma(",");
static std::string space(" ");
static std::string at("@");
static std::string begin("begin");
static std::string commit("commit");
static std::string rollback("rollback");
static std::string sethasql("set hasql on");


int runsql(cdb2_hndl_tp *h, std::string &sql, const char *node)
{
    int rc = cdb2_run_statement(h, sql.c_str());
    //printf("Here: node %s cdb2_run_statement: %s\n", node, sql.c_str());

    if (rc != 0) {
        fprintf(stderr, "Error: cdb2_run_statement failed: %d %s (%s)\n", rc,
                cdb2_errstr(h), sql.c_str());
        exit(1);
    }

    int cnt = -1;
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

int runclustersql(cdb2_hndl_tp *h, std::vector<cdb2_hndl *> &db_node_hndls, std::vector<std::string> &db_node_names, std::string &sql)
{
    int cnt = runsql(h, sql, "orig");
    for(int i = 0; i < db_node_hndls.size(); i++) {
        int lclcnt = runsql(db_node_hndls[i], sql, db_node_names[i].c_str());
        if (lclcnt != cnt) {
            fprintf(stderr, "Error: runsql for node i returned %d instead of %d %s\n", lclcnt, cnt, sql.c_str());
            exit(1);
        }
    }
    return cnt;
}


std::string run_ins_sp(cdb2_hndl_tp *h)
{
    const char * sql = "exec procedure txnins('ttt')";
    int rc = cdb2_run_statement(h, sql);
    if (rc != 0) {
        fprintf(stderr, "Error: cdb2_run_statement failed: %d %s (%s)\n", rc, cdb2_errstr(h), sql);
        exit(1);
    }
    rc = cdb2_next_record(h);
    assert(rc == CDB2_OK);
    assert(cdb2_numcolumns(h) == 1);
    if(cdb2_column_type(h, 0) != CDB2_BLOB) {
        fprintf(stderr, "Error: coltype is: %d nstead of %d\n", cdb2_column_type(h, 0), CDB2_BLOB);
        exit(1);
    }

    unsigned char *v = (unsigned char *)cdb2_column_value(h, 0);
    int len = cdb2_column_size(h, 0);
    std::string ret = "x'";

    for (int i = 0; i < len; i++) {
        char arr[3] = {0};
        snprintf(arr, sizeof(arr), "%02x", v[i]);
        ret += arr;
    }
    ret += "'";

    rc = cdb2_next_record(h);
    assert(rc == CDB2_OK_DONE);

    return ret;
}

char * dbname;

typedef struct {
    unsigned int thrid;
    unsigned int count;
} thr_info_t;

void create_tbl()
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

    //runsql(db, "drop table if exists " + gbl_tbl);
    //runsql(db, "create table " + gbl_tbl + "(id int state int instime datetime  DEFAULT( current_timestamp ) updtime datetime)";
    //runsql(db, "create table " + gbl_tbl + "(id int state int instime datetime  DEFAULT( current_timestamp ) updtime datetime)";
    cdb2_close(db);
}

int get_nodes(cdb2_hndl_tp *h, std::vector<cdb2_hndl_tp *> &host_handles, std::vector<std::string> &node_names)
{
    const char * sql = "select host from comdb2_cluster";
    int rc = cdb2_run_statement(h, sql);
    if (rc != 0) {
        fprintf(stderr, "Error: cdb2_run_statement failed: %d %s (%s)\n", rc, cdb2_errstr(h), sql);
        exit(1);
    }

    rc = cdb2_next_record(h);
    assert(rc == CDB2_OK);
    assert(cdb2_numcolumns(h) == 1);
    if(cdb2_column_type(h, 0) != CDB2_CSTRING) {
        fprintf(stderr, "Error: coltype is: %d nstead of %d\n", cdb2_column_type(h, 0), CDB2_CSTRING);
        exit(1);
    }

    do {
        const char *v = (const char *)cdb2_column_value(h, 0);
        node_names.push_back(v);
        cdb2_hndl_tp *hndl;
        host_handles.push_back(hndl);
        printf("node %s\n", v);
        rc = cdb2_open(&(host_handles.back()), dbname, v, CDB2_DIRECT_CPU);
        if (rc != 0) {
            fprintf(stderr, "Error: cdb2_open failed to open direct to %s : %d\n", v, rc);
            exit(1);
        }
        rc = cdb2_next_record(h);
    } while(rc == CDB2_OK);

    assert(rc == CDB2_OK_DONE);
    return 0;
}

void *thr(void *arg)
{
    cdb2_hndl_tp *db; //main handle to do the writes
    std::vector<cdb2_hndl_tp *> db_node_hndls;
    std::vector<std::string> db_node_names;

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

    get_nodes(db, db_node_hndls, db_node_names);
    //runsql(db, sethasql);
    

    for (unsigned int j = 0; j < tinfo->count; j++) {
        //usleep(10); // slow down just a bit
        gbl_iterations++; // don't need absolute precision

        std::string id = run_ins_sp(db);
        std::string sel ="select state from ttt where id=" + id;
        int st = runclustersql(db, db_node_hndls, db_node_names, sel);
        if (st != 1) {
            fprintf(stderr, "Error: state is %d but should be %d\n", st, 1);
            exit(1);
        }

        for (int i = 2; i < 10; i++) {
            std::ostringstream ss;
            ss << "update ttt set updtime=now(), state = " << i << " where id=" + id;
            std::string upd = ss.str();
            runsql(db, upd, "update");

            st = runclustersql(db, db_node_hndls, db_node_names, sel);
            if (st != i) {
                fprintf(stderr, "Error: state is %d but should be %d\n", st, i);
                exit(1);
            }
        }

        std::string del = "delete from ttt where id=" + id;
        runsql(db, del, "delete");

        std::string countsql ="select count(*) from ttt where id=" + id;
        int cnt = runclustersql(db, db_node_hndls, db_node_names, countsql);
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

void int_handler(int signum)
{
    printf("Proccessed %d Iterations\n", gbl_iterations);
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
    while ((c = getopt_long(argc, argv, "d:n:i:?", long_options, &index)) != -1) {
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

    fprintf(stderr, "starting %d threads\n", numthreads);

    struct sigaction sact = {0};
    sact.sa_handler = int_handler;
    sigaction(SIGUSR1, &sact, NULL);

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
