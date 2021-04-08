#include <string>
#include <vector>
#include <limits.h>
#include <string.h>
#include <arpa/inet.h>
#include <cdb2api.h>
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

unsigned int gbl_transize;
enum { G_COMMIT, G_ROLLBACK, G_DISCONNECT, G_CLOSEOPEN };
unsigned int atcommit = G_COMMIT;

int runsql(cdb2_hndl_tp *h, std::string &sql)
{
    int rc = cdb2_run_statement(h, sql.c_str());
    if (rc != 0) {
        fprintf(stderr, "Error: cdb2_run_statement failed: %d %s\n", rc,
                cdb2_errstr(h));
        return rc;
    }

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
        }
        if(*c) printf("\n");
        rc = cdb2_next_record(h);
    }

    if (rc == CDB2_OK_DONE)
        rc = 0;
    else
        fprintf(stderr, "Error: cdb2_next_record failed: %d %s\n", rc, cdb2_errstr(h));
    return rc;
}


int runtag(cdb2_hndl_tp *h, std::string &sql, std::vector<int> &types)
{
    int rc =
        cdb2_run_statement_typed(h, sql.c_str(), types.size(), types.data());
    if (rc != 0) {
        fprintf(stderr, "Error: cdb2_run_statement_typed failed: %d %s\n", rc,
                cdb2_errstr(h));
        return rc;
    }

    rc = cdb2_next_record(h);
    while (rc == CDB2_OK) {
        int n = cdb2_numcolumns(h);
        int i;
        const char *c = "";
        for (i = 0; i < n; ++i) {
            printf("%s%s", c, (char *)cdb2_column_value(h, i));
            c = ", ";
        }
        rc = cdb2_next_record(h);
    }

    if (rc == CDB2_OK_DONE)
        rc = 0;
    else
        fprintf(stderr, "Error: cdb2_next_record failed: %d %s\n", rc, cdb2_errstr(h));
    return rc;
}

void add_param(std::string &sp, std::string &sql, std::vector<int> &types, std::string name,
               std::string extra1 = "", std::string extra2 = "")
{
    sp += comma + space + quote + at + name + quote;
    sql += comma + space + extra1 + at + name + extra2;
    types.push_back(CDB2_CSTRING);
}
char * dbname;
const char *table = "t1";

typedef struct {
    unsigned int thrid;
    unsigned int start;
    unsigned int count;
} thr_info_t;

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

    thr_info_t *tinfo = (thr_info_t *)arg;
    int i = tinfo->thrid;
    
    std::ostringstream ss;
    ss << "insert into " << table << "(i, j) values (@i, @j)" ;
    std::string s = ss.str();

    if (gbl_transize > 1)
        runsql(db, begin);

    // insert records with bind params
    int count = 0;
    for (unsigned int j = tinfo->start; j < tinfo->start + tinfo->count; j++) {
        std::vector<int> types;
        if(cdb2_bind_param(db, "i", CDB2_INTEGER, &i, sizeof(i)) )
            fprintf(stderr, "Error binding i.\n");
        if(cdb2_bind_param(db, "j", CDB2_INTEGER, &j, sizeof(j)) )
            fprintf(stderr, "Error binding j.\n");
        types.push_back(CDB2_INTEGER);
        types.push_back(CDB2_INTEGER);

        int rc = runsql(db, s);
        if(rc) return NULL;

        //runtag(db, s, types);
        cdb2_clearbindings(db);
        if((++count & 0xff) == 0) std::cout << "Thr " << i << " Items " << count << std::endl;
        if (gbl_transize > 1 && (count % gbl_transize) == 0) {
            switch (atcommit) {
            case G_COMMIT: runsql(db, commit); break;
            case G_ROLLBACK: runsql(db, rollback); break;
            case G_DISCONNECT: 
                return NULL;
            case G_CLOSEOPEN: 
                cdb2_close(db);
                rc = cdb2_open(&db, dbname, "default", 0);
                if (rc != 0)
                    rc = cdb2_open(&db, dbname, "local", 0);
                if (rc != 0) {
                    fprintf(stderr, "Error: cdb2_open failed: %d\n", rc);
                    exit(1);
                }
                break;
            default: abort();
            }

            runsql(db, begin);
        }
    }
    if (gbl_transize > 1)
        runsql(db, commit);

    cdb2_close(db);
    std::cout << "Done thr " << i << std::endl;
    return NULL;
}

void usage(const char *p, const char *err) {
    fprintf(stderr, "%s\n", err);
    fprintf(stderr, "Usage %s --dbname DBNAME --numthreads NUMTHREADS --cntperthread CNTPERTHREAD\n"
                    "--iterations ITERATIONS [--transize TRANSIZE] [--atcommit commit|rollback|disconnect]\n", p);
    exit(1);
}

int main(int argc, char *argv[])
{
    int numthreads = 0;
    int cntperthread = 0;
    int iterations = 0;

    if(argc < 5)
        usage(argv[0], "Required parameters were NOT provided"); //exit too

    static struct option long_options[] =
    {
        //{char *name; int has_arg; int *flag; int val;}
        {"dbname", required_argument, NULL, 'd'},
        {"numthreads", required_argument, NULL, 'n'},
        {"cntperthread", required_argument, NULL, 'c'},
        {"iterations", required_argument, NULL, 'i'},
        {"transize", required_argument, NULL, 't'},
        {"atcommit", required_argument, NULL, 'm'}, //rollback instead of commit
        {NULL, 0, NULL, 0}
    };

    int c;
    int index;
    while ((c = getopt_long(argc, argv, "d:n:c:i:t:rd?", long_options, &index)) != -1) {
        //printf("c '%c' %d index %d optarg '%s'\n", c, c, index, optarg);
        switch(c) {
            case 'd': dbname = strdup(optarg); break;
            case 'n': numthreads = atoi(optarg); break;
            case 'c': cntperthread = atoi(optarg); break;
            case 'i': iterations = atoi(optarg); break;
            case 't': gbl_transize = atoi(optarg); break;
            case 'm': 
                  if (strcmp(optarg, "rollback") == 0)
                      atcommit = G_ROLLBACK;
                  else if (strcmp(optarg, "disconnect") == 0)
                      atcommit = G_DISCONNECT;
                  break;
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
    if (cntperthread < 1)
        usage(argv[0], "Parameter cntperthread is not set"); //exit too
    if (iterations < 1)
        usage(argv[0], "Parameter iterations is not set"); //exit too

    //printf("%s %d %d %d %d\n", dbname, numthreads, cntperthread, iterations, transize);//

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);
    else
        fprintf(stderr, "Warning: no config was set from getenv(\"CDB2_CONFIG\")\n");

    pthread_t *t = (pthread_t *) malloc(sizeof(pthread_t) * numthreads);
    thr_info_t *tinfo = (thr_info_t *) malloc(sizeof(thr_info_t) * numthreads);
    for(unsigned int it = 0; it < iterations; it++) {
        fprintf(stderr, "starting %d threads\n", numthreads);

        /* create threads */
        for (unsigned long long i = 0; i < numthreads; ++i) {
            tinfo[i].thrid = i;
            tinfo[i].start = it * cntperthread + 1;
            tinfo[i].count = cntperthread;
            pthread_create(&t[i], NULL, thr, (void *)&tinfo[i]);
        }

        void *r;
        for (unsigned int i = 0; i < numthreads; ++i)
            pthread_join(t[i], &r);
    }
    std::cout << "Done Main" << std::endl;
    return 0;
}
