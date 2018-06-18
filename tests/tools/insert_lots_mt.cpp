#include <string>
#include <vector>
#include <limits.h>
#include <string.h>
#include <arpa/inet.h>
#include <cdb2api.h>
#include <time.h>
#include <sstream>
#include <iostream>

static std::string quote("");
static std::string comma(",");
static std::string space(" ");
static std::string at("@");

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
    if (rc != 0) {
        fprintf(stderr, "Error: cdb2_open failed: %d\n", rc);
        exit(1);
    }

    thr_info_t *tinfo = (thr_info_t *)arg;
    int i = tinfo->thrid;
    
    std::ostringstream ss;
    ss << "insert into " << table << "(i, j) values (@i, @j)" ;
    std::string s = ss.str();

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
    }

    cdb2_close(db);
    std::cout << "Done thr " << i << std::endl;
    return NULL;
}

int main(int argc, char *argv[])
{
    if(argc < 5) {
        fprintf(stderr, "Usage %s DBNAME NUMTHREADS CNTPERTHREAD ITERATIONS\n", argv[0]);
        return 1;
    }

    dbname = argv[1];
    unsigned int numthreads = atoi(argv[2]);
    unsigned int cntperthread = atoi(argv[3]);
    unsigned int iterations = atoi(argv[4]);

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);
    else
        fprintf(stderr, "Error: no config was set\n");

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
