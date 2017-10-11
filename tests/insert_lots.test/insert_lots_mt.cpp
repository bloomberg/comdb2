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
        fprintf(stderr, "cdb2_run_statement failed: %d %s\n", rc,
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
        fprintf(stderr, "cdb2_next_record failed: %d %s\n", rc, cdb2_errstr(h));
    return rc;
}


int runtag(cdb2_hndl_tp *h, std::string &sql, std::vector<int> &types)
{
    int rc =
        cdb2_run_statement_typed(h, sql.c_str(), types.size(), types.data());
    if (rc != 0) {
        fprintf(stderr, "cdb2_run_statement_typed failed: %d %s\n", rc,
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
        fprintf(stderr, "cdb2_next_record failed: %d %s\n", rc, cdb2_errstr(h));
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
unsigned int numthreads;
unsigned int cntperthread;
const char *table = "t1";


void *thr(void *arg)
{
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);
    else
        fprintf(stderr, "no config was set\n");


    cdb2_hndl_tp *db;
    int rc = cdb2_open(&db, dbname, "default", 0);
    if (rc != 0) {
        fprintf(stderr, "cdb2_open failed: %d\n", rc);
    }

    int i = (unsigned long long int) arg;
    
    std::ostringstream ss;
    ss << "insert into " << table << "(i, j) values (@i, @j)" ;
    std::string s = ss.str();

    // insert records with bind params
    for (unsigned int j = 0; j < cntperthread; j++) {
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
        if((j & 0x0f) == 0) std::cout << "Thr " << i << " Items " << j << std::endl;
    }

    cdb2_close(db);
    std::cout << "Done thr " << i << std::endl;
    return NULL;
}

int main(int argc, char *argv[])
{
    if(argc < 4) {
        fprintf(stderr, "Usage %s DBNAME NUMTHREADS CNTPERTHREAD\n", argv[0]);
        return 1;
    }

    dbname = argv[1];
    numthreads = atoi(argv[2]);
    cntperthread = atoi(argv[3]);

    fprintf(stderr, "starting %d threads\n", numthreads);
    pthread_t *t = (pthread_t *) alloca(sizeof(pthread_t) * numthreads);
    /* create threads */
    for (unsigned long long i = 0; i < numthreads; ++i)
        pthread_create(&t[i], NULL, thr, (void *)i);

    void *r;
    for (unsigned int i = 0; i < numthreads; ++i)
        pthread_join(t[i], &r);
    std::cout << "Done Main" << std::endl;
    return 0;
}
