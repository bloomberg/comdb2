#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <strings.h>
#include <cdb2api.h>
#include <errno.h>
#include <assert.h>
#include <stdint.h>
#include <unistd.h>

static char *argv0=NULL;
int num_failed = 0;
int num_succeeded = 0;

void usage(FILE *f)
{
    fprintf(f,"Usage: %s <cmd-line>\n",argv0);
    fprintf(f,"     -d <dbname>             -   set dbname.\n" );
    fprintf(f,"     -h                      -   this menu.\n" );
    fprintf(f,"     -i <iters>              -   set update iterations.\n" );
    fprintf(f,"     -t <thds>               -   set number of serial threads.\n" );
    fprintf(f,"     -s                      -   set transaction serial\n" );
    fprintf(f,"     -g                      -   set stage\n" );
    exit(1);
}

typedef struct config
{
    char        *dbname;
    char        *stage;
    int         iters;
    int         threads;
    int         fastinit;
    int         insert;
    int         update;
    int         verbose;
    int         wrcoll;
    int         serial;
}
config_t;

static config_t *default_config(void)
{
    config_t *c = malloc(sizeof(*c));
    bzero(c, sizeof(*c));
    c->iters = 10000;
    c->threads = 20;
    c->fastinit = 1;
    c->insert = 1;
    c->update = 1;
    c->verbose = 0;
    c->wrcoll = 0;
    c->serial = 0;
    return c;
}

unsigned int myrand(void)
{
    static int first = 1;
    static unsigned int seed;
    static unsigned int adds;

    if(first)
    {
        seed = time(NULL);
        adds = (unsigned int)pthread_self();
        first = 0;
    }

    seed = (seed << 7) ^ ((seed >> 25) + adds);
    adds = (adds << 7) ^ ((adds >> 25) + 0xbabeface);

    return seed;
}

typedef struct insert_thread
{
    config_t *c;
    int64_t id; 
}
insert_thread_t;

void *insert_records_thd(void *arg)
{
    insert_thread_t *i = (insert_thread_t *)arg;
    config_t *c = i->c;
    cdb2_hndl_tp *sqlh;
    char sql[1024];
    int ret;
    int64_t acct = 0;
    int64_t balance = 5000;

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);


    if ((ret = cdb2_open(&sqlh, c->dbname, c->stage, 0)) != 0)
    {
        fprintf(stderr, "%s error opening db, dbname=%s ret=%d\n",
                __func__, c->dbname, ret);
        exit(1);
    }

    snprintf(sql, sizeof(sql), "insert into t1 (id, acct, bal) values (@id, @acct, @bal)");

    for(acct = 0; acct<5; acct++) {
        cdb2_clearbindings(sqlh);
        if (cdb2_bind_param(sqlh, "id", CDB2_INTEGER, &i->id, sizeof(int64_t)) != 0)
        {
            fprintf(stderr, "error binding id column, ret=%d\n", ret);
            exit(1);
        }

        if (cdb2_bind_param(sqlh, "acct", CDB2_INTEGER, &acct, sizeof(int64_t)) != 0)
        {
            fprintf(stderr, "error binding acct column, ret=%d.\n", ret);
            exit(1);
        }

        if (cdb2_bind_param(sqlh, "bal", CDB2_INTEGER, &balance, sizeof(int64_t)) != 0)
        {
            fprintf(stderr, "error binding bal column, ret=%d.\n", ret);
            exit(1);
        }

        if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
        {
            fprintf(stderr, "error inserting record ret=%d %s.\n", ret, cdb2_errstr(sqlh));
            exit(1);
        }

        do
        {
            ret = cdb2_next_record(sqlh);
        }
        while(ret == CDB2_OK);
    }

    cdb2_close(sqlh);
    free(arg);
    return NULL;
}

int insert_records(config_t *c)
{
    pthread_attr_t attr;
    pthread_t *thds;
    int i, ret;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    thds = calloc(c->threads, sizeof(pthread_t));

    for(i = 0 ; i < c->threads ; i++)
    {
        insert_thread_t *ins = (insert_thread_t *) malloc(sizeof(*ins));
        ins->c = c;
        ins->id = i;
        if ((ret = pthread_create(&thds[i], &attr, insert_records_thd, ins)) != 0)
        {
            fprintf(stderr, "Error creating pthread, ret=%d\n", ret);
            exit(1);
        }
    }

    for(i = 0; i < c->threads ; i++)
    {
        pthread_join(thds[i], NULL);
    }

    pthread_attr_destroy(&attr);
    return 0;
}

typedef struct update_thread
{
    config_t *c;
    int iters;
    int thd;
}
update_thread_t;

void *update_records_thd(void *arg)
{
    update_thread_t *u = (update_thread_t *)arg;
    config_t *c = u->c;
    cdb2_hndl_tp *sqlh;
    char sql[1024];
    int ret, cnt, type;
    long long *ll, sum, curbal, newbal;
    char *ch;

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);


    if ((ret = cdb2_open(&sqlh, c->dbname, c->stage, 0)) != 0)
    {
        fprintf(stderr, "%s error opening db, dbname=%s ret=%d\n",
                __func__, c->dbname, ret);
        exit(1);
    }

     if(c->serial)
         snprintf(sql, sizeof(sql), "set transaction serial");
     else
         snprintf(sql, sizeof(sql), "set transaction snapisol");

     if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
     {
         fprintf(stderr, "error setting transaction level ret=%d.\n", ret);
         exit(1);
     }
     do
     {
         ret = cdb2_next_record(sqlh);
     }
     while(ret == CDB2_OK);

    for(cnt = 0; cnt < u->iters || u->iters < 0; cnt++)
    {
       int64_t id = myrand() % c->threads;
       int64_t acct = myrand() % 5;

        snprintf(sql, sizeof(sql), "begin");
        if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
        {
            fprintf(stderr, "error in begin, ret=%d.\n", ret);
            exit(1);
        }
        do
        {
            ret = cdb2_next_record(sqlh);
        }
        while(ret == CDB2_OK);

        snprintf(sql, sizeof(sql), "select sum(bal) from t1 where id=@id");

        cdb2_clearbindings(sqlh);
        if ((ret = cdb2_bind_param(sqlh, "id", CDB2_INTEGER, &id, sizeof(int64_t))) != 0)
        {
            fprintf(stderr, "error binding id column, ret=%d.\n", ret);
            exit(1);
        }
        if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
        {
            fprintf(stderr, "error selecting record id=%"PRId64", ret=%d.\n", id, ret);
            exit(1);
        }

        ret = cdb2_next_record(sqlh);
        if(ret != CDB2_OK) 
        {
            fprintf(stderr, "error from sqlhndl_next_record: %d\n", ret);
            exit(1);
        }

        if((type = cdb2_column_type(sqlh, 0)) == CDB2_CSTRING)
        {
            ch = cdb2_column_value(sqlh, 0);
            sum = atoll(ch);
        }
        else if(type == CDB2_INTEGER)
        {
            ll = (long long *)cdb2_column_value(sqlh, 0);
            sum = *ll;
        }
        else
        {
            fprintf(stderr, "Unexpected type from sqlhndl_next_record, %d\n", type);
            exit(1);
        }
        do
        {
           ret = cdb2_next_record(sqlh);
        }
        while(ret == CDB2_OK);

        if(sum < 20000) {
           fprintf(stderr, "id = %"PRId64", sum = %lld < 20,000\n", id, sum);
           exit(1);
        }

        snprintf(sql, sizeof(sql), "select bal from t1 where id=@id and acct=@acct");
        cdb2_clearbindings(sqlh);
        if ((ret = cdb2_bind_param(sqlh, "id", CDB2_INTEGER, &id, sizeof(int64_t))) != 0)
        {
            fprintf(stderr, "error binding id column, ret=%d.\n", ret);
            exit(1);
        }
        if ((ret = cdb2_bind_param(sqlh, "acct", CDB2_INTEGER, &acct, sizeof(int64_t))) != 0)
        {
            fprintf(stderr, "error binding acct column, ret=%d.\n", ret);
            exit(1);
        }
        if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
        {
            fprintf(stderr, "error selecting record id=%"PRId64" and acct=%"PRId64", ret=%d.\n", id, acct, ret);
            exit(1);
        }

        ret = cdb2_next_record(sqlh);
        if(ret != CDB2_OK) 
        {
            fprintf(stderr, "error from sqlhndl_next_record: %d\n", ret);
            exit(1);
        }
        
        if((type = cdb2_column_type(sqlh, 0)) == CDB2_CSTRING)
        {
            ch = cdb2_column_value(sqlh, 0);
            curbal = atoll(ch);
        }
        else if(type == CDB2_INTEGER)
        {
            ll = (long long *)cdb2_column_value(sqlh, 0);
            curbal = *ll;
        }
        else
        {
            fprintf(stderr, "Unexpaced type from sqlhndl_next_record, %d\n", type);
            exit(1);
        }
        do
        {
           ret = cdb2_next_record(sqlh);
        }
        while(ret == CDB2_OK);

        snprintf(sql, sizeof(sql), "update t1 set bal=@bal where id=@id and acct=@acct");
        cdb2_clearbindings(sqlh);
        if ((ret = cdb2_bind_param(sqlh, "id", CDB2_INTEGER, &id, sizeof(int64_t))) != 0)
        {
            fprintf(stderr, "error binding id column, ret=%d.\n", ret);
            exit(1);
        }
        if ((ret = cdb2_bind_param(sqlh, "acct", CDB2_INTEGER, &acct, sizeof(int64_t))) != 0)
        {
            fprintf(stderr, "error binding acct column, ret=%d.\n", ret);
            exit(1);
        }
        if (sum>20000) {
            newbal = (curbal<=sum-20000)? 0 : (curbal+20000-sum);
            if ((ret = cdb2_bind_param(sqlh, "bal", CDB2_INTEGER, &newbal, sizeof(int64_t))) != 0)
            {
                fprintf(stderr, "error binding bal column, ret=%d.\n", ret);
                exit(1);
            }
        }
        else {
            newbal = curbal + (myrand() % 2000);
            if ((ret = cdb2_bind_param(sqlh, "bal", CDB2_INTEGER, &newbal, sizeof(int64_t))) != 0)
            {
                fprintf(stderr, "error binding bal column, ret=%d.\n", ret);
                exit(1);
            }
        }
        if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
        {
            fprintf(stderr, "error selecting record id=%"PRId64" and acct=%"PRId64", ret=%d.\n", id, acct, ret);
            exit(1);
        }
        do
        {
            ret = cdb2_next_record(sqlh);
        }
        while(ret == CDB2_OK);

        snprintf(sql, sizeof(sql), "commit");
        cdb2_clearbindings(sqlh);
        ret = cdb2_run_statement(sqlh, sql);
        if (ret)
           num_failed++;
        else
           num_succeeded++;
        if(ret == 0){
            do
            {
                ret = cdb2_next_record(sqlh);
            }
            while(ret == CDB2_OK);
        }
    }
    
    cdb2_close(sqlh);
    free(arg);
    return NULL;
}

int update_records(config_t *c)
{
    pthread_attr_t attr;
    pthread_t *thds;
    int i, ret;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    thds = calloc(c->threads, sizeof(pthread_t));

    for(i = 0 ; i < c->threads ; i++)
    {
        update_thread_t *upd = (update_thread_t *) malloc(sizeof(*upd));
        upd->c = c;
        upd->iters = c->iters;
        upd->thd = i;

        if ((ret = pthread_create(&thds[i], &attr, update_records_thd, upd)) != 0)
        {
            fprintf(stderr, "Error creating pthread, ret=%d\n", ret);
            exit(1);
        }
    }

    for(i = 0; i < c->threads ; i++)
    {
        pthread_join(thds[i], NULL);
    }

    pthread_attr_destroy(&attr);
    return 0;
}

int main(int argc,char *argv[])
{
    config_t *c;
    int err = 0, opt;
    char *stage = "default";

    argv0 = argv[0];
    c = default_config();

    /* char *optarg=argument, int optind = argv index  */
    while ((opt = getopt(argc,argv,"d:h:i:t:sg:"))!=EOF)
    {
        switch(opt)
        {
            case 'd':
                c->dbname = optarg;
                break;

            case 'h':
                usage(stdout);
                break;

            case 'i':
                c->iters = atoi(optarg);
                break;

            case 't':
                c->threads = atoi(optarg);
                break;

            case 's':
                c->serial = 1;
                break;

            case 'g':
                stage = optarg;
                break;

            default:
                fprintf(stderr, "Unknown flag, '%c'.\n", optopt);
                err++;
                break;
        }
    }

    /* Make sure dbname is set. */
    if( NULL == c->dbname)
    {
        fprintf( stderr, "dbname is unset.\n");
        err++;
    }
    c->stage = stage;

    /* Punt before pekludge if there were errors. */
    if( err )
    {
        usage(stderr);
        exit(1);
    }

    // start test here
    insert_records(c);
    update_records(c);
    fprintf(stderr, "Number of successful commits: %d\n", num_succeeded);
    fprintf(stderr, "Number of failed commits: %d\n", num_failed);

    return 0;
}
