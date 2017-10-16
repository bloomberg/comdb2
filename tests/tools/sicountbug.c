#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <strings.h>
#include <cdb2api.h>
#include <epochlib.h>
#include <errno.h>
#include <assert.h>
#include <signal.h>
#include <getopt.h>

static char *argv0=NULL;

void usage(FILE *f)
{
    fprintf(f,"Usage: %s <cmd-line>\n",argv0);
    fprintf(f,"     -d <dbname>             -   set dbname.\n" );
    fprintf(f,"     -h                      -   this menu.\n" );
    fprintf(f,"     -r <records>            -   number of records.\n");
    fprintf(f,"     -i <iters>              -   set update iterations.\n" );
    fprintf(f,"     -t <thds>               -   set number of serial threads.\n" );
    fprintf(f,"     -o                      -   snapisol test only.\n" );
    fprintf(f,"     -s                      -   set transaction serial.\n" );
    fprintf(f,"     -g                      -   set stage\n" );
    fprintf(f,"     -D                      -   enable debug trace\n");
    exit(1);
}

typedef struct config
{
    char        *dbname;
    int         records;
    char        *stage;
    int         iters;
    int         threads;
    int         fastinit;
    int         insert;
    int         update;
    int         verbose;
    int         debug;
    int         wrcoll;
    int         serial;
    int         snapisol_only;
}
config_t;

static config_t *default_config(void)
{
    config_t *c = malloc(sizeof(*c));
    bzero(c, sizeof(*c));
    c->iters = 10000;
    c->records = 2000;
    c->threads = 10;
    c->fastinit = 1;
    c->insert = 1;
    c->update = 1;
    c->verbose = 0;
    c->debug = 0;
    c->wrcoll = 0;
    c->serial = 0;
    c->snapisol_only = 0;
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
    int64_t id = 0;

    if ((ret = cdb2_open(&sqlh, c->dbname, c->stage, 0)) != 0)
    {
        fprintf(stderr, "Error getting sql handle, ret=%d\n", ret);
        exit(1);
    }

    if (c->debug)
        cdb2_set_debug_trace(sqlh);

    snprintf(sql, sizeof(sql), "insert into t1 (id, acct, bal) values (@id, @acct, @bal)");
    for(id = 0; id< 20; id++) {

        for(acct = 0; acct<5; acct++) {
            cdb2_clearbindings(sqlh);
            if ((ret = cdb2_bind_param(sqlh, "id", CDB2_INTEGER, &id, sizeof(int64_t))) != 0)
            {
                fprintf(stderr, "error binding id column, ret=%d\n", ret);
                exit(1);
            }

            if ((ret = cdb2_bind_param(sqlh, "acct", CDB2_INTEGER, &acct, sizeof(int64_t))) != 0)
            {
                fprintf(stderr, "error binding acct column, ret=%d.\n", ret);
                exit(1);
            }

            int64_t bal = 5000;
            if ((ret = cdb2_bind_param(sqlh, "bal", CDB2_INTEGER, &bal, sizeof(int64_t))) != 0)
            {
                fprintf(stderr, "error binding bal column, ret=%d.\n", ret);
                exit(1);
            }

            if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
            {
                fprintf(stderr, "error inserting record ret=%d.\n", ret);
                exit(1);
            }

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

int insert_records(config_t *c)
{
    int i, ret;

    insert_thread_t *ins = (insert_thread_t *) malloc(sizeof(*ins));
    ins->c = c;
    insert_records_thd(ins);
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
    cdb2_hndl_tp *sqlh = NULL;
    char sql[1024];
    int ret, cnt, type, nacct, nacct2, nacct3;
    long long *ll, sum, sum2, sum3, curbal, newbal;
    char *ch;

    if ((ret = cdb2_open(&sqlh, c->dbname, c->stage, 0)) != 0)
    {
        fprintf(stderr, "Error opening db, ret=%d\n", ret);
        exit(1);
    }

    if (c->debug) 
        cdb2_set_debug_trace(sqlh);

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
        int64_t id = myrand() % 20;
        int64_t acct = myrand() % 5;

        snprintf(sql, sizeof(sql), "begin");
        if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
        {
            fprintf(stderr, "td %u error in begin, ret=%d, %s.\n", 
                    (uint32_t) pthread_self(), ret, cdb2_errstr(sqlh));
            exit(1);
        }
        do
        {
            ret = cdb2_next_record(sqlh);
        }
        while(ret == CDB2_OK);

        snprintf(sql, sizeof(sql), "select sum(bal), count(bal) from t1 where id=@id");
        cdb2_clearbindings(sqlh);
        if ((ret = cdb2_bind_param(sqlh, "id", CDB2_INTEGER, &id, sizeof(int64_t))) != 0)
        {
            fprintf(stderr, "error binding id column, ret=%d.\n", ret);
            exit(1);
        }
        if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
        {
            fprintf(stderr, "error selecting record id=%ld, ret=%d.\n", id, ret);
            exit(1);
        }

        /* In single-node mode, this returns CDB2_OK rather than CDB2_OK_DONE.. 
         * this is wrong */
        ret = cdb2_next_record(sqlh);
        if(ret != CDB2_OK) 
        {
            fprintf(stderr, "error from cdb2_next_record: %d line %d\n", ret, __LINE__);
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
            fprintf(stderr, "Unexpected type from cdb2_next_record, %d\n", type);
            exit(1);
        }
        if((type = cdb2_column_type(sqlh, 1)) == CDB2_CSTRING)
        {
            ch = cdb2_column_value(sqlh, 1);
            nacct = atoll(ch);
        }
        else if(type == CDB2_INTEGER)
        {
            ll = (long long *)cdb2_column_value(sqlh, 1);
            nacct = *ll;
        }
        else
        {
            fprintf(stderr, "Unexpaced type from cdb2_next_record, %d\n", type);
            exit(1);
        }
        do
        {
            ret = cdb2_next_record(sqlh);
        }
        while(ret == CDB2_OK);

        if(nacct < 5) 
        {
            snprintf(sql, sizeof(sql), "select sum(bal), count(bal) from t1 where id=@id");
            if ((ret = cdb2_bind_param(sqlh, "id", CDB2_INTEGER, &id, sizeof(int64_t))) != 0)
            {
                fprintf(stderr, "error binding id column, ret=%d.\n", ret);
                exit(1);
            }
            if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
            {
                fprintf(stderr, "error selecting record id=%ld, ret=%d.\n", id, ret);
                exit(1);
            }

            ret = cdb2_next_record(sqlh);
            if(ret != CDB2_OK) 
            {
                fprintf(stderr, "error from cdb2_next_record: %d line %d\n", ret, __LINE__);
                exit(1);
            }
            if((type = cdb2_column_type(sqlh, 0)) == CDB2_CSTRING)
            {
                ch = cdb2_column_value(sqlh, 0);
                sum2 = atoll(ch);
            }
            else if(type == CDB2_INTEGER)
            {
                ll = (long long *)cdb2_column_value(sqlh, 0);
                sum2 = *ll;
            }
            else
            {
                fprintf(stderr, "Unexpected type from cdb2_next_record, %d\n", type);
                exit(1);
            }
            if((type = cdb2_column_type(sqlh, 1)) == CDB2_CSTRING)
            {
                ch = cdb2_column_value(sqlh, 1);
                nacct2 = atoll(ch);
            }
            else if(type == CDB2_INTEGER)
            {
                ll = (long long *)cdb2_column_value(sqlh, 1);
                nacct2 = *ll;
            }
            else
            {
                fprintf(stderr, "Unexpected type from cdb2_next_record, %d\n", type);
                exit(1);
            }
            do
            {
                ret = cdb2_next_record(sqlh);
            }
            while(ret == CDB2_OK);

            sum3 = 0;
            for(nacct3 = 0; nacct3 < 5; nacct3++) {
                snprintf(sql, sizeof(sql), "select bal from t1 where id=@id and acct=@acct");
                if ((ret = cdb2_bind_param(sqlh, "id", CDB2_INTEGER, &id, sizeof(int64_t))) != 0)
                {
                    fprintf(stderr, "error binding id column, ret=%d.\n", ret);
                    exit(1);
                }
                if ((ret = cdb2_bind_param(sqlh, "acct", CDB2_INTEGER, &nacct3, sizeof(int64_t))) != 0)
                {
                    fprintf(stderr, "error binding acct column, ret=%d.\n", ret);
                    exit(1);
                }
                if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
                {
                    fprintf(stderr, "error selecting record id=%ld and acct=%ld, ret=%d.\n", id, acct, ret);
                    exit(1);
                }

                ret = cdb2_next_record(sqlh);
                if(ret != CDB2_OK) 
                {
                    fprintf(stderr, "error from cdb2_next_record: %d line %d\n", ret, __LINE__);
                    exit(1);
                }

                if((type = cdb2_column_type(sqlh, 0)) == CDB2_CSTRING)
                {
                    ch = cdb2_column_value(sqlh, 0);
                    sum3 += atoll(ch);
                    fprintf(stderr, "id=%ld, acct=%d, bal=%lld\n", id, nacct3, atoll(ch));
                }
                else if(type == CDB2_INTEGER)
                {
                    ll = (long long *)cdb2_column_value(sqlh, 0);
                    sum3 += *ll;
                    fprintf(stderr, "id=%ld, acct=%d, bal=%lld\n", id, nacct3, *ll);
                }
                else
                {
                    fprintf(stderr, "Unexpected type from cdb2_next_record, %d\n", type);
                    exit(1);
                }
                do
                {
                    ret = cdb2_next_record(sqlh);
                }
                while(ret == CDB2_OK);
            }
            fprintf(stderr, "userid=%ld account sum=%lld sum2=%lld, sum3=%lld, nacct=%d, nacct2=%d, nacct3=%d\n", 
                    id, sum, sum2, sum3, nacct, nacct2, nacct3);
            fprintf(stderr, "sum and nacct is the result from the first select sum(bal) and count(bal).\n");
            fprintf(stderr, "sum2 and nacct2 is the result from the second select sum(bal) and count(bal) after first one failed.\n");
            fprintf(stderr, "sum3 and nacct3 is the result got by selecting each acct one by one. These are the correct sum and count\n");
            fprintf(stderr, "Failed on iteration %d thread %lu\n", cnt, pthread_self());
            //fprintf(stderr, "userid=%d account sum=%lld sum2=%lld, nacct=%d, nacct2=%d\n", id, sum, sum2, nacct, nacct2);
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
            fprintf(stderr, "error selecting record id=%ld and acct=%ld, ret=%d.\n", id, acct, ret);
            exit(1);
        }

        ret = cdb2_next_record(sqlh);
        if(ret != CDB2_OK) 
        {
            fprintf(stderr, "error from cdb2_next_record: %d line %d\n", ret, __LINE__);
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
            fprintf(stderr, "Unexpected type from cdb2_next_record, %d\n", type);
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
            fprintf(stderr, "error selecting record id=%ld and acct=%ld, ret=%d.\n", id, acct, ret);
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
        /*if(ret)
          fprintf(stderr, "Commit Failed\n");
          else
          fprintf(stderr, "Commit Succeeded\n");*/
        if(ret == 0)
        {
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
    cdb2_hndl_tp *sqlh;
    config_t *c;
    int err = 0, ret, opt;
    char *stage = "local";

    argv0 = argv[0];
    setlinebuf(stdout);
    setlinebuf(stderr);
    signal(SIGPIPE, SIG_IGN);
    c = default_config();

    /* char *optarg=argument, int optind = argv index  */
    while ((opt = getopt(argc,argv,"d:hr:i:tsoD"))!=EOF)
    {
        switch(opt)
        {
            case 'D':
                c->debug = 1;
                break;

            case 'd':
                c->dbname = optarg;
                break;

            case 'h':
                usage(stdout);
                break;

            case 'r':
                c->records = atoi(optarg);
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

            case 'o':
                c->snapisol_only = 1;
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
    if(NULL == c->dbname)
    {
        fprintf( stderr, "dbname is unset.\n");
        err++;
    }
    c->stage = stage;

    if(c->snapisol_only && c->serial)
    {
        fprintf( stderr, "can't run snapisol only test in serial mode.\n");
        err++;
    }

    /* Punt before pekludge if there were errors. */
    if( err )
    {
        usage(stderr);
        exit(1);
    }

    if (0 == err && (ret = cdb2_open(&sqlh, c->dbname, c->stage, 0)) != 0)
    {
        fprintf(stderr, "Error getting sql handle, ret=%d\n", ret);
        err++;
    }

    /* Punt if there were errors. */
    if (err)
    {
        usage(stderr);
        exit(1);
    }
    if ((ret = cdb2_run_statement(sqlh, "truncate t1")) != 0)
    {
        fprintf(stderr, "error inserting record ret=%d.\n", ret);
        exit(1);
    }
    // start test here
    insert_records(c);
    update_records(c);
    return 0;
}
