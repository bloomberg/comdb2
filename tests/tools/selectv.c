#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <assert.h>
#include <stdint.h>
#include <unistd.h>

#include <cdb2api.h>

static char *argv0=NULL;

void usage(FILE *f)
{
    fprintf(f,"Usage: %s <cmd-line>\n",argv0);
    fprintf(f,"     -d <dbname>             -   set dbname.\n" );
    fprintf(f,"     -h                      -   this menu.\n" );
    exit(1);
}

typedef struct config
{
    cdb2_hndl_tp *hndl;
    char        *dbname;
    int         records;
    int         iters;
    int         threads;
    int         fastinit;
    int         insert;
    int         update;
    int         verbose;
    int         wrcoll;
    int         serial;
    int         snapisol_only;
}
config_t;

static config_t *default_config(void)
{
    config_t *c = malloc(sizeof(*c));
    bzero(c, sizeof(*c));
    c->iters = 50;
    c->records = 2000;
    c->threads = 20;
    c->fastinit = 1;
    c->insert = 1;
    c->update = 1;
    c->verbose = 0;
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
    int64_t *ll, instid;
    char *ch;

    int64_t host = pthread_self();

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    if ((ret = cdb2_open(&sqlh, c->dbname, "default", 0)) != 0)
    {
        fprintf(stderr, "%s:%d Error getting sql handle, ret=%d\n", __func__, __LINE__, ret);
        exit(1);
    }

     snprintf(sql, sizeof(sql), "set transaction read committed");

     if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
     {
         fprintf(stderr, "%s:%d Error setting transaction level ret=%d.\n", __func__, __LINE__, ret);
         exit(1);
     }
     do
     {
         ret = cdb2_next_record(sqlh);
     }
     while(ret == CDB2_OK);

    for(cnt = 0; cnt < u->iters || u->iters < 0; cnt++)
    {
        snprintf(sql, sizeof(sql), "begin");
        if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
        {
            fprintf(stderr, "%s:%d error in begin, ret=%d.\n", __func__, __LINE__, ret);
            exit(1);
        }
        do
        {
            ret = cdb2_next_record(sqlh);
        }
        while(ret == CDB2_OK);

        snprintf(sql, sizeof(sql), "selectv s.instid,i.rqstid,start,tztag from schedule s join jobinstance i on i.instid=s.instid left join dbgopts d on d.rqstid=i.rqstid where s.state=0 and start < (now() + cast(90 as second)) order by start limit 15");
        if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
        {
            fprintf(stderr, "%s:%d error selecting record.\n", __func__, __LINE__);
            exit(1);
        }
        /* In single-node mode, this returns CDB2_OK rather than CDB2_OK_DONE.. 
         * this is wrong */

        ret = cdb2_next_record(sqlh);
        if(ret == CDB2_OK) 
        {
            if((type = cdb2_column_type(sqlh, 0)) == CDB2_CSTRING)
            {
                ch = cdb2_column_value(sqlh, 0);
                instid = atoll(ch);
            }
            else if(type == CDB2_INTEGER)
            {
                ll = (int64_t *)cdb2_column_value(sqlh, 0);
                instid = *ll;
            }
            else
            {
                fprintf(stderr, "%s:%d Unexpected type from cdb2_next_record, %d\n", __func__, __LINE__, type);
                exit(1);
            }
            do
            {
                ret = cdb2_next_record(sqlh);
            }
            while(ret == CDB2_OK);

            cdb2_clearbindings(sqlh);
            snprintf(sql, sizeof(sql), "update schedule set state = state + 1, updatehost = @localhost where instid = @instid");
            if ((ret = cdb2_bind_param(sqlh, "localhost", CDB2_INTEGER, &host, sizeof(int))) != 0)
            {
                fprintf(stderr, "%s:%d error binding localhost, ret=%d.\n", __func__, __LINE__, ret);
                exit(1);
            }
            if ((ret = cdb2_bind_param(sqlh, "instid", CDB2_INTEGER, &instid, sizeof(int))) != 0)
            {
                fprintf(stderr, "%s:%d error binding instid, ret=%d.\n", __func__, __LINE__, ret);
                exit(1);
            }
            if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
            {
                fprintf(stderr, "%s:%d error selectv record.\n", __func__, __LINE__);
                exit(1);
            }

            do
            {
                ret = cdb2_next_record(sqlh);
            }
            while(ret == CDB2_OK);
            cdb2_clearbindings(sqlh);
        }

        snprintf(sql, sizeof(sql), "commit");
        ret = cdb2_run_statement(sqlh, sql);
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

int verify_update(config_t *c)
{
    cdb2_hndl_tp *sqlh;
    char sql[1024];
    int ret, type;
    long long *ll, count;
    char *ch;

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    if ((ret = cdb2_open(&sqlh, c->dbname, "default", 0)) != 0)
    {
        fprintf(stderr, "%s:%d Error getting sql handle, ret=%d\n", __func__, __LINE__, ret);
        exit(1);
    }

    snprintf(sql, sizeof(sql), "select count(*) from schedule where state > 1");
    if ((ret = cdb2_run_statement(sqlh, sql)) != 0)
    {
        fprintf(stderr, "%s:%d error selecting record.\n", __func__, __LINE__);
        exit(1);
    }
    ret = cdb2_next_record(sqlh);
    if(ret == CDB2_OK) 
    {
        if((type = cdb2_column_type(sqlh, 0)) == CDB2_CSTRING)
        {
            ch = cdb2_column_value(sqlh, 0);
            count = atoll(ch);
        }
        else if(type == CDB2_INTEGER)
        {
            ll = (long long *)cdb2_column_value(sqlh, 0);
            count = *ll;
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
    else
    {
        fprintf(stderr, "%s:%d error cdb2_next_record.\n", __func__, __LINE__);
        exit(1);
    }

    if (count != 0) {
        fprintf(stderr, "More than one threads share the same schedule\n");
        exit(1);
    }
    
    cdb2_close(sqlh);
    return 0;
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

    argv0 = argv[0];
    c = default_config();

    /* char *optarg=argument, int optind = argv index  */
    while ((opt = getopt(argc,argv,"d:h"))!=EOF)
    {
        switch(opt)
        {
            case 'd':
                c->dbname = optarg;
                break;

            case 'h':
                usage(stdout);
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

    /* Punt if there were errors. */
    if( err )
    {
        exit(1);
    }

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    /* Allocate an sql handle. */
    if (0 == err && cdb2_open(&sqlh, c->dbname, "default", 0))
    {
        fprintf(stderr, "error opening sql handle for '%s'.\n", c->dbname);
        err++;
    }
    /* Punt if there were errors. */
    if (err)
    {
        usage(stderr);
        exit(1);
    }

    update_records(c);
    printf("Verifying records...\n");
    verify_update(c);
    return 0;
}
