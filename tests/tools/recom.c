#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <cdb2api.h>
#include <sqlite3.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

/* Global argv0 */
static char *argv0=NULL;

/* Global dbname */
static char *dbname=NULL;

/* Global iter */
static int iterations=100;

/* Global verbose */
static int verbose=1;

/* Usage */
void usage(FILE *f)
{
    fprintf(f,"Usage: %s <cmd-line>\n",argv0);
    fprintf(f," -d <dbname>                 - set dbname.\n");
    fprintf(f," -t <threads>                - set number of threads.\n");
    fprintf(f," -i <iterations>             - set number of iterations.\n");
    fprintf(f," -x                          - disable fastinit.\n");
    fprintf(f," -v                          - set verbose flag.\n");
    fprintf(f," -h                          - print this menu.\n");
    exit(1);
}

/* Run and discard results */
static inline int run_statement(cdb2_hndl_tp *sqlh, const char *sql, int iter)
{
    int rc, cnt=0;

    rc = cdb2_run_statement(sqlh, sql);
    if(verbose && CDB2ERR_VERIFY_ERROR == rc) {
        fprintf(stdout, "Thread %p verify error.\n", (void *)pthread_self());
        return rc;
    }
    
    if(rc)
    {
        fprintf(stderr, "Thread %p error %d while running '%s' on iter %d.\n", 
                (void *)pthread_self(), rc, sql, iter);
        fprintf( stderr, "Error string: '%s'.\n", cdb2_errstr(sqlh));
        exit(1);
    }

    do
    {
        rc = cdb2_next_record(sqlh);
        cnt++;
    } 
    while(CDB2_OK == rc);

    return 0;
}

/* Run and retrieve id */
static inline int run_statement_get_rtn(cdb2_hndl_tp *sqlh, const char *sql, int ignoredups, int iter, int isupdate)
{
    int rc, err=0, rtn;
    long long val;

    do
    {
        rc = cdb2_run_statement(sqlh, sql);
        if(verbose && CDB2ERR_VERIFY_ERROR == rc)
        {
            fprintf(stderr, "Thread %p verify error.\n", (void *)pthread_self());
        }
    }
    while(CDB2ERR_VERIFY_ERROR == rc);
    
    if(rc)
    {
        fprintf( stderr, "Thread %p error %d while running '%s'.\n", 
                (void *)pthread_self(), rc, sql);
        exit(1);
    }

    if (!isupdate) {
        rc = cdb2_next_record(sqlh);

        if( CDB2_CSTRING == cdb2_column_type(sqlh, 0))
        {
            val = atoi(cdb2_column_value(sqlh, 0));
        }
        else
        {
            val = *(long long *)cdb2_column_value(sqlh, 0);
        }

        rc = cdb2_next_record(sqlh);

        while(CDB2_OK == rc)
        {
            val = *(long long *)cdb2_column_value(sqlh, 0);
            if(!ignoredups)
            {
                fprintf(stderr, "Thread %p found another record when running '%s' on iteration %d.\n", 
                        (void *)pthread_self(), sql, iter);
            }
            rc = cdb2_next_record(sqlh);
            err++;
        }
        rtn = (int)val;
    }
    else {
        cdb2_effects_tp ef;
        rc = cdb2_get_effects(sqlh, &ef);
        if (rc) {
            fprintf(stderr, "cdb2_get_effects rc %d\n", rc);
            exit(1);
        }
        rtn = ef.num_affected = ef.num_selected + ef.num_updated + ef.num_deleted + ef.num_inserted;
    }

    if(err && !ignoredups)
    {
        exit(1);
    }


    return rtn;
}


/* Run read-committed sql */
void *work_func(void *arg)
{
    intptr_t td = (intptr_t)arg;
    cdb2_hndl_tp *sqlh;
    char sql[128];
    int ii, rc, id, rtn;

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    /* Get id, update it to id+1, update it to id+1, commit */
    ii = 0;
    while(ii < iterations)
    {
        if(cdb2_open(&sqlh, dbname, "default", CDB2_RANDOM))
        {
            fprintf(stderr, "Error allocating an sql handle in thread %p.\n", (void *)pthread_self());
            exit(1);
        }
        rc = cdb2_run_statement(sqlh, "SET VERIFYRETRY OFF");
        if (rc) {
            fprintf(stderr, "can't turn off verifyretry\n");
            exit(1);
        }

        sqlite3_snprintf(sizeof(sql), sql, "SET TRANSACTION READ COMMITTED");
        run_statement(sqlh, sql, ii);

        sqlite3_snprintf(sizeof(sql), sql, "BEGIN TRANSACTION");
        run_statement(sqlh, sql, ii);

        sqlite3_snprintf(sizeof(sql), sql, "SELECT id FROM t1");
        id = run_statement_get_rtn(sqlh, sql, 0, ii, 0);
        //printf("%d: sel sql: %s, id=%d\n", td, sql, id);

        sqlite3_snprintf(sizeof(sql), sql, "UPDATE t1 SET id=%d WHERE id=%d", id+1, id);
#if 0        
        int upd;
        printf("%d: upd1 sql: %s\n", td, sql);
        upd = 
#endif
        run_statement_get_rtn(sqlh, sql, 0, ii, 1);

        sqlite3_snprintf(sizeof(sql), sql, "UPDATE t1 SET id=%d WHERE id=%d", id+2, id+1);
#if 0
        printf("%d: upd2 sql: %s\n", td, sql);
        int upd2;
        upd2 = 
#endif
        run_statement_get_rtn(sqlh, sql, 0, ii, 1);

        sqlite3_snprintf(sizeof(sql), sql, "COMMIT");
        rtn = run_statement(sqlh, sql, ii);

        cdb2_close(sqlh);
        if(rtn == 0) 
           ii++;
    }
    printf("%d: Thread Done\n", (int) td);
    return NULL;
}

int main(int argc,char *argv[])
{
    int c, err=0, numthds=5, rc;
    cdb2_hndl_tp *sqlh;
    pthread_t *thds;
    argv0=argv[0];
    char sql[128];

    /* char *optarg=argument, int optind = argv index  */
    while ((c = getopt(argc,argv,"vi:d:t:h"))!=EOF)
    {
            switch(c)
            {
                case 'd':
                    dbname=optarg;
                    break;

                case 't':
                    numthds=atoi(optarg);
                    break;

                case 'h':
                    usage(stdout);
                    break;

                case 'v':
                    verbose=1;
                    break;

                case 'i':
                    iterations=atoi(optarg);
                    break;

                default:
                    fprintf(stderr, "Unknown option, '%c'.\n", c);
                    err++;
                    break;
            }
    }
    
    /* Punt if dbname is NULL */
    if(NULL == dbname)
    {
        fprintf(stderr, "Database name is not specified.\n");
        err++;
    }
    
    /* Punt on err */
    if (err) 
    {
        usage(stderr);
    }

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    /* Allocate an sql handle */
    if(cdb2_open(&sqlh, dbname, "default", CDB2_RANDOM))
    {
        fprintf(stderr, "Error allocating an sql handle.\n");
        exit(1);
    }

    /* Write sql */
    sqlite3_snprintf(sizeof(sql), sql,
            "INSERT INTO t1 (id) values (1)");

    /* Insert */
    run_statement(sqlh, sql, -1);

    /* Create thread handles */
    thds = (pthread_t *)malloc(sizeof(pthread_t) * numthds);

    /* Create threads */
    intptr_t ii;
    for(ii = 0 ; ii < numthds ; ii++)
    {
        if( 0 != ( rc = pthread_create(&thds[ii], NULL, work_func, (void *)ii) ) )
        {
            fprintf(stderr, "Error creating thread: %d %s.\n", rc, 
                    strerror(errno));
            exit(1);
        }
    }

    /* Join threads */
    for(ii = 0 ; ii < numthds; ii++)
    {
        pthread_join(thds[ii], NULL);
    }

    sqlite3_snprintf(sizeof(sql), sql, "SELECT id FROM t1");
    int id = run_statement_get_rtn(sqlh, sql, 0, 200, 0);

    printf("sel sql: %s, id=%d\n",sql, id);

    /* Success! */
    return 0;
}

