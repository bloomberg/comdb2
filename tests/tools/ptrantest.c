#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <cdb2api.h>
#include <pthread.h>
#include <string.h>
#include <errno.h>
#include <sqlite3.h>
#include <inttypes.h>

/* use a single appname */
static char *appname="seed";

/* use a single domain */
static char *domain="test";

/* arg0 */
static char *argv0=NULL;

/* usage */
void usage(FILE *f)
{
    fprintf(f,"Usage: %s <dbname> [opts]\n",argv0);
    fprintf(f,"     -t <threads>            - number of updater threads\n");
    fprintf(f,"     -i <iters>              - number of iterations\n");
    fprintf(f,"     -a <appname>            - set 'appname' column value (defaults to 'seed')\n");
    fprintf(f,"     -d <domain>             - set 'domain' column value (defaults to 'test')\n");
    fprintf(f,"     -s                      - use snapshot isolation\n");
    fprintf(f,"     -F                      - disable initial fastinit\n");
    fprintf(f,"     -I                      - disable initial insert-record\n");
    fprintf(f,"     -v                      - enable verbose trace\n");
    fprintf(f,"     -h                      - print this menu\n");
    exit(1);
}

/* default number of updater threads */
static int num_threads = 2;

/* verbose flag */
static int verbose = 0;

/* isolation level */
static char *isolation_level = NULL;

/* set to 1 if we need to exit */
static int exit_pthread = 0;

/* keep track of the updated_value */
static long long updated_value=0;

/* this is the number of total iterations */
static long long iterations=5000;

/* function to set the update value*/
static int set_update_value(long long val, FILE *f)
{
    int rc=0;
    static pthread_mutex_t lk=PTHREAD_MUTEX_INITIALIZER;

    pthread_mutex_lock(&lk);

    if(verbose) {
        fprintf(f,"Thread %"PRIdPTR" updating current seed to %lld\n", (intptr_t)pthread_self(), val);
    }

    /* This doesn't have to be sequential to be correct, but trying to update to an 
       existing value is always wrong */
    if(val == updated_value) {
        if(verbose) {
            fprintf(stderr,"Error!  Thread %"PRIdPTR" succeeded in updating to current value (%lld)?\n",
                    (intptr_t)pthread_self(), val);
        }
        rc=1;
    }
    updated_value=val;

    iterations--;

    pthread_mutex_unlock(&lk);

    return rc;
}

/* updater-thread */
static void *updater(void *arg)
{
    char *dbname=(char *)arg;
    char sql[1024];
    long long val;
    int rc, nupd, ret=0;

    cdb2_hndl_tp *sqlh = NULL;
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    rc = cdb2_open(&sqlh, dbname, "default", 0);
    if(0 != rc) {
        fprintf(stderr,"cdb2_open failed for thread %"PRIdPTR"\n", (intptr_t)pthread_self());
        exit_pthread=1;
        pthread_exit((void *)1);
        exit(1);
    }


    if(isolation_level) {
        sqlite3_snprintf(sizeof(sql), sql, 
                "set transaction %s", isolation_level);
        rc = cdb2_run_statement( sqlh, sql );

        if(rc) {
            fprintf(stderr,"error running set transaction, rc=%d, '%s'\n", rc,
                cdb2_errstr( sqlh ));
            exit_pthread=1;
            pthread_exit((void *)1);
        }

        do {
            rc = cdb2_next_record( sqlh );
        } while(rc == CDB2_OK);

        if(rc != CDB2_OK_DONE) {
            fprintf(stderr, "Protocal error setting transaction level\n");
            exit_pthread=1;
            pthread_exit((void *)1);
        }
    }


    /* loop until we're finished */
    while(iterations > 0) {
        int rc;

        if(exit_pthread)
        {
            pthread_exit((void *)(uintptr_t)ret);
        }

        sqlite3_snprintf(sizeof(sql), sql, 
                "select seed from t1 where appname=%Q and domain=%Q", appname, domain);
        rc = cdb2_run_statement( sqlh, sql );
        if(rc) {
            fprintf(stderr,"error running select statement, rc=%d, '%s'\n", rc,
                cdb2_errstr( sqlh ));
            exit_pthread=1;
            pthread_exit((void *)1);
        }

        rc = cdb2_next_record( sqlh );
        if(CDB2_OK_DONE == rc) {
            /* ignore for now- the 'random-stripe' logic can cause this state */
            continue; 
        }

        if(CDB2_OK != rc) {
            fprintf(stderr,"Thread %"PRIdPTR" error finding record rc=%d %s\n",
                    (intptr_t)pthread_self(), rc, cdb2_errstr( sqlh ));
            exit_pthread=1;
            pthread_exit((void *)1);
        }

        val = *(long long*) cdb2_column_value(sqlh, 0);
        rc = cdb2_next_record( sqlh );

        while(CDB2_OK == rc) {
            val = *(long long*) cdb2_column_value(sqlh, 0);
            fprintf(stderr,"Thread %"PRIdPTR" found another record with seed=%lld\n",
                    (intptr_t)pthread_self(), val);
            rc = cdb2_next_record( sqlh );
        }

        sqlite3_snprintf(sizeof(sql), sql, 
                "update t1 set seed=%lld where appname=%Q and domain=%Q and seed=%lld",
                val+1, appname, domain, val);

        do {
            rc = cdb2_run_statement( sqlh, sql );
            if(CDB2ERR_VERIFY_ERROR == rc) {
                /* fprintf(stderr,"Thread %d verify error\n",(int)pthread_self()); */
            }
        } while(CDB2ERR_VERIFY_ERROR == rc);

        if(rc) {
            fprintf(stderr,"Thread %"PRIdPTR" error on update rc=%d %s\n",(intptr_t)pthread_self(),
                    rc, cdb2_errstr( sqlh ));
            fprintf(stderr,"(Trying to set %lld to %lld)\n", val, val+1);
            exit_pthread=1;
            pthread_exit((void *)1);
        }

        rc = cdb2_next_record( sqlh );
        if(rc) {
            fprintf(stderr,"Thread %"PRIdPTR" error on next-rec, rc=%d %s\n",
                    (intptr_t)pthread_self(), rc, cdb2_errstr( sqlh ));
            exit_pthread=1;
            pthread_exit((void *)1);
        }

        /* This will be an int or text depending on whether we are block or socksql. */
        int type = cdb2_column_type(sqlh, 0 );
        if( CDB2_CSTRING == type) {
            nupd = atoi( (char *)cdb2_column_value(sqlh, 0 ) );
        }
        else if( CDB2_INTEGER == type) {
            nupd = (int) *(long long *)cdb2_column_value(sqlh, 0);
        } 
        else {
            fprintf( stderr, "Unexpected results from select .. type = %d.\n", 
                    cdb2_column_type(sqlh, 0 ) );
            exit_pthread=1;
            pthread_exit((void *)1);
        }

        /* failed to update */
        if(0 == nupd)
            continue;

        if(nupd < 0 || nupd > 1) {
            fprintf(stderr,"Thread %"PRIdPTR" updated %d records??\n", (intptr_t)pthread_self(),
                    nupd);
            exit_pthread=1;
            pthread_exit((void *)1);
        }
        
        /* nupd succeeded - set the update value */
        rc = set_update_value(val+1, stdout);
        if(rc) {
            fprintf(stderr,"Thread %"PRIdPTR" updated to %lld but the last updated value was already %lld???\n",
                (intptr_t)pthread_self(), val+1, val+1);
        }
        ret+=rc;
    }
    return (void *)(uintptr_t)ret;
}


/* main */
int main(int argc,char *argv[])
{
    char *dbname=NULL, sql[1024];
    int rc, joinrc=0, i, c, fastinit=1, initinsert=1, err=0;
    pthread_t *tids=NULL;
    cdb2_hndl_tp *sqlh = NULL;

    /* latch progname */
    argv0=argv[0];

    /* verify arguments */
    if(argc < 2) 
        usage(stderr);

    /* grab dbname */
    dbname=argv[1];

    /* skip past the first argument */
    argc--; argv++;

    /* char *optarg=argument, int optind = argv index  */
    while ((c = getopt(argc,argv,"t:i:a:d:sFIvh"))!=EOF) {
        switch(c) {
            case 't':
                num_threads = atoi(optarg);
                printf("set number of updater threads to %d\n",num_threads);
                break;
            case 'i':
                iterations = atoi(optarg);
                printf("set max updates to %lld\n",iterations);
                break;
            case 'a':
                appname = optarg;
                printf("set appname to '%s'\n", appname);
                break;
            case 'd':
                domain = optarg;
                printf("set domain to '%s'\n", domain);
                break;
            case 's':
                isolation_level = "snapshot isolation";
                break;
            case 'F':
                fastinit = 0;
                printf("disabled fastinit\n");
                break;
            case 'I':
                initinsert = 0;
                printf("disabled initial insert\n");
                break;
            case 'v':
                verbose = 1;
                printf("enabled verbose trace\n");
                break;
            case 'h':
                usage(stdout);
                break;
            default:
                fprintf(stderr,"unknown argument '%c'\n", c);
                err++;
                break;
        }
    }

    if(err)
        usage(stderr);

    /* add to env */
    /* increase lclpooled buffers to max */
    //setlclbfpoolsz(256);
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    rc = cdb2_open(&sqlh, dbname, "default", 0);
    if(0 != rc) {
        fprintf(stderr,"error allocating sqlhandle\n");
        exit(1);
    }

    /* fastinit table */
    if(fastinit) {
        rc = cdb2_run_statement( sqlh, "truncate t1" );
        if(rc) {
            fprintf(stderr,"truncate failed - rc=%d\n", rc);
            exit(1);
        }
    }
   
    /* insert first record */
    if(initinsert) {
        sqlite3_snprintf(sizeof(sql), sql, 
                "insert into t1(appname, domain, seed) values(%Q, %Q, 1)", appname,
                domain);

        /* run statement */
        rc = cdb2_run_statement( sqlh, sql );
        if(rc) {
            fprintf(stderr,"error running insert statement: '%s', rc=%d, '%s'\n", 
                sql, rc, cdb2_errstr( sqlh ));
            exit(1);
        }

        /* set initial updated value */
        updated_value=1;
    }

    /* create space to hold thread-ids */
    tids=(pthread_t *)calloc(num_threads, sizeof(pthread_t));

    /* create all threads */
    for(i = 0 ; i < num_threads ; i++) {
        /* create thread 1 */
        rc=pthread_create(&tids[i], NULL, updater, dbname);
        if(rc) {
            fprintf(stderr,"pthread_create error for thread %d: %s\n", 
                    i, strerror(errno));
            exit(1);
        }
    }

    /* now join them all */
    for(i = 0 ; i < num_threads ; i++) {
        int ret;
        rc = pthread_join(tids[i], (void**)&ret);
        if(rc) {
            fprintf(stderr,"pthread_join error for thread 1: %s\n", 
                    strerror(errno));
            exit(1);
        }
        if(ret) {
            fprintf(stderr,"ERROR FROM THREAD #%"PRIdPTR"\n", (intptr_t)tids[i]);
        }
        joinrc += ret;
    }

    if(0 == joinrc) {
        printf("SUCCESS\n");
    }

    return joinrc;

}
