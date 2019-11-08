#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <cdb2api.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <pthread.h>
#include <getopt.h>

static char *argv0=NULL;
static char *dbname=NULL;
static char *stage="default";
static int thd_interval = 1;
static int thd_count = 10;
static int total_time = 300;
static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cd = PTHREAD_COND_INITIALIZER;
static int current_gen = 0;
static int count = 0;
static int exit_thd = 0;

void usage(FILE *f)
{
    fprintf(f,"Usage: %s <cmd-line>\n", argv0);
    fprintf(f," -d <dbname>         - sets dbname\n");
    fprintf(f," -t <thd-count>      - sets number of threads\n");
    fprintf(f," -i <thd-interval>   - sets time between actions\n");
    fprintf(f," -x <time>           - total test-time\n");
    fprintf(f," -f <cfg>            - set comdb2config\n");
    fprintf(f," -s <stage>          - set stage\n");
    fprintf(f," -h                  - this menu\n");
    exit(1);
}

void runsql(int id, int mygen)
{
    cdb2_hndl_tp *hndl;
    char sql[256];
    int rc, val=(mygen * thd_count) + id;
    rc = cdb2_open(&hndl, dbname, stage, CDB2_RANDOM);
    if (rc) {
        fprintf(stderr, "Error opening handle id %d, %s\n", id,
                cdb2_errstr(hndl));
        exit(1);
    }
    snprintf(sql, sizeof(sql), "insert into t1 values(%d)", val);
    rc = cdb2_run_statement(hndl, sql);
    if (rc!=0) {
        fprintf(stderr, "%s thd %d error running insert, %s\n", __func__, id,
                cdb2_errstr(hndl));
        exit(1);
    }
    rc = cdb2_next_record(hndl);
    while(rc == CDB2_OK) {
        rc = cdb2_next_record(hndl);
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "%s: line %d cdb2_next_record returns %s\n", __func__,
                __LINE__, cdb2_errstr(hndl));
        exit(1);
    }
    rc = cdb2_run_statement(hndl, "select * from t1 order by a");
    if (rc!=0) {
        fprintf(stderr, "%s thd %d error running select, %s\n", __func__, id,
                cdb2_errstr(hndl));
        exit(1);
    }
    rc = cdb2_next_record(hndl);
    while(rc == CDB2_OK) {
        rc = cdb2_next_record(hndl);
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "%s: line %d cdb2_next_record returns %s\n", __func__,
                __LINE__, cdb2_errstr(hndl));
        exit(1);
    }
    cdb2_close(hndl);
}

void create_table(void)
{
    cdb2_hndl_tp *hndl;
    int rc;

    rc = cdb2_open(&hndl, dbname, stage, CDB2_RANDOM);
    if (rc) {
        fprintf(stderr, "Error opening handle for create_table %s\n",
                cdb2_errstr(hndl));
        exit(1);
    }
    rc = cdb2_run_statement(hndl, "drop table t1");
    rc = cdb2_next_record(hndl);
    while(rc == CDB2_OK) {
        rc = cdb2_next_record(hndl);
    }

    rc = cdb2_run_statement(hndl, "create table t1 { schema { int a } keys { \"a\" = a } }");
    if (rc!=0) {
        fprintf(stderr, "Error running create_table, %s\n", cdb2_errstr(hndl));
        exit(1);
    }
    rc = cdb2_next_record(hndl);
    while(rc == CDB2_OK) {
        rc = cdb2_next_record(hndl);
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "%s: line %d cdb2_next_record returns %s\n", __func__,
                __LINE__, cdb2_errstr(hndl));
        exit(1);
    }
    cdb2_close(hndl);
}

void *thd(void *x)
{
    int id = *(int *)x;
    int mygen=0;

    while(1) {
        pthread_mutex_lock(&lk);
        while(exit_thd == 0 && mygen == current_gen) {
            pthread_cond_wait(&cd, &lk);
        }
        pthread_mutex_unlock(&lk);
        if (exit_thd) {
            return NULL;
        }
        runsql(id, mygen);
        mygen++; count++;
    }
}

int main(int argc, char *argv[])
{
    int opt, err=0, i, target_time, rc, *ids;
    pthread_t *tids;

    argv0=argv[0];
    srand(time(NULL)*getpid());

    while ((opt = getopt(argc,argv,"d:t:i:x:s:f:h"))!=EOF)
    {
        switch(opt)
        {
            case 'd':
                dbname=optarg;
                break;
            case 't':
                thd_count=atoi(optarg);
                break;
            case 'i':
                thd_interval=atoi(optarg);
                break;
            case 'x':
                total_time=atoi(optarg);
                break;
            case 's':
                stage=optarg;
                break;
            case 'f':
                cdb2_set_comdb2db_config(optarg);
                break;
            case 'h':
                usage(stdout);
                break;
            default:
                fprintf(stderr, "Unknown option, '%c'.\n", opt);
                err++;
                break;
        }
    }

    if(NULL == dbname)
    {
        fprintf(stderr, "Dbname is not set!\n");
        err++;
    }

    if(err)
    {
        usage(stderr);
    }

    create_table();

    tids=(pthread_t *)calloc(sizeof(pthread_t), thd_count);
    ids=(int *)calloc(sizeof(int), thd_count);

    for(i = 0; i < thd_count; i++) {
        ids[i] = i;
        if((rc = pthread_create(&tids[i],NULL,thd,(void *)&ids[i]))) {
            fprintf(stderr, "Error creating threads: %d %s\n", rc, 
                    strerror(errno));
            exit(1);
        }
    }

    target_time = (time(NULL) + total_time);

    while(total_time == 0 || time(NULL) < target_time) {
        pthread_mutex_lock(&lk);
        current_gen++;
        pthread_cond_broadcast(&cd);
        pthread_mutex_unlock(&lk);
        sleep(thd_interval);
    }

    pthread_mutex_lock(&lk);
    exit_thd = 1;
    pthread_cond_broadcast(&cd);
    pthread_mutex_unlock(&lk);

    for(i = 0; i < thd_count; i++) {
        pthread_join(tids[i], NULL);
    }

    return 0;
}

