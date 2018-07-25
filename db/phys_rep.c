#include <cdb2api.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>

#include "phys_rep_lsn.h"
#include "dbinc/rep_types.h"

#include "comdb2.h"
#include "phys_rep.h"

/* for replication */
static cdb2_hndl_tp* repl_db;
static char* repl_db_name;

static int do_repl;

/* internal implementation */
typedef struct DB_Connection {
    char *hostname;
    int is_up;
    time_t last_cnct;
} DB_Connection;

static DB_Connection **local_rep_dbs = NULL;
static int cnct_len = 0;
static int idx = 0;

static DB_Connection* get_connect(char* hostname);
static int insert_connect(char* hostname);
static void delete_connect(DB_Connection* cnct);
static LOG_INFO handle_record();

static pthread_t sync_thread;
/* internal implementation */

/* externs here */
extern struct dbenv* thedb;
extern int sc_ready(void);

/* external API */
int set_repl_db_name(char* name)
{
    int rc;
    repl_db_name = name;

    /* TODO: may want to verify local db exists */
    rc = 0;

    return rc;
}

int add_replicant_host(char *hostname) 
{
    return insert_connect(hostname);
}

int remove_replicant_host(char *hostname) {
    DB_Connection* cnct = get_connect(hostname);
    if (cnct)
    {
        delete_connect(cnct);
        return 0;
    }
    return -1;
}

void cleanup_hosts()
{
    DB_Connection* cnct;

    for (int i = 0; i < idx; i++)
    {
        cnct = local_rep_dbs[i];
        delete_connect(cnct);
    }

    free(repl_db_name);

    cdb2_close(repl_db);
}

const char* start_replication()
{
    int rc;
    DB_Connection* cnct;

    for (int i = 0; i < idx; i++)
    {
        cnct = local_rep_dbs[i];

        if ((rc = cdb2_open(&repl_db, repl_db_name, 
                        cnct->hostname, CDB2_DIRECT_CPU)) == 0)
        {
            fprintf(stdout, "Attached to %s and db %s for replication\n", 
                    cnct->hostname,
                    repl_db_name);
            if (pthread_create(&sync_thread, NULL, keep_in_sync, NULL))
            {
                fprintf(stderr, "Couldn't create thread to sync\n");
                cdb2_close(repl_db);
            }

            return cnct->hostname;
        }

        fprintf(stdout, "%s\n", cnct->hostname);
    }

    fprintf(stderr, "Couldn't find any remote dbs to connect to\n");
    return NULL;
}

void* keep_in_sync(void* args)
{
    /* vars for syncing */
    int rc;
    struct timespec wait_spec;
    struct timespec remain_spec;
    // char* sql_cmd = "select * from comdb2_transaction_logs('{@file:@offset}')"; 
    size_t sql_cmd_len = 100;
    char sql_cmd[sql_cmd_len];


    do_repl = 1;
    wait_spec.tv_sec = 1;
    wait_spec.tv_nsec = 0;

    /* cannot call get_last_lsn if thedb is not ready */
    while (!sc_ready())
        nanosleep(&wait_spec, &remain_spec);

    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);

    while(do_repl)
    {
        LOG_INFO info = get_last_lsn(thedb->bdb_env);
        LOG_INFO prev_info = info;

        rc = snprintf(sql_cmd, sql_cmd_len, 
                "select * from comdb2_transaction_logs('{%u:%u}')", 
                info.file, info.offset);
        if (rc < 0 || rc >= sql_cmd_len)
            fprintf(stderr, "sql_cmd buffer is not long enough!\n");

        rc = cdb2_run_statement(repl_db, sql_cmd);

        /* should verify that this is the record we want */
        if ((rc = cdb2_next_record(repl_db)) != CDB2_OK)
        {
            fprintf(stderr, "Can't find the next record\n");
        }
        // TODO: check the record

        while((rc = cdb2_next_record(repl_db)) == CDB2_OK)
        {
            /* should have ncols as 5 
            for(col = 0; col < 5; col++)
            {
                val = cdb2_column_value(repl_db, col); 
                fprintf(stderr, "%s, type: %d", 
                        cdb2_column_name(repl_db, col),
                        cdb2_column_type(repl_db, col));
            } */

            prev_info = handle_record(prev_info);

        }

        /* check we finished correctly */
        if (rc == CDB2_OK_DONE)
        {
            //fprintf(stdout, "Finished reading from xsaction logs\n");
        }
        else
        {
            fprintf(stderr, "Had an error %d\n", rc);
        }

        //fprintf(stdout, "I sleep for 1 second\n");
        nanosleep(&wait_spec, &remain_spec);
    }

    cdb2_close(repl_db);

    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);

    return NULL;
}

void stop_sync()
{
    do_repl = 0;

    if (pthread_join(sync_thread, NULL)) 
    {
        fprintf(stderr, "sync thread didn't join back :(\n");
    }

}

/* privates */
static LOG_INFO handle_record(LOG_INFO prev_info)
{
    /* vars for 1 record */
    void* blob;
    int blob_len;
    char* lsn, *gen, *timestamp, *token;
    const char delim[2] = ":";
    int64_t rectype; 
    int rc, file, offset;

    lsn = (char *) cdb2_column_value(repl_db, 0);
    rectype = *(int64_t *) cdb2_column_value(repl_db, 1);
    gen = (char *) cdb2_column_value(repl_db, 2);
    timestamp = (char *) cdb2_column_value(repl_db, 3);
    blob = cdb2_column_value(repl_db, 4);
    blob_len = cdb2_column_size(repl_db, 4);

    fprintf(stdout, "lsn: %s", lsn);

    /* TODO: consider using sqlite/ext/comdb2/tranlog.c */
    token = strtok(lsn, delim);    
    if (token)
    {
        /* assuming it's {#### */
        file = atoi(token + 1);
    }
    else
    {
        file = -1;
    }

    token = strtok(NULL, delim);
    if (token)
    {
        token[strlen(token) - 1] = '\0';
        offset = atoi(token);
    }
    else
    {
        offset = -1;
    }
    
    fprintf(stdout, ": %d:%d, rectype: %ld\n", file, offset, rectype);
    

    // TODO: implement this to use __rep_apply for one log record
    // Change it later
    printf("I'm expecting offset at: %u\n", 
            get_next_offset(thedb->bdb_env->dbenv, prev_info)); 
    if (prev_info.file < file)
    {
        rc = apply_log(thedb->bdb_env->dbenv, prev_info.file, 
                get_next_offset(thedb->bdb_env->dbenv, prev_info), 
                REP_NEWFILE, NULL, 0); 
    }
    rc = apply_log(thedb->bdb_env->dbenv, file, offset, 
            REP_LOG, blob, blob_len); 

    if (rc != 0)
    {
        fprintf(stderr, "Something went wrong with applying the logs\n");
    }

    LOG_INFO next_info;
    next_info.file = file;
    next_info.offset = offset;
    next_info.size = blob_len;

    return next_info;
}

/* data struct implementation */
static DB_Connection* get_connect(char* hostname)
{
    DB_Connection* cnct; 

    for (int i = 0; i < idx; i++) 
    {
        cnct = local_rep_dbs[i];

        if (strcmp(cnct->hostname, hostname) == 0) 
        {
            return cnct;
        }
    }

    return NULL;
}

static int insert_connect(char* hostname)
{
    if (idx >= cnct_len)
    {
        // on initialize
        if (!local_rep_dbs)
        {
            cnct_len = 4;
            local_rep_dbs = malloc(cnct_len * sizeof(DB_Connection*));
        }
        else 
        {
            cnct_len *= 2;
            DB_Connection** temp = realloc(local_rep_dbs, 
                    cnct_len * sizeof(*local_rep_dbs));

            if (temp) 
            {
                local_rep_dbs = temp; 
            }
            else 
            {
                fprintf(stderr, "Failed to realloc hostnames");
                return -1;
            }
        }

    }

    DB_Connection* cnct = malloc(sizeof(DB_Connection));
    cnct->hostname = hostname;
    cnct->last_cnct = 0;
    cnct->is_up = 1;

    local_rep_dbs[idx++] = cnct;

    return 0;
}

static void delete_connect(DB_Connection* cnct)
{
    free(cnct->hostname);
    free(cnct);
}

