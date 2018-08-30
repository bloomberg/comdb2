#include "phys_rep.h"
#include <cdb2api.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>
#include <time.h>

#include "phys_rep_lsn.h"
#include "dbinc/rep_types.h"

#include "comdb2.h"
#include "truncate_log.h"

#include <parse_lsn.h>
#include <logmsg.h>

/* internal implementation */
typedef struct DB_Connection {
    char *hostname;
    int is_up;            // was the db available for connection. Default nonzero if not connected before 
    time_t last_cnct;     // when was the last time we connected
    time_t last_failed;   // when was the last time a connection failed
} DB_Connection;

static DB_Connection ***local_rep_dbs = NULL;
static size_t tiers = 0;
static size_t curr_tier = 0;
static size_t tier_len = 0;
static size_t* cnct_len = NULL;
static size_t* cnct_idx = NULL;

static time_t retry_time = 3;

/* forward declarations */
static DB_Connection* get_connect(char* hostname);
static int insert_connect(char* hostname, size_t tier);
static void delete_connect(DB_Connection* cnct);
static LOG_INFO handle_record();
static void failed_connect();
static int find_new_repl_db();
static DB_Connection* get_rand_connect(size_t tier);
static void* keep_in_sync(void* args);

static pthread_t sync_thread;
static volatile sig_atomic_t running;
/* internal implementation */

/* for replication */
static cdb2_hndl_tp* repl_db;
static DB_Connection* curr_cnct;
static char* repl_db_name;

static volatile int do_repl;

int gbl_deferred_phys_flag = 0;

/* externs here */
extern struct dbenv* thedb;
extern int sc_ready(void);
extern char gbl_dbname[];

/* external API */
int set_repl_db_name(char* name)
{
    int rc;
    repl_db_name = name;

    /* TODO: may want to verify local db exists */
    rc = 0;

    return rc;
}

int add_replicant_host(char *hostname, size_t tier) 
{
    return insert_connect(hostname, tier);
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

    for (int i = 0; i < tiers; i++)
    {
        for (int j = 0; j < cnct_idx[i]; j++)
        {
            cnct = local_rep_dbs[i][j];
            delete_connect(cnct);
            local_rep_dbs[i][j] = NULL;
        }
        free(local_rep_dbs[i]);
        local_rep_dbs[i] = NULL;
    }

    free(repl_db_name);
    repl_db_name = NULL;

    free(local_rep_dbs);
    local_rep_dbs = NULL;

    cdb2_close(repl_db);
}

int start_replication()
{
    if (running)
    {
        logmsg(LOGMSG_ERROR, "Please call stop_replication before "
                "starting a new thread!\n");
        return 1;
    }

    /* initialize a random seed for db connections */
    srand(time(NULL));

    if (pthread_create(&sync_thread, NULL, keep_in_sync, NULL))
    {
        logmsg(LOGMSG_ERROR, "Couldn't create thread to sync\n");
        return -1;
    }

    running = 1;
    return 0;
}

static void* keep_in_sync(void* args)
{
    /* vars for syncing */
    int rc;
    volatile int64_t gen;
    struct timespec wait_spec;
    struct timespec remain_spec;
    // char* sql_cmd = "select * from comdb2_transaction_logs('{@file:@offset}')"; 
    size_t sql_cmd_len = 100;
    char sql_cmd[sql_cmd_len];
    LOG_INFO info;
    LOG_INFO prev_info;


    do_repl = 1;
    wait_spec.tv_sec = 1;
    wait_spec.tv_nsec = 0;

    /* cannot call get_last_lsn if thedb is not ready */
    while (!sc_ready())
        nanosleep(&wait_spec, &remain_spec);

    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);

    /* get a fresh db connection */
    if (find_new_repl_db() == 0)
    {
        /* do truncation to start fresh */
        info = get_last_lsn(thedb->bdb_env);
        prev_info = handle_truncation(repl_db, info);
        gen = prev_info.gen;
        fprintf(stderr, "gen: %lld\n", gen);
    }

    while(do_repl)
    {
        info = get_last_lsn(thedb->bdb_env);
        prev_info = info;

        rc = snprintf(sql_cmd, sql_cmd_len, 
                "select * from comdb2_transaction_logs('{%u:%u}')", 
                info.file, info.offset);
        if (rc < 0 || rc >= sql_cmd_len)
            logmsg(LOGMSG_ERROR, "sql_cmd buffer is not long enough!\n");

        if ((rc = cdb2_run_statement(repl_db, sql_cmd)) != CDB2_OK)
        {
            logmsg(LOGMSG_ERROR, "Couldn't query the database, retrying\n");
            failed_connect();
            continue;
        }

        /* queries one extra record: our last record, so skip this record */
        if ((rc = cdb2_next_record(repl_db)) != CDB2_OK)
        {
            logmsg(LOGMSG_WARN, "Can't find the next record\n");

            if (rc == CDB2_OK_DONE)
            {
                fprintf(stderr, "Let's do truncation\n");
                prev_info = handle_truncation(repl_db, info);
            }
        }
        else
        {
            int broke_early = 0;

            /* our log matches, so apply each record log received */
            while((rc = cdb2_next_record(repl_db)) == CDB2_OK && do_repl)
            {
                /* check the generation id to make sure the master hasn't switched */
                int64_t* rec_gen =  (int64_t *) cdb2_column_value(repl_db, 2);
                if (rec_gen && *rec_gen > gen)
                {
                    int64_t new_gen = *rec_gen;
                    logmsg(LOGMSG_WARN, "My master changed, do truncation!\n");
                    /* fprintf(stderr, "gen: %lld, rec_gen: %lld\n", gen, *rec_gen); */
                    prev_info = handle_truncation(repl_db, info);
                    gen = new_gen;
                    /* fprintf(stderr, "new gen: %lld, prev_rec_gen: %u\n", gen, prev_info.gen); */
                    broke_early = 1;
                    break;
                }
                prev_info = handle_record(prev_info);

            }

            /* check we finished correctly */
            if ((!broke_early && rc != CDB2_OK_DONE ) || 
                    (broke_early && rc != CDB2_OK))
            {
                logmsg(LOGMSG_ERROR, "Had an error or replication was stopped%d\n", rc);
                fprintf(stderr, "rc=%d\n", rc);
            }

        }

        nanosleep(&wait_spec, &remain_spec);
    }

    cdb2_close(repl_db);

    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);

    return NULL;
}

int stop_replication()
{
    do_repl = 0;

    if (pthread_join(sync_thread, NULL)) 
    {
        logmsg(LOGMSG_ERROR, "sync thread didn't join back :(\n");
        return 1;
    }

    running = 0;
    return 0;
}

/* stored procedure functions */
int is_valid_lsn(unsigned int file, unsigned int offset)
{
    LOG_INFO info = get_last_lsn(thedb->bdb_env);

    return file == info.file && 
        offset == get_next_offset(thedb->bdb_env->dbenv, info);
}


int apply_log_procedure(unsigned int file, unsigned int offset,
        void* blob, int blob_len, int newfile)
{
    int64_t rectype;
    if (!is_valid_lsn(file, offset))
    {
        return 1;
    }
    rectype = newfile ? REP_NEWFILE : REP_LOG;

    return apply_log(thedb->bdb_env->dbenv, file, offset, 
            rectype, blob, blob_len); 
}

/* privates */
static LOG_INFO handle_record(LOG_INFO prev_info)
{
    /* vars for 1 record */
    void* blob;
    int blob_len;
    char* lsn, *token;
    int64_t rectype; 
    int64_t* timestamp;
    int rc; 
    unsigned int file, offset;

    lsn = (char *) cdb2_column_value(repl_db, 0);
    rectype = *(int64_t *) cdb2_column_value(repl_db, 1);
    timestamp = (int64_t *) cdb2_column_value(repl_db, 3);
    blob = cdb2_column_value(repl_db, 4);
    blob_len = cdb2_column_size(repl_db, 4);

    if ((rc = char_to_lsn(lsn, &file, &offset)) != 0)
    {
        logmsg(LOGMSG_ERROR, "Could not parse lsn:%s\n", lsn);
    }    

    /* sleep while not ready to apply this log */
    if (gbl_deferred_phys_flag && timestamp)
    {
        time_t curr_time = time(NULL);
        if (*timestamp + gbl_deferred_phys_update > curr_time)
        {
            struct timespec wait_spec;
            struct timespec remain_spec;
            wait_spec.tv_sec = (*timestamp + gbl_deferred_phys_update) - curr_time;
            wait_spec.tv_nsec = 0;

            logmsg(LOGMSG_WARN, "Deferring log update for %ld seconds\n", 
                    wait_spec.tv_sec);

            nanosleep(&wait_spec, &remain_spec);
        }
    }
    
    if (do_repl)
    {
        /* check if we need to call new file flag */
        if (prev_info.file < file)
        {
            rc = apply_log(thedb->bdb_env->dbenv, prev_info.file, 
                    get_next_offset(thedb->bdb_env->dbenv, prev_info), 
                    REP_NEWFILE, NULL, 0); 
        }

        rc = apply_log(thedb->bdb_env->dbenv, file, offset, 
                REP_LOG, blob, blob_len); 
    }
    else
    {
        logmsg(LOGMSG_WARN, "Been asked to stop, drop LSN {%u:%u}\n",
                file, offset);
        return prev_info;
    }

    if (rc != 0)
    {
        logmsg(LOGMSG_ERROR, "Something went wrong with applying the logs\n");
    }

    LOG_INFO next_info;
    next_info.file = file;
    next_info.offset = offset;
    next_info.size = blob_len;

    return next_info;
}


static int find_new_repl_db()
{
    int rc;
    DB_Connection* cnct;

    size_t sql_len = 200;
    char* get_tier[sql_len];
    LOG_INFO info = get_last_lsn(thedb->bdb_env);

    /* TODO: Change this from local host to gbl_mynode */
    rc = snprintf(get_tier, sql_len, "exec procedure " 
        "sys.cmd.register_replicant('%s', '%s', '{%u:%u}')", 
        gbl_dbname, "localhost", info.file, info.offset);

    if (rc < 0 || rc >= sql_len)
        logmsg(LOGMSG_ERROR, "lua call buffer is not long enough!\n");

    {
        struct timespec wait_spec;
        struct timespec remain_spec;
        wait_spec.tv_sec = 1;
        wait_spec.tv_nsec = 0;

        while (do_repl && rc = cdb2_run_statemnt(repl_db, get_tier) != CDB2_OK)
        {
            logmsg(LOGMSG_ERROR, "Couldn't query the database\n");
            nanosleep(&wait_spec, &remain_spec);
        }
    }

    while(do_repl)
    {
        cnct = get_rand_connect(curr_tier);

        if ((rc = cdb2_open(&repl_db, repl_db_name, 
                        cnct->hostname, CDB2_DIRECT_CPU)) == 0)
        {
            logmsg(LOGMSG_WARN, "Attached to '%s' and db '%s' for replication\n", 
                    cnct->hostname,
                    repl_db_name);

            cnct->last_cnct = time(NULL);
            cnct->is_up = 1;
            curr_cnct = cnct;

            return 0;
        }

        logmsg(LOGMSG_WARN, "Couldn't connect to %s\n", cnct->hostname);
    }

    logmsg(LOGMSG_WARN, "Stopping replication\n");
    return -1;
}

static void failed_connect()
{
    curr_cnct->last_failed = time(NULL);
    curr_cnct->is_up = 0;
    cdb2_close(repl_db);

    find_new_repl_db();
}

/* TODO: not 2D arrary aweare */
static DB_Connection* get_rand_connect(size_t tier)
{
    if (tier > tiers)
    {
        logmsg(LOGMSG_ERROR, "Attempted to access tier not assigned\n");
        return NULL;
    }

    DB_Connection* cnct;
    size_t rand_idx;

    /* wait period */
    int64_t ratio = 1000000000;
    int64_t ns = retry_time * ratio / cnct_idx[tier];

    struct timespec wait_spec;
    struct timespec remain_spec;

    wait_spec.tv_sec = ns / ratio;
    wait_spec.tv_nsec = ns % ratio;

    rand_idx = rand() % cnct_idx[tier];
    cnct = local_rep_dbs[tier][rand_idx];

    while(cnct->last_failed + retry_time > time(NULL))
    {
        logmsg(LOGMSG_WARN, "Timeout for %s\n", cnct->hostname);
        nanosleep(&wait_spec, &remain_spec);

        rand_idx = rand() % cnct_idx[tier];
        cnct = local_rep_dbs[tier][rand_idx];
    }

    return cnct;
}

/* data struct implementation */
static DB_Connection* get_connect(char* hostname)
{
    DB_Connection* cnct; 

    for (int i = 0; i < tiers; i++) 
    {
        for (int j = 0; j < cnct_idx[i]; j++)
        {
            cnct = local_rep_dbs[i][j];

            if (strcmp(cnct->hostname, hostname) == 0) 
            {
                return cnct;
            }
        }
    }

    return NULL;
}

static int insert_connect(char* hostname, size_t tier)
{
    if (cnct_len == NULL)
    {
        tier_len = 4;
        cnct_len = calloc(tier_len, sizeof(*cnct_len));
        cnct_idx = calloc(tier_len, sizeof(*cnct_idx));
        /* populate our 2D array */
        local_rep_dbs = malloc(tier_len * sizeof(*local_rep_dbs));
    }

    if (tier >= tier_len)
    {
        size_t old_tier_len = tier_len;

        if (tier_len > 1024)
        {
            logmsg(LOGMSG_ERROR, "Please keep tier sizes manageable (tier < 1024).\n");
            return 1;
        }

        while (tier_len <= tier)
        {
            tier_len *= 2;
        }
        /* update our max tier */ 
        tiers = tier;

        DB_Connection*** temp_cnct = 
            realloc(local_rep_dbs, tier_len * sizeof(*local_rep_dbs));
        size_t* tmp_len = realloc(cnct_len, tier_len * sizeof(*cnct_len));
        size_t* tmp_idx = realloc(cnct_idx, tier_len * sizeof(*cnct_idx));

        if (temp_cnct)
        {
            local_rep_dbs = temp_cnct;
            cnct_len = tmp_len;
            cnct_idx = tmp_idx;
        }
        else
        {
            logmsg(LOGMSG_ERROR, "Failed to realloc hostnames");
            return -1;
        }


        for (int i = old_tier_len; i < tier_len; i++)
        {
            cnct_len[i] = 0;
            cnct_idx[i] = 0;
        }
    }

    if (cnct_idx[tier] >= cnct_len[tier])
    {
        // on initialize
        if (cnct_len[tier] == 0)
        {
            cnct_len[tier] = 4;
            
            local_rep_dbs[tier] = malloc(cnct_len[tier] * sizeof(**local_rep_dbs));
        }
        else 
        {
            cnct_len[tier] *= 2;
            DB_Connection** temp = realloc(local_rep_dbs[tier], 
                    cnct_len[tier] * sizeof(**local_rep_dbs));

            if (temp) 
            {
                local_rep_dbs[tier] = temp; 
            }
            else 
            {
                logmsg(LOGMSG_ERROR, "Failed to realloc hostnames");
                return -2;
            }
        }

    }

    DB_Connection* cnct = malloc(sizeof(*cnct));
    cnct->hostname = hostname;
    cnct->last_cnct = 0;
    cnct->last_failed = 0;
    cnct->is_up = 1;

    local_rep_dbs[tier][cnct_idx[tier]++] = cnct;

    return 0;
}

static void delete_connect(DB_Connection* cnct)
{
    free(cnct->hostname);
    cnct->hostname = NULL;
    free(cnct);
}
