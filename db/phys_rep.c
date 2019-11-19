#include "phys_rep.h"
#include <cdb2api.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>

#include <bdb_int.h>
#include "phys_rep_lsn.h"
#include "dbinc/rep_types.h"

#include "comdb2.h"
#include "truncate_log.h"

#include <parse_lsn.h>
#include <logmsg.h>

/* internal implementation */
typedef struct DB_Connection {
    char *hostname;
    char *dbname;
    uint32_t seed;
    int is_up; // was the db available for connection. Default nonzero if not
               // connected before
    time_t last_cnct;   // when was the last time we connected
    time_t last_failed; // when was the last time a connection failed
} DB_Connection;

int gbl_verbose_physrep = 0;
static DB_Connection ***local_rep_dbs = NULL;
static size_t tiers = 0;
static size_t curr_tier = 0;
static size_t tier_len = 0;
static size_t *cnct_len = NULL;
static size_t *cnct_idx = NULL;
static time_t retry_time = 3;

/* forward declarations */
static DB_Connection *get_connect(char *hostname);
static int insert_connect(char *hostname, char *dbname, size_t tier);
static void delete_connect(DB_Connection *cnct);
static LOG_INFO handle_record(LOG_INFO prev_info);
static int find_new_repl_db(void);
static DB_Connection *get_rand_connect(size_t tier);
static void *keep_in_sync(void *args);

static pthread_t sync_thread;
static volatile sig_atomic_t running;
/* internal implementation */

/* for replication */
static cdb2_hndl_tp *repl_db = NULL;
static int repl_db_connected = 0;
static DB_Connection *curr_cnct = NULL;

static volatile int do_repl;

int gbl_deferred_phys_flag = 0;

/* externs here */
extern struct dbenv *thedb;
extern int sc_ready(void);
extern char gbl_dbname[];

/* external API */
int add_replicant_host(char *hostname, char *dbname, size_t tier)
{
    return insert_connect(hostname, dbname, tier);
}

void cleanup_hosts()
{
    DB_Connection *cnct;

    /* Clean all but level 0 (cluster machines) */
    for (int i = 1; i < tiers; i++) {
        for (int j = 0; j < cnct_idx[i]; j++) {
            cnct = local_rep_dbs[i][j];
            delete_connect(cnct);
            local_rep_dbs[i][j] = NULL;
        }
        free(local_rep_dbs[i]);
        local_rep_dbs[i] = NULL;
    }
}

int start_replication(void)
{
    if (running) {
        logmsg(LOGMSG_ERROR, "Please call stop_replication before "
                             "starting a new thread!\n");
        return 1;
    }

    if (pthread_create(&sync_thread, NULL, keep_in_sync, NULL)) {
        logmsg(LOGMSG_ERROR, "Couldn't create thread to sync\n");
        return -1;
    }

    running = 1;
    return 0;
}

void close_repl_connection(void)
{
    curr_cnct->last_failed = time(NULL);
    curr_cnct->is_up = 0;
    cdb2_close(repl_db);
    repl_db_connected = 0;
    if (gbl_verbose_physrep) {
        logmsg(LOGMSG_USER, "%s closed handle\n", __func__);
    }
}

int gbl_physrep_register_interval = 3600;
static int last_register;
int gbl_blocking_physrep = 0;

static void *keep_in_sync(void *args)
{
    /* vars for syncing */
    int rc;
    volatile int64_t gen, highest_gen = 0;
    size_t sql_cmd_len = 150;
    char sql_cmd[sql_cmd_len];
    int do_truncate = 0;
    int now;
    LOG_INFO info;
    LOG_INFO prev_info;

    do_repl = 1;

    /* cannot call get_last_lsn if thedb is not ready */
    while (!sc_ready())
        sleep(1);

    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);

repl_loop:
    while (do_repl) {
        if (repl_db_connected && ((now = time(NULL)) - last_register) >
                                     gbl_physrep_register_interval) {
            close_repl_connection();
            if (gbl_verbose_physrep) {
                logmsg(LOGMSG_USER, "%s: forcing re-registration\n", __func__);
            }
        }

        if (repl_db_connected == 0) {
            if (find_new_repl_db() == 0) {
                /* do truncation to start fresh */
                info = get_last_lsn(thedb->bdb_env);
                do_truncate = 1;
            } else {
                sleep(1);
                continue;
            }
        }

        if (do_truncate) {
            info = get_last_lsn(thedb->bdb_env);
            prev_info = handle_truncation(repl_db, info);
            if (prev_info.file == 0) {
                close_repl_connection();
                continue;
            }

            gen = prev_info.gen;
            if (gbl_verbose_physrep)
                logmsg(LOGMSG_USER, "%s: gen: %" PRId64 "\n", __func__, gen);
            do_truncate = 0;
        }

        if (repl_db_connected == 0)
            continue;

        info = get_last_lsn(thedb->bdb_env);
        if (info.file <= 0) {
            sleep(1);
            continue;
        }

        prev_info = info;

        rc = snprintf(sql_cmd, sql_cmd_len,
                      "select * from comdb2_transaction_logs('{%u:%u}'%s)",
                      info.file, info.offset,
                      (gbl_blocking_physrep ? ", NULL, 1" : ""));
        if (rc < 0 || rc >= sql_cmd_len)
            logmsg(LOGMSG_ERROR, "sql_cmd buffer is not long enough!\n");

        if ((rc = cdb2_run_statement(repl_db, sql_cmd)) != CDB2_OK) {
            logmsg(LOGMSG_ERROR, "Couldn't query the database, retrying\n");
            close_repl_connection();
            continue;
        }

        if ((rc = cdb2_next_record(repl_db)) != CDB2_OK) {
            if (gbl_verbose_physrep)
                logmsg(LOGMSG_USER, "%s can't find the next record\n",
                       __func__);
            close_repl_connection();
            continue;
        }

        /* our log matches, so apply each record log received */
        while (do_repl && !do_truncate &&
               (rc = cdb2_next_record(repl_db)) == CDB2_OK) {
            /* check the generation id to make sure the master hasn't
             * switched */
            int64_t *rec_gen = (int64_t *)cdb2_column_value(repl_db, 2);
            if (rec_gen && *rec_gen > highest_gen) {
                int64_t new_gen = *rec_gen;
                if (gbl_verbose_physrep) {
                    logmsg(LOGMSG_USER,
                           "%s: My master changed, set truncate flag\n",
                           __func__);
                    logmsg(LOGMSG_USER,
                           "%s: gen: %" PRId64 ", rec_gen: %" PRId64 "\n",
                           __func__, gen, *rec_gen);
                }
                do_truncate = 1;
                highest_gen = new_gen;
                goto repl_loop;
            }
            prev_info = handle_record(prev_info);
        }

        if (rc != CDB2_OK_DONE || do_truncate) {
            do_truncate = 1;
            continue;
        }
        sleep(1);
    }

    close_repl_connection();
    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);

    return NULL;
}

int stop_replication()
{
    do_repl = 0;

    if (pthread_join(sync_thread, NULL)) {
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

int apply_log_procedure(unsigned int file, unsigned int offset, void *blob,
                        int blob_len, int newfile)
{
    int64_t rectype;
    if (!is_valid_lsn(file, offset)) {
        return 1;
    }
    rectype = newfile ? REP_NEWFILE : REP_LOG;

    return apply_log(thedb->bdb_env->dbenv, file, offset, rectype, blob,
                     blob_len);
}

/* privates */
static LOG_INFO handle_record(LOG_INFO prev_info)
{
    /* vars for 1 record */
    void *blob;
    int blob_len;
    char *lsn;
    int64_t *timestamp;
    int rc;
    unsigned int file, offset;

    lsn = (char *)cdb2_column_value(repl_db, 0);
    timestamp = (int64_t *)cdb2_column_value(repl_db, 3);
    blob = cdb2_column_value(repl_db, 4);
    blob_len = cdb2_column_size(repl_db, 4);

    if ((rc = char_to_lsn(lsn, &file, &offset)) != 0) {
        logmsg(LOGMSG_ERROR, "Could not parse lsn:%s\n", lsn);
    }

    if (gbl_deferred_phys_flag && timestamp) {
        time_t curr_time = time(NULL);
        /* Change this to sleep only once a second to test the
         * value of tunable */
        while (do_repl && (*timestamp + gbl_deferred_phys_update) > curr_time) {
            sleep(1);
            curr_time = time(NULL);
            if (gbl_verbose_physrep) {
                logmsg(LOGMSG_USER,
                       "Deferring update, commit-ts %" PRId64 ", "
                       "target %ld\n",
                       *timestamp, curr_time + gbl_deferred_phys_update);
            }
        }
    }

    if (do_repl) {
        /* check if we need to call new file flag */
        if (prev_info.file < file) {
            rc = apply_log(thedb->bdb_env->dbenv, prev_info.file,
                           get_next_offset(thedb->bdb_env->dbenv, prev_info),
                           REP_NEWFILE, NULL, 0);
        }

        rc = apply_log(thedb->bdb_env->dbenv, file, offset, REP_LOG, blob,
                       blob_len);
    } else {
        logmsg(LOGMSG_WARN, "Been asked to stop, drop LSN {%u:%u}\n", file,
               offset);
        return prev_info;
    }

    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "Something went wrong with applying the logs\n");
    }

    LOG_INFO next_info;
    next_info.file = file;
    next_info.offset = offset;
    next_info.size = blob_len;

    return next_info;
}

static int register_self()
{
    int rc;
    DB_Connection *cnct;
    size_t sql_len = 400;
    char get_tier[sql_len];
    LOG_INFO info = get_last_lsn(thedb->bdb_env);

    /* do a cleanup to get new list of tiered replicants */
    cleanup_hosts();

    /* TODO: Change this from local host to gbl_mynode */
    rc = snprintf(get_tier, sql_len,
                  "exec procedure "
                  "sys.cmd.register_replicant('%s', '%s', '%u', '%u')",
                  gbl_dbname, gbl_mynode, info.file, info.offset);

    if (rc < 0 || rc >= sql_len) {
        logmsg(LOGMSG_ERROR, "lua call buffer is not long enough!\n");
    }

    int64_t max_tier;

    /* update our table of possible connections */
    cdb2_hndl_tp *cluster;
    while (do_repl) {
        /* tier 0 is the cluster */
        if ((cnct = get_rand_connect(0)) == NULL) {
            logmsg(LOGMSG_FATAL, "Physical replicant cannot find cluster\n");
            abort();
        }

        if ((rc = cdb2_open(&cluster, cnct->dbname, cnct->hostname,
                            CDB2_DIRECT_CPU)) == 0) {

            cnct->last_cnct = time(NULL);
            cnct->is_up = 1;
            curr_cnct = cnct;
            max_tier = 0;
            if ((rc = cdb2_run_statement(cluster, get_tier)) == CDB2_OK) {
                while ((rc = cdb2_next_record(cluster)) == CDB2_OK) {
                    int64_t tier = *(int64_t *)cdb2_column_value(cluster, 0);
                    curr_tier =
                        (max_tier = (tier > max_tier) ? tier : max_tier);
                    char *dbname = (char *)cdb2_column_value(cluster, 1);
                    char *hostname = (char *)cdb2_column_value(cluster, 2);
                    insert_connect(hostname, dbname, tier);
                }
                cdb2_close(cluster);
                last_register = time(NULL);
                return 0;
            } else {
                logmsg(LOGMSG_ERROR, "%s query statement returned %d\n",
                       __func__, rc);
            }
            cdb2_close(cluster);
        } else {
            logmsg(LOGMSG_ERROR,
                   "Couldn't open connection to the cluster to find tier\n");
        }
        sleep(1);
    }

    logmsg(LOGMSG_WARN, "Been told to stop replicating\n");
    return 1;
}

int gbl_physrep_reconnect_penalty = 5;

static int seedsort(const void *arg1, const void *arg2)
{
    DB_Connection *cnct1 = (DB_Connection *)arg1;
    DB_Connection *cnct2 = (DB_Connection *)arg2;
    if (cnct1->seed > cnct2->seed)
        return 1;
    if (cnct1->seed < cnct2->seed)
        return -1;
    return 0;
}

static int find_new_repl_db(void)
{
    int rc;
    int notfound = 0;
    DB_Connection *cnct;

    assert(repl_db_connected == 0);
    while (do_repl) {

        while (repl_db == NULL && (rc = register_self()) != 0) {
            int level;
            notfound++;
            if (gbl_verbose_physrep)
                level = gbl_verbose_physrep;
            else if (notfound >= 10)
                level = LOGMSG_ERROR;
            else
                level = LOGMSG_DEBUG;
            logmsg(level, "%s failed to register against cluster, attempt %d\n",
                   __func__, notfound);
            sleep(1);
        }

        int j = 0;
        int i = curr_tier;
        int now;

        while (i >= 0) {

            for (j = 0; j < cnct_idx[i]; j++)
                local_rep_dbs[i][j]->seed = rand();

            /* Random sort this tier */
            qsort(local_rep_dbs[i], cnct_idx[i], sizeof(**local_rep_dbs),
                  seedsort);
            for (j = 0; j < cnct_idx[i]; j++) {
                cnct = local_rep_dbs[i][j];
                if (((now = time(NULL)) - cnct->last_failed) >
                    gbl_physrep_reconnect_penalty) {
                    if (gbl_verbose_physrep) {
                        logmsg(LOGMSG_USER,
                               "%s connecting against mach %s "
                               "db %s tier %d idx %d\n",
                               __func__, cnct->hostname, cnct->dbname, i, j);
                    }
                    if ((rc = cdb2_open(&repl_db, cnct->dbname, cnct->hostname,
                                        CDB2_DIRECT_CPU)) == 0 &&
                        (rc = cdb2_run_statement(repl_db, "select 1")) ==
                            CDB2_OK) {
                        while (cdb2_next_record(repl_db) == CDB2_OK)
                            ;
                        logmsg(LOGMSG_INFO,
                               "Attached to '%s' db '%s' for replication\n",
                               cnct->hostname, cnct->dbname);

                        cnct->last_cnct = time(NULL);
                        cnct->is_up = 1;
                        curr_cnct = cnct;
                        repl_db_connected = 1;
                        return 0;
                    } else {
                        if (gbl_verbose_physrep) {
                            logmsg(LOGMSG_USER,
                                   "%s setting mach %s "
                                   "db %s tier %d idx %d last_fail\n",
                                   __func__, cnct->hostname, cnct->dbname, i,
                                   j);
                        }
                        cnct->last_failed = time(NULL);
                    }
                } else {
                    if (gbl_verbose_physrep) {
                        logmsg(LOGMSG_USER,
                               "%s skipping mach %s db %s tier %d idx %d: on "
                               "recent last_fail @%ld vs %d\n",
                               __func__, cnct->hostname, cnct->dbname, i, j,
                               cnct->last_failed, now);
                    }
                }
            }
            if (gbl_verbose_physrep) {
                logmsg(LOGMSG_USER,
                       "%s: couldn't connect to any machine in "
                       "tier %d\n",
                       __func__, i);
            }
            i--;
        }

        if (gbl_verbose_physrep) {
            logmsg(LOGMSG_USER,
                   "%s: couldn't connect to any machine, sleeping for 1\n",
                   __func__);
        }

        sleep(1);
    }

    logmsg(LOGMSG_WARN, "Stopping replication\n");
    return -1;
}

static DB_Connection *get_rand_connect(size_t tier)
{
    if (tier > tiers) {
        logmsg(LOGMSG_ERROR, "Attempted to access tier not assigned\n");
        return NULL;
    }

    DB_Connection *cnct;
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

    while (cnct->last_failed + retry_time > time(NULL)) {
        logmsg(LOGMSG_WARN, "Timeout for %s\n", cnct->hostname);
        nanosleep(&wait_spec, &remain_spec);

        rand_idx = rand() % cnct_idx[tier];
        cnct = local_rep_dbs[tier][rand_idx];
    }

    return cnct;
}

static int insert_connect(char *hostname, char *dbname, size_t tier)
{
    if (cnct_len == NULL) {
        tier_len = 4;
        cnct_len = calloc(tier_len, sizeof(*cnct_len));
        cnct_idx = calloc(tier_len, sizeof(*cnct_idx));
        /* populate our 2D array */
        local_rep_dbs = malloc(tier_len * sizeof(*local_rep_dbs));
    }

    if (tier >= tier_len) {
        size_t old_tier_len = tier_len;

        if (tier > 1024) {
            logmsg(LOGMSG_ERROR,
                   "Please keep tier sizes manageable (tier < 1024).\n");
            return 1;
        }

        while (tier_len <= tier) {
            tier_len *= 2;
        }
        /* update our max tier */
        tiers = tier;

        DB_Connection ***temp_cnct =
            realloc(local_rep_dbs, tier_len * sizeof(*local_rep_dbs));
        size_t *tmp_len = realloc(cnct_len, tier_len * sizeof(*cnct_len));
        size_t *tmp_idx = realloc(cnct_idx, tier_len * sizeof(*cnct_idx));

        if (temp_cnct) {
            local_rep_dbs = temp_cnct;
            cnct_len = tmp_len;
            cnct_idx = tmp_idx;
        } else {
            logmsg(LOGMSG_ERROR, "Failed to realloc hostnames");
            return -1;
        }

        for (int i = old_tier_len; i < tier_len; i++) {
            cnct_len[i] = 0;
            cnct_idx[i] = 0;
        }
    }

    if (cnct_idx[tier] >= cnct_len[tier]) {
        // on initialize
        if (cnct_len[tier] == 0) {
            cnct_len[tier] = 4;

            local_rep_dbs[tier] =
                malloc(cnct_len[tier] * sizeof(**local_rep_dbs));
        } else {
            cnct_len[tier] *= 2;
            DB_Connection **temp = realloc(
                local_rep_dbs[tier], cnct_len[tier] * sizeof(**local_rep_dbs));

            if (temp) {
                local_rep_dbs[tier] = temp;
            } else {
                logmsg(LOGMSG_ERROR, "Failed to realloc hostnames");
                return -2;
            }
        }
    }

    tiers = (tier > tiers) ? tier : tiers;

    /* Don't add same machine multiple times */
    for (int i = 0; i < cnct_idx[tier]; i++) {
        DB_Connection *c = local_rep_dbs[tier][i];
        if ((strcmp(c->hostname, hostname) == 0) &&
            strcmp(c->dbname, dbname) == 0) {
            logmsg(LOGMSG_DEBUG, "%s mach %s db %s found\n", __func__, hostname,
                   dbname);
            return 0;
        }
    }

    DB_Connection *cnct = malloc(sizeof(*cnct));
    cnct->hostname = strdup(hostname);
    cnct->dbname = strdup(dbname);
    cnct->last_cnct = 0;
    cnct->last_failed = 0;
    cnct->is_up = 1;

    local_rep_dbs[tier][cnct_idx[tier]++] = cnct;

    return 0;
}

static void delete_connect(DB_Connection *cnct)
{
    free(cnct->hostname);
    free(cnct->dbname);
    cnct->hostname = cnct->dbname = NULL;
    free(cnct);
}
