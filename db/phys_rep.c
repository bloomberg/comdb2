/*
   Copyright 2023 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

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
#include <bdbglue.h>
#include "phys_rep_lsn.h"
#include "dbinc/rep_types.h"

#include "comdb2.h"
#include "truncate_log.h"
#include "reversesql.h"
#include "reverse_conn.h"

#include <parse_lsn.h>
#include <logmsg.h>

/* internal implementation */
typedef struct DB_Connection {
    char *hostname;
    char *dbname;
    uint32_t seed;
    int is_up; // was the db available for connection. Default non-zero if not
               // connected before
    time_t last_cnct;   // when was the last time we connected
    time_t last_failed; // when was the last time a connection failed
} DB_Connection;

#define physrep_logmsg(lvl, ...)                                               \
    do {                                                                       \
        logmsg(lvl, "physrep: " __VA_ARGS__);                                  \
    } while (0)

int gbl_physrep_debug = 0;
int gbl_physrep_register_interval = 600; // force re-registration every 10 mins
int gbl_physrep_reconnect_penalty = 0;
int gbl_blocking_physrep = 0;
int gbl_physrep_fanout = 8;
int gbl_physrep_max_candidates = 6;
int gbl_physrep_max_pending_replicants = 10;
int gbl_deferred_phys_flag = 0;
int gbl_physrep_source_nodes_refresh_freq_sec = 10;
int gbl_physrep_slow_replicant_check_freq_sec = 10;
int gbl_physrep_keepalive_freq_sec = 10;
int gbl_physrep_check_minlog_freq_sec = 10;
int gbl_physrep_hung_replicant_check_freq_sec = 10;
int gbl_physrep_hung_replicant_threshold = 60;
int gbl_physrep_revconn_check_interval = 60;
int gbl_physrep_update_registry_interval = 60;
int gbl_physrep_shuffle_host_list = 0;
int gbl_physrep_i_am_metadb = 0;
int gbl_started_physrep_threads = 0;

unsigned int physrep_min_logfile;
unsigned int gbl_deferred_phys_update;

char *gbl_physrep_source_dbname;
char *gbl_physrep_source_host;
char *gbl_physrep_metadb_name;
char *gbl_physrep_metadb_host;

char *gbl_physrep_repl_name = NULL;
char *gbl_physrep_repl_host = NULL;

static int repl_db_connected = 0;

static pthread_t physrep_worker_thread;
static pthread_t physrep_watcher_thread;

static volatile sig_atomic_t physrep_worker_running;
static volatile sig_atomic_t physrep_watcher_running;

static volatile sig_atomic_t stop_physrep_worker;
static volatile sig_atomic_t stop_physrep_watcher;

static DB_Connection **repl_dbs = NULL;
static size_t repl_dbs_sz;

reverse_conn_handle_tp *rev_conn_hndl = NULL;

static int last_register;

static int add_replicant_host(char *hostname, char *dbname);
static void dump_replicant_hosts(void);
static void delete_replicant_host(DB_Connection *cnct);

extern struct dbenv *thedb;
extern int gbl_replicant_retry_on_not_durable;
extern char gbl_dbname[];

int sc_ready(void);
int is_commit(u_int32_t rectype);
int physrep_bdb_wait_for_seqnum(bdb_state_type *bdb_state, DB_LSN *lsn, void *data);

static hash_t *physrep_fanout = NULL;
static pthread_mutex_t fanout_lk = PTHREAD_MUTEX_INITIALIZER;

struct fanout_override {
    char *dbname;
    int fanout;
};

void physrep_fanout_override(const char *dbname, int fanout)
{
    Pthread_mutex_lock(&fanout_lk);
    if (!physrep_fanout) {
        physrep_fanout = hash_init_strptr(offsetof(struct fanout_override, dbname));
    }
    struct fanout_override *fo = hash_find(physrep_fanout, &dbname);
    if (!fo) {
        fo = malloc(sizeof(struct fanout_override));
        fo->dbname = strdup(dbname);
        hash_add(physrep_fanout, fo);
    }
    fo->fanout = fanout;
    Pthread_mutex_unlock(&fanout_lk);
}

int physrep_fanout_get(const char *dbname)
{
    int fanout = gbl_physrep_fanout;
    Pthread_mutex_lock(&fanout_lk);
    if (physrep_fanout) {
        struct fanout_override *fo = hash_find(physrep_fanout, &dbname);
        if (fo) {
            fanout = fo->fanout;
        }
    }
    Pthread_mutex_unlock(&fanout_lk);
    return fanout;
}

static int physrep_fanout_print(void *obj, void *arg)
{
    struct fanout_override *fo = (struct fanout_override *)obj;
    physrep_logmsg(LOGMSG_USER, "dbname %s fanout %d\n", fo->dbname, fo->fanout);
    return 0;
}

void physrep_fanout_dump(void)
{
    Pthread_mutex_lock(&fanout_lk);
    if (physrep_fanout) {
        hash_for(physrep_fanout, physrep_fanout_print, NULL);
    } else {
        physrep_logmsg(LOGMSG_USER, "no fanout overrides\n");
    }
    Pthread_mutex_unlock(&fanout_lk);
}

void cleanup_hosts()
{
    DB_Connection *cnct;

    for (int i = 0; i < repl_dbs_sz; i++) {
        cnct = repl_dbs[i];
        delete_replicant_host(cnct);
        repl_dbs[i] = NULL;
    }
    free(repl_dbs);
    repl_dbs = NULL;
    repl_dbs_sz = 0;
}

static void set_repl_db_connected(char *dbname, char *host)
{
    gbl_physrep_repl_name = dbname;
    gbl_physrep_repl_host = host;
    repl_db_connected = 1;
}

static void set_repl_db_disconnected()
{
    repl_db_connected = 0;
    gbl_physrep_repl_name = NULL;
    gbl_physrep_repl_host = NULL;
}

static void close_repl_connection(DB_Connection *cnct, cdb2_hndl_tp *repl_db,
                                  const char *func, int line)
{
    cnct->last_failed = time(NULL);
    cnct->is_up = 0;
    set_repl_db_disconnected();
    cdb2_close(repl_db);
    repl_db = NULL;
    if (rev_conn_hndl) {
        // Set the 'done' flag to signal 'reversesql' plugin to perform the
        // cleanup and return
        pthread_mutex_lock(&rev_conn_hndl->mu);
        rev_conn_hndl->done = 1;
        pthread_cond_signal(&rev_conn_hndl->cond);
        pthread_mutex_unlock(&rev_conn_hndl->mu);
        rev_conn_hndl = NULL;
    }
    if (gbl_physrep_debug) {
        physrep_logmsg(LOGMSG_USER, "%s:%d Handle closed (called from %s:%d)\n",
                       __func__, __LINE__, func, line);
    }
}

// Append the list of nodes in the local cluster to the specified buffer.
static int append_quoted_local_hosts(char *buf, int buf_len, const char *separator) {
    const char *nodes[REPMAX];
    bdb_state_type *bdb_state = gbl_bdb_state;
    int bytes_written = 0;

    int num_nodes = net_get_sanctioned_node_list(bdb_state->repinfo->netinfo, REPMAX, nodes);
    for (int i = 0; i < num_nodes; ++i) {
        bytes_written += snprintf(buf+bytes_written, buf_len-bytes_written,
                                  "%s'%s'", (i>0) ? separator : "", nodes[i]);
        if (bytes_written >= buf_len) {
            goto err;
        }
    }

err:
    return bytes_written;
}

/*
  Append the list of nodes in the source cluster to the specified buffer.
  This information would be used by the registrar to tell which source
  node(s) would the replicant be replicating off of.
*/
static int append_quoted_source_hosts(char *buf, int buf_len, int *rc) {
    int bytes_written = 0;

    // Source db plays the role of 'replication metadb', if latter is not
    // explicitly specified.
    assert (gbl_physrep_source_dbname);
    assert (gbl_physrep_source_host);

    *rc = 0;

    // If the source host is not a valid 'tier', we will just use the source host-list
    // specified in the lrl.
    if (!is_valid_mach_class(gbl_physrep_source_host)) {
        int count = 0;
        char *saveptr = NULL;
        char *hosts = gbl_physrep_source_host;

        char *host = strtok_r(hosts, ",", &saveptr);
        while (host != NULL)  {
            if (host[0] == '@') ++host;
            bytes_written += snprintf(buf+bytes_written, buf_len-bytes_written, "%s'%s'", (count == 0) ? "" : ", ", host);
            ++count;

            host = strtok_r(NULL, ",", &saveptr);
        }
        return bytes_written;
    }

    // The specified host is a valid tier. Retrieve the host list from comdb2db.

    cdb2_hndl_tp *comdb2db;
    const char *query = "select m.name from machines as m, clusters as c, databases as d"
                        "  where c.name=@dbname and c.cluster_name=@class and "
                        "        m.cluster=c.cluster_machs and d.name=@dbname";

    static char *comdb2dbclass = NULL;
    static char *comdb2dbname = NULL;

    if (!comdb2dbname) {
        cdb2_get_comdb2db(&comdb2dbname);
        comdb2dbclass = (strcmp(comdb2dbname, "comdb3db") == 0) ? "dev" : "prod";
    }

    *rc = cdb2_open(&comdb2db, comdb2dbname, comdb2dbclass, 0);
    if (*rc) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to connect to %s@%s (err: %s rc: %d)\n", __func__, __LINE__,
                       comdb2dbname, comdb2dbclass, cdb2_errstr(comdb2db), *rc);
        goto err;
    }

    *rc = cdb2_bind_param(comdb2db, "dbname", CDB2_CSTRING, gbl_physrep_source_dbname, strlen(gbl_physrep_source_dbname));
    if (*rc) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to bind dbname (rc: %d)\n", __func__, __LINE__, *rc);
        goto err;
    }

    *rc = cdb2_bind_param(comdb2db, "class", CDB2_CSTRING, gbl_physrep_source_host,
                          strlen(gbl_physrep_source_host));
    if (*rc) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to bind class (rc: %d)\n", __func__, __LINE__, *rc);
        goto err;
    }

    *rc = cdb2_run_statement(comdb2db, query);
    if (*rc) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to execute query against %s@%s (err: %s rc: %d)\n",
                       __func__, __LINE__, comdb2dbname, comdb2dbclass,
                       cdb2_errstr(comdb2db), *rc);
        goto err;
    }

    int count = 0;
    while ((*rc = cdb2_next_record(comdb2db)) == CDB2_OK) {
        const char *host = (const char *)cdb2_column_value(comdb2db, 0);
        bytes_written += snprintf(buf+bytes_written, buf_len-bytes_written, "%s'%s'", (count == 0) ? "" : ", ", host);
        ++count;
    }
    if (*rc == CDB2_OK_DONE)
        *rc = 0;

    cdb2_close(comdb2db);
    return bytes_written;

err:
    cdb2_close(comdb2db);
    return -1;
}

static int get_local_hndl(cdb2_hndl_tp **hndl) {
    int rc = cdb2_open(hndl, gbl_dbname, "local", 0);
    if (rc != 0) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to connect to %s@%s (rc: %d)\n",
                       __func__, __LINE__, gbl_dbname, "local", rc);
        cdb2_close(*hndl);
    }
    if (gbl_physrep_debug) {
        physrep_logmsg(LOGMSG_USER, "%s:%d Returning handle for: %s@%s\n",
                       __func__, __LINE__, gbl_dbname, "local");
    }
    return rc;
}

static char **physrep_metadb_hosts = NULL;
static int physrep_metadb_host_count = 0;

static int get_metadb_hndl(cdb2_hndl_tp **hndl) {
    // Source db becomes 'replication metadb', if latter is not specified
    char *dbname = (gbl_physrep_metadb_name) ? gbl_physrep_metadb_name : gbl_physrep_source_dbname;
    char *host = (gbl_physrep_metadb_host) ? gbl_physrep_metadb_host : gbl_physrep_source_host;


    if (!is_valid_mach_class(host)) {
        static pthread_mutex_t physrep_metadb_hosts_mu = PTHREAD_MUTEX_INITIALIZER; // Protects physrep_metadb_hosts
        static __thread int current_physrep_metadb_host = 0;

        pthread_mutex_lock(&physrep_metadb_hosts_mu);

        if (physrep_metadb_hosts == NULL) {
            char *saveptr = NULL;
            char *hst = strtok_r(host, ",", &saveptr);

            physrep_metadb_host_count = 0;

            while (hst != NULL)  {
                if (hst[0] == '@') ++hst;
                ++physrep_metadb_host_count;
                physrep_metadb_hosts = realloc(physrep_metadb_hosts, sizeof(char *)*physrep_metadb_host_count);
                physrep_metadb_hosts[physrep_metadb_host_count-1] = strdup(hst);

                hst = strtok_r(NULL, ",", &saveptr);
            }
        }

        int max_connect_attempts = 10 * physrep_metadb_host_count;
        for (int i = 0; i < max_connect_attempts; ++i) {
            char *direct_host = physrep_metadb_hosts[current_physrep_metadb_host];

            // Move to the next host for the next attempt
            current_physrep_metadb_host = (current_physrep_metadb_host + 1) % physrep_metadb_host_count;

            int rc = cdb2_open(hndl, dbname, direct_host, CDB2_DIRECT_CPU);
            if (rc != 0) {
                physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to connect to %s@%s (rc: %d, attempt: %d)\n",
                               __func__, __LINE__, dbname, direct_host, rc, i+1);
                cdb2_close(*hndl);

                // Try to connect to other hosts in the list (if any)
                sleep(1);
                continue;
            }

            rc = cdb2_run_statement(*hndl, "select 1");
            if (rc != CDB2_OK) {
                physrep_logmsg(LOGMSG_ERROR, "%s:%d: Couldn't execute 'select 1' against %s@%s (rc: %d error: %s)\n",
                               __func__, __LINE__, dbname, direct_host, rc, cdb2_errstr(*hndl));

                // Try to connect to other hosts in the list (if any)
                sleep(1);
                continue;
            }
            while (cdb2_next_record(*hndl) == CDB2_OK) {}

            if (gbl_physrep_debug) {
                physrep_logmsg(LOGMSG_USER, "%s:%d Returning handle for: %s@%s\n",
                               __func__, __LINE__, dbname, direct_host);
            }
            pthread_mutex_unlock(&physrep_metadb_hosts_mu);
            return 0;
        }
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to connect to any host in the list\n",
                       __func__, __LINE__);
        pthread_mutex_unlock(&physrep_metadb_hosts_mu);
        return 1;
    }

    int rc = cdb2_open(hndl, dbname, host, 0);
    if (rc != 0) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to connect to %s@%s (rc: %d)\n",
                       __func__, __LINE__, dbname, host, rc);
        cdb2_close(*hndl);
        return rc;
    }

    if (gbl_physrep_debug) {
        physrep_logmsg(LOGMSG_USER, "%s:%d Returning handle for: %s@%s\n",
                       __func__, __LINE__, dbname, host);
    }
    return 0;
}

int physrep_get_metadb_or_local_hndl(cdb2_hndl_tp **hndl) {
    return (gbl_physrep_metadb_name || gbl_physrep_source_dbname)
      ?  get_metadb_hndl(hndl) : get_local_hndl(hndl);
}

static int update_registry(cdb2_hndl_tp *repl_metadb, const char *remote_dbname, const char *remote_host)
{
    const size_t nodes_list_sz = REPMAX * (255 + 1) + 3;
    char cmd[120 + nodes_list_sz];
    int bytes_written = 0;
    int rc;

    char *buf = cmd;
    size_t buf_len = sizeof(cmd);

    bytes_written += snprintf(buf + bytes_written, buf_len - bytes_written,
                              "exec procedure sys.physrep.update_registry"
                              "('%s', '%s', '%s', '%s', \"",
                              gbl_dbname, gbl_myhostname, (remote_dbname) ? remote_dbname : "NULL",
                              (remote_host) ? remote_host : "NULL");
    if (bytes_written >= buf_len) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Buffer is not long enough!\n", __func__, __LINE__);
        return 1;
    }

    bytes_written += append_quoted_local_hosts(buf + bytes_written, buf_len - bytes_written, " ");
    if (bytes_written >= buf_len) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Buffer is not long enough!\n", __func__, __LINE__);
        return 1;
    }

    bytes_written += snprintf(buf + bytes_written, buf_len - bytes_written, "\")");
    if (bytes_written >= buf_len) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Buffer is not long enough!\n", __func__, __LINE__);
        return 1;
    }

    if (gbl_physrep_debug) {
        physrep_logmsg(LOGMSG_USER, "%s:%d Executing: %s\n", __func__, __LINE__, cmd);
    }

    if ((rc = cdb2_run_statement(repl_metadb, cmd)) == CDB2_OK) {
        while ((rc = cdb2_next_record(repl_metadb)) == CDB2_OK)
            ;
    } else {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to execute (rc: %d)\n", __func__, __LINE__, rc);
        return 1;
    }
    return 0;
}

static int update_registry_periodic(const char *remote_dbname, const char *remote_host)
{
    cdb2_hndl_tp *repl_metadb = NULL;
    int rc;

    if (gbl_physrep_debug)
        logmsg(LOGMSG_USER, "%s: updating-registry %s/%s\n", __func__, remote_dbname, remote_host);

    if ((rc = physrep_get_metadb_or_local_hndl(&repl_metadb)) != 0) {
        return rc;
    }

    rc = update_registry(repl_metadb, remote_dbname, remote_host);

    cdb2_close(repl_metadb);
    return rc;
}

static int send_reset_nodes(const char *state)
{
    const size_t nodes_list_sz = REPMAX * (255+1) + 3;
    char cmd[120+nodes_list_sz];
    int bytes_written = 0;
    int rc = 0;

    char *buf = cmd;
    size_t buf_len = sizeof(cmd);

    bytes_written += snprintf(buf+bytes_written, buf_len-bytes_written,
                              "exec procedure sys.physrep.reset_nodes('%s', \"", gbl_dbname);
    if (bytes_written >= buf_len) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Buffer is not long enough!\n", __func__, __LINE__);
        return 1;
    }

    /* Only master can reset all the nodes in the cluster */
    if (thedb->master != gbl_myhostname) {
        bytes_written += snprintf(buf+bytes_written, buf_len-bytes_written, "'%s'", gbl_myhostname);
    } else {
        bytes_written += append_quoted_local_hosts(buf+bytes_written, buf_len-bytes_written, " ");
    }
    if (bytes_written >= buf_len) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Buffer is not long enough!\n", __func__, __LINE__);
        return 1;
    }

    bytes_written += snprintf(buf+bytes_written, buf_len-bytes_written, "\", '%s')", state);
    if (bytes_written >= buf_len) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Buffer is not long enough!\n", __func__, __LINE__);
        return 1;
    }

    cdb2_hndl_tp *repl_metadb;
    if ((rc = physrep_get_metadb_or_local_hndl(&repl_metadb)) != 0) {
        return rc;
    }

    if (gbl_physrep_debug) {
        physrep_logmsg(LOGMSG_USER, "%s:%d Executing: %s\n", __func__, __LINE__, cmd);
    }

    if ((rc = cdb2_run_statement(repl_metadb, cmd)) == CDB2_OK) {
        while ((rc = cdb2_next_record(repl_metadb)) == CDB2_OK)
            ;
        if (rc == CDB2_OK_DONE)
            rc = 0;
    } else {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to execute (rc: %d)\n", __func__, __LINE__, rc);
        rc = -1;
    }
    cdb2_close(repl_metadb);
    return rc;
}

char *physrep_master_cached = NULL;
int gbl_physrep_force_registration = 0;

int force_registration()
{
    int rtn = 0;
    if (gbl_physrep_force_registration) {
        logmsg(LOGMSG_USER, "%s: forcing registration on flag\n", __func__);
        rtn = 1;
        gbl_physrep_force_registration = 0;
    }

    if (!physrep_master_cached || ((strcmp(gbl_myhostname, physrep_master_cached)) != 0)) {
        free(physrep_master_cached);
        physrep_master_cached = strdup(gbl_myhostname);
        rtn = 1;
    }
    return rtn;
}

time_t gbl_physrep_last_applied_time;

/* stored procedure functions */
int is_valid_lsn(unsigned int file, unsigned int offset)
{
    LOG_INFO info = get_last_lsn(thedb->bdb_env);

    return file == info.file &&
           offset == get_next_offset(thedb->bdb_env->dbenv, info);
}

static LOG_INFO handle_record(cdb2_hndl_tp *repl_db, LOG_INFO prev_info)
{
    /* vars for 1 record */
    void *blob;
    int blob_len;
    char *lsn;
    int64_t *timestamp;
    int rc;
    unsigned int file, offset;
    int64_t *rectype;
    lsn = (char *)cdb2_column_value(repl_db, 0);
    rectype = (int64_t *)cdb2_column_value(repl_db, 1);
    timestamp = (int64_t *)cdb2_column_value(repl_db, 3);
    blob = cdb2_column_value(repl_db, 4);
    blob_len = cdb2_column_size(repl_db, 4);

    if ((rc = char_to_lsn(lsn, &file, &offset)) != 0) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d: Could not parse lsn %s\n",
                       __func__, __LINE__, lsn);
    }
    if (gbl_physrep_debug) {
        physrep_logmsg(LOGMSG_USER, "%s:%d: Processing record (lsn %d:%d)\n",
                       __func__, __LINE__, file, offset);
    }

    if (gbl_deferred_phys_flag && timestamp) {
        time_t curr_time = time(NULL);
        /* Change this to sleep only once a second to test the
         * value of tunable */
        while (stop_physrep_worker == 0 && (*timestamp + gbl_deferred_phys_update) > curr_time) {
            sleep(1);
            curr_time = time(NULL);
            if (gbl_physrep_debug) {
                physrep_logmsg(LOGMSG_USER, "%s:%d: Deferring update, commit-ts %" PRId64 ", target %ld\n",
                               __func__, __LINE__, *timestamp, curr_time + gbl_deferred_phys_update);
            }
        }
    }

    if (stop_physrep_worker == 0) {
        /* check if we need to call new file flag */
        if (prev_info.file < file) {
            rc = apply_log(thedb->bdb_env->dbenv, prev_info.file,
                           get_next_offset(thedb->bdb_env->dbenv, prev_info),
                           REP_NEWFILE, NULL, 0);
            if (rc != 0) {
                physrep_logmsg(LOGMSG_FATAL, "%s:%d: Something went wrong with applying the logs (rc: %d)\n",
                               __func__, __LINE__, rc);
                exit(1);
            }
        }

        rc = apply_log(thedb->bdb_env->dbenv, file, offset, REP_LOG, blob,
                       blob_len);

        if (is_commit((u_int32_t)*rectype)) {
            if (gbl_physrep_debug) {
                physrep_logmsg(LOGMSG_USER, "%s:%d: Got commit record (lsn %d:%d), going to wait for other nodes to ack\n",
                               __func__, __LINE__, file, offset);
            }

            DB_LSN lsn;
            lsn.file = file;
            lsn.offset = offset;
            int start = comdb2_time_epochms();
            rc = physrep_bdb_wait_for_seqnum(thedb->bdb_env, &lsn, blob);
            if (rc != 0) {
                physrep_logmsg(LOGMSG_ERROR, "%s:%d bdb_wait_for_seqnum_from_all() failed (rc = %d)\n",
                               __func__, __LINE__, rc);
            } else {
                if (gbl_physrep_debug) {
                    physrep_logmsg(LOGMSG_USER, "%s:%d: Got ACKs, (waited: %d ms)\n",
                                   __func__, __LINE__, comdb2_time_epochms()-start);
                }
            }
        }
    } else {
        physrep_logmsg(LOGMSG_WARN, "Been asked to stop, drop lsn (%u:%u)\n", file, offset);
        return prev_info;
    }

    if (rc != 0) {
        physrep_logmsg(LOGMSG_FATAL, "%s:%d: Something went wrong with applying the logs (rc: %d)\n", __func__,
                       __LINE__, rc);
        exit(1);
    }

    LOG_INFO next_info;
    next_info.file = file;
    next_info.offset = offset;
    next_info.size = blob_len;

    return next_info;
}

static int register_self(cdb2_hndl_tp *repl_metadb)
{
    const size_t nodes_list_sz = REPMAX * (255+1) + 3;
    char cmd[120+nodes_list_sz];
    int bytes_written = 0;
    int rc;

    if (gbl_physrep_debug) {
        physrep_logmsg(LOGMSG_USER, "%s:%d Registering self\n", __func__, __LINE__);
    }

    // Reset all the nodes from this physical replication cluster; and mark them
    // 'Inactive'.
    //
    // This is required to ensure that the metadb does not return one the nodes
    // of this cluster as a potential source when one of the nodes tries to
    // re-register as a physical replicant.
    if (gbl_physrep_debug) {
        physrep_logmsg(LOGMSG_USER, "%s:%d Send-reset nodes\n", __func__, __LINE__);
    }
    rc = send_reset_nodes("Inactive");
    if (rc != 0) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to reset info in replication metadb tables (rc: %d)\n",
                       __func__, __LINE__, rc);
        return 1;
    }

    char *buf = cmd;
    size_t buf_len = sizeof(cmd);
    LOG_INFO info = get_last_lsn(thedb->bdb_env);

    bytes_written = snprintf(buf+bytes_written, buf_len-bytes_written,
                  "exec procedure sys.physrep.register_replicant('%s', '%s', '%u:%u', '%s', \"",
                  gbl_dbname, gbl_myhostname, info.file, info.offset, gbl_physrep_source_dbname);
    if (bytes_written >= buf_len) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Buffer is not long enough!\n", __func__, __LINE__);
        return 1;
    }

    bytes_written += append_quoted_source_hosts(buf+bytes_written, buf_len-bytes_written, &rc);
    if (rc != 0 || bytes_written >= buf_len) {
        if (rc != 0) {
            physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to append source host(s)!\n", __func__, __LINE__);
            return rc;
        }
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Buffer is not long enough!\n", __func__, __LINE__);
        return 1;
    }

    bytes_written += snprintf(buf+bytes_written, buf_len-bytes_written, "\")");
    if (bytes_written >= buf_len) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Buffer is not long enough!\n", __func__, __LINE__);
        return 1;
    }

    // Do a cleanup to get new list of tiered replicants
    cleanup_hosts();

    while (stop_physrep_worker == 0) {
        if (gbl_physrep_debug) {
            physrep_logmsg(LOGMSG_USER, "%s:%d Executing: %s\n", __func__, __LINE__, cmd);
        }

        int candidate_leaders_count = 0;
        if ((rc = cdb2_run_statement(repl_metadb, cmd)) == CDB2_OK) {
            while ((rc = cdb2_next_record(repl_metadb)) == CDB2_OK) {
                char *dbname = (char *)cdb2_column_value(repl_metadb, 1);
                char *hostname = (char *)cdb2_column_value(repl_metadb, 2);

                add_replicant_host(hostname, dbname);

                ++ candidate_leaders_count;
            }
            last_register = time(NULL);

            if (candidate_leaders_count > 0) {
                if (gbl_physrep_debug) {
                    dump_replicant_hosts();
                }
                return 0;
            }
            if (gbl_physrep_debug)
                physrep_logmsg(LOGMSG_USER, "%s:%d: No candidate leaders! retrying registration in a second\n",
                               __func__, __LINE__);
        } else {
            physrep_logmsg(LOGMSG_ERROR, "%s:%d Query statement returned %d\n", __func__, __LINE__, rc);
            return rc;
        }
        sleep(1);
    }
    physrep_logmsg(LOGMSG_WARN, "Been told to stop replicating\n");
    return 1;
}

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

static DB_Connection *find_new_repl_db(cdb2_hndl_tp *repl_metadb, cdb2_hndl_tp **repl_db) {
    int rc, count = 0;
    DB_Connection *cnct;

    assert(repl_db_connected == 0);

    while (stop_physrep_worker == 0) {

        if (gbl_physrep_shuffle_host_list == 1) {
            qsort(repl_dbs, repl_dbs_sz, sizeof(DB_Connection *), seedsort);
        }

        int now;
        for (int i = 0; i < repl_dbs_sz; i++) {
            cnct = repl_dbs[i];

            if (((now = time(NULL)) - cnct->last_failed) <= gbl_physrep_reconnect_penalty) {
                if (gbl_physrep_debug) {
                    physrep_logmsg(LOGMSG_USER, "%s:%d Skipping mach %s@%s last_fail @%ld vs %d\n",
                                   __func__, __LINE__, cnct->hostname, cnct->dbname, cnct->last_failed, now);
                }
                continue;
            }

            if (gbl_physrep_debug) {
                physrep_logmsg(LOGMSG_USER, "%s:%d Connecting against mach %s db %s\n",
                               __func__, __LINE__, cnct->hostname, cnct->dbname);
            }

            rc = cdb2_open(repl_db, cnct->dbname, cnct->hostname, CDB2_DIRECT_CPU);
            if (rc != 0) {
                physrep_logmsg(LOGMSG_ERROR, "%s:%d: Couldn't connect to %s@%s (rc: %d error: %s)\n",
                               __func__, __LINE__, cnct->dbname,
                               cnct->hostname, rc, cdb2_errstr(*repl_db));
                cdb2_close(*repl_db);
                cnct->last_failed = time(NULL);
                continue;
            }

            rc = cdb2_run_statement(*repl_db, "select 1");
            if (rc != CDB2_OK) {
                physrep_logmsg(LOGMSG_ERROR, "%s:%d: Couldn't execute 'select 1' against %s@%s (rc: %d error: %s)\n",
                               __func__, __LINE__, cnct->dbname, cnct->hostname, rc, cdb2_errstr(*repl_db));
                cnct->last_failed = time(NULL);
                continue;
            }
            while (cdb2_next_record(*repl_db) == CDB2_OK) {
            }

            physrep_logmsg(LOGMSG_USER, "Attached to '%s' db '%s' for replication\n", cnct->hostname, cnct->dbname);

            /* Execute sys.physrep.update_registry() on the replication metadb cluster */
            rc = update_registry(repl_metadb, cnct->dbname, cnct->hostname);
            if (rc != 0) {
                physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to exec sys.physrep.update_registry() on %s:%s\n", __func__,
                               __LINE__, cnct->dbname, cnct->hostname);
            }

            cnct->last_cnct = time(NULL);
            cnct->is_up = 1;
            set_repl_db_connected(cnct->dbname, cnct->hostname);
            return cnct;
        }

        count++;
        if (count < 10) {
            physrep_logmsg(LOGMSG_USER,
                           "%s:%d: Couldn't connect to any of the replication source hosts, retrying in a second\n",
                           __func__, __LINE__);

            sleep(1);
        } else {
            physrep_logmsg(LOGMSG_USER,
                           "%s:%d: Couldn't connect to any of the replication source hosts, break to re-register\n",
                           __func__, __LINE__);
            return NULL;
        }
    }

    physrep_logmsg(LOGMSG_WARN, "Stopping replication\n");
    return NULL;
}

static void dump_replicant_hosts()
{
    for (int i = 0; i < repl_dbs_sz; i++) {
        DB_Connection *cnct = repl_dbs[i];
        physrep_logmsg(LOGMSG_USER, "%s:%d: %s:%s\n", __func__, __LINE__, cnct->hostname, cnct->dbname);
    }
}

static int add_replicant_host(char *hostname, char *dbname)
{

    /* Don't add same machine multiple times */
    for (int i = 0; i < repl_dbs_sz; i++) {
        DB_Connection *c = repl_dbs[i];
        if ((strcmp(c->hostname, hostname) == 0) && strcmp(c->dbname, dbname) == 0) {
            if (gbl_physrep_debug) {
                physrep_logmsg(LOGMSG_USER, "%s Found %s:%s not adding\n", __func__, hostname, dbname);
            }
            return 0;
        }
    }

    DB_Connection *cnct = malloc(sizeof(DB_Connection));
    cnct->hostname = strdup(hostname);
    cnct->dbname = strdup(dbname);
    cnct->last_cnct = 0;
    cnct->last_failed = 0;
    cnct->is_up = 1;
    cnct->seed = rand();

    repl_dbs = realloc(repl_dbs, (repl_dbs_sz + 1) * sizeof(DB_Connection *));
    repl_dbs[repl_dbs_sz ++] = cnct;

    if (gbl_physrep_debug) {
        physrep_logmsg(LOGMSG_USER, "%s:%d Adding %s:%s\n", __func__, __LINE__, hostname, dbname);
    }

    return 0;
}

static void delete_replicant_host(DB_Connection *cnct)
{
    free(cnct->hostname);
    free(cnct->dbname);
    cnct->hostname = cnct->dbname = NULL;
    free(cnct);
}

static int send_keepalive() {
    int rc = 0;
    char cmd[400];
    LOG_INFO info;

    info = get_last_lsn(thedb->bdb_env);

    rc = snprintf(cmd, sizeof(cmd),
                  "exec procedure sys.physrep.keepalive('%s', '%s', %u, %u)",
                  gbl_dbname, gbl_myhostname, info.file, info.offset);
    if (rc < 0 || rc >= sizeof(cmd)) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d: Buffer is not long enough!\n", __func__, __LINE__);
        return 1;
    }

    cdb2_hndl_tp *repl_metadb;
    if ((rc = physrep_get_metadb_or_local_hndl(&repl_metadb)) != 0) {
        return rc;
    }

    if (gbl_physrep_debug)
        physrep_logmsg(LOGMSG_USER, "%s:%d: Executing: %s\n", __func__, __LINE__, cmd);

    rc = cdb2_run_statement(repl_metadb, cmd);
    if (rc == CDB2_OK) {
        while (cdb2_next_record(repl_metadb) == CDB2_OK) {}
    } else if (gbl_physrep_debug)
        physrep_logmsg(LOGMSG_USER, "%s:%d Failed to send keepalive\n", __func__, __LINE__);

    cdb2_close(repl_metadb);
    return rc;
}

unsigned int physrep_min_filenum() {
    return physrep_min_logfile;
}

static int check_for_reverse_conn(cdb2_hndl_tp *hndl) {
    int rc;
    char cmd[400];
    int do_wait = 0;

    rc = snprintf(cmd, sizeof(cmd),
                  "exec procedure sys.physrep.should_wait_for_con('%s', '%s')",
                  gbl_dbname, (gbl_machine_class) ? gbl_machine_class : gbl_myhostname);

    if (rc < 0 || rc >= sizeof(cmd)) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d: Buffer is not long enough!\n", __func__, __LINE__);
        return -1;
    }

    if (gbl_physrep_debug)
        physrep_logmsg(LOGMSG_USER, "%s:%d Executing: %s\n", __func__, __LINE__, cmd);

    if ((rc = cdb2_run_statement(hndl, cmd)) == CDB2_OK) {
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
            int64_t val = *(int64_t *)cdb2_column_value(hndl, 0);
            do_wait = (val != 0) ? 1 : 0;
            if (gbl_physrep_debug) {
                physrep_logmsg(LOGMSG_USER, "%s:%d Will %s for connection from source node(s)\n",
                               __func__, __LINE__, (do_wait) ?  "wait" : "not wait");
            }
        }
        if (rc == CDB2_OK_DONE)
            rc = 0;
    }
    return (rc == 0) ? do_wait : -1;
}

void physrep_update_low_file_num(int *lowfilenum, int *local_lowfilenum) {
    unsigned int physrep_minfilenum;
    if ((get_dbtable_by_name("comdb2_physreps")) == NULL) {
        return;
    }

    physrep_minfilenum = physrep_min_filenum();
    if (physrep_minfilenum <= 0) {
        if (gbl_physrep_debug) {
            physrep_logmsg(LOGMSG_USER, "%s:%d: lowfilenum unchanged (physrep_minfilenum: %d)\n",
                           __func__, __LINE__, physrep_minfilenum);
        }
    } else {
        if (physrep_minfilenum <= *lowfilenum) {
            if (gbl_physrep_debug) {
                physrep_logmsg(LOGMSG_USER, "%s:%d: lowfilenum %d being changed "
                               "physical replicant(s) (physrep_minfilenum: %d)\n",
                               __func__, __LINE__, *lowfilenum, physrep_minfilenum);
            }
            *lowfilenum = physrep_minfilenum - 1;
        }
        if (physrep_minfilenum <= *local_lowfilenum) {
            *local_lowfilenum = physrep_minfilenum - 1;
        }
    }

    if (gbl_physrep_debug) {
        physrep_logmsg(LOGMSG_USER, "%s:%d: lowfilenum: %d (physrep_minfilenum: %d)\n",
                       __func__, __LINE__, *lowfilenum, physrep_minfilenum);
    }
}

static int slow_replicants_count(unsigned int *count) {
    char query[200];
    int rc = 0;

    *count = 0;

    sprintf(query,
            "select count(*) from comdb2_physreps where cast(NOW() as "
            "integer) - cast(last_keepalive as integer) >= %d",
            gbl_physrep_hung_replicant_threshold);

    cdb2_hndl_tp *repl_metadb;
    if ((rc = physrep_get_metadb_or_local_hndl(&repl_metadb)) != 0) {
        return rc;
    }

    rc = cdb2_run_statement(repl_metadb, query);
    if (rc == CDB2_OK) {
        while ((rc = cdb2_next_record(repl_metadb)) == CDB2_OK) {
            int64_t *val = (int64_t *)cdb2_column_value(repl_metadb, 0);
            *count = (unsigned int) *val;
        }
        if (rc == CDB2_OK_DONE)
            rc = 0;
    } else {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to execute (rc: %d)\n", __func__, __LINE__, rc);
    }

    cdb2_close(repl_metadb);
    return rc;
}

static int update_min_logfile(void) {
    const size_t nodes_list_sz = REPMAX * (255+1) + 3;
    char cmd[120+nodes_list_sz];
    char *buf;
    size_t buf_len;
    int bytes_written;
    int rc = 0;

    if (gbl_ready == 0)
        return 0;

    bytes_written = 0;
    buf = cmd;
    buf_len = sizeof(cmd);

    bytes_written +=
        snprintf(buf+bytes_written, buf_len-bytes_written,
                "WITH RECURSIVE replication_tree(dbname, host, file) AS "
                "    (SELECT dbname, host, file FROM comdb2_physreps "
                "         WHERE dbname='%s' AND host IN (",
                gbl_dbname);
    if (bytes_written >= buf_len) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Buffer is not long enough!\n", __func__, __LINE__);
        return 1;
    }

    bytes_written += append_quoted_local_hosts(buf+bytes_written, buf_len-bytes_written, ",");
    if (bytes_written >= buf_len) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Buffer is not long enough!\n", __func__, __LINE__);
        return 1;
    }

    bytes_written +=
        snprintf(buf+bytes_written, buf_len-bytes_written,
                "     ) "
                "     UNION "
                "     SELECT p.dbname, p.host, p.file FROM comdb2_physreps p, "
                "         comdb2_physrep_connections c, replication_tree t "
                "         WHERE p.state = 'ACTIVE' AND p.file <> 0 AND "
                "             t.dbname = c.source_dbname AND c.dbname = p.dbname) "
                "    SELECT file FROM replication_tree WHERE file IS NOT NULL ORDER BY file LIMIT 1");
    if (bytes_written >= buf_len) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Buffer is not long enough!\n", __func__, __LINE__);
        return 1;
    }

    if (gbl_physrep_debug) {
        physrep_logmsg(LOGMSG_USER, "%s:%d Executing: %s\n", __func__, __LINE__, cmd);
    }

    cdb2_hndl_tp *repl_metadb;
    if ((rc = physrep_get_metadb_or_local_hndl(&repl_metadb)) != 0) {
        return rc;
    }

    rc = cdb2_run_statement(repl_metadb, cmd);
    if (rc == CDB2_OK) {
        while ((rc = cdb2_next_record(repl_metadb)) == CDB2_OK) {
            int64_t *minfile = (int64_t *)cdb2_column_value(repl_metadb, 0);
            physrep_min_logfile = (unsigned int) *minfile;
        }
        if (rc == CDB2_OK_DONE)
            rc = 0;
    } else {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to execute (rc: %d)\n", __func__, __LINE__, rc);
    }

    cdb2_close(repl_metadb);
    return rc;
}

/*
  Check whether we need to wait for a connection from one of the nodes
  in the source db.
*/
static int do_wait_for_reverse_conn(cdb2_hndl_tp *repl_metadb) {
    int do_wait = check_for_reverse_conn(repl_metadb);

    if (do_wait == -1) {
        if (gbl_physrep_debug)
            physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to check for reverse connection\n",
                           __func__, __LINE__);
        return -1;
    }

    return do_wait;
}

/*
  Physical replication worker thread:

                              ,-------,
                              | Start |
                              `-------'
                                  |
                                  v
                   ,-------------------------------,
        ,--- Y --- | wait for reverse_connection ? | --- N ---,
        |          `-------------------------------'          |
        v                                                     |
        * Wait for a handle from 'source db'                  |
        * Connect to replication metadb                       v
        * Update replication metadata (metadb/local)          |
        |                                                     |
    ,---'                                                     |
    |   ,-------------------------<---------------------------'
    |   |
    |   v
    |   * Connect to replication metadb or sourcedb
    v   * Execute register_replicant()
    |   * Establish a connection to one of the returned potential replicants
    |   * Update replication metadata (via update_registry())
    |   |
    `->-'
        |
        v
        * Source logs (`select .. from .. comdb2_transaction_logs ..`)
        * Apply logs
        |
    ,-------,
    |  End  |
    `-------'

    During regristration, the physical replicant could connect to 2
    different datatbase:

    1. Replication metadb (repl_metadb):
        This is the db that hosts all the comdb2_physrepXXX tables.
        If specified, we first connect to this db to register the physical
        replicant, otherwise, the replicant connects to a node in the source
        database (root) to register.

    2. Replication leader/parent db (repl_db):
       This is the database/node that to replicant connects to retrieve and
       apply physical logs.
*/
extern __thread int physrep_out_of_order;
static void *physrep_worker(void *args)
{
    comdb2_name_thread(__func__);

    volatile int64_t gen, highest_gen = 0;
    size_t sql_cmd_len = 150;
    char sql_cmd[sql_cmd_len];
    int do_truncate = 0;
    int rc;
    int now;
    int is_revconn = -1;
    int last_revconn_check = 0;
    int last_update_registry = 0;
    LOG_INFO info;
    LOG_INFO prev_info;
    DB_Connection *repl_db_cnct = NULL;

    DB_Connection rev_db_cnct = {0};

    cdb2_hndl_tp *repl_db = NULL;

    stop_physrep_worker = 0;
    physrep_worker_running = 1;

    /* Cannot call get_last_lsn if thedb is not ready */
    while (!sc_ready())
        sleep(1);

    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDONLY);

    bdb_attr_set(thedb->bdb_attr, BDB_ATTR_ENABLE_SEQNUM_GENERATIONS, 0);
    bdb_attr_set(thedb->bdb_attr, BDB_ATTR_DURABLE_LSNS, 0);
    gbl_replicant_retry_on_not_durable = 0;

repl_loop:
    while (stop_physrep_worker == 0) {
        if (thedb->master != gbl_myhostname) {
            if (repl_db_connected) {
                close_repl_connection(repl_db_cnct, repl_db, __func__, __LINE__);
            }
            if (gbl_physrep_debug)
                physrep_logmsg(LOGMSG_USER, "I am not the LEADER node, skipping async-replication\n");
            goto sleep_and_retry;
        }

        if (repl_db_connected &&
            (force_registration() || (((now = time(NULL)) - last_register) > gbl_physrep_register_interval))) {
            close_repl_connection(repl_db_cnct, repl_db, __func__, __LINE__);
            if (gbl_physrep_debug) {
                physrep_logmsg(LOGMSG_USER, "%s:%d: Forcing re-registration\n", __func__, __LINE__);
            }
        }

        int revconn_ck = gbl_physrep_revconn_check_interval;
        if (repl_db_connected && revconn_ck > 0 && comdb2_time_epoch() - last_revconn_check > revconn_ck) {
            cdb2_hndl_tp *repl_metadb = NULL;
            last_revconn_check = comdb2_time_epoch();
            if (gbl_physrep_debug) {
                physrep_logmsg(LOGMSG_USER, "%s:%d Re-checking for reverse connection\n", __func__, __LINE__);
            }
            if ((rc = physrep_get_metadb_or_local_hndl(&repl_metadb)) == 0) {
                int do_revconn = do_wait_for_reverse_conn(repl_metadb);
                cdb2_close(repl_metadb);
                if (gbl_physrep_debug) {
                    physrep_logmsg(LOGMSG_USER, "%s:%d Reverse connection check: do-revcon=%d, is-revcon=%d\n",
                                   __func__, __LINE__, do_revconn, is_revconn);
                }
                if ((do_revconn && !is_revconn) || (!do_revconn && is_revconn)) {
                    logmsg(LOGMSG_USER, "Revconn changed, do_revconn=%d, is_revconn=%d\n", do_revconn, is_revconn);
                    close_repl_connection(repl_db_cnct, repl_db, __func__, __LINE__);
                }
            }
        }

        if (repl_db_connected == 0) {
            cdb2_hndl_tp *repl_metadb = NULL;

            if ((rc = get_metadb_hndl(&repl_metadb)) != 0) {
                goto sleep_and_retry;
            }

            last_revconn_check = comdb2_time_epoch();
            if (do_wait_for_reverse_conn(repl_metadb) == 1) {
                is_revconn = 1;
                int wait_timeout_sec = 60;

                if (gbl_physrep_debug)
                    physrep_logmsg(LOGMSG_USER, "%s:%d Waiting for a connection from source node(s)\n", __func__, __LINE__);

                /*
                  Get a 'reverse' connection to one of the nodes in source db.

                  In case of cross-tier replication it could be possible that this
                  node (running in lower tier) cannot directly connect to the source
                  db (running in a higher tier), in which case, this node has to wait
                  for a connection to show up from the source db.

                  (See db/reverse_conn.c)
                */
                rev_conn_hndl = wait_for_reverse_conn(wait_timeout_sec);
                if (rev_conn_hndl == NULL || rev_conn_hndl->failed == 1) {
                    physrep_logmsg(LOGMSG_ERROR, "%s:%d Could not get a connection from source node(s) in %d secs\n",
                                   __func__, __LINE__, wait_timeout_sec);
                    cdb2_close(repl_metadb);
                    goto sleep_and_retry;
                }

                if (gbl_physrep_debug)
                    physrep_logmsg(LOGMSG_USER, "%s:%d Got a connection from %s@%s\n", __func__, __LINE__,
                                   rev_conn_hndl->remote_dbname, rev_conn_hndl->remote_host);

                repl_db = rev_conn_hndl->hndl;
                rev_db_cnct.dbname = rev_conn_hndl->remote_dbname;
                rev_db_cnct.hostname = rev_conn_hndl->remote_host;
                repl_db_cnct = &rev_db_cnct;

                rc = update_registry(repl_metadb, rev_conn_hndl->remote_dbname, rev_conn_hndl->remote_host);
                if (rc != 0) {
                    physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to exec sys.physrep.update_registry() (rc: %d)\n",
                                   __func__, __LINE__, rc);
                }

                /* Perform truncation to start fresh */
                do_truncate = 1;

                set_repl_db_connected(rev_conn_hndl->remote_dbname, rev_conn_hndl->remote_host);
            } else {
                is_revconn = 0;
                int notfound = 0;
                while (stop_physrep_worker == 0) {
                    if ((rc = register_self(repl_metadb)) == 0)
                        break;

                    int level;
                    notfound++;
                    if (gbl_physrep_debug)
                        level = gbl_physrep_debug;
                    else if (notfound >= 10)
                        level = LOGMSG_ERROR;
                    else
                        level = LOGMSG_DEBUG;

                    physrep_logmsg(level, "%s:%d Failed to register against cluster, attempt %d\n",
                                   __func__, __LINE__, notfound);

                    sleep(1);
                    while ((rc = get_metadb_hndl(&repl_metadb)) != 0) {
                        sleep(1);
                    }
                }

                repl_db_cnct = find_new_repl_db(repl_metadb, &repl_db);

                if (repl_db_cnct == NULL) {
                    cdb2_close(repl_metadb);
                    goto sleep_and_retry;
                }

                last_update_registry = comdb2_time_epoch();

                /* Perform truncation to start fresh */
                do_truncate = 1;
            }

            // Close the connection to metadb as we now have a connection to the
            // parent db.
            cdb2_close(repl_metadb);
        }

        physrep_logmsg(LOGMSG_USER, "Physical replicant is now replicating from %s@%s\n",
                       repl_db_cnct->dbname, repl_db_cnct->hostname);

        if (do_truncate && repl_db) {
            info = get_last_lsn(thedb->bdb_env);
            prev_info = handle_truncation(repl_db, info);
            if (prev_info.file == 0) {
                close_repl_connection(repl_db_cnct, repl_db, __func__, __LINE__);
                goto sleep_and_retry;
            }

            gen = prev_info.gen;
            if (gbl_physrep_debug)
                physrep_logmsg(LOGMSG_USER, "%s: gen: %" PRId64 "\n", __func__, gen);
            do_truncate = 0;
        }

        if (repl_db_connected == 0)
            goto sleep_and_retry;

        info = get_last_lsn(thedb->bdb_env);
        if (info.file <= 0) {
            goto sleep_and_retry;
        }

        prev_info = info;

        rc = snprintf(sql_cmd, sql_cmd_len,
                      "select * from comdb2_transaction_logs('{%u:%u}'%s)",
                      info.file, info.offset,
                      (gbl_blocking_physrep ? ", NULL, 1" : ""));
        if (rc < 0 || rc >= sql_cmd_len)
            physrep_logmsg(LOGMSG_ERROR, "%s:%d Command buffer is not long enough!\n", __func__, __LINE__);
        if (gbl_physrep_debug)
            physrep_logmsg(LOGMSG_USER, "%s:%d: Executing: %s\n", __func__, __LINE__, sql_cmd);
        if ((rc = cdb2_run_statement(repl_db, sql_cmd)) != CDB2_OK) {
            physrep_logmsg(LOGMSG_ERROR, "Couldn't query the database, rcode=%d '%s' retrying\n", rc,
                           cdb2_errstr(repl_db));
            close_repl_connection(repl_db_cnct, repl_db, __func__, __LINE__);
            goto sleep_and_retry;
        }

        if ((rc = cdb2_next_record(repl_db)) != CDB2_OK) {
            if (gbl_physrep_debug) {
                physrep_logmsg(LOGMSG_USER, "%s:%d: Can't find the next record (rc: %d '%s')\n", __func__, __LINE__, rc,
                               cdb2_errstr(repl_db));
            }
            close_repl_connection(repl_db_cnct, repl_db, __func__, __LINE__);
            goto sleep_and_retry;
        }

        /* our log matches, so apply each record log received */
        while (stop_physrep_worker == 0 && thedb->master == gbl_myhostname && !do_truncate &&
               (rc = cdb2_next_record(repl_db)) == CDB2_OK) {
            /* check the generation id to make sure the master hasn't
             * switched */

            int64_t *rec_gen = (int64_t *)cdb2_column_value(repl_db, 2);
            if (rec_gen && *rec_gen > highest_gen) {
                int64_t new_gen = *rec_gen;
                if (gbl_physrep_debug) {
                    physrep_logmsg(LOGMSG_USER, "%s:%d: My master changed, set truncate flag\n", __func__, __LINE__);
                    physrep_logmsg(LOGMSG_USER, "%s:%d: gen: %" PRId64 ", rec_gen: %" PRId64 "\n", __func__, __LINE__,
                                   gen, *rec_gen);
                }
                close_repl_connection(repl_db_cnct, repl_db, __func__, __LINE__);
                do_truncate = 1;
                highest_gen = new_gen;
                goto repl_loop;
            }

            prev_info = handle_record(repl_db, prev_info);
            if (physrep_out_of_order) {
                physrep_out_of_order = 0;
                do_truncate = 1;
                goto repl_loop;
            }

            gbl_physrep_last_applied_time = time(NULL);
            revconn_ck = gbl_physrep_revconn_check_interval;
            if (revconn_ck > 0 && comdb2_time_epoch() - last_revconn_check > revconn_ck) {
                if (gbl_physrep_debug) {
                    logmsg(LOGMSG_USER, "Checking reverse connection status\n");
                }
                goto repl_loop;
            }

            int update_regck = gbl_physrep_update_registry_interval;
            if (repl_db_connected && update_regck > 0 && (comdb2_time_epoch() - last_update_registry) > update_regck) {
                update_registry_periodic(repl_db_cnct->dbname, repl_db_cnct->hostname);
                last_update_registry = comdb2_time_epoch();
            }
        }

        if (gbl_physrep_debug) {
            logmsg(LOGMSG_USER, "%s:%d: next-record rc = %d, '%s'\n", __func__, __LINE__, rc, cdb2_errstr(repl_db));
        }

        if (thedb->master != gbl_myhostname || rc != CDB2_OK_DONE) {
            close_repl_connection(repl_db_cnct, repl_db, __func__, __LINE__);
            do_truncate = 1;
        }
sleep_and_retry:
        sleep(1);
    }

    if (repl_db_connected == 1) {
        close_repl_connection(repl_db_cnct, repl_db, __func__, __LINE__);
    }

    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDONLY);

    physrep_worker_running = 0;

    return NULL;
}

static int stop_physrep_worker_thread() {
    int rc = 0;

    if (physrep_worker_running == 0) {
        return 0;
    }

    stop_physrep_worker = 1;

    // The worker thread could be blocked on cdb2_next() when running in
    // 'blocking' mode. Let's try to kill it.
    pthread_cancel(physrep_worker_thread);

    if ((rc = pthread_join(physrep_worker_thread, NULL)) != 0) {
        logmsg(LOGMSG_ERROR, "physrep worker thread failed to join (rc : %d)\n", rc);
        return 1;
    }
    physrep_logmsg(LOGMSG_USER, "physrep worker thread has stopped\n");

    physrep_worker_running = 0;
    stop_physrep_worker = 0;
    return 0;
}

static int check_and_log_slow_replicants() {
    bdb_state_type *bdb_state = gbl_bdb_state;
    // Perform this check only on the master
    if (bdb_state->repinfo->master_host != bdb_state->repinfo->myhost)
        return 0;

    unsigned int slow_physreps = 0;
    int rc = slow_replicants_count(&slow_physreps);
    if (rc != 0) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to retrieve slow replicant count (rc: %d)\n",
                       __func__, __LINE__, rc);
    } else if (slow_physreps > 0) {
        physrep_logmsg(LOGMSG_WARN, "%d replicants are either stuck or unresponsive\n", slow_physreps);
    }

    return 0;
}

/*
   This function checks whether this replicant is potentially hung. It
   does so by comparing it's lsn with that of source node(s).
*/
static void am_i_hung(time_t cur_time) {
    const size_t nodes_list_sz = REPMAX * (255+1) + 3;
    char query[120+nodes_list_sz];
    int bytes_written = 0;
    int rc;

    // Return if gbl_physrep_last_applied_time has never been set, otherwise it will cause
    // inaccurate time difference to be logged in the message below.
    if (gbl_physrep_last_applied_time == 0) {
        gbl_physrep_last_applied_time = cur_time;
        return;
    }

    // No need to compare the LSNs if last applied time is within the threshold.
    if ((cur_time - gbl_physrep_last_applied_time) <= gbl_physrep_hung_replicant_threshold) {
        return;
    }

    char *buf = query;
    size_t buf_len = sizeof(query);

    bytes_written = snprintf(buf+bytes_written, buf_len-bytes_written,
                             "select file, offset from comdb2_physreps where "
                             "dbname='%s' and host in (",
                             gbl_physrep_source_dbname);
    if (bytes_written >= buf_len) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Buffer is not long enough!\n", __func__, __LINE__);
        return;
    }

    bytes_written += append_quoted_source_hosts(buf+bytes_written, buf_len-bytes_written, &rc);
    if (rc != 0 || bytes_written >= buf_len) {
        if (rc != 0) {
            physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to append source host(s)!\n", __func__, __LINE__);
            return;
        }
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Buffer is not long enough!\n", __func__, __LINE__);
        return;
    }

    bytes_written += snprintf(buf+bytes_written, buf_len-bytes_written, ") limit 1");
    if (bytes_written >= buf_len) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Buffer is not long enough!\n", __func__, __LINE__);
        return;
    }

    cdb2_hndl_tp *repl_metadb;
    if ((rc = get_metadb_hndl(&repl_metadb)) != 0) {
        return;
    }

    rc = cdb2_run_statement(repl_metadb, query);
    if (rc == CDB2_OK) {
        while ((rc = cdb2_next_record(repl_metadb)) == CDB2_OK) {
            int64_t *file = (int64_t *)cdb2_column_value(repl_metadb, 0);
            int64_t *offset = (int64_t *)cdb2_column_value(repl_metadb, 1);

            if (!file || !offset) {
                continue;
            }

            LOG_INFO info = get_last_lsn(thedb->bdb_env);

            if (info.file < *file || info.offset < *offset) {
                physrep_logmsg(LOGMSG_WARN,
                               "Physical replicant has been inactive for last %ld seconds "
                               "(my lsn: (%d:%d) source lsn: (%d:%d))\n",
                               cur_time - gbl_physrep_last_applied_time,
                               info.file, info.offset, (int)*file, (int)*offset);
            }
        }
    } else {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to execute (rc: %d)\n", __func__, __LINE__, rc);
    }

    cdb2_close(repl_metadb);
}

static void *physrep_watcher(void *args) {
    static int physrep_source_nodes_last_refreshed;
    static int physrep_slow_replicant_last_checked;
    static int physrep_keepalive_last_sent;
    static int physrep_hung_replicant_last_checked;
    static int physrep_minlog_last_checked;

    while (!gbl_exit && stop_physrep_watcher == 0) {
        sleep(1);

        time_t now = time(NULL);

        if (gbl_physrep_source_dbname == NULL) {
            // Physical replicantion source nodes:
            //   1) Periodically refresh the member information in the source cluster
            //   2) Dectect and log about slow replicants

            // Refresh 'source nodes' list in the replication metadb
            if ((gbl_physrep_metadb_name != NULL || get_dbtable_by_name("comdb2_physreps") != NULL) &&
                thedb->master == gbl_myhostname) {
                if ((now - physrep_source_nodes_last_refreshed) >= gbl_physrep_source_nodes_refresh_freq_sec) {
                    // Add/update information about nodes in the current cluster in comdb2_physreps table.
                    // Note that the table could either be local or in 'replication meta db'.
                    int rc = send_reset_nodes("Active");
                    if (rc != 0) {
                        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to reset info in replication metadb tables (rc: %d)\n",
                                       __func__, __LINE__, rc);
                    }
                    physrep_source_nodes_last_refreshed = now;
                }
            }

            // Log about slow physical replicants.
            if ((now - physrep_slow_replicant_last_checked) >= gbl_physrep_slow_replicant_check_freq_sec) {
                check_and_log_slow_replicants();
                physrep_slow_replicant_last_checked = now;
            }

        } else {
            // Physical replicants:
            //   1) Check and log hung replicants

            // Log whether this replicant has not been replicating for a while.
            // In a replicated cluster the following check is only logical for
            // the master node.
            if (thedb->master == gbl_myhostname &&
                ((now - physrep_hung_replicant_last_checked) >= gbl_physrep_hung_replicant_check_freq_sec)) {
                am_i_hung(now);
                physrep_hung_replicant_last_checked = now;
            }
        }
        // Common:
        //   1) Send keepalives
        //   2) Update maximum log file number upto which it is safe to delete

        // Periodically send keepalive to report its LSN.
        if ((now - physrep_keepalive_last_sent) >= gbl_physrep_keepalive_freq_sec) {
            send_keepalive();
            physrep_keepalive_last_sent = now;
        }

        // Update the 'minimum log file' marker upto which it is safe to
        // delete log files.
        if ((now - physrep_minlog_last_checked) >= gbl_physrep_check_minlog_freq_sec) {
            update_min_logfile();
            physrep_minlog_last_checked = now;
        }
    }
    return NULL;
}

static int stop_physrep_watcher_thread() {
    int rc = 0;

    if (physrep_watcher_running == 0) {
        return 0;
    }

    stop_physrep_watcher = 1;

    if ((rc = pthread_join(physrep_watcher_thread, NULL)) != 0) {
        logmsg(LOGMSG_ERROR, "Watcher thread failed to join (rc : %d)\n", rc);
        return 1;
    }
    physrep_logmsg(LOGMSG_USER, "Watcher thread has stopped\n");

    physrep_watcher_running = 0;
    stop_physrep_watcher = 0;
    return 0;
}

static int is_a_physrep_source_or_dest() {
    if (gbl_physrep_i_am_metadb == 1) {                   // Is not a physical replication metadb
        return 0;
    }

    if (gbl_physrep_source_dbname == NULL &&              // Is not a plysical replicant, AND
        gbl_physrep_metadb_name == NULL &&                // Does not connect to a replication metadb, AND
        get_dbtable_by_name("comdb2_physreps") == NULL) { // There's no local 'comdb2_physreps' table, AND
        return 0;
    }
    return 1;
}

/*
  The physical-log based replication is enabled via 3 types of threads.
  1. physrep worker:
         This threads runs on every physical replicant and is responsible
         registering this node with the replication metadb and retrieving
         and applying the physical logs.
  2. physrep watcher:
         This thread performs bookkeeping tasks and runs on both physical
         replicants as well as the physical replication root (source) nodes.
  3. Reverse connections manager:
         This thread runs exclusively on replication source/root nodes to
         enable cross-tier replication. (See db/reverse_conn.c)
*/
int start_physrep_threads() {
    int rc;

    if (!is_a_physrep_source_or_dest()) {
        return 0;
    }

    // If this is a 'physical replication' source, we would need to actively
    // try and connect to the replicants in the lower tier. (See db/reverse_conn.c)
    // This task is done by 'Reverse connections' manager thread.
    if ((rc = start_reverse_connections_manager()) != 0) {
        physrep_logmsg(LOGMSG_ERROR, "Couldn't start 'reverse connections' manager (rc: %d)\n" ,rc);
        return -1;
    }
    physrep_logmsg(LOGMSG_USER, "'reverse connections' manager thread has started!\n");

    // Start physical replication worker
    if (gbl_physrep_source_dbname != NULL) {
        if (physrep_worker_running == 1) {
            physrep_logmsg(LOGMSG_ERROR, "Worker thread is already running!\n");
        } else if ((rc = pthread_create(&physrep_worker_thread, NULL, physrep_worker, NULL)) != 0) {
            physrep_logmsg(LOGMSG_ERROR, "Couldn't create physical replication worker thread (rc: %d)\n", rc);
            return -1;
        }
        physrep_logmsg(LOGMSG_USER, "Worker thread has started!\n");
    } else {
        physrep_logmsg(LOGMSG_USER, "Not starting worker thread\n");
    }

    // Start physical replication watcher
    if (gbl_physrep_source_dbname != NULL || gbl_physrep_metadb_name != NULL
        || get_dbtable_by_name("comdb2_physreps") != NULL) {
        if (physrep_watcher_running == 1) {
            physrep_logmsg(LOGMSG_ERROR, "Watcher thread is already running!\n");
        } else {
            if ((rc = pthread_create(&physrep_watcher_thread, NULL, physrep_watcher, NULL)) != 0) {
                physrep_logmsg(LOGMSG_ERROR, "Couldn't create physical replication monitor thread (rc: %d)\n", rc);
                return -1;
            }
            physrep_watcher_running = 1;
            physrep_logmsg(LOGMSG_USER, "Watcher thread has started!\n");
        }
    } else {
        physrep_logmsg(LOGMSG_USER, "Not starting watcher thread\n");
    }

    gbl_started_physrep_threads = 1;
    return 0;
}

int stop_physrep_threads() {
    if (!is_a_physrep_source_or_dest() || !gbl_started_physrep_threads) {
        if (gbl_physrep_debug && !gbl_started_physrep_threads)
            physrep_logmsg(LOGMSG_USER, "%s:%d: This node is neither a physical replication "
                                        "source nor a replicant, nothing to stop here\n",
                                        __func__, __LINE__);
        return 0;
    }

    if (gbl_physrep_debug)
        physrep_logmsg(LOGMSG_USER, "Stopping all physrep threads\n");

    stop_physrep_worker_thread();
    stop_physrep_watcher_thread();
    stop_reverse_connections_manager();
    return 0;
}

void physrep_cleanup() {
    if (!is_a_physrep_source_or_dest()) {
        return;
    }

    int rc = send_reset_nodes("Inactive");
    if (rc != 0) {
        physrep_logmsg(LOGMSG_ERROR, "%s:%d Failed to reset info in replication metadb tables (rc: %d)\n",
                       __func__, __LINE__, rc);
    }
    for (int i = 0; i < physrep_metadb_host_count; ++i) {
        free(physrep_metadb_hosts[i]);
    }
    free(physrep_metadb_hosts);
}

int physrep_exited() {
    return (physrep_worker_running == 1) ? 0 : 1;
}
