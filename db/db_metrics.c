/*
   Copyright 2017 Bloomberg Finance L.P.

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

#include <assert.h>
#include <stdint.h>
#include <unistd.h>
#include <sql.h>
#include <bdb_int.h>
#include <comdb2.h>
#include <comdb2_atomic.h>
#include <metrics.h>
#include <bdb_api.h>
#include <net.h>
#include <thread_stats.h>
#include <net_appsock.h>

#include <sys/time.h>
#include <sys/resource.h>
#include "comdb2_query_preparer.h"

struct comdb2_metrics_store {
    int64_t cache_hits;
    int64_t cache_misses;
    double  cache_hit_rate;
    int64_t commits;
    int64_t connections;
    int64_t connection_timeouts;
    double  connection_to_sql_ratio;
    double  cpu_percent;
    int64_t deadlocks;
    int64_t locks_aborted;
    int64_t fstraps;
    int64_t lockrequests;
    int64_t lockwaits;
    int64_t lock_wait_time_us;
    int64_t memory_ulimit;
    int64_t memory_usage;
    int64_t preads;
    int64_t pwrites;
    int64_t retries;
    int64_t sql_cost;
    int64_t sql_count;
    int64_t sql_ssl_count;
    int64_t start_time;
    int64_t threads;
    int64_t current_connections;
    int64_t diskspace;
    double service_time;
    double queue_depth;
    double concurrent_sql;
    double concurrent_connections;
    int64_t ismaster;
    uint64_t num_sc_done;
    int64_t last_checkpoint_ms;
    int64_t total_checkpoint_ms;
    int64_t checkpoint_count;
    int64_t rcache_hits;
    int64_t rcache_misses;
    int64_t last_election_ms;
    int64_t total_election_ms;
    int64_t election_count;
    int64_t last_election_time;
    int64_t udp_sent;
    int64_t udp_failed_send;
    int64_t udp_received;
    int64_t active_transactions;
    int64_t maxactive_transactions;
    int64_t total_commits;
    int64_t total_aborts;
    double sql_queue_time;
    int64_t sql_queue_timeouts;
    double handle_buf_queue_time;
    int64_t denied_appsock_connections;
    int64_t locks;
    int64_t temptable_created;
    int64_t temptable_create_reqs;
    int64_t temptable_spills;
    int64_t net_drops;
    int64_t net_queue_size;
    int64_t rep_deadlocks;
    int64_t rw_evicts;
    int64_t standing_queue_time;
    int64_t minimum_truncation_file;
    int64_t minimum_truncation_offset;
    int64_t minimum_truncation_timestamp;
    int64_t reprepares;
    int64_t nonsql;
    int64_t vreplays;
    int64_t nsslfullhandshakes;
    int64_t nsslpartialhandshakes;
    double weighted_queue_depth;
    int64_t weighted_standing_queue_time;
    int64_t auth_allowed;
    int64_t auth_denied;

    /* Legacy request metrics */
    int64_t fastsql_execute_inline_params;
    int64_t fastsql_set_isolation_level;
    int64_t fastsql_set_timeout;
    int64_t fastsql_set_info;
    int64_t fastsql_execute_inline_params_tz;
    int64_t fastsql_set_heartbeat;
    int64_t fastsql_pragma;
    int64_t fastsql_reset;
    int64_t fastsql_execute_replaceable_params;
    int64_t fastsql_set_sql_debug;
    int64_t fastsql_grab_dbglog;
    int64_t fastsql_set_user;
    int64_t fastsql_set_password;
    int64_t fastsql_set_endian;
    int64_t fastsql_execute_replaceable_params_tz;
    int64_t fastsql_get_effects;
    int64_t fastsql_set_planner_effort;
    int64_t fastsql_set_remote_access;
    int64_t fastsql_osql_max_trans;
    int64_t fastsql_set_datetime_precision;
    int64_t fastsql_sslconn;
    int64_t fastsql_execute_stop;
};

static struct comdb2_metrics_store stats;

/*
  List of (almost) all comdb2 stats.
  Please keep'em sorted.
*/
comdb2_metric gbl_metrics[] = {
    {"cache_hits", "Buffer pool hits", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.cache_hits, NULL},
    {"cache_misses", "Buffer pool misses",
     (int64_t)STATISTIC_COLLECTION_TYPE_CUMULATIVE, (int64_t)STATISTIC_INTEGER,
     &stats.cache_misses, NULL},
    {"cache_hit_rate", "Buffer pool request hit rate", STATISTIC_DOUBLE,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.cache_hit_rate, NULL},
    {"commits", "Number of commits", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.commits, NULL},
    {"concurrent_sql", "Concurrent SQL queries", STATISTIC_DOUBLE,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.concurrent_sql, NULL},
    {"concurrent_connections", "Number of concurrent connections ",
     STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.concurrent_connections, NULL},
    {"connections", "Total connections", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.connections, NULL},
    {"connection_timeouts", "Timed out connection attempts", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.connection_timeouts, NULL},
    {"connection_to_sql_ratio",
     "Ratio of total number of connections to sql "
     "(and nonsql) request counts",
     STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.connection_to_sql_ratio, NULL},
    {"cpu_percent", "Database CPU time over last 5 seconds", STATISTIC_DOUBLE,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.cpu_percent, NULL},
    {"current_connections", "Number of current connections", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.current_connections, NULL},
    {"deadlocks", "Number of deadlocks", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.deadlocks, NULL},
    {"locks_aborted", "Number of locks aborted", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.locks_aborted, NULL},
    {"diskspace", "Disk space used (bytes)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.diskspace, NULL},
    {"fstraps", "Number of socket requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fstraps, NULL},
    {"ismaster", "Is this machine the current master", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.ismaster, NULL},
    {"lockrequests", "Total lock requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.lockrequests, NULL},
    {"lockwaits", "Number of lock waits", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.lockwaits, NULL},
    {"lockwait_time", "Time spent in lock waits (us)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.lock_wait_time_us, NULL},
    {"memory_ulimit", "Virtual address space ulimit", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.memory_ulimit, NULL},
    {"memory_usage", "Address space size", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.memory_usage, NULL},
    {"preads", "Number of pread()'s", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.preads, NULL},
    {"pwrites", "Number of pwrite()'s", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.pwrites, NULL},
    {"queue_depth", "Request queue depth", STATISTIC_DOUBLE,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.queue_depth, NULL},
    {"retries", "Number of retries", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.retries, NULL},
    {"service_time", "Service time", STATISTIC_DOUBLE,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.service_time, NULL},
    {"sql_cost", "Number of sql steps executed (cost)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sql_cost, NULL},
    {"sql_count", "Number of sql queries executed", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sql_count, NULL},
    {"sql_ssl_count", "Number of sql queries executed, via SSL", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sql_ssl_count, NULL},
    {"start_time", "Server start time", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.start_time, NULL},
    {"threads", "Number of threads", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.threads, NULL},
    {"num_sc_done", "Number of schema changes done", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.num_sc_done, NULL},
    {"checkpoint_ms", "Time taken for last checkpoint", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.last_checkpoint_ms},
    {"checkpoint_total_ms", "Total time taken for checkpoints",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.total_checkpoint_ms},
    {"checkpoint_count", "Total number of checkpoints taken", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.checkpoint_count},
    {"rcache_hits", "Count of root-page cache hits", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.rcache_hits},
    {"rcache_misses", "Count of root-page cache misses", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.rcache_misses},
    {"last_election_ms", "Time taken to resolve last election",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.last_election_ms, NULL},
    {"total_election_ms", "Total time taken to resolve elections",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.total_election_ms, NULL},
    {"election_count", "Total number of elections", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.election_count, NULL},
    {"last_election_time", "Wallclock time last election completed",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.last_election_time, NULL},
    {"udp_sent", "Number of udp packets sent", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.udp_sent, NULL},
    {"udp_failed_send", "Number of failed udp sends", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.udp_failed_send, NULL},
    {"udp_received", "Number of udp receives", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.udp_received, NULL},
    {"active_transactions", "Number of active transactions", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.active_transactions, NULL},
    {"max_active_transactions", "Maximum number of active transactions",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.maxactive_transactions, NULL},
    {"total_commits", "Number of transaction commits", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.total_commits, NULL},
    {"total_aborts", "Number of transaction aborts", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.total_aborts, NULL},
    {"sql_queue_time", "Average ms spent waiting in sql queue",
     STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST, &stats.sql_queue_time,
     NULL},
    {"sql_queue_timeouts", "Number of sql items timed-out waiting on queue",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.sql_queue_timeouts, NULL},
    {"handle_buf_queue_time", "Average ms spent waiting in handle-buf queue",
     STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.handle_buf_queue_time, NULL},
    {"denied_appsocks", "Number of denied appsock connections",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.denied_appsock_connections, NULL},
    {"locks", "Number of currently held locks", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.locks, NULL},
    {"temptable_created", "Number of temporary tables created",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.temptable_created, NULL},
    {"temptable_create_requests", "Number of create temporary table requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.temptable_create_reqs, NULL},
    {"temptable_spills",
     "Number of temporary tables that had to be spilled to disk-backed tables",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.temptable_spills, NULL},
    {"net_drops",
     "Number of packets that didn't fit on network queue and were dropped",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.net_drops,
     NULL},
    {"net_queue_size", "Size of largest outgoing net queue", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.net_queue_size, NULL},
    {"rep_deadlocks", "Replication deadlocks", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.rep_deadlocks, NULL},
    {"rw_evictions", "Dirty page evictions", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.rw_evicts, NULL},
    {"standing_queue_time", "How long the database has had a standing queue",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.standing_queue_time, NULL},
    {"nonsql", "Number of non-sql requests (eg: tagged)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.nonsql, NULL},
#if 0
    {"minimum_truncation_file", "Minimum truncation file", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.minimum_truncation_file, NULL},
    {"minimum_truncation_offset", "Minimum truncation offset",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.minimum_truncation_offset, NULL},
    {"minimum_truncation_timestamp", "Minimum truncation timestamp",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.minimum_truncation_timestamp, NULL},
#endif
    {"reprepares", "Number of times statements are reprepared by sqlitex",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.reprepares,
     NULL},
    {"verify_replays", "Number of replays on verify errors", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.vreplays, NULL},
    {"nsslfullhandshakes", "Number of SSL full handshakes", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.nsslfullhandshakes, NULL},
    {"nsslpartialhandshakes", "Number of SSL partial handshakes",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.nsslpartialhandshakes, NULL},
    {"weighted_queue_depth", "Weighted queue depth", STATISTIC_DOUBLE,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.weighted_queue_depth, NULL},
    {"weighted_standing_queue_time",
     "How long the database has had a weighted standing queue",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.weighted_standing_queue_time, NULL},
    {"auth_allowed", "Number of successful authentication requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST, &stats.auth_allowed,
     NULL},
    {"auth_denied", "Number of failed authentication requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST, &stats.auth_denied,
     NULL},

    {"fastsql_execute_inline_params", "Number of fastsql 'execute' requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_execute_inline_params, NULL},
    {"fastsql_set_isolation_level",
     "Number of fastsql 'set isolation level' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_set_isolation_level,
     NULL},
    {"fastsql_set_timeout", "Number of fastsql 'set timeout' requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_set_timeout, NULL},
    {"fastsql_set_info", "Number of fastsql 'set info' requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_set_info, NULL},
    {"fastsql_execute_inline_params_tz",
     "Number of fastsql 'execute with timezone' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_execute_inline_params_tz, NULL},
    {"fastsql_set_heartbeat", "Number of fastsql 'set heartbeat' requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_set_heartbeat, NULL},
    {"fastsql_pragma", "Number of fastsql pragma requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_pragma, NULL},
    {"fastsql_reset", "Number of fastsql reset requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_reset, NULL},
    {"fastsql_execute_replaceable_params",
     "Number of fastsql 'execute with replacable parameters' requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_execute_replaceable_params, NULL},
    {"fastsql_set_sql_debug", "Number of fastsql 'set sql debug' requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_set_sql_debug, NULL},
    {"fastsql_grab_dbglog", "Number of fastsql 'grab dbglog' requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_grab_dbglog, NULL},
    {"fastsql_set_user", "Number of fastsql 'set user' requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_set_user, NULL},
    {"fastsql_set_password", "Number of fastsql 'set password' requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_set_password, NULL},
    {"fastsql_set_endian", "Number of fastsql 'set endian' requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_set_endian, NULL},
    {"fastsql_execute_replaceable_params_tz",
     "Number of fastsql 'execute with replacable parameters & timezone' "
     "requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_execute_replaceable_params_tz, NULL},
    {"fastsql_get_effects", "Number of fastsql 'get effects' requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_get_effects, NULL},
    {"fastsql_set_planner_effort",
     "Number of fastsql 'set planner effort' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_set_planner_effort,
     NULL},
    {"fastsql_set_remote_access",
     "Number of fastsql 'set remote access' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_set_remote_access,
     NULL},
    {"fastsql_osql_max_trans", "Number of fastsql 'osql max trans' requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_osql_max_trans, NULL},
    {"fastsql_set_datetime_precision",
     "Number of fastsql 'set datetime precision' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_set_datetime_precision, NULL},
    {"fastsql_sslconn", "Number of fastsql 'sslconn' requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_sslconn, NULL},
    {"fastsql_execute_stop", "Number of fastsql 'execute stop' requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_execute_stop, NULL},
};

const char *metric_collection_type_string(comdb2_collection_type t) {
    switch (t) {
        case STATISTIC_COLLECTION_TYPE_CUMULATIVE:
            return "cumulative";
        case STATISTIC_COLLECTION_TYPE_LATEST:
            return "latest";
    }
    return "???";
}

int gbl_metrics_count = sizeof(gbl_metrics) / sizeof(comdb2_metric);

extern int n_commits;
extern long n_fstrap;

/* Legacy request metrics */
int64_t gbl_fastsql_execute_inline_params;
int64_t gbl_fastsql_set_isolation_level;
int64_t gbl_fastsql_set_timeout;
int64_t gbl_fastsql_set_info;
int64_t gbl_fastsql_execute_inline_params_tz;
int64_t gbl_fastsql_set_heartbeat;
int64_t gbl_fastsql_pragma;
int64_t gbl_fastsql_reset;
int64_t gbl_fastsql_execute_replaceable_params;
int64_t gbl_fastsql_set_sql_debug;
int64_t gbl_fastsql_grab_dbglog;
int64_t gbl_fastsql_set_user;
int64_t gbl_fastsql_set_password;
int64_t gbl_fastsql_set_endian;
int64_t gbl_fastsql_execute_replaceable_params_tz;
int64_t gbl_fastsql_get_effects;
int64_t gbl_fastsql_set_planner_effort;
int64_t gbl_fastsql_set_remote_access;
int64_t gbl_fastsql_osql_max_trans;
int64_t gbl_fastsql_set_datetime_precision;
int64_t gbl_fastsql_sslconn;
int64_t gbl_fastsql_execute_stop;

static void update_fastsql_metrics() {
    stats.fastsql_execute_inline_params = gbl_fastsql_execute_inline_params;
    stats.fastsql_set_isolation_level = gbl_fastsql_set_isolation_level;
    stats.fastsql_set_timeout = gbl_fastsql_set_timeout;
    stats.fastsql_set_info = gbl_fastsql_set_info;
    stats.fastsql_execute_inline_params_tz = gbl_fastsql_execute_inline_params_tz;
    stats.fastsql_set_heartbeat = gbl_fastsql_set_heartbeat;
    stats.fastsql_pragma = gbl_fastsql_pragma;
    stats.fastsql_reset = gbl_fastsql_reset;
    stats.fastsql_execute_replaceable_params = gbl_fastsql_execute_replaceable_params;
    stats.fastsql_set_sql_debug = gbl_fastsql_set_sql_debug;
    stats.fastsql_grab_dbglog = gbl_fastsql_grab_dbglog;
    stats.fastsql_set_user = gbl_fastsql_set_user;
    stats.fastsql_set_password = gbl_fastsql_set_password;
    stats.fastsql_set_endian = gbl_fastsql_set_endian;
    stats.fastsql_execute_replaceable_params_tz = gbl_fastsql_execute_replaceable_params_tz;
    stats.fastsql_get_effects = gbl_fastsql_get_effects;
    stats.fastsql_set_planner_effort = gbl_fastsql_set_planner_effort;
    stats.fastsql_set_remote_access = gbl_fastsql_set_remote_access;
    stats.fastsql_osql_max_trans = gbl_fastsql_osql_max_trans;
    stats.fastsql_set_datetime_precision = gbl_fastsql_set_datetime_precision;
    stats.fastsql_sslconn = gbl_fastsql_sslconn;
    stats.fastsql_execute_stop = gbl_fastsql_execute_stop;
}

static int64_t refresh_diskspace(struct dbenv *dbenv, tran_type *tran)
{
    int64_t total = 0;
    int ndb;
    struct dbtable *db;
    unsigned int num_logs;

    static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
    Pthread_mutex_lock(&lk);

    for(ndb = 0; ndb < dbenv->num_dbs; ndb++)
    {
        db = dbenv->dbs[ndb];
        total += calc_table_size_tran(tran, db, 0);
    }
    for(ndb = 0; ndb < dbenv->num_qdbs; ndb++)
    {
        db = dbenv->qdbs[ndb];
        total += calc_table_size_tran(tran, db, 0);
    }
    total += bdb_logs_size(dbenv->bdb_env, &num_logs);

    Pthread_mutex_unlock(&lk);
    return total;
}

/* see the definition in db_tunables.h */
int gbl_track_weighted_queue_metrics_separately = 0;
/* Weighted queue depth */
static double weighted_queue_depth = 0;
/* When did weighted queue start */
static time_t weighted_queue_start_time = 0;
/* Return weighted standing queue time */
static time_t metrics_weighted_standing_queue_time(void)
{
    return (weighted_queue_start_time == 0) ? 0 : (time(NULL) - weighted_queue_start_time);
}
/* Update weighted queue depth over the last N samples:
   Weighted_queue_depth = (N-1) / N * Weighted_queue_depth + 1/N * Current_queue_depth

   Also update when a weighted standing queue first started, accordingly */
static void update_weighted_standing_queue_metrics(void)
{
    extern int gbl_metric_maxage;
    extern int gbl_update_metrics_interval;
    int curr_depth = thd_queue_depth() /* tag */ + thdpool_get_queue_depth(get_default_sql_pool(0)) /* sql */;
    double weight = gbl_metric_maxage / gbl_update_metrics_interval;

    if (weight < 0)
        weight = 1;

    /* Fix an edge case: if queue depth is constantly 1, the weighted average only approximates 1.
       In this case we set the weighted average to 1 so it'll be reported too. */
    if (weighted_queue_depth < 1 && curr_depth == 1)
        weighted_queue_depth = 1;
    else
        weighted_queue_depth = (weight - 1) / weight * weighted_queue_depth + curr_depth / weight;

    if (weighted_queue_depth < 1)
        weighted_queue_start_time = 0;
    else if (weighted_queue_start_time == 0)
        weighted_queue_start_time = time(NULL);
}

/* TODO: this isn't threadsafe. */

void refresh_queue_size(struct dbenv *dbenv) 
{
    const char *hostlist[REPMAX];
    int num_nodes;
    int max_queue_size = 0;
    struct net_host_stats stat;

    /* do this for both nets */
    num_nodes = net_get_all_nodes_connected(dbenv->handle_sibling, hostlist);
    for (int i = 0; i < num_nodes; i++) {
        if (net_get_host_stats(dbenv->handle_sibling, hostlist[i], &stat) == 0) {
            if (stat.queue_size > max_queue_size)
                max_queue_size = stat.queue_size;
        }
    }
    /* do this for both nets */
    for (int i = 0; i < num_nodes; i++) {
        if (net_get_host_stats(dbenv->handle_sibling_offload, hostlist[i], &stat) == 0) {
            if (stat.queue_size > max_queue_size)
                max_queue_size = stat.queue_size;
        }
    }
    stats.net_queue_size = max_queue_size;
}

static time_t metrics_standing_queue_time(void);

int refresh_metrics(void)
{
    int rc;
    const struct berkdb_thread_stats *pstats;
    int bdberr;
#if 0
    int min_file, min_offset;
    int32_t min_timestamp;
#endif

    if (db_is_exiting())
        return 1;

    stats.commits = n_commits;
    stats.fstraps = n_fstrap;
    stats.nonsql = n_fstrap + n_qtrap - n_dbinfo;
    stats.retries = n_retries;
    stats.sql_cost = gbl_nsql_steps + gbl_nnewsql_steps;
    stats.sql_count = gbl_nsql + gbl_nnewsql;
    stats.sql_ssl_count = gbl_nnewsql_ssl;
    stats.current_connections = net_get_num_current_non_appsock_accepts(thedb->handle_sibling) + active_appsock_conns;

    rc = bdb_get_lock_counters(thedb->bdb_env, &stats.deadlocks,
                               &stats.locks_aborted, &stats.lockwaits,
                               &stats.lockrequests);
    if (rc) {
        logmsg(LOGMSG_ERROR, "failed to refresh statistics (%s:%d)\n", __FILE__,
               __LINE__);
        return 1;
    }

    rc = bdb_get_bpool_counters(thedb->bdb_env, &stats.cache_hits,
                                &stats.cache_misses, &stats.rw_evicts);
    if (rc) {
        logmsg(LOGMSG_ERROR, "failed to refresh statistics (%s:%d)\n", __FILE__,
               __LINE__);
        return 1;
    }

    pstats = bdb_get_process_stats();
    stats.preads = pstats->n_preads;
    stats.pwrites = pstats->n_pwrites;
    stats.lock_wait_time_us = pstats->lock_wait_time_us;

    /* connections stats */
    stats.connections = net_get_num_accepts(thedb->handle_sibling);
    stats.connection_timeouts = net_get_num_accept_timeouts(thedb->handle_sibling);

    int64_t total_reqs = stats.sql_count + stats.nonsql;
    stats.connection_to_sql_ratio = (total_reqs > 0) ? (stats.connections/(double)total_reqs) : 0;

    /* cache hit rate */
    uint64_t hits, misses;
    bdb_get_cache_stats(thedb->bdb_env, &hits, &misses, NULL, NULL, NULL, NULL);
    stats.cache_hit_rate = 100 * ((double) hits / ((double) hits + (double) misses));

    stats.memory_ulimit = 0;
    stats.memory_usage = 0;
    stats.threads = 0;
#ifdef _LINUX_SOURCE
    /* memory */
    struct rlimit rl;
    rc = getrlimit(RLIMIT_AS, &rl);
    if (rc == 0) {
        if (rl.rlim_cur == RLIM_INFINITY)
            stats.memory_ulimit = 0;
        else
            stats.memory_ulimit = rl.rlim_cur / (1024*1024);
    }
    else {
        stats.memory_ulimit = 0;
    }
    FILE *f = fopen("/proc/self/stat", "r");
    if (f) {
        char line[1024];
        char *tmp = fgets(line, sizeof(line), f);
        if (!tmp) {
            logmsg(LOGMSG_ERROR, "failed to read from /proc/self/stat\n");
        }
        fclose(f);
        long num_threads;
        unsigned long vmsize;
        rc = sscanf(line,
                    "%*d %*s %*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %*u "
                    "%*u %*d %*d %*d %*d %ld %*d %*u %lu",
                    &num_threads, &vmsize);
        if (rc == 2) {
            stats.threads = num_threads;
            stats.memory_usage = vmsize / (1024*1024);
        }
    }
    stats.cpu_percent = gbl_cpupercent;
#endif
    stats.service_time = time_metric_average(thedb->service_time);
    stats.weighted_queue_depth = weighted_queue_depth;
    if (gbl_track_weighted_queue_metrics_separately)
        stats.queue_depth = time_metric_average(thedb->queue_depth);
    else
        stats.queue_depth = stats.weighted_queue_depth;
    stats.concurrent_sql = time_metric_average(thedb->concurrent_queries);
    stats.sql_queue_time = time_metric_average(thedb->sql_queue_time);
    stats.sql_queue_timeouts = get_all_sql_pool_timeouts();
    stats.handle_buf_queue_time =
        time_metric_average(thedb->handle_buf_queue_time);
    stats.concurrent_connections = time_metric_average(thedb->connections);
    int master =
        bdb_whoismaster((bdb_state_type *)thedb->bdb_env) == gbl_myhostname ? 1
                                                                            : 0;
    stats.ismaster = master;
    stats.last_checkpoint_ms = gbl_last_checkpoint_ms;
    stats.total_checkpoint_ms = gbl_total_checkpoint_ms;
    stats.checkpoint_count = gbl_checkpoint_count;
    stats.rcache_hits = rcache_hits;
    stats.rcache_misses = rcache_miss;
    stats.last_election_ms = gbl_last_election_time_ms;
    stats.total_election_ms = gbl_total_election_time_ms;
    stats.election_count = gbl_election_count;
    stats.last_election_time = gbl_election_time_completed;
    unsigned int sent, failed_send, received;
    udp_stats(&sent, &failed_send, &received);
    stats.udp_sent = sent;
    stats.udp_failed_send = failed_send;
    stats.udp_received = received;
    bdb_get_txn_stats(thedb->bdb_env, &stats.active_transactions,
                      &stats.maxactive_transactions, &stats.total_commits,
                      &stats.total_aborts);
    stats.denied_appsock_connections = gbl_denied_appsock_connection_count;
    if (bdb_lock_stats(thedb->bdb_env, &stats.locks))
        stats.locks = 0;
    stats.temptable_created = gbl_temptable_created;
    stats.temptable_create_reqs = gbl_temptable_create_reqs;
    stats.temptable_spills = gbl_temptable_spills;

    struct net_stats net_stats;
    rc = net_get_stats(thedb->handle_sibling, &net_stats);
    if (rc == 0) {
        stats.net_drops = net_stats.num_drops;
    }
    else {
        stats.net_drops = 0;
    }

    refresh_queue_size(thedb);

    bdb_rep_deadlocks(thedb->bdb_env, &stats.rep_deadlocks);

    stats.weighted_standing_queue_time = metrics_weighted_standing_queue_time();
    if (gbl_track_weighted_queue_metrics_separately)
        stats.standing_queue_time = metrics_standing_queue_time();
    else
        stats.standing_queue_time = stats.weighted_standing_queue_time;

#if 0
    bdb_min_truncate(thedb->bdb_env, &min_file, &min_offset, &min_timestamp);
    stats.minimum_truncation_file = min_file;
    stats.minimum_truncation_offset = min_offset;
    stats.minimum_truncation_timestamp = min_timestamp;
#endif

    if (gbl_old_column_names && query_preparer_plugin &&
        query_preparer_plugin->sqlitex_get_metrics) {
        query_preparer_plugin->sqlitex_get_metrics(&stats.reprepares);
    }

    tran_type *trans = curtran_gettran();
    rc = bdb_get_num_sc_done(((bdb_state_type *)thedb->bdb_env), trans, (unsigned long long *)&stats.num_sc_done,
                             &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "failed to refresh statistics (%s:%d)\n", __FILE__, __LINE__);
        return 1;
    }
    stats.diskspace = refresh_diskspace(thedb, trans);
    stats.vreplays = gbl_verify_tran_replays;
    stats.nsslfullhandshakes = gbl_ssl_num_full_handshakes;
    stats.nsslpartialhandshakes = gbl_ssl_num_partial_handshakes;
    stats.auth_allowed = gbl_num_auth_allowed;
    stats.auth_denied = gbl_num_auth_denied;
    curtran_puttran(trans);

    update_fastsql_metrics();

    return 0;
}

int init_metrics(void)
{
    time_t t;

    memset(&stats, 0, sizeof(struct comdb2_metrics_store));

    t = time(NULL);
    stats.start_time = (int64_t) t;
    return 0;
}

const char *metric_type(comdb2_metric_type type)
{
    switch (type) {
    case STATISTIC_INTEGER:
        return "INTEGER";
    case STATISTIC_DOUBLE: {
        return "DOUBLE";
    }
    default:
        abort();
    }
}

static time_t queue_start_time = 0;

static time_t metrics_standing_queue_time(void) {
    if (queue_start_time == 0)
        return 0;
    else
        return time(NULL) - queue_start_time;
}

static void update_standing_queue_time(void) {
    if (time_metric_average(thedb->queue_depth) < 1)
        queue_start_time = 0;
    else if (queue_start_time == 0)
        queue_start_time = time(NULL);
}

static void update_cpu_percent(void) 
{
#if _LINUX_SOURCE
   static time_t last_time = 0;
   static int64_t last_counter = 0;
   int hz = sysconf(_SC_CLK_TCK);

   double cpu_percent = 0;

   FILE *f = fopen("/proc/self/stat", "r");
   if (f) {
      char line[1024];
      char *tmp = fgets(line, sizeof(line), f);
      if (!tmp) {
          logmsg(LOGMSG_ERROR, "failed to read from /proc/self/stat\n");
      }
      fclose(f);
      unsigned long utime, stime;
      /* usertime=14 systemtime=15 */
      int rc = sscanf(
          line, "%*d %*s %*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu %lu",
          &utime, &stime);
      if (rc == 2) {
         if (last_time == 0) {
            cpu_percent = 0;
            last_time = time(NULL);
            last_counter = utime + stime;
         }
         else {
            stats.cpu_percent = 0;
            time_t now = time(NULL);
            int64_t sys_ticks = (now - last_time) * hz;

            cpu_percent = ((double) ((utime+stime) - last_counter) / (double) sys_ticks) * 100;

            last_counter = utime+stime;
            last_time = now;
         }
      }
   }

   gbl_cpupercent = cpu_percent;
#endif
}

void update_metrics(void) {
    update_cpu_percent();
    update_standing_queue_time();
    update_weighted_standing_queue_metrics();
}
