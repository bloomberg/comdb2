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
#include "comdb2.h"
#include "comdb2_atomic.h"
#include "metrics.h"
#include "bdb_api.h"
#include "net.h"
#include "thread_stats.h"

#include <sys/time.h>
#include <sys/resource.h>

struct comdb2_metrics_store {
    int64_t cache_hits;
    int64_t cache_misses;
    double  cache_hit_rate;
    int64_t commits;
    int64_t connections;
    int64_t connection_timeouts;
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


static int64_t refresh_diskspace(struct dbenv *dbenv)
{
    int64_t total = 0;
    int ndb;
    struct dbtable *db;
    unsigned int num_logs;

    for(ndb = 0; ndb < dbenv->num_dbs; ndb++)
    {
        db = dbenv->dbs[ndb];
        total += calc_table_size(db, 0);
    }
    for(ndb = 0; ndb < dbenv->num_qdbs; ndb++)
    {
        db = dbenv->qdbs[ndb];
        total += calc_table_size(db, 0);
    }
    total += bdb_logs_size(dbenv->bdb_env, &num_logs);
    return total;
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
    extern int active_appsock_conns; int bdberr;
#if 0
    int min_file, min_offset;
    int32_t min_timestamp;
#endif

    /* Check whether the server is exiting. */
    if (db_is_stopped())
        return 1;

    stats.commits = n_commits;
    stats.fstraps = n_fstrap;
    stats.retries = n_retries;
    stats.sql_cost = gbl_nsql_steps + gbl_nnewsql_steps;
    stats.sql_count = gbl_nsql + gbl_nnewsql;
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
    stats.diskspace = refresh_diskspace(thedb);
    stats.service_time = time_metric_average(thedb->service_time);
    stats.queue_depth = time_metric_average(thedb->queue_depth);
    stats.concurrent_sql = time_metric_average(thedb->concurrent_queries);
    stats.sql_queue_time = time_metric_average(thedb->sql_queue_time);
    stats.sql_queue_timeouts = thdpool_get_timeouts(gbl_sqlengine_thdpool);
    stats.handle_buf_queue_time =
        time_metric_average(thedb->handle_buf_queue_time);
    stats.concurrent_connections = time_metric_average(thedb->connections);
    int master =
        bdb_whoismaster((bdb_state_type *)thedb->bdb_env) == gbl_mynode ? 1 : 0;
    stats.ismaster = master;
    rc = bdb_get_num_sc_done(((bdb_state_type *)thedb->bdb_env), NULL,
                             (unsigned long long *)&stats.num_sc_done, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "failed to refresh statistics (%s:%d)\n", __FILE__,
               __LINE__);
        return 1;
    }
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

    bdb_rep_stats(thedb->bdb_env, &stats.rep_deadlocks);

    stats.standing_queue_time = metrics_standing_queue_time();

#if 0
    bdb_min_truncate(thedb->bdb_env, &min_file, &min_offset, &min_timestamp);
    stats.minimum_truncation_file = min_file;
    stats.minimum_truncation_offset = min_offset;
    stats.minimum_truncation_timestamp = min_timestamp;
#endif
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
    double qdepth = time_metric_average(thedb->queue_depth) + time_metric_average(thedb->handle_buf_queue_time);
    if (queue_start_time == 0 && qdepth > 1)
        queue_start_time = time(NULL);
    else if (qdepth < 1)
        queue_start_time = 0;
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
}
