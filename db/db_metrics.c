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

#include <sys/time.h>
#include <sys/resource.h>

struct comdb2_metrics_store {
    int64_t bpool_hits;
    int64_t bpool_misses;
    double  cache_hit_rate;
    int64_t commits;
    int64_t connections;
    int64_t connection_timeouts;
    double  cpu_percent;
    int64_t deadlocks;
    int64_t fstraps;
    int64_t lockrequests;
    int64_t lockwaits;
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
};

static struct comdb2_metrics_store stats;

/*
  List of (almost) all comdb2 stats.
  Please keep'em sorted.
*/
comdb2_metric gbl_metrics[] = {
    {"bpool_hits", "Buffer pool hits", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.bpool_hits,
     NULL},
    {"bpool_misses", "Buffer pool misses", STATISTIC_COLLECTION_TYPE_CUMULATIVE, STATISTIC_INTEGER,
     &stats.bpool_misses, NULL},
    {"cache_hit_rate", "Buffer pool request hit rate", STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST, 
     &stats.cache_hit_rate, NULL},
    {"commits", "Number of commits", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.commits,
     NULL},
    {"concurrent_sql", "Concurrent SQL queries",  STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST, 
     &stats.concurrent_sql, NULL},
    {"concurrent_connections", "Number of concurrent connections ",  STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST, 
     &stats.concurrent_connections, NULL},
    {"connections", "Total connections", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, 
     &stats.connections, NULL},
    {"connection_timeouts", "Timed out connection attempts", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, 
     &stats.connection_timeouts, NULL},
    {"cpu_percent", "Database CPU time over last 5 seconds", STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST, 
     &stats.cpu_percent, NULL},
    {"current_connections", "Number of current connections", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.current_connections, NULL},
    {"deadlocks", "Number of deadlocks", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, 
     &stats.deadlocks, NULL},
    {"diskspace", "Disk space used (bytes)",  STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST, 
     &stats.diskspace, NULL},
    {"fstraps", "Number of socket requests", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, 
     &stats.fstraps, NULL}, 
    {"ismaster", "Is this machine the current master", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST, 
     &stats.ismaster, NULL},
    {"lockrequests", "Total lock requests", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.lockrequests, NULL},
    {"lockwaits", "Number of lock waits", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, 
     &stats.lockwaits, NULL},
    {"memory_ulimit", "Virtual address space ulimit", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST, 
     &stats.memory_ulimit, NULL},
    {"memory_usage", "Address space size",  STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.memory_usage, NULL},
    {"preads", "Number of pread()'s", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,  &stats.preads,
     NULL},
    {"pwrites", "Number of pwrite()'s", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.pwrites,
     NULL},
    {"queue_depth", "Request queue depth",  STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST, 
     &stats.queue_depth, NULL},
    {"retries", "Number of retries", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.retries,
     NULL},
    {"service_time", "Service time",  STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST, 
     &stats.service_time, NULL},
    {"sql_cost", "Number of sql steps executed (cost)", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.sql_cost, NULL},
    {"sql_count", "Number of sql queries executed", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.sql_count, NULL},
    {"start_time", "Server start time", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.start_time, NULL},
    {"threads", "Number of threads",  STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.threads, NULL},
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


static int64_t refresh_diskspace(struct dbenv *dbenv) {
    int64_t total = 0;
    int ndb;
    struct dbtable *db;
    unsigned int num_logs;

    for(ndb = 0; ndb < dbenv->num_dbs; ndb++)
    {
        db = dbenv->dbs[ndb];
        total += calc_table_size(db);
    }
    for(ndb = 0; ndb < dbenv->num_qdbs; ndb++)
    {
        db = dbenv->qdbs[ndb];
        total += calc_table_size(db);
    }
    total += bdb_logs_size(dbenv->bdb_env, &num_logs);
    return total;
}

/* TODO: this isn't threadsafe. */
static time_t last_time;
static int64_t last_counter;

int refresh_metrics(void)
{
    int rc;
    const struct bdb_thread_stats *pstats;
    extern int active_appsock_conns;

    /* Check whether the server is exiting. */
    if (thedb->exiting || thedb->stopped)
        return 1;

    stats.commits = n_commits;
    stats.fstraps = n_fstrap;
    stats.retries = n_retries;
    stats.sql_cost = gbl_nsql_steps + gbl_nnewsql_steps;
    stats.sql_count = gbl_nsql + gbl_nnewsql;
    stats.current_connections = net_get_num_current_non_appsock_accepts(thedb->handle_sibling) + active_appsock_conns;

    rc = bdb_get_lock_counters(thedb->bdb_env, &stats.deadlocks,
                               &stats.lockwaits, &stats.lockrequests);
    if (rc) {
        fprintf(stderr, "failed to refresh statistics (%s:%d)\n", __FILE__,
               __LINE__);
        return 1;
    }

    rc = bdb_get_bpool_counters(thedb->bdb_env, &stats.bpool_hits,
                                &stats.bpool_misses);
    if (rc) {
        fprintf(stderr, "failed to refresh statistics (%s:%d)\n", __FILE__,
               __LINE__);
        return 1;
    }

    pstats = bdb_get_process_stats();
    stats.preads = pstats->n_preads;
    stats.pwrites = pstats->n_pwrites;

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
        fgets(line, sizeof(line), f);
        fclose(f);
        long num_threads;
        unsigned long vmsize;
        rc = sscanf(line, "%*d %*s %*c %*d %*d %*d %*d %*d %*u %*lu %*lu %*lu %*lu %*lu %*lu %*ld %*ld %*ld %*ld %ld %*ld %*llu %lu", &num_threads, &vmsize);
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
    stats.concurrent_connections = time_metric_average(thedb->connections);
    int master = bdb_whoismaster((bdb_state_type*) thedb->bdb_env) == gbl_mynode ? 1 : 0; 
    stats.ismaster = master;

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

void update_cpu_percent(void) {
   static time_t last_time = 0;
   static int64_t last_counter = 0;
   int hz = sysconf(_SC_CLK_TCK);

   double cpu_percent = 0;

   FILE *f = fopen("/proc/self/stat", "r");
   if (f) {
      char line[1024];
      fgets(line, sizeof(line), f);
      fclose(f);
      unsigned long utime, stime;
      /* usertime=14 systemtime=15 */
      int rc = sscanf(line, "%*d %*s %*c %*d %*d %*d %*d %*d %*u %*lu %*lu %*lu %*lu %lu %lu", &utime, &stime);
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
}


