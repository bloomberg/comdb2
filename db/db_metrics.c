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
    {"connections", "Total connections", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, 
     &stats.connections, NULL},
    {"connection_timeouts", "Timed out connection attempts", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, 
     &stats.connection_timeouts, NULL},
    {"cpu_percent", "Timed out connection attempts", STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST, 
     &stats.cpu_percent, NULL},
    {"deadlocks", "Number of deadlocks", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, 
     &stats.deadlocks, NULL},
    {"fstraps", "Number of socket requests", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, 
     &stats.fstraps, NULL}, 
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
    {"retries", "Number of retries", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.retries,
     NULL},
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


/* TODO: this isn't threadsafe. */
static time_t last_time;
static int64_t last_counter;

int refresh_metrics(void)
{
    int rc;
    const struct bdb_thread_stats *pstats;

    /* Check whether the server is exiting. */
    if (thedb->exiting || thedb->stopped)
        return 1;

    stats.commits = n_commits;
    stats.fstraps = n_fstrap;
    stats.retries = n_retries;
    stats.sql_cost = gbl_nsql_steps + gbl_nnewsql_steps;
    stats.sql_count = gbl_nsql + gbl_nnewsql;

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
    int hz = sysconf(_SC_CLK_TCK);
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
        unsigned long vmsize, utime, stime;
        /* usertime=14 systemtime=15 threads=20 vm=23 */
        rc = sscanf(line, "%*d %*s %*c %*d %*d %*d %*d %*d %*u %*lu %*lu %*lu %*lu %lu %lu %*ld %*ld %*ld %*ld %ld %*ld %*llu %lu", &utime, &stime, &num_threads, &vmsize);
        if (rc == 4) {
            stats.threads = num_threads;
            stats.memory_usage = vmsize / (1024*1024);
            if (last_time == 0) {
                stats.cpu_percent = 0;
                last_time = time(NULL);
                last_counter = utime + stime;
            }
            else {
                stats.cpu_percent = 0;
                time_t now = time(NULL);
                int64_t sys_ticks = (now - last_time) * hz;

                stats.cpu_percent = ((double) ((utime+stime) - last_counter) / (double) sys_ticks) * 100;
                last_counter = utime+stime;
                last_time = now;
            }
        }
    }
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
