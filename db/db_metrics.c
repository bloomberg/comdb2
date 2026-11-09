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
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <sql.h>
#include <bdb_int.h>
#include <comdb2.h>
#include <comdb2_atomic.h>
#include <metrics.h>
#include <bdb_api.h>
#include <net.h>
#include <thread_stats.h>
#include <request_stats.h>
#include <net_appsock.h>
#include <sqllogfill.h>

#include <sys/time.h>
#include <sys/resource.h>
#include "comdb2_query_preparer.h"
#include "net_int.h"

struct comdb2_metrics_store {
    int64_t cache_hits;
    int64_t cache_misses;
    double  cache_hit_rate;
    int64_t leaf_cache_hits;
    int64_t leaf_cache_misses;
    double leaf_cache_hit_rate;
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
    int64_t sorter_rewinds_started;
    int64_t sorter_rewinds_finished;
    /* Only non-zero with the lock_instrumentation tunable on. */
    int64_t lockwait_reader_us;
    int64_t lockwait_reader_count;
    int64_t lockwait_writer_us;
    int64_t lockwait_writer_count;
    int64_t lockwait_repl_us;
    int64_t lockwait_repl_count;
    int64_t lockwait_other_us;
    int64_t lockwait_other_count;
    int64_t release_locks_total_us;
    int64_t release_locks_unlock_us;
    int64_t release_locks_putcurtran_us;
    int64_t release_locks_sleep_us;
    int64_t release_locks_reacquire_us;
    int64_t release_locks_revalidate_us;
    int64_t release_locks_count;
    int64_t release_locks_pagelocks_only_count;
    int64_t sync_dta_us;
    int64_t sync_dta_count;
    int64_t lockprobe_us;
    int64_t lockprobe_count;
    int64_t lockprobe_sampled;
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
    int64_t max_current_connections;
    int64_t diskspace;
    double service_time;
    double queue_depth;
    double concurrent_sql;
    double concurrent_connections;
    int64_t active_connections;
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
    double watchdog_time;
    int64_t distributed_commits;
    int64_t not_durable_commits;
    int64_t incoherent_slow_skips;
    int64_t inmem_repdb_memory;
    int64_t physrep_metadb_sql_count;
    int64_t physrep_no_viable_source;
    int64_t forwarded_block_reqs;
    int64_t rep_logbytes_last_minute;
    int64_t sql_socksql_requests;
    int64_t sql_read_committed_requests;
    int64_t sql_serializable_requests;
    int64_t sql_snapshot_requests;
    int64_t sql_snapshot_asof_requests;

    int64_t page_reads;
    int64_t page_writes;
    int64_t failed_page_reads;
    int64_t failed_page_writes;
    int64_t fsyncs;
    int64_t mempgets;
    int64_t page_bytes_read;
    int64_t page_bytes_written;
    int64_t failed_page_bytes_read;
    int64_t failed_page_bytes_written;

    int64_t sql_logfill_applied_records;
    int64_t sql_logfill_applied_bytes;
    int64_t sql_logfill_sql_finds;
    int64_t sql_logfill_sql_nexts;
    int64_t sql_logfill_queue_size;
    int64_t sql_logfill_queue_blocks;

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
    int64_t legacy_requests;
    int64_t blob_cache_shrinks;
    int64_t blob_cache_shrunk_bytes;
};

/* Published metric values. Writers fill a private copy and publish it under
 * stats_lk; readers copy it out under stats_lk (see metrics_snapshot()). */
static struct comdb2_metrics_store stats;
static pthread_mutex_t stats_lk = PTHREAD_MUTEX_INITIALIZER;

/*
  List of (almost) all comdb2 stats.
  Please keep'em sorted.
*/
comdb2_metric gbl_metrics[] = {
    {"active_connections",
     "Number of client connections the server cannot close (new, queued, running, waiting to retry, or idle in a "
     "transaction)",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST, &stats.active_connections, NULL},
    {"cache_hits", "Buffer pool hits", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.cache_hits,
     NULL},
    {"cache_misses", "Buffer pool misses", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.cache_misses,
     NULL},
    {"cache_hit_rate", "Buffer pool request hit rate", STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.cache_hit_rate, NULL},
    {"leaf_cache_hits", "Buffer pool leaf-page hits", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.leaf_cache_hits, NULL},
    {"leaf_cache_misses", "Buffer pool leaf-page misses", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.leaf_cache_misses, NULL},
    {"leaf_cache_hit_rate", "Buffer pool leaf-page request hit rate", STATISTIC_DOUBLE,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.leaf_cache_hit_rate, NULL},
    {"commits", "Number of commits", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.commits, NULL},
    {"concurrent_sql", "Concurrent SQL queries", STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.concurrent_sql, NULL},
    {"concurrent_connections", "Number of concurrent connections ", STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.concurrent_connections, NULL},
    {"connections", "Total connections", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.connections,
     NULL},
    {"connection_timeouts", "Timed out connection attempts", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.connection_timeouts, NULL},
    {"connection_to_sql_ratio",
     "Ratio of total number of connections to sql "
     "(and nonsql) request counts",
     STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST, &stats.connection_to_sql_ratio, NULL},
    {"cpu_percent", "Database CPU time over last 5 seconds", STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.cpu_percent, NULL},
    {"current_connections", "Number of current connections", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.current_connections, NULL},
    {"deadlocks", "Number of deadlocks", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.deadlocks,
     NULL},
    {"locks_aborted", "Number of locks aborted", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.locks_aborted, NULL},
    {"diskspace", "Disk space used (bytes)", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST, &stats.diskspace,
     NULL},
    {"fstraps", "Number of socket requests", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fstraps,
     NULL},
    {"ismaster", "Is this machine the current master", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.ismaster, NULL},
    {"lockrequests", "Total lock requests", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.lockrequests, NULL},
    {"lockwaits", "Number of lock waits", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.lockwaits,
     NULL},
    {"lockwait_time", "Time spent in lock waits (us)", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.lock_wait_time_us, NULL},
    {"sorter_rewinds_started", "Sorts that have entered the sorter's blocking rewind step", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sorter_rewinds_started, NULL},
    {"sorter_rewinds_finished", "Sorts that have left it; started minus finished is how many are sorting now",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sorter_rewinds_finished, NULL},
    /* The rest of the lockwait_/release_locks_/sync_dta_ family stays zero
       unless the lock_instrumentation tunable is on. */
    {"lockwait_reader_time", "Lock wait time by SQL read cursors (us)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.lockwait_reader_us, NULL},
    {"lockwait_reader_count", "Lock waits by SQL read cursors", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.lockwait_reader_count, NULL},
    {"lockwait_writer_time", "Lock wait time by SQL write transactions (us)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.lockwait_writer_us, NULL},
    {"lockwait_writer_count", "Lock waits by SQL write transactions", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.lockwait_writer_count, NULL},
    {"lockwait_repl_time", "Lock wait time by the replication apply thread (us)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.lockwait_repl_us, NULL},
    {"lockwait_repl_count", "Lock waits by the replication apply thread", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.lockwait_repl_count, NULL},
    {"lockwait_other_time", "Lock wait time by neither (verify, analyze, internal threads) (us)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.lockwait_other_us, NULL},
    {"lockwait_other_count", "Lock waits by neither (replication, utilities)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.lockwait_other_count, NULL},
    {"release_locks_time", "Total time in recover_deadlock, all callers (us)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.release_locks_total_us, NULL},
    {"release_locks_unlock_time", "Dropping cursor page locks (us)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.release_locks_unlock_us, NULL},
    {"release_locks_putcurtran_time", "Dropping the curtran, full release only (us)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.release_locks_putcurtran_us, NULL},
    {"release_locks_sleep_time", "Deliberate backoff inside a release, not contention (us)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.release_locks_sleep_us, NULL},
    {"release_locks_reacquire_time", "Waiting to get the locks back (us)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.release_locks_reacquire_us, NULL},
    {"release_locks_revalidate_time", "Cursor revalidation after a release; not a lock wait (us)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.release_locks_revalidate_us, NULL},
    {"release_locks_count", "Number of recover_deadlock calls", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.release_locks_count, NULL},
    {"release_locks_pagelocks_only_count", "Of those, page-locks-only releases", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.release_locks_pagelocks_only_count, NULL},
    {"sync_dta_time", "Syncing data cursors and blobs before a release (us)", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sync_dta_us, NULL},
    {"sync_dta_count", "Number of pre-release data cursor syncs", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sync_dta_count, NULL},
    {"lockprobe_time", "Checking for lockers waiting on our page locks, over the sampled probes only (us)",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.lockprobe_us, NULL},
    {"lockprobe_count", "Number of has-waiters probes", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.lockprobe_count, NULL},
    {"lockprobe_sampled", "Of those, how many were actually timed", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.lockprobe_sampled, NULL},
    {"memory_ulimit", "Virtual address space ulimit", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.memory_ulimit, NULL},
    {"memory_usage", "Address space size", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST, &stats.memory_usage,
     NULL},
    {"preads", "Number of pread()'s", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.preads, NULL},
    {"pwrites", "Number of pwrite()'s", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.pwrites, NULL},
    {"queue_depth", "Request queue depth", STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST, &stats.queue_depth,
     NULL},
    {"retries", "Number of retries", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.retries, NULL},
    {"service_time", "Service time", STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST, &stats.service_time, NULL},
    {"sql_cost", "Number of sql steps executed (cost)", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.sql_cost, NULL},
    {"sql_count", "Number of sql queries executed", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.sql_count, NULL},
    {"sql_ssl_count", "Number of sql queries executed, via SSL", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sql_ssl_count, NULL},
    {"start_time", "Server start time", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST, &stats.start_time, NULL},
    {"threads", "Number of threads", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST, &stats.threads, NULL},
    {"num_sc_done", "Number of schema changes done", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.num_sc_done, NULL},
    {"checkpoint_ms", "Time taken for last checkpoint", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.last_checkpoint_ms},
    {"checkpoint_total_ms", "Total time taken for checkpoints", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.total_checkpoint_ms},
    {"checkpoint_count", "Total number of checkpoints taken", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.checkpoint_count},
    {"rcache_hits", "Count of root-page cache hits", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.rcache_hits},
    {"rcache_misses", "Count of root-page cache misses", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.rcache_misses},
    {"last_election_ms", "Time taken to resolve last election", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.last_election_ms, NULL},
    {"total_election_ms", "Total time taken to resolve elections", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.total_election_ms, NULL},
    {"election_count", "Total number of elections", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.election_count, NULL},
    {"last_election_time", "Wallclock time last election completed", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.last_election_time, NULL},
    {"udp_sent", "Number of udp packets sent", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.udp_sent,
     NULL},
    {"udp_failed_send", "Number of failed udp sends", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.udp_failed_send, NULL},
    {"udp_received", "Number of udp receives", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.udp_received, NULL},
    {"active_transactions", "Number of active transactions", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.active_transactions, NULL},
    {"max_active_transactions", "Maximum number of active transactions", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.maxactive_transactions, NULL},
    {"total_commits", "Number of transaction commits", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.total_commits, NULL},
    {"total_aborts", "Number of transaction aborts", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.total_aborts, NULL},
    {"sql_queue_time", "Average ms spent waiting in sql queue", STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.sql_queue_time, NULL},
    {"sql_queue_timeouts", "Number of sql items timed-out waiting on queue", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.sql_queue_timeouts, NULL},
    {"handle_buf_queue_time", "Average ms spent waiting in handle-buf queue", STATISTIC_DOUBLE,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.handle_buf_queue_time, NULL},
    {"denied_appsocks", "Number of denied appsock connections", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.denied_appsock_connections, NULL},
    {"locks", "Number of currently held locks", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST, &stats.locks,
     NULL},
    {"temptable_created", "Number of temporary tables created", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.temptable_created, NULL},
    {"temptable_create_requests", "Number of create temporary table requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.temptable_create_reqs, NULL},
    {"temptable_spills", "Number of temporary tables that had to be spilled to disk-backed tables", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.temptable_spills, NULL},
    {"net_drops", "Number of packets that didn't fit on network queue and were dropped", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.net_drops, NULL},
    {"rep_deadlocks", "Replication deadlocks", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.rep_deadlocks, NULL},
    {"rw_evictions", "Dirty page evictions", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.rw_evicts,
     NULL},
    {"standing_queue_time", "How long the database has had a standing queue", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.standing_queue_time, NULL},
    {"nonsql", "Number of non-sql requests (eg: tagged)", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.nonsql, NULL},
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
    {"reprepares", "Number of times statements are reprepared by sqlitex", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.reprepares, NULL},
    {"verify_replays", "Number of replays on verify errors", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.vreplays, NULL},
    {"nsslfullhandshakes", "Number of SSL full handshakes", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.nsslfullhandshakes, NULL},
    {"nsslpartialhandshakes", "Number of SSL partial handshakes", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.nsslpartialhandshakes, NULL},
    {"weighted_queue_depth", "Weighted queue depth", STATISTIC_DOUBLE, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.weighted_queue_depth, NULL},
    {"weighted_standing_queue_time", "How long the database has had a weighted standing queue", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.weighted_standing_queue_time, NULL},
    {"auth_allowed", "Number of successful authentication requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.auth_allowed, NULL},
    {"auth_denied", "Number of failed authentication requests", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.auth_denied, NULL},
    {"watchdog_time", "Number of seconds for a successful watchdog test run", STATISTIC_DOUBLE,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.watchdog_time, NULL},
    {"distributed_commits", "Number of distributed commits", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.distributed_commits, NULL},
    {"not_durable_commits", "Number of not durable commits", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.not_durable_commits, NULL},
    {"disabled_incoherent_slows", "Disabled incoherent-slow count", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.incoherent_slow_skips, NULL},
    {"inmem_repdb_memory", "Memory utilized by in-memory repdb", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.inmem_repdb_memory, NULL},
    {"physrep_metadb_sql_count", "Count of SQL statements executed against physrep metadb", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.physrep_metadb_sql_count, NULL},
    {"physrep_no_viable_source", "This physical replicant found no source retaining logs that cover its LSN",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST, &stats.physrep_no_viable_source, NULL},
    {"forwarded_block_reqs", "Number of tagged write requests forwarded from a replicant to the master",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.forwarded_block_reqs, NULL},
    {"rep_logbytes_last_minute", "Log bytes applied by this replicant over the last minute", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.rep_logbytes_last_minute, NULL},
    {"sql_socksql_requests", "Number of client sql requests run at socksql isolation", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sql_socksql_requests, NULL},
    {"sql_read_committed_requests", "Number of client sql requests run at read committed isolation", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sql_read_committed_requests, NULL},
    {"sql_serializable_requests", "Number of client sql requests run at serializable isolation", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sql_serializable_requests, NULL},
    {"sql_snapshot_requests", "Number of client sql requests run at snapshot isolation", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sql_snapshot_requests, NULL},
    {"sql_snapshot_asof_requests",
     "Number of point-in-time (BEGIN TRANSACTION AS OF) snapshot or serializable transactions", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sql_snapshot_asof_requests, NULL},
    {"page_reads", "Total page reads", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.page_reads,
     NULL},
    {"page_writes", "Total page writes", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.page_writes,
     NULL},
    {"failed_page_reads", "Total failed page reads", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.failed_page_reads, NULL},
    {"failed_page_writes", "Total failed page writes", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.failed_page_writes, NULL},
    {"fsyncs", "Fsync count", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fsyncs, NULL},
    {"mempgets", "Mempget count", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.mempgets, NULL},
    {"page_bytes_read", "Total page bytes read", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.page_bytes_read, NULL},
    {"page_bytes_written", "Total page bytes written", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.page_bytes_written, NULL},
    {"failed_page_bytes_read", "Total failed page bytes read", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.failed_page_bytes_read, NULL},
    {"failed_page_bytes_written", "Total failed page bytes written", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.failed_page_bytes_written, NULL},
    {"sql_logfill_applied_records", "Number of sqllogfill applied records", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sql_logfill_applied_records, NULL},
    {"sql_logfill_applied_bytes", "Number of sqllogfill applied bytes", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sql_logfill_applied_bytes, NULL},
    {"sql_logfill_sql_finds", "Number of sqllogfill sql finds", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.sql_logfill_sql_finds, NULL},
    {"sql_logfill_sql_nexts", "Number of sqllogfill sql nexts", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.sql_logfill_sql_nexts, NULL},
    {"sql_logfill_queue_size", "Size of the sqllogfill queue", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_LATEST,
     &stats.sql_logfill_queue_size, NULL},
    {"sql_logfill_queue_blocks", "Number of blocks in the sqllogfill queue", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.sql_logfill_queue_blocks, NULL},
    {"fastsql_execute_inline_params", "Number of fastsql 'execute' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_execute_inline_params, NULL},
    {"fastsql_set_isolation_level", "Number of fastsql 'set isolation level' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_set_isolation_level, NULL},
    {"fastsql_set_timeout", "Number of fastsql 'set timeout' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_set_timeout, NULL},
    {"fastsql_set_info", "Number of fastsql 'set info' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_set_info, NULL},
    {"fastsql_execute_inline_params_tz", "Number of fastsql 'execute with timezone' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_execute_inline_params_tz, NULL},
    {"fastsql_set_heartbeat", "Number of fastsql 'set heartbeat' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_set_heartbeat, NULL},
    {"fastsql_pragma", "Number of fastsql pragma requests", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_pragma, NULL},
    {"fastsql_reset", "Number of fastsql reset requests", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_reset, NULL},
    {"fastsql_execute_replaceable_params", "Number of fastsql 'execute with replacable parameters' requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_execute_replaceable_params, NULL},
    {"fastsql_set_sql_debug", "Number of fastsql 'set sql debug' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_set_sql_debug, NULL},
    {"fastsql_grab_dbglog", "Number of fastsql 'grab dbglog' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_grab_dbglog, NULL},
    {"fastsql_set_user", "Number of fastsql 'set user' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_set_user, NULL},
    {"fastsql_set_password", "Number of fastsql 'set password' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_set_password, NULL},
    {"fastsql_set_endian", "Number of fastsql 'set endian' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_set_endian, NULL},
    {"fastsql_execute_replaceable_params_tz",
     "Number of fastsql 'execute with replacable parameters & timezone' "
     "requests",
     STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_execute_replaceable_params_tz, NULL},
    {"fastsql_get_effects", "Number of fastsql 'get effects' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_get_effects, NULL},
    {"fastsql_set_planner_effort", "Number of fastsql 'set planner effort' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_set_planner_effort, NULL},
    {"fastsql_set_remote_access", "Number of fastsql 'set remote access' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_set_remote_access, NULL},
    {"fastsql_osql_max_trans", "Number of fastsql 'osql max trans' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_osql_max_trans, NULL},
    {"fastsql_set_datetime_precision", "Number of fastsql 'set datetime precision' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_set_datetime_precision, NULL},
    {"fastsql_sslconn", "Number of fastsql 'sslconn' requests", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.fastsql_sslconn, NULL},
    {"fastsql_execute_stop", "Number of fastsql 'execute stop' requests", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.fastsql_execute_stop, NULL},
    {"legacy_requests", "Number of non-cdb2api requests", STATISTIC_INTEGER, STATISTIC_COLLECTION_TYPE_CUMULATIVE,
     &stats.legacy_requests, NULL},
    {"max_current_connections", "Max current connections for sampled interval", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_LATEST, &stats.max_current_connections, NULL},
    {"blob_cache_shrinks", "Number of times a cursor's cached blob buffer was shrunk", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.blob_cache_shrinks, NULL},
    {"blob_cache_shrunk_bytes", "Bytes released by shrinking cursors' cached blob buffers", STATISTIC_INTEGER,
     STATISTIC_COLLECTION_TYPE_CUMULATIVE, &stats.blob_cache_shrunk_bytes, NULL},
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

extern int64_t gbl_distributed_commit_count;
extern int64_t gbl_not_durable_commit_count;
extern int64_t gbl_incoherent_slow_skips;
extern int64_t gbl_inmem_repdb_memory;
extern int64_t gbl_physrep_metadb_sql_count;
extern int64_t gbl_blob_cache_shrinks;
extern int64_t gbl_blob_cache_shrunk_bytes;
extern int gbl_physrep_no_viable_source;
extern int64_t gbl_forwarded_block_reqs;
extern int64_t gbl_rep_logbytes;
extern int64_t gbl_sql_socksql_requests;
extern int64_t gbl_sql_read_committed_requests;
extern int64_t gbl_sql_serializable_requests;
extern int64_t gbl_sql_snapshot_requests;
extern int64_t gbl_sql_snapshot_asof_requests;

static void update_sqllogfill_metrics(struct comdb2_metrics_store *s)
{
    sql_logfill_metrics(&s->sql_logfill_applied_records, &s->sql_logfill_applied_bytes, &s->sql_logfill_sql_finds,
                        &s->sql_logfill_sql_nexts, &s->sql_logfill_queue_size, &s->sql_logfill_queue_blocks);
}

static void update_fastsql_metrics(struct comdb2_metrics_store *s)
{
    s->fastsql_execute_inline_params = gbl_fastsql_execute_inline_params;
    s->fastsql_set_isolation_level = gbl_fastsql_set_isolation_level;
    s->fastsql_set_timeout = gbl_fastsql_set_timeout;
    s->fastsql_set_info = gbl_fastsql_set_info;
    s->fastsql_execute_inline_params_tz = gbl_fastsql_execute_inline_params_tz;
    s->fastsql_set_heartbeat = gbl_fastsql_set_heartbeat;
    s->fastsql_pragma = gbl_fastsql_pragma;
    s->fastsql_reset = gbl_fastsql_reset;
    s->fastsql_execute_replaceable_params = gbl_fastsql_execute_replaceable_params;
    s->fastsql_set_sql_debug = gbl_fastsql_set_sql_debug;
    s->fastsql_grab_dbglog = gbl_fastsql_grab_dbglog;
    s->fastsql_set_user = gbl_fastsql_set_user;
    s->fastsql_set_password = gbl_fastsql_set_password;
    s->fastsql_set_endian = gbl_fastsql_set_endian;
    s->fastsql_execute_replaceable_params_tz = gbl_fastsql_execute_replaceable_params_tz;
    s->fastsql_get_effects = gbl_fastsql_get_effects;
    s->fastsql_set_planner_effort = gbl_fastsql_set_planner_effort;
    s->fastsql_set_remote_access = gbl_fastsql_set_remote_access;
    s->fastsql_osql_max_trans = gbl_fastsql_osql_max_trans;
    s->fastsql_set_datetime_precision = gbl_fastsql_set_datetime_precision;
    s->fastsql_sslconn = gbl_fastsql_sslconn;
    s->fastsql_execute_stop = gbl_fastsql_execute_stop;
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

/* Ring of (time, gbl_rep_logbytes) samples taken by update_metrics() */
#define LOGBYTES_SAMPLES 64
static struct {
    time_t time;
    int64_t bytes;
} logbytes_samples[LOGBYTES_SAMPLES];
static int logbytes_next = 0;
static int64_t rep_logbytes_last_minute = 0;

static void update_rep_logbytes_metrics(void)
{
    time_t now = time(NULL);
    int64_t bytes = ATOMIC_LOAD64(gbl_rep_logbytes);
    int64_t base = bytes;

    /* Baseline is the newest sample at least 60s old, else the oldest we have */
    for (int i = 1; i <= LOGBYTES_SAMPLES; i++) {
        int idx = (logbytes_next - i + LOGBYTES_SAMPLES) % LOGBYTES_SAMPLES;
        if (logbytes_samples[idx].time == 0)
            break;
        base = logbytes_samples[idx].bytes;
        if (now - logbytes_samples[idx].time >= 60)
            break;
    }
    rep_logbytes_last_minute = bytes - base;

    logbytes_samples[logbytes_next].time = now;
    logbytes_samples[logbytes_next].bytes = bytes;
    logbytes_next = (logbytes_next + 1) % LOGBYTES_SAMPLES;
}

static time_t metrics_standing_queue_time(void);

static int fetch_metrics(struct comdb2_metrics_store *s)
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

    s->commits = n_commits;
    s->blob_cache_shrinks = gbl_blob_cache_shrinks;
    s->blob_cache_shrunk_bytes = gbl_blob_cache_shrunk_bytes;
    s->fstraps = n_fstrap;
    s->nonsql = n_fstrap                  /* legacy protocol over socketrequest */
                + n_qtrap                 /* legacy protocol */
                + gbl_legacy_requests_all /* legacy protocol over cdb2api */
                - n_dbinfo;
    s->retries = n_retries;
    s->sql_cost = gbl_nsql_steps + gbl_nnewsql_steps;
    s->sql_count = gbl_nsql + gbl_nnewsql;
    s->sql_ssl_count = gbl_nnewsql_ssl;
    s->current_connections = net_get_num_current_non_appsock_accepts(thedb->handle_sibling) + active_appsock_conns;
    s->active_connections = get_active_connections();

    rc = bdb_get_lock_counters(thedb->bdb_env, &s->deadlocks, &s->locks_aborted, &s->lockwaits, &s->lockrequests);
    if (rc) {
        logmsg(LOGMSG_ERROR, "failed to refresh statistics (%s:%d)\n", __FILE__,
               __LINE__);
        return 1;
    }

    {
        struct lock_role_stats role;
        bdb_get_lock_role_counters(&role);
        s->lockwait_reader_us = role.reader_wait_us;
        s->lockwait_reader_count = role.reader_waits;
        s->lockwait_writer_us = role.writer_wait_us;
        s->lockwait_writer_count = role.writer_waits;
        s->lockwait_repl_us = role.repl_wait_us;
        s->lockwait_repl_count = role.repl_waits;
        s->lockwait_other_us = role.other_wait_us;
        s->lockwait_other_count = role.other_waits;

        extern uint64_t gbl_rdlk_total_us, gbl_rdlk_unlock_us, gbl_rdlk_release_us, gbl_rdlk_sleep_us,
            gbl_rdlk_reacquire_us, gbl_rdlk_revalidate_us, gbl_rdlk_count, gbl_rdlk_pagelocks_only_count,
            gbl_sync_dta_us, gbl_sync_dta_count;
        s->release_locks_total_us = ATOMIC_LOAD64(gbl_rdlk_total_us);
        s->release_locks_unlock_us = ATOMIC_LOAD64(gbl_rdlk_unlock_us);
        s->release_locks_putcurtran_us = ATOMIC_LOAD64(gbl_rdlk_release_us);
        s->release_locks_sleep_us = ATOMIC_LOAD64(gbl_rdlk_sleep_us);
        s->release_locks_reacquire_us = ATOMIC_LOAD64(gbl_rdlk_reacquire_us);
        s->release_locks_revalidate_us = ATOMIC_LOAD64(gbl_rdlk_revalidate_us);
        s->release_locks_count = ATOMIC_LOAD64(gbl_rdlk_count);
        s->release_locks_pagelocks_only_count = ATOMIC_LOAD64(gbl_rdlk_pagelocks_only_count);
        s->sync_dta_us = ATOMIC_LOAD64(gbl_sync_dta_us);
        s->sync_dta_count = ATOMIC_LOAD64(gbl_sync_dta_count);

        extern uint64_t gbl_probe_us, gbl_probes, gbl_probes_timed;
        s->lockprobe_us = ATOMIC_LOAD64(gbl_probe_us);
        s->lockprobe_count = ATOMIC_LOAD64(gbl_probes);
        s->lockprobe_sampled = ATOMIC_LOAD64(gbl_probes_timed);
    }

    rc = bdb_get_bpool_counters(thedb->bdb_env, &s->cache_hits, &s->cache_misses, &s->leaf_cache_hits,
                                &s->leaf_cache_misses, NULL, NULL, &s->rw_evicts);
    if (rc) {
        logmsg(LOGMSG_ERROR, "failed to refresh statistics (%s:%d)\n", __FILE__,
               __LINE__);
        return 1;
    }

    pstats = bdb_get_process_stats();
    s->preads = pstats->n_preads;
    s->pwrites = pstats->n_pwrites;
    s->lock_wait_time_us = pstats->lock_wait_time_us;

    /* Lets a concurrent session act exactly when a sort's blocking window opens,
       instead of guessing at it with a sleep. */
    {
        extern uint64_t gbl_sorter_rewinds_started, gbl_sorter_rewinds_finished;
        /* Read finished first: it only ever trails started, so sampling it
           earlier keeps started-minus-finished from going briefly negative when
           a sort completes between the two reads. */
        s->sorter_rewinds_finished = ATOMIC_LOAD64(gbl_sorter_rewinds_finished);
        s->sorter_rewinds_started = ATOMIC_LOAD64(gbl_sorter_rewinds_started);
    }

    /* connections stats */
    s->connections = net_get_num_accepts(thedb->handle_sibling);
    s->connection_timeouts = net_get_num_accept_timeouts(thedb->handle_sibling);

    int64_t total_reqs = s->sql_count + s->nonsql;
    s->connection_to_sql_ratio = (total_reqs > 0) ? (s->connections / (double)total_reqs) : 0;

    /* cache hit rate (total) and leaf-page hit rate */
    int64_t th = s->cache_hits, tm = s->cache_misses;
    s->cache_hit_rate = (th + tm) ? 100 * ((double)th / ((double)th + (double)tm)) : 100.0;
    int64_t lh = s->leaf_cache_hits, lm = s->leaf_cache_misses;
    s->leaf_cache_hit_rate = (lh + lm) ? 100 * ((double)lh / ((double)lh + (double)lm)) : 100.0;

    s->memory_ulimit = 0;
    s->memory_usage = 0;
    s->threads = 0;
#ifdef _LINUX_SOURCE
    /* memory */
    struct rlimit rl;
    rc = getrlimit(RLIMIT_AS, &rl);
    if (rc == 0) {
        if (rl.rlim_cur == RLIM_INFINITY)
            s->memory_ulimit = 0;
        else
            s->memory_ulimit = rl.rlim_cur / (1024 * 1024);
    }
    else {
        s->memory_ulimit = 0;
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
            s->threads = num_threads;
            s->memory_usage = vmsize / (1024 * 1024);
        }
    }
    s->cpu_percent = gbl_cpupercent;
#endif
    s->service_time = time_metric_average(thedb->service_time);
    s->weighted_queue_depth = weighted_queue_depth;
    if (gbl_track_weighted_queue_metrics_separately)
        s->queue_depth = time_metric_average(thedb->queue_depth);
    else
        s->queue_depth = s->weighted_queue_depth;
    s->concurrent_sql = time_metric_average(thedb->concurrent_queries);
    s->sql_queue_time = time_metric_average(thedb->sql_queue_time);
    s->sql_queue_timeouts = get_all_sql_pool_timeouts();
    s->handle_buf_queue_time = time_metric_average(thedb->handle_buf_queue_time);
    s->concurrent_connections = time_metric_average(thedb->connections);
    s->watchdog_time = time_metric_average(thedb->watchdog_time);
    s->distributed_commits = gbl_distributed_commit_count;
    s->not_durable_commits = gbl_not_durable_commit_count;
    s->incoherent_slow_skips = gbl_incoherent_slow_skips;
    s->inmem_repdb_memory = gbl_inmem_repdb_memory;
    s->physrep_metadb_sql_count = gbl_physrep_metadb_sql_count;
    s->physrep_no_viable_source = gbl_physrep_no_viable_source;
    s->forwarded_block_reqs = gbl_forwarded_block_reqs;
    s->rep_logbytes_last_minute = rep_logbytes_last_minute;
    s->sql_socksql_requests = gbl_sql_socksql_requests;
    s->sql_read_committed_requests = gbl_sql_read_committed_requests;
    s->sql_serializable_requests = gbl_sql_serializable_requests;
    s->sql_snapshot_requests = gbl_sql_snapshot_requests;
    s->sql_snapshot_asof_requests = gbl_sql_snapshot_asof_requests;
    struct global_stats gstats = {0};

    global_request_stats(&gstats);

    s->page_reads = gstats.page_reads;
    s->page_writes = gstats.page_writes;
    s->failed_page_reads = gstats.failed_page_reads;
    s->failed_page_writes = gstats.failed_page_writes;
    s->fsyncs = gstats.fsyncs;
    s->mempgets = gstats.mempgets;
    s->page_bytes_read = gstats.page_bytes_read;
    s->page_bytes_written = gstats.page_bytes_written;
    s->failed_page_bytes_read = gstats.failed_page_bytes_read;
    s->failed_page_bytes_written = gstats.failed_page_bytes_written;

    int master =
        bdb_whoismaster((bdb_state_type *)thedb->bdb_env) == gbl_myhostname ? 1
                                                                            : 0;
    s->ismaster = master;
    s->last_checkpoint_ms = gbl_last_checkpoint_ms;
    s->total_checkpoint_ms = gbl_total_checkpoint_ms;
    s->checkpoint_count = gbl_checkpoint_count;
    s->rcache_hits = rcache_hits;
    s->rcache_misses = rcache_miss;
    s->last_election_ms = gbl_last_election_time_ms;
    s->total_election_ms = gbl_total_election_time_ms;
    s->election_count = gbl_election_count;
    s->last_election_time = gbl_election_time_completed;
    unsigned int sent, failed_send, received;
    udp_stats(&sent, &failed_send, &received);
    s->udp_sent = sent;
    s->udp_failed_send = failed_send;
    s->udp_received = received;
    bdb_get_txn_stats(thedb->bdb_env, &s->active_transactions, &s->maxactive_transactions, &s->total_commits,
                      &s->total_aborts);
    s->denied_appsock_connections = gbl_denied_appsock_connection_count;
    if (bdb_lock_stats(thedb->bdb_env, &s->locks))
        s->locks = 0;
    s->temptable_created = gbl_temptable_created;
    s->temptable_create_reqs = gbl_temptable_create_reqs;
    s->temptable_spills = gbl_temptable_spills;

    s->net_drops = get_hosts_metric("replication", NET_DROPS);

    bdb_rep_deadlocks(thedb->bdb_env, &s->rep_deadlocks);

    s->weighted_standing_queue_time = metrics_weighted_standing_queue_time();
    if (gbl_track_weighted_queue_metrics_separately)
        s->standing_queue_time = metrics_standing_queue_time();
    else
        s->standing_queue_time = s->weighted_standing_queue_time;

#if 0
    bdb_min_truncate(thedb->bdb_env, &min_file, &min_offset, &min_timestamp);
    s->minimum_truncation_file = min_file;
    s->minimum_truncation_offset = min_offset;
    s->minimum_truncation_timestamp = min_timestamp;
#endif

    if (gbl_old_column_names && query_preparer_plugin &&
        query_preparer_plugin->sqlitex_get_metrics) {
        query_preparer_plugin->sqlitex_get_metrics(&s->reprepares);
    }

    tran_type *trans = curtran_gettran();
    rc = bdb_get_num_sc_done(((bdb_state_type *)thedb->bdb_env), trans, (unsigned long long *)&s->num_sc_done, &bdberr);
    if (rc) {
        curtran_puttran(trans);
        logmsg(LOGMSG_ERROR, "failed to refresh statistics (%s:%d)\n", __FILE__, __LINE__);
        return 1;
    }
    s->diskspace = refresh_diskspace(thedb, trans);
    s->vreplays = gbl_verify_tran_replays;
    s->nsslfullhandshakes = gbl_ssl_num_full_handshakes;
    s->nsslpartialhandshakes = gbl_ssl_num_partial_handshakes;
    s->auth_allowed = gbl_num_auth_allowed;
    s->auth_denied = gbl_num_auth_denied;
    s->legacy_requests = gbl_legacy_requests;
    curtran_puttran(trans);

    update_sqllogfill_metrics(s);
    update_fastsql_metrics(s);
    s->max_current_connections = time_metric_max(thedb->connections);

    return 0;
}

/* Fetch all values into a private copy, then publish them in one step, so
 * concurrent readers never see a half-refreshed or transiently zeroed set. */
int refresh_metrics(void)
{
    struct comdb2_metrics_store local;

    Pthread_mutex_lock(&stats_lk);
    local = stats; /* keep fields that are not refreshed (start_time) */
    Pthread_mutex_unlock(&stats_lk);

    int rc = fetch_metrics(&local);
    if (rc)
        return rc;

    Pthread_mutex_lock(&stats_lk);
    stats = local;
    Pthread_mutex_unlock(&stats_lk);
    return 0;
}

void *metrics_snapshot(void)
{
    struct comdb2_metrics_store *snap = malloc(sizeof(*snap));
    if (snap == NULL)
        return NULL;
    Pthread_mutex_lock(&stats_lk);
    *snap = stats;
    Pthread_mutex_unlock(&stats_lk);
    return snap;
}

const void *metric_snapshot_value(const void *snap, const comdb2_metric *m)
{
    ptrdiff_t off = (const char *)m->var - (const char *)&stats;
    assert(off >= 0 && off < sizeof(stats));
    return (const char *)snap + off;
}

int init_metrics(void)
{
    time_t t;

    t = time(NULL);
    Pthread_mutex_lock(&stats_lk);
    memset(&stats, 0, sizeof(struct comdb2_metrics_store));
    stats.start_time = (int64_t) t;
    Pthread_mutex_unlock(&stats_lk);
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
    update_rep_logbytes_metrics();
}
