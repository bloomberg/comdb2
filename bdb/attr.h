/*
   Copyright 2015 Bloomberg Finance L.P.

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

/* This file defines the bdb attributes. It is included multiple times in some
 * files so should not have any include guards.
 *
 * DEF_ATTR() should be a predefined macro with these parameters:
 *
 * DEF_ATTR(NAME, name, type, default)
 *
 *  NAME - an enum constant with the name BDB_ATTR_##NAME will be defined
 *         in bdb_api.h
 *
 *  name - name of the variable in the bdb_attr_tag struct
 *  type - one of these constants:
 *      SECS
 *      MSECS
 *      BYTES
 *      MBYTES
 *      BOOLEAN
 *      QUANTITY
 *      PERCENT
 *      these actually map to BDB_ATTRTYPE_ constants
 *  default - default value as an integer
 *  description - description of the variable
 */

/* If we reorder or delete any of thse you must do a full rebuild of bdb
 * and of db as the BDB_ATTR_ constants will have changed. */
DEF_ATTR(REPTIMEOUT, reptimeout, SECS, 20, "Replication timeout")
DEF_ATTR(CHECKPOINTTIME, checkpointtime, SECS, 60,
         "Write a checkpoint at this interval.")
DEF_ATTR(CHECKPOINTTIMEPOLL, checkpointtimepoll, MSECS, 100,
         "Poll a random amount of time lesser than this before writing a "
         "checkpoint (attempting to prevent mulitple databases from "
         "checkpointing at the same exact time).")
DEF_ATTR(LOGFILESIZE, logfilesize, BYTES, 41943040,
         "Attempt to keep each log file around this size.")
DEF_ATTR(LOGFILEDELTA, logfiledelta, BYTES, 4096,
         "Treat LSN differences less than this as 'close enough' for the "
         "purpose of determining if a new node is in sync.")
DEF_ATTR(LOGMEMSIZE, logmemsize, BYTES, 10485760,
         "Use this much memory for a log file in-memory buffer.")
DEF_ATTR(LOGDELETEAGE, logdeleteage, SECS, 7200, NULL)
DEF_ATTR(LOGDELETELOWFILENUM, logdeletelowfilenum, QUANTITY, -1,
         "Set the lowest deleteable log file number.")
DEF_ATTR(SYNCTRANSACTIONS, synctransactions, BOOLEAN, 0, NULL)
DEF_ATTR(CACHESIZE, cachesize, KBYTES, /* 4MB */ 4194304 / 1024, NULL)
DEF_ATTR(CACHESEGSIZE, cache_seg_size, MBYTES, 1024, NULL)
DEF_ATTR(
    NUMBERKDBCACHES, num_berkdb_caches, QUANTITY, 0,
    "Split the cache into this many segments.") /* Set non zero to override */
DEF_ATTR(CREATEDBS, createdbs, BOOLEAN, 0, NULL)
DEF_ATTR(FULLRECOVERY, fullrecovery, BOOLEAN, 0,
         "Instead of recovering from the last checkpoint, run recovery from "
         "the start of the available logs.")
DEF_ATTR(REPALWAYSWAIT, repalwayswait, BOOLEAN, 0, NULL)
DEF_ATTR(PAGESIZEDTA, pagesizedta, BYTES, 4096, NULL)
DEF_ATTR(PAGESIZEIX, pagesizeix, BYTES, 4096, NULL)
DEF_ATTR(ORDEREDRRNS, orderedrrns, BOOLEAN, 1, NULL)
DEF_ATTR(GENIDS, genids, BOOLEAN, 0, NULL)
DEF_ATTR(USEPHASE3, usephase3, BOOLEAN, 1, NULL)
DEF_ATTR(I_AM_MASTER, i_am_master, BOOLEAN, 0, NULL)
DEF_ATTR(SBUFTIMEOUT, sbuftimeout, SECS, 0, NULL)
DEF_ATTR(TEMPHASH_CACHESZ, tmphashsz, MBYTES, 5, NULL) /* XXX deficated */
DEF_ATTR(DTASTRIPE, dtastripe, QUANTITY, 0,
         "Partition each table's data into this many stripes. Note that this "
         "is ONLY settable at database creation time.")
DEF_ATTR(COMMITDELAY, commitdelay, QUANTITY, 0,
         "Add a delay after every commit. This is occasionally useful to "
         "throttle the transaction rate.")
DEF_ATTR(REPSLEEP, repsleep, QUANTITY, 0,
         "Add a delay on replicants before completing processing a log record.")
/*
 * Default numbers are based on
 * - waiting less than 10 seconds is scary
 * - bigrcv times out at 50 seconds, so don't wait that long
 * - single threaded on a 6x sparc cluster I got ~700bpms; with heavy
 *   concurrent load it could drop as low as ~100bpms; but let's be
 *   optimisitic here.
 * - fudge it by adding 5 seconds to whatever we calculate.
 */
DEF_ATTR(MINREPTIMEOUT, minreptimeoutms, MSECS, 10 * 1000,
         "Wait at least for this long for a replication event before marking a "
         "node incoherent.")
DEF_ATTR(BLOBSTRIPE, blobstripe, BOOLEAN, 0,
         "Settable ONLY at database creation time. Create stripes for every "
         "variable length field (e.g. blobs).")
/* do we do checksums on replication.  BOTH SIDES must agree on this! */
DEF_ATTR(REPCHECKSUM, repchecksum, BOOLEAN, 0,
         "Enable to do additional checksumming of replication stream (log "
         "records in replication stream already have checksums).")
DEF_ATTR(MAXLOCKERS, maxlockers, QUANTITY, 256,
         "Initial size of the lockers table (there is no current maximum).")
DEF_ATTR(MAXLOCKS, maxlocks, QUANTITY, 1024, NULL)
DEF_ATTR(MAXLOCKOBJECTS, maxlockobjects, QUANTITY, 1024, NULL)
DEF_ATTR(MAXTXN, maxtxn, QUANTITY, 128, "Maximum concurrent transactions.")
DEF_ATTR(MAXSOCKCACHED, maxsockcached, QUANTITY, 500,
         "After this many connections, start requesting that further "
         "connections are no longer pooled.")
DEF_ATTR(
    MAXAPPSOCKSLIMIT, maxappsockslimit, QUANTITY, 1400,
    "Start dropping new connections on this many connections to the database.")
DEF_ATTR(APPSOCKSLIMIT, appsockslimit, QUANTITY, 500,
         "Start warning on this many connections to the database.")
DEF_ATTR(SQLITE_SORTER_TEMPDIR_REQFREE, sqlite_sorter_tempdir_reqfree, PERCENT,
         6, "Refuse to create a sorter for queries if less than this percent "
            "of disk space is available (and return an error to the "
            "application).")
DEF_ATTR(LONGBLOCKCACHE, longblockcache, MBYTES, 1, NULL)
DEF_ATTR(DIRECTIO, directio, BOOLEAN, 1,
         "Bypass filesystem cache for page I/O.")
/* keep the cache clean (written to disk) so that blocks can be
   evicted cheaply - if there are only dirty blocks, a read will cause
   a write, as the read has no cache blocks to use, and the eviction will
   result in a write. */
DEF_ATTR(MEMPTRICKLEPERCENT, memptricklepercent, PERCENT, 99,
         "Try to keep at least this percentage of the buffer pool clean. Write "
         "pages periodically until that's achieved.")
DEF_ATTR(MEMPTRICKLEMSECS, memptricklemsecs, MSECS, 1000,
         "Pause for this many ms between runs of the cache flusher.")
DEF_ATTR(CHECKSUMS, checksums, BOOLEAN, 1,
         "Checksum data pages. Turning this off is highly discouraged.")
DEF_ATTR(LITTLE_ENDIAN_BTREES, little_endian_btrees, BOOLEAN, 1,
         "Enabling this sets byte ordering for pages to little endian.")
DEF_ATTR(COMMITDELAYMAX, commitdelaymax, QUANTITY, 8,
         "Introduce a delay after each transaction before returning control to "
         "the application. Occasionally useful to allow replicants to catch up "
         "on startup with a very busy system.")
DEF_ATTR(SCATTERKEYS, scatterkeys, BOOLEAN, 0, "")
DEF_ATTR(SNAPISOL, snapisol, BOOLEAN, 0, NULL)
DEF_ATTR(LOWDISKTHRESHOLD, lowdiskthreshold, PERCENT, 95,
         "Sets the low headroom threshold (percent of filesystem full) above "
         "which Comdb2 will start removing logs against set policy.")
DEF_ATTR(SQLBULKSZ, sqlbulksz, BYTES, 2 * 1024 * 1024,
         "For index/data scans, the database will retrieve data in bulk "
         "instead of singlestepping a cursor. This sets the buffer size for "
         "the bulk retrieval.")
DEF_ATTR(
    ZLIBLEVEL, zlib_level, QUANTITY, 6,
    "If zlib compression is enabled, this determines the compression level.")
DEF_ATTR(ZTRACE, ztrace, QUANTITY, 0, NULL)
DEF_ATTR(PANICLOGSNAP, paniclogsnap, BOOLEAN, 1, NULL)
DEF_ATTR(UPDATEGENIDS, updategenids, BOOLEAN, 0, NULL)
DEF_ATTR(ROUND_ROBIN_STRIPES, round_robin_stripes, BOOLEAN, 0,
         "Alternate to which table stripe new records are written. The default "
         "is to keep stripe affinity by writter.")
DEF_ATTR(AUTODEADLOCKDETECT, autodeadlockdetect, BOOLEAN, 1,
         "When enabled, deadlock detection will run on every lock conflict. "
         "When disabled, it'll run periodically (every DEADLOCKDETECTMS ms).")
DEF_ATTR(DEADLOCKDETECTMS, deadlockdetectms, MSECS, 100,
         "When automatic deadlock detection is disabled, run the deadlock "
         "detector this often.")
DEF_ATTR(LOGSEGMENTS, logsegments, QUANTITY, 1,
         "Changing this can create multiple logfile segments. Multiple "
         "segments can allow the log to be written while other segments are "
         "being flushed.")

#ifdef BERKDB_4_2
#define REPLIMIT_DEFAULT (256 * 1024)
#elif defined(BERKDB_4_3) || defined(BERKDB_4_5) || defined(BERKDB_46)
#define REPLIMIT_DEFAULT (1024 * 1024)
#else
/* Note: no BERKDB #defines are set in the db layer, so if that layer tries to
 * use a DEF_ATTR macro that uses default parameters, this undefined word should
 * cause a syntax error */
#define REPLIMIT_DEFAULT NOT_DEFINED_ERROR
#endif

DEF_ATTR(REPLIMIT, replimit, BYTES, REPLIMIT_DEFAULT,
         "Replication messages will be limited to this size.")

#undef REPLIMIT_DEFAULT

/* Set to true to enable queue scan mode optimisation - see queue.c
 * this optimisation appears to be very dangerous - I am disabling this
 * for now. */
DEF_ATTR(QSCANMODE, qscanmode, BOOLEAN, 0,
         "Enables queue scan mode optimisation.")
DEF_ATTR(NEWQDELMODE, newqdelmode, BOOLEAN, 1,
         "Enables new queue deletion mode.")

/* Set to true to make us take a full diagnostic on a panic.
 * This disables Berkeley's panic checks so there is a risk that other
 * threads may try to operate against the panicced environment once this
 * is done.
 */
DEF_ATTR(PANICFULLDIAG, panic_fulldiag, BOOLEAN, 0,
         "Enables full diagnostic on a panic.")
DEF_ATTR(DONT_REPORT_DEADLOCK, dont_report_deadlock, BOOLEAN, 1, NULL)
DEF_ATTR(REPTIMEOUT_LAG, rep_timeout_lag, PERCENT, 50,
         "Used in replication. Once a node has received our update, we will "
         "wait REPTIMEOUT_LAG% of the time that took for all other nodes.")
/* We set this quite high as otherwise databases just fall incoherent all the
 * time. */
DEF_ATTR(REPTIMEOUT_MINMS, rep_timeout_minms, MSECS, 10000,
         "Wait at least this many ms for replication to complete on other "
         "nodes, before marking them incoherent.")
DEF_ATTR(REPTIMEOUT_MAXMS, rep_timeout_maxms, MSECS, 5 * 60 * 1000,
         "We should wait this long for one node to acknowledge replication. If "
         "after this time we have failed to replicate anywhere then the entire "
         "cluster is incoherent!")
DEF_ATTR(REP_DEBUG_DELAY, rep_debug_delay, MSECS, 0,
         "Set an artificial replication delay (used for debugging).")

/* size of the per thread fstdump buffer.  This used to be 1MB, I'm shrinking
 * it a bit to try to reduce the number of long reads that fstdumping databases
 * seem to do. */
DEF_ATTR(FSTDUMP_BUFFER_LENGTH, fstdump_buffer_length, BYTES, 256 * 1024,
         "Size of the per-thread fstdump buffer.")
DEF_ATTR(FSTDUMP_LONGREQ, fstdump_longreq, MSECS, 5000,
         "Long request threshold for fstdump reads.")
DEF_ATTR(FSTDUMP_THREAD_STACKSZ, fstdump_thread_stacksz, BYTES, 256 * 1024,
         "Size of the fstdump thread stack.")
DEF_ATTR(FSTDUMP_MAXTHREADS, fstdump_maxthreads, QUANTITY, 0,
         "Maximum number of fstdump threads. (0 for single-threaded, 16 for "
         "maximum database thrashing)")
DEF_ATTR(REP_LONGREQ, rep_longreq, SECS, 1,
         "Warn if replication events are taking this long to process.")
DEF_ATTR(COMMITDELAYBEHINDTHRESH, commitdelaybehindthresh, BYTES, 1048576,
         "Call for election again and ask the master to delay commits if we "
         "are further than this far behind on startup.")
/* we really should just remove this option */
DEF_ATTR(NUMTIMESBEHIND, numtimesbehind, QUANTITY, 1000000000, NULL)
DEF_ATTR(TOOMANYSKIPPED, toomanyskipped, QUANTITY, 2,
         "Call for election again and delay commits if more than this many "
         "nodes are incoherent.")
DEF_ATTR(SKIPDELAYBASE, skipdelaybase, MSECS, 100, "Delay commits by at least "
                                                   "this much if forced to "
                                                   "delay by incoherent nodes.")
DEF_ATTR(REPMETHODMAXSLEEP, repmethodmaxsleep, SECS, 300,
         "Delay commits by at most this much if forced to delay by incoherent "
         "nodes.")
DEF_ATTR(TEMPTABLE_MEM_THRESHOLD, temptable_mem_threshold, QUANTITY, 512,
         "If in-memory temp tables contain more than this many entries, spill "
         "them to disk.")
DEF_ATTR(TEMPTABLE_CACHESZ, temptable_cachesz, BYTES, 262144,
         "Cache size for temporary tables. Temp tables do not share the "
         "database's main buffer pool.")
DEF_ATTR(PARTICIPANTID_BITS, participantid_bits, QUANTITY, 0,
         "Number of bits allocated for the participant stripe ID (remaining "
         "bits are used for the update ID).")
DEF_ATTR(BULK_SQL_MODE, bulk_sql_mode, BOOLEAN, 1,
         "Enable reading data in bulk when performing a scan (alternative is "
         "single-stepping a cursor).")
DEF_ATTR(BULK_SQL_ROWLOCKS, bulk_sql_rowlocks, BOOLEAN, 1, NULL)
DEF_ATTR(ROWLOCKS_PAGELOCK_OPTIMIZATION, rowlocks_pagelock_optimization,
         BOOLEAN, 1,
         "Upgrade rowlocks to pagelocks if possible on cursor traversals.")
/* TODO(Nirbhay) : Add log_region_size */
DEF_ATTR_2(
    LOGREGIONSZ, log_region_sz, QUANTITY, 1024 * 1024,
    "Size of the log region (in KB). This is used by BerkeleyDB to store "
    "information about open files and other things.",
    0, NULL, log_region_sz_update)
DEF_ATTR(ENABLECURSORSER, enable_cursor_ser, BOOLEAN, 0, NULL)
DEF_ATTR(ENABLECURSORPAUSE, enable_cursor_pause, BOOLEAN, 0, NULL)
DEF_ATTR(NONAMES, nonames, BOOLEAN, 1, "Use database name for some environment "
                                       "files (older setting, should remain "
                                       "off).")
DEF_ATTR(SHADOWS_NONBLOCKING, shadows_nonblocking, BOOLEAN, 0, NULL)
DEF_ATTR(GENIDPLUSPLUS, genidplusplus, BOOLEAN, 0, NULL)
DEF_ATTR(ELECTTIMEBASE, electtimebase, MSECS, 50,
         "Master election timeout base value.")
DEF_ATTR(DEBUGBERKDBCURSOR, dbgberkdbcursor, BOOLEAN, 0, NULL)
DEF_ATTR(BULK_SQL_THRESHOLD, bulk_sql_threshold, QUANTITY, 2, NULL)
DEF_ATTR(DEBUG_BDB_LOCK_STACK, debug_bdb_lock_stack, BOOLEAN, 0, NULL)
DEF_ATTR(LLMETA, llmeta, BOOLEAN, 1, NULL)
DEF_ATTR(SQL_OPTIMIZE_SHADOWS, sql_optimize_shadows, BOOLEAN, 0, NULL)
DEF_ATTR(CHECK_LOCKER_LOCKS, check_locker_locks, BOOLEAN, 0,
         "Sanity check locks at end of transaction.")
DEF_ATTR(DEADLOCK_MOST_WRITES, deadlock_most_writes, BOOLEAN, 0,
         "If AUTODEADLOCKDETECT is off, prefer transaction with most writes as "
         "deadlock victim.")
DEF_ATTR(DEADLOCK_WRITERS_WITH_LEAST_WRITES, deadlock_least_writes, BOOLEAN, 1,
         "If AUTODEADLOCKDETECT is off, prefer transaction with least write as "
         "deadlock victim.")
DEF_ATTR(DEADLOCK_YOUNGEST_EVER, deadlock_youngest_ever, BOOLEAN, 0,
         "If AUTODEADLOCKDETECT is off, prefer youngest transaction as "
         "deadlock victim.")
DEF_ATTR(DEADLOCK_LEAST_WRITES_EVER, deadlock_least_writes_ever, BOOLEAN, 1,
         "If AUTODEADLOCKDETECT is off, prefer transaction with least write as "
         "deadlock victim.")
DEF_ATTR(DISABLE_WRITER_PENALTY_DEADLOCK, disable_writer_penalty_deadlock,
         BOOLEAN, 0, "If set, won't shrink max #writers on deadlock.")
DEF_ATTR(CHECKPOINTRAND, checkpointrand, SECS, 30,
         "Stagger scheduled checkpoints by this random amount within this many "
         "seconds (to prevent multiple databases from checkpointing at the "
         "same time).")
DEF_ATTR(MIN_KEEP_LOGS, min_keep_logs, QUANTITY, 5, NULL)
DEF_ATTR(
    MIN_KEEP_LOGS_AGE, min_keep_logs_age, SECS, 0,
    "Keep logs that are at least this old (0: do not keep logs based on time).")
DEF_ATTR(MIN_KEEP_LOGS_AGE_HWM, min_keep_logs_age_hwm, QUANTITY, 0, NULL)
DEF_ATTR(LOG_DEBUG_CTRACE_THRESHOLD, log_debug_ctrace_threshold, QUANTITY, 20,
         "Limit trace about log file deletion to this many events.")
DEF_ATTR(GOOSE_REPLICATION_FOR_INCOHERENT_NODES,
         goose_replication_for_incoherent_nodes, BOOLEAN, 0,
         "Call for election for nodes affected by COMMITDELAYBEHINDTHRESH.")
DEF_ATTR(DISABLE_UPDATE_STRIPE_CHANGE, disable_update_stripe_change, BOOLEAN, 1,
         "Enable to move records between stripes on an update.")
DEF_ATTR(REP_SKIP_PHASE_3, rep_skip_phase_3, BOOLEAN, 0, NULL)
DEF_ATTR(PAGE_ORDER_TABLESCAN, page_order_tablescan, BOOLEAN, 1,
         "Scan tables in order of pages, not in order of rowids (faster for "
         "non-sparse tables).")
DEF_ATTR_2(
    TABLESCAN_CACHE_UTILIZATION, tablescan_cache_utilization, PERCENT, 20,
    "Attempt to keep no more than this percentage of the buffer pool for "
    "table scans.",
    0, percent_verify, 0)
DEF_ATTR(INDEX_PRIORITY_BOOST, index_priority_boost, BOOLEAN, 1,
         "Treat index pages as higher priority in the buffer pool.")
DEF_ATTR(REP_WORKERS, rep_workers, QUANTITY, 16,
         "Size of worker pool for applying page changes on behalf of "
         "transactions. (only has effect when REP_PROCESSORS is set)")
DEF_ATTR(REP_PROCESSORS, rep_processors, QUANTITY, 4,
         "Try to apply this many transactions in parallel in the replication "
         "stream.")
DEF_ATTR(REP_PROCESSORS_ROWLOCKS, rep_processors_rowlocks, QUANTITY, 0,
         "Rowlocks touches 1 file/txn; it's handled by the processor thread.")
DEF_ATTR(REP_LSN_CHAINING, rep_lsn_chaining, BOOLEAN, 0,
         "If set, will force trasnactions on replicant to always release locks "
         "in LSN order.")
DEF_ATTR(REP_MEMSIZE, rep_memsize, QUANTITY, 524288,
         "Maximum size for a local copy of log records for transaciton "
         "processors on replicants. Larger transactions will read from the log "
         "directly.")
DEF_ATTR(ELECT_DISABLE_NETSPLIT_PATCH, elect_forbid_perfect_netsplit, BOOLEAN,
         1, "When false, on a net split, the side with the master keeps it "
            "instead of downgrading. New masters still can't be elected "
            "without a quorum.")
DEF_ATTR(OSYNC, osync, BOOLEAN, 0, "Enables O_SYNC on data files (reads still "
                                   "go through FS cache) if directio isn't "
                                   "set.")
DEF_ATTR(ALLOW_OFFLINE_UPGRADES, allow_offline_upgrades, BOOLEAN, 0,
         "Allow machines marked offline to become master.")
DEF_ATTR(MAX_VLOG_LSNS, max_vlog_lsns, QUANTITY, 10000000,
         "Apply up to this many replication record trying to maintain a "
         "snapshot transaction.")
DEF_ATTR(PAGE_EXTENT_SIZE, page_extent_size, QUANTITY, 0,
         "If set, allocate pages in blocks of this many (extents).")
DEF_ATTR(DELAYED_OLDFILE_CLEANUP, delayed_oldfile_cleanup, BOOLEAN, 1,
         "If set, don't delete unused data/index files in the critical path of "
         "schema change; schedule them for deletion later.")
DEF_ATTR(DISABLE_PAGEORDER_RECSZ_CHK, disable_pageorder_recsz_chk, BOOLEAN, 0,
         "If set, allow page-order table scans even for larger record sizes "
         "where they don't necessarily lead to improvement.")
DEF_ATTR(RECOVERY_PAGES, recovery_pages, QUANTITY, 0,
         "Disabled if set to 0. Othersize, number of pages to write in "
         "addition to writing datapages. This works around corner recovery "
         "cases on questionable filesystems.")
DEF_ATTR(REP_DB_PAGESIZE, rep_db_pagesize, QUANTITY, 0,
         "Page size for BerkeleyDB's replication cache db.")
DEF_ATTR_2(
    PAGEDEADLOCK_RETRIES, pagedeadlock_retries, QUANTITY, 500,
    "On a page deadlock, retry the page operation up to this many times.",
    NOZERO, NULL, NULL)
DEF_ATTR(PAGEDEADLOCK_MAXPOLL, pagedeadlock_maxpoll, QUANTITY, 5,
         "If retrying on deadlock (see pagedeadlock_retries), poll up to this "
         "many ms on each retry.")
DEF_ATTR(ENABLE_TEMPTABLE_CLEAN_EXIT, temp_table_clean_exit, BOOLEAN, 0,
         "On exit, clean up temptables (they are deleted on next startup "
         "regardless).")
DEF_ATTR(MAX_SQL_IDLE_TIME, max_sql_idle_time, QUANTITY, 3600,
         "Warn when an SQL connection remains idle for this long.")
DEF_ATTR(SEQNUM_WAIT_INTERVAL, seqnum_wait_interval, QUANTITY, 500,
         "Wake up to check the state of the world this often while waiting for "
         "replication ACKs.")
DEF_ATTR(SOSQL_MAX_COMMIT_WAIT_SEC, sosql_max_commit_wait_sec, QUANTITY, 600,
         "Wait for the master to commit a transaction for up to this long.")
DEF_ATTR(SOSQL_POKE_TIMEOUT_SEC, sosql_poke_timeout_sec, QUANTITY, 12,
         "On replicants, when checking on master for transaction status, retry "
         "the check after this many seconds.")
DEF_ATTR(SOSQL_POKE_FREQ_SEC, sosql_poke_freq_sec, QUANTITY, 5,
         "On replicants, check this often for transaction status.")
DEF_ATTR(SQL_QUEUEING_DISABLE_TRACE, sql_queueing_disable, BOOLEAN, 0,
         "Disable trace when SQL requests are starting to queue.")
DEF_ATTR(SQL_QUEUEING_CRITICAL_TRACE, sql_queueing_critical_trace, QUANTITY,
         100, "Produce trace when SQL request queue is this deep.")
DEF_ATTR(NOMASTER_ALERT_SECONDS, nomaster_alert_seconds, QUANTITY, 60,
         "Replicants will alarm if there's no master for this many seconds.")
DEF_ATTR(PRINT_FLUSH_LOG_MSG, print_flush_log_msg, BOOLEAN, 0,
         "Produce trace when flushing log files.")
DEF_ATTR(ENABLE_INCOHERENT_DELAYMORE, enable_incoherent_delaymore, BOOLEAN, 0,
         NULL)
DEF_ATTR(MASTER_REJECT_REQUESTS, master_reject_requests, BOOLEAN, 1,
         "Master will reject SQL requests - they'll be routed to a replicant. "
         "The master can serve SQL requests, but it's better to avoid it for "
         "better workload balancing.")
DEF_ATTR(NEW_MASTER_DUMMY_ADD_DELAY, new_master_dummy_add_delay, SECS, 5,
         "Force a transaction after this delay, after becoming master.")
DEF_ATTR(TRACK_REPLICATION_TIMES, track_replication_times, BOOLEAN, 1,
         "Track how long each replicant takes to ack all transactions.")
DEF_ATTR(WARN_SLOW_REPLICANTS, warn_slow_replicants, BOOLEAN, 1,
         "Warn if any replicant's average response times over the last 10 "
         "seconds are significantly worse than the second worst replicant's.")
DEF_ATTR(MAKE_SLOW_REPLICANTS_INCOHERENT, make_slow_replicants_incoherent,
         BOOLEAN, 1, "Make slow replicants incoherent.")
DEF_ATTR(SLOWREP_INCOHERENT_FACTOR, slowrep_incoherent_factor, QUANTITY, 2,
         "Make replicants incoherent that are this many times worse than the "
         "second worst replicant. This is the threshold for "
         "WARN_SLOW_REPLICANTS and MAKE_SLOW_REPLICANTS_INCOHERENT.")
DEF_ATTR(SLOWREP_INCOHERENT_MINTIME, slowrep_incoherent_mintime, QUANTITY, 2,
         "Ignore replicantion events faster than this.")
DEF_ATTR(
    SLOWREP_INACTIVE_TIMEOUT, slowrep_inactive_timeout, SECS, 5000,
    "If a 'slow' replicant hasn't responded in this long, mark him incoherent.")
DEF_ATTR(TRACK_REPLICATION_TIMES_MAX_LSNS, track_replication_times_max_lsns,
         QUANTITY, 50,
         "Track replication times for up to this many transactions.")
DEF_ATTR(GENID_COMP_THRESHOLD, genid_comp_threshold, QUANTITY, 60,
         "Try to compress rowids if the record data is smaller than this size.")
DEF_ATTR(MASTER_REJECT_SQL_IGNORE_SANC, master_reject_sql_ignore_sanc, BOOLEAN,
         0, "If MASTER_REJECT_REQUESTS is set, reject if no other connected "
            "nodes are available.")
DEF_ATTR(KEEP_REFERENCED_FILES, keep_referenced_files, BOOLEAN, 1,
         "Don't remove any files that may still be referenced by the logs.")
DEF_ATTR(DISABLE_PGORDER_MIN_NEXTS, disable_pgorder_min_nexts, QUANTITY, 1000,
         "Don't disable page order table scans for tables less than this many "
         "pages.")
DEF_ATTR(DISABLE_PGORDER_THRESHOLD, disable_pgorder_threshold, PERCENT, 60,
         "Disable page order table scans if skipping this percentage of pages "
         "on a scan.")
DEF_ATTR(DEFAULT_ANALYZE_PERCENT, default_analyze_percent, PERCENT, 20,
         "Controls analyze coverage.")
DEF_ATTR(AUTOANALYZE, autoanalyze, BOOLEAN, 0, "Set to enable auto-analyze.")
DEF_ATTR(AA_COUNT_UPD, aa_count_upd, BOOLEAN, 0,
         "Also consider updates towards the count of operations.")
DEF_ATTR(MIN_AA_OPS, min_aa_ops, QUANTITY, 100000,
         "Start analyze after this many operations.")
DEF_ATTR(CHK_AA_TIME, chk_aa_time, SECS, 3 * 60,
         "Check whether we should start analyze this often.")
DEF_ATTR(MIN_AA_TIME, min_aa_time, SECS, 2 * 60 * 60,
         "Don't re-run auto-analyze if already ran within this many seconds.")
DEF_ATTR(AA_LLMETA_SAVE_FREQ, aa_llmeta_save_freq, QUANTITY, 1,
         "Persist change counters per table on every Nth iteration (called "
         "every CHK_AA_TIME seconds).")
DEF_ATTR(AA_MIN_PERCENT, aa_min_percent, QUANTITY, 20,
         "Percent change above which we kick off analyze.")
DEF_ATTR(AA_MIN_PERCENT_JITTER, aa_min_percent_jitter, QUANTITY, 300,
         "Additional jitter factor for determining percent change.")
DEF_ATTR(PLANNER_SHOW_SCANSTATS, planner_show_scanstats, BOOLEAN, 0, NULL)
DEF_ATTR(PLANNER_WARN_ON_DISCREPANCY, planner_warn_on_discrepancy, BOOLEAN, 0,
         NULL)
DEF_ATTR(PLANNER_EFFORT, planner_effort, QUANTITY, 1,
         "Planner effort (try harder) levels. (Default: 1)")
DEF_ATTR(SHOW_COST_IN_LONGREQ, show_cost_in_longreq, BOOLEAN, 1,
         "Show query cost in the database long requests log.")
DEF_ATTR(SC_RESTART_SEC, sc_restart_sec, QUANTITY, 0,
         "Delay restarting schema change for this many seconds after "
         "startup/new master election.")
DEF_ATTR(INDEXREBUILD_SAVE_EVERY_N, indexrebuild_save_every_n, QUANTITY, 1,
         "Save schema change state to every n-th row for index only rebuilds.")
DEF_ATTR(SC_DECREASE_THRDS_ON_DEADLOCK, sc_decrease_thrds_on_deadlock, BOOLEAN,
         1, "Decrease number of schema change threads on deadlock - way to "
            "have schema change backoff.")
DEF_ATTR(SC_CHECK_LOCKWAITS_SEC, sc_check_lockwaits_sec, QUANTITY, 1,
         "Frequency of checking lockwaits during schemachange (in seconds).")
DEF_ATTR(SC_USE_NUM_THREADS, sc_use_num_threads, QUANTITY, 0,
         "Start up to this many threads for parallel rebuilding during schema "
         "change. 0 means use one per dtastripe. Setting is capped at "
         "dtastripe.")
DEF_ATTR(SC_NO_REBUILD_THR_SLEEP, sc_no_rebuild_thr_sleep, QUANTITY, 10,
         "Sleep this many microsec when conversion threads count is at max.")
DEF_ATTR(SC_FORCE_DELAY, sc_force_delay, BOOLEAN, 0,
         "Force schemachange to delay after every record inserted - to have sc "
         "backoff.")
DEF_ATTR(SC_RESUME_AUTOCOMMIT, sc_resume_autocommit, BOOLEAN, 1,
         "Always resume autocommit schemachange if possible.")
DEF_ATTR(SC_RESUME_WATCHDOG_TIMER, sc_resume_watchdog_timer, QUANTITY, 60,
         "sc_resuming_watchdog timer")
DEF_ATTR(USE_VTAG_ONDISK_VERMAP, use_vtag_ondisk_vermap, BOOLEAN, 1,
         "Use vtag_to_ondisk_vermap conversion function from vtag_to_ondisk.")
DEF_ATTR(UDP_DROP_DELTA_THRESHOLD, udp_drop_delta_threshold, QUANTITY, 10,
         "Warn if delta of dropped packets exceeds this treshold.")
DEF_ATTR(UDP_DROP_WARN_PERCENT, udp_drop_warn_percent, PERCENT, 10,
         "Warn only if percentage of dropped packets exceeds this.")
DEF_ATTR(UDP_DROP_WARN_TIME, udp_drop_warn_time, SECS, 300,
         "Print no more than one warning per UDP_DROP_WARN_TIME seconds.")
DEF_ATTR(UDP_AVERAGE_OVER_EPOCHS, udp_average_over_epochs, QUANTITY, 4,
         "Average over these many TCP epochs.")
DEF_ATTR(RAND_UDP_FAILS, rand_udp_fails, QUANTITY, 0,
         "Rate of drop of UDP packets (for testing).")
DEF_ATTR(HOSTILE_TAKEOVER_RETRIES, hostile_takeover_retries, QUANTITY, 0,
         "Attempt to take over mastership if the master machine is marked "
         "offline, and the current machine is online.")
DEF_ATTR(MAX_ROWLOCKS_REPOSITION, max_rowlocks_reposition, QUANTITY, 10,
         "Release a physical cursor an re-establish.")
DEF_ATTR(PHYSICAL_COMMIT_INTERVAL, physical_commit_interval, QUANTITY, 512,
         "Force a physical commit after this many physical operations.")
DEF_ATTR(ROWLOCKS_MICRO_COMMIT, rowlocks_micro_commit, BOOLEAN, 1,
         "Commit on every btree operation.")
/* XXX temporary attr to help me try to reproduce a deadlock - this will always
 * be ON */
DEF_ATTR(SET_ABORT_FLAG_IN_LOCKER, set_abort_flag_in_locker, BOOLEAN, 1, NULL)
/* XXX These seem to kill performance */
DEF_ATTR(PHYSICAL_ACK_INTERVAL, physical_ack_interval, QUANTITY, 0,
         "For logical transactions, have the slave send an 'ack' after this "
         "many physical operations.")
DEF_ATTR(ACK_ON_REPLAG_THRESHOLD, ack_on_replag_threshold, QUANTITY, 0, NULL)
DEF_ATTR(UNDO_DEADLOCK_THRESHOLD, undo_deadlock_threshold, QUANTITY, 1,
         "Drop the undo interval to 1 after this many deadlocks.")
/* temporary, as an emergency switch */
DEF_ATTR(USE_RECOVERY_START_FOR_LOG_DELETION,
         use_recovery_start_for_log_deletion, BOOLEAN, 1, NULL)
DEF_ATTR(DEBUG_LOG_DELETION, debug_log_deletion, BOOLEAN, 0, NULL)
DEF_ATTR(NET_INORDER_LOGPUTS, net_inorder_logputs, BOOLEAN, 0,
         "Attempt to order messages to ensure they go out in LSN order.")
DEF_ATTR(RCACHE_COUNT, rcache_count, QUANTITY, 257,
         "Number of entries in root page cache.")
DEF_ATTR(RCACHE_PGSZ, rcache_pgsz, BYTES, 4096,
         "Size of pages in root page cache.")
DEF_ATTR(DEADLK_PRIORITY_BUMP_ON_FSTBLK, deadlk_priority_bump_on_fstblk,
         QUANTITY, 5, NULL)
DEF_ATTR(FSTBLK_MINQ, fstblk_minq, QUANTITY, 262144, NULL)
DEF_ATTR(
    DISABLE_CACHING_STMT_WITH_FDB, disable_caching_stmt_with_fdb, BOOLEAN, 1,
    "Don't cache query plans for statements with foreign table references.")
DEF_ATTR(FDB_SQLSTATS_CACHE_LOCK_WAITTIME_NSEC,
         fdb_sqlstats_cache_waittime_nsec, QUANTITY, 1000, NULL)
DEF_ATTR(PRIVATE_BLKSEQ_CACHESZ, private_blkseq_cachesz, BYTES, 4194304,
         "Cache size of the blkseq table.")
DEF_ATTR(PRIVATE_BLKSEQ_MAXAGE, private_blkseq_maxage, SECS, 600,
         "Maximum time in seconds to let 'old' transactions live.")
DEF_ATTR(PRIVATE_BLKSEQ_MAXTRAVERSE, private_blkseq_maxtraverse, QUANTITY, 4,
         NULL)
DEF_ATTR(PRIVATE_BLKSEQ_STRIPES, private_blkseq_stripes, QUANTITY, 1,
         "Number of stripes for the blkseq table.")
DEF_ATTR(PRIVATE_BLKSEQ_ENABLED, private_blkseq_enabled, BOOLEAN, 1,
         "Sets whether dupe detection is enabled.")
DEF_ATTR(PRIVATE_BLKSEQ_CLOSE_WARN_TIME, private_blkseq_close_warn_time,
         BOOLEAN, 100,
         "Warn when it takes longer than this many MS to roll a blkseq table.")
DEF_ATTR(LOG_DELETE_LOW_HEADROOM_BREAKTIME, log_delete_low_headroom_breaktime,
         QUANTITY, 10, "Try to delete logs this many times if the filesystem "
                       "is getting full before giving up.")
DEF_ATTR(ONE_PASS_DELETE, enable_one_pass_delete, BOOLEAN, 1, NULL)
DEF_ATTR(
    REMOVE_COMMITDELAY_ON_COHERENT_CLUSTER,
    remove_commitdelay_on_coherent_cluster, BOOLEAN, 1,
    "Stop delaying commits when all the nodes in the cluster are coherent.")
DEF_ATTR(DISABLE_SERVER_SOCKPOOL, disable_sockpool, BOOLEAN, 1,
         "Don't get connections to other databases from sockpool.")
DEF_ATTR(TIMEOUT_SERVER_SOCKPOOL, timeout_sockpool, SECS, 10,
         "Timeout for getting a connection to another database from sockpool.")
DEF_ATTR(COHERENCY_LEASE, coherency_lease, MSECS, 500,
         "A coherency lease grants a replicant the right to be coherent for "
         "this many ms.")
DEF_ATTR(ADDITIONAL_DEFERMS, additional_deferms, MSECS, 0,
         "Wait-fudge to ensure that a replicant has gone incoherent.")
DEF_ATTR(COHERENCY_LEASE_UDP, coherency_lease_udp, BOOLEAN, 1,
         "Use udp to issue leases.")
DEF_ATTR(LEASE_RENEW_INTERVAL, lease_renew_interval, MSECS, 200,
         "How often we renew leases.")
DEF_ATTR(DOWNGRADE_PENALTY, downgrade_penalty, MSECS, 10000,
         "Prevent upgrades for at least this many ms after a downgrade.")
DEF_ATTR(CATCHUP_WINDOW, catchup_window, BYTES, 1000000,
         "Start waiting in waitforseqnum if replicant is within this many "
         "bytes of master.")
DEF_ATTR(CATCHUP_ON_COMMIT, catchup_on_commit, BOOLEAN, 1,
         "Replicant to INCOHERENT_WAIT rather than INCOHERENT on commit if "
         "within CATCHUP_WINDOW.")
DEF_ATTR(
    ADD_RECORD_INTERVAL, add_record_interval, SECS, 1,
    "Add a record every seconds while there are incoherent_wait replicants.")
DEF_ATTR(RLLIST_STEP, rllist_step, QUANTITY, 10,
         "Reallocate rowlock lists in steps of this size.")
DEF_ATTR(GENID48_WARN_THRESHOLD, genid48_warn_threshold, QUANTITY, 500000000,
         "Print a warning when there are only as few genids remaining.")
DEF_ATTR(DISABLE_SELECTVONLY_TRAN_NOP, disable_selectvonly_tran_nop, BOOLEAN, 0,
         "Disable verifying rows selected via SELECTV if there's no other "
         "action done by the same transaction.")
DEF_ATTR(SC_VIA_DDL_ONLY, ddl_only, BOOLEAN, 0,
         "If set, we don't do checks needed for comdb2sc.")
DEF_ATTR(PAGE_COMPACT_UDP, page_compact_udp, BOOLEAN, 0,
         "Enables sending of page compact requests over UDP.")
DEF_ATTR(PAGE_COMPACT_INDEXES, page_compact_indexes, BOOLEAN, 0,
         "Enables page compaction for indexes.")
DEF_ATTR(ASOF_THREAD_POLL_INTERVAL_MS, asof_thread_poll_interval_ms, MSECS, 500,
         "For how long should the BEGIN TRANSACTION AS OF thread sleep after "
         "draining its work queue.")
DEF_ATTR(ASOF_THREAD_DRAIN_LIMIT, asof_thread_drain_limit, QUANTITY, 0,
         "How many entries at maximum should the BEGIN TRANSACTION AS OF "
         "thread drain per run.")
DEF_ATTR(REP_VERIFY_MAX_TIME, rep_verify_max_time, SECS, 300,
         "Maximum amount of time we allow a replicant to roll back its logs in "
         "an attempt to sync up to the master.")
DEF_ATTR(REP_VERIFY_MIN_PROGRESS, rep_verify_min_progress, BYTES, 10485760,
         "Abort replicant if it doesn't make this much progress while rolling "
         "back logs to sync up to master.")
DEF_ATTR(REP_VERIFY_LIMIT_ENABLED, rep_verify_limit_enabled, BOOLEAN, 1,
         "Enable aborting replicant if it doesn't make sufficient progress "
         "while rolling back logs to sync up to master.")
DEF_ATTR(TIMEPART_ABORT_ON_PREPERROR, timepart_abort_on_preperror, BOOLEAN, 0,
         NULL)
DEF_ATTR(REPORT_DECIMAL_CONVERSION, report_decimal_conversion, BOOLEAN, 0, NULL)
DEF_ATTR(TIMEPART_CHECK_SHARD_EXISTENCE, timepart_check_shard_existence,
         BOOLEAN, 0,
         "Check at startup/time-partition creation that all shard files exist.")
/* Keep enabled for the merge */
DEF_ATTR(DURABLE_LSNS, durable_lsns, BOOLEAN, 0, NULL)
DEF_ATTR(DURABLE_LSN_POLL_INTERVAL_MS, durable_lsn_poll_interval_ms, MSECS, 200,
         NULL)
/* Keep disabled:  we get it when we add to the trn_repo */
DEF_ATTR(RETRIEVE_DURABLE_LSN_AT_BEGIN, retrieve_durable_lsn_at_begin, BOOLEAN,
         0, NULL)
DEF_ATTR(
    DURABLE_MAXWAIT_MS, durable_maxwait_ms, MSECS, 4000,
    "Maximum time a replicant will spend waiting for an LSN to become durable.")
DEF_ATTR(DOWNGRADE_ON_SEQNUM_GEN_MISMATCH, downgrade_on_seqnum_gen_mismatch,
         BOOLEAN, 1, NULL)
DEF_ATTR(ENABLE_SEQNUM_GENERATIONS, enable_seqnum_generations, BOOLEAN, 1, NULL)
DEF_ATTR(ELECT_ON_MISMATCHED_MASTER, elect_on_mismatched_master, BOOLEAN, 1,
         NULL)
DEF_ATTR(SET_REPINFO_MASTER_TRACE, set_repinfo_master_trace, BOOLEAN, 0, NULL)
DEF_ATTR(LEASEBASE_TRACE, leasebase_trace, BOOLEAN, 0, NULL)
DEF_ATTR(MASTER_LEASE, master_lease, MSECS, 500, NULL)
DEF_ATTR(MASTER_LEASE_RENEW_INTERVAL, master_lease_renew_interval, MSECS, 200,
         NULL)
DEF_ATTR(VERBOSE_DURABLE_BLOCK_TRACE, verbose_durable_block_trace, BOOLEAN, 0,
         NULL)
DEF_ATTR(LOGDELETE_RUN_INTERVAL, logdelete_run_interval, SECS, 30, NULL)
DEF_ATTR(REQUEST_DURABLE_LSN_FROM_MASTER, request_durable_lsn_from_master,
         BOOLEAN, 1, NULL)
DEF_ATTR(DURABLE_LSN_REQUEST_WAITMS, durable_lsn_request_waitms, MSECS, 1000,
         NULL)
DEF_ATTR(VERIFY_MASTER_LEASE_TRACE, verify_master_lease_trace, BOOLEAN, 0, NULL)
DEF_ATTR(RECEIVE_COHERENCY_LEASE_TRACE, receive_coherency_lease_trace, BOOLEAN,
         0, NULL)
DEF_ATTR(REQUEST_DURABLE_LSN_TRACE, request_durable_lsn_trace, BOOLEAN, 0, NULL)
DEF_ATTR(MASTER_LEASE_SET_TRACE, master_lease_set_trace, BOOLEAN, 0, NULL)
DEF_ATTR(RECEIVE_START_LSN_REQUEST_TRACE, receive_start_lsn_request_trace,
         BOOLEAN, 0, NULL)
DEF_ATTR(WAIT_FOR_SEQNUM_TRACE, wait_for_seqnum_trace, BOOLEAN, 0, NULL)
DEF_ATTR(STARTUP_SYNC_ATTEMPTS, startup_sync_attempts, QUANTITY, 5, NULL)
DEF_ATTR(DEBUG_TIMEPART_CRON, dbg_timepart_cron, BOOLEAN, 0, NULL)
DEF_ATTR(DEBUG_TIMEPART_SQLITE, dbg_timepart_SQLITE, BOOLEAN, 0, NULL)
DEF_ATTR(DELAY_LOCK_TABLE_RECORD_C, delay_lock_table_record_c, MSECS, 0, NULL)

/*
  BDB_ATTR_REPTIMEOUT
     amount of time to wait for acks.  when the time is exceeded,
     the transaction will proceed without the cooperation of the
     faulting node.  the routine registered in
     bdb_register_reptimeout_rtn will be called.  the application
     should take whatever action is needed to maintain cache
     coherency requirements.
  BDB_ATTR_CHECKPOINTTIME
     number of seconds between checkpoints.  checkpoints are needed
     to apply records from the log files to the database files.  log
     files can only be removed once they have been applied to the
     database files.  checkpoints occur in a separate thread.  i
     currently do not know the real performance impact of running a
     checkpoint during database update activity.  this is also the
     timer used for deletion of log files.
  BDB_ATTR_LOGFILESIZE
     size in bytes of a log file.  log files are extented until they
     reach BDB_ATTR_LOGFILESIZE, then a new log file is created
     with an extension of .N+1 if the previous log file was .N
  BDB_ATTR_LOGMEMSIZE
     this buffer is used when BDB_ATTR_SYNCTRANSACTIONS is set to 0.
     commits will not cause the log file to be flushed to stable
     storage.  the log file will be flushed only when this memory
     buffer fills.  when BDB_ATTR_SYNCTRANSACTIONS is set to 1, this
     buffer is used as temorary storage between the bdb_tran_begin
     phase and the bdb_tran_commit phase.
  BDB_ATTR_LOGDELETEAGE
     log files that are both no longer needed (no longer needed means
     that all data in the log file has been succesfully applied to
     the local database file.  the definition of no longer needed
     is not currently extended to know anything about replication)
     and are older than BDB_ATTR_LOGDELETEAGE seconds will be
     deleted.  the deletion attempt happens on the checkpoint
     interval, immediately following a checkpoint.  two special
     values exist.  LOGDELETEAGE_NEVER means never delete log files.
     LOGDELETEAGE_NOW means immediately remove any unneeded log file.
     the full range of positive integers >=0 are avaiable to the
     application.
  BDB_ATTR_SYNCTRANSACTIONS
     when this attribute is set to 1, a call to bdb_tran_commit()
     will cause the log files to be syncronously flushed before
     control returns to the application.  when this attribute is
     set to 0, control will return to the application as soon as
     the transaction is reflected in cache.
  BDB_ATTR_CACHESIZE
     the size in kbytes of the cache to be used for all databases
     associated with the bdb_handle.  this cache is shared.
  BDB_ATTR_CREATEDBS
     when this attribute is set to 1, the bdb library will create
     and initialize any on disk structures as needed.  when this
     attribute is set to 0, the bdb library will make no such
     attempt.
  BDB_ATTR_FULLRECOVERY
     if this attribute is set to 1, we run full recovery in bdb_open.
     this is NECESSARY the first time a new site is brought into
     a replication group via a live copy of the current database.
     this should not be needed in other cases (perhaps necessary
     as an integrity check)
  BDB_ATTR_RRNCACHESIZE
     this is the number of rrn records from the freerec db that stay in
     memory for fast rrn allocation
  BDB_ATTR_RRNEXTENDAMOUNT
     we add this many records in one batch transaction to the rrn freerec
     db when the freerec is empty
  BDB_ATTR_REPALWAYSWAIT
     when this is set, bdb_wait_for_seqnum_from_all() and
     bdb_wait_for_seqnum_from_node() will wait for nodes that do not have
     an working comm at the moment of the call.  the wait can still succeed
     if the node comes re-estabilishes comm.
  BDB_ATTR_PAGESIZEDTA
     the pagesize of the .dta file
  BDB_ATTR_PAGESIZEFREEREC
     the pagesize of the .freerec file
  BDB_ATTR_PAGESIZEIX
     the pagesize of the .ix[0-n] files
  BDB_ATTR_RRNCACHELOWAT
     try to keep this many rrns in the rrn cache.
  BDB_ATTR_ORDEREDRRNS
     keep rrn allocation sequentially ordered whenever possible
  BDB_ATTR_GENIDS
     do we internally maintain an 8 byte generation id associated with each
     record.
  BDB_ATTR_DELRQUIRESGENIDS
     if we are running in genid mode, do we allow access from clients that
     dont understand genids for DELETE requests.
  BDB_ATTR_UPDATEREQUIRESGENIDS
     if we are running in genid mode, do we allow access from clients that
     dont understand genids for UPDATE requests.
  BDB_ATTR_I_AM_MASTER
     become master on startup, do not elect.
  BDB_ATTR_FREERECQUEUE
     use alternate queue implementation of freerec.  can only be turned on
     if all nodes in the cluster support this.
  BDB_ATTR_REPFAILCHK
     controls replicant check on failure feature:
        OFF: feature is ENABLED
        ON:  feature is DISABLED
  BDB_ATTR_REPFAILCHKTIME
     time frame in seconds where replicant timeout failures are counted
     once time frame is exhausted then count is reset to 0
  BDB_ATTR_REPFAILCHKTHRESH
     # of 'allowed' replicant timeout during a BDB_ATTR_REPFAILCHKTIME
     before we go have a look at what is happening for replicant.
  BDB_ATTR_PANICLOGSNAP
     controls snapshot of last log on panic feature
        OFF: feature is ENABLED
        ON:  feature is DISABLED
  BDB_ATTR_ENABLECURSORSER
     if true bdb_fetch_int() will attempt to use cursor serialization
  BDB_ATTR_ENABLECURSORPAUSE
     if true bdb_cursor will attempt to use cursor pausing

  BDB_ATTR_SOSQL_MAX_COMMIT_WAIT_SEC
     maximum number of seconds we wait for a transaction to commit
     since the last message (OSQL_DONE) was sent to the master

  BDB_ATTR_SOSQL_POKE_TIMEOUT_SEC
     maximum number of seconds we wait since the last confirmed checked
     before we fail the transaction and return SQLHERR_MASTER_TIMEOUT to
     client

  BDB_ATTR_SOSQL_POKE_EVERY_SEC
     minimum number of seconds we wait since last poke before we sent
     one again to the master

  BDB_ATTR_SQL_QUEUEING_DISABLE_TRACE
     disables the Queuing sql trace, which detects sql overload cases

  BDB_ATTR_SQL_QUEUEING_CRITICAL_TRACE
     maximum number of sql queries we can queue without alerting when
     SQL_QUEUEING_DISABLE_TRACE is on

  MASTER_REJECT_SQL_IGNORE_SANC
     normally master rejects sql if there are coherent(connected) nodes that
     are also in the sanc list.
     enabling this check only if there are coherent (connected) nodes before
     rejecting - this breaks with proxies using sanc list as cluster view
*/
