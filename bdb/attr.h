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

/* This file defines the bdb attributes.  It is included multiple times in some
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
 */

/* If we reorder or delete any of thse you must do a full rebuild of bdb
 * and of db as the BDB_ATTR_ constants will have changed. */
DEF_ATTR(REPTIMEOUT, reptimeout, SECS, 20)
DEF_ATTR(CHECKPOINTTIME, checkpointtime, SECS, 60)
DEF_ATTR(CHECKPOINTTIMEPOLL, checkpointtimepoll, MSECS, 100)
DEF_ATTR(LOGFILESIZE, logfilesize, BYTES, 41943040)
DEF_ATTR(LOGFILEDELTA, logfiledelta, BYTES, 4096)
DEF_ATTR(LOGMEMSIZE, logmemsize, BYTES, 10485760)
DEF_ATTR(LOGDELETEAGE, logdeleteage, SECS, 7200)
DEF_ATTR(LOGDELETELOWFILENUM, logdeletelowfilenum, QUANTITY, -1)
DEF_ATTR(SYNCTRANSACTIONS, synctransactions, BOOLEAN, 0)

DEF_ATTR(CACHESIZE, cachesize, KBYTES, /* 4MB */ 4194304 / 1024)
DEF_ATTR(CACHESEGSIZE, cache_seg_size, MBYTES, 1024)
DEF_ATTR(NUMBERKDBCACHES, num_berkdb_caches, QUANTITY,
         0) /* set non zero to override */

DEF_ATTR(CREATEDBS, createdbs, BOOLEAN, 0)
DEF_ATTR(FULLRECOVERY, fullrecovery, BOOLEAN, 0)
DEF_ATTR(REPALWAYSWAIT, repalwayswait, BOOLEAN, 0)
DEF_ATTR(PAGESIZEDTA, pagesizedta, BYTES, 4096)
DEF_ATTR(PAGESIZEIX, pagesizeix, BYTES, 4096)
DEF_ATTR(ORDEREDRRNS, orderedrrns, BOOLEAN, 1)
DEF_ATTR(GENIDS, genids, BOOLEAN, 0)
DEF_ATTR(USEPHASE3, usephase3, BOOLEAN, 1)
DEF_ATTR(I_AM_MASTER, i_am_master, BOOLEAN, 0)
DEF_ATTR(SBUFTIMEOUT, sbuftimeout, SECS, 0)
DEF_ATTR(TEMPHASH_CACHESZ, tmphashsz, MBYTES, 5) /* XXX deficated */
DEF_ATTR(DTASTRIPE, dtastripe, QUANTITY, 0)
DEF_ATTR(COMMITDELAY, commitdelay, QUANTITY, 0)
DEF_ATTR(REPSLEEP, repsleep, QUANTITY, 0)
/*
 * Default numbers are based on
 * - waiting less than 10 seconds is scary
 * - bigrcv times out at 50 seconds, so don't wait that long
 * - single threaded on a 6x sparc cluster I got ~700bpms; with heavy
 *   concurrent load it could drop as low as ~100bpms; but let's be
 *   optimisitic here.
 * - fudge it by adding 5 seconds to whatever we calculate.
 */
DEF_ATTR(MINREPTIMEOUT, minreptimeoutms, MSECS, 10 * 1000)
DEF_ATTR(BLOBSTRIPE, blobstripe, BOOLEAN, 0)
/* do we do checksums on replication.  BOTH SIDES must agree on this! */
DEF_ATTR(REPCHECKSUM, repchecksum, BOOLEAN, 0)
/* 512 people locking things.  this seems like a lot.  except that making
 * this number 512 stops lots of databases from starting up, sjtestdb being
 * one example (there were others). */
DEF_ATTR(MAXLOCKERS, maxlockers, QUANTITY, 256)
DEF_ATTR(MAXLOCKS, maxlocks, QUANTITY, 1024)
DEF_ATTR(MAXLOCKOBJECTS, maxlockobjects, QUANTITY, 1024)
DEF_ATTR(MAXTXN, maxtxn, QUANTITY, 128)
DEF_ATTR(MAXSOCKCACHED, maxsockcached, QUANTITY, 500)
DEF_ATTR(MAXAPPSOCKSLIMIT, maxappsockslimit, QUANTITY, 1400)
DEF_ATTR(APPSOCKSLIMIT, appsockslimit, QUANTITY, 500)
DEF_ATTR(SQLITE_SORTER_TEMPDIR_REQFREE, sqlite_sorter_tempdir_reqfree, MBYTES, 6)
DEF_ATTR(LONGBLOCKCACHE, longblockcache, MBYTES, 1)
DEF_ATTR(DIRECTIO, directio, BOOLEAN, 1)
/* keep the cache clean (written to disk) so that blocks can be
   evicted cheaply - if there are only dirty blocks, a read will cause
   a write, as the read has no cache blocks to use, and the eviction will
   result in a write. */
DEF_ATTR(MEMPTRICKLEPERCENT, memptricklepercent, PERCENT, 99)
DEF_ATTR(MEMPTRICKLEMSECS, memptricklemsecs, MSECS, 1000)
DEF_ATTR(CHECKSUMS, checksums, BOOLEAN, 1)
DEF_ATTR(LITTLE_ENDIAN_BTREES, little_endian_btrees, BOOLEAN, 1)
DEF_ATTR(COMMITDELAYMAX, commitdelaymax, QUANTITY, 8)
DEF_ATTR(SCATTERKEYS, scatterkeys, BOOLEAN, 0)
DEF_ATTR(SNAPISOL, snapisol, BOOLEAN, 0)
/* if disk utilisation goes above this then disk space is low */
DEF_ATTR(LOWDISKTHRESHOLD, lowdiskthreshold, PERCENT, 95)
DEF_ATTR(SQLBULKSZ, sqlbulksz, BYTES, 2 * 1024 * 1024)
DEF_ATTR(ZLIBLEVEL, zlib_level, QUANTITY, 6)
DEF_ATTR(ZTRACE, ztrace, QUANTITY, 0)
DEF_ATTR(PANICLOGSNAP, paniclogsnap, BOOLEAN, 1)
DEF_ATTR(UPDATEGENIDS, updategenids, BOOLEAN, 0)
DEF_ATTR(ROUND_ROBIN_STRIPES, round_robin_stripes, BOOLEAN, 0)

DEF_ATTR(AUTODEADLOCKDETECT, autodeadlockdetect, BOOLEAN, 1)
DEF_ATTR(DEADLOCKDETECTMS, deadlockdetectms, MSECS, 100)
DEF_ATTR(LOGSEGMENTS, logsegments, QUANTITY, 1)

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

DEF_ATTR(REPLIMIT, replimit, BYTES, REPLIMIT_DEFAULT)

#undef REPLIMIT_DEFAULT

/* set to true to enable queue scan mode optimisation - see queue.c
 * this optimisation appears to be very dangerous - I am disabling this
 * for now */
DEF_ATTR(QSCANMODE, qscanmode, BOOLEAN, 0)

/* set to true to enable new queue deletion mode */
DEF_ATTR(NEWQDELMODE, newqdelmode, BOOLEAN, 1)

/* Set to true to make us take a full diagnostic on a panic.
 * This disables Berkeley's panic checks so there is a risk that other
 * threads may try to operate against the panicced environment once this
 * is done.
 */
DEF_ATTR(PANICFULLDIAG, panic_fulldiag, BOOLEAN, 0)
DEF_ATTR(DONT_REPORT_DEADLOCK, dont_report_deadlock, BOOLEAN, 1)

/* used in replication.  once node node has received our update, we will wait
 * TIMEOUT_LAG% of the time that took for all other nodes. */
DEF_ATTR(REPTIMEOUT_LAG, rep_timeout_lag, PERCENT, 50)

/* even if the first node comes back super super fast, wait at least this many
 * ms to replicate to other nodes.  We set this quite high as otherwise
 * databases just fall incoherent all the time. */
DEF_ATTR(REPTIMEOUT_MINMS, rep_timeout_minms, MSECS, 10000)

/* we should wait this long for one node to acknowledge replication.
 * if after this time we have failed to replicate anywhere then the entire
 * cluster is incoherent! */
DEF_ATTR(REPTIMEOUT_MAXMS, rep_timeout_maxms, MSECS, 5 * 60 * 1000)

/* for debugging - set an artificial replication delay */
DEF_ATTR(REP_DEBUG_DELAY, rep_debug_delay, MSECS, 0)

/* size of the per thread fstdump buffer.  This used to be 1MB, I'm shrinking
 * it a bit to try to reduce the number of long reads that fstdumping databases
 * seem to do. */
DEF_ATTR(FSTDUMP_BUFFER_LENGTH, fstdump_buffer_length, BYTES, 256 * 1024)

/* long request threshold for fstdump reads */
DEF_ATTR(FSTDUMP_LONGREQ, fstdump_longreq, MSECS, 5000)

/* fstdump thread stack size (doesn't need to be 1MB, the old hardcoded
 * setting.  Also limit the number of threads we use for fstdumping.  Set this
 * to 0 to be single threaded, or 16 for maximum database thrashing. */
DEF_ATTR(FSTDUMP_THREAD_STACKSZ, fstdump_thread_stacksz, BYTES, 256 * 1024)
DEF_ATTR(FSTDUMP_MAXTHREADS, fstdump_maxthreads, QUANTITY, 0)

/* Long request time for relication */
DEF_ATTR(REP_LONGREQ, rep_longreq, SECS, 1)

DEF_ATTR(COMMITDELAYBEHINDTHRESH, commitdelaybehindthresh, BYTES, 1048576)

/* we really should just remove this option */
DEF_ATTR(NUMTIMESBEHIND, numtimesbehind, QUANTITY, 1000000000)

DEF_ATTR(TOOMANYSKIPPED, toomanyskipped, QUANTITY, 2)

DEF_ATTR(SKIPDELAYBASE, skipdelaybase, MSECS, 100)

DEF_ATTR(REPMETHODMAXSLEEP, repmethodmaxsleep, SECS, 300)

DEF_ATTR(TEMPTABLE_MEM_THRESHOLD, temptable_mem_threshold, QUANTITY, 512)

DEF_ATTR(TEMPTABLE_CACHESZ, temptable_cachesz, BYTES, 262144)

/* the number of bits allocated for the participant stripe id.  The remaining
 * bits are used for the update id */
DEF_ATTR(PARTICIPANTID_BITS, participantid_bits, QUANTITY, 0)

DEF_ATTR(BULK_SQL_MODE, bulk_sql_mode, BOOLEAN, 1)

DEF_ATTR(BULK_SQL_ROWLOCKS, bulk_sql_rowlocks, BOOLEAN, 1)

DEF_ATTR(ROWLOCKS_PAGELOCK_OPTIMIZATION, rowlocks_pagelock_optimization,
         BOOLEAN, 1)

DEF_ATTR(LOGREGIONSZ, log_region_sz, QUANTITY, 1024*1024)

DEF_ATTR(ENABLECURSORSER, enable_cursor_ser, BOOLEAN, 0)

DEF_ATTR(ENABLECURSORPAUSE, enable_cursor_pause, BOOLEAN, 0)

DEF_ATTR(NONAMES, nonames, BOOLEAN, 1)

DEF_ATTR(SHADOWS_NONBLOCKING, shadows_nonblocking, BOOLEAN, 0)

DEF_ATTR(GENIDPLUSPLUS, genidplusplus, BOOLEAN, 0)

DEF_ATTR(ELECTTIMEBASE, electtimebase, MSECS, 50)

DEF_ATTR(DEBUGBERKDBCURSOR, dbgberkdbcursor, BOOLEAN, 0)

DEF_ATTR(BULK_SQL_THRESHOLD, bulk_sql_threshold, QUANTITY, 2)

DEF_ATTR(DEBUG_BDB_LOCK_STACK, debug_bdb_lock_stack, BOOLEAN, 0)

DEF_ATTR(LLMETA, llmeta, BOOLEAN, 1)

DEF_ATTR(SQL_QUERY_IGNORE_NEWER_UPDATES, sql_query_ignore_newer_updates,
         BOOLEAN, 0)

DEF_ATTR(SQL_OPTIMIZE_SHADOWS, sql_optimize_shadows, BOOLEAN, 0)

DEF_ATTR(CHECK_LOCKER_LOCKS, check_locker_locks, BOOLEAN, 0)

DEF_ATTR(DEADLOCK_MOST_WRITES, deadlock_most_writes, BOOLEAN, 0)
DEF_ATTR(DEADLOCK_WRITERS_WITH_LEAST_WRITES, deadlock_least_writes, BOOLEAN, 1)
DEF_ATTR(DEADLOCK_YOUNGEST_EVER, deadlock_youngest_ever, BOOLEAN, 0)
DEF_ATTR(DEADLOCK_LEAST_WRITES_EVER, deadlock_least_writes_ever, BOOLEAN, 1)
DEF_ATTR(DISABLE_WRITER_PENALTY_DEADLOCK, disable_writer_penalty_deadlock,
         BOOLEAN, 0)

DEF_ATTR(CHECKPOINTRAND, checkpointrand, SECS, 30)

DEF_ATTR(MIN_KEEP_LOGS, min_keep_logs, QUANTITY, 5)
DEF_ATTR(MIN_KEEP_LOGS_AGE, min_keep_logs_age, SECS, 0)
DEF_ATTR(MIN_KEEP_LOGS_AGE_HWM, min_keep_logs_age_hwm, QUANTITY, 0)
DEF_ATTR(LOG_DEBUG_CTRACE_THRESHOLD, log_debug_ctrace_threshold, QUANTITY, 20)

DEF_ATTR(GOOSE_REPLICATION_FOR_INCOHERENT_NODES,
         goose_replication_for_incoherent_nodes, BOOLEAN, 0)

DEF_ATTR(DISABLE_UPDATE_STRIPE_CHANGE, disable_update_stripe_change, BOOLEAN, 1)

DEF_ATTR(REP_SKIP_PHASE_3, rep_skip_phase_3, BOOLEAN, 0)

DEF_ATTR(PAGE_ORDER_TABLESCAN, page_order_tablescan, BOOLEAN, 1)

DEF_ATTR(TABLESCAN_CACHE_UTILIZATION, tablescan_cache_utilization, PERCENT, 20)

DEF_ATTR(INDEX_PRIORITY_BOOST, index_priority_boost, BOOLEAN, 1)

DEF_ATTR(REP_WORKERS, rep_workers, QUANTITY, 16)
DEF_ATTR(REP_PROCESSORS, rep_processors, QUANTITY, 4)

/* Rowlocks touches 1 file / txn - it's handled by the processor thread */
DEF_ATTR(REP_PROCESSORS_ROWLOCKS, rep_processors_rowlocks, QUANTITY, 0)

DEF_ATTR(REP_LSN_CHAINING, rep_lsn_chaining, BOOLEAN, 0)

DEF_ATTR(REP_MEMSIZE, rep_memsize, QUANTITY, 524288)

DEF_ATTR(ELECT_DISABLE_NETSPLIT_PATCH, elect_forbid_perfect_netsplit, BOOLEAN, 1)

DEF_ATTR(OSYNC, osync, BOOLEAN, 0)

DEF_ATTR(ALLOW_OFFLINE_UPGRADES, allow_offline_upgrades, BOOLEAN, 0)

DEF_ATTR(MAX_VLOG_LSNS, max_vlog_lsns, QUANTITY, 10000000)
DEF_ATTR(PAGE_EXTENT_SIZE, page_extent_size, QUANTITY, 0)
DEF_ATTR(DELAYED_OLDFILE_CLEANUP, delayed_oldfile_cleanup, BOOLEAN, 1)
DEF_ATTR(DISABLE_PAGEORDER_RECSZ_CHK, disable_pageorder_recsz_chk, BOOLEAN, 0)
DEF_ATTR(RECOVERY_PAGES, recovery_pages, QUANTITY, 0)
DEF_ATTR(REP_DB_PAGESIZE, rep_db_pagesize, QUANTITY, 0)
DEF_ATTR(PAGEDEADLOCK_RETRIES, pagedeadlock_retries, QUANTITY, 500)
DEF_ATTR(PAGEDEADLOCK_MAXPOLL, pagedeadlock_maxpoll, QUANTITY, 5)

DEF_ATTR(ENABLE_TEMPTABLE_CLEAN_EXIT, temp_table_clean_exit, BOOLEAN, 0)

DEF_ATTR(MAX_SQL_IDLE_TIME, max_sql_idle_time, QUANTITY, 3600)

DEF_ATTR(SEQNUM_WAIT_INTERVAL, seqnum_wait_interval, QUANTITY, 500)

DEF_ATTR(SOSQL_MAX_COMMIT_WAIT_SEC, sosql_max_commit_wait_sec, QUANTITY, 600)
DEF_ATTR(SOSQL_POKE_TIMEOUT_SEC, sosql_poke_timeout_sec, QUANTITY, 12)
DEF_ATTR(SOSQL_POKE_FREQ_SEC, sosql_poke_freq_sec, QUANTITY, 5)

DEF_ATTR(SQL_QUEUEING_DISABLE_TRACE, sql_queueing_disable, BOOLEAN, 0)
DEF_ATTR(SQL_QUEUEING_CRITICAL_TRACE, sql_queueing_critical_trace, QUANTITY,
         100)

DEF_ATTR(NOMASTER_ALERT_SECONDS, nomaster_alert_seconds, QUANTITY, 60)

DEF_ATTR(PRINT_FLUSH_LOG_MSG, print_flush_log_msg, BOOLEAN, 0)

DEF_ATTR(ENABLE_INCOHERENT_DELAYMORE, enable_incoherent_delaymore, BOOLEAN, 0)

DEF_ATTR(MASTER_REJECT_REQUESTS, master_reject_requests, BOOLEAN, 1)

DEF_ATTR(NEW_MASTER_DUMMY_ADD_DELAY, new_master_dummy_add_delay, SECS, 5)
DEF_ATTR(TRACK_REPLICATION_TIMES, track_replication_times, BOOLEAN, 1)
DEF_ATTR(WARN_SLOW_REPLICANTS, warn_slow_replicants, BOOLEAN, 1)
DEF_ATTR(MAKE_SLOW_REPLICANTS_INCOHERENT, make_slow_replicants_incoherent,
         BOOLEAN, 1)
DEF_ATTR(SLOWREP_INCOHERENT_FACTOR, slowrep_incoherent_factor, QUANTITY, 2)
DEF_ATTR(SLOWREP_INCOHERENT_MINTIME, slowrep_incoherent_mintime, QUANTITY, 2)
DEF_ATTR(SLOWREP_INACTIVE_TIMEOUT, slowrep_inactive_timeout, SECS, 5000)
DEF_ATTR(TRACK_REPLICATION_TIMES_MAX_LSNS, track_replication_times_max_lsns,
         QUANTITY, 50)

DEF_ATTR(GENID_COMP_THRESHOLD, genid_comp_threshold, QUANTITY, 60)

DEF_ATTR(MASTER_REJECT_SQL_IGNORE_SANC, master_reject_sql_ignore_sanc, BOOLEAN,
         0)

DEF_ATTR(KEEP_REFERENCED_FILES, keep_referenced_files, BOOLEAN, 1)
DEF_ATTR(DISABLE_PGORDER_MIN_NEXTS, disable_pgorder_min_nexts, QUANTITY, 1000)
DEF_ATTR(DISABLE_PGORDER_THRESHOLD, disable_pgorder_threshold, PERCENT, 60)

/* AA: AUTO ANALYZE */
DEF_ATTR(AUTOANALYZE, autoanalyze, BOOLEAN, 0)
DEF_ATTR(AA_COUNT_UPD, aa_count_upd, BOOLEAN, 0)
DEF_ATTR(MIN_AA_OPS, min_aa_ops, QUANTITY, 100000) // threshold for auto analyze
DEF_ATTR(CHK_AA_TIME, chk_aa_time, SECS, 3 * 60)   // check stats every 3 min
DEF_ATTR(MIN_AA_TIME, min_aa_time, SECS, 2 * 60 * 60) // don't rerun within 2hrs
DEF_ATTR(AA_LLMETA_SAVE_FREQ, aa_llmeta_save_freq, QUANTITY,
         1) // write llmeta every N saves
DEF_ATTR(AA_MIN_PERCENT, aa_min_percent, QUANTITY,
         20) // min percent change to trigger analyze
DEF_ATTR(AA_MIN_PERCENT_JITTER, aa_min_percent_jitter, QUANTITY,
         300) // min operations used in conjunction with percent change to
              // trigger analyze

DEF_ATTR(PLANNER_SHOW_SCANSTATS, planner_show_scanstats, BOOLEAN, 0)
DEF_ATTR(PLANNER_WARN_ON_DISCREPANCY, planner_warn_on_discrepancy, BOOLEAN, 0)
/* Planner effort (try harder) levels, default is 1*/
DEF_ATTR(PLANNER_EFFORT, planner_effort, QUANTITY, 1)
DEF_ATTR(SHOW_COST_IN_LONGREQ, show_cost_in_longreq, BOOLEAN, 1)
/* Delay restarting SC by this many seconds from db up*/
DEF_ATTR(SC_RESTART_SEC, sc_restart_sec, QUANTITY, 60)
/* Save to llmeta every n-th genid for index only rebuilds */
DEF_ATTR(INDEXREBUILD_SAVE_EVERY_N, indexrebuild_save_every_n, QUANTITY, 1)
/* decrease number of SC threads on DEADLOCK -- way to have sc backoff */
DEF_ATTR(SC_DECREASE_THRDS_ON_DEADLOCK, sc_decrease_thrds_on_deadlock, BOOLEAN,
         1)
/* Frequency of checking lockwaits during schemachange in seconds */
DEF_ATTR(SC_CHECK_LOCKWAITS_SEC, sc_check_lockwaits_sec, QUANTITY, 1)
/* Start up to NUM threads for parallel rebuilding -- means gbl_dtastripe */
DEF_ATTR(SC_USE_NUM_THREADS, sc_use_num_threads, QUANTITY, 0)
/* Sleep this many microsec when conversion threads count is at max */
DEF_ATTR(SC_NO_REBUILD_THR_SLEEP, sc_no_rebuild_thr_sleep, QUANTITY, 10)
/* force delay schemachange after every record inserted -- to have sc backoff */
DEF_ATTR(SC_FORCE_DELAY, sc_force_delay, BOOLEAN, 0)

/* use vtag_to_ondisk_vermap conversion function from vtag_to_ondisk */
DEF_ATTR(USE_VTAG_ONDISK_VERMAP, use_vtag_ondisk_vermap, BOOLEAN, 1)

/* warn only if delta of dropped packets exceeds this */
DEF_ATTR(UDP_DROP_DELTA_THRESHOLD, udp_drop_delta_threshold, QUANTITY, 10)
/* warn only if percentage of dropped packets exceeds this */
DEF_ATTR(UDP_DROP_WARN_PERCENT, udp_drop_warn_percent, PERCENT, 10)
/* no more than one warning per UDP_DROP_WARN_TIME */
DEF_ATTR(UDP_DROP_WARN_TIME, udp_drop_warn_time, SECS, 300)
/* average over these many tcp epochs */
DEF_ATTR(UDP_AVERAGE_OVER_EPOCHS, udp_average_over_epochs, QUANTITY, 4)
/* for testing, rate of drop of udp packets */
DEF_ATTR(RAND_UDP_FAILS, rand_udp_fails, QUANTITY, 0)

DEF_ATTR(HOSTILE_TAKEOVER_RETRIES, hostile_takeover_retries, QUANTITY, 0)
DEF_ATTR(MAX_ROWLOCKS_REPOSITION, max_rowlocks_reposition, QUANTITY, 10)

/* Force a physical commit after this many physical operations */
DEF_ATTR(PHYSICAL_COMMIT_INTERVAL, physical_commit_interval, QUANTITY, 512)

/* Commit on every btree operation */
DEF_ATTR(ROWLOCKS_MICRO_COMMIT, rowlocks_micro_commit, BOOLEAN, 1)

/* XXX temporary attr to help me try to reproduce a deadlock - this will always
 * be ON */
DEF_ATTR(SET_ABORT_FLAG_IN_LOCKER, set_abort_flag_in_locker, BOOLEAN, 1)

/* For logical transactions, have the slave send an 'ack' after this many
 * physical operations */
/* XXX These seem to kill performance */
DEF_ATTR(PHYSICAL_ACK_INTERVAL, physical_ack_interval, QUANTITY, 0)
DEF_ATTR(ACK_ON_REPLAG_THRESHOLD, ack_on_replag_threshold, QUANTITY, 0)

/* Drop the undo interval to 1 after this many deadlocks */
DEF_ATTR(UNDO_DEADLOCK_THRESHOLD, undo_deadlock_threshold, QUANTITY, 1)

/* temporary, as an emergency switch */
DEF_ATTR(USE_RECOVERY_START_FOR_LOG_DELETION,
         use_recovery_start_for_log_deletion, BOOLEAN, 1)
DEF_ATTR(DEBUG_LOG_DELETION, debug_log_deletion, BOOLEAN, 0)

DEF_ATTR(NET_INORDER_LOGPUTS, net_inorder_logputs, BOOLEAN, 0)

DEF_ATTR(RCACHE_COUNT, rcache_count, QUANTITY, 257)
DEF_ATTR(RCACHE_PGSZ, rcache_pgsz, BYTES, 4096)
DEF_ATTR(DEADLK_PRIORITY_BUMP_ON_FSTBLK, deadlk_priority_bump_on_fstblk,
         QUANTITY, 5)
DEF_ATTR(FSTBLK_MINQ, fstblk_minq, QUANTITY, 262144)
DEF_ATTR(DISABLE_CACHING_STMT_WITH_FDB, disable_caching_stmt_with_fdb, BOOLEAN,
         1)
DEF_ATTR(FDB_SQLSTATS_CACHE_LOCK_WAITTIME_NSEC,
         fdb_sqlstats_cache_waittime_nsec, QUANTITY, 1000)

DEF_ATTR(PRIVATE_BLKSEQ_CACHESZ, private_blkseq_cachesz, BYTES, 4194304)
DEF_ATTR(PRIVATE_BLKSEQ_MAXAGE, private_blkseq_maxage, SECS, 600)
DEF_ATTR(PRIVATE_BLKSEQ_MAXTRAVERSE, private_blkseq_maxtraverse, QUANTITY, 4)
DEF_ATTR(PRIVATE_BLKSEQ_STRIPES, private_blkseq_stripes, QUANTITY, 1)
DEF_ATTR(PRIVATE_BLKSEQ_ENABLED, private_blkseq_enabled, BOOLEAN, 1)
DEF_ATTR(PRIVATE_BLKSEQ_CLOSE_WARN_TIME, private_blkseq_close_warn_time,
         BOOLEAN, 100)

DEF_ATTR(LOG_DELETE_LOW_HEADROOM_BREAKTIME, log_delete_low_headroom_breaktime,
         QUANTITY, 10)
DEF_ATTR(ONE_PASS_DELETE, enable_one_pass_delete, BOOLEAN, 1)

DEF_ATTR(REMOVE_COMMITDELAY_ON_COHERENT_CLUSTER,
         remove_commitdelay_on_coherent_cluster, BOOLEAN, 1)

DEF_ATTR(DISABLE_SERVER_SOCKPOOL, disable_sockpool, BOOLEAN, 1)
DEF_ATTR(TIMEOUT_SERVER_SOCKPOOL, timeout_sockpool, SECS, 10)

DEF_ATTR(COHERENCY_LEASE, coherency_lease, MSECS, 500)

/* Wait-fudge to ensure that a replicant has gone incoherent */
DEF_ATTR(ADDITIONAL_DEFERMS, additional_deferms, MSECS, 100)

/* Use udp to issue leases */
DEF_ATTR(COHERENCY_LEASE_UDP, coherency_lease_udp, BOOLEAN, 1)

/* How often we renew leases */
DEF_ATTR(LEASE_RENEW_INTERVAL, lease_renew_interval, MSECS, 200)

/* Prevent upgrades for at least this many milliseconds after a downgrade */
DEF_ATTR(DOWNGRADE_PENALTY, downgrade_penalty, MSECS, 10000)

/* Start waiting in waitforseqnum if replicant is within this many bytes of
 * master */
DEF_ATTR(CATCHUP_WINDOW, catchup_window, BYTES, 1000000)

/* Replicant to INCOHERENT_WAIT rather than INCOHERENT on commit if within
 * CATCHUP_WINDOW */
DEF_ATTR(CATCHUP_ON_COMMIT, catchup_on_commit, BOOLEAN, 1)

/* Add a record every <interval> seconds while there are incoherent_wait
 * replicants */
DEF_ATTR(ADD_RECORD_INTERVAL, add_record_interval, SECS, 1)
DEF_ATTR(RLLIST_STEP, rllist_step, QUANTITY, 10)

/* Print a warning when there are only a few genids remaining */
DEF_ATTR(GENID48_WARN_THRESHOLD, genid48_warn_threshold, QUANTITY, 500000000)

DEF_ATTR(DISABLE_SELECTVONLY_TRAN_NOP, disable_selectvonly_tran_nop, BOOLEAN, 0)

/* If DDL_ONLY is set, we dont do checks needed for comdb2sc */
DEF_ATTR(SC_VIA_DDL_ONLY, ddl_only, BOOLEAN, 0)

/* Send page compact requests over UDP */
DEF_ATTR(PAGE_COMPACT_UDP, page_compact_udp, BOOLEAN, 0)

/* Enable page compaction for indexes */
DEF_ATTR(PAGE_COMPACT_INDEXES, page_compact_indexes, BOOLEAN, 0)

DEF_ATTR(ASOF_THREAD_POLL_INTERVAL_MS, asof_thread_poll_interval_ms, MSECS, 500)
DEF_ATTR(ASOF_THREAD_DRAIN_LIMIT, asof_thread_drain_limit, QUANTITY, 0)

DEF_ATTR(REP_VERIFY_MAX_TIME, rep_verify_max_time, SECS, 300)
DEF_ATTR(REP_VERIFY_MIN_PROGRESS, rep_verify_min_progress, BYTES, 10485760)
DEF_ATTR(REP_VERIFY_LIMIT_ENABLED, rep_verify_limit_enabled, BOOLEAN, 1)
DEF_ATTR(TIMEPART_ABORT_ON_PREPERROR, timepart_abort_on_preperror, BOOLEAN, 0)

DEF_ATTR(REPORT_DECIMAL_CONVERSION, report_decimal_conversion, BOOLEAN, 0)
DEF_ATTR(TIMEPART_CHECK_SHARD_EXISTENCE, timepart_check_shard_existence, BOOLEAN, 0)

/* Keep enabled for the merge */
DEF_ATTR(DURABLE_LSNS, durable_lsns, BOOLEAN, 0)
DEF_ATTR(DURABLE_LSN_POLL_INTERVAL_MS, durable_lsn_poll_interval_ms, MSECS, 200)

/* Enabled by default */
DEF_ATTR(RETRIEVE_DURABLE_LSN_AT_BEGIN, retrieve_durable_lsn_at_begin, BOOLEAN, 1)

/* Maximum time a replicant will spend waiting for an lsn to become durable */
DEF_ATTR(DURABLE_MAXWAIT_MS, durable_maxwait_ms, MSECS, 4000)
DEF_ATTR(DOWNGRADE_ON_SEQNUM_GEN_MISMATCH, downgrade_on_seqnum_gen_mismatch, BOOLEAN, 1)
DEF_ATTR(ENABLE_SEQNUM_GENERATIONS, enable_seqnum_generations, BOOLEAN, 1)
DEF_ATTR(ELECT_ON_MISMATCHED_MASTER, elect_on_mismatched_master, BOOLEAN, 1)
DEF_ATTR(SET_REPINFO_MASTER_TRACE, set_repinfo_master_trace, BOOLEAN, 0)
DEF_ATTR(LEASEBASE_TRACE, leasebase_trace, BOOLEAN, 0)
DEF_ATTR(MASTER_LEASE, master_lease, MSECS, 500)
DEF_ATTR(MASTER_LEASE_RENEW_INTERVAL, master_lease_renew_interval, MSECS, 200)
DEF_ATTR(VERBOSE_DURABLE_BLOCK_TRACE, verbose_durable_block_trace, BOOLEAN, 0)
DEF_ATTR(LOGDELETE_RUN_INTERVAL, logdelete_run_interval, SECS, 30)
DEF_ATTR(REQUEST_DURABLE_LSN_FROM_MASTER, request_durable_lsn_from_master, BOOLEAN, 1)
DEF_ATTR(DURABLE_LSN_REQUEST_WAITMS, durable_lsn_request_waitms, MSECS, 1000)
DEF_ATTR(VERIFY_MASTER_LEASE_TRACE, verify_master_lease_trace, BOOLEAN, 0)
DEF_ATTR(RECEIVE_COHERENCY_LEASE_TRACE, receive_coherency_lease_trace, BOOLEAN, 0)
DEF_ATTR(REQUEST_DURABLE_LSN_TRACE, request_durable_lsn_trace, BOOLEAN, 0)
DEF_ATTR(MASTER_LEASE_SET_TRACE, master_lease_set_trace, BOOLEAN, 0)
DEF_ATTR(RECEIVE_START_LSN_REQUEST_TRACE, receive_start_lsn_request_trace, BOOLEAN, 0)
DEF_ATTR(WAIT_FOR_SEQNUM_TRACE, wait_for_seqnum_trace, BOOLEAN, 0)
DEF_ATTR(STARTUP_SYNC_ATTEMPTS, startup_sync_attempts, QUANTITY, 5)
DEF_ATTR(DEBUG_TIMEPART_CRON, dbg_timepart_cron, BOOLEAN, 0)
DEF_ATTR(DEBUG_TIMEPART_SQLITE, dbg_timepart_SQLITE, BOOLEAN, 0)

/* artificial delay before we lock table in record.c -- for testing */
DEF_ATTR(DELAY_LOCK_TABLE_RECORD_C, delay_lock_table_record_c, MSECS, 0)

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
