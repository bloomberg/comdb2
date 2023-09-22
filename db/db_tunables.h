/*
   Copyright 2017, 2021, Bloomberg Finance L.P.

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

#ifndef _DB_TUNABLES_H
#define _DB_TUNABLES_H
/*
  We need this guard to avoid accidental inclusion of this file
  at multiple places.
*/

REGISTER_TUNABLE("abort_on_in_use_rqid", NULL, TUNABLE_BOOLEAN,
                 &gbl_abort_on_clear_inuse_rqid, READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("accept_osql_mismatch", NULL, TUNABLE_BOOLEAN,
                 &gbl_reject_osql_mismatch, READONLY | INVERSE_VALUE | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("allow_lua_print", "Enable to allow stored "
                                    "procedures to print trace on "
                                    "DB's stdout. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_allow_lua_print, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("allow_lua_dynamic_libs",
                 "Enable to allow use of dynamic "
                 "libraries (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_allow_lua_dynamic_libs, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("allow_pragma",
                 "Enable to allow use of the PRAGMA command (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_allow_pragma,
                 NOARG | EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("allow_negative_column_size",
                 "Allow negative column size in csc2 schema. Added mostly for "
                 "backwards compatibility. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_allow_neg_column_size, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("allow_portmux_route", NULL, TUNABLE_BOOLEAN,
                 &gbl_pmux_route_enabled, READONLY | NOARG | READEARLY, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("allow_user_schema",
                 "Enable to allow per-user schemas. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_allow_user_schema, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("analyze_comp_threads",
                 "Number of thread to use when generating samples for "
                 "computing index statistics. (Default: 10)",
                 TUNABLE_INTEGER, &analyze_max_comp_threads, READONLY, NULL,
                 NULL, analyze_set_max_sampling_threads, NULL);
REGISTER_TUNABLE("analyze_comp_threshold",
                 "Index file size above which we'll do sampling, rather than "
                 "scan the entire index. (Default: 104857600)",
                 TUNABLE_INTEGER, &sampling_threshold, READONLY, NULL, NULL,
                 analyze_set_sampling_threshold, NULL);
REGISTER_TUNABLE("analyze_tbl_threads",
                 "Number of threads to go through generated samples when "
                 "generating index statistics. (Default: 5)",
                 TUNABLE_INTEGER, &analyze_max_table_threads, READONLY, NULL,
                 NULL, analyze_set_max_table_threads, NULL);
REGISTER_TUNABLE("badwrite_intvl", NULL, TUNABLE_INTEGER,
                 &gbl_test_badwrite_intvl, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("bbenv", NULL, TUNABLE_BOOLEAN, &gbl_bbenv,
                 DEPRECATED_TUNABLE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("blob_mem_mb", "Blob allocator: Sets the max "
                                "memory limit to allow for blob "
                                "values (in MB). (Default: 0)",
                 TUNABLE_INTEGER, &gbl_blobmem_cap, READONLY, NULL, NULL,
                 blob_mem_mb_update, NULL);
REGISTER_TUNABLE("blobmem_sz_thresh_kb",
                 "Sets the threshold (in KB) above which blobs are allocated "
                 "by the blob allocator. (Default: 0)",
                 TUNABLE_INTEGER, &gbl_blob_sz_thresh_bytes, READONLY, NULL,
                 NULL, blobmem_sz_thresh_kb_update, NULL);
REGISTER_TUNABLE("blobstripe", NULL, TUNABLE_BOOLEAN, &gbl_blobstripe,
                 READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("broken_max_rec_sz", NULL, TUNABLE_INTEGER,
                 &gbl_broken_max_rec_sz, READONLY, NULL, NULL,
                 broken_max_rec_sz_update, NULL);
REGISTER_TUNABLE("broken_num_parser", NULL, TUNABLE_BOOLEAN,
                 &gbl_broken_num_parser, READONLY | NOARG | READEARLY, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("buffers_per_context", NULL, TUNABLE_INTEGER,
                 &gbl_buffers_per_context, READONLY | NOZERO, NULL, NULL, NULL,
                 NULL);
/*
REGISTER_TUNABLE("cache",
                 "Database cache size (in kb) . (Default: 64mb)",
                 TUNABLE_INTEGER, &db->cacheszkb, READONLY, NULL, NULL, NULL,
                 NULL);
*/
REGISTER_TUNABLE("cachekb", NULL, TUNABLE_INTEGER, &db->cacheszkb, READONLY,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("cachekbmax", NULL, TUNABLE_INTEGER, &db->cacheszkbmax,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("cachekbmin", NULL, TUNABLE_INTEGER, &db->cacheszkbmin,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("checkctags", NULL, TUNABLE_ENUM, &gbl_check_client_tags,
                 READONLY, checkctags_value, NULL, checkctags_update, NULL);
REGISTER_TUNABLE("check_constraint_feature", "Enables support for CHECK CONSTRAINTs (Default: ON)",
                 TUNABLE_BOOLEAN, &gbl_check_constraint_feature, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("chkpoint_alarm_time",
                 "Warn if checkpoints are taking more than this many seconds. "
                 "(Default: 60 secs)",
                 TUNABLE_INTEGER, &gbl_chkpoint_alarm_time, READONLY, NULL,
                 NULL, NULL, NULL);
/* Generate the value of 'cluster' on fly (define value()). */
/*
REGISTER_TUNABLE("cluster",
                 "List of nodes that comprise the cluster for this database.",
                 TUNABLE_STRING, &placeholder, READONLY, NULL, NULL, NULL,
                 NULL);
*/
REGISTER_TUNABLE("compress_page_compact_log", NULL, TUNABLE_BOOLEAN,
                 &gbl_compress_page_compact_log, READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
/* NOTE: THIS OPTION IS CURRENTLY IGNORED */
REGISTER_TUNABLE("convflush", NULL, TUNABLE_INTEGER, &gbl_conv_flush_freq,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("crc32c",
                 "Use crc32c (alternate faster implementation of CRC32, "
                 "different checksums) for page checksums. (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_crc32c, READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("create_dba_user",
                 "Automatically create 'dba' user if it does not exist already "
                 "(Default: on)",
                 TUNABLE_BOOLEAN, &gbl_create_dba_user, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("create_default_user",
                 "Automatically create 'default' user when authentication is "
                 "enabled. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_create_default_user, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("crypto", NULL, TUNABLE_STRING, &gbl_crypto, READONLY, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("ctrace_dbdir",
                 "If set, debug trace files will go to the data directory "
                 "instead of `$COMDB2_ROOT/var/log/cdb2/). (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_ctrace_dbdir, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("ctrace_gzip", NULL, TUNABLE_INTEGER, &ctrace_gzip,
                 DEPRECATED_TUNABLE | READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE(
    "ddl_cascade_drop",
    "On DROP, also drop the dependent keys/constraints. (Default: 1)",
    TUNABLE_BOOLEAN, &gbl_ddl_cascade_drop, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("deadlock_policy_override", NULL, TUNABLE_INTEGER,
                 &gbl_deadlock_policy_override, READONLY, NULL, NULL,
                 deadlock_policy_override_update, NULL);
/*
REGISTER_TUNABLE("decimal_rounding", NULL, TUNABLE_INTEGER,
                 &gbl_decimal_rounding, READONLY, NULL, NULL, NULL, NULL);
                 */
REGISTER_TUNABLE("decom_time", "Decomission time. (Default: 0)",
                 TUNABLE_INTEGER, &gbl_decom, READONLY | NOZERO, NULL, NULL,
                 NULL, NULL);
/*
REGISTER_TUNABLE("default_datetime_precision", NULL,
                 TUNABLE_INTEGER, &gbl_datetime_precision, READONLY, NULL, NULL,
                 NULL, NULL);
*/
REGISTER_TUNABLE("default_function_feature", "Enables support for SQL function as default value in column definitions (Default: ON)",
                 TUNABLE_BOOLEAN, &gbl_default_function_feature, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("dir",
                 "Database directory. (Default: $COMDB2_ROOT/var/cdb2/$DBNAME)",
                 TUNABLE_STRING, &db->basedir, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("disable_cache_internal_nodes",
                 "Disables 'enable_cache_internal_nodes'. B-tree leaf nodes "
                 "are treated same as internal nodes.",
                 TUNABLE_BOOLEAN, &gbl_enable_cache_internal_nodes,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("disable_datetime_ms_us_sc",
                 "Disables 'enable_datetime_ms_us_sc'", TUNABLE_BOOLEAN,
                 &gbl_forbid_datetime_ms_us_s2s, READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("disable_datetime_promotion",
                 "Disables 'enable_datetime_promotion'", TUNABLE_BOOLEAN,
                 &gbl_forbid_datetime_promotion, READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("disable_datetime_truncation",
                 "Disables 'enable_datetime_truncation'", TUNABLE_BOOLEAN,
                 &gbl_forbid_datetime_truncation, READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("disable_inplace_blob_optimization",
                 "Disables 'enable_inplace_blob_optimization'", TUNABLE_BOOLEAN,
                 &gbl_inplace_blob_optimization,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("disable_inplace_blobs", "Disables 'enable_inplace_blobs'",
                 TUNABLE_BOOLEAN, &gbl_inplace_blobs,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("disable_lowpri_snapisol", "Disables 'enable_lowpri_snapisol'",
                 TUNABLE_BOOLEAN, &gbl_lowpri_snapisol_sessions,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
/*
REGISTER_TUNABLE("disable_new_snapshot",
                 "Disables 'enable_new_snapshot'", TUNABLE_BOOLEAN,
                 &gbl_new_snapisol, INVERSE_VALUE | READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
*/
REGISTER_TUNABLE("disable_osql_blob_optimization",
                 "Disables 'enable_osql_blob_optimization'", TUNABLE_BOOLEAN,
                 &gbl_osql_blob_optimization, INVERSE_VALUE | READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("disable_overflow_page_trace",
                 "Disables 'enable_overflow_page_trace'", TUNABLE_BOOLEAN,
                 &gbl_disable_overflow_page_trace, NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("disable_page_compact", "Sets 'page_compact_thresh_ff' to 0.",
                 TUNABLE_BOOLEAN, &gbl_pg_compact_thresh,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("disable_page_compact_backward_scan",
                 "Disables 'enable_page_compact_backward_scan'",
                 TUNABLE_BOOLEAN, &gbl_disable_backward_scan, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("disable_page_latches", "Disables 'page_latches'",
                 TUNABLE_BOOLEAN, &gbl_page_latches,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("disable_partial_indexes", "Disables 'enable_partial_indexes'",
                 TUNABLE_BOOLEAN, &gbl_partial_indexes,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("disable_prefault_udp", "Disables 'enable_prefault_udp'",
                 TUNABLE_BOOLEAN, &gbl_prefault_udp, INVERSE_VALUE | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("disable_replicant_latches", "Disables 'replicant_latches'",
                 TUNABLE_BOOLEAN, &gbl_replicant_latches,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("disable_rowlock_locking", NULL, TUNABLE_BOOLEAN,
                 &gbl_disable_rowlocks, READONLY | NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("stack_at_lock_get", "Stores stack-id for every lock-get.  (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_stack_at_lock_get, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("stack_at_lock_handle", "Stores stack-id for every lock-handle.  (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_stack_at_lock_handle, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("stack_at_write_lock", "Stores stack-id for every write-lock.  (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_stack_at_write_lock, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("stack_at_lock_gen_increment", "Stores stack-id when lock's generation increments.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_stack_at_lock_gen_increment, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("disable_seekscan_optimization",
                 "Disables SEEKSCAN optimization", TUNABLE_BOOLEAN,
                 &gbl_disable_seekscan_optimization, NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("disable_skip_rows", NULL, TUNABLE_BOOLEAN,
                 &gbl_disable_skip_rows, READONLY | NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("disable_sparse_lockerid_map",
                 "Disables 'enable_sparse_lockerid_map'", TUNABLE_BOOLEAN,
                 &gbl_sparse_lockerid_map, INVERSE_VALUE | READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("disable_sql_dlmalloc",
                 "If set, will use default system malloc for SQL state "
                 "machines. By default, each thread running SQL gets a "
                 "dedicated memory pool. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_disable_sql_dlmalloc, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("disable_tagged_api", "Disables 'enable_tagged_api'",
                 TUNABLE_BOOLEAN, &gbl_disable_tagged_api, NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("disable_temptable_pool", "Sets 'temptable_limit' to 0.",
                 TUNABLE_BOOLEAN, &gbl_temptable_pool_capacity,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("disable_upgrade_ahead", "Sets 'enable_upgrade_ahead' to 0.",
                 TUNABLE_BOOLEAN, &gbl_num_record_upgrades,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("disallow_portmux_route", "Disables 'allow_portmux_route'",
                 TUNABLE_BOOLEAN, &gbl_pmux_route_enabled,
                 INVERSE_VALUE | READONLY | NOARG | READEARLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("dont_abort_on_in_use_rqid", "Disable 'abort_on_in_use_rqid'",
                 TUNABLE_BOOLEAN, &gbl_abort_on_clear_inuse_rqid,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("dont_forbid_ulonglong", "Disables 'forbid_ulonglong'",
                 TUNABLE_BOOLEAN, &gbl_forbid_ulonglong,
                 INVERSE_VALUE | NOARG | READEARLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("dont_init_with_inplace_updates",
                 "Disables 'init_with_inplace_updates'", TUNABLE_BOOLEAN,
                 &gbl_init_with_ipu, INVERSE_VALUE | READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("dont_init_with_instant_schema_change",
                 "Disables 'instant_schema_change'", TUNABLE_BOOLEAN,
                 &gbl_init_with_instant_sc, INVERSE_VALUE | READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("dont_init_with_ondisk_header",
                 "Disables 'init_with_ondisk_header'", TUNABLE_BOOLEAN,
                 &gbl_init_with_odh, INVERSE_VALUE | READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("dont_init_queue_with_persistent_sequence",
                 "Disables 'init_queue_with_persistent_sequence'",
                 TUNABLE_BOOLEAN, &gbl_init_with_queue_persistent_seq,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("dont_init_with_queue_persistent_sequence",
                 "Disables 'dont_init_with_queue_ondisk_header'",
                 TUNABLE_BOOLEAN, &gbl_init_with_queue_persistent_seq,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("dont_optimize_repdb_truncate",
                 "Disable 'optimize_repdb_truncate'", TUNABLE_BOOLEAN,
                 &gbl_optimize_truncate_repdb,
                 INVERSE_VALUE | READONLY | NOARG | READEARLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("dont_prefix_foreign_keys", "Disables 'prefix_foreign_keys'",
                 TUNABLE_BOOLEAN, &gbl_fk_allow_prefix_keys,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("dont_superset_foreign_keys",
                 "Disables 'superset_foreign_keys'", TUNABLE_BOOLEAN,
                 &gbl_fk_allow_superset_keys, INVERSE_VALUE | READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("dont_sort_nulls_with_header",
                 "Disables 'sort_nulls_with_header'", TUNABLE_BOOLEAN,
                 &gbl_sort_nulls_correctly, INVERSE_VALUE | READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("dtastripe", NULL, TUNABLE_INTEGER, &gbl_dtastripe,
                 READONLY | NOZERO, NULL, dtastripe_verify, NULL, NULL);
REGISTER_TUNABLE("early",
                 "When set, replicants will ack a transaction as soon as they "
                 "acquire locks - note that replication must succeed at that "
                 "point, and reads on that node will either see the records or "
                 "block. (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_early, READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("enable_berkdb_retry_deadlock_bias", NULL, TUNABLE_BOOLEAN,
                 &gbl_enable_berkdb_retry_deadlock_bias, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE(
    "enable_cache_internal_nodes",
    "B-tree internal nodes have a higher cache priority. (Default: on)",
    TUNABLE_BOOLEAN, &gbl_enable_cache_internal_nodes, READONLY | NOARG, NULL,
    NULL, NULL, NULL);
REGISTER_TUNABLE("enable_datetime_ms_us_sc", NULL, TUNABLE_BOOLEAN,
                 &gbl_forbid_datetime_ms_us_s2s,
                 READONLY | INVERSE_VALUE | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("enable_datetime_promotion", NULL, TUNABLE_BOOLEAN,
                 &gbl_forbid_datetime_promotion,
                 READONLY | INVERSE_VALUE | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("enable_datetime_truncation", NULL, TUNABLE_BOOLEAN,
                 &gbl_forbid_datetime_truncation,
                 READONLY | INVERSE_VALUE | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("enable_inplace_blob_optimization",
                 "Enables inplace blob updates (blobs are updated in place in "
                 "their b-tree when possible, not deleted/added) Note: This "
                 "changes the data-format. (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_inplace_blob_optimization,
                 READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE(
    "enable_inplace_blobs",
    "Do not update the rowid of a blob entry on an update. (Default: on)",
    TUNABLE_BOOLEAN, &gbl_inplace_blobs, READONLY | NOARG, NULL, NULL, NULL,
    NULL);
REGISTER_TUNABLE("enable_lowpri_snapisol",
                 "Give lower priority to locks acquired when updating snapshot "
                 "state. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_lowpri_snapisol_sessions,
                 READONLY | NOARG, NULL, NULL, NULL, NULL);

/*
REGISTER_TUNABLE("enable_new_snapshot",
                 "Enable new SNAPSHOT implementation. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_new_snapisol, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE(
    "enable_new_snapshot_asof",
    "Enable new BEGIN TRANSACTION AS OF implementation. (Default: off)",
    TUNABLE_BOOLEAN, &gbl_new_snapisol_asof, READONLY | NOARG, NULL, NULL, NULL,
    NULL);
REGISTER_TUNABLE("enable_new_snapshot_logging",
                 "Enable alternate logging scheme. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_new_snapisol_logging, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
*/
REGISTER_TUNABLE("enable_osql_blob_optimization",
                 "Replicant tracks which columns are modified in a transaction "
                 "to allow blob updates to be ommitted if possible. (Default: "
                 "on)",
                 TUNABLE_BOOLEAN, &gbl_osql_blob_optimization, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("enable_overflow_page_trace",
                 "If set, warn when a page order table scan encounters an "
                 "overflow page. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_disable_overflow_page_trace,
                 INVERSE_VALUE | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("enable_page_compact_backward_scan", NULL, TUNABLE_INTEGER,
                 &gbl_disable_backward_scan, INVERSE_VALUE | READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE(
    "enable_partial_indexes",
    "If set, allows partial index definitions in table schema. (Default: off)",
    TUNABLE_BOOLEAN, &gbl_partial_indexes, READONLY | NOARG, NULL, NULL, NULL,
    NULL);
REGISTER_TUNABLE("enable_prefault_udp",
                 "Send lossy prefault requests to replicants. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_prefault_udp, NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("enable_selectv_range_check",
                 "If set, SELECTV will send ranges for verification, not every "
                 "touched record. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_selectv_rangechk, NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("disable_selectv_range_check",
                 "Disables 'enable_selectv_range_check'", TUNABLE_BOOLEAN,
                 &gbl_selectv_rangechk, INVERSE_VALUE | NOARG, NULL, NULL, NULL,
                 NULL);
/*
REGISTER_TUNABLE("enable_snapshot_isolation",
                 "Enable to allow SNAPSHOT level transactions to run against "
                 "the database. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_snapisol, READONLY, NULL, NULL, NULL,
                 NULL);
*/
REGISTER_TUNABLE("enable_sparse_lockerid_map",
                 "If set, allocates a sparse map of lockers for deadlock "
                 "resolution. (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_sparse_lockerid_map, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("enable_sp_strict_assignments", NULL, TUNABLE_INTEGER,
                 &gbl_spstrictassignments, READONLY | NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE(
    "enable_sq_flattening_optimization",
    "Enable subquery flattening optimization for OUTER JOINS (Default: off)",
    TUNABLE_BOOLEAN, &gbl_enable_sq_flattening_optimization,
    NOARG | INTERNAL | EXPERIMENTAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE(
    "enable_internal_sql_stmt_caching",
    "Enable caching of query plans for internal sql statements (Default: on)",
    TUNABLE_BOOLEAN, &gbl_enable_internal_sql_stmt_caching, READONLY | NOARG,
    NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("enable_sql_stmt_caching",
                 "Enable caching of query plans. If followed by \"all\" will "
                 "cache all queries, including those without parameters. "
                 "(Default: on)",
                 TUNABLE_ENUM, &gbl_enable_sql_stmt_caching, READONLY | NOARG,
                 enable_sql_stmt_caching_value, NULL,
                 enable_sql_stmt_caching_update, NULL);
REGISTER_TUNABLE("enable_tagged_api",
                 "Enables tagged api requests. (Default: on)", TUNABLE_BOOLEAN,
                 &gbl_disable_tagged_api, INVERSE_VALUE | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("enable_upgrade_ahead",
                 "Occasionally update read records to the newest schema "
                 "version (saves some processing when reading them later). "
                 "(Default: off)",
                 TUNABLE_INTEGER, &gbl_num_record_upgrades, READONLY | NOARG,
                 NULL, NULL, enable_upgrade_ahead_update, NULL);
REGISTER_TUNABLE("enque_flush_interval", NULL, TUNABLE_INTEGER,
                 &gbl_enque_flush_interval, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("enque_reorder_lookahead", NULL, TUNABLE_INTEGER,
                 &gbl_enque_reorder_lookahead, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("env_messages", NULL, TUNABLE_BOOLEAN, &gbl_noenv_messages,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("epochms_repts", NULL, TUNABLE_BOOLEAN,
                 &gbl_berkdb_epochms_repts, READONLY | NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("erroff", "Disables 'erron'", TUNABLE_BOOLEAN, &db->errstaton,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("erron", NULL, TUNABLE_BOOLEAN, &db->errstaton,
                 READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE(
    "exclusive_blockop_qconsume",
    "Enables serialization of blockops and queue consumes. (Default: off)",
    TUNABLE_BOOLEAN, &gbl_exclusive_blockop_qconsume, READONLY | NOARG, NULL,
    NULL, NULL, NULL);
REGISTER_TUNABLE("exitalarmsec", NULL, TUNABLE_INTEGER, &gbl_exit_alarm_sec,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("exit_on_internal_failure", NULL, TUNABLE_BOOLEAN,
                 &gbl_exit_on_internal_error, READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("fdbdebg", NULL, TUNABLE_INTEGER, &gbl_fdb_track, 0, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("fdbtrackhints", NULL, TUNABLE_INTEGER, &gbl_fdb_track_hints,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("forbid_ulonglong", "Disallow u_longlong. (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_forbid_ulonglong,
                 NOARG | READEARLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("force_highslot", NULL, TUNABLE_BOOLEAN, &gbl_force_highslot,
                 READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("foreign_db_allow_cross_class", NULL, TUNABLE_BOOLEAN,
                 &gbl_fdb_allow_cross_classes, READONLY | NOARG | READEARLY,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("foreign_db_resolve_local", NULL, TUNABLE_BOOLEAN,
                 &gbl_fdb_resolve_local, READONLY | NOARG | READEARLY, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("foreign_db_push_remote", NULL, TUNABLE_BOOLEAN,
                 &gbl_fdb_push_remote, NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("fullrecovery", "Attempt to run database "
                                 "recovery from the beginning of "
                                 "available logs. (Default : off)",
                 TUNABLE_BOOLEAN, &gbl_fullrecovery, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("gbl_exit_on_pthread_create_fail",
                 "If set, database will exit if thread pools aren't able to "
                 "create threads. (Default: 1)",
                 TUNABLE_INTEGER, &gbl_exit_on_pthread_create_fail, READONLY,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("goslow", NULL, TUNABLE_BOOLEAN, &gbl_goslow, NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("gofast", NULL, TUNABLE_BOOLEAN, &gbl_goslow,
                 INVERSE_VALUE | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE(
    "group_concat_memory_limit",
    "Restrict GROUP_CONCAT from using more than this amount of memory; 0 "
    "implies SQLITE_MAX_LENGTH, the limit imposed by sqlite. (Default: 0)",
    TUNABLE_INTEGER, &gbl_group_concat_mem_limit, READONLY, NULL, NULL, NULL,
    NULL);
REGISTER_TUNABLE("heartbeat_check_time",
                 "Raise an error if no heartbeat for this amount of time (in "
                 "secs). (Default: 10 secs)",
                 TUNABLE_INTEGER, &gbl_heartbeat_check, READONLY | NOZERO, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("heartbeat_send_time",
                 "Send heartbeats this often. (Default: 5secs)",
                 TUNABLE_INTEGER, &gbl_heartbeat_send, READONLY | NOZERO, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("hostname", NULL, TUNABLE_STRING, &gbl_myhostname,
                 READONLY | READEARLY, NULL, NULL, hostname_update, NULL);
REGISTER_TUNABLE("incoherent_alarm_time", NULL, TUNABLE_INTEGER,
                 &gbl_incoherent_alarm_time, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("incoherent_msg_freq", NULL, TUNABLE_INTEGER,
                 &gbl_incoherent_msg_freq, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("inflatelog", NULL, TUNABLE_INTEGER, &gbl_inflate_log,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("init_with_bthash", NULL, TUNABLE_INTEGER,
                 &gbl_init_with_bthash, READONLY | NOZERO, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("init_with_compr", NULL, TUNABLE_ENUM, &gbl_init_with_compr,
                 READONLY, init_with_compr_value, NULL, init_with_compr_update,
                 NULL);
REGISTER_TUNABLE("init_with_queue_compr", NULL, TUNABLE_ENUM,
                 &gbl_init_with_queue_compr, READONLY, init_with_compr_value,
                 NULL, init_with_queue_compr_update, NULL);
REGISTER_TUNABLE("init_with_compr_blobs", NULL, TUNABLE_ENUM,
                 &gbl_init_with_compr_blobs, READONLY, init_with_compr_value,
                 NULL, init_with_compr_blobs_update, NULL);
REGISTER_TUNABLE("init_with_genid48",
                 "Enables Genid48 for the database. (Default: on)",
                 TUNABLE_INTEGER, &gbl_init_with_genid48, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("init_with_inplace_updates",
                 "Initialize tables with inplace-update support. (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_init_with_ipu, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("init_with_instant_schema_change",
                 "Same as 'instant_schema_change'", TUNABLE_BOOLEAN,
                 &gbl_init_with_instant_sc, READONLY | NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("init_with_ondisk_header",
                 "Initialize tables with on-disk header. (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_init_with_odh, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("init_with_queue_ondisk_header",
                 "Initialize queues with on-disk header. (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_init_with_queue_odh, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("init_with_queue_persistent_sequence",
                 "Initialize queues with persistent sequence numbers. "
                 "(Default: on)",
                 TUNABLE_BOOLEAN, &gbl_init_with_queue_persistent_seq,
                 READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("init_with_rowlocks",
                 "Enables row-locks for the database. (Default: 0)",
                 TUNABLE_INTEGER, &gbl_init_with_rowlocks, READONLY | NOARG,
                 NULL, NULL, init_with_rowlocks_update, NULL);
REGISTER_TUNABLE(
    "init_with_rowlocks_master_only",
    "Enables row-locks for the database (master-only). (Default: 0)",
    TUNABLE_INTEGER, &gbl_init_with_rowlocks, READONLY | NOARG, NULL, NULL,
    init_with_rowlocks_master_only_update, NULL);
REGISTER_TUNABLE("init_with_time_based_genids", "Enables time-based GENIDs",
                 TUNABLE_BOOLEAN, &gbl_init_with_genid48,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("instant_schema_change",
                 "When possible (eg: when just adding fields) schema change "
                 "will not rebuild the underlying tables. (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_init_with_instant_sc, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("ioqueue",
                 "Maximum depth of the I/O prefaulting queue. (Default: 0)",
                 TUNABLE_INTEGER, &gbl_ioqueue, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("iothreads",
                 "Number of threads to use for I/O prefaulting. (Default: 0)",
                 TUNABLE_INTEGER, &gbl_iothreads, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("keycompr",
                 "Enable index compression (applies to newly allocated index "
                 "pages, rebuild table to force for all pages.",
                 TUNABLE_BOOLEAN, &gbl_keycompr, READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("largepages", "Enables large pages. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_largepages, READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("lclpooledbufs", NULL, TUNABLE_INTEGER, &gbl_lclpooled_buffers,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("lk_hash", NULL, TUNABLE_INTEGER, &gbl_lk_hash,
                 READONLY | READEARLY, NULL, lk_verify, NULL, NULL);
REGISTER_TUNABLE("lk_part", NULL, TUNABLE_INTEGER, &gbl_lk_parts,
                 READONLY | READEARLY, NULL, lk_verify, NULL, NULL);
REGISTER_TUNABLE("lkr_hash", NULL, TUNABLE_INTEGER, &gbl_lkr_hash,
                 READONLY | READEARLY, NULL, lk_verify, NULL, NULL);
REGISTER_TUNABLE("lkr_part", NULL, TUNABLE_INTEGER, &gbl_lkr_parts,
                 READONLY | READEARLY, NULL, lk_verify, NULL, NULL);
REGISTER_TUNABLE("lock_conflict_trace",
                 "Dump count of lock conflicts every second. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_lock_conflict_trace, NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("lock_dba_user",
                 "When enabled, 'dba' user cannot be removed and its access "
                 "permissions cannot be modified. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_lock_dba_user, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("log_delete_age", "Log deletion policy", TUNABLE_INTEGER,
                 &db->log_delete_age, READONLY, NULL, NULL, NULL, NULL);
/* The following 3 tunables have been marked internal as we do not want them
 * to be listed via comdb2_tunables system table. log_delete_age's value
 * should be sufficient to reflect on the current log deletion policy. */
REGISTER_TUNABLE(
    "log_delete_after_backup",
    "Set log deletion policy to disable log deletion (can be set by backups)",
    TUNABLE_INTEGER, &db->log_delete_age, READONLY | NOARG | INTERNAL, NULL,
    NULL, log_delete_after_backup_update, NULL);
REGISTER_TUNABLE(
    "log_delete_before_startup",
    "Set log deletion policy to disable logs older than database startup time.",
    TUNABLE_INTEGER, &db->log_delete_age, READONLY | NOARG | INTERNAL, NULL,
    NULL, log_delete_before_startup_update, NULL);
REGISTER_TUNABLE("log_delete_now",
                 "Set log deletion policy to delete logs as soon as possible.",
                 TUNABLE_INTEGER, &db->log_delete_age,
                 READONLY | NOARG | INTERNAL, NULL, NULL, log_delete_now_update,
                 NULL);
REGISTER_TUNABLE("loghist", NULL, TUNABLE_INTEGER, &gbl_loghist,
                 READONLY | NOARG, NULL, NULL, loghist_update, NULL);
REGISTER_TUNABLE("loghist_verbose", NULL, TUNABLE_BOOLEAN, &gbl_loghist_verbose,
                 READONLY | NOARG, NULL, NULL, NULL, NULL);
/*
REGISTER_TUNABLE("mallocregions", NULL, TUNABLE_INTEGER,
                 &gbl_malloc_regions, READONLY, NULL, NULL, NULL, NULL);
*/

REGISTER_TUNABLE("mask_internal_tunables",
                 "When enabled, comdb2_tunables system table would not list "
                 "INTERNAL tunables (Default: on)", TUNABLE_BOOLEAN,
                 &gbl_mask_internal_tunables, NOARG, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("allow_readonly_runtime_mod",
                 "When enabled, allow modification of READONLY tunables at runtime.  (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_allow_readonly_runtime_mod, NOARG | INTERNAL, NULL, NULL, NULL, NULL);

/*
  Note: master_retry_poll_ms' value < 0 was previously ignored without
  any error.
*/
REGISTER_TUNABLE("master_retry_poll_ms",
                 "Have a node wait this long after a master swing before "
                 "retrying a transaction. (Default: 100ms)",
                 TUNABLE_INTEGER, &gbl_master_retry_poll_ms, READONLY, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("debug_blkseq_race", "Pause after adding blkseq to reproduce blkseq race.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_debug_blkseq_race, INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("master_swing_osql_verbose",
                 "Produce verbose trace for SQL handlers detecting a master "
                 "change. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_master_swing_osql_verbose,
                 READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("master_swing_osql_verbose_off",
                 "Disables 'master_swing_osql_verbose'", TUNABLE_BOOLEAN,
                 &gbl_master_swing_osql_verbose, INVERSE_VALUE | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("master_swing_sock_restart_sleep",
                 "For testing: sleep in osql_sock_restart when master swings",
                 TUNABLE_INTEGER, &gbl_master_swing_sock_restart_sleep,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("maxblobretries", NULL, TUNABLE_INTEGER, &gbl_maxblobretries,
                 READONLY, NULL, maxretries_verify, NULL, NULL);
REGISTER_TUNABLE("maxblockops", NULL, TUNABLE_INTEGER, &gbl_maxblockops,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("maxcolumns",
                 "Raise the maximum permitted number of columns per table. "
                 "There is a hard limit of 1024. (Default: 255)",
                 TUNABLE_INTEGER, &gbl_max_columns_soft_limit, READONLY, NULL,
                 maxcolumns_verify, NULL, NULL);
REGISTER_TUNABLE("maxcontextskips", NULL, TUNABLE_INTEGER, &gbl_maxcontextskips,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("maxosqltransfer",
                 "Maximum number of record modifications allowed per "
                 "transaction. (Default: 50000)",
                 TUNABLE_INTEGER, &g_osql_max_trans, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("maxthrottletime", NULL, TUNABLE_INTEGER,
                 &gbl_osql_max_throttle_sec, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("max_incoherent_nodes", NULL, TUNABLE_INTEGER,
                 &gbl_max_incoherent_nodes, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("max_lua_instructions",
                 "Maximum lua opcodes to execute before we assume the stored "
                 "procedure is looping and kill it. (Default: 10000)",
                 TUNABLE_INTEGER, &gbl_max_lua_instructions, READONLY, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("max_num_compact_pages_per_txn", NULL, TUNABLE_INTEGER,
                 &gbl_max_num_compact_pages_per_txn, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("maxq",
                 "Maximum queue depth for write requests. (Default: 192)",
                 TUNABLE_INTEGER, &gbl_maxqueue, READONLY, NULL, NULL,
                 maxq_update, NULL);
REGISTER_TUNABLE("maxretries", "Maximum number of times a "
                               "transactions will be retried on a "
                               "deadlock. (Default: 500)",
                 TUNABLE_INTEGER, &gbl_maxretries, READONLY, NULL,
                 maxretries_verify, NULL, NULL);
REGISTER_TUNABLE(
    "max_sqlcache_hints",
    "Maximum number of \"hinted\" query plans to keep (global). (Default: 100)",
    TUNABLE_INTEGER, &gbl_max_sql_hint_cache, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("max_sqlcache_per_thread",
                 "Maximum number of plans to cache per sql thread (statement "
                 "cache is per-thread). (Default: 10)",
                 TUNABLE_INTEGER, &gbl_max_sqlcache, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("maxt", NULL, TUNABLE_INTEGER, &gbl_maxthreads,
                 READONLY | NOZERO, NULL, NULL, maxt_update, NULL);
REGISTER_TUNABLE(
    "maxwt",
    "Maximum number of threads processing write requests. (Default: 8)",
    TUNABLE_INTEGER, &gbl_maxwthreads, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("max_query_fingerprints",
                 "Maximum number of queries to be placed into the fingerprint "
                 "hash (Default: 1000)",
                 TUNABLE_INTEGER, &gbl_fingerprint_max_queries, 0, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("memnice", NULL, TUNABLE_INTEGER, &gbl_mem_nice,
                 READONLY | NOARG, NULL, NULL, memnice_update, NULL);
REGISTER_TUNABLE("mempget_timeout", NULL, TUNABLE_INTEGER,
                 &__gbl_max_mpalloc_sleeptime, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("memstat_autoreport_freq",
                 "Dump memory usage to trace files at this frequency (in "
                 "secs). (Default: 180 secs)",
                 TUNABLE_INTEGER, &gbl_memstat_freq, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("morecolumns", NULL, TUNABLE_BOOLEAN, &gbl_morecolumns,
                 READONLY | NOARG | READEARLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("move_deadlock_max_attempt", NULL, TUNABLE_INTEGER,
                 &gbl_move_deadlk_max_attempt, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("name", NULL, TUNABLE_STRING, &gbl_name, DEPRECATED_TUNABLE | READONLY,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("natural_types", "Same as 'nosurprise'", TUNABLE_BOOLEAN,
                 &gbl_surprise, INVERSE_VALUE | READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("netbufsz", "Size of the network buffer (per "
                             "node) for the replication network. "
                             "(Default: 1MB)",
                 TUNABLE_INTEGER, &gbl_netbufsz, READONLY | NOZERO, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE(
    "net_explicit_flush_trace",
    "Produce a stack dump for long network flushes. (Default: off)",
    TUNABLE_BOOLEAN, &explicit_flush_trace, READONLY | NOARG, NULL, NULL, NULL,
    NULL);
REGISTER_TUNABLE("net_lmt_upd_incoherent_nodes", NULL, TUNABLE_INTEGER,
                 &gbl_net_lmt_upd_incoherent_nodes, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("net_max_mem",
                 "Maximum size (in MB) of items keep on replication network "
                 "queue before dropping (per replicant). (Default: 0)",
                 TUNABLE_INTEGER, &gbl_net_max_mem, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("net_max_queue",
                 "Maximum number of items to keep on replication network queue "
                 "before dropping (per replicant). (Default: 25000)",
                 TUNABLE_INTEGER, &gbl_net_max_queue, READONLY, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("net_poll",
                 "Allow a connection to linger for this many milliseconds "
                 "before identifying itself. Connections that take longer are "
                 "shut down. (Default: 100ms)",
                 TUNABLE_INTEGER, &gbl_net_poll, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("netpoll", "Alias of net_poll", TUNABLE_INTEGER, &gbl_net_poll, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("net_portmux_register_interval",
                 "Check on this interval if our port is correctly registered "
                 "with pmux for the replication net. (Default: 600ms)",
                 TUNABLE_INTEGER, &gbl_net_portmux_register_interval, READONLY,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("net_throttle_percent", NULL, TUNABLE_INTEGER,
                 &gbl_net_throttle_percent, READONLY, NULL, percent_verify,
                 NULL, NULL);
REGISTER_TUNABLE("noblobstripe", "Disables 'blobstripe'", TUNABLE_BOOLEAN,
                 &gbl_blobstripe, INVERSE_VALUE | READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("no_compress_page_compact_log",
                 "Disables 'compress_page_compact_log'", TUNABLE_BOOLEAN,
                 &gbl_compress_page_compact_log,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("nocrc32c", "Disables 'crc32c'", TUNABLE_BOOLEAN, &gbl_crc32c,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("nodeid", NULL, TUNABLE_INTEGER, &gbl_mynodeid, READONLY, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("noearly", "Disables 'early'", TUNABLE_BOOLEAN, &gbl_early,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("noenv_messages", NULL, TUNABLE_BOOLEAN, &gbl_noenv_messages,
                 READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("no_epochms_repts", "Disables 'epochms_repts'",
                 TUNABLE_BOOLEAN, &gbl_berkdb_epochms_repts,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("no_exit_on_internal_failure",
                 "Disables 'exit_on_internal_failure'", TUNABLE_BOOLEAN,
                 &gbl_exit_on_internal_error, INVERSE_VALUE | READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("nokeycompr", "Disables 'keycompr'", TUNABLE_BOOLEAN,
                 &gbl_keycompr, INVERSE_VALUE | READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("no_lock_conflict_trace", "Disables 'lock_conflict_trace'",
                 TUNABLE_BOOLEAN, &gbl_lock_conflict_trace,
                 INVERSE_VALUE | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("nonames", NULL, TUNABLE_BOOLEAN, &gbl_nonames,
                 READONLY | NOARG | READEARLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("no_net_explicit_flush_trace",
                 "Disables 'net_explicit_flush_trace'", TUNABLE_BOOLEAN,
                 &explicit_flush_trace, INVERSE_VALUE | READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("no_null_blob_fix", "Disables 'null_blob_fix'",
                 TUNABLE_BOOLEAN, &gbl_disallow_null_blobs,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("norcache", "Disables 'rcache'", TUNABLE_BOOLEAN, &gbl_rcache,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("noreallearly", "Disables 'reallearly'", TUNABLE_BOOLEAN,
                 &gbl_reallyearly, INVERSE_VALUE | READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("norepdebug", "Disables 'repdebug'", TUNABLE_BOOLEAN,
                 &gbl_repdebug, INVERSE_VALUE | READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("no_rep_process_txn_trace", "Disables 'rep_process_txn_trace'",
                 TUNABLE_BOOLEAN, &gbl_rep_process_txn_time,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("no_round_robin_stripes", "Disables 'round_robin_stripes'",
                 TUNABLE_BOOLEAN, &gbl_round_robin_stripes,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("no_sc_inco_chk", NULL, TUNABLE_BOOLEAN, &gbl_sc_inco_chk,
                 READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("no_static_tag_blob_fix", NULL, TUNABLE_BOOLEAN,
                 &gbl_force_notnull_static_tag_blobs,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("nosurprise", NULL, TUNABLE_BOOLEAN, &gbl_surprise,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("notimeout", "Turns off SQL timeouts. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_notimeouts, NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("no_toblock_net_throttle", "Disables 'toblock_net_throttle'",
                 TUNABLE_BOOLEAN, &gbl_toblock_net_throttle,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("noudp", NULL, TUNABLE_BOOLEAN, &gbl_udp,
                 INVERSE_VALUE | READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("no_update_delete_limit", NULL, TUNABLE_BOOLEAN,
                 &gbl_update_delete_limit, INVERSE_VALUE | READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("nowatch", "Disable watchdog. Watchdog aborts "
                            "the database if basic things like "
                            "creating threads, allocating memory, "
                            "etc. doesn't work. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_watchdog_disable_at_start,
                 READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("null_blob_fix", NULL, TUNABLE_BOOLEAN,
                 &gbl_disallow_null_blobs, READONLY | NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE(
    "nullfkey",
    "Do not enforce foreign key constraints for null keys. (Default: on)",
    TUNABLE_BOOLEAN, &gbl_nullfkey, READONLY | NOARG | READEARLY, NULL, NULL,
    NULL, NULL);
/*
REGISTER_TUNABLE("nullsort", NULL, TUNABLE_ENUM,
                 &placeholder, READONLY, NULL, NULL, NULL, NULL);
*/
REGISTER_TUNABLE("num_contexts", NULL, TUNABLE_INTEGER, &gbl_num_contexts,
                 READONLY | NOZERO, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("num_record_converts",
                 "During schema changes, pack this many records into a "
                 "transaction. (Default: 100)",
                 TUNABLE_INTEGER, &gbl_num_record_converts, READONLY | NOZERO,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE(
    "old_column_names",
    "Generate and use column names from sqlite version 3.8.9 (Default: on)",
    TUNABLE_BOOLEAN, &gbl_old_column_names, EXPERIMENTAL | INTERNAL, NULL,
    NULL, NULL, NULL);
/* Backwards compatibility: This tunable DOES expect an argument. */
REGISTER_TUNABLE("oldrangexlim", NULL, TUNABLE_BOOLEAN,
                 &gbl_honor_rangextunit_for_old_apis, READONLY, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("on_del_set_null_feature", "Enables support for ON DELETE SET NULL foreign key constraint action (Default: ON)",
                 TUNABLE_BOOLEAN, &gbl_on_del_set_null_feature, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("optimize_repdb_truncate",
                 "Enables use of optimized repdb truncate code. (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_optimize_truncate_repdb,
                 READONLY | NOARG | READEARLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("osql_bkoff_netsend", NULL, TUNABLE_INTEGER,
                 &gbl_osql_bkoff_netsend, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("osql_bkoff_netsend_lmt", NULL, TUNABLE_INTEGER,
                 &gbl_osql_bkoff_netsend_lmt, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("osql_blockproc_timeout_sec", NULL, TUNABLE_INTEGER,
                 &gbl_osql_blockproc_timeout_sec, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("osql_heartbeat_alert_time", NULL, TUNABLE_INTEGER,
                 &gbl_osql_heartbeat_alert, READONLY | NOZERO, NULL,
                 osql_heartbeat_alert_time_verify, NULL, NULL);
REGISTER_TUNABLE("osql_heartbeat_send_time", NULL, TUNABLE_INTEGER,
                 &gbl_osql_heartbeat_send, READONLY | NOZERO, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("osql_max_queue", NULL, TUNABLE_INTEGER, &gbl_osql_max_queue,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("osql_net_poll",
                 "Like net_sql, but for the offload network (used by write "
                 "transactions on replicants to send work to the master) "
                 "(Default: 100ms)",
                 TUNABLE_INTEGER, &gbl_osql_net_poll, READONLY, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("osql_net_portmux_register_interval", NULL, TUNABLE_INTEGER,
                 &gbl_osql_net_portmux_register_interval, READONLY, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("osqlprefaultthreads",
                 "If set, send prefaulting hints to nodes. (Default: 0)",
                 TUNABLE_INTEGER, &gbl_osqlpfault_threads, READONLY, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("osql_verify_ext_chk",
                 "For block transaction mode only - after this many verify "
                 "errors, check if transaction is non-commitable (see default "
                 "isolation level). (Default: on)",
                 TUNABLE_INTEGER, &gbl_osql_verify_ext_chk, READONLY, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("osql_verify_retry_max",
                 "Retry a transaction on a verify error this many times (see "
                 "optimistic concurrency control). (Default: 499)",
                 TUNABLE_INTEGER, &gbl_osql_verify_retries_max, READONLY, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("override_cachekb", NULL, TUNABLE_INTEGER,
                 &db->override_cacheszkb, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("page_compact_latency_ms", NULL, TUNABLE_INTEGER,
                 &gbl_pg_compact_latency_ms, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("page_compact_target_ff", NULL, TUNABLE_DOUBLE,
                 &gbl_pg_compact_target_ff, NOARG, NULL, NULL,
                 page_compact_target_ff_update, NULL);
REGISTER_TUNABLE("page_compact_thresh_ff", NULL, TUNABLE_DOUBLE,
                 &gbl_pg_compact_thresh, READONLY | NOARG, NULL, NULL,
                 page_compact_thresh_ff_update, NULL);
REGISTER_TUNABLE("page_latches",
                 "If set, in rowlocks mode, will acquire fast latches on pages "
                 "instead of full locks. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_page_latches, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE(
    "pageordertablescan",
    "Perform table scans in page order and not row order. (Default: off)",
    TUNABLE_BOOLEAN, &gbl_page_order_table_scan, NOARG, NULL, NULL,
    page_order_table_scan_update, NULL);
/*
REGISTER_TUNABLE("pagesize", NULL, TUNABLE_INTEGER,
                 &placeholder, DEPRECATED_TUNABLE|READONLY, NULL, NULL, NULL,
                 NULL);
*/
REGISTER_TUNABLE("parallel_recovery", NULL, TUNABLE_INTEGER,
                 &gbl_parallel_recovery_threads, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("penaltyincpercent", NULL, TUNABLE_INTEGER,
                 &gbl_penaltyincpercent, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("perfect_ckp", NULL, TUNABLE_INTEGER, &gbl_use_perfect_ckp,
                 READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("portmux_bind_path", NULL, TUNABLE_STRING,
                 NULL, READONLY | READEARLY, portmux_bind_path_get, NULL,
                 portmux_bind_path_set, NULL);
REGISTER_TUNABLE("portmux_port", NULL, TUNABLE_INTEGER, &portmux_port,
                 READONLY | READEARLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("prefaulthelper_blockops", NULL, TUNABLE_INTEGER,
                 &gbl_prefaulthelper_blockops, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("prefaulthelper_sqlreadahead", NULL, TUNABLE_INTEGER,
                 &gbl_prefaulthelper_sqlreadahead, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("prefaulthelperthreads",
                 "Max number of prefault helper threads. (Default: 0)",
                 TUNABLE_INTEGER, &gbl_prefaulthelperthreads, READONLY, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("print_syntax_err",
                 "Trace all SQL with syntax errors. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_print_syntax_err, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("prioritize_queries",
                 "Prioritize SQL queries based on loaded rulesets. "
                 "(Default: off)", TUNABLE_BOOLEAN, &gbl_prioritize_queries,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("verbose_prioritize_queries",
                 "Show prioritized SQL queries based on origin and "
                 "fingerprint.  (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_verbose_prioritize_queries, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("random_lock_release_interval", NULL, TUNABLE_INTEGER,
                 &gbl_sql_random_release_interval, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("rangextlim", NULL, TUNABLE_INTEGER, &gbl_rangextunit,
                 READONLY | NOZERO, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE(
    "rcache", "Keep a lookaside cache of root pages for B-trees. (Default: off)",
    TUNABLE_BOOLEAN, &gbl_rcache, READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("reallearly",
                 "Acknowledge as soon as a commit record is seen by the "
                 "replicant (before it's applied). This effectively makes "
                 "replication asynchronous, so reads may not see the effects "
                 "of a committed transaction yet. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_reallyearly, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("reject_osql_mismatch", "(Default: on)", TUNABLE_BOOLEAN,
                 &gbl_reject_osql_mismatch, READONLY | NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("repchecksum",
                 "Enable to perform additional checksumming of replication "
                 "stream. Note: Log records in replication stream already have "
                 "checksums. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_repchecksum, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("repdebug", "Enables replication debug messages.",
                 TUNABLE_BOOLEAN, &gbl_repdebug, READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("replicant_latches",
                 "Also acquire latches on replicants. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_replicant_latches, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("replicate_local",
                 "When enabled, record all database events to a comdb2_oplog "
                 "table. This can be used to set clusters/instances that are "
                 "fed data from a database cluster. Alternate ways of doing "
                 "this are being planned, so enabling this option should not "
                 "be needed in the near future. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_replicate_local, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("replicate_local_concurrent", NULL, TUNABLE_BOOLEAN,
                 &gbl_replicate_local_concurrent, READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("report_deadlock_verbose",
                 "If set, dump the current thread's stack for every deadlock. "
                 "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_disable_deadlock_trace, NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("deadlkon", "Same as 'report_deadlock_verbose'",
                 TUNABLE_BOOLEAN, &gbl_disable_deadlock_trace, NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("deadlkoff", "Disables 'report_deadlock_verbose'",
                 TUNABLE_BOOLEAN, &gbl_disable_deadlock_trace,
                 INVERSE_VALUE | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("rep_process_txn_trace",
                 "If set, report processing time on replicant for all "
                 "transactions. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_rep_process_txn_time, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("reqldiffstat", NULL, TUNABLE_INTEGER, &diffstat_thresh,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("reqltruncate", NULL, TUNABLE_INTEGER, &reqltruncate, READONLY,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("retry", NULL, TUNABLE_INTEGER, &db->retry, READONLY, NULL,
                 NULL, retry_update, NULL);
REGISTER_TUNABLE("round_robin_stripes",
                 "Alternate to which table stripe new records are written. The "
                 "default is to keep stripe affinity by writer. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_round_robin_stripes, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("rr_enable_count_changes", NULL, TUNABLE_BOOLEAN,
                 &gbl_rrenablecountchanges, READONLY | NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("sbuftimeout", NULL, TUNABLE_INTEGER, &gbl_sbuftimeout,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("sc_del_unused_files_threshold", NULL, TUNABLE_INTEGER,
                 &gbl_sc_del_unused_files_threshold_ms, READONLY | NOZERO, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("sequence_feature", "Enables support for SEQUENCES in column definitions (Default: ON)",
                 TUNABLE_BOOLEAN, &gbl_sequence_feature, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("simulate_rowlock_deadlock", NULL, TUNABLE_INTEGER,
                 &gbl_simulate_rowlock_deadlock_interval, 0, NULL, NULL,
                 simulate_rowlock_deadlock_update, NULL);
REGISTER_TUNABLE("singlemeta", NULL, TUNABLE_INTEGER, &gbl_init_single_meta,
                 READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("skip_clear_queue_extents", NULL, TUNABLE_BOOLEAN,
                 &skip_clear_queue_extents, READONLY | NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("slowfget", NULL, TUNABLE_INTEGER, &__slow_memp_fget_ns,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("slowread", NULL, TUNABLE_INTEGER, &__slow_read_ns, READONLY,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("slow_rep_process_txn_freq", NULL, TUNABLE_INTEGER,
                 &gbl_slow_rep_process_txn_freq, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("slow_rep_process_txn_maxms", NULL, TUNABLE_INTEGER,
                 &gbl_slow_rep_process_txn_maxms, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("slowwrite", NULL, TUNABLE_INTEGER, &__slow_write_ns, READONLY,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("sort_nulls_with_header",
                 "Using record headers in key sorting. (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_sort_nulls_correctly, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("spfile", NULL, TUNABLE_STRING, &gbl_spfile_name, READONLY,
                 NULL, NULL, file_update, NULL);
REGISTER_TUNABLE("timepartitions", NULL, TUNABLE_STRING,
                 &gbl_timepart_file_name, READONLY, NULL, NULL, file_update,
                 NULL);
REGISTER_TUNABLE("sqlflush", "Force flushing the current record "
                             "stream to client every specified "
                             "number of records. (Default: 0)",
                 TUNABLE_INTEGER, &gbl_sqlflush_freq, READONLY, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("sqlreadahead", NULL, TUNABLE_INTEGER, &gbl_sqlreadahead,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("sqlreadaheadthresh", NULL, TUNABLE_INTEGER,
                 &gbl_sqlreadaheadthresh, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("sqlsortermem", "Maximum amount of memory to be "
                                 "allocated to the sqlite sorter. "
                                 "(Default: 314572800)",
                 TUNABLE_INTEGER, &gbl_sqlite_sorter_mem, READONLY, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("sql_stat4_scan", "Possibly adjust the cost of a full table "
                                   "scan based on STAT4 data.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_sqlite_stat4_scan, READONLY | INTERNAL |
                 EXPERIMENTAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("sqlsortermult", NULL, TUNABLE_INTEGER, &gbl_sqlite_sortermult,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("sqlsorterpenalty",
                 "Sets the sorter penalty for query planner to prefer plans without explicit sort (Default: 5)",
                 TUNABLE_INTEGER, &gbl_sqlite_sorterpenalty, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("sql_time_threshold",
                 "Sets the threshold time in ms after which queries are "
                 "reported as running a long time. (Default: 5000 ms)",
                 TUNABLE_INTEGER, &gbl_sql_time_threshold, READONLY, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("sql_tranlevel_default",
                 "Sets the default SQL transaction level for the database.",
                 TUNABLE_ENUM, &gbl_sql_tranlevel_default, READONLY,
                 sql_tranlevel_default_value, NULL,
                 sql_tranlevel_default_update, NULL);
REGISTER_TUNABLE(
    "sqlwrtimeout",
    "Set timeout for writing to an SQL connection. (Default: 10000ms)",
    TUNABLE_INTEGER, &gbl_sqlwrtimeoutms, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("static_tag_blob_fix", NULL, TUNABLE_BOOLEAN,
                 &gbl_force_notnull_static_tag_blobs, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("surprise", NULL, TUNABLE_BOOLEAN, &gbl_surprise,
                 READONLY | NOARG, NULL, NULL, NULL, NULL);
/*
  Note: survive_n_master_swings' value < 0 was previously ignored without
  any error.
*/
REGISTER_TUNABLE("survive_n_master_swings",
                 "Have a node retry applying a transaction against a new "
                 "master this many times before giving up. (Default: 600)",
                 TUNABLE_INTEGER, &gbl_allow_bplog_restarts, READONLY, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("temptable_limit",
                 "Set the maximum number of temporary tables the database can "
                 "create. (Default: 8192)",
                 TUNABLE_INTEGER, &gbl_temptable_pool_capacity, READONLY, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("test_blob_race", NULL, TUNABLE_INTEGER, &gbl_test_blob_race,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("test_scindex_deadlock",
                 "Test index on expressions schema change deadlock",
                 TUNABLE_BOOLEAN, &gbl_test_scindex_deadlock, READONLY, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("test_sc_resume_race",
                 "Test race between schemachange resume and blockprocessor",
                 TUNABLE_BOOLEAN, &gbl_test_sc_resume_race, READONLY, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("throttlesqloverlog",
                 "On a full queue of SQL requests, dump the current thread "
                 "pool this often (in secs). (Default: 5sec)",
                 TUNABLE_INTEGER, &gbl_throttle_sql_overload_dump_sec, READONLY,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("toblock_net_throttle",
                 "Throttle writes in apply_changes. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_toblock_net_throttle, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("track_berk_locks", NULL, TUNABLE_INTEGER,
                 &gbl_berkdb_track_locks, READONLY | NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("db_lock_maxid_override", "Override berkley lock_maxid for "
                 "testing. (Default: 0)", TUNABLE_INTEGER,
                 &gbl_db_lock_maxid_override, EXPERIMENTAL | INTERNAL, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("udp", NULL, TUNABLE_BOOLEAN, &gbl_udp, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("unnatural_types", "Same as 'surprise'", TUNABLE_BOOLEAN,
                 &gbl_surprise, READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("update_delete_limit", NULL, TUNABLE_BOOLEAN,
                 &gbl_update_delete_limit, READONLY | NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("updategenids",
                 "Enable use of update genid scheme. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_updategenids, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("update_shadows_interval",
                 "Set to higher than 0 to update snaphots on every Nth "
                 "operation. (Default: 0, update on for every operation)",
                 TUNABLE_INTEGER, &gbl_update_shadows_interval, 0, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("upd_null_cstr_return_conv_err", NULL, TUNABLE_INTEGER,
                 &gbl_upd_null_cstr_return_conv_err, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("use_appsock_as_sqlthread", NULL, TUNABLE_INTEGER,
                 &gbl_use_appsock_as_sqlthread, READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("use_live_schema_change", NULL, TUNABLE_INTEGER,
                 &gbl_default_livesc, READONLY | NOARG, NULL, NULL, NULL, NULL);
/*
REGISTER_TUNABLE("use_llmeta", NULL, TUNABLE_INTEGER,
                 &gbl_use_llmeta, READONLY, NULL, NULL, NULL, NULL);
*/
REGISTER_TUNABLE("llmeta_deadlock_poll", "Max poll for llmeta on deadlock.  (Default: 0ms)", TUNABLE_INTEGER,
                 &gbl_llmeta_deadlock_poll, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("usenames", NULL, TUNABLE_BOOLEAN, &gbl_nonames,
                 INVERSE_VALUE | READONLY | NOARG | READEARLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("use_node_priority",
                 "Sets node priority for the db. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_use_node_pri, READONLY | NOARG, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("use_nondedicated_network", NULL, TUNABLE_BOOLEAN,
                 &_non_dedicated_subnet, READONLY | NOARG, NULL, NULL,
                 net_add_nondedicated_subnet, NULL);
/*
REGISTER_TUNABLE(
    "use_parallel_schema_change",
    "Scan stripes for a table in parallel during schema change. (Default: on)",
    TUNABLE_BOOLEAN, &gbl_default_sc_scanmode, READONLY, NULL, NULL, NULL,
    NULL);
*/

REGISTER_TUNABLE("use_planned_schema_change",
                 "Only change entities that need to change on a schema change. "
                 "Disable to always rebuild all data files and indices for the "
                 "changing table. (Default: 1)",
                 TUNABLE_INTEGER, &gbl_default_plannedsc, READONLY | NOARG,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("watchthreshold",
                 "Panic if node has been unhealty (unresponsive, out of resources, etc.) for more "
                 "than this many seconds. The default value is 60.",
                 TUNABLE_INTEGER, &gbl_watchdog_watch_threshold, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("ctrace_nlogs",
                 "When rolling trace files, keep this many. The older files "
                 "will have incrementing number suffixes (.1, .2, etc.). "
                 "(Default: 7)",
                 TUNABLE_INTEGER, &nlogs, READONLY | NOZERO, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("ctrace_rollat",
                 "Roll database debug trace file "
                 "($COMDB2_ROOT/var/log/cdb2/$dbname.trc.c) at specified size. "
                 "Set to 0 (default) to never roll.",
                 TUNABLE_INTEGER, &rollat, READONLY | NOZERO, NULL, NULL,
                 ctrace_set_rollat, NULL);
REGISTER_TUNABLE(
    "debugthreads",
    "If set to 'on' enables trace on thread events. (Default: off)",
    TUNABLE_BOOLEAN, &thread_debug, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE(
    "dumpthreadonexit",
    "If set to 'on' dump resources held by a thread on exit. (Default: off)",
    TUNABLE_BOOLEAN, &dump_resources_on_thread_exit, READONLY, NULL, NULL, NULL,
    NULL);
REGISTER_TUNABLE("stack_disable", NULL, TUNABLE_BOOLEAN, &gbl_walkback_enabled,
                 INVERSE_VALUE | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("stack_enable", NULL, TUNABLE_BOOLEAN, &gbl_walkback_enabled,
                 NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("stack_warn_threshold", NULL, TUNABLE_INTEGER, &gbl_warnthresh,
                 READONLY | NOZERO, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("ack_trace",
                 "Every second, produce trace for ack messages. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_ack_trace, READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("no_ack_trace", "Disables 'ack_trace'", TUNABLE_BOOLEAN,
                 &gbl_ack_trace, INVERSE_VALUE | READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("bdblock_debug", NULL, TUNABLE_BOOLEAN, &gbl_bdblock_debug,
                 READONLY | NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("debug.autoanalyze", "debug autoanalyze operations",
                 TUNABLE_BOOLEAN, &gbl_debug_aa, NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("debug.thdpool_queue_only",
                 "Force SQL query work items to be queued by the thread pool "
                 "even when a thread may be available.  (Default: 0)",
                 TUNABLE_BOOLEAN, &gbl_thdpool_queue_only,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("debug.random_sql_work_delayed",
                 "Force a random SQL query to be delayed 1/this many times.  "
                 "(Default: 0)", TUNABLE_INTEGER, &gbl_random_sql_work_delayed,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("debug.random_sql_work_rejected",
                 "Force a random SQL query to be rejected 1/this many times.  "
                 "(Default: 0)", TUNABLE_INTEGER, &gbl_random_sql_work_rejected,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("debug.osql_random_restart", "randomly restart osql operations",
                 TUNABLE_BOOLEAN, &gbl_osql_random_restart, NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("debug.toblock_random_deadlock_trans",
                 "return deadlock for a fraction of txns", TUNABLE_BOOLEAN,
                 &gbl_toblock_random_deadlock_trans, NOARG, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("debug.tmptbl_corrupt_mem",
                 "Deliberately corrupt memory before freeing", TUNABLE_BOOLEAN,
                 &gbl_debug_tmptbl_corrupt_mem, INTERNAL, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("debug.omit_dta_write",
                 "Deliberately corrupt insertion randomly to debug db_verify", TUNABLE_BOOLEAN,
                 &gbl_debug_omit_dta_write, INTERNAL, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("debug.omit_idx_write",
                 "Deliberately corrupt insertion randomly to debug db_verify", TUNABLE_BOOLEAN,
                 &gbl_debug_omit_idx_write, INTERNAL, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("debug.omit_blob_write",
                 "Deliberately corrupt insertion randomly to debug db_verify", TUNABLE_BOOLEAN,
                 &gbl_debug_omit_blob_write, INTERNAL, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE(
    "debug.skip_constraintscheck_on_insert",
    "Deliberately allow insertion without constraint check to debug db_verify",
    TUNABLE_BOOLEAN, &gbl_debug_skip_constraintscheck_on_insert, INTERNAL, NULL,
    NULL, NULL, NULL);
REGISTER_TUNABLE("debug.protobuf_connectmsg_dbname_check",
                 "Send the wrong dbname in the connect message to test that a node in the same cluster should not "
                 "connect in this case (Default: 0)",
                 TUNABLE_BOOLEAN, &gbl_debug_pb_connectmsg_dbname_check, INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("debug.protobuf_connectmsg_gibberish",
                 "Don't understand the new protobuf connnect message to test connecting with older versions of comdb2"
                 " (Default: 0)",
                 TUNABLE_BOOLEAN, &gbl_debug_pb_connectmsg_gibberish, INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("debug.omit_zap_on_rebuild",
                 "Omit zeroing out record on rebuild to test whether array types are already zeroed out after end of field"
                 " (Default: 0)", TUNABLE_BOOLEAN,
                 &gbl_debug_omit_zap_on_rebuild, INTERNAL, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("debug.txn_sleep",
                 "Sleep during a transaction to test transaction state systable", TUNABLE_INTEGER,
                 &gbl_debug_txn_sleep, INTERNAL, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE(
    "query_plan_percentage",
    "Alarm if the average cost per row of current query plan is n percent above the cost for different query plan."
    " (Default: 50)",
    TUNABLE_DOUBLE, &gbl_query_plan_percentage, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("max_plan_query_plans",
                 "Maximum number of plans to be placed into the query plan "
                 "hash for each fingerprint (Default: 20)",
                 TUNABLE_INTEGER, &gbl_query_plan_max_plans, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("bdboslog", NULL, TUNABLE_INTEGER, &gbl_namemangle_loglevel,
                 READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("deadlock_rep_retry_max", NULL, TUNABLE_INTEGER,
                 &max_replication_trans_retries, READONLY | NOZERO, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("logmsg", NULL, TUNABLE_COMPOSITE, NULL, INTERNAL | READEARLY,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("logmsg.level",
                 "All messages below this level will not be logged.",
                 TUNABLE_ENUM, NULL, READEARLY, logmsg_level_value, NULL,
                 logmsg_level_update, NULL);
REGISTER_TUNABLE(
    "logmsg.skiplevel",
    "Skip appending level information to the log message. This was added to "
    "keep up with the historical behavior (Default: on)",
    TUNABLE_BOOLEAN, NULL, NOARG | READEARLY | INVERSE_VALUE | INTERNAL,
    logmsg_prefix_level_value, NULL, logmsg_prefix_level_update, NULL);
REGISTER_TUNABLE("logmsg.syslog", "Log messages to syslog.", TUNABLE_BOOLEAN,
                 NULL, NOARG | READEARLY, logmsg_syslog_value, NULL,
                 logmsg_syslog_update, NULL);
REGISTER_TUNABLE("logmsg.timestamp", "Stamp all messages with timestamp.",
                 TUNABLE_BOOLEAN, NULL, NOARG | READEARLY,
                 logmsg_timestamp_value, NULL, logmsg_timestamp_update, NULL);
REGISTER_TUNABLE("logmsg.notimestamp", "Disables 'syslog.timestamp'.",
                 TUNABLE_BOOLEAN, NULL, INVERSE_VALUE | NOARG | READEARLY,
                 logmsg_timestamp_value, NULL, logmsg_timestamp_update, NULL);
REGISTER_TUNABLE("block_set_commit_genid_trace",
                 "Print trace when blocking set commit_genid. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_block_set_commit_genid_trace, INTERNAL,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("debug_random_prepare", "Prepare randomly. (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_random_prepare_commit, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("debug_all_prepare_commit", "Prepare all transactions. (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_all_prepare_commit, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("debug_all_prepare_abort", "Prepare and abort all transactions. (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_all_prepare_abort, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("debug_all_prepare_leak", "Prepare and leak all transactions. (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_all_prepare_leak, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("flush_on_prepare", "Flush replicant log on prepare. (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_flush_on_prepare, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("abort_on_unset_ha_flag",
                 "Abort in snap_uid_retry if ha is unset. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_abort_on_unset_ha_flag, INTERNAL, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("write_dummy_trace",
                 "Print trace when doing a dummy write. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_write_dummy_trace, INTERNAL, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("seed_genid", "Set genid-seed in hex for genid48 test.",
                 TUNABLE_STRING, NULL, EXPERIMENTAL | INTERNAL,
                 next_genid_value, NULL, genid_seed_update, NULL);
REGISTER_TUNABLE("abort_on_bad_upgrade",
                 "Abort in upgrade current-generation exceeds ctrl-gen.",
                 TUNABLE_BOOLEAN, &gbl_abort_on_incorrect_upgrade,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("poll_in_pgfree_recover", "Poll pgfree recovery handler.",
                 TUNABLE_BOOLEAN, &gbl_poll_in_pg_free_recover,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("rep_badgen_trace", "Trace on rep mismatched generations.",
                 TUNABLE_BOOLEAN, &gbl_rep_badgen_trace,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("dump_zero_coherency_ts", "Enable zero-coherency-ts trace.",
                 TUNABLE_BOOLEAN, &gbl_dump_zero_coherency_timestamp,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("allow_incoherent_sql", "Enable sql against incoherent nodes.",
                 TUNABLE_BOOLEAN, &gbl_allow_incoherent_sql,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("rep_process_msg_print_rc", "Print rc from rep_process_msg.",
                 TUNABLE_BOOLEAN, &gbl_rep_process_msg_print_rc,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("verbose_master_req",
                 "Print trace showing master-req protocol.", TUNABLE_BOOLEAN,
                 &gbl_verbose_master_req, EXPERIMENTAL | INTERNAL, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("verbose_send_cohlease",
                 "Print trace from lease-issue thread.", TUNABLE_BOOLEAN,
                 &gbl_verbose_send_coherency_lease, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("reset_on_unelectable_cluster", "Reset master if unelectable.",
                 TUNABLE_BOOLEAN, &gbl_reset_on_unelectable_cluster,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("dedup_rep_all_reqs", "Only allow a single rep-all on queue to the master. (Default: off)",
                 TUNABLE_INTEGER, &gbl_dedup_rep_all_reqs, EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("decoupled_logputs",
                 "Perform logputs out-of-band. (Default: on)", TUNABLE_BOOLEAN,
                 &gbl_decoupled_logputs, EXPERIMENTAL | INTERNAL, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("apply_pollms",
                 "Apply-thread poll time before checking queue. "
                 "(Default: 100ms)",
                 TUNABLE_INTEGER, &gbl_apply_thread_pollms,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("rep_verify_always_grab_writelock",
                 "Force every rep_verify to grab writelock.", TUNABLE_BOOLEAN,
                 &gbl_rep_verify_always_grab_writelock, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("rep_verify_will_recover_trace",
                 "Trace rep_verify_will_recover.", TUNABLE_BOOLEAN,
                 &gbl_rep_verify_will_recover_trace, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("max_wr_rows_per_txn",
                 "Set the max written rows per transaction.", TUNABLE_INTEGER,
                 &gbl_max_wr_rows_per_txn, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("written_rows_warn",
                 "Set warning threshold for rows written in a transaction.  "
                 "(Default: 0)",
                 TUNABLE_INTEGER, &gbl_written_rows_warn, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("max_cascaded_rows_per_txn",
                 "Set the max cascaded rows updated per transaction.", TUNABLE_INTEGER,
                 &gbl_max_cascaded_rows_per_txn, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("max_time_per_txn_ms",
                 "Set the max time allowed for transaction to finish", TUNABLE_INTEGER,
                 &gbl_max_time_per_txn_ms, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("print_deadlock_cycles",
                 "Print every Nth deadlock cycle, set to 0 to turn off. (Default: 100)", TUNABLE_INTEGER,
                 &gbl_print_deadlock_cycles, NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("always_send_cnonce",
                 "Always send cnonce to master. (Default: on)", TUNABLE_BOOLEAN,
                 &gbl_always_send_cnonce, NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("force_serial_on_writelock", "Disable parallel rep on "
                                              "upgrade.  (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_force_serial_on_writelock,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("processor_thd_poll", "Poll before dispatching worker thds. "
                                       "(Default: 0ms)",
                 TUNABLE_INTEGER, &gbl_processor_thd_poll,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("time_rep_apply", "Display rep-apply times periodically. "
                                   "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_time_rep_apply, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("logput_window",
                 "Drop log-broadcasts for incoherent nodes "
                 "more than this many bytes behind.  (Default: 0)",
                 TUNABLE_INTEGER, &gbl_incoherent_logput_window,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("dump_full_netqueue", "Dump net-queue on full rcode. "
                                       "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_dump_full_net_queue,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("dump_net_queue_on_partial_write",
                 "Dump net-queue info on partial write. (Default: off):",
                 TUNABLE_BOOLEAN, &gbl_dump_net_queue_on_partial_write,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("debug_partial_write", "Simulate partial write in net.  "
                 "(Default: 0)", TUNABLE_INTEGER, &gbl_debug_partial_write,
                 EXPERIMENTAL | INTERNAL, NULL,NULL, NULL, NULL);
REGISTER_TUNABLE("debug_verify_sleep", "Sleep one-second per record in verify.  "
                 "(Default: off)", TUNABLE_BOOLEAN, &gbl_debug_sleep_on_verify,
                 EXPERIMENTAL | INTERNAL, NULL,NULL, NULL, NULL);
REGISTER_TUNABLE("debug_drop_nth_rep_message", "Drop the Nth replication message "
                 "for testing purposes (Default: 0)", TUNABLE_INTEGER,
                 &gbl_debug_drop_nth_rep_message, EXPERIMENTAL | INTERNAL, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE(
    "max_clientstats",
    "Max number of client stats stored in comdb2_clientstats. (Default: 10000)",
    TUNABLE_INTEGER, &gbl_max_clientstats_cache, 0, NULL, NULL, NULL,
    NULL);
REGISTER_TUNABLE("max_logput_queue",
                 "Maximum queued log-records.  (Default: 100000)",
                 TUNABLE_INTEGER, &gbl_max_logput_queue,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("master_req_waitms",
                 "Request master once per this interval.  (Default: 200ms)",
                 TUNABLE_INTEGER, &gbl_master_req_waitms,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE(
    "master_sends_query_effects",
    "Enables master to send query effects to the replicant. (Default: on)",
    TUNABLE_BOOLEAN, &gbl_master_sends_query_effects, NOARG | READONLY, NULL,
    NULL, NULL, NULL);
REGISTER_TUNABLE("req_all_threshold",
                 "Use req_all if a replicant is behind by "
                 "this amount or more.  (Default: 1048476)",
                 TUNABLE_INTEGER, &gbl_req_all_threshold,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("req_all_time_threshold",
                 "Use req_all if a replicant hasn't updated its "
                 "lsn in more than this many ms.  (Default: 0)",
                 TUNABLE_INTEGER, &gbl_req_all_time_threshold,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("file_permissions",
                 "Default filesystem permissions for database files. (Default: 0660)",
                 TUNABLE_STRING, NULL, 0, file_permissions_value, NULL,
                 file_permissions_update, NULL);

REGISTER_TUNABLE("fill_throttle",
                 "Throttle fill-reqs to once per fill-throttle ms.  "
                 "(Default: 500ms)",
                 TUNABLE_INTEGER, &gbl_fills_waitms, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("verbose_fills", "Print fill trace.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_verbose_fills, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("verbose_repdups",
                 "Print trace on duplicate replication.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_verbose_repdups, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("warn_queue_latency",
                 "Trace for log queues processed that are older than this.  "
                 "(Default: 500ms)",
                 TUNABLE_INTEGER, &gbl_warn_queue_latency_threshold,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("print_net_queue_size",
                 "Trace for net queue size.  (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_print_net_queue_size, EXPERIMENTAL | INTERNAL, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("verbose_repmore_trace",
                 "Verbose trace for rep-more requests.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_trace_repmore_reqs,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("throttle_logput_trace",
                 "Print trace when stopping logputs "
                 "to incoherent nodes.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_throttle_logput_trace,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("catchup_window_trace",
                 "Print master catchup window trace.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_catchup_window_trace,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("early_ack_trace",
                 "Print trace when sending an early ack.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_early_ack_trace, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("commit_delay_timeout_seconds",
                 "Set timeout for commit-delay on copy.  (Default: 10)",
                 TUNABLE_INTEGER, &gbl_commit_delay_timeout, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("commit_delay_on_copy_ms",
                 "Set automatic delay-ms for commit-delay on copy.  (Default: 0)",
                 TUNABLE_INTEGER, &gbl_commit_delay_copy_ms, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("commit_delay_trace", "Verbose commit-delays.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_commit_delay_trace,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("commit_lsn_map", "Maintain a map of transaction commit LSNs. (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_commit_lsn_map,
                 NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("set_coherent_state_trace",
                 "Verbose coherency trace.  (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_set_coherent_state_trace, EXPERIMENTAL | INTERNAL, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("finish_fill_threshold",
                 "Fill to end if end is less than this.  (Default: 60000000)",
                 TUNABLE_INTEGER, &gbl_finish_fill_threshold,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("req_delay_count_threshold",
                 "Request commit-delay if falling "
                 "behind this many times.  (Default: 5)",
                 TUNABLE_INTEGER, &gbl_req_delay_count_threshold,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("max_apply_dequeue",
                 "Limit apply-processing to this many per "
                 "loop.  this many times.  (Default: 100000)",
                 TUNABLE_INTEGER, &gbl_max_apply_dequeue,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("last_locked_seqnum",
                 "Broadcast last-locked variable as seqnum.  (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_last_locked_seqnum,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("rep_getlock_latency",
                 "Sleep on replicant before getting locks.  (Default: 0)",
                 TUNABLE_INTEGER, &gbl_getlock_latencyms,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("net_writer_poll_ms",
                 "Poll time for net writer thread.  (Default: 1000)",
                 TUNABLE_INTEGER, &gbl_net_writer_thread_poll_ms,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("inmem_repdb",
                 "Use in memory structure for repdb (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_inmem_repdb,
                 EXPERIMENTAL | INTERNAL | READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("inmem_repdb_maxlog",
                 "Maximum records for in-memory replist.  "
                 "(Default: 10000)",
                 TUNABLE_INTEGER, &gbl_inmem_repdb_maxlog,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("durable_set_trace",
                 "Trace setting durable lsn.  (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_durable_set_trace, EXPERIMENTAL | INTERNAL, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("set_seqnum_trace",
                 "Trace setting setting seqnum.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_set_seqnum_trace,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("elect_priority_bias",
                 "Bias this node's election priority by this amount.  "
                 "(Default: 0)",
                 TUNABLE_INTEGER, &gbl_elect_priority_bias, 0, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("apply_queue_memory",
                 "Current memory usage of apply-queue.  (Default: 0)",
                 TUNABLE_INTEGER, &gbl_apply_queue_memory, READONLY, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("inmem_repdb_memory",
                 "Current memory usage of in-memory repdb.  (Default: 0)",
                 TUNABLE_INTEGER, &gbl_inmem_repdb_memory, READONLY, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("queuedb_genid_filename",
                 "Use genid in queuedb filenames.  (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_queuedb_genid_filename, READONLY, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("queuedb_file_threshold",
                 "Maximum queuedb file size (in MB) before enqueueing to the "
                 "alternate file.  (Default: 0)", TUNABLE_INTEGER,
                 &gbl_queuedb_file_threshold, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("queuedb_file_interval",
                 "Check on this interval each queuedb against its configured "
                 "maximum file size. (Default: 60000ms)", TUNABLE_INTEGER,
                 &gbl_queuedb_file_interval, READONLY, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("random_election_timeout",
                 "Use a random timeout in election.  (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_rand_elect_timeout,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("random_elect_min_ms",
                 "Minimum election timeout.  (Default: 1000)", TUNABLE_INTEGER,
                 &gbl_rand_elect_min_ms, EXPERIMENTAL | INTERNAL, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("random_elect_max_ms",
                 "Maximum election timeout.  (Default: 7000)", TUNABLE_INTEGER,
                 &gbl_rand_elect_max_ms, EXPERIMENTAL | INTERNAL, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("legacy_defaults", "Configure server with legacy defaults",
                 TUNABLE_BOOLEAN, &gbl_legacy_defaults,
                 NOARG | INTERNAL | READONLY | READEARLY,
                 NULL, NULL, pre_read_legacy_defaults, NULL);
REGISTER_TUNABLE("abort_on_reconstruct_failure",
                 "Abort database if snapshot fails to reconstruct a record.  "
                 "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_abort_on_reconstruct_failure,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("netconndumptime",
                 "Dump connection statistics to ctrace this often.",
                 TUNABLE_INTEGER, NULL, 0, netconndumptime_value, NULL, 
                 netconndumptime_update, NULL);

REGISTER_TUNABLE("timeseries_metrics_maxpoints",
                 "Maximum data points to keep in memory for various metrics",
                 TUNABLE_INTEGER, &gbl_metric_maxpoints, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("timeseries_metrics_maxage",
                 "Time to keep metrics in memory (seconds)",
                 TUNABLE_INTEGER, &gbl_metric_maxage, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("timeseries_metrics",
                 "Keep time series data for some metrics",
                 TUNABLE_BOOLEAN, &gbl_timeseries_metrics, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("handle_buf_latency_ms",
                 "Add up to this much artificial latency to handle-buf.  "
                 "(Default: 0)",
                 TUNABLE_INTEGER, &gbl_handle_buf_add_latency_ms,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("queuedb_timeout_sec",
                 "Unassign Lua consumer/trigger if no heartbeat received for this time",
                 TUNABLE_INTEGER, &gbl_queuedb_timeout_sec, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("osql_send_startgen",
                 "Send start-generation in osql stream. (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_osql_send_startgen,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("client_heartbeat_ms",
                 "Number of milliseconds between client api heartbeats.  "
                 "(Default: 100)",
                 TUNABLE_INTEGER, &gbl_client_heartbeat_ms,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("rep_release_wait_ms",
                 "Release sql-locks if rep-thd is blocked for this many ms."
                 "  (Default: 60000)",
                 TUNABLE_INTEGER, &gbl_rep_wait_release_ms,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("rep_wait_core_ms",
                 "Abort if rep-thread waits longer than this threshold for "
                 "locks.  (Default: 0)",
                 TUNABLE_INTEGER, &gbl_rep_wait_core_ms,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("rep_mon_threshold",
                 "Pstack rep-thread if blocked this long.  (Default: 0)",
                 TUNABLE_INTEGER, &gbl_rep_mon_threshold,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("random_get_curtran_failures",
                 "Force a random get-curtran failure 1/this many times.  "
                 "(Default: 0)",
                 TUNABLE_INTEGER, &gbl_random_get_curtran_failures,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("dont_block_delete_files_thread", "Ignore files that would block delete-files thread.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_txn_fop_noblock, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("debug_random_fop_block", "Randomly return .  (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_debug_random_block_on_fop, EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("random_thdpool_work_timeout",
                 "Force a random thread pool work item timeout 1/this many "
                 "times.  (Default: 0)",
                 TUNABLE_INTEGER, &gbl_random_thdpool_work_timeout,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("dohsql_disable",
                 "Disable running queries in distributed mode", TUNABLE_BOOLEAN,
                 &gbl_dohsql_disable, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("dohsql_verbose",
                 "Run distributed queries in verbose/debug mode",
                 TUNABLE_BOOLEAN, &gbl_dohsql_verbose, 0, NULL, NULL, NULL,
                 NULL);

REGISTER_TUNABLE("dohast_disable",
                 "Disable generating AST for queries. This disables "
                 "distributed mode as well.",
                 TUNABLE_BOOLEAN, &gbl_dohast_disable, 0, NULL, NULL, NULL,
                 NULL);

REGISTER_TUNABLE("dohast_verbose",
                 "Print debug information when creating AST for statements",
                 TUNABLE_BOOLEAN, &gbl_dohast_verbose, 0, NULL, NULL, NULL,
                 NULL);

REGISTER_TUNABLE("dohsql_max_queued_kb_highwm",
                 "Maximum shard queue size, in KB; shard sqlite will pause "
                 "once queued bytes limit is reached.",
                 TUNABLE_INTEGER, &gbl_dohsql_max_queued_kb_highwm, 0, NULL,
                 NULL, NULL, NULL);

REGISTER_TUNABLE(
    "dohsql_max_threads",
    "Maximum number of parallel threads, otherwise run sequential.",
    TUNABLE_INTEGER, &gbl_dohsql_max_threads, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE(
    "dohsql_pool_thread_slack",
    "Forbid parallel sql coordinators from running on this many sql engines"
    " (if 0, defaults to 24).",
    TUNABLE_INTEGER, &gbl_dohsql_pool_thr_slack, NOZERO, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE(
    "dohsql_full_queue_poll_msec",
    "Poll milliseconds while waiting for coordinator to consume from queue.",
    TUNABLE_INTEGER, &gbl_dohsql_full_queue_poll_msec, 0, NULL, NULL, NULL,
    NULL);

REGISTER_TUNABLE("random_fail_client_write_lock",
                 "Force a random client write-lock failure 1/this many times.  "
                 "(Default: 0)",
                 TUNABLE_INTEGER, &gbl_fail_client_write_lock,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("reorder_socksql_no_deadlock", "Reorder sock sql to have no deadlocks ",
                 TUNABLE_BOOLEAN, &gbl_reorder_socksql_no_deadlock, EXPERIMENTAL,
                 NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("reorder_idx_writes", "reorder_idx_writes",
                 TUNABLE_BOOLEAN, &gbl_reorder_idx_writes, EXPERIMENTAL,
                 NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("disable_tpsc_tblvers",
                 "Disable table version checks for time partition schema "
                 "changes",
                 TUNABLE_BOOLEAN, &gbl_disable_tpsc_tblvers, NOARG, NULL, NULL,
                 NULL, NULL);

REGISTER_TUNABLE("abort_irregular_set_durable_lsn",
                 "Abort incorrect calls to set_durable_lsn. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_abort_irregular_set_durable_lsn,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("instrument_dblist",
                 "Extended dblist-trace in berkley.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_instrument_dblist,
                 READONLY | EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("match_on_ckp",
                 "Allow rep_verify_match on ckp records.  (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_match_on_ckp, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);

/* physical replication */
REGISTER_TUNABLE("blocking_physrep",
                 "Physical replicant blocks on select. (Default: false)",
                 TUNABLE_BOOLEAN, &gbl_blocking_physrep, 0, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("physrep_check_minlog_freq_sec",
                 "Check the minimum log number to keep this often. (Default: 10)",
                 TUNABLE_INTEGER, &gbl_physrep_check_minlog_freq_sec, 0, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("physrep_debug",
                 "Print extended physrep trace. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_physrep_debug, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("physrep_exit_on_invalid_logstream", "Exit physreps on invalid logstream.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_physrep_exit_on_invalid_logstream, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("physrep_fanout",
                 "Maximum number of physical replicants that a node can service (Default: 8)",
                 TUNABLE_INTEGER, &gbl_physrep_fanout, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("physrep_hung_replicant_check_freq_sec",
                 "Check for hung physical replicant this often. (Default: 10)",
                 TUNABLE_INTEGER, &gbl_physrep_hung_replicant_check_freq_sec, 0, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("physrep_hung_replicant_threshold",
                 "Report if the physical replicant has been inactive for this duration. (Default: 60)",
                 TUNABLE_INTEGER, &gbl_physrep_hung_replicant_threshold, 0, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("physrep_i_am_metadb", "I am physical replication metadb (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_physrep_i_am_metadb, NOARG, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("physrep_keepalive_freq_sec",
                 "Periodically send lsn to source node after this interval. (Default: 10)",
                 TUNABLE_INTEGER, &gbl_physrep_keepalive_freq_sec, 0, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("physrep_max_candidates",
                 "Maximum number of candidates that should be returned to a "
                 "new physical replicant during registration. (Default: 6)",
                 TUNABLE_INTEGER, &gbl_physrep_max_candidates, 0, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("physrep_max_pending_replicants",
                 "There can be no more than this many physical replicants in "
                 "pending state. (Default: 10)",
                 TUNABLE_INTEGER, &gbl_physrep_max_pending_replicants, 0, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("physrep_metadb_host", "List of physical replication metadb cluster hosts.",
                 TUNABLE_STRING, &gbl_physrep_metadb_host, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("physrep_metadb_name", "Physical replication metadb cluster name.",
                 TUNABLE_STRING, &gbl_physrep_metadb_name, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("physrep_reconnect_penalty",
                 "Physrep wait seconds before retry to the same node. (Default: 5)",
                 TUNABLE_INTEGER, &gbl_physrep_reconnect_penalty, 0, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("physrep_register_interval",
                 "Interval for physical replicant re-registration. (Default: 3600)",
                 TUNABLE_INTEGER, &gbl_physrep_register_interval, 0, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("physrep_shuffle_host_list",
                 "Shuffle the host list returned by register_replicant() "
                 "before connecting to the hosts. (Default: OFF)",
                 TUNABLE_BOOLEAN, &gbl_physrep_shuffle_host_list, 0, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("physrep_source_dbname", "Physical replication source cluster dbname.",
                 TUNABLE_STRING, &gbl_physrep_source_dbname, READONLY, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("physrep_source_host", "List of physical replication source cluster hosts.",
                 TUNABLE_STRING, &gbl_physrep_source_host, READONLY, NULL, NULL, NULL,
                 NULL);

/* reversql-sql */
REGISTER_TUNABLE("revsql_allow_command_execution",
                 "Allow processing and execution of command over the 'reverse connection' "
                 "that has come in as part of the request. This is mostly intended for "
                 "testing. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_revsql_allow_command_exec, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("revsql_cdb2_debug",
                 "Print extended reversql-sql cdb2 related trace. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_revsql_cdb2_debug, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("revsql_connect_freq_sec", "This node will attempt to (reverse) "
                 "connect to the remote host at this frequency. (Default: 5secs)",
                 TUNABLE_INTEGER, &gbl_revsql_connect_freq_sec, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("revsql_debug",
                 "Print extended reversql-sql trace. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_revsql_debug, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("revsql_host_refresh_freq_sec", "The frequency at which the "
                 "reverse connection host list will be refreshed (Default: 5secs)",
                 TUNABLE_INTEGER, &gbl_revsql_host_refresh_freq_sec, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("logdelete_lock_trace",
                 "Print trace getting and releasing the logdelete lock.  "
                 "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_logdelete_lock_trace,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("flush_log_at_checkpoint",
                 "Replicants flush the log at checkpoint records.  "
                 "(Default: on)",
                 TUNABLE_BOOLEAN, &gbl_flush_log_at_checkpoint,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("verbose_set_sc_in_progress",
                 "Prints a line of trace when sc_in_progress is set.  "
                 "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_verbose_set_sc_in_progress,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("send_failed_dispatch_message",
                 "Send explicit failed-dispatch message to the api.  "
                 "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_send_failed_dispatch_message,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("legacy_schema", "Only allow legacy compatible csc2 schema",
                 TUNABLE_BOOLEAN, &gbl_legacy_schema,
                 EXPERIMENTAL | INTERNAL | READEARLY, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("force_incoherent",
                 "Force this node to be incoherent.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_force_incoherent,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("ignore_coherency",
                 "Force this node to be coherent.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_ignore_coherency,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("forbid_incoherent_writes",
                 "Prevent writes against a node which was incoherent at "
                 "transaction start.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_forbid_incoherent_writes,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("skip_catchup_logic",
                 "Skip initial catchup logic.  (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_skip_catchup_logic, EXPERIMENTAL | INTERNAL, NULL, NULL,
                 NULL, NULL);

REGISTER_TUNABLE("protobuf_connectmsg", "Use protobuf in net library for the connect message. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_pb_connectmsg, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("libevent", "Use libevent in net library. (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_libevent, READONLY, 0, 0, 0, 0);

REGISTER_TUNABLE("libevent_appsock", "Use libevent for appsock connections. (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_libevent_appsock, READONLY, 0, 0, 0, 0);

REGISTER_TUNABLE("libevent_rte_only", "Prevent listening on TCP socket. (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_libevent_rte_only, READONLY, 0, 0, 0, 0);

REGISTER_TUNABLE("online_recovery",
                 "Don't get the bdb-writelock for recovery.  (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_online_recovery, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("forbid_remote_admin",
                 "Forbid non-local admin requests.  (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_forbid_remote_admin, 0, NULL, NULL, NULL,
                 NULL);

REGISTER_TUNABLE("abort_on_dta_lookup_error",
                 "Abort on dta lookup lost the race.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_abort_on_dta_lookup_error,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE(
    "pbkdf2_iterations",
    "Number of iterations of PBKDF2 algorithm for password hashing.",
    TUNABLE_INTEGER, &gbl_pbkdf2_iterations, NOZERO | SIGNED, NULL, NULL,
    pbkdf2_iterations_update, NULL);

REGISTER_TUNABLE("kafka_topic", NULL, TUNABLE_STRING, &gbl_kafka_topic,
                 READONLY | READEARLY, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("kafka_brokers", NULL, TUNABLE_STRING, &gbl_kafka_brokers,
                 READONLY | READEARLY, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("machine_class",
                 "override for the machine class from this db perspective.",
                 TUNABLE_STRING, &gbl_machine_class, READEARLY | READONLY, NULL,
                 NULL, NULL, NULL);

REGISTER_TUNABLE("selectv_writelock_on_update",
                 "Acquire a writelock for updated selectv records.  "
                 "(Default: on)",
                 TUNABLE_BOOLEAN, &gbl_selectv_writelock_on_update,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("selectv_writelock",
                 "Acquire a writelock for selectv records.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_selectv_writelock,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("perform_full_clean_exit",
                 "Perform full clean exit on exit signal (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_perform_full_clean_exit,
                 NOARG, NULL, NULL, NULL, NULL);


REGISTER_TUNABLE("clean_exit_on_sigterm",
                 "Attempt to do orderly shutdown on SIGTERM (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_clean_exit_on_sigterm,
                 NOARG, NULL, NULL, update_clean_exit_on_sigterm, NULL);

REGISTER_TUNABLE("msgwaittime",
                 "Network timeout for pushnext & queue changes.  (Default: 10000)",
                 TUNABLE_INTEGER, &gbl_msgwaittime, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("scwaittime",
                 "Network timeout for schema changes.  (Default: 1000)",
                 TUNABLE_INTEGER, &gbl_scwaittime, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("debug_children_lock",
                 "Stacktrace when database acquires or releases children lock."
                 "  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_debug_children_lock,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("serialize_reads_like_writes",
                 "Send read-only multi-statement schedules to the master.  "
                 "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_serialize_reads_like_writes, 0, NULL,
                 NULL, NULL, NULL);

REGISTER_TUNABLE("long_log_truncation_warn_thresh_sec",
                 "Warn if log truncation takes more than this many seconds."
                 "  (Default: 2147483647)",
                 TUNABLE_INTEGER, &gbl_long_log_truncation_warn_thresh_sec,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("long_log_truncation_abort_thresh_sec",
                 "SIGABRT if log truncation takes more than this many seconds."
                 "  (Default: 2147483647)",
                 TUNABLE_INTEGER, &gbl_long_log_truncation_abort_thresh_sec,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("cache_flush_interval",
                 "Save bufferpool once every this many seconds.  "
                 "(Default: 30)",
                 TUNABLE_INTEGER, &gbl_cache_flush_interval, 0, NULL, NULL,
                 NULL, NULL);

REGISTER_TUNABLE("load_cache_threads",
                 "Number of threads loading pages to cache.  "
                 "(Default: 8)",
                 TUNABLE_INTEGER, &gbl_load_cache_threads, 0, NULL, NULL, NULL,
                 NULL);

REGISTER_TUNABLE("load_cache_max_pages",
                 "Maximum number of pages that will load into cache.  Setting "
                 "to 0 means that there is no limit.  (Default: 0)",
                 TUNABLE_INTEGER, &gbl_load_cache_max_pages, 0, NULL, NULL,
                 NULL, NULL);

REGISTER_TUNABLE("dump_cache_max_pages",
                 "Maximum number of pages that will dump into a pagelist.  "
                 "Setting to 0 means that there is no limit.  (Default: 0)",
                 TUNABLE_INTEGER, &gbl_dump_cache_max_pages, 0, NULL, NULL,
                 NULL, NULL);

REGISTER_TUNABLE("max_pages_per_cache_thread",
                 "Number of threads loading pages to cache.  "
                 "(Default: 8192)",
                 TUNABLE_INTEGER, &gbl_max_pages_per_cache_thread, INTERNAL,
                 NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("memp_dump_cache_threshold",
                 "Don't flush the cache until this percentage of pages have "
                 "changed.  (Default: 20)",
                 TUNABLE_INTEGER, &gbl_memp_dump_cache_threshold, 0, NULL, NULL,
                 NULL, NULL);

REGISTER_TUNABLE("snapshot_serial_verify_retry",
                 "Automatic retries on verify errors for clients that haven't "
                 "read results.  (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_snapshot_serial_verify_retry, 0, NULL,
                 NULL, NULL, NULL);

REGISTER_TUNABLE("strict_double_quotes",
                 "In SQL queries, forbid the use of double-quotes to denote "
                 "a string literal.  Any attempts to do so will result in a "
                 "syntax error (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_strict_dbl_quotes, EXPERIMENTAL | INTERNAL, NULL, NULL,
                 NULL, NULL);

REGISTER_TUNABLE("longreq_log_freq_sec",
                 "Log information about long running statements at this frequency"
                 " (Default: 60sec)",
                 TUNABLE_INTEGER, &gbl_longreq_log_freq_sec, 0, NULL, NULL,
                 NULL, NULL);

REGISTER_TUNABLE("eventlog_nkeep", "Keep this many eventlog files (Default: 2)",
                 TUNABLE_INTEGER, &eventlog_nkeep, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("waitalive_iterations",
                 "Wait this many iterations for a "
                 "socket to be usable.  (Default: 3)",
                 TUNABLE_INTEGER, &gbl_waitalive_iterations,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("disable_ckp", "Disable checkpoints to debug.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_disable_ckp, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("ufid_log", "Generate ufid logs.  (Default: off)", TUNABLE_BOOLEAN, &gbl_ufid_log,
                 EXPERIMENTAL | INTERNAL | READONLY, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("utxnid_log", "Generate utxnid logs. (Default: on)", TUNABLE_BOOLEAN, &gbl_utxnid_log,
				 NOARG|READEARLY, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("ufid_add_on_collect", "Add to ufid-hash on collect.  (Default: off)", TUNABLE_BOOLEAN, 
                 &gbl_ufid_add_on_collect, EXPERIMENTAL | INTERNAL | READONLY, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("collect_before_locking", "Collect a transaction from the log before acquiring locks.  (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_collect_before_locking, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("debug_ddlk", "Generate random deadlocks.  (Default: 0)", TUNABLE_INTEGER, &gbl_ddlk,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("abort_on_missing_ufid", "Abort if ufid is not found.  (Default: off)", 
                 TUNABLE_BOOLEAN, &gbl_abort_on_missing_ufid, EXPERIMENTAL | INTERNAL | READONLY,
                 NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("ufid_dbreg_test", "Enable ufid-dbreg test.  (Default: off)", TUNABLE_BOOLEAN, &gbl_ufid_dbreg_test,
                 EXPERIMENTAL | INTERNAL | READONLY, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("javasp_early_release", "Release javasp-lock before distributed commit.  (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_javasp_early_release, EXPERIMENTAL | INTERNAL, 
                 NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("debug_add_replication_latency", "Sleep after distributed commit.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_debug_add_replication_latency, EXPERIMENTAL | INTERNAL, 
                 NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("ref_sync_pollms",
                 "Set pollms for ref_sync thread.  "
                 "(Default: 250)",
                 TUNABLE_INTEGER, &gbl_ref_sync_pollms, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("ref_sync_iterations",
                 "Set iterations for ref_sync thread.  "
                 "(Default: 4)",
                 TUNABLE_INTEGER, &gbl_ref_sync_iterations,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("ref_sync_wait_txnlist",
                 "Wait for running txns to complete on sync failure.  "
                 "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_ref_sync_wait_txnlist,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("sc_close_txn",
                 "Use separate close txn in schemachange.  "
                 "(Default: on)",
                 TUNABLE_BOOLEAN, &gbl_sc_close_txn, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("abort_on_illegal_log_put",
                 "Abort if replicant log_puts.  "
                 "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_abort_on_illegal_log_put,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("sc_pause_at_end",
                 "Pause schema-change after converters have completed.  "
                 "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_sc_pause_at_end, EXPERIMENTAL | INTERNAL,
                 NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("sc_is_at_end",
                 "Schema-change has converted all records.  "
                 "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_sc_is_at_end, EXPERIMENTAL, NULL, NULL,
                 NULL, NULL);

REGISTER_TUNABLE("cached_output_buffer_max_bytes",
                 "Maximum size in bytes of the output buffer of an appsock "
                 "thread.  (Default: 8 MiB)",
                 TUNABLE_INTEGER, &gbl_cached_output_buffer_max_bytes, 0, NULL,
                 NULL, NULL, NULL);

REGISTER_TUNABLE("debug_queuedb",
                 "Enable debug-trace for queuedb.  "
                 "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_debug_queuedb, EXPERIMENTAL, NULL, NULL,
                 NULL, NULL);

REGISTER_TUNABLE("lua_prepare_retries",
                 "Maximum number of times to retry SQL query preparation "
                 "when faced with 'database schema has changed' errors in "
                 "the Lua subsystem.  (Default: 0)", TUNABLE_INTEGER,
                 &gbl_lua_prepare_max_retries, EXPERIMENTAL | INTERNAL, NULL,
                 NULL, NULL, NULL);

REGISTER_TUNABLE("lua_prepare_retry_sleep",
                 "The number of milliseconds in between SQL query preparation "
                 "retries in the Lua subsystem.  (Default: 200)",
                 TUNABLE_INTEGER, &gbl_lua_prepare_retry_sleep,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("dump_sql_on_repwait_sec",
                 "Dump sql queries that are blocking the replication thread "
                 "for more than this duration (Default: 10secs)",
                 TUNABLE_INTEGER, &gbl_dump_sql_on_repwait_sec, EXPERIMENTAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("client_queued_slow_seconds",
                 "If a client connection remains \"queued\" longer than this "
                 "period of time (in seconds), it is considered to be \"slow\", "
                 "which may trigger an action by the watchdog.  (Default: off)",
                 TUNABLE_INTEGER, &gbl_client_queued_slow_seconds,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("client_running_slow_seconds",
                 "If a client connection remains \"running\" longer than this "
                 "period of time (in seconds), it is considered to be \"slow\", "
                 "which may trigger an action by the watchdog.  (Default: off)",
                 TUNABLE_INTEGER, &gbl_client_running_slow_seconds,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("client_abort_on_slow",
                 "Enable watchdog to abort if a \"slow\" client is detected."
                 "  (Default: off)", TUNABLE_BOOLEAN, &gbl_client_abort_on_slow,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("test_log_file",
                 "Dedicated log file for use by the test suite only.  "
                 "(Default: off)", TUNABLE_STRING, &gbl_test_log_file,
                 EXPERIMENTAL | INTERNAL | READEARLY, NULL, NULL,
                 test_log_file_update, NULL);

REGISTER_TUNABLE(
    "max_password_cache_size",
    "Password cache size, set to <=0 to turn off caching (Default: 100)",
    TUNABLE_INTEGER, &gbl_max_password_cache_size, 0, NULL, NULL,
    max_password_cache_size_update, NULL);

REGISTER_TUNABLE("debug_systable_locks",
                 "Grab the comdb2_systables lock in every schema change.  "
                 "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_debug_systable_locks, EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("assert_systable_locks",
                 "Assert that schema change holds the correct locks on replicants.  "
                 "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_assert_systable_locks, EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("track_curtran_gettran_locks", "Stack-trace curtran_gettran threads at lock-get.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_track_curtran_gettran_locks, EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("permit_small_sequences", "Allow int32 and int16 length sequences.  (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_permit_small_sequences, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("max_trigger_threads", "Maximum number of trigger threads allowed", TUNABLE_INTEGER,
                 &gbl_max_trigger_threads, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("test_fdb_io", "Testing fail mode remote sql.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_test_io_errors, INTERNAL, NULL, NULL,
                 NULL, NULL);

REGISTER_TUNABLE("debug_sleep_in_sql_tick", "Sleep for a second in sql tick.  (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_debug_sleep_in_sql_tick, INTERNAL | EXPERIMENTAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("debug_sleep_in_analyze", "Sleep analyze sql tick.  (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_debug_sleep_in_analyze, INTERNAL | EXPERIMENTAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("debug_sleep_in_summarize", "Sleep analyze summarize.  (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_debug_sleep_in_summarize, INTERNAL | EXPERIMENTAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("debug_sleep_in_trigger_info", "Sleep trigger info.  (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_debug_sleep_in_trigger_info, INTERNAL | EXPERIMENTAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("debug_consumer_lock",
                 "Enable debug-trace for consumer lock.  "
                 "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_instrument_consumer_lock, EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("reject_mixed_ddl_dml", "Reject write schedules which mix DDL and DML.  (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_reject_mixed_ddl_dml, EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("debug_create_master_entry", "Reproduce startup race in create_master_entry.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_debug_create_master_entry, EXPERIMENTAL | INTERNAL, 
                 NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("protobuf_prealloc_buffer_size", "Size of protobuf preallocated buffer.  (Default: 8192)", TUNABLE_INTEGER,
                 &gbl_protobuf_prealloc_buffer_size, INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("sockbplog",
                 "Enable sending transactions over socket instead of net",
                 TUNABLE_BOOLEAN, &gbl_sockbplog, READONLY | NOARG, NULL, NULL,
                 NULL, NULL);
REGISTER_TUNABLE("sockbplog_sockpool",
                 "Enable sockpool when for sockbplog feature", TUNABLE_BOOLEAN,
                 &gbl_sockbplog_sockpool, READONLY | NOARG, NULL, NULL, NULL,
                 NULL);

REGISTER_TUNABLE("replicant_retry_on_not_durable", "Replicant retries non-durable writes.  (Default: off)",
                 TUNABLE_BOOLEAN, &gbl_replicant_retry_on_not_durable, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE(
    "lightweight_rename",
    "Replaces the ondisk file rename with an aliasing at llmeta level",
    TUNABLE_BOOLEAN, &gbl_lightweight_rename, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("alternate_normalize",
                 "Use alternate SQL normalization algorithm.  (Default: on)",
                 TUNABLE_BOOLEAN, &gbl_alternate_normalize,
                 EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("sc_logbytes_per_second",
                 "Throttle schema-changes to this many logbytes per second.  (Default: 10000000)",
                 TUNABLE_INTEGER, &gbl_sc_logbytes_per_second, EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("net_somaxconn",
                 "listen() backlog setting.  (Default: 0, implies system default)",
                 TUNABLE_INTEGER, &gbl_net_maxconn, READONLY, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("throttle_txn_chunks_msec", "Wait that many milliseconds before starting a new chunk  (Default: 0)",
                 TUNABLE_INTEGER, &gbl_throttle_txn_chunks_msec, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("partitioned_table_enabled",
                 "Allow syntax create/alter table ... partitioned by ...",
                 TUNABLE_BOOLEAN, &gbl_partitioned_table_enabled, 0, NULL, NULL,
                 NULL, NULL);

REGISTER_TUNABLE("merge_table_enabled",
                 "Allow syntax create/alter table ... merge ...",
                 TUNABLE_BOOLEAN, &gbl_merge_table_enabled, 0, NULL, NULL,
                 NULL, NULL);

REGISTER_TUNABLE("externalauth", NULL, TUNABLE_BOOLEAN, &gbl_uses_externalauth, NOARG | READEARLY,
                 NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("externalauth_connect", "Check for externalauth only once on connect", TUNABLE_BOOLEAN,
                 &gbl_uses_externalauth_connect, NOARG | READEARLY, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("externalauth_warn", "Warn instead of returning error in case of missing authdata",
                 TUNABLE_BOOLEAN, &gbl_externalauth_warn, NOARG | READEARLY,
                 NULL, NULL, NULL, NULL);


REGISTER_TUNABLE("view_feature", "Enables support for VIEWs (Default: ON)",
                 TUNABLE_BOOLEAN, &gbl_view_feature, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("foreign_metadb", "Forces metadb for fdb queries to the one specified (Default:NULL)",
                 TUNABLE_STRING, &gbl_foreign_metadb, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("foreign_metadb_class", "Forces metadb for fdb queries to class specified (Default:NULL)",
                 TUNABLE_STRING, &gbl_foreign_metadb_class, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("foreign_metadb_config", "Cdb2api config file for fdb metadb; superceded by foreign_metadb (Default:NULL)",
                 TUNABLE_RAW, &gbl_foreign_metadb_config, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("allow_unauthenticated_tag_access", NULL, TUNABLE_BOOLEAN, &gbl_unauth_tag_access, NOARG | READEARLY,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("admin_mode", "Fail non-admin client requests (Default: False)", TUNABLE_BOOLEAN, &gbl_server_admin_mode, NOARG | READEARLY,
                 NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("sqlite_use_temptable_for_rowset",
                 "Use temptable instead of sqlite's binary search tree, for recording rowids (Default: ON)",
                 TUNABLE_BOOLEAN, &gbl_sqlite_use_temptable_for_rowset, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("max_identity_cache", "Max cache size of externalauth identities (Default: 500)",
                 TUNABLE_INTEGER, &gbl_identity_cache_max, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("track_weighted_queue_metrics_separately",
                 "When on, report both average and weighted average queue metrics;"
                 "When off, report only weighted average queue metrics "
                 "(Default: off)",
                 TUNABLE_BOOLEAN, &gbl_track_weighted_queue_metrics_separately, INTERNAL, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("force_directio", "Force directio on all file opens if set on environment (Default: ON)",
                 TUNABLE_BOOLEAN, &gbl_force_direct_io, 0, NULL, NULL, NULL, NULL);

REGISTER_TUNABLE("pgcomp_dryrun", "Dry-run page compaction (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_pgcomp_dryrun, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("pgcomp_dbg_stdout", "Enable debugging stdout trace for page compaction (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_pgcomp_dbg_stdout, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("pgcomp_dbg_ctrace", "Enable debugging ctrace for page compaction (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_pgcomp_dbg_ctrace, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("dump_history_on_too_many_verify_errors",
                 "Dump osql history and client info on too many verify errors (Default: off)", TUNABLE_BOOLEAN,
                 &gbl_dump_history_on_too_many_verify_errors, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("warn_on_equiv_type_mismatch", "Warn about mismatch of different but equivalent data types "
                 "returned by different sqlite versions (Default off)", TUNABLE_BOOLEAN,
                 &gbl_warn_on_equiv_type_mismatch, NOARG | EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("seekscan_maxsteps",
                 "Overrides the max number of steps for a seekscan optimization", TUNABLE_INTEGER,
                 &gbl_seekscan_maxsteps, SIGNED, NULL, NULL, NULL,
                 NULL);
REGISTER_TUNABLE("wal_osync", "Open WAL files using the O_SYNC flag (Default: off)", TUNABLE_BOOLEAN, &gbl_wal_osync, 0,
                 NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("sc_headroom", 
                 "Percentage threshold for low headroom calculation. (Default: 10)",
                 TUNABLE_DOUBLE, &gbl_sc_headroom, INTERNAL | SIGNED, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("fdb_incoherence_percentage",
                 "Generate random incoherent errors in remsql", TUNABLE_INTEGER,
                 &gbl_fdb_incoherence_percentage, INTERNAL, NULL, percent_verify, NULL, NULL);
REGISTER_TUNABLE("fdb_io_error_retries",
                 "Number of retries for io error remsql", TUNABLE_INTEGER,
                 &gbl_fdb_io_error_retries, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("fdb_io_error_retries_phase_1",
                 "Number of immediate retries; capped by fdb_io_error_retries",
                 TUNABLE_INTEGER, &gbl_fdb_io_error_retries_phase_1, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("fdb_io_error_retries_phase_2_poll",
                 "Poll initial value for slow retries in phase 2; doubled for each retry", TUNABLE_INTEGER,
                 &gbl_fdb_io_error_retries_phase_2_poll, 0, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("unexpected_last_type_warn",
                 "print a line of trace if the last response server sent before sockpool reset isn't LAST_ROW",
                 TUNABLE_INTEGER, &gbl_unexpected_last_type_warn, EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("unexpected_last_type_abort",
                 "Panic if the last response server sent before sockpool reset isn't LAST_ROW",
                 TUNABLE_INTEGER, &gbl_unexpected_last_type_abort, EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("pstack_self",
                 "Dump stack traces on certain slow events.",
                 TUNABLE_BOOLEAN, &gbl_pstack_self, EXPERIMENTAL | INTERNAL, NULL, NULL, NULL, NULL);
REGISTER_TUNABLE("noleader_retry_duration_ms",
                 "The amount of time in milliseconds that a replicant retries if there isn't a leader. (Default: 50,000)",
                 TUNABLE_INTEGER, &gbl_noleader_retry_duration_ms, INTERNAL, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("noleader_retry_poll_ms",
                 "Wait this long before retrying on no-leader. (Default: 10)",
                 TUNABLE_INTEGER, &gbl_noleader_retry_poll_ms, INTERNAL, NULL,
                 NULL, NULL, NULL);
REGISTER_TUNABLE("cdb2api_policy_override", "Use this policy override with cdb2api. (Default: none)",
                TUNABLE_STRING, &gbl_cdb2api_policy_override, 0, NULL, NULL, NULL, NULL);
#endif /* _DB_TUNABLES_H */
