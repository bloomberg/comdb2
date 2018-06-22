/*
   Copyright 2015, 2018, Bloomberg Finance L.P.

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

int __berkdb_write_alarm_ms;
int __berkdb_read_alarm_ms;
int __berkdb_fsync_alarm_ms;

extern int gbl_berkdb_track_locks;

void __berkdb_set_num_read_ios(long long *n);
void __berkdb_set_num_write_ios(long long *n);
void __berkdb_set_num_fsyncs(long long *n);
void berk_memp_sync_alarm_ms(int);

#include <pthread.h>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <berkdb/dbinc/queue.h>
#include <limits.h>

#include "limit_fortify.h"
#include <alloca.h>
#include <ctype.h>
#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <netdb.h>
#include <poll.h>
#include <pwd.h>
#include <libgen.h>

#include <mem_uncategorized.h>

#include <logmsg.h>
#include <epochlib.h>
#include <segstr.h>
#include <lockmacro.h>

#include <list.h>
#include <mem.h>

#include <str0.h>
#include <rtcpu.h>
#include <ctrace.h>

#include <memory_sync.h>

#include <net.h>
#include <bdb_api.h>
#include <sbuf2.h>
#include "quantize.h"
#include "timers.h"

#include "comdb2.h"
#include "sql.h"

#include "comdb2_trn_intrl.h"
#include "history.h"
#include "tag.h"
#include "types.h"
#include "timer.h"
#include <plhash.h>
#include <dynschemaload.h>
#include "util.h"
#include "verify.h"
#include "ssl_bend.h"
#include "switches.h"
#include "sqloffload.h"
#include "osqlblockproc.h"

#include <sqliteInt.h>

#include "thdpool.h"
#include "memdebug.h"
#include "bdb_access.h"
#include "analyze.h"

#include "comdb2_info.h"
#include "osqlcomm.h"
#include <cdb2_constants.h>

#include <crc32c.h>

#include "fdb_fend.h"
#include "fdb_bend.h"
#include <flibc.h>

#include <autoanalyze.h>
#include <sqlglue.h>

#include "dbdest.h"
#include "intern_strings.h"
#include "bb_oscompat.h"
#include "comdb2uuid.h"
#include "debug_switches.h"
#include "eventlog.h"
#include "config.h"

#include "views.h"

#include <autoanalyze.h>
#include <cdb2_constants.h>
#include <bb_oscompat.h>
#include <schemachange.h>

#define tokdup strndup

int gbl_sc_timeoutms = 1000 * 60;
char gbl_dbname[MAX_DBNAME_LENGTH];
int gbl_largepages;
int gbl_llmeta_open = 0;

int gbl_sqlite_sortermult = 1;

int gbl_sqlite_sorter_mem = 300 * 1024 * 1024; /* 300 meg */

int gbl_rep_node_pri = 0;
int gbl_handoff_node = 0;
int gbl_use_node_pri = 0;
int gbl_allow_lua_print = 0;
int gbl_allow_lua_dynamic_libs = 0;
int gbl_master_changed_oldfiles = 0;
int gbl_recovery_timestamp = 0;
int gbl_recovery_lsn_file = 0;
int gbl_recovery_lsn_offset = 0;
int gbl_trace_prepare_errors = 0;
int gbl_trigger_timepart = 0;
int gbl_extended_sql_debug_trace = 0;
extern int gbl_dump_fsql_response;

void myctrace(const char *c) { ctrace("%s", c); }

void berkdb_use_malloc_for_regions_with_callbacks(void *mem,
                                                  void *(*alloc)(void *, int),
                                                  void (*free)(void *, void *));

static int put_all_csc2();

static void *purge_old_blkseq_thread(void *arg);
static void *purge_old_files_thread(void *arg);
static int lrllinecmp(char *lrlline, char *cmpto);
static void ttrap(struct timer_parm *parm);
int clear_temp_tables(void);

pthread_key_t comdb2_open_key;

/*---GLOBAL SETTINGS---*/
const char *const gbl_db_release_name = "R7.0pre";
int gbl_enque_flush_interval;
int gbl_enque_reorder_lookahead = 20;
int gbl_morecolumns = 0;
int gbl_return_long_column_names = 1;
int gbl_maxreclen;
int gbl_penaltyincpercent = 20;
int gbl_maxwthreadpenalty;
int gbl_spstrictassignments = 0;
int gbl_lock_conflict_trace = 0;
int gbl_move_deadlk_max_attempt = 500;

int gbl_uses_password;
int gbl_uses_accesscontrol_tableXnode;
int gbl_blocksql_grace =
    10; /* how many seconds we wait for a blocksql during downgrade */
int gbl_upd_key;
unsigned long long gbl_sqltick;
int gbl_watchdog_watch_threshold = 60;
int gbl_watchdog_disable_at_start = 0; /* don't enable watchdog on start */
int gbl_nonames = 1;
int gbl_reject_osql_mismatch = 1;
int gbl_abort_on_clear_inuse_rqid = 1;

pthread_t gbl_invalid_tid; /* set this to our main threads tid */

/* lots of defaults. */
int gbl_exit_on_pthread_create_fail = 0;
int gbl_exit_on_internal_error = 1;
int gbl_osql_blockproc_timeout_sec = 5;  /* wait for 5 seconds for a blocproc*/
int gbl_osql_max_throttle_sec = 60 * 10; /* 10-minute default */
int gbl_osql_bkoff_netsend_lmt = 5 * 60 * 1000; /* 5 mins */
int gbl_osql_bkoff_netsend = 100;               /* wait 100 msec */
int gbl_net_max_queue = 25000;
int gbl_net_max_mem = 0;
int gbl_net_poll = 100;
int gbl_net_throttle_percent = 50;
int gbl_osql_net_poll = 100;
int gbl_osql_max_queue = 10000;
int gbl_osql_net_portmux_register_interval = 600;
int gbl_net_portmux_register_interval = 600;

int gbl_max_sqlcache = 10;
int gbl_new_row_data = 0;
int gbl_extended_tm_from_sql =
    0; /* Keep a count of our extended-tm requests from sql. */

int gbl_upgrade_blocksql_2_socksql =
    1; /* this is set if blocksock is in any parsed lrl
          files; if any blocksql requests will actually
          by socksql */

int gbl_serialise_sqlite3_open = 0;

int gbl_nice = 0;

int gbl_notimeouts = 0; /* set this if you don't need the server timeouts
                           (use this for new code testing) */

int gbl_nullfkey = 0;

/* Default fast sql timeouts */
int gbl_sqlwrtimeoutms = 10000;

long long gbl_converted_blocksql_requests = 0;

int gbl_rangextunit =
    16; /* dont do more than 16 records in a single rangext op */
int gbl_honor_rangextunit_for_old_apis = 0;

/* various roles for the prefault helper threads.  */
int gbl_prefaulthelper_blockops = 1;
int gbl_prefaulthelper_sqlreadahead = 1;

/* this many "next" ops trigger a readahead */
int gbl_sqlreadaheadthresh = 0;

int gbl_iothreads = 0;
int gbl_sqlreadahead = 0;
int gbl_ioqueue = 0;
int gbl_prefaulthelperthreads = 0;
int gbl_osqlpfault_threads = 0;
int gbl_prefault_udp = 0;
__thread int send_prefault_udp = 0;
__thread snap_uid_t *osql_snap_info; /* contains cnonce */

int gbl_starttime = 0;
int gbl_use_sqlthrmark = 1000;
int gbl_repchecksum = 0;
int gbl_pfault = 0;
int gbl_pfaultrmt = 1;
int gbl_dtastripe = 8;
int gbl_blobstripe = 1;
int gbl_rebuild_mode = 0;
int gbl_dbsizetime = 15 * 60; /* number of seconds for db size calculation */
int gbl_debug = 0;            /* operation debugging */
int gbl_sdebug = 0;           /* sql operation debugging */
int gbl_debug_until = 0;      /* "debg" debugging */
int gbl_who = 0;              /* "who" debugging */
int gbl_maxthreads = 48;      /* max # of threads */
int gbl_maxwthreads = 8;      /* max # of threads */
int gbl_maxqueue = 192;       /* max pending requests.*/
int gbl_thd_linger = 5;       /* number of seconds for threads to linger */
int gbl_report = 0;           /* update rate to log */
int gbl_report_last;
long gbl_report_last_n;
long gbl_report_last_r;
char *gbl_mynode;     /* my hostname */
struct in_addr gbl_myaddr; /* my IPV4 address */
int gbl_mynodeid = 0; /* node number, for backwards compatibility */
char *gbl_myhostname; /* added for now to merge fdb source id */
pid_t gbl_mypid;      /* my pid */
int gbl_myroom;
int gbl_exit = 0;        /* exit requested.*/
int gbl_create_mode = 0; /* turn on create-if-not-exists mode*/
const char *gbl_repoplrl_fname = NULL; /* if != NULL this is the fname of the
                                        * external lrl file to create with
                                        * this db's settings and table defs */
int gbl_local_mode = 0;                /* local mode, no siblings */
int gbl_fullrecovery = 0;              /* backend full-recovery mode*/
int gbl_maxretries = 500;              /* thats a lotta retries */
int gbl_maxblobretries =
    0; /* everyone assures me this can't happen unless the data is corrupt */
int gbl_maxcontextskips = 10000; /* that's a whole whale of a lotta retries */
int gbl_heartbeat_check = 0, gbl_heartbeat_send = 0, gbl_decom = 0;
int gbl_netbufsz = 1 * 1024 * 1024;
int gbl_loghist = 0;
int gbl_loghist_verbose = 0;
int gbl_repdebug = -1;
int gbl_elect_time_secs = 0;
char *gbl_pmblock = NULL;
int gbl_rtcpu_debug = 0;
int gbl_longblk_trans_purge_interval =
    30; /* initially, set this to 30 seconds */
int gbl_sqlflush_freq = 0;
int gbl_sbuftimeout = 0;
int gbl_conv_flush_freq = 100; /* this is currently ignored */
pthread_attr_t gbl_pthread_attr;
int gbl_meta_lite = 1;
int gbl_context_in_key = 1;
int gbl_ready = 0; /* gets set just before waitft is called
                      and never gets unset */
int gbl_debug_verify_tran = 0;
int gbl_readonly = 0;
int gbl_init_single_meta = 1;
int gbl_schedule = 0;

int gbl_init_with_rowlocks = 0;
int gbl_init_with_genid48 = 1;
int gbl_init_with_odh = 1;
int gbl_init_with_ipu = 1;
int gbl_init_with_instant_sc = 1;
int gbl_init_with_compr = BDB_COMPRESS_CRLE;
int gbl_init_with_compr_blobs = BDB_COMPRESS_LZ4;
int gbl_init_with_bthash = 0;

unsigned int gbl_nsql;
long long gbl_nsql_steps;

unsigned int gbl_nnewsql;
long long gbl_nnewsql_steps;

unsigned int gbl_masterrejects = 0;

volatile int gbl_dbopen_gen = 0;
volatile int gbl_analyze_gen = 0;
volatile int gbl_views_gen = 0;
int gbl_sqlhistsz = 25;
int gbl_lclpooled_buffers = 32;

int gbl_maxblockops = 25000;

int gbl_replicate_local = 0;
int gbl_replicate_local_concurrent = 0;

/* TMP BROKEN DATETIME */
int gbl_allowbrokendatetime = 1;
int gbl_sort_nulls_correctly = 1;
int gbl_check_client_tags = 1;
char *gbl_lrl_fname = NULL;
char *gbl_spfile_name = NULL;
int gbl_max_lua_instructions = 10000;
int gbl_check_wrong_cmd = 1;
int gbl_updategenids = 0;
int gbl_osql_heartbeat_send = 5;
int gbl_osql_heartbeat_alert = 7;
int gbl_chkpoint_alarm_time = 60;
int gbl_incoherent_msg_freq = 60 * 60;  /* one hour between messages */
int gbl_incoherent_alarm_time = 2 * 60; /* alarm if we are incoherent for
                                           more than two minutes */
int gbl_max_incoherent_nodes = 1;       /* immediate alarm if more than
                                           this number of (online) nodes fall
                                           incoherent */

int gbl_bad_lrl_fatal = 0;

int gbl_force_highslot = 0;
int gbl_num_contexts = 16;
int gbl_buffers_per_context = 255;

int gbl_max_columns_soft_limit = 255; /* this is the old hard limit */

int gbl_dispatch_rowlocks_bench = 1;
int gbl_rowlocks_bench_logical_rectype = 1;
int gbl_verbose_toblock_backouts = 0;
/* TODO */
int gbl_page_latches = 0;
int gbl_replicant_latches = 0;
int gbl_disable_update_shadows = 0;
int gbl_disable_rowlocks_logging = 0;
int gbl_disable_rowlocks = 0;
int gbl_disable_rowlocks_sleepns = 0;
int gbl_random_rowlocks = 0;
int gbl_already_aborted_trace = 0;
int gbl_deadlock_policy_override = -1;
int gbl_dump_sql_dispatched = 0; /* dump all sql strings dispatched */
int gbl_time_osql = 0;           /* dump timestamps for osql steps */
int gbl_time_fdb = 0;            /* dump timestamps for remote sql */

int gbl_goslow = 0; /* set to disable "gofast" */

int gbl_selectv_rangechk = 0; /* disable selectv range check by default */

int gbl_sql_tranlevel_preserved = SQL_TDEF_SOCK;
int gbl_sql_tranlevel_default = SQL_TDEF_SOCK;
int gbl_exit_alarm_sec = 300;
int gbl_test_blkseq_replay_code = 0;
int gbl_dump_blkseq = 0;
int gbl_test_curtran_change_code = 0;
int gbl_enable_pageorder_trace = 0;
int gbl_disable_deadlock_trace = 1;
int gbl_disable_overflow_page_trace = 1;
int gbl_simulate_rowlock_deadlock_interval = 0;
int gbl_enable_berkdb_retry_deadlock_bias = 0;
int gbl_enable_cache_internal_nodes = 1;
int gbl_use_appsock_as_sqlthread = 0;
int gbl_rep_process_txn_time = 0;

int gbl_osql_verify_retries_max =
    499; /* how many times we retry osql for verify */
int gbl_osql_verify_ext_chk =
    1; /* extended verify-checking after this many failures */
int gbl_test_badwrite_intvl = 0;
int gbl_test_blob_race = 0;
int gbl_skip_ratio_trace = 0;

int gbl_throttle_sql_overload_dump_sec = 5;
int gbl_toblock_net_throttle = 0;

int gbl_temptable_pool_capacity = 8192;

int gbl_ftables = 0;

/* cdb2 features */
int gbl_disable_skip_rows = 0;

#if 0
u_int gbl_blk_pq_shmkey = 0;
#endif
int gbl_enable_sql_stmt_caching = STMT_CACHE_ALL;

int gbl_round_robin_stripes = 0;
int gbl_num_record_converts = 100;

int gbl_rrenablecountchanges = 0;

int gbl_debug_log_twophase = 0;
int gbl_debug_log_twophase_transactions = 0;

int gbl_early_blkseq_check = 0;

int gbl_sql_time_threshold = 5000;

int gbl_allow_mismatched_tag_size = 0;

double gbl_sql_cost_error_threshold = -1;

int gbl_parallel_recovery_threads = 0;

int gbl_fdb_resolve_local = 0;
int gbl_fdb_allow_cross_classes = 0;

/*---COUNTS---*/
long n_qtrap;
long n_fstrap;
long n_qtrap_notcoherent;
long n_bad_parm;
long n_bad_swapin;
long n_retries;
long n_missed;

int n_commits;
long long n_commit_time; /* in micro seconds.*/
pthread_mutex_t commit_stat_lk = PTHREAD_MUTEX_INITIALIZER;

int n_retries_transaction_active = 0;
int n_retries_transaction_done = 0;
int gbl_num_rr_rejected = 0;

history *reqhist;

struct dbenv *thedb;              /*handles 1 db for now*/

int gbl_exclusive_blockop_qconsume = 0;
pthread_rwlock_t gbl_block_qconsume_lock = PTHREAD_RWLOCK_INITIALIZER;
pthread_rwlock_t thedb_lock = PTHREAD_RWLOCK_INITIALIZER;

int gbl_malloc_regions = 1;
int gbl_rowlocks = 0;
int gbl_disable_tagged_api = 1;
int gbl_snapisol = 0;
int gbl_new_snapisol = 0;
int gbl_new_snapisol_asof = 0;
int gbl_new_snapisol_logging = 0;
int gbl_disable_new_snapshot = 0;
int gbl_newsi_use_timestamp_table = 0;
int gbl_update_shadows_interval = 0;
int gbl_lowpri_snapisol_sessions = 0;
int gbl_support_sock_luxref = 1;

struct quantize *q_min;
struct quantize *q_hour;
struct quantize *q_all;

struct quantize *q_sql_min;
struct quantize *q_sql_hour;
struct quantize *q_sql_all;

struct quantize *q_sql_steps_min;
struct quantize *q_sql_steps_hour;
struct quantize *q_sql_steps_all;

extern int gbl_net_lmt_upd_incoherent_nodes;
extern int gbl_allow_user_schema;
extern int gbl_skip_cget_in_db_put;

int gbl_argc;
char **gbl_argv;

int gbl_stop_thds_time = 0;
int gbl_stop_thds_time_threshold = 60;
pthread_mutex_t stop_thds_time_lk = PTHREAD_MUTEX_INITIALIZER;

int gbl_disallow_null_blobs = 1;
int gbl_force_notnull_static_tag_blobs = 1;
int gbl_key_updates = 1;

int gbl_partial_indexes = 1;
int gbl_expressions_indexes = 1;
int gbl_new_indexes = 0;

int gbl_optimize_truncate_repdb = 1;

static void set_datetime_dir(void);

extern void tz_hash_init(void);
extern void tz_hash_free(void);
void set_tzdir(char *dir);
void free_tzdir();

extern void init_sql_hint_table();
extern void init_clientstats_table();
extern int bdb_osql_log_repo_init(int *bdberr);

int gbl_use_plan = 1;

double gbl_querylimits_maxcost = 0;
int gbl_querylimits_tablescans_ok = 1;
int gbl_querylimits_temptables_ok = 1;

double gbl_querylimits_maxcost_warn = 0;
int gbl_querylimits_tablescans_warn = 0;
int gbl_querylimits_temptables_warn = 0;
extern int gbl_empty_strings_dont_convert_to_numbers;

extern int gbl_survive_n_master_swings;
extern int gbl_master_retry_poll_ms;

int gbl_check_schema_change_permissions = 1;

int gbl_print_syntax_err = 0;

extern int gbl_verify_direct_io;

extern int gbl_verify_lsn_written;
extern int gbl_parallel_memptrickle;

int gbl_verify_dbreg = 0;
extern int gbl_checkpoint_paranoid_verify;

int gbl_forbid_ulonglong = 1;

int gbl_support_datetime_in_triggers = 1;

int gbl_use_block_mode_status_code = 1;

unsigned int gbl_delayed_skip = 0;
int gbl_enable_osql_logging = 0;
int gbl_enable_osql_longreq_logging = 0;

int gbl_broken_num_parser = 0;

int gbl_fk_allow_prefix_keys = 1;

int gbl_fk_allow_superset_keys = 1;

int gbl_update_delete_limit = 1;

int verbose_deadlocks = 0;

int gbl_early = 1;
int gbl_reallyearly = 0;

int gbl_udp = 1;

int gbl_berkdb_verify_skip_skipables = 0;

int gbl_berkdb_epochms_repts = 0;

int gbl_disable_sql_dlmalloc = 0;

int gbl_decimal_rounding = DEC_ROUND_HALF_EVEN;
int gbl_sparse_lockerid_map = 1;
int gbl_inplace_blobs = 1;
int gbl_osql_blob_optimization = 1;
int gbl_inplace_blob_optimization = 1;
int gbl_report_sqlite_numeric_conversion_errors = 1;
int gbl_max_sql_hint_cache = 100;

unsigned long long gbl_untouched_blob_cnt = 0;
unsigned long long gbl_update_genid_blob_cnt = 0;
unsigned long long gbl_inplace_blob_cnt = 0;
unsigned long long gbl_delupd_blob_cnt = 0;
unsigned long long gbl_addupd_blob_cnt = 0;
unsigned long long gbl_rowlocks_deadlock_retries = 0;

int gbl_use_fastseed_for_comdb2_seqno = 0;

int gbl_disable_stable_for_ipu = 1;

int gbl_disable_exit_on_thread_error = 0;

int gbl_berkdb_iomap = 1;
int gbl_check_dbnum_conflicts = 1;
int gbl_requeue_on_tran_dispatch = 1;
int gbl_crc32c = 1;
int gbl_repscore = 0;
int gbl_surprise = 1; // TODO: change name to something else
int gbl_check_wrong_db = 1;
int gbl_broken_max_rec_sz = 0;
int gbl_private_blkseq = 1;
int gbl_use_blkseq = 1;

char *gbl_recovery_options = NULL;

#include <stdbool.h>
bool gbl_rcache = true;

int gbl_noenv_messages = 1;

int gbl_check_sql_source = 0;
int skip_clear_queue_extents = 0;

int gbl_flush_check_active_peer = 1;

int gbl_ctrace_dbdir = 0;
int gbl_inflate_log = 0;

int gbl_skip_llmeta_progress_updates_on_schema_change = 0;
int gbl_sc_inco_chk = 1;
int gbl_track_queue_time = 1;
int gbl_locks_check_waiters = 1;
int gbl_update_startlsn_printstep = 0;
int gbl_rowlocks_commit_on_waiters = 1;
int gbl_rowlocks_deadlock_trace = 0;

int gbl_durable_wait_seqnum_test = 0;
int gbl_durable_replay_test = 0;
int gbl_durable_set_trace = 0;
int gbl_durable_calc_trace = 0;
int gbl_dumptxn_at_commit = 0;

char *gbl_crypto = NULL;

int gbl_log_fstsnd_triggers = 0;
int gbl_broadcast_check_rmtpol = 1;
int gbl_replicate_rowlocks = 1;
int gbl_replicant_gather_rowlocks = 1;
int gbl_force_old_cursors = 0;
int gbl_track_curtran_locks = 0;
int gbl_print_deadlock_cycles = 0;
int gbl_always_send_cnonce = 1;
int gbl_dump_page_on_byteswap_error = 0;
int gbl_dump_after_byteswap = 0;
int gbl_micro_retry_on_deadlock = 1;
int gbl_disable_blob_check = 0;
int gbl_disable_new_snapisol_overhead = 0;
int gbl_verify_all_pools = 0;
int gbl_print_blockp_stats = 0;
int gbl_allow_parallel_rep_on_pagesplit = 1;
int gbl_allow_parallel_rep_on_prefix = 1;
// XXX remove before merging jepsen
int gbl_only_match_commit_records = 1;

/* Release locks if replication is waiting on a lock you hold (si-only) */
int gbl_sql_release_locks_on_si_lockwait = 1;
/* If this is set, recom_replay will see the same row multiple times in a scan &
 * fail */
int gbl_sql_release_locks_on_emit_row = 0;
int gbl_sql_release_locks_on_slow_reader = 1;
int gbl_sql_no_timeouts_on_release_locks = 1;
int gbl_sql_release_locks_in_update_shadows = 1;
int gbl_sql_random_release_interval = 0;
int gbl_sql_release_locks_trace = 0;
int gbl_lock_get_verbose_waiter = 0;
int gbl_lock_get_list_start = 0;
int gbl_dump_locks_on_repwait = 0;

int gbl_slow_rep_process_txn_maxms = 0;
int gbl_slow_rep_process_txn_freq = 0;
int gbl_check_page_in_recovery = 0;
int gbl_cmptxn_inherit_locks = 1;
int gbl_rep_printlock = 0;

int gbl_keycompr = 1;
int gbl_memstat_freq = 60 * 5;
int gbl_accept_on_child_nets = 0;
int gbl_disable_etc_services_lookup = 0;
int gbl_fingerprint_queries = 1;
int gbl_stable_rootpages_test = 0;

/* Only allows the ability to enable: must be enabled on a session via 'set' */
int gbl_allow_incoherent_sql = 1;

char *gbl_dbdir = NULL;
static int gbl_backend_opened = 0;

extern int gbl_verbose_net;

static void create_service_file(const char *lrlname);

/* FOR PAGE COMPACTION.
   The threshold should be kept under 0.5. By default, we make it lg(2)/2
   (see comment in bdb/rep.c to learn more about expected node utilization). */
/* Disabling for the time being */
double gbl_pg_compact_thresh = 0;
int gbl_pg_compact_latency_ms = 0;
int gbl_large_str_idx_find = 1;
int gbl_abort_invalid_query_info_key;

extern int gbl_allow_user_schema;
extern int gbl_uses_password;

extern int gbl_direct_count;
extern int gbl_parallel_count;
extern int gbl_debug_sqlthd_failures;
extern int gbl_random_get_curtran_failures;
extern int gbl_random_blkseq_replays;
extern int gbl_disable_cnonce_blkseq;
int gbl_mifid2_datetime_range = 1;

int gbl_early_verify = 1;

int gbl_bbenv;

comdb2_tunables *gbl_tunables; /* All registered tunables */
int init_gbl_tunables();
int free_gbl_tunables();
int register_db_tunables(struct dbenv *tbl);

int init_plugins(void);
int destroy_plugins(void);
void register_plugin_tunables(void);
int install_static_plugins(void);
int run_init_plugins(void);

inline int getkeyrecnums(const struct dbtable *tbl, int ixnum)
{
    if (ixnum < 0 || ixnum >= tbl->nix)
        return -1;
    return tbl->ix_recnums[ixnum] != 0;
}
inline int getkeysize(const struct dbtable *tbl, int ixnum)
{
    if (ixnum < 0 || ixnum >= tbl->nix)
        return -1;
    return tbl->ix_keylen[ixnum];
}

inline int getdatsize(const struct dbtable *tbl)
{
    return tbl->lrl;
}

/*lookup dbs..*/
struct dbtable *getdbbynum(int num)
{
    int ii;
    struct dbtable *p_db = NULL;
    pthread_rwlock_rdlock(&thedb_lock);
    for (ii = 0; ii < thedb->num_dbs; ii++) {
        if (thedb->dbs[ii]->dbnum == num) {
            p_db = thedb->dbs[ii];
            pthread_rwlock_unlock(&thedb_lock);
            return p_db;
        }
    }
    pthread_rwlock_unlock(&thedb_lock);
    return 0;
}

int getdbidxbyname(const char *p_name)
{
    struct dbtable *tbl;
    tbl = hash_find_readonly(thedb->db_hash, &p_name);
    return (tbl) ? tbl->dbs_idx : -1;
}

struct dbtable *get_dbtable_by_name(const char *p_name)
{
    struct dbtable *p_db = NULL;

    pthread_rwlock_rdlock(&thedb_lock);
    p_db = hash_find_readonly(thedb->db_hash, &p_name);
    pthread_rwlock_unlock(&thedb_lock);

    return p_db;
}

struct dbtable *getqueuebyname(const char *name)
{
    return hash_find_readonly(thedb->qdb_hash, &name);
}

int get_max_reclen(struct dbenv *dbenv)
{
    int max = 0;
    char *fname, fname_tail[] = "_file_vers_map";
    int file, fnamelen;
    SBUF2 *sbfile;
    char line[256];
    char tablename[64];
    int reclen;
    int rc;

    /* get the mem we need for fname */
    fnamelen = strlen(dbenv->basedir) + strlen(dbenv->envname) +
               strlen(fname_tail) + 2 /* one for / and one for NULL byte */;
    fname = malloc(fnamelen);
    if (!fname) {
        logmsg(LOGMSG_ERROR, "get_max_reclen: failed to malloc file name\n");
        return -1;
    }

    /* construct the file's name */
    if (gbl_nonames)
        rc = snprintf(fname, fnamelen, "%s/file_vers_map", dbenv->basedir);
    else
        rc = snprintf(fname, fnamelen, "%s/%s%s", dbenv->basedir,
                      dbenv->envname, fname_tail);

    if (rc < 0 || rc >= fnamelen) {
        logmsg(LOGMSG_ERROR, "get_max_reclen: failed to create file name\n");
        free(fname);
        return -1;
    }

    /* open file */
    file = open(fname, O_RDONLY);
    free(fname);
    if (file == -1) {
        logmsg(LOGMSG_ERROR, "get_max_reclen: failed to open %s for writing\n",
                fname);
        return -1;
    }

    sbfile = sbuf2open(file, 0);
    if (!sbfile) {
        logmsg(LOGMSG_ERROR, "get_max_reclen: failed to open sbuf2\n");
        close(file);
        return -1;
    }

    while (sbuf2gets(line, 256, sbfile) > 0) {
        reclen = 0;
        sscanf(line, "table %s %d\n", tablename, &reclen);
        if (reclen) {
            if (strncmp(tablename, "sqlite_stat", 11) != 0) {
                if (reclen > max)
                    max = reclen;
            }
        }
    }

    sbuf2close(sbfile);

    return max;
}

void showdbenv(struct dbenv *dbenv)
{
    int ii, jj;
    struct dbtable *usedb;
    logmsg(LOGMSG_USER, "-----\n");
    for (jj = 0; jj < dbenv->num_dbs; jj++) {
        usedb = dbenv->dbs[jj]; /*de-stink*/
        logmsg(LOGMSG_USER,
               "table '%s' comdbg compat dbnum %d\ndir '%s' lrlfile '%s' "
               "nconns %d  nrevconns %d\n",
               usedb->tablename, usedb->dbnum, dbenv->basedir,
               (usedb->lrlfname) ? usedb->lrlfname : "NULL",
               usedb->n_constraints, usedb->n_rev_constraints);
        logmsg(LOGMSG_ERROR, "   data reclen %-3d bytes\n", usedb->lrl);

        for (ii = 0; ii < usedb->nix; ii++) {
            logmsg(LOGMSG_USER,
                   "   index %-2d keylen %-3d bytes  dupes? %c recnums? %c"
                   " datacopy? %c collattr? %c uniqnulls %c disabled %c\n",
                   ii, usedb->ix_keylen[ii],
                   (usedb->ix_dupes[ii] ? 'y' : 'n'),
                   (usedb->ix_recnums[ii] ? 'y' : 'n'),
                   (usedb->ix_datacopy[ii] ? 'y' : 'n'),
                   (usedb->ix_collattr[ii] ? 'y' : 'n'),
                   (usedb->ix_nullsallowed[ii] ? 'y' : 'n'),
                   (usedb->ix_disabled[ii] ? 'y' : 'n'));
        }
    }
    for (ii = 0; ii < dbenv->nsiblings; ii++) {
        logmsg(LOGMSG_USER, "sibling %-2d host %s:%d\n", ii,
               dbenv->sibling_hostname[ii], *dbenv->sibling_port[ii]);
    }
}

enum OPENSTATES {
    OPENSTATE_THD_CREATE = 1,
    OPENSTATE_BACKEND_OPEN = 2,
    OPENSTATE_FAILED = -1,
    OPENSTATE_SUCCESS = 3
};

void no_new_requests(struct dbenv *dbenv)
{
    thedb->stopped = 1;
    MEMORY_SYNC;
}

int db_is_stopped(void) { return (thedb->stopped || thedb->exiting); }

void print_dbsize(void);

static void init_q_vars()
{
    q_min = quantize_new(10, 2000, "ms");
    q_hour = quantize_new(10, 2000, "ms");
    q_all = quantize_new(10, 2000, "ms");

    q_sql_min = quantize_new(100, 100000, "steps");
    q_sql_hour = quantize_new(100, 100000, "steps");
    q_sql_all = quantize_new(100, 100000, "steps");

    q_sql_steps_min = quantize_new(100, 100000, "steps");
    q_sql_steps_hour = quantize_new(100, 100000, "steps");
    q_sql_steps_all = quantize_new(100, 100000, "steps");
}

static void cleanup_q_vars()
{
    quantize_free(q_min);
    quantize_free(q_hour);
    quantize_free(q_all);

    quantize_free(q_sql_min);
    quantize_free(q_sql_hour);
    quantize_free(q_sql_all);

    quantize_free(q_sql_steps_min);
    quantize_free(q_sql_steps_hour);
    quantize_free(q_sql_steps_all);
}

/* Send an alert about the fact that I'm incoherent */
static int send_incoherent_message(int num_online, int duration)
{
    char *tmpfile;
    FILE *fh;
    struct utsname u;
    int hours, mins, secs;
    uuid_t uuid;
    uuidstr_t us;

    comdb2uuid(uuid);
    comdb2uuidstr(uuid, us);
    tmpfile = comdb2_location("tmp", "comdb2_incoh_msg.%s.%s.txt",
                              thedb->envname, us);

    fh = fopen(tmpfile, "w");
    if (!fh) {
        logmsg(LOGMSG_ERROR, "%s: cannot open '%s': %d %s\n", __func__, tmpfile,
                errno, strerror(errno));
        free(tmpfile);
        return -1;
    }

    uname(&u);
    fprintf(fh, "%s %s HAS %d INCOHERENT ONLINE NODES\n", u.nodename,
            thedb->envname, num_online);

    hours = duration / (60 * 60);
    mins = (duration / 60) % 60;
    secs = duration % 60;
    fprintf(fh, "Nodes have been incoherent for %02d:%02d:%02d\n", hours, mins,
            secs);

    bdb_short_netinfo_dump(fh, thedb->bdb_env);

    fclose(fh);

    logmsg(LOGMSG_WARN, "incoherent nodes present for longer than desired, details in %s\n",
           tmpfile);
    free(tmpfile);

    return 0;
}

/* sorry guys, i hijacked this to be more of a "purge stuff in general" thread
 * -- SJ
 * now blkseq doesn't exist anymore much less a purge function for it, now this
 * thread is really misnamed
 * cpick */
static void *purge_old_blkseq_thread(void *arg)
{
    struct dbenv *dbenv;
    dbenv = arg;
    struct thr_handle *thr_self;
    int loop;

    thr_self = thrman_register(THRTYPE_PURGEBLKSEQ);
    thread_started("blkseq");

    dbenv->purge_old_blkseq_is_running = 1;
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDONLY);

    loop = 0;
    sleep(1);

    while (!db_is_stopped()) {

        /* Check del unused files progress about twice per threshold  */
        if (!(loop % (gbl_sc_del_unused_files_threshold_ms /
                      (2 * 1000 /*ms per sec*/))))
            sc_del_unused_files_check_progress();

        if (loop == 3600)
            loop = 0;
        else
            ++loop;

        if (debug_switch_check_for_hung_checkpoint_thread() &&
            dbenv->master == gbl_mynode) {
            int chkpoint_time = bdb_get_checkpoint_time(dbenv->bdb_env);
            if (gbl_chkpoint_alarm_time > 0 &&
                chkpoint_time > gbl_chkpoint_alarm_time) {
                logmsg(LOGMSG_ERROR, "CHECKPOINT THREAD HUNG FOR %d SECONDS\n",
                        chkpoint_time);

                /* Grab diagnostics once a minute */
                if ((gbl_chkpoint_alarm_time - chkpoint_time) % 60 == 0) {
                    char cmd[100];
                    int len;
                    len = snprintf(cmd, sizeof(cmd),
                                   "f %s/chkpoint_hung_full_diag fulldiag",
                                   dbenv->basedir);

                    logmsg(LOGMSG_ERROR,
                            "Running bdb '%s' command to grab diagnostics\n",
                            cmd);
                    bdb_process_user_command(dbenv->bdb_env, cmd, len, 0);
                }
            }
        }

        if (dbenv->master == gbl_mynode) {
            static int last_incoh_msg_time = 0;
            static int peak_online_count = 0;
            int num_incoh, since_epoch;
            const char *incoh_list[REPMAX];
            int now = comdb2_time_epoch();

            bdb_get_notcoherent_list(dbenv->bdb_env, incoh_list, REPMAX,
                                     &num_incoh, &since_epoch);

            if (num_incoh > 0) {
                int online_count, ii;
                int duration = comdb2_time_epoch() - since_epoch;

                /* Exclude rtcpu'd nodes from our list of problem machines */
                for (online_count = 0, ii = 0; ii < num_incoh && ii < REPMAX;
                     ii++) {
                    if (is_node_up(incoh_list[ii]))
                        online_count++;
                }

                /* Filter out momentary incoherency unless it is more than
                 * 2 incoherent nodes */
                if (online_count < 3 && duration < 20) {
                    /* No message */
                } else if (online_count > 0 &&
                           (duration >= gbl_incoherent_alarm_time ||
                            online_count > gbl_max_incoherent_nodes)) {
                    /* Send a message if it's been a while or if things are
                     * worse than ever before */
                    if (last_incoh_msg_time == 0 ||
                        now - last_incoh_msg_time >= gbl_incoherent_msg_freq ||
                        online_count < peak_online_count) {
                        if (online_count < peak_online_count)
                            online_count = peak_online_count;
                        last_incoh_msg_time = now;

                        /* Send a message about these dreadful incoherent
                         * nodes */
                        send_incoherent_message(online_count, duration);
                    }
                }
            }
        }

        if (gbl_private_blkseq) {
            thrman_where(thr_self, "clean_blkseq");
            int nstripes;

            nstripes =
                bdb_attr_get(dbenv->bdb_attr, BDB_ATTR_PRIVATE_BLKSEQ_STRIPES);
            for (int stripe = 0; stripe < nstripes; stripe++) {
                int rc;

                rc = bdb_blkseq_clean(dbenv->bdb_env, stripe);
                if (rc)
                    logmsg(LOGMSG_ERROR, "bdb_blkseq_clean %d rc %d\n", stripe, rc);
            }
            thrman_where(thr_self, NULL);
        }

        /* purge old blobs.  i didn't want to make a whole new thread just
         * for this -- SJ */
        thrman_where(thr_self, "purge_old_cached_blobs");
        purge_old_cached_blobs();
        thrman_where(thr_self, NULL);

        /* queue consumer thread admin */
        thrman_where(thr_self, "dbqueue_admin");
        dbqueue_admin(dbenv);
        thrman_where(thr_self, NULL);

        /* update per node stats */
        process_nodestats();

        /* Claim is this is not needed in the new incoherency scheme
         * if I am not coherent, make sure the master hasn't forgotten about me
        if(!bdb_am_i_coherent(dbenv->bdb_env))
            send_forgetmenot();
         */

        if ((loop % 30) == 0 && gbl_verify_dbreg)
            bdb_verify_dbreg(dbenv->bdb_env);

        sleep(1);
    }

    dbenv->purge_old_blkseq_is_running = 0;
    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDONLY);
    return NULL;
}

static void *purge_old_files_thread(void *arg)
{
    struct dbenv *dbenv = (struct dbenv *)arg;
    int rc;
    tran_type *trans;
    struct ireq iq;
    int bdberr = 0;
    int empty = 0;
    int empty_pause = 5; // seconds
    int retries = 0;

    thrman_register(THRTYPE_PURGEFILES);
    thread_started("purgefiles");

    dbenv->purge_old_files_is_running = 1;
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDONLY);

    while (!db_is_stopped()) {
        /* even though we only add files to be deleted on the master,
         * don't try to delete files, ever, if you're a replicant */
        if (thedb->master != gbl_mynode) {
            sleep(empty_pause);
            continue;
        }
        if (db_is_stopped())
            continue;

        if (!bdb_have_unused_files() && gbl_master_changed_oldfiles) {
            gbl_master_changed_oldfiles = 0;
            if ((rc = bdb_process_each_table_version_entry(
                     dbenv->bdb_env, bdb_check_files_on_disk, &bdberr)) != 0) {
                logmsg(LOGMSG_ERROR,
                       "%s: bdb_list_unused_files failed with rc=%d\n",
                       __func__, rc);
                sleep(empty_pause);
                continue;
            }
        }

        init_fake_ireq(thedb, &iq);
        iq.use_handle = thedb->bdb_env;

        /* ok, get to work now */
        retries = 0;
    retry:
        rc = trans_start_sc(&iq, NULL, &trans);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s: failed to create transaction\n", __func__);
            sleep(empty_pause);
            continue;
        }

        empty = 0;
        rc = bdb_purge_unused_files(dbenv->bdb_env, trans, &bdberr);
        if (rc == 1) {
            empty = 1;
            rc = 0;
        }

        if (rc == 0) {
            rc = trans_commit(&iq, trans, gbl_mynode);
            if (rc) {
                if (rc == RC_INTERNAL_RETRY && retries < 10) {
                    retries++;
                    goto retry;
                }
                logmsg(LOGMSG_ERROR, "%s: failed to commit purged file\n", __func__);
                sleep(empty_pause);
                continue;
            }

            if (empty) {
                sleep(empty_pause);
                continue;
            }
        } else {
            logmsg(LOGMSG_ERROR,
                   "%s: bdb_purge_unused_files failed rc=%d bdberr=%d\n",
                   __func__, rc, bdberr);
            trans_abort(&iq, trans);
            sleep(empty_pause);
            continue;
        }
    }

    dbenv->purge_old_files_is_running = 0;
    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDONLY);

    return NULL;
}

/* remove every file that contains ".csc2" anywhere in its name.
   this should be safe */
int clear_csc2_files(void)
{
    char path[256];
    DIR *dirp = NULL;
    struct dirent *dp = NULL;
    bzero(path, sizeof(path));

    snprintf(path, 256, "%s", thedb->basedir);

    dirp = opendir(path);
    if (dirp == NULL)
        return -1;
    while (dirp) {
        errno = 0;
        if ((dp = readdir(dirp)) != NULL) {
            char fullfile[512];
            char *ptr;

            if (!strcmp(dp->d_name, ".") || !strcmp(dp->d_name, ".."))
                continue;

            ptr = strstr(dp->d_name, ".csc2");

            if (ptr) {
                int rc;
                sprintf(fullfile, "%s/%s", path, dp->d_name);
                logmsg(LOGMSG_INFO, "removing csc2 file %s\n", fullfile);
                rc = unlink(fullfile);
                if (rc)
                    logmsg(LOGMSG_ERROR, "unlink rc %d errno %d\n", rc, errno);
            }
        } else {
            break;
        }
    }
    closedir(dirp);
    return 0;
}

/* gets called single threaded during startup to initialize */
char *comdb2_get_tmp_dir(void)
{
    static char path[256];
    static int once = 0;

    if (!once) {
        bzero(path, sizeof(path));

        if (gbl_nonames)
            snprintf(path, 256, "%s/tmp", thedb->basedir);
        else
            snprintf(path, 256, "%s/%s.tmpdbs", thedb->basedir, thedb->envname);
    }

    return path;
}

/* check space similar to bdb/summarize.c: check_free_space()
 * dir is fetched from comdb2_get_tmp_dir()
 */
int comdb2_tmpdir_space_low() {
    char * path = comdb2_get_tmp_dir();
    int reqfree = bdb_attr_get(thedb->bdb_attr, 
            BDB_ATTR_SQLITE_SORTER_TEMPDIR_REQFREE);
    
    int has_low_headroom(const char * path, int headroom, int debug);
    return has_low_headroom(path, 100 - reqfree, 1);
}

int clear_temp_tables(void)
{
    char *path;
    DIR *dirp = NULL;
    struct dirent *dp = NULL;

    path = comdb2_get_tmp_dir();

    dirp = opendir(path);
    if (dirp == NULL)
        return -1;
    while (dirp) {
        errno = 0;
        if ((dp = readdir(dirp)) != NULL) {
            char filepath[PATH_MAX];
            if (!strcmp(dp->d_name, ".") || !strcmp(dp->d_name, ".."))
                continue;
            snprintf(filepath, sizeof(filepath) - 1, "%s/%s", path, dp->d_name);
            logmsg(LOGMSG_INFO, "removing temporary table %s\n", filepath);
            unlink(filepath);
        } else {
            break;
        }
    }
    closedir(dirp);
    return 0;
}

void clean_exit_sigwrap(int signum) {
   signal(SIGTERM, SIG_DFL);
   clean_exit();
}

static void free_sqlite_table(struct dbenv *dbenv)
{
    for (int i = dbenv->num_dbs - 1; i >= 0; i--) {
        struct dbtable *tbl = dbenv->dbs[i];
        delete_schema(tbl->tablename); // tags hash
        delete_db(tbl->tablename);     // will free db
        bdb_cleanup_fld_hints(tbl->handle);
        freedb(tbl);
    }
    free(dbenv->dbs);
}

/* clean_exit will be called to cleanup db structures upon exit
 * NB: This function can be called by clean_exit_sigwrap() when the db is not
 * up yet at which point we may not have much to cleanup.
 */
void clean_exit(void)
{
    int rc;

    thedb->exiting = 1;
    stop_threads(thedb);
    logmsg(LOGMSG_INFO, "stopping db engine...\n");

    /* TODO: (NC) Instead of sleep(), maintain a counter of threads and wait for
      them to quit.
    */
    if (!gbl_create_mode)
        sleep(4);

    cleanup_q_vars();
    cleanup_switches();
    free_gbl_tunables();
    free_tzdir();
    tz_hash_free();
    destroy_plugins();
    destroy_appsock();
    bdb_cleanup_private_blkseq(thedb->bdb_env);

    if (gbl_create_mode) {
        logmsg(LOGMSG_USER, "Created database %s.\n", thedb->envname);
    }

    rc = backend_close(thedb);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "error backend_close() rc %d\n", rc);
    }

    eventlog_stop();

    cleanup_file_locations();
    ctrace_closelog();

    backend_cleanup(thedb);
    net_cleanup_subnets();
    cleanup_sqlite_master();

    free_sqlite_table(thedb);

    if (thedb->db_hash) {
        hash_clear(thedb->db_hash);
        hash_free(thedb->db_hash);
        thedb->db_hash = NULL;
    }

    cleanup_interned_strings();
    cleanup_peer_hash();
    free(gbl_dbdir);
    free(gbl_myhostname);

    cleanresources(); // list of lrls
    clear_portmux_bind_path();
    // TODO: would be nice but other threads need to exit first:
    // comdb2ma_exit();

    logmsg(LOGMSG_USER, "goodbye\n");
    exit(0);
}

int get_elect_time_microsecs(void)
{
    if (gbl_elect_time_secs > 0) {
        /* local override has first priority */
        return gbl_elect_time_secs * 1000000;
    } else {
        /* This is set in config_init, and hasn't changed in 10 years.  Let's
         * call it
         * fixed, unless there's an override above
         */
        return 5000000;
    }

    /* No preference, bdblib will do its own thing */
    return 0;
}

/* compare cmpto againt the lrl file line lrlline to make sure that the
 * words are the same even if the whitespace is different. */
static int lrllinecmp(char *lrlline, char *cmpto)
{
    char *lrl_tok;
    int lrl_st = 0, lrl_ltok, lrl_linelen;
    char *cmp_tok;
    int cmp_st = 0, cmp_ltok, cmp_linelen;

    lrl_linelen = strlen(lrlline);
    cmp_linelen = strlen(cmpto);

    do {
        lrl_tok = segtok(lrlline, lrl_linelen, &lrl_st, &lrl_ltok);
        cmp_tok = segtok(cmpto, cmp_linelen, &cmp_st, &cmp_ltok);

        if (lrl_ltok != cmp_ltok)
            return -1;
        if (strncasecmp(lrl_tok, cmp_tok, lrl_ltok) != 0)
            return -1;
    } while (lrl_ltok);

    return 0;
}

struct dbtable *newqdb(struct dbenv *env, const char *name, int avgsz, int pagesize,
                  int isqueuedb)
{
    struct dbtable *tbl;
    int rc;

    tbl = calloc(1, sizeof(struct dbtable));
    tbl->tablename = strdup(name);
    tbl->dbenv = env;
    tbl->is_readonly = 0;
    tbl->dbtype = isqueuedb ? DBTYPE_QUEUEDB : DBTYPE_QUEUE;
    tbl->avgitemsz = avgsz;
    tbl->queue_pagesize_override = pagesize;

    if (tbl->dbtype == DBTYPE_QUEUEDB) {
        rc = pthread_rwlock_init(&tbl->consumer_lk, NULL);
        if (rc) {
            logmsg(LOGMSG_ERROR, "create consumer rwlock rc %d %s\n", rc,
                    strerror(rc));
            return NULL;
        }
    }

    return tbl;
}

void cleanup_newdb(struct dbtable *tbl)
{
    if (!tbl)
        return;

    if (tbl->tablename) {
        free(tbl->tablename);
        tbl->tablename = NULL;
    }

    if (tbl->lrlfname) {
        free(tbl->lrlfname);
        tbl->lrlfname = NULL;
    }

    if (tbl->ixuse) {
        free(tbl->ixuse);
        tbl->ixuse = NULL;
    }
    if (tbl->sqlixuse) {
        free(tbl->sqlixuse);
        tbl->sqlixuse = NULL;
    }
    free(tbl);
    tbl = NULL;
}

struct dbtable *newdb_from_schema(struct dbenv *env, char *tblname, char *fname,
                             int dbnum, int dbix, int is_foreign)
{
    struct dbtable *tbl;
    int ii;
    int tmpidxsz;
    int rc;

    tbl = calloc(1, sizeof(struct dbtable));
    if (tbl == NULL) {
        logmsg(LOGMSG_FATAL, "%s: Memory allocation error\n", __func__);
        return NULL;
    }

    tbl->dbs_idx = dbix;

    tbl->dbtype = DBTYPE_TAGGED_TABLE;
    if (fname)
        tbl->lrlfname = strdup(fname);
    tbl->tablename = strdup(tblname);
    tbl->dbenv = env;
    tbl->is_readonly = 0;
    tbl->dbnum = dbnum;
    tbl->lrl = dyns_get_db_table_size(); /* this gets adjusted later */
    if (dbnum == 0) {
        /* if no dbnumber then no default tag is required ergo lrl can be 0 */
        if (tbl->lrl < 0)
            tbl->lrl = 0;
        else if (tbl->lrl > MAXLRL) {
            logmsg(LOGMSG_ERROR, "bad data lrl %d in csc schema %s\n", tbl->lrl,
                    tblname);
            cleanup_newdb(tbl);
            return NULL;
        }
    } else {
        /* this table must have a default tag */
        int ntags, itag;
        ntags = dyns_get_table_count();
        for (itag = 0; itag < ntags; itag++) {
            if (strcasecmp(dyns_get_table_tag(itag), ".DEFAULT") == 0)
                break;
        }
        if (ntags == itag) {
            logmsg(LOGMSG_ERROR, "csc schema %s requires comdbg compatibility but "
                            "has no default tag\n",
                    tblname);
            cleanup_newdb(tbl);
            return NULL;
        }

        if (tbl->lrl < 1 || tbl->lrl > MAXLRL) {
            logmsg(LOGMSG_ERROR, "bad data lrl %d in csc schema %s\n", tbl->lrl,
                    tblname);
            cleanup_newdb(tbl);
            return NULL;
        }
    }
    tbl->nix = dyns_get_idx_count();
    if (tbl->nix > MAXINDEX) {
        logmsg(LOGMSG_ERROR, "too many indices %d in csc schema %s\n", tbl->nix,
                tblname);
        cleanup_newdb(tbl);
        return NULL;
    }
    if (tbl->nix < 0) {
        logmsg(LOGMSG_ERROR, "too few indices %d in csc schema %s\n", tbl->nix,
                tblname);
        cleanup_newdb(tbl);
        return NULL;
    }
    for (ii = 0; ii < tbl->nix; ii++) {
        tmpidxsz = dyns_get_idx_size(ii);
        if (tmpidxsz < 1 || tmpidxsz > MAXKEYLEN) {
            logmsg(LOGMSG_ERROR, "index %d bad keylen %d in csc schema %s\n", ii,
                    tmpidxsz, tblname);
            cleanup_newdb(tbl);
            return NULL;
        }
        tbl->ix_keylen[ii] = tmpidxsz; /* ix lengths are adjusted later */

        tbl->ix_dupes[ii] = dyns_is_idx_dup(ii);
        if (tbl->ix_dupes[ii] < 0) {
            logmsg(LOGMSG_ERROR, "cant find index %d dupes in csc schema %s\n", ii,
                    tblname);
            cleanup_newdb(tbl);
            return NULL;
        }

        tbl->ix_recnums[ii] = dyns_is_idx_recnum(ii);
        if (tbl->ix_recnums[ii] < 0) {
            logmsg(LOGMSG_ERROR, "cant find index %d recnums in csc schema %s\n", ii,
                    tblname);
            cleanup_newdb(tbl);
            return NULL;
        }

        tbl->ix_datacopy[ii] = dyns_is_idx_datacopy(ii);
        if (tbl->ix_datacopy[ii] < 0) {
            logmsg(LOGMSG_ERROR, "cant find index %d datacopy in csc schema %s\n",
                    ii, tblname);
            cleanup_newdb(tbl);
            return NULL;
        } else if (tbl->ix_datacopy[ii]) {
            tbl->has_datacopy_ix = 1;
        }

        tbl->ix_nullsallowed[ii] = dyns_is_idx_uniqnulls(ii);
        if (tbl->ix_nullsallowed[ii] < 0) {
          logmsg(LOGMSG_ERROR, "cant find index %d uniqnulls in csc schema %s\n",
                  ii, tblname);
          cleanup_newdb(tbl);
          return NULL;
        }
    }
    tbl->n_rev_constraints =
        0; /* this will be initialized at verification time */
    tbl->n_constraints = dyns_get_constraint_count();
    if (tbl->n_constraints > 0) {
        char *consname = NULL;
        char *keyname = NULL;
        int rulecnt = 0, flags = 0;
        if (tbl->n_constraints >= MAXCONSTRAINTS) {
            logmsg(LOGMSG_ERROR, "too many constraints for table %s (%d>=%d)\n",
                    tblname, tbl->n_constraints, MAXCONSTRAINTS);
            cleanup_newdb(tbl);
            return NULL;
        }
        for (ii = 0; ii < tbl->n_constraints; ii++) {
            rc = dyns_get_constraint_at(ii, &consname, &keyname, &rulecnt,
                                        &flags);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "Cannot get constraint at %d (cnt=%d)!\n", ii,
                        tbl->n_constraints);
                cleanup_newdb(tbl);
                return NULL;
            }
            tbl->constraints[ii].flags = flags;
            tbl->constraints[ii].lcltable = tbl;
            tbl->constraints[ii].consname = consname ? strdup(consname) : 0;
            tbl->constraints[ii].lclkeyname = strdup(keyname);
            tbl->constraints[ii].nrules = rulecnt;
            if (tbl->constraints[ii].nrules >= MAXCONSTRAINTS) {
                logmsg(LOGMSG_ERROR, "too many constraint rules for table %s:%s (%d>=%d)\n",
                        tblname, keyname, tbl->constraints[ii].nrules,
                        MAXCONSTRAINTS);
                cleanup_newdb(tbl);
                return NULL;
            } else if (tbl->constraints[ii].nrules > 0) {
                int jj = 0;
                for (jj = 0; jj < tbl->constraints[ii].nrules; jj++) {
                    char *tblnm = NULL;
                    rc = dyns_get_constraint_rule(ii, jj, &tblnm, &keyname);
                    if (rc != 0) {
                        logmsg(LOGMSG_ERROR, "cannot get constraint rule %d table %s:%s\n",
                                jj, tblname, keyname);
                        cleanup_newdb(tbl);
                        return NULL;
                    }
                    tbl->constraints[ii].table[jj] = strdup(tblnm);
                    tbl->constraints[ii].keynm[jj] = strdup(keyname);
                }
            }
        } /* for (ii...) */
    }     /* if (n_constraints > 0) */
    tbl->ixuse = calloc(tbl->nix, sizeof(unsigned long long));
    tbl->sqlixuse = calloc(tbl->nix, sizeof(unsigned long long));
    return tbl;
}

/* lock mgr partition defaults */
size_t gbl_lk_parts = 73;
size_t gbl_lkr_parts = 23;
size_t gbl_lk_hash = 32;
size_t gbl_lkr_hash = 16;

char **qdbs = NULL;
char **sfuncs = NULL;
char **afuncs = NULL;

#define llmeta_set_lua_funcs(pfx)                                              \
    do {                                                                       \
        if (pfx##funcs == NULL)                                                \
            break;                                                             \
        char **func = &pfx##funcs[0];                                          \
        while (*func) {                                                        \
            int bdberr;                                                        \
            int rc = bdb_llmeta_add_lua_##pfx##func(*func, &bdberr);           \
            if (rc) {                                                          \
               logmsg(LOGMSG_ERROR, "could not add sql lua " #pfx "func:%s to llmeta\n",\
                       *func);                                                 \
                return -1;                                                     \
            } else {                                                           \
               logmsg(LOGMSG_INFO, "Added Lua SQL " #pfx "func:%s\n", *func);  \
            }                                                                  \
            ++func;                                                            \
        }                                                                      \
    } while (0)

#define llmeta_load_lua_funcs(pfx)                                             \
    do {                                                                       \
        int bdberr = 0;                                                        \
        int rc = bdb_llmeta_get_lua_##pfx##funcs(                              \
            &thedb->lua_##pfx##funcs, &thedb->num_lua_##pfx##funcs, &bdberr);  \
        if (rc) {                                                              \
            logmsg(LOGMSG_ERROR, "bdb_llmeta_get_lua_" #pfx "funcs bdberr:%d\n",\
                    bdberr);                                                   \
        }                                                                      \
        logmsg(LOGMSG_INFO, "loaded num_lua_" #pfx "funcs:%d\n",               \
               thedb->num_lua_##pfx##funcs);                                   \
        return rc;                                                             \
    } while (0)

#define get_funcs(funcs, num_funcs, pfx)                                       \
    do {                                                                       \
        *funcs = thedb->lua_##pfx##funcs;                                      \
        *num_funcs = thedb->num_lua_##pfx##funcs;                              \
    } while (0)

#define find_lua_func(name, pfx)                                               \
    do {                                                                       \
        int i;                                                                 \
        rdlock_schema_lk();                                                    \
        for (i = 0; i < thedb->num_lua_##pfx##funcs; ++i) {                    \
            if (strcmp(thedb->lua_##pfx##funcs[i], name) == 0)                 \
                break;                                                         \
        }                                                                      \
        i = i < thedb->num_lua_##pfx##funcs;                                   \
        unlock_schema_lk();                                                    \
        return i;                                                              \
    } while (0)

int llmeta_load_lua_sfuncs() { llmeta_load_lua_funcs(s); }

int llmeta_load_lua_afuncs() { llmeta_load_lua_funcs(a); }

void get_sfuncs(char ***funcs, int *num_funcs)
{
    get_funcs(funcs, num_funcs, s);
}

void get_afuncs(char ***funcs, int *num_funcs)
{
    get_funcs(funcs, num_funcs, a);
}

int find_lua_sfunc(const char *name) { find_lua_func(name, s); }

int find_lua_afunc(const char *name) { find_lua_func(name, a); }

void check_access_controls(struct dbenv *dbenv)
{
    int rc;
    int bdberr;

    rc = bdb_authentication_get(dbenv->bdb_env, NULL, &bdberr);

    if (rc == 0) {
        if (!gbl_uses_password) {
            gbl_uses_password = 1;
            gbl_upgrade_blocksql_2_socksql = 1;
            logmsg(LOGMSG_INFO, "User authentication enabled\n");
        }
    } else {
        gbl_uses_password = 0;
        logmsg(LOGMSG_WARN, "User authentication disabled\n");
    }

    rc = bdb_accesscontrol_tableXnode_get(dbenv->bdb_env, NULL, &bdberr);
    if (rc != 0) {
        gbl_uses_accesscontrol_tableXnode = 0;
        return;
    }

    if (!bdb_access_create(dbenv->bdb_env, &bdberr)) {
        logmsg(LOGMSG_ERROR, "fail to enable tableXnode control bdberr=%d\n",
                bdberr);
        gbl_uses_accesscontrol_tableXnode = 0;
        return;
    }

    gbl_uses_accesscontrol_tableXnode = 1;
    logmsg(LOGMSG_INFO, "access control tableXnode enabled\n");
}

int llmeta_load_tables_older_versions(struct dbenv *dbenv)
{
    int rc = 0, bdberr, dbnums[MAX_NUM_TABLES], fndnumtbls, i;
    char *tblnames[MAX_NUM_TABLES];
    struct dbtable *tbl;

    /* nothing to do */
    if (gbl_create_mode)
        return 0;

    /* re-load the tables from the low level metatable */
    if (bdb_llmeta_get_tables(NULL, tblnames, dbnums, sizeof(tblnames),
                              &fndnumtbls, &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "couldn't load tables from low level meta table"
                        "\n");
        return 1;
    }

    for (i = 0; i < fndnumtbls; ++i) {
        int ver;
        int bdberr;

        tbl = get_dbtable_by_name(tblnames[i]);
        if (tbl == NULL) {
            if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_IGNORE_BAD_TABLE)) {
                logmsg(LOGMSG_ERROR, "ignoring missing table %s\n",
                       tblnames[i]);
                continue;
            }
            logmsg(LOGMSG_ERROR, "Can't find handle for table %s\n",
                   tblnames[i]);
            rc = -1;
            goto cleanup;
        }

        rc = bdb_get_csc2_highest(NULL, tblnames[i], &ver, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "couldn't get highest version number for %s\n",
                    tblnames[i]);
            rc = 1;
            goto cleanup;
        }

        int isc = 0;
        get_db_instant_schema_change(tbl, &isc);
        if (isc) {
            /* load schema for older versions */
            for (int v = 1; v <= ver; ++v) {
                char *csc2text;
                if (get_csc2_file(tbl->tablename, v, &csc2text, NULL)) {
                    logmsg(LOGMSG_ERROR, "get_csc2_file failed %s:%d\n", __FILE__,
                            __LINE__);
                    continue;
                }

                struct schema *s =
                    create_version_schema(csc2text, v, tbl->dbenv);

                if (s == NULL) {
                    free(csc2text);
                    rc = 1;
                    goto cleanup;
                }

                add_tag_schema(tbl->tablename, s);
                free(csc2text);
            }
        }
    }
cleanup:
    for (i = 0; i < fndnumtbls; ++i) {
        free(tblnames[i]);
    }

    return rc;
}

static int llmeta_load_queues(struct dbenv *dbenv)
{
    char *qnames[MAX_NUM_QUEUES];
    int fnd_queues;
    int rc;
    int bdberr;

    rc = bdb_llmeta_get_queues(qnames, MAX_NUM_QUEUES, &fnd_queues, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "bdb_llmeta_get_queues bdberr %d\n", bdberr);
        return rc;
    }

    if (fnd_queues == 0)
        return 0;

    dbenv->qdbs = realloc(dbenv->qdbs,
                          (dbenv->num_qdbs + fnd_queues) * sizeof(struct dbtable *));
    if (dbenv->qdbs == NULL) {
        logmsg(LOGMSG_ERROR, "can't allocate memory for queue list\n");
        return -1;
    }
    for (int i = 0; i < fnd_queues; i++) {
        struct dbtable *tbl;
        char **dests;
        int ndests;
        char *config;

        tbl = newqdb(dbenv, qnames[i],
                    65536 /* TODO: pass from comdb2sc, store in llmeta? */,
                    65536, 1);
        if (tbl == NULL) {
            logmsg(LOGMSG_ERROR, "can't create queue \"%s\"\n", qnames[i]);
            return -1;
        }
        dbenv->qdbs[dbenv->num_qdbs++] = tbl;

        /* Add queue the hash. */
        hash_add(dbenv->qdb_hash, tbl);

        rc = bdb_llmeta_get_queue(qnames[i], &config, &ndests, &dests, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "can't get information for queue \"%s\"\n",
                    qnames[i]);
            return -1;
        }

        rc = dbqueue_add_consumer(tbl, 0, dests[0], 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "can't add consumer for queue \"%s\"\n", qnames[i]);
            return -1;
        }

        /* create a procedure (needs to go away, badly) */
        rc = javasp_load_procedure_int(qnames[i], NULL, config);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: javasp_load_procedure_int returned rc %d\n",
                    __func__, rc);
            return -1;
        }

        /* the final step (starting consumers) requires the dbs to be open, so
         * we defer that
         * until after backend_open() is done. */
    }

    return 0;
}

/* gets the table names and dbnums from the low level meta table and sets up the
 * dbenv accordingly.  returns 0 on success and anything else otherwise */
static int llmeta_load_tables(struct dbenv *dbenv, char *dbname)
{
    int rc = 0, bdberr, dbnums[MAX_NUM_TABLES], fndnumtbls, i;
    char *tblnames[MAX_NUM_TABLES];
    struct dbtable *tbl;

    /* load the tables from the low level metatable */
    if (bdb_llmeta_get_tables(NULL, tblnames, dbnums, sizeof(tblnames),
                              &fndnumtbls, &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "couldn't load tables from low level meta table"
                        "\n");
        return 1;
    }

    /* set generic settings, likely already set when env was opened, but make
     * sure */
    bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_GENIDS, 1);

    /* make room for dbs */
    dbenv->dbs = realloc(dbenv->dbs, fndnumtbls * sizeof(struct dbtable *));

    for (i = 0; i < fndnumtbls; ++i) {
        char *csc2text = NULL;
        int ver;
        int bdberr;

        /* if db number matches parent database db number then
         * table name must match parent database name.  otherwise
         * we get mysterious failures to receive qtraps (setting
         * taskname to something not our task name causes initque
         * to fail, and the ldgblzr papers over this) */
        if (dbenv->dbnum && dbnums[i] == dbenv->dbnum &&
            strcasecmp(dbname, tblnames[i]) != 0) {
            logmsg(LOGMSG_ERROR, "Table %s has same db number as parent database but "
                   "different name\n",
                   tblnames[i]);
            rc = 1;
            break;
        }

        /* get schema version from llmeta */
        rc = bdb_get_csc2_highest(NULL, tblnames[i], &ver, &bdberr);
        if (rc)
            break;

        /* create latest version of db */
        rc = get_csc2_file(tblnames[i], ver, &csc2text, NULL);
        if (rc) {
            logmsg(LOGMSG_ERROR, "get_csc2_file failed %s:%d\n", __FILE__, __LINE__);
            break;
        }
        rc = dyns_load_schema_string(csc2text, dbname, tblnames[i]);
        if (rc) {
            logmsg(LOGMSG_ERROR, "dyns_load_schema_string failed %s:%d\n", __FILE__,
                    __LINE__);
            break;
        }
        free(csc2text);
        csc2text = NULL;
        tbl = newdb_from_schema(dbenv, tblnames[i], NULL, dbnums[i], i, 0);
        if (tbl == NULL) {
            logmsg(LOGMSG_ERROR, "newdb_from_schema failed %s:%d\n", __FILE__,
                    __LINE__);
            rc = 1;
            break;
        }
        tbl->version = ver;

        /* We only want to load older schema versions for ODH databases.  ODH
         * information
         * is stored in the meta table (not the llmeta table), so it's not
         * loaded yet. */

        /* set tbl values and add to env */
        tbl->dbs_idx = i;
        dbenv->dbs[i] = tbl;

        /* Add table to the hash. */
        hash_add(dbenv->db_hash, tbl);

        /* just got a bunch of data. remember it so key forming
           routines and SQL can get at it */
        rc = add_cmacc_stmt(tbl, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "Failed to load schema: can't process schema file "
                   "%s\n",
                   tbl->tablename);
            ++i; /* this tblname has already been marshalled so we dont want to
                  * free it below */
            rc = 1;
            break;
        }

        /* Free the table name. */
        free(tblnames[i]);
    }

    /* we have to do this after all the meta table lookups so that the hack in
     * get_meta_int works */
    dbenv->num_dbs = fndnumtbls;

    /* if we quit early bc of an error free the rest */
    while (i < fndnumtbls)
        free(tblnames[i++]);

    return rc;
}

int llmeta_load_timepart(struct dbenv *dbenv)
{
    /* We need to do this before resuming schema chabge , if any */
    logmsg(LOGMSG_INFO, "Reloading time partitions\n");
    dbenv->timepart_views = timepart_views_init(dbenv);

    return thedb->timepart_views ? 0 : -1;
}

/* replace the table names and dbnums saved in the low level meta table with the
 * ones in the dbenv.  returns 0 on success and anything else otherwise */
int llmeta_set_tables(tran_type *tran, struct dbenv *dbenv)
{
    int i, bdberr, dbnums[MAX_NUM_TABLES];
    char *tblnames[MAX_NUM_TABLES];

    /* gather all the table names and tbl numbers */
    for (i = 0; i < dbenv->num_dbs; ++i) {
        tblnames[i] = dbenv->dbs[i]->tablename;
        dbnums[i] = dbenv->dbs[i]->dbnum;
    }

    /* put the values in the low level meta table */
    if (bdb_llmeta_set_tables(tran, tblnames, dbnums, dbenv->num_dbs,
                              &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "couldn't set tables in low level meta table\n");
        return 1;
    }

    return 0; /* success */
}

/* prints out a file (datadir/dbname_file_vers_map) that provides a mapping of
 * all the file types and numbers to version numbers, for example,
 * for each table the file will have output
 * similar to:
 * table tablename
 *      data files: version_num
 *      blob files
 *          blob num 1: version_num
 *          blob num 2: version_num
 *          ...
 *      index files
 *          index 0: version_num
 *          index 1: version_num
 *          ...
 * ...
 *
 * the db never uses this file it is only to make it easier for people to tell
 * what files belong to what parts of a table, etc */
int llmeta_dump_mapping_tran(void *tran, struct dbenv *dbenv)
{
    int i, rc;
    char *fname, fname_tail[] = "_file_vers_map";
    int file, fnamelen;
    SBUF2 *sbfile;

    /* get the mem we need for fname */
    fnamelen = strlen(dbenv->basedir) + strlen(dbenv->envname) +
               strlen(fname_tail) + 2 /* one for / and one for NULL byte */;
    fname = malloc(fnamelen);
    if (!fname) {
        logmsg(LOGMSG_ERROR, "llmeta_dump_mapping: failed to malloc file name\n");
        return -1;
    }

    /* construct the file's name */
    if (gbl_nonames)
        rc = snprintf(fname, fnamelen, "%s/file_vers_map", dbenv->basedir);
    else
        rc = snprintf(fname, fnamelen, "%s/%s%s", dbenv->basedir,
                      dbenv->envname, fname_tail);

    if (rc < 0 || rc >= fnamelen) {
        logmsg(LOGMSG_ERROR, "llmeta_dump_mapping: failed to create file name\n");
        free(fname);
        return -1;
    }

    /* open file */
    file = open(fname, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    free(fname);
    if (file == -1) {
        logmsg(LOGMSG_ERROR, "llmeta_dump_mapping: failed to open %s for writing\n",
                fname);
        return -1;
    }
    sbfile = sbuf2open(file, 0);
    if (!sbfile) {
        logmsg(LOGMSG_ERROR, "llmeta_dump_mapping: failed to open sbuf2\n");
        close(file);
        return -1;
    }

    rc = 0;

    /* print out the versions of each table's files */
    for (i = 0; i < dbenv->num_dbs; ++i) {
        int j, bdberr;
        unsigned long long version_num;

        /* print the main data file's version number */
        if (bdb_get_file_version_data(dbenv->dbs[i]->handle, tran, 0 /*dtanum*/,
                                      &version_num, &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "llmeta_dump_mapping: failed to fetch version "
                                 "number for %s's main data files\n",
                   dbenv->dbs[i]->tablename);
            rc = -1;
            goto done;
        }

        sbuf2printf(sbfile,
                    "table %s %d\n\tdata files: %016llx\n\tblob files\n",
                    dbenv->dbs[i]->tablename, dbenv->dbs[i]->lrl,
                    flibc_htonll(version_num));

        /* print the indicies' version numbers */
        for (j = 1; j <= dbenv->dbs[i]->numblobs; ++j) {
            if (bdb_get_file_version_data(dbenv->dbs[i]->handle, tran,
                                          j /*dtanum*/, &version_num,
                                          &bdberr) ||
                bdberr != BDBERR_NOERROR) {
                logmsg(LOGMSG_ERROR,
                       "llmeta_dump_mapping: failed to fetch version "
                       "number for %s's blob num %d's files\n",
                       dbenv->dbs[i]->tablename, j);
                rc = -1;
                goto done;
            }

            sbuf2printf(sbfile, "\t\tblob num %d: %016llx\n", j,
                        flibc_htonll(version_num));
        }

        /* print the indicies' version numbers */
        sbuf2printf(sbfile, "\tindex files\n");
        for (j = 0; j < dbenv->dbs[i]->nix; ++j) {
            if (bdb_get_file_version_index(dbenv->dbs[i]->handle, tran,
                                           j /*dtanum*/, &version_num,
                                           &bdberr) ||
                bdberr != BDBERR_NOERROR) {
                logmsg(LOGMSG_ERROR,
                       "llmeta_dump_mapping: failed to fetch version "
                       "number for %s's index num %d\n",
                       dbenv->dbs[i]->tablename, j);
                rc = -1;
                goto done;
            }

            sbuf2printf(sbfile, "\t\tindex num %d: %016llx\n", j,
                        flibc_htonll(version_num));
        }
    }

done:
    sbuf2close(sbfile);
    return rc;
}

int llmeta_dump_mapping(struct dbenv *dbenv)
{
    return llmeta_dump_mapping_tran(NULL, dbenv);
}

int llmeta_dump_mapping_table_tran(void *tran, struct dbenv *dbenv,
                                   const char *table, int err)
{
    int i;
    int bdberr;
    unsigned long long version_num;
    struct dbtable *p_db;

    if (!(p_db = get_dbtable_by_name(table)))
        return -1;

    /* print out the versions of each of the table's files */

    /* print the main data file's version number */
    if (bdb_get_file_version_data(p_db->handle, tran, 0 /*dtanum*/,
                                  &version_num, &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        if (err)
            logmsg(LOGMSG_ERROR, "llmeta_dump_mapping: failed to fetch version "
                                 "number for %s's main data files\n",
                   p_db->tablename);
        else
            ctrace("llmeta_dump_mapping: failed to fetch version number for "
                   "%s's main data files\n",
                   p_db->tablename);
        return -1;
    }

    if (err)
        logmsg(LOGMSG_INFO, "table %s\n\tdata files: %016lx\n\tblob files\n",
               p_db->tablename, flibc_htonll(version_num));
    else
        ctrace("table %s\n\tdata files: %016llx\n\tblob files\n",
               p_db->tablename,
               (long long unsigned int)flibc_htonll(version_num));

    /* print the blobs' version numbers */
    for (i = 1; i <= p_db->numblobs; ++i) {
        if (bdb_get_file_version_data(p_db->handle, tran, i /*dtanum*/,
                                      &version_num, &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            if (err)
                logmsg(LOGMSG_ERROR,
                       "llmeta_dump_mapping: failed to fetch version "
                       "number for %s's blob num %d's files\n",
                       p_db->tablename, i);
            else
                ctrace("llmeta_dump_mapping: failed to fetch version number "
                       "for %s's blob num %d's files\n",
                       p_db->tablename, i);
            return -1;
        }
        if (err)
            logmsg(LOGMSG_INFO, "\t\tblob num %d: %016lx\n", i,
                   flibc_htonll(version_num));
        else
            ctrace("\t\tblob num %d: %016llx\n", i,
                   (long long unsigned int)flibc_htonll(version_num));
    }

    /* print the indicies' version numbers */
    logmsg(LOGMSG_INFO, "\tindex files\n");
    for (i = 0; i < p_db->nix; ++i) {
        if (bdb_get_file_version_index(p_db->handle, tran, i /*dtanum*/,
                                       &version_num, &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            if (err)
                logmsg(LOGMSG_ERROR,
                       "llmeta_dump_mapping: failed to fetch version "
                       "number for %s's index num %d\n",
                       p_db->tablename, i);
            else
                ctrace("llmeta_dump_mapping: failed to fetch version number "
                       "for %s's index num %d\n",
                       p_db->tablename, i);
            return -1;
        }

        if (err)
            logmsg(LOGMSG_INFO, "\t\tindex num %d: %016lx\n", i,
                   flibc_htonll(version_num));
        else
            ctrace("\t\tindex num %d: %016llx\n", i,
                   (long long unsigned int)flibc_htonll(version_num));
    }

    return 0;
}

int llmeta_dump_mapping_table(struct dbenv *dbenv, const char *table, int err)
{
    return llmeta_dump_mapping_table_tran(NULL, dbenv, table, err);
}

static struct dbenv *newdbenv(char *dbname, char *lrlname)
{
    int rc;
    struct dbenv *dbenv = calloc(1, sizeof(struct dbenv));
    if (dbenv == 0) {
        logmsg(LOGMSG_FATAL, "newdb:calloc dbenv");
        return NULL;
    }

    dbenv->cacheszkbmin = 65536;
    dbenv->bdb_attr = bdb_attr_create();

    /* default retry = 10 seconds.  this used to be 180 seconds (3 minutes)
     * which was a complete farce really since the proxy ignored it and used
     * a value of 10 anyway. */
    dbenv->retry = 10;

    /*default sync mode:*/
    dbenv->rep_sync = REP_SYNC_FULL;
    dbenv->log_sync_time = 10;        /*sync logs every n seconds */
    dbenv->log_mem_size = 128 * 1024; /*sync logs every n seconds */
    dbenv->log_delete = 1;            /*delete logs.*/
    dbenv->log_delete_filenum = -1;
    listc_init(&dbenv->log_delete_state_list,
               offsetof(struct log_delete_state, linkv));
    pthread_mutex_init(&dbenv->log_delete_counter_mutex, NULL);

    /* assume I want a cluster, unless overridden by -local or cluster none */

    dbenv->envname = strdup(dbname);

    listc_init(&dbenv->managed_participants,
               offsetof(struct managed_component, lnk));
    listc_init(&dbenv->managed_coordinators,
               offsetof(struct managed_component, lnk));
    pthread_mutex_init(&dbenv->incoherent_lk, NULL);

    /* Initialize the table/queue hashes. */
    dbenv->db_hash =
        hash_init_user((hashfunc_t *)strhashfunc, (cmpfunc_t *)strcmpfunc,
                       offsetof(struct dbtable, tablename), 0);
    dbenv->qdb_hash =
        hash_init_user((hashfunc_t *)strhashfunc, (cmpfunc_t *)strcmpfunc,
                       offsetof(struct dbtable, tablename), 0);

    if ((rc = pthread_mutex_init(&dbenv->dbqueue_admin_lk, NULL)) != 0) {
        logmsg(LOGMSG_FATAL, "can't init lock %d %s\n", rc, strerror(errno));
        return NULL;
    }

    /* Register all db tunables. */
    if ((register_db_tunables(dbenv))) {
        logmsg(LOGMSG_FATAL, "Failed to initialize tunables\n");
        exit(1);
    }

    if (read_lrl_files(dbenv, lrlname)) {
        logmsg(LOGMSG_FATAL, "Failure in reading lrl file(s)\n");
        exit(1);
    }

    logmsg(LOGMSG_INFO, "database %s starting\n", dbenv->envname);

    if (!dbenv->basedir) {
        logmsg(LOGMSG_FATAL, "DB directory is not set in lrl\n");
        return NULL;
    } else if (gbl_create_mode) {
        /* make sure the database directory exists! */
        rc = mkdir(dbenv->basedir, 0774);
        if (rc && errno != EEXIST) {
            logmsg(LOGMSG_ERROR, "mkdir(%s): %s\n", dbenv->basedir,
                   strerror(errno));
            /* continue, this will make us fail later */
        }
    } else {
        struct stat sb;
        stat(dbenv->basedir, &sb);
        if (!S_ISDIR(sb.st_mode)) {
            logmsg(LOGMSG_FATAL, "DB directory '%s' does not exist\n",
                   dbenv->basedir);
            return NULL;
        }
    }

    tz_hash_init();
    init_sql_hint_table();
    init_clientstats_table();

    dbenv->long_trn_table = hash_init(sizeof(unsigned long long));

    if (dbenv->long_trn_table == NULL) {
        logmsg(LOGMSG_ERROR, "couldn't allocate long transaction lookup table\n");
        return NULL;
    }
    if (pthread_mutex_init(&dbenv->long_trn_mtx, NULL) != 0) {
        logmsg(LOGMSG_ERROR, "couldn't allocate long transaction lookup table\n");
        hash_free(dbenv->long_trn_table);
        dbenv->long_trn_table = NULL;
        return NULL;
    }

    if (gbl_local_mode) {
        /*force no siblings for local mode*/
        dbenv->nsiblings = 0;
        dbenv->rep_sync = REP_SYNC_NONE;
        dbenv->log_sync = 0;
        dbenv->log_sync_time = 30;
        dbenv->log_mem_size = 1024 * 1024;
        dbenv->log_delete = 1; /*delete logs.*/
    }

    listc_init(&dbenv->sql_threads, offsetof(struct sql_thread, lnk));
    listc_init(&dbenv->sqlhist, offsetof(struct sql_hist, lnk));
    dbenv->master = NULL; /*no known master at this point.*/
    dbenv->errstaton = 1; /* ON */

    return dbenv;
}

#ifndef BERKDB_46
extern pthread_key_t DBG_FREE_CURSOR;
#endif

/* check that we don't have name clashes, and other sanity checks, this also
 * populates some values like reverse constraints and db->dtastripe */
static int db_finalize_and_sanity_checks(struct dbenv *dbenv)
{
    int have_bad_schema = 0, ii, jj;

    if (!dbenv->num_dbs) {
        have_bad_schema = 1;
        logmsg(LOGMSG_FATAL, "No tables have been loaded.");
    }

    for (ii = 0; ii < dbenv->num_dbs; ii++) {
        dbenv->dbs[ii]->dtastripe = 1;

        for (jj = 0; jj < dbenv->num_dbs; jj++) {
            if (jj != ii) {
                if (strcasecmp(dbenv->dbs[ii]->tablename,
                               dbenv->dbs[jj]->tablename) == 0) {
                    have_bad_schema = 1;
                    logmsg(LOGMSG_FATAL,
                           "Two tables have identical names (%s) tblnums %d "
                           "%d\n",
                           dbenv->dbs[ii]->tablename, ii, jj);
                }
            }
        }

        if ((strcasecmp(dbenv->dbs[ii]->tablename, dbenv->envname) == 0) &&
            (dbenv->dbs[ii]->dbnum != 0) &&
            (dbenv->dbnum != dbenv->dbs[ii]->dbnum)) {

            have_bad_schema = 1;
            logmsg(LOGMSG_FATAL, "Table name and database name conflict (%s) tblnum %d\n",
                   dbenv->envname, ii);
        }

        if (dbenv->dbs[ii]->nix > MAXINDEX) {
            have_bad_schema = 1;
            logmsg(LOGMSG_FATAL, "Database %s has too many indexes (%d)\n",
                   dbenv->dbs[ii]->tablename, dbenv->dbs[ii]->nix);
        }

        /* last ditch effort to stop invalid schemas getting through */
        for (jj = 0; jj < dbenv->dbs[ii]->nix && jj < MAXINDEX; jj++)
            if (dbenv->dbs[ii]->ix_keylen[jj] > MAXKEYLEN) {
                have_bad_schema = 1;
                logmsg(LOGMSG_FATAL, "Database %s index %d too large (%d)\n",
                       dbenv->dbs[ii]->tablename, jj,
                       dbenv->dbs[ii]->ix_keylen[jj]);
            }

        /* verify constraint names and add reverse constraints here */
        if (populate_reverse_constraints(dbenv->dbs[ii]))
            have_bad_schema = 1;
    }

    return have_bad_schema;
}

static int dump_queuedbs(char *dir)
{
    for (int i = 0;
         i < thedb->num_qdbs && thedb->qdbs[i]->dbtype == DBTYPE_QUEUEDB; ++i) {
        char *config;
        int ndests;
        char **dests;
        int bdberr;
        char *name = thedb->qdbs[i]->tablename;
        int rc;
        rc = bdb_llmeta_get_queue(name, &config, &ndests, &dests, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Can't get data for %s: bdberr %d\n",
                   thedb->qdbs[i]->tablename, bdberr);
            return -1;
        }
        char path[PATH_MAX];
        snprintf(path, sizeof(path), REPOP_QDB_FMT, dir, thedb->envname, i);
        FILE *f = fopen(path, "w");
        if (f == NULL) {
            logmsg(LOGMSG_ERROR, "%s:fopen(\"%s\"):%s\n", __func__, path,
                    strerror(errno));
            return -1;
        }
        fprintf(f, "%s\n%d\n", thedb->qdbs[i]->tablename, ndests);
        for (int j = 0; j < ndests; ++j) {
            fprintf(f, "%s\n", dests[j]);
        }
        fprintf(f, "%s", config);
        fclose(f);
        logmsg(LOGMSG_INFO, "%s wrote file:%s for queuedb:%s\n", __func__, path,
               thedb->qdbs[i]->tablename);
        free(config);
        for (int j = 0; j < ndests; ++j)
            free(dests[j]);
        free(dests);
    }
    return 0;
}

int repopulate_lrl(const char *p_lrl_fname_out)
{
    /* can't put this on stack, it will overflow appsock thread */
    struct {
        char lrl_fname_out_dir[256];
        size_t lrl_fname_out_dir_len;

        char *p_table_names[MAX_NUM_TABLES];
        char csc2_paths[MAX_NUM_TABLES][256];
        char *p_csc2_paths[MAX_NUM_TABLES];
        int table_nums[MAX_NUM_TABLES];
    } * p_data;
    int i;

    /* make sure the output path is absolute */
    if (p_lrl_fname_out[0] != '/') {
        logmsg(LOGMSG_ERROR, "%s: output lrl fname is must be absolute: %s\n",
                __func__, p_lrl_fname_out);
        return -1;
    }

    if (!(p_data = malloc(sizeof(*p_data)))) {
        logmsg(LOGMSG_ERROR, "%s: malloc failed\n", __func__);
        return -1;
    }

    /* pull out the out lrl's path */

    p_data->lrl_fname_out_dir_len =
        strrchr(p_lrl_fname_out, '/') - p_lrl_fname_out;
    if ((p_data->lrl_fname_out_dir_len + 1 /*NUL byte*/) >
        sizeof(p_data->lrl_fname_out_dir)) {
        logmsg(LOGMSG_ERROR, "%s: output lrl dir too long: %s\n", __func__,
                p_lrl_fname_out);

        free(p_data);
        return -1;
    }

    strncpy(p_data->lrl_fname_out_dir, p_lrl_fname_out,
            p_data->lrl_fname_out_dir_len);
    p_data->lrl_fname_out_dir[p_data->lrl_fname_out_dir_len] = '\0';

    /* collect all of the table's info */
    for (i = 0; i < thedb->num_dbs; ++i) {
        /* come up with the csc2's fname in the out lrl's dir */
        if (get_csc2_fname(thedb->dbs[i], p_data->lrl_fname_out_dir,
                           p_data->csc2_paths[i],
                           sizeof(p_data->csc2_paths[i]))) {
            logmsg(LOGMSG_ERROR, "%s: get_csc2_fname failed for: %s\n",
                   __func__, thedb->dbs[i]->tablename);

            free(p_data);
            return -1;
        }

        /* dump the csc2 */
        if (dump_table_csc2_to_disk_fname(thedb->dbs[i],
                                          p_data->csc2_paths[i])) {
            logmsg(LOGMSG_ERROR,
                   "%s: dump_table_csc2_to_disk_fname failed for: "
                   "%s\n",
                   __func__, thedb->dbs[i]->tablename);

            free(p_data);
            return -1;
        }

        p_data->p_table_names[i] = thedb->dbs[i]->tablename;
        p_data->p_csc2_paths[i] = p_data->csc2_paths[i];
        p_data->table_nums[i] = thedb->dbs[i]->dbnum;
    }

    int has_sp =
        dump_spfile(p_data->lrl_fname_out_dir, thedb->envname, SP_FILE_NAME);

    if (dump_queuedbs(p_data->lrl_fname_out_dir) != 0) {
        free(p_data);
        return -1;
    }

    /* write out the lrl */
    if (rewrite_lrl_un_llmeta(getresourcepath("lrl"), p_lrl_fname_out,
                              p_data->p_table_names, p_data->p_csc2_paths,
                              p_data->table_nums, thedb->num_dbs,
                              p_data->lrl_fname_out_dir, has_sp)) {
        logmsg(LOGMSG_ERROR, "%s: rewrite_lrl_un_llmeta failed\n", __func__);

        free(p_data);
        return -1;
    }

    free(p_data);
    return 0;
}

int llmeta_open(void)
{
    /* now that we have bdb_env open, we can get at llmeta */
    char llmetaname[256];
    int bdberr = 0;

    /*get the table's name*/
    if (gbl_nonames)
        snprintf(llmetaname, sizeof(llmetaname), "comdb2_llmeta");
    else
        snprintf(llmetaname, sizeof(llmetaname), "%s.llmeta", thedb->envname);

    /*open the table*/
    if (bdb_llmeta_open(llmetaname, thedb->basedir, thedb->bdb_env,
                        0 /*create_override*/, &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_FATAL, "Failed to open low level meta table, rc: %d\n",
               bdberr);
        return -1;
    }
    return 0;
}

static void get_txndir(char *txndir, size_t sz_txndir)
{
    if (gbl_nonames)
        snprintf(txndir, sz_txndir, "%s/logs", thedb->basedir);
    else
        snprintf(txndir, sz_txndir, "%s/%s.txn", thedb->basedir, gbl_dbname);
}

static void get_savdir(char *savdir, size_t sz_savdir)
{
    if (gbl_nonames)
        snprintf(savdir, sz_savdir, "%s/savs", thedb->basedir);
    else
        snprintf(savdir, sz_savdir, "%s/%s.sav", thedb->basedir, gbl_dbname);
}

const char *get_basedir(void) { return thedb->basedir; }

typedef struct extentsentry {
    uint64_t num;
    char name[PATH_MAX];
    LIST_ENTRY(extentsentry) link;
} ExtentsEntry;

typedef struct {
    char name[PATH_MAX];
    unsigned count;
    LIST_HEAD(, extentsentry) head;
} ExtentsQueue;

// sort desc
static int cmp_extents(const void *a, const void *b)
{
    uint64_t i = *(uint64_t *)a, j = *(uint64_t *)b;
    return i < j ? +1 : i > j ? -1 : 0;
}

static int purge_extents(void *obj, void *arg)
{
    ExtentsQueue *q = obj;
    uint64_t nums[q->count];
    ExtentsEntry *e, *del = NULL;
    int i = 0;
    LIST_FOREACH(e, &q->head, link)
    {
        free(del);
        del = e;
        nums[i++] = e->num;
    }
    free(del);
    qsort(nums, i, sizeof(nums[0]), cmp_extents);
    int j = 1;
    while (j < i) {
        if (nums[j - 1] - nums[j] != 1)
            break;
        ++j;
    }
    if (j != i) {
        logmsg(LOGMSG_ERROR, "queue:%s has gap in extents %" PRIu64 " -> %" PRIu64
               "..%" PRIu64 "\n",
               q->name, nums[j - 1], nums[j], nums[i - 1]);
        char txndir[PATH_MAX], savdir[PATH_MAX];
        get_txndir(txndir, sizeof(txndir));
        get_savdir(savdir, sizeof(savdir));
        if (mkdir(savdir, 0774) != 0 && errno != EEXIST) {
            logmsg(LOGMSG_ERROR, "mkdir(%s): %s\n", savdir, strerror(errno));
        }
        while (j < i) {
            int qlen, slen;
            char qfile[PATH_MAX], sfile[PATH_MAX];
            qlen = snprintf(qfile, PATH_MAX, "%s/__dbq.%s.queue.%" PRIu64,
                            txndir, q->name, nums[j]);
            slen = snprintf(sfile, PATH_MAX, "%s/__dbq.%s.queue.%" PRIu64,
                            savdir, q->name, nums[j]);
            if (qlen >= sizeof(qfile) || slen >= sizeof(sfile)) {
                logmsg(LOGMSG_ERROR, "Truncated paths %s and %s\n", qfile,
                       sfile);
            }
            if (rename(qfile, sfile) == 0) {
                logmsg(LOGMSG_INFO, "%s -> %s\n", qfile, sfile);
            } else {
                logmsg(LOGMSG_ERROR, "%s -> %s failed:%s\n", qfile, sfile,
                       strerror(errno));
            }
            ++j;
        }
    }
    free(q);
    return 0;
}

static void clear_queue_extents(void)
{
    DIR *dir;
    struct dirent *entry;
    char txndir[PATH_MAX];
    get_txndir(txndir, sizeof(txndir));
    if ((dir = opendir(txndir)) == NULL) {
        if (!gbl_create_mode) {
            logmsg(LOGMSG_ERROR, "%s failed opendir(%s): %s\n", __func__,
                   txndir, strerror(errno));
        }
        return;
    }
    hash_t *hash_table = hash_init_str(offsetof(ExtentsQueue, name));
    while ((entry = readdir(dir)) != NULL) {
        ExtentsQueue *q;
        char file[PATH_MAX], *__dbq, *name, *queue, *num, *ptr = NULL;
        if (strncmp(entry->d_name, "__dbq", 5) != 0)
            continue;
        strcpy(file, entry->d_name);
        if ((__dbq = strtok_r(file, ".", &ptr)) == NULL)
            continue;
        if ((name = strtok_r(NULL, ".", &ptr)) == NULL)
            continue;
        if ((queue = strtok_r(NULL, ".", &ptr)) == NULL)
            continue;
        if ((num = strtok_r(NULL, ".", &ptr)) == NULL)
            continue;
        if (strcmp(queue, "queue") != 0)
            continue;
        if ((q = hash_find(hash_table, name)) == NULL) {
            q = malloc(sizeof(ExtentsQueue));
            q->count = 0;
            LIST_INIT(&q->head);
            strncpy(q->name, name, sizeof(q->name));
            hash_add(hash_table, q);
        }
        ExtentsEntry *e = malloc(sizeof(ExtentsEntry));
        e->num = strtoull(num, NULL, 10);
        LIST_INSERT_HEAD(&q->head, e, link);
        ++q->count;
    }
    closedir(dir);
    hash_for(hash_table, purge_extents, NULL);
    hash_free(hash_table);
}

static int llmeta_set_qdb(const char *file)
{
    // lazy - ok to leak on error
    // db is in create mode and will exit anyway
    FILE *f = fopen(file, "r");
    if (f == NULL) {
        logmsg(LOGMSG_ERROR, "%s:fopen(\"%s\"):%s\n", __func__, file,
                strerror(errno));
        return -1;
    }
    size_t n;
    ssize_t s;
    // Name of queue
    char *name = NULL;
    if ((s = getline(&name, &n, f)) == -1) {
        fclose(f);
        return -1;
    }
    name[s - 1] = 0;
    // Num of dests
    char *str_ndest = NULL;
    if ((s = getline(&str_ndest, &n, f)) == -1) {
        fclose(f);
        return -1;
    }
    int ndests = atoi(str_ndest);
    // Dests
    char *dests[ndests];
    for (int i = 0; i < ndests; ++i) {
        dests[i] = NULL;
        if ((s = getline(&dests[i], &n, f)) == -1) {
            fclose(f);
            return -1;
        }
        dests[i][s - 1] = 0;
    }
    // Config - Read till EOF
    long here = ftell(f);
    fseek(f, 0, SEEK_END);
    long end = ftell(f);
    fseek(f, here, SEEK_SET);
    n = end - here;
    char config[n];
    if (fread(config, n, 1, f) == 0) {
        fclose(f);
        return -1;
    }
    config[n - 1] = 0;
    // Save to LLMETA
    int rc, bdberr;
    if ((rc = bdb_llmeta_add_queue(thedb->bdb_env, NULL, name, config, ndests,
                                   dests, &bdberr)) == 0) {
        logmsg(LOGMSG_INFO, "Added queuedb: %s\n", name);
    }
    fclose(f);
    free(name);
    free(str_ndest);
    for (int i = 0; i < ndests; ++i) {
        free(dests[i]);
    }
    if (rc != 0) {
        return -1;
    }
    return 0;
}

static char *create_default_lrl_file(char *dbname, char *dir) {
    char *lrlfile_name;
    FILE *lrlfile;

    lrlfile_name = malloc(strlen(dir) + strlen(dbname) + 4 /*.lrl*/ +
                          1 /*slash*/ + 1 /*nul*/);
    sprintf(lrlfile_name, "%s/%s.lrl", dir, dbname);
    lrlfile = fopen(lrlfile_name, "w");
    if (lrlfile == NULL) {
        logmsg(LOGMSG_ERROR, "fopen(\"%s\") rc %d %s.\n", lrlfile_name, 
                errno, strerror(errno));
        free(lrlfile_name);
        return NULL;
    }

    fprintf(lrlfile, "name    %s\n", dbname);
    fprintf(lrlfile, "dir     %s\n", dir);
    add_cmd_line_tunables_to_file(lrlfile);
    fclose(lrlfile);

    return lrlfile_name;
}

static int init_db_dir(char *dbname, char *dir)
{
    struct stat st;
    int rc;

    rc = stat(dir, &st);
    if (rc) {
        if (errno == ENOENT) {
            rc = mkdir(dir, 0770);
            if (rc) {
                logmsg(LOGMSG_ERROR, "\"%s\" doesn't exist and can't create it.\n", dir);
                return -1;
            }
            rc = stat(dir, &st);
            if (rc) {
                logmsg(LOGMSG_ERROR, "stat(\"%s\") rc %d %s.\n", dir, errno,
                        strerror(errno));
                return -1;
            }
        } else {
            logmsg(LOGMSG_ERROR, "stat(\"%s\") rc %d %s.\n", dir, errno, strerror(errno));
            return -1;
        }
    }
    if (!S_ISDIR(st.st_mode)) {
        logmsg(LOGMSG_ERROR, "\"%s\" is not a directory.\n", dir);
        return -1;
    }
    return 0;
}

static int llmeta_set_qdbs(void)
{
    if (qdbs == NULL)
        return 0;
    int rc = 0;
    while (*qdbs && (rc = llmeta_set_qdb(*qdbs++)) == 0)
        ;
    return rc;
}

static int init_sqlite_table(struct dbenv *dbenv, char *table)
{
    int rc;
    struct dbtable *tbl;

    dbenv->dbs =
        realloc(dbenv->dbs, (dbenv->num_dbs + 1) * sizeof(struct dbtable *));

    /* This used to just pull from installed files.  Let's just do it from memory
       so comdb2 can run standalone with no support files. */
    const char *sqlite_stat1 = 
"tag ondisk { "
"    cstring tbl[64] "
"    cstring idx[64] null=yes "
"    cstring stat[4096] "
"} "
" "
"keys { "
"    \"0\" = tbl + idx "
"} ";

    const char *sqlite_stat4 =
"tag ondisk "
"{ "
"    cstring tbl[64] "
"    cstring idx[64] "
"    int     samplelen "
"    byte    sample[1024] /* entire record in sqlite format */ "
"} "
" "
"keys "
"{ "
"    dup \"0\" = tbl + idx "
"} ";

    const char *schema;

    if (strcmp(table, "sqlite_stat1") == 0) {
       schema = sqlite_stat1;
    }
    else if (strcmp(table, "sqlite_stat4") == 0) {
       schema = sqlite_stat4;
    }
    else {
       logmsg(LOGMSG_ERROR, "unknown sqlite table \"%s\"\n", table);
       return -1;
    }

    rc = dyns_load_schema_string((char*) schema, dbenv->envname, table);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Can't parse schema for %s\n", table);
        return -1;
    }
    tbl = newdb_from_schema(dbenv, table, NULL, 0, dbenv->num_dbs, 0);
    if (tbl == NULL) {
        logmsg(LOGMSG_ERROR, "Can't init table %s from schema\n", table);
        return -1;
    }
    tbl->dbs_idx = dbenv->num_dbs;
    tbl->csc2_schema = strdup(schema);
    dbenv->dbs[dbenv->num_dbs++] = tbl;

    /* Add table to the hash. */
    hash_add(dbenv->db_hash, tbl);

    if (add_cmacc_stmt(tbl, 0)) {
        logmsg(LOGMSG_ERROR, "Can't init table structures %s from schema\n", table);
        return -1;
    }
    return 0;
}

static void load_dbstore_tableversion(struct dbenv *dbenv)
{
    int i;
    for (i = 0; i < dbenv->num_dbs; i++) {
        struct dbtable *tbl = dbenv->dbs[i];
        update_dbstore(tbl);

        tbl->tableversion = table_version_select(tbl, NULL);
        if (tbl->tableversion == -1) {
            logmsg(LOGMSG_ERROR, "Failed reading table version\n");
        }
    }
}

int init_sqlite_tables(struct dbenv *dbenv)
{
    int rc;
    rc = init_sqlite_table(dbenv, "sqlite_stat1");
    if (rc)
        return rc;
    /* There's no 2 or 3.  There used to be 2.  There was never 3. */
    rc = init_sqlite_table(dbenv, "sqlite_stat4");
    if (rc)
        return rc;
    return 0;
}

static int create_db(char *dbname, char *dir) {
   int rc;

   char *fulldir;
   fulldir = realpath(dir, NULL);
   if (fulldir == NULL) {
      rc = mkdir(dir, 0755);
      if (rc) {
         logmsg(LOGMSG_FATAL, 
               "%s doesn't exist, and couldn't create: %s\n", dir,
               strerror(errno));
         return -1;
      }
      fulldir = realpath(dir, NULL);
      if (fulldir == NULL) {
         logmsg(LOGMSG_FATAL, "Can't figure out full path for %s\n", dir);
         return -1;
      }
   }
   dir = fulldir;
   logmsg(LOGMSG_INFO, "Creating db in %s\n", dir);
   setenv("COMDB2_DB_DIR", fulldir, 1);

   if (init_db_dir(dbname, dir)) return -1;

   /* set up as a create run */
   gbl_local_mode = 1;
   /* delay 'gbl_create_mode' so we can use for --create */
   gbl_exit = 1;

   return 0;
}

static int init(int argc, char **argv)
{
    char *dbname, *lrlname = NULL, ctmp[64];
    static int noabort = 0;
    int cacheszkb = 0, ii;
    int rc;
    int bdberr;
    int cacheszkb_suggestion = 0;

    if (argc < 2) {
        print_usage_and_exit();
    }

    dyns_allow_bools();

    rc = bdb_osql_log_repo_init(&bdberr);
    if (rc) {
        logmsg(LOGMSG_FATAL, "bdb_osql_log_repo_init failed to init log repository "
                        "rc %d bdberr %d\n",
                rc, bdberr);
        return -1;
    }

    if (thd_init()) {
        logmsg(LOGMSG_FATAL, "failed initialize thread module\n");
        return -1;
    }
    /* This also initializes the appsock handler hash. */
    if (appsock_init()) {
        logmsg(LOGMSG_FATAL, "failed initialize appsock module\n");
        return -1;
    }
    if (sqlpool_init()) {
        logmsg(LOGMSG_FATAL, "failed to initialise sql module\n");
        return -1;
    }
    if (udppfault_thdpool_init()) {
        logmsg(LOGMSG_FATAL, "failed to initialise udp prefault module\n");
        return -1;
    }
    if (pgcompact_thdpool_init()) {
        logmsg(LOGMSG_FATAL, "failed to initialise page compact module\n");
        return -1;
    }

    /* Initialize the opcode handler hash */
    if (init_opcode_handlers()) {
        logmsg(LOGMSG_FATAL, "failed to initialise opcode handler hash\n");
        return -1;
    }

    /* Install all static plugins. */
    if ((install_static_plugins())) {
        logmsg(LOGMSG_FATAL, "Failed to install static plugins\n");
        exit(1);
    }

    toblock_init();

    handle_cmdline_options(argc, argv, &lrlname);

    if (gbl_create_mode) {        /*  10  */
        logmsg(LOGMSG_INFO, "create mode.\n");
        gbl_exit = 1;
    }
    if (gbl_fullrecovery) {       /*  11  */
        logmsg(LOGMSG_FATAL, "force full recovery.\n");
        gbl_exit = 1;
    }

    /* every option that sets exit implies local mode */
    if (gbl_exit) {
        gbl_local_mode = 1; /*local mode, so no connect to network*/
    }
    
    if (optind >= argc) {
        fprintf(stderr, "Must provide DBNAME as first argument\n");
        exit(1);
    }
    dbname = argv[optind++];
    int namelen = strlen(dbname);
    if (namelen == 0 || namelen >= MAX_DBNAME_LENGTH) {
        logmsg(LOGMSG_FATAL, "Invalid dbname, must be < %d characters\n",
               MAX_DBNAME_LENGTH);
        return -1;
    }
    strcpy(gbl_dbname, dbname);

    if (optind < argc && isdigit((int)argv[optind][0])) {
        cacheszkb = atoi(argv[optind]);
    }

    gbl_mynodeid = machine_num(gbl_mynode);

    pthread_attr_init(&gbl_pthread_attr);
    pthread_attr_setstacksize(&gbl_pthread_attr, DEFAULT_THD_STACKSZ);
    pthread_attr_setdetachstate(&gbl_pthread_attr, PTHREAD_CREATE_DETACHED);

    rc = pthread_key_create(&comdb2_open_key, NULL);
    if (rc) {
        logmsg(LOGMSG_FATAL, "pthread_key_create comdb2_open_key %d\n", rc);
        return -1;
    }

#ifndef BERKDB_46
    rc = pthread_key_create(&DBG_FREE_CURSOR, free);
    if (rc) {
        logmsg(LOGMSG_FATAL, "pthread_key_create DBG_FREE_CURSOR %d\n", rc);
        return -1;
    }
#endif

    if (lrlname == NULL) {
        char *lrlenv = getenv("COMDB2_CONFIG");
        if (lrlenv)
            lrlname = lrlenv;
        else {
            snprintf0(ctmp, sizeof(ctmp), "%s.lrl", dbname);
            if (access(ctmp, F_OK) == 0) lrlname = ctmp;
        }
    }

    /* If user didn't specify where the db lives, try current directory first */
    if (!gbl_create_mode && lrlname == NULL && gbl_dbdir == NULL) {
        struct stat st;
        int rc;

        if (gbl_nonames) {
            rc = stat("logs", &st);
        } else {
            /* TODO: change when merging long names */
            char logdir[100];
            snprintf(logdir, sizeof(logdir), "%s.txn", gbl_dbname);
            rc = stat(logdir, &st);
        }

        if (rc == 0 && (st.st_mode & S_IFDIR)) {
            gbl_dbdir = realpath(".", NULL);
        } else {
            /* can't access or can't find logs in current directory, assume db
             * isn't here */
        }
    }

    init_file_locations(lrlname);

    if (gbl_create_mode && lrlname == NULL) {
       if (gbl_dbdir == NULL)
          gbl_dbdir = comdb2_location("database", "%s", dbname);
       rc = create_db(dbname, gbl_dbdir);
       if (rc) {
          logmsg(LOGMSG_FATAL, "Can't init database directory\n");
          return -1;
       }

       lrlname = create_default_lrl_file(dbname, gbl_dbdir);
    }

#if 0
    if (lrlname == NULL) {
       char *l = comdb2_asprintf("%s.lrl", dbname);
       if (access(l, F_OK) == 0)
          lrlname = l;
       free(l);
    }
    if (lrlname == NULL && gbl_dbdir) {
       char *l = comdb2_asprintf("%s/%s.lrl", gbl_dbdir, dbname);
       if (access(l, F_OK) == 0)
          lrlname = l;
       free(l);
    }
    if (lrlname == NULL && gbl_dbdir == NULL) {
       char *base = comdb2_location("database", "%s", dbname);
       char *l = comdb2_asprintf("%s/%s.lrl", base, dbname);
       if (access(l, F_OK) == 0)
          lrlname = l;
       free(l);
    }
#endif

    initresourceman(lrlname);
    rc = schema_init();
    if (rc)
        return -1;

    /* open database environment, and all dbs */
    thedb = newdbenv(dbname, lrlname);
    if (thedb == 0)
        return -1;

#if WITH_SSL
    /* Initialize SSL backend before creating any net.
       If we're in creat mode, don't bother. */
    if (!gbl_create_mode && ssl_bend_init(thedb->basedir) != 0) {
        logmsg(LOGMSG_FATAL, "Failed to initialize SSL backend.\n");
        return -1;
    }
    logmsg(LOGMSG_INFO, "SSL backend initialized.\n");
#endif

    if (init_blob_cache() != 0) return -1;

    if (osqlpfthdpool_init()) {
        logmsg(LOGMSG_FATAL, "failed to initialise sql module\n");
        return -1;
    }

    if (gbl_ctrace_dbdir)
        ctrace_openlog_taskname(thedb->basedir, dbname);
    else {
        char *dir;
        dir = comdb2_location("logs", NULL);
        ctrace_openlog_taskname(dir, dbname);
        free(dir);
    }

    /* Don't startup if there exists a copylock file in the data directory.
     * This would indicate that a copycomdb2 was done but never completed.
     * Exceptions:
     *  - create mode - since we will blat everything
     *  - recovery mode - since we won't network, may as well let this through
     * Also refuse to start up if we have no log files.
     * Why do we do this?  because we discovered that a half
     * copied database can get the master into some whacked up state in which
     * it decides it needs to fixcomdb2 itself.  From itself.
     */

    if (!gbl_exit) {
        char copylockfile[256], txndir[256];
        struct stat st;
        DIR *dh;
        struct dirent *dp;
        int nlogfiles;
        snprintf(copylockfile, sizeof(copylockfile), "%s/%s.copylock",
                 thedb->basedir, dbname);
        if (stat(copylockfile, &st) == 0) {
            logmsg(LOGMSG_FATAL, "%s exists:\n", copylockfile);
            logmsg(LOGMSG_FATAL, "This database is the result of an incomplete copy!\n");
            logmsg(LOGMSG_FATAL, "Probably a copycomdb2 was interrupted before it\n");
            logmsg(LOGMSG_FATAL, "was complete.  The files for this database are\n");
            logmsg(LOGMSG_FATAL, "probably inconsistent.  If this is a clustered\n");
            logmsg(LOGMSG_FATAL, "database, you should copy from another node .\n");
            if (!noabort)
                exit(1);
        }
        get_txndir(txndir, sizeof(txndir));
        dh = opendir(txndir);
        if (!dh) {
            logmsg(LOGMSG_FATAL, "Cannot open directory %s: %d %s\n", txndir, errno,
                    strerror(errno));
            if (!noabort)
                exit(1);
        }
        nlogfiles = 0;
        errno = 0;
        while ((dp = readdir(dh))) {
            if (strncmp(dp->d_name, "log.", 4) == 0) {
                int ii;
                for (ii = 4; ii < 14; ii++)
                    if (!isdigit(dp->d_name[ii]))
                        break;
                if (ii == 14 && dp->d_name[ii] == 0)
                    nlogfiles++;
            }
            errno = 0;
        }
        if (errno != 0) {
            logmsg(LOGMSG_FATAL, "Cannot scan directory %s: %d %s\n", txndir, errno,
                    strerror(errno));
            if (!noabort)
                exit(1);
        }
        closedir(dh);
        logmsg(LOGMSG_INFO, "%d log files found in %s\n", nlogfiles, txndir);
        if (nlogfiles == 0) {
            logmsg(LOGMSG_FATAL, "ZERO log files found in %s!\n", txndir);
            logmsg(LOGMSG_FATAL, "Cannot start without logfiles.\n");
            logmsg(LOGMSG_FATAL, "If this is a clustered database then you "
                                 "should copycomdb2 from the master.\n");
            if (!noabort)
                exit(1);
        }
    }

    /* Rules for setting cache size:
     * Set to users's cachekb setting, if any.
     * If that's smaller than 2mb, set to 2mb.
     * If the setting is larger than the max, cap at the max.
     * If there's an override, use that.
     * If size specified on command line, use that.
     * If still not set, use the suggester script (common case, sadly)
     * If the result is smaller than the min, set to the min */

    /* Adjust to minimum ONLY if there's an explicit override */
    if (thedb->cacheszkb && thedb->cacheszkb < 2 * 1048) {
        thedb->cacheszkb = 2 * 1024;
        logmsg(LOGMSG_WARN, "too little cache, adjusted to %d kb\n", thedb->cacheszkb);
    }

    if (thedb->cacheszkbmax && thedb->cacheszkb > thedb->cacheszkbmax) {
        logmsg(LOGMSG_INFO, "adjusting cache to specified max of %d\n",
                thedb->cacheszkbmax);
        thedb->cacheszkb = thedb->cacheszkbmax;
    }

    if (thedb->override_cacheszkb > 0) {
        thedb->cacheszkb = thedb->override_cacheszkb;
        logmsg(LOGMSG_INFO, "Using override cache size of %dkb\n",
               thedb->override_cacheszkb);
    } else {
        if (cacheszkb != 0) /*command line overrides.*/
        {
            logmsg(LOGMSG_INFO, "command line cache size specified %dkb\n", cacheszkb);
            thedb->cacheszkb = cacheszkb;
        } else if (thedb->cacheszkb <= 0) {
            thedb->cacheszkb = 2 * 1048;
            logmsg(LOGMSG_INFO, "no cache size specified, using default value of %dkb\n",
                   thedb->cacheszkb);

            if (thedb->cacheszkb < cacheszkb_suggestion) {
                thedb->cacheszkb = cacheszkb_suggestion;
                logmsg(LOGMSG_INFO, "I've been suggested to use %d kb of cache. Using %d kb "
                       "of cache.\n",
                       cacheszkb_suggestion, thedb->cacheszkb);
            }
        }
    }

    if (thedb->cacheszkbmin > thedb->cacheszkb) {
        logmsg(LOGMSG_INFO, "adjusting cache to specified min of %d\n",
                thedb->cacheszkbmin);
        thedb->cacheszkb = thedb->cacheszkbmin;
    }

    /* 040407dh: crash 32bits or run on 64bits
    else if (thedb->cacheszkb > 1500000)
    {
        thedb->cacheszkb=2000000;
        printf("too much cache, adjusted to %d kb\n",thedb->cacheszkb);
    }
    */

    if (thedb->dbnum == 0) {
        logmsg(LOGMSG_DEBUG, "No db number set (missing/invalid dbnum lrl entry?)\n");
    }

    /* This is to force the trc file to open before we go multithreaded */
    reqlog_init(dbname);

    /* Grab our ports early and try to listen on them.
     * * Networks come up after database recovery, making it very easy to start
     * a database twice, run recovery against a running copy (could be bad),
     * and only then fail. */
    rc = setup_net_listen_all(thedb);
    if (rc)
        return -1;

    gbl_myroom = getroom_callback(NULL, gbl_mynode);

    if (skip_clear_queue_extents) {
        logmsg(LOGMSG_INFO, "skipping clear_queue_extents()\n");
    } else {
        clear_queue_extents();
    }

    rc = clear_temp_tables();
    if (rc)
        logmsg(LOGMSG_INFO, "Cleared temporary tables rc=%d\n", rc);

    gbl_starttime = comdb2_time_epoch();

    /* Get all the LONG PREAD and LONG PWRITE outof act.log; I am truly fed up
     * with the entire company asking me if this is a problem. -- SJ */
    berk_set_long_trace_func(myctrace);

    /* disallow bools on test machines.  Prod will continue
     * to allow them because at least one prod database uses them.
     * Still alow bools for people who want to copy/test prod dbs
     * that use them.  Don't allow new databases to have bools. */
    if ((get_mach_class(gbl_mynode) == CLASS_TEST) && gbl_create_mode) {
        if (dyns_used_bools()) {
            logmsg(LOGMSG_FATAL, "bools in schema.  This is now deprecated.\n");
            logmsg(LOGMSG_FATAL, "Exiting since this is a test machine.\n");
            exit(1);
        }
        dyns_disallow_bools();
    }

    /* Now process all the directives we saved up from the lrl file. */
    for (ii = 0; ii < thedb->num_allow_lines; ii++) {
        char *line = thedb->allow_lines[ii];
        if (process_allow_command(line, strlen(line)) != 0)
            return -1;
    }

    if (thedb->nsiblings == 1) {
        logmsg(LOGMSG_INFO, "Forcing master on single node cluster\n");
        bdb_attr_set(thedb->bdb_attr, BDB_ATTR_I_AM_MASTER, 1);
    }

    if (gbl_updategenids) {
        logmsg(LOGMSG_INFO, "Using update genid scheme.");
        bdb_attr_set(thedb->bdb_attr, BDB_ATTR_UPDATEGENIDS, 1);
    }

    if (gbl_round_robin_stripes) {
        logmsg(LOGMSG_INFO, "Will round-robin between data/blob stripes.\n");
        bdb_attr_set(thedb->bdb_attr, BDB_ATTR_ROUND_ROBIN_STRIPES, 1);
    }

    if (gbl_nonames)
        bdb_attr_set(thedb->bdb_attr, BDB_ATTR_NONAMES, 1);
    else
        bdb_attr_set(thedb->bdb_attr, BDB_ATTR_NONAMES, 0);

    if (gbl_sbuftimeout)
        bdb_attr_set(thedb->bdb_attr, BDB_ATTR_SBUFTIMEOUT, gbl_sbuftimeout);

    /* open up the bdb_env now that we have set all the attributes */
    if (open_bdb_env(thedb)) {
        logmsg(LOGMSG_FATAL, "failed to open bdb_env for %s\n", dbname);
        return -1;
    }

    if (gbl_berkdb_iomap) 
        bdb_berkdb_iomap_set(thedb->bdb_env, 1);

    if (gbl_new_snapisol && gbl_snapisol) {
        bdb_attr_set(thedb->bdb_attr, BDB_ATTR_PAGE_ORDER_TABLESCAN, 0);

        if (bdb_gbl_pglogs_mem_init(thedb->bdb_env) != 0)
            exit(1);

        if (bdb_gbl_pglogs_init(thedb->bdb_env) != 0)
            exit(1);
        logmsg(LOGMSG_INFO, "new snapisol is running\n");
    } else {
        logmsg(LOGMSG_INFO, "new snapisol is not running\n");
        gbl_new_snapisol = 0;
        gbl_new_snapisol_asof = 0;
#ifdef NEWSI_STAT
        bdb_newsi_stat_init();
#endif
    }

    /* We grab alot of genids in the process of opening llmeta */
    if (gbl_init_with_genid48 && gbl_create_mode)
        bdb_genid_set_format(thedb->bdb_env, LLMETA_GENID_48BIT);

    /* open the table */
    if (llmeta_open()) {
        return -1;
    }

    logmsg(LOGMSG_INFO, "Successfully opened low level meta table\n");

    gbl_llmeta_open = 1;

    if (!gbl_create_mode) {
       uint64_t format;
       bdb_get_genid_format(&format, &bdberr);
       bdb_genid_set_format(thedb->bdb_env, format);
    }

    set_datetime_dir();

    /* get/set the table names from llmeta */
    if (gbl_create_mode) {
       if (init_sqlite_tables(thedb))
          return -1;

        /* schemas are stored in the meta table after the backend is fully
         * opened below */
    }
    /* if we are using low level meta table and this isn't the create pass,
     * we shouldn't see any table definitions in the lrl. they should have
     * been removed during initialization */
    else if (thedb->num_dbs != 0) {
        logmsg(LOGMSG_FATAL, "lrl contains table definitions, they should not be "
                        "present after the database has been created\n");
        return -1;
    }
    /* we will load the tables from the llmeta table */
    else {
        int waitfileopen =
            bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DELAY_FILE_OPEN);
        if (waitfileopen) {
            logmsg(LOGMSG_INFO, "Waiting to open file\n");
            poll(NULL, 0, waitfileopen);
            logmsg(LOGMSG_INFO, "Done waiting\n");
        }

        /* we would like to open the files under schema lock, so that
           we don't race with a schema change from master (at this point
           environment is opened, but files are not !*/
        pthread_rwlock_wrlock(&schema_lk);

        if (llmeta_load_tables(thedb, dbname)) {
            logmsg(LOGMSG_FATAL, "could not load tables from the low level meta "
                            "table\n");
            pthread_rwlock_unlock(&schema_lk);
            return -1;
        }

        if (llmeta_load_timepart(thedb)) {
            logmsg(LOGMSG_ERROR, "could not load time partitions\n");
            pthread_rwlock_unlock(&schema_lk);
            return -1;
        }
        pthread_rwlock_unlock(&schema_lk);

        if (llmeta_load_queues(thedb)) {
            logmsg(LOGMSG_FATAL, "could not load queues from the low level meta "
                            "table\n");
            return -1;
        }

        if (llmeta_load_lua_sfuncs()) {
            logmsg(LOGMSG_FATAL, "could not load lua funcs from llmeta\n");
            return -1;
        }

        if (llmeta_load_lua_afuncs()) {
            logmsg(LOGMSG_FATAL, "could not load lua aggs from llmeta\n");
            return -1;
        }

        /* if we are repopulating the .lrl with the table definitions */
        if (gbl_repoplrl_fname) {
            /* print all the schemas to disk ie /data/dir/tablename.csc2 */
            if (dump_all_csc2_to_disk())
                logmsg(LOGMSG_FATAL, "error printing tables, continuing anyway\n");

            if (repopulate_lrl(gbl_repoplrl_fname)) {
                logmsg(LOGMSG_FATAL, "repopulate_lrl failed\n");
                return -1;
            }

            /* quit successfully */
            logmsg(LOGMSG_INFO, "-exiting.\n");
            clean_exit();
        }
    }

    /* if we're in repopulate .lrl mode we should have already exited */
    if (gbl_repoplrl_fname) {
        logmsg(LOGMSG_FATAL, "Repopulate .lrl mode failed. Possible causes: db not "
                        "using llmeta or .lrl file already had table defs\n");
        return -1;
    }

    /* do sanity checks and populate a few per db values */
    if (db_finalize_and_sanity_checks(thedb))
        return -1;

    if (!gbl_exit) {
        int i;

        check_access_controls(thedb); /* Check authentication settings */

        if (!have_all_schemas()) {
            logmsg(LOGMSG_ERROR,
                  "Server-side keyforming not supported - missing schemas\n");
            return -1;
        }

        fix_lrl_ixlen(); /* set lrl, ix lengths: ignore lrl file, use info from
                            schema */

        rc = pthread_key_create(&query_info_key, NULL);
        if (rc) {
            logmsg(LOGMSG_FATAL, "pthread_key_create query_info_key rc %d\n", rc);
            return -1;
        }
    }

    /* historical requests */
    if (gbl_loghist) {
        reqhist = malloc(sizeof(history));
        rc = init_history(reqhist, gbl_loghist);
        if (gbl_loghist_verbose)
            reqhist->wholereq = 1;
        if (rc) {
            logmsg(LOGMSG_FATAL, "History init failed\n");
            return -1;
        }
    }

    if (gbl_create_mode) {
        create_service_file(lrlname);
    }

    /* open db engine */
    logmsg(LOGMSG_INFO, "starting backend db engine\n");

    pthread_rwlock_wrlock(&schema_lk);

    if (backend_open(thedb) != 0) {
        logmsg(LOGMSG_FATAL, "failed to open '%s'\n", dbname);
        return -1;
    }

    if (llmeta_load_tables_older_versions(thedb)) {
        logmsg(LOGMSG_FATAL, "llmeta_load_tables_older_versions failed\n");
        return -1;
    }

    load_dbstore_tableversion(thedb);

    gbl_backend_opened = 1;
    pthread_rwlock_unlock(&schema_lk);

    sqlinit();
    rc = create_sqlmaster_records(NULL);
    if (rc) {
        logmsg(LOGMSG_FATAL, "create_sqlmaster_records rc %d\n", rc);
        return -1;
    }
    create_sqlite_master(); /* create sql statements */

    load_auto_analyze_counters(); /* on starting, need to load counters */

    /* There could have been an in-process schema change.  Add those tables now
     * before logical recovery */
    /* xxx this is a temporary workaround.  revist & fix for rowlocks. */

    /* Fabio: I removed this after askign mark, now, merging, I saw it back here
      again.
      I am leaving it in here now, I will ask mark */

    if (gbl_rowlocks) {
        add_schema_change_tables();

        bdb_attr_set(thedb->bdb_attr, BDB_ATTR_PAGE_ORDER_TABLESCAN, 0);
        bdb_attr_set(thedb->bdb_attr, BDB_ATTR_SNAPISOL, 1);
        gbl_snapisol = 1;
    }

    /* This runs logical recovery.  */
    rc = bdb_env_init_after_llmeta(thedb->bdb_env);
    if (rc) {
        logmsg(LOGMSG_FATAL, "Post-llmeta db init failed, rc %d\n", rc);
        return -1;
    }

    if (gbl_create_mode) {
        if (llmeta_set_tables(NULL /*tran*/, thedb)) /* add tables to meta */
        {
            logmsg(LOGMSG_FATAL, "could not add tables to the low level meta "
                            "table\n");
            return -1;
        }

        /* store our schemas in meta */
        if (put_all_csc2()) {
            logmsg(LOGMSG_FATAL, "error storing schemas in meta table\n");
            return -1;
        }

        if (gbl_spfile_name) {
            read_spfile(gbl_spfile_name);
        }

        if (llmeta_set_qdbs() != 0) {
            logmsg(LOGMSG_FATAL, "failed to add queuedbs to llmeta\n");
            return -1;
        }

        llmeta_set_lua_funcs(s);
        llmeta_set_lua_funcs(a);

        /* remove table defs from and add use_llmeta to the lrl file */
        if (rewrite_lrl_remove_tables(getresourcepath("lrl"))) {
            logmsg(LOGMSG_FATAL, "Failed to remove table definitions\n");
            return -1;
        }

        /* dump a mapping of files to their version numbers, the db never uses
         * this file it is only to make it easier for people to tell what files
         * belong to what parts of a table, etc */
        if (llmeta_dump_mapping(thedb))
            logmsg(LOGMSG_WARN, "Failed to dump a mapping of files to their "
                    "versions, the file is helpful for debugging problems but "
                    "not essential, continuing anyway\n");
    }

    if (!gbl_exit && !gbl_create_mode &&
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DURABLE_LSNS) &&
        thedb->nsiblings == 1) {
        extern void bdb_durable_lsn_for_single_node(void *in_bdb_state);
        bdb_durable_lsn_for_single_node(thedb->bdb_env);
    }

    logmsg(LOGMSG_INFO, "backend db engine started.  master is %s\n", thedb->master);
    if (gbl_repdebug == 0) /* turn off explicitly */
        bdb_process_user_command(thedb->bdb_env, "repdbgn", 7, 0);
    else if (gbl_repdebug == 1) /* turn on explicitly */
        bdb_process_user_command(thedb->bdb_env, "repdbgy", 7, 0);

    clear_csc2_files();

    if (gbl_exit) {
        logmsg(LOGMSG_INFO, "-exiting.\n");
        clean_exit();
    }

#if 0
    /* We can't do this anymore - recovery may still be holding transactions
       open waiting for the master to write an abort record. */
    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDONLY);
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);
    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDONLY);
#endif

    /* some dbs have lots of tables and spew on startup.  this just wastes
     * peoples time shunting spew */
    if (getenv("CDB2_SHOW_DBENV")) {
        showdbenv(thedb);
    }

    if (gbl_net_max_queue) {
        net_set_max_queue(thedb->handle_sibling, gbl_net_max_queue);
    }

    if (gbl_net_max_mem) {
        uint64_t bytes;
        bytes = 1024 * 1024 * gbl_net_max_mem;
        net_set_max_bytes(thedb->handle_sibling, bytes);
    }

    if (gbl_net_throttle_percent) {
        net_set_throttle_percent(thedb->handle_sibling,
                                 gbl_net_throttle_percent);
    }

    if (gbl_net_portmux_register_interval) {
        net_set_portmux_register_interval(thedb->handle_sibling,
                                          gbl_net_portmux_register_interval);
    }

    if (gbl_enque_flush_interval) {
        net_set_enque_flush_interval(thedb->handle_sibling,
                                     gbl_enque_flush_interval);
    }

    if (gbl_enque_reorder_lookahead) {
        net_set_enque_reorder_lookahead(thedb->handle_sibling,
                                        gbl_enque_reorder_lookahead);
    }

    if (gbl_net_poll) {
        net_set_poll(thedb->handle_sibling, gbl_net_poll);
    }

    if (gbl_heartbeat_send) {
        net_set_heartbeat_send_time(thedb->handle_sibling, gbl_heartbeat_send);
    }
    if (gbl_heartbeat_check) {
        net_set_heartbeat_check_time(thedb->handle_sibling,
                                     gbl_heartbeat_check);
    }

    net_setbufsz(thedb->handle_sibling, gbl_netbufsz);

    if (javasp_init_procedures() != 0) {
        logmsg(LOGMSG_FATAL, "*ERROR* cannot initialise Java stored procedures\n");
        return -1;
    }

    csc2_free_all();

    return 0;
}

char *getorigin(struct ireq *iq)
{
    if (iq->is_fake || iq->corigin[0] == 0)
        return "INTERNAL";

    /* is_fromsocket case in init_ireq should set corigin, and
     * we no longer have any other kind. */

    return iq->corigin;
}

#define TOUPPER(x) (((x >= 'a') && (x <= 'z')) ? x - 32 : x)

static char *strtoupper(char instr[])
{
    int instrlen;

    instrlen = strlen(instr);
    while ((--instrlen >= 0) && (instr[instrlen] = TOUPPER(instr[instrlen])))
        ;
    return instr;
} /* strtoupper() */

static void ttrap(struct timer_parm *parm)
{
    char *msg;
    switch (parm->parm) {
    case TMEV_ENABLE_LOG_DELETE:
        msg = "sync log-delete on";
        process_command(thedb, msg, strlen(msg), 0);
        break;
    case TMEV_PURGE_OLD_LONGTRN:
        (void)purge_expired_long_transactions(thedb);
        break;
    case TMEV_EXIT:
        pthread_exit(NULL);
        break;
    default:
        comdb2_cantim(parm->parm);
        break;
    }
}

void create_old_blkseq_thread(struct dbenv *dbenv)
{
    int rc;

    if (!dbenv->purge_old_blkseq_is_running) {
        rc = pthread_create(&dbenv->purge_old_blkseq_tid, &gbl_pthread_attr,
                            purge_old_blkseq_thread, thedb);
        if (rc)
            logmsg(LOGMSG_WARN, 
                "Warning: can't start purge_old_blkseq thread: rc %d err %s\n",
                rc, strerror(rc));
    }

    if (!dbenv->purge_old_files_is_running) {
        rc = pthread_create(&dbenv->purge_old_files_tid, &gbl_pthread_attr,
                            purge_old_files_thread, thedb);
        if (rc)
            logmsg(LOGMSG_WARN, "Warning: can't start purge_oldfiles thread: rc %d err %s\n",
                   rc, strerror(rc));
    }
}

/* bump up ulimit for no. fds up to hard limit */
static void adjust_ulimits(void)
{
#   if !defined(__hpux) && !defined(__APPLE__)
    struct rlimit64 rlim;

    if (-1 == getrlimit64(RLIMIT_DATA, &rlim)) {
        logmsg(LOGMSG_ERROR, "%s:getrlimit64: %d %s\n", __func__, errno,
                strerror(errno));
    } else if (rlim.rlim_cur != RLIM64_INFINITY &&
               (rlim.rlim_max == RLIM64_INFINITY ||
                rlim.rlim_cur < rlim.rlim_max)) {
        rlim.rlim_cur = rlim.rlim_max;
        if (-1 == setrlimit64(RLIMIT_DATA, &rlim)) {
            logmsg(LOGMSG_ERROR, "%s:setrlimit64: %d %s\n", __func__, errno,
                    strerror(errno));
        } else if (rlim.rlim_cur == RLIM64_INFINITY) {
            logmsg(LOGMSG_INFO, "%s: set ulimit for data to unlimited\n", __func__);
        } else {
            logmsg(LOGMSG_INFO, "%s: set ulimit for data to %d\n", __func__,
                    (int)rlim.rlim_cur);
        }
    } else {
        logmsg(LOGMSG_INFO, "ulimit for data already set\n");
    }

    if (-1 == getrlimit64(RLIMIT_NOFILE, &rlim)) {
        logmsg(LOGMSG_ERROR, "%s:getrlimit64: %d %s\n", __func__, errno,
                strerror(errno));
    } else if (rlim.rlim_cur != RLIM64_INFINITY &&
               (rlim.rlim_max == RLIM64_INFINITY ||
                rlim.rlim_cur < rlim.rlim_max)) {
        rlim.rlim_cur = rlim.rlim_max;
        if (-1 == setrlimit64(RLIMIT_NOFILE, &rlim)) {
            logmsg(LOGMSG_ERROR, "%s:setrlimit64: %d %s\n", __func__, errno,
                    strerror(errno));
        } else if (rlim.rlim_cur == RLIM64_INFINITY) {
            logmsg(LOGMSG_INFO, "%s: set ulimit for no. fds to unlimited\n",
                    __func__);
        } else {
            logmsg(LOGMSG_INFO, "%s: set ulimit for no. fds to %d\n", __func__,
                    (int)rlim.rlim_cur);
        }

    } else {
        logmsg(LOGMSG_INFO, "ulimit for no. fds already set\n");
    }
#   endif //__hpux
}

extern void set_throttle(int);
extern int get_throttle(void);
extern int get_calls_per_sec(void);
void reset_calls_per_sec(void);
int throttle_lim = 10000;
int cpu_throttle_threshold = 100000;

#if 0
void *pq_thread(void *);
#endif

void *statthd(void *p)
{
    struct dbenv *dbenv;
    int nqtrap;
    int nfstrap;
    int nsql;
    long long nsql_steps;
    int ncommits;
    double ncommit_time;
    int newsql;
    long long newsql_steps;
    int nretries;
    int64_t ndeadlocks = 0, nlockwaits = 0;
    int64_t vreplays;

    int diff_qtrap;
    int diff_fstrap;
    int diff_nsql;
    int diff_nsql_steps;
    int diff_ncommits;
    long long diff_ncommit_time;
    int diff_newsql;
    int diff_nretries;
    int diff_deadlocks;
    int diff_lockwaits;
    int diff_vreplays;

    int last_qtrap = 0;
    int last_fstrap = 0;
    int last_nsql = 0;
    long long last_nsql_steps = 0;
    int last_ncommits = 0;
    long long last_ncommit_time = 0;
    int last_newsql = 0;
    int last_nretries = 0;
    int64_t last_ndeadlocks = 0, last_nlockwaits = 0;
    int64_t last_vreplays = 0;

    int count = 0;
    int last_report_nqtrap = n_qtrap;
    int last_report_nfstrap = n_fstrap;
    int last_report_nsql = gbl_nsql;
    long long last_report_nsql_steps = gbl_nsql_steps;
    int last_report_ncommits = n_commits;
    long long last_report_ncommit_time = n_commit_time;
    int last_report_newsql = gbl_nnewsql;
    long long last_report_newsql_steps = gbl_nnewsql_steps;
    int last_report_nretries = n_retries;
    int64_t last_report_deadlocks = 0;
    int64_t last_report_lockwaits = 0;
    int64_t last_report_vreplays = 0;
    uint64_t bpool_hits = 0;
    uint64_t bpool_misses = 0;
    uint64_t diff_bpool_hits;
    uint64_t diff_bpool_misses;
    uint64_t last_bpool_hits = 0;
    uint64_t last_bpool_misses = 0;
    uint64_t last_report_bpool_hits = 0;
    uint64_t last_report_bpool_misses = 0;

    struct reqlogger *statlogger = NULL;
    struct bdb_thread_stats last_bdb_stats = {0};
    struct bdb_thread_stats cur_bdb_stats;
    const struct bdb_thread_stats *pstats;
    char lastlsn[63] = "", curlsn[64];
    uint64_t lastlsnbytes = 0, curlsnbytes;
    int ii;
    int jj;
    int hdr;
    int diff;
    int thresh;
    struct dbtable *tbl;
    char hdr_fmt[] = "DIFF REQUEST STATS FOR DB %d '%s'\n";
    int have_scon_header = 0;
    int have_scon_stats = 0;

    dbenv = p;

    /* initialize */
    statlogger = reqlog_alloc();
    reqlog_diffstat_init(statlogger);

    while (!db_is_stopped()) {
        nqtrap = n_qtrap;
        nfstrap = n_fstrap;
        ncommits = n_commits;
        ncommit_time = n_commit_time;
        nsql = gbl_nsql;
        nsql_steps = gbl_nsql_steps;
        newsql = gbl_nnewsql;
        newsql_steps = gbl_nnewsql_steps;
        nretries = n_retries;
        vreplays = gbl_verify_tran_replays;

        bdb_get_bpool_counters(thedb->bdb_env, (int64_t *)&bpool_hits,
                               (int64_t *)&bpool_misses);

        bdb_get_lock_counters(thedb->bdb_env, &ndeadlocks, &nlockwaits);
        diff_deadlocks = ndeadlocks - last_ndeadlocks;
        diff_lockwaits = nlockwaits - last_nlockwaits;

        diff_qtrap = nqtrap - last_qtrap;
        diff_fstrap = nfstrap - last_fstrap;
        diff_nsql = nsql - last_nsql;
        diff_nsql_steps = nsql_steps - last_nsql_steps;
        diff_newsql = newsql - last_newsql;
        diff_nretries = nretries - last_nretries;
        diff_vreplays = vreplays - last_vreplays;
        diff_ncommits = ncommits - last_ncommits;
        diff_ncommit_time = ncommit_time - last_ncommit_time;
        diff_bpool_hits = bpool_hits - last_bpool_hits;
        diff_bpool_misses = bpool_misses - last_bpool_misses;

        last_qtrap = nqtrap;
        last_fstrap = nfstrap;
        last_nsql = nsql;
        last_nsql_steps = nsql_steps;
        last_newsql = newsql;
        last_nretries = nretries;
        last_ndeadlocks = ndeadlocks;
        last_nlockwaits = nlockwaits;
        last_vreplays = vreplays;
        last_ncommits = ncommits;
        last_ncommit_time = ncommit_time;
        last_bpool_hits = bpool_hits;
        last_bpool_misses = bpool_misses;

        have_scon_header = 0;
        have_scon_stats = 0;

        if (diff_qtrap || diff_nsql || diff_newsql || diff_nsql_steps ||
            diff_fstrap || diff_vreplays || diff_bpool_hits ||
            diff_bpool_misses || diff_ncommit_time) {
            if (gbl_report) {
                logmsg(LOGMSG_USER, "diff");
                have_scon_header = 1;
                if (diff_qtrap)
                    logmsg(LOGMSG_USER, " n_reqs %d", diff_qtrap);
                if (diff_fstrap)
                    logmsg(LOGMSG_USER, " n_fsreqs %d", diff_fstrap);
                if (diff_nsql)
                    logmsg(LOGMSG_USER, " nsql %d", diff_nsql);
                if (diff_nsql_steps)
                    logmsg(LOGMSG_USER, " nsqlsteps %d", diff_nsql_steps);
                if (diff_deadlocks)
                    logmsg(LOGMSG_USER, " ndeadlocks %d", diff_deadlocks);
                if (diff_lockwaits)
                    logmsg(LOGMSG_USER, " nlockwaits %d", diff_lockwaits);
                if (diff_nretries)
                    logmsg(LOGMSG_USER, " n_retries %d", diff_nretries);
                if (diff_vreplays)
                    logmsg(LOGMSG_USER, " vreplays %d", diff_vreplays);
                if (diff_newsql)
                    logmsg(LOGMSG_USER, " nnewsql %d", diff_newsql);
                if (diff_ncommit_time)
                    logmsg(LOGMSG_USER, " n_commit_time %f ms",
                           (double)diff_ncommit_time / (1000 * diff_ncommits));
                if (diff_bpool_hits)
                    logmsg(LOGMSG_USER, " cache_hits %lu", diff_bpool_hits);
                if (diff_bpool_misses)
                    logmsg(LOGMSG_USER, " cache_misses %lu", diff_bpool_misses);
                have_scon_stats = 1;
            }
        }
        if (gbl_report)
            have_scon_stats |= osql_comm_diffstat(NULL, &have_scon_header);

        if (have_scon_stats)
            logmsg(LOGMSG_USER, "\n");

        if (!gbl_schema_change_in_progress) {
            thresh = reqlog_diffstat_thresh();
            if ((thresh > 0) && (count == thresh)) {
                strbuf *logstr = strbuf_new();
                diff_qtrap = nqtrap - last_report_nqtrap;
                diff_fstrap = nfstrap - last_report_nfstrap;
                diff_nsql = nsql - last_report_nsql;
                diff_nsql_steps = nsql_steps - last_report_nsql_steps;
                diff_newsql = newsql - last_report_newsql;
                int diff_newsql_steps = newsql_steps - last_report_newsql_steps;
                diff_nretries = nretries - last_report_nretries;
                diff_ncommits = ncommits - last_report_ncommits;
                diff_ncommit_time = ncommit_time - last_report_ncommit_time;

                diff_bpool_hits = bpool_hits - last_report_bpool_hits;
                diff_bpool_misses = bpool_misses - last_report_bpool_misses;

                if (diff_qtrap || diff_newsql || diff_nsql || diff_nsql_steps ||
                    diff_fstrap || diff_bpool_hits || diff_bpool_misses ||
                    diff_ncommit_time) {

                    strbuf_appendf(logstr, "diff");
                    if (diff_qtrap || diff_nretries) {
                        strbuf_appendf(logstr, " n_reqs %d n_retries %d",
                                       diff_qtrap, diff_nretries);
                    }
                    if (diff_nsql) {
                        strbuf_appendf(logstr, " nsql %d", diff_nsql);
                    }
                    if (diff_newsql) {
                        strbuf_appendf(logstr, " newsql %d", diff_newsql);
                    }
                    if (diff_nsql_steps) {
                        strbuf_appendf(logstr, " nsqlsteps %d",
                                       diff_nsql_steps);
                    }
                    if (diff_newsql_steps) {
                        strbuf_appendf(logstr, " newsqlsteps %d",
                                       diff_newsql_steps);
                    }
                    if (diff_fstrap) {
                        strbuf_appendf(logstr, " n_fsreqs %d", diff_fstrap);
                    }
                    if (diff_ncommit_time && diff_ncommits) {
                        strbuf_appendf(logstr, " n_commit_time %lld ms",
                                       diff_ncommit_time /
                                           (1000 * diff_ncommits));
                    }
                    if (diff_bpool_hits) {
                        strbuf_appendf(logstr, " n_cache_hits %llu",
                                       (long long unsigned int)diff_bpool_hits);
                    }
                    if (diff_bpool_misses) {
                        strbuf_appendf(
                            logstr, " n_cache_misses %llu",
                            (long long unsigned int)diff_bpool_misses);
                    }
                    strbuf_appendf(logstr, "\n");
                    reqlog_logl(statlogger, REQL_INFO, strbuf_buf(logstr));
                }

                for (ii = 0; ii < dbenv->num_dbs; ++ii) {
                    tbl = dbenv->dbs[ii];
                    hdr = 0;

                    for (jj = 0; jj <= MAXTYPCNT; jj++) {
                        diff = tbl->typcnt[jj] - tbl->prev_typcnt[jj];
                        if (diff > 0) {
                            if (hdr == 0) {
                                reqlog_logf(statlogger, REQL_INFO, hdr_fmt,
                                            tbl->dbnum, tbl->tablename);
                                hdr = 1;
                            }
                            reqlog_logf(statlogger, REQL_INFO, "%-20s %u\n",
                                        req2a(jj), diff);
                        }
                        tbl->prev_typcnt[jj] = tbl->typcnt[jj];
                    }

                    for (jj = 0; jj < BLOCK_MAXOPCODE; jj++) {
                        diff = tbl->blocktypcnt[jj] - tbl->prev_blocktypcnt[jj];
                        if (diff) {
                            if (hdr == 0) {
                                reqlog_logf(statlogger, REQL_INFO, hdr_fmt,
                                            tbl->dbnum, tbl->tablename);
                                hdr = 1;
                            }
                            reqlog_logf(statlogger, REQL_INFO, "    %-20s %u\n",
                                        breq2a(jj), diff);
                        }
                        tbl->prev_blocktypcnt[jj] = tbl->blocktypcnt[jj];
                    }
                    for (jj = 0; jj < MAX_OSQL_TYPES; jj++) {
                        diff = tbl->blockosqltypcnt[jj] -
                               tbl->prev_blockosqltypcnt[jj];
                        if (diff) {
                            if (hdr == 0) {
                                reqlog_logf(statlogger, REQL_INFO, hdr_fmt,
                                            tbl->dbnum, tbl->tablename);
                                hdr = 1;
                            }
                            reqlog_logf(statlogger, REQL_INFO, "    %-20s %u\n",
                                        osql_breq2a(jj), diff);
                        }
                        tbl->prev_blockosqltypcnt[jj] = tbl->blockosqltypcnt[jj];
                    }

                    diff = dbenv->txns_committed - dbenv->prev_txns_committed;
                    if (diff) {
                        if (hdr == 0) {
                            reqlog_logf(statlogger, REQL_INFO, hdr_fmt,
                                        tbl->dbnum, tbl->tablename);
                            hdr = 1;
                        }
                        reqlog_logf(statlogger, REQL_INFO, "    %-20s %u\n",
                                    "txns committed", diff);
                    }
                    dbenv->prev_txns_committed = dbenv->txns_committed;

                    diff = dbenv->txns_aborted - dbenv->prev_txns_aborted;
                    if (diff) {
                        if (hdr == 0) {
                            reqlog_logf(statlogger, REQL_INFO, hdr_fmt,
                                        tbl->dbnum, tbl->tablename);
                            hdr = 1;
                        }
                        reqlog_logf(statlogger, REQL_INFO, "    %-20s %u\n",
                                    "txns aborted", diff);
                    }
                    dbenv->prev_txns_aborted = dbenv->txns_aborted;

                    diff = tbl->nsql - tbl->prev_nsql;
                    if (diff) {
                        if (hdr == 0) {
                            reqlog_logf(statlogger, REQL_INFO, hdr_fmt,
                                        tbl->dbnum, tbl->tablename);
                            hdr = 1;
                        }
                        reqlog_logf(statlogger, REQL_INFO, "    %-20s %u\n",
                                    "nsql", diff);
                    }
                    tbl->prev_nsql = tbl->nsql;
                }

                pstats = bdb_get_process_stats();
                cur_bdb_stats = *pstats;
                if (cur_bdb_stats.n_lock_waits > last_bdb_stats.n_lock_waits) {
                    unsigned nreads = cur_bdb_stats.n_lock_waits -
                                      last_bdb_stats.n_lock_waits;
                    reqlog_logf(statlogger, REQL_INFO,
                                "%u locks, avg time %ums\n", nreads,
                                U2M(cur_bdb_stats.lock_wait_time_us -
                                    last_bdb_stats.lock_wait_time_us) /
                                    nreads);
                }
                if (cur_bdb_stats.n_preads > last_bdb_stats.n_preads) {
                    unsigned npreads =
                        cur_bdb_stats.n_preads - last_bdb_stats.n_preads;
                    reqlog_logf(statlogger, REQL_INFO,
                                "%u preads, %u bytes, avg time %ums\n", npreads,
                                cur_bdb_stats.pread_bytes -
                                    last_bdb_stats.pread_bytes,
                                U2M(cur_bdb_stats.pread_time_us -
                                    last_bdb_stats.pread_time_us) /
                                    npreads);
                }
                if (cur_bdb_stats.n_pwrites > last_bdb_stats.n_pwrites) {
                    unsigned npwrites =
                        cur_bdb_stats.n_pwrites - last_bdb_stats.n_pwrites;
                    reqlog_logf(statlogger, REQL_INFO,
                                "%u pwrites, %u bytes, avg time %ums\n",
                                npwrites, cur_bdb_stats.pwrite_bytes -
                                              last_bdb_stats.pwrite_bytes,
                                U2M(cur_bdb_stats.pwrite_time_us -
                                    last_bdb_stats.pwrite_time_us) /
                                    npwrites);
                }
                last_bdb_stats = cur_bdb_stats;

                diff_deadlocks = ndeadlocks - last_report_deadlocks;
                diff_lockwaits = nlockwaits - last_report_lockwaits;
                diff_vreplays = vreplays - last_report_vreplays;

                if (diff_deadlocks || diff_lockwaits || diff_vreplays)
                    reqlog_logf(statlogger, REQL_INFO,
                                "ndeadlocks %d, nlockwaits %d, vreplays %d\n",
                                diff_deadlocks, diff_lockwaits, diff_vreplays);

                bdb_get_cur_lsn_str(thedb->bdb_env, &curlsnbytes, curlsn,
                                    sizeof(curlsn));
                if (strcmp(curlsn, lastlsn) != 0) {
                    reqlog_logf(statlogger, REQL_INFO, "LSN %s diff %llu\n",
                                curlsn, curlsnbytes - lastlsnbytes);
                    strncpy0(lastlsn, curlsn, sizeof(lastlsn));
                    lastlsnbytes = curlsnbytes;
                }

                reqlog_diffstat_dump(statlogger);

                count = 0;
                last_report_nqtrap = nqtrap;
                last_report_nfstrap = nfstrap;
                last_report_nsql = nsql;
                last_report_nsql_steps = nsql_steps;
                last_report_newsql = newsql;
                last_report_newsql_steps = newsql_steps;
                last_report_nretries = nretries;
                last_report_bpool_hits = bpool_hits;
                last_report_bpool_misses = bpool_misses;

                last_report_deadlocks = ndeadlocks;
                last_report_lockwaits = nlockwaits;
                last_report_vreplays = vreplays;

                last_report_ncommits = ncommits;
                last_report_ncommit_time = ncommit_time;

                osql_comm_diffstat(statlogger, NULL);
                strbuf_free(logstr);
            }

            if (count % 60 == 0) {
                /* dump here */
                quantize_ctrace(q_min, "Tagged requests this minute");
                quantize_clear(q_min);
                quantize_ctrace(q_sql_min, "SQL requests this minute");
                quantize_clear(q_sql_min);
                quantize_ctrace(q_sql_steps_min, "SQL steps this minute");
                quantize_clear(q_sql_steps_min);
            }
            if (count % 3600 == 0) {
                /* dump here */
                quantize_ctrace(q_hour, "Tagged requests this hour");
                quantize_clear(q_hour);
                quantize_ctrace(q_sql_hour, "SQL requests this hour");
                quantize_clear(q_sql_hour);
                quantize_ctrace(q_sql_steps_hour, "SQL steps this hour");
                quantize_clear(q_sql_steps_hour);
            }
        }

        if (gbl_repscore)
            bdb_show_reptimes_compact(thedb->bdb_env);

        ++count;
        sleep(1);
    }

    reqlog_free(statlogger);
    return NULL;
}

void create_stat_thread(struct dbenv *dbenv)
{
    pthread_t stat_tid;
    int rc;

    rc = pthread_create(&stat_tid, &gbl_pthread_attr, statthd, dbenv);
    if (rc) {
        logmsg(LOGMSG_FATAL, "pthread_create statthd rc %d\n", rc);
        abort();
    }
}
/* set datetime global if directory exists */
static void set_datetime_dir(void)
{

    struct stat st;
    char *dir = comdb2_location("tzdata", "zoneinfo");

    /* this is a stupid test to prevent running comdb2 that have no datetime
       support
       files; this only test for directory presence and access to it, nothing
       else
    */
    if (stat(dir, &st)) {
        free(dir);
        logmsg(LOGMSG_FATAL, "This machine has no datetime support file;\n");
        abort();
    }

    set_tzdir(dir);
}

static void iomap_on(void *p)
{
    gbl_berkdb_iomap = 1;
    if (thedb && thedb->bdb_env)
        bdb_berkdb_iomap_set(thedb->bdb_env, 1);
}

static void iomap_off(void *p)
{
    gbl_berkdb_iomap = 0;
    if (thedb && thedb->bdb_env)
        bdb_berkdb_iomap_set(thedb->bdb_env, 0);
}

cron_sched_t *memstat_sched;
static void *memstat_cron_event(void *arg1, void *arg2, void *arg3, void *arg4,
                                struct errstat *err)
{
    int tm;
    void *rc;

    // cron jobs always write to ctrace
    (void)comdb2ma_stats(NULL, 1, 0, COMDB2MA_TOTAL_DESC, COMDB2MA_GRP_NONE, 1);

    if (gbl_memstat_freq > 0) {
        tm = comdb2_time_epoch() + gbl_memstat_freq;
        rc = cron_add_event(memstat_sched, NULL, tm, (FCRON) memstat_cron_event, NULL,
                            NULL, NULL, NULL, err);

        if (rc == NULL)
            logmsg(LOGMSG_ERROR, "Failed to schedule next memstat event. "
                            "rc = %d, errstr = %s\n",
                    err->errval, err->errstr);
    }
    return NULL;
}

static void *memstat_cron_kickoff(void *arg1, void *arg2, void *arg3,
                                  void *arg4, struct errstat *err)
{
    int tm;
    void *rc;

    logmsg(LOGMSG_INFO, "Starting memstat cron job. "
                    "Will print memory usage every %d seconds.\n",
            gbl_memstat_freq);

    tm = comdb2_time_epoch() + gbl_memstat_freq;
    rc = cron_add_event(memstat_sched, NULL, tm, (FCRON) memstat_cron_event, NULL, NULL,
                        NULL, NULL, err);
    if (rc == NULL)
        logmsg(LOGMSG_ERROR, "Failed to schedule next memstat event. "
                        "rc = %d, errstr = %s\n",
                err->errval, err->errstr);

    return NULL;
}

static int comdb2ma_stats_cron(void)
{
    struct errstat xerr = {0};

    if (gbl_memstat_freq > 0) {
        memstat_sched = cron_add_event(
            memstat_sched, memstat_sched == NULL ? "memstat_cron" : NULL,
            INT_MIN, (FCRON) memstat_cron_kickoff, NULL, NULL, NULL, NULL, &xerr);

        if (memstat_sched == NULL)
            logmsg(LOGMSG_ERROR, "Failed to schedule memstat cron job. "
                            "rc = %d, errstr = %s\n",
                    xerr.errval, xerr.errstr);
    }

    return xerr.errval;
}

static void register_all_int_switches()
{
    register_int_switch("bad_lrl_fatal",
                        "Unrecognised lrl options are fatal errors",
                        &gbl_bad_lrl_fatal);
    register_int_switch("t2t", "New tag->tag conversion code", &gbl_use_t2t);
    register_int_switch("fix_cstr", "Fix validation of cstrings",
                        &gbl_fix_validate_cstr);
    register_int_switch("warn_cstr", "Warn on validation of cstrings",
                        &gbl_warn_validate_cstr);
    register_int_switch("scpushlogs", "Push to next log after a schema changes",
                        &gbl_pushlogs_after_sc);
    register_int_switch("pfltverbose", "Verbose errors in prefaulting code",
                        &gbl_prefault_verbose);
    register_int_switch("plannedsc", "Use planned schema change by default",
                        &gbl_default_plannedsc);
    register_int_switch("pflt_toblock_lcl",
                        "Prefault toblock operations locally",
                        &gbl_prefault_toblock_local);
    register_int_switch("pflt_toblock_rep",
                        "Prefault toblock operations on replicants",
                        &gbl_prefault_toblock_bcast);
    register_int_switch("dflt_livesc", "Use live schema change by default",
                        &gbl_default_livesc);
    register_int_switch("dflt_plansc", "Use planned schema change by default",
                        &gbl_default_plannedsc);
    register_int_switch("consumer_rtcpu",
                        "Don't send update broadcasts to rtcpu'd machines",
                        &gbl_consumer_rtcpu_check);
    register_int_switch(
        "node1_rtcpuable",
        "If off then consumer code won't do rtcpu checks on node 1",
        &gbl_node1rtcpuable);
    register_int_switch("sqlite3openserial",
                        "Serialise calls to sqlite3_open to prevent excess CPU",
                        &gbl_serialise_sqlite3_open);
    register_int_switch(
        "thread_stats",
        "Berkeley DB will keep stats on what its threads are doing",
        &gbl_bb_berkdb_enable_thread_stats);
    register_int_switch(
        "lock_timing",
        "Berkeley DB will keep stats on time spent waiting for locks",
        &gbl_bb_berkdb_enable_lock_timing);
    register_int_switch(
        "memp_timing",
        "Berkeley DB will keep stats on time spent in __memp_fget",
        &gbl_bb_berkdb_enable_memp_timing);
    register_int_switch(
        "memp_pg_timing",
        "Berkeley DB will keep stats on time spent in __memp_pg",
        &gbl_bb_berkdb_enable_memp_pg_timing);
    register_int_switch("shalloc_timing", "Berkeley DB will keep stats on time "
                                          "spent in shallocs and shalloc_frees",
                        &gbl_bb_berkdb_enable_shalloc_timing);
    register_int_switch("allow_mismatched_tag_size",
                        "Allow variants in padding in static tag struct sizes",
                        &gbl_allow_mismatched_tag_size);
    register_int_switch("reset_queue_cursor_mode",
                        "Reset queue consumeer read cursor after each consume",
                        &gbl_reset_queue_cursor);
    register_int_switch("key_updates",
                        "Update non-dupe keys instead of delete/add",
                        &gbl_key_updates);
    register_int_switch("emptystrnum", "Empty strings don't convert to numbers",
                        &gbl_empty_strings_dont_convert_to_numbers);
    register_int_switch("schemachange_perms",
                        "Check if schema change allowed from source machines",
                        &gbl_check_schema_change_permissions);
    register_int_switch("verifylsn",
                        "Verify if LSN written before writing page",
                        &gbl_verify_lsn_written);
    register_int_switch("allow_broken_datetimes", "Allow broken datetimes",
                        &gbl_allowbrokendatetime);
    register_int_switch("verify_directio",
                        "Run expensive checks on directio calls",
                        &gbl_verify_direct_io);
    register_int_switch("parallel_sync",
                        "Run checkpoint/memptrickle code with parallel writes",
                        &gbl_parallel_memptrickle);
    register_int_switch("verify_dbreg",
                        "Periodically check if dbreg entries are correct",
                        &gbl_verify_dbreg);
    register_int_switch("verifycheckpoints",
                        "Highly paranoid checkpoint validity checks",
                        &gbl_checkpoint_paranoid_verify);
    register_int_switch(
        "support_datetime_in_triggers",
        "Enable support for datetime/interval types in triggers",
        &gbl_support_datetime_in_triggers);
    register_int_switch("prefix_foreign_keys",
                        "Allow foreign key to be a prefix of your key",
                        &gbl_fk_allow_prefix_keys);
    register_int_switch("superset_foreign_keys",
                        "Allow foreign key to be a superset of your key",
                        &gbl_fk_allow_superset_keys);
    register_int_switch("repverifyrecs",
                        "Verify every berkeley log record received",
                        &gbl_verify_rep_log_records);
    register_int_switch(
        "enable_osql_logging",
        "Log every osql packet received in a special file, per iq\n",
        &gbl_enable_osql_logging);
    register_int_switch("enable_osql_longreq_logging",
                        "Log untruncated osql strings\n",
                        &gbl_enable_osql_longreq_logging);
    register_int_switch(
        "check_sparse_files",
        "When allocating a page, check that we aren't creating a sparse file\n",
        &gbl_check_sparse_files);
    register_int_switch(
        "core_on_sparse_file",
        "Generate a core if we catch berkeley creating a sparse file\n",
        &gbl_core_on_sparse_file);
    register_int_switch(
        "check_sqlite_numeric_types",
        "Report if our numeric conversion disagrees with SQLite's\n",
        &gbl_report_sqlite_numeric_conversion_errors);
    register_int_switch(
        "use_fastseed_for_comdb2_seqno",
        "Use fastseed instead of context for comdb2_seqno unique values\n",
        &gbl_use_fastseed_for_comdb2_seqno);
    register_int_switch(
        "disable_stable_for_ipu",
        "For inplace update tables, disable stable find-next cursors",
        &gbl_disable_stable_for_ipu);
    register_int_switch("debug_mpalloc_size",
                        "Alarm on suspicious allocation requests",
                        &gbl_debug_memp_alloc_size);
    register_int_switch("disable_exit_on_thread_error",
                        "don't exit on thread errors",
                        &gbl_disable_exit_on_thread_error);
    register_int_switch("support_sock_luxref",
                        "support proxy socket request with a set luxref",
                        &gbl_support_sock_luxref);
    register_switch("berkdb_iomap",
                    "enable berkdb writing memptrickle status to a mapped file",
                    iomap_on, iomap_off, int_stat_fn, &gbl_berkdb_iomap);
    register_int_switch("requeue_on_tran_dispatch",
                        "Requeue transactional statement if not enough threads",
                        &gbl_requeue_on_tran_dispatch);
    register_int_switch("check_wrong_db",
                        "Return error if connecting to wrong database",
                        &gbl_check_wrong_db);
    register_int_switch("debug_temp_tables", "Debug temp tables",
                        &gbl_debug_temptables);
    register_int_switch("check_sql_source", "Check sql source",
                        &gbl_check_sql_source);
    register_int_switch(
        "flush_check_active_peer",
        "Check if still have active connection when trying to flush",
        &gbl_flush_check_active_peer);
    register_int_switch("private_blkseq", "Keep a private blkseq",
                        &gbl_private_blkseq);
    register_int_switch("use_blkseq", "Enable blkseq", &gbl_use_blkseq);
    register_int_switch("track_queue_time",
                        "Track time sql requests spend on queue",
                        &gbl_track_queue_time);
    register_int_switch("update_startlsn_printstep",
                        "Print steps walked in update_startlsn code",
                        &gbl_update_startlsn_printstep);
    register_int_switch("locks_check_waiters",
                        "Light a flag if a lockid has waiters",
                        &gbl_locks_check_waiters);
    register_int_switch(
        "rowlocks_commit_on_waiters",
        "Don't commit a physical transaction unless there are lock waiters",
        &gbl_rowlocks_commit_on_waiters);
    register_int_switch("log_fstsnd_triggers",
                        "Log all fstsnd triggers to file",
                        &gbl_log_fstsnd_triggers);
    register_int_switch("broadcast_check_rmtpol",
                        "Check rmtpol before sending triggers",
                        &gbl_broadcast_check_rmtpol);
    register_int_switch("noenv_requests",
                        "Send requests compatible with no environment",
                        &gbl_noenv_messages);
    register_int_switch("track_curtran_locks", "Print curtran lockinfo",
                        &gbl_track_curtran_locks);
    register_int_switch("replicate_rowlocks", "Replicate rowlocks",
                        &gbl_replicate_rowlocks);
    register_int_switch("gather_rowlocks_on_replicant",
                        "Replicant will gather rowlocks",
                        &gbl_replicant_gather_rowlocks);
    register_int_switch("force_old_cursors", "Replicant will use old cursors",
                        &gbl_force_old_cursors);
    register_int_switch("disable_rowlocks_logging",
                        "Don't add logical logging for rowlocks",
                        &gbl_disable_rowlocks_logging);
    register_int_switch("disable_rowlocks",
                        "Follow rowlocks codepath but don't lock",
                        &gbl_disable_rowlocks);
    /*
      Alias of "disable_rowlocks" to handle lrl option for backward
      compatibility.
    */
    register_int_switch("disable_rowlock_locking",
                        "Follow rowlocks codepath but don't lock",
                        &gbl_disable_rowlocks);
    register_int_switch("random_rowlocks",
                        "Grab random, guaranteed non-conflicting rowlocks",
                        &gbl_random_rowlocks);
    register_int_switch(
        "already_aborted_trace",
        "Print trace when dd_abort skips an 'already-aborted' locker",
        &gbl_already_aborted_trace);
    register_int_switch("disable_update_shadows",
                        "stub out update shadows code",
                        &gbl_disable_update_shadows);
    register_int_switch("verbose_toblock_backouts",
                        "print verbose toblock backout trace",
                        &gbl_verbose_toblock_backouts);
    register_int_switch(
        "sql_release_locks_on_si_lockwait",
        "Release sql locks from si if the rep thread is waiting",
        &gbl_sql_release_locks_on_si_lockwait);
    register_int_switch("sql_release_locks_on_emit_row_lockwait",
                        "Release sql locks when we are about to emit a row",
                        &gbl_sql_release_locks_on_emit_row);
    register_int_switch("sql_release_locks_on_slow_reader",
                        "Release sql locks if a tcp write to the client blocks",
                        &gbl_sql_release_locks_on_slow_reader);
    register_int_switch("no_timeouts_on_release_locks",
                        "Disable client-timeouts if we're releasing locks",
                        &gbl_sql_no_timeouts_on_release_locks);
    register_int_switch("sql_release_locks_in_update_shadows",
                        "Release sql locks in update_shadows on lockwait",
                        &gbl_sql_release_locks_in_update_shadows);
    register_int_switch("release_locks_trace",
                        "Print trace if we release locks",
                        &gbl_sql_release_locks_trace);
    register_int_switch("verbose_waiter_flag",
                        "Print trace setting the waiter flag in lock code",
                        &gbl_lock_get_verbose_waiter);
    register_int_switch("dump_locks_on_repwait", "Dump locks on repwaits",
                        &gbl_dump_locks_on_repwait);
    register_int_switch("dump_page_on_byteswap_error",
                        "fsnap a malformed page from byteswap",
                        &gbl_dump_page_on_byteswap_error);
    register_int_switch("dump_after_byteswap", "dump page after byteswap",
                        &gbl_dump_after_byteswap);
    register_int_switch("rl_retry_on_deadlock",
                        "retry micro commit on deadlock",
                        &gbl_micro_retry_on_deadlock);
    register_int_switch("disable_blob_check",
                        "return immediately in check_blob_buffers",
                        &gbl_disable_blob_check);
    register_int_switch("disable_new_si_overhead",
                        "return immediately in several new snapisol functions",
                        &gbl_disable_new_snapisol_overhead);
    register_int_switch("verify_all_pools",
                        "verify objects are returned to the correct pools",
                        &gbl_verify_all_pools);
    register_int_switch("print_blockp_stats",
                        "print thread-count in block processor",
                        &gbl_print_blockp_stats);
    register_int_switch("allow_parallel_rep_on_pagesplit",
                        "allow parallel rep on pgsplit",
                        &gbl_allow_parallel_rep_on_pagesplit);
    register_int_switch("allow_parallel_rep_on_prefix",
                        "allow parallel rep on bam_prefix",
                        &gbl_allow_parallel_rep_on_prefix);
    register_int_switch("verbose_net", "Net prints lots of messages",
                        &gbl_verbose_net);
    register_int_switch("only_match_on_commit",
                        "Only rep_verify_match on commit records",
                        &gbl_only_match_commit_records);
    register_int_switch("check_page_in_recovery",
                        "verify that a page has or hasn't gotten corrupt",
                        &gbl_check_page_in_recovery);
    register_int_switch("comptxn_inherit_locks",
                        "Compensating transactions inherit pagelocks",
                        &gbl_cmptxn_inherit_locks);
    register_int_switch("rep_printlock", "Print locks in rep commit",
                        &gbl_rep_printlock);
    register_int_switch("accept_on_child_nets",
                        "listen on separate port for osql net",
                        &gbl_accept_on_child_nets);
    register_int_switch("disable_etc_services_lookup",
                        "When on, disables using /etc/services first to "
                        "discover database ports",
                        &gbl_disable_etc_services_lookup);
    register_int_switch("rowlocks_deadlock_trace",
                        "Prints deadlock trace in phys.c",
                        &gbl_rowlocks_deadlock_trace);
    register_int_switch("durable_wait_seqnum_test",
                        "Enables periodic durable failures in wait-for-seqnum",
                        &gbl_durable_wait_seqnum_test);
    register_int_switch("durable_replay_test",
                        "Enables periodic durable failures in blkseq replay",
                        &gbl_durable_replay_test);
    register_int_switch(
        "stable_rootpages_test",
        "Delay sql processing to allow a schema change to finish",
        &gbl_stable_rootpages_test);
    register_int_switch("durable_set_trace",
                        "Print trace set durable and commit lsn trace",
                        &gbl_durable_set_trace);
    register_int_switch("dumptxn_at_commit",
                        "Print the logs for a txn at commit",
                        &gbl_dumptxn_at_commit);
    register_int_switch("durable_calc_trace",
                        "Print all lsns for calculate_durable_lsn",
                        &gbl_durable_calc_trace);
    register_int_switch("extended_sql_debug_trace",
                        "Print extended trace for durable sql debugging",
                        &gbl_extended_sql_debug_trace);
    register_int_switch("dump_fsql_response", "Dump fsql out messages",
                        &gbl_dump_fsql_response);
    register_int_switch("large_str_idx_find",
                        "Allow index search using out or range strings",
                        &gbl_large_str_idx_find);
    register_int_switch("fingerprint_queries",
                        "Compute fingerprint for SQL queries",
                        &gbl_fingerprint_queries);
    register_int_switch("test_curtran_change", 
                        "Test change-curtran codepath (for debugging only)",
                        &gbl_test_curtran_change_code);
    register_int_switch("test_blkseq_replay",
                        "Test blkseq replay codepath (for debugging only)",
                        &gbl_test_blkseq_replay_code);
    register_int_switch("dump_blkseq", "Dump all blkseq inserts and replays",
                        &gbl_dump_blkseq);
    register_int_switch("skip_cget_in_db_put",
                        "Don't perform a cget when we do a cput",
                        &gbl_skip_cget_in_db_put);
    register_int_switch("direct_count",
                        "skip cursor layer for simple count stmts",
                        &gbl_direct_count);
    register_int_switch("parallel_count",
                        "When 'direct_count' is on, enable thread-per-stripe",
                        &gbl_parallel_count);
    register_int_switch("debug_sqlthd_failures",
                        "Force sqlthd failures in unusual places",
                        &gbl_debug_sqlthd_failures);
    register_int_switch("random_get_curtran_failures",
                        "Force random get_curtran failures",
                        &gbl_random_get_curtran_failures);
    register_int_switch("abort_invalid_query_info_key",
                        "Abort in thread-teardown for invalid query_info_key",
                        &gbl_abort_invalid_query_info_key);
    register_int_switch("cause_random_blkseq_replays",
                        "Cause random blkseq replays from replicant",
                        &gbl_random_blkseq_replays);
    register_int_switch("disable_cnonce_blkseq",
                        "Don't use cnonce for blkseq (for testing)",
                        &gbl_disable_cnonce_blkseq);
    register_int_switch("early_verify",
                        "Give early verify errors for updates in SQLite layer",
                        &gbl_early_verify);
    register_int_switch("new_indexes",
                        "Let replicants send indexes values to master",
                        &gbl_new_indexes);
    register_int_switch("mifid2_datetime_range",
                        "Extend datetime range to meet mifid2 requirements",
                        &gbl_mifid2_datetime_range);
    register_int_switch("return_long_column_names",
                        "Enables returning of long column names. (Default: ON)",
                        &gbl_return_long_column_names);
}

static void getmyid(void)
{
    char name[1024];

    if (gethostname(name, sizeof(name))) {
        logmsg(LOGMSG_ERROR, "%s: Failure to get local hostname!!!\n", __func__);
        gbl_myhostname = "UNKNOWN";
        gbl_mynode = "localhost";
    } else {
        name[1023] = '\0'; /* paranoia, just in case of truncation */

        gbl_myhostname = strdup(name);
        gbl_mynode = intern(gbl_myhostname);
    }

    getmyaddr();
    gbl_mypid = getpid();
}

void create_marker_file() 
{
    char *marker_file;
    int tmpfd;
    for (int ii = 0; ii < thedb->num_dbs; ii++) {
        if (thedb->dbs[ii]->dbnum) {
            marker_file =
                comdb2_location("marker", "%s.trap", thedb->dbs[ii]->tablename);
            tmpfd = creat(marker_file, 0666);
            free(marker_file);
            if (tmpfd != -1) close(tmpfd);
        }
    }
    marker_file = comdb2_location("marker", "%s.trap", thedb->envname);
    tmpfd = creat(marker_file, 0666);
    free(marker_file);
    if (tmpfd != -1) close(tmpfd);
}

static void handle_resume_sc()
{
    /* if there is an active schema changes, resume it, this is automatically
     * done every time the master changes, but on startup the low level meta
     * table wasn't open yet so we couldn't check to see if a schema change was
     * in progress */
    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_RESUME_AUTOCOMMIT) &&
        thedb->master == gbl_mynode) {
        int irc = resume_schema_change();
        if (irc)
            logmsg(LOGMSG_ERROR, 
                    "failed trying to resume schema change, "
                    "if one was in progress it will have to be restarted\n");
    }
}


#define TOOL(x) #x,

#define TOOLS           \
   TOOL(cdb2_dump)      \
   TOOL(cdb2_printlog)  \
   TOOL(cdb2_stat)      \
   TOOL(cdb2_verify)

#undef TOOL
#define TOOL(x) int tool_ ##x ##_main(int argc, char *argv[]);

TOOLS

#undef TOOL
#define TOOL(x) { #x, tool_ ##x ##_main },

struct tool {
   const char *tool;
   int (*main_func)(int argc, char *argv[]);
};

struct tool tool_callbacks[] = {
   TOOLS
   NULL
};

static void wait_for_coherent()
{
    const unsigned int cslp = 10000;                 /* 10000us == 10ms */
    const unsigned int wrn_cnt = 5 * 1000000 / cslp; /* 5s */
    unsigned int counter = 1;
    while (!bdb_am_i_coherent(thedb->bdb_env)) {
        if ((++counter % wrn_cnt) == 0) {
            logmsg(LOGMSG_ERROR, "I am still incoherent\n");
        }
        usleep(cslp);
    }
}

int main(int argc, char **argv)
{
    char *marker_file;
    int ii;
    int rc;

    char *exe = NULL;

    /* clean left over transactions every 5 minutes */
    int clean_mins = 5 * 60 * 1000;

    /* allocate initializer first */
    comdb2ma_init(0, 0);

    /* more reliable */
#ifdef __linux__
    char fname[PATH_MAX];
    rc = readlink("/proc/self/exe", fname, sizeof(fname));
    if (rc > 0 && rc < sizeof(fname)) {
        fname[rc] = 0;
        exe = basename(fname);
    }
#endif
    if (exe == NULL) {
       /* more portable */
       char *arg = strdup(argv[0]);
       exe = basename(arg);
    }

    for (int i = 0; tool_callbacks[i].tool; i++) {
       if (strcmp(tool_callbacks[i].tool, exe) == 0)
          return tool_callbacks[i].main_func(argc, argv);
    }

    /* Initialize the tunables. */
    if ((init_gbl_tunables())) {
        logmsg(LOGMSG_FATAL, "Failed to initialize tunables");
        exit(1);
    }

    init_debug_switches();

    /* Initialize the plugin sub-system. */
    if ((init_plugins())) {
        logmsg(LOGMSG_FATAL, "Failed to initialize plugin sub-system");
        exit(1);
    }

    /* Initialize plugin tunables. */
    register_plugin_tunables();

    timer_init(ttrap);

    if (isatty(fileno(stdout)))
        logmsg_set_time(0);

    /* what is my local hostname */
    getmyid();

    /* ignore too large files signals */
    struct sigaction sact;
    sact.sa_handler = SIG_IGN;
    sigemptyset(&sact.sa_mask);
    sact.sa_flags = 0;
    sigaction(SIGXFSZ, &sact, NULL);

    signal(SIGTERM, clean_exit_sigwrap);

    if (debug_switch_skip_skipables_on_verify())
        gbl_berkdb_verify_skip_skipables = 1;

    init_q_vars();

    srand(time(NULL) ^ getpid() << 16);

    if (debug_switch_verbose_deadlocks())
        verbose_deadlocks = 1;

    /* line buffering in stdout */
    setvbuf(stdout, 0, _IOLBF, 0);

    crc32c_init(0);

    adjust_ulimits();
    sqlite3_tunables_init();
    thread_util_init();
    user_request_init();

    fdb_cache_init(10);
    fdb_svc_init();

    gbl_invalid_tid = pthread_self();

    logmsg(LOGMSG_INFO, "setting i/o alarm threshold to 100ms\n");

    __berkdb_write_alarm_ms = 100;
    __berkdb_read_alarm_ms = 100;
    __berkdb_fsync_alarm_ms = 100;

    berk_write_alarm_ms(__berkdb_write_alarm_ms);
    berk_read_alarm_ms(__berkdb_read_alarm_ms);
    berk_fsync_alarm_ms(__berkdb_fsync_alarm_ms);
    berk_memp_sync_alarm_ms(500);

    sighold(SIGPIPE); /*dothis before kicking off any threads*/

    thrman_init();
    javasp_once_init();

    register_all_int_switches();
    repl_list_init();

    set_portmux_bind_path(NULL);

    gbl_argc = argc;
    gbl_argv = argv;

    if (init(argc, argv) == -1) {
        logmsg(LOGMSG_FATAL, "failed to start\n");
        exit(1);
    }

    /*
      Place a freeze on tunables' registration. This is done to
      avoid multiple re-registration during the creation of temp
      table env.
    */
    gbl_tunables->freeze = 1;

    handle_resume_sc();

    /* Creating a server context wipes out the db #'s dbcommon entries.
     * Recreate them. */
    fix_lrl_ixlen();
    create_marker_file();

    create_watchdog_thread(thedb);
    create_old_blkseq_thread(thedb);
    create_stat_thread(thedb);

    /* create the offloadsql repository */
    if (!gbl_create_mode && thedb->nsiblings > 0) {
        if (osql_open(thedb)) {
            logmsg(LOGMSG_FATAL, "Failed to init osql\n");
            exit(1);
        }
    }

    /* if not using the nowatch lrl option */
    if (!gbl_watchdog_disable_at_start)
        watchdog_enable();

    llmeta_dump_mapping(thedb);

    void init_lua_dbtypes(void);
    init_lua_dbtypes();

    comdb2_timprm(clean_mins, TMEV_PURGE_OLD_LONGTRN);

    if (comdb2ma_stats_cron() != 0)
        abort();

    process_deferred_options(thedb, DEFERRED_SEND_COMMAND, NULL,
                             deferred_do_commands);
    clear_deferred_options(thedb, DEFERRED_SEND_COMMAND);

    // db started - disable recsize kludge so
    // new schemachanges won't allow broken size.
    gbl_broken_max_rec_sz = 0;
    wait_for_coherent();

    if (run_init_plugins()) {
        logmsg(LOGMSG_FATAL, "Initializer plugin failed\n");
        exit(1);
    }

    gbl_ready = 1;
    logmsg(LOGMSG_WARN, "I AM READY.\n");

    extern void *timer_thread(void *);
    pthread_t timer_tid;
    rc = pthread_create(&timer_tid, NULL, timer_thread, NULL);
    if (rc) {
        logmsg(LOGMSG_FATAL, "Can't create timer thread %d %s\n", rc, strerror(rc));
        return 1;
    }
    void *ret;
    rc = pthread_join(timer_tid, &ret);
    if (rc) {
        logmsg(LOGMSG_FATAL, "Can't wait for timer thread %d %s\n", rc,
                strerror(rc));
        return 1;
    }

    return 0;
}

int add_db(struct dbtable *db)
{
    pthread_rwlock_wrlock(&thedb_lock);

    if (hash_find_readonly(thedb->db_hash, db) != 0) {
        pthread_rwlock_unlock(&thedb_lock);
        return -1;
    }

    thedb->dbs =
        realloc(thedb->dbs, (thedb->num_dbs + 1) * sizeof(struct dbtable *));
    db->dbs_idx = thedb->num_dbs;
    thedb->dbs[thedb->num_dbs++] = db;

    /* Add table to the hash. */
    hash_add(thedb->db_hash, db);

    pthread_rwlock_unlock(&thedb_lock);
    return 0;
}

void delete_db(char *db_name)
{
    int idx;

    pthread_rwlock_wrlock(&thedb_lock);
    if ((idx = getdbidxbyname(db_name)) < 0) {
        logmsg(LOGMSG_FATAL, "%s: failed to find tbl for deletion: %s\n", __func__,
                db_name);
        exit(1);
    }

    /* Remove the table from hash. */
    hash_del(thedb->db_hash, thedb->dbs[idx]);

    for (int i = idx; i < (thedb->num_dbs - 1); i++) {
        thedb->dbs[i] = thedb->dbs[i + 1];
        thedb->dbs[i]->dbs_idx = i;
    }

    thedb->num_dbs -= 1;
    thedb->dbs[thedb->num_dbs] = NULL;
    pthread_rwlock_unlock(&thedb_lock);
}

/* rename in memory db names; fragile */
int rename_db(struct dbtable *db, const char *newname)
{
    int rc;
    char *tag_name = strdup(newname);
    char *bdb_name = strdup(newname);

    if (!tag_name || !bdb_name)
        return -1;

    pthread_rwlock_wrlock(&thedb_lock);

    /* tags */
    rename_schema(db->tablename, tag_name);

    /* bdb_state */
    bdb_state_rename(db->handle, bdb_name);

    /* db */
    hash_del(thedb->db_hash, db);
    db->tablename = (char *)newname;
    db->version = 0; /* reset, new table */
    hash_add(thedb->db_hash, db);

    pthread_rwlock_unlock(&thedb_lock);
    return 0;
}

void replace_db_idx(struct dbtable *p_db, int idx)
{
    int move = 0;
    pthread_rwlock_wrlock(&thedb_lock);

    if (idx < 0 || idx >= thedb->num_dbs ||
        strcasecmp(thedb->dbs[idx]->tablename, p_db->tablename) != 0) {
        thedb->dbs =
            realloc(thedb->dbs, (thedb->num_dbs + 1) * sizeof(struct dbtable *));
        if (idx < 0 || idx >= thedb->num_dbs) idx = thedb->num_dbs;
        thedb->num_dbs++;
        move = 1;
    }

    for (int i = (thedb->num_dbs - 1); i > idx && move; i--) {
        thedb->dbs[i] = thedb->dbs[i - 1];
        thedb->dbs[i]->dbs_idx = i;
    }

    if (!move) p_db->dbnum = thedb->dbs[idx]->dbnum;

    p_db->dbs_idx = idx;
    thedb->dbs[idx] = p_db;

    /* Add table to the hash. */
    if (move == 1) {
        hash_add(thedb->db_hash, p_db);
    }

    pthread_rwlock_unlock(&thedb_lock);
}

void epoch2a(int epoch, char *buf, size_t buflen)
{
    struct tm tmres;
    int pos;
    localtime_r((const time_t *)&epoch, &tmres);
#if defined(_SUN_SOURCE) || defined(_IBM_SOURCE)
    asctime_r(&tmres, buf);
#else
    strncpy0(buf, "epoch2a:ARCH?", buflen);
#endif
    for (pos = strlen(buf) - 1; pos >= 0; pos--) {
        if (!isspace((int)buf[pos]))
            break;
        buf[pos] = '\0';
    }
}

/* store our schemas in meta */
static int put_all_csc2()
{
    int ii;
    for (ii = 0; ii < thedb->num_dbs; ii++) {
        if (thedb->dbs[ii]->dbtype == DBTYPE_TAGGED_TABLE) {
            int rc;
            
            if (thedb->dbs[ii]->lrlfname)
                rc = load_new_table_schema_file(
                    thedb, thedb->dbs[ii]->tablename, thedb->dbs[ii]->lrlfname);
            else
                rc = load_new_table_schema_tran(thedb, NULL,
                                                thedb->dbs[ii]->tablename,
                                                thedb->dbs[ii]->csc2_schema);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "error storing schema for table '%s'\n",
                       thedb->dbs[ii]->tablename);
                return -1;
            }
        }
    }

    return 0; /*success*/
}

int check_current_schemas(void)
{
    if (gbl_create_mode) {
        /* store our schemas in meta */
        if (put_all_csc2()) {
            logmsg(LOGMSG_ERROR, "error storing schemas in meta table\n");
            return -1;
        }
    } else {
        int ii;
        int schema_errors = 0;
        for (ii = 0; ii < thedb->num_dbs; ii++) {
            if (thedb->dbs[ii]->dbtype == DBTYPE_TAGGED_TABLE) {
                int rc;
                rc = check_table_schema(thedb, thedb->dbs[ii]->tablename,
                                        thedb->dbs[ii]->lrlfname);
                if (rc != 0)
                    schema_errors++;
            }
        }
        if (schema_errors) {
            logmsg(LOGMSG_ERROR, "SCHEMA MISMATCHES DETECTED, SEE ABOVE\n");
            return -1;
        }
    }

    return 0;
}

void log_delete_add_state(struct dbenv *dbenv, struct log_delete_state *state)
{
    pthread_mutex_lock(&dbenv->log_delete_counter_mutex);
    listc_atl(&dbenv->log_delete_state_list, state);
    pthread_mutex_unlock(&dbenv->log_delete_counter_mutex);
}

void log_delete_rem_state(struct dbenv *dbenv, struct log_delete_state *state)
{
    pthread_mutex_lock(&dbenv->log_delete_counter_mutex);
    listc_rfl(&dbenv->log_delete_state_list, state);
    pthread_mutex_unlock(&dbenv->log_delete_counter_mutex);
}

void log_delete_counter_change(struct dbenv *dbenv, int action)
{
    struct log_delete_state *pstate;
    int filenum;
    pthread_mutex_lock(&dbenv->log_delete_counter_mutex);
    switch (action) {
    case LOG_DEL_ABS_ON:
        dbenv->log_delete = 1;
        break;
    case LOG_DEL_ABS_OFF:
        dbenv->log_delete = 0;
        break;
    }
    /* Find the lowest filenum in our log delete states */
    if (!dbenv->log_delete) {
        filenum = 0;
    } else {
        filenum = -1; /* delete any log file */
        LISTC_FOR_EACH(&dbenv->log_delete_state_list, pstate, linkv)
        {
            if (pstate->filenum < filenum || filenum == -1)
                filenum = pstate->filenum;
        }
    }
    dbenv->log_delete_filenum = filenum;
    pthread_mutex_unlock(&dbenv->log_delete_counter_mutex);
}

inline int debug_this_request(int until)
{
    int now = comdb2_time_epoch();
    return now <= until;
}

int thdpool_alarm_on_queing(int len)
{
    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SQL_QUEUEING_DISABLE_TRACE) &&
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SQL_QUEUEING_CRITICAL_TRACE) >=
            len)
        return 0;
    return 1;
}

int gbl_hostname_refresh_time = 60;

int comdb2_is_standalone(void *dbenv)
{
    return bdb_is_standalone(dbenv, thedb->bdb_env);
}

const char *comdb2_get_dbname(void)
{
    return thedb->envname;
}

int sc_ready(void)
{
    return gbl_backend_opened;
}

#define QUOTE_(x) #x
#define QUOTE(x) QUOTE_(x)

static void create_service_file(const char *lrlname)
{
#ifdef _LINUX_SOURCE
    char *comdb2_path = comdb2_location("scripts", "comdb2");

    char *service_file =
        comdb2_asprintf("%s/%s.service", thedb->basedir, thedb->envname);
    char lrl[PATH_MAX];
    if (lrlname) {
        if (realpath(lrlname, lrl) == NULL) {
            logmsg(LOGMSG_ERROR, "can't resolve path to lrl file\n");
        }
    }

    struct passwd *pw = getpwuid(getuid());
    if (pw == NULL) {
        logmsg(LOGMSG_ERROR, "can't resolve current user: %d %s\n", errno,
               strerror(errno));
        return;
    }

    FILE *f = fopen(service_file, "w");
    free(service_file);
    if (f == NULL) {
        logmsg(LOGMSG_ERROR, "can't create service file: %d %s\n", errno,
               strerror(errno));
        return;
    }

    fprintf(f, "[Unit]\n");
    fprintf(f, "Description=Comdb2 database instance for %s\n\n", thedb->envname);
    fprintf(f, "[Service]\n");
    fprintf(f, "ExecStart=%s --lrl %s %s\n", comdb2_path, lrl, thedb->envname);

    fprintf(f, "User=%s\n"
               "Restart=always\n"
               "RestartSec=1\n\n"
               "[Install]\n"
               "WantedBy=multi-user.target\n",
            pw->pw_name);

    free(comdb2_path);
    fclose(f);
#endif
    return;
}

#undef QUOTE
