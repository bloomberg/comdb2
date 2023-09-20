/*
   Copyright 2015, 2021, Bloomberg Finance L.P.

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

extern int gbl_delay_sql_lock_release_sec;

void __berkdb_set_num_read_ios(long long *n);
void __berkdb_set_num_write_ios(long long *n);
void __berkdb_set_num_fsyncs(long long *n);
void berk_memp_sync_alarm_ms(int);

#include <pthread.h>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <berkdb/dbinc/queue.h>
#include <limits.h>

#include <arpa/inet.h>
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

#include <epochlib.h>
#include <segstr.h>
#include "thread_stats.h"

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
#include "logmsg.h"
#include "reqlog.h"

#include "comdb2_trn_intrl.h"
#include "history.h"
#include "tag.h"
#include "types.h"
#include "timer.h"
#include <plhash.h>
#include "verify.h"
#include "ssl_bend.h"
#include "switches.h"
#include "sqloffload.h"
#include "osqlblockproc.h"
#include "osqlblkseq.h"

#include <sqliteInt.h>

#include "thdpool.h"
#include "memdebug.h"
#include "bdb_access.h"
#include "analyze.h"

#include "osqlcomm.h"
#include <cdb2_constants.h>

#include <crc32c.h>

#include "fdb_fend.h"
#include "fdb_bend.h"
#include <flibc.h>
#include "perf.h"

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
#include "comdb2_atomic.h"
#include "cron.h"
#include "metrics.h"
#include "time_accounting.h"
#include <build/db.h>
#include "comdb2_ruleset.h"
#include <hostname_support.h>
#include "string_ref.h"
#include "sql_stmt_cache.h"
#include "phys_rep.h"
#include "comdb2_query_preparer.h"
#include <net_appsock.h>
#include "sc_csc2.h"
#include "reverse_conn.h"

#define tokdup strndup

int gbl_thedb_stopped = 0;
int gbl_sc_timeoutms = 1000 * 60;
char gbl_dbname[MAX_DBNAME_LENGTH];
int gbl_largepages;
int gbl_llmeta_open = 0;

int gbl_sqlite_sortermult = 1;

int gbl_sqlite_sorter_mem = 300 * 1024 * 1024; /* 300 meg */

int gbl_strict_dbl_quotes = 0;
int gbl_rep_node_pri = 0;
int gbl_handoff_node = 0;
int gbl_use_node_pri = 0;
int gbl_allow_lua_print = 0;
int gbl_allow_lua_dynamic_libs = 0;
int gbl_lua_prepare_max_retries = 0;
int gbl_lua_prepare_retry_sleep = 200;
int gbl_allow_pragma = 0;
int gbl_master_changed_oldfiles = 0;
int gbl_recovery_timestamp = 0;
int gbl_recovery_lsn_file = 0;
int gbl_recovery_lsn_offset = 0;
int gbl_trace_prepare_errors = 0;
int gbl_trigger_timepart = 0;
int gbl_extended_sql_debug_trace = 0;
int gbl_perform_full_clean_exit = 1;
struct ruleset *gbl_ruleset = NULL;

void myctrace(const char *c) { ctrace("%s", c); }

void berkdb_use_malloc_for_regions_with_callbacks(void *mem,
                                                  void *(*alloc)(void *, int),
                                                  void (*free)(void *, void *));

extern void bb_berkdb_reset_worst_lock_wait_time_us();
extern int has_low_headroom(const char *path, int headroom, int debug);
extern void *clean_exit_thd(void *unused);
extern void bdb_durable_lsn_for_single_node(void *in_bdb_state);
/* How frequent metrics are refreshed, once per this many seconds */
int gbl_update_metrics_interval = 5;
extern void update_metrics(void);
extern void *timer_thread(void *);
extern void comdb2_signal_timer();
void init_lua_dbtypes(void);
static int put_all_csc2();

static void *purge_old_blkseq_thread(void *arg);
static void *purge_old_files_thread(void *arg);
static int lrllinecmp(char *lrlline, char *cmpto);
static void ttrap(struct timer_parm *parm);
int clear_temp_tables(void);

pthread_key_t comdb2_open_key;

/*---GLOBAL SETTINGS---*/
#define QUOTE_(x) #x
#define QUOTE(x) QUOTE_(x)
const char *const gbl_db_git_version_sha = QUOTE(GIT_VERSION_SHA=COMDB2_GIT_VERSION_SHA);
const char gbl_db_version[] = QUOTE(COMDB2_BUILD_VERSION);
const char gbl_db_semver[] = QUOTE(COMDB2_SEMVER);
const char gbl_db_codename[] = QUOTE(COMDB2_CODENAME);
const char gbl_db_buildtype[] = QUOTE(COMDB2_BUILD_TYPE);

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
int gbl_unauth_tag_access = 0;
int64_t gbl_num_auth_allowed = 0;
int64_t gbl_num_auth_denied = 0;
int gbl_uses_externalauth = 0;
int gbl_uses_externalauth_connect = 0;
int gbl_externalauth_warn = 0;
int gbl_identity_cache_max = 500;
int gbl_uses_accesscontrol_tableXnode;
int gbl_upd_key;
unsigned long long gbl_sqltick;
int gbl_watchdog_watch_threshold = 60;
int gbl_watchdog_disable_at_start = 0; /* don't enable watchdog on start */
int gbl_nonames = 1;
int gbl_reject_osql_mismatch = 1;
int gbl_abort_on_clear_inuse_rqid = 1;

pthread_t gbl_invalid_tid; /* set this to our main threads tid */

/* lots of defaults. */
int gbl_exit_on_pthread_create_fail = 1;
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
int gbl_serialise_sqlite3_open = 0;

int gbl_notimeouts = 0; /* set this if you don't need the server timeouts
                           (use this for new code testing) */

int gbl_nullfkey = 1;

/* Default fast sql timeouts */
int gbl_sqlwrtimeoutms = 10000;

long long gbl_converted_blocksql_requests = 0;

/* TODO: delete */
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
char *gbl_myhostname;      /* my hostname */
char *gbl_mycname;      /* my cname */
struct in_addr gbl_myaddr; /* my IPV4 address */
int gbl_mynodeid = 0; /* node number, for backwards compatibility */
pid_t gbl_mypid;      /* my pid */
char *gbl_myuri;      /* added for fdb uri for this db: dbname@hostname */
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
static int gbl_db_is_exiting = 0; /* Indicates this process is exiting */


int gbl_debug_omit_dta_write;
int gbl_debug_omit_idx_write;
int gbl_debug_omit_blob_write;
int gbl_debug_omit_zap_on_rebuild = 0;
int gbl_debug_txn_sleep = 0;
int gbl_debug_skip_constraintscheck_on_insert;
int gbl_debug_pb_connectmsg_dbname_check = 0;
int gbl_debug_pb_connectmsg_gibberish = 0;
double gbl_query_plan_percentage = 50;
int gbl_readonly = 0;
int gbl_init_single_meta = 1;
int gbl_schedule = 0;

int gbl_init_with_rowlocks = 0;
int gbl_init_with_genid48 = 1;
int gbl_init_with_odh = 1;
int gbl_init_with_queue_odh = 1;
int gbl_init_with_queue_compr = BDB_COMPRESS_LZ4;
int gbl_init_with_queue_persistent_seq = 0;
int gbl_init_with_ipu = 1;
int gbl_init_with_instant_sc = 1;
int gbl_init_with_compr = BDB_COMPRESS_CRLE;
int gbl_init_with_compr_blobs = BDB_COMPRESS_LZ4;
int gbl_init_with_bthash = 0;

uint32_t gbl_nsql;
long long gbl_nsql_steps;

uint32_t gbl_nnewsql;
uint32_t gbl_nnewsql_ssl;
long long gbl_nnewsql_steps;

uint32_t gbl_masterrejects = 0;

volatile uint32_t gbl_analyze_gen = 0;
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
char *gbl_spfile_name = NULL;
char *gbl_timepart_file_name = NULL;
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
int gbl_utxnid_log = 1;
int gbl_commit_lsn_map = 1;

/* how many times we retry osql for verify */
int gbl_osql_verify_retries_max = 499;

/* extended verify-checking after this many failures */
int gbl_osql_verify_ext_chk = 1;

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
uint64_t gbl_sc_headroom = 10;
/*---COUNTS---*/
long n_qtrap;
long n_fstrap;
long n_qtrap_notcoherent;
long n_bad_parm;
long n_bad_swapin;
long n_retries;
long n_missed;
long n_dbinfo;

int n_commits;
long long n_commit_time; /* in micro seconds.*/

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
int gbl_allow_user_schema;

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
extern void set_stop_mempsync_thread();
extern void bdb_prepare_close(bdb_state_type *bdb_state);
extern void bdb_stop_recover_threads(bdb_state_type *bdb_state);

int gbl_use_plan = 1;

double gbl_querylimits_maxcost = 0;
int gbl_querylimits_tablescans_ok = 1;
int gbl_querylimits_temptables_ok = 1;

double gbl_querylimits_maxcost_warn = 0;
int gbl_querylimits_tablescans_warn = 0;
int gbl_querylimits_temptables_warn = 0;
extern int gbl_empty_strings_dont_convert_to_numbers;

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
int gbl_reorder_socksql_no_deadlock = 0;
int gbl_reorder_idx_writes = 0;

char *gbl_recovery_options = NULL;

int gbl_rcache = 0;

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
int gbl_print_deadlock_cycles = 100;
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
int gbl_query_plans = 1;
int gbl_prioritize_queries = 1;
int gbl_verbose_normalized_queries = 0;
int gbl_verbose_prioritize_queries = 0;
int gbl_stable_rootpages_test = 0;

/* Only allows the ability to enable: must be enabled on a session via 'set' */
int gbl_allow_incoherent_sql = 0;

char *gbl_dbdir = NULL;
int gbl_backup_logfiles = 0;
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

extern int gbl_direct_count;
extern int gbl_parallel_count;
extern int gbl_debug_sqlthd_failures;
extern int gbl_random_get_curtran_failures;
extern int gbl_random_blkseq_replays;
extern int gbl_disable_cnonce_blkseq;
extern int gbl_create_dba_user;

int gbl_mifid2_datetime_range = 1;

int gbl_early_verify = 1;

int gbl_bbenv;
extern int gbl_legacy_defaults;

int64_t gbl_temptable_created;
int64_t gbl_temptable_create_reqs;
int64_t gbl_temptable_spills;

int gbl_osql_odh_blob = 1;

int gbl_clean_exit_on_sigterm = 1;

int gbl_is_physical_replicant;
int gbl_server_admin_mode = 0;

comdb2_tunables *gbl_tunables; /* All registered tunables */
int init_gbl_tunables();
int free_gbl_tunables();
int register_db_tunables(struct dbenv *tbl);

int destroy_plugins(void);
void register_plugin_tunables(void);
int install_static_plugins(void);
int run_init_plugins(int phase);
extern void clear_sqlhist();

int gbl_hostname_refresh_time = 60;

int gbl_pstack_self = 1;

char *gbl_cdb2api_policy_override = NULL;

int close_all_dbs_tran(tran_type *tran);

int open_all_dbs_tran(void *tran);
int reload_lua_sfuncs();
int reload_lua_afuncs();
void oldfile_clear(void);

inline int getkeyrecnums(const dbtable *tbl, int ixnum)
{
    if (ixnum < 0 || ixnum >= tbl->nix)
        return -1;
    return tbl->ix_recnums[ixnum] != 0;
}

inline int getkeysize(const dbtable *tbl, int ixnum)
{
    if (ixnum < 0 || ixnum >= tbl->nix)
        return -1;
    return tbl->ix_keylen[ixnum];
}

inline int getdatsize(const dbtable *tbl)
{
    return tbl->lrl;
}

/*lookup dbs..*/
dbtable *getdbbynum(int num)
{
    int ii;
    dbtable *p_db = NULL;
    Pthread_rwlock_rdlock(&thedb_lock);
    for (ii = 0; ii < thedb->num_dbs; ii++) {
        if (thedb->dbs[ii]->dbnum == num) {
            p_db = thedb->dbs[ii];
            break;
        }
    }
    Pthread_rwlock_unlock(&thedb_lock);
    return p_db;
}

static inline dbtable *_db_hash_find(const char *p_name)
{
    dbtable *tbl;
    tbl = hash_find_readonly(thedb->db_hash, &p_name);
    if (!tbl)
        tbl = hash_find_readonly(thedb->sqlalias_hash, &p_name);
    return tbl;
}

static inline void _db_hash_add(dbtable *tbl)
{
    hash_add(thedb->db_hash, tbl);
    if (tbl->sqlaliasname)
        hash_add(thedb->sqlalias_hash, tbl);
}

static inline void _db_hash_del(dbtable *tbl)
{
    hash_del(thedb->db_hash, tbl);

    if (tbl->sqlaliasname)
        hash_del(thedb->sqlalias_hash, tbl);
}

/* lockless -- thedb_lock should be gotten from caller */
int getdbidxbyname_ll(const char *p_name)
{
    dbtable *tbl = _db_hash_find(p_name);

    return (tbl) ? tbl->dbs_idx : -1;
}

/* get the index offset of table tablename in thedb->dbs array
 * notice that since the caller does not hold the lock, accessing
 * thedb->dbs[idx] can result in undefined behavior if that table
 * is dropped and idx would point to a different table or worse
 */
int get_dbtable_idx_by_name(const char *tablename)
{
    Pthread_rwlock_rdlock(&thedb_lock);
    int idx = getdbidxbyname_ll(tablename);
    Pthread_rwlock_unlock(&thedb_lock);
    return idx;
}

dbtable *get_dbtable_by_name(const char *p_name)
{
    dbtable *p_db = NULL;

    Pthread_rwlock_rdlock(&thedb_lock);
    p_db = _db_hash_find(p_name);
    Pthread_rwlock_unlock(&thedb_lock);
    if (!p_db && !strcmp(p_name, COMDB2_STATIC_TABLE))
        p_db = &thedb->static_table;

    return p_db;
}

dbtable *getqueuebyname(const char *name)
{
    return hash_find_readonly(thedb->qdb_hash, &name);
}

int get_max_reclen(struct dbenv *dbenv)
{
    int max = -1;
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
    if (file == -1) {
        logmsg(LOGMSG_ERROR, "get_max_reclen: failed to open %s for writing\n",
                fname);
        free(fname);
        return -1;
    }
    free(fname);

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
    dbtable *usedb;
    logmsg(LOGMSG_USER, "-----\n");
    for (jj = 0; jj < dbenv->num_dbs; jj++) {
        usedb = dbenv->dbs[jj]; /*de-stink*/
        logmsg(LOGMSG_USER,
               "table '%s' comdbg compat dbnum %d\ndir '%s' lrlfile '%s' "
               "nconns %zu  nrevconns %zu\n",
               usedb->tablename, usedb->dbnum, dbenv->basedir, (usedb->lrlfname) ? usedb->lrlfname : "NULL",
               usedb->n_constraints, usedb->n_rev_constraints);
        logmsg(LOGMSG_ERROR, "   data reclen %-3d bytes\n", usedb->lrl);

        for (ii = 0; ii < usedb->nix; ii++) {
            logmsg(LOGMSG_USER,
                   "   index %-2d keylen %-3d bytes  dupes? %c recnums? %c"
                   " datacopy? %c collattr? %c uniqnulls %c datacopylen %-3d bytes\n",
                   ii, usedb->ix_keylen[ii], (usedb->ix_dupes[ii] ? 'y' : 'n'), (usedb->ix_recnums[ii] ? 'y' : 'n'),
                   (usedb->ix_datacopy[ii] ? 'y' : 'n'), (usedb->ix_collattr[ii] ? 'y' : 'n'),
                   (usedb->ix_nullsallowed[ii] ? 'y' : 'n'), (usedb->ix_datacopy[ii] && !usedb->ix_datacopylen[ii] ? usedb->lrl : usedb->ix_datacopylen[ii]));
        }
    }
    for (ii = 0; ii < dbenv->nsiblings; ii++) {
        logmsg(LOGMSG_USER, "sibling %-2d host %s:%d\n", ii, dbenv->sibling_hostname[ii], *dbenv->sibling_port[ii]);
    }
}

enum OPENSTATES {
    OPENSTATE_THD_CREATE = 1,
    OPENSTATE_BACKEND_OPEN = 2,
    OPENSTATE_FAILED = -1,
    OPENSTATE_SUCCESS = 3
};

void block_new_requests(struct dbenv *dbenv)
{
    gbl_thedb_stopped = 1;
    MEMORY_SYNC;
}

void allow_new_requests(struct dbenv *dbenv)
{
    gbl_thedb_stopped = 0;
    MEMORY_SYNC;
}

int db_is_stopped(void)
{
    return gbl_thedb_stopped;
}

int db_is_exiting()
{
    return ATOMIC_LOAD32(gbl_db_is_exiting) != 0;
}

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
    int loop;

    struct thr_handle *thr_self = thrman_register(THRTYPE_PURGEBLKSEQ);
    thread_started("blkseq");

    dbenv->purge_old_blkseq_is_running = 1;
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDONLY);

    loop = 0;
    sleep(1);

    while (!db_is_exiting()) {

        /* Check del unused files progress about twice per threshold  */
        if (!(loop % (gbl_sc_del_unused_files_threshold_ms / (2 * 1000 /*ms per sec*/))))
            sc_del_unused_files_check_progress();

        if (loop == 3600)
            loop = 0;
        else
            ++loop;

        if (debug_switch_check_for_hung_checkpoint_thread() &&
            dbenv->master == gbl_myhostname) {
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

        if (dbenv->master == gbl_myhostname) {
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

        /* queue consumer thread admin */
        thrman_where(thr_self, "dbqueue_admin");
        rdlock_schema_lk();
        dbqueuedb_admin(dbenv);
        unlock_schema_lk();
        thrman_where(thr_self, NULL);

        /* purge old blobs.  i didn't want to make a whole new thread just
         * for this -- SJ */
        thrman_where(thr_self, "purge_old_cached_blobs");
        purge_old_cached_blobs();
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

/* sleep for secs in total by checking every second for db_is_exiting()
 * and stopping short if that's the case */
static inline void sleep_with_check_for_exiting(int secs)
{
    for(int i = 0; i < secs && !db_is_exiting(); i++)
        sleep(1);
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
    extern int gbl_all_prepare_leak;

    thrman_register(THRTYPE_PURGEFILES);
    thread_started("purgefiles");

    dbenv->purge_old_files_is_running = 1;
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDONLY);

    assert(!gbl_is_physical_replicant);

    while (!db_is_exiting()) {
        /* even though we only add files to be deleted on the master,
         * don't try to delete files, ever, if you're a replicant */
        if (thedb->master != gbl_myhostname || gbl_all_prepare_leak) {
            sleep_with_check_for_exiting(empty_pause);
            continue;
        }

        if (db_is_exiting())
            continue;

        if (!bdb_have_unused_files() && gbl_master_changed_oldfiles) {
            gbl_master_changed_oldfiles = 0;
            if ((rc = bdb_process_each_table_version_entry(
                     dbenv->bdb_env, bdb_check_files_on_disk, &bdberr)) != 0) {
                logmsg(LOGMSG_ERROR,
                       "%s: bdb_list_unused_files failed with rc=%d\n",
                       __func__, rc);
                sleep_with_check_for_exiting(empty_pause);
                continue;
            }
        }

        init_fake_ireq(thedb, &iq);

        /* ok, get to work now */
        retries = 0;
    retry:
        rc = trans_start_sc_fop(&iq, &trans);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s: failed to create transaction\n", __func__);
            sleep_with_check_for_exiting(empty_pause);
            continue;
        }

        empty = 0;
        rc = bdb_purge_unused_files(dbenv->bdb_env, trans, &bdberr);
        if (rc == 1) {
            empty = 1;
            rc = 0;
        }

        if (rc == 0) {
            rc = trans_commit(&iq, trans, gbl_myhostname);
            if (rc) {
                if (rc == RC_INTERNAL_RETRY && retries < 10) {
                    retries++;
                    goto retry;
                }
                logmsg(LOGMSG_ERROR,
                       "%s: failed to commit purged file, "
                       "rc=%d\n",
                       __func__, rc);
                sleep_with_check_for_exiting(empty_pause);
                continue;
            }

            if (empty) {
                sleep_with_check_for_exiting(empty_pause);
                continue;
            }
        } else {
            logmsg(LOGMSG_ERROR,
                   "%s: bdb_purge_unused_files failed rc=%d bdberr=%d\n",
                   __func__, rc, bdberr);
            trans_abort(&iq, trans);
            sleep_with_check_for_exiting(empty_pause);
            continue;
        }
    }

    dbenv->purge_old_files_is_running = 0;
    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDONLY);

    return NULL;
}

/* Remove all csc2 files */
static int clear_csc2_files(void)
{
    char path[PATH_MAX] = {0};
    DIR *dirp = NULL;
    struct dirent *dp = NULL;

    /* TODO: Why copy thedb->basedir? */
    snprintf(path, sizeof(path), "%s", thedb->basedir);

    dirp = opendir(path);
    if (dirp == NULL)
        return -1;
    while (dirp) {
        errno = 0;
        if ((dp = readdir(dirp)) != NULL) {
            char fullfile[PATH_MAX * 2];
            char *ptr;

            if (!strcmp(dp->d_name, ".") || !strcmp(dp->d_name, ".."))
                continue;

            ptr = strrchr(dp->d_name, '.');

            if (ptr && (strncmp(ptr, ".csc2", sizeof(".csc2")) == 0)) {
                int rc;
                snprintf(fullfile, sizeof(fullfile), "%s/%s", path, dp->d_name);
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
    logmsg(LOGMSG_WARN, "Received SIGTERM...exiting\n");

    /* Call the wrapper which checks the exit flag
       to avoid multiple clean-exit's. */
    clean_exit_thd(NULL);
}

static void free_dbtables(struct dbenv *dbenv)
{
    for (int i = dbenv->num_dbs - 1; i >= 0; i--) {
        dbtable *tbl = dbenv->dbs[i];
        delete_schema(tbl->tablename); // tags hash
        rem_dbtable_from_thedb_dbs(tbl);
        bdb_cleanup_fld_hints(tbl->handle);
        freedb(tbl);
    }
    free(dbenv->dbs);
}

static void free_view_hash(hash_t *view_hash)
{
    void *ent;
    unsigned int bkt;
    struct dbview *view;

    for (view = (struct dbview *)hash_first(view_hash, &ent, &bkt); view;
         view = (struct dbview *)hash_next(view_hash, &ent, &bkt)) {
        free(view->view_name);
        free(view->view_def);
    }
    hash_clear(view_hash);
    hash_free(view_hash);
}

/* second part of full cleanup which performs freeing of all the remaining
 * threads (not controlled by thrmgr) and free all structures
 */
static void finish_clean()
{
    int rc = backend_close(thedb);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "error backend_close() rc %d\n", rc);
    }
    bdb_cleanup_private_blkseq(thedb->bdb_env);

    /* gbl_trickle_thdpool is needed up to here */
    stop_trickle_threads();
    close_all_dbs_tran(NULL);

    eventlog_stop();

    cleanup_file_locations();
    ctrace_closelog();

    backend_cleanup(thedb);
    net_cleanup();
    cleanup_sqlite_master();

    free_dbtables(thedb);

    if (thedb->sqlalias_hash) {
        hash_clear(thedb->sqlalias_hash);
        hash_free(thedb->sqlalias_hash);
        thedb->sqlalias_hash = NULL;
    }

    if (thedb->db_hash) {
        hash_clear(thedb->db_hash);
        hash_free(thedb->db_hash);
        thedb->db_hash = NULL;
    }

    if (thedb->view_hash) {
        free_view_hash(thedb->view_hash);
        thedb->view_hash = NULL;
    }
    free(thedb->envname);

    cleanup_interned_strings();
    cleanup_peer_hash();
    free(gbl_dbdir);
    gbl_dbdir = NULL;

    cleanresources(); // list of lrls
    // TODO: would be nice but other threads need to exit first:
    // comdb2ma_exit();
    free_tzdir();
    tz_hash_free();
    clear_sqlhist();
    thd_cleanup();
    if(!all_string_references_cleared())
        abort();
}

void call_abort(int s)
{
    abort();
}

/* Full way of clean exit called when gbl_perform_full_clean_exit is enabled
 * its done in two parts, first we set gbl_db_is_exiting and wait for all
 * the thrmgr threads to exit.
 * The second part is done from main() where we then call finish_clean()
 */
static void begin_clean_exit(void)
{
    int alarmtime = (gbl_exit_alarm_sec > 0 ? gbl_exit_alarm_sec : 300);

    logmsg(LOGMSG_INFO, "CLEAN EXIT: alarm time %d\n", alarmtime);

#ifndef NDEBUG
    signal(SIGALRM, call_abort);
#endif
    /* this defaults to 5 minutes */
    alarm(alarmtime);

    XCHANGE32(gbl_db_is_exiting, comdb2_time_epoch());

    /* dont let any new requests come in.  we're going to go non-coherent
       here in a second, so letting new reads in would be bad. */
    block_new_requests(thedb);

    print_all_time_accounting();
    wait_for_sc_to_stop("exit", __func__, __LINE__);

    /* let the lower level start advertising high lsns to go non-coherent
       - dont hang the master waiting for sync replication to an exiting
       node. */
    bdb_exiting(thedb->static_table.handle);

    comdb2_signal_timer();
    stop_threads(thedb);
    set_stop_mempsync_thread();
    flush_db();

    cleanup_q_vars();
    cleanup_switches();
    free_gbl_tunables();
    destroy_plugins();
    destroy_appsock();
    bdb_prepare_close(thedb->bdb_env);
    bdb_stop_recover_threads(thedb->bdb_env);

    thrman_wait_type_exit(THRTYPE_PURGEFILES);
    // close the generic threads
    thrman_wait_type_exit(THRTYPE_GENERIC);

    if (gbl_create_mode) {
        logmsg(LOGMSG_USER, "Created database %s.\n", thedb->envname);
        finish_clean();
    }
}

/* clean_exit will be called to cleanup db structures upon exit
 * NB: This function can be called by clean_exit_sigwrap() when the db is not
 * up yet at which point we may not have much to cleanup.
 */
void clean_exit(void)
{
    report_fastseed_users(LOGMSG_ERROR);

    if(gbl_perform_full_clean_exit) {
        begin_clean_exit();
        return;
    }

    int alarmtime = (gbl_exit_alarm_sec > 0 ? gbl_exit_alarm_sec : 300);

    logmsg(LOGMSG_INFO, "CLEAN EXIT: alarm time %d\n", alarmtime);

    /* this defaults to 5 minutes */
    alarm(alarmtime);

    XCHANGE32(gbl_db_is_exiting, 1);

    /* dont let any new requests come in.  we're going to go non-coherent
       here in a second, so letting new reads in would be bad. */
    block_new_requests(thedb);

    print_all_time_accounting();
    wait_for_sc_to_stop("exit", __func__, __LINE__);

    /* let the lower level start advertising high lsns to go non-coherent
       - dont hang the master waiting for sync replication to an exiting
       node. */
    bdb_exiting(thedb->static_table.handle);

    stop_threads(thedb);
    flush_db();
    if (gbl_backend_opened)
        llmeta_dump_mapping(thedb);

#   if 0
    /* TODO: (NC) Instead of sleep(), maintain a counter of threads and wait for
      them to quit.
    */
    if (!gbl_create_mode)
        sleep(4);
#   endif

    cleanup_q_vars();
    cleanup_switches();
    free_gbl_tunables();
    destroy_plugins();
    destroy_appsock();

    if (gbl_create_mode) {
        logmsg(LOGMSG_USER, "Created database %s.\n", thedb->envname);
    }

    int rc = backend_close(thedb);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "error backend_close() rc %d\n", rc);
    }
    bdb_cleanup_private_blkseq(thedb->bdb_env);

    eventlog_stop();

    cleanup_file_locations();
    ctrace_closelog();

    backend_cleanup(thedb);
    net_cleanup();
    cleanup_sqlite_master();

    free_dbtables(thedb);

    if (thedb->sqlalias_hash) {
        hash_clear(thedb->sqlalias_hash);
        hash_free(thedb->sqlalias_hash);
        thedb->sqlalias_hash = NULL;
    }

    if (thedb->db_hash) {
        hash_clear(thedb->db_hash);
        hash_free(thedb->db_hash);
        thedb->db_hash = NULL;
    }

    if (thedb->view_hash) {
        free_view_hash(thedb->view_hash);
        thedb->view_hash = NULL;
    }

    cleanup_interned_strings();
    cleanup_peer_hash();
    free(gbl_dbdir);

    cleanresources(); // list of lrls
    // TODO: would be nice but other threads need to exit first:
    // comdb2ma_exit();
    free_tzdir();
    tz_hash_free();
    clear_sqlhist();
    if(!all_string_references_cleared())
        abort();

    logmsg(LOGMSG_USER, "goodbye\n");
    exit(0);
}

int get_elect_time_microsecs(void)
{
    if (gbl_elect_time_secs > 0) {
        /* local override has first priority */
        return gbl_elect_time_secs * 1000000;
    } 

    /* This is set in config_init, and hasn't changed in 10 years.  Let's
     * call it fixed, unless there's an override above
     */
    return 5000000;
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

dbtable *newqdb(struct dbenv *env, const char *name, int avgsz, int pagesize,
                int isqueuedb)
{
    dbtable *tbl;

    tbl = calloc(1, sizeof(dbtable));
    tbl->tablename = strdup(name);
    tbl->dbenv = env;
    tbl->dbtype = isqueuedb ? DBTYPE_QUEUEDB : DBTYPE_QUEUE;
    tbl->avgitemsz = avgsz;
    tbl->queue_pagesize_override = pagesize;
    Pthread_mutex_init(&tbl->rev_constraints_lk, NULL);

    if (tbl->dbtype == DBTYPE_QUEUEDB || tbl->dbtype == DBTYPE_QUEUE) {
        Pthread_rwlock_init(&tbl->consumer_lk, NULL);
    }

    return tbl;
}

void cleanup_newdb(dbtable *tbl)
{
    if (!tbl)
        return;

    free(tbl->sqlaliasname);

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

    if (tbl->rev_constraints) {
        free(tbl->rev_constraints);
        tbl->rev_constraints = NULL;
    }

    for (int i = 0; i < tbl->n_constraints; ++i) {
        if (tbl->constraints == NULL)
            break;
        for (int j = 0; j < tbl->constraints[i].nrules; ++j) {
            free(tbl->constraints[i].table[j]);
            free(tbl->constraints[i].keynm[j]);
        }
        free(tbl->constraints[i].table);
        free(tbl->constraints[i].keynm);
    }

    if (tbl->constraints) {
        free(tbl->constraints);
        tbl->constraints = NULL;
    }

    if (tbl->check_constraints) {
        free(tbl->check_constraints);
        tbl->check_constraints = NULL;
    }

    for (int i = 0; i < tbl->n_check_constraints; i++) {
        if ((tbl->check_constraint_query == NULL) || (tbl->check_constraint_query[i] == NULL))
            break;
        free(tbl->check_constraint_query[i]);
        tbl->check_constraint_query[i] = NULL;
    }

    if (tbl->check_constraint_query) {
        free(tbl->check_constraint_query);
        tbl->check_constraint_query = NULL;
    }

    Pthread_mutex_destroy(&tbl->rev_constraints_lk);

    if (tbl->ix_func) {
        for (int i = 0; i < tbl->num_lua_sfuncs; ++i) {
            free(tbl->lua_sfuncs[i]);
        }
        free(tbl->lua_sfuncs);
    }

    if (tbl->dbtype == DBTYPE_QUEUEDB)
        Pthread_rwlock_destroy(&tbl->consumer_lk);

    free(tbl);
}

static int resize_reverse_constraints(struct dbtable *db, size_t newsize)
{
    constraint_t **temp = realloc(db->rev_constraints, newsize * sizeof(constraint_t *));
    if (temp) {
        db->rev_constraints = temp;
        db->cap_rev_constraints = newsize;
        return 0;
    } else {
        free(db->rev_constraints);
        return 1;
    }
}

void init_reverse_constraints(struct dbtable *db)
{
    db->rev_constraints = calloc(INITREVCONSTRAINTS, sizeof(constraint_t *));
    db->cap_rev_constraints = INITREVCONSTRAINTS;
    db->n_rev_constraints = 0;
}

int add_reverse_constraint(struct dbtable *db, constraint_t *cnstrt)
{
    int rc = 0;
    if (db->n_rev_constraints >= db->cap_rev_constraints) {
        if ((rc = resize_reverse_constraints(db, 2 * db->cap_rev_constraints)) != 0) {
            return rc;
        };
    }
    db->rev_constraints[db->n_rev_constraints++] = cnstrt;
    return rc;
}

int delete_reverse_constraint(struct dbtable *db, size_t idx)
{
    int rc = 0;
    if (idx > db->n_rev_constraints) {
        return 1;
    }

    db->rev_constraints[idx] = NULL;

    for (size_t i = idx; i < db->n_rev_constraints - 1; ++i) {
        db->rev_constraints[i] = db->rev_constraints[i + 1];
        db->rev_constraints[i + 1] = NULL;
    }

    db->n_rev_constraints--;

    if (db->n_rev_constraints > 0 && db->n_rev_constraints <= db->cap_rev_constraints / 4) {
        if (db->n_rev_constraints >= db->cap_rev_constraints) {
            if ((rc = resize_reverse_constraints(db, (db->cap_rev_constraints / 2))) != 0) {
                return rc;
            };
        }
    }
    return rc;
}

/* lock mgr partition defaults */
size_t gbl_lk_parts = 73;
size_t gbl_lkr_parts = 23;
size_t gbl_lk_hash = 32;
size_t gbl_lkr_hash = 16;

char **sfuncs = NULL;
char **afuncs = NULL;

#define llmeta_set_lua_funcs(pfx)                                              \
    do {                                                                       \
        if (pfx##funcs == NULL)                                                \
            break;                                                             \
        char **func = &pfx##funcs[0];                                          \
        while (*func) {                                                        \
            int bdberr;                                                        \
            int rc = bdb_llmeta_add_lua_##pfx##func(*func, NULL, &bdberr);     \
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
            &thedb->lua_##pfx##funcs, &bdberr);                                \
        if (rc) {                                                              \
            logmsg(LOGMSG_ERROR, "bdb_llmeta_get_lua_" #pfx "funcs bdberr:%d\n",\
                    bdberr);                                                   \
        }                                                                      \
        logmsg(LOGMSG_INFO, "loaded num_lua_" #pfx "funcs:%d\n",               \
               listc_size(&thedb->lua_##pfx##funcs));                          \
        return rc;                                                             \
    } while (0)

#define get_funcs(funcs, pfx)                                                  \
    do {                                                                       \
        *funcs = *(listc_t*)&thedb->lua_##pfx##funcs;                          \
    } while (0)

#define find_lua_func(name, pfx)                                               \
    do {                                                                       \
        rdlock_schema_lk();                                                    \
        struct lua_func_t * func;                                              \
        int found = 0;                                                         \
        LISTC_FOR_EACH(&thedb->lua_##pfx##funcs, func, lnk)                    \
            if (strcmp(func->name, name) == 0)                                 \
                found = 1;                                                     \
        unlock_schema_lk();                                                    \
        return found;                                                          \
    } while (0)

int llmeta_load_lua_sfuncs() { llmeta_load_lua_funcs(s); }

int llmeta_load_lua_afuncs() { llmeta_load_lua_funcs(a); }

void get_sfuncs(listc_t * funcs)
{
    get_funcs(funcs, s);
}

void get_afuncs(listc_t * funcs)
{
    get_funcs(funcs, a);
}

int lua_sfunc_used(const char *func, char**tbl)
{
    rdlock_schema_lk();
    struct dbtable *db = 0;
    int used = 0;
    struct dbtable **dbs = thedb->dbs;

    for (int i = 0; i < thedb->num_dbs && used != 1; ++i) {
        db = dbs[i];
        if (!is_sqlite_stat(db->tablename) && db->ix_func) {
            for (int j = 0; j < db->num_lua_sfuncs; ++j) {
                used |= (int)(strcmp(db->lua_sfuncs[j], func) == 0);
                if (used && tbl) {
                    *tbl = db->tablename;
                }
            }
        }
    }
    unlock_schema_lk();
    return used;
}

int find_lua_sfunc(const char *name) { find_lua_func(name, s); }

int find_lua_afunc(const char *name) { find_lua_func(name, a); }

int llmeta_load_tables_older_versions(struct dbenv *dbenv, void *tran)
{
    int rc = 0, bdberr, dbnums[MAX_NUM_TABLES], fndnumtbls, i;
    char *tblnames[MAX_NUM_TABLES];
    dbtable *tbl;

    /* nothing to do */
    if (gbl_create_mode)
        return 0;

    /* re-load the tables from the low level metatable */
    if (bdb_llmeta_get_tables(tran, tblnames, dbnums, MAX_NUM_TABLES,
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

        rc = bdb_get_csc2_highest(tran, tblnames[i], &ver, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "couldn't get highest version number for %s\n",
                    tblnames[i]);
            rc = 1;
            goto cleanup;
        }

        int isc = 0;
        get_db_instant_schema_change_tran(tbl, &isc, tran);
        if (isc) {
            /* load schema for older versions */
            for (int v = 1; v <= ver; ++v) {
                char *csc2text;
                if (get_csc2_file_tran(tbl->tablename, v, &csc2text, NULL,
                                       tran)) {
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
                          (dbenv->num_qdbs + fnd_queues) * sizeof(dbtable *));
    if (dbenv->qdbs == NULL) {
        logmsg(LOGMSG_ERROR, "can't allocate memory for queue list\n");
        return -1;
    }
    for (int i = 0; i < fnd_queues; i++) {
        dbtable *tbl;
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

        rc = bdb_llmeta_get_queue(NULL, qnames[i], &config, &ndests, &dests,
                                  &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "can't get information for queue \"%s\"\n",
                    qnames[i]);
            return -1;
        }

        rc = dbqueuedb_add_consumer(tbl, 0, dests[0], 0);
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

int llmeta_load_views(struct dbenv *dbenv, void *tran)
{
    int rc = 0;
    int bdberr = 0;
    char *view_names[MAX_NUM_VIEWS];
    char *view_def;
    int view_count = 0;
    hash_t *view_hash;

    view_hash = hash_init_strcaseptr(offsetof(struct dbview, view_name));

    /* load the tables from the low level metatable */
    if (bdb_get_view_names(tran, (char **)view_names, &view_count)) {
        logmsg(
            LOGMSG_ERROR,
            "couldn't load view names from low level meta table (bdberr: %d)\n",
            bdberr);
        return 1;
    }

    for (int i = 0; i < view_count; i++) {
        struct dbview *view = calloc(1, sizeof(struct dbview));
        if (!view) {
            logmsg(LOGMSG_ERROR, "%s:%d system out of memory\n", __func__,
                   __LINE__);
            rc = 1;
            goto err;
        }

        if (bdb_get_view(tran, view_names[i], &view_def)) {
            logmsg(LOGMSG_ERROR,
                   "couldn't load view definition from low level meta table "
                   "(bdberr: %d)\n",
                   bdberr);
            free(view);
            goto err;
        }

        view->view_name = view_names[i];
        view->view_def = view_def;
        hash_add(view_hash, view);
    }

    free_view_hash(thedb->view_hash);
    thedb->view_hash = view_hash;
    return 0;

err:
    for (int i = 0; i < view_count; i++) {
        free(view_names[i]);
    }
    free_view_hash(view_hash);
    return rc;
}

static inline int db_get_alias(void *tran, dbtable *tbl)
{
    char *sqlalias = NULL;
    int rc;

    rc = bdb_get_table_sqlalias_tran(tbl->tablename, tran, &sqlalias);
    if (rc < 0)
        return -1;
    if (sqlalias) {
        tbl->sqlaliasname = sqlalias;
        logmsg(LOGMSG_INFO, "%s: found table %s alias %s\n", __func__,
               tbl->tablename, tbl->sqlaliasname);
    }
    return 0;
}


/* gets the table names and dbnums from the low level meta table and sets up the
 * dbenv accordingly.  returns 0 on success and anything else otherwise */
static int llmeta_load_tables(struct dbenv *dbenv, void *tran)
{
    char *dbname = dbenv->envname;
    int rc = 0, bdberr, dbnums[MAX_NUM_TABLES], fndnumtbls, i;
    char *tblnames[MAX_NUM_TABLES];
    dbtable *tbl;

    /* load the tables from the low level metatable */
    if (bdb_llmeta_get_tables(tran, tblnames, dbnums, MAX_NUM_TABLES,
                              &fndnumtbls, &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "couldn't load tables from low level meta table"
                             "\n");
        return 1;
    }

    /* set generic settings, likely already set when env was opened, but make
     * sure */
    bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_GENIDS, 1);

    /* Initialize static table once */
    if (dbenv->static_table.dbs_idx == 0) {
        logmsg(LOGMSG_INFO, "%s initializing static table '%s'\n", __func__,
               COMDB2_STATIC_TABLE);
        dbenv->static_table.dbs_idx = -1;
        dbenv->static_table.tablename = COMDB2_STATIC_TABLE;
        dbenv->static_table.dbenv = dbenv;
        dbenv->static_table.dbtype = DBTYPE_TAGGED_TABLE;
        dbenv->static_table.handle = dbenv->bdb_env;
    }

    /* make room for dbs */
    dbenv->dbs = realloc(dbenv->dbs, fndnumtbls * sizeof(dbtable *));
    bzero(dbenv->dbs, fndnumtbls * sizeof(dbtable *));

    for (i = 0; i < fndnumtbls; ++i) {
        char *csc2text = NULL;
        int ver;
        int bdberr;

        logmsg(LOGMSG_INFO, "%s loading table '%s'\n", __func__, tblnames[i]);
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
        rc = bdb_get_csc2_highest(tran, tblnames[i], &ver, &bdberr);
        if (rc) {
            logmsg(LOGMSG_DEBUG, "%s get_csc2_highest for '%s' returns %d\n",
                   __func__, tblnames[i], rc);
            break;
        }

        logmsg(LOGMSG_DEBUG, "%s got version %d for table '%s'\n", __func__,
               ver, tblnames[i]);

        /* create latest version of db */
        rc = get_csc2_file_tran(tblnames[i], ver, &csc2text, NULL, tran);
        if (rc) {
            logmsg(LOGMSG_ERROR, "get_csc2_file failed %s:%d\n", __FILE__, __LINE__);
            break;
        }

        struct errstat err = {0};
        tbl = create_new_dbtable(dbenv, tblnames[i], csc2text, dbnums[i], i, 0,
                                 0, 0, &err);
        free(csc2text);
        csc2text = NULL;
        if (!tbl) {
            logmsg(LOGMSG_ERROR, "%s (%s:%d)\n", err.errstr, __FILE__,
                   __LINE__);
            rc = 1;
            break;
        }

        tbl->schema_version = ver;

        rc = db_get_alias(tran, tbl);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to get sqlalias for table %s rc %d\n", __func__,
                   tbl->tablename, rc);
            rc = 1;
            break;
        }

        /* We only want to load older schema versions for ODH databases.  ODH
         * information is stored in the meta table (not the llmeta table), so
         * it's not loaded yet.
         */

        /* add to env */
        dbenv->dbs[i] = tbl;
        /* Add table to the hash. */
        _db_hash_add(tbl);
    }

    /* we have to do this after all the meta table lookups so that the hack in
     * get_meta_int works */
    dbenv->num_dbs = fndnumtbls;

    i = 0;
    while (i < fndnumtbls)
        free(tblnames[i++]);

    if (rc == 0) {
        rc = llmeta_load_views(dbenv, tran);
    }

    return rc;
}

int llmeta_load_timepart(struct dbenv *dbenv)
{
    /* We need to do this before resuming schema change, if any */
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
                    "table %s %d\n\tdata files: %016" PRIx64 "\n\tblob files\n",
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

            sbuf2printf(sbfile, "\t\tblob num %d: %016" PRIx64 "\n", j,
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

            sbuf2printf(sbfile, "\t\tindex num %d: %016" PRIx64 "\n", j,
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
    dbtable *p_db;

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
        logmsg(LOGMSG_INFO,
               "table %s\n\tdata files: %016" PRIx64 "\n\tblob files\n",
               p_db->tablename, flibc_htonll(version_num));
    else
        ctrace("table %s\n\tdata files: %016" PRIx64 "\n\tblob files\n",
               p_db->tablename, flibc_htonll(version_num));

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
            logmsg(LOGMSG_INFO, "\t\tblob num %d: %016" PRIx64 "\n", i,
                   flibc_htonll(version_num));
        else
            ctrace("\t\tblob num %d: %016" PRIx64 "\n", i,
                   flibc_htonll(version_num));
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
            logmsg(LOGMSG_INFO, "\t\tindex num %d: %016" PRIx64 "\n", i,
                   flibc_htonll(version_num));
        else
            ctrace("\t\tindex num %d: %016" PRIx64 "\n", i,
                   flibc_htonll(version_num));
    }

    return 0;
}

int llmeta_dump_mapping_table(struct dbenv *dbenv, const char *table, int err)
{
    return llmeta_dump_mapping_table_tran(NULL, dbenv, table, err);
}

struct dbenv *newdbenv(char *dbname, char *lrlname)
{
    int rc;
    struct dbenv *dbenv = calloc(1, sizeof(struct dbenv));
    if (dbenv == 0) {
        logmsg(LOGMSG_FATAL, "newdb:calloc dbenv");
        return NULL;
    }
    thedb = dbenv;

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
    Pthread_mutex_init(&dbenv->log_delete_counter_mutex, NULL);

    /* assume I want a cluster, unless overridden by -local or cluster none */

    dbenv->envname = strdup(dbname);

    Pthread_mutex_init(&dbenv->incoherent_lk, NULL);

    listc_init(&dbenv->lua_sfuncs, offsetof(struct lua_func_t, lnk));
    listc_init(&dbenv->lua_afuncs, offsetof(struct lua_func_t, lnk));

    /* Initialize the table/queue hashes. */
    dbenv->db_hash = hash_init_strcaseptr(offsetof(dbtable, tablename));
    dbenv->sqlalias_hash =
        hash_init_strcaseptr(offsetof(dbtable, sqlaliasname));
    dbenv->qdb_hash = hash_init_strcaseptr(offsetof(dbtable, tablename));
    dbenv->view_hash = hash_init_strcaseptr(offsetof(struct dbview, view_name));

    /* Register all db tunables. */
    if ((register_db_tunables(dbenv))) {
        logmsg(LOGMSG_FATAL, "Failed to initialize tunables\n");
        exit(1);
    }

    listc_init(&dbenv->lrl_handlers, offsetof(struct lrl_handler, lnk));
    listc_init(&dbenv->message_handlers, offsetof(struct message_handler, lnk));
    tz_hash_init();

    plugin_post_dbenv_hook(dbenv);

    if (read_lrl_files(dbenv, lrlname)) {
        logmsg(LOGMSG_FATAL, "Failure in reading lrl file(s)\n");
        exit(1);
    }

    logmsg(LOGMSG_INFO, "database %s starting\n", dbenv->envname);

    if (!dbenv->basedir) {
        logmsg(LOGMSG_FATAL, "DB directory is not set in lrl\n");
        return NULL;
    } 

    if (gbl_create_mode) {
        logmsg(LOGMSG_DEBUG, "gbl_create_mode is on, "
               "creating the database directory exists %s\n", dbenv->basedir);
        rc = mkdir(dbenv->basedir, 0774);
        if (rc && errno != EEXIST) {
            logmsg(LOGMSG_ERROR, "mkdir(%s): %s\n", dbenv->basedir,
                   strerror(errno));
        }
    } 

    /* make sure the database directory exists! */
    struct stat sb = {0};
    rc = stat(dbenv->basedir, &sb);
    if (rc || !S_ISDIR(sb.st_mode)) {
        logmsg(LOGMSG_FATAL, "DB directory '%s' does not exist\n",
               dbenv->basedir);
        return NULL;
    }

    init_sql_hint_table();
    init_clientstats_table();

    dbenv->long_trn_table = hash_init(sizeof(unsigned long long));

    if (dbenv->long_trn_table == NULL) {
        logmsg(LOGMSG_ERROR, "couldn't allocate long transaction lookup table\n");
        return NULL;
    }
    Pthread_mutex_init(&dbenv->long_trn_mtx, NULL);

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

    thedb_set_master(db_eid_invalid);
    dbenv->errstaton = 1; /* ON */

    dbenv->handle_buf_queue_time = time_metric_new("handle_buf_time_in_queue");
    dbenv->sql_queue_time = time_metric_new("sql_time_in_queue");
    dbenv->service_time = time_metric_new("service_time");
    dbenv->queue_depth = time_metric_new("queue_depth");
    dbenv->concurrent_queries = time_metric_new("concurrent_queries");
    dbenv->connections = time_metric_new("connections");

    return dbenv;
}

// TODO: call this remove all rather than
// free
int lua_func_list_free(void * list) {
    struct lua_func_t *item, *tmp;
    listc_t *list_ptr = list;

    /* free each item */
    LISTC_FOR_EACH_SAFE(list_ptr, item, tmp, lnk)
    /* remove and free item */
    free(listc_rfl(list, item));

    listc_init(list, offsetof(struct lua_func_t, lnk));
    return 0;
}

extern pthread_key_t DBG_FREE_CURSOR;

/* check that we don't have name clashes, and other sanity checks, this also
 * populates some values like reverse constraints and db->dtastripe */
static int db_finalize_and_sanity_checks(struct dbenv *dbenv)
{
    int have_bad_schema = 0, ii, jj;

    for (ii = 0; ii < dbenv->num_dbs; ii++) {
        struct dbtable * db = dbenv->dbs[ii];
        db->dtastripe = 1;

        for (jj = 0; jj < dbenv->num_dbs; jj++) {
            if (jj != ii) {
                if (strcasecmp(db->tablename,
                               dbenv->dbs[jj]->tablename) == 0) {
                    have_bad_schema = 1;
                    logmsg(LOGMSG_FATAL,
                           "Two tables have identical names (%s) tblnums %d "
                           "%d\n",
                           db->tablename, ii, jj);
                }
            }
        }

        if ((strcasecmp(db->tablename, dbenv->envname) == 0) &&
            (db->dbnum != 0) &&
            (dbenv->dbnum != db->dbnum)) {

            have_bad_schema = 1;
            logmsg(LOGMSG_FATAL, "Table name and database name conflict (%s) tblnum %d\n",
                   dbenv->envname, ii);
        }

        if (db->nix > MAXINDEX) {
            have_bad_schema = 1;
            logmsg(LOGMSG_FATAL, "Database %s has too many indexes (%d)\n",
                   db->tablename, db->nix);
        }

        /* last ditch effort to stop invalid schemas getting through */
        for (jj = 0; jj < db->nix && jj < MAXINDEX; jj++)
            if (db->ix_keylen[jj] > MAXKEYLEN + 1) {
                have_bad_schema = 1;
                logmsg(LOGMSG_FATAL, "Database %s index %d too large (%d)\n",
                       db->tablename, jj,
                       db->ix_keylen[jj]);
            }

        /* verify constraint names and add reverse constraints here */
        if (populate_reverse_constraints(db))
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
        rc =
            bdb_llmeta_get_queue(NULL, name, &config, &ndests, &dests, &bdberr);
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

static int dump_timepartitions(const char *dir, const char *dbname,
                               const char *filename)
{
    int has_tp = 0;
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s_%s", dir, dbname, filename);
    FILE *f = fopen(path, "w");
    if (f == NULL) {
        logmsg(LOGMSG_ERROR, "%s:fopen(\"%s\"):%s\n", __func__, path,
               strerror(errno));
        return -1;
    }

    /* save the configuration */
    has_tp = timepart_dump_timepartitions(f);

    fclose(f);
    logmsg(LOGMSG_INFO, "%s wrote time partitions configuration in: %s\n",
           __func__, path);

    return has_tp;
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

    int has_tp = dump_timepartitions(p_data->lrl_fname_out_dir, thedb->envname,
                                     TIMEPART_FILE_NAME);
    if (has_tp < 0) {
        free(p_data);
        return -1;
    }

    /* write out the lrl */
    if (rewrite_lrl_un_llmeta(getresourcepath("lrl"), p_lrl_fname_out,
                              p_data->p_table_names, p_data->p_csc2_paths,
                              p_data->table_nums, thedb->num_dbs,
                              p_data->lrl_fname_out_dir, has_sp, has_tp)) {
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
            strncpy0(q->name, name, sizeof(q->name));
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
    char config[n + 1];
    if (fread(config, n, 1, f) == 0) {
        fclose(f);
        return -1;
    }
    config[n] = 0;
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

static int init_sqlite_tables(struct dbenv *dbenv)
{
    /* There's no 2 or 3.  There used to be 2.  There was never 3. */
    const char *sqlite_stats_name[2] = {"sqlite_stat1", "sqlite_stat4"};
    const char *sqlite_stats_csc2[2] = {
        "tag ondisk { "
        "    cstring tbl[64] "
        "    cstring idx[64] null=yes "
        "    cstring stat[4096] "
        "} "
        " "
        "keys { "
        "    \"0\" = tbl + idx "
        "} ",
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
        "} "};
    dbtable *tbl;
    struct errstat err = {0};
    int i;

    /* This used to just pull from installed files.  Let's just do it from memory
       so comdb2 can run standalone with no support files. */

    for (i = 0; i < 2; i++) {
        if (get_dbtable_by_name(sqlite_stats_name[i]))
            continue;

        tbl = create_new_dbtable(dbenv, (char *)sqlite_stats_name[i],
                                 (char *)sqlite_stats_csc2[i], 0,
                                 dbenv->num_dbs, 0, 0, 0, &err);
        if (!tbl) {
            logmsg(LOGMSG_ERROR, "%s\n", err.errstr);
            return -1;
        }
        tbl->csc2_schema = strdup(sqlite_stats_csc2[i]);

        /* Add table to the hash. */
        _db_hash_add(tbl);

        /* Add table to thedb->dbs */
        dbenv->dbs =
            realloc(dbenv->dbs, (dbenv->num_dbs + 1) * sizeof(dbtable *));
        dbenv->dbs[dbenv->num_dbs++] = tbl;
    }
    return 0;
}

static void load_dbstore_tableversion(struct dbenv *dbenv, tran_type *tran)
{
    int i;

    for (i = 0; i < dbenv->num_dbs; i++) {
        dbtable *tbl = dbenv->dbs[i];
        update_dbstore(tbl);

        tbl->tableversion = table_version_select(tbl, tran);
        if (tbl->tableversion == -1) {
            logmsg(LOGMSG_ERROR, "Failed reading table version\n");
        }
    }
}

static int create_db(char *dbname, char *dir) {
   int rc;

   char *fulldir;
   fulldir = comdb2_realpath(dir, NULL);
   if (fulldir == NULL) {
      rc = mkdir(dir, 0755);
      if (rc) {
         logmsg(LOGMSG_FATAL, 
               "%s doesn't exist, and couldn't create: %s\n", dir,
               strerror(errno));
         return -1;
      }
      fulldir = comdb2_realpath(dir, NULL);
      if (fulldir == NULL) {
         logmsg(LOGMSG_FATAL, "Can't figure out full path for %s\n", dir);
         return -1;
      }
   }
   dir = fulldir;
   logmsg(LOGMSG_INFO, "Creating db in %s\n", dir);
   setenv("COMDB2_DB_DIR", fulldir, 1);

   rc = init_db_dir(dbname, dir);
   free(fulldir);
   if (rc) return -1;

   /* set up as a create run */
   gbl_local_mode = 1;
   /* delay 'gbl_create_mode' so we can use for --create */
   gbl_exit = 1;

   return 0;
}

static void setup_backup_logfiles_dir()
{
    char *backupdir = comdb2_location_in_hash("backup_logfiles_dir", NULL);
    if (!backupdir) {
        logmsg(LOGMSG_DEBUG, "%s: Location for backup_logfiles_dir is not set\n", __func__);
        goto cleanup;
    }

    /* if path like "..../%dbname" then substitute %dbname with thedb->envname */
    char *loc = strstr(backupdir, "%dbname");
    if (loc) {
        int dbnamelen = strlen(thedb->envname);
        int diff = dbnamelen - sizeof("%dbname");
        if (diff > 0) {
            int newlen = (loc - backupdir) + dbnamelen + 1;
            char *newd = realloc(backupdir, newlen);
            if (!newd) {
                logmsg(LOGMSG_ERROR, "%s: Cannot realloc backupdir newlen %d\n",
                       __func__, newlen);
                goto cleanup;
            }
            loc = newd + (loc - backupdir);
            backupdir = newd;
        }
        strcpy(loc, thedb->envname);
        update_file_location("backup_logfiles_dir", backupdir);
    }

    struct stat stats;
    int rc = stat(backupdir, &stats);
    if (rc)
        logmsg(LOGMSG_WARN, "%s: Cannot stat directory %s: %d %s\n",
               __func__, backupdir, errno, strerror(errno));

    if (S_ISDIR(stats.st_mode)) {
        gbl_backup_logfiles = 1;
    } else {
        int mask = umask(0);
        umask(mask);
        //try to create directory, if successful turn on feature
        rc = mkdir(backupdir, 0777 & (~mask));
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: Cannot create directory %s (bad path or parent directory): %d %s\n",
                   __func__, backupdir, errno, strerror(errno));
        } else {
            logmsg(LOGMSG_DEBUG, "%s: Created directory %s\n", __func__, backupdir);
            gbl_backup_logfiles = 1;
        }
    }
cleanup:
    free(backupdir);
}

static int init(int argc, char **argv)
{
    char *dbname, *lrlname = NULL, ctmp[64];
    static int noabort = 0;
    int cacheszkb = 0, ii;
    int rc;
    int bdberr;
    int cacheszkb_suggestion = 0;
    int stripes, blobstripe;

    if (argc < 2) {
        print_usage_and_exit(1);
    }

    csc2_allow_bools();

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
    if (clnt_stats_init()) {
        logmsg(LOGMSG_FATAL, "failed to initialise connection tracking module\n");
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

    initresourceman(NULL);

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

    if (mach_class_init()) {
        logmsg(LOGMSG_FATAL, "Failed to initialize machine classes\n");
        exit(1);
    }

    handle_cmdline_options(argc, argv, &lrlname);

    if (gbl_create_mode) {        /*  10  */
        logmsg(LOGMSG_INFO, "create mode.\n");
        gbl_exit = 1;
    }
    if (gbl_fullrecovery) {       /*  11  */
        logmsg(LOGMSG_WARN, "force full recovery.\n");
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
    char tmpuri[1024];
    snprintf(tmpuri, sizeof(tmpuri), "%s@%s", gbl_dbname, gbl_myhostname);
    gbl_myuri = intern(tmpuri);

    if (optind < argc && isdigit((int)argv[optind][0])) {
        cacheszkb = atoi(argv[optind]);
    }

    gbl_mynodeid = machine_num(gbl_myhostname);

    Pthread_attr_init(&gbl_pthread_attr);
    Pthread_attr_setstacksize(&gbl_pthread_attr, DEFAULT_THD_STACKSZ);
    Pthread_attr_setdetachstate(&gbl_pthread_attr, PTHREAD_CREATE_DETACHED);

    /* Initialize the statistics. */
    init_metrics();

    Pthread_key_create(&comdb2_open_key, NULL);
    Pthread_key_create(&query_info_key, NULL);
    Pthread_key_create(&DBG_FREE_CURSOR, free);

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
            gbl_dbdir = comdb2_realpath(".", NULL);
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

    run_init_plugins(COMDB2_PLUGIN_INITIALIZER_PRE);

    /* open database environment, and all dbs */
    thedb = newdbenv(dbname, lrlname);
    if (thedb == 0)
        return -1;

    /* Initialize SSL backend before creating any net.
       If we're exiting, don't bother. */
    if (!gbl_exit && ssl_bend_init(thedb->basedir) != 0) {
        logmsg(LOGMSG_FATAL, "Failed to initialize SSL backend.\n");
        return -1;
    }
    logmsg(LOGMSG_INFO, "SSL backend initialized.\n");

    /* prepare the server class ahead of time, after libssl is initialized. */
    get_my_mach_class();

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
    setup_backup_logfiles_dir();

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
        if (!gbl_noenv_messages) {
            logmsg(LOGMSG_FATAL,
                   "This server build cannot run with legacy messages! Please rollback\n");
            abort();
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
        if (cacheszkb != 0) { /*command line overrides.*/
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

    gbl_myroom = getroom_callback(NULL, gbl_myhostname);

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

    berk_init_rep_lockobj();

    /* disallow bools on test machines.  Prod will continue
     * to allow them because at least one prod database uses them.
     * Still alow bools for people who want to copy/test prod dbs
     * that use them.  Don't allow new databases to have bools. */
    if ((get_my_mach_class() == CLASS_TEST) && gbl_create_mode) {
        if (csc2_used_bools()) {
            logmsg(LOGMSG_FATAL, "bools in schema.  This is now deprecated.\n");
            logmsg(LOGMSG_FATAL, "Exiting since this is a test machine.\n");
            exit(1);
        }
        csc2_disallow_bools();
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

    if (!gbl_exit && gbl_new_snapisol && gbl_snapisol) {
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

    wrlock_schema_lk();

    /* open the table */
    if (llmeta_open()) {
        return -1;
    }

    logmsg(LOGMSG_INFO, "Successfully opened low level meta table\n");

    gbl_llmeta_open = 1;

    if (gbl_create_mode &&
        bdb_set_global_stripe_info(NULL, gbl_dtastripe, gbl_blobstripe,
                                   &bdberr) != 0) {
        logmsg(LOGMSG_FATAL, "Error writing global stripe info\n");
        exit(1);
    }

    if (!gbl_create_mode &&
        bdb_get_global_stripe_info(NULL, &stripes, &blobstripe, &bdberr) == 0 &&
        stripes > 0) {
        gbl_dtastripe = stripes;
        gbl_blobstripe = blobstripe;
        bdb_attr_set(thedb->bdb_attr, BDB_ATTR_DTASTRIPE, gbl_dtastripe);
        bdb_attr_set(thedb->bdb_attr, BDB_ATTR_BLOBSTRIPE, gbl_blobstripe);
    }

    if (!gbl_create_mode) {
       uint64_t format;
       bdb_get_genid_format(&format, &bdberr);
       bdb_genid_set_format(thedb->bdb_env, format);
    }

    set_datetime_dir();

    /* get/set the table names from llmeta */
    if (gbl_create_mode) {
        if (!gbl_legacy_defaults)
            if (init_sqlite_tables(thedb))
                return -1;
    } else if (thedb->num_dbs != 0) {
        /* if we are using low level meta table and this isn't the create
         * pass, we shouldn't see any table definitions in the lrl. they
         * should have been removed during initialization */
        logmsg(LOGMSG_FATAL, "lrl contains table definitions, they should not "
                             "be present after the database has been "
                             "created\n");
        return -1;
    } else {
        /* we will load the tables from the llmeta table */
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

        if (llmeta_load_tables(thedb, NULL)) {
            logmsg(LOGMSG_FATAL, "could not load tables from the low level meta "
                            "table\n");
            unlock_schema_lk();
            return -1;
        }

        if (llmeta_load_timepart(thedb)) {
            logmsg(LOGMSG_ERROR, "could not load time partitions\n");
            unlock_schema_lk();
            return -1;
        }

        if (llmeta_load_queues(thedb)) {
            logmsg(LOGMSG_FATAL, "could not load queues from the low level meta "
                            "table\n");
            unlock_schema_lk();
            return -1;
        }

        if (llmeta_load_lua_sfuncs()) {
            logmsg(LOGMSG_FATAL, "could not load lua funcs from llmeta\n");
            unlock_schema_lk();
            return -1;
        }

        if (llmeta_load_lua_afuncs()) {
            logmsg(LOGMSG_FATAL, "could not load lua aggs from llmeta\n");
            unlock_schema_lk();
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
            unlock_schema_lk();
            gbl_perform_full_clean_exit = 0;
            clean_exit();
        }
    }

    /* if we're in repopulate .lrl mode we should have already exited */
    if (gbl_repoplrl_fname) {
        logmsg(LOGMSG_FATAL, "Repopulate .lrl mode failed. Possible causes: db not "
                        "using llmeta or .lrl file already had table defs\n");
        unlock_schema_lk();
        return -1;
    }

    /* do sanity checks and populate a few per db values */
    if (db_finalize_and_sanity_checks(thedb))
        return -1;

    if (!gbl_exit) {
        check_access_controls(thedb); /* Check authentication settings */

        if (!have_all_schemas()) {
            logmsg(LOGMSG_ERROR,
                  "Server-side keyforming not supported - missing schemas\n");
            unlock_schema_lk();
            return -1;
        }

        fix_lrl_ixlen(); /* set lrl, ix lengths: ignore lrl file, use info from
                            schema */
    }

    /* historical requests */
    if (gbl_loghist) {
        reqhist = malloc(sizeof(history));
        rc = init_history(reqhist, gbl_loghist);
        if (gbl_loghist_verbose)
            reqhist->wholereq = 1;
        if (rc) {
            logmsg(LOGMSG_FATAL, "History init failed\n");
            unlock_schema_lk();
            return -1;
        }
    }

    if (gbl_create_mode) {
        create_service_file(lrlname);
    }

    /* open db engine */
    logmsg(LOGMSG_INFO, "starting backend db engine\n");

    if (backend_open(thedb) != 0) {
        logmsg(LOGMSG_FATAL, "failed to open '%s'\n", dbname);
        unlock_schema_lk();
        return -1;
    }

    if (llmeta_load_tables_older_versions(thedb, NULL)) {
        logmsg(LOGMSG_FATAL, "llmeta_load_tables_older_versions failed\n");
        unlock_schema_lk();
        return -1;
    }

    load_dbstore_tableversion(thedb, NULL);

    gbl_backend_opened = 1;

    /* Recovered prepares need the osql-cnonce hash */
    if (!gbl_exit && !gbl_create_mode) {
        rc = osql_blkseq_init();
        if (rc) {
            logmsg(LOGMSG_FATAL, "failed to initialize osql_blkseq hash\n");
            return -1;
        }
    }

    if (!gbl_exit && !gbl_create_mode && (thedb->nsiblings == 1 || thedb->master == gbl_myhostname)) {
        bdb_upgrade_all_prepared(thedb->bdb_env);
    }

    sqlinit();
    rc = create_datacopy_arrays();
    if (rc) {
        logmsg(LOGMSG_FATAL, "create_datacopy_arrays rc %d\n", rc);
        return -1;
    }
    rc = create_sqlmaster_records(NULL);
    if (rc) {
        logmsg(LOGMSG_FATAL, "create_sqlmaster_records rc %d\n", rc);
        return -1;
    }
    create_sqlite_master(); /* create sql statements */

    if ((rc = resolve_sfuncs_for_db(thedb)) != 0) {
        logmsg(LOGMSG_FATAL, "resolve_sfuncs_for_db failed rc %d\n", rc);
        return -1;
    };

    load_auto_analyze_counters(); /* on starting, need to load counters */
    unlock_schema_lk();

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

        if (gbl_timepart_file_name) {
            if (timepart_apply_file(gbl_timepart_file_name)) {
                logmsg(LOGMSG_FATAL, "Failed to create time partitions!\n");
                return -1;
            }
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
        gbl_perform_full_clean_exit = 0;
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
        net_set_throttle_percent(thedb->handle_sibling, gbl_net_throttle_percent);
    }
    if (gbl_net_portmux_register_interval) {
        net_set_portmux_register_interval(thedb->handle_sibling, gbl_net_portmux_register_interval);
    }
    if (gbl_enque_flush_interval) {
        net_set_enque_flush_interval(thedb->handle_sibling, gbl_enque_flush_interval);
    }
    if (gbl_enque_reorder_lookahead) {
        net_set_enque_reorder_lookahead(thedb->handle_sibling, gbl_enque_reorder_lookahead);
    }
    if (gbl_net_poll) {
        net_set_poll(thedb->handle_sibling, gbl_net_poll);
    }
    if (gbl_heartbeat_send) {
        net_set_heartbeat_send_time(thedb->handle_sibling, gbl_heartbeat_send);
    }
    if (gbl_heartbeat_check) {
        net_set_heartbeat_check_time(thedb->handle_sibling, gbl_heartbeat_check);
    }

    net_setbufsz(thedb->handle_sibling, gbl_netbufsz);

    if (javasp_init_procedures() != 0) {
        logmsg(LOGMSG_FATAL, "*ERROR* cannot initialise Java stored procedures\n");
        return -1;
    }

    csc2_free_all();

    init_password_cache();

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
    switch (parm->parm) {
    case TMEV_ENABLE_LOG_DELETE:
        /* We already hold timerlk. process_sync_command("sync log-delete on")
           will attempt to grab timerlk one more time. So call the routine
           directly without faking a message trap. */
        log_delete_counter_change(thedb, LOG_DEL_ABS_ON);
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

    if (!dbenv->purge_old_files_is_running && !gbl_is_physical_replicant) {
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

double gbl_cpupercent;
#include <sc_util.h>


static inline void log_tbl_item(int curr, unsigned int *prev, const char *(*type_to_str)(int), int type, char *string,
                                int *hdr_p, struct reqlogger *statlogger, dbtable *tbl, int first) 
{
    int diff = curr - *prev;
    if (diff > 0) {
        if (*hdr_p == 0) {
            const char hdr_fmt[] = "DIFF REQUEST STATS FOR TABLE '%s'\n";
            reqlog_logf(statlogger, REQL_INFO, hdr_fmt, tbl->tablename);
            *hdr_p = 1;
        }
        reqlog_logf(statlogger, REQL_INFO, "%s%-22s %u\n", (first ? "" : "    "),
                    (type_to_str ? type_to_str(type) : string), diff);
    }
    *prev = curr;
}

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
    int64_t ndeadlocks = 0;
    int64_t nlocks_aborted = 0;
    int64_t nlockwaits = 0;
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
    int diff_locks_aborted;
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
    int64_t last_ndeadlocks = 0, last_nlocks_aborted = 0, last_nlockwaits = 0;
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

    int64_t conns=0, conn_timeouts=0, curr_conns = 0;
    int64_t last_conns=0, last_conn_timeouts=0, last_curr_conns=0; 
    int64_t diff_conns=0, diff_conn_timeouts=0, diff_curr_conns=0; 
    int64_t last_report_conns = 0;
    int64_t last_report_conn_timeouts = 0;
    int64_t last_report_curr_conns=0;

    struct reqlogger *statlogger = NULL;
    struct berkdb_thread_stats last_bdb_stats = {0};
    struct berkdb_thread_stats cur_bdb_stats;
    const struct berkdb_thread_stats *pstats;
    char lastlsn[63] = "", curlsn[64];
    uint64_t lastlsnbytes = 0, curlsnbytes;
    int ii;
    int jj;
    int thresh;
    int have_scon_header = 0;
    int have_scon_stats = 0;
    int64_t rw_evicts;


    thrman_register(THRTYPE_GENERIC);
    thread_started("statthd");

    dbenv = p;

    /* initialize */
    statlogger = reqlog_alloc();
    reqlog_diffstat_init(statlogger);

    while (!db_is_exiting()) {
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

        conns = net_get_num_accepts(thedb->handle_sibling);
        curr_conns = net_get_num_current_non_appsock_accepts(thedb->handle_sibling) + active_appsock_conns;
        conn_timeouts = net_get_num_accept_timeouts(thedb->handle_sibling);

        bdb_get_bpool_counters(thedb->bdb_env, (int64_t *)&bpool_hits,
                               (int64_t *)&bpool_misses, &rw_evicts);

        bdb_get_lock_counters(thedb->bdb_env, &ndeadlocks, &nlocks_aborted,
                              &nlockwaits, NULL);
        diff_deadlocks = ndeadlocks - last_ndeadlocks;
        diff_locks_aborted = nlocks_aborted - last_nlocks_aborted;
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
        diff_conns = conns - last_conns;
        diff_curr_conns = curr_conns - last_curr_conns;
        diff_conn_timeouts = conn_timeouts - last_conn_timeouts;

        last_qtrap = nqtrap;
        last_fstrap = nfstrap;
        last_nsql = nsql;
        last_nsql_steps = nsql_steps;
        last_newsql = newsql;
        last_nretries = nretries;
        last_ndeadlocks = ndeadlocks;
        last_nlocks_aborted = nlocks_aborted;
        last_nlockwaits = nlockwaits;
        last_vreplays = vreplays;
        last_ncommits = ncommits;
        last_ncommit_time = ncommit_time;
        last_bpool_hits = bpool_hits;
        last_bpool_misses = bpool_misses;
        last_conns = conns;
        last_curr_conns = curr_conns;
        last_conn_timeouts = conn_timeouts;

        have_scon_header = 0;
        have_scon_stats = 0;

        if (diff_qtrap || diff_nsql || diff_newsql || diff_nsql_steps ||
            diff_fstrap || diff_vreplays || diff_bpool_hits ||
            diff_bpool_misses || diff_ncommit_time ||
            diff_conns || diff_conn_timeouts || diff_curr_conns) {
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
                    logmsg(LOGMSG_USER, " cache_hits %" PRIu64,
                           diff_bpool_hits);
                if (diff_bpool_misses)
                    logmsg(LOGMSG_USER, " cache_misses %" PRIu64,
                           diff_bpool_misses);
                if (diff_conns)
                    logmsg(LOGMSG_USER, " connects %"PRId64, diff_conns);
                if (diff_curr_conns)
                    logmsg(LOGMSG_USER, " current_connects %"PRId64, diff_curr_conns);
                if (diff_conn_timeouts)
                    logmsg(LOGMSG_USER, " connect_timeouts %"PRId64, diff_conn_timeouts);
                have_scon_stats = 1;
            }
        }
        if (gbl_report)
            have_scon_stats |= osql_comm_diffstat(NULL, &have_scon_header);

        if (have_scon_stats)
            logmsg(LOGMSG_USER, "\n");

        if (count % gbl_update_metrics_interval == 0)
            update_metrics();

        if (!get_schema_change_in_progress(__func__, __LINE__)) {
            thresh = reqlog_diffstat_thresh();
            if ((thresh > 0) && (count >= thresh)) { /* every thresh-seconds */
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

                int aa_include_updates = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_COUNT_UPD);
                rdlock_schema_lk();
                for (ii = 0; ii < dbenv->num_dbs; ++ii) {
                    dbtable *tbl = dbenv->dbs[ii];
                    int hdr = 0;

                    for (jj = 0; jj <= MAXTYPCNT; jj++) {
                        log_tbl_item(tbl->typcnt[jj], &tbl->prev_typcnt[jj], req2a, 
                                     jj, NULL, &hdr, statlogger, tbl, 1);
                    }

                    for (jj = 0; jj < BLOCK_MAXOPCODE; jj++) {
                        log_tbl_item(tbl->blocktypcnt[jj], &tbl->prev_blocktypcnt[jj],
                                     breq2a, jj, NULL, &hdr, statlogger, tbl, 0);
                    }
                    for (jj = 0; jj < MAX_OSQL_TYPES; jj++) {
                        log_tbl_item(tbl->blockosqltypcnt[jj], &tbl->prev_blockosqltypcnt[jj],
                                     osql_reqtype_str, jj, NULL, &hdr, statlogger, tbl, 0);
                    }

                    log_tbl_item(dbenv->txns_committed, &dbenv->prev_txns_committed,
                                 NULL, 0, "txns committed", &hdr, statlogger, tbl, 0);

                    log_tbl_item(dbenv->txns_aborted, &dbenv->prev_txns_aborted, NULL,
                                 0, "txns aborted", &hdr, statlogger, tbl, 0);

                    log_tbl_item(tbl->nsql, &tbl->prev_nsql, NULL, 0, "nsql",
                                 &hdr, statlogger, tbl, 0);

                    // log write_count, save in saved_write_count, compute autoanalyze delta
                    unsigned prev = tbl->saved_write_count[RECORD_WRITE_DEL] +
                                    tbl->saved_write_count[RECORD_WRITE_INS];
                    unsigned curr = tbl->write_count[RECORD_WRITE_DEL] +
                                    tbl->write_count[RECORD_WRITE_INS];

                    if (aa_include_updates) {
                        prev += tbl->saved_write_count[RECORD_WRITE_UPD];
                        curr += tbl->write_count[RECORD_WRITE_UPD];
                    }

                    ATOMIC_ADD32(tbl->aa_saved_counter, (curr - prev));
                    log_tbl_item(tbl->write_count[RECORD_WRITE_INS], &tbl->saved_write_count[RECORD_WRITE_INS],
                                 NULL, 0, "inserted rows", &hdr, statlogger, tbl, 0);
                    log_tbl_item(tbl->write_count[RECORD_WRITE_UPD], &tbl->saved_write_count[RECORD_WRITE_UPD],
                                 NULL, 0, "updated rows", &hdr, statlogger, tbl, 0);
                    log_tbl_item(tbl->write_count[RECORD_WRITE_DEL], &tbl->saved_write_count[RECORD_WRITE_DEL],
                                 NULL, 0, "deleted rows", &hdr, statlogger, tbl, 0);
                    log_tbl_item(tbl->casc_write_count, &tbl->saved_casc_write_count,
                                 NULL, 0, "cascaded upd/del rows", &hdr, statlogger, tbl, 0);
                    log_tbl_item(tbl->deadlock_count, &tbl->saved_deadlock_count,
                                 NULL, 0, "deadlock count", &hdr, statlogger, tbl, 0);
                }
                unlock_schema_lk();

                pstats = bdb_get_process_stats();
                cur_bdb_stats = *pstats;
                if (cur_bdb_stats.n_lock_waits > last_bdb_stats.n_lock_waits) {
                    unsigned nwaits = cur_bdb_stats.n_lock_waits -
                                      last_bdb_stats.n_lock_waits;
                    reqlog_logf(statlogger, REQL_INFO,
                                "%u locks, avg time %ums, worst time %ums\n",
                                nwaits,
                                U2M(cur_bdb_stats.lock_wait_time_us -
                                    last_bdb_stats.lock_wait_time_us) /
                                    nwaits,
                                U2M(cur_bdb_stats.worst_lock_wait_time_us));
                    bb_berkdb_reset_worst_lock_wait_time_us();
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
                if (cur_bdb_stats.n_memp_fgets > last_bdb_stats.n_memp_fgets) {
                    unsigned n_memp_fgets = cur_bdb_stats.n_memp_fgets -
                                            last_bdb_stats.n_memp_fgets;
                    unsigned us = cur_bdb_stats.memp_fget_time_us -
                                  last_bdb_stats.memp_fget_time_us;
                    reqlog_logf(statlogger, REQL_INFO,
                                "n_memp_fgets=%u, memp_fgets avg time=%ums\n",
                                n_memp_fgets, U2M(us) / n_memp_fgets);
                }
                if (cur_bdb_stats.n_memp_pgs > last_bdb_stats.n_memp_pgs) {
                    unsigned n_memp_pgs =
                        cur_bdb_stats.n_memp_pgs - last_bdb_stats.n_memp_pgs;
                    unsigned us = cur_bdb_stats.memp_pg_time_us -
                                  last_bdb_stats.memp_pg_time_us;
                    reqlog_logf(statlogger, REQL_INFO,
                                "n_memp_pgs=%u, memp_pgs avg time=%ums\n",
                                n_memp_pgs, U2M(us) / n_memp_pgs);
                }
                last_bdb_stats = cur_bdb_stats;

                diff_deadlocks = ndeadlocks - last_report_deadlocks;
                diff_lockwaits = nlockwaits - last_report_lockwaits;
                diff_vreplays = vreplays - last_report_vreplays;

                if (diff_deadlocks || diff_lockwaits || diff_vreplays)
                    reqlog_logf(statlogger, REQL_INFO,
                                "ndeadlocks %d, nlockwaits %d, vreplays %d, "
                                "locks aborted %d\n",
                                diff_deadlocks, diff_lockwaits, diff_vreplays,
                                diff_locks_aborted);

                bdb_get_cur_lsn_str(thedb->bdb_env, &curlsnbytes, curlsn,
                                    sizeof(curlsn));
                if (strcmp(curlsn, lastlsn) != 0) {
                    reqlog_logf(statlogger, REQL_INFO, "LSN %s diff %llu\n",
                                curlsn, curlsnbytes - lastlsnbytes);
                    strncpy0(lastlsn, curlsn, sizeof(lastlsn));
                    lastlsnbytes = curlsnbytes;
                }

                if (conns - last_report_conns || curr_conns - last_report_curr_conns) {
                   reqlog_logf(statlogger, REQL_INFO, "connections %lld timeouts %lld current_connections %lld\n", 
                         conns - last_report_conns,
                         conn_timeouts - last_report_conn_timeouts,
                         curr_conns - last_report_curr_conns);
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

                last_report_conns = conns;
                last_report_conn_timeouts = conn_timeouts;
                last_report_curr_conns = curr_conns;

                osql_comm_diffstat(statlogger, NULL);
                strbuf_free(logstr);

                dump_client_sql_data(statlogger, 1);
            }
        }

        if (gbl_repscore)
            bdb_show_reptimes_compact(thedb->bdb_env);


        /* Push out old metrics */
        time_metric_purge_old(thedb->handle_buf_queue_time);
        time_metric_purge_old(thedb->sql_queue_time);
        time_metric_purge_old(thedb->service_time);
        time_metric_purge_old(thedb->queue_depth);
        time_metric_purge_old(thedb->concurrent_queries);
        time_metric_purge_old(thedb->connections);

        ++count;
        sleep(1);
    }

    reqlog_free(statlogger);
    return NULL;
}

void create_stat_thread(struct dbenv *dbenv)
{
    pthread_t stat_tid;
    Pthread_create(&stat_tid, &gbl_pthread_attr, statthd, dbenv);
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

/* Global cron job scheduler for time-insensitive, lightweight jobs. */
cron_sched_t *gbl_cron;
static void *memstat_cron_event(struct cron_event *_, struct errstat *err)
{
    int tm;
    void *rc;

    // cron jobs always write to ctrace
    (void)comdb2ma_stats(NULL, 0, 0, COMDB2MA_TOTAL_DESC, COMDB2MA_GRP_NONE, 1);

    if (gbl_memstat_freq > 0) {
        tm = comdb2_time_epoch() + gbl_memstat_freq;
        rc = cron_add_event(gbl_cron, NULL, tm, (FCRON)memstat_cron_event, NULL,
                            NULL, NULL, NULL, NULL, err, NULL);

        if (rc == NULL)
            logmsg(LOGMSG_ERROR, "Failed to schedule next memstat event. "
                            "rc = %d, errstr = %s\n",
                    err->errval, err->errstr);
    }
    return NULL;
}

static void *memstat_cron_kickoff(struct cron_event *_, struct errstat *err)
{

    int tm;
    void *rc;

    logmsg(LOGMSG_INFO, "Starting memstat cron job. "
                    "Will print memory usage every %d seconds.\n",
            gbl_memstat_freq);

    tm = comdb2_time_epoch() + gbl_memstat_freq;
    rc = cron_add_event(gbl_cron, NULL, tm, (FCRON)memstat_cron_event, NULL,
                        NULL, NULL, NULL, NULL, err, NULL);
    if (rc == NULL)
        logmsg(LOGMSG_ERROR, "Failed to schedule next memstat event. "
                        "rc = %d, errstr = %s\n",
                err->errval, err->errstr);

    return NULL;
}

static char *gbl_cron_describe(sched_if_t *impl)
{
    return strdup("Default cron scheduler");
}

static char *gbl_cron_event_describe(sched_if_t *impl, cron_event_t *event)
{
    const char *name;
    if (event->func == (FCRON)memstat_cron_event)
        name = "Module memory stats update";
    else if (event->func == (FCRON)memstat_cron_kickoff)
        name = "Module memory stats kickoff";
    else
        name = "Unknown";

    return strdup(name);
}

static int comdb2ma_stats_cron(void)
{
    struct errstat xerr = {0};

    if (gbl_memstat_freq > 0) {
        if (!gbl_cron) {
            sched_if_t impl = {0};
            time_cron_create(&impl, gbl_cron_describe, gbl_cron_event_describe);
            gbl_cron = cron_add_event(NULL, "Global Job Scheduler", INT_MIN,
                                      (FCRON)memstat_cron_kickoff, NULL, NULL,
                                      NULL, NULL, NULL, &xerr, &impl);

        } else {
            gbl_cron = cron_add_event(gbl_cron, NULL, INT_MIN,
                                      (FCRON)memstat_cron_kickoff, NULL, NULL,
                                      NULL, NULL, NULL, &xerr, NULL);
        }
        if (gbl_cron == NULL)
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
    register_int_switch("enable_osql_logging",
                        "Log every osql packet and operation",
                        &gbl_enable_osql_logging);
    register_int_switch("enable_osql_longreq_logging",
                        "Log untruncated osql strings",
                        &gbl_enable_osql_longreq_logging);
    register_int_switch(
        "check_sparse_files",
        "When allocating a page, check that we aren't creating a sparse file",
        &gbl_check_sparse_files);
    register_int_switch(
        "core_on_sparse_file",
        "Generate a core if we catch berkeley creating a sparse file",
        &gbl_core_on_sparse_file);
    register_int_switch(
        "check_sqlite_numeric_types",
        "Report if our numeric conversion disagrees with SQLite's",
        &gbl_report_sqlite_numeric_conversion_errors);
    register_int_switch(
        "use_fastseed_for_comdb2_seqno",
        "Use fastseed instead of context for comdb2_seqno unique values",
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
    register_int_switch(
        "large_str_idx_find",
        "Allow index search using out-of-range strings or bytearrays",
        &gbl_large_str_idx_find);
    register_int_switch("fingerprint_queries",
                        "Compute fingerprint for SQL queries",
                        &gbl_fingerprint_queries);
    register_int_switch("query_plans", "Keep track of query plans and their costs for each query", &gbl_query_plans);
    register_int_switch("verbose_normalized_queries",
                        "For new fingerprints, show normalized queries.",
                        &gbl_verbose_normalized_queries);
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
    register_int_switch(
        "logical_live_sc",
        "Enables online schema change with logical redo. (Default: OFF)",
        &gbl_logical_live_sc);
    register_int_switch("osql_odh_blob",
                        "Send ODH'd blobs to master. (Default: ON)",
                        &gbl_osql_odh_blob);
    register_int_switch("delay_sql_lock_release",
                        "Delay release locks in cursor move if bdb lock "
                        "desired but client sends rows back",
                        &gbl_delay_sql_lock_release_sec);
    register_int_switch("sqlite_makerecord_for_comdb2",
                        "Enable MakeRecord optimization which converts Mem to comdb2 row data directly",
                        &gbl_sqlite_makerecord_for_comdb2);
}

static void getmyid(void)
{
    int rc;
    char *cname = NULL;
    char buf[NI_MAXHOST];
    if ((rc = gethostname(buf, sizeof(buf))) != 0) {
        logmsg(LOGMSG_FATAL, "gethostname failed rc:%d err:%s\n", rc, strerror(errno));
        abort();
    }
    gbl_myhostname = intern(buf);
    get_os_hostbyname()(&gbl_myhostname, &gbl_myaddr , &cname); // -> os_get_host_and_cname_by_name
    if (cname) {
        gbl_mycname = intern(cname);
        free(cname);
    } else {
        gbl_mycname = gbl_myhostname;
    }
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
        thedb->master == gbl_myhostname) {
        int irc = resume_schema_change();
        if (irc)
            logmsg(LOGMSG_ERROR, 
                    "failed trying to resume schema change, "
                    "if one was in progress it will have to be restarted\n");
    }
}

static void goodbye()
{
    logmsg(LOGMSG_USER, "goodbye\n");
#ifndef NDEBUG //TODO:wrap the follwing lines before checking in
    if (gbl_perform_full_clean_exit) {
        char cmd[400];
        sprintf(cmd,
                "bash -c 'gdb --batch --eval-command=\"thr app all ba\" "
                "/proc/%d/exe %d 2>&1 > %s/logs/%s.atexit'",
                gbl_mypid, gbl_mypid, getenv("TESTDIR"), gbl_dbname);

        logmsg(LOGMSG_ERROR, "goodbye: running %s\n", cmd);
        int rc = system(cmd);
        if (rc) {
            logmsg(LOGMSG_ERROR, "goodbye: system  returned rc %d\n", rc);
        }
    }
#endif
}

#define TOOL(x) #x,

#define TOOLS           \
   TOOL(cdb2_dump)      \
   TOOL(cdb2_load)      \
   TOOL(cdb2_printlog)  \
   TOOL(cdb2_stat)      \
   TOOL(cdb2_verify)    \
   TOOL(cdb2_pgdump)

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
   {NULL, NULL}
};

#ifdef COMDB2_BBCMAKE
static void hash_no_op_callback(hash_t *const restrict hash, plhash_event_t event, const void *event_info)
{ }
#endif

int main(int argc, char **argv)
{
    int rc;

    char *exe = NULL;

    /* clean left over transactions every 5 minutes */
    int clean_mins = 5 * 60 * 1000;

    /* allocate initializer first */
    comdb2ma_init(0, 0);

#   ifdef COMDB2_BBCMAKE
    hash_set_global_event_callback(hash_no_op_callback);
#   endif

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

    init_peer_hash();

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

    if (gbl_clean_exit_on_sigterm)
        signal(SIGTERM, clean_exit_sigwrap);

    if (debug_switch_skip_skipables_on_verify())
        gbl_berkdb_verify_skip_skipables = 1;

    init_q_vars();

    srandom(comdb2_time_epochus());

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

    signal(SIGPIPE, SIG_IGN); /* do this before kicking off any threads */

    thrman_init();
    javasp_once_init();

    register_all_int_switches();
    repl_list_init();

    gbl_argc = argc;
    gbl_argv = argv;

    init_cron();

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

    init_lua_dbtypes();

    comdb2_timprm(clean_mins, TMEV_PURGE_OLD_LONGTRN);

    if (comdb2ma_stats_cron() != 0)
        abort();

    process_deferred_options(thedb, deferred_do_commands);
    clear_deferred_options();

    // db started - disable recsize kludge so
    // new schemachanges won't allow broken size.
    gbl_broken_max_rec_sz = 0;

    if (run_init_plugins(COMDB2_PLUGIN_INITIALIZER_POST)) {
        logmsg(LOGMSG_FATAL, "Initializer plugin failed\n");
        exit(1);
    }

    if (bdb_queuedb_create_cron(thedb) != 0)
        abort();

    /* Create DBA user if it does not already exist */
    if ((thedb->master == gbl_myhostname) && (gbl_create_dba_user == 1)) {
        /*
          Skip if authentication is enabled, as we do not want to open a
          backdoor by automatically creating an OP user with no password
          (the default DBA user attributes).
        */
        if (gbl_uses_password == 0) {
            bdb_create_dba_user(thedb->bdb_env);
        } else {
            logmsg(LOGMSG_USER, "authentication enabled, DBA user not created\n");
        }
    }

    logmsg(LOGMSG_USER, "hostname:%s  cname:%s\n", gbl_myhostname, gbl_mycname);
    logmsg(LOGMSG_USER, "I AM READY.\n");
    increase_net_buf();
    gbl_ready = 1;

    pthread_t timer_tid;
    pthread_attr_t timer_attr;
    Pthread_attr_init(&timer_attr);
    Pthread_attr_setstacksize(&timer_attr, DEFAULT_THD_STACKSZ);
    if (gbl_perform_full_clean_exit) {
        Pthread_attr_setdetachstate(&timer_attr, PTHREAD_CREATE_DETACHED);
    } else {
        Pthread_attr_setdetachstate(&timer_attr, PTHREAD_CREATE_JOINABLE);
    }
    Pthread_create(&timer_tid, &timer_attr, timer_thread, NULL);
    Pthread_attr_destroy(&timer_attr);

    start_physrep_threads();

    if (!gbl_perform_full_clean_exit) {
        void *ret;
        rc = pthread_join(timer_tid, &ret);
        if (rc) {
            logmsg(LOGMSG_FATAL, "Can't wait for timer thread %d %s\n", rc,
                    strerror(rc));
            return 1;
        }
        return 0;
    }

    int wait_counter = 0;
    while (!db_is_exiting()) {
        sleep(1);
        wait_counter++;
    }

    /* wait until THRTYPE_CLEANEXIT thread has exited
     * THRTYPE_CLEANEXIT threads calls begin_clean_exit() which in turn
     * will wait for all the generic threads to exit */
    thrman_wait_type_exit(THRTYPE_CLEANEXIT);
    finish_clean();

    for (int ii = 0; ii < thedb->num_dbs; ii++) {
        struct dbtable *db = thedb->dbs[ii];
        free(db->handle);
    }
    free(thedb);
    thedb = NULL;

    goodbye();
    return 0;
}

int add_dbtable_to_thedb_dbs(dbtable *table)
{
    Pthread_rwlock_wrlock(&thedb_lock);

    if (_db_hash_find(table->tablename) != 0) {
        Pthread_rwlock_unlock(&thedb_lock);
        return -1;
    }

    thedb->dbs = realloc(thedb->dbs, (thedb->num_dbs + 1) * sizeof(dbtable *));
    table->dbs_idx = thedb->num_dbs;
    thedb->dbs[thedb->num_dbs++] = table;

    /* Add table to the hash. */
    _db_hash_add(table);

    Pthread_rwlock_unlock(&thedb_lock);
    return 0;
}

void rem_dbtable_from_thedb_dbs(dbtable *table)
{
    Pthread_rwlock_wrlock(&thedb_lock);
    /* Remove the table from hash. */
    _db_hash_del(table);

    for (int i = table->dbs_idx; i < (thedb->num_dbs - 1); i++) {
        thedb->dbs[i] = thedb->dbs[i + 1];
        thedb->dbs[i]->dbs_idx = i;
    }

    thedb->num_dbs -= 1;
    thedb->dbs[thedb->num_dbs] = NULL;
    Pthread_rwlock_unlock(&thedb_lock);
}

static void add_sqlalias_db(dbtable *tbl, char *newname)
{
    char *name;

    Pthread_rwlock_wrlock(&thedb_lock);
    name = tbl->sqlaliasname;
    tbl->sqlaliasname = newname;
    hash_add(thedb->sqlalias_hash, tbl);
    Pthread_rwlock_unlock(&thedb_lock);

    free(name);
}

static void delete_sqlalias_db(dbtable *tbl)
{
    char *name;

    Pthread_rwlock_wrlock(&thedb_lock);
    name = tbl->sqlaliasname;
    if (name) {
        hash_del(thedb->sqlalias_hash, tbl);
        tbl->sqlaliasname = NULL;
    }
    Pthread_rwlock_unlock(&thedb_lock);

    free(name);
}

void hash_sqlalias_db(dbtable *tbl, const char *newname)
{
    if (strncasecmp(tbl->tablename, newname, MAXTABLELEN) == 0) {
        /* this is actually an alias removal */
        delete_sqlalias_db(tbl);
    } else {
        add_sqlalias_db(tbl, strdup(newname));
    }
}

/* rename in memory db names; fragile */
int rename_db(dbtable *db, const char *newname)
{
    char *tag_name = strdup(newname);
    char *bdb_name = strdup(newname);

    if (!tag_name || !bdb_name)
        return -1;

    Pthread_rwlock_wrlock(&thedb_lock);

    /* tags */
    rename_schema(db->tablename, tag_name);

    /* bdb_state */
    bdb_state_rename(db->handle, bdb_name);

    /* db */
    _db_hash_del(db);
    db->tablename = (char *)newname;
    _db_hash_add(db);

    Pthread_rwlock_unlock(&thedb_lock);
    return 0;
}

void replace_db_idx(dbtable *p_db, int idx)
{
    int move = 0;
    Pthread_rwlock_wrlock(&thedb_lock);

    if (idx < 0 || idx >= thedb->num_dbs ||
        strcasecmp(thedb->dbs[idx]->tablename, p_db->tablename) != 0) {
        thedb->dbs =
            realloc(thedb->dbs, (thedb->num_dbs + 1) * sizeof(dbtable *));
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
        _db_hash_add(p_db);
    }

    Pthread_rwlock_unlock(&thedb_lock);
}

struct dbview *get_view_by_name(const char *view_name)
{
    struct dbview *view;
    Pthread_rwlock_wrlock(&thedb_lock);
    view = hash_find_readonly(thedb->view_hash, &view_name);
    Pthread_rwlock_unlock(&thedb_lock);
    return view;
}

int count_views()
{
    int count = 0;
    Pthread_rwlock_wrlock(&thedb_lock);
    count = hash_get_num_entries(thedb->view_hash);
    Pthread_rwlock_unlock(&thedb_lock);
    return count;
}

int add_view(struct dbview *view)
{
    Pthread_rwlock_wrlock(&thedb_lock);

    if (hash_find_readonly(thedb->view_hash, &view->view_name) != 0) {
        Pthread_rwlock_unlock(&thedb_lock);
        return -1;
    }

    /* Add view to the hash. */
    hash_add(thedb->view_hash, view);

    Pthread_rwlock_unlock(&thedb_lock);
    return 0;
}

void delete_view(char *view_name)
{
    struct dbview *view;
    Pthread_rwlock_wrlock(&thedb_lock);

    view = hash_find_readonly(thedb->view_hash, &view_name);
    if (view) {
        /* Remove the view from hash. */
        hash_del(thedb->view_hash, view);

        free(view->view_name);
        free(view->view_def);
        free(view);
    }

    Pthread_rwlock_unlock(&thedb_lock);
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

int log_delete_is_stopped(void)
{
    int rc;
    struct dbenv *dbenv = thedb;
    Pthread_mutex_lock(&dbenv->log_delete_counter_mutex);
    rc = dbenv->log_delete_state_list.count;
    Pthread_mutex_unlock(&dbenv->log_delete_counter_mutex);
    return rc;
}

void log_delete_add_state(struct dbenv *dbenv, struct log_delete_state *state)
{
    Pthread_mutex_lock(&dbenv->log_delete_counter_mutex);
    listc_atl(&dbenv->log_delete_state_list, state);
    Pthread_mutex_unlock(&dbenv->log_delete_counter_mutex);
}

void log_delete_rem_state(struct dbenv *dbenv, struct log_delete_state *state)
{
    Pthread_mutex_lock(&dbenv->log_delete_counter_mutex);
    listc_rfl(&dbenv->log_delete_state_list, state);
    Pthread_mutex_unlock(&dbenv->log_delete_counter_mutex);
}

void log_delete_counter_change(struct dbenv *dbenv, int action)
{
    struct log_delete_state *pstate;
    int filenum;
    Pthread_mutex_lock(&dbenv->log_delete_counter_mutex);
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
    Pthread_mutex_unlock(&dbenv->log_delete_counter_mutex);
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

int comdb2_recovery_cleanup(void *dbenv, void *inlsn, int is_master)
{
    int commit_lsn_map = gbl_commit_lsn_map;
    int *file = &(((int *)(inlsn))[0]);
    int *offset = &(((int *)(inlsn))[1]);
    int rc;

    assert(*file >= 0 && *offset >= 0);
    logmsg(LOGMSG_INFO, "%s starting for [%d:%d] as %s\n", __func__, *file,
           *offset, is_master ? "MASTER" : "REPLICANT");

    if (commit_lsn_map) {
        rc = truncate_commit_lsn_map(thedb->bdb_env, *file);
    }
    rc = truncate_asof_pglogs(thedb->bdb_env, *file, *offset);

    logmsg(LOGMSG_INFO, "%s complete [%d:%d] rc=%d\n", __func__, *file, *offset,
           rc);
    return rc;
}

int comdb2_replicated_truncate(void *dbenv, void *inlsn, uint32_t flags)
{
    int *file = &(((int *)(inlsn))[0]);
    int *offset = &(((int *)(inlsn))[1]);
    int is_master = (flags & DB_REP_TRUNCATE_MASTER);
    int wait_seqnum = (flags & DB_REP_TRUNCATE_ONLINE);

    logmsg(LOGMSG_INFO, "%s starting for [%d:%d] as %s\n", __func__, *file,
           *offset, is_master ? "MASTER" : "REPLICANT");
    if (is_master && !gbl_is_physical_replicant) {
        /* We've asked the replicants to truncate their log files.  The master
         * incremented it's generation number before truncating.  The newmaster
         * message with the higher generation forces the replicants into
         * REP_VERIFY_MATCH */
        send_newmaster(thedb->bdb_env, wait_seqnum);
    }

    /* Run logical recovery */
    if (gbl_rowlocks)
        bdb_run_logical_recovery(thedb->bdb_env,
                                 is_master && !gbl_is_physical_replicant);

    logmsg(LOGMSG_INFO, "%s complete [%d:%d]\n", __func__, *file, *offset);

    return 0;
}

int gbl_comdb2_reload_schemas = 0;

/* This is for online logfile truncation across a schema-change */
int comdb2_reload_schemas(void *dbenv, void *inlsn)
{
    extern int gbl_watcher_thread_ran;
    uint64_t format;
    int bdberr = 0;
    int rlstate;
    int rc;
    int ii;
    int stripes, blobstripe;
    int retries = 0;
    tran_type *tran;
    dbtable *db;
    struct sql_thread *sqlthd;
    struct sqlthdstate *thd;
    int *file = &(((int *)(inlsn))[0]);
    int *offset = &(((int *)(inlsn))[1]);

    if (thedb->bdb_env == NULL) {
        logmsg(LOGMSG_INFO, "%s backend not open, returning\n", __func__);
        return 0;
    }

    gbl_comdb2_reload_schemas = 1;
    logmsg(LOGMSG_INFO, "%s starting for [%d:%d]\n", __func__, *file, *offset);
    wrlock_schema_lk();
retry_tran:
    tran = bdb_tran_begin_flags(thedb->bdb_env, NULL, &bdberr, BDB_TRAN_NOLOG);
    if (tran == NULL) {
        logmsg(LOGMSG_FATAL, "%s: failed to start tran\n", __func__);
        abort();
    }

    for (ii = 0; ii < thedb->num_dbs; ii++) {
        db = thedb->dbs[ii];
        if ((rc = bdb_lock_table_write(db->handle, tran)) != 0) {
            if (rc == BDBERR_DEADLOCK && retries < 10) {
                retries++;
                bdb_tran_abort(thedb->bdb_env, tran, &bdberr);
                logmsg(LOGMSG_ERROR, "%s: got deadlock acquiring tablelocks\n",
                       __func__);
                goto retry_tran;
            } else {
                logmsg(LOGMSG_FATAL, "%s: got error %d acquiring tablelocks\n",
                       __func__, rc);
                abort();
            }
        }
    }

    /* Test this incrementally with all schema-change types */
    if ((rc = close_all_dbs_tran(tran)) != 0) {
        logmsg(LOGMSG_FATAL, "%s: close_all_dbs_tran returns %d\n", __func__,
               rc);
        abort();
    }

    free_dbtables(thedb);
    thedb->dbs = NULL;

    if (thedb->db_hash)
        hash_clear(thedb->db_hash);
    if (thedb->sqlalias_hash)
        hash_clear(thedb->sqlalias_hash);

    if (bdb_get_global_stripe_info(tran, &stripes, &blobstripe, &bdberr) == 0 &&
        stripes > 0) {
        gbl_dtastripe = stripes;
        gbl_blobstripe = blobstripe;
        bdb_attr_set(thedb->bdb_attr, BDB_ATTR_DTASTRIPE, gbl_dtastripe);
        bdb_attr_set(thedb->bdb_attr, BDB_ATTR_BLOBSTRIPE, gbl_blobstripe);
    }

    if (llmeta_load_tables(thedb, tran)) {
        logmsg(LOGMSG_FATAL, "could not load tables from the low level meta "
                             "table\n");
        abort();
    }

    if (llmeta_load_timepart(thedb)) {
        logmsg(LOGMSG_ERROR, "could not load time partitions\n");
        abort();
    }

    if ((rc = bdb_get_rowlocks_state(&rlstate, tran, &bdberr)) != 0) {
        logmsg(LOGMSG_ERROR, "Get rowlocks llmeta failed, rc=%d bdberr=%d\n",
               rc, bdberr);
        abort();
    }

    switch (rlstate) {
    case LLMETA_ROWLOCKS_ENABLED:
    case LLMETA_ROWLOCKS_ENABLED_MASTER_ONLY:
        gbl_rowlocks = 1;
        gbl_sql_tranlevel_default = SQL_TDEF_SNAPISOL;
    case LLMETA_ROWLOCKS_DISABLED:
        gbl_rowlocks = 0;
        gbl_sql_tranlevel_default = gbl_sql_tranlevel_preserved;
    default:
        break;
    }

    bdb_get_genid_format(&format, &bdberr);
    bdb_genid_set_format(thedb->bdb_env, format);

    if (reload_lua_sfuncs()) {
        logmsg(LOGMSG_FATAL, "could not load lua funcs from llmeta\n");
        abort();
    }

    if (reload_lua_afuncs()) {
        logmsg(LOGMSG_FATAL, "could not load lua aggs from llmeta\n");
        abort();
    }

    if ((rc = db_finalize_and_sanity_checks(thedb)) != 0) {
        logmsg(LOGMSG_FATAL, "%s: db_finalize_and_sanity_checks returns %d\n",
               __func__, rc);
        abort();
    }

    fix_lrl_ixlen_tran(tran);

    if ((rc = backend_open_tran(thedb, tran, 0)) != 0) {
        logmsg(LOGMSG_FATAL, "%s: backend_open_tran returns %d\n", __func__,
               rc);
        abort();
    }

    if (llmeta_load_tables_older_versions(thedb, tran)) {
        logmsg(LOGMSG_FATAL, "llmeta_load_tables_older_versions failed\n");
        abort();
    }

    load_dbstore_tableversion(thedb, tran);

    /* Clean up */
    sqlthd = pthread_getspecific(query_info_key);
    if (sqlthd) {
        thd = sqlthd->clnt->thd;
        stmt_cache_reset(thd->stmt_cache);
        sqlite3_close_serial(&thd->sqldb);

        /* Also clear sqlitex's db handle */
        if (gbl_old_column_names && query_preparer_plugin &&
            query_preparer_plugin->do_cleanup_thd) {
            query_preparer_plugin->do_cleanup_thd(thd);
        }
    }

    create_datacopy_arrays();
    create_sqlmaster_records(tran);
    create_sqlite_master();
    oldfile_clear();

    BDB_BUMP_DBOPEN_GEN(invalid, NULL);

    if ((rc = bdb_tran_commit(thedb->bdb_env, tran, &bdberr)) != 0) {
        logmsg(LOGMSG_FATAL, "%s: bdb_tran_commit returns %d\n", __func__, rc);
        bdb_flush(thedb->bdb_env, &bdberr);
        abort();
    }

    gbl_watcher_thread_ran = comdb2_time_epoch();
    unlock_schema_lk();
    logmsg(LOGMSG_INFO, "%s complete [%d:%d]\n", __func__, *file, *offset);

    gbl_comdb2_reload_schemas = 0;
    return 0;
}

int comdb2_is_standalone(void *dbenv)
{
    return bdb_is_standalone(dbenv, thedb->bdb_env);
}

const char *comdb2_get_dbname(void)
{
    return thedb->envname;
}

int backend_opened(void)
{
    return gbl_backend_opened;
}

int sc_ready(void)
{
    return backend_opened();
}

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
