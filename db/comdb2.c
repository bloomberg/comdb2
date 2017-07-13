/*
   Copyright 2015, 2017, Bloomberg Finance L.P.

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
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <netdb.h>
#include <poll.h>
#include <pwd.h>
#include <getopt.h>
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
/* temporarily pull in a local copy of comdb2_shm.h until it's in the libraries
 */
#include "comdb2_shm.h"
#include "sql.h"

#include "comdb2_trn_intrl.h"
#include "history.h"
#include "tag.h"
#include "types.h"
#include "timer.h"
#include <plhash.h>
#include <dynschemaload.h>
#include "translistener.h"
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
#include <sqllog.h>

#include <autoanalyze.h>
#include <sqlglue.h>

#include "dbdest.h"
#include "intern_strings.h"
#include "bb_oscompat.h"
#include "comdb2util.h"
#include "comdb2uuid.h"
#include "plugin.h"

#include "debug_switches.h"
#include "machine.h"
#include "eventlog.h"

#define COMDB2_ERRSTAT_ENABLED() 1
#define COMDB2_DIFFSTAT_REPORT() 1
#define COMDB2_USE_DEFAULT_CACHESZ() 1
#define COMDB2_OFFLOADSQL_ENABLED() (gbl_enable_osql)
#define COMDB2_RECOM_ENABLED() 1
#define COMDB2_SNAPISOL_ENABLED() 1
#define COMDB2_SERIAL_ENABLED() 1
#define COMDB2_SOCK_FSTSND_ENABLED() (gbl_enable_sock_fstsnd == 1)
#include "views.h"

#include <autoanalyze.h>
#include <cdb2_constants.h>
#include <bb_oscompat.h>
#include <schemachange.h>

#define tokdup strndup

static inline int qtrap_lock(pthread_mutex_t *x)
{
    return pthread_mutex_lock(x);
}
static inline int qtrap_unlock(pthread_mutex_t *x)
{
    return pthread_mutex_unlock(x);
}

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
int gbl_master_changed_oldfiles = 0;
int gbl_use_bbipc_global_fastseed = 0;
int gbl_recovery_timestamp = 0;
int gbl_recovery_lsn_file = 0;
int gbl_recovery_lsn_offset = 0;
int gbl_trace_prepare_errors = 0;
int gbl_trigger_timepart = 0;
int gbl_extended_sql_debug_trace = 0;
extern int gbl_dump_fsql_response;
static char *gbl_pidfile = NULL;

void myctrace(const char *c) { ctrace("%s", c); }

void berkdb_use_malloc_for_regions_with_callbacks(void *mem,
                                                  void *(*alloc)(void *, int),
                                                  void (*free)(void *, void *));

/* some random prototypes that should have their own header */
void buffer_origin_(int *mch, int *pid, int *slot);
void swapinit_(int *, int *);
int set_db_rngkeymode(int dbnum);
int set_db_rngextmode(int dbnum);
void enable_ack_trace(void);
void disable_ack_trace(void);
void set_cursor_rowlocks(int cr);
void walkback_set_warnthresh(int thresh);
void walkback_disable(void);
void walkback_enable(void);

static int put_all_csc2();

static void *purge_old_blkseq_thread(void *arg);
static void *purge_old_files_thread(void *arg);
static int lrltokignore(char *tok, int ltok);
static int lrllinecmp(char *lrlline, char *cmpto);
static void ttrap(struct timer_parm *parm);
int clear_temp_tables(void);
static int write_pidfile(const char *pidfile);

int q_reqs_len(void);
int handle_buf_bbipc(struct dbenv *, uint8_t *p_buf, const uint8_t *p_buf_end,
                     int debug, int frommach, int do_inline);

pthread_key_t comdb2_open_key;
pthread_key_t blockproc_retry_key;

/*---GLOBAL SETTINGS---*/
const char *const gbl_db_release_name = "R7.0pre";
int gbl_enque_flush_interval;
int gbl_enque_flush_interval_signal;
int gbl_enque_reorder_lookahead = 20;
int gbl_morecolumns = 0;
int gbl_return_long_column_names = 1;
int gbl_maxreclen;
int gbl_penaltyincpercent = 20;
int gbl_maxwthreadpenalty;
int gbl_spstrictassignments = 0;
int gbl_delayed_ondisk_tempdbs = 0;
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
int gbl_abort_on_missing_session = 0;

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
int gbl_net_max_queue_signal = 100;
int gbl_net_poll = 100;
int gbl_net_throttle_percent = 50;
int gbl_osql_net_poll = 100;
int gbl_osql_max_queue = 10000;
int gbl_osql_net_portmux_register_interval = 600;
int gbl_signal_net_portmux_register_interval = 600;
int gbl_net_portmux_register_interval = 600;

int gbl_max_sqlcache = 10;
int gbl_new_row_data = 0;
int gbl_extended_tm_from_sql =
    0; /* Keep a count of our extended-tm requests from sql. */

int gbl_upgrade_blocksql_to_socksql = 0;

int gbl_upgrade_blocksql_2_socksql =
    1; /* this is set if blocksock is in any parsed lrl
          files; if any blocksql requests will actually
          by socksql */

int gbl_serialise_sqlite3_open = 1;

int gbl_notimeouts = 0; /* set this if you don't need the server timeouts
                           (use this for new code testing) */

int gbl_nullfkey = 0;

/* Default fast sql timeouts */
int gbl_sqlrdtimeoutms = 10000;
int gbl_sqlwrtimeoutms = 10000;

int gbl_sql_client_stats = 1;

long long gbl_converted_blocksql_requests = 0;

int gbl_rangextunit =
    16; /* dont do more than 16 records in a single rangext op */
int gbl_honor_rangextunit_for_old_apis = 0;

/* various roles for the prefault helper threads.  */
int gbl_prefaulthelper_blockops = 1;
int gbl_prefaulthelper_sqlreadahead = 1;
int gbl_prefaulthelper_tagreadahead = 1;

/* this many "next" ops trigger a readahead */
int gbl_readaheadthresh = 0;
int gbl_sqlreadaheadthresh = 0;

/* readahead this many records */
int gbl_readahead = 0;
int gbl_sqlreadahead = 0;

int gbl_iothreads = 0;
int gbl_ioqueue = 0;
int gbl_prefaulthelperthreads = 0;
int gbl_osqlpfault_threads = 0;
int gbl_prefault_udp = 0;
__thread int send_prefault_udp = 0;

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
int gbl_nogbllrl = 0;                /* don't load /bb/bin/comdb2*.lrl */
int gbl_local_mode = 0;                /* local mode, no siblings */
int gbl_fullrecovery = 0;              /* backend full-recovery mode*/
int gbl_maxretries = 500;              /* thats a lotta retries */
int gbl_maxblobretries =
    0; /* everyone assures me this can't happen unless the data is corrupt */
int gbl_maxcontextskips = 10000; /* that's a whole whale of a lotta retries */
char gbl_cwd[256];               /* start directory */
int gbl_heartbeat_check = 0, gbl_heartbeat_send = 0, gbl_decom = 0;
int gbl_heartbeat_check_signal = 0, gbl_heartbeat_send_signal = 0;
int gbl_netbufsz = 1 * 1024 * 1024;
int gbl_netbufsz_signal = 64 * 1024;
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
int gbl_use_bbipc = 1; /* on by default. lrl option disable_bbipc to turn off */
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

volatile int gbl_dbopen_gen = 0;
volatile int gbl_analyze_gen = 0;
volatile int gbl_views_gen = 0;
int gbl_sqlhistsz = 25;
int gbl_force_bad_directory = 1;
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

int gbl_updategenids = 0;

int gbl_osql_heartbeat_send = 5, gbl_osql_heartbeat_alert = 7;

int gbl_chkpoint_alarm_time = 60;
int gbl_dump_queues_on_exit = 1;
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
int gbl_sql_tranlevel_sosql_pref = 1; /* set this to 1 if everytime the
                                       * client mentions blocksql, it
                                       * means sosql; this does not switch
                                       * all the users to sosql */

int gbl_test_blkseq_replay_code = 0;
int gbl_dump_blkseq = 0;
int gbl_test_curtran_change_code = 0;
int gbl_enable_block_offload = 0;
int gbl_enable_pageorder_trace = 0;
int gbl_disable_deadlock_trace = 1;
int gbl_disable_overflow_page_trace = 1;
int gbl_debug_rowlocks = 1; /* Default this to 0 if you see it */
int gbl_simulate_rowlock_deadlock_interval = 0;
int gbl_enable_berkdb_retry_deadlock_bias = 0;
int gbl_enable_cache_internal_nodes = 1;
int gbl_use_appsock_as_sqlthread = 0;
int gbl_rep_collect_txn_time = 0;
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

/* block/offload sql */
int gbl_enable_sock_fstsnd = 1;
#if 0
u_int gbl_blk_pq_shmkey = 0;
#endif
int gbl_enable_position_apis = 0;
int gbl_enable_sql_stmt_caching = 0;

int gbl_round_robin_stripes = 0;
int gbl_num_record_converts = 100;

int gbl_rrenablecountchanges = 0;

int gbl_debug_log_twophase = 0;
int gbl_debug_log_twophase_transactions = 0;

int gbl_use_llmeta = 1; /* use low level meta table; this is required in this
                         * version of comdb2 */

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
extern int gbl_pmux_route_enabled;
extern int gbl_skip_cget_in_db_put;

int gbl_stop_thds_time = 0;
int gbl_stop_thds_time_threshold = 60;
pthread_mutex_t stop_thds_time_lk = PTHREAD_MUTEX_INITIALIZER;

int gbl_disallow_null_blobs = 1;
int gbl_force_notnull_static_tag_blobs = 1;
int gbl_enable_good_sql_return_codes = 0;
int gbl_key_updates = 1;

int gbl_partial_indexes = 1;
int gbl_expressions_indexes = 1;
int gbl_new_indexes = 0;

int gbl_optimize_truncate_repdb = 1;

extern void tz_hash_init(void);
static void set_datetime_dir(void);
void tz_set_dir(char *dir);
extern void init_sql_hint_table();
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

int gbl_fkrcode = 1;
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

int gbl_bbipc_slotidx;

int gbl_sql_use_random_readnode = 0;
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
int gbl_catch_response_on_retry = 1;
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
static int skip_clear_queue_extents = 0;

int gbl_flush_check_active_peer = 1;

int gbl_ctrace_dbdir = 0;
int gbl_inflate_log = 0;

int gbl_skip_llmeta_progress_updates_on_schema_change = 0;
int gbl_sc_inco_chk = 1;
int gbl_track_queue_time = 1;
int gbl_locks_check_waiters = 1;
int gbl_update_startlsn_printstep = 0;
int gbl_rowlocks_commit_on_waiters = 0;
int gbl_rowlocks_deadlock_trace = 0;

int gbl_durable_wait_seqnum_test = 0;
int gbl_durable_replay_test = 0;
int gbl_durable_block_test = 0;
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

char *gbl_dbdir = NULL;

extern int gbl_verbose_net;

static int create_service_file(char *lrlname);

static const char *help_text = {
  "usage: comdb2 [--lrl LRLFILE] [--recovertotime EPOCH]\n"
  "              [--recovertolsn FILE:OFFSET]\n"
  "              [--fullrecovery] NAME\n"
  "\n"
  "       comdb2 --create [--lrl LRLFILE] [--dir PATH] NAME\n"
  "\n"
  "        --lrl                      specify alternate lrl file\n"
  "        --fullrecovery             runs full recovery after a hot copy\n"
  "        --recovertolsn             recovers database to file:offset\n"
  "        --recovertotime            recovers database to epochtime\n"
  "        --create                   creates a new database\n"
  "        --dir                      specify path to database directory\n"
  "\n"
  "        NAME                       database name\n"
  "        LRLFILE                    lrl configuration file\n"
  "        FILE                       ID of a database file\n"
  "        OFFSET                     offset within FILE\n"
  "        EPOCH                      time in seconds since 1970\n"
  "        PATH                       path to database directory\n"
};

/* FOR PAGE COMPACTION.
   The threshold should be kept under 0.5. By default, we make it lg(2)/2
   (see comment in bdb/rep.c to learn more about expected node utilization). */
/* Disabling for the time being */
double gbl_pg_compact_thresh = 0;
int gbl_pg_compact_latency_ms = 0;
int gbl_large_str_idx_find = 1;

extern int gbl_allow_user_schema;
extern int gbl_uses_password;

extern int gbl_direct_count;
extern int gbl_parallel_count;
extern int gbl_debug_sqlthd_failures;
extern int gbl_random_get_curtran_failures;
extern int gbl_abort_invalid_query_info_key;

int gbl_bbenv;

/* 040407dh: sys_nerr and sys_errlist are deprecated but still
   in use in util.c and ../berkdb/4.2.52/clib/strerror.c
   Not available in SUN 64 bits, add them here
   TODO:revise this hack
*/
#ifdef _IBM_SOURCE
#ifdef BB64BIT

int sys_nerr = -1; /* this will prevent accessing the sys_errlist */
char *sys_errlist[1] = {0};
#endif
#endif

static void usage(void)
{
    logmsg(LOGMSG_USER, "%s\n", help_text);
    exit(1);
}

int getkeyrecnums(const struct db *db, int ixnum)
{
    if (ixnum < 0 || ixnum >= db->nix)
        return -1;
    return db->ix_recnums[ixnum] != 0;
}
int getkeysize(const struct db *db, int ixnum)
{
    if (ixnum < 0 || ixnum >= db->nix)
        return -1;
    return db->ix_keylen[ixnum];
}

int getdatsize(const struct db *db) { return db->lrl; }

/*lookup dbs..*/
struct db *getdbbynum(int num)
{
    int ii;
    struct db *p_db = NULL;
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
    struct db *db;
    db = hash_find_readonly(thedb->db_hash, &p_name);
    return (db) ? db->dbs_idx : -1;
}

struct db *getdbbyname(const char *p_name)
{
    struct db *p_db = NULL;

    pthread_rwlock_rdlock(&thedb_lock);
    p_db = hash_find_readonly(thedb->db_hash, &p_name);
    pthread_rwlock_unlock(&thedb_lock);

    return p_db;
}

struct db *getqueuebyname(const char *name)
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
    struct db *usedb;
    logmsg(LOGMSG_USER, "-----\n");
    for (jj = 0; jj < dbenv->num_dbs; jj++) {
        usedb = dbenv->dbs[jj]; /*de-stink*/
        logmsg(LOGMSG_USER, "table '%s' comdbg compat dbnum %d\ndir '%s' lrlfile '%s' "
               "nconns %d  nrevconns %d\n",
               usedb->dbname, usedb->dbnum, dbenv->basedir,
               (usedb->lrlfname) ? usedb->lrlfname : "NULL",
               usedb->n_constraints, usedb->n_rev_constraints);
       logmsg(LOGMSG_ERROR, "   data reclen %-3d bytes\n", usedb->lrl);

        for (ii = 0; ii < usedb->nix; ii++) {
            logmsg(LOGMSG_USER, "   index %-2d keylen %-3d bytes  dupes? %c recnums? %c\n",
                   ii, usedb->ix_keylen[ii], (usedb->ix_dupes[ii] ? 'y' : 'n'),
                   (usedb->ix_recnums[ii] ? 'y' : 'n'));
        }
    }
    for (ii = 0; ii < dbenv->nsiblings; ii++) {
        logmsg(LOGMSG_USER, "sibling %-2d host %s:%d\n", ii, dbenv->sibling_hostname[ii],
               dbenv->sibling_port[ii]);
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

#ifdef DEBUG
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
#endif

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

    while (1) {
        sleep(1);

        /* Check del unused files progress about twice per threshold  */
        if (!(loop % (gbl_sc_del_unused_files_threshold_ms /
                      (2 * 1000 /*ms per sec*/))))
            sc_del_unused_files_check_progress();

        if (loop == 3600)
            loop = 0;
        else
            ++loop;

        if (dbenv->exiting || dbenv->stopped) {
            dbenv->purge_old_blkseq_is_running = 0;
            backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDONLY);
            return NULL;
        }

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
            int now = time_epoch();

            bdb_get_notcoherent_list(dbenv->bdb_env, incoh_list, REPMAX,
                                     &num_incoh, &since_epoch);

            if (num_incoh > 0) {
                int online_count, ii;
                int duration = time_epoch() - since_epoch;

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
    }
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

    while (!dbenv->exiting) {
        /* even though we only add files to be deleted on the master,
         * don't try to delete files, ever, if you're a replicant */
        if (thedb->master != gbl_mynode) {
            sleep(empty_pause);
            continue;
        }

        if (!bdb_have_unused_files() || dbenv->stopped) {
            sleep(empty_pause);
            continue;
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
            rc = trans_abort(&iq, trans);
            sleep(empty_pause);
            continue;
        }

        if (empty && gbl_master_changed_oldfiles) {
            gbl_master_changed_oldfiles = 0;
            if ((rc = bdb_list_unused_files(dbenv->bdb_env, &bdberr,
                                            "purge_old_files_thread"))) {
                logmsg(LOGMSG_ERROR, "%s: bdb_list_unused_files failed with rc=%d\n",
                        __func__, rc);
                sleep(empty_pause);
                continue;
            }
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
            char filepath[256];
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

void clean_exit(void)
{
    int rc, ii;
    char *indicator_file;
    int fd;

    thedb->exiting = 1;
    stop_threads(thedb);

    logmsg(LOGMSG_INFO, "stopping db engine...\n");
    rc = backend_close(thedb);
    if (rc != 0)
       logmsg(LOGMSG_ERROR, "error backend_close() rc %d\n", rc);

    logmsg(LOGMSG_WARN, "goodbye\n");

    if (COMDB2_SOCK_FSTSND_ENABLED()) {
        comdb2_shm_clr_flag(thedb->dbnum, CMDB2_SHMFLG_SOCK_FSTSND);
    }

    for (ii = 0; ii < thedb->num_dbs; ii++) {
        if (thedb->dbs[ii]->dbnum) {
            if (COMDB2_SOCK_FSTSND_ENABLED()) {
                comdb2_shm_clr_flag(thedb->dbs[ii]->dbnum,
                                    CMDB2_SHMFLG_SOCK_FSTSND);
            }

            indicator_file =
                comdb2_location("marker", "%s.done", thedb->dbs[ii]->dbname);
            fd = creat(indicator_file, 0666);
            if (fd != -1)
                close(fd);
            free(indicator_file);
        }
    }

    eventlog_stop();

    indicator_file = comdb2_location("marker", "%s.done", thedb->envname);
    fd = creat(indicator_file, 0666);
    if (fd != -1)
        close(fd);
    logmsg(LOGMSG_INFO, "creating %s\n", indicator_file);
    free(indicator_file);

#ifdef DEBUG
    sleep(4); // wait for other threads to exit by themselves

    backend_cleanup(thedb);
    net_cleanup_subnets();
    cleanup_q_vars();
    cleanup_switches();
#endif

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

/* defines lrl directives that get ignored by comdb2 */
static int lrltokignore(char *tok, int ltok)
{
    /* used by comdb2backup script */
    if (tokcmp(tok, ltok, "backup") == 0)
        return 0;
    /* reserved for use by cmdb2filechk script */
    if (tokcmp(tok, ltok, "filechkopts") == 0)
        return 0;
    /*not a reserved token */
    return 1;
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

/* handles "if"'s, returns 1 if this isn't an "if" statement or if the statement
 * is true, 0 if it is false (ie if this line should be skipped)
 * this replaces a couple duplicate sections of code */
static int lrl_if(char **tok_inout, char *line, int line_len, int *st,
                  int *ltok)
{
    char *tok = *tok_inout;
    if (tokcmp(tok, *ltok, "if") == 0) {
        enum mach_class my_class = get_my_mach_class();
        tok = segtok(line, line_len, st, ltok);
        if (my_class == CLASS_TEST && tokcmp(tok, *ltok, "test") &&
            tokcmp(tok, *ltok, "dev"))
            return 0;
        if (my_class == CLASS_ALPHA && tokcmp(tok, *ltok, "alpha"))
            return 0;
        if (my_class == CLASS_UAT && tokcmp(tok, *ltok, "uat"))
            return 0;
        if (my_class == CLASS_BETA && tokcmp(tok, *ltok, "beta"))
            return 0;
        if (my_class == CLASS_PROD && tokcmp(tok, *ltok, "prod"))
            return 0;
        if (my_class == CLASS_UNKNOWN)
            return 0;

        tok = segtok(line, line_len, st, ltok);
        *tok_inout = tok;
    }

    return 1; /* there was no "if" statement or it was true */
}

void set_sbuftimeout(int timeout)
{
    bdb_attr_set(thedb->bdb_attr, BDB_ATTR_SBUFTIMEOUT, timeout);
}

struct db *newqdb(struct dbenv *env, const char *name, int avgsz, int pagesize,
                  int isqueuedb)
{
    struct db *db;
    int rc;

    db = calloc(1, sizeof(struct db));
    db->dbname = strdup(name);
    db->dbenv = env;
    db->is_readonly = 0;
    db->dbtype = isqueuedb ? DBTYPE_QUEUEDB : DBTYPE_QUEUE;
    db->avgitemsz = avgsz;
    db->queue_pagesize_override = pagesize;

    if (db->dbtype == DBTYPE_QUEUEDB) {
        rc = pthread_rwlock_init(&db->consumer_lk, NULL);
        if (rc) {
            logmsg(LOGMSG_ERROR, "create consumer rwlock rc %d %s\n", rc,
                    strerror(rc));
            return NULL;
        }
    }

    return db;
}

void cleanup_newdb(struct db *db)
{
    if (!db)
        return;

    if (db->dbname) {
        free(db->dbname);
        db->dbname = NULL;
    }

    if (db->lrlfname) {
        free(db->lrlfname);
        db->lrlfname = NULL;
    }

    if (db->ixuse) {
        free(db->ixuse);
        db->ixuse = NULL;
    }
    if (db->sqlixuse) {
        free(db->sqlixuse);
        db->sqlixuse = NULL;
    }
    free(db);
    db = NULL;
}

struct db *newdb_from_schema(struct dbenv *env, char *tblname, char *fname,
                             int dbnum, int dbix, int is_foreign)
{
    struct db *db;
    int ii;
    int tmpidxsz;
    int rc;

    db = calloc(1, sizeof(struct db));
    if (db == NULL) {
        logmsg(LOGMSG_FATAL, "%s: Memory allocation error\n", __func__);
        return NULL;
    }

    db->dbs_idx = dbix;

    db->dbtype = DBTYPE_TAGGED_TABLE;
    if (fname)
        db->lrlfname = strdup(fname);
    db->dbname = strdup(tblname);
    db->dbenv = env;
    db->is_readonly = 0;
    db->dbnum = dbnum;
    db->lrl = dyns_get_db_table_size(); /* this gets adjusted later */
    if (dbnum == 0) {
        /* if no dbnumber then no default tag is required ergo lrl can be 0 */
        if (db->lrl < 0)
            db->lrl = 0;
        else if (db->lrl > MAXLRL) {
            logmsg(LOGMSG_ERROR, "bad data lrl %d in csc schema %s\n", db->lrl,
                    tblname);
            cleanup_newdb(db);
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
            cleanup_newdb(db);
            return NULL;
        }

        if (db->lrl < 1 || db->lrl > MAXLRL) {
            logmsg(LOGMSG_ERROR, "bad data lrl %d in csc schema %s\n", db->lrl,
                    tblname);
            cleanup_newdb(db);
            return NULL;
        }
    }
    db->nix = dyns_get_idx_count();
    if (db->nix > MAXINDEX) {
        logmsg(LOGMSG_ERROR, "too many indices %d in csc schema %s\n", db->nix,
                tblname);
        cleanup_newdb(db);
        return NULL;
    }
    if (db->nix < 0) {
        logmsg(LOGMSG_ERROR, "too few indices %d in csc schema %s\n", db->nix,
                tblname);
        cleanup_newdb(db);
        return NULL;
    }
    for (ii = 0; ii < db->nix; ii++) {
        tmpidxsz = dyns_get_idx_size(ii);
        if (tmpidxsz < 1 || tmpidxsz > MAXKEYLEN) {
            logmsg(LOGMSG_ERROR, "index %d bad keylen %d in csc schema %s\n", ii,
                    tmpidxsz, tblname);
            cleanup_newdb(db);
            return NULL;
        }
        db->ix_keylen[ii] = tmpidxsz; /* ix lengths are adjusted later */

        db->ix_dupes[ii] = dyns_is_idx_dup(ii);
        if (db->ix_dupes[ii] < 0) {
            logmsg(LOGMSG_ERROR, "cant find index %d dupes in csc schema %s\n", ii,
                    tblname);
            cleanup_newdb(db);
            return NULL;
        }

        db->ix_recnums[ii] = dyns_is_idx_recnum(ii);
        if (db->ix_recnums[ii] < 0) {
            logmsg(LOGMSG_ERROR, "cant find index %d recnums in csc schema %s\n", ii,
                    tblname);
            cleanup_newdb(db);
            return NULL;
        }

        db->ix_datacopy[ii] = dyns_is_idx_datacopy(ii);
        if (db->ix_datacopy[ii] < 0) {
            logmsg(LOGMSG_ERROR, "cant find index %d datacopy in csc schema %s\n",
                    ii, tblname);
            cleanup_newdb(db);
            return NULL;
        }

        db->ix_nullsallowed[ii] = 0;
        /*
          XXX todo
          db->ix_nullsallowed[ii]=dyns_is_idx_nullsallowed(ii);
          if (db->ix_nullallowed[ii]<0)
          {
          fprintf(stderr,"cant find index %d datacopy in csc schema %s\n",
          ii,tblname);
            cleanup_newdb(db);
            return NULL;
          }
        */
    }
    db->n_rev_constraints =
        0; /* this will be initialized at verification time */
    db->n_constraints = dyns_get_constraint_count();
    if (db->n_constraints > 0) {
        char *keyname = NULL;
        int rulecnt = 0, flags = 0;
        if (db->n_constraints >= MAXCONSTRAINTS) {
            logmsg(LOGMSG_ERROR, "too many constraints for table %s (%d>=%d)\n",
                    tblname, db->n_constraints, MAXCONSTRAINTS);
            cleanup_newdb(db);
            return NULL;
        }
        for (ii = 0; ii < db->n_constraints; ii++) {
            rc = dyns_get_constraint_at(ii, &keyname, &rulecnt, &flags);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "Cannot get constraint at %d (cnt=%d)!\n", ii,
                        db->n_constraints);
                cleanup_newdb(db);
                return NULL;
            }
            db->constraints[ii].flags = flags;
            db->constraints[ii].lcltable = db;
            db->constraints[ii].lclkeyname = strdup(keyname);
            db->constraints[ii].nrules = rulecnt;
            if (db->constraints[ii].nrules >= MAXCONSTRAINTS) {
                logmsg(LOGMSG_ERROR, "too many constraint rules for table %s:%s (%d>=%d)\n",
                        tblname, keyname, db->constraints[ii].nrules,
                        MAXCONSTRAINTS);
                cleanup_newdb(db);
                return NULL;
            } else if (db->constraints[ii].nrules > 0) {
                int jj = 0;
                for (jj = 0; jj < db->constraints[ii].nrules; jj++) {
                    char *tblnm = NULL;
                    rc = dyns_get_constraint_rule(ii, jj, &tblnm, &keyname);
                    if (rc != 0) {
                        logmsg(LOGMSG_ERROR, "cannot get constraint rule %d table %s:%s\n",
                                jj, tblname, keyname);
                        cleanup_newdb(db);
                        return NULL;
                    }
                    db->constraints[ii].table[jj] = strdup(tblnm);
                    db->constraints[ii].keynm[jj] = strdup(keyname);
                }
            }
        } /* for (ii...) */
    }     /* if (n_constraints > 0) */
    db->ixuse = calloc(db->nix, sizeof(unsigned long long));
    db->sqlixuse = calloc(db->nix, sizeof(unsigned long long));
    return db;
}

/* lock mgr partition defaults */
size_t gbl_lk_parts = 73;
size_t gbl_lkr_parts = 23;
size_t gbl_lk_hash = 32;
size_t gbl_lkr_hash = 16;

void add_legacy_default_options(struct dbenv *dbenv) {
   char *legacy_options[] = {
      "disallow write from beta if prod",
      "noblobstripe",
      "nullsort high",
      "dont_sort_nulls_with_header",
      "nochecksums",
      "sql_tranlevel_default comdb2",              /* check this one*/
      "sql_tranlevel_default prefer_oldblock",     /* and this one */
      "off fix_cstr",
      "no_null_blob_fix",
      "no_static_tag_blob_fix",
      "dont_forbid_ulonglong",
      "dont_init_with_ondisk_header",
      "dont_init_with_instant_schema_change",
      "dont_init_with_inplace_updates",
      "dont_prefix_foreign_keys",
      "dont_superset_foreign_keys",
      "disable_inplace_blobs",
      "disable_inplace_blob_optimization",
      "disable_osql_blob_optimization",
      "nocrc32c",
      "enable_tagged_api",
      "nokeycompr",
      "norcache",
      "usenames",
      "dont_return_long_column_names",
      "setattr DIRECTIO 0"
   };
   for (int i = 0; i < sizeof(legacy_options)/sizeof(legacy_options[0]); i++)
      defer_option(dbenv, DEFERRED_LEGACY_DEFAULTS, legacy_options[i], -1, 0);
}

static void getmyaddr(void)
{
    struct hostent *h;

    h = bb_gethostbyname(gbl_mynode);
    if (h == NULL || h->h_addrtype != AF_INET) {
        /* default to localhost */
        gbl_myaddr.s_addr = INADDR_LOOPBACK;
        return;
    }
    memcpy(&gbl_myaddr.s_addr, h->h_addr, h->h_length);
}

static int pre_read_option(struct dbenv *dbenv, char *line,
                           int llen)
{
    char *tok;
    int st = 0;
    int ltok;

    tok = segtok(line, llen, &st, &ltok);
    if (ltok == 0 || tok[0] == '#') return 0;

    /* if this is an "if" statement that evaluates to false, skip */
    if (!lrl_if(&tok, line, llen, &st, &ltok)) {
        return 0;
    }

    if (tokcmp(tok, ltok, "legacy_defaults") == 0) {
        add_legacy_default_options(dbenv);
    }

    if (tokcmp(tok, ltok, "disable_direct_writes") == 0) {
        dbenv->enable_direct_writes = 0;
    }

    else if (tokcmp(tok, ltok, "morecolumns") == 0) {
        logmsg(LOGMSG_INFO, "allowing 1024 columns per table\n");
        gbl_morecolumns = 1;
    }

    else if (tokcmp(tok, ltok, "nullfkey") == 0) {
        gbl_nullfkey = 1;
    } else if (tokcmp(tok, ltok, "disallow_portmux_route") == 0) {
        gbl_pmux_route_enabled = 0;
        logmsg(LOGMSG_INFO, "Won't allow portmux route\n");
    } else if (tokcmp(tok, ltok, "allow_portmux_route") == 0) {
        gbl_pmux_route_enabled = 1;
        logmsg(LOGMSG_INFO, "Will allow portmux route\n");
    } else if (tokcmp(tok, ltok, "portmux_port") == 0) {
        tok = segtok(line, llen, &st, &ltok);
        int portmux_port = toknum(tok, ltok);
        logmsg(LOGMSG_WARN, "Using portmux port %d\n", portmux_port);
        set_portmux_port(portmux_port);
    } else if (tokcmp(tok, ltok, "portmux_bind_path") == 0) {
        char path[108];
        tok = segtok(line, llen, &st, &ltok);
        tokcpy(tok, ltok, path);
        int retrc = set_portmux_bind_path(path);
        if (retrc) {
            logmsg(LOGMSG_ERROR, "Failed in setting portmux bind path %s\n", path);
        } else {
            logmsg(LOGMSG_INFO, "Using portmux bind path %s\n", path);
        }
    } else if (tokcmp(tok, ltok, "usenames") == 0) {
        gbl_nonames = 0;
    } else if (tokcmp(tok, ltok, "nonames") == 0) {
        gbl_nonames = 1;
    } else if (tokcmp(tok, ltok, "forbid_ulonglong") == 0) {
        gbl_forbid_ulonglong = 1;
       logmsg(LOGMSG_INFO, "Will disallow u_longlong\n");
    } else if (tokcmp(tok, ltok, "dont_forbid_ulonglong") == 0) {
        gbl_forbid_ulonglong = 0;
    } else if (tokcmp(tok, ltok, "broken_num_parser") == 0) {
        gbl_broken_num_parser = 1;
    } else if (tokcmp(tok, ltok, "optimize_repdb_truncate") == 0) {
       logmsg(LOGMSG_INFO, "Will use optimized repdb truncate code.\n");
        gbl_optimize_truncate_repdb = 1;
    } else if (tokcmp(tok, ltok, "dont_optimize_repdb_truncate") == 0) {
       logmsg(LOGMSG_INFO, "Will use unoptimized repdb truncate code.\n");
        gbl_optimize_truncate_repdb = 0;
    }

    else if (tokcmp(tok, ltok, "lk_part") == 0) {
        tok = segtok(line, llen, &st, &ltok);
        int parts = toknum(tok, ltok);
        if (parts > 0 && parts < 2049) {
            gbl_lk_parts = parts;
           logmsg(LOGMSG_INFO, "Overriding #Lock Partitions: %u\n", gbl_lk_parts);
        } else {
           logmsg(LOGMSG_INFO, "Lock Partition override out of range:%d (range: 0-2048)\n",
                   parts);
        }
    } else if (tokcmp(tok, ltok, "lkr_part") == 0) {
        tok = segtok(line, llen, &st, &ltok);
        int parts = toknum(tok, ltok);
        if (parts > 0 && parts < 2049) {
            gbl_lkr_parts = parts;
           logmsg(LOGMSG_INFO, "Overriding #Locker Partitions: %u\n", gbl_lkr_parts);
        } else {
            logmsg(LOGMSG_INFO, "Locker Partition override out of range:%d (range: 0-2048)\n",
                parts);
        }
    } else if (tokcmp(tok, ltok, "lk_hash") == 0) {
        tok = segtok(line, llen, &st, &ltok);
        int parts = toknum(tok, ltok);
        if (parts > 0 && parts < 2049) {
            gbl_lk_hash = parts;
           logmsg(LOGMSG_INFO, "Overriding #Hash Buckets per lock partitions: %u\n",
                   gbl_lk_hash);
        } else {
           logmsg(LOGMSG_INFO, "Hash Buckets per lock partition override out of range:%d "
                   "(range: 0-2048)\n",
                   parts);
        }
    } else if (tokcmp(tok, ltok, "lkr_hash") == 0) {
        tok = segtok(line, llen, &st, &ltok);
        int parts = toknum(tok, ltok);
        if (parts > 0 && parts < 2049) {
            gbl_lkr_hash = parts;
           logmsg(LOGMSG_INFO, "Overriding #Hash Buckets per lock partitions: %u\n",
                   gbl_lkr_hash);
        } else {
           logmsg(LOGMSG_INFO, "Hash Buckets per locker partition override out of range:%d "
                   "(range: 0-2048)\n",
                   parts);
        }
    } else if (tokcmp(tok, ltok, "foreign_db_resolve_local") == 0) {
        gbl_fdb_resolve_local = 1;
    } else if (tokcmp(tok, ltok, "foreign_db_allow_cross_class") == 0) {
        gbl_fdb_allow_cross_classes = 1;
    } else if (tokcmp(line, ltok, "hostname") == 0) {
        tok = segtok(line, llen, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_INFO, "Expected hostname for 'hostname' directive.\n");
            return -1;
        }
        gbl_mynode = internn(tok, ltok);
        getmyaddr();
    } else if (tokcmp(line, ltok, "logmsg") == 0) {
       logmsg_process_message(line, llen);
    }
    return 0;
}

static int pre_read_deferred_callback(struct dbenv *env, char *option, void *p,
                                      int len)
{
    pre_read_option(env, option, len);
    return 0;
}

static void pre_read_lrl_file(struct dbenv *dbenv,
                              const char *lrlname, const char *dbname)
{
    FILE *ff;
    char line[512];

    ff = fopen(lrlname, "r");

    if (ff == 0) {
        return;
    }

    while (fgets(line, sizeof(line), ff)) {
        pre_read_option(dbenv, line, strlen(line));
    }

    process_deferred_options(dbenv, DEFERRED_LEGACY_DEFAULTS, NULL,
                             pre_read_deferred_callback);

    fclose(ff); /* lets get one fd back */
}

int defer_option(struct dbenv *dbenv, enum deferred_option_level lvl,
                 char *option, int len, int line)
{
    struct deferred_option *opt;
    if (len == -1) len = strlen(option);
    opt = malloc(sizeof(struct deferred_option));
    if (opt == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d out of memory\n", __FILE__, __LINE__);
        return 1;
    }
    opt->option = calloc(1, len + 1);
    if (opt->option == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d out of memory\n", __FILE__, __LINE__);
        free(opt);
        return 1;
    }
    memcpy(opt->option, option, len);
    opt->line = line;
    opt->len = strlen(opt->option);
    listc_abl(&dbenv->deferred_options[lvl], opt);
    return 0;
}

static int deferred_do_commands(struct dbenv *env, char *option, void *p,
                                int len)
{
    char *tok;
    int st = 0, tlen = 0;

    tok = segtok(option, len, &st, &tlen);
    if (tokcmp(tok, tlen, "sqllogger") == 0)
        sqllogger_process_message(option + st, len - st);
    else if (tokcmp(tok, tlen, "do") == 0)
        process_command(env, option + st, len - st, 0);
    return 0;
}

int process_deferred_options(struct dbenv *dbenv,
                             enum deferred_option_level lvl, void *usrdata,
                             int (*callback)(struct dbenv *env, char *option,
                                             void *p, int len))
{
    struct deferred_option *opt;
    int rc;

    opt = listc_rtl(&dbenv->deferred_options[lvl]);
    while (opt) {
        rc = callback(dbenv, opt->option, usrdata, opt->len);

        if (rc) return rc;
        free(opt->option);
        free(opt);
        opt = listc_rtl(&dbenv->deferred_options[lvl]);
    }
    return 0;
}

void init_deferred_options(struct dbenv *dbenv)
{
    for (int lvl = 0; lvl < DEFERRED_OPTION_MAX; lvl++) {
        listc_init(&dbenv->deferred_options[lvl],
                   offsetof(struct deferred_option, lnk));
    }
}

static char **qdbs = NULL;
static char **sfuncs = NULL;
static char **afuncs = NULL;

#define parse_lua_funcs(pfx)                                                   \
    do {                                                                       \
        tok = segtok(line, sizeof(line), &st, &ltok);                          \
        int num = toknum(tok, ltok);                                           \
        pfx##funcs = malloc(sizeof(char *) * (num + 1));                       \
        int i;                                                                 \
        for (i = 0; i < num; ++i) {                                            \
            tok = segtok(line, sizeof(line), &st, &ltok);                      \
            pfx##funcs[i] = tokdup(tok, ltok);                                 \
        }                                                                      \
        pfx##funcs[i] = NULL;                                                  \
    } while (0)

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

static char *get_qdb_name(const char *file)
{
    FILE *f = fopen(file, "r");
    if (f == NULL) {
        logmsg(LOGMSG_ERROR, "%s:fopen(\"%s\"):%s\n", __func__, file,
                strerror(errno));
        return NULL;
    }
    size_t n;
    ssize_t s;
    // Name of queue
    char *name = NULL;
    s = getline(&name, &n, f);
    fclose(f);
    if (s == -1)
        return NULL;

    name[s - 1] = 0;
    return name;
}

struct read_lrl_option_type {
    int lineno;
    const char *lrlname;
    const char *dbname;
};

static struct dbenv *read_lrl_file_int(struct dbenv *dbenv,
                                       const char *lrlname, const char *dbname,
                                       int required);

static struct dbenv *read_lrl_file(struct dbenv *dbenv,
                                   const char *lrlname, const char *dbname,
                                   int required)
{

    struct lrlfile *lrlfile;
    struct dbenv *out;
    out = read_lrl_file_int(dbenv, (char*) lrlname, dbname, required);

    lrlfile = listc_rtl(&dbenv->lrl_files);
    free(lrlfile->file);
    free(lrlfile);

    return out;
}

static int read_lrl_option(struct dbenv *dbenv, char *line, void *p, int len)
{
    char *tok;
    int st = 0;
    int ltok;
    int ii, kk;
    int num;
    int rc;
    struct read_lrl_option_type *options = (struct read_lrl_option_type *)p;

    tok = segtok(line, len, &st, &ltok);
    if (ltok == 0 || tok[0] == '#') return 0;

    /* if this is an "if" statement that evaluates to false, skip */
    if (!lrl_if(&tok, line, len, &st, &ltok))
        return 1;

    if (tokcmp(tok, ltok, "name") == 0) {
        /* Copycomdb2 still wants this option, but it's useless for anything else.
           Ignore. */
    } else if (tokcmp(tok, ltok, "usenames") == 0) {
        /* This was preventing setting usenames at a global level
           which prevented restoring and starting up from a backup file */
        gbl_nonames = 0;
    } else if(tokcmp(tok, ltok, "enable_direct_writes") == 0) {
        dbenv->enable_direct_writes = 1;
    } else if (tokcmp(tok, ltok, "disable_direct_writes") == 0) {
        dbenv->enable_direct_writes = 0;
    }
    else if (tokcmp(tok, ltok, "nullfkey") == 0) {
        logmsg(LOGMSG_INFO, "fkey relationship not enforced for nulls\n");
        gbl_nullfkey = 1;
    }
    else if (tokcmp(tok, ltok, "fullrecovery") == 0) {
        logmsg(LOGMSG_INFO, "enabling full recovery\n");
        gbl_fullrecovery = 1;
    }

    else if (tokcmp(tok, ltok, "bdblock_debug") == 0) {
        logmsg(LOGMSG_INFO, "setting bdblock_debug\n");
        bdb_bdblock_debug();
    }

    else if (tokcmp(tok, ltok, "sort_nulls_with_header") == 0) {
        gbl_sort_nulls_correctly = 1;
        logmsg(LOGMSG_INFO, "using record headers in key sorting\n");
    } else if (tokcmp(tok, ltok, "dir") == 0) {
        tok = segtok(line, len, &st, &ltok);
        dbenv->basedir = tokdup(tok, ltok);
        if (!gooddir(dbenv->basedir)) {
            logmsg(LOGMSG_INFO, "bad directory %s in lrl %s\n", dbenv->basedir,
                   options->lrlname);
            return -1;
        }
    } else if (tokcmp(tok, ltok, "singlemeta") == 0) {
        gbl_init_single_meta = 1;
    } else if (tokcmp(tok, ltok, "delayed_ondisk_tempdbs") == 0) {
        gbl_delayed_ondisk_tempdbs = 1;
    } else if (tokcmp(tok, ltok, "init_with_genid48") == 0) {
        logmsg(LOGMSG_INFO, "Genid48 will be enabled for this database\n");
        gbl_init_with_genid48 = 1;
    } else if (tokcmp(tok, ltok, "init_with_time_based_genids") == 0) {
        logmsg(LOGMSG_INFO, "This will use time-based genids\n");
        gbl_init_with_genid48 = 0;
    } else if (tokcmp(tok, ltok, "init_with_rowlocks") == 0) {
        logmsg(LOGMSG_INFO, "Rowlocks will be enabled for this database\n");
        gbl_init_with_rowlocks = 1;
    } else if (tokcmp(tok, ltok, "init_with_rowlocks_master_only") == 0) {
        logmsg(LOGMSG_INFO, "Rowlocks will be enabled for this database (master only)\n");
        gbl_init_with_rowlocks = 2;
    } else if (tokcmp(tok, ltok, "init_with_ondisk_header") == 0) {
        logmsg(LOGMSG_INFO, "Tables will be initialized with ODH\n");
        gbl_init_with_odh = 1;
    } else if (tokcmp(tok, ltok, "init_with_inplace_updates") == 0) {
        logmsg(LOGMSG_INFO, "Tables will be initialized with inplace-update support\n");
        gbl_init_with_ipu = 1;
    } else if (tokcmp(tok, ltok, "init_with_instant_schema_change") == 0) {
        logmsg(LOGMSG_INFO, 
               "Tables will be initialized with instant schema-change support\n");
        gbl_init_with_instant_sc = 1;
    } else if (tokcmp(tok, ltok, "instant_schema_change") == 0) {
        logmsg(LOGMSG_INFO,
               "Tables will be initialized with instant schema-change support\n");
        gbl_init_with_instant_sc = 1;
    } else if (tokcmp(line, ltok, "init_with_compr") == 0) {
        tok = segtok(line, len, &st, &ltok);
        char *algo = tokdup(tok, ltok);
        gbl_init_with_compr = bdb_compr2algo(algo);
        free(algo);
        logmsg(LOGMSG_INFO, "New tables will be compressed: %s\n",
               bdb_algo2compr(gbl_init_with_compr));
    } else if (tokcmp(line, ltok, "init_with_compr_blobs") == 0) {
        tok = segtok(line, len, &st, &ltok);
        char *algo = tokdup(tok, ltok);
        gbl_init_with_compr_blobs = bdb_compr2algo(algo);
        free(algo);
        logmsg(LOGMSG_INFO, "Blobs in new tables will be compressed: %s\n",
               bdb_algo2compr(gbl_init_with_compr_blobs));
    } else if (tokcmp(line, ltok, "init_with_bthash") == 0) {
        int szkb;
        tok = segtok(line, len, &st, &ltok);
        szkb = toknum(tok, ltok);
        if (szkb <= 0) {
            logmsg(LOGMSG_INFO, "Invalid hash size. init_with_bthash DISABLED\n");
        } else {
            gbl_init_with_bthash = szkb;
            logmsg(LOGMSG_INFO, "Init with bthash %dkb per stripe\n", gbl_init_with_bthash);
        }
    } else if (tokcmp(tok, ltok, "directio") == 0) {
        logmsg(LOGMSG_INFO, "enabling direct io\n");
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_DIRECTIO, 1);
    } else if (tokcmp(tok, ltok, "osync") == 0) {
        logmsg(LOGMSG_INFO, "enabling osync io\n");
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_OSYNC, 1);
    } else if (tokcmp(tok, ltok, "nonames") == 0) {
        logmsg(LOGMSG_INFO, "we will not embed our db name in our files\n");
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_NONAMES, 1);
    } 
    else if (tokcmp(tok, ltok, "checksums") == 0) {
        logmsg(LOGMSG_INFO, "enabling checksums\n");
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_CHECKSUMS, 1);
    }

    else if (tokcmp(tok, ltok, "memptricklepercent") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting memptricklepercent to %d\n", ii);

        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_MEMPTRICKLEPERCENT, ii);
    }


    else if (tokcmp(tok, ltok, "memptricklemsecs") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting memp trickle to run every %d "
               "milliseconds\n",
               ii);

        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_MEMPTRICKLEMSECS, ii);
    }

    else if (tokcmp(tok, ltok, "log_region_size") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting log region size to %d KBs\n", ii);
        ii = ii * 1024;

        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_LOGREGIONSZ, ii);
    }

    else if (tokcmp(tok, ltok, "commitdelaymax") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting commitdelaymax to %ll\n");
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_COMMITDELAYMAX, ii);
    }

    else if (tokcmp(tok, ltok, "lock_conflict_trace") == 0) {
        logmsg(LOGMSG_INFO, "Enabling lock-conflict trace.\n");
        gbl_lock_conflict_trace = 1;
    }

    else if (tokcmp(tok, ltok, "no_lock_conflict_trace") == 0) {
        logmsg(LOGMSG_INFO, "Disabling lock-conflict trace.\n");
        gbl_lock_conflict_trace = 0;
    }

    else if (tokcmp(tok, ltok, "move_deadlock_max_attempt") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting deadlock-on-move max-attempts to %d\n",
               ii);
        gbl_move_deadlk_max_attempt = ii;
    }

    else if (tokcmp(tok, ltok, "net_max_queue") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting net_max_queue to %d\n", ii);
        gbl_net_max_queue = ii;
    }

    /* MB */
    else if (tokcmp(tok, ltok, "net_max_mem") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting net_max_mem to %d\n", ii);
        gbl_net_max_mem = ii;
    }

    else if (tokcmp(tok, ltok, "net_throttle_percent") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        if (ii >= 0 || ii <= 100) {
            logmsg(LOGMSG_INFO, "setting net_throttle_percent to %d\n", ii);
            gbl_net_throttle_percent = ii;
        } else {
            logmsg(LOGMSG_ERROR, "invalid net throttle percent, %d\n", ii);
        }
    }

    else if (tokcmp(tok, ltok, "net_max_queue_signal") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting net_max_queue_signal to %d\n", ii);
        gbl_net_max_queue_signal = ii;
    }

    else if (tokcmp(tok, ltok, "blocksql_grace") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting blocksql grace timeout to %d seconds\n",
               ii);
        gbl_blocksql_grace = ii;
    } else if (tokcmp(tok, ltok, "net_poll") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting net_poll to %d\n", ii);
        gbl_net_poll = ii;
    }

    else if (tokcmp(tok, ltok, "osql_net_poll") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting osql_net_poll to %d\n", ii);
        gbl_osql_net_poll = ii;
    }

    else if (tokcmp(tok, ltok, "net_portmux_register_interval") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting osql_portmux_register_interval to %d\n",
               ii);
        gbl_net_portmux_register_interval = ii;
    } else if (tokcmp(tok, ltok, "signal_net_portmux_register_interval") ==
               0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting signal_portmux_register_interval to %d\n",
               ii);
        gbl_signal_net_portmux_register_interval = ii;
    } else if (tokcmp(tok, ltok, "osql_net_portmux_register_interval") ==
               0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting osql_portmux_register_interval to %d\n",
               ii);
        gbl_osql_net_portmux_register_interval = ii;
    }

    else if (tokcmp(tok, ltok, "osql_max_queue") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting osql_max_queue to %d\n", ii);
        gbl_osql_max_queue = ii;
    }

    else if (tokcmp(tok, ltok, "osql_bkoff_netsend") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting osql_bkoff_netsend to %d\n", ii);
        gbl_osql_bkoff_netsend = ii;
    }

    else if (tokcmp(tok, ltok, "osql_bkoff_netsend_lmt") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting osql_bkoff_netsend_lmt to %d\n", ii);
        gbl_osql_bkoff_netsend_lmt = ii;
    }

    else if (tokcmp(tok, ltok, "osql_blockproc_timeout_sec") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting osql_blockproc_timeout_sec to %d\n", ii);
        gbl_osql_blockproc_timeout_sec = ii;
    }

    else if (tokcmp(tok, ltok, "gbl_exit_on_pthread_create_fail") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting exit_on_pthread_create_fail to %d\n", ii);
        gbl_exit_on_pthread_create_fail = ii;
    }

    else if (tokcmp(tok, ltok, "enable_sock_fstsnd") == 0) {
        if (gbl_enable_sock_fstsnd != -1) {
            logmsg(LOGMSG_INFO, "enabling fstsnd over socket\n");
            gbl_enable_sock_fstsnd = 1;
        } else {
            logmsg(LOGMSG_ERROR,
                   "fstsnd over socket can't be enabled for this DB\n");
        }
    } else if (tokcmp(tok, ltok, "disable_sock_fstsnd") == 0) {
        logmsg(LOGMSG_INFO, "disabling fstsnd over socket\n");
        gbl_enable_sock_fstsnd = -1;
    } else if (tokcmp(tok, ltok, "disable_skip_rows") == 0) {
        logmsg(LOGMSG_INFO, "disabling skip rows\n");
        gbl_disable_skip_rows = 1;
    } else if (tokcmp(tok, ltok, "enable_sp_strict_assignments") == 0) {
        gbl_spstrictassignments = 1;
    } else if (tokcmp(tok, ltok, "use_appsock_as_sqlthread") == 0) {
        logmsg(LOGMSG_INFO, "Using appsock thread for sql queries.\n");
        gbl_use_appsock_as_sqlthread = 1;
    } else if (tokcmp(tok, ltok, "enable_sql_stmt_caching") == 0) {
        logmsg(LOGMSG_INFO, "enabling SQL statement caching\n");
        tok = segtok(line, len, &st, &ltok);
        if (tokcmp(tok, ltok, "all") == 0) {
            gbl_enable_sql_stmt_caching = STMT_CACHE_ALL;
            logmsg(LOGMSG_INFO, "Statement caching is for ALL queries\n");
        } else {
            gbl_enable_sql_stmt_caching = STMT_CACHE_PARAM;
            logmsg(LOGMSG_INFO, "Statement caching is for PARAM queries\n");
        }

    } else if (tokcmp(tok, ltok, "max_sqlcache_per_thread") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO,
               "Maximum of %d statements will be cached per thread.\n",
               ii);
        gbl_max_sqlcache = ii;
    }
    else if (tokcmp(tok, ltok, "max_sqlcache_hints") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "Maximum of %d sql hints will be cached.\n", ii);
        gbl_max_sql_hint_cache = ii;
    } else if (tokcmp(tok, ltok, "max_lua_instructions") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting maximum lua instructions %d\n", ii);
        gbl_max_lua_instructions = ii;
    } else if (tokcmp(tok, ltok, "iothreads") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting numiothreads to %d\n", ii);
        gbl_iothreads = ii;
    }

    else if (tokcmp(tok, ltok, "ioqueue") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting ioqueue to %d\n", ii);
        gbl_ioqueue = ii;
    }

    else if (tokcmp(tok, ltok, "prefaulthelperthreads") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting prefaulthelperthreads to %d\n", ii);
        gbl_prefaulthelperthreads = ii;
    }

    else if (tokcmp(tok, ltok, "osqlprefaultthreads") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        gbl_osqlpfault_threads = ii;
        logmsg(LOGMSG_INFO, "setting prefaulthelperthreads to %d\n",
               gbl_osqlpfault_threads);
    }

    else if (tokcmp(tok, ltok, "enable_prefault_udp") == 0) {
        gbl_prefault_udp = 1;
    }

    else if (tokcmp(tok, ltok, "disable_prefault_udp") == 0) {
        gbl_prefault_udp = 0;
    }

    else if (tokcmp(tok, ltok, "readahead") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting readahead to %d\n", ii);
        gbl_readahead = ii;
    }

    else if (tokcmp(tok, ltok, "sqlreadahead") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting sqlreadahead to %d\n", ii);
        gbl_sqlreadahead = ii;
    }

    else if (tokcmp(tok, ltok, "prefaulthelper_blockops") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting prefaulthelper_blockops to %d\n", ii);
        gbl_prefaulthelper_blockops = ii;
    }

    else if (tokcmp(tok, ltok, "prefaulthelper_sqlreadahead") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting prefaulthelper_sqlreadahead to %d\n", ii);
        gbl_prefaulthelper_sqlreadahead = ii;
    }

    else if (tokcmp(tok, ltok, "prefaulthelper_tagreadahead") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting prefaulthelper_tagreadahead to %d\n", ii);
        gbl_prefaulthelper_tagreadahead = ii;
    }

    else if (tokcmp(tok, ltok, "readaheadthresh") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting readaheadthresh to %d\n", ii);
        gbl_readaheadthresh = ii;
    }

    else if (tokcmp(tok, ltok, "sqlreadaheadthresh") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting sqlreadaheadthresh to %d\n", ii);
        gbl_sqlreadaheadthresh = ii;
    }

    else if (tokcmp(tok, ltok, "sqlsortermem") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting sqlsortermem to %d\n", ii);
        gbl_sqlite_sorter_mem = ii;
    }

    else if (tokcmp(tok, ltok, "sqlsortermaxmmapsize") == 0) {
        tok = segtok(line, len, &st, &ltok);
        long long maxmmapsz = toknumll(tok, ltok);
        logmsg(LOGMSG_INFO, "setting sqlsortermaxmmapsize to %ld bytes\n",
               maxmmapsz);
        sqlite3_config(SQLITE_CONFIG_MMAP_SIZE, SQLITE_DEFAULT_MMAP_SIZE,
                       maxmmapsz);
    }

    else if (tokcmp(tok, ltok, "sqlsortermult") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting sqlsortermult to %d\n", ii);
        gbl_sqlite_sortermult = ii;
    }

    // cache <nn> <kb|mb|gb>
    else if (tokcmp(tok, ltok, "cache") == 0) {
        tok = segtok(line, len, &st, &ltok);
        int nn = toknum(tok, ltok);
        tok = segtok(line, len, &st, &ltok);
        if (tokcmp(tok, ltok, "kb") == 0)
            dbenv->cacheszkb = nn;
        else if (tokcmp(tok, ltok, "mb") == 0)
            dbenv->cacheszkb = nn * 1024;
        else if (tokcmp(tok, ltok, "gb") == 0)
            dbenv->cacheszkb = nn * 1024 * 1024;
        else
            logmsg(LOGMSG_ERROR, "bad unit for cache sz - needs kb|mb|gb\n");
        logmsg(LOGMSG_INFO, "cache size is %dKB\n", dbenv->cacheszkb);
    }

    else if (tokcmp(tok, ltok, "cachekb") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        dbenv->cacheszkb = ii;
        logmsg(LOGMSG_INFO, "cachekbsz is %d\n", dbenv->cacheszkb);
    }

    else if (tokcmp(tok, ltok, "cachekbmin") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        dbenv->cacheszkbmin = ii;
    } else if (tokcmp(tok, ltok, "cachekbmax") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        dbenv->cacheszkbmax = ii;
    } else if (tokcmp(tok, ltok, "override_cachekb") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        dbenv->override_cacheszkb = ii;
    } else if (tokcmp(tok, ltok, "dedicated_network_suffixes") == 0) {
        while (1) {
            char suffix[50];

            tok = segtok(line, len, &st, &ltok);
            if (ltok == 0)
                break;
            tokcpy(tok, ltok, suffix);

            if (net_add_to_subnets(suffix, options->lrlname)) {
               return -1;
            }
      }
   } else if (tokcmp(tok, ltok, "use_nondedicated_network") == 0) {
      net_add_nondedicated_subnet();
   }
      /* enable client side retrys for n seconds.
      keep blkseq's around for 2 * this time. */
   else if (tokcmp(tok, ltok, "retry") == 0) {
      tok = segtok(line, len, &st, &ltok);
      ii = toknum(tok, ltok);
      /* For now the proxy will treat the number 180 as meaning "this
       * is old comdb2.tsk which defaults to 180 seconds", so we put in
       * this HACK!!!! to get around that.  remove this hack when this
       * build of comdb2.tsk is everywhere. */
      if (ii == 180)
         ii = 181;
      dbenv->retry = ii;
   }

    else if (tokcmp(tok, ltok, "maxcontextskips") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting maxcontextskips to %d\n", ii);
        gbl_maxcontextskips = ii;
    }

    else if (tokcmp(tok, ltok, "appsockslimit") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting soft limit of appsock connections to %d\n",
               ii);
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_APPSOCKSLIMIT, ii);
    }

    else if (tokcmp(tok, ltok, "maxappsockslimit") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting max appsock connections to %d\n", ii);
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_MAXAPPSOCKSLIMIT, ii);
    } else if (tokcmp(tok, ltok, "maxsockcached") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting max sockets cached to %d\n", ii);
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_MAXSOCKCACHED, ii);
    } else if (tokcmp(tok, ltok, "maxlockers") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting maxlockers to %d\n", ii);
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_MAXLOCKERS, ii);
    }

    else if (tokcmp(tok, ltok, "maxlocks") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting maxlocks to %d\n", ii);
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_MAXLOCKS, ii);
    }

    else if (tokcmp(tok, ltok, "maxlockobjects") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting maxlockobjects to %d\n", ii);
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_MAXLOCKOBJECTS, ii);
    }

    else if (tokcmp(tok, ltok, "maxtxn") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting maxtxn to %d\n", ii);
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_MAXTXN, ii);
    }

    else if (tokcmp(tok, ltok, "longblockcache") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting longblockcache to %d\n", ii);
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_LONGBLOCKCACHE, ii);
    }

    else if (tokcmp(tok, ltok, "largepages") == 0) {
        logmsg(LOGMSG_INFO, "enabling large page support\n");
        gbl_largepages = 1;
    }

    else if (tokcmp(tok, ltok, "nullsort") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected argument for nullsort\n");
            return -1;
        } else if (tokcmp(tok, ltok, "high") == 0) {
            /* default */
            comdb2_types_set_null_bit(null_bit_high);
            logmsg(LOGMSG_INFO, "nulls will sort high\n");
        } else if (tokcmp(tok, ltok, "low") == 0) {
            comdb2_types_set_null_bit(null_bit_low);
            logmsg(LOGMSG_INFO, "nulls will sort low\n");
        } else {
            logmsg(LOGMSG_ERROR, "Invalid argument for nullsort\n");
        }
    }

    else if (tokcmp(tok, ltok, "logmemsize") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting logmemsize to %d\n", ii);
        dbenv->log_mem_size = ii;
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_LOGMEMSIZE, ii);
    }

    else if (tokcmp(tok, ltok, "checkpointtime") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting checkpointtime to %d\n", ii);
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_CHECKPOINTTIME, ii);
    }

    else if (tokcmp(tok, ltok, "logfilesize") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting logfilesize to %d\n", ii);
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_LOGFILESIZE, ii);
    }

    else if (tokcmp(tok, ltok, "maxosqltransfer") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting maxosqltransfer to %d\n", ii);
        set_osql_maxtransfer(ii);
    } else if (tokcmp(tok, ltok, "fdbdebg") == 0) {
        extern int gbl_fdb_track;
        tok = segtok(line, len, &st, &ltok);
        gbl_fdb_track = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "%s fdb debugging\n",
               (gbl_fdb_track) ? "Enabling" : "Disabling");
    } else if (tokcmp(tok, ltok, "fdbtrackhints") == 0) {
        extern int gbl_fdb_track_hints;
        tok = segtok(line, len, &st, &ltok);
        gbl_fdb_track_hints = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "%s fdb hint tracking\n",
               (gbl_fdb_track_hints) ? "Enabling" : "Disabling");
    }

    else if (tokcmp(tok, ltok, "maxthrottletime") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting maxthrottle seconds to %d\n", ii);
        set_osql_maxthrottle_sec(ii);
    } else if (tokcmp(tok, ltok, "port") == 0) {
        char hostname[255];
        int port;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR,
                   "Expected hostname port for \"port\" directive\n");
            return -1;
        }
        if (ltok >= sizeof(hostname)) {
            logmsg(LOGMSG_ERROR,
                   "Unexpectedly long hostname %.*s len %d max %d\n",
                   ltok, hostname, ltok, sizeof(hostname));
            return -1;
        }
        tokcpy(tok, ltok, hostname);
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR,
                   "Expected hostname port for \"port\" directive\n");
            return -1;
        }
        port = toknum(tok, ltok);
        if (port <= 0 || port >= 65536) {
            logmsg(LOGMSG_ERROR, "Port out of range for \"port\" directive\n");
            return -1;
        }
        if (dbenv->nsiblings > 1) {
            for (ii = 0; ii < dbenv->nsiblings; ii++) {
                if (strcmp(dbenv->sibling_hostname[ii], hostname) == 0) {
                    dbenv->sibling_port[ii][NET_REPLICATION] = port;
                    dbenv->sibling_port[ii][NET_SQL] = port;
                    dbenv->sibling_port[ii][NET_SIGNAL] = port;
                    break;
                }
            }
            if (ii >= dbenv->nsiblings) {
                logmsg(LOGMSG_ERROR,
                       "Don't recognize %s as part of my cluster.  Please make "
                       "sure \"port\" directives follow \"cluster nodes\"\n",
                       hostname);
                return -1;
            }
        } else if (strcmp(hostname, "localhost") == 0) {
            /* nsiblings == 1 means there's no other nodes in the cluster */
            dbenv->sibling_port[0][NET_REPLICATION] = port;
            dbenv->sibling_port[0][NET_SQL] = port;
            dbenv->sibling_port[0][NET_SIGNAL] = port;
        }
    } else if (tokcmp(tok, ltok, "cluster") == 0) {
        /*parse line...*/
        tok = segtok(line, len, &st, &ltok);
        if (tokcmp(tok, ltok, "nodes") == 0) {
            /*create replication group. only me by default*/
            while (1) {
                char nodename[512];
                struct hostent *h;

                tok = segtok(line, len, &st, &ltok);
                if (ltok == 0) break;
                if (ltok > sizeof(nodename)) {
                    logmsg(LOGMSG_ERROR,
                           "host %.*s name too long (expected < %d)\n",
                           ltok, tok, sizeof(nodename));
                    return -1;
                }
                tokcpy(tok, ltok, nodename);
                errno = 0;

                if (dbenv->nsiblings >= MAXSIBLINGS) {
                    logmsg(LOGMSG_ERROR,
                           "too many sibling nodes (max=%d) in lrl %s\n",
                           MAXSIBLINGS, options->lrlname);
                    return -1;
                }

                /* Check to see if this name is another name for me. */
                h = bb_gethostbyname(nodename);
                if (h && h->h_addrtype == AF_INET &&
                    memcmp(&gbl_myaddr.s_addr, h->h_addr,
                           h->h_length == 0)) {
                    /* Assume I am better known by this name. */
                    gbl_mynode = intern(nodename);
                }
                if (strcmp(gbl_mynode, nodename) == 0 &&
                    gbl_rep_node_pri == 0) {
                    /* assign the priority of current node according to its
                     * sequence in nodes list. */
                    gbl_rep_node_pri = MAXSIBLINGS - dbenv->nsiblings;
                    continue;
                }
                /* lets ignore duplicate for now and make a list out of what is
                 * given in lrl */
                for (kk = 1; kk < dbenv->nsiblings &&
                         strcmp(dbenv->sibling_hostname[kk], nodename);
                     kk++)
                    ; /*look for dupes*/
                if (kk == dbenv->nsiblings) {
                    /*not a dupe.*/
                    dbenv->sibling_hostname[dbenv->nsiblings] =
                        intern(nodename);
                    for (int netnum = 0; netnum < MAXNETS; netnum++)
                        dbenv->sibling_port[dbenv->nsiblings][netnum] = 0;
                    dbenv->nsiblings++;
                }
            }
            dbenv->sibling_hostname[0] = gbl_mynode;
        }
    }

    else if (tokcmp(tok, ltok, "pagesize") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify options for pagesize.\n");
            return -1;
        }

        if (tokcmp(tok, ltok, "all") == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify size for all\n");
            return -1;
#if 0 
            NOT SURE WHY THE SHORTEN PATH, but commenting out to
                remove compiler warning
                ii=toknum(tok,ltok);

            bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGESIZEDTA, ii);
            bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGESIZEFREEREC, ii);
            bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGESIZEIX, ii);
            bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGESIZEBLOB, ii);
#endif
        } else if (tokcmp(tok, ltok, "dta") == 0) {
            tok = segtok(line, len, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "Need to specify size for dta\n");
                return -1;
            }
            ii = toknum(tok, ltok);

            bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGESIZEDTA, ii);
        } else if (tokcmp(tok, ltok, "ix") == 0) {
            tok = segtok(line, len, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "Need to specify size for ix\n");
                return -1;
            }
            ii = toknum(tok, ltok);
            bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGESIZEIX, ii);
        } else {
            logmsg(LOGMSG_ERROR, "Need to specify options for pagesize.\n");
            return -1;
        }
    }

    else if (tokcmp(tok, ltok, "heartbeat_send_time") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify time for heartbeat send\n");
            return -1;
        }
        num = toknum(tok, ltok);
        if (num <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid heartbeat send time\n");
            return -1;
        }
        gbl_heartbeat_send = num;
    }

    else if (tokcmp(tok, ltok, "sc_del_unused_files_threshold") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, 
                   "Need to specify ms for sc_del_unused_files_threshold\n");
            return -1;
        }
        num = toknum(tok, ltok);
        if (num <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid sc_del_unused_files_threshold\n");
            return -1;
        }
        gbl_sc_del_unused_files_threshold_ms = num;
    }

    else if (tokcmp(tok, ltok, "netbufsz") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify bytes for netbufsz\n");
            return -1;
        }
        num = toknum(tok, ltok);
        if (num <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid netbufsz\n");
            return -1;
        }
        gbl_netbufsz = num;
    } else if (tokcmp(tok, ltok, "netbufsz_signal") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify bytes for netbufsz_signal\n");
            return -1;
        }
        num = toknum(tok, ltok);
        if (num <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid netbufsz_signal\n");
            return -1;
        }
        gbl_netbufsz_signal = num;
    }

    else if (tokcmp(tok, ltok, "heartbeat_check_time") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify time for heartbeat check\n");
            return -1;
        }
        num = toknum(tok, ltok);
        if (num <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid heartbeat check time\n");
            return -1;
        }
        gbl_heartbeat_check = num;
    } else if (tokcmp(tok, ltok, "dtastripe") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify num for dtastripe\n");
            return -1;
        }
        num = toknum(tok, ltok);
        if (num <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid dtastripe\n");
            return -1;
        }
        gbl_dtastripe = num;
    } else if (tokcmp(tok, ltok, "rr_enable_count_changes") == 0) {
        gbl_rrenablecountchanges = 1;
    } else if (tokcmp(tok, ltok, "blobstripe") == 0) {
        gbl_blobstripe = 1;
    } else if (tokcmp(tok, ltok, "penaltyincpercent") == 0) {
        int nthrk = 0;
        tok = segtok(line, len, &st, &ltok);
        nthrk = toknum(tok, ltok);
        gbl_penaltyincpercent = nthrk;
    } else if (tokcmp(tok, ltok, "maxwt") == 0) {
        int nthrk = 0;
        tok = segtok(line, len, &st, &ltok);
        nthrk = toknum(tok, ltok);
        if (nthrk < 1) {
            logmsg(LOGMSG_ERROR, "bad number %d of writer threads %s\n", nthrk,
                   options->lrlname);
            return -1;
        } else if (nthrk > gbl_maxthreads) {
            logmsg(LOGMSG_ERROR, "number of writer threads (%d) must be <= to "
                   "total number of threads (%d)\n",
                   nthrk, gbl_maxthreads);
        } else {
            gbl_maxwthreads = nthrk;
            logmsg(LOGMSG_INFO, "max number of writer threads set to %d in lrl\n",
                   gbl_maxwthreads);
        }
    }

    else if (tokcmp(tok, ltok, "maxt") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify num for maxt\n");
            return -1;
        }
        num = toknum(tok, ltok);
        if (num <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid maxt\n");
            return -1;
        }
        gbl_maxthreads = num;
        logmsg(LOGMSG_INFO, "max number of threads set to %d in lrl\n", gbl_maxthreads);
        if (gbl_maxwthreads > gbl_maxthreads) {
            logmsg(LOGMSG_INFO, "reducing max number of writer threads in lrl to %d\n",
                   gbl_maxthreads);
            gbl_maxwthreads = gbl_maxthreads;
        }
    }

    else if (tokcmp(tok, ltok, "maxq") == 0) {
        int z;
        tok = segtok(line, len, &st, &ltok);
        z = toknum(tok, ltok);
        if (z < 1 || z > 1000 /*can't be more than swapinit!*/) {
            logmsg(LOGMSG_ERROR, "bad max number of items on queue\n");
            return -1;
        } else {
            gbl_maxqueue = z; /* max pending requests.*/
            logmsg(LOGMSG_INFO, "max number of items on queue set to %d\n",
                   gbl_maxqueue);
        }
    }

    else if (tokcmp(tok, ltok, "nice") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify nice value for nice\n");
            return -1;
        }
        num = toknum(tok, ltok);
        errno = 0;
        logmsg(LOGMSG_INFO, "nice %d: %s\n", nice(num), strerror(errno));
    }

    /* we cant actually BECOME a different version of comdb2 at runtime,
       just because the lrl file told us to.  what we can do is check for
       some confused situations where the lrl file is for a different
       version of comdb2 than we are */
    else if (tokcmp(tok, ltok, "version") == 0) {
        tok = segtok(line, len, &st, &ltok);
        for (ii = 0; ii < 10; ii++)
            if (!isprint(tok[ii])) tok[ii] = '\0';

        logmsg(LOGMSG_ERROR, "lrl file for comdb2 version %s found\n", tok);

        if (strcmp(tok, COMDB2_VERSION)) {
            logmsg(LOGMSG_ERROR, "but we are version %s\n", COMDB2_VERSION);
            exit(1);
        }
    }

    else if (tokcmp(tok, ltok, "decom_time") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify time for decomission time\n");
            return -1;
        }
        num = toknum(tok, ltok);
        if (num <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid decomission time\n");
            return -1;
        }
        gbl_decom = num;
    } else if (tokcmp(tok, ltok, "checkctags") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify options for checkctags.\n");
            return -1;
        }

        if (tokcmp(tok, ltok, "off") == 0) {
            logmsg(LOGMSG_INFO, "check tag logic is now off\n");
            gbl_check_client_tags = 0;
        } else if (tokcmp(tok, ltok, "soft") == 0) {
            logmsg(LOGMSG_INFO, "check tag logic will now produce warning\n");
            gbl_check_client_tags = 2;
        } else if (tokcmp(tok, ltok, "full") == 0) {
            logmsg(LOGMSG_INFO, "check tag logic will now error out to client\n");
            gbl_check_client_tags = 1;
        } else {
            logmsg(LOGMSG_INFO, "Need to specify options for checktags.\n");
            return -1;
        }
    } else if (tokcmp(tok, ltok, "sync") == 0) {
        rc = process_sync_command(dbenv, line, len, st);
        if (rc != 0) {
            return -1;
        }
    } else if (tokcmp(tok, ltok, "queue") == 0) {
        struct db *db;
        char *qname;
        int avgsz;
        int pagesize = 0;

        /*
          queue <qname>
        */
        tok = segtok(line, len, &st, &ltok); /* queue name */
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Malformed \"queue\" directive\n");
            return -1;
        }
        qname = tokdup(tok, ltok);

        tok = segtok(line, len, &st, &ltok); /* item sz*/
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Malformed \"queue\" directive\n");
            return -1;
        }
        avgsz = toknum(tok, ltok);
        if (avgsz == 0) {
            logmsg(LOGMSG_ERROR, "Malformed \"queue\" directive\n");
            return -1;
        }

        /* This code is dupliated in the message trap parser.. sorry */
        tok = segtok(line, len, &st, &ltok);
        while (ltok) {
            char ctok[64];
            tokcpy0(tok, ltok, ctok, sizeof(ctok));
            if (strncmp(ctok, "pagesize=", 9) == 0) {
                pagesize = atoi(ctok + 9);
            } else {
                logmsg(LOGMSG_ERROR, "Bad queue attribute '%s'\n", ctok);
                return -1;
            }
            tok = segtok(line, len, &st, &ltok);
        }

        db = newqdb(dbenv, qname, avgsz, pagesize, 0);
        if (!db) {
            return -1;
        }
        db->dbs_idx = -1;

        dbenv->qdbs = realloc(dbenv->qdbs, (dbenv->num_qdbs + 1) * sizeof(struct db *));
        dbenv->qdbs[dbenv->num_qdbs++] = db;

        /* Add queue to the hash. */
        hash_add(dbenv->qdb_hash, db);

    } else if (tokcmp(tok, ltok, "consumer") == 0) {
        char *qname;
        int consumer;
        char *method;
        struct db *db;

        /*
         * consumer <qname> <consumer#> <method>
         */
        tok = segtok(line, len, &st, &ltok); /* queue name */
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Malformed \"consumer\" directive\n");
            return -1;
        }
        qname = tokdup(tok, ltok);
        tok = segtok(line, len, &st, &ltok); /* consumer # */
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Malformed \"consumer\" directive\n");
            return -1;
        }
        consumer = toknum(tok, ltok);
        tok = segtok(line, len, &st, &ltok); /* method */
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Malformed \"consumer\" directive\n");
            return -1;
        }
        method = tokdup(tok, ltok);

        db = getqueuebyname(qname);
        if (!db) {
            logmsg(LOGMSG_ERROR, "No such queue '%s'\n", qname);
            return -1;
        }

        if (dbqueue_add_consumer(db, consumer, method, 1) != 0) {
            return -1;
        }
    } else if (tokcmp(tok, ltok, "spfile") == 0) {
        logmsg(LOGMSG_INFO, "read the stored procedures.\n");
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "expected file name\n");
            return -1;
        }
        gbl_spfile_name = getdbrelpath(tokdup(tok, ltok));
    } else if (tokcmp(tok, ltok, "sfuncs") == 0) {
        parse_lua_funcs(s);
    } else if (tokcmp(tok, ltok, "afuncs") == 0) {
        parse_lua_funcs(a);
    } else if (tokcmp(tok, ltok, "num_qdbs") == 0) {
        tok = segtok(line, len, &st, &ltok);
        int num = toknum(tok, ltok);
        dbenv->num_qdbs = num;
        dbenv->qdbs = calloc(num, sizeof(struct db *));
        qdbs = calloc(num + 1, sizeof(char *));
    } else if (tokcmp(tok, ltok, "queuedb") == 0) {
        char **slot = &qdbs[0];
        while (*slot)
            ++slot;
        tok = segtok(line, len, &st, &ltok);
        *slot = tokdup(tok, ltok);
        struct db **qdb = &dbenv->qdbs[0];
        while (*qdb)
            ++qdb;
        char *name = get_qdb_name(*slot);
        if (name == NULL) {
            logmsg(LOGMSG_ERROR, "Failed to obtain queuedb name from:%s\n",
                   *slot);
            return -1;
        }
        *qdb = newqdb(dbenv, name, 65536, 65536, 1);
        if (*qdb == NULL) {
            logmsg(LOGMSG_ERROR, "newqdb failed for:%s\n", name);
            return -1;
        }
        free(name);
    } else if (tokcmp(tok, ltok, "table") == 0) {
        /*
         * variants:
         * table foo foo.lrl        # load a table given secondary lrl files
         * table foo foo.csc        # load a table from a csc file
         * table foo foo.csc dbnum  # load a table from a csc file and have
         * # it also accept requests as db# dbnum
         *
         * relative paths are looked for relative to the parent lrl
         *
         * table     sqlite_stat1 bin/comdb2_stats.csc2
         */
        char *fname;
        char *tblname;
        char tmpname[MAXTABLELEN];

        dbenv->dbs = realloc(dbenv->dbs, (dbenv->num_dbs + 1) * sizeof(struct db *));

        tok = segtok(line, len, &st, &ltok); /* tbl name */
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Malformed \"table\" directive\n");
            return -1;
        }

        tblname = tokdup(tok, ltok);

        tok = segtok(line, len, &st, &ltok);
        fname = getdbrelpath(tokdup(tok, ltok));

        /* if it's a schema file, allocate a struct db, populate with crap
         * data, then load schema.  if it's an lrl file, we don't support it
         * anymore */

        if (strstr(fname, ".lrl") != 0) {
            logmsg(LOGMSG_ERROR, "this version of comdb2 does not support "
                   "loading another lrl from this lrl\n");
            return -1;
        } else if (strstr(fname, ".csc2") != 0) {
            int dbnum;
            struct db *db;

            bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_GENIDS, 1);

            /* optional dbnum */
            tok = segtok(line, len, &st, &ltok);
            tokcpy(tok, ltok, tmpname);
            if (ltok == 0)
                dbnum = 0;
            else {
                dbnum = toknum(tok, ltok);
                if (dbnum <= 0) {
                    logmsg(LOGMSG_ERROR, "Invalid dbnum entry in \"table\" directive\n");
                    return -1;
                }

                /* if db number matches parent database db number then
                 * table name must match parent database name.  otherwise
                 * we get mysterious failures to receive qtraps (setting
                 * taskname to something not our task name causes initque
                 * to fail, and the ldgblzr papers over this) */
                if (dbnum == dbenv->dbnum &&
                    strcasecmp(gbl_dbname, tblname) != 0) {
                    logmsg(LOGMSG_ERROR, "Table %s has same db number as parent database "
                           "but different name\n",
                           tblname);
                    return -1;
                }
            }
            rc = dyns_load_schema(fname, (char *)gbl_dbname, tblname);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "Error loading %s schema.\n", tok);
                return -1;
            }

            /* create one */
            db = newdb_from_schema(dbenv, tblname, fname, dbnum,
                                   dbenv->num_dbs, 0);
            if (db == NULL) {
                return -1;
            }

            db->dbs_idx = dbenv->num_dbs;
            dbenv->dbs[dbenv->num_dbs++] = db;

            /* Add table to the hash. */
            hash_add(dbenv->db_hash, db);

            /* just got a bunch of data. remember it so key forming
               routines and SQL can get at it */
            if (add_cmacc_stmt(db, 0)) {
                logmsg(LOGMSG_ERROR,
                       "Failed to load schema: can't process schema file %s\n",
                       tok);
                return -1;
            }
        } else {
            logmsg(LOGMSG_ERROR, "Invalid table option\n");
            return -1;
        }
    } else if (tokcmp(tok, ltok, "loghist") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0)
            gbl_loghist = 10000;
        else {
            gbl_loghist = toknum(tok, ltok);
            if (gbl_loghist <= 0) {
                logmsg(LOGMSG_ERROR, "Invalid gbl_loghist value\n");
                return -1;
            }
        }
        logmsg(LOGMSG_INFO, "Enabled request logging: %d requests\n", gbl_loghist);
    } else if (tokcmp(tok, ltok, "repdebug") == 0) {
        /* turn on replication debug messages */
        gbl_repdebug = 1;
    } else if (tokcmp(tok, ltok, "norepdebug") == 0) {
        /* turn off replication debug messages */
        gbl_repdebug = 0;
    } else if (tokcmp(tok, ltok, "loghist_verbose") == 0) {
        gbl_loghist_verbose = 1;
    } else if (tokcmp(tok, ltok, "allow") == 0 ||
               tokcmp(tok, ltok, "disallow") == 0 ||
               tokcmp(tok, ltok, "clrpol") == 0 ||
               tokcmp(tok, ltok, "setclass") == 0) {
        if (dbenv->num_allow_lines >= dbenv->max_allow_lines) {
            dbenv->max_allow_lines += 1;
            dbenv->allow_lines =
                realloc(dbenv->allow_lines,
                        sizeof(char *) * dbenv->max_allow_lines);
            if (!dbenv->allow_lines) {
                logmsg(LOGMSG_ERROR, "out of memory\n");
                return -1;
            }
        }
        dbenv->allow_lines[dbenv->num_allow_lines] =
            strdup(line);
        if (!dbenv->allow_lines[dbenv->num_allow_lines]) {
            logmsg(LOGMSG_ERROR, "out of memory\n");
            return -1;
        }
        dbenv->num_allow_lines++;
    } else if (tokcmp(tok, ltok, "sqlflush") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_INFO, "Expected #records for sqlflush\n");
            return -1;
        }
        gbl_sqlflush_freq = toknum(tok, ltok);
        if (gbl_sqlflush_freq < 0) {
            logmsg(LOGMSG_ERROR, "Invalid flush frequency\n");
            gbl_sqlflush_freq = 0;
            return -1;
        }
    } else if (tokcmp(tok, ltok, "sbuftimeout") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected time values (ms) for sbuftimeout\n");
            return -1;
        }
        gbl_sbuftimeout = toknum(tok, ltok);
        if (gbl_sbuftimeout < 0) {
            gbl_sbuftimeout = 0;
            logmsg(LOGMSG_ERROR, "Invalid sbuf timeout\n");
            return -1;
        }
        set_sbuftimeout(gbl_sbuftimeout);
    } else if (tokcmp(tok, ltok, "throttlesqloverlog") == 0) {
        int secs;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0 || ((secs = toknum(tok, ltok)) < 0)) {
            logmsg(LOGMSG_ERROR, "Expected number of seconds for throttlesqloverlog\n");
        } else {
            gbl_throttle_sql_overload_dump_sec = secs;
            logmsg(LOGMSG_ERROR, 
                   "Set at most one sql overload trace per every %d seconds\n",
                   gbl_throttle_sql_overload_dump_sec);
        }
    }

    else if (tokcmp(tok, ltok, "toblock_net_throttle") == 0) {
        gbl_toblock_net_throttle = 1;
        logmsg(LOGMSG_INFO, "I will throttle my writes in apply_changes\n");
    } else if (tokcmp(tok, ltok, "no_toblock_net_throttle") == 0) {
        gbl_toblock_net_throttle = 0;
        logmsg(LOGMSG_INFO, "I will not throttle my writes in apply_changes\n");
    }

    else if (tokcmp(tok, ltok, "enque_flush_interval") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected time value for enque_flush_interval\n");
            return -1;
        }
        gbl_enque_flush_interval = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "net_set_enque_flush_interval %d\n",
               gbl_enque_flush_interval);
    }

    else if (tokcmp(tok, ltok, "enque_flush_interval_signal") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected time value for enque_flush_interval_signal\n");
            return -1;
        }
        gbl_enque_flush_interval_signal = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "net_set_enque_flush_interval_signal %d\n",
               gbl_enque_flush_interval_signal);
    }

    else if (tokcmp(tok, ltok, "slow_rep_process_txn_maxms") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected value for slow_rep_process_txn_maxms\n");
            return -1;
        }
        gbl_slow_rep_process_txn_maxms = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "slow_rep_process_txn_maxms set to %d ms\n",
               gbl_slow_rep_process_txn_maxms);
    }

    else if (tokcmp(tok, ltok, "slow_rep_process_txn_freq") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected value for slow_rep_process_txn_freq\n");
            return -1;
        }
        gbl_slow_rep_process_txn_freq = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "slow_rep_process_txn_freq set to %d ms\n",
               gbl_slow_rep_process_txn_freq);
    }

    else if (tokcmp(tok, ltok, "enque_reorder_lookahead") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected value for enque_reorder_lookahead\n");
            return -1;
        }
        gbl_enque_reorder_lookahead = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "enque_reorder_lookahead %d\n",
               gbl_enque_reorder_lookahead);
    }

    else if (tokcmp(tok, ltok, "inflatelog") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, 
                   "Inflatelog requires argument (percentage to inflate)\n");
            return -1;
        }
        gbl_inflate_log = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "Will inflate log-stream by %d%%\n", gbl_inflate_log);
    } else if (tokcmp(tok, ltok, "enable_position_apis") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_enable_position_apis = 1;
        logmsg(LOGMSG_INFO, "DB supports position APIs\n");
    } else if (tokcmp(tok, ltok, "use_node_priority") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_use_node_pri = 1;
        logmsg(LOGMSG_INFO, "Node priority is set for the db\n");
    } else if (tokcmp(tok, ltok, "allow_lua_print") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_allow_lua_print = 1;
        logmsg(LOGMSG_INFO, "Allow use of lua print for debugging.\n");
    } else if (tokcmp(tok, ltok, "allow_user_schema") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_allow_user_schema = 1;
        logmsg(LOGMSG_INFO, "Allow user schema.\n");
    } else if (tokcmp(tok, ltok, "return_long_column_names") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_return_long_column_names = 1;
        logmsg(LOGMSG_INFO, "Return long column names.\n");
    } else if (tokcmp(tok, ltok, "debug") == 0) {
        tok = segtok(line, len, &st, &ltok);
        while (ltok > 0) {
            if (tokcmp(tok, ltok, "rtcpu") == 0) {
                logmsg(LOGMSG_INFO, "enable rtcpu debugging\n");
                gbl_rtcpu_debug = 1;
            }
            tok = segtok(line, len, &st, &ltok);
        }
    } else if (tokcmp(tok, ltok, "convflush") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected schema change flush frequency\n");
        } else {
            int num = toknum(tok, ltok);
            gbl_conv_flush_freq = num;
            logmsg(LOGMSG_INFO, "On non-live schema change, flushing on each %d records "
                   "NOTE: THIS OPTION IS CURRENTLY IGNORED\n",
                   num);
        }
    } else if (tokcmp(tok, ltok, "resource") == 0) {
        /* I used to allow a one argument version of resource -
         *   resource <filepath>
         * This wasn't implemented too well and caused problems.  Explicit
         * two argument version is better:
         *   resource <name> <filepath>
         */
        char *name = NULL;
        char *file = NULL;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected resource name\n");
            return -1;
        }
        name = tokdup(tok, ltok);
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected resource path\n");
            free(name);
            return -1;
        }
        file = tokdup(tok, ltok);
        addresource(name, file);
        if (name)
            free(name);
        if (file)
            free(file);
    } else if (tokcmp(tok, ltok, "procedure") == 0) {
        char *name;
        char *jartok;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected stored procedure name\n");
            return -1;
        }
        name = tokdup(tok, ltok);
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected stored procedure jar resource name\n");
            free(name);
            return -1;
        }
        jartok = tokdup(tok, ltok);
        if (javasp_add_procedure(name, jartok, line + st) != 0)
            return -1;
        free(name);
        free(jartok);
    }
    else if (tokcmp(tok, ltok, "repchecksum") == 0) {
        logmsg(LOGMSG_INFO, "replication checksums enabled\n");
        gbl_repchecksum = 1;
    } else if (tokcmp(tok, ltok, "use_live_schema_change") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok > 0)
            gbl_default_livesc = toknum(tok, ltok);
        else
            gbl_default_livesc = 1;
        logmsg(LOGMSG_INFO, "setting gbl_default_livesc = %d\n", gbl_default_livesc);
    } else if (tokcmp(tok, ltok, "use_parallel_schema_change") == 0) {
        gbl_default_sc_scanmode = SCAN_PARALLEL;
        logmsg(LOGMSG_INFO, "using parallel scan mode for schema changes by default\n");
    } else if (tokcmp(tok, ltok, "use_llmeta") == 0) {
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_LLMETA, 1);
        gbl_use_llmeta = 1;
        logmsg(LOGMSG_INFO, "using low level meta table\n");
    } else if (tokcmp(tok, ltok, "use_planned_schema_change") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok > 0)
            gbl_default_plannedsc = toknum(tok, ltok);
        else
            gbl_default_plannedsc = 1;
        logmsg(LOGMSG_INFO, "setting gbl_default_plannedsc = %d\n",
               gbl_default_plannedsc);
    } else if (tokcmp(tok, ltok, "bdboslog") == 0) {
        tok = segtok(line, len, &st, &ltok);
        bdb_set_os_log_level(toknum(tok, ltok));
    } else if (tokcmp(tok, ltok, "queuepoll") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_queue_sleeptime = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting gbl_queue_sleeptime to %d\n", gbl_queue_sleeptime);
    } else if (tokcmp(tok, ltok, "lclpooledbufs") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_lclpooled_buffers = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting gbl_lclpooled_buffers to %d\n",
               gbl_lclpooled_buffers);
    }
    else if (tokcmp(tok, ltok, "maxblockops") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_maxblockops = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting gbl_maxblockops to %d\n", gbl_maxblockops);
    } else if (tokcmp(tok, ltok, "replicate_local") == 0) {
        gbl_replicate_local = 1;
    } else if (tokcmp(tok, ltok, "replicate_local_concurrent") == 0) {
        gbl_replicate_local_concurrent = 1;
    } else if (tokcmp(tok, ltok, "erron") == 0) {
        dbenv->errstaton = 1; /* ON */
        logmsg(LOGMSG_INFO, "db error report turned on\n");
    } else if (tokcmp(tok, ltok, "erroff") == 0) {
        dbenv->errstaton = 0; /* OFF */
        logmsg(LOGMSG_INFO, "db error report turned off\n");
    } else if (tokcmp(tok, ltok, "disable_tagged_api") == 0) {
        logmsg(LOGMSG_INFO, "Disabled tagged api requests.\n");
        gbl_disable_tagged_api = 1;
    } else if (tokcmp(tok, ltok, "enable_tagged_api") == 0) {
        logmsg(LOGMSG_INFO, "Enabled tagged api requests.\n");
        gbl_disable_tagged_api = 0;
    } else if (tokcmp(tok, ltok, "enable_snapshot_isolation") == 0) {
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_SNAPISOL, 1);
        gbl_snapisol = 1;
    } else if (tokcmp(tok, ltok, "enable_new_snapshot") == 0) {
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_SNAPISOL, 1);
        gbl_snapisol = 1;
        gbl_new_snapisol = 1;
        gbl_new_snapisol_logging = 1;
        logmsg(LOGMSG_INFO, "Enabled new snapshot\n");
    } else if (tokcmp(tok, ltok, "enable_new_snapshot_asof") == 0) {
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_SNAPISOL, 1);
        gbl_snapisol = 1;
        gbl_new_snapisol = 1;
        gbl_new_snapisol_asof = 1;
        gbl_new_snapisol_logging = 1;
        logmsg(LOGMSG_INFO, "Enabled new snapshot\n");
    } else if (tokcmp(tok, ltok, "enable_new_snapshot_logging") == 0) {
        gbl_new_snapisol_logging = 1;
        logmsg(LOGMSG_INFO, "Enabled new snapshot logging\n");
    } else if (tokcmp(tok, ltok, "disable_new_snapshot") == 0) {
        gbl_disable_new_snapshot = 1;
        logmsg(LOGMSG_INFO, "Disabled new snapshot\n");
    } else if (tokcmp(tok, ltok, "enable_serial_isolation") == 0) {
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_SNAPISOL, 1);
        gbl_snapisol = 1;
        gbl_selectv_rangechk = 1;
    } else if (tokcmp(tok, ltok, "update_shadows_interval") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_update_shadows_interval = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "setting update_shadows_interval to %d\n",
               gbl_update_shadows_interval);
    } else if (tokcmp(tok, ltok, "enable_lowpri_snapisol") == 0) {
        gbl_lowpri_snapisol_sessions = 1;
    } else if (tokcmp(tok, ltok, "stack_disable") == 0) {
        walkback_disable();
        logmsg(LOGMSG_INFO, "Disabled walkbacks\n");
    } else if (tokcmp(tok, ltok, "stack_enable") == 0) {
        walkback_enable();
        logmsg(LOGMSG_INFO, "Enabled walkbacks\n");
    } else if (tokcmp(tok, ltok, "stack_warn_threshold") == 0) {
        int thresh;
        tok = segtok(line, len, &st, &ltok);
        if (ltok > 0 && (thresh = toknum(tok, ltok)) >= 0) {
            walkback_set_warnthresh(thresh);
            logmsg(LOGMSG_INFO, 
                   "Set walkback warn-threshold to %d walkbacks per second\n",
                   thresh);
        } else {
            logmsg(LOGMSG_ERROR, 
                   "stack_warn_threshold requires a non-negative argument\n");
        }
    } else if (tokcmp(tok, ltok, "disable_lowpri_snapisol") == 0) {
        gbl_lowpri_snapisol_sessions = 0;
    } else if (tokcmp(tok, ltok, "sqlrdtimeout") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_sqlrdtimeoutms = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "SQL read timeout now set to %d ms\n", gbl_sqlrdtimeoutms);
    } else if (tokcmp(tok, ltok, "sqlwrtimeout") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_sqlwrtimeoutms = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "SQL write timeout now set to %d ms\n", gbl_sqlwrtimeoutms);
    } else if (tokcmp(tok, ltok, "log_delete_now") == 0) {
        logmsg(LOGMSG_INFO, "Will delete log files as soon as possible\n");
        dbenv->log_delete_age = 0;
    } else if (tokcmp(tok, ltok, "log_delete_after_backup") == 0) {
        /* epoch time of 1; so we won't delete any logfiles until
         * after backup */
        logmsg(LOGMSG_INFO, "Will delete log files after backup\n");
        dbenv->log_delete_age = 1;
    } else if (tokcmp(tok, ltok, "log_delete_before_startup") == 0) {
        /* delete log files from before we started up */
        logmsg(LOGMSG_INFO, "Will delete log files predating this startup\n");
        dbenv->log_delete_age = (int)time(NULL);
    } else if (tokcmp(tok, ltok, "on") == 0) {
        change_switch(1, line, len, st);
    } else if (tokcmp(tok, ltok, "off") == 0) {
        change_switch(0, line, len, st);
    } else if (tokcmp(tok, ltok, "setattr") == 0) {
        char name[48] = {0}; // oh valgrind
        int value;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "%s:%d: expected attribute name\n", options->lrlname,
                   options->lineno);
            return -1;
        }
        tokcpy0(tok, ltok, name, sizeof(name));
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "%s:%d: expected attribute value\n", options->lrlname,
                   options->lineno);
            return -1;
        }
        value = toknum(tok, ltok);
        if (bdb_attr_set_by_name(NULL, dbenv->bdb_attr, name, value) != 0) {
            logmsg(LOGMSG_ERROR, "%s:%d: bad attribute name %s\n", options->lrlname, options->lineno, name);
        }
    }
    else if (tokcmp(tok, ltok, "reqldiffstat") == 0) {
        tok = segtok(line, len, &st, &ltok);
        reqlog_set_diffstat_thresh(toknum(tok, ltok));
    }

    else if (tokcmp(tok, ltok, "reqltruncate") == 0) {
        tok = segtok(line, len, &st, &ltok);
        reqlog_set_truncate(toknum(tok, ltok));
    }

    else if (tokcmp(tok, ltok, "mallocregions") == 0) {
        if ((strcmp(COMDB2_VERSION, "2") == 0) ||
            (strcmp(COMDB2_VERSION, "old") == 0)) {
            logmsg(LOGMSG_INFO, "Using os-supplied malloc for regions\n");
            berkdb_use_malloc_for_regions();
            gbl_malloc_regions = 1;
        }
    }

    else if (tokcmp(tok, ltok, "use_bbipc_fastseed") == 0) {
        gbl_use_bbipc_global_fastseed = 1;
        logmsg(LOGMSG_INFO, "Using bbipc_global_fastseed.\n");
    } else if (tokcmp(tok, ltok, "dont_use_bbipc_fastseed") == 0) {
        gbl_use_bbipc_global_fastseed = 0;
        logmsg(LOGMSG_INFO, "Disabling bbipc_global_fastseed.\n");
    }

    else if (tokcmp(tok, ltok, "appsockpool") == 0) {
        thdpool_process_message(gbl_appsock_thdpool, line, len,
                                st);
    } else if (tokcmp(tok, ltok, "sqlenginepool") == 0) {
        thdpool_process_message(gbl_sqlengine_thdpool, line, len,
                                st);
    } else if (tokcmp(tok, ltok, "osqlpfaultpool") == 0) {
        thdpool_process_message(gbl_osqlpfault_thdpool, line, len,
                                st);
    } else if (tokcmp(tok, ltok, "updategenids") == 0) {
        gbl_updategenids = 1;
    } else if (tokcmp(tok, ltok, "no_round_robin_stripes") == 0) {
        gbl_round_robin_stripes = 0;
    } else if (tokcmp(tok, ltok, "round_robin_stripes") == 0) {
        gbl_round_robin_stripes = 1;
    } else if (tokcmp(tok, ltok, "exclusive_blockop_qconsume") == 0) {
        logmsg(LOGMSG_INFO, "Blockops and queue consumes will be serialized\n");
        gbl_exclusive_blockop_qconsume = 1;
    } else if (tokcmp(tok, ltok, "chkpoint_alarm_time") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_chkpoint_alarm_time = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "Checkpoint thread hang alarm time is %d seconds\n",
               gbl_chkpoint_alarm_time);
    }

    else if (tokcmp(tok, ltok, "incoherent_msg_freq") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_incoherent_msg_freq = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "Incoherent message freq is %d seconds\n",
               gbl_incoherent_msg_freq);
    } else if (tokcmp(tok, ltok, "incoherent_alarm_time") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_incoherent_alarm_time = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "Incoherent alarm time is %d seconds\n",
               gbl_incoherent_alarm_time);
    } else if (tokcmp(tok, ltok, "max_incoherent_nodes") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_max_incoherent_nodes = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "Max incoherent nodes allowed before alarm is %d\n",
               gbl_max_incoherent_nodes);
    } else if (lrltokignore(tok, ltok) == 0) {
        /* ignore this line */
    } else if (tokcmp(tok, ltok, "osql_heartbeat_send_time") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_INFO, "Need to specify time for heartbeat send\n");
            return -1;
        }
        num = toknum(tok, ltok);
        if (num <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid heartbeat send time\n");
            return -1;
        }
        gbl_osql_heartbeat_send = num;
    } else if (tokcmp(tok, ltok, "osql_heartbeat_alert_time") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify time for heartbeat alert\n");
            return -1;
        }
        num = toknum(tok, ltok);
        if (num <= 0 || gbl_osql_heartbeat_send < num) {
            logmsg(LOGMSG_ERROR, "Invalid heartbeat alert time, need to define "
                   "osql_heartbeat_send_time first\n");
            return -1;
        }
        gbl_osql_heartbeat_alert = num;
    } else if (tokcmp(tok, ltok, "net_lmt_upd_incoherent_nodes") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, 
                   "Need to specify time for replication update threshold\n");
            return -1;
        }
        num = toknum(tok, ltok);
        if (num < 0) {
            logmsg(LOGMSG_ERROR, "Invalid replication update threshold, "
                   "disabling throttling\n");
            return -1;
        }
        gbl_net_lmt_upd_incoherent_nodes = num;
        logmsg(LOGMSG_INFO, "Setting replication update throttling to %d%% of queue "
               "capacity\n",
               gbl_net_lmt_upd_incoherent_nodes);
    } else if (tokcmp(tok, ltok, "report_deadlock_verbose") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0 || toknum(tok, ltok)) {
            gbl_disable_deadlock_trace = 1;
        } else {
            gbl_disable_deadlock_trace = 0;
        }
        logmsg(LOGMSG_INFO, "Deadlock trace %s.\n",
               (gbl_disable_deadlock_trace) ? "disabled" : "enabled");
    } else if (tokcmp(tok, ltok, "disable_pageorder_recsz_check") == 0) {
        logmsg(LOGMSG_INFO, "Disabled pageorder records per page check\n");
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_DISABLE_PAGEORDER_RECSZ_CHK,
                     1);
    } else if (tokcmp(tok, ltok, "enable_pageorder_recsz_check") == 0) {
        logmsg(LOGMSG_INFO, "Enabled pageorder records per page check\n");
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_DISABLE_PAGEORDER_RECSZ_CHK,
                     0);
    } else if (tokcmp(tok, ltok, "disable_overflow_page_trace") == 0) {
        if (gbl_disable_overflow_page_trace) {
            logmsg(LOGMSG_INFO, "Overflow page trace is not enabled\n");
        } else {
            gbl_disable_overflow_page_trace = 1;
            logmsg(LOGMSG_INFO, "Disabled berkdb overflow page trace.\n");
        }
    } else if (tokcmp(tok, ltok, "enable_overflow_page_trace") == 0) {
        if (!gbl_disable_overflow_page_trace) {
            logmsg(LOGMSG_INFO, "Overflow page trace is already enabled\n");
        } else {
            gbl_disable_overflow_page_trace = 0;
            logmsg(LOGMSG_INFO, "Enabled berkdb overflow page trace.\n");
        }
    } else if (tokcmp(tok, ltok, "simulate_rowlock_deadlock") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify rowlock deadlock interval.\n");
            return -1;
        }
        num = toknum(tok, ltok);
        if (num == 0) {
            logmsg(LOGMSG_INFO, "Disabling rowlock_deadlock simulator.\n");
            gbl_simulate_rowlock_deadlock_interval = 0;
        } else if (num < 2) {
            logmsg(LOGMSG_ERROR, "Invalid rowlock_deadlock interval.\n");
        } else {
            logmsg(LOGMSG_INFO, 
                   "Will throw a rowlock deadlock every %d tries.\n", num);
            gbl_simulate_rowlock_deadlock_interval = num;
        }
    } else if (tokcmp(tok, ltok, "debug_rowlocks") == 0) {
        if (gbl_debug_rowlocks) {
            logmsg(LOGMSG_INFO, "Debug-rowlocks flag is already enabled.\n");
        } else {
            gbl_debug_rowlocks = 1;
            logmsg(LOGMSG_INFO, "Enabled debug rowlocks flag.\n");
        }
    } else if (tokcmp(tok, ltok, "nodebug_rowlocks") == 0) {
        if (!gbl_debug_rowlocks) {
            logmsg(LOGMSG_INFO, "Debug-rowlocks flag is already disabled.\n");
        } else {
            gbl_debug_rowlocks = 0;
            logmsg(LOGMSG_INFO, "Disabled debug rowlocks flag.\n");
        }
    } else if (tokcmp(tok, ltok, "enable_selectv_range_check") == 0) {
        gbl_selectv_rangechk = 1;
    } else if (tokcmp(tok, ltok, "disable_rowlock_logging") == 0) {
        gbl_disable_rowlocks_logging = 1;
        logmsg(LOGMSG_INFO, "I will not log any rowlocks records\n");
    } else if (tokcmp(tok, ltok, "disable_rowlock_locking") == 0) {
        gbl_disable_rowlocks = 1;
        logmsg(LOGMSG_INFO, "I will not actually acquire any rowlocks (but will still "
               "follow the codepath)\n");
    } else if (tokcmp(tok, ltok, "rep_process_txn_trace") == 0) {
        gbl_rep_process_txn_time = 1;
    } 
    else if (tokcmp(tok, ltok, "exitalarmsec")==0) {
        int alarmsec;
        tok = segtok(line,len,&st,&ltok);
        alarmsec = toknum(tok,ltok);
        logmsg(LOGMSG_INFO, "Setting exit alarm to %d seconds\n", alarmsec);
        gbl_exit_alarm_sec = alarmsec;
    } else if (tokcmp(tok, ltok, "deadlock_policy_override") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok > 0) {
            extern int gbl_deadlock_policy_override;
            extern char *deadlock_policy_str(int policy);
            gbl_deadlock_policy_override = toknum(tok, ltok);
            logmsg(LOGMSG_INFO, "Set deadlock policy to %s\n",
                   deadlock_policy_str(gbl_deadlock_policy_override));
        } else {
            logmsg(LOGMSG_ERROR, "deadlock_policy_override requires a number argument\n");
        }
    } else if (tokcmp(tok, ltok, "no_rep_process_txn_trace") == 0) {
        gbl_rep_process_txn_time = 0;
    } else if (tokcmp(tok, ltok, "rep_collect_trace") == 0) {
        gbl_rep_collect_txn_time = 1;
    } else if (tokcmp(tok, ltok, "no_rep_collect_trace") == 0) {
        gbl_rep_collect_txn_time = 0;
    } else if (tokcmp(tok, ltok, "net_explicit_flush_trace") == 0) {
        net_enable_explicit_flush_trace();
    } else if (tokcmp(tok, ltok, "no_net_explicit_flush_trace") == 0) {
        net_disable_explicit_flush_trace();
    } else if (tokcmp(tok, ltok, "ack_trace") == 0) {
        enable_ack_trace();
    } else if (tokcmp(tok, ltok, "no_ack_trace") == 0) {
        disable_ack_trace();
    } else if (tokcmp(tok, ltok, "sql_tranlevel_default") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, 
                   "Need to specify default type for sql_tranlevel_default\n");
            return -1;
        }
        if (ltok == 6 && !strncasecmp(tok, "comdb2", 6)) {
            gbl_sql_tranlevel_default = SQL_TDEF_COMDB2;
            logmsg(LOGMSG_INFO, "sql default mode is comdb2\n");

        } else if (ltok == 5 && !strncasecmp(tok, "block", 5)) {
            gbl_sql_tranlevel_default = (gbl_upgrade_blocksql_2_socksql)
                ? SQL_TDEF_SOCK
                : SQL_TDEF_BLOCK;
            logmsg(LOGMSG_INFO, "sql default mode is %s\n",
                   (gbl_sql_tranlevel_default == SQL_TDEF_SOCK)
                   ? "socksql"
                   : "blocksql");
        } else if (ltok == 9 && !strncasecmp(tok, "blocksock", 9) ||
                   tokcmp(tok, ltok, "default") == 0) {
            gbl_upgrade_blocksql_2_socksql = 1;
            if (gbl_sql_tranlevel_default == SQL_TDEF_BLOCK) {
                gbl_sql_tranlevel_default = SQL_TDEF_SOCK;
            }
            logmsg(LOGMSG_INFO, "sql default mode is %s\n",
                   (gbl_sql_tranlevel_default == SQL_TDEF_SOCK)
                   ? "socksql"
                   : "blocksql"
                );
            gbl_use_block_mode_status_code = 0;
        } else if (ltok == 16 &&
                   !strncasecmp(tok, "prefer_blocksock", 16)) {
            gbl_sql_tranlevel_sosql_pref = 1;
            logmsg(LOGMSG_INFO, "prefer socksql over blocksql\n");

        } else if (ltok == 5 && !strncasecmp(tok, "recom", 5)) {
            gbl_sql_tranlevel_default = SQL_TDEF_RECOM;
            logmsg(LOGMSG_INFO, "sql default mode is read committed\n");

        } else if (ltok == 8 && !strncasecmp(tok, "snapshot", 8)) {
            gbl_sql_tranlevel_default = SQL_TDEF_SNAPISOL;
            logmsg(LOGMSG_INFO, "sql default mode is snapshot isolation\n");

        } else if (ltok == 6 && !strncasecmp(tok, "serial", 6)) {
            gbl_sql_tranlevel_default = SQL_TDEF_SERIAL;
            logmsg(LOGMSG_INFO, "sql default mode is serializable\n");

        } else {
            logmsg(LOGMSG_ERROR, "The default sql mode \"%s\" is not supported, "
                   "defaulting to socksql\n",
                   tok);
            gbl_sql_tranlevel_default = SQL_TDEF_SOCK;
        }
        gbl_sql_tranlevel_preserved = gbl_sql_tranlevel_default;
    } else if (tokcmp(tok, ltok, "proxy") == 0) {
        char *proxy_line;
        tok = segline(line, len, &st, &ltok);
        if (ltok > 0) {
            proxy_line = tokdup(tok, ltok);
            handle_proxy_lrl_line(proxy_line);
            free(proxy_line);
        }
    }

    else if (tokcmp(tok, ltok, "sql_time_threshold") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected numeric threshold for sql_time_threshold\n");
            return -1;
        }
        gbl_sql_time_threshold = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "Set SQL warning threshold time to %dms\n",
               gbl_sql_time_threshold);
    } else if (tokcmp(tok, ltok, "blocksql_throttle") == 0) {
        int tmp = 0;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected numeric threshold for blocksql_throttle\n");
            return -1;
        }
        tmp = toknum(tok, ltok);
        logmsg(LOGMSG_INFO, "Set blocksql maximum concurrent in-transaction sessions to "
               "%d sessions\n",
               tmp);
        osql_bplog_setlimit(tmp);
    } else if (tokcmp(tok, ltok, "force_highslot") == 0) {
        gbl_force_highslot = 1;
    } else if (tokcmp(tok, ltok, "num_contexts") == 0) {
        int n;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected number of contexts for num_contexts\n");
            return -1;
        }
        n = toknum(tok, ltok);
        if (n <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid number of contexts\n");
            return -1;
        }
        gbl_num_contexts = n;
    } else if (tokcmp(tok, ltok, "buffers_per_context") == 0) {
        int n;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected number of buffers for buffers_per_context\n");
            return -1;
        }
        n = toknum(tok, ltok);
        if (n <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid number of buffers\n");
            return -1;
        }
        gbl_buffers_per_context = n;
    } else if (tokcmp(tok, ltok, "allow_mismatched_tag_size") == 0) {
        logmsg(LOGMSG_INFO, "Allowing mismatched tag size\n");
        gbl_allow_mismatched_tag_size = 1;
    } else if (tokcmp(tok, ltok, "nowatch") == 0) {
        logmsg(LOGMSG_INFO, "disabling watcher thread\n");
        gbl_watchdog_disable_at_start = 1;
    } else if (tokcmp(tok, ltok, "watchthreshold") == 0) {
        int tmp = 0;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected watch threshold\n");
            return -1;
        }
        tmp = toknum(tok, ltok);
        if (tmp > 0) {
            gbl_watchdog_watch_threshold = tmp;
            logmsg(LOGMSG_INFO, "Set watchdog watch-time to %d\n", gbl_watchdog_watch_threshold);
        }
        else {
            logmsg(LOGMSG_INFO, "Expected positive value for watchdog watch-threshold\n");
        }
    } else if (tokcmp(tok, ltok, "enable_blockoffload") == 0) {
        logmsg(LOGMSG_INFO, "enabling block offload.\n");
        gbl_enable_block_offload = 1;
    } else if (tokcmp(tok, ltok, "page_latches") == 0) {
        logmsg(LOGMSG_INFO, "enabled page latches\n");
        gbl_page_latches = 1;
    } else if (tokcmp(tok, ltok, "replicant_latches") == 0) {
        logmsg(LOGMSG_INFO, "enabled latches on replicant\n");
        gbl_replicant_latches = 1;
    } else if (tokcmp(tok, ltok, "disable_page_latches") == 0) {
        logmsg(LOGMSG_INFO, "disabled page latches\n");
        gbl_page_latches = 0;
    } else if (tokcmp(tok, ltok, "disable_replicant_latches") == 0) {
        logmsg(LOGMSG_INFO, "disabled replicant page latches\n");
        gbl_replicant_latches = 0;
    } else if (tokcmp(tok, ltok, "notimeout") == 0) {
        logmsg(LOGMSG_INFO, "disabling timeouts\n");
        gbl_notimeouts = 1;
    } else if (tokcmp(tok, ltok, "enable_cursor_ser") == 0) {
        logmsg(LOGMSG_INFO, "enabling cursor serialization\n");
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_ENABLECURSORSER, 1);
    } else if (tokcmp(tok, ltok, "enable_cursor_pause") == 0) {
        logmsg(LOGMSG_INFO, "enabling cursor pause\n");
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_ENABLECURSORPAUSE, 1);
    } else if (tokcmp(tok, ltok, "setsqlattr") == 0) {
        char *attrname = NULL;
        char *attrval = NULL;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected sql attribute name\n");
            return -1;
        }
        attrname = tokdup(tok, ltok);
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected sql attribute name\n");
            free(attrname);
            return -1;
        }
        attrval = tokdup(tok, ltok);
        sqlite3_set_tunable_by_name(attrname, attrval);
        free(attrname);
        free(attrval);
    } else if (tokcmp(tok, ltok, "track_berk_locks") == 0) {
        logmsg(LOGMSG_INFO, "Will track berkdb locks\n");
        gbl_berkdb_track_locks = 1;
    } else if (tokcmp(tok, ltok, "master_swing_osql_verbose") == 0) {
        logmsg(LOGMSG_INFO, "Will spew osql actions in swings\n");
        gbl_master_swing_osql_verbose = 1;
    } else if (tokcmp(tok, ltok, "debugthreads") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "debugthreads: expected on or off\n");
            return -1;
        }
        if (tokcmp(tok, ltok, "on") == 0) {
            thread_util_enable_debug();
        } else if (tokcmp(tok, ltok, "off") == 0) {
            thread_util_disable_debug();
        } else {
            logmsg(LOGMSG_ERROR, "debugthreads: expected on or off\n");
            return -1;
        }
    } else if (tokcmp(tok, ltok, "dumpthreadonexit") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "dumpthreadonexit: expected on or off\n");
            return -1;
        }
        if (tokcmp(tok, ltok, "on") == 0) {
            thread_util_dump_on_exit_enable();
        } else if (tokcmp(tok, ltok, "off") == 0) {
            thread_util_dump_on_exit_disable();
        } else {
            logmsg(LOGMSG_ERROR, "dumpthreadonexit: expected on or off\n");
            return -1;
        }
    } else if (tokcmp(tok, ltok, "rangextlim") == 0) {
        int num;

        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected limit\n");
            return -1;
        }
        num = toknum(tok, ltok);
        if (num < 0) {
            logmsg(LOGMSG_ERROR, "Invalid limit\n");
            return -1;
        } else if (num == 0) {
            logmsg(LOGMSG_INFO, "Disabled old range extract per-buffer limits\n");
        } else {
            logmsg(LOGMSG_INFO, "Old rangeextract per-buffer limit set to %d records\n",
                   num);
            gbl_rangextunit = num;
        }
    } else if (tokcmp(tok, ltok, "num_record_converts") == 0) {
        int n;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected number of record converts in single "
                   "transaction.\n");
            return -1;
        }
        n = toknum(tok, ltok);
        if (n <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid number of record converts \n");
            return -1;
        }
        gbl_num_record_converts = n;
    } else if (tokcmp(tok, ltok, "oldrangexlim") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected on/off for oldrangexlim\n");
            return -1;
        } else if (tokcmp(tok, ltok, "on") == 0) {
            logmsg(LOGMSG_INFO, "Will honor limit for old rangextract calls\n");
            gbl_honor_rangextunit_for_old_apis = 1;
        } else if (tokcmp(tok, ltok, "off") == 0) {
            logmsg(LOGMSG_INFO, "Won't honor limit for old rangextract calls\n");
            gbl_honor_rangextunit_for_old_apis = 0;
        } else {
            logmsg(LOGMSG_ERROR, "Expected on/off for oldrangexlim\n");
            return -1;
        }
    } else if (tokcmp(tok, ltok, "maxcolumns") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected a column limit value\n");
            return -1;
        }
        gbl_max_columns_soft_limit = toknum(tok, ltok);
        if (gbl_max_columns_soft_limit <= 0 ||
            gbl_max_columns_soft_limit > MAXCOLUMNS) {
            logmsg(LOGMSG_ERROR, 
                   "Bad value for maxcolumns soft limit - must be 1..%d\n",
                   MAXCOLUMNS);
            return -1;
        }
    } else if (tokcmp(tok, ltok, "null_blob_fix") == 0) {
        gbl_disallow_null_blobs = 1;
        logmsg(LOGMSG_INFO, "Will check null constraint for blobs\n");
    } else if (tokcmp(tok, ltok, "static_tag_blob_fix") == 0) {
        gbl_force_notnull_static_tag_blobs = 1;
        logmsg(LOGMSG_INFO, "Will force static-tag blobs to be not-null.\n");
    } else if (tokcmp(tok, ltok, "enable_partial_indexes") == 0) {
        gbl_partial_indexes = 1;
        logmsg(LOGMSG_INFO, "Enabled partial indexes\n");
    } else if (tokcmp(tok, ltok, "disable_partial_indexes") == 0) {
        gbl_partial_indexes = 0;
        logmsg(LOGMSG_INFO, "Disabled partial indexes\n");
    } else if (tokcmp(tok, ltok, "enable_good_sql_return_codes") == 0) {
        if (!gbl_enable_good_sql_return_codes) {
            gbl_enable_good_sql_return_codes = 1;
            logmsg(LOGMSG_INFO, "Enabled good sql return codes\n");
        } else
            logmsg(LOGMSG_INFO, "Good sql return codes already enabled\n");
    } else if (tokcmp(tok, ltok, "disable_good_sql_return_codes") == 0) {
        if (gbl_enable_good_sql_return_codes) {
            gbl_enable_good_sql_return_codes = 0;
            logmsg(LOGMSG_INFO, "Disabled good sql return codes\n");
        } else
            logmsg(LOGMSG_INFO, "Good sql return codes already disabled\n");
    } else if (tokcmp(tok, ltok, "querylimit") == 0) {
        rc = query_limit_cmd(line, len, st);
        if (rc)
            return -1;
    } else if (tokcmp(tok, ltok, "maxretries") == 0) {
        int n;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_INFO, "Expected argument for maxretries\n");
            return -1;
        }
        n = toknum(tok, ltok);
        if (n < 2) {
            logmsg(LOGMSG_INFO, "Invalid setting for maxretries\n");
            return -1;
        }
        gbl_maxretries = n;
        logmsg(LOGMSG_INFO, "Set max retries to %d\n", gbl_maxretries);
    } else if (tokcmp(tok, ltok, "maxblobretries") == 0) {
        int n;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_INFO, "Expected argument for maxblobretries\n");
            return -1;
        }
        n = toknum(tok, ltok);
        if (n < 2) {
            logmsg(LOGMSG_ERROR, "Invalid setting for maxblobretries\n");
            return -1;
        }
        gbl_maxblobretries = n;
        logmsg(LOGMSG_INFO, "Set max blob retries to %d\n", gbl_maxblobretries);
    }

    else if (tokcmp(tok, ltok, "deadlock_rep_retry_max") == 0) {
        int lim;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected value for deadlock limit\n");
            return 0;
        }
        lim = toknum(tok, ltok);
        if (lim <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid value for deadlock limit\n");
            return 0;
        }
        berkdb_set_max_rep_retries(lim);
        logmsg(LOGMSG_INFO, "Set deadlock retry limit to %d\n", lim);
    } else if (tokcmp(tok, ltok, "enable_berkdb_retry_deadlock_bias") ==
               0) {
        gbl_enable_berkdb_retry_deadlock_bias = 1;
    } else if (tokcmp(tok, ltok, "enable_sparse_lockerid_map") == 0) {
        gbl_sparse_lockerid_map = 1;
        logmsg(LOGMSG_INFO, "Enabled sparse lockerid map.\n");
    } else if (tokcmp(tok, ltok, "disable_sparse_lockerid_map") == 0) {
        gbl_sparse_lockerid_map = 0;
        logmsg(LOGMSG_INFO, "Disabled sparse lockerid map.\n");
    } else if (tokcmp(tok, ltok, "enable_inplace_blobs") == 0) {
        gbl_inplace_blobs = 1;
        logmsg(LOGMSG_INFO, "Enabled inplace blobs.\n");
    } else if (tokcmp(tok, ltok, "disable_inplace_blobs") == 0) {
        gbl_inplace_blobs = 0;
        logmsg(LOGMSG_INFO, "Disabled inplace blobs.\n");
    }
    /* Careful!  This changes the data-format.. */
    else if (tokcmp(tok, ltok, "enable_inplace_blob_optimization") == 0) {
        gbl_inplace_blob_optimization = 1;
        logmsg(LOGMSG_INFO, "Enabled optimized inplace blobs.\n");
    } else if (tokcmp(tok, ltok, "pagedeadlock_maxpoll") == 0) {
        int cnt;
        tok = segtok(line, len, &st, &ltok);

        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected value for pagedeadlock_maxpoll\n");
            return 0;
        }
        cnt = toknum(tok, ltok);
        if (cnt < 0) {
            logmsg(LOGMSG_ERROR, "Invalid value for pagedeadlock_maxpoll\n");
            return 0;
        }

        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGEDEADLOCK_MAXPOLL, cnt);
        logmsg(LOGMSG_INFO, "Set pagedeadlock maxpoll limit to %d\n", cnt);
    } else if (tokcmp(tok, ltok, "pagedeadlock_retries") == 0) {
        int cnt;
        tok = segtok(line, len, &st, &ltok);

        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected value for pagedeadlock_retries\n");
            return 0;
        }
        cnt = toknum(tok, ltok);
        if (cnt <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid value for pagedeadlock_retries\n");
            return 0;
        }

        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGEDEADLOCK_RETRIES, cnt);
        logmsg(LOGMSG_INFO, "Set pagedeadlock deadlock retry count to %d\n", cnt);
    } else if (tokcmp(tok, ltok, "disable_inplace_blob_optimization") ==
               0) {
        gbl_inplace_blob_optimization = 0;
        logmsg(LOGMSG_INFO, "Disabled optimized inplace blobs.\n");
    } else if (tokcmp(tok, ltok, "enable_osql_blob_optimization") == 0) {
        gbl_osql_blob_optimization = 1;
        logmsg(LOGMSG_INFO, "Enable osql optimized blobs.\n");
    } else if (tokcmp(tok, ltok, "disable_osql_blob_optimization") == 0) {
        gbl_osql_blob_optimization = 0;
        logmsg(LOGMSG_INFO, "Disabled osql optimized blobs.\n");
    } else if (tokcmp(tok, ltok, "enable_cache_internal_nodes") == 0) {
        gbl_enable_cache_internal_nodes = 1;
        logmsg(LOGMSG_INFO, "Will increase cache-priority for btree internal nodes.\n");
    } else if (tokcmp(tok, ltok, "disable_cache_internal_nodes") == 0) {
        gbl_enable_cache_internal_nodes = 0;
        logmsg(LOGMSG_INFO, "Will treat btree internal nodes the same as leaf-nodes.\n");
    } else if (tokcmp(tok, ltok, "fkrcode") == 0) {
        gbl_fkrcode = 1;
    } else if (tokcmp(tok, ltok, "analyze_tbl_threads") == 0) {
        int thds;
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected value for analyze_tbl_threads\n");
            return 0;
        }
        thds = toknum(tok, ltok);
        rc = analyze_set_max_table_threads(thds);
        if (-1 == rc)
            return -1;
    } else if (tokcmp(tok, ltok, "analyze_comp_threads") == 0) {
        int thds;
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected value for analyze_comp_threads\n");
            return 0;
        }
        thds = toknum(tok, ltok);
        rc = analyze_set_max_sampling_threads(thds);
        if (-1 == rc)
            return -1;
    } else if (tokcmp(tok, ltok, "analyze_comp_threshold") == 0) {
        int thresh;
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected value for analyze_comp_threshold\n");
            return 0;
        }
        thresh = toknum(tok, ltok);
        rc = analyze_set_sampling_threshold(thresh);
        if (-1 == rc)
            return -1;
    } else if (tokcmp(tok, ltok, "print_syntax_err") == 0) {
        gbl_print_syntax_err = 1;
    } else if (tokcmp(tok, ltok, "survive_n_master_swings") == 0) {
        int tmp;
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected value for survive_n_master_swings\n");
            return 0;
        }
        tmp = toknum(tok, ltok);
        if (tmp >= 0) {
            gbl_survive_n_master_swings = tmp;
        }
    } else if (tokcmp(tok, ltok, "master_retry_poll_ms") == 0) {
        int tmp;
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected value for master_retry_poll_ms\n");
            return 0;
        }
        tmp = toknum(tok, ltok);
        if (tmp >= 0) {
            gbl_master_retry_poll_ms = tmp;
        }
    } else if (tokcmp(tok, ltok, "random_lock_release_interval") == 0) {
        int tmp;
        tok = segtok(line, len, &st, &ltok);

        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected value for random_release_locks_interval\n");
            return 0;
        }

        tmp = toknum(tok, ltok);
        if (tmp >= 0) {
            logmsg(LOGMSG_INFO, "Will release locks randomly an average once every %d "
                   "checks\n",
                   tmp);
            gbl_sql_random_release_interval = tmp;
        } else {
            logmsg(LOGMSG_INFO, "Disabled random release-locks\n");
            gbl_sql_random_release_interval = 0;
        }
    } else if (tokcmp(tok, ltok, "osql_verify_retry_max") == 0) {
        int tmp;
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected value for blocksql_verify_retry_max\n");
            return 0;
        }
        tmp = toknum(tok, ltok);
        if (tmp >= 0) {
            logmsg(LOGMSG_INFO, "Osql transaction will repeat %d times if verify error "
                   "(was %d times)\n",
                   tmp, gbl_osql_verify_retries_max);
            gbl_osql_verify_retries_max = tmp;
        }
    } else if (tokcmp(tok, ltok, "osql_verify_ext_chk") == 0) {
        int tmp;
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected value for osql_verify_ext_chk\n");
            return 0;
        }
        tmp = toknum(tok, ltok);
        if (tmp >= 0) {
            logmsg(LOGMSG_INFO, "Osql will do extended genid-checking after %d verify "
                   "errors (was %d)\n",
                   tmp, gbl_osql_verify_ext_chk);
            gbl_osql_verify_ext_chk = tmp;
        }
    } else if (tokcmp(tok, ltok, "badwrite_intvl") == 0) {
        int tmp;
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected value for badwrite_intvl\n");
            return 0;
        }
        tmp = toknum(tok, ltok);
        if (tmp >= 0) {
            logmsg(LOGMSG_INFO, "Will force a bad-write and abort randomly every %d "
                   "pgwrites.\n",
                   tmp);
            gbl_test_badwrite_intvl = tmp;
        } else {
            logmsg(LOGMSG_ERROR, "Invalid badwrite_intvl.\n");
        }
    } else if (tokcmp(tok, ltok, "test_blob_race") == 0) {
        int tmp;
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected value for test_blob_race\n");
            return 0;
        }
        tmp = toknum(tok, ltok);
        if (tmp >= 1) {
            logmsg(LOGMSG_INFO, 
                   "Will force a blob-race condition once every %d lookups.\n",
                   tmp);
            gbl_test_blob_race = tmp;
        } else if (tmp == 0) {
            logmsg(LOGMSG_INFO, "Disabled blob-race testing.\n");
            gbl_test_blob_race = 0;
        } else {
            logmsg(LOGMSG_ERROR, "Invalid test_blob_race value: should be above 2 (or 0 "
                   "to disable).\n");
        }
    } else if (tokcmp(tok, ltok, "iopool") == 0) {
        berkdb_iopool_process_message(line, len, st);
    } else if (tokcmp(tok, ltok, "pageordertablescan") == 0) {
        logmsg(LOGMSG_INFO, "Enabled page-order tablescans.\n");
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGE_ORDER_TABLESCAN, 1);
    } else if (tokcmp(tok, ltok, "tablescan_cache_utilization") == 0) {
        tok = segtok(line, len, &st, &ltok);
        ii = toknum(tok, ltok);
        if (ii < 0 || ii > 100) {
            logmsg(LOGMSG_ERROR, 
                   "Max tablescan cache should be between 0 and 100.\n");
        } else {
            logmsg(LOGMSG_INFO, "Set max tablescan cache utilization to %d.\n",
                   ii);
            bdb_attr_set(dbenv->bdb_attr,
                         BDB_ATTR_TABLESCAN_CACHE_UTILIZATION, ii);
        }
    } else if (tokcmp(tok, ltok, "prefix_foreign_keys") == 0) {
        gbl_fk_allow_prefix_keys = 1;
    } else if (tokcmp(tok, ltok, "superset_foreign_keys") == 0) {
        gbl_fk_allow_superset_keys = 1;
    } else if (tokcmp(tok, ltok, "blocksql_over_sockets") == 0) {
        gbl_upgrade_blocksql_to_socksql = 1;
    } else if (tokcmp(tok, ltok, "noblocksql_over_sockets") == 0) {
        gbl_upgrade_blocksql_to_socksql = 0;
    } else if (tokcmp(tok, ltok, "exit_on_internal_failure") == 0) {
        gbl_exit_on_internal_error = 1;
    } else if (tokcmp(tok, ltok, "no_exit_on_internal_failure") == 0) {
        gbl_exit_on_internal_error = 0;
    } else if (tokcmp(tok, ltok, "reject_osql_mismatch") == 0) {
        gbl_reject_osql_mismatch = 1;
    } else if (tokcmp(tok, ltok, "accept_osql_mismatch") == 0) {
        gbl_reject_osql_mismatch = 0;
    } else if (tokcmp(tok, ltok, "abort_on_in_use_rqid") == 0) {
        gbl_abort_on_clear_inuse_rqid = 1;
    } else if (tokcmp(tok, ltok, "abort_on_missing_session") == 0) {
        gbl_abort_on_missing_session = 1;
    } else if (tokcmp(tok, ltok, "dont_abort_on_missing_session") == 0) {
        gbl_abort_on_missing_session = 0;
    } else if (tokcmp(tok, ltok, "dont_abort_on_in_use_rqid") == 0) {
        gbl_abort_on_clear_inuse_rqid = 0;
    } else if (tokcmp(tok, ltok, "parallel_recovery") == 0) {
        int nthreads;
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for parallel_recovery.\n");
            return 0;
        }
        nthreads = toknum(tok, ltok);
        if (nthreads < 0) {
            logmsg(LOGMSG_ERROR, "Invalid # for parallel_recovery.\n");
            return 0;
        }
        gbl_parallel_recovery_threads = nthreads;
        /* we can't call bdb_set_parallel_recovery_threads because there
         * is no dbenv set up yet */
    } else if (tokcmp(tok, ltok, "update_delete_limit") == 0) {
        gbl_update_delete_limit = 1;
    } else if (tokcmp(tok, ltok, "no_update_delete_limit") == 0) {
        gbl_update_delete_limit = 0;
    } else if (tokcmp(tok, ltok, "disable_bbipc") == 0) {
        gbl_use_bbipc = 0;
    } else if (tokcmp(tok, ltok, "goslow") == 0) {
        gbl_goslow = 1;
    } else if (tokcmp(tok, ltok, "early") == 0) {
        gbl_early = 1;
    } else if (tokcmp(tok, ltok, "noearly") == 0) {
        gbl_early = 0;
    } else if (tokcmp(tok, ltok, "reallearly") == 0) {
        gbl_reallyearly = 1;
    } else if (tokcmp(tok, ltok, "noreallearly") == 0) {
        gbl_reallyearly = 0;
    } else if (tokcmp(tok, ltok, "noudp") == 0) {
        gbl_udp = 0;
    } else if (tokcmp(tok, ltok, "udp") == 0) {
        gbl_udp = 1;
    } else if (tokcmp(tok, ltok, "slowfget") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for slowfget.\n");
            return 0;
        }
        __slow_memp_fget_ns = toknum(tok, ltok);
    } else if (tokcmp(tok, ltok, "slowread") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for slowfget.\n");
            return 0;
        }
        __slow_read_ns = toknum(tok, ltok);
    } else if (tokcmp(tok, ltok, "slowwrite") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for slowfget.\n");
            return 0;
        }
        __slow_write_ns = toknum(tok, ltok);
    } else if (tokcmp(tok, ltok, "ctrace_rollat") == 0) {
        int n;
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for ctrace_rollat.\n");
            return 0;
        }
        n = toknum(tok, ltok);
        ctrace_set_rollat(n);
    } else if (tokcmp(tok, ltok, "ctrace_gzip") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for ctrace_gzip.\n");
            return 0;
        }
        toknum(tok, ltok);
    } else if (tokcmp(tok, ltok, "ctrace_nlogs") == 0) {
        int n;
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for ctrace_nlogs.\n");
            return 0;
        }
        n = toknum(tok, ltok);
        ctrace_set_nlogs(n);
    } else if (tokcmp(tok, ltok, "ctrace_dbdir") == 0) {
        /* put the ctrace log in database directory */
        gbl_ctrace_dbdir = 1;
    } else if (tokcmp(tok, ltok, "disable_sql_dlmalloc") == 0) {
        gbl_disable_sql_dlmalloc = 1;
    } else if (tokcmp(tok, ltok, "epochms_repts") == 0) {
        gbl_berkdb_epochms_repts = 1;
    } else if (tokcmp(tok, ltok, "no_epochms_repts") == 0) {
        gbl_berkdb_epochms_repts = 0;
    } else if (tokcmp(tok, ltok, "decimal_rounding") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok > 0 && tok[0]) {
            gbl_decimal_rounding = dec_parse_rounding(tok, ltok);
            logmsg(LOGMSG_INFO, "Default decimal rounding is %s\n",
                   dec_print_mode(gbl_decimal_rounding));
        } else {
            logmsg(LOGMSG_ERROR, 
                   "Missing option for decimal rounding, current is %s\n",
                   dec_print_mode(gbl_decimal_rounding));
        }
    } else if (tokcmp(tok, ltok, "mempget_timeout") == 0) {
        extern int __gbl_max_mpalloc_sleeptime;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected value for mempget_timeout\n");
            return -1;
        }
        __gbl_max_mpalloc_sleeptime = toknum(tok, ltok);
    } else if (tokcmp(tok, ltok, "berkattr") == 0) {
        char *attr = NULL, *value = NULL;
        tok = segtok(line, len, &st, &ltok);
        if (tok) {
            attr = tokdup(tok, ltok);
            tok = segtok(line, len, &st, &ltok);
            if (tok) {
                value = tokdup(tok, ltok);
            }
        }
        if (attr && value)
            bdb_berkdb_set_attr_after_open(dbenv->bdb_attr, attr, value,
                                           atoi(value));
        free(attr);
        free(value);
    } else if (tokcmp(line, ltok, "keycompr") == 0) {
        if (gbl_keycompr != 1) {
            logmsg(LOGMSG_INFO, "ENABLE KEY COMPRESSION\n");
            gbl_keycompr = 1;
        }
    } else if (tokcmp(line, ltok, "nokeycompr") == 0) {
        if (gbl_keycompr != 0) {
            logmsg(LOGMSG_INFO, "DISABLE KEY COMPRESSION\n");
            gbl_keycompr = 0;
        }
    } else if (tokcmp(tok, ltok, "crypto") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_FATAL, "EXPECTED CRYPTO PATH");
            exit(1);
        }
        if (gbl_crypto != NULL) {
            logmsg(LOGMSG_FATAL, "ALREADY HAVE CRYPTO");
            exit(1);
        }
        gbl_crypto = tokdup(tok, ltok);
    } else if (tokcmp(tok, ltok, "crc32c") == 0) {
        if (gbl_crc32c != 1) {
            logmsg(LOGMSG_INFO, "CRC32C FOR CHECKSUMS\n");
            gbl_crc32c = 1;
        }
    } else if (tokcmp(tok, ltok, "nocrc32c") == 0) {
        if (gbl_crc32c != 0) {
            logmsg(LOGMSG_INFO, "DISABLE CRC32C FOR CHECKSUMS\n");
            gbl_crc32c = 0;
        }
    } else if (tokcmp(line, ltok, "nosurprise") == 0 ||
               tokcmp(line, ltok, "natural_types") == 0) {
        gbl_surprise = 0;
    } else if (tokcmp(line, ltok, "surprise") == 0 ||
               tokcmp(line, ltok, "unnatural_types") == 0) {
        gbl_surprise = 1;
    } else if (tokcmp(tok, ltok, "rcache") == 0) {
        gbl_rcache = true;
    } else if (tokcmp(tok, ltok, "norcache") == 0) {
        gbl_rcache = false;
    } else if (tokcmp(line, ltok, "broken_max_rec_sz") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_broken_max_rec_sz = toknum(tok, ltok);
        if (gbl_broken_max_rec_sz <= 0 || gbl_broken_max_rec_sz >= 1024)
            gbl_broken_max_rec_sz = 512;
        logmsg(LOGMSG_INFO, "Allow db to start with max record size of %d\n",
               COMDB2_MAX_RECORD_SIZE + gbl_broken_max_rec_sz);
    } else if (tokcmp(line, ltok, "bbenv") == 0) {
        gbl_bbenv = 1;
    } else if (tokcmp(line, ltok, "noenv_messages") == 0) {
        gbl_noenv_messages = 1;
    } else if (tokcmp(line, ltok, "env_messages") == 0) {
        gbl_noenv_messages = 0;
    } else if (tokcmp(line, ltok, "nodeid") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected node number for eid\n");
            return -1;
        }
        gbl_mynodeid = toknum(tok, ltok);
    } else if (tokcmp(line, ltok, "skip_clear_queue_extents") == 0) {
        skip_clear_queue_extents = 1;
    } else if (tokcmp(line, ltok, "sqllogger") == 0) {
        /* This is one of several things we can't do until we have more of
         * an environment set up.
         * What would be nice is if processing options was decoupled from
         * reading files, so we
         * could build a list of deferred options and call process_lrl_line
         * on them one by one.
         * One day. No, pre_read_lrl_file isn't what I want. */
        defer_option(dbenv, DEFERRED_SEND_COMMAND, line, len, options->lineno);
    } else if (tokcmp(line, ltok, "no_sc_inco_chk") == 0) {
        gbl_sc_inco_chk = 0;
    } else if (tokcmp(line, ltok, "location") == 0) {
        /* ignore - these are processed by init_file_locations */
    } else if (tokcmp(tok, ltok, "include") == 0) {
        char *file;
        struct lrlfile *lrlfile;

        tok = segtok(line, len, &st, &ltok);
        if (tok == NULL) {
            logmsg(LOGMSG_ERROR, "expected file after include\n");
            return -1;
        }
        file = tokdup(tok, ltok);

        LISTC_FOR_EACH(&dbenv->lrl_files, lrlfile, lnk) {
            if (strcmp(lrlfile->file, file) == 0) {
                logmsg(LOGMSG_ERROR, "Attempted to nest includes for %s\n", file);
                LISTC_FOR_EACH(&dbenv->lrl_files, lrlfile, lnk) {
                    logmsg(LOGMSG_ERROR, ">> %s\n", lrlfile->file);
                }
                free(file);
                return -1;
            }
        }

        read_lrl_file(dbenv, file, dbenv->envname, 0);
    } else if (tokcmp(tok, ltok, "plugin") == 0) {
        rc = process_plugin_command(dbenv, line, len, st, ltok);
        if (rc)
            return -1;

    } else if (tokcmp(line, ltok, "temptable_limit") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for temptable_limit.\n");
            return 0;
        }
        gbl_temptable_pool_capacity = toknum(tok, ltok);
    } else if (tokcmp(line, ltok, "disable_temptable_pool") == 0) {
        gbl_temptable_pool_capacity = 0;
    } else if (tokcmp(line, ltok, "enable_upgrade_ahead") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0)
            gbl_num_record_upgrades = 32;
        else
            gbl_num_record_upgrades = toknum(tok, ltok);
    } else if (tokcmp(line, ltok, "disable_upgrade_ahead") == 0) {
        gbl_num_record_upgrades = 0;
    } else if (tokcmp(line, ltok, "do") == 0) {
        defer_option(dbenv, DEFERRED_SEND_COMMAND, line, len, options->lineno);
    } else if (tokcmp(line, ltok, "memstat_autoreport_freq") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for memstat_autoreport_freq.\n");
            return 0;
        }
        gbl_memstat_freq = toknum(tok, ltok);
    } else if (tokcmp(line, ltok, "page_compact_target_ff") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_pg_compact_target_ff =
            (ltok <= 0) ? 0.693 : (toknumd(tok, ltok) / 100.0F);
    } else if (tokcmp(line, ltok, "page_compact_thresh_ff") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_pg_compact_thresh =
            (ltok <= 0) ? 0.346 : (toknumd(tok, ltok) / 100.0F);
    } else if (tokcmp(line, ltok, "disable_page_compact") == 0) {
        gbl_pg_compact_thresh = 0;
    } else if (tokcmp(line, ltok, "enable_page_compact_backward_scan") ==
               0) {
        gbl_disable_backward_scan = 0;
    } else if (tokcmp(line, ltok, "disable_page_compact_backward_scan") ==
               0) {
        gbl_disable_backward_scan = 1;
    } else if (tokcmp(line, ltok, "compress_page_compact_log") == 0) {
        gbl_compress_page_compact_log = 1;
    } else if (tokcmp(line, ltok, "no_compress_page_compact_log") == 0) {
        gbl_compress_page_compact_log = 0;
    } else if (tokcmp(line, ltok, "max_num_compact_pages_per_txn") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, 
                   "Expected # for max_num_compact_pages_per_txn.\n");
            return 0;
        }
        gbl_max_num_compact_pages_per_txn = (unsigned int)toknum(tok, ltok);
    } else if (tokcmp(line, ltok, "page_compact_latency_ms") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for page_compact_latency_ms\n");
            return 0;
        }
        gbl_pg_compact_latency_ms = toknum(tok, ltok);
    } else if (tokcmp(line, ltok, "blob_mem_mb") == 0) {
        // long_blk_and_blob_mem_mb -1 to disable
        int blobmemszmb = -1;
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for blob_mem_mb.\n");
            return 0;
        }
        blobmemszmb = toknum(tok, ltok);
        if (blobmemszmb == -1)
            gbl_blobmem_cap = (size_t)-1;
        else
            gbl_blobmem_cap = (1 << 20) * (size_t)toknum(tok, ltok);
    } else if (tokcmp(line, ltok, "blobmem_sz_thresh_kb") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for blobmem_sz_thresh_kb.\n");
            return 0;
        }
        gbl_blob_sz_thresh_bytes = 1024 * (size_t)toknum(tok, ltok);
    } else if (tokcmp(line, ltok, "enable_datetime_truncation") == 0) {
        gbl_forbid_datetime_truncation = 0;
    } else if (tokcmp(line, ltok, "enable_datetime_promotion") == 0) {
        gbl_forbid_datetime_promotion = 0;
    } else if (tokcmp(line, ltok, "enable_datetime_ms_us_sc") == 0) {
        gbl_forbid_datetime_ms_us_s2s = 0;
    } else if (tokcmp(line, ltok, "disable_datetime_truncation") == 0) {
        gbl_forbid_datetime_truncation = 1;
    } else if (tokcmp(line, ltok, "disable_datetime_promotion") == 0) {
        gbl_forbid_datetime_promotion = 1;
    } else if (tokcmp(line, ltok, "disable_datetime_ms_us_sc") == 0) {
        gbl_forbid_datetime_ms_us_s2s = 1;
    } else if (tokcmp(line, ltok, "default_datetime_precision") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for temptable_limit.\n");
            return 0;
        }
        DTTZ_TEXT_TO_PREC(tok, gbl_datetime_precision, 0, return 0);
    } else if (tokcmp(tok, ltok, "noblobstripe") == 0) {
        gbl_blobstripe = 0;
    } else if (tokcmp(tok,ltok,"dont_sort_nulls_with_header")==0) {
        gbl_sort_nulls_correctly=0;
    } else if (tokcmp(tok, ltok, "nochecksums") == 0) {
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_CHECKSUMS, 0);
    } else if (tokcmp(tok, ltok, "no_null_blob_fix") == 0) {
        gbl_disallow_null_blobs = 0;
    } else if (tokcmp(tok, ltok, "no_static_tag_blob_fix") == 0) {
        gbl_force_notnull_static_tag_blobs = 0;
    } else if (tokcmp(tok, ltok, "dont_forbid_ulonglong") == 0 ) {
        gbl_forbid_ulonglong = 0;
    } else if (tokcmp(tok,ltok,"dont_init_with_ondisk_header")==0) {
        gbl_init_with_odh = 0;
    } else if (tokcmp(tok,ltok, "dont_init_with_instant_schema_change") == 0) {
        logmsg(LOGMSG_INFO, "Tables will be initialized with instant schema-change support\n");
        gbl_init_with_instant_sc = 0;
    } else if (tokcmp(tok,ltok, "dont_init_with_inplace_updates") == 0) {
        gbl_init_with_ipu = 0;
    } else if (tokcmp(tok, ltok, "dont_prefix_foreign_keys") == 0) {
        gbl_fk_allow_prefix_keys = 0;
    } else if (tokcmp(tok, ltok, "dont_superset_foreign_keys") == 0) {
        gbl_fk_allow_superset_keys = 0;
    } else if (tokcmp(line, strlen("ssl"), "ssl") == 0) {
        /* Let's have a separate function for ssl directives. */
        rc = ssl_process_lrl(line, len);
        if (rc != 0)
            return -1;
    } else if (tokcmp(line, ltok, "perfect_ckp") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_use_perfect_ckp = (ltok <= 0) ? 1 : toknum(tok, ltok);
    } else if (tokcmp(line, ltok, "memnice") == 0) {
        int nicerc;
        tok = segtok(line, len, &st, &ltok);
        nicerc = comdb2ma_nice((ltok <= 0) ? 1 : toknum(tok, ltok));
        if (nicerc != 0) {
            logmsg(LOGMSG_ERROR, "Failed to change mem niceness: rc = %d.\n",
                   nicerc);
            return -1;
        }
    } else if (tokcmp(line, ltok, "upd_null_cstr_return_conv_err") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_upd_null_cstr_return_conv_err = (tok <= 0) ? 1 : toknum(tok, ltok);
    } 
    else {
        logmsg(LOGMSG_ERROR, "unknown opcode '%.*s' in lrl %s\n", ltok, tok,
               options->lrlname);
        if (gbl_bad_lrl_fatal)
            return -1;
    }

    if (gbl_disable_new_snapshot) {
        gbl_new_snapisol = 0;
        gbl_new_snapisol_asof = 0;
        gbl_new_snapisol_logging = 0;
    }

    if (gbl_rowlocks) {
        /* We can't ever choose a writer as a deadlock victim in rowlocks mode
           (at least without some kludgery, or snapshots) */
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGE_ORDER_TABLESCAN, 0);
    }


    return 0;
}

struct dbenv *read_lrl_file_int(struct dbenv *dbenv,
                                       const char *lrlname, const char *dbname,
                                       int required) {
    FILE *ff;
    char line[512] = {0}; // valgrind doesn't like sse42 instructions
    int rc;
    struct lrlfile *lrlfile;
    struct read_lrl_option_type options = {
        .lineno = 0,
        .lrlname = lrlname,
        .dbname = dbname,
    };

    dbenv->nsiblings = 1;

    lrlfile = malloc(sizeof(struct lrlfile));
    lrlfile->file = strdup(lrlname);
    listc_atl(&dbenv->lrl_files, lrlfile);

    ff = fopen(lrlname, "r");

    if (ff == 0) {
        if (required) {
            logmsg(LOGMSG_FATAL, "%s : %s\n", lrlname, strerror(errno));
            return 0;
        } else if (errno != ENOENT) {
            logmsgperror(lrlname);
        }
        return dbenv;
    }

    logmsg(LOGMSG_DEBUG, "processing %s...\n", lrlname);
    while (fgets(line, sizeof(line), ff)) {
        char *s = strchr(line, '\n');
        if (s) *s = 0;
        options.lineno++;
        read_lrl_option(dbenv, line, &options, strlen(line));
    }
    options.lineno = 0;
    rc = process_deferred_options(dbenv, DEFERRED_LEGACY_DEFAULTS, &options,
                                  read_lrl_option);
    if (rc) {
       logmsg(LOGMSG_WARN, "process_deferred_options rc %d\n", rc);
       fclose(ff);
    }
    if (rc) {
       fclose(ff);
       return NULL;
    }

    /* process legacy options (we deferred them) */

    if (gbl_disable_new_snapshot) {
        gbl_new_snapisol = 0;
        gbl_new_snapisol_asof = 0;
        gbl_new_snapisol_logging = 0;
    }

    if (gbl_rowlocks) {
        /* We can't ever choose a writer as a deadlock victim in rowlocks mode
           (at least without some kludgery, or snapshots) */
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGE_ORDER_TABLESCAN, 0);
    }

    fclose(ff);

    return dbenv;
}

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
            int valid_user;
            bdb_user_password_check(DEFAULT_USER, DEFAULT_PASSWORD,
                                    &valid_user);
            if (!valid_user)
                bdb_user_password_set(NULL, DEFAULT_USER, DEFAULT_PASSWORD);
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
    struct db *db;

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

        db = getdbbyname(tblnames[i]);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "Can't find handle for table %s\n", tblnames[i]);
            return -1;
        }

        rc = bdb_get_csc2_highest(NULL, tblnames[i], &ver, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "couldn't get highest version number for %s\n",
                    tblnames[i]);
            return 1;
        }

        int isc = 0;
        rc = get_db_instant_schema_change(db, &isc);
        if (isc) {
            /* load schema for older versions */
            for (int v = 1; v <= ver; ++v) {
                char *csc2text;
                if (get_csc2_file(db->dbname, v, &csc2text, NULL)) {
                    logmsg(LOGMSG_ERROR, "get_csc2_file failed %s:%d\n", __FILE__,
                            __LINE__);
                    continue;
                }

                struct schema *s =
                    create_version_schema(csc2text, v, db->dbenv);

                if (s == NULL) {
                    free(csc2text);
                    return 1;
                }

                add_tag_schema(db->dbname, s);
                free(csc2text);
            }
        }
    }
    return 0;
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
                          (dbenv->num_qdbs + fnd_queues) * sizeof(struct db *));
    if (dbenv->qdbs == NULL) {
        logmsg(LOGMSG_ERROR, "can't allocate memory for queue list\n");
        return -1;
    }
    for (int i = 0; i < fnd_queues; i++) {
        struct db *db;
        char **dests;
        int ndests;
        char *config;

        db = newqdb(dbenv, qnames[i],
                    65536 /* TODO: pass from comdb2sc, store in llmeta? */,
                    65536, 1);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "can't create queue \"%s\"\n", qnames[i]);
            return -1;
        }
        dbenv->qdbs[dbenv->num_qdbs++] = db;

        /* Add queue the hash. */
        hash_add(dbenv->qdb_hash, db);

        rc = bdb_llmeta_get_queue(qnames[i], &config, &ndests, &dests, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "can't get information for queue \"%s\"\n",
                    qnames[i]);
            return -1;
        }

        rc = dbqueue_add_consumer(db, 0, dests[0], 0);
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
    struct db *db;

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
    dbenv->dbs = realloc(dbenv->dbs, fndnumtbls * sizeof(struct db *));

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
        db = newdb_from_schema(dbenv, tblnames[i], NULL, dbnums[i], i, 0);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "newdb_from_schema failed %s:%d\n", __FILE__,
                    __LINE__);
            rc = 1;
            break;
        }
        db->version = ver;

        /* We only want to load older schema versions for ODH databases.  ODH
         * information
         * is stored in the meta table (not the llmeta table), so it's not
         * loaded yet. */

        /* set db values and add to env */
        db->shmflags = 0;
        db->dbs_idx = i;
        dbenv->dbs[i] = db;

        /* Add table to the hash. */
        hash_add(dbenv->db_hash, db);

        /* just got a bunch of data. remember it so key forming
           routines and SQL can get at it */
        rc = add_cmacc_stmt(db, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Failed to load schema: can't process schema file "
                            "%s\n",
                    db->dbname);
            ++i; /* this tblname has already been marshalled so we dont want to
                  * free it below */
            rc = 1;
            break;
        }
    }

    /* we have to do this after all the meta table lookups so that the hack in
     * get_meta_int works */
    dbenv->num_dbs = fndnumtbls;

    /* if we quit early bc of an error free the rest */
    while (i < fndnumtbls)
        free(tblnames[i++]);

    return rc;
}

/* replace the table names and dbnums saved in the low level meta table with the
 * ones in the dbenv.  returns 0 on success and anything else otherwise */
int llmeta_set_tables(tran_type *tran, struct dbenv *dbenv)
{
    int i, bdberr, dbnums[MAX_NUM_TABLES];
    char *tblnames[MAX_NUM_TABLES];

    /* gather all the table names and db numbers */
    for (i = 0; i < dbenv->num_dbs; ++i) {
        tblnames[i] = dbenv->dbs[i]->dbname;
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
                    dbenv->dbs[i]->dbname);
            rc = -1;
            goto done;
        }

        sbuf2printf(sbfile,
                    "table %s %d\n\tdata files: %016llx\n\tblob files\n",
                    dbenv->dbs[i]->dbname, dbenv->dbs[i]->lrl,
                    flibc_htonll(version_num));

        /* print the indicies' version numbers */
        for (j = 1; j <= dbenv->dbs[i]->numblobs; ++j) {
            if (bdb_get_file_version_data(dbenv->dbs[i]->handle, tran,
                                          j /*dtanum*/, &version_num,
                                          &bdberr) ||
                bdberr != BDBERR_NOERROR) {
                logmsg(LOGMSG_ERROR, "llmeta_dump_mapping: failed to fetch version "
                                "number for %s's blob num %d's files\n",
                        dbenv->dbs[i]->dbname, j);
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
                logmsg(LOGMSG_ERROR, "llmeta_dump_mapping: failed to fetch version "
                                "number for %s's index num %d\n",
                        dbenv->dbs[i]->dbname, j);
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
    struct db *p_db;

    if (!(p_db = getdbbyname(table)))
        return -1;

    /* print out the versions of each of the table's files */

    /* print the main data file's version number */
    if (bdb_get_file_version_data(p_db->handle, tran, 0 /*dtanum*/,
                                  &version_num, &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        if (err)
            logmsg(LOGMSG_ERROR, "llmeta_dump_mapping: failed to fetch version "
                            "number for %s's main data files\n",
                    p_db->dbname);
        else
            ctrace("llmeta_dump_mapping: failed to fetch version number for "
                   "%s's main data files\n",
                   p_db->dbname);
        return -1;
    }

    if (err)
        logmsg(LOGMSG_INFO, "table %s\n\tdata files: %016llx\n\tblob files\n",
                p_db->dbname, flibc_htonll(version_num));
    else
        ctrace("table %s\n\tdata files: %016llx\n\tblob files\n", p_db->dbname,
               (long long unsigned int)flibc_htonll(version_num));

    /* print the blobs' version numbers */
    for (i = 1; i <= p_db->numblobs; ++i) {
        if (bdb_get_file_version_data(p_db->handle, tran, i /*dtanum*/,
                                      &version_num, &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            if (err)
                logmsg(LOGMSG_ERROR, "llmeta_dump_mapping: failed to fetch version "
                                "number for %s's blob num %d's files\n",
                        p_db->dbname, i);
            else
                ctrace("llmeta_dump_mapping: failed to fetch version number "
                       "for %s's blob num %d's files\n",
                       p_db->dbname, i);
            return -1;
        }
        if (err)
            logmsg(LOGMSG_INFO, "\t\tblob num %d: %016llx\n", i,
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
                logmsg(LOGMSG_ERROR, "llmeta_dump_mapping: failed to fetch version "
                                "number for %s's index num %d\n",
                        p_db->dbname, i);
            else
                ctrace("llmeta_dump_mapping: failed to fetch version number "
                       "for %s's index num %d\n",
                       p_db->dbname, i);
            return -1;
        }

        if (err)
            logmsg(LOGMSG_INFO, "\t\tindex num %d: %016llx\n", i,
                    flibc_htonll(version_num));
        else
            ctrace("\t\tindex num %d: %016llx\n", i,
                   (long long unsigned int)flibc_htonll(version_num));
    }

    return 0;
}

static int read_config_dir(struct dbenv *dbenv, char *dbname, char *dir)
{
    DIR *d = NULL;
    int rc = 0;
    struct dirent ent, *out;

    d = opendir(dir);
    if (d == NULL) {
        rc = -1;
        goto done;
    }
    while (bb_readdir(d, &ent, &out) == 0 && out != NULL) {
        int len;
        len = strlen(ent.d_name);
        if (strcmp(ent.d_name + len - 4, ".lrl") == 0) {
            char *file = comdb2_asprintf("%s/%s", dir, ent.d_name);
            pre_read_lrl_file(dbenv, file, dbname);
            rc = (read_lrl_file(dbenv, file, dbname, 0) == NULL);
            if (rc)
                logmsg(LOGMSG_ERROR, "Error processing %s\n", file);
            free(file);
            if (rc)
                goto done;
        }
    }

done:
    if (d)
        closedir(d);
    return rc;
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

    /* default pagesizes */
    dbenv->pagesize_dta = 4096;
    dbenv->pagesize_freerec = 512;
    dbenv->pagesize_ix = 4096;

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
    init_deferred_options(dbenv);
    listc_init(&dbenv->lrl_files, offsetof(struct lrlfile, lnk));

    /* Initialize the table/queue hashes. */
    dbenv->db_hash =
        hash_init_user((hashfunc_t *)strhashfunc, (cmpfunc_t *)strcmpfunc,
                       offsetof(struct db, dbname), 0);
    dbenv->qdb_hash =
        hash_init_user((hashfunc_t *)strhashfunc, (cmpfunc_t *)strcmpfunc,
                       offsetof(struct db, dbname), 0);

    if ((rc = pthread_mutex_init(&dbenv->dbqueue_admin_lk, NULL)) != 0) {
        logmsg(LOGMSG_FATAL, "can't init lock %d %s\n", rc, strerror(errno));
        return NULL;
    }

    if (lrlname) pre_read_lrl_file(dbenv, lrlname, dbname);

    /* if we havn't been told not to load the /bb/bin/ config files */
    if (!gbl_nogbllrl) {
        char *lrlfile;

        /* firm wide defaults */
        lrlfile = comdb2_location("config", "comdb2.lrl");

        if (!read_lrl_file(dbenv, lrlfile, dbname, 0 /*not required*/)) {
            free(lrlfile);
            return 0;
        }
        free(lrlfile);

        /* local defaults */
        lrlfile = comdb2_location("config", "comdb2_local.lrl");
        if (!read_lrl_file(dbenv, lrlfile, dbname, 0)) {
            free(lrlfile);
            return 0;
        }
        free(lrlfile);

        char *confdir = comdb2_location("config", "comdb2.d");
        struct stat st;
        int rc = stat(confdir, &st);
        if (rc == 0 && S_ISDIR(st.st_mode)) {
            if (read_config_dir(dbenv, dbname, confdir)) {
                return 0;
            }
        }
    }

    /* look for overriding lrl's in the local directory */
    if (!read_lrl_file(dbenv, "comdb2.lrl", dbname, 0 /*not required*/)) {
        return 0;
    }

    /* local defaults */
    if (!read_lrl_file(dbenv, "comdb2_local.lrl", dbname, 0 /*not required*/)) {
        return 0;
    }

    /* if env variable is set, process another lrl.. */
    const char *envlrlname = getenv("COMDB2_CONFIG");
    if (envlrlname &&
        !read_lrl_file(dbenv, envlrlname, dbname, 1 /*required*/)) {
        return 0;
    }

    /* this database */
    if (lrlname && !read_lrl_file(dbenv, lrlname, dbname, 1 /*required*/)) {
        return 0;
    }

    logmsg(LOGMSG_INFO, "database %s starting\n", dbenv->envname);

    /* switch to keyless mode as long as no mode has been selected yet */
    bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_GENIDS, 1);

    if (dbenv->basedir == NULL && gbl_dbdir) dbenv->basedir = gbl_dbdir;
    if (dbenv->basedir == NULL) dbenv->basedir = getenv("COMDB2_DB_DIR");
    if (dbenv->basedir == NULL)
        dbenv->basedir = comdb2_location("database", "%s", dbname);

    if (dbenv->basedir==NULL) {
        logmsg(LOGMSG_ERROR, "must specify database directory\n");
        return 0;
    }

    if (lrlname == NULL) {
        char *lrl = comdb2_asprintf("%s/%s.lrl", dbenv->basedir, dbname);
        if (access(lrl, F_OK) == 0) {
            if (read_lrl_file(dbenv, lrl, dbname, 0) == NULL) {
                return 0;
            }
            lrlname = lrl;
        } else
            free(lrlname);
    }

    if (gbl_create_mode) {
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
            return 0;
        }
    }

    tz_hash_init();
    init_sql_hint_table();

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
                if (strcasecmp(dbenv->dbs[ii]->dbname,
                               dbenv->dbs[jj]->dbname) == 0) {
                    have_bad_schema = 1;
                    logmsg(LOGMSG_FATAL, "Two tables have identical names (%s) tblnums %d "
                           "%d\n",
                           dbenv->dbs[ii]->dbname, ii, jj);
                }
            }
        }

        if ((strcasecmp(dbenv->dbs[ii]->dbname, dbenv->envname) == 0) &&
            (dbenv->dbs[ii]->dbnum != 0) &&
            (dbenv->dbnum != dbenv->dbs[ii]->dbnum)) {

            have_bad_schema = 1;
            logmsg(LOGMSG_FATAL, "Table name and database name conflict (%s) tblnum %d\n",
                   dbenv->envname, ii);
        }

        if (dbenv->dbs[ii]->nix > MAXINDEX) {
            have_bad_schema = 1;
            logmsg(LOGMSG_FATAL, "Database %s has too many indexes (%d)\n",
                   dbenv->dbs[ii]->dbname, dbenv->dbs[ii]->nix);
        }

        /* last ditch effort to stop invalid schemas getting through */
        for (jj = 0; jj < dbenv->dbs[ii]->nix && jj < MAXINDEX; jj++)
            if (dbenv->dbs[ii]->ix_keylen[jj] > MAXKEYLEN) {
                have_bad_schema = 1;
                logmsg(LOGMSG_FATAL, "Database %s index %d too large (%d)\n",
                       dbenv->dbs[ii]->dbname, jj,
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
    for (int i = 0; i < thedb->num_qdbs; ++i) {
        char *config;
        int ndests;
        char **dests;
        int bdberr;
        char *name = thedb->qdbs[i]->dbname;
        bdb_llmeta_get_queue(name, &config, &ndests, &dests, &bdberr);
        char path[PATH_MAX];
        snprintf(path, sizeof(path), REPOP_QDB_FMT, dir, thedb->envname, i);
        FILE *f = fopen(path, "w");
        if (f == NULL) {
            logmsg(LOGMSG_ERROR, "%s:fopen(\"%s\"):%s\n", __func__, path,
                    strerror(errno));
            return -1;
        }
        logmsg(LOGMSG_INFO, "%s\n%d\n", thedb->qdbs[i]->dbname, ndests);
        for (int j = 0; j < ndests; ++j) {
            logmsg(LOGMSG_INFO, "%s\n", dests[j]);
        }
        logmsg(LOGMSG_INFO, "%s", config);
        fclose(f);
        logmsg(LOGMSG_INFO, "%s wrote file:%s for queuedb:%s\n", __func__, path,
               thedb->qdbs[i]->dbname);
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
            logmsg(LOGMSG_ERROR, "%s: get_csc2_fname failed for: %s\n", __func__,
                    thedb->dbs[i]->dbname);

            free(p_data);
            return -1;
        }

        /* dump the csc2 */
        if (dump_table_csc2_to_disk_fname(thedb->dbs[i],
                                          p_data->csc2_paths[i])) {
            logmsg(LOGMSG_ERROR, "%s: dump_table_csc2_to_disk_fname failed for: "
                            "%s\n",
                    __func__, thedb->dbs[i]->dbname);

            free(p_data);
            return -1;
        }

        p_data->p_table_names[i] = thedb->dbs[i]->dbname;
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

int appsock_repopnewlrl(SBUF2 *sb, int *keepsocket)
{
    char lrl_fname_out[256];
    int rc;

    if (((rc = sbuf2gets(lrl_fname_out, sizeof(lrl_fname_out), sb)) <= 0) ||
        (lrl_fname_out[rc - 1] != '\n')) {
        logmsg(LOGMSG_ERROR, "%s: I/O error reading out lrl fname\n", __func__);
        return -1;
    }
    lrl_fname_out[rc - 1] = '\0';

    if (repopulate_lrl(lrl_fname_out)) {
        logmsg(LOGMSG_ERROR, "%s: repopulate_lrl failed\n", __func__);
        return -1;
    }

    if (sbuf2printf(sb, "OK\n") < 0 || sbuf2flush(sb) < 0) {
        logmsg(LOGMSG_ERROR, "%s: failed to send done ack text\n", __func__);
        return -1;
    }

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
        logmsg(LOGMSG_ERROR, "Failed to open low level meta table, rc: %d\n",
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
            char qfile[PATH_MAX], sfile[PATH_MAX];
            snprintf(qfile, PATH_MAX, "%s/__dbq.%s.queue.%" PRIu64, txndir,
                     q->name, nums[j]);
            snprintf(sfile, PATH_MAX, "%s/__dbq.%s.queue.%" PRIu64, savdir,
                     q->name, nums[j]);
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
    lrlfile_name = malloc(strlen(dir) + strlen(dbname) + 4 /*.lrl*/ +
                          1 /*slash*/ + 1 /*nul*/);
    sprintf(lrlfile_name, "%s/%s.lrl", dir, dbname);
    lrlfile = fopen(lrlfile_name, "w");
    if (lrlfile == NULL) {
        logmsg(LOGMSG_ERROR, "fopen(\"%s\") rc %d %s.\n", errno, strerror(errno));
        return NULL;
    }
    fprintf(lrlfile, "name    %s\n", dbname);
    fprintf(lrlfile, "dir     %s\n\n", dir);
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

static int llmeta_set_qdbs()
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
    struct db *db;

    dbenv->dbs =
        realloc(dbenv->dbs, (dbenv->num_dbs + 1) * sizeof(struct db *));

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
    db = newdb_from_schema(dbenv, table, NULL, 0, dbenv->num_dbs, 0);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "Can't init table %s from schema\n", table);
        return -1;
    }
    db->dbs_idx = dbenv->num_dbs;
    db->csc2_schema = strdup(schema);
    dbenv->dbs[dbenv->num_dbs++] = db;

    /* Add table to the hash. */
    hash_add(dbenv->db_hash, db);

    if (add_cmacc_stmt(db, 0)) {
        logmsg(LOGMSG_ERROR, "Can't init table structures %s from schema\n", table);
        return -1;
    }
    return 0;
}

static void load_dbstore_tableversion(struct dbenv *dbenv)
{
    int i;
    for (i = 0; i < dbenv->num_dbs; i++) {
        struct db *db = dbenv->dbs[i];
        update_dbstore(db);

        db->tableversion = table_version_select(db, NULL);
        if (db->tableversion == -1) {
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

static void replace_args(int argc, char *argv[])
{
    int ii;
    for (ii = 1; ii < argc; ii++) {
        if (strcasecmp(argv[ii], "-lrl") == 0) {
            argv[ii] = "--lrl";
        } else if (strcasecmp(argv[ii], "-repopnewlrl") == 0) {
            argv[ii] = "--repopnewlrl";
        } else if (strcasecmp(argv[ii], "-recovertotime") == 0) {
            argv[ii] = "--recovertotime";
        } else if (strcasecmp(argv[ii], "-recovertolsn") == 0) {
            argv[ii] = "--recovertolsn";
        } else if (strcasecmp(argv[ii], "-recovery_lsn") == 0) {
            argv[ii] = "--recovery_lsn";
        } else if (strcasecmp(argv[ii], "-pidfile") == 0) {
            argv[ii] = "--pidfile";
        } else if (strcasecmp(argv[ii], "-help") == 0) {
            argv[ii] = "--help";
            /* This option is mutually exclusive */
            return;
        } else if (strcasecmp(argv[ii], "-create") == 0) {
            argv[ii] = "--create";
        } else if (strcasecmp(argv[ii], "-fullrecovery") == 0) {
            argv[ii] = "--fullrecovery";
        }
    }
}

static int init(int argc, char **argv)
{
    char *dbname, *lrlname = NULL, ctmp[64], *p;
    char c;
    static int noabort = 0;
    int cacheszkb = 0, ii;
    int rc;
    int bdberr;
    int cacheszkb_suggestion = 0;
    int options_idx;

    if (argc < 2)
        usage();

    dyns_allow_bools();

    rc = bdb_osql_log_repo_init(&bdberr);
    if (rc) {
        logmsg(LOGMSG_FATAL, "bdb_osql_log_repo_init failed to init log repository "
                        "rc %d bdberr %d\n",
                rc, bdberr);
        return -1;
    }

    /* get my working directory */
    if (getcwd(gbl_cwd, sizeof(gbl_cwd)) == 0) {
        logmsgperror("failed to getcwd");
        return -1;
    }

    if (thd_init()) {
        logmsg(LOGMSG_FATAL, "failed initialize thread module\n");
        return -1;
    }
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
    toblock_init();


    replace_args(argc, argv);
    static struct option long_options[] = {
        {"lrl",                    required_argument, NULL,              0},
        {"repopnewlrl",            required_argument, NULL,              0},
        {"recovertotime",          required_argument, NULL,              0},
        {"recovertolsn",           required_argument, NULL,              0},
        {"recoverylsn",            required_argument, NULL,              0},
        {"pidfile",                required_argument, NULL,              0},
        {"help",                   no_argument,       NULL,            'h'},
        {"create",                 no_argument, &gbl_create_mode,        1},
        {"fullrecovery",           no_argument, &gbl_fullrecovery,       1},
        {"no-global-lrl",          no_argument, &gbl_nogbllrl,           1},
        {"dir",                    required_argument, NULL,              0},
        {NULL,                     0,                 NULL,              0}
    };

    while ((c = getopt_long(argc, argv, "h",
                            long_options, &options_idx)) != -1) {
        if (c == 'h')
            usage();
        if (c == '?')
            return -1;

        switch (options_idx) {
           case 0:   /* lrl */
              lrlname = optarg;
              break;
           case 1:   /* repopnewlrl */
              logmsg(LOGMSG_INFO, "repopulate external .lrl mode.\n");
              gbl_repoplrl_fname = optarg;
              gbl_exit = 1;
              break;
           case 2:   /* recovertotime */
              logmsg(LOGMSG_FATAL, "force full recovery to timestamp %u\n",
                    gbl_recovery_timestamp);
              gbl_recovery_timestamp = strtoul(optarg, NULL, 10);
              gbl_fullrecovery = 1;
              break;
           case 3:   /* recovertolsn */
              if ((p = strchr(optarg, ':')) == NULL) {
                 logmsg(LOGMSG_FATAL, "recovertolsn: invalid lsn format.\n");
                 exit(1);
              }

              p++;
              gbl_recovery_lsn_file = atoi(optarg);
              gbl_recovery_lsn_offset = atoi(p);
              logmsg(LOGMSG_FATAL, "force full recovery to lsn %d:%d\n",
                    gbl_recovery_lsn_file, gbl_recovery_lsn_offset);
              gbl_fullrecovery = 1;
              break;
           case 4:   /* recovery_lsn */
              gbl_recovery_options = optarg;
              break;
           case 5: /* pidfile */
              gbl_pidfile = optarg;
              write_pidfile(gbl_pidfile);
              break;
           case 10: /* dir */
              gbl_dbdir = optarg;
              break;
        }
    }

    if (gbl_create_mode) {        /*  10  */
        logmsg(LOGMSG_INFO, "create mode.\n");
        gbl_exit = 1;
    }
    if (gbl_fullrecovery) {       /*  11  */
        logmsg(LOGMSG_FATAL, "force full recovery.\n");
        gbl_exit = 1;
    }
    if (gbl_nogbllrl) {         /*  14  */
        /* disable loading comdb2.lrl and comdb2_local.lrl with an absolute
         * path in /bb/bin. comdb2.lrl and comdb2_local.lrl in the pwd are
         * still loaded */
        logmsg(LOGMSG_INFO, "not loading %s/bin/comdb2.lrl and "
               "%s/bin/comdb2_local.lrl.\n",
               gbl_config_root, gbl_config_root);
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
    if (strlen(dbname) == 0 || strlen(dbname) >= MAX_DBNAME_LENGTH) {
       logmsg(LOGMSG_FATAL, "Invalid dbname, must be < %d characters\n", 
                MAX_DBNAME_LENGTH - 1);
        return -1;
    }
    strcpy(gbl_dbname, dbname);

    if (optind < argc && isdigit((int)argv[optind][0])) {
        cacheszkb = atoi(argv[optind]);
    }

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

    /* Initialize SSL backend before creating any net.
       If we're in creat mode, don't bother. */
    if (!gbl_create_mode && ssl_bend_init(thedb->basedir) != 0) {
        logmsg(LOGMSG_FATAL, "Failed to initialize SSL backend.\n");
        return -1;
    }
    logmsg(LOGMSG_INFO, "SSL backend initialized.\n");

    if (init_blob_cache() != 0) return -1;

    if (osqlpfthdpool_init()) {
        logmsg(LOGMSG_FATAL, "failed to initialise sql module\n");
        return -1;
    }

    /* Since we moved bbipc context code lower, we need to explicitly
     * initialize ctrace stuff, or our ctrace files will have names like
     * dum50624.trace which isn't helpful */
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
            logmsg(LOGMSG_FATAL, "Cannot start without logfiles.  If this is\n");
            logmsg(LOGMSG_INFO, "a clustered database then you should fixcomdb2\n");
            logmsg(LOGMSG_INFO, "from the master.\n");
            if (!noabort)
                exit(1);
        }
    } else {
        /* if we are going to exit, don't use bbipc */
        gbl_use_bbipc = 0;
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

    gbl_starttime = time_epoch();

    /* Get all the LONG PREAD and LONG PWRITE outof act.log; I am truly fed up
     * with the entire company asking me if this is a problem. -- SJ */
    berk_set_long_trace_func(myctrace);

    /* disallow bools on test machines.  Prod will continue
     * to allow them because at least one prod database uses them.
     * Still alow bools for people who want to copy/test prod dbs
     * that use them.  Don't allow new databases to have bools. */
    if ((get_mach_class(machine()) == CLASS_TEST) && gbl_create_mode) {
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
    }

    /* We grab alot of genids in the process of opening llmeta */
    if (gbl_init_with_genid48 && gbl_create_mode)
        bdb_genid_set_format(thedb->bdb_env, LLMETA_GENID_48BIT);

    /* open the table */
    if (llmeta_open()) {
        logmsg(LOGMSG_FATAL, "Failed to open low level meta table, rc: %d\n",
                bdberr);
        return -1;
    }

    logmsg(LOGMSG_INFO, "Successfully opened low level meta table\n");

    gbl_llmeta_open = 1;

    if (!gbl_create_mode) {
       uint64_t format;
       bdb_get_genid_format(&format, &bdberr);
       bdb_genid_set_format(thedb->bdb_env, format);
    }

    rc = pthread_key_create(&query_info_key, NULL);
    if (rc) {
        logmsg(LOGMSG_FATAL, "pthread_key_create query_info_key rc %d\n", rc);
        return -1;
    }

    tz_hash_init();
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
        if (llmeta_load_tables(thedb, dbname)) {
            logmsg(LOGMSG_FATAL, "could not load tables from the low level meta "
                            "table\n");
            return -1;
        }

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

        /* not clearing all the flags makes not possible to backout the
           server and clear all the bits.  this will prevent this from
           this point on
         */
        comdb2_shm_clear_and_set_flags(thedb->dbnum, 0);

        /* turn off keyless bit (turn them on later if db is in fact keyless) */
        for (ii = 0; ii < thedb->num_dbs; ii++) {
            if (thedb->dbs[ii]->dbnum) {
                comdb2_shm_clear_and_set_flags(thedb->dbs[ii]->dbnum, 0);
            }
        }

        /* if(!gbl_notimeouts)
           ARGHHHHHHHHHHHHHHHHH
           This is used for both client heartbeats and reset! I cannot
           turn off one without the other... Please don't overload bits.
        */
        comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_HEARTBEAT);

        /* we always use server-side keyforming now, adjust lrl/ixlen/etc.,
         * set db flags */

        comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_KEYLESS_API);

        /* Enable linux client in this version. */
        comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_LINUX_CLIENT);

        if (COMDB2_SOCK_FSTSND_ENABLED()) {
            comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_SOCK_FSTSND);
        }

#if 0
       if(gbl_blk_pq_shmkey) {
            comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_PQENABLED);
            comdb2_shm_pq_shmkey_set(thedb->dbnum, gbl_blk_pq_shmkey);
            fprintf(stderr,"\n setting the pq shared mem key to %d ", gbl_blk_pq_shmkey);

       }
#endif

        comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_ALLOCV2_ENABLED);

        if (gbl_enable_position_apis) {
            comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_POSITION_API);
        }

        comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_TZ);

        comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_TZDMP);

        comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_FAILEDDISP);

        if (COMDB2_ERRSTAT_ENABLED())
            comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_ERRSTAT);

        comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_OSQL);

        comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_OSQL_SOCK);

        if (COMDB2_RECOM_ENABLED())
            comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_RECOM);

        if (gbl_sql_tranlevel_default != SQL_TDEF_COMDB2)
            comdb2_shm_set_flag(thedb->dbnum, gbl_sql_tranlevel_default);

        if (gbl_sql_tranlevel_sosql_pref)
            comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_PREFER_SOSQL);

        if (gbl_enable_block_offload)
            comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_BLOCK_OFFLOAD);

        if (COMDB2_SNAPISOL_ENABLED())
            comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_SERIAL);

        if (COMDB2_SERIAL_ENABLED())
            comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_SERIAL);

        if (gbl_enable_good_sql_return_codes)
            comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_GOODSQLCODES);

        if (gbl_fkrcode)
            comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_FKRCODE);

        if (!have_all_schemas()) {
            logmsg(LOGMSG_ERROR,
                  "Server-side keyforming not supported - missing schemas\n");
            return -1;
        }

        if (gbl_sql_use_random_readnode)
            comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_SQL_RANDNODE);

        fix_lrl_ixlen(); /* set lrl, ix lengths: ignore lrl file, use info from
                            schema */

        for (i = 0; i < thedb->num_dbs; i++) {
            if (thedb->dbs[i]->dbnum) {
                comdb2_shm_set_flag(thedb->dbs[i]->dbnum,
                                    CMDB2_SHMFLG_KEYLESS_API);
                comdb2_shm_set_flag(thedb->dbs[i]->dbnum, CMDB2_SHMFLG_TZ);
                comdb2_shm_set_flag(thedb->dbs[i]->dbnum, CMDB2_SHMFLG_TZDMP);

                if (COMDB2_SOCK_FSTSND_ENABLED()) {
                    comdb2_shm_set_flag(thedb->dbs[i]->dbnum,
                                        CMDB2_SHMFLG_SOCK_FSTSND);
                }
                if (COMDB2_ERRSTAT_ENABLED())
                    comdb2_shm_set_flag(thedb->dbs[i]->dbnum,
                                        CMDB2_SHMFLG_ERRSTAT);

                comdb2_shm_set_flag(thedb->dbs[i]->dbnum, CMDB2_SHMFLG_OSQL);
                comdb2_shm_set_flag(thedb->dbs[i]->dbnum,
                                    CMDB2_SHMFLG_OSQL_SOCK);

                if (COMDB2_RECOM_ENABLED())
                    comdb2_shm_set_flag(thedb->dbs[i]->dbnum,
                                        CMDB2_SHMFLG_RECOM);

                if (gbl_sql_tranlevel_default != SQL_TDEF_COMDB2)
                    comdb2_shm_set_flag(thedb->dbs[i]->dbnum,
                                        gbl_sql_tranlevel_default);

                if (gbl_sql_tranlevel_sosql_pref)
                    comdb2_shm_set_flag(thedb->dbs[i]->dbnum,
                                        CMDB2_SHMFLG_PREFER_SOSQL);

                if (gbl_enable_block_offload)
                    comdb2_shm_set_flag(thedb->dbs[i]->dbnum,
                                        CMDB2_SHMFLG_BLOCK_OFFLOAD);

                if (COMDB2_SNAPISOL_ENABLED())
                    comdb2_shm_set_flag(thedb->dbs[i]->dbnum,
                                        CMDB2_SHMFLG_SERIAL);

                if (COMDB2_SERIAL_ENABLED())
                    comdb2_shm_set_flag(thedb->dbs[i]->dbnum,
                                        CMDB2_SHMFLG_SERIAL);

                if (gbl_enable_good_sql_return_codes)
                    comdb2_shm_set_flag(thedb->dbs[i]->dbnum,
                                        CMDB2_SHMFLG_GOODSQLCODES);

                comdb2_shm_set_flag(thedb->dbs[i]->dbnum,
                                    CMDB2_SHMFLG_FAILEDDISP);

                if (gbl_sql_use_random_readnode)
                    comdb2_shm_set_flag(thedb->dbs[i]->dbnum,
                                        CMDB2_SHMFLG_SQL_RANDNODE);
            }
        }

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
       if (create_service_file(lrlname))
          logmsg(LOGMSG_ERROR, "couldn't create service file\n");
    }

    /* open db engine */
    logmsg(LOGMSG_INFO, "starting backend db engine\n");
    if (backend_open(thedb) != 0) {
        logmsg(LOGMSG_FATAL, "failed to open '%s'\n", dbname);
        return -1;
    }

    if (llmeta_load_tables_older_versions(thedb)) {
        logmsg(LOGMSG_FATAL, "llmeta_load_tables_older_versions failed\n");
        return -1;
    }

    load_dbstore_tableversion(thedb);

    sqlinit();
    rc = create_sqlmaster_records(NULL);
    if (rc) {
        logmsg(LOGMSG_FATAL, "create_sqlmaster_records rc %d\n", rc);
        return -1;
    }
    create_master_tables(); /* create sql statements */

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

        if (llmeta_set_qdbs(qdbs) != 0) {
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
        if (gbl_create_mode)
           logmsg(LOGMSG_USER, "Created database %s.\n", thedb->envname);
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
    /*showdbenv(thedb);*/

    if (gbl_net_max_queue) {
        net_set_max_queue(thedb->handle_sibling, gbl_net_max_queue);
    }

    if (gbl_net_max_mem) {
        uint64_t bytes;
        bytes = 1024 * 1024 * gbl_net_max_mem;
        net_set_max_bytes(thedb->handle_sibling, bytes);
    }

    if (gbl_net_max_queue_signal) {
        net_set_max_queue(thedb->handle_sibling_signal,
                          gbl_net_max_queue_signal);
    }

    if (gbl_net_throttle_percent) {
        net_set_throttle_percent(thedb->handle_sibling,
                                 gbl_net_throttle_percent);
    }

    if (gbl_net_portmux_register_interval) {
        net_set_portmux_register_interval(thedb->handle_sibling,
                                          gbl_net_portmux_register_interval);
    }

    if (gbl_signal_net_portmux_register_interval) {
        net_set_portmux_register_interval(
            thedb->handle_sibling_signal,
            gbl_signal_net_portmux_register_interval);
    }
    if (!gbl_accept_on_child_nets)
        net_set_portmux_register_interval(thedb->handle_sibling_signal, 0);

    if (gbl_enque_flush_interval) {
        net_set_enque_flush_interval(thedb->handle_sibling,
                                     gbl_enque_flush_interval);
    }

    if (gbl_enque_flush_interval_signal) {
        net_set_enque_flush_interval(thedb->handle_sibling_signal,
                                     gbl_enque_flush_interval_signal);
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
    if (gbl_heartbeat_send_signal) {
        net_set_heartbeat_send_time(thedb->handle_sibling_signal,
                                    gbl_heartbeat_send_signal);
    }
    if (gbl_heartbeat_check_signal) {
        net_set_heartbeat_check_time(thedb->handle_sibling_signal,
                                     gbl_heartbeat_check_signal);
    }

    net_setbufsz(thedb->handle_sibling, gbl_netbufsz);
    net_setbufsz(thedb->handle_sibling_signal, gbl_netbufsz_signal);

    if (javasp_init_procedures() != 0) {
        logmsg(LOGMSG_FATAL, "*ERROR* cannot initialise Java stored procedures\n");
        return -1;
    }

    comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_STATS_OK);

    /* Advertise that we support blocksql semantics over sockets. */
    if (gbl_upgrade_blocksql_to_socksql &&
        gbl_sql_tranlevel_default != SQL_TDEF_SOCK)
        comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_SOSQL_DFLT);

    /*bdb_set_parallel_recovery_threads(thedb->bdb_env,
     * gbl_parallel_recovery_threads);*/

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

static __thread ssize_t bbipc_id = 0;
static pthread_once_t bbipc_exit_once = PTHREAD_ONCE_INIT;

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
        cantim(parm->parm);
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

#ifdef __hpux
static void adjust_ulimits(void) {}
#else

/* bump up ulimit for no. fds up to hard limit */
static void adjust_ulimits(void)
{
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
}

#endif

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
    int diff_newsql_steps;
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
    long long last_newsql_steps = 0;
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
    struct db *db;
    char hdr_fmt[] = "DIFF REQUEST STATS FOR DB %d '%s'\n";
    int have_scon_header = 0;
    int have_scon_stats = 0;

    dbenv = p;

    if (COMDB2_DIFFSTAT_REPORT()) {
        /* initialize */
        statlogger = reqlog_alloc();
        reqlog_diffstat_init(statlogger);
    }

    for (;;) {
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

        bdb_get_bpool_counters(thedb->bdb_env, &bpool_hits, &bpool_misses);

        if (!dbenv->exiting && !dbenv->stopped) {
            bdb_get_lock_counters(thedb->bdb_env, &ndeadlocks, &nlockwaits);
            diff_deadlocks = ndeadlocks - last_ndeadlocks;
            diff_lockwaits = nlockwaits - last_nlockwaits;
        } else {
            diff_deadlocks = 0;
            diff_lockwaits = 0;
            reqlog_free(statlogger);
            return NULL;
        }

        diff_qtrap = nqtrap - last_qtrap;
        diff_fstrap = nfstrap - last_fstrap;
        diff_nsql = nsql - last_nsql;
        diff_nsql_steps = nsql_steps - last_nsql_steps;
        diff_newsql = newsql - last_newsql;
        diff_newsql_steps = newsql_steps - last_newsql_steps;
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
        last_newsql_steps = newsql_steps;
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
                    logmsg(LOGMSG_USER, " vreplays %lld", diff_vreplays);
                if (diff_newsql)
                    logmsg(LOGMSG_USER, " nnewsql %lld", diff_newsql);
                if (diff_ncommit_time)
                    logmsg(LOGMSG_USER, " n_commit_time %f ms",
                           diff_ncommit_time / (1000 * diff_ncommits));
                if (diff_bpool_hits)
                    logmsg(LOGMSG_USER, " cache_hits %llu", diff_bpool_hits);
                if (diff_bpool_misses)
                    logmsg(LOGMSG_USER, " cache_misses %llu",
                           diff_bpool_misses);
                have_scon_stats = 1;
            }
        }
        if (gbl_report)
            have_scon_stats |= osql_comm_diffstat(NULL, &have_scon_header);

        if (have_scon_stats)
            logmsg(LOGMSG_USER, "\n");

        if (COMDB2_DIFFSTAT_REPORT() && !gbl_schema_change_in_progress) {
            thresh = reqlog_diffstat_thresh();
            if ((thresh > 0) && (count > thresh)) {
                strbuf *logstr = strbuf_new();
                diff_qtrap = nqtrap - last_report_nqtrap;
                diff_fstrap = nfstrap - last_report_nfstrap;
                diff_nsql = nsql - last_report_nsql;
                diff_nsql_steps = nsql_steps - last_report_nsql_steps;
                diff_newsql = newsql - last_report_newsql;
                diff_newsql_steps = newsql_steps - last_report_newsql_steps;
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
                    db = dbenv->dbs[ii];
                    hdr = 0;

                    for (jj = 0; jj <= MAXTYPCNT; jj++) {
                        diff = db->typcnt[jj] - db->prev_typcnt[jj];
                        if (diff > 0) {
                            if (hdr == 0) {
                                reqlog_logf(statlogger, REQL_INFO, hdr_fmt,
                                            db->dbnum, db->dbname);
                                hdr = 1;
                            }
                            reqlog_logf(statlogger, REQL_INFO, "%-20s %u\n",
                                        req2a(jj), diff);
                        }
                        db->prev_typcnt[jj] = db->typcnt[jj];
                    }

                    for (jj = 0; jj < BLOCK_MAXOPCODE; jj++) {
                        diff = db->blocktypcnt[jj] - db->prev_blocktypcnt[jj];
                        if (diff) {
                            if (hdr == 0) {
                                reqlog_logf(statlogger, REQL_INFO, hdr_fmt,
                                            db->dbnum, db->dbname);
                                hdr = 1;
                            }
                            reqlog_logf(statlogger, REQL_INFO, "    %-16s %u\n",
                                        breq2a(jj), diff);
                        }
                        db->prev_blocktypcnt[jj] = db->blocktypcnt[jj];
                    }
                    for (jj = 0; jj < MAX_OSQL_TYPES; jj++) {
                        diff = db->blockosqltypcnt[jj] -
                               db->prev_blockosqltypcnt[jj];
                        if (diff) {
                            if (hdr == 0) {
                                reqlog_logf(statlogger, REQL_INFO, hdr_fmt,
                                            db->dbnum, db->dbname);
                                hdr = 1;
                            }
                            reqlog_logf(statlogger, REQL_INFO, "    %-16s %u\n",
                                        osql_breq2a(jj), diff);
                        }
                        db->prev_blockosqltypcnt[jj] = db->blockosqltypcnt[jj];
                    }

                    diff = dbenv->txns_committed - dbenv->prev_txns_committed;
                    if (diff) {
                        if (hdr == 0) {
                            reqlog_logf(statlogger, REQL_INFO, hdr_fmt,
                                        db->dbnum, db->dbname);
                            hdr = 1;
                        }
                        reqlog_logf(statlogger, REQL_INFO, "    %-16s %u\n",
                                    "txns committed", diff);
                    }
                    dbenv->prev_txns_committed = dbenv->txns_committed;

                    diff = dbenv->txns_aborted - dbenv->prev_txns_aborted;
                    if (diff) {
                        if (hdr == 0) {
                            reqlog_logf(statlogger, REQL_INFO, hdr_fmt,
                                        db->dbnum, db->dbname);
                            hdr = 1;
                        }
                        reqlog_logf(statlogger, REQL_INFO, "    %-16s %u\n",
                                    "txns aborted", diff);
                    }
                    dbenv->prev_txns_aborted = dbenv->txns_aborted;

                    diff = db->nsql - db->prev_nsql;
                    if (diff) {
                        if (hdr == 0) {
                            reqlog_logf(statlogger, REQL_INFO, hdr_fmt,
                                        db->dbnum, db->dbname);
                            hdr = 1;
                        }
                        reqlog_logf(statlogger, REQL_INFO, "    %-16s %u\n",
                                    "nsql", diff);
                    }
                    db->prev_nsql = db->nsql;
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

    tz_set_dir(dir);
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
        tm = time_epoch() + gbl_memstat_freq;
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

    tm = time_epoch() + gbl_memstat_freq;
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
    register_int_switch("pflt_readahead",
                        "Enable prefaulting of readahead operations",
                        &gbl_prefault_readahead);
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
    register_int_switch("clnt_sql_stats", "Trace back fds to client machines",
                        &gbl_sql_client_stats);
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
    register_int_switch("qdump_atexit", "Dump queue stats at exit",
                        &gbl_dump_queues_on_exit);
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
    register_int_switch("sqlclient_use_random_readnode",
                        "Sql client will use random sql allocation by default "
                        "(while still calling sqlhndl_alloc()\n",
                        &gbl_sql_use_random_readnode);
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
    register_int_switch("catch_response_on_retry",
                        "print trace when we try to send replies on a retry",
                        &gbl_catch_response_on_retry);
    register_int_switch("requeue_on_tran_dispatch",
                        "Requeue transactional statement if not enough threads",
                        &gbl_requeue_on_tran_dispatch);
    register_int_switch("check_wrong_db",
                        "Return error if connecting to wrong database",
                        &gbl_check_wrong_db);
    register_int_switch("dbglog_use_sockpool",
                        "Use sockpool for connections opened for dbglog",
                        &gbl_use_sockpool_for_debug_logs);
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
    register_int_switch("print_deadlock_cycles", "Print all deadlock cycles",
                        &gbl_print_deadlock_cycles);
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
                        "listen on separate port for osql/signal nets",
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
        "durable_block_test",
        "Return periodic durability failures from bdb_durable_block",
        &gbl_durable_block_test);
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
                comdb2_location("marker", "%s.trap", thedb->dbs[ii]->dbname);
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

static void set_timepart_and_handle_resume_sc()
{
    /* We need to do this before resuming schema chabge , if any */
    logmsg(LOGMSG_INFO, "Reloading time partitions\n");
    thedb->timepart_views = timepart_views_init(thedb);
    if (!thedb->timepart_views)
        abort();

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
#ifdef _LINUX_SOURCE
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

    init_debug_switches();

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

    if ((rc = io_override_init()))
        return -1;

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

    if (init(argc, argv) == -1) {
        logmsg(LOGMSG_FATAL, "failed to start\n");
        exit(1);
    }

    set_datetime_dir();
    set_timepart_and_handle_resume_sc();

    repl_list_init();

    /* Creating a server context wipes out the db #'s dbcommon entries.
     * Recreate them. */
    fix_lrl_ixlen();
    create_marker_file();

    create_watchdog_thread(thedb);
    create_old_blkseq_thread(thedb);
    create_stat_thread(thedb);

    /* create the offloadsql repository */
    if (thedb->nsiblings > 0) {
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

    timprm(clean_mins, TMEV_PURGE_OLD_LONGTRN);

    if (comdb2ma_stats_cron() != 0)
        abort();

    if (process_deferred_options(thedb, DEFERRED_SEND_COMMAND, NULL,
                                 deferred_do_commands)) {
        logmsg(LOGMSG_FATAL, "failed to process deferred options\n");
        exit(1);
    }

    // db started - disable recsize kludge so
    // new schemachanges won't allow broken size.
    gbl_broken_max_rec_sz = 0;
    wait_for_coherent();

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

void delete_db(char *db_name)
{
    int idx;

    pthread_rwlock_wrlock(&thedb_lock);
    if ((idx = getdbidxbyname(db_name)) < 0) {
        logmsg(LOGMSG_FATAL, "%s: failed to find db for deletion: %s\n", __func__,
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

    pthread_rwlock_unlock(&thedb_lock);
}

void replace_db_idx(struct db *p_db, int idx, int add)
{
    int move = 0;
    pthread_rwlock_wrlock(&thedb_lock);
    if (!add && idx < 0) {
        logmsg(LOGMSG_FATAL, "%s: failed to find db for replacement: %s\n",
               __func__, p_db->dbname);
        exit(1);
    }

    if (idx < 0 || idx >= thedb->num_dbs ||
        strcasecmp(thedb->dbs[idx]->dbname, p_db->dbname) != 0) {
        thedb->dbs =
            realloc(thedb->dbs, (thedb->num_dbs + 1) * sizeof(struct db *));
        if (idx < 0 || idx >= thedb->num_dbs) idx = thedb->num_dbs;
        thedb->num_dbs++;
        move = 1;
    }

    for (int i = (thedb->num_dbs - 1); i > idx && move; i--) {
        thedb->dbs[i] = thedb->dbs[i - 1];
        thedb->dbs[i]->dbs_idx = i;
    }

    p_db->dbnum = thedb->dbs[idx]->dbnum; /* save dbnum since we can't load if
                                         * from the schema anymore */
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
               rc = load_new_table_schema_file(thedb, thedb->dbs[ii]->dbname,
                     thedb->dbs[ii]->lrlfname);
            else
               rc = load_new_table_schema_tran(thedb, NULL, thedb->dbs[ii]->dbname, thedb->dbs[ii]->csc2_schema);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "error storing schema for table '%s'\n",
                       thedb->dbs[ii]->dbname);
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
                rc = check_table_schema(thedb, thedb->dbs[ii]->dbname,
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
    int now = time_epoch();
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

static int write_pidfile(const char *pidfile)
{
    FILE *f;
    f = fopen(pidfile, "w");
    if (f == NULL) {
        logmsg(LOGMSG_ERROR, "%s %s\n", pidfile, strerror(errno));
        return -1;
    }
    fprintf(f, "%d\n", (int)getpid());
    fclose(f);
    return 0;
}

int comdb2_is_standalone(void *dbenv)
{
    return bdb_is_standalone(dbenv, thedb->bdb_env);
}

#define QUOTE_(x) #x
#define QUOTE(x) QUOTE_(x)

static int create_service_file(char *lrlname)
{
#ifdef _LINUX_SOURCE
    char *comdb2_path = comdb2_location("scripts", "comdb2");

    char *service_file =
        comdb2_asprintf("%s/%s.service", thedb->basedir, thedb->envname);
    struct passwd *pw;
    char *user;
    char lrl[PATH_MAX];
    if (lrlname) {
       if (realpath(lrlname, lrl) == NULL) {
          logmsg(LOGMSG_ERROR, "can't resolve path to lrl file\n");
       }
    }

    FILE *f = fopen(service_file, "w");

    pw = getpwuid(getuid());
    if (pw == NULL) {
        logmsg(LOGMSG_ERROR, "can't resolve current user: %d %s\n", errno,
                strerror(errno));
        free(service_file);
    }
    user = pw->pw_name;

    if (f == NULL) {
        logmsg(LOGMSG_ERROR, "can't create service file: %d %s\n", errno,
                strerror(errno));
        free(service_file);
        fclose(f);
        return 1;
    }

    fprintf(f, "[Unit]\n");
    fprintf(f, "Description=Comdb2 database instance for %s\n\n", thedb->envname);
    fprintf(f, "[Service]\n");
    fprintf(f, "ExecStart=%s --lrl %s %s\n", comdb2_path, lrl, thedb->envname);

    fprintf(f, "User=%s\n"
               "Restart=always\n"
               "RestartSec=1\n\n"
               "[Install]\n"
               "WantedBy=multi-user.target\n", user);

    fclose(f);
#endif
    return 0;
}

#undef QUOTE

/* vim: set sw=4 ts=4 et: */
