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
#include <ctype.h>
#include <errno.h>
#include <sys/resource.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "comdb2.h"
#include "tunables.h"
#include "logmsg.h"
#include "util.h"
#include "analyze.h"
#include "intern_strings.h"
#include "portmuxapi.h"

/* Maximum allowable size of the value of tunable. */
#define MAX_TUNABLE_VALUE_SIZE 512

/* Separator for composite tunable components. */
#define COMPOSITE_TUNABLE_SEP '.'

extern int gbl_allow_lua_print;
extern int gbl_allow_lua_dynamic_libs;
extern int gbl_berkdb_epochms_repts;
extern int gbl_pmux_route_enabled;
extern int gbl_allow_user_schema;
extern int gbl_test_badwrite_intvl;
extern int gbl_blocksql_grace;
extern int gbl_broken_max_rec_sz;
extern int gbl_broken_num_parser;
extern int gbl_crc32c;
extern int gbl_decom;
extern int gbl_disable_rowlocks;
extern int gbl_disable_rowlocks_logging;
extern int gbl_disable_skip_rows;
extern int gbl_disable_sql_dlmalloc;
extern int gbl_enable_berkdb_retry_deadlock_bias;
extern int gbl_enable_cache_internal_nodes;
extern int gbl_partial_indexes;
extern int gbl_sparse_lockerid_map;
extern int gbl_spstrictassignments;
extern int gbl_early;
extern int gbl_enque_reorder_lookahead;
extern int gbl_exit_alarm_sec;
extern int gbl_fdb_track;
extern int gbl_fdb_track_hints;
extern int gbl_forbid_ulonglong;
extern int gbl_force_highslot;
extern int gbl_fdb_allow_cross_classes;
extern int gbl_fdb_resolve_local;
extern int gbl_goslow;
extern int gbl_heartbeat_send;
extern int gbl_keycompr;
extern int gbl_largepages;
extern int gbl_loghist;
extern int gbl_loghist_verbose;
extern int gbl_master_retry_poll_ms;
extern int gbl_master_swing_osql_verbose;
extern int gbl_master_swing_sock_restart_sleep;
extern int gbl_max_lua_instructions;
extern int gbl_max_sqlcache;
extern int __gbl_max_mpalloc_sleeptime;
extern int gbl_mem_nice;
extern int gbl_netbufsz;
extern int gbl_net_lmt_upd_incoherent_nodes;
extern int gbl_net_max_mem;
extern int gbl_net_throttle_percent;
extern int gbl_nice;
extern int gbl_notimeouts;
extern int gbl_watchdog_disable_at_start;
extern int gbl_osql_verify_retries_max;
extern int gbl_page_latches;
extern int gbl_prefault_udp;
extern int gbl_print_syntax_err;
extern int gbl_lclpooled_buffers;
extern int gbl_reallyearly;
extern int gbl_repdebug;
extern int gbl_replicant_latches;
extern int gbl_return_long_column_names;
extern int gbl_round_robin_stripes;
extern int skip_clear_queue_extents;
extern int gbl_slow_rep_process_txn_freq;
extern int gbl_slow_rep_process_txn_maxms;
extern int gbl_sqlite_sorter_mem;
extern int gbl_survive_n_master_swings;
extern int gbl_test_blob_race;
extern int gbl_test_scindex_deadlock;
extern int gbl_test_sc_resume_race;
extern int gbl_berkdb_track_locks;
extern int gbl_udp;
extern int gbl_update_delete_limit;
extern int gbl_updategenids;
extern int gbl_use_appsock_as_sqlthread;
extern int gbl_use_node_pri;
extern int gbl_watchdog_watch_threshold;
extern int portmux_port;
extern int g_osql_blocksql_parallel_max;
extern int g_osql_max_trans;
extern int gbl_osql_max_throttle_sec;
extern int gbl_osql_random_restart;
extern int diffstat_thresh;
extern int reqltruncate;
extern int analyze_max_comp_threads;
extern int analyze_max_table_threads;
extern int gbl_block_set_commit_genid_trace;
extern int gbl_abort_on_unset_ha_flag;
extern int gbl_write_dummy_trace;
extern int gbl_abort_on_incorrect_upgrade;
extern int gbl_poll_in_pg_free_recover;
extern int gbl_print_deadlock_cycles;
extern int gbl_always_send_cnonce;
extern int gbl_rep_badgen_trace;
extern int gbl_dump_zero_coherency_timestamp;
extern int gbl_allow_incoherent_sql;
extern int gbl_rep_process_msg_print_rc;
extern int gbl_verbose_master_req;
extern int gbl_verbose_send_coherency_lease;
extern int gbl_reset_on_unelectable_cluster;
extern int gbl_rep_verify_always_grab_writelock;
extern int gbl_rep_verify_will_recover_trace;
extern int gbl_max_wr_rows_per_txn;
extern int gbl_force_serial_on_writelock;
extern int gbl_processor_thd_poll;
extern int gbl_time_rep_apply;
extern int gbl_incoherent_logput_window;
extern int gbl_dump_full_net_queue;
extern int gbl_max_clientstats_cache;
extern int gbl_decoupled_logputs;
extern int gbl_apply_queue_memory;
extern int gbl_inmem_repdb_maxlog;
extern int gbl_inmem_repdb_memory;
extern int gbl_net_writer_thread_poll_ms;
extern int gbl_max_apply_dequeue;
extern int gbl_catchup_window_trace;
extern int gbl_early_ack_trace;
extern int gbl_throttle_logput_trace;
extern int gbl_fills_waitms;
extern int gbl_finish_fill_threshold;
extern int gbl_long_read_threshold;
extern int gbl_always_ack_fills;
extern int gbl_verbose_fills;
extern int gbl_getlock_latencyms;
extern int gbl_last_locked_seqnum;
extern int gbl_set_coherent_state_trace;
extern int gbl_durable_set_trace;
extern int gbl_set_seqnum_trace;
extern int gbl_enque_log_more;
extern int gbl_trace_repmore_reqs;
extern int gbl_verbose_repdups;
extern int gbl_apply_thread_pollms;
extern int gbl_warn_queue_latency_threshold;
extern int gbl_req_all_threshold;
extern int gbl_req_delay_count_threshold;
extern int gbl_rep_verify_always_grab_writelock;
extern int gbl_rep_verify_will_recover_trace;
extern int gbl_max_wr_rows_per_txn;
extern int gbl_force_serial_on_writelock;
extern int gbl_processor_thd_poll;
extern int gbl_time_rep_apply;
extern int gbl_incoherent_logput_window;
extern int gbl_dump_full_net_queue;
extern int gbl_max_clientstats_cache;
extern int gbl_dbreg_stack_on_null_txn;
extern int gbl_dbreg_abort_on_null_txn;
extern int gbl_simulate_dropping_request;
extern int gbl_max_logput_queue;
extern int gbl_blocking_enque;
extern int gbl_master_req_waitms;
extern int gbl_print_net_queue_size;
extern int gbl_commit_delay_trace;
extern int gbl_elect_priority_bias;

extern long long sampling_threshold;

extern size_t gbl_lk_hash;
extern size_t gbl_lk_parts;
extern size_t gbl_lkr_hash;
extern size_t gbl_lkr_parts;

extern uint8_t _non_dedicated_subnet;

extern char *gbl_crypto;
extern char *gbl_spfile_name;
extern char *gbl_portmux_unix_socket;

/* util/ctrace.c */
extern int nlogs;
extern unsigned long long rollat;

/* util/thread_util.c */
extern int thread_debug;
extern int dump_resources_on_thread_exit;

/* util/walkback.c */
extern int gbl_walkback_enabled;
extern int gbl_warnthresh;

/* bdb/bdb_net.c */
extern int gbl_ack_trace;

/* bdb/bdblock.c */
extern int gbl_bdblock_debug;

extern int gbl_debug_aa;

/* bdb/os_namemangle_46.c */
extern int gbl_namemangle_loglevel;

/* berkdb/rep/rep_record.c */
extern int max_replication_trans_retries;

/* net/net.c */
extern int explicit_flush_trace;

/* bdb/genid.c */
unsigned long long get_genid(bdb_state_type *bdb_state, unsigned int dtafile);
void seed_genid48(bdb_state_type *bdb_state, uint64_t seed);

#include <stdbool.h>
extern bool gbl_rcache;

static char *name = NULL;
static int ctrace_gzip;

int gbl_ddl_cascade_drop = 1;

/*
  =========================================================
  Value/Update/Verify functions for some tunables that need
  special treatment.
  =========================================================
*/

static void *init_with_compr_value(void *context)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    return (void *)bdb_algo2compr(*(int *)tunable->var);
}

static int init_with_compr_update(void *context, void *algo)
{
    gbl_init_with_compr = bdb_compr2algo((char *)algo);
    logmsg(LOGMSG_INFO, "New tables will be compressed: %s\n",
           bdb_algo2compr(gbl_init_with_compr));
    return 0;
}

static int init_with_compr_blobs_update(void *context, void *algo)
{
    gbl_init_with_compr_blobs = bdb_compr2algo((char *)algo);
    logmsg(LOGMSG_INFO, "Blobs in new tables will be compressed: %s\n",
           bdb_algo2compr(gbl_init_with_compr_blobs));
    return 0;
}

static int init_with_rowlocks_update(void *context, void *unused)
{
    gbl_init_with_rowlocks = 1;
    return 0;
}

static int init_with_rowlocks_master_only_update(void *context, void *unused)
{
    gbl_init_with_rowlocks = 2;
    return 0;
}

/* A generic function to check if the specified number is >= 0 & <= 100. */
int percent_verify(void *unused, void *percent)
{
    if (*(int *)percent < 0 || *(int *)percent > 100) {
        logmsg(LOGMSG_ERROR,
               "Invalid value for tunable; should be in range [0, 100].\n");
        return 1;
    }
    return 0;
}

struct enable_sql_stmt_caching_st {
    const char *name;
    int code;
} enable_sql_stmt_caching_vals[] = {{"NONE", STMT_CACHE_NONE},
                                    {"PARAM", STMT_CACHE_PARAM},
                                    {"ALL", STMT_CACHE_ALL}};

static int enable_sql_stmt_caching_update(void *context, void *value)
{
    comdb2_tunable *tunable;
    char *tok;
    int st = 0;
    int ltok;
    int len;

    tunable = (comdb2_tunable *)context;
    len = strlen(value);

    tok = segtok(value, len, &st, &ltok);

    for (int i = 0; i < (sizeof(enable_sql_stmt_caching_vals) /
                         sizeof(struct enable_sql_stmt_caching_st));
         i++) {
        if (tokcmp(tok, ltok, enable_sql_stmt_caching_vals[i].name) == 0) {
            *(int *)tunable->var = enable_sql_stmt_caching_vals[i].code;
            return 0;
        }
    }

    /* Backward compatibility */
    *(int *)tunable->var = STMT_CACHE_PARAM;

    return 0;
}

static void *enable_sql_stmt_caching_value(void *context)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;

    for (int i = 0; i < (sizeof(enable_sql_stmt_caching_vals) /
                         sizeof(struct enable_sql_stmt_caching_st));
         i++) {
        if (enable_sql_stmt_caching_vals[i].code == *(int *)tunable->var) {
            return (void *)enable_sql_stmt_caching_vals[i].name;
        }
    }
    return "unknown";
}

struct checkctags_st {
    const char *name;
    int code;
} checkctags_vals[] = {{"OFF", 0}, {"FULL", 1}, {"SOFT", 2}};

static int checkctags_update(void *context, void *value)
{
    comdb2_tunable *tunable;
    char *tok;
    int st = 0;
    int ltok;
    int len;

    tunable = (comdb2_tunable *)context;
    len = strlen(value);

    tok = segtok(value, len, &st, &ltok);

    for (int i = 0;
         i < (sizeof(checkctags_vals) / sizeof(struct checkctags_st)); i++) {
        if (tokcmp(tok, ltok, checkctags_vals[i].name) == 0) {
            *(int *)tunable->var = checkctags_vals[i].code;
            return 0;
        }
    }
    return 1;
}

static void *checkctags_value(void *context)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;

    for (int i = 0;
         i < (sizeof(checkctags_vals) / sizeof(struct checkctags_st)); i++) {
        if (checkctags_vals[i].code == *(int *)tunable->var) {
            return (void *)checkctags_vals[i].name;
        }
    }
    return "unknown";
}

static void *next_genid_value(void *context)
{
    /*comdb2_tunable *tunable = (comdb2_tunable *)context;*/
    static char genid_str[64];
    unsigned long long flipgenid, genid = get_genid(thedb->bdb_env, 0);

    int *genptr = (int *)&genid, *flipptr = (int *)&flipgenid;

    flipptr[0] = htonl(genptr[1]);
    flipptr[1] = htonl(genptr[0]);

    snprintf(genid_str, sizeof(genid_str), "0x%016llx 0x%016llx %llu", genid,
             flipgenid, genid);

    return (void *)genid_str;
}

static int genid_seed_update(void *context, void *value)
{
    /*comdb2_tunable *tunable = (comdb2_tunable *)context;*/
    char *seedstr = (char *)value;
    unsigned long long seed;
    seed = strtoll(seedstr, 0, 16);
    seed_genid48(thedb->bdb_env, seed);
    return 0;
}

/*
  Enable client side retrys for n seconds. Keep blkseq's around
  for 2 * this time.
*/
static int retry_update(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    int val = *(int *)value;

    /*
      For now the proxy will treat the number 180 as meaning "this is
      old comdb2.tsk which defaults to 180 seconds", so we put in this
      HACK!!!! to get around that. Remove this hack when this build of
      comdb2.tsk is everywhere.
    */
    if (val == 180) {
        val = 181;
    }
    *(int *)tunable->var = val;
    return 0;
}

static int maxt_update(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    int val = *(int *)value;

    *(int *)tunable->var = val;

    if (gbl_maxwthreads > gbl_maxthreads) {
        logmsg(LOGMSG_INFO,
               "Reducing max number of writer threads in lrl to %d\n", val);
        gbl_maxwthreads = val;
    }

    return 0;
}

static int maxq_update(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    int val = *(int *)value;

    /* Can't be more than swapinit! */
    if ((val < 1) || (val > 1000)) {
        logmsg(LOGMSG_ERROR, "Invalid value for tunable '%s'\n", tunable->name);
        return 1;
    }

    *(int *)tunable->var = val;
    return 0;
}

static int spfile_update(void *context, void *value)
{
    comdb2_tunable *tunable;
    char *spfile_tmp;
    char *tok;
    int st = 0;
    int ltok;
    int len;

    tunable = (comdb2_tunable *)context;
    len = strlen((char *)value);
    tok = segtok(value, len, &st, &ltok);
    spfile_tmp = tokdup(tok, ltok);
    free(*(char **)tunable->var);
    *(char **)tunable->var = getdbrelpath(spfile_tmp);
    free(spfile_tmp);
    return 0;
}

extern char **qdbs;

static int num_qdbs_update(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    int val = *(int *)value;

    *(int *)tunable->var = val;
    thedb->qdbs = calloc(val, sizeof(struct dbtable *));
    qdbs = calloc(val + 1, sizeof(char *));
    return 0;
}

static int lk_verify(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;

    if ((*(int *)value <= 0) || (*(int *)value > 2048)) {
        logmsg(LOGMSG_ERROR, "Invalid value for '%s'. (range: 1-2048)\n",
               tunable->name);
        return 1;
    }
    return 0;
}

static int memnice_update(void *context, void *value)
{
    int nicerc;
    nicerc = comdb2ma_nice(*(int *)value);
    if (nicerc != 0) {
        logmsg(LOGMSG_ERROR, "Failed to change mem niceness: rc = %d\n",
               nicerc);
        return 1;
    }
    return 0;
}

static int maxretries_verify(void *context, void *value)
{
    if (*(int *)value < 2) {
        return 1;
    }
    return 0;
}

static int maxcolumns_verify(void *context, void *value)
{
    if (*(int *)value <= 0 || *(int *)value > MAXCOLUMNS) {
        return 1;
    }
    return 0;
}

static int loghist_update(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    int val;

    if ((tunable->flags & EMPTY) != 0) {
        *(int *)tunable->var = 10000;
    } else {
        *(int *)tunable->var = *(int *)value;
    }

    return 0;
}

static int page_compact_target_ff_update(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;

    if ((tunable->flags & EMPTY) != 0) {
        *(double *)tunable->var = 0.693;
    } else {
        *(double *)tunable->var = *(double *)value / 100.0F;
    }
    return 0;
}

static int page_compact_thresh_ff_update(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    double val;

    if ((tunable->flags & EMPTY) != 0) {
        val = 0.346;
    } else {
        val = *(double *)value / 100.0F;
    }

    *(double *)tunable->var = val;
    return 0;
}

/* TODO(Nirbhay) : Test */
static int blob_mem_mb_update(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    int val = *(int *)value;

    if (val == -1) {
        *(int *)tunable->var = -1;
    } else {
        *(int *)tunable->var = (1 << 20) * val;
    }
    return 0;
}

/* TODO(Nirbhay) : Test */
static int blobmem_sz_thresh_kb_update(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    *(int *)tunable->var = 1024 * (*(int *)value);
    return 0;
}

static int enable_upgrade_ahead_update(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    int val;

    if ((tunable->flags & EMPTY) != 0) {
        *(int *)tunable->var = 32;
    } else {
        *(int *)tunable->var = *(int *)value;
    }
    return 0;
}

static int broken_max_rec_sz_update(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    int val = *(int *)value;

    if (val >= 1024) {
        *(int *)tunable->var = 512;
    } else {
        *(int *)tunable->var = val;
    }
    logmsg(LOGMSG_INFO, "Allow db to start with max record size of %d\n",
           COMDB2_MAX_RECORD_SIZE + gbl_broken_max_rec_sz);
    return 0;
}

const char *deadlock_policy_str(int policy);
int deadlock_policy_max();

static int deadlock_policy_override_update(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    int val = *(int *)value;

    if (val > deadlock_policy_max()) {
        return 1;
    }

    *(int *)tunable->var = val;
    logmsg(LOGMSG_INFO, "Set deadlock policy to %s\n",
           deadlock_policy_str(val));
    return 0;
}

static int osql_heartbeat_alert_time_verify(void *context, void *value)
{
    if ((*(int *)value <= 0) || (*(int *)value > gbl_osql_heartbeat_send)) {
        logmsg(LOGMSG_ERROR, "Invalid heartbeat alert time, need to define "
                             "osql_heartbeat_send_time first.\n");
        return 1;
    }
    return 0;
}

static int simulate_rowlock_deadlock_update(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;

    if (*(int *)value == 0) {
        logmsg(LOGMSG_INFO, "Disabling rowlock_deadlock simulator.\n");
    } else if (*(int *)value < 2) {
        logmsg(LOGMSG_ERROR, "Invalid rowlock_deadlock interval.\n");
        return 1;
    } else {
        logmsg(LOGMSG_INFO, "Will throw a rowlock deadlock every %d tries.\n",
               *(int *)value);
    }

    *(int *)tunable->var = *(int *)value;
    return 0;
}

static int log_delete_before_startup_update(void *context, void *unused)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    *(int *)tunable->var = (int)time(NULL);
    return 0;
}

static int hostname_update(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    *(char **)tunable->var = intern((char *)value);
    return 0;
}

/* Forward declaration */
int ctrace_set_rollat(void *unused, void *value);

/* Return the value for sql_tranlevel_default. */
static void *sql_tranlevel_default_value()
{
    switch (gbl_sql_tranlevel_default) {
    case SQL_TDEF_COMDB2: return "COMDB2";
    case SQL_TDEF_BLOCK: return "BLOCK";
    case SQL_TDEF_SOCK: return "BLOCKSOCK";
    case SQL_TDEF_RECOM: return "RECOM";
    case SQL_TDEF_SNAPISOL: return "SNAPSHOT ISOLATION";
    case SQL_TDEF_SERIAL: return "SERIAL";
    default: return "invalid";
    }
}

static int sql_tranlevel_default_update(void *context, void *value)
{
    char *line;
    char *tok;
    int st = 0;
    int llen;
    int ltok;

    line = (char *)value;
    llen = strlen(line);

    tok = segtok(line, llen, &st, &ltok);
    if (tok == NULL) {
        logmsg(LOGMSG_USER, "expected transaction level\n");
        return 1;
    } else if (tokcmp(tok, ltok, "blocksock") == 0) {
        gbl_sql_tranlevel_default = SQL_TDEF_SOCK;
    } else if (tokcmp(tok, ltok, "recom") == 0) {
        gbl_sql_tranlevel_default = SQL_TDEF_RECOM;
    } else if (tokcmp(tok, ltok, "snapisol") == 0) {
        gbl_sql_tranlevel_default = SQL_TDEF_SNAPISOL;
    } else if (tokcmp(tok, ltok, "serial") == 0) {
        gbl_sql_tranlevel_default = SQL_TDEF_SERIAL;
    } else {
        logmsg(LOGMSG_ERROR, "Unknown transaction level requested\n");
        gbl_sql_tranlevel_default = SQL_TDEF_SOCK;
        return 1;
    }
    gbl_sql_tranlevel_preserved = gbl_sql_tranlevel_default;
    logmsg(LOGMSG_USER, "Set default transaction level to %s\n",
           (char *)sql_tranlevel_default_value());
    return 0;
}

/* Routines for the tunable system itself - tunable-specific
 * routines belong above */

static void tunable_tolower(char *str)
{
    char *tmp = str;
    while (*tmp) {
        *tmp = tolower(*tmp);
        tmp++;
    }
}

/* Get the current nice value of the process. */
static int get_nice_value()
{
    int ret;

    ret = getpriority(PRIO_PROCESS, getpid());
    if (ret == -1) {
        logmsg(LOGMSG_ERROR, "%s:%d Failed to obtain the nice value "
                             "of the process (%s)\n",
               __FILE__, __LINE__, strerror(errno));
        return 0;
    }
    return ret;
}

/* Set the default values of the tunables. */
static int set_defaults()
{
    gbl_nice = get_nice_value();
    return 0;
}

/*
  Initialize global tunables.

  @return
    0           Success
    1           Failure
*/
int init_gbl_tunables()
{
    int rc;

    /* Set the default values. */
    if ((set_defaults())) {
        logmsg(LOGMSG_ERROR, "%s:%d Failed to set the default values "
                             "for the tunables.\n",
               __FILE__, __LINE__);
        return 1;
    }

    /* Initialize dbenv::tunables. */
    if (!(gbl_tunables = calloc(1, sizeof(comdb2_tunables)))) {
        logmsg(LOGMSG_ERROR, "%s:%d Out-of-memory\n", __FILE__, __LINE__);
        return 1;
    }

    /* Initialize the tunables hash. */
    gbl_tunables->hash =
        hash_init_user((hashfunc_t *)strhashfunc, (cmpfunc_t *)strcmpfunc,
                       offsetof(comdb2_tunable, name), 0);
    logmsg(LOGMSG_DEBUG, "Global tunables hash initialized\n");

    rc = pthread_mutex_init(&gbl_tunables->mu, NULL);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR,
               "%s:%d Failed to initialize mutex for global tunables.\n",
               __FILE__, __LINE__);
        return 1;
    }

    return 0;
}

/* Free memory acquired by tunable members. */
static inline int free_tunable(comdb2_tunable *tunable)
{
    if (tunable->destroy) tunable->destroy(tunable);
    hash_del(gbl_tunables->hash, tunable);
    free(tunable->name);
    return 0;
}

/* Reclaim memory acquired by global tunables. */
int free_gbl_tunables()
{
    comdb2_tunable *tunable;
    for (int i = 0; i < gbl_tunables->count; i++) {
        free_tunable(gbl_tunables->array[i]);
        free(gbl_tunables->array[i]);
    }
    if (gbl_tunables->hash) {
        hash_clear(gbl_tunables->hash);
        hash_free(gbl_tunables->hash);
        gbl_tunables->hash = NULL;
    }
    free(gbl_tunables->array);
    pthread_mutex_destroy(&gbl_tunables->mu);
    free(gbl_tunables);
    gbl_tunables = NULL;
    return 0;
}

/*
  Register the tunable.

  @return
    0           Success
    1           Failure
*/
int register_tunable(comdb2_tunable tunable)
{
    comdb2_tunable *t;
    int already_exists = 0;
    int slot = -1;

    if ((!gbl_tunables) || (gbl_tunables->freeze == 1)) return 0;

    /*
      Check whether a tunable with the same name has already been
      registered.
    */
    if ((t = hash_find_readonly(gbl_tunables->hash, &tunable.name))) {
        /*
          Overwrite & reuse the existing slot.

          Berkdb tunables are registered during the creation of the main bdb
          environment (dbenv_open()). But, right before that, we also create
          blkseq db, which also tries to create env and thus (pre-)register
          same set of berkdb tunables. We tolerate this by simply overwriting
          them when creating the main bdb environment. The subsequent calls
          to db_env_create() has no effect here as gbl_tunables->freeze is set
          and we return above ignoring the request to (re-)register/overwrite
          same tunables all over again.

          (See bdb_open_int() & berkdb/env/env_attr.c)
        */
        for (int i = 0; i < gbl_tunables->count; i++) {
            if (gbl_tunables->array[i] == t) {
                slot = i;
                break;
            }
        }
        free_tunable(t);

        already_exists = 1;
    } else if ((t = malloc(sizeof(comdb2_tunable))) == NULL)
        goto oom_err;

    if (!tunable.name) {
        logmsg(LOGMSG_ERROR, "%s: Tunable must have a name.\n", __func__);
        goto err;
    }
    if ((t->name = strdup(tunable.name)) == NULL)
        goto oom_err;
    /* Keep tunable names in lower case (to be consistent). */
    tunable_tolower(t->name);

    t->descr = tunable.descr;

    if (tunable.type >= TUNABLE_INVALID) {
        logmsg(LOGMSG_ERROR, "%s: Tunable must have a valid type.\n", __func__);
        goto err;
    }
    t->type = tunable.type;
    if (!tunable.var && !tunable.value &&
        !(tunable.type == TUNABLE_COMPOSITE) &&
        ((tunable.flags & INTERNAL) == 0)) {
        logmsg(LOGMSG_ERROR,
               "%s: A non-composite/non-internal tunable with no var pointer "
               "set, must have its value function defined.\n",
               __func__);
        goto err;
    }
    t->var = tunable.var;

    t->flags = tunable.flags;
    t->value = tunable.value;
    t->verify = tunable.verify;
    t->update = tunable.update;
    t->destroy = tunable.destroy;

    if (already_exists) {
        assert(slot != -1);
        gbl_tunables->array[slot] = t;
    } else {
        gbl_tunables->array =
            realloc(gbl_tunables->array,
                    sizeof(comdb2_tunable *) * (gbl_tunables->count + 1));
        if (gbl_tunables->array == NULL) {
            goto oom_err;
        }
        gbl_tunables->array[gbl_tunables->count] = t;
        gbl_tunables->count++;
    }

    /* Add the tunable to the hash. */
    hash_add(gbl_tunables->hash, t);

    return 0;

err:
    logmsg(LOGMSG_ERROR, "%s: Failed to register tunable (%s).\n", __func__,
           (tunable.name) ? tunable.name : "????");
    return 1;

oom_err:
    logmsg(LOGMSG_FATAL, "%s: Out of memory\n", __func__);
    abort();
}

const char *tunable_type(comdb2_tunable_type type)
{
    switch (type) {
    case TUNABLE_INTEGER: return "INTEGER";
    case TUNABLE_DOUBLE: return "DOUBLE";
    case TUNABLE_BOOLEAN: return "BOOLEAN";
    case TUNABLE_STRING: return "STRING";
    case TUNABLE_ENUM: return "ENUM";
    case TUNABLE_COMPOSITE: return "COMPOSITE";
    default: assert(0);
    }
    return "???";
}

/* Register all db tunables. */
int register_db_tunables(struct dbenv *db)
{
#include "db_tunables.h"
    return 0;
}

/*
  Parse the given buffer for an integer and store it at the specified
  location.

  @return
    0           Success
    1           Failure
*/
int parse_int(const char *value, int *num)
{
    char *endptr;

    errno = 0;

    *num = strtol(value, &endptr, 10);

    if (errno != 0) {
        logmsg(LOGMSG_DEBUG, "parse_int(): Invalid value '%s'.\n", value);
        return 1;
    }

    if (value == endptr) {
        logmsg(LOGMSG_DEBUG, "parse_int(): No value supplied.\n");
        return 1;
    }

    if (*endptr != '\0') {
        logmsg(LOGMSG_DEBUG, "parse_int(): Couldn't fully parse the number.\n");
        return 1;
    }

    return 0;
}

/*
  Parse the given buffer for a double and store it at the specified
  location.

  @return
    0           Success
    1           Failure
*/
int parse_double(const char *value, double *num)
{
    char *endptr;

    errno = 0;

    *num = strtod(value, &endptr);

    if (errno != 0) {
        logmsg(LOGMSG_DEBUG, "parse_float(): Invalid value '%s'.\n", value);
        return 1;
    }

    if (value == endptr) {
        logmsg(LOGMSG_DEBUG, "parse_float(): No value supplied.\n");
        return 1;
    }

    if (*endptr != '\0') {
        logmsg(LOGMSG_DEBUG,
               "parse_float(): Couldn't fully parse the number.\n");
        return 1;
    }

    return 0;
}

/*
  Parse the given buffer for a boolean and store it at the specified
  location.

  @return
    0           Success
    1           Failure
*/
static int parse_bool(const char *value, int *num)
{
    int n;
    int ret;

    if (!(strncasecmp(value, "on", sizeof("on"))) ||
        !(strncasecmp(value, "yes", sizeof("yes")))) {
        *num = 1;
        return 0;
    }

    if (!(strncasecmp(value, "off", sizeof("off"))) ||
        !(strncasecmp(value, "no", sizeof("no")))) {
        *num = 0;
        return 0;
    }

    if (!(parse_int(value, &n)) && (n == 0 || n == 1)) {
        *num = n;
        return 0;
    }
    return 1;
}

/* Parse the next token and store it into a buffer. */
#define PARSE_TOKEN                                                            \
    tok = segtok((char *)value, value_len, &st, &ltok);                        \
    tokcpy0(tok, ltok, buf, MAX_TUNABLE_VALUE_SIZE);

/* Use the custom verify function if one's provided. */
#define DO_VERIFY(t, value)                                                    \
    if (t->verify && t->verify(t, (void *)value)) {                            \
        logmsg(LOGMSG_ERROR, "Invalid argument for '%s'.\n", t->name);         \
        return TUNABLE_ERR_INVALID_VALUE; /* Verification failure. */          \
    }

/*
  Use the custom update function if one's provided. Note: Its tunable's
  update() function's responsibility to check for the trailing junk in
  the value.
*/
#define DO_UPDATE(t, value)                                                    \
    ret = t->update(t, (void *)value);                                         \
    if (ret) {                                                                 \
        logmsg(LOGMSG_ERROR, "Failed to update the value of tunable '%s'.\n",  \
               t->name);                                                       \
        return TUNABLE_ERR_INTERNAL;                                           \
    }

/*
  Update the tunable.

  @return
    0           Success
    >0          Failure
*/
static comdb2_tunable_err update_tunable(comdb2_tunable *t, const char *value)
{
    char *tok;
    char buf[MAX_TUNABLE_VALUE_SIZE];
    int ret;
    int ltok;
    int st = 0;
    int value_len = strlen(value);

    assert(t);

    switch (t->type) {
    case TUNABLE_INTEGER: {
        int num;
        PARSE_TOKEN;

        if ((ret = parse_int(buf, &num))) {
            logmsg(LOGMSG_ERROR, "Invalid argument for '%s'.\n", t->name);
            return TUNABLE_ERR_INVALID_VALUE;
        }

        /*
          Verify the validity of the specified argument. We perform this
          check for all INTEGER types.
        */
        if ((t->flags & SIGNED) == 0) {
            if (((t->flags & NOZERO) != 0) && (num <= 0)) {
                logmsg(LOGMSG_ERROR,
                       "Invalid argument for '%s' (should be > 0).\n", t->name);
                return TUNABLE_ERR_INVALID_VALUE;
            } else if (num < 0) {
                logmsg(LOGMSG_ERROR,
                       "Invalid argument for '%s' (should be >= 0).\n",
                       t->name);
                return TUNABLE_ERR_INVALID_VALUE;
            }
        }

        /* Inverse the value, if needed. */
        if ((t->flags & INVERSE_VALUE) != 0) {
            num = (num != 0) ? 0 : 1;
        }

        /* Perform additional checking if defined. */
        DO_VERIFY(t, &num);

        if (t->update) {
            DO_UPDATE(t, &num);
        } else {
            *(int *)t->var = num;
        }

        logmsg(LOGMSG_DEBUG, "Tunable '%s' set to %d\n", t->name, num);
        break;
    }
    case TUNABLE_DOUBLE: {
        double num;
        PARSE_TOKEN;

        if ((ret = parse_double(buf, &num))) {
            logmsg(LOGMSG_ERROR, "Invalid argument for '%s'.\n", t->name);
            return TUNABLE_ERR_INVALID_VALUE;
        }

        /*
          Verify the validity of the specified argument. We perform this
          check for all DOUBLE types.
        */
        if ((t->flags & SIGNED) == 0) {
            if (((t->flags & NOZERO) != 0) && (num <= 0)) {
                logmsg(LOGMSG_ERROR,
                       "Invalid argument for '%s' (should be > 0).\n", t->name);
                return TUNABLE_ERR_INVALID_VALUE;
            } else if (num < 0) {
                logmsg(LOGMSG_ERROR,
                       "Invalid argument for '%s' (should be >= 0).\n",
                       t->name);
                return TUNABLE_ERR_INVALID_VALUE;
            }
        }

        /* Perform additional checking if defined. */
        DO_VERIFY(t, &num);

        if (t->update) {
            DO_UPDATE(t, &num);
        } else {
            *(double *)t->var = num;
        }

        logmsg(LOGMSG_DEBUG, "Tunable '%s' set to %f\n", t->name, num);
        break;
    }
    case TUNABLE_BOOLEAN: {
        int num;
        PARSE_TOKEN;

        if ((ret = parse_bool(buf, &num))) {
            logmsg(LOGMSG_ERROR, "Invalid argument for '%s'.\n", t->name);
            return TUNABLE_ERR_INVALID_VALUE;
        }

        /* Inverse the value, if needed. */
        if ((t->flags & INVERSE_VALUE) != 0) {
            num = (num != 0) ? 0 : 1;
        }

        /* Perform checking if defined. */
        DO_VERIFY(t, &num);

        if (t->update) {
            DO_UPDATE(t, &num);
        } else {
            *(int *)t->var = num;
        }

        logmsg(LOGMSG_DEBUG, "Tunable '%s' set to %s\n", t->name,
               (num) ? "ON" : "OFF");
        break;
    }
    case TUNABLE_STRING: {
        PARSE_TOKEN;
        DO_VERIFY(t, buf);

        if (t->update) {
            DO_UPDATE(t, buf);
        } else {
            free(*(char **)t->var);
            *((char **)t->var) = strdup(buf);
        }

        logmsg(LOGMSG_DEBUG, "Tunable '%s' set to %s\n", t->name, value);
        break;
    }
    /* ENUM types must have at least value and update functions defined. */
    case TUNABLE_ENUM: {
        /* The following 2 must be set for ENUM tunables. */
        assert(t->update);
        assert(t->value);

        PARSE_TOKEN;
        DO_VERIFY(t, buf);
        DO_UPDATE(t, buf);
        logmsg(LOGMSG_DEBUG, "Tunable '%s' set to %s\n", t->name,
               (const char *)t->value(t));
        break;
    }
    default: assert(0);
    }

    /* Check/warn for unparsed/unexpected junk in the value. */
    tok = segtok((char *)value, value_len, &st, &ltok);
    if (ltok != 0) {
        logmsg(LOGMSG_WARN,
               "Found junk in the value supplied for tunable '%s'\n", t->name);
    }
    return TUNABLE_ERR_OK;
}

/*
  Update the tunable at runtime.

  @return
    0           Success
    >0          Failure
*/
comdb2_tunable_err handle_runtime_tunable(const char *name, const char *value)
{
    comdb2_tunable *t;
    comdb2_tunable_err ret;

    assert(gbl_tunables);

    if (!(t = hash_find_readonly(gbl_tunables->hash, &name))) {
        logmsg(LOGMSG_DEBUG, "Non-registered tunable '%s'.\n", name);
        return TUNABLE_ERR_INVALID_TUNABLE;
    }

    if ((t->flags & READONLY) != 0) {
        logmsg(LOGMSG_DEBUG, "Attempt to update a READ-ONLY tunable '%s'.\n",
               name);
        return TUNABLE_ERR_READONLY;
    }

    pthread_mutex_lock(&gbl_tunables->mu);
    ret = update_tunable(t, value);
    pthread_mutex_unlock(&gbl_tunables->mu);

    return ret;
}

#define MIN(x, y) ((x) < (y) ? (x) : (y))

/*
  Update the tunable read from lrl file or updated at runtime via
  process_command().

  @return
    0           Success
    >0          Failure
*/

comdb2_tunable_err handle_lrl_tunable(char *name, int name_len, char *value,
                                      int value_len, int flags)
{
    comdb2_tunable *t;
    char buf[MAX_TUNABLE_VALUE_SIZE];
    char *tok;
    int st = 0;
    int ltok;
    int len;
    comdb2_tunable_err ret;

    assert(gbl_tunables);

    /* Avoid buffer overrun. */
    len = MIN(name_len, sizeof(buf) - 1);
    memcpy(buf, name, len);
    buf[len] = 0;
    tok = &buf[0];

    if (!(t = hash_find_readonly(gbl_tunables->hash, &tok))) {
        /* Do not warn in READEARLY phase. */
        if ((flags & READEARLY == 0)) {
            logmsg(LOGMSG_WARN, "Non-registered tunable '%s'.\n", tok);
        }
        return TUNABLE_ERR_INVALID_TUNABLE;
    }

    /* Bail out if we were asked to process READEARLY tunables only
     * but the matched tunable is non-READEARLY. */
    if ((flags & READEARLY) && ((t->flags & READEARLY) == 0)) {
        return TUNABLE_ERR_OK;
    }

    if ((flags & DYNAMIC) && ((t->flags & READONLY) != 0)) {
        logmsg(LOGMSG_ERROR, "Attempt to update a READ-ONLY tunable '%s'.\n",
               name);
        return TUNABLE_ERR_READONLY;
    }

    /* Check if we have a value specified after the name. */
    tok = segtok(value, value_len, &st, &ltok);
    if (ltok == 0) {
        /*
          No argument specified. Check if NOARG flag is
          set for the tunable, in which case its ok.
        */
        if (((t->flags & NOARG) != 0) &&
            ((t->type == TUNABLE_INTEGER) || (t->type == TUNABLE_BOOLEAN))) {

            strcpy(buf, "1");

            /*
              Also set the EMPTY flags for lower functions
              to detect that no argument was supplied.
            */
            t->flags |= EMPTY;
        } else {
            logmsg(LOGMSG_ERROR,
                   "An argument must be specified for tunable '%s'\n", t->name);
            return TUNABLE_ERR_INVALID_VALUE; /* Error */
        }
    } else {
        /* Remove leading space(s). */
        char *val = value;
        int len = value_len;
        while (*val && isspace(*val)) {
            val++;
            len--;
        }

        /* Check whether its a composite tunable. */
        if (t->type == TUNABLE_COMPOSITE) {
            /* Prepare the name of the composite tunable. */
            buf[name_len] = COMPOSITE_TUNABLE_SEP;
            memcpy(buf + name_len + 1, tok, ltok);
            /* No need to null-terminate */

            /* Fix new value and its length */
            val = tok + ltok;
            len = len - ltok;
            while (*val && isspace(*val)) {
                val++;
                len--;
            }

            return handle_lrl_tunable(buf, name_len + ltok + 1, val, len,
                                      flags);
        }

        /* Safety check. */
        if (len > sizeof(buf)) {
            logmsg(LOGMSG_ERROR, "Line too long in the lrl file.\n");
            return TUNABLE_ERR_INVALID_VALUE;
        }

        /*
          Copy rest of the value in the buffer to be processed by
          update_tunable().
        */
        memcpy(buf, val, len);
        buf[len] = 0;
    }

    ret = update_tunable(t, buf);

    /* Reset the EMPTY flag. */
    t->flags &= ~EMPTY;

    return ret;
}

const char *tunable_error(comdb2_tunable_err code)
{
    switch (code) {
    case TUNABLE_ERR_INTERNAL: return "Internal error, check server log";
    case TUNABLE_ERR_INVALID_TUNABLE: return "Invalid tunable";
    case TUNABLE_ERR_INVALID_VALUE: return "Invalid tunable value";
    case TUNABLE_ERR_READONLY:
        return "Cannot modify READ-ONLY tunable at runtime";
    }
    return "????";
}
