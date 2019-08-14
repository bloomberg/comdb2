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

#ifndef _SQL_H_
#define _SQL_H_

#include "cdb2api.h"
#include "comdb2.h"

#include <bdb_api.h>
#include <bdb_cursor.h>

#include "tag.h"
#include "osql_srs.h"
#include "osqlsqlthr.h"
#include "osqlcheckboard.h"
#include "osqlshadtbl.h"
#include "fwd_types.h"

#include "fdb_fend.h"
#include <sp.h>

/* Modern transaction modes, more or less */
enum transaction_level {
    TRANLEVEL_INVALID = -1,
    TRANLEVEL_SOSQL = 9,
    /* SQL MODE, so-called read-commited:
       - server-side parsing
       - transaction-internal updates are visible only inside transaction thread
       - external (commited) updates are visible inside transaction thread
    */
    TRANLEVEL_RECOM = 10,
    TRANLEVEL_SERIAL = 11,
    TRANLEVEL_SNAPISOL = 12
};

/* I'm now splitting handle_fastsql_requests into two functions.  The
 * outer function will maintain state (such as temporary buffers etc) while
 * the inner function will actually run the sql query.  This will allow me
 * to eventually split the socket i/o (specifically, the waiting for the next
 * query part) and the running of the query into different threads.  This way
 * we can have a small pool of sql threads with big stacks, and a large pool
 * of appsock threads with small stacks. */

#define MAX_HASH_SQL_LENGTH 8192
#define FINGERPRINTSZ 16

/* Static rootpages numbers. */
enum { RTPAGE_SQLITE_MASTER = 1, RTPAGE_START = 2 };

struct fingerprint_track {
    char fingerprint[FINGERPRINTSZ]; /* md5 digest hex string */
    int64_t count;    /* Cumulative number of times executed */
    int64_t cost;     /* Cumulative cost */
    int64_t time;     /* Cumulative preparation and execution time */
    int64_t prepTime; /* Cumulative preparation time only */
    int64_t rows;     /* Cumulative number of rows selected */
    char *zNormSql;   /* The normalized SQL query */
    size_t nNormSql;  /* Length of normalized SQL query */
};

typedef struct stmt_hash_entry {
    char sql[MAX_HASH_SQL_LENGTH];
    sqlite3_stmt *stmt;
    char *query;
    //struct schema *params_to_bind;
    LINKC_T(struct stmt_hash_entry) stmtlist_linkv;
} stmt_hash_entry_type;

struct sql_authorizer_state {
    struct sqlclntstate *clnt;         /* pointer to current client info */
    int flags;                         /* DDL, PRAGMA, CREATE TRIGGER denied? */
    int numDdls;                       /* number of DDLs found */
};

/* Thread specific sql state */
struct sqlthdstate {
    struct reqlogger *logger;
    struct sql_thread *sqlthd;
    struct thr_handle *thr_self;
    sqlite3 *sqldb;
    struct sql_authorizer_state authState; /* SQL authorizer state info */

    char lastuser[MAX_USERNAME_LEN]; // last user to use this sqlthd
    hash_t *stmt_caching_table; // statement cache table: caches vdbe engines

    LISTC_T(stmt_hash_entry_type) param_stmt_list;   // list of cached stmts
    LISTC_T(stmt_hash_entry_type) noparam_stmt_list; // list of cached stmts

    int dbopen_gen;
    int analyze_gen;
    int views_gen;
};

typedef struct osqltimings {
    unsigned long long query_received; /* query received, in need of dispatch */
    unsigned long long query_dispatched; /* start sql processing */
    unsigned long long
        query_finished; /* done processing this query (includes commit) */
    unsigned long long commit_prep;  /* start shipping requests (relevant for
                                        recom/snapisol/serial) */
    unsigned long long commit_start; /* send commit signal */
    unsigned long long commit_end;   /* received signal back (relevant for
                                        socksql/recom/snapisol,serial) */
} osqltimings_t;

typedef struct fdbtimings {
    unsigned long long
        total_time; /* total time for doing remote access, synchronous part */
    unsigned long long total_calls; /* total number of remote rcp calls */
    unsigned long long max_call;    /* longest sync call */
} fdbtimings_t;

typedef struct {
    const char *spname;
    genid_t genid;
} shadbq_t;

typedef struct osqlstate {

    /* == sql_thread == */
    char *host;              /* matching remote node */
    unsigned long long rqid; /* per node offload request session */
    uuid_t uuid;             /* session id, take 2 */
    char *tablename;         /* malloc-ed cache of send tablename for usedb */
    int tablenamelen;        /* tablename length */
    int sentops;             /* number of operations per statement */
    int tran_ops;            /* actual number of operations for a transaction */
    int replicant_numops; /* total num of ops sent by replicant to master which
                             includes USEDB, BLOB, etc. */

    SBUF2 *logsb; /* help debugging */

    osql_sqlthr_t *
        sess_blocksock; /* pointer to osql thread registration entry */

    /* == sqlclntstate == */

    int count_changes;   /* enable pragma count_changes=1, for rr, sosql, recom,
                            snapisol, serial */

    /* the phantom menace */
    LISTC_T(struct shad_tbl)
        shadtbls;    /* storage for shadow tables created by offloading */
    shadbq_t shadbq; /* storage for dbq's shadtbl */

    struct temp_table *
        verify_tbl; /* storage for verify, common for all transaction */
    struct temp_cursor *verify_cur; /* verify cursor */

    struct temp_table
        *sc_tbl; /* storage for schemachange, common for all transaction */
    struct temp_cursor *sc_cur; /* schemachange cursor */

    struct temp_table
        *bpfunc_tbl; /* storage for bpfunc, common for all transaction */
    struct temp_cursor *bpfunc_cur; /* bpfunc cursor */
    int bpfunc_seq;

    struct errstat xerr; /* extended error */

    /* performance */
    osqltimings_t timings; /* measure various timings */
    fdbtimings_t fdbtimes; /* measure remote access */

    /* verify handling */
    srs_tran_t *
        history; /* keep the log of sql strings for the current transaction */
    int replay;  /* set this when a session is replayed, used by sorese */
    int sent_column_data; /* set this if we've already sent the column data */

    /* XXX for debugging */
    char *replay_file;
    int replay_line;
    int last_replay;

    int error_is_remote; /* set if xerr is the error for a distributed tran
                            (i.e. already translated */
    int dirty; /* optimization to nop selectv only transactions */
    int running_ddl; /* ddl transaction */
    bool is_reorder_on : 1;

    /* set to 1 if we have already called osql_sock_start in socksql mode */
    bool sock_started : 1;
} osqlstate_t;

enum ctrl_sqleng {
    SQLENG_NORMAL_PROCESS =
        0, /* no user specified transactions, i.e. begin/commit */
    SQLENG_PRE_STRT_STATE =
        1, /* "begin" was submitted, mark this as user transaction begin */
    SQLENG_STRT_STATE = 2,     /* we are waiting for a non-"begin" user query */
    SQLENG_INTRANS_STATE = 3,  /* we have a transaction, ignore further
                                  BtreeTransBegin until commit/rollback */
    SQLENG_FNSH_STATE = 4,     /* "commit" was submitted */
    SQLENG_FNSH_RBK_STATE = 5, /* "rollback" was submitted */
    SQLENG_WRONG_STATE = 6,     /* duplicated command submitted */
};

void sql_set_sqlengine_state(struct sqlclntstate *clnt, char *file, int line,
                             int newstate);

typedef struct {
    enum transaction_level mode; /* TRANLEVEL_SOSQL, TRANLEVEL_RECOM, ... */

    struct cursor_tran *
        cursor_tran; /* id used to open cursors sharing same deadlock identity*/
    tran_type *
        shadow_tran; /* used to keep local changes to btree, uncommitted yet */
    tran_type *logical_tran; /* used by rowlocks ? */

    fdb_distributed_tran_t *
        dtran; /* remote transactions, contain each remote cluster tran */

    sqlite3_stmt *pStmt; /* if sql is in progress, points at the engine */
    fdb_tbl_ent_t **lockedRemTables; /* list of fdb_tbl_ent_t* for read-locked
                                        remote tables */
    int nLockedRemTables; /* number of pointers in lockedRemTablesRootp */
} dbtran_type;
typedef dbtran_type trans_t;

/* analyze sampled (previously misnamed compressed) idx */
typedef struct {
    char name[MAXTABLELEN];
    int ixnum;
    sampler_t *sampler;
    int sampling_pct;
    unsigned long long n_recs;
    unsigned long long n_sampled_recs;
} sampled_idx_t;

typedef struct sqlclntstate_fdb {
    SBUF2 *remote_sql_sb; /* IN REMOTE DB: set if this is on behalf of a remote
                             sql session */
    int flags; /* requester flags, like is this a sqlite_master special request
                  ?*/
    char *trim_key;  /* key used in prefiltering for find ops (sqlite_packed) */
    int trim_keylen; /* lenght of the trim key */
    fdb_access_t *access; /* access control */
    int version;          /* version of the remote-cached object */
    char *dbname;  /* if err is set, this indicate which fdb is responsible, if
                      any */
    char *tblname; /* if err is set, this indicate which tablename is
                      responsible */
    int code_release;    /* code release in the remote requester */
    fdb_affinity_t *aff; /* location affinity information */
    struct errstat
        xerr; /* error in fdb component, used to override sqlite and osql.xerr
                 errors */
    int preserve_err; /* set to ignore up-stream errors when lower system sets
                         xerr */
    /* source side fields */
    int n_fdb_affinities; /* number of fdbs in the fdb_ids and fdb_nodes arrays
                             */
    char **fdb_ids;       /* the fdb for which we have affinity */
    char **fdb_nodes;     /* node numbers preferred for each fdb in fdb_ids */
    int *fdb_last_status; /* used to mark a node bad after a failure */
    int failed_heartbeats; /* used to signal failed communication with remotes */
} sqlclntstate_fdb_t;

CurRange *currange_new();
#define CURRANGEARR_INIT_CAP 2
void currangearr_init(CurRangeArr *arr);
void currangearr_append(CurRangeArr *arr, CurRange *r);
CurRange *currangearr_get(CurRangeArr *arr, int n);
void currangearr_double_if_full(CurRangeArr *arr);
void currangearr_build_hash(CurRangeArr *arr);
void currangearr_free(CurRangeArr *arr);
void currangearr_print(CurRangeArr *arr);
void currange_free(CurRange *cr);

struct stored_proc;
struct lua_State;
struct dohsql;
typedef struct dohsql dohsql_t;

enum early_verify_error {
    EARLY_ERR_VERIFY = 1,
    EARLY_ERR_SELECTV = 2,
    EARLY_ERR_GENCHANGE = 3
};

enum connection_state
{
    CONNECTION_NEW,
    CONNECTION_IDLE,
    CONNECTION_RESET,
    CONNECTION_QUEUED,
    CONNECTION_RUNNING
};

enum {
  ERR_GENERIC = -1,
  ERR_PREPARE = -2,
  ERR_PREPARE_RETRY = -3,
  ERR_ROW_HEADER = -4,
  ERR_CONVERSION_DT = -5,
};


#define RESPONSE_TYPES                                                         \
    XRESPONSE(RESPONSE_COLUMNS)                                                \
    XRESPONSE(RESPONSE_COLUMNS_LUA)                                            \
    XRESPONSE(RESPONSE_COLUMNS_STR)                                            \
    XRESPONSE(RESPONSE_COST)                                                   \
    XRESPONSE(RESPONSE_DEBUG)                                                  \
    XRESPONSE(RESPONSE_EFFECTS)                                                \
    XRESPONSE(RESPONSE_ERROR)                                                  \
    XRESPONSE(RESPONSE_ERROR_ACCESS)                                           \
    XRESPONSE(RESPONSE_ERROR_BAD_STATE)                                        \
    XRESPONSE(RESPONSE_ERROR_PREPARE)                                          \
    XRESPONSE(RESPONSE_ERROR_PREPARE_RETRY)                                    \
    XRESPONSE(RESPONSE_ERROR_REJECT)                                           \
    XRESPONSE(RESPONSE_FLUSH)                                                  \
    XRESPONSE(RESPONSE_HEARTBEAT)                                              \
    XRESPONSE(RESPONSE_ROW)                                                    \
    XRESPONSE(RESPONSE_ROW_LAST)                                               \
    XRESPONSE(RESPONSE_ROW_LAST_DUMMY)                                         \
    XRESPONSE(RESPONSE_ROW_LUA)                                                \
    XRESPONSE(RESPONSE_ROW_STR)                                                \
    XRESPONSE(RESPONSE_TRACE)

#define XRESPONSE(x) x,
enum WriteResponsesEnum { RESPONSE_TYPES };
#undef XRESPONSE

/* read response */
enum {
    RESPONSE_PING_PONG,
    RESPONSE_SP_CMD,
};

struct response_data {
    sqlite3_stmt *stmt;
    struct errstat *err;
    uint64_t row_id;
    /* For RESPONSE_COLUMNS_LUA, RESPONSE_ROW_LUA */
    int ncols;
    int pingpong;
    struct stored_proc *sp;
};

char *sp_column_name(struct response_data *, int);
int sp_column_type(struct response_data *, int, size_t, int);
int sp_column_nil(struct response_data *, int);
int sp_column_val(struct response_data *, int, int, void *);
void *sp_column_ptr(struct response_data *, int, int, size_t *);

typedef int(plugin_func)(struct sqlclntstate *);
typedef int(response_func)(struct sqlclntstate *, int, void *, int);
typedef void *(replay_func)(struct sqlclntstate *, void *);
typedef int(param_index_func)(struct sqlclntstate *, const char *, int64_t *);
typedef int(param_value_func)(struct sqlclntstate *, struct param_data *, int);
typedef int(cnonce_value_func)(struct sqlclntstate *, snap_uid_t *);
typedef int(get_snapshot_func)(struct sqlclntstate *, int *, int *);
typedef void(add_steps_func)(struct sqlclntstate *, double steps);
typedef void(setup_client_info_func)(struct sqlclntstate *, struct sqlthdstate *, char *);
typedef int(skip_row_func)(struct sqlclntstate *, uint64_t);
typedef int(log_context_func)(struct sqlclntstate *, struct reqlogger *);
typedef uint64_t(ret_uint64_func)(struct sqlclntstate *);
typedef int(override_type_func)(struct sqlclntstate *, int);

#define SQLITE_CALLBACK_API(ret, name)                                         \
    ret (*column_##name)(struct sqlclntstate *, sqlite3_stmt *, int)

struct plugin_callbacks {
    response_func *write_response; /* newsql_write_response */
    response_func *read_response; /* newsql_read_response */

    replay_func *save_stmt; /* newsql_save_stmt */
    replay_func *restore_stmt; /* newsql_restore_stmt */
    replay_func *destroy_stmt; /* newsql_destroy_stmt */
    replay_func *print_stmt; /* newsql_print_stmt */

    // bound params
    plugin_func *param_count; /* newsql_param_count */
    param_index_func *param_index; /* newsql_param_index */
    param_value_func *param_value; /* newsql_param_value */

    // run_statement_typed
    plugin_func *override_count; /* newsql_override_count */
    override_type_func *override_type; /* newsql_override_type */

    plugin_func *has_cnonce; /* newsql_has_cnonce */
    plugin_func *set_cnonce; /* newsql_set_cnonce */
    plugin_func *clr_cnonce; /* newsql_clr_cnonce */
    cnonce_value_func *get_cnonce; /* newsql_has_cnonce */

    get_snapshot_func *get_snapshot; /* newsql_get_snapshot */
    plugin_func *upd_snapshot; /* newsql_update_snapshot */
    plugin_func *clr_snapshot; /* newsql_clear_snapshot */

    plugin_func *has_high_availability; /* newsql_has_high_availability */
    plugin_func *set_high_availability; /* newsql_set_high_availability */
    plugin_func *clr_high_availability; /* newsql_clr_high_availability */
    plugin_func *get_high_availability; /* newsql_get_high_availability*/
    plugin_func *has_parallel_sql;      /* newsql_has_parallel_sql */

    add_steps_func *add_steps; /* newsql_add_steps */
    setup_client_info_func *setup_client_info; /* newsql_setup_client_info */
    skip_row_func *skip_row; /* newsql_skip_row */
    log_context_func *log_context; /* newsql_log_context */
    ret_uint64_func *get_client_starttime; /* newsql_get_client_starttime */
    plugin_func *get_client_retries;       /* newsql_get_client_retries */
    plugin_func *send_intrans_response; /* newsql_send_intrans_response */
    /* optional */
    void *state;
    int (*column_count)(struct sqlclntstate *,
                        sqlite3_stmt *); /* sqlite3_column_count */
    int (*next_row)(struct sqlclntstate *, sqlite3_stmt *); /* sqlite3_step */
    SQLITE_CALLBACK_API(int, type);                   /* sqlite3_column_type */
    SQLITE_CALLBACK_API(sqlite_int64, int64);         /* sqlite3_column_int64*/
    SQLITE_CALLBACK_API(double, double);              /* sqlite3_column_double*/
    SQLITE_CALLBACK_API(const unsigned char *, text); /* sqlite3_column_text */
    SQLITE_CALLBACK_API(int, bytes);                  /* sqlite3_column_bytes */
    SQLITE_CALLBACK_API(const void *, blob);          /* sqlite3_column_bytes */
    SQLITE_CALLBACK_API(const dttz_t *, datetime); /* sqlite3_column_datetime */
    const intv_t *(*column_interval)(struct sqlclntstate *, sqlite3_stmt *, int,
                                     int); /* sqlite3_column_interval*/
    int (*sqlite_error)(struct sqlclntstate *, sqlite3_stmt *,
                        const char **errstr); /* sqlite3_errcode */
};

#define make_plugin_callback(clnt, name, func)                                 \
    (clnt)->plugin.func = name##_##func
#define make_plugin_optional_null(clnt, name)                                  \
    (clnt)->plugin.column_##name = NULL

#define plugin_set_callbacks(clnt, name)                                       \
    do {                                                                       \
        make_plugin_callback(clnt, name, write_response);                      \
        make_plugin_callback(clnt, name, read_response);                       \
        make_plugin_callback(clnt, name, save_stmt);                           \
        make_plugin_callback(clnt, name, restore_stmt);                        \
        make_plugin_callback(clnt, name, destroy_stmt);                        \
        make_plugin_callback(clnt, name, print_stmt);                          \
        make_plugin_callback(clnt, name, param_count);                         \
        make_plugin_callback(clnt, name, param_index);                         \
        make_plugin_callback(clnt, name, param_value);                         \
        make_plugin_callback(clnt, name, override_count);                      \
        make_plugin_callback(clnt, name, override_type);                       \
        make_plugin_callback(clnt, name, has_cnonce);                          \
        make_plugin_callback(clnt, name, set_cnonce);                          \
        make_plugin_callback(clnt, name, clr_cnonce);                          \
        make_plugin_callback(clnt, name, get_cnonce);                          \
        make_plugin_callback(clnt, name, get_snapshot);                        \
        make_plugin_callback(clnt, name, upd_snapshot);                        \
        make_plugin_callback(clnt, name, clr_snapshot);                        \
        make_plugin_callback(clnt, name, has_high_availability);               \
        make_plugin_callback(clnt, name, set_high_availability);               \
        make_plugin_callback(clnt, name, clr_high_availability);               \
        make_plugin_callback(clnt, name, get_high_availability);               \
        make_plugin_callback(clnt, name, has_parallel_sql);                    \
        make_plugin_callback(clnt, name, add_steps);                           \
        make_plugin_callback(clnt, name, setup_client_info);                   \
        make_plugin_callback(clnt, name, skip_row);                            \
        make_plugin_callback(clnt, name, log_context);                         \
        make_plugin_callback(clnt, name, get_client_starttime);                \
        make_plugin_callback(clnt, name, get_client_retries);                  \
        make_plugin_callback(clnt, name, send_intrans_response);               \
        make_plugin_optional_null(clnt, count);                                \
        make_plugin_optional_null(clnt, type);                                 \
        make_plugin_optional_null(clnt, int64);                                \
        make_plugin_optional_null(clnt, double);                               \
        make_plugin_optional_null(clnt, text);                                 \
        make_plugin_optional_null(clnt, bytes);                                \
        make_plugin_optional_null(clnt, blob);                                 \
        make_plugin_optional_null(clnt, datetime);                             \
        make_plugin_optional_null(clnt, interval);                             \
        (clnt)->plugin.state = NULL;                                           \
        (clnt)->plugin.next_row = NULL;                                        \
    } while (0)

int param_count(struct sqlclntstate *);
int param_index(struct sqlclntstate *, const char *, int64_t *);
int param_value(struct sqlclntstate *, struct param_data *, int);
int override_count(struct sqlclntstate *);
int override_type(struct sqlclntstate *, int);
int get_cnonce(struct sqlclntstate *, snap_uid_t *);
int has_high_availability(struct sqlclntstate *);
int has_parallel_sql(struct sqlclntstate *);
int set_high_availability(struct sqlclntstate *);
int clr_high_availability(struct sqlclntstate *);
uint64_t get_client_starttime(struct sqlclntstate *);
int get_client_retries(struct sqlclntstate *);

struct clnt_ddl_context {
    /* Name of the table */
    char *name;
    /* Pointer to a comdb2_ddl_context */
    void *ctx;
    /* Memory allocator of the comdb2_ddl_context */
    comdb2ma mem;
};

#if INSTRUMENT_RECOVER_DEADLOCK_FAILURE
#define RECOVER_DEADLOCK_MAX_STACK 16348
#endif

#define HINT_LEN 127
enum cache_status {
    CACHE_DISABLED = 0,
    CACHE_HAS_HINT = 1,
    CACHE_FOUND_STMT = 2,
    CACHE_FOUND_STR = 4,
};
enum prepare_flags {
    PREPARE_NONE = 0,
    PREPARE_RECREATE = 1,
    PREPARE_DENY_CREATE_TRIGGER = 2,
    PREPARE_DENY_PRAGMA = 4,
    PREPARE_DENY_DDL = 8,
    PREPARE_IGNORE_ERR = 16,
    PREPARE_NO_NORMALIZE = 32
};
struct sql_state {
    enum cache_status status;          /* populated by get_prepared_stmt */
    sqlite3_stmt *stmt;                /* cached engine, if any */
    char cache_hint[HINT_LEN];         /* hint copy, if any */
    const char *sql;                   /* the actual string used */
    stmt_hash_entry_type *stmt_entry;  /* fast pointer to hashed record */
    int prepFlags;                     /* flags to get_prepared_stmt_int */
};

/* This structure is designed to hold several pieces of data related to
 * work-in-progress on client SQL requests. */
struct sqlworkstate {
    const char *zNormSql; /* Normalized version of latest SQL query. */
    char *zOrigNormSql;   /* Normalized version of original SQL query. */
    struct sql_state rec; /* Prepared statement for original SQL query. */
    unsigned char aFingerprint[FINGERPRINTSZ]; /* MD5 of normalized SQL. */
};

struct sql_hist_cost {
    double cost;
    int64_t time;
    int64_t prepTime;
    int64_t rows;
};

/* Client specific sql state */
struct sqlclntstate {
    struct sqlworkstate work;  /* This is the primary data related to the SQL
                                * client request in progress.  This includes
                                * the original SQL query and its normalized
                                * variant (if applicable). */

    /* appsock plugin specific data */
    void *appdata;
    struct plugin_callbacks plugin;

    dbtran_type dbtran;
    pthread_mutex_t dtran_mtx; /* protect dbtran.dtran, if any,
                                  for races betweem sql thread created and
                                  other readers, like appsock */
    SBUF2 *sb;
    int must_close_sb;

    /* These are only valid while a query is in progress and will point into
     * the i/o thread's buf */
    char *sql;
    int recno;
    int client_understands_query_stats;
    char tzname[CDB2_MAX_TZNAME];
    int dtprec;
    struct conninfo conninfo;

    /* For SQL engine dispatch. */
    int inited_mutex;
    pthread_mutex_t wait_mutex;
    pthread_cond_t wait_cond;
    pthread_mutex_t write_lock;
    pthread_cond_t write_cond;
    int query_rc;

    struct rawnodestats *rawnodestats;

    osqlstate_t osql;                /* offload sql state is kept here */
    enum ctrl_sqleng ctrl_sqlengine; /* use to mark a begin/end out of state,
                                        see enum ctrl_sqleng
                                     */
    int intrans; /* THIS FIELD IS USED BY sqlglue.c TO RECORD THE ENTRANCE (=1)
                   AND THE EXIT(=0) in a sql transaction marked by a succesfull
                   call to BeginTrans, and Commit/Rollback respectively
                   THIS DOES NOT MATCH THE CLIENT TRANSACTION EXCERPT FOR
                   SINGULAR STATEMENTS;
                   STATE OF A CLIENT TRANSACTION IS KEPT HERE
                 */
    struct convert_failure fail_reason; /* detailed error */
    int early_retry;

    /* analyze variables */
    int n_cmp_idx;
    sampled_idx_t *sampled_idx_tbl;

    int debug_sqlclntstate;
    int last_check_time;
    int query_timeout;
    int stop_this_statement;
    int statement_timedout;
    struct conninfo conn;

    uint8_t heartbeat;
    uint8_t ready_for_heartbeats;
    uint8_t no_more_heartbeats;
    uint8_t done;

    int using_case_insensitive_like;
    int deadlock_recovered;

    /* lua stored procedure */
    int trans_has_sp;
    struct stored_proc *sp;
    int exec_lua_thread;
    int want_stored_procedure_trace;
    int want_stored_procedure_debug;
    char spname[MAX_SPNAME + 1];
    struct spversion_t spversion;

    unsigned int bdb_osql_trak; /* 32 debug bits interpreted by bdb for your
                                   "set debug bdb"*/
    struct client_query_stats *query_stats;

    SBUF2 *dbglog;
    int queryid;
    unsigned long long dbglog_cookie;
    unsigned long long master_dbglog_cookie;

    int have_query_limits;
    struct query_limits limits;

    struct query_effects effects;
    struct query_effects log_effects;
    int64_t nsteps;

    int have_user;
    char user[MAX_USERNAME_LEN];
    int is_x509_user; /* True if the user is retrieved
                         from a client certificate. */

    int have_password;
    char password[MAX_PASSWORD_LEN];

    int authgen;

    int no_transaction;

    int have_extended_tm;
    int extended_tm;

    char *origin;
    char origin_space[255];
    uint8_t dirty[256]; /* We can track upto 2048 tables */

    int had_errors; /* to remain compatible with blocksql: if a user starts a
                       transaction, we
                       need to pend the first error until a commit is issued.
                       any statements
                       past the first error are ignored. */
    int in_client_trans; /* clnt is in a client transaction (ie: client ran
                            "begin" but not yet commit or abort) */
    char *saved_errstr;  /* if had_errors, save the error string */
    int saved_rc;        /* if had_errors, save the return code */

    int prep_rc;    /* last value returned from sqlite3_prepare_v3() */
    int step_rc;    /* last value returned from sqlite3_step() */
    int isselect;   /* track if the query is a select query.*/
    int isUnlocked;
    int writeTransaction;
    int prepare_only;
    int verify_retries; /* how many verify retries we've borne */
    int verifyretry_off;
    int pageordertablescan;
    int snapshot; /* snapshot epoch placeholder */
    int snapshot_file;
    int snapshot_offset;
    int is_hasql_retry;
    int is_readonly;
    int is_expert;
    int added_to_hist;

    struct thr_handle *thr_self;
    arch_tid appsock_id;
    int holding_pagelocks_flag; /* Rowlocks optimization */

    int *hinted_cursors;
    int hinted_cursors_alloc;
    int hinted_cursors_used;

    /* remote settings, used in run_sql */
    sqlclntstate_fdb_t fdb_state;

    int nrows;
    struct sql_hist_cost spcost;

    int planner_effort;
    int osql_max_trans;
    /* read-set validation */
    CurRangeArr *arr;
    CurRangeArr *selectv_arr;
    char *prev_cost_string;

    int num_retry;
    unsigned int file;
    unsigned int offset;

    uint64_t enque_timeus;
    uint64_t deque_timeus;

    /* due to some sqlite vagaries, cursor is closed
       and I lose the side row; cache it here! */
    unsigned long long keyDdl;
    int nDataDdl;
    char *dataDdl;

    /* partial indexes */
    unsigned long long ins_keys;
    unsigned long long del_keys;
    int has_sqliterow;
    int verify_indexes;
    void *schema_mems;

    /* indexes on expressions */
    uint8_t **idxInsert;
    uint8_t **idxDelete;

    int8_t wrong_db;
    int8_t is_lua_sql_thread;

    int8_t high_availability_flag;
    int8_t hasql_on;

    int8_t has_recording;
    int8_t is_retry;
    int8_t get_cost;
    int8_t is_explain;
    uint8_t is_analyze;
    uint8_t is_overlapping;
    uint32_t init_gen;
    int8_t gen_changed;
    uint8_t skip_peer_chk;
    uint8_t queue_me;
    uint8_t fail_dispatch;

    int ncontext;
    char **context;

    hash_t *ddl_tables;
    hash_t *dml_tables;
    hash_t *ddl_contexts;

    int ignore_coherency;
    int statement_query_effects;

    int verify_remote_schemas;

    /* sharding scheme */
    dohsql_t *conns;
    int nconns;
    int conns_idx;
    int shard_slice;

    char *argv0;
    char *stack;

    int translevel_changed;
    int admin;

    uint32_t start_gen;
    int emitting_flag;
    int need_recover_deadlock;
    int recover_deadlock_rcode;
    int heartbeat_lock;
#ifdef INSTRUMENT_RECOVER_DEADLOCK_FAILURE
    const char *recover_deadlock_func;
    int recover_deadlock_line;
    pthread_t recover_deadlock_thd;
    char recover_deadlock_stack[RECOVER_DEADLOCK_MAX_STACK];
#endif
    struct sqlthdstate *thd;
    int had_lease_at_begin;

    int64_t connid;
    int64_t total_sql;
    int64_t sql_since_reset;
    int64_t num_resets;
    time_t connect_time;
    time_t last_reset_time;
    int state_start_time;
    enum connection_state state;
    pthread_mutex_t state_lk;
    /* The node doesn't change.  The pid does as connections get donated.  We
     * latch both values here since conninfo is lost when connections are reset. */
    int last_pid;
    char* origin_host;
    int8_t sent_data_to_client;
    int8_t is_asof_snapshot;
    LINKC_T(struct sqlclntstate) lnk;
};

/* Query stats. */
struct query_path_component {
    char lcl_tbl_name[MAXTABLELEN];
    char rmt_db[MAX_DBNAME_LENGTH];
    int ix;
    int nfind;
    int nnext;
    int nwrite;
    int nblobs;
    LINKC_T(struct query_path_component) lnk;
};

struct temptable {
    int rootpage;
    struct temp_cursor *cursor;
    struct temp_table *tbl;
    int flags;
    Btree *owner;
    pthread_mutex_t *lk;
};

struct Btree {
    /* for debugging */
    int btreeid;
    struct reqlogger *reqlogger;

    bdb_temp_hash *genid_hash; /* rrn hash for non dtastripe support */

    LISTC_T(BtCursor) cursors;

    unsigned is_temporary : 1;
    unsigned is_hashtable : 1;
    unsigned is_remote : 1;

    hash_t *temp_tables;
    int num_temp_tables;

    void *schema;
    void (*free_schema)(void *);

    char *zFilename;
    fdb_t *fdb;
};

enum { CFIRST, CNEXT, CPREV, CLAST };

typedef enum {
    CURSORCLASS_TEMPTABLE = 1,
    CURSORCLASS_SQLITEMASTER,
    CURSORCLASS_TABLE,
    CURSORCLASS_INDEX,
    CURSORCLASS_STAT24,
    CURSORCLASS_REMOTE,
} cursorclass_type;


struct BtCursor {
    /* direct pointers to stuff -- avoids thread local lookup */
    struct sqlclntstate *clnt;
    struct sql_thread *thd;
    sqlite3 *sqlite;
    Vdbe *vdbe;
    Btree *bt;
    struct dbtable *db;

    int rootpage;

    /* various buffers: */
    uint8_t writeTransaction; /* save tran type during cursor open */
    void *ondisk_buf;         /* ondisk data */
    void *ondisk_key; /* ondisk key. this is effectively also the pointer into
                         the index */

    void *lastkey; /* last key: swap with ondisk_key for subsequent lookups */
    void *fndkey;  /* this key is actually found */

    int eof;   /* we reached the end of an index, but the current entry still
                  contains valid data */
    int empty; /* there are no entries in the db - no results to return for any
                  query */
    LINKC_T(BtCursor) lnk;

    /* these are sqlite format buffers */
    void *dtabuf;
    int dtabuflen;
    void *keybuf;
    int keybuflen;

    int dtabuf_alloc;
    int keybuf_alloc;
    int ondisk_dtabuf_alloc;
    int ondisk_keybuf_alloc;

    int tblnum;
    int ixnum;

    int cursorid; /* for debugging */
    struct reqlogger *reqlogger;
    int rrn; /* record number */
    char sqlrrn[5];
    int sqlrrnlen;
    unsigned long long genid;

    struct KeyInfo *pKeyInfo;

    /* special case for master table: the table is fake,
       just keep track of which entry we are pointing to */
    int tblpos;
    fdb_tbl_ent_t *crt_sqlite_master_row;

    /* special case for a temp table: pointer to a temp table handle */
    struct temptable *tmptable;

    sampler_t *sampler;

    blob_status_t blobs;

    bdb_cursor_ifn_t *bdbcur;

    int nmove, nfind, nwrite;
    int nblobs;
    int num_nexts;

    int numblobs;

    struct schema *sc; /* points to the schema for the underlying table for
                          this cursor */

    cursorclass_type
        cursor_class; /* TEMPTABLE, SQLITEMASTER, TABLE, INDEX, STAT2 */

    void *shadtbl; /* fast pointer to shadows, used during transaction */

    int next_is_eof; /* see comments in sqlite3BtreeMoveto */
    int prev_is_eof; /* see comments in sqlite3BtreeMoveto */

    bdb_cursor_ser_t cur_ser;

    /* move me */
    int (*cursor_move)(BtCursor *, int *pRes, int how);
    /* temptables have these -- lua ones need locking */
    int (*cursor_del)(bdb_state_type *, struct temp_cursor *, int *bdberr,
                      BtCursor *);
    int (*cursor_put)(bdb_state_type *, struct temp_table *, void *key,
                      int keylen, void *data, int dtalen, void *unpacked,
                      int *bdberr, BtCursor *);
    int (*cursor_close)(bdb_state_type *, BtCursor *, int *bdberr);
    int (*cursor_find)(bdb_state_type *, struct temp_cursor *, const void *key,
                       int keylen, void *unpacked, int *bdberr, BtCursor *);
    unsigned long long (*cursor_rowid)(struct temp_table *tbl, BtCursor *);
    int (*cursor_count)(BtCursor *, long long *);

    double find_cost;
    double move_cost;
    double write_cost;
    double blob_cost;

    int nCookFields;
    uint8_t
        is_recording; /* set for indexes; will store deep copies of data&blobs
                               to prevent verify errors */
    uint8_t is_sampled_idx; /* set to 1 if this is a sampled (previously
                               misnamed compressed) index */
    uint8_t is_btree_count;

    uint8_t on_list;

    blob_status_t blob_descriptor;
    int have_blob_descriptor;

    unsigned long long last_cached_genid;

    /* remotes */
    fdb_cursor_if_t *fdbc;

    /* cursor access range */
    CurRange *range;
    unsigned char is_equality; /* sqlite will "hint" back if a SeekGE is
                                  actually a SeekEQ */

    unsigned long long col_mask; /* tracking first 63 columns, if bit is set,
                                    column is needed */

    unsigned long long keyDdl; /* rowid for side DDL row */
    char *dataDdl;             /* DDL row, cached during CREATE operations */
    int nDataDdl;   /* length of the cached row for DDL instructions */
    int open_flags; /* flags used to open it */

    int tableversion;
};

struct sql_hist {
    LINKC_T(struct sql_hist) lnk;
    char *sql;
    struct sql_hist_cost cost;
    int when;
    int64_t txnid;
    struct conninfo conn;
};

struct sql_thread {
    LINKC_T(struct sql_thread) lnk;
    pthread_mutex_t lk;
    struct Btree *bt, *bttmp;
    int startms;
    int prepms;
    int stime;
    int nmove;
    int nfind;
    int nwrite;
    int bufsz;
    int id;
    char *buf;
    LISTC_T(struct query_path_component) query_stats;
    hash_t *query_hash;
    double cost;
    struct sqlclntstate *clnt;
    /* custom error message to send to client */
    char *error;
    struct master_entry *rootpages;
    int rootpage_nentries;
    int selective_rootpages;
    unsigned char had_temptables;
    unsigned char had_tablescans;

    /* current shard; cut 0 we support only one partition */
    int crtshard;
};

struct connection_info {
    char *host;
    int64_t connection_id;
    int64_t pid;
    int64_t total_sql;
    int64_t sql_since_reset;
    int64_t num_resets;
    int64_t steps;
    cdb2_client_intv_ds_t time_in_state; 
    cdb2_client_datetime_t connect_time;
    cdb2_client_datetime_t last_reset_time;
    char *state;
    char *sql;

    /* latched in sqlinterfaces, not returned */ 
    time_t connect_time_int;
    time_t last_reset_time_int;
    int node_int;
    int time_in_state_int;
    enum connection_state state_int;
};

/* makes master swing verbose */
extern int gbl_master_swing_osql_verbose;
/* for testing: sleep in osql_sock_restart when master swings */
extern int gbl_master_swing_sock_restart_sleep;

/* takes care of both stat1 and stat2 */
#define is_sqlite_stat(x)                                                      \
    strncmp((x), "sqlite_stat", sizeof("sqlite_stat") - 1) == 0

#define is_stat1(x) (strcmp((x), "sqlite_stat1") == 0)
#define is_stat2(x) (strcmp((x), "sqlite_stat2") == 0)
#define is_stat4(x) (strcmp((x), "sqlite_stat4") == 0)

/* functions to get/put a locker id to be used for all nontransactional cursors
 */
int get_curtran(bdb_state_type *bdb_state, struct sqlclntstate *clnt);
int put_curtran(bdb_state_type *bdb_state, struct sqlclntstate *clnt);
int get_curtran_flags(bdb_state_type *bdb_state, struct sqlclntstate *clnt,
                      uint32_t flags);
int put_curtran_flags(bdb_state_type *bdb_state, struct sqlclntstate *clnt,
                      uint32_t flags);

unsigned long long osql_log_time(void);
void osql_log_time_done(struct sqlclntstate *clnt);

int dispatch_sql_query(struct sqlclntstate *clnt);
int wait_for_sql_query(struct sqlclntstate *clnt);
void signal_clnt_as_done(struct sqlclntstate *clnt);

int handle_sql_begin(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                     int sendresponse);
int handle_sql_commitrollback(struct sqlthdstate *thd,
                              struct sqlclntstate *clnt, int sendresponse);

int replicant_is_able_to_retry(struct sqlclntstate *clnt);
void sql_get_query_id(struct sql_thread *thd);

void sql_dlmalloc_init(void);
int sql_mem_init(void *dummy);
void sql_mem_shutdown(void *dummy);

int sqlite3_open_serial(const char *filename, sqlite3 **, struct sqlthdstate *);

void reset_clnt(struct sqlclntstate *, SBUF2 *, int initial);
void cleanup_clnt(struct sqlclntstate *);
void reset_query_effects(struct sqlclntstate *);

int sqlite_to_ondisk(struct schema *s, const void *inp, int len, void *outp,
                     const char *tzname, blob_buffer_t *outblob, int maxblobs,
                     struct convert_failure *fail_reason, BtCursor *pCur);

int has_sqlcache_hint(const char *sql, const char **start, const char **end);

void clnt_reset_cursor_hints(struct sqlclntstate *clnt);

void sqlite3VdbeRecordPack(UnpackedRecord *unpacked, Mem *pOut);
char *sql_field_default_trans(struct field *f, int is_out);

void fdb_packedsqlite_process_sqlitemaster_row(char *row, int rowlen,
                                               char **etype, char **name,
                                               char **tbl_name, int *rootpage,
                                               char **sql, char **csc2,
                                               unsigned long long *version,
                                               int new_rootpage);

int fdb_packedsqlite_extract_genid(char *key, int *outlen, char *outbuf);

unsigned long long comdb2_table_version(const char *tablename);

int fdb_add_remote_time(BtCursor *pCur, unsigned long long start,
                        unsigned long long end);

int sqlite3LockStmtTables(sqlite3_stmt *pStmt);
int sqlite3UnlockStmtTablesRemotes(struct sqlclntstate *clnt);
void sql_remote_schema_changed(struct sqlclntstate *clnt, sqlite3_stmt *pStmt);
int release_locks_on_emit_row(struct sqlthdstate *thd,
                              struct sqlclntstate *clnt);

void clearClientSideRow(struct sqlclntstate *clnt);
void comdb2_set_tmptbl_lk(pthread_mutex_t *);
struct temptable get_tbl_by_rootpg(const sqlite3 *, int);
void clone_temp_table(sqlite3 *, const sqlite3 *, const char *,
                      struct temptable *);
int sqlengine_prepare_engine(struct sqlthdstate *, struct sqlclntstate *,
                             int recreate);
int sqlserver2sqlclient_error(int rc);
uint16_t stmt_num_tbls(sqlite3_stmt *);
int newsql_dump_query_plan(struct sqlclntstate *clnt, sqlite3 *hndl);
void init_cursor(BtCursor *, Vdbe *, Btree *);
void run_stmt_setup(struct sqlclntstate *, sqlite3_stmt *);
int sql_index_name_trans(char *namebuf, int len, struct schema *schema,
                         struct dbtable *db, int ixnum, void *trans);

int get_prepared_stmt(struct sqlthdstate *, struct sqlclntstate *,
                      struct sql_state *, struct errstat *, int);
int get_prepared_stmt_try_lock(struct sqlthdstate *, struct sqlclntstate *,
                               struct sql_state *, struct errstat *, int);
void put_prepared_stmt(struct sqlthdstate *, struct sqlclntstate *,
                       struct sql_state *, int outrc);
void sqlengine_thd_start(struct thdpool *, struct sqlthdstate *, enum thrtype);
void sqlengine_thd_end(struct thdpool *, struct sqlthdstate *);

int get_data(BtCursor *pCur, struct schema *sc, uint8_t *in, int fnum, Mem *m,
             uint8_t flip_orig, const char *tzname);

#define cur_is_remote(pCur) (pCur->cursor_class == CURSORCLASS_REMOTE)

response_func write_response;
response_func read_response;
int sql_writer(SBUF2 *, const char *, int);
int typestr_to_type(const char *ctype);
int column_count(struct sqlclntstate *, sqlite3_stmt *);
int sqlite_error(struct sqlclntstate *, sqlite3_stmt *, const char **errstr);
int next_row(struct sqlclntstate *, sqlite3_stmt *);
int sqlite_stmt_error(sqlite3_stmt *stmt, const char **errstr);
int sqlite3_is_success(int);
int sqlite3_is_prepare_only(struct sqlclntstate *);
int sqlite3_maybe_step(struct sqlclntstate *, sqlite3_stmt *);
int sqlite3_can_get_column_type_and_data(struct sqlclntstate *, sqlite3_stmt *);

#define SQLITE_PROTO_API(ret, type)                                            \
    ret column_##type(struct sqlclntstate *, sqlite3_stmt *, int)

SQLITE_PROTO_API(int, type);
SQLITE_PROTO_API(sqlite_int64, int64);
SQLITE_PROTO_API(double, double);
SQLITE_PROTO_API(const unsigned char *, text);
SQLITE_PROTO_API(int, bytes);
SQLITE_PROTO_API(const void *, blob);
SQLITE_PROTO_API(const dttz_t *, datetime);
const intv_t *column_interval(struct sqlclntstate *, sqlite3_stmt *, int, int);

struct query_stats {
    int64_t nfstrap;
    int64_t nsql;
    int64_t nsteps;
    int64_t ncommits;
    int64_t nretries;
    int64_t ndeadlocks;
    int64_t nlockwaits;
    int64_t nbpoolhits;
    int64_t nbpoolmisses;
    int64_t npreads;
    int64_t npwrites;
};
int get_query_stats(struct query_stats *stats);

void save_thd_cost_and_reset(struct sqlthdstate *thd, Vdbe *pVdbe);
void restore_thd_cost_and_reset(struct sqlthdstate *thd, Vdbe *pVdbe);
void clnt_query_cost(struct sqlthdstate *thd, double *pCost, int64_t *pPrepMs);

void calc_fingerprint(const char *zNormSql, size_t *pnNormSql,
                      unsigned char fingerprint[FINGERPRINTSZ]);
void add_fingerprint(const char *, const char *, int64_t, int64_t, int64_t,
                     int64_t, struct reqlogger *, unsigned char *fingerprint_out);

long long run_sql_return_ll(const char *query, struct errstat *err);
long long run_sql_thd_return_ll(const char *query, struct sql_thread *thd,
                                struct errstat *err);

/* Connection tracking */
int gather_connection_info(struct connection_info **info, int *num_connections);
void clnt_change_state(struct sqlclntstate *clnt, enum connection_state state);
void clnt_register(struct sqlclntstate *clnt);
void clnt_unregister(struct sqlclntstate *clnt);

/* Returns the current user for the session */
char *get_current_user(struct sqlclntstate *clnt);

struct client_sql_systable_data {
    char *host;
    char *task;
    char *fingerprint;
    int64_t count;
    int64_t timems;
    int64_t cost;
    int64_t rows;

    char fp[FINGERPRINTSZ*2+1];
};

struct query_count {
    char fingerprint[FINGERPRINTSZ];

    // TODO: counter_t that we automatically reset when needed
    int64_t count;
    int64_t last_count;

    int64_t cost;
    int64_t last_cost;

    int64_t rows;
    int64_t last_rows;

    int64_t timems;
    int64_t last_timems;
};

void add_fingerprint_to_rawstats(struct rawnodestats *stats, unsigned char *fingerprint, int cost, int rows, int timems);

#endif
