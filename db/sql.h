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

#include "comdb2.h"

#include <bdb_api.h>
#include <bdb_cursor.h>

#include "tag.h"
#include "osql_srs.h"
#include "osqlsqlthr.h"
#include "osqlcheckboard.h"
#include "osqlshadtbl.h"

#define TYPEDEF(x) typedef struct x x;
TYPEDEF(BtCursor)
TYPEDEF(Btree)
TYPEDEF(Mem)
TYPEDEF(Schema)
TYPEDEF(Table)
TYPEDEF(UnpackedRecord)
TYPEDEF(Vdbe)

#include "fdb_fend.h"
#include <sp.h>

/* Modern transaction modes, more or less */
enum transaction_level {
    TRANLEVEL_INVALID = -1,

    /* TRANLEVEL_OSQL = 7, */

    /* block sql over socket */
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

typedef struct stmt_hash_entry {
    char sql[MAX_HASH_SQL_LENGTH];
    sqlite3_stmt *stmt;
    char *query;
    struct schema *params_to_bind;
    struct stmt_hash_entry *prev;
    struct stmt_hash_entry *next;
} stmt_hash_entry_type;

/* Thread specific sql state */
struct sqlthdstate {
    struct reqlogger *logger;
    struct sql_thread *sqlthd;

    /* Scratch buffer. */
    char *buf;
    int maxbuflen;
    int buflen;

    struct column_info *cinfo;
    struct sqlfield *offsets;

    struct thr_handle *thr_self;
    sqlite3 *sqldb;

    hash_t *stmt_table;

    stmt_hash_entry_type *param_stmt_head;
    stmt_hash_entry_type *param_stmt_tail;

    stmt_hash_entry_type *noparam_stmt_head;
    stmt_hash_entry_type *noparam_stmt_tail;

    int param_cache_entries;
    int noparam_cache_entries;

    int dbopen_gen;
    int analyze_gen;
    int views_gen;
    int ncols;
    int started_backend;
};

int find_stmt_table(hash_t *stmt_table, const char *sql,
                    stmt_hash_entry_type **entry);
void touch_stmt_entry(struct sqlthdstate *thd, stmt_hash_entry_type *entry);
int add_stmt_table(struct sqlthdstate *, const char *sql, char *actual_sql,
                   sqlite3_stmt *, struct schema *params_to_bind);

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
    int long_request;
    int dirty; /* optimization to nop selectv only transactions */
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
    struct temp_table *sampled_table;
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
    errstat_t err;        /* remote execution specific error */
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
} sqlclntstate_fdb_t;

CurRange *currange_new();
#define CURRANGEARR_INIT_CAP 2
void currangearr_init(CurRangeArr *arr);
void currangearr_append(CurRangeArr *arr, CurRange *r);
CurRange *currangearr_get(CurRangeArr *arr, int n);
void currangearr_double_if_full(CurRangeArr *arr);
int currange_cmp(const void *p, const void *q);
void currangearr_sort(CurRangeArr *arr);
void currangearr_merge_neighbor(CurRangeArr *arr);
void currangearr_coalesce(CurRangeArr *arr);
void currangearr_build_hash(CurRangeArr *arr);
void currangearr_free(CurRangeArr *arr);
void currangearr_print(CurRangeArr *arr);
void currange_free(CurRange *cr);

struct stored_proc;
struct lua_State;

/* Client specific sql state */
struct sqlclntstate {

    dbtran_type dbtran;
    pthread_mutex_t dtran_mtx; /* protect dbtran.dtran, if any,
                                  for races betweem sql thread created and
                                  other readers, like appsock */
    SBUF2 *sb;
    int must_close_sb;

    /* These are only valid while a query is in progress and will point into
     * the i/o thread's buf */
    char *sql;
    int sqllen;
    int *type_overrides;
    int recno;
    struct fsqlreq req;
    int client_understands_query_stats;
    char tzname[CDB2_MAX_TZNAME];
    int dtprec;
    struct conninfo conninfo;

    /* For SQL engine dispatch. */
    int inited_mutex;
    pthread_mutex_t wait_mutex;
    pthread_mutex_t write_lock;
    pthread_cond_t wait_cond;
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
    int sp_cdata_sent;
    int want_stored_procedure_trace;
    int want_stored_procedure_debug;
    char spname[MAX_SPNAME + 1];
    struct spversion_t spversion;
    int n_lua_stmt;
    int max_lua_stmt;

    char *tag;
    void *tagbuf; /* note: this is a pointer into the client appsock thread
                     buffer, don't free */
    int tagbufsz;
    void *nullbits;
    int numnullbits;

    int numblobs;
    void **blobs;
    int *bloblens;
    void *inline_blobs[MAXBLOBS];
    int inline_bloblens[MAXBLOBS];
    void **alloc_blobs;
    int *alloc_bloblens;
    int numallocblobs;


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

    int have_user;
    char user[17];

    int have_password;
    char password[19];

    int have_endian;
    int endian;

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

    int iswrite;    /* track each query if it is a read or a write */
    int isselect;   /* track if the query is a select query.*/
    int isUnlocked;
    int writeTransaction; /* different from iswrite above */
    int want_query_effects;
    int send_one_row;
    int verify_retries; /* how many verify retries we've borne */
    int verifyretry_off;
    int pageordertablescan;
    int snapshot; /* snapshot epoch placeholder */
    int snapshot_file;
    int snapshot_offset;
    int is_hasql_retry;
    int is_readonly;
    int is_newsql;
    CDB2SQLQUERY *sql_query; /* Needed to fetch the bind variables. */
    CDB2QUERY *query;
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
    int8_t skip_feature;
    int8_t high_availability;
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

    char fingerprint[16];
    int ncontext;
    char **context;

    hash_t *ddl_tables;
    hash_t *dml_tables;
};

/* Query stats. */
struct query_path_component {
    union {
        struct dbtable *db;           /* local db, or tmp if NULL */
        struct fdb_tbl_ent *fdb; /* remote db */
    } u;
    int ix;
    int nfind;
    int nnext;
    int nwrite;
    int nblobs;
    int remote; /* mark this as remote, see *u */
    LINKC_T(struct query_path_component) lnk;
};

struct Btree {
    /* for debugging */
    int btreeid;
    struct reqlogger *reqlogger;

    bdb_temp_hash *genid_hash; /* rrn hash for non dtastripe support */

    LISTC_T(BtCursor) cursors;

    /* temp table stuff */
    int is_temporary;
    /* number and array of temp tables under this btree (generally 1) */
    int num_temp_tables;
    struct temptable *temp_tables;
    int tempid;

    int is_hashtable;

    int is_remote;

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

    /* sampled (previously misnamed compressed) idx temptable */
    struct temptable *sampled_idx;

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
};

struct sql_hist {
    LINKC_T(struct sql_hist) lnk;
    char *sql;
    double cost;
    int time;
    int when;
    int64_t txnid;
    struct conninfo conn;
};

struct sql_thread {
    LINKC_T(struct sql_thread) lnk;
    pthread_mutex_t lk;
    struct Btree *bt, *bttmp;
    int startms;
    int stime;
    int nmove;
    int nfind;
    int nwrite;
    int ntmpwrite;
    int ntmpread;
    int nblobs;
    int bufsz;
    int id;
    char *buf;
    LISTC_T(struct query_path_component) query_stats;
    hash_t *query_hash;
    double cost;
    struct sqlclntstate *sqlclntstate; /* pointer to originating sqlclnt */
    /* custom error message to send to client */
    char *error;
    struct rootpage *rootpages;
    int rootpage_nentries;
    unsigned char had_temptables;
    unsigned char had_tablescans;
};

/* makes master swing verbose */
extern int gbl_master_swing_osql_verbose;

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

unsigned long long osql_log_time(void);
void osql_log_time_done(struct sqlclntstate *clnt);

int dispatch_sql_query(struct sqlclntstate *clnt);

int handle_sql_begin(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                     int sendresponse);
int handle_sql_commitrollback(struct sqlthdstate *thd,
                              struct sqlclntstate *clnt, int sendresponse);

void sql_get_query_id(struct sql_thread *thd);

void sql_dlmalloc_init(void);
int sql_mem_init(void *dummy);
void sql_mem_shutdown(void *dummy);

int sqlite3_open_serial(const char *filename, sqlite3 **, struct sqlthdstate *);

void reset_clnt(struct sqlclntstate *, SBUF2 *, int initial);
void reset_query_effects(struct sqlclntstate *);

int sqlite_to_ondisk(struct schema *s, const void *inp, int len, void *outp,
                     const char *tzname, blob_buffer_t *outblob, int maxblobs,
                     struct convert_failure *fail_reason, BtCursor *pCur);

int emit_sql_row(struct sqlthdstate *thd, struct column_info *cols,
                 struct sqlfield *offsets, struct sqlclntstate *clnt,
                 sqlite3_stmt *stmt, int *irc, char *errstr, int maxerrstr);

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
void clone_temp_table(sqlite3 *dest, const sqlite3 *src, const char *sql,
                      int rootpg); //, pthread_mutex_t *lk);
void sqlengine_prepare_engine(struct sqlthdstate *, struct sqlclntstate *);
int check_thd_gen(struct sqlthdstate *, struct sqlclntstate *);
int sqlserver2sqlclient_error(int rc);
uint16_t stmt_num_tbls(sqlite3_stmt *);
int newsql_dump_query_plan(struct sqlclntstate *clnt, sqlite3 *hndl);
void init_cursor(BtCursor *, Vdbe *, Btree *);
void run_stmt_setup(struct sqlclntstate *, sqlite3_stmt *);

#endif
