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

/* code needed to support various comdb2 interfaces to the sql engine */

#include <poll.h>
#include <stdlib.h>
#include <stdio.h>
#include <strings.h>

#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <pthread.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <inttypes.h>
#include <fcntl.h>
#include <limits.h>
#include <time.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

#include <epochlib.h>

#include <plhash.h>
#include <segstr.h>

#include <list.h>

#include <sbuf2.h>
#include <bdb_api.h>

#include "comdb2.h"
#include "types.h"
#include "tag.h"
#include "thdpool.h"
#include "ssl_bend.h"

#include <cdb2api.h>

#include <sys/time.h>
#include <strbuf.h>
#include <math.h>

#include <sqlite3.h>
#include <sqliteInt.h>
#include <vdbeInt.h>

#include "sql.h"
#include "sqlinterfaces.h"

#include "locks.h"
#include "sqloffload.h"
#include "osqlcomm.h"
#include "osqlcheckboard.h"
#include "osqlsqlthr.h"
#include "osqlshadtbl.h"

#include <sqlresponse.pb-c.h>
#include <sqlite3expert.h>
#include <carray.h>

#include <alloca.h>
#include <fsnapf.h>

#include "flibc.h"

#include "sp.h"

#include <ctrace.h>
#include <bb_oscompat.h>
#include <netdb.h>

#include "fdb_bend_sql.h"
#include "fdb_access.h"
#include "sqllog.h"
#include <quantize.h>
#include <str0.h>

#include "debug_switches.h"
#include "intern_strings.h"
#include "views.h"
#include "mem.h"
#include "comdb2_atomic.h"
#include "logmsg.h"
#include "reqlog.h"
#include "eventlog.h"
#include "perf.h"
#include "tohex.h"

#include "dohsql.h"
#include "comdb2_query_preparer.h"
#include "string_ref.h"

#include "osqlsqlsocket.h"
#include <net_appsock.h>
#include <typessql.h>

/*
** WARNING: These enumeration values are not arbitrary.  They represent
**          indexes into the array of meta-command names contained in
**          the is_transaction_meta_sql() function.  New values should
**          generally be added at the end and all of these values must
**          be kept in sync with the azMeta string array contained in
**          the is_transaction_meta_sql() function.
*/
enum tsql_meta_command {
  TSMC_NONE = 0,
  TSMC_BEGIN = 1,
  TSMC_COMMIT = 2,
  TSMC_ROLLBACK = 3
};

typedef enum tsql_meta_command tsql_meta_command_t;

/* delete this after comdb2_api.h changes makes it through */
#define SQLHERR_MASTER_QUEUE_FULL -108
#define SQLHERR_MASTER_TIMEOUT -109

extern unsigned long long gbl_sql_deadlock_failures;
extern int gbl_allow_pragma;
extern int g_osql_max_trans;
extern int gbl_fdb_track;
extern int gbl_stable_rootpages_test;
extern int gbl_verbose_normalized_queries;
extern int gbl_group_concat_mem_limit;
extern int gbl_expressions_indexes;
extern int gbl_old_column_names;
extern hash_t *gbl_fingerprint_hash;
extern pthread_mutex_t gbl_fingerprint_hash_mu;
extern int gbl_alternate_normalize;
extern int gbl_typessql;
extern int gbl_modsnap_asof;
extern int gbl_use_modsnap_for_snapshot;

/* Once and for all:

   struct sqlthdstate:
      This is created per thread executing SQL.  Has per-thread resources
      like an SQLite handle, logger, etc.

   struct sqlclntstate:
      Per connection.  If a connection is handed off to another handle on the
      client side (via sockpool), client request a reset of this structure.

   struct sql_thread:
      Linked from sqlthdstate.  Has per query stats like accumulated cost, etc
      as well as the connection lock (which is really a per-session resource
      that should be in sqlclntstate).  Also has to Btree* which probably
      also belongs in sqlthdstate, or sqlclntstate, or lord only knows where
   else.

   struct Btree:
      This is per instance of sqlite, which may be shared when idle among
   multiple
      connections.
*/

/* An alternate interface. */
extern int gbl_dump_sql_dispatched; /* dump all sql strings dispatched */
extern int gbl_time_osql; /* dump timestamps for osql steps */
extern int gbl_time_fdb;  /* dump timestamps for remote sql */
extern int gbl_print_syntax_err;
extern int gbl_max_sqlcache;
extern int gbl_track_sqlengine_states;
extern int gbl_disable_sql_dlmalloc;
extern struct ruleset *gbl_ruleset;
extern int gbl_sql_release_locks_on_slow_reader;
extern int gbl_sql_no_timeouts_on_release_locks;
extern int get_snapshot(struct sqlclntstate *clnt, int *f, int *o);

extern void clnt_try_enable_logdel(struct sqlclntstate *clnt);

/* gets incremented each time a user's password is changed. */
int gbl_bpfunc_auth_gen = 1;

uint64_t gbl_clnt_seq_no = 0;

int gbl_dump_history_on_too_many_verify_errors = 0;
int gbl_thdpool_queue_only = 0;
int gbl_random_sql_work_delayed = 0;
int gbl_random_sql_work_rejected = 0;

comdb2_query_preparer_t *query_preparer_plugin;

void rcache_init(size_t, size_t);
void rcache_destroy(void);
void sql_reset_sqlthread(struct sql_thread *thd);
int blockproc2sql_error(int rc, const char *func, int line);
static int test_no_btcursors(struct sqlthdstate *thd);
static void sql_thread_describe(void *obj, FILE *out);
static char *get_query_cost_as_string(struct sql_thread *thd,
                                      struct sqlclntstate *clnt);

void handle_sql_intrans_unrecoverable_error(struct sqlclntstate *clnt);

void comdb2_set_sqlite_vdbe_dtprec(Vdbe *p);
static int execute_sql_query_offload(struct sqlthdstate *,
                                     struct sqlclntstate *);
static int record_query_cost(struct sql_thread *, struct sqlclntstate *);

static int sql_debug_logf_int(struct sqlclntstate *clnt, const char *func,
                              int line, const char *fmt, va_list args)
{
    char *s;
    int cn_len;
    snap_uid_t snap = {{0}};
    char *cnonce;
    int len;
    int nchars;
    va_list args_c;

    if (clnt && get_cnonce(clnt, &snap) == 0) {
        cn_len = snap.keylen;
        cnonce = alloca(cn_len + 1);
        memcpy(cnonce, snap.key, cn_len);
        cnonce[cn_len] = '\0';
    } else {
        cnonce = "(no-cnonce)";
    }

    len = 256;
    s = malloc(len);
    if (!s) {
        logmsg(LOGMSG_ERROR, "%s:malloc(%d) failed\n", __func__, len);
        return -1;
    }

    va_copy(args_c, args);
    nchars = vsnprintf(s, len, fmt, args);

    if (nchars >= len) {
        len = nchars + 1;
        char *news = realloc(s, len);
        if (!news) {
            logmsg(LOGMSG_ERROR, "%s:realloc(%d) failed\n", __func__, len);
            va_end(args_c);
            free(s);
            return -1;
        }
        s = news;
        len = vsnprintf(s, len, fmt, args_c);
    } else {
        len = strlen(s);
    }
    va_end(args_c);

    logmsg(LOGMSG_USER, "cnonce=%s %s line %d: %s", cnonce, func, line, s);

    free(s);
    return 0;
}

int sql_debug_logf(struct sqlclntstate *clnt, const char *func, int line,
                   const char *fmt, ...)
{
    if (!gbl_extended_sql_debug_trace)
        return 0;
    else {
        va_list args;
        int rc;
        va_start(args, fmt);
        rc = sql_debug_logf_int(clnt, func, line, fmt, args);
        va_end(args);
        return rc;
    }
}

static inline void comdb2_set_sqlite_vdbe_tzname_int(Vdbe *p,
                                                     struct sqlclntstate *clnt)
{
    memcpy(p->tzname, clnt->tzname, TZNAME_MAX);
}

static inline void comdb2_set_sqlite_vdbe_dtprec_int(Vdbe *p,
                                                     struct sqlclntstate *clnt)
{
    p->dtprec = clnt->dtprec;
}

int disable_server_sql_timeouts(void)
{
    return gbl_sql_release_locks_on_slow_reader && gbl_sql_no_timeouts_on_release_locks;
}

#define XRESPONSE(x) #x,
const char *WriteRespString[] = { RESPONSE_TYPES };
#undef XRESPONSE

int gbl_client_heartbeat_ms = 100;
int gbl_fail_client_write_lock = 0;

struct sqlclntstate *get_sql_clnt(void){
  struct sql_thread *thd = pthread_getspecific(query_info_key);
  if (thd == NULL) return NULL;
  return thd->clnt;
}

static inline int lock_client_write_lock_int(struct sqlclntstate *clnt, int try)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    int rc = 0;

    if (thd && clnt)
        clnt->emitting_flag = 1;
again:
    if (try) {
        if ((rc = pthread_mutex_trylock(&clnt->write_lock))) {
            if (thd && clnt)
                clnt->emitting_flag = 0;
            return rc;
        }
    } else {
        Pthread_mutex_lock(&clnt->write_lock);
    }
    if (clnt->heartbeat_lock && thd) {
        if (clnt->need_recover_deadlock) {
            /* Call only if there isn't a previous failure */
            if (!clnt->recover_deadlock_rcode) {
                uint32_t flags = 0;
                if (gbl_fail_client_write_lock && !(rand() % gbl_fail_client_write_lock)) {
                    flags = RECOVER_DEADLOCK_FORCE_FAIL;
                }
                recover_deadlock_flags(thedb->bdb_env, clnt, NULL, 0, __func__, __LINE__, flags);
            }
            clnt->need_recover_deadlock = 0;
            if (clnt->recover_deadlock_rcode) {
                assert(bdb_lockref() == 0);
                logmsg(LOGMSG_WARN, "%s recover_deadlock returned %d\n",
                       __func__, clnt->recover_deadlock_rcode);
            }
        }
        Pthread_cond_signal(&clnt->write_cond);
        Pthread_mutex_unlock(&clnt->write_lock);
        goto again;
    }
    return 0;
}

int lock_client_write_trylock(struct sqlclntstate *clnt)
{
    return lock_client_write_lock_int(clnt, 1);
}

int lock_client_write_lock(struct sqlclntstate *clnt)
{
    return lock_client_write_lock_int(clnt, 0);
}

void unlock_client_write_lock(struct sqlclntstate *clnt)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (thd && clnt)
        clnt->emitting_flag = 0;
    Pthread_mutex_unlock(&clnt->write_lock);
}

int write_response(struct sqlclntstate *clnt, int R, void *D, int I)
{
#ifdef DEBUG
    logmsg(LOGMSG_DEBUG, "write_response(%s,%p,%d)\n", WriteRespString[R], D, I);
#endif
    return clnt->plugin.write_response(clnt, R, D, I); /* newsql_write_response */
}

int read_response(struct sqlclntstate *clnt, int R, void *D, int I)
{
    return clnt->plugin.read_response(clnt, R, D, I);
}

int get_fileno(struct sqlclntstate *clnt)
{
    return clnt->plugin.get_fileno(clnt);
}

int column_count(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    struct sql_thread *thd = NULL;

    if (!clnt) {
        thd = pthread_getspecific(query_info_key);
        if (thd)
            clnt = thd->clnt;
    }

    if (clnt && clnt->plugin.column_count)
        return clnt->plugin.column_count(clnt, stmt);

    return sqlite3_column_count(stmt);
}

int column_type(struct sqlclntstate *clnt, sqlite3_stmt *stmt, int iCol)
{
    if (clnt && clnt->plugin.column_type) return clnt->plugin.column_type(clnt, stmt, iCol);
    return sqlite3_column_type(stmt, iCol);
}

sqlite_int64 column_int64(struct sqlclntstate *clnt, sqlite3_stmt *stmt, int iCol)
{
    if (clnt && clnt->plugin.column_int64) return clnt->plugin.column_int64(clnt, stmt, iCol);
    return sqlite3_column_int64(stmt, iCol);
}

double column_double(struct sqlclntstate *clnt, sqlite3_stmt *stmt, int iCol)
{
    if (clnt && clnt->plugin.column_double) return clnt->plugin.column_double(clnt, stmt, iCol);
    return sqlite3_column_double(stmt, iCol);
}

const unsigned char *column_text(struct sqlclntstate *clnt, sqlite3_stmt *stmt, int iCol)
{
    if (clnt && clnt->plugin.column_text) return clnt->plugin.column_text(clnt, stmt, iCol);
    return sqlite3_column_text(stmt, iCol);
}

int column_bytes(struct sqlclntstate *clnt, sqlite3_stmt *stmt, int iCol)
{
    if (clnt && clnt->plugin.column_bytes) return clnt->plugin.column_bytes(clnt, stmt, iCol);
    return sqlite3_column_bytes(stmt, iCol);
}

const void *column_blob(struct sqlclntstate *clnt, sqlite3_stmt *stmt, int iCol)
{
    if (clnt && clnt->plugin.column_blob) return clnt->plugin.column_blob(clnt, stmt, iCol);
    return sqlite3_column_blob(stmt, iCol);
}

const dttz_t *column_datetime(struct sqlclntstate *clnt, sqlite3_stmt *stmt, int iCol)
{
    if (clnt && clnt->plugin.column_datetime) return clnt->plugin.column_datetime(clnt, stmt, iCol);
    return sqlite3_column_datetime(stmt, iCol);
}

const intv_t *column_interval(struct sqlclntstate *clnt, sqlite3_stmt *stmt, int iCol, int type)
{
    if (clnt && clnt->plugin.column_interval) return clnt->plugin.column_interval(clnt, stmt, iCol, type);
    return sqlite3_column_interval(stmt, iCol, type);
}

sqlite3_value *column_value(struct sqlclntstate *clnt, sqlite3_stmt *stmt, int iCol)
{
    if (clnt && clnt->plugin.column_value) return clnt->plugin.column_value(clnt, stmt, iCol);
    return sqlite3_column_value(stmt, iCol);
}

int sqlite_stmt_error(sqlite3_stmt *stmt, const char **errstr)
{
    sqlite3 *db = sqlite3_db_handle(stmt);
    int errcode;

    *errstr = NULL;

    errcode = sqlite3_errcode(db);
    if (errcode && errcode != SQLITE_ROW) {
        *errstr = sqlite3_errmsg(db);
    }
    return errcode;
}

int sqlite_error(struct sqlclntstate *clnt, sqlite3_stmt *stmt,
                 const char **errstr)
{
    if (clnt && clnt->plugin.sqlite_error)
        return clnt->plugin.sqlite_error(clnt, stmt, errstr);

    return sqlite_stmt_error(stmt, errstr);
}

int next_row(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    if (clnt && clnt->plugin.next_row)
        return clnt->plugin.next_row(clnt, stmt);
    return sqlite3_maybe_step(clnt, stmt);
}

int has_cnonce(struct sqlclntstate *clnt)
{
    return clnt->plugin.has_cnonce(clnt);
}

int set_cnonce(struct sqlclntstate *clnt)
{
    return clnt->plugin.set_cnonce(clnt);
}

int clr_cnonce(struct sqlclntstate *clnt)
{
    return clnt->plugin.clr_cnonce(clnt);
}

int get_cnonce(struct sqlclntstate *clnt, snap_uid_t *snap)
{
    return clnt->plugin.get_cnonce(clnt, snap);
}

static int clr_snapshot(struct sqlclntstate *clnt)
{
    return clnt->plugin.clr_snapshot(clnt);
}

static int upd_snapshot(struct sqlclntstate *clnt)
{
    return clnt->plugin.upd_snapshot(clnt);
}

int has_high_availability(struct sqlclntstate *clnt)
{
    return clnt->plugin.has_high_availability(clnt);
}

int set_high_availability(struct sqlclntstate *clnt)
{
    return clnt->plugin.set_high_availability(clnt);
}

int clr_high_availability(struct sqlclntstate *clnt)
{
    return clnt->plugin.clr_high_availability(clnt);
}

static int get_high_availability(struct sqlclntstate *clnt)
{
    return clnt->plugin.get_high_availability(clnt);
}

int has_parallel_sql(struct sqlclntstate *clnt)
{

    if (!clnt) {
        struct sql_thread *thd = pthread_getspecific(query_info_key);
        if (thd) clnt = thd->clnt;
    }
    /* disable anything involving shared shadows;
       recom requires a read-only share;
       snapisol and serializable requires a read-write share
    */
    if (!clnt || clnt->dbtran.mode != TRANLEVEL_SOSQL || clnt->dohsql_disable)
        return 0;

    return clnt && clnt->plugin.has_parallel_sql && clnt->plugin.has_parallel_sql(clnt);
}

static void setup_client_info(struct sqlclntstate *clnt, struct sqlthdstate *thd, char *replay)
{
    clnt->plugin.setup_client_info(clnt, thd, replay);
}

uint64_t get_client_starttime(struct sqlclntstate *clnt)
{
    return clnt->plugin.get_client_starttime(clnt);
}

int get_client_retries(struct sqlclntstate *clnt)
{
    return clnt->plugin.get_client_retries(clnt);
}

void *get_authdata(struct sqlclntstate *clnt)
{
    return clnt->plugin.get_authdata(clnt);
}

static int skip_row(struct sqlclntstate *clnt, uint64_t rowid)
{
    return clnt->plugin.skip_row(clnt, rowid);
}

static int log_context(struct sqlclntstate *clnt, struct reqlogger *logger)
{
    return clnt->plugin.log_context(clnt, logger);
}

static int send_intrans_response(struct sqlclntstate *clnt)
{
    return clnt->plugin.send_intrans_response(clnt);
}

void handle_failed_dispatch(struct sqlclntstate *clnt, char *errstr)
{
    Pthread_mutex_lock(&clnt->wait_mutex);
    write_response(clnt, RESPONSE_ERROR_REJECT, errstr, 0);
    Pthread_mutex_unlock(&clnt->wait_mutex);
}

char *tranlevel_tostr(int lvl)
{
    switch (lvl) {
    case TRANLEVEL_SOSQL:
        return "TRANLEVEL_SOSQL";
    case TRANLEVEL_RECOM:
        return "TRANLEVEL_RECOM";
    case TRANLEVEL_MODSNAP:
        return "TRANLEVEL_MODSNAP";
    case TRANLEVEL_SERIAL:
        return "TRANLEVEL_SERIAL";
    default:
        return "???";
    };
}

int toggle_case_sensitive_like(sqlite3 *db, int enable)
{
    char sql[80];
    int rc;
    char *err;

    snprintf(sql, sizeof(sql), "PRAGMA case_sensitive_like = %d;",
             enable ? 0 : 1);
    rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    if (rc)
        logmsg(LOGMSG_ERROR, "Failed to set case_insensitive_like rc %d err \"%s\"\n", rc,
                err ? err : "");
    if (err)
        sqlite3_free(err);
    return rc;
}

static int64_t connid = 0;

/* lru_evbuffers may be accessed by multiple sql threads, hence we need to protect it with a mutex. */
static pthread_mutex_t lru_evbuffers_mtx = PTHREAD_MUTEX_INITIALIZER;
static TAILQ_HEAD(lru_evbuffers, sqlclntstate) lru_evbuffers = TAILQ_HEAD_INITIALIZER(lru_evbuffers);
static TAILQ_HEAD(sql_evbuffers, sqlclntstate) sql_evbuffers = TAILQ_HEAD_INITIALIZER(sql_evbuffers);

static __thread comdb2ma sql_mspace = NULL;
static void sql_mem_create()
{
    /* We used to start with 1MB - this isn't quite necessary
       as comdb2_malloc pre-allocation is much smarter now.
       We also name it "SQLITE" (uppercase) to differentiate it
       from those implicitly created per-thread allocators
       whose names are "sqlite" (lowercase). Those allocators
       are used by other types of threads, e.g., appsock threads. */
    sql_mspace = comdb2ma_create(0, 0, "SQLITE", COMDB2MA_MT_UNSAFE);
    if (sql_mspace == NULL) {
        logmsg(LOGMSG_FATAL, "%s: comdb2a_create failed\n", __func__);
        exit(1);
    }
}

int sql_mem_init(void *arg)
{
    if (unlikely(sql_mspace)) {
        return 0;
    }
    sql_mem_create();
    return 0;
}

int sql_mem_init_with_save(void *arg, void **poldm)
{
    *poldm = sql_mspace;
    sql_mem_create();
    return 0;
}

void sql_mem_shutdown(void *arg)
{
    if (sql_mspace) {
        comdb2ma_destroy(sql_mspace);
        sql_mspace = NULL;
    }
}

void sql_mem_shutdown_and_restore(void *arg, void **poldm)
{
    sql_mem_shutdown(arg);
    sql_mspace = *poldm; *poldm = NULL;
}

static void *sql_mem_malloc(int size)
{
    if (unlikely(sql_mspace == NULL))
        sql_mem_init(NULL);

    return comdb2_malloc(sql_mspace, size);
}

static void sql_mem_free(void *mem)
{
    comdb2_free(mem);
}

static void *sql_mem_realloc(void *mem, int size)
{
    if (unlikely(sql_mspace == NULL))
        sql_mem_init(NULL);

    return comdb2_realloc(sql_mspace, mem, size);
}

static int sql_mem_size(void *mem) { return comdb2_malloc_usable_size(mem); }

static int sql_mem_roundup(int i) { return i; }

void sql_dlmalloc_init(void)
{
    sqlite3_mem_methods m;
    if (gbl_disable_sql_dlmalloc) {
        return;
    }
    m.xMalloc = sql_mem_malloc;
    m.xFree = sql_mem_free;
    m.xRealloc = sql_mem_realloc;
    m.xSize = sql_mem_size;
    m.xRoundup = sql_mem_roundup;
    m.xInit = sql_mem_init;
    m.xShutdown = sql_mem_shutdown;
    m.pAppData = NULL;
    sqlite3_config(SQLITE_CONFIG_MALLOC, &m);
}

/* Called once from comdb2. Do all static intialization here */
void sqlinit(void)
{
    Pthread_mutex_init(&gbl_sql_lock, NULL);
    sql_dlmalloc_init();
    /* initialize global structures in sqlite */
    if (sqlite3_initialize())
        abort();
}

static char *vtable_lockname(sqlite3 *db, const char *vtable, int *is_system_table)
{
    *is_system_table = 0;
    if (vtable == NULL || db == NULL || db->aModule.count == 0)
        return NULL;
    struct Module *module;
    char *lockname = NULL;
    sqlite3_mutex_enter(db->mutex);
    if ((module = sqlite3HashFind(&db->aModule, vtable)) != NULL) {
        *is_system_table = 1;
        lockname = module->pModule->systable_lock;
    }
    sqlite3_mutex_leave(db->mutex);
    return lockname;
}

static int vtable_search(char **vtables, int ntables, const char *table)
{
    for (int i = 0; i < ntables; i++) {
        if (strcmp(vtables[i], table) == 0) {
            return 1;
        }
    }
    return 0;
}

static void record_locked_vtable(struct sql_authorizer_state *pAuthState, const char *table)
{
    int is_system_table;
    const char *vtable_lock = vtable_lockname(pAuthState->db, table, &is_system_table);
    if (vtable_lock && !vtable_search(pAuthState->vTableLocks, pAuthState->numVTableLocks, vtable_lock)) {
        pAuthState->vTableLocks =
            (char **)realloc(pAuthState->vTableLocks, sizeof(char *) * (pAuthState->numVTableLocks + 1));
        pAuthState->vTableLocks[pAuthState->numVTableLocks++] = strdup(vtable_lock);
    }
    if (is_system_table && !pAuthState->hasVTables)
        pAuthState->hasVTables = 1;
}

static int comdb2_authorizer_for_sqlite(
  void *pArg,        /* IN: NOT USED */
  int code,          /* IN: NOT USED */
  const char *zArg1, /* IN: NOT USED */
  const char *zArg2, /* IN: NOT USED */
  const char *zArg3, /* IN: NOT USED */
  const char *zArg4  /* IN: NOT USED */
#ifdef SQLITE_USER_AUTHENTICATION
  ,const char *zArg5 /* IN: NOT USED */
#endif
){
  struct sql_authorizer_state *pAuthState = pArg;
  if (pAuthState == NULL) {
    return SQLITE_DENY;
  }
  int denyCreateTrigger = (pAuthState->flags & PREPARE_DENY_CREATE_TRIGGER);
  int denyPragma = (pAuthState->flags & PREPARE_DENY_PRAGMA);
  int denyDdl = (pAuthState->flags & PREPARE_DENY_DDL);
  int allowTempDDL = (pAuthState->flags & PREPARE_ALLOW_TEMP_DDL);
  switch (code) {
    case SQLITE_CREATE_INDEX:
    case SQLITE_CREATE_TABLE:
    case SQLITE_CREATE_VIEW:
    case SQLITE_DROP_INDEX:
    case SQLITE_DROP_TABLE:
    case SQLITE_DROP_TRIGGER:
    case SQLITE_DROP_VIEW:
    case SQLITE_ALTER_TABLE:
    case SQLITE_REINDEX:
    case SQLITE_ANALYZE:
    case SQLITE_CREATE_VTABLE:
    case SQLITE_DROP_VTABLE:
    case SQLITE_REBUILD_TABLE:       /* COMDB2 ONLY */
    case SQLITE_REBUILD_INDEX:       /* COMDB2 ONLY */
    case SQLITE_REBUILD_DATA:        /* COMDB2 ONLY */
    case SQLITE_REBUILD_DATABLOB:    /* COMDB2 ONLY */
    case SQLITE_TRUNCATE_TABLE:      /* COMDB2 ONLY */
    case SQLITE_CREATE_PROC:         /* COMDB2 ONLY */
    case SQLITE_DROP_PROC:           /* COMDB2 ONLY */
    case SQLITE_CREATE_PART:         /* COMDB2 ONLY */
    case SQLITE_DROP_PART:           /* COMDB2 ONLY */
    case SQLITE_GET_TUNABLE:         /* COMDB2 ONLY */
    case SQLITE_PUT_TUNABLE:         /* COMDB2 ONLY */
    case SQLITE_GRANT:               /* COMDB2 ONLY */
    case SQLITE_REVOKE:              /* COMDB2 ONLY */
    case SQLITE_CREATE_LUA_FUNCTION: /* COMDB2 ONLY */
    case SQLITE_DROP_LUA_FUNCTION:   /* COMDB2 ONLY */
    case SQLITE_CREATE_LUA_TRIGGER:  /* COMDB2 ONLY */
    case SQLITE_DROP_LUA_TRIGGER:    /* COMDB2 ONLY */
    case SQLITE_CREATE_LUA_CONSUMER: /* COMDB2 ONLY */
    case SQLITE_DROP_LUA_CONSUMER:   /* COMDB2 ONLY */
      pAuthState->numDdls++;
      return denyDdl ? SQLITE_DENY : SQLITE_OK;
    case SQLITE_PRAGMA:
      pAuthState->numDdls++;
      if (denyDdl || denyPragma) {
        return SQLITE_DENY;
      } else if (pAuthState->clnt != NULL) {
        logmsg(LOGMSG_DEBUG, "%s:%d %s ALLOWING PRAGMA [%s]\n", __FILE__,
               __LINE__, __func__, pAuthState->clnt->sql);
        return SQLITE_OK;
      } else {
        return SQLITE_DENY;
      }
    case SQLITE_CREATE_TRIGGER:
      pAuthState->numDdls++;
      if (denyDdl || denyCreateTrigger) {
        return SQLITE_DENY;
      } else {
        return SQLITE_OK;
      }
    case SQLITE_CREATE_TEMP_INDEX:
    case SQLITE_CREATE_TEMP_TABLE:
    case SQLITE_CREATE_TEMP_TRIGGER:
    case SQLITE_CREATE_TEMP_VIEW:
    case SQLITE_DROP_TEMP_INDEX:
    case SQLITE_DROP_TEMP_TABLE:
    case SQLITE_DROP_TEMP_TRIGGER:
    case SQLITE_DROP_TEMP_VIEW:
      pAuthState->numDdls++;
      return allowTempDDL ? SQLITE_OK : SQLITE_DENY;
    case SQLITE_READ:
        record_locked_vtable(pAuthState, zArg1);
        return SQLITE_OK;
    default:
      return SQLITE_OK;
  }
}

static void comdb2_reset_authstate(struct sqlthdstate *thd)
{
    bzero(&thd->authState, sizeof(thd->authState));
}

static void comdb2_set_authstate(struct sqlthdstate *thd, struct sqlclntstate *clnt, int flags)
{
    thd->authState.clnt = clnt;
    thd->authState.flags = flags;
    thd->authState.numDdls = 0;
    thd->authState.numVTableLocks = 0;
    thd->authState.vTableLocks = NULL;
    thd->authState.hasVTables = 0;
    thd->authState.db = thd->sqldb;
}

static void comdb2_setup_authorizer_for_sqlite(
  sqlite3 *db,
  struct sql_authorizer_state *pAuthState,
  int bEnable
){
  if( !db ) return;
  if( bEnable ){
    sqlite3_set_authorizer(db, comdb2_authorizer_for_sqlite, pAuthState);
  }else{
    sqlite3_set_authorizer(db, NULL, NULL);
  }
}

int sqlite3_is_success(int rc){
  return (rc==SQLITE_OK) || (rc==SQLITE_ROW) || (rc==SQLITE_DONE);
}

int sqlite3_is_prepare_only(
  struct sqlclntstate *clnt
){
  if( clnt!=NULL && clnt->prepare_only ){
    return 1;
  }
  return 0;
}

int sqlite3_maybe_step(
  struct sqlclntstate *clnt,
  sqlite3_stmt *stmt
){
  assert( clnt );
  assert( stmt );
  int steps = clnt->nsteps++;
  if( unlikely(sqlite3_is_prepare_only(clnt)) ){
    if( sqlite3_column_count(stmt)>0 ){
      return steps==0 ? SQLITE_ROW : SQLITE_DONE;
    }else{
      return SQLITE_DONE;
    }
  }
  clnt->step_rc = sqlite3_step(stmt);
  return clnt->step_rc;
}

int sqlite3_can_get_column_type_and_data(struct sqlclntstate *clnt,
                                         sqlite3_stmt *stmt)
{
    if (!sqlite3_is_prepare_only(clnt)) {
        /*
        ** When the client is not in 'prepare only' mode, the result set
        ** should always be available (i.e. anytime after sqlite3_step()
        ** is called).  The column type / data should be available -IF-
        ** this is not a write transaction.  An assert is used here to
        ** verify this invariant.
        */
        assert(clnt->step_rc != SQLITE_ROW || sqlite3_hasResultSet(stmt));
        return 1;
    }
    if (sqlite3_hasResultSet(stmt)) {
        /*
        ** If the result set is available for the prepared statement, e.g.
        ** due to sqlite3_step() having been called, it can always be used
        ** to query the column type and data.  It shouldn't be possible to
        ** reach this point in 'prepare only' mode; therefore, assert this
        ** invariant here.
        */
        assert(!sqlite3_is_prepare_only(clnt));
        return 1;
    }
    return 0;
}

int validate_columns(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    int ncols = column_count(clnt, stmt), type;
    if (!sqlite3_can_get_column_type_and_data(clnt, stmt))
        return 0;
    for (int i = 0; i < ncols; ++i) {
        type = column_type(clnt, stmt, i);
        if (type == (int)SQLITE_NEXTSEQ)
            return -1;
    }
    return 0;
}

int get_sqlite3_column_type(struct sqlclntstate *clnt, sqlite3_stmt *stmt,
                            int col, int skip_decltype)
{
    int type = SQLITE_NULL;
    int ncols = column_count(clnt, stmt);

    if (sqlite3_can_get_column_type_and_data(clnt, stmt)) {
        int colNum = col;
        if (clnt->typessql_state && !skip_decltype)
            colNum += ncols;
        type = column_type(clnt, stmt, colNum);
        if (type == SQLITE_NULL && !skip_decltype) {
            type = typestr_to_type(sqlite3_column_decltype(stmt, col));
        }
        if (type == SQLITE_DECIMAL) {
            type = SQLITE_TEXT;
        }
    }
    return type;
}

int is_column_type_null(struct sqlclntstate *clnt, sqlite3_stmt *stmt, int col)
{
    if (!clnt->fdb_push) {
        return get_sqlite3_column_type(clnt, stmt, col, 1) == SQLITE_NULL ||
               column_type(clnt, stmt, col) == SQLITE_NULL;
    }

    return column_type(clnt, stmt, col) == SQLITE_NULL;
}

pthread_mutex_t open_serial_lock = PTHREAD_MUTEX_INITIALIZER;

int sqlite3_open_serial(const char *filename, sqlite3 **ppDb,
                        struct sqlthdstate *thd)
{
    int serial = gbl_serialise_sqlite3_open;
    if (serial)
        Pthread_mutex_lock(&open_serial_lock);
    int rc = sqlite3_open(filename, ppDb, thd);
    if (rc == SQLITE_OK && thd != NULL) {
        comdb2_setup_authorizer_for_sqlite(*ppDb, &thd->authState, 1);
    }
    if (serial)
        Pthread_mutex_unlock(&open_serial_lock);
    return rc;
}

int sqlite3_close_serial(sqlite3 **ppDb)
{
    int rc = SQLITE_ERROR;
    int serial = gbl_serialise_sqlite3_open;
    if( serial ) Pthread_mutex_lock(&open_serial_lock);
    if( ppDb && *ppDb ){
        rc = sqlite3_close(*ppDb);
        if( rc==SQLITE_OK ){
            *ppDb = NULL;
        }else{
            logmsg(LOGMSG_ERROR,
                   "%s: sqlite3_close FAILED rc=%d, msg=%s\n",
                   __func__, rc, sqlite3_errmsg(*ppDb));
        }
    }
    if( serial ) Pthread_mutex_unlock(&open_serial_lock);
    return rc;
}

/* We'll probably play around with this formula quite a bit. The
   idea is that reads/writes to/from temp tables are cheap, since
   they are in memory, writes to real tables are really expensive
   since they need to replicate, finds are more expensive then
   nexts. The last assertion is less true if we are in index mode
   since a next is effectively a find, but we'll overlook that here
   since we're moving towards cursors these days. Temp table
   reads/writes should also be considered more expensive if the
   temp table spills to disk, etc.

   Previously, the following formula was (presumably) used in this
   function:

                      (   thd->nwrite  * 100.0)
                    + (    thd->nfind  *  10.0)
                    + (    thd->nmove  *   1.0)
                    + (thd->ntmpwrite  *   0.2)
                    + ( thd->ntmpread  *   0.1)

   Interestingly, the "nblobs" field was incremented (in sqlglue.c)
   but not used by this formula (nor was it used anywhere else).
*/
double query_cost(struct sql_thread *thd)
{
    return thd->cost;
}

void save_thd_cost_and_reset(
  struct sqlthdstate *thd,
  Vdbe *pVdbe
){
  pVdbe->luaSavedCost = thd->sqlthd->cost;
  thd->sqlthd->cost = 0.0;
}

void restore_thd_cost_and_reset(
  struct sqlthdstate *thd,
  Vdbe *pVdbe
){
  thd->sqlthd->cost = pVdbe->luaSavedCost;
  pVdbe->luaSavedCost = 0.0;
}

void clnt_query_cost(
  struct sqlthdstate *thd,
  double *pCost,
  int64_t *pPrepMs
){
  struct sql_thread *sqlthd = thd->sqlthd;
  if (pCost != NULL) *pCost = sqlthd->cost;
  if (pPrepMs != NULL) *pPrepMs = sqlthd->prepms;
}

void sql_dump_hist_statements(void)
{
    struct sql_hist *h;
    struct tm tm;
    char rqid[50];

    Pthread_mutex_lock(&gbl_sql_lock);
    LISTC_FOR_EACH(&thedb->sqlhist, h, lnk)
    {
        time_t t;
        if (h->txnid)
            snprintf(rqid, sizeof(rqid), "txn %016llx ",
                     (unsigned long long)h->txnid);
        else
            rqid[0] = 0;

        t = h->when;
        localtime_r((time_t *)&t, &tm);
        if (h->conn.pename[0]) {
            logmsg(LOGMSG_USER, "%02d/%02d/%02d %02d:%02d:%02d %spindex %d task %.8s pid %d "
                   "mach %d time %lldms prepTime %lldms cost %f sql: %s\n",
                   tm.tm_mon + 1, tm.tm_mday, 1900 + tm.tm_year, tm.tm_hour,
                   tm.tm_min, tm.tm_sec, rqid, h->conn.pindex,
                   (char *)h->conn.pename, h->conn.pid, h->conn.node,
                   (long long int)h->cost.time, (long long int)h->cost.prepTime,
                   h->cost.cost, string_ref_cstr(h->sql_ref));
        } else {
            logmsg(LOGMSG_USER,
                   "%02d/%02d/%02d %02d:%02d:%02d %stime %lldms prepTime %lldms cost %f sql: %s\n",
                   tm.tm_mon + 1, tm.tm_mday, 1900 + tm.tm_year, tm.tm_hour,
                   tm.tm_min, tm.tm_sec, rqid, (long long int)h->cost.time,
                   (long long int)h->cost.prepTime, h->cost.cost, string_ref_cstr(h->sql_ref));
        }
    }
    Pthread_mutex_unlock(&gbl_sql_lock);
}

static void clear_cost(struct sql_thread *thd)
{
    if (thd) {
        hash_clear(thd->query_hash);
        thd->cost = 0;
        thd->had_tablescans = 0;
        thd->had_temptables = 0;
    }
}

static void reset_sql_steps(struct sql_thread *thd)
{
    thd->nmove = thd->nfind = thd->nwrite = 0;
}

static int get_sql_steps(struct sql_thread *thd)
{
    return thd->nmove + thd->nfind + thd->nwrite;
}

static void add_steps(struct sqlclntstate *clnt, double steps)
{
    clnt->plugin.add_steps(clnt, steps);
}

/*
** NOTE: This function checks if zSql starts with one of the SQL (meta)
**       command names from the azMeta array.  The azMeta array must have
**       a final element with a NULL value.  The return value will either
**       be zero upon failing to find a match -OR- one plus the matching
**       index upon finding a match.
*/
static int is_meta_sql(const char *zSql, const char *azMeta[])
{
    size_t len = strlen(zSql);
    for (int i = 0; azMeta[i]; i++) {
        size_t metaLen = strlen(azMeta[i]);
        if (strncasecmp(zSql, azMeta[i], metaLen) == 0) {
            if (len == metaLen) {
                return i + 1; /* end-of-string */
            } else {
                char nextCh = zSql[metaLen];
                if ((nextCh == ';') || isspace(nextCh)) {
                    return i + 1; /* command delimiter */
                }
            }
        }
    }
    return 0;
}

static int is_stored_proc_sql(const char *zSql)
{
    /*
    ** WARNING: The last element of this array must be NULL.
    */
    static const char *azMeta[] = { "EXEC", "EXECUTE", NULL };
    return is_meta_sql(zSql, azMeta);
}

int is_stored_proc(struct sqlclntstate *clnt)
{
    return is_stored_proc_sql(clnt->sql);
}

static tsql_meta_command_t is_transaction_meta_sql(const char *zSql)
{
    /*
    ** WARNING: The last element of this array must be NULL.  If this
    **          array is changed, the tsql_meta_command enumeration must
    **          be changed as well.  The values in the tsql_meta_command
    **          enumeration represent the indexes of the associated
    **          meta-command names string in this array.
    */
    static const char *azMeta[] = { "BEGIN", "COMMIT", "ROLLBACK", NULL };
    return is_meta_sql(zSql, azMeta);
}

static tsql_meta_command_t is_transaction_meta(struct sqlclntstate *clnt)
{
    return is_transaction_meta_sql(clnt->sql);
}

/* Save copy of sql statement and performance data.  If any other code
   should run after a sql statement is completed it should end up here. */
static void sql_statement_done(struct sql_thread *thd, struct reqlogger *logger,
                               struct sqlclntstate *clnt, sqlite3_stmt *stmt,
                               int stmt_rc)
{
    if (thd == NULL || clnt == NULL) {
        return;
    }

    if (clnt->limits.maxcost_warn && (thd->cost > clnt->limits.maxcost_warn)) {
        logmsg(LOGMSG_USER,
               "[%s] warning: query exceeded cost threshold (%f >= %f): %s\n",
               clnt->origin, thd->cost, clnt->limits.maxcost_warn, clnt->sql);
    }
    if (clnt->limits.tablescans_warn && thd->had_tablescans) {
        logmsg(LOGMSG_USER, "[%s] warning: query had a table scan: %s\n",
               clnt->origin, clnt->sql);
    }
    if (clnt->limits.temptables_warn && thd->had_temptables) {
        logmsg(LOGMSG_USER,
               "[%s] warning: query created a temporary table: %s\n",
               clnt->origin, clnt->sql);
    }

    thd->crtshard = 0;

    unsigned long long rqid = clnt->osql.rqid;
    if (rqid != 0 && rqid != OSQL_RQID_USE_UUID)
        reqlog_set_rqid(logger, &rqid, sizeof(rqid));
    else if (!comdb2uuid_is_zero(clnt->osql.uuid)) {
        /* have an "id_set" instead? */
        reqlog_set_rqid(logger, clnt->osql.uuid, sizeof(uuid_t));
    }

    LISTC_T(struct sql_hist) lst;
    listc_init(&lst, offsetof(struct sql_hist, lnk));

    if (!clnt->sql_ref && clnt->sql) {
        // incomming from dohast will not have sql_ref set
        clnt->sql_ref = create_string_ref(clnt->sql);
    }
    struct sql_hist *h = calloc(1, sizeof(struct sql_hist));
    h->sql_ref = get_ref(clnt->sql_ref);
    h->cost.cost = query_cost(thd);
    h->cost.time = comdb2_time_epochms() - thd->startms;
    h->cost.prepTime = thd->prepms;
    h->when = thd->stime;
    h->txnid = rqid;

    time_metric_add(thedb->service_time, h->cost.time);
    clnt->last_cost = (int64_t) h->cost.cost;

    /* request logging framework takes care of logging long sql requests */
    reqlog_set_cost(logger, h->cost.cost);
    if (rqid) {
        reqlog_logf(logger, REQL_INFO, "rqid=%llx", rqid);
    }


    unsigned char fingerprint[FINGERPRINTSZ];
    int have_fingerprint = 0;
    double cost;
    int64_t time;
    int64_t prepTime;
    int64_t rows;
    int is_lua;

    if (1 || clnt->query_stats == NULL) {
        record_query_cost(thd, clnt);
        reqlog_set_path(logger, clnt->query_stats);
    }

    if (gbl_fingerprint_queries) {
        if (h->sql_ref) {
            if ((is_lua = is_stored_proc_sql(string_ref_cstr(h->sql_ref)))) {
                cost = clnt->spcost.cost;
                time = clnt->spcost.time;
                prepTime = clnt->spcost.prepTime;
                rows = clnt->spcost.rows;
            } else {
                cost = h->cost.cost;
                time = h->cost.time;
                prepTime = h->cost.prepTime;
                rows = clnt->nrows;
            }
            if (clnt->work.zOrigNormSql) { /* NOTE: Not subject to prepare. */
                add_fingerprint(clnt, stmt, h->sql_ref, clnt->work.zOrigNormSql, cost, time, prepTime, rows, logger,
                                fingerprint, is_lua);
                have_fingerprint = 1;
            } else if (clnt->work.zNormSql &&
                       sqlite3_is_success(clnt->prep_rc)) {
                add_fingerprint(clnt, stmt, h->sql_ref, clnt->work.zNormSql, cost, time, prepTime, rows, logger,
                                fingerprint, is_lua);
                have_fingerprint = 1;
            } else {
                reqlog_reset_fingerprint(logger, FINGERPRINTSZ);
            }
        } else {
            reqlog_reset_fingerprint(logger, FINGERPRINTSZ);
        }
    }

    reqlog_set_vreplays(logger, clnt->verify_retries);

    if (clnt->saved_rc)
        reqlog_set_error(logger, clnt->saved_errstr, clnt->saved_rc);

    reqlog_set_rows(logger, clnt->nrows);
    reqlog_end_request(logger, stmt_rc, __func__, __LINE__);
    if (clnt->osql.sock_started == 0)
        comdb2uuid_clear(clnt->osql.uuid);

    if (have_fingerprint) {
        /*
        ** NOTE: The intent of this code is to check if a fingerprint was
        **       already calculated as part of SQL query prioritization;
        **       if so, it should match the fingerprint calculated above
        **       for use in the event log, etc.  If that is not the case,
        **       issue an error message to the trace log file.
        */
        char zFingerprint1[FINGERPRINTSZ*2+1];
        char zFingerprint2[FINGERPRINTSZ*2+1];
        memset(zFingerprint1, 0, sizeof(zFingerprint1));
        memset(zFingerprint2, 0, sizeof(zFingerprint2));
        if ((memcmp(clnt->work.aFingerprint, zFingerprint1, FINGERPRINTSZ) != 0) &&
            (memcmp(fingerprint, clnt->work.aFingerprint, FINGERPRINTSZ) != 0)) {
            util_tohex(zFingerprint1, (char *)fingerprint, FINGERPRINTSZ);
            util_tohex(zFingerprint2, (char *)clnt->work.aFingerprint, FINGERPRINTSZ);
            logmsg(LOGMSG_ERROR, "%s: mismatch between fingerprint #1 {%s} (log) "
                   "and #2 {%s} (work)\n", __func__, zFingerprint1, zFingerprint2);
        }
    }

    if (clnt->rawnodestats) {
        clnt->rawnodestats->sql_steps += get_sql_steps(thd);
        time_metric_add(clnt->rawnodestats->svc_time, h->cost.time);
        if (have_fingerprint)
            add_fingerprint_to_rawstats(clnt->rawnodestats, fingerprint, cost,
                                        rows, time);
        update_api_history(clnt->rawnodestats->api_history, clnt->api_driver_name, clnt->api_driver_version);
    }

    reset_sql_steps(thd);

    if (clnt->conninfo.pename[0]) {
        h->conn = clnt->conninfo;
    }

    Pthread_mutex_lock(&gbl_sql_lock);
    {
        quantize(q_sql_min, h->cost.time);
        quantize(q_sql_hour, h->cost.time);
        quantize(q_sql_all, h->cost.time);
        quantize(q_sql_steps_min, h->cost.cost);
        quantize(q_sql_steps_hour, h->cost.cost);
        quantize(q_sql_steps_all, h->cost.cost);

        add_steps(clnt, h->cost.cost);

        listc_abl(&thedb->sqlhist, h);
        while (listc_size(&thedb->sqlhist) > gbl_sqlhistsz) {
            h = listc_rtl(&thedb->sqlhist);
            listc_abl(&lst, h);
        }
    }
    Pthread_mutex_unlock(&gbl_sql_lock);
    for (h = listc_rtl(&lst); h; h = listc_rtl(&lst)) {
        put_ref(&h->sql_ref);
        free(h);
    }

    struct query_path_component *qc = listc_rtl(&thd->query_stats);
    while (qc) {
        free(qc);
        qc = listc_rtl(&thd->query_stats);
    }

    clear_cost(thd);

    if (gbl_old_column_names && query_preparer_plugin &&
        query_preparer_plugin->do_cleanup) {
        query_preparer_plugin->do_cleanup(clnt);
    }
}

void clear_sqlhist()
{
    Pthread_mutex_lock(&gbl_sql_lock);
    for (struct sql_hist *h = listc_rtl(&thedb->sqlhist); h; h = listc_rtl(&thedb->sqlhist)) {
        put_ref(&h->sql_ref);
        free(h);
    }
    Pthread_mutex_unlock(&gbl_sql_lock);
}

void sql_set_sqlengine_state(struct sqlclntstate *clnt, char *file, int line,
                             int newstate)
{
    if (gbl_track_sqlengine_states)
        logmsg(LOGMSG_USER, "%p: %p %s:%d %d->%d\n", (void *)pthread_self(), clnt, file, line, clnt->ctrl_sqlengine,
               newstate);

    if (newstate == SQLENG_WRONG_STATE) {
        logmsg(LOGMSG_ERROR, "sqlengine entering wrong state from state %d file %s line %d.\n",
               clnt->ctrl_sqlengine, file, line);
    }

    clnt->sqlengine_state_file = file;
    clnt->sqlengine_state_line = line;
    clnt->last_sqlengine_state = clnt->ctrl_sqlengine;
    clnt->ctrl_sqlengine = newstate;
}

static int retrieve_snapshot_info(char *sql, char *tzname)
{
    char *str = sql;

    if (str && *str) {
        str = skipws(str);

        if (str && *str) {
            /* skip "transaction" if any */
            if (!strncasecmp(str, "transaction", 11)) {
                str += 11;
                str = skipws(str);
            }

            if (str && *str) {
                /* handle "as of" */
                if (!strncasecmp(str, "as", 2)) {
                    str += 2;
                    str = skipws(str);
                    if (str && *str) {
                        if (!strncasecmp(str, "of", 2)) {
                            str += 2;
                            str = skipws(str);
                        }
                    } else {
                        logmsg(LOGMSG_ERROR,
                               "Incorrect syntax, use begin ... as of ...\n");
                        return -1;
                    }
                } else {
                    logmsg(LOGMSG_USER,
                           "Incorrect syntax, use begin ... as of ...\n");
                    return -1;
                }
            } else
                return 0;

            if (str && *str) {
                if (!strncasecmp(str, "datetime", 8)) {
                    str += 8;
                    str = skipws(str);

                    if (str && *str) {
                        /* convert this to a decimal and pass it along */
                        server_datetime_t sdt;
                        struct field_conv_opts_tz convopts = {0};
                        int outdtsz;
                        long long ret = 0;
                        int isnull = 0;

                        memcpy(convopts.tzname, tzname,
                               sizeof(convopts.tzname));
                        convopts.flags = FLD_CONV_TZONE;

                        if (CLIENT_CSTR_to_SERVER_DATETIME(
                                str, strlen(str) + 1, 0,
                                (struct field_conv_opts *)&convopts, NULL, &sdt,
                                sizeof(sdt), &outdtsz, NULL, NULL)) {
                            logmsg(LOGMSG_ERROR,
                                   "Failed to parse snapshot datetime value\n");
                            return -1;
                        }

                        if (SERVER_DATETIME_to_CLIENT_INT(
                                &sdt, sizeof(sdt), NULL, NULL, &ret,
                                sizeof(ret), &isnull, &outdtsz, NULL, NULL)) {
                            logmsg(LOGMSG_ERROR, "Failed to convert snapshot "
                                                 "datetime value to epoch\n");
                            return -1;
                        } else {
                            long long lcl_ret = flibc_ntohll(ret);
                            if ((gbl_new_snapisol_asof || gbl_modsnap_asof) &&
                                bdb_is_timestamp_recoverable(thedb->bdb_env,
                                                             lcl_ret) <= 0) {
                                logmsg(LOGMSG_ERROR,
                                       "No log file to maintain "
                                       "snapshot epoch %lld\n",
                                       lcl_ret);
                                return -1;
                            } else {
                                logmsg(LOGMSG_DEBUG,
                                       "Detected snapshot epoch %lld\n",
                                       lcl_ret);
                                return lcl_ret;
                            }
                        }
                    } else {
                        logmsg(LOGMSG_ERROR,
                               "Missing datetime info for snapshot\n");
                        return -1;
                    }
                } else {
                    logmsg(LOGMSG_ERROR,
                           "Missing snapshot information or garbage "
                           "after \"begin\"\n");
                    return -1;
                }
            } else {
                logmsg(LOGMSG_ERROR, "Missing snapshot info after \"as of\"\n");
                return -1;
            }
        }
    }

    return 0;
}

static inline void set_asof_snapshot(struct sqlclntstate *clnt, int val,
                                     const char *func, int line)
{
    clnt->is_asof_snapshot = val;
}

static inline int get_asof_snapshot(struct sqlclntstate *clnt)
{
    return clnt->is_asof_snapshot;
}

static int snapshot_as_of(struct sqlclntstate *clnt)
{
    int epoch = 0;

    if (strlen(clnt->sql) > 6)
        epoch = retrieve_snapshot_info(&clnt->sql[6], clnt->tzname);

    if (epoch < 0) {
        /* overload this for now */
        clnt->had_errors = 1;
        sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_WRONG_STATE);
        return -1;
    } else {
        clnt->snapshot = epoch;
        set_asof_snapshot(clnt, (epoch != 0), __func__, __LINE__);
    }
    return 0;
}

void set_sent_data_to_client(struct sqlclntstate *clnt, int val,
                             const char *func, int line)
{
    clnt->sent_data_to_client = val;
}

/**
 * Cluster here all pre-sqlite parsing, detecting requests that
 * are not handled by sqlite (transaction commands, pragma, stored proc,
 * blocked sql, and so on)
 */
static void sql_update_usertran_state(struct sqlclntstate *clnt)
{
    const char *sql = clnt->sql;

    if (!in_client_trans(clnt)) {
        clnt->start_gen = bdb_get_rep_gen(thedb->bdb_env);
        set_sent_data_to_client(clnt, 0, __func__, __LINE__);
        set_asof_snapshot(clnt, 0, __func__, __LINE__);
    }

    if (!sql)
        return;

    /* begin, commit, rollback should arrive over the socket only
       for socksql, recom, snapisol and serial */
    tsql_meta_command_t meta = is_transaction_meta_sql(clnt->sql);

    if (meta == TSMC_BEGIN) {
        clnt->snapshot = 0;

        /*fprintf(stderr, "got begin\n");*/
        if (clnt->ctrl_sqlengine != SQLENG_NORMAL_PROCESS) {
            /* already in a transaction */
            if (clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
                logmsg(LOGMSG_ERROR, "%s CLNT %p I AM ALREADY IN TRANS\n", __func__,
                        clnt);
            } else {
                logmsg(LOGMSG_ERROR, "%s I AM IN TRANS-STATE %d\n", __func__,
                        clnt->ctrl_sqlengine);
            }
            sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                    SQLENG_WRONG_STATE);
        } else {
            sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                    SQLENG_PRE_STRT_STATE);

            upd_snapshot(clnt);
            if (snapshot_as_of(clnt))
                return;

            clnt->in_client_trans = 1;

            assert(clnt->ddl_tables == NULL && clnt->dml_tables == NULL &&
                   clnt->ddl_contexts == NULL);
            clnt->ddl_tables = hash_init_strcase(0);
            clnt->dml_tables = hash_init_strcase(0);
            clnt->ddl_contexts = hash_init_strcaseptr(offsetof(struct clnt_ddl_context, name));
        }
    } else if (meta == TSMC_COMMIT) {
        clnt->snapshot = 0;

        if (clnt->ctrl_sqlengine != SQLENG_INTRANS_STATE &&
            clnt->ctrl_sqlengine != SQLENG_STRT_STATE &&
            clnt->ctrl_sqlengine != SQLENG_FNSH_ABORTED_STATE) {
            /* this is for empty transactions */

            /* not in a transaction */
            sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                    SQLENG_WRONG_STATE);
        } else {
            if (clnt->had_errors) {
                sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                        SQLENG_FNSH_RBK_STATE);
            } else {
                sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                        SQLENG_FNSH_STATE);
            }
            clnt->dbtran.crtchunksize = clnt->dbtran.maxchunksize = 0;
            clnt->dbtran.trans_has_sp = 0;
            clnt->in_client_trans = 0;
        }
    } else if (meta == TSMC_ROLLBACK) {
        clnt->snapshot = 0;

        if (clnt->ctrl_sqlengine != SQLENG_INTRANS_STATE &&
            clnt->ctrl_sqlengine != SQLENG_STRT_STATE &&
            clnt->ctrl_sqlengine != SQLENG_FNSH_ABORTED_STATE)
        /* this is for empty transactions */
        {
            /* not in a transaction */
            sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                    SQLENG_WRONG_STATE);
        } else {
            sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                    SQLENG_FNSH_RBK_STATE);
            clnt->dbtran.crtchunksize = clnt->dbtran.maxchunksize = 0;
            clnt->dbtran.trans_has_sp = 0;
            clnt->in_client_trans = 0;
        }
    }
}

static void log_queue_time(struct reqlogger *logger, struct sqlclntstate *clnt)
{
    if (!gbl_track_queue_time)
        return;
    if (clnt->deque_timeus > clnt->enque_timeus)
        reqlog_logf(logger, REQL_INFO, "queuetime=%dms",
                    U2M(clnt->deque_timeus - clnt->enque_timeus));
    reqlog_set_queue_time(logger, clnt->deque_timeus - clnt->enque_timeus);
}

static void log_cost(struct reqlogger *logger, int64_t cost, int64_t rows) {
    reqlog_set_cost(logger, cost);
    reqlog_set_rows(logger, rows);
}

static void reqlog_setup_begin_commit_rollback(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    query_stats_setup(thd, clnt);

    if (reqlog_get_event(thd->logger) == EV_SQL) {
        unsigned char fp[FINGERPRINTSZ];
        size_t len;
        char stmt[9] = {0}; // begin, commit, or rollback
        int i = 0;
        for (const char *s = clnt->sql; *s != '\0' && *s != ' ' && i < sizeof(stmt) - 1; s++, i++) {
            stmt[i] = (char) tolower(*s);
        }
        if (gbl_fingerprint_queries) {
            calc_fingerprint(stmt, &len, fp);
            reqlog_set_fingerprint(thd->logger, (const char *)fp, FINGERPRINTSZ);
        }
    }
    log_queue_time(thd->logger, clnt);
}

/* begin; send return code */
int handle_sql_begin(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                     enum trans_clntcomm sideeffects)
{
    int rc;

    rc = SQLITE_OK;

    Pthread_mutex_lock(&clnt->wait_mutex);
    /* if this is a new chunk, do not stop the hearbeats.*/
    if (sideeffects != TRANS_CLNTCOMM_CHUNK)
        clnt->ready_for_heartbeats = 0;

    if (sideeffects == TRANS_CLNTCOMM_NORMAL) {
        /* for chunks and SPs (which implicitly call begin)
           we don't want to set up reqlog again */
        reqlog_setup_begin_commit_rollback(thd, clnt);
    }

    /* this is a good "begin", just say "ok" */
    sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_STRT_STATE);

    /* clients don't expect column data if it's a converted request */
    reqlog_logf(thd->logger, REQL_QUERY, "\"%s\" new transaction\n",
                (clnt->sql) ? clnt->sql : "(???.)");

    /* Latch the last commit LSN */
    struct dbtable *db = &thedb->static_table;
    assert(db->handle);
    if (clnt->dbtran.mode == TRANLEVEL_MODSNAP) {
        if (clnt->is_hasql_retry) {
            get_snapshot(clnt, (int *) &clnt->modsnap_start_lsn_file, (int *) &clnt->modsnap_start_lsn_offset);
        }
        if (bdb_get_modsnap_start_state(db->handle, clnt->is_hasql_retry, clnt->snapshot,
                    &clnt->modsnap_start_lsn_file, &clnt->modsnap_start_lsn_offset, 
                    &clnt->last_checkpoint_lsn_file, &clnt->last_checkpoint_lsn_offset)) {
            logmsg(LOGMSG_ERROR, "%s: Failed to get modsnap txn start state\n", __func__);
            rc = SQLITE_INTERNAL;
            goto done;
        }

        if (bdb_register_modsnap(db->handle, clnt->last_checkpoint_lsn_file, clnt->last_checkpoint_lsn_offset, &clnt->modsnap_registration)) {
            logmsg(LOGMSG_ERROR, "%s: Failed to register modsnap txn\n", __func__);
            rc = SQLITE_INTERNAL;
            goto done;
        }
        clnt->modsnap_in_progress = 1;
    }

    if (clnt->osql.replay)
        goto done;

    if (sideeffects == TRANS_CLNTCOMM_NORMAL) {
        write_response(clnt, RESPONSE_ROW_LAST_DUMMY, NULL, 0);
    }

done:
    Pthread_mutex_unlock(&clnt->wait_mutex);

    if (srs_tran_add_query(clnt))
        logmsg(LOGMSG_ERROR, "Fail to create a transaction replay session\n");

    if (sideeffects == TRANS_CLNTCOMM_NORMAL) {
        /* for chunks and SPs, don't end the request */
        reqlog_end_request(thd->logger, -1, __func__, __LINE__);
    }

    return rc;
}

static int handle_sql_wrongstate(struct sqlthdstate *thd,
                                 struct sqlclntstate *clnt)
{

    sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_NORMAL_PROCESS);

    reqlog_new_sql_request(thd->logger, clnt->sql_ref);
    log_queue_time(thd->logger, clnt);

    reqlog_logf(thd->logger, REQL_QUERY,
                "\"%s\" wrong transaction command receive\n",
                (clnt->sql) ? clnt->sql : "(???.)");

    write_response(clnt, RESPONSE_ERROR_BAD_STATE,
                   "sqlinterfaces: wrong sql handle state\n", 0);

    clnt->intrans = 0;
    reset_clnt_flags(clnt);

    reqlog_end_request(thd->logger, -1, __func__, __LINE__);
    if (clnt->osql.sock_started == 0)
        comdb2uuid_clear(clnt->osql.uuid);

    if (srs_tran_destroy(clnt))
        logmsg(LOGMSG_ERROR, "Fail to destroy transaction replay session\n");

    return SQLITE_INTERNAL;
}

void reset_query_effects(struct sqlclntstate *clnt)
{
    bzero(&clnt->effects, sizeof(clnt->effects));
    bzero(&clnt->log_effects, sizeof(clnt->effects));
    bzero(&clnt->chunk_effects, sizeof(clnt->chunk_effects));
}

char *sqlenginestate_tostr(int state)
{
    switch (state) {
    case SQLENG_NORMAL_PROCESS:
        return "SQLENG_NORMAL_PROCESS";
        break;
    case SQLENG_PRE_STRT_STATE:
        return "SQLENG_PRE_STRT_STATE";
        break;
    case SQLENG_STRT_STATE:
        return "SQLENG_STRT_STATE";
        break;
    case SQLENG_INTRANS_STATE:
        return "SQLENG_INTRANS_STATE";
        break;
    case SQLENG_FNSH_STATE:
        return "SQLENG_FNSH_STATE";
        break;
    case SQLENG_FNSH_RBK_STATE:
        return "SQLENG_FNSH_RBK_STATE";
        break;
    case SQLENG_FNSH_ABORTED_STATE:
        return "SQLENG_FNSH_ABORTED_STATE";
        break;
    case SQLENG_WRONG_STATE:
        return "SQLENG_WRONG_STATE";
        break;
    default:
        return "???";
    }
}

int gbl_snapshot_serial_verify_retry = 1;

inline int replicant_is_able_to_retry(struct sqlclntstate *clnt)
{
    if (clnt->verifyretry_off || clnt->dbtran.trans_has_sp || clnt->is_participant)
        return 0;

    if ((clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
         clnt->dbtran.mode == TRANLEVEL_SERIAL ||
         clnt->dbtran.mode == TRANLEVEL_MODSNAP) &&
        !get_asof_snapshot(clnt) && gbl_snapshot_serial_verify_retry)
        return !clnt->sent_data_to_client;

    return clnt->dbtran.mode != TRANLEVEL_SNAPISOL &&
           clnt->dbtran.mode != TRANLEVEL_SERIAL &&
           clnt->dbtran.mode != TRANLEVEL_MODSNAP;
}

static inline int replicant_can_retry_rc(struct sqlclntstate *clnt, int rc)
{
    if (clnt->verifyretry_off || clnt->dbtran.trans_has_sp || clnt->is_participant)
        return 0;

    /* Any isolation level can retry if nothing has been read */
    if ((rc == CDB2ERR_NOTSERIAL || rc == CDB2ERR_VERIFY_ERROR) &&
        !clnt->sent_data_to_client && !get_asof_snapshot(clnt) &&
        gbl_snapshot_serial_verify_retry)
        return 1;

    /* Verify error can be retried in reccom or lower */
    return (rc == CDB2ERR_VERIFY_ERROR) &&
           (clnt->dbtran.mode != TRANLEVEL_SNAPISOL) &&
           (clnt->dbtran.mode != TRANLEVEL_SERIAL) && 
           (clnt->dbtran.mode != TRANLEVEL_MODSNAP);
}

static int free_clnt_ddl_context(void *obj, void *arg)
{
    struct clnt_ddl_context *ctx = obj;
    comdb2ma_destroy(ctx->mem);
    free(ctx->ctx);
    free(ctx);
    return 0;
}
int free_it(void *obj, void *arg)
{
    free(obj);
    return 0;
}
void destroy_hash(hash_t *h, int (*free_func)(void *, void *))
{
    if (!h)
        return;
    hash_for(h, free_func, NULL);
    hash_clear(h);
    hash_free(h);
}

extern int gbl_early_verify;
extern int gbl_osql_send_startgen;
extern int gbl_forbid_incoherent_writes;

void abort_dbtran(struct sqlclntstate *clnt)
{
    switch (clnt->dbtran.mode) {
    case TRANLEVEL_SOSQL:
        osql_sock_abort(clnt, OSQL_SOCK_REQ);
        if (clnt->selectv_arr) {
            currangearr_free(clnt->selectv_arr);
            clnt->selectv_arr = NULL;
        }
        break;

    case TRANLEVEL_RECOM:
    case TRANLEVEL_MODSNAP:
        recom_abort(clnt);
        break;

    case TRANLEVEL_SNAPISOL:
    case TRANLEVEL_SERIAL:
        serial_abort(clnt);
        if (clnt->arr) {
            currangearr_free(clnt->arr);
            clnt->arr = NULL;
        }
        if (clnt->selectv_arr) {
            currangearr_free(clnt->selectv_arr);
            clnt->selectv_arr = NULL;
        }

        break;

    default:
        /* I don't expect this */
        abort();
    }

    clnt->intrans = 0;
    sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                            SQLENG_FNSH_ABORTED_STATE);
    reset_query_effects(clnt);
}

void handle_sql_intrans_unrecoverable_error(struct sqlclntstate *clnt)
{
    if (clnt && clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE)
        abort_dbtran(clnt);
}

static int do_commitrollback(struct sqlthdstate *thd, struct sqlclntstate *clnt, enum trans_clntcomm sideeffects)
{
    int irc = 0, rc = 0, bdberr = 0;

    clnt->modsnap_in_progress = 0;
    if (clnt->modsnap_registration) {
        bdb_unregister_modsnap(thedb->bdb_env, clnt->modsnap_registration);
        clnt->modsnap_registration = NULL;
    }

    if (!clnt->intrans) {
        reqlog_logf(thd->logger, REQL_QUERY, "\"%s\" ignore (no transaction)\n",
                    (clnt->sql) ? clnt->sql : "(???.)");

        rc = SQLITE_OK;
    } else {
        clear_session_tbls(clnt);
        clear_participants(clnt);
        sql_debug_logf(clnt, __func__, __LINE__, "starting\n");

        switch (clnt->dbtran.mode) {
        case TRANLEVEL_RECOM:
        case TRANLEVEL_MODSNAP: {
            /* here we handle the communication with bp */
            if (clnt->ctrl_sqlengine == SQLENG_FNSH_STATE) {
                rc = recom_commit(clnt, thd->sqlthd, clnt->tzname, 0);
                /* if a transaction exists
                   (it doesn't for empty begin/commit */
                if (clnt->dbtran.shadow_tran) {
                    if (rc == SQLITE_OK) {
                        irc = trans_commit_shadow(clnt->dbtran.shadow_tran,
                                                  &bdberr);

                        reqlog_logf(thd->logger, REQL_QUERY,
                                    "\"%s\" RECOM commit irc=%d rc=%d\n",
                                    (clnt->sql) ? clnt->sql : "(???.)", irc,
                                    rc);
                    } else {
                        if (rc == SQLITE_ABORT) {
                            /* convert this to user code */
                            rc = blockproc2sql_error(clnt->osql.xerr.errval,
                                                     __func__, __LINE__);
                        }
                        irc = trans_abort_shadow(
                            (void **)&clnt->dbtran.shadow_tran, &bdberr);

                        reqlog_logf(thd->logger, REQL_QUERY,
                                    "\"%s\" RECOM abort irc=%d rc=%d\n",
                                    (clnt->sql) ? clnt->sql : "(???.)", irc,
                                    rc);
                    }
                    if (irc) {
                        logmsg(LOGMSG_ERROR, "%s: failed %s rc=%d bdberr=%d\n",
                               __func__, (rc == SQLITE_OK) ? "commit" : "abort",
                               irc, bdberr);
                    }
                    clnt->dbtran.shadow_tran = NULL;
                }
            } else {
                reset_query_effects(clnt);
                rc = recom_abort(clnt);
                if (rc)
                    logmsg(LOGMSG_ERROR, "%s: recom abort failed %d??\n",
                           __func__, rc);
                reqlog_logf(thd->logger, REQL_QUERY,
                            "\"%s\" RECOM abort(2) irc=%d rc=%d\n",
                            (clnt->sql) ? clnt->sql : "(???.)", irc, rc);
            }

            break;
        }

        case TRANLEVEL_SNAPISOL:
        case TRANLEVEL_SERIAL: {

            /* here we handle the communication with bp */
            if (clnt->ctrl_sqlengine == SQLENG_FNSH_STATE) {
                if (clnt->dbtran.mode == TRANLEVEL_SERIAL) {
                    rc = serial_commit(clnt, thd->sqlthd, clnt->tzname);
                    sql_debug_logf(clnt, __func__, __LINE__,
                                   "serial-txn returns %d\n", rc);
                } else {
                    rc = snapisol_commit(clnt, thd->sqlthd, clnt->tzname);
                    sql_debug_logf(clnt, __func__, __LINE__,
                                   "snapshot-txn returns %d\n", rc);
                }
                /* if a transaction exists
                   (it doesn't for empty begin/commit */
                if (clnt->dbtran.shadow_tran) {
                    if (rc == SQLITE_OK) {
                        irc = trans_commit_shadow(clnt->dbtran.shadow_tran,
                                                  &bdberr);

                        reqlog_logf(thd->logger, REQL_QUERY,
                                    "\"%s\" %s commit irc=%d rc=%d\n",
                                    (clnt->sql) ? clnt->sql : "(???.)",
                                    (clnt->dbtran.mode == TRANLEVEL_SERIAL)
                                        ? "SERIAL"
                                        : "SNAPISOL",
                                    irc, rc);
                    } else {
                        if (rc == SQLITE_ABORT) {
                            /* convert this to user code */
                            rc = blockproc2sql_error(clnt->osql.xerr.errval,
                                                     __func__, __LINE__);
                            sql_debug_logf(clnt, __func__, __LINE__,
                                           "returning"
                                           " converted-rc %d\n",
                                           rc);
                        } else if (rc == SQLITE_CLIENT_CHANGENODE) {
                            rc = has_high_availability(clnt)
                                     ? CDB2ERR_CHANGENODE
                                     : SQLHERR_MASTER_TIMEOUT;
                        }
                        irc = trans_abort_shadow(
                            (void **)&clnt->dbtran.shadow_tran, &bdberr);

                        reqlog_logf(thd->logger, REQL_QUERY,
                                    "\"%s\" %s abort irc=%d rc=%d\n",
                                    (clnt->sql) ? clnt->sql : "(???.)",
                                    (clnt->dbtran.mode == TRANLEVEL_SERIAL)
                                        ? "SERIAL"
                                        : "SNAPISOL",
                                    irc, rc);
                    }
                    if (irc) {
                        logmsg(LOGMSG_ERROR, "%s: failed %s rc=%d bdberr=%d\n",
                               __func__, (rc == SQLITE_OK) ? "commit" : "abort",
                               irc, bdberr);
                    }
                    clnt->dbtran.shadow_tran = NULL;
                } else {
                    sql_debug_logf(clnt, __func__, __LINE__,
                                   "no-shadow-tran returning %d\n", rc);
                    if (rc == SQLITE_ABORT) {
                        rc = blockproc2sql_error(clnt->osql.xerr.errval,
                                                 __func__, __LINE__);
                        logmsg(LOGMSG_ERROR, "td=%p no-shadow-tran %s line %d, returning %d\n", (void *)pthread_self(),
                               __func__, __LINE__, rc);
                    } else if (rc == SQLITE_CLIENT_CHANGENODE) {
                        rc = has_high_availability(clnt)
                                 ? CDB2ERR_CHANGENODE
                                 : SQLHERR_MASTER_TIMEOUT;
                        logmsg(LOGMSG_ERROR, "td=%p no-shadow-tran %s line %d, returning %d\n", (void *)pthread_self(),
                               __func__, __LINE__, rc);
                    }
                }
            } else {
                reset_query_effects(clnt);
                if (clnt->dbtran.mode == TRANLEVEL_SERIAL) {
                    rc = serial_abort(clnt);
                } else {
                    rc = snapisol_abort(clnt);
                }
                if (rc)
                    logmsg(LOGMSG_ERROR, "%s: serial abort failed %d??\n",
                           __func__, rc);

                reqlog_logf(thd->logger, REQL_QUERY,
                            "\"%s\" %s abort(2) irc=%d rc=%d\n",
                            (clnt->sql) ? clnt->sql : "(???.)",
                            (clnt->dbtran.mode == TRANLEVEL_SERIAL)
                                ? "SERIAL"
                                : "SNAPISOL",
                            irc, rc);
            }

            if (clnt->arr) {
                currangearr_free(clnt->arr);
                clnt->arr = NULL;
            }
            if (clnt->selectv_arr) {
                currangearr_free(clnt->selectv_arr);
                clnt->selectv_arr = NULL;
            }

            break;
        }

        case TRANLEVEL_SOSQL:

            if (clnt->ctrl_sqlengine == SQLENG_FNSH_RBK_STATE) {
                /* user cancelled the transaction */
                clnt->osql.xerr.errval = SQLITE_INTERNAL;
                /* this will cancel the bp tran */

                sql_debug_logf(clnt, __func__, __LINE__,
                               "setting errval to SQLITE_INTERNAL\n");

                reqlog_logf(
                    thd->logger, REQL_QUERY, "\"%s\" SOCKSL abort replay=%d\n",
                    (clnt->sql) ? clnt->sql : "(???.)", clnt->osql.replay);
            }
            if (clnt->ctrl_sqlengine == SQLENG_FNSH_STATE) {
                if (!clnt->skip_peer_chk && gbl_early_verify && !clnt->early_retry && gbl_osql_send_startgen &&
                    clnt->start_gen) {
                    if (clnt->start_gen != bdb_get_rep_gen(thedb->bdb_env))
                        clnt->early_retry = EARLY_ERR_GENCHANGE;
                }
                if (gbl_selectv_rangechk)
                    rc = selectv_range_commit(clnt);
                if (rc) {
                    irc = osql_sock_abort(clnt, OSQL_SOCK_REQ);
                    if (irc) {
                        logmsg(
                            LOGMSG_ERROR,
                            "%s: failed to abort sorese transaction irc=%d\n",
                            __func__, irc);
                    }
                    rc = SQLITE_ABORT;
                } else if (clnt->early_retry) {
                    irc = osql_sock_abort(clnt, OSQL_SOCK_REQ);
                    if (irc) {
                        logmsg(
                            LOGMSG_ERROR,
                            "%s: failed to abort sorese transaction irc=%d\n",
                            __func__, irc);
                    }
                    if (clnt->early_retry == EARLY_ERR_VERIFY) {
                        clnt->osql.xerr.errval = ERR_BLOCK_FAILED + ERR_VERIFY;
                        errstat_cat_str(&(clnt->osql.xerr),
                                        "unable to update record rc = 4");
                    } else if (clnt->early_retry == EARLY_ERR_SELECTV) {
                        clnt->osql.xerr.errval = ERR_CONSTR;
                        errstat_cat_str(&(clnt->osql.xerr),
                                        "constraints error, no genid");
                    } else if (clnt->early_retry == EARLY_ERR_GENCHANGE) {
                        clnt->osql.xerr.errval = ERR_BLOCK_FAILED + ERR_VERIFY;
                        errstat_cat_str(&(clnt->osql.xerr),
                                        "verify error on master swing");
                    }
                    clnt->early_retry = 0;
                    rc = SQLITE_ABORT;
                } else {
                    rc = osql_sock_commit(clnt, OSQL_SOCK_REQ, sideeffects);
                }
                if (rc == SQLITE_ABORT) {
                    /* convert this to user code */
                    rc = blockproc2sql_error(clnt->osql.xerr.errval, __func__,
                                             __LINE__);
                    if (clnt->osql.xerr.errval == ERR_UNCOMMITTABLE_TXN) {
                        osql_set_replay(__FILE__, __LINE__, clnt,
                                        OSQL_RETRY_LAST);
                    }
                    sql_debug_logf(
                        clnt, __func__, __LINE__,
                        "'%s' socksql failed commit rc=%d replay=%d\n",
                        clnt->sql ? clnt->sql : "(?)", rc, clnt->osql.replay);
                    reqlog_logf(thd->logger, REQL_QUERY,
                                "\"%s\" SOCKSL failed commit rc=%d replay=%d\n",
                                (clnt->sql) ? clnt->sql : "(???.)", rc,
                                clnt->osql.replay);
                } else if (rc == 0) {
                    sql_debug_logf(clnt, __func__, __LINE__,
                                   "'%s' socksql commit rc=%d replay=%d\n",
                                   clnt->sql ? clnt->sql : "(?)", rc,
                                   clnt->osql.replay);
                    reqlog_logf(thd->logger, REQL_QUERY,
                                "\"%s\" SOCKSL commit rc=%d replay=%d\n",
                                (clnt->sql) ? clnt->sql : "(???.)", rc,
                                clnt->osql.replay);
                }

                if (rc) {
                    clnt->saved_rc = rc;
                    if (clnt->saved_errstr)
                        free(clnt->saved_errstr);
                    clnt->saved_errstr = strdup(clnt->osql.xerr.errstr);
                }
            } else {
                reset_query_effects(clnt);
                rc = osql_sock_abort(clnt, OSQL_SOCK_REQ);
                sql_debug_logf(clnt, __func__, __LINE__,
                               "'%s' socksql abort rc=%d replay=%d\n",
                               clnt->sql ? clnt->sql : "(?)", rc,
                               clnt->osql.replay);
                reqlog_logf(thd->logger, REQL_QUERY,
                            "\"%s\" SOCKSL abort(2) rc=%d replay=%d\n",
                            (clnt->sql) ? clnt->sql : "(???.)", rc,
                            clnt->osql.replay);
            }

            if (clnt->selectv_arr) {
                currangearr_free(clnt->selectv_arr);
                clnt->selectv_arr = NULL;
            }

            break;
        case TRANLEVEL_INVALID:
            break; // TODO: should return here?
        }
    }

    return rc;
}

/* In a transaction, whenever a non-COMMIT/ROLLBACK command fails, we set
 * clnt->had_errors and report error to the client. Once set, we must not
 * send anything to the client (per the wire protocol?) unless intransresults
 * is set.
 */
static int do_send_commitrollback_response(struct sqlclntstate *clnt,
                                           int sideeffects)
{
    if (sideeffects == TRANS_CLNTCOMM_NORMAL &&
        (send_intrans_response(clnt) || !clnt->had_errors))
        return 1;
    return 0;
}

int handle_sql_commitrollback(struct sqlthdstate *thd,
                              struct sqlclntstate *clnt,
                              enum trans_clntcomm sideeffects)
{

    int rc = 0;
    int outrc = 0;

    clnt->modsnap_in_progress = 0;
    if (clnt->modsnap_registration) {
        bdb_unregister_modsnap(thedb->bdb_env, clnt->modsnap_registration);
        clnt->modsnap_registration = NULL;
    }

    if (sideeffects == TRANS_CLNTCOMM_NORMAL) {
    /* Don't setup(reset) logger for commits of individual chunks,
       and for the implicit commit from an SP */
        reqlog_setup_begin_commit_rollback(thd, clnt);
    }

    int64_t rows = clnt->log_effects.num_updated +
                   clnt->log_effects.num_deleted +
                   clnt->log_effects.num_inserted;

    reqlog_set_cost(thd->logger, 0);
    reqlog_set_rows(thd->logger, rows);

    if (rows > 0 && gbl_forbid_incoherent_writes && !clnt->had_lease_at_begin) {
        abort_dbtran(clnt);
        errstat_cat_str(&clnt->osql.xerr, "failed write from incoherent node");
        clnt->osql.xerr.errval = ERR_BLOCK_FAILED + ERR_VERIFY;
        sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                SQLENG_NORMAL_PROCESS);
        outrc = CDB2ERR_VERIFY_ERROR;
        Pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;
        Pthread_mutex_unlock(&clnt->wait_mutex);
        if (do_send_commitrollback_response(clnt, sideeffects)) {
            write_response(clnt, RESPONSE_ERROR, clnt->osql.xerr.errstr, outrc);
        }
        goto done;
    }

    rc = do_commitrollback(thd, clnt, sideeffects);

    clnt->ins_keys = 0ULL;
    clnt->del_keys = 0ULL;

    if (clnt->arr) {
        currangearr_free(clnt->arr);
        clnt->arr = NULL;
    }
    if (clnt->selectv_arr) {
        currangearr_free(clnt->selectv_arr);
        clnt->selectv_arr = NULL;
    }

    destroy_hash(clnt->ddl_tables, free_it);
    destroy_hash(clnt->dml_tables, free_it);
    clnt->ddl_tables = NULL;
    clnt->dml_tables = NULL;
    destroy_hash(clnt->ddl_contexts, free_clnt_ddl_context);
    clnt->ddl_contexts = NULL;

    /* reset the state after send_done; we use ctrl_sqlengine to know
       if this is a user rollback or an sqlite engine error */
    sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_NORMAL_PROCESS);

#ifdef DEBUG
    if (gbl_debug_sql_opcodes) {
        logmsg(LOGMSG_USER, "%p (U) commits transaction %p %d intran=%d\n", clnt, (void *)pthread_self(),
               clnt->dbtran.mode, clnt->intrans);
    }
#endif

    /* we are out of transaction, mark this here */
    clnt->intrans = 0;
    clnt->dbtran.shadow_tran = NULL;

    if (rc == SQLITE_OK) {
        /* send return code */

        Pthread_mutex_lock(&clnt->wait_mutex);

        write_response(clnt, RESPONSE_EFFECTS, 0, 0);

        /* do not turn heartbeats if this is a chunked transaction */
        if (sideeffects != TRANS_CLNTCOMM_CHUNK)
            clnt->ready_for_heartbeats = 0;

        if (do_send_commitrollback_response(clnt, sideeffects)) {
            /* This is a commit, so we'll have something to send here even on a
             * retry.  Don't trigger code in fsql_write_response that's there
             * to catch bugs when we send back responses on a retry.
             */
            write_response(clnt, RESPONSE_ROW_LAST_DUMMY, NULL, 0);
        }

        outrc = SQLITE_OK; /* the happy case */

        Pthread_mutex_unlock(&clnt->wait_mutex);

        if (clnt->osql.replay != OSQL_RETRY_NONE) {
            /* successful retry */
            osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);

            reqlog_logf(thd->logger, REQL_QUERY,
                        "\"%s\" SOCKSL retried done sendresp=%d\n",
                        (clnt->sql) ? clnt->sql : "(???.)", sideeffects);
        }
    } else {
        /* If this is a verify or serializable error and the client hasn't
         * read any data then it is safe to retry */
        int can_retry = replicant_can_retry_rc(clnt, rc);
        if (can_retry && !clnt->has_recording &&
            clnt->osql.replay != OSQL_RETRY_LAST) {
            if (srs_tran_add_query(clnt))
                logmsg(LOGMSG_USER,
                       "Fail to add commit to transaction replay session\n");

            osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_DO);

            reqlog_logf(thd->logger, REQL_QUERY, "\"%s\" SOCKSL retrying\n",
                        (clnt->sql) ? clnt->sql : "(???.)");

            outrc = SQLITE_OK; /* logical error */
            goto done;
        }

        /* last retry */
        if (can_retry &&
            (clnt->osql.replay == OSQL_RETRY_LAST || clnt->verifyretry_off)) {
            reqlog_logf(thd->logger, REQL_QUERY,
                        "\"%s\" SOCKSL retried done (hit last) sendresp=%d\n",
                        (clnt->sql) ? clnt->sql : "(???.)", sideeffects);
            osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);
        }
        /* if this is still an error, but not verify, pass it back to client */
        else if (!can_retry) {
            reqlog_logf(thd->logger, REQL_QUERY,
                        "\"%s\" SOCKSL retried done "
                        "(non verify error rc=%d) "
                        "sendresp=%d\n",
                        (clnt->sql) ? clnt->sql : "(???.)", rc, sideeffects);
            osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);
        } else {
            assert(can_retry && clnt->has_recording &&
                   clnt->osql.replay == OSQL_RETRY_NONE);
            if (!can_retry || !clnt->has_recording ||
                clnt->osql.replay != OSQL_RETRY_NONE)
                abort();
        }

        if (rc == SQLITE_TOOBIG) {
            strncpy(clnt->osql.xerr.errstr, "transaction too big",
                    sizeof(clnt->osql.xerr.errstr));
            rc = CDB2__ERROR_CODE__TRAN_TOO_BIG;
        }

        Pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;
        Pthread_mutex_unlock(&clnt->wait_mutex);

        outrc = rc;

        if (do_send_commitrollback_response(clnt, sideeffects)) {
            write_response(clnt, RESPONSE_ERROR, clnt->osql.xerr.errstr, outrc);
        }
    }

    if (sideeffects == TRANS_CLNTCOMM_CHUNK)
        return outrc;

done:
    reset_clnt_flags(clnt);
    if (clnt->osql.sock_started == 0)
        comdb2uuid_clear(clnt->osql.uuid);

    if (sideeffects == TRANS_CLNTCOMM_NORMAL) {
        /* end request only for non-chunk and non-SP transactions */
        reqlog_end_request(thd->logger, -1, __func__, __LINE__);
    }

    /* if this is a retry, let the upper layer free the structure */
    if (clnt->osql.replay == OSQL_RETRY_NONE) {
        /* if the last verify retry has failed, dump the transaction */
        if (outrc && clnt->osql.last_replay == OSQL_RETRY_LAST && gbl_dump_history_on_too_many_verify_errors) {
            logmsg(LOGMSG_ERROR, "too many verify errors host=%s task=%s\n", clnt->origin, clnt->argv0);
            srs_tran_print_history(clnt, 2);
            logmsg(LOGMSG_WARN, "------\n");
        }
        if (srs_tran_destroy(clnt))
            logmsg(LOGMSG_ERROR,
                   "Fail to destroy transaction replay session\n");
    }

    return outrc;
}

static const char *osqlretrystr(int i)
{
    switch (i) {
    case OSQL_RETRY_NONE:
        return "OSQL_RETRY_NONE";
    case OSQL_RETRY_DO:
        return "OSQL_RETRY_DO";
    case OSQL_RETRY_LAST:
        return "OSQL_RETRY_LAST";
    default:
        return "???";
    }
}

/* Execute the query.  Caller should flush the sbuf when this returns.
 * Returns -1 if we should abort the client connection, 0 otherwise.
 * We handle the verify comming during commit phase from the master
 */

pthread_key_t current_sql_query_key;
pthread_once_t current_sql_query_once = PTHREAD_ONCE_INIT;

void free_sql(void *p) { free(p); }

void init_current_current_sql_key(void)
{
    Pthread_key_create(&current_sql_query_key, free_sql);
}

extern int gbl_debug_temptables;

int typestr_to_type(const char *ctype)
{
    if (ctype == NULL)
        return SQLITE_TEXT;
    if ((strcmp("smallint", ctype) == 0) || (strcmp("int", ctype) == 0) ||
        (strcmp("largeint", ctype) == 0) || (strcmp("integer", ctype) == 0) ||
        (strcmp("bigint", ctype) == 0))
        return SQLITE_INTEGER;
    else if ((strcmp("smallfloat", ctype) == 0) ||
             (strcmp("float", ctype) == 0) ||
             (strcmp("double", ctype) == 0))
        return SQLITE_FLOAT;
    else if ((strncmp("char", ctype, 4) == 0) ||
             (strncmp("varchar", ctype, 4) == 0))
        return SQLITE_TEXT;
    else if (strncmp("blob", ctype, 4) == 0)
        return SQLITE_BLOB;
    else if (strncmp("datetimeus", ctype, 9) == 0)
        return SQLITE_DATETIMEUS;
    else if (strncmp("datetime", ctype, 8) == 0)
        return SQLITE_DATETIME;
    else if (strstr(ctype, "year") || strstr(ctype, "month"))
        return SQLITE_INTERVAL_YM;
    else if (strstr(ctype, "dayus") || strstr(ctype, "usec"))
        return SQLITE_INTERVAL_DSUS;
    else if (strstr(ctype, "day") || strstr(ctype, "sec"))
        return SQLITE_INTERVAL_DS;
    else {
        return SQLITE_TEXT;
    }
}

static int is_with_statement(char *sql)
{
    if (!sql)
        return 0;
    sql = skipws(sql);
    if (strncasecmp(sql, "with", 4) == 0)
        return 1;
    return 0;
}

static void compare_estimate_cost(sqlite3_stmt *stmt)
{
    int showScanStat =
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_PLANNER_SHOW_SCANSTATS);
#define MAX_DISC_SHOW 30
    int i, k, n, mx;
    if (showScanStat)
        logmsg(LOGMSG_USER, "-------- scanstats --------\n");
    mx = 0;
    for (k = 0; k <= mx; k++) {
        double rEstLoop = 1.0;
        struct {
            double rEst;
            double rActual;
            double delta;
            int isSignificant;
        } discrepancies[MAX_DISC_SHOW] = {{0}};
        int hasDiscrepancy = 0;

        for (i = n = 0; 1; i++) {
            sqlite3_int64 nLoop, nVisit;
            double rEst;
            int iSid;
            const char *zExplain;
            if (sqlite3_stmt_scanstatus(stmt, i, SQLITE_SCANSTAT_NLOOP,
                                        (void *)&nLoop)) {
                break;
            }
            sqlite3_stmt_scanstatus(stmt, i, SQLITE_SCANSTAT_SELECTID,
                                    (void *)&iSid);
            if (iSid > mx)
                mx = iSid;
            if (iSid != k)
                continue;
            if (n == 0) {
                rEstLoop = (double)nLoop;
                if (k > 0)
                    if (showScanStat)
                        logmsg(LOGMSG_USER, "-------- subquery %d -------\n", k);
            }
            sqlite3_stmt_scanstatus(stmt, i, SQLITE_SCANSTAT_NVISIT,
                                    (void *)&nVisit);
            sqlite3_stmt_scanstatus(stmt, i, SQLITE_SCANSTAT_EST,
                                    (void *)&rEst);
            sqlite3_stmt_scanstatus(stmt, i, SQLITE_SCANSTAT_EXPLAIN,
                                    (void *)&zExplain);

            rEstLoop *= rEst;
            double rActual = nLoop > 0 ? (double)nVisit / nLoop
                                       : 0.0; // actual rows per loop
            double delta = fabs(rActual - rEst);

            if (n < MAX_DISC_SHOW) {
                discrepancies[n].rActual = rActual;
                discrepancies[n].rEst = rEst;
                discrepancies[n].delta = delta;
            }
            if ((rActual < 5000 && delta > 10 * rActual) ||
                (rActual >= 5000 && rActual < 10000 && delta > rActual) ||
                (rActual >= 10000 && rActual < 1000000 &&
                 delta > 0.5 * rActual) ||
                (rActual >= 1000000 && delta > 0.1 * rActual)) {
                discrepancies[n].isSignificant = 1;
                hasDiscrepancy++;
            }

            n++;
            if (showScanStat) {
                logmsg(LOGMSG_USER, "Loop %2d: %s\n", n, zExplain);
                logmsg(LOGMSG_USER, "         nLoop=%-8lld nVisit=%-8lld estRowAcc=%-8lld "
                       "rEst=%-8g loopEst=%-8g rAct=%-8g D=%-8g\n",
                       nLoop, nVisit, (sqlite3_int64)(rEstLoop + 0.5), rEst,
                       rEst * nLoop, rActual, delta);
            }
        }

        if (hasDiscrepancy > 0 &&
            bdb_attr_get(thedb->bdb_attr,
                         BDB_ATTR_PLANNER_WARN_ON_DISCREPANCY)) {
            // printf("Problem on:\nLoop    nVisit <> loopEst :: delta\n");
            for (int i = 0; i < n; i++) {
                if (discrepancies[i].isSignificant)
                    logmsg(LOGMSG_USER, "Problem Loop: %-8d rActual: %-8g rEst:%-8g "
                           "Delta:%-8g\n",
                           i, discrepancies[i].rActual, discrepancies[i].rEst,
                           discrepancies[i].delta);
            }
        }
    }
    if (showScanStat)
        logmsg(LOGMSG_USER, "---------------------------\n");
}

static int reload_analyze(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                          int analyze_gen)
{
    // if analyze is running, don't reload
    extern volatile int analyze_running_flag;
    if (analyze_running_flag)
        return 0;
    int rc, got_curtran;
    rc = got_curtran = 0;
    if (!clnt->dbtran.cursor_tran) {
        if ((rc = get_curtran(thedb->bdb_env, clnt)) != 0) {
            logmsg(LOGMSG_ERROR, "%s get_curtran rc:%d\n", __func__, rc);
            return SQLITE_INTERNAL;
        }
        got_curtran = 1;
    }
    sqlite3_mutex_enter(sqlite3_db_mutex(thd->sqldb));
    if ((rc = sqlite3AnalysisLoad(thd->sqldb, 0)) == SQLITE_OK) {
        thd->analyze_gen = analyze_gen;
    } else {
        logmsg(LOGMSG_ERROR, "%s sqlite3AnalysisLoad rc:%d\n", __func__, rc);
    }
    sqlite3_mutex_leave(sqlite3_db_mutex(thd->sqldb));
    if (got_curtran && put_curtran(thedb->bdb_env, clnt)) {
        logmsg(LOGMSG_ERROR, "%s failed to put_curtran\n", __func__);
    }
    return rc;
}

#define TRK \
    if (gbl_fdb_track) \
        logmsg(LOGMSG_USER, \
               "XXX: thd dbopen=%d vs %d thd analyze %d vs %d views %d vs %d\n", \
               thd->dbopen_gen, bdb_get_dbopen_gen(), thd->analyze_gen, \
               cached_analyze_gen, thd->views_gen, gbl_views_gen);

// Call with schema_lk held and in_sqlite_init == 1
static int check_thd_gen(struct sqlthdstate *thd, struct sqlclntstate *clnt, int flags)
{
    int allow_temp = flags & PREPARE_ALLOW_TEMP_DDL;
    int recreate = flags & PREPARE_RECREATE;
    if (!recreate && allow_temp) {
        /* Never stale to operate on sqlite_temp_master */
        return SQLITE_OK;
    }
    /* cache analyze gen first because gbl_analyze_gen is NOT protected by
     * schema_lk */
    int cached_analyze_gen = gbl_analyze_gen;

    if (thd->dbopen_gen != bdb_get_dbopen_gen()) {
        TRK;
        return SQLITE_SCHEMA;
    }
    if (thd->analyze_gen != cached_analyze_gen) {
        int ret;
        TRK;
        stmt_cache_reset(thd->stmt_cache);
        ret = reload_analyze(thd, clnt, cached_analyze_gen);
        return ret;
    }

    if (thd->views_gen != gbl_views_gen) {
        TRK;
        return SQLITE_SCHEMA_REMOTE;
    }

    return SQLITE_OK;
}

int release_locks_int(const char *trace, const char *func, int line, struct sqlclntstate *clnt)
{
    if (!clnt) {
        struct sql_thread *thd = pthread_getspecific(query_info_key);
        if (thd) clnt = thd->clnt;
    }
    if (!clnt || !clnt->dbtran.cursor_tran) return -1;
    extern int gbl_sql_release_locks_trace;
    if (gbl_sql_release_locks_trace) {
        logmsg(LOGMSG_USER, "Releasing locks for lockid %d, %s\n",
               bdb_get_lid_from_cursortran(clnt->dbtran.cursor_tran), trace);
    }
    return recover_deadlock_flags(thedb->bdb_env, clnt, NULL, -1, func, line, 0);
}

/* Release-locks if rep-thread is blocked longer than this many ms */
int gbl_rep_wait_release_ms = 60000;

int release_locks_on_emit_row(struct sqlclntstate *clnt)
{
    extern int gbl_locks_check_waiters;
    extern int gbl_sql_release_locks_on_emit_row;
    extern int gbl_rep_lock_time_ms;
    extern int gbl_sql_random_release_interval;
    int rep_lock_time_ms = gbl_rep_lock_time_ms;

    /* Always release if we're emitting during a master change */
    if (bdb_lock_desired(thedb->bdb_env))
        return release_locks_int("release locks on emit-row for lock-desired", __func__, __LINE__, clnt);

    /* Short circuit if check-waiters or tunable is disabled */
    if (!gbl_locks_check_waiters)
        return 0;

    if (!gbl_sql_release_locks_on_emit_row)
        return 0;

    /* Release locks randomly for testing */
    if (gbl_sql_random_release_interval &&
        !(rand() % gbl_sql_random_release_interval))
        return release_locks_int("random release emit-row", __func__, __LINE__, clnt);

    /* Short circuit if we don't have any waiters */
    if (!bdb_curtran_has_waiters(thedb->bdb_env, clnt->dbtran.cursor_tran))
        return 0;

    /* We're emitting a row & have waiters */
    if (!gbl_rep_wait_release_ms || thedb->master == gbl_myhostname)
        return release_locks_int("release locks on emit-row", __func__, __LINE__, clnt);

    /* We're emitting a row and are blocking replication */
    if (rep_lock_time_ms &&
        (comdb2_time_epochms() - rep_lock_time_ms) > gbl_rep_wait_release_ms)
        return release_locks_int("long repwait at emit-row", __func__, __LINE__, clnt);

    return 0;
}


void thr_set_current_sql(const char *sql)
{
    char *prevsql;
    pthread_once(&current_sql_query_once, init_current_current_sql_key);
    /* TODO: if we could put_ref at thread exit, we would be able to always keep a reference to sql in this variable at no cost */
    if (gbl_debug_temptables) {
        prevsql = pthread_getspecific(current_sql_query_key);
        if (prevsql) {
            free(prevsql);
            Pthread_setspecific(current_sql_query_key, NULL);
        }
        Pthread_setspecific(current_sql_query_key, strdup(sql));
    }
}

static void setup_reqlog(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    char info_nvreplays[40];
    info_nvreplays[0] = '\0';

    if (clnt->verify_retries)
        snprintf(info_nvreplays, sizeof(info_nvreplays), "vreplays=%d",
                 clnt->verify_retries);

    setup_client_info(clnt, thd, info_nvreplays);
    if(!clnt->sql_ref)
        clnt->sql_ref = create_string_ref(clnt->sql);
    reqlog_new_sql_request(thd->logger, clnt->sql_ref);
    log_context(clnt, thd->logger);
}

void query_stats_setup(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    /* debug */
    thr_set_current_sql(clnt->sql);

    /* debug */
    clnt->debug_sqlclntstate = pthread_self();

    clnt->nrows = 0;

    /* berkdb stats */
    bdb_reset_thread_stats();

    if (clnt->rawnodestats)
        clnt->rawnodestats->sql_queries++;

    /* sql thread stats */
    thd->sqlthd->startms = comdb2_time_epochms();
    thd->sqlthd->stime = comdb2_time_epoch();

    /* stats added to rawnodestats->sql_steps */
    reset_sql_steps(thd->sqlthd);

    /* reqlog */
    setup_reqlog(thd, clnt);

    /* using case sensitive like? enable */
    if (clnt->using_case_insensitive_like)
        toggle_case_sensitive_like(thd->sqldb, 1);

    if (gbl_dump_sql_dispatched)
        logmsg(LOGMSG_USER, "SQL mode=%d [%s]\n", clnt->dbtran.mode, clnt->sql);

    reqlog_set_clnt(thd->logger, clnt);
    reqlog_set_api_type(thd->logger, clnt->plugin.api_type(clnt));
}

int param_count(struct sqlclntstate *clnt)
{
    return clnt->plugin.param_count(clnt);
}

int param_index(struct sqlclntstate *clnt, const char *name, int64_t *index)
{
    return clnt->plugin.param_index(clnt, name, index);
}

int param_value(struct sqlclntstate *clnt, struct param_data *param, int n)
{
    return clnt->plugin.param_value(clnt, param, n);
}

int override_count(struct sqlclntstate *clnt)
{
    return clnt->plugin.override_count(clnt);
}

int override_type(struct sqlclntstate *clnt, int i)
{
    return clnt->plugin.override_type(clnt, i);
}


static void update_schema_remotes(struct sqlclntstate *clnt,
                                  struct sql_state *rec)
{
    /* reset set error since this is a retry */
    clnt->osql.error_is_remote = 0;
    clnt->osql.xerr.errval = 0;

    /* if I jump here because of sqlite3_step failure, I have local
     * cache already freed */
    sqlite3UnlockStmtTablesRemotes(clnt); /*lose all the locks boyo! */

    int bdberr = 0, rc = bdb_free_curtran_locks(thedb->bdb_env, clnt->dbtran.cursor_tran, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Failed to unlock curtran locks for %p, rc=%d bdberr=%d\n", clnt->dbtran.cursor_tran, rc,
               bdberr);
    }

    /* terminate the current statement; we are gonna reprepare */
    sqlite3_finalize(rec->stmt);
    rec->stmt = NULL;
}

static void _prepare_error(struct sqlthdstate *thd,
                                struct sqlclntstate *clnt,
                                struct sql_state *rec, int rc,
                                struct errstat *err)
{
    const char *errstr;

    if (rc == SQLITE_SCHEMA_DOHSQL)
        return;

    if (in_client_trans(clnt) &&
        (rec->status & CACHE_HAS_HINT ||
         has_sqlcache_hint(clnt->sql, NULL, NULL)) &&
        !(rec->status & CACHE_FOUND_STR) &&
        (clnt->osql.replay == OSQL_RETRY_NONE)) {

        errstr = (char *)sqlite3_errmsg(thd->sqldb);
        reqlog_logf(thd->logger, REQL_TRACE, "sqlite3_prepare failed %d: %s\n",
                    rc, errstr);
        errstat_set_rcstrf(err, ERR_PREPARE_RETRY, "%s", errstr);

        //srs_tran_del_last_query(clnt);
        return;
    }

    if(rc == ERR_SQL_PREPARE && !rec->stmt)
        errstr = "no statement";
    if(rc == SQLITE_SCHEMA && rec->stmt && clnt->remsql_set.is_remsql) {
        errstr = clnt->remsql_set.xerr.errstr;
    } else if (clnt->fdb_state.xerr.errval) {
        errstr = clnt->fdb_state.xerr.errstr;
    } else {
        errstr = (char *)sqlite3_errmsg(thd->sqldb);
    }
    reqlog_logf(thd->logger, REQL_TRACE, "sqlite3_prepare failed %d: %s\n", rc,
                errstr);
    errstat_set_rcstrf(err, ERR_PREPARE, "%s", errstr);
    if (clnt->saved_errstr) {
        free(clnt->saved_errstr);
    }
    clnt->saved_errstr = strdup(errstr);

    int ignoreErr = rec->prepFlags & PREPARE_IGNORE_ERR;
    if (!ignoreErr) clnt->had_errors = 1;

    if (gbl_print_syntax_err) {
        logmsg(LOGMSG_WARN, "sqlite3_prepare() failed for: %s [%s]\n", clnt->sql,
                errstr);
    }

    if (!ignoreErr && clnt->ctrl_sqlengine != SQLENG_NORMAL_PROCESS) {
        /* multiple query transaction
           keep sending back error */
        handle_sql_intrans_unrecoverable_error(clnt);
    }

    /* make sure this was not a delayed parsing error; sqlite finishes a
    statement even though there is trailing garbage, and report error
    afterwards. Clean any parallel distribution if any */
    if (!ignoreErr && unlikely(clnt->conns))
        dohsql_handle_delayed_syntax_error(clnt);
}

static int send_run_error(struct sqlclntstate *clnt, const char *err, int rc)
{
    Pthread_mutex_lock(&clnt->wait_mutex);
    clnt->ready_for_heartbeats = 0;
    Pthread_mutex_unlock(&clnt->wait_mutex);
    return write_response(clnt, RESPONSE_ERROR, (void *)err, rc);
}

static int handle_bad_engine(struct sqlclntstate *clnt)
{
    logmsg(LOGMSG_ERROR, "unable to obtain sql engine\n");
    send_run_error(clnt, "Client api should change nodes", CDB2ERR_CHANGENODE);
    clnt->query_rc = -1;
    return -1;
}

static int handle_bad_transaction_mode(struct sqlthdstate *thd,
                                       struct sqlclntstate *clnt)
{
    logmsg(LOGMSG_ERROR, "unable to set_transaction_mode\n");
    write_response(clnt, RESPONSE_ERROR_PREPARE, "Failed to set transaction mode", 0);
    reqlog_logf(thd->logger, REQL_TRACE, "Failed to set transaction mode\n");
    if (put_curtran(thedb->bdb_env, clnt)) {
        logmsg(LOGMSG_ERROR, "%s: unable to destroy a CURSOR transaction!\n",
               __func__);
    }
    clnt->query_rc = 0;
    clnt->osql.timings.query_finished = osql_log_time();
    osql_log_time_done(clnt);
    return -2;
}

static int prepare_engine(struct sqlthdstate *, struct sqlclntstate *, int);
int sqlengine_prepare_engine(struct sqlthdstate *thd,
                             struct sqlclntstate *clnt, int flags)
{
    clnt->in_sqlite_init = 1;
    int rc = prepare_engine(thd, clnt, flags);
    clnt->in_sqlite_init = 0;
    return rc;
}

static void free_normalized_sql(
  struct sqlclntstate *clnt
){
  if (clnt->work.zNormSql) {
    /* NOTE: Actual memory owned by SQLite, do not free. */
    clnt->work.zNormSql = 0;
  }
}

void free_original_normalized_sql(
  struct sqlclntstate *clnt
){
  if (clnt->work.zOrigNormSql) {
    free(clnt->work.zOrigNormSql);
    clnt->work.zOrigNormSql = 0;
  }
}

static void normalize_stmt_and_store(
  struct sqlclntstate *clnt,
  struct sql_state *rec,
  int iDefDqId /* Default return value for double-quote identifiers:
                *
                * A value of zero means that double-quoted strings should
                * always be treated as identifiers when there is no Vdbe
                * available.
                *
                * A value of non-zero means that double-quoted strings
                * should always be treated as string literals when there
                * is no Vdbe available.
                *
                * In general, this function is called from two primary
                * places: 1) prior to executing a SQL query, in order to
                * help calculate the fingerprint for use by the ruleset
                * engine. 2) after executing a SQL query, in order to
                * calculate the fingerprint (again) based on the prepared
                * statement.  For case 1), there will be no Vdbe, because
                * that work is performed on the AppSock thread, not a SQL
                * engine thread.
                */
){
  if (gbl_fingerprint_queries) {
    /*
    ** NOTE: Query fingerprints are enabled.  There are two cases where this
    **       function will be called:
    **
    **       1. From within the SQL query preparation pipeline (the function
    **          "get_prepared_stmt_int()" and/or its friends).  In this case,
    **          the "rec" pointer to this function will be non-NULL and have
    **          the actual SQLite prepared statement along with its SQL text.
    **
    **       2. From within the "non-SQL" request handling pipeline, which is
    **          currently limited to handling "EXEC PROCEDURE".  In this case,
    **          the "rec" pointer to this function will be NULL and the SQL
    **          text will be obtained directly from "clnt" instead.
    */
    if (rec != NULL) {
      assert(rec->stmt);
      assert(rec->sql);
      const char *zNormSql = sqlite3_normalized_sql(rec->stmt);
      if (zNormSql) {
        assert(clnt->work.zNormSql==0);
        clnt->work.zNormSql = zNormSql;
      } else if (gbl_verbose_normalized_queries) {
        logmsg(LOGMSG_USER, "FAILED sqlite3_normalized_sql({%s})\n", rec->sql);
      }
    } else {
      assert(clnt->sql);
      char *zOrigNormSql;
      if (gbl_alternate_normalize) {
        zOrigNormSql = sqlite3Normalize_alternate(0, clnt->sql, iDefDqId);
      } else {
        zOrigNormSql = sqlite3Normalize(0, clnt->sql, iDefDqId);
      }
      if (zOrigNormSql) {
        assert(clnt->work.zOrigNormSql==0);
        clnt->work.zOrigNormSql = strdup(zOrigNormSql);
        sqlite3_free(zOrigNormSql);
      } else if (gbl_verbose_normalized_queries) {
        logmsg(LOGMSG_USER, "FAILED sqlite3Normalize({%s})\n", clnt->sql);
      }
    }
  }
}

static struct fingerprint_track *prepare_fingerprint(struct sqlclntstate *clnt,
                                                     struct sql_state *rec,
                                                     unsigned char fingerprint[FINGERPRINTSZ],
                                                     int flags) {
    struct fingerprint_track *t = NULL;
    const char *zNormSql;
    size_t unused;

    if (!gbl_fingerprint_queries) {
        return NULL;
    }

    /* Generate normalized sql */
    if (!(flags & PREPARE_NO_NORMALIZE)) {
        free_normalized_sql(clnt);
        free_original_normalized_sql(clnt);
        normalize_stmt_and_store(clnt, rec, 0);
        zNormSql = clnt->work.zNormSql;
    } else {
        zNormSql = sqlite3_normalized_sql(rec->stmt);
    }

    /* Calculate fingerprint */
    calc_fingerprint(zNormSql, &unused, fingerprint);

    /* Store for virtual table use. */
    memcpy(clnt->work.aFingerprint, fingerprint, FINGERPRINTSZ);

    Pthread_mutex_lock(&gbl_fingerprint_hash_mu);
    if (gbl_fingerprint_hash) {
        t = hash_find(gbl_fingerprint_hash, fingerprint);
    }
    Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);

    return t;
}

/**
 * Get a sqlite engine, either from cache or building a new one
 * Locks tables to prevent any schema changes for them
 *
 */
static int get_prepared_stmt_int(struct sqlthdstate *thd,
                                 struct sqlclntstate *clnt,
                                 struct sql_state *rec, struct errstat *err,
                                 int flags)
{
    struct fingerprint_track *t = NULL;
    unsigned char fingerprint[FINGERPRINTSZ];

    int prepareOnly = (flags & PREPARE_ONLY);

    int rc = sqlengine_prepare_engine(thd, clnt, flags);

    if (thd->sqldb == NULL) {
        return handle_bad_engine(clnt);
    } else if (rc) {
        return rc;
    }
    if (sql_set_transaction_mode(thd->sqldb, clnt, clnt->dbtran.mode) != 0) {
        return handle_bad_transaction_mode(thd, clnt);
    }
    query_stats_setup(thd, clnt);
    stmt_cache_get(thd, clnt, rec, flags);
    int sqlPrepFlags = 0;

    if (gbl_fingerprint_queries)
        sqlPrepFlags |= SQLITE_PREPARE_NORMALIZE;

    if (prepareOnly || sqlite3_is_prepare_only(clnt))
        sqlPrepFlags |= SQLITE_PREPARE_ONLY;

    if (!gbl_allow_pragma)
        flags |= PREPARE_DENY_PRAGMA;

    flags |= PREPARE_DENY_CREATE_TRIGGER; /* UNSUPPORTED: was in is_stored_proc() */

    const char *tail = NULL;

    /* If we did not get a cached stmt, need to prepare it in sql engine */
    int startPrepMs = comdb2_time_epochms(); /* start of prepare phase */
    while (rec->stmt == NULL) {
        clnt->in_sqlite_init = 1;
        comdb2_set_authstate(thd, clnt, flags);
        rec->prepFlags = flags;

        clnt->prep_rc = rc = sqlite3_prepare_v3(thd->sqldb, rec->sql, -1,
                                                sqlPrepFlags, &rec->stmt, &tail);

        if (rc == SQLITE_OK && rec->stmt != NULL) {
            t = prepare_fingerprint(clnt, rec, fingerprint, flags);
        }

        /* Prepare the query with the query_preparer plugin. */
        if (rc == SQLITE_OK && gbl_old_column_names && rec->stmt &&
            !clnt->fdb_state.remote_sql_sb && query_preparer_plugin &&
            query_preparer_plugin->do_prepare &&
            sqlite3_stmt_readonly(rec->stmt) &&
            !sqlite3_stmt_isexplain(rec->stmt) &&
            (thd->authState.numDdls == 0)) {
            /* Prepare the query again with the *old sqlite version*, if
             * - there is no fingerprint, or
             * - it resulted in mismatching column name(s)/type(s) in past executions
             */
            if (!t || (t->typeMismatch || t->nameMismatch)) {
                char **column_names;
                char **column_decltypes;
                int column_count;
                rc = query_preparer_plugin->do_prepare(thd, clnt, rec->sql, &column_names, &column_decltypes, &column_count);
                if (rc) {
                    return rc;
                }
                if (rec->stmt) {
                    stmt_set_cached_columns(rec->stmt, column_names, column_decltypes, column_count);
                }
            }
        }

        if (rec->stmt) {
            stmt_set_vlock_tables(rec->stmt, thd->authState.vTableLocks, thd->authState.numVTableLocks,
                                  thd->authState.hasVTables, thd->authState.flags);
            thd->authState.numVTableLocks = 0;
            thd->authState.vTableLocks = NULL;
            thd->authState.hasVTables = 0;
        } else {
            for (int i = 0; i < thd->authState.numVTableLocks; i++) {
                free(thd->authState.vTableLocks[i]);
            }
            free(thd->authState.vTableLocks);
            thd->authState.numVTableLocks = 0;
            thd->authState.vTableLocks = NULL;
            thd->authState.hasVTables = 0;
        }

        thd->authState.flags = 0;
        clnt->in_sqlite_init = 0;
        if (rc == SQLITE_OK) {
            if (!prepareOnly) rc = sqlite3LockStmtTables(rec->stmt);
        } else if (rc == SQLITE_ERROR && comdb2_get_verify_remote_schemas()) {
            sqlite3ResetFdbSchemas(thd->sqldb);
            return SQLITE_SCHEMA_REMOTE;
        }
        if (rc != SQLITE_SCHEMA_REMOTE) {
            break;
        }
        sql_remote_schema_changed(clnt, rec->stmt);
        update_schema_remotes(clnt, rec);
    }

    if (rec->stmt) {
        thd->sqlthd->prepms = comdb2_time_epochms() - startPrepMs;
        if (!t) prepare_fingerprint(clnt, rec, fingerprint, flags);
        reqlog_set_fingerprint(thd->logger, (const char *)fingerprint, FINGERPRINTSZ);

        sqlite3_resetclock(rec->stmt);
        thr_set_current_sql(rec->sql);
    } else if (rc == 0) {
        // No stmt and no error -> Empty sql string or just comment.
        rc = ERR_SQL_PREPARE;
    }
    if (rc) {
        _prepare_error(thd, clnt, rec, rc, err);
    } else {
        clnt->verify_remote_schemas = 0;
    }
    return rc;
}

int get_prepared_stmt(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                      struct sql_state *rec, struct errstat *err, int flags)
{
    curtran_assert_nolocks();
    rdlock_schema_lk();
    int rc = get_prepared_stmt_int(thd, clnt, rec, err,
                                   flags | PREPARE_RECREATE);
    unlock_schema_lk();
    if (gbl_stable_rootpages_test) {
        static int skip = 0;
        if (!skip) {
            skip = 1;
            sleep(60);
        } else
            skip = 0;
    }
    return rc;
}

/* Only customer is creating and dropping temp-tables from stored procedures */
int get_prepared_stmt_no_lock(struct sqlthdstate *thd,
                               struct sqlclntstate *clnt, struct sql_state *rec,
                               struct errstat *err, int flags)
{
    return get_prepared_stmt_int(thd, clnt, rec, err, flags);
}

/*
** Only customer is stored-procedure.
** This prevents lock inversion between tbllk and schemalk.
*/
int get_prepared_stmt_try_lock(struct sqlthdstate *thd,
                               struct sqlclntstate *clnt, struct sql_state *rec,
                               struct errstat *err, int flags)
{
    if (tryrdlock_schema_lk() != 0) {
        // only schemachange will wrlock(schema)
        sql_debug_logf(clnt, __func__, __LINE__,
                       "Returning SQLITE_PERM on tryrdlock failure\n");
        return SQLITE_PERM;
    }
    int rc = get_prepared_stmt_int(thd, clnt, rec, err, flags);
    unlock_schema_lk();
    return rc;
}

int bind_parameters(struct reqlogger *logger, sqlite3_stmt *stmt, struct sqlclntstate *clnt, char **err,
                    int sample_queries)
{
    int rc = 0;
    int params = param_count(clnt);
    struct cson_array *arr = get_bind_array(logger, sample_queries);
    struct param_data p;
    char intspace[12]; // enough space to fit string representation of integer
    for (int i = 0; i < params; ++i) {
        memset(&p, 0, sizeof(p));
        if ((rc = param_value(clnt, &p, i)) != 0) {
            rc = SQLITE_ERROR;
            goto out;
        }
        if (!sample_queries && p.pos == 0) {
            p.pos = sqlite3_bind_parameter_index(stmt, p.name);
        }
        if (!sample_queries && p.pos == 0) {
            rc = SQLITE_ERROR;
            goto out;
        }

        char *name;
        if (strlen(p.name) <= 0) { // name is blank because this was from cdb2_bind_index()
            name = intspace;
            sprintf(name, "?%d", p.pos);
        } else
            name = p.name;

        if (p.null || p.type == COMDB2_NULL_TYPE) {
            if (!sample_queries)
                rc = sqlite3_bind_null(stmt, p.pos);
            eventlog_bind_null(arr, name);
            if (rc) { /* position out-of-bounds, etc? */
                goto out;
            }
            continue;
        }
        if (p.arraylen) {
            int flag;
            switch (p.type) {
            case CLIENT_REAL: flag = CARRAY_DOUBLE; break;
            case CLIENT_CSTR: /* fall-through */
            case CLIENT_VUTF8: flag = CARRAY_TEXT; break;
            case CLIENT_BLOB: flag = CARRAY_BLOB; break;
            case CLIENT_INT:
            if (p.len == sizeof(int32_t)) {
                flag = CARRAY_INT32;
                break;
            }
            if (p.len == sizeof(int64_t)) {
                flag = CARRAY_INT64;
                break;
            }
            /* fall-through */
            default:
                goto carray_err;
            }
            if (!sample_queries)
                rc = sqlite3_carray_bind(stmt, p.pos, p.u.p, p.arraylen, flag, SQLITE_STATIC);
            eventlog_bind_array(arr, name, p.u.p, p.arraylen, flag);
            if (rc) {
carray_err:     *err = sqlite3_mprintf("carray_bind: invalid param:%s count:%d type:%d size:%d\n",
                                       name, p.arraylen, p.type, p.len);
                return -1;
            }
            continue;
        }
        switch (p.type) {
        case CLIENT_INT:
        case CLIENT_UINT:
            if (!sample_queries)
                rc = sqlite3_bind_int64(stmt, p.pos, p.u.i);
            eventlog_bind_int64(arr, name, p.u.i, p.len);
            break;
        case CLIENT_REAL:
            if (!sample_queries)
                rc = sqlite3_bind_double(stmt, p.pos, p.u.r);
            eventlog_bind_double(arr, name, p.u.r, p.len);
            break;
        case CLIENT_CSTR:
        case CLIENT_PSTR:
        case CLIENT_PSTR2:
            /* Keeping with the R6 behavior, we need to 'silently' truncate
             * the string on first '\0'.
             */
            p.len = strnlen((char *)p.u.p, p.len);
            if (!sample_queries)
                rc = sqlite3_bind_text(stmt, p.pos, p.u.p, p.len, NULL);
            eventlog_bind_text(arr, name, p.u.p, p.len);
            break;
        case CLIENT_VUTF8: /* Unclear to me how sql client can bind a CLIENT_VUTF8 parameter */
            if (!sample_queries)
                rc = sqlite3_bind_text(stmt, p.pos, p.u.p, p.len, NULL);
            eventlog_bind_varchar(arr, name, p.u.p, p.len);
            break;
        case CLIENT_BLOB:
        case CLIENT_BYTEARRAY:
            if (!sample_queries)
                rc = sqlite3_bind_blob(stmt, p.pos, p.u.p, p.len, NULL);
            eventlog_bind_blob(arr, name, p.u.p, p.len);
            break;
        case CLIENT_DATETIME:
        case CLIENT_DATETIMEUS:
            if (!sample_queries)
                rc = sqlite3_bind_datetime(stmt, p.pos, &p.u.dt, clnt->tzname);
            eventlog_bind_datetime(arr, name, &p.u.dt, clnt->tzname);
            break;
        case CLIENT_INTVYM:
        case CLIENT_INTVDS:
        case CLIENT_INTVDSUS:
            if (!sample_queries)
                rc = sqlite3_bind_interval(stmt, p.pos, &p.u.tv);
            eventlog_bind_interval(arr, name, &p.u.tv);
            break;
        default:
            rc = SQLITE_ERROR;
            break;
        }
        if (rc) {
            goto out;
        }
    }
out:if (rc) {
        *err = sqlite3_mprintf("Bad parameter:%s type:%d\n", p.name, p.type);
    }
    return rc;
}

static int bind_params(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                       struct sql_state *rec, struct errstat *err)
{
    char *errstr = NULL;
    int rc = bind_parameters(thd->logger, rec->stmt, clnt, &errstr, 0);
    if (rc) {
        errstat_set_rcstrf(err, ERR_PREPARE, "%s", errstr);
    }
    return rc;
}

struct param_data *clnt_find_param(struct sqlclntstate *clnt, const char *name,
                                   int index)
{
    int params = param_count(clnt);
    int rc = 0;
    struct param_data *p = calloc(1, sizeof(struct param_data));
    if (!p)
        return NULL;

    for (int i = 0; i < params; ++i) {
        if ((rc = param_value(clnt, p, i)) != 0)
            goto done;

        if (p->pos > 0 && p->pos == index)
            return p;

        if (name[0] && !strncmp(name, p->name, strlen(name) + 1))
            return p;
    }
done:
    free(p);
    return NULL;
}

/**
 * Get a sqlite engine with bound parameters set, if any
 *
 */
static int get_prepared_bound_stmt(struct sqlthdstate *thd,
                                   struct sqlclntstate *clnt,
                                   struct sql_state *rec,
                                   struct errstat *err,
                                   int flags)
{
    int rc;
    if ((rc = get_prepared_stmt(thd, clnt, rec, err, flags)) != 0) {
        return rc;
    }

    int bind_cnt = sqlite3_bind_parameter_count(rec->stmt);
    int par_cnt = param_count(clnt);
    if (bind_cnt != par_cnt) {
        errstat_set_rcstrf(err, ERR_PREPARE,
                           "parameters in stmt:%d parameters provided:%d",
                           bind_cnt, par_cnt);
        return -1;
    }
    int cols = column_count(clnt, rec->stmt);
    int overrides = override_count(clnt);
    if (overrides && overrides != cols) {
        errstat_set_rcstrf(err, ERR_PREPARE,
                           "columns in stmt:%d column types provided:%d", cols,
                           overrides);
        return -1;
    }

    reqlog_logf(thd->logger, REQL_INFO, "ncols=%d", cols);
    if (bind_cnt == 0)
        return 0;

    return bind_params(thd, clnt, rec, err);
}

static void handle_stored_proc(struct sqlthdstate *, struct sqlclntstate *);

static void handle_expert_query(struct sqlthdstate *thd,
                                struct sqlclntstate *clnt, int *outrc)
{
    int rc;
    char *zErr = 0;

    *outrc = 0;
    rdlock_schema_lk();
    rc = sqlengine_prepare_engine(thd, clnt, PREPARE_RECREATE);
    unlock_schema_lk();
    if (thd->sqldb == NULL) {
        *outrc = handle_bad_engine(clnt);
        return;
    } else if (rc) {
        *outrc = rc;
        return;
    }

    comdb2_set_authstate(thd, clnt, PREPARE_RECREATE);
    rc = -1;
    sqlite3expert *p = sqlite3_expert_new(thd->sqldb, &zErr);

    if (p) {
        rc = sqlite3_expert_sql(p, clnt->sql, &zErr);
    }

    if (clnt->is_fast_expert)
        sqlite3_expert_config(p, EXPERT_CONFIG_SAMPLE, 0);

    if (rc == SQLITE_OK) {
        rc = sqlite3_expert_analyze(p, &zErr);
    }

    if (rc == SQLITE_OK) {
        const char *zCand =
            sqlite3_expert_report(p, 0, EXPERT_REPORT_CANDIDATES);
        fprintf(stdout, "-- Candidates -------------------------------\n");
        fprintf(stdout, "%s\n", zCand);
        write_response(clnt, RESPONSE_TRACE, "---------- Recommended Indexes --------------\n", 0);
        write_response(clnt, RESPONSE_TRACE, (void *)zCand, 0);
        write_response(clnt, RESPONSE_TRACE, "---------------------------------------------\n", 0);
    } else {
        fprintf(stderr, "Error: %s\n", zErr ? zErr : "?");
        write_response(clnt, RESPONSE_TRACE, zErr, 0);
    }
    write_response(clnt, RESPONSE_ROW_LAST_DUMMY, NULL, 0);
    write_response(clnt, RESPONSE_FLUSH, NULL, 0);
    sqlite3_expert_destroy(p);
    sqlite3_free(zErr);
    clnt->in_sqlite_init = 0;
    return; /* Don't process anything else */
}

/* return 0 continue, 1 return *outrc */
static int handle_non_sqlite_requests(struct sqlthdstate *thd,
                                      struct sqlclntstate *clnt, int *outrc)
{
    sql_update_usertran_state(clnt);

    switch (clnt->ctrl_sqlengine) {

    case SQLENG_PRE_STRT_STATE:
        reqlog_set_event(thd->logger, EV_SQL);
        *outrc = handle_sql_begin(thd, clnt, TRANS_CLNTCOMM_NORMAL);
        return 1;

    case SQLENG_WRONG_STATE:
        *outrc = handle_sql_wrongstate(thd, clnt);
        return 1;

    case SQLENG_FNSH_STATE:
    case SQLENG_FNSH_RBK_STATE:
        reqlog_set_event(thd->logger, EV_SQL);
        *outrc = handle_sql_commitrollback(thd, clnt, TRANS_CLNTCOMM_NORMAL);
        return 1;

    case SQLENG_NORMAL_PROCESS:
    case SQLENG_INTRANS_STATE:
    case SQLENG_STRT_STATE:
        /* FALL-THROUGH for update query execution */
        break;
    }

    if (is_stored_proc(clnt)) {
        handle_stored_proc(thd, clnt);
        *outrc = 0;
        return 1;
    } else if (clnt->is_explain) { // only via newsql--cdb2api
        rdlock_schema_lk();
        int rc = sqlengine_prepare_engine(thd, clnt, PREPARE_RECREATE);
        unlock_schema_lk();
        if (thd->sqldb == NULL) {
            *outrc = handle_bad_engine(clnt);
        } else if (!rc) {
            *outrc = newsql_dump_query_plan(clnt, thd->sqldb);
        } else {
            *outrc = rc;
        }
        return 1;
    } else if (clnt->is_expert) {
        handle_expert_query(thd, clnt, outrc);
        return 1;
    }

    /* 0, this is an sqlite request, use an engine */
    return 0;
}

static int skip_response_int(struct sqlclntstate *clnt, int from_error)
{
    if (clnt->osql.replay == OSQL_RETRY_DO || clnt->osql.replay == OSQL_RETRY_LAST)
        return 1;
    if (clnt->isselect || is_with_statement(clnt->sql))
        return 0;
    if (in_client_trans(clnt)) {
        if (from_error && !clnt->had_errors) /* send on first error */
            return 0;
        if (send_intrans_response(clnt)) {
            return 0;
        }
        return 1;
    }
    return 0; /* single stmt by itself (read or write) */
}

static int skip_response(struct sqlclntstate *clnt)
{
    return skip_response_int(clnt, 0);
}

static int skip_response_error(struct sqlclntstate *clnt)
{
    return skip_response_int(clnt, 1);
}

static int send_columns(struct sqlclntstate *clnt, struct sqlite3_stmt *stmt)
{
    if (clnt->osql.sent_column_data || skip_response(clnt))
        return 0;
    clnt->osql.sent_column_data = 1;
    return write_response(clnt, RESPONSE_COLUMNS, stmt, 0);
}

int send_row(struct sqlclntstate *clnt, struct sqlite3_stmt *stmt,
             uint64_t row_id, int postpone, struct errstat *err)
{
    if (skip_row(clnt, row_id))
        return 0;
    struct response_data arg = {0};
    arg.err = err;
    arg.stmt = stmt;
    arg.row_id = row_id;
    return write_response(clnt, RESPONSE_ROW, &arg, postpone);
}

/* will do a tiny cleanup of clnt */
void run_stmt_setup(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    Vdbe *v = (Vdbe *)stmt;
    clnt->isselect = sqlite3_stmt_readonly(stmt);
    /* TODO: we can be more precise and retry things at a later LSN so long as
     * nothing has overwritten the original readsets */
    if (clnt->isselect || is_with_statement(clnt->sql)) {
        set_sent_data_to_client(clnt, 1, __func__, __LINE__);
    }
    if (clnt->in_client_trans) {
        clnt->has_recording |= v->recording;
    } else {
        clnt->has_recording = v->recording;
    }
    clnt->nsteps = 0;
    comdb2_set_sqlite_vdbe_tzname_int(v, clnt);
    comdb2_set_sqlite_vdbe_dtprec_int(v, clnt);

#ifdef DEBUG
    if (gbl_debug_sql_opcodes) {
        fprintf(stderr, "%s () sql: '%s'\n", __func__, v->zSql);
    }
#endif
}

static int rc_sqlite_to_client(struct sqlthdstate *thd,
                               struct sqlclntstate *clnt,
                               struct sql_state *rec, char **perrstr)
{
    sqlite3_stmt *stmt = rec->stmt;
    int irc;

    /* get the engine error code, which is what we should pass
       back to the client!
     */
    irc = sql_check_errors(clnt, thd->sqldb, stmt, (const char **)perrstr);
    if (irc) {
        irc = sqlserver2sqlclient_error(irc);
    }

    if (!irc) {
        irc = errstat_get_rc(&clnt->osql.xerr);
        if (irc) {
            *perrstr = (char *)errstat_get_str(&clnt->osql.xerr);
            /* Do not retry on ERR_UNCOMMITTABLE_TXN. */
            if (irc == ERR_UNCOMMITTABLE_TXN) {
                osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_LAST);
            } else if ((irc == ERR_SC) && (*perrstr == NULL || (*perrstr)[0] == '\0')) {
                *perrstr = "a schema change error occurred";
            }
            /* convert this to a user code */
            irc = (clnt->osql.error_is_remote)
                      ? irc
                      : blockproc2sql_error(irc, __func__, __LINE__);
            if (replicant_can_retry_rc(clnt, irc) && !clnt->has_recording &&
                clnt->osql.replay == OSQL_RETRY_NONE) {
                osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_DO);
            }
        }
        /* if this is a single query, we need to send back an answer here */
        if (clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS) {
            /* if this is still a verify error but we tried to many times,
               send error back anyway by resetting the replay field */
            if (replicant_can_retry_rc(clnt, irc) &&
                clnt->osql.replay == OSQL_RETRY_LAST) {
                osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);
                if (gbl_dump_history_on_too_many_verify_errors) {
                    logmsg(LOGMSG_ERROR, "too many verify errors host=%s task=%s\n", clnt->origin, clnt->argv0);
                    srs_tran_print_history(clnt, 2);
                    logmsg(LOGMSG_WARN, "------\n");
                }
            }
            /* if this is still an error, but not verify, pass it back to
               client */
            else if (irc && !replicant_can_retry_rc(clnt, irc)) {
                osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);
            }
            /* if this is a successful retrial of a previous verified-failed
               query, reset here replay so we can send success message back
               to client */
            else if (!irc && clnt->osql.replay != OSQL_RETRY_NONE) {
                osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);
            }
        }
    } else if (clnt->osql.replay != OSQL_RETRY_NONE) {
        /* this was a retry that got an different error;
           send error to the client and stop retrying */
        osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_HALT);
    }

    return irc;
}

static int post_sqlite_processing(struct sqlthdstate *thd,
                                  struct sqlclntstate *clnt,
                                  struct sql_state *rec, int postponed_write,
                                  uint64_t row_id)
{
    test_no_btcursors(thd);
    if (clnt->client_understands_query_stats) {
        record_query_cost(thd->sqlthd, clnt);
        write_response(clnt, RESPONSE_COST, 0, 0);
    } else if (clnt->get_cost) {
        if (clnt->prev_cost_string) {
            free(clnt->prev_cost_string);
            clnt->prev_cost_string = NULL;
        }
        clnt->prev_cost_string = get_query_cost_as_string(thd->sqlthd, clnt);
    }
    char *errstr = NULL;
    int rc = rc_sqlite_to_client(thd, clnt, rec, &errstr);
    if (rc != 0) {
        if (!skip_response_error(clnt)) {
            send_run_error(clnt, errstr, rc);
        }
        clnt->had_errors = 1;
    } else {
        Pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;
        Pthread_mutex_unlock(&clnt->wait_mutex);
        if (!skip_response(clnt)) {
            if (postponed_write)
                send_row(clnt, NULL, row_id, 0, NULL);
            write_response(clnt, RESPONSE_EFFECTS, 0, 1);
            write_response(clnt, RESPONSE_ROW_LAST, 0, 0);
        }
    }
    return 0;
}

/* The design choice here for communication is to send row data inside this
   function, and delegate the error sending to the caller (since we send
   multiple rows, but we send error only once and stop processing at that time)
 */
static int run_stmt(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                    struct sql_state *rec, int *fast_error, struct errstat *err)
{
    int rc, t_rc;
    uint64_t row_id = 0;
    int rowcount = 0;
    int postponed_write = 0;
    sqlite3_stmt *stmt = rec->stmt;

    run_stmt_setup(clnt, stmt);
    if ((gbl_typessql || clnt->typessql) && clnt->isselect && !dohsql_is_parallel_shard() && !clnt->fdb_push &&
        !((Vdbe *)stmt)->hasVTables && !((Vdbe *)stmt)->hasScalarFunc)
        typessql_initialize(clnt, stmt);

    /* this is a regular sql query, add it to history */
    if (srs_tran_add_query(clnt))
        logmsg(LOGMSG_ERROR,
               "Fail to add query to transaction replay session\n");

    /* Get first row to figure out column structure */
    clnt->last_sent_row_sec = time(NULL);
    int steprc = next_row(clnt, stmt);
    if (steprc == SQLITE_SCHEMA_REMOTE) {
        /* remote schema changed;
           Only safe to recover here
           NOTE: not a fast error;
         */
        return steprc;
    }

    *fast_error = 1;

    if (clnt->verify_indexes && steprc == SQLITE_ROW) {
        clnt->has_sqliterow = 1;
        return verify_indexes_column_value(clnt, stmt, clnt->schema_mems);
    } else if (clnt->verify_indexes && steprc == SQLITE_DONE) {
        clnt->has_sqliterow = 0;
        return 0;
    }

    if ((rc = validate_columns(clnt, stmt)) != 0) {
        send_run_error(clnt,
                       "NEXTSEQUENCE function is not legal in this context",
                       CDB2ERR_PREPARE_ERROR);
        return rc;
    }

    if ((rc = send_columns(clnt, stmt)) != 0) {
        return rc;
    }

    if (clnt->intrans == 0) {
        reset_query_effects(clnt);
    }

    /* no rows actually ? */
    if (steprc != SQLITE_ROW) {
        rc = steprc;
        goto postprocessing;
    }

    do {
        clnt->last_sent_row_sec = time(NULL);

        /* replication contention reduction */
        rc = release_locks_on_emit_row(clnt);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: release_locks_on_emit_row failed\n",
                   __func__);
            return rc;
        }

        if (clnt->isselect == 1) {
            clnt->effects.num_selected++;
            clnt->log_effects.num_selected++;
            clnt->nrows++;
        }

        /* return row, if needed */
        if ((clnt->isselect && clnt->osql.replay != OSQL_RETRY_DO) ||
            ((Vdbe *)stmt)->explain) {
            postponed_write = 0;
            ++row_id;
            rc = send_row(clnt, stmt, row_id, 0, err);
            if (rc)
                return rc;
        } else {
            postponed_write = 1;
            send_row(clnt, stmt, row_id, 1, NULL);
        }

        rowcount++;
        reqlog_set_rows(thd->logger, rowcount);
        clnt->recno++;
        if (clnt->rawnodestats)
            clnt->rawnodestats->sql_rows++;

    } while ((rc = next_row(clnt, stmt)) == SQLITE_ROW);

    /* whatever sqlite returns in sqlite3_step is only used to step out of the
     * loop, otherwise ignored; we are gonna get it from sqlite (or osql.xerr)
     */

postprocessing:
    /* if we get this message, it means we had to stop the sqlite early
       and we must reset the state */
    if (rc == SQLITE_EARLYSTOP_DOHSQL)
        sqlite3_reset(stmt);
    if (rc == SQLITE_DONE || rc == SQLITE_OK) /* good rcodes */
        rc = 0;
    /* closing: error codes, postponed write result and so on*/
    t_rc = post_sqlite_processing(thd, clnt, rec, postponed_write, row_id);
    if (t_rc != 0 && rc == 0)
        rc = t_rc;

    return rc;
}

static void handle_sqlite_error(struct sqlthdstate *thd,
                                struct sqlclntstate *clnt,
                                struct sql_state *rec, int rc)
{
    reqlog_set_error(thd->logger, sqlite3_errmsg(thd->sqldb), rc);

    if (clnt->conns) {
        dohsql_signal_done(clnt);
    }

    if (thd->sqlthd)
        reset_sql_steps(thd->sqlthd);

    clnt->had_errors = 1;

    if (clnt->using_case_insensitive_like)
        toggle_case_sensitive_like(thd->sqldb, 0);
}

static void sqlite_done(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                        struct sql_state *rec, int outrc)
{
    sqlite3_stmt *stmt = rec->stmt;
    int distributed = 0;

    if (clnt->conns) {
        dohsql_end_distribute(clnt, thd->logger);
        distributed = 1;
    }
    if (clnt->fdb_push)
        fdb_push_free(&clnt->fdb_push);
    if (clnt->typessql_state)
        typessql_end(clnt);

    sql_statement_done(thd->sqlthd, thd->logger, clnt, stmt, outrc);

    if (stmt && !((Vdbe *)stmt)->explain && ((Vdbe *)stmt)->nScan > 1 &&
        (BDB_ATTR_GET(thedb->bdb_attr, PLANNER_WARN_ON_DISCREPANCY) == 1 ||
         BDB_ATTR_GET(thedb->bdb_attr, PLANNER_SHOW_SCANSTATS) == 1)) {
        compare_estimate_cost(stmt);
    }

    stmt_cache_put_distributed(thd, clnt, rec, outrc, distributed);

    if (clnt->using_case_insensitive_like)
        toggle_case_sensitive_like(thd->sqldb, 0);

    /* the ethereal sqlite objects insert into clnt->...Ddl fields
       we need to clear them out after the statement is done, or else
       the next read in sqlite master will find them and try to use them
     */
    clearClientSideRow(clnt);
}

static void handle_stored_proc(struct sqlthdstate *thd,
                               struct sqlclntstate *clnt)
{
    struct sql_state rec = {0};
    char *errstr = NULL;
    query_stats_setup(thd, clnt);
    reqlog_set_event(thd->logger, EV_SP);
    clnt->dbtran.trans_has_sp = 1;

    /*
    ** NOTE: The "EXEC PROCEDURE" command cannot be prepared
    **       because its execution bypasses the SQL engine;
    **       however, the parser now recognizes it and so it
    **       can be normalized.
    */
    free_normalized_sql(clnt);
    free_original_normalized_sql(clnt);
    normalize_stmt_and_store(clnt, NULL, 1);

    memset(&clnt->spcost, 0, sizeof(struct sql_hist_cost));
    int rc = exec_procedure(thd, clnt, &errstr);
    if (rc) {
        if (!errstr) {
            logmsg(LOGMSG_USER, "handle_stored_proc: error occured, rc = %d\n",
                   rc);
            errstr = strdup("Error occured");
        }
        clnt->had_errors = 1;
        if (rc == -1)
            rc = -3;
        write_response(clnt, RESPONSE_ERROR, errstr, rc);
        free(errstr);
    }
    if (!in_client_trans(clnt))
        clnt->dbtran.trans_has_sp = 0;
    test_no_btcursors(thd);
    if (clnt->work.zOrigNormSql) {
        size_t nOrigNormSql = 0;

        // calculate aFingerprint here since exec_procedure will update aFingerprint to last sql stmt found in stored
        // procedure
        calc_fingerprint(clnt->work.zOrigNormSql, &nOrigNormSql, clnt->work.aFingerprint);
    }
    sqlite_done(thd, clnt, &rec, 0);
}

static inline void post_run_reqlog(struct sqlthdstate *thd,
                                   struct sqlclntstate *clnt,
                                   struct sql_state *rec)
{
    reqlog_set_event(thd->logger, EV_SQL);
    log_queue_time(thd->logger, clnt);
}

int handle_sqlite_requests(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    int rc;
    struct errstat err = {0};
    struct sql_state rec = {0};
    rec.sql = clnt->sql;
    char *allocd_str = NULL;

    do {
retry_legacy_remote:
        /* clean old stats */
        clear_cost(thd->sqlthd);

        /* get an sqlite engine */
        assert(rec.stmt == NULL);
        rc = get_prepared_bound_stmt(thd, clnt, &rec, &err, PREPARE_NONE);
        if (rc == SQLITE_SCHEMA_REMOTE)
            continue;
        if (rc == SQLITE_SCHEMA_DOHSQL) {
            if (allocd_str)
                free(allocd_str);
            allocd_str = strdup(dohsql_get_sql(clnt, 0));
            rec.sql = (const char *)allocd_str;
            continue;
        }
        if (rc == SQLITE_SCHEMA_PUSH_REMOTE) {
            rc = handle_fdb_push(clnt, &err);
            if (rc == -2) {
                /* remote server does not support proxy, retry without */
                clnt->disable_fdb_push = 1;
                goto retry_legacy_remote;
            }
            goto done;
        }

        if (rc) {
            int irc = errstat_get_rc(&err);
            /* certain errors are saved, in that case we don't send anything */
            if (irc == ERR_PREPARE) {
                write_response(clnt, RESPONSE_ERROR_PREPARE, err.errstr, 0);
                handle_sqlite_error(thd, clnt, &rec, rc);
            } else if (irc == ERR_PREPARE_RETRY) {
                write_response(clnt, RESPONSE_ERROR_PREPARE_RETRY, err.errstr, 0);
                rc = 0;
            } else {
                handle_sqlite_error(thd, clnt, &rec, rc);
            }
            break;
        }

        if (clnt->statement_query_effects)
            reset_query_effects(clnt);

        int fast_error = 0;

        /* run the engine */
        rc = run_stmt(thd, clnt, &rec, &fast_error, &err);
        if (rc) {
            int irc = errstat_get_rc(&err);
            switch(irc) {
            case ERR_ROW_HEADER:
            case ERR_CONVERSION_DT:
                send_run_error(clnt, errstat_get_str(&err), CDB2ERR_CONV_FAIL);
                break;
            }
            if (fast_error) {
                handle_sqlite_error(thd, clnt, &rec, rc);
                break;
            }
        }

        if (rc == SQLITE_SCHEMA_REMOTE) {
            update_schema_remotes(clnt, &rec);
        }

    } while (rc == SQLITE_SCHEMA_REMOTE || rc == SQLITE_SCHEMA_DOHSQL);

    /* set these after sending response so client gets results a bit sooner */
    post_run_reqlog(thd, clnt, &rec);

done:
    sqlite_done(thd, clnt, &rec, rc);

    if (allocd_str)
        free(allocd_str);
    return rc;
}

/* SHARD
    int irc = 0;
    if(clnt->shard_slice<=1) {
        if (clnt->is_newsql) {
            if (!rc && (clnt->num_retry == clnt->sql_query->retry) &&
                    (clnt->num_retry == 0 || clnt->sql_query->has_skip_rows == 0
|| (clnt->sql_query->skip_rows < row_id))) irc = send_row_new(thd, clnt, ncols,
row_id, columns); } else { irc = send_row_old(thd, clnt, new_row_data_type);
        }
    }

    if(1) {
       if(clnt->conns && clnt->conns_idx == 1) {
         shard_flush_conns(clnt, 0);
       }
    }
    .....
        // if parallel, drain the shards
    if (clnt->conns && clnt->conns_idx == 1) {
        shard_flush_conns(clnt, 1);

    ......

#ifdef DEBUG
        int hasn;
        if (!(hasn = sqlite3_hasNColumns(stmt, ncols))) {
            printf("Does not have %d cols\n", ncols);
            abort();
        }
#endif

        // create return row
        rc = make_retrow(thd, clnt, rec, new_row_data_type, ncols, rowcount,
                         fast_error, err);
        if (rc)
            goto out;

        int sz = clnt->sql_query->cnonce.len;
        char cnonce[256];
        cnonce[0] = '\0';

        if (gbl_extended_sql_debug_trace) {
            bzero(cnonce, sizeof(cnonce));
            snprintf(cnonce, 256, "%s", clnt->sql_query->cnonce.data);
            logmsg(LOGMSG_USER, "%s: cnonce '%s': iswrite=%d replay=%d "
                    "want_query_effects is %d, isselect is %d\n",
                    __func__, cnonce, clnt->iswrite, clnt->osql.replay,
                    clnt->want_query_effects,
                    clnt->isselect);
        }

        // return row, if needed
        if (!clnt->iswrite && clnt->osql.replay != OSQL_RETRY_DO) {
            postponed_write = 0;
            row_id++;

            if (!clnt->want_query_effects || clnt->isselect) {
                if(comm->send_row_data) {
                    if (gbl_extended_sql_debug_trace) {
                        logmsg(LOGMSG_USER, "%s: cnonce '%s' sending row\n",
__func__, cnonce);
                    }
                    rc = comm->send_row_data(thd, clnt, new_row_data_type,
                                             ncols, row_id, rc, columns);
                    if (rc)
                        goto out;
                }
            }
        } else {
            if (gbl_extended_sql_debug_trace) {
                logmsg(LOGMSG_USER, "%s: cnonce '%s' setting postponed_write\n",
                        __func__, cnonce);
            }
            postponed_write = 1;
        }

        rowcount++;
        reqlog_set_rows(thd->logger, rowcount);
        clnt->recno++;
        if (clnt->rawnodestats)
            clnt->rawnodestats->sql_rows++;

        // flush
        if(comm->flush && !clnt->iswrite) {
            rc = comm->flush(clnt);
            if (rc)
                goto out;
        }
    } while ((rc = sqlite3_step(stmt)) == SQLITE_ROW);

// whatever sqlite returns in sqlite3_step is only used to step out of the loop,
 //  otherwise ignored; we are gonna
 //  get it from sqlite (or osql.xerr)

postprocessing:
    if (rc == SQLITE_DONE)
        rc = 0;

    // closing: error codes, postponed write result and so on
    rc =
        post_sqlite_processing(thd, clnt, rec, postponed_write, ncols, row_id,
                               columns, comm);

out:
    newsql_dealloc_row(columns, ncols);
    return rc;

  ....


  int handle_sqlite_requests(struct sqlthdstate *thd,
                           struct sqlclntstate *clnt,
                           struct client_comm_if *comm)
{
    struct sql_state rec;
    int rc;
    int fast_error;
    struct errstat err = {0};

    bzero(&rec, sizeof(rec));

    // loop if possible in case when cached remote schema becomes stale
    do {
        // get an sqlite engine
        rc = get_prepared_bound_stmt(thd, clnt, &rec, &err, PREPARE_NONE);
        if (rc) {
            int irc = errstat_get_rc(&err);
            // certain errors are saved, in that case we don't send anything
            if(irc == ERR_PREPARE || irc == ERR_PREPARE_RETRY)
                if(comm->send_prepare_error)
                    comm->send_prepare_error(clnt, err.errstr,
                                             (irc == ERR_PREPARE_RETRY));
            goto errors;
        }

        // run the engine
        fast_error = 0;
        rc = run_stmt(thd, clnt, &rec, &fast_error, &err, comm);
        if (rc) {
            int irc = errstat_get_rc(&err);
            switch(irc) {
                case ERR_ROW_HEADER:
                    if(comm->send_run_error)
                        comm->send_run_error(clnt, errstat_get_str(&err),
                                             CDB2ERR_CONV_FAIL);
                    break;
                case ERR_CONVERSION_DT:
                    if(comm->send_run_error)
                        comm->send_run_error(clnt, errstat_get_str(&err),
                                             DB_ERR_CONV_FAIL);
                    break;
            }
            if (fast_error)
                goto errors;
        }

        if (rc == SQLITE_SCHEMA_REMOTE) {
            update_schema_remotes(clnt, &rec);
        }

    } while (rc == SQLITE_SCHEMA_REMOTE);

done:
    sqlite_done(thd, clnt, &rec, rc);

    return rc;

errors:
    handle_sqlite_error(thd, clnt, &rec);
    goto done;
}

*/

static int check_done_func(void *obj)
{
    struct sqlclntstate *clnt = (struct sqlclntstate *)obj;
    /* Don't find the client which we already killed */
    if (clnt->done && !in_client_trans(clnt) && !clnt->statement_timedout) {
        return 0;
    }
    return -1;
}

/**
 * Main driver of SQL processing, for both sqlite and non-sqlite requests
 */
static int execute_sql_query(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    int outrc = 0;
    int rc;

#ifdef DEBUG
    logmsg(LOGMSG_DEBUG, "execute_sql_query: '%.30s'\n", clnt->sql);
#endif

    /* access control */
    rc = check_sql_access(thd, clnt);
    if (rc)
        return rc;

    /* is this a snapshot? special processing */
    rc = get_high_availability(clnt);
    if (rc) {
        logmsg(LOGMSG_DEBUG, "ha_retrieve_snapshot() returned rc=%d\n", rc);
        return 0;
    }

    /* All requests that do not require a sqlite engine are processed next,
     * rc != 0 means processing done */
    if ((rc = handle_non_sqlite_requests(thd, clnt, &outrc)) != 0) {
        return outrc;
    }

    /* This is a request that requires a sqlite engine */
    return handle_sqlite_requests(thd, clnt);
}

// call with schema_lk held + in_sqlite_init
static int prepare_engine(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                          int flags)
{
    int recreate = flags & PREPARE_RECREATE;

    struct errstat xerr;
    int rc = 0;
    int got_views_lock = 0;
    int got_curtran = 0;

    /* Do this here, before setting up Btree structures!
       so we can get back at our "session" information */
    clnt->debug_sqlclntstate = pthread_self();
    struct sql_thread *sqlthd;
    if ((sqlthd = pthread_getspecific(query_info_key)) != NULL) {
        sqlthd->clnt = clnt;
    }

check_version:
    if (thd->sqldb && (rc = check_thd_gen(thd, clnt, flags)) != SQLITE_OK) {
        if (rc != SQLITE_SCHEMA_REMOTE) {
            if (!recreate) {
                goto done;
            }
            stmt_cache_reset(thd->stmt_cache);
            sqlite3_close_serial(&thd->sqldb);

            /* Force sqlitex to recreate db handle. */
            if (gbl_old_column_names && query_preparer_plugin &&
                query_preparer_plugin->do_cleanup_thd) {
                query_preparer_plugin->do_cleanup_thd(thd);
            }
        }
    }
    assert(!thd->sqldb || rc == SQLITE_OK || rc == SQLITE_SCHEMA_REMOTE);

    if (gbl_enable_sql_stmt_caching && (thd->stmt_cache == NULL)) {
        thd->stmt_cache = stmt_cache_new(NULL);
    }

    if (!thd->sqldb || (rc == SQLITE_SCHEMA_REMOTE)) {
        /* need to refresh things; we need to grab views lock */
        if (!got_views_lock) {
            unlock_schema_lk();

            if (!clnt->dbtran.cursor_tran) {
                int ctrc = get_curtran(thedb->bdb_env, clnt);
                if (ctrc) {
                    logmsg(LOGMSG_ERROR, "%s td %p: unable to get a CURSOR transaction, rc = %d!\n", __func__,
                           (void *)pthread_self(), ctrc);
                    if (thd->sqldb) {
                        stmt_cache_reset(thd->stmt_cache);
                        sqlite3_close_serial(&thd->sqldb);
                    }
                    rdlock_schema_lk();
                    return ctrc;
                } else {
                    got_curtran = 1;
                }
            }

            rdlock_schema_lk();

            views_lock();
            got_views_lock = 1;
            if (thd->sqldb) {
                /* we kept engine, but the versions might have changed while
                 * we released the schema lock */
                goto check_version;
            }
        }

        if (!thd->sqldb) {
            /* cache analyze gen first because gbl_analyze_gen is NOT protected
             * by schema_lk */
            thd->analyze_gen = gbl_analyze_gen;
            int rc = sqlite3_open_serial("db", &thd->sqldb, thd);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "%s:sqlite3_open_serial failed %d: %s\n", __func__,
                       rc, sqlite3_errmsg(thd->sqldb));
                sqlite3_close_serial(&thd->sqldb);
                /* there is no really way forward, grab core */
                abort();
            }
            thd->dbopen_gen = bdb_get_dbopen_gen();
        }

        comdb2_reset_authstate(thd);
        get_copy_rootpages_nolock(thd->sqlthd);
        if (clnt->dbtran.cursor_tran) {
            if (thedb->timepart_views) {
                /* how about we are gonna add the views ? */
                rc = views_sqlite_update(thedb->timepart_views, thd->sqldb,
                                         &xerr, 0);
                if (rc != VIEW_NOERR) {
                    logmsg(LOGMSG_FATAL,
                           "failed to create views rc=%d errstr=\"%s\"\n",
                           xerr.errval, xerr.errstr);
                    /* there is no really way forward */
                    abort();
                }
            }

            /* save the views generation number */
            thd->views_gen = gbl_views_gen;
        }
    }
 done: /* reached via goto for error handling case. */
    if (got_views_lock) {
        views_unlock();
    }

    if (got_curtran && put_curtran(thedb->bdb_env, clnt)) {
        logmsg(LOGMSG_ERROR, "%s: unable to destroy a CURSOR transaction!\n",
               __func__);
    }

    return rc;
}

int done_cb_evbuffer(struct sqlclntstate *clnt)
{
    if (clnt->query_rc == CDB2ERR_IO_ERROR) { /* dispatch timed out */
        return -1;
    }
    if (clnt->osql.replay == OSQL_RETRY_DO) {
        plugin_func *save_cb = clnt->done_cb;
        clnt->done_cb = NULL;
        int rc  = srs_tran_replay_inline(clnt);
        if (rc && !clnt->query_rc) {
            clnt->query_rc = rc;
        }
        clnt->done_cb = save_cb;
    } else if (clnt->osql.history && clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS) {
        srs_tran_destroy(clnt);
    }
    Pthread_mutex_lock(&lru_evbuffers_mtx); /* protect log_long_running_stmts_evbuffer() */
    clnt->sql = NULL;
    clnt->done = 1;
    Pthread_mutex_unlock(&lru_evbuffers_mtx);
    if (!clnt->query_rc) {
        return 0;
    }
    if (in_client_trans(clnt)) {
        clnt->had_errors = 1;
        return 0;
    }
#   if 0
    /* sbuf2 clients drop connection here - connections are expensive */
    if (appdata->want_query_effects) {
        return 0;
    }
    return -1;
#   else
    reset_clnt_flags(clnt);
    return 0;
#   endif
}

void signal_clnt_as_done(struct sqlclntstate *clnt)
{
    struct sql_thread *thd = (clnt->thd && clnt->thd->sqlthd) ? clnt->thd->sqlthd : NULL;

    /* Clear the client from the sql thread, so that sql-dump won't see it. */
    if (thd) {
        Pthread_mutex_lock(&gbl_sql_lock);
        thd->clnt = NULL;
        Pthread_mutex_unlock(&gbl_sql_lock);
    }

    if (clnt->done_cb) {
        clnt->done_cb(clnt); /* newsql_done_cb */
    } else {
        Pthread_mutex_lock(&clnt->wait_mutex);
        clnt->done = 1;
        Pthread_cond_signal(&clnt->wait_cond);
        Pthread_mutex_unlock(&clnt->wait_mutex);
    }
}

void thr_set_user(const char *label, intptr_t id)
{
    char thdinfo[40];
    snprintf(thdinfo, sizeof(thdinfo), "appsock %" PRIxPTR, id);
    thrman_setid(thrman_self(), thdinfo);
}

static void debug_close_clnt(struct sqlclntstate *clnt)
{
    static int once = 0;

    if (debug_switch_sql_close_sbuf()) {
        if (!once) {
            once = 1;
            clnt->plugin.close(clnt);
        }
    } else
        once = 0;
}

static void sqlengine_work_lua_thread(void *thddata, void *work)
{
    struct sqlthdstate *thd = thddata;
    struct sqlclntstate *clnt = work;
    int rc;

    if (!clnt->exec_lua_thread)
        abort();

    thr_set_user("appsock", (intptr_t)clnt->appsock_id);

    clnt->osql.timings.query_dispatched = osql_log_time();
    clnt->deque_timeus = comdb2_time_epochus();
    clnt->thd = thd;
    /* Reset the cancel-statement flag */
    thd->sqlthd->stop_this_statement = 0;
    sql_update_usertran_state(clnt);

    rdlock_schema_lk();
    rc = sqlengine_prepare_engine(thd, clnt, PREPARE_RECREATE);
    unlock_schema_lk();

    if (thd->sqldb == NULL || rc) {
        handle_bad_engine(clnt);
        return;
    }

    reqlog_set_origin(thd->logger, "%s", clnt->origin);

    exec_thread(thd, clnt);

    clnt->osql.timings.query_finished = osql_log_time();
    osql_log_time_done(clnt);

    debug_close_clnt(clnt);
    signal_clnt_as_done(clnt);

    thrman_setid(thrman_self(), "[done]");
}

int gbl_debug_sqlthd_failures;
int gbl_enable_internal_sql_stmt_caching = 1;

static int execute_verify_indexes(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    int rc;
    stmt_cache_entry_t *cached_entry = NULL;
    if (thd->sqldb == NULL) {
        /* open sqlite db without copying rootpages */
        rc = sqlite3_open_serial("db", &thd->sqldb, thd);
        if (unlikely(rc != 0)) {
            logmsg(LOGMSG_ERROR, "%s:sqlite3_open_serial failed %d: %s\n",
                   __func__, rc, sqlite3_errmsg(thd->sqldb));
            sqlite3_close_serial(&thd->sqldb);
        } else {
            /* setting gen to -1 so real SQLs will reopen vm */
            thd->dbopen_gen = -1;
            thd->analyze_gen = -1;
        }
    }

    sqlite3_stmt *stmt = NULL;
    const char *tail;

    if (gbl_enable_internal_sql_stmt_caching) {
        if (thd->stmt_cache == NULL) {
            thd->stmt_cache = stmt_cache_new(NULL);
        }

        if ((stmt_cache_find_and_remove_entry(thd->stmt_cache, clnt->sql, &cached_entry)) == 0) {
            stmt = cached_entry->stmt;
        }
    }

    if (!stmt) {
        clnt->prep_rc = rc =
            sqlite3_prepare_v2(thd->sqldb, clnt->sql, -1, &stmt, &tail);
        if (rc != SQLITE_OK) {
            return rc;
        }
    }

    bind_verify_indexes_query(stmt, clnt->schema_mems);
    run_stmt_setup(clnt, stmt);
    if ((clnt->step_rc = rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        clnt->has_sqliterow = 1;
        rc = verify_indexes_column_value(clnt, stmt, clnt->schema_mems);
        if (gbl_enable_internal_sql_stmt_caching) {
            if (cached_entry)
                stmt_cache_requeue_old_entry(thd->stmt_cache, cached_entry);
            else
                stmt_cache_add_new_entry(thd->stmt_cache, clnt->sql, 0, stmt, clnt);
        } else {
            sqlite3_finalize(stmt);
        }
        return rc;
    }

    if (gbl_enable_internal_sql_stmt_caching) {
        if (cached_entry)
            stmt_cache_requeue_old_entry(thd->stmt_cache, cached_entry);
        else
            stmt_cache_add_new_entry(thd->stmt_cache, clnt->sql, 0, stmt, clnt);
    } else {
        sqlite3_finalize(stmt);
    }

    clnt->has_sqliterow = 0;
    if (rc == SQLITE_DONE) {
        return 0;
    }
    return rc;
}

static int preview_and_calc_fingerprint(struct sqlclntstate *clnt)
{
    if (is_transaction_meta(clnt)) {
        /*
        ** NOTE: The BEGIN, COMMIT, and ROLLBACK SQL (meta-)commands
        **       do not go through the SQLite parser (i.e. they are
        **       processed out-of-band).  Therefore, they are exempt
        **       from fingerprinting.
        */
        return 0; /* success */
    } else {
        /*
        ** NOTE: The "EXEC PROCEDURE" command cannot be prepared
        **       because its execution bypasses the SQL engine;
        **       however, the parser now recognizes it and so it
        **       can be normalized.  Since the "EXEC PROCEDURE"
        **       commands are never prepared, any double-quoted
        **       strings they may contain are always treated as
        **       literals, not quoted identifiers.  All other
        **       SQL commands will treat their double-quoted
        **       strings as quoted identifiers here, by design.
        **       This is safe because the caller(s) will enforce
        **       that the "strict_double_quotes" tunable is on
        **       prior to calling into this function on any SQL
        **       query that is not an "EXEC", "BEGIN", "COMMIT",
        **       or "ROLLBACK".
        */
        free_normalized_sql(clnt);
        free_original_normalized_sql(clnt);
        normalize_stmt_and_store(clnt, NULL, is_stored_proc_sql(clnt->sql));

        if (clnt->work.zOrigNormSql) {
            size_t nOrigNormSql = 0;

            calc_fingerprint(clnt->work.zOrigNormSql, &nOrigNormSql,
                             clnt->work.aFingerprint);
        }

        return 0; /* success */
    }
}

void clnt_to_ruleset_item_criteria(
  struct sqlclntstate *clnt,            /* in */
  struct ruleset_item_criteria *context /* out */
){
  if ((clnt == NULL) || (context == NULL)) return;
  context->zOriginHost = clnt->origin_host;
  context->zOriginTask = clnt->conninfo.pename;
  context->zUser =
      clnt->current_user.have_name ? clnt->current_user.name : NULL;
  context->zSql = clnt->sql;
  context->pFingerprint = clnt->work.aFingerprint;
}

static int can_execute_sql_query_now(
  struct sqlthdstate *thd,
  struct sqlclntstate *clnt,
  int *pRuleNo,
  int *pbRejected,
  int *pbTryAgain
){
  struct ruleset_item_criteria context = {0};
  struct ruleset_result result = {0};
  clnt->pPool = NULL; /* NOTE: By default, start with the "default" pool. */
  clnt_to_ruleset_item_criteria(clnt, &context);
  size_t count = comdb2_evaluate_ruleset(NULL, gbl_ruleset, &context, &result);
  comdb2_ruleset_result_to_str(
    &result, clnt->work.zRuleRes, sizeof(clnt->work.zRuleRes)
  );
  if (gbl_verbose_prioritize_queries) {
    logmsg(LOGMSG_INFO, "%s: PRE count=%d, sql={%s}, %s\n",
           __func__, (int)count, clnt->sql, clnt->work.zRuleRes);
  }
  *pRuleNo = -1; /* no rule was specifically responsible */
  *pbRejected = 0; /* initially, SQL work item is always allowed */
  /* BEGIN FAULT INJECTION TEST CODE */
  if ((result.action != RULESET_A_REJECT) && /* skip already adverse actions */
      (result.action != RULESET_A_REJECT_ALL)) {
    if (gbl_random_sql_work_rejected &&
        !(rand() % gbl_random_sql_work_rejected)) {
      logmsg(LOGMSG_WARN,
             "%s: POST forcing random SQL work item {%s} reject\n",
             __func__, clnt->sql);
      *pbRejected = 1;
      *pbTryAgain = 0;
      return 0;
    } else if (gbl_random_sql_work_delayed &&
        !(rand() % gbl_random_sql_work_delayed)) {
      logmsg(LOGMSG_WARN,
             "%s: POST forcing random SQL work item {%s} delay\n",
             __func__, clnt->sql);
      return 0;
    }
  }
  /* END FAULT INJECTION TEST CODE */
  struct thdpool *pool = get_sql_pool(clnt);
  switch (result.action) {
    case RULESET_A_NONE: {
      /* do nothing */
      break;
    }
    case RULESET_A_REJECT: {
      *pRuleNo = result.ruleNo;
      *pbRejected = 1;
      *pbTryAgain = 1;
      return 0;
    }
    case RULESET_A_REJECT_ALL: {
      *pRuleNo = result.ruleNo;
      *pbRejected = 1;
      *pbTryAgain = 0;
      return 0;
    }
    case RULESET_A_UNREJECT: {
      *pRuleNo = result.ruleNo;
      break;
    }
    case RULESET_A_SET_POOL: {
      *pRuleNo = result.ruleNo;
      if (result.zPool != NULL) {
        pool = get_named_sql_pool(
            result.zPool, result.flags & RULESET_F_DYN_POOL,
            SQL_POOL_NAMED_MAX_THREADS
        );
        if (pool != NULL) {
          clnt->pPool = pool;
        } else {
          logmsg(LOGMSG_ERROR,
                 "%s: missing named pool '%s' for ruleset 0x%p\n",
                 __func__, result.zPool, gbl_ruleset);
        }
      }
      break;
    }
    default: {
      logmsg(LOGMSG_ERROR,
             "%s: unsupported action 0x%x for ruleset 0x%p\n",
             __func__, result.action, gbl_ruleset);
      break;
    }
  }
  if (gbl_verbose_prioritize_queries) {
    logmsg(LOGMSG_INFO,
      "%s: POST count=%d, sql={%s}, pool={%s}\n",
      __func__, (int)count, clnt->sql, thdpool_get_name(pool));
  }
  return 1;
}

void sqlengine_work_appsock(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    struct sql_thread *sqlthd = thd->sqlthd;

    assert(sqlthd);
    sqlthd->clnt = clnt;
    clnt->thd = thd;
    /* Reset the cancel-statement flag */
    sqlthd->stop_this_statement = 0;

    thr_set_user("appsock", (intptr_t)clnt->appsock_id);

    clnt->added_to_hist = clnt->isselect = 0;
    clnt_change_state(clnt, CONNECTION_RUNNING);
    clnt->osql.timings.query_dispatched = osql_log_time();
    clnt->deque_timeus = comdb2_time_epochus();

    reqlog_set_origin(thd->logger, "%s", clnt->origin);

    if (clnt->dbtran.mode == TRANLEVEL_SOSQL &&
        clnt->client_understands_query_stats && clnt->osql.rqid)
        osql_query_dbglog(sqlthd, clnt->queryid);

    assert(clnt->dbtran.pStmt == NULL);

    /* everything going in is cursor based */
    int rc = get_curtran(thedb->bdb_env, clnt);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s td %p: unable to get a CURSOR transaction, rc=%d!\n", __func__, (void *)pthread_self(),
               rc);
        send_run_error(clnt, "Client api should change nodes",
                       CDB2ERR_CHANGENODE);
        clnt->query_rc = -1;
        clnt->osql.timings.query_finished = osql_log_time();
        osql_log_time_done(clnt);
        clnt_change_state(clnt, CONNECTION_IDLE);
        signal_clnt_as_done(clnt);
        return;
    }

    /* it is a new query, it is time to clean the error */
    if (clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS)
        bzero(&clnt->osql.xerr, sizeof(clnt->osql.xerr));

    if (clnt->ctrl_sqlengine == SQLENG_STRT_STATE ||
        clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS) {
        clnt->had_lease_at_begin = (thedb->master == gbl_myhostname)
                                       ? 1
                                       : bdb_valid_lease(thedb->bdb_env);
    }

    /* assign this query a unique id */
    sql_get_query_id(sqlthd);

    /* actually execute the query */
    thrman_setfd(thd->thr_self, get_fileno(clnt));

    osql_shadtbl_begin_query(thedb->bdb_env, clnt);

    if (clnt->fdb_state.remote_sql_sb) {
        clnt->query_rc = execute_sql_query_offload(thd, clnt);
    } else if (clnt->verify_indexes) {
        clnt->query_rc = execute_verify_indexes(thd, clnt);
    } else {
        clnt->query_rc = execute_sql_query(thd, clnt);
    }

    if (clnt->sql_ref) {
        put_ref(&clnt->sql_ref);
    }

    osql_shadtbl_done_query(thedb->bdb_env, clnt);
    thrman_setfd(thd->thr_self, -1);

    /* this is a compromise; we release the curtran here, even though
       we might have a begin/commit transaction pending
       any query inside the begin/commit will be performed under its
       own locker id;
    */
    if (put_curtran(thedb->bdb_env, clnt)) {
        logmsg(LOGMSG_ERROR, "%s: unable to destroy a CURSOR transaction!\n",
                __func__);
    }
    clnt->osql.timings.query_finished = osql_log_time();
    osql_log_time_done(clnt);
    clnt_change_state(clnt, CONNECTION_IDLE);
    debug_close_clnt(clnt);
    signal_clnt_as_done(clnt);

    thrman_setid(thrman_self(), "[done]");
}

static void sqlengine_work_appsock_pp(struct thdpool *pool, void *work,
                                      void *thddata, int op)
{
    struct sqlclntstate *clnt = work;

    switch (op) {
    case THD_RUN:
        if (clnt->exec_lua_thread)
            sqlengine_work_lua_thread(thddata, work);
        else
            sqlengine_work_appsock(thddata, work);
        break;
    case THD_FREE:
        /* we just mark the client done here, with error */
        clnt->query_rc = CDB2ERR_IO_ERROR;
        signal_clnt_as_done(clnt);
        break;
    }
    bdb_temp_table_maybe_reset_priority_thread(thedb->bdb_env, 1);
}

static int send_heartbeat(struct sqlclntstate *clnt)
{
    /* if client didnt ask for heartbeats, dont send them */
    if (!clnt->heartbeat)
        return 0;

    if (!clnt->ready_for_heartbeats) {
        return 0;
    }

    write_response(clnt, RESPONSE_HEARTBEAT, 0, 0);

    return 0;
}

/* timeradd() for struct timespec*/
#define TIMESPEC_ADD(a, b, result)                                             \
    do {                                                                       \
        (result).tv_sec = (a).tv_sec + (b).tv_sec;                             \
        (result).tv_nsec = (a).tv_nsec + (b).tv_nsec;                          \
        if ((result).tv_nsec >= 1000000000) {                                  \
            ++(result).tv_sec;                                                 \
            (result).tv_nsec -= 1000000000;                                    \
        }                                                                      \
    } while (0)

/* timersub() for struct timespec*/
#define TIMESPEC_SUB(a, b, result)                                             \
    do {                                                                       \
        (result).tv_sec = (a).tv_sec - (b).tv_sec;                             \
        (result).tv_nsec = (a).tv_nsec - (b).tv_nsec;                          \
        if ((result).tv_nsec < 0) {                                            \
            --(result).tv_sec;                                                 \
            (result).tv_nsec += 1000000000;                                    \
        }                                                                      \
    } while (0)

static int enqueue_sql_query(struct sqlclntstate *clnt)
{
    char msg[1024];
    int rc;
    int fail_dispatch = 0;
    int q_depth_tag_and_sql;

    struct thr_handle *self = thrman_self();
    if (self) {
        if (clnt->exec_lua_thread)
            thrman_set_subtype(self, THRSUBTYPE_LUA_SQL);
        else
            thrman_set_subtype(self, THRSUBTYPE_TOPLEVEL_SQL);
    }

    bzero(&clnt->osql.timings, sizeof(osqltimings_t));
    bzero(&clnt->osql.fdbtimes, sizeof(fdbtimings_t));
    clnt->osql.timings.query_received = osql_log_time();

    Pthread_mutex_lock(&clnt->wait_mutex);
    clnt->deadlock_recovered = 0;

    /* Reset clnt->thd: there's no guarantee that this clnt is going to be
       dispatched to the same sql thread it previously ran under; Also, that
       thread may not even exist any more (eg aged out), causing
       reqlog_long_running_clnt() to segfault when reading clnt->thd, which is
       alloca'd on the stack by the sql thread */
    clnt->thd = NULL;
    clnt->done = 0;
    if (clnt->statement_timedout) fail_dispatch = 1;
    clnt->total_sql++;
    clnt->sql_since_reset++;

    /* keep track so we can display it in stat thr */
    clnt->appsock_id = getarchtid();
    if (!clnt->sql_ref && clnt->sql) {
        clnt->sql_ref = create_string_ref(clnt->sql);
    }

    Pthread_mutex_unlock(&clnt->wait_mutex);

    if (fail_dispatch)
        return -1;

    struct thdpool *pool = get_sql_pool(clnt);
    clnt->enque_timeus = comdb2_time_epochus();

    if (gbl_verbose_normalized_queries) {
        snprintf(msg, sizeof(msg), "%s {%s} ==> pool {%s} at %llu",
                 clnt->origin, clnt->sql, (pool != NULL) ? thdpool_get_name(pool) :
                 "<null>", (long long unsigned int)clnt->enque_timeus);

        logmsg(LOGMSG_DEBUG, "%s: %s\n", __func__, msg);
    }

    q_depth_tag_and_sql = thd_queue_depth();
    /* If all threads are busy, the request itself will likely be queued. Count it in. */
    if (thdpool_get_nbusythds(pool) == thdpool_get_maxthds(pool))
        q_depth_tag_and_sql += thdpool_get_queue_depth(pool) + 1;

    time_metric_add(thedb->concurrent_queries, thdpool_get_nbusythds(pool));
    time_metric_add(thedb->queue_depth, q_depth_tag_and_sql);

    assert(clnt->dbtran.pStmt == NULL);
    uint32_t flags = (clnt->admin ? THDPOOL_FORCE_DISPATCH : 0);
    if (gbl_thdpool_queue_only) {
        flags |= THDPOOL_QUEUE_ONLY;
    }
    if ((thedb->nsiblings <= 1) && (clnt->queue_me == 0)) {
        /*
        ** NOTE: For a single-node cluster, always attempt to enter the queue
        **       if needed, because we know there are no other nodes available
        **       that can service requests immediately (i.e. without queueing).
        */
        clnt->queue_me = 1;
    }

    struct string_ref *sr = get_ref(clnt->sql_ref);
    if ((rc = thdpool_enqueue(pool, sqlengine_work_appsock_pp,
                              clnt, clnt->queue_me, sr, flags)) != 0) {
        if ((in_client_trans(clnt) || clnt->osql.replay == OSQL_RETRY_DO) &&
            gbl_requeue_on_tran_dispatch) {
            /* force this request to queue */
            rc = thdpool_enqueue(pool,
                                 sqlengine_work_appsock_pp, clnt, 1, sr,
                                 flags | THDPOOL_FORCE_QUEUE);
        }

        if (rc) {
            logmsg(LOGMSG_DEBUG, "%s: failed to enqueue: %s\n", __func__, string_ref_cstr(clnt->sql_ref));
            put_ref(&sr); // failed to enqueue so we still own this reference
            /* say something back, if the client expects it */
            if (clnt->fail_dispatch) {
                snprintf(msg, sizeof(msg), "%s: unable to dispatch sql query, rc=%d\n",
                         __func__, rc);
                handle_failed_dispatch(clnt, msg);
            }
        }
    }
    return rc;
}

static int wait_for_sql_query(struct sqlclntstate *clnt)
{
    /* successful dispatch or queueing, enable heartbeats */
    Pthread_mutex_lock(&clnt->wait_mutex);
    if (clnt->exec_lua_thread)
        clnt->ready_for_heartbeats = 0;
    else
        clnt->ready_for_heartbeats = 1;
    Pthread_mutex_unlock(&clnt->wait_mutex);

    /* SQL thread will unlock mutex when it is done, allowing us to lock it
     * again.  We block until then. */
    struct thr_handle *self = thrman_self();

    if (self)
        thrman_where(self, "waiting for query");

    if (clnt->heartbeat) {
        if (clnt->osql.replay != OSQL_RETRY_NONE || in_client_trans(clnt)) {
            send_heartbeat(clnt);
        }
        struct timespec mshb;
        struct timespec first, last;
        clock_gettime(CLOCK_REALTIME, &first);
        last = first;
        while (1) {
            struct timespec now, st;
            clock_gettime(CLOCK_REALTIME, &now);
            mshb.tv_sec = (gbl_client_heartbeat_ms / 1000);
            mshb.tv_nsec = (gbl_client_heartbeat_ms % 1000) * 1000000;
            TIMESPEC_ADD(now, mshb, st);

            Pthread_mutex_lock(&clnt->wait_mutex);
            if (clnt->done) {
                Pthread_mutex_unlock(&clnt->wait_mutex);
                goto done;
            }
            int rc;
            rc = pthread_cond_timedwait(&clnt->wait_cond, &clnt->wait_mutex,
                                        &st);
            if (clnt->done) {
                Pthread_mutex_unlock(&clnt->wait_mutex);
                goto done;
            }
            if (rc == ETIMEDOUT) {
                struct timespec diff;
                TIMESPEC_SUB(st, last, diff);
                if (diff.tv_sec >= clnt->heartbeat) {
                    last = st;
                    send_heartbeat(clnt);
                }
                if (clnt->query_timeout > 0 && !clnt->statement_timedout) {
                    TIMESPEC_SUB(st, first, diff);
                    if (diff.tv_sec >= clnt->query_timeout) {
                        clnt->statement_timedout = 1;
                        logmsg(LOGMSG_WARN,
                                "%s:%d Query exceeds max allowed time %d.\n",
                                __FILE__, __LINE__, clnt->query_timeout);
                    }
                }
            } else if (rc) {
                logmsg(LOGMSG_FATAL,
                        "%s:%d pthread_cond_timedwait rc %d", __FILE__,
                        __LINE__, rc);
                exit(1);
            }
            clnt->plugin.flush(clnt);
            Pthread_mutex_unlock(&clnt->wait_mutex);
        }
    } else {
        Pthread_mutex_lock(&clnt->wait_mutex);
        while (!clnt->done) {
            Pthread_cond_wait(&clnt->wait_cond, &clnt->wait_mutex);
        }
        Pthread_mutex_unlock(&clnt->wait_mutex);
    }

done:
    if (self)
        thrman_where(self, "query done");
    return clnt->query_rc;
}

static int verify_dispatch_sql_query(struct sqlclntstate *clnt)
{
    memset(clnt->work.zRuleRes, 0, sizeof(clnt->work.zRuleRes));

    if (clnt->admin || !gbl_prioritize_queries || !gbl_ruleset) {
        return 0;
    }

    if (gbl_fingerprint_queries &&
        comdb2_ruleset_fingerprints_allowed()) {
        /* IGNORED */
        preview_and_calc_fingerprint(clnt);
    }

    int ruleNo = 0;
    int bRejected = 0;
    int bTryAgain = 0;

    int ret = can_execute_sql_query_now(clnt->thd, clnt, &ruleNo, &bRejected, &bTryAgain);
    if (ret || !bRejected) {
        return 0;
    }

    int rc = bTryAgain ? CDB2ERR_REJECTED: ERR_QUERY_REJECTED;
    char zRuleRes[100];
    memset(zRuleRes, 0, sizeof(zRuleRes));
    snprintf0(zRuleRes, sizeof(zRuleRes), "Rejected due to rule #%d", ruleNo);
    if (gbl_verbose_prioritize_queries) {
        logmsg(LOGMSG_ERROR, "%s: REJECTED rc=%d {%s}: %s\n",
               __func__, rc, clnt->sql, zRuleRes);
    }
    send_run_error(clnt, zRuleRes, rc);
    return rc;
}

static int dispatch_sql_query_int(struct sqlclntstate *clnt)
{
    int rc = verify_dispatch_sql_query(clnt);
    if (rc != 0) {
        return rc;
    }
    return enqueue_sql_query(clnt);
}

int dispatch_sql_query(struct sqlclntstate *clnt)
{
    int rc = dispatch_sql_query_int(clnt);
    return rc ? rc : wait_for_sql_query(clnt);
}

int dispatch_sql_query_no_wait(struct sqlclntstate *clnt)
{
    return dispatch_sql_query_int(clnt);
}

int tdef_to_tranlevel(int tdef)
{
    switch (tdef) {
    case SQL_TDEF_COMDB2:
    case SQL_TDEF_BLOCK:
    case SQL_TDEF_SOCK:
        return TRANLEVEL_SOSQL;

    case SQL_TDEF_RECOM:
        return TRANLEVEL_RECOM;

    case SQL_TDEF_SERIAL:
        return TRANLEVEL_SERIAL;

    case SQL_TDEF_SNAPISOL:
        return gbl_use_modsnap_for_snapshot ?
               TRANLEVEL_MODSNAP : TRANLEVEL_SNAPISOL;

    default:
        logmsg(LOGMSG_FATAL, "%s: line %d Unknown modedef: %d", __func__, __LINE__,
                tdef);
        abort();
    }
}

void free_client_info(struct sqlclntstate *clnt)
{
    if (clnt->argv0) {
        free(clnt->argv0);
        clnt->argv0 = NULL;
    }
    if (clnt->stack) {
        free(clnt->stack);
        clnt->stack = NULL;
    }
    if (clnt->api_driver_name){
        free(clnt->api_driver_name);
        clnt->api_driver_name = NULL;
    }
    if (clnt->api_driver_version){
        free(clnt->api_driver_version);
        clnt->api_driver_version = NULL;
    }
}

void cleanup_clnt(struct sqlclntstate *clnt)
{
    if (clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
        handle_sql_intrans_unrecoverable_error(clnt);
    }
    if (clnt->rawnodestats) {
        release_node_stats(clnt->argv0, clnt->stack, clnt->origin);
        clnt->rawnodestats = NULL;
    }
    close_sp(clnt);
    osql_clean_sqlclntstate(clnt);
    clnt_try_enable_logdel(clnt);
    if (clnt->dbglog) {
        sbuf2close(clnt->dbglog);
        clnt->dbglog = NULL;
    }
    if (clnt->saved_errstr) {
        free(clnt->saved_errstr);
        clnt->saved_errstr = NULL;
    }
    if (clnt->dist_txnid) {
        free(clnt->dist_txnid);
        clnt->dist_txnid = NULL;
        clnt->dist_timestamp = 0;
    }
    if (clnt->coordinator_dbname) {
        free(clnt->coordinator_dbname);
        clnt->coordinator_dbname = NULL;
    }
    if (clnt->coordinator_tier) {
        free(clnt->coordinator_tier);
        clnt->coordinator_tier = NULL;
    }
    if (clnt->coordinator_master) {
        free(clnt->coordinator_master);
        clnt->coordinator_master = NULL;
    }

    clnt->sqlite_errstr = NULL;
    if (clnt->context) {
        for (int i = 0; i < clnt->ncontext; i++) {
            free(clnt->context[i]);
        }
        free(clnt->context);
        clnt->context = NULL;
        clnt->ncontext = 0;
    }

    if (clnt->pTemporal[0].pFrom)
        free(clnt->pTemporal[0].pFrom);
    clnt->pTemporal[0].pFrom = NULL;
    if (clnt->pTemporal[0].pTo)
        free(clnt->pTemporal[0].pTo);
    clnt->pTemporal[0].pTo = NULL;
    clnt->pTemporal[0].iIncl = 0;
    clnt->pTemporal[0].iAll = 0;
    clnt->pTemporal[0].iBus = 0;
    if (clnt->pTemporal[1].pFrom)
        free(clnt->pTemporal[1].pFrom);
    clnt->pTemporal[1].pFrom = NULL;
    if (clnt->pTemporal[1].pTo)
        free(clnt->pTemporal[1].pTo);
    clnt->pTemporal[1].pTo = NULL;
    clnt->pTemporal[1].iIncl = 0;
    clnt->pTemporal[1].iAll = 0;
    clnt->pTemporal[1].iBus = 0;

    clnt->pTemporalParser = NULL;
    
    if (clnt->selectv_arr) {
        currangearr_free(clnt->selectv_arr);
        clnt->selectv_arr = NULL;
    }
    if (clnt->arr) {
        currangearr_free(clnt->arr);
        clnt->arr = NULL;
    }
    if (clnt->spversion.version_str) {
        free(clnt->spversion.version_str);
        clnt->spversion.version_str = NULL;
    }
    if (clnt->query_stats) {
        free(clnt->query_stats);
        clnt->query_stats = NULL;
    }
    if (clnt->sql_ref) {
        put_ref(&clnt->sql_ref);
    }
    if (gbl_expressions_indexes) {
        if (clnt->idxInsert)
            free(clnt->idxInsert);
        if (clnt->idxDelete)
            free(clnt->idxDelete);
        clnt->idxInsert = clnt->idxDelete = NULL;
    }
    free_client_info(clnt);
    free_normalized_sql(clnt);
    free_original_normalized_sql(clnt);
    memset(&clnt->work.rec, 0, sizeof(struct sql_state));
    memset(clnt->work.aFingerprint, 0, FINGERPRINTSZ);

    clear_session_tbls(clnt);
    free(clnt->authdata);
    clnt->authdata = NULL;

    destroy_hash(clnt->ddl_tables, free_it);
    destroy_hash(clnt->dml_tables, free_it);
    clnt->ddl_tables = NULL;
    clnt->dml_tables = NULL;
    destroy_hash(clnt->ddl_contexts, free_clnt_ddl_context);
    clnt->ddl_contexts = NULL;

    Pthread_mutex_destroy(&clnt->wait_mutex);
    Pthread_cond_destroy(&clnt->wait_cond);
    Pthread_mutex_destroy(&clnt->write_lock);
    Pthread_cond_destroy(&clnt->write_cond);
    Pthread_mutex_destroy(&clnt->dtran_mtx);
    Pthread_mutex_destroy(&clnt->state_lk);
    Pthread_mutex_destroy(&clnt->sql_tick_lk);
    Pthread_mutex_destroy(&clnt->sql_lk);
}

int gbl_unexpected_last_type_warn = 1;
int gbl_unexpected_last_type_abort = 0;

void reset_clnt(struct sqlclntstate *clnt, int initial)
{
    if (initial) {
        bzero(clnt, sizeof(*clnt));
        Pthread_mutex_init(&clnt->wait_mutex, NULL);
        Pthread_cond_init(&clnt->wait_cond, NULL);
        Pthread_mutex_init(&clnt->write_lock, NULL);
        Pthread_cond_init(&clnt->write_cond, NULL);
        Pthread_mutex_init(&clnt->dtran_mtx, NULL);
        Pthread_mutex_init(&clnt->state_lk, NULL);
        Pthread_mutex_init(&clnt->sql_tick_lk, NULL);
        Pthread_mutex_init(&clnt->sql_lk, NULL);
        TAILQ_INIT(&clnt->session_tbls);
        listc_init(&clnt->participants, offsetof(struct participant, linkv));
    } else {
        clnt->sql_since_reset = 0;
        clnt->num_resets++;
        clnt->last_reset_time = comdb2_time_epoch();
        clnt_change_state(clnt, CONNECTION_RESET);
        if (clnt->lastresptype != RESPONSE_TYPE__LAST_ROW && clnt->lastresptype != 0) {
            if (gbl_unexpected_last_type_warn)
                logmsg(LOGMSG_ERROR, "Unexpected previous response type %d origin %s task %s\n", clnt->lastresptype,
                       clnt->origin, clnt->argv0);
            if (gbl_unexpected_last_type_abort)
                abort();
        }
    }

    clnt->pPool = NULL; /* REDUNDANT? */

    clnt_try_enable_logdel(clnt);

    if (clnt->rawnodestats) {
        release_node_stats(clnt->argv0, clnt->stack, clnt->origin);
        clnt->rawnodestats = NULL;
    }
    clnt->recno = 1;
    clnt->done = 1;
    strcpy(clnt->tzname, "America/New_York");
    clnt->dtprec = gbl_datetime_precision;
    bzero(&clnt->conninfo, sizeof(clnt->conninfo));
    clnt->using_case_insensitive_like = 0;

    if (clnt->ctrl_sqlengine != SQLENG_INTRANS_STATE)
        clnt->intrans = 0;

    /* start off in comdb2 mode till we're told otherwise */
    clnt->dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
    clnt->dbtran.nchunks = 0;
    clnt->heartbeat = 0;
    clnt->limits.maxcost = gbl_querylimits_maxcost;
    clnt->limits.tablescans_ok = gbl_querylimits_tablescans_ok;
    clnt->limits.temptables_ok = gbl_querylimits_temptables_ok;
    clnt->limits.maxcost_warn = gbl_querylimits_maxcost_warn;
    clnt->limits.tablescans_warn = gbl_querylimits_tablescans_warn;
    clnt->limits.temptables_warn = gbl_querylimits_temptables_warn;

    reset_query_effects(clnt);

    reset_user(clnt);

    /* reset authentication status */
    clnt->authgen = 0;

    clnt->prepare_only = 0;
    clnt->is_readonly = 0;
    clnt->is_readonly_set = 0;
    clnt->admin = 0;

    /* reset page-order. */
    clnt->pageordertablescan =
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_PAGE_ORDER_TABLESCAN);

    /* let's reset osql structure as well */
    osql_clean_sqlclntstate(clnt);
    /* clear dbtran after aborting unfinished shadow transactions. */
    bzero(&clnt->dbtran, sizeof(dbtran_type));

    clnt->dbtran.crtchunksize = clnt->dbtran.maxchunksize = 0;
    clnt->in_client_trans = 0;
    clnt->had_errors = 0;
    clnt->statement_timedout = 0;
    clnt->query_timeout = 0;
    if (clnt->saved_errstr) {
        free(clnt->saved_errstr);
        clnt->saved_errstr = NULL;
    }
    clnt->saved_rc = 0;
    clnt->sqlite_errstr = NULL;
    clnt->want_stored_procedure_debug = 0;
    clnt->want_stored_procedure_trace = 0;
    clnt->verifyretry_off = 0;
    clnt->is_expert = 0;
    clnt->is_fast_expert = 0;

    /* Reset the version, we have to set it for every run */
    clnt->spname[0] = 0;
    clnt->spversion.version_num = 0;
    free(clnt->spversion.version_str);
    clnt->spversion.version_str = NULL;

    clnt->is_explain = 0;
    clnt->get_cost = 0;
    clnt->snapshot = 0;
    clnt->num_retry = 0;
    clnt->early_retry = 0;

    clnt->use_2pc = 0;
    clnt->is_coordinator = 0;
    clnt->is_participant = 0;
    clear_participants(clnt);
    if (clnt->dist_txnid) {
        free(clnt->dist_txnid);
        clnt->dist_txnid = NULL;
        clnt->dist_timestamp = 0;
    }

    clear_session_tbls(clnt);

    clnt->planner_effort = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_PLANNER_EFFORT);
    clnt->osql_max_trans = g_osql_max_trans;
    clnt->group_concat_mem_limit = gbl_group_concat_mem_limit;

    free_client_info(clnt);
    free_normalized_sql(clnt);
    free_original_normalized_sql(clnt);
    memset(&clnt->work.rec, 0, sizeof(struct sql_state));
    memset(clnt->work.aFingerprint, 0, FINGERPRINTSZ);

    clnt->arr = NULL;
    clnt->selectv_arr = NULL;
    clnt->file = 0;
    clnt->offset = 0;
    clnt->enque_timeus = clnt->deque_timeus = 0;
    reset_clnt_flags(clnt);

    clnt->ins_keys = 0ULL;
    clnt->del_keys = 0ULL;
    clnt->has_sqliterow = 0;
    clnt->verify_indexes = 0;
    clnt->schema_mems = NULL;
    clnt->init_gen = 0;
    for (int i = 0; i < clnt->ncontext; i++) {
        free(clnt->context[i]);
    }
    free(clnt->context);
    if (clnt->authdata) {
        free(clnt->authdata);
        clnt->authdata = NULL;
    }
    clnt->context = NULL;
    clnt->ncontext = 0;

    if (clnt->pTemporal[0].pFrom)
        free(clnt->pTemporal[0].pFrom);
    clnt->pTemporal[0].pFrom = NULL;
    if (clnt->pTemporal[0].pTo)
        free(clnt->pTemporal[0].pTo);
    clnt->pTemporal[0].pTo = NULL;
    clnt->pTemporal[0].iIncl = 0;
    clnt->pTemporal[0].iAll = 0;
    clnt->pTemporal[0].iBus = 0;
    if (clnt->pTemporal[1].pFrom)
        free(clnt->pTemporal[1].pFrom);
    clnt->pTemporal[1].pFrom = NULL;
    if (clnt->pTemporal[1].pTo)
        free(clnt->pTemporal[1].pTo);
    clnt->pTemporal[1].pTo = NULL;
    clnt->pTemporal[1].iIncl = 0;
    clnt->pTemporal[1].iAll = 0;
    clnt->pTemporal[1].iBus = 0;

    clnt->pTemporalParser = NULL;
    
    clnt->statement_query_effects = 0;
    clnt->wrong_db = 0;
    set_sent_data_to_client(clnt, 0, __func__, __LINE__);
    set_asof_snapshot(clnt, 0, __func__, __LINE__);
    clnt->sqltick = 0;
    clnt->rowbuffer = 1;
    clnt->flat_col_vals = 0;
    clnt->request_fp = 0;
    clnt->can_redirect_fdb = 0;
    clnt->force_fdb_push_redirect = 0;
    clnt->force_fdb_push_remote = 0;
    clnt->typessql = 0;
    clnt->return_long_column_names = 0;
    free(clnt->prev_cost_string);
    clnt->prev_cost_string = NULL;

    if (gbl_sockbplog) {
        init_bplog_socket(clnt);
    }

    clnt->modsnap_in_progress = 0;
    if (clnt->modsnap_registration) {
        bdb_unregister_modsnap(thedb->bdb_env, clnt->modsnap_registration);
        clnt->modsnap_registration = NULL;
    }
}

void reset_clnt_flags(struct sqlclntstate *clnt)
{
    clnt->early_retry = 0;
    clnt->has_recording = 0;
    clnt->statement_timedout = 0;
    clnt->writeTransaction = 0;
    clnt->modsnap_in_progress = 0;
    if (clnt->modsnap_registration) {
        bdb_unregister_modsnap(thedb->bdb_env, clnt->modsnap_registration);
        clnt->modsnap_registration = NULL;
    }
}

int sbuf_is_local(SBUF2 *sb)
{
    struct sockaddr_in addr;

    if (net_appsock_get_addr(sb, &addr))
        return 1;

    if (addr.sin_addr.s_addr == gbl_myaddr.s_addr)
        return 1;

    if (addr.sin_addr.s_addr == htonl(INADDR_LOOPBACK))
        return 1;

    return 0;
}

int recover_deadlock_evbuffer(struct sqlclntstate *clnt)
{
    if (!gbl_sql_release_locks_on_slow_reader) {
        return 0;
    }
    bdb_state_type *env = thedb->bdb_env;
    if (!bdb_curtran_has_waiters(env, clnt->dbtran.cursor_tran) && !bdb_lock_desired(env)) {
        return 0;
    }
    int flags = 0;
    if (gbl_fail_client_write_lock && !(rand() % gbl_fail_client_write_lock)) {
        flags = RECOVER_DEADLOCK_FORCE_FAIL;
    }
    if (!recover_deadlock_flags(env, clnt, NULL, 0, __func__, __LINE__, flags)) {
        return -1;
    }
    return 0;
}

static int recover_deadlock_sbuf(struct sqlclntstate *clnt)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    int count = 0;

    /* Short circuit */
    if (!clnt) {
        assert(bdb_lockref() == 0);
        return 1;
    }

    /* Sql thread */
    if (thd) {
        if (release_locks_int("slow reader", __func__, __LINE__, clnt) != 0) {
            assert(bdb_lockref() == 0);
            logmsg(LOGMSG_ERROR, "%s release_locks failed\n", __func__);
            return 1;
        }
        assert(bdb_lockref() > 0);
        return 0;
    }

    /* Appsock/heartbeat thread emitting a row */
    if (clnt && clnt->emitting_flag) {
        assert(clnt->heartbeat_lock == 0);
        assert(clnt->need_recover_deadlock == 0);
        clnt->heartbeat_lock = 1;
        clnt->need_recover_deadlock = 1;
        do {
            struct timespec ts;
            count++;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec++;
            if (count > 5) {
                logmsg(LOGMSG_ERROR,
                       "%s wait for sql to release locks, count=%d\n", __func__,
                       count);
            }
            pthread_cond_timedwait(&clnt->write_cond, &clnt->write_lock, &ts);
        /* Must check emitting_flag to handle trylock failures */
        } while (clnt->need_recover_deadlock == 1 && clnt->emitting_flag);
        clnt->heartbeat_lock = 0;
        return clnt->need_recover_deadlock;
    }

    /* Recover deadlock not run */
    return 1;
}

int sql_write_sbuf(SBUF2 *sb, const char *buf, int nbytes)
{
    ssize_t nwrite, written = 0;
    struct sqlclntstate *clnt = sbuf2getclnt(sb);
    int retry = -1;
    int released_locks = 0;

retry:
    retry++;
    while (written < nbytes) {
        struct pollfd pd;
        pd.fd = sbuf2fileno(sb);
        pd.events = POLLOUT;
        errno = 0;
        int rc = poll(&pd, 1, 100);

        if (rc < 0) {
            if (errno == EINTR || errno == EAGAIN)
                goto retry;
            return -1;
        }
        if (rc == 0) {
            if ((gbl_sql_release_locks_on_slow_reader &&
                 (!released_locks ||
                  bdb_curtran_has_waiters(thedb->bdb_env,
                                          clnt->dbtran.cursor_tran))) ||
                bdb_lock_desired(thedb->bdb_env)) {
                rc = recover_deadlock_sbuf(clnt);
                if (rc == 0)
                    released_locks = 1;
            }

#ifdef _SUN_SOURCE
            if (gbl_flush_check_active_peer) {
                /* On Solaris, we end up with sockets with
                 * no peer, on which poll cheerfully returns
                 * no events. So after several retries check if
                 * still connected. */
                if (retry % 10 == 0) {
                    /* if we retried for a second, see if
                     * the connection is still around.
                     */
                    struct sockaddr_in peeraddr;
                    socklen_t len = sizeof(peeraddr);
                    rc = getpeername(pd.fd, (struct sockaddr *)&peeraddr, &len);
                    if (rc == -1 && errno == ENOTCONN) {
                        ctrace("fd %d disconnected\n", pd.fd);
                        return -1;
                    }
                }
            }
#endif
            (void)retry;
            goto retry;
        }
        if (pd.revents & POLLOUT) {
            /* I dislike this code in this layer - it should be in net. */
            nwrite = sbuf2unbufferedwrite(sb, &buf[written], nbytes - written);
            if (nwrite < 0)
                return -1;
            written += nwrite;
        } else if (pd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            return -1;
        }
    }
    return written;
}

pthread_mutex_t gbl_sql_lock;

/* Write sql interface.  This will replace sqlnet.c */

/* don't let connections get more than this much memory */
static int alloc_lim = 1024 * 1024 * 8;

/* any state associated with a connection (including open db handles)
   needs to be stored here.  everything is cleaned up on end of thread
   in destroy_sqlconn */
struct sqlconn {
    pthread_t tid;
    SBUF2 *sb;
    hash_t *handles;
    sqlite3 *db;
    int reqsz;

    struct statement_handle *last_handle;

    /* all reads are done to this buffer */
    char *buf;
    int bufsz;

    /* for debugging/stats: current state and timestamp when it was entered */
    char *state;
    int tm;

    LINKC_T(struct sqlconn) lnk;
};

static int write_str(struct sqlconn *conn, char *err);

static void conn_set_state(struct sqlconn *conn, char *state)
{
    conn->state = state;
    conn->tm = time(NULL);
}

static void conn_alloc(struct sqlconn *conn, int sz)
{
    if (conn->bufsz >= sz)
        return;
    conn->bufsz = sz;
    conn->buf = realloc(conn->buf, conn->bufsz);
}

/* handles are always a per-connection deal, and a connection
   always has a dedicated thread, so no need to lock around
   handles */
typedef unsigned long long handle_tp;
enum req_code {
    REQ_EOF = -2,
    REQ_INVALID = -1,
    REQ_CONNECT = 0,
    REQ_PREPARE = 1,
    REQ_VERSION = 2,
    REQ_CHANGES = 3,
    REQ_FINALIZE = 4,
    REQ_STEP = 5,
    REQ_RESET = 6
};

/* request and responses go back in this format */
struct reqhdr {
    int rq;
    int followlen;
};

/* column for results coming back */

struct statement_handle {
    /* context: need to swap these when switching between handles */
    struct sqlthdstate sqlstate;
    struct sqlclntstate clnt;

    handle_tp hid;
    sqlite3_stmt *p;
    int *types;
};

static void switch_context(struct sqlconn *conn, struct statement_handle *h)
{
    return;

#if 0
    struct sql_thread *thd;
    int i;
    sqlite3 *db;
    /* don't do anything if we are working with the same statemtn as last time */
    if (conn->last_handle == h)
        return;

    conn->last_handle = h;

    thd = pthread_getspecific(query_info_key);
    h->sqlstate.sqlthd = thd;
    h->clnt.debug_sqlclntstate = pthread_self();


    db = conn->db;
    /* reset client handle - we need one per statement */
    for (i = 0; i < db->nDb; i++) {
         if (db->aDb[i].pBt) {
            db->aDb[i].pBt->clnt = &h->clnt;
         }
    }
#endif
}

#if 0
static int closehndl(void *obj, void *arg) {
    struct sqlconn *conn;
    struct statement_handle *h;

    conn = (struct sqlconn*) arg;
    h = (struct statement_handle*) obj;

    sqlite3_finalize(h->p);
    free(h);
}
#endif

/* read request from connection, write to connection's buffer. return request
 * type */
static enum req_code read_req(struct sqlconn *conn)
{
    struct reqhdr rq;
    int rc;

    conn->reqsz = 0;

    /* header */
    rc = sbuf2fread((char *)&rq, sizeof(struct reqhdr), 1, conn->sb);
    if (rc == 0)
        return REQ_EOF;

    if (rc != 1)
        return REQ_INVALID;

    rq.rq = ntohl(rq.rq);
    rq.followlen = ntohl(rq.followlen);

    /* sanity check buffer size required */
    if (rq.followlen < 0 || rq.followlen > alloc_lim)
        return REQ_INVALID;

    conn_alloc(conn, rq.followlen);

    conn->reqsz = rq.followlen;
    rc = sbuf2fread((char *)conn->buf, rq.followlen, 1, conn->sb);
    if (rc != 1)
        return REQ_INVALID;

    return rq.rq;
}

/* Called when a query is done, while all the cursors are still open.  Traverses
   the list of cursors and saves the query path and cost. */
static int record_query_cost(struct sql_thread *thd, struct sqlclntstate *clnt)
{
    struct client_query_path_component *stats;
    int i;
    struct client_query_stats *query_info;
    struct query_path_component *c;
    int max_components;
    int sz;

    if (clnt->query_stats) {
        free(clnt->query_stats);
        clnt->query_stats = NULL;
    }

    if (!thd)
        return -1;

    max_components = listc_size(&thd->query_stats);
    sz = offsetof(struct client_query_stats, path_stats) +
         sizeof(struct client_query_path_component) * max_components;
    query_info = calloc(1, sz);
    clnt->query_stats = query_info;
    stats = query_info->path_stats;

    query_info->nlocks = -1;
    query_info->n_write_ios = -1;
    query_info->n_read_ios = -1;
    query_info->cost = query_cost(thd);
    query_info->n_components = max_components;
    query_info->n_rows =
        0; /* client computes from #records read, this is only
             useful for writes where this information doesn't come
             back */
    query_info->queryid = clnt->queryid;
    memset(query_info->reserved, 0xff, sizeof(query_info->reserved));

    i = 0;
    LISTC_FOR_EACH(&thd->query_stats, c, lnk)
    {
        if (i >= max_components) {
            free(clnt->query_stats);
            clnt->query_stats = 0;
            return -1;
        }

        if (c->nfind == 0 && c->nnext == 0 && c->nwrite == 0) {
            query_info->n_components--;
            continue;
        }
        stats[i].nfind = c->nfind;
        stats[i].nnext = c->nnext;
        stats[i].nwrite = c->nwrite;
        stats[i].ix = c->ix;
        stats[i].table[0] = 0;
        if (c->rmt_db[0]) {
            snprintf0(stats[i].table, sizeof(stats[i].table), "%s.%s",
                      c->rmt_db, c->lcl_tbl_name[0] ? c->lcl_tbl_name : "NULL");
        } else if (c->lcl_tbl_name[0]) {
            strncpy0(stats[i].table, c->lcl_tbl_name, sizeof(stats[i].table));
        }
        i++;
    }
    return 0;
}

int dbglog_begin(const char *pragma)
{
    struct sql_thread *thd;
    struct sqlclntstate *clnt;

    thd = pthread_getspecific(query_info_key);
    if (thd == NULL)
        return -1;

    clnt = thd->clnt;
    if (clnt == NULL)
        return -1;

    return dbglog_process_debug_pragma(clnt, pragma);
}

struct client_query_stats *get_query_stats_from_thd()
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (!thd)
        return NULL;

    struct sqlclntstate *clnt = thd->clnt;
    if (!clnt)
        return NULL;

    record_query_cost(thd, clnt);

    return clnt->query_stats;
}

char *comdb2_get_prev_query_cost()
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (!thd)
        return NULL;

    struct sqlclntstate *clnt = thd->clnt;
    if (!clnt)
        return NULL;

    return clnt->prev_cost_string;
}

void comdb2_free_prev_query_cost()
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (!thd)
        return;

    struct sqlclntstate *clnt = thd->clnt;
    if (!clnt)
        return;

    if (clnt->prev_cost_string) {
        free(clnt->prev_cost_string);
        clnt->prev_cost_string = NULL;
    }
}

int comdb2_get_server_port()
{
    return thedb->sibling_port[0][NET_REPLICATION];
}

/* get sql query cost and return it as char *
 * function will allocate memory for string
 * and caller should free that memory area
 */
static char *get_query_cost_as_string(struct sql_thread *thd,
                                      struct sqlclntstate *clnt)
{
    if (!clnt || !thd)
        return NULL;
    record_query_cost(thd, clnt);

    if (!clnt->query_stats)
        return NULL;

    strbuf *out = strbuf_new();
    struct client_query_stats *st = clnt->query_stats;

    strbuf_appendf(out, "Cost: %.2lf NRows: %d\n", st->cost, clnt->nrows);
    for (int ii = 0; ii < st->n_components; ii++) {
        strbuf_append(out, "    ");
        if (st->path_stats[ii].table[0] == '\0') {
            strbuf_appendf(out, "temp index finds %d ",
                           st->path_stats[ii].nfind);
            if (st->path_stats[ii].nnext)
                strbuf_appendf(out, "next/prev %d ", st->path_stats[ii].nnext);
            if (st->path_stats[ii].nwrite)
                strbuf_appendf(out, "nwrite %d ", st->path_stats[ii].nwrite);
        } else {
            if (st->path_stats[ii].ix >= 0)
                strbuf_appendf(out, "index %d on ", st->path_stats[ii].ix);
            strbuf_appendf(out, "table %s finds %d", st->path_stats[ii].table,
                           st->path_stats[ii].nfind);
            if (st->path_stats[ii].nnext > 0) {
                strbuf_appendf(out, " next/prev %d", st->path_stats[ii].nnext);
                if (st->path_stats[ii].ix < 0)
                    strbuf_appendf(out, "[TABLE SCAN]");
            }
        }
        strbuf_append(out, "\n");
    }

    char *str = strbuf_disown(out);
    strbuf_free(out);
    return str;
}

static int execute_sql_query_offload_inner_loop(struct sqlclntstate *clnt,
                                                struct sqlthdstate *poolthd,
                                                sqlite3_stmt *stmt)
{
    int ret;
    UnpackedRecord upr;
    Mem res;
    char *cid;
    int rc = 0;
    int tmp;
    int sent;

    if (!clnt->fdb_state.remote_sql_sb) {
        while ((ret = next_row(clnt, stmt)) == SQLITE_ROW)
            ;
    } else {
        bzero(&res, sizeof(res));

        if (clnt->osql.rqid == OSQL_RQID_USE_UUID)
            cid = (char *)&clnt->osql.uuid;
        else
            cid = (char *)&clnt->osql.rqid;

        sent = 0;
        while (1) {
            /* NOTE: in the recom and serial mode, the cursors look at the
            shared shadow_tran
            while the transaction updates are arriving! Mustering the parallel
            computing!
            Get the LOCK!
            */
            if (clnt->dbtran.mode == TRANLEVEL_RECOM ||
                clnt->dbtran.mode == TRANLEVEL_SERIAL ||
                clnt->dbtran.mode == TRANLEVEL_MODSNAP) {
                Pthread_mutex_lock(&clnt->dtran_mtx);
            }

            ret = next_row(clnt, stmt);

            if (clnt->dbtran.mode == TRANLEVEL_RECOM ||
                clnt->dbtran.mode == TRANLEVEL_SERIAL ||
                clnt->dbtran.mode == TRANLEVEL_MODSNAP) {
                Pthread_mutex_unlock(&clnt->dtran_mtx);
            }

            if (ret != SQLITE_ROW) {
                break;
            }

            if (res.z) {
                /* now we have the packed sqlite row in Mem->z */
                rc = fdb_svc_sql_row(clnt->fdb_state.remote_sql_sb, cid, res.z,
                                     res.n, IX_FNDMORE);
                if (rc) {
                    /*
                    fprintf(stderr, "%s: failed to send back sql row\n",
                    __func__);
                    */
                    break;
                }
            }

            bzero(&upr, sizeof(upr));
            sqlite3VdbeMemRelease(&res);
            bzero(&res, sizeof(res));

            upr.aMem = sqlite3GetCachedResultRow(stmt, &tmp);
            if (!upr.aMem) {
                logmsg(LOGMSG_ERROR, "%s: failed to retrieve result set\n",
                        __func__);
                return -1;
            }

            upr.nField = tmp;

            /* special treatment for sqlite_master */
            if (clnt->fdb_state.flags == FDB_RUN_SQL_SCHEMA) {
                rc = fdb_svc_alter_schema(clnt, stmt, &upr);
                if (rc) {
                    /* break; Ignore for now, this will run less optimized */
                }
            }

            sqlite3VdbeRecordPack(&upr, &res);
            sent = 1;
        }

        /* send the last row, marking flag as such */
        if (!rc) {
            if (sent == 1) {
                rc = fdb_svc_sql_row(clnt->fdb_state.remote_sql_sb, cid, res.z,
                                     res.n, IX_FND);
            } else {
                rc = fdb_svc_sql_row(clnt->fdb_state.remote_sql_sb, cid, res.z,
                                     res.n, IX_EMPTY);
            }
            if (rc) {
                /*
                fprintf(stderr, "%s: failed to send back sql row\n", __func__);
                */
            }
        }

        /* cleanup last row */
        sqlite3VdbeMemRelease(&res);
    }

    /* blocksql doesn't look at sqlite3_step, result of transaction commit
       are submitted by lower levels; we need to fix this for remote cursors

       NOTE: when caller closes the socket early, ret == SQLITE_ROW.  This is
       not
       an error, caller decided it needs no more rows.

       */

    if (ret == SQLITE_DONE || ret == SQLITE_ROW) {
        ret = 0;
    }

    return ret;
}

static int execute_sql_query_offload(struct sqlthdstate *poolthd,
                                     struct sqlclntstate *clnt)
{
    int ret = 0;
    struct sql_thread *thd = poolthd->sqlthd;
    char *cid;
    if (!thd) {
        logmsg(LOGMSG_ERROR, "%s: no sql_thread\n", __func__);
        return SQLITE_INTERNAL;
    }
    if (clnt->osql.rqid == OSQL_RQID_USE_UUID)
        cid = (char *)&clnt->osql.uuid;
    else
        cid = (char *)&clnt->osql.rqid;

    if(!clnt->sql_ref)
        clnt->sql_ref = create_string_ref(clnt->sql);
    reqlog_new_sql_request(poolthd->logger, clnt->sql_ref);

    log_queue_time(poolthd->logger, clnt);
    bzero(&clnt->fail_reason, sizeof(clnt->fail_reason));
    bzero(&clnt->osql.xerr, sizeof(clnt->osql.xerr));
    struct sql_state rec = {0};
    rec.sql = clnt->sql;
    if (get_prepared_bound_stmt(poolthd, clnt, &rec, &clnt->osql.xerr,
                                PREPARE_NONE)) {
        /* if prepare failed, and this is not a versioning issue,
           report error */
        if (clnt->fdb_state.xerr.errval != SQLITE_SCHEMA)
            ret = ERR_SQL_PREP;
        goto done;
    }
    thrman_wheref(poolthd->thr_self, "%s", rec.sql);
    user_request_begin(REQUEST_TYPE_QTRAP, FLAG_REQUEST_TRACK_EVERYTHING);
    if (gbl_dump_sql_dispatched)
        logmsg(LOGMSG_USER, "BLOCKSQL mode=%d [%s]\n", clnt->dbtran.mode,
                clnt->sql);
    ret = execute_sql_query_offload_inner_loop(clnt, poolthd, rec.stmt);
done:
    if ((gbl_who > 0) || debug_this_request(gbl_debug_until)) {
        struct per_request_stats *st;
        st = user_request_get_stats();
        if (st)
            logmsg(LOGMSG_USER,
                    "nreads %d (%lld bytes) nwrites %d (%lld bytes) nmempgets %d\n",
                    st->nreads, st->readbytes, st->nwrites, st->writebytes,
                    st->mempgets);
        gbl_who--;
    }
    if (clnt->client_understands_query_stats) {
        record_query_cost(thd, thd->clnt);
    }
    /* if we turned on case sensitive like, turn it off since the sql handle we
       just used may be used by another connection with this disabled */
    if (clnt->using_case_insensitive_like)
        toggle_case_sensitive_like(poolthd->sqldb, 0);
    /* check for conversion errors;
       in the case of an error, osql.xerr.errval will be set probably to
       SQLITE_INTERNAL
     */
    char *errstr = NULL;
    int rc = sql_check_errors(clnt, poolthd->sqldb, rec.stmt, (const char **)&errstr);
    if (rc) {
        /* check for prepare errors */
        if (ret == ERR_SQL_PREP)
            rc = ERR_SQL_PREP;
        errstat_set_rc(&clnt->osql.xerr, rc);
        errstat_set_str(&clnt->osql.xerr, errstr);
    }

    if (ret) {
        const char *tmp = errstat_get_str(&clnt->osql.xerr);
        tmp = tmp ? tmp : "error string not set";
        rc = fdb_svc_sql_row(clnt->fdb_state.remote_sql_sb, cid, (char *)tmp,
                             strlen(tmp) + 1, errstat_get_rc(&clnt->osql.xerr));
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s failed to send back error rc=%d errstr=%s\n", __func__,
                   errstat_get_rc(&clnt->osql.xerr), tmp);
        }
    }

    sqlite_done(poolthd, clnt, &rec, ret);

    return ret;
}

int sqlpool_init(void)
{
    /* NOTE: Force creation of the default SQL engine thread pool. */
    (void)get_default_sql_pool(1);
    return 0;
}

static const char* connstate_str(enum connection_state s) {
    switch (s) {
        case CONNECTION_NEW:
            return "CONNECTION_NEW";

        case CONNECTION_IDLE:
            return "CONNECTION_IDLE";

        case CONNECTION_RESET:
            return "CONNECTION_RESET";

        case CONNECTION_QUEUED:
            return "CONNECTION_QUEUED";

        case CONNECTION_RUNNING:
            return "CONNECTION_RUNNING";

        default:
            return "???";
    }
}

void clnt_change_state(struct sqlclntstate *clnt, enum connection_state state) {
    clnt->state_start_time = comdb2_time_epochms();
    Pthread_mutex_lock(&clnt->state_lk);
    clnt->state = state;
    Pthread_mutex_unlock(&clnt->state_lk);
}

/**
 * Resets sqlite engine to retrieve the error code
 */
int sql_check_errors(struct sqlclntstate *clnt, sqlite3 *sqldb,
                     sqlite3_stmt *stmt, const char **errstr)
{
    int rc, fdb_rc;

    rc = sqlite3_reset(stmt);

    if (clnt->fdb_state.preserve_err &&
        (fdb_rc = errstat_get_rc(&clnt->fdb_state.xerr))) {
        rc = fdb_rc;
        *errstr = errstat_get_str(&clnt->fdb_state.xerr);
        goto done;
    }
    switch (rc) {
    case 0:
        rc = sqlite3_errcode(sqldb);
        if (rc)
            *errstr = sqlite3_errmsg(sqldb);
        break;

    case SQLITE_DEADLOCK:
        gbl_sql_deadlock_failures++;
        *errstr = sqlite3_errmsg(sqldb);
        break;

    case SQLITE_TOOBIG: /* We interpret SQLITE_TOOBIG differently from SQLite. */
        if (clnt->sqlite_errstr) {
            *errstr = clnt->sqlite_errstr;
            clnt->saved_errstr = 0;
        } else {
            *errstr = "transaction too big";
        }
        rc = ERR_TRAN_TOO_BIG;
        break;

    case SQLITE_ABORT:
        /* no error in this case, regular abort or
           block processor failure to commit */
        rc = SQLITE_OK;
        break;

    case SQLITE_ERROR:
        /* check for convertion failure, stored in clnt->fail_reason */
        if (clnt->fail_reason.reason != CONVERT_OK)
            rc = ERR_CONVERT_DTA;
        *errstr = sqlite3_errmsg(sqldb);
        break;

    case SQLITE_ACCESS:
        *errstr = errstat_get_str(&clnt->osql.xerr);
        if (!*errstr || (*errstr)[0] == 0) {
            *errstr = sqlite3_errmsg(sqldb);
            /* hate it please fix */
            if (*errstr == NULL || strcmp(*errstr, "not an error") == 0)
                *errstr = "access denied";
        }
        break;

    case SQLITE_CONV_ERROR:
        if (!*errstr)
            *errstr = sqlite3_errmsg(sqldb);
        break;

    case 147: // 147 = 0 - SQLHERR_MASTER_TIMEOUT
        rc = SQLITE_CLIENT_CHANGENODE;
        *errstr = sqlite3_errstr(rc);
        break;

    case SQLITE_SCHEMA_REMOTE:
        rc = SQLITE_OK; /* this is processed based on clnt->osql.xerr */
        break;

    case SQLITE_TRANTOOCOMPLEX:
    case SQLITE_TRAN_CANCELLED:
    case SQLITE_TRAN_NOLOG:
    case SQLITE_TRAN_NOUNDO:
    case SQLITE_CLIENT_CHANGENODE:
    case SQLITE_COMDB2SCHEMA:
    case SQLITE_TIMEDOUT:
    case SQLITE_COST_TOO_HIGH:
    case SQLITE_NO_TEMPTABLES:
    case SQLITE_NO_TABLESCANS:
    case SQLITE_ANALYZE_ALREADY_RUNNING:
        *errstr = sqlite3_errmsg(sqldb);
        break;

    default:
        logmsg(LOGMSG_DEBUG, "sql_check_errors got rc = %d, "
                             "returning as SQLITE_INTERNAL\n",
               rc);
        rc = SQLITE_INTERNAL;
        *errstr = sqlite3_errmsg(sqldb);
        break;
    }

done:
    /* for distributed queries, it is possible that this
       particular shard to succeed, but still have an error
       in another shard; check that */
    if (rc == 0 && unlikely(clnt->conns)) {
        return dohsql_error(clnt, errstr);
    }

    return rc;
}

enum {
    /* LEGACY CODES */
    DB_ERR_BAD_REQUEST = 110,  /* 199 */
    DB_ERR_BAD_COMM_BUF = 111, /* 998 */
    DB_ERR_BAD_COMM = 112,     /* 999 */
    DB_ERR_NONKLESS = 114,     /* 212 */
    /* GENERAL BLOCK TRN RCODES */
    DB_ERR_TRN_BUF_INVALID = 200,  /* 105 */
    DB_ERR_TRN_BUF_OVERFLOW = 201, /* 106 */
    DB_ERR_TRN_OPR_OVERFLOW = 202, /* 205 */
    DB_ERR_TRN_DB_FAIL = 204,      /* 220 */
    DB_ERR_TRN_NOT_SERIAL = 230,
    DB_ERR_TRN_SC = 240,
    /* INTERNAL DB ERRORS */
    DB_ERR_INTR_GENERIC = 304,
};

/*
 * convert a block processor code error
 * to an sql code error
 * this is also done for blocksql on the client side
 */
int blockproc2sql_error(int rc, const char *func, int line)
{
    switch (rc) {
    case 0:
        return CDB2_OK;
    /* error dispatched by the block processor */
    case 102:
        return CDB2ERR_NOMASTER;
    case 105:
        return DB_ERR_TRN_BUF_INVALID;
    case 106:
        return DB_ERR_TRN_BUF_OVERFLOW;
    case ERR_READONLY: //195
        return CDB2ERR_READONLY;
    case 199:
        return DB_ERR_BAD_REQUEST;
    case 208:
        return DB_ERR_TRN_OPR_OVERFLOW;
    case 212:
        return DB_ERR_NONKLESS;
    case 220:
        return CDB2ERR_DEADLOCK;
    case 222:
        return CDB2__ERROR_CODE__DUP_OLD;
    case 224:
        return CDB2ERR_VERIFY_ERROR;
    case 225:
        return DB_ERR_TRN_DB_FAIL;
    case 230:
        return DB_ERR_TRN_NOT_SERIAL;
    case 240:
        return DB_ERR_TRN_SC;
    case 301:
        return CDB2ERR_CONV_FAIL;
    case 998:
        return DB_ERR_BAD_COMM_BUF;
    case 999:
        return DB_ERR_BAD_COMM;
    case 2000:
        return DB_ERR_TRN_DB_FAIL;
    case 2001:
        return CDB2ERR_PREPARE_ERROR;

    /* hack for now; if somehow we get a 300/RC_INTERNAL_RETRY
       it means that due to schema change or similar issues
       and we report deadlock error;
       in the future, this could be retried
     */
    case 300:
        return CDB2ERR_DEADLOCK;

    /* error dispatched on the sql side */
    case ERR_NOMASTER:
        return CDB2ERR_NOMASTER;

    case ERR_CONSTR:
        return CDB2ERR_CONSTRAINTS;

    case ERR_DIST_ABORT:
        return CDB2ERR_DIST_ABORT;

    case ERR_NULL_CONSTRAINT:
        return CDB2ERR_NULL_CONSTRAINT;

    case SQLITE_ACCESS:
        return CDB2ERR_ACCESS;

    case 1229: /* ERR_BLOCK_FAILED + OP_FAILED_INTERNAL + ERR_FIND_CONSTRAINT */
        return CDB2ERR_FKEY_VIOLATION;

    case ERR_UNCOMMITTABLE_TXN:
        return CDB2ERR_VERIFY_ERROR;

    case ERR_REJECTED:
        return SQLHERR_MASTER_QUEUE_FULL;

    case SQLHERR_MASTER_TIMEOUT:
        return SQLHERR_MASTER_TIMEOUT;

    case ERR_NOT_DURABLE:
        return CDB2ERR_CHANGENODE;

    case ERR_CHECK_CONSTRAINT + ERR_BLOCK_FAILED:
        return CDB2ERR_CHECK_CONSTRAINT;

    case ERR_QUERY_REJECTED:
        return CDB2ERR_QUERY_REJECTED;

    default:
        return DB_ERR_INTR_GENERIC;
    }
}

int sqlserver2sqlclient_error(int rc)
{
    switch (rc) {
    case SQLITE_DEADLOCK:
    case SQLITE_BUSY:
        return CDB2ERR_DEADLOCK;
    case SQLITE_TIMEDOUT:
    case SQLITE_COST_TOO_HIGH:
    case SQLITE_NO_TEMPTABLES:
    case SQLITE_NO_TABLESCANS:
        return CDB2ERR_QUERYLIMIT;
    case SQLITE_TRANTOOCOMPLEX:
        return SQLHERR_ROLLBACKTOOLARGE;
    case SQLITE_CLIENT_CHANGENODE:
        return CDB2ERR_CHANGENODE;
    case SQLITE_TRAN_CANCELLED:
        return SQLHERR_ROLLBACK_TOOOLD;
    case SQLITE_TRAN_NOLOG:
        return SQLHERR_ROLLBACK_NOLOG;
    case SQLITE_ACCESS:
        return CDB2ERR_ACCESS;
    case ERR_TRAN_TOO_BIG:
        return DB_ERR_TRN_OPR_OVERFLOW;
    case SQLITE_INTERNAL:
        return CDB2ERR_INTERNAL;
    case ERR_CONVERT_DTA:
        return CDB2ERR_CONV_FAIL;
    case SQLITE_TRAN_NOUNDO:
        return SQLHERR_ROLLBACK_NOLOG; /* this will suffice */
    case SQLITE_COMDB2SCHEMA:
        return CDB2ERR_SCHEMA;
    case CDB2ERR_PREPARE_ERROR:
        return CDB2ERR_PREPARE_ERROR;
    case SQLITE_ANALYZE_ALREADY_RUNNING:
        return CDB2ERR_ANALYZE_ALREADY_RUNNING;
    default:
        return CDB2ERR_UNKNOWN;
    }
}

static int test_no_btcursors(struct sqlthdstate *thd)
{

    sqlite3 *db;
    if ((db = thd->sqldb) == NULL) {
        return 0;
    }
    BtCursor *pCur = NULL;
    int leaked = 0;
    int i = 0;
    int rc = 0;

    for (i = 0; i < db->nDb; i++) {

        Btree *pBt = db->aDb[i].pBt;

        if (!pBt)
            continue;
        if (pBt->cursors.count) {
            logmsg(LOGMSG_ERROR, "%s: detected %d leaked btcursors\n", __func__,
                    pBt->cursors.count);
            leaked = 1;
            while (pBt->cursors.count) {

                pCur = listc_rtl(&pBt->cursors);
                if (pCur->bdbcur) {
                    logmsg(LOGMSG_ERROR, "%s: btcursor has bdbcursor opened\n",
                            __func__);
                }
                rc = sqlite3BtreeCloseCursor(pCur);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "sqlite3BtreeClose:sqlite3BtreeCloseCursor rc %d\n",
                           rc);
                }
            }
        }
    }

    return leaked;
}

unsigned long long osql_log_time(void)
{
    if (0) {
        return 1000 * ((unsigned long long)comdb2_time_epoch()) +
               comdb2_time_epochms();
    } else {
        struct timeval tv;

        gettimeofday(&tv, NULL);

        return 1000 * ((unsigned long long)tv.tv_sec) +
               ((unsigned long long)tv.tv_usec) / 1000;
    }
}

struct global_sql_timings {
    unsigned long long sql_time;
    unsigned long long wait_time;
    unsigned long long queued_time;
} gbl_sql_tmng = {0};

pthread_mutex_t gbl_sql_tmngs_mtx = PTHREAD_MUTEX_INITIALIZER;

void global_sql_timings_print(void)
{
    logmsg(LOGMSG_USER, "Total sql %lld\nTotal wait %lld\nTotal queued %lld\n",
           gbl_sql_tmng.sql_time, gbl_sql_tmng.wait_time,
           gbl_sql_tmng.queued_time);
}

void osql_log_time_done(struct sqlclntstate *clnt)
{
    osqlstate_t *osql = &clnt->osql;
    osqltimings_t *tms = &osql->timings;
    fdbtimings_t *fdbtms = &osql->fdbtimes;

    if (!gbl_time_osql)
        goto fdb;

    /* fix short paths */
    if (tms->commit_end == 0)
        tms->commit_end = tms->query_finished;

    if (tms->commit_start == 0)
        tms->commit_start = tms->commit_end;

    if (tms->commit_prep == 0)
        tms->commit_prep = tms->commit_start;

    logmsg(LOGMSG_USER, "rqid=%llu total=%llu (queued=%llu) sql=%llu (exec=%llu "
            "prep=%llu commit=%llu)\n",
            osql->rqid, tms->query_finished - tms->query_received, /*total*/
            tms->query_dispatched - tms->query_received,           /*queued*/
            tms->query_finished - tms->query_dispatched, /*sql processing*/
            tms->commit_prep -
                tms->query_dispatched, /*local sql execution, before commit*/
            tms->commit_start - tms->commit_prep, /*time to ship shadows*/
            tms->commit_end -
                tms->commit_start /*ship commit, replicate, received rc*/
            );

fdb:
    if (!gbl_time_fdb)
        goto done;

    logmsg(LOGMSG_USER, "total=%llu msec (longest=%llu msec) calls=%llu\n",
            fdbtms->total_time, fdbtms->max_call, fdbtms->total_calls);

done:
    Pthread_mutex_lock(&gbl_sql_tmngs_mtx);
    gbl_sql_tmng.queued_time += tms->query_dispatched - tms->query_received;
    gbl_sql_tmng.sql_time += tms->query_finished - tms->query_dispatched;
    gbl_sql_tmng.wait_time += tms->commit_end - tms->commit_start;
    Pthread_mutex_unlock(&gbl_sql_tmngs_mtx);
}

static void sql_thread_describe(void *obj, FILE *out)
{
    struct sqlclntstate *clnt = (struct sqlclntstate *)obj;
    if (!clnt) {
        logmsg(LOGMSG_USER, "non sql thread ???\n");
        return;
    }
    logmsg(LOGMSG_USER, "%s \"%s\"\n", clnt->origin, clnt->sql);
}

/**
 * Callback for sqlite during prepare, to retrieve default tzname
 * Required by stat4 which need datetime conversions during prepare
 *
 */
void comdb2_set_sqlite_vdbe_time_info(Vdbe *p)
{
    struct sql_thread *sqlthd = pthread_getspecific(query_info_key);
    if (!sqlthd)
        return;
    /* set the default timezone */
    comdb2_set_sqlite_vdbe_tzname_int(p, sqlthd->clnt);
    /* set the default datetime precision */
    p->dtprec = sqlthd->clnt->dtprec;
    /* set the now() value */
    clock_gettime(CLOCK_REALTIME, &p->tspec);
}

void comdb2_set_sqlite_vdbe_dtprec(Vdbe *p)
{
    struct sql_thread *sqlthd = pthread_getspecific(query_info_key);
    if (!sqlthd)
        return;
    comdb2_set_sqlite_vdbe_dtprec_int(p, sqlthd->clnt);
}

void run_internal_sql(char *sql)
{
    struct sqlclntstate clnt;
    start_internal_sql_clnt(&clnt);
    clnt.sql = skipws(sql);
    int rc = dispatch_sql_query(&clnt);
    if (rc || clnt.query_rc || clnt.saved_errstr) {
        if (clnt.query_rc)
            logmsg(LOGMSG_ERROR, "%s: Error from query: '%s' (rc = %d) \n", __func__, sql, clnt.query_rc);
        if (clnt.saved_errstr)
            logmsg(LOGMSG_ERROR, "%s: Error: '%s' \n", __func__, clnt.saved_errstr);
    }
    end_internal_sql_clnt(&clnt);
}

static void gather_connection_int(struct connection_info *c, struct sqlclntstate *clnt)
{
    c->connection_id = clnt->connid;
    c->pid = clnt->last_pid;
    c->total_sql = clnt->total_sql;
    c->sql_since_reset = clnt->sql_since_reset;
    c->num_resets = clnt->num_resets;
    c->connect_time_int = clnt->connect_time;
    c->last_reset_time_int = clnt->last_reset_time;
    c->num_resets = clnt->num_resets;
    c->host = clnt->origin;
    c->state_int = clnt->state;
    c->time_in_state_int = clnt->state_start_time;
    c->is_admin = clnt->admin;
    c->is_ssl = clnt->plugin.has_ssl(clnt);
    c->has_cert = clnt->plugin.has_x509(clnt);
    if (!c->has_cert) {
        c->common_name = NULL;
    } else {
        memset(c->common_name_str, 0, sizeof(c->common_name_str));
        clnt->plugin.get_x509_attr(clnt, NID_commonName, c->common_name_str, sizeof(c->common_name_str));
        c->common_name = c->common_name_str;
    }
    Pthread_mutex_lock(&clnt->state_lk);
    if (clnt->state == CONNECTION_RUNNING || clnt->state == CONNECTION_QUEUED) {
        char zFingerprint[FINGERPRINTSZ * 2 + 1];
        util_tohex(zFingerprint, (char *)clnt->work.aFingerprint, FINGERPRINTSZ);
        c->sql = strdup(clnt->sql);
        c->fingerprint = strdup(zFingerprint);
    } else {
        c->sql = NULL;
        c->fingerprint = NULL;
    }
    c->in_transaction = clnt->in_client_trans;
    c->in_local_cache = clnt->in_local_cache;
    Pthread_mutex_unlock(&clnt->state_lk);
}

static void gather_connections_evbuffer(struct connection_info **info, int *num_connections)
{
    int num = 0;
    struct sqlclntstate *clnt;
    Pthread_mutex_lock(&lru_evbuffers_mtx);
    TAILQ_FOREACH(clnt, &sql_evbuffers, sql_entry) {
        ++num;
    }
    *num_connections = num;
    struct connection_info *c = *info = malloc(num * sizeof(struct connection_info));
    TAILQ_FOREACH(clnt, &sql_evbuffers, sql_entry) {
        gather_connection_int(c, clnt);
        ++c;
    }
    Pthread_mutex_unlock(&lru_evbuffers_mtx);
}

int gather_connection_info(struct connection_info **info, int *num_connections)
{
    gather_connections_evbuffer(info, num_connections);
    return 0;
}

void free_connection_info(struct connection_info *info, int num_connections)
{
    if (info == NULL) return;
    for (int i = 0; i < num_connections; i++) {
        if (info[i].sql) free(info[i].sql);
        if (info[i].fingerprint) free(info[i].fingerprint);
        /* state is static, don't free */
    }
    free(info);
}

void reqlog_long_running_sql_statements(void)
{
    struct sqlclntstate *clnt;
    Pthread_mutex_lock(&lru_evbuffers_mtx);
    TAILQ_FOREACH(clnt, &sql_evbuffers, sql_entry) {
        Pthread_mutex_lock(&clnt->sql_lk);
        reqlog_long_running_clnt(clnt);
        Pthread_mutex_unlock(&clnt->sql_lk);
    }
    Pthread_mutex_unlock(&lru_evbuffers_mtx);
}

void add_sql_evbuffer(struct sqlclntstate *clnt)
{
    clnt->state = CONNECTION_NEW;
    clnt->connect_time = comdb2_time_epoch();
    clnt->connid = ATOMIC_ADD64(connid, 1);
    Pthread_mutex_lock(&lru_evbuffers_mtx);
    TAILQ_INSERT_TAIL(&sql_evbuffers, clnt, sql_entry);
    Pthread_mutex_unlock(&lru_evbuffers_mtx);
}

void rem_sql_evbuffer(struct sqlclntstate *clnt)
{
    Pthread_mutex_lock(&lru_evbuffers_mtx);
    TAILQ_REMOVE(&sql_evbuffers, clnt, sql_entry);
    Pthread_mutex_unlock(&lru_evbuffers_mtx);
}

void init_lru_evbuffer(struct sqlclntstate *clnt)
{
    TAILQ_NEXT(clnt, lru_entry) = clnt;
}

void add_lru_evbuffer(struct sqlclntstate *clnt)
{
    Pthread_mutex_lock(&lru_evbuffers_mtx);
    if (in_client_trans(clnt)) {
        TAILQ_NEXT(clnt, lru_entry) = clnt; /* Point to self -> not in lru_evbuffers list */
    } else if (TAILQ_NEXT(clnt, lru_entry) == clnt) {
        TAILQ_INSERT_HEAD(&lru_evbuffers, clnt, lru_entry);
    }
    Pthread_mutex_unlock(&lru_evbuffers_mtx);
}

static void rem_lru_evbuffer_int(struct sqlclntstate *clnt)
{
    if (TAILQ_NEXT(clnt, lru_entry) != clnt) {
        TAILQ_REMOVE(&lru_evbuffers, clnt, lru_entry);
        TAILQ_NEXT(clnt, lru_entry) = clnt;
    }
}

void rem_lru_evbuffer(struct sqlclntstate *clnt)
{
    Pthread_mutex_lock(&lru_evbuffers_mtx);
    rem_lru_evbuffer_int(clnt);
    Pthread_mutex_unlock(&lru_evbuffers_mtx);
}

static int close_lru_evbuffer_int(struct sqlclntstate *self)
{
    struct sqlclntstate *clnt = TAILQ_LAST(&lru_evbuffers, lru_evbuffers);
    if (!clnt || clnt == self) {
        return -1;
    }
    rem_lru_evbuffer_int(clnt);
    clnt->plugin.close(clnt); /* newsql_close_evbuffer */
    return 0;
}

static int close_lru_evbuffer(struct sqlclntstate *self)
{
    Pthread_mutex_lock(&lru_evbuffers_mtx);
    int ret = close_lru_evbuffer_int(self);
    Pthread_mutex_unlock(&lru_evbuffers_mtx);
    return ret;
}

int add_appsock_connection_evbuffer(struct sqlclntstate *clnt)
{
    extern unsigned long long total_appsock_conns;
    if (clnt->admin) {
       return 0;
    }
    int max = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAXAPPSOCKSLIMIT);
    int warn = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_APPSOCKSLIMIT);
    int lim = warn < max ? warn : max;
    total_appsock_conns++;
    int current = ATOMIC_ADD32(active_appsock_conns, 1);
    time_metric_add(thedb->connections, current);
    if (current > lim) {
        static time_t last_trace = 0;
        time_t now = time(NULL);
        if (current > max) {
            if (close_lru_evbuffer(clnt) != 0) {
                return -1;
            }
        } else if (now - last_trace) {
            last_trace = now;
            logmsg(LOGMSG_WARN, "%s: SQL connection limit approaching (%d/%d)\n", __func__, current, max);
        }
    }
    return 0;
}

void rem_appsock_connection_evbuffer(struct sqlclntstate *clnt)
{
    if (clnt->admin) {
        return;
    }
    ATOMIC_ADD32(active_appsock_conns, -1);
}

static int internal_write_response(struct sqlclntstate *a, int b, void *c, int d)
{
    return 0;
}
static int internal_read_response(struct sqlclntstate *a, int b, void *c, int d)
{
    return -1;
}
static void *internal_save_stmt(struct sqlclntstate *clnt, void *arg)
{
    return strdup(clnt->sql);
}
static void *internal_restore_stmt(struct sqlclntstate *clnt, void *arg)
{
    clnt->sql = arg;
    return NULL;
}
static void *internal_destroy_stmt(struct sqlclntstate *clnt, void *arg)
{
    free(arg);
    return NULL;
}
static void *internal_print_stmt(struct sqlclntstate *clnt, void *arg)
{
    return arg;
}
static int internal_param_count(struct sqlclntstate *a)
{
    return 0;
}
static int internal_param_index(struct sqlclntstate *a, const char *b, int64_t *c)
{
    return -1;
}
static int internal_param_value(struct sqlclntstate *a, struct param_data *b, int c)
{
    return -1;
}
static int internal_override_count(struct sqlclntstate *a)
{
    return 0;
}
static int internal_override_type(struct sqlclntstate *a, int b)
{
    return 0;
}
static int internal_clr_cnonce(struct sqlclntstate *a)
{
    return -1;
}
static int internal_has_cnonce(struct sqlclntstate *a)
{
    return 0;
}
static int internal_set_cnonce(struct sqlclntstate *a)
{
    return -1;
}
static int internal_get_cnonce(struct sqlclntstate *a, snap_uid_t *b)
{
    return -1;
}
static int internal_get_snapshot(struct sqlclntstate *a, int *b, int *c)
{
    return -1;
}
static int internal_upd_snapshot(struct sqlclntstate *a)
{
    return -1;
}
static int internal_clr_snapshot(struct sqlclntstate *a)
{
    return -1;
}
static int internal_has_high_availability(struct sqlclntstate *a)
{
    return 0;
}
static int internal_set_high_availability(struct sqlclntstate *a)
{
    return -1;
}
static int internal_clr_high_availability(struct sqlclntstate *a)
{
    return -1;
}
static int internal_get_high_availability(struct sqlclntstate *a)
{
    return 0;
}
static int internal_has_parallel_sql(struct sqlclntstate *a)
{
    return 0;
}
static void internal_add_steps(struct sqlclntstate *a, double b)
{
}
static void internal_setup_client_info(struct sqlclntstate *a, struct sqlthdstate *b, char *c)
{
}
static int internal_skip_row(struct sqlclntstate *a, uint64_t b)
{
    return 0;
}
static int internal_log_context(struct sqlclntstate *a, struct reqlogger *b)
{
    reqlog_reset(b);
    return 0;
}
static uint64_t internal_get_client_starttime(struct sqlclntstate *a)
{
    return 0;
}
static int internal_get_client_retries(struct sqlclntstate *a)
{
    return 0;
}
static int internal_send_intrans_response(struct sqlclntstate *a)
{
    return 1;
}
static int internal_peer_check(struct sqlclntstate *a)
{
    return 0;
}
void *internal_get_authdata(struct sqlclntstate *a)
{
    return NULL;
}
static int internal_local_check(struct sqlclntstate *a)
{
    return 1;
}
static int internal_get_fileno(struct sqlclntstate *a)
{
    return -1;
}
static int internal_close(struct sqlclntstate *a)
{
    return -1;
}
static int internal_flush(struct sqlclntstate *a)
{
    return 0;
}
static int internal_has_ssl(struct sqlclntstate *a)
{
    return 0;
}
static int internal_has_x509(struct sqlclntstate *a)
{
    return 0;
}
static int internal_get_x509_attr(struct sqlclntstate *a, int b, void *c, int d)
{
    return -1;
}
static const char * internal_api_type(struct sqlclntstate *clnt)
{
    return "internal";
}

void start_internal_sql_clnt(struct sqlclntstate *clnt)
{
    reset_clnt(clnt, 1);
    plugin_set_callbacks(clnt, internal);
    clnt->dbtran.mode = TRANLEVEL_SOSQL;
    clr_high_availability(clnt);
}

int run_internal_sql_clnt(struct sqlclntstate *clnt, char *sql)
{
#ifdef DEBUGQUERY
    printf("run_internal_sql_clnt() sql '%s'\n", sql);
#endif
    clnt->sql = skipws(sql);
    int rc = dispatch_sql_query(clnt);
    if (rc || clnt->query_rc || clnt->saved_errstr) {
        if (clnt->query_rc)
            logmsg(LOGMSG_ERROR, "%s: Error from query: '%s' (rc = %d) \n", __func__, sql, clnt->query_rc);
        if (clnt->saved_errstr)
            logmsg(LOGMSG_ERROR, "%s: Error: '%s' \n", __func__, clnt->saved_errstr);
        rc = 1;
    }
    return rc;
}

static inline void init_internal_sql_clnt(struct sqlclntstate *clnt, struct schema_mem *sm) 
{
    start_internal_sql_clnt(clnt);
    
    if (sm) {
        clnt->verify_indexes = 1;
        clnt->schema_mems = (void *)sm;
    }
}

static inline void init_mem_info(struct mem_info *info, struct sqlclntstate *clnt, struct schema *sc,
                                 blob_buffer_t *outblob, const char *tzname,
                                 struct convert_failure *fail_reason) 
{
    if (!tzname || !tzname[0]) tzname = "America/New_York";
    
    info->s = sc;
    info->m = ((struct schema_mem *)clnt->schema_mems)->mout;
    info->tzname = tzname;
    info->outblob = outblob;
    info->maxblobs = MAXBLOBS;
    info->fail_reason = fail_reason;
    info->fldidx = -1;
}

int run_internal_sql_function(void *outbuf, struct field *dest, const char *sqlfn,
                              struct schema *sc, blob_buffer_t *outblob, const char *tzname,
                              struct convert_failure *fail_reason)
{   
    if (strlen(sqlfn) < 1) return -1;

    int rc = 0;
    struct sqlclntstate clnt = {0};
    struct schema_mem sm = {0};
    Mem mout = {0};
    sm.mout = &mout;
    
    init_internal_sql_clnt(&clnt, &sm);
    char *stmt = sqlite3_mprintf("select %s", sqlfn);
    
    if (run_internal_sql_clnt(&clnt, stmt)) {
        rc = -1;
        goto done;
    }

    struct mem_info info = {0};
    struct field_conv_opts_tz convopts = {.flags = 0};
    
    init_mem_info(&info, &clnt, sc, outblob, tzname, fail_reason); 
    info.convopts = &convopts;
    
    if (mem_to_ondisk(outbuf, dest, &info, NULL)) {
        rc = -1;
        goto done;
    }

done:
    if (clnt.schema_mems) {
        Mem *mout = ((struct schema_mem *)clnt.schema_mems)->mout;
        if (mout && mout->zMalloc) free(mout->zMalloc);
    }
    
    end_internal_sql_clnt(&clnt);
    sqlite3_free(stmt);
    
    return rc;
}

void end_internal_sql_clnt(struct sqlclntstate *clnt)
{
    cleanup_clnt(clnt);
}

void update_col_info(struct sql_col_info *info, int ncols)
{
    if (info->capacity < ncols) {
        size_t n = ncols + 32; /* Keep space for a few more than requested */
        size_t sz = n * sizeof(info->type[0]);
        if (!info->type) {
            info->type = calloc(n, sizeof(info->type[0])); /* o hi, valgrind */
        } else {
            info->type = realloc(info->type, sz);
        }
        if (!info->type) {
            logmsg(LOGMSG_FATAL, "%s realloc(%zu) errno:%d %s\n", __func__, sz,
                   errno, strerror(errno));
            abort();
        }
        info->capacity = n;
    }
    info->count = ncols;
}

void clnt_plugin_reset(struct sqlclntstate *clnt)
{
    struct plugin_callbacks *backup = &clnt->backup;
    struct plugin_callbacks *adapter_backup = &clnt->adapter_backup;

    clnt->adapter.column_count = adapter_backup->column_count;
    clnt->adapter.next_row = adapter_backup->next_row;
    clnt->adapter.column_type = adapter_backup->column_type;
    clnt->adapter.column_int64 = adapter_backup->column_int64;
    clnt->adapter.column_double = adapter_backup->column_double;
    clnt->adapter.column_text = adapter_backup->column_text;
    clnt->adapter.column_bytes = adapter_backup->column_bytes;
    clnt->adapter.column_blob = adapter_backup->column_blob;
    clnt->adapter.column_datetime = adapter_backup->column_datetime;
    clnt->adapter.column_interval = adapter_backup->column_interval;
    clnt->adapter.sqlite_error = adapter_backup->sqlite_error;
    clnt->adapter.param_count = adapter_backup->param_count;
    clnt->adapter.param_value = adapter_backup->param_value;
    clnt->adapter.param_index = adapter_backup->param_index;

    clnt->plugin.column_count = backup->column_count;
    clnt->plugin.next_row = backup->next_row;
    clnt->plugin.column_type = backup->column_type;
    clnt->plugin.column_int64 = backup->column_int64;
    clnt->plugin.column_double = backup->column_double;
    clnt->plugin.column_text = backup->column_text;
    clnt->plugin.column_bytes = backup->column_bytes;
    clnt->plugin.column_blob = backup->column_blob;
    clnt->plugin.column_datetime = backup->column_datetime;
    clnt->plugin.column_interval = backup->column_interval;
    clnt->plugin.sqlite_error = backup->sqlite_error;
    clnt->plugin.param_count = backup->param_count;
    clnt->plugin.param_value = backup->param_value;
    clnt->plugin.param_index = backup->param_index;
}

void exhausted_appsock_connections(struct sqlclntstate *clnt)
{
    write_response(clnt, RESPONSE_ERROR, "Exhausted appsock connections.", CDB2__ERROR_CODE__APPSOCK_LIMIT);
    gbl_denied_appsock_connection_count++;
    static time_t last = 0;
    time_t now = time(NULL);
    if (now != last) {
        logmsg(LOGMSG_USER,
               "%s: Exhausted appsock connections, total %d connections denied-connection count=%" PRId64 "\n",
               __func__, active_appsock_conns, gbl_denied_appsock_connection_count);
        last = now;
    }
}

int maxquerytime_cb(struct sqlclntstate *clnt)
{
    clnt->statement_timedout = 1;
    return write_response(clnt, RESPONSE_ERROR, (char *)sqlite3ErrStr(SQLITE_TIMEDOUT), CDB2ERR_QUERYLIMIT);
}

/* Sqlite prototypes this as i64, since it insists on C89 compatibility, which precludes
 * using int64_t.  We'll just make the assumption that i64 and int64_t are the same underlying
 * type. */
int64_t comdb2_last_stmt_cost(void) {
   struct sql_thread *thd = pthread_getspecific(query_info_key);
   if (!thd)
      return -1;

   return thd->clnt ? thd->clnt->last_cost : -1;
}

char *clnt_tzname(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    if (clnt->plugin.tzname)
        return clnt->plugin.tzname(clnt, stmt);

    return stmt_tzname(stmt);
}

int gbl_transaction_grace_period = 60;
void wait_for_transactions(void) {
    int ntrans;
    int nwaits = 0;


    for (nwaits = 0; nwaits < gbl_transaction_grace_period; nwaits++) {
        ntrans = 0;
        struct connection_info *connections;
        int nconnections;

        int rc = gather_connection_info(&connections, &nconnections);
        if (rc) {
            logmsg(LOGMSG_ERROR, "failed to get connection info\n");
            return;
        }
        for (int i = 0; i < nconnections; i++) {
            struct connection_info *c = &connections[i];
            if (c->in_transaction) {
                ntrans++;
                if (nwaits > 10) {
                    if (ntrans == 1)
                        logmsg(LOGMSG_INFO, "waiting for transactions:\n------------------------\n");
                    logmsg(LOGMSG_INFO, "open transaction on connection from %s pid %d\n", c->host, (int) c->pid);
                }
            }
        }
        free_connection_info(connections, nconnections);
        if (ntrans) {
            if (nwaits > 10)
                logmsg(LOGMSG_INFO, "------------------------\n");
            sleep(1);
            nwaits++;
        }
        else
            break;
    }
    if (ntrans && nwaits)
        logmsg(LOGMSG_INFO, "giving up and exiting with pending transactions\n");
}
