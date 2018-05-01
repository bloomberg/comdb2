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

#ifdef __sun
#define __EXTENSIONS__
#endif
#include <limits.h> // PTHREAD_STACK_MIN
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <ctype.h>
#include <arpa/inet.h>
#include <alloca.h>
#include <poll.h>
#include <unistd.h>

#include <sqlite3.h>
#include <sqliteInt.h>
#include <comdb2.h>
#include <sqlglue.h>
#include <types.h>
#include <sql.h>
#include <sqlinterfaces.h>
#include <sqloffload.h>
#include <flibc.h>
#include <plbitlib.h>
#include <sqlresponse.pb-c.h>
#include <str0.h>

#include <bdb_api.h>
#include <bdb_queue.h>
#include <strbuf.h>

#include <cson_amalgamation_core.h>
#include <translistener.h>
#include <net_types.h>
#include <locks.h>
#include <trigger.h>
#include <thread_malloc.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <sp_int.h>
#include <luaglue.h>
#include <luautil.h>
#include <logmsg.h>
#include <util.h>

extern int gbl_dump_sql_dispatched; /* dump all sql strings dispatched */
extern int gbl_return_long_column_names;
extern int gbl_max_sqlcache;
extern int gbl_lua_new_trans_model;
extern int gbl_max_lua_instructions;
extern int gbl_lua_version;
extern int gbl_break_lua;

char *gbl_break_spname;
void *debug_clnt;

pthread_mutex_t lua_debug_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t lua_debug_cond = PTHREAD_COND_INITIALIZER;

struct tmptbl_info_t {
    int rootpg;
    char *sql;
    char name[MAXTABLELEN * 2]; // namespace.name
    pthread_mutex_t *lk;
    LIST_ENTRY(tmptbl_info_t) entries;
};

typedef struct {
    DBTYPES_COMMON;
} db_t;

typedef struct {
    DBTYPES_COMMON;
    SP parent;
    int is_temp_tbl;
    char table_name[MAXTABLELEN * 2]; // namespace.name
} dbtable_t;

struct dbstmt_t {
    DBTYPES_COMMON;
    sqlite3_stmt *stmt;
    int rows_changed;
    uint16_t num_tbls;
    uint8_t fetched;
    uint8_t initial; // 1: stmt tables are locked
    struct sql_state *rec; // only db:prepare will set
    LIST_ENTRY(dbstmt_t) entries;
};

typedef struct {
    DBTYPES_COMMON;
    pthread_mutex_t lua_thread_mutex;
    pthread_cond_t lua_thread_cond;
    int finished_run;
    pthread_t lua_tid;
    char *sql;
    Lua lua;
    SP sp;
    char error[128];
    struct sqlclntstate *clnt;
} dbthread_t;

typedef struct {
    DBTYPES_COMMON;
    struct ireq iq;
    struct consumer *consumer;
    genid_t genid;

    /* signaling from libdb on qdb insert */
    pthread_mutex_t *lock;
    pthread_cond_t *cond;
    const uint8_t *open;
    trigger_reg_t info; // must be last in struct
} dbconsumer_t;

struct qfound {
    struct bdb_queue_found *item;
    size_t len;
    size_t dtaoff;
};

static int db_reset(Lua);
static SP create_sp(char **err);
static int push_trigger_args_int(Lua, dbconsumer_t *, struct qfound *, char **);
static void reset_sp(SP);

#define getdb(x) (x)->thd->sqldb
#define dbconsumer_sz(spname)                                                  \
    (sizeof(dbconsumer_t) - sizeof(trigger_reg_t) + trigger_reg_sz(spname))

static int setup_dbconsumer(dbconsumer_t *q, struct consumer *consumer,
                            struct dbtable *qdb, trigger_reg_t *info)
{
    init_fake_ireq(thedb, &q->iq);
    int spname_len = htonl(info->spname_len);
    q->iq.usedb = qdb;
    q->consumer = consumer;
    q->info = *info;
    // memcpy because variable size struct breaks fortify checks in strcpy.
    memcpy(q->info.spname, info->spname, spname_len + 1);
    strcpy(q->info.spname + spname_len + 1, info->spname + spname_len + 1);
    return bdb_trigger_subscribe(qdb->handle, &q->cond, &q->lock, &q->open);
}

static int db_emiterror(lua_State *lua);
static int db_emit(lua_State *lua);
static int db_column_name(lua_State *lua);
static int db_column_type(lua_State *lua);
static int db_num_columns(lua_State *lua);
static int db_trace(lua_State *lua);
static int db_print(lua_State *lua);
static int db_isnull(lua_State *lua);
static int db_setnull(Lua);
static int db_settyped(Lua);
static int db_setmaxinstructions(lua_State *lua);
static int db_getinstructioncount(lua_State *lua);
static int db_now(Lua lua);
static int db_copyrow(lua_State *lua);
static int db_settimezone(lua_State *lua);
static int db_gettimezone(Lua L);
static int db_bind(Lua L);
static int db_exec(Lua);
static int dbstmt_emit(Lua);
static int db_prepare(Lua lua);
static int db_create_thread(Lua L);
static int db_table(lua_State *lua);
static int db_cast(Lua L);
static int db_sleep(Lua lua);
static int db_csv_to_table(Lua L);
static int db_json_to_table(Lua lua);
static int db_table_to_json(Lua L);
static int db_get_trans(Lua lua);
static int db_NULL(Lua L);
static int db_sqlerror(lua_State *lua);
static int db_debug(lua_State *lua);
static int db_db_debug(Lua lua);
static int db_begin(Lua L);
static int db_commit(Lua L);
static int db_rollback(Lua L);
static int db_bootstrap(Lua);

static const luaL_Reg tran_funcs[] = {
    {"begin", db_begin},
    {"commit", db_commit},
    {"rollback", db_rollback},
    {"create_thread", db_create_thread}, // not tran func; same visibility
    {NULL, NULL}};

/*
** n1: namespace - main, temp, fdb ...
** n2: unqualified table name
*/
static int two_part_tbl_name(const char *name, char *n1, char *n2)
{
    char *dot;
    if ((dot = strstr(name, ".")) != NULL) {
        if (dot - name >= MAXTABLELEN) {
            return -1;
        }
        strncpy0(n1, name, dot - name + 1);
        ++dot;
        if (strlen(dot) >= MAXTABLELEN) {
            return -1;
        }
        strcpy(n2, dot);
    } else {
        if (strlen(name) >= MAXTABLELEN) {
            return -1;
        }
        strcpy(n1, "");
        strcpy(n2, name);
    }
    return 0;
}

static void query_tbl_name(const char *name, char *n1, char *sep, char *n2)
{
    two_part_tbl_name(name, n1, n2);
    if (strcmp(n1, "") == 0)
        strcpy(sep, "");
    else
        strcpy(sep, ".");
}

//////////////////////////////////
// Borrowed from sqlite/shell.c //
//////////////////////////////////

/*
** An object used to read a CSV file
*/
typedef struct CSVReader CSVReader;
struct CSVReader {
    // const char *zFile;  /* Name of the input file */
    // FILE *in;           /* Read the CSV text from this input stream */
    Lua lua;
    const char *in;
    size_t pos;
    char *z;        /* Accumulated text for a field */
    int n;          /* Number of bytes in z */
    int nAlloc;     /* Space allocated for z[] */
    int nLine;      /* Current line number */
    int cTerm;      /* Character that terminated the most recent field */
    int cSeparator; /* The separator character.  (Usually ",") */
};

/* Append a single byte to z[] */
static void csv_append_char(CSVReader *p, int c)
{
    if (p->n + 1 >= p->nAlloc) {
        p->nAlloc += p->nAlloc + 100;
        p->z = realloc(p->z, p->nAlloc);
        if (p->z == 0) {
            logmsg(LOGMSG_FATAL, "out of memory\n");
            exit(1);
        }
    }
    p->z[p->n++] = (char)c;
}

/* Read a single field of CSV text.  Compatible with rfc4180 and extended
** with the option of having a separator other than ",".
**
**   +  Input comes from p->in.
**   +  Store results in p->z of length p->n.  Space to hold p->z comes
**      from sqlite3_malloc().
**   +  Use p->cSep as the separator.  The default is ",".
**   +  Keep track of the line number in p->nLine.
**   +  Store the character that terminates the field in p->cTerm.  Store
**      EOF on end-of-file.
**   +  Report syntax errors on stderr
*/
static char *csv_read_one_field(CSVReader *p)
{
    int c, pc, ppc;
    int cSep = p->cSeparator;
    p->n = 0;
    // c = fgetc(p->in);
    c = p->in[p->pos++];
    if (c == 0 /* || seenInterrupt */) {
        p->cTerm = 0;
        return 0;
    }
    if (c == '"') {
        int startLine = p->nLine;
        int cQuote = c;
        pc = ppc = 0;
        while (1) {
            // c = fgetc(p->in);
            c = p->in[p->pos++];
            if (c == '\n') p->nLine++;
            if (c == cQuote) {
                if (pc == cQuote) {
                    pc = 0;
                    continue;
                }
            }
            if ((c == cSep && pc == cQuote) || (c == '\n' && pc == cQuote) ||
                (c == '\n' && pc == '\r' && ppc == cQuote) ||
                (c == 0 && pc == cQuote)) {
                do {
                    p->n--;
                } while (p->z[p->n] != cQuote);
                p->cTerm = c;
                break;
            }
            if (pc == cQuote && c != '\r') {
                free(p->z);
                luabb_error(p->lua, NULL,
                            "CSV line:%d: unescaped %c character\n", p->nLine,
                            cQuote);
            }
            if (c == 0) {
                free(p->z);
                luabb_error(p->lua, NULL,
                            "CSV line:%d: unterminated %c-quoted field\n",
                            p->nLine, startLine, cQuote);
                p->cTerm = 0;
                break;
            }
            csv_append_char(p, c);
            ppc = pc;
            pc = c;
        }
    } else {
        while (c != 0 && c != cSep && c != '\n') {
            csv_append_char(p, c);
            // c = fgetc(p->in);
            c = p->in[p->pos++];
        }
        if (c == '\n') {
            p->nLine++;
            if (p->n > 0 && p->z[p->n - 1] == '\r') p->n--;
        }
        p->cTerm = c;
    }
    if (p->z) p->z[p->n] = 0;
    return p->z;
}

static int check_register_condition(Lua L, dbconsumer_t *q)
{
    if (q->info.elect_cookie == htonl(gbl_master_changes)) {
        return 0;
    }

    logmsg(LOGMSG_ERROR,
           "%s stale cookie -- register with master name:%s cookie:%d\n",
           __func__, q->info.spname, q->info.elect_cookie);
    return 1;
}

static int check_retry_conditions(Lua L, int initial)
{
    SP sp = getsp(L);

    if (thedb->stopped || thedb->exiting) {
        luabb_error(L, sp, "database exiting");
        return -1;
    }

    if (peer_dropped_connection(sp->clnt)) {
        luabb_error(L, sp, "client disconnect");
        return -2;
    }

    if (bdb_lock_desired(thedb->bdb_env)) {
        int rc;
        if ((rc = recover_deadlock(thedb->bdb_env, sp->thd->sqlthd, NULL, 0)) !=
            0) {
            luabb_error(L, sp, "recover deadlock failed");
            return -3;
        }
    }

    // Server goes incoherent during election.
    // Mask failure during registration.
    if (initial) {
        return 0;
    }

    if (!bdb_am_i_coherent(thedb->bdb_env)) {
        luabb_error(L, sp, "incoherent server");
        return -4;
    }

    return 0;
}

static int luabb_trigger_register(Lua L, trigger_reg_t *reg)
{
    logmsg(LOGMSG_DEBUG,
           "%s waiting for %s elect_cookie:%d trigger_cookie:0x%lx\n",
           __func__, reg->spname, ntohl(reg->elect_cookie), reg->trigger_cookie);
    int rc;
    SP sp = getsp(L);
    while ((rc = trigger_register_req(reg)) != CDB2_TRIG_REQ_SUCCESS) {
        if (check_retry_conditions(L, 1) != 0) {
            return luabb_error(L, sp, sp->error);
        }
        switch (rc) {
        case NET_SEND_FAIL_TIMEOUT:
        case NET_SEND_FAIL_INVALIDNODE:
        case CDB2_TRIG_ASSIGNED_OTHER:
        case CDB2_TRIG_NOT_MASTER: sleep(1); break;
        default:
            luabb_error(L, sp, "failed to register trigger:%s rc:%d",
                        reg->spname, rc);
            return -1;
        }
    }
    logmsg(LOGMSG_DEBUG, "%s rc:%d %s elect_cookie:%d trigger_cookie:0x%lx\n",
           __func__, rc, reg->spname, ntohl(reg->elect_cookie),
           reg->trigger_cookie);
    return rc;
}

static void luabb_trigger_unregister(dbconsumer_t *q)
{
    if (q->lock) {
        pthread_mutex_lock(q->lock);
        if (*q->open) {
            bdb_trigger_unsubscribe(q->iq.usedb->handle);
        }
        pthread_mutex_unlock(q->lock);
    }

    logmsg(LOGMSG_DEBUG,
           "%s waiting for %s elect_cookie:%d trigger_cookie:0x%lx\n",
           __func__, q->info.spname, ntohl(q->info.elect_cookie),
           q->info.trigger_cookie);
    int rc;
    int retry = 10;
    do {
        --retry;
        rc = trigger_unregister_req(&q->info);
        switch (rc) {
        case NET_SEND_FAIL_INTERNAL:
        case NET_SEND_FAIL_TIMEOUT:
        case NET_SEND_FAIL_INVALIDNODE:
        case CDB2_TRIG_NOT_MASTER:
            // retry only if couldn't reach master
            break;
        default: retry = 0; break;
        }
    } while (retry > 0);
    logmsg(LOGMSG_DEBUG, "%s rc:%d %s elect_cookie:%d trigger_cookie:0x%lx\n",
           __func__, rc, q->info.spname, ntohl(q->info.elect_cookie),
           q->info.trigger_cookie);
}

static void ping(Lua L)
{
    SP sp = getsp(L);
    sp->pingpong = 1;
}

static void pong(Lua L)
{
    SP sp = getsp(L);
    struct sqlclntstate *clnt = sp->clnt;
    if (clnt->read_response(clnt, RESPONSE_PING_PONG, NULL, 0) != 0) {
        luaL_error(L, "client protocol error");
    }
    sp->pingpong = 0;
}

static int dbtype_to_client_type(lua_dbtypes_t *t)
{
    switch (t->dbtype) {
    case DBTYPES_INTEGER: return SQLITE_INTEGER;
    case DBTYPES_DECIMAL:
    case DBTYPES_CSTRING: return SQLITE_TEXT;
    case DBTYPES_REAL: return SQLITE_FLOAT;
    case DBTYPES_DATETIME: {
        lua_datetime_t *ldt = (lua_datetime_t *)t;
        return (ldt->val.prec == DTTZ_PREC_MSEC) ? SQLITE_DATETIME
                                                 : SQLITE_DATETIMEUS;
    }
    case DBTYPES_BLOB: return SQLITE_BLOB;
    case DBTYPES_INTERVALYM: return SQLITE_INTERVAL_YM;
    case DBTYPES_INTERVALDS: {
        lua_intervalds_t *lds = (lua_intervalds_t *)t;
        return (lds->val.u.ds.prec == DTTZ_PREC_MSEC) ? SQLITE_INTERVAL_DS
                                                      : SQLITE_INTERVAL_DSUS;
    }
    default: return -1;
    }
}

static int lua_to_client_type(Lua lua, int idx)
{
    if (lua_isnumber(lua, idx)) {
        return SQLITE_FLOAT;
    } else if (lua_isstring(lua, idx)) {
        return SQLITE_TEXT;
    } else if (lua_type(lua, idx) == LUA_TUSERDATA) {
        lua_dbtypes_t *t = lua_touserdata(lua, idx);
        return dbtype_to_client_type(t);
    } else {
        return -1;
    }
}

#define col_to_idx(narg, col) lua_gettop(L) - narg + col + 1

int sp_column_type(struct response_data *arg, int col, size_t typed_stmt, int type)
{
    SP parent = arg->sp->parent;
    if (typed_stmt) {
        parent->clnttype[col] = type;
        return type;
    }
    if (parent->clnttype[col] > 0) {
        return parent->clnttype[col];
    }
    if (type > 0) {
        parent->clnttype[col] = type;
        return type;
    }
    Lua L = arg->sp->lua;
    int idx = col_to_idx(arg->ncols, col);
    type = lua_to_client_type(L, idx);
    if (type == -1) {
        type = SQLITE_TEXT;
    }
    parent->clnttype[col] = type;
    return type;
}

int sp_column_nil(struct response_data *arg, int col)
{
    SP sp = arg->sp;
    Lua L = sp->lua;
    int idx = col_to_idx(arg->ncols, col);
    return lua_isnil(L, idx) || luabb_isnull(L, idx);
}

int sp_column_val(struct response_data *arg, int col, int type, void *out)
{
    SP sp = arg->sp;
    Lua L = sp->lua;
    int idx = col_to_idx(arg->ncols, col);
    switch (type) {
    case SQLITE_INTEGER: luabb_tointeger(L, idx, out); return 0;
    case SQLITE_FLOAT: luabb_toreal(L, idx, out); return 0;
    case SQLITE_DATETIME: /* fall through */
    case SQLITE_DATETIMEUS: luabb_todatetime(L, idx, out); return 0;
    case SQLITE_INTERVAL_YM: luabb_tointervalym(L, idx, out); return 0;
    case SQLITE_INTERVAL_DS: /* fall through */
    case SQLITE_INTERVAL_DSUS: luabb_tointervalds(L, idx, out); return 0;
    }
    return -1;
}

void *sp_column_ptr(struct response_data *arg, int col, int type, size_t *len)
{
    SP sp = arg->sp;
    Lua L = sp->lua;
    int idx = col_to_idx(arg->ncols, col);
    char *c;
    blob_t b;
    lua_cstring_t *cs;
    switch (type) {
    case SQLITE_TEXT:
        switch (luabb_type(L, idx)) {
        case DBTYPES_LNUMBER:
        case DBTYPES_LSTRING:
            c = (char *)lua_tolstring(L, idx, len);
            break;
        case DBTYPES_CSTRING:
            cs = lua_touserdata(L, idx);
            c = cs->val;
            *len = strlen(c);
            break;
        default:
            c = strdup(luabb_tostring(L, idx));
            *len = strlen(c);
            luabb_pushcstring_dl(L, c);
            lua_replace(L, idx);
        }
        return c;
    case SQLITE_BLOB:
        luabb_toblob(L, idx, &b);
        if (luabb_type(L, idx) != DBTYPES_BLOB) {
            luabb_pushblob_dl(L, &b);
            lua_replace(L, idx);
        }
        *len = b.length;
        return b.data;
    }
    return NULL;
}

char *sp_column_name(struct response_data *arg, int col)
{
    SP parent = arg->sp->parent;
    if (parent->clntname[col] == NULL) {
        sqlite3_stmt *stmt = arg->stmt;
        if (stmt) {
            parent->clntname[col] = strdup(sqlite3_column_name(stmt, col));
        } else {
            size_t n = snprintf(NULL, 0, "$%d", col);
            char *name = malloc(n + 1);
            snprintf(name, n + 1, "$%d", col);
            parent->clntname[col] = name;
        }
    }
    return parent->clntname[col];
}

static int dbq_pushargs(Lua L, dbconsumer_t *q, struct qfound *f)
{
    char *err;
    int rc = push_trigger_args_int(L, q, f, &err);
    free(f->item);
    if (rc != 1) {
        SP sp = getsp(L);
        luabb_error(L, sp, err);
        free(err);
    }
    return rc;
}

static const int dbq_delay = 1000; // ms
// Call with q->lock held.
// Unlocks q->lock on return.
// Returns  -2:stopped -1:error  0:IX_NOTFND  1:IX_FND
// If IX_FND will push Lua table on stack.
static int dbq_poll_int(Lua L, dbconsumer_t *q)
{
    struct qfound f = {0};
    int rc = dbq_get(&q->iq, 0, NULL, (void**)&f.item, &f.len, &f.dtaoff, NULL, NULL);
    pthread_mutex_unlock(q->lock);
    getsp(L)->num_instructions = 0;
    if (rc == 0) {
        return dbq_pushargs(L, q, &f);
    }
    if (rc == IX_NOTFND) {
        return 0;
    }
    return -1;
}

static int dbq_poll(Lua L, dbconsumer_t *q, int delay)
{
    SP sp = getsp(L);
    while (1) {
        int rc;
        if ((rc = check_retry_conditions(L, 0)) != 0) {
            return rc;
        }
        if ((rc = check_register_condition(L, q)) != 0) {
            if (luabb_trigger_register(L, &q->info) != CDB2_TRIG_REQ_SUCCESS) {
                return -1;
            }
        }
        pthread_mutex_lock(q->lock);
again:  if (*q->open) {
            rc = dbq_poll_int(L, q); // call will release q->lock
        } else {
            pthread_mutex_unlock(q->lock);
            rc = -2;
        }
        if (rc == 1) {
            return rc;
        }
        if (rc < 0) {
            luabb_error(L, sp, "failed to read from:%s rc:%d", q->info.spname, rc);
            return rc;
        }
        delay -= dbq_delay;
        if (delay < 0) {
            return 0;
        }
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += (dbq_delay / 1000);
        pthread_mutex_lock(q->lock);
        if (pthread_cond_timedwait(q->cond, q->lock, &ts) == 0) {
            // was woken up -- try getting from queue
            goto again;
        }
        pthread_mutex_unlock(q->lock);
    }
}

// this call will block until queue item available
static int dbconsumer_get_int(Lua L, dbconsumer_t *q)
{
    int rc;
    while ((rc = dbq_poll(L, q, dbq_delay)) == 0)
        ;
    return rc;
}

static int dbconsumer_get(Lua L)
{
    dbconsumer_t *q = luaL_checkudata(L, 1, dbtypes.dbconsumer);
    int rc;
    if ((rc = dbconsumer_get_int(L, q)) > 0) return rc;
    return luaL_error(L, getsp(L)->error);
}

static int dbconsumer_poll(Lua L)
{
    dbconsumer_t *q = luaL_checkudata(L, 1, dbtypes.dbconsumer);
    lua_Number arg = luaL_checknumber(L, 2);
    lua_Integer delay; // ms
    lua_number2integer(delay, arg);
    delay += (dbq_delay - delay % dbq_delay); // multiple of dbq_delay
    int rc = dbq_poll(L, q, delay);
    if (rc >= 0) {
        return rc;
    }
    return luaL_error(L, getsp(L)->error);
}

static inline int push_and_return(Lua L, int rc)
{
    lua_pushinteger(L, rc);
    return 1;
}

static const char *begin_parent(Lua);
static const char *commit_parent(Lua);

static int in_parent_trans(SP sp)
{
    return (sp->in_parent_trans || !sp->make_parent_trans);
}

static int lua_trigger_impl(Lua L, dbconsumer_t *q)
{
    SP sp = getsp(L);
    struct sqlclntstate *clnt = sp->clnt;
    if (!clnt->intrans) {
        int rc;
        if ((rc = osql_sock_start(clnt, OSQL_SOCK_REQ, 0)) != 0) {
            return rc;
        }
        clnt->intrans = 1;
    }
    clnt->ctrl_sqlengine = SQLENG_INTRANS_STATE;
    return osql_dbq_consume_logic(clnt, q->info.spname, q->genid);
}

/*
** (1) No explicit db:begin()
** (2) Have explicit db:begin(), but no writes yet.
** Start a new transaction in either case.
** Commit transaction only for (1)
*/
static int lua_consumer_impl(Lua L, dbconsumer_t *q)
{
    int rc = 0;
    SP sp = getsp(L);
    int start = in_parent_trans(sp);
    struct sqlclntstate *clnt = sp->clnt;
    if (start || clnt->intrans == 0) {
        if ((rc = osql_sock_start(clnt, OSQL_SOCK_REQ, 0)) != 0) {
            luaL_error(L, "%s osql_sock_start rc:%d\n", __func__, rc);
        }
        clnt->intrans = 1;
    }
    if ((rc = osql_dbq_consume_logic(clnt, q->info.spname, q->genid)) != 0) {
        if (start) {
            osql_sock_abort(clnt, OSQL_SOCK_REQ);
        }
        luaL_error(L, "%s osql_dbq_consume_logic rc:%d\n", __func__, rc);
    }
    if (start) {
        if ((rc = osql_sock_commit(clnt, OSQL_SOCK_REQ)) != 0) {
            luaL_error(L, "%s osql_sock_commit rc:%d\n", __func__, rc);
        }
    } else {
        clnt->ctrl_sqlengine = SQLENG_INTRANS_STATE;
    }
    return rc;
}

static int dbconsumer_consume_int(Lua L, dbconsumer_t *q)
{
    //check_register_condition(L, q);
    if (q->genid == 0) {
        return -1;
    }
    enum consumer_t type = consumer_type(q->consumer);
    int rc = (type == CONSUMER_TYPE_LUA) ? lua_trigger_impl(L, q)
                                         : lua_consumer_impl(L, q);
    q->genid = 0;
    return rc;
}

static int dbconsumer_consume(Lua L)
{
    dbconsumer_t *q = luaL_checkudata(L, 1, dbtypes.dbconsumer);
    return push_and_return(L, dbconsumer_consume_int(L, q));
}

static int db_emit_int(Lua);
static int dbconsumer_emit(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.dbconsumer);
    lua_remove(L, 1);

    ping(L);
    int rc = db_emit_int(L);
    pong(L);
    return rc;
}

static int dbconsumer_free(Lua L)
{
    dbconsumer_t *q = luaL_checkudata(L, 1, dbtypes.dbconsumer);
    luabb_trigger_unregister(q);
    return 0;
}

static int l_global_undef(lua_State *lua)
{
    lua_pushnumber(lua, -1);
    return luaL_error(lua, "Global variables not allowed.");
}

int to_positive_index(Lua L, int idx)
{
    if (idx < 0) {
        idx = lua_gettop(L) + idx + 1;
    }
    return idx;
}

static void *lua_mem_init()
{
    /* We used to start with 1MB - this isn't quite necessary
       as comdb2_malloc pre-allocation is much smarter now.
       We also name it "LUA" (uppercase) to differentiate it
       from those implicitly created per-thread allocators
       whose names are "lua" (lowercase). Those allocators are
       mainly used to bootstrap Lua environment. */
    void *mspace = comdb2ma_create(0, 0, "LUA", COMDB2MA_MT_UNSAFE);
    if (mspace == NULL) {
        logmsg(LOGMSG_FATAL, "%s: comdb2ma_create failed\n", __func__);
        exit(1);
    }
    return mspace;
}

static void free_tmptbl(SP sp, tmptbl_info_t *tbl)
{
    LIST_REMOVE(tbl, entries);
    if (sp->parent == sp) {
        pthread_mutex_destroy(tbl->lk);
        free(tbl->lk);
    }
    free(tbl->sql);
    free(tbl);
}

static void free_tmptbls(SP sp)
{
    tmptbl_info_t *tbl, *tmp;
    LIST_FOREACH_SAFE(tbl, &sp->tmptbls, entries, tmp)
    {
        free_tmptbl(sp, tbl);
    }
    LIST_INIT(&sp->tmptbls);
}

static void reset_dbtable(dbtable_t *dbtable)
{
    bzero(dbtable, sizeof(dbtable_t));
    init_new_t(dbtable, DBTYPES_DBTABLE);
}

static int lua_check_errors(struct sqlclntstate *clnt, sqlite3 *sqldb,
                            sqlite3_stmt *stmt, const char **errstr)
{
    int rc = sql_check_errors(clnt, sqldb, stmt, errstr);
    if (rc) {
        rc = sqlserver2sqlclient_error(rc);
    } else {
        rc = errstat_get_rc(&clnt->osql.xerr);
        if (rc) {
            *errstr = errstat_get_str(&clnt->osql.xerr);
        }
    }
    return rc;
}

/*
** Call this with parent emit mutex held
** Will allocate space for 'ntypes' columns
*/
static void new_col_info(SP sp, int ntypes)
{
    SP parent = sp->parent;
    int prev = parent->ntypes;
    if (prev >= ntypes) {
        return;
    }
    parent->clntname =
        realloc(parent->clntname, ntypes * sizeof(parent->clntname[0]));
    parent->clnttype =
        realloc(parent->clnttype, ntypes * sizeof(parent->clnttype[0]));
    for (int i = prev; i < ntypes; ++i) {
        parent->clntname[i] = NULL;
        parent->clnttype[i] = -1;
    }
    parent->ntypes = ntypes;
}

/* TODO: delete it once typestr_to_type starts giving decimals. */
static int sqlite_str_to_type(const char *ctype)
{
    if (ctype == NULL) return SQLITE_TEXT;
    if ((strcmp("smallint", ctype) == 0) || (strcmp("int", ctype) == 0) ||
        (strcmp("largeint", ctype) == 0))
        return SQLITE_INTEGER;
    else if ((strcmp("smallfloat", ctype) == 0) ||
             (strcmp("float", ctype) == 0))
        return SQLITE_FLOAT;
    else if (strncmp("char", ctype, 4) == 0)
        return SQLITE_TEXT;
    else if (strncmp("blob", ctype, 4) == 0)
        return SQLITE_BLOB;
    else if (strncmp("datetime", ctype, 8) == 0)
        return SQLITE_DATETIME;
    else if (strncmp("datetimeus", ctype, 8) == 0)
        return SQLITE_DATETIMEUS;
    else if (strstr(ctype, "year"))
        return SQLITE_INTERVAL_YM;
    else if (strstr(ctype, "day"))
        return SQLITE_INTERVAL_DS;
    else if (strncmp("deci", ctype, 4) == 0)
        return SQLITE_DECIMAL;
    else {
        return SQLITE_TEXT;
    }
}

static dbtypes_enum sqlite_type_to_dbtype(int sqltype)
{
    switch (sqltype) {
    case SQLITE_INTEGER: return DBTYPES_INTEGER;
    case SQLITE_FLOAT: return DBTYPES_REAL;
    case SQLITE_BLOB: return DBTYPES_BLOB;
    case SQLITE_DATETIME: return DBTYPES_DATETIME;
    case SQLITE_DATETIMEUS: return DBTYPES_DATETIME;
    case SQLITE_INTERVAL_YM: return DBTYPES_INTERVALYM;
    case SQLITE_INTERVAL_DS: return DBTYPES_INTERVALDS;
    case SQLITE_INTERVAL_DSUS: return DBTYPES_INTERVALDS;
    case SQLITE_DECIMAL: return DBTYPES_DECIMAL;
    case SQLITE_TEXT:
    default: return DBTYPES_CSTRING;
    }
}

static int check_param_count(struct sqlclntstate *clnt, int num)
{
    CDB2SQLQUERY *query = clnt->sql_query;
    if ((query && query->n_types && query->n_types != num) ||
        (clnt->req.parm && clnt->req.parm != num)) {
        /* cdb2api or comdb2api params exist and don't match number of args */
        return -1;
    }
    return 0;
}

static int check_param_range(struct sqlclntstate *clnt, int num)
{
    CDB2SQLQUERY *query = clnt->sql_query;
    if ((query && query->n_types && query->n_types > num) ||
        clnt->req.parm > num) {
        return -1;
    }
    return 0;
}

static int typed_stmt(struct sqlclntstate *clnt)
{
    CDB2SQLQUERY *query = clnt->sql_query;
    if ((query && query->n_types) || clnt->req.parm) {
        return 1;
    }
    return 0;
}

static void donate_stmt(SP sp, dbstmt_t *dbstmt)
{
    sqlite3_stmt *stmt = dbstmt->stmt;
    if (stmt == NULL) return;

    if (!gbl_enable_sql_stmt_caching || !dbstmt->rec) {
        sqlite3_finalize(stmt);
    } else {
        put_prepared_stmt(sp->thd, sp->clnt, dbstmt->rec, sp->rc);
    }
    if (dbstmt->num_tbls) {
        LIST_REMOVE(dbstmt, entries);
    }
    free(dbstmt->rec);
    dbstmt->rec = NULL;
    dbstmt->stmt = NULL;
    dbstmt->num_tbls = 0;
}

static int enable_global_variables(lua_State *lua)
{
    /* Override the old meta table. */
    lua_createtable(lua, 0, 2);
    lua_setmetatable(lua, LUA_GLOBALSINDEX);
    return 0;
}

static int lua_prepare_sql(Lua, SP, const char *sql, sqlite3_stmt **);

/*
** Lua stack:
** 1: Lua str (tmptbl name)
** 2: Lua tbl (tmptbl schema)
*/
static int create_temp_table(Lua lua, pthread_mutex_t **lk, const char **name)
{
    int rc = -1;
    strbuf *sql = NULL;
    SP sp = getsp(lua);
    char n1[MAXTABLELEN], n2[MAXTABLELEN];
    *name = lua_tostring(lua, 1);
    if (two_part_tbl_name(*name, n1, n2) != 0) {
        luabb_error(lua, sp, "bad table name:%s", *name);
        goto out;
    }
    if (strcasecmp(n1, "") != 0 && strcasecmp(n1, "temp") != 0) {
        luabb_error(lua, sp, "bad table name:%s", *name);
        goto out;
    }
    sql = strbuf_new();
    strbuf_appendf(sql, "CREATE TEMP TABLE \"%s\" (", n2);
    size_t num = lua_objlen(lua, 2);
    const char *comma = "";
    for (size_t i = 1; i <= num; ++i) {
        lua_rawgeti(lua, 2, i);
        if (lua_objlen(lua, -1) != 2) { // need {"name", "type"}
            luabb_error(lua, sp, "bad argument (columns) to 'table'");
            goto out;
        }
        lua_rawgeti(lua, -1, 1);
        lua_rawgeti(lua, -2, 2);
        char *quoted_col = sqlite3_mprintf("\"%w\"", lua_tostring(lua, -2));
        strbuf_appendf(sql, "%s%s %s", comma, quoted_col,
                       lua_tostring(lua, -1));
        sqlite3_free(quoted_col);
        lua_pop(lua, 3);
        comma = ", ";
    }
    strbuf_append(sql, ")");

    // Following can throw exception which may leak strbuf.
    // Copy DDL string onto stack instead.
    int len = strbuf_len(sql) + 1;
    char *ddl = alloca(len);
    memcpy(ddl, strbuf_buf(sql), len);
    strbuf_free(sql);
    sql = NULL;
    sqlite3_stmt *stmt;
    if ((rc = lua_prepare_sql(lua, sp, ddl, &stmt)) != 0) {
        goto out;
    }

    // Run ddl stmt 'create temp table...' under schema lk
    if (tryrdlock_schema_lk() != 0) {
        sqlite3_finalize(stmt);
        return luaL_error(sp->lua, sqlite3ErrStr(SQLITE_SCHEMA));
    }
    // now, actually create the temp table
    *lk = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(*lk, NULL);
    comdb2_set_tmptbl_lk(*lk);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
        ;
    comdb2_set_tmptbl_lk(NULL);
    unlock_schema_lk();
    sqlite3_finalize(stmt);

    if (rc == SQLITE_DONE) {
        return 0;
    } else {
        luabb_error(lua, sp, sqlite3ErrStr(rc));
    }
out:
    if (sql) strbuf_free(sql);
    return -1;
}

static int comdb2_table(Lua lua)
{
    if (!lua_isstring(lua, 1))
        return luabb_error(lua, getsp(lua), "bad argument to 'table'");
    const char *table_name = lua_tostring(lua, 1);
    while (isspace(*table_name))
        ++table_name;
    char n1[MAXTABLELEN], n2[MAXTABLELEN];
    if (two_part_tbl_name(table_name, n1, n2) != 0) {
        return luabb_error(lua, getsp(lua), "bad argument to 'table'");
    }

    dbtable_t *dbtable;
    new_lua_t(dbtable, dbtable_t, DBTYPES_DBTABLE);
    strcpy(dbtable->table_name, table_name);

    // figure out if dbtable is a tmp-tbl
    if (strcasecmp(n1, "temp") == 0) {
        dbtable->is_temp_tbl = 1;
    } else if (strcmp(n1, "") == 0) {
        // not main.tablename or fdb.tablename
        // go through tmp tbls
        SP sp = getsp(lua);
        tmptbl_info_t *tbl;
        LIST_FOREACH(tbl, &sp->tmptbls, entries)
        {
            char n3[MAXTABLELEN], n4[MAXTABLELEN];
            two_part_tbl_name(tbl->name, n3, n4);
            if (strcasecmp(n2, n4) == 0) {
                dbtable->is_temp_tbl = 1;
                break;
            }
        }
    }
    lua_pushinteger(lua, 0);
    return 2;
}

static int new_temp_table(Lua lua)
{
    if (!lua_isstring(lua, 1))
        return luabb_error(lua, NULL, "bad argument to 'table'");
    if (!lua_istable(lua, 2))
        return luabb_error(lua, NULL, "bad argument to 'table'");

    int rc;
    const char *name;
    pthread_mutex_t *lk;
    if (create_temp_table(lua, &lk, &name) == 0) {
        // success - create dbtable
        rc = 0;
        SP sp = getsp(lua);
        dbtable_t *table;
        new_lua_t(table, dbtable_t, DBTYPES_DBTABLE);
        table->is_temp_tbl = 1;
        table->parent = sp->parent;
        strcpy(table->table_name, name);

        // add to list of tmp tbls
        tmptbl_info_t *tmp = malloc(sizeof(tmptbl_info_t));
        tmp->lk = lk;
        tmp->sql = NULL;
        strcpy(tmp->name, name);
        LIST_INSERT_HEAD(&sp->tmptbls, tmp, entries);
    } else {
        // make this fatal for sp
        // temptable name may shadow real tbl name and bad things
        // will happen if sloppy sp keeps using tbl name
        luaL_error(lua, "failed to create tmptable:%s", name);
#       if 0
        rc = 1;
        lua_pushnil(lua);
#       endif
    }
    lua_pushinteger(lua, rc);
    return 2;
}

static int disable_global_variables(lua_State *lua)
{
    lua_createtable(lua, 0, 2);
    lua_pushliteral(lua, "__newindex");
    lua_pushcfunction(lua, l_global_undef);
    lua_rawset(lua, -3);
    lua_pushliteral(lua, "__index");
    lua_pushcfunction(lua, l_global_undef);
    lua_rawset(lua, -3);
    lua_setmetatable(lua, LUA_GLOBALSINDEX);
    return 0;
}

/*
** This returns dbrow or nil on end of result set. There is no way to signal
** error. I'll throw runtime error -- seems appropriate, SQL encountered some
** runtime error (type conversion, row corruption, etc). SPs probably not
** written to handle this situation anyway.
*/
static int lua_sql_step(Lua lua, sqlite3_stmt *stmt)
{
    SP sp = getsp(lua);
    struct sqlthdstate *thd = sp->thd;
    struct sqlclntstate *clnt = sp->clnt;
    int rc = sqlite3_step(stmt);

    if (rc == SQLITE_DONE) {
        return rc;
    } else if (rc != SQLITE_ROW) {
        return luaL_error(lua, sqlite3_errmsg(getdb(sp)));
    }

    lua_newtable(lua);

    int ncols = sqlite3_column_count(stmt);
    for (int col = 0; col < ncols; col++) {
        int type = sqlite3_column_type(stmt, col);
        switch (type) {
        case SQLITE_NULL: {
            int sqltype =
                sqlite_str_to_type(sqlite3_column_decltype(stmt, col));
            luabb_pushnull(lua, sqlite_type_to_dbtype(sqltype));
            break;
        }
        case SQLITE_INTEGER: {
            long long ival = sqlite3_column_int64(stmt, col);
            luabb_pushinteger(lua, ival);
            break;
        }
        case SQLITE_FLOAT: {
            double dval = sqlite3_column_double(stmt, col);
            luabb_pushreal(lua, dval);
            break;
        }
        case SQLITE_TEXT: {
            char *tval = (char *)sqlite3_column_text(stmt, col);
            luabb_pushcstring(lua, tval);
            break;
        }
        case SQLITE_DATETIME: {
            cdb2_client_datetime_t cdt;
            datetime_t datetime;
            const dttz_t *dt = sqlite3_column_datetime(stmt, col);
            dttz_to_client_datetime(dt, stmt_tzname(stmt), &cdt);
            client_datetime_to_datetime_t(&cdt, &datetime, 0);
            luabb_pushdatetime(lua, &datetime);
            break;
        }
        case SQLITE_DATETIMEUS: {
            cdb2_client_datetimeus_t cdt;
            datetime_t datetime;
            const dttz_t *dt = sqlite3_column_datetime(stmt, col);
            dttz_to_client_datetimeus(dt, stmt_tzname(stmt), &cdt);
            client_datetimeus_to_datetime_t(&cdt, &datetime, 0);
            luabb_pushdatetime(lua, &datetime);
            break;
        }
        case SQLITE_BLOB: {
            blob_t blob;
            blob.length = sqlite3_column_bytes(stmt, col);
            blob.data = (char *)sqlite3_column_blob(stmt, col);
            luabb_pushblob(lua, &blob);
            break;
        }
        case SQLITE_INTERVAL_YM: {
            const intv_t *val =
                sqlite3_column_interval(stmt, col, SQLITE_AFF_INTV_MO);
            luabb_pushintervalym(lua, val);
            break;
        }
        case SQLITE_INTERVAL_DSUS:
        case SQLITE_INTERVAL_DS: {
            const intv_t *val =
                sqlite3_column_interval(stmt, col, SQLITE_AFF_INTV_SE);
            luabb_pushintervalds(lua, val);
            break;
        }
        case SQLITE_DECIMAL: {
            const intv_t *val =
                sqlite3_column_interval(stmt, col, SQLITE_AFF_DECIMAL);
            luabb_pushdecimal(lua, &val->u.dec);
            break;
        }
        default:
            return luaL_error(lua, "unknown field type:%d for col:%s",
                              type, sqlite3_column_name(stmt, col));
        }
        lua_setfield(lua, -2, sqlite3_column_name(stmt, col));
    }
    return rc;
}

static void set_sqlrow_stmt(Lua L)
{
    /*
    **  stack:
    **    1. row (lua table)
    **    2. stmt
    **  tag stmt to row by:
    **    newtbl = {}
    **    newtbl.__metatable = stmt
    **    setmetatable(row, newtbl)
    */
    lua_newtable(L);
    lua_pushvalue(L, -3);
    lua_setfield(L, -2, "__metatable");
    lua_setmetatable(L, -2);
}

static sqlite3_stmt *get_sqlrow_stmt(Lua L)
{
    dbstmt_t *stmt = NULL;
    if (lua_getmetatable(L, -1) == 0) return NULL;
    lua_getfield(L, -1, "__metatable");
    if (luabb_type(L, -1) == DBTYPES_DBSTMT) stmt = lua_touserdata(L, -1);
    lua_pop(L, 2);
    return stmt ? stmt->stmt : NULL;
}

static int stmt_sql_step(Lua L, dbstmt_t *stmt)
{
    int rc;
    if ((rc = lua_sql_step(L, stmt->stmt)) == SQLITE_ROW) {
        set_sqlrow_stmt(L);
    }
    return rc;
}

typedef struct client_info {
    struct sqlclntstate *clnt;
    int thread_id;
    char buffer[250];
    int has_buffer;
} clnt_info;

static int send_sp_trace(struct sqlclntstate *clnt, const char *trace, int want_response)
{
    int type = want_response ? RESPONSE_DEBUG : RESPONSE_TRACE;
    return clnt->write_response(clnt, type, (void *)trace, 0);
}

static clnt_info info_buf;

static int get_remote_input(lua_State *lua, char *buffer, size_t sz)
{
    SP sp = getsp(lua);
    struct sqlclntstate *clnt = sp->debug_clnt;

    info_buf.has_buffer = 0;

    char *trace = "\ncomdb2_lua> ";
    sp->rc = send_sp_trace(clnt, trace, 1);
    if (sp->rc) {
        return luabb_error(lua, sp, "%s: couldn't send results back", __func__);
    }
    pthread_mutex_lock(&lua_debug_mutex);
    int rc = clnt->read_response(clnt, RESPONSE_SP_CMD, buffer, sz);
    pthread_mutex_unlock(&lua_debug_mutex);
    return rc;
}

void *read_client_socket(void *in)
{
    int rc;
    struct client_info *info = in;
    struct sqlclntstate *clnt = info->clnt;
    rc = pthread_mutex_trylock(&lua_debug_mutex);
    if (rc) {
        free(info);
        return NULL;
    }
    rc = sbuf2fread(info->buffer, 250, 1, clnt->sb);
    if (rc && (strncmp(info->buffer, "HALT", 4) == 0)) {
        gbl_break_lua = info->thread_id;
        pthread_mutex_unlock(&lua_debug_mutex);
    } else if (rc) {
        info->has_buffer = 1;
        info_buf = *info;
        pthread_mutex_unlock(&lua_debug_mutex);
    } else {
        clnt->sb = NULL;
        pthread_mutex_unlock(&lua_debug_mutex);
    }
    /* Socket is disconnected now. */
    free(info);
    return NULL;
}

static void InstructionCountHook(Lua, lua_Debug *);
static int db_debug(lua_State *lua)
{
    int nargs = lua_gettop(lua);
    const char *trace;
    int rc;

    if (nargs > 2)
        return luabb_error(lua, NULL,
                           "wrong number of  arguments %d, should be 1", nargs);

    SP sp = getsp(lua);

    if (sp->debug_clnt->sp == NULL) {
        /* To be given as lrl value. */
        logmsg(LOGMSG_ERROR, "debug client no longer valid \n");
        sp->debug_clnt = sp->clnt;
        lua_sethook(lua, InstructionCountHook, LUA_MASKCOUNT, 1);
        pthread_cond_broadcast(&lua_debug_cond);
        return 0;
    }

    if (sp->debug_clnt->want_stored_procedure_debug) {
        trace = lua_tostring(lua, -1);
        rc = send_sp_trace(sp->debug_clnt, trace, 0);
        if (rc) {
            if (sp->debug_clnt != sp->clnt) {
                /* To be given as lrl value. */
                sp->debug_clnt = sp->clnt;
                lua_sethook(lua, InstructionCountHook, LUA_MASKCOUNT, 1);
                pthread_cond_broadcast(&lua_debug_cond);
                return 0;
            }
            sleep(2);
            return luabb_error(lua, NULL, "Error in sending back data.");
        }
    }

    if (gbl_break_lua == INT_MAX) {
        char sp_info[128];
        int len;
        if (sp->spversion.version_num) {
            sprintf(sp_info, "%s:%d", sp->spname, sp->spversion.version_num);
        } else {
            sprintf(sp_info, "%s:%s", sp->spname, sp->spversion.version_str);
        }
        len = strlen(sp_info);
        if (strncasecmp(sp_info, gbl_break_spname, len) == 0) {
            gbl_break_lua = pthread_self();
        }
    }

    if (gbl_break_lua && (gbl_break_lua == pthread_self())) {
        if (debug_clnt) {
            sp->debug_clnt = debug_clnt;
            sp->debug_clnt->sp = sp;
            debug_clnt = NULL;
        }
        char *buffer = "_SP.debug_next()";
        if (luaL_loadbuffer(lua, buffer, strlen(buffer), "=(debug command)") ||
            lua_pcall(lua, 0, 0, 0)) {
            return luabb_error(lua, NULL, "Error in the call for breakpoint");
        }
        lua_settop(lua, 0); /* remove eventual returns */
        gbl_break_lua = 0;
    }

    return 0;
}

static int db_db_debug(Lua lua)
{
    int i, read;
    char *replace_from = NULL;
    int finish_execute = 0;
    int is_cont = 0;
    for (;;) {
        char buffer[250] = {0};
        char old_buffer[250];
        read = get_remote_input(lua, buffer, sizeof(buffer));
        if (strncmp(buffer, "cont", 4) == 0) {
            pthread_cond_broadcast(&lua_debug_cond);
            sprintf(buffer, " %s", "_SP.do_next = false \n if (db.emit) then "
                                   "\n db_emit = db.emit \n end");
            finish_execute = 1;
            is_cont = 1;
        } else if (strncmp(buffer, "help", 4) == 0) {
            sprintf(buffer, " %s()", "_SP.help");
        } else if (strncmp(buffer, "next", 4) == 0) {
            sprintf(buffer, " %s()", "_SP.debug_next");
            finish_execute = 1;
        } else if (strncmp(buffer, "breakpoints", 11) == 0) {
            sprintf(buffer, " %s()", "_SP.bkps");
        } else if (strncmp(buffer, "stop at", 7) == 0) {
            i = atoi(&buffer[7]);
            sprintf(buffer, " %s(%d)", "_SP.set_breakpoint", i);
        } else if (strncmp(buffer, "list", 4) == 0) {
            i = atoi(&buffer[4]);
            sprintf(buffer, " %s(%d)", "_SP.list_code", i);
        } else if (strncmp(buffer, "delete at", 9) == 0) {
            i = atoi(&buffer[9]);
            sprintf(buffer, " %s(%d)", "_SP.delete_breakpoint", i);
        } else if (strncmp(buffer, "print ", 6) == 0) {
            sprintf(old_buffer, "%s", buffer);
            int len = strlen(&old_buffer[6]);
            old_buffer[6 + len - 1] = '\0';
            sprintf(buffer, "eval('%s')", &old_buffer[6]);
        } else if (strncmp(buffer, "getinfo", 7) == 0) {
            sprintf(buffer, " %s", "_SP.getinfo(5)");
        } else if (strncmp(buffer, "HALT", 4) == 0) {
            logmsg(LOGMSG_USER, "Halt should not be coming here...\n");
            continue;
        } else if (strncmp(buffer, "where", 5) == 0) {
            sprintf(buffer, " %s", "_SP.where()");
        } else if ((replace_from = strstr(buffer, "getvariable")) != 0) {
            strncpy(replace_from, "_SP.get_var", 11);
            replace_from = NULL;
        } else if ((replace_from = strstr(buffer, "setvariable")) != 0) {
            strncpy(replace_from, "_SP.set_var", 11);
            replace_from = NULL;
        } else if (read == 0) {
            /* Debugging socket is closed, let the program continue. */
            pthread_cond_broadcast(&lua_debug_cond);
            sprintf(buffer, " %s", "db_emit = db.emit");
            finish_execute = 1;
        } else if (buffer[0] == '\0') {
            /* Debugging socket is closed, let the program continue. */
            sprintf(buffer, " %s()", "_SP.debug_next");
        }

        logmsg(LOGMSG_USER, "Running buffer%s ", buffer);

        if (luaL_loadbuffer(lua, buffer, strlen(buffer), "=(debug command)") ||
            lua_pcall(lua, 0, 0, 0)) {
            db_debug(lua);
            logmsg(LOGMSG_ERROR, "Problem in running LUA %s\n", buffer);
        }
        lua_settop(lua, 0); /* remove eventual returns */
        if (finish_execute) {
            SP sp = getsp(lua);
            if (is_cont && (sp->clnt == sp->debug_clnt) &&
                sp->clnt->want_stored_procedure_debug) {
                clnt_info *info = malloc(sizeof(clnt_info));
                info->clnt = sp->clnt;
                info->thread_id = pthread_self();
                pthread_t read_client;
                pthread_create(&read_client, NULL, read_client_socket, info);
            }
            return 0;
        }
    }
}

static int db_trace(lua_State *lua)
{
    int nargs = lua_gettop(lua);
    const char *trace;
    int rc;

    if (nargs > 2)
        return luabb_error(lua, NULL, "wrong number of  arguments %d", nargs);

    trace = lua_tostring(lua, -1);

    if (trace == NULL) return 0;

    SP sp = getsp(lua);

    if (sp->clnt->want_stored_procedure_trace) {
        sp->rc = send_sp_trace(sp->clnt, trace, 0);
    }

    return 0;
}

static int l_panic(lua_State *lua)
{
    logmsg(LOGMSG_ERROR, "PANIC: %s\n", lua_tostring(lua, -1));
    return 0;
}

static void stack_trace(lua_State *lua)
{
    lua_getfield(lua, LUA_GLOBALSINDEX, "debug");
    if (!lua_istable(lua, -1)) {
        lua_pop(lua, 1);
        return;
    }
    lua_getfield(lua, -1, "traceback");
    if (!lua_isfunction(lua, -1)) {
        lua_pop(lua, 2);
        return;
    }
    lua_pushvalue(lua, 1);
    lua_pushinteger(lua, 2);
    lua_call(lua, 2, 1);
    logmsg(LOGMSG_USER, "\n");
}

static char *no_such_procedure(const char *name, struct spversion_t *spversion)
{
    strbuf *buf = strbuf_new();
    if (spversion->version_num) {
        strbuf_appendf(buf, "no such procedure: %s ver:%d", name,
                       spversion->version_num);
    } else {
        strbuf_appendf(buf, "no such procedure: %s ver:%s", name,
                       spversion->version_str ? spversion->version_str : "0");
    }
    char *ret = strbuf_disown(buf);
    strbuf_free(buf);
    return ret;
}

static char bootstrap_src[] = "\n"
                              "local function comdb2_main()\n"
                              "    db:bootstrap()\n"
                              "end\n"
                              "comdb2_main()";

static char *load_default_src(char *spname, struct spversion_t *spversion,
                              int *size)
{
    char *src = NULL;
    int bdberr;
    int v = bdb_get_sp_get_default_version(spname, &bdberr);
    if (v > 0) {
        if (bdb_get_sp_lua_source(NULL, NULL, spname, &src, v, size, &bdberr) ==
            0)
            spversion->version_num = v;
        return src;
    }
    if (bdb_get_default_versioned_sp(spname, &spversion->version_str) != 0) {
        return NULL;
    }
    if (bdb_get_versioned_sp(spname, spversion->version_str, &src) == 0) {
        *size = strlen(src) + 1;
    }
    return src;
}

#define IS_SYS(spname) (!strncasecmp(spname, "sys.", 4))

static char *load_src(char *spname, struct spversion_t *spversion,
                      int bootstrap, char **err)
{
    int rc, bdb_err;
    char *src;
    int size = 0;
    if (IS_SYS(spname)) {
        src = find_syssp(spname);
        if (src == NULL) {
            *err = no_such_procedure(spname, spversion);
            return NULL;
        }
        if (bootstrap) {
            size = strlen(src) + 1;
            char *bsrc = malloc(size + sizeof(bootstrap_src));
            strcpy(bsrc, src);
            strcat(bsrc, bootstrap_src);
            return bsrc;
        } else {
            return strdup(src);
        }
    }
    if (spversion->version_num == 0 && spversion->version_str == NULL) {
        if ((src = load_default_src(spname, spversion, &size)) == NULL) {
            *err = no_such_procedure(spname, spversion);
            return NULL;
        }
    } else if (spversion->version_num > 0) {
        if ((rc = bdb_get_sp_lua_source(thedb->bdb_env, NULL, spname, &src,
                                        spversion->version_num, &size,
                                        &bdb_err)) != 0) {
            *err = no_such_procedure(spname, spversion);
            return NULL;
        }
    } else {
        if (bdb_get_versioned_sp(spname, spversion->version_str, &src) != 0) {
            *err = no_such_procedure(spname, spversion);
            return NULL;
        }
        size = strlen(src) + 1;
    }
    if (bootstrap) {
        src = realloc(src, size + sizeof(bootstrap_src));
        strcat(src, bootstrap_src);
    }
    return src;
}

static int load_debugging_information(struct stored_proc *sp, char **err)
{
    int i, bdb_err, rc = 0;
    char *s;
    int idx = 1;
    const char *err_str = NULL;
    char *debug;
    char *sp_source;
    int source_size;

    enable_global_variables(sp->lua);

    sp_source = load_src(sp->spname, &sp->spversion, 0, err);
    if (sp_source) {
        source_size = strlen(sp_source);
    } else {
        reset_sp(sp);
        return -1;
    }

    s = sp_source;
    lua_newtable(sp->lua);
    for (i = 0; i < source_size; i++) {
        if (sp_source[i] == '\n') {
            sp_source[i] = '\0';
            /*printf("%d> %s\n", idx, s);*/
            lua_pushstring(sp->lua, s);
            lua_rawseti(sp->lua, -2, idx++);
            s = &sp_source[i + 1];
        }
    }
    /* make this more hidden somehow? */
    lua_setglobal(sp->lua, "_thecode");
    if (sp->clnt->want_stored_procedure_trace) {
        debug = "function trace (event, line)\n"
                "  local s = debug.getinfo(0).name \n"
                "  if (s == 'return_type') then  \n"
                "     s = nil  \n"
                "  end  \n"
                " if (s and _thecode[line]) then \n"
                "   db.trace(line .. ':' .. _thecode[line])\n"
                " end\n"
                "end\n"
                "debug.sethook(trace, \"l\")\n";
    } else {
        /* REM : Use level 4 when trying to access variables in debugging. */
        debug = "_SP._breakpoints = {}\n"
                "_SP._variables = {}\n"
                "_SP.do_next = true\n" /* Have the breakpoint at first running
                                          line. */
                "_SP.curr_line = 0\n"
                "_SP.set_breakpoint = function(line) \n"
                " _SP._breakpoints[line] = true  \n"
                " db.debug('Breakpoint set at line : ' .. line)\n"
                "end \n"
                "_SP.delete_breakpoint = function(line) \n"
                " _SP._breakpoints[line] = false  \n"
                " db.debug('Breakpoint deleted from line : ' .. line)\n"
                "end \n"
                "_SP.list_code = function(line) \n"
                " local curr_line = line \n"
                " while( curr_line <line+10) do \n"
                "   if(_thecode[curr_line]) then \n"
                "     db.debug( curr_line .. ' : ' ..  _thecode[curr_line])\n"
                "   end\n"
                "   curr_line = curr_line + 1\n"
                " end\n"
                "end \n"
                "_SP.has_breakpoint = function(line)\n"
                " return _SP._breakpoints[line] \n"
                "end \n"
                "_SP.debug_next = function(line)\n"
                " _SP.do_next = true \n"
                "end \n"
                "_SP.help = function(line)\n"
                "     db.debug('stop at <line no>        -- Adds Breakpoint')\n"
                "     db.debug('delete at <line no>      -- Deletes "
                "Breakpoint')\n"
                "     db.debug('next                     -- Next Line')\n"
                "     db.debug('getinfo                  -- Get info of local "
                "variables')\n"
                "     db.debug('getvariable(num)         -- Get local variable "
                "of the number displayed in getinfo call.')\n"
                "     db.debug('setvariable(num)         -- Set local variable "
                "of the number displayed in getinfo call.')\n"
                "     db.debug('print                    -- Display Local "
                "Variable by name')\n"
                "     db.debug('cont                     -- Continue')\n"
                "     db.debug('help                     -- This menu')\n"
                "end \n"
                "eval = function(object, value)\n"
                " if(_SP._variables[object]) then\n"
                "   value = _SP._variables[object]  \n"
                " end\n"
                "   if(value and type(value) == 'table') then\n"
                "    db.debug(tostring(object)) \n"
                "    display_table(value)\n"
                "    return\n"
                "   end\n"
                "   if (value) then \n"
                "     db.debug(tostring(object) ..' : ' .. tostring(value)) \n"
                "   else \n"
                "     db.debug(tostring(object)) \n"
                "   end \n"
                "end \n"
                "display_table = function(object)\n"
                " if(_SP._variables[object]) then\n"
                "   table.foreach(_SP._variables[object], eval) \n"
                " else\n"
                "   table.foreach(object, eval) \n"
                " end\n"
                "end \n"
                "_SP.bkps = function(object)\n"
                " local line = 1 \n"
                " while true do \n"
                "   if(_thecode[line]) then \n"
                "     if(_SP.has_breakpoint(line)) then\n"
                "       db.debug('line no ' .. line .. ':' .. _thecode[line])\n"
                "     end \n"
                "   else \n"
                "     return \n"
                "   end \n"
                "   line = line + 1\n"
                " end \n"
                "end \n"
                "_SP.get_var = function(num) \n"
                "  local name, x = debug.getlocal(5, num) \n"
                "  if name then  \n"
                "    db.debug('got local variable ' .. name .. ' : ' .. "
                "tostring(x))\n"
                "  end \n"
                "  return x\n"
                "end \n"
                "_SP.set_var = function(num, val) \n"
                "  local name, x = debug.setlocal(5, num,val) \n"
                "end \n"
                "_SP.where = function() \n"
                "  db.debug('line no ' .. _SP.curr_line .. ':' .. "
                "_thecode[_SP.curr_line])\n"
                "end \n"
                "_SP.getinfo = function(num) \n"
                "     local a = 1\n"
                "     while true do \n"
                "       local name, value = debug.getlocal(num, a) \n"
                "       if not name then break end \n"
                "       _SP._variables[name] = value \n"
                "       db.debug('local var ' .. a .. '). '.. name .. ' : ' .. "
                "tostring(value))\n"
                "       a = a + 1\n"
                "     end\n"
                "end\n"
                "local function debug_emit(obj) \n"
                "  table.foreach(obj, eval) \n"
                "  db.emit(obj)\n"
                "end \n"
                "function comdb2_debug (event, line)\n"
                "  _SP.curr_line = line\n"
                "  local s = debug.getinfo(0).name \n"
                "  if (s == 'return_type') then  \n"
                "     s = nil  \n"
                "  end  \n"
                "  if(s and _thecode[line]) then \n"
                "    db.debug('line no ' .. line .. ':' .. _thecode[line])\n"
                "    if(_SP.has_breakpoint(line) or _SP.do_next) then\n"
                "       local db_emit = debug_emit\n"
                "       local a = 1\n"
                "       while true do \n"
                "         local name, value = debug.getlocal(2, a) \n"
                "         if not name then break end \n"
                "         _SP._variables[name] = value \n"
                "         a = a + 1\n"
                "       end\n"
                "       _SP.do_next = false \n"
                "       db.db_debug()\n"
                "    end  \n"
                "   end \n"
                "end\n"
                "\n"
                "debug.sethook(comdb2_debug, \"l\")\n";
    }
    rc = luaL_loadstring(sp->lua, debug);
    if (rc) {
        err_str = lua_tostring(sp->lua, -1);
        if (err_str)
            logmsg(LOGMSG_ERROR, "err: [%d] %s\n", rc, err_str);
        else
            logmsg(LOGMSG_ERROR, "err: [%d]\n", rc);
        stack_trace(sp->lua);
    }

    rc = lua_pcall(sp->lua, 0, 0, 0);
    if (rc) {
        if (lua_isnumber(sp->lua, -1)) {
            rc = lua_tonumber(sp->lua, -1);
            if (sp->error) err_str = sp->error;
        } else {
            err_str = lua_tostring(sp->lua, -1);
            rc = -1;
        }

        if (err)
            logmsg(LOGMSG_ERROR, "err: [%d] %s\n", rc, err_str);
        else
            logmsg(LOGMSG_ERROR, "err: [%d]\n", rc);
        stack_trace(sp->lua);
    }
    free(sp_source);
    return rc;
}

static void InstructionCountHook(lua_State *lua, lua_Debug *debug)
{
    SP sp = getsp(lua);
    if (sp) {
        lua_pop(lua, 1);
        if (sp->num_instructions > sp->max_num_instructions) {
            luabb_error(
                lua, NULL,
                "Exceeded instruction quota (%d). Set db:setmaxinstructions()",
                sp->max_num_instructions);
        }
        sp->num_instructions++;

        if (gbl_break_lua == INT_MAX) {
            lua_getstack(lua, 1, debug);
            lua_getinfo(lua, "nSl", debug);
            char sp_info[128];
            lua_getstack(lua, 1, debug);
            lua_getinfo(lua, "nSl", debug);
            if (sp->spversion.version_num) {
                sprintf(sp_info, "%s:%d:%d", sp->spname,
                        sp->spversion.version_num, debug->currentline);
            } else {
                sprintf(sp_info, "%s:%s:%d", sp->spname,
                        sp->spversion.version_str, debug->currentline);
            }
            if (strcasecmp(sp_info, gbl_break_spname) == 0) {
                gbl_break_lua = pthread_self();
            }
        }

        if (gbl_break_lua && (gbl_break_lua == pthread_self())) {
            if (debug_clnt) {
                char *err = NULL;
                load_debugging_information(sp, &err);
            }
        }
        extern int gbl_notimeouts;
        extern int gbl_epoch_time;

        if (gbl_epoch_time) {
            if ((gbl_epoch_time - sp->clnt->last_check_time) > 5) {
                sp->clnt->last_check_time = gbl_epoch_time;
                if (!gbl_notimeouts) {
                    if (peer_dropped_connection(sp->clnt)) {
                        luaL_error(lua, "Socket Connection DROPPED");
                    }
                }
            }
        }
    }
}

static int stmt_bind_int(Lua lua, sqlite3_stmt *stmt, int name, int value)
{
    SP sp = getsp(lua);
    int position;
    if (lua_isnumber(lua, name)) {
        position = lua_tonumber(lua, name);
    } else if (lua_isstring(lua, name)) {
        char parmname[32];
        snprintf(parmname, sizeof(parmname), "@%s", lua_tostring(lua, name));
        position = sqlite3_bind_parameter_index(stmt, parmname);
    } else {
        return luabb_error(lua, sp, "bad argument to 'bind'");
    }

    if (position == 0) {
        return luabb_error(lua, sp, "invalid position");
    }

    dttz_t dt;
    const char *c;
    const void *p;
    blob_t *b;
    intv_t *i;
    datetime_t *d;
    int type;

    if ((type = luabb_dbtype(lua, value)) > DBTYPES_MINTYPE) {
        if (luabb_isnull(lua, value)) {
            return sqlite3_bind_null(stmt, position);
        }
        p = lua_topointer(lua, value);
    }
    switch (type) {
    case DBTYPES_LNIL: return sqlite3_bind_null(stmt, position);
    case DBTYPES_LSTRING:
        c = lua_tostring(lua, value);
        return sqlite3_bind_text(stmt, position, c, strlen(c), NULL);
    case DBTYPES_LNUMBER:
        return sqlite3_bind_double(stmt, position, lua_tonumber(lua, value));
    case DBTYPES_INTEGER:
        return sqlite3_bind_int64(stmt, position, ((lua_int_t *)p)->val);
    case DBTYPES_REAL:
        return sqlite3_bind_double(stmt, position, ((lua_real_t *)p)->val);
    case DBTYPES_CSTRING:
        c = ((lua_cstring_t *)p)->val;
        return sqlite3_bind_text(stmt, position, c, strlen(c), NULL);
    case DBTYPES_BLOB:
        b = &((lua_blob_t *)p)->val;
        return sqlite3_bind_blob(stmt, position, b->data, b->length, NULL);
    case DBTYPES_DATETIME:
        d = &((lua_datetime_t *)p)->val;
        datetime_t_to_dttz(d, &dt);
        return sqlite3_bind_datetime(stmt, position, &dt, d->tzname);
    case DBTYPES_INTERVALYM:
        i = &((lua_intervalym_t *)p)->val;
        return sqlite3_bind_interval(stmt, position, *i);
    case DBTYPES_INTERVALDS:
        i = &((lua_intervalds_t *)p)->val;
        return sqlite3_bind_interval(stmt, position, *i);
    default: return luabb_error(lua, NULL, "unsupported type for bind");
    }
}

static int stmt_bind(Lua L, sqlite3_stmt *stmt)
{
    int rc = stmt_bind_int(L, stmt, 2, 3);
    lua_pushinteger(L, rc);
    return 1;
}

static inline void no_stmt_chk(Lua L, const dbstmt_t *dbstmt)
{
    if (dbstmt == NULL || dbstmt->stmt == NULL) luaL_error(L, "no active stmt");
}

static int dbstmt_bind_int(Lua lua, dbstmt_t *dbstmt)
{
    no_stmt_chk(lua, dbstmt);
    if (dbstmt->fetched) {
        dbstmt->fetched = 0;
        sqlite3_reset(dbstmt->stmt);
    }
    return stmt_bind(lua, dbstmt->stmt);
}

static int lua_prepare_sql_int(Lua L, SP sp, const char *sql,
                               sqlite3_stmt **stmt, struct sql_state *rec)
{
    struct errstat err = {0};
    struct sql_state rec_lcl = {0};
    struct sql_state *rec_ptr = rec ? rec : &rec_lcl;
    rec_ptr->sql = sql;
    sp->rc = get_prepared_stmt_try_lock(sp->thd, sp->clnt, rec_ptr, &err, sp->initial);
    sp->initial = 0;
    if (sp->rc == 0) {
        *stmt = rec_ptr->stmt;
        rec_ptr->sql = sqlite3_sql(*stmt);
    } else if (sp->rc == SQLITE_SCHEMA) {
        return luaL_error(L, sqlite3ErrStr(sp->rc));
    } else {
        luabb_error(L, sp, "%s in stmt: %s", err.errstr, sql);
    }
    return sp->rc;
}

static int lua_prepare_sql(Lua L, SP sp, const char *sql, sqlite3_stmt **stmt)
{
    return lua_prepare_sql_int(L, sp, sql, stmt, NULL);
}

static void push_clnt_cols(Lua L, SP sp)
{
    SP parent = sp->parent;
    int cols = parent->ntypes;
    if (parent->clntname == NULL) {
        luaL_error(L, "attempt to emit row without defining columns");
        return;
    }
    lua_checkstack(L, cols + 10);
    for (int i = 0; i < cols; ++i) {
        if (parent->clntname[i] == NULL) {
            luaL_error(L, "attempt to emit row without defining columns");
            return;
        }
        lua_getfield(L, -1, parent->clntname[i]);
        lua_insert(L, -2);
    }
}

static int l_send_back_row(Lua, sqlite3_stmt *, int);
static int luatable_emit(Lua L)
{
    int cols;
    SP sp = getsp(L);
    sqlite3_stmt *stmt = get_sqlrow_stmt(L);
    if (stmt) {
        cols = sqlite3_column_count(stmt);
    } else if (sp->parent->ntypes) {
        push_clnt_cols(L, sp);
        lua_pop(L, 1);
        cols = lua_gettop(L);
    } else {
        return luaL_error(L, "attempt to emit row without defining columns");
    }
    int rc = l_send_back_row(L, stmt, cols);
    lua_pushinteger(L, rc);
    return 1;
}

static int dbtable_insert(Lua lua)
{
    SP sp = getsp(lua);

    int rc, nargs, len;
    strbuf *columns, *params, *sql;

    nargs = lua_gettop(lua);
    if (nargs != 2) {
        return luabb_error(lua, NULL, "bad arguments to 'insert'");
    }

    luaL_checkudata(lua, 1, dbtypes.dbtable);
    dbtable_t *table = lua_touserdata(lua, 1);

    columns = strbuf_new();
    params = strbuf_new();

    strbuf_append(columns, "(");
    strbuf_append(params, "(");

    luaL_checktype(lua, 2, LUA_TTABLE);

    // Iterate lua table to create prepared statement
    lua_pushnil(lua);
    while (lua_next(lua, 2)) {
        const char *col = lua_tostring(lua, -2);
        char *quoted_col = sqlite3_mprintf("\"%w\",", col);
        strbuf_append(columns, quoted_col);
        sqlite3_free(quoted_col);
        strbuf_appendf(params, "?,");
        lua_pop(lua, 1);
    }
    strbuf_del(columns, 1);
    strbuf_del(params, 1);

    strbuf_append(columns, ")");
    strbuf_append(params, ")");

    char n1[MAXTABLELEN], separator[2], n2[MAXTABLELEN];
    query_tbl_name(table->table_name, n1, separator, n2);
    sql = strbuf_new();
    strbuf_appendf(sql, "INSERT INTO %s%s\"%s\" %s VALUES %s", n1, separator,
                   n2, strbuf_buf(columns), strbuf_buf(params));
    char *sqlstr = strbuf_disown(sql);

    strbuf_free(sql);
    strbuf_free(columns);
    strbuf_free(params);

    db_reset(lua);
    sqlite3_stmt *stmt = NULL;
    rc = lua_prepare_sql(lua, sp, sqlstr, &stmt);
    free(sqlstr);
    if (rc != 0) {
        lua_pushinteger(lua, rc); /* Failure return code. */
        return 1;
    }

    // Iterate lua table again to bind params to stmt
    lua_pushnil(lua);
    int pos = 1;
    while (lua_next(lua, 2)) {
        lua_pushinteger(lua, pos++);
        if ((rc = stmt_bind_int(lua, stmt, 5, 4)) != 0) goto out;
        lua_pop(lua, 2);
    }
    lua_pop(lua, 1); /* Keep just dbtable on stack. */

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
        ;

    if (rc == SQLITE_DONE) rc = 0;

out:
    sqlite3_finalize(stmt);
    lua_pushinteger(lua, rc); /* Success return code. */
    return 1;
}

static int dbtable_copyfrom(Lua lua)
{
    SP sp = getsp(lua);

    dbtable_t *tbl1, *tbl2;
    const char *where_clause = NULL;

    int nargs = lua_gettop(lua);
    if (nargs > 3) {
        return luabb_error(lua, sp, "expected 2 arguments found %d %s", nargs,
                           __func__);
    }

    luaL_checkudata(lua, 1, dbtypes.dbtable);
    tbl1 = (dbtable_t *)lua_topointer(lua, 1);

    luaL_checkudata(lua, 2, dbtypes.dbtable);
    tbl2 = (dbtable_t *)lua_topointer(lua, 2);

    if (lua_isstring(lua, 3)) where_clause = (const char *)lua_tostring(lua, 3);

    char t1n1[MAXTABLELEN], t1sep[2], t1n2[MAXTABLELEN];
    query_tbl_name(tbl1->table_name, t1n1, t1sep, t1n2);

    char t2n1[MAXTABLELEN], t2sep[2], t2n2[MAXTABLELEN];
    query_tbl_name(tbl2->table_name, t2n1, t2sep, t2n2);

    strbuf *sql = strbuf_new();
    if (where_clause)
        strbuf_appendf(
            sql, "insert into %s%s\"%s\" select * from %s%s\"%s\" where %s",
            t1n1, t1sep, t1n2, t2n1, t2sep, t2n2, where_clause);
    else
        strbuf_appendf(sql, "insert into %s%s\"%s\" select * from %s%s\"%s\"",
                       t1n1, t1sep, t1n2, t2n1, t2sep, t2n2);

    db_reset(lua);
    sqlite3_stmt *stmt = NULL;
    int rc = lua_prepare_sql(lua, sp, strbuf_buf(sql), &stmt);
    strbuf_free(sql);
    if (rc != 0) {
        return rc;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
        ;

    sqlite3_finalize(stmt);

    lua_pushinteger(lua, 0); /* Success return code. */
    return 1;
}

static int dbtable_name(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.dbtable);
    dbtable_t *dbtable = lua_touserdata(L, 1);
    lua_pushstring(L, dbtable->table_name);
    return 1;
}

// dbtable_emit = db_exec + dbstmt_emit
static int dbtable_emit_int(Lua L, dbtable_t *table)
{
    char n1[MAXTABLELEN], separator[2], n2[MAXTABLELEN];
    query_tbl_name(table->table_name, n1, separator, n2);

    char sql[128];
    sprintf(sql, "select * from %s%s\"%s\"", n1, separator, n2);

    lua_getglobal(L, "db");
    lua_pushstring(L, sql);
    db_exec(L);
    if (lua_isnumber(L, -1) && lua_tonumber(L, -1) == 0) {
        /*
        **  Lua stack:
        **    1. number
        **    2. stmt
        **    3. sql string
        **    4. db
        */
        lua_pop(L, 1);
        lua_insert(L, 1);
        lua_settop(L, 1);
        /*
        **  Lua stack:
        **    1. stmt
        */
        return dbstmt_emit(L);
    }
    return 1;
}

static int dbtable_emit(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.dbtable);
    dbtable_t *table = lua_touserdata(L, 1);
    lua_pop(L, 1);
    return dbtable_emit_int(L, table);
}

dbstmt_t *new_dbstmt(Lua lua, SP sp, sqlite3_stmt *stmt)
{
    dbstmt_t *dbstmt;
    new_lua_t(dbstmt, dbstmt_t, DBTYPES_DBSTMT);
    dbstmt->stmt = stmt;
    dbstmt->num_tbls = stmt_num_tbls(stmt);
    if (dbstmt->num_tbls) {
        LIST_INSERT_HEAD(&sp->dbstmts, dbstmt, entries);
    }
    return dbstmt;
}
static int dbtable_where(lua_State *lua)
{
    SP sp = getsp(lua);

    int nargs = lua_gettop(lua);
    if (nargs != 2) {
        luabb_error(lua, sp, "wrong number of arguments to 'where'");
        return 2;
    }
    luaL_checkudata(lua, 1, dbtypes.dbtable);

    const dbtable_t *tbl = lua_topointer(lua, 1);
    char n1[MAXTABLELEN], separator[2], n2[MAXTABLELEN];
    query_tbl_name(tbl->table_name, n1, separator, n2);
    const char *condition = lua_tostring(lua, 2);
    strbuf *sql = strbuf_new();
    if (condition && strlen(condition))
        strbuf_appendf(sql, "select * from %s%s\"%s\" where %s", n1, separator,
                       n2, condition);
    else
        strbuf_appendf(sql, "select * from %s%s\"%s\"", n1, separator, n2);
    db_reset(lua);
    sqlite3_stmt *stmt = NULL;
    int rc = lua_prepare_sql(lua, sp, strbuf_buf(sql), &stmt);
    strbuf_free(sql);
    if (rc) {
        lua_pushnil(lua);
        lua_pushinteger(lua, rc); /* Failure return code. */
        return 2;
    }
    new_dbstmt(lua, sp, stmt);
    lua_pushnumber(lua, 0);
    return 2;
}

static inline const char *bad_handle()
{
    return "Wrong sql handle state";
}

static inline const char *no_transaction()
{
    return "No transaction to COMMIT/ROLLBACK";
}

static void reset_stmt(SP sp, dbstmt_t *dbstmt)
{
    if (dbstmt->rec) { // prepared stmt
        dbstmt->fetched = 0;
        dbstmt->initial = 0;
        sqlite3_reset(dbstmt->stmt);
    } else {
        donate_stmt(sp, dbstmt);
    }
}

static void reset_stmts(SP sp)
{
    dbstmt_t *dbstmt, *tmp;
    LIST_FOREACH_SAFE(dbstmt, &sp->dbstmts, entries, tmp)
    {
        reset_stmt(sp, dbstmt);
    }
}

// _int variants don't modify lua stack, just return success/error code
static const char * db_begin_int(Lua, int *);
static const char * db_commit_int(Lua, int *);
static const char * db_rollback_int(Lua, int *);

static int db_begin(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.db);
    int rc;          // begin rc
    const char *err; // logic err - terminate lua prog
    if ((err = db_begin_int(L, &rc)) == NULL) return push_and_return(L, rc);
    return luaL_error(L, err);
}

static int db_commit(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.db);
    SP sp = getsp(L);
    if (sp->in_parent_trans) { // explicit commit w/o begin
        return luaL_error(L, no_transaction());
    }
    int rc;          // commit rc
    const char *err; // logic err - terminate lua prog
    if ((err = db_commit_int(L, &rc)) == NULL) return push_and_return(L, rc);
    return luaL_error(L, err);
}

static int db_rollback(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.db);
    SP sp = getsp(L);
    if (sp->in_parent_trans) { // explicit commit w/o begin
        return luaL_error(L, no_transaction());
    }
    int rc;          // rollback rc
    const char *err; // logic err - terminate lua prog
    if ((err = db_rollback_int(L, &rc)) == NULL) return push_and_return(L, rc);
    return luaL_error(L, err);
}

static const char *db_begin_int(Lua L, int *rc)
{
    SP sp = getsp(L);
    if (sp->in_parent_trans && sp->make_parent_trans) {
        const char *err;
        if ((err = db_commit_int(L, rc)) != NULL) return err;
        sp->in_parent_trans = 0;
    } else {
        reset_stmts(sp);
    }
    if (sp->clnt->ctrl_sqlengine != SQLENG_NORMAL_PROCESS) {
        sql_set_sqlengine_state(sp->clnt, __FILE__, __LINE__,
                                SQLENG_WRONG_STATE);
        return "BEGIN in the middle of transaction";
    }
    sql_set_sqlengine_state(sp->clnt, __FILE__, __LINE__,
                            SQLENG_PRE_STRT_STATE);
    *rc = handle_sql_begin(sp->thd, sp->clnt, 0);
    sp->clnt->ready_for_heartbeats = 1;
    return NULL;
}

static const char *db_commit_int(Lua L, int *rc)
{
    SP sp = getsp(L);
    if (sp->clnt->ctrl_sqlengine != SQLENG_INTRANS_STATE &&
        sp->clnt->ctrl_sqlengine != SQLENG_STRT_STATE) {
        sql_set_sqlengine_state(sp->clnt, __FILE__, __LINE__,
                                SQLENG_WRONG_STATE);
        return no_transaction();
    }
    reset_stmts(sp);
    sql_set_sqlengine_state(sp->clnt, __FILE__, __LINE__, SQLENG_FNSH_STATE);
    if ((*rc = handle_sql_commitrollback(sp->thd, sp->clnt, 0)) == 0) {
        free(sp->error);
        sp->error = NULL;
    } else {
        luabb_error(L, sp, errstat_get_str(&sp->clnt->osql.xerr));
    }
    sp->clnt->ready_for_heartbeats = 1; /* Don't stop sending heartbeats. */
    if ((sp->in_parent_trans == 0) && sp->make_parent_trans) {
        int tmp;
        if (db_begin_int(L, &tmp) == 0) sp->in_parent_trans = 1;
    }
    sp->clnt->osql.tran_ops = 0;
    return NULL;
}

static const char *db_rollback_int(Lua L, int *rc)
{
    SP sp = getsp(L);
    if (sp->clnt->ctrl_sqlengine != SQLENG_INTRANS_STATE &&
        sp->clnt->ctrl_sqlengine != SQLENG_STRT_STATE) {
        sql_set_sqlengine_state(sp->clnt, __FILE__, __LINE__,
                                SQLENG_WRONG_STATE);
        return no_transaction();
    }
    reset_stmts(sp);
    sql_set_sqlengine_state(sp->clnt, __FILE__, __LINE__,
                            SQLENG_FNSH_RBK_STATE);
    reqlog_set_event(sp->thd->logger, "sp");
    *rc = handle_sql_commitrollback(sp->thd, sp->clnt, 0);
    sp->clnt->ready_for_heartbeats = 1;
    if ((sp->in_parent_trans == 0) && sp->make_parent_trans) {
        int tmp;
        if (db_begin_int(L, &tmp) == 0) sp->in_parent_trans = 1;
    }
    sp->clnt->osql.tran_ops = 0;
    return NULL;
}

static const char *begin_parent(Lua L)
{
    SP sp = getsp(L);
    if (sp->clnt->ctrl_sqlengine != SQLENG_NORMAL_PROCESS) {
        return bad_handle();
    }
    const char *err;
    int rc;
    if ((err = db_begin_int(L, &rc)) == NULL && rc == 0) {
        sp->in_parent_trans = 1;
        sp->make_parent_trans = 1;
        return NULL;
    }
    if (err == NULL) err = "BEGIN failed for implicit transaction";
    return err;
}

static const char *commit_parent(Lua L)
{
    SP sp = getsp(L);
    if (sp->in_parent_trans && sp->make_parent_trans) {
        db_commit_int(L, &sp->rc);
        if (sp->rc == 0) {
            sp->in_parent_trans = 0;
            sp->make_parent_trans = 0;
            return NULL;
        }
        if (sp->error == NULL)
            sp->error = "COMMIT failed for implicit transaction";
        return sp->error;
    }
    return NULL;
}

static void *dispatch_lua_thread(void *lt)
{
    dbthread_t *l_thread = lt;
    struct sqlclntstate clnt;
    reset_clnt(&clnt, l_thread->clnt->sb, 1);
    clnt.want_stored_procedure_trace =
        l_thread->clnt->want_stored_procedure_trace;
    clnt.dbtran.mode = l_thread->clnt->dbtran.mode;
    clnt.is_newsql = l_thread->clnt->is_newsql;
    clnt.write_response = l_thread->clnt->write_response;
    clnt.read_response = l_thread->clnt->read_response;
    clnt.sp = l_thread->sp;
    clnt.sql = l_thread->sql;
    clnt.must_close_sb = 0;
    clnt.exec_lua_thread = 1;
    clnt.trans_has_sp = 1;
    pthread_mutex_init(&clnt.wait_mutex, NULL);
    pthread_cond_init(&clnt.wait_cond, NULL);
    pthread_mutex_init(&clnt.write_lock, NULL);
    pthread_mutex_init(&clnt.dtran_mtx, NULL);
    strcpy(clnt.tzname, l_thread->clnt->tzname);

    dispatch_sql_query(&clnt); // --> exec_thread()

    cleanup_clnt(&clnt);
    pthread_mutex_destroy(&clnt.wait_mutex);
    pthread_cond_destroy(&clnt.wait_cond);
    pthread_mutex_destroy(&clnt.write_lock);
    pthread_mutex_destroy(&clnt.dtran_mtx);

    l_thread->finished_run = 1;
    l_thread->lua_tid = 0;
    pthread_cond_broadcast(&l_thread->lua_thread_cond);
    pthread_cond_destroy(&l_thread->lua_thread_cond);

    return NULL;
}

typedef struct {
    SP sp1;
    SP sp2;
} mycallback_t;

int mycallback(void *arg_, int cols, char **text, char **name)
{
    mycallback_t *arg = arg_;
    SP sp1 = arg->sp1;
    SP sp2 = arg->sp2;

    tmptbl_info_t *tmp1;
    LIST_FOREACH(tmp1, &sp1->tmptbls, entries)
    {
        char n1[MAXTABLELEN], n2[MAXTABLELEN];
        two_part_tbl_name(tmp1->name, n1, n2);
        if (strcmp(text[0], n2) == 0) {
            tmptbl_info_t *tmp2 = malloc(sizeof(tmptbl_info_t));
            strcpy(tmp2->name, tmp1->name);
            tmp2->lk = tmp1->lk;
            tmp2->rootpg = atoi(text[1]);
            tmp2->sql = strdup(text[2]);
            LIST_INSERT_HEAD(&sp2->tmptbls, tmp2, entries);
            return 0;
        }
    }
    return 2;
}

static int copy_tmptbl_info(dbtable_t *t, SP sp1, SP sp2)
{
    char n1[MAXTABLELEN], n2[MAXTABLELEN];
    two_part_tbl_name(t->table_name, n1, n2);
    char sql[512];
    sprintf(sql, "select name,rootpage,sql from sqlite_temp_master "
                 "where type='table' and tbl_name='%s'",
            n2);
    mycallback_t arg = {.sp1 = sp1, .sp2 = sp2};
    return sqlite3_exec(getdb(sp1), sql, mycallback, &arg, NULL);
}

#define copy_type(dbtype, lua_t)                                               \
    const lua_t *f1 = lua_topointer(lua1, index);                              \
    lua_t *f2 = lua_newuserdata(lua2, sizeof(lua_t));                          \
    *f2 = *f1;                                                                 \
    luaL_getmetatable(lua2, dbtype);                                           \
    lua_setmetatable(lua2, -2);

static void copy_comdb2_type(lua_State *lua1, lua_State *lua2, int index,
                             int allow_tmptbl, hash_t *tmptbls)
{
    if (luabb_istype(lua1, index, DBTYPES_INTEGER)) {
        copy_type(dbtypes.integer, lua_int_t);
    } else if (luabb_istype(lua1, index, DBTYPES_REAL)) {
        copy_type(dbtypes.real, lua_real_t);
    } else if (luabb_istype(lua1, index, DBTYPES_DATETIME)) {
        copy_type(dbtypes.datetime, lua_datetime_t);
    } else if (luabb_istype(lua1, index, DBTYPES_DECIMAL)) {
        copy_type(dbtypes.decimal, lua_dec_t);
    } else if (luabb_istype(lua1, index, DBTYPES_INTERVALYM)) {
        copy_type(dbtypes.intervalym, lua_intervalym_t);
    } else if (luabb_istype(lua1, index, DBTYPES_INTERVALDS)) {
        copy_type(dbtypes.intervalds, lua_intervalds_t);
    } else if (luabb_istype(lua1, index, DBTYPES_CSTRING)) {
        copy_type(dbtypes.cstring, lua_cstring_t);
        if (f1->is_null) {
            f2->val = NULL;
        } else {
            f2->val = strdup(f1->val);
        }
    } else if (luabb_istype(lua1, index, DBTYPES_BLOB)) {
        copy_type(dbtypes.blob, lua_blob_t);
        if (f1->is_null) {
            f2->val.data = NULL;
        } else {
            f2->val.data = malloc(f1->val.length);
            memcpy(f2->val.data, f1->val.data, f1->val.length);
        }
    } else if (luabb_istype(lua1, index, DBTYPES_DBTABLE)) {
        if (!allow_tmptbl)
            return; // luabb_error(lua2, NULL, "attemp to return temp table");
        dbtable_t *src = lua_touserdata(lua1, index);
        dbtable_t *dest = lua_newuserdata(lua2, sizeof(dbtable_t));
        memcpy(dest, src, sizeof(dbtable_t));
        luaL_getmetatable(lua2, dbtypes.dbtable);
        lua_setmetatable(lua2, -2);
        if (dest->is_temp_tbl &&
            !hash_find_readonly(tmptbls, dest->table_name)) {
            SP sp1 = getsp(lua1);
            SP sp2 = getsp(lua2);
            if (copy_tmptbl_info(dest, sp1, sp2) != 0) {
                // TODO FIXME XXX
                // Handle failure - need to cleaup
                // return luaL_error(lua1, "error passing temp tables to
                // thread");
                abort();
            }
            hash_add(tmptbls, dest->table_name);
        }
    }
}

static int copy_state_table(lua_State *lua1, lua_State *lua2, int i,
                            int allow_tmptbl, hash_t *tmptbls)
{
    int type;
    lua_newtable(lua2);
    int new_table = lua_gettop(lua2);
    lua_pushnil(lua1);
    while (lua_next(lua1, i) != 0) {
        type = lua_type(lua1, -2);
        if (type == LUA_TUSERDATA) {
            copy_comdb2_type(lua1, lua2, -2, allow_tmptbl, tmptbls);
        } else if (lua_isnumber(lua1, -2)) {
            double num = lua_tonumber(lua1, -2);
            lua_pushnumber(lua2, num);
        } else {
            const char *arg = lua_tostring(lua1, -2);
            lua_pushstring(lua2, arg);
        }

        type = lua_type(lua1, -1);
        if (type == LUA_TUSERDATA) {
            copy_comdb2_type(lua1, lua2, -1, allow_tmptbl, tmptbls);
        } else if (lua_isnumber(lua1, -1)) {
            double num = lua_tonumber(lua1, -1);
            lua_pushnumber(lua2, num);
        } else if (lua_istable(lua1, -1)) {
            int num = lua_gettop(lua1);
            copy_state_table(lua1, lua2, num, allow_tmptbl, tmptbls);
        } else {
            const char *arg = lua_tostring(lua1, -1);
            lua_pushstring(lua2, arg);
        }
        lua_settable(lua2, new_table);
        lua_pop(lua1, 1);
    }
    return 0;
}

static int copy_state_stacks(Lua lua1, Lua lua2, int allow_tmptbl)
{
    hash_t *tmptbls = NULL;
    if (allow_tmptbl) {
        tmptbls = hash_init_str(0);
    }
    int num_returns = lua_gettop(lua1);
    for (int i = 1; i <= num_returns; i++) {
        int type = lua_type(lua1, i);
        if (type == LUA_TUSERDATA) {
            copy_comdb2_type(lua1, lua2, i, allow_tmptbl, tmptbls);
        } else if (lua_istable(lua1, i)) {
            copy_state_table(lua1, lua2, i, allow_tmptbl, tmptbls);
            lua_settop(lua1, num_returns);
        } else if (lua_isnumber(lua1, i)) {
            double num = lua_tonumber(lua1, i);
            lua_pushnumber(lua2, num);
        } else {
            const char *arg = lua_tostring(lua1, i);
            if (arg == NULL)
                lua_pushnil(lua2);
            else
                lua_pushstring(lua2, arg);
        }
    }
    if (tmptbls) {
        hash_free(tmptbls);
    }
    return num_returns;
}

static int get_func_by_ref(Lua L, const char *entry, char **err)
{
    // return function to call on the lua stack
    lua_getglobal(L, "_SP");
    lua_pushstring(L, "cdb2_func_refs");
    lua_gettable(L, -2); // result, _SP{}
    if (!lua_istable(L, -1)) {
        goto bad;
    }
    lua_remove(L, 1); // _SP{}
    lua_pushstring(L, entry);
    lua_gettable(L, -2);
    if (!lua_isfunction(L, -1)) {
        goto bad;
    }
    lua_remove(L, 1); // cdb2_func_refs
    // return with function on stack
    return 0;

bad:
    // return with nothing on the stack
    lua_settop(L, 0);
    *err = strdup("get_func_by_ref failed");
    return -1;
}

// return 'main' function to call on the lua stack
static int get_func_by_name(Lua L, const char *func, char **err)
{
    lua_getglobal(L, "_SP");
    lua_pushstring(L, "cdb2_func_names");
    lua_gettable(L, -2); // result, _SP{}
    if (!lua_istable(L, -1)) {
        logmsg(LOGMSG_ERROR, "%s fail at %d\n", __func__, __LINE__);
        *err = strdup("get_func_by_name failed");
        return -1;
    }
    lua_pushstring(L, func);
    lua_gettable(L, -2);
    if (!lua_isfunction(L, -1)) {
        if (strcmp(func, "main") != 0) {
            logmsg(LOGMSG_ERROR, "%s fail at %d\n", __func__, __LINE__);
            *err = strdup("get_func_by_name failed");
            return -2;
        }
        lua_pop(L, 1); // nil
        // local function main()  -- not found
        // look for _SP.spname
        SP sp = getsp(L);
        lua_pushstring(L, sp->spname);
        lua_gettable(L, -3); // _SP.spname ?
        if (!lua_isfunction(L, -1)) {
            logmsg(LOGMSG_ERROR, "%s fail at %d\n", __func__, __LINE__);
            *err = strdup("get_func_by_name failed");
            return -3;
        }
    }
    lua_insert(L, 1);
    lua_settop(L, 1);
    return 0;
}

static int process_src(Lua L, const char *src, char **err)
{
    int rc;
    if ((rc = luaL_dostring(L, src)) != 0) {
        *err = strdup(lua_tostring(L, -1));
        return -1;
    }
    // TODO FIXME XXX: HOW IS THERE CRAP ON THE STACK HERE??
    lua_settop(L, 0);
    return 0;
}

static int have_tran_funcs(Lua L)
{
    luaL_getmetatable(L, dbtypes.db);
    lua_getfield(L, -1, "begin");
    int have = !lua_isnil(L, -1);
    lua_pop(L, 2);
    return have;
}

static void remove_tran_funcs(Lua L)
{
    luaL_getmetatable(L, dbtypes.db);
    for (int i = 0; tran_funcs[i].name; ++i) {
        lua_pushnil(L);
        lua_setfield(L, -2, tran_funcs[i].name);
    }
    lua_pop(L, 1);
}

static void remove_consumer(Lua L)
{
    luaL_getmetatable(L, dbtypes.db);
    lua_pushnil(L);
    lua_setfield(L, -2, "consumer");
}

static void remove_emit(Lua L)
{
    luaL_getmetatable(L, dbtypes.db);
    lua_pushnil(L);
    lua_setfield(L, -2, "emit");

    luaL_getmetatable(L, dbtypes.dbstmt);
    lua_pushnil(L);
    lua_setfield(L, -2, "emit");
}

static void update_tran_funcs(Lua L, int in_tran)
{
    int have = have_tran_funcs(L);
    if (in_tran) {
        if (have) remove_tran_funcs(L);
        return;
    }
    if (have) return;
    luaL_getmetatable(L, dbtypes.db);
    luaL_openlib(L, NULL, tran_funcs, 0);
    lua_pop(L, 1);
}

static void drop_temp_tables(SP sp)
{
    tmptbl_info_t *tbl;
    LIST_FOREACH(tbl, &sp->tmptbls, entries)
    {
        char n1[MAXTABLELEN], n2[MAXTABLELEN];
        two_part_tbl_name(tbl->name, n1, n2);
        char drop_sql[128];
        sprintf(drop_sql, "DROP TABLE temp.\"%s\"", n2);
        sqlite3 *db = getdb(sp);
        sp->clnt->skip_peer_chk = 1;
        sqlite3_exec(db, drop_sql, NULL, NULL, NULL);
        sp->clnt->skip_peer_chk = 0;
    }
}

// SP ready to run again
static void reset_sp(SP sp)
{
    if (sp->lua) {
        lua_gc(sp->lua, LUA_GCCOLLECT, 0);
        drop_temp_tables(sp);
    }
    sp->in_parent_trans = 0;
    sp->make_parent_trans = 0;
    if (sp->parent == sp) {
        if (sp->clntname) {
            for (int i = 0; i < sp->ntypes; ++i) {
                free(sp->clntname[i]);
                sp->clntname[i] = NULL;
            }
            free(sp->clntname);
            sp->clntname = NULL;
        }
        free(sp->clnttype);
        sp->clnttype = NULL;
    }
    free_tmptbls(sp);
    if (sp->buf) {
        free(sp->buf);
        sp->buf = NULL;
    }
    if (sp->error) {
        free(sp->error);
        sp->error = NULL;
    }
    sp->have_consumer = 0;
    sp->pingpong = 0;
    sp->ntypes = 0;
    sp->bufsz = 0;
    sp->rc = 0;
    sp->nrows = 0;
    sp->num_instructions = 0;
    sp->max_num_instructions = gbl_max_lua_instructions;
    LIST_INIT(&sp->dbstmts);
    LIST_INIT(&sp->tmptbls);
}

static void free_spversion(SP sp)
{
    sp->spname[0] = 0;
    free(sp->src);
    sp->src = NULL;
    sp->spversion.version_num = 0;
    free(sp->spversion.version_str);
    sp->spversion.version_str = NULL;
}

// SP can't be used anymore
static void close_sp_int(SP sp, int freesp)
{
    if (!sp) return;
    reset_sp(sp);
    if (sp->lua) lua_close(sp->lua);
    comdb2ma mspace = sp->mspace;
    free_spversion(sp);
    comdb2ma_destroy(mspace);
    free(sp);
}

static int db_create_thread_int(Lua lua, const char *funcname)
{
    /* Make a new cloned working state. */
    dbthread_t *lt;
    new_lua_t(lt, dbthread_t, DBTYPES_THREAD);
    pthread_mutex_init(&lt->lua_thread_mutex, NULL);
    pthread_cond_init(&lt->lua_thread_cond, NULL);

    SP sp = getsp(lua);
    SP newsp = NULL;
    char *err = NULL;
    int rc;
    if ((newsp = create_sp(&err)) == NULL) {
        free(err);
        return -1;
    }
    Lua newlua = newsp->lua;
    update_tran_funcs(newlua, sp->clnt->in_client_trans);

    lua_sethook(newlua, InstructionCountHook, 0, 1); /*This means no hook.*/
    lt->lua = newlua;
    lt->sp = newsp;
    lt->sql = sp->clnt->sql;
    lt->clnt = sp->clnt;

    strcpy(newsp->spname, sp->spname);
    newsp->spversion = sp->spversion;
    if (newsp->spversion.version_str)
        newsp->spversion.version_str = strdup(newsp->spversion.version_str);
    newsp->parent = sp->parent;

    if (process_src(newlua, sp->src, &err) != 0) goto bad;

    if ((rc = get_func_by_name(newlua, funcname, &err)) != 0) {
        goto bad;
    }

    copy_state_stacks(lua, newlua, 1);

    pthread_attr_t attr; // small stack for dispatch_lua_thread
    pthread_attr_init(&attr);
#ifdef PTHREAD_STACK_MIN
    pthread_attr_setstacksize(&attr, PTHREAD_STACK_MIN + 16 * 1024);
#endif
    rc = pthread_create(&lt->lua_tid, &attr, dispatch_lua_thread, lt);
    pthread_attr_destroy(&attr);
    if (rc == 0) return 1;
    luabb_error(lua, sp, "failed to create thread");
bad:
    free(err);
    lua_close(newlua);
    reset_sp(newsp);
    return 0;
}

static int dbthread_error(lua_State *lua)
{
    dbthread_t *lt = lua_touserdata(lua, -1);
    if (lt) {
        lua_pushstring(lua, lt->error);
        return 1;
    } else {
        lua_pushstring(lua, "");
        return 1;
    }
}

static int dbthread_free(lua_State *lua)
{
    dbthread_t *lt = lua_touserdata(lua, -1);
    void *rc;
    if ((lt->finished_run == 0) && lt->lua_tid) {
        pthread_join(lt->lua_tid, &rc);
        lt->lua_tid = 0;
    }
    return 0;
}

static int dbthread_join(Lua lua1)
{
    dbthread_t *lt = lua_touserdata(lua1, -1);
    pthread_mutex_lock(&lt->lua_thread_mutex);
    while (lt->finished_run == 0) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        pthread_cond_timedwait(&lt->lua_thread_cond, &lt->lua_thread_mutex,
                               &ts);
    }
    pthread_mutex_unlock(&lt->lua_thread_mutex);

    Lua lua2 = lt->lua;
    SP sp = lt->sp;
    int num_returns = copy_state_stacks(lua2, lua1, 0);
    if (sp->error) strncpy(lt->error, sp->error, sizeof(lt->error));
    close_sp_int(sp, 1);
    return num_returns;
}

static int dbstmt_free(Lua L)
{
    dbstmt_t *stmt = lua_touserdata(L, -1);
    donate_stmt(getsp(L), stmt);
    return 0;
}

static int dbstmt_bind(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.dbstmt);
    return dbstmt_bind_int(L, lua_touserdata(L, 1));
}

static inline void setup_first_sqlite_step(SP sp, dbstmt_t *dbstmt)
{
    if (dbstmt->fetched) {
        // tbls already locked by previous step()
        return;
    }
    run_stmt_setup(sp->clnt, dbstmt->stmt);
    dbstmt->fetched = 1;
    if (dbstmt->rec == NULL) {
        // Not a prepared-stmt.
        // tbls locked by get_prepared_stmt_try_lock()
        return;
    }
    if (dbstmt->initial) {
        // Initial run of prepared-stmt.
        // tbls locked by get_prepared_stmt_try_lock()
        dbstmt->initial = 0;
        return;
    }
    // Need to lock tbls. We may be holding some other tbl locks and locking
    // schema can cause deadlock; trylock instead.
    if (tryrdlock_schema_lk() != 0) {
        luaL_error(sp->lua, sqlite3ErrStr(SQLITE_SCHEMA));
        return;
    }
    int rc = sqlengine_prepare_engine(sp->thd, sp->clnt, 0);
    if (rc == 0) {
        sqlite3LockStmtTables(dbstmt->stmt);
        unlock_schema_lk();
    } else {
        unlock_schema_lk();
        luaL_error(sp->lua, sqlite3ErrStr(rc));
        return;
    }
}

static int dbstmt_exec(Lua lua)
{
    SP sp = getsp(lua);
    sqlite3 *sqldb = getdb(sp);

    luaL_checkudata(lua, 1, dbtypes.dbstmt);
    dbstmt_t *dbstmt = lua_touserdata(lua, 1);
    no_stmt_chk(lua, dbstmt);
    setup_first_sqlite_step(sp, dbstmt);
    sqlite3_stmt *stmt = dbstmt->stmt;
    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
        ;
    dbstmt->rows_changed = sqlite3_changes(sqldb);
    if (rc == SQLITE_DONE) {
        sqlite3_reset(stmt);
        db_reset(lua);
        lua_pushinteger(lua, 0);
        return 1;
    }
    const char *errstr = NULL;
    sql_check_errors(sp->clnt, sqldb, stmt, &errstr);
    donate_stmt(getsp(lua), dbstmt);
    return luabb_error(lua, sp, errstr);
}

static int dbstmt_fetch(Lua lua)
{
    luaL_checkudata(lua, 1, dbtypes.dbstmt);
    dbstmt_t *dbstmt = lua_touserdata(lua, 1);
    no_stmt_chk(lua, dbstmt);
    setup_first_sqlite_step(getsp(lua), dbstmt);
    int rc = stmt_sql_step(lua, dbstmt);
    if (rc == SQLITE_ROW) return 1;
    donate_stmt(getsp(lua), dbstmt);
    return 0;
}

static int dbstmt_emit(Lua L)
{
    SP sp = getsp(L);
    luaL_checkudata(L, 1, dbtypes.dbstmt);
    dbstmt_t *dbstmt = lua_touserdata(L, 1);
    no_stmt_chk(L, dbstmt);
    setup_first_sqlite_step(sp, dbstmt);
    sqlite3_stmt *stmt = dbstmt->stmt;
    int cols = sqlite3_column_count(stmt);
    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        l_send_back_row(L, stmt, cols);
    }
    reset_stmt(sp, dbstmt);
    if (rc == SQLITE_DONE) rc = 0;
    return push_and_return(L, rc);
}

static int dbstmt_close(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.dbstmt);
    dbstmt_t *dbstmt = lua_touserdata(L, -1);
    donate_stmt(getsp(L), dbstmt);
    return 0;
}

static int dbstmt_rows_changed(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.dbstmt);
    const dbstmt_t *dbstmt = lua_topointer(L, 1);
    no_stmt_chk(L, dbstmt);
    lua_pushinteger(L, dbstmt->rows_changed);
    return 1;
}

static int db_exec(Lua lua)
{
    luaL_checkudata(lua, 1, dbtypes.db);
    lua_remove(lua, 1);

    SP sp = getsp(lua);

    int rc;
    const char *sql = lua_tostring(lua, -1);
    if (sql == NULL) {
        luabb_error(lua, sp, "expected sql string");
        return 2;
    }

    while (isspace(*sql))
        ++sql;

    sqlite3_stmt *stmt = NULL;
    if ((rc = lua_prepare_sql(lua, sp, sql, &stmt)) != 0) {
        lua_pushnil(lua);
        lua_pushinteger(lua, rc);
        return 2;
    }
    dbstmt_t *dbstmt = new_dbstmt(lua, sp, stmt);
    if (sqlite3_stmt_readonly(stmt)) {
        // dbstmt:fetch() will run it
        lua_pushinteger(lua, 0);
        return 2;
    }

    // a write stmt - run it now
    setup_first_sqlite_step(sp, dbstmt);
    sqlite3 *sqldb = getdb(sp);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
        ;
    if (rc == SQLITE_DONE) {
        dbstmt->rows_changed = sqlite3_changes(sqldb);
        sp->rc = 0;
    } else {
        const char *err;
        sp->rc = lua_check_errors(sp->clnt, sqldb, stmt, &err);
        sp->error = strdup(err);
    }

    if (sp->rc) {
        lua_pushnil(lua);
        lua_pushinteger(lua, sp->rc);
    } else {
        lua_pushinteger(lua, 0); /* Success return code */
    }
    return 2;
}

static int db_prepare(Lua L)
{
    SP sp = getsp(L);
    luaL_checkudata(L, 1, dbtypes.db);
    const char *sql = luabb_tostring(L, 2);
    lua_settop(L, 0);
    if (sql == NULL) {
        luabb_error(L, sp, "bad argument to 'prepare'");
        return 2;
    }
    sqlite3_stmt *stmt = NULL;
    struct sql_state *rec = calloc(1, sizeof(*rec));
    if (lua_prepare_sql_int(L, sp, sql, &stmt, rec) != 0) {
        free(rec);
        return 2;
    }
    dbstmt_t *dbstmt = new_dbstmt(L, sp, stmt);
    dbstmt->initial = 1;
    dbstmt->rec = rec;
    sp->prev_dbstmt = dbstmt;
    lua_pushinteger(L, 0);
    return 2;
}

static int db_create_thread(Lua L)
{
    // funcarg5
    // ...
    // funcarg2
    // funcarg1
    // func
    // db

    luaL_checkudata(L, 1, dbtypes.db);
    lua_remove(L, 1);

    luaL_checktype(L, 1, LUA_TFUNCTION);
    lua_getglobal(L, "_SP");
    lua_getfield(L, -1, "cdb2_func_refs");
    lua_remove(L, -2); // _SP

    lua_pushvalue(L, 1); // key for lookup is func; move to top
    lua_remove(L, 1);
    lua_gettable(L, -2);

    const char *funcname = lua_tostring(L, -1);
    lua_pop(L, 2); // string, cdb2_func_refs

    // funcarg5
    // ...
    // funcarg2
    // funcarg1

    return db_create_thread_int(L, funcname);
}

static int db_table(lua_State *lua)
{
    luaL_checkudata(lua, 1, dbtypes.db);
    lua_remove(lua, 1);

    switch (lua_gettop(lua)) {
    case 1: return comdb2_table(lua);
    case 2: return new_temp_table(lua);
    default: return luabb_error(lua, NULL, "bad arguments to 'table'");
    }
}

static int db_cast(Lua L)
{
    /* db:cast(src obj, dest type as string) */
    luaL_checkudata(L, 1, dbtypes.db);
    lua_remove(L, 1);

    const char *typename = lua_tostring(L, 2);
    lua_remove(L, 2);

    int type = luabb_type_by_name(typename);
    luabb_typeconvert_int(L, 1, type, typename);

    return 1;
}

static int db_sleep(Lua lua)
{
    luaL_checkudata(lua, 1, dbtypes.db);
    luaL_checknumber(lua, 2);
    int secs = lua_tonumber(lua, 2);
    sleep(secs);
    return 0;
}

/*
** local csv=[[a,b,"hello, world!","is ""quoted"" back there"]]
** local tbl = db:csv_to_table(csv)
** => #tbl = 4
**    tbl[1] = a
**    tbl[2] = b
**    tbl[3] = hello, world!
**    tbl[4] = is "quoted" back there
*/
static int db_csv_to_table(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.db);
    lua_remove(L, 1);

    CSVReader csv = {0};
    csv.in = luaL_checkstring(L, 1);
    csv.nLine = 1;
    csv.lua = L;
    csv.cSeparator = ',';
    csv_append_char(&csv, 0); /* To ensure sCsv.z is allocated */

    lua_newtable(L);
    int cols = 0, lines = 0;
    lua_newtable(L);
    while (csv_read_one_field(&csv)) {
        lua_pushstring(L, csv.z);
        lua_rawseti(L, -2, ++cols);
        if (csv.cTerm != csv.cSeparator) {
            lua_rawseti(L, -2, ++lines);
            if (csv.cTerm == 0) break;
            cols = 0;
            lua_newtable(L);
        }
    }
    free(csv.z);
    if (lines == 1) lua_rawgeti(L, -1, 1);
    return 1;
}

static int cson_to_table_annotated(Lua, cson_value *, int);
static int db_json_to_table(Lua lua)
{
    int rc;
    if (lua_gettop(lua) < 2) {
        return luaL_error(lua, "bad arguments to 'json_to_table'");
    }
    luaL_checkudata(lua, 1, dbtypes.db);
    int annotate = 0;
    if (lua_gettop(lua) == 3) {
        luaL_checktype(lua, 3, LUA_TTABLE);
        lua_pushnil(lua);
        int processed = 0;
        while (lua_next(lua, -2)) {
            const char *key = luabb_tostring(lua, -2);
            if (strcmp(key, "type_annotate") == 0) {
                if (luabb_type(lua, -1) == DBTYPES_LBOOLEAN) {
                    annotate = lua_toboolean(lua, -2);
                    processed = 1;
                    lua_pop(lua, 1);
                    break;
                }
            }
        }
        if (processed == 0) {
            return luaL_error(lua, "bad argument #2 to 'json_to_table'");
        }
        lua_pop(lua, 1);
    }
    const char *json = luabb_tostring(lua, 2);
    cson_value *cson = NULL;
    if ((rc = cson_parse_string(&cson, json, strlen(json), NULL, NULL)) != 0) {
        luaL_error(lua, "Parsing JSON rc:%d err:%s", rc, cson_rc_string(rc));
    }
    lua_newtable(lua);
    rc = cson_to_table_annotated(lua, cson, annotate);
    cson_free_value(cson);
    if (rc != 0) {
        luaL_error(lua, "bad argument #1 to json_to_table");
    }
    return 1;
}

typedef struct {
#define CONV_FLAG_UTF8_FATAL 0x01
#define CONV_FLAG_UTF8_NIL 0x02
#define CONV_FLAG_UTF8_TRUNCATE 0x04
#define CONV_FLAG_UTF8_HEX 0x08
#define CONV_FLAG_UTF8_MASK 0x0f
#define CONV_FLAG_ANNOTATE 0x10
    unsigned flag;

#define CONV_REASON_UTF8_FATAL 0x01
#define CONV_REASON_UTF8_NIL 0x02
#define CONV_REASON_UTF8_TRUNCATE 0x04
#define CONV_REASON_UTF8_HEX 0x08
#define CONV_REASON_ARGS_FATAL 0x10
    unsigned reason;   // output param
    const char *error; // output param
} json_conv;

static int process_json_conv(Lua L, json_conv *conv)
{
    const char *key, *value;
    switch (luabb_type(L, -2)) {
    case DBTYPES_LSTRING:
    case DBTYPES_CSTRING:
        key = luabb_tostring(L, -2);
        if (strcmp(key, "invalid_utf8") == 0) {
            value = luabb_tostring(L, -1);
            if (value == NULL) {
                return -1;
            } else if (strcmp(value, "fail") == 0) {
                conv->flag |= CONV_FLAG_UTF8_NIL;
                return 0;
            } else if (strcmp(value, "truncate") == 0) {
                conv->flag |= CONV_FLAG_UTF8_TRUNCATE;
                return 0;
            } else if (strcmp(value, "hex") == 0) {
                conv->flag |= CONV_FLAG_UTF8_HEX;
                return 0;
            } else {
                return -2;
            }
        } else if (strcmp(key, "type_annotate") == 0) {
            if (luabb_type(L, -1) == DBTYPES_LBOOLEAN) {
                if (lua_toboolean(L, -2)) {
                    conv->flag |= CONV_FLAG_ANNOTATE;
                }
                return 0;
            } else {
                return -3;
            }
        }
    }
    return -4;
}

static cson_value *table_to_cson(Lua, int, json_conv *);
static int db_table_to_json(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.db);
    luaL_checktype(L, 2, LUA_TTABLE);
    json_conv conv = {0};
    if (lua_gettop(L) == 3) {
        luaL_checktype(L, 3, LUA_TTABLE);
        lua_pushnil(L);
        int processed = 0;
        while (lua_next(L, -2)) {
            if (process_json_conv(L, &conv) != 0) {
            out:
                return luaL_error(L, "bad argument #2 to 'table_to_json'");
            }
            processed = 1;
            lua_pop(L, 1);
        }
        if (processed == 0) goto out;
        lua_pop(L, 1);
    }
    if ((conv.flag & CONV_FLAG_UTF8_MASK) == 0)
        conv.flag |= CONV_FLAG_UTF8_FATAL;
    cson_value *cson = table_to_cson(L, 0, &conv);
    if (cson == NULL) {
        if (conv.reason & (CONV_REASON_UTF8_FATAL | CONV_REASON_ARGS_FATAL)) {
            return luaL_error(L, conv.error);
        }
        lua_pushnil(L); // CONV_REASON_UTF8_NIL
    } else {
        cson_buffer buf = cson_buffer_empty;
        cson_output_buffer(cson, &buf, NULL);
        lua_pushstring(L, (char *)buf.mem);
        cson_buffer_reserve(&buf, 0);
        cson_free_value(cson);
    }
    // non-zero rc if nil'd, truncated or hexified cstring
    lua_pushinteger(L, conv.reason);
    return 2;
}

static int db_emit_int(Lua L)
{
    if (luabb_istype(L, 1, DBTYPES_DBTABLE)) {
        return dbtable_emit(L);
    } else if (luabb_istype(L, 1, DBTYPES_DBSTMT)) {
        return dbstmt_emit(L);
    } else if (lua_istable(L, 1)) {
        return luatable_emit(L);
    } else {
        int rc = l_send_back_row(L, NULL, lua_gettop(L));
        return push_and_return(L, rc);
    }
}

static int db_emit(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.db);
    lua_remove(L, 1);
    return db_emit_int(L);
}

static int db_emiterror(lua_State *lua)
{
    SP sp = getsp(lua);
    const char *errstr = lua_tostring(lua, -1);
    if (errstr) {
        logmsg(LOGMSG_ERROR, "err: %s\n", errstr);
        if (sp->rc == 0) sp->rc = -1;
        struct sqlclntstate *clnt = sp->clnt;
        clnt->write_response(clnt, RESPONSE_ERROR, (void *)errstr, sp->rc);
    }
    return 0;
}

static int db_column_name(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.db);
    luaL_checkstring(L, 2);
    luaL_checknumber(L, 3);
    SP sp = getsp(L);
    SP parent = sp->parent;
    char *name = strdup(luabb_tostring(L, 2));
    int index = lua_tonumber(L, 3);
    if (name == NULL || index < 1) {
        free(name);
        return luaL_error(L, "bad arguments to 'column_name'");
    }
    struct sqlclntstate *parent_clnt = parent->clnt;
    if (check_param_range(parent_clnt, index) != 0) {
        free(name);
        return luaL_error(L,
                          "bad arguments to 'column_name' for typed-statement");
    }
    pthread_mutex_lock(parent->emit_mutex);
    if (parent_clnt->osql.sent_column_data) {
        pthread_mutex_unlock(parent->emit_mutex);
        free(name);
        return luaL_error(L, "attempt to change column name");
    }
    new_col_info(sp, index);
    --index;
    if (parent->clntname[index]) {
        free(parent->clntname[index]);
        parent->clntname[index] = NULL;
    }
    parent->clntname[index] = name;
    pthread_mutex_unlock(parent->emit_mutex);
    lua_pushinteger(L, 0);
    return 1;
}

static int db_column_type(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.db);
    luaL_checkstring(L, 2);
    luaL_checknumber(L, 3);
    SP sp = getsp(L);
    SP parent = sp->parent;
    char *name = strdup(luabb_tostring(L, 2));
    int index = lua_tonumber(L, 3);
    if (name == NULL || index < 1) {
        free(name);
        return luaL_error(L, "bad arguments to 'column_type'");
    }
    struct sqlclntstate *parent_clnt = parent->clnt;
    if (typed_stmt(parent_clnt)) {
        free(name);
        return luaL_error(L, "attempt to set column type for typed-statement");
    }
    int clnttype;
    if (strcmp(name, "short") == 0 || strcmp(name, "int") == 0 ||
        strcmp(name, "integer") == 0 || strcmp(name, "longlong") == 0 ||
        strcmp(name, "u_short") == 0 || strcmp(name, "u_int") == 0) {
        clnttype = SQLITE_INTEGER;
    } else if (strcmp(name, "float") == 0 || strcmp(name, "double") == 0 ||
               strcmp(name, "number") == 0 || strcmp(name, "real") == 0) {
        clnttype = SQLITE_FLOAT;
    } else if (strcmp(name, "blob") == 0 || strcmp(name, "byte") == 0) {
        clnttype = SQLITE_BLOB;
    } else if (strcmp(name, "datetimeus") == 0) {
        clnttype = SQLITE_DATETIMEUS;
    } else if (strcmp(name, "datetime") == 0 || strncmp(name, "date", 4) == 0) {
        clnttype = SQLITE_DATETIME;
    } else if (strcmp(name, "intervaldsus") == 0) {
        clnttype = SQLITE_INTERVAL_DSUS;
    } else if (strcmp(name, "intervalds") == 0) {
        clnttype = SQLITE_INTERVAL_DS;
    } else if (strcmp(name, "intervalym") == 0) {
        clnttype = SQLITE_INTERVAL_YM;
    } else {
        /* If the type is not known lets have the type as
        ** TEXT just like sqlite. */
        clnttype = SQLITE_TEXT;
    }
    free(name);
    pthread_mutex_lock(parent->emit_mutex);
    if (parent_clnt->osql.sent_column_data) {
        pthread_mutex_unlock(parent->emit_mutex);
        return luaL_error(L, "attempt to change column type");
    }
    new_col_info(sp, index);
    --index;
    parent->clnttype[index] = clnttype;
    pthread_mutex_unlock(parent->emit_mutex);
    lua_pushinteger(L, 0);
    return 1;
}

static int db_num_columns(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.db);
    luaL_checknumber(L, 2);
    SP sp = getsp(L);
    SP parent = sp->parent;
    struct sqlclntstate *parent_clnt = parent->clnt;
    if (typed_stmt(parent_clnt)) {
        return luaL_error(
            L, "attempt to set number of columns for typed-statement");
    }
    pthread_mutex_lock(parent->emit_mutex);
    if (parent_clnt->osql.sent_column_data) {
        pthread_mutex_unlock(parent->emit_mutex);
        return luaL_error(L, "attempt to change number of columns");
    }
    int num_cols = lua_tonumber(L, 2);
    if (num_cols < parent->ntypes) {
        for (int i = num_cols - 1; i < parent->ntypes; ++i) {
            free(parent->clntname[i]);
            parent->clntname[i] = NULL;
        }
        parent->ntypes = num_cols;
    } else {
        new_col_info(sp, num_cols);
    }
    pthread_mutex_unlock(parent->emit_mutex);
    lua_pushinteger(L, 0);
    return 1;
}

static int db_reset(lua_State *lua)
{
    SP sp = getsp(lua);
    /*
    if (sp->thd->stmt) {
        sqlite3_reset(sp->thd->stmt);
        sp->thd->stmt = NULL;
    }
    */
    sp->rc = 0;
    if (sp->error) {
        free(sp->error);
        sp->error = NULL;
    }
    return 0;
}

static int db_get_trans(Lua L)
{
    struct sqlclntstate *clnt = getsp(L)->clnt;
    lua_pushboolean(L, clnt->in_client_trans);
    lua_pushstring(L, tranlevel_tostr(clnt->dbtran.mode));
    return 2;
}

static int db_copyrow(Lua lua)
{
    if (lua_istable(lua, 1) && lua_istable(lua, 2)) {
        lua_remove(lua, 1);
    }
    if (!lua_istable(lua, 1)) {
        lua_pushnil(lua);
        return 1;
    }
    lua_newtable(lua);
    int new_table = lua_gettop(lua);
    lua_pushnil(lua); /* first key */
    while (lua_next(lua, 1) != 0) {
        lua_pushvalue(lua, -2);       /* Move the key to the top. */
        lua_pushvalue(lua, -2);       /* Move the value to the top. */
        lua_settable(lua, new_table); /* set the value in new table.*/
        lua_pop(lua,
                1); /* Pop the value, leaves only key to be used in lua_next. */
    }
    return 1;
}

static int db_print(Lua lua)
{
    int nargs = lua_gettop(lua);

    if (nargs > 2)
        return luabb_error(lua, NULL, "wrong number of  arguments %d", nargs);

    SP sp = getsp(lua);
    const char *trace = lua_tostring(lua, -1);
    if (trace == NULL) return 0;

    struct sqlclntstate *clnt = sp->clnt;
    int rc  = clnt->write_response(clnt, RESPONSE_DEBUG, (void*)trace, 0);
    if (rc)
        return luabb_error(lua, sp, "%s: couldn't send results back", __func__);

    return 0;
}

static int db_isnull(lua_State *lua)
{
    lua_pushboolean(lua, luabb_isnull(lua, -1));
    return 1;
}

static int db_setnull(Lua lua)
{
    lua_cstring_t *c;
    lua_blob_t *b;
    if (lua_type(lua, -1) != LUA_TUSERDATA) {
        return luaL_error(lua, "bad argument to 'setnull'");
    }

    lua_dbtypes_t *t = (lua_dbtypes_t *)lua_topointer(lua, -1);
    assert(t->magic == DBTYPES_MAGIC);
    assert(t->dbtype > DBTYPES_MINTYPE);
    assert(t->dbtype < DBTYPES_MAXTYPE);
    switch (t->dbtype) {
    case DBTYPES_CSTRING:
        c = (lua_cstring_t *)t;
        free(c->val);
        c->val = NULL;
        break;
    case DBTYPES_BLOB:
        b = (lua_blob_t *)t;
        free(b->val.data);
        b->val.data = NULL;
        b->val.length = 0;
        break;
    default: return luaL_error(lua, "bad argument to 'setnull'");
    }
    t->is_null = 1;
    return 0;
}

static int db_settyped(Lua lua)
{
    lua_cstring_t *c;
    lua_blob_t *b;
    if (lua_type(lua, -1) != LUA_TUSERDATA) {
        return luaL_error(lua, "bad argument to 'settyped'");
    }

    lua_dbtypes_t *t = (lua_dbtypes_t *)lua_topointer(lua, -1);
    assert(t->magic == DBTYPES_MAGIC);
    assert(t->dbtype > DBTYPES_MINTYPE);
    assert(t->dbtype < DBTYPES_MAXTYPE);
    t->is_typed = 1;
    return 0;
}

static int db_setmaxinstructions(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.db);
    luaL_checknumber(L, 2);

    SP sp = getsp(L);
    int num = lua_tonumber(L, -1);
    if (num <= 99 || num > 1000000000) {
        return luabb_error(
            L, NULL, "Supported number of instructions Max:1000000000 Min:100");
    }
    sp->max_num_instructions = num;
    return 0;
}

static int db_getinstructioncount(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.db);
    SP sp = getsp(L);
    lua_pushinteger(L, sp->num_instructions);
    return 1;
}

static int db_now(Lua lua)
{
    SP sp;
    dttz_t dt;
    const char *tz;
    int precision;
    struct timespec ts;
    datetime_t datetime;
    cdb2_client_datetimeus_t cdtus;
    cdb2_client_datetime_t cdtms;

    const char *z = NULL;
    int nargs = lua_gettop(lua);
    sp = getsp(lua);
    tz = sp->clnt->tzname;

    if (nargs > 2)
        return luaL_error(lua, "wrong number of arguments %d", (nargs - 1));
    if (nargs <= 1)
        precision = sp->clnt->dtprec;
    else {
        z = luabb_tostring(lua, 2);
        if (z != NULL) {
            DTTZ_TEXT_TO_PREC(
                z, precision, 0,
                return luaL_error(lua, "incorrect precision %s", z));
        }
    }

    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) goto err;
    if (timespec_to_dttz(&ts, &dt, precision) != 0) goto err;
    if (precision == DTTZ_PREC_MSEC) {
        if (dttz_to_client_datetime(&dt, tz, &cdtms) != 0) goto err;
        client_datetime_to_datetime_t(&cdtms, &datetime, 0);
    } else {
        if (dttz_to_client_datetimeus(&dt, tz, &cdtus) != 0) goto err;
        client_datetimeus_to_datetime_t(&cdtus, &datetime, 0);
    }
    luabb_pushdatetime(lua, &datetime);
    return 1;
err:
    return luabb_error(lua, sp, "datetime conversion failed");
}

static int db_settimezone(lua_State *lua)
{
    SP sp = getsp(lua);
    const char *tz = lua_tostring(lua, -1);
    strcpy(sp->clnt->tzname, tz);
    lua_pop(lua, -1);
    return 0;
}

static int db_gettimezone(Lua L)
{
    SP sp = getsp(L);
    lua_pushstring(L, sp->clnt->tzname);
    return 1;
}

static int db_setdatetimeprecision(lua_State *lua)
{
    int rc = 0;
    SP sp = getsp(lua);
    const char *z = lua_tostring(lua, -1);
    DTTZ_TEXT_TO_PREC(z, sp->clnt->dtprec, 0, goto err);
    if (0) {
    err:
        rc = luaL_error(lua, "incorrect precision %s", z);
    }
    lua_pop(lua, -1);
    return rc;
}

static int db_getdatetimeprecision(Lua L)
{
    SP sp = getsp(L);
    switch (sp->clnt->dtprec) {
    case DTTZ_PREC_MSEC: lua_pushstring(L, "millisecond"); break;
    case DTTZ_PREC_USEC: lua_pushstring(L, "microsecond"); break;
    default:
        luabb_error(L, sp, "incorrect precision %d", sp->clnt->dtprec);
        break;
    }
    return 1;
}

static int db_bind_warn = 20;
static int db_bind(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.db);
    SP sp = getsp(L);
    if (db_bind_warn > 0) {
        --db_bind_warn;
        if (sp->spversion.version_num) {
            logmsg(LOGMSG_WARN,
                   "deprecated method db:bind() called in sp:%s ver:%d\n",
                   sp->spname, sp->spversion.version_num);
        } else {
            logmsg(LOGMSG_WARN,
                   "deprecated method db:bind() called in sp:%s ver:%s\n",
                   sp->spname, sp->spversion.version_str);
        }
    }
    return dbstmt_bind_int(L, sp->prev_dbstmt);
}

static int db_null(Lua L)
{
    lua_getglobal(L, "NULL");
    return 1;
}

static int db_sp(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.db);
    char *name = (char *)luaL_checkstring(L, 2);
    struct spversion_t spversion = {0};
    if (lua_isnumber(L, 3)) {
        spversion.version_num = lua_tonumber(L, 3);
    } else if (lua_isstring(L, 3)) {
        spversion.version_str = (char *)lua_tostring(L, 3);
    }
    char *err = NULL;
    rdlock_schema_lk();
    char *src = load_src(name, &spversion, 0, &err);
    unlock_schema_lk();
    free(spversion.version_str);
    if (src == NULL) {
        luabb_error(L, getsp(L), err);
        lua_pushnil(L);
        free(err);
        return 1;
    }
    size_t size = strlen(src);
    char buf[size + 32];
    sprintf(buf, "%s\nreturn main", src);
    free(src);
    if (luaL_dostring(L, buf) != 0) {
        luabb_error(L, getsp(L), lua_tostring(L, -1));
        lua_pushnil(L);
    }
    return 1;
}

static int db_error(lua_State *lua)
{
    SP sp = getsp(lua);
    if (sp && sp->error) {
        lua_pushstring(lua, sp->error);
    } else {
        lua_pushstring(lua, "");
    }
    return 1;
}

static int db_consumer(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.db);

    SP sp = getsp(L);
    if (sp->parent->have_consumer) {
        return 0;
    }
    char spname[strlen(sp->spname) + 1];
    strcpy(spname, sp->spname);
    Q4SP(qname, spname);

    struct dbtable *db = getqueuebyname(qname);
    if (db == NULL) {
        return luaL_error(L, "trigger not found for sp:%s", spname);
    }

    struct consumer *consumer = db->consumers[0];
    if (consumer == NULL) {
        return luaL_error(L, "consumer not found for sp:%s", spname);
    }

    enum consumer_t type = consumer_type(consumer);
    trigger_reg_t *t;
    if (type == CONSUMER_TYPE_DYNLUA) {
        // will block until registration completes
        trigger_reg_init(t, spname);
        int rc = luabb_trigger_register(L, t);
        if (rc != CDB2_TRIG_REQ_SUCCESS) return luaL_error(L, sp->error);
    } else {
        luabb_error(L, sp, "no such consumer");
        lua_pushnil(L);
        return 1;
    }

    dbconsumer_t *q;
    size_t sz = dbconsumer_sz(spname);
    new_lua_t_sz(L, q, dbconsumer_t, DBTYPES_DBCONSUMER, sz);
    if (setup_dbconsumer(q, consumer, db, t) != 0) {
        luabb_error(L, sp, "failed to register consumer with qdb");
        lua_pushnil(L);
        return 1;
    }
    sp->parent->have_consumer = 1;
    return 1;
}

static int db_bootstrap(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.db);
    lua_pop(L, 1);

    lua_Debug ar = {0};
    const int frame = 2; // 0 -> db_bootstrap , 1 -> comdb2_main, 2 -> SP
    if (lua_getstack(L, frame, &ar) == 0) return 0;

    lua_getglobal(L, "_SP");
    lua_newtable(L);
    lua_setfield(L, -2, "cdb2_func_refs");
    lua_newtable(L);
    lua_setfield(L, -2, "cdb2_func_names");

    lua_getfield(L, 1, "cdb2_func_refs");
    lua_getfield(L, 1, "cdb2_func_names");

    // Stack:
    //   3: table for lookup by name
    //   2: table for lookup by ref
    //   1: _SP{}

    int n = 1; // 1st local variable
    const char *name;
    while ((name = lua_getlocal(L, &ar, n++)) != NULL) {
        if (!lua_isfunction(L, -1)) {
            lua_pop(L, 1);
            continue;
        }
        lua_pushvalue(L, -1);     // push copy of func
        lua_setfield(L, 3, name); // _SP.cdb2_func_names[funcname] = func
        lua_pushstring(L, name);
        lua_settable(L, 2); // _SP.cdb2_func_refs[func] = funcname
    }
    lua_settop(L, 0);

    return 0;
}

static const luaL_Reg db_funcs[] = {
    {"exec", db_exec},
    {"prepare", db_prepare},
    {"table", db_table},
    {"cast", db_cast},
    {"csv_to_table", db_csv_to_table},
    {"json_to_table", db_json_to_table},
    {"table_to_json", db_table_to_json},
    {"emit", db_emit},
    {"emiterror", db_emiterror},
    {"column_name", db_column_name},
    {"column_type", db_column_type},
    {"num_columns", db_num_columns},
    {"reset", db_reset},
    {"get_trans", db_get_trans},
    {"copyrow", db_copyrow},
    {"print", db_print},
    {"isnull", db_isnull},
    {"setnull", db_setnull},
    {"settyped", db_settyped},
    {"setmaxinstructions", db_setmaxinstructions},
    {"getinstructioncount", db_getinstructioncount},
    {"now", db_now},
    {"settimezone", db_settimezone},
    {"gettimezone", db_gettimezone},
    {"setdatetimeprecision", db_setdatetimeprecision},
    {"getdatetimeprecision", db_getdatetimeprecision},
    {"bind", db_bind},
    {"null", db_null},
    {"NULL", db_null}, // why upper-case? -- deprecate
    {"sp", db_sp},
    {"sqlerror", db_error}, // every error isn't from SQL -- deprecate
    {"error", db_error},
    /************ CONSUMER **************/
    {"consumer", db_consumer},
    /************** DEBUG ***************/
    {"debug", db_debug},
    {"db_debug", db_db_debug},
    {"trace", db_trace},
    /************ INTERNAL **************/
    {"sleep", db_sleep},
    {"bootstrap", db_bootstrap},
    {NULL, NULL}
};

static void init_db_funcs(Lua L)
{
    db_t *db = lua_newuserdata(L, sizeof(db_t));
    init_new_t(db, DBTYPES_DB);

    luaL_newmetatable(L, dbtypes.db);

    lua_pushinteger(L, CDB2__ERROR_CODE__DUP_OLD);
    lua_setfield(L, -2, "err_dup");

    lua_pushinteger(L, CDB2ERR_VERIFY_ERROR);
    lua_setfield(L, -2, "err_verify");

    lua_pushinteger(L, CDB2ERR_FKEY_VIOLATION);
    lua_setfield(L, -2, "err_fkey");

    lua_pushinteger(L, CDB2ERR_NULL_CONSTRAINT);
    lua_setfield(L, -2, "err_null_constraint");

    lua_pushinteger(L, CDB2ERR_CONSTRAINTS);
    lua_setfield(L, -2, "err_selectv");

    lua_pushinteger(L, CDB2ERR_CONV_FAIL);
    lua_setfield(L, -2, "err_conv");

    lua_pushstring(L, "__index");
    lua_pushvalue(L, -2);
    lua_settable(L, -3); // db.metatable.__index = db.metatable
    luaL_openlib(L, NULL, db_funcs, 0);

    lua_setmetatable(L, -2);
    lua_setglobal(L, "db");
}

static const struct luaL_Reg dbtable_funcs[] = {
    {"insert", dbtable_insert},
    {"copyfrom", dbtable_copyfrom},
    {"name", dbtable_name},
    {"emit", dbtable_emit},
    {"where", dbtable_where},
    {NULL, NULL}
};

static void init_dbtable_funcs(Lua L)
{
    luaL_newmetatable(L, dbtypes.dbtable);
    lua_pushvalue(L, -1);
    lua_setfield(L, -2, "__index");
    luaL_openlib(L, NULL, dbtable_funcs, 0);
    lua_pop(L, 1);
}

static const struct luaL_Reg dbstmt_funcs[] = {
    {"__gc", dbstmt_free},
    {"bind", dbstmt_bind},
    {"exec", dbstmt_exec},
    {"fetch", dbstmt_fetch},
    {"emit", dbstmt_emit},
    {"close", dbstmt_close},
    {"rows_changed", dbstmt_rows_changed},
    {NULL, NULL}
};

static void init_dbstmt_funcs(Lua L)
{
    luaL_newmetatable(L, dbtypes.dbstmt);
    lua_pushvalue(L, -1);
    lua_setfield(L, -2, "__index");
    luaL_openlib(L, NULL, dbstmt_funcs, 0);
    lua_pop(L, 1);
}

static const struct luaL_Reg dbthread_funcs[] = {
    {"__gc", dbthread_free},
    {"join", dbthread_join},
    {"sqlerror", dbthread_error}, // every error isn't from SQL -- deprecate
    {"error", dbthread_error},
    {NULL, NULL}
};

static void init_dbthread_funcs(Lua L)
{
    luaL_newmetatable(L, dbtypes.dbthread);
    lua_pushvalue(L, -1);
    lua_setfield(L, -2, "__index");
    luaL_openlib(L, NULL, dbthread_funcs, 0);
    lua_pop(L, 1);
}

static const struct luaL_Reg dbconsumer_funcs[] = {
    {"__gc", dbconsumer_free},
    {"get", dbconsumer_get},
    {"poll", dbconsumer_poll},
    {"consume", dbconsumer_consume},
    {"emit", dbconsumer_emit},
    {NULL, NULL}
};

static void init_dbconsumer_funcs(Lua L)
{
    luaL_newmetatable(L, dbtypes.dbconsumer);
    lua_pushvalue(L, -1);
    lua_setfield(L, -2, "__index");
    luaL_openlib(L, NULL, dbconsumer_funcs, 0);
    lua_pop(L, 1);
}

static void *lua_alloc(void *ud, void *ptr, size_t osize, size_t nsize)
{
    return comdb2_realloc(ud, ptr, nsize);
}

static int create_sp_int(SP sp, char **err)
{
    Lua lua;

    sp->mspace = lua_mem_init();
    lua = lua_newstate(lua_alloc, sp->mspace);

    lua_setsp(lua, sp);
    luaL_openlibs(lua);
    lua_atpanic(lua, l_panic);

    sp->lua = lua;
    sp->max_num_instructions = gbl_max_lua_instructions;
    LIST_INIT(&sp->dbstmts);
    LIST_INIT(&sp->tmptbls);

    init_db_funcs(lua);
    init_dbtable_funcs(lua);
    init_dbstmt_funcs(lua);
    init_dbthread_funcs(lua);
    init_dbconsumer_funcs(lua);
    init_dbtypes(lua);

    lua_newtable(lua);
    lua_setglobal(lua, "_SP");

    extern int gbl_allow_lua_print;
    if (!gbl_allow_lua_print) {
        if (luaL_dostring(lua, "function print(...) end") != 0) {
            *err = strdup(lua_tostring(lua, -1));
            close_sp_int(sp, 1);
            return -1;
        }
    }

    disable_global_variables(lua);

    /* To be given as lrl value. */
    lua_sethook(lua, InstructionCountHook, LUA_MASKCOUNT, 1);
    return 0;
}

static SP create_sp(char **err)
{
    SP sp = calloc(1, sizeof(struct stored_proc));
    if (create_sp_int(sp, err) != 0) {
        free(sp);
        return NULL;
    }
    return sp;
}

static int cson_to_table(Lua, cson_value *);
static int cson_push_value(Lua lua, cson_value *v)
{
    if (cson_value_is_undef(v)) {
        lua_pushnil(lua); // TODO: this just disappers in lua
    } else if (cson_value_is_null(v)) {
        luabb_pushnull(lua, DBTYPES_INTEGER);
    } else if (cson_value_is_bool(v)) {
        lua_pushboolean(lua, cson_value_get_bool(v));
    } else if (cson_value_is_integer(v)) {
        lua_pushinteger(lua, cson_value_get_integer(v));
    } else if (cson_value_is_double(v)) {
        lua_pushnumber(lua, cson_value_get_double(v));
    } else if (cson_value_is_string(v)) {
        lua_pushstring(lua, cson_value_get_cstr(v));
    } else if (cson_value_is_array(v) || cson_value_is_object(v)) {
        lua_newtable(lua);
        return cson_to_table(lua, v);
    }
    return 0;
}

static int cson_to_table(Lua lua, cson_value *v)
{
    int rc;
    if (cson_value_is_object(v)) {
        cson_object *o = cson_value_get_object(v);
        cson_object_iterator i;
        cson_object_iter_init(o, &i);
        cson_kvp *kv;
        while ((kv = cson_object_iter_next(&i)) != NULL) {
            lua_pushstring(lua, cson_string_cstr(cson_kvp_key(kv)));
            if (cson_push_value(lua, cson_kvp_value(kv)) != 0) return -1;
            lua_settable(lua, -3);
        }
    } else if (cson_value_is_array(v)) {
        cson_array *a = cson_value_get_array(v);
        unsigned int i, len = cson_array_length_get(a);
        for (i = 0; i < len; ++i) {
            if (cson_push_value(lua, cson_array_get(a, i)) != 0) return -1;
            lua_rawseti(lua, -2, i + 1);
        }
    } else {
        return -1;
    }
    return 0;
}

static int cson_push_null(Lua L, const char *type)
{
    int t;
    if (strcmp(type, "cstring") == 0)
        t = DBTYPES_CSTRING;
    else if (strcmp(type, "integer") == 0)
        t = DBTYPES_INTEGER;
    else if (strcmp(type, "double") == 0)
        t = DBTYPES_REAL;
    else if (strcmp(type, "blob") == 0)
        t = DBTYPES_BLOB;
    else if (strcmp(type, "datetime") == 0)
        t = DBTYPES_DATETIME;
    else if (strcmp(type, "decimal") == 0)
        t = DBTYPES_DECIMAL;
    else if (strcmp(type, "intervalds") == 0)
        t = DBTYPES_INTERVALDS;
    else if (strcmp(type, "intervalym") == 0)
        t = DBTYPES_INTERVALYM;
    else
        return 1;
    luabb_pushnull(L, t);
    return 0;
}

static const char *json_type = "type";
static const char *json_value = "value";

static int cson_push_value_annotated(Lua L, cson_value *val)
{
    cson_object *o = cson_value_get_object(val);
    cson_value *t = cson_object_get(o, json_type);
    cson_value *v = cson_object_get(o, json_value);
    if (t == NULL || v == NULL || !cson_value_is_string(t)) return -1;
    char const *type = cson_value_get_cstr(t);
    if (cson_value_is_null(v)) {
        return cson_push_null(L, type);
    } else if (strcmp(type, "object") == 0 || strcmp(type, "array") == 0) {
        lua_newtable(L);
        return cson_to_table_annotated(L, val, 1);
    } else if (strcmp(type, "string") == 0) {
        if (!cson_value_is_string(v)) return -1;
        lua_pushstring(L, cson_value_get_cstr(v));
    } else if (strcmp(type, "cstring") == 0) {
        if (!cson_value_is_string(v)) return -1;
        luabb_pushcstring(L, cson_value_get_cstr(v));
    } else if (strcmp(type, "hexstring") == 0) {
        if (!cson_value_is_string(v)) return -1;
        const char *s = cson_value_get_cstr(v);
        size_t l = strlen(s);
        if (l % 2 != 0) return -1;
        uint8_t *b = malloc(l / 2);
        luabb_fromhex(b, (uint8_t *)s, l);
        luabb_pushcstring_dl(L, s);
    } else if (strcmp(type, "integer") == 0) {
        if (!cson_value_is_integer(v)) return -1;
        int64_t i = cson_value_get_integer(v);
        luabb_pushinteger(L, i);
    } else if (strcmp(type, "double") == 0) {
        if (!cson_value_is_double(v)) return -1;
        double d = cson_value_get_double(v);
        luabb_pushreal(L, d);
    } else if (strcmp(type, "number") == 0) {
        if (cson_value_is_integer(v)) {
            int64_t i = cson_value_get_integer(v);
            luabb_pushinteger(L, i);
        } else if (cson_value_is_double(v)) {
            double d = cson_value_get_double(v);
            lua_pushnumber(L, d);
        } else {
            return -1;
        }
    } else if (strcmp(type, "bool") == 0) {
        if (!cson_value_is_bool(v)) return -1;
        lua_pushboolean(L, cson_value_get_bool(v));
    } else if (strcmp(type, "blob") == 0) {
        if (!cson_value_is_string(v)) return -1;
        const char *s = cson_value_get_cstr(v);
        size_t l = strlen(s);
        uint8_t *b = malloc(l / 2);
        luabb_fromhex(b, (uint8_t *)s, l);
        blob_t x = {.data = b, .length = l / 2};
        luabb_pushblob_dl(L, &x);
    } else if (strcmp(type, "datetime") == 0) {
        if (!cson_value_is_string(v)) return -1;
        datetime_t d;
        lua_pushstring(L, cson_value_get_cstr(v));
        luabb_todatetime(L, -1, &d);
        lua_pop(L, 1);
        luabb_pushdatetime(L, &d);
    } else if (strcmp(type, "decimal") == 0) {
        if (!cson_value_is_string(v)) return -1;
        decQuad d;
        lua_pushstring(L, cson_value_get_cstr(v));
        luabb_todecimal(L, -1, &d);
        lua_pop(L, 1);
        luabb_pushdecimal(L, &d);
    } else if (strcmp(type, "intervalds") == 0) {
        if (!cson_value_is_string(v)) return -1;
        intv_t i;
        lua_pushstring(L, cson_value_get_cstr(v));
        luabb_tointervalds(L, -1, &i);
        lua_pop(L, 1);
        luabb_pushintervalds(L, &i);
    } else if (strcmp(type, "intervalym") == 0) {
        if (!cson_value_is_string(v)) return -1;
        intv_t i;
        lua_pushstring(L, cson_value_get_cstr(v));
        luabb_tointervalym(L, -1, &i);
        lua_pop(L, 1);
        luabb_pushintervalym(L, &i);
    } else {
        return -1;
    }
    return 0;
}

static int cson_to_table_annotated(Lua L, cson_value *val, int annotate)
{
    if (annotate == 0) return cson_to_table(L, val);
    if (!cson_value_is_object(val)) return -1;
    if (lua_checkstack(L, 5) == 0) return -1;
    cson_object *o = cson_value_get_object(val);
    cson_value *t = cson_object_get(o, json_type);
    cson_value *v = cson_object_get(o, json_value);
    if (t == NULL || v == NULL || !cson_value_is_string(t)) return -1;
    char const *type = cson_value_get_cstr(t);
    if (strcmp(type, "object") == 0) {
        cson_object *o = cson_value_get_object(v);
        cson_object_iterator i;
        cson_object_iter_init(o, &i);
        cson_kvp *kv;
        while ((kv = cson_object_iter_next(&i)) != NULL) {
            lua_pushstring(L, cson_string_cstr(cson_kvp_key(kv)));
            if (cson_push_value_annotated(L, cson_kvp_value(kv)) != 0)
                return -1;
            lua_settable(L, -3);
        }
    } else if (strcmp(type, "array") == 0) {
        cson_array *a = cson_value_get_array(v);
        unsigned int i, len = cson_array_length_get(a);
        for (i = 0; i < len; ++i) {
            if (cson_push_value_annotated(L, cson_array_get(a, i)) != 0)
                return -1;
            lua_rawseti(L, -2, i + 1);
        }
    } else {
        return -1;
    }
    return 0;
}

// value to convert is on top of stack
static cson_value *table_to_cson_array(Lua L, int lvl, json_conv *conv)
{
    cson_value *v = cson_value_new_array();
    cson_array *a = cson_value_get_array(v);
    int n = luaL_getn(L, -1);
    cson_array_reserve(a, n);
    for (int i = 0; i < n; ++i) {
        lua_rawgeti(L, -1, i + 1);
        cson_value *val = table_to_cson(L, lvl, conv);
        if (val == NULL) {
            cson_free_value(v);
            return NULL;
        }
        cson_array_append(a, val);
        lua_pop(L, 1);
    }
    return v;
}

// value to convert is on top of stack
static cson_value *table_to_cson_object(Lua L, int lvl, json_conv *conv)
{
    cson_value *v = cson_value_new_object();
    cson_object *o = cson_value_get_object(v);
    lua_pushnil(L);
    while (lua_next(L, -2)) {
        cson_value *val = table_to_cson(L, lvl, conv); // get value
        if (val == NULL) {
            cson_free_value(v);
            return NULL;
        }
        lua_pop(L, 1); // remove value

        lua_pushvalue(L, -1);                    // push copy of key
        const char *key = luabb_tostring(L, -1); // conv key to str
        cson_object_set(o, key, val);
        lua_pop(L, 1); // pop copy of key (now a string)
    }
    return v;
}

static int is_array(Lua L)
{
    int n = luaL_getn(L, -1);
    if (n < 1) return 0;
    lua_pushnumber(L, n);
    if (lua_next(L, -2)) {
        // not array: there is stuff after numeric index
        lua_pop(L, 2);
        return 0;
    }
    return 1;
}

// value to convert is on top of stack
static cson_value *table_to_cson(Lua L, int lvl, json_conv *conv)
{
    const char *type = NULL;
    ++lvl;
    if (lvl > 100 || lua_checkstack(L, 1) == 0) {
        conv->error = "too many nested tables";
        conv->reason |= CONV_REASON_ARGS_FATAL;
        return NULL;
    }
    cson_value *v = NULL;
    long long integer;
    double dbl;

    dbtypes_enum dbtype = luabb_dbtype(L, -1);
    if (dbtype > DBTYPES_MINTYPE && dbtype < DBTYPES_MAXTYPE) { // null chk
        const lua_dbtypes_t *nullchk = lua_touserdata(L, -1);
        if (nullchk->is_null) {
            switch (dbtype) {
            case DBTYPES_CSTRING: type = "cstring"; break;
            case DBTYPES_INTEGER: type = "integer"; break;
            case DBTYPES_REAL: type = "double"; break;
            case DBTYPES_BLOB: type = "blob"; break;
            case DBTYPES_DATETIME: type = "datetime"; break;
            case DBTYPES_DECIMAL: type = "decimal"; break;
            case DBTYPES_INTERVALDS: type = "intervalds"; break;
            case DBTYPES_INTERVALYM: type = "intervalym"; break;
            default:
                conv->error = "unsupported type for 'table_to_json'";
                conv->reason |= CONV_REASON_ARGS_FATAL;
                return NULL;
            }
            v = cson_value_null();
            goto annotate;
        }
    }

    int tostr = 0; // 1->validate utf8   2->no validation
    int utf8_len;
    switch (dbtype) {
    case DBTYPES_LTABLE:
        if (is_array(L)) {
            type = "array";
            v = table_to_cson_array(L, lvl, conv);
        } else {
            type = "object";
            v = table_to_cson_object(L, lvl, conv);
        }
        break;
    case DBTYPES_LBOOLEAN:
        type = "bool";
        v = cson_value_new_bool(lua_toboolean(L, -1));
        break;
    case DBTYPES_LNUMBER:
        type = "number";
        dbl = lua_tonumber(L, -1);
        integer = lua_tointeger(L, -1);
        v = (dbl == integer) ? cson_value_new_integer(integer)
                             : cson_value_new_double(dbl);
        break;
    case DBTYPES_LNIL:
        type = "nil";
        v = cson_value_null();
        break;
    case DBTYPES_INTEGER:
        type = "integer";
        luabb_tointeger(L, -1, &integer);
        v = cson_value_new_integer(integer);
        break;
    case DBTYPES_REAL:
        type = "double";
        luabb_toreal(L, -1, &dbl);
        v = cson_value_new_double(dbl);
        break;
    case DBTYPES_LSTRING:
        type = "string";
        tostr = 1;
        break;
    case DBTYPES_CSTRING:
        type = "cstring";
        tostr = 1;
        break;
    case DBTYPES_BLOB:
        type = "blob";
        tostr = 2;
        break;
    case DBTYPES_DECIMAL:
        type = "decimal";
        tostr = 2;
        break;
    case DBTYPES_DATETIME:
        type = "datetime";
        tostr = 2;
        break;
    case DBTYPES_INTERVALYM:
        type = "intervalym";
        tostr = 2;
        break;
    case DBTYPES_INTERVALDS:
        type = "intervalds";
        tostr = 2;
        break;
    default:
        conv->error = "unsupported type for 'table_to_json'";
        conv->reason |= CONV_REASON_ARGS_FATAL;
        return NULL;
    }
    if (tostr) {
        char *hexstr = NULL;
        const char *s = luabb_tostring(L, -1);
        if (tostr == 1) {
            if (utf8_validate(s, -1, &utf8_len) != 0) {
                if (conv->flag & CONV_FLAG_UTF8_FATAL) {
                    conv->error = "invalid utf-8 cstring in 'table_to_json'";
                    conv->reason |= CONV_REASON_UTF8_FATAL;
                    return NULL;
                } else if (conv->flag & CONV_FLAG_UTF8_NIL) {
                    conv->error = "invalid utf-8 cstring in 'table_to_json'";
                    conv->reason |= CONV_REASON_UTF8_NIL;
                    return NULL;
                } else if (conv->flag & CONV_FLAG_UTF8_TRUNCATE) {
                    conv->error = "invalid utf-8 cstring in 'table_to_json'";
                    conv->reason |= CONV_REASON_UTF8_TRUNCATE;
                } else if (conv->flag & CONV_FLAG_UTF8_HEX) {
                    conv->error = "invalid utf-8 cstring in 'table_to_json'";
                    conv->reason |= CONV_REASON_UTF8_HEX;
                    size_t slen = strlen(s) + 1; // include terminating null
                    size_t hexlen = slen * 2 + 1;
                    hexstr = malloc(hexlen);
                    util_tohex(hexstr, s, slen);
                    type = "hexstring";
                    s = hexstr;
                    utf8_len = hexlen;
                }
            }
        } else {
            utf8_len = strlen(s);
        }
        v = cson_value_new_string(s, utf8_len);
        free(hexstr);
    }
annotate:
    if (v == NULL) return NULL;
    if ((conv->flag & CONV_FLAG_ANNOTATE) == 0) return v;
    if (type == NULL) {
        cson_free_value(v);
        conv->error = "unsupported type for 'table_to_json'";
        conv->reason |= CONV_REASON_ARGS_FATAL;
        return NULL;
    }
    if (cson_value_is_null(v)) {
        cson_object *o = cson_new_object();
        cson_object_set(o, json_type,
                        cson_value_new_string(type, strlen(type)));
        cson_object_set(o, json_value, v);
        v = cson_object_value(o);
    } else {
        cson_object *o = cson_new_object();
        cson_object_set(o, json_type,
                        cson_value_new_string(type, strlen(type)));
        cson_object_set(o, json_value, v);
        v = cson_object_value(o);
    }
    return v;
}

static int l_send_back_row(Lua lua, sqlite3_stmt *stmt, int nargs)
{
    int rc = 0;
    SP sp = getsp(lua);
    assert(nargs > 0);
    if (nargs > MAXCOLUMNS) {
        return luabb_error(lua, sp, "attempt to read %d cols (maxcols:%d)",
                           nargs, MAXCOLUMNS);
    }
    release_locks_on_emit_row(sp->thd, sp->clnt);
    struct response_data arg = {0};
    arg.ncols = nargs;
    arg.stmt = stmt;
    arg.sp = sp;
    arg.pingpong = sp->pingpong;
    struct sqlclntstate *clnt = sp->parent->clnt;
    if (clnt->osql.sent_column_data == 0) {
        pthread_mutex_lock(sp->emit_mutex);
        if (clnt->osql.sent_column_data == 0) {
            new_col_info(sp, nargs);
            rc = clnt->write_response(clnt, RESPONSE_COLUMNS_LUA, &arg, 0);
            clnt->osql.sent_column_data = 1;
        }
        pthread_mutex_unlock(sp->emit_mutex);
        if (rc) return rc;
    }
    int type = stmt ? RESPONSE_ROW : RESPONSE_ROW_LUA;
    return clnt->write_response(clnt, type, &arg, 0);
}

static int flush_rows(SP sp)
{
    if (sp->clnt->sb == NULL)
        return 0;
    struct sqlclntstate *clnt = sp->clnt;
    if ((sp->parent && !sp->parent->clnt->osql.sent_column_data) ||
        (!sp->parent && sp->nrows == 0)) {
        return clnt->write_response(clnt, RESPONSE_ROW_LAST_DUMMY, NULL, 0);
    }
    return clnt->write_response(clnt, RESPONSE_ROW_LAST, NULL, 0);
}

static int push_param(Lua lua, struct sqlclntstate *clnt, struct schema *params,
                      int pos, int *blobno)
{
    void *bufp = clnt->tagbuf;
    void *nullbits = clnt->nullbits;
    int numblobs = clnt->numblobs;
    void **blobs = clnt->blobs;
    int *bloblens = clnt->bloblens;

    uint8_t *buf = bufp;
    char parmname[32];
    int isnull = 0;
    blob_t blob;
    int64_t ival;
    uint64_t uval;
    double dval;
    char *str;
    int datalen;
    dttz_t dt;
    int little_endian = 0;

    CDB2SQLQUERY *sqlquery = clnt->sql_query;

    struct field c_fld;
    struct field *f = NULL;

    if (params) {
        f = &params->member[pos];
        if (nullbits) isnull = btst(nullbits, pos) ? 1 : 0;
    } else {
        c_fld.type = convert_client_ftype(sqlquery->bindvars[pos]->type);
        c_fld.datalen = sqlquery->bindvars[pos]->value.len;
        c_fld.idx = -1;
        c_fld.name = sqlquery->bindvars[pos]->varname;
        c_fld.offset = 0;
        f = &c_fld;
        if ((buf = sqlquery->bindvars[pos]->value.data) == NULL) {
            isnull = 1;
        } else if (sqlquery->little_endian) {
            if ((f->type == CLIENT_INT) || (f->type == CLIENT_UINT) ||
                (f->type == CLIENT_REAL)) {
#ifndef _LINUX_SOURCE
                uint8_t val1[8];
                memcpy(&val1, buf, c_fld.datalen);
                const void *new_buf = buf_little_get(buf, c_fld.datalen, val1,
                                                     val1 + c_fld.datalen);
#else
                const void *new_buf =
                    buf_get(buf, c_fld.datalen, buf, buf + c_fld.datalen);
#endif
            }
            little_endian = 1;
        }
    }

    if (gbl_dump_sql_dispatched) {
        logmsg(LOGMSG_USER,
               "binding field %d name %s type %d %s pos %d null %d\n", pos,
               f->name, f->type, strtype(f->type), pos, isnull);
    }

    if (isnull) {
        luabb_pushnull(lua, DBTYPES_INTEGER);
        return 0;
    }

    int rc = 0;
    switch (f->type) {
    case CLIENT_INT:
        if ((rc = get_int_field(f, buf, &ival)) == 0)
            luabb_pushinteger(lua, ival);
        break;
    case CLIENT_UINT:
        if ((rc = get_uint_field(f, buf, &uval)) == 0)
            luabb_pushinteger(lua, uval);
        break;
    case CLIENT_REAL:
        if ((rc = get_real_field(f, buf, &dval)) == 0)
            luabb_pushreal(lua, dval);
        break;
    case CLIENT_CSTR:
    case CLIENT_PSTR:
    case CLIENT_PSTR2:
        if ((rc = get_str_field(f, buf, &str, &datalen)) == 0)
            luabb_pushcstringlen(lua, str, datalen);
        break;
    case CLIENT_BYTEARRAY:
        if ((rc = get_byte_field(f, buf, &blob.data, &blob.length)) == 0) {
            luabb_pushblob(lua, &blob);
        }
        break;
    case CLIENT_BLOB:
        if (params) {
            if ((rc = get_blob_field(*blobno, clnt, &blob.data,
                                     &blob.length)) == 0) {
                luabb_pushblob(lua, &blob);
                ++(*blobno);
            }
        } else {
            blob.data = buf;
            blob.length = f->datalen;
            luabb_pushblob(lua, &blob);
            ++(*blobno);
        }
        break;
    case CLIENT_DATETIME:
        if ((rc = get_datetime_field(f, buf, clnt->tzname, &dt,
                                     little_endian)) == 0) {
            /* TODO: need dttz -> datetime_t */
            cdb2_client_datetime_t t;
            datetime_t datetime;
            dttz_to_client_datetime(&dt, clnt->tzname, &t);
            client_datetime_to_datetime_t(&t, &datetime, 0);
            luabb_pushdatetime(lua, &datetime);
        }
        break;
    case CLIENT_DATETIMEUS:
        if ((rc = get_datetimeus_field(f, buf, clnt->tzname, &dt,
                                       little_endian)) == 0) {
            /* TODO: need dttz -> datetime_t */
            cdb2_client_datetimeus_t t;
            datetime_t datetime;
            dttz_to_client_datetimeus(&dt, clnt->tzname, &t);
            client_datetimeus_to_datetime_t(&t, &datetime, 0);
            luabb_pushdatetime(lua, &datetime);
        }
        break;
    case CLIENT_VUTF8:
        if (params) {
            if ((rc = get_blob_field(*blobno, clnt, (void **)&str, &datalen)) ==
                0) {
                luabb_pushcstringlen(lua, str, datalen);
                ++(*blobno);
            }
        } else {
            luabb_pushcstringlen(lua, (char *)buf, f->datalen);
            ++(*blobno);
        }
        break;
    default: logmsg(LOGMSG_ERROR, "Unknown type %d\n", f->type); rc = -1;
    }

    if (gbl_dump_sql_dispatched)
        logmsg(LOGMSG_USER, "pos %d %s type %d %s len %d null %d bind rc %d\n",
               pos, f->name, f->type, strtype(f->type), f->datalen, isnull, rc);

    return rc;
}

typedef enum {
    arg_err = -1,
    arg_end = 0,
    arg_null,
    arg_param,
    arg_int,
    arg_real,
    arg_str,
    arg_blob,
    arg_bool
} arg_t;

typedef struct {
    arg_t type;
    char buf[32]; // scratch buf
    char *mbuf;   // malloc buf
    union {
        int64_t i; // int arg or position of bound param
        double d;
        char *c;
        blob_t b;
    } u;
} sparg_t;

static int getparam(struct schema *params, CDB2SQLQUERY *sqlquery,
                    const char *name, int64_t *pos)
{
    int i;
    if (sqlquery) {
        for (i = 0; i < sqlquery->n_bindvars; ++i) {
            if (strcmp(sqlquery->bindvars[i]->varname, name) == 0) {
                *pos = i;
                return 0;
            }
        }
        return 1;
    } else {
        for (i = 0; i < params->nmembers; ++i) {
            if (strcmp(params->member[i].name, name) == 0) {
                *pos = i;
                return 0;
            }
        }
        return 1;
    }
}

static const char *getnext(const char *in, char **a, char **b, void *buf,
                           int bufsz)
{
    *a = NULL;
    ptrdiff_t len;
    const char *start;

    /* QUOTED ARG */
    if (*in == '"' || *in == '\'') {
        start = in++;
        char qt = *start;
        while (*in && *in != qt)
            ++in;
        if (*in != qt) return NULL;
        ++in;
        goto out;
    }

    /* UNQUOTED ARG */
    start = in;
    while (*in && *in != ')' && *in != ',' && !isspace(*in))
        ++in;

out:
    len = in - start;
    if (len == 0) return NULL;
    if (len + 1 < bufsz)
        *a = buf;
    else
        *a = malloc(len + 1);
    memcpy(*a, start, len);
    (*a)[len] = '\0';
    *b = *a + len;

    /* move pointer to next arg */
    while (isspace(*in))
        ++in;
    if (*in == ',') ++in;
    while (isspace(*in))
        ++in;
    return in;
}

static inline int ascii2num(int a)
{
    a = tolower(a);
    return isdigit(a) ? a - '0' : isalpha(a) ? 0x0a + a - 'a' : 0xff;
}

static int getarg(const char **s_, CDB2SQLQUERY *sqlquery,
                  struct schema *params, sparg_t *arg)
{
    arg->mbuf = NULL;
    uint8_t quoted = 0;
    char *endptr;
    char *a, *b;
    const char *s = *s_;
    while (isspace(*s))
        ++s;
    if (*s == ')' || *s == '\0') return arg_end;
    if ((s = getnext(s, &a, &b, arg->buf, sizeof(arg->buf))) == NULL) {
        goto err;
    }

    arg->type = arg_err;
    if (a != arg->buf) arg->mbuf = a;
    switch (*a) {
    case '"':
    case '\'':
        quoted = 1;
        if (a[1] != '@') {
            arg->type = arg_str;
            // lose the quotes
            arg->u.c = ++a;
            --b;
            b[0] = '\0';
            break;
        } else {
            // lose the quotes
            ++a;
            --b;
            b[0] = '\0';
            // fall through
        }
    case '@':
        arg->type = arg_param;
        ++a; // lose '@'
        if (!(params || sqlquery) ||
            getparam(params, sqlquery, a, &arg->u.i) != 0) {
            if (quoted) {
                arg->type = arg_str;
                arg->u.c = --a;
            } else {
                goto err;
            }
        }
        break;
    case 'x':
        arg->type = arg_blob;
        ++a;
        --b;
        if (*a != '\'' && *b != '\'') goto err;
        ++a;
        --b; // lose the quotes
        arg->u.b.length = b - a + 1;
        if (arg->u.b.length % 2 != 0) goto err;
        arg->u.b.length /= 2;
        arg->u.b.data = arg->u.b.length < sizeof(arg->buf)
                            ? arg->buf
                            : (arg->mbuf = malloc(arg->u.b.length));
        if (parseblob(a, arg->u.b.length * 2, arg->u.b.data) != 0) goto err;
        break;
    case 't':
    case 'T':
        arg->type = arg_bool;
        if (strcasecmp(a, "true") != 0) goto err;
        arg->u.i = 1;
        goto out;
        break;
    case 'f':
    case 'F':
        arg->type = arg_bool;
        if (strcasecmp(a, "false") != 0) goto err;
        arg->u.i = 0;
        goto out;
        break;
    }

    if (arg->type != arg_err) {
        goto out;
    }

    int len = b - a;
    if (len == 4 && strcasecmp(a, "null") == 0) {
        arg->type = arg_null;
        goto out;
    }

    errno = 0;
    arg->u.i = strtoll(a, &endptr, 10);
    if (errno == 0 && *endptr == '\0') {
        arg->type = arg_int;
        goto out;
    }
    if (*endptr != '.' && *endptr != 'e' && *endptr != 'E') {
        goto err;
    }

    errno = 0;
    arg->u.d = strtod(a, &endptr);
    if (errno == 0 && *endptr == '\0') {
        arg->type = arg_real;
        goto out;
    }

err:
    free(arg->mbuf);
    arg->type = arg_err;
    return arg->type;

out:
    *s_ = s;
    return arg->type;
}

static int debug_sp(struct sqlclntstate *clnt)
{
    int arg1 = 0;
    char *carg1 = NULL;
halt_here:
    debug_clnt = clnt;
    if (arg1 == 0) {
        gbl_break_lua = INT_MAX;
        gbl_break_spname = carg1;
    } else {
        gbl_break_lua = arg1;
    }
    clnt->want_stored_procedure_debug = 1;
wait_here:
    pthread_cond_broadcast(&lua_debug_cond); /* 1 debugger at a time. */
    pthread_mutex_lock(&lua_debug_mutex);
    pthread_cond_wait(&lua_debug_cond, &lua_debug_mutex);
    pthread_mutex_unlock(&lua_debug_mutex);
do_continue:
    logmsg(LOGMSG_USER, "CoNtInUe \n");
    info_buf.has_buffer = 0;
    pthread_mutex_lock(&lua_debug_mutex);
    int rc = sbuf2fread(info_buf.buffer, 250, 1, clnt->sb);
    if (rc && (strncmp(info_buf.buffer, "HALT", 4) == 0)) {
        pthread_mutex_unlock(&lua_debug_mutex);
        goto halt_here;
    } else if (rc) {
        info_buf.has_buffer = 1;
        if ((strncmp(info_buf.buffer, "cont", 4) == 0)) {
            pthread_mutex_unlock(&lua_debug_mutex);
            goto do_continue;
        }
        logmsg(LOGMSG_USER, "This was not continue \n");
        pthread_mutex_unlock(&lua_debug_mutex);
        goto wait_here;
    } else {
        pthread_mutex_unlock(&lua_debug_mutex);
    }

    if (debug_clnt == clnt) {
        debug_clnt = NULL;
    }
    clnt->sp = NULL;
    sleep(2);
    logmsg(LOGMSG_USER, "Exit debugging \n");
    return 0;
}

static int get_spname(struct sqlclntstate *clnt, const char **exec,
                      char *spname, char **err)
{
    const char *s = *exec;
    while (s && isspace(*s))
        s++;
    if (!s || strncasecmp(s, "exec", 4)) {
        *err = strdup("syntax error, expected 'exec'");
        return -1;
    }
    s += 4;

    const char *start, *end;
    if (has_sqlcache_hint(s, &start, &end)) s = end;

    while (isspace(*s))
        s++;
    if (strncasecmp(s, "procedure", 9)) {
        *err = strdup("syntax error, expected 'procedure'");
        return -1;
    }
    s += 9;

    /* Get procedure name */
    while (isspace(*s))
        s++;
    if (!isalpha(*s) && *s != '_') {
        *err = strdup("syntax error, expected procedure name");
        return -1;
    }
    start = s;

    /* allow sys., otherwise '.' is not a valid character  */
    int is_sys = IS_SYS(s);
    while (s && (isalnum(*s) || *s == '_' || (is_sys && *s == '.')))
        s++;

    if ((s - start + 1) > MAX_SPNAME) {
        *err = strdup("bad procedure name");
        return -1;
    }
    memcpy(spname, start, s - start);
    spname[s - start] = 0;

    /* Get procedure args */
    while (s && isspace(*s))
        s++;
    if (*s != '(') {
        *err = strdup("syntax error, expected '('");
        return -1;
    }
    s++;

    int i = strlen(s);
    end = s + i - 1;
    while (isspace(*end))
        --end;
    if (*end == ';') --end;
    while (isspace(*end))
        --end;
    if (*end != ')') {
        *err = strdup("syntax error, expected ')'");
        return -1;
    }

    *exec = s;
    return 0;
}

static void apply_clnt_override(struct sqlclntstate *clnt, SP sp)
{
    sp->spversion.version_num = clnt->spversion.version_num;
    if (clnt->spversion.version_str)
        sp->spversion.version_str = strdup(clnt->spversion.version_str);
}

// Clnt has override for this sp
static void process_clnt_sp_override(struct sqlclntstate *clnt)
{
    SP sp = clnt->sp;
    if (strcmp(clnt->spname, sp->spname) != 0) {
        free_spversion(sp);
    } else if (clnt->spversion.version_num) {
        if (clnt->spversion.version_num != sp->spversion.version_num) {
            free_spversion(sp);
        }
    } else if (clnt->spversion.version_str) {
        if (sp->spversion.version_str == NULL ||
            strcmp(clnt->spversion.version_str, sp->spversion.version_str) !=
                0) {
            free_spversion(sp);
        }
    }
    apply_clnt_override(clnt, sp);
}

static int setup_sp(char *spname, struct sqlthdstate *thd,
                    struct sqlclntstate *clnt,
                    int *new_vm, // out param
                    char **err)  // out param
{
    SP sp = clnt->sp;
    if (sp) {
        if (clnt->want_stored_procedure_trace ||
            clnt->want_stored_procedure_debug) {
            close_sp(clnt);
            sp = NULL;
        }
    }
    if (sp && sp->lua) {
        // Have lua vm
        if (strcmp(spname, clnt->spname) == 0) {
            // Clnt has override for this sp
            process_clnt_sp_override(clnt);
        } else if (strcmp(spname, sp->spname) != 0) {
            // Run some other sp
            free_spversion(sp);
        } else if (sp->spversion.version_num != 0) {
            // Have src for some version_num. Check if num is default.
            int bdberr;
            int num = bdb_get_sp_get_default_version(spname, &bdberr);
            if (num != sp->spversion.version_num) {
                free_spversion(sp);
            }
        } else if (sp->spversion.version_str) {
            // Have src for some version_str. Check if str is the default.
            char *version_str;
            if (bdb_get_default_versioned_sp(spname, &version_str) == 0) {
                int cmp = strcmp(sp->spversion.version_str, version_str);
                free(version_str);
                if (cmp) {
                    free_spversion(sp);
                }
            } else {
                // Failed to obtain default version_str
                free_spversion(sp);
            }
        }
        if (sp->lua_version != gbl_lua_version) {
            // Stale src
            free(sp->src);
            sp->src = NULL;
        }
    } else {
        // Create lua vm
        if (sp) {
            free_spversion(sp);
            if (create_sp_int(sp, err) != 0) {
                return -1;
            }
        } else if ((sp = create_sp(err)) == NULL) {
            return -1;
        }
        if (strcmp(spname, clnt->spname) == 0) {
            apply_clnt_override(clnt, sp);
        }
    }

    clnt->sp = sp;
    sp->clnt = clnt;
    sp->emit_mutex = &clnt->wait_mutex;
    sp->debug_clnt = clnt;
    sp->thd = thd;
    sp->parent = sp;
    sp->initial = 1;

    *new_vm = 0;
    if (sp->src == NULL) {
        int locked=0;
        if (!IS_SYS(spname)) {
            rdlock_schema_lk();
            locked = 1;
        }
        sp->src = load_src(spname, &sp->spversion, 1, err);
        sp->lua_version = gbl_lua_version;
        if (locked)
            unlock_schema_lk();
        if (sp->src == NULL) {
            close_sp(clnt);
            return -1;
        }
        *new_vm = 1;
        strcpy(sp->spname, spname);
    }
    if (clnt && (clnt->want_stored_procedure_trace ||
                 clnt->want_stored_procedure_debug)) {
        if (load_debugging_information(sp, err)) {
            return -1;
        }
    }
    return 0;
}

static int push_args(const char **argstr, struct sqlclntstate *clnt, char **err,
                     struct schema *params, int *argc)
{
    const char *s = *argstr;
    SP sp = clnt->sp;
    Lua lua = sp->lua;

    const char *msg = s;
    int argcnt = 0;
    sparg_t arg;
    int b = 0;
    blob_t blob;

    int rc;
    while ((rc = getarg(&s, clnt->sql_query, params, &arg)) > arg_end) {
        if ((rc = !lua_checkstack(lua, 1)) != 0) break;
        switch (arg.type) {
        case arg_null: luabb_pushnull(lua, DBTYPES_CSTRING); break;
        case arg_int: lua_pushinteger(lua, arg.u.i); break;
        case arg_real: lua_pushnumber(lua, arg.u.d); break;
        case arg_str: lua_pushstring(lua, arg.u.c); break;
        case arg_blob: luabb_pushblob(lua, &arg.u.b); break;
        case arg_bool: lua_pushboolean(lua, arg.u.i); break;
        case arg_param: rc = push_param(lua, clnt, params, arg.u.i, &b); break;
        default: rc = 99; break;
        }
        free(arg.mbuf);
        ++argcnt;
        if (rc) break;
        msg = s;
    }
    if (rc != arg_end) {
        *err = malloc(64);
        if (snprintf((char *)*err, 60, "bad argument -> %s", msg) >= 60) {
            strcat(*err, "...");
        }
        reset_sp(sp);
        return -1;
    }
    *argstr = s;
    *argc = argcnt;
    return 0;
}

static int run_sp_int(struct sqlclntstate *clnt, int argcnt, char **err)
{
    int rc;
    SP sp = clnt->sp;
    Lua lua = sp->lua;

    if ((rc = lua_pcall(lua, argcnt, LUA_MULTRET, 0)) != 0) {
        if (lua_gettop(lua) > 0 && !lua_isnil(lua, -1)) {
            *err = strdup(luabb_tostring(lua, -1));
        } else {
            *err = strdup("");
        }
        rc = -1;
        int tmp;
        /* Don't make new parent transaction on this rollback. */
        sp->make_parent_trans = 0;
        db_rollback_int(lua, &tmp);
    }

    if (gbl_break_lua && (gbl_break_lua == pthread_self())) {
        gbl_lua_version++;
        gbl_break_lua = 0;
    }

    return rc;
}

static int run_sp(struct sqlclntstate *clnt, int argcnt, char **err)
{
    int verifyretry_off = clnt->verifyretry_off;
    clnt->verifyretry_off = 1;
    int rc = run_sp_int(clnt, argcnt, err);
    clnt->verifyretry_off = verifyretry_off;
    return rc;
}

#define copy(dest, type, src, conv)                                            \
    dest = conv(*(type *)src);                                                 \
    src += sizeof(dest)

#define copypush(dest, type, src, conv, push)                                  \
    copy(dest, type, src, conv);                                               \
    push(lua, dest)

static uint8_t *push_trigger_field(Lua lua, char *oldnew, char *name,
                                   uint8_t type, uint8_t *payload)
{
    if (payload == NULL) return NULL;
    union {
        int16_t i16;
        uint16_t u16;
        int32_t i32;
        uint32_t u32;
        int64_t i64;
        uint64_t u64;
        float f;
        double d;
        char *s;
        void *b;
        datetime_t dt;
        intv_t in;
    } u;
    int32_t szstr;
    lua_blob_t *blob;
    cdb2_client_intv_ym_t *ym;
    cdb2_client_intv_ds_t *ds;
    cdb2_client_intv_dsus_t *dsus;

    lua_getfield(lua, -1, oldnew);
    switch (type) {
    case SP_FIELD_INT16:
        copypush(u.i16, int16_t, payload, ntohs, luabb_pushinteger);
        break;
    case SP_FIELD_UINT16:
        copypush(u.u16, uint16_t, payload, ntohs, luabb_pushinteger);
        break;
    case SP_FIELD_INT32:
        copypush(u.i32, int32_t, payload, ntohl, luabb_pushinteger);
        break;
    case SP_FIELD_UINT32:
        copypush(u.u32, uint32_t, payload, ntohl, luabb_pushinteger);
        break;
    case SP_FIELD_INT64:
        copypush(u.i64, int64_t, payload, flibc_ntohll, luabb_pushinteger);
        break;
    case SP_FIELD_UINT64:
        copypush(u.u64, uint64_t, payload, flibc_ntohll, luabb_pushinteger);
        break;
    case SP_FIELD_REAL32:
        copypush(u.f, float, payload, flibc_ntohf, luabb_pushreal);
        break;
    case SP_FIELD_REAL64:
        copypush(u.d, double, payload, flibc_ntohd, lua_pushnumber);
        break;
    case SP_FIELD_STRING:
        copy(szstr, int32_t, payload, ntohl);
        luabb_pushcstringlen(lua, (char *)payload, szstr);
        payload += szstr + 1;
        break;
    case SP_FIELD_BLOB:
    case SP_FIELD_BYTEARRAY:
        copy(szstr, int32_t, payload, ntohl);
        new_lua_t(blob, lua_blob_t, DBTYPES_BLOB);
        blob->val.length = szstr;
        blob->val.data = malloc(szstr);
        memcpy(blob->val.data, payload, szstr);
        payload += szstr;
        break;
    case SP_FIELD_DATETIME:
#ifdef _LINUX_SOURCE
        client_datetime_to_datetime_t((cdb2_client_datetime_t *)payload, &u.dt,
                                      1);
#else
        client_datetime_to_datetime_t((cdb2_client_datetime_t *)payload, &u.dt,
                                      0);
#endif
        luabb_pushdatetime(lua, &u.dt);
        payload += sizeof(cdb2_client_datetime_t);
        break;
    case SP_FIELD_DATETIMEUS:
#ifdef _LINUX_SOURCE
        client_datetimeus_to_datetime_t((cdb2_client_datetimeus_t *)payload,
                                        &u.dt, 1);
#else
        client_datetimeus_to_datetime_t((cdb2_client_datetimeus_t *)payload,
                                        &u.dt, 0);
#endif
        luabb_pushdatetime(lua, &u.dt);
        payload += sizeof(cdb2_client_datetimeus_t);
        break;
    case SP_FIELD_INTERVALYM:
        ym = (cdb2_client_intv_ym_t *)payload;
        u.in.type = INTV_YM_TYPE;
        u.in.sign = ntohl(ym->sign);
        u.in.u.ym.years = ntohl(ym->years);
        u.in.u.ym.months = ntohl(ym->months);
        luabb_pushintervalym(lua, &u.in);
        payload += sizeof(cdb2_client_intv_ym_t);
        break;
    case SP_FIELD_INTERVALDS:
        ds = (cdb2_client_intv_ds_t *)payload;
        u.in.type = INTV_DS_TYPE;
        u.in.sign = ntohl(ds->sign);
        u.in.u.ds.days = ntohl(ds->days);
        u.in.u.ds.mins = ntohl(ds->mins);
        u.in.u.ds.sec = ntohl(ds->sec);
        u.in.u.ds.frac = ntohl(ds->msec);
        u.in.u.ds.prec = DTTZ_PREC_MSEC;
        u.in.u.ds.conv = 1;
        luabb_pushintervalds(lua, &u.in);
        payload += sizeof(cdb2_client_intv_ds_t);
        break;
    case SP_FIELD_INTERVALDSUS:
        dsus = (cdb2_client_intv_dsus_t *)payload;
        u.in.type = INTV_DSUS_TYPE;
        u.in.sign = ntohl(dsus->sign);
        u.in.u.ds.days = ntohl(dsus->days);
        u.in.u.ds.mins = ntohl(dsus->mins);
        u.in.u.ds.sec = ntohl(dsus->sec);
        u.in.u.ds.frac = ntohl(dsus->usec);
        u.in.u.ds.prec = DTTZ_PREC_USEC;
        u.in.u.ds.conv = 1;
        luabb_pushintervalds(lua, &u.in);
        payload += sizeof(cdb2_client_intv_dsus_t);
        break;
    }
    lua_setfield(lua, -2, name);
    lua_pop(lua, 1);
    return payload;
}

static void push_trigger_null(Lua L, char *oldnew, char *name)
{
    lua_getfield(L, -1, oldnew);
    lua_getglobal(L, "NULL");
    lua_setfield(L, -2, name);
    lua_pop(L, 1);
}

static uint8_t *consume_field(Lua L, uint8_t *payload)
{
    uint8_t szfld = *payload;
    if (szfld == 0) {
        return NULL;
    }
    payload += 1;
    uint8_t type = *payload;
    payload += 1;
    uint8_t before = *payload;
    payload += 1;
    uint8_t after = *payload;
    payload += 1;
    char fld[szfld + 1];
    memcpy(fld, payload, szfld);
    fld[szfld] = '\0';
    payload += szfld + 1;

    if (before == FIELD_FLAG_NULL) {
        push_trigger_null(L, "old", fld);
    } else if (before == FIELD_FLAG_VALUE) {
        payload = push_trigger_field(L, "old", fld, type, payload);
    }

    if (after == FIELD_FLAG_NULL) {
        push_trigger_null(L, "new", fld);
    } else if (after == FIELD_FLAG_VALUE) {
        payload = push_trigger_field(L, "new", fld, type, payload);
    }
    return payload;
}

static int push_trigger_args_int(Lua L, dbconsumer_t *q, struct qfound *f, char **err)
{
    uint8_t *payload = ((uint8_t *)f->item) + f->dtaoff;
    size_t len = f->len - f->dtaoff;
    q->genid = f->item->genid;
    /*
    char header[] = "CDB2_UPD";
    if (memcmp(payload, header, sizeof(header)) != 0) {
        *err = strdup("bad payload header");
        return -1;
    }
    */
    uint8_t zeros[4] = {0};
    if (memcmp(payload + len - 4, zeros, sizeof(zeros)) != 0) {
        *err = strdup("bad payload tail");
        return -1;
    }

    len -= 4; // 4 bytes of zeroes
    uint8_t *end = payload + len;
    payload += 40; // 8 + 32

    int32_t flags = ntohl(*(int *)(payload));
    payload += 4;

    int sztbl = ntohs(*(short *)(payload)) + 1;
    payload += 2;

    char tbl[sztbl];
    strcpy(tbl, (char *)payload);
    payload += sztbl;

    lua_newtable(L);
    lua_pushstring(L, tbl);
    lua_setfield(L, -2, "name");

    blob_t id = {.length = sizeof(genid_t), .data = &f->item->genid};
    luabb_pushblob(L, &id);
    lua_setfield(L, -2, "id");

    if (flags & TYPE_TAGGED_ADD) {
        lua_newtable(L);
        lua_setfield(L, -2, "new");
        lua_pushstring(L, "add");
        lua_setfield(L, -2, "type");
    } else if (flags & TYPE_TAGGED_DEL) {
        lua_newtable(L);
        lua_setfield(L, -2, "old");
        lua_pushstring(L, "del");
        lua_setfield(L, -2, "type");
    } else if (flags & TYPE_TAGGED_UPD) {
        lua_newtable(L);
        lua_setfield(L, -2, "new");
        lua_newtable(L);
        lua_setfield(L, -2, "old");
        lua_pushstring(L, "upd");
        lua_setfield(L, -2, "type");
    }

    while (payload && payload < end) {
        payload = consume_field(L, payload);
    }

    if (payload == NULL) {
        *err = strdup("consume_field failed");
        return -1;
    }

    return 1; // trigger sp receives only one argument
}

static void clone_temp_tables(SP sp)
{
    sqlite3 *src = getdb(sp->parent);
    if (!src) return;
    sqlite3 *dest = getdb(sp);
    tmptbl_info_t *tbl;
    LIST_FOREACH(tbl, &sp->tmptbls, entries)
    {
        strbuf *sql = strbuf_new();
        const char *create = tbl->sql;
        create += sizeof("CREATE TABLE");
        strbuf_appendf(sql, "CREATE TEMP TABLE %s", create);
        clone_temp_table(dest, src, strbuf_buf(sql), tbl->rootpg);
        strbuf_free(sql);
    }
}

static int begin_sp(struct sqlclntstate *clnt, char **err)
{
    if (clnt->in_client_trans) return 0;

    const char *tmp;
    if ((tmp = begin_parent(clnt->sp->lua)) == NULL) return 0;

    *err = strdup(tmp);
    return -8;
}

static int commit_sp(Lua L, char **err)
{
    SP sp = getsp(L);
    if (in_parent_trans(sp)) {
        const char *commit_err;
        if ((commit_err = commit_parent(L)) == NULL) return 0;
        *err = strdup(commit_err);
        return -8;
    }
    int tmp;
    /* Don't make new parent transaction on this rollback. */
    sp->make_parent_trans = 0;
    db_rollback_int(L, &tmp);
    *err = strdup("unterminated transaction (no commit or rollback)");
    return -222;
}

static int flush_sp(Lua L, char **err)
{
    /* If we got here, the stored procedure is done generating rows, so flush
     * the results and we're done */
    /* TODO: if didn't send rows, need to generate a fake columns header? */
    SP sp = getsp(L);
    int rc;
    if ((rc = flush_rows(sp)) == 0) return rc;
    free(*err);
    *err = strdup("error while flushing the rows");
    return rc;
}

static int emit_result(Lua L, long long *sprc, char **err)
{
    const char *retstr = NULL;
    dbtable_t *rettab = NULL;
    long long retnum = 0;
    int retargs = lua_gettop(L);
    for (int i = 1; i <= retargs; ++i) {
        switch (luabb_dbtype(L, i)) {
        case DBTYPES_LSTRING:
        case DBTYPES_CSTRING: retstr = luabb_tostring(L, i); break;
        case DBTYPES_LNUMBER:
        case DBTYPES_INTEGER: luabb_tointeger(L, i, &retnum); break;
        case DBTYPES_DBTABLE: rettab = lua_touserdata(L, i); break;
        default:
            // TODO FIXME XXX: silently ignore other types??
            break;
        }
    }
    // lua_settop(L, 0); -- not necessary, right??

    if (rettab) dbtable_emit_int(L, rettab);

    if (retnum && (retnum < -299 || retnum > -200)) retnum = -200;
    *sprc = retnum;

    if (retstr) retstr = strdup(retstr);
    *err = (char *)retstr;

    return 0;
}

struct sp_state {
    SP sp;
};

static inline void swap_sp(SP *sp1, SP *sp2)
{
    SP tmp = *sp1;
    *sp1 = *sp2;
    *sp2 = tmp;
}

static void check_sp(const char *spname, struct sqlclntstate *clnt,
                     struct sp_state *state)
{
    SP sp = clnt->sp;
    if (sp == NULL) {
        return;
    }
    if (strcmp(sp->spname, spname) != 0) {
        swap_sp(&clnt->sp, &state->sp);
        return;
    }
    if (strcmp(clnt->spname, spname) != 0) {
        return;
    }
    if (clnt->spversion.version_num) {
        if (clnt->spversion.version_num == sp->spversion.version_num) return;
    } else if (clnt->spversion.version_str) {
        if (strcmp(clnt->spversion.version_str, sp->spversion.version_str) == 0)
            return;
    }
    swap_sp(&clnt->sp, &state->sp);
}

static void restore_sp(struct sqlclntstate *clnt, struct sp_state *state)
{
    clnt->sp = state->sp;
}

static int lua_to_sqlite(Lua L, sqlite3_context *context)
{
    if (lua_isnil(L, 1) || luabb_isnull(L, 1)) {
        sqlite3_result_null(context);
        return 0;
    }

    all_types_t t;

    switch (luabb_type(L, 1)) {
    case LUA_TBOOLEAN:
        sqlite3_result_int64(context, lua_toboolean(L, 1));
        break;
    case LUA_TNUMBER: sqlite3_result_double(context, lua_tonumber(L, 1)); break;
    case DBTYPES_REAL:
        sqlite3_result_double(context, luabb_tonumber(L, 1));
        break;
    case LUA_TSTRING:
    case DBTYPES_CSTRING:
        sqlite3_result_text(context, luabb_tostring(L, 1), -1,
                            SQLITE_TRANSIENT);
        break;
    case DBTYPES_INTEGER:
        luabb_tointeger(L, 1, &t.in);
        sqlite3_result_int64(context, t.in);
        break;
    default:
        sqlite3_result_error(context, "can't return that type, yet..", -1);
        return -1;
    }
    return 0;
}

static int sqlite_to_lua(Lua L, const char *tz, int argc, sqlite3_value **argv)
{
    blob_t b;
    datetime_t d;
    const dttz_t *dt;
    const intv_t *tv;

    for (int i = 0; i < argc; ++i) {
        switch (sqlite3_value_type(argv[i])) {
        case SQLITE_INTEGER:
            luabb_pushinteger(L, sqlite3_value_int64(argv[i]));
            break;
        case SQLITE_FLOAT:
            luabb_pushreal(L, sqlite3_value_double(argv[i]));
            break;
        case SQLITE_BLOB:
            b.data = (void *)sqlite3_value_blob(argv[i]);
            b.length = sqlite3_value_bytes(argv[i]);
            luabb_pushblob(L, &b);
            break;
        case SQLITE_NULL: lua_pushnil(L); break;
        case SQLITE_TEXT:
            luabb_pushcstring(L, (char *)sqlite3_value_text(argv[i]));
            break;
        case SQLITE_DATETIME:
        case SQLITE_DATETIMEUS:
            dt = sqlite3_value_datetime(argv[i]);
            dttz_to_datetime_t(dt, tz, &d);
            luabb_pushdatetime(L, &d);
            break;
        case SQLITE_INTERVAL_YM:
            tv = sqlite3_value_interval(argv[i], SQLITE_INTERVAL_YM);
            luabb_pushintervalym(L, tv);
            break;
        case SQLITE_INTERVAL_DS:
        case SQLITE_INTERVAL_DSUS:
            tv = sqlite3_value_interval(argv[i], SQLITE_INTERVAL_DS);
            luabb_pushintervalds(L, tv);
            break;
        case SQLITE_DECIMAL:
            tv = sqlite3_value_interval(argv[i], SQLITE_DECIMAL);
            luabb_pushdecimal(L, &tv->u.dec);
            break;
        default: return -1;
        }
    }

    return 0;
}

static int emit_sqlite_result(Lua L, sqlite3_context *context)
{
    if (lua_gettop(L) != 1) {
        sqlite3_result_error(context, "attempt to return multiple values", -1);
        return -1;
    }

    return lua_to_sqlite(L, context);
}

static int lua_final_int(char *spname, char **err, struct sqlthdstate *thd,
                         struct sqlclntstate *clnt, sqlite3_context *context)
{
    int rc, new_vm;
    if ((rc = setup_sp(spname, thd, clnt, &new_vm, err)) != 0) return rc;
    if (new_vm) {
        *err = "failed to obtain lua aggregate object";
        return -1;
    }
    Lua L = clnt->sp->lua;
    get_func_by_name(L, "final", err);

    if ((rc = run_sp(clnt, 0, err)) != 0) return rc;

    return emit_sqlite_result(L, context);
}

static int lua_step_int(char *spname, char **err, struct sqlthdstate *thd,
                        struct sqlclntstate *clnt, sqlite3_context *context,
                        int argc, sqlite3_value **argv)
{
    int rc, new_vm;
    if ((rc = setup_sp(spname, thd, clnt, &new_vm, err)) != 0) return rc;
    Lua L = clnt->sp->lua;
    SP sp = clnt->sp;

    if (new_vm) {
        if ((rc = process_src(L, sp->src, err)) != 0) return rc;
        remove_tran_funcs(L);
    }

    get_func_by_name(L, "step", err);

    if ((rc = sqlite_to_lua(L, clnt->tzname, argc, argv)) != 0) {
        return rc;
    }

    sp->num_instructions = 0;

    return run_sp(clnt, argc, err);
}

static int lua_func_int(char *spname, char **err, struct sqlthdstate *thd,
                        struct sqlclntstate *clnt, sqlite3_context *context,
                        int argc, sqlite3_value **argv)
{
    int rc, new_vm;
    if ((rc = setup_sp(spname, thd, clnt, &new_vm, err)) != 0) return rc;
    Lua L = clnt->sp->lua;
    SP sp = clnt->sp;
    if ((rc = process_src(L, sp->src, err)) != 0) return rc;
    remove_tran_funcs(L);

    get_func_by_name(L, spname, err);

    if ((rc = sqlite_to_lua(L, clnt->tzname, argc, argv)) != 0) return rc;

    sp->num_instructions = 0;

    if ((rc = run_sp(clnt, argc, err)) != 0) return rc;

    return emit_sqlite_result(L, context);
}

static int exec_thread_int(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    // Lua Stack:
    //   arg n
    //   arg n - 1
    //   ...
    //   arg 2
    //   arg 1
    //   lua-func to call
    SP sp = clnt->sp;
    sp->clnt = clnt;
    sp->thd = thd;
    Lua L = sp->lua;
    clnt->exec_lua_thread = 0;

    if (tryrdlock_schema_lk() != 0) {
        return -1;
    }
    clone_temp_tables(sp);
    unlock_schema_lk();
    int args = lua_gettop(L) - 1;
    int rc;
    char *err = NULL;

    if ((rc = begin_sp(clnt, &err)) != 0) return rc;

    if ((rc = run_sp(clnt, args, &err)) != 0) return rc;

    if ((rc = commit_sp(L, &err)) != 0) return rc;

    // dbthread_join() performs clean-up
    lua_gc(L, LUA_GCCOLLECT, 0);
    return rc;
}

static int exec_procedure_int(const char *s, char **err,
                              struct sqlthdstate *thd, struct schema *params,
                              struct sqlclntstate *clnt)
{
    char spname[MAX_SPNAME];
    long long sprc = 0;
    int rc, args, new_vm;
    *err = NULL;

    reqlog_set_event(thd->logger, "sp");

    if ((rc = get_spname(clnt, &s, spname, err)) != 0)
        return rc;

    if (strcmp(spname, "debug") == 0) return debug_sp(clnt);

    if ((rc = setup_sp(spname, thd, clnt, &new_vm, err)) != 0) return rc;

    SP sp = clnt->sp;
    Lua L = sp->lua;

    if ((rc = process_src(L, sp->src, err)) != 0) return rc;

    if ((rc = get_func_by_name(L, "main", err)) != 0) return rc;

    update_tran_funcs(L, clnt->in_client_trans);

    if (IS_SYS(spname)) init_sys_funcs(L);

    if ((rc = push_args(&s, clnt, err, params, &args)) != 0) return rc;

    if ((rc = begin_sp(clnt, err)) != 0) return rc;

    if ((rc = run_sp(clnt, args, err)) != 0) return rc;

    if ((rc = emit_result(L, &sprc, err)) != 0) return rc;

    if ((rc = commit_sp(L, err)) != 0) return rc;

    if (sprc) return sprc;

    return flush_sp(L, err);
}

static int setup_sp_for_trigger(trigger_reg_t *reg, char **err,
                                struct sqlthdstate *thd,
                                struct sqlclntstate *clnt, dbconsumer_t **q)
{
    int new_vm;
    int rc = setup_sp(reg->spname, thd, clnt, &new_vm, err);
    if (rc != 0) return rc;
    SP sp = clnt->sp;
    Lua L = sp->lua;

    if (new_vm) {
        if ((rc = process_src(L, sp->src, err)) != 0) return rc;
    }

    if ((rc = get_func_by_name(L, "main", err)) != 0) return rc;

    if (new_vm == 0) return 0;

    sp->parent->have_consumer = 1;
    remove_tran_funcs(L);
    remove_consumer(L);
    remove_emit(L);

    char *spname = reg->spname;
    Q4SP(qname, spname);
    struct dbtable *db = getqueuebyname(qname);
    if (db == NULL) {
        *err = strdup("getqueuebyname failed");
        return -1;
    }
    struct consumer *consumer = db->consumers[0];
    if (consumer == NULL) {
        *err = strdup("no consumer for db");
        return -1;
    }

    size_t sz = dbconsumer_sz(spname);
    dbconsumer_t *newq = calloc(1, sz);
    init_new_t(newq, DBTYPES_DBCONSUMER);
    if (setup_dbconsumer(newq, consumer, db, reg) != 0) {
        *err = strdup("failed to register trigger with qdb");
        return -1;
    }
    *q = newq;

    lua_settop(L, 1);
    return rc;
}

////////////////////////
/// PUBLIC INTERFACE ///
////////////////////////

int db_verify_table_callback(void *v, const char *buf)
{
    if (!buf || !v) return 0;

    Lua L = v;
    SP sp = getsp(L);

    if (peer_dropped_connection(sp->clnt)) {
        luabb_error(L, sp, "client disconnect");
        return -2;
    }
    if (buf[0] == '!' || buf[0] == '?') buf++;
    char *row[] = {(char*)buf};
    struct sqlclntstate *clnt = sp->clnt;
    clnt->write_response(clnt, RESPONSE_ROW_STR, row, 1);
    return 0;
}

void close_sp(struct sqlclntstate *clnt)
{
    close_sp_int(clnt->sp, 1);
    clnt->sp = NULL;
}

void lua_final(sqlite3_context *context)
{
    lua_func_arg_t *arg = sqlite3_user_data(context);
    struct sqlthdstate *thd = arg->thd;
    char *spname = arg->name;
    struct sqlclntstate *clnt = thd->sqlthd->clnt;
    char *err = NULL;
    struct sp_state *state = sqlite3_aggregate_context(context, sizeof(*state));
    if (state->sp == NULL) {
        // step never got called - no matching rows
        sqlite3_result_null(context);
        return;
    }
    if (state->sp != clnt->sp) {
        swap_sp(&clnt->sp, &state->sp);
    }
    int rc = lua_final_int(spname, &err, thd, clnt, context);
    if (rc != 0) {
        sqlite3_result_error(context, err, -1);
    }
    if (state->sp == clnt->sp) {
        reset_sp(clnt->sp);
    } else {
        close_sp(clnt);
        restore_sp(clnt, state);
    }
}

void lua_step(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    lua_func_arg_t *arg = sqlite3_user_data(context);
    struct sqlthdstate *thd = arg->thd;
    char *spname = arg->name;
    struct sqlclntstate *clnt = thd->sqlthd->clnt;
    char *err = NULL;
    struct sp_state *state = sqlite3_aggregate_context(context, sizeof(*state));
    if (state->sp == NULL) {
        if (clnt->sp && clnt->sp->lua) {
            if (strcmp(clnt->sp->spname, spname) == 0) {
                // first call and have cached lua vm.
                // reset it by parsing again.
                process_src(clnt->sp->lua, clnt->sp->src, &err);
            }
        }
    }
    if (state->sp != clnt->sp) {
        check_sp(spname, clnt, state);
    }
    int rc = lua_step_int(spname, &err, thd, clnt, context, argc, argv);
    if (rc != 0) {
        sqlite3_result_error(context, err, -1);
    }
    if (state->sp != clnt->sp) {
        if (state->sp) {
            swap_sp(&clnt->sp, &state->sp);
        } else {
            state->sp = clnt->sp;
        }
    }
}

void lua_func(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    lua_func_arg_t *arg = sqlite3_user_data(context);
    struct sqlthdstate *thd = arg->thd;
    char *spname = arg->name;
    struct sqlclntstate *clnt = thd->sqlthd->clnt;
    char *err = NULL;
    struct sp_state state = {0};
    check_sp(spname, clnt, &state);
    int rc = lua_func_int(spname, &err, thd, clnt, context, argc, argv);
    if (rc != 0) {
        sqlite3_result_error(context, err, -1);
    }
    if (state.sp) {
        close_sp(clnt);
        restore_sp(clnt, &state);
    } else {
        reset_sp(clnt->sp);
    }
}

void *exec_trigger(trigger_reg_t *reg)
{
    char sql[128];
    snprintf(sql, sizeof(sql), "exec procedure %s()", reg->spname);

    struct sqlclntstate clnt;
    reset_clnt(&clnt, NULL, 1);
    clnt.dbtran.mode = TRANLEVEL_SOSQL;
    clnt.sql = sql;

    struct sqlthdstate thd;
    sqlengine_thd_start(NULL, &thd, THRTYPE_TRIGGER);
    thrman_set_subtype(thd.thr_self, THRSUBTYPE_LUA_SQL);
    thd.sqlthd->clnt = &clnt;

    // We're making unprotected calls to lua below.
    // luaL_error() will cause abort()
    dbconsumer_t *q = NULL;
    while (1) {
        int rc, args;
        char *err = NULL;
        get_curtran(thedb->bdb_env, &clnt);
        if (setup_sp_for_trigger(reg, &err, &thd, &clnt, &q) != 0) {
            goto bad;
        }
        if (q == NULL) {
            goto bad;
        }
        SP sp = clnt.sp;
        Lua L = sp->lua;
        if ((args = dbconsumer_get_int(L, q)) < 0) {
            err = strdup(sp->error);
            goto bad;
        }
        if ((rc = begin_sp(&clnt, &err)) != 0) {
            err = strdup(sp->error);
            goto bad;
        }
        if ((rc = run_sp(&clnt, args, &err)) != 0) {
        rollback:
            db_rollback_int(L, &rc);
        bad:
            puts(err);
            free(err);
            if (args != -2) {
                sleep(5); // slow down buggy sp from spinning
            }
            break;
        }
        if (lua_gettop(L) != 1 || !lua_isnumber(L, 1) ||
            (rc = lua_tonumber(L, 1)) != 0) {
            logmsg(LOGMSG_ERROR, "trigger:%s rc:%s\n", sp->spname,
                   lua_tostring(L, 1));
            err = strdup("trigger returned bad rc");
            goto rollback;
        }
        if ((rc = dbconsumer_consume_int(L, q)) != 0) {
            err = strdup("trigger failed to consume");
            goto rollback;
        }
        if ((rc = commit_sp(L, &err)) != 0) {
            logmsg(LOGMSG_ERROR, "trigger:%s commit failed rc:%d -- %s\n",
                   sp->spname, rc, sp->error);
            goto bad;
        }
        put_curtran(thedb->bdb_env, &clnt);
    }
    put_curtran(thedb->bdb_env, &clnt);
    if (q) {
        luabb_trigger_unregister(q);
        free(q);
    } else {
        //setup fake dbconsumer_t to send unregister
        q = alloca(dbconsumer_sz(reg->spname));
        q->lock = NULL;
        memcpy(&q->info, reg, trigger_reg_sz(reg->spname));
        luabb_trigger_unregister(q);
    }
    close_sp(&clnt);
    cleanup_clnt(&clnt);
    sqlengine_thd_end(NULL, &thd);
    return NULL;
}

int exec_thread(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    get_curtran(thedb->bdb_env, clnt);
    int rc = exec_thread_int(thd, clnt);
    drop_temp_tables(clnt->sp);
    free_tmptbls(clnt->sp);
    put_curtran(thedb->bdb_env, clnt);
    return rc;
}

int exec_procedure(const char *s, char **err, struct sqlthdstate *thd,
                   struct schema *params, struct sqlclntstate *clnt)
{
    int rc = exec_procedure_int(s, err, thd, params, clnt);
    if (clnt->sp) {
        reset_sp(clnt->sp);
    }
    return rc;
}
