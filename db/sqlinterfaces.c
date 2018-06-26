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

/* code needed to support various comdb2 interfaces to the sql engine */

#include <poll.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <strings.h>

#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <pthread.h>
#include <sys/types.h>
#include <util.h>
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
#include <lockmacro.h>

#include <list.h>

#include <sbuf2.h>
#include <bdb_api.h>

#include "pb_alloc.h"
#include "comdb2.h"
#include "types.h"
#include "tag.h"
#include "thdpool.h"
#include "ssl_bend.h"

#include <dynschematypes.h>
#include <dynschemaload.h>
#include <cdb2api.h>

#include <sys/time.h>
#include <plbitlib.h>
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
#include <ext/expert/sqlite3expert.h>

#include <alloca.h>
#include <fsnap.h>

#include "flibc.h"

#include "sp.h"
#include "lrucache.h"

#include <ctrace.h>
#include <bb_oscompat.h>
#include <netdb.h>

#include "fdb_bend_sql.h"
#include "fdb_access.h"
#include "sqllog.h"
#include <stdbool.h>
#include <quantize.h>
#include <intern_strings.h>

#include "debug_switches.h"

#include "views.h"
#include "mem.h"
#include "comdb2_atomic.h"
#include "logmsg.h"
#include <str0.h>
#include <eventlog.h>

/* delete this after comdb2_api.h changes makes it through */
#define SQLHERR_MASTER_QUEUE_FULL -108
#define SQLHERR_MASTER_TIMEOUT -109

extern unsigned long long gbl_sql_deadlock_failures;
extern unsigned int gbl_new_row_data;
extern int gbl_use_appsock_as_sqlthread;
extern int g_osql_max_trans;
extern int gbl_fdb_track;
extern int gbl_return_long_column_names;
extern int gbl_stable_rootpages_test;

extern int gbl_expressions_indexes;

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
int gbl_dump_fsql_response = 0;
extern int gbl_time_osql; /* dump timestamps for osql steps */
extern int gbl_time_fdb;  /* dump timestamps for remote sql */
extern int gbl_print_syntax_err;
extern int gbl_max_sqlcache;
extern int gbl_track_sqlengine_states;
extern int gbl_disable_sql_dlmalloc;

extern int active_appsock_conns;
int gbl_check_access_controls;

struct thdpool *gbl_sqlengine_thdpool = NULL;

static void sql_reset_sqlthread(sqlite3 *db, struct sql_thread *thd);
int blockproc2sql_error(int rc, const char *func, int line);
static int test_no_btcursors(struct sqlthdstate *thd);
static void sql_thread_describe(void *obj, FILE *out);
int watcher_warning_function(void *arg, int timeout, int gap);
static char *get_query_cost_as_string(struct sql_thread *thd,
                                      struct sqlclntstate *clnt);

void handle_sql_intrans_unrecoverable_error(struct sqlclntstate *clnt);

void comdb2_set_sqlite_vdbe_tzname(Vdbe *p);
void comdb2_set_sqlite_vdbe_dtprec(Vdbe *p);
static int execute_sql_query_offload(struct sqlthdstate *,
                                     struct sqlclntstate *);
static int record_query_cost(struct sql_thread *, struct sqlclntstate *);

static int sql_debug_logf_int(struct sqlclntstate *clnt, const char *func,
                              int line, const char *fmt, va_list args)
{
    char *s;
    int cn_len;
    snap_uid_t snap = {0};
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

    logmsg(LOGMSG_USER, "%s %s line %d: %s", cnonce, func, line, s);

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
    extern int gbl_sql_release_locks_on_slow_reader;
    extern int gbl_sql_no_timeouts_on_release_locks;

    return (gbl_sql_release_locks_on_slow_reader &&
            gbl_sql_no_timeouts_on_release_locks);
}

int write_response(struct sqlclntstate *clnt, int R, void *D, int I)
{
    return clnt->plugin.write_response(clnt, R, D, I);
}

int read_response(struct sqlclntstate *clnt, int R, void *D, int I)
{
    return clnt->plugin.read_response(clnt, R, D, I);
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

static int skip_row(struct sqlclntstate *clnt, uint64_t rowid)
{
    return clnt->plugin.skip_row(clnt, rowid);
}

static int log_context(struct sqlclntstate *clnt, struct reqlogger *logger)
{
    return clnt->plugin.log_context(clnt, logger);
}

void handle_failed_dispatch(struct sqlclntstate *clnt, char *errstr)
{
    pthread_mutex_lock(&clnt->wait_mutex);
    write_response(clnt, RESPONSE_ERROR_REJECT, errstr, 0);
    pthread_mutex_unlock(&clnt->wait_mutex);
}

char *tranlevel_tostr(int lvl)
{
    switch (lvl) {
    case TRANLEVEL_SOSQL:
        return "TRANLEVEL_SOSQL";
    case TRANLEVEL_RECOM:
        return "TRANLEVEL_RECOM";
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

#ifdef DEBUG_SQLITE_MEMORY
#ifdef __GLIBC__
extern int backtrace(void **, int);
extern void backtrace_symbols_fd(void *const *, int, int);
#else
#define backtrace(A, B) 1
#define backtrace_symbols_fd(A, B, C)
#endif

#include <execinfo.h>

#define MAX_DEBUG_FRAMES 50

struct blk {
    int nframes;
    void *frames[MAX_DEBUG_FRAMES];
    int in_init;
    size_t sz;
    void *p;
};

static __thread hash_t *sql_blocks;

static int dump_block(void *obj, void *arg)
{
    struct blk *b = (struct blk *)obj;
    int i;
    int *had_blocks = (int *)arg;

    if (!b->in_init) {
        if (!*had_blocks) {
            logmsg(LOGMSG_USER, "outstanding blocks:\n");
            *had_blocks = 1;
        }
        logmsg(LOGMSG_USER, "%lld %x ", b->sz, b->p);
        for (int i = 0; i < b->nframes; i++)
            logmsg(LOGMSG_USER, "%x ", b->frames[i]);
        logmsg(LOGMSG_USER, "\n");
    }

    return 0;
}

static __thread int in_init = 0;

void sqlite_init_start(void) { in_init = 1; }

void sqlite_init_end(void) { in_init = 0; }

#endif // DEBUG_SQLITE_MEMORY

static __thread comdb2ma sql_mspace = NULL;
int sql_mem_init(void *arg)
{
    if (unlikely(sql_mspace)) {
        return 0;
    }

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

#ifdef DEBUG_SQLITE_MEMORY
    sql_blocks = hash_init_o(offsetof(struct blk, p), sizeof(void));
#endif

    return 0;
}

void sql_mem_shutdown(void *arg)
{
    if (sql_mspace) {
        comdb2ma_destroy(sql_mspace);
        sql_mspace = NULL;
    }
}

static void *sql_mem_malloc(int size)
{
    if (unlikely(sql_mspace == NULL))
        sql_mem_init(NULL);

    void *out = comdb2_malloc(sql_mspace, size);

#ifdef DEBUG_SQLITE_MEMORY
    struct blk *b = malloc(sizeof(struct blk));
    b->p = out;
    b->sz = size;
    b->nframes = backtrace(b->frames, MAX_DEBUG_FRAMES);
    b->in_init = in_init;
    if (!in_init) {
        fprintf(stderr, "allocated %d bytes in non-init\n", size);
    }
    if (b->nframes <= 0)
        free(b);
    else {
        hash_add(sql_blocks, b);
    }
#endif

    return out;
}

static void sql_mem_free(void *mem)
{
#ifdef DEBUG_SQLITE_MEMORY
    struct blk *b;
    b = hash_find(sql_blocks, &mem);
    if (!b) {
        fprintf(stderr, "no block associated with %p\n", mem);
        abort();
    }
    hash_del(sql_blocks, b);
    free(b);
#endif
    comdb2_free(mem);
}

static void *sql_mem_realloc(void *mem, int size)
{
    if (unlikely(sql_mspace == NULL))
        sql_mem_init(NULL);

    void *out = comdb2_realloc(sql_mspace, mem, size);

#ifdef DEBUG_SQLITE_MEMORY
    struct blk *b;
    b = hash_find(sql_blocks, &mem);
    if (!b) {
        fprintf(stderr, "no block associated with %p\n", mem);
        abort();
    }
    hash_del(sql_blocks, b);
    b->nframes = backtrace(b->frames, MAX_DEBUG_FRAMES);
    b->p = out;
    b->sz = size;
    b->in_init = in_init;
    if (b->nframes <= 0)
        free(b);
    else {
        hash_add(sql_blocks, b);
    }
#endif

    return out;
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

static pthread_mutex_t open_serial_lock = PTHREAD_MUTEX_INITIALIZER;
int sqlite3_open_serial(const char *filename, sqlite3 **ppDb,
                        struct sqlthdstate *thd)
{
    int serial = gbl_serialise_sqlite3_open;
    if (serial)
        pthread_mutex_lock(&open_serial_lock);
    int rc = sqlite3_open(filename, ppDb, thd);
    if (serial)
        pthread_mutex_unlock(&open_serial_lock);
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
   temp table spills to disk, etc. */
double query_cost(struct sql_thread *thd)
{
#if 0
    return (double) thd->nwrite * 100 + (double) thd->nfind * 10 + 
        (double) thd->nmove * 1 + (double) thd->ntmpwrite * 0.2 + 
        (double) thd->ntmpread * 0.1;
#endif
    return thd->cost;
}

void sql_dump_hist_statements(void)
{
    struct sql_hist *h;
    struct tm tm;
    char rqid[50];

    pthread_mutex_lock(&gbl_sql_lock);
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
                   "mach %d time %dms cost %f sql: %s\n",
                   tm.tm_mon + 1, tm.tm_mday, 1900 + tm.tm_year, tm.tm_hour,
                   tm.tm_min, tm.tm_sec, rqid, h->conn.pindex,
                   (char *)h->conn.pename, h->conn.pid, h->conn.node, h->time,
                   h->cost, h->sql);
        } else {
            logmsg(LOGMSG_USER, 
                   "%02d/%02d/%02d %02d:%02d:%02d %stime %dms cost %f sql: %s\n",
                   tm.tm_mon + 1, tm.tm_mday, 1900 + tm.tm_year, tm.tm_hour,
                   tm.tm_min, tm.tm_sec, rqid, h->time, h->cost, h->sql);
        }
    }
    pthread_mutex_unlock(&gbl_sql_lock);
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

static void add_steps(struct sqlclntstate *clnt, double steps)
{
    clnt->plugin.add_steps(clnt, steps);
}

/* Save copy of sql statement and performance data.  If any other code
   should run after a sql statement is completed it should end up here. */
static void sql_statement_done(struct sql_thread *thd, struct reqlogger *logger,
                               struct sqlclntstate *clnt, int stmt_rc)
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

    unsigned long long rqid = clnt->osql.rqid;
    if (rqid != 0 && rqid != OSQL_RQID_USE_UUID)
        reqlog_set_rqid(logger, &rqid, sizeof(rqid));
    else if (!comdb2uuid_is_zero(clnt->osql.uuid)) {
        /* have an "id_set" instead? */
        reqlog_set_rqid(logger, clnt->osql.uuid, sizeof(uuid_t));
    }

    LISTC_T(struct sql_hist) lst;
    listc_init(&lst, offsetof(struct sql_hist, lnk));

    struct sql_hist *h = calloc(1, sizeof(struct sql_hist));
    if (clnt->sql)
        h->sql = strdup(clnt->sql);
    else
        h->sql = strdup("unknown");
    h->cost = query_cost(thd);
    h->time = comdb2_time_epochms() - thd->startms;
    h->when = thd->stime;
    h->txnid = rqid;

    /* request logging framework takes care of logging long sql requests */
    reqlog_set_cost(logger, h->cost);
    if (rqid) {
        reqlog_logf(logger, REQL_INFO, "rqid=%llx", rqid);
    }

    if (clnt->query_stats == NULL) {
        record_query_cost(thd, clnt);
        reqlog_set_path(logger, clnt->query_stats);
    }
    reqlog_set_vreplays(logger, clnt->verify_retries);

    if (clnt->saved_rc)
        reqlog_set_error(logger, clnt->saved_errstr, clnt->saved_rc);

    reqlog_set_rows(logger, clnt->nrows);
    reqlog_end_request(logger, stmt_rc, __func__, __LINE__);

    if (clnt->rawnodestats) {
        clnt->rawnodestats->sql_steps += thd->nmove + thd->nfind + thd->nwrite;
    }

    thd->nmove = thd->nfind = thd->nwrite = thd->ntmpread = thd->ntmpwrite = 0;

    if (clnt->conninfo.pename[0]) {
        h->conn = clnt->conninfo;
    }

    pthread_mutex_lock(&gbl_sql_lock);
    {
        quantize(q_sql_min, h->time);
        quantize(q_sql_hour, h->time);
        quantize(q_sql_all, h->time);
        quantize(q_sql_steps_min, h->cost);
        quantize(q_sql_steps_hour, h->cost);
        quantize(q_sql_steps_all, h->cost);

        add_steps(clnt, h->cost);

        listc_abl(&thedb->sqlhist, h);
        while (listc_size(&thedb->sqlhist) > gbl_sqlhistsz) {
            h = listc_rtl(&thedb->sqlhist);
            listc_abl(&lst, h);
        }
    }
    pthread_mutex_unlock(&gbl_sql_lock);
    for (h = listc_rtl(&lst); h; h = listc_rtl(&lst)) {
        free(h->sql);
        free(h);
    }

    struct query_path_component *qc = listc_rtl(&thd->query_stats);
    while (qc) {
        free(qc);
        qc = listc_rtl(&thd->query_stats);
    }

    clear_cost(thd);
}

void sql_set_sqlengine_state(struct sqlclntstate *clnt, char *file, int line,
                             int newstate)
{
    if (gbl_track_sqlengine_states)
        logmsg(LOGMSG_USER, "%lu: %p %s:%d %d->%d\n", pthread_self(), clnt,
               file, line, clnt->ctrl_sqlengine, newstate);

    if (newstate == SQLENG_WRONG_STATE) {
        logmsg(LOGMSG_ERROR, "sqlengine entering wrong state from %s line %d.\n",
                file, line);
    }

    clnt->ctrl_sqlengine = newstate;
}

/* skip spaces and tabs, requires at least one space */
static inline char *skipws(char *str)
{
    if (str) {
        while (*str && isspace(*str))
            str++;
    }
    return str;
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
                            if (gbl_new_snapisol_asof &&
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
                /*
                   else if (!strncasecmp(str, "genid"))
                   {

                   }
                 */
            } else {
                logmsg(LOGMSG_ERROR, "Missing snapshot info after \"as of\"\n");
                return -1;
            }
        }
    }

    return 0;
}

static void snapshot_as_of(struct sqlclntstate *clnt)
{
    int epoch = 0;
    if (strlen(clnt->sql) > 6)
        epoch = retrieve_snapshot_info(&clnt->sql[6], clnt->tzname);

    if (epoch < 0) {
        /* overload this for now */
        sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_WRONG_STATE);
    } else {
        clnt->snapshot = epoch;
    }
}

/**
 * Cluster here all pre-sqlite parsing, detecting requests that
 * are not handled by sqlite (transaction commands, pragma, stored proc,
 * blocked sql, and so on)
 */
static void sql_update_usertran_state(struct sqlclntstate *clnt)
{
    const char *sql = clnt->sql;

    if (!sql)
        return;

    /* begin, commit, rollback should arrive over the socket only
       for socksql, recom, snapisol and serial */
    if (!strncasecmp(clnt->sql, "begin", 5)) {
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
            clnt->in_client_trans = 1;

            assert(clnt->ddl_tables == NULL && clnt->dml_tables == NULL);
            clnt->ddl_tables = hash_init_strcase(0);
            clnt->dml_tables = hash_init_strcase(0);

            upd_snapshot(clnt);
            snapshot_as_of(clnt);
        }
    } else if (!strncasecmp(clnt->sql, "commit", 6)) {
        clnt->snapshot = 0;

        if (clnt->ctrl_sqlengine != SQLENG_INTRANS_STATE &&
            clnt->ctrl_sqlengine != SQLENG_STRT_STATE) {
            /* this is for empty transactions */

            /* not in a transaction */
            sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                    SQLENG_WRONG_STATE);
        } else {
            sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                    SQLENG_FNSH_STATE);
            clnt->in_client_trans = 0;
            clnt->trans_has_sp = 0;
        }
    } else if (!strncasecmp(clnt->sql, "rollback", 8)) {
        clnt->snapshot = 0;

        if (clnt->ctrl_sqlengine != SQLENG_INTRANS_STATE &&
            clnt->ctrl_sqlengine != SQLENG_STRT_STATE)
        /* this is for empty transactions */
        {
            /* not in a transaction */
            sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                    SQLENG_WRONG_STATE);
        } else {
            sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                    SQLENG_FNSH_RBK_STATE);
            clnt->in_client_trans = 0;
            clnt->trans_has_sp = 0;
        }
    }
}

static void log_queue_time(struct reqlogger *logger, struct sqlclntstate *clnt)
{
    if (!gbl_track_queue_time)
        return;
    if (clnt->deque_timeus > clnt->enque_timeus)
        reqlog_logf(logger, REQL_INFO, "queuetime took %dms",
                    U2M(clnt->deque_timeus - clnt->enque_timeus));
    reqlog_set_queue_time(logger, clnt->deque_timeus - clnt->enque_timeus);
}

static void log_cost(struct reqlogger *logger, int64_t cost, int64_t rows) {
    reqlog_set_cost(logger, cost);
    reqlog_set_rows(logger, rows);
}

/* begin; send return code */
int handle_sql_begin(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                     int sendresponse)
{
    pthread_mutex_lock(&clnt->wait_mutex);
    clnt->ready_for_heartbeats = 0;

    reqlog_new_sql_request(thd->logger, clnt->sql);
    log_queue_time(thd->logger, clnt);

    /* this is a good "begin", just say "ok" */
    sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_STRT_STATE);

    /* clients don't expect column data if it's a converted request */
    reqlog_logf(thd->logger, REQL_QUERY, "\"%s\" new transaction\n",
                (clnt->sql) ? clnt->sql : "(???.)");

    if (clnt->osql.replay)
        goto done;

    if (sendresponse) {
        write_response(clnt, RESPONSE_ROW_LAST_DUMMY, NULL, 0);
    }

done:
    pthread_mutex_unlock(&clnt->wait_mutex);

    if (srs_tran_add_query(clnt))
        logmsg(LOGMSG_ERROR, "Fail to create a transaction replay session\n");

    reqlog_end_request(thd->logger, -1, __func__, __LINE__);

    return SQLITE_OK;
}

static int handle_sql_wrongstate(struct sqlthdstate *thd,
                                 struct sqlclntstate *clnt)
{


    sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_NORMAL_PROCESS);

    reqlog_new_sql_request(thd->logger, clnt->sql);
    log_queue_time(thd->logger, clnt);

    reqlog_logf(thd->logger, REQL_QUERY,
                "\"%s\" wrong transaction command receive\n",
                (clnt->sql) ? clnt->sql : "(???.)");

    write_response(clnt, RESPONSE_ERROR_BAD_STATE,
                   "sqlinterfaces: wrong sql handle state\n", 0);

    if (srs_tran_destroy(clnt))
        logmsg(LOGMSG_ERROR, "Fail to destroy transaction replay session\n");

    clnt->intrans = 0;
    reset_clnt_flags(clnt);

    reqlog_end_request(thd->logger, -1, __func__, __LINE__);

    return SQLITE_INTERNAL;
}

void reset_query_effects(struct sqlclntstate *clnt)
{
    bzero(&clnt->effects, sizeof(clnt->effects));
    bzero(&clnt->log_effects, sizeof(clnt->effects));
}

static char *sqlenginestate_tostr(int state)
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
    case SQLENG_WRONG_STATE:
        return "SQLENG_WRONG_STATE";
        break;
    default:
        return "???";
    }
}

inline int replicant_can_retry(struct sqlclntstate *clnt)
{
    return clnt->dbtran.mode != TRANLEVEL_SNAPISOL &&
           clnt->dbtran.mode != TRANLEVEL_SERIAL &&
           clnt->verifyretry_off == 0;
}

static int free_it(void *obj, void *arg)
{
    free(obj);
    return 0;
}
static inline void destroy_hash(hash_t *h)
{
    if (!h)
        return;
    hash_for(h, free_it, NULL);
    hash_clear(h);
    hash_free(h);
}

int handle_sql_commitrollback(struct sqlthdstate *thd,
                              struct sqlclntstate *clnt, int sendresponse)
{
    int bdberr = 0;
    int rc = 0;
    int irc = 0;
    int outrc = 0;

    reqlog_new_sql_request(thd->logger, clnt->sql);
    log_queue_time(thd->logger, clnt);

    int64_t rows = clnt->log_effects.num_updated +
               clnt->log_effects.num_deleted +
               clnt->log_effects.num_inserted;

    reqlog_set_cost(thd->logger, 0);
    reqlog_set_rows(thd->logger, rows);


    if (!clnt->intrans) {
        reqlog_logf(thd->logger, REQL_QUERY, "\"%s\" ignore (no transaction)\n",
                    (clnt->sql) ? clnt->sql : "(???.)");

        rc = SQLITE_OK;
    } else {
        bzero(clnt->dirty, sizeof(clnt->dirty));

        sql_debug_logf(clnt, __func__, __LINE__, "starting\n");

        switch (clnt->dbtran.mode) {
        case TRANLEVEL_RECOM: {
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
                                __func__,
                                (rc == SQLITE_OK) ? "commit" : "abort", irc,
                                bdberr);
                    }
                }
            } else {
                reset_query_effects(clnt);
                rc = recom_abort(clnt);
                if (rc)
                    logmsg(LOGMSG_ERROR, "%s: recom abort failed %d??\n", __func__,
                            rc);
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
                                __func__,
                                (rc == SQLITE_OK) ? "commit" : "abort", irc,
                                bdberr);
                    }
                } else {
                    sql_debug_logf(clnt, __func__, __LINE__,
                                   "no-shadow-tran returning %d\n", rc);
                    if (rc == SQLITE_ABORT) {
                        rc = blockproc2sql_error(clnt->osql.xerr.errval,
                                                 __func__, __LINE__);
                        logmsg(
                            LOGMSG_ERROR,
                            "td=%lu no-shadow-tran %s line %d, returning %d\n",
                            pthread_self(), __func__, __LINE__, rc);
                    } else if (rc == SQLITE_CLIENT_CHANGENODE) {
                        rc = has_high_availability(clnt) ? CDB2ERR_CHANGENODE
                                                     : SQLHERR_MASTER_TIMEOUT;
                        logmsg(
                            LOGMSG_ERROR,
                            "td=%lu no-shadow-tran %s line %d, returning %d\n",
                            pthread_self(), __func__, __LINE__, rc);
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
                    logmsg(LOGMSG_ERROR, "%s: serial abort failed %d??\n", __func__,
                            rc);

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
                if (gbl_selectv_rangechk) {
                    rc = selectv_range_commit(clnt);
                }
                if (rc || clnt->early_retry) {
                    int irc = 0;
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
                    }
                    if (clnt->early_retry) {
                        clnt->early_retry = 0;
                        rc = SQLITE_ABORT;
                    }
                } else {
                    rc = osql_sock_commit(clnt, OSQL_SOCK_REQ);
                }
                if (rc == SQLITE_ABORT) {
                    /* convert this to user code */
                    rc = blockproc2sql_error(clnt->osql.xerr.errval, __func__,
                                             __LINE__);
                    if (clnt->osql.xerr.errval == ERR_UNCOMMITABLE_TXN) {
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
                    clnt->had_errors = 1;
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

    destroy_hash(clnt->ddl_tables);
    destroy_hash(clnt->dml_tables);
    clnt->ddl_tables = NULL;
    clnt->dml_tables = NULL;

    /* reset the state after send_done; we use ctrl_sqlengine to know
       if this is a user rollback or an sqlite engine error */
    sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_NORMAL_PROCESS);

/* we are out of transaction, mark this here */
#ifdef DEBUG
    if (gbl_debug_sql_opcodes) {
        logmsg(LOGMSG_USER, "%p (U) commits transaction %d %d intran=%d\n", clnt,
                pthread_self(), clnt->dbtran.mode, clnt->intrans);
    }
#endif

    clnt->intrans = 0;
    clnt->dbtran.shadow_tran = NULL;

    if (rc == SQLITE_OK) {
        /* send return code */

        write_response(clnt, RESPONSE_EFFECTS, 0, 0);

        pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;

        if (sendresponse) {
            /* This is a commit, so we'll have something to send here even on a
             * retry.  Don't trigger code in fsql_write_response that's there
             * to catch bugs when we send back responses on a retry. */
            write_response(clnt, RESPONSE_ROW_LAST_DUMMY, NULL, 0);
        }

        outrc = SQLITE_OK; /* the happy case */

        pthread_mutex_unlock(&clnt->wait_mutex);

        if (clnt->osql.replay != OSQL_RETRY_NONE) {
            /* successful retry */
            osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);

            reqlog_logf(thd->logger, REQL_QUERY,
                        "\"%s\" SOCKSL retried done sendresp=%d\n",
                        (clnt->sql) ? clnt->sql : "(???.)", sendresponse);
        }
    } else {
        /* error */

        /* if this is a verify error and we are not running in
           snapshot/serializable mode, repeat this request ad nauseam
           (Alex and Sam made me do it) */
        if (rc == CDB2ERR_VERIFY_ERROR && replicant_can_retry(clnt) &&
            !clnt->has_recording && clnt->osql.replay != OSQL_RETRY_LAST) {
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
        if (rc == CDB2ERR_VERIFY_ERROR &&
            (clnt->osql.replay == OSQL_RETRY_LAST || clnt->verifyretry_off)) {
            reqlog_logf(thd->logger, REQL_QUERY,
                        "\"%s\" SOCKSL retried done (hit last) sendresp=%d\n",
                        (clnt->sql) ? clnt->sql : "(???.)", sendresponse);
            osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);
        }
        /* if this is still an error, but not verify, pass it back to client */
        else if (rc != CDB2ERR_VERIFY_ERROR) {
            reqlog_logf(thd->logger, REQL_QUERY, "\"%s\" SOCKSL retried done "
                                                 "(non verify error rc=%d) "
                                                 "sendresp=%d\n",
                        (clnt->sql) ? clnt->sql : "(???.)", rc, sendresponse);
            osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);
        }

        if (rc == SQLITE_TOOBIG) {
            strncpy(clnt->osql.xerr.errstr,
                    "transaction too big, try increasing the limit using 'SET "
                    "maxtransize N'",
                    sizeof(clnt->osql.xerr.errstr));
            rc = CDB2__ERROR_CODE__TRAN_TOO_BIG;
        }

        pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;
        pthread_mutex_unlock(&clnt->wait_mutex);


        outrc = rc;

        if (sendresponse) {
            write_response(clnt, RESPONSE_ERROR, clnt->osql.xerr.errstr, rc);
        }
    }

    /* if this is a retry, let the upper layer free the structure */
    if (clnt->osql.replay == OSQL_RETRY_NONE) {
        if (srs_tran_destroy(clnt))
            logmsg(LOGMSG_ERROR, "Fail to destroy transaction replay session\n");
    }

done:

    reset_clnt_flags(clnt);

    reqlog_end_request(thd->logger, -1, __func__, __LINE__);
    return outrc;
}

static int strcmpfunc_stmt(char *a, char *b, int len) { return strcmp(a, b); }

static u_int strhashfunc_stmt(u_char *keyp, int len)
{
    unsigned hash;
    int jj;
    u_char *key = keyp;
    for (hash = 0; *key; key++)
        hash = ((hash % 8388013) << 8) + ((*key));
    return hash;
}

static int finalize_stmt_hash(void *stmt_entry, void *args)
{
    stmt_hash_entry_type *entry = (stmt_hash_entry_type *)stmt_entry;
    sqlite3_finalize(entry->stmt);
    if (entry->query && gbl_debug_temptables) {
        free(entry->query);
        entry->query = NULL;
    }
    sqlite3_free(entry);
    return 0;
}

void delete_stmt_caching_table(hash_t *stmt_caching_table)
{
    assert(stmt_caching_table);
    /* parse through hash table and finalize all the statements */
    hash_for(stmt_caching_table, finalize_stmt_hash, NULL);
    hash_clear(stmt_caching_table);
    hash_free(stmt_caching_table);
}

static inline void init_stmt_caching_table(struct sqlthdstate *thd)
{
    thd->stmt_caching_table = hash_init_user(
        (hashfunc_t *)strhashfunc_stmt, (cmpfunc_t *)strcmpfunc_stmt,
        offsetof(stmt_hash_entry_type, sql), MAX_HASH_SQL_LENGTH);

    assert(thd->stmt_caching_table && "hash_init_user: can not init");
    listc_init(&thd->param_stmt_list,
               offsetof(struct stmt_hash_entry, stmtlist_linkv));
    listc_init(&thd->noparam_stmt_list,
               offsetof(struct stmt_hash_entry, stmtlist_linkv));
}

/*
 * Reque a stmt that was previously removed from the queues
 * by calling remove_stmt_entry().
 * Similar to add_stmt_table() but does not need to allocate.
 */
int requeue_stmt_entry(struct sqlthdstate *thd, stmt_hash_entry_type *entry)
{
    if (hash_find(thd->stmt_caching_table, entry->sql) != NULL)
        return -1; // already there, dont add again

    if (hash_add(thd->stmt_caching_table, entry) != 0) {
        return -1;
    }
    sqlite3_reset(entry->stmt); // reset vdbe when adding to hash tbl
    void *list;
    if (sqlite3_bind_parameter_count(entry->stmt)) {
        list = &thd->param_stmt_list;
    } else {
        list = &thd->noparam_stmt_list;
    }
    listc_atl(list, entry);
    return 0;
}

static void cleanup_stmt_entry(stmt_hash_entry_type *entry)
{
    if (entry->query && gbl_debug_temptables) {
        free(entry->query);
        entry->query = NULL;
    }
    sqlite3_finalize(entry->stmt);
    sqlite3_free(entry);
}

static void delete_last_stmt_entry(struct sqlthdstate *thd, void *list)
{
    stmt_hash_entry_type *entry = listc_rbl(list);
    int rc = hash_del(thd->stmt_caching_table, entry);
    assert(rc == 0);
    cleanup_stmt_entry(entry);
}

/*
 * Remove from queue and stmt_caching_table this entry so that
 * subsequent finds of the same sql will not find it but
 * rather create a new stmt (to avoid having stmt vdbe
 * used by two sql at the same time).
 */
static void remove_stmt_entry(struct sqlthdstate *thd,
                              stmt_hash_entry_type *entry)
{
    assert(entry);

    void *list = NULL;
    if (sqlite3_bind_parameter_count(entry->stmt)) {
        list = &thd->param_stmt_list;
    } else {
        list = &thd->noparam_stmt_list;
    }
    listc_rfl(list, entry);
    int rc = hash_del(thd->stmt_caching_table, entry->sql);
    assert(rc == 0);
}

/* On error will return non zero and
 * caller will need to finalize_stmt() in that case
 */
static int add_stmt_table(struct sqlthdstate *thd, const char *sql,
                          const char *actual_sql, sqlite3_stmt *stmt)
{
    if (strlen(sql) >= MAX_HASH_SQL_LENGTH) {
        return -1;
    }

    assert(thd->stmt_caching_table);
    /* stored procedure can call same stmt a lua thread more than once
     * so we should not add stmt that exists already */
    if (hash_find(thd->stmt_caching_table, sql) != NULL) {
        return -1;
    }

    void *list = NULL;
    if (sqlite3_bind_parameter_count(stmt)) {
        list = &thd->param_stmt_list;
    } else {
        list = &thd->noparam_stmt_list;
    }

    /* remove older entries */
    if (gbl_max_sqlcache <= listc_size(list)) {
        delete_last_stmt_entry(thd, list);
    }

    stmt_hash_entry_type *entry = sqlite3_malloc(sizeof(stmt_hash_entry_type));
    strcpy(entry->sql, sql); /* sql is at most MAX_HASH_SQL_LENGTH - 1 */
    entry->stmt = stmt;
    if (actual_sql && gbl_debug_temptables)
        entry->query = strdup(actual_sql);
    else
        entry->query = NULL;
    return requeue_stmt_entry(thd, entry);
}

static inline int find_stmt_table(struct sqlthdstate *thd, const char *sql,
                                  stmt_hash_entry_type **entry)
{
    assert(thd);
    hash_t *stmt_caching_table = thd->stmt_caching_table;
    if (stmt_caching_table == NULL)
        return -1;

    if (strlen(sql) >= MAX_HASH_SQL_LENGTH)
        return -1;

    *entry = hash_find(stmt_caching_table, sql);

    if (*entry == NULL)
        return -1;

    remove_stmt_entry(thd, *entry); // will add again when done

    return 0;
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

/** Table which stores sql strings and sql hints
  * We will hit this table if the thread running the queries from
  * certain sql control changes.
  **/

lrucache *sql_hints = NULL;

/* sql_hint/sql_str/tag all point to mem (tag can also be NULL) */
typedef struct {
    char *sql_hint;
    char *sql_str;
    lrucache_link lnk;
    char mem[0];
} sql_hint_hash_entry_type;

void delete_sql_hint_table() { lrucache_destroy(sql_hints); }

static unsigned int sqlhint_hash(const void *p, int len)
{
    unsigned char *s;
    unsigned h = 0;

    memcpy(&s, p, sizeof(char *));

    while (*s) {
        h = ((h % 8388013) << 8) + (*s);
        s++;
    }
    return h;
}

int sqlhint_cmp(const void *key1, const void *key2, int len)
{
    char *s1, *s2;
    memcpy(&s1, key1, sizeof(char *));
    memcpy(&s2, key2, sizeof(char *));
    return strcmp((char *)s1, (char *)s2);
}

void init_sql_hint_table()
{
    sql_hints = lrucache_init(sqlhint_hash, sqlhint_cmp, free,
                              offsetof(sql_hint_hash_entry_type, lnk),
                              offsetof(sql_hint_hash_entry_type, sql_hint),
                              sizeof(char *), gbl_max_sql_hint_cache);
}

void reinit_sql_hint_table()
{
    pthread_mutex_lock(&gbl_sql_lock);
    {
        delete_sql_hint_table();
        init_sql_hint_table();
    }
    pthread_mutex_unlock(&gbl_sql_lock);
}

static void add_sql_hint_table(char *sql_hint, char *sql_str)
{
    int sql_hint_len = strlen(sql_hint) + 1;
    int sql_len = strlen(sql_str) + 1;
    int len = sql_hint_len + sql_len;
    sql_hint_hash_entry_type *entry = malloc(sizeof(*entry) + len);

    entry->sql_hint = entry->mem;
    memcpy(entry->sql_hint, sql_hint, sql_hint_len);

    entry->sql_str = entry->sql_hint + sql_hint_len;
    memcpy(entry->sql_str, sql_str, sql_len);

    pthread_mutex_lock(&gbl_sql_lock);
    {
        if (lrucache_hasentry(sql_hints, &sql_hint) == 0) {
            lrucache_add(sql_hints, entry);
        } else {
            free(entry);
            logmsg(LOGMSG_ERROR, "Client BUG: Two threads using same SQL tag.\n");
        }
    }
    pthread_mutex_unlock(&gbl_sql_lock);
}

static int find_sql_hint_table(char *sql_hint, char **sql_str)
{
    sql_hint_hash_entry_type *entry;
    pthread_mutex_lock(&gbl_sql_lock);
    {
        entry = lrucache_find(sql_hints, &sql_hint);
    }
    pthread_mutex_unlock(&gbl_sql_lock);
    if (entry) {
        *sql_str = entry->sql_str;
        return 0;
    }
    return -1;
}

static int has_sql_hint_table(char *sql_hint)
{
    int ret;
    pthread_mutex_lock(&gbl_sql_lock);
    {
        ret = lrucache_hasentry(sql_hints, &sql_hint);
    }
    pthread_mutex_unlock(&gbl_sql_lock);
    return ret;
}

#define SQLCACHEHINT "/*+ RUNCOMDB2SQL"

int has_sqlcache_hint(const char *sql, const char **pstart, const char **pend)
{
    char *start, *end;
    start = strstr(sql, SQLCACHEHINT);
    if (pstart)
        *pstart = start;
    if (start) {
        end = strstr(start, " */");
        if (pend)
            *pend = end;
        if (end) {
            end += 3;
            if (pend)
                *pend = end;
            return 1;
        }
    }
    return 0;
}

int extract_sqlcache_hint(const char *sql, char *hint, int hintlen)
{
    const char *start = NULL;
    const char *end = NULL;
    int length;
    int ret;

    ret = has_sqlcache_hint(sql, &start, &end);

    if (ret) {
        length = end - start;
        if (length >= hintlen) {
            logmsg(LOGMSG_WARN, "Query has very long hint! \"%s\"\n", sql);
            length = hintlen - 1;
        }
        strncpy(hint, start, length);
        hint[length] = '\0';
    }
    return ret;
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
    int rc;
    rc = pthread_key_create(&current_sql_query_key, free_sql);
    if (rc) {
        logmsg(LOGMSG_FATAL, "pthread_key_create current_sql_query_key rc %d\n", rc);
        exit(1);
    }
}

extern int gbl_debug_temptables;

static const char *type_to_typestr(int type)
{
    switch (type) {
    case SQLITE_NULL:
        return "null";
    case SQLITE_INTEGER:
        return "integer";
    case SQLITE_FLOAT:
        return "real";
    case SQLITE_TEXT:
        return "text";
    case SQLITE_BLOB:
        return "blob";
    case SQLITE_DATETIME:
        return "datetime";
    case SQLITE_DATETIMEUS:
        return "datetimeus";
    case SQLITE_INTERVAL_YM:
        return "year";
    case SQLITE_INTERVAL_DSUS:
        return "dayus";
    case SQLITE_INTERVAL_DS:
        return "day";
    default:
        return "???";
    }
}

int typestr_to_type(const char *ctype)
{
    if (ctype == NULL)
        return SQLITE_TEXT;
    if ((strcmp("smallint", ctype) == 0) || (strcmp("int", ctype) == 0) ||
        (strcmp("largeint", ctype) == 0) || (strcmp("integer", ctype) == 0))
        return SQLITE_INTEGER;
    else if ((strcmp("smallfloat", ctype) == 0) ||
             (strcmp("float", ctype) == 0))
        return SQLITE_FLOAT;
    else if (strncmp("char", ctype, 4) == 0)
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
        } discrepancies[MAX_DISC_SHOW] = {0};
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
    if ((rc = sqlite3AnalysisLoad(thd->sqldb, 0)) == SQLITE_OK) {
        thd->analyze_gen = analyze_gen;
    } else {
        logmsg(LOGMSG_ERROR, "%s sqlite3AnalysisLoad rc:%d\n", __func__, rc);
    }
    if (got_curtran && put_curtran(thedb->bdb_env, clnt)) {
        logmsg(LOGMSG_ERROR, "%s failed to put_curtran\n", __func__);
    }
    return rc;
}

static inline void delete_prepared_stmts(struct sqlthdstate *thd)
{
    if (thd->stmt_caching_table) {
        delete_stmt_caching_table(thd->stmt_caching_table);
        init_stmt_caching_table(thd);
    }
}

// Call with schema_lk held and no_transaction == 1
static int check_thd_gen(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    /* cache analyze gen first because gbl_analyze_gen is NOT protected by
     * schema_lk */
    int cached_analyze_gen = gbl_analyze_gen;
    if (gbl_fdb_track)
        logmsg(LOGMSG_USER, "XXX: thd dbopen=%d vs %d thd analyze %d vs %d\n",
               thd->dbopen_gen, gbl_dbopen_gen, thd->analyze_gen,
               cached_analyze_gen);

    if (thd->dbopen_gen != gbl_dbopen_gen) {
        return SQLITE_SCHEMA;
    }
    if (thd->analyze_gen != cached_analyze_gen) {
        int ret;
        delete_prepared_stmts(thd);
        ret = reload_analyze(thd, clnt, cached_analyze_gen);
        return ret;
    }

    if (thd->views_gen != gbl_views_gen) {
        return SQLITE_SCHEMA_REMOTE;
    }

    return SQLITE_OK;
}

int release_locks(const char *trace)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd ? thd->clnt : NULL;
    int rc = 0;

    if (clnt && clnt->dbtran.cursor_tran) {
        extern int gbl_sql_release_locks_trace;
        if (gbl_sql_release_locks_trace)
            logmsg(LOGMSG_USER, "Releasing locks for lockid %d, %s\n",
                   bdb_get_lid_from_cursortran(clnt->dbtran.cursor_tran),
                   trace);
        rc = recover_deadlock_silent(thedb->bdb_env, thd, NULL, -1);
    }
    return rc;
}

int release_locks_on_emit_row(struct sqlthdstate *thd,
                              struct sqlclntstate *clnt)
{
    extern int gbl_sql_release_locks_on_emit_row;
    extern int gbl_locks_check_waiters;
    int rc = 0;
    if (gbl_locks_check_waiters && gbl_sql_release_locks_on_emit_row) {
        extern int gbl_sql_random_release_interval;
        if (bdb_curtran_has_waiters(thedb->bdb_env, clnt->dbtran.cursor_tran)) {
            rc = release_locks("lockwait at emit-row");
        } else if (gbl_sql_random_release_interval &&
                   !(rand() % gbl_sql_random_release_interval)) {
            rc = release_locks("random release emit-row");
        }
    }
    return rc;
}

static int check_sql(struct sqlclntstate *clnt, int *sp)
{
    char buf[256];
    char *sql = clnt->sql;
    size_t len = sizeof("exec") - 1;
    if (strncasecmp(sql, "exec", len) == 0) {
        sql += len;
        if (isspace(*sql)) {
            *sp = 1;
            return 0;
        }
    }
    len = sizeof("pragma") - 1;
    if (strncasecmp(sql, "pragma", len) == 0) {
        sql += len;
        if (isspace(*sql)) {
            goto error;
        }
        return 0;
    }
    len = sizeof("create") - 1;
    if (strncasecmp(sql, "create", len) == 0) {
        char *trigger = sql;
        trigger += len;
        if (!isspace(*trigger)) {
            return 0;
        }
        trigger = skipws(trigger);
        len = sizeof("trigger") - 1;
        if (strncasecmp(trigger, "trigger", len) != 0) {
            return 0;
        }
        trigger += len;
        if (isspace(*trigger)) {
            goto error;
        }
    }
    return 0;

error: /* pretend that a real prepare error occured */
    strcpy(buf, "near \"");
    strncat(buf + len, sql, len);
    strcat(buf, "\": syntax error");
    write_response(clnt, RESPONSE_ERROR_PREPARE, buf, 0);
    return SQLITE_ERROR;
}

/* if userpassword does not match this function
 * will write error response and return a non 0 rc
 */
static inline int check_user_password(struct sqlclntstate *clnt)
{
    int password_rc = 0;
    int valid_user;

    if (!gbl_uses_password)
        return 0;

    if (!clnt->have_user) {
        clnt->have_user = 1;
        strcpy(clnt->user, DEFAULT_USER);
    }

    if (!clnt->have_password) {
        clnt->have_password = 1;
        strcpy(clnt->password, DEFAULT_PASSWORD);
    }

    password_rc =
        bdb_user_password_check(clnt->user, clnt->password, &valid_user);

    if (password_rc != 0) {
        write_response(clnt, RESPONSE_ERROR_ACCESS, "access denied", 0);
        return 1;
    }
    return 0;
}

void thr_set_current_sql(const char *sql)
{
    char *prevsql;
    pthread_once(&current_sql_query_once, init_current_current_sql_key);
    if (gbl_debug_temptables) {
        prevsql = pthread_getspecific(current_sql_query_key);
        if (prevsql) {
            free(prevsql);
            pthread_setspecific(current_sql_query_key, NULL);
        }
        pthread_setspecific(current_sql_query_key, strdup(sql));
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
    reqlog_new_sql_request(thd->logger, NULL);
    log_context(clnt, thd->logger);
    log_queue_time(thd->logger, clnt);
}

static void query_stats_setup(struct sqlthdstate *thd,
                              struct sqlclntstate *clnt)
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
    thd->sqlthd->nmove = thd->sqlthd->nfind = thd->sqlthd->nwrite = 0;

    /* reqlog */
    setup_reqlog(thd, clnt);

    /* fingerprint info */
    if (gbl_fingerprint_queries)
        sqlite3_fingerprint_enable(thd->sqldb);
    else
        sqlite3_fingerprint_disable(thd->sqldb);

    /* using case sensitive like? enable */
    if (clnt->using_case_insensitive_like)
        toggle_case_sensitive_like(thd->sqldb, 1);

    if (gbl_dump_sql_dispatched)
        logmsg(LOGMSG_USER, "SQL mode=%d [%s]\n", clnt->dbtran.mode, clnt->sql);

    reqlog_set_clnt(thd->logger, clnt);
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

static void get_cached_stmt(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                            struct sql_state *rec)
{
    rec->status = CACHE_DISABLED;
    if (gbl_enable_sql_stmt_caching == STMT_CACHE_NONE)
        return;
    if (gbl_enable_sql_stmt_caching == STMT_CACHE_PARAM &&
        param_count(clnt) == 0)
        return;
    if (extract_sqlcache_hint(rec->sql, rec->cache_hint, HINT_LEN)) {
        rec->status = CACHE_HAS_HINT;
        if (find_stmt_table(thd, rec->cache_hint, &rec->stmt_entry) == 0) {
            rec->status |= CACHE_FOUND_STMT;
            rec->stmt = rec->stmt_entry->stmt;
        } else {
            /* We are not able to find the statement in cache, and this is a
             * partial statement. Try to find sql string stored in hash table */
            if (find_sql_hint_table(rec->cache_hint, (char **)&rec->sql) == 0) {
                rec->status |= CACHE_FOUND_STR;
            }
        }
    } else {
        if (find_stmt_table(thd, rec->sql, &rec->stmt_entry) == 0) {
            rec->status = CACHE_FOUND_STMT;
            rec->stmt = rec->stmt_entry->stmt;
        }
    }
    if (rec->stmt) {
        rec->sql = sqlite3_sql(rec->stmt); // save expanded query
        int rc = sqlite3LockStmtTables(rec->stmt);
        if (rc) {
            cleanup_stmt_entry(rec->stmt_entry);
            rec->stmt = NULL;
        }
    }
}

/* This is called at the time of put_prepared_stmt_int()
 * to determine whether the given sql should be cached.
 * We should not cache ddl stmts, analyze commands,
 * rebuild commands, truncate commands, explain commands.
 * Ddl stmts and explain commands should not get to
 * put_prepared_stmt_int() so are not handled in this function.
 */
#define sql_equal(keyword) strncasecmp(sql, keyword, sizeof(keyword) - 1) == 0
static inline int dont_cache_sql(const char *sql)
{
    if (sql_equal("create") || sql_equal("alter") || sql_equal("rebuild") ||
        sql_equal("drop") || sql_equal("analyze") || sql_equal("truncate") ||
        sql_equal("put") || sql_equal("explain")) {
        return 1;
    }
    return 0;
}

static int put_prepared_stmt_int(struct sqlthdstate *thd,
                                 struct sqlclntstate *clnt,
                                 struct sql_state *rec, int outrc)
{
    if (gbl_enable_sql_stmt_caching == STMT_CACHE_NONE) {
        return 1;
    }
    sqlite3_stmt *stmt = rec->stmt;
    if (stmt == NULL) {
        return 1;
    }
    if (gbl_enable_sql_stmt_caching == STMT_CACHE_PARAM &&
        param_count(clnt) == 0) {
        return 1;
    }
    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DISABLE_CACHING_STMT_WITH_FDB) &&
        sqlite3_stmt_has_remotes(stmt)) {
        return 1;
    }
    if (rec->stmt_entry != NULL) {
        if (requeue_stmt_entry(thd, rec->stmt_entry))
            cleanup_stmt_entry(rec->stmt_entry);

        return 0;
    }

    const char *sqlptr = clnt->sql;
    if (rec->sql)
        sqlptr = rec->sql;

    if (dont_cache_sql(sqlptr)) {
        return 1;
    }

    if (rec->status & CACHE_HAS_HINT) {
        sqlptr = rec->cache_hint;
        if (!(rec->status & CACHE_FOUND_STR)) {
            add_sql_hint_table(rec->cache_hint, clnt->sql);
        }
    }
    return add_stmt_table(thd, sqlptr, gbl_debug_temptables ? rec->sql : NULL,
                          stmt);
}

/**
 * Cache a stmt if needed; struct sql_state is prepared by
 * get_prepared_stmt(), and it is cleaned here as well
 *
 */
void put_prepared_stmt(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                       struct sql_state *rec, int outrc)
{
    int rc = put_prepared_stmt_int(thd, clnt, rec, outrc);
    if (rc != 0 && rec->stmt) {
        sqlite3_finalize(rec->stmt);
        rec->stmt = NULL;
    }
    if ((rec->status & CACHE_HAS_HINT) && (rec->status & CACHE_FOUND_STR)) {
        char *k = rec->cache_hint;
        pthread_mutex_lock(&gbl_sql_lock);
        {
            lrucache_release(sql_hints, &k);
        }
        pthread_mutex_unlock(&gbl_sql_lock);
    }
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

    if (clnt->in_client_trans && (rec->status & CACHE_HAS_HINT ||
                                  has_sqlcache_hint(clnt->sql, NULL, NULL)) &&
        !(rec->status & CACHE_FOUND_STR) &&
        (clnt->osql.replay == OSQL_RETRY_NONE)) {

        errstr = (char *)sqlite3_errmsg(thd->sqldb);
        reqlog_logf(thd->logger, REQL_TRACE, "sqlite3_prepare failed %d: %s\n",
                    rc, errstr);
        errstat_set_rcstrf(err, ERR_PREPARE_RETRY, "%s", errstr);

        srs_tran_del_last_query(clnt);
        return;
    }

    if(rc == ERR_SQL_PREPARE && !rec->stmt)
        errstr = "no statement";
    else if (clnt->fdb_state.xerr.errval) {
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
    clnt->had_errors = 1;
    if (gbl_print_syntax_err) {
        logmsg(LOGMSG_WARN, "sqlite3_prepare() failed for: %s [%s]\n", clnt->sql,
                errstr);
    }

    if (clnt->ctrl_sqlengine != SQLENG_NORMAL_PROCESS) {
        /* multiple query transaction
           keep sending back error */
        handle_sql_intrans_unrecoverable_error(clnt);
    }
}

static int send_run_error(struct sqlclntstate *clnt, const char *err, int rc)
{
    pthread_mutex_lock(&clnt->wait_mutex);
    clnt->ready_for_heartbeats = 0;
    pthread_mutex_unlock(&clnt->wait_mutex);
    return write_response(clnt, RESPONSE_ERROR, (void *)err, rc);
}

static int handle_bad_engine(struct sqlclntstate *clnt)
{
    logmsg(LOGMSG_ERROR, "unable to obtain sql engine\n");
    send_run_error(clnt, "Client api should change nodes", CDB2ERR_CHANGENODE);
    clnt->query_rc = -1;
    pthread_mutex_lock(&clnt->wait_mutex);
    clnt->done = 1;
    pthread_cond_signal(&clnt->wait_cond);
    pthread_mutex_unlock(&clnt->wait_mutex);
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
    pthread_mutex_lock(&clnt->wait_mutex);
    clnt->done = 1;
    pthread_cond_signal(&clnt->wait_cond);
    pthread_mutex_unlock(&clnt->wait_mutex);
    clnt->osql.timings.query_finished = osql_log_time();
    osql_log_time_done(clnt);
    return -2;
}

static int prepare_engine(struct sqlthdstate *, struct sqlclntstate *, int);
int sqlengine_prepare_engine(struct sqlthdstate *thd,
                             struct sqlclntstate *clnt, int recreate)
{
    clnt->no_transaction = 1;
    int rc = prepare_engine(thd, clnt, recreate);
    clnt->no_transaction = 0;
    return rc;
}

/**
 * Get a sqlite engine, either from cache or building a new one
 * Locks tables to prevent any schema changes for them
 *
 */
static int get_prepared_stmt_int(struct sqlthdstate *thd,
                                 struct sqlclntstate *clnt,
                                 struct sql_state *rec, struct errstat *err,
                                 int recreate)
{
    int rc = sqlengine_prepare_engine(thd, clnt, recreate);
    if (thd->sqldb == NULL) {
        return handle_bad_engine(clnt);
    } else if (rc != 0) {
        return rc;
    }
    if (sql_set_transaction_mode(thd->sqldb, clnt, clnt->dbtran.mode) != 0) {
        return handle_bad_transaction_mode(thd, clnt);
    }
    query_stats_setup(thd, clnt);
    get_cached_stmt(thd, clnt, rec);
    if (rec->sql)
        reqlog_set_sql(thd->logger, rec->sql);
    const char *tail = NULL;
    while (rec->stmt == NULL) {
        clnt->no_transaction = 1;
        rc = sqlite3_prepare_v2(thd->sqldb, rec->sql, -1, &rec->stmt, &tail);
        clnt->no_transaction = 0;
        if (rc == SQLITE_OK) {
            rc = sqlite3LockStmtTables(rec->stmt);
        } else if (rc == SQLITE_ERROR && comdb2_get_verify_remote_schemas()) {
            sqlite3ResetFdbSchemas(thd->sqldb);
            return rc = SQLITE_SCHEMA_REMOTE;
        }
        if (rc != SQLITE_SCHEMA_REMOTE) {
            break;
        }
        sql_remote_schema_changed(clnt, rec->stmt);
        update_schema_remotes(clnt, rec);
    }
    if (rec->stmt) {
        sqlite3_resetclock(rec->stmt);
        thr_set_current_sql(rec->sql);
    } else if (rc == 0) {
        // No stmt and no error -> Empty sql string or just comment.
        rc = ERR_SQL_PREPARE;
    }
    if (gbl_fingerprint_queries) {
        reqlog_set_fingerprint(thd->logger, sqlite3_fingerprint(thd->sqldb),
                               sqlite3_fingerprint_size(thd->sqldb));
    }
    if (rc) {
        _prepare_error(thd, clnt, rec, rc, err);
    } else {
        clnt->verify_remote_schemas = 0;
    }
    if (tail && *tail) {
        logmsg(LOGMSG_INFO,
               "TRAILING CHARACTERS AFTER QUERY TERMINATION: \"%s\"\n", tail);
    }
    return rc;
}

int get_prepared_stmt(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                      struct sql_state *rec, struct errstat *err)
{
    rdlock_schema_lk();
    int rc = get_prepared_stmt_int(thd, clnt, rec, err, 1);
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

/*
** Only customer is stored-procedure.
** This prevents lock inversion between tbllk and schemalk.
*/
int get_prepared_stmt_try_lock(struct sqlthdstate *thd,
                               struct sqlclntstate *clnt, struct sql_state *rec,
                               struct errstat *err)
{
    if (tryrdlock_schema_lk() != 0) {
        // only schemachange will wrlock(schema)
        sql_debug_logf(clnt, __func__, __LINE__,
                       "Returning SQLITE_SCHEMA on tryrdlock failure\n");
        return SQLITE_SCHEMA;
    }
    int rc = get_prepared_stmt_int(thd, clnt, rec, err, 0);
    unlock_schema_lk();
    return rc;
}

static int bind_parameters(struct reqlogger *logger, sqlite3_stmt *stmt,
                           struct sqlclntstate *clnt, char **err)
{
    int rc = 0;
    int params = param_count(clnt);
    struct cson_array *arr = get_bind_array(logger, params);
    struct param_data p = {0};
    for (int i = 0; i < params; ++i) {
        if ((rc = param_value(clnt, &p, i)) != 0) {
            rc = SQLITE_ERROR;
            goto out;
        }
        if (p.pos == 0) {
            p.pos = sqlite3_bind_parameter_index(stmt, p.name);
        }
        if (p.pos == 0) {
            rc = SQLITE_ERROR;
            goto out;
        }
        if (p.null || p.type == COMDB2_NULL_TYPE) {
            rc = sqlite3_bind_null(stmt, p.pos);
            eventlog_bind_null(arr, p.name);
            continue;
        }
        switch (p.type) {
        case CLIENT_INT:
        case CLIENT_UINT:
            rc = sqlite3_bind_int64(stmt, p.pos, p.u.i);
            eventlog_bind_int64(arr, p.name, p.u.i, p.len);
            break;
        case CLIENT_REAL:
            rc = sqlite3_bind_double(stmt, p.pos, p.u.r);
            eventlog_bind_double(arr, p.name, p.u.r, p.len);
            break;
        case CLIENT_CSTR:
        case CLIENT_PSTR:
        case CLIENT_PSTR2:
            rc = sqlite3_bind_text(stmt, p.pos, p.u.p, p.len, NULL);
            eventlog_bind_text(arr, p.name, p.u.p, p.len);
            break;
        case CLIENT_VUTF8:
            rc = sqlite3_bind_text(stmt, p.pos, p.u.p, p.len, NULL);
            eventlog_bind_varchar(arr, p.name, p.u.p, p.len);
            break;
        case CLIENT_BLOB:
        case CLIENT_BYTEARRAY:
            rc = sqlite3_bind_blob(stmt, p.pos, p.u.p, p.len, NULL);
            eventlog_bind_blob(arr, p.name, p.u.p, p.len);
            break;
        case CLIENT_DATETIME:
        case CLIENT_DATETIMEUS:
            rc = sqlite3_bind_datetime(stmt, p.pos, &p.u.dt, clnt->tzname);
            eventlog_bind_datetime(arr, p.name, &p.u.dt, clnt->tzname);
            break;
        case CLIENT_INTVYM:
        case CLIENT_INTVDS:
        case CLIENT_INTVDSUS:
            rc = sqlite3_bind_interval(stmt, p.pos, &p.u.tv);
            eventlog_bind_interval(arr, p.name, &p.u.tv);
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
    int rc = bind_parameters(thd->logger, rec->stmt, clnt, &errstr);
    if (rc) {
        errstat_set_rcstrf(err, ERR_PREPARE, "%s", errstr);
    }
    return rc;
}

/**
 * Get a sqlite engine with bound parameters set, if any
 *
 */
static int get_prepared_bound_stmt(struct sqlthdstate *thd,
                                   struct sqlclntstate *clnt,
                                   struct sql_state *rec,
                                   struct errstat *err)
{
    int rc;
    if ((rc = get_prepared_stmt(thd, clnt, rec, err)) != 0) {
        return rc;
    }
    int a = sqlite3_bind_parameter_count(rec->stmt);
    int b = param_count(clnt);
    if (a != b) {
        errstat_set_rcstrf(err, ERR_PREPARE,
                           "parameters in stmt:%d  "
                           "parameters provided:%d",
                           a, b);
        return -1;
    }
    int cols = sqlite3_column_count(rec->stmt);
    int overrides = override_count(clnt);
    if (overrides && overrides != cols) {
        errstat_set_rcstrf(err, ERR_PREPARE,
                           "columns in stmt:%d  "
                           "column types provided:%d",
                           cols, overrides);
        return -1;
    }
    reqlog_logf(thd->logger, REQL_INFO, "ncols=%d", cols);
    if (a == 0)
        return 0;
    return bind_params(thd, clnt, rec, err);
}

static void handle_stored_proc(struct sqlthdstate *, struct sqlclntstate *);

static void handle_expert_query(struct sqlthdstate *thd,
                                struct sqlclntstate *clnt)
{
    rdlock_schema_lk();
    sqlengine_prepare_engine(thd, clnt, 1);
    unlock_schema_lk();
    int rc = -1;
    char *zErr = 0;
    sqlite3expert *p = sqlite3_expert_new(thd->sqldb, &zErr);

    if (p) {
        rc = sqlite3_expert_sql(p, clnt->sql, &zErr);
    }

    if (rc == SQLITE_OK) {
        rc = sqlite3_expert_analyze(p, &zErr);
    }

    if (rc == SQLITE_OK) {
        int nQuery = sqlite3_expert_count(p);
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
    clnt->no_transaction = 0;
    return; /* Don't process anything else */
}

/* return 0 continue, 1 return *outrc */
static int handle_non_sqlite_requests(struct sqlthdstate *thd,
                                      struct sqlclntstate *clnt, int *outrc)
{
    int rc;

    sql_update_usertran_state(clnt);

    switch (clnt->ctrl_sqlengine) {

    case SQLENG_PRE_STRT_STATE:
        *outrc = handle_sql_begin(thd, clnt, 1);
        return 1;

    case SQLENG_WRONG_STATE:
        *outrc = handle_sql_wrongstate(thd, clnt);
        return 1;

    case SQLENG_FNSH_STATE:
    case SQLENG_FNSH_RBK_STATE:
        *outrc = handle_sql_commitrollback(thd, clnt, 1);
        return 1;

    case SQLENG_NORMAL_PROCESS:
    case SQLENG_INTRANS_STATE:
    case SQLENG_STRT_STATE:
        /* FALL-THROUGH for update query execution */
        break;
    }

    /* additional non-sqlite requests */
    int stored_proc = 0;
    if ((rc = check_sql(clnt, &stored_proc)) != 0) {
        // TODO: set this: outrc = rc;
        return rc;
    }

    if (stored_proc) {
        handle_stored_proc(thd, clnt);
        *outrc = 0;
        return 1;
    } else if (clnt->is_explain) { // only via newsql--cdb2api
        rdlock_schema_lk();
        sqlengine_prepare_engine(thd, clnt, 1);
        *outrc = newsql_dump_query_plan(clnt, thd->sqldb);
        unlock_schema_lk();
        return 1;
    } else if (clnt->is_expert) {
        handle_expert_query(thd, clnt);
        return 1;
    }

    /* 0, this is an sqlite request, use an engine */
    return 0;
}

static int skip_response_int(struct sqlclntstate *clnt, int from_error)
{
    if (clnt->osql.replay == OSQL_RETRY_DO)
        return 1;
    if (clnt->isselect || is_with_statement(clnt->sql))
        return 0;
    if (clnt->in_client_trans) {
        if (from_error && !clnt->had_errors) /* send on first error */
            return 0;
        if (clnt->send_intrans_results)
            return 0;
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

static int send_row(struct sqlclntstate *clnt, struct sqlite3_stmt *stmt,
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
    clnt->has_recording |= v->recording;
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
            /* convert this to a user code */
            irc = (clnt->osql.error_is_remote)
                      ? irc
                      : blockproc2sql_error(irc, __func__, __LINE__);
            if (irc == CDB2ERR_VERIFY_ERROR && replicant_can_retry(clnt) &&
                !clnt->has_recording && clnt->osql.replay == OSQL_RETRY_NONE) {
                osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_DO);
            }
        }
        /* if this is a single query, we need to send back an answer here */
        if (clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS) {
            /* if this is still a verify error but we tried to many times,
               send error back anyway by resetting the replay field */
            if (irc == CDB2ERR_VERIFY_ERROR &&
                clnt->osql.replay == OSQL_RETRY_LAST) {
                osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);
            }
            /* if this is still an error, but not verify, pass it back to
               client */
            else if (irc && irc != CDB2ERR_VERIFY_ERROR) {
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
                                  int ncols, uint64_t row_id)
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
        pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;
        pthread_mutex_unlock(&clnt->wait_mutex);
        if (!skip_response(clnt)) {
            if (postponed_write)
                send_row(clnt, NULL, row_id, 0, NULL);
            write_response(clnt, RESPONSE_EFFECTS, 0, 0);
            write_response(clnt, RESPONSE_ROW_LAST, 0, 0);
        }
    }
    return 0;
}

/* The design choice here for communication is to send row data inside this function,
   and delegate the error sending to the caller (since we send multiple rows, but we 
   send error only once and stop processing at that time)
 */   
static int run_stmt(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                    struct sql_state *rec, int *fast_error, 
                    struct errstat *err)
{
    int rc;
    int steprc;
    uint64_t row_id = 0;
    int rowcount = 0;
    int postponed_write = 0;
    sqlite3_stmt *stmt = rec->stmt;

    reqlog_set_event(thd->logger, "sql");
    run_stmt_setup(clnt, stmt);

    /* this is a regular sql query, add it to history */
    if (srs_tran_add_query(clnt))
        logmsg(LOGMSG_ERROR,
               "Fail to add query to transaction replay session\n");

    int ncols = sqlite3_column_count(stmt);

    /* Get first row to figure out column structure */
    steprc = sqlite3_step(stmt);
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
        return verify_indexes_column_value(stmt, clnt->schema_mems);
    } else if (clnt->verify_indexes && steprc == SQLITE_DONE) {
        clnt->has_sqliterow = 0;
        return 0;
    }

    if ((rc = send_columns(clnt, stmt)) != 0) {
        goto out;
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
        /* replication contention reduction */
        rc = release_locks_on_emit_row(thd, clnt);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: release_locks_on_emit_row failed\n",
                   __func__);
            goto out;
        }

        if (clnt->isselect == 1) {
            clnt->effects.num_selected++;
            clnt->log_effects.num_selected++;
            clnt->nrows++;
        }

        /* return row, if needed */
        if (clnt->isselect && clnt->osql.replay != OSQL_RETRY_DO) {
            postponed_write = 0;
            ++row_id;
            rc = send_row(clnt, stmt, row_id, 0, err);
            if (rc)
                goto out;
        } else {
            postponed_write = 1;
            send_row(clnt, stmt, row_id, 1, NULL);
        }

        rowcount++;
        reqlog_set_rows(thd->logger, rowcount);
        clnt->recno++;
        if (clnt->rawnodestats)
            clnt->rawnodestats->sql_rows++;

    } while ((rc = sqlite3_step(stmt)) == SQLITE_ROW);

    /* whatever sqlite returns in sqlite3_step is only used to step out of the
     * loop, otherwise ignored; we are gonna get it from sqlite (or osql.xerr)
     */

postprocessing:
    /* closing: error codes, postponed write result and so on*/
    rc = post_sqlite_processing(thd, clnt, rec, postponed_write, ncols, row_id);

out:
    return rc;
}

static void handle_sqlite_error(struct sqlthdstate *thd,
                                struct sqlclntstate *clnt,
                                struct sql_state *rec)
{
    if (thd->sqlthd)
        thd->sqlthd->nmove = thd->sqlthd->nfind = thd->sqlthd->nwrite =
            thd->sqlthd->ntmpread = thd->sqlthd->ntmpwrite = 0;

    clnt->had_errors = 1;

    if (clnt->using_case_insensitive_like)
        toggle_case_sensitive_like(thd->sqldb, 0);

    reqlog_end_request(thd->logger, -1, __func__, __LINE__);
}

static void sqlite_done(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                        struct sql_state *rec, int outrc)
{
    sqlite3_stmt *stmt = rec->stmt;

    sql_statement_done(thd->sqlthd, thd->logger, clnt, outrc);

    if (stmt && !((Vdbe *)stmt)->explain && ((Vdbe *)stmt)->nScan > 1 &&
        (BDB_ATTR_GET(thedb->bdb_attr, PLANNER_WARN_ON_DISCREPANCY) == 1 ||
         BDB_ATTR_GET(thedb->bdb_attr, PLANNER_SHOW_SCANSTATS) == 1)) {
        compare_estimate_cost(stmt);
    }

    put_prepared_stmt(thd, clnt, rec, outrc);

    if (clnt->using_case_insensitive_like)
        toggle_case_sensitive_like(thd->sqldb, 0);

#ifdef DEBUG_SQLITE_MEMORY
    int had_blocks = 0;
    hash_for(sql_blocks, dump_block, &had_blocks);
    if (had_blocks)
        printf("end of blocks\n");
#endif

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
    char *errstr;
    reqlog_set_event(thd->logger, "sp");
    clnt->trans_has_sp = 1;
    int rc = exec_procedure(thd, clnt, &errstr);
    if (rc) {
        clnt->had_errors = 1;
        if (rc == -1)
            rc = -3;
        write_response(clnt, RESPONSE_ERROR, errstr, rc);
        if (errstr) {
            free(errstr);
        }
    }
    if (!clnt->in_client_trans)
        clnt->trans_has_sp = 0;
    test_no_btcursors(thd);
    sqlite_done(thd, clnt, &rec, 0);
}

static int handle_sqlite_requests(struct sqlthdstate *thd,
                                  struct sqlclntstate *clnt)
{
    int rc;
    int fast_error;
    struct errstat err = {0};
    struct sql_state rec = {0};
    rec.sql = clnt->sql;

    do {
        /* clean old stats */
        clear_cost(thd->sqlthd);

        /* get an sqlite engine */
        rc = get_prepared_bound_stmt(thd, clnt, &rec, &err);
        if (rc == SQLITE_SCHEMA_REMOTE)
            continue;
        if (rc) {
            int irc = errstat_get_rc(&err);
            /* certain errors are saved, in that case we don't send anything */
            if (irc == ERR_PREPARE)
                write_response(clnt, RESPONSE_ERROR_PREPARE, err.errstr, 0);
            else if (irc == ERR_PREPARE_RETRY)
                write_response(clnt, RESPONSE_ERROR_PREPARE_RETRY, err.errstr, 0);
            goto errors;
        }

        /* run the engine */
        fast_error = 0;

        if (clnt->statement_query_effects)
            reset_query_effects(clnt);

        rc = run_stmt(thd, clnt, &rec, &fast_error, &err);
        if (rc) {
            int irc = errstat_get_rc(&err);
            switch(irc) {
            case ERR_ROW_HEADER:
            case ERR_CONVERSION_DT:
                send_run_error(clnt, errstat_get_str(&err), CDB2ERR_CONV_FAIL);
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
    reqlog_set_event(thd->logger, "sql"); /* set before error */
    reqlog_set_error(thd->logger, sqlite3_errmsg(thd->sqldb), rc);
    handle_sqlite_error(thd, clnt, &rec);
    goto done;
}

static int check_sql_access(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    int rc;

    if (gbl_check_access_controls) {
        check_access_controls(thedb);
        gbl_check_access_controls = 0;
    }

    /* If 1) this is an SSL connection, 2) and client sends a certificate,
       3) and client does not override the user, let it through. */
    if (sslio_has_x509(clnt->sb) && clnt->is_x509_user)
        rc = 0;
    else
        rc = check_user_password(clnt);

    if (rc == 0) {
        if (thd->lastuser[0] != '\0' && strcmp(thd->lastuser, clnt->user) != 0)
            delete_prepared_stmts(thd);
        strcpy(thd->lastuser, clnt->user);
    }
    return rc;
}

/**
 * Main driver of SQL processing, for both sqlite and non-sqlite requests
 */
static int execute_sql_query(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    int outrc;
    int rc;

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

    /* All requests that do not require a sqlite engine
       are processed below.  A return != 0 means processing
       done
     */
    if ((rc = handle_non_sqlite_requests(thd, clnt, &outrc)) != 0) {
        return outrc;
    }

    /* This is a request that require a sqlite engine */
    return handle_sqlite_requests(thd, clnt);
}

// call with schema_lk held + no_transaction
static int prepare_engine(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                          int recreate)
{
    struct errstat xerr;
    int rc;
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
    if (thd->sqldb && (rc = check_thd_gen(thd, clnt)) != SQLITE_OK) {
        if (rc != SQLITE_SCHEMA_REMOTE) {
            if (!recreate) {
                return rc;
            }
            delete_prepared_stmts(thd);
            sqlite3_close(thd->sqldb);
            thd->sqldb = NULL;
        }
    }
    if (gbl_enable_sql_stmt_caching && (thd->stmt_caching_table == NULL)) {
        init_stmt_caching_table(thd);
    }
    if (!thd->sqldb || (rc == SQLITE_SCHEMA_REMOTE)) {
        /* need to refresh things; we need to grab views lock */
        if (!got_views_lock) {
            unlock_schema_lk();

            if (!clnt->dbtran.cursor_tran) {
                rc = get_curtran(thedb->bdb_env, clnt);
                if (rc) {
                    logmsg(LOGMSG_ERROR,
                           "%s: unable to get a CURSOR transaction, rc = %d!\n",
                           __func__, rc);
                } else {
                    got_curtran = 1;
                }
            }

            views_lock();
            rdlock_schema_lk();
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
                logmsg(LOGMSG_ERROR, "%s:sqlite3_open_serial failed %d\n", __func__,
                        rc);
                thd->sqldb = NULL;
                /* there is no really way forward, grab core */
                abort();
            }
            thd->dbopen_gen = gbl_dbopen_gen;
        }

        get_copy_rootpages_nolock(thd->sqlthd);
        if (clnt->dbtran.cursor_tran) {
            if (thedb->timepart_views) {
                int cnonce = has_cnonce(clnt);
                clr_cnonce(clnt);
                /* how about we are gonna add the views ? */
                rc = views_sqlite_update(thedb->timepart_views, thd->sqldb,
                                         &xerr, 0);
                if (rc != VIEW_NOERR) {
                    logmsg(LOGMSG_ERROR, 
                            "failed to create views rc=%d errstr=\"%s\"\n",
                            xerr.errval, xerr.errstr);
                }
                if (cnonce)
                    set_cnonce(clnt);
            }

            /* save the views generation number */
            thd->views_gen = gbl_views_gen;
        }
    }
    if (got_views_lock) {
        views_unlock();
    }

    if (got_curtran && put_curtran(thedb->bdb_env, clnt)) {
        logmsg(LOGMSG_ERROR, "%s: unable to destroy a CURSOR transaction!\n",
               __func__);
    }

    return rc;
}

static void clean_queries_not_cached_in_srs(struct sqlclntstate *clnt)
{
    pthread_mutex_lock(&clnt->wait_mutex);
    clnt->done = 1;
    pthread_cond_signal(&clnt->wait_cond);
    pthread_mutex_unlock(&clnt->wait_mutex);
}

static void thr_set_user(int id)
{
    char thdinfo[40];
    snprintf(thdinfo, sizeof(thdinfo), "appsock %u", id);
    thrman_setid(thrman_self(), thdinfo);
}

static void debug_close_sb(struct sqlclntstate *clnt)
{
    static int once = 0;

    if (debug_switch_sql_close_sbuf()) {
        if (!once) {
            once = 1;
            sbuf2close(clnt->sb);
        }
    } else
        once = 0;
}

static void sqlengine_work_lua_thread(void *thddata, void *work)
{
    struct sqlthdstate *thd = thddata;
    struct sqlclntstate *clnt = work;

    if (!clnt->exec_lua_thread)
        abort();

    thr_set_user(clnt->appsock_id);

    clnt->osql.timings.query_dispatched = osql_log_time();
    clnt->deque_timeus = comdb2_time_epochus();

    rdlock_schema_lk();
    sqlengine_prepare_engine(thd, clnt, 1);
    unlock_schema_lk();

    reqlog_set_origin(thd->logger, "%s", clnt->origin);

    clnt->query_rc = exec_thread(thd, clnt);

    sql_reset_sqlthread(thd->sqldb, thd->sqlthd);

    clnt->osql.timings.query_finished = osql_log_time();
    osql_log_time_done(clnt);

    clean_queries_not_cached_in_srs(clnt);

    debug_close_sb(clnt);

    thrman_setid(thrman_self(), "[done]");
}

int gbl_debug_sqlthd_failures;

static int execute_verify_indexes(struct sqlthdstate *thd,
                                  struct sqlclntstate *clnt)
{
    int rc;
    if (thd->sqldb == NULL) {
        /* open sqlite db without copying rootpages */
        rc = sqlite3_open_serial("db", &thd->sqldb, thd);
        if (unlikely(rc != 0)) {
            logmsg(LOGMSG_ERROR, "%s:sqlite3_open_serial failed %d\n", __func__,
                   rc);
            thd->sqldb = NULL;
        } else {
            /* setting gen to -1 so real SQLs will reopen vm */
            thd->dbopen_gen = -1;
            thd->analyze_gen = -1;
        }
    }
    sqlite3_stmt *stmt;
    const char *tail;
    rc = sqlite3_prepare_v2(thd->sqldb, clnt->sql, -1, &stmt, &tail);
    if (rc != SQLITE_OK) {
        return rc;
    }
    bind_verify_indexes_query(stmt, clnt->schema_mems);
    run_stmt_setup(clnt, stmt);
    if ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        clnt->has_sqliterow = 1;
        return verify_indexes_column_value(stmt, clnt->schema_mems);
    }
    clnt->has_sqliterow = 0;
    if (rc == SQLITE_DONE) {
        return 0;
    }
    return rc;
}

void sqlengine_work_appsock(void *thddata, void *work)
{
    struct sqlthdstate *thd = thddata;
    struct sqlclntstate *clnt = work;
    struct sql_thread *sqlthd = thd->sqlthd;
    if (sqlthd) {
        sqlthd->clnt = clnt;
    } else {
        abort();
    }

    thr_set_user(clnt->appsock_id);

    clnt->added_to_hist = clnt->isselect = 0;
    clnt->osql.timings.query_dispatched = osql_log_time();
    clnt->deque_timeus = comdb2_time_epochus();

    reqlog_set_origin(thd->logger, "%s", clnt->origin);

    if (clnt->dbtran.mode == TRANLEVEL_SOSQL &&
        clnt->client_understands_query_stats && clnt->osql.rqid)
        osql_query_dbglog(sqlthd, clnt->queryid);

    /* everything going in is cursor based */
    int rc = get_curtran(thedb->bdb_env, clnt);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: unable to get a CURSOR transaction, rc=%d!\n",
                __func__, rc);
        send_run_error(clnt, "Client api should change nodes",
                       CDB2ERR_CHANGENODE);
        clnt->query_rc = -1;
        pthread_mutex_lock(&clnt->wait_mutex);
        clnt->done = 1;
        pthread_cond_signal(&clnt->wait_cond);
        pthread_mutex_unlock(&clnt->wait_mutex);
        clnt->osql.timings.query_finished = osql_log_time();
        osql_log_time_done(clnt);
        return;
    }

    /* it is a new query, it is time to clean the error */
    if (clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS)
        bzero(&clnt->osql.xerr, sizeof(clnt->osql.xerr));

    /* this could be done on sql_set_transaction_mode, but it
       affects all code paths and I don't like it */
    if (clnt->dbtran.mode == TRANLEVEL_RECOM ||
        clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
        clnt->dbtran.mode == TRANLEVEL_SERIAL ||
        /* socksql has special needs because of inlining */
        (clnt->dbtran.mode == TRANLEVEL_SOSQL &&
         (clnt->ctrl_sqlengine == SQLENG_STRT_STATE ||
          clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS))) {
        clnt->osql.host = (thedb->master == gbl_mynode) ? 0 : thedb->master;
    }

    /* assign this query a unique id */
    sql_get_query_id(sqlthd);

    /* actually execute the query */
    thrman_setfd(thd->thr_self, sbuf2fileno(clnt->sb));

    osql_shadtbl_begin_query(thedb->bdb_env, clnt);

    if (clnt->fdb_state.remote_sql_sb) {
        clnt->query_rc = execute_sql_query_offload(thd, clnt);
        /* execute sql query might have generated an overriding fdb error;
           reset it here before returning */
        bzero(&clnt->fdb_state.xerr, sizeof(clnt->fdb_state.xerr));
        clnt->fdb_state.preserve_err = 0;
    } else if (clnt->verify_indexes) {
        clnt->query_rc = execute_verify_indexes(thd, clnt);
    } else {
        clnt->query_rc = execute_sql_query(thd, clnt);
    }

    osql_shadtbl_done_query(thedb->bdb_env, clnt);
    thrman_setfd(thd->thr_self, -1);
    sql_reset_sqlthread(thd->sqldb, sqlthd);
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
    clean_queries_not_cached_in_srs(clnt);
    debug_close_sb(clnt);
    thrman_setid(thrman_self(), "[done]");
}

static void sqlengine_work_appsock_pp(struct thdpool *pool, void *work,
                                      void *thddata, int op)
{
    struct sqlclntstate *clnt = work;
    int rc = 0;

    switch (op) {
    case THD_RUN:
        if (clnt->exec_lua_thread)
            sqlengine_work_lua_thread(thddata, work);
        else
            sqlengine_work_appsock(thddata, work);
        break;
    case THD_FREE:
        /* we just mark the client done here, with error */
        ((struct sqlclntstate *)work)->query_rc = CDB2ERR_IO_ERROR;
        ((struct sqlclntstate *)work)->done =
            1; /* that's gonna revive appsock thread */
        break;
    }
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

int dispatch_sql_query(struct sqlclntstate *clnt)
{
    int done;
    char msg[1024];
    char *sqlcpy;
    char thdinfo[40];
    int rc;
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

    pthread_mutex_lock(&clnt->wait_mutex);
    clnt->deadlock_recovered = 0;
    clnt->done = 0;
    clnt->statement_timedout = 0;

    /* keep track so we can display it in stat thr */
    clnt->appsock_id = getarchtid();

    pthread_mutex_unlock(&clnt->wait_mutex);

    snprintf(msg, sizeof(msg), "%s \"%s\"", clnt->origin, clnt->sql);
    clnt->enque_timeus = comdb2_time_epochus();

    sqlcpy = strdup(msg);
    if ((rc = thdpool_enqueue(gbl_sqlengine_thdpool, sqlengine_work_appsock_pp,
                              clnt, clnt->queue_me,
                              sqlcpy)) != 0) {
        if ((clnt->in_client_trans || clnt->osql.replay == OSQL_RETRY_DO) &&
            gbl_requeue_on_tran_dispatch) {
            /* force this request to queue */
            rc = thdpool_enqueue(gbl_sqlengine_thdpool,
                                 sqlengine_work_appsock_pp, clnt, 1, sqlcpy);
        }

        if (rc) {
            free(sqlcpy);
            /* say something back, if the client expects it */
            if (clnt->fail_dispatch) {
                snprintf(msg, sizeof(msg), "%s: unable to dispatch sql query\n",
                         __func__);
                handle_failed_dispatch(clnt, msg);
            }

            return -1;
        }
    }

    /* successful dispatch or queueing, enable heartbeats */
    pthread_mutex_lock(&clnt->wait_mutex);
    clnt->ready_for_heartbeats = 1;
    pthread_mutex_unlock(&clnt->wait_mutex);

    /* SQL thread will unlock mutex when it is done, allowing us to lock it
     * again.  We block until then. */
    if (self)
        thrman_where(self, "waiting for query");

    if (clnt->heartbeat) {
        if (clnt->osql.replay != OSQL_RETRY_NONE || clnt->in_client_trans) {
            send_heartbeat(clnt);
        }
        const struct timespec ms100 = {.tv_sec = 0, .tv_nsec = 100000000};
        struct timespec first, last;
        clock_gettime(CLOCK_REALTIME, &first);
        last = first;
        while (1) {
            struct timespec now, st;
            clock_gettime(CLOCK_REALTIME, &now);
            TIMESPEC_ADD(now, ms100, st);

            pthread_mutex_lock(&clnt->wait_mutex);
            if (clnt->done) {
                pthread_mutex_unlock(&clnt->wait_mutex);
                goto done;
            }
            int rc;
            rc = pthread_cond_timedwait(&clnt->wait_cond, &clnt->wait_mutex,
                                        &st);
            if (clnt->done) {
                pthread_mutex_unlock(&clnt->wait_mutex);
                goto done;
            }
            if (rc == ETIMEDOUT) {
                struct timespec diff;
                TIMESPEC_SUB(st, last, diff);
                if (diff.tv_sec >= clnt->heartbeat) {
                    last = st;
                    send_heartbeat(clnt);
                    fdb_heartbeats(clnt);
                }
                if (clnt->query_timeout > 0 && !clnt->statement_timedout) {
                    TIMESPEC_SUB(st, first, diff);
                    if (diff.tv_sec >= clnt->query_timeout) {
                        clnt->statement_timedout = 1;
                        logmsg(LOGMSG_WARN, "%s:%d Query exceeds max allowed time %d.\n",
                                __FILE__, __LINE__, clnt->query_timeout);
                    }
                }
            } else if (rc) {
                logmsg(LOGMSG_FATAL, "%s:%d pthread_cond_timedwait rc %d", __FILE__,
                        __LINE__, rc);
                exit(1);
            }

            if (pthread_mutex_trylock(&clnt->write_lock) == 0) {
                sbuf2flush(clnt->sb);
                pthread_mutex_unlock(&clnt->write_lock);
            }
            pthread_mutex_unlock(&clnt->wait_mutex);
        }
    } else {
        pthread_mutex_lock(&clnt->wait_mutex);
        while (!clnt->done) {
            pthread_cond_wait(&clnt->wait_cond, &clnt->wait_mutex);
        }
        pthread_mutex_unlock(&clnt->wait_mutex);
    }

done:
    if (self)
        thrman_where(self, "query done");
    return clnt->query_rc;
}

void sqlengine_thd_start(struct thdpool *pool, struct sqlthdstate *thd,
                         enum thrtype type)
{
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);

    sql_mem_init(NULL);

    if (!gbl_use_appsock_as_sqlthread)
        thd->thr_self = thrman_register(type);

    thd->logger = thrman_get_reqlogger(thd->thr_self);
    thd->sqldb = NULL;
    thd->stmt_caching_table = NULL;
    thd->lastuser[0] = '\0';

    start_sql_thread();

    thd->sqlthd = pthread_getspecific(query_info_key);
    void rcache_init(size_t, size_t);
    rcache_init(bdb_attr_get(thedb->bdb_attr, BDB_ATTR_RCACHE_COUNT),
                bdb_attr_get(thedb->bdb_attr, BDB_ATTR_RCACHE_PGSZ));
}

int gbl_abort_invalid_query_info_key;

void sqlengine_thd_end(struct thdpool *pool, struct sqlthdstate *thd)
{
    void rcache_destroy(void);
    rcache_destroy();
    struct sql_thread *sqlthd;
    if ((sqlthd = pthread_getspecific(query_info_key)) != NULL) {
        /* sqlclntstate shouldn't be set: sqlclntstate is memory on another
         * thread's stack that will not be valid at this point. */

        if (sqlthd->clnt) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d sqlthd->clnt set in thd-teardown\n", __FILE__,
                   __LINE__);
            if (gbl_abort_invalid_query_info_key) {
                abort();
            }
            sqlthd->clnt = NULL;
        }
    }

    if (thd->stmt_caching_table)
        delete_stmt_caching_table(thd->stmt_caching_table);
    if (thd->sqldb)
        sqlite3_close(thd->sqldb);

    /* AZ moved after the close which uses thd for rollbackall */
    done_sql_thread();

    sql_mem_shutdown(NULL);

    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
}

static void thdpool_sqlengine_start(struct thdpool *pool, void *thd)
{
    sqlengine_thd_start(pool, (struct sqlthdstate *) thd, THRTYPE_SQLENGINEPOOL);
}

static void thdpool_sqlengine_end(struct thdpool *pool, void *thd)
{
    sqlengine_thd_end(pool, (struct sqlthdstate *) thd);
}

int tdef_to_tranlevel(int tdef)
{
    switch (tdef) {
    case SQL_TDEF_SOCK:
        return TRANLEVEL_SOSQL;

    case SQL_TDEF_RECOM:
        return TRANLEVEL_RECOM;

    case SQL_TDEF_SERIAL:
        return TRANLEVEL_SERIAL;

    case SQL_TDEF_SNAPISOL:
        return TRANLEVEL_SNAPISOL;

    default:
        logmsg(LOGMSG_FATAL, "%s: line %d Unknown modedef: %d", __func__, __LINE__,
                tdef);
        abort();
    }
}

void cleanup_clnt(struct sqlclntstate *clnt)
{
    if (clnt->argv0) {
        free(clnt->argv0);
        clnt->argv0 = NULL;
    }

    if (clnt->stack) {
        free(clnt->stack);
        clnt->stack = NULL;
    }

    if (clnt->saved_errstr) {
        free(clnt->saved_errstr);
        clnt->saved_errstr = NULL;
    }

    if (clnt->context) {
        for (int i = 0; i < clnt->ncontext; i++) {
            free(clnt->context[i]);
        }
        free(clnt->context);
        clnt->context = NULL;
    }

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

    if (gbl_expressions_indexes) {
        if (clnt->idxInsert)
            free(clnt->idxInsert);
        if (clnt->idxDelete)
            free(clnt->idxDelete);
        clnt->idxInsert = clnt->idxDelete = NULL;
    }

    destroy_hash(clnt->ddl_tables);
    destroy_hash(clnt->dml_tables);
    clnt->ddl_tables = NULL;
    clnt->dml_tables = NULL;
}

void reset_clnt(struct sqlclntstate *clnt, SBUF2 *sb, int initial)
{
    int wrtimeoutsec = 0;
    if (initial) {
        bzero(clnt, sizeof(*clnt));
    }
    if (clnt->rawnodestats) {
        release_node_stats(clnt->argv0, clnt->stack, clnt->origin);
        clnt->rawnodestats = NULL;
    }
    clnt->sb = sb;
    clnt->must_close_sb = 1;
    clnt->recno = 1;
    strcpy(clnt->tzname, "America/New_York");
    clnt->dtprec = gbl_datetime_precision;
    bzero(&clnt->conninfo, sizeof(clnt->conninfo));
    clnt->using_case_insensitive_like = 0;

    if (clnt->ctrl_sqlengine != SQLENG_INTRANS_STATE)
        clnt->intrans = 0;

    /* start off in comdb2 mode till we're told otherwise */
    clnt->dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
    clnt->heartbeat = 0;
    clnt->limits.maxcost = gbl_querylimits_maxcost;
    clnt->limits.tablescans_ok = gbl_querylimits_tablescans_ok;
    clnt->limits.temptables_ok = gbl_querylimits_temptables_ok;
    clnt->limits.maxcost_warn = gbl_querylimits_maxcost_warn;
    clnt->limits.tablescans_warn = gbl_querylimits_tablescans_warn;
    clnt->limits.temptables_warn = gbl_querylimits_temptables_warn;


    reset_query_effects(clnt);

    /* reset the user */
    clnt->have_user = 0;
    clnt->is_x509_user = 0;
    bzero(clnt->user, sizeof(clnt->user));

    /* reset the password */
    clnt->have_password = 0;
    bzero(clnt->password, sizeof(clnt->password));

    /* reset extended_tm */
    clnt->have_extended_tm = 0;
    clnt->extended_tm = 0;

    clnt->is_readonly = 0;
    clnt->ignore_coherency = 0;

    /* reset page-order. */
    clnt->pageordertablescan =
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_PAGE_ORDER_TABLESCAN);

    /* let's reset osql structure as well */
    osql_clean_sqlclntstate(clnt);
    /* clear dbtran after aborting unfinished shadow transactions. */
    bzero(&clnt->dbtran, sizeof(dbtran_type));
    clnt->origin = intern(get_origin_mach_by_buf(sb));

    clnt->in_client_trans = 0;
    clnt->had_errors = 0;
    clnt->statement_timedout = 0;
    clnt->query_timeout = 0;
    if (clnt->saved_errstr) {
        free(clnt->saved_errstr);
        clnt->saved_errstr = NULL;
    }
    clnt->saved_rc = 0;
    clnt->want_stored_procedure_debug = 0;
    clnt->want_stored_procedure_trace = 0;
    clnt->verifyretry_off = 0;
    clnt->is_expert = 0;

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
    clnt_reset_cursor_hints(clnt);

    clnt->send_intrans_results = 1;

    bzero(clnt->dirty, sizeof(clnt->dirty));

    if (gbl_sqlwrtimeoutms == 0)
        wrtimeoutsec = 0;
    else
        wrtimeoutsec = gbl_sqlwrtimeoutms / 1000;

    if (sb && sbuf2fileno(sb) > 2) // if valid sb and sb->fd is not stdio
    {
        net_add_watch_warning(
            clnt->sb, bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAX_SQL_IDLE_TIME),
            wrtimeoutsec, clnt, watcher_warning_function);
    }
    clnt->planner_effort =
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_PLANNER_EFFORT);
    clnt->osql_max_trans = g_osql_max_trans;

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
    clnt->context = NULL;
    clnt->ncontext = 0;
    clnt->statement_query_effects = 0;
    clnt->wrong_db = 0;
}

void reset_clnt_flags(struct sqlclntstate *clnt)
{
    clnt->writeTransaction = 0;
    clnt->has_recording = 0;
}

void handle_sql_intrans_unrecoverable_error(struct sqlclntstate *clnt)
{
    int osqlrc = 0;
    int rc = 0;
    int bdberr = 0;

    if (clnt && clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
        switch (clnt->dbtran.mode) {
        case TRANLEVEL_SOSQL:
            osql_sock_abort(clnt, OSQL_SOCK_REQ);
            if (clnt->selectv_arr) {
                currangearr_free(clnt->selectv_arr);
                clnt->selectv_arr = NULL;
            }
            break;

        case TRANLEVEL_RECOM:
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
                                SQLENG_NORMAL_PROCESS);
    }
}

int sbuf_is_local(SBUF2 *sb)
{
    struct sockaddr_in addr;

    if (net_appsock_get_addr(sb, &addr))
        return 1;

    if (addr.sin_addr.s_addr == gbl_myaddr.s_addr)
        return 1;

    if (addr.sin_addr.s_addr == INADDR_LOOPBACK)
        return 1;

    return 0;
}

int sql_writer(SBUF2 *sb, const char *buf, int nbytes)
{
    extern int gbl_sql_release_locks_on_slow_reader;
    ssize_t nwrite, written = 0;
    int retry = -1;
    int released_locks = 0;
retry:
    retry++;
    while (written < nbytes) {
        struct pollfd pd;
        int fd = pd.fd = sbuf2fileno(sb);
        pd.events = POLLOUT;
        errno = 0;
        int rc = poll(&pd, 1, 100);
        if (rc < 0) {
            if (errno == EINTR || errno == EAGAIN) {
                if (gbl_sql_release_locks_on_slow_reader && !released_locks) {
                    if (release_locks("slow reader") != 0) {
                        logmsg(LOGMSG_ERROR, "%s release_locks failed\n",
                               __func__);
                        return -1;
                    }
                    released_locks = 1;
                }
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "%s poll rc:%d errno:%d errstr:%s\n", __func__,
                   rc, errno, strerror(errno));
            return rc;
        }
        if (rc == 0) { // descriptor not ready, write will block
            if (gbl_sql_release_locks_on_slow_reader && !released_locks) {
                rc = release_locks("slow reader");
                if (rc) {
                    logmsg(LOGMSG_ERROR,
                           "%s release_locks generation changed\n", __func__);
                    return -1;
                }
                released_locks = 1;
            }

            if (bdb_lock_desired(thedb->bdb_env)) {
                struct sql_thread *thd = pthread_getspecific(query_info_key);
                if (thd) {
                    rc = recover_deadlock(thedb->bdb_env, thd, NULL, 0);
                    if (rc) {
                        logmsg(LOGMSG_ERROR,
                               "%s recover_deadlock generation changed\n",
                               __func__);
                        return -1;
                    }
                }
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
                    rc = getpeername(fd, (struct sockaddr *)&peeraddr, &len);
                    if (rc == -1 && errno == ENOTCONN) {
                        ctrace("fd %d disconnected\n", fd);
                        return -1;
                    }
                }
            }
#endif
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

static LISTC_T(struct sqlconn) conns;

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
    struct sql_thread *thd;
    sqlite3 *db;
    int i;

    return;

#if 0
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
    double cost;
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
struct client_query_stats *get_query_stats_from_thd()
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (!thd)
        return NULL;

    struct sqlclntstate *clnt = thd->clnt;
    if (!clnt)
        return NULL;

    if (!clnt->query_stats)
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
        while ((ret = sqlite3_step(stmt)) == SQLITE_ROW)
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
                clnt->dbtran.mode == TRANLEVEL_SERIAL) {
                pthread_mutex_lock(&clnt->dtran_mtx);
            }

            ret = sqlite3_step(stmt);

            if (clnt->dbtran.mode == TRANLEVEL_RECOM ||
                clnt->dbtran.mode == TRANLEVEL_SERIAL) {
                pthread_mutex_unlock(&clnt->dtran_mtx);
            }

            if (ret != SQLITE_ROW) {
                break;
            }

            if (res.z) {
                /* now we have the packed sqlite row in Mem->z */
                rc = fdb_svc_sql_row(clnt->fdb_state.remote_sql_sb, cid, res.z,
                                     res.n, IX_FNDMORE,
                                     clnt->osql.rqid == OSQL_RQID_USE_UUID);
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
                                     res.n, IX_FND,
                                     clnt->osql.rqid == OSQL_RQID_USE_UUID);
            } else {
                rc = fdb_svc_sql_row(clnt->fdb_state.remote_sql_sb, cid, res.z,
                                     res.n, IX_EMPTY,
                                     clnt->osql.rqid == OSQL_RQID_USE_UUID);
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
    if (!thd) {
        logmsg(LOGMSG_ERROR, "%s: no sql_thread\n", __func__);
        return SQLITE_INTERNAL;
    }
    reqlog_new_sql_request(poolthd->logger, clnt->sql);
    log_queue_time(poolthd->logger, clnt);
    bzero(&clnt->fail_reason, sizeof(clnt->fail_reason));
    bzero(&clnt->osql.xerr, sizeof(clnt->osql.xerr));
    struct sql_state rec = {0};
    rec.sql = clnt->sql;
    if (get_prepared_bound_stmt(poolthd, clnt, &rec, &clnt->osql.xerr)) {
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

    if (!clnt->fdb_state.remote_sql_sb) {
        rc = osql_block_commit(thd);
        if (rc)
            logmsg(LOGMSG_ERROR, 
                    "%s: sqloff_block_send_done failed to write reply\n",
                    __func__);
    }

    sqlite_done(poolthd, clnt, &rec, ret);

    return ret;
}

int sql_testrun(char *sql, int sqllen) { return 0; }

int sqlpool_init(void)
{
    gbl_sqlengine_thdpool =
        thdpool_create("sqlenginepool", sizeof(struct sqlthdstate));

    if (gbl_exit_on_pthread_create_fail)
        thdpool_set_exit(gbl_sqlengine_thdpool);

    /* big fat stack to handle big queries */
    thdpool_set_stack_size(gbl_sqlengine_thdpool, 4 * 1024 * 1024);
    thdpool_set_init_fn(gbl_sqlengine_thdpool, thdpool_sqlengine_start);
    thdpool_set_delt_fn(gbl_sqlengine_thdpool, thdpool_sqlengine_end);
    thdpool_set_minthds(gbl_sqlengine_thdpool, 4);
    thdpool_set_maxthds(gbl_sqlengine_thdpool, 48);
    thdpool_set_linger(gbl_sqlengine_thdpool, 30);
    thdpool_set_maxqueueoverride(gbl_sqlengine_thdpool, 500);
    thdpool_set_maxqueueagems(gbl_sqlengine_thdpool, 5 * 60 * 1000);
    thdpool_set_dump_on_full(gbl_sqlengine_thdpool, 1);

    return 0;
}

/* we have to clear
      - sqlclntstate (key, pointers in Bt, thd)
      - thd->tran and mode (this is actually done in Commit/Rollback)
 */
static void sql_reset_sqlthread(sqlite3 *db, struct sql_thread *thd)
{
    int i;

    if (thd) {
        thd->clnt = NULL;
    }
}

/**
 * Resets sqlite engine to retrieve the error code
 */
int sql_check_errors(struct sqlclntstate *clnt, sqlite3 *sqldb,
                     sqlite3_stmt *stmt, const char **errstr)
{

    int rc = sqlite3_reset(stmt);

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

    case SQLITE_TOOBIG:
        *errstr = "transaction too big, try increasing the limit using 'SET "
                  "maxtransize N'";
        rc = ERR_TRAN_TOO_BIG;
        break;

    case SQLITE_ABORT:
        /* no error in this case, regular abort or
           block processor failure to commit */
        rc = SQLITE_OK;
        break;

    case SQLITE_ERROR:
        /* check for convertion failure, stored in clnt->fail_reason */
        if (clnt->fail_reason.reason != CONVERT_OK) {
            rc = ERR_CONVERT_DTA;
        }
        *errstr = sqlite3_errmsg(sqldb);
        break;

    case SQLITE_LIMIT:
        *errstr = "Query exceeded set limits";
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
            *errstr = "type conversion failure";
        break;

    case SQLITE_TRANTOOCOMPLEX:
        *errstr = "Transaction rollback too large";
        break;

    case SQLITE_TRAN_CANCELLED:
        *errstr = "Unable to maintain snapshot, too many resources blocked";
        break;

    case SQLITE_TRAN_NOLOG:
        *errstr = "Unable to maintain snapshot, too many log files";
        break;

    case SQLITE_TRAN_NOUNDO:
        *errstr = "Database changed due to sc or fastinit; snapshot failure";
        break;

    case SQLITE_CLIENT_CHANGENODE:
        *errstr = "Client api should run query against a different node";
        break;

    case 147: // 147 = 0 - SQLHERR_MASTER_TIMEOUT
        *errstr = "Client api should run query against a different node";
        rc = SQLITE_CLIENT_CHANGENODE;
        break;

    case SQLITE_SCHEMA_REMOTE:
        rc = SQLITE_OK; /* this is processed based on clnt->osql.xerr */
        break;

    case SQLITE_COMDB2SCHEMA:
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

    return rc;
}

enum {
    /* LEGACY CODES */
    DB_ERR_BAD_REQUEST = 110,  /* 199 */
    DB_ERR_BAD_COMM_BUF = 111, /* 998 */
    DB_ERR_BAD_COMM = 112,     /* 999 */
    DB_ERR_NONKLESS = 114,     /* 212 */
    /* GENERAL BLOCK TRN RCODES */
    DB_ERR_TRN_BUF_INVALID = 200,   /* 105 */
    DB_ERR_TRN_BUF_OVERFLOW = 201,  /* 106 */
    DB_ERR_TRN_OPR_OVERFLOW = 202,  /* 205 */
    DB_ERR_TRN_DB_FAIL = 204,       /* 220 */
    DB_ERR_TRN_NOT_SERIAL = 230,
    DB_ERR_TRN_SC = 240, /* should be client side code as well*/
    /* INTERNAL DB ERRORS */
    DB_ERR_INTR_GENERIC = 304,
    DB_ERR_INTR_READ_ONLY = 305,
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
    case 195:
        return DB_ERR_INTR_READ_ONLY;
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

    case ERR_NULL_CONSTRAINT:
        return CDB2ERR_NULL_CONSTRAINT;

    case SQLITE_ACCESS:
        return CDB2ERR_ACCESS;

    case 1229: /* ERR_BLOCK_FAILED + OP_FAILED_INTERNAL + ERR_FIND_CONSTRAINT */
        return CDB2ERR_FKEY_VIOLATION;

    case ERR_UNCOMMITABLE_TXN:
        return CDB2ERR_VERIFY_ERROR;

    case ERR_REJECTED:
        return SQLHERR_MASTER_QUEUE_FULL;

    case SQLHERR_MASTER_TIMEOUT:
        return SQLHERR_MASTER_TIMEOUT;

    case ERR_NOT_DURABLE:
        return CDB2ERR_CHANGENODE;

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
    case SQLITE_LIMIT:
        return SQLHERR_LIMIT;
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
        return;

    logmsg(LOGMSG_USER, "total=%llu msec (longest=%llu msec) calls=%llu\n",
            fdbtms->total_time, fdbtms->max_call, fdbtms->total_calls);
}

static void sql_thread_describe(void *obj, FILE *out)
{
    struct sqlclntstate *clnt = (struct sqlclntstate *)obj;
    char *host;

    if (!clnt) {
        logmsg(LOGMSG_USER, "non sql thread ???\n");
        return;
    }

    if (clnt->origin[0]) {
        logmsg(LOGMSG_USER, "%s \"%s\"\n", clnt->origin, clnt->sql);
    } else {
        host = get_origin_mach_by_buf(clnt->sb);
        logmsg(LOGMSG_USER, "(old client) %s \"%s\"\n", host, clnt->sql);
    }
}

int watcher_warning_function(void *arg, int timeout, int gap)
{
    struct sqlclntstate *clnt = (struct sqlclntstate *)arg;

    logmsg(LOGMSG_WARN, 
            "WARNING: appsock idle for %d seconds (%d), connected from %s\n",
            gap, timeout, (clnt->origin) ? clnt->origin : "(unknown)");

    return 1; /* cancel recurrent */
}

static void dump_sql_hint_entry(void *item, void *p)
{
    int *count = (int *)p;
    sql_hint_hash_entry_type *entry = (sql_hint_hash_entry_type *)item;

    logmsg(LOGMSG_USER, "%d hit %d ref %d   %s  => %s\n", *count, entry->lnk.hits,
           entry->lnk.ref, entry->sql_hint, entry->sql_str);
    (*count)++;
}

void sql_dump_hints(void)
{
    int count = 0;
    pthread_mutex_lock(&gbl_sql_lock);
    {
        lrucache_foreach(sql_hints, dump_sql_hint_entry, &count);
    }
    pthread_mutex_unlock(&gbl_sql_lock);
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

    dispatch_sql_query(&clnt);
    if (clnt.query_rc || clnt.saved_errstr) {
        logmsg(LOGMSG_ERROR, "%s: Error from query: '%s' (rc = %d) \n", __func__, sql,
               clnt.query_rc);
        if (clnt.saved_errstr)
            logmsg(LOGMSG_ERROR, "%s: Error: '%s' \n", __func__, clnt.saved_errstr);
    }
    clnt_reset_cursor_hints(&clnt);
    osql_clean_sqlclntstate(&clnt);

    if (clnt.dbglog) {
        sbuf2close(clnt.dbglog);
        clnt.dbglog = NULL;
    }

    cleanup_clnt(&clnt);

    pthread_mutex_destroy(&clnt.wait_mutex);
    pthread_cond_destroy(&clnt.wait_cond);
    pthread_mutex_destroy(&clnt.write_lock);
    pthread_mutex_destroy(&clnt.dtran_mtx);
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
void start_internal_sql_clnt(struct sqlclntstate *clnt)
{
    reset_clnt(clnt, NULL, 1);
    plugin_set_callbacks(clnt, internal);
    pthread_mutex_init(&clnt->wait_mutex, NULL);
    pthread_cond_init(&clnt->wait_cond, NULL);
    pthread_mutex_init(&clnt->write_lock, NULL);
    pthread_mutex_init(&clnt->dtran_mtx, NULL);
    clnt->dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
    clr_high_availability(clnt);
}

int run_internal_sql_clnt(struct sqlclntstate *clnt, char *sql)
{
#ifdef DEBUGQUERY
    printf("run_internal_sql_clnt() sql '%s'\n", sql);
#endif
    clnt->sql = skipws(sql);
    dispatch_sql_query(clnt);
    int rc = 0;

    if (clnt->query_rc || clnt->saved_errstr) {
        logmsg(LOGMSG_ERROR, "%s: Error from query: '%s' (rc = %d) \n", __func__, sql,
               clnt->query_rc);
        if (clnt->saved_errstr)
            logmsg(LOGMSG_ERROR, "%s: Error: '%s' \n", __func__, clnt->saved_errstr);
        rc = 1;
    }
    return rc;
}

void end_internal_sql_clnt(struct sqlclntstate *clnt)
{
    clnt_reset_cursor_hints(clnt);
    osql_clean_sqlclntstate(clnt);

    if (clnt->dbglog) {
        sbuf2close(clnt->dbglog);
        clnt->dbglog = NULL;
    }

    clnt->dbtran.mode = TRANLEVEL_INVALID;
    cleanup_clnt(clnt);

    pthread_mutex_destroy(&clnt->wait_mutex);
    pthread_cond_destroy(&clnt->wait_cond);
    pthread_mutex_destroy(&clnt->write_lock);
    pthread_mutex_destroy(&clnt->dtran_mtx);
}
