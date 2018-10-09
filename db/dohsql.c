/*
   Copyright 2018 Bloomberg Finance L.P.

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

#include <poll.h>
#include "comdb2.h"
#include "sql.h"
#include "shard_range.h"
#include "sqliteInt.h"
#include "queue.h"
#include "dohsql.h"
#include "sqlinterfaces.h"
#include "memcompare.c"

static int gbl_plugin_api_debug = 0;
#define verbose bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DOHSQL_VERBOSE)

struct col {
    int type;
    char *name;
};
typedef struct col col_t;

enum doh_status { DOH_RUNNING = 0, DOH_MASTER_DONE = 1, DOH_CLIENT_DONE = 2 };

struct dohsql_connector {
    struct sqlclntstate *clnt;
    queue_type *que;      /* queue to caller */
    queue_type *que_free; /* de-queued rows come here to be freed */
    pthread_mutex_t mtx;  /* mutex for queueing operations and related counts */
    char *thr_where;      /* cached where status */
    col_t *cols;          /* cached cols values */
    int ncols;            /* number of columns */
    int rc;
    int nrows;              /* current total queued rows */
    enum doh_status status; /* caller is done */
};

typedef struct dohsql_connector dohsql_connector_t;

typedef Mem row_t;
enum {
    ILIMIT_MEM_IDX = 0,
    ILIMIT_SAVED_MEM_IDX = 1,
    IOFFSET_MEM_IDX = 2,
    IOFFSETLIMIT_MEM_IDX = 3,
    IOFFSET_SAVED_MEM_IDX = 4,
    MAX_MEM_IDX = 5
};

struct dohsql {
    int nconns;
    dohsql_connector_t *conns;
    col_t *cols;
    int ncols;
    row_t *row;
    int row_src;
    int rc;
    /* LIMIT support */
    int limitRegs[MAX_MEM_IDX]; /* sqlite engine limit registers*/
    int limit;                  /* any limit */
    int nrows;                  /* sent rows so far */
    /* OFFSET support */
    int offset;  /* any offset */
    int skipped; /* how many rows where skipped so far */
    /* ORDER BY support */
    int filling;
    int active;
    int *order;
    int top_idx;
};
typedef struct dohsql dohsql_t;

static void sqlengine_work_shard_pp(struct thdpool *pool, void *work,
                                    void *thddata, int op);
static void sqlengine_work_shard(struct thdpool *pool, void *work,
                                 void *thddata);
static int order_init(dohsql_t *conns);
static int dohsql_dist_next_row_ordered(struct sqlclntstate *clnt,
                                        sqlite3_stmt *stmt);

static void sqlengine_work_shard_pp(struct thdpool *pool, void *work,
                                    void *thddata, int op)
{
    switch (op) {
    case THD_RUN:
        sqlengine_work_shard(pool, work, thddata);
        break;
    case THD_FREE:
        /* error, we are done */
        ((struct sqlclntstate *)work)->query_rc = -1;
        ((struct sqlclntstate *)work)->done = 1;
        break;
    }
}

void handle_child_error(struct sqlclntstate *clnt, int errcode)
{
    dohsql_connector_t *conn = (dohsql_connector_t *)clnt->plugin.state;

    if (conn) {
        pthread_mutex_lock(&conn->mtx);
        conn->rc = -1;
        pthread_mutex_unlock(&conn->mtx);
    }
}

static void sqlengine_work_shard(struct thdpool *pool, void *work,
                                 void *thddata)
{
    struct sqlthdstate *thd = thddata;
    struct sqlclntstate *clnt = (struct sqlclntstate *)work;
    int rc;

    thr_set_user("shard thread", clnt->appsock_id);

    rdlock_schema_lk();
    sqlengine_prepare_engine(thd, clnt, 0);
    unlock_schema_lk();

    reqlog_set_origin(thd->logger, "%s", clnt->origin);

    rc = get_curtran(thedb->bdb_env, clnt);
    if (rc) {
        handle_child_error(clnt, rc);
        return;
    }

    /* assign this query a unique id */
    sql_get_query_id(thd->sqlthd);

    /*
    Review: read committed and friends
    // Set whatever mode this client needs
    rc = sql_set_transaction_mode(thd->sqldb, clnt, clnt_parent->dbtran.mode);
    osql_shadtbl_begin_query(thedb->bdb_env, clnt);
    //expanded execute_sql_query
    query_stats_setup(thd, clnt);
    */

    clnt->query_rc = handle_sqlite_requests(thd, clnt);

    if (clnt->query_rc != SQLITE_OK) {
        if (verbose)
            logmsg(LOGMSG_DEBUG, "XXX: client %p returned error %d\n",
                   clnt->plugin.state, clnt->query_rc);
        handle_child_error(clnt, clnt->query_rc);
    }

    sql_reset_sqlthread(thd->sqlthd);

    if (put_curtran(thedb->bdb_env, clnt)) {
        logmsg(LOGMSG_ERROR, "%s: unable to destroy a CURSOR transaction!\n",
               __func__);
    }

    clnt->osql.timings.query_finished = osql_log_time();
    osql_log_time_done(clnt);

    /*thrman_setid(thrman_self(), "[done]");*/
}

static int inner_type(sqlite3_stmt *stmt, int col)
{
    int type = sqlite3_column_type(stmt, col);
    if (type == SQLITE_NULL) {
        type = typestr_to_type(sqlite3_column_decltype(stmt, col));
    }
    if (type == SQLITE_DECIMAL) {
        type = SQLITE_TEXT;
    }
    return type;
}

static int inner_columns(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    dohsql_connector_t *conn = (dohsql_connector_t *)clnt->plugin.state;
    int ncols, i;

    ncols = sqlite3_column_count(stmt);

    if (!conn->cols || conn->ncols < ncols) {
        conn->cols = (col_t *)realloc(conn->cols, ncols * sizeof(col_t));
        if (!conn->cols)
            return -1;
    }
    conn->ncols = ncols;

    for (i = 0; i < ncols; i++) {
        conn->cols[i].type = inner_type(stmt, i);
    }
    return 0;
}

static void trimQue(sqlite3_stmt *stmt, queue_type *que, int limit)
{
    row_t *row;
    return;
    while (queue_count(que) > limit) {
        row = queue_next(que);
        if (verbose)
            logmsg(LOGMSG_DEBUG, "XXX: %p freed older row limit %d\n", que,
                   limit);
        sqlite3CloneResultFree(stmt, &row);
    }
}

static int inner_row(struct sqlclntstate *clnt, struct response_data *resp,
                     int postpone)
{
    dohsql_connector_t *conn = (dohsql_connector_t *)clnt->plugin.state;
    sqlite3_stmt *stmt = resp->stmt;

    row_t *row;
    row_t *oldrow;

    oldrow = NULL;
    pthread_mutex_lock(&conn->mtx);

    if (conn->status == DOH_MASTER_DONE) {
        if (verbose)
            logmsg(LOGMSG_DEBUG, "%lx %s master done q %d qf %d\n",
                   pthread_self(), __func__, queue_count(conn->que),
                   queue_count(conn->que_free));
        /* work is done, need to clean-up */
        trimQue(stmt, conn->que, 0);
        trimQue(stmt, conn->que_free, 0);

        conn->rc = SQLITE_DONE; /* signal master this is clear */

        pthread_mutex_unlock(&conn->mtx);

        return SQLITE_DONE; /* any != 0 will do, this impersonates a normal end
                             */
    }

    /* try to steal an old row */
    if (queue_count(conn->que_free) > 0) {
        if (verbose)
            logmsg(LOGMSG_DEBUG, "%lx %s retrieved older row\n", pthread_self(),
                   __func__);
        oldrow = queue_next(conn->que_free);
    }
    pthread_mutex_unlock(&conn->mtx);

    row = sqlite3CloneResult(stmt, &oldrow);
    if (!row)
        return SHARD_ERR_GENERIC;

    pthread_mutex_lock(&conn->mtx);
    conn->rc = SQLITE_ROW;
    queue_add(conn->que, row);
    if (verbose)
        logmsg(LOGMSG_DEBUG, "%lx XXX: %p added new row\n", pthread_self(),
               conn);

    /* inline cleanup */
    if (queue_count(conn->que_free) > 10) {
        trimQue(stmt, conn->que, 5);
    }
    pthread_mutex_unlock(&conn->mtx);

    return SHARD_NOERR;
}

static int inner_row_last(struct sqlclntstate *clnt)
{
    dohsql_connector_t *conn = (dohsql_connector_t *)clnt->plugin.state;

    pthread_mutex_lock(&conn->mtx);
    conn->rc = SQLITE_DONE;
    pthread_mutex_unlock(&conn->mtx);

    return SHARD_NOERR;
}

/* override sqlite engine */
static int dohsql_dist_column_count(struct sqlclntstate *clnt,
                                    sqlite3_stmt *stmt)
{
    return clnt->conns->ncols;
}

#define FUNC_COLUMN_TYPE(ret, type)                                            \
    static ret dohsql_dist_column_##type(struct sqlclntstate *clnt,            \
                                         sqlite3_stmt *stmt, int iCol)         \
    {                                                                          \
        dohsql_t *conns = clnt->conns;                                         \
        if (conns->row_src == 0)                                               \
            return sqlite3_column_##type(stmt, iCol);                          \
        return sqlite3_value_##type(&conns->row[iCol]);                        \
    }

FUNC_COLUMN_TYPE(int, type)
FUNC_COLUMN_TYPE(sqlite_int64, int64)
FUNC_COLUMN_TYPE(double, double)
FUNC_COLUMN_TYPE(int, bytes)
FUNC_COLUMN_TYPE(const unsigned char *, text)
FUNC_COLUMN_TYPE(const void *, blob)
FUNC_COLUMN_TYPE(const dttz_t *, datetime)

static const intv_t *dohsql_dist_column_interval(struct sqlclntstate *clnt,
                                                 sqlite3_stmt *stmt, int iCol,
                                                 int type)
{
    dohsql_t *conns = clnt->conns;
    if (conns->row_src == 0)
        return sqlite3_column_interval(stmt, iCol, type);
    return sqlite3_value_interval(&conns->row[iCol], type);
}

static sqlite3_value *dohsql_dist_column_value(struct sqlclntstate *clnt,
                                               sqlite3_stmt *stmt, int i)
{
    dohsql_t *conns = clnt->conns;

    if (conns->row_src == 0)
        return sqlite3_column_value(stmt, i);

    return &conns->row[i];
}

static void add_row(dohsql_t *conns, int i, row_t *row)
{
    if (conns->row && conns->row_src) {
        /* put the used row in the free list */
        if (i != conns->row_src)
            pthread_mutex_lock(&conns->conns[conns->row_src].mtx);
        queue_add(conns->conns[conns->row_src].que_free, conns->row);
        if (i != conns->row_src)
            pthread_mutex_unlock(&conns->conns[conns->row_src].mtx);
    }
    /* new row */
    conns->row = row;
    conns->row_src = i;
}

#define CHILD_DONE(kid)                                                        \
    (queue_count(conns->conns[(kid)].que) == 0 &&                              \
     conns->conns[(kid)].rc == SQLITE_DONE)
#define CHILD_ERROR(kid)                                                       \
    (conns->conns[(kid)].rc != SQLITE_ROW &&                                   \
     conns->conns[(kid)].rc != SQLITE_DONE)

static void _signal_children_master_is_done(dohsql_t *conns)
{
    int child_num;

    for (child_num = 1; child_num < conns->nconns; child_num++) {
        pthread_mutex_lock(&conns->conns[child_num].mtx);
        if (conns->conns[child_num].status != DOH_CLIENT_DONE) {
            if (verbose)
                logmsg(LOGMSG_DEBUG, "%s: signalling client done, ignoring\n",
                       __func__);
            conns->conns[child_num].status = DOH_MASTER_DONE;
        }
        pthread_mutex_unlock(&conns->conns[child_num].mtx);
    }
}

static int _get_a_parallel_row(dohsql_t *conns, row_t **prow)
{
    int child_num;
    int rc = SQLITE_DONE;

    *prow = NULL;

    for (child_num = 1; child_num < conns->nconns && (*prow) == NULL;
         child_num++) {
        pthread_mutex_lock(&conns->conns[child_num].mtx);
        /* done */
        if (CHILD_DONE(child_num)) {
            pthread_mutex_unlock(&conns->conns[child_num].mtx);
            continue;
        }
        /* error */
        if (CHILD_ERROR(child_num)) {
            /* debatable if we wanna clear cached rows before check for error */
            rc = conns->conns[child_num].rc;
            pthread_mutex_unlock(&conns->conns[child_num].mtx);
            /* we could envision a case when child is retried for cut 2*/
            /* for now, signal all children that we are done and pass error
               to caller */
            _signal_children_master_is_done(conns);
            return rc;
        }
        rc = SQLITE_OK;
        if (queue_count(conns->conns[child_num].que) > 0) {
            *prow = queue_next(conns->conns[child_num].que);
            if (verbose)
                logmsg(LOGMSG_DEBUG, "XXX: %p retrieved row\n",
                       &conns->conns[child_num]);
            add_row(conns, child_num, *prow);
            rc = SQLITE_ROW;
        }
        pthread_mutex_unlock(&conns->conns[child_num].mtx);
    }

    if (verbose)
        logmsg(LOGMSG_DEBUG, "XXX: parallel row rc = %d\n", rc);

    return rc;
}

static int init_next_row(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    dohsql_t *conns = clnt->conns;
    int rc;

    rc = sqlite3_step(stmt);

    if (verbose) {
        logmsg(LOGMSG_DEBUG, "%lx %s: sqlite3_step rc %d\n", pthread_self(),
               __func__, rc);
        if (conns->limitRegs[ILIMIT_SAVED_MEM_IDX] > 0)
            logmsg(LOGMSG_DEBUG,
                   "%lx clnt %p conns %p limitMem %d:%d limit %d "
                   "offsetMem %d:%d offset %d\n",
                   pthread_self(), clnt, conns,
                   conns->limitRegs[ILIMIT_MEM_IDX],
                   conns->limitRegs[ILIMIT_SAVED_MEM_IDX], conns->limit,
                   conns->limitRegs[IOFFSET_MEM_IDX],
                   conns->limitRegs[IOFFSET_SAVED_MEM_IDX], conns->offset);
    }

    if (rc == SQLITE_ROW)
        return rc;

    if (rc == SQLITE_DONE) {
        conns->conns[0].rc = SQLITE_DONE;
        return SQLITE_DONE;
    }

    _signal_children_master_is_done(conns);
    return rc;
}

static int _check_limit(sqlite3_stmt *stmt, dohsql_t *conns)
{
    if (conns->limitRegs[ILIMIT_SAVED_MEM_IDX] > 0 && conns->limit >= 0 &&
        conns->nrows >= conns->limit) {
        if (verbose)
            logmsg(LOGMSG_DEBUG, "%lx REACHED LIMIT rc =%d!\n", pthread_self(),
                   conns->conns[0].rc);
        if (conns->conns[0].rc != SQLITE_DONE) {
            if (verbose)
                logmsg(LOGMSG_DEBUG, "RESET STMT!\n");
            sqlite3_reset(stmt);
        }

        _signal_children_master_is_done(conns);
        return SQLITE_DONE;
    }

    return SQLITE_OK;
}

static int _check_offset(dohsql_t *conns)
{
    if (conns->limitRegs[IOFFSET_SAVED_MEM_IDX] &&
        conns->skipped < conns->offset) {
        conns->skipped++;
        if (verbose)
            logmsg(LOGMSG_DEBUG,
                   "XXX: skipped client %d row skipped %d, offset =%d\n",
                   conns->row_src, conns->skipped, conns->offset);
        /* skip it */
        return SQLITE_OK;
    }

    if (verbose)
        logmsg(LOGMSG_DEBUG, "XXX: returned source %d row\n", conns->row_src);
    return SQLITE_ROW;
}

/**
 * this is a non-ordered merge of N engine outputs
 *
 */
static int dohsql_dist_next_row(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    dohsql_t *conns = clnt->conns;
    row_t *row;
    int empty;
    int rc;

    if (verbose)
        logmsg(LOGMSG_DEBUG, "%lx %s: start\n", pthread_self(), __func__);
    if (conns->nrows == 0) {
        rc = init_next_row(clnt, stmt);
        if (rc == SQLITE_ROW) {
            add_row(conns, 0, NULL);
            goto got_row;
        }
        if (rc != SQLITE_DONE)
            return rc;
    }

    rc = _check_limit(stmt, conns);
    if (rc != SQLITE_OK)
        return rc;

wait_for_others:
    rc = 0;
    empty = 1;
    rc = _get_a_parallel_row(conns, &row);
    if (rc == SQLITE_ROW) {
        assert(row);
        goto got_row;
    }
    if (rc == SQLITE_OK)
        empty = 0;
    else if (rc != SQLITE_DONE)
        return rc;

    /* no row in others (yet) */
    if (conns->conns[0].rc != SQLITE_DONE) {
        rc = sqlite3_step(stmt);

        if (verbose)
            logmsg(LOGMSG_DEBUG, "%s: rc =%d\n", __func__, rc);

        if (rc == SQLITE_DONE)
            conns->conns[0].rc = SQLITE_DONE;
        else {
            if (rc == SQLITE_ROW) {
                add_row(conns, 0, NULL);
                goto got_row;
            }

            _signal_children_master_is_done(conns);
            return rc;
        }
    }
    if (!empty) {
        poll(NULL, 0, 10);
        goto wait_for_others;
    }

    /* error or done */
    return SQLITE_DONE;

got_row:
    /* limit support */
    rc = _check_offset(conns);
    if (rc != SQLITE_ROW)
        goto wait_for_others;

    conns->nrows++;
    return SQLITE_ROW;
}

static int dohsql_write_response(struct sqlclntstate *c, int t, void *a, int i)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s %d\n", pthread_self(), __func__, t);
    switch (t) {
    case RESPONSE_COLUMNS:
        return inner_columns(c, a);
    case RESPONSE_COLUMNS_STR:
        return 0 /*newsql_columns_str(c, a, i)*/;
    case RESPONSE_DEBUG:
        return 0 /*newsql_debug(c, a)*/;
    case RESPONSE_ERROR:
        return 0 /*newsql_error(c, a, i)*/;
    case RESPONSE_ERROR_ACCESS:
        return 0 /*newsql_error(c, a, CDB2ERR_ACCESS)*/;
    case RESPONSE_ERROR_BAD_STATE:
        return 0 /*newsql_error(c, a, CDB2ERR_BADSTATE)*/;
    case RESPONSE_ERROR_PREPARE:
        return 0 /*newsql_error(c, a, CDB2ERR_PREPARE_ERROR)*/;
    case RESPONSE_ERROR_REJECT:
        return 0 /*newsql_error(c, a, CDB2ERR_REJECTED)*/;
    case RESPONSE_FLUSH:
        return 0 /*newsql_flush(c)*/;
    case RESPONSE_HEARTBEAT:
        return 0 /*newsql_heartbeat(c)*/;
    case RESPONSE_ROW:
        return inner_row(c, a, i);
    case RESPONSE_ROW_LAST:
        return inner_row_last(c);
    case RESPONSE_ROW_LAST_DUMMY:
        return 0 /*newsql_row_last_dummy(c)*/;
    case RESPONSE_ROW_LUA:
        return 0 /*newsql_row_lua(c, a)*/;
    case RESPONSE_ROW_STR:
        return 0 /*newsql_row_str(c, a, i)*/;
    case RESPONSE_TRACE:
        return 0 /*newsql_trace(c, a)*/;
    /* fastsql only messages */
    case RESPONSE_COST:
    case RESPONSE_EFFECTS:
    case RESPONSE_ERROR_PREPARE_RETRY:
        return 0;
    case RESPONSE_COLUMNS_LUA:
    default:
        abort();
    }
    return 0;
}
static int dohsql_read_response(struct sqlclntstate *a, int b, void *c, int d)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s %d\n", pthread_self(), __func__, b);
    return -1;
}
static void *dohsql_save_stmt(struct sqlclntstate *clnt, void *arg)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return strdup(clnt->sql);
}
static void *dohsql_restore_stmt(struct sqlclntstate *clnt, void *arg)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    clnt->sql = arg;
    return NULL;
}
static void *dohsql_destroy_stmt(struct sqlclntstate *clnt, void *arg)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    free(arg);
    return NULL;
}
static void *dohsql_print_stmt(struct sqlclntstate *clnt, void *arg)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return arg;
}
static int dohsql_param_count(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s TODO\n", pthread_self(), __func__);
    return 0;
}
static int dohsql_param_index(struct sqlclntstate *a, const char *b, int64_t *c)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return -1;
}
static int dohsql_param_value(struct sqlclntstate *a, struct param_data *b,
                              int c)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return -1;
}
static int dohsql_override_count(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s TODO\n", pthread_self(), __func__);
    return 0;
}
static int dohsql_clr_cnonce(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return -1;
}
static int dohsql_has_cnonce(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return 0;
}
static int dohsql_set_cnonce(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return -1;
}
static int dohsql_get_cnonce(struct sqlclntstate *a, snap_uid_t *b)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return -1;
}
static int dohsql_get_snapshot(struct sqlclntstate *a, int *b, int *c)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return -1;
}
static int dohsql_upd_snapshot(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return -1;
}
static int dohsql_clr_snapshot(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return -1;
}
static int dohsql_has_high_availability(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return 0;
}
static int dohsql_set_high_availability(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return -1;
}
static int dohsql_clr_high_availability(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return -1;
}
static int dohsql_get_high_availability(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return 0;
}
static void dohsql_add_steps(struct sqlclntstate *a, double b)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
}
static void dohsql_setup_client_info(struct sqlclntstate *clnt,
                                     struct sqlthdstate *b, char *c)
{
    dohsql_connector_t *conn = (dohsql_connector_t *)clnt->plugin.state;

    if (conn->thr_where)
        thrman_wheref(thrman_self(), "%s", conn->thr_where);

    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s %s\n", pthread_self(), __func__,
               (conn->thr_where) ? conn->thr_where : "NULL");
}
static int dohsql_skip_row(struct sqlclntstate *a, uint64_t b)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return 0;
}
static int dohsql_log_context(struct sqlclntstate *a, struct reqlogger *b)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s TODO\n", pthread_self(), __func__);
    return 0;
}
static uint64_t dohsql_get_client_starttime(struct sqlclntstate *clnt)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return 0;
}
static int dohsql_get_client_retries(struct sqlclntstate *clnt)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return 0;
}
static int dohsql_send_intrans_response(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%lx %s\n", pthread_self(), __func__);
    return 0;
}

static int _shard_connect(struct sqlclntstate *clnt, dohsql_connector_t *conn,
                          const char *sql)
{
    conn->clnt = (struct sqlclntstate *)calloc(1, sizeof(struct sqlclntstate));
    if (!conn->clnt) {
        return SHARD_ERR_MALLOC;
    }
    conn->que = queue_new();
    if (!conn->que) {
        free(conn->clnt);
        conn->clnt = NULL;
        return SHARD_ERR_MALLOC;
    }
    conn->que_free = queue_new();
    if (!conn->que_free) {
        queue_free(conn->que);
        conn->que = NULL;
        free(conn->clnt);
        conn->clnt = NULL;
        return SHARD_ERR_MALLOC;
    }
    pthread_mutex_init(&conn->mtx, NULL);

    comdb2uuid(conn->clnt->osql.uuid);
    conn->clnt->appsock_id = getarchtid();
    init_sqlclntstate(conn->clnt, (char *)conn->clnt->osql.uuid, 1);
    conn->clnt->origin = clnt->origin;
    conn->clnt->sql = strdup(sql);
    plugin_set_callbacks(conn->clnt, dohsql);
    conn->clnt->plugin.state = conn;
    conn->thr_where = strdup(thrman_get_where(thrman_self()));
    conn->rc = SQLITE_ROW;

    return SHARD_NOERR;
}

static void _shard_disconnect(dohsql_connector_t *conn)
{
    struct sqlclntstate *clnt = conn->clnt;

    free(conn->thr_where);
    queue_free(conn->que);
    queue_free(conn->que_free);

    free(clnt->sql);
    clnt->sql = NULL;
    cleanup_clnt(clnt);
    pthread_mutex_destroy(&clnt->wait_mutex);
    pthread_cond_destroy(&clnt->wait_cond);
    pthread_mutex_destroy(&clnt->write_lock);
    pthread_mutex_destroy(&clnt->dtran_mtx);
    free(clnt);
}

static void _master_clnt_set(struct sqlclntstate *clnt)
{
    clnt->plugin.column_count = dohsql_dist_column_count;
    clnt->plugin.next_row = (clnt->conns->order) ? dohsql_dist_next_row_ordered
                                                 : dohsql_dist_next_row;
    clnt->plugin.column_type = dohsql_dist_column_type;
    clnt->plugin.column_int64 = dohsql_dist_column_int64;
    clnt->plugin.column_double = dohsql_dist_column_double;
    clnt->plugin.column_text = dohsql_dist_column_text;
    clnt->plugin.column_bytes = dohsql_dist_column_bytes;
    clnt->plugin.column_blob = dohsql_dist_column_blob;
    clnt->plugin.column_datetime = dohsql_dist_column_datetime;
    clnt->plugin.column_interval = dohsql_dist_column_interval;
}

static void _master_clnt_reset(struct sqlclntstate *clnt)
{
    clnt->plugin.column_count = NULL;
    clnt->plugin.next_row = NULL;
    clnt->plugin.column_type = NULL;
    clnt->plugin.column_int64 = NULL;
    clnt->plugin.column_double = NULL;
    clnt->plugin.column_text = NULL;
    clnt->plugin.column_bytes = NULL;
    clnt->plugin.column_blob = NULL;
    clnt->plugin.column_datetime = NULL;
    clnt->plugin.column_interval = NULL;
}

int dohsql_distribute(dohsql_node_t *node)
{
    GET_CLNT;
    dohsql_t *conns;
    char *sqlcpy;
    int i, rc;

    /* setup communication queue */
    conns = (dohsql_t *)calloc(
        1, sizeof(dohsql_t) + node->nnodes * sizeof(dohsql_connector_t));
    if (!conns)
        return SHARD_ERR_MALLOC;
    conns->conns = (dohsql_connector_t *)(conns + 1);
    conns->nconns = node->nnodes;
    conns->ncols = node->ncols;
    if (node->has_order) {
        if (order_init(conns)) {
            free(conns);
            return SHARD_ERR_MALLOC;
        }
    }
    clnt->conns = conns;
    /* augment interface */
    _master_clnt_set(clnt);

    /* start peers */
    for (i = 0; i < conns->nconns; i++) {
        if ((rc = _shard_connect(clnt, &conns->conns[i],
                                 node->nodes[i]->sql)) != 0)
            return rc;

        if (i > 0) {
            /* launch the new sqlite engine a the next shard */
            rc = thdpool_enqueue(gbl_sqlengine_thdpool, sqlengine_work_shard_pp,
                                 clnt->conns->conns[i].clnt, 1,
                                 sqlcpy = strdup(node->nodes[i]->sql), 0);
            if (rc) {
                free(sqlcpy);
                return SHARD_ERR_GENERIC;
            }
        }
    }

    return SHARD_NOERR;
}

int dohsql_end_distribute(struct sqlclntstate *clnt)
{
    dohsql_t *conns = clnt->conns;
    int i;

    if (!clnt->conns)
        return SHARD_NOERR;

    for (i = 1; i < conns->nconns; i++) {
        pthread_mutex_lock(&conns->conns[i].mtx);
        while (conns->conns[i].status != DOH_CLIENT_DONE) {
            pthread_mutex_unlock(&conns->conns[i].mtx);
            poll(NULL, 0, 10);
            pthread_mutex_lock(&conns->conns[i].mtx);
        }
        pthread_mutex_unlock(&conns->conns[i].mtx);
    }

    for (i = 1; i < conns->nconns; i++) {
        _shard_disconnect(&conns->conns[i]);
    }

    if (clnt->conns->order)
        free(clnt->conns->order);
    free(clnt->conns);
    _master_clnt_reset(clnt);
    clnt->conns = NULL;

    return SHARD_NOERR;
}

#define DOHSQL_CLIENT                                                          \
    (clnt->plugin.state && clnt->plugin.write_response == dohsql_write_response)

void dohsql_wait_for_master(sqlite3_stmt *stmt, struct sqlclntstate *clnt)
{
    dohsql_connector_t *conn;

    if (!stmt || !DOHSQL_CLIENT)
        return;

    conn = clnt->plugin.state;

    pthread_mutex_lock(&conn->mtx);

    /* wait if run ended ok, master is not done, and there are cached rows */
    if (!clnt->query_rc) {
        while (conn->status == DOH_RUNNING && queue_count(conn->que) > 0) {
            pthread_mutex_unlock(&conn->mtx);
            poll(NULL, 0, 10);
            pthread_mutex_lock(&conn->mtx);
        }
    }

    trimQue(stmt, conn->que, 0);
    trimQue(stmt, conn->que_free, 0);

    conn->status = DOH_CLIENT_DONE;

    pthread_mutex_unlock(&conn->mtx);
}

const char *dohsql_get_sql(struct sqlclntstate *clnt, int index)
{
    return clnt->conns->conns[index].clnt->sql;
}

int comdb2_register_limit(int iLimit, int iSavedLimit)
{
    GET_CLNT;
    if (unlikely(clnt->conns)) {
        clnt->conns->limitRegs[ILIMIT_SAVED_MEM_IDX] = iSavedLimit;
        clnt->conns->limitRegs[ILIMIT_MEM_IDX] = iLimit;
        if (verbose)
            logmsg(LOGMSG_DEBUG, "%lx setting saved limit to %d limit is %d\n",
                   pthread_self(), iSavedLimit, iLimit);
        return 1;
    }
    return 0;
}

void comdb2_register_offset(int iOffset, int iLimitOffset, int iSavedOffset)
{
    GET_CLNT;
    if (likely(clnt->conns)) {
        clnt->conns->limitRegs[IOFFSET_SAVED_MEM_IDX] = iSavedOffset;
        clnt->conns->limitRegs[IOFFSETLIMIT_MEM_IDX] = iLimitOffset;
        clnt->conns->limitRegs[IOFFSET_MEM_IDX] = iOffset;
        if (verbose)
            logmsg(LOGMSG_DEBUG,
                   "%lx setting saved offset to %d offset is %d\n",
                   pthread_self(), iSavedOffset, iOffset);
    }
}

#define DOHSQL_MASTER                                                          \
    (clnt->plugin.next_row == dohsql_dist_next_row ||                          \
     clnt->plugin.next_row == dohsql_dist_next_row_ordered)

void comdb2_handle_limit(Vdbe *v, Mem *m)
{
    GET_CLNT;
    dohsql_t *conns = clnt->conns;
    Mem *reg;

    if (!DOHSQL_MASTER)
        return;

    /* limit or offset ? */
    if (m == &v->aMem[conns->limitRegs[ILIMIT_MEM_IDX]]) {
        reg = &v->aMem[conns->limitRegs[ILIMIT_SAVED_MEM_IDX]];
        /* limit */
        conns->limit = sqlite3_value_int64(reg);
        if (verbose)
            logmsg(LOGMSG_DEBUG, "%lx found limit %d from register %d\n",
                   pthread_self(), conns->limit, conns->limitRegs[0]);
    } else {
        if (m == &v->aMem[conns->limitRegs[IOFFSET_MEM_IDX]]) {
            reg = &v->aMem[conns->limitRegs[IOFFSET_SAVED_MEM_IDX]];
            /* offset */
            conns->offset = sqlite3_value_int64(reg);

            if (verbose)
                logmsg(LOGMSG_DEBUG,
                       "%lx found offset %d from register %d, adjusting limit "
                       "to %d"
                       " updating internal offset mems to %lld->0 and "
                       "%lld->%lld\n",
                       pthread_self(), conns->offset, conns->limitRegs[1],
                       (conns->limit < 0) ? conns->limit
                                          : (conns->limit - conns->offset),
                       v->aMem[conns->limitRegs[IOFFSET_MEM_IDX]].u.i,
                       v->aMem[conns->limitRegs[IOFFSETLIMIT_MEM_IDX]].u.i,
                       v->aMem[conns->limitRegs[IOFFSETLIMIT_MEM_IDX]].u.i -
                           conns->offset);
            if (conns->limit >= 0 && conns->offset > 0)
                conns->limit = conns->limit - conns->offset;
            /* nuke the offset, maybe do this only for order?*/
            v->aMem[conns->limitRegs[IOFFSET_MEM_IDX]].u.i = 0;
            if (conns->offset > 0)
                v->aMem[conns->limitRegs[IOFFSETLIMIT_MEM_IDX]].u.i -=
                    conns->offset;
        } else
            abort(); /*sqlite changed assumptions*/
    }
}

#define Q_LOCK(x) pthread_mutex_lock(&conns->conns[x].mtx)
#define Q_UNLOCK(x) pthread_mutex_unlock(&conns->conns[x].mtx)

static int _cmp(dohsql_t *conns, int idx_a, int idx_b)
{
    int *order = conns->order;
    row_t *a, *b;
    int i;
    int ret;
    int qc_a, qc_b;

    if (idx_a == idx_b)
        return 0;

    /* note idx_a is the referrence, and it is locked here */
    Q_LOCK(order[idx_b]);

    qc_a = queue_count(conns->conns[order[idx_a]].que);
    qc_b = queue_count(conns->conns[order[idx_b]].que);
    if (qc_a == 0) {
        if (qc_b == 0) {
            ret = 0;
        } else {
            ret = -1;
        }
    } else if (qc_b == 0) {
        ret = 1;
    } else {
        a = conns->conns[order[idx_a]].que->lst.top->obj;
        b = conns->conns[order[idx_b]].que->lst.top->obj;

        for (i = 0; i < conns->ncols; i++) {
            ret = sqlite3MemCompare(&a[i], &b[i], NULL);
            if (ret)
                break;
        }
    }

    Q_UNLOCK(order[idx_b]);

    return ret;
}

int _pos(dohsql_t *conns, int index)
{
    int left;
    int right;
    int pivot;
    /*
        if (conns->filling == 0)
            return conns->top_idx;
    */
    left = conns->top_idx;
    right = (conns->top_idx + conns->filling /*-1*/) % conns->nconns;

    if (left > right) {
        if (_cmp(conns, index, conns->nconns - 1) < 0) {
            right = conns->nconns - 1;
        } else {
            left = 0;
        }
    }

    while (left < right) {
        pivot = left + (right - left) / 2;
        if (_cmp(conns, index, pivot) < 0) {
            right = pivot;
            continue;
        }
        left = pivot + 1;
    }

    return left;
}

static void _print_order_info(dohsql_t *conns, const char *label)
{
    int *order = conns->order;
    int i;

    if (!verbose)
        return;

    logmsg(LOGMSG_DEBUG,
           "%lx Order %s: top_idx=%d filling=%d active=%d nconns=%d\n[",
           pthread_self(), label, conns->top_idx, conns->filling, conns->active,
           conns->nconns);
    for (i = 0; i < conns->nconns; i++) {
        logmsg(LOGMSG_DEBUG, "(%d, %d, %d) ", order[i],
               (order[i] >= 0) ? conns->conns[order[i]].rc : -1,
               (order[i] >= 0) ? queue_count(conns->conns[order[i]].que) : -1);
    }
    logmsg(LOGMSG_DEBUG, "]\n");
}

static int q_top(dohsql_t *conns)
{
    int *order = conns->order;
    int ret = conns->top_idx;
    int ret_val = order[ret];
    int last;

    assert(conns->filling > 0);

    conns->top_idx = (conns->top_idx + 1) % conns->nconns;
    conns->filling--;

    if (conns->active < conns->nconns) {
        last = (conns->top_idx + conns->active - 1) % conns->nconns;
        order[last] = order[ret];
        order[ret] = -1;
    }
    if (verbose)
        _print_order_info(conns, "retrieved_ordered_row");

    return ret_val;
}

static int q_insert(dohsql_t *conns, int indx)
{
    int *order = conns->order;
    int pos = _pos(conns, indx);
    int last = (conns->top_idx + conns->filling) % conns->nconns;
    int saved = order[indx];

    if (pos > last) {
        if (last > 0) {
            memmove(&order[1], &order[0], sizeof(order[0]) * last);
        }
        last = conns->nconns - 1;
        order[0] = order[last];
    }
    if (pos < last) {
        memmove(&order[pos + 1], &order[pos], sizeof(order[0]) * (last - pos));
    }
    conns->order[pos] = saved;
    conns->filling++;

    return SHARD_NOERR;
}

static void _move_client_done(dohsql_t *conns, int idx)
{
    int *order = conns->order;
    int last;

    last = (conns->top_idx + conns->active - 1) % conns->nconns;
    if (verbose)
        logmsg(LOGMSG_DEBUG, "%lx %s: client %d done\n", pthread_self(),
               __func__, order[idx]);
    if (last != idx)
        order[idx] = order[last];
    order[last] = -1;
    conns->active--;

    if (verbose)
        _print_order_info(conns, "client_done");
}

static void _move_client_row(dohsql_t *conns, int idx)
{
    int *order = conns->order;
    int last, tmp;
    /* flip to the non-ready pole-position */
    last = (conns->top_idx + conns->filling) % conns->nconns;
    if (last != idx) {
        tmp = order[idx];
        order[idx] = order[last];
        order[last] = tmp;
        idx = last;
    }
    q_insert(conns, idx);

    if (verbose)
        _print_order_info(conns, "insert_new_row");
}

static int _local_step(struct sqlclntstate *clnt, sqlite3_stmt *stmt,
                       int crt_idx)
{
    dohsql_t *conns = clnt->conns;

    conns->conns[0].rc = init_next_row(clnt, stmt);
    if (conns->conns[0].rc == SQLITE_ROW) {
        queue_add(conns->conns[0].que, ((Vdbe *)stmt)->pResultSet);
        _move_client_row(conns, crt_idx);
    } else if (conns->conns[0].rc == SQLITE_DONE) {
        _move_client_done(conns, crt_idx);
    } else {
        return conns->conns[0].rc;
    }
    return SQLITE_OK;
}

static int dohsql_dist_next_row_ordered(struct sqlclntstate *clnt,
                                        sqlite3_stmt *stmt)
{
    dohsql_t *conns = clnt->conns;
    int *order = conns->order;
    int i;
    int idx;
    int found;
    row_t *ret_row;
    int rc = SQLITE_OK;
    int que_idx;

    if (verbose)
        logmsg(LOGMSG_DEBUG, "%lx %s: start\n", pthread_self(), __func__);
    if (conns->nrows == 0) {
        rc = _local_step(clnt, stmt, 0);
        if (rc != SQLITE_OK)
            return rc;
    }

    /* fast path: all channels ready, get the top; if there is
       another row ready, order that, return */
retry_row:
    if (conns->active == 0)
        return SQLITE_DONE;

    if (conns->filling == conns->active) {
        /* merge-sort primed */
        rc = _check_limit(stmt, conns);
        if (rc != SQLITE_OK)
            return rc;

        /* get the top */
        found = q_top(conns);

        Q_LOCK(found);
        ret_row = queue_next(conns->conns[found].que);
        Q_UNLOCK(found);

        if (verbose)
            logmsg(LOGMSG_DEBUG, "Retrieved client %d row %p\n", found,
                   ret_row);

        add_row(conns, found, ret_row);

        rc = _check_offset(conns);
        if (rc != SQLITE_ROW) {
            goto retry_row;
        }

        conns->nrows++;

        return SQLITE_ROW;
    }

    /* slow path: go through all the channels, if some are not ready,
       check to see if they have rows ready or done, if rows,
       order them */
    assert(conns->active > conns->filling);

    int unready = conns->active - conns->filling;
    int unready_start = conns->filling;

    for (i = 0; i < unready && rc == SQLITE_OK; i++) {
        idx = (conns->top_idx + unready_start + i) % conns->nconns;
        que_idx = order[idx];
        assert(que_idx >= 0);

        if (que_idx) {
            Q_LOCK(que_idx);
            if (queue_count(conns->conns[que_idx].que) > 0) {
                _move_client_row(conns, idx);
            } else {
                if (conns->conns[que_idx].rc == SQLITE_DONE) {
                    _move_client_done(conns, idx);
                    /* above moves the location of done client at the end,
                    i points to the next row*/
                    i--;
                    unready--;
                } else if (conns->conns[que_idx].rc != SQLITE_ROW) {
                    rc = conns->conns[que_idx].rc;
                    continue;
                }
            }
            Q_UNLOCK(que_idx);
        } else {
            rc = _local_step(clnt, stmt, idx);
            if (rc != SQLITE_OK) {
                continue;
            }
        }
    }
    if (rc != SQLITE_OK)
        return rc;
    goto retry_row;
}

int order_init(dohsql_t *conns)
{
    int i;
    conns->order = (int *)calloc(sizeof(int), conns->nconns);

    if (!conns->order)
        return SHARD_ERR_MALLOC;

    conns->active = conns->nconns;
    conns->filling = 0;
    conns->top_idx = 0;
    for (i = 0; i < conns->active; i++) {
        conns->order[i] = i;
    }

    return SHARD_NOERR;
}

int is_parallel_shard(void)
{
    GET_CLNT;
    return (clnt->conns || DOHSQL_CLIENT);
}
