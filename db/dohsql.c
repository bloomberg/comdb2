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
#include "reqlog.h"
#include "dohsql.h"
#include "sqlinterfaces.h"
#include "memcompare.c"
#include "string_ref.h"

extern char *print_mem(Mem *m);

int gbl_dohsql_disable = 0;
int gbl_dohsql_verbose = 0;
int gbl_dohsql_max_queued_kb_highwm = 10000;    /* 10 MB */
int gbl_dohsql_full_queue_poll_msec = 10;       /* 10msec */
int gbl_dohsql_max_threads = 8; /* do not run more than 8 threads */
int gbl_dohsql_pool_thr_slack = 24; /* half default sqlengine pool maxthds */
/* for now we keep this tunning "private */
static int gbl_dohsql_track_stats = 1;
static int gbl_dohsql_que_free_highwm = 10;
static int gbl_dohsql_que_free_lowwm = 5;
static int gbl_dohsql_max_queued_kb_lowwm = 1000; /* 1 GB */

struct col {
    int type;
    char *name;
};
typedef struct col my_col_t;

enum doh_status { DOH_RUNNING = 0, DOH_MASTER_DONE = 1, DOH_CLIENT_DONE = 2 };

struct dohsql_connector_stats {
    int max_queue_len;         /* over the execution time */
    int max_free_queue_len;    /* -- " -- */
    long long max_queue_bytes; /* -- " -- */
};
typedef struct dohsql_connector_stats dohsql_connector_stats_t;

struct dohsql_connector {
    struct sqlclntstate *clnt;
    queue_type *que;      /* queue to caller */
    queue_type *que_free; /* de-queued rows come here to be freed */
    int selected;        /* true if a row from this engine is being used by
                             coordinator */
    pthread_mutex_t mtx;  /* mutex for queueing operations and related counts */
    char *thr_where;      /* cached where status */
    my_col_t *cols;       /* cached cols values */
    int ncols;            /* number of columns */
    int rc;
    int nrows;              /* current total queued rows */
    enum doh_status status; /* caller is done */
    long long queue_size;   /* size of queue in bytes */
    int nparams;            /* parameters for the child */
    struct param_data *params;
    dohsql_connector_stats_t stats;
};
typedef struct dohsql_connector dohsql_connector_t;

struct row {
    char *packed;  /* allocated&freed by child thread */
    Mem *unpacked; /* allocated&freed by coordinator thread */
    long long row_size;
};
typedef struct row row_t;

enum {
    ILIMIT_MEM_IDX = 0,
    ILIMIT_SAVED_MEM_IDX = 1,
    IOFFSET_MEM_IDX = 2,
    IOFFSETLIMIT_MEM_IDX = 3,
    IOFFSET_SAVED_MEM_IDX = 4,
    MAX_MEM_IDX = 5
};

struct dohsql_req_stats {
    int max_queue_len;         /* among all shards */
    int max_free_queue_len;    /* -- " -- */
    long long max_queue_bytes; /* -- " -- */
};
typedef struct dohsql_req_stats dohsql_req_stats_t;

struct dohsql {
    int nconns;
    dohsql_connector_t *conns;
    my_col_t *cols;
    int ncols;
    row_t *row;
    int row_src;
    int rc;
    int child_err; /* if a child error occurred, which one?*/
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
    int order_size;
    int *order_dir;
    int nparams;
    /* stats */
    dohsql_req_stats_t stats;
};

struct dohsql_stats {
    long long num_reqs;
    int max_distribution;
    int max_queue_len;
    int max_free_queue_len;
    long long max_queue_bytes;
};
typedef struct dohsql_stats dohsql_stats_t;

pthread_mutex_t dohsql_stats_mtx = PTHREAD_MUTEX_INITIALIZER;
dohsql_stats_t gbl_dohsql_stats;       /* updated only on request completion */
dohsql_stats_t gbl_dohsql_stats_dirty; /* updated dynamically, unlocked */

static int gbl_plugin_api_debug = 0;

static void sqlengine_work_shard_pp(struct thdpool *pool, void *work,
                                    void *thddata, int op);
static void sqlengine_work_shard(struct thdpool *pool, void *work,
                                 void *thddata);
static int order_init(dohsql_t *conns, dohsql_node_t *node);
static int dohsql_dist_next_row_ordered(struct sqlclntstate *clnt,
                                        sqlite3_stmt *stmt);
static int _param_index(dohsql_connector_t *conn, const char *b, int64_t *c);
static int _param_value(dohsql_connector_t *conn, struct param_data *b, int c,
                        const char *src);

static void sqlengine_work_shard_pp(struct thdpool *pool, void *work,
                                    void *thddata, int op)
{
    struct sqlclntstate *clnt = work;
    switch (op) {
    case THD_RUN:
        sqlengine_work_shard(pool, work, thddata);
        break;
    case THD_FREE:
        /* error, we are done */
        clnt->query_rc = -1;
        signal_clnt_as_done(clnt);
        break;
    }
}

void handle_child_error(struct sqlclntstate *clnt, int errcode)
{
    dohsql_connector_t *conn = (dohsql_connector_t *)clnt->plugin.state;

    if (conn) {
        Pthread_mutex_lock(&conn->mtx);
        conn->rc = -1;
        Pthread_mutex_unlock(&conn->mtx);
    }
}

static void _mark_shard_done(dohsql_connector_t *conn)
{
    Pthread_mutex_lock (&conn->mtx);
    conn->status = DOH_CLIENT_DONE;
    Pthread_mutex_unlock (&conn->mtx);
}

static void sqlengine_work_shard(struct thdpool *pool, void *work,
                                 void *thddata)
{
    struct sqlthdstate *thd = thddata;
    struct sqlclntstate *clnt = (struct sqlclntstate *)work;
    int rc;

    thr_set_user("shard thread", (intptr_t) clnt->appsock_id);

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
        if (gbl_dohsql_verbose)
            logmsg(LOGMSG_USER, "XXX: client %p returned error %d\n",
                   clnt->plugin.state, clnt->query_rc);
        handle_child_error(clnt, clnt->query_rc);
    }

    /* clear the child clnt from this sql thread */
    if (thd->sqlthd) {
        Pthread_mutex_lock(&gbl_sql_lock);
        thd->sqlthd->clnt = NULL;
        Pthread_mutex_unlock(&gbl_sql_lock);
    }

    if (put_curtran(thedb->bdb_env, clnt)) {
        logmsg(LOGMSG_ERROR, "%s: unable to destroy a CURSOR transaction!\n",
               __func__);
    }

    clnt->osql.timings.query_finished = osql_log_time();
    osql_log_time_done(clnt);

    /*thrman_setid(thrman_self(), "[done]");*/

    /* after this clnt is toast */
    _mark_shard_done(clnt->plugin.state);
}

static int inner_columns(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    dohsql_connector_t *conn = (dohsql_connector_t *)clnt->plugin.state;
    int ncols, i;

    ncols = sqlite3_column_count(stmt);

    if (!conn->cols || conn->ncols < ncols) {
        conn->cols = (my_col_t *)realloc(conn->cols, ncols * sizeof(my_col_t));
        if (!conn->cols)
            return -1;
    }
    conn->ncols = ncols;

    for (i = 0; i < ncols; i++) {
        conn->cols[i].type = get_sqlite3_column_type(clnt, stmt, i, 0);
    }
    return 0;
}

/* run by the child thread */
static void trimQue(dohsql_connector_t *conn, sqlite3_stmt *stmt,
                    queue_type *que, int limit)
{
    row_t *row;
    long long row_size;

    while (queue_count(que) > limit) {
        row = queue_next(que);
        assert(row->unpacked == NULL);
        row_size = row->row_size;
        if (gbl_dohsql_verbose)
            logmsg(LOGMSG_USER,
                   "%p XXX: conn %p %s %p freed older row size %lld limit %d\n",
                   (void *)pthread_self(), conn,
                   que == conn->que ? "que" : "que_free", que, row_size, limit);
        sqlite3_free(row->packed);
        free(row);

        if (gbl_dohsql_max_queued_kb_highwm) {
            conn->queue_size -= row_size;
        }
    }
}

/* locked */
static void _track_que_free(dohsql_connector_t *conn)
{
    if (unlikely(!gbl_dohsql_track_stats))
        return;

    int len = queue_count(conn->que_free);
    if (conn->stats.max_free_queue_len < len) {
        conn->stats.max_free_queue_len = len;
        if (gbl_dohsql_stats_dirty.max_free_queue_len < len)
            gbl_dohsql_stats_dirty.max_free_queue_len = len;
    }
}

/* conn is locked */
static void _que_limiter(dohsql_connector_t *conn, sqlite3_stmt *stmt,
                         int row_size)
{
    int rc;
    int lowwm = gbl_dohsql_que_free_lowwm;

    if (gbl_dohsql_max_queued_kb_highwm) {
        conn->queue_size += row_size;
    }

    /* inline cleanup */
    if (queue_count(conn->que_free) > gbl_dohsql_que_free_highwm) {
    cleanup:
        _track_que_free(conn);
        trimQue(conn, stmt, conn->que_free, lowwm);
    }

    if (gbl_dohsql_max_queued_kb_highwm) {
        if (conn->queue_size > gbl_dohsql_max_queued_kb_highwm * 1000) {
            if ((conn->queue_size > gbl_dohsql_max_queued_kb_lowwm * 1000) &&
                conn->status != DOH_MASTER_DONE) {
                Pthread_mutex_unlock(&conn->mtx);
                poll(NULL, 0, gbl_dohsql_full_queue_poll_msec);
                if (bdb_lock_desired(thedb->bdb_env)) {
                    rc = recover_deadlock_simple(thedb->bdb_env);
                    if (rc) {
                        Pthread_mutex_lock(&conn->mtx);
                        logmsg(LOGMSG_ERROR,
                               "%s: failed recover_deadlock rc=%d\n", __func__,
                               rc);
                        return;
                    }
                }
                Pthread_mutex_lock(&conn->mtx);
                /* reduce the low watermark, maybe some cached rows are big
                enough to take over the whole quota */
                if (lowwm > 0)
                    lowwm--;
                goto cleanup;
            }
        }
    }
    if (likely(gbl_dohsql_track_stats)) {
        int tmp = queue_count(conn->que);
        if (conn->stats.max_queue_len < tmp) {
            conn->stats.max_queue_len = tmp;
            if (gbl_dohsql_stats_dirty.max_queue_len < tmp)
                gbl_dohsql_stats_dirty.max_queue_len = tmp;
        }
        if (conn->stats.max_queue_bytes < conn->queue_size) {
            conn->stats.max_queue_bytes = conn->queue_size;
            if (gbl_dohsql_stats_dirty.max_queue_bytes < conn->queue_size)
                gbl_dohsql_stats_dirty.max_queue_bytes = conn->queue_size;
        }
    }
}

static int inner_error(struct sqlclntstate *clnt, int rc, char *errstr)
{
    dohsql_connector_t *conn = (dohsql_connector_t *)clnt->plugin.state;

    Pthread_mutex_lock(&conn->mtx);
    clnt->saved_rc = rc;
    clnt->saved_errstr = strdup(errstr);
    conn->rc = rc;
    Pthread_mutex_unlock(&conn->mtx);

    return 0;
}

static int inner_row(struct sqlclntstate *clnt, struct response_data *resp,
                     int postpone)
{
    dohsql_connector_t *conn = (dohsql_connector_t *)clnt->plugin.state;
    sqlite3_stmt *stmt = resp->stmt;

    row_t *row;

    Pthread_mutex_lock(&conn->mtx);
    if (conn->status == DOH_MASTER_DONE) {
        if (gbl_dohsql_verbose)
            logmsg(LOGMSG_USER, "%p %s master done q %d qf %d\n",
                   (void *)pthread_self(), __func__, queue_count(conn->que),
                   queue_count(conn->que_free));
        /* work is done, need to clean-up */
        _track_que_free(conn);
        trimQue(conn, stmt, conn->que, 0);
        trimQue(conn, stmt, conn->que_free, 0);

        conn->rc = SQLITE_DONE; /* signal master this is clear */

        Pthread_mutex_unlock(&conn->mtx);

        return SQLITE_DONE; /* any != 0 will do, this impersonates a normal end
                             */
    }
    Pthread_mutex_unlock(&conn->mtx);

    row = calloc(1, sizeof(row_t));
    if (!row)
        return SHARD_ERR_GENERIC;

    row->packed = sqlite3PackedResult(stmt, &row->row_size);
    if (gbl_dohsql_verbose)
        logmsg(LOGMSG_USER, "%p: XXX: packed result %p in %p size %lld\n",
               (void *)pthread_self(), row->packed, row, row->row_size);

    if (!row->packed) {
        free(row);
        return SHARD_ERR_GENERIC;
    }

    Pthread_mutex_lock(&conn->mtx);
    conn->rc = SQLITE_ROW;
    if (queue_add(conn->que, row))
        abort();

    if (gbl_dohsql_verbose)
        logmsg(LOGMSG_USER,
               "%p XXX: added new row conn %p que %p que_free %p\n",
               (void *)pthread_self(), conn, conn->que, conn->que_free);

    _que_limiter(conn, stmt, row->row_size);

    Pthread_mutex_unlock(&conn->mtx);

    return SHARD_NOERR;
}

static int inner_row_last(struct sqlclntstate *clnt)
{
    dohsql_connector_t *conn = (dohsql_connector_t *)clnt->plugin.state;

    Pthread_mutex_lock(&conn->mtx);
    conn->rc = SQLITE_DONE;
    Pthread_mutex_unlock(&conn->mtx);

    return SHARD_NOERR;
}

/* override sqlite engine */
static int dohsql_dist_column_count(struct sqlclntstate *clnt, sqlite3_stmt *_)
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
        if (!conns->row->unpacked) {                                           \
            conns->row->unpacked = sqlite3UnpackedResult(                      \
                stmt, conns->ncols, conns->row->packed, conns->row->row_size); \
        }                                                                      \
        return sqlite3_value_##type(&conns->row->unpacked[iCol]);              \
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

    if (!conns->row->unpacked) {
        conns->row->unpacked = sqlite3UnpackedResult(
            stmt, conns->ncols, conns->row->packed, conns->row->row_size);
    }
    return sqlite3_value_interval(&conns->row->unpacked[iCol], type);
}

static sqlite3_value *dohsql_dist_column_value(struct sqlclntstate *clnt,
                                               sqlite3_stmt *stmt, int i)
{
    dohsql_t *conns = clnt->conns;

    if (conns->row_src == 0)
        return sqlite3_column_value(stmt, i);

    if (!conns->row->unpacked) {
        conns->row->unpacked = sqlite3UnpackedResult(
            stmt, conns->ncols, conns->row->packed, conns->row->row_size);
    }
    return &conns->row->unpacked[i];
}

#define Q_LOCK(x) Pthread_mutex_lock(&conns->conns[x].mtx)
#define Q_UNLOCK(x) Pthread_mutex_unlock(&conns->conns[x].mtx)

static int dohsql_dist_sqlite_error(struct sqlclntstate *clnt,
                                    sqlite3_stmt *stmt, const char **errstr)
{
    dohsql_t *conns = clnt->conns;
    int errcode;
    int src = conns->row_src;

    if (src == 0) {
        return sqlite_stmt_error(stmt, errstr);
    }

    Q_LOCK(src);

    *errstr = NULL;
    errcode = conns->conns[src].rc;

    if (errcode != SQLITE_ROW && errcode != SQLITE_DONE)
        *errstr = (const char *)conns->row;

    Q_UNLOCK(src);

    return errcode;
}

static int dohsql_dist_param_count(struct sqlclntstate *clnt)
{
    return clnt->conns->conns[0].nparams;
}

static int dohsql_dist_param_index(struct sqlclntstate *clnt, const char *name,
                                   int64_t *index)
{
    /* coordinator param subset */
    return _param_index(&clnt->conns->conns[0], name, index);
}

static int dohsql_dist_param_value(struct sqlclntstate *clnt,
                                   struct param_data *param, int n)
{
    /* coordinator param subset */
    return _param_value(&clnt->conns->conns[0], param, n, __func__);
}

static void donate_current_row(dohsql_t *conns, int locked)
{
    if (conns->row) {
        if (conns->row_src) {
            /* free what coordinator allocated before sending the row back */
            if (conns->row->unpacked) {
                sqlite3UnpackedResultFree(&conns->row->unpacked, conns->ncols);
            }

            if (gbl_dohsql_verbose)
                logmsg(LOGMSG_USER, "%p %s donating current row %p src %d\n",
                       (void *)pthread_self(), __func__, conns->row,
                       conns->row_src);
            if (!locked)
                Q_LOCK(conns->row_src);
            if (queue_add(conns->conns[conns->row_src].que_free, conns->row))
                abort();
            conns->conns[conns->row_src].selected = 0;
            if (!locked)
                Q_UNLOCK(conns->row_src);
            conns->row = NULL;
            conns->row_src = 0;
        } else {
            /* local row; there is no use of que_free, here, but we still need
             * to free the dummy row_t that points to the Vdbe->pResultSet
             */
            free(conns->row);
            conns->row = NULL;
        }
    }
}

static void add_row(dohsql_t *conns, int i, row_t *row)
{
    /* put the used row in the free list */
    donate_current_row(conns, i == conns->row_src);

    /* new row */
    conns->row = row;
    conns->row_src = i;
    conns->conns[conns->row_src].selected = 1;
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
        Q_LOCK(child_num);

        /* is coordinator sitting on a row? */
        if (conns->row && conns->row_src == child_num) {
            donate_current_row(conns, 1);
        }
        if (conns->conns[child_num].que->lst.top) {
            /* make sure we free the Unpacked of top of the queue,
             * which is allocated if we do an ordered select
             */
            row_t *r = conns->conns[child_num].que->lst.top->obj;
            if (r->unpacked) {
                sqlite3UnpackedResultFree(&r->unpacked, conns->ncols);
            }
        }

        if (conns->conns[child_num].status != DOH_CLIENT_DONE) {
            if (gbl_dohsql_verbose)
                logmsg(LOGMSG_USER, "%s: signalling client done, ignoring\n",
                       __func__);
            conns->conns[child_num].status = DOH_MASTER_DONE;
        }
        Q_UNLOCK(child_num);
    }
}

static int _get_a_parallel_row(dohsql_t *conns, row_t **prow, int *error_child)
{
    int child_num;
    int rc = SQLITE_DONE;

    *prow = NULL;

    for (child_num = 1; child_num < conns->nconns && (*prow) == NULL;
         child_num++) {
        Q_LOCK(child_num);
        /* done */
        if (CHILD_DONE(child_num)) {
            Q_UNLOCK(child_num);
            continue;
        }
        /* error */
        if (CHILD_ERROR(child_num)) {
            /* debatable if we wanna clear cached rows before check for error */
            rc = conns->conns[child_num].rc;
            if (error_child)
                *error_child = child_num;
            Q_UNLOCK(child_num);

            /* we could envision a case when child is retried for cut 2*/
            /* for now, signal all children that we are done and pass error
               to caller */
            _signal_children_master_is_done(conns);
            return rc;
        }
        rc = SQLITE_OK;
        *prow = queue_next(conns->conns[child_num].que);
        if (*prow != NULL) {
            if (gbl_dohsql_verbose)
                logmsg(LOGMSG_USER, "%p XXX: %p retrieved row\n",
                       (void *)pthread_self(), &conns->conns[child_num]);
            add_row(conns, child_num, *prow);
            rc = SQLITE_ROW;
        }
        Q_UNLOCK(child_num);
    }

    if (gbl_dohsql_verbose)
        logmsg(LOGMSG_USER, "%p XXX: parallel row rc = %d\n",
               (void *)pthread_self(), rc);

    if (rc == SQLITE_OK) {
        /* none of the children had any rows; donate the row we
        are sitting on to be freed, if any */
        donate_current_row(conns, 0);
    }

    return rc;
}

static int init_next_row(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    dohsql_t *conns = clnt->conns;
    int rc;

    rc = sqlite3_maybe_step(clnt, stmt);

    if (gbl_dohsql_verbose) {
        logmsg(LOGMSG_USER, "%p %s: sqlite3_maybe_step rc %d\n",
               (void *)pthread_self(), __func__, rc);
        if (conns->limitRegs[ILIMIT_SAVED_MEM_IDX] > 0)
            logmsg(LOGMSG_USER,
                   "%p clnt %p conns %p limitMem %d:%d limit %d "
                   "offsetMem %d:%d offset %d\n",
                   (void *)pthread_self(), clnt, conns,
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
        if (gbl_dohsql_verbose)
            logmsg(LOGMSG_USER, "%p REACHED LIMIT rc =%d!\n",
                   (void *)pthread_self(), conns->conns[0].rc);
        if (conns->conns[0].rc != SQLITE_DONE) {
            if (gbl_dohsql_verbose)
                logmsg(LOGMSG_USER, "RESET STMT!\n");
            /* here we already send some rows, and columns, so it is safe to
             * reset */
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
        if (gbl_dohsql_verbose)
            logmsg(LOGMSG_USER,
                   "XXX: skipped client %d row skipped %d, offset =%d\n",
                   conns->row_src, conns->skipped, conns->offset);
        /* skip it */
        return SQLITE_OK;
    }

    if (gbl_dohsql_verbose)
        logmsg(LOGMSG_USER, "%p XXX: returned source %d row\n",
               (void *)pthread_self(), conns->row_src);
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

    if (gbl_dohsql_verbose)
        logmsg(LOGMSG_USER, "%p %s: start\n", (void *)pthread_self(), __func__);
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
    rc = _get_a_parallel_row(conns, &row, &conns->child_err);
    if (rc == SQLITE_ROW) {
        assert(row);
        goto got_row;
    }
    if (rc == SQLITE_OK)
        empty = 0;
    else if (rc != SQLITE_DONE) {

        /* it seems some shard failed, reset current since we are bailing out */
        if (conns->conns[0].rc != SQLITE_DONE) {
            if (gbl_dohsql_verbose)
                logmsg(LOGMSG_USER, "%s Child %d return error %d!\n", __func__,
                       conns->child_err, rc);
            /* we cannot reset stmt here since caller will need that to send
            back columns, if this is the first row; send proper rc so we reset
            stmt in caller */
            return SQLITE_EARLYSTOP_DOHSQL;
        }

        return rc;
    }

    /* no row in others (yet) */
    if (conns->conns[0].rc != SQLITE_DONE) {
        rc = sqlite3_maybe_step(clnt, stmt);

        if (gbl_dohsql_verbose)
            logmsg(LOGMSG_USER, "%p %s: rc =%d\n", (void *)pthread_self(),
                   __func__, rc);

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
        if (bdb_lock_desired(thedb->bdb_env)) {
            rc = recover_deadlock_simple(thedb->bdb_env);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: failed recover_deadlock rc=%d\n",
                       __func__, rc);
                return rc;
            }
        }

        /* did client disconnect? */
        if (check_sql_client_disconnect(clnt, __FILE__, __LINE__)) {
            _signal_children_master_is_done(conns);
            return SQLITE_EARLYSTOP_DOHSQL;
        }

        goto wait_for_others;
    }

    donate_current_row(conns, 0);

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
        logmsg(LOGMSG_WARN, "%p %s type %d code %d\n", (void *)pthread_self(),
               __func__, t, i);
    switch (t) {
    case RESPONSE_COLUMNS:
        return inner_columns(c, a);
    case RESPONSE_COLUMNS_STR:
        return 0 /*newsql_columns_str(c, a, i)*/;
    case RESPONSE_DEBUG:
        return 0 /*newsql_debug(c, a)*/;
    case RESPONSE_ERROR:
        return inner_error(c, i, (char *)a);
    case RESPONSE_ERROR_ACCESS:
        return inner_error(c, CDB2ERR_ACCESS, (char *)a);
    case RESPONSE_ERROR_BAD_STATE:
        return inner_error(c, CDB2ERR_BADSTATE, (char *)a);
    case RESPONSE_ERROR_PREPARE:
        return inner_error(c, CDB2ERR_PREPARE_ERROR, (char *)a);
    case RESPONSE_ERROR_REJECT:
        return inner_error(c, CDB2ERR_REJECTED, (char *)a);
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
    case RESPONSE_COLUMNS_LUA:
        return 0;
    default:
        logmsg(LOGMSG_ERROR, "Unsupported option %d\n", t);
        abort();
    }
    return 0;
}

static int dohsql_param_count(struct sqlclntstate *c)
{
    dohsql_connector_t *conn = (dohsql_connector_t *)c->plugin.state;

    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", (void *)pthread_self(), __func__);

    return conn->nparams;
}

static int _param_index(dohsql_connector_t *conn, const char *b, int64_t *c)
{
    int i;
    for (i = 0; i < conn->nparams; i++) {
        if (!strcmp(b, conn->params[i].name)) {
            *c = conn->params[i].pos;
            return 0;
        }
    }
    return -1;
}

static int dohsql_param_index(struct sqlclntstate *a, const char *b, int64_t *c)
{
    dohsql_connector_t *conn = (dohsql_connector_t *)a->plugin.state;

    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", (void *)pthread_self(), __func__);

    return _param_index(conn, b, c);
}

static int _param_value(dohsql_connector_t *conn, struct param_data *b, int c,
                        const char *src)
{
    if (c < 0 || c >= conn->nparams)
        return -1;
    *b = conn->params[c];
    if (b->name[0] == '\0') {
        /* these are index based that are renamed; use their index position as
        their identity, matching the sql query generated */
        b->pos = c + 1;
    }
    return 0;
}

static int dohsql_param_value(struct sqlclntstate *a, struct param_data *b,
                              int c)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", (void *)pthread_self(), __func__);

    return _param_value((dohsql_connector_t *)a->plugin.state, b, c, __func__);
}

static void dohsql_setup_client_info(struct sqlclntstate *clnt,
                                     struct sqlthdstate *b, char *c)
{
    dohsql_connector_t *conn = (dohsql_connector_t *)clnt->plugin.state;

    if (conn->thr_where)
        thrman_wheref(thrman_self(), "%s", conn->thr_where);

    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s %s\n", (void *)pthread_self(), __func__,
               (conn->thr_where) ? conn->thr_where : "NULL");
}

static void make_dohsql_plugin(struct sqlclntstate *clnt)
{
    clnt->plugin.write_response = dohsql_write_response;
    clnt->plugin.param_count = dohsql_param_count;
    clnt->plugin.param_index = dohsql_param_index;
    clnt->plugin.param_value = dohsql_param_value;
    clnt->plugin.setup_client_info = dohsql_setup_client_info;
}

static int _shard_connect(struct sqlclntstate *clnt, dohsql_connector_t *conn,
                          const char *sql, int nparams,
                          struct param_data *params)
{
    const char *where = NULL;

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
    Pthread_mutex_init(&conn->mtx, NULL);

    comdb2uuid(conn->clnt->osql.uuid);
    conn->clnt->appsock_id = getarchtid();
    init_sqlclntstate(conn->clnt, (char *)conn->clnt->osql.uuid);
    conn->clnt->origin = clnt->origin;
    conn->clnt->current_user = clnt->current_user;
    conn->clnt->sql = strdup(sql);
    make_dohsql_plugin(conn->clnt);
    conn->clnt->plugin.state = conn;
    where = thrman_get_where(thrman_self());
    conn->thr_where = strdup(where ? where : "");
    conn->nparams = nparams;
    conn->params = params;
    if (gbl_dohsql_verbose) {
        logmsg(LOGMSG_USER, "%p %p saved nparams %d\n", (void *)pthread_self(),
               __func__, conn->nparams);
        for (int i = 0; i < conn->nparams; i++) {
            logmsg(LOGMSG_USER, "%p %p saved params %d name \"%s\" pos %d\n",
                   (void *)pthread_self(), __func__, i, conn->params[i].name,
                   conn->params[i].pos);
        }
    }
    conn->rc = SQLITE_ROW;

    return SHARD_NOERR;
}

extern pthread_mutex_t clnt_lk;

static void _shard_disconnect(dohsql_connector_t *conn)
{
    struct sqlclntstate *clnt = conn->clnt;

    free(conn->thr_where);

    if (gbl_dohsql_verbose)
        logmsg(
            LOGMSG_USER, "%p XXX: %s conn %p destroying que %p que_free %p\n",
            (void *)pthread_self(), __func__, conn, conn->que, conn->que_free);

    queue_free(conn->que);
    queue_free(conn->que_free);
    if (conn->cols)
        free(conn->cols);

    free(conn->params);
    Pthread_mutex_lock(&clnt_lk);
    free(clnt->sql);
    clnt->sql = NULL;
    Pthread_mutex_unlock(&clnt_lk);
    cleanup_clnt(clnt);
    free(clnt);
}

static void _master_clnt_set(struct sqlclntstate *clnt)
{
    assert(clnt->conns);

    clnt->backup = clnt->plugin;

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
    clnt->plugin.sqlite_error = dohsql_dist_sqlite_error;
    clnt->plugin.param_count = dohsql_dist_param_count;
    clnt->plugin.param_value = dohsql_dist_param_value;
    clnt->plugin.param_index = dohsql_dist_param_index;
}


static void _save_params(dohsql_node_t *node, struct param_data **p, int *np)
{
    *p = NULL;
    *np = 0;
    if (node->params) {
        *np = node->params->nparams;
        *p = node->params->params;
        free(node->params);
        node->params = NULL;
    }
}

/*
   make sure at least one engine runs no coordinators; this makes
   sure that we make progress even when the pool is full, allowing at
   least N engines to run shards/non-parallel queries
*/
static int parallel_load = 0;
pthread_mutex_t parallel_load_mtx = PTHREAD_MUTEX_INITIALIZER;

static int _add_parallel_load(struct sqlclntstate *clnt)
{
    static int alert_parallel_disabled = 0;
    struct thdpool *pool = get_sql_pool(clnt);
    int maxt = thdpool_get_maxthds(pool);
    int thr_slack = (gbl_dohsql_pool_thr_slack > 0) ? gbl_dohsql_pool_thr_slack : 1;

    Pthread_mutex_lock(&parallel_load_mtx);
    if ((maxt - thr_slack) <= parallel_load) {
        if (!alert_parallel_disabled) {
            logmsg(LOGMSG_INFO,
                   "Sql engine full, switching to non-parallel mode\n");
            alert_parallel_disabled = 1;
        }
        Pthread_mutex_unlock(&parallel_load_mtx);
        return SHARD_ERR_LOAD;
    } else {
        if (alert_parallel_disabled) {
            logmsg(LOGMSG_INFO,
                   "Sql engine load cleared, re-activating parallel mode\n");
            alert_parallel_disabled = 0;
        }
    }
    parallel_load++;
    Pthread_mutex_unlock(&parallel_load_mtx);

    return SHARD_NOERR;
}

static void _rem_parallel_load(void)
{
    Pthread_mutex_lock(&parallel_load_mtx);
    parallel_load--;
    Pthread_mutex_unlock(&parallel_load_mtx);
}

int dohsql_distribute(dohsql_node_t *node)
{
    GET_CLNT;
    dohsql_t *conns;
    int i, rc;
    int clnt_nparams;
    int flags = 0;

    if (gbl_dohsql_max_threads && node->nnodes > gbl_dohsql_max_threads) {
        return SHARD_ERR_TOOMANYTHR;
    }

    clnt_nparams = param_count(clnt);
    if (clnt_nparams != node->nparams) {
        return SHARD_ERR_PARAMS;
    }

    rc = _add_parallel_load(clnt);
    if (rc)
        return rc;

    /* setup communication queue */
    conns = (dohsql_t *)calloc(
        1, sizeof(dohsql_t) + node->nnodes * sizeof(dohsql_connector_t));
    if (!conns)
        return SHARD_ERR_MALLOC;
    conns->conns = (dohsql_connector_t *)(conns + 1);
    conns->nconns = node->nnodes;
    conns->ncols = node->ncols;
    conns->nparams = node->nparams;

    if (node->order_size) {
        if (order_init(conns, node)) {
            free(conns);
            return SHARD_ERR_MALLOC;
        }
        flags = THDPOOL_FORCE_DISPATCH;
    }
    /* there is a slack to allow non-coordinator tasks to drain;
     * it is still possible to fill the sql queue; force the 
     * worker shards on the queue in any case
     */
    flags |= THDPOOL_FORCE_QUEUE;
    clnt->conns = conns;
    /* augment interface */
    _master_clnt_set(clnt);

    /* start peers */
    for (i = 0; i < conns->nconns; i++) {
        struct param_data *params;
        int nparams;
        _save_params(node->nodes[i], &params, &nparams);
        if ((rc = _shard_connect(clnt, &conns->conns[i], node->nodes[i]->sql,
                                 nparams, params)) != 0)
            return rc;

        if (i > 0) {
            struct string_ref *sr = create_string_ref(node->nodes[i]->sql);

            /* launch the new sqlite engine a the next shard */
            rc = thdpool_enqueue(get_sql_pool(clnt), sqlengine_work_shard_pp,
                                 clnt->conns->conns[i].clnt, 1,
                                 sr, flags);
            if (rc) {
                /* this should not fail since we force queue */
                abort();
#if 0
                put_ref(&sr);
                return SHARD_ERR_GENERIC;
#endif
            }
        }
    }

    if (gbl_dohsql_track_stats) {
        gbl_dohsql_stats_dirty.num_reqs++;
        if (gbl_dohsql_stats_dirty.max_distribution < conns->nconns)
            gbl_dohsql_stats_dirty.max_distribution = conns->nconns;

        Pthread_mutex_lock(&dohsql_stats_mtx);
        if (gbl_dohsql_max_queued_kb_lowwm > gbl_dohsql_max_queued_kb_highwm)
            gbl_dohsql_max_queued_kb_lowwm =
                (gbl_dohsql_max_queued_kb_highwm / 2)
                    ? gbl_dohsql_max_queued_kb_highwm / 2
                    : 1;
        Pthread_mutex_unlock(&dohsql_stats_mtx);
    }

    return SHARD_NOERR;
}

int dohsql_end_distribute(struct sqlclntstate *clnt, struct reqlogger *logger)
{
    dohsql_t *conns = clnt->conns;
    int i, rc;

    if (!clnt->conns)
        return SHARD_NOERR;

    donate_current_row(conns, 0);

    for (i = 1; i < conns->nconns; i++) {
        Pthread_mutex_lock(&conns->conns[i].mtx);
        while (conns->conns[i].status != DOH_CLIENT_DONE) {
            Pthread_mutex_unlock(&conns->conns[i].mtx);
            poll(NULL, 0, 10);
            if (bdb_lock_desired(thedb->bdb_env)) {
                rc = recover_deadlock_simple(thedb->bdb_env);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "%s: failed recover_deadlock rc=%d\n",
                           __func__, rc);
                    goto done;
                }
            }
            Pthread_mutex_lock(&conns->conns[i].mtx);
        }
        Pthread_mutex_unlock(&conns->conns[i].mtx);
    }

    for (i = 0; i < conns->nconns; i++) {
        if (likely(gbl_dohsql_track_stats)) {
            if (conns->stats.max_queue_len <
                conns->conns[i].stats.max_queue_len)
                conns->stats.max_queue_len =
                    conns->conns[i].stats.max_queue_len;
            if (conns->stats.max_free_queue_len <
                conns->conns[i].stats.max_free_queue_len)
                conns->stats.max_free_queue_len =
                    conns->conns[i].stats.max_free_queue_len;
            if (conns->stats.max_queue_bytes <
                conns->conns[i].stats.max_queue_bytes)
                conns->stats.max_queue_bytes =
                    conns->conns[i].stats.max_queue_bytes;
            if (logger) {
                reqlog_logf(logger, REQL_INFO,
                            "shard %d max_queue %d max_free_queue %d "
                            "max_queued_bytes %lld\n",
                            i, conns->conns[i].stats.max_queue_len,
                            conns->conns[i].stats.max_free_queue_len,
                            conns->conns[i].stats.max_queue_bytes);
            }
        }
        _shard_disconnect(&conns->conns[i]);
    }
    if (likely(gbl_dohsql_track_stats)) {
        Pthread_mutex_lock(&dohsql_stats_mtx);
        gbl_dohsql_stats.num_reqs++;
        if (gbl_dohsql_stats.max_distribution < conns->nconns)
            gbl_dohsql_stats.max_distribution = conns->nconns;
        if (gbl_dohsql_stats.max_queue_len < conns->stats.max_queue_len)
            gbl_dohsql_stats.max_queue_len = conns->stats.max_queue_len;
        if (gbl_dohsql_stats.max_free_queue_len <
            conns->stats.max_free_queue_len)
            gbl_dohsql_stats.max_free_queue_len =
                conns->stats.max_free_queue_len;
        if (gbl_dohsql_stats.max_queue_bytes < conns->stats.max_queue_bytes)
            gbl_dohsql_stats.max_queue_bytes = conns->stats.max_queue_bytes;
        Pthread_mutex_unlock(&dohsql_stats_mtx);
    }

    if (logger) {
        reqlog_logf(
            logger, REQL_INFO,
            "dist %d max_queue %d max_free_queue %d max_queued_bytes %lld\n",
            conns->nconns, conns->stats.max_queue_len,
            conns->stats.max_free_queue_len, conns->stats.max_queue_bytes);
    }

    if (conns->order) {
        free(conns->order);
        free(conns->order_dir);
    }
    clnt_plugin_reset(clnt);
    clnt->conns = NULL;
    free(conns);

    _rem_parallel_load();
    rc = SHARD_NOERR;

done:
    return rc;
}

#define DOHSQL_CLIENT                                                          \
    (clnt->plugin.state && clnt->plugin.write_response == dohsql_write_response)

void dohsql_wait_for_master(sqlite3_stmt *stmt, struct sqlclntstate *clnt)
{
    int rc;
    dohsql_connector_t *conn;

    if (!stmt || !DOHSQL_CLIENT)
        return;

    if (gbl_dohsql_verbose)
        logmsg(LOGMSG_USER, "%p %s waiting for master\n",
               (void *)pthread_self(), __func__);

    conn = clnt->plugin.state;

    Pthread_mutex_lock(&conn->mtx);

    /* wait if run ended ok, master is not done, and there are cached rows */
    if (!clnt->query_rc) {
        while (conn->status == DOH_RUNNING &&
               (conn->selected || (queue_count(conn->que) > 0))) {
            Pthread_mutex_unlock(&conn->mtx);
            poll(NULL, 0, 10);
            if (bdb_lock_desired(thedb->bdb_env)) {
                rc = recover_deadlock_simple(thedb->bdb_env);
                if (rc) {
                    logmsg(LOGMSG_ERROR,
                           "%s failed recover_deadlock "
                           "rc=%d\n",
                           __func__, rc);
                    return;
                }
            }
            Pthread_mutex_lock(&conn->mtx);
        }
    }

    if (gbl_dohsql_verbose)
        logmsg(LOGMSG_USER,
               "%p %s done waiting que %p count %d que_free %p count %d\n",
               (void *)pthread_self(), __func__, conn->que,
               queue_count(conn->que), conn->que_free,
               queue_count(conn->que_free));

    _track_que_free(conn);
    trimQue(conn, stmt, conn->que, 0);
    trimQue(conn, stmt, conn->que_free, 0);

    /*
        This has to be done after the sql thread has done touching clnt
    structure conn->status = DOH_CLIENT_DONE;
    */

    Pthread_mutex_unlock(&conn->mtx);
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
        if (gbl_dohsql_verbose)
            logmsg(LOGMSG_USER, "%p setting saved limit to %d limit is %d\n",
                   (void *)pthread_self(), iSavedLimit, iLimit);
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
        if (gbl_dohsql_verbose)
            logmsg(LOGMSG_USER, "%p setting saved offset to %d offset is %d\n",
                   (void *)pthread_self(), iSavedOffset, iOffset);
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
        if (gbl_dohsql_verbose)
            logmsg(LOGMSG_USER, "%p found limit %d from register %d\n",
                   (void *)pthread_self(), conns->limit, conns->limitRegs[0]);
    } else {
        if (m == &v->aMem[conns->limitRegs[IOFFSET_MEM_IDX]]) {
            reg = &v->aMem[conns->limitRegs[IOFFSET_SAVED_MEM_IDX]];
            /* offset */
            conns->offset = sqlite3_value_int64(reg);

            if (gbl_dohsql_verbose)
                logmsg(LOGMSG_USER,
                       "%p found offset %d from register %d, adjusting limit "
                       "to %d updating internal offset mems to %lld->0 and "
                       "%lld->%lld\n",
                       (void *)pthread_self(), conns->offset,
                       conns->limitRegs[1],
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

static Mem *_get_mem(sqlite3_stmt *stmt, dohsql_t *conns, int idx)
{
    row_t *r = conns->conns[idx].que->lst.top->obj;
    if (!r->unpacked) {
        assert(idx > 0);
        r->unpacked =
            sqlite3UnpackedResult(stmt, conns->ncols, r->packed, r->row_size);
    }
    return r->unpacked;
}

static int _cmp(sqlite3_stmt *stmt, dohsql_t *conns, int idx_a, int idx_b)
{
    int *order = conns->order;
    Mem *a, *b;
    int i;
    int ret = 0;
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
        a = _get_mem(stmt, conns, order[idx_a]);
        b = _get_mem(stmt, conns, order[idx_b]);

        for (i = 0; i < conns->order_size /*conns->ncols*/; i++) {
            int orderby_idx = (conns->order_dir[i] > 0)
                                  ? conns->order_dir[i]
                                  : (-conns->order_dir[i]);
            assert(orderby_idx > 0);
            orderby_idx--;
            if (gbl_dohsql_verbose) {
                logmsg(LOGMSG_USER, "%p COMPARE %s <> %s\n", (void *)pthread_self(), print_mem(&a[orderby_idx]),
                       print_mem(&b[orderby_idx]));
            }

            ret = sqlite3MemCompare(&a[orderby_idx], &b[orderby_idx], NULL);
            if (ret) {
                if (conns->order_dir[i] < 0)
                    ret = -ret;
                break;
            }
        }
    }

    Q_UNLOCK(order[idx_b]);

    return ret;
}

int _pos(sqlite3_stmt *stmt, dohsql_t *conns, int index)
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
        if (_cmp(stmt, conns, index, conns->nconns - 1) < 0) {
            right = conns->nconns - 1;
        } else {
            left = 0;
        }
    }

    while (left < right) {
        pivot = left + (right - left) / 2;
        if (_cmp(stmt, conns, index, pivot) < 0) {
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

    if (!gbl_dohsql_verbose)
        return;

    logmsg(LOGMSG_USER,
           "%p Order %s: top_idx=%d filling=%d active=%d nconns=%d\n[",
           (void *)pthread_self(), label, conns->top_idx, conns->filling,
           conns->active, conns->nconns);
    for (i = 0; i < conns->nconns; i++) {
        logmsg(LOGMSG_USER, "(%d, %d, %d) ", order[i],
               (order[i] >= 0) ? conns->conns[order[i]].rc : -1,
               (order[i] >= 0) ? queue_count(conns->conns[order[i]].que) : -1);
    }
    logmsg(LOGMSG_USER, "]\n");
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
    if (gbl_dohsql_verbose)
        _print_order_info(conns, "retrieved_ordered_row");

    return ret_val;
}

static int q_insert(sqlite3_stmt *stmt, dohsql_t *conns, int indx)
{
    int *order = conns->order;
    int pos = _pos(stmt, conns, indx);
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
    if (gbl_dohsql_verbose)
        logmsg(LOGMSG_USER, "%p %s: client %d done\n", (void *)pthread_self(),
               __func__, order[idx]);
    if (last != idx)
        order[idx] = order[last];
    order[last] = -1;
    conns->active--;

    if (gbl_dohsql_verbose)
        _print_order_info(conns, "client_done");
}

static void _move_client_row(sqlite3_stmt *stmt, dohsql_t *conns, int idx)
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
    q_insert(stmt, conns, idx);

    if (gbl_dohsql_verbose)
        _print_order_info(conns, "insert_new_row");
}

static int _local_step(struct sqlclntstate *clnt, sqlite3_stmt *stmt,
                       int crt_idx)
{
    dohsql_t *conns = clnt->conns;

    conns->conns[0].rc = init_next_row(clnt, stmt);
    if (conns->conns[0].rc == SQLITE_ROW) {
        if (gbl_dohsql_verbose)
            logmsg(LOGMSG_USER, "%p XXX: %s added local new row\n",
                   (void *)pthread_self(), __func__);
        row_t *row = calloc(1, sizeof(row_t));
        row->unpacked = ((Vdbe *)stmt)->pResultSet;
        if (queue_add(conns->conns[0].que, row))
            abort();
        _move_client_row(stmt, conns, crt_idx);
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

    if (gbl_dohsql_verbose)
        logmsg(LOGMSG_USER, "%p %s: start\n", (void *)pthread_self(), __func__);
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

        if (gbl_dohsql_verbose)
            logmsg(LOGMSG_USER, "%p XXXX %s Retrieved client %d row %p\n",
                   (void *)pthread_self(), __func__, found, ret_row);

        add_row(conns, found, ret_row);
        Q_UNLOCK(found);

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
                _move_client_row(stmt, conns, idx);
            } else {
                if (conns->conns[que_idx].rc == SQLITE_DONE) {
                    _move_client_done(conns, idx);
                    /* above moves the location of done client at the end,
                    i points to the next row*/
                    i--;
                    unready--;
                } else if (conns->conns[que_idx].rc != SQLITE_ROW) {
                    /* detected an error from a child, stop processing */
                    rc = conns->conns[que_idx].rc;
                    if (gbl_dohsql_verbose)
                        logmsg(LOGMSG_USER, "%s Child %d return error %d!\n",
                               __func__, conns->child_err, rc);
                    Q_UNLOCK(que_idx);
                    _signal_children_master_is_done(conns);
                    /* we cannot reset stmt here since caller will need that to
                       send back columns, if this is the first row; send proper
                       rc so we reset stmt in caller */
                    return SQLITE_EARLYSTOP_DOHSQL;
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
    /* we look at all contributing children, and need to wait;
       since we have the bdb read lock here, check if we need
       to run recovery_deadlock */
    if (bdb_lock_desired(thedb->bdb_env)) {
        rc = recover_deadlock_simple(thedb->bdb_env);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed recover_deadlock rc=%d\n",
                   __func__, rc);
            return rc;
        }
    }

    /* did client disconnect? */
    if (check_sql_client_disconnect(clnt, __FILE__, __LINE__)) {
        _signal_children_master_is_done(conns);
        return SQLITE_EARLYSTOP_DOHSQL;
    }

    /* it is possible that the row the coordinator owns takes all
     * capacity of the source shard, release it here */
    if (rc == SQLITE_OK && conns->active > conns->filling) {
        donate_current_row(conns, 0);
    }

    goto retry_row;
}

int order_init(dohsql_t *conns, dohsql_node_t *node)
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

    conns->order_size = node->order_size;
    conns->order_dir = node->order_dir;
    node->order_size = 0;
    node->order_dir = NULL;

    return SHARD_NOERR;
}

int dohsql_is_parallel_shard(void)
{
    GET_CLNT;
    /* exclude statements that do not arrive
       by a supported plugin */
    if (!clnt->plugin.write_response)
        return 1;

    return (clnt->conns || DOHSQL_CLIENT);
}

/**
 * Retrieve error from a distributed execution plan, if any
 *
 */
int dohsql_error(struct sqlclntstate *clnt, const char **errstr)
{
    struct sqlclntstate *child_clnt;

    if (clnt && clnt->conns && clnt->conns->child_err) {
        child_clnt = clnt->conns->conns[clnt->conns->child_err].clnt;
        *errstr = child_clnt->saved_errstr;
        return child_clnt->saved_rc;
    }

    *errstr = NULL;
    return 0;
}

/**
 * End distribution of the original sql query had a delayed syntax error
 *
 */
void dohsql_handle_delayed_syntax_error(struct sqlclntstate *clnt)
{
    int rc;

    if (!clnt->conns)
        return;

    _signal_children_master_is_done(clnt->conns);

    rc = dohsql_end_distribute(clnt, NULL);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to clear parallel distribution rc=%d",
               __func__, rc);
    }
}

/**
 * Return global stats
 *
 */
void dohsql_stats(void)
{
    logmsg(LOGMSG_USER, "Num requests: %lld [%lld]\n",
           gbl_dohsql_stats.num_reqs, gbl_dohsql_stats_dirty.num_reqs);
    logmsg(LOGMSG_USER, "Max distribution: %d [%d]\n",
           gbl_dohsql_stats.max_distribution,
           gbl_dohsql_stats_dirty.max_distribution);
    logmsg(LOGMSG_USER, "Max queue length: %d [%d]\n",
           gbl_dohsql_stats.max_queue_len,
           gbl_dohsql_stats_dirty.max_queue_len);
    logmsg(LOGMSG_USER, "Max free queue length: %d [%d]\n",
           gbl_dohsql_stats.max_free_queue_len,
           gbl_dohsql_stats_dirty.max_free_queue_len);
    logmsg(LOGMSG_USER, "Max queue bytes: %lld [%lld]\n",
           gbl_dohsql_stats.max_queue_bytes,
           gbl_dohsql_stats_dirty.max_queue_bytes);
}

/* return explain distribution information */
void explain_distribution(dohsql_node_t *node)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt;
    char str[256];
    char *cols[] = {"Plan"};
    int i;

    if (!thd || (!(clnt = thd->clnt)))
        return;

    if (write_response(clnt, RESPONSE_COLUMNS_STR, &cols, 1))
        return;

    if (node->type == AST_TYPE_UNION) {
        snprintf(str, sizeof(str), "Threads %d", node->nnodes);
        char *pstr = &str[0];

        if (write_response(clnt, RESPONSE_ROW_STR, &pstr, 1))
            return;

        for (i = 0; i < node->nnodes; i++) {
            if (write_response(clnt, RESPONSE_ROW_STR, &node->nodes[i]->sql, 1))
                return;
        }
    }

    write_response(clnt, RESPONSE_ROW_LAST, NULL, 0);
}

void dohsql_signal_done(struct sqlclntstate *clnt)
{
    _signal_children_master_is_done(clnt->conns);
}

/**
 * Note: this is called during prepare of the coordinator; the load
 * is not distributed yet, and the coordinator callbacks are not in place
 * Calling param_count/param_value will use the original plugin callbacks
 *
 */
struct params_info *dohsql_params_append(struct params_info **pparams,
                                         const char *name, int index)
{
    struct params_info *params;
    struct param_data *newparam, *temparr;
    int i = 0;

    /* alloc params, if not ready yet */
    if (!(params = *pparams)) {
        struct sql_thread *thd = pthread_getspecific(query_info_key);
        if (!thd || !thd->clnt)
            return NULL;
        params = *pparams = calloc(1, sizeof(struct params_info));
        if (!params)
            return NULL;
        params->clnt = thd->clnt;
    } else {
        /* if already allocated, check to see if name is already in */
        for (i = 0; i < params->nparams; i++) {
            if (!strcmp(name, params->params[i].name)) {
                /* done here */
                return params;
            }
        }
    }

    /* if name is not already in, retrieve value from client plugin
       NOTE: it is important here that the clnt plugin callbacks are not
       changed yet
    */
    newparam = clnt_find_param(params->clnt, name + 1, index);
    if (!newparam) {
        /* clnt parameters are incorrect, fallback to single thread to err */
        free(params->params);
        free(params);
        *pparams = NULL;
        return NULL;
    }
    /* found, add it to the node->params array */
    temparr = realloc(params->params,
                      sizeof(struct param_data) * (params->nparams + 1));
    if (!temparr) {
        if (params->params)
            free(params->params);
        free(params);
        *pparams = NULL;
        return NULL;
    }
    params->params = temparr;
    params->params[params->nparams++] = *newparam;
    free(newparam);
    return *pparams = params;
}

int dohsql_clone_params(int nparams, struct param_data * params,
                        int *pnparams, struct param_data **pparams)
{
    struct param_data *pout;
    int i, tmp;

    /* do not support param arrays yet */
    for (i=0; i < nparams; i++) {
        if (params[i].arraylen > 0) {
            return -1;
        }
    }

    pout = calloc(nparams, sizeof(struct param_data));
    if (!pout)
        return -1;
    memcpy(pout, params, nparams * sizeof(struct param_data));
    for (i=0; i < nparams; i++) {
        if (params[i].name) {
            pout[i].name = strdup(params[i].name);
            if (!pout[i].name) {
                goto err;
            }
        }
        if ((params[i].type == CLIENT_CSTR||
             params[i].type == CLIENT_BLOB)  && params[i].len > 0) {
            pout[i].u.p = malloc(params[i].len);
            if (!pout[i].u.p)
                goto err;
            memcpy(pout[i].u.p, params[i].u.p, params[i].len);
        }
    }

    *pparams = pout;
    *pnparams = nparams;
    return 0;

err:
    tmp = nparams;
    dohsql_free_params(&tmp, &pout, i);
    return -1;
}

void dohsql_free_params(int *pnparams, struct param_data **pparams, int index)
{
    struct param_data * params = *pparams;

    if (index >= *pnparams)
        abort();

    while (index--) {
        free(params[index].name);
        if ((params[index].type == CLIENT_CSTR ||
             params[index].type == CLIENT_BLOB)  && params[index].len > 0) {
            free(params[index].u.p);
        }
    }
    free(params);
    *pnparams = 0;
    *pparams = NULL;
}
