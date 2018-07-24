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

static int gbl_plugin_api_debug = 0;
static int verbose = 0;

struct col {
    int type;
    char *name;
};
typedef struct col col_t;

struct dohsql_connector {
    struct sqlclntstate *clnt;
    queue_type *que; /* queue to caller */
    queue_type *que_free; /* de-queued rows come here to be freed */
    pthread_mutex_t mtx; /* mutex for queueing operations and related counts */
    char *thr_where; /* cached where status */
    col_t *cols;  /* cached cols values */
    int ncols;  /* number of columns */
    int rc;
    int nrows; /* current total queued rows */
    int master_done; /* caller is done */
};

typedef struct dohsql_connector dohsql_connector_t;

typedef Mem row_t;

struct dohsql {
    int nconns;
    dohsql_connector_t * conns;
    col_t *cols;
    int ncols;
    row_t *row;
    int row_src;
    int rc;
    /* LIMIT support */
    int offset; /* any offset */
    int skipped; /* how many rows where skipped so far */
    int limit; /* any limit */
    int nrows; /* sent rows so far */
};
typedef struct dohsql dohsql_t;

static void sqlengine_work_shard_pp(struct thdpool *pool, void *work,
                                      void *thddata, int op);
static void sqlengine_work_shard(struct thdpool *pool, void *work,
                                   void *thddata);
static Expr* _create_low(Parse *pParse, struct Token *col, int iColumn,
        ExprList *list, int shard);
static Expr* _create_high(Parse *pParse, struct Token *col, int iColumn,
        ExprList *list, int shard);
static int _colIndex(Table *pTab, const char *zCol);

#define GET_CLNT \
    struct sql_thread *thd = pthread_getspecific(query_info_key); \
    struct sqlclntstate *clnt = thd->clnt;

#define GET_CONN \
    GET_CLNT; \
    dohsql_connector_t *conn;


/* Create a range structure */
void shard_range_create(struct Parse *pParser, const char *tblname, 
        struct Token *col, ExprList *limits)
{
    sqlite3 *sqldb = pParser->db;
    struct dbtable *db;
    struct schema *s;
    int i;
    char *colname;
    Table *pTab;
    int iColumn;

    db = get_dbtable_by_name(tblname);
    if(!db)
        return;

    pTab = sqlite3FindTableCheckOnly(pParser->db, db->tablename, "main");
    if(!pTab)
        abort(); /* this should not happen, we are past table lookup */

    db->sharding = (shard_limits_t*)calloc(1, sizeof(shard_limits_t));
    if(!db->sharding)
        return;  /* SHARD_ERR_MALLOC; TODO: set engine err */

    db->sharding->nlimits = limits->nExpr;
    db->sharding->col = (struct Token*)malloc(sizeof(struct Token)+col->n+1);
    if(!db->sharding->col)
        return;  /* SHARD_ERR_MALLOC; TODO: set engine err */
       
    db->sharding->col->n = col->n+1;
    db->sharding->col->z = (const char*)(db->sharding->col+1);
    colname = (char*)db->sharding->col->z;
    memcpy(colname, col->z, col->n);
    colname[col->n]='\0';

    iColumn = _colIndex(pTab, colname);

   
    db->sharding->low = (struct Expr**)calloc(2*(db->sharding->nlimits+1), 
            sizeof(struct Expr*));
    if(!db->sharding->low)
        return;  /* SHARD_ERR_MALLOC; TODO: set engine err */
    db->sharding->high = &db->sharding->low[db->sharding->nlimits+1];
    for(i=0;i<db->sharding->nlimits+1; i++) {
        db->sharding->low[i] = _create_low(pParser, col, iColumn, limits, i+1);
        db->sharding->high[i] = _create_high(pParser, col, iColumn, limits, i+1);
    }
}

/* Destroy a range structure */
void shard_range_destroy(shard_limits_t *shards) {
    /* TODO */
}

/**
 * Check if the index with rootpage 'iTable' is configured for
 * parallelized workload, and make sure we have a shard
 * associated with the thread
 */ 
int shard_check_parallelism(int iTable)
{
    GET_CLNT;
    struct dbtable *db;
    shard_limits_t *shards;
    int tblnum;
    int ixnum;
    int rc;
    char *sqlcpy;
    int i;

    /* is this an sql thread ?*/
    if(!thd) 
        return SHARD_NOERR;

    db = get_sqlite_db(thd, iTable, &ixnum);
    if(!db || ixnum<0)
        return SHARD_NOERR;

    if(thd->crtshard>=1)
        return SHARD_NOERR; /*SHARD_NOERR_DONE; */
    
    if(!(shards=db->sharding))
        return SHARD_NOERR; /*SHARD_NOERR_DONE; */
   
    /* grab first shard */
    thd->crtshard=1;

    /*.... create shards ...*/

    return SHARD_NOERR_DONE;
}

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


int handle_setup_error(struct sqlclntstate *clnt)
{
    /* possible way to communicate this */
    return 0;
}


#if 0
static int cache_row(struct sqlthdstate *thd, struct sqlclntstate *clnt,
        int new_row_data_type, int ncols, int row_id, int rc,
        CDB2SQLRESPONSE__Column **columns);
static void mark_done(struct sqlthdstate *thd, struct sqlclntstate *clnt,
        const char *func, int line);

struct client_comm_if shard_client = {
    NULL, /* send schema */
    cache_row, /* send row */
    NULL, /* prepare error */
    NULL, /* runtime error */
    NULL, /* flush */
    NULL, /* send cost */
    NULL, /* send effects */
    mark_done, /* send done */
    NULL, /* send dummy */
};
#endif

static void sqlengine_work_shard(struct thdpool *pool, void *work,
                                 void *thddata)
{
    struct sqlthdstate *thd = thddata;
    struct sqlclntstate *clnt= (struct sqlclntstate*)work;
    int bdberr;
    int inherited_curtrans = 0;
    int rc;
    int created_shadtbl = 0;
    char thdinfo[40];

    thr_set_user("shard thread", clnt->appsock_id);

    rdlock_schema_lk();
    sqlengine_prepare_engine(thd, clnt, 0);
    unlock_schema_lk();

    reqlog_set_origin(thd->logger, "%s", clnt->origin);

    rc = get_curtran(thedb->bdb_env, clnt);
    if (rc) {
        handle_setup_error(clnt);
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

    sql_reset_sqlthread(thd->sqldb, thd->sqlthd);

    if (put_curtran(thedb->bdb_env, clnt)) {
        fprintf(stderr, "%s: unable to destroy a CURSOR transaction!\n",
                __func__);
    }

    clnt->osql.timings.query_finished = osql_log_time();
    osql_log_time_done(clnt);

    /*thrman_setid(thrman_self(), "[done]");*/
}

/**
 * Expand the predicates to include shard predicates for left
 * and right range limits
 *
 */
int comdb2_shard_table_constraints(Parse *pParser, 
        const char *zName, const char *zDatabase, Expr **pWhere)
{
    GET_CLNT;
    struct dbtable *db;
    int idx = clnt->conns_idx; /* this is 1 based */
    Expr *pExprLeft, *pExprRight, *pExpr;
    struct Token tok = {"a", 2};
    struct Table *pTab;

    /* is this is a parallel shard ? */
    if(!clnt->conns/*|| (idx = clnt->conns_idx) < 1*/) {
        return SHARD_NOERR;
    }

    /* check conditions */
    db = get_dbtable_by_name(zName);
    if(!db)
        return SHARD_NOERR;

    /* for version 1, shard only one way */
    if(!db->sharding)
        return SHARD_NOERR;

    /* get the table */
    pTab = sqlite3FindTableCheckOnly(pParser->db, db->tablename, "main");

#if 0
    /* limits, limits */
    if( idx>0 ) {
        /* add left inclusive limit */
        pExprLeft = sqlite3PExpr(pParse, TK_GE, 
                /* column */
                sqlite3ExprAlloc(pParse->db, TK_COLUMN, &tok, 0),
                /* limit */
                sqlite3ExprDup(pParse->db, db->shards[0]->limits->a[idx-1].pExpr, 0),
                0);
        pExprLeft->pLeft->pTab = pTab;
    } else {
        pExprLeft = NULL;
    }
    if(idx<db->shards[0]->limits->nExpr) {
        /* add right exclusive limit */
        pExprRight = sqlite3PExpr(pParse, TK_LT, 
                /* column */
                sqlite3ExprAlloc(pParse->db, TK_COLUMN, &tok, 0),
                /* limit */
                sqlite3ExprDup(pParse->db, db->shards[0]->limits->a[idx].pExpr, 0),
                0);
        pExprRight->pLeft->pTab = pTab;
    } else {
        pExprRight = NULL;
    }
#endif
    if(db->sharding->low[idx-1]) {
        pExprLeft = sqlite3ExprDup(pParser->db, 
                db->sharding->low[idx-1], 0);
        pExprLeft->pLeft->pTab = pTab;
    } else {
        pExprLeft = NULL;
    }
    if(db->sharding->high[idx-1]) {
        pExprRight = sqlite3ExprDup(pParser->db, 
                db->sharding->high[idx-1], 0);
        pExprRight->pLeft->pTab = pTab;
    } else {
        pExprRight = NULL;
    }
    if(pExprLeft)  {
        if(pExprRight) {
            pExprRight->pLeft->pTab = pTab;
            pExpr = sqlite3PExpr(pParser, TK_AND, 
                pExprLeft, pExprRight, 0);
        } else {
            pExpr = pExprLeft;
        }
    } else {
        assert(pExprRight);
        pExpr = pExprRight;
    }

    *pWhere = sqlite3PExpr(pParser, TK_AND, *pWhere, pExpr, 0);

    return SHARD_NOERR_DONE;
}

#if 0

#define _has_snapshot(clnt, sql_response)               \
    CDB2SQLRESPONSE__Snapshotinfo snapshotinfo = CDB2__SQLRESPONSE__SNAPSHOTINFO__INIT; \
                                                        \
    if (clnt->high_availability) {                      \
        int file = 0, offset = 0;                       \
        bdb_tran_get_start_file_offset(thedb->bdb_env, clnt->dbtran.shadow_tran, \
                                       &file, &offset); \
        if (file) {                                     \
            snapshotinfo.file = file;                   \
            snapshotinfo.offset = offset;               \
            sql_response.snapshot_info = &snapshotinfo; \
        }                                               \
    }


static int form_new_row(struct sqlclntstate *clnt, int type,
        CDB2SQLRESPONSE *sql_response,
        void *(*alloc)(size_t size), 
        const char *func, int line,
        dohsql_connector_t *con);


static int cache_row_new(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                        int ncols, int row_id,
                        CDB2SQLRESPONSE__Column **columns, dohsql_connector_t *con)
{
    CDB2SQLRESPONSE sql_response = CDB2__SQLRESPONSE__INIT;
    int rc = 0;
    int i;

    for (i = 0; i < ncols; i++) {
        cdb2__sqlresponse__column__init(columns[i]);
        columns[i]->has_type = 0;
        if (thd->offsets[i].len == -1) {
            columns[i]->has_isnull = 1;
            columns[i]->isnull = 1;
            columns[i]->value.len = 0;
            columns[i]->value.data = NULL;
        } else {
            columns[i]->value.len = thd->offsets[i].len;
            columns[i]->value.data = thd->buf + thd->offsets[i].offset;
        }
    }

    if (clnt->num_retry) {
        sql_response.has_row_id = 1;
        sql_response.row_id = row_id;
    }

    _has_snapshot(clnt, sql_response);

    if (gbl_extended_sql_debug_trace) {
        int sz = clnt->sql_query->cnonce.len;
        char cnonce[256];
        snprintf(cnonce, 256, "%s", clnt->sql_query->cnonce.data);
        fprintf(stderr, "%s: cnonce '%s' snapshot is [%d][%d]\n", __func__,
                cnonce, sql_response.snapshot_info->file, 
                sql_response.snapshot_info->offset);
    }

    sql_response.value = columns;
    /*
    return _push_row_new(clnt, RESPONSE_TYPE__COLUMN_VALUES, &sql_response, 
                         columns, ncols, 
                         ((cdb2__sqlresponse__get_packed_size(&sql_response)+1)
                            > gbl_blob_sz_thresh_bytes) 
                            ?  blob_alloc_override: malloc,
                         0);
     */
    sql_response.response_type = RESPONSE_TYPE__COLUMN_VALUES;
    sql_response.n_value = ncols;
    sql_response.value = columns;
    sql_response.error_code = 0;

    /* cache the response */
    form_new_row(clnt, RESPONSE_HEADER__SQL_RESPONSE, &sql_response, malloc, 
            __func__, __LINE__, con);

    return SHARD_NOERR;
}

extern int gbl_dump_fsql_response;
#endif
typedef struct cached_row {
    void *buf;
    int len;
} cached_row_t;

#if 0
static int form_new_row(struct sqlclntstate *clnt, int type,
        CDB2SQLRESPONSE *sql_response,
        void *(*alloc)(size_t size), 
        const char *func, int line,
        dohsql_connector_t *con)
{
    struct newsqlheader *phdr;
    cached_row_t *row;
    int len;

    if (clnt->in_client_trans && clnt->sql_query &&
        clnt->sql_query->skip_rows == -1 && (clnt->isselect != 0)) {
        // Client doesn't expect any response at this point.
        printf("sending nothing back to client \n");
        return 0;
    }


    /* payload */
    if (sql_response) {
        len = cdb2__sqlresponse__get_packed_size(sql_response);
    } else {
        len = 0;
    }

    /* header */
    row = (cached_row_t*)(*alloc)(sizeof(*row)+sizeof(*phdr)+len+1);
    if(!row)
        return SHARD_ERR_MALLOC;
    row->buf = phdr = (struct newsqlheader*)(row+1);
    row->len = sizeof(*phdr)+len;

    phdr->type = ntohl(type);
    phdr->compression = 0;
    phdr->dummy = 0;
    phdr->length = ntohl(len);

    if(len)
        cdb2__sqlresponse__pack(sql_response, (uint8_t *)(phdr+1));


    if (gbl_dump_fsql_response) {
        printf("Caching response=%d dta length %d to %s for sql %s"
               " from %s line %d\n",
               type, len, clnt->origin, clnt->sql, func ? func : "(NULL)",
               line);
        hexdump(row->buf, sizeof(*phdr));
        printf("\n");
        hexdump(row->buf+sizeof(*phdr), row->len-sizeof(*phdr));
        printf("\n");
    }

    pthread_mutex_lock(&con->mtx);

    queue_add(con->que, row);

    pthread_mutex_unlock(&con->mtx);

    return SHARD_NOERR;
}

static int cache_row_old(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                        int new_row_data_type, dohsql_connector_t *con)
{
    struct fsqlresp resp;
    cached_row_t *row;

    if (new_row_data_type) {
        resp.response = FSQL_NEW_ROW_DATA;
    } else {
        resp.response = FSQL_ROW_DATA;
    }
    resp.flags = 0;
    resp.rcode = FSQL_OK;
    resp.parm = 0;

    row = (cached_row_t*)malloc(sizeof(cached_row_t) + sizeof(resp) + thd->buflen);
    row->len = thd->buflen;
    row->buf = (char*)(row+1);
    resp.followlen = thd->buflen;
    fsqlresp_put(&resp, row->buf, ((char*)row->buf) + sizeof(resp));
    memcpy(((char*)row->buf) + sizeof(resp), thd->buf, thd->buflen);

    /*
    return fsql_write_response(clnt, &resp, thd->buf, thd->buflen, 0, __func__,
                               __LINE__);
     */

    /* queue row */
    pthread_mutex_lock(&con->mtx);

    queue_add(con->que, row);

    pthread_mutex_unlock(&con->mtx);

    return SHARD_NOERR;

}

static int cache_row(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                    int new_row_data_type, int ncols, int row_id, int rc,
                    CDB2SQLRESPONSE__Column **columns)
{
    dohsql_connector_t *con;
    if (!clnt->conns)
        return SHARD_ERR_GENERIC;
    if(clnt->conns_idx<=1 || !(con=&clnt->conns[clnt->conns_idx-2]))
        return SHARD_ERR_GENERIC;
    if(clnt != con->clnt)
        return SHARD_ERR_GENERIC;

    int irc = 0;
    if (clnt->is_newsql) {
        if (!rc && (clnt->num_retry == clnt->sql_query->retry) &&
                (clnt->num_retry == 0 || clnt->sql_query->has_skip_rows == 0 ||
                 (clnt->sql_query->skip_rows < row_id)))
            irc = cache_row_new(thd, clnt, ncols, row_id, columns, con);
    } else {
        irc = cache_row_old(thd, clnt, new_row_data_type, con);
    }
    return irc;
}

void shard_flush_conns(struct sqlclntstate *clnt, int waitfordone)
{
    int leftovers;
    int i;

    do {
        leftovers = 0;
        for(i=0;i<clnt->nconns;i++) {
            cached_row_t *row;
            if(waitfordone) {
                if(!clnt->conns[i].done)
                    leftovers = 1;
            }
            pthread_mutex_lock(&clnt->conns[i].mtx);
            while(row = queue_next(clnt->conns[i].que)) {
                pthread_mutex_unlock(&clnt->conns[i].mtx);
                /* shard slicing */
                if(clnt->shard_slice <= 0 || clnt->shard_slice == i+2) {
                    if (gbl_dump_fsql_response) {
                        struct newsqlheader *hdr = (struct newsqlheader*)row->buf;

                        printf("Sending shard response=%d dta length %d to node %s for sql "
                                "%s newsql-flag %d\n",
                                ntohl(hdr->type), ntohl(hdr->length), clnt->origin, clnt->sql, 
                                clnt->is_newsql);
                        hexdump(row->buf, sizeof(*hdr));
                        printf("\n");
                        hexdump(row->buf+sizeof(*hdr), row->len-sizeof(*hdr));
                        printf("\n");
                    }
                    pthread_mutex_lock(&clnt->write_lock);
                    sbuf2write(row->buf, row->len, clnt->sb); 
                    pthread_mutex_unlock(&clnt->write_lock);
                }

                pthread_mutex_lock(&clnt->conns[i].mtx);

                free(row);
            }
            pthread_mutex_unlock(&clnt->conns[i].mtx);
        }
    } while(leftovers);
}

#endif

static int _colIndex(Table *pTab, const char *zCol) 
{
    int j;
    Column *pCol;

    for(j=0, pCol=pTab->aCol; j<pTab->nCol; j++, pCol++)
        if( sqlite3StrICmp(pCol->zName, zCol)==0 )
            return j;
    return -1;
}

static Expr* _create_low(Parse *pParser, struct Token *col, int iColumn,
        ExprList *list, int shard)
{
    Expr *ret;

    assert(shard>0);

    if (shard == 1)
        return NULL;
    if(shard-2 >= list->nExpr)
        return NULL;

    ret = sqlite3PExpr(pParser, TK_GE, 
            /* column */
            sqlite3ExprAlloc(pParser->db, TK_COLUMN, col, 0),
            /* limit */
            sqlite3ExprDup(pParser->db, list->a[shard-2].pExpr, 0),
            0);
    if(ret) {
        ret->pLeft->iColumn = iColumn;
    }

    return ret; 
}

static Expr* _create_high(Parse *pParser, struct Token *col, int iColumn, 
        ExprList *list, int shard)
{
    Expr *ret;

    assert(shard>0);
   
    /* no high limit for last shard */
    if (shard-1 >= list->nExpr)
        return NULL;

    ret = sqlite3PExpr(pParser, TK_LT, 
            /* column */
            sqlite3ExprAlloc(pParser->db, TK_COLUMN, col, 0),
            /* limit */
            sqlite3ExprDup(pParser->db, list->a[shard-1].pExpr, 0),
            0);
    if(ret) {
        ret->pLeft->iColumn = iColumn;
    }

    return ret;
}
/*
static void mark_done(struct sqlthdstate *thd, struct sqlclntstate *clnt,
        const char *func, int line)
{
    assert(clnt == clnt->conns[clnt->conns_idx-2].clnt);

    clnt->conns[clnt->conns_idx-2].done = 1;
}
*/
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
    dohsql_connector_t *conn = (dohsql_connector_t*)clnt->plugin.state;
    int ncols, i;

    ncols = sqlite3_column_count(stmt);

    if (!conn->cols || conn->ncols < ncols) {
        conn->cols = (col_t*)realloc(conn->cols, ncols*sizeof(col_t));
        if(!conn->cols)
            return -1;
    }
    conn->ncols = ncols;

    for(i=0;i<ncols;i++) {
        conn->cols[i].type = inner_type(stmt, i);
    }
    return 0;
}

static void trimQue(sqlite3_stmt *stmt, queue_type* que, int limit)
{
    row_t   *row;
    return;
    while (queue_count(que)>limit) {
        row = queue_next(que);
        if (verbose)
            logmsg(LOGMSG_DEBUG, "XXX: %p freed older row limit %d\n", que, limit);
        sqlite3CloneResultFree(stmt, &row);
    }
}

static int inner_row(struct sqlclntstate *clnt, struct response_data *resp, int postpone)
{
    dohsql_connector_t *conn = (dohsql_connector_t*)clnt->plugin.state;
    sqlite3_stmt *stmt = resp->stmt;
    struct errstat *err = resp->err;
    
    row_t   *row;
    row_t   *oldrow;

    oldrow = NULL;
    pthread_mutex_lock(&conn->mtx);
    
    if(conn->master_done) {
        if (verbose)
            logmsg(LOGMSG_DEBUG, "%s: %d master done q %d qf %d\n", 
                    __func__, pthread_self(), queue_count(conn->que),
                    queue_count(conn->que_free));
        /* work is done, need to clean-up */
        trimQue(stmt, conn->que, 0);
        trimQue(stmt, conn->que_free, 0);
                
        conn->rc = SQLITE_DONE; /* signal master this is clear */

        return  SQLITE_DONE;    /* any != 0 will do, this impersonates a normal end */
    }

    /* try to steal an old row */
    if (queue_count(conn->que_free)>0){
        fprintf(stderr, "%s: %d retrieved older row\n", __func__, pthread_self());
        oldrow = queue_next(conn->que_free);
    }
    pthread_mutex_unlock(&conn->mtx);

    row = sqlite3CloneResult(stmt, &oldrow);
    if(!row)
        return SHARD_ERR_GENERIC;

    pthread_mutex_lock(&conn->mtx);
    conn->rc = SQLITE_ROW;
    queue_add(conn->que, row);
    if(verbose)
        logmsg(LOGMSG_DEBUG, "XXX: %p added new row\n", conn);

    /* inline cleanup */
    if (queue_count(conn->que_free)>10) {
        trimQue(stmt, conn->que, 5);
    }
    pthread_mutex_unlock(&conn->mtx);

    return SHARD_NOERR;
}

static int inner_row_last(struct sqlclntstate *clnt)
{
    dohsql_connector_t *conn = (dohsql_connector_t*)clnt->plugin.state;

    pthread_mutex_lock(&conn->mtx);
    conn->rc = SQLITE_DONE;
    pthread_mutex_unlock(&conn->mtx);

    return SHARD_NOERR;
}


/* override sqlite engine */
static int dohsql_dist_column_count(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    return clnt->conns->ncols;
}

#define FUNC_COLUMN_TYPE(ret, type) \
static ret dohsql_dist_column_ ## type (struct sqlclntstate *clnt, sqlite3_stmt *stmt, int iCol) \
{ \
    dohsql_t *conns = clnt->conns; \
    if (conns->row_src == 0) \
        return sqlite3_column_ ## type (stmt, iCol); \
    return sqlite3_value_ ## type (&conns->row[iCol]); \
} 

FUNC_COLUMN_TYPE( int, type)
FUNC_COLUMN_TYPE( sqlite_int64, int64)
FUNC_COLUMN_TYPE( double, double)
FUNC_COLUMN_TYPE( int, bytes)
FUNC_COLUMN_TYPE( const unsigned char *, text)
FUNC_COLUMN_TYPE( const void *, blob)
FUNC_COLUMN_TYPE( const dttz_t*, datetime)

static const intv_t* dohsql_dist_column_interval(struct sqlclntstate *clnt, sqlite3_stmt *stmt,
        int iCol, int type)
{ 
    dohsql_t *conns = clnt->conns; 
    if (conns->row_src == 0) 
        return sqlite3_column_interval(stmt, iCol, type); 
    return sqlite3_value_interval(&conns->row[iCol], type);
} 

static sqlite3_value* dohsql_dist_column_value(struct sqlclntstate *clnt, sqlite3_stmt *stmt, int i)
{
    dohsql_t *conns = clnt->conns;

    if (conns->row_src == 0)
        return sqlite3_column_value(stmt, i);

    return &conns->row[i];
}


static void add_row(dohsql_t *conns, int i, row_t *row)
{
    if (conns->row) {
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
    conns->nrows++;
}

#define CHILD_DONE(kid) (queue_count(conns->conns[(kid)].que) == 0 && \
                conns->conns[(kid)].rc == SQLITE_DONE )
#define CHILD_ERROR(kid) (conns->conns[(kid)].rc != SQLITE_ROW && \
                conns->conns[(kid)].rc != SQLITE_DONE)

/**
 * this is a non-ordered merge of N engine outputs 
 *
 */
static int dohsql_dist_next_row(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    dohsql_t *conns = clnt->conns;
    row_t *row;
    int rc;
    int child_num, err_child_num;
    int empty;


    if(conns->nrows == 0) {
        rc = sqlite3_step(stmt);

        if (verbose)
            logmsg(LOGMSG_DEBUG, "%s: sqlite3_step rc %d\n", __func__, rc);

        if(conns->limit>0) {
            conns->limit = sqlite3_value_int64(
                    &((Vdbe*)stmt)->aMem[conns->limit-1])+1;
            if (verbose)
                logmsg(LOGMSG_DEBUG, "XXX: found limit, set to %d\n", conns->limit);
            assert(rc!=SQLITE_ROW || conns->limit>1); /* sqlite planning */
        }

        if (rc == SQLITE_DONE)
            conns->conns[0].rc = SQLITE_DONE;
        else {
            if (rc == SQLITE_ROW) {
                add_row(conns, 0, NULL);
                goto got_row;
            }

            goto signal_children_master_is_done;
        }
    }


    if (conns->limit>0 && conns->nrows >= conns->limit-1) {
        if(conns->conns[0].rc != SQLITE_DONE) {
            sqlite3_reset(stmt);
        }

        rc = SQLITE_DONE;
        goto signal_children_master_is_done;
    }

wait_for_others:
    rc = 0;
    row = NULL;
    empty = 1;
    for(child_num=1;child_num<conns->nconns && row == NULL;child_num++) {
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
            err_child_num = child_num;
            /* we could envision a case when child is retried for cut 2*/
            /* for now, signal all children that we are done and pass error
               to caller */
            goto signal_children_master_is_done;
        }
        empty = 0;
        if (queue_count(conns->conns[child_num].que)>0) {
            row = queue_next(conns->conns[child_num].que);
            if (verbose)
                logmsg(LOGMSG_DEBUG, "XXX: %p retrieved row\n", &conns->conns[child_num]);
            add_row(conns, child_num, row);
            rc = SQLITE_ROW;
            pthread_mutex_unlock(&conns->conns[child_num].mtx);
            goto got_row;
        }
        pthread_mutex_unlock(&conns->conns[child_num].mtx);
    }

    /* no row in others (yet) */
    if (conns->conns[0].rc != SQLITE_DONE) {
        rc = sqlite3_step(stmt);
        
        if( verbose)
            logmsg(LOGMSG_DEBUG, "%s: rc =%d\n", __func__, rc);

        if (rc == SQLITE_DONE)
            conns->conns[0].rc = SQLITE_DONE;
        else {
            if (rc == SQLITE_ROW) {
                /*if(conns->limit>0)
                    conns->limit++;*/
                add_row(conns, 0, NULL);
                goto got_row;
            }

            goto signal_children_master_is_done;
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
    if (conns->offset && conns->skipped < conns->offset) {
        conns->skipped++;
        /* skip it */
        goto wait_for_others;
    }
    return SQLITE_ROW;

signal_children_master_is_done:
    for(child_num=1;child_num<conns->nconns;child_num++) {
        pthread_mutex_lock(&conns->conns[child_num].mtx);
        conns->conns[child_num].master_done = 1;
        pthread_mutex_unlock(&conns->conns[child_num].mtx);
    }
    return rc;
}

static int dohsql_write_response(struct sqlclntstate *c, int t, void *a, int i)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s %d\n", pthread_self(), __func__, t); 
    switch (t) {
    case RESPONSE_COLUMNS: return inner_columns(c, a);
    case RESPONSE_COLUMNS_STR: return 0/*newsql_columns_str(c, a, i)*/;
    case RESPONSE_DEBUG: return 0/*newsql_debug(c, a)*/;
    case RESPONSE_ERROR: return 0/*newsql_error(c, a, i)*/;
    case RESPONSE_ERROR_ACCESS: return 0/*newsql_error(c, a, CDB2ERR_ACCESS)*/;
    case RESPONSE_ERROR_BAD_STATE: return 0/*newsql_error(c, a, CDB2ERR_BADSTATE)*/;
    case RESPONSE_ERROR_PREPARE: return 0/*newsql_error(c, a, CDB2ERR_PREPARE_ERROR)*/;
    case RESPONSE_ERROR_REJECT: return 0/*newsql_error(c, a, CDB2ERR_REJECTED)*/;
    case RESPONSE_FLUSH: return 0/*newsql_flush(c)*/;
    case RESPONSE_HEARTBEAT: return 0/*newsql_heartbeat(c)*/;
    case RESPONSE_ROW: return inner_row(c, a, i);
    case RESPONSE_ROW_LAST: return inner_row_last(c);
    case RESPONSE_ROW_LAST_DUMMY: return 0/*newsql_row_last_dummy(c)*/;
    case RESPONSE_ROW_LUA: return 0/*newsql_row_lua(c, a)*/;
    case RESPONSE_ROW_STR: return 0/*newsql_row_str(c, a, i)*/;
    case RESPONSE_TRACE: return 0/*newsql_trace(c, a)*/;
    /* fastsql only messages */
    case RESPONSE_COST:
    case RESPONSE_EFFECTS:
    case RESPONSE_ERROR_PREPARE_RETRY: return 0;
    case RESPONSE_COLUMNS_LUA:
    default: abort();
    }
    return 0;
}
static int dohsql_read_response(struct sqlclntstate *a, int b, void *c, int d)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s %d\n", pthread_self(), __func__, b); 
    return -1;
}
static void *dohsql_save_stmt(struct sqlclntstate *clnt, void *arg)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return strdup(clnt->sql);
}
static void *dohsql_restore_stmt(struct sqlclntstate *clnt, void *arg)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    clnt->sql = arg;
    return NULL;
}
static void *dohsql_destroy_stmt(struct sqlclntstate *clnt, void *arg)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    free(arg);
    return NULL;
}
static void *dohsql_print_stmt(struct sqlclntstate *clnt, void *arg)
{
     if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return arg;
}
static int dohsql_param_count(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s TODO\n", pthread_self(), __func__); 
    return 0;
}
static int dohsql_param_index(struct sqlclntstate *a, const char *b, int64_t *c)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return -1;
}
static int dohsql_param_value(struct sqlclntstate *a, struct param_data *b, int c)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return -1;
}
static int dohsql_override_count(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s TODO\n", pthread_self(), __func__); 
    return 0;
}
static int dohsql_clr_cnonce(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return -1;
}
static int dohsql_has_cnonce(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return 0;
}
static int dohsql_set_cnonce(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return -1;
}
static int dohsql_get_cnonce(struct sqlclntstate *a, snap_uid_t *b)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return -1;
}
static int dohsql_get_snapshot(struct sqlclntstate *a, int *b, int *c)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return -1;
}
static int dohsql_upd_snapshot(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return -1;
}
static int dohsql_clr_snapshot(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return -1;
}
static int dohsql_has_high_availability(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return 0;
}
static int dohsql_set_high_availability(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return -1;
}
static int dohsql_clr_high_availability(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return -1;
}
static int dohsql_get_high_availability(struct sqlclntstate *a)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return 0;
}
static void dohsql_add_steps(struct sqlclntstate *a, double b)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
}
static void dohsql_setup_client_info(struct sqlclntstate *clnt, struct sqlthdstate *b, char *c)
{
    dohsql_connector_t *conn = (dohsql_connector_t*)clnt->plugin.state;
   
    if(conn->thr_where)
        thrman_wheref(thrman_self(), "%s", conn->thr_where);

    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s %s\n", pthread_self(), __func__, (conn->thr_where)?conn->thr_where: "NULL"); 
}
static int dohsql_skip_row(struct sqlclntstate *a, uint64_t b)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return 0;
}
static int dohsql_log_context(struct sqlclntstate *a, struct reqlogger *b)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s TODO\n", pthread_self(), __func__); 
    return 0;
}
static uint64_t dohsql_get_client_starttime(struct sqlclntstate *clnt)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return 0;
}
static int dohsql_get_client_retries(struct sqlclntstate *clnt)
{
    if (gbl_plugin_api_debug)
        logmsg(LOGMSG_WARN, "%p %s\n", pthread_self(), __func__); 
    return 0;
}

static int _shard_connect(struct sqlclntstate *clnt, dohsql_connector_t *conn,
        const char *sql)
{
    conn->clnt = (struct sqlclntstate*)calloc(1, sizeof(struct sqlclntstate));
    if(!conn->clnt) {
        return SHARD_ERR_MALLOC;
    }
    conn->que = queue_new();
    if(!conn->que) {
        free(conn->clnt);
        conn->clnt = NULL;
        return SHARD_ERR_MALLOC;
    }
    conn->que_free = queue_new();
    if(!conn->que_free) {
        queue_free(conn->que);
        conn->que = NULL;
        free(conn->clnt);
        conn->clnt = NULL;
        return SHARD_ERR_MALLOC;
    }
    pthread_mutex_init(&conn->mtx, NULL);

    comdb2uuid(conn->clnt->osql.uuid);
    conn->clnt->appsock_id = getarchtid();
    init_sqlclntstate(conn->clnt, conn->clnt->osql.uuid, 1);
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
    clnt->sql=NULL;
    cleanup_clnt(clnt);
    pthread_mutex_destroy(&clnt->wait_mutex);
    pthread_cond_destroy(&clnt->wait_cond);
    pthread_mutex_destroy(&clnt->write_lock);
    pthread_mutex_destroy(&clnt->dtran_mtx);
    free(clnt);
}

int dohsql_distribute(dohsql_node_t *node)
{
    GET_CLNT;
    dohsql_t *conns;
    char *sqlcpy;
    int i,rc;
    
    /* setup communication queue */
    conns = (dohsql_t*)calloc(1, sizeof(dohsql_t) +
            node->nnodes*sizeof(dohsql_connector_t));
    if(!conns)
        return SHARD_ERR_MALLOC;
    conns->conns = (dohsql_connector_t*)(conns+1);
    conns->nconns = node->nnodes;
    conns->ncols = node->ncols;
    clnt->conns = conns;
    /* augment interface */
    clnt->plugin.column_count = dohsql_dist_column_count;
    clnt->plugin.next_row = dohsql_dist_next_row;
    clnt->plugin.column_type = dohsql_dist_column_type;
    clnt->plugin.column_int64 = dohsql_dist_column_int64;
    clnt->plugin.column_double = dohsql_dist_column_double;
    clnt->plugin.column_text = dohsql_dist_column_text;
    clnt->plugin.column_bytes = dohsql_dist_column_bytes;
    clnt->plugin.column_blob = dohsql_dist_column_blob;
    clnt->plugin.column_datetime = dohsql_dist_column_datetime;
    clnt->plugin.column_interval = dohsql_dist_column_interval;

    /* start peers */
    for(i=0;i<conns->nconns;i++)
    {
        if(rc = _shard_connect(clnt, &conns->conns[i], node->nodes[i]->sql))
            return rc;

        if(i>0) {
            /* launch the new sqlite engine a the next shard */
            rc = thdpool_enqueue(gbl_sqlengine_thdpool, 
                    sqlengine_work_shard_pp, clnt->conns->conns[i].clnt, 1, 
                    sqlcpy=strdup(node->nodes[i]->sql));
            if(rc) {
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

    if(!clnt->conns)
        return SHARD_NOERR;

    for(i=1; i<conns->nconns; i++) {
        pthread_mutex_lock(&conns->conns[i].mtx);
        while(conns->conns[i].rc) {
            pthread_mutex_unlock(&conns->conns[i].mtx);
            poll(NULL, 0, 10);
            pthread_mutex_lock(&conns->conns[i].mtx);
        }
        pthread_mutex_unlock(&conns->conns[i].mtx);
    }

    for(i=1; i<conns->nconns; i++) {
        _shard_disconnect(&conns->conns[i]);
    }

    free(clnt->conns);
    clnt->conns = NULL;

    return SHARD_NOERR;
}

#define DOHSQL_CLIENT (clnt->plugin.state && \
                        clnt->plugin.write_response == dohsql_write_response)

void dohsql_wait_for_master(sqlite3_stmt* stmt, struct sqlclntstate *clnt)
{
    dohsql_connector_t *conn;

    if(!stmt || !DOHSQL_CLIENT)
        return;

    conn = clnt->plugin.state;

    pthread_mutex_lock(&conn->mtx);

    /* wait if run ended ok, master is not done, and there are cached rows */
    if (!clnt->query_rc) {
        while(!conn->master_done && queue_count(conn->que)>0) {
            pthread_mutex_unlock(&conn->mtx);
            poll(NULL, 0, 10);
            pthread_mutex_lock(&conn->mtx);
        }
    }

    trimQue(stmt, conn->que, 0);
    trimQue(stmt, conn->que_free, 0);

    conn->rc = 0;

    pthread_mutex_unlock(&conn->mtx);
}

const char* dohsql_get_sql(struct sqlclntstate *clnt, int index)
{
    return clnt->conns->conns[index].clnt->sql;
}

void comdb2_register_limit(Select *p)
{
    GET_CLNT;

    if (unlikely(clnt->conns)) {
        clnt->conns->limit = p->iLimit+1;
    }
}
