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

#include "comdb2.h"
#include "sql.h"
#include "shard_range.h"
#include "sqliteInt.h"
#include "queue.h"

extern void query_stats_setup(struct sqlthdstate *thd, struct sqlclntstate *clnt);
extern int handle_sqlite_requests(struct sqlthdstate *thd,
                                  struct sqlclntstate *clnt);
extern void sql_reset_sqlthread(sqlite3 *db, struct sql_thread *thd);


struct par_connector
{
    struct sqlclntstate *clnt;
    queue_type *que;
    pthread_mutex_t mtx;
    int done;
};

static void sqlengine_work_shard_pp(struct thdpool *pool, void *work,
                                      void *thddata, int op);
static void sqlengine_work_shard(struct thdpool *pool, void *work,
                                   void *thddata);
static int _shard_connect(struct sqlclntstate *clnt, int i);
static Expr* _create_low(Parse *pParse, struct Token *col, int iColumn,
        ExprList *list, int shard);
static Expr* _create_high(Parse *pParse, struct Token *col, int iColumn,
        ExprList *list, int shard);
static int _colIndex(Table *pTab, const char *zCol);

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
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;
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

    /* setup communication queue */
    clnt->conns = (par_connector_t*)calloc(shards->nlimits, sizeof(par_connector_t));
    if(!clnt->conns)
        return SHARD_ERR_MALLOC;
    /*this initial will take care of the first shard */
    clnt->conns_idx = thd->crtshard;
    clnt->nconns = shards->nlimits;

    for(i=0;i<clnt->nconns;i++)
    {
        if(rc = _shard_connect(clnt, i+2))
            return rc;

        /* launch the new sqlite engine a the next shard */
        rc = thdpool_enqueue(gbl_sqlengine_thdpool, sqlengine_work_shard_pp, clnt->conns[i].clnt, 1, 
                             sqlcpy=strdup(clnt->sql));
        if(rc) {
            free(sqlcpy);
            return SHARD_ERR_GENERIC;
        }
    }

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

    /* associate the sql thread with the shard */
    thd->sqlthd->crtshard = clnt->conns_idx; /* 1 is the originator */
    snprintf(thdinfo, sizeof(thdinfo), "shard thread %u", clnt->appsock_id);
    thrman_setid(thrman_self(), thdinfo);

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
    */
    
    /*expanded execute_sql_query */
    query_stats_setup(thd, clnt);

    clnt->query_rc = handle_sqlite_requests(thd, clnt);

    sql_reset_sqlthread(thd->sqldb, thd->sqlthd);

    if (put_curtran(thedb->bdb_env, clnt)) {
        fprintf(stderr, "%s: unable to destroy a CURSOR transaction!\n",
                __func__);
    }

    clnt->osql.timings.query_finished = osql_log_time();
    osql_log_time_done(clnt);

    thrman_setid(thrman_self(), "[done]");
}

/**
 * Expand the predicates to include shard predicates for left
 * and right range limits
 *
 */
int comdb2_shard_table_constraints(Parse *pParser, 
        const char *zName, const char *zDatabase, Expr **pWhere)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;
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
        par_connector_t *con);


static int cache_row_new(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                        int ncols, int row_id,
                        CDB2SQLRESPONSE__Column **columns, par_connector_t *con)
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
        par_connector_t *con)
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
                        int new_row_data_type, par_connector_t *con)
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
    par_connector_t *con;
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

static void mark_done(struct sqlthdstate *thd, struct sqlclntstate *clnt,
        const char *func, int line)
{
    assert(clnt == clnt->conns[clnt->conns_idx-2].clnt);

    clnt->conns[clnt->conns_idx-2].done = 1;
}

static int dohsql_write_response(struct sqlclntstate *a, int b, void *c, int d)
{
    return 0;
}
static int dohsql_read_response(struct sqlclntstate *a, int b, void *c, int d)
{
    return -1;
}
static void *dohsql_save_stmt(struct sqlclntstate *clnt, void *arg)
{
    return strdup(clnt->sql);
}
static void *dohsql_restore_stmt(struct sqlclntstate *clnt, void *arg)
{
    clnt->sql = arg;
    return NULL;
}
static void *dohsql_destroy_stmt(struct sqlclntstate *clnt, void *arg)
{
    free(arg);
    return NULL;
}
static void *dohsql_print_stmt(struct sqlclntstate *clnt, void *arg)
{
    return arg;
}
static int dohsql_param_count(struct sqlclntstate *a)
{
    return 0;
}
static int dohsql_param_index(struct sqlclntstate *a, const char *b, int64_t *c)
{
    return -1;
}
static int dohsql_param_value(struct sqlclntstate *a, struct param_data *b, int c)
{
    return -1;
}
static int dohsql_override_count(struct sqlclntstate *a)
{
    return 0;
}
static int dohsql_clr_cnonce(struct sqlclntstate *a)
{
    return -1;
}
static int dohsql_has_cnonce(struct sqlclntstate *a)
{
    return 0;
}
static int dohsql_set_cnonce(struct sqlclntstate *a)
{
    return -1;
}
static int dohsql_get_cnonce(struct sqlclntstate *a, snap_uid_t *b)
{
    return -1;
}
static int dohsql_get_snapshot(struct sqlclntstate *a, int *b, int *c)
{
    return -1;
}
static int dohsql_upd_snapshot(struct sqlclntstate *a)
{
    return -1;
}
static int dohsql_clr_snapshot(struct sqlclntstate *a)
{
    return -1;
}
static int dohsql_has_high_availability(struct sqlclntstate *a)
{
    return 0;
}
static int dohsql_set_high_availability(struct sqlclntstate *a)
{
    return -1;
}
static int dohsql_clr_high_availability(struct sqlclntstate *a)
{
    return -1;
}
static int dohsql_get_high_availability(struct sqlclntstate *a)
{
    return 0;
}
static void dohsql_add_steps(struct sqlclntstate *a, double b)
{
}
static void dohsql_setup_client_info(struct sqlclntstate *a, struct sqlthdstate *b, char *c)
{
}
static int dohsql_skip_row(struct sqlclntstate *a, uint64_t b)
{
    return 0;
}
static int dohsql_log_context(struct sqlclntstate *a, struct reqlogger *b)
{
    return 0;
}


static int _shard_connect(struct sqlclntstate *clnt, int i)
{
    par_connector_t *par = &clnt->conns[i-2]; 

    /* we need to create our own clnt structure; we can revise this later to share the same clnt */
    par->clnt = (struct sqlclntstate*)calloc(1, sizeof(struct sqlclntstate));
    if(!par->clnt) {
        return SHARD_ERR_MALLOC;
    }
    par->que = queue_new();
    if(!par->que) {
        return SHARD_ERR_MALLOC;
    }
    pthread_mutex_init(&par->mtx, NULL);

    comdb2uuid(par->clnt->osql.uuid);
    par->clnt->appsock_id = getarchtid();
    init_sqlclntstate(par->clnt, par->clnt->osql.uuid, 1);
    par->clnt->conns = clnt->conns;
    par->clnt->conns_idx = i;
    par->clnt->origin = clnt->origin;
    par->clnt->sql = strdup(clnt->sql);

    plugin_set_callbacks(par->clnt, dohsql);

    return SHARD_NOERR;
}

