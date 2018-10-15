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
#include "sqlinterfaces.h"

struct col {
    int type;
    char *name;
};

static Expr *_create_low(Parse *pParse, struct Token *col, int iColumn,
                         ExprList *list, int shard);
static Expr *_create_high(Parse *pParse, struct Token *col, int iColumn,
                          ExprList *list, int shard);
static int _colIndex(Table *pTab, const char *zCol);

#define GET_CLNT                                                               \
    struct sql_thread *thd = pthread_getspecific(query_info_key);              \
    struct sqlclntstate *clnt = thd->clnt;

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
    if (!db)
        return;

    pTab = sqlite3FindTableCheckOnly(pParser->db, db->tablename, "main");
    if (!pTab)
        abort(); /* this should not happen, we are past table lookup */

    db->sharding = (shard_limits_t *)calloc(1, sizeof(shard_limits_t));
    if (!db->sharding)
        return; /* SHARD_ERR_MALLOC; TODO: set engine err */

    db->sharding->nlimits = limits->nExpr;
    db->sharding->col =
        (struct Token *)malloc(sizeof(struct Token) + col->n + 1);
    if (!db->sharding->col)
        return; /* SHARD_ERR_MALLOC; TODO: set engine err */

    db->sharding->col->n = col->n + 1;
    db->sharding->col->z = (const char *)(db->sharding->col + 1);
    colname = (char *)db->sharding->col->z;
    memcpy(colname, col->z, col->n);
    colname[col->n] = '\0';

    iColumn = _colIndex(pTab, colname);

    db->sharding->low = (struct Expr **)calloc(2 * (db->sharding->nlimits + 1),
                                               sizeof(struct Expr *));
    if (!db->sharding->low)
        return; /* SHARD_ERR_MALLOC; TODO: set engine err */
    db->sharding->high = &db->sharding->low[db->sharding->nlimits + 1];
    for (i = 0; i < db->sharding->nlimits + 1; i++) {
        db->sharding->low[i] =
            _create_low(pParser, col, iColumn, limits, i + 1);
        db->sharding->high[i] =
            _create_high(pParser, col, iColumn, limits, i + 1);
    }
}

/* Destroy a range structure */
void shard_range_destroy(shard_limits_t *shards)
{
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
    if (!thd)
        return SHARD_NOERR;

    db = get_sqlite_db(thd, iTable, &ixnum);
    if (!db || ixnum < 0)
        return SHARD_NOERR;

    if (thd->crtshard >= 1)
        return SHARD_NOERR; /*SHARD_NOERR_DONE; */

    if (!(shards = db->sharding))
        return SHARD_NOERR; /*SHARD_NOERR_DONE; */

    /* grab first shard */
    thd->crtshard = 1;

    /*.... create shards ...*/

    return SHARD_NOERR_DONE;
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

/**
 * Expand the predicates to include shard predicates for left
 * and right range limits
 *
 */
int comdb2_shard_table_constraints(Parse *pParser, const char *zName,
                                   const char *zDatabase, Expr **pWhere)
{
    GET_CLNT;
    struct dbtable *db;
    int idx = clnt->conns_idx; /* this is 1 based */
    Expr *pExprLeft, *pExprRight, *pExpr;
    struct Token tok = {"a", 2};
    struct Table *pTab;

    /* is this is a parallel shard ? */
    if (!clnt->conns /*|| (idx = clnt->conns_idx) < 1*/) {
        return SHARD_NOERR;
    }

    /* check conditions */
    db = get_dbtable_by_name(zName);
    if (!db)
        return SHARD_NOERR;

    /* for version 1, shard only one way */
    if (!db->sharding)
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
    if (db->sharding->low[idx - 1]) {
        pExprLeft = sqlite3ExprDup(pParser->db, db->sharding->low[idx - 1], 0);
        pExprLeft->pLeft->pTab = pTab;
    } else {
        pExprLeft = NULL;
    }
    if (db->sharding->high[idx - 1]) {
        pExprRight =
            sqlite3ExprDup(pParser->db, db->sharding->high[idx - 1], 0);
        pExprRight->pLeft->pTab = pTab;
    } else {
        pExprRight = NULL;
    }
    if (pExprLeft) {
        if (pExprRight) {
            pExprRight->pLeft->pTab = pTab;
            pExpr = sqlite3PExpr(pParser, TK_AND, pExprLeft, pExprRight, 0);
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

static int _colIndex(Table *pTab, const char *zCol)
{
    int j;
    Column *pCol;

    for (j = 0, pCol = pTab->aCol; j < pTab->nCol; j++, pCol++)
        if (sqlite3StrICmp(pCol->zName, zCol) == 0)
            return j;
    return -1;
}

static Expr *_create_low(Parse *pParser, struct Token *col, int iColumn,
                         ExprList *list, int shard)
{
    Expr *ret;

    assert(shard > 0);

    if (shard == 1)
        return NULL;
    if (shard - 2 >= list->nExpr)
        return NULL;

    ret = sqlite3PExpr(pParser, TK_GE,
                       /* column */
                       sqlite3ExprAlloc(pParser->db, TK_COLUMN, col, 0),
                       /* limit */
                       sqlite3ExprDup(pParser->db, list->a[shard - 2].pExpr, 0),
                       0);
    if (ret) {
        ret->pLeft->iColumn = iColumn;
    }

    return ret;
}

static Expr *_create_high(Parse *pParser, struct Token *col, int iColumn,
                          ExprList *list, int shard)
{
    Expr *ret;

    assert(shard > 0);

    /* no high limit for last shard */
    if (shard - 1 >= list->nExpr)
        return NULL;

    ret = sqlite3PExpr(pParser, TK_LT,
                       /* column */
                       sqlite3ExprAlloc(pParser->db, TK_COLUMN, col, 0),
                       /* limit */
                       sqlite3ExprDup(pParser->db, list->a[shard - 1].pExpr, 0),
                       0);
    if (ret) {
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
