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
#include "sqliteInt.h"
#include "vdbeInt.h"
#include "ast.h"
#include "dohsql.h"
#include "sql.h"

int gbl_dohast_disable = 0;
int gbl_dohast_verbose = 0;

static void node_free(dohsql_node_t **pnode, sqlite3 *db);

static char *_gen_col_expr(Vdbe *v, Expr *expr, const char **tblname);

static char *generate_columns(Vdbe *v, ExprList *c, const char **tbl)
{
    sqlite3 *db = v->db;
    char *cols = NULL;
    char *accum = NULL;
    Expr *expr = NULL;
    char *sExpr;
    int i;

    if (tbl)
        *tbl = NULL;
    for (i = 0; i < c->nExpr; i++) {
        expr = c->a[i].pExpr;
        if ((sExpr = _gen_col_expr(v, expr, tbl)) == NULL) {
            if (cols)
                sqlite3DbFree(db, cols);
            return NULL;
        }
        if (!cols)
            cols = sqlite3_mprintf("%s%s%s%s", sExpr,
                                   (c->a[i].zName) ? " aS \"" : "",
                                   (c->a[i].zName) ? c->a[i].zName : "",
                                   (c->a[i].zName) ? "\" " : "");
        else {
            accum = sqlite3_mprintf("%s, %s%s%s%s", cols, sExpr,
                                    (c->a[i].zName) ? " aS \"" : "",
                                    (c->a[i].zName) ? c->a[i].zName : "",
                                    (c->a[i].zName) ? "\" " : "");
            sqlite3DbFree(db, cols);
            cols = accum;
        }
        sqlite3DbFree(db, sExpr);
        if (!cols)
            return NULL;
    }

    return cols;
}

static char *describeExprList(Vdbe *v, const ExprList *lst, int *order_size,
                              int **order_dir)
{
    char *ret;
    char *tmp;
    char *newterm;
    int i;

    ret = sqlite3ExprDescribe(v, lst->a[0].pExpr);
    if (!ret)
        return NULL;

    /* handle descending keys */
    *order_size = lst->nExpr;
    *order_dir = (int *)malloc((*order_size) * sizeof(int));
    if (!*order_dir) {
        sqlite3DbFree(v->db, ret);
        return NULL;
    }

    if (((*order_dir)[0] = lst->a[0].sortOrder) != 0) {
        tmp = sqlite3_mprintf("%s DeSC", ret);
        sqlite3DbFree(v->db, ret);
        ret = tmp;
    }
    for (i = 1; i < lst->nExpr; i++) {
        newterm = sqlite3ExprDescribe(v, lst->a[i].pExpr);
        if (!newterm) {
            sqlite3DbFree(v->db, ret);
            if (*order_dir)
                free(*order_dir);
            return NULL;
        }
        tmp = sqlite3_mprintf(
            "%s, %s%s", ret, newterm,
            (((*order_dir)[i] = lst->a[i].sortOrder) != 0) ? " DeSC" : "");
        sqlite3DbFree(v->db, newterm);
        sqlite3DbFree(v->db, ret);
        ret = tmp;
    }

    return ret;
}

char *sqlite_struct_to_string(Vdbe *v, Select *p, Expr *extraRows,
                              int *order_size, int **order_dir)
{
    char *cols = NULL;
    const char *tbl = NULL;
    char *select = NULL;
    char *where = NULL;
    sqlite3 *db = v->db;
    char *limit = NULL;
    char *offset = NULL;
    char *extra = NULL;
    char *orderby = NULL;

    if (p->recording)
        return NULL; /* no selectv */
    if (p->pWith)
        return NULL; /* no CTE */
    if (p->pHaving)
        return NULL; /* no having */
    if (p->pGroupBy)
        return NULL; /* no group by */
    if (p->pSrc->nSrc > 1)
        return NULL; /* no joins */

    if (p->pPrior && p->op != TK_ALL)
        return NULL; /* only union all */

    if (p->pWhere) {
        where = sqlite3ExprDescribe(v, p->pWhere);
        if (!where)
            return NULL;
    }

    if (p->pOrderBy) {
        orderby = describeExprList(v, p->pOrderBy, order_size, order_dir);
        if (!orderby) {
            sqlite3DbFree(db, where);
            return NULL;
        }
    }

    cols = generate_columns(v, p->pEList, &tbl);
    if (!cols) {
        sqlite3DbFree(db, orderby);
        sqlite3DbFree(db, where);
        return NULL;
    }

    if (unlikely(!tbl)) {
        select =
            sqlite3_mprintf("SeLeCT %s%s%s%s%s", cols, (where) ? " WHeRe " : "",
                            (where) ? where : "", (orderby) ? " oRDeR By " : "",
                            (orderby) ? orderby : "");
    } else if (!p->pLimit) {
        select = sqlite3_mprintf("SeLeCT %s FRoM \"%s\"%s%s%s%s", cols, tbl,
                                 (where) ? " WHeRe " : "", (where) ? where : "",
                                 (orderby) ? " oRDeR By " : "",
                                 (orderby) ? orderby : "");
    } else {
        limit = sqlite3ExprDescribe(v, p->pLimit);
        if (!limit) {
            sqlite3DbFree(db, orderby);
            sqlite3DbFree(db, where);
            sqlite3DbFree(db, cols);
            return NULL;
        }
        if (p->pOffset) {
            offset = sqlite3ExprDescribe(v, p->pOffset);
            if (!offset) {
                sqlite3DbFree(db, limit);
                sqlite3DbFree(db, orderby);
                sqlite3DbFree(db, where);
                sqlite3DbFree(db, cols);
                return NULL;
            }
            select = sqlite3_mprintf(
                "SeLeCT %s FRoM \"%s\"%s%s%s%s "
                "LiMit (CaSe wHeN (%s)<0 THeN (%s) eLSe ((%s) + "
                "(CaSe wHeN (%s)<0 THeN 0 eLSe (%s) eND)"
                ") eND) oFFSeT (%s)",
                cols, tbl, (where) ? " WHeRe " : "", (where) ? where : "",
                (orderby) ? " oRDeR By " : "", (orderby) ? orderby : "", limit,
                limit, limit, offset, offset, offset);
        } else if (!extraRows) {
            select = sqlite3_mprintf(
                "SeLeCT %s FRoM \"%s\"%s%s%s%s LiMit %s", cols, tbl,
                (where) ? " WHeRe " : "", (where) ? where : "",
                (orderby) ? " oRDeR By " : "", (orderby) ? orderby : "", limit);
        } else {
            extra = sqlite3ExprDescribe(v, extraRows);
            if (!extra) {
                sqlite3DbFree(db, limit);
                sqlite3DbFree(db, orderby);
                sqlite3DbFree(db, where);
                sqlite3DbFree(db, cols);
                return NULL;
            }
            select = sqlite3_mprintf(
                "SeLeCT %s FRoM \"%s\"%s%s%s%s LiMit (CaSe wHeN (%s)<0 THeN "
                "(%s) "
                "eLSe ((%s) + "
                "(CaSe wHeN (%s)<0 THeN 0 eLSe (%s) eND)"
                ") eND)",
                cols, tbl, (where) ? " WHeRe " : "", (where) ? where : "",
                (orderby) ? " oRDeR By " : "", (orderby) ? orderby : "", limit,
                limit, limit, extra, extra);
        }
    }

    sqlite3DbFree(db, cols);
    if (extra)
        sqlite3DbFree(db, extra);
    if (offset)
        sqlite3DbFree(db, offset);
    if (limit)
        sqlite3DbFree(db, limit);
    if (orderby)
        sqlite3DbFree(db, orderby);
    if (where)
        sqlite3DbFree(db, where);

    return select;
}

struct ast_node {
    enum ast_type op;
    void *obj;
};
typedef struct ast_node ast_node_t;

#define AST_STACK_INIT 10
struct ast {
    ast_node_t *stack;
    int nused;
    int nalloc;
};

ast_t *ast_init(void)
{
    ast_t *ast;

    ast = calloc(1, sizeof(ast_t));
    if (!ast)
        return NULL;

    ast->nused = 0;
    ast->nalloc = AST_STACK_INIT;
    ast->stack = calloc(ast->nalloc, sizeof(ast_node_t));
    if (!ast->stack) {
        free(ast);
        ast = NULL;
    }

    return ast;
}

void ast_destroy(ast_t **past, sqlite3 *db)
{
    int i;
    ast_t *ast = *past;

    *past = NULL;
    for (i = 0; i < ast->nused; i++) {
        if (ast->stack[i].obj) {
            switch (ast->stack[i].op) {
            case AST_TYPE_SELECT:
            case AST_TYPE_UNION: {
                node_free((dohsql_node_t **)&ast->stack[i].obj, db);
                break;
            }
            }
        }
    }
    free(ast->stack);
    free(ast);
}

static dohsql_node_t *gen_oneselect(Vdbe *v, Select *p, Expr *extraRows,
                                    int *order_size, int **order_dir)
{
    dohsql_node_t *node;
    Select *prior = p->pPrior;
    Select *next = p->pNext;

    p->selFlags |= SF_ASTIncluded;

    node = (dohsql_node_t *)calloc(1, sizeof(dohsql_node_t));
    if (!node)
        return NULL;

    if (!order_size) {
        order_size = &node->order_size;
        order_dir = &node->order_dir;
    }

    node->type = AST_TYPE_SELECT;
    p->pPrior = p->pNext = NULL;
    node->sql = sqlite_struct_to_string(v, p, extraRows, order_size, order_dir);
    p->pPrior = prior;
    p->pNext = next;

    node->ncols = p->pEList->nExpr;

    if (!node->sql) {
        free(node);
        node = NULL;
    }

    return node;
}

static void node_free(dohsql_node_t **pnode, sqlite3 *db)
{
    dohsql_node_t *node = *pnode;
    int i;

    /* children */
    for (i = 0; i < node->nnodes && node->nodes[i]; i++) {
        node_free(&node->nodes[i], db);
    }

    /* current node */
    if ((*pnode)->sql) {
        sqlite3DbFree(db, (*pnode)->sql);
    }
    if ((*pnode)->order_dir) {
        free((*pnode)->order_dir);
    }
    free(*pnode);
    *pnode = NULL;
}

static dohsql_node_t *gen_union(Vdbe *v, Select *p, int span)
{
    dohsql_node_t *node;
    dohsql_node_t **psub;
    Select *crt;
    Expr *pLimit = NULL;
    Expr *pOffset = NULL;

    assert(p->op == TK_ALL && span > 1);

    node = (dohsql_node_t *)calloc(1, sizeof(dohsql_node_t) +
                                          span * sizeof(void *));
    if (!node)
        return NULL;

    node->type = AST_TYPE_UNION;
    node->nodes = (dohsql_node_t **)(node + 1);
    node->nnodes = span;
    node->ncols = p->pEList->nExpr;

    psub = node->nodes;

    pLimit = p->pLimit;
    pOffset = p->pOffset;
    /* syntax errors */
    crt = p->pPrior;
    while (crt) {
        /* no limit in the middle */
        if (crt->pLimit)
            return NULL;
        /* no order by in the middle */
        if (crt->pOrderBy)
            return NULL;
        crt = crt->pPrior;
    }

    crt = p;
    if (crt->pPrior) {

        /* go from left to right */
        while (crt->pPrior) {
            crt->pLimit = pLimit;
            crt = crt->pPrior;
        }
        crt->pLimit = pLimit;
        crt->pOffset = pOffset;
        p->pOffset = NULL;
    }

    /* generate queries */
    while (crt) {
        crt->pOrderBy = p->pOrderBy;
        *psub = gen_oneselect(v, crt, (pOffset != p->pOffset) ? pOffset : NULL,
                              &node->order_size, &node->order_dir);
        crt->pLimit = NULL;
        crt->pOffset = NULL;
        if (crt != p)
            crt->pOrderBy = NULL;

        if (!(*psub)) {
            node_free(&node, v->db);
            goto done;
        }
        if (psub != node->nodes) {
            char *tmp =
                sqlite3_mprintf("%s uNioN aLL %s", (*psub)->sql, node->sql);
            sqlite3DbFree(v->db, node->sql);
            node->sql = tmp;
            if (!tmp) {
                node_free(&node, v->db);
                goto done;
            }
        } else {
            node->sql = sqlite3_mprintf("%s", (*psub)->sql);
        }
        crt = crt->pNext;
        psub++;
    }
done:
    p->pLimit = pLimit;
    p->pOffset = pOffset;

    return node;
}

static int skip_tables(Select *p)
{
    int i;
    int j;
    char *ignored[] = {"comdb2_", "sqlite_", NULL};
    int lens[] = {7, 7, 0};

    for (i = 0; i < p->pSrc->nSrc; i++) {
        if(!p->pSrc->a[i].zName) continue; /* skip subqueries */
        j = 0;
        while (ignored[j]) {
            if (strncasecmp(p->pSrc->a[i].zName, ignored[j], lens[j]) == 0)
                return 1;
            j++;
        }
    }
    return 0;
}

static dohsql_node_t *gen_select(Vdbe *v, Select *p)
{
    Select *crt;
    int span = 0;
    dohsql_node_t *ret = NULL;
    int not_recognized = 0;

    /* mark everything done, either way or another */
    crt = p;
    while (crt) {
        crt->selFlags |= SF_ASTIncluded;
        span++;
        /* only handle union all */
        if (crt->op != TK_SELECT && crt->op != TK_ALL)
            not_recognized = 1;

        /* skip certain tables */
        if (skip_tables(crt))
            not_recognized = 1;

        crt = crt->pPrior;
    }

    /* no with, joins or subqueries */
    if (not_recognized || p->pSrc->nSrc == 0 /*with*/ ||
        p->pSrc->nSrc > 1 /*joins*/ || p->pSrc->a->pSelect /*subquery*/ ||
        (span == 1 &&
         p->op == TK_ALL) /* insert rowset which links values on pNext */
    )
        return NULL;

    if (p->op == TK_SELECT)
        ret = gen_oneselect(v, p, NULL, NULL, NULL);
    else
        ret = gen_union(v, p, span);

    return ret;
}

int ast_push(ast_t *ast, enum ast_type op, Vdbe *v, void *obj)
{
    int ignore = 0;

    if (gbl_dohast_disable)
        return 0;

    if (dohsql_is_parallel_shard())
        return 0;

    if (ast->nused >= ast->nalloc) {
        ast->stack = realloc(ast->stack, (ast->nalloc + AST_STACK_INIT) *
                                             sizeof(ast->stack[0]));
        if (!ast->stack)
            return -1;
        bzero(&ast->stack[ast->nalloc], AST_STACK_INIT * sizeof(ast->stack[0]));
        ast->nalloc += AST_STACK_INIT;
    }

    switch (op) {
    case AST_TYPE_SELECT: {
        Select *p = (Select *)obj;

        if ((p->selFlags & SF_ASTIncluded) == 0) {
            ast->stack[ast->nused].op = op;
            ast->stack[ast->nused].obj = gen_select(v, p);
            ast->nused++;
        } else {
            ignore = 1;
        }
        break;
    }
    default: {
        ast->stack[ast->nused].op = op;
        ast->stack[ast->nused].obj = obj;
        ast->nused++;
        break;
    }
    }

    if (gbl_dohast_verbose && !ignore) {
        ast_print(ast);
    }

    return 0;
}

int ast_pop(ast_t *ast, ast_node_t **pnode)
{
    if (ast->nused > 0) {
        ast->nused--;
        *pnode = &ast->stack[ast->nused];
    } else
        return -1;

    return ast->nused;
}

const char *ast_type_str(enum ast_type type)
{
    switch (type) {
    case AST_TYPE_SELECT:
        return "SELECT";
    case AST_TYPE_INSERT:
        return "INSERT";
    case AST_TYPE_IN:
        return "IN_SELECT";
    case AST_TYPE_DELETE:
        return "DELETE";
    case AST_TYPE_UPDATE:
        return "UPDATE";
    default:
        return "INVALID";
    }
}

const char *ast_param_str(enum ast_type type, void *obj)
{
    switch (type) {
    case AST_TYPE_SELECT:
        return obj ? ((dohsql_node_t *)obj)->sql : "NULL";
    case AST_TYPE_INSERT:
        return "()";
    case AST_TYPE_IN:
        return "()";
    case AST_TYPE_DELETE:
        return "()";
    case AST_TYPE_UPDATE:
        return "()";
    default:
        return "INVALID";
    }
}

void ast_print(ast_t *ast)
{
    int i;
    logmsg(LOGMSG_DEBUG, "AST: [%d]\n", ast->nused);
    for (i = 0; i < ast->nused; i++)
        logmsg(LOGMSG_DEBUG, "\t %d. %s \"%s\"\n", i,
               ast_type_str(ast->stack[i].op),
               ast_param_str(ast->stack[i].op, ast->stack[i].obj));
}

int comdb2_check_parallel(Parse *pParse)
{
    ast_t *ast = pParse->ast;
    dohsql_node_t *node;
    int i;

    if (gbl_dohsql_disable)
        return 0;

    if (has_parallel_sql(NULL) == 0)
        return 0;

    if (ast->nused > 1)
        return 0;

    if (ast->stack[0].op != AST_TYPE_SELECT)
        return 0;
    if (!ast->stack[0].obj)
        return 0;

    node = (dohsql_node_t *)ast->stack[0].obj;

    if (pParse->explain && pParse->explain != 3)
        return 0;

    if (node->type == AST_TYPE_SELECT) {
        if (gbl_dohast_verbose)
            logmsg(LOGMSG_DEBUG, "%lx Single query \"%s\"\n", pthread_self(),
                   node->sql);
        return 0;
    }

    if (node->type == AST_TYPE_UNION) {
        if (gbl_dohast_verbose) {
            logmsg(LOGMSG_DEBUG, "%lx Parallelizable union %d threads:\n",
                   pthread_self(), node->nnodes);
            for (i = 0; i < node->nnodes; i++) {
                logmsg(LOGMSG_DEBUG, "\t Thread %d: \"%s\"\n", i + 1,
                       node->nodes[i]->sql);
            }
        }

        if (!pParse->explain) {
            if (dohsql_distribute(node))
                return 0;
            return 1;
        } else {
            pParse->explain = 2;
        }
    }
    return 0;
}

static int _exprCallback(Walker *pWalker, Expr *pExpr)
{
    switch (pExpr->op) {
    case TK_COLUMN:
        if (pWalker->pParse)
            return WRC_Abort;
        pWalker->pParse = (Parse *)pExpr->pTab->zName;
    case TK_PLUS:
    case TK_MINUS:
    case TK_INTEGER:
        return WRC_Continue;
    default:
        return WRC_Abort;
    }
}

static int _selectCallback(Walker *pWalker, Select *pSelect)
{
    return WRC_Continue;
}

static char *_gen_col_expr(Vdbe *v, Expr *expr, const char **tblname)
{
    Walker w = {0};

    w.pParse = NULL;
    w.xExprCallback = _exprCallback;
    w.xSelectCallback = _selectCallback;
    if (sqlite3WalkExpr(&w, expr) != WRC_Continue)
        return NULL;

    if (tblname && w.pParse)
        *tblname = (const char *)w.pParse;

    return sqlite3ExprDescribe(v, expr);
}
