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
static void _save_params(Parse *pParse, dohsql_node_t *node);

static char *_gen_col_expr(Vdbe *v, Expr *expr, const char **tblname,
                           struct params_info **pParamsOut);

static char *generate_columns(Vdbe *v, ExprList *c, const char **tbl,
                              struct params_info **pParamsOut)
{
    char *cols = NULL;
    char *accum = NULL;
    Expr *expr = NULL;
    char *sExpr;
    int i;

    if (tbl)
        *tbl = NULL;
    for (i = 0; i < c->nExpr; i++) {
        expr = c->a[i].pExpr;
        if ((sExpr = _gen_col_expr(v, expr, tbl, pParamsOut)) == NULL) {
            if (cols)
                sqlite3_free(cols);
            return NULL;
        }
        if (!cols)
            cols = sqlite3_mprintf("%s%s%w%s", sExpr,
                                   (c->a[i].zName) ? " aS \"" : "",
                                   (c->a[i].zName) ? c->a[i].zName : "",
                                   (c->a[i].zName) ? "\" " : "");
        else {
            accum = sqlite3_mprintf("%s, %s%s%w%s", cols, sExpr,
                                    (c->a[i].zName) ? " aS \"" : "",
                                    (c->a[i].zName) ? c->a[i].zName : "",
                                    (c->a[i].zName) ? "\" " : "");
            sqlite3_free(cols);
            cols = accum;
        }
        sqlite3_free(sExpr);
        if (!cols)
            return NULL;
    }

    return cols;
}

static char *describeExprList(Vdbe *v, const ExprList *lst, int *order_size,
                              int **order_dir, struct params_info **pParamsOut,
                              int is_union)
{
    char *ret;
    char *tmp;
    char *newterm;
    int i;

    ret = sqlite3ExprDescribeParams(v, lst->a[0].pExpr, pParamsOut);
    if (!ret)
        return NULL;

    /* handle descending keys */
    *order_size = lst->nExpr;
    *order_dir = (int *)malloc((*order_size) * sizeof(int));
    if (!*order_dir) {
        sqlite3_free(ret);
        return NULL;
    }

    if (((*order_dir)[0] = lst->a[0].sortOrder) != 0) {
        tmp = sqlite3_mprintf("%s DeSC", ret);
        sqlite3_free(ret);
        ret = tmp;
    }
    for (i = 1; i < lst->nExpr; i++) {
        newterm = sqlite3ExprDescribeParams(v, lst->a[i].pExpr, pParamsOut);
        if (!newterm) {
            sqlite3_free(ret);
            if (*order_dir)
                free(*order_dir);
            return NULL;
        }
        tmp = sqlite3_mprintf(
            "%s, %s%s", ret, newterm,
            (((*order_dir)[i] = lst->a[i].sortOrder) != 0) ? " DeSC" : "");
        sqlite3_free(newterm);
        sqlite3_free(ret);
        ret = tmp;
    }

    if (is_union) {
        /* restriction allows us direct indexing in result set */
        for (i = 0; i < lst->nExpr; i++) {
            (*order_dir)[i] =
                lst->a[i].u.x.iOrderByCol * (((*order_dir)[i]) ? -1 : 1);
        }
    }

    return ret;
}

char *sqlite_struct_to_string(Vdbe *v, Select *p, Expr *extraRows,
                              int *order_size, int **order_dir,
                              struct params_info **pParamsOut, int is_union)
{
    char *cols = NULL;
    const char *tbl = NULL;
    char *select = NULL;
    char *where = NULL;
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
        where = sqlite3ExprDescribeParams(v, p->pWhere, pParamsOut);
        if (!where)
            return NULL;
    }

    if (p->pOrderBy) {
        orderby = describeExprList(v, p->pOrderBy, order_size, order_dir,
                                   pParamsOut, is_union);
        if (!orderby) {
            sqlite3_free(where);
            return NULL;
        }
    }

    cols = generate_columns(v, p->pEList, &tbl, pParamsOut);
    if (!cols) {
        sqlite3_free(orderby);
        sqlite3_free(where);
        return NULL;
    }
    if (!tbl && p->pSrc->nSrc) {
        /* select 1 from tbl */
        tbl = (const char *)p->pSrc->a[0].zName;
    }

    if (unlikely(!tbl)) {
        select =
            sqlite3_mprintf("SeLeCT %s%s%s%s%s", cols, (where) ? " WHeRe " : "",
                            (where) ? where : "", (orderby) ? " oRDeR By " : "",
                            (orderby) ? orderby : "");
    } else if (!p->pLimit) {
        select = sqlite3_mprintf("SeLeCT %s FRoM \"%w\"%s%s%s%s", cols, tbl,
                                 (where) ? " WHeRe " : "", (where) ? where : "",
                                 (orderby) ? " oRDeR By " : "",
                                 (orderby) ? orderby : "");
    } else {
        limit = sqlite3ExprDescribeParams(v, p->pLimit->pLeft, pParamsOut);
        if (!limit) {
            sqlite3_free(orderby);
            sqlite3_free(where);
            sqlite3_free(cols);
            return NULL;
        }
        if (/* p->pLimit && */ p->pLimit->pRight) {
            offset =
                sqlite3ExprDescribeParams(v, p->pLimit->pRight, pParamsOut);
            if (!offset) {
                sqlite3_free(limit);
                sqlite3_free(orderby);
                sqlite3_free(where);
                sqlite3_free(cols);
                return NULL;
            }
            select = sqlite3_mprintf(
                "SeLeCT %s FRoM \"%w\"%s%s%s%s "
                "LiMit (CaSe wHeN (%s)<0 THeN (%s) eLSe ((%s) + "
                "(CaSe wHeN (%s)<0 THeN 0 eLSe (%s) eND)"
                ") eND) oFFSeT (%s)",
                cols, tbl, (where) ? " WHeRe " : "", (where) ? where : "",
                (orderby) ? " oRDeR By " : "", (orderby) ? orderby : "", limit,
                limit, limit, offset, offset, offset);
        } else if (!extraRows) {
            select = sqlite3_mprintf(
                "SeLeCT %s FRoM \"%w\"%s%s%s%s LiMit %s", cols, tbl,
                (where) ? " WHeRe " : "", (where) ? where : "",
                (orderby) ? " oRDeR By " : "", (orderby) ? orderby : "", limit);
        } else {
            extra = sqlite3ExprDescribeParams(v, extraRows, pParamsOut);
            if (!extra) {
                sqlite3_free(limit);
                sqlite3_free(orderby);
                sqlite3_free(where);
                sqlite3_free(cols);
                return NULL;
            }
            select = sqlite3_mprintf(
                "SeLeCT %s FRoM \"%w\"%s%s%s%s LiMit (CaSe wHeN (%s)<0 THeN "
                "(%s) "
                "eLSe ((%s) + "
                "(CaSe wHeN (%s)<0 THeN 0 eLSe (%s) eND)"
                ") eND)",
                cols, tbl, (where) ? " WHeRe " : "", (where) ? where : "",
                (orderby) ? " oRDeR By " : "", (orderby) ? orderby : "", limit,
                limit, limit, extra, extra);
        }
    }

    sqlite3_free(cols);
    if (extra)
        sqlite3_free(extra);
    if (offset)
        sqlite3_free(offset);
    if (limit)
        sqlite3_free(limit);
    if (orderby)
        sqlite3_free(orderby);
    if (where)
        sqlite3_free(where);

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
    int unsupported;
};

ast_t *ast_init(Parse *pParse, const char *caller)
{
    ast_t *ast;

    if (pParse->ast)
        return pParse->ast->unsupported ? NULL : pParse->ast;

    if (gbl_dohast_verbose)
        logmsg(LOGMSG_USER, "TTT: %lu %s from %s\n", pthread_self(), __func__,
               caller);

    if (!sqlite3IsToplevel(pParse)) {
        ast = ast_init(sqlite3ParseToplevel(pParse), __func__);
        if (ast)
            ast->unsupported = 1;
        return NULL;
    }

    ast = calloc(1, sizeof(ast_t));
    if (!ast)
        return NULL;
    ast->nalloc = AST_STACK_INIT;
    ast->stack = calloc(ast->nalloc, sizeof(ast_node_t));
    if (!ast->stack) {
        free(ast);
        ast = NULL;
    }

    return pParse->ast = ast;
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
                                    int *order_size, int **order_dir,
                                    int is_union)
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
    node->sql = sqlite_struct_to_string(v, p, extraRows, order_size, order_dir,
                                        &node->params, is_union);
    p->pPrior = prior;
    p->pNext = next;

    node->ncols = p->pEList->nExpr;

    if (!node->sql) {
        if (node->params) {
            free(node->params->params);
            free(node->params);
        }
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

    /* params */
    if ((*pnode)->params) {
        free((*pnode)->params->params);
        free((*pnode)->params);
    }

    /* current node */
    if ((*pnode)->sql) {
        sqlite3_free((*pnode)->sql);
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
    Expr *pLimitNoOffset = NULL;
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
    pOffset = p->pLimit ? p->pLimit->pRight : NULL;

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
        if (pLimit && pLimit->pRight) {
            Expr *pSavedRight = pLimit->pRight;
            pLimit->pRight = 0;
            pLimitNoOffset = sqlite3ExprDup(v->db, pLimit, 0);
            pLimit->pRight = pSavedRight;
        } else {
            pLimitNoOffset = pLimit;
        }

        /* go from left to right */
        while (crt->pPrior) {
            assert(crt == p || !crt->pLimit); /* can "restore" to NULL? */
            crt->pLimit = pLimitNoOffset;
            crt = crt->pPrior;
        }
        crt->pLimit = pLimit; /* we need this in the head */
    }

    /* generate queries */
    while (crt) {
        assert(crt == p || !crt->pOrderBy); /* can "restore" to NULL? */
        crt->pOrderBy = p->pOrderBy;
        *psub = gen_oneselect(v, crt, pOffset, &node->order_size,
                              &node->order_dir, 1);
        crt->pLimit = NULL;
        if (crt != p)
            crt->pOrderBy = NULL;

        if (!(*psub)) {
            node_free(&node, v->db);
            goto done;
        }
        if (psub != node->nodes) {
            char *tmp =
                sqlite3_mprintf("%s uNioN aLL %s", (*psub)->sql, node->sql);
            sqlite3_free(node->sql);
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
    crt = p;
    while (crt) {
        crt->pLimit = NULL;
        crt = crt->pPrior;
    }
    p->pLimit = pLimit;

    if (pLimitNoOffset != NULL && pLimitNoOffset != pLimit) {
        sqlite3ExprDelete(v->db, pLimitNoOffset);
    }

#ifdef SQLITE_DEBUG
    crt = p;
    while (crt->pPrior) {
        if (crt->pLimit) {
            logmsg(LOGMSG_DEBUG,
                   "%s: Select %p has Limit %p, orig %p, no offset %p\n",
                   __func__, crt, crt->pLimit, pLimit, pLimitNoOffset);
        }
        crt = crt->pPrior;
    }
#endif

    return node;
}

static int skip_tables(Select *p)
{
    int i;
    int j;
    char *ignored[] = {"comdb2_", "sqlite_", NULL};
    int lens[] = {7, 7, 0};

    for (i = 0; i < p->pSrc->nSrc; i++) {
        if (!p->pSrc->a[i].zName)
            continue; /* skip subqueries */
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
        if ((crt->op != TK_SELECT && crt->op != TK_ALL) || crt->recording)
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
        ret = gen_oneselect(v, p, NULL, NULL, NULL, 0);
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
    if (gbl_dohast_verbose) {
        logmsg(LOGMSG_USER, "AST: [%d]\n", ast->nused);
        for (i = 0; i < ast->nused; i++)
            logmsg(LOGMSG_USER, "\t %d. %s \"%s\"\n", i,
                    ast_type_str(ast->stack[i].op),
                    ast_param_str(ast->stack[i].op, ast->stack[i].obj));
    }
}

extern int comdb2IsPrepareOnly(Parse*);

int comdb2_check_parallel(Parse *pParse)
{
    if (comdb2IsPrepareOnly(pParse))
        return 0;

    ast_t *ast = pParse->ast;
    dohsql_node_t *node;
    int i;

    if (gbl_dohsql_disable)
        return 0;

    if (ast && ast->unsupported)
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

    if (pParse->explain) {
        if (pParse->explain == 3)
            explain_distribution(node);
        return 0;
    }

    if (node->type == AST_TYPE_SELECT) {
        if (gbl_dohast_verbose)
            logmsg(LOGMSG_USER, "%lx Single query \"%s\"\n", pthread_self(),
                   node->sql);
        return 0;
    }

    if (node->type == AST_TYPE_UNION) {
        _save_params(pParse, node);

        if (gbl_dohast_verbose) {
            logmsg(LOGMSG_USER, "%lx Parallelizable union %d threads:\n",
                   pthread_self(), node->nnodes);
            for (i = 0; i < node->nnodes; i++) {
                logmsg(LOGMSG_USER, "\t Thread %d: \"%s\"\n", i + 1,
                       node->nodes[i]->sql);
                if (node->nodes[i]->params) {
                    logmsg(LOGMSG_USER, "\t\t Params %d:\n",
                           node->nodes[i]->params->nparams);
                    for (int j = 0; j < node->nodes[i]->params->nparams; j++) {
                        logmsg(LOGMSG_USER, "\t\t\t %d \"%s\" %d\n", j + 1,
                               node->nodes[i]->params->params[j].name,
                               node->nodes[i]->params->params[j].pos);
                    }
                }
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
        pWalker->pParse = (Parse *)pExpr->y.pTab->zName;
    case TK_PLUS:
    case TK_MINUS:
    case TK_INTEGER:
    case TK_STRING:
        return WRC_Continue;
    case TK_FUNCTION:
        if (strcasecmp(pExpr->u.zToken, "comdb2_sysinfo") == 0) {
            return WRC_Continue;
        }
        /* fallthrough */
    default:
        return WRC_Abort;
    }
}

static int _selectCallback(Walker *pWalker, Select *pSelect)
{
    return WRC_Continue;
}

static char *_gen_col_expr(Vdbe *v, Expr *expr, const char **tblname,
                           struct params_info **pParamsOut)
{
    Walker w = {0};

    w.pParse = NULL;
    w.xExprCallback = _exprCallback;
    w.xSelectCallback = _selectCallback;
    if (sqlite3WalkExpr(&w, expr) != WRC_Continue)
        return NULL;

    if (tblname && w.pParse)
        *tblname = (const char *)w.pParse;

    return sqlite3ExprDescribeParams(v, expr, pParamsOut);
}

static void _save_params(Parse *pParse, dohsql_node_t *node)
{
    Vdbe *v = pParse->db->pVdbe;
    if (!v)
        return;

    if (gbl_dohast_verbose) {
        logmsg(LOGMSG_USER, "%lx Caching bound parameters length %p\n",
                pthread_self(), v->pVList);
        sqlite3VListPrint(LOGMSG_USER, v->pVList);
    }

    node->nparams = sqlite3_bind_parameter_count((sqlite3_stmt *)v);
    /* we don't really need node->params for parent union at this point */
}
