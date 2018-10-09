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

#define ast_verbose bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DOHAST_VERBOSE)

char *generate_columns(sqlite3 *db, ExprList *c, const char **tbl)
{
    char *cols = NULL;
    Expr *expr = NULL;
    int i;

    for (i = 0; i < c->nExpr; i++) {
        expr = c->a[i].pExpr;
        if (expr->op != TK_COLUMN) {
            if (cols)
                sqlite3DbFree(db, cols);
            return NULL;
        }
        if (!cols)
            cols = sqlite3_mprintf("%s.\"%s\"%s%s%s", expr->pTab->zName,
                                   c->a[i].pExpr->u.zToken,
                                   (c->a[i].zName) ? " AS \"" : "",
                                   (c->a[i].zName) ? c->a[i].zName : "",
                                   (c->a[i].zName) ? "\" " : "");
        else
            cols = sqlite3_mprintf("%s, %s.\"%s\"%s%s%s", cols,
                                   expr->pTab->zName, c->a[i].pExpr->u.zToken,
                                   (c->a[i].zName) ? " AS \"" : "",
                                   (c->a[i].zName) ? c->a[i].zName : "",
                                   (c->a[i].zName) ? "\" " : "");
        if (!cols)
            return NULL;
        if (tbl)
            *tbl = expr->pTab->zName;
    }

    return cols;
}

static char *describeExprList(Vdbe *v, const ExprList *lst)
{
    char *ret;
    char *tmp;
    char *newterm;
    int i;

    ret = sqlite3ExprDescribe(v, lst->a[0].pExpr);
    if (!ret)
        return NULL;
    for (i = 1; i < lst->nExpr; i++) {
        newterm = sqlite3ExprDescribe(v, lst->a[i].pExpr);
        if (!newterm) {
            sqlite3DbFree(v->db, ret);
            return NULL;
        }
        tmp = sqlite3_mprintf("%s, %s", ret, newterm);
        sqlite3DbFree(v->db, newterm);
        sqlite3DbFree(v->db, ret);
        ret = tmp;
    }

    return ret;
}

char *sqlite_struct_to_string(Vdbe *v, Select *p, Expr *extraRows,
                              int *has_order)
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
        orderby = describeExprList(v, p->pOrderBy);
        if (!orderby) {
            sqlite3DbFree(db, where);
            return NULL;
        }
        *has_order = 1;
    }

    cols = generate_columns(db, p->pEList, &tbl);
    if (!cols) {
        sqlite3DbFree(db, orderby);
        sqlite3DbFree(db, where);
        return NULL;
    }

    if (!p->pLimit) {
        select = sqlite3_mprintf("SeLeCT %s FRoM %s%s%s%s%s", cols, tbl,
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
                "SeLeCT %s FRoM %s%s%s%s%s "
                "LiMit (CaSe wHeN (%s)<0 THeN (%s) eLSe ((%s) + "
                "(CaSe wHeN (%s)<0 THeN 0 eLSe (%s) eND)"
                ") eND) oFFSeT (%s)",
                cols, tbl, (where) ? " WHeRe " : "", (where) ? where : "",
                (orderby) ? " oRDeR By " : "", (orderby) ? orderby : "", limit,
                limit, limit, offset, offset, offset);
        } else if (!extraRows) {
            select = sqlite3_mprintf(
                "SeLeCT %s FRoM %s%s%s%s%s LiMit %s", cols, tbl,
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
                "SeLeCT %s FRoM %s%s%s%s%s LiMit (CaSe wHeN (%s)<0 THeN (%s) "
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

void ast_destroy(ast_t **ast)
{
    free((*ast)->stack);
    free(*ast);
    *ast = NULL;
}

static dohsql_node_t *gen_oneselect(Vdbe *v, Select *p, Expr *extraRows)
{
    dohsql_node_t *node;
    Select *prior = p->pPrior;
    Select *next = p->pNext;

    p->selFlags |= SF_ASTIncluded;

    node = (dohsql_node_t *)calloc(1, sizeof(dohsql_node_t));
    if (!node)
        return NULL;

    node->type = AST_TYPE_SELECT;
    p->pPrior = p->pNext = NULL;
    node->sql = sqlite_struct_to_string(v, p, extraRows, &node->has_order);
    p->pPrior = prior;
    p->pNext = next;

    node->ncols = p->pEList->nExpr;

    if (!node->sql) {
        free(node);
        node = NULL;
    }

    return node;
}

static void node_free(Vdbe *v, dohsql_node_t **pnode)
{
    dohsql_node_t *node = *pnode;
    int i;

    /* children */
    for (i = 0; i < node->nnodes && node->nodes[i]; i++) {
        node_free(v, &node->nodes[i]);
    }

    /* current node */
    if ((*pnode)->sql) {
        sqlite3DbFree(v->db, (*pnode)->sql);
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
    if (p->pOrderBy)
        node->has_order = 1;

    psub = node->nodes;

    pLimit = p->pLimit;
    pOffset = p->pOffset;
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
        *psub = gen_oneselect(v, crt, (pOffset != p->pOffset) ? pOffset : NULL);
        crt->pLimit = NULL;
        crt->pOffset = NULL;
        if (crt != p)
            crt->pOrderBy = NULL;

        if (!(*psub)) {
            node_free(v, &node);
            return NULL;
        }
        if (psub != node->nodes) {
            char *tmp =
                sqlite3_mprintf("%s uNioN aLL %s", (*psub)->sql, node->sql);
            sqlite3DbFree(v->db, node->sql);
            node->sql = tmp;
            if (!tmp) {
                node_free(v, &node);
                return NULL;
            }
        } else {
            node->sql = sqlite3_mprintf("%s", (*psub)->sql);
        }
        crt = crt->pNext;
        psub++;
    }
    p->pLimit = pLimit;
    p->pOffset = pOffset;

    return node;
}

static dohsql_node_t *gen_select(Vdbe *v, Select *p)
{
    Select *crt;
    int span = 0;
    dohsql_node_t *ret;

    p->selFlags |= SF_ASTIncluded;

    /* no joins or subqueries */
    if (p->pSrc->nSrc > 1 || p->pSrc->a->pSelect)
        return NULL;

    /* only handle union all */
    crt = p;
    while (crt) {
        span++;
        if (crt->op != TK_SELECT && crt->op != TK_ALL)
            return NULL;
        crt = crt->pPrior;
    }

    /* this is the insert rowset which links values on pNext!*/
    if (span == 1 && p->op == TK_ALL)
        return NULL;

    if (p->op == TK_SELECT)
        ret = gen_oneselect(v, p, NULL);
    else
        ret = gen_union(v, p, span);

    return ret;
}

int ast_push(ast_t *ast, enum ast_type op, Vdbe *v, void *obj)
{
    int ignore = 0;

    if (is_parallel_shard())
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

    if (ast_verbose && !ignore) {
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
    logmsg(LOGMSG_ERROR, "AST: [%d]\n", ast->nused);
    for (i = 0; i < ast->nused; i++)
        logmsg(LOGMSG_ERROR, "\t %d. %s \"%s\"\n", i,
               ast_type_str(ast->stack[i].op),
               ast_param_str(ast->stack[i].op, ast->stack[i].obj));
}

int comdb2_check_parallel(Parse *pParse)
{
    ast_t *ast = pParse->ast;
    dohsql_node_t *node;
    int i;

    if (ast->nused > 1)
        return 0;

    if (ast->stack[0].op != AST_TYPE_SELECT)
        return 0;
    if (!ast->stack[0].obj)
        return 0;

    node = (dohsql_node_t *)ast->stack[0].obj;

    if (strstr(node->sql, "sqlite_"))
        return 0;
    if (strstr(node->sql, "comdb2_"))
        return 0;

    if (pParse->explain && pParse->explain != 3)
        return 0;

    if (node->type == AST_TYPE_SELECT) {
        if (ast_verbose)
            logmsg(LOGMSG_DEBUG, "%lx Single query \"%s\"\n", pthread_self(),
                   node->sql);
        return 0;
    }

    if (node->type == AST_TYPE_UNION) {
        if (ast_verbose) {
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
