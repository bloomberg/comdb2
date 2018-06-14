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

#include "sqliteInt.h"
#include "vdbeInt.h"
#include "dhrite.h"


static int ast_verbouse = 1;

char *generate_columns(sqlite3 *db, ExprList *c, const char **tbl)
{
    char *cols = NULL;
    Expr *expr = NULL;
    int i;

    for (i=0;i<c->nExpr;i++) {
        expr = c->a[i].pExpr;
        if (expr->op != TK_COLUMN) {
            if (cols) sqlite3DbFree(db, cols);
            return NULL;
        }
        if (!cols)
            cols = sqlite3_mprintf("%s.\"%s\"",
                    expr->pTab->zName, c->a[i].pExpr->u.zToken);
        else
            cols = sqlite3_mprintf("%s, %s.\"%s\"",
                    cols, expr->pTab->zName, 
                    c->a[i].pExpr->u.zToken);
        if (!cols) return NULL;
        if(tbl) *tbl = expr->pTab->zName;
    }

    return cols;
}

char* sqlite_struct_to_string(Vdbe *v, Select *p)
{
    char *cols = NULL;
    const char *tbl = NULL;
    char *select = NULL;
    char *selectPrior = NULL;
    char *where = NULL;
    sqlite3 *db = v->db;

    if (p->recording) return NULL; /* no selectv */
    if (p->pWith) return NULL; /* no CTE */
    if (p->pOffset) return NULL; /* no Offset */
    if (p->pLimit) return NULL; /* no limit */
    if (p->pOrderBy) return NULL; /* no order by */
    if (p->pHaving) return NULL; /* no having */
    if (p->pGroupBy) return NULL; /* no group by */
    if (p->pSrc->nSrc>1) return NULL; /* no joins */
    if (p->pPrior &&  p->op != TK_ALL) return NULL; /* only union all */

    if (p->pPrior) {
        selectPrior = sqlite_struct_to_string(v, p->pPrior);
    }

    if (p->pWhere) {
        where = sqlite3ExprDescribe(v, p->pWhere);
        if (!where) return NULL;
    }

    cols = generate_columns(db, p->pEList, &tbl);
    if(!cols) {
        if (where) sqlite3DbFree(db, where);
        return NULL;
    }

    select = sqlite3_mprintf("%s%sSeLeCT %s FRoM %s%s%s",
            (selectPrior)?selectPrior:"", (selectPrior)?" uNioN aLL ":"",
            cols, tbl,
            (where)?" WHeRe ":"", (where)?where:"");

    sqlite3DbFree(db, cols);
    if (where) sqlite3DbFree(db, where);
    if (selectPrior) sqlite3DbFree(db, where);

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

ast_t* ast_init(void)
{
    ast_t *ast;

    ast = malloc(sizeof ast);
    if(!ast) return NULL;

    ast->nused = 0;
    ast->nalloc = AST_STACK_INIT;
    ast->stack = calloc(ast->nalloc, sizeof(ast_node_t));
    if(!ast->stack) {
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

int ast_push(ast_t *ast, enum ast_type op, void* obj)
{
    if (ast->nused>=ast->nalloc) {
        ast->stack = realloc(ast->stack, ast->nalloc+AST_STACK_INIT);
        if (!ast->stack)
            return -1;
        ast->nalloc += AST_STACK_INIT;
    }

    ast->stack[ast->nused].op = op;
    ast->stack[ast->nused].obj = obj;
    ast->nused++;


    if(ast_verbouse) {
        ast_print(ast);
    }

    return 0;
}

int ast_pop(ast_t *ast, ast_node_t **pnode)
{
    if (ast->nused>0) {
        ast->nused--;
        *pnode = &ast->stack[ast->nused];
    } else return -1;

    return ast->nused;
}

const char* ast_type_str(enum ast_type type)
{
    switch(type) {
    case AST_TYPE_SELECT: return "SELECT";
    case AST_TYPE_INSERT: return "INSERT";
    }
    return "INVALID";
}

void ast_print(ast_t *ast)
{
    int i;
    logmsg(LOGMSG_ERROR, "AST: [%d]\n", ast->nused);
    for(i=0;i<ast->nused;i++)
        logmsg(LOGMSG_ERROR, "\t %d. %s\n", i, ast_type_str(ast->stack[i].op));
}

