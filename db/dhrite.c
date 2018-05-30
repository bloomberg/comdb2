#include "sqliteInt.h"
#include "vdbeInt.h"

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
                    expr->pTab->zName, c->a[i].zName);
        else
            cols = sqlite3_mprintf("%s, %s.\"%s\"",
                    cols, expr->pTab->zName, c->a[i].zName);
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
    char *where = NULL;
    sqlite3 *db = v->db;

    if (p->recording) return NULL; /* no selectv */
    if (p->pWith) return NULL; /* no CTE */
    if (p->pOffset) return NULL; /* no Offset */
    if (p->pLimit) return NULL; /* no limit */
    if (p->pOrderBy) return NULL; /* no order by */
    if (p->pHaving) return NULL; /* no having */
    if (p->pGroupBy) return NULL; /* no group by */
    /*if (p->pWhere) return NULL;  no predicates */
    if (p->pPrior || p->pNext) return NULL; /* no union */
    if (p->pSrc->nSrc>1) return NULL; /* no joins */

    if (p->pWhere) {
        where = sqlite3ExprDescribeAtRuntime(v, p->pWhere);
        if (!where) return NULL;
    }

    cols = generate_columns(db, p->pEList, &tbl);
    if(!cols) {
        if (where) sqlite3DbFree(db, where);
        return NULL;
    }


    select = sqlite3_mprintf("SeLeCT %s FRoM %s%s%s", cols, tbl,
            (where)?" WHeRe ":"", (where)?where:"");

    sqlite3DbFree(db, cols);
    if (where) sqlite3DbFree(db, where);

    return select;
}
