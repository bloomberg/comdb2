#include "sqliteInt.h"
#include "vdbeInt.h"
#include "md5.h"
#include "flibc.h"

/* COMDB2 MODIFICATION */
int sqlitex_fingerprint(sqlitex_stmt *s, char fingerprint[16]){
  Vdbe *vdbe = (Vdbe*) s;
  if (s == NULL)
      memset(fingerprint, 0, 16);
  else
      memcpy(fingerprint, vdbe->fingerprint, sizeof(vdbe->fingerprint) );
  return 0;
}

int sqlitex_fingerprint_enable(sqlitex *db){
  db->should_fingerprint = 1;
  return 0;
}

int sqlitex_fingerprint_disable(sqlitex *db){
  db->should_fingerprint = 0;
  return 0;
}

static void fingerprintSelectInt(sqlitex *db, MD5Context *c, Select *p);
static void fingerprintExpr(sqlitex *db, MD5Context *c, Expr *p);
static void fingerprintExprList(sqlitex *db, MD5Context *c, ExprList *l);
static void fingerprintWith(sqlitex *db, MD5Context *c, With *pWith);

static void addToFP(MD5Context *c, const unsigned char *buf, unsigned int len) {
    u64 ll; 
    u32 i;
    u16 s;
    void *p = (void*) buf;

#ifndef _LINUX_SOURCE
    switch (len) {
        case 8:
            memcpy(&ll, buf, len);
            ll = flibc_llflip(ll);
            p = &ll;
            break;

        case 4:
            memcpy(&i, buf, len);
            i = flibc_intflip(i);
            p = &i;
            break;

        case 2:
            memcpy(&s, buf, len);
            s = flibc_shortflip(s);
            p = &s;
            break;
    }
#endif

    MD5Update(c, (const unsigned char*) p, len);
}

static void addToFP_noflip(MD5Context *c, const unsigned char *buf, unsigned int len) {
    MD5Update(c, buf, len);
}

static void fingerprintExprList(sqlitex *db, MD5Context *c, ExprList *l) {
  int i;
  struct ExprList_item *pItem;
  if (l == NULL)
      return;
  for(pItem=l->a, i=0; i<l->nExpr; i++, pItem++){
    fingerprintExpr(db, c, pItem->pExpr);
  }
}

static void fingerprintSubqueryList(sqlitex *db, MD5Context *c, ExprList *l) {
  int i;
  struct ExprList_item *pItem;
  if (l == NULL) return;
  for(pItem=l->a, i=0; i<l->nExpr; i++, pItem++){
    if( ExprHasProperty(pItem->pExpr, EP_Subquery) )
      fingerprintExpr(db, c, pItem->pExpr);
  }
}

static void fingerprintTable(sqlitex *db, MD5Context *c, Table *pTab) {
    if (pTab == NULL)
        return;
    if (pTab->zName)
        addToFP_noflip(c, (const unsigned char*) pTab->zName, strlen(pTab->zName));
}

int sqlitexIsLiteral(int op) {
    return op == TK_STRING || op == TK_FLOAT || op == TK_INTEGER || op == TK_BLOB;
}

static void fingerprintExpr(sqlitex *db, MD5Context *c, Expr *p) {
    if (p == NULL || p->visited)
        return;

    p->visited = 1;

    /*
      The operator and flag values are ignored for literals in order to
      avoid having different fingerprints for queries differing only in
      literal types. For instance, the following queries should have same
      fingerprints.

          SELECT 1
          SELECT 'hello'

      For all other types we hash their operators and flags, except for
      identifiers, names of which are hashed instead of operator.
    */

    u32 flags = p->flags;
    flags &= ~EP_IntValue;
    flags &= ~EP_DblQuoted;

    if ((p->op == TK_UMINUS || p->op == TK_UPLUS) && sqlitexIsLiteral(p->pLeft->op)) {
        /* Treat -literal the same as literal */
        p = p->pLeft;
    }

    if(p->op == TK_ID) {
        /* Hash the name of the identifier. */
        addToFP_noflip(c, (const unsigned char*) p->u.zToken, strlen(p->u.zToken));
    } 

    if (sqlitexIsLiteral(p->op)) {
        u8 op = TK_STRING;
        addToFP(c, (const unsigned char*) &op, sizeof(u8));
    }
    else {
        addToFP(c, (const unsigned char*) &p->op, sizeof(u8));
    }

    addToFP(c, (const unsigned char*) &flags, sizeof(u32));

    if (p->op == TK_COLUMN) {
        addToFP(c, (const unsigned char*) &p->iTable, sizeof(int));
        addToFP(c, (const unsigned char*) &p->iColumn, sizeof(ynVar));
    }
    addToFP(c, (const unsigned char*) &p->iAgg, sizeof(i16));
    addToFP(c, (const unsigned char*) &p->iRightJoinTable, sizeof(i16));
    addToFP(c, (const unsigned char*) &p->op2, sizeof(u8));
    if (p->iTable == TK_COLUMN)
        fingerprintTable(db, c, p->pTab);

    if( !ExprHasProperty(p, EP_TokenOnly) ){
        if( p->pLeft ) fingerprintExpr(db, c, p->pLeft);
        if( p->pRight ) fingerprintExpr(db, c, p->pRight);
        if( ExprHasProperty(p, EP_xIsSelect) ){
            fingerprintSelectInt(db, c, p->x.pSelect);
        }else if( p->op == TK_IN ){
            /* We want all IN clauses that contain only literals to fingerprint the same regardless
             * of number of terms. So instead of calling fingerprintSubqueryList directly, walk the
             * list and fingerprint anything that's not a literal. */
            if (ExprHasProperty(p, EP_Subquery))
                fingerprintSubqueryList(db, c, p->x.pList);
            else {
                for (int i = 0; i < p->x.pList->nExpr; i++) {
                    if (p->x.pList->a[i].pExpr && !sqlitexIsLiteral(p->x.pList->a[i].pExpr->op))
                        fingerprintExpr(db, c, p->x.pList->a[i].pExpr);
                }
            }
        }else{
            fingerprintExprList(db, c, p->x.pList);
        }
    }
}

static void fingerprintBitmask(sqlitex *db, MD5Context *c, Bitmask b) {
#ifdef SQLITE_BITMASK_TYPE
    addToFP(c, (u8*) &b, sizeof(SQLITE_BITMASK_TYPE));
#else
    addToFP(c, (u8*) &b, sizeof(u64));
#endif
}

static void fingerprintIdList(sqlitex *db, MD5Context *c, IdList *l) {
    int i;

    if (l == NULL)
        return;

    for (i = 0; i < l->nId; i++) {
        if (l->a[i].zName)
            addToFP_noflip(c, (unsigned char *)l->a[i].zName, strlen(l->a[i].zName));
        addToFP(c, (u8*) &l->a[i].idx, sizeof(int));
    }
}

static void fingerprintSrcListItem(sqlitex *db, MD5Context *c, struct SrcList_item *src) {
    /* TODO: src->Schema - select ... from a.tbl,   select .. from tbl   are different */
    if (src->zDatabase)
        addToFP_noflip(c, (const unsigned char *)src->zDatabase, strlen(src->zDatabase));
    if (src->zName)
        addToFP_noflip(c, (const unsigned char *)src->zName, strlen(src->zName));
    /* alias part - skip?  select a as b   same as select ? */
    if (src->pSelect)
        fingerprintSelectInt(db, c, src->pSelect);
    addToFP(c, (u8*) &src->addrFillSub, sizeof(int));
    addToFP(c, (u8*) &src->regReturn, sizeof(int));
    addToFP(c, (u8*) &src->regResult, sizeof(int));
    addToFP(c, (u8*) &src->fg, sizeof(src->fg));
    addToFP(c, (u8*) &src->iCursor, sizeof(src->iCursor));
    fingerprintExpr(db, c, src->pOn);
    fingerprintIdList(db, c, src->pUsing);
    fingerprintBitmask(db, c, src->colUsed);
    if (src->fg.isIndexedBy)
        addToFP_noflip(c, (const unsigned char *)src->u1.zIndexedBy, strlen(src->u1.zIndexedBy));
    else if (src->fg.isTabFunc)
        fingerprintExprList(db, c, src->u1.pFuncArg);
}

static void fingerprintSrcList(sqlitex *db, MD5Context *c, SrcList *src) {
    int i;
    if (src == NULL)
        return;
    for (i = 0; i < src->nSrc; i++) {
        fingerprintSrcListItem(db, c, &src->a[i]);
    }
}

static void fingerprintSelectInt(sqlitex *db, MD5Context *c, Select *p) {
    if (p == NULL)
        return;
    fingerprintExprList(db, c, p->pEList);
    fingerprintExpr(db, c, p->pWhere);
    fingerprintSrcList(db, c, p->pSrc);
    fingerprintExprList(db, c, p->pGroupBy);
    fingerprintExpr(db, c, p->pHaving);
    fingerprintExprList(db, c, p->pOrderBy);
    fingerprintSelectInt(db, c, p->pNext);
    fingerprintExpr(db, c, p->pLimit);
    fingerprintExpr(db, c, p->pOffset);
    fingerprintWith(db, c, p->pWith);
}

/* This is just like clearSelect, except we recursively checksum all
   the components instead of freeing them. */
void sqlitexFingerprintSelect(sqlitex_stmt *stmt, Select *p) {
    MD5Context c;

    if (stmt == NULL)
        return;

    Vdbe *vdbe = (Vdbe*) stmt;
    sqlitex *db = vdbe->db;

    if (!db->should_fingerprint || db->init.busy)
        return;

    MD5Init(&c);
    fingerprintSelectInt(db, &c, p);


    MD5Final((unsigned char *)vdbe->fingerprint, &c);
}

static void fingerprintWith(sqlitex *db, MD5Context *c, With *pWith) {
    int i;
    if (pWith == NULL)
        return;
    for (i = 0; i < pWith->nCte; i++) {
        addToFP_noflip(c, (const unsigned char *)pWith->a[i].zName, strlen(pWith->a[i].zName));
        fingerprintExprList(db, c, pWith->a[i].pCols);
        /* we don't do pWith->a[i].pSelect - we expect fingerprintSelectInt to
           be called on the corresponding select which will point back to us */
    }
}

static void fingerprintInsertInt(sqlitex *db, MD5Context *c, SrcList *pTabList, Select *pSelect, IdList *pColumn, With *pWith) {
    fingerprintSrcList(db,c, pTabList);
    fingerprintSelectInt(db, c, pSelect);
    fingerprintIdList(db, c, pColumn);
    fingerprintWith(db, c, pWith);
}

#include <fsnapf.h>

/* Why isn't this in insert.c?  Because Insert doesn't introduce any new structures 
   that aren't already processed here */
void sqlitexFingerprintInsert(sqlitex_stmt *s, SrcList *pTabList, Select *pSelect, IdList *pColumn, With *pWith, ExprList *pList) {
    Vdbe *vdbe = (Vdbe*) s;
    MD5Context c;
    sqlitex *db = vdbe->db;

    if (!db->should_fingerprint || db->init.busy)
        return;

    MD5Init(&c);
    fingerprintInsertInt(db, &c, pTabList, pSelect, pColumn, pWith);
    fingerprintExprList(db, &c, pList);
    MD5Final((unsigned char *)vdbe->fingerprint, &c);
}

void sqlitexFingerprintDelete(sqlitex_stmt *s, SrcList *pTabList, Expr *pWhere) {
    Vdbe *vdbe = (Vdbe*) s;
    MD5Context c;
    sqlitex *db = vdbe->db;

    if (!db->should_fingerprint || db->init.busy)
        return;

    MD5Init(&c);
    fingerprintSrcList(db, &c, pTabList);
    fingerprintExpr(db, &c, pWhere);
    MD5Final((unsigned char *)vdbe->fingerprint, &c);
}

void sqlitexFingerprintUpdate(sqlitex_stmt *s, SrcList *pTabList, ExprList *pChanges, Expr *pWhere, int onError) {
    Vdbe *vdbe = (Vdbe*) s;
    MD5Context c;
    sqlitex *db = vdbe->db;

    if (!db->should_fingerprint || db->init.busy)
        return;

    MD5Init(&c);
    fingerprintSrcList(db, &c, pTabList);
    fingerprintExprList(db, &c, pChanges);
    fingerprintExpr(db, &c, pWhere);
    addToFP(&c, (u8*) &onError, sizeof(int));
    MD5Final((unsigned char *)vdbe->fingerprint, &c);
}

/* } COMDB2 MODIFICATION */


