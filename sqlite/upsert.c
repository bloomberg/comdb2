/*
** 2018-04-12
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code to implement various aspects of UPSERT
** processing and handling of the Upsert object.
*/
#include "sqliteInt.h"

#ifndef SQLITE_OMIT_UPSERT

int is_comdb2_index_unique(const char *tbl, char *idx);
int comdb2_get_index(const char *dbname, char *idx);

/*
** Free a list of Upsert objects
*/
void sqlite3UpsertDelete(sqlite3 *db, Upsert *p){
  if( p ){
    sqlite3ExprListDelete(db, p->pUpsertTarget);
    sqlite3ExprDelete(db, p->pUpsertTargetWhere);
    sqlite3ExprListDelete(db, p->pUpsertSet);
    sqlite3ExprDelete(db, p->pUpsertWhere);
    sqlite3DbFree(db, p);
  }
}

/*
** Duplicate an Upsert object.
*/
Upsert *sqlite3UpsertDup(sqlite3 *db, Upsert *p){
  if( p==0 ) return 0;
  return sqlite3UpsertNew(db,
           sqlite3ExprListDup(db, p->pUpsertTarget, 0),
           sqlite3ExprDup(db, p->pUpsertTargetWhere, 0),
           sqlite3ExprListDup(db, p->pUpsertSet, 0),
           sqlite3ExprDup(db, p->pUpsertWhere, 0),
           p->oeFlag
         );
}

/*
** Create a new Upsert object.
*/
Upsert *sqlite3UpsertNew(
  sqlite3 *db,           /* Determines which memory allocator to use */
  ExprList *pTarget,     /* Target argument to ON CONFLICT, or NULL */
  Expr *pTargetWhere,    /* Optional WHERE clause on the target */
  ExprList *pSet,        /* UPDATE columns, or NULL for a DO NOTHING */
  Expr *pWhere,          /* WHERE clause for the ON CONFLICT UPDATE */
  int oeFlag             /* ON CONFLICT action flag */
){
  Upsert *pNew;
  pNew = sqlite3DbMallocRaw(db, sizeof(Upsert));
  if( pNew==0 ){
    sqlite3ExprListDelete(db, pTarget);
    sqlite3ExprDelete(db, pTargetWhere);
    sqlite3ExprListDelete(db, pSet);
    sqlite3ExprDelete(db, pWhere);
    return 0;
  }else{
    pNew->pUpsertTarget = pTarget;
    pNew->pUpsertTargetWhere = pTargetWhere;
    pNew->pUpsertSet = pSet;
    pNew->pUpsertWhere = pWhere;
    pNew->pUpsertIdx = 0;
    pNew->oeFlag = oeFlag;
  }
  return pNew;
}

/*
** Analyze the ON CONFLICT clause described by pUpsert.  Resolve all
** symbols in the conflict-target.
**
** Return SQLITE_OK if everything works, or an error code is something
** is wrong.
*/
int sqlite3UpsertAnalyzeTarget(
  Parse *pParse,     /* The parsing context */
  SrcList *pTabList, /* Table into which we are inserting */
  Upsert *pUpsert    /* The ON CONFLICT clauses */
){
  Table *pTab;            /* That table into which we are inserting */
  int rc;                 /* Result code */
  int iCursor;            /* Cursor used by pTab */
  Index *pIdx;            /* One of the indexes of pTab */
  ExprList *pTarget;      /* The conflict-target clause */
  Expr *pTerm;            /* One term of the conflict-target clause */
  NameContext sNC;        /* Context for resolving symbolic names */
  Expr sCol[2];           /* Index column converted into an Expr */

  assert( pTabList->nSrc==1 );
  assert( pTabList->a[0].pTab!=0 );
  assert( pUpsert!=0 );
  assert( pUpsert->pUpsertTarget!=0 );

  /* Resolve all symbolic names in the conflict-target clause, which
  ** includes both the list of columns and the optional partial-index
  ** WHERE clause.
  */
  memset(&sNC, 0, sizeof(sNC));
  sNC.pParse = pParse;
  sNC.pSrcList = pTabList;
  rc = sqlite3ResolveExprListNames(&sNC, pUpsert->pUpsertTarget);
  if( rc ) return rc;
  rc = sqlite3ResolveExprNames(&sNC, pUpsert->pUpsertTargetWhere);
  if( rc ) return rc;

  /* Check to see if the conflict target matches the rowid. */  
  pTab = pTabList->a[0].pTab;
  pTarget = pUpsert->pUpsertTarget;
  iCursor = pTabList->a[0].iCursor;
  if( HasRowid(pTab) 
   && pTarget->nExpr==1
   && (pTerm = pTarget->a[0].pExpr)->op==TK_COLUMN
   && pTerm->iColumn==XN_ROWID
  ){
    /* The conflict-target is the rowid of the primary table */
    assert( pUpsert->pUpsertIdx==0 );
    return SQLITE_OK;
  }

  /* Initialize sCol[0..1] to be an expression parse tree for a
  ** single column of an index.  The sCol[0] node will be the TK_COLLATE
  ** operator and sCol[1] will be the TK_COLUMN operator.  Code below
  ** will populate the specific collation and column number values
  ** prior to comparing against the conflict-target expression.
  */
  memset(sCol, 0, sizeof(sCol));
  sCol[0].op = TK_COLLATE;
  sCol[0].pLeft = &sCol[1];
  sCol[1].op = TK_COLUMN;
  sCol[1].iTable = pTabList->a[0].iCursor;

  /* Check for matches against other indexes */
  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    int ii, jj, nn;
    int is_comdb2_unique = is_comdb2_index_unique(pIdx->pTable->zName,
                                                  pIdx->zName);
    if( !is_comdb2_unique && !IsUniqueIndex(pIdx) ) continue;
    if( pTarget->nExpr!=pIdx->nKeyCol ) continue;
    if( pIdx->pPartIdxWhere ){
      if( pUpsert->pUpsertTargetWhere==0 ) continue;
      if( sqlite3ExprCompare(pUpsert->pUpsertTargetWhere,
                             pIdx->pPartIdxWhere, iCursor)!=0 ){
        continue;
      }
    }
    nn = pIdx->nKeyCol;
    for(ii=0; ii<nn; ii++){
      Expr *pExpr;
      sCol[0].u.zToken = (char*)pIdx->azColl[ii];
      if( pIdx->aiColumn[ii]==XN_EXPR ){
        assert( pIdx->aColExpr!=0 );
        assert( pIdx->aColExpr->nExpr>ii );
        pExpr = pIdx->aColExpr->a[ii].pExpr;
        if( pExpr->op!=TK_COLLATE ){
          sCol[0].pLeft = pExpr;
          pExpr = &sCol[0];
        }
      }else{
        sCol[0].pLeft = &sCol[1];
        sCol[1].iColumn = pIdx->aiColumn[ii];
        pExpr = &sCol[0];
      }
      for(jj=0; jj<nn; jj++){
        if( sqlite3ExprCompare(pTarget->a[jj].pExpr, pExpr,iCursor)<2 ){
          break;  /* Column ii of the index matches column jj of target */
        }
      }
      if( jj>=nn ){
        /* The target contains no match for column jj of the index */
        break;
      }
    }
    if( ii<nn ){
      /* Column ii of the index did not match any term of the conflict target.
      ** Continue the search with the next index. */
      continue;
    }
    pUpsert->pUpsertIdx = pIdx;

    /* COMDB2 MOFIFICATION */
    Vdbe *v = pParse->pVdbe;
    int idx = comdb2_get_index(pIdx->pTable->zName, pIdx->zName);
    comdb2SetUpsertIdx(v, idx);

    return SQLITE_OK;
  }
  sqlite3ErrorMsg(pParse, "ON CONFLICT clause does not match any "
                          "PRIMARY KEY or UNIQUE constraint");
  return SQLITE_ERROR;
}

/*
** Generate bytecode that does an UPDATE as part of an upsert.
**
** If pIdx is NULL, then the UNIQUE constraint that failed was the IPK.
** In this case parameter iCur is a cursor open on the table b-tree that
** currently points to the conflicting table row. Otherwise, if pIdx
** is not NULL, then pIdx is the constraint that failed and iCur is a
** cursor points to the conflicting row.
*/
void sqlite3UpsertDoUpdate(
  Parse *pParse,        /* The parsing and code-generating context */
  Upsert *pUpsert,      /* The ON CONFLICT clause for the upsert */
  Table *pTab,          /* The table being updated */
  Index *pIdx,          /* The UNIQUE constraint that failed */
  int iCur              /* Cursor for pIdx (or pTab if pIdx==NULL) */
){
  Vdbe *v = pParse->pVdbe;
  sqlite3 *db = pParse->db;
  int regKey;               /* Register(s) containing the key */
  Expr *pWhere;             /* Where clause for the UPDATE */
  Expr *pE1, *pE2;
  SrcList *pSrc;            /* FROM clause for the UPDATE */

  assert( v!=0 );
  VdbeNoopComment((v, "Begin DO UPDATE of UPSERT"));
  pWhere = sqlite3ExprDup(db, pUpsert->pUpsertWhere, 0);
  if( pIdx==0 || HasRowid(pTab) ){
    /* We are dealing with an IPK */
    regKey = ++pParse->nMem;
    if( pIdx ){
      sqlite3VdbeAddOp2(v, OP_IdxRowid, iCur, regKey);
    }else{
      sqlite3VdbeAddOp2(v, OP_Rowid, iCur, regKey);
    }
    pE1 = sqlite3ExprAlloc(db, TK_COLUMN, 0, 0);
    if( pE1 ){
      pE1->pTab = pTab;
      pE1->iTable = pParse->nTab;
      pE1->iColumn = -1;
    }
    pE2 = sqlite3ExprAlloc(db, TK_REGISTER, 0, 0);
    if( pE2 ){
      pE2->iTable = regKey;
      pE2->affinity = SQLITE_AFF_INTEGER;
    }
    pWhere = sqlite3ExprAnd(db,pWhere,sqlite3PExpr(pParse, TK_EQ, pE1, pE2, 0));
  }else{
    /* a WITHOUT ROWID table */
    int i, j;
    int iTab = pParse->nTab+1;
    Index *pX;
    for(pX=pTab->pIndex; ALWAYS(pX) && !IsPrimaryKeyIndex(pX); pX=pX->pNext){
      iTab++;
    }
    for(i=0; i<pIdx->nKeyCol; i++){
      regKey = ++pParse->nMem;
      sqlite3VdbeAddOp3(v, OP_Column, iCur, i, regKey);
      j = pIdx->aiColumn[i];
      VdbeComment((v, "%s", pTab->aCol[j].zName));
      pE1 = sqlite3ExprAlloc(db, TK_COLUMN, 0, 0);
      if( pE1 ){
        pE1->pTab = pTab;
        pE1->iTable = iTab;
        pE1->iColumn = j;
      }
      pE2 = sqlite3ExprAlloc(db, TK_REGISTER, 0, 0);
      if( pE2 ){
        pE2->iTable = regKey;
        pE2->affinity = pTab->zColAff[j];
      }
      pWhere = sqlite3ExprAnd(db,pWhere,sqlite3PExpr(pParse, TK_EQ, pE1, pE2,
                                                     0));
    }
  }
  /* pUpsert does not own pUpsertSrc - the outer INSERT statement does.  So
  ** we have to make a copy before passing it down into sqlite3Update() */
  pSrc = sqlite3SrcListDup(db, pUpsert->pUpsertSrc, 0);
  sqlite3Update(pParse, pSrc, pUpsert->pUpsertSet,
      pWhere, OE_Abort, 0, 0, 0, pUpsert);
  pUpsert->pUpsertSet = 0;  /* Will have been deleted by sqlite3Update() */
  VdbeNoopComment((v, "End DO UPDATE of UPSERT"));
}

#endif /* SQLITE_OMIT_UPSERT */
