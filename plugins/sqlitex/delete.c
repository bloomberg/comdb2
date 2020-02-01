/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains C code routines that are called by the parser
** in order to generate code for DELETE FROM statements.
*/
#include "sqliteInt.h"
extern int gbl_update_delete_limit;

/*
** While a SrcList can in general represent multiple tables and subqueries
** (as in the FROM clause of a SELECT statement) in this case it contains
** the name of a single table, as one might find in an INSERT, DELETE,
** or UPDATE statement.  Look up that table in the symbol table and
** return a pointer.  Set an error message and return NULL if the table 
** name is not found or if any other error occurs.
**
** The following fields are initialized appropriate in pSrc:
**
**    pSrc->a[0].pTab       Pointer to the Table object
**    pSrc->a[0].pIndex     Pointer to the INDEXED BY index, if there is one
**
*/
Table *sqlitexSrcListLookup(Parse *pParse, SrcList *pSrc){
  struct SrcList_item *pItem = pSrc->a;
  Table *pTab;
  assert( pItem && pSrc->nSrc==1 );
  pTab = sqlitexLocateTableItem(pParse, 0, pItem);
  sqlitexDeleteTable(pParse->db, pItem->pTab);
  pItem->pTab = pTab;
  if( pTab ){
    pTab->nRef++;
  }
  if( sqlitexIndexedByLookup(pParse, pItem) ){
    pTab = 0;
  }
  return pTab;
}

/*
** Check to make sure the given table is writable.  If it is not
** writable, generate an error message and return 1.  If it is
** writable return 0;
*/
int sqlitexIsReadOnly(Parse *pParse, Table *pTab, int viewOk){
  /* A table is not writable under the following circumstances:
  **
  **   1) It is a virtual table and no implementation of the xUpdate method
  **      has been provided, or
  **   2) It is a system table (i.e. sqlite_master), this call is not
  **      part of a nested parse and writable_schema pragma has not 
  **      been specified.
  **
  ** In either case leave an error message in pParse and return non-zero.
  */
  if( ( IsVirtual(pTab) 
     && sqlitexGetVTable(pParse->db, pTab)->pMod->pModule->xUpdate==0 )
   || ( (pTab->tabFlags & TF_Readonly)!=0
     && (pParse->db->flags & SQLITE_WriteSchema)==0
     && pParse->nested==0 )
  ){
    sqlitexErrorMsg(pParse, "table %s may not be modified", pTab->zName);
    return 1;
  }

#ifndef SQLITE_OMIT_VIEW
  if( !viewOk && pTab->pSelect ){
    sqlitexErrorMsg(pParse,"cannot modify %s because it is a view",pTab->zName);
    return 1;
  }
#endif
  return 0;
}


#if !defined(SQLITE_OMIT_VIEW) && !defined(SQLITE_OMIT_TRIGGER)
/*
** Evaluate a view and store its result in an ephemeral table.  The
** pWhere argument is an optional WHERE clause that restricts the
** set of rows in the view that are to be added to the ephemeral table.
*/
void sqlitexMaterializeView(
  Parse *pParse,       /* Parsing context */
  Table *pView,        /* View definition */
  Expr *pWhere,        /* Optional WHERE clause to be added */
  ExprList *pOrderBy,  /* Optional ORDER BY clause */
  Expr *pLimit,        /* Optional LIMIT clause */
  Expr *pOffset,       /* Optional OFFSET clause */
  int iCur             /* Cursor number for ephemeral table */
){
  SelectDest dest;
  Select *pSel;
  SrcList *pFrom;
  sqlitex *db = pParse->db;
  int iDb = sqlitexSchemaToIndex(db, pView->pSchema);
  pWhere = sqlitexExprDup(db, pWhere, 0);
  pFrom = sqlitexSrcListAppend(db, 0, 0, 0);
  if( pFrom ){
    assert( pFrom->nSrc==1 );
    pFrom->a[0].zName = sqlitexDbStrDup(db, pView->zName);
    pFrom->a[0].zDatabase = sqlitexDbStrDup(db, db->aDb[iDb].zName);
    assert( pFrom->a[0].pOn==0 );
    assert( pFrom->a[0].pUsing==0 );
  }
  pSel = sqlitexSelectNew(pParse, 0, pFrom, pWhere, 0, 0, pOrderBy,
                          SF_IncludeHidden, pLimit, pOffset);
  sqlitexSelectDestInit(&dest, SRT_EphemTab, iCur);
  sqlitexSelect(pParse, pSel, &dest);
  sqlitexSelectDelete(db, pSel);
}
#endif /* !defined(SQLITE_OMIT_VIEW) && !defined(SQLITE_OMIT_TRIGGER) */

#if defined(SQLITE_ENABLE_UPDATE_DELETE_LIMIT) && !defined(SQLITE_OMIT_SUBQUERY)
/*
** Generate an expression tree to implement the WHERE, ORDER BY,
** and LIMIT/OFFSET portion of DELETE and UPDATE statements.
**
**     DELETE FROM table_wxyz WHERE a<5 ORDER BY a LIMIT 1;
**                            \__________________________/
**                               pLimitWhere (pInClause)
*/
Expr *sqlitexLimitWhere(
  Parse *pParse,               /* The parser context */
  SrcList *pSrc,               /* the FROM clause -- which tables to scan */
  Expr *pWhere,                /* The WHERE clause.  May be null */
  ExprList *pOrderBy,          /* The ORDER BY clause.  May be null */
  Expr *pLimit,                /* The LIMIT clause.  May be null */
  Expr *pOffset,               /* The OFFSET clause.  May be null */
  char *zStmtType              /* Either DELETE or UPDATE.  For err msgs. */
){
  sqlitex *db = pParse->db;
  Expr *pLhs = NULL;           /* LHS of IN(SELECT...) operator */
  Expr *pInClause = NULL;      /* WHERE rowid IN ( select ) */
  ExprList *pEList = NULL;     /* Expression list contaning only pSelectRowid */
  SrcList *pSelectSrc = NULL;  /* SELECT rowid FROM x ... (dup of pSrc) */
  Select *pSelect = NULL;      /* Complete SELECT tree */
  Table *pTab;

  /* COMDB2 MODIFICATION */
  if (pLimit && gbl_update_delete_limit == 0) {
    sqlitexErrorMsg(pParse, "LIMIT on %s without lrl option: update_delete_limit", zStmtType);
    goto limit_where_cleanup;
  }

  /* Check that there isn't an ORDER BY without a LIMIT clause.
  */
  if( pOrderBy && pLimit==0 ) {
    sqlitexErrorMsg(pParse, "ORDER BY without LIMIT on %s", zStmtType);
    goto limit_where_cleanup;
  }

  /* We only need to generate a select expression if there
  ** is a limit/offset term to enforce.
  */
  if( pLimit == 0 ) {
    /* if pLimit is null, pOffset will always be null as well. */
    assert( pOffset == 0 );
    return pWhere;
  }

  /* Generate a select expression tree to enforce the limit/offset 
  ** term for the DELETE or UPDATE statement.  For example:
  **   DELETE FROM table_a WHERE col1=1 ORDER BY col2 LIMIT 1 OFFSET 1
  ** becomes:
  **   DELETE FROM table_a WHERE rowid IN ( 
  **     SELECT rowid FROM table_a WHERE col1=1 ORDER BY col2 LIMIT 1 OFFSET 1
  **   );
  */

  pTab = pSrc->a[0].pTab;
  if( HasRowid(pTab) ){
    pLhs = sqlitexPExpr(pParse, TK_ROW, 0, 0, 0);
    pEList = sqlitexExprListAppend(
        pParse, 0, sqlitexPExpr(pParse, TK_ROW, 0, 0, 0)
    );
  }else{
    Index *pPk = sqlitexPrimaryKeyIndex(pTab);
    if( pPk->nKeyCol==1 ){
      const char *zName = pTab->aCol[pPk->aiColumn[0]].zName;
      pLhs = sqlitexExpr(db, TK_ID, zName);
      pEList = sqlitexExprListAppend(pParse, 0, sqlitexExpr(db, TK_ID, zName));
    }else{
      sqlitexErrorMsg(pParse, 
          "ORDER BY and LIMIT are not supported for table %s", pTab->zName
      );
      goto limit_where_cleanup;
    }
  }

  /* duplicate the FROM clause as it is needed by both the DELETE/UPDATE tree
  ** and the SELECT subtree. */
  pSrc->a[0].pTab = 0;
  pSelectSrc = sqlitexSrcListDup(pParse->db, pSrc, 0);
  pSrc->a[0].pTab = pTab;
  /* generate the SELECT expression tree. */
  pSelect = sqlitexSelectNew(pParse, pEList, pSelectSrc, pWhere, 0, 0,
                             pOrderBy, 0, pLimit, pOffset);

  /* now generate the new WHERE rowid IN clause for the DELETE/UDPATE */
  pInClause = sqlitexPExpr(pParse, TK_IN, pLhs, 0, 0);
  if( pInClause ){
    pInClause->x.pSelect = pSelect;
    ExprSetProperty(pInClause, EP_xIsSelect|EP_Subquery);
    sqlitexExprSetHeightAndFlags(pParse, pInClause);
  }else{
    sqlitexSelectDelete(pParse->db, pSelect);
  }  
  return pInClause;

limit_where_cleanup:
  sqlitexExprDelete(pParse->db, pWhere);
  sqlitexExprListDelete(pParse->db, pOrderBy);
  sqlitexExprDelete(pParse->db, pLimit);
  sqlitexExprDelete(pParse->db, pOffset);
  return 0;
}
#endif /* defined(SQLITE_ENABLE_UPDATE_DELETE_LIMIT) */
       /*      && !defined(SQLITE_OMIT_SUBQUERY) */

/*
** Generate code for a DELETE FROM statement.
**
**     DELETE FROM table_wxyz WHERE a<5 AND b NOT NULL;
**                 \________/       \________________/
**                  pTabList              pWhere
*/
void sqlitexDeleteFrom(
  Parse *pParse,         /* The parser context */
  SrcList *pTabList,     /* The table from which we should delete things */
  Expr *pWhere,          /* The WHERE clause.  May be null */
  ExprList *pOrderBy,    /* ORDER BY clause. May be null */
  Expr *pLimit,          /* LIMIT clause. May be null */
  Expr *pOffset          /* OFFSET clause. May be null */
){
  Vdbe *v;               /* The virtual database engine */
  Table *pTab;           /* The table from which records will be deleted */
  const char *zDb;       /* Name of database holding pTab */
  int i;                 /* Loop counter */
  WhereInfo *pWInfo;     /* Information about the WHERE clause */
  Index *pIdx;           /* For looping over indices of the table */
  int iTabCur;           /* Cursor number for the table */
  int iDataCur = 0;      /* VDBE cursor for the canonical data source */
  int iIdxCur = 0;       /* Cursor number of the first index */
  int nIdx;              /* Number of indices */
  sqlitex *db;           /* Main database structure */
  AuthContext sContext;  /* Authorization context */
  NameContext sNC;       /* Name context to resolve expressions in */
  int iDb;               /* Database number */
  int memCnt = -1;       /* Memory cell used for change counting */
  int rcauth;            /* Value returned by authorization callback */
  int eOnePass;          /* ONEPASS_OFF or _SINGLE or _MULTI */
  int aiCurOnePass[2];   /* The write cursors opened by WHERE_ONEPASS */
  u8 *aToOpen = 0;       /* Open cursor iTabCur+j if aToOpen[j] is true */
  Index *pPk;            /* The PRIMARY KEY index on the table */
  int iPk = 0;           /* First of nPk registers holding PRIMARY KEY value */
  i16 nPk = 1;           /* Number of columns in the PRIMARY KEY */
  int iKey;              /* Memory cell holding key of row to be deleted */
  i16 nKey;              /* Number of memory cells in the row key */
  int iEphCur = 0;       /* Ephemeral table holding all primary key values */
  int iRowSet = 0;       /* Register for rowset of rows to delete */
  int addrBypass = 0;    /* Address of jump over the delete logic */
  int addrLoop = 0;      /* Top of the delete loop */
  int addrEphOpen = 0;   /* Instruction to open the Ephemeral table */
 
#ifndef SQLITE_OMIT_TRIGGER
  int isView;                  /* True if attempting to delete from a view */
  Trigger *pTrigger;           /* List of table triggers, if required */
  int bComplex;                /* True if there are either triggers or FKs */
#endif

  memset(&sContext, 0, sizeof(sContext));
  db = pParse->db;
  if( pParse->nErr || db->mallocFailed ){
    goto delete_from_cleanup;
  }
  assert( pTabList->nSrc==1 );

  /* Locate the table which we want to delete.  This table has to be
  ** put in an SrcList structure because some of the subroutines we
  ** will be calling are designed to work with multiple tables and expect
  ** an SrcList* parameter instead of just a Table* parameter.
  */
  pTab = sqlitexSrcListLookup(pParse, pTabList);
  if( pTab==0 )  goto delete_from_cleanup;

  /* Figure out if we have any triggers and if the table being
  ** deleted from is a view
  */
#ifndef SQLITE_OMIT_TRIGGER
  pTrigger = sqlitexTriggersExist(pParse, pTab, TK_DELETE, 0, 0);
  isView = pTab->pSelect!=0;
  bComplex = pTrigger || sqlitexFkRequired(pParse, pTab, 0, 0);
#else
# define pTrigger 0
# define isView 0
# define bComplex 0
#endif
#ifdef SQLITE_OMIT_VIEW
# undef isView
# define isView 0
#endif

#ifdef SQLITE_ENABLE_UPDATE_DELETE_LIMIT
  if( !isView ){
    pWhere = sqlitexLimitWhere(
        pParse, pTabList, pWhere, pOrderBy, pLimit, pOffset, "DELETE"
    );
    pOrderBy = 0;
    pLimit = pOffset = 0;
  }
#endif

  /* If pTab is really a view, make sure it has been initialized.
  */
  if( sqlitexViewGetColumnNames(pParse, pTab) ){
    goto delete_from_cleanup;
  }

  if( sqlitexIsReadOnly(pParse, pTab, (pTrigger?1:0)) ){
    goto delete_from_cleanup;
  }
  iDb = sqlitexSchemaToIndex(db, pTab->pSchema);
  assert( iDb<db->nDb );
  zDb = db->aDb[iDb].zName;
  rcauth = sqlitexAuthCheck(pParse, SQLITE_DELETE, pTab->zName, 0, zDb);
  assert( rcauth==SQLITE_OK || rcauth==SQLITE_DENY || rcauth==SQLITE_IGNORE );
  if( rcauth==SQLITE_DENY ){
    goto delete_from_cleanup;
  }
  assert(!isView || pTrigger);

  /* Assign cursor numbers to the table and all its indices.
  */
  assert( pTabList->nSrc==1 );
  iTabCur = pTabList->a[0].iCursor = pParse->nTab++;
  for(nIdx=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, nIdx++){
    pParse->nTab++;
  }

  /* Start the view context
  */
  if( isView ){
    sqlitexAuthContextPush(pParse, &sContext, pTab->zName);
  }

  /* Begin generating code.
  */
  v = sqlitexGetVdbe(pParse);
  if( v==0 ){
    goto delete_from_cleanup;
  }
  if( pParse->nested==0 ) sqlitexVdbeCountChanges(v);
  sqlitexBeginWriteOperation(pParse, 1, iDb);

  /* If we are trying to delete from a view, realize that view into
  ** an ephemeral table.
  */
#if !defined(SQLITE_OMIT_VIEW) && !defined(SQLITE_OMIT_TRIGGER)
  if( isView ){
    sqlitexMaterializeView(pParse, pTab,
        pWhere, pOrderBy, pLimit, pOffset, iTabCur
    );
    iDataCur = iIdxCur = iTabCur;
    pOrderBy = 0;
    pLimit = pOffset = 0;
  }
#endif

  /* Resolve the column names in the WHERE clause.
  */
  memset(&sNC, 0, sizeof(sNC));
  sNC.pParse = pParse;
  sNC.pSrcList = pTabList;
  if( sqlitexResolveExprNames(&sNC, pWhere) ){
    goto delete_from_cleanup;
  }

  /* Initialize the counter of the number of rows deleted, if
  ** we are counting rows.
  */
  if( db->flags & SQLITE_CountRows ){
    memCnt = ++pParse->nMem;
    sqlitexVdbeAddOp2(v, OP_Integer, 0, memCnt);
  }

#ifndef SQLITE_OMIT_TRUNCATE_OPTIMIZATION
  /* Special case: A DELETE without a WHERE clause deletes everything.
  ** It is easier just to erase the whole table. Prior to version 3.6.5,
  ** this optimization caused the row change count (the value returned by 
  ** API function sqlitex_count_changes) to be set incorrectly.  */
  if( rcauth==SQLITE_OK && pWhere==0 && !bComplex && !IsVirtual(pTab) ){
    assert( !isView );
    sqlitexTableLock(pParse, iDb, pTab->tnum, 1, pTab->zName);
    if( HasRowid(pTab) ){
      sqlitexVdbeAddOp4(v, OP_Clear, pTab->tnum, iDb, memCnt,
                        pTab->zName, P4_STATIC);
    }
    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
      assert( pIdx->pSchema==pTab->pSchema );
      sqlitexVdbeAddOp2(v, OP_Clear, pIdx->tnum, iDb);
    }
  }else
#endif /* SQLITE_OMIT_TRUNCATE_OPTIMIZATION */
  {
    u16 wcf = WHERE_ONEPASS_DESIRED|WHERE_DUPLICATES_OK;
    wcf |= (bComplex ? 0 : WHERE_ONEPASS_MULTIROW);
    if( HasRowid(pTab) ){
      /* For a rowid table, initialize the RowSet to an empty set */
      pPk = 0;
      nPk = 1;
      iRowSet = ++pParse->nMem;
      sqlitexVdbeAddOp2(v, OP_Null, 0, iRowSet);
    }else{
      /* For a WITHOUT ROWID table, create an ephemeral table used to
      ** hold all primary keys for rows to be deleted. */
      pPk = sqlitexPrimaryKeyIndex(pTab);
      assert( pPk!=0 );
      nPk = pPk->nKeyCol;
      iPk = pParse->nMem+1;
      pParse->nMem += nPk;
      iEphCur = pParse->nTab++;
      addrEphOpen = sqlitexVdbeAddOp2(v, OP_OpenEphemeral, iEphCur, nPk);
      sqlitexVdbeSetP4KeyInfo(pParse, pPk);
    }
  
    /* Construct a query to find the rowid or primary key for every row
    ** to be deleted, based on the WHERE clause. Set variable eOnePass
    ** to indicate the strategy used to implement this delete:
    **
    **   ONEPASS_OFF:    Two-pass approach - use a FIFO for rowids/PK values.
    **   ONEPASS_SINGLE: One-pass approach - at most one row deleted.
    **   ONEPASS_MULTI:  One-pass approach - any number of rows may be deleted.
    */
    pWInfo = sqlitexWhereBegin(pParse, pTabList, pWhere, 0, 0, wcf, iTabCur+1);
    if( pWInfo==0 ) goto delete_from_cleanup;
    eOnePass = sqlitexWhereOkOnePass(pWInfo, aiCurOnePass);
    assert( IsVirtual(pTab)==0 || eOnePass==ONEPASS_OFF );

    /* Keep track of the number of rows to be deleted */
    if( db->flags & SQLITE_CountRows ){
      sqlitexVdbeAddOp2(v, OP_AddImm, memCnt, 1);
    }
  
    /* Extract the rowid or primary key for the current row */
    if( pPk ){
      for(i=0; i<nPk; i++){
        sqlitexExprCodeGetColumnOfTable(v, pTab, iTabCur,
                                        pPk->aiColumn[i], iPk+i);
      }
      iKey = iPk;
    }else{
      iKey = pParse->nMem + 1;
      iKey = sqlitexExprCodeGetColumn(pParse, pTab, -1, iTabCur, iKey, 0);
      if( iKey>pParse->nMem ) pParse->nMem = iKey;
    }
  
    if( eOnePass!=ONEPASS_OFF){
      /* For ONEPASS, no need to store the rowid/primary-key.  There is only
      ** one, so just keep it in its register(s) and fall through to the
      ** delete code.  */
      nKey = nPk; /* OP_Found will use an unpacked key */
      aToOpen = sqlitexDbMallocRaw(db, nIdx+2);
      if( aToOpen==0 ){
        sqlitexWhereEnd(pWInfo);
        goto delete_from_cleanup;
      }
      memset(aToOpen, 1, nIdx+1);
      aToOpen[nIdx+1] = 0;
      if( aiCurOnePass[0]>=0 ) aToOpen[aiCurOnePass[0]-iTabCur] = 0;
      if( aiCurOnePass[1]>=0 ) aToOpen[aiCurOnePass[1]-iTabCur] = 0;
      if( addrEphOpen ) sqlitexVdbeChangeToNoop(v, addrEphOpen);
    }else{
      if( pPk ){
         /* Add the PK key for this row to the temporary table */
         iKey = ++pParse->nMem;
         nKey = 0;   /* Zero tells OP_Found to use a composite key */
         sqlitexVdbeAddOp4(v, OP_MakeRecord, iPk, nPk, iKey,
               sqlitexIndexAffinityStr(v, pPk), nPk);
         sqlitexVdbeAddOp2(v, OP_IdxInsert, iEphCur, iKey);
      }else{
         /* Add the rowid of the row to be deleted to the RowSet */
         nKey = 1;  /* OP_Seek always uses a single rowid */
         sqlitexVdbeAddOp2(v, OP_RowSetAdd, iRowSet, iKey);
      }
    }

    /* If this DELETE cannot use the ONEPASS strategy, this is the 
    ** end of the WHERE loop */
    if( eOnePass!=ONEPASS_OFF ){
      addrBypass = sqlitexVdbeMakeLabel(v);
    }else{
      sqlitexWhereEnd(pWInfo);
    }

    /* Unless this is a view, open cursors for the table we are 
    ** deleting from and all its indices. If this is a view, then the
    ** only effect this statement has is to fire the INSTEAD OF 
    ** triggers.
    */
    if( !isView ){
      int iAddrOnce = 0;
      if( eOnePass==ONEPASS_MULTI ){
        iAddrOnce = sqlitexCodeOnce(pParse); VdbeCoverage(v);
      }
      testcase( IsVirtual(pTab) );
      sqlitexOpenTableAndIndices(pParse, pTab, OP_OpenWrite, OPFLAG_FORDELETE, 
                                 iTabCur, aToOpen, &iDataCur, &iIdxCur);
      assert( pPk || IsVirtual(pTab) || iDataCur==iTabCur );
      assert( pPk || IsVirtual(pTab) || iIdxCur==iDataCur+1 );
      if( eOnePass==ONEPASS_MULTI ) sqlitexVdbeJumpHere(v, iAddrOnce);
    }
  
    /* Set up a loop over the rowids/primary-keys that were found in the
    ** where-clause loop above.
    */
    if( eOnePass!=ONEPASS_OFF ){
      assert( nKey==nPk );  /* OP_Found will use an unpacked key */
      if( aToOpen[iDataCur-iTabCur] ){
        assert( pPk!=0 || pTab->pSelect!=0 );
        sqlitexVdbeAddOp4Int(v, OP_NotFound, iDataCur, addrBypass, iKey, nKey);
        VdbeCoverage(v);
      }
    }else if( pPk ){
      addrLoop = sqlitexVdbeAddOp1(v, OP_Rewind, iEphCur); VdbeCoverage(v);
      sqlitexVdbeAddOp2(v, OP_RowKey, iEphCur, iKey);
      assert( nKey==0 );  /* OP_Found will use a composite key */
    }else{
      addrLoop = sqlitexVdbeAddOp3(v, OP_RowSetRead, iRowSet, 0, iKey);
      VdbeCoverage(v);
      assert( nKey==1 );
    }  
  
    /* Delete the row */
#ifndef SQLITE_OMIT_VIRTUALTABLE
    if( IsVirtual(pTab) ){
      const char *pVTab = (const char *)sqlitexGetVTable(db, pTab);
      sqlitexVtabMakeWritable(pParse, pTab);
      sqlitexVdbeAddOp4(v, OP_VUpdate, 0, 1, iKey, pVTab, P4_VTAB);
      sqlitexVdbeChangeP5(v, OE_Abort);
      sqlitexMayAbort(pParse);
    }else
#endif
    {
      int count = (pParse->nested==0);    /* True to count changes */
      int iIdxNoSeek = -1;
      if( bComplex==0 && aiCurOnePass[1]!=iDataCur ){
        iIdxNoSeek = aiCurOnePass[1];
      }
      sqlitexGenerateRowDelete(pParse, pTab, pTrigger, iDataCur, iIdxCur,
            iKey, nKey, count, OE_Default, eOnePass, iIdxNoSeek);
    }
  
    /* End of the loop over all rowids/primary-keys. */
    if( eOnePass!=ONEPASS_OFF ){
      sqlitexVdbeResolveLabel(v, addrBypass);
      sqlitexWhereEnd(pWInfo);
    }else if( pPk ){
      sqlitexVdbeAddOp2(v, OP_Next, iEphCur, addrLoop+1); VdbeCoverage(v);
      sqlitexVdbeJumpHere(v, addrLoop);
    }else{
      sqlitexVdbeAddOp2(v, OP_Goto, 0, addrLoop);
      sqlitexVdbeJumpHere(v, addrLoop);
    }     
  
    /* Close the cursors open on the table and its indexes. */
    if( !isView && !IsVirtual(pTab) ){
      if( !pPk ) sqlitexVdbeAddOp1(v, OP_Close, iDataCur);
      for(i=0, pIdx=pTab->pIndex; pIdx; i++, pIdx=pIdx->pNext){
        sqlitexVdbeAddOp1(v, OP_Close, iIdxCur + i);
      }
    }
  } /* End non-truncate path */

  /* Update the sqlite_sequence table by storing the content of the
  ** maximum rowid counter values recorded while inserting into
  ** autoincrement tables.
  */
  if( pParse->nested==0 && pParse->pTriggerTab==0 ){
    sqlitexAutoincrementEnd(pParse);
  }

  /* Return the number of rows that were deleted. If this routine is 
  ** generating code because of a call to sqlitexNestedParse(), do not
  ** invoke the callback function.
  */
  if( (db->flags&SQLITE_CountRows) && !pParse->nested && !pParse->pTriggerTab ){
    sqlitexVdbeAddOp2(v, OP_ResultRow, memCnt, 1);
    sqlitexVdbeSetNumCols(v, 1);
    sqlitexVdbeSetColName(v, 0, COLNAME_NAME, "rows deleted", SQLITEX_STATIC);
  }

  sqlitexFingerprintDelete((struct sqlitex_stmt *)pParse->pVdbe, pTabList, pWhere);

delete_from_cleanup:
  sqlitexAuthContextPop(&sContext);
  sqlitexSrcListDelete(db, pTabList);
  sqlitexExprDelete(db, pWhere);
#if defined(SQLITE_ENABLE_UPDATE_DELETE_LIMIT) 
  sqlitexExprListDelete(db, pOrderBy);
  sqlitexExprDelete(db, pLimit);
  sqlitexExprDelete(db, pOffset);
#endif
  sqlitexDbFree(db, aToOpen);
  return;
}
/* Make sure "isView" and other macros defined above are undefined. Otherwise
** they may interfere with compilation of other functions in this file
** (or in another file, if this file becomes part of the amalgamation).  */
#ifdef isView
 #undef isView
#endif
#ifdef pTrigger
 #undef pTrigger
#endif

/*
** This routine generates VDBE code that causes a single row of a
** single table to be deleted.  Both the original table entry and
** all indices are removed.
**
** Preconditions:
**
**   1.  iDataCur is an open cursor on the btree that is the canonical data
**       store for the table.  (This will be either the table itself,
**       in the case of a rowid table, or the PRIMARY KEY index in the case
**       of a WITHOUT ROWID table.)
**
**   2.  Read/write cursors for all indices of pTab must be open as
**       cursor number iIdxCur+i for the i-th index.
**
**   3.  The primary key for the row to be deleted must be stored in a
**       sequence of nPk memory cells starting at iPk.  If nPk==0 that means
**       that a search record formed from OP_MakeRecord is contained in the
**       single memory location iPk.
**
** eMode:
**   Parameter eMode may be passed either ONEPASS_OFF (0), ONEPASS_SINGLE, or
**   ONEPASS_MULTI.  If eMode is not ONEPASS_OFF, then the cursor
**   iDataCur already points to the row to delete. If eMode is ONEPASS_OFF
**   then this function must seek iDataCur to the entry identified by iPk
**   and nPk before reading from it.
**
**   If eMode is ONEPASS_MULTI, then this call is being made as part
**   of a ONEPASS delete that affects multiple rows. In this case, if 
**   iIdxNoSeek is a valid cursor number (>=0), then its position should
**   be preserved following the delete operation. Or, if iIdxNoSeek is not
**   a valid cursor number, the position of iDataCur should be preserved
**   instead.
**
** iIdxNoSeek:
**   If iIdxNoSeek is a valid cursor number (>=0), then it identifies an
**   index cursor (from within array of cursors starting at iIdxCur) that
**   already points to the index entry to be deleted.
*/
void sqlitexGenerateRowDelete(
  Parse *pParse,     /* Parsing context */
  Table *pTab,       /* Table containing the row to be deleted */
  Trigger *pTrigger, /* List of triggers to (potentially) fire */
  int iDataCur,      /* Cursor from which column data is extracted */
  int iIdxCur,       /* First index cursor */
  int iPk,           /* First memory cell containing the PRIMARY KEY */
  i16 nPk,           /* Number of PRIMARY KEY memory cells */
  u8 count,          /* If non-zero, increment the row change counter */
  u8 onconf,         /* Default ON CONFLICT policy for triggers */
  u8 eMode,          /* ONEPASS_OFF, _SINGLE or _MULTI. See above */
  int iIdxNoSeek     /* Cursor number of cursor that does not need seeking */
){
  Vdbe *v = pParse->pVdbe;        /* Vdbe */
  int iOld = 0;                   /* First register in OLD.* array */
  int iLabel;                     /* Label resolved to end of generated code */
  u8 opSeek;                      /* Seek opcode */

  /* Vdbe is guaranteed to have been allocated by this stage. */
  assert( v );
  VdbeModuleComment((v, "BEGIN: GenRowDel(%d,%d,%d,%d)",
                         iDataCur, iIdxCur, iPk, (int)nPk));

  /* Seek cursor iCur to the row to delete. If this row no longer exists 
  ** (this can happen if a trigger program has already deleted it), do
  ** not attempt to delete it or fire any DELETE triggers.  */
  iLabel = sqlitexVdbeMakeLabel(v);
  opSeek = HasRowid(pTab) ? OP_NotExists : OP_NotFound;
  if( eMode==ONEPASS_OFF ){
    sqlitexVdbeAddOp4Int(v, opSeek, iDataCur, iLabel, iPk, nPk);
    VdbeCoverageIf(v, opSeek==OP_NotExists);
    VdbeCoverageIf(v, opSeek==OP_NotFound);
  }
 
  /* If there are any triggers to fire, allocate a range of registers to
  ** use for the old.* references in the triggers.  */
  if( sqlitexFkRequired(pParse, pTab, 0, 0) || pTrigger ){
    u32 mask;                     /* Mask of OLD.* columns in use */
    int iCol;                     /* Iterator used while populating OLD.* */
    int addrStart;                /* Start of BEFORE trigger programs */

    /* TODO: Could use temporary registers here. Also could attempt to
    ** avoid copying the contents of the rowid register.  */
    mask = sqlitexTriggerColmask(
        pParse, pTrigger, 0, 0, TRIGGER_BEFORE|TRIGGER_AFTER, pTab, onconf
    );
    mask |= sqlitexFkOldmask(pParse, pTab);
    iOld = pParse->nMem+1;
    pParse->nMem += (1 + pTab->nCol);

    /* Populate the OLD.* pseudo-table register array. These values will be 
    ** used by any BEFORE and AFTER triggers that exist.  */
    sqlitexVdbeAddOp2(v, OP_Copy, iPk, iOld);
    for(iCol=0; iCol<pTab->nCol; iCol++){
      testcase( mask!=0xffffffff && iCol==31 );
      testcase( mask!=0xffffffff && iCol==32 );
      if( mask==0xffffffff || (iCol<=31 && (mask & MASKBIT32(iCol))!=0) ){
        sqlitexExprCodeGetColumnOfTable(v, pTab, iDataCur, iCol, iOld+iCol+1);
      }
    }

    /* Invoke BEFORE DELETE trigger programs. */
    addrStart = sqlitexVdbeCurrentAddr(v);
    sqlitexCodeRowTrigger(pParse, pTrigger, 
        TK_DELETE, 0, TRIGGER_BEFORE, pTab, iOld, onconf, iLabel
    );

    /* If any BEFORE triggers were coded, then seek the cursor to the 
    ** row to be deleted again. It may be that the BEFORE triggers moved
    ** the cursor or of already deleted the row that the cursor was
    ** pointing to.
    */
    if( addrStart<sqlitexVdbeCurrentAddr(v) ){
      sqlitexVdbeAddOp4Int(v, opSeek, iDataCur, iLabel, iPk, nPk);
      VdbeCoverageIf(v, opSeek==OP_NotExists);
      VdbeCoverageIf(v, opSeek==OP_NotFound);
    }

    /* Do FK processing. This call checks that any FK constraints that
    ** refer to this table (i.e. constraints attached to other tables) 
    ** are not violated by deleting this row.  */
    sqlitexFkCheck(pParse, pTab, iOld, 0, 0, 0);
  }

  /* Delete the index and table entries. Skip this step if pTab is really
  ** a view (in which case the only effect of the DELETE statement is to
  ** fire the INSTEAD OF triggers).  */ 
  if( pTab->pSelect==0 ){
    u8 p5 = 0;
    sqlitexGenerateRowIndexDelete(pParse, pTab, iDataCur, iIdxCur, 0,iIdxNoSeek);
    sqlitexVdbeAddOp2(v, OP_Delete, iDataCur, (count?OPFLAG_NCHANGE:0));
    if( count ){
      sqlitexVdbeChangeP4(v, -1, pTab->zName, P4_TRANSIENT);
    }
    if( eMode!=ONEPASS_OFF ){
      sqlitexVdbeChangeP5(v, OPFLAG_AUXDELETE);
    }
    if( iIdxNoSeek>=0 ){
      sqlitexVdbeAddOp1(v, OP_Delete, iIdxNoSeek);
    }
    if( eMode==ONEPASS_MULTI ) p5 |= OPFLAG_SAVEPOSITION;
    sqlitexVdbeChangeP5(v, p5);
  }

  /* Do any ON CASCADE, SET NULL or SET DEFAULT operations required to
  ** handle rows (possibly in other tables) that refer via a foreign key
  ** to the row just deleted. */ 
  sqlitexFkActions(pParse, pTab, 0, iOld, 0, 0);

  /* Invoke AFTER DELETE trigger programs. */
  sqlitexCodeRowTrigger(pParse, pTrigger, 
      TK_DELETE, 0, TRIGGER_AFTER, pTab, iOld, onconf, iLabel
  );

  /* Jump here if the row had already been deleted before any BEFORE
  ** trigger programs were invoked. Or if a trigger program throws a 
  ** RAISE(IGNORE) exception.  */
  sqlitexVdbeResolveLabel(v, iLabel);
  VdbeModuleComment((v, "END: GenRowDel()"));
}

/*
** This routine generates VDBE code that causes the deletion of all
** index entries associated with a single row of a single table, pTab
**
** Preconditions:
**
**   1.  A read/write cursor "iDataCur" must be open on the canonical storage
**       btree for the table pTab.  (This will be either the table itself
**       for rowid tables or to the primary key index for WITHOUT ROWID
**       tables.)
**
**   2.  Read/write cursors for all indices of pTab must be open as
**       cursor number iIdxCur+i for the i-th index.  (The pTab->pIndex
**       index is the 0-th index.)
**
**   3.  The "iDataCur" cursor must be already be positioned on the row
**       that is to be deleted.
*/
void sqlitexGenerateRowIndexDelete(
  Parse *pParse,     /* Parsing and code generating context */
  Table *pTab,       /* Table containing the row to be deleted */
  int iDataCur,      /* Cursor of table holding data. */
  int iIdxCur,       /* First index cursor */
  int *aRegIdx,      /* Only delete if aRegIdx!=0 && aRegIdx[i]>0 */
  int iIdxNoSeek     /* Do not delete from this cursor */  
){

  /* COMDB2 MODIFICATION */
#if 0
  int i;             /* Index loop counter */
  int r1 = -1;       /* Register holding an index key */
  int iPartIdxLabel; /* Jump destination for skipping partial index entries */
  Index *pIdx;       /* Current index */
  Index *pPrior = 0; /* Prior index */
  Vdbe *v;           /* The prepared statement under construction */
  Index *pPk;        /* PRIMARY KEY index, or NULL for rowid tables */

  v = pParse->pVdbe;
  pPk = HasRowid(pTab) ? 0 : sqlitexPrimaryKeyIndex(pTab);
  for(i=0, pIdx=pTab->pIndex; pIdx; i++, pIdx=pIdx->pNext){
    assert( iIdxCur+i!=iDataCur || pPk==pIdx );
    if( aRegIdx!=0 && aRegIdx[i]==0 ) continue;
    if( pIdx==pPk ) continue;
    if( iIdxCur+i==iIdxNoSeek ) continue;
    VdbeModuleComment((v, "GenRowIdxDel for %s", pIdx->zName));
    r1 = sqlitexGenerateIndexKey(pParse, pIdx, iDataCur, 0, 1,
          &iPartIdxLabel, pPrior, r1);
    sqlitexVdbeAddOp3(v, OP_IdxDelete, iIdxCur+i, r1,
          pIdx->uniqNotNull ? pIdx->nKeyCol : pIdx->nColumn);
    sqlitexResolvePartIdxLabel(pParse, iPartIdxLabel);
    pPrior = pIdx;
  }
#endif
}

/*
** Generate code that will assemble an index key and stores it in register
** regOut.  The key with be for index pIdx which is an index on pTab.
** iCur is the index of a cursor open on the pTab table and pointing to
** the entry that needs indexing.  If pTab is a WITHOUT ROWID table, then
** iCur must be the cursor of the PRIMARY KEY index.
**
** Return a register number which is the first in a block of
** registers that holds the elements of the index key.  The
** block of registers has already been deallocated by the time
** this routine returns.
**
** If *piPartIdxLabel is not NULL, fill it in with a label and jump
** to that label if pIdx is a partial index that should be skipped.
** The label should be resolved using sqlitexResolvePartIdxLabel().
** A partial index should be skipped if its WHERE clause evaluates
** to false or null.  If pIdx is not a partial index, *piPartIdxLabel
** will be set to zero which is an empty label that is ignored by
** sqlitexResolvePartIdxLabel().
**
** The pPrior and regPrior parameters are used to implement a cache to
** avoid unnecessary register loads.  If pPrior is not NULL, then it is
** a pointer to a different index for which an index key has just been
** computed into register regPrior.  If the current pIdx index is generating
** its key into the same sequence of registers and if pPrior and pIdx share
** a column in common, then the register corresponding to that column already
** holds the correct value and the loading of that register is skipped.
** This optimization is helpful when doing a DELETE or an INTEGRITY_CHECK 
** on a table with multiple indices, and especially with the ROWID or
** PRIMARY KEY columns of the index.
*/
int sqlitexGenerateIndexKey(
  Parse *pParse,       /* Parsing context */
  Index *pIdx,         /* The index for which to generate a key */
  int iDataCur,        /* Cursor number from which to take column data */
  int regOut,          /* Put the new key into this register if not 0 */
  int prefixOnly,      /* Compute only a unique prefix of the key */
  int *piPartIdxLabel, /* OUT: Jump to this label to skip partial index */
  Index *pPrior,       /* Previously generated index key */
  int regPrior         /* Register holding previous generated key */
){
  Vdbe *v = pParse->pVdbe;
  int j;
  Table *pTab = pIdx->pTable;
  int regBase;
  int nCol;

  if( piPartIdxLabel ){
    if( pIdx->pPartIdxWhere ){
      *piPartIdxLabel = sqlitexVdbeMakeLabel(v);
      pParse->iPartIdxTab = iDataCur;
      sqlitexExprCachePush(pParse);
      sqlitexExprIfFalse(pParse, pIdx->pPartIdxWhere, *piPartIdxLabel, 
                         SQLITE_JUMPIFNULL);
    }else{
      *piPartIdxLabel = 0;
    }
  }
  nCol = (prefixOnly && pIdx->uniqNotNull) ? pIdx->nKeyCol : pIdx->nColumn;
  regBase = sqlitexGetTempRange(pParse, nCol);
  if( pPrior && (regBase!=regPrior || pPrior->pPartIdxWhere) ) pPrior = 0;
  for(j=0; j<nCol; j++){
    if( pPrior && pPrior->aiColumn[j]==pIdx->aiColumn[j] ) continue;
    sqlitexExprCodeGetColumnOfTable(v, pTab, iDataCur, pIdx->aiColumn[j],
                                    regBase+j);
    /* If the column affinity is REAL but the number is an integer, then it
    ** might be stored in the table as an integer (using a compact
    ** representation) then converted to REAL by an OP_RealAffinity opcode.
    ** But we are getting ready to store this value back into an index, where
    ** it should be converted by to INTEGER again.  So omit the OP_RealAffinity
    ** opcode if it is present */
    sqlitexVdbeDeletePriorOpcode(v, OP_RealAffinity);
  }
  if( regOut ){
    sqlitexVdbeAddOp3(v, OP_MakeRecord, regBase, nCol, regOut);
  }
  sqlitexReleaseTempRange(pParse, regBase, nCol);
  return regBase;
}

/*
** If a prior call to sqlitexGenerateIndexKey() generated a jump-over label
** because it was a partial index, then this routine should be called to
** resolve that label.
*/
void sqlitexResolvePartIdxLabel(Parse *pParse, int iLabel){
  if( iLabel ){
    sqlitexVdbeResolveLabel(pParse->pVdbe, iLabel);
    sqlitexExprCachePop(pParse);
  }
}
