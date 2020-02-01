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
** to handle UPDATE statements.
*/
#include "sqliteInt.h"

#ifndef SQLITE_OMIT_VIRTUALTABLE
/* Forward declaration */
static void updateVirtualTable(
  Parse *pParse,       /* The parsing context */
  SrcList *pSrc,       /* The virtual table to be modified */
  Table *pTab,         /* The virtual table */
  ExprList *pChanges,  /* The columns to change in the UPDATE statement */
  Expr *pRowidExpr,    /* Expression used to recompute the rowid */
  int *aXRef,          /* Mapping from columns of pTab to entries in pChanges */
  Expr *pWhere,        /* WHERE clause of the UPDATE statement */
  int onError          /* ON CONFLICT strategy */
);
#endif /* SQLITE_OMIT_VIRTUALTABLE */

/*
** The most recently coded instruction was an OP_Column to retrieve the
** i-th column of table pTab. This routine sets the P4 parameter of the 
** OP_Column to the default value, if any.
**
** The default value of a column is specified by a DEFAULT clause in the 
** column definition. This was either supplied by the user when the table
** was created, or added later to the table definition by an ALTER TABLE
** command. If the latter, then the row-records in the table btree on disk
** may not contain a value for the column and the default value, taken
** from the P4 parameter of the OP_Column instruction, is returned instead.
** If the former, then all row-records are guaranteed to include a value
** for the column and the P4 value is not required.
**
** Column definitions created by an ALTER TABLE command may only have 
** literal default values specified: a number, null or a string. (If a more
** complicated default expression value was provided, it is evaluated 
** when the ALTER TABLE is executed and one of the literal values written
** into the sqlite_master table.)
**
** Therefore, the P4 parameter is only required if the default value for
** the column is a literal number, string or null. The sqlitexValueFromExpr()
** function is capable of transforming these types of expressions into
** sqlitex_value objects.
**
** If parameter iReg is not negative, code an OP_RealAffinity instruction
** on register iReg. This is used when an equivalent integer value is 
** stored in place of an 8-byte floating point value in order to save 
** space.
*/
void sqlitexColumnDefault(Vdbe *v, Table *pTab, int i, int iReg){
  assert( pTab!=0 );
  if( !pTab->pSelect ){
    sqlitex_value *pValue = 0;
    u8 enc = ENC(sqlitexVdbeDb(v));
    Column *pCol = &pTab->aCol[i];
    VdbeComment((v, "%s.%s", pTab->zName, pCol->zName));
    assert( i<pTab->nCol );
    sqlitexValueFromExpr(sqlitexVdbeDb(v), pCol->pDflt, enc, 
                         pCol->affinity, &pValue);
    if( pValue ){
      sqlitexVdbeChangeP4(v, -1, (const char *)pValue, P4_MEM);
    }
#ifndef SQLITE_OMIT_FLOATING_POINT
    /* COMDB2 MODIFICATION */
    if( iReg >= 0 && (pTab->aCol[i].affinity==SQLITE_AFF_REAL ||
         pTab->aCol[i].affinity==SQLITE_AFF_SMALL) ){
      sqlitexVdbeAddOp1(v, OP_RealAffinity, iReg);
    }
#endif
  }
}

/*
** Process an UPDATE statement.
**
**   UPDATE OR IGNORE table_wxyz SET a=b, c=d WHERE e<5 AND f NOT NULL;
**          \_______/ \________/     \______/       \________________/
*            onError   pTabList      pChanges             pWhere
*/
void sqlitexUpdate(
  Parse *pParse,         /* The parser context */
  SrcList *pTabList,     /* The table in which we should change things */
  ExprList *pChanges,    /* Things to be changed */
  Expr *pWhere,          /* The WHERE clause.  May be null */
  int onError,            /* How to handle constraint errors */
  ExprList *pOrderBy,    /* ORDER BY clause. May be null */
  Expr *pLimit,          /* LIMIT clause. May be null */
  Expr *pOffset          /* OFFSET clause. May be null */
){
  int i, j;              /* Loop counters */
  Table *pTab;           /* The table to be updated */
  int addrTop = 0;       /* VDBE instruction address of the start of the loop */
  WhereInfo *pWInfo;     /* Information about the WHERE clause */
  Vdbe *v;               /* The virtual database engine */
  Index *pIdx;           /* For looping over indices */
  Index *pPk;            /* The PRIMARY KEY index for WITHOUT ROWID tables */
  int nIdx;              /* Number of indices that need updating */
  int iBaseCur;          /* Base cursor number */
  int iDataCur;          /* Cursor for the canonical data btree */
  int iIdxCur;           /* Cursor for the first index */
  sqlitex *db;           /* The database structure */
  int *aRegIdx = 0;      /* One register assigned to each index to be updated */
  int *aXRef = 0;        /* aXRef[i] is the index in pChanges->a[] of the
                         ** an expression for the i-th column of the table.
                         ** aXRef[i]==-1 if the i-th column is not changed. */
  u8 *aToOpen;           /* 1 for tables and indices to be opened */
  u8 chngPk;             /* PRIMARY KEY changed in a WITHOUT ROWID table */
  u8 chngRowid;          /* Rowid changed in a normal table */
  u8 chngKey;            /* Either chngPk or chngRowid */
  Expr *pRowidExpr = 0;  /* Expression defining the new record number */
  AuthContext sContext;  /* The authorization context */
  NameContext sNC;       /* The name-context to resolve expressions in */
  int iDb;               /* Database containing the table being updated */
  int okOnePass;         /* True for one-pass algorithm without the FIFO */
  int hasFK;             /* True if foreign key processing is required */
  int labelBreak;        /* Jump here to break out of UPDATE loop */
  int labelContinue;     /* Jump here to continue next step of UPDATE loop */

#ifndef SQLITE_OMIT_TRIGGER
  int isView;            /* True when updating a view (INSTEAD OF trigger) */
  Trigger *pTrigger;     /* List of triggers on pTab, if required */
  int tmask;             /* Mask of TRIGGER_BEFORE|TRIGGER_AFTER */
#endif
  int newmask;           /* Mask of NEW.* columns accessed by BEFORE triggers */
  int iEph = 0;          /* Ephemeral table holding all primary key values */
  int nKey = 0;          /* Number of elements in regKey for WITHOUT ROWID */
  int aiCurOnePass[2];   /* The write cursors opened by WHERE_ONEPASS */

  /* Register Allocations */
  int regRowCount = 0;   /* A count of rows changed */
  int regOldRowid;       /* The old rowid */
  int regNewRowid;       /* The new rowid */
  int regNew;            /* Content of the NEW.* table in triggers */
  int regOld = 0;        /* Content of OLD.* table in triggers */
  int regRowSet = 0;     /* Rowset of rows to be updated */
  int regKey = 0;        /* composite PRIMARY KEY value */

  memset(&sContext, 0, sizeof(sContext));
  db = pParse->db;
  if( pParse->nErr || db->mallocFailed ){
    goto update_cleanup;
  }
  assert( pTabList->nSrc==1 );

  /* Locate the table which we want to update. 
  */
  pTab = sqlitexSrcListLookup(pParse, pTabList);
  if( pTab==0 ) goto update_cleanup;
  iDb = sqlitexSchemaToIndex(pParse->db, pTab->pSchema);

  /* Figure out if we have any triggers and if the table being
  ** updated is a view.
  */
#ifndef SQLITE_OMIT_TRIGGER
  pTrigger = sqlitexTriggersExist(pParse, pTab, TK_UPDATE, pChanges, &tmask);
  isView = pTab->pSelect!=0;
  assert( pTrigger || tmask==0 );
#else
# define pTrigger 0
# define isView 0
# define tmask 0
#endif
#ifdef SQLITE_OMIT_VIEW
# undef isView
# define isView 0
#endif

#ifdef SQLITE_ENABLE_UPDATE_DELETE_LIMIT
  if( !isView ){
    pWhere = sqlitexLimitWhere(
        pParse, pTabList, pWhere, pOrderBy, pLimit, pOffset, "UPDATE"
    );
    pOrderBy = 0;
    pLimit = pOffset = 0;
  }
#endif

  if( sqlitexViewGetColumnNames(pParse, pTab) ){
    goto update_cleanup;
  }
  if( sqlitexIsReadOnly(pParse, pTab, tmask) ){
    goto update_cleanup;
  }

  /* Allocate a cursors for the main database table and for all indices.
  ** The index cursors might not be used, but if they are used they
  ** need to occur right after the database cursor.  So go ahead and
  ** allocate enough space, just in case.
  */
  pTabList->a[0].iCursor = iBaseCur = iDataCur = pParse->nTab++;
  iIdxCur = iDataCur+1;
  pPk = HasRowid(pTab) ? 0 : sqlitexPrimaryKeyIndex(pTab);
  for(nIdx=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, nIdx++){
    if( IsPrimaryKeyIndex(pIdx) && pPk!=0 ){
      iDataCur = pParse->nTab;
      pTabList->a[0].iCursor = iDataCur;
    }
    pParse->nTab++;
  }

  /* Allocate space for aXRef[], aRegIdx[], and aToOpen[].  
  ** Initialize aXRef[] and aToOpen[] to their default values.
  */
  aXRef = sqlitexDbMallocRaw(db, sizeof(int) * (pTab->nCol+nIdx) + nIdx+2 );
  if( aXRef==0 ) goto update_cleanup;
  aRegIdx = aXRef+pTab->nCol;
  aToOpen = (u8*)(aRegIdx+nIdx);
  memset(aToOpen, 1, nIdx+1);
  aToOpen[nIdx+1] = 0;
  for(i=0; i<pTab->nCol; i++) aXRef[i] = -1;

  /* Initialize the name-context */
  memset(&sNC, 0, sizeof(sNC));
  sNC.pParse = pParse;
  sNC.pSrcList = pTabList;

  /* Begin generating code. */
  v = sqlitexGetVdbe(pParse);
  if( v==0 ) goto update_cleanup;

  /* Resolve the column names in all the expressions of the
  ** of the UPDATE statement.  Also find the column index
  ** for each column to be updated in the pChanges array.  For each
  ** column to be updated, make sure we have authorization to change
  ** that column.
  */
  chngRowid = chngPk = 0;
  for(i=0; i<pChanges->nExpr; i++){
    if( sqlitexResolveExprNames(&sNC, pChanges->a[i].pExpr) ){
      goto update_cleanup;
    }
    for(j=0; j<pTab->nCol; j++){
      if( sqlitexStrICmp(pTab->aCol[j].zName, pChanges->a[i].zName)==0 ){
        if( j==pTab->iPKey ){
          chngRowid = 1;
          pRowidExpr = pChanges->a[i].pExpr;
        }else if( pPk && (pTab->aCol[j].colFlags & COLFLAG_PRIMKEY)!=0 ){
          chngPk = 1;
        }
        aXRef[j] = i;
        break;
      }
    }
    if( j>=pTab->nCol ){
      if( pPk==0 && sqlitexIsRowid(pChanges->a[i].zName) ){
        j = -1;
        chngRowid = 1;
        pRowidExpr = pChanges->a[i].pExpr;
      }else{
        sqlitexErrorMsg(pParse, "no such column: %s", pChanges->a[i].zName);
        pParse->checkSchema = 1;
        goto update_cleanup;
      }
    }
#ifndef SQLITE_OMIT_AUTHORIZATION
    {
      int rc;
      rc = sqlitexAuthCheck(pParse, SQLITE_UPDATE, pTab->zName,
                            j<0 ? "ROWID" : pTab->aCol[j].zName,
                            db->aDb[iDb].zName);
      if( rc==SQLITE_DENY ){
        goto update_cleanup;
      }else if( rc==SQLITE_IGNORE ){
        aXRef[j] = -1;
      }
    }
#endif
  }
  assert( (chngRowid & chngPk)==0 );
  assert( chngRowid==0 || chngRowid==1 );
  assert( chngPk==0 || chngPk==1 );
  chngKey = chngRowid + chngPk;

  /* The SET expressions are not actually used inside the WHERE loop.
  ** So reset the colUsed mask
  */
  pTabList->a[0].colUsed = 0;

  hasFK = sqlitexFkRequired(pParse, pTab, aXRef, chngKey);

  /* There is one entry in the aRegIdx[] array for each index on the table
  ** being updated.  Fill in aRegIdx[] with a register number that will hold
  ** the key for accessing each index.  
  */
  for(j=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, j++){
    int reg;
    if( chngKey || hasFK || pIdx->pPartIdxWhere || pIdx==pPk ){
      reg = ++pParse->nMem;
    }else{
      reg = 0;
      for(i=0; i<pIdx->nKeyCol; i++){
        if( aXRef[pIdx->aiColumn[i]]>=0 ){
          reg = ++pParse->nMem;
          break;
        }
      }
    }
    if( reg==0 ) aToOpen[j+1] = 0;
    aRegIdx[j] = reg;
  }

  /*
   * Comdb2 modification: create our updCols array.
   */
  if(isView && strncmp(pTab->aCol[0].zName, "__hidden__rowid",
                       strlen("__hidden__rowid")+1) == 0){
          sqlitexCreateUpdCols(v, db, pTab->nCol-1, aXRef+1);
  } else {
    sqlitexCreateUpdCols(v, db, pTab->nCol, aXRef);
  }

  if( pParse->nested==0 ) sqlitexVdbeCountChanges(v);
  sqlitexBeginWriteOperation(pParse, 1, iDb);

#ifndef SQLITE_OMIT_VIRTUALTABLE
  /* Virtual tables must be handled separately */
  if( IsVirtual(pTab) ){
    updateVirtualTable(pParse, pTabList, pTab, pChanges, pRowidExpr, aXRef,
                       pWhere, onError);
    pWhere = 0;
    pTabList = 0;
    goto update_cleanup;
  }
#endif

  /* Allocate required registers. */
  regRowSet = ++pParse->nMem;
  regOldRowid = regNewRowid = ++pParse->nMem;
  if( chngPk || pTrigger || hasFK ){
    regOld = pParse->nMem + 1;
    pParse->nMem += pTab->nCol;
  }
  if( chngKey || pTrigger || hasFK ){
    regNewRowid = ++pParse->nMem;
  }
  regNew = pParse->nMem + 1;
  pParse->nMem += pTab->nCol;

  /* Start the view context. */
  if( isView ){
    sqlitexAuthContextPush(pParse, &sContext, pTab->zName);
  }

  /* If we are trying to update a view, realize that view into
  ** an ephemeral table.
  */
#if !defined(SQLITE_OMIT_VIEW) && !defined(SQLITE_OMIT_TRIGGER)
  if( isView ){
    sqlitexMaterializeView(pParse, pTab,
        pWhere, pOrderBy, pLimit, pOffset, iDataCur
    );
    pOrderBy = 0;
    pLimit = pOffset = 0;
  }
#endif

  /* Resolve the column names in all the expressions in the
  ** WHERE clause.
  */
  if( sqlitexResolveExprNames(&sNC, pWhere) ){
    goto update_cleanup;
  }

  /* Begin the database scan
  */
  if( HasRowid(pTab) ){
    sqlitexVdbeAddOp3(v, OP_Null, 0, regRowSet, regOldRowid);
    pWInfo = sqlitexWhereBegin(
        pParse, pTabList, pWhere, 0, 0, WHERE_ONEPASS_DESIRED, iIdxCur
    );
    if( pWInfo==0 ) goto update_cleanup;
    okOnePass = sqlitexWhereOkOnePass(pWInfo, aiCurOnePass);
  
    /* Remember the rowid of every item to be updated.
    */
    sqlitexVdbeAddOp2(v, OP_Rowid, iDataCur, regOldRowid);
    if( !okOnePass ){
      sqlitexVdbeAddOp2(v, OP_RowSetAdd, regRowSet, regOldRowid);
    }
  
    /* End the database scan loop.
    */
    sqlitexWhereEnd(pWInfo);
  }else{
    int iPk;         /* First of nPk memory cells holding PRIMARY KEY value */
    i16 nPk;         /* Number of components of the PRIMARY KEY */
    int addrOpen;    /* Address of the OpenEphemeral instruction */

    assert( pPk!=0 );
    nPk = pPk->nKeyCol;
    iPk = pParse->nMem+1;
    pParse->nMem += nPk;
    regKey = ++pParse->nMem;
    iEph = pParse->nTab++;
    sqlitexVdbeAddOp2(v, OP_Null, 0, iPk);
    addrOpen = sqlitexVdbeAddOp2(v, OP_OpenEphemeral, iEph, nPk);
    sqlitexVdbeSetP4KeyInfo(pParse, pPk);
    pWInfo = sqlitexWhereBegin(pParse, pTabList, pWhere, 0, 0, 
                               WHERE_ONEPASS_DESIRED, iIdxCur);
    if( pWInfo==0 ) goto update_cleanup;
    okOnePass = sqlitexWhereOkOnePass(pWInfo, aiCurOnePass);
    for(i=0; i<nPk; i++){
      sqlitexExprCodeGetColumnOfTable(v, pTab, iDataCur, pPk->aiColumn[i],
                                      iPk+i);
    }
    if( okOnePass ){
      sqlitexVdbeChangeToNoop(v, addrOpen);
      nKey = nPk;
      regKey = iPk;
    }else{
      sqlitexVdbeAddOp4(v, OP_MakeRecord, iPk, nPk, regKey,
                        sqlitexIndexAffinityStr(v, pPk), nPk);
      sqlitexVdbeAddOp2(v, OP_IdxInsert, iEph, regKey);
    }
    sqlitexWhereEnd(pWInfo);
  }

  /* Initialize the count of updated rows
  */
  if( (db->flags & SQLITE_CountRows) && !pParse->pTriggerTab ){
    regRowCount = ++pParse->nMem;
    sqlitexVdbeAddOp2(v, OP_Integer, 0, regRowCount);
  }

  labelBreak = sqlitexVdbeMakeLabel(v);
  if( !isView ){
    /* 
    ** Open every index that needs updating.  Note that if any
    ** index could potentially invoke a REPLACE conflict resolution 
    ** action, then we need to open all indices because we might need
    ** to be deleting some records.
    */
    if( onError==OE_Replace ){
      memset(aToOpen, 1, nIdx+1);
    }else{
      for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
        if( pIdx->onError==OE_Replace ){
          memset(aToOpen, 1, nIdx+1);
          break;
        }
      }
    }
    if( okOnePass ){
      if( aiCurOnePass[0]>=0 ) aToOpen[aiCurOnePass[0]-iBaseCur] = 0;
      if( aiCurOnePass[1]>=0 ) aToOpen[aiCurOnePass[1]-iBaseCur] = 0;
    }
    /* COMDB2 MODIFICATION */
    /* AZ: if i dont call, i get a segfault */
    sqlitexOpenTableAndIndices(pParse, pTab, OP_OpenWrite, 0, iBaseCur, aToOpen,
                               0, 0);
  }

  /* Top of the update loop */
  if( okOnePass ){
    if( aToOpen[iDataCur-iBaseCur] && !isView ){
      assert( pPk );
      sqlitexVdbeAddOp4Int(v, OP_NotFound, iDataCur, labelBreak, regKey, nKey);
      VdbeCoverageNeverTaken(v);
    }
    labelContinue = labelBreak;
    sqlitexVdbeAddOp2(v, OP_IsNull, pPk ? regKey : regOldRowid, labelBreak);
    VdbeCoverageIf(v, pPk==0);
    VdbeCoverageIf(v, pPk!=0);
  }else if( pPk ){
    labelContinue = sqlitexVdbeMakeLabel(v);
    sqlitexVdbeAddOp2(v, OP_Rewind, iEph, labelBreak); VdbeCoverage(v);
    addrTop = sqlitexVdbeAddOp2(v, OP_RowKey, iEph, regKey);
    sqlitexVdbeAddOp4Int(v, OP_NotFound, iDataCur, labelContinue, regKey, 0);
    /* COMDB2 MODIFICATION */
    /* use P5 to trigger verify error if not found */
    sqlitexVdbeChangeP5(v, 1);
    VdbeCoverage(v);
  }else{
    labelContinue = sqlitexVdbeAddOp3(v, OP_RowSetRead, regRowSet, labelBreak,
                             regOldRowid);
    VdbeCoverage(v);
    sqlitexVdbeAddOp3(v, OP_NotExists, iDataCur, labelContinue, regOldRowid);
    /* COMDB2 MODIFICATION */
    /* use P5 to trigger verify error if not found */
    sqlitexVdbeChangeP5(v, 1);
    VdbeCoverage(v);
  }

  /* If the record number will change, set register regNewRowid to
  ** contain the new value. If the record number is not being modified,
  ** then regNewRowid is the same register as regOldRowid, which is
  ** already populated.  */
  assert( chngKey || pTrigger || hasFK || regOldRowid==regNewRowid );
  if( chngRowid ){
    sqlitexExprCode(pParse, pRowidExpr, regNewRowid);
    sqlitexVdbeAddOp1(v, OP_MustBeInt, regNewRowid); VdbeCoverage(v);
  }

  /* Compute the old pre-UPDATE content of the row being changed, if that
  ** information is needed */
  if( chngPk || hasFK || pTrigger ){
    u32 oldmask = (hasFK ? sqlitexFkOldmask(pParse, pTab) : 0);
    oldmask |= sqlitexTriggerColmask(pParse, 
        pTrigger, pChanges, 0, TRIGGER_BEFORE|TRIGGER_AFTER, pTab, onError
    );
    for(i=0; i<pTab->nCol; i++){
      if( oldmask==0xffffffff
       || (i<32 && (oldmask & MASKBIT32(i))!=0)
       || (pTab->aCol[i].colFlags & COLFLAG_PRIMKEY)!=0
      ){
        testcase(  oldmask!=0xffffffff && i==31 );
        sqlitexExprCodeGetColumnOfTable(v, pTab, iDataCur, i, regOld+i);
      }else{
        sqlitexVdbeAddOp2(v, OP_Null, 0, regOld+i);
      }
    }
    if( chngRowid==0 && pPk==0 ){
      sqlitexVdbeAddOp2(v, OP_Copy, regOldRowid, regNewRowid);
    }
  }

  /* Populate the array of registers beginning at regNew with the new
  ** row data. This array is used to check constants, create the new
  ** table and index records, and as the values for any new.* references
  ** made by triggers.
  **
  ** If there are one or more BEFORE triggers, then do not populate the
  ** registers associated with columns that are (a) not modified by
  ** this UPDATE statement and (b) not accessed by new.* references. The
  ** values for registers not modified by the UPDATE must be reloaded from 
  ** the database after the BEFORE triggers are fired anyway (as the trigger 
  ** may have modified them). So not loading those that are not going to
  ** be used eliminates some redundant opcodes.
  */
  newmask = sqlitexTriggerColmask(
      pParse, pTrigger, pChanges, 1, TRIGGER_BEFORE, pTab, onError
  );
  /*sqlitexVdbeAddOp3(v, OP_Null, 0, regNew, regNew+pTab->nCol-1);*/
  for(i=0; i<pTab->nCol; i++){
    if( i==pTab->iPKey ){
      sqlitexVdbeAddOp2(v, OP_Null, 0, regNew+i);
    }else{
      j = aXRef[i];
      if( j>=0 ){
        sqlitexExprCode(pParse, pChanges->a[j].pExpr, regNew+i);
      }else if( 0==(tmask&TRIGGER_BEFORE) || i>31 || (newmask & MASKBIT32(i)) ){
        /* This branch loads the value of a column that will not be changed 
        ** into a register. This is done if there are no BEFORE triggers, or
        ** if there are one or more BEFORE triggers that use this value via
        ** a new.* reference in a trigger program.
        */
        testcase( i==31 );
        testcase( i==32 );
        sqlitexExprCodeGetColumnOfTable(v, pTab, iDataCur, i, regNew+i);
      }else{
        sqlitexVdbeAddOp2(v, OP_Null, 0, regNew+i);
      }
    }
  }

  /* Fire any BEFORE UPDATE triggers. This happens before constraints are
  ** verified. One could argue that this is wrong.
  */
  if( tmask&TRIGGER_BEFORE ){
    sqlitexTableAffinity(v, pTab, regNew);
    sqlitexCodeRowTrigger(pParse, pTrigger, TK_UPDATE, pChanges, 
        TRIGGER_BEFORE, pTab, regOldRowid, onError, labelContinue);

    /* The row-trigger may have deleted the row being updated. In this
    ** case, jump to the next row. No updates or AFTER triggers are 
    ** required. This behavior - what happens when the row being updated
    ** is deleted or renamed by a BEFORE trigger - is left undefined in the
    ** documentation.
    */
    if( pPk ){
      sqlitexVdbeAddOp4Int(v, OP_NotFound, iDataCur, labelContinue,regKey,nKey);
      VdbeCoverage(v);
    }else{
      sqlitexVdbeAddOp3(v, OP_NotExists, iDataCur, labelContinue, regOldRowid);
      VdbeCoverage(v);
    }

    /* If it did not delete it, the row-trigger may still have modified 
    ** some of the columns of the row being updated. Load the values for 
    ** all columns not modified by the update statement into their 
    ** registers in case this has happened.
    */
    for(i=0; i<pTab->nCol; i++){
      if( aXRef[i]<0 && i!=pTab->iPKey ){
        sqlitexExprCodeGetColumnOfTable(v, pTab, iDataCur, i, regNew+i);
      }
    }
  }

  if( !isView ){
    int j1 = 0;           /* Address of jump instruction */
    int bReplace = 0;     /* True if REPLACE conflict resolution might happen */

    /* Do constraint checks. */
    assert( regOldRowid>0 );
    sqlitexGenerateConstraintChecks(pParse, pTab, aRegIdx, iDataCur, iIdxCur,
        regNewRowid, regOldRowid, chngKey, onError, labelContinue, &bReplace);

    /* Do FK constraint checks. */
    if( hasFK ){
      sqlitexFkCheck(pParse, pTab, regOldRowid, 0, aXRef, chngKey);
    }

    /* Delete the index entries associated with the current record.  */
    if( bReplace || chngKey ){
      if( pPk ){
        j1 = sqlitexVdbeAddOp4Int(v, OP_NotFound, iDataCur, 0, regKey, nKey);
      }else{
        j1 = sqlitexVdbeAddOp3(v, OP_NotExists, iDataCur, 0, regOldRowid);
      }
      VdbeCoverageNeverTaken(v);
    }
    sqlitexGenerateRowIndexDelete(pParse, pTab, iDataCur, iIdxCur, aRegIdx, -1);
  
    /* If changing the record number, delete the old record.  */
    if( hasFK || chngKey || pPk!=0 ){
      sqlitexVdbeAddOp2(v, OP_Delete, iDataCur, 0);
    }
    if( bReplace || chngKey ){
      sqlitexVdbeJumpHere(v, j1);
    }

    if( hasFK ){
      sqlitexFkCheck(pParse, pTab, 0, regNewRowid, aXRef, chngKey);
    }
  
    /* Insert the new index entries and the new record. */
    sqlitexCompleteInsertion(pParse, pTab, iDataCur, iIdxCur,
                             regNewRowid, aRegIdx, 1, 0, 0);

    /* Do any ON CASCADE, SET NULL or SET DEFAULT operations required to
    ** handle rows (possibly in other tables) that refer via a foreign key
    ** to the row just updated. */ 
    if( hasFK ){
      sqlitexFkActions(pParse, pTab, pChanges, regOldRowid, aXRef, chngKey);
    }
  }

  /* Increment the row counter 
  */
  if( (db->flags & SQLITE_CountRows) && !pParse->pTriggerTab){
    sqlitexVdbeAddOp2(v, OP_AddImm, regRowCount, 1);
  }

  sqlitexCodeRowTrigger(pParse, pTrigger, TK_UPDATE, pChanges, 
      TRIGGER_AFTER, pTab, regOldRowid, onError, labelContinue);

  /* Repeat the above with the next record to be updated, until
  ** all record selected by the WHERE clause have been updated.
  */
  if( okOnePass ){
    /* Nothing to do at end-of-loop for a single-pass */
  }else if( pPk ){
    sqlitexVdbeResolveLabel(v, labelContinue);
    sqlitexVdbeAddOp2(v, OP_Next, iEph, addrTop); VdbeCoverage(v);
  }else{
    sqlitexVdbeAddOp2(v, OP_Goto, 0, labelContinue);
  }
  sqlitexVdbeResolveLabel(v, labelBreak);

  /* Close all tables */
  /* COMDB2 MODIFICATION */
#if 0
  for(i=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, i++){
    assert( aRegIdx );
    if( aToOpen[i+1] ){
      sqlitexVdbeAddOp2(v, OP_Close, iIdxCur+i, 0);
    }
  }
#endif
  if( iDataCur<iIdxCur ) sqlitexVdbeAddOp2(v, OP_Close, iDataCur, 0);

  /* Update the sqlite_sequence table by storing the content of the
  ** maximum rowid counter values recorded while inserting into
  ** autoincrement tables.
  */
  if( pParse->nested==0 && pParse->pTriggerTab==0 ){
    sqlitexAutoincrementEnd(pParse);
  }

  /*
  ** Return the number of rows that were changed. If this routine is 
  ** generating code because of a call to sqlitexNestedParse(), do not
  ** invoke the callback function.
  */
  if( (db->flags&SQLITE_CountRows) && !pParse->pTriggerTab && !pParse->nested ){
    sqlitexVdbeAddOp2(v, OP_ResultRow, regRowCount, 1);
    sqlitexVdbeSetNumCols(v, 1);
    sqlitexVdbeSetColName(v, 0, COLNAME_NAME, "rows updated", SQLITEX_STATIC);
  }

  sqlitexFingerprintUpdate((sqlitex_stmt *)pParse->pVdbe, pTabList, pChanges, pWhere, onError);

update_cleanup:
  sqlitexAuthContextPop(&sContext);
  sqlitexDbFree(db, aXRef); /* Also frees aRegIdx[] and aToOpen[] */
  sqlitexSrcListDelete(db, pTabList);
  sqlitexExprListDelete(db, pChanges);
  sqlitexExprDelete(db, pWhere);
#if defined(SQLITE_ENABLE_UPDATE_DELETE_LIMIT) 
  sqlitexExprListDelete(db, pOrderBy);
  sqlitexExprDelete(db, pLimit);
  sqlitexExprDelete(db, pOffset);
#endif
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

#ifndef SQLITE_OMIT_VIRTUALTABLE
/*
** Generate code for an UPDATE of a virtual table.
**
** The strategy is that we create an ephemeral table that contains
** for each row to be changed:
**
**   (A)  The original rowid of that row.
**   (B)  The revised rowid for the row. (note1)
**   (C)  The content of every column in the row.
**
** Then we loop over this ephemeral table and for each row in
** the ephemeral table call VUpdate.
**
** When finished, drop the ephemeral table.
**
** (note1) Actually, if we know in advance that (A) is always the same
** as (B) we only store (A), then duplicate (A) when pulling
** it out of the ephemeral table before calling VUpdate.
*/
static void updateVirtualTable(
  Parse *pParse,       /* The parsing context */
  SrcList *pSrc,       /* The virtual table to be modified */
  Table *pTab,         /* The virtual table */
  ExprList *pChanges,  /* The columns to change in the UPDATE statement */
  Expr *pRowid,        /* Expression used to recompute the rowid */
  int *aXRef,          /* Mapping from columns of pTab to entries in pChanges */
  Expr *pWhere,        /* WHERE clause of the UPDATE statement */
  int onError          /* ON CONFLICT strategy */
){
  Vdbe *v = pParse->pVdbe;  /* Virtual machine under construction */
  ExprList *pEList = 0;     /* The result set of the SELECT statement */
  Select *pSelect = 0;      /* The SELECT statement */
  Expr *pExpr;              /* Temporary expression */
  int ephemTab;             /* Table holding the result of the SELECT */
  int i;                    /* Loop counter */
  int addr;                 /* Address of top of loop */
  int iReg;                 /* First register in set passed to OP_VUpdate */
  sqlitex *db = pParse->db; /* Database connection */
  const char *pVTab = (const char*)sqlitexGetVTable(db, pTab);
  SelectDest dest;

  /* Construct the SELECT statement that will find the new values for
  ** all updated rows. 
  */
  pEList = sqlitexExprListAppend(pParse, 0, sqlitexExpr(db, TK_ID, "_rowid_"));
  if( pRowid ){
    pEList = sqlitexExprListAppend(pParse, pEList,
                                   sqlitexExprDup(db, pRowid, 0));
  }
  assert( pTab->iPKey<0 );
  for(i=0; i<pTab->nCol; i++){
    if( aXRef[i]>=0 ){
      pExpr = sqlitexExprDup(db, pChanges->a[aXRef[i]].pExpr, 0);
    }else{
      pExpr = sqlitexExpr(db, TK_ID, pTab->aCol[i].zName);
    }
    pEList = sqlitexExprListAppend(pParse, pEList, pExpr);
  }
  pSelect = sqlitexSelectNew(pParse, pEList, pSrc, pWhere, 0, 0, 0, 0, 0, 0);
  
  /* Create the ephemeral table into which the update results will
  ** be stored.
  */
  assert( v );
  ephemTab = pParse->nTab++;
  sqlitexVdbeAddOp2(v, OP_OpenEphemeral, ephemTab, pTab->nCol+1+(pRowid!=0));
  sqlitexVdbeChangeP5(v, BTREE_UNORDERED);

  /* fill the ephemeral table 
  */
  sqlitexSelectDestInit(&dest, SRT_Table, ephemTab);
  sqlitexSelect(pParse, pSelect, &dest);

  /* Generate code to scan the ephemeral table and call VUpdate. */
  iReg = ++pParse->nMem;
  pParse->nMem += pTab->nCol+1;
  addr = sqlitexVdbeAddOp2(v, OP_Rewind, ephemTab, 0); VdbeCoverage(v);
  sqlitexVdbeAddOp3(v, OP_Column,  ephemTab, 0, iReg);
  sqlitexVdbeAddOp3(v, OP_Column, ephemTab, (pRowid?1:0), iReg+1);
  for(i=0; i<pTab->nCol; i++){
    sqlitexVdbeAddOp3(v, OP_Column, ephemTab, i+1+(pRowid!=0), iReg+2+i);
  }
  sqlitexVtabMakeWritable(pParse, pTab);
  sqlitexVdbeAddOp4(v, OP_VUpdate, 0, pTab->nCol+2, iReg, pVTab, P4_VTAB);
  sqlitexVdbeChangeP5(v, onError==OE_Default ? OE_Abort : onError);
  sqlitexMayAbort(pParse);
  sqlitexVdbeAddOp2(v, OP_Next, ephemTab, addr+1); VdbeCoverage(v);
  sqlitexVdbeJumpHere(v, addr);
  sqlitexVdbeAddOp2(v, OP_Close, ephemTab, 0);

  /* Cleanup */
  sqlitexSelectDelete(db, pSelect);  
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */
