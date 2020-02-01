/*
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains the implementation for TRIGGERs
*/
#include "sqliteInt.h"

#ifndef SQLITE_OMIT_TRIGGER
/*
** Delete a linked list of TriggerStep structures.
*/
void sqlitexDeleteTriggerStep(sqlitex *db, TriggerStep *pTriggerStep){
  while( pTriggerStep ){
    TriggerStep * pTmp = pTriggerStep;
    pTriggerStep = pTriggerStep->pNext;

    sqlitexExprDelete(db, pTmp->pWhere);
    sqlitexExprListDelete(db, pTmp->pExprList);
    sqlitexSelectDelete(db, pTmp->pSelect);
    sqlitexIdListDelete(db, pTmp->pIdList);

    sqlitexDbFree(db, pTmp);
  }
}

/*
** Given table pTab, return a list of all the triggers attached to 
** the table. The list is connected by Trigger.pNext pointers.
**
** All of the triggers on pTab that are in the same database as pTab
** are already attached to pTab->pTrigger.  But there might be additional
** triggers on pTab in the TEMP schema.  This routine prepends all
** TEMP triggers on pTab to the beginning of the pTab->pTrigger list
** and returns the combined list.
**
** To state it another way:  This routine returns a list of all triggers
** that fire off of pTab.  The list will include any TEMP triggers on
** pTab as well as the triggers lised in pTab->pTrigger.
*/
Trigger *sqlitexTriggerList(Parse *pParse, Table *pTab){
  Schema * const pTmpSchema = pParse->db->aDb[1].pSchema;
  Trigger *pList = 0;                  /* List of triggers to return */

  if( pParse->disableTriggers ){
    return 0;
  }

  if( pTmpSchema!=pTab->pSchema ){
    HashElem *p;
    assert( sqlitexSchemaMutexHeld(pParse->db, 0, pTmpSchema) );
    for(p=sqliteHashFirst(&pTmpSchema->trigHash); p; p=sqliteHashNext(p)){
      Trigger *pTrig = (Trigger *)sqliteHashData(p);
      if( pTrig->pTabSchema==pTab->pSchema
       && 0==sqlitexStrICmp(pTrig->table, pTab->zName) 
      ){
        pTrig->pNext = (pList ? pList : pTab->pTrigger);
        pList = pTrig;
      }
    }
  }

  return (pList ? pList : pTab->pTrigger);
}

/*
** This is called by the parser when it sees a CREATE TRIGGER statement
** up to the point of the BEGIN before the trigger actions.  A Trigger
** structure is generated based on the information available and stored
** in pParse->pNewTrigger.  After the trigger actions have been parsed, the
** sqlitexFinishTrigger() function is called to complete the trigger
** construction process.
*/
void sqlitexBeginTrigger(
  Parse *pParse,      /* The parse context of the CREATE TRIGGER statement */
  Token *pName1,      /* The name of the trigger */
  Token *pName2,      /* The name of the trigger */
  int tr_tm,          /* One of TK_BEFORE, TK_AFTER, TK_INSTEAD */
  int op,             /* One of TK_INSERT, TK_UPDATE, TK_DELETE */
  IdList *pColumns,   /* column list if this is an UPDATE OF trigger */
  SrcList *pTableName,/* The name of the table/view the trigger applies to */
  Expr *pWhen,        /* WHEN clause */
  int isTemp,         /* True if the TEMPORARY keyword is present */
  int noErr           /* Suppress errors if the trigger already exists */
){
  Trigger *pTrigger = 0;  /* The new trigger */
  Table *pTab;            /* Table that the trigger fires off of */
  char *zName = 0;        /* Name of the trigger */
  sqlitex *db = pParse->db;  /* The database connection */
  int iDb;                /* The database to store the trigger in */
  Token *pName;           /* The unqualified db name */
  DbFixer sFix;           /* State vector for the DB fixer */
  int iTabDb;             /* Index of the database holding pTab */

  assert( pName1!=0 );   /* pName1->z might be NULL, but not pName1 itself */
  assert( pName2!=0 );
  assert( op==TK_INSERT || op==TK_UPDATE || op==TK_DELETE );
  assert( op>0 && op<0xff );
  if( isTemp ){
    /* If TEMP was specified, then the trigger name may not be qualified. */
    if( pName2->n>0 ){
      sqlitexErrorMsg(pParse, "temporary trigger may not have qualified name");
      goto trigger_cleanup;
    }
    iDb = 1;
    pName = pName1;
  }else{
    /* Figure out the db that the trigger will be created in */
    iDb = sqlitexTwoPartName(pParse, pName1, pName2, &pName);
    if( iDb<0 ){
      goto trigger_cleanup;
    }
  }
  if( !pTableName || db->mallocFailed ){
    goto trigger_cleanup;
  }

  /* A long-standing parser bug is that this syntax was allowed:
  **
  **    CREATE TRIGGER attached.demo AFTER INSERT ON attached.tab ....
  **                                                 ^^^^^^^^
  **
  ** To maintain backwards compatibility, ignore the database
  ** name on pTableName if we are reparsing out of SQLITE_MASTER.
  */
  if( db->init.busy && iDb!=1 ){
    sqlitexDbFree(db, pTableName->a[0].zDatabase);
    pTableName->a[0].zDatabase = 0;
  }

  /* If the trigger name was unqualified, and the table is a temp table,
  ** then set iDb to 1 to create the trigger in the temporary database.
  ** If sqlitexSrcListLookup() returns 0, indicating the table does not
  ** exist, the error is caught by the block below.
  */
  pTab = sqlitexSrcListLookup(pParse, pTableName);
  if( db->init.busy==0 && pName2->n==0 && pTab
        && pTab->pSchema==db->aDb[1].pSchema ){
    iDb = 1;
  }

  /* Ensure the table name matches database name and that the table exists */
  if( db->mallocFailed ) goto trigger_cleanup;
  assert( pTableName->nSrc==1 );
  sqlitexFixInit(&sFix, pParse, iDb, "trigger", pName);
  if( sqlitexFixSrcList(&sFix, pTableName) ){
    goto trigger_cleanup;
  }
  pTab = sqlitexSrcListLookup(pParse, pTableName);
  if( !pTab ){
    /* The table does not exist. */
    if( db->init.iDb==1 ){
      /* Ticket #3810.
      ** Normally, whenever a table is dropped, all associated triggers are
      ** dropped too.  But if a TEMP trigger is created on a non-TEMP table
      ** and the table is dropped by a different database connection, the
      ** trigger is not visible to the database connection that does the
      ** drop so the trigger cannot be dropped.  This results in an
      ** "orphaned trigger" - a trigger whose associated table is missing.
      */
      db->init.orphanTrigger = 1;
    }
    goto trigger_cleanup;
  }
  if( IsVirtual(pTab) ){
    sqlitexErrorMsg(pParse, "cannot create triggers on virtual tables");
    goto trigger_cleanup;
  }

  /* Check that the trigger name is not reserved and that no trigger of the
  ** specified name exists */
  zName = sqlitexNameFromToken(db, pName);
  if( !zName || SQLITE_OK!=sqlitexCheckObjectName(pParse, zName) ){
    goto trigger_cleanup;
  }
  assert( sqlitexSchemaMutexHeld(db, iDb, 0) );
  if( sqlitexHashFind(&(db->aDb[iDb].pSchema->trigHash),zName) ){
    if( !noErr ){
      sqlitexErrorMsg(pParse, "trigger %T already exists", pName);
    }else{
      assert( !db->init.busy );
      sqlitexCodeVerifySchema(pParse, iDb);
    }
    goto trigger_cleanup;
  }

  /* Do not create a trigger on a system table */
  if( sqlitexStrNICmp(pTab->zName, "sqlite_", 7)==0 ){
    sqlitexErrorMsg(pParse, "cannot create trigger on system table");
    pParse->nErr++;
    goto trigger_cleanup;
  }

  /* INSTEAD of triggers are only for views and views only support INSTEAD
  ** of triggers.
  */
  if( pTab->pSelect && tr_tm!=TK_INSTEAD ){
    sqlitexErrorMsg(pParse, "cannot create %s trigger on view: %S", 
        (tr_tm == TK_BEFORE)?"BEFORE":"AFTER", pTableName, 0);
    goto trigger_cleanup;
  }
  if( !pTab->pSelect && tr_tm==TK_INSTEAD ){
    sqlitexErrorMsg(pParse, "cannot create INSTEAD OF"
        " trigger on table: %S", pTableName, 0);
    goto trigger_cleanup;
  }
  iTabDb = sqlitexSchemaToIndex(db, pTab->pSchema);

#ifndef SQLITE_OMIT_AUTHORIZATION
  {
    int code = SQLITE_CREATE_TRIGGER;
    const char *zDb = db->aDb[iTabDb].zName;
    const char *zDbTrig = isTemp ? db->aDb[1].zName : zDb;
    if( iTabDb==1 || isTemp ) code = SQLITE_CREATE_TEMP_TRIGGER;
    if( sqlitexAuthCheck(pParse, code, zName, pTab->zName, zDbTrig) ){
      goto trigger_cleanup;
    }
    if( sqlitexAuthCheck(pParse, SQLITE_INSERT, SCHEMA_TABLE(iTabDb),0,zDb)){
      goto trigger_cleanup;
    }
  }
#endif

  /* INSTEAD OF triggers can only appear on views and BEFORE triggers
  ** cannot appear on views.  So we might as well translate every
  ** INSTEAD OF trigger into a BEFORE trigger.  It simplifies code
  ** elsewhere.
  */
  if (tr_tm == TK_INSTEAD){
    tr_tm = TK_BEFORE;
  }

  /* Build the Trigger object */
  pTrigger = (Trigger*)sqlitexDbMallocZero(db, sizeof(Trigger));
  if( pTrigger==0 ) goto trigger_cleanup;
  pTrigger->zName = zName;
  zName = 0;
  pTrigger->table = sqlitexDbStrDup(db, pTableName->a[0].zName);
  pTrigger->pSchema = db->aDb[iDb].pSchema;
  pTrigger->pTabSchema = pTab->pSchema;
  pTrigger->op = (u8)op;
  pTrigger->tr_tm = tr_tm==TK_BEFORE ? TRIGGER_BEFORE : TRIGGER_AFTER;
  pTrigger->pWhen = sqlitexExprDup(db, pWhen, EXPRDUP_REDUCE);
  pTrigger->pColumns = sqlitexIdListDup(db, pColumns);
  assert( pParse->pNewTrigger==0 );
  pParse->pNewTrigger = pTrigger;

trigger_cleanup:
  sqlitexDbFree(db, zName);
  sqlitexSrcListDelete(db, pTableName);
  sqlitexIdListDelete(db, pColumns);
  sqlitexExprDelete(db, pWhen);
  if( !pParse->pNewTrigger ){
    sqlitexDeleteTrigger(db, pTrigger);
  }else{
    assert( pParse->pNewTrigger==pTrigger );
  }
}

/*
** This routine is called after all of the trigger actions have been parsed
** in order to complete the process of building the trigger.
*/
void sqlitexFinishTrigger(
  Parse *pParse,          /* Parser context */
  TriggerStep *pStepList, /* The triggered program */
  Token *pAll             /* Token that describes the complete CREATE TRIGGER */
){
  Trigger *pTrig = pParse->pNewTrigger;   /* Trigger being finished */
  char *zName;                            /* Name of trigger */
  sqlitex *db = pParse->db;               /* The database */
  DbFixer sFix;                           /* Fixer object */
  int iDb;                                /* Database containing the trigger */
  Token nameToken;                        /* Trigger name for error reporting */

  pParse->pNewTrigger = 0;
  if( NEVER(pParse->nErr) || !pTrig ) goto triggerfinish_cleanup;
  zName = pTrig->zName;
  iDb = sqlitexSchemaToIndex(pParse->db, pTrig->pSchema);
  pTrig->step_list = pStepList;
  while( pStepList ){
    pStepList->pTrig = pTrig;
    pStepList = pStepList->pNext;
  }
  nameToken.z = pTrig->zName;
  nameToken.n = sqlitexStrlen30(nameToken.z);
  sqlitexFixInit(&sFix, pParse, iDb, "trigger", &nameToken);
  if( sqlitexFixTriggerStep(&sFix, pTrig->step_list) 
   || sqlitexFixExpr(&sFix, pTrig->pWhen) 
  ){
    goto triggerfinish_cleanup;
  }

  /* if we are not initializing,
  ** build the sqlite_master entry
  */
  /* COMDB2 MODIFICATION */
  /* FIXME TODO XXX prod has a different scheme. 
   * see how it fits there */
  if( !db->init.busy ){
    Vdbe *v;
    char *z;

    /* Make an entry in the sqlite_master table */
    v = sqlitexGetVdbe(pParse);
    if( v==0 ) goto triggerfinish_cleanup;
    sqlitexBeginWriteOperation(pParse, 0, iDb);
    z = sqlitexDbStrNDup(db, (char*)pAll->z, pAll->n);
    sqlitexNestedParse(pParse,
       "INSERT INTO %Q.%s VALUES('trigger',%Q,%Q,0,'CREATE TRIGGER %q', NULL)",
       db->aDb[iDb].zName, SCHEMA_TABLE(iDb), zName,
       pTrig->table, z);
    sqlitexDbFree(db, z);
    sqlitexChangeCookie(pParse, iDb);
    sqlitexVdbeAddParseSchemaOp(v, iDb,
        sqlitexMPrintf(db, "type='trigger' AND name='%q'", zName));
  }

  if( db->init.busy ){
    Trigger *pLink = pTrig;
    Hash *pHash = &db->aDb[iDb].pSchema->trigHash;
    assert( sqlitexSchemaMutexHeld(db, iDb, 0) );
    pTrig = sqlitexHashInsert(pHash, zName, pTrig);
    if( pTrig ){
      db->mallocFailed = 1;
    }else if( pLink->pSchema==pLink->pTabSchema ){
      Table *pTab;
      pTab = sqlitexHashFind(&pLink->pTabSchema->tblHash, pLink->table);
      assert( pTab!=0 );
      pLink->pNext = pTab->pTrigger;
      pTab->pTrigger = pLink;
    }
  }

triggerfinish_cleanup:
  sqlitexDeleteTrigger(db, pTrig);
  assert( !pParse->pNewTrigger );
  sqlitexDeleteTriggerStep(db, pStepList);
}

/*
** Turn a SELECT statement (that the pSelect parameter points to) into
** a trigger step.  Return a pointer to a TriggerStep structure.
**
** The parser calls this routine when it finds a SELECT statement in
** body of a TRIGGER.  
*/
TriggerStep *sqlitexTriggerSelectStep(sqlitex *db, Select *pSelect){
  TriggerStep *pTriggerStep = sqlitexDbMallocZero(db, sizeof(TriggerStep));
  if( pTriggerStep==0 ) {
    sqlitexSelectDelete(db, pSelect);
    return 0;
  }
  pTriggerStep->op = TK_SELECT;
  pTriggerStep->pSelect = pSelect;
  pTriggerStep->orconf = OE_Default;
  return pTriggerStep;
}

/*
** Allocate space to hold a new trigger step.  The allocated space
** holds both the TriggerStep object and the TriggerStep.target.z string.
**
** If an OOM error occurs, NULL is returned and db->mallocFailed is set.
*/
static TriggerStep *triggerStepAllocate(
  sqlitex *db,                /* Database connection */
  u8 op,                      /* Trigger opcode */
  Token *pName                /* The target name */
){
  TriggerStep *pTriggerStep;

  pTriggerStep = sqlitexDbMallocZero(db, sizeof(TriggerStep) + pName->n);
  if( pTriggerStep ){
    char *z = (char*)&pTriggerStep[1];
    memcpy(z, pName->z, pName->n);
    pTriggerStep->target.z = z;
    pTriggerStep->target.n = pName->n;
    pTriggerStep->op = op;
  }
  return pTriggerStep;
}

/*
** Build a trigger step out of an INSERT statement.  Return a pointer
** to the new trigger step.
**
** The parser calls this routine when it sees an INSERT inside the
** body of a trigger.
*/
TriggerStep *sqlitexTriggerInsertStep(
  sqlitex *db,        /* The database connection */
  Token *pTableName,  /* Name of the table into which we insert */
  IdList *pColumn,    /* List of columns in pTableName to insert into */
  Select *pSelect,    /* A SELECT statement that supplies values */
  u8 orconf           /* The conflict algorithm (OE_Abort, OE_Replace, etc.) */
){
  TriggerStep *pTriggerStep;

  assert(pSelect != 0 || db->mallocFailed);

  pTriggerStep = triggerStepAllocate(db, TK_INSERT, pTableName);
  if( pTriggerStep ){
    pTriggerStep->pSelect = sqlitexSelectDup(db, pSelect, EXPRDUP_REDUCE);
    pTriggerStep->pIdList = pColumn;
    pTriggerStep->orconf = orconf;
  }else{
    sqlitexIdListDelete(db, pColumn);
  }
  sqlitexSelectDelete(db, pSelect);

  return pTriggerStep;
}

/*
** Construct a trigger step that implements an UPDATE statement and return
** a pointer to that trigger step.  The parser calls this routine when it
** sees an UPDATE statement inside the body of a CREATE TRIGGER.
*/
TriggerStep *sqlitexTriggerUpdateStep(
  sqlitex *db,         /* The database connection */
  Token *pTableName,   /* Name of the table to be updated */
  ExprList *pEList,    /* The SET clause: list of column and new values */
  Expr *pWhere,        /* The WHERE clause */
  u8 orconf            /* The conflict algorithm. (OE_Abort, OE_Ignore, etc) */
){
  TriggerStep *pTriggerStep;

  pTriggerStep = triggerStepAllocate(db, TK_UPDATE, pTableName);
  if( pTriggerStep ){
    pTriggerStep->pExprList = sqlitexExprListDup(db, pEList, EXPRDUP_REDUCE);
    pTriggerStep->pWhere = sqlitexExprDup(db, pWhere, EXPRDUP_REDUCE);
    pTriggerStep->orconf = orconf;
  }
  sqlitexExprListDelete(db, pEList);
  sqlitexExprDelete(db, pWhere);
  return pTriggerStep;
}

/*
** Construct a trigger step that implements a DELETE statement and return
** a pointer to that trigger step.  The parser calls this routine when it
** sees a DELETE statement inside the body of a CREATE TRIGGER.
*/
TriggerStep *sqlitexTriggerDeleteStep(
  sqlitex *db,            /* Database connection */
  Token *pTableName,      /* The table from which rows are deleted */
  Expr *pWhere            /* The WHERE clause */
){
  TriggerStep *pTriggerStep;

  pTriggerStep = triggerStepAllocate(db, TK_DELETE, pTableName);
  if( pTriggerStep ){
    pTriggerStep->pWhere = sqlitexExprDup(db, pWhere, EXPRDUP_REDUCE);
    pTriggerStep->orconf = OE_Default;
  }
  sqlitexExprDelete(db, pWhere);
  return pTriggerStep;
}

/* 
** Recursively delete a Trigger structure
*/
void sqlitexDeleteTrigger(sqlitex *db, Trigger *pTrigger){
  if( pTrigger==0 ) return;
  sqlitexDeleteTriggerStep(db, pTrigger->step_list);
  sqlitexDbFree(db, pTrigger->zName);
  sqlitexDbFree(db, pTrigger->table);
  sqlitexExprDelete(db, pTrigger->pWhen);
  sqlitexIdListDelete(db, pTrigger->pColumns);
  sqlitexDbFree(db, pTrigger);
}

/*
** This function is called to drop a trigger from the database schema. 
**
** This may be called directly from the parser and therefore identifies
** the trigger by name.  The sqlitexDropTriggerPtr() routine does the
** same job as this routine except it takes a pointer to the trigger
** instead of the trigger name.
**/
void sqlitexDropTrigger(Parse *pParse, SrcList *pName, int noErr){
  Trigger *pTrigger = 0;
  int i;
  const char *zDb;
  const char *zName;
  sqlitex *db = pParse->db;

  if( db->mallocFailed ) goto drop_trigger_cleanup;
  if( SQLITE_OK!=sqlitexReadSchema(pParse) ){
    goto drop_trigger_cleanup;
  }

  assert( pName->nSrc==1 );
  zDb = pName->a[0].zDatabase;
  zName = pName->a[0].zName;
  assert( zDb!=0 || sqlite3BtreeHoldsAllMutexes(db) );
  for(i=OMIT_TEMPDB; i<db->nDb; i++){
    int j = (i<2) ? i^1 : i;  /* Search TEMP before MAIN */
    if( zDb && sqlitexStrICmp(db->aDb[j].zName, zDb) ) continue;
    assert( sqlitexSchemaMutexHeld(db, j, 0) );
    pTrigger = sqlitexHashFind(&(db->aDb[j].pSchema->trigHash), zName);
    if( pTrigger ) break;
  }
  if( !pTrigger ){
    if( !noErr ){
      sqlitexErrorMsg(pParse, "no such trigger: %S", pName, 0);
    }else{
      sqlitexCodeVerifyNamedSchema(pParse, zDb);
    }
    pParse->checkSchema = 1;
    goto drop_trigger_cleanup;
  }
  sqlitexDropTriggerPtr(pParse, pTrigger);

drop_trigger_cleanup:
  sqlitexSrcListDelete(db, pName);
}

/*
** Return a pointer to the Table structure for the table that a trigger
** is set on.
*/
static Table *tableOfTrigger(Trigger *pTrigger){
  return sqlitexHashFind(&pTrigger->pTabSchema->tblHash, pTrigger->table);
}


/*
** Drop a trigger given a pointer to that trigger. 
*/
void sqlitexDropTriggerPtr(Parse *pParse, Trigger *pTrigger){
  Table   *pTable;
  Vdbe *v;
  sqlitex *db = pParse->db;
  int iDb;

  iDb = sqlitexSchemaToIndex(pParse->db, pTrigger->pSchema);
  assert( iDb>=0 && iDb<db->nDb );
  pTable = tableOfTrigger(pTrigger);
  assert( pTable );
  assert( pTable->pSchema==pTrigger->pSchema || iDb==1 );
#ifndef SQLITE_OMIT_AUTHORIZATION
  {
    int code = SQLITE_DROP_TRIGGER;
    const char *zDb = db->aDb[iDb].zName;
    const char *zTab = SCHEMA_TABLE(iDb);
    if( iDb==1 ) code = SQLITE_DROP_TEMP_TRIGGER;
    if( sqlitexAuthCheck(pParse, code, pTrigger->zName, pTable->zName, zDb) ||
      sqlitexAuthCheck(pParse, SQLITE_DELETE, zTab, 0, zDb) ){
      return;
    }
  }
#endif

  /* Generate code to destroy the database record of the trigger.
  */
  assert( pTable!=0 );
  if( (v = sqlitexGetVdbe(pParse))!=0 ){
    int base;
    static const int iLn = VDBE_OFFSET_LINENO(2);
    static const VdbeOpList dropTrigger[] = {
      { OP_Rewind,     0, ADDR(9),  0},
      { OP_String8,    0, 1,        0}, /* 1 */
      { OP_Column,     0, 1,        2},
      { OP_Ne,         2, ADDR(8),  1},
      { OP_String8,    0, 1,        0}, /* 4: "trigger" */
      { OP_Column,     0, 0,        2},
      { OP_Ne,         2, ADDR(8),  1},
      { OP_Delete,     0, 0,        0},
      { OP_Next,       0, ADDR(1),  0}, /* 8 */
    };

    sqlitexBeginWriteOperation(pParse, 0, iDb);
    sqlitexOpenMasterTable(pParse, iDb);
    base = sqlitexVdbeAddOpList(v,  ArraySize(dropTrigger), dropTrigger, iLn);
    sqlitexVdbeChangeP4(v, base+1, pTrigger->zName, P4_TRANSIENT);
    sqlitexVdbeChangeP4(v, base+4, "trigger", P4_STATIC);
    sqlitexChangeCookie(pParse, iDb);
    sqlitexVdbeAddOp2(v, OP_Close, 0, 0);
    sqlitexVdbeAddOp4(v, OP_DropTrigger, iDb, 0, 0, pTrigger->zName, 0);
    if( pParse->nMem<3 ){
      pParse->nMem = 3;
    }
  }
}

/*
** Remove a trigger from the hash tables of the sqlite* pointer.
*/
void sqlitexUnlinkAndDeleteTrigger(sqlitex *db, int iDb, const char *zName){
  Trigger *pTrigger;
  Hash *pHash;

  assert( sqlitexSchemaMutexHeld(db, iDb, 0) );
  pHash = &(db->aDb[iDb].pSchema->trigHash);
  pTrigger = sqlitexHashInsert(pHash, zName, 0);
  if( ALWAYS(pTrigger) ){
    if( pTrigger->pSchema==pTrigger->pTabSchema ){
      Table *pTab = tableOfTrigger(pTrigger);
      Trigger **pp;
      for(pp=&pTab->pTrigger; *pp!=pTrigger; pp=&((*pp)->pNext));
      *pp = (*pp)->pNext;
    }
    sqlitexDeleteTrigger(db, pTrigger);
    db->flags |= SQLITE_InternChanges;
  }
}

/*
** pEList is the SET clause of an UPDATE statement.  Each entry
** in pEList is of the format <id>=<expr>.  If any of the entries
** in pEList have an <id> which matches an identifier in pIdList,
** then return TRUE.  If pIdList==NULL, then it is considered a
** wildcard that matches anything.  Likewise if pEList==NULL then
** it matches anything so always return true.  Return false only
** if there is no match.
*/
static int checkColumnOverlap(IdList *pIdList, ExprList *pEList){
  int e;
  if( pIdList==0 || NEVER(pEList==0) ) return 1;
  for(e=0; e<pEList->nExpr; e++){
    if( sqlitexIdListIndex(pIdList, pEList->a[e].zName)>=0 ) return 1;
  }
  return 0; 
}

/*
** Return a list of all triggers on table pTab if there exists at least
** one trigger that must be fired when an operation of type 'op' is 
** performed on the table, and, if that operation is an UPDATE, if at
** least one of the columns in pChanges is being modified.
*/
Trigger *sqlitexTriggersExist(
  Parse *pParse,          /* Parse context */
  Table *pTab,            /* The table the contains the triggers */
  int op,                 /* one of TK_DELETE, TK_INSERT, TK_UPDATE */
  ExprList *pChanges,     /* Columns that change in an UPDATE statement */
  int *pMask              /* OUT: Mask of TRIGGER_BEFORE|TRIGGER_AFTER */
){
  int mask = 0;
  Trigger *pList = 0;
  Trigger *p;

  if( (pParse->db->flags & SQLITE_EnableTrigger)!=0 ){
    pList = sqlitexTriggerList(pParse, pTab);
  }
  assert( pList==0 || IsVirtual(pTab)==0 );
  for(p=pList; p; p=p->pNext){
    if( p->op==op && checkColumnOverlap(p->pColumns, pChanges) ){
      mask |= p->tr_tm;
    }
  }
  if( pMask ){
    *pMask = mask;
  }
  return (mask ? pList : 0);
}

/*
** Convert the pStep->target token into a SrcList and return a pointer
** to that SrcList.
**
** This routine adds a specific database name, if needed, to the target when
** forming the SrcList.  This prevents a trigger in one database from
** referring to a target in another database.  An exception is when the
** trigger is in TEMP in which case it can refer to any other database it
** wants.
*/
static SrcList *targetSrcList(
  Parse *pParse,       /* The parsing context */
  TriggerStep *pStep   /* The trigger containing the target token */
){
  int iDb;             /* Index of the database to use */
  SrcList *pSrc;       /* SrcList to be returned */

  pSrc = sqlitexSrcListAppend(pParse->db, 0, &pStep->target, 0);
  if( pSrc ){
    assert( pSrc->nSrc>0 );
    assert( pSrc->a!=0 );
    iDb = sqlitexSchemaToIndex(pParse->db, pStep->pTrig->pSchema);
    if( iDb==0 || iDb>=2 ){
      sqlitex *db = pParse->db;
      assert( iDb<pParse->db->nDb );
      pSrc->a[pSrc->nSrc-1].zDatabase = sqlitexDbStrDup(db, db->aDb[iDb].zName);
    }
  }
  return pSrc;
}

/*
** Generate VDBE code for the statements inside the body of a single 
** trigger.
*/
static int codeTriggerProgram(
  Parse *pParse,            /* The parser context */
  TriggerStep *pStepList,   /* List of statements inside the trigger body */
  int orconf                /* Conflict algorithm. (OE_Abort, etc) */  
){
  TriggerStep *pStep;
  Vdbe *v = pParse->pVdbe;
  sqlitex *db = pParse->db;

  assert( pParse->pTriggerTab && pParse->pToplevel );
  assert( pStepList );
  assert( v!=0 );
  for(pStep=pStepList; pStep; pStep=pStep->pNext){
    /* Figure out the ON CONFLICT policy that will be used for this step
    ** of the trigger program. If the statement that caused this trigger
    ** to fire had an explicit ON CONFLICT, then use it. Otherwise, use
    ** the ON CONFLICT policy that was specified as part of the trigger
    ** step statement. Example:
    **
    **   CREATE TRIGGER AFTER INSERT ON t1 BEGIN;
    **     INSERT OR REPLACE INTO t2 VALUES(new.a, new.b);
    **   END;
    **
    **   INSERT INTO t1 ... ;            -- insert into t2 uses REPLACE policy
    **   INSERT OR IGNORE INTO t1 ... ;  -- insert into t2 uses IGNORE policy
    */
    pParse->eOrconf = (orconf==OE_Default)?pStep->orconf:(u8)orconf;
    assert( pParse->okConstFactor==0 );

    switch( pStep->op ){
      case TK_UPDATE: {
        sqlitexUpdate(pParse, 
          targetSrcList(pParse, pStep),
          sqlitexExprListDup(db, pStep->pExprList, 0), 
          sqlitexExprDup(db, pStep->pWhere, 0), 
          pParse->eOrconf, 0, 0, 0
        );
        break;
      }
      case TK_INSERT: {
        sqlitexInsert(pParse, 
          targetSrcList(pParse, pStep),
          sqlitexSelectDup(db, pStep->pSelect, 0), 
          sqlitexIdListDup(db, pStep->pIdList), 
          pParse->eOrconf
        );
        break;
      }
      case TK_DELETE: {
        sqlitexDeleteFrom(pParse, 
          targetSrcList(pParse, pStep),
          sqlitexExprDup(db, pStep->pWhere, 0), 0, 0, 0
        );
        break;
      }
      default: assert( pStep->op==TK_SELECT ); {
        SelectDest sDest;
        Select *pSelect = sqlitexSelectDup(db, pStep->pSelect, 0);
        sqlitexSelectDestInit(&sDest, SRT_Discard, 0);
        sqlitexSelect(pParse, pSelect, &sDest);
        sqlitexSelectDelete(db, pSelect);
        break;
      }
    } 
    if( pStep->op!=TK_SELECT ){
      sqlitexVdbeAddOp0(v, OP_ResetCount);
    }
  }

  return 0;
}

#ifdef SQLITE_ENABLE_EXPLAIN_COMMENTS
/*
** This function is used to add VdbeComment() annotations to a VDBE
** program. It is not used in production code, only for debugging.
*/
static const char *onErrorText(int onError){
  switch( onError ){
    case OE_Abort:    return "abort";
    case OE_Rollback: return "rollback";
    case OE_Fail:     return "fail";
    case OE_Replace:  return "replace";
    case OE_Ignore:   return "ignore";
    case OE_Default:  return "default";
  }
  return "n/a";
}
#endif

/*
** Parse context structure pFrom has just been used to create a sub-vdbe
** (trigger program). If an error has occurred, transfer error information
** from pFrom to pTo.
*/
static void transferParseError(Parse *pTo, Parse *pFrom){
  assert( pFrom->zErrMsg==0 || pFrom->nErr );
  assert( pTo->zErrMsg==0 || pTo->nErr );
  if( pTo->nErr==0 ){
    pTo->zErrMsg = pFrom->zErrMsg;
    pTo->nErr = pFrom->nErr;
  }else{
    sqlitexDbFree(pFrom->db, pFrom->zErrMsg);
  }
}

/*
** Create and populate a new TriggerPrg object with a sub-program 
** implementing trigger pTrigger with ON CONFLICT policy orconf.
*/
static TriggerPrg *codeRowTrigger(
  Parse *pParse,       /* Current parse context */
  Trigger *pTrigger,   /* Trigger to code */
  Table *pTab,         /* The table pTrigger is attached to */
  int orconf           /* ON CONFLICT policy to code trigger program with */
){
  Parse *pTop = sqlitexParseToplevel(pParse);
  sqlitex *db = pParse->db;   /* Database handle */
  TriggerPrg *pPrg;           /* Value to return */
  Expr *pWhen = 0;            /* Duplicate of trigger WHEN expression */
  Vdbe *v;                    /* Temporary VM */
  NameContext sNC;            /* Name context for sub-vdbe */
  SubProgram *pProgram = 0;   /* Sub-vdbe for trigger program */
  Parse *pSubParse;           /* Parse context for sub-vdbe */
  int iEndTrigger = 0;        /* Label to jump to if WHEN is false */

  assert( pTrigger->zName==0 || pTab==tableOfTrigger(pTrigger) );
  assert( pTop->pVdbe );

  /* Allocate the TriggerPrg and SubProgram objects. To ensure that they
  ** are freed if an error occurs, link them into the Parse.pTriggerPrg 
  ** list of the top-level Parse object sooner rather than later.  */
  pPrg = sqlitexDbMallocZero(db, sizeof(TriggerPrg));
  if( !pPrg ) return 0;
  pPrg->pNext = pTop->pTriggerPrg;
  pTop->pTriggerPrg = pPrg;
  pPrg->pProgram = pProgram = sqlitexDbMallocZero(db, sizeof(SubProgram));
  if( !pProgram ) return 0;
  sqlitexVdbeLinkSubProgram(pTop->pVdbe, pProgram);
  pPrg->pTrigger = pTrigger;
  pPrg->orconf = orconf;
  pPrg->aColmask[0] = 0xffffffff;
  pPrg->aColmask[1] = 0xffffffff;

  /* Allocate and populate a new Parse context to use for coding the 
  ** trigger sub-program.  */
  pSubParse = sqlitexStackAllocZero(db, sizeof(Parse));
  if( !pSubParse ) return 0;
  memset(&sNC, 0, sizeof(sNC));
  sNC.pParse = pSubParse;
  pSubParse->db = db;
  pSubParse->pTriggerTab = pTab;
  pSubParse->pToplevel = pTop;
  pSubParse->zAuthContext = pTrigger->zName;
  pSubParse->eTriggerOp = pTrigger->op;
  pSubParse->nQueryLoop = pParse->nQueryLoop;

  v = sqlitexGetVdbe(pSubParse);
  if( v ){
    VdbeComment((v, "Start: %s.%s (%s %s%s%s ON %s)", 
      pTrigger->zName, onErrorText(orconf),
      (pTrigger->tr_tm==TRIGGER_BEFORE ? "BEFORE" : "AFTER"),
        (pTrigger->op==TK_UPDATE ? "UPDATE" : ""),
        (pTrigger->op==TK_INSERT ? "INSERT" : ""),
        (pTrigger->op==TK_DELETE ? "DELETE" : ""),
      pTab->zName
    ));
#ifndef SQLITE_OMIT_TRACE
    sqlitexVdbeChangeP4(v, -1, 
      sqlitexMPrintf(db, "-- TRIGGER %s", pTrigger->zName), P4_DYNAMIC
    );
#endif

    /* If one was specified, code the WHEN clause. If it evaluates to false
    ** (or NULL) the sub-vdbe is immediately halted by jumping to the 
    ** OP_Halt inserted at the end of the program.  */
    if( pTrigger->pWhen ){
      pWhen = sqlitexExprDup(db, pTrigger->pWhen, 0);
      if( SQLITE_OK==sqlitexResolveExprNames(&sNC, pWhen) 
       && db->mallocFailed==0 
      ){
        iEndTrigger = sqlitexVdbeMakeLabel(v);
        sqlitexExprIfFalse(pSubParse, pWhen, iEndTrigger, SQLITE_JUMPIFNULL);
      }
      sqlitexExprDelete(db, pWhen);
    }

    /* Code the trigger program into the sub-vdbe. */
    codeTriggerProgram(pSubParse, pTrigger->step_list, orconf);

    /* Insert an OP_Halt at the end of the sub-program. */
    if( iEndTrigger ){
      sqlitexVdbeResolveLabel(v, iEndTrigger);
    }
    sqlitexVdbeAddOp0(v, OP_Halt);
    VdbeComment((v, "End: %s.%s", pTrigger->zName, onErrorText(orconf)));

    transferParseError(pParse, pSubParse);
    if( db->mallocFailed==0 ){
      pProgram->aOp = sqlitexVdbeTakeOpArray(v, &pProgram->nOp, &pTop->nMaxArg);
    }
    pProgram->nMem = pSubParse->nMem;
    pProgram->nCsr = pSubParse->nTab;
    pProgram->nOnce = pSubParse->nOnce;
    pProgram->token = (void *)pTrigger;
    pPrg->aColmask[0] = pSubParse->oldmask;
    pPrg->aColmask[1] = pSubParse->newmask;
    sqlitexVdbeDelete(v);
  }

  assert( !pSubParse->pAinc       && !pSubParse->pZombieTab );
  assert( !pSubParse->pTriggerPrg && !pSubParse->nMaxArg );
  sqlitexParserReset(pSubParse);
  sqlitexStackFree(db, pSubParse);

  return pPrg;
}
    
/*
** Return a pointer to a TriggerPrg object containing the sub-program for
** trigger pTrigger with default ON CONFLICT algorithm orconf. If no such
** TriggerPrg object exists, a new object is allocated and populated before
** being returned.
*/
static TriggerPrg *getRowTrigger(
  Parse *pParse,       /* Current parse context */
  Trigger *pTrigger,   /* Trigger to code */
  Table *pTab,         /* The table trigger pTrigger is attached to */
  int orconf           /* ON CONFLICT algorithm. */
){
  Parse *pRoot = sqlitexParseToplevel(pParse);
  TriggerPrg *pPrg;

  assert( pTrigger->zName==0 || pTab==tableOfTrigger(pTrigger) );

  /* It may be that this trigger has already been coded (or is in the
  ** process of being coded). If this is the case, then an entry with
  ** a matching TriggerPrg.pTrigger field will be present somewhere
  ** in the Parse.pTriggerPrg list. Search for such an entry.  */
  for(pPrg=pRoot->pTriggerPrg; 
      pPrg && (pPrg->pTrigger!=pTrigger || pPrg->orconf!=orconf); 
      pPrg=pPrg->pNext
  );

  /* If an existing TriggerPrg could not be located, create a new one. */
  if( !pPrg ){
    pPrg = codeRowTrigger(pParse, pTrigger, pTab, orconf);
  }

  return pPrg;
}

/*
** Generate code for the trigger program associated with trigger p on 
** table pTab. The reg, orconf and ignoreJump parameters passed to this
** function are the same as those described in the header function for
** sqlitexCodeRowTrigger()
*/
void sqlitexCodeRowTriggerDirect(
  Parse *pParse,       /* Parse context */
  Trigger *p,          /* Trigger to code */
  Table *pTab,         /* The table to code triggers from */
  int reg,             /* Reg array containing OLD.* and NEW.* values */
  int orconf,          /* ON CONFLICT policy */
  int ignoreJump       /* Instruction to jump to for RAISE(IGNORE) */
){
  Vdbe *v = sqlitexGetVdbe(pParse); /* Main VM */
  TriggerPrg *pPrg;
  pPrg = getRowTrigger(pParse, p, pTab, orconf);
  assert( pPrg || pParse->nErr || pParse->db->mallocFailed );

  /* Code the OP_Program opcode in the parent VDBE. P4 of the OP_Program 
  ** is a pointer to the sub-vdbe containing the trigger program.  */
  if( pPrg ){
    int bRecursive = (p->zName && 0==(pParse->db->flags&SQLITE_RecTriggers));

    sqlitexVdbeAddOp3(v, OP_Program, reg, ignoreJump, ++pParse->nMem);
    sqlitexVdbeChangeP4(v, -1, (const char *)pPrg->pProgram, P4_SUBPROGRAM);
    VdbeComment(
        (v, "Call: %s.%s", (p->zName?p->zName:"fkey"), onErrorText(orconf)));

    /* Set the P5 operand of the OP_Program instruction to non-zero if
    ** recursive invocation of this trigger program is disallowed. Recursive
    ** invocation is disallowed if (a) the sub-program is really a trigger,
    ** not a foreign key action, and (b) the flag to enable recursive triggers
    ** is clear.  */
    sqlitexVdbeChangeP5(v, (u8)bRecursive);
  }
}

/*
** This is called to code the required FOR EACH ROW triggers for an operation
** on table pTab. The operation to code triggers for (INSERT, UPDATE or DELETE)
** is given by the op parameter. The tr_tm parameter determines whether the
** BEFORE or AFTER triggers are coded. If the operation is an UPDATE, then
** parameter pChanges is passed the list of columns being modified.
**
** If there are no triggers that fire at the specified time for the specified
** operation on pTab, this function is a no-op.
**
** The reg argument is the address of the first in an array of registers 
** that contain the values substituted for the new.* and old.* references
** in the trigger program. If N is the number of columns in table pTab
** (a copy of pTab->nCol), then registers are populated as follows:
**
**   Register       Contains
**   ------------------------------------------------------
**   reg+0          OLD.rowid
**   reg+1          OLD.* value of left-most column of pTab
**   ...            ...
**   reg+N          OLD.* value of right-most column of pTab
**   reg+N+1        NEW.rowid
**   reg+N+2        OLD.* value of left-most column of pTab
**   ...            ...
**   reg+N+N+1      NEW.* value of right-most column of pTab
**
** For ON DELETE triggers, the registers containing the NEW.* values will
** never be accessed by the trigger program, so they are not allocated or 
** populated by the caller (there is no data to populate them with anyway). 
** Similarly, for ON INSERT triggers the values stored in the OLD.* registers
** are never accessed, and so are not allocated by the caller. So, for an
** ON INSERT trigger, the value passed to this function as parameter reg
** is not a readable register, although registers (reg+N) through 
** (reg+N+N+1) are.
**
** Parameter orconf is the default conflict resolution algorithm for the
** trigger program to use (REPLACE, IGNORE etc.). Parameter ignoreJump
** is the instruction that control should jump to if a trigger program
** raises an IGNORE exception.
*/
void sqlitexCodeRowTrigger(
  Parse *pParse,       /* Parse context */
  Trigger *pTrigger,   /* List of triggers on table pTab */
  int op,              /* One of TK_UPDATE, TK_INSERT, TK_DELETE */
  ExprList *pChanges,  /* Changes list for any UPDATE OF triggers */
  int tr_tm,           /* One of TRIGGER_BEFORE, TRIGGER_AFTER */
  Table *pTab,         /* The table to code triggers from */
  int reg,             /* The first in an array of registers (see above) */
  int orconf,          /* ON CONFLICT policy */
  int ignoreJump       /* Instruction to jump to for RAISE(IGNORE) */
){
  Trigger *p;          /* Used to iterate through pTrigger list */

  assert( op==TK_UPDATE || op==TK_INSERT || op==TK_DELETE );
  assert( tr_tm==TRIGGER_BEFORE || tr_tm==TRIGGER_AFTER );
  assert( (op==TK_UPDATE)==(pChanges!=0) );

  for(p=pTrigger; p; p=p->pNext){

    /* Sanity checking:  The schema for the trigger and for the table are
    ** always defined.  The trigger must be in the same schema as the table
    ** or else it must be a TEMP trigger. */
    assert( p->pSchema!=0 );
    assert( p->pTabSchema!=0 );
    assert( p->pSchema==p->pTabSchema 
         || p->pSchema==pParse->db->aDb[1].pSchema );

    /* Determine whether we should code this trigger */
    if( p->op==op 
     && p->tr_tm==tr_tm 
     && checkColumnOverlap(p->pColumns, pChanges)
    ){
      sqlitexCodeRowTriggerDirect(pParse, p, pTab, reg, orconf, ignoreJump);
    }
  }
}

/*
** Triggers may access values stored in the old.* or new.* pseudo-table. 
** This function returns a 32-bit bitmask indicating which columns of the 
** old.* or new.* tables actually are used by triggers. This information 
** may be used by the caller, for example, to avoid having to load the entire
** old.* record into memory when executing an UPDATE or DELETE command.
**
** Bit 0 of the returned mask is set if the left-most column of the
** table may be accessed using an [old|new].<col> reference. Bit 1 is set if
** the second leftmost column value is required, and so on. If there
** are more than 32 columns in the table, and at least one of the columns
** with an index greater than 32 may be accessed, 0xffffffff is returned.
**
** It is not possible to determine if the old.rowid or new.rowid column is 
** accessed by triggers. The caller must always assume that it is.
**
** Parameter isNew must be either 1 or 0. If it is 0, then the mask returned
** applies to the old.* table. If 1, the new.* table.
**
** Parameter tr_tm must be a mask with one or both of the TRIGGER_BEFORE
** and TRIGGER_AFTER bits set. Values accessed by BEFORE triggers are only
** included in the returned mask if the TRIGGER_BEFORE bit is set in the
** tr_tm parameter. Similarly, values accessed by AFTER triggers are only
** included in the returned mask if the TRIGGER_AFTER bit is set in tr_tm.
*/
u32 sqlitexTriggerColmask(
  Parse *pParse,       /* Parse context */
  Trigger *pTrigger,   /* List of triggers on table pTab */
  ExprList *pChanges,  /* Changes list for any UPDATE OF triggers */
  int isNew,           /* 1 for new.* ref mask, 0 for old.* ref mask */
  int tr_tm,           /* Mask of TRIGGER_BEFORE|TRIGGER_AFTER */
  Table *pTab,         /* The table to code triggers from */
  int orconf           /* Default ON CONFLICT policy for trigger steps */
){
  const int op = pChanges ? TK_UPDATE : TK_DELETE;
  u32 mask = 0;
  Trigger *p;

  assert( isNew==1 || isNew==0 );
  for(p=pTrigger; p; p=p->pNext){
    if( p->op==op && (tr_tm&p->tr_tm)
     && checkColumnOverlap(p->pColumns,pChanges)
    ){
      TriggerPrg *pPrg;
      pPrg = getRowTrigger(pParse, p, pTab, orconf);
      if( pPrg ){
        mask |= pPrg->aColmask[isNew];
      }
    }
  }

  return mask;
}

#endif /* !defined(SQLITE_OMIT_TRIGGER) */
