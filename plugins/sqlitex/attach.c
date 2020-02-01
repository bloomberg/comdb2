/*
** 2003 April 6
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used to implement the ATTACH and DETACH commands.
*/
#include <string.h>
#include "sqliteInt.h"

#ifndef SQLITE_OMIT_ATTACH
/*
** Resolve an expression that was part of an ATTACH or DETACH statement. This
** is slightly different from resolving a normal SQL expression, because simple
** identifiers are treated as strings, not possible column names or aliases.
**
** i.e. if the parser sees:
**
**     ATTACH DATABASE abc AS def
**
** it treats the two expressions as literal strings 'abc' and 'def' instead of
** looking for columns of the same name.
**
** This only applies to the root node of pExpr, so the statement:
**
**     ATTACH DATABASE abc||def AS 'db2'
**
** will fail because neither abc or def can be resolved.
*/
static int resolveAttachExpr(NameContext *pName, Expr *pExpr)
{
  int rc = SQLITE_OK;
  if( pExpr ){
    if( pExpr->op!=TK_ID ){
      rc = sqlitexResolveExprNames(pName, pExpr);
    }else{
      pExpr->op = TK_STRING;
    }
  }
  return rc;
}

int comdb2_dynamic_attachXX(sqlitex *db, sqlitex_context *context, sqlitex_value **argv,
      const char *zName, const char *zFile, char **pzErrDyn, int version)
{
  int i;
  int rc = 0;
  char *zPath = 0;
  char *zErr = 0;
  unsigned int flags;
  Db *aNew;
  char *zErrDyn = 0;
  sqlitex_vfs *pVfs;
  char *dbName;
  char *tblName;
  int  iFndDb;

  /* Check for the following errors:
  **
  **     * Too many attached databases,
  **     * Transaction currently open
  **     * Specified database name already being used.
  */
  if( db->nDb>=db->aLimit[SQLITE_LIMIT_ATTACHED]+2 ){
    zErrDyn = sqlitexMPrintf(db, "too many attached databases - max %d", 
      db->aLimit[SQLITE_LIMIT_ATTACHED]
    );
    goto attach_error;
  }
  if( !db->autoCommit ){
    zErrDyn = sqlitexMPrintf(db, "cannot ATTACH database within transaction");
    goto attach_error;
  }

  /* we store one Btree per foreign db, therefore we need to extract the dbname from table */
  dbName = sqlitexDbStrDup(db, zName);
  if( rc==SQLITE_OK && dbName==0 ){
    rc = SQLITE_NOMEM;
    goto attach_error;
  }
  else
  {
     /* to keep table names unique, zName includes the dbname as well.
        But the Btree will be named after dbname, since they will collect all
        the tables for this db */
     char *ptr = strchr(dbName, '.');
     if (ptr)
     {
        *ptr = '\0';
        tblName = (ptr+1);
     }
     else
     {
        fprintf(stderr, "%s: remote dbname URI incomplete \"%s\"?\n", __func__, dbName);
        zErrDyn = sqlitexMPrintf(db, "%s: remote dbname URI incomplete \"%s\"?\n", __func__, dbName);
        sqlitexDbFree(db, dbName);

        rc = SQLITE_ERROR;
        goto attach_error;
     }
  }

  for(i=0; i<db->nDb; i++){
    char *z = db->aDb[i].zName;
    assert( z && dbName);
    if( sqlitexStrICmp(z, dbName)==0 ){
    /*
      zErrDyn = sqlitexMPrintf(db, "database %s is already in use", zName);
      goto attach_error;
      */
      break;
    }
  }

  iFndDb = i;
  if(i==db->nDb)
  {
     /* Allocate the new entry in the db->aDb[] array and initialize the schema
      ** hash tables.
      */
     if( db->aDb==db->aDbStatic ){
        aNew = sqlitexDbMallocRaw(db, sizeof(db->aDb[0])*3 );
        if( aNew==0 ) return -1;
        memcpy(aNew, db->aDb, sizeof(db->aDb[0])*2);
     }else{
        aNew = sqlitexDbRealloc(db, db->aDb, sizeof(db->aDb[0])*(db->nDb+1) );
        if( aNew==0 ) return -1;
     }
     db->aDb = aNew;
     aNew = &db->aDb[db->nDb];
     memset(aNew, 0, sizeof(*aNew));

     /* Open the database file. If the btree is successfully opened, use
      ** it to obtain the database schema. At this point the schema may
      ** or may not be initialized.
      */
     flags = db->openFlags;
     rc = sqlitexParseUri(db->pVfs->zName, zFile, &flags, &pVfs, &zPath, &zErr);
     if( rc!=SQLITE_OK ){
        if( rc==SQLITE_NOMEM ) db->mallocFailed = 1;
        if (context)
           sqlitex_result_error(context, zErr, -1);
        sqlitex_free(zErr);
        return -1;
     }
     assert( pVfs );
     flags |= SQLITE_OPEN_MAIN_DB;
     rc = sqlite3BtreeOpen(pVfs, zPath, db, &aNew->pBt, 0, flags);
     sqlitex_free( zPath );
     db->nDb++;
     if( rc==SQLITE_CONSTRAINT ){
        rc = SQLITE_ERROR;
        zErrDyn = sqlitexMPrintf(db, "database is already attached");
     }else if( rc==SQLITE_OK ){
        Pager *pPager;
        aNew->pSchema = sqlitexSchemaGet(db, aNew->pBt);
        if( !aNew->pSchema ){
           rc = SQLITE_NOMEM;
        }else if( aNew->pSchema->file_format && aNew->pSchema->enc!=ENC(db) ){
           zErrDyn = sqlitexMPrintf(db, 
                 "attached databases must use the same text encoding as main database");
           rc = SQLITE_ERROR;
        }
        sqlite3BtreeEnter(aNew->pBt);
        pPager = sqlite3BtreePager(aNew->pBt);
        sqlite3PagerLockingMode(pPager, db->dfltLockMode);
        sqlite3BtreeSecureDelete(aNew->pBt,
              sqlite3BtreeSecureDelete(db->aDb[0].pBt,-1) );
#ifndef SQLITE_OMIT_PAGER_PRAGMAS
        sqlite3BtreeSetPagerFlags(aNew->pBt, 3 | (db->flags & PAGER_FLAGS_MASK));
#endif
     }
     aNew->safety_level = 3;
     aNew->zName = dbName;


#ifdef SQLITE_HAS_CODEC
     if( rc==SQLITE_OK ){
        extern int sqlitexCodecAttach(sqlitex*, int, const void*, int);
        extern void sqlitexCodecGetKey(sqlitex*, int, void**, int*);
        int nKey;
        char *zKey;
        int t = sqlitex_value_type(argv[2]);
        switch( t ){
           case SQLITE_INTEGER:
           case SQLITE_FLOAT:
              zErrDyn = sqlitexDbStrDup(db, "Invalid key value");
              rc = SQLITE_ERROR;
              break;

           case SQLITE3_TEXT:
           case SQLITE_BLOB:
              nKey = sqlitex_value_bytes(argv[2]);
              zKey = (char *)sqlitex_value_blob(argv[2]);
              rc = sqlitexCodecAttach(db, db->nDb-1, zKey, nKey);
              break;

           case SQLITE_NULL:
              /* No key specified.  Use the key from the main database */
              sqlitexCodecGetKey(db, 0, (void**)&zKey, &nKey);
              if( nKey>0 || sqlite3BtreeGetOptimalReserve(db->aDb[0].pBt)>0 ){
                 rc = sqlitexCodecAttach(db, db->nDb-1, zKey, nKey);
              }
              break;
        }
     }
#endif
  }

  /* If the file was opened successfully, read the schema for the new database.
   ** If this fails, or if opening the file failed, then close the file and 
   ** remove the entry from the db->aDb[] array. i.e. put everything back the way
   ** we found it.
   */
  if( rc==SQLITE_OK ){
     Table *p;

     sqlite3BtreeEnterAll(db);
     rc = sqlitexInitTable(db, &zErrDyn, zName);

     /* COMDB2 MODIFICATION */
     /* Need to set the version to the table to support per table schema refresh */
     p = sqlitexHashFind(&db->aDb[iFndDb].pSchema->tblHash, tblName);
     if(!p)
     {
       fprintf(stderr, "%s: failed to find table \"%s\" after init\n", __func__, tblName);
       rc = SQLITE_ERROR;
     }
     else
     {
       p->version = version;
       p->iDb = iFndDb;
     }

     sqlite3BtreeLeaveAll(db);
  }
#ifdef SQLITE_USER_AUTHENTICATION
  if( rc==SQLITE_OK ){
    u8 newAuth = 0;
    rc = sqlitexUserAuthCheckLogin(db, zName, &newAuth);
    if( newAuth<db->auth.authLevel ){
      rc = SQLITE_AUTH_USER;
    }
  }
#endif
  if( rc ){
     int iDb = db->nDb - 1;
     assert( iDb>=2 );
     if( db->aDb[iDb].pBt ){
        sqlite3BtreeClose(db->aDb[iDb].pBt);
        db->aDb[iDb].pBt = 0;
        db->aDb[iDb].pSchema = 0;
     }
     sqlitexResetAllSchemasOfConnection(db);
     db->nDb = iDb;
     if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ){
        db->mallocFailed = 1;
        sqlitexDbFree(db, zErrDyn);
        zErrDyn = sqlitexMPrintf(db, "out of memory");
     }else if( zErrDyn==0 ){
        zErrDyn = sqlitexMPrintf(db, "unable to open database: %s", zFile);
     }
     goto attach_error;
  }

  return 0;

attach_error:
  /* Return an error if we get here */
  *pzErrDyn = zErrDyn;
  return rc;
}


/*
** An SQL user-function registered to do the work of an ATTACH statement. The
** three arguments to the function come directly from an attach statement:
**
**     ATTACH DATABASE x AS y KEY z
**
**     SELECT sqlite_attach(x, y, z)
**
** If the optional "KEY z" syntax is omitted, an SQL NULL is passed as the
** third argument.
*/
static void attachFunc(
  sqlitex_context *context,
  int NotUsed,
  sqlitex_value **argv
){
  sqlitex *db = sqlitex_context_db_handle(context);
  const char *zName;
  const char *zFile;
  char *zErrDyn;
  int rc = 0;


  UNUSED_PARAMETER(NotUsed);

  zFile = (const char *)sqlitex_value_text(argv[0]);
  zName = (const char *)sqlitex_value_text(argv[1]);
  if( zFile==0 ) zFile = "";
  if( zName==0 ) zName = "";

  zErrDyn = NULL;
  rc = comdb2_dynamic_attachXX(db, context, argv, zName, zFile, &zErrDyn, 0);
  if( zErrDyn ){
    sqlitex_result_error(context, zErrDyn, -1);
    sqlitexDbFree(db, zErrDyn);
  }
  if( rc ) sqlitex_result_error_code(context, rc);
}

/*
** An SQL user-function registered to do the work of an DETACH statement. The
** three arguments to the function come directly from a detach statement:
**
**     DETACH DATABASE x
**
**     SELECT sqlite_detach(x)
*/
static void detachFunc(
  sqlitex_context *context,
  int NotUsed,
  sqlitex_value **argv
){
  const char *zName = (const char *)sqlitex_value_text(argv[0]);
  sqlitex *db = sqlitex_context_db_handle(context);
  int i;
  Db *pDb = 0;
  char zErr[128];

  UNUSED_PARAMETER(NotUsed);

  if( zName==0 ) zName = "";
  for(i=0; i<db->nDb; i++){
    pDb = &db->aDb[i];
    if( pDb->pBt==0 ) continue;
    if( sqlitexStrICmp(pDb->zName, zName)==0 ) break;
  }

  if( i>=db->nDb ){
    sqlitex_snprintf(sizeof(zErr),zErr, "no such database: %s", zName);
    goto detach_error;
  }
  if( i<2 ){
    sqlitex_snprintf(sizeof(zErr),zErr, "cannot detach database %s", zName);
    goto detach_error;
  }
  if( !db->autoCommit ){
    sqlitex_snprintf(sizeof(zErr), zErr,
                     "cannot DETACH database within transaction");
    goto detach_error;
  }
  if( sqlite3BtreeIsInReadTrans(pDb->pBt) || sqlite3BtreeIsInBackup(pDb->pBt) ){
    sqlitex_snprintf(sizeof(zErr),zErr, "database %s is locked", zName);
    goto detach_error;
  }

  sqlite3BtreeClose(pDb->pBt);
  pDb->pBt = 0;
  pDb->pSchema = 0;
  sqlitexResetAllSchemasOfConnection(db);
  return;

detach_error:
  sqlitex_result_error(context, zErr, -1);
}

/*
** This procedure generates VDBE code for a single invocation of either the
** sqlite_detach() or sqlite_attach() SQL user functions.
*/
static void codeAttach(
  Parse *pParse,       /* The parser context */
  int type,            /* Either SQLITE_ATTACH or SQLITE_DETACH */
  FuncDef const *pFunc,/* FuncDef wrapper for detachFunc() or attachFunc() */
  Expr *pAuthArg,      /* Expression to pass to authorization callback */
  Expr *pFilename,     /* Name of database file */
  Expr *pDbname,       /* Name of the database to use internally */
  Expr *pKey           /* Database key for encryption extension */
){
  int rc;
  NameContext sName;
  Vdbe *v;
  sqlitex* db = pParse->db;
  int regArgs;

  memset(&sName, 0, sizeof(NameContext));
  sName.pParse = pParse;

  if( 
      SQLITE_OK!=(rc = resolveAttachExpr(&sName, pFilename)) ||
      SQLITE_OK!=(rc = resolveAttachExpr(&sName, pDbname)) ||
      SQLITE_OK!=(rc = resolveAttachExpr(&sName, pKey))
  ){
    pParse->nErr++;
    goto attach_end;
  }

#ifndef SQLITE_OMIT_AUTHORIZATION
  if( pAuthArg ){
    char *zAuthArg;
    if( pAuthArg->op==TK_STRING ){
      zAuthArg = pAuthArg->u.zToken;
    }else{
      zAuthArg = 0;
    }
    rc = sqlitexAuthCheck(pParse, type, zAuthArg, 0, 0);
    if(rc!=SQLITE_OK ){
      goto attach_end;
    }
  }
#endif /* SQLITE_OMIT_AUTHORIZATION */


  v = sqlitexGetVdbe(pParse);
  regArgs = sqlitexGetTempRange(pParse, 4);
  sqlitexExprCode(pParse, pFilename, regArgs);
  sqlitexExprCode(pParse, pDbname, regArgs+1);
  sqlitexExprCode(pParse, pKey, regArgs+2);

  assert( v || db->mallocFailed );
  if( v ){
    sqlitexVdbeAddOp3(v, OP_Function, 0, regArgs+3-pFunc->nArg, regArgs+3);
    assert( pFunc->nArg==-1 || (pFunc->nArg&0xff)==pFunc->nArg );
    sqlitexVdbeChangeP5(v, (u8)(pFunc->nArg));
    sqlitexVdbeChangeP4(v, -1, (char *)pFunc, P4_FUNCDEF);

    /* Code an OP_Expire. For an ATTACH statement, set P1 to true (expire this
    ** statement only). For DETACH, set it to false (expire all existing
    ** statements).
    */
    sqlitexVdbeAddOp1(v, OP_Expire, (type==SQLITE_ATTACH));
  }
  
attach_end:
  sqlitexExprDelete(db, pFilename);
  sqlitexExprDelete(db, pDbname);
  sqlitexExprDelete(db, pKey);
}

/*
** Called by the parser to compile a DETACH statement.
**
**     DETACH pDbname
*/
void sqlitexDetach(Parse *pParse, Expr *pDbname){
  static const FuncDef detach_func = {
    1,                /* nArg */
    SQLITE_UTF8,      /* funcFlags */
    0,                /* pUserData */
    0,                /* pNext */
    detachFunc,       /* xFunc */
    0,                /* xStep */
    0,                /* xFinalize */
    "sqlite_detach",  /* zName */
    0,                /* pHash */
    0                 /* pDestructor */
  };
  codeAttach(pParse, SQLITE_DETACH, &detach_func, pDbname, 0, 0, pDbname);
}

/*
** Called by the parser to compile an ATTACH statement.
**
**     ATTACH p AS pDbname KEY pKey
*/
void sqlitexAttach(Parse *pParse, Expr *p, Expr *pDbname, Expr *pKey){
  static const FuncDef attach_func = {
    3,                /* nArg */
    SQLITE_UTF8,      /* funcFlags */
    0,                /* pUserData */
    0,                /* pNext */
    attachFunc,       /* xFunc */
    0,                /* xStep */
    0,                /* xFinalize */
    "sqlite_attach",  /* zName */
    0,                /* pHash */
    0                 /* pDestructor */
  };
  codeAttach(pParse, SQLITE_ATTACH, &attach_func, p, p, pDbname, pKey);
}
#endif /* SQLITE_OMIT_ATTACH */

/*
** Initialize a DbFixer structure.  This routine must be called prior
** to passing the structure to one of the sqliteFixAAAA() routines below.
*/
void sqlitexFixInit(
  DbFixer *pFix,      /* The fixer to be initialized */
  Parse *pParse,      /* Error messages will be written here */
  int iDb,            /* This is the database that must be used */
  const char *zType,  /* "view", "trigger", or "index" */
  const Token *pName  /* Name of the view, trigger, or index */
){
  sqlitex *db;

  db = pParse->db;
  assert( db->nDb>iDb );
  pFix->pParse = pParse;
  pFix->zDb = db->aDb[iDb].zName;
  pFix->pSchema = db->aDb[iDb].pSchema;
  pFix->zType = zType;
  pFix->pName = pName;
  pFix->bVarOnly = (iDb==1);
}

/*
** The following set of routines walk through the parse tree and assign
** a specific database to all table references where the database name
** was left unspecified in the original SQL statement.  The pFix structure
** must have been initialized by a prior call to sqlitexFixInit().
**
** These routines are used to make sure that an index, trigger, or
** view in one database does not refer to objects in a different database.
** (Exception: indices, triggers, and views in the TEMP database are
** allowed to refer to anything.)  If a reference is explicitly made
** to an object in a different database, an error message is added to
** pParse->zErrMsg and these routines return non-zero.  If everything
** checks out, these routines return 0.
*/
int sqlitexFixSrcList(
  DbFixer *pFix,       /* Context of the fixation */
  SrcList *pList       /* The Source list to check and modify */
){
  int i;
  const char *zDb;
  struct SrcList_item *pItem;

  if( NEVER(pList==0) ) return 0;
  zDb = pFix->zDb;
  for(i=0, pItem=pList->a; i<pList->nSrc; i++, pItem++){
    if( pFix->bVarOnly==0 ){
      if( pItem->zDatabase && sqlitexStrICmp(pItem->zDatabase, zDb) ){
        sqlitexErrorMsg(pFix->pParse,
            "%s %T cannot reference objects in database %s",
            pFix->zType, pFix->pName, pItem->zDatabase);
        return 1;
      }
      sqlitexDbFree(pFix->pParse->db, pItem->zDatabase);
      pItem->zDatabase = 0;
      pItem->pSchema = pFix->pSchema;
    }
#if !defined(SQLITE_OMIT_VIEW) || !defined(SQLITE_OMIT_TRIGGER)
    if( sqlitexFixSelect(pFix, pItem->pSelect) ) return 1;
    if( sqlitexFixExpr(pFix, pItem->pOn) ) return 1;
#endif
  }
  return 0;
}
#if !defined(SQLITE_OMIT_VIEW) || !defined(SQLITE_OMIT_TRIGGER)
int sqlitexFixSelect(
  DbFixer *pFix,       /* Context of the fixation */
  Select *pSelect      /* The SELECT statement to be fixed to one database */
){
  while( pSelect ){
    if( sqlitexFixExprList(pFix, pSelect->pEList) ){
      return 1;
    }
    if( sqlitexFixSrcList(pFix, pSelect->pSrc) ){
      return 1;
    }
    if( sqlitexFixExpr(pFix, pSelect->pWhere) ){
      return 1;
    }
    if( sqlitexFixExprList(pFix, pSelect->pGroupBy) ){
      return 1;
    }
    if( sqlitexFixExpr(pFix, pSelect->pHaving) ){
      return 1;
    }
    if( sqlitexFixExprList(pFix, pSelect->pOrderBy) ){
      return 1;
    }
    if( sqlitexFixExpr(pFix, pSelect->pLimit) ){
      return 1;
    }
    if( sqlitexFixExpr(pFix, pSelect->pOffset) ){
      return 1;
    }
    pSelect = pSelect->pPrior;
  }
  return 0;
}
int sqlitexFixExpr(
  DbFixer *pFix,     /* Context of the fixation */
  Expr *pExpr        /* The expression to be fixed to one database */
){
  while( pExpr ){
    if( pExpr->op==TK_VARIABLE ){
      if( pFix->pParse->db->init.busy ){
        pExpr->op = TK_NULL;
      }else{
        sqlitexErrorMsg(pFix->pParse, "%s cannot use variables", pFix->zType);
        return 1;
      }
    }
    if( ExprHasProperty(pExpr, EP_TokenOnly) ) break;
    if( ExprHasProperty(pExpr, EP_xIsSelect) ){
      if( sqlitexFixSelect(pFix, pExpr->x.pSelect) ) return 1;
    }else{
      if( sqlitexFixExprList(pFix, pExpr->x.pList) ) return 1;
    }
    if( sqlitexFixExpr(pFix, pExpr->pRight) ){
      return 1;
    }
    pExpr = pExpr->pLeft;
  }
  return 0;
}
int sqlitexFixExprList(
  DbFixer *pFix,     /* Context of the fixation */
  ExprList *pList    /* The expression to be fixed to one database */
){
  int i;
  struct ExprList_item *pItem;
  if( pList==0 ) return 0;
  for(i=0, pItem=pList->a; i<pList->nExpr; i++, pItem++){
    if( sqlitexFixExpr(pFix, pItem->pExpr) ){
      return 1;
    }
  }
  return 0;
}
#endif

#ifndef SQLITE_OMIT_TRIGGER
int sqlitexFixTriggerStep(
  DbFixer *pFix,     /* Context of the fixation */
  TriggerStep *pStep /* The trigger step be fixed to one database */
){
  while( pStep ){
    if( sqlitexFixSelect(pFix, pStep->pSelect) ){
      return 1;
    }
    if( sqlitexFixExpr(pFix, pStep->pWhere) ){
      return 1;
    }
    if( sqlitexFixExprList(pFix, pStep->pExprList) ){
      return 1;
    }
    pStep = pStep->pNext;
  }
  return 0;
}
#endif
