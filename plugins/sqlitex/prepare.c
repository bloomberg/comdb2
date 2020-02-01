/*
** 2005 May 25
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains the implementation of the sqlitex_prepare()
** interface, and routines that contribute to loading the database schema
** from disk.
*/
#include "sqliteInt.h"

/* COMDB2 MODIFICATION */
#include "vdbeInt.h"
#include <time.h>
#include <ctype.h>
#include <datetime.h>
void comdb2SetWriteFlag(int);

/*
** Fill the InitData structure with an error message that indicates
** that the database is corrupt.
*/
static void corruptSchema(
  InitData *pData,     /* Initialization context */
  const char *zObj,    /* Object being parsed at the point of error */
  const char *zExtra   /* Error information */
){
  sqlitex *db = pData->db;
  if( !db->mallocFailed && (db->flags & SQLITE_RecoveryMode)==0 ){
    if( zObj==0 ) zObj = "?";
    sqlitexSetString(pData->pzErrMsg, db,
		     "malformed database schema (%s)", zObj);
    if( zExtra ){
      *pData->pzErrMsg = sqlitexMAppendf(db, *pData->pzErrMsg,
					 "%s - %s", *pData->pzErrMsg, zExtra);
    }
  }
  pData->rc = db->mallocFailed ? SQLITE_NOMEM : SQLITE_CORRUPT_BKPT;
}

/*
** This is the callback routine for the code that initializes the
** database.  See sqlitexInit() below for additional information.
** This routine is also called from the OP_ParseSchema opcode of the VDBE.
**
** Each callback contains the following information:
**
**     argv[0] = name of thing being created
**     argv[1] = root page number for table or index. 0 for trigger or view.
**     argv[2] = SQL text for the CREATE statement.
**
*/
int sqlitexInitCallback(void *pInit, int argc, char **argv, char **NotUsed){
  InitData *pData = (InitData*)pInit;
  sqlitex *db = pData->db;
  int iDb = pData->iDb;

  assert( argc==3 );
  UNUSED_PARAMETER2(NotUsed, argc);
  assert( sqlitex_mutex_held(db->mutex) );
  DbClearProperty(db, iDb, DB_Empty);
  if( db->mallocFailed ){
    corruptSchema(pData, argv[0], 0);
    return 1;
  }

  assert( iDb>=0 && iDb<db->nDb );
  if( argv==0 ) return 0;   /* Might happen if EMPTY_RESULT_CALLBACKS are on */
  if( argv[1]==0 ){
    corruptSchema(pData, argv[0], 0);
  }else if( argv[2] && argv[2][0] ){
    /* Call the parser to process a CREATE TABLE, INDEX or VIEW.
    ** But because db->init.busy is set to 1, no VDBE code is generated
    ** or executed.  All the parser does is build the internal data
    ** structures that describe the table, index, or view.
    */
    int rc;
    sqlitex_stmt *pStmt;
    TESTONLY(int rcp);            /* Return code from sqlitex_prepare() */

    assert( db->init.busy );
    db->init.iDb = iDb;
    db->init.newTnum = sqlitexAtoi(argv[1]);
    db->init.orphanTrigger = 0;

    extern int gbl_fdb_track;
    if (gbl_fdb_track)
       fprintf(stderr, "Prep iDb=%d \"%s\"\n", iDb, argv[2]);

    TESTONLY(rcp = ) sqlitex_prepare(db, argv[2], -1, &pStmt, 0);
    rc = db->errCode;
    assert( (rc&0xFF)==(rcp&0xFF) );
    db->init.iDb = 0;
    if( SQLITE_OK!=rc ){
      if( db->init.orphanTrigger ){
        assert( iDb==1 );
      }else{
        pData->rc = rc;
        if( rc==SQLITE_NOMEM ){
          db->mallocFailed = 1;
        }else if( rc!=SQLITE_INTERRUPT && (rc&0xFF)!=SQLITE_LOCKED ){
          corruptSchema(pData, argv[0], sqlitex_errmsg(db));
          extern int gbl_trace_prepare_errors;
          if(gbl_trace_prepare_errors)
            fprintf(stderr, "Prepare \"%s\"\n", argv[2]);
        }
      }
    }
    sqlitex_finalize(pStmt);
  }else if( argv[0]==0 ){
    corruptSchema(pData, 0, 0);
  }else{
    /* If the SQL column is blank it means this is an index that
    ** was created to be the PRIMARY KEY or to fulfill a UNIQUE
    ** constraint for a CREATE TABLE.  The index should have already
    ** been created when we processed the CREATE TABLE.  All we have
    ** to do here is record the root page number for that index.
    */
    Index *pIndex;
    pIndex = sqlitexFindIndex(db, argv[0], db->aDb[iDb].zName);
    if( pIndex==0 ){
      /* This can occur if there exists an index on a TEMP table which
      ** has the same name as another index on a permanent index.  Since
      ** the permanent table is hidden by the TEMP table, we can also
      ** safely ignore the index on the permanent table.
      */
      /* Do Nothing */;
    }else if( sqlitexGetInt32(argv[1], &pIndex->tnum)==0 ){
      corruptSchema(pData, argv[0], "invalid rootpage");
    }
  }
  return 0;
}

/*
** Attempt to read the database schema and initialize internal
** data structures for a single database file.  The index of the
** database file is given by iDb.  iDb==0 is used for the main
** database.  iDb==1 should never be used.  iDb>=2 is used for
** auxiliary databases.  Return one of the SQLITE_ error codes to
** indicate success or failure.
*/
static int sqlitexInitOne(sqlitex *db, int iDb, char **pzErrMsg){
  int rc;
  int i;
#ifndef SQLITE_OMIT_DEPRECATED
  int size;
#endif
  Table *pTab;
  Db *pDb;
  char const *azArg[4];
  int meta[5];
  InitData initData;
  char const *zMasterSchema;
  char const *zMasterName;
  int openedTransaction = 0;

  /*
  ** The master database table has a structure like this
  */
  static const char master_schema[] = 
     "CREATE TABLE sqlite_master(\n"
     "  type text,\n"
     "  name text,\n"
     "  tbl_name text,\n"
     "  rootpage integer,\n"
     "  sql text,\n"
     "  csc2 text"          /* COMDB2 MODIFICATION */
     ")"
  ;
#ifndef SQLITE_OMIT_TEMPDB
  static const char temp_master_schema[] = 
     "CREATE TEMP TABLE sqlite_temp_master(\n"
     "  type text,\n"
     "  name text,\n"
     "  tbl_name text,\n"
     "  rootpage integer,\n"
     "  sql text\n"
     ")"
  ;
#else
  #define temp_master_schema 0
#endif

  assert( iDb>=0 && iDb<db->nDb );
  assert( db->aDb[iDb].pSchema );
  assert( sqlitex_mutex_held(db->mutex) );
  assert( iDb==1 || sqlite3BtreeHoldsMutex(db->aDb[iDb].pBt) );

  /* zMasterSchema and zInitScript are set to point at the master schema
  ** and initialisation script appropriate for the database being
  ** initialized. zMasterName is the name of the master table.
  */
  if( !OMIT_TEMPDB && iDb==1 ){
    zMasterSchema = temp_master_schema;
  }else{
    zMasterSchema = master_schema;
  }
  zMasterName = SCHEMA_TABLE(iDb);

  /* have we created already sqlite_master for this one?
  ** COMDB2_changes: remote shares the same sqlite_master with "main"
  **/
  pTab = sqlitexFindTableCheckOnly(db, zMasterName, /*(iDb<=1)?(db->aDb[iDb].zName):(db->aDb[0].zName)*/ db->aDb[iDb].zName);
  if(pTab == NULL)
  {
    azArg[0] = zMasterName;
    azArg[1] = "1";
    azArg[2] = zMasterSchema;
    azArg[3] = 0;
    initData.db = db;
    initData.iDb = iDb;
    initData.rc = SQLITE_OK;
    initData.pzErrMsg = pzErrMsg;
    sqlitexInitCallback(&initData, 3, (char **)azArg, 0);
    if( initData.rc ){
      rc = initData.rc;
      goto error_out;
    }
    pTab = sqlitexFindTableCheckOnly(db, zMasterName, db->aDb[iDb].zName);
  }
  else
    {
      initData.db = db;
      initData.iDb = iDb;
      initData.rc = SQLITE_OK;
      initData.pzErrMsg = pzErrMsg;
    }

  if( ALWAYS(pTab) ){
    pTab->tabFlags |= TF_Readonly;
  }

  /* Create a cursor to hold the database open
  **/
  pDb = &db->aDb[iDb];
  if( pDb->pBt==0 ){
    if( !OMIT_TEMPDB && ALWAYS(iDb==1) ){
      DbSetProperty(db, 1, DB_SchemaLoaded);
    }
    return SQLITE_OK;
  }

  /* If there is not already a read-only (or read-write) transaction opened
  ** on the b-tree database, open one now. If a transaction is opened, it
  ** will be closed before this function returns.  */
  sqlite3BtreeEnter(pDb->pBt);
  if( !sqlite3BtreeIsInReadTrans(pDb->pBt) ){
    rc = sqlite3BtreeBeginTrans(NULL, pDb->pBt, 0, 0);
    comdb2SetWriteFlag(0);
    if( rc!=SQLITE_OK ){
      sqlitexSetString(pzErrMsg, db, "%s", sqlitexErrStr(rc));
      goto initone_error_out;
    }
    openedTransaction = 1;
  }

  /* Get the database meta information.
  **
  ** Meta values are as follows:
  **    meta[0]   Schema cookie.  Changes with each schema change.
  **    meta[1]   File format of schema layer.
  **    meta[2]   Size of the page cache.
  **    meta[3]   Largest rootpage (auto/incr_vacuum mode)
  **    meta[4]   Db text encoding. 1:UTF-8 2:UTF-16LE 3:UTF-16BE
  **    meta[5]   User version
  **    meta[6]   Incremental vacuum mode
  **    meta[7]   unused
  **    meta[8]   unused
  **    meta[9]   unused
  **
  ** Note: The #defined SQLITE_UTF* symbols in sqliteInt.h correspond to
  ** the possible values of meta[4].
  */
  for(i=0; i<ArraySize(meta); i++){
    sqlite3BtreeGetMeta(pDb->pBt, i+1, (u32 *)&meta[i]);
  }
  pDb->pSchema->schema_cookie = meta[BTREE_SCHEMA_VERSION-1];

  /* If opening a non-empty database, check the text encoding. For the
  ** main database, set sqlitex.enc to the encoding of the main database.
  ** For an attached db, it is an error if the encoding is not the same
  ** as sqlitex.enc.
  */
  if( meta[BTREE_TEXT_ENCODING-1] ){  /* text encoding */
    if( iDb==0 ){
#ifndef SQLITE_OMIT_UTF16
      u8 encoding;
      /* If opening the main database, set ENC(db). */
      encoding = (u8)meta[BTREE_TEXT_ENCODING-1] & 3;
      if( encoding==0 ) encoding = SQLITE_UTF8;
      ENC(db) = encoding;
#else
      SCHEMA_ENC(db) = SQLITE_UTF8;
#endif
    }else{
      /* If opening an attached database, the encoding much match ENC(db) */
      if( meta[BTREE_TEXT_ENCODING-1]!=ENC(db) ){
        sqlitexSetString(pzErrMsg, db, "attached databases must use the same"
			 " text encoding as main database");
        rc = SQLITE_ERROR;
        goto initone_error_out;
      }
    }
  }else{
    DbSetProperty(db, iDb, DB_Empty);
  }
  pDb->pSchema->enc = ENC(db);

  if( pDb->pSchema->cache_size==0 ){
#ifndef SQLITE_OMIT_DEPRECATED
    size = sqlitexAbsInt32(meta[BTREE_DEFAULT_CACHE_SIZE-1]);
    if( size==0 ){ size = SQLITE_DEFAULT_CACHE_SIZE; }
    pDb->pSchema->cache_size = size;
#else
    pDb->pSchema->cache_size = SQLITE_DEFAULT_CACHE_SIZE;
#endif
    sqlite3BtreeSetCacheSize(pDb->pBt, pDb->pSchema->cache_size);
  }

  /*
  ** file_format==1    Version 3.0.0.
  ** file_format==2    Version 3.1.3.  // ALTER TABLE ADD COLUMN
  ** file_format==3    Version 3.1.4.  // ditto but with non-NULL defaults
  ** file_format==4    Version 3.3.0.  // DESC indices.  Boolean constants
  */
  pDb->pSchema->file_format = (u8)meta[BTREE_FILE_FORMAT-1];
  if( pDb->pSchema->file_format==0 ){
    pDb->pSchema->file_format = 1;
  }
  if( pDb->pSchema->file_format>SQLITE_MAX_FILE_FORMAT ){
    sqlitexSetString(pzErrMsg, db, "unsupported file format");
    rc = SQLITE_ERROR;
    goto initone_error_out;
  }

  /* Ticket #2804:  When we open a database in the newer file format,
  ** clear the legacy_file_format pragma flag so that a VACUUM will
  ** not downgrade the database and thus invalidate any descending
  ** indices that the user might have created.
  */
  if( iDb==0 && meta[BTREE_FILE_FORMAT-1]>=4 ){
    db->flags &= ~SQLITE_LegacyFileFmt;
  }

  /* Read the schema information out of the schema tables
  */
  assert( db->init.busy );
  {
    char *zSql;
    zSql = sqlitexMPrintf(db,
        "SELECT name, rootpage, sql FROM '%q'.%s ORDER BY rowid",
        /*(iDb>1)?db->aDb[0].zName:db->aDb[iDb].zName*/db->aDb[iDb].zName, zMasterName);
#ifndef SQLITE_OMIT_AUTHORIZATION
    {
      sqlitex_xauth xAuth;
      xAuth = db->xAuth;
      db->xAuth = 0;
#endif
      rc = sqlitex_exec(db, zSql, sqlitexInitCallback, &initData, 0);
#ifndef SQLITE_OMIT_AUTHORIZATION
      db->xAuth = xAuth;
    }
#endif
    if( rc==SQLITE_OK ) rc = initData.rc;
    sqlitexDbFree(db, zSql);
#ifndef SQLITE_OMIT_ANALYZE
    if( rc==SQLITE_OK ){
      sqlitexAnalysisLoad(db, iDb);
    }
#endif
  }
  if( db->mallocFailed ){
    rc = SQLITE_NOMEM;
    sqlitexResetAllSchemasOfConnection(db);
  }
  if( rc==SQLITE_OK || (db->flags&SQLITE_RecoveryMode)){
    /* Black magic: If the SQLITE_RecoveryMode flag is set, then consider
    ** the schema loaded, even if errors occurred. In this situation the
    ** current sqlitex_prepare() operation will fail, but the following one
    ** will attempt to compile the supplied statement against whatever subset
    ** of the schema was loaded before the error occurred. The primary
    ** purpose of this is to allow access to the sqlite_master table
    ** even when its contents have been corrupted.
    */
    DbSetProperty(db, iDb, DB_SchemaLoaded);
    rc = SQLITE_OK;
  }

  /* Jump here for an error that occurs after successfully allocating
  ** curMain and calling sqlite3BtreeEnter(). For an error that occurs
  ** before that point, jump to error_out.
  */
initone_error_out:
  if( openedTransaction ){
    sqlite3BtreeCommit(pDb->pBt);
  }
  sqlite3BtreeLeave(pDb->pBt);

error_out:
  if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ){
    db->mallocFailed = 1;
  }
  return rc;
}

/*
**
** Please see int sqlitexInit(sqlitex *db, char **pzErrMsg)
** The additional parameter, zName, allows initializing only
** one table in that database, to support dynamically attached
** tables.  If it is NULL, all the tables are initialized
*/
int sqlitexInitTable(sqlitex *db, char **pzErrMsg, const char *zName)
{
  int i, rc;
  int commit_internal = !(db->flags&SQLITE_InternChanges);
  char *tmp;
  char dbname[32];   /* ok, this needs to ship! */

  assert( sqlitex_mutex_held(db->mutex) );
  assert( sqlite3BtreeHoldsMutex(db->aDb[0].pBt) );
  assert( db->init.busy==0 );
  rc = SQLITE_OK;
  db->init.busy = 1;
  /* COMDB2 MODIFICATION */
  dbname[0] = '\0';
  if( zName ){
    db->init.zTblName = strdup(zName);
    tmp = strchr(db->init.zTblName, '.');
    if( tmp ){
      memcpy(dbname, db->init.zTblName, tmp-db->init.zTblName);
      dbname[tmp-db->init.zTblName+1] = '\0';
      memmove(db->init.zTblName, tmp+1, strlen(tmp));
    }else{
      fprintf(stderr, "%s: confusing name %s\n", __func__, db->init.zTblName);
      dbname[0] = '\0';
    }
  }

  ENC(db) = SCHEMA_ENC(db);
  for(i=0; rc==SQLITE_OK && i<db->nDb; i++){
    if(i==1) continue; /* skip temp tables */
    if(DbHasProperty(db, i, DB_SchemaLoaded) && i==0) continue; /* skip loaded local schemas */
    if(!zName && i>1) continue; /* skip remote that are not doing a table prepare */

    /* remote tables are updated on a table basis; check if the schema for
       this table is actually present */
    if (dbname[0] && (sqlitexFindTableCheckOnly(db, db->init.zTblName, db->aDb[i].zName) != 0)) continue;

    rc = sqlitexInitOne(db, i, pzErrMsg);
    if( rc ){
      sqlitexResetOneSchema(db, i);
    }
  }

  /* Once all the other databases have been initialized, load the schema
  ** for the TEMP database. This is loaded last, as the TEMP database
  ** schema may contain references to objects in other databases.
  */
#ifndef SQLITE_OMIT_TEMPDB
  assert( db->nDb>1 );
  /* COMDB2 MODIFICATION */
  if( !zName && rc==SQLITE_OK && !DbHasProperty(db, 1, DB_SchemaLoaded) ){
    rc = sqlitexInitOne(db, 1, pzErrMsg);
    if( rc ){
      sqlitexResetOneSchema(db, 1);
    }
  }
#endif
  /* COMDB2 MODIFICATION */
  if( zName ){
    free(db->init.zTblName);
    db->init.zTblName = NULL;
  }
  db->init.busy = 0;
  if( rc==SQLITE_OK && commit_internal ){
    sqlitexCommitInternalChanges(db);
  }

  return rc;
}

/*
** Initialize all database files - the main database file, the file
** used to store temporary tables, and any additional database files
** created using ATTACH statements.  Return a success code.  If an
** error occurs, write an error message into *pzErrMsg.
**
** After a database is initialized, the DB_SchemaLoaded bit is set
** bit is set in the flags field of the Db structure. If the database
** file was of zero-length, then the DB_Empty flag is also set.
*/
int sqlitexInit(sqlitex *db, char **pzErrMsg)
{
   int rc;

#ifdef DEBUG_SQLITE_MEMORY
   extern void sqlite_init_start(void);
   sqlite_init_start();
#endif

   rc = sqlitexInitTable(db, pzErrMsg, NULL);

#ifdef DEBUG_SQLITE_MEMORY
   extern void sqlite_init_end(void);
   sqlite_init_end();
#endif
   return rc;
}

/*
** This routine is a no-op if the database schema is already initialized.
** Otherwise, the schema is loaded. An error code is returned.
*/
int sqlitexReadSchema(Parse *pParse){
  int rc = SQLITE_OK;
  sqlitex *db = pParse->db;
  assert( sqlitex_mutex_held(db->mutex) );
  if( !db->init.busy ){
    rc = sqlitexInit(db, &pParse->zErrMsg);
  }
  if( rc!=SQLITE_OK ){
    pParse->rc = rc;
    pParse->nErr++;
  }
  return rc;
}


/*
** Check schema cookies in all databases.  If any cookie is out
** of date set pParse->rc to SQLITE_SCHEMA.  If all schema cookies
** make no changes to pParse->rc.
*/
static void schemaIsValid(Parse *pParse){
  sqlitex *db = pParse->db;
  int iDb;
  int rc;
  int cookie;

  assert( pParse->checkSchema );
  assert( sqlitex_mutex_held(db->mutex) );
  for(iDb=0; iDb<db->nDb; iDb++){
    int openedTransaction = 0;         /* True if a transaction is opened */
    Btree *pBt = db->aDb[iDb].pBt;     /* Btree database to read cookie from */
    if( pBt==0 ) continue;

/** COMDB2 MODIFICATION
    sqlite3BtreeGetMeta is a memory only operation; we do not
    need a transaction for it
    call it w/out a transaction and hopefully prevent messing
    up with the transactions
*/
    sqlite3BtreeGetMeta(pBt, BTREE_SCHEMA_VERSION, (u32 *)&cookie);
    if( cookie!=db->aDb[iDb].pSchema->schema_cookie ){
      pParse->rc = SQLITE_SCHEMA;
    }

#if 0
    /* SEE COMDB2 MODIFICATION ABOVE */
    /* If there is not already a read-only (or read-write) transaction opened
    ** on the b-tree database, open one now. If a transaction is opened, it
    ** will be closed immediately after reading the meta-value. */
    if( !sqlite3BtreeIsInReadTrans(pBt) ){
      rc = sqlite3BtreeBeginTrans(pBt, 0);
      if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ){
        db->mallocFailed = 1;
      }
      if( rc!=SQLITE_OK ) return;
      openedTransaction = 1;
    }

    /* Read the schema cookie from the database. If it does not match the
    ** value stored as part of the in-memory schema representation,
    ** set Parse.rc to SQLITE_SCHEMA. */
    sqlite3BtreeGetMeta(pBt, BTREE_SCHEMA_VERSION, (u32 *)&cookie);
    assert( sqlitexSchemaMutexHeld(db, iDb, 0) );
    if( cookie!=db->aDb[iDb].pSchema->schema_cookie ){
      sqlitexResetOneSchema(db, iDb);
      pParse->rc = SQLITE_SCHEMA;
    }

    /* Close the transaction, if one was opened. */
    if( openedTransaction ){
      sqlite3BtreeCommit(pBt);
    }
#endif
  }
}

/*
** Convert a schema pointer into the iDb index that indicates
** which database file in db->aDb[] the schema refers to.
**
** If the same database is attached more than once, the first
** attached database is returned.
*/
int sqlitexSchemaToIndex(sqlitex *db, Schema *pSchema){
  int i = -1000000;

  /* If pSchema is NULL, then return -1000000. This happens when code in 
  ** expr.c is trying to resolve a reference to a transient table (i.e. one
  ** created by a sub-select). In this case the return value of this 
  ** function should never be used.
  **
  ** We return -1000000 instead of the more usual -1 simply because using
  ** -1000000 as the incorrect index into db->aDb[] is much 
  ** more likely to cause a segfault than -1 (of course there are assert()
  ** statements too, but it never hurts to play the odds).
  */
  assert( sqlitex_mutex_held(db->mutex) );
  if( pSchema ){
    for(i=0; ALWAYS(i<db->nDb); i++){
      if( db->aDb[i].pSchema==pSchema ){
        break;
      }
    }
    assert( i>=0 && i<db->nDb );
  }
  return i;
}

/*
** Free all memory allocations in the pParse object
*/
void sqlitexParserReset(Parse *pParse){
  if( pParse ){
    sqlitex *db = pParse->db;
    sqlitexDbFree(db, pParse->aLabel);
    sqlitexExprListDelete(db, pParse->pConstExpr);
  }
}

/*
** Compile the UTF-8 encoded SQL statement zSql into a statement handle.
*/
static int sqlitexPrepare(
  sqlitex *db,              /* Database handle. */
  const char *zSql,         /* UTF-8 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  int saveSqlFlag,          /* True to copy SQL text into the sqlitex_stmt */
  Vdbe *pReprepare,         /* VM being reprepared */
  sqlitex_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const char **pzTail,      /* OUT: End of parsed string */
  /* COMDB2 MODIFICATION */
  int flags
){
  Parse *pParse;            /* Parsing context */
  char *zErrMsg = 0;        /* Error message */
  int rc = SQLITE_OK;       /* Result code */
  int i;                    /* Loop counter */

  /* Allocate the parsing context */
  pParse = sqlitexStackAllocZero(db, sizeof(*pParse));
  if( pParse==0 ){
    rc = SQLITE_NOMEM;
    goto end_prepare;
  }

  /* COMDB2 MODIFICATION */
  if (flags & SQLITE3_ENABLE_QUERY_PLAN)
    pParse->enableExplainTrace = 1;
  if (flags & SQLITE3_TRACK_IDENTIFIERS) {
    pParse->trackIdentifiers = 1;
    sqlitexHashInit(&pParse->identifiers);
  }

  pParse->pReprepare = pReprepare;
  assert( ppStmt && *ppStmt==0 );
  assert( !db->mallocFailed );
  assert( sqlitex_mutex_held(db->mutex) );

  /* Check to verify that it is possible to get a read lock on all
  ** database schemas.  The inability to get a read lock indicates that
  ** some other database connection is holding a write-lock, which in
  ** turn means that the other connection has made uncommitted changes
  ** to the schema.
  **
  ** Were we to proceed and prepare the statement against the uncommitted
  ** schema changes and if those schema changes are subsequently rolled
  ** back and different changes are made in their place, then when this
  ** prepared statement goes to run the schema cookie would fail to detect
  ** the schema change.  Disaster would follow.
  **
  ** This thread is currently holding mutexes on all Btrees (because
  ** of the sqlite3BtreeEnterAll() in sqlitexLockAndPrepare()) so it
  ** is not possible for another thread to start a new schema change
  ** while this routine is running.  Hence, we do not need to hold 
  ** locks on the schema, we just need to make sure nobody else is 
  ** holding them.
  **
  ** Note that setting READ_UNCOMMITTED overrides most lock detection,
  ** but it does *not* override schema lock detection, so this all still
  ** works even if READ_UNCOMMITTED is set.
  */
  for(i=0; i<db->nDb; i++) {
    Btree *pBt = db->aDb[i].pBt;
    if( pBt ){
      assert( sqlite3BtreeHoldsMutex(pBt) );
      rc = sqlite3BtreeSchemaLocked(pBt);
      if( rc ){
        const char *zDb = db->aDb[i].zName;
        sqlitexErrorWithMsg(db, rc, "database schema is locked: %s", zDb);
        testcase( db->flags & SQLITE_ReadUncommitted );
        goto end_prepare;
      }
    }
  }

  sqlitexVtabUnlockList(db);

  pParse->db = db;
  pParse->nQueryLoop = 0;  /* Logarithmic, so 0 really means 1 */
  if( nBytes>=0 && (nBytes==0 || zSql[nBytes-1]!=0) ){
    char *zSqlCopy;
    int mxLen = db->aLimit[SQLITE_LIMIT_SQL_LENGTH];
    testcase( nBytes==mxLen );
    testcase( nBytes==mxLen+1 );
    if( nBytes>mxLen ){
      sqlitexErrorWithMsg(db, SQLITE_TOOBIG, "statement too long");
      rc = sqlitexApiExit(db, SQLITE_TOOBIG);
      goto end_prepare;
    }
    zSqlCopy = sqlitexDbStrNDup(db, zSql, nBytes);
    if( zSqlCopy ){
      sqlitexRunParser(pParse, zSqlCopy, &zErrMsg);
      sqlitexDbFree(db, zSqlCopy);
      pParse->zTail = &zSql[pParse->zTail-zSqlCopy];
    }else{
      pParse->zTail = &zSql[nBytes];
    }
  }else{
    int ret = sqlitexRunParser(pParse, zSql, &zErrMsg);
  }
  assert( 0==pParse->nQueryLoop );

  if( db->mallocFailed ){
    pParse->rc = SQLITE_NOMEM;
  }
  if( pParse->rc==SQLITE_DONEX ) pParse->rc = SQLITE_OK;
  if( pParse->checkSchema ){
    schemaIsValid(pParse);
  }
  if( db->mallocFailed ){
    pParse->rc = SQLITE_NOMEM;
  }
  if( pzTail ){
    *pzTail = pParse->zTail;
  }
  rc = pParse->rc;

#ifndef SQLITE_OMIT_EXPLAIN
  if( rc==SQLITE_OK && pParse->pVdbe && pParse->explain ){
    static const char * const azColName[] = {
       "addr", "opcode", "p1", "p2", "p3", "p4", "p5", "comment",
       "selectid", "order", "from", "detail"
    };
    int iFirst, mx;
    if( pParse->explain==2 ){
      sqlitexVdbeSetNumCols(pParse->pVdbe, 4);
      iFirst = 8;
      mx = 12;
    }else{
      sqlitexVdbeSetNumCols(pParse->pVdbe, 8);
      iFirst = 0;
      mx = 8;
    }
    for(i=iFirst; i<mx; i++){
      sqlitexVdbeSetColName(pParse->pVdbe, i-iFirst, COLNAME_NAME,
                            azColName[i], SQLITEX_STATIC);
    }
  }
#endif

  if( db->init.busy==0 ){
    Vdbe *pVdbe = pParse->pVdbe;
    sqlitexVdbeSetSql(pVdbe, zSql, (int)(pParse->zTail-zSql), saveSqlFlag);
  }
  if( pParse->pVdbe && (rc!=SQLITE_OK || db->mallocFailed) ){
    sqlitexVdbeFinalize(pParse->pVdbe);
    assert(!(*ppStmt));
  }else{
    *ppStmt = (sqlitex_stmt*)pParse->pVdbe;

    /* COMDB2 MODIFICATION */
    /* set time when the request is prepared, see now() function */
    if( pParse->pVdbe )
      clock_gettime(CLOCK_REALTIME, &pParse->pVdbe->tspec);
  }

  if( zErrMsg ){
    sqlitexErrorWithMsg(db, rc, "%s", zErrMsg);
    sqlitexDbFree(db, zErrMsg);
  }else{
    sqlitexError(db, rc);
  }

  if (pParse->pVdbe && pParse->trackIdentifiers) {
      /* Add tables to list of identifiers. */
      for (int i = 0; i < pParse->pVdbe->numTables; i++) {
          Table *t = pParse->pVdbe->tbls[i];
          if (sqlitexHashFind(&pParse->identifiers, t->zName) == NULL) {
              char *s = strdup(t->zName);
              sqlitexHashInsert(&pParse->identifiers, s, s);
          }
      }
      pParse->pVdbe->trackIdentifiers = pParse->trackIdentifiers;
      pParse->pVdbe->identifiers = pParse->identifiers;
  }

  /* Delete any TriggerPrg structures allocated while parsing this statement. */
  while( pParse->pTriggerPrg ){
    TriggerPrg *pT = pParse->pTriggerPrg;
    pParse->pTriggerPrg = pT->pNext;
    sqlitexDbFree(db, pT);
  }


end_prepare:

  sqlitexParserReset(pParse);
  sqlitexStackFree(db, pParse);
  rc = sqlitexApiExit(db, rc);
  assert( (rc&db->errMask)==rc );
  return rc;
}

static int sqlitexLockAndPrepare(
  sqlitex *db,              /* Database handle. */
  const char *zSql,         /* UTF-8 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  int saveSqlFlag,          /* True to copy SQL text into the sqlitex_stmt */
  Vdbe *pOld,               /* VM being reprepared */
  sqlitex_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const char **pzTail,      /* OUT: End of parsed string */
  /* COMDB2 MODIFICATION */
  int flags
){
  int rc;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( ppStmt==0 ) return SQLITE_MISUSE_BKPT;
#endif
  *ppStmt = 0;
  if( !sqlitexSafetyCheckOk(db)||zSql==0 ){
    return SQLITE_MISUSE_BKPT;
  }
  sqlitex_mutex_enter(db->mutex);
  sqlite3BtreeEnterAll(db);
  /* COMDB2 MODIFICATION */
  rc = sqlitexPrepare(db, zSql, nBytes, saveSqlFlag, pOld, ppStmt, pzTail, flags);
  if( rc==SQLITE_SCHEMA ){
    sqlitex_finalize(*ppStmt);
    /* COMDB2 MODIFICATION */
    rc = sqlitexPrepare(db, zSql, nBytes, saveSqlFlag, pOld, ppStmt, pzTail, flags);
  }
  sqlite3BtreeLeaveAll(db);
  sqlitex_mutex_leave(db->mutex);
  assert( rc==SQLITE_OK || *ppStmt==0 );
  return rc;
}

/*
** Rerun the compilation of a statement after a schema change.
**
** If the statement is successfully recompiled, return SQLITE_OK. Otherwise,
** if the statement cannot be recompiled because another connection has
** locked the sqlitex_master table, return SQLITE_LOCKED. If any other error
** occurs, return SQLITE_SCHEMA.
*/
int sqlitexReprepare(Vdbe *p){
  int rc;
  Vdbe *pNew; /* COMDB2 */
  const char *zSql;
  sqlitex *db;

  assert( sqlitex_mutex_held(sqlitexVdbeDb(p)->mutex) );
  zSql = sqlitex_sql((sqlitex_stmt *)p);
  assert( zSql!=0 );  /* Reprepare only called for prepare_v2() statements */
  db = sqlitexVdbeDb(p);
  assert( sqlitex_mutex_held(db->mutex) );
  /* COMDB2 MODIFICATION */
  rc = sqlitexLockAndPrepare(db, zSql, -1, 0, p, (sqlitex_stmt**) &pNew, 0, 0);
  if( rc ){
    if( rc==SQLITE_NOMEM ){
      db->mallocFailed = 1;
    }
    assert( pNew==0 );
    return rc;
  }else{
    assert( pNew!=0 );
  }

  /* COMDB2 MODIFICATION */
  /* Keep the previous state machine's tzname and misc comdb2 state. */

#if 0
    NOTE: sqlitexVdbeSwap takes care of preserving new updCols info !!!
  memcpy(&pNew->updCols, &p->updCols, sizeof(Vdbe) - offsetof(Vdbe, updCols));
  bzero(&p->updCols, sizeof(Vdbe) - offsetof(Vdbe, updCols));
#endif
  memcpy(pNew->tzname, p->tzname, TZNAME_MAX);
  pNew->dtprec = p->dtprec;

  sqlitexVdbeSwap((Vdbe*)pNew, p);
  sqlitexTransferBindings((sqlitex_stmt*)pNew, (sqlitex_stmt*)p);
  sqlitexVdbeResetStepResult((Vdbe*)pNew);
  sqlitexVdbeFinalize((Vdbe*)pNew);
  return SQLITE_OK;
}

/* COMDB2 MODIFICATION */
char* sqlitex_prepare_plan(sqlitex_stmt *stmt)
{
    if (stmt == NULL)
      return NULL;
    return ((Vdbe*) stmt)->explainTrace;
}

/* COMDB2 MODIFICATION */
int sqlitex_prepare_flags(
  sqlitex *db,
  const char *zSql,
  int nBytes,
  sqlitex_stmt **ppStmt,
  const char** pzTail,
  int flags);

inline int sqlitex_prepare(
  sqlitex *db,              /* Database handle. */
  const char *zSql,         /* UTF-8 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  sqlitex_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const char **pzTail       /* OUT: End of parsed string */
){
  return sqlitex_prepare_flags(db, zSql, nBytes, ppStmt, pzTail, 0);
}

inline int sqlitex_prepare_clone(
  sqlitex *db,              /* Database handle. */
  const char *zSql,         /* UTF-8 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  sqlitex_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const char **pzTail       /* OUT: End of parsed string */
){
  return sqlitex_prepare_flags(db, zSql, nBytes, ppStmt, pzTail, 0);
}


/*
** Two versions of the official API.  Legacy and new use.  In the legacy
** version, the original SQL text is not saved in the prepared statement
** and so if a schema change occurs, SQLITE_SCHEMA is returned by
** sqlitex_step().  In the new version, the original SQL text is retained
** and the statement is automatically recompiled if an schema change
** occurs.
*/
/* COMDB2 MODIFICATION */
int sqlitex_prepare_flags(
  sqlitex *db,              /* Database handle. */
  const char *zSql,         /* UTF-8 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  sqlitex_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const char **pzTail,      /* OUT: End of parsed string */
  /* COMDB2 MODIFICATION */
  int flags
){
  int rc;
  /* COMDB2 MODIFICATION */
  rc = sqlitexLockAndPrepare(db,zSql,nBytes,1,0,ppStmt,pzTail,flags);
  assert( rc==SQLITE_OK || ppStmt==0 || *ppStmt==0 );  /* VERIFY: F13021 */
  return rc;
}

int sqlitex_prepare_v2(
  sqlitex *db,              /* Database handle. */
  const char *zSql,         /* UTF-8 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  sqlitex_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const char **pzTail       /* OUT: End of parsed string */
){
  int rc;
  /* COMDB2 MODIFICATION */
  rc = sqlitexLockAndPrepare(db,zSql,nBytes,1,0,ppStmt,pzTail,0);
  assert( rc==SQLITE_OK || ppStmt==0 || *ppStmt==0 );  /* VERIFY: F13021 */
  return rc;
}

#ifndef SQLITE_OMIT_UTF16
/*
** Compile the UTF-16 encoded SQL statement zSql into a statement handle.
*/
static int sqlitexPrepare16(
  sqlitex *db,              /* Database handle. */
  const void *zSql,         /* UTF-16 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  int saveSqlFlag,          /* True to save SQL text into the sqlitex_stmt */
  sqlitex_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const void **pzTail       /* OUT: End of parsed string */
){
  /* This function currently works by first transforming the UTF-16
  ** encoded string to UTF-8, then invoking sqlitex_prepare(). The
  ** tricky bit is figuring out the pointer to return in *pzTail.
  */
  char *zSql8;
  const char *zTail8 = 0;
  int rc = SQLITE_OK;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( ppStmt==0 ) return SQLITE_MISUSE_BKPT;
#endif
  *ppStmt = 0;
  if( !sqlitexSafetyCheckOk(db)||zSql==0 ){
    return SQLITE_MISUSE_BKPT;
  }
  if( nBytes>=0 ){
    int sz;
    const char *z = (const char*)zSql;
    for(sz=0; sz<nBytes && (z[sz]!=0 || z[sz+1]!=0); sz += 2){}
    nBytes = sz;
  }
  sqlitex_mutex_enter(db->mutex);
  zSql8 = sqlitexUtf16to8(db, zSql, nBytes, SQLITE_UTF16NATIVE);
  if( zSql8 ){
    /* COMDB2 MODIFICATION */
    rc = sqlitexLockAndPrepare(db, zSql8, -1, saveSqlFlag, 0, ppStmt, &zTail8, 0);
  }

  if( zTail8 && pzTail ){
    /* If sqlitex_prepare returns a tail pointer, we calculate the
    ** equivalent pointer into the UTF-16 string by counting the unicode
    ** characters between zSql8 and zTail8, and then returning a pointer
    ** the same number of characters into the UTF-16 string.
    */
    int chars_parsed = sqlitexUtf8CharLen(zSql8, (int)(zTail8-zSql8));
    *pzTail = (u8 *)zSql + sqlitexUtf16ByteLen(zSql, chars_parsed);
  }
  sqlitexDbFree(db, zSql8);
  rc = sqlitexApiExit(db, rc);
  sqlitex_mutex_leave(db->mutex);
  return rc;
}

/*
** Two versions of the official API.  Legacy and new use.  In the legacy
** version, the original SQL text is not saved in the prepared statement
** and so if a schema change occurs, SQLITE_SCHEMA is returned by
** sqlitex_step().  In the new version, the original SQL text is retained
** and the statement is automatically recompiled if an schema change
** occurs.
*/
int sqlitex_prepare16(
  sqlitex *db,              /* Database handle. */
  const void *zSql,         /* UTF-16 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  sqlitex_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const void **pzTail       /* OUT: End of parsed string */
){
  int rc;
  rc = sqlitexPrepare16(db,zSql,nBytes,0,ppStmt,pzTail);
  assert( rc==SQLITE_OK || ppStmt==0 || *ppStmt==0 );  /* VERIFY: F13021 */
  return rc;
}
int sqlitex_prepare16_v2(
  sqlitex *db,              /* Database handle. */
  const void *zSql,         /* UTF-16 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  sqlitex_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const void **pzTail       /* OUT: End of parsed string */
){
  int rc;
  rc = sqlitexPrepare16(db,zSql,nBytes,1,ppStmt,pzTail);
  assert( rc==SQLITE_OK || ppStmt==0 || *ppStmt==0 );  /* VERIFY: F13021 */
  return rc;
}

#endif /* SQLITE_OMIT_UTF16 */
