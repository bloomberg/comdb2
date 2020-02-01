/*
** 2006 June 10
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used to help implement virtual tables.
*/
#ifndef SQLITE_OMIT_VIRTUALTABLE
#include "sqliteInt.h"

/*
** Before a virtual table xCreate() or xConnect() method is invoked, the
** sqlitex.pVtabCtx member variable is set to point to an instance of
** this struct allocated on the stack. It is used by the implementation of 
** the sqlitex_declare_vtab() and sqlitex_vtab_config() APIs, both of which
** are invoked only from within xCreate and xConnect methods.
*/
struct VtabCtx {
  VTable *pVTable;    /* The virtual table being constructed */
  Table *pTab;        /* The Table object to which the virtual table belongs */
};

/*
** The actual function that does the work of creating a new module.
** This function implements the sqlitex_create_module() and
** sqlitex_create_module_v2() interfaces.
*/
static int createModule(
  sqlitex *db,                    /* Database in which module is registered */
  const char *zName,              /* Name assigned to this module */
  const sqlitex_module *pModule,  /* The definition of the module */
  void *pAux,                     /* Context pointer for xCreate/xConnect */
  void (*xDestroy)(void *)        /* Module destructor function */
){
  int rc = SQLITE_OK;
  int nName;

  sqlitex_mutex_enter(db->mutex);
  nName = sqlitexStrlen30(zName);
  if( sqlitexHashFind(&db->aModule, zName) ){
    rc = SQLITE_MISUSE_BKPT;
  }else{
    Module *pMod;
    pMod = (Module *)sqlitexDbMallocRaw(db, sizeof(Module) + nName + 1);
    if( pMod ){
      Module *pDel;
      char *zCopy = (char *)(&pMod[1]);
      memcpy(zCopy, zName, nName+1);
      pMod->zName = zCopy;
      pMod->pModule = pModule;
      pMod->pAux = pAux;
      pMod->xDestroy = xDestroy;
      pMod->pEpoTab = 0;
      pDel = (Module *)sqlitexHashInsert(&db->aModule,zCopy,(void*)pMod);
      assert( pDel==0 || pDel==pMod );
      if( pDel ){
        db->mallocFailed = 1;
        sqlitexDbFree(db, pDel);
      }
    }
  }
  rc = sqlitexApiExit(db, rc);
  if( rc!=SQLITE_OK && xDestroy ) xDestroy(pAux);

  sqlitex_mutex_leave(db->mutex);
  return rc;
}


/*
** External API function used to create a new virtual-table module.
*/
int sqlitex_create_module(
  sqlitex *db,                    /* Database in which module is registered */
  const char *zName,              /* Name assigned to this module */
  const sqlitex_module *pModule,  /* The definition of the module */
  void *pAux                      /* Context pointer for xCreate/xConnect */
){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) || zName==0 ) return SQLITE_MISUSE_BKPT;
#endif
  return createModule(db, zName, pModule, pAux, 0);
}

/*
** External API function used to create a new virtual-table module.
*/
int sqlitex_create_module_v2(
  sqlitex *db,                    /* Database in which module is registered */
  const char *zName,              /* Name assigned to this module */
  const sqlitex_module *pModule,  /* The definition of the module */
  void *pAux,                     /* Context pointer for xCreate/xConnect */
  void (*xDestroy)(void *)        /* Module destructor function */
){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) || zName==0 ) return SQLITE_MISUSE_BKPT;
#endif
  return createModule(db, zName, pModule, pAux, xDestroy);
}

/*
** Lock the virtual table so that it cannot be disconnected.
** Locks nest.  Every lock should have a corresponding unlock.
** If an unlock is omitted, resources leaks will occur.  
**
** If a disconnect is attempted while a virtual table is locked,
** the disconnect is deferred until all locks have been removed.
*/
void sqlitexVtabLock(VTable *pVTab){
  pVTab->nRef++;
}


/*
** pTab is a pointer to a Table structure representing a virtual-table.
** Return a pointer to the VTable object used by connection db to access 
** this virtual-table, if one has been created, or NULL otherwise.
*/
VTable *sqlitexGetVTable(sqlitex *db, Table *pTab){
  VTable *pVtab;
  assert( IsVirtual(pTab) );
  for(pVtab=pTab->pVTable; pVtab && pVtab->db!=db; pVtab=pVtab->pNext);
  return pVtab;
}

/*
** Decrement the ref-count on a virtual table object. When the ref-count
** reaches zero, call the xDisconnect() method to delete the object.
*/
void sqlitexVtabUnlock(VTable *pVTab){
  sqlitex *db = pVTab->db;

  assert( db );
  assert( pVTab->nRef>0 );
  assert( db->magic==SQLITE_MAGIC_OPEN || db->magic==SQLITE_MAGIC_ZOMBIE );

  pVTab->nRef--;
  if( pVTab->nRef==0 ){
    sqlitex_vtab *p = pVTab->pVtab;
    if( p ){
      p->pModule->xDisconnect(p);
    }
    sqlitexDbFree(db, pVTab);
  }
}

/*
** Table p is a virtual table. This function moves all elements in the
** p->pVTable list to the sqlitex.pDisconnect lists of their associated
** database connections to be disconnected at the next opportunity. 
** Except, if argument db is not NULL, then the entry associated with
** connection db is left in the p->pVTable list.
*/
static VTable *vtabDisconnectAll(sqlitex *db, Table *p){
  VTable *pRet = 0;
  VTable *pVTable = p->pVTable;
  p->pVTable = 0;

  /* Assert that the mutex (if any) associated with the BtShared database 
  ** that contains table p is held by the caller. See header comments 
  ** above function sqlitexVtabUnlockList() for an explanation of why
  ** this makes it safe to access the sqlitex.pDisconnect list of any
  ** database connection that may have an entry in the p->pVTable list.
  */
  assert( db==0 || sqlitexSchemaMutexHeld(db, 0, p->pSchema) );

  while( pVTable ){
    sqlitex *db2 = pVTable->db;
    VTable *pNext = pVTable->pNext;
    assert( db2 );
    if( db2==db ){
      pRet = pVTable;
      p->pVTable = pRet;
      pRet->pNext = 0;
    }else{
      pVTable->pNext = db2->pDisconnect;
      db2->pDisconnect = pVTable;
    }
    pVTable = pNext;
  }

  assert( !db || pRet );
  return pRet;
}

/*
** Table *p is a virtual table. This function removes the VTable object
** for table *p associated with database connection db from the linked
** list in p->pVTab. It also decrements the VTable ref count. This is
** used when closing database connection db to free all of its VTable
** objects without disturbing the rest of the Schema object (which may
** be being used by other shared-cache connections).
*/
void sqlitexVtabDisconnect(sqlitex *db, Table *p){
  VTable **ppVTab;

  assert( IsVirtual(p) );
  assert( sqlite3BtreeHoldsAllMutexes(db) );
  assert( sqlitex_mutex_held(db->mutex) );

  for(ppVTab=&p->pVTable; *ppVTab; ppVTab=&(*ppVTab)->pNext){
    if( (*ppVTab)->db==db  ){
      VTable *pVTab = *ppVTab;
      *ppVTab = pVTab->pNext;
      sqlitexVtabUnlock(pVTab);
      break;
    }
  }
}


/*
** Disconnect all the virtual table objects in the sqlitex.pDisconnect list.
**
** This function may only be called when the mutexes associated with all
** shared b-tree databases opened using connection db are held by the 
** caller. This is done to protect the sqlitex.pDisconnect list. The
** sqlitex.pDisconnect list is accessed only as follows:
**
**   1) By this function. In this case, all BtShared mutexes and the mutex
**      associated with the database handle itself must be held.
**
**   2) By function vtabDisconnectAll(), when it adds a VTable entry to
**      the sqlitex.pDisconnect list. In this case either the BtShared mutex
**      associated with the database the virtual table is stored in is held
**      or, if the virtual table is stored in a non-sharable database, then
**      the database handle mutex is held.
**
** As a result, a sqlitex.pDisconnect cannot be accessed simultaneously 
** by multiple threads. It is thread-safe.
*/
void sqlitexVtabUnlockList(sqlitex *db){
  VTable *p = db->pDisconnect;
  db->pDisconnect = 0;

  assert( sqlite3BtreeHoldsAllMutexes(db) );
  assert( sqlitex_mutex_held(db->mutex) );

  if( p ){
    sqlitexExpirePreparedStatements(db);
    do {
      VTable *pNext = p->pNext;
      sqlitexVtabUnlock(p);
      p = pNext;
    }while( p );
  }
}

/*
** Clear any and all virtual-table information from the Table record.
** This routine is called, for example, just before deleting the Table
** record.
**
** Since it is a virtual-table, the Table structure contains a pointer
** to the head of a linked list of VTable structures. Each VTable 
** structure is associated with a single sqlitex* user of the schema.
** The reference count of the VTable structure associated with database 
** connection db is decremented immediately (which may lead to the 
** structure being xDisconnected and free). Any other VTable structures
** in the list are moved to the sqlitex.pDisconnect list of the associated 
** database connection.
*/
void sqlitexVtabClear(sqlitex *db, Table *p){
  if( !db || db->pnBytesFreed==0 ) vtabDisconnectAll(0, p);
  if( p->azModuleArg ){
    int i;
    for(i=0; i<p->nModuleArg; i++){
      if( i!=1 ) sqlitexDbFree(db, p->azModuleArg[i]);
    }
    sqlitexDbFree(db, p->azModuleArg);
  }
}

/*
** Add a new module argument to pTable->azModuleArg[].
** The string is not copied - the pointer is stored.  The
** string will be freed automatically when the table is
** deleted.
*/
static void addModuleArgument(sqlitex *db, Table *pTable, char *zArg){
  int i = pTable->nModuleArg++;
  int nBytes = sizeof(char *)*(1+pTable->nModuleArg);
  char **azModuleArg;
  azModuleArg = sqlitexDbRealloc(db, pTable->azModuleArg, nBytes);
  if( azModuleArg==0 ){
    int j;
    for(j=0; j<i; j++){
      sqlitexDbFree(db, pTable->azModuleArg[j]);
    }
    sqlitexDbFree(db, zArg);
    sqlitexDbFree(db, pTable->azModuleArg);
    pTable->nModuleArg = 0;
  }else{
    azModuleArg[i] = zArg;
    azModuleArg[i+1] = 0;
  }
  pTable->azModuleArg = azModuleArg;
}

/*
** The parser calls this routine when it first sees a CREATE VIRTUAL TABLE
** statement.  The module name has been parsed, but the optional list
** of parameters that follow the module name are still pending.
*/
void sqlitexVtabBeginParse(
  Parse *pParse,        /* Parsing context */
  Token *pName1,        /* Name of new table, or database name */
  Token *pName2,        /* Name of new table or NULL */
  Token *pModuleName,   /* Name of the module for the virtual table */
  int ifNotExists       /* No error if the table already exists */
){
  int iDb;              /* The database the table is being created in */
  Table *pTable;        /* The new virtual table */
  sqlitex *db;          /* Database connection */

  sqlitexStartTable(pParse, pName1, pName2, 0, 0, 1, ifNotExists);
  pTable = pParse->pNewTable;
  if( pTable==0 ) return;
  assert( 0==pTable->pIndex );

  db = pParse->db;
  iDb = sqlitexSchemaToIndex(db, pTable->pSchema);
  assert( iDb>=0 );

  pTable->tabFlags |= TF_Virtual;
  pTable->nModuleArg = 0;
  addModuleArgument(db, pTable, sqlitexNameFromToken(db, pModuleName));
  addModuleArgument(db, pTable, 0);
  addModuleArgument(db, pTable, sqlitexDbStrDup(db, pTable->zName));
  assert( (pParse->sNameToken.z==pName2->z && pName2->z!=0)
       || (pParse->sNameToken.z==pName1->z && pName2->z==0)
  );
  pParse->sNameToken.n = (int)(
      &pModuleName->z[pModuleName->n] - pParse->sNameToken.z
  );

#ifndef SQLITE_OMIT_AUTHORIZATION
  /* Creating a virtual table invokes the authorization callback twice.
  ** The first invocation, to obtain permission to INSERT a row into the
  ** sqlite_master table, has already been made by sqlitexStartTable().
  ** The second call, to obtain permission to create the table, is made now.
  */
  if( pTable->azModuleArg ){
    sqlitexAuthCheck(pParse, SQLITE_CREATE_VTABLE, pTable->zName, 
            pTable->azModuleArg[0], pParse->db->aDb[iDb].zName);
  }
#endif
}

/*
** This routine takes the module argument that has been accumulating
** in pParse->zArg[] and appends it to the list of arguments on the
** virtual table currently under construction in pParse->pTable.
*/
static void addArgumentToVtab(Parse *pParse){
  if( pParse->sArg.z && pParse->pNewTable ){
    const char *z = (const char*)pParse->sArg.z;
    int n = pParse->sArg.n;
    sqlitex *db = pParse->db;
    addModuleArgument(db, pParse->pNewTable, sqlitexDbStrNDup(db, z, n));
  }
}

/*
** The parser calls this routine after the CREATE VIRTUAL TABLE statement
** has been completely parsed.
*/
void sqlitexVtabFinishParse(Parse *pParse, Token *pEnd){
  Table *pTab = pParse->pNewTable;  /* The table being constructed */
  sqlitex *db = pParse->db;         /* The database connection */

  if( pTab==0 ) return;
  addArgumentToVtab(pParse);
  pParse->sArg.z = 0;
  if( pTab->nModuleArg<1 ) return;
  
  /* If the CREATE VIRTUAL TABLE statement is being entered for the
  ** first time (in other words if the virtual table is actually being
  ** created now instead of just being read out of sqlite_master) then
  ** do additional initialization work and store the statement text
  ** in the sqlite_master table.
  */
  if( !db->init.busy ){
    char *zStmt;
    char *zWhere;
    int iDb;
    int iReg;
    Vdbe *v;

    /* Compute the complete text of the CREATE VIRTUAL TABLE statement */
    if( pEnd ){
      pParse->sNameToken.n = (int)(pEnd->z - pParse->sNameToken.z) + pEnd->n;
    }
    zStmt = sqlitexMPrintf(db, "CREATE VIRTUAL TABLE %T", &pParse->sNameToken);

    /* A slot for the record has already been allocated in the 
    ** SQLITE_MASTER table.  We just need to update that slot with all
    ** the information we've collected.  
    **
    ** The VM register number pParse->regRowid holds the rowid of an
    ** entry in the sqlite_master table tht was created for this vtab
    ** by sqlitexStartTable().
    */
    iDb = sqlitexSchemaToIndex(db, pTab->pSchema);
    sqlitexNestedParse(pParse,
      "UPDATE %Q.%s "
         "SET type='table', name=%Q, tbl_name=%Q, rootpage=0, sql=%Q "
       "WHERE rowid=#%d",
      db->aDb[iDb].zName, SCHEMA_TABLE(iDb),
      pTab->zName,
      pTab->zName,
      zStmt,
      pParse->regRowid
    );
    sqlitexDbFree(db, zStmt);
    v = sqlitexGetVdbe(pParse);
    sqlitexChangeCookie(pParse, iDb);

    sqlitexVdbeAddOp2(v, OP_Expire, 0, 0);
    zWhere = sqlitexMPrintf(db, "name='%q' AND type='table'", pTab->zName);
    sqlitexVdbeAddParseSchemaOp(v, iDb, zWhere);

    iReg = ++pParse->nMem;
    sqlitexVdbeAddOp4(v, OP_String8, 0, iReg, 0, pTab->zName, 0);
    sqlitexVdbeAddOp2(v, OP_VCreate, iDb, iReg);
  }

  /* If we are rereading the sqlite_master table create the in-memory
  ** record of the table. The xConnect() method is not called until
  ** the first time the virtual table is used in an SQL statement. This
  ** allows a schema that contains virtual tables to be loaded before
  ** the required virtual table implementations are registered.  */
  else {
    Table *pOld;
    Schema *pSchema = pTab->pSchema;
    const char *zName = pTab->zName;
    assert( sqlitexSchemaMutexHeld(db, 0, pSchema) );
    pOld = sqlitexHashInsert(&pSchema->tblHash, zName, pTab);
    if( pOld ){
      db->mallocFailed = 1;
      assert( pTab==pOld );  /* Malloc must have failed inside HashInsert() */
      return;
    }
    pParse->pNewTable = 0;
  }
}

/*
** The parser calls this routine when it sees the first token
** of an argument to the module name in a CREATE VIRTUAL TABLE statement.
*/
void sqlitexVtabArgInit(Parse *pParse){
  addArgumentToVtab(pParse);
  pParse->sArg.z = 0;
  pParse->sArg.n = 0;
}

/*
** The parser calls this routine for each token after the first token
** in an argument to the module name in a CREATE VIRTUAL TABLE statement.
*/
void sqlitexVtabArgExtend(Parse *pParse, Token *p){
  Token *pArg = &pParse->sArg;
  if( pArg->z==0 ){
    pArg->z = p->z;
    pArg->n = p->n;
  }else{
    assert(pArg->z < p->z);
    pArg->n = (int)(&p->z[p->n] - pArg->z);
  }
}

/*
** Invoke a virtual table constructor (either xCreate or xConnect). The
** pointer to the function to invoke is passed as the fourth parameter
** to this procedure.
*/
static int vtabCallConstructor(
  sqlitex *db, 
  Table *pTab,
  Module *pMod,
  int (*xConstruct)(sqlitex*,void*,int,const char*const*,sqlitex_vtab**,char**),
  char **pzErr
){
  VtabCtx sCtx, *pPriorCtx;
  VTable *pVTable;
  int rc;
  const char *const*azArg = (const char *const*)pTab->azModuleArg;
  int nArg = pTab->nModuleArg;
  char *zErr = 0;
  char *zModuleName = sqlitexMPrintf(db, "%s", pTab->zName);
  int iDb;

  if( !zModuleName ){
    return SQLITE_NOMEM;
  }

  pVTable = sqlitexDbMallocZero(db, sizeof(VTable));
  if( !pVTable ){
    sqlitexDbFree(db, zModuleName);
    return SQLITE_NOMEM;
  }
  pVTable->db = db;
  pVTable->pMod = pMod;

  iDb = sqlitexSchemaToIndex(db, pTab->pSchema);
  pTab->azModuleArg[1] = db->aDb[iDb].zName;

  /* Invoke the virtual table constructor */
  assert( &db->pVtabCtx );
  assert( xConstruct );
  sCtx.pTab = pTab;
  sCtx.pVTable = pVTable;
  pPriorCtx = db->pVtabCtx;
  db->pVtabCtx = &sCtx;
  rc = xConstruct(db, pMod->pAux, nArg, azArg, &pVTable->pVtab, &zErr);
  db->pVtabCtx = pPriorCtx;
  if( rc==SQLITE_NOMEM ) db->mallocFailed = 1;

  if( SQLITE_OK!=rc ){
    if( zErr==0 ){
      *pzErr = sqlitexMPrintf(db, "vtable constructor failed: %s", zModuleName);
    }else {
      *pzErr = sqlitexMPrintf(db, "%s", zErr);
      sqlitex_free(zErr);
    }
    sqlitexDbFree(db, pVTable);
  }else if( ALWAYS(pVTable->pVtab) ){
    /* Justification of ALWAYS():  A correct vtab constructor must allocate
    ** the sqlitex_vtab object if successful.  */
    memset(pVTable->pVtab, 0, sizeof(pVTable->pVtab[0]));
    pVTable->pVtab->pModule = pMod->pModule;
    pVTable->nRef = 1;
    if( sCtx.pTab ){
      const char *zFormat = "vtable constructor did not declare schema: %s";
      *pzErr = sqlitexMPrintf(db, zFormat, pTab->zName);
      sqlitexVtabUnlock(pVTable);
      rc = SQLITE_ERROR;
    }else{
      int iCol;
      /* If everything went according to plan, link the new VTable structure
      ** into the linked list headed by pTab->pVTable. Then loop through the 
      ** columns of the table to see if any of them contain the token "hidden".
      ** If so, set the Column COLFLAG_HIDDEN flag and remove the token from
      ** the type string.  */
      pVTable->pNext = pTab->pVTable;
      pTab->pVTable = pVTable;

      for(iCol=0; iCol<pTab->nCol; iCol++){
        char *zType = pTab->aCol[iCol].zType;
        int nType;
        int i = 0;
        if( !zType ) continue;
        nType = sqlitexStrlen30(zType);
        if( sqlitexStrNICmp("hidden", zType, 6)||(zType[6] && zType[6]!=' ') ){
          for(i=0; i<nType; i++){
            if( (0==sqlitexStrNICmp(" hidden", &zType[i], 7))
             && (zType[i+7]=='\0' || zType[i+7]==' ')
            ){
              i++;
              break;
            }
          }
        }
        if( i<nType ){
          int j;
          int nDel = 6 + (zType[i+6] ? 1 : 0);
          for(j=i; (j+nDel)<=nType; j++){
            zType[j] = zType[j+nDel];
          }
          if( zType[i]=='\0' && i>0 ){
            assert(zType[i-1]==' ');
            zType[i-1] = '\0';
          }
          pTab->aCol[iCol].colFlags |= COLFLAG_HIDDEN;
        }
      }
    }
  }

  sqlitexDbFree(db, zModuleName);
  return rc;
}

/*
** This function is invoked by the parser to call the xConnect() method
** of the virtual table pTab. If an error occurs, an error code is returned 
** and an error left in pParse.
**
** This call is a no-op if table pTab is not a virtual table.
*/
int sqlitexVtabCallConnect(Parse *pParse, Table *pTab){
  sqlitex *db = pParse->db;
  const char *zMod;
  Module *pMod;
  int rc;

  assert( pTab );
  if( (pTab->tabFlags & TF_Virtual)==0 || sqlitexGetVTable(db, pTab) ){
    return SQLITE_OK;
  }

  /* Locate the required virtual table module */
  zMod = pTab->azModuleArg[0];
  pMod = (Module*)sqlitexHashFind(&db->aModule, zMod);

  if( !pMod ){
    const char *zModule = pTab->azModuleArg[0];
    sqlitexErrorMsg(pParse, "no such module: %s", zModule);
    rc = SQLITE_ERROR;
  }else{
    char *zErr = 0;
    rc = vtabCallConstructor(db, pTab, pMod, pMod->pModule->xConnect, &zErr);
    if( rc!=SQLITE_OK ){
      sqlitexErrorMsg(pParse, "%s", zErr);
    }
    sqlitexDbFree(db, zErr);
  }

  return rc;
}
/*
** Grow the db->aVTrans[] array so that there is room for at least one
** more v-table. Return SQLITE_NOMEM if a malloc fails, or SQLITE_OK otherwise.
*/
static int growVTrans(sqlitex *db){
  const int ARRAY_INCR = 5;

  /* Grow the sqlitex.aVTrans array if required */
  if( (db->nVTrans%ARRAY_INCR)==0 ){
    VTable **aVTrans;
    int nBytes = sizeof(sqlitex_vtab *) * (db->nVTrans + ARRAY_INCR);
    aVTrans = sqlitexDbRealloc(db, (void *)db->aVTrans, nBytes);
    if( !aVTrans ){
      return SQLITE_NOMEM;
    }
    memset(&aVTrans[db->nVTrans], 0, sizeof(sqlitex_vtab *)*ARRAY_INCR);
    db->aVTrans = aVTrans;
  }

  return SQLITE_OK;
}

/*
** Add the virtual table pVTab to the array sqlitex.aVTrans[]. Space should
** have already been reserved using growVTrans().
*/
static void addToVTrans(sqlitex *db, VTable *pVTab){
  /* Add pVtab to the end of sqlitex.aVTrans */
  db->aVTrans[db->nVTrans++] = pVTab;
  sqlitexVtabLock(pVTab);
}

/*
** This function is invoked by the vdbe to call the xCreate method
** of the virtual table named zTab in database iDb. 
**
** If an error occurs, *pzErr is set to point an an English language
** description of the error and an SQLITE_XXX error code is returned.
** In this case the caller must call sqlitexDbFree(db, ) on *pzErr.
*/
int sqlitexVtabCallCreate(sqlitex *db, int iDb, const char *zTab, char **pzErr){
  int rc = SQLITE_OK;
  Table *pTab;
  Module *pMod;
  const char *zMod;

  pTab = sqlitexFindTable(db, zTab, db->aDb[iDb].zName);
  assert( pTab && (pTab->tabFlags & TF_Virtual)!=0 && !pTab->pVTable );

  /* Locate the required virtual table module */
  zMod = pTab->azModuleArg[0];
  pMod = (Module*)sqlitexHashFind(&db->aModule, zMod);

  /* If the module has been registered and includes a Create method, 
  ** invoke it now. If the module has not been registered, return an 
  ** error. Otherwise, do nothing.
  */
  if( pMod==0 || pMod->pModule->xCreate==0 || pMod->pModule->xDestroy==0 ){
    *pzErr = sqlitexMPrintf(db, "no such module: %s", zMod);
    rc = SQLITE_ERROR;
  }else{
    rc = vtabCallConstructor(db, pTab, pMod, pMod->pModule->xCreate, pzErr);
  }

  /* Justification of ALWAYS():  The xConstructor method is required to
  ** create a valid sqlitex_vtab if it returns SQLITE_OK. */
  if( rc==SQLITE_OK && ALWAYS(sqlitexGetVTable(db, pTab)) ){
    rc = growVTrans(db);
    if( rc==SQLITE_OK ){
      addToVTrans(db, sqlitexGetVTable(db, pTab));
    }
  }

  return rc;
}

/*
** This function is used to set the schema of a virtual table.  It is only
** valid to call this function from within the xCreate() or xConnect() of a
** virtual table module.
*/
int sqlitex_declare_vtab(sqlitex *db, const char *zCreateTable){
  Parse *pParse;

  int rc = SQLITE_OK;
  Table *pTab;
  char *zErr = 0;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) || zCreateTable==0 ){
    return SQLITE_MISUSE_BKPT;
  }
#endif
  sqlitex_mutex_enter(db->mutex);
  if( !db->pVtabCtx || !(pTab = db->pVtabCtx->pTab) ){
    sqlitexError(db, SQLITE_MISUSE);
    sqlitex_mutex_leave(db->mutex);
    return SQLITE_MISUSE_BKPT;
  }
  assert( (pTab->tabFlags & TF_Virtual)!=0 );

  pParse = sqlitexStackAllocZero(db, sizeof(*pParse));
  if( pParse==0 ){
    rc = SQLITE_NOMEM;
  }else{
    pParse->declareVtab = 1;
    pParse->db = db;
    pParse->nQueryLoop = 1;
  
    if( SQLITE_OK==sqlitexRunParser(pParse, zCreateTable, &zErr) 
     && pParse->pNewTable
     && !db->mallocFailed
     && !pParse->pNewTable->pSelect
     && (pParse->pNewTable->tabFlags & TF_Virtual)==0
    ){
      if( !pTab->aCol ){
        pTab->aCol = pParse->pNewTable->aCol;
        pTab->nCol = pParse->pNewTable->nCol;
        pParse->pNewTable->nCol = 0;
        pParse->pNewTable->aCol = 0;
      }
      db->pVtabCtx->pTab = 0;
    }else{
      sqlitexErrorWithMsg(db, SQLITE_ERROR, (zErr ? "%s" : 0), zErr);
      sqlitexDbFree(db, zErr);
      rc = SQLITE_ERROR;
    }
    pParse->declareVtab = 0;
  
    if( pParse->pVdbe ){
      sqlitexVdbeFinalize(pParse->pVdbe);
    }
    sqlitexDeleteTable(db, pParse->pNewTable);
    sqlitexParserReset(pParse);
    sqlitexStackFree(db, pParse);
  }

  assert( (rc&0xff)==rc );
  rc = sqlitexApiExit(db, rc);
  sqlitex_mutex_leave(db->mutex);
  return rc;
}

/*
** This function is invoked by the vdbe to call the xDestroy method
** of the virtual table named zTab in database iDb. This occurs
** when a DROP TABLE is mentioned.
**
** This call is a no-op if zTab is not a virtual table.
*/
int sqlitexVtabCallDestroy(sqlitex *db, int iDb, const char *zTab){
  int rc = SQLITE_OK;
  Table *pTab;

  pTab = sqlitexFindTable(db, zTab, db->aDb[iDb].zName);
  if( ALWAYS(pTab!=0 && pTab->pVTable!=0) ){
    VTable *p = vtabDisconnectAll(db, pTab);
    int (*xDestroy)(sqlitex_vtab *);

    assert( rc==SQLITE_OK );
    xDestroy = p->pMod->pModule->xDestroy;
    assert( xDestroy!=0 );  /* Checked before the virtual table is created */
    rc = xDestroy(p->pVtab);
    /* Remove the sqlitex_vtab* from the aVTrans[] array, if applicable */
    if( rc==SQLITE_OK ){
      assert( pTab->pVTable==p && p->pNext==0 );
      p->pVtab = 0;
      pTab->pVTable = 0;
      sqlitexVtabUnlock(p);
    }
  }

  return rc;
}

/*
** This function invokes either the xRollback or xCommit method
** of each of the virtual tables in the sqlitex.aVTrans array. The method
** called is identified by the second argument, "offset", which is
** the offset of the method to call in the sqlitex_module structure.
**
** The array is cleared after invoking the callbacks. 
*/
static void callFinaliser(sqlitex *db, int offset){
  int i;
  if( db->aVTrans ){
    for(i=0; i<db->nVTrans; i++){
      VTable *pVTab = db->aVTrans[i];
      sqlitex_vtab *p = pVTab->pVtab;
      if( p ){
        int (*x)(sqlitex_vtab *);
        x = *(int (**)(sqlitex_vtab *))((char *)p->pModule + offset);
        if( x ) x(p);
      }
      pVTab->iSavepoint = 0;
      sqlitexVtabUnlock(pVTab);
    }
    sqlitexDbFree(db, db->aVTrans);
    db->nVTrans = 0;
    db->aVTrans = 0;
  }
}

/*
** Invoke the xSync method of all virtual tables in the sqlitex.aVTrans
** array. Return the error code for the first error that occurs, or
** SQLITE_OK if all xSync operations are successful.
**
** If an error message is available, leave it in p->zErrMsg.
*/
int sqlitexVtabSync(sqlitex *db, Vdbe *p){
  int i;
  int rc = SQLITE_OK;
  VTable **aVTrans = db->aVTrans;

  db->aVTrans = 0;
  for(i=0; rc==SQLITE_OK && i<db->nVTrans; i++){
    int (*x)(sqlitex_vtab *);
    sqlitex_vtab *pVtab = aVTrans[i]->pVtab;
    if( pVtab && (x = pVtab->pModule->xSync)!=0 ){
      rc = x(pVtab);
      sqlitexVtabImportErrmsg(p, pVtab);
    }
  }
  db->aVTrans = aVTrans;
  return rc;
}

/*
** Invoke the xRollback method of all virtual tables in the 
** sqlitex.aVTrans array. Then clear the array itself.
*/
int sqlitexVtabRollback(sqlitex *db){
  callFinaliser(db, offsetof(sqlitex_module,xRollback));
  return SQLITE_OK;
}

/*
** Invoke the xCommit method of all virtual tables in the 
** sqlitex.aVTrans array. Then clear the array itself.
*/
int sqlitexVtabCommit(sqlitex *db){
  callFinaliser(db, offsetof(sqlitex_module,xCommit));
  return SQLITE_OK;
}

/*
** If the virtual table pVtab supports the transaction interface
** (xBegin/xRollback/xCommit and optionally xSync) and a transaction is
** not currently open, invoke the xBegin method now.
**
** If the xBegin call is successful, place the sqlitex_vtab pointer
** in the sqlitex.aVTrans array.
*/
int sqlitexVtabBegin(sqlitex *db, VTable *pVTab){
  int rc = SQLITE_OK;
  const sqlitex_module *pModule;

  /* Special case: If db->aVTrans is NULL and db->nVTrans is greater
  ** than zero, then this function is being called from within a
  ** virtual module xSync() callback. It is illegal to write to 
  ** virtual module tables in this case, so return SQLITE_LOCKED.
  */
  if( sqlitexVtabInSync(db) ){
    return SQLITE_LOCKED;
  }
  if( !pVTab ){
    return SQLITE_OK;
  } 
  pModule = pVTab->pVtab->pModule;

  if( pModule->xBegin ){
    int i;

    /* If pVtab is already in the aVTrans array, return early */
    for(i=0; i<db->nVTrans; i++){
      if( db->aVTrans[i]==pVTab ){
        return SQLITE_OK;
      }
    }

    /* Invoke the xBegin method. If successful, add the vtab to the 
    ** sqlitex.aVTrans[] array. */
    rc = growVTrans(db);
    if( rc==SQLITE_OK ){
      rc = pModule->xBegin(pVTab->pVtab);
      if( rc==SQLITE_OK ){
        addToVTrans(db, pVTab);
      }
    }
  }
  return rc;
}

/*
** Invoke either the xSavepoint, xRollbackTo or xRelease method of all
** virtual tables that currently have an open transaction. Pass iSavepoint
** as the second argument to the virtual table method invoked.
**
** If op is SAVEPOINT_BEGIN, the xSavepoint method is invoked. If it is
** SAVEPOINT_ROLLBACK, the xRollbackTo method. Otherwise, if op is 
** SAVEPOINT_RELEASE, then the xRelease method of each virtual table with
** an open transaction is invoked.
**
** If any virtual table method returns an error code other than SQLITE_OK, 
** processing is abandoned and the error returned to the caller of this
** function immediately. If all calls to virtual table methods are successful,
** SQLITE_OK is returned.
*/
int sqlitexVtabSavepoint(sqlitex *db, int op, int iSavepoint){
  int rc = SQLITE_OK;

  assert( op==SAVEPOINT_RELEASE||op==SAVEPOINT_ROLLBACK||op==SAVEPOINT_BEGIN );
  assert( iSavepoint>=0 );
  if( db->aVTrans ){
    int i;
    for(i=0; rc==SQLITE_OK && i<db->nVTrans; i++){
      VTable *pVTab = db->aVTrans[i];
      const sqlitex_module *pMod = pVTab->pMod->pModule;
      if( pVTab->pVtab && pMod->iVersion>=2 ){
        int (*xMethod)(sqlitex_vtab *, int);
        switch( op ){
          case SAVEPOINT_BEGIN:
            xMethod = pMod->xSavepoint;
            pVTab->iSavepoint = iSavepoint+1;
            break;
          case SAVEPOINT_ROLLBACK:
            xMethod = pMod->xRollbackTo;
            break;
          default:
            xMethod = pMod->xRelease;
            break;
        }
        if( xMethod && pVTab->iSavepoint>iSavepoint ){
          rc = xMethod(pVTab->pVtab, iSavepoint);
        }
      }
    }
  }
  return rc;
}

/*
** The first parameter (pDef) is a function implementation.  The
** second parameter (pExpr) is the first argument to this function.
** If pExpr is a column in a virtual table, then let the virtual
** table implementation have an opportunity to overload the function.
**
** This routine is used to allow virtual table implementations to
** overload MATCH, LIKE, GLOB, and REGEXP operators.
**
** Return either the pDef argument (indicating no change) or a 
** new FuncDef structure that is marked as ephemeral using the
** SQLITE_FUNC_EPHEM flag.
*/
FuncDef *sqlitexVtabOverloadFunction(
  sqlitex *db,    /* Database connection for reporting malloc problems */
  FuncDef *pDef,  /* Function to possibly overload */
  int nArg,       /* Number of arguments to the function */
  Expr *pExpr     /* First argument to the function */
){
  Table *pTab;
  sqlitex_vtab *pVtab;
  sqlitex_module *pMod;
  void (*xFunc)(sqlitex_context*,int,sqlitex_value**) = 0;
  void *pArg = 0;
  FuncDef *pNew;
  int rc = 0;
  char *zLowerName;
  unsigned char *z;


  /* Check to see the left operand is a column in a virtual table */
  if( NEVER(pExpr==0) ) return pDef;
  if( pExpr->op!=TK_COLUMN ) return pDef;
  pTab = pExpr->pTab;
  if( NEVER(pTab==0) ) return pDef;
  if( (pTab->tabFlags & TF_Virtual)==0 ) return pDef;
  pVtab = sqlitexGetVTable(db, pTab)->pVtab;
  assert( pVtab!=0 );
  assert( pVtab->pModule!=0 );
  pMod = (sqlitex_module *)pVtab->pModule;
  if( pMod->xFindFunction==0 ) return pDef;
 
  /* Call the xFindFunction method on the virtual table implementation
  ** to see if the implementation wants to overload this function 
  */
  zLowerName = sqlitexDbStrDup(db, pDef->zName);
  if( zLowerName ){
    for(z=(unsigned char*)zLowerName; *z; z++){
      *z = sqlitexUpperToLower[*z];
    }
    rc = pMod->xFindFunction(pVtab, nArg, zLowerName, &xFunc, &pArg);
    sqlitexDbFree(db, zLowerName);
  }
  if( rc==0 ){
    return pDef;
  }

  /* Create a new ephemeral function definition for the overloaded
  ** function */
  pNew = sqlitexDbMallocZero(db, sizeof(*pNew)
                             + sqlitexStrlen30(pDef->zName) + 1);
  if( pNew==0 ){
    return pDef;
  }
  *pNew = *pDef;
  pNew->zName = (char *)&pNew[1];
  memcpy(pNew->zName, pDef->zName, sqlitexStrlen30(pDef->zName)+1);
  pNew->xFunc = xFunc;
  pNew->pUserData = pArg;
  pNew->funcFlags |= SQLITE_FUNC_EPHEM;
  return pNew;
}

/*
** Make sure virtual table pTab is contained in the pParse->apVirtualLock[]
** array so that an OP_VBegin will get generated for it.  Add pTab to the
** array if it is missing.  If pTab is already in the array, this routine
** is a no-op.
*/
void sqlitexVtabMakeWritable(Parse *pParse, Table *pTab){
  Parse *pToplevel = sqlitexParseToplevel(pParse);
  int i, n;
  Table **apVtabLock;

  assert( IsVirtual(pTab) );
  for(i=0; i<pToplevel->nVtabLock; i++){
    if( pTab==pToplevel->apVtabLock[i] ) return;
  }
  n = (pToplevel->nVtabLock+1)*sizeof(pToplevel->apVtabLock[0]);
  apVtabLock = sqlitex_realloc(pToplevel->apVtabLock, n);
  if( apVtabLock ){
    pToplevel->apVtabLock = apVtabLock;
    pToplevel->apVtabLock[pToplevel->nVtabLock++] = pTab;
  }else{
    pToplevel->db->mallocFailed = 1;
  }
}

/*
** Check to see if virtual tale module pMod can be have an eponymous
** virtual table instance.  If it can, create one if one does not already
** exist. Return non-zero if the eponymous virtual table instance exists
** when this routine returns, and return zero if it does not exist.
**
** An eponymous virtual table instance is one that is named after its
** module, and more importantly, does not require a CREATE VIRTUAL TABLE
** statement in order to come into existance.  Eponymous virtual table
** instances always exist.  They cannot be DROP-ed.
**
** Any virtual table module for which xConnect and xCreate are the same
** method can have an eponymous virtual table instance.
*/
int sqlitexVtabEponymousTableInit(Parse *pParse, Module *pMod){
  const sqlitex_module *pModule = pMod->pModule;
  Table *pTab;
  char *zErr = 0;
  int nName;
  int rc;
  sqlitex *db = pParse->db;
  if( pMod->pEpoTab ) return 1;
  if( pModule->xCreate!=0 && pModule->xCreate!=pModule->xConnect ) return 0;
  nName = sqlitexStrlen30(pMod->zName) + 1;
  pTab = sqlitexDbMallocZero(db, sizeof(Table) + nName);
  if( pTab==0 ) return 0;
  pMod->pEpoTab = pTab;
  pTab->zName = (char*)&pTab[1];
  memcpy(pTab->zName, pMod->zName, nName);
  pTab->nRef = 1;
  pTab->pSchema = db->aDb[0].pSchema;
  pTab->tabFlags |= TF_Virtual;
  pTab->nModuleArg = 0;
  pTab->iPKey = -1;
  addModuleArgument(db, pTab, sqlitexDbStrDup(db, pTab->zName));
  addModuleArgument(db, pTab, 0);
  addModuleArgument(db, pTab, sqlitexDbStrDup(db, pTab->zName));
  rc = vtabCallConstructor(db, pTab, pMod, pModule->xConnect, &zErr);
  if( rc ){
    sqlitexErrorMsg(pParse, "%s", zErr);
    sqlitexDbFree(db, zErr);
    sqlitexVtabEponymousTableClear(db, pMod);
    return 0;
  }
  return 1;
}

/*
** Erase the eponymous virtual table instance associated with
** virtual table module pMod, if it exists.
*/
void sqlitexVtabEponymousTableClear(sqlitex *db, Module *pMod){
  Table *pTab = pMod->pEpoTab;
  if( (pTab = pMod->pEpoTab)!=0 ){
    sqlitexDeleteColumnNames(db, pTab);
    sqlitexVtabClear(db, pTab);
    sqlitexDbFree(db, pTab);
    pMod->pEpoTab = 0;
  }
}

/*
** Return the ON CONFLICT resolution mode in effect for the virtual
** table update operation currently in progress.
**
** The results of this routine are undefined unless it is called from
** within an xUpdate method.
*/
int sqlitex_vtab_on_conflict(sqlitex *db){
  static const unsigned char aMap[] = { 
    SQLITE_ROLLBACK, SQLITE_ABORT, SQLITE_FAIL, SQLITE_IGNORE, SQLITE_REPLACE 
  };
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif
  assert( OE_Rollback==1 && OE_Abort==2 && OE_Fail==3 );
  assert( OE_Ignore==4 && OE_Replace==5 );
  assert( db->vtabOnConflict>=1 && db->vtabOnConflict<=5 );
  return (int)aMap[db->vtabOnConflict-1];
}

/*
** Call from within the xCreate() or xConnect() methods to provide 
** the SQLite core with additional information about the behavior
** of the virtual table being implemented.
*/
int sqlitex_vtab_config(sqlitex *db, int op, ...){
  va_list ap;
  int rc = SQLITE_OK;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif
  sqlitex_mutex_enter(db->mutex);
  va_start(ap, op);
  switch( op ){
    case SQLITE_VTAB_CONSTRAINT_SUPPORT: {
      VtabCtx *p = db->pVtabCtx;
      if( !p ){
        rc = SQLITE_MISUSE_BKPT;
      }else{
        assert( p->pTab==0 || (p->pTab->tabFlags & TF_Virtual)!=0 );
        p->pVTable->bConstraint = (u8)va_arg(ap, int);
      }
      break;
    }
    default:
      rc = SQLITE_MISUSE_BKPT;
      break;
  }
  va_end(ap);

  if( rc!=SQLITE_OK ) sqlitexError(db, rc);
  sqlitex_mutex_leave(db->mutex);
  return rc;
}

#endif /* SQLITE_OMIT_VIRTUALTABLE */
