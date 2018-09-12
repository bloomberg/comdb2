/*
**
** Vtables interface for Schema Tables.
**
** Though this is technically an extension, currently it must be
** built as part of SQLITE_CORE, as comdb2 does not support
** run time extensions at this time.
**
** For a little while we had to use our own "fake" tables, because
** eponymous system tables did not exist. Now that they do, we
** have moved schema tables to their own extension.
**
** We have piggy backed off of SQLITE_BUILDING_FOR_COMDB2 here, though
** a new #define would also suffice.
*/
#if (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
    && !defined(SQLITE_OMIT_VIRTUALTABLE)

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
# define SQLITE_CORE 1
#endif

#include <stdlib.h>
#include <string.h>

#include "comdb2.h"
#include "sql.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"

/* permissions_cursor is a subclass of sqlite3_vtab_cursor which serves
** as the underlying cursor to enumerate the rows in this vtable.
*/
typedef struct permissions_cursor permissions_cursor;
struct permissions_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  int iUser;
  int iTable;
  char **ppUsers;
  int nUsers;
  char **ppTables;
  int nTables;
};

typedef struct {
    sqlite3_vtab base; /* Base class - must be first */
    sqlite3 *db;       /* To access registered system tables */
} systbl_permissions_vtab;

static int permissionsConnect(
  sqlite3 *db,
  void *pAux,
  int argc,
  const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pErr
){
  systbl_permissions_vtab *pNew;
  int rc;

  /* Column numbers */
# define STTP_TABLE 0
# define STTP_USER 1
# define STTP_READ 2
# define STTP_WRITE 3
# define STTP_DDL 4

  rc = sqlite3_declare_vtab(db, "CREATE TABLE comdb2_tablepermissions"
                                "(tablename,username,READ,WRITE,DDL);");
  if( rc==SQLITE_OK ){
    pNew = sqlite3_malloc( sizeof(systbl_permissions_vtab) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(systbl_permissions_vtab));
    pNew->db = db;
  }

  *ppVtab = (sqlite3_vtab *)pNew;

  return rc;
}

/*
** Destructor for sqlite3_vtab objects.
*/
static int permissionsDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int permissionsOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  systbl_permissions_vtab *vtab = (systbl_permissions_vtab *)p;

  permissions_cursor *pCur = sqlite3_malloc(sizeof(*pCur));
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));

  struct sql_thread *thd = pthread_getspecific(query_info_key);
  char *usr = thd->clnt->user;

  rdlock_schema_lk();
  pCur->ppTables = sqlite3_malloc(sizeof(char*) * (thedb->num_dbs +
                                                   vtab->db->aModule.count));
  for(int i=0;i<thedb->num_dbs;++i) {
    // skip sqlite_stat* ?
    char *tbl = thedb->dbs[i]->tablename;
    int err;
    if( bdb_check_user_tbl_access(NULL, usr, tbl, ACCESS_READ, &err)!=0 ){
      continue;
    }
    pCur->ppTables[pCur->nTables++] = strdup(tbl);
  }
  bdb_user_get_all(&pCur->ppUsers, &pCur->nUsers);
  unlock_schema_lk();

  HashElem *systbl;
  for(systbl = sqliteHashFirst(&vtab->db->aModule);
      systbl;
      systbl = sqliteHashNext(systbl)) {
    pCur->ppTables[pCur->nTables++] = strdup(systbl->pKey);
  }

  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

static int permissionsClose(sqlite3_vtab_cursor *cur){
  int i;
  permissions_cursor *pCur = (permissions_cursor*)cur;
  /* Memory for users[] is owned by bdb */
  for(i=0; i<pCur->nUsers; ++i){
    free(pCur->ppUsers[i]);
  }
  free(pCur->ppUsers);
  for(i=0; i<pCur->nTables; ++i){
    /* Table names were strdup'd */
    free(pCur->ppTables[i]);
  }
  sqlite3_free(pCur->ppTables);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int permissionsNext(sqlite3_vtab_cursor *cur){
  permissions_cursor *pCur = (permissions_cursor*)cur;
  ++pCur->iUser;
  if( pCur->iUser>=pCur->nUsers ){
    pCur->iUser = 0;
    ++pCur->iTable;
  }
  return SQLITE_OK;
}

static int permissionsColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  permissions_cursor *pCur = (permissions_cursor*)cur;
  char *tbl = pCur->ppTables[pCur->iTable];
  char *usr = pCur->ppUsers[pCur->iUser];
  switch( i ){
    case STTP_TABLE: {
      sqlite3_result_text(ctx, tbl, -1, NULL);
      break;
    }
    case STTP_USER: {
      sqlite3_result_text(ctx, usr, -1, NULL);
      break;
    }
    case STTP_READ: 
    case STTP_WRITE:
    case STTP_DDL: {
      int access, err;
      if( i==STTP_READ ){
        access = ACCESS_READ;
      }else if( i==STTP_WRITE ){
        access = ACCESS_WRITE;
      }else{
        access = ACCESS_DDL;
      }
      sqlite3_result_text(ctx,
        YESNO(!bdb_check_user_tbl_access(NULL, usr, tbl, access, &err)),
        -1, SQLITE_STATIC);
      break;
    }
  }
  return SQLITE_OK;
};

static int permissionsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  permissions_cursor *pCur = (permissions_cursor*)cur;
  *pRowid = pCur->iUser;
  return SQLITE_OK;
}

static int permissionsEof(sqlite3_vtab_cursor *cur){
  permissions_cursor *pCur = (permissions_cursor*)cur;
  if (pCur->nUsers == 0)
      return 1;
  return pCur->iTable >= pCur->nTables;
}

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to xColumn() or xRowid() or xEof().
*/
static int permissionsFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  permissions_cursor *pCur = (permissions_cursor*)pVtabCursor;

  pCur->iUser = 0;
  pCur->iTable = 0;
  return SQLITE_OK;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int permissionsBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlite3_module systblTablePermissionsModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  permissionsConnect,        /* xConnect */
  permissionsBestIndex,      /* xBestIndex */
  permissionsDisconnect,     /* xDisconnect */
  0,                         /* xDestroy */
  permissionsOpen,           /* xOpen - open a cursor */
  permissionsClose,          /* xClose - close a cursor */
  permissionsFilter,         /* xFilter - configure scan constraints */
  permissionsNext,           /* xNext - advance a cursor */
  permissionsEof,            /* xEof - check for end of scan */
  permissionsColumn,         /* xColumn - read data */
  permissionsRowid,          /* xRowid - read data */
  0,                         /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindMethod */
  0,                         /* xRename */
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
