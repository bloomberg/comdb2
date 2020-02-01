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

/* systbl_tablepermissions_cursor is a subclass of sqlitex_vtab_cursor which serves
** as the underlying cursor to enumerate the rows in this vtable.
*/
typedef struct systbl_tablepermissions_cursor systbl_tablepermissions_cursor;
struct systbl_tablepermissions_cursor {
  sqlitex_vtab_cursor base;  /* Base class - must be first */
  sqlitex_int64 iRowid;     /* The rowid */
  sqlitex_int64 iDbid;      /* The Dbid */
  int           is_last;
  char          user[100];
  int           is_op;
  char          key[1000];
};

static int systblTablePermissionsConnect(
  sqlitex *db,
  void *pAux,
  int argc,
  const char *const*argv,
  sqlitex_vtab **ppVtab,
  char **pErr
){
  sqlitex_vtab *pNew;
  int rc;

/* Column numbers */
#define STTP_TABLE     0
#define STTP_USER    1
#define STTP_READ      2
#define STTP_WRITE      3
#define STTP_DDL   4

  rc = sqlitex_declare_vtab(db,
     "CREATE TABLE comdb2_tablepermissions(tablename,"
                                   "username,"
                                   "READ,"
                                   "WRITE,"
                                   "OP)");
  if( rc==SQLITE_OK ){
    pNew = *ppVtab = sqlitex_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }
  return rc;
}

/*
** Destructor for sqlitex_vtab objects.
*/
static int systblTablePermissionsDisconnect(sqlitex_vtab *pVtab){
  sqlitex_free(pVtab);
  return SQLITE_OK;
}

extern int gbl_allow_user_schema;

static int checkRowidAccess(systbl_tablepermissions_cursor *pCur) {
  while (pCur->iDbid < thedb->num_dbs) {
    struct db *pDb = thedb->dbs[pCur->iDbid];
    char *x = pDb->dbname;
    int bdberr;
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    int rc = bdb_check_user_tbl_access(thedb->bdb_env, thd->clnt->user, x, ACCESS_READ, &bdberr);
    if (rc == 0)
       return SQLITE_OK;
    pCur->iDbid++;
  }
  return SQLITE_OK;
}

/*
** Constructor for systbl_tablepermissions_cursor objects.
*/
static int systblTablePermissionsOpen(sqlitex_vtab *p, sqlitex_vtab_cursor **ppCursor){
  systbl_tablepermissions_cursor *pCur;

  pCur = sqlitex_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  if (gbl_allow_user_schema) {
    checkRowidAccess(pCur);
    int bdberr;
    int rc = bdb_first_user_get(thedb->bdb_env, NULL,pCur->key,pCur->user, &pCur->is_op, &bdberr);
    if (rc)
        pCur->is_last = 1;
  }  else {
    pCur->is_last = 1;
  }
  return SQLITE_OK;
}

/*
** Destructor for systbl_tablepermissions_cursor.
*/
static int systblTablePermissionsClose(sqlitex_vtab_cursor *cur){
  sqlitex_free(cur);
  return SQLITE_OK;
}

/*
** Advance to the next table name from thedb.
*/
static int systblTablePermissionsNext(sqlitex_vtab_cursor *cur){
  systbl_tablepermissions_cursor *pCur = (systbl_tablepermissions_cursor*)cur;
  
  int bdberr;
  int rc = bdb_next_user_get(thedb->bdb_env, NULL, pCur->key, pCur->user, &pCur->is_op, &bdberr);


  if (rc) {
      pCur->iDbid++;
      pCur->is_last = 1;
      bzero(pCur->key, sizeof(pCur->key));
      bdb_first_user_get(thedb->bdb_env, NULL,pCur->key,pCur->user, &pCur->is_op, &bdberr);
  }

  pCur->iRowid++;

  return SQLITE_OK;
}

/*
** Return the table name for the current row.
*/
static int systblTablePermissionsColumn(
  sqlitex_vtab_cursor *cur,
  sqlitex_context *ctx,
  int i
){
  systbl_tablepermissions_cursor *pCur = (systbl_tablepermissions_cursor*)cur;
  struct db *pDb = thedb->dbs[pCur->iDbid];

  int bdberr;
  int rc;
  int access_type;
  switch( i ){
    case STTP_TABLE: {
      sqlitex_result_text(ctx, pDb->dbname, -1, NULL);
      break;
    }
    case STTP_USER: {
      sqlitex_result_text(ctx, pCur->user, -1, NULL);
      break;
    }
    case STTP_READ: 
    case STTP_WRITE:
    case STTP_DDL:
      if (i == STTP_READ) {
        access_type = ACCESS_READ;  
      } else if (i == STTP_WRITE) {
        access_type = ACCESS_WRITE;  
      } else {
        access_type = ACCESS_DDL;  
      }
      rc = bdb_check_user_tbl_access(thedb->bdb_env, pCur->user, pDb->dbname, access_type, &bdberr);
      if (rc == 0) {
        sqlitex_result_text(ctx,  "Y", -1, SQLITEX_STATIC);
      } else {
        sqlitex_result_text(ctx, "N", -1, SQLITEX_STATIC);
      }
      break;
  }
  return SQLITE_OK;
};

/*
** Return the rowid for the current row. 
*/
static int systblTablePermissionsRowid(sqlitex_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_tablepermissions_cursor *pCur = (systbl_tablepermissions_cursor*)cur;

  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int systblTablePermissionsEof(sqlitex_vtab_cursor *cur){
  systbl_tablepermissions_cursor *pCur = (systbl_tablepermissions_cursor*)cur;

  return pCur->iDbid >= thedb->num_dbs;
}

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to xColumn() or xRowid() or xEof().
*/
static int systblTablePermissionsFilter(
  sqlitex_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlitex_value **argv
){
  systbl_tablepermissions_cursor *pCur =
    (systbl_tablepermissions_cursor*)pVtabCursor;

  pCur->iRowid = 0;
  pCur->iDbid = 0;
  return SQLITE_OK;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int systblTablePermissionsBestIndex(
  sqlitex_vtab *tab,
  sqlitex_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlitex_module systblTablePermissionsModule = {
  0,                          /* iVersion */
  0,                          /* xCreate */
  systblTablePermissionsConnect,       /* xConnect */
  systblTablePermissionsBestIndex,     /* xBestIndex */
  systblTablePermissionsDisconnect,    /* xDisconnect */
  0,                          /* xDestroy */
  systblTablePermissionsOpen,          /* xOpen - open a cursor */
  systblTablePermissionsClose,         /* xClose - close a cursor */
  systblTablePermissionsFilter,        /* xFilter - configure scan constraints */
  systblTablePermissionsNext,          /* xNext - advance a cursor */
  systblTablePermissionsEof,           /* xEof - check for end of scan */
  systblTablePermissionsColumn,        /* xColumn - read data */
  systblTablePermissionsRowid,         /* xRowid - read data */
  0,                          /* xUpdate */
  0,                          /* xBegin */
  0,                          /* xSync */
  0,                          /* xCommit */
  0,                          /* xRollback */
  0,                          /* xFindMethod */
  0,                          /* xRename */
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */



