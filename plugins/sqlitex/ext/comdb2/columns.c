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

/* systbl_columns_cursor is a subclass of sqlitex_vtab_cursor which serves
** as the underlying cursor to enumerate the rows in this vtable. The 
** rows in this vtable are of course the list of tables in the database.
** That is, "select name from sqlite_master where type='table'"
*/
typedef struct systbl_columns_cursor systbl_columns_cursor;
struct systbl_columns_cursor {
  sqlitex_vtab_cursor base;  /* Base class - must be first */
  sqlitex_int64 iRowid;      /* The rowid */
  sqlitex_int64 iColid;      /* The column we're on */
};

static int systblColumnsConnect(
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
#define STCOL_TABLE     0
#define STCOL_COLUMN    1
#define STCOL_TYPE      2
#define STCOL_SIZE      3
#define STCOL_SQLTYPE   4
#define STCOL_INLINESZ  5
#define STCOL_DEFVAL    6
#define STCOL_DBLOAD    7
#define STCOL_ALLOWNULL 8

  rc = sqlitex_declare_vtab(db, "CREATE TABLE comdb2_columns(tablename,"
                                "columnname,"
                                "type,"
                                "size,"
                                "sqltype,"
                                "varinlinesize,"
                                "defaultvalue,"
                                "dbload,"
                                "isnullable)");
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
static int systblColumnsDisconnect(sqlitex_vtab *pVtab){
  sqlitex_free(pVtab);
  return SQLITE_OK;
}

static int checkRowidAccess(systbl_columns_cursor *pCur) {
  while (pCur->iRowid < thedb->num_dbs) {
    struct db *pDb = thedb->dbs[pCur->iRowid];
    char *x = pDb->dbname;
    int bdberr;
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    int rc = bdb_check_user_tbl_access(thedb->bdb_env, thd->clnt->user, x, ACCESS_READ, &bdberr);
    if (rc == 0)
       return SQLITE_OK;
    pCur->iRowid++;
  }
  return SQLITE_OK;
}

/*
** Constructor for systbl_columns_cursor objects.
*/
static int systblColumnsOpen(sqlitex_vtab *p, sqlitex_vtab_cursor **ppCursor){
  systbl_columns_cursor *pCur;

  pCur = sqlitex_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  checkRowidAccess(pCur);
  return SQLITE_OK;
}

/*
** Destructor for systbl_columns_cursor.
*/
static int systblColumnsClose(sqlitex_vtab_cursor *cur){
  sqlitex_free(cur);
  return SQLITE_OK;
}

/*
** Advance to the next table name from thedb.
*/
static int systblColumnsNext(sqlitex_vtab_cursor *cur){
  systbl_columns_cursor *pCur = (systbl_columns_cursor*)cur;

  if( ++pCur->iColid == thedb->dbs[pCur->iRowid]->schema->nmembers ){
    pCur->iColid = 0;
    pCur->iRowid++;
    checkRowidAccess(pCur);
  }
  return SQLITE_OK;
}

/*
** Return the table name for the current row.
*/
static int systblColumnsColumn(
  sqlitex_vtab_cursor *cur,
  sqlitex_context *ctx,
  int i
){
  systbl_columns_cursor *pCur = (systbl_columns_cursor*)cur;
  struct db *pDb = thedb->dbs[pCur->iRowid];
  struct field *pField = &pDb->schema->member[pCur->iColid];

  switch( i ){
    case STCOL_TABLE: {
      sqlitex_result_text(ctx, pDb->dbname, -1, NULL);
      break;
    }
    case STCOL_COLUMN: {
      sqlitex_result_text(ctx, pField->name, -1, NULL);
      break;
    }
    case STCOL_TYPE: {
      sqlitex_result_text(ctx, csc2type(pField), -1, SQLITEX_STATIC);
      break;
    }
    case STCOL_SIZE: {
      sqlitex_result_int64(ctx, (sqlitex_int64)pField->len);
      break;
    }
    case STCOL_SQLTYPE: {
      char *stype;

      /* sizeof("interval month") == 15 */
      stype = sqlitex_malloc(15);
      sqltype(pField, stype, 15);
      sqlitex_result_text(ctx, stype, -1, sqlitex_free);
      break;
    }
    case STCOL_INLINESZ: {
      if( pField->type == SERVER_BLOB2
       || pField->type == SERVER_VUTF8 ){
        sqlitex_result_int64(ctx, (sqlitex_int64)pField->len -5);
      }else{
        sqlitex_result_null(ctx);
      }
      break;
    }
    case STCOL_DEFVAL: {
      if( pField->in_default ){
        char *x = sql_field_default_trans(pField, 0);
        sqlitex_result_text(ctx, x, -1, sqlitex_free);
      }else{
        sqlitex_result_null(ctx);
      }
      break;
    }
    case STCOL_DBLOAD: {
      if( pField->out_default ){
        char *x = sql_field_default_trans(pField, 1);
        sqlitex_result_text(ctx, x, -1, sqlitex_free);
      }else{
        sqlitex_result_null(ctx);
      }
      break;
    }
    case STCOL_ALLOWNULL: {
      sqlitex_result_text(ctx, YESNO(!(pField->flags & NO_NULL)),
        -1, SQLITEX_STATIC);
    }
  }
  return SQLITE_OK;
};

/*
** Return the rowid for the current row. The rowid is the just the
** index of this column in the current row, multiplied by every columns
** of every row preceeding this one.
*/
static int systblColumnsRowid(sqlitex_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_columns_cursor *pCur = (systbl_columns_cursor*)cur;

  *pRowid = 0;
  for( int i = 0; i < pCur->iRowid - 1; i++ ){
    *pRowid += thedb->dbs[i]->schema->nmembers;
  }
  *pRowid += pCur->iColid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int systblColumnsEof(sqlitex_vtab_cursor *cur){
  systbl_columns_cursor *pCur = (systbl_columns_cursor*)cur;

  return pCur->iRowid >= thedb->num_dbs;
}

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to seriesColumn() or seriesRowid() or
** seriesEof().
*/
static int systblColumnsFilter(
  sqlitex_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlitex_value **argv
){
  systbl_columns_cursor *pCur = (systbl_columns_cursor*)pVtabCursor;

  pCur->iRowid = 0;
  pCur->iColid = 0;
  return SQLITE_OK;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int systblColumnsBestIndex(
  sqlitex_vtab *tab,
  sqlitex_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlitex_module systblColumnsModuleX = {
  0,                          /* iVersion */
  0,                          /* xCreate */
  systblColumnsConnect,       /* xConnect */
  systblColumnsBestIndex,     /* xBestIndex */
  systblColumnsDisconnect,    /* xDisconnect */
  0,                          /* xDestroy */
  systblColumnsOpen,          /* xOpen - open a cursor */
  systblColumnsClose,         /* xClose - close a cursor */
  systblColumnsFilter,        /* xFilter - configure scan constraints */
  systblColumnsNext,          /* xNext - advance a cursor */
  systblColumnsEof,           /* xEof - check for end of scan */
  systblColumnsColumn,        /* xColumn - read data */
  systblColumnsRowid,         /* xRowid - read data */
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



