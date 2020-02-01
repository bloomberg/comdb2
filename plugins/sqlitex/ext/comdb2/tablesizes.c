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
#include "comdb2systbl.h"
#include "comdb2systblInt.h"

/* systbl_tblsize_cursor is a subclass of sqlitex_vtab_cursor which
** serves as the underlying cursor to enumerate the rows in this
** vtable. As of now the only thing this cursor reports is the size of
** the table.
*/
typedef struct systbl_tblsize_cursor systbl_tblsize_cursor;
struct systbl_tblsize_cursor {
  sqlitex_vtab_cursor base;  /* Base class - must be first */
  sqlitex_int64 iRowid;      /* The rowid */
};

static int systblTblSizeConnect(
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
#define STTS_TABLE     0
#define STTS_SIZE      1

  rc = sqlitex_declare_vtab(
    db, "CREATE TABLE comdb2_tablesizes(tablename, bytes)");
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
static int systblTblSizeDisconnect(sqlitex_vtab *pVtab){
  sqlitex_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for systbl_tblsize_cursor objects.
*/
static int systblTblSizeOpen(sqlitex_vtab *p, sqlitex_vtab_cursor **ppCursor){
  systbl_tblsize_cursor *pCur;

  pCur = sqlitex_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for systbl_tblsize_cursor.
*/
static int systblTblSizeClose(sqlitex_vtab_cursor *cur){
  sqlitex_free(cur);
  return SQLITE_OK;
}

/*
** Advance to the next table name from thedb.
*/
static int systblTblSizeNext(sqlitex_vtab_cursor *cur){
  systbl_tblsize_cursor *pCur = (systbl_tblsize_cursor*)cur;

  pCur->iRowid++;
  return SQLITE_OK;
}

/*
** Return the table name for the current row.
*/
static int systblTblSizeColumn(
  sqlitex_vtab_cursor *cur,
  sqlitex_context *ctx,
  int i
){
  systbl_tblsize_cursor *pCur = (systbl_tblsize_cursor*)cur;
  struct db *pDb = thedb->dbs[pCur->iRowid];
  char *x = pDb->dbname;

  switch( i ){
    case STTS_TABLE: {
      sqlitex_result_text(ctx, x, -1, NULL);
      break;
    }
    case STTS_SIZE: {
      calc_table_size(pDb);
      sqlitex_result_int64(ctx, (sqlitex_int64)pDb->totalsize);
    }
  }
  return SQLITE_OK;
};

/*
** Return the rowid for the current row. The rowid is the just the
** index of this table into the db array.
*/
static int systblTblSizeRowid(sqlitex_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_tblsize_cursor *pCur = (systbl_tblsize_cursor*)cur;

  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int systblTblSizeEof(sqlitex_vtab_cursor *cur){
  systbl_tblsize_cursor *pCur = (systbl_tblsize_cursor*)cur;

  return pCur->iRowid >= thedb->num_dbs;
}

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to xColumn() or xRowid() or xEof().
*/
static int systblTblSizeFilter(
  sqlitex_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlitex_value **argv
){
  systbl_tblsize_cursor *pCur = (systbl_tblsize_cursor*)pVtabCursor;

  pCur->iRowid = 0;
  return SQLITE_OK;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int systblTblSizeBestIndex(
  sqlitex_vtab *tab,
  sqlitex_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlitex_module systblTblSizeModule = {
  0,                            /* iVersion */
  0,                            /* xCreate */
  systblTblSizeConnect,       /* xConnect */
  systblTblSizeBestIndex,     /* xBestIndex */
  systblTblSizeDisconnect,    /* xDisconnect */
  0,                            /* xDestroy */
  systblTblSizeOpen,          /* xOpen - open a cursor */
  systblTblSizeClose,         /* xClose - close a cursor */
  systblTblSizeFilter,        /* xFilter - configure scan constraints */
  systblTblSizeNext,          /* xNext - advance a cursor */
  systblTblSizeEof,           /* xEof - check for end of scan */
  systblTblSizeColumn,        /* xColumn - read data */
  systblTblSizeRowid,         /* xRowid - read data */
  0,                            /* xUpdate */
  0,                            /* xBegin */
  0,                            /* xSync */
  0,                            /* xCommit */
  0,                            /* xRollback */
  0,                            /* xFindMethod */
  0,                            /* xRename */
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
