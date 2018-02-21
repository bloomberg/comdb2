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

#include "comdb2.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "osqlblockproc.h"

/* systbl_tablehits_cursor is a subclass of sqlite3_vtab_cursor which serves
** as the underlying cursor to enumerate the rows in this vtable. The 
** rows in this vtable are of course the list of tables in the database.
** That is, "select name from sqlite_master where type='table'"
*/
typedef struct systbl_tablehits_cursor systbl_tablehits_cursor;
struct systbl_tablehits_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  int nhits;
  struct table_hits *hits;
  sqlite3_int64 iRowid;      /* The rowid */
};

static int systblTableHitsConnect(
  sqlite3 *db,
  void *pAux,
  int argc,
  const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pErr
){
  sqlite3_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db, "CREATE TABLE comdb2_table_hits(tablename, finds, nexts, inserts, deletes, updates)");
  if( rc==SQLITE_OK ){
    pNew = *ppVtab = sqlite3_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }
  return rc;
}

/*
** Destructor for sqlite3_vtab objects.
*/
static int systblTableHitsDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int checkRowidAccess(systbl_tablehits_cursor *pCur) {
  while (pCur->iRowid < thedb->num_dbs) {
    struct dbtable *pDb = thedb->dbs[pCur->iRowid];
    char *x = pDb->tablename;
    int bdberr;
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    int rc = bdb_check_user_tbl_access(thedb->bdb_env, thd->sqlclntstate->user, x, ACCESS_READ, &bdberr);
    if (rc == 0)
       return SQLITE_OK;
    pCur->iRowid++;
  }
  return SQLITE_OK;
}

extern pthread_rwlock_t schema_lk;

/*
** Constructor for systbl_tablehits_cursor objects.
*/
static int systblTableHitsOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  systbl_tablehits_cursor *pCur;
  int i;

  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  checkRowidAccess(pCur);
  pthread_rwlock_rdlock(&schema_lk);
  pCur->hits = sqlite3_malloc(thedb->num_dbs * sizeof(struct table_hits));
  for (i = 0; i < thedb->num_dbs; i++) {
      pCur->hits[i].table = strdup(thedb->dbs[i]->tablename);
      pCur->hits[i].inserts = thedb->dbs[i]->inserts;
      pCur->hits[i].deletes = thedb->dbs[i]->deletes;
      pCur->hits[i].updates = thedb->dbs[i]->updates;
      pCur->hits[i].finds = thedb->dbs[i]->finds;
      pCur->hits[i].nexts = thedb->dbs[i]->nexts;
  }
  pCur->nhits = thedb->num_dbs;
  pthread_rwlock_unlock(&schema_lk);
  return SQLITE_OK;
}

/*
** Destructor for systbl_tablehits_cursor.
*/
static int systblTableHitsClose(sqlite3_vtab_cursor *cur){
  systbl_tablehits_cursor *pCur = (systbl_tablehits_cursor*)cur;

  sqlite3_free(pCur->hits);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/*
** Advance to the next table name from thedb.
*/
static int systblTableHitsNext(sqlite3_vtab_cursor *cur){
  systbl_tablehits_cursor *pCur = (systbl_tablehits_cursor*)cur;
  pCur->iRowid++;
  checkRowidAccess(pCur);
  return SQLITE_OK;
}

/*
** Return the table name for the current row.
*/
static int systblTableHitsColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  systbl_tablehits_cursor *pCur = (systbl_tablehits_cursor*)cur;

  switch(i) {
      case 0:
          sqlite3_result_text(ctx, pCur->hits[pCur->iRowid].table, -1, NULL);
          break;
      case 1:
          sqlite3_result_int64(ctx, pCur->hits[pCur->iRowid].finds);
          break;
      case 2:
          sqlite3_result_int64(ctx, pCur->hits[pCur->iRowid].nexts);
          break;
      case 3:
          sqlite3_result_int64(ctx, pCur->hits[pCur->iRowid].inserts);
          break;
      case 4:
          sqlite3_result_int64(ctx, pCur->hits[pCur->iRowid].deletes);
          break;
      case 5:
          sqlite3_result_int64(ctx, pCur->hits[pCur->iRowid].updates);
          break;
  }

  return SQLITE_OK;
};

/*
** Return the rowid for the current row. The rowid is the just the
** index of this table into the db array.
*/
static int systblTableHitsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_tablehits_cursor *pCur = (systbl_tablehits_cursor*)cur;

  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int systblTableHitsEof(sqlite3_vtab_cursor *cur){
  systbl_tablehits_cursor *pCur = (systbl_tablehits_cursor*)cur;
  return pCur->iRowid >= pCur->nhits;
}

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to seriesColumn() or seriesRowid() or
** seriesEof().
*/
static int systblTableHitsFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  systbl_tablehits_cursor *pCur = (systbl_tablehits_cursor*)pVtabCursor;

  pCur->iRowid = 0;
  return SQLITE_OK;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int systblTableHitsBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlite3_module systblTableHitsModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  systblTableHitsConnect,       /* xConnect */
  systblTableHitsBestIndex,     /* xBestIndex */
  systblTableHitsDisconnect,    /* xDisconnect */
  0,                         /* xDestroy */
  systblTableHitsOpen,          /* xOpen - open a cursor */
  systblTableHitsClose,         /* xClose - close a cursor */
  systblTableHitsFilter,        /* xFilter - configure scan constraints */
  systblTableHitsNext,          /* xNext - advance a cursor */
  systblTableHitsEof,           /* xEof - check for end of scan */
  systblTableHitsColumn,        /* xColumn - read data */
  systblTableHitsRowid,         /* xRowid - read data */
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
