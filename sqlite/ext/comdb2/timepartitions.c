/*
   Copyright 2018 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
#if (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
    && !defined(SQLITE_OMIT_VIRTUALTABLE)

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
# define SQLITE_CORE 1
#endif

#include "views.h"

typedef struct systbl_timepart_cursor systbl_timepart_cursor;
struct systbl_timepart_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
  int maxRowid;
};

/*
** Constructor for sqlite3_vtab object
*/
static int systblTimepartConnect(
  sqlite3 *db,
  void *pAux,
  int argc,
  const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pErr
){
  sqlite3_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db, "CREATE TABLE comdb2_timepart(name, period, retention, nshards, version, shard0name, starttime, source_id)");
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
static int systblTimepartDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for systbl_tables_cursor objects.
*/
static int systblTimepartOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  systbl_timepart_cursor *pCur;

  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  pCur->iRowid = -1;
  views_lock();
  pCur->maxRowid = timepart_get_views();

  return SQLITE_OK;
}

/*
** Destructor for systbl_tables_cursor.
*/
static int systblTimepartClose(sqlite3_vtab_cursor *cur){
  views_unlock();
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Advance to the next table name from thedb.
*/
static int systblTimepartNext(sqlite3_vtab_cursor *cur){
  systbl_timepart_cursor *pCur = (systbl_timepart_cursor*)cur;
  int rc;

  if(pCur->iRowid<pCur->maxRowid)
      pCur->iRowid++;

  return SQLITE_OK;
}

/*
** Return the table name for the current row.
*/
static int systblTimepartColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  systbl_timepart_cursor *pCur = (systbl_timepart_cursor*)cur;

  timepart_systable_get_column(ctx, pCur->iRowid, i);
  return SQLITE_OK;
};

/*
** Return the rowid for the current row (which is the index of the view
** in the view array 
*/
static int systblTimepartRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_timepart_cursor *pCur = (systbl_timepart_cursor*)cur;

  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int systblTimepartEof(sqlite3_vtab_cursor *cur){
  systbl_timepart_cursor *pCur = (systbl_timepart_cursor*)cur;

  return pCur->iRowid >= pCur->maxRowid;
}

static int systblTimepartFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  systbl_timepart_cursor *pCur = (systbl_timepart_cursor*)pVtabCursor;

  pCur->iRowid = 0;
  return SQLITE_OK;
}

static int systblTimepartBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}



const sqlite3_module systblTimepartModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  systblTimepartConnect,     /* xConnect */
  systblTimepartBestIndex,   /* xBestIndex */
  systblTimepartDisconnect,  /* xDisconnect */
  0,                         /* xDestroy */
  systblTimepartOpen,        /* xOpen - open a cursor */
  systblTimepartClose,       /* xClose - close a cursor */
  systblTimepartFilter,      /* xFilter - configure scan constraints */
  systblTimepartNext,        /* xNext - advance a cursor */
  systblTimepartEof,         /* xEof - check for end of scan */
  systblTimepartColumn,      /* xColumn - read data */
  systblTimepartRowid,       /* xRowid - read data */
  0,                         /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindMethod */
  0,                         /* xRename */
};

#endif /* SQLITE_BUILDING_FOR_COMDB2 */
