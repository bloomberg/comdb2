/*
   Copyright 2020 Bloomberg Finance L.P.

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


/*
**
** Vtables interface for Schema Tables.
**
** Though this is technically an extension, currently it must be

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
#include "views.h"
#include "timepart_systable.h"

/* systbl_keys_cursor is a subclass of sqlite3_vtab_cursor which
** serves as the underlying cursor to enumerate keys. We keep track
** of the table we're on and the Key in the given table.
*/
typedef struct systbl_keys_cursor systbl_keys_cursor;
struct systbl_keys_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
  sqlite3_int64 iKeyid;      /* The column we're on */
};

static int systblKeysConnect(
  sqlite3 *db,
  void *pAux,
  int argc,
  const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pErr
){
  sqlite3_vtab *pNew;
  int rc;

/* Column numbers */
#define STKEY_TABLE     0
#define STKEY_KEY       1
#define STKEY_KEYNUM    2
#define STKEY_UNIQUE    3
#define STKEY_DATACOPY  4
#define STKEY_RECNUM    5
#define STKEY_CONDITION 6
#define STKEY_UNIQNULLS 7
#define STKEY_PARTIALDATACOPY 8

  rc = sqlite3_declare_vtab(db, "CREATE TABLE comdb2_keys(tablename,"
                                "keyname,"
                                "keynumber,"
                                "isunique,"
                                "isdatacopy,"
                                "isrecnum,"
                                "condition,"
                                "uniqnulls,"
                                "ispartialdatacopy)");
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
static int systblKeysDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for systbl_keys_cursor objects.
*/
static int systblKeysOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  systbl_keys_cursor *pCur;

  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;

  comdb2_next_allowed_table(&pCur->iRowid);

  return SQLITE_OK;
}

/*
** Destructor for systbl_keys_cursor.
*/
static int systblKeysClose(sqlite3_vtab_cursor *cur){
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Advance to the next key. 
*/
static int systblKeysNext(sqlite3_vtab_cursor *cur){
  systbl_keys_cursor *pCur = (systbl_keys_cursor*)cur;
  struct dbtable *pDb = comdb2_get_dbtable_or_shard0(pCur->iRowid);;

  pCur->iKeyid++;

  /* Test just in case cursor is in a bad state */
  if( pCur->iRowid < timepart_systable_num_tables_and_views() ){
    do{
      while( pCur->iKeyid < pDb->schema->nix
       && pDb->ixsql[pCur->iKeyid] == NULL
      ){
        pCur->iKeyid++;
      }
      if( pCur->iKeyid >= pDb->schema->nix ){
        pCur->iKeyid = 0;
        pCur->iRowid++;
        pDb = comdb2_get_dbtable_or_shard0(pCur->iRowid);;
      }else{
	break;
      }
    } while( pCur->iRowid < timepart_systable_num_tables_and_views() );
  }

  comdb2_next_allowed_table(&pCur->iRowid);

  return SQLITE_OK;
}

/*
** Return the table name for the current row.
*/
static int systblKeysColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  systbl_keys_cursor *pCur = (systbl_keys_cursor*)cur;
  struct dbtable *pDb = comdb2_get_dbtable_or_shard0(pCur->iRowid);;
  struct schema *pSchema = pDb->ixschema[pCur->iKeyid];
  const char *readable_name = pDb->timepartition_name ? pDb->timepartition_name : pDb->tablename;

  switch( i ){
    case STKEY_TABLE: {
      sqlite3_result_text(ctx, readable_name, -1, NULL);
      break;
    }
    case STKEY_KEY: {
      sqlite3_result_text(ctx, pSchema->csctag, -1, NULL);
      break;
    }
    case STKEY_KEYNUM: {
      sqlite3_result_int64(ctx, (sqlite3_int64)pSchema->ixnum);
      break;
    }
    case STKEY_UNIQUE: {
      sqlite3_result_text(ctx, YESNO(!(pSchema->flags & SCHEMA_DUP)),
        -1, SQLITE_STATIC);
      break;
    }
    case STKEY_DATACOPY: {
      sqlite3_result_text(ctx, YESNO(pSchema->flags & SCHEMA_DATACOPY), 
        -1, SQLITE_STATIC);
      break;
    }
    case STKEY_RECNUM: {
      sqlite3_result_text(ctx, YESNO(pSchema->flags & SCHEMA_RECNUM),
        -1, SQLITE_STATIC);
      break;
    }
    case STKEY_UNIQNULLS: {
      sqlite3_result_text(ctx, YESNO(pSchema->flags & SCHEMA_UNIQNULLS),
        -1, SQLITE_STATIC);
      break;
    }
    case STKEY_CONDITION: {
      sqlite3_result_text(ctx, pSchema->where, -1, SQLITE_STATIC);
      break;
    }
    case STKEY_PARTIALDATACOPY: {
      sqlite3_result_text(ctx, YESNO(pSchema->flags & SCHEMA_PARTIALDATACOPY),
        -1, SQLITE_STATIC);
      break;
    }
  }
  return SQLITE_OK; 
}

/*
** Return the rowid for the current key. We arrive at this number by
** iterating through all the keys on all the tables previous to this,
** and then adding the current Keyid for the table that we're
** presently on.
*/
static int systblKeysRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_keys_cursor *pCur = (systbl_keys_cursor*)cur;

  *pRowid = 0;
  for( int i = 0; i < pCur->iRowid - 1; i++ ){
    *pRowid += comdb2_get_dbtable_or_shard0(i)->nsqlix;
  }
  *pRowid += pCur->iKeyid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int systblKeysEof(sqlite3_vtab_cursor *cur){
  systbl_keys_cursor *pCur = (systbl_keys_cursor*)cur;

  return pCur->iRowid >= timepart_systable_num_tables_and_views();
}

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to xColumn() or xRowid() or xEof().
*/
static int systblKeysFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  systbl_keys_cursor *pCur = (systbl_keys_cursor*)pVtabCursor;
  struct dbtable *pDb = comdb2_get_dbtable_or_shard0(0);

  pCur->iRowid = 0;
  pCur->iKeyid = 0;

  /* Advance to the first key, as it's possible that the cursor will
  ** start on a table without a key.
  */
  if( pDb->nsqlix == 0
   || pDb->ixsql[pCur->iKeyid] == NULL
  ){
    systblKeysNext(pVtabCursor);
  }

  return SQLITE_OK;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int systblKeysBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlite3_module systblKeysModule = {
  0,                       /* iVersion */
  0,                       /* xCreate */
  systblKeysConnect,       /* xConnect */
  systblKeysBestIndex,     /* xBestIndex */
  systblKeysDisconnect,    /* xDisconnect */
  0,                       /* xDestroy */
  systblKeysOpen,          /* xOpen - open a cursor */
  systblKeysClose,         /* xClose - close a cursor */
  systblKeysFilter,        /* xFilter - configure scan constraints */
  systblKeysNext,          /* xNext - advance a cursor */
  systblKeysEof,           /* xEof - check for end of scan */
  systblKeysColumn,        /* xColumn - read data */
  systblKeysRowid,         /* xRowid - read data */
  0,                       /* xUpdate */
  0,                       /* xBegin */
  0,                       /* xSync */
  0,                       /* xCommit */
  0,                       /* xRollback */
  0,                       /* xFindMethod */
  0,                       /* xRename */
  0,                       /* xSavepoint */
  0,                       /* xRelease */
  0,                       /* xRollbackTo */
  0,                       /* xShadowName */
  .access_flag = CDB2_ALLOW_ALL,
  .systable_lock = "comdb2_tables",
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
