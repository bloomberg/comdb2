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

/* systbl_field_cursor is a subclass of sqlite3_vtab_cursor which
** can enumerate fields of all the fields in systbl_fields_cursor
**
** Though these are called "key components" in the sql, that gets
** unwieldly fast. Instead name them fields, which is what they are
** called in tag.h anyway.
*/
typedef struct systbl_fields_cursor systbl_fields_cursor;
struct systbl_fields_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The tableid */
  sqlite3_int64 iKeyid;      /* The keyid within the table */
  sqlite3_int64 iFieldid;    /* The key field id within the key */
};

static int systblFieldsConnect(
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
#define STFIELD_TABLE      0
#define STFIELD_KEY        1
#define STFIELD_COLNO      2
#define STFIELD_COLNAME    3
#define STFIELD_DEC        4

  rc = sqlite3_declare_vtab(db, "CREATE TABLE comdb2_keycomponents(tablename,"
                                "keyname,"
                                "columnnumber,"
                                "columnname,"
                                "isdescending)");
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
static int systblFieldsDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for systbl_fields_cursor objects.
*/
static int systblFieldsOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  systbl_fields_cursor *pCur;

  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  comdb2_next_allowed_table(&pCur->iRowid);

  return SQLITE_OK;
}

/*
** Destructor for systbl_fields_cursor.
*/
static int systblFieldsClose(sqlite3_vtab_cursor *cur){
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Advance to the next key.
*/
static int systblFieldsNext(sqlite3_vtab_cursor *cur){
  systbl_fields_cursor *pCur = (systbl_fields_cursor*)cur;
  struct dbtable *pDb = comdb2_get_dbtable_or_shard0(pCur->iRowid);;

  pCur->iFieldid++;

  /* Test just in case cursor is in a bad state */
  if( pCur->iRowid < timepart_systable_num_tables_and_views()){
    struct schema *pSchema = NULL;

    /* TODO May be better to re-cast cursor and call systblKeysNext() */
    if( pCur->iKeyid < pDb->schema->nix ){
      pSchema = pDb->schema->ix[pCur->iKeyid];
    }
    if( pSchema == NULL
     || pDb->ixsql[pCur->iKeyid] == NULL
     || pCur->iFieldid >= pSchema->nmembers 
    ){
      pCur->iFieldid = 0;
      pCur->iKeyid++;
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
        } else {
	  break;
	}
      } while( pCur->iRowid < timepart_systable_num_tables_and_views() );
    }
  }

  comdb2_next_allowed_table(&pCur->iRowid);

  return SQLITE_OK;
}

/*
** Return the table name for the current row.
*/
static int systblFieldsColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  systbl_fields_cursor *pCur = (systbl_fields_cursor*)cur;
  struct dbtable *pDb = comdb2_get_dbtable_or_shard0(pCur->iRowid);;
  struct schema *pSchema = pDb->ixschema[pCur->iKeyid];
  struct field *pField = &pSchema->member[pCur->iFieldid];
  const char *readable_name = pDb->timepartition_name ? pDb->timepartition_name : pDb->tablename;

  switch( i ){
    case STFIELD_TABLE: {
      sqlite3_result_text(ctx, readable_name, -1, NULL);
      break;
    }
    case STFIELD_KEY: {
      sqlite3_result_text(ctx, pSchema->csctag, -1, NULL);
      break;
    }
    case STFIELD_COLNO: {
      sqlite3_result_int64(ctx, (sqlite3_int64)pCur->iFieldid);
      break;
    }
    case STFIELD_COLNAME: {
      sqlite3_result_text(ctx, pField->name, -1, NULL);
      break;
    }
    case STFIELD_DEC: {
      sqlite3_result_text(ctx, YESNO(pField->flags & INDEX_DESCEND),
        -1, SQLITE_STATIC);
      break;
    }
  }
  return SQLITE_OK; 
}

/*
** Return the rowid for the current key. We arrive at this number by
** iterating through all the fields on all the keys on all the tables
** previous to this, and then adding the current Fieldid for the key
** that we're presently on.
*/
static int systblFieldsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_fields_cursor *pCur = (systbl_fields_cursor*)cur;
  struct dbtable *pDb = comdb2_get_dbtable_or_shard0(0);
  int i;

  *pRowid = 0;
  for( i = 0; i < pCur->iRowid - 1; i++ ){
    pDb = comdb2_get_dbtable_or_shard0(i);
    for( int j = 0; j < pDb->schema->nix - 1; j++ ){
      if( pDb->ixsql[j] == NULL ) continue;
      *pRowid += pDb->schema->ix[j]->nmembers;
    }
  }
  for( int j = 0; j < pCur->iKeyid - 1; j++ ){
    if( pDb->ixsql[j] == NULL ) continue;
    *pRowid += pDb->schema->ix[j]->nmembers;
  }
  *pRowid += pCur->iFieldid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int systblFieldsEof(sqlite3_vtab_cursor *cur){
  systbl_fields_cursor *pCur = (systbl_fields_cursor*)cur;

  return pCur->iRowid >= timepart_systable_num_tables_and_views();
}

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to xColumn() or xRowid() or xEof().
*/
static int systblFieldsFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  systbl_fields_cursor *pCur = (systbl_fields_cursor*)pVtabCursor;
  struct dbtable *pDb = comdb2_get_dbtable_or_shard0(0);

  pCur->iRowid = 0;
  pCur->iKeyid = 0;
  pCur->iFieldid = 0;

  /* Advance to the first key, as it's possible that the cursor will
  ** start on a table without a key.
  */
  if( pDb->nsqlix == 0
   || pDb->ixsql[pCur->iKeyid] == NULL
  ){
    systblFieldsNext(pVtabCursor);
  }

  return SQLITE_OK;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int systblFieldsBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlite3_module systblFieldsModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  systblFieldsConnect,       /* xConnect */
  systblFieldsBestIndex,     /* xBestIndex */
  systblFieldsDisconnect,    /* xDisconnect */
  0,                         /* xDestroy */
  systblFieldsOpen,          /* xOpen - open a cursor */
  systblFieldsClose,         /* xClose - close a cursor */
  systblFieldsFilter,        /* xFilter - configure scan constraints */
  systblFieldsNext,          /* xNext - advance a cursor */
  systblFieldsEof,           /* xEof - check for end of scan */
  systblFieldsColumn,        /* xColumn - read data */
  systblFieldsRowid,         /* xRowid - read data */
  0,                         /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindMethod */
  0,                         /* xRename */
  0,                         /* xSavepoint */
  0,                         /* xRelease */
  0,                         /* xRollbackTo */
  0,                         /* xShadowName */
  .access_flag = CDB2_ALLOW_ALL,
  .systable_lock = "comdb2_tables",
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
