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

#include <bdb_int.h>
#include <sql.h>
#include <comdb2.h>
#include <comdb2systbl.h>
#include <comdb2systblInt.h>

/* systbl_users_cursor is a subclass of sqlite3_vtab_cursor which
** serves as the underlying cursor to enumerate the rows in this
** vtable. As of now the only thing this cursor reports is the size of
** the table.
*/
typedef struct systbl_users_cursor systbl_users_cursor;
struct systbl_users_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
  char **ppUsers;
  int nUsers;
};

static int systblUsersConnect(
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
#define STUSER_USER     0
#define STUSER_ISOP     1

  rc = sqlite3_declare_vtab(db, "CREATE TABLE comdb2_users(username, isOP)");
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
static int systblUsersDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for systbl_users_cursor objects.
*/
static int systblUsersOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  systbl_users_cursor *pCur = sqlite3_malloc(sizeof(*pCur));
  tran_type *trans = curtran_gettran();
  if (!trans) {
      logmsg(LOGMSG_ERROR, "%s cannot create transaction object\n", __func__);
      return -1;
  }
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  if( bdb_user_get_all_tran(trans, &pCur->ppUsers, &pCur->nUsers) ) {
    sqlite3_free(pCur);
    logmsg(LOGMSG_ERROR, "%s error get all trans\n", __func__);
    return SQLITE_INTERNAL;
  }
  *ppCursor = &pCur->base;
  curtran_puttran(trans);
  return SQLITE_OK;
}

/*
** Destructor for systbl_users_cursor.
*/
static int systblUsersClose(sqlite3_vtab_cursor *cur){
  int i;
  systbl_users_cursor *pCur = (systbl_users_cursor*)cur;
  /* Memory for users[] is owned by bdb */
  for(i=0; i<pCur->nUsers; ++i){
    free(pCur->ppUsers[i]);
  }
  free(pCur->ppUsers);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/*
** Advance to the next user name
*/
static int systblUsersNext(sqlite3_vtab_cursor *cur){
  systbl_users_cursor *pCur = (systbl_users_cursor*)cur;
  ++pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return the user name for the current row.
*/
static int systblUsersColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  systbl_users_cursor *pCur = (systbl_users_cursor*)cur;
  char *user = pCur->ppUsers[pCur->iRowid];
  switch( i ){
    case STUSER_USER: {
      sqlite3_result_text(ctx, user, -1, NULL);
      break;
    }
    case STUSER_ISOP: {
      int bdberr;
      sqlite3_result_text(ctx,
        YESNO(!bdb_tbl_op_access_get(NULL, NULL, 0, "", user, &bdberr)),
        -1, NULL);
      break;
    }
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row. The rowid is the just the
** index of this user into the db array.
*/
static int systblUsersRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_users_cursor *pCur = (systbl_users_cursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int systblUsersEof(sqlite3_vtab_cursor *cur){
  systbl_users_cursor *pCur = (systbl_users_cursor*)cur;
  return  pCur->iRowid == pCur->nUsers;
}

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to xColumn() or xRowid() or xEof().
*/
static int systblUsersFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  systbl_users_cursor *pCur = (systbl_users_cursor*)pVtabCursor;

  pCur->iRowid = 0;
  return SQLITE_OK;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int systblUsersBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlite3_module systblUsersModule = {
  0,                     /* iVersion */
  0,                     /* xCreate */
  systblUsersConnect,    /* xConnect */
  systblUsersBestIndex,  /* xBestIndex */
  systblUsersDisconnect, /* xDisconnect */
  0,                     /* xDestroy */
  systblUsersOpen,       /* xOpen - open a cursor */
  systblUsersClose,      /* xClose - close a cursor */
  systblUsersFilter,     /* xFilter - configure scan constraints */
  systblUsersNext,       /* xNext - advance a cursor */
  systblUsersEof,        /* xEof - check for end of scan */
  systblUsersColumn,     /* xColumn - read data */
  systblUsersRowid,      /* xRowid - read data */
  0,                     /* xUpdate */
  0,                     /* xBegin */
  0,                     /* xSync */
  0,                     /* xCommit */
  0,                     /* xRollback */
  0,                     /* xFindMethod */
  0,                     /* xRename */
  0,                     /* xSavepoint */
  0,                     /* xRelease */
  0,                     /* xRollbackTo */
  0,                     /* xShadowName */
  .access_flag = CDB2_ALLOW_ALL,
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
