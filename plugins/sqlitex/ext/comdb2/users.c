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

/* systbl_users_cursor is a subclass of sqlitex_vtab_cursor which
** serves as the underlying cursor to enumerate the rows in this
** vtable. As of now the only thing this cursor reports is the size of
** the table.
*/
typedef struct systbl_users_cursor systbl_users_cursor;
struct systbl_users_cursor {
  sqlitex_vtab_cursor base;  /* Base class - must be first */
  sqlitex_int64 iRowid;      /* The rowid */
  int           is_last;
  char          user[100];
  int           is_op;
  char          key[1000];
};

static int systblUsersConnect(
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
#define STUSER_USER     0
#define STUSER_ISOP     1

  rc = sqlitex_declare_vtab(db,
     "CREATE TABLE comdb2_users(username, isOP)");
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
static int systblUsersDisconnect(sqlitex_vtab *pVtab){
  sqlitex_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for systbl_users_cursor objects.
*/
static int systblUsersOpen(sqlitex_vtab *p, sqlitex_vtab_cursor **ppCursor){
  systbl_users_cursor *pCur;

  pCur = sqlitex_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  int bdberr;
  int rc = bdb_first_user_get(thedb->bdb_env, NULL, pCur->key, pCur->user, &pCur->is_op, &bdberr);
  if (rc)
      pCur->is_last = 1;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for systbl_users_cursor.
*/
static int systblUsersClose(sqlitex_vtab_cursor *cur){
  sqlitex_free(cur);
  return SQLITE_OK;
}

/*
** Advance to the next user name
*/
static int systblUsersNext(sqlitex_vtab_cursor *cur){
  systbl_users_cursor *pCur = (systbl_users_cursor*)cur;

  int bdberr;
  int rc = bdb_next_user_get(thedb->bdb_env, NULL, pCur->key, pCur->user, &pCur->is_op, &bdberr);
  if (rc)
      pCur->is_last = 1;
  pCur->iRowid++;
  return SQLITE_OK;
}

/*
** Return the user name for the current row.
*/
static int systblUsersColumn(
  sqlitex_vtab_cursor *cur,
  sqlitex_context *ctx,
  int i
){
  systbl_users_cursor *pCur = (systbl_users_cursor*)cur;

  switch( i ){
    case STUSER_USER: {
      sqlitex_result_text(ctx, pCur->user, -1, NULL);
      break;
    }
    case STUSER_ISOP: {
      sqlitex_result_int64(ctx, (sqlitex_int64)pCur->is_op);
    }
  }
  return SQLITE_OK;
};

/*
** Return the rowid for the current row. The rowid is the just the
** index of this user into the db array.
*/
static int systblUsersRowid(sqlitex_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_users_cursor *pCur = (systbl_users_cursor*)cur;

  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int systblUsersEof(sqlitex_vtab_cursor *cur){
  systbl_users_cursor *pCur = (systbl_users_cursor*)cur;
  return  pCur->is_last;
}

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to xColumn() or xRowid() or xEof().
*/
static int systblUsersFilter(
  sqlitex_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlitex_value **argv
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
  sqlitex_vtab *tab,
  sqlitex_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlitex_module systblUsersModule = {
  0,                            /* iVersion */
  0,                            /* xCreate */
  systblUsersConnect,       /* xConnect */
  systblUsersBestIndex,     /* xBestIndex */
  systblUsersDisconnect,    /* xDisconnect */
  0,                            /* xDestroy */
  systblUsersOpen,          /* xOpen - open a cursor */
  systblUsersClose,         /* xClose - close a cursor */
  systblUsersFilter,        /* xFilter - configure scan constraints */
  systblUsersNext,          /* xNext - advance a cursor */
  systblUsersEof,           /* xEof - check for end of scan */
  systblUsersColumn,        /* xColumn - read data */
  systblUsersRowid,         /* xRowid - read data */
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
