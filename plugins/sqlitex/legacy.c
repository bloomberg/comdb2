/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Main file for the SQLite library.  The routines in this file
** implement the programmer interface to the library.  Routines in
** other files are for internal use by SQLite and should not be
** accessed by users of the library.
*/

#include "sqliteInt.h"

/*
** Execute SQL code.  Return one of the SQLITE_ success/failure
** codes.  Also write an error message into memory obtained from
** malloc() and make *pzErrMsg point to that message.
**
** If the SQL is a query, then for each row in the query result
** the xCallback() function is called.  pArg becomes the first
** argument to xCallback().  If xCallback=NULL then no callback
** is invoked, even for queries.
*/
int sqlitex_exec(
  sqlitex *db,                /* The database on which the SQL executes */
  const char *zSql,           /* The SQL to be executed */
  sqlitex_callback xCallback, /* Invoke this callback routine */
  void *pArg,                 /* First argument to xCallback() */
  char **pzErrMsg             /* Write error messages here */
){
  int rc = SQLITE_OK;         /* Return code */
  const char *zLeftover;      /* Tail of unprocessed SQL */
  sqlitex_stmt *pStmt = 0;    /* The current SQL statement */
  char **azCols = 0;          /* Names of result columns */
  int callbackIsInit;         /* True if callback data is initialized */

  if( !sqlitexSafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
  if( zSql==0 ) zSql = "";

  sqlitex_mutex_enter(db->mutex);
  sqlitexError(db, SQLITE_OK);
  while( rc==SQLITE_OK && zSql[0] ){
    int nCol;
    char **azVals = 0;

    pStmt = 0;
    rc = sqlitex_prepare_v2(db, zSql, -1, &pStmt, &zLeftover);
    assert( rc==SQLITE_OK || pStmt==0 );
    if( rc!=SQLITE_OK ){
      continue;
    }
    if( !pStmt ){
      /* this happens for a comment or white-space */
      zSql = zLeftover;
      continue;
    }

    callbackIsInit = 0;
    nCol = sqlitex_column_count(pStmt);

    while( 1 ){
      int i;
      rc = sqlitex_step(pStmt);

      /* Invoke the callback function if required */
      if( xCallback && (SQLITE_ROW==rc || 
          (SQLITE_DONEX==rc && !callbackIsInit
                           && db->flags&SQLITE_NullCallback)) ){
        if( !callbackIsInit ){
          azCols = sqlitexDbMallocZero(db, 2*nCol*sizeof(const char*) + 1);
          if( azCols==0 ){
            goto exec_out;
          }
          for(i=0; i<nCol; i++){
            azCols[i] = (char *)sqlitex_column_name(pStmt, i);
            /* sqlitexVdbeSetColName() installs column names as UTF8
            ** strings so there is no way for sqlitex_column_name() to fail. */
            assert( azCols[i]!=0 );
          }
          callbackIsInit = 1;
        }
        if( rc==SQLITE_ROW ){
          azVals = &azCols[nCol];
          for(i=0; i<nCol; i++){
            azVals[i] = (char *)sqlitex_column_text(pStmt, i);
            if( !azVals[i] && sqlitex_column_type(pStmt, i)!=SQLITE_NULL ){
              db->mallocFailed = 1;
              goto exec_out;
            }
          }
        }
        if( xCallback(pArg, nCol, azVals, azCols) ){
          /* EVIDENCE-OF: R-38229-40159 If the callback function to
          ** sqlitex_exec() returns non-zero, then sqlitex_exec() will
          ** return SQLITE_ABORT. */
          rc = SQLITE_ABORT;
          sqlitexVdbeFinalize((Vdbe *)pStmt);
          pStmt = 0;
          sqlitexError(db, SQLITE_ABORT);
          goto exec_out;
        }
      }

      if( rc!=SQLITE_ROW ){
        rc = sqlitexVdbeFinalize((Vdbe *)pStmt);
        pStmt = 0;
        zSql = zLeftover;
        while( sqlitexIsspace(zSql[0]) ) zSql++;
        break;
      }
    }

    sqlitexDbFree(db, azCols);
    azCols = 0;
  }

exec_out:
  if( pStmt ) sqlitexVdbeFinalize((Vdbe *)pStmt);
  sqlitexDbFree(db, azCols);

  rc = sqlitexApiExit(db, rc);
  if( rc!=SQLITE_OK && pzErrMsg ){
    int nErrMsg = 1 + sqlitexStrlen30(sqlitex_errmsg(db));
    *pzErrMsg = sqlitexMalloc(nErrMsg);
    if( *pzErrMsg ){
      memcpy(*pzErrMsg, sqlitex_errmsg(db), nErrMsg);
    }else{
      rc = SQLITE_NOMEM;
      sqlitexError(db, SQLITE_NOMEM);
    }
  }else if( pzErrMsg ){
    *pzErrMsg = 0;
  }

  assert( (rc&db->errMask)==rc );
  sqlitex_mutex_leave(db->mutex);
  return rc;
}
