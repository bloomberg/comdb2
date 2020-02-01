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
** This file contains the sqlitex_get_table() and sqlitex_free_table()
** interface routines.  These are just wrappers around the main
** interface routine of sqlitex_exec().
**
** These routines are in a separate files so that they will not be linked
** if they are not used.
*/
#include "sqliteInt.h"
#include <stdlib.h>
#include <string.h>

#ifndef SQLITE_OMIT_GET_TABLE

/*
** This structure is used to pass data from sqlitex_get_table() through
** to the callback function is uses to build the result.
*/
typedef struct TabResult {
  char **azResult;   /* Accumulated output */
  char *zErrMsg;     /* Error message text, if an error occurs */
  u32 nAlloc;        /* Slots allocated for azResult[] */
  u32 nRow;          /* Number of rows in the result */
  u32 nColumn;       /* Number of columns in the result */
  u32 nData;         /* Slots used in azResult[].  (nRow+1)*nColumn */
  int rc;            /* Return code from sqlitex_exec() */
} TabResult;

/*
** This routine is called once for each row in the result table.  Its job
** is to fill in the TabResult structure appropriately, allocating new
** memory as necessary.
*/
static int sqlitex_get_table_cb(void *pArg, int nCol, char **argv, char **colv){
  TabResult *p = (TabResult*)pArg;  /* Result accumulator */
  int need;                         /* Slots needed in p->azResult[] */
  int i;                            /* Loop counter */
  char *z;                          /* A single column of result */

  /* Make sure there is enough space in p->azResult to hold everything
  ** we need to remember from this invocation of the callback.
  */
  if( p->nRow==0 && argv!=0 ){
    need = nCol*2;
  }else{
    need = nCol;
  }
  if( p->nData + need > p->nAlloc ){
    char **azNew;
    p->nAlloc = p->nAlloc*2 + need;
    azNew = sqlitex_realloc64( p->azResult, sizeof(char*)*p->nAlloc );
    if( azNew==0 ) goto malloc_failed;
    p->azResult = azNew;
  }

  /* If this is the first row, then generate an extra row containing
  ** the names of all columns.
  */
  if( p->nRow==0 ){
    p->nColumn = nCol;
    for(i=0; i<nCol; i++){
      z = sqlitex_mprintf("%s", colv[i]);
      if( z==0 ) goto malloc_failed;
      p->azResult[p->nData++] = z;
    }
  }else if( (int)p->nColumn!=nCol ){
    sqlitex_free(p->zErrMsg);
    p->zErrMsg = sqlitex_mprintf(
       "sqlitex_get_table() called with two or more incompatible queries"
    );
    p->rc = SQLITE_ERROR;
    return 1;
  }

  /* Copy over the row data
  */
  if( argv!=0 ){
    for(i=0; i<nCol; i++){
      if( argv[i]==0 ){
        z = 0;
      }else{
        int n = sqlitexStrlen30(argv[i])+1;
        z = sqlitex_malloc( n );
        if( z==0 ) goto malloc_failed;
        memcpy(z, argv[i], n);
      }
      p->azResult[p->nData++] = z;
    }
    p->nRow++;
  }
  return 0;

malloc_failed:
  p->rc = SQLITE_NOMEM;
  return 1;
}

/*
** Query the database.  But instead of invoking a callback for each row,
** malloc() for space to hold the result and return the entire results
** at the conclusion of the call.
**
** The result that is written to ***pazResult is held in memory obtained
** from malloc().  But the caller cannot free this memory directly.  
** Instead, the entire table should be passed to sqlitex_free_table() when
** the calling procedure is finished using it.
*/
int sqlitex_get_table(
  sqlitex *db,                /* The database on which the SQL executes */
  const char *zSql,           /* The SQL to be executed */
  char ***pazResult,          /* Write the result table here */
  int *pnRow,                 /* Write the number of rows in the result here */
  int *pnColumn,              /* Write the number of columns of result here */
  char **pzErrMsg             /* Write error messages here */
){
  int rc;
  TabResult res;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) || pazResult==0 ) return SQLITE_MISUSE_BKPT;
#endif
  *pazResult = 0;
  if( pnColumn ) *pnColumn = 0;
  if( pnRow ) *pnRow = 0;
  if( pzErrMsg ) *pzErrMsg = 0;
  res.zErrMsg = 0;
  res.nRow = 0;
  res.nColumn = 0;
  res.nData = 1;
  res.nAlloc = 20;
  res.rc = SQLITE_OK;
  res.azResult = sqlitex_malloc(sizeof(char*)*res.nAlloc );
  if( res.azResult==0 ){
     db->errCode = SQLITE_NOMEM;
     return SQLITE_NOMEM;
  }
  res.azResult[0] = 0;
  rc = sqlitex_exec(db, zSql, sqlitex_get_table_cb, &res, pzErrMsg);
  assert( sizeof(res.azResult[0])>= sizeof(res.nData) );
  res.azResult[0] = SQLITE_INT_TO_PTR(res.nData);
  if( (rc&0xff)==SQLITE_ABORT ){
    sqlitex_free_table(&res.azResult[1]);
    if( res.zErrMsg ){
      if( pzErrMsg ){
        sqlitex_free(*pzErrMsg);
        *pzErrMsg = sqlitex_mprintf("%s",res.zErrMsg);
      }
      sqlitex_free(res.zErrMsg);
    }
    db->errCode = res.rc;  /* Assume 32-bit assignment is atomic */
    return res.rc;
  }
  sqlitex_free(res.zErrMsg);
  if( rc!=SQLITE_OK ){
    sqlitex_free_table(&res.azResult[1]);
    return rc;
  }
  if( res.nAlloc>res.nData ){
    char **azNew;
    azNew = sqlitex_realloc( res.azResult, sizeof(char*)*res.nData );
    if( azNew==0 ){
      sqlitex_free_table(&res.azResult[1]);
      db->errCode = SQLITE_NOMEM;
      return SQLITE_NOMEM;
    }
    res.azResult = azNew;
  }
  *pazResult = &res.azResult[1];
  if( pnColumn ) *pnColumn = res.nColumn;
  if( pnRow ) *pnRow = res.nRow;
  return rc;
}

/*
** This routine frees the space the sqlitex_get_table() malloced.
*/
void sqlitex_free_table(
  char **azResult            /* Result returned from sqlitex_get_table() */
){
  if( azResult ){
    int i, n;
    azResult--;
    assert( azResult!=0 );
    n = SQLITE_PTR_TO_INT(azResult[0]);
    for(i=1; i<n; i++){ if( azResult[i] ) sqlitex_free(azResult[i]); }
    sqlitex_free(azResult);
  }
}

#endif /* SQLITE_OMIT_GET_TABLE */
