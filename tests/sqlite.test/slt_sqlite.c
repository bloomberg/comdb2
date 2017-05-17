/*
** Copyright (c) 2008 D. Richard Hipp
**
** This program is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public
** License version 2 as published by the Free Software Foundation.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** General Public License for more details.
** 
** You should have received a copy of the GNU General Public
** License along with this library; if not, write to the
** Free Software Foundation, Inc., 59 Temple Place - Suite 330,
** Boston, MA  02111-1307, USA.
**
** Author contact information:
**   drh@hwaci.com
**   http://www.hwaci.com/drh/
**
*******************************************************************************
** Here begins the implementation of the SQLite DbEngine object.
**
** Use this interface as a model for other database engine interfaces.
*/
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>

#include "sqlite3.h"
#include "sqllogictest.h"

extern char *dbname;

/*
** This routine is called to open a connection to a new, empty database.
** The zConnectStr argument is the value of the -connection command-line
** option.  This is intended to contain information on how to connect to
** the database engine.  The zConnectStr argument will be NULL if there
** is no -connection on the command-line.  In the case of SQLite, the
** zConnectStr is the name of the database file to open.
**
** An object that describes the newly opened and initialized database
** connection is returned by writing into *ppConn.
**
** This routine returns 0 on success and non-zero if there are any errors.
*/
static int sqliteConnect(
  void *NotUsed,              /* Argument from DbEngine object.  Not used */
  const char *zConnectStr,    /* Connection string */
  void **ppConn               /* Write completed connection here */
){
  sqlite3 *db;
  int rc;

  /* If the database filename is defined and the database already exists,
  ** then delete the database before we start, thus resetting it to an
  ** empty database.
  */
  zConnectStr = "test.db";

  if (dbname)
      zConnectStr = dbname;

  if( zConnectStr ){
#ifndef WIN32
    unlink(zConnectStr);
#else
    _unlink(zConnectStr);
#endif
  }

  /* Open a connection to the new database.
  */
  rc = sqlite3_open(zConnectStr, &db);
  if( rc!=SQLITE_OK ){
    return 1;
  }
  sqlite3_exec(db, "PRAGMA synchronous=OFF", 0, 0, 0);
  *ppConn = (void*)db;
  return 0;  
}

/*
** Evaluate the single SQL statement given in zSql.  Return 0 on success.
** return non-zero if any error occurs.
*/
static int sqliteStatement(
  void *pConn,                /* Connection created by xConnect */
  const char *zSql            /* SQL statement to evaluate */
){
  int rc;
  sqlite3 *db;

  db = (sqlite3*)pConn;
  rc = sqlite3_exec(db, zSql, 0, 0, 0);
  return rc!=SQLITE_OK;
}

/*
** Structure used to accumulate a result set.
*/
typedef struct ResAccum ResAccum;
struct ResAccum {
  char **azValue;   /* Array of pointers to values, each malloced separately */
  int nAlloc;       /* Number of slots allocated in azValue */
  int nUsed;        /* Number of slots in azValue used */
};

/*
** Append a value to a result set.  zValue is copied into memory obtained
** from malloc.  Or if zValue is NULL, then a NULL pointer is appended.
*/
static void appendValue(ResAccum *p, const char *zValue){
  char *z;
  if( zValue ){
    z = sqlite3_mprintf("%s", zValue);
    if( z==0 ){
      fprintf(stderr, "out of memory at %s:%d\n", __FILE__,__LINE__);
      exit(1);
    }
  }else{
    z = 0;
  }
  if( p->nUsed>=p->nAlloc ){
    char **az;
    p->nAlloc += 200;
    az = realloc(p->azValue, p->nAlloc*sizeof(p->azValue[0]));
    if( az==0 ){
      fprintf(stderr, "out of memory at %s:%d\n", __FILE__,__LINE__);
      exit(1);
    }
    p->azValue = az;
  }
  p->azValue[p->nUsed++] = z;
}

/*
** This interface runs a query and accumulates the results into an array
** of pointers to strings.  *pazResult is made to point to the resulting
** array and *pnResult is set to the number of elements in the array.
**
** NULL values in the result set should be represented by a string "NULL".
** Empty strings should be shown as "(empty)".  Unprintable and
** control characters should be rendered as "@".
**
** Return 0 on success and 1 if there is an error.  It is not necessary
** to initialize *pazResult or *pnResult if an error occurs.
*/
static int sqliteQuery(
  void *pConn,                /* Connection created by xConnect */
  const char *zSql,           /* SQL statement to evaluate */
  const char *zType,          /* One character for each column of result */
  char ***pazResult,          /* RETURN:  Array of result values */
  int *pnResult               /* RETURN:  Number of result values */
){
  sqlite3 *db;                /* The database connection */
  sqlite3_stmt *pStmt;        /* Prepared statement */
  int rc;                     /* Result code from subroutine calls */
  ResAccum res;               /* query result accumulator */
  char zBuffer[200];          /* Buffer to render numbers */

  memset(&res, 0, sizeof(res));
  db = (sqlite3*)pConn;
  rc = sqlite3_prepare(db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    sqlite3_finalize(pStmt);
    return 1;
  }
  if( strlen(zType)!=sqlite3_column_count(pStmt) ){
    fprintf(stderr, "wrong number of result columns: expected %d but got %d\n",
            (int)strlen(zType), sqlite3_column_count(pStmt));
    return 1;
  }
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    int i;
    for(i=0; zType[i]; i++){
      if( sqlite3_column_type(pStmt, i)==SQLITE_NULL ){
        appendValue(&res, "NULL");
      }else{
        switch( zType[i] ){
          case 'T': {
            const char *zValue = (const char*)sqlite3_column_text(pStmt, i);
            char *z;
            if( zValue[0]==0 ) zValue = "(empty)";
            appendValue(&res, zValue);

            /* Convert non-printing and control characters to '@' */
            z = res.azValue[res.nUsed-1];
            while( *z ){
              if( *z<' ' || *z>'~' ){ *z = '@'; }
              z++;
            }
            break;
          }
          case 'I': {
            int ii = sqlite3_column_int(pStmt, i);
            sqlite3_snprintf(sizeof(zBuffer), zBuffer, "%d", ii);
            appendValue(&res, zBuffer);
            break;
          }
          case 'R': {
            double r = sqlite3_column_double(pStmt, i);
            sqlite3_snprintf(sizeof(zBuffer), zBuffer, "%.3f", r);
            appendValue(&res, zBuffer);
            break;
          }
          default: {
            sqlite3_finalize(pStmt);
            fprintf(stderr, "unknown character in type-string: %c\n", zType[i]);
            return 1;
          }
        }
      }
    }
  }
  sqlite3_finalize(pStmt);
  *pazResult = res.azValue;
  *pnResult = res.nUsed;
  return 0;
}

/*
** This interface is called to free the memory that was returned
** by xQuery.
**
** It might be the case that nResult==0 or azResult==0.
*/
static int sqliteFreeResults(
  void *pConn,                /* Connection created by xConnect */
  char **azResult,            /* The results to be freed */
  int nResult                 /* Number of rows of result */
){
  int i;
  for(i=0; i<nResult; i++){
    sqlite3_free(azResult[i]);
  }
  sqlite3_free((char *)azResult);
  return 0;
}

/*
** This routine is called to close a connection previously opened
** by xConnect.
**
** This routine may or may not delete the database.  Whichever way
** it works, steps should be taken to avoid an accumulation of left-over
** database files.  If the database is deleted here, that is one approach.
** The other approach is to delete left-over databases in the xConnect
** method.  The SQLite interface takes the latter approach.
*/
static int sqliteDisconnect(
  void *pConn                 /* Connection created by xConnect */
){
  sqlite3 *db = (sqlite3*)pConn;
  sqlite3_close(db);
  return 0;
}

/*
** This routine is called to return the name of the DB engine
** used by the connection pConn.  This name may or may not
** be the same as specified in the DbEngine structure.
**
** Then returned DB name does not have to be freed by the called.
**
** This routine should be called only after a valid connection
** has been establihed with xConnect.
**
** For ODBC connections, the engine name is resolved by the 
** driver manager after a connection is made.
*/
static int sqliteGetEngineName(
  void *pConn,                /* Connection created by xConnect */
  const char **zName          /* SQL statement to evaluate */
){
  static char *zDmbsName = "SQLite";
  *zName = zDmbsName;
  return 0;
}

/*
** This routine registers the SQLite database engine with the main
** driver.  New database engine interfaces should have a single
** routine similar to this one.  The main() function below should be
** modified to call that routine upon startup.
*/
void registerSqlite(void){
  /*
  ** This is the object that defines the database engine interface.
  */
  static const DbEngine sqliteDbEngine = {
     "SQLite",             /* zName */
     0,                    /* pAuxData */
     sqliteConnect,        /* xConnect */
     sqliteGetEngineName,  /* xGetEngineName */
     sqliteStatement,      /* xStatement */
     sqliteQuery,          /* xQuery */
     sqliteFreeResults,    /* xFreeResults */
     sqliteDisconnect      /* xDisconnect */
  };
  sqllogictestRegisterEngine(&sqliteDbEngine);
}

/*
**************** End of the SQLite database engine interface *****************
*****************************************************************************/
