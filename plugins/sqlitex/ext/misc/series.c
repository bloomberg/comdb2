/*
** 2015-08-18
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file demonstrates how to create a table-valued-function using
** a virtual table.  This demo implements the generate_series() function
** which gives similar results to the eponymous function in PostgreSQL.
** Examples:
**
**      SELECT * FROM generate_series(0,100,5);
**
** The query above returns integers from 0 through 100 counting by steps
** of 5.
**
**      SELECT * FROM generate_series(0,100);
**
** Integers from 0 through 100 with a step size of 1.
**
**      SELECT * FROM generate_series(20) LIMIT 10;
**
** Integers 20 through 29.
**
** HOW IT WORKS
**
** The generate_series "function" is really a virtual table with the
** following schema:
**
**     CREATE FUNCTION generate_series(
**       value,
**       start HIDDEN,
**       stop HIDDEN,
**       step HIDDEN
**     );
**
** Function arguments in queries against this virtual table are translated
** into equality constraints against successive hidden columns.  In other
** words, the following pairs of queries are equivalent to each other:
**
**    SELECT * FROM generate_series(0,100,5);
**    SELECT * FROM generate_series WHERE start=0 AND stop=100 AND step=5;
**
**    SELECT * FROM generate_series(0,100);
**    SELECT * FROM generate_series WHERE start=0 AND stop=100;
**
**    SELECT * FROM generate_series(20) LIMIT 10;
**    SELECT * FROM generate_series WHERE start=20 LIMIT 10;
**
** The generate_series virtual table implementation leaves the xCreate method
** set to NULL.  This means that it is not possible to do a CREATE VIRTUAL
** TABLE command with "generate_series" as the USING argument.  Instead, there
** is a single generate_series virtual table that is always available without
** having to be created first.
**
** The xBestIndex method looks for equality constraints against the hidden
** start, stop, and step columns, and if present, it uses those constraints
** to bound the sequence of generated values.  If the equality constraints
** are missing, it uses 0 for start, 4294967295 for stop, and 1 for step.
** xBestIndex returns a small cost when both start and stop are available,
** and a very large cost if either start or stop are unavailable.  This
** encourages the query planner to order joins such that the bounds of the
** series are well-defined.
*/
#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
# define SQLITE_CORE 1
#endif

#include "sqlitexext.h"
#ifndef SQLITE_BUILDING_FOR_COMDB2
SQLITE_EXTENSION_INIT1
#endif 

#include <assert.h>
#include <string.h>

#ifndef SQLITE_OMIT_VIRTUALTABLE



/* series_cursor is a subclass of sqlitex_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct series_cursor series_cursor;
struct series_cursor {
  sqlitex_vtab_cursor base;  /* Base class - must be first */
  int isDesc;                /* True to count down rather than up */
  sqlitex_int64 iRowid;      /* The rowid */
  sqlitex_int64 iValue;      /* Current value ("value") */
  sqlitex_int64 mnValue;     /* Mimimum value ("start") */
  sqlitex_int64 mxValue;     /* Maximum value ("stop") */
  sqlitex_int64 iStep;       /* Increment ("step") */
};

/*
** The seriesConnect() method is invoked to create a new
** series_vtab that describes the generate_series virtual table.
**
** Think of this routine as the constructor for series_vtab objects.
**
** All this routine needs to do is:
**
**    (1) Allocate the series_vtab object and initialize all fields.
**
**    (2) Tell SQLite (via the sqlitex_declare_vtab() interface) what the
**        result set of queries against generate_series will look like.
*/
static int seriesConnect(
  sqlitex *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlitex_vtab **ppVtab,
  char **pzErr
){
  sqlitex_vtab *pNew;
  int rc;

/* Column numbers */
#define SERIES_COLUMN_VALUE 0
#define SERIES_COLUMN_START 1
#define SERIES_COLUMN_STOP  2
#define SERIES_COLUMN_STEP  3

  rc = sqlitex_declare_vtab(db,
     "CREATE TABLE x(value,start hidden,stop hidden,step hidden)");
  if( rc==SQLITE_OK ){
    pNew = *ppVtab = sqlitex_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }
  return rc;
}

/*
** This method is the destructor for series_cursor objects.
*/
static int seriesDisconnect(sqlitex_vtab *pVtab){
  sqlitex_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for a new series_cursor object.
*/
static int seriesOpen(sqlitex_vtab *p, sqlitex_vtab_cursor **ppCursor){
  series_cursor *pCur;
  pCur = sqlitex_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a series_cursor.
*/
static int seriesClose(sqlitex_vtab_cursor *cur){
  sqlitex_free(cur);
  return SQLITE_OK;
}


/*
** Advance a series_cursor to its next row of output.
*/
static int seriesNext(sqlitex_vtab_cursor *cur){
  series_cursor *pCur = (series_cursor*)cur;
  if( pCur->isDesc ){
    pCur->iValue -= pCur->iStep;
  }else{
    pCur->iValue += pCur->iStep;
  }
  pCur->iRowid++;
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the series_cursor
** is currently pointing.
*/
static int seriesColumn(
  sqlitex_vtab_cursor *cur,   /* The cursor */
  sqlitex_context *ctx,       /* First argument to sqlitex_result_...() */
  int i                       /* Which column to return */
){
  series_cursor *pCur = (series_cursor*)cur;
  sqlitex_int64 x = 0;
  switch( i ){
    case SERIES_COLUMN_START:  x = pCur->mnValue; break;
    case SERIES_COLUMN_STOP:   x = pCur->mxValue; break;
    case SERIES_COLUMN_STEP:   x = pCur->iStep;   break;
    default:                   x = pCur->iValue;  break;
  }
  sqlitex_result_int64(ctx, x);
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int seriesRowid(sqlitex_vtab_cursor *cur, sqlite_int64 *pRowid){
  series_cursor *pCur = (series_cursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int seriesEof(sqlitex_vtab_cursor *cur){
  series_cursor *pCur = (series_cursor*)cur;
  if( pCur->isDesc ){
    return pCur->iValue < pCur->mnValue;
  }else{
    return pCur->iValue > pCur->mxValue;
  }
}

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to seriesColumn() or seriesRowid() or
** seriesEof().
**
** The query plan selected by seriesBestIndex is passed in the idxNum
** parameter.  (idxStr is not used in this implementation.)  idxNum
** is a bitmask showing which constraints are available:
**
**    1:    start=VALUE
**    2:    stop=VALUE
**    4:    step=VALUE
**
** Also, if bit 8 is set, that means that the series should be output
** in descending order rather than in ascending order.
**
** This routine should initialize the cursor and position it so that it
** is pointing at the first row, or pointing off the end of the table
** (so that seriesEof() will return true) if the table is empty.
*/
static int seriesFilter(
  sqlitex_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlitex_value **argv
){
  series_cursor *pCur = (series_cursor *)pVtabCursor;
  int i = 0;
  if( idxNum & 1 ){
    pCur->mnValue = sqlitex_value_int64(argv[i++]);
  }else{
    pCur->mnValue = 0;
  }
  if( idxNum & 2 ){
    pCur->mxValue = sqlitex_value_int64(argv[i++]);
  }else{
    pCur->mxValue = 0xffffffff;
  }
  if( idxNum & 4 ){
    pCur->iStep = sqlitex_value_int64(argv[i++]);
    if( pCur->iStep<1 ) pCur->iStep = 1;
  }else{
    pCur->iStep = 1;
  }
  if( idxNum & 8 ){
    pCur->isDesc = 1;
    pCur->iValue = pCur->mxValue;
    if( pCur->iStep>0 ){
      pCur->iValue -= (pCur->mxValue - pCur->mnValue)%pCur->iStep;
    }
  }else{
    pCur->isDesc = 0;
    pCur->iValue = pCur->mnValue;
  }
  pCur->iRowid = 1;
  return SQLITE_OK;
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the generate_series virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
**
** In this implementation idxNum is used to represent the
** query plan.  idxStr is unused.
**
** The query plan is represented by bits in idxNum:
**
**  (1)  start = $value  -- constraint exists
**  (2)  stop = $value   -- constraint exists
**  (4)  step = $value   -- constraint exists
**  (8)  output in descending order
*/
static int seriesBestIndex(
  sqlitex_vtab *tab,
  sqlitex_index_info *pIdxInfo
){
  int i;                 /* Loop over constraints */
  int idxNum = 0;        /* The query plan bitmask */
  int startIdx = -1;     /* Index of the start= constraint, or -1 if none */
  int stopIdx = -1;      /* Index of the stop= constraint, or -1 if none */
  int stepIdx = -1;      /* Index of the step= constraint, or -1 if none */
  int nArg = 0;          /* Number of arguments that seriesFilter() expects */

  const struct sqlitex_index_constraint *pConstraint;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( pConstraint->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pConstraint->iColumn ){
      case SERIES_COLUMN_START:
        startIdx = i;
        idxNum |= 1;
        break;
      case SERIES_COLUMN_STOP:
        stopIdx = i;
        idxNum |= 2;
        break;
      case SERIES_COLUMN_STEP:
        stepIdx = i;
        idxNum |= 4;
        break;
    }
  }
  if( startIdx>=0 ){
    pIdxInfo->aConstraintUsage[startIdx].argvIndex = ++nArg;
    pIdxInfo->aConstraintUsage[startIdx].omit = 1;
  }
  if( stopIdx>=0 ){
    pIdxInfo->aConstraintUsage[stopIdx].argvIndex = ++nArg;
    pIdxInfo->aConstraintUsage[stopIdx].omit = 1;
  }
  if( stepIdx>=0 ){
    pIdxInfo->aConstraintUsage[stepIdx].argvIndex = ++nArg;
    pIdxInfo->aConstraintUsage[stepIdx].omit = 1;
  }
  if( pIdxInfo->nOrderBy==1 ){
    if( pIdxInfo->aOrderBy[0].desc ) idxNum |= 8;
    pIdxInfo->orderByConsumed = 1;
  }
  if( (idxNum & 3)==3 ){
    /* Both start= and stop= boundaries are available.  This is the 
    ** the preferred case */
    pIdxInfo->estimatedCost = (double)1;
  }else{
    /* If either boundary is missing, we have to generate a huge span
    ** of numbers.  Make this case very expensive so that the query
    ** planner will work hard to avoid it. */
    pIdxInfo->estimatedCost = (double)2000000000;
  }
  pIdxInfo->idxNum = idxNum;
  return SQLITE_OK;
}

/*
** This following structure defines all the methods for the 
** generate_series virtual table.
*/
static sqlitex_module seriesModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  seriesConnect,             /* xConnect */
  seriesBestIndex,           /* xBestIndex */
  seriesDisconnect,          /* xDisconnect */
  0,                         /* xDestroy */
  seriesOpen,                /* xOpen - open a cursor */
  seriesClose,               /* xClose - close a cursor */
  seriesFilter,              /* xFilter - configure scan constraints */
  seriesNext,                /* xNext - advance a cursor */
  seriesEof,                 /* xEof - check for end of scan */
  seriesColumn,              /* xColumn - read data */
  seriesRowid,               /* xRowid - read data */
  0,                         /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindMethod */
  0,                         /* xRename */
};

#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlitex_series_init(
  sqlitex *db, 
  char **pzErrMsg, 
  const sqlitex_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
#ifndef SQLITE_OMIT_VIRTUALTABLE
  if( sqlitex_libversion_number()<3008012 ){
    *pzErrMsg = sqlitex_mprintf(
        "generate_series() requires SQLite 3.8.12 or later");
    return SQLITE_ERROR;
  }
  rc = sqlitex_create_module(db, "generate_series", &seriesModule, 0);
#endif
  return rc;
}

/* Comdb2 does not allow run-time linking for sqlitex modules, so
** We must link this staticaly as we do with system tables.
*/
#ifdef SQLITE_BUILDING_FOR_COMDB2
# ifdef SQLITE_ENABLE_SERIES
int sqlitexSeriesInit(
  sqlitex *db
){
  int rc = SQLITE_OK;
  rc = sqlitex_create_module(db, "generate_series", &seriesModule, 0);
  return rc;
}
# endif
#endif
  
