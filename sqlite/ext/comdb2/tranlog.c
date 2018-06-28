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


#include "sqlite3ext.h"
#include "tranlog.h"
#include <assert.h>
#include <string.h>
#include "comdb2.h"
#include "build/db.h"
#include "dbinc/db_swap.h"
#include "dbinc_auto/txn_auto.h"

/* Column numbers */
#define TRANLOG_COLUMN_START        0
#define TRANLOG_COLUMN_STOP         1
#define TRANLOG_COLUMN_FLAGS        2
#define TRANLOG_COLUMN_LSN          3
#define TRANLOG_COLUMN_RECTYPE      4
#define TRANLOG_COLUMN_GENERATION   5
#define TRANLOG_COLUMN_TIMESTAMP    6
#define TRANLOG_COLUMN_LOG          7


/* Modeled after generate_series */
typedef struct tranlog_cursor tranlog_cursor;
struct tranlog_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
  sqlite3_int64 idx;
  DB_LSN curLsn;             /* Current LSN */
  DB_LSN minLsn;             /* Minimum LSN */
  DB_LSN maxLsn;             /* Maximum LSN */
  char *minLsnStr;
  char *maxLsnStr;
  char *curLsnStr;
  int flags;           /* 1 if we should block */
  int hitLast;
  int notDurable;
  int openCursor;
  DB_LOGC *logc;             /* Log Cursor */
  DBT data;
};

static int tranlogConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  sqlite3_vtab *pNew;
  int rc;
  rc = sqlite3_declare_vtab(db,
     "CREATE TABLE x(minlsn hidden,maxlsn hidden, flags hidden,lsn,rectype,generation,timestamp,payload)");
  if( rc==SQLITE_OK ){
    pNew = *ppVtab = sqlite3_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }
  return rc;
}

static int tranlogDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int tranlogOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  tranlog_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

static int tranlogClose(sqlite3_vtab_cursor *cur){
  tranlog_cursor *pCur = (tranlog_cursor*)cur;
  if (pCur->openCursor) {
      assert(pCur->logc);
      pCur->logc->close(pCur->logc, 0);
      pCur->openCursor = 0;
  }
  if (pCur->data.data)
      free(pCur->data.data);
  if (pCur->minLsnStr)
      sqlite3_free(pCur->minLsnStr);
  if (pCur->maxLsnStr)
      sqlite3_free(pCur->maxLsnStr);
  if (pCur->curLsnStr)
      sqlite3_free(pCur->curLsnStr);
  sqlite3_free(pCur);
  return SQLITE_OK;
}
#include <bdb/bdb_int.h>

extern pthread_mutex_t gbl_logput_lk;
extern pthread_cond_t gbl_logput_cond;
extern pthread_mutex_t gbl_durable_lsn_lk;
extern pthread_cond_t gbl_durable_lsn_cond;

/*
** Advance a tranlog cursor to the next log entry
*/
static int tranlogNext(sqlite3_vtab_cursor *cur){
  struct sql_thread *thd = NULL;
  tranlog_cursor *pCur = (tranlog_cursor*)cur;
  DB_LSN durable_lsn = {0};
  int durable_gen=0, rc, getflags;
  bdb_state_type *bdb_state = thedb->bdb_env;

  if (pCur->notDurable || pCur->hitLast)
      return SQLITE_OK;

  if (!pCur->openCursor) {
      if (rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &pCur->logc, 0)
              != 0) {
          logmsg(LOGMSG_ERROR, "%s line %d error getting a log cursor rc=%d\n",
                  __func__, __LINE__, rc);
          return SQLITE_INTERNAL;
      }
      pCur->openCursor = 1;
      pCur->data.flags = DB_DBT_REALLOC;

      if (pCur->minLsn.file == 0) {
          getflags = DB_FIRST;
      } else {
          pCur->curLsn = pCur->minLsn;
          getflags = DB_SET;
      }
  } else {
      getflags = DB_NEXT;
  }

  /* Special case for durable first requests: we need to know the lsn */
  if ((pCur->flags & TRANLOG_FLAGS_DURABLE) && getflags == DB_FIRST) {
      if (pCur->logc->get(
          pCur->logc, &pCur->curLsn, &pCur->data, getflags) != 0) {
          return SQLITE_INTERNAL;
      }
  }

  /* Don't advance cursor until this is durable */
  if (pCur->flags & TRANLOG_FLAGS_DURABLE) {
      do {
          char *master;
          struct timespec ts;
          bdb_state->dbenv->get_durable_lsn(bdb_state->dbenv,
                  &durable_lsn, &durable_gen);

          /* We've already returned this lsn: break when the next is durable */
          if (log_compare(&durable_lsn, &pCur->curLsn) >= 0)
              break;

          /* Return immediately if we are non-blocking */
          else if (!bdb_amimaster(bdb_state) ||
                  (pCur->flags & TRANLOG_FLAGS_BLOCK) == 0) {
              pCur->notDurable = 1;
              break;
          }

          /* We want to downgrade */
          if (bdb_the_lock_desired()) {
              pCur->notDurable = 1;
              break;
          }

          /* Wait on a condition variable */
          clock_gettime(CLOCK_REALTIME, &ts);
          ts.tv_nsec += (200 * 1000000);
          pthread_mutex_lock(&gbl_durable_lsn_lk);
          pthread_cond_timedwait(&gbl_durable_lsn_cond, &gbl_durable_lsn_lk, &ts);
          pthread_mutex_unlock(&gbl_durable_lsn_lk);

      } while (1);
  }

  if (rc = pCur->logc->get(pCur->logc, &pCur->curLsn, &pCur->data, getflags) != 0) {
      if (getflags != DB_NEXT) {
          return SQLITE_INTERNAL;
      }
      if (pCur->flags & TRANLOG_FLAGS_BLOCK) {
          do {
              struct timespec ts;
              clock_gettime(CLOCK_REALTIME, &ts);
              ts.tv_nsec += (200 * 1000000);
              pthread_mutex_lock(&gbl_logput_lk);
              pthread_cond_timedwait(&gbl_logput_cond, &gbl_logput_lk, &ts);
              pthread_mutex_unlock(&gbl_logput_lk);

              int sleepms = 100;
              while (bdb_the_lock_desired()) {
                  if (thd == NULL) {
                      pthread_getspecific(query_info_key);
                  }
                  recover_deadlock(thedb->bdb_env, thd, NULL, sleepms);
                  sleepms*=2;
                  if (sleepms > 10000)
                      sleepms = 10000;
              }
          } while (rc = pCur->logc->get(pCur->logc, &pCur->curLsn, &pCur->data, DB_NEXT));
          rc = pCur->logc->get(pCur->logc, &pCur->curLsn,
                  &pCur->data, DB_NEXT) != 0;
      } else {
          pCur->hitLast = 1;
      }
  }

  pCur->iRowid++;
  return SQLITE_OK;
}

#define skipws(p) { while (*p != '\0' && *p == ' ') p++; }
#define isnum(p) ( *p >= '0' && *p <= '9' )

static inline void tranlog_lsn_to_str(char *st, DB_LSN *lsn)
{
    sprintf(st, "{%d:%d}", lsn->file, lsn->offset);
}

static inline int parse_lsn(const char *lsnstr, DB_LSN *lsn)
{
    const char *p = lsnstr;
    int file, offset;
    while (*p != '\0' && *p == ' ') p++;
    skipws(p);

    /* Parse opening '{' */
    if (*p != '{')
        return -1;
    p++;
    skipws(p);
    if ( !isnum(p) )
        return -1;

    /* Parse file */
    file = atoi(p);
    while( isnum(p) )
        p++;
    skipws(p);
    if ( *p != ':' )
        return -1;
    p++;
    skipws(p);
    if ( !isnum(p) )
        return -1;

    /* Parse offset */
    offset = atoi(p);
    while( isnum(p) )
        p++;

    skipws(p);

    /* Parse closing '}' */
    if (*p != '}')
        return -1;
    p++;

    skipws(p);
    if (*p != '\0')
        return -1;

    lsn->file = file;
    lsn->offset = offset;
    return 0;
}

static u_int64_t get_timestamp_from_regop_gen_record(char *data)
{
    u_int64_t timestamp;
    LOGCOPY_64( &timestamp, &data[ 4 + 4 + 8 + 4 + 4 + 8] );
    return timestamp;
}

static u_int32_t get_generation_from_regop_gen_record(char *data)
{
    u_int32_t generation;
    LOGCOPY_32( &generation, &data[ 4 + 4 + 8 + 4] );
    return generation;
}

static u_int64_t get_timestamp_from_regop_rowlocks_record(char *data)
{
    u_int64_t timestamp;
    LOGCOPY_64( &timestamp, &data[4 + 4 + 8 + 4 + 8 + 8 + 8 + 8] );
    return timestamp;
}

static u_int32_t get_generation_from_regop_rowlocks_record(char *data)
{
    u_int32_t generation;
    LOGCOPY_32( &generation, &data[4 + 4 + 8 + 4 + 8 + 8 + 8 + 8 + 8 + 4] );
    return generation;
}

static u_int32_t get_timestamp_from_regop_record(char *data)
{
    u_int32_t timestamp;
    LOGCOPY_32( &timestamp, &data[4 + 4 + 8 + 4] );
    return timestamp;
}

static u_int32_t get_timestamp_from_ckp_record(char *data)
{
    u_int32_t timestamp;
    LOGCOPY_32( &timestamp, &data[4 + 4 + 8 + 8 + 8] );
    return timestamp;
}

static u_int32_t get_generation_from_ckp_record(char *data)
{
    u_int32_t generation;
    LOGCOPY_32( &generation, &data[4 + 4 + 8 + 8 + 8 + 4] );
    return generation;
}

/*
** Return values of columns for the row at which the series_cursor
** is currently pointing.
*/
static int tranlogColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
)
{
  tranlog_cursor *pCur = (tranlog_cursor*)cur;
  int rc;
  u_int32_t rectype = 0;
  u_int32_t generation = 0;
  int64_t timestamp = 0;

  switch( i ){
    case TRANLOG_COLUMN_START:
        if (!pCur->minLsnStr) {
            pCur->minLsnStr = sqlite3_malloc(32);
            tranlog_lsn_to_str(pCur->minLsnStr, &pCur->minLsn);
        }
        sqlite3_result_text(ctx, pCur->minLsnStr, -1, NULL);
        break;

    case TRANLOG_COLUMN_STOP:
        if (!pCur->maxLsnStr) {
            pCur->maxLsnStr = sqlite3_malloc(32);
            tranlog_lsn_to_str(pCur->maxLsnStr, &pCur->maxLsn);
        }
        sqlite3_result_text(ctx, pCur->maxLsnStr, -1, NULL);
        break;

    case TRANLOG_COLUMN_FLAGS:
        sqlite3_result_int64(ctx, pCur->flags);
        break;
    case TRANLOG_COLUMN_LSN:
        if (!pCur->curLsnStr) {
            pCur->curLsnStr = sqlite3_malloc(32);
        }
        tranlog_lsn_to_str(pCur->curLsnStr, &pCur->curLsn);
        sqlite3_result_text(ctx, pCur->curLsnStr, -1, NULL);
        break;
    case TRANLOG_COLUMN_RECTYPE:
        if (pCur->data.data)
            LOGCOPY_32(&rectype, pCur->data.data); 
        sqlite3_result_int64(ctx, rectype);
        break;
    case TRANLOG_COLUMN_GENERATION:
        if (pCur->data.data)
            LOGCOPY_32(&rectype, pCur->data.data); 

        if (rectype == DB___txn_regop_gen){
            generation = get_generation_from_regop_gen_record(pCur->data.data);
        }

        if (rectype == DB___txn_regop_rowlocks) {
            generation = get_generation_from_regop_rowlocks_record(pCur->data.data);
        }

        if (rectype == DB___txn_ckp) {
            generation = get_generation_from_ckp_record(pCur->data.data);
        }

        if (generation > 0) {
            sqlite3_result_int64(ctx, generation);
        } else {
            sqlite3_result_null(ctx);
        }
        break;

    case TRANLOG_COLUMN_TIMESTAMP:
        if (pCur->data.data)
            LOGCOPY_32(&rectype, pCur->data.data); 

        if (rectype == DB___txn_regop_gen){
            timestamp = get_timestamp_from_regop_gen_record(pCur->data.data);
        }

        if (rectype == DB___txn_regop_rowlocks) {
            timestamp = get_timestamp_from_regop_rowlocks_record(pCur->data.data);
        }

        if (rectype == DB___txn_regop) {
            timestamp = get_timestamp_from_regop_record(pCur->data.data);
        }

        if (rectype == DB___txn_ckp) {
            timestamp = get_timestamp_from_ckp_record(pCur->data.data);
        }

        if (timestamp > 0) {
            sqlite3_result_int64(ctx, timestamp);
        } else {
            sqlite3_result_null(ctx);
        }
        break;
    case TRANLOG_COLUMN_LOG:
        sqlite3_result_blob(ctx, &pCur->data.data, pCur->data.size, NULL);
        break;
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int tranlogRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  tranlog_cursor *pCur = (tranlog_cursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int tranlogEof(sqlite3_vtab_cursor *cur){
  tranlog_cursor *pCur = (tranlog_cursor*)cur;
  int rc;

  /* If we are not positioned, position now */
  if (pCur->openCursor == 0) {
      if ((rc=tranlogNext(cur)) != SQLITE_OK)
          return rc;
  }
  if (pCur->hitLast || pCur->notDurable)
      return 1;
  if (pCur->maxLsn.file > 0 && log_compare(&pCur->curLsn, &pCur->maxLsn) > 0)
      return 1;
  return 0;
}

static int tranlogFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  tranlog_cursor *pCur = (tranlog_cursor *)pVtabCursor;
  int i = 0;

  bzero(&pCur->minLsn, sizeof(pCur->minLsn));
  if( idxNum & 1 ){
    const char *minLsn = sqlite3_value_text(argv[i++]);
    if (minLsn && parse_lsn(minLsn, &pCur->minLsn)) {
        return SQLITE_CONV_ERROR;
    }
  }
  bzero(&pCur->maxLsn, sizeof(pCur->maxLsn));
  if( idxNum & 2 ){
    const char *maxLsn = sqlite3_value_text(argv[i++]);
    if (maxLsn && parse_lsn(maxLsn, &pCur->maxLsn)) {
        return SQLITE_CONV_ERROR;
    }
  }
  pCur->flags = 0;
  if( idxNum & 4 ){
    int64_t flags = sqlite3_value_int64(argv[i++]);
    pCur->flags = flags;
  }
  pCur->iRowid = 1;
  return SQLITE_OK;
}

static int tranlogBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;                 /* Loop over constraints */
  int idxNum = 0;        /* The query plan bitmask */
  int startIdx = -1;     /* Index of the start= constraint, or -1 if none */
  int stopIdx = -1;      /* Index of the stop= constraint, or -1 if none */
  int flagsIdx = -1;     /* Index of the block= constraint, block waiting if set */
  int nArg = 0;          /* Number of arguments that seriesFilter() expects */

  const struct sqlite3_index_constraint *pConstraint;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( pConstraint->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pConstraint->iColumn ){
      case TRANLOG_COLUMN_START:
        startIdx = i;
        idxNum |= 1;
        break;
      case TRANLOG_COLUMN_STOP:
        stopIdx = i;
        idxNum |= 2;
        break;
      case TRANLOG_COLUMN_FLAGS:
        flagsIdx = i;
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
  if( flagsIdx>=0 ){
    pIdxInfo->aConstraintUsage[flagsIdx].argvIndex = ++nArg;
    pIdxInfo->aConstraintUsage[flagsIdx].omit = 1;
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
sqlite3_module systblTransactionLogsModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  tranlogConnect,            /* xConnect */
  tranlogBestIndex,          /* xBestIndex */
  tranlogDisconnect,         /* xDisconnect */
  0,                         /* xDestroy */
  tranlogOpen,               /* xOpen - open a cursor */
  tranlogClose,              /* xClose - close a cursor */
  tranlogFilter,             /* xFilter - configure scan constraints */
  tranlogNext,               /* xNext - advance a cursor */
  tranlogEof,                /* xEof - check for end of scan */
  tranlogColumn,             /* xColumn - read data */
  tranlogRowid,              /* xRowid - read data */
  0,                         /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindMethod */
  0,                         /* xRename */
};


