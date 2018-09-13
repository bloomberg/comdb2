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

/* systbl_queues_cursor is a subclass of sqlite3_vtab_cursor which
** serves as the underlying cursor to enumerate the rows in this
** vtable. As of now the only thing this cursor reports is the size of
** the table.
*/
typedef struct systbl_queues_cursor systbl_queues_cursor;
struct systbl_queues_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
  char          queue_name[32];
  char          spname[256];
  unsigned long long     depth;
  unsigned long long     age;
  int           last_qid;
  int           is_last;
};

/* Column numbers */
#define STQUEUE_QUEUENAME    0
#define STQUEUE_SPNAME       1
#define STQUEUE_HEADTIME     2
#define STQUEUE_DEPTH        3

static int systblQueuesConnect(
  sqlite3 *db,
  void *pAux,
  int argc,
  const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pErr
){
  sqlite3_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db,
     "CREATE TABLE comdb2_queues(queuename, spname, head_age, depth)");
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
static int systblQueuesDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int get_stats(struct systbl_queues_cursor *pCur) {
  struct consumer_stat stats[MAXCONSUMERS] = {0};
  unsigned long long depth = 0;
  char *spname = NULL;

  dbqueuedb_get_name(thedb->qdbs[pCur->last_qid], &spname);
  strcpy(pCur->queue_name, thedb->qdbs[pCur->last_qid]->tablename);
  if (spname) {
      strcpy(pCur->spname, spname);
      free(spname);
  }
  else
      pCur->spname[0] = 0;

  int rc = dbqueuedb_get_stats(thedb->qdbs[pCur->last_qid], stats);
  if (rc) {
      /* TODO: signal error? */
  }
  for (int consumern = 0; consumern < MAXCONSUMERS; consumern++) {
      if (stats[consumern].has_stuff)
          depth += stats[consumern].depth;
  }

  pCur->depth = depth;
  if (stats[0].epoch)
      pCur->age  = comdb2_time_epoch() - stats[0].epoch;
  else
      pCur->age  = 0;
  return 0;
}


/*
** Constructor for systbl_queues_cursor objects.
*/
static int systblQueuesOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  systbl_queues_cursor *pCur;

  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  if (thedb->num_qdbs) {
      pCur->last_qid = 0;
      get_stats(pCur);
  } else {
      pCur->is_last = 1;
  }
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for systbl_queues_cursor.
*/
static int systblQueuesClose(sqlite3_vtab_cursor *cur){
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Advance to the next queue
*/
static int systblQueuesNext(sqlite3_vtab_cursor *cur){
  systbl_queues_cursor *pCur = (systbl_queues_cursor*)cur;
  pCur->last_qid++;
  if (pCur->last_qid >= thedb->num_qdbs) {
      pCur->is_last = 1;
  } else {
      get_stats(pCur);
  }
  pCur->iRowid++;
  return SQLITE_OK;
}

/*
** Return the queue info for the current row.
*/
static int systblQueuesColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  systbl_queues_cursor *pCur = (systbl_queues_cursor*)cur;

  switch( i ){
    case STQUEUE_QUEUENAME: {
      sqlite3_result_text(ctx, pCur->queue_name, -1, NULL);
      break;
    }
    case STQUEUE_SPNAME: {
      if (pCur->spname[0] == 0)
        sqlite3_result_null(ctx);
      else
        sqlite3_result_text(ctx, pCur->spname, -1, NULL);
      break;
    }
    case STQUEUE_DEPTH: {
      sqlite3_result_int64(ctx, (sqlite3_int64)pCur->depth);
      break;
    }    
    case STQUEUE_HEADTIME: {
      sqlite3_result_int64(ctx, (sqlite3_int64)pCur->age);
      break;
    }
  }
  return SQLITE_OK;
};

/*
** Return the rowid for the current row. The rowid is the just the
** index of this queue in qdb array.
*/
static int systblQueuesRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_queues_cursor *pCur = (systbl_queues_cursor*)cur;

  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int systblQueuesEof(sqlite3_vtab_cursor *cur){
  systbl_queues_cursor *pCur = (systbl_queues_cursor*)cur;
  return  pCur->is_last;
}

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to xColumn() or xRowid() or xEof().
*/
static int systblQueuesFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  systbl_queues_cursor *pCur = (systbl_queues_cursor*)pVtabCursor;

  pCur->iRowid = 0;
  return SQLITE_OK;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int systblQueuesBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlite3_module systblQueuesModule = {
  0,                            /* iVersion */
  0,                            /* xCreate */
  systblQueuesConnect,       /* xConnect */
  systblQueuesBestIndex,     /* xBestIndex */
  systblQueuesDisconnect,    /* xDisconnect */
  0,                            /* xDestroy */
  systblQueuesOpen,          /* xOpen - open a cursor */
  systblQueuesClose,         /* xClose - close a cursor */
  systblQueuesFilter,        /* xFilter - configure scan constraints */
  systblQueuesNext,          /* xNext - advance a cursor */
  systblQueuesEof,           /* xEof - check for end of scan */
  systblQueuesColumn,        /* xColumn - read data */
  systblQueuesRowid,         /* xRowid - read data */
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
