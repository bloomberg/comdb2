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
#if (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
    && !defined(SQLITE_OMIT_VIRTUALTABLE)

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
# define SQLITE_CORE 1
#endif

#include "views.h"

typedef struct timepart_cursor timepart_cursor;
struct timepart_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
  int maxTimepartitions;
  int eof;
};

/*
** Constructor for sqlite3_vtab object
*/
static int timepartConnect(
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
          "CREATE TABLE comdb2_timepartitions(name, period, retention,"
          " nshards, version, shard0name, starttime, sourceid)");
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
static int timepartDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/* cursor open */
static int timepartOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  timepart_cursor *pCur;

  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  pCur->iRowid = -1;
  views_lock();
  pCur->maxTimepartitions = timepart_get_num_views();
  pCur->eof = (pCur->maxTimepartitions == 0);

  return SQLITE_OK;
}

/* cursor close */
static int timepartClose(sqlite3_vtab_cursor *cur){
  views_unlock();
  sqlite3_free(cur);
  return SQLITE_OK;
}

/* cursor next */
static int timepartNext(sqlite3_vtab_cursor *cur){
  timepart_cursor *pCur = (timepart_cursor*)cur;

  if(pCur->iRowid<pCur->maxTimepartitions)
      pCur->iRowid++;
    
  pCur->eof = pCur->iRowid >= pCur->maxTimepartitions;

  return SQLITE_OK;
}

/* cursor get column */
static int timepartColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  timepart_cursor *pCur = (timepart_cursor*)cur;

  timepart_systable_column(ctx, pCur->iRowid, i);
  return SQLITE_OK;
};

/* cursor rowid */
static int timepartRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  timepart_cursor *pCur = (timepart_cursor*)cur;

  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/* is cursor eof */
static int timepartEof(sqlite3_vtab_cursor *cur) {
  timepart_cursor *pCur = (timepart_cursor*)cur;

  return pCur->eof;
}

static int timepartFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  timepart_cursor *pCur = (timepart_cursor*)pVtabCursor;

  pCur->iRowid = 0;
  pCur->eof = (pCur->maxTimepartitions == 0);
  return SQLITE_OK;
}

static int timepartBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlite3_module systblTimepartModule = {
  0,                   /* iVersion */
  0,                   /* xCreate */
  timepartConnect,     /* xConnect */
  timepartBestIndex,   /* xBestIndex */
  timepartDisconnect,  /* xDisconnect */
  0,                   /* xDestroy */
  timepartOpen,        /* xOpen - open a cursor */
  timepartClose,       /* xClose - close a cursor */
  timepartFilter,      /* xFilter - configure scan constraints */
  timepartNext,        /* xNext - advance a cursor */
  timepartEof,         /* xEof - check for end of scan */
  timepartColumn,      /* xColumn - read data */
  timepartRowid,       /* xRowid - read data */
  0,                   /* xUpdate */
  0,                   /* xBegin */
  0,                   /* xSync */
  0,                   /* xCommit */
  0,                   /* xRollback */
  0,                   /* xFindMethod */
  0,                   /* xRename */
};

static int timepartShardsConnect(
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
          "CREATE TABLE comdb2_timepartshards(name, shardname, start, end)");
  if( rc==SQLITE_OK ){
      pNew = *ppVtab = sqlite3_malloc( sizeof(*pNew) );
      if( pNew==0 ) return SQLITE_NOMEM;
      memset(pNew, 0, sizeof(*pNew));
  }
  return rc;

}

#define PARTID(s) (((s) & 0x0FFFF0000)>>16)
#define SHARDID(s) ((s) & 0x0FFFF)

/* cursor next */
static int timepartShardsNext(sqlite3_vtab_cursor *cur) {
  timepart_cursor *pCur = (timepart_cursor*)cur;
  int tpid = PARTID(pCur->iRowid);
  int shardid = SHARDID(pCur->iRowid);

  if(!pCur->eof) {
      timepart_systable_next_shard(&tpid, &shardid);

      pCur->eof = tpid >= pCur->maxTimepartitions;

      pCur->iRowid = tpid<<16 | shardid;
  }

  return SQLITE_OK;
}

/* cursor get column */
static int timepartShardsColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  timepart_cursor *pCur = (timepart_cursor*)cur;
  int tpid = PARTID(pCur->iRowid);
  int shardid = SHARDID(pCur->iRowid);

  timepart_systable_shard_column(ctx, tpid, shardid, i);
  return SQLITE_OK;
};


const sqlite3_module systblTimepartShardsModule = {
  0,                    /* iVersion */
  0,                    /* xCreate */
  timepartShardsConnect,/* xConnect */
  timepartBestIndex,    /* xBestIndex */
  timepartDisconnect,   /* xDisconnect */
  0,                    /* xDestroy */
  timepartOpen,         /* xOpen - open a cursor */
  timepartClose,        /* xClose - close a cursor */
  timepartFilter,       /* xFilter - configure scan constraints */
  timepartShardsNext,   /* xNext - advance a cursor */
  timepartEof,          /* xEof - check for end of scan */
  timepartShardsColumn, /* xColumn - read data */
  timepartRowid,        /* xRowid - read data */
  0,                    /* xUpdate */
  0,                    /* xBegin */
  0,                    /* xSync */
  0,                    /* xCommit */
  0,                    /* xRollback */
  0,                    /* xFindMethod */
  0,                    /* xRename */
};



typedef struct timepart_events_cursor timepart_events_cursor;
struct timepart_events_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
  int maxEvents;
  int eof;
};


static int timepartEventsConnect(
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
          "CREATE TABLE comdb2_timepartevents(name, swhen, sourceid, arg1, arg2, arg3)");
  if( rc==SQLITE_OK ){
      pNew = *ppVtab = sqlite3_malloc( sizeof(*pNew) );
      if( pNew==0 ) return SQLITE_NOMEM;
      memset(pNew, 0, sizeof(*pNew));
  }
  return rc;

}


/* cursor open */
static int timepartEventsOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  timepart_events_cursor *pCur;
  int rc;

  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  pCur->iRowid = -1;

  rc = timepart_events_open(&pCur->maxEvents);
  if(rc)
    return SQLITE_ERROR;

  pCur->eof = (pCur->maxEvents == 0);

  return SQLITE_OK;
}

/* cursor close */
static int timepartEventsClose(sqlite3_vtab_cursor *cur){
  timepart_events_close();
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int timepartEventsFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  timepart_events_cursor *pCur = (timepart_events_cursor*)pVtabCursor;

  pCur->iRowid = 0;
  pCur->eof = (pCur->maxEvents==0);
  return SQLITE_OK;
}

/* cursor next */
static int timepartEventsNext(sqlite3_vtab_cursor *cur){
  timepart_events_cursor *pCur = (timepart_events_cursor*)cur;

  if(pCur->iRowid<pCur->maxEvents)
      pCur->iRowid++;
    
  pCur->eof = (pCur->iRowid >= pCur->maxEvents);

  return SQLITE_OK;
}

/* cursor get column */
static int timepartEventsColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  timepart_events_cursor *pCur = (timepart_events_cursor*)cur;

  timepart_events_column(ctx, pCur->iRowid, i);
  return SQLITE_OK;
};


const sqlite3_module systblTimepartEventsModule = {
  0,                    /* iVersion */
  0,                    /* xCreate */
  timepartEventsConnect,/* xConnect */
  timepartBestIndex,    /* xBestIndex */
  timepartDisconnect,   /* xDisconnect */
  0,                    /* xDestroy */
  timepartEventsOpen,   /* xOpen - open a cursor */
  timepartEventsClose,  /* xClose - close a cursor */
  timepartEventsFilter, /* xFilter - configure scan constraints */
  timepartEventsNext,   /* xNext - advance a cursor */
  timepartEof,          /* xEof - check for end of scan */
  timepartEventsColumn, /* xColumn - read data */
  timepartRowid,        /* xRowid - read data */
  0,                    /* xUpdate */
  0,                    /* xBegin */
  0,                    /* xSync */
  0,                    /* xCommit */
  0,                    /* xRollback */
  0,                    /* xFindMethod */
  0,                    /* xRename */
};

#endif /* SQLITE_BUILDING_FOR_COMDB2 */
