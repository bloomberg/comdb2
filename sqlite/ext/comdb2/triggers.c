/*
   Copyright 2015 Bloomberg Finance L.P.

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

/* Implement comdb2_triggers to introspect triggers and consumers */

#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include <schema_lk.h>
#include <comdb2.h>
#include <dbqueue.h>
#include <translistener.h>

typedef struct trigger trigger;
struct trigger {
  LINKC_T(trigger) lnk;
  char *name;
  int type;
  trigger_info info;
};
typedef struct {
  sqlite3_vtab_cursor base;
  sqlite3_int64 iRowid;
  LISTC_T(trigger) trgs; /* List of triggers */
  trigger *trg;          /* Current trigger */
  trigger_tbl_info *tbl; /* Current trigger-table */
  trigger_col_info *col; /* Current trigger-col */
} trigger_cursor;

enum {
  TRIGGER_NAME,
  TRIGGER_TYPE,
  TRIGGER_TABLE,
  TRIGGER_EVENT,
  TRIGGER_COL,
};

static int triggerConnect(
  sqlite3 *db,
  void *pAux,
  int argc,
  const char *const *argv,
  sqlite3_vtab **ppVtab,
  char **pErr
){
  int rc = sqlite3_declare_vtab(db, "CREATE TABLE comdb2_triggers("
                                    "\"name\",\"type\",\"tbl_name\","
                                    "\"event\",\"col\")");
  if( rc == SQLITE_OK ){
    if( (*ppVtab = sqlite3_malloc(sizeof(sqlite3_vtab))) == 0)
      return SQLITE_NOMEM;
    memset(*ppVtab, 0, sizeof(*ppVtab));
  }
  return rc;
}

static int triggerBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){
  return SQLITE_OK;
}

static int triggerDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static void get_info(trigger_cursor *cur){
  trigger *t;
again:
  t = cur->trg;
  get_trigger_info(t->name, &t->info);
  cur->tbl = LISTC_TOP(&t->info.tbls);
  if( cur->tbl ){
    cur->col = LISTC_TOP(&cur->tbl->cols);
  } else { // no tbls or trigger no longer present
    if( (cur->trg = LISTC_NEXT(cur->trg, lnk)) != NULL ){
      goto again;
    }
  }
}

static int triggerOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  trigger_cursor *cur = sqlite3_malloc(sizeof(trigger_cursor));
  if( cur == 0)
    return SQLITE_NOMEM;
  memset(cur, 0, sizeof(*cur));
  listc_init(&cur->trgs, offsetof(trigger, lnk));
  rdlock_schema_lk(); // protect thedb access
  trigger *t;
  for(int i = 0; i < thedb->num_qdbs; ++i){
    if( thedb->qdbs[i] == NULL )
      continue;
    t = sqlite3_malloc(sizeof(trigger));
    t->name = strdup(thedb->qdbs[i]->dbname);
    t->type = -1;
    if( thedb->qdbs[i]->consumers && thedb->qdbs[i]->consumers[0] )
      t->type = thedb->qdbs[i]->consumers[0]->type;
    listc_abl(&cur->trgs, t);
  }
  unlock_schema_lk();
  if( (t = LISTC_TOP(&cur->trgs)) != NULL ){
    cur->trg = t;
    get_info(cur);
  }

  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static void free_info(trigger_cursor *cur){
  if( cur->trg == NULL)
    return;
  trigger_tbl_info *tbl, *tmp1;
  LISTC_FOR_EACH_SAFE(&cur->trg->info.tbls, tbl, tmp1, lnk){
    trigger_col_info *col, *tmp2;
    LISTC_FOR_EACH_SAFE(&tbl->cols, col, tmp2, lnk){
      free(col->name);
      free(col);
    }
    free(tbl->name);
    free(tbl);
  }
  cur->col = NULL;
  cur->tbl = NULL;
}

static int triggerClose(sqlite3_vtab_cursor *cur){
  trigger_cursor *pCur = (trigger_cursor *)cur;
  free_info(pCur);
  trigger *t, *tmp;
  LISTC_FOR_EACH_SAFE(&pCur->trgs, t, tmp, lnk){
    free(t->name);
    sqlite3_free(t);
  }
  return SQLITE_OK;
}

static int triggerFilter(sqlite3_vtab_cursor *pVtabCursor,
  int idxNum,
  const char *idxStr,
  int argc,
  sqlite3_value **argv
){
  trigger_cursor *cur = (trigger_cursor *)pVtabCursor;
  trigger *t;

  free_info(cur);
  if( (t = LISTC_TOP(&cur->trgs)) != NULL ){
    cur->trg = t;
    get_info(cur);
  }

  return SQLITE_OK;
}

static int triggerEof(sqlite3_vtab_cursor *cur){
  trigger_cursor *pCur = (trigger_cursor *)cur;
  return pCur->trg == NULL;
}

static int triggerNext(sqlite3_vtab_cursor *cur){
  trigger_cursor *pCur = (trigger_cursor *)cur;
  ++pCur->iRowid;
  if( (pCur->col = LISTC_NEXT(pCur->col, lnk)) == NULL ){
    if( (pCur->tbl = LISTC_NEXT(pCur->tbl, lnk)) == NULL ){
      free_info(pCur);
      if( (pCur->trg = LISTC_NEXT(pCur->trg, lnk)) != NULL ){
        get_info(pCur);
      }
    } else {
      pCur->col = LISTC_TOP(&pCur->tbl->cols);
    }
  }
  return SQLITE_OK;
}

static int triggerColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){
  trigger_cursor *pCur = (trigger_cursor *)cur;
  trigger *trg = pCur->trg;
  if( trg == NULL ){
    sqlite3_result_null(ctx);
    return SQLITE_OK;
  }
  const char *name = trg->name;
  if( strncmp(name, "__q", 3) == 0 ){
    name += 3;
  }
  trigger_tbl_info *t = pCur->tbl;
  trigger_col_info *c = pCur->col;
  char *type;
  switch(i){
    case TRIGGER_NAME: sqlite3_result_text(ctx, name, -1, NULL); break;
    case TRIGGER_TYPE:
      switch(trg->type){
        case CONSUMER_TYPE_LUA:
          sqlite3_result_text(ctx, "trigger", -1, NULL);
          break;
        case CONSUMER_TYPE_DYNLUA:
          sqlite3_result_text(ctx, "consumer", -1, NULL);
          break;
        default:
          sqlite3_result_null(ctx);
          break;
      }
      break;
    case TRIGGER_TABLE:
      if( t ){
        sqlite3_result_text(ctx, t->name, -1, NULL);
      } else {
        sqlite3_result_null(ctx);
      }
      break;
    case TRIGGER_EVENT:
      if( c ){
        switch(c->type){
          case JAVASP_TRANS_LISTEN_AFTER_ADD: type = "add"; break;
          case JAVASP_TRANS_LISTEN_AFTER_DEL: type = "del"; break;
          case JAVASP_TRANS_LISTEN_AFTER_UPD: type = "upd"; break;
          default: type = "???"; break;
        }
        sqlite3_result_text(ctx, type, -1, NULL);
      } else {
        sqlite3_result_null(ctx);
      }
      break;
    case TRIGGER_COL:
      if( c ){
        sqlite3_result_text(ctx, c->name, -1, NULL);
      } else {
        sqlite3_result_null(ctx);
      }
      break;
  }
  return SQLITE_OK;
}

static int triggerRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  trigger_cursor *pCur = (trigger_cursor *)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

const sqlite3_module systblTriggersModule = {
  0,                 /* iVersion */
  0,                 /* xCreate */
  triggerConnect,    /* xConnect */
  triggerBestIndex,  /* xBestIndex */
  triggerDisconnect, /* xDisconnect */
  0,                 /* xDestroy */
  triggerOpen,       /* xOpen - open a cursor */
  triggerClose,      /* xClose - close a cursor */
  triggerFilter,     /* xFilter - configure scan constraints */
  triggerNext,       /* xNext - advance a cursor */
  triggerEof,        /* xEof - check for end of scan */
  triggerColumn,     /* xColumn - read data */
  triggerRowid,      /* xRowid - read data */
  0,                 /* xUpdate */
  0,                 /* xBegin */
  0,                 /* xSync */
  0,                 /* xCommit */
  0,                 /* xRollback */
  0,                 /* xFindMethod */
  0,                 /* xRename */
};
