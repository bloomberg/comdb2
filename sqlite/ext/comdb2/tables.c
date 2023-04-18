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

#include "comdb2.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "views.h"
#include "timepart_systable.h"

/* systbl_tables_cursor is a subclass of sqlite3_vtab_cursor which serves
** as the underlying cursor to enumerate the rows in this vtable. The 
** rows in this vtable are of course the list of tables in the database.
** That is, "select name from sqlite_master where type='table'"
*/
typedef struct systbl_tables_cursor systbl_tables_cursor;
struct systbl_tables_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
};

static int systblTablesConnect(
  sqlite3 *db,
  void *pAux,
  int argc,
  const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pErr
){
  sqlite3_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db, "CREATE TABLE comdb2_tables(tablename)");
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
static int systblTablesDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for systbl_tables_cursor objects.
*/
static int systblTablesOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  systbl_tables_cursor *pCur;

  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;

  comdb2_next_allowed_table(&pCur->iRowid);

  return SQLITE_OK;
}

/*
** Destructor for systbl_tables_cursor.
*/
static int systblTablesClose(sqlite3_vtab_cursor *cur){
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Advance to the next table name from thedb.
*/
static int systblTablesNext(sqlite3_vtab_cursor *cur){
  systbl_tables_cursor *pCur = (systbl_tables_cursor*)cur;
  pCur->iRowid++;
  comdb2_next_allowed_table(&pCur->iRowid);
  return SQLITE_OK;
}

/*
** Return the table name for the current row.
*/
static int systblTablesColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  systbl_tables_cursor *pCur = (systbl_tables_cursor*)cur;
  const char *x = NULL;

  if (pCur->iRowid < thedb->num_dbs) {
    struct dbtable *pDb = thedb->dbs[pCur->iRowid];
    x = pDb->sqlaliasname ? pDb->sqlaliasname : pDb->tablename;
  } else {
    x = timepart_name(pCur->iRowid - thedb->num_dbs);
  }

  sqlite3_result_text(ctx, x, -1, NULL);
  return SQLITE_OK;
}

/*
** Return the rowid for the current row. The rowid is the just the
** index of this table into the db array.
*/
static int systblTablesRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_tables_cursor *pCur = (systbl_tables_cursor*)cur;

  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int systblTablesEof(sqlite3_vtab_cursor *cur){
  systbl_tables_cursor *pCur = (systbl_tables_cursor*)cur;

  return pCur->iRowid >= timepart_systable_num_tables_and_views();
}

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to seriesColumn() or seriesRowid() or
** seriesEof().
*/
static int systblTablesFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  systbl_tables_cursor *pCur = (systbl_tables_cursor*)pVtabCursor;
  pCur->iRowid = 0;
  comdb2_next_allowed_table(&pCur->iRowid);
  return SQLITE_OK;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int systblTablesBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlite3_module systblTablesModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  systblTablesConnect,       /* xConnect */
  systblTablesBestIndex,     /* xBestIndex */
  systblTablesDisconnect,    /* xDisconnect */
  0,                         /* xDestroy */
  systblTablesOpen,          /* xOpen - open a cursor */
  systblTablesClose,         /* xClose - close a cursor */
  systblTablesFilter,        /* xFilter - configure scan constraints */
  systblTablesNext,          /* xNext - advance a cursor */
  systblTablesEof,           /* xEof - check for end of scan */
  systblTablesColumn,        /* xColumn - read data */
  systblTablesRowid,         /* xRowid - read data */
  0,                         /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindMethod */
  0,                         /* xRename */
  0,                         /* xSavepoint */
  0,                         /* xRelease */
  0,                         /* xRollbackTo */
  0,                         /* xShadowName */
  .systable_lock = "comdb2_tables",
};

/* This initializes this table but also a bunch of other schema tables
** that fall under the similar use. */
extern int sqlite3CompletionVtabInit(sqlite3 *);
int sqlite3_carray_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi);

int comdb2SystblInit(
  sqlite3 *db
){
  int rc = SQLITE_OK;
#ifndef SQLITE_OMIT_VIRTUALTABLE
  rc = sqlite3_create_module(db, "comdb2_tables", &systblTablesModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_columns", &systblColumnsModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_keys", &systblKeysModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_keycomponents", &systblFieldsModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_constraints", &systblConstraintsModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_tablesizes", &systblTblSizeModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_procedures", &systblSPsModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_users", &systblUsersModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_queues", &systblQueuesModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_triggers", &systblTriggersModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_keywords", &systblKeywordsModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_limits", &systblLimitsModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_tunables", &systblTunablesModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_threadpools", &systblThreadPoolsModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_plugins", &systblPluginsModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_appsock_handlers",
                               &systblAppsockHandlersModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_opcode_handlers",
                               &systblOpcodeHandlersModule, 0);
  if (rc == SQLITE_OK){
    rc = sqlite3CompletionVtabInit(db);
  }
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_clientstats", &systblClientStatsModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_transaction_logs", &systblTransactionLogsModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_metrics", &systblMetricsModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_timeseries", &systblTimeseriesModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_repl_stats", &systblReplStatsModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_logical_operations", &systblLogicalOpsModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_systables", &systblSystabsModule, 0);
  if (rc == SQLITE_OK)
    rc = systblPartialDatacopiesInit(db);
  if (rc == SQLITE_OK)
    rc = systblTablePropertiesInit(db);
  if (rc == SQLITE_OK)
    rc = systblTimepartInit(db);
  if (rc == SQLITE_OK)
    rc = systblCronInit(db);
  if (rc == SQLITE_OK)
    rc = systblTypeSamplesInit(db);
  if (rc == SQLITE_OK)
    rc = systblRepNetQueueStatInit(db);
  if (rc == SQLITE_OK)
    rc = systblActivelocksInit(db);
  if (rc == SQLITE_OK)
    rc = systblSqlpoolQueueInit(db);
  if (rc == SQLITE_OK)
    rc = systblNetUserfuncsInit(db);
  if (rc == SQLITE_OK)
    rc = systblClusterInit(db);
  if (rc == SQLITE_OK)
    rc = systblActiveOsqlsInit(db);
  if (rc == SQLITE_OK)
    rc = systblBlkseqInit(db);
  if (rc == SQLITE_OK)
    rc = systblFingerprintsInit(db);
  if (rc == SQLITE_OK)
    rc = systblQueryPlansInit(db);
  if (rc == SQLITE_OK)
    rc = systblScStatusInit(db);
  if (rc == SQLITE_OK)
    rc = systblScHistoryInit(db);
  if (rc == SQLITE_OK)
    rc = systblConnectionsInit(db);
  if (rc == SQLITE_OK)
    rc = systblViewsInit(db);
  if (rc == SQLITE_OK)
    rc = systblSQLClientStats(db);
  if (rc == SQLITE_OK)
    rc = systblSQLIndexStatsInit(db);
  if (rc == SQLITE_OK)
    rc = systblTemporaryFileSizesModuleInit(db);
  if (rc == SQLITE_OK)
    rc = systblFunctionsInit(db);
  if (rc == SQLITE_OK)
    rc = systblTablePermissionsInit(db);
  if (rc == SQLITE_OK)
    rc = systblSystabPermissionsInit(db);
  if (rc == SQLITE_OK)
    rc = systblTimepartPermissionsInit(db);
  if (rc == SQLITE_OK)
    rc = systblFdbInfoInit(db);
  if (rc == SQLITE_OK)
    rc = sqlite3_carray_init(db, 0, 0);
  if (rc == SQLITE_OK)
    rc = systblMemstatsInit(db);
  if (rc == SQLITE_OK)
    rc = systblTransactionStateInit(db);
  if (rc == SQLITE_OK)  
    rc = systblStacks(db);
#ifdef INCLUDE_DEBUG_ONLY_SYSTABLES
  if (rc == SQLITE_OK)
    rc = systblTranCommitInit(db);
#endif
  if (rc == SQLITE_OK)  
    rc = systblPreparedInit(db);
#endif
  return rc;
}
#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
