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
#include "ezsystables.h"

sqlite3_module systblTablesModule = {
    .access_flag = CDB2_ALLOW_ALL,
    .systable_lock = "comdb2_tables"
};

static int collect_tables(void **pd, int *pn)
{
    int ntables = 0;
    sqlite3_int64 tableid = 0;
    struct dbtable *pDb = NULL;
    int nviewable = 0;
    char **data = NULL;

    ntables = timepart_systable_num_tables_and_views();
    data = calloc(ntables, sizeof(char *));
    for (; comdb2_next_allowed_table(&tableid) == SQLITE_OK && tableid < ntables; ++tableid, ++nviewable) {
        if (tableid >= thedb->num_dbs) {
            data[nviewable] = strdup(timepart_name(tableid - thedb->num_dbs));
        } else {
            pDb = thedb->dbs[tableid];
            data[nviewable] = strdup(pDb->sqlaliasname ? pDb->sqlaliasname : pDb->tablename);
        }
    }

    *pn = nviewable;
    *pd = data;

    return 0;
}

static void free_tables(void *data, int n)
{
    char **tables = data;
    int i;
    for (i = 0; i != n; ++i)
        free(tables[i]);
    free(data);
}

int systblTablesInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_tables", &systblTablesModule,
        collect_tables,
        free_tables,  sizeof(char *),
        CDB2_CSTRING, "tablename", -1, 0,
        SYSTABLE_END_OF_FIELDS);
}

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
  rc = systblTablesInit(db);
  if (rc == SQLITE_OK)
    rc = systblColumnsInit(db);
  if (rc == SQLITE_OK)
    rc = systblTagsInit(db);
  if (rc == SQLITE_OK)
    rc = systblTagColumnsInit(db);
  if (rc == SQLITE_OK)
    rc = systblKeysInit(db);
  if (rc == SQLITE_OK)
    rc = systblKeyComponentsInit(db);
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
    rc = sqlite3_create_module(db, "comdb2_files", &systblFilesModule, 0);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_filenames", &systblFilenamesModule, 0);
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
    rc = systblSampleQueriesInit(db);
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
    rc = systblAutoAnalyzeTablesInit(db);
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
    rc = systblTriggersInit(db);
  if (rc == SQLITE_OK)  
    rc = systblStacks(db);
  if (rc == SQLITE_OK)
    rc = systblStringRefsInit(db);
#ifdef COMDB2_TEST
  if (rc == SQLITE_OK)
    rc = systblTranCommitInit(db);
#endif
  if (rc == SQLITE_OK)  
    rc = systblPreparedInit(db);
  if (rc == SQLITE_OK)
    rc = systblSchemaVersionsInit(db);
  if (rc == SQLITE_OK)
    rc = systblTableMetricsInit(db);
  if (rc == SQLITE_OK)
    rc = systblApiHistoryInit(db);
  if (rc == SQLITE_OK)
    rc = systblDbInfoInit(db);
#endif
  return rc;
}
#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
