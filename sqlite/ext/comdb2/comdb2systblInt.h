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


#ifndef comdb2systblInt_h
#define comdb2systblInt_h

#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

extern const sqlite3_module systblTablesModule;
extern const sqlite3_module systblColumnsModule;
extern const sqlite3_module systblKeysModule;
extern const sqlite3_module systblFieldsModule;
extern const sqlite3_module systblConstraintsModule;
extern const sqlite3_module systblTblSizeModule;
extern const sqlite3_module systblSPsModule;
extern const sqlite3_module systblUsersModule;
extern const sqlite3_module systblQueuesModule;
extern const sqlite3_module systblTriggersModule;
extern const sqlite3_module systblKeywordsModule;
extern const sqlite3_module systblLimitsModule;
extern const sqlite3_module systblTunablesModule;
extern const sqlite3_module systblThreadPoolsModule;
extern const sqlite3_module systblPluginsModule;
extern const sqlite3_module systblAppsockHandlersModule;
extern const sqlite3_module systblOpcodeHandlersModule;
extern const sqlite3_module completionModule; // in ext/misc
extern const sqlite3_module systblClientStatsModule;
extern const sqlite3_module systblTimepartModule;
extern const sqlite3_module systblTimepartShardsModule;
extern const sqlite3_module systblTimepartEventsModule;
extern const sqlite3_module systblCronSchedsModule;
extern const sqlite3_module systblCronEventsModule;
extern const sqlite3_module systblTransactionLogsModule;
extern const sqlite3_module systblMetricsModule;
extern const sqlite3_module systblTimeseriesModule;
extern const sqlite3_module systblReplStatsModule;
extern const sqlite3_module systblLogicalOpsModule;
extern const sqlite3_module systblSystabsModule;
extern const sqlite3_module systblFdbInfoModule;
extern sqlite3_module systblTablePermissionsModule;
extern sqlite3_module systblSystabPermissionsModule;
extern sqlite3_module systblTimepartPermissionsModule;

int systblTypeSamplesInit(sqlite3 *db);
int systblRepNetQueueStatInit(sqlite3 *db);
int systblSqlpoolQueueInit(sqlite3 *db);
int systblActivelocksInit(sqlite3 *db);
int systblNetUserfuncsInit(sqlite3 *db);
int systblClusterInit(sqlite3 *db);
int systblActiveOsqlsInit(sqlite3 *db);
int systblBlkseqInit(sqlite3 *db);
int systblPartialDatacopiesInit(sqlite3 *db);
int systblTablePropertiesInit(sqlite3 *db);
int systblTimepartInit(sqlite3*db);
int systblCronInit(sqlite3*db);
int systblFingerprintsInit(sqlite3 *);
int systblQueryPlansInit(sqlite3 *db);
int systblViewsInit(sqlite3 *);
int systblSQLClientStats(sqlite3 *);
int systblSQLIndexStatsInit(sqlite3 *);
int systblTemporaryFileSizesModuleInit(sqlite3 *);

int comdb2_next_allowed_table(sqlite3_int64 *tabId);
struct dbtable *comdb2_get_dbtable_or_shard0(sqlite3_int64 tabId);

int systblScStatusInit(sqlite3 *db);
int systblScHistoryInit(sqlite3 *db);
int systblConnectionsInit(sqlite3 *db);
int systblFunctionsInit(sqlite3 *db);
int systblTablePermissionsInit(sqlite3 *db);
int systblSystabPermissionsInit(sqlite3 *db);
int systblTimepartPermissionsInit(sqlite3 *db);
int systblFdbInfoInit(sqlite3 *db);
int systblTranCommitInit(sqlite3 *db);
int systblTransactionStateInit(sqlite3 *db);
int systblMemstatsInit(sqlite3 *db);
int systblStacks(sqlite3 *db);
int systblPreparedInit(sqlite3 *db);

/* Simple yes/no answer for booleans */
#define YESNO(x) ((x) ? "Y" : "N")

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */

#endif
