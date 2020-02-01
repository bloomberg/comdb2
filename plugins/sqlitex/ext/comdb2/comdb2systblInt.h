#include "sqlitex.h"

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */

#if 0
const sqlitex_module systblTablesModule;
const sqlitex_module systblColumnsModule;
const sqlitex_module systblKeysModule;
const sqlitex_module systblFieldsModule;
const sqlitex_module systblConstraintsModule;
const sqlitex_module systblTblSizeModule;
const sqlitex_module systblSPsModule;
const sqlitex_module systblUsersModule;
const sqlitex_module systblQueuesModule;
const sqlitex_module systblTablePermissionsModule;
const sqlitex_module systblTriggersModule;
const sqlitex_module systblFingerprintsModule;
const sqlitex_module systblMetricsModule;
const sqlitex_module systblClientStatsModule;
const sqlitex_module systblTimeseriesModule;
const sqlitex_module systblReplStatsModule;

int systblTypeSamplesInit(sqlitex *db);
int systblConnectionsInit(sqlitex *db);
int systblClusterInit(sqlitex *db);
#endif

/* Simple yes/no answer for booleans */
#define YESNO(x) ((x) ? "Y" : "N")

#ifdef __cplusplus
}  /* extern "C" */
#endif  /* __cplusplus */
