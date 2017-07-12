#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */

const sqlite3_module systblTablesModule;
const sqlite3_module systblColumnsModule;
const sqlite3_module systblKeysModule;
const sqlite3_module systblFieldsModule;
const sqlite3_module systblConstraintsModule;
const sqlite3_module systblTblSizeModule;
const sqlite3_module systblSPsModule;
const sqlite3_module systblUsersModule;
const sqlite3_module systblTablePermissionsModule;
const sqlite3_module systblTriggersModule;
const sqlite3_module systblKeywordsModule;

/* Simple yes/no answer for booleans */
#define YESNO(x) ((x) ? "Y" : "N")

#ifdef __cplusplus
}  /* extern "C" */
#endif  /* __cplusplus */
