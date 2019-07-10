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
#include "ezsystables.h"

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */

/* This initializes this table but also a bunch of other schema tables
** that fall under the similar use. */
#ifdef SQLITE_BUILDING_FOR_COMDB2
extern int sqlite3CompletionVtabInit(sqlite3 *);

static sqlite3_module systblTablesModule = {
    .access_flag = CDB2_ALLOW_USER,
};

struct table {
    char *name;
};

static int get_tables(void **data, int *npoints) {
    struct table *tables;

    for (int i = 0; i < thedb->num_dbs; i++) {
        struct dbtable *db = thedb->dbs[i];
        if (!db->is_systable)
            (*npoints)++;
    }
    tables = malloc(sizeof(struct table) * *npoints);

    int i = 0;
    for (int tbl = 0; tbl < thedb->num_dbs; tbl++) {
        struct dbtable *db = thedb->dbs[tbl];
        if (!db->is_systable)
            tables[i++].name = strdup(db->tablename);
    }
    *data = tables;
    return 0;
}

static void free_tables(void *p, int n) {
    struct table *t = (struct table *)p;
    for (int i = 0; i < n; i++) {
        free(t[i].name);
    }
    free(t);
}

int systblTablesInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_tables",
            &systblTablesModule, get_tables, free_tables,
            sizeof(struct table),
            CDB2_CSTRING, "name", -1, offsetof(struct table, name),
            SYSTABLE_END_OF_FIELDS);
}


/* This initializes this table but also a bunch of other schema tables
** that fall under the similar use. */
extern int sqlite3CompletionVtabInit(sqlite3 *);

int comdb2SystblInit(
  sqlite3 *db
){
  int rc = SQLITE_OK;
#ifndef SQLITE_OMIT_VIRTUALTABLE
  rc = systblSystablesInit(db);
  if (rc == SQLITE_OK)
    rc = sqlite3_create_module(db, "comdb2_columns", &systblColumnsModule, 0);
  if (rc == SQLITE_OK)
    rc = systblTablesInit(db);
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
    rc = sqlite3_create_module(db, "comdb2_tablepermissions", &systblTablePermissionsModule, 0);
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
    rc = systblScStatusInit(db);
  if (rc == SQLITE_OK)
    rc = systblConnectionsInit(db);
  if (rc == SQLITE_OK)
    rc = systblViewsInit(db);
  if (rc == SQLITE_OK)
    rc = systblSQLClientStats(db);
  if (rc == SQLITE_OK)
    rc = systblSQLIndexStatsInit(db);
#endif
  return rc;
}
#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
