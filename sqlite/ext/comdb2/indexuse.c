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

#if (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) &&          \
    !defined(SQLITE_OMIT_VIRTUALTABLE)

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
#define SQLITE_CORE 1
#endif

#include "comdb2.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include "cdb2api.h"

static sqlite3_module systblIndexUsageModule = {
    .access_flag = CDB2_ALLOW_USER,
    .systable_lock = "comdb2_tables"
};

struct index_usage {
    char *tablename;
    int64_t ixnum;
    char *cscname;
    char *sqlname;
    int64_t steps;
    int64_t non_sql_steps;
};
typedef struct index_usage index_usage;

static void free_index_usage(void *recsp, int nrecs) {
    struct index_usage *ixs = (struct index_usage*) recsp;
    for (int i = 0; i < nrecs; i++) {
        struct index_usage *ix = &ixs[i];
        free(ix->cscname);
        free(ix->sqlname);
        free(ix->tablename);
    }
    free(ixs);
}

static int get_index_usage(void **recsp, int *nrecs) {
    struct dbtable *db;
    struct index_usage *ixs = NULL;
    int allocated = 0;
    int nix = 0;


    for (int dbn = 0; dbn < thedb->num_dbs; dbn++) {
        db = thedb->dbs[dbn];
        if (strncmp(db->tablename, "sqlite_stat", strlen("sqlite_stat")) == 0)
            continue;
        logmsg(LOGMSG_USER, "table '%s'\n", db->tablename);
        for (int ixnum = 0; ixnum < db->nix; ixnum++) {
            if (nix == allocated) {
                allocated = allocated * 2 + 16;
                struct index_usage *n = realloc(ixs, allocated * sizeof(struct index_usage));
                if (n == NULL) {
                    free_index_usage(ixs, nix);
                    return -1;
                }
                ixs = n;
            }
            struct index_usage *ix = &ixs[nix];
            ix->tablename = strdup(db->tablename);
            ix->ixnum = ixnum;
            ix->cscname = strdup(db->schema->ix[ixnum]->csctag);
            ix->sqlname =  strdup(db->schema->ix[ixnum]->sqlitetag);
            ix->steps = db->sqlixuse[ixnum];
            ix->non_sql_steps = db->ixuse[ixnum];

            nix++;
        }
    }
    *nrecs = nix;
    *recsp = ixs;
    return 0;
}

int systblSQLIndexStatsInit(sqlite3 *db) {
    int rc = create_system_table(db, "comdb2_index_usage", &systblIndexUsageModule,
            get_index_usage, free_index_usage, sizeof(index_usage),
            CDB2_CSTRING, "table_name", -1, offsetof(index_usage, tablename),
            CDB2_INTEGER, "ix_num", -1, offsetof(index_usage, ixnum),
            CDB2_CSTRING, "csc_name", -1, offsetof(index_usage, cscname),
            CDB2_CSTRING, "sql_name", -1, offsetof(index_usage, sqlname),
            CDB2_INTEGER, "steps", -1, offsetof(index_usage, steps),
            CDB2_INTEGER, "non_sql_steps", -1, offsetof(index_usage, non_sql_steps),
            SYSTABLE_END_OF_FIELDS);
    return rc;
}


#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
