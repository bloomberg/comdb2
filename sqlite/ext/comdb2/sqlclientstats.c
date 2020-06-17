/*
   Copyright 2019 Bloomberg Finance L.P.

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

#include <pthread.h>
#include <comdb2systblInt.h>
#include <ezsystables.h>
#include "sql.h"

int gather_client_sql_data(void **ent, int *npoints);
void free_client_sql_data(void *data, int npoints);

static sqlite3_module systblSQLClientStatsModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int systblSQLClientStats(sqlite3 *db) {
    return create_system_table(db, "comdb2_sql_client_stats", &systblSQLClientStatsModule,
            gather_client_sql_data, free_client_sql_data, sizeof(struct client_sql_systable_data),
            CDB2_CSTRING, "host", -1, offsetof(struct client_sql_systable_data, host),
            CDB2_CSTRING, "task", -1, offsetof(struct client_sql_systable_data, task),
            CDB2_CSTRING, "fingerprint", -1, offsetof(struct client_sql_systable_data, fingerprint),
            CDB2_INTEGER, "qcount", -1, offsetof(struct client_sql_systable_data, count),
            CDB2_INTEGER, "timems", -1, offsetof(struct client_sql_systable_data, timems),
            CDB2_INTEGER, "cost", -1, offsetof(struct client_sql_systable_data, cost),
            CDB2_INTEGER, "rows", -1, offsetof(struct client_sql_systable_data, rows),
            SYSTABLE_END_OF_FIELDS);
}

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
