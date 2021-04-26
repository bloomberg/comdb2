/*
   Copyright 2019-2020 Bloomberg Finance L.P.

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

#include <stddef.h>
#include "sqlite3.h"
#include "ezsystables.h"
#include "fdb_systable.h"

sqlite3_module systblFdbInfoModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int systblFdbInfoInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_fdb_info", &systblFdbInfoModule,
        fdb_systable_info_collect, fdb_systable_info_free,
        sizeof(fdb_systable_ent_t),
        CDB2_CSTRING, "dbname", -1, offsetof(fdb_systable_ent_t, dbname),
        CDB2_CSTRING, "location", -1, offsetof(fdb_systable_ent_t, location),
        CDB2_CSTRING, "tablename", -1, offsetof(fdb_systable_ent_t, tablename),
        CDB2_CSTRING, "indexname", -1, offsetof(fdb_systable_ent_t, indexname),
        CDB2_INTEGER, "rootpage", -1, offsetof(fdb_systable_ent_t, rootpage),
        CDB2_INTEGER, "remoterootpage", -1, offsetof(fdb_systable_ent_t, remoterootpage),
        CDB2_INTEGER, "version", -1, offsetof(fdb_systable_ent_t, version),
        SYSTABLE_END_OF_FIELDS);
}

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
