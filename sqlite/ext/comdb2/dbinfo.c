/*
   Copyright 2024 Bloomberg Finance L.P.

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

#include <assert.h>
#include "sqliteInt.h"
#include "comdb2systblInt.h"
#include "comdb2.h"
#include "tag.h"
#include "ezsystables.h"

static sqlite3_module systblDbInfoModule = {
    .access_flag = CDB2_ALLOW_ALL,
    .systable_lock = "comdb2_tables",
};

int systblDbInfoInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_dbinfo", &systblDbInfoModule,
            collect_systable_dbinfo, release_systable_dbinfo, sizeof(struct dbinfo_systable_entry), 
            CDB2_CSTRING, "table_name", -1, offsetof(struct dbinfo_systable_entry, table_name),
            CDB2_INTEGER, "lux", -1, offsetof(struct dbinfo_systable_entry, lux),
            CDB2_INTEGER, "lrl", -1, offsetof(struct dbinfo_systable_entry, lrl),
            CDB2_INTEGER, "ix", -1, offsetof(struct dbinfo_systable_entry, ix),
            CDB2_INTEGER, "keylen", -1, offsetof(struct dbinfo_systable_entry, keylen),
            SYSTABLE_END_OF_FIELDS
            );
}


#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
