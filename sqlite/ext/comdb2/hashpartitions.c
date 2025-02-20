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

#include "comdb2systbl.h"
#include "ezsystables.h"
#include "hash_partition.h"

sqlite3_module systblHashPartitionsModule = {
    .access_flag = CDB2_ALLOW_USER,
    .systable_lock = "comdb2_hashpartitions",
};

int systblHashPartitionsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_hashpartitions", &systblHashPartitionsModule,
        hash_systable_collect,
        hash_systable_free,  sizeof(systable_hashpartition_t),
        CDB2_CSTRING, "name", -1, offsetof(systable_hashpartition_t, name),
        CDB2_CSTRING, "shardname", -1, offsetof(systable_hashpartition_t, shardname),
        CDB2_INTEGER, "minKey", -1, offsetof(systable_hashpartition_t, minKey),
        CDB2_INTEGER, "maxKey", -1, offsetof(systable_hashpartition_t, maxKey),
        SYSTABLE_END_OF_FIELDS);
}
#endif
