/*
   Copyright 2021 Bloomberg Finance L.P.

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

#include <comdb2systblInt.h>
#include <ezsystables.h>
#include "progress_tracker.h"

sqlite3_module systblProgressModule = {
    .access_flag = CDB2_ALLOW_USER,
    .systable_lock = "comdb2_progress",
};

// clang-format off
int systblProgressInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_progress", &systblProgressModule,
        progress_tracker_copy_data, progress_tracker_release_data, sizeof(progress_entry_t),
        CDB2_CSTRING, "name", -1, offsetof(progress_entry_t, name),
        CDB2_CSTRING, "sub_name", -1, offsetof(progress_entry_t, sub_name),
        CDB2_CSTRING, "type", -1, offsetof(progress_entry_t, type),
        CDB2_INTEGER, "thread_id", -1, offsetof(progress_entry_t, thread_id),
        CDB2_INTEGER, "thread_sub_id", -1, offsetof(progress_entry_t, thread_sub_id),
        CDB2_CSTRING, "seed", -1, offsetof(progress_entry_t, seed),
        CDB2_CSTRING, "stage", -1, offsetof(progress_entry_t, stage),
        CDB2_CSTRING, "status", -1, offsetof(progress_entry_t, status),
        CDB2_DATETIME, "start_time", -1, offsetof(progress_entry_t, start_time),
        CDB2_DATETIME, "end_time", -1, offsetof(progress_entry_t, end_time),
        CDB2_INTEGER, "processed_records", -1, offsetof(progress_entry_t, processed_records),
        CDB2_INTEGER, "total_records", -1, offsetof(progress_entry_t, total_records),
        CDB2_INTEGER, "rate", -1, offsetof(progress_entry_t, rate_per_sec),
        CDB2_INTEGER, "remaining_time", -1, offsetof(progress_entry_t, remaining_time_sec),
        SYSTABLE_END_OF_FIELDS);
}
// clang-format on
#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
