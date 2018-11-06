/*
   Copyright 2018 Bloomberg Finance L.P.

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

#define SQLITE_CORE 1

#include <comdb2systblInt.h>
#include <ext/comdb2/ezsystables.h>

static int fingerprints_callback(void **data, int *npoints)
{
    *npoints = 0;
    *data = NULL;
    return 0;
}

static void release_callback(void *data, int npoints)
{
}

int systblFingerprintsInit(sqlite3 *db)
{
    return create_system_table(db,
        "comdb2_fingerprints",
        fingerprints_callback, release_callback, 0,
        CDB2_CSTRING, "fingerprint", -1, 0,
        CDB2_INTEGER, "count", -1, 0,
        CDB2_INTEGER, "total_cost", -1, 0,
        CDB2_INTEGER, "total_time", -1, 0,
        CDB2_INTEGER, "total_rows", -1, 0,
        CDB2_CSTRING, "normalized_sql", -1, 0,
        SYSTABLE_END_OF_FIELDS);
}
