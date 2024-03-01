/*
   Copyright 2022 Bloomberg Finance L.P.

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

#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include "comdb2.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include "types.h"

sqlite3_module systblMemstatsModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int get_usages(void **data, int *num_points) {
#ifdef USE_SYS_ALLOC
    (*num_points) = 0;
    return 0;
#else
    return comdb2ma_usages((comdb2ma_usage **)data, num_points);
#endif
}

void free_usages(void *data, int num_points) {
    free(data);
}

int systblMemstatsInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_memstats",
            &systblMemstatsModule, get_usages, free_usages, sizeof(comdb2ma_usage),
            CDB2_CSTRING, "name", -1, offsetof(comdb2ma_usage, name),
            CDB2_CSTRING, "scope", -1, offsetof(comdb2ma_usage, scope),
            CDB2_INTEGER, "total", -1, offsetof(comdb2ma_usage, total),
            CDB2_INTEGER, "used", -1, offsetof(comdb2ma_usage, used),
            CDB2_INTEGER, "free", -1, offsetof(comdb2ma_usage, unused),
            CDB2_INTEGER, "peak", -1, offsetof(comdb2ma_usage, peak),
            SYSTABLE_END_OF_FIELDS);
}
