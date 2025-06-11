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

#include <stdlib.h>
#include <stddef.h>

#include "comdb2.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "ezsystables.h"
#include "sql.h"
#include "util.h"

static sqlite3_module systblLocationsModule = {
        .access_flag = CDB2_ALLOW_ALL,
};

int get_locations(void **data, int *npoints) {
    struct location *locations;
    int nlocations = fetch_file_locations(&locations);
    if (nlocations == -1)
        return -1;
    *npoints = nlocations;
    *data = locations;
    return 0 ;
}

void free_locations(void *data, int npoints) {
    struct location *locations = (struct location *) data;
    for (int i = 0; i < npoints; i++) {
        free(locations[i].type);
        free(locations[i].dir);
    }
    free(locations);
}

int systblLocationsInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_locations",
                               &systblLocationsModule, get_locations, free_locations,
                               sizeof(struct location),
                               CDB2_CSTRING, "type", -1, offsetof(struct location, type),
                               CDB2_CSTRING, "path", -1, offsetof(struct location, dir),
                               SYSTABLE_END_OF_FIELDS);
}
