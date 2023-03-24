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
#include <string.h>
#include <stddef.h>

#include "comdb2.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include "types.h"

#include "bdb_api.h"

static int get_cluster(void **data, int *num_points) {
    return bdb_fill_cluster_info(data, num_points);
}

static void free_cluster(void *data, int num_points) {
    struct cluster_info *info = (struct cluster_info*) data;
    for (int i = 0; i < num_points; i++) {
        free(info[i].host);
    }
    free(info);
}

sqlite3_module systblClusterModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int systblClusterInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_cluster", &systblClusterModule,
            get_cluster, free_cluster, sizeof(struct cluster_info),
            CDB2_CSTRING, "host", -1, offsetof(struct cluster_info, host),
            CDB2_INTEGER, "port",  -1, offsetof(struct cluster_info, port),
            CDB2_CSTRING, "is_master",  -1, offsetof(struct cluster_info, is_master),
            CDB2_CSTRING, "coherent_state",  -1, offsetof(struct cluster_info, coherent_state),
            CDB2_INTEGER, "logfile",  -1, offsetof(struct cluster_info, logfile),
            CDB2_INTEGER, "logoffset",  -1, offsetof(struct cluster_info, logoffset),
            SYSTABLE_END_OF_FIELDS);
}
