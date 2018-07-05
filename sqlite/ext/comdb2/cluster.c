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

int systblClusterInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_cluster", get_cluster, free_cluster, sizeof(struct cluster_info),
            CDB2_CSTRING, "host", offsetof(struct cluster_info, host),
            CDB2_INTEGER, "port",  offsetof(struct cluster_info, port),
            CDB2_CSTRING, "is_master",  offsetof(struct cluster_info, is_master),
            CDB2_CSTRING, "coherent_state",  offsetof(struct cluster_info, coherent_state),
            SYSTABLE_END_OF_FIELDS);
}
