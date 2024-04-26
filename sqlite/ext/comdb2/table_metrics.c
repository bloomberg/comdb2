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

#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include "comdb2.h"
#include "sqlite3.h"
#include "ezsystables.h"

extern struct dbenv *thedb;
sqlite3_module systblTableMetricsModule = {
    .access_flag = CDB2_ALLOW_ALL,
    .systable_lock = "comdb2_tables",
};

typedef struct systable_table_metrics {
    char *table_name;
    int64_t num_queries;
    int64_t num_index_used;
    int64_t num_records_read;
    int64_t num_records_inserted;
    int64_t num_records_updated;
    int64_t num_records_deleted;
} systable_table_metrics_t;

int get_table_metrics(void **data, int *nrecords) {
    *nrecords = thedb->num_dbs;
    struct dbtable **dbs = thedb->dbs;
    
    systable_table_metrics_t *systable = calloc(*nrecords, sizeof(systable_table_metrics_t));
    for (int i = 0; i < *nrecords; i++) {
        struct dbtable *db = dbs[i];

        systable[i].table_name = strdup(db->tablename);
        systable[i].num_queries = db->nsql;
        systable[i].num_index_used = db->index_used_count;
        systable[i].num_records_read = db->read_count;
        systable[i].num_records_inserted = db->write_count[RECORD_WRITE_INS];
        systable[i].num_records_updated = db->write_count[RECORD_WRITE_UPD];
        systable[i].num_records_deleted = db->write_count[RECORD_WRITE_DEL];
    }

    *data = systable;
    return 0;
}

void free_table_metrics(void *data, int nrecords) {
    systable_table_metrics_t *systable = (systable_table_metrics_t *)data;

    for (int i = 0; i < nrecords; i++) {
        free(systable[i].table_name);
    }

    free(data);
}

int systblTableMetricsInit(sqlite3 *db) {
    return create_system_table(
        db, "comdb2_table_metrics", &systblTableMetricsModule,
        get_table_metrics, free_table_metrics, sizeof(systable_table_metrics_t),
        CDB2_CSTRING, "table_name", -1, offsetof(systable_table_metrics_t, table_name),
        CDB2_INTEGER, "num_queries", -1, offsetof(systable_table_metrics_t, num_queries),
        CDB2_INTEGER, "num_index_used", -1, offsetof(systable_table_metrics_t, num_index_used),
        CDB2_INTEGER, "num_records_read", -1, offsetof(systable_table_metrics_t, num_records_read),
        CDB2_INTEGER, "num_records_inserted", -1, offsetof(systable_table_metrics_t, num_records_inserted),
        CDB2_INTEGER, "num_records_updated", -1, offsetof(systable_table_metrics_t, num_records_updated),
        CDB2_INTEGER, "num_records_deleted", -1, offsetof(systable_table_metrics_t, num_records_deleted),
        SYSTABLE_END_OF_FIELDS);
}
