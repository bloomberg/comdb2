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
#include "sqlite3.h"
#include "ezsystables.h"

extern struct dbenv *thedb;

sqlite3_module systblPartialDatacopiesModule = {
    .access_flag = CDB2_ALLOW_USER,
    .systable_lock = "comdb2_tables",
};

typedef struct systable_partial_datacopies {
    char *tablename;
    char *keyname;
    char *columnname;
} systable_partial_datacopies_t;

int partial_datacopies_systable_collect(void **data, int *nrecords)
{
    // first find number of records
    *nrecords = 0;
    struct dbtable *db;
    struct schema *schema;
    for (int i = 0; i < thedb->num_dbs; i++) {
        db = thedb->dbs[i];
        for (int j = 0; j < db->nix; j++) {
            schema = db->ixschema[j];
            if (schema->flags & SCHEMA_PARTIALDATACOPY) {
                *nrecords += schema->partial_datacopy->nmembers;
            }
        }
    }

    int idx = 0;
    struct field *member;
    systable_partial_datacopies_t *arr = calloc(*nrecords, sizeof(systable_partial_datacopies_t));
    for (int i = 0; i < thedb->num_dbs; i++) {
        db = thedb->dbs[i];
        for (int j = 0; j < db->nix; j++) {
            schema = db->ixschema[j];
            if (schema->flags & SCHEMA_PARTIALDATACOPY) {
                for (int k = 0; k < schema->partial_datacopy->nmembers; k++) {
                    member = &schema->partial_datacopy->member[k];
                    arr[idx].tablename = strdup(db->tablename);
                    arr[idx].keyname = strdup(schema->csctag);
                    arr[idx].columnname = strdup(member->name);
                    idx++;
                }
            }
        }
    }

    *data = arr;
    return 0;
}

void partial_datacopies_systable_free(void *arr, int nrecords)
{
    systable_partial_datacopies_t *parr = (systable_partial_datacopies_t *)arr;
    int i;

    for (i = 0; i < nrecords; i++) {
        free(parr[i].tablename);
        free(parr[i].keyname);
        free(parr[i].columnname);
    }
    free(arr);
}

int systblPartialDatacopiesInit(sqlite3*db)
{
    return create_system_table(
        db, "comdb2_partial_datacopies", &systblPartialDatacopiesModule,
        partial_datacopies_systable_collect, partial_datacopies_systable_free,
        sizeof(systable_partial_datacopies_t),
        CDB2_CSTRING, "tablename", -1, offsetof(systable_partial_datacopies_t, tablename),
        CDB2_CSTRING, "keyname", -1, offsetof(systable_partial_datacopies_t, keyname),
        CDB2_CSTRING, "columnname", -1, offsetof(systable_partial_datacopies_t, columnname),
        SYSTABLE_END_OF_FIELDS);
}
