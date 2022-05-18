// Copyright 2022 Bloomberg Finance L.P.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <sqlite3.h>
#include <comdb2.h>
#include <ezsystables.h>
#include <comdb2systblInt.h>

static sqlite3_module systblCsc2Module = {
        .access_flag = CDB2_ALLOW_USER,
        .systable_lock = "comdb2_tables"
};

struct csc2 {
    char *tablename;
    char *schema;
};

int collect_csc2(void **data, int *count) {
    sqlite_int64 id = 0;
    *count = 0;
    struct csc2 *csc2 = NULL;
    int rc;

    // we may overallocate here, alternative is looping through the tables twice
    csc2 = malloc(sizeof(struct csc2) * thedb->num_dbs);

    while ((rc = comdb2_next_allowed_table(&id)) == SQLITE_OK && id < thedb->num_dbs) {
        csc2[*count].tablename = strdup(thedb->dbs[id]->tablename);
        csc2[*count].schema = strdup(thedb->dbs[id]->csc2_schema);
        (*count)++;
        id++;
    }
    if (rc) {
        free(csc2);
        return rc;
    }
    *data = csc2;
    return 0;
}

void release_csc2(void *data, int count) {
    struct csc2 *csc2 = (struct csc2*) data;
    for (int i = 0; i < count; i++) {
        free(csc2[i].tablename);
        free(csc2[i].schema);
    }
    free(data);
}

int systblCsc2Init(sqlite3 *db) {
    create_system_table(db, "comdb2_csc2", &systblCsc2Module, collect_csc2, release_csc2, sizeof(struct csc2),
                        CDB2_CSTRING, "tablename", -1, offsetof(struct csc2, tablename),
                        CDB2_CSTRING, "schema", -1, offsetof(struct csc2, schema),
                        SYSTABLE_END_OF_FIELDS);
    return 0;
}