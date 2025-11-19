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
#include "phys_rep.h"

typedef struct systable_physrep_altmetadb_t {
    char *dbname;
    char *host;
} systable_physrep_altmetadb_t;
 
static int collect_physrep_alt_metadbs(void **pdata, int *pn) {
    systable_physrep_altmetadb_t *rows;
    int nrows = 0;

    extern int gbl_altmetadb_count;
    extern struct metadb gbl_altmetadb[MAX_ALTERNATE_METADBS];
    
    if (gbl_altmetadb_count == 0) {
        *pdata = NULL;
        *pn = 0;
        return 0;
    }
    
    rows = calloc(gbl_altmetadb_count, sizeof(systable_physrep_altmetadb_t));
    if (!rows) return -1;
    
    // Populate rows from gbl_altmetadb array
    for (int i = 0; i < gbl_altmetadb_count; i++) {
        rows[i].dbname = strdup(gbl_altmetadb[i].dbname);
        rows[i].host = strdup(gbl_altmetadb[i].host);
        nrows++;
    }
    *pdata = rows;
    *pn = nrows;
    return 0;
}


static void free_physrep_altmetadb(void *data, int nrows) {
    systable_physrep_altmetadb_t *rows = (systable_physrep_altmetadb_t *)data;
    
    for (int i = 0; i < nrows; i++) {
        free(rows[i].dbname);
        free(rows[i].host);
    }
    free(rows);
}

sqlite3_module systblPhysrepAltmetadbModule = {
    .access_flag = CDB2_ALLOW_ALL,
};

int systblPhysrepAltmetadbInit(sqlite3 *db) {
    return create_system_table(
        db, "comdb2_physrep_altmetadb", &systblPhysrepAltmetadbModule,
        collect_physrep_alt_metadbs, free_physrep_altmetadb,
        sizeof(systable_physrep_altmetadb_t),
        CDB2_CSTRING, "dbname", -1, offsetof(systable_physrep_altmetadb_t, dbname),
        CDB2_CSTRING, "host", -1, offsetof(systable_physrep_altmetadb_t, host),
        SYSTABLE_END_OF_FIELDS
    );
}
