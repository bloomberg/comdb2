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

#if (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) &&          \
    !defined(SQLITE_OMIT_VIRTUALTABLE)

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
#define SQLITE_CORE 1
#endif

#include <assert.h>
#include "sqliteInt.h"
#include "hash.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"

static sqlite3_module systblSystablesModule = {
    .access_flag = CDB2_ALLOW_USER,
};

struct systable {
    char *name;
};

extern pthread_key_t query_info_key;

static int get_tables(void **data, int *npoints) {
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    sqlite3 *sqldb = thd->clnt->thd->sqldb;
    struct systable *systables;

    *npoints = sqldb->aModule.count;
    for (int i = 0; i < thedb->num_dbs; i++) {
        struct dbtable *db = thedb->dbs[i];
        if (db->disallow_drop)
            (*npoints)++;
    }
    systables = malloc(sizeof(struct systable) * *npoints);
    int i;

    HashElem *p;
    for (p = sqliteHashFirst(&sqldb->aModule), i = 0; p; p = sqliteHashNext(p), i++) {
        systables[i].name = strdup(p->pKey);
    }
    for (int tbl = 0; tbl < thedb->num_dbs; tbl++) {
        struct dbtable *db = thedb->dbs[tbl];
        if (db->disallow_drop)
            systables[i++].name = db->tablename;
    }
    *data = systables;
    return 0;
}

static void free_tables(void *p, int n) {
    struct systable *t = (struct systable*) p;
    for (int i = 0; i < n; i++) {
        free(t[i].name);
    }
    free(t);
}

int systblSystablesInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_systables",
            &systblSystablesModule, get_tables, free_tables,
            sizeof(struct systable),
            CDB2_CSTRING, "name", -1, offsetof(struct systable, name),
            SYSTABLE_END_OF_FIELDS);
}




#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
