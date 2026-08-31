/*
   Copyright 2026 Bloomberg Finance L.P.

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

/*
**
** Vtables interface for Schema Tables.
**
** Though this is technically an extension, currently it must be
** built as part of SQLITE_CORE, as comdb2 does not support
** run time extensions at this time.
**
** We have piggy backed off of SQLITE_BUILDING_FOR_COMDB2 here, though
** a new #define would also suffice.
*/
#if (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
    && !defined(SQLITE_OMIT_VIRTUALTABLE)

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
# define SQLITE_CORE 1
#endif

#include <stdlib.h>
#include <stddef.h>

#include "comdb2.h"
#include "comdb2systblInt.h"
#include "views.h"
#include "ezsystables.h"

struct systable_schema {
    char *tablename;
    char *csc2;
};

static int collect_schemas(void **pd, int *pn)
{
    int ntables = 0;
    int nrows = 0;
    sqlite3_int64 tableid = 0;
    struct systable_schema *data = NULL;

    ntables = thedb->num_dbs + timepart_num_views();

    data = calloc(ntables, sizeof(struct systable_schema));
    if (!data && ntables > 0)
        return SQLITE_NOMEM;

    for (; comdb2_next_allowed_table(&tableid) == SQLITE_OK && tableid < ntables; ++tableid) {
        struct dbtable *pDb = comdb2_get_dbtable_or_shard0(tableid);
        const char *name = pDb->timepartition_name ? pDb->timepartition_name : pDb->tablename;

        if (!name)
            continue;

        data[nrows].tablename = strdup(name);
        data[nrows].csc2 = pDb->csc2_schema ? strdup(pDb->csc2_schema) : NULL;
        ++nrows;
    }

    *pn = nrows;
    *pd = data;

    return 0;
}

static void free_schemas(void *data, int n)
{
    struct systable_schema *schemas = data;
    int i;
    for (i = 0; i != n; ++i) {
        free(schemas[i].tablename);
        free(schemas[i].csc2);
    }
    free(data);
}

sqlite3_module systblSchemasModule = {
    .access_flag = CDB2_ALLOW_ALL,
    .systable_lock_count = 1,
    .systable_locks = (const char *[]){ "comdb2_tables" }
};

int systblSchemasInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_schemas", &systblSchemasModule,
        collect_schemas, free_schemas, sizeof(struct systable_schema),
        CDB2_CSTRING, "tablename", -1, offsetof(struct systable_schema, tablename),
        CDB2_CSTRING, "csc2", -1, offsetof(struct systable_schema, csc2),
        SYSTABLE_END_OF_FIELDS);
}

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
