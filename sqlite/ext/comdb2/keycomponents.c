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


/*
**
** Vtables interface for Schema Tables.
**
** Though this is technically an extension, currently it must be
** run time extensions at this time.
**
** For a little while we had to use our own "fake" tables, because
** eponymous system tables did not exist. Now that they do, we
** have moved schema tables to their own extension.
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
#include <string.h>

#include "comdb2.h"
#include "sql.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "views.h"
#include "timepart_systable.h"
#include "ezsystables.h"

struct systable_keycomponent {
    char *table;
    char *key;
    int64_t colno;
    char *colname;
    char *dec;
};

static int collect_keycomponents(void **pd, int *pn)
{
    int ntables = 0;
    sqlite3_int64 tableid = 0;
    sqlite3_int64 ixid = 0;
    sqlite3_int64 keyid = 0;
    struct dbtable *pDb = NULL;
    struct schema *pSchema = NULL;
    struct field *pField = NULL;
    struct systable_keycomponent *data = NULL, *p = NULL;
    int ncols = 0;

    ntables = timepart_systable_num_tables_and_views();
    for (; comdb2_next_allowed_table(&tableid) == SQLITE_OK && tableid < ntables; ++tableid) {
        pDb = comdb2_get_dbtable_or_shard0(tableid);
        for (ixid = 0; ixid < pDb->schema->nix; ++ixid) {
            if (pDb->ixsql[ixid] == NULL)
                continue;
            pSchema = pDb->ixschema[ixid];
            for (keyid = 0; keyid < pSchema->nmembers; ++keyid) {
                pField = &pSchema->member[keyid];
                data = realloc(data, sizeof(struct systable_keycomponent) * (ncols + 1));
                p = &data[ncols];
                p->table = strdup(pDb->timepartition_name ? pDb->timepartition_name : pDb->tablename);
                p->key = strdup(pSchema->csctag);
                p->colno = keyid;
                p->colname = strdup(pField->name);
                p->dec = YESNO(pField->flags & INDEX_DESCEND);
                ++ncols;
            }
        }
    }

    *pn = ncols;
    *pd = data;

    return 0;

}

static void free_keycomponents(void *data, int n)
{
    struct systable_keycomponent *keycomponents = data, *p;
    int i;
    for (i = 0; i != n; ++i) {
        p = &keycomponents[i];
        free(p->table);
        free(p->key);
        free(p->colname);
    }
    free(keycomponents);
}

sqlite3_module systblKeyComponentsModule = {
  .access_flag = CDB2_ALLOW_ALL,
  .systable_lock = "comdb2_tables",
};

int systblKeyComponentsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_keycomponents", &systblKeyComponentsModule,
        collect_keycomponents, free_keycomponents, sizeof(struct systable_keycomponent),
        CDB2_CSTRING, "tablename", -1, 0,
        CDB2_CSTRING, "keyname", -1, offsetof(struct systable_keycomponent, key),
        CDB2_INTEGER, "columnnumber", -1, offsetof(struct systable_keycomponent, colno),
        CDB2_CSTRING, "columnname", -1, offsetof(struct systable_keycomponent, colname),
        CDB2_CSTRING, "isdescending", -1, offsetof(struct systable_keycomponent, dec),
        SYSTABLE_END_OF_FIELDS);
}

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
