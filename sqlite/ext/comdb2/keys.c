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

struct systable_key {
    char *table;
    char *key;
    int64_t keynum;
    char *unique;
    char *datacopy;
    char *recnum;
    char *uniqnulls;
    char *condition;
    char *partialdatacopy;
};

static int collect_keys(void **pd, int *pn)
{
    int ntables = 0;
    sqlite3_int64 tableid = 0;
    sqlite3_int64 ixid = 0;
    struct dbtable *pDb = NULL;
    struct schema *pSchema = NULL;
    struct systable_key *data = NULL, *p = NULL;
    int ncols = 0;

    ntables = timepart_systable_num_tables_and_views();
    for (; comdb2_next_allowed_table(&tableid) == SQLITE_OK && tableid < ntables; ++tableid) {
        pDb = comdb2_get_dbtable_or_shard0(tableid);
        for (ixid = 0; ixid < pDb->schema->nix; ++ixid) {
            if (pDb->ixsql[ixid] == NULL)
                continue;
            pSchema = pDb->ixschema[ixid];
            data = realloc(data, sizeof(struct systable_key) * (ncols + 1));
            p = &data[ncols];
            p->table = strdup(pDb->timepartition_name ? pDb->timepartition_name : pDb->tablename);
            p->key = strdup(pSchema->csctag);
            p->keynum = pSchema->ixnum;
            p->unique = YESNO(!(pSchema->flags & SCHEMA_DUP));
            p->datacopy = YESNO(pSchema->flags & SCHEMA_DATACOPY);
            p->recnum = YESNO(pSchema->flags & SCHEMA_RECNUM);
            p->uniqnulls = YESNO(pSchema->flags & SCHEMA_UNIQNULLS);
            p->condition = pSchema->where ? strdup(pSchema->where) : NULL;
            p->partialdatacopy = YESNO(pSchema->flags & SCHEMA_PARTIALDATACOPY);
            ++ncols;
        }
    }

    *pn = ncols;
    *pd = data;

    return 0;
}

static void free_keys(void *data, int n)
{
    struct systable_key *keys = data, *p;
    int i;
    for (i = 0; i != n; ++i) {
        p = &keys[i];
        free(p->table);
        free(p->key);
        free(p->condition);
    }
    free(keys);
}

sqlite3_module systblKeysModule = {
  .access_flag = CDB2_ALLOW_ALL,
  .systable_lock = "comdb2_tables",
};

int systblKeysInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_keys", &systblKeysModule,
        collect_keys, free_keys, sizeof(struct systable_key),
        CDB2_CSTRING, "tablename", -1, 0,
        CDB2_CSTRING, "keyname", -1, offsetof(struct systable_key, key),
        CDB2_INTEGER, "keynumber", -1, offsetof(struct systable_key, keynum),
        CDB2_CSTRING, "isunique", -1, offsetof(struct systable_key, unique),
        CDB2_CSTRING, "isdatacopy", -1, offsetof(struct systable_key, datacopy),
        CDB2_CSTRING, "isrecnum", -1, offsetof(struct systable_key, recnum),
        CDB2_CSTRING, "condition", -1, offsetof(struct systable_key, condition),
        CDB2_CSTRING, "uniqnulls", -1, offsetof(struct systable_key, uniqnulls),
        CDB2_CSTRING, "ispartialdatacopy", -1, offsetof(struct systable_key, partialdatacopy),
        SYSTABLE_END_OF_FIELDS);
}

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
