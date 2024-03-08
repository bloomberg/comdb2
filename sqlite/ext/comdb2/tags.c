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


#if (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
    && !defined(SQLITE_OMIT_VIRTUALTABLE)

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
# define SQLITE_CORE 1
#endif

#include <stdlib.h>
#include <string.h>

#include "comdb2.h"
#include "sql.h"
#include "tag.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "ezsystables.h"

struct systable_tag {
    char *tablename;
    char *tagname;
    int ixnum;
    int64_t recsize;
    int64_t nmembers;
    char *sqlitetag;
    char *csctag;
    int numblobs;
    int64_t nix;
};

int get_tags_count(int columns)
{
    sqlite3_int64 tableid = 0;
    int ntags;

    for (tableid = 0, ntags = 0;
            comdb2_next_allowed_table(&tableid) == SQLITE_OK && tableid < thedb->num_dbs;
            ++tableid) {
        ntags += get_table_tags_count(thedb->dbs[tableid]->tablename, columns);
    }

    return ntags;
}

int get_tags_details(struct systable_tag *data, int ntags)
{
    sqlite3_int64 tableid;
    int sum, rc;
    struct dbtable *table;

    for (tableid = 0, sum = 0;
            comdb2_next_allowed_table(&tableid) == SQLITE_OK &&
            tableid < thedb->num_dbs && sum < ntags;
            ++tableid) {
        table = thedb->dbs[tableid];

        rc = get_table_tags(table->tablename, &data[sum], 0, ntags - sum);
        if (rc < 0)
            return -1;
        sum += rc;
    }

    return 0;
}

static int collect_tags(void **pd, int *pn)
{
    struct systable_tag *data = NULL;
    int ntags, rc;

    /* Get number of rows, and allocate a continuous chunk of memory. */
    ntags = get_tags_count(0);
    data = calloc(ntags, sizeof(struct systable_tag));
     
    rc = get_tags_details(data, ntags);
    if (rc < 0) {
        *pd = NULL;
        *pn = 0;
    } else {
        *pd = data;
        *pn = ntags;
    }
    return rc;
}


int systable_set_tag(struct systable_tag *parr, int num, const char * tblname,
                     const struct  schema *s)
{
    struct systable_tag *p = &parr[num];
    p->tablename = strdup(tblname);
    p->tagname = strdup(s->tag);
    p->ixnum = s->ixnum;
    p->recsize = s->recsize;
    p->nmembers = s->nmembers;
    p->sqlitetag = strdup(s->sqlitetag ? s->sqlitetag : "");
    p->csctag = strdup(s->csctag ? s->csctag : "");
    p->numblobs = s->numblobs;
    p->nix = s->nix;
    return 1;
}

static void free_tags(void *data, int n)
{
    struct systable_tag *tags= data, *p;
    int i;
    for (i = 0; i < n; ++i) {
        p = &tags[i];
        free(p->tablename);
        free(p->tagname);
        free(p->sqlitetag);
        free(p->csctag);
    }
    free(tags);
}

sqlite3_module systblTagsModule = {
  .access_flag = CDB2_ALLOW_ALL,
  .systable_lock = "comdb2_tables"
};

int systblTagsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_tags", &systblTagsModule,
        collect_tags, free_tags, sizeof(struct systable_tag),
        CDB2_CSTRING, "tablename", -1, 0,
        CDB2_CSTRING, "tagname", -1, offsetof(struct systable_tag, tagname),
        CDB2_INTEGER, "ixnum", -1, offsetof(struct systable_tag, ixnum),
        CDB2_INTEGER, "size", -1, offsetof(struct systable_tag, recsize),
        CDB2_INTEGER, "numcolumns", -1, offsetof(struct systable_tag, nmembers),
        CDB2_CSTRING, "sqlitekeyname", -1, offsetof(struct systable_tag, sqlitetag),
        CDB2_CSTRING, "keyname", -1, offsetof(struct systable_tag, csctag),
        CDB2_INTEGER, "numblobs", -1, offsetof(struct systable_tag, numblobs),
        CDB2_INTEGER, "numindexes", -1, offsetof(struct systable_tag, nix),
        SYSTABLE_END_OF_FIELDS);
}

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
