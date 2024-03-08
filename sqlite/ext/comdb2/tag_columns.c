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

struct systable_tag_col {
    char *tablename;
    char *tagname;
    char *colname;
    int64_t index;
    int64_t type;
    int64_t offset;
    int64_t length;
    int64_t datalength;
    int64_t flags;
    int64_t is_expr;
    char *defval;
    char *dbload;
    int64_t conv_flags;
    int64_t conv_dbpad;
    int64_t conv_step;
    int64_t blobindx;
};

int get_tag_columns_details(struct systable_tag_col *data, int ncols)
{
    sqlite3_int64 tableid;
    int sum, rc;
    struct dbtable *table;

    for (tableid = 0, sum = 0;
            comdb2_next_allowed_table(&tableid) == SQLITE_OK &&
            tableid < thedb->num_dbs && sum < ncols;
            ++tableid) {
        table = thedb->dbs[tableid];

        rc = get_table_tags(table->tablename, &data[sum], 1, ncols - sum);
        if (rc < 0)
            return -1;
        sum += rc;
    }

    return 0;
}


int get_tags_count(int columns);

static int collect_tag_cols(void **pd, int *pn)
{
    struct systable_tag_col *data = NULL;
    int ncols, rc;

    /* Get number of rows, and allocate a continuous chunk of memory. */
    ncols = get_tags_count(1);
    data = calloc(ncols, sizeof(struct systable_tag_col));

    rc = get_tag_columns_details(data, ncols);
    if (rc < 0) {
        *pd = NULL;
        *pn = 0;
    } else {
        *pd = data;
        *pn = ncols;
    }
    return rc;
}

int systable_set_tag_cols(struct systable_tag_col *parr, int num, int max,
                          const char * tblname, const struct  schema *s)
{
    int col;

    for (col = 0; col < s->nmembers; col++, num++) {
        assert(num < max);
        struct systable_tag_col *p = &parr[num];
        assert(col <= s->nmembers);
        struct field *f = &s->member[col];

        p->tablename = strdup(tblname);
        p->tagname = strdup(s->tag);
        p->colname = strdup(f->name);
        p->index = col;
        p->type = f->type;
        p->offset = f->offset;
        p->length = f->len;
        p->datalength = f->datalen;
        p->flags = f->flags;
        p->is_expr = f->isExpr;
        if (f->in_default)
            p->defval = sql_field_default_trans(f, 0);
        else
            p->defval = NULL;
        if (f->out_default)
            p->dbload = sql_field_default_trans(f, 1);
        else
            p->dbload = NULL;
        p->conv_flags = f->convopts.flags;
        p->conv_dbpad = f->convopts.dbpad;
        p->conv_step = f->convopts.step;
        p->blobindx = f->blob_index;
    }

    return col;
}

static void free_tag_cols(void *data, int n)
{
    struct systable_tag_col *cols = data, *p;
    int i;
    for (i = 0; i != n; ++i) {
        p = &cols[i];
        free(p->tablename);
        free(p->tagname);
        free(p->colname);
        sqlite3_free(p->defval);
        sqlite3_free(p->dbload);
    }
    free(cols);
}

sqlite3_module systblTagColsModule = {
  .access_flag = CDB2_ALLOW_ALL,
  .systable_lock = "comdb2_tables"
};

int systblTagColumnsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_tag_columns", &systblTagColsModule,
        collect_tag_cols, free_tag_cols, sizeof(struct systable_tag_col),
        CDB2_CSTRING, "tablename", -1, offsetof(struct systable_tag_col, tablename),
        CDB2_CSTRING, "tagname", -1, offsetof(struct systable_tag_col, tagname),
        CDB2_CSTRING, "name", -1, offsetof(struct systable_tag_col, colname),
        CDB2_INTEGER, "indx", -1, offsetof(struct systable_tag_col, index),
        CDB2_INTEGER, "type", -1, offsetof(struct systable_tag_col, type),
        CDB2_INTEGER, "offset", -1, offsetof(struct systable_tag_col, offset),
        CDB2_INTEGER, "length", -1, offsetof(struct systable_tag_col, length),
        CDB2_INTEGER, "datalength", -1, offsetof(struct systable_tag_col, datalength),
        CDB2_INTEGER, "flags", -1, offsetof(struct systable_tag_col, flags),
        CDB2_INTEGER, "expr", -1, offsetof(struct systable_tag_col, is_expr),
        CDB2_CSTRING, "defaultvalue", -1, offsetof(struct systable_tag_col, defval),
        CDB2_CSTRING, "dbload", -1, offsetof(struct systable_tag_col, dbload),
        CDB2_INTEGER, "conversionflags", -1, offsetof(struct systable_tag_col, conv_flags),
        CDB2_INTEGER, "conversiondbpad", -1, offsetof(struct systable_tag_col, conv_dbpad),
        CDB2_INTEGER, "conversionstep", -1, offsetof(struct systable_tag_col, conv_step),
        CDB2_INTEGER, "blobindx", -1, offsetof(struct systable_tag_col, blobindx),
        SYSTABLE_END_OF_FIELDS);
}

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
