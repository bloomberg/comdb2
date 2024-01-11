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
** built as part of SQLITE_CORE, as comdb2 does not support
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

extern pthread_rwlock_t views_lk;

struct systable_column {
    char *tablename;
    char *columnname;
    char *type;
    int64_t size;
    char *sqltype;
    int64_t *pinlinesz;
    int64_t inlinesz;
    char *defval;
    char *dbload;
    char *allownull;
    int64_t *pnextseq;
    int64_t nextseq;
};

static int collect_columns(void **pd, int *pn)
{
    int ntables = 0;
    sqlite3_int64 tableid = 0;
    sqlite3_int64 colid = 0;
    struct dbtable *pDb = NULL;
    struct field *pField = NULL;
    struct systable_column *data = NULL, *p = NULL;
    int ncols = 0;

    Pthread_rwlock_rdlock(&views_lk);

    ntables = timepart_systable_num_tables_and_views();

    /* Get number of rows, and allocate a continuous chunk of memory. */
    for (tableid = 0, ncols = 0; comdb2_next_allowed_table(&tableid) == SQLITE_OK && tableid < ntables; ++tableid) {
        pDb = comdb2_get_dbtable_or_shard0(tableid);
        ncols += pDb->schema->nmembers;
    }

    data = calloc(ncols, sizeof(struct systable_column));

    for (tableid = 0, ncols = 0; comdb2_next_allowed_table(&tableid) == SQLITE_OK && tableid < ntables; ++tableid) {
        pDb = comdb2_get_dbtable_or_shard0(tableid);
        for (colid = 0; colid < pDb->schema->nmembers; ++colid) {
            pField = &pDb->schema->member[colid];
            p = &data[ncols];
            p->tablename = strdup(pDb->timepartition_name ? pDb->timepartition_name : pDb->tablename);
            p->columnname = strdup(pField->name);
            p->type = strdup(csc2type(pField));
            p->size = pField->len;
            p->sqltype = sqlite3_malloc(15); /* sizeof("interval month") == 15 */
            sqltype(pField, p->sqltype, 15);
            if (pField->type == SERVER_BLOB2 || pField->type == SERVER_VUTF8) {
                p->inlinesz = pField->len - 5;
                p->pinlinesz = &p->inlinesz; /* reference myself; must update if relocated */
            } else {
                p->inlinesz = -1;
                p->pinlinesz = NULL;
            }

            if (pField->in_default) {
                p->defval = sql_field_default_trans(pField, 0);
            } else {
                p->defval = NULL;
            }
            if (pField->out_default) {
                p->dbload = sql_field_default_trans(pField, 1);
            } else {
                p->dbload = NULL;
            }

            p->allownull = YESNO(!(pField->flags & NO_NULL));
            if (pField->in_default_type != SERVER_SEQUENCE) {
                p->nextseq = -1;
                p->pnextseq = NULL;
            } else {
                tran_type *trans = curtran_gettran();
                int64_t seq;
                int bdberr;
                int rc = bdb_get_sequence(trans, pDb->tablename, pField->name, &seq, &bdberr);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "bdb_get_sequence %s %s -> rc %d bdberr %d\n", pDb->tablename, pField->name, rc, bdberr);
                    p->nextseq = -1;
                } else {
                    p->nextseq = seq + 1;
                }
                p->pnextseq = &p->nextseq; /* reference myself; must update if relocated */
                curtran_puttran(trans);
            }
            ++ncols;
        }
    }

    Pthread_rwlock_unlock(&views_lk);

    *pn = ncols;
    *pd = data;

    return 0;
}

static void free_columns(void *data, int n)
{
    struct systable_column *columns = data, *p;
    int i;
    for (i = 0; i != n; ++i) {
        p = &columns[i];
        free(p->tablename);
        free(p->columnname);
        free(p->type);
        sqlite3_free(p->sqltype);
        sqlite3_free(p->defval);
        sqlite3_free(p->dbload);
    }
    free(columns);
}

sqlite3_module systblColumnsModule = {
  .access_flag = CDB2_ALLOW_ALL,
  .systable_lock = "comdb2_tables"
};

int systblColumnsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_columns", &systblColumnsModule,
        collect_columns, free_columns, sizeof(struct systable_column),
        CDB2_CSTRING, "tablename", -1, 0,
        CDB2_CSTRING, "columnname", -1, offsetof(struct systable_column, columnname),
        CDB2_CSTRING, "type", -1, offsetof(struct systable_column, type),
        CDB2_INTEGER, "size", -1, offsetof(struct systable_column, size),
        CDB2_CSTRING, "sqltype", -1, offsetof(struct systable_column, sqltype),
        CDB2_INTEGER | SYSTABLE_FIELD_NULLABLE, "varinlinesize", -1, offsetof(struct systable_column, pinlinesz),
        CDB2_CSTRING, "defaultvalue", -1, offsetof(struct systable_column, defval),
        CDB2_CSTRING, "dbload", -1, offsetof(struct systable_column, dbload),
        CDB2_CSTRING, "isnullable", -1, offsetof(struct systable_column, allownull),
        CDB2_INTEGER | SYSTABLE_FIELD_NULLABLE, "lastsequence", -1, offsetof(struct systable_column, pnextseq),
        SYSTABLE_END_OF_FIELDS);
}

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
