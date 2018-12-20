/*
   Copyright 2017, 2018 Bloomberg Finance L.P.

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
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "cdb2_constants.h"
#include "csc2/dynschemaload.h" //MAXIDXNAMELEN
#include "sqliteLimit.h"

struct limit_t {
    const char *name;
    const char *descr;
    int value;
} limits[] = {
    {"max_blob_fields", "Maximum number of blob/vutf8 fields per table",
     MAXBLOBS},
    {"max_blob_length", "Maximum blob length", MAXBLOBLENGTH},
    {"max_bounded_parameters",
     "Maximum number of bounded parameters per prepared statement",
     MAXDYNTAGCOLUMNS},
    {"max_column_name_length", "Maximum column name length", MAXCOLNAME},
    {"max_columns", "Maximum columns per table", MAXCOLUMNS},
    {"max_constraints", "Maximum number of constraints per table",
     MAXCONSTRAINTS},
    {"max_consumers", "Maximum queue consumers per table", MAXCONSUMERS},
    {"max_database_name_length", "Maximum length of database name",
     MAX_DBNAME_LENGTH},
    {"max_data_files", "Maximum number of data files per table", MAXDTAFILES},
    {"max_data_stipes", "Maximum number of data stripes per table",
     MAXDTASTRIPE},
    {"max_expr_depth", "Maximum depth of an expression tree",
     SQLITE_MAX_EXPR_DEPTH},
    {"max_indexes", "Maximum number of indexes per table", MAXINDEX},
    {"max_key_length", "Maximum key length", MAXKEYLEN},
    {"max_key_name_length", "Maximum key name length", MAXIDXNAMELEN - 1},
    {"max_networks", "Maximum number of networks", MAXNETS},
    {"max_nodes", "Maximum number of nodes", REPMAX},
    {"max_operation_name_length", "Maximum length of a custom operation name",
     MAXCUSTOPNAME},
    {"max_pattern_length",
     "Maximum length of the pattern in a LIKE or GLOB operator",
     SQLITE_MAX_LIKE_PATTERN_LENGTH},
    {"max_plugins", "Maximum number of plugins", MAXPLUGINS},
    {"max_record_size", "Maximum record size", COMDB2_MAX_RECORD_SIZE},
    {"max_sp_name_length", "Maximum stored procedure name length", MAX_SPNAME},
    {"max_sp_version_length", "Maximum length of stored procedure version",
     MAX_SPVERSION_LEN},
    {"max_sql_length", "Maximum query length", SQLITE_MAX_SQL_LENGTH},
    {"max_table_name_length", "Maximum table name length", MAXTABLELEN - 1},
    {"max_tables", "Maximum number of tables", MAXTABLES},
    {"max_tag_name_length", "Maximum tag name length", MAXTAGLEN},
};

/*
  comdb2_limits: Comdb2 hard limits.
*/

typedef struct {
    sqlite3_vtab_cursor base; /* Base class - must be first */
    sqlite3_int64 rowid;      /* Row ID */
} systbl_limits_cursor;

/* Column numbers (always keep the below table definition in sync). */
enum { LIMITS_COLUMN_NAME, LIMITS_COLUMN_DESCR, LIMITS_COLUMN_VALUE };

static int systblLimitsConnect(sqlite3 *db, void *pAux, int argc,
                               const char *const *argv, sqlite3_vtab **ppVtab,
                               char **pErr)
{
    int rc;

    rc = sqlite3_declare_vtab(
        db, "CREATE TABLE comdb2_limits(\"name\", \"description\", \"value\")");

    if (rc == SQLITE_OK) {
        if ((*ppVtab = sqlite3_malloc(sizeof(sqlite3_vtab))) == 0) {
            return SQLITE_NOMEM;
        }
        memset(*ppVtab, 0, sizeof(ppVtab));
    }

    return 0;
}

static int systblLimitsBestIndex(sqlite3_vtab *tab,
                                 sqlite3_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

static int systblLimitsDisconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int systblLimitsOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor)
{
    systbl_limits_cursor *cur = sqlite3_malloc(sizeof(systbl_limits_cursor));
    if (cur == 0) {
        return SQLITE_NOMEM;
    }
    memset(cur, 0, sizeof(*cur));
    *ppCursor = &cur->base;
    return SQLITE_OK;
}

static int systblLimitsClose(sqlite3_vtab_cursor *cur)
{
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int systblLimitsFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                              const char *idxStr, int argc,
                              sqlite3_value **argv)
{
    systbl_limits_cursor *pCur = (systbl_limits_cursor *)pVtabCursor;
    pCur->rowid = 0;
    return SQLITE_OK;
}

static int systblLimitsNext(sqlite3_vtab_cursor *cur)
{
    systbl_limits_cursor *pCur = (systbl_limits_cursor *)cur;
    pCur->rowid++;
    return SQLITE_OK;
}

static int systblLimitsEof(sqlite3_vtab_cursor *cur)
{
    systbl_limits_cursor *pCur = (systbl_limits_cursor *)cur;
    return (pCur->rowid >= (sizeof(limits) / sizeof(struct limit_t))) ? 1 : 0;
}

static int systblLimitsColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
                              int pos)
{
    switch (pos) {
    case LIMITS_COLUMN_NAME:
        sqlite3_result_text(
            ctx, limits[((systbl_limits_cursor *)cur)->rowid].name, -1, NULL);
        break;
    case LIMITS_COLUMN_DESCR:
        sqlite3_result_text(
            ctx, limits[((systbl_limits_cursor *)cur)->rowid].descr, -1, NULL);
        break;
    case LIMITS_COLUMN_VALUE:
        sqlite3_result_int(ctx,
                           limits[((systbl_limits_cursor *)cur)->rowid].value);
        break;
    default:
        assert(0);
    };

    return SQLITE_OK;
}

static int systblLimitsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    systbl_limits_cursor *pCur = (systbl_limits_cursor *)cur;
    *pRowid = pCur->rowid;

    return SQLITE_OK;
}

const sqlite3_module systblLimitsModule = {
    0,                      /* iVersion */
    0,                      /* xCreate */
    systblLimitsConnect,    /* xConnect */
    systblLimitsBestIndex,  /* xBestIndex */
    systblLimitsDisconnect, /* xDisconnect */
    0,                      /* xDestroy */
    systblLimitsOpen,       /* xOpen - open a cursor */
    systblLimitsClose,      /* xClose - close a cursor */
    systblLimitsFilter,     /* xFilter - configure scan constraints */
    systblLimitsNext,       /* xNext - advance a cursor */
    systblLimitsEof,        /* xEof - check for end of scan */
    systblLimitsColumn,     /* xColumn - read data */
    systblLimitsRowid,      /* xRowid - read data */
    0,                      /* xUpdate */
    0,                      /* xBegin */
    0,                      /* xSync */
    0,                      /* xCommit */
    0,                      /* xRollback */
    0,                      /* xFindMethod */
    0,                      /* xRename */
    0,                      /* xSavepoint */
    0,                      /* xRelease */
    0,                      /* xRollbackTo */
    0,                      /* xShadowName */
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
