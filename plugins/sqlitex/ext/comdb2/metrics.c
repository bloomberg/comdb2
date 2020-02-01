/*
   Copyright 2017 Bloomberg Finance L.P.

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
#include "sql.h"
#include "metrics.h"

/*
  comdb2_metrics: List various query related metrics.
*/

typedef struct {
    sqlitex_vtab_cursor base; /* Base class - must be first */
    sqlitex_int64 rowid;      /* Row ID */
} systbl_metrics_cursor;

/* Column numbers (always keep the below table definition in sync). */
enum {
    COLUMN_NAME,
    COLUMN_DESCR,
    COLUMN_TYPE,
    COLUMN_VALUE,
    COLUMN_COLLECTION_TYPE
};

static int systblMetricsConnect(sqlitex *db, void *pAux, int argc,
                              const char *const *argv, sqlitex_vtab **ppVtab,
                              char **pErr)
{
    int rc;

    rc = sqlitex_declare_vtab(
        db,
        "CREATE TABLE comdb2_metrics(\"name\", \"description\", \"type\", "
        "\"value\", \"collection_type\")");


    if (rc == SQLITE_OK) {
        if ((*ppVtab = sqlitex_malloc(sizeof(sqlitex_vtab))) == 0) {
            return SQLITE_NOMEM;
        }
        memset(*ppVtab, 0, sizeof(*ppVtab));
    }

    return 0;
}

static int systblMetricsBestIndex(sqlitex_vtab *tab, sqlitex_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

static int systblMetricsDisconnect(sqlitex_vtab *pVtab)
{
    sqlitex_free(pVtab);
    return SQLITE_OK;
}

static int systblMetricsOpen(sqlitex_vtab *p, sqlitex_vtab_cursor **ppCursor)
{
    int rc;

    systbl_metrics_cursor *cur = sqlitex_malloc(sizeof(systbl_metrics_cursor));
    if (cur == 0) {
        return SQLITE_NOMEM;
    }
    memset(cur, 0, sizeof(*cur));

    /* Refresh the metrics. */
    rc = refresh_metrics();
    if (rc) {
        return SQLITE_INTERNAL;
    }

    *ppCursor = &cur->base;
    return SQLITE_OK;
}

static int systblMetricsClose(sqlitex_vtab_cursor *cur)
{
    sqlitex_free(cur);
    return SQLITE_OK;
}

static int systblMetricsFilter(sqlitex_vtab_cursor *pVtabCursor, int idxNum,
                             const char *idxStr, int argc, sqlitex_value **argv)
{
    systbl_metrics_cursor *pCur = (systbl_metrics_cursor *)pVtabCursor;
    pCur->rowid = 0;
    return SQLITE_OK;
}

static int systblMetricsNext(sqlitex_vtab_cursor *cur)
{
    systbl_metrics_cursor *pCur = (systbl_metrics_cursor *)cur;
    pCur->rowid++;
    return SQLITE_OK;
}

static int systblMetricsEof(sqlitex_vtab_cursor *cur)
{
    systbl_metrics_cursor *pCur = (systbl_metrics_cursor *)cur;
    return (pCur->rowid >= gbl_metrics_count) ? 1 : 0;
}


static int systblMetricsColumn(sqlitex_vtab_cursor *cur, sqlitex_context *ctx,
                             int pos)
{
    systbl_metrics_cursor *pCur = (systbl_metrics_cursor *)cur;
    comdb2_metric *stat = &gbl_metrics[pCur->rowid];

    switch (pos) {
        case COLUMN_NAME:
            sqlitex_result_text(ctx, stat->name, -1, NULL);
            break;
        case COLUMN_DESCR:
            sqlitex_result_text(ctx, stat->descr, -1, NULL);
            break;
        case COLUMN_TYPE:
            sqlitex_result_text(ctx, metric_type(stat->type), -1, NULL);
            break;
        case COLUMN_VALUE:
            switch (stat->type) {
                case STATISTIC_INTEGER:
                    sqlitex_result_int64(ctx, *(int64_t *)stat->var);
                    break;
                case STATISTIC_DOUBLE:
                    sqlitex_result_double(ctx, *(double *)stat->var);
                    break;
            }
            break;
        case COLUMN_COLLECTION_TYPE:
            sqlitex_result_text(ctx,metric_collection_type_string(stat->collection_type), -1, NULL);
            break;

        default:
            assert(0);
    };

    return SQLITE_OK;
}

static int systblMetricsRowid(sqlitex_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    systbl_metrics_cursor *pCur = (systbl_metrics_cursor *)cur;
    *pRowid = pCur->rowid;

    return SQLITE_OK;
}

const sqlitex_module systblMetricsModule = {
    0,                     /* iVersion */
    0,                     /* xCreate */
    systblMetricsConnect,    /* xConnect */
    systblMetricsBestIndex,  /* xBestIndex */
    systblMetricsDisconnect, /* xDisconnect */
    0,                     /* xDestroy */
    systblMetricsOpen,       /* xOpen - open a cursor */
    systblMetricsClose,      /* xClose - close a cursor */
    systblMetricsFilter,     /* xFilter - configure scan constraints */
    systblMetricsNext,       /* xNext - advance a cursor */
    systblMetricsEof,        /* xEof - check for end of scan */
    systblMetricsColumn,     /* xColumn - read data */
    systblMetricsRowid,      /* xRowid - read data */
    0,                     /* xUpdate */
    0,                     /* xBegin */
    0,                     /* xSync */
    0,                     /* xCommit */
    0,                     /* xRollback */
    0,                     /* xFindMethod */
    0,                     /* xRename */
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */

