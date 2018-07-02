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

/*
  comdb2_stats: List various query related stats.
*/

typedef struct {
    sqlite3_vtab_cursor base; /* Base class - must be first */
    sqlite3_int64 rowid;      /* Row ID */
    struct query_stats stats; /* Statistics */
} systbl_stats_cursor;

/* Column numbers (always keep the below table definition in sync). */
enum {
    COLUMN_FSTRAP,
    COLUMN_SQL,
    COLUMN_STEPS,
    COLUMN_COMMITS,
    COLUMN_RETRIES,
    COLUMN_DEADLOCKS,
    COLUMN_LOCKWAITS,
    COLUMN_BPOOL_HITS,
    COLUMN_BPOOL_MISSES,
    COLUMN_PWRITES,
    COLUMN_PREADS,
};

static int systblStatsConnect(sqlite3 *db, void *pAux, int argc,
                              const char *const *argv, sqlite3_vtab **ppVtab,
                              char **pErr)
{
    int rc;

    rc = sqlite3_declare_vtab(
        db, "CREATE TABLE comdb2_stats(\"fstrap\", \"sql\", \"steps\", "
            "\"commits\", \"retries\", \"deadlocks\", \"lockwaits\", "
            "\"bpool_hits\", \"bpool_misses\", \"pwrites\", \"preads\")");

    if (rc == SQLITE_OK) {
        if ((*ppVtab = sqlite3_malloc(sizeof(sqlite3_vtab))) == 0) {
            return SQLITE_NOMEM;
        }
        memset(*ppVtab, 0, sizeof(*ppVtab));
    }

    return 0;
}

static int systblStatsBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

static int systblStatsDisconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int systblStatsOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor)
{
    int rc;

    systbl_stats_cursor *cur = sqlite3_malloc(sizeof(systbl_stats_cursor));
    if (cur == 0) {
        return SQLITE_NOMEM;
    }
    memset(cur, 0, sizeof(*cur));

    /* Gather the stats. */
    rc = get_query_stats(&cur->stats);
    if (rc) {
        return SQLITE_INTERNAL;
    }

    *ppCursor = &cur->base;
    return SQLITE_OK;
}

static int systblStatsClose(sqlite3_vtab_cursor *cur)
{
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int systblStatsFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                             const char *idxStr, int argc, sqlite3_value **argv)
{
    systbl_stats_cursor *pCur = (systbl_stats_cursor *)pVtabCursor;
    pCur->rowid = 0;
    return SQLITE_OK;
}

static int systblStatsNext(sqlite3_vtab_cursor *cur)
{
    systbl_stats_cursor *pCur = (systbl_stats_cursor *)cur;
    pCur->rowid++;
    return SQLITE_OK;
}

static int systblStatsEof(sqlite3_vtab_cursor *cur)
{
    systbl_stats_cursor *pCur = (systbl_stats_cursor *)cur;
    return (pCur->rowid == 1) ? 1 : 0;
}

static int systblStatsColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
                             int pos)
{
    systbl_stats_cursor *pCur = (systbl_stats_cursor *)cur;

    switch (pos) {
    case COLUMN_FSTRAP:
        sqlite3_result_int(ctx, pCur->stats.nfstrap);
        break;
    case COLUMN_SQL:
        sqlite3_result_int(ctx, pCur->stats.nsql);
        break;
    case COLUMN_STEPS:
        sqlite3_result_int(ctx, pCur->stats.nsteps);
        break;
    case COLUMN_COMMITS:
        sqlite3_result_int(ctx, pCur->stats.ncommits);
        break;
    case COLUMN_RETRIES:
        sqlite3_result_int(ctx, pCur->stats.nretries);
        break;
    case COLUMN_DEADLOCKS:
        sqlite3_result_int(ctx, pCur->stats.ndeadlocks);
        break;
    case COLUMN_LOCKWAITS:
        sqlite3_result_int(ctx, pCur->stats.nlockwaits);
        break;
    case COLUMN_BPOOL_HITS:
        sqlite3_result_int(ctx, pCur->stats.nbpoolhits);
        break;
    case COLUMN_BPOOL_MISSES:
        sqlite3_result_int(ctx, pCur->stats.nbpoolmisses);
        break;
    case COLUMN_PWRITES:
        sqlite3_result_int(ctx, pCur->stats.npwrites);
        break;
    case COLUMN_PREADS:
        sqlite3_result_int(ctx, pCur->stats.npreads);
        break;
    default:
        assert(0);
    };

    return SQLITE_OK;
}

static int systblStatsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    systbl_stats_cursor *pCur = (systbl_stats_cursor *)cur;
    *pRowid = pCur->rowid;

    return SQLITE_OK;
}

const sqlite3_module systblStatsModule = {
    0,                     /* iVersion */
    0,                     /* xCreate */
    systblStatsConnect,    /* xConnect */
    systblStatsBestIndex,  /* xBestIndex */
    systblStatsDisconnect, /* xDisconnect */
    0,                     /* xDestroy */
    systblStatsOpen,       /* xOpen - open a cursor */
    systblStatsClose,      /* xClose - close a cursor */
    systblStatsFilter,     /* xFilter - configure scan constraints */
    systblStatsNext,       /* xNext - advance a cursor */
    systblStatsEof,        /* xEof - check for end of scan */
    systblStatsColumn,     /* xColumn - read data */
    systblStatsRowid,      /* xRowid - read data */
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


