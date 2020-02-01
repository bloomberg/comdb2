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
#include <arpa/inet.h>

#include "comdb2.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"

typedef struct {
  sqlitex_vtab_cursor base; /* Base class - must be first */
  sqlitex_int64 rowid;      /* Row ID */
  struct summary_nodestats *summaries;
  unsigned nodes_cnt;
} systbl_clientstats_cursor;

/* Column numbers (always keep the below table definition in sync). */
enum {
    COLUMN_TASK,
    COLUMN_STACK,
    COLUMN_NODE,
    COLUMN_IP,
    COLUMN_FINDS,
    COLUMN_RNGEXTS,
    COLUMN_WRITES,
    COLUMN_OTHER_FSTSNDS,
    COLUMN_ADDS,
    COLUMN_UPDS,
    COLUMN_DELS,
    COLUMN_BSQL,
    COLUMN_RECOM,
    COLUMN_SNAPISOL,
    COLUMN_SERIAL,
    COLUMN_SQL_QUERIES,
    COLUMN_SQL_STEPS,
    COLUMN_SQL_ROWS,
};

static int systblClientStatsConnect(sqlitex *db, void *pAux, int argc,
                                    const char *const *argv,
                                    sqlitex_vtab **ppVtab, char **pErr)
{
    int rc;

    rc = sqlitex_declare_vtab(
        db, "CREATE TABLE comdb2_clientstats(\"task\", \"stack\", \"node\" "
            "INTEGER, \"ip\", \"finds\" INTEGER, \"rngexts\" INTEGER, "
            "\"writes\" INTEGER, \"other_fstsnds\" INTEGER, \"adds\" INTEGER, "
            "\"upds\" INTEGER, \"dels\" INTEGER, \"bsql\" INTEGER, \"recom\" "
            "INTEGER, \"snapisol\" INTEGER, \"serial\" INTEGER, "
            "\"sql_queries\" INTEGER, \"sql_steps\" INTEGER, \"sql_rows\" "
            "INTEGER)");

    if (rc == SQLITE_OK) {
        if ((*ppVtab = sqlitex_malloc(sizeof(sqlitex_vtab))) == 0) {
            return SQLITE_NOMEM;
        }
        memset(*ppVtab, 0, sizeof(*ppVtab));
    }

    return SQLITE_OK;
}

static int systblClientStatsBestIndex(sqlitex_vtab *tab,
                                      sqlitex_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

static int systblClientStatsDisconnect(sqlitex_vtab *pVtab)
{
    sqlitex_free(pVtab);
    return SQLITE_OK;
}

static int systblClientStatsOpen(sqlitex_vtab *p,
                                 sqlitex_vtab_cursor **ppCursor)
{
    systbl_clientstats_cursor *cur =
        sqlitex_malloc(sizeof(systbl_clientstats_cursor));
    if (cur == 0) {
        return SQLITE_NOMEM;
    }
    memset(cur, 0, sizeof(*cur));
    *ppCursor = &cur->base;

    cur->summaries = get_nodestats_summary(&(cur->nodes_cnt), 0);
    cur->rowid = 0;

    return SQLITE_OK;
}

static int systblClientStatsClose(sqlitex_vtab_cursor *cur)
{
    int i;
    systbl_clientstats_cursor *pCur = (systbl_clientstats_cursor *)cur;
    for (i = 0; i < pCur->nodes_cnt; i++) {
        if (pCur->summaries[i].task)
            free(pCur->summaries[i].task);
        if (pCur->summaries[i].stack)
            free(pCur->summaries[i].stack);
    }
    free(pCur->summaries);
    sqlitex_free(cur);
    return SQLITE_OK;
}

static int systblClientStatsFilter(sqlitex_vtab_cursor *pVtabCursor, int idxNum,
                                   const char *idxStr, int argc,
                                   sqlitex_value **argv)
{
    systbl_clientstats_cursor *pCur = (systbl_clientstats_cursor *)pVtabCursor;
    pCur->rowid = 0;
    return SQLITE_OK;
}

static int systblClientStatsNext(sqlitex_vtab_cursor *cur)
{
    systbl_clientstats_cursor *pCur = (systbl_clientstats_cursor *)cur;
    pCur->rowid++;
    return SQLITE_OK;
}

static int systblClientStatsEof(sqlitex_vtab_cursor *cur)
{
    systbl_clientstats_cursor *pCur = (systbl_clientstats_cursor *)cur;
    return pCur->rowid >= pCur->nodes_cnt;
}

static int systblClientStatsColumn(sqlitex_vtab_cursor *cur,
                                   sqlitex_context *ctx, int pos)
{
    /* We already have a lock on clientstats. */
    systbl_clientstats_cursor *pCur = (systbl_clientstats_cursor *)cur;
    unsigned ii = pCur->rowid;
    struct summary_nodestats *summaries = pCur->summaries;

    assert(pCur->rowid < pCur->nodes_cnt);

    switch (pos) {
    case COLUMN_TASK:
        sqlitex_result_text(ctx, summaries[ii].task, -1, NULL);
        break;
    case COLUMN_STACK:
        sqlitex_result_text(ctx, summaries[ii].stack, -1, NULL);
        break;
    case COLUMN_NODE:
        sqlitex_result_int64(ctx, summaries[ii].node);
        break;
    case COLUMN_IP:
        sqlitex_result_text(ctx, inet_ntoa(summaries[ii].addr), -1, NULL);
        break;
    case COLUMN_FINDS:
        sqlitex_result_int64(ctx, summaries[ii].finds);
        break;
    case COLUMN_RNGEXTS:
        sqlitex_result_int64(ctx, summaries[ii].rngexts);
        break;
    case COLUMN_WRITES:
        sqlitex_result_int64(ctx, summaries[ii].writes);
        break;
    case COLUMN_OTHER_FSTSNDS:
        sqlitex_result_int64(ctx, summaries[ii].other_fstsnds);
        break;
    case COLUMN_ADDS:
        sqlitex_result_int64(ctx, summaries[ii].adds);
        break;
    case COLUMN_UPDS:
        sqlitex_result_int64(ctx, summaries[ii].upds);
        break;
    case COLUMN_DELS:
        sqlitex_result_int64(ctx, summaries[ii].dels);
        break;
    case COLUMN_BSQL:
        sqlitex_result_int64(ctx, summaries[ii].bsql);
        break;
    case COLUMN_RECOM:
        sqlitex_result_int64(ctx, summaries[ii].recom);
        break;
    case COLUMN_SNAPISOL:
        sqlitex_result_int64(ctx, summaries[ii].snapisol);
        break;
    case COLUMN_SERIAL:
        sqlitex_result_int64(ctx, summaries[ii].serial);
        break;
    case COLUMN_SQL_QUERIES:
        sqlitex_result_int64(ctx, summaries[ii].sql_queries);
        break;
    case COLUMN_SQL_STEPS:
        sqlitex_result_int64(ctx, summaries[ii].sql_steps);
        break;
    case COLUMN_SQL_ROWS:
        sqlitex_result_int64(ctx, summaries[ii].sql_rows);
        break;
    default: assert(0);
    };

    return SQLITE_OK;
}

static int systblClientStatsRowid(sqlitex_vtab_cursor *cur,
                                  sqlite_int64 *pRowid)
{
    systbl_clientstats_cursor *pCur = (systbl_clientstats_cursor *)cur;
    *pRowid = pCur->rowid;

    return SQLITE_OK;
}

const sqlitex_module systblClientStatsModule = {
    0,                           /* iVersion */
    0,                           /* xCreate */
    systblClientStatsConnect,    /* xConnect */
    systblClientStatsBestIndex,  /* xBestIndex */
    systblClientStatsDisconnect, /* xDisconnect */
    0,                           /* xDestroy */
    systblClientStatsOpen,       /* xOpen - open a cursor */
    systblClientStatsClose,      /* xClose - close a cursor */
    systblClientStatsFilter,     /* xFilter - configure scan constraints */
    systblClientStatsNext,       /* xNext - advance a cursor */
    systblClientStatsEof,        /* xEof - check for end of scan */
    systblClientStatsColumn,     /* xColumn - read data */
    systblClientStatsRowid,      /* xRowid - read data */
    0,                           /* xUpdate */
    0,                           /* xBegin */
    0,                           /* xSync */
    0,                           /* xCommit */
    0,                           /* xRollback */
    0,                           /* xFindMethod */
    0,                           /* xRename */
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
