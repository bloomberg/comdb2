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
  sqlite3_vtab_cursor base; /* Base class - must be first */
  sqlite3_int64 rowid;      /* Row ID */
  struct summary_nodestats *summaries;
  unsigned nodes_cnt;
} systbl_clientstats_cursor;

/* Column numbers (always keep the below table definition in sync). */
enum {
    COLUMN_TASK,
    COLUMN_STACK,
    COLUMN_HOST,
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
    COLUMN_SVC_TIME,
    COLUMN_IS_SSL
};

static int systblClientStatsConnect(sqlite3 *db, void *pAux, int argc,
                                    const char *const *argv,
                                    sqlite3_vtab **ppVtab, char **pErr)
{
    int rc;

    rc = sqlite3_declare_vtab(
        db, "CREATE TABLE comdb2_clientstats(\"task\", \"stack\", \"node\", "
            "\"ip\", \"finds\" INTEGER, \"rngexts\" INTEGER, "
            "\"writes\" INTEGER, \"other_fstsnds\" INTEGER, \"adds\" INTEGER, "
            "\"upds\" INTEGER, \"dels\" INTEGER, \"bsql\" INTEGER, \"recom\" "
            "INTEGER, \"snapisol\" INTEGER, \"serial\" INTEGER, "
            "\"sql_queries\" INTEGER, \"sql_steps\" INTEGER, \"sql_rows\" "
            "INTEGER, \"svc_time\" DOUBLE, \"is_ssl\" INTEGER)");

    if (rc == SQLITE_OK) {
        if ((*ppVtab = sqlite3_malloc(sizeof(sqlite3_vtab))) == 0) {
            return SQLITE_NOMEM;
        }
        memset(*ppVtab, 0, sizeof(sqlite3_vtab));
    }

    return SQLITE_OK;
}

static int systblClientStatsBestIndex(sqlite3_vtab *tab,
                                      sqlite3_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

static int systblClientStatsDisconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int systblClientStatsOpen(sqlite3_vtab *p,
                                 sqlite3_vtab_cursor **ppCursor)
{
    systbl_clientstats_cursor *cur =
        sqlite3_malloc(sizeof(systbl_clientstats_cursor));
    if (cur == 0) {
        return SQLITE_NOMEM;
    }
    memset(cur, 0, sizeof(*cur));
    *ppCursor = &cur->base;

    cur->summaries = get_nodestats_summary(&(cur->nodes_cnt), 0);
    cur->rowid = 0;

    return SQLITE_OK;
}

static int systblClientStatsClose(sqlite3_vtab_cursor *cur)
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
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int systblClientStatsFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                                   const char *idxStr, int argc,
                                   sqlite3_value **argv)
{
    systbl_clientstats_cursor *pCur = (systbl_clientstats_cursor *)pVtabCursor;
    pCur->rowid = 0;
    return SQLITE_OK;
}

static int systblClientStatsNext(sqlite3_vtab_cursor *cur)
{
    systbl_clientstats_cursor *pCur = (systbl_clientstats_cursor *)cur;
    pCur->rowid++;
    return SQLITE_OK;
}

static int systblClientStatsEof(sqlite3_vtab_cursor *cur)
{
    systbl_clientstats_cursor *pCur = (systbl_clientstats_cursor *)cur;
    return pCur->rowid >= pCur->nodes_cnt;
}

static int systblClientStatsColumn(sqlite3_vtab_cursor *cur,
                                   sqlite3_context *ctx, int pos)
{
    /* We already have a lock on clientstats. */
    systbl_clientstats_cursor *pCur = (systbl_clientstats_cursor *)cur;
    unsigned ii = pCur->rowid;
    struct summary_nodestats *summaries = pCur->summaries;

    assert(pCur->rowid < pCur->nodes_cnt);

    switch (pos) {
    case COLUMN_TASK:
        sqlite3_result_text(ctx, summaries[ii].task, -1, NULL);
        break;
    case COLUMN_STACK:
        sqlite3_result_text(ctx, summaries[ii].stack, -1, NULL);
        break;
    case COLUMN_HOST:
        sqlite3_result_text(ctx, summaries[ii].host, -1, NULL);
        break;
    case COLUMN_IP:
        sqlite3_result_text(ctx, inet_ntoa(summaries[ii].addr), -1, NULL);
        break;
    case COLUMN_FINDS:
        sqlite3_result_int64(ctx, summaries[ii].finds);
        break;
    case COLUMN_RNGEXTS:
        sqlite3_result_int64(ctx, summaries[ii].rngexts);
        break;
    case COLUMN_WRITES:
        sqlite3_result_int64(ctx, summaries[ii].writes);
        break;
    case COLUMN_OTHER_FSTSNDS:
        sqlite3_result_int64(ctx, summaries[ii].other_fstsnds);
        break;
    case COLUMN_ADDS:
        sqlite3_result_int64(ctx, summaries[ii].adds);
        break;
    case COLUMN_UPDS:
        sqlite3_result_int64(ctx, summaries[ii].upds);
        break;
    case COLUMN_DELS:
        sqlite3_result_int64(ctx, summaries[ii].dels);
        break;
    case COLUMN_BSQL:
        sqlite3_result_int64(ctx, summaries[ii].bsql);
        break;
    case COLUMN_RECOM:
        sqlite3_result_int64(ctx, summaries[ii].recom);
        break;
    case COLUMN_SNAPISOL:
        sqlite3_result_int64(ctx, summaries[ii].snapisol);
        break;
    case COLUMN_SERIAL:
        sqlite3_result_int64(ctx, summaries[ii].serial);
        break;
    case COLUMN_SQL_QUERIES:
        sqlite3_result_int64(ctx, summaries[ii].sql_queries);
        break;
    case COLUMN_SQL_STEPS:
        sqlite3_result_int64(ctx, summaries[ii].sql_steps);
        break;
    case COLUMN_SQL_ROWS:
        sqlite3_result_int64(ctx, summaries[ii].sql_rows);
        break;
    case COLUMN_SVC_TIME:
        sqlite3_result_double(ctx, summaries[ii].svc_time);
        break;
    case COLUMN_IS_SSL:
        sqlite3_result_int(ctx, summaries[ii].is_ssl);
        break;
    default: assert(0);
    };

    return SQLITE_OK;
}

static int systblClientStatsRowid(sqlite3_vtab_cursor *cur,
                                  sqlite_int64 *pRowid)
{
    systbl_clientstats_cursor *pCur = (systbl_clientstats_cursor *)cur;
    *pRowid = pCur->rowid;

    return SQLITE_OK;
}

const sqlite3_module systblClientStatsModule = {
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
    0,                           /* xSavepoint */
    0,                           /* xRelease */
    0,                           /* xRollbackTo */
    0,                           /* xShadowName */
    .access_flag = CDB2_ALLOW_USER,
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
