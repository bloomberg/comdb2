/*
   Copyright 2017-2020 Bloomberg Finance L.P.

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
#include <pthread.h>
#include "comdb2.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "bdb_api.h"

/*
  comdb2_repl_stats: Print replication-related statistics.
*/

typedef struct {
    sqlite3_vtab_cursor base;        /* Base class - must be first */
    sqlite3_int64 rowid;             /* Row ID */
    repl_wait_and_net_use_t *stats;  /* Statistics */
    int cluster_size;                /* Number of hosts in the cluster */
} systbl_repl_stats_cursor;

/* Column numbers (always keep the below table definition in sync). */
enum {
    COLUMN_HOST,
    COLUMN_BYTES_WRITTEN,
    COLUMN_BYTES_READ,
    COLUMN_THROTTLE_WAITS,
    COLUMN_REORDERS,
    COLUMN_AVG_WAIT_OVER_10SEC,
    COLUMN_MAX_WAIT_OVER_10SEC,
    COLUMN_AVG_WAIT_OVER_1MIN,
    COLUMN_MAX_WAIT_OVER_1MIN,
    COLUMN_LSN,
    COLUMN_LSN_BYTES_BEHIND_MASTER
};

static int systblReplStatsConnect(sqlite3 *db, void *pAux, int argc,
                                  const char *const *argv,
                                  sqlite3_vtab **ppVtab, char **pErr)
{
    int rc;

    rc = sqlite3_declare_vtab(
        db, "CREATE TABLE comdb2_repl_stats(\"host\", "
            "\"bytes_written\", \"bytes_read\", \"throttle_waits\", "
            "\"reorders\", \"avg_wait_over_10secs\", \"max_wait_over_10secs\", "
            "\"avg_wait_over_1min\", \"max_wait_over_1min\", "
            "\"lsn\", \"lsn_bytes_behind_master\")");

    if (rc == SQLITE_OK) {
        if ((*ppVtab = sqlite3_malloc(sizeof(sqlite3_vtab))) == 0) {
            return SQLITE_NOMEM;
        }
        memset(*ppVtab, 0, sizeof(sqlite3_vtab));
    }

    return 0;
}

static int systblReplStatsBestIndex(sqlite3_vtab *tab,
                                    sqlite3_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

static int systblReplStatsDisconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int systblReplStatsOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor)
{
    systbl_repl_stats_cursor *cur =
        sqlite3_malloc(sizeof(systbl_repl_stats_cursor));
    if (cur == 0) {
        return SQLITE_NOMEM;
    }
    memset(cur, 0, sizeof(*cur));

    if (db_is_stopped()) {
        logmsg(LOGMSG_ERROR, "%s db is stopped!\n", __func__);
        return SQLITE_INTERNAL;
    }

    cur->stats = bdb_get_repl_wait_and_net_stats(thedb->bdb_env, &cur->cluster_size);

    *ppCursor = &cur->base;
    return SQLITE_OK;
}

static int systblReplStatsClose(sqlite3_vtab_cursor *cur)
{

    systbl_repl_stats_cursor *pCur = (systbl_repl_stats_cursor *)cur;
    free(pCur->stats);
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int systblReplStatsFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                                 const char *idxStr, int argc,
                                 sqlite3_value **argv)
{
    systbl_repl_stats_cursor *pCur = (systbl_repl_stats_cursor *)pVtabCursor;
    pCur->rowid = 0;
    return SQLITE_OK;
}

static int systblReplStatsNext(sqlite3_vtab_cursor *cur)
{
    systbl_repl_stats_cursor *pCur = (systbl_repl_stats_cursor *)cur;
    pCur->rowid++;
    return SQLITE_OK;
}

static int systblReplStatsEof(sqlite3_vtab_cursor *cur)
{
    systbl_repl_stats_cursor *pCur = (systbl_repl_stats_cursor *)cur;
    return (pCur->rowid >= pCur->cluster_size) ? 1 : 0;
}

static int systblReplStatsColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
                                 int pos)
{
    systbl_repl_stats_cursor *pCur;
    repl_wait_and_net_use_t *stats;

    pCur = (systbl_repl_stats_cursor *)cur;
    stats = &pCur->stats[pCur->rowid];

    switch (pos) {
    case COLUMN_HOST:
        sqlite3_result_text(ctx, stats->host, -1, NULL);
        break;
    case COLUMN_BYTES_WRITTEN:
        sqlite3_result_int64(ctx, stats->bytes_written);
        break;
    case COLUMN_BYTES_READ:
        sqlite3_result_int64(ctx, stats->bytes_read);
        break;
    case COLUMN_THROTTLE_WAITS:
        sqlite3_result_int64(ctx, stats->throttle_waits);
        break;
    case COLUMN_REORDERS:
        sqlite3_result_int64(ctx, stats->reorders);
        break;
    case COLUMN_AVG_WAIT_OVER_10SEC:
        sqlite3_result_double(ctx, stats->avg_wait_over_10secs);
        break;
    case COLUMN_MAX_WAIT_OVER_10SEC:
        sqlite3_result_double(ctx, stats->max_wait_over_10secs);
        break;
    case COLUMN_AVG_WAIT_OVER_1MIN:
        sqlite3_result_double(ctx, stats->avg_wait_over_1min);
        break;
    case COLUMN_MAX_WAIT_OVER_1MIN:
        sqlite3_result_double(ctx, stats->max_wait_over_1min);
        break;
    case COLUMN_LSN:
        sqlite3_result_text(ctx, stats->lsn_text, -1, NULL);
        break;
    case COLUMN_LSN_BYTES_BEHIND_MASTER:
        sqlite3_result_int64(ctx, stats->lsn_bytes_behind);
        break;
    default:
        assert(0);
    };

    return SQLITE_OK;
}

static int systblReplStatsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    systbl_repl_stats_cursor *pCur = (systbl_repl_stats_cursor *)cur;
    *pRowid = pCur->rowid;

    return SQLITE_OK;
}

const sqlite3_module systblReplStatsModule = {
    0,                         /* iVersion */
    0,                         /* xCreate */
    systblReplStatsConnect,    /* xConnect */
    systblReplStatsBestIndex,  /* xBestIndex */
    systblReplStatsDisconnect, /* xDisconnect */
    0,                         /* xDestroy */
    systblReplStatsOpen,       /* xOpen - open a cursor */
    systblReplStatsClose,      /* xClose - close a cursor */
    systblReplStatsFilter,     /* xFilter - configure scan constraints */
    systblReplStatsNext,       /* xNext - advance a cursor */
    systblReplStatsEof,        /* xEof - check for end of scan */
    systblReplStatsColumn,     /* xColumn - read data */
    systblReplStatsRowid,      /* xRowid - read data */
    0,                         /* xUpdate */
    0,                         /* xBegin */
    0,                         /* xSync */
    0,                         /* xCommit */
    0,                         /* xRollback */
    0,                         /* xFindMethod */
    0,                         /* xRename */
    0,                         /* xSavepoint */
    0,                         /* xRelease */
    0,                         /* xRollbackTo */
    0,                         /* xShadowName */
    .access_flag = CDB2_ALLOW_USER,
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */

