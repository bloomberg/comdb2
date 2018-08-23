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
#include <pthread.h>
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "list.h"
#include "thdpool.h"

extern pthread_mutex_t pool_list_lk;
extern LISTC_T(struct thdpool) threadpools;

/*
  comdb2_threadpools: Information about thread pools.
*/

typedef struct {
    sqlite3_vtab_cursor base; /* Base class - must be first */
    sqlite3_int64 rowid;      /* Row ID */
    struct thdpool *pool;     /* Current thread pool */
} systbl_threadpools_cursor;

/* Column numbers (always keep the below table definition in sync). */
enum {
    COLUMN_NAME,
    COLUMN_STATUS,
    COLUMN_NUM_THD,
    COLUMN_FREE_THD,
    COLUMN_PEAK_THD,
    COLUMN_NUM_CREATES,
    COLUMN_NUM_EXITS,
    COLUMN_NUM_PASSED,
    COLUMN_NUM_ENQUEUED,
    COLUMN_NUM_DEQUEUED,
    COLUMN_NUM_TIMEOUT,
    COLUMN_NUM_FAILED_DISPATCHES,
    COLUMN_MIN_THDS,
    COLUMN_MAX_THDS,
    COLUMN_PEAK_QUEUE,
    COLUMN_MAX_QUEUE,
    COLUMN_QUEUE,
    COLUMN_LONG_WAIT_MS,
    COLUMN_LINGER_SECS,
    COLUMN_STACK_SIZE,
    COLUMN_MAX_QUEUE_OVERRIDE,
    COLUMN_MAX_QUEUE_AGE_MS,
    COLUMN_EXIT_ON_CREATE_FAIL,
    COLUMN_DUMP_ON_FULL,
    /*COLUMN_HISTOGRAM,*/
};

static int systblThreadPoolsConnect(sqlite3 *db, void *pAux, int argc,
                                    const char *const *argv,
                                    sqlite3_vtab **ppVtab, char **pErr)
{
    int rc;

    rc = sqlite3_declare_vtab(
        db, "CREATE TABLE comdb2_pools(\"name\", \"status\", \"num_thd\", "
            "\"free_thd\", \"peak_thd\", \"num_creates\", \"num_exits\", "
            "\"num_passed\", \"num_enqueued\", \"num_dequeued\", "
            "\"num_timeout\", \"num_failed_dispatches\", \"min_thds\", "
            "\"max_thds\", \"peak_queue\", \"max_queue\", \"queue\", "
            "\"long_wait_ms\", \"linger_secs\", \"stack_size\", "
            "\"max_queue_override\", \"max_queue_age_ms\", "
            "\"exit_on_create_fail\", \"dump_on_full\")");

    if (rc == SQLITE_OK) {
        if ((*ppVtab = sqlite3_malloc(sizeof(sqlite3_vtab))) == 0) {
            return SQLITE_NOMEM;
        }
        memset(*ppVtab, 0, sizeof(*ppVtab));
    }

    return SQLITE_OK;
}

static int systblThreadPoolsBestIndex(sqlite3_vtab *tab,
                                      sqlite3_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

static int systblThreadPoolsDisconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int systblThreadPoolsOpen(sqlite3_vtab *p,
                                 sqlite3_vtab_cursor **ppCursor)
{
    /* Do not allow non-OP users if authentication is enabled. */
    int rc = comdb2CheckOpAccess();
    if( rc!=SQLITE_OK )
        return rc;

    systbl_threadpools_cursor *cur =
        sqlite3_malloc(sizeof(systbl_threadpools_cursor));
    if (cur == 0) {
        return SQLITE_NOMEM;
    }
    memset(cur, 0, sizeof(*cur));
    *ppCursor = &cur->base;

    /* Unlocked in systblThreadPoolsClose() */
    pthread_mutex_lock(&pool_list_lk);
    cur->pool = LISTC_TOP(&threadpools);

    return SQLITE_OK;
}

static int systblThreadPoolsClose(sqlite3_vtab_cursor *cur)
{
    pthread_mutex_unlock(&pool_list_lk);
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int systblThreadPoolsFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                                   const char *idxStr, int argc,
                                   sqlite3_value **argv)
{
    systbl_threadpools_cursor *pCur = (systbl_threadpools_cursor *)pVtabCursor;
    pCur->rowid = 0;
    return SQLITE_OK;
}

static int systblThreadPoolsNext(sqlite3_vtab_cursor *cur)
{
    systbl_threadpools_cursor *pCur = (systbl_threadpools_cursor *)cur;
    pCur->pool = thdpool_next_pool(pCur->pool);
    pCur->rowid++;
    return SQLITE_OK;
}

static int systblThreadPoolsEof(sqlite3_vtab_cursor *cur)
{
    systbl_threadpools_cursor *pCur = (systbl_threadpools_cursor *)cur;
    return (pCur->pool == NULL) ? 1 : 0;
}

static int systblThreadPoolsColumn(sqlite3_vtab_cursor *cur,
                                   sqlite3_context *ctx, int pos)
{
    /* We already have a lock on threadpools. */
    systbl_threadpools_cursor *pCur = (systbl_threadpools_cursor *)cur;
    struct thdpool *pool = pCur->pool;

    /* Lock the pool. */
    thdpool_lock(pool);

    switch (pos) {
    case COLUMN_NAME:
        sqlite3_result_text(ctx, thdpool_get_name(pool), -1, NULL);
        break;
    case COLUMN_STATUS:
        sqlite3_result_text(
            ctx, thdpool_get_status(pool) ? "stopped" : "running", -1, NULL);
        break;
    case COLUMN_NUM_THD:
        sqlite3_result_int(ctx, thdpool_get_nthds(pool));
        break;
    case COLUMN_FREE_THD:
        sqlite3_result_int(ctx, thdpool_get_nfreethds(pool));
        break;
    case COLUMN_PEAK_THD:
        sqlite3_result_int(ctx, thdpool_get_peaknthds(pool));
        break;
    case COLUMN_NUM_CREATES:
        sqlite3_result_int(ctx, thdpool_get_creates(pool));
        break;
    case COLUMN_NUM_EXITS:
        sqlite3_result_int(ctx, thdpool_get_exits(pool));
        break;
    case COLUMN_NUM_PASSED:
        sqlite3_result_int(ctx, thdpool_get_passed(pool));
        break;
    case COLUMN_NUM_ENQUEUED:
        sqlite3_result_int(ctx, thdpool_get_enqueued(pool));
        break;
    case COLUMN_NUM_DEQUEUED:
        sqlite3_result_int(ctx, thdpool_get_dequeued(pool));
        break;
    case COLUMN_NUM_TIMEOUT:
        sqlite3_result_int(ctx, thdpool_get_timeouts(pool));
        break;
    case COLUMN_NUM_FAILED_DISPATCHES:
        sqlite3_result_int(ctx, thdpool_get_failed_dispatches(pool));
        break;
    case COLUMN_MIN_THDS:
        sqlite3_result_int(ctx, thdpool_get_minnthd(pool));
        break;
    case COLUMN_MAX_THDS:
        sqlite3_result_int(ctx, thdpool_get_maxnthd(pool));
        break;
    case COLUMN_PEAK_QUEUE:
        sqlite3_result_int(ctx, thdpool_get_peakqueue(pool));
        break;
    case COLUMN_MAX_QUEUE:
        sqlite3_result_int(ctx, thdpool_get_maxqueue(pool));
        break;
    case COLUMN_QUEUE:
        sqlite3_result_int(ctx, thdpool_get_nqueuedworks(pool));
        break;
    case COLUMN_LONG_WAIT_MS:
        sqlite3_result_int(ctx, thdpool_get_longwaitms(pool));
        break;
    case COLUMN_LINGER_SECS:
        sqlite3_result_int(ctx, thdpool_get_lingersecs(pool));
        break;
    case COLUMN_STACK_SIZE:
        sqlite3_result_int(ctx, thdpool_get_stacksz(pool));
        break;
    case COLUMN_MAX_QUEUE_OVERRIDE:
        sqlite3_result_int(ctx, thdpool_get_maxqueueoverride(pool));
        break;
    case COLUMN_MAX_QUEUE_AGE_MS:
        sqlite3_result_int(ctx, thdpool_get_maxqueueagems(pool));
        break;
    case COLUMN_EXIT_ON_CREATE_FAIL:
        sqlite3_result_text(ctx, YESNO(thdpool_get_exit_on_create_fail(pool)),
                            -1, NULL);
        break;
    case COLUMN_DUMP_ON_FULL:
        sqlite3_result_text(ctx, YESNO(thdpool_get_dump_on_full(pool)), -1,
                            NULL);
        break;
    default: assert(0);
    };

    /* Unlock the pool. */
    thdpool_unlock(pool);

    return SQLITE_OK;
}

static int systblThreadPoolsRowid(sqlite3_vtab_cursor *cur,
                                  sqlite_int64 *pRowid)
{
    systbl_threadpools_cursor *pCur = (systbl_threadpools_cursor *)cur;
    *pRowid = pCur->rowid;

    return SQLITE_OK;
}

const sqlite3_module systblThreadPoolsModule = {
    0,                           /* iVersion */
    0,                           /* xCreate */
    systblThreadPoolsConnect,    /* xConnect */
    systblThreadPoolsBestIndex,  /* xBestIndex */
    systblThreadPoolsDisconnect, /* xDisconnect */
    0,                           /* xDestroy */
    systblThreadPoolsOpen,       /* xOpen - open a cursor */
    systblThreadPoolsClose,      /* xClose - close a cursor */
    systblThreadPoolsFilter,     /* xFilter - configure scan constraints */
    systblThreadPoolsNext,       /* xNext - advance a cursor */
    systblThreadPoolsEof,        /* xEof - check for end of scan */
    systblThreadPoolsColumn,     /* xColumn - read data */
    systblThreadPoolsRowid,      /* xRowid - read data */
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
