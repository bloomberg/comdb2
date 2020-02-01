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
#include "plhash.h"
#include "sql.h"
#include "util.h"

extern hash_t *gbl_fingerprint_hash;
extern pthread_mutex_t gbl_fingerprint_hash_mu;

/*
  comdb2_fingerprints: List all statistics per query fingerprint.
*/

typedef struct {
    sqlitex_vtab_cursor base; /* Base class - must be first */
    sqlitex_int64 rowid;      /* Row ID */
    int nfingerprints;
    int current_init_fingerprint;
    struct fingerprint_track **fingerprint;
    unsigned int bkt;
} systbl_fingerprints_cursor;

/* Column numbers (always keep the below table definition in sync). */
enum {
    COLUMN_FINGERPRINT,
    COLUMN_COUNT,
    COLUMN_TOTAL_COST,
    COLUMN_TOTAL_TIME,
    COLUMN_TOTAL_ROWS,
    COLUMN_NORMALIZED_SQL,
};

static int systblFingerprintsConnect(sqlitex *db, void *pAux, int argc,
                                     const char *const *argv,
                                     sqlitex_vtab **ppVtab, char **pErr)
{
    int rc;

    rc = sqlitex_declare_vtab(
        db, "CREATE TABLE comdb2_fingerprints(\"fingerprint\", \"count\", "
            "\"total_cost\", \"total_time\", \"total_rows\", \"normalized_sql\")");

    if (rc == SQLITE_OK) {
        if ((*ppVtab = sqlitex_malloc(sizeof(sqlitex_vtab))) == 0) {
            return SQLITE_NOMEM;
        }
        memset(*ppVtab, 0, sizeof(*ppVtab));
    }

    return 0;
}

static int systblFingerprintsBestIndex(sqlitex_vtab *tab,
                                       sqlitex_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

static int systblFingerprintsDisconnect(sqlitex_vtab *pVtab)
{
    sqlitex_free(pVtab);
    return SQLITE_OK;
}

/* Just make a shallow copy.  Fingerprints don't go away. */
int init_fingerprints_cursor(void *obj, void *arg) {
    struct fingerprint_track *t = (struct fingerprint_track*) obj;
    systbl_fingerprints_cursor *cur = (systbl_fingerprints_cursor*) arg; 
    cur->fingerprint[cur->current_init_fingerprint++] = t;
    return 0;
}

static int systblFingerprintsOpen(sqlitex_vtab *p,
                                  sqlitex_vtab_cursor **ppCursor)
{
    systbl_fingerprints_cursor *cur =
        sqlitex_malloc(sizeof(systbl_fingerprints_cursor));
    if (cur == 0) {
        return SQLITE_NOMEM;
    }
    memset(cur, 0, sizeof(*cur));

    /* I'm nervous about having this lock held across calls, so just make a copy here. */
    pthread_mutex_lock(&gbl_fingerprint_hash_mu);
    if (gbl_fingerprint_hash) {
        cur->nfingerprints = hash_get_num_entries(gbl_fingerprint_hash);
        cur->fingerprint = sqlitex_malloc(cur->nfingerprints * sizeof(struct fingerprint_track*));
        hash_for(gbl_fingerprint_hash, init_fingerprints_cursor, cur);
    }
    else {
        cur->nfingerprints = 0;
        cur->fingerprint = NULL;
    }
    pthread_mutex_unlock(&gbl_fingerprint_hash_mu);

    *ppCursor = &cur->base;
    return SQLITE_OK;
}

static int systblFingerprintsClose(sqlitex_vtab_cursor *cur)
{
    systbl_fingerprints_cursor *pCur = (systbl_fingerprints_cursor*) cur;
    if (pCur->fingerprint)
        sqlitex_free(pCur->fingerprint);
    sqlitex_free(cur);
    return SQLITE_OK;
}

static int systblFingerprintsFilter(sqlitex_vtab_cursor *pVtabCursor,
                                    int idxNum, const char *idxStr, int argc,
                                    sqlitex_value **argv)
{
    systbl_fingerprints_cursor *pCur =
        (systbl_fingerprints_cursor *)pVtabCursor;
    pCur->rowid = 0;
    return SQLITE_OK;
}

static int systblFingerprintsNext(sqlitex_vtab_cursor *cur)
{
    systbl_fingerprints_cursor *pCur = (systbl_fingerprints_cursor *)cur;
    assert(gbl_fingerprint_hash);
    pCur->rowid++;
    return SQLITE_OK;
}

static int systblFingerprintsEof(sqlitex_vtab_cursor *cur)
{
    systbl_fingerprints_cursor *pCur = (systbl_fingerprints_cursor *)cur;
    return (!gbl_fingerprint_hash ||
            (pCur->rowid >= pCur->nfingerprints))
               ? 1
               : 0;
}

static int systblFingerprintsColumn(sqlitex_vtab_cursor *cur,
                                    sqlitex_context *ctx, int pos)
{
    systbl_fingerprints_cursor *pCur;
    struct fingerprint_track *fingerprint;
    char *buf;

    pCur = (systbl_fingerprints_cursor *)cur;
    fingerprint = pCur->fingerprint[pCur->rowid];

    switch (pos) {
    case COLUMN_FINGERPRINT:
        buf = sqlitex_malloc(FINGERPRINTSZ * 2 + 1);
        util_tohex(buf, fingerprint->fingerprint, FINGERPRINTSZ);
        sqlitex_result_text(ctx, buf, -1, sqlitex_free);
        break;
    case COLUMN_COUNT:
        sqlitex_result_int64(ctx, fingerprint->count);
        break;
    case COLUMN_TOTAL_COST:
        sqlitex_result_int64(ctx, fingerprint->cost);
        break;
    case COLUMN_TOTAL_TIME:
        sqlitex_result_int64(ctx, fingerprint->time);
        break;
    case COLUMN_TOTAL_ROWS:
        sqlitex_result_int64(ctx, fingerprint->rows);
        break;
    case COLUMN_NORMALIZED_SQL:
        sqlitex_result_text(ctx, fingerprint->normalized_query, -1, NULL);
        break;
    default:
        assert(0);
    };

    return SQLITE_OK;
}

static int systblFingerprintsRowid(sqlitex_vtab_cursor *cur,
                                   sqlite_int64 *pRowid)
{
    systbl_fingerprints_cursor *pCur = (systbl_fingerprints_cursor *)cur;
    *pRowid = pCur->rowid;

    return SQLITE_OK;
}

const sqlitex_module systblFingerprintsModuleX = {
    0,                            /* iVersion */
    0,                            /* xCreate */
    systblFingerprintsConnect,    /* xConnect */
    systblFingerprintsBestIndex,  /* xBestIndex */
    systblFingerprintsDisconnect, /* xDisconnect */
    0,                            /* xDestroy */
    systblFingerprintsOpen,       /* xOpen - open a cursor */
    systblFingerprintsClose,      /* xClose - close a cursor */
    systblFingerprintsFilter,     /* xFilter - configure scan constraints */
    systblFingerprintsNext,       /* xNext - advance a cursor */
    systblFingerprintsEof,        /* xEof - check for end of scan */
    systblFingerprintsColumn,     /* xColumn - read data */
    systblFingerprintsRowid,      /* xRowid - read data */
    0,                            /* xUpdate */
    0,                            /* xBegin */
    0,                            /* xSync */
    0,                            /* xCommit */
    0,                            /* xRollback */
    0,                            /* xFindMethod */
    0,                            /* xRename */
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
