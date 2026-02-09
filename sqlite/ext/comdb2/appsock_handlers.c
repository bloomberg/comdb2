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

/*
  comdb2_appsock_handlers: list of registered appsock handlers.
*/

#include <assert.h>
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "comdb2_appsock.h"
#include <plhash_glue.h>

extern hash_t *gbl_appsock_hash;

typedef struct {
    sqlite3_vtab_cursor base; /* Base class - must be first */
    sqlite3_int64 rowid;      /* Row ID */
    comdb2_appsock_t *appsock;
    /* Used to save the state during iteration. */
    void *ent;
    unsigned int bkt;
} systbl_appsock_handlers_cursor;

/* Column numbers (always keep the below table definition in sync). */
enum {
    APPSOCK_COLUMN_NAME,
    APPSOCK_COLUMN_USAGE,
    APPSOCK_COLUMN_EXEC_COUNT,
};

static int systblAppsockHandlersConnect(sqlite3 *db, void *pAux, int argc,
                                        const char *const *argv,
                                        sqlite3_vtab **ppVtab, char **pErr)
{
    int rc;

    rc = sqlite3_declare_vtab(db, "CREATE TABLE "
                                  "comdb2_appsock_handlers(\"name\", "
                                  "\"usage\", \"exec_count\")");

    if (rc == SQLITE_OK) {
        if ((*ppVtab = sqlite3_malloc(sizeof(sqlite3_vtab))) == 0) {
            return SQLITE_NOMEM;
        }
        memset(*ppVtab, 0, sizeof(sqlite3_vtab));
    }

    return 0;
}

static int systblAppsockHandlersBestIndex(sqlite3_vtab *tab,
                                          sqlite3_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

static int systblAppsockHandlersDisconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int systblAppsockHandlersOpen(sqlite3_vtab *p,
                                     sqlite3_vtab_cursor **ppCursor)
{
    systbl_appsock_handlers_cursor *cur =
        sqlite3_malloc(sizeof(systbl_appsock_handlers_cursor));
    if (cur == 0) {
        return SQLITE_NOMEM;
    }
    memset(cur, 0, sizeof(*cur));

    /* Fetch the first appsock handler. */
    cur->appsock = hash_first(gbl_appsock_hash, &cur->ent, &cur->bkt);

    *ppCursor = &cur->base;
    return SQLITE_OK;
}

static int systblAppsockHandlersClose(sqlite3_vtab_cursor *cur)
{
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int systblAppsockHandlersFilter(sqlite3_vtab_cursor *pVtabCursor,
                                       int idxNum, const char *idxStr, int argc,
                                       sqlite3_value **argv)
{
    return SQLITE_OK;
}

static int systblAppsockHandlersNext(sqlite3_vtab_cursor *cur)
{
    systbl_appsock_handlers_cursor *pCur =
        (systbl_appsock_handlers_cursor *)cur;
    pCur->appsock = hash_next(gbl_appsock_hash, &pCur->ent, &pCur->bkt);
    pCur->rowid++;
    return SQLITE_OK;
}

static int systblAppsockHandlersEof(sqlite3_vtab_cursor *cur)
{
    systbl_appsock_handlers_cursor *pCur =
        (systbl_appsock_handlers_cursor *)cur;
    return (pCur->rowid >= hash_get_num_entries(gbl_appsock_hash)) ? 1 : 0;
}

static int systblAppsockHandlersColumn(sqlite3_vtab_cursor *cur,
                                       sqlite3_context *ctx, int pos)
{
    systbl_appsock_handlers_cursor *pCur =
        (systbl_appsock_handlers_cursor *)cur;
    comdb2_appsock_t *appsock = pCur->appsock;
    assert(appsock);

    switch (pos) {
    case APPSOCK_COLUMN_NAME:
        sqlite3_result_text(ctx, appsock->name, -1, NULL);
        break;
    case APPSOCK_COLUMN_USAGE:
        sqlite3_result_text(ctx, appsock->usage, -1, NULL);
        break;
    case APPSOCK_COLUMN_EXEC_COUNT:
        sqlite3_result_int(ctx, appsock->exec_count);
        break;
    default:
        assert(0);
    };

    return SQLITE_OK;
}

static int systblAppsockHandlersRowid(sqlite3_vtab_cursor *cur,
                                      sqlite_int64 *pRowid)
{
    systbl_appsock_handlers_cursor *pCur =
        (systbl_appsock_handlers_cursor *)cur;
    *pRowid = pCur->rowid;

    return SQLITE_OK;
}

const sqlite3_module systblAppsockHandlersModule = {
    0,                               /* iVersion */
    0,                               /* xCreate */
    systblAppsockHandlersConnect,    /* xConnect */
    systblAppsockHandlersBestIndex,  /* xBestIndex */
    systblAppsockHandlersDisconnect, /* xDisconnect */
    0,                               /* xDestroy */
    systblAppsockHandlersOpen,       /* xOpen - open a cursor */
    systblAppsockHandlersClose,      /* xClose - close a cursor */
    systblAppsockHandlersFilter,     /* xFilter - configure scan constraints */
    systblAppsockHandlersNext,       /* xNext - advance a cursor */
    systblAppsockHandlersEof,        /* xEof - check for end of scan */
    systblAppsockHandlersColumn,     /* xColumn - read data */
    systblAppsockHandlersRowid,      /* xRowid - read data */
    0,                               /* xUpdate */
    0,                               /* xBegin */
    0,                               /* xSync */
    0,                               /* xCommit */
    0,                               /* xRollback */
    0,                               /* xFindMethod */
    0,                               /* xRename */
    0,                               /* xSavepoint */
    0,                               /* xRelease */
    0,                               /* xRollbackTo */
    0,                               /* xShadowName */
    .access_flag = CDB2_ALLOW_USER,
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
