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
#include "tunables.h"
#include "comdb2.h"

/*
  comdb2_tunables: query various attributes of tunables.

  TODO(Nirbhay): Check user permissions
*/

typedef struct {
    sqlite3_vtab_cursor base; /* Base class - must be first */
    sqlite3_int64 rowid;      /* Row ID */
} systbl_tunables_cursor;

/* Column numbers (always keep the below table definition in sync). */
enum {
    TUNABLES_COLUMN_NAME,
    TUNABLES_COLUMN_DESCR,
    TUNABLES_COLUMN_TYPE,
    TUNABLES_COLUMN_VALUE,
    TUNABLES_COLUMN_READONLY,
};

static int systblTunablesConnect(sqlite3 *db, void *pAux, int argc,
                                 const char *const *argv, sqlite3_vtab **ppVtab,
                                 char **pErr)
{
    int rc;

    rc = sqlite3_declare_vtab(db, "CREATE TABLE comdb2_tunables(\"name\", "
                                  "\"description\", \"type\", \"value\", "
                                  "\"read_only\")");

    if (rc == SQLITE_OK) {
        if ((*ppVtab = sqlite3_malloc(sizeof(sqlite3_vtab))) == 0) {
            return SQLITE_NOMEM;
        }
        memset(*ppVtab, 0, sizeof(*ppVtab));
    }

    return 0;
}

static int systblTunablesBestIndex(sqlite3_vtab *tab,
                                   sqlite3_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

static int systblTunablesDisconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int systblTunablesOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor)
{
    systbl_tunables_cursor *cur =
        sqlite3_malloc(sizeof(systbl_tunables_cursor));
    if (cur == 0) {
        return SQLITE_NOMEM;
    }
    memset(cur, 0, sizeof(*cur));
    *ppCursor = &cur->base;
    return SQLITE_OK;
}

static int systblTunablesClose(sqlite3_vtab_cursor *cur)
{
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int systblTunablesFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                                const char *idxStr, int argc,
                                sqlite3_value **argv)
{
    systbl_tunables_cursor *pCur = (systbl_tunables_cursor *)pVtabCursor;
    pCur->rowid = 0;
    return SQLITE_OK;
}

static int systblTunablesNext(sqlite3_vtab_cursor *cur)
{
    systbl_tunables_cursor *pCur = (systbl_tunables_cursor *)cur;
    pCur->rowid++;
    return SQLITE_OK;
}

static int systblTunablesEof(sqlite3_vtab_cursor *cur)
{
    systbl_tunables_cursor *pCur = (systbl_tunables_cursor *)cur;
    return (pCur->rowid >= gbl_tunables->count) ? 1 : 0;
}

static int systblTunablesColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
                                int pos)
{
    comdb2_tunable *tunable =
        gbl_tunables->array[((systbl_tunables_cursor *)cur)->rowid];

    pthread_mutex_lock(&gbl_tunables->mu);

    switch (pos) {
    case TUNABLES_COLUMN_NAME:
        sqlite3_result_text(ctx, tunable->name, -1, NULL);
        break;
    case TUNABLES_COLUMN_DESCR:
        sqlite3_result_text(ctx, (tunable->descr) ? tunable->descr : "", -1,
                            NULL);
        break;
    case TUNABLES_COLUMN_TYPE:
        sqlite3_result_text(ctx, tunable_type(tunable->type), -1, NULL);
        break;
    case TUNABLES_COLUMN_VALUE:
        if (tunable->value) {
            sqlite3_result_text(ctx, tunable->value(tunable), -1, NULL);
        } else {
            switch (tunable->type) {
            case TUNABLE_INTEGER:
                sqlite3_result_int(ctx, *(int *)tunable->var);
                break;
            case TUNABLE_DOUBLE:
                sqlite3_result_double(ctx, *(double *)tunable->var);
                break;
            case TUNABLE_BOOLEAN: {
                int val = *(int *)tunable->var;
                if ((tunable->flags & INVERSE_VALUE) != 0) {
                    val = (val == 0) ? 1 : 0;
                }
                sqlite3_result_text(ctx, (val) ? "ON" : "OFF", -1, NULL);
                break;
            }
            case TUNABLE_STRING:
                sqlite3_result_text(ctx, *(char **)tunable->var, -1, NULL);
                break;
            case TUNABLE_ENUM: /* fallthrough */
            default: assert(0);
            }
        }
        break;
    case TUNABLES_COLUMN_READONLY:
        sqlite3_result_text(ctx, YESNO(tunable->flags & READONLY), -1, NULL);
        break;
    default: assert(0);
    };

    pthread_mutex_unlock(&gbl_tunables->mu);

    return SQLITE_OK;
}

static int systblTunablesRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    systbl_tunables_cursor *pCur = (systbl_tunables_cursor *)cur;
    *pRowid = pCur->rowid;

    return SQLITE_OK;
}

const sqlite3_module systblTunablesModule = {
    0,                        /* iVersion */
    0,                        /* xCreate */
    systblTunablesConnect,    /* xConnect */
    systblTunablesBestIndex,  /* xBestIndex */
    systblTunablesDisconnect, /* xDisconnect */
    0,                        /* xDestroy */
    systblTunablesOpen,       /* xOpen - open a cursor */
    systblTunablesClose,      /* xClose - close a cursor */
    systblTunablesFilter,     /* xFilter - configure scan constraints */
    systblTunablesNext,       /* xNext - advance a cursor */
    systblTunablesEof,        /* xEof - check for end of scan */
    systblTunablesColumn,     /* xColumn - read data */
    systblTunablesRowid,      /* xRowid - read data */
    0,                        /* xUpdate */
    0,                        /* xBegin */
    0,                        /* xSync */
    0,                        /* xCommit */
    0,                        /* xRollback */
    0,                        /* xFindMethod */
    0,                        /* xRename */
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
