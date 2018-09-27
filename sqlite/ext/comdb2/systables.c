/*
   Copyright 2018 Bloomberg Finance L.P.

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
#include "sqliteInt.h"
#include "hash.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"

/*
  comdb2_systabs: System tables
*/

typedef struct {
    sqlite3_vtab_cursor base; /* Base class - must be first */
    sqlite3_int64 rowid;      /* Row ID */
    Hash *tabs;               /* Registered system tables */
    HashElem *current;        /* Current system table */
} systbl_systabs_cursor;

typedef struct {
    sqlite3_vtab base; /* Base class - must be first */
    sqlite3 *db;       /* To access registered system tables */
} systbl_systabs_vtab; /* such name !! */

/* Column numbers (always keep the below table definition in sync). */
enum { SYSTABS_COLUMN_NAME };

static int systblSystabsConnect(sqlite3 *db, void *pAux, int argc,
                                const char *const *argv, sqlite3_vtab **ppVtab,
                                char **pErr)
{
    int rc;
    systbl_systabs_vtab *systabs;

    rc = sqlite3_declare_vtab(db, "CREATE TABLE comdb2_systabs(\"name\")");

    if (rc == SQLITE_OK) {
        if ((systabs = sqlite3_malloc(sizeof(systbl_systabs_vtab))) == 0) {
            return SQLITE_NOMEM;
        }
        memset(systabs, 0, sizeof(systbl_systabs_vtab));
        systabs->db = db;
    }

    *ppVtab = (sqlite3_vtab *)systabs;

    return rc;
}

static int systblSystabsBestIndex(sqlite3_vtab *tab,
                                  sqlite3_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

static int systblSystabsDisconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int systblSystabsOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor)
{
    systbl_systabs_vtab *tab = (systbl_systabs_vtab *)p;

    systbl_systabs_cursor *cur = sqlite3_malloc(sizeof(systbl_systabs_cursor));
    if (cur == 0) {
        return SQLITE_NOMEM;
    }
    memset(cur, 0, sizeof(*cur));
    cur->tabs = &tab->db->aModule;
    cur->current = sqliteHashFirst(cur->tabs);

    *ppCursor = &cur->base;
    return SQLITE_OK;
}

static int systblSystabsClose(sqlite3_vtab_cursor *cur)
{
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int systblSystabsFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                               const char *idxStr, int argc,
                               sqlite3_value **argv)
{
    systbl_systabs_cursor *pCur = (systbl_systabs_cursor *)pVtabCursor;
    pCur->rowid = 0;
    return SQLITE_OK;
}

static int systblSystabsNext(sqlite3_vtab_cursor *cur)
{
    systbl_systabs_cursor *pCur = (systbl_systabs_cursor *)cur;
    pCur->current = sqliteHashNext(pCur->current);
    pCur->rowid++;
    return SQLITE_OK;
}

static int systblSystabsEof(sqlite3_vtab_cursor *cur)
{
    systbl_systabs_cursor *pCur = (systbl_systabs_cursor *)cur;
    return (pCur->rowid >= pCur->tabs->count) ? 1 : 0;
}

static int systblSystabsColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
                               int pos)
{
    systbl_systabs_cursor *pCur = (systbl_systabs_cursor *)cur;
    switch (pos) {
    case SYSTABS_COLUMN_NAME:
        sqlite3_result_text(ctx, pCur->current->pKey, -1, NULL);
        break;
    default:
        assert(0);
    };

    return SQLITE_OK;
}

static int systblSystabsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    systbl_systabs_cursor *pCur = (systbl_systabs_cursor *)cur;
    *pRowid = pCur->rowid;

    return SQLITE_OK;
}

const sqlite3_module systblSystabsModule = {
    0,                       /* iVersion */
    0,                       /* xCreate */
    systblSystabsConnect,    /* xConnect */
    systblSystabsBestIndex,  /* xBestIndex */
    systblSystabsDisconnect, /* xDisconnect */
    0,                       /* xDestroy */
    systblSystabsOpen,       /* xOpen - open a cursor */
    systblSystabsClose,      /* xClose - close a cursor */
    systblSystabsFilter,     /* xFilter - configure scan constraints */
    systblSystabsNext,       /* xNext - advance a cursor */
    systblSystabsEof,        /* xEof - check for end of scan */
    systblSystabsColumn,     /* xColumn - read data */
    systblSystabsRowid,      /* xRowid - read data */
    0,                       /* xUpdate */
    0,                       /* xBegin */
    0,                       /* xSync */
    0,                       /* xCommit */
    0,                       /* xRollback */
    0,                       /* xFindMethod */
    0,                       /* xRename */
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
