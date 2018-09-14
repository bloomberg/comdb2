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

/*
  comdb2_plugins: list of installed plugins.
*/

#include <assert.h>
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "comdb2_plugin.h"

typedef struct {
    sqlite3_vtab_cursor base; /* Base class - must be first */
    sqlite3_int64 rowid;      /* Row ID */
} systbl_plugins_cursor;

/* Column numbers (always keep the below table definition in sync). */
enum {
    PLUGINS_COLUMN_NAME,
    PLUGINS_COLUMN_DESCR,
    PLUGINS_COLUMN_TYPE,
    PLUGINS_COLUMN_VERSION,
    PLUGINS_COLUMN_IS_STATIC,
};

static int systblPluginsConnect(sqlite3 *db, void *pAux, int argc,
                                const char *const *argv, sqlite3_vtab **ppVtab,
                                char **pErr)
{
    int rc;

    rc = sqlite3_declare_vtab(
        db, "CREATE TABLE comdb2_plugins(\"name\", "
            "\"description\", \"type\", \"version\", \"is_static\")");

    if (rc == SQLITE_OK) {
        if ((*ppVtab = sqlite3_malloc(sizeof(sqlite3_vtab))) == 0) {
            return SQLITE_NOMEM;
        }
        memset(*ppVtab, 0, sizeof(*ppVtab));
    }

    return 0;
}

static int systblPluginsBestIndex(sqlite3_vtab *tab,
                                  sqlite3_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

static int systblPluginsDisconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int systblPluginsOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor)
{
    systbl_plugins_cursor *cur = sqlite3_malloc(sizeof(systbl_plugins_cursor));
    if (cur == 0) {
        return SQLITE_NOMEM;
    }
    memset(cur, 0, sizeof(*cur));
    *ppCursor = &cur->base;
    return SQLITE_OK;
}

static int systblPluginsClose(sqlite3_vtab_cursor *cur)
{
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int systblPluginsFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                               const char *idxStr, int argc,
                               sqlite3_value **argv)
{
    systbl_plugins_cursor *pCur = (systbl_plugins_cursor *)pVtabCursor;
    pCur->rowid = 0;
    return SQLITE_OK;
}

static int systblPluginsNext(sqlite3_vtab_cursor *cur)
{
    systbl_plugins_cursor *pCur = (systbl_plugins_cursor *)cur;
    pCur->rowid++;
    return SQLITE_OK;
}

static int systblPluginsEof(sqlite3_vtab_cursor *cur)
{
    systbl_plugins_cursor *pCur = (systbl_plugins_cursor *)cur;
    return (gbl_plugins[pCur->rowid]) ? 0 : 1;
}

static int systblPluginsColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
                               int pos)
{
    comdb2_plugin_t *plugin =
        gbl_plugins[((systbl_plugins_cursor *)cur)->rowid];
    switch (pos) {
    case PLUGINS_COLUMN_NAME:
        sqlite3_result_text(ctx, plugin->name, -1, NULL);
        break;
    case PLUGINS_COLUMN_DESCR:
        sqlite3_result_text(ctx, plugin->descr, -1, NULL);
        break;
    case PLUGINS_COLUMN_TYPE:
        sqlite3_result_text(ctx, comdb2_plugin_type_to_str(plugin->type), -1,
                            NULL);
        break;
    case PLUGINS_COLUMN_VERSION:
        sqlite3_result_int(ctx, plugin->version);
        break;
    case PLUGINS_COLUMN_IS_STATIC:
        sqlite3_result_text(ctx, YESNO(plugin->flags & COMDB2_PLUGIN_STATIC),
                            -1, NULL);
        break;
    default:
        assert(0);
    };

    return SQLITE_OK;
}

static int systblPluginsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    systbl_plugins_cursor *pCur = (systbl_plugins_cursor *)cur;
    *pRowid = pCur->rowid;

    return SQLITE_OK;
}

const sqlite3_module systblPluginsModule = {
    0,                       /* iVersion */
    0,                       /* xCreate */
    systblPluginsConnect,    /* xConnect */
    systblPluginsBestIndex,  /* xBestIndex */
    systblPluginsDisconnect, /* xDisconnect */
    0,                       /* xDestroy */
    systblPluginsOpen,       /* xOpen - open a cursor */
    systblPluginsClose,      /* xClose - close a cursor */
    systblPluginsFilter,     /* xFilter - configure scan constraints */
    systblPluginsNext,       /* xNext - advance a cursor */
    systblPluginsEof,        /* xEof - check for end of scan */
    systblPluginsColumn,     /* xColumn - read data */
    systblPluginsRowid,      /* xRowid - read data */
    0,                       /* xUpdate */
    0,                       /* xBegin */
    0,                       /* xSync */
    0,                       /* xCommit */
    0,                       /* xRollback */
    0,                       /* xFindMethod */
    0,                       /* xRename */
    .access_flag = ALLOW_USER,
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
