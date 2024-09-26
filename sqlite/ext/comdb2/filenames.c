/*
   Copyright 2024 Bloomberg Finance L.P.

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

#include <assert.h>
#include <comdb2systblInt.h>
#include <ezsystables.h>
#include "files_util.h"
 
static int filenamesBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo)
{
    const int rc = files_util_best_index(tab, pIdxInfo);
    assert(rc != SQLITE_OK
           || pIdxInfo->idxNum == 0
           || pIdxInfo->idxNum == FILES_FILE_PATTERN_FLAG);

    return rc;
}

static int filenamesConnect(sqlite3 *db, void *pAux, int argc,
                        const char *const *argv, sqlite3_vtab **ppVtab,
                        char **pzErr)
{
    sqlite3_vtab *pNew;
    int rc;

    rc = sqlite3_declare_vtab( db, "CREATE TABLE x(filename, dir, type)");
    if (rc == SQLITE_OK) {
        pNew = *ppVtab = sqlite3_malloc(sizeof(*pNew));
        if (pNew == 0) return SQLITE_NOMEM;
        memset(pNew, 0, sizeof(*pNew));
    }
    return rc;
}

static int filenamesDisconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int filenamesOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor)
{
    return files_util_open(p, ppCursor);
}

static int filenamesClose(sqlite3_vtab_cursor *cur)
{
    return files_util_close(cur);
}

static int filenamesFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                       const char *idxStr, int argc, sqlite3_value **argv)
{
    assert(idxNum == 0
           || idxNum == FILES_FILE_PATTERN_FLAG);

    return files_util_filter(pVtabCursor, idxNum, idxStr, argc, argv);
}

static int filenamesEof(sqlite3_vtab_cursor *cur)
{
    return files_util_eof(cur);
}

static int filenamesNext(sqlite3_vtab_cursor *cur)
{
    systbl_files_cursor * const pCur = (systbl_files_cursor *) cur;
    ++pCur->rowid;
    return SQLITE_OK;
}

static int
filenamesColumn(sqlite3_vtab_cursor *cur, /* The cursor */
                sqlite3_context *ctx, /* First argument to sqlite3_result_...() */
                int i                 /* Which column to return */
) {
    assert(i == FILES_COLUMN_FILENAME
           || i == FILES_COLUMN_DIR
           || i == FILES_COLUMN_TYPE);
    return files_util_column(cur, ctx, i);
}

static int filenamesRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    return files_util_rowid(cur, pRowid);
}

/*
** This following structure defines all the methods for the
** generate_series virtual table.
*/
const sqlite3_module systblFilenamesModule = {
    0,               /* iVersion */
    0,               /* xCreate */
    filenamesConnect,    /* xConnect */
    filenamesBestIndex,  /* xBestIndex */
    filenamesDisconnect, /* xDisconnect */
    0,               /* xDestroy */
    filenamesOpen,       /* xOpen - open a cursor */
    filenamesClose,      /* xClose - close a cursor */
    filenamesFilter,     /* xFilter - configure scan constraints */
    filenamesNext,       /* xNext - advance a cursor */
    filenamesEof,        /* xEof - check for end of scan */
    filenamesColumn,     /* xColumn - read data */
    filenamesRowid,      /* xRowid - read data */
    0,               /* xUpdate */
    0,               /* xBegin */
    0,               /* xSync */
    0,               /* xCommit */
    0,               /* xRollback */
    0,               /* xFindMethod */
    0,               /* xRename */
    0,               /* xSavepoint */
    0,               /* xRelease */
    0,               /* xRollbackTo */
    0,               /* xShadowName */
    .access_flag = CDB2_ALLOW_USER};
