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

#include <string.h>
#include <assert.h>
#include "comdb2systbl.h"
#include "comdb2systblInt.h"

#define INCLUDE_KEYWORDHASH_H
#include "keywordhash.h" /* SQLITE_N_KEYWORD */

/*
  [NC] We need the following struct definition here to
  avoid multiple redefinitions of f_keywords.
*/
typedef struct {
    char reserved;
    char *name;
} FinalKeyword_t;

extern FinalKeyword_t f_keywords[];

/*
  comdb2_keywords: list of keywords
*/

typedef struct {
    sqlite3_vtab_cursor base; /* Base class - must be first */
    sqlite3_int64 rowid;      /* Row ID */
} systbl_keywords_cursor;

/* Column numbers (always keep the below table definition in sync). */
enum {
    KEYWORDS_COLUMN_NAME,
    KEYWORDS_COLUMN_RESERVED,
};

static int systblKeywordsConnect(sqlite3 *db, void *pAux, int argc,
                                 const char *const *argv, sqlite3_vtab **ppVtab,
                                 char **pErr)
{
    int rc;

    rc = sqlite3_declare_vtab(
        db, "CREATE TABLE comdb2_keywords(\"name\", \"reserved\")");

    if (rc == SQLITE_OK) {
        if ((*ppVtab = sqlite3_malloc(sizeof(sqlite3_vtab))) == 0) {
            return SQLITE_NOMEM;
        }
        memset(*ppVtab, 0, sizeof(*ppVtab));
    }

    return 0;
}

static int systblKeywordsBestIndex(sqlite3_vtab *tab,
                                   sqlite3_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

static int systblKeywordsDisconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int systblKeywordsOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor)
{
    systbl_keywords_cursor *cur =
        sqlite3_malloc(sizeof(systbl_keywords_cursor));
    if (cur == 0) {
        return SQLITE_NOMEM;
    }
    memset(cur, 0, sizeof(*cur));
    *ppCursor = &cur->base;
    return SQLITE_OK;
}

static int systblKeywordsClose(sqlite3_vtab_cursor *cur)
{
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int systblKeywordsFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                                const char *idxStr, int argc,
                                sqlite3_value **argv)
{
    systbl_keywords_cursor *pCur = (systbl_keywords_cursor *)pVtabCursor;
    pCur->rowid = 0;
    return SQLITE_OK;
}

static int systblKeywordsNext(sqlite3_vtab_cursor *cur)
{
    systbl_keywords_cursor *pCur = (systbl_keywords_cursor *)cur;
    pCur->rowid++;
    return SQLITE_OK;
}

static int systblKeywordsEof(sqlite3_vtab_cursor *cur)
{
    systbl_keywords_cursor *pCur = (systbl_keywords_cursor *)cur;
    return (pCur->rowid >= SQLITE_N_KEYWORD) ? 1 : 0;
}

static int systblKeywordsColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
                                int pos)
{
    switch (pos) {
    case KEYWORDS_COLUMN_NAME:
        sqlite3_result_text(
            ctx, f_keywords[((systbl_keywords_cursor *)cur)->rowid].name, -1,
            NULL);
        break;
    case KEYWORDS_COLUMN_RESERVED:
        sqlite3_result_text(
            ctx,
            YESNO(f_keywords[((systbl_keywords_cursor *)cur)->rowid].reserved ==
                  1),
            -1, NULL);
        break;
    default: assert(0);
    };

    return SQLITE_OK;
}

static int systblKeywordsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    systbl_keywords_cursor *pCur = (systbl_keywords_cursor *)cur;
    *pRowid = pCur->rowid;

    return SQLITE_OK;
}

const sqlite3_module systblKeywordsModule = {
    0,                        /* iVersion */
    0,                        /* xCreate */
    systblKeywordsConnect,    /* xConnect */
    systblKeywordsBestIndex,  /* xBestIndex */
    systblKeywordsDisconnect, /* xDisconnect */
    0,                        /* xDestroy */
    systblKeywordsOpen,       /* xOpen - open a cursor */
    systblKeywordsClose,      /* xClose - close a cursor */
    systblKeywordsFilter,     /* xFilter - configure scan constraints */
    systblKeywordsNext,       /* xNext - advance a cursor */
    systblKeywordsEof,        /* xEof - check for end of scan */
    systblKeywordsColumn,     /* xColumn - read data */
    systblKeywordsRowid,      /* xRowid - read data */
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
