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
  comdb2_opcode_handlers: list of installed opcode handlers.
*/

struct ireq;

#include <string.h>
#include <assert.h>
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "comdb2_opcode.h"
#include <plhash_glue.h>

extern hash_t *gbl_opcode_hash;

typedef struct {
    sqlite3_vtab_cursor base; /* Base class - must be first */
    sqlite3_int64 rowid;      /* Row ID */
    comdb2_opcode_t *opcode;
    /* Used to save the state during iteration. */
    void *ent;
    unsigned int bkt;
} systbl_opcode_handlers_cursor;

/* Column numbers (always keep the below table definition in sync). */
enum {
    PLUGINS_COLUMN_OPCODE,
    PLUGINS_COLUMN_NAME,
};

static int systblOpcodeHandlersConnect(sqlite3 *db, void *pAux, int argc,
                                       const char *const *argv,
                                       sqlite3_vtab **ppVtab, char **pErr)
{
    int rc;

    rc = sqlite3_declare_vtab(
        db, "CREATE TABLE comdb2_opcode_handlers(\"opcode\", \"name\")");

    if (rc == SQLITE_OK) {
        if ((*ppVtab = sqlite3_malloc(sizeof(sqlite3_vtab))) == 0) {
            return SQLITE_NOMEM;
        }
        memset(*ppVtab, 0, sizeof(sqlite3_vtab));
    }

    return 0;
}

static int systblOpcodeHandlersBestIndex(sqlite3_vtab *tab,
                                         sqlite3_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

static int systblOpcodeHandlersDisconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int systblOpcodeHandlersOpen(sqlite3_vtab *p,
                                    sqlite3_vtab_cursor **ppCursor)
{
    systbl_opcode_handlers_cursor *cur =
        sqlite3_malloc(sizeof(systbl_opcode_handlers_cursor));
    if (cur == 0) {
        return SQLITE_NOMEM;
    }
    memset(cur, 0, sizeof(*cur));

    /* Fetch the first opcode handler. */
    cur->opcode = hash_first(gbl_opcode_hash, &cur->ent, &cur->bkt);

    *ppCursor = &cur->base;
    return SQLITE_OK;
}

static int systblOpcodeHandlersClose(sqlite3_vtab_cursor *cur)
{
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int systblOpcodeHandlersFilter(sqlite3_vtab_cursor *pVtabCursor,
                                      int idxNum, const char *idxStr, int argc,
                                      sqlite3_value **argv)
{
    return SQLITE_OK;
}

static int systblOpcodeHandlersNext(sqlite3_vtab_cursor *cur)
{
    systbl_opcode_handlers_cursor *pCur = (systbl_opcode_handlers_cursor *)cur;
    pCur->opcode = hash_next(gbl_opcode_hash, &pCur->ent, &pCur->bkt);
    pCur->rowid++;
    return SQLITE_OK;
}

static int systblOpcodeHandlersEof(sqlite3_vtab_cursor *cur)
{
    systbl_opcode_handlers_cursor *pCur = (systbl_opcode_handlers_cursor *)cur;
    return (pCur->rowid >= hash_get_num_entries(gbl_opcode_hash)) ? 1 : 0;
}

static int systblOpcodeHandlersColumn(sqlite3_vtab_cursor *cur,
                                      sqlite3_context *ctx, int pos)
{
    systbl_opcode_handlers_cursor *pCur = (systbl_opcode_handlers_cursor *)cur;
    comdb2_opcode_t *opcode = pCur->opcode;
    assert(opcode);
    switch (pos) {
    case PLUGINS_COLUMN_OPCODE:
        sqlite3_result_int(ctx, opcode->opcode);
        break;
    case PLUGINS_COLUMN_NAME:
        sqlite3_result_text(ctx, opcode->name, -1, NULL);
        break;
    default:
        assert(0);
    };

    return SQLITE_OK;
}

static int systblOpcodeHandlersRowid(sqlite3_vtab_cursor *cur,
                                     sqlite_int64 *pRowid)
{
    systbl_opcode_handlers_cursor *pCur = (systbl_opcode_handlers_cursor *)cur;
    *pRowid = pCur->rowid;

    return SQLITE_OK;
}

const sqlite3_module systblOpcodeHandlersModule = {
    0,                              /* iVersion */
    0,                              /* xCreate */
    systblOpcodeHandlersConnect,    /* xConnect */
    systblOpcodeHandlersBestIndex,  /* xBestIndex */
    systblOpcodeHandlersDisconnect, /* xDisconnect */
    0,                              /* xDestroy */
    systblOpcodeHandlersOpen,       /* xOpen - open a cursor */
    systblOpcodeHandlersClose,      /* xClose - close a cursor */
    systblOpcodeHandlersFilter,     /* xFilter - configure scan constraints */
    systblOpcodeHandlersNext,       /* xNext - advance a cursor */
    systblOpcodeHandlersEof,        /* xEof - check for end of scan */
    systblOpcodeHandlersColumn,     /* xColumn - read data */
    systblOpcodeHandlersRowid,      /* xRowid - read data */
    0,                              /* xUpdate */
    0,                              /* xBegin */
    0,                              /* xSync */
    0,                              /* xCommit */
    0,                              /* xRollback */
    0,                              /* xFindMethod */
    0,                              /* xRename */
    0,                              /* xSavepoint */
    0,                              /* xRelease */
    0,                              /* xRollbackTo */
    0,                              /* xShadowName */
    .access_flag = CDB2_ALLOW_USER,
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
