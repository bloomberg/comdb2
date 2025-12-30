/*
    Copyright 2022, Bloomberg Finance L.P.

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

#include "comdb2systblInt.h"
#include "comdb2.h"
#include "bdb_api.h"
#include "sqliteInt.h"
#include "vdbeInt.h"

struct logfiles_systable {
    const sqlite3_module *pModule;  /* The module for this virtual table */
    int nRef;                       /* Number of open cursors */
    char *zErrMsg;                  /* Error message from sqlite3_mprintf() */
};

struct logfiles_systable_cursor {
    sqlite3_vtab_cursor base;  /* Base class - must be first */
    uint32_t rowid; // aka log file number
    uint32_t maxrowid;
    void *logfile;
    uint32_t size;
};

// TODO: hold log deletion while this is active

int systblLogfilesConnect(sqlite3* db, void *pAux, int argc, const char* const *argv, sqlite3_vtab **ppVTab, char **pErrr) {
    int rc = sqlite3_declare_vtab(db, "CREATE TABLE comdb2_logfiles(lognum, maxlognum, logfile)");
    if( rc==SQLITE_OK ){
        struct logfiles_systable *pNew = sqlite3_malloc( sizeof(*pNew) );
        *ppVTab = (sqlite3_vtab*) pNew;
        if( pNew==0 ) return SQLITE_NOMEM;
        memset(pNew, 0, sizeof(*pNew));
    }
    return 0;
}

int systblLogfilesDisconnect(sqlite3_vtab *pVTab) {
    sqlite3_free(pVTab);
    return SQLITE_OK;
}

int systblLogfilesOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor) {
    struct logfiles_systable_cursor *cur = sqlite3_malloc(sizeof(struct logfiles_systable_cursor));
    memset(cur, 0, sizeof(struct logfiles_systable_cursor));
    *ppCursor = (sqlite3_vtab_cursor*) cur;
    return 0;
}

int systblLogfilesClose(sqlite3_vtab_cursor *p) {
    struct logfiles_systable_cursor *pCur = (struct logfiles_systable_cursor*) p;
    if (pCur->logfile)
        sqlite3_free(pCur->logfile);
    sqlite3_free(p);
    return 0;
}

int systblLogfilesNext(sqlite3_vtab_cursor *p) {
    struct logfiles_systable_cursor *pCur = (struct logfiles_systable_cursor*) p;
    if (pCur->logfile) {
        free(pCur->logfile);
        pCur->logfile = NULL;
    }
    pCur->rowid++;
    return 0;
}

int systblLogfilesEof(sqlite3_vtab_cursor *p) {
    struct logfiles_systable_cursor *pCur = (struct logfiles_systable_cursor*) p;

    if (pCur->rowid > pCur->maxrowid)
        return 1;

    return 0;
}

int systblLogfilesColumn(sqlite3_vtab_cursor *p, sqlite3_context *ctx, int i) {
    struct logfiles_systable_cursor *pCur = (struct logfiles_systable_cursor*) p;
    if (pCur->logfile == NULL) {
        int rc = bdb_fetch_log(thedb->bdb_env, pCur->rowid, &pCur->logfile, &pCur->size);
        if (rc) {
            // TODO: error string
            return SQLITE_INTERNAL;
        }
    }
    switch (i) {
        case 0:
            sqlite3_result_int(ctx, pCur->rowid);
            break;
        case 1: {
            sqlite3_result_int(ctx, pCur->maxrowid);
            break;
        }
        case 2: {
            void *copy = malloc(pCur->size);
            memcpy(copy, pCur->logfile, pCur->size);
            sqlite3_result_blob(ctx, copy, pCur->size, free);
            break;
        }
        default:
            return SQLITE_INTERNAL;
    }
    return SQLITE_OK;
}

int systblLogfilesRowid(sqlite3_vtab_cursor *p, sqlite3_int64 *pRowid) {
    struct logfiles_systable_cursor *pCur = (struct logfiles_systable_cursor*) p;
    *pRowid = pCur->rowid;
    return 0;
}

int systblLogfilesBestIndex(sqlite3_vtab *pVTab, sqlite3_index_info *info) {
    int argc = 0;
    int return_ordered = 0;

    if (info->nOrderBy == 1 && info->aOrderBy[0].iColumn == 0 && info->aOrderBy[0].desc == 0)
        return_ordered = 1;

    for (int i = 0; i < info->nConstraint; i++) {
        if (!info->aConstraint[i].usable)
            continue;
        if (info->aConstraint[i].iColumn == 0 && (
                        info->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_EQ ||
                        info->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_GE ||
                        info->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_GT ||
                        info->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_IS))
        {
            info->aConstraintUsage[i].argvIndex = ++argc;
            info->aConstraintUsage[i].omit = 0;
        }
    }
    if (argc > 0) {
        info->idxNum = 1;
        info->orderByConsumed = return_ordered;
    }
    return 0;
}

extern char *print_mem(sqlite3_value *v);

int systblLogfilesFilter(sqlite3_vtab_cursor *p, int idxNum, const char *idxStr, int argc, sqlite3_value **argv) {
    struct logfiles_systable_cursor *pCur = (struct logfiles_systable_cursor*) p;
    uint32_t first_log = 0;

    // int bdb_log_range(bdb_state_type *bdb_state, uint32_t *start_log, uint32_t *end_log);
    int rc = bdb_log_range(thedb->bdb_env, &first_log, &pCur->maxrowid);
    if (rc) {
        // TODO: error
        return SQLITE_INTERNAL;
    }

    if (idxNum == 1 && argc == 1 && argv[0]->flags & MEM_Int) {
        pCur->rowid = sqlite3_value_int(argv[0]);
    }
    else {
        pCur->rowid = first_log;
    }

    return SQLITE_OK;
}

static sqlite3_module systblLogfilesModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  systblLogfilesConnect,       /* xConnect */
  systblLogfilesBestIndex,                     /* xBestIndex */
  systblLogfilesDisconnect,    /* xDisconnect */
  0,                         /* xDestroy */
  systblLogfilesOpen,          /* xOpen - open a cursor */
  systblLogfilesClose,         /* xClose - close a cursor */
  systblLogfilesFilter,                         /* xFilter - configure scan constraints */
  systblLogfilesNext,          /* xNext - advance a cursor */
  systblLogfilesEof,           /* xEof - check for end of scan */
  systblLogfilesColumn,        /* xColumn - read data */
  systblLogfilesRowid,         /* xRowid - read data */
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

int systblLogfilesInit(sqlite3 *db) {
    int rc = sqlite3_create_module(db, "comdb2_logfiles", &systblLogfilesModule, 0);
    return rc;
}
