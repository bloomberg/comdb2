/*
   Copyright 2021 Bloomberg Finance L.P.

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

#include <stdlib.h>
#include <zlib.h>

#include "comdb2.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "eventlog.h"

/*
 * Exposes eventlog as a system table.  Handy for queries like
 *

select
  json_extract(value, '$.time') as time,
  json_extract(value, '$.fingerprint') as fingerprint,
  normalized_sql,
  json_extract(value, '$.perf.tottime') as runtime
from comdb2_events
join comdb2_fingerprints on fingerprint = comdb2_fingerprints.fingerprint
where json_extract(value, '$.type') = 'sql'

 ie: order the queries run by runtime

 I had grandiose plans where this can retrieve any log file (statreqs, longreqs, trc.c, etc.)
 That would require a little more work to make generic (specify a pattern to a "collect logs" call,
 specify whether logs are compressed, etc.)  Stopped at evenlogs since they're the only structured
 logs, and have the most value returned as row data.

 */

struct sqlite3_vtab_files {
    const sqlite3_module *pModule;  /* The module for this virtual table */
    int nRef;                       /* Number of open cursors */
    char *zErrMsg;                  /* Error message from sqlite3_mprintf() */
    /* custom fields */
};

#define MAX_LINE (16*1024)

struct sqlite3_vtab_cursor_files {
    sqlite3_vtab *pVtab;      /* Virtual table of this cursor */
    // above is what sqlite expects, we own the rest
    sqlite_int64 rowid;
    int eof;            // end of result set
    int current_eof;    // end of current file

    int nfiles;
    char **files;
    gzFile f;
    char current_line[MAX_LINE];
};

static void collect_files(struct sqlite3_vtab_files *vt) {
}

static int systblFilesConnect(sqlite3 *db, void *pAux, int argc,
                            const char *const *argv, sqlite3_vtab **ppVtab,
                            char **pErr) {
    *pErr = NULL;
    struct sqlite3_vtab_files *vt = calloc(1, sizeof(struct sqlite3_vtab_files));
    *ppVtab = (sqlite3_vtab*) vt;
    collect_files(vt);
    int rc = sqlite3_declare_vtab(db, "create table comdb2_events(value)");
    return rc;
}

static int systblFilesBestIndex(sqlite3_vtab *tab,
                              sqlite3_index_info *pIdxInfo) {
    return SQLITE_OK;
}

static int systblFilesDisconnect(sqlite3_vtab *pVtab) {
    struct sqlite3_vtab_files *vt = (struct sqlite3_vtab_files*)pVtab;
    free(vt);
    return 0;
}

static int systblFilesOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor) {
    struct sqlite3_vtab_cursor_files *c = calloc(1, sizeof(struct sqlite3_vtab_cursor_files));
    *ppCursor = (sqlite3_vtab_cursor*) c;

    c->nfiles = eventlog_list_files(&c->files);
    if (c->nfiles == -1) {
        // TODO: pass error string? Valid ways to get here, eg if log directory doesn't exist.
        return SQLITE_ERROR;
    }
    c->rowid = 0;
    c->eof = 0;
    c->current_eof = 1;

    return SQLITE_OK;
}

static int systblFilesClose(sqlite3_vtab_cursor *cur) {
    struct sqlite3_vtab_cursor_files *c = (struct sqlite3_vtab_cursor_files*) cur;
    free(c->files);
    free(c);
    return SQLITE_OK;
}

static int systblFilesFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                           const char *idxStr, int argc,
                           sqlite3_value **argv) {
    return SQLITE_OK;
}

static int systblFilesNext(sqlite3_vtab_cursor *cur) {
    struct sqlite3_vtab_cursor_files *c = (struct sqlite3_vtab_cursor_files *) cur;
    // If we're at the end of the current file, advance to the next one
again:
    while (c->current_eof) {
        if (c->rowid + 1 == c->nfiles) {
            c->eof = 1;
            return 0;
        }
        c->rowid++;
        if (c->f) {
            gzclose(c->f);
            c->f = NULL;
        }
        c->f = gzopen64(c->files[c->rowid], "r");
        if (c->f != NULL)
            break;
    }
    if (gzgets(c->f, c->current_line, sizeof(c->current_line)) == Z_NULL) {
        c->current_eof = 1;
        goto again;
    }
    // We may have incomplete entries (for unflushed logs). Consider anything not newline-terminated as incomplete
    char *s = strchr(c->current_line, '\n');
    if (s == NULL) {
        c->current_eof = 1;
        goto again;
    }
    else
        *s = 0;

    c->current_eof = 0;

    return SQLITE_OK;
}

static int systblFilesEof(sqlite3_vtab_cursor *cur) {
    struct sqlite3_vtab_cursor_files *c = (struct sqlite3_vtab_cursor_files*) cur;
    if (c->current_eof) {
        int rc = systblFilesNext(cur);
        if (rc)
            return rc;
    }
    if (c->rowid == c->nfiles) {
        c->eof = 1;
    }
    return c->eof;
}

static int systblFilesColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
                           int pos) {
    struct sqlite3_vtab_cursor_files *c = (struct sqlite3_vtab_cursor_files*) cur;
    assert(pos == 0);
    sqlite3_result_text(ctx, c->current_line, -1, SQLITE_TRANSIENT);
    return SQLITE_OK;
}

static int systblFilesRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid) {
    struct sqlite3_vtab_cursor_files *c = (struct sqlite3_vtab_cursor_files*) cur;
    *pRowid = c->rowid;
    return 0;
}

static const sqlite3_module systblFilesModule = {
        0,                       /* iVersion */
        0,                       /* xCreate */
        systblFilesConnect,    /* xConnect */
        systblFilesBestIndex,  /* xBestIndex */
        systblFilesDisconnect, /* xDisconnect */
        0,                       /* xDestroy */
        systblFilesOpen,       /* xOpen - open a cursor */
        systblFilesClose,      /* xClose - close a cursor */
        systblFilesFilter,     /* xFilter - configure scan constraints */
        systblFilesNext,       /* xNext - advance a cursor */
        systblFilesEof,        /* xEof - check for end of scan */
        systblFilesColumn,     /* xColumn - read data */
        systblFilesRowid,      /* xRowid - read data */
        0,                       /* xUpdate */
        0,                       /* xBegin */
        0,                       /* xSync */
        0,                       /* xCommit */
        0,                       /* xRollback */
        0,                       /* xFindMethod */
        0,                       /* xRename */
        0,                       /* xSavepoint */
        0,                       /* xRelease */
        0,                       /* xRollbackTo */
        0,                       /* xShadowName */
        .access_flag = CDB2_ALLOW_ALL,
};

int systblFilesInit(sqlite3 *db) {
    int rc =  sqlite3_create_module(db, "comdb2_events", &systblFilesModule, NULL);
    return rc;
}
