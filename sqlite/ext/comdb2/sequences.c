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
#include "sequences.h"
#include "comdb2.h"

/*
comdb2_sequences: query various attributes of sequences.

next_start_val would only be valid on master
*/

typedef struct {
    sqlite3_vtab_cursor base; /* Base class - must be first */
    sqlite3_int64 rowid;      /* Row ID */
} systbl_sequences_cursor;

/* Column numbers (always keep the below table definition in sync). */
enum {
    SEQUENCE_COLUMN_NAME,
    SEQUENCE_COLUMN_MINVALUE,
    SEQUENCE_COLUMN_MAXVALUE,
    SEQUENCE_COLUMN_INCREMENT,
    SEQUENCE_COLUMN_STARTVAL,
    SEQUENCE_COLUMN_NEXT_STARTVAL,
    SEQUENCE_COLUMN_CHUNK,
    SEQUENCE_COLUMN_CYCLE,
    SEQUENCE_COLUMN_EXHAUSTED,
};

static int systblSequencesConnect(sqlite3 *db, void *pAux, int argc,
                                 const char *const *argv, sqlite3_vtab **ppVtab,
                                 char **pErr)
{
    int rc;

    rc = sqlite3_declare_vtab(db, "CREATE TABLE comdb2_sequences(\"name\", "
                                  "\"min_val=\", \"max_val\", \"increment\", "
                                  "\"start_val\", \"next_start_val\", "
                                  "\"chunk_size\", \"cycle\", \"exhausted\")");

    if (rc == SQLITE_OK) {
        if ((*ppVtab = sqlite3_malloc(sizeof(sqlite3_vtab))) == 0) {
            return SQLITE_NOMEM;
        }
        memset(*ppVtab, 0, sizeof(*ppVtab));
    }

    return 0;
}

static int systblSequencesBestIndex(sqlite3_vtab *tab,
                                   sqlite3_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

static int systblSequencesDisconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int systblSequencesOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor)
{
    systbl_sequences_cursor *cur =
        sqlite3_malloc(sizeof(systbl_sequences_cursor));
    if (cur == 0) {
        return SQLITE_NOMEM;
    }
    memset(cur, 0, sizeof(*cur));
    *ppCursor = &cur->base;
    return SQLITE_OK;
}

static int systblSequencesClose(sqlite3_vtab_cursor *cur)
{
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int systblSequencesFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                                const char *idxStr, int argc,
                                sqlite3_value **argv)
{
    systbl_sequences_cursor *pCur = (systbl_sequences_cursor *)pVtabCursor;
    pCur->rowid = 0;
    return SQLITE_OK;
}

static int systblSequencesNext(sqlite3_vtab_cursor *cur)
{
    sequence_t *sequence;
    systbl_sequences_cursor *pCur = (systbl_sequences_cursor *) cur;

    pCur->rowid++;
    if (pCur->rowid >= thedb->num_sequences){
        sequence = thedb->sequences[((systbl_sequences_cursor *)cur)->rowid];
    }

    return SQLITE_OK;
}

static int systblSequencesEof(sqlite3_vtab_cursor *cur)
{
    systbl_sequences_cursor *pCur = (systbl_sequences_cursor *)cur;
    return (pCur->rowid >= thedb->num_sequences) ? 1 : 0;
}

static int systblSequencesColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
                                int pos)
{
    sequence_t *sequence =
        thedb->sequences[((systbl_sequences_cursor *)cur)->rowid];

    pthread_mutex_lock(&sequence->seq_lk);

    switch (pos) {
    case SEQUENCE_COLUMN_NAME:
        sqlite3_result_text(ctx, sequence->name, -1, NULL);
        break;
    case SEQUENCE_COLUMN_MINVALUE:
        sqlite3_result_int64(ctx, sequence->min_val);
        break;
    case SEQUENCE_COLUMN_MAXVALUE:
        sqlite3_result_int64(ctx, sequence->max_val);
        break;
    case SEQUENCE_COLUMN_INCREMENT:
        sqlite3_result_int64(ctx, sequence->increment);
        break;
    case SEQUENCE_COLUMN_STARTVAL:
        sqlite3_result_int64(ctx, sequence->start_val);
        break;
    case SEQUENCE_COLUMN_NEXT_STARTVAL:
        sqlite3_result_int64(ctx, sequence->next_start_val);
        break;
    case SEQUENCE_COLUMN_CHUNK:
        sqlite3_result_int64(ctx, sequence->chunk_size);
        break;
    case SEQUENCE_COLUMN_CYCLE:
        sqlite3_result_text(ctx, YESNO(sequence->cycle), -1, NULL);
        break;
    case SEQUENCE_COLUMN_EXHAUSTED:
        sqlite3_result_text(ctx, YESNO(sequence->flags & SEQUENCE_EXHAUSTED),
                            -1, NULL);
        break;
    default:
        assert(0);
    };

    pthread_mutex_unlock(&sequence->seq_lk);

    return SQLITE_OK;
}

static int systblSequencesRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    systbl_sequences_cursor *pCur = (systbl_sequences_cursor *)cur;
    *pRowid = pCur->rowid;

    return SQLITE_OK;
}

const sqlite3_module systblSequencesModule = {
    0,                        /* iVersion */
    0,                        /* xCreate */
    systblSequencesConnect,    /* xConnect */
    systblSequencesBestIndex,  /* xBestIndex */
    systblSequencesDisconnect, /* xDisconnect */
    0,                        /* xDestroy */
    systblSequencesOpen,       /* xOpen - open a cursor */
    systblSequencesClose,      /* xClose - close a cursor */
    systblSequencesFilter,     /* xFilter - configure scan constraints */
    systblSequencesNext,       /* xNext - advance a cursor */
    systblSequencesEof,        /* xEof - check for end of scan */
    systblSequencesColumn,     /* xColumn - read data */
    systblSequencesRowid,      /* xRowid - read data */
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
