/*
   Copyright 2021, 2023, Bloomberg Finance L.P.

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

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
#define SQLITE_CORE 1
#endif

#include "files_util.h"

typedef unsigned char u_int8_t;

int endianness_mismatch(struct sqlclntstate *clnt);
void berk_fix_checkpoint_endianness(u_int8_t *buffer);

extern struct dbenv *thedb;

static void fix_checkpoint_endianness(uint8_t *buffer, size_t size)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    int do_flip = endianness_mismatch(thd->clnt);

    if (do_flip != 1) return;

    berk_fix_checkpoint_endianness(buffer);
}

static int memory_writer(void *ctx, uint8_t *in_buf, size_t size, size_t offset)
{
    systbl_files_cursor *pCur = (systbl_files_cursor *)ctx;
    db_file_t *file = &pCur->files[pCur->rowid];

    if (offset == 0) {
      file->chunk_seq = 0;
    } else {
      file->chunk_seq++;
      assert(file->current_chunk.buffer);
      free(file->current_chunk.buffer);
    }

    if (file->type == FILES_TYPE_CHECKPOINT) {
        fix_checkpoint_endianness(in_buf, size);
    }

    file->current_chunk.offset = offset;
    file->current_chunk.size = size;
    file->current_chunk.buffer = malloc(size);
    if (file->current_chunk.buffer == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d: out-of-memory\n", __FILE__, __LINE__);
        return SQLITE_NOMEM;
    }

    logmsg(LOGMSG_DEBUG, "%s:%d copying %ld bytes of %s(file):%ld(offset)\n",
           __func__, __LINE__, file->current_chunk.size, file->name, file->current_chunk.offset);

    memcpy(file->current_chunk.buffer, in_buf, size);
    return 0;
}

static int check_and_append_new_log_files(systbl_files_cursor *pCur)
{
    if (pCur->rowid < (pCur->nfiles - 1)) {
        // We are not processing the last log file, not need to do anything yet
        return 0;
    }

    long long logNumber = 0;

    const char *logFile = pCur->files[pCur->rowid].name;
    for (int pos = 4 /* strlen("log.") */;
         pos < strlen(logFile) && isdigit(logFile[pos]); ++pos) {
        logNumber = (logNumber * 10) + (int)(logFile[pos] - '0');
    }

    long long nextLogNumber = logNumber + 1;
    while (1) {
        struct stat st;
        char path[1024];
        char nextLogFile[32];

        snprintf(nextLogFile, sizeof(nextLogFile), "log.%010lld",
                 nextLogNumber);
        snprintf(path, sizeof(path), "%s/%s/%s", thedb->basedir,
                 pCur->files[pCur->rowid].dir, nextLogFile);
        if (stat(path, &st) != 0) {
            logmsg(LOGMSG_DEBUG, "%s:%d: %s does not exist\n", __func__,
                   __LINE__, path);
            break;
        }

        logmsg(LOGMSG_DEBUG, "%s:%d: %s exists, appending to the list\n",
               __func__, __LINE__, path);

        pCur->files =
            realloc(pCur->files, sizeof(db_file_t) * (++pCur->nfiles));
        if (!pCur->files) {
            logmsg(LOGMSG_ERROR, "%s:%d: out-of-memory\n", __FILE__, __LINE__);
            return SQLITE_NOMEM;
        }
        db_file_t *newFile = &pCur->files[pCur->nfiles - 1];

        newFile->name = strdup(nextLogFile);
        newFile->dir = strdup(path);
        newFile->current_chunk.buffer = NULL;
        newFile->current_chunk.size = 0; // To be determined at read time
        newFile->current_chunk.offset = 0;
        newFile->info = NULL;
        newFile->type = FILES_TYPE_LOGFILE;

        newFile->info = os_calloc(1, sizeof(dbfile_info));
        newFile->info->filename = os_strdup(path);

        // Continue to check if there are more newer log files.
        ++nextLogNumber;
    }
    return 0;
}

static int read_next_chunk(systbl_files_cursor *pCur)
{
    while (pCur->rowid < pCur->nfiles) {
        logmsg(LOGMSG_DEBUG, "%s:%d processing %s\n", __func__, __LINE__,
               pCur->files[pCur->rowid].name);

        if (pCur->files[pCur->rowid].type == FILES_TYPE_LOGFILE) {
            if (check_and_append_new_log_files(pCur) != 0) {
                logmsg(LOGMSG_ERROR, "%s:%d Failed to process file %s\n", __func__, __LINE__, pCur->files[pCur->rowid].name);
                return SQLITE_ERROR;
            }
        }

        int rc = read_write_file(pCur->files[pCur->rowid].info, pCur, memory_writer);
        if (rc > 0) {
            logmsg(LOGMSG_ERROR, "%s:%d Failed to process file %s\n", __func__, __LINE__, pCur->files[pCur->rowid].name);
            return SQLITE_ERROR;
        } else if (rc == 0) {
            break;
        }
        assert(rc == -1);

        // This buffer is allocated when the first chunk in a file
        // is processed. It is reused for each subsequent chunk in
        // the file. Now we are about to move to the next file,
        // so we can free the buffer for the current file.
        if (pCur->files[pCur->rowid].info->pagebuf) {
            os_free(pCur->files[pCur->rowid].info->pagebuf);
            pCur->files[pCur->rowid].info->pagebuf = NULL;
        }

        pCur->rowid++; // Read the next file
    }

    return SQLITE_OK;
}

static int filesConnect(sqlite3 *db, void *pAux, int argc,
                        const char *const *argv, sqlite3_vtab **ppVtab,
                        char **pzErr)
{
    sqlite3_vtab *pNew;
    int rc;

    rc = sqlite3_declare_vtab( db, "CREATE TABLE x(filename, dir, type, content, offset, size, chunk_size hidden)");
    if (rc == SQLITE_OK) {
        pNew = *ppVtab = sqlite3_malloc(sizeof(*pNew));
        if (pNew == 0) return SQLITE_NOMEM;
        memset(pNew, 0, sizeof(*pNew));
    }
    return rc;
}

static int filesDisconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int filesOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor)
{
    return files_util_open(p, ppCursor);
}

static int filesClose(sqlite3_vtab_cursor *cur)
{
    return files_util_close(cur);
}

static int filesNext(sqlite3_vtab_cursor *cur)
{
    systbl_files_cursor *pCur = (systbl_files_cursor *)cur;
    return (read_next_chunk(pCur)) ? SQLITE_ERROR : SQLITE_OK;
}

static int
filesColumn(sqlite3_vtab_cursor *cur, /* The cursor */
            sqlite3_context *ctx, /* First argument to sqlite3_result_...() */
            int i                 /* Which column to return */
)
{
    return files_util_column(cur, ctx, i);
}

static int filesRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    return files_util_rowid(cur, pRowid);
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int filesEof(sqlite3_vtab_cursor *cur)
{
    return files_util_eof(cur);
}

static int filesFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                       const char *idxStr, int argc, sqlite3_value **argv)
{
    const int rc = files_util_filter(pVtabCursor, idxNum, idxStr, argc, argv);
    if (rc) { return rc; }

    systbl_files_cursor * const pCur = (systbl_files_cursor *)pVtabCursor;
    return read_next_chunk(pCur) ? SQLITE_ERROR : SQLITE_OK;
}

static int filesBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo)
{
    return files_util_best_index(tab, pIdxInfo);
}

/*
** This following structure defines all the methods for the
** generate_series virtual table.
*/
const sqlite3_module systblFilesModule = {
    0,               /* iVersion */
    0,               /* xCreate */
    filesConnect,    /* xConnect */
    filesBestIndex,  /* xBestIndex */
    filesDisconnect, /* xDisconnect */
    0,               /* xDestroy */
    filesOpen,       /* xOpen - open a cursor */
    filesClose,      /* xClose - close a cursor */
    filesFilter,     /* xFilter - configure scan constraints */
    filesNext,       /* xNext - advance a cursor */
    filesEof,        /* xEof - check for end of scan */
    filesColumn,     /* xColumn - read data */
    filesRowid,      /* xRowid - read data */
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
