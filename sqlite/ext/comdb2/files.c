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

#include <fcntl.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <comdb2systblInt.h>
#include <ezsystables.h>
#include <bb_oscompat.h>
#include <comdb2.h>
#include <errno.h>
#include "ar_wrap.h"
#include "cdb2_constants.h"
#include "sqliteInt.h"
#include "sql.h"
#include "list.h"

typedef unsigned char u_int8_t;

extern char *comdb2_get_sav_dir_name(void);
extern char *comdb2_get_tmp_dir_name(void);
int endianness_mismatch(struct sqlclntstate *clnt);
void berk_fix_checkpoint_endianness(u_int8_t *buffer);

static const struct compareInfo globCaseInfo = {'%', '_', '[', 0};
extern int patternCompare(const u8 *, const u8 *, const struct compareInfo *,
                          u32);

extern struct dbenv *thedb;
extern char gbl_dbname[MAX_DBNAME_LENGTH];

/* Column numbers */
enum {
    FILES_COLUMN_FILENAME,
    FILES_COLUMN_DIR,
    FILES_COLUMN_TYPE,
    FILES_COLUMN_CONTENT,
    FILES_COLUMN_CONTENT_OFFSET,
    FILES_COLUMN_CONTENT_SIZE,
    FILES_COLUMN_CHUNK_SIZE,
};

enum {
    FILES_FILE_PATTERN_FLAG = 1,
    FILES_CHUNK_SIZE_FLAG = 2,
};

enum {
    FILES_TYPE_UNKNOWN,
    FILES_TYPE_CHECKPOINT,
    FILES_TYPE_BERKDB,
    FILES_TYPE_LOGFILE, /* Always keep LOGFILE in the end for them to sort
                           higher */
};

typedef struct db_chunk {
    uint8_t *buffer;
    size_t size;
    size_t offset;
    LINKC_T(struct db_chunk) lnk;
} db_chunk_t;

typedef struct db_file {
    char *name; /* Name of the file */
    char *dir;  /* Name of the directory */
    int type;
    int chunk_seq;
    struct db_chunk current_chunk;
    dbfile_info *info;
} db_file_t;

typedef struct {
    sqlite3_vtab_cursor base;
    sqlite3_int64 rowid;
    db_file_t *files;
    size_t nfiles;
    off_t content_offset;
    size_t content_size;
    size_t chunk_size;
    char *file_pattern;
    struct log_delete_state log_delete_state;
} systbl_files_cursor;

static const char *print_file_type(int type)
{
    switch (type) {
    case FILES_TYPE_UNKNOWN: return "unknown";
    case FILES_TYPE_BERKDB: return "berkdb";
    case FILES_TYPE_CHECKPOINT: return "checkpoint";
    case FILES_TYPE_LOGFILE: return "log";
    }
    return "unknown";
}

static const char *print_column_name(int col)
{
    switch (col) {
        case FILES_COLUMN_FILENAME: return "filename";
        case FILES_COLUMN_DIR: return "dir";
        case FILES_COLUMN_TYPE: return "type";
        case FILES_COLUMN_CONTENT: return "content";
        case FILES_COLUMN_CONTENT_OFFSET: return "offset";
        case FILES_COLUMN_CONTENT_SIZE: return "size";
        case FILES_COLUMN_CHUNK_SIZE: return "chunk_size";
    }
    return "unknown";
}

static void release_files(void *data, int npoints)
{
    if (!data) { return; }

    db_file_t *files = data;
    for (int i = 0; i < npoints; ++i) {
        if (files[i].name) {
            free(files[i].name);
        }
        if (files[i].dir) {
            free(files[i].dir);
        }
        if (files[i].current_chunk.buffer) {
            free(files[i].current_chunk.buffer);
        }
        if (files[i].info) {
            dbfile_deinit(files[i].info);
        }
    }
    free(files);
}

static int read_file(const char *path, uint8_t **buffer, size_t sz)
{
    int fd;
    int rc;

    fd = open(path, O_RDONLY);
    if (fd == -1) {
        logmsg(LOGMSG_ERROR, "%s:%d %s\n", __func__, __LINE__, strerror(errno));
        return -1;
    }

    *buffer = malloc(sz);
    if (*buffer == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d out-of-memory\n", __func__, __LINE__);
        goto err;
    }

    rc = read(fd, *buffer, sz);
    if (rc == -1) {
        logmsg(LOGMSG_ERROR, "%s:%d %s\n", __func__, __LINE__, strerror(errno));
        goto err;
    }
    return 0;

err:
    free(*buffer);
    Close(fd);
    return -1;
}

static void set_chunk_size(db_file_t *f, size_t chunk_size)
{
    size_t page_size = dbfile_pagesize(f->info);
    if (page_size == 0) {
        page_size = DEFAULT_PAGE_SIZE;
    }

    // Default to 'page_size' for data files if chunk_size isn't specified
    if (chunk_size == 0 && f->type == FILES_TYPE_BERKDB) {
        chunk_size = page_size;
    } else if (chunk_size > 0) {
        if (chunk_size < page_size) {
            chunk_size = page_size;
        } else {
            chunk_size /= page_size;
            chunk_size *= page_size;
        }
    } else if (chunk_size == 0) {
        chunk_size = MAX_BUFFER_SIZE;
    }

    dbfile_set_chunk_size(f->info, chunk_size);
}

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

/*
 * Returns 1 if dirent should be skipped on the basis of its name; otherwise, returns 0.
 *
 * d_name: The name of the dirent to be checked
 */
static int should_skip_dirent(const char *d_name) {
    const char *excluded_dirents[] =
        {".", "..", "watchdog", comdb2_get_tmp_dir_name(), comdb2_get_sav_dir_name(), "" /* sentinel */};
    const char *excluded;
    int rc;

    rc = 0;

    for (int i=0; (excluded = excluded_dirents[i]), excluded[0] != '\0'; ++i) {
        rc = (strcmp(d_name, excluded) == 0);

        if (rc) {
            goto err;
        }
    }

err:
    return rc;
}

static int read_dir(const char *dirname, db_file_t **files, int *count, char *file_pattern, size_t chunk_size)
{
    struct dirent buf;
    struct dirent *de;
    struct stat st;
    int rc = 0;
    int t_rc = 0;

    DIR *d = opendir(dirname);
    if (!d) {
        logmsg(LOGMSG_ERROR, "failed to read data directory\n");
        return -1;
    }

    while (bb_readdir(d, &buf, &de) == 0 && de) {
        if (should_skip_dirent(de->d_name)) {
            continue;
        }

        char path[4096];
        snprintf(path, sizeof(path), "%s/%s", dirname, de->d_name);
        rc = stat(path, &st);
        if (rc == -1) {
            logmsg(LOGMSG_ERROR, "%s:%d couldn't stat %s (%s)\n", __func__,
                   __LINE__, path, strerror(errno));
            break;
        }

        if (!(st.st_mode & S_IRGRP)) {
            logmsg(LOGMSG_WARN, "%s:%d: ignoring %s because it is read-restricted\n",
                    __func__, __LINE__, de->d_name);
            continue;
        }

        t_rc = access(path, R_OK);
        if (t_rc == -1) {
            if (errno == EACCES) {
                logmsg(LOGMSG_WARN, "%s:%d: ignoring %s because access check failed with errno %d\n",
                        __func__, __LINE__, de->d_name, errno);
                continue;
            } else {
                logmsg(LOGMSG_ERROR, "%s:%d: checking access permissions for %s failed with errno %d\n",
                        __func__, __LINE__, de->d_name, errno);
                rc = t_rc;
                break;
            }
        }

        if (S_ISDIR(st.st_mode)) {
            rc = read_dir(path, files, count, file_pattern, chunk_size);
            if (rc != 0) {
                break;
            }
            continue;
        }

        if (file_pattern &&
            (patternCompare((u8 *)file_pattern, (u8 *)de->d_name, &globCaseInfo,
                            '[') != 0)) {
            logmsg(LOGMSG_DEBUG, "%s:%d: ignoring %s\n", __func__, __LINE__,
                   de->d_name);
            continue;
        }

        logmsg(LOGMSG_DEBUG, "%s:%d: using %s\n", __func__, __LINE__,
               de->d_name);

        db_file_t *files_tmp =
            realloc(*files, sizeof(db_file_t) * (++(*count)));
        if (!files_tmp) {
            logmsg(LOGMSG_ERROR, "%s:%d: out-of-memory\n", __FILE__, __LINE__);
            rc = -1;
            break;
        }
        *files = files_tmp;
        db_file_t *f = (*files) + (*count) - 1;

        f->name = strdup(de->d_name);
        f->dir = NULL;
        f->current_chunk.buffer = NULL;
        f->current_chunk.size = 0; // To be determined at read time
        f->current_chunk.offset = 0;
        f->info = NULL;

        uint8_t is_data_file = 0;
        uint8_t is_queue_file = 0;
        uint8_t is_queuedb_file = 0;
        char *table_name = alloca(MAXTABLELEN);

        if ((recognize_data_file(f->name, &is_data_file, &is_queue_file,
                                 &is_queuedb_file, &table_name)) == 1) {
            f->info = dbfile_init(NULL, path);
            if (!f->info) {
                logmsg(LOGMSG_ERROR, "%s:%d: couldn't retrieve file info\n",
                       __FILE__, __LINE__);
                rc = -1;
                break;
            }
        } else {
            f->info = os_calloc(1, sizeof(dbfile_info));
            f->info->filename = os_strdup(path);
        }

        if (is_data_file == 1) {
            f->type = FILES_TYPE_BERKDB;
        } else if (strncmp(f->name, "log.", 4) == 0) {
            f->type = FILES_TYPE_LOGFILE;
        } else if (strncmp(f->name, "checkpoint", 10) == 0) {
            f->type = FILES_TYPE_CHECKPOINT;
        } else {
            f->type = FILES_TYPE_UNKNOWN;
        }

        set_chunk_size(f, chunk_size);

        // Remove the data directory prefix
        if (strcmp(dirname, thedb->basedir) == 0) {
            f->dir = strdup("");
        } else {
            f->dir = strdup(dirname + strlen(thedb->basedir) + 1);
        }
    }

    closedir(d);

    return rc;
}

static int get_files(void **data, size_t *npoints, char *file_pattern, size_t chunk_size)
{
    db_file_t *files = NULL;
    int count = 0;
    int rc = 0;

    rc = read_dir(thedb->basedir, &files, &count, file_pattern, chunk_size);
    if (rc != 0) {
        *npoints = -1;
    } else {
        *data = files;
        *npoints = count;
    }

    return rc;
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
    systbl_files_cursor *pCur;

    pCur = sqlite3_malloc(sizeof(*pCur));
    if (pCur == 0) return SQLITE_NOMEM;

    memset(pCur, 0, sizeof(*pCur));

    *ppCursor = &pCur->base;

    pCur->log_delete_state.filenum = 0;
    log_delete_add_state(thedb, &(pCur->log_delete_state));
    log_delete_counter_change(thedb, LOG_DEL_REFRESH);
    logmsg(LOGMSG_INFO, "disabling log file deletion\n");

    logdelete_lock(__func__, __LINE__);
    backend_update_sync(thedb);
    logdelete_unlock(__func__, __LINE__);

    return SQLITE_OK;
}

static int filesClose(sqlite3_vtab_cursor *cur)
{
    systbl_files_cursor *pCur = (systbl_files_cursor *)cur;

    logmsg(LOGMSG_INFO, "re-enabling log file deletion\n");
    log_delete_rem_state(thedb, &(pCur->log_delete_state));
    log_delete_counter_change(thedb, LOG_DEL_REFRESH);
    backend_update_sync(thedb);

    release_files(pCur->files, pCur->nfiles);

    sqlite3_free(pCur);
    return SQLITE_OK;
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
    systbl_files_cursor *pCur = (systbl_files_cursor *)cur;

    switch (i) {
    case FILES_COLUMN_FILENAME:
        sqlite3_result_text(ctx, pCur->files[pCur->rowid].name, -1, NULL);
        break;
    case FILES_COLUMN_DIR:
        sqlite3_result_text(ctx, pCur->files[pCur->rowid].dir, -1, NULL);
        break;
    case FILES_COLUMN_TYPE:
        sqlite3_result_text(
            ctx, print_file_type(pCur->files[pCur->rowid].type), -1, NULL);
        break;
    case FILES_COLUMN_CONTENT:
        sqlite3_result_blob(ctx, pCur->files[pCur->rowid].current_chunk.buffer,
                            pCur->files[pCur->rowid].current_chunk.size, NULL);
        break;
    case FILES_COLUMN_CONTENT_OFFSET:
        sqlite3_result_int64(ctx, pCur->files[pCur->rowid].current_chunk.offset);
        break;
    case FILES_COLUMN_CONTENT_SIZE:
        sqlite3_result_int64(ctx, pCur->files[pCur->rowid].current_chunk.size);
        break;
    case FILES_COLUMN_CHUNK_SIZE:
        sqlite3_result_int64(ctx, pCur->files[pCur->rowid].info->chunk_size);
        break;
    }
    return SQLITE_OK;
}

static int filesRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    systbl_files_cursor *pCur = (systbl_files_cursor *)cur;
    *pRowid = pCur->rowid;
    return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int filesEof(sqlite3_vtab_cursor *cur)
{
    systbl_files_cursor *pCur = (systbl_files_cursor *)cur;
    return (pCur->rowid >= pCur->nfiles) ? 1 : 0;
}

static int file_cmp(const void *file1, const void *file2)
{
    db_file_t *f1 = (db_file_t *)file1;
    db_file_t *f2 = (db_file_t *)file2;
    if (f1->type < f2->type)
        return -1;
    else if (f1->type > f2->type)
        return 1;
    return strcmp(f1->name, f2->name);
}

static int filesFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                       const char *idxStr, int argc, sqlite3_value **argv)
{
    systbl_files_cursor *pCur = (systbl_files_cursor *)pVtabCursor;
    int i = 0;

    if (idxNum & FILES_FILE_PATTERN_FLAG) {
        pCur->file_pattern = (char *)sqlite3_value_text(argv[i++]);
    } else {
        pCur->file_pattern = 0;
    }

    if (idxNum & FILES_CHUNK_SIZE_FLAG) {
        pCur->chunk_size = sqlite3_value_int64(argv[i++]);
    } else {
        pCur->chunk_size = 0;
    }

    if (get_files((void **)&pCur->files, &pCur->nfiles, pCur->file_pattern, pCur->chunk_size)) {
        logmsg(LOGMSG_ERROR, "%s:%d: Failed to get files following filter\n",
               __FILE__, __LINE__);
        return SQLITE_ERROR;
    }

    pCur->rowid = 0;

    qsort(pCur->files, pCur->nfiles, sizeof(db_file_t), file_cmp);

    return (read_next_chunk(pCur)) ? SQLITE_ERROR : SQLITE_OK;
}

static int filesBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo)
{
    int i;                  /* Loop over constraints */
    int idxNum = 0;         /* The query plan bitmask */
    int nArg = 0;           /* Number of arguments that filesFilter() expects */
    int filenameIdx = -1;
    int chunkSizeIdx = -1;

    const struct sqlite3_index_constraint *pConstraint;
    pConstraint = pIdxInfo->aConstraint;
    for (i = 0; i < pIdxInfo->nConstraint; i++, pConstraint++) {
        if (pConstraint->usable == 0) continue;
        switch (pConstraint->iColumn) {
        case FILES_COLUMN_FILENAME:
            if (pConstraint->op != SQLITE_INDEX_CONSTRAINT_LIKE) {
                logmsg(LOGMSG_ERROR, "%s:%d: Column '%s' can only be constrained with 'like'\n",
                       __FILE__, __LINE__, print_column_name(FILES_COLUMN_FILENAME));
                return SQLITE_ERROR;
            }
            idxNum |= FILES_FILE_PATTERN_FLAG;
            filenameIdx = i;
            break;
        case FILES_COLUMN_CHUNK_SIZE:
            if (pConstraint->op != SQLITE_INDEX_CONSTRAINT_EQ) {
                logmsg(LOGMSG_ERROR, "%s:%d: Column '%s' can only be constrained with '='\n",
                       __FILE__, __LINE__, print_column_name(FILES_COLUMN_CHUNK_SIZE));
                return SQLITE_ERROR;
            }
            idxNum |= FILES_CHUNK_SIZE_FLAG;
            chunkSizeIdx = i;
            break;
        }
    }

    if (filenameIdx >= 0) {
        pIdxInfo->aConstraintUsage[filenameIdx].argvIndex = ++nArg;
        pIdxInfo->aConstraintUsage[filenameIdx].omit = 1;
    }

    if (chunkSizeIdx >= 0) {
        pIdxInfo->aConstraintUsage[chunkSizeIdx].argvIndex = ++nArg;
        pIdxInfo->aConstraintUsage[chunkSizeIdx].omit = 1;
    }

    pIdxInfo->idxNum = idxNum;
    return SQLITE_OK;
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
