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

#include "files_util.h"

extern struct dbenv *thedb;
extern char *comdb2_get_sav_dir_name(void);
extern char *comdb2_get_tmp_dir_name(void);
extern char gbl_dbname[MAX_DBNAME_LENGTH];

static const struct compareInfo globCaseInfo = {'%', '_', '[', 0};
extern int patternCompare(const u8 *, const u8 *, const struct compareInfo *,
                          u32);

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


int files_util_open(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor)
{
    systbl_files_cursor *pCur;
    pCur = sqlite3_malloc(sizeof(*pCur));
    if (pCur == 0) return SQLITE_NOMEM;
    memset(pCur, 0, sizeof(*pCur));

    *ppCursor = &pCur->base;
    return SQLITE_OK;
}

int files_util_close(sqlite3_vtab_cursor *cur)
{
    systbl_files_cursor *pCur = (systbl_files_cursor *)cur;
    release_files(pCur->files, pCur->nfiles);
    sqlite3_free(pCur);
    return SQLITE_OK;
}

int
files_util_column(sqlite3_vtab_cursor *cur, /* The cursor */
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

int files_util_rowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    systbl_files_cursor *pCur = (systbl_files_cursor *)cur;
    *pRowid = pCur->rowid;
    return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
int files_util_eof(sqlite3_vtab_cursor *cur)
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


int files_util_filter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
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
    
    return SQLITE_OK;
}


int files_util_best_index(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo)
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
