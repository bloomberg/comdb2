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

#ifdef WITH_ARCHIVE
#include <archive.h>
#include <archive_entry.h>
#endif /* WITH_ARCHIVE */

char *comdb2_get_tmp_dir(void);

static const struct compareInfo globCaseInfo = { '%', '_', '[', 0 };
extern int patternCompare(const u8*,const u8*,const struct compareInfo*,u32);

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
    FILES_COLUMN_ARCHIVE_FMT,
    FILES_COLUMN_COMPR_ALGO,
};

enum {
    FILES_ARCHIVE_NONE,
    FILES_ARCHIVE_TAR,
};

enum {
    FILES_COMPR_NONE,
    FILES_COMPR_ZLIB,
    FILES_COMPR_LZ4,
};

enum {
    FILES_ARCHIVE_FORMAT_FLAG = 1,
    FILES_COMPR_ALGO_FLAG = 2,
    FILES_FILE_PATTERN_FLAG = 4,
    FILES_CHUNK_SIZE_FLAG = 8,
};

enum {
    FILES_TYPE_UNKNOWN,
    FILES_TYPE_BERKDB,
    FILES_TYPE_LOGFILE, /* Always keep LOGFILE in the end for them to sort higher */
};

typedef struct file_entry {
    char    *file; /* Name of the file */
    char    *dir;  /* Name of the directory */
    int     type;
    uint8_t *content;
    uint8_t *write_ptr;
    size_t  size;
    size_t  offset;
    dbfile_info *info;
} file_entry_t;

typedef struct {
    sqlite3_vtab_cursor     base;
    sqlite3_int64           rowid;
    file_entry_t            *entries;
    size_t                  nentries;
    off_t                   content_offset;
    size_t                  content_size;
    size_t                  chunk_size;
    int                     archive_fmt;
    int                     compr_algo;
    char                    *file_pattern;
    struct log_delete_state log_delete_state;
} systbl_files_cursor;

static int get_archive_format(const char *fmt)
{
    if (strcasecmp(fmt, "none") == 0) {
        return FILES_ARCHIVE_NONE;
#ifdef WITH_ARCHIVE
    } else if (strcasecmp(fmt, "tar") == 0) {
        return FILES_ARCHIVE_TAR;
#endif /* WITH_ARCHIVE */
    }
    return -1;
}

static const char *print_archive_format(int fmt)
{
    switch (fmt) {
    case FILES_ARCHIVE_NONE: return "none";
    case FILES_ARCHIVE_TAR: return "tar";
    }
    return "unknown";
}

static int get_compr_algo(const char *algo)
{
    if (strcasecmp(algo, "none") == 0) {
        return FILES_COMPR_NONE;
    } else if (strcasecmp(algo, "zlib") == 0) {
        return FILES_COMPR_ZLIB;
    } else if (strcasecmp(algo, "lz4") == 0) {
        return FILES_COMPR_LZ4;
    }
    return -1;
}

static const char *print_compr_algo(int algo)
{
    switch (algo) {
    case FILES_COMPR_NONE: return "none";
    case FILES_COMPR_ZLIB: return "zlib";
    case FILES_COMPR_LZ4: return "lz4";
    }
    return "unknown";
}

static const char *print_file_type(int type)
{
    switch (type) {
    case FILES_TYPE_UNKNOWN: return "unknown";
    case FILES_TYPE_BERKDB: return "berkdb";
    case FILES_TYPE_LOGFILE: return "log";
    }
    return "unknown";
}

static void release_files(void *data, int npoints)
{
    file_entry_t *files = data;
    for (int i = 0; i < npoints; ++i) {
        free(files[i].file);
        free(files[i].dir);
        free(files[i].content);
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
    close(fd);
    return -1;
}

static void set_chunk_size(file_entry_t *f, size_t chunk_size) {
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
    }

    dbfile_set_chunk_size(f->info, chunk_size);
}

int memory_writer(void *ctx, uint8_t *in_buf, size_t size, size_t offset)
{
    file_entry_t *file = ctx;

    if (file->content == file->write_ptr) {
        file->offset = offset;
    }
    memcpy(file->write_ptr, in_buf, size);
    file->write_ptr += size;

    file->size = file->write_ptr - file->content;
    return 0;
}

#ifdef WITH_ARCHIVE
static int archive_writer(void *ctx, uint8_t *in_buf, size_t size,
                          size_t offset)
{
    struct archive *a = ctx;
    ssize_t rc = archive_write_data(a, in_buf, size);
    if (rc < 0) {
        logmsg(LOGMSG_ERROR, "%s:%d archive_write_data() failed (%s)\n",
               __func__, __LINE__, strerror(errno));
        return 1;
    }
    return 0;
}
#endif

static int create_archive(const char *archive_path, file_entry_t *files,
                          int count, int archive_fmt, int compr_algo)
{
#ifdef WITH_ARCHIVE
    struct archive *a;
    struct archive_entry *entry;
    struct stat st;
    char path[4096];
    char full_path[4096];
    int rc;

    a = archive_write_new();
    archive_write_add_filter_gzip(a);
    archive_write_set_format_pax_restricted(a);
    archive_write_open_filename(a, archive_path);

    for (int i = 0; i < count; ++i) {
        snprintf(path, sizeof(path), "%s/%s/%s", gbl_dbname, files[i].dir,
                 files[i].file);
        snprintf(full_path, sizeof(full_path), "%s/%s/%s", thedb->basedir,
                 files[i].dir, files[i].file);

        rc = stat(full_path, &st);
        if (rc == -1) {
            logmsg(LOGMSG_ERROR, "%s:%d couldn't stat %s (%s)\n", __func__,
                   __LINE__, path, strerror(errno));
            return rc;
        }

        entry = archive_entry_new();
        archive_entry_set_pathname(entry, path);
        archive_entry_set_size(entry, st.st_size);
        archive_entry_set_filetype(entry, AE_IFREG);
        archive_entry_set_perm(entry, 0600);
        archive_write_header(a, entry);

        dbfile_info info = {0};
        uint8_t is_data_file = 0;
        uint8_t is_queue_file = 0;
        uint8_t is_queuedb_file = 0;
        char *table_name = alloca(MAXTABLELEN);

        if ((recognize_data_file(files[i].file, &is_data_file, &is_queue_file,
                                 &is_queuedb_file, &table_name)) == 1) {
            if (!(dbfile_init(&info, full_path))) {
                logmsg(LOGMSG_ERROR, "%s:%d: couldn't retrieve file info\n",
                       __FILE__, __LINE__);
                rc = -1;
                break;
            }
        }
        info.filename = full_path;

        rc = read_write_file(&info, a, archive_writer);
        if (rc != 0) {
            return -1;
        }

        archive_entry_free(entry);
    }
    archive_write_close(a);
    archive_write_free(a);
    return 0;

#else

    logmsg(LOGMSG_ERROR,
           "%s:%d Archive not supported. Re-compile with archive library\n",
           __func__, __LINE__);
    return 1;

#endif /* WITH_ARCHIVE */
}

static int read_dir(const char *dirname, file_entry_t **files, int *count,
                    int compr_algo, char *file_pattern, size_t chunk_size)
{
    struct dirent buf;
    struct dirent *de;
    struct stat st;
    int rc = 0;

    DIR *d = opendir(dirname);
    if (!d) {
        logmsg(LOGMSG_ERROR, "failed to read data directory\n");
        return -1;
    }

    while (bb_readdir(d, &buf, &de) == 0 && de) {
        if ((strcmp(de->d_name, ".") == 0) || (strcmp(de->d_name, "..") == 0)) {
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

        if (S_ISDIR(st.st_mode)) {
            rc = read_dir(path, files, count, compr_algo, file_pattern, chunk_size);
            if (rc != 0) {
                break;
            }
            continue;
        }

        if (file_pattern &&
            (patternCompare((u8*)file_pattern, (u8*)de->d_name, &globCaseInfo, '[') != 0)) {
            logmsg(LOGMSG_DEBUG, "%s:%d: ignoring %s\n", __func__, __LINE__,
                   de->d_name);
            continue;
        }

        logmsg(LOGMSG_DEBUG, "%s:%d: using %s\n", __func__, __LINE__,
               de->d_name);

        file_entry_t *files_tmp =
            realloc(*files, sizeof(file_entry_t) * (++(*count)));
        if (!files_tmp) {
            logmsg(LOGMSG_ERROR, "%s:%d: out-of-memory\n", __FILE__, __LINE__);
            rc = -1;
            break;
        }
        *files = files_tmp;
        file_entry_t *f = (*files) + (*count) - 1;

        f->file = strdup(de->d_name);
        f->dir = NULL;
        f->content = NULL;
        f->write_ptr = NULL;
        f->size = st.st_size;
        f->offset = 0;
        f->info = NULL;

        uint8_t is_data_file = 0;
        uint8_t is_queue_file = 0;
        uint8_t is_queuedb_file = 0;
        char *table_name = alloca(MAXTABLELEN);

        if ((recognize_data_file(f->file, &is_data_file, &is_queue_file,
                                 &is_queuedb_file, &table_name)) == 1) {
            f->info = dbfile_init(NULL, path);
            if (!f->info) {
                logmsg(LOGMSG_ERROR, "%s:%d: couldn't retrieve file info\n",
                       __FILE__, __LINE__);
                rc = -1;
                break;
            }
        } else {
            f->info = calloc(1, sizeof(dbfile_info));
            f->info->filename = strdup(path);
        }

        if (is_data_file == 1) {
            f->type = FILES_TYPE_BERKDB;
        } else if (strncmp(f->file, "log.", 4) == 0) {
            f->type = FILES_TYPE_LOGFILE;
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

static int get_files(void **data, size_t *npoints, int archive_fmt,
                     int compr_algo, char *file_pattern, size_t chunk_size)
{
    file_entry_t *files = NULL;
    int count = 0;
    int rc = 0;

    rc = read_dir(thedb->basedir, &files, &count, compr_algo, file_pattern,
                  chunk_size);
    if (rc != 0) {
        *npoints = -1;
        goto done;
    }

    *data = files;
    *npoints = count;

    if (archive_fmt != FILES_ARCHIVE_NONE) {
#ifdef WITH_ARCHIVE
        struct stat st;
        char archive_file_name[1024];
        char archive_path[4096];

        snprintf(archive_file_name, sizeof(archive_file_name), "%s.tar.gz",
                 gbl_dbname);
        snprintf(archive_path, sizeof(archive_path), "%s/%s",
                 comdb2_get_tmp_dir(), archive_file_name);

        rc =
            create_archive(archive_path, files, count, archive_fmt, compr_algo);

        free(files);

        file_entry_t *files_tmp = malloc(sizeof(file_entry_t));
        rc = stat(archive_path, &st);
        if (rc == -1) {
            logmsg(LOGMSG_ERROR, "%s:%d couldn't stat %s (%s)\n", __func__,
                   __LINE__, archive_path, strerror(errno));
            goto done;
        }

        rc = read_file(archive_path, &files_tmp->content, st.st_size);
        files_tmp->size = st.st_size;
        files_tmp->file = strdup(archive_file_name);
        files_tmp->dir = strdup("");

        unlink(archive_path);

        *data = files_tmp;
        *npoints = 1;
#else

        logmsg(LOGMSG_ERROR,
               "%s:%d Archive not supported. Re-compile with archive library\n",
               __func__, __LINE__);
        rc = 1;

#endif
    }

done:
    return rc;

}

static int filesConnect(sqlite3 *db, void *pAux, int argc,
                        const char *const *argv, sqlite3_vtab **ppVtab,
                        char **pzErr)
{
    sqlite3_vtab *pNew;
    int rc;

    rc = sqlite3_declare_vtab(
        db, "CREATE TABLE x(filename, dir, type, content, offset, size, chunk_size "
            "hidden, archive_format hidden, compression_algorithm hidden)");
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

    release_files(pCur->entries, pCur->nentries);
    sqlite3_free(pCur);
    return SQLITE_OK;
}

static int filesNext(sqlite3_vtab_cursor *cur)
{
    int rc;
    systbl_files_cursor *pCur = (systbl_files_cursor *)cur;
    while (pCur->rowid < pCur->nentries) {
        file_entry_t *f = &pCur->entries[pCur->rowid];
        if (f->info->chunk_size > 0) {
            f->size = f->info->chunk_size;
        }

        f->content = malloc(f->size);
        f->write_ptr = f->content;

        rc = read_write_file(f->info, f, memory_writer);
        if (rc == 1) {
            return SQLITE_ERROR;
        } else if (rc == 0) {
            break;
        }
        pCur->rowid++;
    }
    return SQLITE_OK;
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
        sqlite3_result_text(ctx, pCur->entries[pCur->rowid].file, -1, NULL);
        break;
    case FILES_COLUMN_DIR:
        sqlite3_result_text(ctx, pCur->entries[pCur->rowid].dir, -1, NULL);
        break;
    case FILES_COLUMN_TYPE:
        sqlite3_result_text(ctx, print_file_type(pCur->entries[pCur->rowid].type), -1, NULL);
        break;
    case FILES_COLUMN_CONTENT:
        sqlite3_result_blob(ctx, pCur->entries[pCur->rowid].content,
                            pCur->entries[pCur->rowid].size, NULL);
        break;
    case FILES_COLUMN_CONTENT_OFFSET:
        sqlite3_result_int64(ctx, pCur->entries[pCur->rowid].offset);
        break;
    case FILES_COLUMN_CONTENT_SIZE:
        sqlite3_result_int64(ctx, pCur->entries[pCur->rowid].size);
        break;
    case FILES_COLUMN_CHUNK_SIZE:
        sqlite3_result_int64(ctx, pCur->entries[pCur->rowid].info->chunk_size);
        break;
    case FILES_COLUMN_ARCHIVE_FMT:
        sqlite3_result_text(ctx, print_archive_format(pCur->archive_fmt), -1,
                            NULL);
        break;
    case FILES_COLUMN_COMPR_ALGO:
        sqlite3_result_text(ctx, print_compr_algo(pCur->compr_algo), -1, NULL);
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
    return (pCur->rowid >= pCur->nentries) ? 1 : 0;
}

static int file_cmp(const void *file1, const void *file2) {
    file_entry_t *f1 = (file_entry_t *)file1;
    file_entry_t *f2 = (file_entry_t *)file2;
    if (f1->type < f2->type)
        return -1;
    else if (f1->type > f2->type)
        return 1;
    return strcmp(f1->file, f2->file);
}

static int filesFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                       const char *idxStr, int argc, sqlite3_value **argv)
{
    systbl_files_cursor *pCur = (systbl_files_cursor *)pVtabCursor;
    int i = 0;
    int rc;

    if (idxNum & FILES_ARCHIVE_FORMAT_FLAG) {
        pCur->archive_fmt =
            get_archive_format((const char *)sqlite3_value_text(argv[i++]));
        if (pCur->archive_fmt == -1) {
            return SQLITE_ERROR;
        }
    }

    if (idxNum & FILES_COMPR_ALGO_FLAG) {
        pCur->compr_algo =
            get_compr_algo((const char *)sqlite3_value_text(argv[i++]));
        if (pCur->compr_algo == -1) {
            return SQLITE_ERROR;
        }
    }

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

    if (get_files((void **)&pCur->entries, &pCur->nentries, pCur->archive_fmt,
                  pCur->compr_algo, pCur->file_pattern, pCur->chunk_size))
        return SQLITE_ERROR;

    pCur->rowid = 0;

    qsort(pCur->entries, pCur->nentries, sizeof(file_entry_t), file_cmp);

    while (pCur->rowid < pCur->nentries) {
        file_entry_t *f = &pCur->entries[pCur->rowid];
        if (f->info->chunk_size > 0) {
            f->size = f->info->chunk_size;
        }

        f->content = malloc(f->size);
        f->write_ptr = f->content;

        rc = read_write_file(f->info, f, memory_writer);
        if (rc == 1) {
            return SQLITE_ERROR;
        } else if (rc == 0) {
            f->size = f->write_ptr - f->content;
            // Read the first file/chunk; break here.
            break;
        }
        pCur->rowid++;
    }

    return SQLITE_OK;
}

static int filesBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo)
{
    int i;                  /* Loop over constraints */
    int idxNum = 0;         /* The query plan bitmask */
    int nArg = 0;           /* Number of arguments that filesFilter() expects */
    int archiveFmtIdx = -1; /* Index of archive_fmt=?, or -1 if not specified */
    int comprAlgoIdx = -1;  /* Index of compr_algo=?, or -1 if not specified */
    int filenameIdx = -1;
    int chunkSizeIdx = -1;

    const struct sqlite3_index_constraint *pConstraint;
    pConstraint = pIdxInfo->aConstraint;
    for (i = 0; i < pIdxInfo->nConstraint; i++, pConstraint++) {
        if (pConstraint->usable == 0) continue;
        switch (pConstraint->iColumn) {
        case FILES_COLUMN_ARCHIVE_FMT:
            if (pConstraint->op != SQLITE_INDEX_CONSTRAINT_EQ) {
                return SQLITE_ERROR;
            }
            idxNum |= FILES_ARCHIVE_FORMAT_FLAG;
            archiveFmtIdx = i;
            break;
        case FILES_COLUMN_COMPR_ALGO:
            if (pConstraint->op != SQLITE_INDEX_CONSTRAINT_EQ) {
                return SQLITE_ERROR;
            }
            idxNum |= FILES_COMPR_ALGO_FLAG;
            comprAlgoIdx = i;
            break;
        case FILES_COLUMN_FILENAME:
            if (pConstraint->op != SQLITE_INDEX_CONSTRAINT_LIKE) {
                return SQLITE_ERROR;
            }
            idxNum |= FILES_FILE_PATTERN_FLAG;
            filenameIdx = i;
            break;
        case FILES_COLUMN_CHUNK_SIZE:
            if (pConstraint->op != SQLITE_INDEX_CONSTRAINT_EQ) {
                return SQLITE_ERROR;
            }
            idxNum |= FILES_CHUNK_SIZE_FLAG;
            chunkSizeIdx = i;
            break;
        }
    }
    if (archiveFmtIdx >= 0) {
        pIdxInfo->aConstraintUsage[archiveFmtIdx].argvIndex = ++nArg;
        pIdxInfo->aConstraintUsage[archiveFmtIdx].omit = 1;
    }

    if (comprAlgoIdx >= 0) {
        pIdxInfo->aConstraintUsage[comprAlgoIdx].argvIndex = ++nArg;
        pIdxInfo->aConstraintUsage[comprAlgoIdx].omit = 1;
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
