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

#ifndef __COMDB2_FILES_H_
#define __COMDB2_FILES_H_

#include <fcntl.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <comdb2systblInt.h>
#include <ezsystables.h>
#include <bb_oscompat.h>
#include <errno.h>
#include "cdb2_constants.h"
#include "sqliteInt.h"
#include "sql.h"
#include <comdb2.h>
#include "list.h"
#include "ar_wrap.h"

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
} systbl_files_cursor;

int files_util_open(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor);
int files_util_close(sqlite3_vtab_cursor *cur);
int files_util_column(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i);
int files_util_rowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid);
int files_util_eof(sqlite3_vtab_cursor *cur);
int files_util_best_index(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo);
int files_util_filter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                       const char *idxStr, int argc, sqlite3_value **argv);

#endif
