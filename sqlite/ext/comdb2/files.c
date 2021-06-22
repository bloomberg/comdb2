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

extern struct dbenv *thedb;

static struct log_delete_state log_delete_state;

/* Column numbers */
#define FILES_COLUMN_FILENAME           0
#define FILES_COLUMN_DIR                1
#define FILES_COLUMN_CONTENT            2
#define FILES_COLUMN_CONTENT_COMPR_ALGO 3

#define FILES_COMPR_NONE 0
#define FILES_COMPR_ZLIB 1
#define FILES_COMPR_LZ4  2

typedef struct file_entry {
    char   *file; /* Name of the file */
    char   *dir;  /* Name of the directory */
    void   *content;
    size_t content_sz;
} file_entry_t;

typedef struct {
    sqlite3_vtab_cursor base;
    sqlite3_int64       rowid;
    file_entry_t        *entries;
    size_t              nentries;
    char                *content_compr_algo;
} systbl_files_cursor;

static void release_files(void *data, int npoints)
{
    logmsg(LOGMSG_INFO, "re-enabling log file deletion\n");
    log_delete_rem_state(thedb, &log_delete_state);
    log_delete_counter_change(thedb, LOG_DEL_REFRESH);
    backend_update_sync(thedb);

    file_entry_t *files = data;
    for (int i = 0; i < npoints; ++i) {
        free(files[i].file);
        free(files[i].dir);
        free(files[i].content);
    }
    free(files);
}

static int read_file(const char *path, void **buffer, size_t sz)
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

static int read_dir(const char *dirname, file_entry_t **files, int *count)
{
    struct dirent buf;
    struct dirent *de;
    int rc = 0;

    DIR *d = opendir(dirname);
    if (!d) {
        logmsg(LOGMSG_ERROR, "failed to read data directory\n");
        return -1;
    }

    while (bb_readdir(d, &buf, &de) == 0 && de) {
        struct stat st;

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
            rc = read_dir(path, files, count);
            if (rc != 0) {
                break;
            }
        } else {
            file_entry_t *files_tmp =
                realloc(*files, sizeof(file_entry_t) * (++(*count)));
            if (!files_tmp) {
                logmsg(LOGMSG_ERROR, "%s:%d: out-of-memory\n", __FILE__,
                       __LINE__);
                rc = -1;
                break;
            }
            *files = files_tmp;
            file_entry_t *f = (*files) + (*count) - 1;
            f->file = strdup(de->d_name);
            /* Remove the data directory prefix */
            if (strcmp(dirname, thedb->basedir) == 0) {
                f->dir = strdup("");
            } else {
                f->dir = strdup(dirname + strlen(thedb->basedir) + 1);
            }

            rc = read_file(path, &f->content, st.st_size);
            if (rc == -1) {
                break;
            }
            f->content_sz = st.st_size;
        }
    }

    closedir(d);

    return rc;
}

static int get_files(void **data, size_t *npoints)
{
    file_entry_t *files = NULL;
    int count = 0;

    log_delete_state.filenum = 0;
    log_delete_add_state(thedb, &log_delete_state);
    log_delete_counter_change(thedb, LOG_DEL_REFRESH);
    logmsg(LOGMSG_INFO, "disabling log file deletion\n");

    logdelete_lock(__func__, __LINE__);
    backend_update_sync(thedb);
    logdelete_unlock(__func__, __LINE__);

    int rc = read_dir(thedb->basedir, &files, &count);
    if (rc != 0) {
        *npoints = -1;
        return rc;
    }

    *data = files;
    *npoints = count;
    return 0;
}

static int filesConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  sqlite3_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db, "CREATE TABLE x(filename, dir, content, content_compr_algo hidden)");
  if( rc==SQLITE_OK ){
    pNew = *ppVtab = sqlite3_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }
  return rc;
}

static int filesDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int filesOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  systbl_files_cursor *pCur;

  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 )
    return SQLITE_NOMEM;

  memset(pCur, 0, sizeof(*pCur));

  if (get_files((void **)&pCur->entries, &pCur->nentries))
    return SQLITE_ERROR;

  *ppCursor = &pCur->base;

  return SQLITE_OK;
}

static int filesClose(sqlite3_vtab_cursor *cur){
  systbl_files_cursor *pCur = (systbl_files_cursor*)cur;
  release_files(pCur->entries, pCur->nentries);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int filesNext(sqlite3_vtab_cursor *cur){
  systbl_files_cursor *pCur = (systbl_files_cursor*)cur;
  pCur->rowid++;
  return SQLITE_OK;
}

static int filesColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
)
{
  systbl_files_cursor *pCur = (systbl_files_cursor*)cur;

  switch (i) {
    case FILES_COLUMN_FILENAME:
      sqlite3_result_text(ctx, pCur->entries[pCur->rowid].file, -1, NULL);
      break;
    case FILES_COLUMN_DIR:
      sqlite3_result_text(ctx, pCur->entries[pCur->rowid].dir, -1, NULL);
      break;
    case FILES_COLUMN_CONTENT:
      sqlite3_result_blob(ctx, pCur->entries[pCur->rowid].content,
                          pCur->entries[pCur->rowid].content_sz, NULL);
      break;
    case FILES_COLUMN_CONTENT_COMPR_ALGO:
      sqlite3_result_text(ctx, pCur->content_compr_algo, -1, NULL);
      break;
  }
  return SQLITE_OK;
}

static int filesRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_files_cursor *pCur = (systbl_files_cursor*)cur;
  *pRowid = pCur->rowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int filesEof(sqlite3_vtab_cursor *cur){
  systbl_files_cursor *pCur = (systbl_files_cursor*)cur;
  return (pCur->rowid >= pCur->nentries) ? 1 : 0;
}

static int filesFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  systbl_files_cursor *pCur = (systbl_files_cursor *)pVtabCursor;
  int i = 0;

  if( idxNum & 1 ){
    pCur->content_compr_algo = (char *)sqlite3_value_text(argv[i++]);
  }

  pCur->rowid = 0;
  return SQLITE_OK;
}

static int filesBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;                 /* Loop over constraints */
  int idxNum = 0;        /* The query plan bitmask */
  int nArg = 0;          /* Number of arguments that seriesFilter() expects */
  int comprAlgoIdx = -1; /* Index of content_compr_algo=?, or -1 if not
                            specified */

  const struct sqlite3_index_constraint *pConstraint;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( pConstraint->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pConstraint->iColumn ){
        case FILES_COLUMN_CONTENT_COMPR_ALGO:
        idxNum |= 1;
        comprAlgoIdx = i;
        break;
    }
  }
  if( comprAlgoIdx>=0 ){
    pIdxInfo->aConstraintUsage[comprAlgoIdx].argvIndex = ++nArg;
    pIdxInfo->aConstraintUsage[comprAlgoIdx].omit = 1;
  }else{
    /* If either boundary is missing, we have to generate a huge span
    ** of numbers.  Make this case very expensive so that the query
    ** planner will work hard to avoid it. */
    pIdxInfo->estimatedCost = (double)2000000000;
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
  .access_flag = CDB2_ALLOW_USER
};

