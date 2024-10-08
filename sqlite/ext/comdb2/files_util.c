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

#include <float.h>
#include <math.h>

#include "files_util.h"

extern struct dbenv *thedb;
extern char *comdb2_get_sav_dir_name(void);
extern char *comdb2_get_tmp_dir_name(void);
extern char gbl_dbname[MAX_DBNAME_LENGTH];

static const struct compareInfo globCaseInfo = {'%', '_', '[', 0};
extern int patternCompare(const u8 *, const u8 *, const struct compareInfo *,
                          u32);

struct log_delete_state gbl_log_delete_state;
int gbl_num_files_cursors = 0;
pthread_mutex_t gbl_num_files_cursors_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct constraint_value {
    void *val;
    constraint_type_t type;
} constraint_value_t;

typedef struct comdb2_files_constraints {
    constraint_value_t *file_name_constraints;
    int n_file_name_constraints;

    constraint_value_t *file_type_constraints;
    int n_file_type_constraints;

    int chunk_size_constraint;
} comdb2_files_constraints_t;

static file_type_t str_to_file_type(const char *type) {
    if (!strcmp(type, "berkdb")) { return FILES_TYPE_BERKDB; }
    else if (!strcmp(type, "checkpoint")) { return FILES_TYPE_CHECKPOINT; }
    else if (!strcmp(type, "log")) { return FILES_TYPE_LOGFILE; }
    else { return FILES_TYPE_UNKNOWN; }
}

static const char *print_file_type(file_type_t type)
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

static int file_name_matches_constraint(const constraint_value_t * const constraints,
                                        int n_constraints,
                                        const char * const file_name) {
    int is_match = 1;

    for (int i=0; i<n_constraints && is_match; ++i) {
        const constraint_value_t * const constraint = (constraints + i);
        const char * c_val = (const char *) constraint->val;
        const constraint_type_t c_type = constraint->type;

        if (c_type == FILES_FILE_NAME_EQ_FLAG) {
            is_match = strcmp((const char *) c_val, file_name) == 0;
        } else if (c_type == FILES_FILE_NAME_NEQ_FLAG) {
            is_match = strcmp((const char *) c_val, file_name) != 0;
        } else {
            assert(c_type == FILES_FILE_NAME_LIKE_FLAG);
            is_match = patternCompare((u8 *)c_val, (u8 *)file_name, &globCaseInfo, '[') == 0;
        }
    }

    return is_match;
}

static int file_type_matches_constraint(const constraint_value_t * const constraints,
                                        int n_constraints,
                                        const file_type_t file_type) {
    int is_match = 1;

    for (int i=0; i<n_constraints && is_match; ++i) {
        const constraint_value_t * const constraint = (constraints + i);
        const file_type_t c_val = (const file_type_t) constraint->val;
        const constraint_type_t c_type = constraint->type;

        if (c_type == FILES_FILE_TYPE_EQ_FLAG) {
            is_match = (file_type == c_val);
        } else {
            assert(c_type == FILES_FILE_TYPE_NEQ_FLAG);
            is_match = (file_type != c_val);
        }
    }

    return is_match;
}

static int read_dir(const char *dirname, db_file_t **files, int *count, const comdb2_files_constraints_t *constraints)
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
            rc = read_dir(path, files, count, constraints);
            if (rc != 0) {
                break;
            }
            continue;
        }

        if (!file_name_matches_constraint(constraints->file_name_constraints,
                                          constraints->n_file_name_constraints,
                                          de->d_name)) {
            logmsg(LOGMSG_DEBUG, "%s:%d: ignoring %s\n", __func__, __LINE__,
                de->d_name);
            continue;
        }

        uint8_t is_data_file = 0;
        uint8_t is_queue_file = 0;
        uint8_t is_queuedb_file = 0;
        char *table_name = alloca(MAXTABLELEN);

        const int recognized_data_file = recognize_data_file(de->d_name, &is_data_file, &is_queue_file,
                                 &is_queuedb_file, &table_name) == 1;

        file_type_t type;
        if (is_data_file == 1) {
            type = FILES_TYPE_BERKDB;
        } else if (strncmp(de->d_name, "log.", 4) == 0) {
            type = FILES_TYPE_LOGFILE;
        } else if (strncmp(de->d_name, "checkpoint", 10) == 0) {
            type = FILES_TYPE_CHECKPOINT;
        } else {
            type = FILES_TYPE_UNKNOWN;
        }

        if (!file_type_matches_constraint(constraints->file_type_constraints,
                                          constraints->n_file_type_constraints,
                                          type)) {
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

        if (recognized_data_file) {
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

        f->type = type;

        set_chunk_size(f, constraints->chunk_size_constraint);

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

static int get_files(void **data, size_t *npoints, const comdb2_files_constraints_t *constraints)
{
    db_file_t *files = NULL;
    int count = 0;
    int rc = 0;

    rc = read_dir(thedb->basedir, &files, &count, constraints);
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

    Pthread_mutex_lock(&gbl_num_files_cursors_lock);
    if (gbl_num_files_cursors < 0) {
        logmsg(LOGMSG_ERROR, "%s: Unexpected number of open cursors\n", __func__);
        abort();
    }
    if (++gbl_num_files_cursors == 1) {
        gbl_log_delete_state.filenum = 0;
        log_delete_add_state(thedb, &gbl_log_delete_state);
        log_delete_counter_change(thedb, LOG_DEL_REFRESH);
        logmsg(LOGMSG_INFO, "disabling log file deletion\n");

        logdelete_lock(__func__, __LINE__);
        backend_update_sync(thedb);
        logdelete_unlock(__func__, __LINE__);
    }
    Pthread_mutex_unlock(&gbl_num_files_cursors_lock);

    return SQLITE_OK;
}

int files_util_close(sqlite3_vtab_cursor *cur)
{
    systbl_files_cursor *pCur = (systbl_files_cursor *)cur;

    Pthread_mutex_lock(&gbl_num_files_cursors_lock);
    if (gbl_num_files_cursors <= 0) {
        logmsg(LOGMSG_ERROR, "%s: Unexpected number of open cursors\n", __func__);
        abort();
    }
    if (--gbl_num_files_cursors == 0) {
        logmsg(LOGMSG_INFO, "re-enabling log file deletion\n");
        log_delete_rem_state(thedb, &gbl_log_delete_state);
        log_delete_counter_change(thedb, LOG_DEL_REFRESH);
        backend_update_sync(thedb);
    }
    Pthread_mutex_unlock(&gbl_num_files_cursors_lock);

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

extern unsigned int hash_default_strlen(const unsigned char *key,
                                        int len);
int files_util_rowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    const systbl_files_cursor * const pCur = (systbl_files_cursor *)cur;

    const char * file_name = pCur->files[pCur->rowid].name;
    const char * file_dir = pCur->files[pCur->rowid].dir;
    char file_offset[50];
    sprintf(file_offset, "%ld", pCur->files[pCur->rowid].current_chunk.offset);

    char id_str[strlen(file_name) + strlen(file_dir) + strlen(file_offset) + 1];
    snprintf(id_str, sizeof(id_str), "%s-%s-%s", file_name, file_dir, file_offset);

    *pRowid = hash_default_strlen((unsigned char *) id_str, sizeof(id_str));
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

static int is_file_name_constraint(const constraint_type_t constraint_name) {
    return (constraint_name == FILES_FILE_NAME_LIKE_FLAG
            || constraint_name == FILES_FILE_NAME_EQ_FLAG
            || constraint_name == FILES_FILE_NAME_NEQ_FLAG);
}

static int is_file_type_constraint(const constraint_type_t constraint_name) {
    return (constraint_name == FILES_FILE_TYPE_EQ_FLAG
            || constraint_name == FILES_FILE_TYPE_NEQ_FLAG);
}

int files_util_filter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                       const char *idxStr, int argc, sqlite3_value **argv)
{
    logmsg(LOGMSG_DEBUG, "%s\n", __func__);

    int rc = SQLITE_OK;

    systbl_files_cursor *pCur = (systbl_files_cursor *)pVtabCursor;
    const constraint_type_t * const constraint_names = (const constraint_type_t * const) idxStr;
    comdb2_files_constraints_t constraint_info = {0};

    for (int i=0; i<argc; ++i) {
        const constraint_type_t constraint_name = constraint_names[i];
        constraint_info.n_file_name_constraints += is_file_name_constraint(constraint_name);
        constraint_info.n_file_type_constraints += is_file_type_constraint(constraint_name);
    }

    if (constraint_info.n_file_name_constraints) {
        constraint_info.file_name_constraints = calloc(1,
            constraint_info.n_file_name_constraints * sizeof(constraint_value_t));
        if (!constraint_info.file_name_constraints) { return ENOMEM; }
    }

    if (constraint_info.n_file_type_constraints) {
        constraint_info.file_type_constraints = calloc(1,
            constraint_info.n_file_type_constraints * sizeof(constraint_value_t));
        if (!constraint_info.file_type_constraints) { return ENOMEM; }
    }

    int file_name_idx = 0;
    int file_type_idx = 0;

    for (int i=0; i<argc; ++i) {
        const constraint_type_t constraint_name = constraint_names[i];

        if (is_file_name_constraint(constraint_name)) {
            logmsg(LOGMSG_DEBUG, "%s: Applying file name constraint\n", __func__);
            constraint_info.file_name_constraints[file_name_idx++] = (const constraint_value_t) {
                (char *)sqlite3_value_text(argv[i]),
                constraint_name,
            };
        } else if (is_file_type_constraint(constraint_name)) {
            logmsg(LOGMSG_DEBUG, "%s: Applying file type constraint\n", __func__);
            constraint_info.file_type_constraints[file_type_idx++] = (const constraint_value_t) { 
                (void *)str_to_file_type((char *)sqlite3_value_text(argv[i])),
                constraint_name,
            };
        } else if (constraint_name == FILES_CHUNK_SIZE_EQ_FLAG) {
            logmsg(LOGMSG_DEBUG, "%s: Applying chunk size constraint\n", __func__);
            constraint_info.chunk_size_constraint = sqlite3_value_int64(argv[i]);
        }
    }

    if (pCur->files) { release_files(pCur->files, pCur->nfiles); }
    if (get_files((void **)&pCur->files, &pCur->nfiles, &constraint_info)) {
        logmsg(LOGMSG_ERROR, "%s:%d: Failed to get files following filter\n",
               __FILE__, __LINE__);
        rc = SQLITE_ERROR;
        goto err;
    }

    pCur->rowid = 0;

    qsort(pCur->files, pCur->nfiles, sizeof(db_file_t), file_cmp);
    
err:
    if (constraint_info.file_name_constraints) { free(constraint_info.file_name_constraints); }
    if (constraint_info.file_type_constraints) { free(constraint_info.file_type_constraints); }
    return rc;
}

static void add_constraint(sqlite3_index_info * const pIdxInfo,
                           const int constraint_ix,
                           int * const argv_ix,
                           const int omit) {
    if (constraint_ix >= 0) {
        pIdxInfo->aConstraintUsage[constraint_ix].argvIndex = ++(*argv_ix);
        pIdxInfo->aConstraintUsage[constraint_ix].omit = omit;
    }
}

// Very cursory cost estimate.
static double estimate_cost(const constraint_type_t * const constraints, const int n_constraints) {
    double cost_est = DBL_MAX;
    for (int i=0; i<n_constraints; ++i) {
        const constraint_type_t constraint = constraints[i];

        if (constraint == FILES_FILE_NAME_EQ_FLAG) {
            cost_est /= pow(10, 6);
        }
        if (constraint == FILES_FILE_NAME_LIKE_FLAG) {
            cost_est /= pow(10, 5);
        }
        if (constraint == FILES_FILE_TYPE_EQ_FLAG) {
            cost_est /= pow(10, 4);
        }
        if (constraint == FILES_FILE_TYPE_NEQ_FLAG) {
            cost_est /= pow(10, 3);
        }
        if (constraint == FILES_FILE_NAME_NEQ_FLAG) {
            cost_est /= pow(10, 2);
        }
    }
    return cost_est;
}

static int parse_file_name_constraint(const struct sqlite3_index_constraint * const pConstraint, constraint_type_t * const constraint) {
    if (pConstraint->op == SQLITE_INDEX_CONSTRAINT_LIKE) {
        *constraint = FILES_FILE_NAME_LIKE_FLAG;
    } else if (pConstraint->op == SQLITE_INDEX_CONSTRAINT_EQ) {
        *constraint = FILES_FILE_NAME_EQ_FLAG;
    } else if (pConstraint->op == SQLITE_INDEX_CONSTRAINT_NE) {
        *constraint = FILES_FILE_NAME_NEQ_FLAG;
    } else {
        logmsg(LOGMSG_ERROR, "%s:%d: Column '%s' can only be constrained with 'like', '=', or '!='\n",
               __FILE__, __LINE__, print_column_name(FILES_COLUMN_FILENAME));
        return SQLITE_ERROR;
    }

    return SQLITE_OK;
}

static int parse_file_type_constraint(const struct sqlite3_index_constraint * const pConstraint, constraint_type_t * const constraint) {
    if (pConstraint->op == SQLITE_INDEX_CONSTRAINT_EQ) {
        *constraint = FILES_FILE_TYPE_EQ_FLAG;
    } else if (pConstraint->op == SQLITE_INDEX_CONSTRAINT_NE) {
        *constraint = FILES_FILE_TYPE_NEQ_FLAG;
    } else {
        logmsg(LOGMSG_ERROR, "%s:%d: Column '%s' can only be constrained with '=', or '!='\n",
               __FILE__, __LINE__, print_column_name(FILES_COLUMN_TYPE));
        return SQLITE_ERROR;
    }

    return SQLITE_OK;
}

static int parse_file_chunk_size_constraint(const struct sqlite3_index_constraint * const pConstraint, constraint_type_t * const constraint) {
    if (pConstraint->op == SQLITE_INDEX_CONSTRAINT_EQ) {
        *constraint = FILES_CHUNK_SIZE_EQ_FLAG;
    } else {
        logmsg(LOGMSG_ERROR, "%s:%d: Column '%s' can only be constrained with '=', or '!='\n",
               __FILE__, __LINE__, print_column_name(FILES_COLUMN_CHUNK_SIZE));
        return SQLITE_ERROR;
    }

    return SQLITE_OK;
}

static int parse_constraint(const struct sqlite3_index_constraint * const pConstraint,
                            const int i, constraint_type_t * const constraints) {

    if (pConstraint->iColumn == FILES_COLUMN_FILENAME 
        && parse_file_name_constraint(pConstraint, constraints + i))
    {
        return SQLITE_ERROR;
    } else if (pConstraint->iColumn == FILES_COLUMN_TYPE 
        && parse_file_type_constraint(pConstraint, constraints + i))
    {
        return SQLITE_ERROR;
    } else if (pConstraint->iColumn == FILES_COLUMN_CHUNK_SIZE 
        && parse_file_chunk_size_constraint(pConstraint, constraints + i))
    {
        return SQLITE_ERROR;
    }

    return SQLITE_OK;
}

int files_util_best_index(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo)
{
    logmsg(LOGMSG_DEBUG, "%s\n", __func__);

    int nArg = 0;           /* Number of arguments that filesFilter() expects */

    constraint_type_t * constraints = NULL;
    if (pIdxInfo->nConstraint > 0) {
        constraints = sqlite3_malloc(sizeof(constraint_type_t)*pIdxInfo->nConstraint);
        if (!constraints) { return ENOMEM; }
        memset(constraints, 0, sizeof(constraint_type_t)*pIdxInfo->nConstraint);
    }

    const struct sqlite3_index_constraint *pConstraint;
    pConstraint = pIdxInfo->aConstraint;
    for (int i = 0; i < pIdxInfo->nConstraint; i++, pConstraint++) {
        if (pConstraint->usable == 0) continue;
        if (parse_constraint(pConstraint, i, constraints)
            != SQLITE_OK) {
            return SQLITE_ERROR;
        }
        if (constraints[i] != 0) { add_constraint(pIdxInfo, i, &nArg, 1 /* omit */); }
    }

    pIdxInfo->orderByConsumed = 0;
    pIdxInfo->estimatedCost = estimate_cost(constraints, pIdxInfo->nConstraint);
    pIdxInfo->idxStr = (char *) constraints;
    pIdxInfo->needToFreeIdxStr = constraints != NULL;

    logmsg(LOGMSG_DEBUG, "%s: Estimated cost for index with %d constraints is %f\n",
        __func__, nArg, pIdxInfo->estimatedCost);

    return SQLITE_OK;
}
