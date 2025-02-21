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
#include "math_util.h"

extern struct dbenv *thedb;
extern char *comdb2_get_sav_dir_name(void);
extern char *comdb2_get_tmp_dir_name(void);
extern char gbl_dbname[MAX_DBNAME_LENGTH];

static const struct compareInfo globCaseInfo = {'%', '_', '[', 0};
extern int patternCompare(const u8 *, const u8 *, const struct compareInfo *,
                          u32);

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

typedef struct index_constraint_value {
    struct sqlite3_index_constraint sqlite_constraint;
    void * value;
    LINKC_T(struct index_constraint_value) lnk;
} index_constraint_value_t;

static int file_name_matches_constraints(const listc_t * const constraints,
                                        const char * const file_name) {
    int is_match = 1;

    index_constraint_value_t * constraint;
    LISTC_FOR_EACH(constraints, constraint, lnk)
    {
        if (constraint->sqlite_constraint.iColumn != FILES_COLUMN_FILENAME) {
            continue;
        }

        const int constraint_op = constraint->sqlite_constraint.op;
        const char * constraint_val = (const char *) constraint->value;

        if (constraint_op == SQLITE_INDEX_CONSTRAINT_EQ) {
            is_match = strcmp((const char *) constraint_val, file_name) == 0;
        } else if (constraint_op == SQLITE_INDEX_CONSTRAINT_NE) {
            is_match = strcmp((const char *) constraint_val, file_name) != 0;
        } else if (constraint_op == SQLITE_INDEX_CONSTRAINT_LIKE) {
            is_match = patternCompare((u8 *)constraint_val, (u8 *)file_name, &globCaseInfo, '[') == 0;
        } else {
            logmsg(LOGMSG_ERROR, "%s: Unexpected constraint operation %d\n", __func__, constraint_op);
            abort();
        }

        if (!is_match) { break; }
    }

    return is_match;
}

static int file_type_matches_constraints(const listc_t * const constraints,
                                        const file_type_t file_type) {
    int is_match = 1;

    index_constraint_value_t * constraint;
    LISTC_FOR_EACH(constraints, constraint, lnk)
    {
        if (constraint->sqlite_constraint.iColumn != FILES_COLUMN_TYPE) {
            continue;
        }

        const int constraint_op = constraint->sqlite_constraint.op;
        const file_type_t constraint_val = (const file_type_t) constraint->value;

        if (constraint_op == SQLITE_INDEX_CONSTRAINT_EQ) {
            is_match = constraint_val == file_type;
        } else if (constraint_op == SQLITE_INDEX_CONSTRAINT_NE) {
            is_match = constraint_val != file_type;
        } else {
            logmsg(LOGMSG_ERROR, "%s: Unexpected constraint operation %d\n", __func__, constraint_op);
            abort();
        }

        if (!is_match) { break; }
    }

    return is_match;
}

/* Returns chunk size constraint or 0 if there is none */
static int64_t get_chunk_size_constraint(const listc_t * const constraints) {
    index_constraint_value_t * constraint;
    LISTC_FOR_EACH(constraints, constraint, lnk)
    {
        if (constraint->sqlite_constraint.iColumn != FILES_COLUMN_CHUNK_SIZE) {
            continue;
        }

        return (int64_t) constraint->value;
    }

    return 0;
}


static int read_dir(const char *dirname, db_file_t **files, int *count, const listc_t * const constraints)
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

        if (!file_name_matches_constraints(constraints, de->d_name)) {
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

        if (!file_type_matches_constraints(constraints, type)) {
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

       
        set_chunk_size(f, get_chunk_size_constraint(constraints));

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

static int get_files(void **data, size_t * npoints, const listc_t * const constraints)
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

static void * get_constraint_value(struct sqlite3_index_constraint constraint, sqlite3_value *value) {
    if (constraint.iColumn == FILES_COLUMN_FILENAME) {
        return (void *) sqlite3_value_text(value);
    } else if (constraint.iColumn == FILES_COLUMN_TYPE) {
        return (void *) str_to_file_type((const char *) sqlite3_value_text(value));
    } else if (constraint.iColumn == FILES_COLUMN_CHUNK_SIZE) {
        return (void *) sqlite3_value_int64(value);
    } else {
        logmsg(LOGMSG_ERROR, "%s: Constraint column not expected\n", __func__);
        abort();
    }
}

typedef struct accepted_index_constraint {
    struct sqlite3_index_constraint sqlite_constraint;
    int sqlite_ix; /* constraint's index in sqlite3_index_info.aConstraint */
} accepted_index_constraint_t;

listc_t * parse_constraint_values(const accepted_index_constraint_t * const accepted_constraints,
                                  int num_values, sqlite3_value ** values) {
    listc_t * parsed_values = listc_new(offsetof(index_constraint_value_t, lnk));
    if (!parsed_values) {
        logmsg(LOGMSG_ERROR, "%s:%d: Failed to allocate list\n", __FILE__, __LINE__);
        return NULL;
    }

    for (int i=0; i<num_values; ++i) {
        const struct sqlite3_index_constraint sqlite_constraint = accepted_constraints[i].sqlite_constraint;

        index_constraint_value_t * parsed_value = malloc(sizeof(index_constraint_value_t));
        parsed_value->sqlite_constraint = sqlite_constraint;
        parsed_value->value = get_constraint_value(sqlite_constraint, values[i]);

        listc_atl(parsed_values, parsed_value);
    }

    return parsed_values;
}

int files_util_filter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                       const char *idxStr, int argc, sqlite3_value **argv)
{
    logmsg(LOGMSG_DEBUG, "%s called with %d constraints\n", __func__, argc);

    int rc = SQLITE_OK;

    systbl_files_cursor * const pCur = (systbl_files_cursor *)pVtabCursor;
    if (pCur->files) {
        release_files(pCur->files, pCur->nfiles);
    }

    listc_t * constraint_values =
        parse_constraint_values((accepted_index_constraint_t *) idxStr, argc, argv);
    if (!constraint_values) {
        logmsg(LOGMSG_ERROR, "%s:%d: Failed to parse constraint values\n", __FILE__, __LINE__);
        rc = SQLITE_ERROR;
        goto done;
    }

    rc = get_files((void **)&pCur->files, &pCur->nfiles, constraint_values);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d: Failed to get files following filter\n",
               __FILE__, __LINE__);
        goto done;
    }

    pCur->rowid = 0;
    qsort(pCur->files, pCur->nfiles, sizeof(db_file_t), file_cmp);

done:
    if (constraint_values) {
        LISTC_CLEAN(constraint_values, lnk, 1, index_constraint_value_t);
        free(constraint_values);
    }
    return rc;
}

/* Generates a very cursory cost estimate from a list of accepted constraints.
 *
 * Costs are assigned in this order (from 1-lowest to 4-highest):
 * 
 * 1. Plan contains an '=' constraint on 'filename'
 * 2. Plan contains a 'like' constraint on 'filename'
 * 3. Plan contains a '!=' constraint on 'filename', or a '='/'!=' constraint on 'type'
 * 4. Plan contains no constraints, or only contains constraints on 'chunk_size'
 *
 * These costs are not meant to be a realistic reflection of the expected number of disk
 * accesses. They are just meant to roughly differentiate plans.
*/
static double estimate_cost(const accepted_index_constraint_t * const accepted_constraints) {
    double cost_est = DBL_MAX;

    for (int i=0; accepted_constraints[i].sqlite_ix != -1; ++i) {
        const struct sqlite3_index_constraint constraint = accepted_constraints[i].sqlite_constraint;

        if (constraint.iColumn == FILES_COLUMN_FILENAME
            && constraint.op == SQLITE_INDEX_CONSTRAINT_EQ) {
            cost_est = min(cost_est, pow(10, 4));
        } else if (constraint.iColumn == FILES_COLUMN_FILENAME
            && constraint.op == SQLITE_INDEX_CONSTRAINT_LIKE) {
            cost_est = min(cost_est, pow(10, 5));
        } else if (constraint.iColumn == FILES_COLUMN_FILENAME
            || constraint.iColumn == FILES_COLUMN_TYPE) {
            cost_est = min(cost_est, pow(10, 6));
        }
    }

    return cost_est;
}

static int is_accepted_file_name_constraint(const struct sqlite3_index_constraint * const pConstraint) {
    return (pConstraint->op == SQLITE_INDEX_CONSTRAINT_LIKE
    || pConstraint->op == SQLITE_INDEX_CONSTRAINT_EQ
    || pConstraint->op == SQLITE_INDEX_CONSTRAINT_NE);
}

static int is_accepted_file_type_constraint(const struct sqlite3_index_constraint * const pConstraint) {
    return (pConstraint->op == SQLITE_INDEX_CONSTRAINT_EQ
        || pConstraint->op == SQLITE_INDEX_CONSTRAINT_NE);
}

static int is_accepted_chunk_size_constraint(const struct sqlite3_index_constraint * const pConstraint) {
    return pConstraint->op == SQLITE_INDEX_CONSTRAINT_EQ;
}

static int is_accepted_constraint(const struct sqlite3_index_constraint * const candidate_constraint) {
    if (!candidate_constraint->usable) {
        return 0; 
    } else if (candidate_constraint->iColumn == FILES_COLUMN_FILENAME)
    {
        return is_accepted_file_name_constraint(candidate_constraint);
    } else if (candidate_constraint->iColumn == FILES_COLUMN_TYPE)
    {
        return is_accepted_file_type_constraint(candidate_constraint);
    } else if (candidate_constraint->iColumn == FILES_COLUMN_CHUNK_SIZE)
    {
        return is_accepted_chunk_size_constraint(candidate_constraint);
    } else {
        return 0;
    }
}

/* 
 * Parses sqlite's candidate constraints and returns a list including only those candidate
 * constraints that we can provide an index over.
 *
 * For example, if xBestIndex is passed the following through `sqlite3_index_info.aConstraint`:
 * [{col: 'filename', op: '='}; {col: 'content', op: '='}; {'col: 'type', op: '!='}]
 *
 * then this function will return:
 * [
 *      {constraint: {col: 'filename', op: '='}, sqlite_ix: 0},
 *      {constraint: {'col: 'type', op: '!='}, sqlite_ix: 2}
 * ]
 */
static accepted_index_constraint_t * get_accepted_constraints(sqlite3_index_info * const pIdxInfo) {
    /* Additional value used as sentinel */
    const ssize_t num_constraints = sizeof(accepted_index_constraint_t)*(pIdxInfo->nConstraint+1);
    accepted_index_constraint_t * const accepted_constraints = malloc(num_constraints);
    if (!accepted_constraints) { return NULL; }

    /* We will use .sqlite_ix == -1 as a sentinel */
    memset(accepted_constraints, -1, num_constraints);

    int accepted_constraint_ix = 0;
    for (int sqlite_ix = 0; sqlite_ix < pIdxInfo->nConstraint; sqlite_ix++) {
        const struct sqlite3_index_constraint * const candidate_constraint = 
            pIdxInfo->aConstraint + sqlite_ix;

        if (is_accepted_constraint(candidate_constraint)) { 
            accepted_constraints[accepted_constraint_ix].sqlite_constraint = *candidate_constraint;
            accepted_constraints[accepted_constraint_ix].sqlite_ix = sqlite_ix;
            accepted_constraint_ix++;
        }
    }

    return accepted_constraints;
}

/*
 * Populates `sqlite3_index_info`'s output variables.
 *
 * See sqlite3 virtual table docs for more information about these variables.
 */
static void populate_best_index_outputs(sqlite3_index_info * const pIdxInfo,
        const accepted_index_constraint_t * const accepted_constraints,
        const double estimated_cost) {

    pIdxInfo->orderByConsumed = 0;
    pIdxInfo->estimatedCost = estimated_cost;

    // xFilter is passed an array of arguments. Each of these
    // arguments is the value of a constraint.
    // 
    // xFilter needs a way to figure out which constraint each
    // argument is associated with.
    //
    // We accomplish this by passing our list of accepted constraints
    // to xFilter (via `sqlite3_index_info.idxStr`) such that the
    // n-th constraint in our list of accepted constraints corresponds
    // with the n-th xFilter argument
    pIdxInfo->idxStr = (char *) accepted_constraints;
    pIdxInfo->needToFreeIdxStr = 1;

    int sqlite_ix;
    for (int i=0; (sqlite_ix = accepted_constraints[i].sqlite_ix), sqlite_ix != -1; ++i) {
        // Set argvIndex so that it matches the constraint's
        // position in our list of accepted constraints.
        pIdxInfo->aConstraintUsage[sqlite_ix].argvIndex = i+1; /* argvIndex is 1-based */
        pIdxInfo->aConstraintUsage[sqlite_ix].omit = 1;
    }
}

int files_util_best_index(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo)
{
    if (pIdxInfo->nConstraint <= 0) { return SQLITE_OK; }

    const accepted_index_constraint_t * const accepted_constraints = get_accepted_constraints(pIdxInfo);
    const double estimated_cost = estimate_cost(accepted_constraints);

    logmsg(LOGMSG_DEBUG,
           "%s: Estimated cost for index with %d candidate constraints is %f\n", __func__, pIdxInfo->nConstraint, estimated_cost);

    populate_best_index_outputs(pIdxInfo, accepted_constraints, estimated_cost);
    return SQLITE_OK;
}
