#include <typessql.h>
#include "fdb_fend.h"

int gbl_typessql = 0;
int gbl_typessql_records_max = 1000;

struct row {
    char *packed;
    Mem *unpacked;
    long long row_size;
    sqlite3 *db;
};
typedef struct row row_t;

struct typessql {
    int *col_types;
    int ncols;
    int have_current_row;
    row_t current_row;
    int other_rc; // initialized to -1. Set if rc for grabbing next row is not SQLITE_ROW
    int first_run; // initialized to 1
    struct temp_table *db;
    struct temp_cursor *cur;
    int size;
    int current;
};
typedef struct typessql typessql_t;

// from vdbeapi.c
static void columnMallocFailure(sqlite3_stmt *pStmt)
{
    /* If malloc() failed during an encoding conversion within an
    ** sqlite3_column_XXX API, then set the return code of the statement to
    ** SQLITE_NOMEM. The next call to _step() (if any) will return SQLITE_ERROR
    ** and _finalize() will return NOMEM.
    */
    Vdbe *p = (Vdbe *)pStmt;
    if (p) {
        assert(p->db!=0);
        assert(sqlite3_mutex_held(p->db->mutex));
        p->rc = sqlite3ApiExit(p->db, p->rc);
        sqlite3_mutex_leave(p->db->mutex);
    }
}

static void unpack_current_row(typessql_t *typessql_state, sqlite3_stmt *stmt)
{
    row_t *row = &typessql_state->current_row;
    Vdbe *pVm = (Vdbe *)stmt;
    sqlite3_mutex_enter(pVm->db->mutex); // will be unlocked in free_row
    row->unpacked = sqlite3UnpackedResult(stmt, typessql_state->ncols, row->packed, row->row_size);
    for (int i = 0; i < typessql_state->ncols; i++) {
        row->unpacked[i].db = pVm->db;
        row->unpacked[i].tz = pVm->tzname;
        row->unpacked[i].dtprec = pVm->dtprec;
    }
    row->db = pVm->db;
}

static void free_row(row_t *row, int ncols)
{
    if (!row)
        return;

    if (row->unpacked) {
        sqlite3UnpackedResultFree(&row->unpacked, ncols);
        sqlite3_mutex_leave(row->db->mutex); // locked in unpack_current_row
    }
}

void free_typessql_state(struct sqlclntstate *clnt)
{
    typessql_t *typessql_state = clnt->typessql_state;
    free(typessql_state->col_types);
    free_row(&typessql_state->current_row, typessql_state->ncols);
    int bdberr = 0;
    int rc = 0;
    if (typessql_state->cur) {
        rc = bdb_temp_table_close_cursor(thedb->bdb_env, typessql_state->cur, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: error closing temp cursor rc=%d bdberr=%d\n", __func__, rc, bdberr);
        }
    }
    if (typessql_state->db) {
        rc = bdb_temp_table_close(thedb->bdb_env, typessql_state->db, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: error closing temp table rc=%d bdberr=%d\n", __func__, rc, bdberr);
        }
    }
    free(typessql_state);
}

static int typessql_column_count(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    return clnt->adapter.column_count ? clnt->adapter.column_count(clnt, stmt) : sqlite3_column_count(stmt);
}

// if iCol >= ncols try to return non-null type
// else return type of column for specific row (use case: check if field in row is NULL)
static int typessql_column_type(struct sqlclntstate *clnt, sqlite3_stmt *stmt, int iCol)
{
    typessql_t *typessql_state = clnt->typessql_state;
    int nonNullType = 0;
    if (iCol >= typessql_state->ncols) {
        iCol -= typessql_state->ncols;
        nonNullType = 1;
    }
    if (nonNullType && typessql_state->col_types)
        return typessql_state->col_types[iCol];
    else if (typessql_state->have_current_row) {
        row_t *row = &typessql_state->current_row;
        if (!row->unpacked)
            unpack_current_row(typessql_state, stmt);
        sqlite3_mutex_enter(((Vdbe *)stmt)->db->mutex);
        int val = sqlite3_value_type(&row->unpacked[iCol]);
        columnMallocFailure(stmt);
        return val;
    }

    return clnt->adapter.column_type ? clnt->adapter.column_type(clnt, stmt, iCol) : sqlite3_column_type(stmt, iCol);
}

// return if null column still exists
static int update_column_types(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    int null_col_exists = 0;
    int first_run = 0;
    int type;
    typessql_t *typessql_state = clnt->typessql_state;
    if (!typessql_state->col_types) {
        first_run = 1;
        typessql_state->col_types = malloc(typessql_state->ncols * sizeof(int));
        for (int i = 0; i < typessql_state->ncols; i++) {
            typessql_state->col_types[i] = SQLITE_NULL;
        }
    }

    for (int i = 0; i < typessql_state->ncols; i++) {
        if (typessql_state->col_types[i] != SQLITE_NULL)
            continue;

        // not calling types column type here currently cuz this will read from queue
        int r = clnt->adapter.column_type ? clnt->adapter.column_type(clnt, stmt, i) : sqlite3_column_type(stmt, i);
        typessql_state->col_types[i] = r;
        if (typessql_state->col_types[i] != SQLITE_NULL)
            continue;

        if (first_run) {
            /*
            check declared types if can't find type from first row
            I decided to check the type of the first row and then declared type bc that is the same order that get_sqlite3_column_type uses
            But if done in opposite order, then would be able to initialize array to sqlite3_column_decltype and have cleaner code
            */
            type = typestr_to_type(sqlite3_column_decltype(stmt, i));
            if (type == SQLITE_TEXT) // assume SQLITE_TEXT means that this might be a null type, keep looking through rows
                null_col_exists = 1;
            else
                typessql_state->col_types[i] = type;
        } else
            null_col_exists = 1;
    }

    return null_col_exists;
}

int typessql_next_row(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    int r = SQLITE_ROW;
    typessql_t *typessql_state = clnt->typessql_state;
    if (typessql_state->first_run && gbl_typessql_records_max > 0) {
        typessql_state->first_run = 0;

        int null_col_exists = 1;
        while (null_col_exists && typessql_state->size < gbl_typessql_records_max) {
            r = clnt->adapter.next_row ? clnt->adapter.next_row(clnt, stmt) : sqlite3_maybe_step(clnt, stmt);
            if (r != SQLITE_ROW) {
                typessql_state->other_rc = r;
                break;
            }

            null_col_exists = update_column_types(clnt, stmt);
            if (typessql_state->size == 0) { // this is the first row we are looking at
                if (!null_col_exists) // no need to add row to queue, there are no null types
                    return r;
                else { // allocate temp table
                    int bdberr = 0;
                    struct temp_table *db = bdb_temp_array_create(thedb->bdb_env, &bdberr);
                    if (!db || bdberr) {
                        logmsg(LOGMSG_ERROR, "%s: failed to create temp table bdberr=%d\n", __func__, bdberr);
                        abort();
                    }
                    struct temp_cursor *cur = bdb_temp_table_cursor(thedb->bdb_env, db, NULL, &bdberr);
                    if (!cur || bdberr) {
                        logmsg(LOGMSG_ERROR, "%s: failed to create cursor bdberr=%d\n", __func__, bdberr);
                        abort();
                    }
                    typessql_state->db = db;
                    typessql_state->cur = cur;
                }
            }

            // add row to queue
            int bdberr = 0;
            int count = typessql_state->size++;
            long long packed_size;
            char *packed = sqlite3PackedResult(stmt, &packed_size);
            if (!packed) {
                logmsg(LOGMSG_ERROR, "%s: fail to pack row\n", __func__);
                abort();
            }
            int rc = bdb_temp_table_insert(thedb->bdb_env, typessql_state->cur, &count, sizeof(count), packed,
                                           packed_size, &bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to put into temp tbl rc=%d bdberr=%d\n", __func__, rc, bdberr);
                abort();
            }
            sqlite3_free(packed);
        }
    }

    if (typessql_state->have_current_row) {
        free_row(&typessql_state->current_row, typessql_state->ncols);
        memset(&typessql_state->current_row, 0, sizeof(typessql_state->current_row));
        typessql_state->have_current_row = 0;
    }
    // if queue return from queue
    if (typessql_state->current < typessql_state->size) {
        int bdberr = 0;
        int rc;
        if (typessql_state->current == 0)
            rc = bdb_temp_table_first(thedb->bdb_env, typessql_state->cur, &bdberr);
        else
            rc = bdb_temp_table_next(thedb->bdb_env, typessql_state->cur, &bdberr);
        if (rc)
            abort();
        typessql_state->current_row.packed = bdb_temp_table_data(typessql_state->cur);
        typessql_state->current_row.row_size = bdb_temp_table_datasize(typessql_state->cur);
        typessql_state->have_current_row = 1;
        typessql_state->current++;
        return SQLITE_ROW;
    }

    if (typessql_state->other_rc != -1) {
        r = typessql_state->other_rc;
        typessql_state->other_rc = -1; // reset
    } else
        r = clnt->adapter.next_row ? clnt->adapter.next_row(clnt, stmt) : sqlite3_maybe_step(clnt, stmt);

    return r;
}

#define FUNC_COLUMN_TYPE(ret, type)                                                                                             \
    static ret typessql_column_##type(struct sqlclntstate *clnt,                                                                \
                                      sqlite3_stmt *stmt, int iCol)                                                             \
    {                                                                                                                           \
        typessql_t *typessql_state = clnt->typessql_state;                                                                      \
        if (typessql_state->have_current_row) {                                                                                 \
            row_t *row = &typessql_state->current_row;                                                                          \
            if (!row->unpacked)                                                                                                 \
                unpack_current_row(typessql_state, stmt);                                                                       \
            sqlite3_mutex_enter(((Vdbe *)stmt)->db->mutex);                                                                     \
            ret val = sqlite3_value_##type(&row->unpacked[iCol]);                                                               \
            columnMallocFailure(stmt);                                                                                          \
            return val;                                                                                                         \
        }                                                                                                                       \
        return clnt->adapter.column_##type ? clnt->adapter.column_##type(clnt, stmt, iCol) : sqlite3_column_##type(stmt, iCol); \
    }

FUNC_COLUMN_TYPE(sqlite_int64, int64)
FUNC_COLUMN_TYPE(double, double)
FUNC_COLUMN_TYPE(int, bytes)
FUNC_COLUMN_TYPE(const unsigned char *, text)
FUNC_COLUMN_TYPE(const void *, blob)
FUNC_COLUMN_TYPE(const dttz_t *, datetime)

static const intv_t *typessql_column_interval(struct sqlclntstate *clnt,
                                              sqlite3_stmt *stmt, int iCol,
                                              int type)
{
    typessql_t *typessql_state = clnt->typessql_state;
    if (typessql_state->have_current_row) {
        row_t *row = &typessql_state->current_row;
        if (!row->unpacked)
            unpack_current_row(typessql_state, stmt);
        sqlite3_mutex_enter(((Vdbe *)stmt)->db->mutex);
        const intv_t *val = sqlite3_value_interval(&row->unpacked[iCol], type);
        columnMallocFailure(stmt);
        return val;
    }
    return clnt->adapter.column_interval ? clnt->adapter.column_interval(clnt, stmt, iCol, type) : sqlite3_column_interval(stmt, iCol, type);
}

static sqlite3_value *typessql_column_value(struct sqlclntstate *clnt,
                                            sqlite3_stmt *stmt, int iCol)
{
    typessql_t *typessql_state = clnt->typessql_state;
    if (typessql_state->have_current_row) {
        row_t *row = &typessql_state->current_row;
        if (!row->unpacked)
            unpack_current_row(typessql_state, stmt);
        sqlite3_mutex_enter(((Vdbe *)stmt)->db->mutex);
        Mem *val = &row->unpacked[iCol];
        if (val->flags & MEM_Static) {
            val->flags &= ~MEM_Static;
            val->flags |= MEM_Ephem;
        }
        columnMallocFailure(stmt);
        return (sqlite3_value *)val;
    }
    return clnt->adapter.column_value ? clnt->adapter.column_value(clnt, stmt, iCol) : sqlite3_column_value(stmt, iCol);
}

/*
TODO:
./bound without tz returns "c\0" if db set (would think trying to return "can't convert datetime value to string")
*/
static void _master_clnt_set(struct sqlclntstate *clnt, struct plugin_callbacks *adapter)
{
    assert(clnt->typessql_state);
    if (adapter && adapter->next_row == typessql_next_row) {
        abort();
    }

    clnt->backup = clnt->plugin;
    clnt->adapter_backup = clnt->adapter;

    if (adapter) {
        // TODO: Separate by function
        clnt->adapter = *adapter;
    } else {
        memset(&clnt->adapter, 0, sizeof(clnt->adapter));
    }

    clnt->plugin.column_count = typessql_column_count;
    clnt->plugin.next_row = typessql_next_row;
    clnt->plugin.column_type = typessql_column_type;
    clnt->plugin.column_int64 = typessql_column_int64;
    clnt->plugin.column_double = typessql_column_double;
    clnt->plugin.column_text = typessql_column_text;
    clnt->plugin.column_bytes = typessql_column_bytes;
    clnt->plugin.column_blob = typessql_column_blob;
    clnt->plugin.column_datetime = typessql_column_datetime;
    clnt->plugin.column_interval = typessql_column_interval;
    clnt->plugin.column_value = typessql_column_value;

    if (clnt->adapter.sqlite_error)
        clnt->plugin.sqlite_error = clnt->adapter.sqlite_error;
    if (clnt->adapter.param_count)
        clnt->plugin.param_count = clnt->adapter.param_count;
    if (clnt->adapter.param_value)
        clnt->plugin.param_value = clnt->adapter.param_value;
    if (clnt->adapter.param_index)
        clnt->plugin.param_index = clnt->adapter.param_index;
}

void typessql_end(struct sqlclntstate *clnt)
{
    if (!clnt->typessql_state)
        return;
    free_typessql_state(clnt);
    clnt->typessql_state = NULL;
    clnt_plugin_reset(clnt);
}

int typessql_initialize(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    if (!(gbl_typessql || clnt->typessql))
        return -1;

    typessql_t *typessql_state = calloc(1, sizeof(typessql_t));
    if (!typessql_state) {
        return -1;
    }
    typessql_state->other_rc = -1; // initialize
    typessql_state->first_run = 1;
    if (clnt->typessql_state) {
        assert(!clnt->typessql_state->have_current_row); // either queue is empty or fully traversed
        typessql_end(clnt);
    }
    clnt->typessql_state = typessql_state;

    _master_clnt_set(clnt, NULL);
    typessql_state->ncols = typessql_column_count(clnt, stmt);
    return 0;
}
