#include <pthread.h>
#include "comdb2_plugin.h"
#include "sql.h"
#include "comdb2_query_preparer.h"
#include "locks_wrap.h"
#include "sqlitex.h"

extern pthread_key_t sqlitexVDBEkey;
extern pthread_mutex_t open_serial_lock;
extern int gbl_serialise_sqlite3_open;

/* Skip spaces and tabs, requires at least one space */
static inline char *skipws(char *str)
{
    if (str) {
        while (*str && isspace(*str))
            str++;
    }
    return str;
}

static int sqlitex_open_serial(const char *filename, sqlitex **ppDb,
                               struct sqlthdstate *thd)
{
    int rc;
    int serial;

    serial = gbl_serialise_sqlite3_open;
    if (serial)
        Pthread_mutex_lock(&open_serial_lock);

    rc = sqlitex_open(filename, ppDb, thd);

    if (serial)
        Pthread_mutex_unlock(&open_serial_lock);

    return rc;
}

static int sqlite3_close_serial(sqlite3 **ppDb)
{
    int rc = SQLITE_ERROR;
    int serial = gbl_serialise_sqlite3_open;
    if (serial)
        Pthread_mutex_lock(&open_serial_lock);
    if (ppDb && *ppDb) {
        rc = sqlite3_close(*ppDb);
        if (rc == SQLITE_OK) {
            *ppDb = NULL;
        } else {
            logmsg(LOGMSG_ERROR, "%s: sqlite3_close FAILED rc=%d, msg=%s\n",
                   __func__, rc, sqlite3_errmsg(*ppDb));
        }
    }
    if (serial)
        Pthread_mutex_unlock(&open_serial_lock);
    return rc;
}

static int sqlitex_close_serial(sqlitex **ppDb)
{
    int rc = SQLITE_ERROR;
    int serial = gbl_serialise_sqlite3_open;
    if (serial)
        Pthread_mutex_lock(&open_serial_lock);
    if (ppDb && *ppDb) {
        rc = sqlitex_close(*ppDb);
        if (rc == SQLITE_OK) {
            *ppDb = NULL;
        } else {
            logmsg(LOGMSG_ERROR, "%s: sqlite3_close FAILED rc=%d, msg=%s\n",
                   __func__, rc, sqlitex_errmsg(*ppDb));
        }
    }
    if (serial)
        Pthread_mutex_unlock(&open_serial_lock);
    return rc;
}

static void log_old_columns(int log_level, sqlitex_stmt *stmtx)
{
    int column_count = sqlitex_column_count(stmtx);
    logmsg(log_level, "columns: %d (", column_count);
    for (int i = 0; i < column_count; i++) {
        logmsg(log_level, "%s%s", (i > 0) ? ", " : "",
               sqlitex_column_name(stmtx, i));
    }
    logmsg(log_level, ")\n");
}

static void save_old_columns(struct sqlclntstate *clnt, sqlitex_stmt *stmtx)
{
    int old_columns_count = sqlitex_column_count(stmtx);
    clnt->old_columns = calloc(sizeof(char *), old_columns_count);
    if (clnt->old_columns == NULL) {
        goto oom;
        return;
    }
    clnt->old_columns_count = old_columns_count;

    for (int i = 0; i < old_columns_count; i++) {
        clnt->old_columns[i] = strdup(sqlitex_column_name(stmtx, i));
        if (clnt->old_columns[i] == 0) {
            goto oom;
        }
    }
    return;

oom:
    logmsg(LOGMSG_ERROR, "%s:%d out-of-memory\n", __func__, __LINE__);
}

static void free_old_columns(struct sqlclntstate *clnt)
{
    if (clnt->old_columns == 0) {
        return;
    }

    for (int i = 0; i < clnt->old_columns_count; i++) {
        free(clnt->old_columns[i]);
    }
    free(clnt->old_columns);

    clnt->old_columns = 0;
    clnt->old_columns_count = 0;

    return;
}

static int sqlitex_prepare_query(struct sqlthdstate *thd,
                                 struct sqlclntstate *clnt, const char *sql)
{
    sqlitex *dbx = 0;
    sqlitex_stmt *stmtx = 0;
    const char *tail = 0;
    int rc;

    assert(clnt->old_columns == 0);

    rc = sqlitex_open_serial("db", &dbx, thd);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s:sqlite3_open_serial failed %d: %s\n", __func__,
               rc, sqlite3_errmsg(thd->sqldb));
        sqlitex_close_serial(&dbx);
        /* there is no really way forward, grab core */
        abort();
    }
    clnt->prep_rc = rc = sqlitex_prepare_v2(dbx, sql, -1, &stmtx, &tail);
    save_old_columns(clnt, stmtx);

    sqlitex_finalize(stmtx);
    sqlitex_close_serial(&dbx);

    return 0;
}

static int sqlitex_init(void *unused)
{
    Pthread_key_create(&sqlitexVDBEkey, NULL);
    if (sqlitex_initialize())
        abort();
    sqlitexMemSetDefault();
    return 0;
}

static int sqlitex_cleanup(struct sqlclntstate *clnt)
{
    free_old_columns(clnt);
    return 0;
}

static int sqlitex_destroy()
{
    Pthread_key_delete(sqlitexVDBEkey);
    return 0;
}

comdb2_query_preparer_t sqlitex_plugin = {.do_prepare = sqlitex_prepare_query,
                                          .do_cleanup = sqlitex_cleanup};

#include "plugin.h"
