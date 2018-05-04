/*
   Copyright 2018 Bloomberg Finance L.P.

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

#include "sqloffload.h"
#include "sql.h"
#include "isql.h"

/* Internal SQL execution */

int bind_stmt_mem(struct schema *sc, sqlite3_stmt *stmt, Mem *m);
int tdef_to_tranlevel(int tdef);

int isql_init(struct sqlclntstate *clnt)
{
    assert(clnt);

    reset_clnt(clnt, NULL, 1);

    pthread_mutex_init(&clnt->wait_mutex, NULL);
    pthread_cond_init(&clnt->wait_cond, NULL);
    pthread_mutex_init(&clnt->write_lock, NULL);
    pthread_mutex_init(&clnt->dtran_mtx, NULL);

    clnt->dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
    set_high_availability(clnt, 0);
    clnt->is_newsql = 0;

    clnt->isql_exec_mode = 0;

    assert(!clnt->isql_data);
    clnt->isql_data = calloc(1, sizeof(isql_data_t));
    if (!clnt->isql_data)
        return 1;

    return 0;
}

int isql_destroy(struct sqlclntstate *clnt)
{
    assert(clnt);

    clnt_reset_cursor_hints(clnt);
    osql_clean_sqlclntstate(clnt);

    if (clnt->dbglog) {
        sbuf2close(clnt->dbglog);
        clnt->dbglog = NULL;
    }

    clnt->dbtran.mode = TRANLEVEL_INVALID;
    if (clnt->query_stats)
        free(clnt->query_stats);

    pthread_mutex_destroy(&clnt->wait_mutex);
    pthread_cond_destroy(&clnt->wait_cond);
    pthread_mutex_destroy(&clnt->write_lock);
    pthread_mutex_destroy(&clnt->dtran_mtx);

    if (clnt->isql_data)
        free(clnt->isql_data);

    clnt->isql_exec_mode = 0;

    return 0;
}

static int isql_exec_inline(struct sqlclntstate *clnt)
{
    /* TODO: implement */
    return 0;
}

/*
  Makes a hard copy of the result column from an internal
  sql query so that we have access to the result even after
  the sql thread exits. The hard copy can later be converted
  from sqlite to comdb2 format.
*/
static int isql_fetch_row(sqlite3_stmt *stmt, int col_count, Mem *row)
{
    int rc = 0;
    Mem *pTo;
    Mem *pFrom;

    for (int i = 0; i < col_count; i++) {
        pTo = &row[i];
        pFrom = sqlite3_column_value(stmt, 0);
        assert(pTo);

        memcpy(pTo, pFrom, MEMCELLSIZE);
        pTo->db = NULL;
        pTo->szMalloc = 0;
        pTo->zMalloc = NULL;
        pTo->n = 0;
        pTo->z = NULL;
        pTo->flags &= ~MEM_Dyn;
        if (pFrom->flags & (MEM_Blob | MEM_Str)) {
            if (pFrom->zMalloc && pFrom->szMalloc) {
                pTo->szMalloc = pFrom->szMalloc;
                pTo->zMalloc = malloc(pTo->szMalloc);
                if (pTo->zMalloc == NULL)
                    return SQLITE_NOMEM;
                memcpy(pTo->zMalloc, pFrom->zMalloc, pTo->szMalloc);
                pTo->z = pTo->zMalloc;
                pTo->n = pFrom->n;
            } else if (pFrom->z && pFrom->n) {
                pTo->n = pFrom->n;
                pTo->szMalloc = pFrom->n + 1;
                pTo->zMalloc = malloc(pTo->szMalloc);
                if (pTo->zMalloc == NULL)
                    return SQLITE_NOMEM;
                memcpy(pTo->zMalloc, pFrom->z, pFrom->n);
                pTo->zMalloc[pFrom->n] = 0;
                pTo->z = pTo->zMalloc;
            }
        }
    }

    return rc;
}

int isql_exec(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    sqlite3_stmt *stmt;
    const char *unused;
    int rc;
    int col_count;

    if (thd->sqldb == NULL) {
        rdlock_schema_lk();
        rc = sqlengine_prepare_engine(thd, clnt, 1);
        unlock_schema_lk();
        if (rc) {
            return rc;
        }
    }

    rc = sqlite3_prepare_v2(thd->sqldb, clnt->sql, -1, &stmt, &unused);
    if (rc != SQLITE_OK) {
        return rc;
    }

    bind_stmt_mem(clnt->isql_data->s, stmt, clnt->isql_data->in);
    /* TODO: Handle error */

    run_stmt_setup(clnt, stmt);

    /*
      For now it fetches only the first row from the result set.
      TODO: Fetch further rows if needed.
    */
    if ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        col_count = sqlite3_column_count(stmt);
        if (col_count) {
            clnt->isql_data->out = malloc(col_count * sizeof(Mem));
            if (!clnt->isql_data->out) {
                return 1;
            }

            rc = isql_fetch_row(stmt, col_count, clnt->isql_data->out);
            if (rc) {
                free(clnt->isql_data->out);
                clnt->isql_data->out = 0;
                return rc;
            }
            clnt->isql_data->row_count++;
        }
    }

    if (rc == SQLITE_DONE) {
        return 0;
    }
    return rc;
}

int isql_run(struct sqlclntstate *clnt, const char *sql, int mode)
{
    int rc = 0;

    assert(clnt);

#ifdef DEBUGQUERY
    printf("isql_exec() sql: '%s'\n", sql);
#endif

    clnt->sql = (char *)sql;
    clnt->isql_exec_mode = mode;

    switch (mode) {
    case ISQL_EXEC_NORMAL: /* fallthrough */
    case ISQL_EXEC_QUICK:
        dispatch_sql_query(clnt);
        break;
    case ISQL_EXEC_INLINE:
        rc = isql_exec_inline(clnt);
        break;
    default:
        assert(0);
    }

    if (clnt->query_rc || clnt->saved_errstr) {
        logmsg(LOGMSG_ERROR, "%s: Error from query: '%s' (rc = %d) \n",
               __func__, sql, clnt->query_rc);
        if (clnt->saved_errstr)
            logmsg(LOGMSG_ERROR, "%s: Error: '%s' \n", __func__,
                   clnt->saved_errstr);
        rc = 1;
    }

    return rc;
}
