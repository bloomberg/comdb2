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

#include <stdlib.h>

#include "comdb2.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "cdb2api.h"
#include "strbuf.h"

struct sqlite3_vtab_all {
    const sqlite3_module *pModule;  /* The module for this virtual table */
    int nRef;                       /* Number of open cursors */
    char *zErrMsg;                  /* Error message from sqlite3_mprintf() */
    /* custom fields */
    char *select_sql;
};

struct sqlite3_vtab_cursor_all {
    sqlite3_vtab *pVtab;      /* Virtual table of this cursor */
    // above is what sqlite expects, we own the rest
    int num_nodes;
    char **nodes;
    int64_t rowid;
    int current_node;
    cdb2_hndl_tp *current_db;
    int eof;
};

static int systblAllConnect(sqlite3 *db, void *pAux, int argc,
                                const char *const *argv, sqlite3_vtab **ppVtab,
                                char **pErr) {
    char *table = (char *) pAux;

    Table *t = sqlite3LocateTable(db->pParse, 0, table, NULL);
    if (t == NULL)
        return SQLITE_ERROR;

    // Note: we're only going to wrap system tables, whose names and field names we control, so
    // we can take liberties with not needing to escape them.
    strbuf *strbuf;
    strbuf = strbuf_new();
    strbuf_appendf(strbuf, "CREATE TABLE %s_all(dbhost", table);
    char *sep;
    for (int i = 0; i < t->nCol; i++) {
        strbuf_appendf(strbuf, ", \"%s\"", t->aCol[i].zName);
    }
    strbuf_append(strbuf, ")");
    char *sql = strbuf_disown(strbuf);
    int rc = sqlite3_declare_vtab(db, sql);
    free(sql);
    if (rc)
        return rc;
    struct sqlite3_vtab_all *vt = calloc(1, sizeof(struct sqlite3_vtab_all));
    *ppVtab = (struct sqlite3_vtab*) vt;

    strbuf = strbuf_new();
    strbuf_append(strbuf, "SELECT ");
    sep = "";
    for (int i = 0; i < t->nCol; i++) {
        strbuf_appendf(strbuf, "%s\"%s\"", sep, t->aCol[i].zName);
        sep = ", ";
    }
    strbuf_appendf(strbuf, " FROM \"%s\"", table);
    vt->select_sql = strbuf_disown(strbuf);

    return rc;
}

static int systblAllBestIndex(sqlite3_vtab *tab,
                                  sqlite3_index_info *pIdxInfo) {
    return SQLITE_OK;
}

static int systblAllDisconnect(sqlite3_vtab *pVtab) {
    struct sqlite3_vtab_all *vt = (struct sqlite3_vtab_all*) pVtab;
    free(vt->select_sql);
    free(vt->zErrMsg);
    free(vt);
    return 0;
}

static int systblAllOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor) {
    struct sqlite3_vtab_cursor_all *c = calloc(1, sizeof(struct sqlite3_vtab_cursor_all));
    *ppCursor = (sqlite3_vtab_cursor*) c;

    const char *hosts[REPMAX];
    // net routines don't return our own node, so allocate one extra for that
    c->num_nodes = net_get_all_nodes_connected((netinfo_type*) thedb->handle_sibling, hosts) + 1;
    c->nodes = malloc(c->num_nodes * sizeof(char*));
    int nodenum;
    for (nodenum = 0; nodenum < c->num_nodes-1; nodenum++)
        c->nodes[nodenum] = strdup(hosts[nodenum]);
    c->nodes[nodenum] = strdup(gbl_myhostname);

    c->current_node = -1;
    c->current_db = NULL;
    c->eof = 0;
    return SQLITE_OK;
}

static int systblAllClose(sqlite3_vtab_cursor *cur) {
    struct sqlite3_vtab_cursor_all *c = calloc(1, sizeof(struct sqlite3_vtab_cursor_all));
    for (int i = 0; i < c->num_nodes; i++)
        free(c->nodes[i]);
    free(c->nodes);
    if (c->current_db) {
        cdb2_close(c->current_db);
        c->current_db = NULL;
    }
    free(c);
    return SQLITE_OK;
}

static int systblAllFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                               const char *idxStr, int argc,
                               sqlite3_value **argv) {
    return SQLITE_OK;
}

static int systblAllNext(sqlite3_vtab_cursor *cur) {
    struct sqlite3_vtab_cursor_all *c = (struct sqlite3_vtab_cursor_all *) cur;
    struct sqlite3_vtab_all *vt = (struct sqlite3_vtab_all*) cur->pVtab;
    c->rowid++;
    for (;;) {
        if (!c->current_db) {
            // if we've gone through all the nodes, we're done
            c->current_node++;
            if (c->current_node >= c->num_nodes)
                break;
            char *host = c->nodes[c->current_node];
            // If we're connecting to ourselves, use an admin connection.  This prevents a (really unlikely)
            // deadlock if all the threads are running the *_all query, and none can be allocated to run our local
            // subquery.
            int open_flags = CDB2_DIRECT_CPU;
            if (strcmp(c->nodes[c->current_node], gbl_myhostname) == 0) {
                open_flags |= CDB2_ADMIN;
            }
            int rc = cdb2_open(&c->current_db, thedb->envname, host, CDB2_DIRECT_CPU);
            if (rc) {
                // TODO: a node may be incoherent or busy, skip it
                cdb2_close(c->current_db);
                c->current_db = NULL;
                continue;
            }
            rc = cdb2_run_statement(c->current_db, vt->select_sql);
            if (rc) {
                cdb2_close(c->current_db);
                return SQLITE_ERROR;
            }
        }

        int rc = cdb2_next_record(c->current_db);
        if (rc == CDB2_OK)
            return SQLITE_OK;
        else if (rc == CDB2_OK_DONE) {
            cdb2_close(c->current_db);
            c->current_db = NULL;
            continue;
        } else {
            // signal error?
            return SQLITE_ERROR;
        }
    }
    c->eof = 1;
    return 0;
}

static int systblAllEof(sqlite3_vtab_cursor *cur) {
    struct sqlite3_vtab_cursor_all *c = (struct sqlite3_vtab_cursor_all*) cur;
    if (c->current_node == -1) {
        int rc = systblAllNext(cur);
        if (rc) {
            return rc;
        }
    }

    return c->eof;
}

static int systblAllColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
                               int pos) {
    struct sqlite3_vtab_cursor_all *c = (struct sqlite3_vtab_cursor_all*) cur;

    if (c->current_node == -1) {
        int rc = systblAllNext(cur);
        if (rc) {
            return rc;
        }
    }

    if (pos == 0) {
        sqlite3_result_text(ctx, c->nodes[c->current_node], -1, SQLITE_TRANSIENT);
    }
    else if (pos < 0 || pos >= cdb2_numcolumns(c->current_db)+1) {
        sqlite3_result_text(ctx, "out of range column?", -1, SQLITE_TRANSIENT);
        return SQLITE_MISUSE;
    }
    else if (cdb2_column_value(c->current_db, pos-1) == NULL) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    else {
        int type = cdb2_column_type(c->current_db, pos-1);
        if (type == CDB2_CSTRING) {
            sqlite3_result_text(ctx, (char *) cdb2_column_value(c->current_db, pos - 1), -1, SQLITE_TRANSIENT);
        }
        else if (type == CDB2_INTEGER) {
            sqlite3_result_int64(ctx, *(int64_t *) cdb2_column_value(c->current_db, pos - 1));
        }
        else if (type == CDB2_REAL) {
            sqlite3_result_double(ctx, *(double *) cdb2_column_value(c->current_db, pos - 1));
        }
        else if (type == CDB2_BLOB) {
            sqlite3_result_blob(ctx, cdb2_column_value(c->current_db, pos - 1), cdb2_column_size(c->current_db, pos - 1), SQLITE_TRANSIENT);
        }
        else if (type == CDB2_DATETIME) {
            char date[100];
            cdb2_client_datetime_t dt = *(cdb2_client_datetime_t*) cdb2_column_value(c->current_db, pos - 1);
            sprintf(date, "'%04d-%02d-%02dT%02d:%02d:%02d.%03d %s'", dt.tm.tm_year + 1900, dt.tm.tm_mon + 1, dt.tm.tm_mday,
                    dt.tm.tm_hour, dt.tm.tm_min, dt.tm.tm_sec, dt.msec, dt.tzname);
            sqlite3_result_text(ctx, date, -1, SQLITE_TRANSIENT);
        }
        else if (type == CDB2_INTERVALDS) {
            char d[100];
            cdb2_client_intv_ds_t it = *(cdb2_client_intv_ds_t*) cdb2_column_value(c->current_db, pos - 1);
            sprintf(d, "%s%d %d:%d:%d.%03d", it.sign ? "-" : "", it.days, it.hours, it.mins, it.sec, it.msec);
            sqlite3_result_text(ctx, d, -1, SQLITE_TRANSIENT);
        }
        else {
            sqlite3_result_text(ctx, "unhandled type", -1, SQLITE_TRANSIENT);
            return SQLITE_MISMATCH;
        }
    }

    return SQLITE_OK;
}

static int systblAllRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid) {
    struct sqlite3_vtab_cursor_all *c = (struct sqlite3_vtab_cursor_all*) cur;
    *pRowid = c->rowid;
    return 0;
}

static const sqlite3_module systblAllModule = {
        0,                       /* iVersion */
        0,                       /* xCreate */
        systblAllConnect,    /* xConnect */
        systblAllBestIndex,  /* xBestIndex */
        systblAllDisconnect, /* xDisconnect */
        0,                       /* xDestroy */
        systblAllOpen,       /* xOpen - open a cursor */
        systblAllClose,      /* xClose - close a cursor */
        systblAllFilter,     /* xFilter - configure scan constraints */
        systblAllNext,       /* xNext - advance a cursor */
        systblAllEof,        /* xEof - check for end of scan */
        systblAllColumn,     /* xColumn - read data */
        systblAllRowid,      /* xRowid - read data */
        0,                       /* xUpdate */
        0,                       /* xBegin */
        0,                       /* xSync */
        0,                       /* xCommit */
        0,                       /* xRollback */
        0,                       /* xFindMethod */
        0,                       /* xRename */
        0,                       /* xSavepoint */
        0,                       /* xRelease */
        0,                       /* xRollbackTo */
        0,                       /* xShadowName */
        .access_flag = CDB2_ALLOW_ALL,
};

int all_wrap(sqlite3 *db, const char *tablename) {
    char *name = malloc(strlen(tablename) + sizeof("_all"));
    strcpy(name, tablename);
    strcat(name, "_all");
    int rc =  sqlite3_create_module(db, name, &systblAllModule, strdup(tablename));
    free(name);
    return rc;
}

int systblAllInit(sqlite3 *db) {
    const char *alltables[] = {
            "comdb2_fdb_info",
            "comdb2_index_usage",
            "comdb2_tablesizes",
            "comdb2_temporary_file_sizes",
            "comdb2_fingerprints",
            "comdb2_sql_client_stats",
            "comdb2_threadpools",
            "comdb2_connections",
            "comdb2_cron_schedulers",
            "comdb2_clientstats",
            "comdb2_sqlpool_queue",
            "comdb2_tunables",
            "comdb2_repl_stats",
            "comdb2_replication_netqueue",
            "comdb2_metrics",
            "comdb2_blkseq",
            "comdb2_locks",
            "comdb2_active_osqls",
    };

    for (int i = 0; i < sizeof(alltables)/sizeof(alltables[0]); i++) {
        int rc = all_wrap(db, alltables[i]);
        if (rc)
            return SQLITE_ERROR;
    }
    return SQLITE_OK;
}
