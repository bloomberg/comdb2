/*
   Copyright 2017, 2018 Bloomberg Finance L.P.

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

#include <pthread.h>
#include <stdlib.h>

#include <comdb2_atomic.h>
#include <osqlsqlsocket.h>
#include <reqlog.h>
#include <sbuf2.h>
#include <sp.h>
#include <sql.h>
#include <sqloffload.h>
#include <str0.h>

#include <newsql.h>

void free_original_normalized_sql(struct sqlclntstate *);

extern int gbl_allow_incoherent_sql;
extern int gbl_disable_skip_rows;
extern int gbl_return_long_column_names;

struct newsql_appdata {
    NEWSQL_APPDATA_COMMON
};

static int newsql_clr_snapshot(struct sqlclntstate *);
static int newsql_has_high_availability(struct sqlclntstate *);

/*                (SERVER)                                                */
/*  Default --> (val: 1)                                                  */
/*                  |                                                     */
/*                  +--> Client has SKIP feature?                         */
/*                               |    |                                   */
/*                            NO |    | YES                               */
/*                               |    |                                   */
/*  SET INTRANSRESULTS OFF ------)--->+--> (val: 0) --+                   */
/*                               |                    |                   */
/*                               |  +-----------------+                   */
/*                               |  |                                     */
/*                               |  +---> Send server SKIP feature;       */
/*                               |        Don't send intrans results      */
/*                               |                                        */
/*  SET INTRANSRESULTS ON        +-------> (val: 1) --+                   */
/*            |                                       |                   */
/*            | (val: -1)           +-----------------+                   */
/*            |                     |                                     */
/*            +---------------------+--> Don't send server SKIP feature;  */
/*                                       Send intrans results             */
/*                                                                        */
/*                (CLIENT)                                                */
/*  CDB2_READ_INTRANS_RESULTS is ON?                                      */
/*                 /\                                                     */
/*   NO (default) /  \ YES                                                */
/*               /    \                                                   */
/*   Send Client       \                                                  */
/*   SKIP feature       \                                                 */
/*            /          \                                                */
/*   Server has           \                                               */
/*        SKIP feature?    \                                              */
/*         /          \     \                                             */
/*      Y /            \ N   \                                            */
/*       /              \     \                                           */
/*   Don't read         Read intrans results                              */
/*   intrans results    for writes                                        */
/*   for writes                                                           */
/*                                                                        */
/*  --                                                                    */
/*  Rivers                                                                */

static int fill_snapinfo(struct sqlclntstate *clnt, int *file, int *offset)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sql_query = appdata->sqlquery;
    int rcode = 0;
    if (sql_query && sql_query->snapshot_info &&
        sql_query->snapshot_info->file > 0) {
        *file = sql_query->snapshot_info->file;
        *offset = sql_query->snapshot_info->offset;

        sql_debug_logf(
            clnt, __func__, __LINE__,
            "fill-snapinfo "
            "sql_query->snapinfo is [%d][%d], clnt->snapinfo is [%d][%d]: "
            "use client snapinfo!\n",
            sql_query->snapshot_info->file, sql_query->snapshot_info->offset,
            clnt->snapshot_file, clnt->snapshot_offset);
        return 0;
    }

    if (*file == 0 && sql_query &&
        (in_client_trans(clnt) || clnt->is_hasql_retry) &&
        clnt->snapshot_file) {
        sql_debug_logf(
            clnt, __func__, __LINE__,
            "fill-snapinfo "
            "sql_query->snapinfo is [%d][%d] clnt->snapinfo is [%d][%d]\n",
            (sql_query && sql_query->snapshot_info)
                ? sql_query->snapshot_info->file
                : -1,
            (sql_query && sql_query->snapshot_info)
                ? sql_query->snapshot_info->offset
                : -1,
            clnt->snapshot_file, clnt->snapshot_offset);
        *file = clnt->snapshot_file;
        *offset = clnt->snapshot_offset;
        logmsg(LOGMSG_USER,
               "%s line %d setting newsql snapinfo retry info is [%d][%d]\n",
               __func__, __LINE__, *file, *offset);
        return 0;
    }

    if (*file == 0 && sql_query && clnt->ctrl_sqlengine == SQLENG_STRT_STATE) {

        int rc;
        uint32_t snapinfo_file, snapinfo_offset;

        if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DURABLE_LSNS)) {
            uint32_t durable_gen;

            rc = request_durable_lsn_from_master(
                thedb->bdb_env, &snapinfo_file, &snapinfo_offset, &durable_gen);
            if (rc == 0) {
                sql_debug_logf(
                    clnt, __func__, __LINE__,
                    "master returned "
                    "durable-lsn [%d][%d], clnt->is_hasql_retry=%d\n",
                    *file, *offset, clnt->is_hasql_retry);
            } else {
                sql_debug_logf(clnt, __func__, __LINE__,
                               "durable-lsn request "
                               "returns %d snapshot_file=%d snapshot_offset=%d "
                               "is_hasql_retry=%d\n",
                               rc, clnt->snapshot_file, clnt->snapshot_offset,
                               clnt->is_hasql_retry);
                rcode = -1;
            }
        } else {
            (void)bdb_get_current_lsn(thedb->bdb_env, &snapinfo_file,
                                      &snapinfo_offset);
            rc = 0;
            sql_debug_logf(clnt, __func__, __LINE__,
                           "durable-lsn is disabled. Use my LSN [%d][%d], "
                           "clnt->is_hasql_retry=%d\n",
                           *file, *offset, clnt->is_hasql_retry);
        }

        if (rc == 0) {
            *file = snapinfo_file;
            *offset = snapinfo_offset;
        } else {
            rcode = -1;
        }
        return rcode;
    }

    if (*file == 0) {
        bdb_tran_get_start_file_offset(thedb->bdb_env, clnt->dbtran.shadow_tran,
                                       file, offset);
        sql_debug_logf(clnt, __func__, __LINE__,
                       "start_file_offset snapinfo "
                       "is [%d][%d], sqlengine-state is %d\n",
                       *file, *offset, clnt->ctrl_sqlengine);
    }
    return rcode;
}

static struct query_effects *newsql_get_query_effects(struct sqlclntstate *clnt)
{
    if (clnt->dbtran.nchunks == 0)
        return &clnt->effects;
    if (clnt->dbtran.crtchunksize > 0)
        return &clnt->log_effects;
    clnt->chunk_effects.num_selected = clnt->log_effects.num_selected;
    return &clnt->chunk_effects;
}

#define _has_effects(clnt, sql_response)                                       \
    CDB2EFFECTS effects = CDB2__EFFECTS__INIT;                                 \
    struct query_effects *ep = newsql_get_query_effects(clnt);                 \
    ep->num_affected = ep->num_updated + ep->num_deleted + ep->num_inserted;   \
    effects.num_affected = ep->num_affected;                                   \
    effects.num_selected = ep->num_selected;                                   \
    effects.num_updated = ep->num_updated;                                     \
    effects.num_deleted = ep->num_deleted;                                     \
    effects.num_inserted = ep->num_inserted;                                   \
                                                                               \
    sql_response.effects = &effects;

#define _has_features(clnt, sql_response)                                      \
    CDB2ServerFeatures features[10];                                           \
    int n_features = 0;                                                        \
    struct newsql_appdata *appdata = clnt->appdata;                            \
    if (appdata->send_intrans_response == 0) {                                 \
        features[n_features] = CDB2_SERVER_FEATURES__SKIP_INTRANS_RESULTS;     \
        n_features++;                                                          \
    }                                                                          \
                                                                               \
    if (n_features) {                                                          \
        sql_response.n_features = n_features;                                  \
        sql_response.features = features;                                      \
    }

#define _has_snapshot(clnt, sql_response)                                      \
    CDB2SQLRESPONSE__Snapshotinfo snapshotinfo =                               \
        CDB2__SQLRESPONSE__SNAPSHOTINFO__INIT;                                 \
                                                                               \
    if (newsql_has_high_availability(clnt)) {                                  \
        int file = 0, offset = 0;                                              \
        if (fill_snapinfo(clnt, &file, &offset)) {                             \
            sql_response.error_code = (char)CDB2ERR_CHANGENODE;                \
        }                                                                      \
        if (file) {                                                            \
            snapshotinfo.file = file;                                          \
            snapshotinfo.offset = offset;                                      \
            sql_response.snapshot_info = &snapshotinfo;                        \
        }                                                                      \
    }

/* Skip spaces and tabs, requires at least one space */
static inline char *skipws(char *str)
{
    if (str) {
        while (*str && isspace(*str))
            str++;
    }
    return str;
}

int gbl_abort_on_unset_ha_flag = 0;
static int is_snap_uid_retry(struct sqlclntstate *clnt)
{
    // Retries happen with a 'begin'.  This can't be a retry if we are already
    // in a transaction
    if (clnt->ctrl_sqlengine != SQLENG_NORMAL_PROCESS) {
        return 0;
    }

    // Need to clear snapshot info here: we are not in a transaction.  This code
    // makes sure that snapshot_file is cleared.
    newsql_clr_snapshot(clnt);

    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery;
    if (sqlquery->retry == 0) {
        // Returning 0 because the retry flag is not lit
        return 0;
    }

    if (newsql_has_high_availability(clnt) == 0) {
        if (gbl_abort_on_unset_ha_flag) {
            // We shouldn't be here - try to understand why
            fflush(stdout);
            fflush(stderr);
            abort();
        }
    }

    /**
     * If this is a retry, then:
     *
     * 1) this should be a BEGIN
     * 2) the retry flag should be set (we only set retry flag on a begin)
     * 3) we *could* have a valid snapshot_file and snapshot_offset
     *
     * 3 is the most difficult, as it looks like we don't actually know the
     * durable lsn until the first statement sent after the begin.  This is
     * okay, but to make this work we just need to be extremely careful and
     * only send back the snapshot_file and snapshot_offset at the correct
     * time
     **/

    /* Retry case has flag lit on "begin" */
    if (sqlquery->snapshot_info && sqlquery->snapshot_info->file &&
        strncasecmp(clnt->sql, "begin", 5) == 0) {
        clnt->snapshot_file = sqlquery->snapshot_info->file;
        clnt->snapshot_offset = sqlquery->snapshot_info->offset;
        clnt->is_hasql_retry = 1;
    }
    // XXX short circuiting the last-row optimization until i have a chance
    // to verify it.
    return 0;
}

static inline int newsql_to_client_type(int newsql_type)
{
    switch (newsql_type) {
    case CDB2_INTEGER:
        return CLIENT_INT;
    case CDB2_REAL:
        return CLIENT_REAL;
    case CDB2_CSTRING:
        return CLIENT_CSTR;
    case CDB2_BLOB:
        return CLIENT_BLOB;
    case CDB2_DATETIME:
        return CLIENT_DATETIME;
    case CDB2_DATETIMEUS:
        return CLIENT_DATETIMEUS;
    case CDB2_INTERVALYM:
        return CLIENT_INTVYM;
    case CDB2_INTERVALDS:
        return CLIENT_INTVDS;
    case CDB2_INTERVALDSUS:
        return CLIENT_INTVDSUS;
    default:
        return -1;
    }
}
static int newsql_response_int(struct sqlclntstate *clnt, const CDB2SQLRESPONSE *r, int h, int flush)
{
    struct newsql_appdata *appdata = clnt->appdata;
    clnt->lastresptype = r->response_type;
    return appdata->write(clnt, h, 0, r, flush);
}

static int newsql_response(struct sqlclntstate *c, const CDB2SQLRESPONSE *r, int flush)
{
    return newsql_response_int(c, r, RESPONSE_HEADER__SQL_RESPONSE, flush);
}

static int newsql_send_hdr(struct sqlclntstate *clnt, int h, int s)
{
    struct newsql_appdata *appdata = clnt->appdata;
    return appdata->write_hdr(clnt, h, s);
}

static int get_col_type(struct sqlclntstate *clnt, sqlite3_stmt *stmt, int col)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sql_query = appdata->sqlquery;
    int type = -1;
    if (sql_query->n_types) {
        type = sql_query->types[col];
        if (type == SQLITE_DECIMAL) {
            type = SQLITE_TEXT;
        }
    } else if (stmt) {
        type = get_sqlite3_column_type(clnt, stmt, col, 0);
    }
    return type;
}

#define MAX_COL_NAME_LEN 31
#define ADJUST_LONG_COL_NAME(n, l)                                             \
    do {                                                                       \
        if (!gbl_return_long_column_names && l > MAX_COL_NAME_LEN) {           \
            l = MAX_COL_NAME_LEN + 1;                                          \
            char *namebuf = alloca(l);                                         \
            n = strncpy0(namebuf, n, l);                                       \
        }                                                                      \
    } while (0)

static int newsql_columns(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    int ncols = column_count(clnt, stmt);
    struct newsql_appdata *appdata = clnt->appdata;
    update_col_info(&appdata->col_info, ncols);
    CDB2SQLRESPONSE__Column cols[ncols];
    CDB2SQLRESPONSE__Column *value[ncols];
    for (int i = 0; i < ncols; ++i) {
        value[i] = &cols[i];
        cdb2__sqlresponse__column__init(&cols[i]);
        const char *name = sqlite3_column_name(stmt, i);
        size_t len = strlen(name) + 1;
        ADJUST_LONG_COL_NAME(name, len);
        cols[i].value.data = (uint8_t *)name;
        cols[i].value.len = len;
        cols[i].has_type = 1;
        cols[i].type = appdata->col_info.type[i] = get_col_type(clnt, stmt, i);
    }
    CDB2SQLRESPONSE resp = CDB2__SQLRESPONSE__INIT;
    resp.response_type = RESPONSE_TYPE__COLUMN_NAMES;
    resp.n_value = ncols;
    resp.value = value;
    if (clnt->sqlite_row_format)
        resp.has_sqlite_row = 1;

    if (clnt->request_fp) {
        /* client has requested the fingerprint. if the fingerprint is already
           available in clnt, use it. otherwise compute its value and store
           in clnt. */
        uint8_t empty[FINGERPRINTSZ] = {0};
        if (memcmp(clnt->work.aFingerprint, empty, FINGERPRINTSZ) == 0) {
            size_t nNormSql = 0;
            calc_fingerprint(clnt->work.zNormSql, &nNormSql,
                    clnt->work.aFingerprint);
        }
        resp.has_fp = 1;
        resp.fp.data = clnt->work.aFingerprint;
        resp.fp.len = FINGERPRINTSZ;
    }
    resp.has_flat_col_vals = 1;
    return newsql_response(clnt, &resp, 0);
}

/*
** Derive types from cdb2_run_statement_typed, or defined in sp, or
** from sql statement.
*/
static int newsql_columns_lua(struct sqlclntstate *clnt,
                              struct response_data *arg)
{
    int ncols = arg->ncols;
    sqlite3_stmt *stmt = arg->stmt;
    if (stmt && column_count(clnt, stmt) != ncols) {
        return -1;
    }
    struct newsql_appdata *appdata = clnt->appdata;
    update_col_info(&appdata->col_info, ncols);
    size_t n_types = appdata->sqlquery->n_types;
    if (n_types && n_types != ncols) {
        return -2;
    }
    CDB2SQLRESPONSE__Column cols[ncols];
    CDB2SQLRESPONSE__Column *value[ncols];
    for (int i = 0; i < ncols; ++i) {
        value[i] = &cols[i];
        cdb2__sqlresponse__column__init(&cols[i]);
        const char *name = sp_column_name(arg, i);
        size_t len = strlen(name) + 1;
        ADJUST_LONG_COL_NAME(name, len);
        cols[i].value.data = (uint8_t *)name;
        cols[i].value.len = len;
        cols[i].has_type = 1;
        cols[i].type = appdata->col_info.type[i] =
            sp_column_type(arg, i, n_types, get_col_type(clnt, stmt, i));
    }
    clnt->osql.sent_column_data = 1;
    CDB2SQLRESPONSE resp = CDB2__SQLRESPONSE__INIT;
    resp.response_type = RESPONSE_TYPE__COLUMN_NAMES;
    resp.n_value = ncols;
    resp.value = value;
    return newsql_response(clnt, &resp, 0);
}

static int newsql_columns_str(struct sqlclntstate *clnt, char **names,
                              int ncols)
{
    struct newsql_appdata *appdata = clnt->appdata;
    update_col_info(&appdata->col_info, ncols);
    CDB2SQLRESPONSE__Column cols[ncols];
    CDB2SQLRESPONSE__Column *value[ncols];
    for (int i = 0; i < ncols; ++i) {
        value[i] = &cols[i];
        cdb2__sqlresponse__column__init(&cols[i]);
        const char *name = names[i];
        cols[i].value.data = (uint8_t *)name;
        cols[i].value.len = strlen(name) + 1;
        cols[i].has_type = 1;
        cols[i].type = appdata->col_info.type[i] = SQLITE_TEXT;
    }
    clnt->osql.sent_column_data = 1;
    CDB2SQLRESPONSE resp = CDB2__SQLRESPONSE__INIT;
    resp.response_type = RESPONSE_TYPE__COLUMN_NAMES;
    resp.n_value = ncols;
    resp.value = value;
    return newsql_response(clnt, &resp, 0);
}

int newsql_columns_fdb_push(struct sqlclntstate *clnt, cdb2_hndl_tp *hndl,
                            int ncols)
{
    struct newsql_appdata *appdata = clnt->appdata;
    update_col_info(&appdata->col_info, ncols);
    CDB2SQLRESPONSE__Column cols[ncols];
    CDB2SQLRESPONSE__Column *value[ncols];
    for (int i = 0; i < ncols; ++i) {
        value[i] = &cols[i];
        cdb2__sqlresponse__column__init(&cols[i]);
        const char *name = cdb2_column_name(hndl, i);
        cols[i].value.data = (uint8_t *)name;
        cols[i].value.len = strlen(name) + 1;
        cols[i].has_type = 1;
        cols[i].type = appdata->col_info.type[i] = cdb2_column_type(hndl, i);
    }
    clnt->osql.sent_column_data = 1;
    CDB2SQLRESPONSE resp = CDB2__SQLRESPONSE__INIT;
    resp.response_type = RESPONSE_TYPE__COLUMN_NAMES;
    resp.n_value = ncols;
    resp.value = value;
    return newsql_response(clnt, &resp, 0);
}

static int newsql_debug(struct sqlclntstate *c, char *info)
{
    CDB2SQLRESPONSE r = CDB2__SQLRESPONSE__INIT;
    r.response_type = RESPONSE_TYPE__SP_DEBUG;
    r.info_string = info;
    return newsql_response_int(c, &r, RESPONSE_HEADER__SQL_RESPONSE_TRACE, 1);
}

static int newsql_error(struct sqlclntstate *c, char *r, int e)
{
    CDB2SQLRESPONSE resp = CDB2__SQLRESPONSE__INIT;
    resp.error_code = e;
    resp.error_string = r;
    resp.response_type = c->osql.sent_column_data ? RESPONSE_TYPE__COLUMN_VALUES
                                                  : RESPONSE_TYPE__COLUMN_NAMES;
    return newsql_response(c, &resp, 1);
}

static int newsql_save_postponed_row(struct sqlclntstate *clnt,
                                     CDB2SQLRESPONSE *resp)
{
    size_t len = cdb2__sqlresponse__get_packed_size(resp);
    struct newsql_appdata *appdata = clnt->appdata;
    if (appdata->postponed == NULL) {
        appdata->postponed = calloc(1, sizeof(struct newsql_postponed_data));
        appdata->postponed->hdr.type = ntohl(RESPONSE_HEADER__SQL_RESPONSE);
    }
    appdata->postponed->len = len;
    appdata->postponed->hdr.length = htonl(len);
    appdata->postponed->row = realloc(appdata->postponed->row, len);
    cdb2__sqlresponse__pack(resp, appdata->postponed->row);
    return 0;
}

static int newsql_send_postponed_row(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    return appdata->write_postponed(clnt);
}

#define newsql_null(cols, i)                                                   \
    do {                                                                       \
        cols[i].has_isnull = 1;                                                \
        cols[i].isnull = 1;                                                    \
    } while (0)

#define newsql_integer(cols, i, val, flip)                                     \
    do {                                                                       \
        int64_t *pi64 = alloca(sizeof(int64_t));                               \
        *pi64 = flip ? flibc_llflip(val) : val;                                \
        cols[i].value.len = sizeof(int64_t);                                   \
        cols[i].value.data = (uint8_t *)pi64;                                  \
    } while (0)

#define newsql_double(cols, i, val, flip)                                      \
    do {                                                                       \
        double *pd = alloca(sizeof(double));                                   \
        *pd = flip ? flibc_dblflip(val) : val;                                 \
        cols[i].value.len = sizeof(double);                                    \
        cols[i].value.data = (uint8_t *)pd;                                    \
    } while (0)

#define newsql_ym(cols, i, val, flip)                                          \
    do {                                                                       \
        cdb2_client_intv_ym_t *c = alloca(sizeof(cdb2_client_intv_ym_t));      \
        if (flip) {                                                            \
            c->sign = flibc_intflip(val->sign);                                \
            c->years = flibc_intflip(val->u.ym.years);                         \
            c->months = flibc_intflip(val->u.ym.months);                       \
        } else {                                                               \
            c->sign = val->sign;                                               \
            c->years = val->u.ym.years;                                        \
            c->months = val->u.ym.months;                                      \
        }                                                                      \
        cols[i].value.len = sizeof(*c);                                        \
        cols[i].value.data = (uint8_t *)c;                                     \
    } while (0)

#define newsql_ds(cols, i, val, flip)                                          \
    do {                                                                       \
        int frac = val->u.ds.frac;                                             \
        if (type == SQLITE_INTERVAL_DS && val->u.ds.prec == 6) {               \
            frac /= 1000;                                                      \
        } else if (type == SQLITE_INTERVAL_DSUS && val->u.ds.prec == 3) {      \
            frac *= 1000;                                                      \
        }                                                                      \
        cdb2_client_intv_ds_t *c = alloca(sizeof(cdb2_client_intv_ds_t));      \
        if (flip) {                                                            \
            c->sign = flibc_intflip(val->sign);                                \
            c->days = flibc_intflip(val->u.ds.days);                           \
            c->hours = flibc_intflip(val->u.ds.hours);                         \
            c->mins = flibc_intflip(val->u.ds.mins);                           \
            c->sec = flibc_intflip(val->u.ds.sec);                             \
            c->msec = flibc_intflip(frac);                                     \
        } else {                                                               \
            c->sign = val->sign;                                               \
            c->days = val->u.ds.days;                                          \
            c->hours = val->u.ds.hours;                                        \
            c->mins = val->u.ds.mins;                                          \
            c->sec = val->u.ds.sec;                                            \
            c->msec = frac;                                                    \
        }                                                                      \
        cols[i].value.len = sizeof(*c);                                        \
        cols[i].value.data = (uint8_t *)c;                                     \
    } while (0)

#ifdef _SUN_SOURCE
#include <arpa/nameser_compat.h>
#endif
#ifndef BYTE_ORDER
#error "Missing BYTE_ORDER"
#endif

static int newsql_row(struct sqlclntstate *clnt, struct response_data *arg,
                      int postpone)
{
    sqlite3_stmt *stmt = arg->stmt;
    if (!clnt->fdb_push && stmt == NULL) {
        return newsql_send_postponed_row(clnt);
    }
    int ncols = column_count(clnt, stmt);
    struct newsql_appdata *appdata = clnt->appdata;
    update_col_info(&appdata->col_info, ncols);
    assert(ncols == appdata->col_info.count);
    int flip = 0;
#if BYTE_ORDER == BIG_ENDIAN
    if (appdata->sqlquery->little_endian)
#elif BYTE_ORDER == LITTLE_ENDIAN
    if (!appdata->sqlquery->little_endian)
#endif
        flip = 1;

    /* nested column values */
    CDB2SQLRESPONSE__Column cols[ncols];
    CDB2SQLRESPONSE__Column *value[ncols];

    /* flat column values */
    ProtobufCBinaryData bd[ncols];
    protobuf_c_boolean isnulls[ncols];

    memset(&bd, 0, sizeof(ProtobufCBinaryData) * ncols);
    memset(&isnulls, 0, sizeof(protobuf_c_boolean) * ncols);

    for (int i = 0; i < ncols; ++i) {
        if (!clnt->flat_col_vals)
            value[i] = &cols[i];
        cdb2__sqlresponse__column__init(&cols[i]);
        if (is_column_type_null(clnt, stmt, i)) {
            newsql_null(cols, i);
            if (clnt->flat_col_vals)
                isnulls[i] = cols[i].has_isnull ? cols[i].isnull : 0;
            continue;
        }
        int type = appdata->col_info.type[i];
        switch (type) {
        case SQLITE_INTEGER: {
            int64_t i64 = column_int64(clnt, stmt, i);
            newsql_integer(cols, i, i64, flip);
            break;
        }
        case SQLITE_FLOAT: {
            double d = column_double(clnt, stmt, i);
            newsql_double(cols, i, d, flip);
            break;
        }
        case SQLITE_TEXT: {
            cols[i].value.len = column_bytes(clnt, stmt, i) + 1;
            cols[i].value.data = (uint8_t *)column_text(clnt, stmt, i);
            break;
        }
        case SQLITE_BLOB: {
            cols[i].value.len = column_bytes(clnt, stmt, i);
            cols[i].value.data = (uint8_t *)column_blob(clnt, stmt, i);
            break;
        }
        case SQLITE_DATETIME:
        case SQLITE_DATETIMEUS: {
            const dttz_t *d = column_datetime(clnt, stmt, i);
            cdb2_client_datetime_t *c = alloca(sizeof(*c));
            if (convDttz2ClientDatetime(d, clnt_tzname(clnt, stmt), c, type) !=
                0) {
                char *e =
                    "failed to convert sqlite to client datetime for field";
                errstat_set_rcstrf(arg->err, ERR_CONVERSION_DT, "%s \"%s\"", e,
                                   sqlite3_column_name(stmt, i));
                return -1;
            }
            if (flip) {
                c->msec = flibc_intflip(c->msec);
                c->tm.tm_sec = flibc_intflip(c->tm.tm_sec);
                c->tm.tm_min = flibc_intflip(c->tm.tm_min);
                c->tm.tm_hour = flibc_intflip(c->tm.tm_hour);
                c->tm.tm_mday = flibc_intflip(c->tm.tm_mday);
                c->tm.tm_mon = flibc_intflip(c->tm.tm_mon);
                c->tm.tm_year = flibc_intflip(c->tm.tm_year);
                c->tm.tm_wday = flibc_intflip(c->tm.tm_wday);
                c->tm.tm_yday = flibc_intflip(c->tm.tm_yday);
                c->tm.tm_isdst = flibc_intflip(c->tm.tm_isdst);
            }
            cols[i].value.len = sizeof(*c);
            cols[i].value.data = (uint8_t *)c;
            break;
        }
        case SQLITE_INTERVAL_YM: {
            const intv_t *val =
                column_interval(clnt, stmt, i, SQLITE_AFF_INTV_MO);
            newsql_ym(cols, i, val, flip);
            break;
        }
        case SQLITE_INTERVAL_DS:
        case SQLITE_INTERVAL_DSUS: {
            const intv_t *val =
                column_interval(clnt, stmt, i, SQLITE_AFF_INTV_SE);
            newsql_ds(cols, i, val, flip);
            break;
        }
        case SQLITE_DECIMAL:
        default:
            return -1;
        }

        if (clnt->flat_col_vals)
            bd[i] = cols[i].value;
    }
    CDB2SQLRESPONSE r = CDB2__SQLRESPONSE__INIT;
    r.response_type = RESPONSE_TYPE__COLUMN_VALUES;
    if (clnt->flat_col_vals) {
        r.has_flat_col_vals = 1;
        r.flat_col_vals = 1;
        r.n_values = r.n_isnulls = ncols;
        r.values = bd;
        r.isnulls = isnulls;
    } else {
        r.n_value = ncols;
        r.value = value;
    }
    if (clnt->num_retry) {
        r.has_row_id = 1;
        r.row_id = arg->row_id;
    }

    if (postpone) {
        return newsql_save_postponed_row(clnt, &r);
    } else if (arg->pingpong) {
        return newsql_response_int(clnt, &r, RESPONSE_HEADER__SQL_RESPONSE_PING, 1);
    }
    return newsql_response(clnt, &r, !clnt->rowbuffer);
}

static int newsql_row_last(struct sqlclntstate *clnt)
{
    CDB2SQLRESPONSE resp = CDB2__SQLRESPONSE__INIT;
    resp.response_type = RESPONSE_TYPE__LAST_ROW;
    _has_effects(clnt, resp);
    _has_snapshot(clnt, resp);
    _has_features(clnt, resp);
    return newsql_response(clnt, &resp, 1);
}

static int newsql_row_last_dummy(struct sqlclntstate *clnt)
{
    int rc;
    CDB2SQLRESPONSE resp = CDB2__SQLRESPONSE__INIT;
    resp.response_type = RESPONSE_TYPE__COLUMN_NAMES;
    if ((rc = newsql_response(clnt, &resp, 0)) != 0) {
        return rc;
    }
    return newsql_row_last(clnt);
}

static int newsql_row_lua(struct sqlclntstate *clnt, struct response_data *arg)
{
    struct newsql_appdata *appdata = clnt->appdata;
    int ncols = arg->ncols;
    assert(ncols == appdata->col_info.count);
    int flip = 0;
#if BYTE_ORDER == BIG_ENDIAN
    if (appdata->sqlquery->little_endian)
#elif BYTE_ORDER == LITTLE_ENDIAN
    if (!appdata->sqlquery->little_endian)
#endif
        flip = 1;
    CDB2SQLRESPONSE__Column cols[ncols];
    CDB2SQLRESPONSE__Column *value[ncols];
    for (int i = 0; i < ncols; ++i) {
        value[i] = &cols[i];
        cdb2__sqlresponse__column__init(&cols[i]);
        if (sp_column_nil(arg, i)) {
            newsql_null(cols, i);
            continue;
        }
        int type = appdata->col_info.type[i];
        switch (type) {
        case SQLITE_INTEGER: {
            int64_t i64;
            if (sp_column_val(arg, i, type, &i64)) {
                return -1;
            }
            newsql_integer(cols, i, i64, flip);
            break;
        }
        case SQLITE_FLOAT: {
            double d;
            if (sp_column_val(arg, i, type, &d)) {
                return -1;
            }
            newsql_double(cols, i, d, flip);
            break;
        }
        case SQLITE_TEXT: {
            size_t l;
            if ((cols[i].value.data = sp_column_ptr(arg, i, type, &l)) == NULL) {
                return -1;
            }
            cols[i].value.len = l + 1;
            break;
        }
        case SQLITE_BLOB: {
            size_t l;
            if ((cols[i].value.data = sp_column_ptr(arg, i, type, &l)) == NULL) {
                return -1;
            }
            cols[i].value.len = l;
            break;
        }
        case SQLITE_DATETIME:
        case SQLITE_DATETIMEUS: {
            datetime_t d;
            if (sp_column_val(arg, i, type, &d)) {
                return -1;
            }
            if (d.prec == DTTZ_PREC_MSEC && type == SQLITE_DATETIMEUS)
                d.frac *= 1000;
            else if (d.prec == DTTZ_PREC_USEC && type == SQLITE_DATETIME)
                d.frac /= 1000;
            cdb2_client_datetime_t *c = alloca(sizeof(*c));
            strcpy(c->tzname, d.tzname);
            if (flip) {
                c->msec = flibc_intflip(d.frac);
                c->tm.tm_sec = flibc_intflip(d.tm.tm_sec);
                c->tm.tm_min = flibc_intflip(d.tm.tm_min);
                c->tm.tm_hour = flibc_intflip(d.tm.tm_hour);
                c->tm.tm_mday = flibc_intflip(d.tm.tm_mday);
                c->tm.tm_mon = flibc_intflip(d.tm.tm_mon);
                c->tm.tm_year = flibc_intflip(d.tm.tm_year);
                c->tm.tm_wday = flibc_intflip(d.tm.tm_wday);
                c->tm.tm_yday = flibc_intflip(d.tm.tm_yday);
                c->tm.tm_isdst = flibc_intflip(d.tm.tm_isdst);
            } else {
                c->msec = d.frac;
                c->tm.tm_sec = d.tm.tm_sec;
                c->tm.tm_min = d.tm.tm_min;
                c->tm.tm_hour = d.tm.tm_hour;
                c->tm.tm_mday = d.tm.tm_mday;
                c->tm.tm_mon = d.tm.tm_mon;
                c->tm.tm_year = d.tm.tm_year;
                c->tm.tm_wday = d.tm.tm_wday;
                c->tm.tm_yday = d.tm.tm_yday;
                c->tm.tm_isdst = d.tm.tm_isdst;
            }
            cols[i].value.len = sizeof(*c);
            cols[i].value.data = (uint8_t *)c;
            break;
        }
        case SQLITE_INTERVAL_YM: {
            intv_t in, *val = &in;
            if (sp_column_val(arg, i, type, val)) {
                return -1;
            }
            newsql_ym(cols, i, val, flip);
            break;
        }
        case SQLITE_INTERVAL_DS:
        case SQLITE_INTERVAL_DSUS: {
            intv_t in, *val = &in;
            if (sp_column_val(arg, i, type, &in)) {
                return -1;
            }
            newsql_ds(cols, i, val, flip);
            break;
        }
        default:
            return -1;
        }
    }
    CDB2SQLRESPONSE r = CDB2__SQLRESPONSE__INIT;
    r.response_type = RESPONSE_TYPE__COLUMN_VALUES;
    r.n_value = ncols;
    r.value = value;
    if (arg->pingpong) {
        return newsql_response_int(clnt, &r, RESPONSE_HEADER__SQL_RESPONSE_PING, 1);
    }
    return newsql_response(clnt, &r, 0);
}

static int newsql_row_str(struct sqlclntstate *clnt, char **data, int ncols)
{
    struct newsql_appdata *appdata = clnt->appdata;
    UNUSED_PARAMETER(appdata); /* prod build without assert */
    assert(ncols == appdata->col_info.count);
    CDB2SQLRESPONSE__Column cols[ncols];
    CDB2SQLRESPONSE__Column *value[ncols];
    for (int i = 0; i < ncols; ++i) {
        value[i] = &cols[i];
        cdb2__sqlresponse__column__init(&cols[i]);
        if (data[i] == NULL) {
            cols[i].has_isnull = 1;
            cols[i].isnull = 1;
            continue;
        }
        cols[i].value.data = (uint8_t *)data[i];
        cols[i].value.len = strlen(data[i]) + 1;
    }
    CDB2SQLRESPONSE resp = CDB2__SQLRESPONSE__INIT;
    resp.response_type = RESPONSE_TYPE__COLUMN_VALUES;
    resp.n_value = ncols;
    resp.value = value;
    return newsql_response(clnt, &resp, 0);
}

static int newsql_row_sqlite(struct sqlclntstate *clnt,
                             struct response_data *arg, int postpone)
{
    Mem res;
    int rc;

    assert(postpone == 0); /* read only */

    CDB2SQLRESPONSE r = CDB2__SQLRESPONSE__INIT;
    r.response_type = RESPONSE_TYPE__SQL_ROW;
    r.has_sqlite_row = 1;

    bzero(&res, sizeof(res));
    fdb_sqlite_row(arg->stmt, &res);

    r.sqlite_row.data = (uint8_t *)res.z;
    r.sqlite_row.len = res.n;

    rc = newsql_response(clnt, &r, 1);

    fdb_sqlite_row_free(&res);

    return rc;
}

static int newsql_trace(struct sqlclntstate *clnt, char *info)
{
    CDB2SQLRESPONSE r = CDB2__SQLRESPONSE__INIT;
    r.response_type = RESPONSE_TYPE__SP_TRACE;
    r.info_string = info;
    return newsql_response_int(clnt, &r, RESPONSE_HEADER__SQL_RESPONSE_TRACE, 1);
}

static int newsql_cost(struct sqlclntstate *clnt)
{
    dump_client_query_stats(clnt->dbglog, clnt->query_stats);
    return 0;
}

static int newsql_write_response(struct sqlclntstate *c, int t, void *a, int i)
{
    switch (t) {
    case RESPONSE_COLUMNS: return newsql_columns(c, a);
    case RESPONSE_COLUMNS_LUA: return newsql_columns_lua(c, a);
    case RESPONSE_COLUMNS_STR: return newsql_columns_str(c, a, i);
    case RESPONSE_COLUMNS_FDB_PUSH:
        return newsql_columns_fdb_push(c, a, i);
    case RESPONSE_DEBUG: return newsql_debug(c, a);
    case RESPONSE_ERROR: return newsql_error(c, a, i);
    case RESPONSE_ERROR_ACCESS: return newsql_error(c, a, CDB2ERR_ACCESS);
    case RESPONSE_ERROR_BAD_STATE: return newsql_error(c, a, CDB2ERR_BADSTATE);
    case RESPONSE_ERROR_PREPARE: return newsql_error(c, a, CDB2ERR_PREPARE_ERROR);
    case RESPONSE_ERROR_REJECT: return newsql_error(c, a, CDB2ERR_REJECTED);
    case RESPONSE_FLUSH: return c->plugin.flush(c);
    case RESPONSE_HEARTBEAT: return newsql_heartbeat(c);
    case RESPONSE_ROW:
        return c->sqlite_row_format ? newsql_row_sqlite(c, a, i)
                                    : newsql_row(c, a, i);
    case RESPONSE_ROW_LAST: return newsql_row_last(c);
    case RESPONSE_ROW_LAST_DUMMY: return newsql_row_last_dummy(c);
    case RESPONSE_ROW_LUA: return newsql_row_lua(c, a);
    case RESPONSE_ROW_STR: return newsql_row_str(c, a, i);
    case RESPONSE_TRACE: return newsql_trace(c, a);
    case RESPONSE_COST: return newsql_cost(c);
    /* fastsql only messages */
    case RESPONSE_EFFECTS:
    case RESPONSE_ERROR_PREPARE_RETRY:
    case RESPONSE_QUERY_STATS:
        return 0;
    default:
        abort();
    }
}

static int newsql_ping_pong(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    return appdata->ping_pong(clnt); /* newsql_ping_pong_evbuffer */
}

static int newsql_sp_cmd(struct sqlclntstate *clnt, void *cmd, size_t sz)
{
    struct newsqlheader hdr = {0};
    if (read_response(clnt, RESPONSE_BYTES, &hdr, sizeof(hdr)) != 1) {
        return -1;
    }
    if (ntohl(hdr.type) != CDB2_REQUEST_TYPE__CDB2QUERY) {
        return -2;
    }
    int len = ntohl(hdr.length);
    if (len > sz) {
        return -3;
    }
    uint8_t buf[len];
    if (read_response(clnt, RESPONSE_BYTES, buf, len) != 1) {
        return -4;
    }
    CDB2QUERY *query = cdb2__query__unpack(NULL, len, buf);
    if (!query) {
        return -5;
    }
    strncpy0(cmd, query->spcmd, sz);
    cdb2__query__free_unpacked(query, NULL);
    return 0;
}

static int newsql_read_response(struct sqlclntstate *c, int t, void *r, int e)
{
    struct newsql_appdata *appdata = c->appdata;
    switch (t) {
    case RESPONSE_PING_PONG: return newsql_ping_pong(c);
    case RESPONSE_SP_CMD: return newsql_sp_cmd(c, r, e);
    case RESPONSE_BYTES: return appdata->read(c, r, e, 1);
    default: abort();
    }
}

static void *newsql_save_stmt(struct sqlclntstate *clnt, void *arg)
{
    struct newsql_appdata *appdata = clnt->appdata;
    struct newsql_stmt *stmt = malloc(sizeof(struct newsql_stmt));
    stmt->query = appdata->query;
    appdata->query = NULL;
    strncpy0(stmt->tzname, clnt->tzname, sizeof(stmt->tzname));
    return stmt;
}

static void *newsql_restore_stmt(struct sqlclntstate *clnt, void *arg)
{
    struct newsql_stmt *stmt = arg;
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2QUERY *query = appdata->query = stmt->query;
    appdata->sqlquery = query->sqlquery;
    strncpy0(clnt->tzname, stmt->tzname, sizeof(clnt->tzname));
    Pthread_mutex_lock(&clnt->sql_lk);
    clnt->sql = query->sqlquery->sql_query;
    Pthread_mutex_unlock(&clnt->sql_lk);
    return NULL;
}

static void *newsql_destroy_stmt(struct sqlclntstate *clnt, void *arg)
{
    abort();
    return NULL;
}

static void *newsql_print_stmt(struct sqlclntstate *clnt, void *arg)
{
    struct newsql_stmt *stmt = arg;
    return stmt->query->sqlquery->sql_query;
}

static int newsql_param_count(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    return appdata->sqlquery->n_bindvars;
}

static int newsql_param_index(struct sqlclntstate *clnt, const char *name,
                              int64_t *index)
{
    /*
    ** Currently implemented like sqlite3_bind_parameter_index()
    ** Can be done better with qsort + bsearch
    */
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery;
    size_t n = sqlquery->n_bindvars;
    for (size_t i = 0; i < n; ++i) {
        if (strcmp(sqlquery->bindvars[i]->varname, name) == 0) {
            *index = i;
            return 0;
        }
    }
    return -1;
}

static int newsql_carray_value(CDB2SQLQUERY__Bindvalue__Array *carray, struct param_data *param)
{
    param->null = 0;
    switch (carray->type_case) {
    case CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_I32:
        param->u.p = carray->i32->elements;
        param->arraylen = carray->i32->n_elements;
        param->len = sizeof(int32_t);
        break;
    case CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_I64:
        param->u.p = carray->i64->elements;
        param->arraylen = carray->i64->n_elements;
        param->len = sizeof(int64_t);
        break;
    case CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_DBL:
        param->u.p = carray->dbl->elements;
        param->arraylen = carray->dbl->n_elements;
        break;
    case CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_TXT:
        param->u.p = carray->txt->elements;
        param->arraylen = carray->txt->n_elements;
        break;
    case CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_BLOB:
        param->u.p = carray->blob->elements;
        param->arraylen = carray->blob->n_elements;
        break;
    default:
        return -1;
    }
    return 0;
}

static int newsql_param_value(struct sqlclntstate *clnt,
                              struct param_data *param, int n)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery;
    if (n >= sqlquery->n_bindvars) {
        return -1;
    }
    CDB2SQLQUERY__Bindvalue *val = sqlquery->bindvars[n];
    param->name = val->varname;
    param->pos = val->has_index ? val->index : 0;
    param->type = newsql_to_client_type(val->type);

    if (val->carray) {
        return newsql_carray_value(val->carray, param);
    }

    int len = val->value.len;
    void *p = val->value.data;

    /* The bound parameter is from an old client which does not send isnull,
       and its length is 0. Treat it as a NULL to keep backward-compatible. */
    if (len == 0 && !val->has_isnull) {
        param->null = 1;
        return 0;
    }

    if (val->isnull) {
        param->null = 1;
        return 0;
    }

    int little = appdata->sqlquery->little_endian;

    return get_type(param, p, len, param->type, clnt->tzname, little);
}

static int newsql_override_count(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    return appdata->sqlquery->n_types;
}

static int newsql_override_type(struct sqlclntstate *clnt, int i)
{
    struct newsql_appdata *appdata = clnt->appdata;
    int n = appdata->sqlquery->n_types;
    if (n && i >= 0 && i < n) {
        return appdata->sqlquery->types[i];
    }
    return 0;
}

static int newsql_clr_cnonce(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery;
    sqlquery->has_cnonce = 0;
    return 0;
}

static int newsql_has_cnonce(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery;
    return sqlquery->has_cnonce;
}

static int newsql_set_cnonce(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery;
    sqlquery->has_cnonce = 1;
    return 0;
}

static int newsql_get_cnonce(struct sqlclntstate *clnt, snap_uid_t *snap)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery;
    if (!sqlquery->has_cnonce || sqlquery->cnonce.len > MAX_SNAP_KEY_LEN) {
        return -1;
    }
    snap->keylen = sqlquery->cnonce.len;
    memcpy(snap->key, sqlquery->cnonce.data, sqlquery->cnonce.len);
    return 0;
}

static int newsql_clr_snapshot(struct sqlclntstate *clnt)
{
    clnt->snapshot_file = 0;
    clnt->snapshot_offset = 0;
    clnt->is_hasql_retry = 0;
    return 0;
}

static int newsql_get_snapshot(struct sqlclntstate *clnt, int *file,
                               int *offset)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery;
    if (sqlquery->snapshot_info) {
        *file = sqlquery->snapshot_info->file;
        *offset = sqlquery->snapshot_info->offset;
    }
    return 0;
}

static int newsql_upd_snapshot(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery;
    /* We need to restore send_intrans_response
       on clnt even if the snapshot info has been populated.
       However, dont't attempt to restore if client overrides
       send_intrans_response by setting INTRANSRESULTS to ON. */
    if (appdata->send_intrans_response != -1 && sqlquery->n_features > 0 &&
        gbl_disable_skip_rows == 0) {
        for (int ii = 0; ii < sqlquery->n_features; ii++) {
            if (CDB2_CLIENT_FEATURES__SKIP_INTRANS_RESULTS !=
                sqlquery->features[ii])
                continue;
            appdata->send_intrans_response = 0;
        }
    }

    if (clnt->is_hasql_retry) {
        return 0;
    }

    // If this is a retry, we should already have the snapshot file and offset
    newsql_clr_snapshot(clnt);

    if (sqlquery->snapshot_info) {
        clnt->snapshot_file = sqlquery->snapshot_info->file;
        clnt->snapshot_offset = sqlquery->snapshot_info->offset;
    }
    return 0;
}

static int newsql_has_high_availability(struct sqlclntstate *clnt)
{
    return clnt->high_availability_flag;
}

static int newsql_set_high_availability(struct sqlclntstate *clnt)
{
    clnt->high_availability_flag = 1;
    return 0;
}

static int newsql_clr_high_availability(struct sqlclntstate *clnt)
{
    clnt->high_availability_flag = 0;
    return 0;
}

static int newsql_get_high_availability(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery;
    /* MOHIT -- Check here that we are in high availablity, its cdb2api, and
     * is its a retry. */
    if (clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS) {
        clnt->num_retry = sqlquery->retry;
        if (sqlquery->retry && sqlquery->snapshot_info) {
            clnt->snapshot_file = sqlquery->snapshot_info->file;
            clnt->snapshot_offset = sqlquery->snapshot_info->offset;
        } else {
            clnt->snapshot_file = 0;
            clnt->snapshot_offset = 0;
        }
    }
    return is_snap_uid_retry(clnt);
}

static int newsql_has_parallel_sql(struct sqlclntstate *clnt)
{
    return !gbl_dohsql_disable;
}

static void newsql_add_steps(struct sqlclntstate *clnt, double steps)
{
    gbl_nnewsql_steps += steps;
}

static void newsql_setup_client_info(struct sqlclntstate *clnt,
                                     struct sqlthdstate *thd, char *replay)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery;
    CDB2SQLQUERY__Cinfo *cinfo = sqlquery->client_info;
    if (cinfo == NULL)
        return;
    thrman_wheref(thd->thr_self,
                  "%s pid: %d host_id: %d argv0: %s open-stack: %s sql: %s",
                  replay, cinfo->pid, cinfo->host_id,
                  cinfo->argv0 ? cinfo->argv0 : "(unset)",
                  cinfo->stack ? cinfo->stack : "(no-stack)", clnt->sql);
}

static int newsql_skip_row(struct sqlclntstate *clnt, uint64_t rowid)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery;
    if (clnt->num_retry == sqlquery->retry &&
        (clnt->num_retry == 0 || sqlquery->has_skip_rows == 0 ||
         sqlquery->skip_rows < rowid)) {
        return 0;
    }
    return 1;
}

static int newsql_log_context(struct sqlclntstate *clnt,
                              struct reqlogger *logger)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery;
    if (sqlquery->n_context > 0) {
        for (int i = 0; i < sqlquery->n_context; ++i) {
            reqlog_logf(logger, REQL_INFO, "(%d) %s", i, sqlquery->context[i]);
        }
    }

    /* If request context is set, the client is changing the context. */
    if (sqlquery->context) {
        /* Latch the context - client only re-sends context if
           it changes.  TODO: this seems needlessly expensive. */
        for (int i = 0, len = clnt->ncontext; i != len; ++i)
            free(clnt->context[i]);
        free(clnt->context);

        clnt->ncontext = sqlquery->n_context;
        clnt->context = malloc(sizeof(char *) * sqlquery->n_context);
        for (int i = 0; i < sqlquery->n_context; i++)
            clnt->context[i] = strdup(sqlquery->context[i]);
    }

    /* Whether latched from previous run, or just set, pass this to logger. */
    reqlog_set_context(logger, clnt->ncontext, clnt->context);
    return 0;
}

static uint64_t newsql_get_client_starttime(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery;
    if (sqlquery->req_info) {
        return sqlquery->req_info->timestampus;
    }
    return 0;
}

static int newsql_get_client_retries(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery;
    if (sqlquery->req_info) {
        return sqlquery->req_info->num_retries;
    }
    return 0;
}

static int newsql_send_intrans_response(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    return appdata->send_intrans_response;
}

static int newsql_close(struct sqlclntstate *clnt)
{
    abort();
    return 0;
}
static int newsql_flush(struct sqlclntstate *clnt)
{
    abort();
    return 0;
}
static int newsql_get_fileno(struct sqlclntstate *clnt)
{
    abort();
    return 0;
}
static int newsql_get_x509_attr(struct sqlclntstate *clnt, int nid, void *out, int outsz)
{
    abort();
    return 0;
}
static int newsql_has_ssl(struct sqlclntstate *clnt)
{
    abort();
    return 0;
}
static int newsql_has_x509(struct sqlclntstate *clnt)
{
    abort();
    return 0;
}
static int newsql_local_check(struct sqlclntstate *clnt)
{
    abort();
    return 0;
}
static int newsql_peer_check(struct sqlclntstate *clnt)
{
    abort();
    return 0;
}
static int newsql_set_timeout(struct sqlclntstate *clnt, int timeout_ms)
{
    abort();
    return 0;
}

int handle_set_querylimits(char *sqlstr, struct sqlclntstate *clnt)
{
    int iswarn = 0;
    double cost;
    char *endp;

    sqlstr += 11;
    sqlstr = skipws(sqlstr);

    if (strncasecmp(sqlstr, "warn", 4) == 0) {
        sqlstr += 4;
        sqlstr = skipws(sqlstr);
        iswarn = 1;
    }

    if (strncasecmp(sqlstr, "maxcost", 7) == 0) {
        sqlstr += 7;
        sqlstr = skipws(sqlstr);
        cost = strtod(sqlstr, &endp);
        if (*endp != 0)
            return 1;
        if (iswarn) {
            clnt->limits.maxcost_warn = cost;
        } else {
            clnt->limits.maxcost = cost;
        }
        return 0;
    } else if (strncasecmp(sqlstr, "tablescans", 10) == 0) {
        int onoff = 1;
        sqlstr += 10;
        sqlstr = skipws(sqlstr);
        if (strncasecmp(sqlstr, "on", 2) == 0) {
            onoff = 1;
        } else if (strncasecmp(sqlstr, "off", 3) == 0) {
            onoff = 0;
        } else
            return 0;
        if (iswarn) {
            clnt->limits.tablescans_warn = onoff;
        } else {
            clnt->limits.tablescans_ok = htonl(onoff);
        }
        return 0;
    } else if (strncasecmp(sqlstr, "temptables", 10) == 0) {
        int onoff = 1;
        sqlstr += 10;
        sqlstr = skipws(sqlstr);
        if (strncasecmp(sqlstr, "on", 2) == 0) {
            onoff = 0;
        } else if (strncasecmp(sqlstr, "off", 3) == 0) {
            onoff = 1;
        } else
            return 0;
        if (iswarn) {
            clnt->limits.temptables_warn = htonl(onoff);
        } else {
            clnt->limits.temptables_ok = htonl(onoff);
        }
        return 0;
    } else
        return 1;
}

int process_set_commands(struct sqlclntstate *clnt, CDB2SQLQUERY *sql_query)
{
    struct newsql_appdata *appdata = clnt->appdata;
    int num_commands = 0;
    char *sqlstr = NULL;
    char *endp;
    int rc = 0;
    num_commands = sql_query->n_set_flags;
    for (int ii = 0; ii < num_commands && rc == 0; ii++) {
        sqlstr = sql_query->set_flags[ii];
        sqlstr = skipws(sqlstr);
        if (strncasecmp(sqlstr, "set", 3) == 0) {
            char err[256];
            err[0] = '\0';
            sql_debug_logf(clnt, __func__, __LINE__,
                           "processing set command '%s'\n", sqlstr);
            sqlstr += 3;
            sqlstr = skipws(sqlstr);
            if (strncasecmp(sqlstr, "transaction", 11) == 0) {
                sqlstr += 11;
                sqlstr = skipws(sqlstr);

                if (strncasecmp(sqlstr, "chunk", 5) == 0) {
                    int tmp;
                    sqlstr += 5;
                    sqlstr = skipws(sqlstr);

                    if (!sqlstr || ((tmp = atoi(sqlstr)) <= 0)) {
                        snprintf(err, sizeof(err),
                                 "set transaction chunk N: missing chunk size "
                                 "N \"%s\"",
                                 sqlstr);
                        rc = ii + 1;
                    } else if (clnt->dbtran.mode != TRANLEVEL_SOSQL) {
                        snprintf(err, sizeof(err),
                                 "transaction chunks require SOCKSQL transaction mode");
                        rc = ii + 1;
                    } else {
                        clnt->dbtran.maxchunksize = tmp;
                        /* in chunked mode, we disable verify retries */
                        clnt->verifyretry_off = 1;
                    }
                } else {
                    clnt->dbtran.mode = TRANLEVEL_INVALID;
                    newsql_clr_high_availability(clnt);
                    if (strncasecmp(sqlstr, "read", 4) == 0) {
                        sqlstr += 4;
                        sqlstr = skipws(sqlstr);
                        if (strncasecmp(sqlstr, "committed", 9) == 0) {
                            clnt->dbtran.mode = TRANLEVEL_RECOM;
                        }
                    } else if (strncasecmp(sqlstr, "serial", 6) == 0) {
                        clnt->dbtran.mode = TRANLEVEL_SERIAL;
                        if (clnt->hasql_on == 1) {
                            newsql_set_high_availability(clnt);
                        }
                    } else if (strncasecmp(sqlstr, "blocksql", 7) == 0) {
                        clnt->dbtran.mode = TRANLEVEL_SOSQL;
                    } else if (strncasecmp(sqlstr, "snap", 4) == 0) {
                        sqlstr += 4;
                        clnt->dbtran.mode = TRANLEVEL_SNAPISOL;
                        clnt->verify_retries = 0;
                        if (clnt->hasql_on == 1) {
                            newsql_set_high_availability(clnt);
                            logmsg(LOGMSG_ERROR, "Enabling snapshot isolation "
                                                 "high availability\n");
                        }
                    }
                    if (clnt->dbtran.mode == TRANLEVEL_INVALID) {
                        rc = ii + 1;
                    } else if (clnt->dbtran.mode != TRANLEVEL_SOSQL && clnt->dbtran.maxchunksize) {
                        snprintf(err, sizeof(err),
                                 "transaction chunks require SOCKSQL transaction mode");
                        rc = ii + 1;
                    }
                }
            } else if (strncasecmp(sqlstr, "timeout", 7) == 0) {
                sqlstr += 7;
                sqlstr = skipws(sqlstr);
                int timeout = strtol(sqlstr, &endp, 10);
                clnt->plugin.set_timeout(clnt, timeout);
            } else if (strncasecmp(sqlstr, "maxquerytime", 12) == 0) {
                sqlstr += 12;
                sqlstr = skipws(sqlstr);
                int timeout = strtol(sqlstr, &endp, 10);
                if (timeout >= 0)
                    clnt->query_timeout = timeout;
            } else if (strncasecmp(sqlstr, "timezone", 8) == 0) {
                sqlstr += 8;
                sqlstr = skipws(sqlstr);
                strncpy0(clnt->tzname, sqlstr, sizeof(clnt->tzname));
            } else if (strncasecmp(sqlstr, "datetime", 8) == 0) {
                sqlstr += 8;
                sqlstr = skipws(sqlstr);

                if (strncasecmp(sqlstr, "precision", 9) == 0) {
                    sqlstr += 9;
                    sqlstr = skipws(sqlstr);
                    DTTZ_TEXT_TO_PREC(sqlstr, clnt->dtprec, 0, return -1);
                } else {
                    rc = ii + 1;
                }
            } else if (strncasecmp(sqlstr, "user", 4) == 0) {
                sqlstr += 4;
                sqlstr = skipws(sqlstr);
                if (!sqlite3IsCorrectlyQuoted(sqlstr)) {
                    snprintf(err, sizeof(err),
                             "set user: '%s' is an incorrectly quoted string",
                             sqlstr);
                    rc = ii + 1;
                } else {
                    sqlite3Dequote(sqlstr);
                    if (strlen(sqlstr) >= sizeof(clnt->current_user.name)) {
                        snprintf(err, sizeof(err),
                                 "set user: '%s' exceeds %zu characters",
                                 sqlstr, sizeof(clnt->current_user.name) - 1);
                        rc = ii + 1;
                    } else {
                        clnt->current_user.have_name = 1;
                        /* Re-authenticate the new user. */
                        if (clnt->authgen &&
                            strcmp(clnt->current_user.name, sqlstr) != 0)
                            clnt->authgen = 0;
                        clnt->current_user.is_x509_user = 0;
                        strcpy(clnt->current_user.name, sqlstr);
                    }
                }
            } else if (strncasecmp(sqlstr, "password", 8) == 0) {
                sqlstr += 8;
                sqlstr = skipws(sqlstr);
                if (!sqlite3IsCorrectlyQuoted(sqlstr)) {
                    snprintf(err, sizeof(err),
                             "set user: '%s' is an incorrectly quoted string",
                             sqlstr);
                    rc = ii + 1;
                } else {
                    sqlite3Dequote(sqlstr);
                    if (strlen(sqlstr) >= sizeof(clnt->current_user.password)) {
                        snprintf(err, sizeof(err),
                                 "set password: password length exceeds %zu "
                                 "characters",
                                 sizeof(clnt->current_user.password) - 1);
                        rc = ii + 1;
                    } else {
                        clnt->current_user.have_password = 1;
                        /* Re-authenticate the new password. */
                        if (clnt->authgen &&
                            strcmp(clnt->current_user.password, sqlstr) != 0)
                            clnt->authgen = 0;
                        strcpy(clnt->current_user.password, sqlstr);
                    }
                }
            } else if (strncasecmp(sqlstr, "spversion", 9) == 0) {
                clnt->spversion.version_num = 0;
                free(clnt->spversion.version_str);
                clnt->spversion.version_str = NULL;
                sqlstr += 9;
                sqlstr = skipws(sqlstr);
                char *spname = sqlstr;
                while (!isspace(*sqlstr)) {
                    ++sqlstr;
                }
                *sqlstr = 0;
                if ((sqlstr - spname) < MAX_SPNAME) {
                    strncpy0(clnt->spname, spname, MAX_SPNAME);
                } else {
                    rc = ii + 1;
                }
                ++sqlstr;

                sqlstr = skipws(sqlstr);
                int ver = strtol(sqlstr, &endp, 10);
                if (*sqlstr == '\'' || *sqlstr == '"') { // looks like a str
                    if (strlen(sqlstr) < MAX_SPVERSION_LEN) {
                        clnt->spversion.version_str = strdup(sqlstr);
                        sqlite3Dequote(clnt->spversion.version_str);
                    } else {
                        rc = ii + 1;
                    }
                } else if (*endp == 0) { // parsed entire number successfully
                    clnt->spversion.version_num = ver;
                } else {
                    rc = ii + 1;
                }
            } else if (strncasecmp(sqlstr, "prepare_only", 12) == 0) {
                sqlstr += 12;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->prepare_only = 0;
                } else {
                    clnt->prepare_only = 1;
                }
            } else if (strncasecmp(sqlstr, "readonly", 8) == 0) {
                sqlstr += 8;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->is_readonly_set = 0;
                } else {
                    clnt->is_readonly_set = 1;
                }
            } else if (strncasecmp(sqlstr, "expert", 6) == 0) {
                sqlstr += 6;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->is_expert = 0;
                } else {
                    clnt->is_expert = 1;
                    clnt->is_fast_expert = (strncasecmp(sqlstr, "fast", 4) == 0);
                }
            } else if (strncasecmp(sqlstr, "sptrace", 7) == 0) {
                sqlstr += 7;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->want_stored_procedure_trace = 0;
                } else {
                    clnt->want_stored_procedure_trace = 1;
                }
            } else if (strncasecmp(sqlstr, "cursordebug", 11) == 0) {
                sqlstr += 11;
                sqlstr = skipws(sqlstr);
                bdb_osql_trak(sqlstr, &clnt->bdb_osql_trak);
            } else if (strncasecmp(sqlstr, "spdebug", 7) == 0) {
                sqlstr += 7;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->want_stored_procedure_debug = 0;
                } else {
                    clnt->want_stored_procedure_debug = 1;
                }
            } else if (strncasecmp(sqlstr, "HASQL", 5) == 0) {
                sqlstr += 5;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "on", 2) == 0) {
                    clnt->hasql_on = 1;
                    if (clnt->dbtran.mode == TRANLEVEL_SERIAL ||
                        clnt->dbtran.mode == TRANLEVEL_SNAPISOL) {
                        newsql_set_high_availability(clnt);
                        sql_debug_logf(clnt, __func__, __LINE__,
                                       "setting "
                                       "high_availability\n");
                    }
                } else {
                    clnt->hasql_on = 0;
                    newsql_clr_high_availability(clnt);
                    sql_debug_logf(clnt, __func__, __LINE__,
                                   "clearing "
                                   "high_availability\n");
                }
            } else if (strncasecmp(sqlstr, "verifyretry", 11) == 0) {
                sqlstr += 11;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "on", 2) == 0) {
                    clnt->verifyretry_off = 0;
                } else {
                    clnt->verifyretry_off = 1;
                }
            } else if (strncasecmp(sqlstr, "queryeffects", 12) == 0) {
                sqlstr += 12;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "statement", 9) == 0) {
                    clnt->statement_query_effects = 1;
                }
                if (strncasecmp(sqlstr, "transaction", 11) == 0) {
                    clnt->statement_query_effects = 0;
                }
            } else if (strncasecmp(sqlstr, "remote", 6) == 0) {
                sqlstr += 6;
                sqlstr = skipws(sqlstr);

                int fdbrc = fdb_access_control_create(clnt, sqlstr);
                if (fdbrc) {
                    logmsg(
                        LOGMSG_ERROR,
                        "%s: failed to process remote access settings \"%s\"\n",
                        __func__, sqlstr);
                    rc = ii + 1;
                }
            } else if (strncasecmp(sqlstr, "getcost", 7) == 0) {
                sqlstr += 7;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "on", 2) == 0) {
                    clnt->get_cost = 1;
                } else {
                    clnt->get_cost = 0;
                }
            } else if (strncasecmp(sqlstr, "explain", 7) == 0) {
                /* is_explain flag:
                   1 - show plan
                   2 - show plan + wheretrace */
                sqlstr += 7;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "on", 2) == 0) {
                    clnt->is_explain = 1;
                } else if (strncasecmp(sqlstr, "verbose", 7) == 0) {
                    clnt->is_explain = 2;
                    sqlstr += 7;
                    sqlstr = skipws(sqlstr);

                    /*
                       0x2    -> show headnote and footnote from the solver
                       0x4    -> show how the best index is picked
                       0x8    -> show how the cost of each index is calculated
                       0x10   -> show trace for stat4
                       0x100  -> show all where terms
                       0x200  -> show trace for Or terms
                       0x840  -> show trace for virtual tables
                     */

                    if (sqlstr[0] == '\0')
                        clnt->where_trace_flags = ~0;
                    else
                        clnt->where_trace_flags = (int)strtol(sqlstr, NULL, 16);
                } else {
                    clnt->is_explain = 0;
                }
            } else if (strncasecmp(sqlstr, "maxtransize", 11) == 0) {
                sqlstr += 11;
                int maxtransz = strtol(sqlstr, &endp, 10);
                if (endp != sqlstr && maxtransz >= 0)
                    clnt->osql_max_trans = maxtransz;
                else
                    logmsg(LOGMSG_ERROR,
                           "Error: bad value for maxtransize %s\n", sqlstr);
#ifdef DEBUG
                printf("setting clnt->osql_max_trans to %d\n",
                       clnt->osql_max_trans);
#endif
            } else if (strncasecmp(sqlstr, "groupconcatmemlimit",
                                   sizeof("groupconcatmemlimit") - 1) == 0) {
                sqlstr += sizeof("groupconcatmemlimit");
                int sz = strtol(sqlstr, &endp, 10);
                if (endp != sqlstr && sz >= 0)
                    clnt->group_concat_mem_limit = sz;
                else
                    logmsg(LOGMSG_ERROR,
                           "Error: bad value for groupconcatmemlimit %s\n",
                           sqlstr);
#ifdef DEBUG
                printf("setting clnt->group_concat_mem_limit to %d\n",
                       clnt->group_concat_mem_limit);
#endif
            } else if (strncasecmp(sqlstr, "plannereffort", 13) == 0) {
                sqlstr += 13;
                int effort = strtol(sqlstr, &endp, 10);
                if (0 < effort && effort <= 10)
                    clnt->planner_effort = effort;
#ifdef DEBUG
                printf("setting clnt->planner_effort to %d\n",
                       clnt->planner_effort);
#endif
            } else if (strncasecmp(sqlstr, "intransresults", 14) == 0) {
                sqlstr += 14;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    appdata->send_intrans_response = 0;
                } else {
                    appdata->send_intrans_response = -1;
                }
            } else if (strncasecmp(sqlstr, "admin", 5) == 0) {
                sqlstr += 7;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->admin = 0;
                } else {
                    clnt->admin = 1;
                }
            } else if (strncasecmp(sqlstr, "querylimit", 10) == 0) {
                rc = handle_set_querylimits(sqlstr, clnt);
            } else if (strncasecmp(sqlstr, "rowbuffer", 9) == 0) {
                sqlstr += 9;
                sqlstr = skipws(sqlstr);
                clnt->rowbuffer = (strncasecmp(sqlstr, "on", 2) == 0);
            } else if (strncasecmp(sqlstr, "sockbplog", 10) == 0) {
                init_bplog_socket(clnt);
                rc = 0;
            } else {
                rc = ii + 1;
            }

            if (rc) {
                if (err[0] == '\0')
                    snprintf(err, sizeof(err) - 1, "Invalid set command '%s'",
                             sqlstr);
                newsql_write_response(clnt, RESPONSE_ERROR_PREPARE, err, 0);
            }
        }
    }
    return rc;
}

int forward_set_commands(struct sqlclntstate *clnt, cdb2_hndl_tp *hndl,
                         struct errstat *err)
{
    struct newsql_appdata *appdata = clnt->appdata;
    CDB2SQLQUERY *sql_query = appdata->sqlquery;
    int rc = 0;

    if (!sql_query) {
        errstat_set_rcstrf(err, -1, "Sqlquery not set");
        return -1;
    }

    for (int ii = 0; ii < sql_query->n_set_flags && rc == 0; ii++) {
        char *sqlstr = sql_query->set_flags[ii];
        rc = cdb2_run_statement(hndl, sqlstr);
        if (rc != CDB2_OK) {
            errstat_set_rcstrf(err, -1, "Failed to run \"%s\"", sqlstr);
            return -1;
        }
    }
    return 0;
}

int newsql_heartbeat(struct sqlclntstate *clnt)
{
    int state;

    if (!clnt->heartbeat)
        return 0;
    if (!clnt->ready_for_heartbeats)
        return 0;

    /* We're still in a good state if we're just waiting for the client to consume an event. */
    if (is_pingpong(clnt))
        state = 1;
    else {
        state = (clnt->sqltick > clnt->sqltick_last_seen);
        clnt->sqltick_last_seen = clnt->sqltick;
    }

    return newsql_send_hdr(clnt, RESPONSE_HEADER__SQL_RESPONSE_HEARTBEAT, state);
}

static int do_query_on_master_check(struct sqlclntstate *clnt, CDB2SQLQUERY *sql_query)
{
    int allow_master_exec = 0;
    int allow_master_dbinfo = 0;
    for (int ii = 0; ii < sql_query->n_features; ii++) {
        switch (sql_query->features[ii]) {
        case CDB2_CLIENT_FEATURES__ALLOW_MASTER_EXEC: allow_master_exec = 1; break;
        case CDB2_CLIENT_FEATURES__ALLOW_MASTER_DBINFO: allow_master_dbinfo = 1; break;
        case CDB2_CLIENT_FEATURES__ALLOW_QUEUING: clnt->queue_me = 1; break;
        }
    }
    if (thedb->nsiblings == 1 || thedb->rep_sync == REP_SYNC_NONE || clnt->plugin.local_check(clnt)) {
        return 0;
    }
    if (bdb_master_should_reject(thedb->bdb_env) && allow_master_exec == 0) {
        ATOMIC_ADD32(gbl_masterrejects, 1);
        if (allow_master_dbinfo) {
            struct newsql_appdata *appdata = clnt->appdata;
            appdata->write_dbinfo(clnt);
        }
        logmsg(LOGMSG_DEBUG, "Rejecting query on master\n");
        return 1;
    }
    return 0;
}

static int incoh_reject(int admin, bdb_state_type *bdb_state)
{
    /* If this isn't from an admin session and the node isn't coherent
       and we disallow running queries on an incoherent node, reject */
    return (!admin && !bdb_am_i_coherent(bdb_state) && !gbl_allow_incoherent_sql);
}

int is_commit_rollback(struct sqlclntstate *clnt)
{
    return (strncasecmp(clnt->sql, "commit", 6) == 0) ||
           (strncasecmp(clnt->sql, "rollback", 8) == 0);
}

int newsql_first_run(struct sqlclntstate *clnt, CDB2SQLQUERY *sql_query)
{
    for (int ii = 0; ii < sql_query->n_features; ++ii) {
        switch(sql_query->features[ii]) {
        case CDB2_CLIENT_FEATURES__FLAT_COL_VALS:
            clnt->flat_col_vals = 1;
            break;
        case CDB2_CLIENT_FEATURES__REQUEST_FP:
            clnt->request_fp = 1;
            break;
        }
    }
    if (sql_query->client_info) {
        clnt->conninfo.pid = sql_query->client_info->pid;
        clnt->last_pid = sql_query->client_info->pid;
    } else {
        clnt->conninfo.pid = 0;
        clnt->last_pid = 0;
    }
    clnt->tzname[0] = 0;
    clnt->osql.count_changes = 1;
    clnt->dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
    newsql_clr_high_availability(clnt);
    return clnt->admin ? 0 : do_query_on_master_check(clnt, sql_query);
}

int newsql_loop(struct sqlclntstate *clnt, CDB2SQLQUERY *sql_query)
{
    Pthread_mutex_lock(&clnt->sql_lk);
    clnt->sql = sql_query->sql_query;
    Pthread_mutex_unlock(&clnt->sql_lk);
    clnt->added_to_hist = 0;

    free_original_normalized_sql(clnt);

    if (!in_client_trans(clnt)) {
        bzero(&clnt->effects, sizeof(clnt->effects));
        bzero(&clnt->log_effects, sizeof(clnt->log_effects));
        bzero(&clnt->chunk_effects, sizeof(clnt->chunk_effects));
        clnt->had_errors = 0;
        clnt->ctrl_sqlengine = SQLENG_NORMAL_PROCESS;
    }
    if (clnt->dbtran.mode < TRANLEVEL_SOSQL) {
        clnt->dbtran.mode = TRANLEVEL_SOSQL;
    }
    clnt->osql.sent_column_data = 0;

    if (clnt->tzname[0] == 0 && sql_query->tzname) {
        strncpy0(clnt->tzname, sql_query->tzname, sizeof(clnt->tzname));
    }
    if (sql_query->dbname && thedb->envname && strcasecmp(sql_query->dbname, thedb->envname)) {
        CDB2SQLQUERY__Cinfo *info = sql_query->client_info;
        const char *query = sql_query->sql_query;
        logmsg(LOGMSG_ERROR, "bad  dbname:%s  host:%s  pid:%d  argv0:%s  query:%.32s  len:%d\n",
               sql_query->dbname,
               clnt->origin,
               info ? info->pid : -999,
               info ? info->argv0 : "???",
               query ? query : "???",
               query ? (int)strlen(query) : -999);
        char errstr[64 + (2 * MAX_DBNAME_LENGTH)];
        snprintf(errstr, sizeof(errstr),
                 "DB name mismatch query:%s actual:%s", sql_query->dbname,
                 thedb->envname);
        write_response(clnt, RESPONSE_ERROR, errstr, CDB2__ERROR_CODE__WRONG_DB);
        return -1;
    }
    if (sql_query->client_info) {
        if (clnt->rawnodestats) {
            release_node_stats(clnt->argv0, clnt->stack, clnt->origin);
            clnt->rawnodestats = NULL;
        }
        if (clnt->conninfo.pid && clnt->conninfo.pid != sql_query->client_info->pid) {
            /* Different pid is coming without reset. */
            logmsg(LOGMSG_WARN,
                   "Multiple processes using same socket PID 1 %d PID 2 %d Host %.8x\n",
                   clnt->conninfo.pid, sql_query->client_info->pid,
                   sql_query->client_info->host_id);
        }
        clnt->conninfo.pid = sql_query->client_info->pid;
        clnt->conninfo.node = sql_query->client_info->host_id;
        if (clnt->argv0) {
            free(clnt->argv0);
            clnt->argv0 = NULL;
        }
        if (clnt->stack) {
            free(clnt->stack);
            clnt->stack = NULL;
        }
        if (sql_query->client_info->argv0) {
            clnt->argv0 = strdup(sql_query->client_info->argv0);
        }
        if (sql_query->client_info->stack) {
            clnt->stack = strdup(sql_query->client_info->stack);
        }
        if (sql_query->identity) {
            clnt->externalAuthUser = sql_query->identity->principal;
        }
    }
    if (clnt->rawnodestats == NULL) {
        clnt->rawnodestats =
            get_raw_node_stats(clnt->argv0, clnt->stack, clnt->origin,
                               clnt->plugin.get_fileno(clnt), clnt->plugin.has_ssl(clnt));
    }
    if (process_set_commands(clnt, sql_query)) {
        return -1;
    }

    /* Mark connection as readonly, if the
     * 1. db is running in readonly mode,
     * 2. the connection is forced to run in readonly mode, or
     * 3. the connection is explicitly set to in readonly mode
     */
    if (gbl_readonly || clnt->force_readonly || clnt->is_readonly_set)
        clnt->is_readonly = 1;
    else
        clnt->is_readonly = 0;

    if (gbl_rowlocks && clnt->dbtran.mode != TRANLEVEL_SERIAL) {
        clnt->dbtran.mode = TRANLEVEL_SNAPISOL;
    }
    /* avoid new accepting new queries/transaction on opened connections
     if we are incoherent (and not in a transaction). */
    if (incoh_reject(clnt->admin, thedb->bdb_env) &&
        (clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS)) {
        logmsg(LOGMSG_DEBUG, "%s new query on incoherent node, dropping socket\n", __func__);
        return -1;
    }
    ATOMIC_ADD32(gbl_nnewsql, 1);
    if (clnt->plugin.has_ssl(clnt))
        ATOMIC_ADD32(gbl_nnewsql_ssl, 1);

    return 0;
}

int newsql_should_dispatch(struct sqlclntstate *clnt, int *commit_rollback)
{
    /* return 0 => shoud dispatch */
    *commit_rollback = is_commit_rollback(clnt);
    return clnt->had_errors && !(*commit_rollback);
}

void newsql_reset(struct sqlclntstate *clnt)
{
    if (clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
        /* Discard the pending transaction when receiving RESET from the
               sockpool. We reach here if
               1) the handle is in a open transaction, and
               2) the last statement is a SELECT, and
               3) the client closes the handle and donates the connection
                  to the sockpool, and then,
               4) the client creates a new handle and reuses the connection
                  from the sockpool. */
        handle_sql_intrans_unrecoverable_error(clnt);
    }
    reset_clnt(clnt, 0);
    clnt->tzname[0] = 0;
    clnt->osql.count_changes = 1;
    clnt->heartbeat = 1;
    clnt->dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
}

void free_newsql_appdata(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    cleanup_clnt(clnt);
    if (appdata->postponed) {
        free(appdata->postponed->row);
        free(appdata->postponed);
        appdata->postponed = NULL;
    }
    free(appdata->col_info.type);
}

void *(*externalMakeNewsqlAuthData)(void *, CDB2SQLQUERY__IdentityBlob *id) = NULL;

static void *newsql_get_authdata(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    if (appdata) {
        CDB2SQLQUERY *sql_query = appdata->sqlquery;
        if (sql_query && sql_query->identity) {
            if (externalMakeNewsqlAuthData) {
                return externalMakeNewsqlAuthData(clnt->authdata, sql_query->identity);
            }
        }
    }
    if (clnt->authdata) {
        free(clnt->authdata);
        clnt->authdata = NULL;
    }
    return NULL;
}

void newsql_setup_clnt(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    appdata->send_intrans_response = 1;
    update_col_info(&appdata->col_info, 32);
    plugin_set_callbacks(clnt, newsql);
}

void newsql_effects(CDB2SQLRESPONSE *r, CDB2EFFECTS *e, struct sqlclntstate *clnt)
{
    r->response_type = RESPONSE_TYPE__COMDB2_INFO;
    int verify = !clnt->verifyretry_off;
    enum transaction_level mode = clnt->dbtran.mode;
    if (verify && mode != TRANLEVEL_SNAPISOL && mode != TRANLEVEL_SERIAL) {
        r->error_code = -1;
        r->error_string = "Get effects not supported in transaction with verifyretry on";
        return;
    }
    set_sent_data_to_client(clnt, 1, __func__, __LINE__);
    struct query_effects *effects = newsql_get_query_effects(clnt);
    effects->num_affected = effects->num_updated + effects->num_deleted + effects->num_inserted;
    e->num_affected = effects->num_affected;
    e->num_selected = effects->num_selected;
    e->num_updated = effects->num_updated;
    e->num_deleted = effects->num_deleted;
    e->num_inserted = effects->num_inserted;
    r->effects = e;
}
