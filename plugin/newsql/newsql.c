/*
   Copyright 2017 Bloomberg Finance L.P.

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

#include <alloca.h>
#include <pthread.h>
#include <stdlib.h>

typedef struct VdbeSorter VdbeSorter;

#include "comdb2_plugin.h"
#include "pb_alloc.h"
#include "sp.h"
#include "sql.h"
#include "comdb2_appsock.h"
#include "comdb2_atomic.h"
#include <str0.h>

struct thr_handle;
struct sbuf2;

extern int gbl_sqlwrtimeoutms;
extern int active_appsock_conns;
#if WITH_SSL
extern ssl_mode gbl_client_ssl_mode;
extern SSL_CTX *gbl_ssl_ctx;
#endif

int disable_server_sql_timeouts(void);
int tdef_to_tranlevel(int tdef);
int osql_clean_sqlclntstate(struct sqlclntstate *clnt);
int watcher_warning_function(void *arg, int timeout, int gap);
void handle_sql_intrans_unrecoverable_error(struct sqlclntstate *clnt);
int fdb_access_control_create(struct sqlclntstate *clnt, char *str);
int handle_failed_dispatch(struct sqlclntstate *clnt, char *errstr);
int sbuf_is_local(SBUF2 *sb);

struct newsqlheader {
    int type;        /*  newsql request/response type */
    int compression; /*  Some sort of compression done? */
    int dummy;       /*  Make it equal to fsql header. */
    int length;      /*  length of response */
};

struct newsql_postponed_data {
    size_t len;
    struct newsqlheader hdr;
    uint8_t *row;
};

struct newsql_appdata {
    struct newsql_postponed_data *postponed;
    CDB2SQLQUERY *query;

    /* row buf */
    size_t packed_capacity;
    void *packed_buf;

    /* columns */
    int count;
    int capacity;
    int type[0]; /* must be last */
};

static int fill_snapinfo(struct sqlclntstate *clnt, int *file, int *offset)
{
    char cnonce[256];
    int rcode = 0;
    if (gbl_extended_sql_debug_trace && clnt->sql_query) {
        snprintf(cnonce, 256, "%s", clnt->sql_query->cnonce.data);
    }
    if (clnt->sql_query && clnt->sql_query->snapshot_info &&
        clnt->sql_query->snapshot_info->file > 0) {
        *file = clnt->sql_query->snapshot_info->file;
        *offset = clnt->sql_query->snapshot_info->offset;

        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER,
                    "%s line %d cnonce '%s' sql_query->snapinfo is [%d][%d], "
                    "clnt->snapinfo is [%d][%d]: use client snapinfo!\n",
                    __func__, __LINE__, cnonce,
                    clnt->sql_query->snapshot_info->file,
                    clnt->sql_query->snapshot_info->offset, clnt->snapshot_file,
                    clnt->snapshot_offset);
        }
        return 0;
    }

    if (*file == 0 && clnt->sql_query &&
        (clnt->in_client_trans || clnt->is_hasql_retry) &&
        clnt->snapshot_file) {
        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER,
                    "%s line %d cnonce '%s' sql_query->snapinfo is [%d][%d], "
                    "clnt->snapinfo is [%d][%d]\n",
                    __func__, __LINE__, cnonce,
                    (clnt->sql_query && clnt->sql_query->snapshot_info)
                        ? clnt->sql_query->snapshot_info->file
                        : -1,
                    (clnt->sql_query && clnt->sql_query->snapshot_info)
                        ? clnt->sql_query->snapshot_info->offset
                        : -1,
                    clnt->snapshot_file, clnt->snapshot_offset);
        }

        *file = clnt->snapshot_file;
        *offset = clnt->snapshot_offset;
        logmsg(LOGMSG_USER,
                "%s line %d setting newsql snapinfo retry info is [%d][%d]\n",
                __func__, __LINE__, *file, *offset);
        return 0;
    }

    if (*file == 0 && clnt->is_newsql && clnt->sql_query &&
        clnt->ctrl_sqlengine == SQLENG_STRT_STATE) {

        if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DURABLE_LSNS)) {
            uint32_t durable_file, durable_offset, durable_gen;

            int rc = request_durable_lsn_from_master(
                thedb->bdb_env, &durable_file, &durable_offset, &durable_gen);

            if (rc == 0) {
                *file = durable_file;
                *offset = durable_offset;

                if (gbl_extended_sql_debug_trace) {
                    logmsg(LOGMSG_USER, "%s line %d cnonce='%s' master "
                                        "returned durable-lsn "
                                        "[%d][%d], clnt->is_hasql_retry=%d\n",
                           __func__, __LINE__, cnonce, *file, *offset,
                           clnt->is_hasql_retry);
                }
            } else {
                if (gbl_extended_sql_debug_trace) {
                    logmsg(LOGMSG_USER,
                           "%s line %d cnonce='%s' durable-lsn request "
                           "returns %d "
                           "clnt->snapshot_file=%d clnt->snapshot_offset=%d "
                           "clnt->is_hasql_retry=%d\n",
                           __func__, __LINE__, cnonce, rc, clnt->snapshot_file,
                           clnt->snapshot_offset, clnt->is_hasql_retry);
                }
                rcode = -1;
            }
            return rcode;
        }
    }

    if (*file == 0) {
        bdb_tran_get_start_file_offset(thedb->bdb_env, clnt->dbtran.shadow_tran,
                                       file, offset);
        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER, "%s line %d start_file_offset snapinfo is "
                                "[%d][%d], sqlengine-state is %d\n",
                   __func__, __LINE__, *file, *offset, clnt->ctrl_sqlengine);
        }
    }
    return rcode;
}

#define _has_effects(clnt, sql_response)                                       \
    CDB2EFFECTS effects = CDB2__EFFECTS__INIT;                                 \
                                                                               \
    clnt->effects.num_affected = clnt->effects.num_updated +                   \
                                 clnt->effects.num_deleted +                   \
                                 clnt->effects.num_inserted;                   \
    effects.num_affected = clnt->effects.num_affected;                         \
    effects.num_selected = clnt->effects.num_selected;                         \
    effects.num_updated = clnt->effects.num_updated;                           \
    effects.num_deleted = clnt->effects.num_deleted;                           \
    effects.num_inserted = clnt->effects.num_inserted;                         \
                                                                               \
    sql_response.effects = &effects;

#define _has_features(clnt, sql_response)                                      \
    CDB2ServerFeatures features[10];                                           \
    int n_features = 0;                                                        \
                                                                               \
    if (clnt->skip_feature) {                                                  \
        features[n_features] = CDB2_SERVER_FEATURES__SKIP_ROWS;                \
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
    if (get_high_availability(clnt)) {                                         \
        int file = 0, offset = 0, rc;                                          \
        if (fill_snapinfo(clnt, &file, &offset)) {                             \
            sql_response.error_code = CDB2ERR_CHANGENODE;                      \
        }                                                                      \
        if (file) {                                                            \
            snapshotinfo.file = file;                                          \
            snapshotinfo.offset = offset;                                      \
            sql_response.snapshot_info = &snapshotinfo;                        \
        }                                                                      \
    }


static int newsql_send_hdr(struct sqlclntstate *clnt, int h)
{
    struct newsqlheader hdr = {0};
    hdr.type = ntohl(h);
    int rc;
    pthread_mutex_lock(&clnt->write_lock);
    if ((rc = sbuf2write((char *)&hdr, sizeof(hdr), clnt->sb)) != sizeof(hdr))
        goto done;
    if ((rc = sbuf2flush(clnt->sb)) < 0)
        goto done;
    rc = 0;
done:
    pthread_mutex_unlock(&clnt->write_lock);
    return rc;
}

#define NEWSQL_MAX_RESPONSE_ON_STACK (16 * 1024)

static int newsql_response_int(struct sqlclntstate *clnt,
                               const CDB2SQLRESPONSE *r, int h, int flush)
{
    size_t len = cdb2__sqlresponse__get_packed_size(r);
    uint8_t *buf;
    if (len < NEWSQL_MAX_RESPONSE_ON_STACK) {
        buf = alloca(len);
    } else {
        struct newsql_appdata *appdata = clnt->appdata;
        if (appdata->packed_capacity < len) {
            appdata->packed_capacity = len + 1024;
            appdata->packed_buf =
                malloc_resize(appdata->packed_buf, appdata->packed_capacity);
        }
        buf = appdata->packed_buf;
    }
    cdb2__sqlresponse__pack(r, buf);

    struct newsqlheader hdr = {0};
    hdr.type = ntohl(h);
    hdr.length = ntohl(len);

    int rc;
    pthread_mutex_lock(&clnt->write_lock);
    if ((rc = sbuf2write((char *)&hdr, sizeof(hdr), clnt->sb)) != sizeof(hdr))
        goto done;
    if ((rc = sbuf2write((char *)buf, len, clnt->sb)) != len)
        goto done;
    if (flush && (rc = sbuf2flush(clnt->sb)) < 0)
        goto done;
    rc = 0;
done:
    pthread_mutex_unlock(&clnt->write_lock);
    return rc;
}

static int newsql_response(struct sqlclntstate *c, const CDB2SQLRESPONSE *r,
                           int flush)
{
    return newsql_response_int(c, r, RESPONSE_HEADER__SQL_RESPONSE, flush);
}

static int get_col_type(struct sqlclntstate *clnt, sqlite3_stmt *stmt, int col)
{
    int type = -1;
    if (clnt->sql_query->n_types) {
        type = clnt->sql_query->types[col];
    } else if (stmt) {
        type = sqlite3_column_type(stmt, col);
    }
    if (type == SQLITE_NULL) {
        type = typestr_to_type(sqlite3_column_decltype(stmt, col));
    }
    if (type == SQLITE_DECIMAL) {
        type = SQLITE_TEXT;
    }
    return type;
}

static struct newsql_appdata *newsql_appdata(struct sqlclntstate *clnt,
                                                 int ncols)
{
    struct newsql_appdata *appdata = clnt->appdata;
    if (appdata == NULL) {
        size_t types_sz = ncols * sizeof(appdata->type[0]);
        appdata = calloc(1, sizeof(struct newsql_appdata) + types_sz);
        clnt->appdata = appdata;
        appdata->capacity = ncols;
    } else if (appdata->capacity < ncols) {
        size_t n = ncols + 32;
        size_t types_sz = n * sizeof(appdata->type[0]);
        appdata = realloc(appdata, sizeof(struct newsql_appdata) + types_sz);
        clnt->appdata = appdata;
        appdata->capacity = n;
    }
    appdata->count = ncols;
    return appdata;
}

static void free_newsql_appdata(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    if (appdata == NULL) {
        return;
    }
    if (appdata->postponed) {
        free(appdata->postponed->row);
        free(appdata->postponed);
        appdata->postponed = NULL;
    }
    free(appdata->packed_buf);
    free(appdata);
    clnt->appdata = NULL;
}

extern int gbl_return_long_column_names;
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
    int ncols = sqlite3_column_count(stmt);
    struct newsql_appdata *appdata = newsql_appdata(clnt, ncols);
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
        cols[i].type = appdata->type[i] = get_col_type(clnt, stmt, i);
    }
    CDB2SQLRESPONSE resp = CDB2__SQLRESPONSE__INIT;
    resp.response_type = RESPONSE_TYPE__COLUMN_NAMES;
    resp.n_value = ncols;
    resp.value = value;
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
    if (stmt && sqlite3_column_count(stmt) != ncols) {
        return -1;
    }
    struct newsql_appdata *appdata = newsql_appdata(clnt, ncols);
    size_t n_types = appdata->query ? appdata->query->n_types : 0;
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
        cols[i].type = appdata->type[i] =
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
    struct newsql_appdata *appdata = newsql_appdata(clnt, ncols);
    CDB2SQLRESPONSE__Column cols[ncols];
    CDB2SQLRESPONSE__Column *value[ncols];
    for (int i = 0; i < ncols; ++i) {
        value[i] = &cols[i];
        cdb2__sqlresponse__column__init(&cols[i]);
        const char *name = names[i];
        cols[i].value.data = (uint8_t *)name;
        cols[i].value.len = strlen(name) + 1;
        cols[i].has_type = 1;
        cols[i].type = appdata->type[i] = SQLITE_TEXT;
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

static int newsql_flush(struct sqlclntstate *clnt)
{
    pthread_mutex_lock(&clnt->write_lock);
    int rc = sbuf2flush(clnt->sb);
    pthread_mutex_unlock(&clnt->write_lock);
    return rc < 0;
}

static int newsql_heartbeat(struct sqlclntstate *clnt)
{
    if (!clnt->heartbeat)
        return 0;
    if (!clnt->ready_for_heartbeats)
        return 0;
    return newsql_send_hdr(clnt, 0);
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
    char *hdr = (char *)&appdata->postponed->hdr;
    size_t hdrsz = sizeof(struct newsqlheader);
    char *row = appdata->postponed->row;
    size_t len = appdata->postponed->len;
    int rc;
    pthread_mutex_lock(&clnt->write_lock);
    if ((rc = sbuf2write(hdr, hdrsz, clnt->sb)) != hdrsz)
        goto done;
    if ((rc = sbuf2write(row, len, clnt->sb)) != len)
        goto done;
    rc = 0;
done:
    pthread_mutex_unlock(&clnt->write_lock);
    return rc;
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

#ifndef BYTE_ORDER
#   error "Missing BYTE_ORDER"
#endif

static int newsql_row(struct sqlclntstate *clnt, struct response_data *arg,
                      int postpone)
{
    sqlite3_stmt *stmt = arg->stmt;
    if (stmt == NULL) {
        return newsql_send_postponed_row(clnt);
    }
    int ncols = sqlite3_column_count(stmt);
    struct newsql_appdata *appdata = newsql_appdata(clnt, ncols);
    assert(ncols == appdata->count);
    int flip = 0;
#   if BYTE_ORDER == BIG_ENDIAN
    if (appdata->query->little_endian)
#   elif BYTE_ORDER == LITTLE_ENDIAN
    if (!appdata->query->little_endian)
#   endif
        flip = 1;
    CDB2SQLRESPONSE__Column cols[ncols];
    CDB2SQLRESPONSE__Column *value[ncols];
    for (int i = 0; i < ncols; ++i) {
        value[i] = &cols[i];
        cdb2__sqlresponse__column__init(&cols[i]);
        if (sqlite3_column_type(stmt, i) == SQLITE_NULL) {
            newsql_null(cols, i);
            continue;
        }
        int type = appdata->type[i];
        switch (type) {
        case SQLITE_INTEGER: {
            int64_t i64 = sqlite3_column_int64(stmt, i);
            newsql_integer(cols, i, i64, flip);
            break;
        }
        case SQLITE_FLOAT: {
            double d = sqlite3_column_double(stmt, i);
            newsql_double(cols, i, d, flip);
            break;
        }
        case SQLITE_TEXT: {
            cols[i].value.len = sqlite3_column_bytes(stmt, i) + 1;
            cols[i].value.data = (uint8_t *)sqlite3_column_text(stmt, i);
            break;
        }
        case SQLITE_BLOB: {
            cols[i].value.len = sqlite3_column_bytes(stmt, i);
            cols[i].value.data = (uint8_t *)sqlite3_column_blob(stmt, i);
            break;
        }
        case SQLITE_DATETIME:
        case SQLITE_DATETIMEUS: {
            const dttz_t *d = sqlite3_column_datetime(stmt, i);
            cdb2_client_datetime_t *c = alloca(sizeof(*c));
            if (convDttz2ClientDatetime(d, stmt_tzname(stmt), c, type) != 0) {
                char *e = "failed to convert sqlite to client datetime for field";
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
                sqlite3_column_interval(stmt, i, SQLITE_AFF_INTV_MO);
            newsql_ym(cols, i, val, flip);
            break;
        }
        case SQLITE_INTERVAL_DS:
        case SQLITE_INTERVAL_DSUS: {
            const intv_t *val =
                sqlite3_column_interval(stmt, i, SQLITE_AFF_INTV_SE);
            newsql_ds(cols, i, val, flip);
            break;
        }
        case SQLITE_DECIMAL:
        default: return -1;
        }
    }
    CDB2SQLRESPONSE r = CDB2__SQLRESPONSE__INIT;
    r.response_type = RESPONSE_TYPE__COLUMN_VALUES;
    r.n_value = ncols;
    r.value = value;
    if (clnt->num_retry) {
        r.has_row_id = 1;
        r.row_id = arg->row_id;
    }
    if (postpone) {
        return newsql_save_postponed_row(clnt, &r);
    } else if (arg->pingpong) {
        return newsql_response_int(clnt, &r, RESPONSE_HEADER__SQL_RESPONSE_PING, 1);
    }
    return newsql_response(clnt, &r, 0);
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
    assert(ncols == appdata->count);
    int flip = 0;
#   if BYTE_ORDER == BIG_ENDIAN
    if (appdata->query->little_endian)
#   elif BYTE_ORDER == LITTLE_ENDIAN
    if (!appdata->query->little_endian)
#   endif
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
        int type = appdata->type[i];
        switch (type) {
        case SQLITE_INTEGER: {
            int64_t i64;
            sp_column_val(arg, i, type, &i64);
            newsql_integer(cols, i, i64, flip);
            break;
        }
        case SQLITE_FLOAT: {
            double d;
            sp_column_val(arg, i, type, &d);
            newsql_double(cols, i, d, flip);
            break;
        }
        case SQLITE_TEXT: {
            size_t l;
            cols[i].value.data = sp_column_ptr(arg, i, type, &l);
            cols[i].value.len = l + 1;
            break;
        }
        case SQLITE_BLOB: {
            size_t l;
            cols[i].value.data = sp_column_ptr(arg, i, type, &l);
            cols[i].value.len = l;
            break;
        }
        case SQLITE_DATETIME:
        case SQLITE_DATETIMEUS: {
            datetime_t d;
            sp_column_val(arg, i, type, &d);
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
            sp_column_val(arg, i, type, val);
            cdb2_client_intv_ym_t *c = alloca(sizeof(*c));
            newsql_ym(cols, i, val, flip);
            break;
        }
        case SQLITE_INTERVAL_DS:
        case SQLITE_INTERVAL_DSUS: {
            intv_t in, *val = &in;
            sp_column_val(arg, i, type, &in);
            newsql_ds(cols, i, val, flip);
            break;
        }
        default: return -1;
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
    assert(ncols == appdata->count);
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
        cols[i].value.data = data[i];
        cols[i].value.len = strlen(data[i]) + 1;
    }
    CDB2SQLRESPONSE resp = CDB2__SQLRESPONSE__INIT;
    resp.response_type = RESPONSE_TYPE__COLUMN_VALUES;
    resp.n_value = ncols;
    resp.value = value;
    return newsql_response(clnt, &resp, 0);
}

static int newsql_trace(struct sqlclntstate *clnt, char *info)
{
    CDB2SQLRESPONSE r = CDB2__SQLRESPONSE__INIT;
    r.response_type = RESPONSE_TYPE__SP_TRACE;
    r.info_string = info;
    return newsql_response_int(clnt, &r, RESPONSE_HEADER__SQL_RESPONSE_TRACE, 1);
}

static int newsql_write_response(struct sqlclntstate *c, int t, void *a, int i)
{
    switch (t) {
    case RESPONSE_COLUMNS: return newsql_columns(c, a);
    case RESPONSE_COLUMNS_LUA: return newsql_columns_lua(c, a);
    case RESPONSE_COLUMNS_STR: return newsql_columns_str(c, a, i);
    case RESPONSE_DEBUG: return newsql_debug(c, a);
    case RESPONSE_ERROR: return newsql_error(c, a, i);
    case RESPONSE_ERROR_ACCESS: return newsql_error(c, a, CDB2ERR_ACCESS);
    case RESPONSE_ERROR_BAD_STATE: return newsql_error(c, a, CDB2ERR_BADSTATE);
    case RESPONSE_ERROR_PREPARE: return newsql_error(c, a, CDB2ERR_PREPARE_ERROR);
    case RESPONSE_ERROR_REJECT: return newsql_error(c, a, CDB2ERR_REJECTED);
    case RESPONSE_FLUSH: return newsql_flush(c);
    case RESPONSE_HEARTBEAT: return newsql_heartbeat(c);
    case RESPONSE_ROW: return newsql_row(c, a, i);
    case RESPONSE_ROW_LAST: return newsql_row_last(c);
    case RESPONSE_ROW_LAST_DUMMY: return newsql_row_last_dummy(c);
    case RESPONSE_ROW_LUA: return newsql_row_lua(c, a);
    case RESPONSE_ROW_STR: return newsql_row_str(c, a, i);
    case RESPONSE_TRACE: return newsql_trace(c, a);
    /* fastsql only messages */
    case RESPONSE_COST:
    case RESPONSE_EFFECTS:
    case RESPONSE_ERROR_PREPARE_RETRY: return 0;
    default: abort();
    }
}

static int newsql_ping_pong(struct sqlclntstate *clnt)
{
    struct newsqlheader hdr;
    if (sbuf2fread((void *)&hdr, sizeof(hdr), 1, clnt->sb) != 1) {
        return -1;
    }
    if (ntohl(hdr.type) != RESPONSE_HEADER__SQL_RESPONSE_PONG) {
        return -2;
    }
    return 0;
}

static int newsql_sp_cmd(struct sqlclntstate *clnt, void *cmd, size_t sz)
{
    struct newsqlheader hdr;
    if (sbuf2fread((void *)&hdr, sizeof(hdr), 1, clnt->sb) != 1) {
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
    if (sbuf2fread(buf, len, 1, clnt->sb) != 1) {
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
    switch (t) {
    case RESPONSE_PING_PONG: return newsql_ping_pong(c);
    case RESPONSE_SP_CMD: return newsql_sp_cmd(c, r, e);
    default: abort();
    }
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

/* Process sql query if it is a set command. */
static int process_set_commands(struct dbenv *dbenv, struct sqlclntstate *clnt)
{
    CDB2SQLQUERY *sql_query = NULL;
    int num_commands = 0;
    sql_query = clnt->sql_query;
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
            if (gbl_extended_sql_debug_trace) {
                logmsg(LOGMSG_ERROR,
                       "td %lu %s line %d processing set command '%s'\n",
                       pthread_self(), __func__, __LINE__, sqlstr);
            }
            sqlstr += 3;
            sqlstr = skipws(sqlstr);
            if (strncasecmp(sqlstr, "transaction", 11) == 0) {
                sqlstr += 11;
                sqlstr = skipws(sqlstr);
                clnt->dbtran.mode = TRANLEVEL_INVALID;
                set_high_availability(clnt, 0);
                if (strncasecmp(sqlstr, "read", 4) == 0) {
                    sqlstr += 4;
                    sqlstr = skipws(sqlstr);
                    if (strncasecmp(sqlstr, "committed", 4) == 0) {
                        clnt->dbtran.mode = TRANLEVEL_RECOM;
                    }
                } else if (strncasecmp(sqlstr, "serial", 6) == 0) {
                    clnt->dbtran.mode = TRANLEVEL_SERIAL;
                    if (clnt->hasql_on == 1) {
                        set_high_availability(clnt, 1);
                    }
                } else if (strncasecmp(sqlstr, "blocksql", 7) == 0) {
                    clnt->dbtran.mode = TRANLEVEL_SOSQL;
                } else if (strncasecmp(sqlstr, "snap", 4) == 0) {
                    sqlstr += 4;
                    clnt->dbtran.mode = TRANLEVEL_SNAPISOL;
                    clnt->verify_retries = 0;
                    if (clnt->hasql_on == 1) {
                        set_high_availability(clnt, 1);
                        logmsg(
                            LOGMSG_ERROR,
                            "Enabling snapshot isolation high availability\n");
                    }
                }
                if (clnt->dbtran.mode == TRANLEVEL_INVALID)
                    rc = ii + 1;
            } else if (strncasecmp(sqlstr, "timeout", 7) == 0) {
                sqlstr += 7;
                sqlstr = skipws(sqlstr);
                int timeout = strtol(sqlstr, &endp, 10);
                int notimeout = disable_server_sql_timeouts();
                sbuf2settimeout(clnt->sb, 0, notimeout ? 0 : timeout);
                if (timeout == 0)
                    net_add_watch(clnt->sb, 0, 0);
                else
                    net_add_watch_warning(
                        clnt->sb, bdb_attr_get(dbenv->bdb_attr,
                                               BDB_ATTR_MAX_SQL_IDLE_TIME),
                        notimeout ? 0 : (timeout / 1000), clnt,
                        watcher_warning_function);
            } else if (strncasecmp(sqlstr, "maxquerytime", 12) == 0) {
                sqlstr += 12;
                sqlstr = skipws(sqlstr);
                int timeout = strtol(sqlstr, &endp, 10);
                if (timeout >= 0)
                    clnt->query_timeout = timeout;
            } else if (strncasecmp(sqlstr, "timezone", 8) == 0) {
                sqlstr += 8;
                sqlstr = skipws(sqlstr);
                strncpy(clnt->tzname, sqlstr, sizeof(clnt->tzname));
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
                    if (strlen(sqlstr) >= sizeof(clnt->user)) {
                        snprintf(err, sizeof(err),
                                 "set user: '%s' exceeds %zu characters", sqlstr,
                                 sizeof(clnt->user) - 1);
                        rc = ii + 1;
                    } else {
                        clnt->have_user = 1;
                        strcpy(clnt->user, sqlstr);
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
                    if (strlen(sqlstr) >= sizeof(clnt->password)) {
                        snprintf(err, sizeof(err),
                                 "set password: '%s' exceeds %zu characters",
                                 sqlstr, sizeof(clnt->password) - 1);
                        rc = ii + 1;
                    } else {
                        clnt->have_password = 1;
                        strcpy(clnt->password, sqlstr);
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
                    strncpy(clnt->spname, spname, MAX_SPNAME);
                    clnt->spname[MAX_SPNAME] = '\0';
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
            } else if (strncasecmp(sqlstr, "readonly", 8) == 0) {
                sqlstr += 8;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->is_readonly = 0;
                } else {
                    clnt->is_readonly = 1;
                }
            } else if (strncasecmp(sqlstr, "expert", 6) == 0) {
                sqlstr += 6;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->is_expert = 0;
                } else {
                    clnt->is_expert = 1;
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
                        set_high_availability(clnt, 1);
                        if (gbl_extended_sql_debug_trace) {
                            logmsg(
                                LOGMSG_USER,
                                "td %lu %s line %d setting high_availability\n",
                                pthread_self(), __func__, __LINE__);
                        }
                    }
                } else {
                    clnt->hasql_on = 0;
                    set_high_availability(clnt, 0);
                    if (gbl_extended_sql_debug_trace) {
                        logmsg(LOGMSG_USER,
                               "td %lu %s line %d clearing high_availability\n",
                               pthread_self(), __func__, __LINE__);
                    }
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

                int rc = fdb_access_control_create(clnt, sqlstr);
                if (rc) {
                    logmsg(
                        LOGMSG_ERROR,
                        "%s: failed to process remote access settings \"%s\"\n",
                        __func__, sqlstr);
                }
                rc = ii + 1;
            } else if (strncasecmp(sqlstr, "getcost", 7) == 0) {
                sqlstr += 7;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "on", 2) == 0) {
                    clnt->get_cost = 1;
                } else {
                    clnt->get_cost = 0;
                }
            } else if (strncasecmp(sqlstr, "explain", 7) == 0) {
                sqlstr += 7;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "on", 2) == 0) {
                    clnt->is_explain = 1;
                } else if (strncasecmp(sqlstr, "verbose", 7) == 0) {
                    clnt->is_explain = 2;
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
            } else if (strncasecmp(sqlstr, "plannereffort", 13) == 0) {
                sqlstr += 13;
                int effort = strtol(sqlstr, &endp, 10);
                if (0 < effort && effort <= 10)
                    clnt->planner_effort = effort;
#ifdef DEBUG
                printf("setting clnt->planner_effort to %d\n",
                       clnt->planner_effort);
#endif
            } else if (strncasecmp(sqlstr, "ignorecoherency", 15) == 0) {
                sqlstr += 15;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "on", 2) == 0) {
                    clnt->ignore_coherency = 1;
                } else {
                    clnt->ignore_coherency = 0;
                }
            } else if (strncasecmp(sqlstr, "intransresults", 14) == 0) {
                sqlstr += 14;
                sqlstr = skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->send_intransresults = 0;
                } else {
                    clnt->send_intransresults = 1;
                }
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

static void send_dbinforesponse(struct dbenv *dbenv, SBUF2 *sb)
{
    CDB2DBINFORESPONSE *dbinfo_response = malloc(sizeof(CDB2DBINFORESPONSE));
    cdb2__dbinforesponse__init(dbinfo_response);
    fill_dbinfo(dbinfo_response, dbenv->bdb_env);
    int len = cdb2__dbinforesponse__get_packed_size(dbinfo_response);
    uint8_t *buf, *malloc_buf = NULL;
    if (len > NEWSQL_MAX_RESPONSE_ON_STACK) {
        buf = malloc_buf = malloc(len);
    } else {
        buf = alloca(len);
    }
    cdb2__dbinforesponse__pack(dbinfo_response, buf);
    struct newsqlheader hdr = {0};
    hdr.type = htonl(RESPONSE_HEADER__DBINFO_RESPONSE);
    hdr.length = htonl(len);
    sbuf2write((char *)&hdr, sizeof(hdr), sb);
    sbuf2write((char *)buf, len, sb);
    sbuf2flush(sb);
    free(malloc_buf);
    cdb2__dbinforesponse__free_unpacked(dbinfo_response, &pb_alloc);
}

static int do_query_on_master_check(struct dbenv *dbenv,
                                    struct sqlclntstate *clnt,
                                    CDB2SQLQUERY *sql_query)
{
    int allow_master_exec = 0;
    int allow_master_dbinfo = 0;
    for (int ii = 0; ii < sql_query->n_features; ii++) {
        if (CDB2_CLIENT_FEATURES__ALLOW_MASTER_EXEC ==
            sql_query->features[ii]) {
            allow_master_exec = 1;
        } else if (CDB2_CLIENT_FEATURES__ALLOW_MASTER_DBINFO ==
                   sql_query->features[ii]) {
            allow_master_dbinfo = 1;
        } else if (CDB2_CLIENT_FEATURES__ALLOW_QUEUING ==
                   sql_query->features[ii]) {
            clnt->req.flags |= SQLF_QUEUE_ME;
        }
    }

    int do_master_check;
    if (dbenv->rep_sync == REP_SYNC_NONE || sbuf_is_local(clnt->sb))
        do_master_check = 0;
    else
        do_master_check = 1;

    if (do_master_check && bdb_master_should_reject(dbenv->bdb_env) &&
        allow_master_exec == 0) {
        ATOMIC_ADD(gbl_masterrejects, 1);
        /* Send sql response with dbinfo. */
        if (allow_master_dbinfo)
            send_dbinforesponse(dbenv, clnt->sb);

        logmsg(LOGMSG_DEBUG, "Query on master, will be rejected\n");
        return 1;
    }
    return 0;
}

static CDB2QUERY *read_newsql_query(struct dbenv *dbenv,
                                    struct sqlclntstate *clnt, SBUF2 *sb)
{
    struct newsqlheader hdr;
    int rc;
    int pre_enabled = 0;
    int was_timeout = 0;

retry_read:
    rc = sbuf2fread_timeout((char *)&hdr, sizeof(hdr), 1, sb, &was_timeout);
    if (rc != 1) {
        if (was_timeout) {
            handle_failed_dispatch(clnt, "Socket read timeout.");
        }
        return NULL;
    }

    hdr.type = ntohl(hdr.type);
    hdr.compression = ntohl(hdr.compression);
    hdr.length = ntohl(hdr.length);

    if (hdr.type == CDB2_REQUEST_TYPE__SSLCONN) {
#if WITH_SSL
        /* If client requires SSL and we haven't done that,
           do SSL_accept() now. handle_newsql_request()
           will close the sb if SSL_accept() fails. */

        /* Can't SSL_accept twice - probably a client API logic error.
           Let it disconnect. */
        if (sslio_has_ssl(sb)) {
            logmsg(LOGMSG_WARN, "The connection is already SSL encrypted.\n");
            return NULL;
        }

        /* Flush the SSL ability byte. We need to do this because:
           1) The `require_ssl` field in dbinfo may not reflect the
              actual status of this node;
           2) Doing SSL_accept() immediately would cause too many
              unnecessary EAGAIN/EWOULDBLOCK's for non-blocking BIO. */
        char ssl_able = (gbl_client_ssl_mode >= SSL_ALLOW) ? 'Y' : 'N';
        if ((rc = sbuf2putc(sb, ssl_able)) < 0 || (rc = sbuf2flush(sb)) < 0) {
            logmsg(LOGMSG_DEBUG, "Error in sbuf2putc rc=%d\n", rc);
            return NULL;
        }

        if (ssl_able == 'Y' &&
            sslio_accept(sb, gbl_ssl_ctx, SSL_REQUIRE, NULL, 0) != 1) {
            logmsg(LOGMSG_DEBUG, "Error with sslio_accept\n");
            return NULL;
        }

        if (sslio_verify(sb, gbl_client_ssl_mode, NULL, 0) != 0) {
            newsql_error(clnt, "Client certificate authentication failed.",
                       CDB2ERR_CONNECT_ERROR);
            logmsg(LOGMSG_DEBUG, "sslio_verify returned non zero rc\n");
            return NULL;
        }
#else
        /* Not compiled with SSL. Send back `N' to client and retry read. */
        if ((rc = sbuf2putc(sb, 'N')) < 0 || (rc = sbuf2flush(sb)) < 0) {
            logmsg(LOGMSG_DEBUG, "Error in sbuf2putc rc=%d\n", rc);
            return NULL;
        }
#endif
        goto retry_read;
    } else if (hdr.type == CDB2_REQUEST_TYPE__RESET) { /* Reset from sockpool.*/

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

        reset_clnt(clnt, sb, 0);
        clnt->tzname[0] = '\0';
        clnt->osql.count_changes = 1;
        clnt->heartbeat = 1;
        clnt->dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
        goto retry_read;
    }

    if (hdr.type > 2) {
        logmsg(LOGMSG_ERROR, "%s: Invalid message  %d\n", __func__, hdr.type);
        return NULL;
    }

    int bytes = hdr.length;
    if (bytes <= 0) {
        logmsg(LOGMSG_ERROR, "%s: Junk message  %d\n", __func__, bytes);
        return NULL;
    }

    assert(errno == 0);
    char *p;
    if (bytes <= gbl_blob_sz_thresh_bytes)
        p = malloc(bytes);
    else
        while (1) { // big buffer. most certainly it is a huge blob.
            p = comdb2_timedmalloc(blobmem, bytes, 1000);

            if (p != NULL || errno != ETIMEDOUT)
                break;

            pthread_mutex_lock(&clnt->wait_mutex);
            clnt->heartbeat = 1;
            if (clnt->ready_for_heartbeats == 0) {
                pre_enabled = 1;
                clnt->ready_for_heartbeats = 1;
            }
            newsql_heartbeat(clnt);
            fdb_heartbeats(clnt);
            pthread_mutex_unlock(&clnt->wait_mutex);
        }

    if (pre_enabled) {
        pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;
        pthread_mutex_unlock(&clnt->wait_mutex);
        pre_enabled = 0;
    }

    if (!p) {
        logmsg(LOGMSG_ERROR, "%s: out of memory malloc %d\n", __func__, bytes);
        return NULL;
    }

    rc = sbuf2fread(p, bytes, 1, sb);
    if (rc != 1) {
        free(p);
        logmsg(LOGMSG_DEBUG, "Error in sbuf2fread rc=%d\n", rc);
        return NULL;
    }

    CDB2QUERY *query;
    assert(errno == 0); // precondition for the while loop
    while (1) {
        query = cdb2__query__unpack(&pb_alloc, bytes, (uint8_t *)p);
        // errno can be set by cdb2__query__unpack
        // we retry malloc on out of memory condition

        if (query || errno != ETIMEDOUT)
            break;

        pthread_mutex_lock(&clnt->wait_mutex);
        if (clnt->heartbeat == 0)
            clnt->heartbeat = 1;
        if (clnt->ready_for_heartbeats == 0) {
            pre_enabled = 1;
            clnt->ready_for_heartbeats = 1;
        }
        newsql_heartbeat(clnt);
        fdb_heartbeats(clnt);
        pthread_mutex_unlock(&clnt->wait_mutex);
    }
    free(p);

    if (pre_enabled) {
        pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;
        pthread_mutex_unlock(&clnt->wait_mutex);
    }

    if (!query || errno != 0) {
        return NULL;
    }

    // one of dbinfo or sqlquery must be non-NULL
    if (unlikely(!query->dbinfo && !query->sqlquery)) {
        cdb2__query__free_unpacked(query, &pb_alloc);
        goto retry_read;
    }

    if (query->dbinfo) {
        if (query->dbinfo->has_want_effects &&
            query->dbinfo->want_effects == 1) {
            CDB2SQLRESPONSE sql_response = CDB2__SQLRESPONSE__INIT;
            CDB2EFFECTS effects = CDB2__EFFECTS__INIT;
            sql_response.response_type =
                RESPONSE_TYPE__COMDB2_INFO; /* Last Row. */
            sql_response.n_value = 0;
            if (clnt->verifyretry_off == 1 ||
                clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
                clnt->dbtran.mode == TRANLEVEL_SERIAL) {
                clnt->effects.num_affected = clnt->effects.num_updated +
                                             clnt->effects.num_deleted +
                                             clnt->effects.num_inserted;
                effects.num_affected = clnt->effects.num_affected;
                effects.num_selected = clnt->effects.num_selected;
                effects.num_updated = clnt->effects.num_updated;
                effects.num_deleted = clnt->effects.num_deleted;
                effects.num_inserted = clnt->effects.num_inserted;
                sql_response.effects = &effects;
                sql_response.error_code = 0;
            } else {
                sql_response.error_code = -1;
                sql_response.error_string = "Get effects not supported in "
                                            "transaction with verifyretry on";
            }
            newsql_response_int(clnt, &sql_response, RESPONSE_HEADER__SQL_EFFECTS, 1);
        } else {
            send_dbinforesponse(dbenv, sb);
        }
        cdb2__query__free_unpacked(query, &pb_alloc);
        goto retry_read;
    }

#if WITH_SSL
    /* Do security check before we return. We do it only after
       the query has been unpacked so that we know whether
       it is a new client (new clients have SSL feature).
       The check must be done for every query, otherwise
       attackers could bypass it by using pooled connections
       from sockpool. The overhead of the check is negligible. */
    if (gbl_client_ssl_mode >= SSL_REQUIRE && !sslio_has_ssl(sb)) {
        /* The code block does 2 things:
           1. Return an error to outdated clients;
           2. Send dbinfo to new clients to trigger SSL.
              It may happen when require_ssl is first time
              enabled across the cluster. */
        int client_supports_ssl = 0;
        for (int ii = 0; ii < query->sqlquery->n_features; ++ii) {
            if (CDB2_CLIENT_FEATURES__SSL == query->sqlquery->features[ii]) {
                client_supports_ssl = 1;
                break;
            }
        }

        if (client_supports_ssl) {
            newsql_send_hdr(clnt, RESPONSE_HEADER__SQL_RESPONSE_SSL);
            cdb2__query__free_unpacked(query, &pb_alloc);
            goto retry_read;
        } else {
            newsql_error(clnt, "The database requires SSL connections.",
                       CDB2ERR_CONNECT_ERROR);
        }
        cdb2__query__free_unpacked(query, &pb_alloc);
        logmsg(LOGMSG_DEBUG, "SSL is required by db, client doesnt support\n");
        return NULL;
    }
#endif
    return query;
}

extern int gbl_allow_incoherent_sql;

static int handle_newsql_request(comdb2_appsock_arg_t *arg)
{
    int rc = 0;
    struct sqlclntstate clnt;
    struct thr_handle *thr_self;
    struct sbuf2 *sb;
    struct dbenv *dbenv;
    struct dbtable *tab;
    char *cmdline;

    thr_self = arg->thr_self;
    dbenv = arg->dbenv;
    tab = arg->tab;
    sb = arg->sb;
    cmdline = arg->cmdline;

    if (arg->keepsocket)
        *arg->keepsocket = 1;

    if (tab->dbtype != DBTYPE_TAGGED_TABLE) {
        /*
          Don't change this message. The sql api recognises the first four
          characters (Erro) and can respond gracefully.
        */
        sbuf2printf(sb, "Error: newsql is only supported for tagged DBs\n");
        logmsg(LOGMSG_ERROR,
               "Error: newsql is only supported for tagged DBs\n");
        sbuf2flush(sb);
        return APPSOCK_RETURN_ERR;
    }

    if (!bdb_am_i_coherent(dbenv->bdb_env) && !gbl_allow_incoherent_sql) {
        return APPSOCK_RETURN_OK;
    }

    /* There are points when we can't accept any more connections. */
    if (dbenv->no_more_sql_connections) {
        return APPSOCK_RETURN_OK;
    }

    /*
      If we are NOT the master, and the db is set up for async replication, we
      should return an error at this point rather than proceed with potentially
      incoherent data.
    */
    if (dbenv->rep_sync == REP_SYNC_NONE && dbenv->master != gbl_mynode) {
        return APPSOCK_RETURN_OK;
    }

    /*
      New way. Do the basic socket I/O in line in this thread (which has a very
      small stack); the handle_fastsql_requests function will dispatch to a
      pooled sql engine for performing queries.
    */
    thrman_change_type(thr_self, THRTYPE_APPSOCK_SQL);

    reset_clnt(&clnt, sb, 1);
    clnt.write_response = newsql_write_response;
    clnt.read_response = newsql_read_response;
    clnt.tzname[0] = '\0';
    clnt.is_newsql = 1;

    pthread_mutex_init(&clnt.wait_mutex, NULL);
    pthread_cond_init(&clnt.wait_cond, NULL);
    pthread_mutex_init(&clnt.write_lock, NULL);
    pthread_mutex_init(&clnt.dtran_mtx, NULL);

    if (active_appsock_conns >
        bdb_attr_get(dbenv->bdb_attr, BDB_ATTR_MAXAPPSOCKSLIMIT)) {
        logmsg(LOGMSG_WARN,
               "%s: Exhausted appsock connections, total %d connections \n",
               __func__, active_appsock_conns);
        newsql_error(&clnt, "Exhausted appsock connections.",
                   CDB2__ERROR_CODE__APPSOCK_LIMIT);
        goto done;
    }

    extern int gbl_allow_incoherent_sql;
    if (!gbl_allow_incoherent_sql && !bdb_am_i_coherent(thedb->bdb_env)) {
        logmsg(LOGMSG_ERROR,
               "%s:%d td %u new query on incoherent node, dropping socket\n",
               __func__, __LINE__, (uint32_t)pthread_self());
        goto done;
    }

    CDB2QUERY *query = read_newsql_query(dbenv, &clnt, sb);
    if (query == NULL) {
        logmsg(LOGMSG_DEBUG, "Query on master, will be rejected\n");
        goto done;
    }
    assert(query->sqlquery);
    newsql_appdata(&clnt, 32);

    CDB2SQLQUERY *sql_query = query->sqlquery;
    clnt.query = query;

    if (do_query_on_master_check(dbenv, &clnt, sql_query))
        goto done;

    clnt.osql.count_changes = 1;
    clnt.dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
    set_high_availability(&clnt, 0);

    int notimeout = disable_server_sql_timeouts();
    sbuf2settimeout(
        sb, bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAX_SQL_IDLE_TIME) * 1000,
        notimeout ? 0 : gbl_sqlwrtimeoutms);

    sbuf2flush(sb);
    net_set_writefn(sb, sql_writer);

    int wrtimeoutsec;
    if (gbl_sqlwrtimeoutms == 0 || notimeout)
        wrtimeoutsec = 0;
    else
        wrtimeoutsec = gbl_sqlwrtimeoutms / 1000;

    net_add_watch_warning(
        sb, bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAX_SQL_IDLE_TIME),
        wrtimeoutsec, &clnt, watcher_warning_function);

    /* appsock threads aren't sql threads so for appsock pool threads
     * sqlthd will be NULL */
    struct sql_thread *sqlthd = pthread_getspecific(query_info_key);
    if (sqlthd) {
        bzero(&sqlthd->clnt->conn, sizeof(struct conninfo));
        sqlthd->clnt->origin[0] = 0;
    }

    while (query) {
        assert(query->sqlquery);
        struct newsql_appdata *appdata = clnt.appdata;
        appdata->query = query->sqlquery;
        clnt.sql_query = query->sqlquery;
             sql_query = query->sqlquery;
        clnt.sql = sql_query->sql_query;
        clnt.query = query;
        clnt.added_to_hist = 0;
        logmsg(LOGMSG_DEBUG, "Query '%s'\n", sql_query->sql_query);

        if (!clnt.in_client_trans) {
            bzero(&clnt.effects, sizeof(clnt.effects));
            bzero(&clnt.log_effects, sizeof(clnt.log_effects));
            clnt.trans_has_sp = 0;
        }
        clnt.is_newsql = 1;
        if (clnt.dbtran.mode < TRANLEVEL_SOSQL) {
            clnt.dbtran.mode = TRANLEVEL_SOSQL;
        }
        clnt.osql.sent_column_data = 0;
        clnt.stop_this_statement = 0;

        if ((clnt.tzname[0] == '\0') && sql_query->tzname)
            strncpy(clnt.tzname, sql_query->tzname, sizeof(clnt.tzname));

        if (sql_query->dbname && dbenv->envname &&
            strcasecmp(sql_query->dbname, dbenv->envname)) {
            char errstr[64 + (2 * MAX_DBNAME_LENGTH)];
            snprintf(errstr, sizeof(errstr),
                     "DB name mismatch query:%s actual:%s", sql_query->dbname,
                     dbenv->envname);
            logmsg(LOGMSG_ERROR, "%s\n", errstr);
            newsql_error(&clnt, errstr, CDB2__ERROR_CODE__WRONG_DB);
            goto done;
        }

        if (sql_query->client_info) {
            if (clnt.rawnodestats) {
                release_node_stats(clnt.argv0, clnt.stack, clnt.origin);
                clnt.rawnodestats = NULL;
            }
            if (clnt.conninfo.pid &&
                clnt.conninfo.pid != sql_query->client_info->pid) {
                /* Different pid is coming without reset. */
                logmsg(LOGMSG_WARN,
                       "Multiple processes using same socket PID 1 %d "
                       "PID 2 %d Host %.8x\n",
                       clnt.conninfo.pid, sql_query->client_info->pid,
                       sql_query->client_info->host_id);
            }
            clnt.conninfo.pid = sql_query->client_info->pid;
            clnt.conninfo.node = sql_query->client_info->host_id;
            if (clnt.argv0) {
                free(clnt.argv0);
                clnt.argv0 = NULL;
            }
            if (clnt.stack) {
                free(clnt.stack);
                clnt.stack = NULL;
            }
            if (sql_query->client_info->argv0) {
                clnt.argv0 = strdup(sql_query->client_info->argv0);
            }
            if (sql_query->client_info->stack) {
                clnt.stack = strdup(sql_query->client_info->stack);
            }
        }
        if (clnt.rawnodestats == NULL) {
            clnt.rawnodestats = get_raw_node_stats(
                clnt.argv0, clnt.stack, clnt.origin, sbuf2fileno(clnt.sb));
        }

        if (process_set_commands(dbenv, &clnt))
            goto done;

        if (gbl_rowlocks && clnt.dbtran.mode != TRANLEVEL_SERIAL)
            clnt.dbtran.mode = TRANLEVEL_SNAPISOL;

        /* avoid new accepting new queries/transaction on opened connections
           if we are incoherent (and not in a transaction). */
        if (clnt.ignore_coherency == 0 && !bdb_am_i_coherent(thedb->bdb_env) &&
            (clnt.ctrl_sqlengine == SQLENG_NORMAL_PROCESS)) {
            logmsg(LOGMSG_ERROR,
                   "%s line %d td %u new query on incoherent node, "
                   "dropping socket\n",
                   __func__, __LINE__, (uint32_t)pthread_self());
            goto done;
        }

        clnt.heartbeat = 1;

        if (clnt.had_errors && strncasecmp(clnt.sql, "commit", 6) &&
            strncasecmp(clnt.sql, "rollback", 8)) {
            if (clnt.in_client_trans == 0) {
                clnt.had_errors = 0;
                /* tell blobmem that I want my priority back
                   when the sql thread is done */
                comdb2bma_pass_priority_back(blobmem);
                rc = dispatch_sql_query(&clnt);
            } else {
                /* Do Nothing */
                newsql_heartbeat(&clnt);
            }
        } else if (clnt.had_errors) {
            /* Do Nothing */
            if (clnt.ctrl_sqlengine == SQLENG_STRT_STATE)
                clnt.ctrl_sqlengine = SQLENG_NORMAL_PROCESS;

            clnt.had_errors = 0;
            clnt.in_client_trans = 0;
            rc = -1;
        } else {
            /* tell blobmem that I want my priority back
               when the sql thread is done */
            comdb2bma_pass_priority_back(blobmem);
            rc = dispatch_sql_query(&clnt);
        }

        if (clnt.osql.replay == OSQL_RETRY_DO) {
            if (clnt.trans_has_sp == 0) {
                srs_tran_replay(&clnt, arg->thr_self);
            } else {
                osql_set_replay(__FILE__, __LINE__, &clnt, OSQL_RETRY_NONE);
                srs_tran_destroy(&clnt);
            }
        } else {
            /* if this transaction is done (marked by SQLENG_NORMAL_PROCESS),
               clean transaction sql history
            */
            if (clnt.osql.history &&
                clnt.ctrl_sqlengine == SQLENG_NORMAL_PROCESS)
                srs_tran_destroy(&clnt);
        }

        if (rc && !clnt.in_client_trans)
            goto done;

        pthread_mutex_lock(&clnt.wait_mutex);
        if (clnt.query) {
            if (clnt.added_to_hist == 1) {
                clnt.query = NULL;
            } else {
                cdb2__query__free_unpacked(clnt.query, &pb_alloc);
                clnt.query = NULL;
            }
        }
        pthread_mutex_unlock(&clnt.wait_mutex);

        query = read_newsql_query(dbenv, &clnt, sb);
    }

done:
    if (clnt.ctrl_sqlengine == SQLENG_INTRANS_STATE) {
        handle_sql_intrans_unrecoverable_error(&clnt);
    }

    if (clnt.rawnodestats) {
        release_node_stats(clnt.argv0, clnt.stack, clnt.origin);
        clnt.rawnodestats = NULL;
    }

    if (clnt.argv0) {
        free(clnt.argv0);
        clnt.argv0 = NULL;
    }
    if (clnt.stack) {
        free(clnt.stack);
        clnt.stack = NULL;
    }

    close_sp(&clnt);
    osql_clean_sqlclntstate(&clnt);

    if (clnt.dbglog) {
        sbuf2close(clnt.dbglog);
        clnt.dbglog = NULL;
    }

    if (clnt.query) {
        if (clnt.added_to_hist == 1) {
            clnt.query = NULL;
        } else {
            cdb2__query__free_unpacked(clnt.query, &pb_alloc);
            clnt.query = NULL;
        }
    }

    free_newsql_appdata(&clnt);

    /* XXX free logical tran?  */
    close_appsock(sb);
    cleanup_clnt(&clnt);

    pthread_mutex_destroy(&clnt.wait_mutex);
    pthread_cond_destroy(&clnt.wait_cond);
    pthread_mutex_destroy(&clnt.write_lock);
    pthread_mutex_destroy(&clnt.dtran_mtx);

    return APPSOCK_RETURN_OK;
}

comdb2_appsock_t newsql_plugin = {
    "newsql",             /* Name */
    "",                   /* Usage info */
    0,                    /* Execution count */
    APPSOCK_FLAG_IS_SQL,  /* Flags */
    handle_newsql_request /* Handler function */
};

#include "plugin.h"
