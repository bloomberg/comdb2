#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdarg.h>

#include "sqlite3.h"
#include <cdb2api.h>
#include "strbuf.h"
#include "ezsystables.h"
#include "types.h"

/* This tries to make it easier to add system tables.  There's usually lots of boilerplate
 * code.  A common case though is that you have an array of structures that you want to
 * emit.   create_system_table lets you specify a table name, the size of the structure,
 * and a list of fields and types.  It takes care of the boilerplate. */

enum {
    FIELD_TYPE_MASK = 0x0fff
};

struct sysfield {
    char *name;
    cdb2_coltype type;
    int offset;
};

struct systable {
    char *name;
    int nfields;
    struct sysfield *fields;
    int size;
    int (*init)(void **data, int *npoints);
    void (*release)(void *data, int npoints);
};

struct ez_systable_vtab {
    sqlite3_vtab base;
    struct systable *t;
};
typedef struct ez_systable_vtab ez_systable_vtab;

struct ez_systable_cursor {
    sqlite3_vtab_cursor base;
    struct systable *t;
    int64_t rowid;
    void *data;
    int npoints;
};
typedef struct ez_systable_cursor ez_systable_cursor;

static int systbl_connect(
  sqlite3 *db,
  void *pAux,
  int argc,
  const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pErr
){
    struct systable *t = (struct systable*) pAux;
    strbuf *sql = strbuf_new();

    strbuf_append(sql, "CREATE TABLE \""); 
    strbuf_append(sql, t->name);
    strbuf_append(sql, "\"(");
    char *comma = "";

    for (int i = 0; i < t->nfields; i++) {
        strbuf_appendf(sql, "%s\"%s\"", comma, t->fields[i].name);
        comma = ", ";
    }
    strbuf_append(sql, ");");
    int rc = sqlite3_declare_vtab(db, strbuf_buf(sql));
    strbuf_free(sql);
    if (rc == SQLITE_OK) {
        ez_systable_vtab *vtab = calloc(1, sizeof(ez_systable_vtab));
        vtab->t = pAux;
        *ppVtab = (sqlite3_vtab*) vtab;
    }
    return rc;
}

static int systbl_best_index(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}

static int systbl_disconnect(sqlite3_vtab *pVtab){
    ez_systable_vtab *vtab = (ez_systable_vtab *) pVtab;
    struct systable *t = vtab->t;
    for (int i = 0; i < t->nfields; i++)
        free(t->fields[i].name);
    free(t->fields);
    free(t->name);
    free(t);
    free(pVtab);
  return SQLITE_OK;
}

static int systbl_open(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
    struct ez_systable_cursor *pCur = calloc(1, sizeof(struct ez_systable_cursor));
    struct ez_systable_vtab *vtab = (struct ez_systable_vtab*) p;
    struct systable *t = vtab->t;
    int rc;
    pCur->rowid = 0;
    pCur->t = t;
    rc = t->init(&pCur->data, &pCur->npoints);
    *ppCursor = (sqlite3_vtab_cursor*) pCur;
    return rc;
}

static int systbl_close(sqlite3_vtab_cursor *cur){
    struct ez_systable_cursor *pCur = (struct ez_systable_cursor*) cur;
    struct systable *t = pCur->t;
    t->release(pCur->data, pCur->npoints);
    free(pCur);
    return SQLITE_OK;
}

static int systbl_next(sqlite3_vtab_cursor *cur){
    struct ez_systable_cursor *pCur = (struct ez_systable_cursor*) cur;
    pCur->rowid++;
    return SQLITE_OK;
}

static void* get_field_ptr(struct systable *t, char *rec, int column) {
    void *out;

    switch (t->fields[column].type & FIELD_TYPE_MASK) {
        case CDB2_INTEGER:
        case CDB2_REAL:
        case CDB2_DATETIME:
        case CDB2_INTERVALYM:
        case CDB2_INTERVALDS:
        case CDB2_DATETIMEUS:
        case CDB2_INTERVALDSUS:
        case CDB2_BLOB:
            if (t->fields[column].type & SYSTABLE_FIELD_NULLABLE)
                out = *(void**) &rec[t->fields[column].offset];
            else
                out = &rec[t->fields[column].offset];
            break; 

        case CDB2_CSTRING:
            out = *(void**) &rec[t->fields[column].offset];
            break;
    }
    return out;
}

static int systbl_column(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
    struct ez_systable_cursor *pCur = (struct ez_systable_cursor*) cur;
    struct systable *t = pCur->t;
    int rc = SQLITE_OK;

    char *rec = &((char*) pCur->data)[pCur->rowid * t->size];
    void *field = get_field_ptr(t, rec, i);

    if (field == NULL) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    switch (t->fields[i].type & FIELD_TYPE_MASK) {
        case CDB2_INTEGER: {
            int64_t *ival = (int64_t*) field;
            sqlite3_result_int64(ctx, *ival);
            break;
        }
        case CDB2_REAL: {
            double *dval = (double*) field;
            sqlite3_result_double(ctx, *dval);
            break;
        }
        case CDB2_CSTRING: {
            char *strval = *((char**)(rec + t->fields[i].offset));
            sqlite3_result_text(ctx, strval, -1, NULL);
            break;
        }
        case CDB2_BLOB: {
            systable_blobtype *blob = (systable_blobtype*) field;
            sqlite3_result_blob(ctx, blob->value, blob->size, NULL);
            break;
        }
        case CDB2_DATETIME:
        case CDB2_DATETIMEUS: {
            dttz_t dtz;
            int le = 1;
            char tz[CDB2_MAX_TZNAME];
#ifndef _LINUX_SOURCE
            le = 0;
#endif
            if ((t->fields[i].type & FIELD_TYPE_MASK) == CDB2_DATETIME) {
                cdb2_client_datetime_t *dt = (cdb2_client_datetime_t*) field;
                rc = client_datetime_to_dttz(dt, tz, &dtz, le);
            }
            else {
                cdb2_client_datetimeus_t *dt = (cdb2_client_datetimeus_t*) field;
                rc = client_datetimeus_to_dttz(dt, tz, &dtz, le);
            }

            if (rc == 0)
                sqlite3_result_datetime(ctx, &dtz, tz);
            break;
        }
		case CDB2_INTERVALYM: {
			intv_t interval;
			cdb2_client_intv_ym_t *intv = (cdb2_client_intv_ym_t*) field;
            interval.type = INTV_YM_TYPE;
            interval.sign = intv->sign;
            interval.u.ym.years = intv->years;
            interval.u.ym.months = intv->months;
            sqlite3_result_interval(ctx, &interval);
            break;
		}

		case CDB2_INTERVALDS: {
			intv_t interval;
			cdb2_client_intv_ds_t *intv = (cdb2_client_intv_ds_t*) field;
            interval.type = INTV_DS_TYPE;
            interval.sign = intv->sign;
            interval.u.ds.days = intv->days;
            interval.u.ds.hours = intv->hours;
            interval.u.ds.mins = intv->mins;
            interval.u.ds.sec = intv->sec;
            interval.u.ds.frac = intv->msec;
            interval.u.ds.prec = DTTZ_PREC_MSEC;
            sqlite3_result_interval(ctx, &interval);
            break;
		}

		case CDB2_INTERVALDSUS: {
			intv_t interval;
			cdb2_client_intv_dsus_t *intv = (cdb2_client_intv_dsus_t*) field;
            interval.type = INTV_DS_TYPE;
            interval.sign = intv->sign;
            interval.u.ds.days = intv->days;
            interval.u.ds.hours = intv->hours;
            interval.u.ds.mins = intv->mins;
            interval.u.ds.sec = intv->sec;
            interval.u.ds.frac = intv->usec;
            interval.u.ds.prec = DTTZ_PREC_USEC;
            sqlite3_result_interval(ctx, &interval);
            break;
		}


    }

    return rc;
};

static int systbl_rowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
    struct ez_systable_cursor *pCur = (struct ez_systable_cursor*) cur;
    *pRowid = pCur->rowid;
    return SQLITE_OK;
}


static int systbl_filter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  return SQLITE_OK;
}


static int systbl_eof(sqlite3_vtab_cursor *cur){
    struct ez_systable_cursor *pCur = (struct ez_systable_cursor*) cur;
    return pCur->rowid >= pCur->npoints;
}

static const sqlite3_module systbl = {
    .xConnect = systbl_connect,
    .xBestIndex = systbl_best_index,
    .xDisconnect = systbl_disconnect,
    .xOpen = systbl_open,
    .xClose = systbl_close,
    .xFilter = systbl_filter,
    .xNext = systbl_next,
    .xEof = systbl_eof,
    .xColumn = systbl_column,
    .xRowid = systbl_rowid,

};

int create_system_table(sqlite3 *db, char *name, 
        int(*init_callback)(void **data, int *npoints), 
        void(*release_callback)(void *data, int npoints), 
        size_t struct_size, ...) {
    struct systable *sys;

    sys = malloc(sizeof(struct systable));
    sys->name = strdup(name);
    sys->size = struct_size;
    sys->nfields = 0;
    sys->init = init_callback;
    sys->release = release_callback;
    sys->fields = NULL;

    va_list args;
    va_start(args, struct_size);

    int nalloc = 0;

    int type = va_arg(args, int);
    while (type != SYSTABLE_END_OF_FIELDS) {
        char *name = va_arg(args, char*);
        int offset = va_arg(args, size_t);

        if (sys->nfields >= nalloc) {
            nalloc = nalloc * 2 + 10;
            sys->fields = realloc(sys->fields, nalloc * sizeof(struct sysfield));
        }

        sys->fields[sys->nfields].name = strdup(name);
        sys->fields[sys->nfields].type = type;
        sys->fields[sys->nfields++].offset = offset;

        type = va_arg(args, int);
    }

    int rc = sqlite3_create_module(db, name, &systbl, sys);
    if (rc) {
        fprintf(stderr, "create rc %d %s\n", rc, sqlite3_errmsg(db));
        return rc;
    }

    va_end(args);

    return 0;
}
