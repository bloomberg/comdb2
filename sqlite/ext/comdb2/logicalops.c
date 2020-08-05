/*
   Copyright 2018-2020 Bloomberg Finance L.P.

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

#include <stdarg.h>
#include "sqlite3ext.h"
#include <assert.h>
#include <string.h>
#include "comdb2.h"
#include "sql.h"
#include "build/db.h"
#include "build/db_int.h"
#include "dbinc/db_swap.h"
#include "dbinc/txn.h"
#include "dbinc_auto/txn_ext.h"
#include "dbinc_auto/txn_auto.h"
#include "bdb_osql_log_rec.h"
#include "llog_auto.h"
#include "bdb_osqllog.h"
#include "util.h"
#include <bdb/bdb_int.h>
#include "llog_ext.h"
#include "comdb2systbl.h"

/* Allocate maximum for unpacking */
#define PACKED_MEMORY_SIZE (MAXBLOBLENGTH + 7)

/* Column numbers */
#define LOGICALOPS_COLUMN_START        0
#define LOGICALOPS_COLUMN_STOP         1
#define LOGICALOPS_COLUMN_COMMITLSN    2
#define LOGICALOPS_COLUMN_OPNUM        3
#define LOGICALOPS_COLUMN_OPERATION    4
#define LOGICALOPS_COLUMN_TABLE        5
#define LOGICALOPS_COLUMN_OLDGENID     6
#define LOGICALOPS_COLUMN_OLDRECORD    7
#define LOGICALOPS_COLUMN_GENID        8
#define LOGICALOPS_COLUMN_RECORD       9

/* Dynamically reallocating string type */
typedef struct dynstr {
    char *buf;
    int len;
    int alloced;
} dynstr_t;

/* Modeled after generate_series */
typedef struct logicalops_cursor logicalops_cursor;
struct logicalops_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
  bdb_llog_cursor llog_cur;
  char *minLsnStr;
  char *maxLsnStr;
  char *curLsnStr;
  char *tz;
  char *table;
  void *packedprev;
  void *unpackedprev;
  void *packed;
  void *unpacked;
  strbuf *jsonrec;
  strbuf *oldjsonrec;
  struct dbtable *db;
  struct odh odh;
  struct odh oldodh;
  void *record;
  void *oldrecord;
  char opstring[32];
  char oldgenid[32];
  char genid[32];
  int reclen;
  int oldreclen;
};

static int logicalopsConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  sqlite3_vtab *pNew;
  int rc;
  rc = sqlite3_declare_vtab(db,
     "CREATE TABLE x(minlsn hidden,maxlsn hidden,commitlsn,opnum,operation,tablename,oldgenid,oldrecord,genid,record)");
  if( rc==SQLITE_OK ){
    pNew = *ppVtab = sqlite3_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }

  return rc;
}

static int logicalopsDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int logicalopsOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  logicalops_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  struct sql_thread *thd = pthread_getspecific(query_info_key);
  if (thd && thd->clnt)
      pCur->tz = thd->clnt->tzname;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

static int logicalopsClose(sqlite3_vtab_cursor *cur){
  logicalops_cursor *pCur = (logicalops_cursor*)cur;
  bdb_llog_cursor_close(&pCur->llog_cur);
  if (pCur->minLsnStr)
      sqlite3_free(pCur->minLsnStr);
  if (pCur->maxLsnStr)
      sqlite3_free(pCur->maxLsnStr);
  if (pCur->curLsnStr)
      sqlite3_free(pCur->curLsnStr);
  if (pCur->packed)
      sqlite3_free(pCur->packed);
  if (pCur->unpacked)
      sqlite3_free(pCur->unpacked);
  if (pCur->packedprev)
      sqlite3_free(pCur->packedprev);
  if (pCur->unpackedprev)
      sqlite3_free(pCur->unpackedprev);
  if (pCur->jsonrec)
      strbuf_free(pCur->jsonrec);
  if (pCur->oldjsonrec)
      strbuf_free(pCur->oldjsonrec);
  if (pCur->table)
      free(pCur->table);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static void *retrieve_packed_memory_prev(logicalops_cursor *pCur)
{
    if (pCur->packedprev == NULL) {
        pCur->packedprev = sqlite3_malloc(PACKED_MEMORY_SIZE);
    }
    return pCur->packedprev;
}

static void *retrieve_packed_memory(logicalops_cursor *pCur)
{
    if (pCur->packed == NULL) {
        pCur->packed = sqlite3_malloc(PACKED_MEMORY_SIZE);
    }
    return pCur->packed;
}

static void *retrieve_unpacked_memory(logicalops_cursor *pCur)
{
    if (pCur->unpacked == NULL) {
        pCur->unpacked = sqlite3_malloc(MAXRECSZ);
    }
    return pCur->unpacked;
}

static void *retrieve_unpacked_memory_prev(logicalops_cursor *pCur)
{
    if (pCur->unpackedprev == NULL) {
        pCur->unpackedprev = sqlite3_malloc(MAXRECSZ);
    }
    return pCur->unpackedprev;
}

static int decompress_and_upgrade(logicalops_cursor *pCur, char *table,
        void *dta, int dtalen, int isrec, char *unpackedbuf, struct odh *odh,
        void **record, int *reclen)
{
    int rc;

    if ((rc = bdb_unpack(pCur->db->handle, dta, dtalen, unpackedbuf, MAXRECSZ,
                    odh, NULL)) != 0) {
        logmsg(LOGMSG_ERROR, "%s line %d error unpacking buf %d\n", __func__,
                __LINE__, rc);
        return SQLITE_INTERNAL;
    }

    *record = odh->recptr;

    if (isrec && (rc = vtag_to_ondisk_vermap(pCur->db, *record, reclen,
                    odh->csc2vers)) <= 0) {
        logmsg(LOGMSG_ERROR, "%s line %d vtag-to-ondisk error %d\n", __func__,
                __LINE__, rc);
        return SQLITE_INTERNAL;
    } else {
        *reclen = odh->length;
    }

    return 0;
}

static int json_blob(char *buf, int len, struct schema *sc, int ix,
        strbuf *ds) 
{
    struct field *f;
    assert(ix < sc->nmembers && ix >= 0);
    strbuf_appendf(ds, "{");
    f = &sc->member[ix];
    strbuf_appendf(ds, "\"%s\":", f->name);
    if (buf) {
        strbuf_appendf(ds, "\"x");
        strbuf_hex(ds, (void *)buf, len);
        strbuf_appendf(ds, "\"");
    } else {
        strbuf_appendf(ds, "NULL");
    }
    strbuf_appendf(ds, "}");
    return 0;
}

extern pthread_key_t query_info_key;

/* Copied & revised from printrecord */
static int json_record(logicalops_cursor *pCur, char *buf, int len,
        struct schema *sc, strbuf *ds) 
{
    int field;
    struct field *f;
    struct field_conv_opts_tz tzopts = {0};
    struct field_conv_opts *opts = (struct field_conv_opts *)&tzopts;

    unsigned long long uival;
    long long ival;
    char *sval = NULL;
    double dval;
    int printed = 0;
    int null = 0;
    int flen = 0;
    void *in;

#ifdef _LINUX_SOURCE
    tzopts.flags |= FLD_CONV_LENDIAN;
#endif
    tzopts.flags |= FLD_CONV_TZONE; 
    if (pCur->tz)
        snprintf(tzopts.tzname, sizeof(tzopts.tzname), "%s", pCur->tz);
    else
        snprintf(tzopts.tzname, sizeof(tzopts.tzname), "US/Eastern");

    strbuf_appendf(ds, "{");
    for (field = 0; field < sc->nmembers; field++) {
        int outdtsz = 0;
        null = 0;
        f = &sc->member[field];
        flen = f->len;
        if (len) {
            if (f->offset + f->len >= len) {
                /* ignore partial integer/real fields - shouldn't happen */
                flen = len - f->offset;
                if (flen == 0)
                    break;
            }
        }
        switch (f->type) {
        case SERVER_UINT:
            SERVER_UINT_to_CLIENT_UINT(
                buf + f->offset, flen, opts /*convopts*/, NULL /*blob*/,
                &uival, 8, &null, &outdtsz, opts /*convopts*/, NULL /*blob*/);
            if (printed)
                strbuf_appendf(ds, ",");
            if (null)
                strbuf_appendf(ds, "\"%s\":NULL", f->name);
            else
                strbuf_appendf(ds, "\"%s\"=%llu", f->name, uival);
            printed=1;
            break;
        case SERVER_BINT:
            SERVER_BINT_to_CLIENT_INT(
                buf + f->offset, flen, opts /*convopts*/, NULL /*blob*/, &ival,
                8, &null, &outdtsz, opts /*convopts*/, NULL /*blob*/);
            if (printed)
                strbuf_appendf(ds, ",");
            if (null)
                strbuf_appendf(ds, "\"%s\":NULL", f->name);
            else
                strbuf_appendf(ds, "\"%s\":%lld", f->name, ival);
            printed=1;
            break;
        case SERVER_BREAL:
            SERVER_BREAL_to_CLIENT_REAL(
                buf + f->offset, flen, opts /*convopts*/, NULL /*blob*/, &dval,
                8, &null, &outdtsz, opts /*convopts*/, NULL /*blob*/);
            if (printed)
                strbuf_appendf(ds, ",");
            if (null)
                strbuf_appendf(ds, "\"%s\":NULL", f->name);
            else
                strbuf_appendf(ds, "\"%s\":%f", f->name, dval);
            printed=1;
            break;
        case SERVER_BCSTR:
            sval = realloc(sval, flen + 1);
            SERVER_BCSTR_to_CLIENT_CSTR(
                buf + f->offset, flen, opts /*convopts*/, NULL /*blob*/, sval,
                flen + 1, &null, &outdtsz, opts /*convopts*/, NULL /*blob*/);
            if (printed)
                strbuf_appendf(ds, ",");
            if (null)
                strbuf_appendf(ds, "\"%s\":NULL", f->name);
            else
                strbuf_appendf(ds, "\"%s\":\"%s\"", f->name, sval);
            printed=1;
            break;
        case SERVER_BYTEARRAY:
            in = (buf + f->offset);
            if (printed)
                strbuf_appendf(ds, ",");
            if (null)
                strbuf_appendf(ds, "\"%s\":NULL", f->name);
            else {
                strbuf_appendf(ds, "\"%s\":x", f->name);
                strbuf_hex(ds, in + 1, flen - 1);
            }
            printed=1;
            break;
        case SERVER_DATETIME:
            sval = realloc(sval,100);
            SERVER_DATETIME_to_CLIENT_CSTR(
                buf + f->offset, flen, opts /*convopts*/, NULL /*blob*/, sval,
                100, &null, &outdtsz, opts /*convopts*/, NULL /*blob*/);
            if (printed)
                strbuf_appendf(ds, ",");
            if (null)
                strbuf_appendf(ds, "\"%s\":NULL", f->name);
            else {
                strbuf_appendf(ds, "\"%s\":\"%s\"", f->name, sval);
            }
            printed=1;
            break;
        case SERVER_INTVYM:
            sval = realloc(sval,100);
            SERVER_INTVYM_to_CLIENT_CSTR(
                buf + f->offset, flen, opts /*convopts*/, NULL /*blob*/, sval,
                100, &null, &outdtsz, opts /*convopts*/, NULL /*blob*/);
            if (printed)
                strbuf_appendf(ds, ",");
            if (null)
                strbuf_appendf(ds, "\"%s\":NULL", f->name);
            else {
                strbuf_appendf(ds, "\"%s\":\"%s\"", f->name, sval);
            }
            printed=1;
            break;
        case SERVER_DECIMAL:
            sval = realloc(sval,100);
            SERVER_DECIMAL_to_CLIENT_CSTR(
                buf + f->offset, flen, opts /*convopts*/, NULL /*blob*/, sval,
                100, &null, &outdtsz, opts /*convopts*/, NULL /*blob*/);
            if (printed)
                strbuf_appendf(ds, ",");
            if (null)
                strbuf_appendf(ds, "\"%s\":NULL", f->name);
            else {
                strbuf_appendf(ds, "\"%s\":\"%s\"", f->name, sval);
            }
            printed=1;
            break;
        case SERVER_DATETIMEUS:
            sval = realloc(sval,100);
            SERVER_DATETIMEUS_to_CLIENT_CSTR(
                buf + f->offset, flen, opts /*convopts*/, NULL /*blob*/, sval,
                100, &null, &outdtsz, opts /*convopts*/, NULL /*blob*/);
            if (printed)
                strbuf_appendf(ds, ",");
            if (null)
                strbuf_appendf(ds, "\"%s\":NULL", f->name);
            else {
                strbuf_appendf(ds, "\"%s\":\"%s\"", f->name, sval);
            }
            printed=1;
            break;
        case SERVER_INTVDS:
            sval = realloc(sval,100);
            SERVER_INTVDS_to_CLIENT_CSTR(
                buf + f->offset, flen, opts /*convopts*/, NULL /*blob*/, sval,
                100, &null, &outdtsz, opts /*convopts*/, NULL /*blob*/);
            if (printed)
                strbuf_appendf(ds, ",");
            if (null)
                strbuf_appendf(ds, "\"%s\":NULL", f->name);
            else {
                strbuf_appendf(ds, "\"%s\":\"%s\"", f->name, sval);
            }
            printed=1;
            break;
        case SERVER_INTVDSUS:
            sval = realloc(sval,100);
            SERVER_INTVDSUS_to_CLIENT_CSTR(
                buf + f->offset, flen, opts /*convopts*/, NULL /*blob*/, sval,
                100, &null, &outdtsz, opts /*convopts*/, NULL /*blob*/);
            if (printed)
                strbuf_appendf(ds, ",");
            if (null)
                strbuf_appendf(ds, "\"%s\":NULL", f->name);
            else {
                strbuf_appendf(ds, "\"%s\":\"%s\"", f->name, sval);
            }
            printed=1;
            break;
        case SERVER_BLOB2:
            in = (buf + f->offset);
            if (printed)
                strbuf_appendf(ds, ",");
            if (stype_is_null(in))
                strbuf_appendf(ds, "\"%s\":NULL", f->name);
            else {
                strbuf_appendf(ds, "\"%s\":x", f->name);
                strbuf_hex(ds, (void *)in + 5, flen - 5);
            }
            break;
        case SERVER_VUTF8:
            in = (buf + f->offset);
            if (printed)
                strbuf_appendf(ds, ",");
            if (stype_is_null(in))
                strbuf_appendf(ds, "\"%s\":NULL", f->name);
            else {
                strbuf_appendf(ds, "\"%s\":x", f->name);
                strbuf_hex(ds, (void *)in + 5, flen - 5);
            }
            break;
        case SERVER_BLOB:
            in = (buf + f->offset);
            if (printed)
                strbuf_appendf(ds, ",");
            if (stype_is_null(in)) {
                strbuf_appendf(ds, "\"%s\":NULL", f->name);
            } else {
                strbuf_appendf(ds, "\"%s\":\"\"", f->name);
            }
            break;
        default:
            logmsg(LOGMSG_ERROR, "%s line %d unconverted type, %d\n", __func__,
                    __LINE__, f->type);
        }
    }
    strbuf_appendf(ds, "}");
    if (sval)
        free(sval);
    return 0;
}

static void genid_format(logicalops_cursor *pCur, unsigned long long genid, char *stgenid, int sz)
{
    snprintf(stgenid, sz, "x%016llx", genid);
}

static void reset_json_cursors(logicalops_cursor *pCur)
{
    if (pCur->jsonrec == NULL)
        pCur->jsonrec = strbuf_new();
    else
        strbuf_clear(pCur->jsonrec);
    if (pCur->oldjsonrec == NULL)
        pCur->oldjsonrec = strbuf_new();
    else
        strbuf_clear(pCur->oldjsonrec);
}

static void reset_record_state(logicalops_cursor *pCur)
{
    if (pCur->table) {
        free(pCur->table);
        pCur->table = NULL;
    }
    pCur->record = pCur->oldrecord = NULL;
    pCur->genid[0] = pCur->oldgenid[0] = '\0';
    reset_json_cursors(pCur);
}

static int produce_update_data_record(logicalops_cursor *pCur, DB_LOGC *logc, 
        bdb_osql_log_rec_t *rec, DBT *logdta)
{
    int rc, dtalen, page, index;
    unsigned long long genid, oldgenid;
    short dtafile;
    void *packedbuf = NULL;
    void *packedprevbuf = NULL;
    int prevlen = 0;
    int updlen = 0;
    llog_undo_upd_dta_args *upd_dta = NULL;
    llog_undo_upd_dta_lk_args *upd_dta_lk = NULL;
    bdb_state_type *bdb_state = thedb->bdb_env;

    reset_record_state(pCur);

    if (rec->type == DB_llog_undo_upd_dta_lk) {
        if ((rc = llog_undo_upd_dta_lk_read(bdb_state->dbenv,
                        logdta->data,&upd_dta_lk)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d error unpacking %d\n",
                    __func__, __LINE__, rc);
            rc = SQLITE_INTERNAL;
            goto done;
        }
        genid = upd_dta_lk->newgenid;
        oldgenid = upd_dta_lk->oldgenid;
        dtafile = upd_dta_lk->dtafile;
        /* ?? */
        dtalen = upd_dta_lk->old_dta_len;
        pCur->table = strdup((char *)(upd_dta_lk->table.data));
    } else {
        if ((rc = llog_undo_upd_dta_read(bdb_state->dbenv, logdta->data,
                &upd_dta)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d error unpacking %d\n",
                    __func__, __LINE__, rc);
            rc = SQLITE_INTERNAL;
            goto done;
        }
        genid = upd_dta->newgenid;
        oldgenid = upd_dta->oldgenid;
        dtafile = upd_dta->dtafile;
        /* ?? */
        dtalen = upd_dta->old_dta_len;
        pCur->table = strdup((char *)(upd_dta->table.data));
    }

    assert(dtalen <= PACKED_MEMORY_SIZE);
    ASSERT_PARAMETER(dtalen);
    genid_format(pCur, genid, pCur->genid, sizeof(pCur->genid));
    genid_format(pCur, oldgenid, pCur->oldgenid, sizeof(pCur->oldgenid));
    if (dtafile == 0)
        snprintf(pCur->opstring, sizeof(pCur->opstring), "update-record");
    else
        snprintf(pCur->opstring, sizeof(pCur->opstring), "update-blob");

    if ((packedbuf = retrieve_packed_memory(pCur)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s line %d allocating memory\n", __func__,
                __LINE__);
        rc = SQLITE_NOMEM;
        goto done;
    }

    if ((packedprevbuf = retrieve_packed_memory_prev(pCur)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s line %d allocating memory\n", __func__,
                __LINE__);
        rc = SQLITE_NOMEM;
        goto done;
    }

    if ((pCur->db = get_dbtable_by_name(pCur->table)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s line %d error finding dbtable %s\n", __func__,
                __LINE__, pCur->table);
        return SQLITE_INTERNAL;
    }

    /* Reconstruct record from berkley */
    if (0 == bdb_inplace_cmp_genids(pCur->db->handle, oldgenid, genid)) {
        rc = bdb_reconstruct_inplace_update(bdb_state, &rec->lsn, packedprevbuf,
                &prevlen, packedbuf, &updlen, NULL, NULL, NULL);
    } else {
        prevlen = updlen = PACKED_MEMORY_SIZE;
        rc = bdb_reconstruct_update(bdb_state, &rec->lsn, &page, &index, NULL,
                                    NULL, packedprevbuf, &prevlen, NULL, NULL,
                                    packedbuf, &updlen);
    }

    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s line %d error %d reconstructing update for "
                "%d:%d\n", __func__, __LINE__, rc, rec->lsn.file,
                rec->lsn.offset);
        goto done;
    }

    char *unpacked = retrieve_unpacked_memory(pCur);
    char *unpackedprev = retrieve_unpacked_memory_prev(pCur);

    if (dtalen > 0) {
        /* Decompress and upgrade new record to current version */
        if ((rc = decompress_and_upgrade(pCur, pCur->table, packedbuf, updlen,
                                         dtafile == 0, unpacked, &pCur->odh,
                                         &pCur->record, &pCur->reclen)) != 0) {
            logmsg(LOGMSG_ERROR,
                   "%s line %d error %d reconstructing update for "
                   "%d:%d\n",
                   __func__, __LINE__, rc, rec->lsn.file, rec->lsn.offset);
            goto done;
        }

        /* Decompress and upgrade prev record to current version */
        if ((rc = decompress_and_upgrade(pCur, pCur->table, packedprevbuf,
                                         prevlen, dtafile == 0, unpackedprev,
                                         &pCur->oldodh, &pCur->oldrecord,
                                         &pCur->oldreclen)) != 0) {
            logmsg(LOGMSG_ERROR,
                   "%s line %d error %d reconstructing update for "
                   "%d:%d\n",
                   __func__, __LINE__, rc, rec->lsn.file, rec->lsn.offset);
            goto done;
        }
    } else if (0 == bdb_inplace_cmp_genids(pCur->db->handle, oldgenid, genid)) {
        /* Decompress and upgrade new record to current version */
        if ((rc = decompress_and_upgrade(pCur, pCur->table, packedbuf, updlen,
                                         dtafile == 0, unpacked, &pCur->odh,
                                         &pCur->record, &pCur->reclen)) != 0) {
            logmsg(LOGMSG_ERROR,
                   "%s line %d error %d reconstructing update for "
                   "%d:%d\n",
                   __func__, __LINE__, rc, rec->lsn.file, rec->lsn.offset);
            goto done;
        }
        /* no old data since old_dta_len == 0 */
    } else {
        /* packedprevbuf has the new data */
        if ((rc = decompress_and_upgrade(
                 pCur, pCur->table, packedprevbuf, prevlen, dtafile == 0,
                 unpacked, &pCur->odh, &pCur->record, &pCur->reclen)) != 0) {
            logmsg(LOGMSG_ERROR,
                   "%s line %d error %d reconstructing update for "
                   "%d:%d\n",
                   __func__, __LINE__, rc, rec->lsn.file, rec->lsn.offset);
            goto done;
        }
        /* no old data since dtalen == 0 */
    }

    if (dtafile == 0) {
        if ((rc = json_record(pCur, pCur->record, pCur->reclen, pCur->db->schema,
                        pCur->jsonrec)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d json_record returns %d\n", __func__,
                    __LINE__, rc);
            goto done;
        }
        if ((rc = json_record(pCur, pCur->oldrecord, pCur->oldreclen, pCur->db->schema,
                        pCur->oldjsonrec)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d json_record returns %d\n", __func__,
                    __LINE__, rc);
            goto done;
        }
    } else {
        int ix = get_schema_blob_field_idx((char *) pCur->table, ".ONDISK", dtafile - 1);
        if ((rc = json_blob(pCur->record, pCur->reclen, pCur->db->schema,
                        ix, pCur->jsonrec)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d json_blob returns %d\n", __func__,
                    __LINE__, rc);
            goto done;
        }
        if ((rc = json_blob(pCur->oldrecord, pCur->oldreclen, pCur->db->schema,
                        ix, pCur->oldjsonrec)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d json_blob returns %d\n", __func__,
                    __LINE__, rc);
            goto done;
        }
    }

done:
    if (upd_dta)
        free(upd_dta);
    if (upd_dta_lk)
        free(upd_dta_lk);

    return rc;
}

static int produce_add_data_record(logicalops_cursor *pCur, DB_LOGC *logc, 
        bdb_osql_log_rec_t *rec, DBT *logdta)
{
    int rc, dtalen, ixlen;
    unsigned long long genid;
    short dtafile;
    void *packedbuf = NULL;
    llog_undo_add_dta_args *add_dta = NULL;
    llog_undo_add_dta_lk_args *add_dta_lk = NULL;
    bdb_state_type *bdb_state = thedb->bdb_env;

    reset_record_state(pCur);

    dtalen = PACKED_MEMORY_SIZE;
    if (rec->type == DB_llog_undo_add_dta_lk) {
        if ((rc = llog_undo_add_dta_lk_read(bdb_state->dbenv,
                        logdta->data,&add_dta_lk)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d error unpacking %d\n",
                    __func__, __LINE__, rc);
            rc = SQLITE_INTERNAL;
            goto done;
        }
        genid = add_dta_lk->genid;
        dtafile = add_dta_lk->dtafile;
        pCur->table = strdup((char *)(add_dta_lk->table.data));
    } else {
        if ((rc = llog_undo_add_dta_read(bdb_state->dbenv, logdta->data,
                &add_dta)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d error unpacking %d\n",
                    __func__, __LINE__, rc);
            rc = SQLITE_INTERNAL;
            goto done;
        }
        genid = add_dta->genid;
        dtafile = add_dta->dtafile;
        pCur->table = strdup((char *)(add_dta->table.data));
    }
    genid_format(pCur, genid, pCur->genid, sizeof(pCur->genid));
    if (dtafile == 0) { 
        snprintf(pCur->opstring, sizeof(pCur->opstring), "insert-record");
    } else {
        snprintf(pCur->opstring, sizeof(pCur->opstring), "insert-blob");
    }

    if ((packedbuf = retrieve_packed_memory(pCur)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s line %d allocating memory\n", __func__,
                __LINE__);
        rc = SQLITE_NOMEM;
        goto done;
    }

    if ((pCur->db = get_dbtable_by_name(pCur->table)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s line %d error finding dbtable %s\n", __func__,
                __LINE__, pCur->table);
        return SQLITE_INTERNAL;
    }

    /* Reconstruct record from berkley */
    if ((rc = bdb_reconstruct_add(bdb_state, &rec->lsn, 
                    NULL, sizeof(genid_t), packedbuf, dtalen, &dtalen, &ixlen)) != 0) {
        logmsg(LOGMSG_ERROR, "%s line %d error %d reconstructing insert for "
                "%d:%d\n", __func__, __LINE__, rc, rec->lsn.file,
                rec->lsn.offset);
        goto done;
    }

    char *unpacked = retrieve_unpacked_memory(pCur);

    /* Decompress and upgrade to current version */
    if ((rc = decompress_and_upgrade(pCur, pCur->table, packedbuf, dtalen,
                    dtafile == 0, unpacked, &pCur->odh, &pCur->record, &pCur->reclen)) != 0) {
        logmsg(LOGMSG_ERROR, "%s line %d error %d reconstructing insert for "
                "%d:%d\n", __func__, __LINE__, rc, rec->lsn.file,
                rec->lsn.offset);
        goto done;
    }

    if (dtafile == 0) {
        if ((rc = json_record(pCur, pCur->record, pCur->reclen, pCur->db->schema,
                        pCur->jsonrec)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d json_record returns %d\n", __func__,
                    __LINE__, rc);
            goto done;
        }
    } else {
        int ix = get_schema_blob_field_idx((char *) pCur->table, ".ONDISK", dtafile - 1);
        if ((rc = json_blob(pCur->record, pCur->reclen, pCur->db->schema,
                        ix, pCur->jsonrec)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d json_blob returns %d\n", __func__,
                    __LINE__, rc);
            goto done;
        }
    }

done:
    if (add_dta)
        free(add_dta);
    if (add_dta_lk)
        free(add_dta_lk);

    return rc;
}

static int produce_delete_data_record(logicalops_cursor *pCur, DB_LOGC *logc, 
        bdb_osql_log_rec_t *rec, DBT *logdta)
{
    int rc, dtalen, page, index;
    unsigned long long genid;
    short dtafile;
    void *packedprevbuf = NULL;
    llog_undo_del_dta_args *del_dta = NULL;
    llog_undo_del_dta_lk_args *del_dta_lk = NULL;
    bdb_state_type *bdb_state = thedb->bdb_env;

    reset_record_state(pCur);

    if (rec->type == DB_llog_undo_del_dta_lk) {
        if ((rc = llog_undo_del_dta_lk_read(bdb_state->dbenv,
                        logdta->data,&del_dta_lk)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d error unpacking %d\n",
                    __func__, __LINE__, rc);
            rc = SQLITE_INTERNAL;
            goto done;
        }
        genid = del_dta_lk->genid;
        dtafile = del_dta_lk->dtafile;
        dtalen = del_dta_lk->dtalen;
        pCur->table = strdup((char *)(del_dta_lk->table.data));
    } else {
        if ((rc = llog_undo_del_dta_read(bdb_state->dbenv, logdta->data,
                &del_dta)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d error unpacking %d\n",
                    __func__, __LINE__, rc);
            rc = SQLITE_INTERNAL;
            goto done;
        }
        genid = del_dta->genid;
        dtafile = del_dta->dtafile;
        dtalen = del_dta->dtalen;
        pCur->table = strdup((char *)(del_dta->table.data));
    }

    assert(dtalen <= PACKED_MEMORY_SIZE);
    genid_format(pCur, genid, pCur->oldgenid, sizeof(pCur->oldgenid));

    if (dtafile == 0) {
        snprintf(pCur->opstring, sizeof(pCur->opstring), "delete-record");
    } else {
        snprintf(pCur->opstring, sizeof(pCur->opstring), "delete-blob");
    }

    if ((packedprevbuf = retrieve_packed_memory_prev(pCur)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s line %d allocating memory\n", __func__,
                __LINE__);
        rc = SQLITE_NOMEM;
        goto done;
    }

    if ((pCur->db = get_dbtable_by_name(pCur->table)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s line %d error finding dbtable %s\n", __func__,
                __LINE__, pCur->table);
        return SQLITE_INTERNAL;
    }

    /* Reconstruct record from berkley */
    if ((rc = bdb_reconstruct_delete(bdb_state, &rec->lsn, &page,
                    &index, NULL, sizeof(genid_t), packedprevbuf, dtalen, &dtalen)) != 0) {
        logmsg(LOGMSG_ERROR, "%s line %d error %d reconstructing delete for "
                "%d:%d\n", __func__, __LINE__, rc, rec->lsn.file,
                rec->lsn.offset);
        goto done;
    }

    char *unpackedprev = retrieve_unpacked_memory_prev(pCur);

    /* Decompress and upgrade to current version */
    if ((rc = decompress_and_upgrade(pCur, pCur->table, packedprevbuf, dtalen,
                    dtafile == 0, unpackedprev, &pCur->oldodh, &pCur->oldrecord,
                    &pCur->oldreclen)) != 0) {
        logmsg(LOGMSG_ERROR, "%s line %d error %d reconstructing delete for "
                "%d:%d\n", __func__, __LINE__, rc, rec->lsn.file,
                rec->lsn.offset);
        goto done;
    }

    if (dtafile == 0) {
        if ((rc = json_record(pCur, pCur->oldrecord, pCur->oldreclen, pCur->db->schema,
                        pCur->oldjsonrec)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d json_record returns %d\n", __func__,
                    __LINE__, rc);
            goto done;
        }
    } else {
        int ix = get_schema_blob_field_idx((char *) pCur->table, ".ONDISK", dtafile - 1);
        if ((rc = json_blob(pCur->oldrecord, pCur->oldreclen, pCur->db->schema,
                        ix, pCur->oldjsonrec)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d json_blob returns %d\n", __func__,
                    __LINE__, rc);
            goto done;
        }
    }

done:
    if (del_dta)
        free(del_dta);
    if (del_dta_lk)
        free(del_dta_lk);

    return rc;
}

static int unpack_logical_record(logicalops_cursor *pCur)
{
    bdb_osql_log_rec_t *rec;
    bdb_state_type *bdb_state = thedb->bdb_env;
    u_int32_t rectype;
    DBT logdta = {0};
    DB_LOGC *logc;
    int rc, produced_row = 0;

    if ((rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0))
            != 0) { 
        logmsg(LOGMSG_ERROR, "%s line %d error getting log-cursor %d\n",
                __func__, __LINE__, rc);
        return SQLITE_INTERNAL;
    }

    while (produced_row == 0 &&
           (rec = listc_rtl(&pCur->llog_cur.log->impl->recs)) != NULL) {

        reset_json_cursors(pCur);
        logdta.flags = DB_DBT_REALLOC;
        if ((rc = logc->get(logc, &rec->lsn, &logdta, DB_SET)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d error %d retrieving lsn %d:%d\n",
                    __func__, __LINE__, rc, rec->lsn.file, rec->lsn.offset);
            logc->close(logc, 0);
            return SQLITE_INTERNAL;
        }
        LOGCOPY_32(&rectype, logdta.data);
        assert(rectype == rec->type);

        switch(rec->type) {
            case DB_llog_undo_del_dta:
            case DB_llog_undo_del_dta_lk:
                if ((rc = produce_delete_data_record(pCur, logc, rec, &logdta)) == 0) {
                    pCur->llog_cur.subop++;
                    produced_row=1;
                }
                break;

                /*
                   case DB_llog_undo_del_ix:
                   case DB_llog_undo_del_ix_lk:
                   rc = produce_delete_index_record(pCur, logc, rec, &logdta);
                   break;
                   */
            case DB_llog_undo_add_dta:
            case DB_llog_undo_add_dta_lk:
                if ((rc = produce_add_data_record(pCur, logc, rec, &logdta)) == 0) {
                    pCur->llog_cur.subop++;
                    produced_row=1;
                }
                break;
                /*
                   case DB_llog_undo_add_ix:
                   case DB_llog_undo_add_ix_lk:
                   rc = produce_add_index_record(pCur, logc, rec, &logdta);
                   break;
                   */
            case DB_llog_undo_upd_dta:
            case DB_llog_undo_upd_dta_lk:
                if ((rc = produce_update_data_record(pCur, logc, rec, &logdta)) == 0) {
                    pCur->llog_cur.subop++;
                    produced_row=1;
                }
                break;
                /*
                   case DB_llog_undo_upd_ix:
                   case DB_llog_undo_upd_ix_lk:
                   rc = produce_update_index_record(pCur, logc, rec, &logdta);
                   break;
                   case DB_llog_ltran_comprec:
                   rc = produce_compensation_record(pCur, logc, rec, &logdta);
                   break;
                   */
            default:
                logmsg(LOGMSG_DEBUG, "%s line %d skipping %d\n", __func__,
                       __LINE__, rec->type);
                break;
        }
        free(rec);
    }
    logc->close(logc, 0);

    if (listc_size(&pCur->llog_cur.log->impl->recs) == 0) {
        bdb_osql_log_destroy(pCur->llog_cur.log);
        pCur->llog_cur.log = NULL;
    }

    return (produced_row && rc != 0) ? -1 : !produced_row;
}

/*
** Advance a logicalops cursor to the next log entry
*/
static int logicalopsNext(sqlite3_vtab_cursor *cur){
  logicalops_cursor *pCur = (logicalops_cursor*)cur;
  int rc;

  if (pCur->llog_cur.hitLast)
      return SQLITE_OK;

again:
    if (pCur->llog_cur.log == NULL &&
        (bdb_llog_cursor_next(&pCur->llog_cur) != 0))
        return SQLITE_INTERNAL;

    if (pCur->llog_cur.log && !pCur->llog_cur.hitLast) {
        rc = unpack_logical_record(pCur);
        switch (rc) {
        case 1:
            logmsg(LOGMSG_DEBUG, "%s line %d unpacking record: continuing\n",
                   __func__, __LINE__);
            goto again;
            break;
        case 0:
            break;
        default:
            return SQLITE_INTERNAL;
            break;
        }
        pCur->iRowid++;
  }

  return SQLITE_OK;
}

#define skipws(p) { while (*p != '\0' && *p == ' ') p++; }
#define isnum(p) ( *p >= '0' && *p <= '9' )

static inline void logicalops_lsn_to_str(char *st, DB_LSN *lsn)
{
    sprintf(st, "{%d:%d}", lsn->file, lsn->offset);
}

static inline int parse_lsn(const unsigned char *lsnstr, DB_LSN *lsn)
{
    const char *p = (const char *)lsnstr;
    int file, offset;
    while (*p != '\0' && *p == ' ') p++;
    skipws(p);

    /* Parse opening '{' */
    if (*p != '{')
        return -1;
    p++;
    skipws(p);
    if ( !isnum(p) )
        return -1;

    /* Parse file */
    file = atoi(p);
    while( isnum(p) )
        p++;
    skipws(p);
    if ( *p != ':' )
        return -1;
    p++;
    skipws(p);
    if ( !isnum(p) )
        return -1;

    /* Parse offset */
    offset = atoi(p);
    while( isnum(p) )
        p++;

    skipws(p);

    /* Parse closing '}' */
    if (*p != '}')
        return -1;
    p++;

    skipws(p);
    if (*p != '\0')
        return -1;

    lsn->file = file;
    lsn->offset = offset;
    return 0;
}

static u_int32_t get_generation_from_regop_gen_record(char *data)
{
    u_int32_t generation;
    LOGCOPY_32( &generation, &data[ 4 + 4 + 8 + 4] );
    return generation;
}

static u_int32_t get_generation_from_rowlocks_record(char *data)
{
    u_int32_t generation;
    LOGCOPY_32( &generation, &data[4 + 4 + 8 + 4 + 8 + 8 + 8 + 8 + 8 + 4] );
    return generation;
}

static int logicalopsColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  logicalops_cursor *pCur = (logicalops_cursor*)cur;

  switch( i ){
    case LOGICALOPS_COLUMN_START:
        if (!pCur->minLsnStr) {
            pCur->minLsnStr = sqlite3_malloc(32);
            logicalops_lsn_to_str(pCur->minLsnStr, &pCur->llog_cur.minLsn);
        }
        sqlite3_result_text(ctx, pCur->minLsnStr, -1, NULL);
        break;

    case LOGICALOPS_COLUMN_STOP:
        if (!pCur->maxLsnStr) {
            pCur->maxLsnStr = sqlite3_malloc(32);
            logicalops_lsn_to_str(pCur->maxLsnStr, &pCur->llog_cur.maxLsn);
        }
        sqlite3_result_text(ctx, pCur->maxLsnStr, -1, NULL);
        break;

    case LOGICALOPS_COLUMN_COMMITLSN:
        if (!pCur->curLsnStr) {
            pCur->curLsnStr = sqlite3_malloc(32);
        }
        logicalops_lsn_to_str(pCur->curLsnStr, &pCur->llog_cur.curLsn);
        sqlite3_result_text(ctx, pCur->curLsnStr, -1, NULL);
        break;
    case LOGICALOPS_COLUMN_OPNUM:
        sqlite3_result_int64(ctx, pCur->llog_cur.subop);
        break;
    case LOGICALOPS_COLUMN_GENID:
        if (pCur->genid[0] == '\0')
            sqlite3_result_null(ctx);
        else
            sqlite3_result_text(ctx, pCur->genid, -1, NULL);
        break;
    case LOGICALOPS_COLUMN_OLDGENID:
        if (pCur->oldgenid[0] == '\0')
            sqlite3_result_null(ctx);
        else
            sqlite3_result_text(ctx, pCur->oldgenid, -1, NULL);
        break;
    case LOGICALOPS_COLUMN_OPERATION:
        sqlite3_result_text(ctx, pCur->opstring, -1, NULL);
        break;
    case LOGICALOPS_COLUMN_TABLE:
        sqlite3_result_text(ctx, pCur->table, -1, NULL);
        break;
    case LOGICALOPS_COLUMN_OLDRECORD:
        if (strbuf_len(pCur->oldjsonrec) == 0)
            sqlite3_result_null(ctx);
        else
            sqlite3_result_text(ctx, strbuf_buf(pCur->oldjsonrec), strbuf_len(pCur->oldjsonrec), NULL);
        break;
    case LOGICALOPS_COLUMN_RECORD:
        if (strbuf_len(pCur->jsonrec) == 0)
            sqlite3_result_null(ctx);
        else
            sqlite3_result_text(ctx, strbuf_buf(pCur->jsonrec), strbuf_len(pCur->jsonrec), NULL);
        break;
  }
  return SQLITE_OK;
}

static int logicalopsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  logicalops_cursor *pCur = (logicalops_cursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

static int logicalopsEof(sqlite3_vtab_cursor *cur){
  logicalops_cursor *pCur = (logicalops_cursor*)cur;
  int rc;

  /* If we are not positioned, position now */
  if (pCur->llog_cur.openCursor == 0) {
      if ((rc=logicalopsNext(cur)) != SQLITE_OK)
          return rc;
  }
  if (pCur->llog_cur.hitLast)
      return 1;
  if (pCur->llog_cur.maxLsn.file > 0 &&
      log_compare(&pCur->llog_cur.curLsn, &pCur->llog_cur.maxLsn) > 0)
      return 1;
  return 0;
}

static int logicalopsFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  logicalops_cursor *pCur = (logicalops_cursor *)pVtabCursor;
  int i = 0;

  bzero(&pCur->llog_cur.minLsn, sizeof(pCur->llog_cur.minLsn));
  if( idxNum & 1 ){
      const unsigned char *minLsn = sqlite3_value_text(argv[i++]);
      if (minLsn && parse_lsn(minLsn, &pCur->llog_cur.minLsn)) {
          return SQLITE_CONV_ERROR;
      }
  }
  bzero(&pCur->llog_cur.maxLsn, sizeof(pCur->llog_cur.maxLsn));
  if( idxNum & 2 ){
      const unsigned char *maxLsn = sqlite3_value_text(argv[i++]);
      if (maxLsn && parse_lsn(maxLsn, &pCur->llog_cur.maxLsn)) {
          return SQLITE_CONV_ERROR;
      }
  }
  pCur->iRowid = 1;
  return SQLITE_OK;
}

static int logicalopsBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;
  int idxNum = 0;
  int startIdx = -1;
  int stopIdx = -1;
  int nArg = 0;

  const struct sqlite3_index_constraint *pConstraint;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( pConstraint->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pConstraint->iColumn ){
      case LOGICALOPS_COLUMN_START:
        startIdx = i;
        idxNum |= 1;
        break;
      case LOGICALOPS_COLUMN_STOP:
        stopIdx = i;
        idxNum |= 2;
        break;
    }
  }
  if( startIdx>=0 ){
    pIdxInfo->aConstraintUsage[startIdx].argvIndex = ++nArg;
    pIdxInfo->aConstraintUsage[startIdx].omit = 1;
  }
  if( stopIdx>=0 ){
    pIdxInfo->aConstraintUsage[stopIdx].argvIndex = ++nArg;
    pIdxInfo->aConstraintUsage[stopIdx].omit = 1;
  }
  if( (idxNum & 3)==3 ){
    /* Both start= and stop= boundaries are available.  This is the 
    ** the preferred case */
    pIdxInfo->estimatedCost = (double)1;
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
sqlite3_module systblLogicalOpsModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  logicalopsConnect,         /* xConnect */
  logicalopsBestIndex,       /* xBestIndex */
  logicalopsDisconnect,      /* xDisconnect */
  0,                         /* xDestroy */
  logicalopsOpen,            /* xOpen - open a cursor */
  logicalopsClose,           /* xClose - close a cursor */
  logicalopsFilter,          /* xFilter - configure scan constraints */
  logicalopsNext,            /* xNext - advance a cursor */
  logicalopsEof,             /* xEof - check for end of scan */
  logicalopsColumn,          /* xColumn - read data */
  logicalopsRowid,           /* xRowid - read data */
  0,                         /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindMethod */
  0,                         /* xRename */
  0,                         /* xSavepoint */
  0,                         /* xRelease */
  0,                         /* xRollbackTo */
  0,                         /* xShadowName */
  .access_flag = CDB2_ALLOW_USER,
};


