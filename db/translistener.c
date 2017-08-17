/*
   Copyright 2015 Bloomberg Finance L.P.

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

/*
 *  This file implements a transaction listener.
 */

#include <pthread.h>

#include <stdio.h>
#include <strings.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <segstr.h>
#include <list.h>
#include <sbuf2.h>

#include "comdb2.h"
#include "translistener.h"
#include "endian_core.h"

#include <tcputil.h>
#include <unistd.h>
#include <logmsg.h>

struct javasp_trans_state {
    /* Which events we are subscribed for. */
    int events;

    /* We need this in case the Java code tries to write anything in the
     * trans */
    struct ireq *iq;
    void *trans;
    void *parent_trans;
    int debug;
};

struct sp_rec_blob {
    int status;
    char *blobdta;
    size_t bloboff;
    size_t bloblen;
};

struct sp_field {
    int flags;
    char *name;
    LINKC_T(struct sp_field) lnk;
};

struct sp_table {
    char *name;
    int flags;
    LINKC_T(struct sp_table) lnk;
    LISTC_T(struct sp_field) fields;
};

struct stored_proc {
    char *name;
    char *param;

    char *qname;
    int flags;
    LISTC_T(struct sp_table) tables;
    LINKC_T(struct stored_proc) lnk;
};
LISTC_T(struct stored_proc) stored_procs;
LISTC_T(struct stored_proc) delayed_stored_procs;

pthread_rwlock_t splk = PTHREAD_RWLOCK_INITIALIZER;
#define SP_READLOCK() pthread_rwlock_rdlock(&splk)
#define SP_WRITELOCK() pthread_rwlock_wrlock(&splk)
#define SP_RELLOCK() pthread_rwlock_unlock(&splk)

struct javasp_rec {
    /* If this is not null then this is the transaction under which we can read
     * more blob information. */
    struct javasp_trans_state *trans;

    /* If this record exists on disk then we keep its rrn and genid here */
    int rrn;
    long long genid;

    /* Name of table */
    char tablename[MAXTABLELEN + 1];

    /* Pointer to .ONDISK format record data. */
    const void *ondisk_dta;
    size_t ondisk_dta_len;

    /* Pointer to corresponding schema for this guy. */
    struct schema *schema;

    /* Temporary buffer for various things */
    void *tempbuf;
    size_t tempbuflen;

    /* Keep track of all the blobs for us */
    struct sp_rec_blob blobs[MAXBLOBS];

    /* Do I own ondisk_dta (if I do it will be freed along with this struct) */
    int ondisk_needs_free;

    /* Context that record was read with */
    unsigned long long context;
};

/*
 *
 * Tagged record manipulators.
 *
 */

enum {
    BLOB_NULL = 0,       /* this blob is null */
    BLOB_KNOWN = 1,      /* we have the blob data for this one */
    BLOB_KNOWN_FREE = 2, /* ditto, and we must free it ourselves */
};

void javasp_once_init(void)
{
    listc_init(&stored_procs, offsetof(struct stored_proc, lnk));
    listc_init(&delayed_stored_procs, offsetof(struct stored_proc, lnk));
}

int javasp_init_procedures(void)
{
    struct stored_proc *p;
    int rc;

    p = listc_rtl(&delayed_stored_procs);
    while (p) {
        rc = javasp_load_procedure(p->name, NULL, p->param);
        if (rc)
            return rc;

        free(p->name);
        free(p->param);
        free(p);

        p = listc_rtl(&delayed_stored_procs);
    }
    return 0;
}

void javasp_stat(const char *args)
{
    struct stored_proc *p;
    struct sp_table *t;
    struct sp_field *f;

    SP_READLOCK();

    LISTC_FOR_EACH(&stored_procs, p, lnk)
    {
       logmsg(LOGMSG_USER, "%s\n", p->name);
        LISTC_FOR_EACH(&p->tables, t, lnk)
        {
           logmsg(LOGMSG_USER, "   %s\n", t->name);
            LISTC_FOR_EACH(&t->fields, f, lnk)
            {
               logmsg(LOGMSG_USER, "      %s ", f->name);
                if (f->flags & JAVASP_TRANS_LISTEN_AFTER_ADD)
                   logmsg(LOGMSG_USER, "add ");
                if (f->flags & JAVASP_TRANS_LISTEN_AFTER_DEL)
                   logmsg(LOGMSG_USER, "del ");
                if (f->flags & JAVASP_TRANS_LISTEN_BEFORE_UPD)
                   logmsg(LOGMSG_USER, "pre_upd ");
                if (f->flags & JAVASP_TRANS_LISTEN_AFTER_UPD)
                   logmsg(LOGMSG_USER, "post_upd ");
               logmsg(LOGMSG_USER, "\n");
            }
        }
       logmsg(LOGMSG_USER, "\n");
    }

    SP_RELLOCK();
}

struct javasp_trans_state *javasp_trans_start(int debug)
{
    struct javasp_trans_state *st;
    struct stored_proc *sp;

    st = malloc(sizeof(struct javasp_trans_state));
    st->events = 0;
    st->iq = NULL;
    st->trans = NULL;
    st->parent_trans = NULL;
    st->debug = 0;

    SP_READLOCK();

    LISTC_FOR_EACH(&stored_procs, sp, lnk) { st->events |= sp->flags; }

    SP_RELLOCK();

    return st;
}

void javasp_trans_set_trans(struct javasp_trans_state *javasp_trans_handle,
                            struct ireq *ireq, void *parent_trans, void *trans)
{
    javasp_trans_handle->iq = ireq;
    javasp_trans_handle->trans = trans;
    javasp_trans_handle->parent_trans = parent_trans;
}

void javasp_trans_end(struct javasp_trans_state *javasp_trans_handle)
{
    bzero(javasp_trans_handle, sizeof(struct javasp_trans_state));
    free(javasp_trans_handle);
}

int javasp_trans_misc_trigger(struct javasp_trans_state *javasp_trans_handle,
                              int event)
{
    if (javasp_trans_handle == NULL)
        return 0;
    return 0;
}

struct byte_buffer {
    unsigned char *bytes;
    int used;
    int allocated;
};

static void byte_buffer_init(struct byte_buffer *b)
{
    b->bytes = malloc(1024);
    b->used = 0;
    b->allocated = 1024;
}

static void byte_buffer_free(struct byte_buffer *b)
{
    if (b->bytes)
        free(b->bytes);
}

static void byte_buffer_reserve(struct byte_buffer *b, int sz);

static void byte_buffer_append(struct byte_buffer *b, void *in, int sz)
{
    byte_buffer_reserve(b, sz);
    memcpy(b->bytes + b->used, in, sz);
    b->used += sz;
}

/* Make sure we can write sz more bytes to the buffer */
static void byte_buffer_reserve(struct byte_buffer *b, int sz)
{
    if (b->used + sz > b->allocated) {
        b->allocated += (sz < 1024) ? 1024 : sz;
        b->bytes = realloc(b->bytes, b->allocated);
    }
}

static void byte_buffer_append_zero(struct byte_buffer *b, int n)
{
    unsigned char z = 0;
    byte_buffer_reserve(b, n);
    bzero(b->bytes + b->used, n);
    b->used += n;
}

#define append_flipped(bytes, v)                                               \
    byte_buffer_reserve(bytes, sizeof(v));                                     \
    if (!buf_put(&v, sizeof(v), bytes->bytes + bytes->used,                    \
                 bytes->bytes + bytes->allocated))                             \
        abort(); /* shouldn't happen */                                        \
    bytes->used += sizeof(v);

static void byte_buffer_append_int64(struct byte_buffer *bytes, long long v)
{
    append_flipped(bytes, v);
}

static void byte_buffer_append_uint64(struct byte_buffer *bytes, unsigned int v)
{
    append_flipped(bytes, v);
}

static void byte_buffer_append_int32(struct byte_buffer *bytes, int v)
{
    byte_buffer_reserve(bytes, sizeof(v));
    if (!buf_put(&v, sizeof(v), bytes->bytes + bytes->used,
                 bytes->bytes + bytes->allocated))
        abort(); /* shouldn't happen */
    bytes->used += sizeof(v);
}

static void byte_buffer_append_uint32(struct byte_buffer *bytes, unsigned int v)
{
    append_flipped(bytes, v);
}

static void byte_buffer_append_int16(struct byte_buffer *bytes, short v)
{
    append_flipped(bytes, v);
}

static void byte_buffer_append_uint16(struct byte_buffer *bytes,
                                      unsigned short v)
{
    append_flipped(bytes, v);
}

static void byte_buffer_append_float(struct byte_buffer *bytes, float v)
{
    append_flipped(bytes, v);
}

static void byte_buffer_append_double(struct byte_buffer *bytes, double v)
{
    append_flipped(bytes, v);
}

static void byte_buffer_append_datetime(struct byte_buffer *bytes,
                                        cdb2_client_datetime_t *cdt)
{
    byte_buffer_reserve(bytes, sizeof(cdb2_client_datetime_t));
    byte_buffer_append(bytes, cdt, sizeof(cdb2_client_datetime_t));
}

static void byte_buffer_append_datetimeus(struct byte_buffer *bytes,
                                          cdb2_client_datetimeus_t *cdt)
{
    byte_buffer_reserve(bytes, sizeof(cdb2_client_datetimeus_t));
    byte_buffer_append(bytes, cdt, sizeof(cdb2_client_datetimeus_t));
}

static void byte_buffer_append_intervalym(struct byte_buffer *bytes,
                                          cdb2_client_intv_ym_t *cym)
{
    byte_buffer_reserve(bytes, sizeof(cdb2_client_intv_ym_t));
    byte_buffer_append(bytes, cym, sizeof(cdb2_client_intv_ym_t));
}

static void byte_buffer_append_intervalds(struct byte_buffer *bytes,
                                          cdb2_client_intv_ds_t *cds)
{
    byte_buffer_reserve(bytes, sizeof(cdb2_client_intv_ds_t));
    byte_buffer_append(bytes, cds, sizeof(cdb2_client_intv_ds_t));
}

static void byte_buffer_append_intervaldsus(struct byte_buffer *bytes,
                                            cdb2_client_intv_dsus_t *cds)
{
    byte_buffer_reserve(bytes, sizeof(cdb2_client_intv_dsus_t));
    byte_buffer_append(bytes, cds, sizeof(cdb2_client_intv_dsus_t));
}

static void append_header(struct byte_buffer *bytes, char *tbl, int event)
{
    unsigned char pad[32] = {0};
    int flags = 0;
    int tblname_len;

    byte_buffer_append(bytes, "CDB2_UPD", 8);
    byte_buffer_append(bytes, pad, sizeof(pad));

    if (event & JAVASP_TRANS_LISTEN_AFTER_ADD)
        flags |= TYPE_TAGGED_ADD;
    if (event &
        (JAVASP_TRANS_LISTEN_BEFORE_UPD | JAVASP_TRANS_LISTEN_AFTER_UPD))
        flags |= TYPE_TAGGED_UPD;
    if (event & JAVASP_TRANS_LISTEN_AFTER_DEL)
        flags |= TYPE_TAGGED_DEL;
    tblname_len = strlen(tbl);

    byte_buffer_append_int32(bytes, flags);
    byte_buffer_append_int16(bytes, tblname_len);
    byte_buffer_append(bytes, tbl, tblname_len + 1);
}

static int type_to_sptype(int type, int len)
{
    switch (type) {
    case SERVER_BINT:
        switch (len) {
        case 9:
            return SP_FIELD_INT64;
        case 5:
            return SP_FIELD_INT32;
        case 3:
            return SP_FIELD_INT16;
        default:
            return -1;
        }
    case SERVER_UINT:
        switch (len) {
        case 9:
            return SP_FIELD_UINT64;
        case 5:
            return SP_FIELD_UINT32;
        case 3:
            return SP_FIELD_UINT16;
        default:
            return -1;
        }
    case SERVER_BREAL:
        switch (len) {
        case 9:
            return SP_FIELD_REAL64;
        case 5:
            return SP_FIELD_REAL32;
        default:
            return -1;
        }
    case SERVER_BCSTR:
    case SERVER_VUTF8:
        return SP_FIELD_STRING;
    case SERVER_BYTEARRAY:
        return SP_FIELD_BYTEARRAY;
    case SERVER_BLOB:
        return SP_FIELD_BLOB;

    case SERVER_DATETIME:
        if (gbl_support_datetime_in_triggers)
            return SP_FIELD_DATETIME;
        else
            return -1;

    case SERVER_DATETIMEUS:
        if (gbl_support_datetime_in_triggers)
            return SP_FIELD_DATETIMEUS;
        else
            return -1;

    case SERVER_INTVYM:
        if (gbl_support_datetime_in_triggers)
            return SP_FIELD_INTERVALYM;
        else
            return -1;

    case SERVER_INTVDS:
        if (gbl_support_datetime_in_triggers)
            return SP_FIELD_INTERVALDS;
        else
            return -1;

    case SERVER_INTVDSUS:
        if (gbl_support_datetime_in_triggers)
            return SP_FIELD_INTERVALDSUS;
        else
            return -1;

    case SERVER_DECIMAL:
        return SP_FIELD_STRING;

    default:
        return -1;
    }
}

static void append_field(struct byte_buffer *bytes, struct field *f,
                         struct javasp_rec *record)
{
    long long i8;
    int i4;
    short i2;
    unsigned long long u8;
    unsigned int u4;
    unsigned short u2;
    double r8;
    float r4;
    int rc;
    int null, outsz;
    int slen;
    void *rec;

    /* note: this routine doesn't get called for null values, so don't cover
     * those here */
    rec = (void *)record->ondisk_dta;

    switch (f->type) {
    case SERVER_UINT:
        switch (f->len) {
        case 9:
            rc = SERVER_UINT_to_CLIENT_UINT((uint8_t *)rec + f->offset, f->len,
                                            NULL, NULL, &u8, sizeof(u8), &null,
                                            &outsz, NULL, NULL);
            /* NOTE: these are already in big-endian format, so we don't need to
             * flip them again. */
            byte_buffer_append(bytes, &u8, sizeof(u8));
            break;
        case 5:
            rc = SERVER_UINT_to_CLIENT_UINT((uint8_t *)rec + f->offset, f->len,
                                            NULL, NULL, &u4, sizeof(u4), &null,
                                            &outsz, NULL, NULL);
            byte_buffer_append(bytes, &u4, sizeof(u4));
            break;
        case 3:
            rc = SERVER_UINT_to_CLIENT_UINT((uint8_t *)rec + f->offset, f->len,
                                            NULL, NULL, &u2, sizeof(u2), &null,
                                            &outsz, NULL, NULL);
            byte_buffer_append(bytes, &u2, sizeof(u2));
            break;
        default:
            logmsg(LOGMSG_ERROR, "unknown unsigned integer size %d\n", f->len);
            abort();
        }
        if (rc) {
            logmsg(LOGMSG_FATAL, "unsigned integer field conversion for %s failed rc %d\n",
                   f->name, rc);
            abort();
        }
        break;

    case SERVER_BINT:
        switch (f->len) {
        case 9:
            rc = SERVER_BINT_to_CLIENT_INT((uint8_t *)rec + f->offset, f->len,
                                           NULL, NULL, &i8, sizeof(i8), &null,
                                           &outsz, NULL, NULL);
            byte_buffer_append(bytes, &i8, sizeof(i8));
            break;
        case 5:
            rc = SERVER_BINT_to_CLIENT_INT((uint8_t *)rec + f->offset, f->len,
                                           NULL, NULL, &i4, sizeof(i4), &null,
                                           &outsz, NULL, NULL);
            byte_buffer_append(bytes, &i4, sizeof(i4));
            break;
        case 3:
            rc = SERVER_BINT_to_CLIENT_INT((uint8_t *)rec + f->offset, f->len,
                                           NULL, NULL, &i2, sizeof(i2), &null,
                                           &outsz, NULL, NULL);
            byte_buffer_append(bytes, &i2, sizeof(i2));
            break;
        default:
            logmsg(LOGMSG_FATAL, "unknown integer size %d\n", f->len);
            abort();
        }
        if (rc) {
            logmsg(LOGMSG_FATAL, "integer field conversion for %s failed rc %d\n", f->name,
                   rc);
            abort();
        }
        break;

    case SERVER_BREAL:
        switch (f->len) {
        case 9:
            rc = SERVER_BREAL_to_CLIENT_REAL((uint8_t *)rec + f->offset, f->len,
                                             NULL, NULL, &r8, sizeof(r8), &null,
                                             &outsz, NULL, NULL);
            byte_buffer_append(bytes, &r8, sizeof(r8));
            break;
        case 5:
            rc = SERVER_BREAL_to_CLIENT_REAL((uint8_t *)rec + f->offset, f->len,
                                             NULL, NULL, &r4, sizeof(r4), &null,
                                             &outsz, NULL, NULL);
            byte_buffer_append(bytes, &r4, sizeof(r4));
            break;
        default:
            logmsg(LOGMSG_FATAL, "unknown real size %d\n", f->len);
            abort();
        }
        if (rc) {
            logmsg(LOGMSG_FATAL, "integer field conversion for %s failed rc %d\n", f->name,
                   rc);
            abort();
        }
        break;

    case SERVER_BCSTR:
        slen = cstrlenlim((const char *)rec + f->offset + 1, f->len - 1);
        byte_buffer_append_int32(bytes, slen);
        byte_buffer_append(bytes, (uint8_t *)rec + f->offset + 1, slen);
        byte_buffer_append_zero(bytes, 1);
        break;

    case SERVER_VUTF8:
        /* these are blobs internally, but go out as strings, ie, the type
           of this field is SP_FIELD_STRING */
        if (record->blobs[f->blob_index].bloblen) {
            byte_buffer_append_int32(bytes,
                                     record->blobs[f->blob_index].bloblen - 1);
            byte_buffer_append(bytes, record->blobs[f->blob_index].blobdta,
                               record->blobs[f->blob_index].bloblen - 1);
        } else {
            int len;
            /* The field is not null or append_field would not be called.  It
             * must either be
             * zero-length or fits in the inline portion. */
            memcpy(&len, (uint8_t *)rec + f->offset + 1, sizeof(int));
            len = ntohl(len);
            byte_buffer_append_int32(bytes, len);
            byte_buffer_append(
                bytes, (uint8_t *)rec + f->offset + 1 + sizeof(int), len);
        }
        /* null-terminate */
        byte_buffer_append_zero(bytes, 1);
        break;

    case SERVER_BYTEARRAY:
        byte_buffer_append_int32(bytes, f->len - 1);
        byte_buffer_append(bytes, (uint8_t *)rec + f->offset + 1, f->len - 1);
        break;

    case SERVER_BLOB:
        byte_buffer_append_int32(bytes, record->blobs[f->blob_index].bloblen);
        byte_buffer_append(bytes, record->blobs[f->blob_index].blobdta,
                           record->blobs[f->blob_index].bloblen);
        break;

    case SERVER_DATETIME: {
        if (gbl_support_datetime_in_triggers) {
            cdb2_client_datetime_t cdt;
            int null, outdtsz;
            struct field_conv_opts_tz outopts;

            outopts.flags = FLD_CONV_TZONE;
            strcpy(outopts.tzname, record->trans->iq->tzname);

            rc = SERVER_DATETIME_to_CLIENT_DATETIME(
                (uint8_t *)rec + f->offset, sizeof(server_datetime_t), NULL,
                NULL, (cdb2_client_datetime_t *)&cdt, sizeof(cdt), &null,
                &outdtsz, (void *)&outopts, NULL);
            byte_buffer_append_datetime(bytes, &cdt);
        }
        break;
    }

    case SERVER_DATETIMEUS: {
        if (gbl_support_datetime_in_triggers) {
            cdb2_client_datetimeus_t cdt;
            int null, outdtsz;
            struct field_conv_opts_tz outopts;

            outopts.flags = FLD_CONV_TZONE;
            strcpy(outopts.tzname, record->trans->iq->tzname);

            rc = SERVER_DATETIMEUS_to_CLIENT_DATETIMEUS(
                (uint8_t *)rec + f->offset, sizeof(server_datetimeus_t), NULL,
                NULL, (cdb2_client_datetimeus_t *)&cdt, sizeof(cdt), &null,
                &outdtsz, (void *)&outopts, NULL);
            byte_buffer_append_datetimeus(bytes, &cdt);
        }
        break;
    }

    case SERVER_INTVYM: {
        if (gbl_support_datetime_in_triggers) {
            cdb2_client_intv_ym_t cym;
            int null, outdtsz;
            struct field_conv_opts_tz outopts;

            outopts.flags = FLD_CONV_TZONE;
            strcpy(outopts.tzname, record->trans->iq->tzname);

            rc = SERVER_INTVYM_to_CLIENT_INTVYM(
                (uint8_t *)rec + f->offset, sizeof(server_intv_ym_t), NULL,
                NULL, &cym, sizeof(cym), &null, &outdtsz, (void *)&outopts,
                NULL);
            byte_buffer_append_intervalym(bytes, &cym);
        }

        break;
    }

    case SERVER_INTVDS: {
        if (gbl_support_datetime_in_triggers) {
            cdb2_client_intv_ds_t cds;
            int null, outdtsz;
            struct field_conv_opts_tz outopts;

            outopts.flags = FLD_CONV_TZONE;
            strcpy(outopts.tzname, record->trans->iq->tzname);

            rc = SERVER_INTVDS_to_CLIENT_INTVDS(
                (uint8_t *)rec + f->offset, sizeof(server_intv_ds_t), NULL,
                NULL, &cds, sizeof(cds), &null, &outdtsz, (void *)&outopts,
                NULL);
            byte_buffer_append_intervalds(bytes, &cds);
        }

        break;
    }

    case SERVER_INTVDSUS: {
        if (gbl_support_datetime_in_triggers) {
            cdb2_client_intv_dsus_t cds;
            int null, outdtsz;
            struct field_conv_opts_tz outopts;

            outopts.flags = FLD_CONV_TZONE;
            strcpy(outopts.tzname, record->trans->iq->tzname);

            rc = SERVER_INTVDSUS_to_CLIENT_INTVDSUS(
                (uint8_t *)rec + f->offset, sizeof(server_intv_dsus_t), NULL,
                NULL, &cds, sizeof(cds), &null, &outdtsz, (void *)&outopts,
                NULL);
            byte_buffer_append_intervaldsus(bytes, &cds);
        }

        break;
    }

    case SERVER_DECIMAL: {
        char str[124];
        int outdtsz;

        rc = SERVER_DECIMAL_to_CLIENT_CSTR((uint8_t *)rec + f->offset, f->len,
                                           NULL, NULL, str, sizeof(str), &null,
                                           &outdtsz, NULL, NULL);

        if (null) {
            snprintf(str, sizeof(str), "NULL");
        }

        slen = cstrlenlim(str, sizeof(str) - 1);
        byte_buffer_append_int32(bytes, slen);
        byte_buffer_append(bytes, str, slen);
        byte_buffer_append_zero(bytes, 1);
    }

    break;

    default:
        logmsg(LOGMSG_ERROR, "unknown type %d\n", f->type);
        /* abort(); */
    }
}

/* This is the actual "stored procedure" call. */
static int sp_trigger_run(struct javasp_trans_state *javasp_trans_handle,
                          struct stored_proc *p, struct sp_table *t, int event,
                          struct javasp_rec *oldrec, struct javasp_rec *newrec)
{

    struct schema *s;
    int sz;
    int rc = 0;
    struct byte_buffer bytes;
    struct field *f;
    int i;
    struct sp_field *fld;
    struct dbtable *usedb;
    int nfields = 0;

    /* TODO: can cache most of this information, don't allocate 2 buffers per
       record, don't
       need to search for offsets in schema, etc. */

    byte_buffer_init(&bytes);

    s = find_tag_schema(t->name, ".ONDISK");
    if (s == NULL) {
        logmsg(LOGMSG_ERROR, "can't find schema for %s\n", t->name);
        rc = -1;
        goto done;
    }

    append_header(&bytes, t->name, event);
    /* TODO: again, this can be much better */
    LISTC_FOR_EACH(&t->fields, fld, lnk)
    {
        for (i = 0; i < s->nmembers; i++) {
            unsigned char field_name_len;
            char field_type;
            unsigned char before_flag;
            unsigned char after_flag;

            f = &s->member[i];

            if (strcasecmp(fld->name, f->name) == 0 && (event & fld->flags)) {
                /* we need to output this field */
                field_type = type_to_sptype(f->type, f->len);

                /* if we don't know what this is, don't output it */
                if (field_type == -1)
                    continue;

                field_name_len = strlen(fld->name);
                byte_buffer_append(&bytes, &field_name_len, 1);
                byte_buffer_append(&bytes, &field_type, 1);

                /* before/after flags */
                if (oldrec == NULL)
                    before_flag = FIELD_FLAG_ABSENT;
                else if (stype_is_null((uint8_t *)oldrec->ondisk_dta +
                                       f->offset))
                    before_flag = FIELD_FLAG_NULL;
                else
                    before_flag = FIELD_FLAG_VALUE;
                if (newrec == NULL)
                    after_flag = FIELD_FLAG_ABSENT;
                else if (stype_is_null((uint8_t *)newrec->ondisk_dta +
                                       f->offset))
                    after_flag = FIELD_FLAG_NULL;
                else
                    after_flag = FIELD_FLAG_VALUE;

                byte_buffer_append(&bytes, &before_flag, 1);
                byte_buffer_append(&bytes, &after_flag, 1);

                /* field name */
                byte_buffer_append(&bytes, fld->name, field_name_len + 1);
                if (before_flag == FIELD_FLAG_VALUE) {
                    append_field(&bytes, f, oldrec);
                    nfields++;
                }
                if (after_flag == FIELD_FLAG_VALUE) {
                    append_field(&bytes, f, newrec);
                    nfields++;
                }
                break;
            }
        }
    }
    byte_buffer_append_zero(&bytes, 4);

    /* post it to queue */
    usedb = javasp_trans_handle->iq->usedb;
    javasp_trans_handle->iq->usedb = getqueuebyname(p->qname);
    rc = dbq_add(javasp_trans_handle->iq, javasp_trans_handle->trans,
                 bytes.bytes, bytes.used);
    javasp_trans_handle->iq->usedb = usedb;

done:
    byte_buffer_free(&bytes);

    return rc;
}

int javasp_trans_tagged_trigger(struct javasp_trans_state *javasp_trans_handle,
                                int event, struct javasp_rec *oldrec,
                                struct javasp_rec *newrec, const char *tblname)
{
    struct stored_proc *p;
    struct sp_table *t;
    int rc;

    if (javasp_trans_handle == NULL)
        return 0;

    SP_READLOCK();

    /* TODO: this code can be made a lot more efficient, eg
       should store list of stored procedures interested in a table's updates
       off the table structure */
    LISTC_FOR_EACH(&stored_procs, p, lnk)
    {
        LISTC_FOR_EACH(&p->tables, t, lnk)
        {
            if (strcasecmp(t->name, tblname) == 0 && (t->flags & event)) {
                rc = sp_trigger_run(javasp_trans_handle, p, t, event, oldrec,
                                    newrec);
                if (rc) {
                    SP_RELLOCK();
                    return rc;
                }
                goto nextproc;
            }
        }
    nextproc:
        ;
    }

    SP_RELLOCK();

    return 0;
}

struct javasp_rec *javasp_alloc_rec(const void *od_dta, size_t od_len,
                                    const char *tblname)
{
    struct javasp_rec *rec;
    struct schema *schema;

    schema = find_tag_schema(tblname, ".ONDISK");
    if (!schema)
        return NULL;

    if (!od_dta) {
        logmsg(LOGMSG_ERROR, "%s: cannot create record with data\n", __func__);
        return NULL;
    }

    rec = malloc(sizeof(struct javasp_rec));
    if (!rec) {
        logmsg(LOGMSG_ERROR, "javasp_alloc_rec: malloc %u failed\n",
                sizeof(struct javasp_rec));
        return NULL;
    }
    bzero(rec, sizeof(struct javasp_rec));

    strncpy(rec->tablename, tblname, sizeof(rec->tablename));
    rec->ondisk_dta = od_dta;
    rec->ondisk_dta_len = od_len;
    rec->schema = schema;

    return rec;
}

void javasp_dealloc_rec(struct javasp_rec *rec)
{
    if (rec) {
        int blobn;
        if (rec->tempbuf)
            free(rec->tempbuf);
        for (blobn = 0; blobn < MAXBLOBS; blobn++) {
            if (rec->blobs[blobn].status == BLOB_KNOWN_FREE)
                free(rec->blobs[blobn].blobdta);
        }
        free(rec);
    }
}

static int javasp_do_procedure_op_int(int op, const char *name,
                                      const char *param, const char *paramvalue)
{
    int rc;

    /*
    printf("javasp_do_procedure_op op=%d name='%s' param='%s'\n",
            op, name,  param);
    */

    switch (op) {
    case JAVASP_OP_LOAD:
        return javasp_load_procedure_int(name, param, paramvalue);
        break;

    case JAVASP_OP_RELOAD:
        rc = javasp_unload_procedure_int(name);
        if (rc == 0) {
            rc = javasp_load_procedure_int(name, param, paramvalue);
        }
        return rc;

    case JAVASP_OP_UNLOAD:
        return javasp_unload_procedure_int(name);

    default:
        logmsg(LOGMSG_ERROR, "javasp_do_procedure_op: op=%d??\n", op);
        return -1;
    }
}

int javasp_do_procedure_op(int op, const char *name, const char *param,
                           const char *paramvalue)
{
    int rc;
    SP_WRITELOCK();
    rc = javasp_do_procedure_op_int(op, name, param, paramvalue);
    SP_RELLOCK();
    return rc;
}

int javasp_trans_care_about(struct javasp_trans_state *javasp_trans_handle,
                            int event)
{
    if (javasp_trans_handle)
        return (javasp_trans_handle->events & event) == event;
    else
        return 0;
}

int javasp_custom_write(struct javasp_trans_state *javasp_trans_handle,
                        const char *opname, const void *data, size_t datalen)
{
    return 0;
}

void javasp_rec_set_trans(struct javasp_rec *rec,
                          struct javasp_trans_state *javasp_trans_handle,
                          int rrn, long long genid)
{
    if (rec) {
        rec->trans = javasp_trans_handle;
        rec->rrn = rrn;
        rec->genid = genid;
    }
}

void javasp_rec_set_blobs(struct javasp_rec *rec, blob_status_t *blobs)
{
    if (rec) {
        int blobn;
        for (blobn = 0; blobn < blobs->numcblobs; blobn++) {
            rec->blobs[blobn].status =
                (blobs->blobptrs[blobn] == NULL ? BLOB_NULL : BLOB_KNOWN);
            rec->blobs[blobn].blobdta = blobs->blobptrs[blobn];
            rec->blobs[blobn].bloboff = blobs->bloboffs[blobn];
            rec->blobs[blobn].bloblen = blobs->bloblens[blobn];
        }
    }
}

int javasp_unload_procedure_int(const char *name)
{
    struct stored_proc *sp;
    int rc;

    sp = stored_procs.top;
    while (sp) {
        if (strcasecmp(sp->name, name) == 0) {
            struct sp_table *t;
            struct sp_field *f;

            listc_rfl(&stored_procs, sp);
            free(sp->name);
            free(sp->param);
            free(sp->qname);

            t = listc_rtl(&sp->tables);
            while (t) {
                free(t->name);

                f = listc_rtl(&t->fields);
                while (f) {
                    free(f->name);
                    free(f);
                    f = listc_rtl(&t->fields);
                }

                free(t);
                t = listc_rtl(&sp->tables);
            }
            free(sp);
            break;
        }
        sp = sp->lnk.next;
    }

    return 0;
}

int javasp_unload_procedure(const char *name)
{
    int rc;

    SP_WRITELOCK();
    rc = javasp_unload_procedure_int(name);
    if (rc)
        goto done;
    /* Stop everything */
    stop_threads(thedb);
    broadcast_quiesce_threads();

    if (rc == 0) {
        rc = broadcast_procedure_op(JAVASP_OP_UNLOAD, name, "");
    }

    /* Start up again. */
    broadcast_resume_threads();
    resume_threads(thedb);

done:
    SP_RELLOCK();
    return rc;
}

int javasp_reload_procedure(const char *name, const char *jarfile,
                            const char *param)
{
    int rc;
    SP_WRITELOCK();
    rc = javasp_unload_procedure_int(name);
    if (rc)
        goto done;

    rc = javasp_load_procedure_int(name, param, NULL);

    stop_threads(thedb);
    broadcast_quiesce_threads();

    if (rc == 0) {
        rc = broadcast_procedure_op(JAVASP_OP_RELOAD, name, param);
    }

    /* Start up again. */
    broadcast_resume_threads();
    resume_threads(thedb);

    SP_RELLOCK();
done:
    return rc;
}

int javasp_add_procedure(const char *name, const char *jar, const char *param)
{
    struct stored_proc *p;
    p = malloc(sizeof(struct stored_proc));
    p->name = strdup(name);
    p->param = strdup(param);

    SP_WRITELOCK();
    listc_abl(&delayed_stored_procs, p);
    SP_RELLOCK();

    return 0;
}

static int sp_field_is_a_blob(const char *table, const char *field)
{
    struct dbtable *db;
    int fnum;
    struct field *f;

    db = get_dbtable_by_name(table);
    if (db == NULL)
        return 0;
    for (fnum = 0; fnum < db->schema->nmembers; fnum++) {
        f = &db->schema->member[fnum];
        if (strcasecmp(f->name, field) == 0) {
            /* TODO: need is_a_blob_type(type) */
            if (f->type == SERVER_BLOB || f->type == SERVER_VUTF8) {
                return 1;
            }
            break;
        }
    }
    return 0;
}

static const char *toksep = " \r\n\t";

int javasp_load_procedure_int(const char *name, const char *param,
                              const char *paramvalue)
{
    char *resource;
    char *argv[256];
    char *paramcpy = NULL;
    char *endp;
    int argc = 0;
    int i;
    int rc = 0;
    struct stored_proc *p;
    char *queue;
    struct sp_table *table = NULL;
    struct sp_field *field = NULL;
    int fd;

    char line[1024];
    SBUF2 *config = NULL;

    LISTC_FOR_EACH(&stored_procs, p, lnk)
    {
        if (strcasecmp(name, p->name) == 0 &&
            strcasecmp(param, p->qname) == 0) {
            logmsg(LOGMSG_ERROR, "stored procedure '%s' already loaded\n", name);
            rc = -1;
            goto done;
        }
    }

    p = malloc(sizeof(struct stored_proc));
    if (!p) {
        logmsg(LOGMSG_ERROR, "OOM %s\n", __func__);
        rc = -1;
        goto done;
    }
    p->name = strdup(name);
    if (!p->name) {
    oom:
        logmsg(LOGMSG_ERROR, "OOM %s\n", __func__);
        if (p->qname) free(p->qname);
        free(p);
        rc = -1;
        goto done;
    }
    if (paramvalue) {
        p->qname = strdup(name);
        if (!p->qname) goto oom;
    }
    if (param)
        p->param = strdup(param);
    else
        p->param = strdup("<sc>");
    if (!p->param) goto oom;

    listc_init(&p->tables, offsetof(struct sp_table, lnk));

    if (param) {
        paramcpy = strdup(param);
        argv[0] = strtok_r(paramcpy, toksep, &endp);
        while (argv[argc]) {
            argc++;
            argv[argc] = strtok_r(NULL, toksep, &endp);
        }

        resource = (char *)getresourcepath(argv[0]);
        if (resource == NULL) {
            logmsg(LOGMSG_ERROR, "Can't load resource for stored procedure \"%s\"\n",
                    name);
            rc = -1;
            goto done;
        }

        fd = open(resource, O_RDONLY);
        if (fd == -1) {
            logmsg(LOGMSG_ERROR, "Can't load comdb2translisten configuration from "
                            "\"%s\" for stored procedure \"%s\" %d %s\n",
                    argv[0], name, errno, strerror(errno));
            rc = -1;
            goto done;
        }
        config = sbuf2open(fd, 0);
        if (config == NULL) {
            rc = -1;
            goto done;
        }
    } else {
        /* TODO: this works but isn't nice.  Dump the config to a file, and call
         * the code to parse the file, as before. */
        FILE *f;
        int fd;
        f = tmpfile();
        if (f == NULL) {
            logmsg(LOGMSG_ERROR, 
                    "%s: can't create temp file for queue config %d %s\n",
                    __func__, errno, strerror(errno));
            rc = -1;
            goto done;
        }
        fd = fileno(f);
        fprintf(f, "%s", paramvalue);
        rewind(f);
        fd = dup(fd);
        config = sbuf2open(fd, 0);
        fclose(f);
        if (config == NULL) {
            logmsg(LOGMSG_ERROR, "%s: can't open temp file for queue config %d %s\n",
                    __func__, errno, strerror(errno));
            rc = -1;
            goto done;
        }
    }

    while (sbuf2gets(line, sizeof(line), config) >= 0) {
        char *s;

        s = strtok_r(line, toksep, &endp);
        if (s == NULL || *s == '#')
            continue;

        if (strcasecmp(s, "queue") == 0) {
            int i;

            if (paramvalue) {
                logmsg(LOGMSG_ERROR, 
                        "queue parameter ignored for new queue definition\n");
            } else {
                queue = strtok_r(NULL, toksep, &endp);
                if (queue == NULL) {
                    logmsg(LOGMSG_ERROR, "queue takes one argument (%s)\n", argv[0]);
                    rc = -1;
                    goto done;
                }
                if ((getqueuebyname(queue)) == NULL) {
                    logmsg(LOGMSG_ERROR, "queue '%s' does not exist\n", queue);
                    rc = -1;
                    goto done;
                }
                p->qname = strdup(queue);
            }
        } else if (strcasecmp(s, "table") == 0) {
            char *tablename;

            tablename = strtok_r(NULL, toksep, &endp);
            if (tablename == NULL) {
                logmsg(LOGMSG_ERROR, 
                        "table takes one argument (table %s, config file %s)\n",
                        tablename, argv[0]);
                rc = -1;
                goto done;
            }
            if (strtok_r(NULL, toksep, &endp)) {
                logmsg(LOGMSG_ERROR, "Unexpected text after table name\n");
                rc = -1;
                goto done;
            }
#if 0 // temp disable check - can have two different sp for different operations
      // on same table??
            LISTC_FOR_EACH(&p->tables, table, lnk) {
                if (strcasecmp(table->name, tablename) == 0) {
                    fprintf(stderr, "table '%s' already configured (config file %s)\n", tablename, argv[0]);
                    rc = -1;
                    goto done;
                }
            }
#endif
            table = malloc(sizeof(struct sp_table));
            table->name = strdup(tablename);
            table->flags = 0;
            listc_init(&table->fields, offsetof(struct sp_field, lnk));
            listc_abl(&p->tables, table);
        } else if (strcasecmp(s, "field") == 0) {
            char *fieldname;
            char *flagname;
            int flags;

            flags = 0;

            if (table == NULL) {
                logmsg(LOGMSG_ERROR, "table declaration must precede field list "
                                "(config file %s)\n",
                        argv[0]);
                rc = -1;
                goto done;
            }
            fieldname = strtok_r(NULL, toksep, &endp);
            if (fieldname == NULL) {
                logmsg(LOGMSG_ERROR, "field name not specified (config file %s)\n",
                        argv[0]);
                rc = -1;
                goto done;
            }

            LISTC_FOR_EACH(&table->fields, field, lnk)
            {
                if (strcasecmp(field->name, fieldname) == 0) {
                    logmsg(LOGMSG_ERROR, 
                            "field %s already defined (config file %s)\n",
                            field->name, argv[0]);
                    rc = -1;
                    goto done;
                }
            }

            flagname = strtok_r(NULL, toksep, &endp);
            if (flagname == NULL) {
                logmsg(LOGMSG_ERROR, "field %s at least one event type required "
                                "(config file %s)\n",
                        fieldname, argv[0]);
                rc = -1;
                goto done;
            }
            while (flagname) {
                if (strcasecmp(flagname, "add") == 0)
                    flags |= JAVASP_TRANS_LISTEN_AFTER_ADD;
                else if (strcasecmp(flagname, "del") == 0) {
                    flags |= JAVASP_TRANS_LISTEN_AFTER_DEL;
                    if (sp_field_is_a_blob(table->name, fieldname))
                        flags |= JAVASP_TRANS_LISTEN_SAVE_BLOBS_DEL;
                } else if (strcasecmp(flagname, "pre_upd") == 0) {
                    flags |= JAVASP_TRANS_LISTEN_BEFORE_UPD;
                    if (sp_field_is_a_blob(table->name, fieldname))
                        flags |= JAVASP_TRANS_LISTEN_SAVE_BLOBS_UPD;
                } else if (strcasecmp(flagname, "post_upd") == 0)
                    flags |= JAVASP_TRANS_LISTEN_AFTER_UPD;
                else {
                    logmsg(LOGMSG_ERROR, "field %s unknown flag (config file %s)\n",
                            fieldname, argv[0]);
                    rc = -1;
                    goto done;
                }
                flagname = strtok_r(NULL, toksep, &endp);
            }
            field = malloc(sizeof(struct sp_field));
            field->name = strdup(fieldname);
            field->flags = flags;
            table->flags |= flags;
            listc_abl(&table->fields, field);
            p->flags |= flags;
        } else {
            logmsg(LOGMSG_ERROR, 
                "unknown translisten config directive %s (config file %s)\n", s,
                param ? argv[0] : "<from comdb2sc>");
            rc = -1;
            goto done;
        }
    }
    listc_abl(&stored_procs, p);

done:
    if (paramcpy)
        free(paramcpy);
    if (config)
        sbuf2close(config);
    return rc;
}

int javasp_load_procedure(const char *name, const char *jarfile,
                          const char *param)
{
    int rc;
    SP_WRITELOCK();
    rc = javasp_load_procedure_int(name, param, NULL);
    SP_RELLOCK();
    return rc;
}

void javasp_rec_have_blob(struct javasp_rec *rec, int blobn,
                          const void *blobdta, size_t bloboff, size_t bloblen)
{
    if (rec) {
        rec->blobs[blobn].status = (blobdta == NULL ? BLOB_NULL : BLOB_KNOWN);
        rec->blobs[blobn].blobdta = (char *)blobdta;
        rec->blobs[blobn].bloboff = bloboff;
        rec->blobs[blobn].bloblen = bloblen;
    }
}

int javasp_exists(const char *name)
{
    struct stored_proc *sp;
    SP_READLOCK();
    LISTC_FOR_EACH(&stored_procs, sp, lnk)
    {
        if (strcmp(sp->name, name) == 0)
            break;
    }
    SP_RELLOCK();
    return sp != NULL;
}

static void get_trigger_info_int(const char *name, trigger_info *info)
{
    listc_init(info, offsetof(trigger_tbl_info, lnk));
    struct stored_proc *p;
    LISTC_FOR_EACH(&stored_procs, p, lnk)
    {
        if (strcmp(name, p->name) == 0) break;
    }
    if (p == NULL) return;
    struct sp_table *t;
    LISTC_FOR_EACH(&p->tables, t, lnk)
    {
        trigger_tbl_info *i = malloc(sizeof(trigger_tbl_info));
        i->name = strdup(t->name);
        listc_init(&i->cols, offsetof(trigger_col_info, lnk));
        struct sp_field *f;
        LISTC_FOR_EACH(&t->fields, f, lnk)
        {
            trigger_col_info *col;
            if (f->flags & JAVASP_TRANS_LISTEN_AFTER_ADD) {
                col = malloc(sizeof(trigger_col_info));
                col->name = strdup(f->name);
                col->type = JAVASP_TRANS_LISTEN_AFTER_ADD;
                listc_atl(&i->cols, col);
            }
            if (f->flags & JAVASP_TRANS_LISTEN_AFTER_DEL) {
                col = malloc(sizeof(trigger_col_info));
                col->name = strdup(f->name);
                col->type = JAVASP_TRANS_LISTEN_AFTER_DEL;
                listc_atl(&i->cols, col);
            }
            if (f->flags & JAVASP_TRANS_LISTEN_BEFORE_UPD) {
                col = malloc(sizeof(trigger_col_info));
                col->name = strdup(f->name);
                col->type = JAVASP_TRANS_LISTEN_AFTER_UPD;
                listc_atl(&i->cols, col);
            }
        }
        listc_atl(info, i);
    }
}

void get_trigger_info(const char *name, trigger_info *info)
{
    SP_READLOCK();
    get_trigger_info_int(name, info);
    SP_RELLOCK();
}
