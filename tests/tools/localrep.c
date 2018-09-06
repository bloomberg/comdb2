#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdarg.h>
#include <stdint.h>

#include <cdb2api.h>
#include <arpa/inet.h>

#include "strbuf.h"

int sync(cdb2_hndl_tp *from, int64_t maxfrom, cdb2_hndl_tp *to, int64_t maxto);

void consume(cdb2_hndl_tp *db) {
    int rc = CDB2_OK;
    while (rc == CDB2_OK) {
        rc = cdb2_next_record(db);
    }
}

int64_t query_int(cdb2_hndl_tp *db, char *sql, ...) {
    int64_t ret;
    int rc;
    va_list args;
    char sqlbuf[1024];

    va_start(args, sql);
    vsnprintf(sqlbuf, sizeof(sqlbuf), sql, args);

    cdb2_clearbindings(db);
    rc = cdb2_run_statement(db, sqlbuf);
    if (rc != CDB2_OK) {
        fprintf(stderr, "err %s: %d %s\n", sqlbuf, rc, cdb2_errstr(db));
        return -1;
    }
    rc = cdb2_next_record(db);
    if (rc != CDB2_OK)
        return -1;
    if (cdb2_column_value(db, 0) == NULL) {
        consume(db);
        return 0;
    }
    ret = *(int64_t*) cdb2_column_value(db, 0);
    consume(db);

    va_end(args);
    return ret;
}

/* op types */
enum {
    LCL_OP_ADD    =   1,
    LCL_OP_DEL      = 2,
    LCL_OP_COMMIT   = 3,
    LCL_OP_TOOBIG   = 4,
    LCL_OP_CLEAR    = 5,
    LCL_OP_ANALYZE  = 6
};

#define MAXTABLELEN 32

typedef struct
{
  char         name[MAXTABLELEN]; /* name of field as a \0 terminated string */
  int          type;              /* one of the types in dynschematypes.h    */
  unsigned int len;               /* length of field in bytes                */
  unsigned int off;               /* offset of field in record structure     */
  int          isnull;
  int          reserved[4];
} comdb2_field_type;


enum comdb2_fielddatatypes {
    COMDB2_INT = 1,
    COMDB2_UINT = 2,
    COMDB2_SHORT = 3,
    COMDB2_USHORT = 4,
    COMDB2_FLOAT = 5,
    COMDB2_DOUBLE = 6,
    COMDB2_BYTE = 7,
    COMDB2_CSTR = 8,
    COMDB2_PSTR = 9,
    COMDB2_LONGLONG = 10,
    COMDB2_ULONGLONG = 11,
    COMDB2_BLOB = 12,
    COMDB2_DATETIME = 13,
    COMDB2_UTF8 = 14,
    COMDB2_VUTF8 = 15,
    COMDB2_INTERVALYM = 16,
    COMDB2_INTERVALDS = 17,
    COMDB2_DATETIMEUS = 18,
    COMDB2_INTERVALDSUS = 19,
    COMDB2_LAST_TYPE
};

 
static char *typestr(int type) {
    switch (type) {
        case COMDB2_INT:
            return "int";
        case COMDB2_UINT:
            return "uint";
        case COMDB2_SHORT:
            return "short";
        case COMDB2_USHORT:
            return "ushort";
        case COMDB2_FLOAT:
            return "float";
        case COMDB2_DOUBLE:
            return "double";
        case COMDB2_BYTE:
            return "byte";
        case COMDB2_CSTR:
            return "cstr";
        case COMDB2_PSTR:
            return "pstr";
        case COMDB2_LONGLONG:
            return "longlong";
        case COMDB2_ULONGLONG:
            return "ulonglong";
        case COMDB2_BLOB:
            return "blob";
        case COMDB2_DATETIME:
            return "datetime";
        case COMDB2_UTF8:
            return "utf8";
        case COMDB2_VUTF8:
            return "vutf8";
        case COMDB2_INTERVALYM:
            return "intervalym";
        case COMDB2_INTERVALDS:
            return "intervalds";
        case COMDB2_DATETIMEUS:
            return "datetimeus";
        case COMDB2_INTERVALDSUS:
            return "intervaldsus";
        default:
            return "???";
    }
}

union twin
{
    uint64_t u64;
    uint32_t u32[2];
    uint8_t  u8[8];
};
typedef union twin twin_t;

/* from comdb2 code */
uint64_t flibc_ntohll(uint64_t net_order)
{
    uint64_t host_order;

#ifdef _LINUX_SOURCE
    twin_t net_order_twin;
    twin_t host_order_twin;

    net_order_twin.u64 = net_order;

    host_order_twin.u32[0] = ntohl(net_order_twin.u32[1]);
    host_order_twin.u32[1] = ntohl(net_order_twin.u32[0]);

    host_order = host_order_twin.u64;
#else
    host_order = net_order;
#endif

    return host_order;
}

void hexdump(void *pp, int len) {
    uint8_t *p = (uint8_t*) pp;
    for (int i = 0; i < len; i++) {
        printf("%02x", p[i]);
    }
    printf("\n");
}

static void flip_value(void *value, int type) {
    uint32_t i;
    uint16_t s;
    uint64_t l;
    cdb2_client_datetime_t *d; 
    cdb2_client_intv_ym_t *ym;
    cdb2_client_intv_ds_t *ds;

    switch (type) {
        case COMDB2_INT:
        case COMDB2_UINT:
        case COMDB2_FLOAT:
            memcpy(&i, value, sizeof(uint32_t));
            i = ntohl(i);
            memcpy(value, &i, sizeof(uint32_t));
            break;

        case COMDB2_SHORT:
        case COMDB2_USHORT:
            memcpy(&s, value, sizeof(uint16_t));
            s = ntohs(s);
            memcpy(value, &s, sizeof(uint16_t));
            break;

        case COMDB2_DOUBLE:
        case COMDB2_LONGLONG:
        case COMDB2_ULONGLONG:
            memcpy(&l, value, sizeof(uint64_t));
            l = flibc_ntohll(l);
            memcpy(value, &l, sizeof(uint64_t));
            break;

        case COMDB2_CSTR:
        case COMDB2_PSTR:
        case COMDB2_BLOB: 
        case COMDB2_BYTE:
            /* not flipped */
            break;

        case COMDB2_DATETIME:
            d = (cdb2_client_datetime_t*) value;
            d->tm.tm_sec = ntohl(d->tm.tm_sec);
            d->tm.tm_min = ntohl(d->tm.tm_min);
            d->tm.tm_hour = ntohl(d->tm.tm_hour);
            d->tm.tm_mday = ntohl(d->tm.tm_mday);
            d->tm.tm_mon = ntohl(d->tm.tm_mon);
            d->tm.tm_year = ntohl(d->tm.tm_year);
            d->tm.tm_wday = ntohl(d->tm.tm_wday);
            d->tm.tm_yday = ntohl(d->tm.tm_yday);
            d->tm.tm_isdst = ntohl(d->tm.tm_isdst);
            d->msec = ntohl(d->msec);
            break;

        case COMDB2_INTERVALYM:
            ym = (cdb2_client_intv_ym_t*) value;
            ym->sign = ntohl(ym->sign);
            ym->years = ntohl(ym->years);
            ym->months = ntohl(ym->months);
            break;

        case COMDB2_INTERVALDS:
            ds = (cdb2_client_intv_ds_t*) value;
            ds->sign = ntohl(ds->sign);
            ds->days = ntohl(ds->days);
            ds->hours = ntohl(ds->hours);
            ds->mins = ntohl(ds->mins);
            ds->sec = ntohl(ds->sec);
            ds->msec = ntohl(ds->msec);
            break;
    }   
}


#define ASSIGN(ctype, cdb2_type, cdb2_var) do {          \
    ctype _v;                                            \
    memcpy(&_v, ops + fld->off, sizeof(_v) );            \
    u->cdb2_var = _v;                                    \
    type = cdb2_type;                                    \
    addr = &u->cdb2_var;                                 \
    len = sizeof(u->cdb2_var);                           \
} while(0);

int bind_value(cdb2_hndl_tp *db, void *opsp, int opsz, comdb2_field_type *fld, void **ret) {
    union val {
        int64_t ival;
        double dval;
        char *sval;
        void *bval;
        cdb2_client_datetime_t dtval;
    } *u;
    int rc;
    uint8_t *ops = (uint8_t*) opsp;
    int type, len;
    void *addr;

    /* TODO: other datetime/interval types */

    flip_value(ops + fld->off, fld->type);
    u = malloc(sizeof(union val));
    *ret = u;

    if (fld->isnull) {
        type = CDB2_INTEGER;
        addr = NULL;
        len = 0;
    }
    else {
        switch (fld->type) {
            case COMDB2_FLOAT:
                ASSIGN(float, CDB2_REAL, dval);
                break;

            case COMDB2_DOUBLE:
                ASSIGN(double, CDB2_REAL, dval);
                break;

            case COMDB2_SHORT:
                ASSIGN(int16_t, CDB2_INTEGER, ival);
                break;

            case COMDB2_USHORT:
                ASSIGN(uint16_t, CDB2_INTEGER, ival);
                break;

            case COMDB2_LONGLONG:
                ASSIGN(int64_t, CDB2_INTEGER, ival);
                break;

            case COMDB2_ULONGLONG:
                ASSIGN(uint64_t, CDB2_INTEGER, ival);
                break;

            case COMDB2_INT:
                ASSIGN(int32_t, CDB2_INTEGER, ival);
                break;

            case COMDB2_UINT:
                ASSIGN(uint32_t, CDB2_INTEGER, ival);
                break;

            case COMDB2_CSTR:
            case COMDB2_PSTR:
                addr = ops + fld->off;
                len = strlen(addr); //fld->len;
                type = CDB2_CSTRING;
                break;

            case COMDB2_BYTE:
            case COMDB2_BLOB: 
                addr = ops + fld->off;
                len = fld->len;
                type = CDB2_BLOB;
                break;

            case COMDB2_DATETIME:
                ASSIGN(cdb2_client_datetime_t, CDB2_DATETIME, dtval);
                break;
        }
    }
    rc = cdb2_bind_param(db, fld->name, type, addr, len);
    // printf("bind %s type %d addr %p rc %d\n", fld->name, type, addr, rc);
    if (rc) {
        fprintf(stderr, "bind %s rc %d\n", fld->name, type, rc);
        return 1;
    }
    return rc;
}

#undef ASSIGN
struct delop {
    char table[MAXTABLELEN];
    long long id;
};

int apply_del(cdb2_hndl_tp *db, void *opsp, int opsz) {
    struct delop *delop;
    int rc;
    delop = (struct delop*) opsp;
    delop->id = flibc_ntohll(delop->id);
    cdb2_clearbindings(db);
    cdb2_bind_param(db, "id", CDB2_INTEGER, &delop->id, sizeof(int64_t));

    strbuf *sql = strbuf_new();
    strbuf_appendf(sql, "delete from %s where comdb2_seqno = @id", delop->table);
    char *s;
    s = strbuf_disown(sql);
    strbuf_free(sql);

    rc = cdb2_run_statement(db, s);
    if (rc) {
        fprintf(stderr, "failed deleting id %llx from %s\n", delop->id, delop->table);
        return 1;
    }
    return 0;
}

int apply_add(cdb2_hndl_tp *db, void *opsp, int opsz) {
    int nfields;
    uint8_t *ops = (uint8_t*) opsp;
    char *table;
    comdb2_field_type *fld;
    strbuf *sql;
    int rc;
    void **bound_values = NULL;
    sql = strbuf_new();

    /* extract number of fields */
    memcpy(&nfields, ops, sizeof(int));
    nfields = ntohl(nfields);

    ops += sizeof(int);
    /* extract table */
    table = ops;
    ops += MAXTABLELEN;

    /* extract field descriptions */
    fld = (comdb2_field_type *) ops;

    /* construct sql */
    strbuf_appendf(sql, "insert into %s(", table);

    /* add each field to tag */
    for (int i = 0; i < nfields; i++)
        strbuf_appendf(sql, "\"%s\"%s", fld[i].name, i == nfields-1 ?  "" : ", ");
    strbuf_append(sql, ") values(");

    for (int i = 0; i < nfields; i++) {
        strbuf_appendf(sql, "@%s%s", fld[i].name, i == nfields-1 ?  "" : ", ");
    }
    strbuf_append(sql, ")");

    cdb2_clearbindings(db);
    bound_values = calloc(nfields, sizeof(void*));
    for (int i = 0; i < nfields; i++) {
        fld[i].type = htonl(fld[i].type);
        fld[i].len = htonl(fld[i].len);
        fld[i].off = htonl(fld[i].off);
        fld[i].isnull = htonl(fld[i].isnull);

        rc = bind_value(db, opsp, opsz, &fld[i], &bound_values[i]);
        if (rc)
            goto done;

        // printf("%d %s type %d %s len %d off %d ??? %d\n", i, fld[i].name, fld[i].type, typestr(fld[i].type), fld[i].len, fld[i].off, fld[i].reserved[0]);
    }
    char *s = strbuf_disown(sql);
    rc = cdb2_run_statement(db, s);
    // printf(">> %s rc %d %s\n", s, rc, cdb2_errstr(db));
    free(s);

done:
    if (bound_values) {
        for (int i = 0; i < nfields; i++)
            free(bound_values[i]);
        free(bound_values);
    }
    strbuf_free(sql);
    return rc;
}

int apply_clear(cdb2_hndl_tp *db, void *opsp, int opsz)
{
    char *table = (char *)opsp;
    int rc;
    cdb2_clearbindings(db);

    strbuf *sql = strbuf_new();
    strbuf_appendf(sql, "truncate %s", table);
    char *s;
    s = strbuf_disown(sql);
    strbuf_free(sql);

    rc = cdb2_run_statement(db, s);
    if (rc) {
        fprintf(stderr, "failed truncating %s\n", table);
        return 1;
    }
    return 0;
}

int apply_op(cdb2_hndl_tp *db, int64_t seqno, int64_t blkpos, int64_t type, void *ops, int opsz) {
    uint8_t *p;
    int rc;
    // printf("apply_op seqno %lld blkpos %lld type %d ops ", seqno, blkpos, type);
    // hexdump(ops, opsz);
    p = (uint8_t*) ops;

    void *opscpy = NULL;
    if (ops) {
        opscpy = malloc(opsz);
        memcpy(opscpy, ops, opsz);
    }

    cdb2_clearbindings(db);
    cdb2_bind_param(db, "seqno", CDB2_INTEGER, &seqno, sizeof(int64_t));
    cdb2_bind_param(db, "blkpos", CDB2_INTEGER, &blkpos, sizeof(int64_t));
    cdb2_bind_param(db, "optype", CDB2_INTEGER, &type, sizeof(int64_t));
    cdb2_bind_param(db, "ops", CDB2_BLOB, opscpy, opsz);
    rc = cdb2_run_statement(db, "insert into comdb2_oplog(seqno, blkpos, optype, ops) values(@seqno, @blkpos, @optype, @ops)");
    if (rc) {
        fprintf(stderr, "failed inserting into destination oplog %d %s seqno %lld blkseq %lld\n", rc, cdb2_errstr(db), seqno, blkpos);
        free(opscpy);
        return 1;
    }

    switch (type) {
        case LCL_OP_ADD:
            rc = apply_add(db, ops, opsz);
            break;
        case LCL_OP_DEL:
            rc = apply_del(db, ops, opsz);
            break;
        case LCL_OP_COMMIT:
            rc = cdb2_run_statement(db, "commit");
            if (rc)
                fprintf(stderr, "commit seqno %lld rc %d %s\n", seqno, rc, cdb2_errstr(db));
            break;
        /* TODO: others! */
        case LCL_OP_CLEAR:
            rc = apply_clear(db, ops, opsz);
            if (rc)
                fprintf(stderr, "apply_clear seqno %lld rc %d %s\n", seqno, rc,
                        cdb2_errstr(db));
            break;
        case LCL_OP_ANALYZE:
            break;
        default:
            fprintf(stderr, "unknown op %d\n", type);
            free(opscpy);
            return 1;
    }
    free(opscpy);
    return rc;
}

int apply_seqno(cdb2_hndl_tp *from, cdb2_hndl_tp *to, int64_t seqno, int maxblkpos) {
    int havetrans = 0;
    int rc;

    rc = cdb2_run_statement(to, "begin");
    if (rc) {
        fprintf(stderr, "begin failed for seqno %lld: %d %s\n", seqno, rc, cdb2_errstr(to));
        return 1;
    }
    havetrans = 1;

    for (int64_t blkpos = 0; blkpos <= maxblkpos; blkpos++) {
        cdb2_clearbindings(from);
        cdb2_clearbindings(to);

        cdb2_bind_param(from, "seqno", CDB2_INTEGER, &seqno, sizeof(int64_t));
        cdb2_bind_param(from, "blkpos", CDB2_INTEGER, &blkpos, sizeof(int64_t));
        rc = cdb2_run_statement(from, "select optype, ops from comdb2_oplog where seqno=@seqno and blkpos=@blkpos");
        if (rc) {
            fprintf(stderr, "rc %d %s: getting event for seqno %lld blkpos %lld\n", rc, cdb2_errstr(from), seqno, blkpos);
            goto done;
        }

        rc = cdb2_next_record(from);
        if (rc) {
            fprintf(stderr, "next rc %d %s\n", rc, cdb2_errstr(from));
            goto done;
        }
        int optype = *(int64_t*)cdb2_column_value(from, 0);

        if (optype == LCL_OP_COMMIT)
            havetrans = 0;

        void *ops = cdb2_column_value(from, 1);
        int opsz = cdb2_column_size(from, 1);

        rc = apply_op(to, seqno, blkpos, optype, ops, opsz);
        if (rc)
            goto done;

    }
done:
    if (rc) {
        fprintf(stderr, "done with rc %d\n", rc);
    }
    if (havetrans) {
        int arc = cdb2_run_statement(to, "rollback");
        if (arc) {
            fprintf(stderr, "rollback failed for seqno %lld: %d %s\n", seqno, arc, cdb2_errstr(to));
            return 1;
        }
        consume(from);
    }
    return rc;
}

int sync(cdb2_hndl_tp *from, int64_t maxfrom, cdb2_hndl_tp *to, int64_t maxto) {
    int64_t nops;

    for (int64_t seqno = maxto+1; seqno <= maxfrom; seqno++) {
        int blkpos = (int) query_int(from, "select max(blkpos) from comdb2_oplog where seqno = %lld", seqno);
        // printf("seqno %lld blkpos %d\n", seqno, blkpos);
        int rc;
        rc = apply_seqno(from, to, seqno, blkpos);
        if (rc) {
            fprintf(stderr, "Failed for seqno %lld\n", seqno);
            return 1;
        }
    }
    return 0;
} 

int apply(char *fromdb, char *todb) {
    int64_t maxfrom, maxto;
    int rc;

    cdb2_hndl_tp *from, *to;
    rc = cdb2_open(&from, fromdb, "default", 0);
    if (rc) {
        fprintf(stderr, "can't open %s\n", fromdb);
        return 1;
    }
    rc = cdb2_open(&to, todb, "local", 0);
    if (rc) {
        fprintf(stderr, "can't open %s\n", todb);
        return 1;
    }
    maxfrom = query_int(from, "select max(seqno) from comdb2_oplog");
    maxto = query_int(to, "select max(seqno) from comdb2_oplog");
    // printf("from %lld to %lld\n", (long long) maxfrom, (long long) maxto);
    return sync(from, maxfrom, to, maxto);
}

int main(int argc, char *argv[]) {
    char *fromdb, *todb;

    if (getenv("CDB2_CONFIG"))
        cdb2_set_comdb2db_config(getenv("CDB2_CONFIG"));

    if (argc != 3) {
        fprintf(stderr, "usage: fromdb todb\n");
        return 1;
    }
    fromdb = argv[1];
    todb = argv[2];

    return apply(fromdb, todb);
}
