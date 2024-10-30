#include <log_queue_trigger.h>
#include <bdb_queue.h>
#include <translistener.h>
#include <fsnapf.h>

#ifdef WITH_QKAFKA

#include "librdkafka/rdkafka.h"

#endif

#define copy(dest, type, src, conv)                                                                                    \
    dest = conv(*(type *)src);                                                                                         \
    src += sizeof(dest)

#define copyprint(dest, type, src, conv, pr, oldnew, name, f)                                                          \
    copy(dest, type, src, conv);                                                                                       \
    pr(oldnew, name, dest, f)

static void print_integer(const char *oldnew, const char *name, long long ival, FILE *f)
{
    fprintf(f, "(integer) %s -> %s: %lld\n", oldnew, name, ival);
}

static void print_float(const char *oldnew, const char *name, double fval, FILE *f)
{
    fprintf(f, "(float) %s -> %s: %f\n", oldnew, name, fval);
}

static void print_string(const char *oldnew, const char *name, const char *sval, FILE *f)
{
    fprintf(f, "(string) %s -> %s: %s\n", oldnew, name, sval);
}

static void print_blob(const char *oldnew, const char *name, const u_int8_t *bval, int len, FILE *f)
{
    fprintf(f, "(blob) %s -> %s: %d bytes\n", oldnew, name, len);
    fsnapf(f, bval, len);
}

static void print_datetime(const char *oldnew, const char *name, const datetime_t *dt, FILE *f)
{
    logmsg(LOGMSG_USER,
           "(datetime) %s -> %s: prec: %d sec: %d min: %d hour: %d mday: %d mon: %d year: %d wday: %d yday: %d isdst: "
           "%d frac: %d\n",
           oldnew, name, dt->prec, dt->tm.tm_sec, dt->tm.tm_min, dt->tm.tm_hour, dt->tm.tm_mday, dt->tm.tm_mon,
           dt->tm.tm_year, dt->tm.tm_wday, dt->tm.tm_yday, dt->tm.tm_isdst, dt->frac);
}

static void print_intervalym(const char *oldnew, const char *name, const intv_t *in, FILE *f)
{
    fprintf(f, "(intervalym) %s -> %s: type: %d sign: %d years: %d months: %d\n", oldnew, name, in->type, in->sign,
            in->u.ym.years, in->u.ym.months);
}

static void print_intervalds(const char *oldnew, const char *name, const intv_t *in, FILE *f)
{
    fprintf(f, "(intervalds) %s -> %s: type: %d sign: %d days: %d hours: %d mins: %d sec: %d msec: %d\n", oldnew, name,
            in->type, in->sign, in->u.ds.days, in->u.ds.hours, in->u.ds.mins, in->u.ds.sec, in->u.ds.frac);
}

static void print_trigger_null(char *oldnew, char *name, FILE *f)
{
    fprintf(f, "(null) %s -> %s\n", oldnew, name);
}

extern void client_datetime_to_datetime_t(const cdb2_client_datetime_t *cdt, datetime_t *datetime, int flip);
extern void client_datetimeus_to_datetime_t(const cdb2_client_datetimeus_t *cdt, datetime_t *datetime, int flip);

static uint8_t *print_trigger_field(char *oldnew, char *name, uint8_t type, uint8_t *payload, FILE *f)
{
    if (payload == NULL)
        return NULL;
    union {
        int16_t i16;
        uint16_t u16;
        int32_t i32;
        uint32_t u32;
        int64_t i64;
        uint64_t u64;
        float f;
        double d;
        char *s;
        void *b;
        datetime_t dt;
        intv_t in;
    } u;
    int32_t szstr = 0;
    u_int8_t *blob = NULL;
    char *string = NULL;
    cdb2_client_intv_ym_t *ym;
    cdb2_client_intv_ds_t *ds;
    cdb2_client_intv_dsus_t *dsus;

    switch (type) {
    case SP_FIELD_INT16:
        copyprint(u.i16, int16_t, payload, ntohs, print_integer, oldnew, name, f);
        break;
    case SP_FIELD_UINT16:
        copyprint(u.u16, uint16_t, payload, ntohs, print_integer, oldnew, name, f);
        break;
    case SP_FIELD_INT32:
        copyprint(u.i32, int32_t, payload, ntohl, print_integer, oldnew, name, f);
        break;
    case SP_FIELD_UINT32:
        copyprint(u.u32, uint32_t, payload, ntohl, print_integer, oldnew, name, f);
        break;
    case SP_FIELD_INT64:
        copyprint(u.i64, int64_t, payload, flibc_ntohll, print_integer, oldnew, name, f);
        break;
    case SP_FIELD_UINT64:
        copyprint(u.u64, uint64_t, payload, flibc_ntohll, print_integer, oldnew, name, f);
        break;
    case SP_FIELD_REAL32:
        copyprint(u.f, float, payload, flibc_ntohf, print_float, oldnew, name, f);
        break;
    case SP_FIELD_REAL64:
        copyprint(u.d, double, payload, flibc_ntohd, print_float, oldnew, name, f);
        break;
    case SP_FIELD_STRING:
        copy(szstr, int32_t, payload, ntohl);
        string = malloc(szstr + 1);
        memcpy(string, payload, szstr);
        string[szstr] = '\0';
        payload += szstr + 1;
        print_string(oldnew, name, string, f);
        free(string);
        break;
    case SP_FIELD_BLOB:
    case SP_FIELD_BYTEARRAY:
        copy(szstr, int32_t, payload, ntohl);
        blob = malloc(szstr);
        memcpy(blob, payload, szstr);
        payload += szstr;
        print_blob(oldnew, name, blob, szstr, f);
        free(blob);
        break;
    case SP_FIELD_DATETIME:
#ifdef _LINUX_SOURCE
        client_datetime_to_datetime_t((cdb2_client_datetime_t *)payload, &u.dt, 1);
#else
        client_datetime_to_datetime_t((cdb2_client_datetime_t *)payload, &u.dt, 0);
#endif
        print_datetime(oldnew, name, &u.dt, f);
        payload += sizeof(cdb2_client_datetime_t);
        break;
    case SP_FIELD_DATETIMEUS:
#ifdef _LINUX_SOURCE
        client_datetimeus_to_datetime_t((cdb2_client_datetimeus_t *)payload, &u.dt, 1);
#else
        client_datetimeus_to_datetime_t((cdb2_client_datetimeus_t *)payload, &u.dt, 0);
#endif
        print_datetime(oldnew, name, &u.dt, f);
        payload += sizeof(cdb2_client_datetimeus_t);
        break;
    case SP_FIELD_INTERVALYM:
        ym = (cdb2_client_intv_ym_t *)payload;
        u.in.type = INTV_YM_TYPE;
        u.in.sign = ntohl(ym->sign);
        u.in.u.ym.years = ntohl(ym->years);
        u.in.u.ym.months = ntohl(ym->months);
        print_intervalym(oldnew, name, &u.in, f);
        payload += sizeof(cdb2_client_intv_ym_t);
        break;
    case SP_FIELD_INTERVALDS:
        ds = (cdb2_client_intv_ds_t *)payload;
        u.in.type = INTV_DS_TYPE;
        u.in.sign = ntohl(ds->sign);
        u.in.u.ds.days = ntohl(ds->days);
        u.in.u.ds.hours = ntohl(ds->hours);
        u.in.u.ds.mins = ntohl(ds->mins);
        u.in.u.ds.sec = ntohl(ds->sec);
        u.in.u.ds.frac = ntohl(ds->msec);
        u.in.u.ds.prec = DTTZ_PREC_MSEC;
        u.in.u.ds.conv = 1;
        print_intervalds(oldnew, name, &u.in, f);
        payload += sizeof(cdb2_client_intv_ds_t);
        break;
    case SP_FIELD_INTERVALDSUS:
        dsus = (cdb2_client_intv_dsus_t *)payload;
        u.in.type = INTV_DSUS_TYPE;
        u.in.sign = ntohl(dsus->sign);
        u.in.u.ds.days = ntohl(dsus->days);
        u.in.u.ds.hours = ntohl(dsus->hours);
        u.in.u.ds.mins = ntohl(dsus->mins);
        u.in.u.ds.sec = ntohl(dsus->sec);
        u.in.u.ds.frac = ntohl(dsus->usec);
        u.in.u.ds.prec = DTTZ_PREC_USEC;
        u.in.u.ds.conv = 1;
        print_intervalds(oldnew, name, &u.in, f);
        payload += sizeof(cdb2_client_intv_dsus_t);
        break;
    }
    return payload;
}

/* From lua/sp.c */
static void *print_field(uint8_t *payload, FILE *f)
{
    uint8_t szfld = *payload;
    if (szfld == 0) {
        return NULL;
    }
    payload += 1;
    uint8_t type = *payload;
    payload += 1;
    uint8_t before = *payload;
    payload += 1;
    uint8_t after = *payload;
    payload += 1;
    char fld[szfld + 1];
    memcpy(fld, payload, szfld);
    fld[szfld] = '\0';
    payload += szfld + 1;

    if (before == FIELD_FLAG_NULL) {
        print_trigger_null("old", fld, f);
    } else if (before == FIELD_FLAG_VALUE) {
        payload = print_trigger_field("old", fld, type, payload, f);
    }

    if (after == FIELD_FLAG_NULL) {
        print_trigger_null("new", fld, f);
    } else if (after == FIELD_FLAG_VALUE) {
        payload = print_trigger_field("new", fld, type, payload, f);
    }
    return payload;
}

int bdb_queuedb_trigger_unpack(bdb_state_type *bdb_state, const DBT *key, const DBT *data, int *consumer,
                               uint64_t *genid, struct bdb_queue_found **fnd, size_t *fnddtalen, size_t *fnddtaoff,
                               long long *seq);

static void dump_qtrigger(bdb_state_type *bdb_state, const DB_LSN *commit_lsn, const char *filename, const DBT *key,
                          const DBT *data, void *userptr)
{
    FILE *f = userptr;
    struct bdb_queue_found *fnd = {0};
    size_t fnddtalen = 0, fnddtaoff = 0;
    int consumer;
    long long seq = 0;
    uint64_t genid = 0;
    int rc = bdb_queuedb_trigger_unpack(bdb_state, key, data, &consumer, &genid, &fnd, &fnddtalen, &fnddtaoff, &seq);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "Failed to unpack record for %s\n", filename);
        return;
    }

    uint8_t *payload = ((uint8_t *)fnd) + fnd->data_offset;
    size_t len = fnd->data_len;
    uint8_t zeros[4] = {0};
    if (memcmp(payload + len - 4, zeros, sizeof(zeros)) != 0) {
        logmsg(LOGMSG_ERROR, "Invalid record for %s\n", filename);
        return;
    }

    len -= 4;
    uint8_t *end = payload + len;
    payload += 40;

    int32_t flags = ntohl(*(int *)(payload));
    payload += 4;

    int sztbl = ntohs(*(short *)(payload)) + 1;
    payload += 2;

    char tbl[sztbl];
    strcpy(tbl, (char *)payload);
    payload += sztbl;

    char *type = "UNSET";

    if (flags & TYPE_TAGGED_ADD) {
        type = "ADD";
    } else if (flags & TYPE_TAGGED_DEL) {
        type = "DEL";
    } else if (flags & TYPE_TAGGED_UPD) {
        type = "UPD";
    }

    fprintf(f, "Processing record for %s\n", filename);
    fprintf(f, "Commit-lsn: %u:%u\n", commit_lsn->file, commit_lsn->offset);
    fprintf(f, "Consumer: %d\n", consumer);
    fprintf(f, "Genid: %lu\n", genid);
    fprintf(f, "Seq: %lld\n", seq);
    fprintf(f, "Datalen): %ld\n", fnddtalen);
    fprintf(f, "Qf-genid: %llu\n", fnd->genid);
    fprintf(f, "Qf-datalen: %u\n", fnd->data_len);
    fprintf(f, "Qf-dataoff: %u\n", fnd->data_offset);
    fprintf(f, "Qf-epoch: %u\n", fnd->epoch);
    fprintf(f, "Table: %s\n", tbl);
    fprintf(f, "Type: %s\n", type);
    fprintf(f, "\n");

    while (payload && payload < end) {
        payload = print_field(payload, f);
    }
    if (payload == NULL) {
        logmsg(LOGMSG_USER, "print-field failed\n");
    }
    fflush(f);
}

void register_dump_qtrigger(const char *filename, bdb_state_type *(*gethndl)(const char *q), const char *outfile,
                            int maxsz)
{
    FILE *f = fopen(outfile, "w");
    if (!f) {
        logmsg(LOGMSG_ERROR, "Failed to open %s for writing\n", outfile);
        return;
    }
    setvbuf(f, NULL, _IOLBF, 0);
    logmsg(LOGMSG_USER, "Registered queue-dump for %s -> %s\n", filename, outfile);
    register_logqueue_trigger(filename, gethndl, dump_qtrigger, f, maxsz, LOGQTRIGGER_PUSH);
}

#ifdef WITH_QKAFKA

extern char *gbl_kafka_brokers;

/* Outgoing kafka buffer */
struct kafka_log_queue {
    long long seq;
    uint64_t genid;
    uint8_t key[12];
    size_t dtalen;
    uint8_t data[1];
};

/* Kafka state */
struct kafka_state {
    char *topic;
    rd_kafka_topic_t *rkt_p;
    rd_kafka_t *rk_p;
    uint64_t bytes_written;
};

static void write_kafka(bdb_state_type *bdb_state, const DB_LSN *commit_lsn, const char *filename, const DBT *key,
                        const DBT *data, void *userptr)
{
    struct kafka_state *k = userptr;
    size_t fnddtalen = 0, fnddtaoff = 0;
    int consumer;
    long long seq = 0;
    uint64_t genid = 0;
    struct bdb_queue_found *fnd = {0};

    int rc = bdb_queuedb_trigger_unpack(bdb_state, key, data, &consumer, &genid, &fnd, &fnddtalen, &fnddtaoff, &seq);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s failed to unpack record for %s rc=%d\n", __func__, filename, rc);
        return;
    }

    assert(key->size == 12);

    struct kafka_log_queue *kq =
        (struct kafka_log_queue *)malloc(offsetof(struct kafka_log_queue, data) + fnd->data_len);
    kq->seq = seq;
    kq->genid = genid;
    memcpy(kq->key, key->data, 12);
    kq->dtalen = fnd->data_len;
    uint8_t *payload = ((uint8_t *)fnd) + fnd->data_offset;
    memcpy(kq->data, payload, fnd->data_len);

    rc = rd_kafka_produce(k->rkt_p, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY, (void *)kq,
                          offsetof(struct kafka_log_queue, data) + fnd->data_len, NULL, 0, NULL);

    free(kq);
    if (rc == -1) {
        logmsg(LOGMSG_ERROR, "%s Failed to produce to topic %s: %s\n", __func__, rd_kafka_topic_name(k->rkt_p),
               rd_kafka_err2str(rd_kafka_last_error()));
    }
}

int register_queue_kafka(const char *filename, const char *kafka_topic, bdb_state_type *(*gethndl)(const char *q),
                         int maxsz)
{
    rd_kafka_conf_t *conf;
    rd_kafka_t *rk_p;
    rd_kafka_topic_t *rkt_p;
    char errstr[512];

    if (!kafka_topic || !gbl_kafka_brokers) {
        logmsg(LOGMSG_ERROR, "%s kafka topic or broker not set\n", __func__);
        return -1;
    }
    conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", gbl_kafka_brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        logmsg(LOGMSG_ERROR, "%s rd_kafka_conf_set error: %s\n", __func__, errstr);
        return -1;
    }

    rk_p = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk_p) {
        logmsg(LOGMSG_ERROR, "%s rd_kafka_new error: %s\n", __func__, errstr);
        return -1;
    }

    rkt_p = rd_kafka_topic_new(rk_p, kafka_topic, NULL);
    if (!rkt_p) {
        logmsg(LOGMSG_ERROR, "%s rd_kafka_topic_new error: %s\n", __func__, rd_kafka_err2str(rd_kafka_last_error()));
        return -1;
    }

    struct kafka_state *kstate = calloc(sizeof(struct kafka_state), 1);
    kstate->topic = strdup(kafka_topic);
    kstate->rkt_p = rkt_p;
    kstate->rk_p = rk_p;
    register_logqueue_trigger(filename, gethndl, write_kafka, kstate, maxsz,
                              LOGQTRIGGER_PUSH | LOGQTRIGGER_MASTER_ONLY);
    logmsg(LOGMSG_USER, "Registered kafka-qwrite for %s -> %s\n", filename, kafka_topic);

    return 0;
}

#endif
