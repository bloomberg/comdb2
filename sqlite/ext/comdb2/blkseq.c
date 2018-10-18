#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include "comdb2.h"
#include "comdb2systblInt.h"
#include "ezsystables.h"
#include "cdb2api.h"

#include "comdb2uuid.h"

#include "sql.h"
#include "build/db.h"
#include <bdb/bdb_api.h>

typedef struct systable_blkseq {
    int64_t stripe;
    int64_t ix;
    char *lsn;
    char *id;
    int64_t size;
    int64_t rcode;
    int64_t time;
    int64_t age;
} systable_blkseq_t;

typedef struct getblkseq {
    int count;
    int alloc;
    systable_blkseq_t *records;
} getblkseq_t;

static void collect_blkseq(int stripe, int ix, void *plsn, void *pkey,
                           void *pdata, void *arg)
{
    DB_LSN *lsn = plsn;
    DBT *key = pkey;
    DBT *data = pdata;
    int now = comdb2_time_epoch();

    getblkseq_t *blkseq = arg;
    blkseq->count++;
    if (blkseq->count >= blkseq->alloc) {
        if (blkseq->alloc == 0)
            blkseq->alloc = 128;
        else
            blkseq->alloc = blkseq->alloc * 2;
        blkseq->records = realloc(blkseq->records,
                                  blkseq->alloc * sizeof(systable_blkseq_t));
    }

    systable_blkseq_t *b = &blkseq->records[blkseq->count - 1];
    memset(b, 0, sizeof(*b));

    b->stripe = stripe;
    b->ix = ix;
    b->lsn = malloc(30);
    snprintf(b->lsn, 30, "[%u][%u]", lsn->file, lsn->offset);

    int *k;
    k = (int *)key->data;
    if (data->size < sizeof(int)) {
        logmsg(LOGMSG_ERROR, "%x %x %x invalid sz %d\n", k[0], k[1], k[2],
               data->size);
    } else {
        int timestamp;
        int age;
        int blkseq_get_rcode(void *data, int datalen);
        int rcode = blkseq_get_rcode(data->data, data->size);
        memcpy(&timestamp, (uint8_t *)data->data + (data->size - 4), 4);
        age = now - timestamp;

        b->size = data->size;
        b->rcode = rcode;
        b->time = timestamp;
        b->age = age;

        // this is a cnonce
        if (key->size > 12) {
            char *p = malloc(key->size + 1);
            memcpy(p, key->data, key->size);
            p[key->size] = '\0';
            b->id = p;
        } else {
            b->id = malloc(30);
            snprintf(b->id, 30, "%x-%x-%x", k[0], k[1], k[2]);
        }
    }
}

static int get_blkseq(void **data, int *records)
{
    getblkseq_t blkseq = {0};
    bdb_blkseq_for_each(thedb->bdb_env, &blkseq, collect_blkseq);
    *data = blkseq.records;
    *records = blkseq.count;
    return 0;
}

static void free_blkseq(void *p, int n)
{
    systable_blkseq_t  *t = (systable_blkseq_t *)p;
    for (int i = 0; i < n; i++) {
        if (t[i].lsn)
            free(t[i].lsn);
        if (t[i].id)
            free(t[i].id);
    }
    free(p);
}

int systblBlkseqInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_blkseq", get_blkseq, free_blkseq, sizeof(systable_blkseq_t),
        CDB2_INTEGER, "stripe", -1, offsetof(systable_blkseq_t, stripe),
        CDB2_INTEGER, "index", -1, offsetof(systable_blkseq_t, ix),
        CDB2_CSTRING, "last_lsn", -1, offsetof(systable_blkseq_t, lsn),
        CDB2_CSTRING, "id", -1, offsetof(systable_blkseq_t, id),
        CDB2_INTEGER, "size", -1, offsetof(systable_blkseq_t, size),
        CDB2_INTEGER, "rcode", -1, offsetof(systable_blkseq_t, rcode),
        CDB2_INTEGER, "time", -1, offsetof(systable_blkseq_t, time),
        CDB2_INTEGER, "age", -1, offsetof(systable_blkseq_t, age),
        SYSTABLE_END_OF_FIELDS);
}
