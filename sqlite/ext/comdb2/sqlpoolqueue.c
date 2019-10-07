#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include "comdb2.h"
#include "bdb_int.h"
#include "comdb2systblInt.h"
#include "ezsystables.h"
#include "thdpool.h"
#include "cdb2api.h"

typedef struct systable_sqlpoolqueue {
    int64_t                 time_in_queue_ms;
    char                    *info;
    int                     info_is_null;
    priority_t              priority;
} systable_sqlpoolqueue_t;

typedef struct getsqlpoolqueue {
    int count;
    int alloc;
    systable_sqlpoolqueue_t *records;
} getsqlpoolqueue_t;

static void collect(struct thdpool *pool, struct workitem *item, void *user)
{
    getsqlpoolqueue_t *q = (getsqlpoolqueue_t *)user;
    systable_sqlpoolqueue_t *i;
    q->count++;
    if (q->count >= q->alloc) {
        if (q->alloc == 0) q->alloc = 16;
        else q->alloc = q->alloc * 2;
        q->records = realloc(q->records, q->alloc * sizeof(systable_sqlpoolqueue_t));
    }

    i = &q->records[q->count - 1];
    i->time_in_queue_ms = comdb2_time_epochms() - item->queue_time_ms;
    if (item->persistent_info) {
        i->info = strdup(item->persistent_info);
        i->info_is_null = 0;
    } else {
        i->info = NULL;
        i->info_is_null = 1;
    }
    i->priority = item->priority;
}

extern struct thdpool *gbl_sqlengine_thdpool;

static int get_sqlpoolqueue(void **data, int *records)
{
    getsqlpoolqueue_t q = {0};
    thdpool_foreach(gbl_sqlengine_thdpool, collect, &q);
    *data = q.records;
    *records = q.count;
    return 0;
}

static void free_sqlpoolqueue(void *p, int n)
{
    systable_sqlpoolqueue_t *t = (systable_sqlpoolqueue_t *)p;
    for (int i=0;i<n;i++) {
        if (t[i].info)
            free(t[i].info);
    }
    free(p);
}

sqlite3_module systblSqlpoolQueueModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int systblSqlpoolQueueInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_sqlpool_queue",
        &systblSqlpoolQueueModule, get_sqlpoolqueue, free_sqlpoolqueue,
        sizeof(systable_sqlpoolqueue_t),
        CDB2_INTEGER, "time_in_queue_ms", -1, offsetof(systable_sqlpoolqueue_t,
                                                       time_in_queue_ms),
        CDB2_CSTRING, "sql", offsetof(systable_sqlpoolqueue_t, info_is_null),
        offsetof(systable_sqlpoolqueue_t, info),
        CDB2_INTEGER, "priority", -1, offsetof(systable_sqlpoolqueue_t,
                                               priority),
        SYSTABLE_END_OF_FIELDS);
}

