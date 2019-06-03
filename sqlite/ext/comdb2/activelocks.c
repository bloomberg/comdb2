#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include "comdb2.h"
#include "bdb_int.h"
#include "comdb2systblInt.h"
#include "ezsystables.h"
#include "cdb2api.h"
#include "str0.h"

typedef struct systable_activelocks {
    int64_t                 threadid;
    uint32_t                lockerid;
    const char              *mode;
    const char              *status;
    char                    object_str[64];
    char                    *object;
    char                    type_str[80];
    char                    *type;
    int64_t                 page;
    int                     page_isnull;
} systable_activelocks_t;

typedef struct getactivelocks {
    int count;
    int alloc;
    systable_activelocks_t *records;
} getactivelocks_t;

static int collect(void *args, int64_t threadid, int32_t lockerid,
        const char *mode, const char *status, const char *object,
        int64_t page, const char *rectype)
{
    getactivelocks_t *a = (getactivelocks_t *)args;
    systable_activelocks_t *l;
    a->count++;
    if (a->count >= a->alloc) {
        if (a->alloc == 0) a->alloc = 16;
        else a->alloc = a->alloc * 2;
        a->records = realloc(a->records, a->alloc * sizeof(systable_activelocks_t));
    }
    l = &a->records[a->count - 1];
    l->threadid = threadid;
    l->lockerid = lockerid;
    l->mode = mode;
    l->status = status;
    if (page < 0) {
        l->page_isnull = 1;
    } else {
        l->page = page;
        l->page_isnull = 0;
    }
    if (object)
        strncpy0(l->object_str, object, sizeof(l->object_str));
    else
        l->object_str[0] = '\0';
    l->object = l->object_str;

    if (rectype)
        strncpy0(l->type_str, rectype, sizeof(l->type_str));
    else 
        l->type_str[0] = '\0';
    l->type = l->type_str;

    return 0;
}

static int get_activelocks(void **data, int *records)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    getactivelocks_t a = {0};
    bdb_state->dbenv->collect_locks(bdb_state->dbenv, collect, &a);
    *data = a.records;
    *records = a.count;
    return 0;
}

static void free_activelocks(void *p, int n)
{
    free(p);
}

int systblActivelocksInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_locks", get_activelocks,
            free_activelocks, sizeof(systable_activelocks_t),
            CDB2_INTEGER, "thread", -1, offsetof(systable_activelocks_t, threadid),
            CDB2_INTEGER, "lockerid", -1, offsetof(systable_activelocks_t, lockerid),
            CDB2_CSTRING, "mode", -1, offsetof(systable_activelocks_t, mode),
            CDB2_CSTRING, "status", -1, offsetof(systable_activelocks_t, status),
            CDB2_CSTRING, "object", -1, offsetof(systable_activelocks_t, object),
            CDB2_CSTRING, "locktype", -1, offsetof(systable_activelocks_t, type),
            CDB2_INTEGER, "page", offsetof(systable_activelocks_t, page_isnull), offsetof(systable_activelocks_t, page),
            SYSTABLE_END_OF_FIELDS);
}
