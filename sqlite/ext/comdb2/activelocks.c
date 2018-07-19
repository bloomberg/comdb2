#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include "comdb2.h"
#include "bdb_int.h"
#include "comdb2systblInt.h"
#include "ezsystables.h"
#include "cdb2api.h"

typedef struct systable_activelocks {
    int64_t                 threadid;
    uint32_t                lockerid;
    const char              *mode;
    const char              *status;
    char                    table_str[64];
    char                    *table;
    char                    type_str[80];
    char                    *type;
    int64_t                 page;
} systable_activelocks_t;

typedef struct getactivelocks {
    int count;
    int alloc;
    systable_activelocks_t *records;
} getactivelocks_t;

static int collect(void *args, int64_t threadid, int32_t lockerid,
        const char *mode, const char *status, const char *table,
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
    l->page = page;
    if (table)
        strncpy(l->table_str, table, sizeof(l->table_str));
    else
        l->table_str[0] = '\0';
    l->table = l->table_str;

    if (rectype)
        strncpy(l->type_str, rectype, sizeof(l->type_str));
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
            CDB2_INTEGER, "thread", offsetof(systable_activelocks_t, threadid),
            CDB2_INTEGER, "lockerid", offsetof(systable_activelocks_t, lockerid),
            CDB2_CSTRING, "mode", offsetof(systable_activelocks_t, mode),
            CDB2_CSTRING, "status", offsetof(systable_activelocks_t, status),
            CDB2_CSTRING, "table", offsetof(systable_activelocks_t, table),
            CDB2_CSTRING, "locktype", offsetof(systable_activelocks_t, type),
            CDB2_INTEGER, "page", offsetof(systable_activelocks_t, page),
            SYSTABLE_END_OF_FIELDS);
}
