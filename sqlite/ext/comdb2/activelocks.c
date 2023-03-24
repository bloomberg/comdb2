/*
   Copyright 2020 Bloomberg Finance L.P.

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


#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include "comdb2.h"
#include "bdb_int.h"
#include "comdb2systblInt.h"
#include "ezsystables.h"
#include "cdb2api.h"
#include "str0.h"
#include <stackutil.h>

typedef struct systable_activelocks {
    int64_t                 threadid;
    uint32_t                lockerid;
    const char              *mode;
    const char              *status;
    char                    *object;
    char                    *type;
    int64_t                 page;
    int                     page_isnull;
    int                     frames;
    char                    *stack;
} systable_activelocks_t;

typedef struct getactivelocks {
    int count;
    int alloc;
    systable_activelocks_t *records;
} getactivelocks_t;

static int collect(void *args, int64_t threadid, int32_t lockerid,
        const char *mode, const char *status, const char *object,
        int64_t page, const char *rectype, int stackid)
{
    int64_t hits;
    int nframes;
    char *type;
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
    l->object = strdup(object ? object : "");
    l->type = strdup(rectype ? rectype : "");

    if ((l->stack = stackutil_get_stack_str(stackid, &type, &nframes, &hits)) == NULL) {
        l->stack = strdup("(no-stack)");
        free(type);
    }
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
    systable_activelocks_t *a, *begin = p;
    systable_activelocks_t *end = begin + n;
    for (a = begin; a < end; ++a) {
        free(a->object);
        free(a->type);
        free(a->stack);
    }
    free(p);
}

sqlite3_module systblActiveLocksModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int systblActivelocksInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_locks", &systblActiveLocksModule,
            get_activelocks, free_activelocks, sizeof(systable_activelocks_t),
            CDB2_INTEGER, "thread", -1, offsetof(systable_activelocks_t, threadid),
            CDB2_INTEGER, "lockerid", -1, offsetof(systable_activelocks_t, lockerid),
            CDB2_CSTRING, "mode", -1, offsetof(systable_activelocks_t, mode),
            CDB2_CSTRING, "status", -1, offsetof(systable_activelocks_t, status),
            CDB2_CSTRING, "object", -1, offsetof(systable_activelocks_t, object),
            CDB2_CSTRING, "locktype", -1, offsetof(systable_activelocks_t, type),
            CDB2_INTEGER, "page", offsetof(systable_activelocks_t, page_isnull), offsetof(systable_activelocks_t, page),
            CDB2_CSTRING, "stack", -1, offsetof(systable_activelocks_t, stack),
            SYSTABLE_END_OF_FIELDS);
}
