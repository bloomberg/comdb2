/*
   Copyright 2023 Bloomberg Finance L.P.

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
#include <build/db.h>
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include "types.h"

static sqlite3_module systblPreparedModule = {
    .access_flag = CDB2_ALLOW_USER,
};

typedef struct systable_prepared {
    char *dist_txnid;
    char *flags;
    char *lsn;
    char *begin_lsn;
    uint32_t coordinator_gen;
    char *coordinator_name;
    char *coordinator_tier;
    uint64_t utxnid;
} systable_prepared_t;

typedef struct getprepared {
    int count;
    int alloc;
    systable_prepared_t *records;
} getprepared_t;

static inline void prepared_lsn_to_str(char *st, DB_LSN *lsn)
{
    sprintf(st, "{%d:%d}", lsn->file, lsn->offset);
}

static inline char *dist_flags_to_str(uint32_t flags)
{
    char *r = (char *)calloc(128, 1);
    int first = 0;
    if (flags & DB_DIST_INFLIGHT) {
        if (first) strcat(r, "|");
        strcat(r, "INFLIGHT");
        first = 1;
    }
    if (flags & DB_DIST_RECOVERED) {
        if (first) strcat(r, "|");
        strcat(r, "RECOVERED");
        first = 1;
    }
    if (flags & DB_DIST_HAVELOCKS) {
        if (first) strcat(r, "|");
        strcat(r, "HAVELOCKS");
        first = 1;
    }
    if (flags & DB_DIST_ABORTED) {
        if (first) strcat(r, "|");
        strcat(r, "ABORTED");
        first = 1;
    }
    if (flags & DB_DIST_COMMITTED) {
        if (first) strcat(r, "|");
        strcat(r, "COMMITTED");
        first = 1;
    }
    if (flags & DB_DIST_SCHEMA_LK) {
        if (first) strcat(r, "|");
        strcat(r, "SCHEMA_LK");
        first = 1;
    }
    if (flags & DB_DIST_UPDSHADOWS) {
        if (first) strcat(r, "|");
        strcat(r, "UPDSHADOWS");
        first = 1;
    }
    return r;
}

static int collect_prepared(void *args, char *dist_txnid, uint32_t flags, DB_LSN *lsn, 
    DB_LSN *begin_lsn, uint32_t coordinator_gen, char *coordinator_name, char *coordinator_tier,
    uint64_t utxnid)
{
    getprepared_t *p = (getprepared_t *)args;
    systable_prepared_t *r;
    p->count++;
    if (p->count >= p->alloc) {
        if (p->alloc == 0) p->alloc = 16;
        else p->alloc = p->alloc * 2;
        p->records = realloc(p->records, p->alloc * sizeof(systable_prepared_t));
    }
    r = &p->records[p->count - 1];
    r->dist_txnid = strdup(dist_txnid);
    r->flags = dist_flags_to_str(flags);
    r->lsn = (char *)calloc(32, 1);
    prepared_lsn_to_str(r->lsn, lsn);
    r->begin_lsn = (char *)calloc(32, 1);
    prepared_lsn_to_str(r->begin_lsn, begin_lsn);
    r->coordinator_gen = coordinator_gen;
    r->coordinator_name = strdup(coordinator_name);
    r->coordinator_tier = strdup(coordinator_tier);
    r->utxnid = utxnid;
    return 0;
}

static int get_prepared(void **data, int *records)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    getprepared_t p = {0};
    bdb_state->dbenv->collect_prepared(bdb_state->dbenv, collect_prepared, &p);
    *data = p.records;
    *records = p.count;
    return 0;
}

static void free_prepared(void *p, int n)
{
    systable_prepared_t *a, *begin = p;
    systable_prepared_t *end = begin + n;
    for (a = begin; a < end; ++a) {
        free(a->flags);
        free(a->lsn);
        free(a->begin_lsn);
        free(a->coordinator_name);
        free(a->coordinator_tier);
    }
    free(p);
}

int systblPreparedInit(sqlite3 *db)
{
    return create_system_table(db, "comdb2_prepared", &systblPreparedModule,
        get_prepared, free_prepared, sizeof(systable_prepared_t),
        CDB2_CSTRING, "dist_txnid", -1, offsetof(systable_prepared_t, dist_txnid),
        CDB2_INTEGER, "utxnid", -1, offsetof(systable_prepared_t, utxnid),
        CDB2_CSTRING, "flags", -1, offsetof(systable_prepared_t, flags),
        CDB2_CSTRING, "prepare_lsn", -1, offsetof(systable_prepared_t, lsn),
        CDB2_CSTRING, "begin_lsn", -1, offsetof(systable_prepared_t, begin_lsn),
        CDB2_CSTRING, "coordinator_name", -1, offsetof(systable_prepared_t, coordinator_name),
        CDB2_CSTRING, "coordinator_tier", -1, offsetof(systable_prepared_t, coordinator_tier),
        CDB2_INTEGER, "coordinator_generation", -1, offsetof(systable_prepared_t, coordinator_gen),
        SYSTABLE_END_OF_FIELDS);
}
