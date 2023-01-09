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
#include <pthread.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include "comdb2systblInt.h"
#include "ezsystables.h"
#include "sql.h"
#include "plhash.h"
#include "tohex.h"

extern hash_t *gbl_fingerprint_hash;
extern pthread_mutex_t gbl_fingerprint_hash_mu;

sqlite3_module systblQueryPlansModule = {
    .access_flag = CDB2_ALLOW_USER,
};

typedef struct systable_query_plans {
    char *fingerprint;
    char *zNormSql;
    char *plan;
    double total_cost_per_row;
    int nexecutions;
    double avg_cost_per_row;
} systable_query_plans_t;

int query_plans_systable_collect(void **data, int *nrecords)
{
    *nrecords = 0;
    *data = NULL;
    void *ent, *ent2;
    unsigned int bkt, bkt2;
    struct query_plan_item *q;
    struct fingerprint_track *f;
    int current_plans_count;
    char fp[FINGERPRINTSZ*2+1];

    Pthread_mutex_lock(&gbl_fingerprint_hash_mu);
    if (!gbl_fingerprint_hash) {
        Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
        return 0;
    }

    // first find number of records
    for (f = (struct fingerprint_track *)hash_first(gbl_fingerprint_hash, &ent, &bkt); f;
         f = (struct fingerprint_track *)hash_next(gbl_fingerprint_hash, &ent, &bkt)) {
        if (!f->query_plan_hash) {
            continue;
        }
        hash_info(f->query_plan_hash, NULL, NULL, NULL, NULL, &current_plans_count, NULL, NULL);
        *nrecords += current_plans_count;
    }

    int idx = 0;
    systable_query_plans_t *arr = calloc(*nrecords, sizeof(systable_query_plans_t));
    for (f = (struct fingerprint_track *)hash_first(gbl_fingerprint_hash, &ent, &bkt); f;
         f = (struct fingerprint_track *)hash_next(gbl_fingerprint_hash, &ent, &bkt)) {
        if (!f->query_plan_hash) {
            continue;
        }

        util_tohex(fp, (char *)f->fingerprint, FINGERPRINTSZ);
        for (q = (struct query_plan_item *)hash_first(f->query_plan_hash, &ent2, &bkt2); q;
             q = (struct query_plan_item *)hash_next(f->query_plan_hash, &ent2, &bkt2)) {
                arr[idx].fingerprint = strdup(fp);
                if (f->zNormSql) {
                    arr[idx].zNormSql = strdup(f->zNormSql);
                }
                arr[idx].plan = strdup(q->plan);
                arr[idx].total_cost_per_row = q->total_cost_per_row;
                arr[idx].nexecutions = q->nexecutions;
                arr[idx].avg_cost_per_row = q->avg_cost_per_row;
                idx++;
        }
    }

    *data = arr;
    Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
    return 0;
}

void query_plans_systable_free(void *arr, int nrecords)
{
    systable_query_plans_t *parr = (systable_query_plans_t *)arr;
    int i;

    for (i = 0; i < nrecords; i++) {
        free(parr[i].fingerprint);
        free(parr[i].zNormSql);
        free(parr[i].plan);
    }
    free(arr);
}

int systblQueryPlansInit(sqlite3*db)
{
    return create_system_table(
        db, "comdb2_query_plans", &systblQueryPlansModule,
        query_plans_systable_collect, query_plans_systable_free,
        sizeof(systable_query_plans_t),
        CDB2_CSTRING, "fingerprint", -1, offsetof(systable_query_plans_t, fingerprint),
        CDB2_CSTRING, "normalized_sql", -1, offsetof(systable_query_plans_t, zNormSql),
        CDB2_CSTRING, "plan", -1, offsetof(systable_query_plans_t, plan),
        CDB2_REAL, "total_cost_per_row", -1, offsetof(systable_query_plans_t, total_cost_per_row),
        CDB2_INTEGER, "num_executions", -1, offsetof(systable_query_plans_t, nexecutions),
        CDB2_REAL, "avg_cost_per_row", -1, offsetof(systable_query_plans_t, avg_cost_per_row),
        SYSTABLE_END_OF_FIELDS);
}
