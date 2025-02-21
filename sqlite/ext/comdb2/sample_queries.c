/*
   Copyright 2024 Bloomberg Finance L.P.

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
#include "string_ref.h"
#include <plhash_glue.h>
#include "tohex.h"

extern hash_t *gbl_sample_queries_hash;
extern pthread_mutex_t gbl_fingerprint_hash_mu;

sqlite3_module systblSampleQueriesModule = {
    .access_flag = CDB2_ALLOW_USER,
};

typedef struct systable_sample_queries {
    char *fingerprint;
    char *plan_fingerprint;
    char *query;
    char *query_plan;
    char *params;
    cdb2_client_datetime_t timestamp;
} systable_sample_queries_t;

int sample_queries_systable_collect(void **data, int *nrecords)
{
    *nrecords = 0;
    *data = NULL;
    void *ent;
    unsigned int bkt;
    struct query_field *q;
    char fp[FINGERPRINTSZ*2+1];

    Pthread_mutex_lock(&gbl_fingerprint_hash_mu);
    if (!gbl_sample_queries_hash) {
        Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
        return 0;
    }

    *nrecords = hash_get_num_entries(gbl_sample_queries_hash);

    int idx = 0;
    systable_sample_queries_t *arr = calloc(*nrecords, sizeof(systable_sample_queries_t));
    for (q = (struct query_field *)hash_first(gbl_sample_queries_hash, &ent, &bkt); q;
         q = (struct query_field *)hash_next(gbl_sample_queries_hash, &ent, &bkt)) {
        util_tohex(fp, (char *)q->fingerprint, FINGERPRINTSZ);
        arr[idx].fingerprint = strdup(fp);
        util_tohex(fp, (char *)q->plan_fingerprint, FINGERPRINTSZ);
        arr[idx].plan_fingerprint = strdup(fp);
        arr[idx].query = strdup(string_ref_cstr(q->zSql_ref));
        if (q->query_plan_ref)
            arr[idx].query_plan = strdup(string_ref_cstr(q->query_plan_ref));
        if (q->params)
            arr[idx].params = strdup(q->params);
        gmtime_r(&q->timestamp, (struct tm*)&arr[idx].timestamp.tm);
        strcpy(arr[idx].timestamp.tzname, "UTC");
        idx++;
    }

    *data = arr;
    Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
    return 0;
}

void sample_queries_systable_free(void *arr, int nrecords)
{
    systable_sample_queries_t *parr = (systable_sample_queries_t *)arr;
    int i;

    for (i = 0; i < nrecords; i++) {
        free(parr[i].fingerprint);
        free(parr[i].plan_fingerprint);
        free(parr[i].query);
        free(parr[i].query_plan);
        free(parr[i].params);
    }
    free(arr);
}

int systblSampleQueriesInit(sqlite3*db)
{
    return create_system_table(
        db, "comdb2_sample_queries", &systblSampleQueriesModule,
        sample_queries_systable_collect, sample_queries_systable_free,
        sizeof(systable_sample_queries_t),
        CDB2_CSTRING, "fingerprint", -1, offsetof(systable_sample_queries_t, fingerprint),
        CDB2_CSTRING, "plan_fingerprint", -1, offsetof(systable_sample_queries_t, plan_fingerprint),
        CDB2_CSTRING, "query", -1, offsetof(systable_sample_queries_t, query),
        CDB2_CSTRING | SYSTABLE_FIELD_NULLABLE, "query_plan", -1, offsetof(systable_sample_queries_t, query_plan),
        CDB2_CSTRING | SYSTABLE_FIELD_NULLABLE, "params", -1, offsetof(systable_sample_queries_t, params),
        CDB2_DATETIME, "timestamp", -1, offsetof(systable_sample_queries_t, timestamp),
        SYSTABLE_END_OF_FIELDS);
}
