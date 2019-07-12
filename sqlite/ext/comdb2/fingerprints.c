/*
   Copyright 2018 Bloomberg Finance L.P.

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

#define SQLITE_CORE 1

#include <pthread.h>
#include <comdb2systblInt.h>
#include <ezsystables.h>
#include "sql.h"
#include "plhash.h"

struct fingerprint_track_systbl {
    systable_blobtype fp_blob;
    int64_t count;    /* Cumulative number of times executed */
    int64_t cost;     /* Cumulative cost */
    int64_t time;     /* Cumulative preparation and execution time */
    int64_t prepTime; /* Cumulative preparation time only */
    int64_t rows;     /* Cumulative number of rows selected */
    char *zNormSql;   /* The normalized SQL query */
    size_t nNormSql;  /* Length of normalized SQL query */
};

extern hash_t *gbl_fingerprint_hash;
extern pthread_mutex_t gbl_fingerprint_hash_mu;

static void release_callback(void *data, int npoints)
{
    struct fingerprint_track_systbl *pFp = (struct fingerprint_track_systbl *)data;
    if (pFp != NULL) {
        for (int index = 0; index < npoints; index++) {
            free(pFp[index].fp_blob.value);
            free(pFp[index].zNormSql);
        }
        free(pFp);
    }
}

static int fingerprints_callback(void **data, int *npoints)
{
    int rc = SQLITE_OK;
    *npoints = 0;
    *data = NULL;
    Pthread_mutex_lock(&gbl_fingerprint_hash_mu);
    if (gbl_fingerprint_hash != NULL) {
        int count;
        hash_info(gbl_fingerprint_hash, NULL, NULL, NULL, NULL, &count, NULL, NULL);
        if (count > 0) {
            struct fingerprint_track_systbl *pFp = calloc(count,
                                               sizeof(struct fingerprint_track_systbl));
            if (pFp != NULL) {
                struct fingerprint_track *pEntry;
                int copied = 0;
                void *hash_cur;
                unsigned int hash_cur_buk;
                pEntry = hash_first(gbl_fingerprint_hash, &hash_cur, &hash_cur_buk);
                while (pEntry != NULL) {
                    assert( copied<count );
                    pFp[copied].fp_blob.value = calloc(FINGERPRINTSZ, sizeof(char));
                    if (pFp[copied].fp_blob.value != NULL) {
                        pFp[copied].fp_blob.size = FINGERPRINTSZ;
                        memcpy(pFp[copied].fp_blob.value, pEntry->fingerprint,
                               pFp[copied].fp_blob.size);
                    } else {
                        rc = SQLITE_NOMEM;
                        break;
                    }
                    pFp[copied].count = pEntry->count;
                    pFp[copied].cost = pEntry->cost;
                    pFp[copied].time = pEntry->time;
                    pFp[copied].prepTime = pEntry->prepTime;
                    pFp[copied].rows = pEntry->rows;
                    if (pEntry->zNormSql != NULL) {
                        pFp[copied].zNormSql = strdup(pEntry->zNormSql);
                        pFp[copied].nNormSql = strlen(pEntry->zNormSql);
                        assert( pFp[copied].nNormSql==pEntry->nNormSql );
                    }
                    copied++;
                    pEntry = hash_next(gbl_fingerprint_hash, &hash_cur, &hash_cur_buk);
                }
                if (rc == SQLITE_OK) {
                    *data = pFp;
                    *npoints = count;
                } else {
                    release_callback(pFp, count);
                }
            } else {
                rc = SQLITE_NOMEM;
            }
        }
    }
    Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
    return rc;
}

sqlite3_module systblFingerprintsModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int systblFingerprintsInit(sqlite3 *db)
{
    return create_system_table(db,
        "comdb2_fingerprints",
        &systblFingerprintsModule,
        fingerprints_callback, release_callback,
        sizeof(struct fingerprint_track),
        CDB2_BLOB, "fingerprint", -1,
        offsetof(struct fingerprint_track, fingerprint),
        CDB2_INTEGER, "count", -1,
        offsetof(struct fingerprint_track, count),
        CDB2_INTEGER, "total_cost", -1,
        offsetof(struct fingerprint_track, cost),
        CDB2_INTEGER, "total_time", -1,
        offsetof(struct fingerprint_track, time),
        CDB2_INTEGER, "total_prep_time", -1,
        offsetof(struct fingerprint_track, prepTime),
        CDB2_INTEGER, "total_rows", -1,
        offsetof(struct fingerprint_track, rows),
        CDB2_CSTRING, "normalized_sql", -1,
        offsetof(struct fingerprint_track, zNormSql),
        SYSTABLE_END_OF_FIELDS);
}
