/*
   Copyright 2018-2020 Bloomberg Finance L.P.

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
#include <plhash_glue.h>
#include "tohex.h"
#include <reqlog.h>
#include <bdb_api.h>

struct fingerprint_track_systbl {
    char *fingerprint;
    int64_t count;    /* Cumulative number of times executed */
    int64_t cost;     /* Cumulative cost */
    int64_t time;     /* Cumulative preparation and execution time */
    int64_t prepTime; /* Cumulative preparation time only */
    int64_t rows;     /* Cumulative number of rows selected */
    char *zNormSql;   /* The normalized SQL query */
    size_t nNormSql;  /* Length of normalized SQL query */
    char *excluded;    /* 'Y' if excluded from longreqs */
    int64_t total_sql_pagein_read;    /* Cumulative read (SQL) bufferpool page-ins (hit+miss) */
    int64_t total_sql_pagein_read_io; /* Subset of the above that required disk I/O */
    int64_t total_write_pagein_read;    /* Cumulative write-apply page-ins on the master */
    int64_t total_write_pagein_read_io; /* Subset of the above that required disk I/O */
    int64_t total_replication_pagein_read;    /* Page-ins from applying this statement's
                                                 writes off the log, on a replicant */
    int64_t total_replication_pagein_read_io; /* Subset of the above that required disk I/O */
    char *has_query_info; /* 'Y' if this node has the full query text (a
                             gbl_fingerprint_hash entry); 'N' for rtstats-only
                             fingerprints (e.g. master write-apply accounting) */

    char fp[FINGERPRINTSZ*2+1];
};

extern hash_t *gbl_fingerprint_hash;
extern pthread_mutex_t gbl_fingerprint_hash_mu;

static void release_callback(void *data, int npoints)
{
    struct fingerprint_track_systbl *pFp = (struct fingerprint_track_systbl *)data;
    if (pFp != NULL) {
        for (int index = 0; index < npoints; index++)
            free(pFp[index].zNormSql);
        free(pFp);
    }
}

/* Upper bound on the rtstats-only rows we may append. */
static void rtstats_count_cb(const unsigned char *fingerprint, const uint64_t *counts,
                             int has_main_entry, void *arg)
{
    (void)fingerprint; (void)counts; (void)has_main_entry;
    (*(int *)arg)++;
}

struct rtstats_merge_ctx {
    hash_t *seen;                          /* fingerprints already emitted from gbl_fingerprint_hash */
    struct fingerprint_track_systbl *pFp;  /* output row array */
    int capacity;                          /* allocated rows */
    int copied;                            /* next free row index */
};

/* Appends a row per fingerprint known only via rtstats -- write-apply on the
 * master, replication-apply on a replicant -- so has_query_info='N'.
 *
 * Gated on the write/apply counters because any in-flight query is transiently
 * rtstats-only too (the read side arms at prepare, gbl_fingerprint_hash fills
 * at done), and those would otherwise show up here with no query text. */
static void rtstats_merge_cb(const unsigned char *fingerprint, const uint64_t *counts,
                             int has_main_entry, void *arg)
{
    struct rtstats_merge_ctx *ctx = (struct rtstats_merge_ctx *)arg;
    struct fingerprint_track_systbl *row;
    (void)has_main_entry;

    if (counts[2] == 0 && counts[3] == 0 && counts[4] == 0 && counts[5] == 0)
        return; /* no apply-side activity -- read-side-only/in-flight entry */
    if (ctx->seen != NULL && hash_find(ctx->seen, fingerprint) != NULL)
        return; /* already emitted above with full query info */
    if (ctx->copied >= ctx->capacity)
        return; /* raced with new inserts; snapshot is best-effort */

    row = &ctx->pFp[ctx->copied];
    util_tohex(row->fp, (char *)fingerprint, FINGERPRINTSZ);
    row->fingerprint = row->fp;
    row->zNormSql = NULL;
    row->nNormSql = 0;
    row->excluded =
        reqlog_fingerprint_is_excluded((char *)fingerprint) ? "Y" : "N";
    row->has_query_info = "N";
    row->total_sql_pagein_read = counts[0];
    row->total_sql_pagein_read_io = counts[1];
    row->total_write_pagein_read = counts[2];
    row->total_write_pagein_read_io = counts[3];
    row->total_replication_pagein_read = counts[4];
    row->total_replication_pagein_read_io = counts[5];
    ctx->copied++;
}

static int fingerprints_callback(void **data, int *npoints)
{
    int rc = SQLITE_OK;
    int rtstats_total = 0;
    int count = 0;
    int capacity;
    int copied = 0;
    struct fingerprint_track_systbl *pFp = NULL;
    hash_t *seen = NULL;
    unsigned char *seen_keys = NULL; /* raw 16-byte keys backing `seen` */

    *npoints = 0;
    *data = NULL;

    /* Before the fingerprint-hash lock; this takes only the rtstats lock. */
    bdb_fingerprint_rtstats_foreach(rtstats_count_cb, &rtstats_total);

    Pthread_mutex_lock(&gbl_fingerprint_hash_mu);
    if (gbl_fingerprint_hash != NULL)
        hash_info(gbl_fingerprint_hash, NULL, NULL, NULL, NULL, &count, NULL, NULL);

    capacity = count + rtstats_total;
    if (capacity > 0) {
        pFp = calloc(capacity, sizeof(struct fingerprint_track_systbl));
        if (count > 0) {
            seen = hash_init(FINGERPRINTSZ);
            seen_keys = malloc((size_t)count * FINGERPRINTSZ);
        }
    }

    if (pFp == NULL || (count > 0 && (seen == NULL || seen_keys == NULL))) {
        rc = (capacity > 0) ? SQLITE_NOMEM : SQLITE_OK;
    } else if (count > 0) {
        struct fingerprint_track *pEntry;
        void *hash_cur;
        unsigned int hash_cur_buk;
        pEntry = hash_first(gbl_fingerprint_hash, &hash_cur, &hash_cur_buk);
        while (pEntry != NULL && copied < count) {
            uint64_t counts[BDB_FINGERPRINT_RTSTATS_NCOUNTS] = {0};
            util_tohex(pFp[copied].fp, (char *)pEntry->fingerprint, FINGERPRINTSZ);
            pFp[copied].fingerprint = pFp[copied].fp;
            pFp[copied].count = pEntry->count;
            pFp[copied].cost = pEntry->cost;
            pFp[copied].time = pEntry->time;
            pFp[copied].prepTime = pEntry->prepTime;
            pFp[copied].rows = pEntry->rows;
            pFp[copied].excluded =
                reqlog_fingerprint_is_excluded((char *)pEntry->fingerprint) ? "Y" : "N";
            pFp[copied].has_query_info = "Y";
            if (pEntry->zNormSql != NULL) {
                pFp[copied].zNormSql = strdup(pEntry->zNormSql);
                pFp[copied].nNormSql = strlen(pEntry->zNormSql);
                assert(pFp[copied].nNormSql == pEntry->nNormSql);
            }
            bdb_fingerprint_rtstats_get(pEntry->fingerprint, FINGERPRINTSZ, counts);
            pFp[copied].total_sql_pagein_read = counts[0];
            pFp[copied].total_sql_pagein_read_io = counts[1];
            pFp[copied].total_write_pagein_read = counts[2];
            pFp[copied].total_write_pagein_read_io = counts[3];
            pFp[copied].total_replication_pagein_read = counts[4];
            pFp[copied].total_replication_pagein_read_io = counts[5];
            /* remember this fingerprint so the rtstats merge below skips it */
            memcpy(&seen_keys[copied * FINGERPRINTSZ], pEntry->fingerprint, FINGERPRINTSZ);
            hash_add(seen, &seen_keys[copied * FINGERPRINTSZ]);
            copied++;
            pEntry = hash_next(gbl_fingerprint_hash, &hash_cur, &hash_cur_buk);
        }
    }
    Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);

    /* No fingerprint-hash lock needed here; `seen` is a private snapshot. */
    if (rc == SQLITE_OK && pFp != NULL) {
        struct rtstats_merge_ctx ctx = {seen, pFp, capacity, copied};
        bdb_fingerprint_rtstats_foreach(rtstats_merge_cb, &ctx);
        copied = ctx.copied;
        *data = pFp;
        *npoints = copied;
    } else if (pFp != NULL) {
        release_callback(pFp, copied);
    }

    if (seen != NULL)
        hash_free(seen);
    free(seen_keys);
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
        sizeof(struct fingerprint_track_systbl),
        CDB2_CSTRING, "fingerprint", -1,
        offsetof(struct fingerprint_track_systbl, fingerprint),
        CDB2_INTEGER, "count", -1,
        offsetof(struct fingerprint_track_systbl, count),
        CDB2_INTEGER, "total_cost", -1,
        offsetof(struct fingerprint_track_systbl, cost),
        CDB2_INTEGER, "total_time", -1,
        offsetof(struct fingerprint_track_systbl, time),
        CDB2_INTEGER, "total_prep_time", -1,
        offsetof(struct fingerprint_track_systbl, prepTime),
        CDB2_INTEGER, "total_rows", -1,
        offsetof(struct fingerprint_track_systbl, rows),
        CDB2_INTEGER, "total_sql_pagein_read", -1,
        offsetof(struct fingerprint_track_systbl, total_sql_pagein_read),
        CDB2_INTEGER, "total_sql_pagein_read_io", -1,
        offsetof(struct fingerprint_track_systbl, total_sql_pagein_read_io),
        CDB2_INTEGER, "total_write_pagein_read", -1,
        offsetof(struct fingerprint_track_systbl, total_write_pagein_read),
        CDB2_INTEGER, "total_write_pagein_read_io", -1,
        offsetof(struct fingerprint_track_systbl, total_write_pagein_read_io),
        CDB2_INTEGER, "total_replication_pagein_read", -1,
        offsetof(struct fingerprint_track_systbl, total_replication_pagein_read),
        CDB2_INTEGER, "total_replication_pagein_read_io", -1,
        offsetof(struct fingerprint_track_systbl, total_replication_pagein_read_io),
        CDB2_CSTRING, "normalized_sql", -1,
        offsetof(struct fingerprint_track_systbl, zNormSql),
        CDB2_CSTRING, "excluded_from_longreqs", -1,
        offsetof(struct fingerprint_track_systbl, excluded),
        CDB2_CSTRING, "has_query_info", -1,
        offsetof(struct fingerprint_track_systbl, has_query_info),
        SYSTABLE_END_OF_FIELDS);
}
