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

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <unistd.h>

#include "comdb2.h"
#include "reqlog_int.h"
#include "sqlite3.h"
#include "ezsystables.h"

sqlite3_module systblApiHistoryModule = {
    .access_flag = CDB2_ALLOW_ALL,
    .systable_lock = "comdb2_tables",
};

typedef struct systable_api_history {
    char *host;
    char *task;
    char *api_driver_name;
    char *api_driver_version;
    cdb2_client_datetime_t last_seen;
} systable_api_history_t;

typedef struct api_history_collect_ctx {
    systable_api_history_t *base;
    const char *host;
    const char *task;
    int nrecords;
    int nalloc;
} api_history_collect_ctx_t;

void free_api_history_data(void *data, int nrecords);

static int collect_api_history_rows(void *obj, void *arg)
{
    api_driver_t *entry = (api_driver_t *)obj;
    api_history_collect_ctx_t *ctx = (api_history_collect_ctx_t *)arg;
    if (ctx->nrecords >= ctx->nalloc) {
        
        size_t new_size = ctx->nalloc * 2 * sizeof(systable_api_history_t);
        systable_api_history_t *new_block = realloc(ctx->base, new_size);
        if (!new_block) {
            free_api_history_data(ctx->base, ctx->nrecords);
            ctx->base = NULL;
            ctx->nrecords = 0;
            return SQLITE_NOMEM;
        }
        ctx->base = new_block;
        ctx->nalloc = ctx->nalloc * 2;
    }
    systable_api_history_t *row = &ctx->base[ctx->nrecords];
    row->host = strdup(ctx->host ? ctx->host : "unknown");
    row->task = strdup(ctx->task ? ctx->task : "unknown");
    row->api_driver_name = strdup(entry->name ? entry->name : "unknown");
    row->api_driver_version = strdup(entry->version ? entry->version : "unknown");
    dttz_t dt = (dttz_t){.dttz_sec = entry->last_seen ? entry->last_seen : time(NULL)};
    dttz_to_client_datetime(&dt, "UTC", &row->last_seen);
    ctx->nrecords++;
    return 0;
}

static int count_api_entries(void *obj, void *arg)
{
    nodestats_t *entry = (nodestats_t *)obj;
    int *total = (int *)arg;
    *total += get_num_api_history_entries(&entry->rawtotals);
    return 0;
}

static int collect_node_api_history(void *obj, void *arg)
{
    nodestats_t *entry = (nodestats_t *)obj;
    api_history_collect_ctx_t *ctx = (api_history_collect_ctx_t *)arg;
    ctx->host = entry->host;
    ctx->task = entry->task;
    if (entry->rawtotals.api_history) {
        Pthread_mutex_lock(&entry->rawtotals.lk);
        int rc = hash_for(entry->rawtotals.api_history, collect_api_history_rows, ctx);
        Pthread_mutex_unlock(&entry->rawtotals.lk);
        if (rc) return rc;
    }
    return 0;
}

int init_api_history_data(void **data, int *nrecords)
{
    *nrecords = 0;
    *data = NULL;

    acquire_clientstats_lock(0);

    int total_entries = 0;
    hash_for_clientstats(count_api_entries, &total_entries);

    if (total_entries == 0) { //no clientstats so no api history entries
        release_clientstats_lock();
        return 0;
    }

    systable_api_history_t *systable = calloc(total_entries, sizeof(systable_api_history_t));
    if (!systable) {
        logmsg(LOGMSG_ERROR, "%s: out of memory for %d entries\n", __func__, total_entries);
        release_clientstats_lock();
        return SQLITE_NOMEM;
    }

    api_history_collect_ctx_t ctx = { .base = systable, .host = NULL, .task = NULL, .nrecords = 0, .nalloc = total_entries };
    int rc = hash_for_clientstats(collect_node_api_history, &ctx);
    if (rc) {
        if (ctx.base) free_api_history_data(ctx.base, ctx.nrecords);
        release_clientstats_lock();
        return rc;
    }

    *nrecords = ctx.nrecords;
    *data = ctx.base;
    release_clientstats_lock();
    return 0;
}

void free_api_history_data(void *data, int nrecords)
{
    systable_api_history_t *systable = (systable_api_history_t *)data;

    for (int i = 0; i < nrecords; i++) {
        free(systable[i].host);
        free(systable[i].task);
        free(systable[i].api_driver_name);
        free(systable[i].api_driver_version);
    }

    free(data);
}

int systblApiHistoryInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_api_history", &systblApiHistoryModule,
        init_api_history_data, free_api_history_data, sizeof(systable_api_history_t),
        CDB2_CSTRING, "host", -1, offsetof(systable_api_history_t, host),
        CDB2_CSTRING, "task", -1, offsetof(systable_api_history_t, task),
        CDB2_CSTRING, "api_driver_name", -1, offsetof(systable_api_history_t, api_driver_name),
        CDB2_CSTRING, "api_driver_version", -1, offsetof(systable_api_history_t, api_driver_version),
        CDB2_DATETIME, "last_seen", -1, offsetof(systable_api_history_t, last_seen),
        SYSTABLE_END_OF_FIELDS);
}
