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

static void append_entries(systable_api_history_t **data, int *nrecords, api_history_t *api_history, const char *host, const char *task)
{
    acquire_api_history_lock(api_history, 0);
    int num_entries = get_num_api_history_entries(api_history);
    assert(num_entries > -1);
    if (!num_entries) {
        release_api_history_lock(api_history);
        return;
    }

    int prev = *nrecords;
    *nrecords += num_entries;
    systable_api_history_t *buffer = realloc(*data, *nrecords * sizeof(systable_api_history_t));
    
    void *curr = NULL;
    unsigned int iter = 0;
    for (unsigned int i = prev; i < *nrecords; i++) {
        api_driver_t *entry = get_next_api_history_entry(api_history, &curr, &iter);
        assert(entry);

        buffer[i].host = strdup(host);
        buffer[i].task = strdup(task);
        buffer[i].api_driver_name = strdup(entry->name);
        buffer[i].api_driver_version = strdup(entry->version);

        dttz_t dt = (dttz_t){.dttz_sec = entry->last_seen};
        dttz_to_client_datetime(&dt, "UTC", &buffer[i].last_seen); 
    }

    *data = buffer;
    release_api_history_lock(api_history); 
}

int init_api_history_data(void **data, int *nrecords)
{
    *nrecords = 0;
    systable_api_history_t *systable = calloc(*nrecords, sizeof(systable_api_history_t));
    acquire_clientstats_lock(0);
   
    void *curr = NULL;
    unsigned int iter = 0;
    nodestats_t *entry = get_next_clientstats_entry(&curr, &iter);
    
    while (entry) {
        assert(entry->rawtotals.api_history);
        Pthread_mutex_lock(&entry->rawtotals.lk);
        append_entries(&systable, nrecords, entry->rawtotals.api_history, entry->host, entry->task);
        Pthread_mutex_unlock(&entry->rawtotals.lk);
        entry = get_next_clientstats_entry(&curr, &iter);
    }

    *data = systable;
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
