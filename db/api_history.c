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
#include <ctype.h>
#include <sys_wrap.h>
#include <plhash_glue.h>
#include <pthread.h>
#include <string.h>

#include "logmsg.h"

#include "api_history.h"

struct api_history {
    hash_t *entries;
    pthread_rwlock_t lock;
};

static int hash_entry(api_driver_t *entry, int len)
{
    return hash_default_fixedwidth((unsigned char*)entry->name, strlen(entry->name));
}

static int compare_entries(api_driver_t *entry1, api_driver_t *entry2, int len)
{
    int rc = strcmp(entry1->name, entry2->name);
    if (!rc) rc = strcmp(entry1->version, entry2->version);
    return rc;
}

static char *toLower(char *str)
{
    char *curr = str;
    
    while (*curr) {
        *curr = tolower(*curr);
        ++curr;
    }
    
    return str;
}

static api_driver_t *create_entry(char *api_driver_name, char *api_driver_version)
{
    const char default_value[] = "unknown";
    api_driver_t *entry = malloc(sizeof(api_driver_t));

    entry->name = api_driver_name ? toLower(strdup(api_driver_name)) : strdup(default_value);
    entry->version = api_driver_version ? toLower(strdup(api_driver_version)) : strdup(default_value);

    return entry;
}

static int free_entry(api_driver_t *entry, void *arg)
{
    free(entry->name);
    free(entry->version);
    free(entry);
    return 0;
}

void acquire_api_history_lock(api_history_t *api_history, int write) 
{
    if (write) {
        Pthread_rwlock_wrlock(&api_history->lock);
    } else {
        Pthread_rwlock_rdlock(&api_history->lock);
    }
}

void release_api_history_lock(api_history_t *api_history) 
{
    Pthread_rwlock_unlock(&api_history->lock);
}

api_history_t *init_api_history()
{
    api_history_t *api_history = malloc(sizeof(api_history_t));
    assert(api_history);
    Pthread_rwlock_init(&api_history->lock, NULL);
    
    api_history->entries = hash_init_user((hashfunc_t *)hash_entry, (cmpfunc_t *)compare_entries, 0, 0);
    if (!api_history->entries) {
        logmsg(LOGMSG_FATAL, "Unable to initialize api history.\n");
        abort();
    }
    
    return api_history;
}

int free_api_history(api_history_t *api_history)
{
    assert(api_history);
    acquire_api_history_lock(api_history, 1);
    
    int rc = hash_for(api_history->entries, (hashforfunc_t *)free_entry, NULL);
    if (rc) {
        logmsg(LOGMSG_FATAL, "Unable to free api history entries.\n");
        release_api_history_lock(api_history);
        return rc;
    }

    hash_free(api_history->entries);
    release_api_history_lock(api_history);
    Pthread_rwlock_destroy(&api_history->lock);
    free(api_history);
    return 0;
}

api_driver_t *get_next_api_history_entry(api_history_t *api_history, void **curr, unsigned int *iter)
{
    assert(api_history);
    acquire_api_history_lock(api_history, 0);
    api_driver_t *entry;
    
    if (!*curr && !*iter) {
        entry = (api_driver_t *)hash_first(api_history->entries, curr, iter);
    } else {
        entry = (api_driver_t *)hash_next(api_history->entries, curr, iter);
    }
    
    release_api_history_lock(api_history);
    return entry;
}

int get_num_api_history_entries(api_history_t *api_history) {
    assert(api_history);
    acquire_api_history_lock(api_history, 0);
    
    int num = hash_get_num_entries(api_history->entries);
    
    release_api_history_lock(api_history);
    return num;
}

int update_api_history(api_history_t *api_history, char *api_driver_name, char *api_driver_version)
{
    assert(api_history);
    acquire_api_history_lock(api_history, 1);
    
    api_driver_t *search_entry = create_entry(api_driver_name, api_driver_version);
    api_driver_t *found_entry = hash_find(api_history->entries, search_entry);
    
    if (found_entry) {
        time(&found_entry->last_seen);
        free_entry(search_entry, NULL);
    } else {
        time(&search_entry->last_seen);
        int rc = hash_add(api_history->entries, search_entry);
        if (rc) {
            logmsg(LOGMSG_FATAL, "Unable to add api history entry.\n");
            release_api_history_lock(api_history);
            return rc;
        }
    }

    release_api_history_lock(api_history);
    return 0;
}
