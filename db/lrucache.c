/*
   Copyright 2015 Bloomberg Finance L.P.

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
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>

#include "list.h"
#include "plhash.h"

#include "lrucache.h"
#include <mem_uncategorized.h>
#include <mem_override.h>
#include <logmsg.h>

struct lrucache *lrucache_init(hashfunc_t *hashfunc, cmpfunc_t *cmpfunc,
                               void (*freefunc)(void *), int offset, int keyoff,
                               int keysz, int maxent, int64_t maxsize)
{
    struct lrucache *cache;

    cache = malloc(sizeof(struct lrucache));
    cache->maxent = maxent;
    cache->freefunc = freefunc;
    cache->offset = offset;
    cache->keyoff = keyoff;
    cache->keysz = keysz;
    cache->maxsize = maxsize;
    cache->size = 0;
    listc_init(&cache->lru, offset + offsetof(struct lrucache_link, lnk));
    listc_init(&cache->used, offset + offsetof(struct lrucache_link, lnk));
    cache->h = hash_init_user(hashfunc, cmpfunc, keyoff, keysz);

    return cache;
}

int lrucache_hasentry(struct lrucache *cache, void *key)
{
    void *ent = hash_find(cache->h, key);
    if (ent) {
        return 1;
    }
    return 0;
}

void *lrucache_find(struct lrucache *cache, void *key)
{
    void *ent = hash_find(cache->h, key);
    if (ent) {
        struct lrucache_link *lent;
        lent = (struct lrucache_link *)((uintptr_t)ent + cache->offset);
        if (lent->invalid)
            return NULL;
        lent->ref++;
        lent->hits++;
        if (lent->ref == 1) {
            listc_rfl(&cache->lru, ent);
            listc_abl(&cache->used, ent);
        }
    }
    return ent;
}

void lrucache_add(struct lrucache *cache, void *item, int64_t size)
{
    void *ent;
    struct lrucache_link *lent;

    lent = (struct lrucache_link *)((uintptr_t)item + cache->offset);
    lent->ref = 0;
    lent->hits = 0;
    lent->size = size;
    lent->invalid = 0;

    if (cache->maxent) {
        while (cache->lru.count >= cache->maxent) {
            ent = listc_rtl(&cache->lru);
            if (ent) {
                int ret = hash_del(cache->h, ent);
                if (ret != 0) {
                    logmsg(LOGMSG_ERROR, "NOT DELETED.\n");
                } else {
                    cache->freefunc(ent);
                }
            } else {
                return;
            }
        }
    }

    if (cache->maxsize) {
        while (((cache->size + size) > cache->maxsize)) {
            ent = listc_rtl(&cache->lru);
            if (ent) {
                struct lrucache_link *l = (struct lrucache_link *)((uintptr_t)ent + cache->offset);
                cache->size -= l->size;
                int ret = hash_del(cache->h, ent);
                if (ret != 0) {
                    logmsg(LOGMSG_ERROR, "NOT DELETED.\n");
                } else {
                    cache->freefunc(ent);
                }
            }
        }
        cache->size += size;
    }

    hash_add(cache->h, item);
    listc_abl(&cache->lru, item);
}



static int finalize_hint_hash(void *hash_entry, void *cache_)
{
    struct lrucache *cache = cache_;
    logmsg(LOGMSG_INFO, "DELETING OUT OF LRU CACHE.\n");
    cache->freefunc(hash_entry);
    return 0;
}

void lrucache_destroy(struct lrucache *cache)
{
    void *ent;
    int used_count;

    used_count = cache->used.count;
    if (used_count != 0) {
        logmsg(LOGMSG_WARN, 
            "trying to destroy cache with in-use entries: %d entries on list\n",
            used_count);
        return;
    }

    ent = listc_rtl(&cache->lru);
    while (ent) {
        hash_del(cache->h, ent);
        cache->freefunc(ent);
        ent = listc_rtl(&cache->lru);
    }
    /* Lets see if something is remaining. */
    hash_for(cache->h, finalize_hint_hash, cache);
    hash_free(cache->h);
    free(cache);
}

void lrucache_release(struct lrucache *cache, void *key)
{
    void *ent;
    struct lrucache_link *lent;

    ent = hash_find(cache->h, key);
    if (ent == NULL) {
        logmsg(LOGMSG_ERROR, "releasing key, but not found?\n");
        return;
    }
    lent = (struct lrucache_link *)((uintptr_t)ent + cache->offset);

    lent->ref--;
    if (lent->ref < 0) {
        logmsg(LOGMSG_ERROR, "key released more often than found, ref %d\n",
                lent->ref);
        return;
    } else if (lent->ref == 0) {
        listc_rfl(&cache->used, ent);
        listc_abl(&cache->lru, ent);

        if (lent->invalid) {
            int ret = hash_del(cache->h, ent);
            if (ret == 0)
                cache->freefunc(ent);
        }
    }
}

void lrucache_foreach(struct lrucache *cache, void (*display)(void *, void *),
                      void *usrptr)
{
    void *ent;
    linkc_t *l;

    logmsg(LOGMSG_USER, "%d in lru, %d in used\n", cache->lru.count, cache->used.count);
    hash_dump_stats(cache->h, stdout, NULL);

    logmsg(LOGMSG_USER, "lru:\n");
    ent = cache->lru.top;
    while (ent) {
        display(ent, usrptr);

        uintptr_t p = (uintptr_t)ent + cache->lru.diff +
                      offsetof(struct lrucache_link, lnk);
        l = (linkc_t *)p;
        ent = l->next;
    }

    logmsg(LOGMSG_USER, "used:\n");
    ent = cache->used.top;
    while (ent) {
        display(ent, usrptr);

        uintptr_t p = (uintptr_t)ent + cache->lru.diff +
                      offsetof(struct lrucache_link, lnk);
        l = (linkc_t *)p;
        ent = l->next;
    }
}

void lrucache_invalidate(struct lrucache *cache, void *key) {
    void *ent = hash_find(cache->h, key);
    if (ent) {
        struct lrucache_link *lent;
        lent = (struct lrucache_link *)((uintptr_t)ent + cache->offset);
        lent->invalid = 1;

        // if the entry isn't referenced, delete it in place, otherwise
        // last user will delete on return
        if (lent->ref == 0) {
            int ret = hash_del(cache->h, ent);
            if (ret == 0)
                cache->freefunc(ent);
        }
    }
}
