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

#ifndef INCLUDED_LRUCACHE_H
#define INCLUDED_LRUCACHE_H

#include <plhash_glue.h>
#include "list.h"

struct lrucache {
    int maxent;
    void (*freefunc)(void *);
    hash_t *h;
    int offset;
    int keyoff;
    int keysz;
    listc_t lru;
    listc_t used;
};

struct lrucache_link {
    linkc_t lnk;
    int ref;
    int hits;
};

typedef struct lrucache lrucache;
typedef struct lrucache_link lrucache_link;

struct lrucache *lrucache_init(hashfunc_t *hashfunc, cmpfunc_t *cmpfunc,
                               void (*freefunc)(void *), int offset, int keyoff,
                               int keysz, int maxent);
void *lrucache_find(struct lrucache *cache, void *key);

int lrucache_hasentry(struct lrucache *cache, void *key);

void lrucache_add(struct lrucache *cache, void *item);
void lrucache_destroy(struct lrucache *cache);
void lrucache_foreach(struct lrucache *cache, void (*display)(void *, void *),
                      void *usrptr);
void lrucache_set_maxent(struct lrucache *cache, int maxent);
void lrucache_release(struct lrucache *cache, void *key);

#endif
