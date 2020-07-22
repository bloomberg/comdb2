#ifndef INCLUDE_BT_CACHE_H
#define INCLUDE_BT_CACHE_H

struct __db;
int rcache_find(struct __db *, void **cached_pg, uint32_t * slot);
int rcache_save(struct __db *, void *page);
volatile int rcache_bump(DB *dbp);
void rcache_invalidate(uint32_t slot);

#endif //INCLUDE_BT_CACHE_H
