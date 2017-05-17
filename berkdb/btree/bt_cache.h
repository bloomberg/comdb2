#ifndef INCLUDE_BT_CACHE_H
#define INCLUDE_BT_CACHE_H

struct __db;
int rcache_find(struct __db *, void **cached_pg, void **bfpool_pg,
	uint16_t * gen, uint32_t * slot);
int rcache_save(struct __db *, void *page, uint16_t gen);
void rcache_invalidate(uint32_t slot);

#define GET_BH_GEN(pg) (*(uint16_t *)((uint8_t *)pg - (offsetof(BH, buf) - offsetof(BH, generation))))

#endif //INCLUDE_BT_CACHE_H
