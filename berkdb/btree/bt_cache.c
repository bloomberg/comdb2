#include <db.h>
#include <btree/bt_cache.h>
#include <crc32c.h>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <stdbool.h>
#include <signal.h>
#include <logmsg.h>

uint32_t rcache_hits;
uint32_t rcache_miss;
uint32_t rcache_savd;
uint32_t rcache_invalid;
uint32_t rcache_collide;

typedef struct {
	uint8_t fileid[DB_FILE_ID_LEN];
	uint16_t gen;
	uint32_t hitmiss;
	void *bfpool_pg;
	void *cached_pg;
} CacheSlot;

typedef struct {
	size_t pgsz;
	size_t count;
	CacheSlot slots[];
} CacheHndl;

static __thread CacheHndl *hndl = NULL;

void
rcache_init(size_t count, size_t pgsz)
{
#ifdef __x86_64
	if (pgsz % (4 * 1024) != 0) {
		logmsg(LOGMSG_ERROR, "cache size must be multiple of 4 KB");
		return;
	}
	size_t bytes = sizeof(CacheHndl)
	    + sizeof(CacheSlot) * count + pgsz * count;

	if ((hndl = malloc(bytes)) == NULL) {
		logmsg(LOGMSG_ERROR, "%s malloc failed:%zu bytes\n", __func__, bytes);
		return;
	}
	hndl->count = count;
	hndl->pgsz = pgsz;
	uint8_t *pages = (uint8_t *)&hndl->slots[count];
	CacheSlot *slot = &hndl->slots[0];
	CacheSlot *end = &hndl->slots[count];

	do {
		slot->bfpool_pg = NULL;

		slot->cached_pg = pages;
		pages += pgsz;
	} while (++slot != end);
#endif
}

static inline void
hash_fileid(void *fileid, uint32_t * crc, uint32_t * hash)
{
	*crc = crc32c(fileid, DB_FILE_ID_LEN);
	*hash = *crc % hndl->count;
}

void
rcache_destroy(void)
{
    if (hndl) {
        free(hndl);
        hndl = NULL;
    }
}

int
rcache_find(DB *dbp, void **cached_pg, void **bfpool_pg,
    uint16_t * gen, uint32_t * slot_ptr)
{
	if (hndl == NULL || dbp->pgsize > hndl->pgsz)
		return -1;
	uint32_t crc, slot;

	hash_fileid(dbp->fileid, &crc, &slot);
	if (crc == 0)
		return -1;
	CacheSlot *cache = &hndl->slots[slot];

	if (cache->bfpool_pg
	    && memcmp(cache->fileid, dbp->fileid, DB_FILE_ID_LEN) == 0) {
		*cached_pg = cache->cached_pg;
		*bfpool_pg = cache->bfpool_pg;
		*gen = cache->gen;
		*slot_ptr = slot;
		++rcache_hits;
		if (cache->hitmiss < 256)
			++cache->hitmiss;
		return 0;
	}
	++rcache_miss;
	return -1;
}

int
rcache_save(DB *dbp, void *page, uint16_t gen)
{
	if (hndl == NULL || dbp->pgsize > hndl->pgsz)
		return -1;
	uint32_t crc, slot;

	hash_fileid(dbp->fileid, &crc, &slot);
	if (crc == 0)
		return -1;
	CacheSlot *cache = &hndl->slots[slot];

	if (cache->bfpool_pg) {
		++rcache_collide;
		--cache->hitmiss;
		if (cache->hitmiss) {	// slot in active use
			return -1;
		}
	}
	cache->hitmiss = 1;
	cache->bfpool_pg = page;
	cache->gen = gen;
	memcpy(cache->cached_pg, page, dbp->pgsize);
	memcpy(cache->fileid, dbp->fileid, DB_FILE_ID_LEN);
	++rcache_savd;
	return 0;
}

void
rcache_invalidate(uint32_t slot)
{
	hndl->slots[slot].bfpool_pg = NULL;

	++rcache_invalid;
}
