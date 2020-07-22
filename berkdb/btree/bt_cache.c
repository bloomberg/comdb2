#include <db.h>
#include <db_int.h>
#include <dbinc/btree.h>
#include <dbinc/mp.h>
#include <crc32c.h>
#include <comdb2_atomic.h>
#include <logmsg.h>
#include <tohex.h>

#include "bt_cache.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <stdbool.h>
#include <signal.h>
#include <locks_wrap.h>

uint32_t rcache_hits;
uint32_t rcache_miss;
uint32_t rcache_savd;
uint32_t rcache_invalid;
int32_t rcache_collide;

int gbl_debug_rcache = 0;

typedef struct {
	uint8_t fileid[DB_FILE_ID_LEN];
	u_int64_t gen;
	int valid;
	uint32_t hitmiss;
	void *cached_pg;
} CacheSlot;

typedef struct {
    int id;
	size_t pgsz;
	size_t count;
	CacheSlot slots[];
} CacheHndl;

static __thread CacheHndl *hndl = NULL;

static pthread_mutex_t idlk = PTHREAD_MUTEX_INITIALIZER;
static int cacheid = 0;

static u_int64_t get_rootpage_gen(DB *dbp);

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
    Pthread_mutex_lock(&idlk);
    hndl->id = cacheid++;
    Pthread_mutex_unlock(&idlk);

	do {
		slot->cached_pg = pages;
		slot->hitmiss = 0;
		slot->valid = 0;
		pages += pgsz;
	} while (++slot != end);
#endif
}

static inline void
hash_fileid(void *fileid, uint32_t * crc, uint32_t * hash)
{
    char hex[DB_FILE_ID_LEN*2+1];
    util_tohex(hex, fileid, DB_FILE_ID_LEN);
    if (gbl_debug_rcache)
        logmsg(LOGMSG_USER, "hash %s -> %u slot %d\n", hex, *crc, *crc % hndl->count);
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
rcache_find(DB *dbp, void **cached_pg, uint32_t * slot_ptr) {
    char hex[DB_FILE_ID_LEN*2+1];
    if (hndl == NULL || dbp->pgsize > hndl->pgsz || dbp->type != DB_BTREE)
        return -1;
    uint32_t crc, slot;

    *cached_pg = NULL;

    hash_fileid(dbp->fileid, &crc, &slot);
    if (crc == 0)
        return -1;
    CacheSlot *cache = &hndl->slots[slot];

    if (cache->valid && memcmp(cache->fileid, dbp->fileid, DB_FILE_ID_LEN) == 0) {
        u_int64_t gen = get_rootpage_gen(dbp);
        if (gbl_debug_rcache)
            logmsg(LOGMSG_USER, "%d fnd %s slot %d gen %"PRIu64" rootpagegen %"PRIu64" hitmiss %d valid %d\n",
                   hndl->id, util_tohex(hex, (const char*) dbp->fileid, DB_FILE_ID_LEN),
                   slot, cache->gen, gen, cache->hitmiss, (int) cache->valid);
        if (gen == cache->gen) {
            *cached_pg = cache->cached_pg;
            *slot_ptr = slot;
            ++rcache_hits;
            if (cache->hitmiss < 256)
                ++cache->hitmiss;
            return 0;
        } else {
            rcache_invalidate(slot);
        }
    } else {
        if (gbl_debug_rcache)
            logmsg(LOGMSG_USER, "%d notfnd %s slot %d valid %d\n",
                   hndl->id, util_tohex(hex, (const char*) dbp->fileid, DB_FILE_ID_LEN),
                   slot, cache->valid);
    }
	++rcache_miss;
	return -1;
}

static u_int64_t get_rootpage_gen(DB *dbp) {
   return ATOMIC_LOAD64(dbp->mpf->mfp->rootpagegen);
}

int
rcache_save(DB *dbp, void *page)
{
    char hex[DB_FILE_ID_LEN*2+1];
	if (hndl == NULL || dbp->pgsize > hndl->pgsz)
		return -1;
	uint32_t crc, slot;

	if (dbp->type != DB_BTREE)
	    return -1;

	hash_fileid(dbp->fileid, &crc, &slot);
	if (crc == 0)
		return -1;
	CacheSlot *cache = &hndl->slots[slot];

	if (gbl_debug_rcache)
        printf("%d save %s slot %d valid %d hitmiss %d rootpagegen %"PRIu64" saving %d\n",
               hndl->id, util_tohex(hex, (const char*) dbp->fileid, DB_FILE_ID_LEN),
               slot, (int) cache->valid, cache->hitmiss,
               get_rootpage_gen(dbp),
               cache->valid && (cache->hitmiss - 1) ? 0 : 1);

	if (cache->valid) {
		++rcache_collide;
		--cache->hitmiss;
		if (cache->hitmiss) {	// slot in active use
			return -1;
		}
	}
	cache->hitmiss = 1;
	cache->valid = 1;
	cache->gen = get_rootpage_gen(dbp);
	memcpy(cache->cached_pg, page, dbp->pgsize);
	memcpy(cache->fileid, dbp->fileid, DB_FILE_ID_LEN);
	++rcache_savd;
	return 0;
}

void
rcache_invalidate(uint32_t slot)
{
	hndl->slots[slot].valid = 0;
	if (gbl_debug_rcache)
        printf("%d invalidate slot %d\n", hndl->id, slot);
	++rcache_invalid;
}

volatile int rcache_bump(DB *dbp) {
    char hex[DB_FILE_ID_LEN*2+1];

    if (dbp->type != DB_BTREE)
        return 0;

    // uint64_t val = ATOMIC_ADD64(dbp->mpf->mfp->rootpagegen, 1);
    uint64_t val = dbp->mpf->mfp->rootpagegen++;
    if (gbl_debug_rcache)
        logmsg(LOGMSG_USER, "%d bump %s to %"PRIu64"\n", hndl ? hndl->id : -1,
               util_tohex(hex, (const char*) dbp->fileid, DB_FILE_ID_LEN), val);
    return 0;
}