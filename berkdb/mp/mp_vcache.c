#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <alloca.h>
#include <limits.h>
#include <sys/types.h>
#include <limits.h>
#include <string.h>
#include <pthread.h>
#include <poll.h>

#include "db_config.h"
#include "db_int.h"
#include "dbinc/btree.h"
#include "dbinc/mp.h"
#include "dbinc/log.h"
#include "dbinc/txn.h"
#include "dbinc/db_swap.h"
#include "dbinc/lock.h"
#include "dbinc/mutex.h"
#include "btree/bt_cache.h"
#include "dbinc/db_shash.h"
#include "dbinc/hmac.h"
#include "dbinc_auto/hmac_ext.h"

#include "thdpool.h"
#include "ctrace.h"
#include "logmsg.h"
#include "comdb2_atomic.h"
#include "thrman.h"
#include "thread_util.h"
#include "thread_stats.h"
#include <pool.h>
#include "locks_wrap.h"

extern int free_it(void *obj, void *arg);
extern void destroy_hash(hash_t *h, int (*free_func)(void *, void *));

int MEMPV_CACHE_ENTRY_NOT_FOUND = -1;

void __mempv_cache_dump(MEMPV_CACHE *cache);

static int __mempv_cache_page_destroy(cache_page)
	MEMPV_CACHE_PAGE_VERSIONS *cache_page;
{
	destroy_hash(cache_page->versions, free_it);
	return 0;
}

/*
 * __mempv_cache_init --
 * Initializes a cache. 
 *
 * dbenv: Associated dbenv.
 * cache: Allocated cache to be initialized.
 *
 * Returns 0 on success and non-0 on failure.
 *
 * PUBLIC: int __mempv_cache_init
 * PUBLIC:	__P((DB_ENV *, MEMPV_CACHE *));
 */
int __mempv_cache_init(dbenv, cache)
	DB_ENV *dbenv;
	MEMPV_CACHE *cache;
{
	int ret;

	ret = 0;

	cache->num_cached_pages = 0;
	cache->pages = hash_init_o(offsetof(MEMPV_CACHE_PAGE_VERSIONS, key), sizeof(MEMPV_CACHE_PAGE_KEY)); 
	if (cache->pages == NULL) {
		ret = ENOMEM;
		goto done;
	}

	listc_init(&cache->evict_list, offsetof(MEMPV_CACHE_PAGE_HEADER, evict_link)); 

	pthread_rwlock_init(&(cache->lock), NULL);
done:
	return ret;
}

/*
 * __mempv_cache_destroy --
 * Destroys a cache. 
 *
 * dbenv: Associated dbenv.
 * cache: Cache to be destroyed
 *
 * PUBLIC: void __mempv_cache_destroy
 * PUBLIC:	__P((MEMPV_CACHE *));
 */
void __mempv_cache_destroy(cache)
	MEMPV_CACHE *cache;
{
	hash_for(cache->pages, __mempv_cache_page_destroy, NULL);
	destroy_hash(cache->pages, free_it);

	pthread_rwlock_destroy(&(cache->lock));
}

/*
 * __mempv_cache_evict_page --
 * Evicts a page version from the cache and frees its resources.
 * If the evicted version is the only version of a page in the cache, then the list of versions 
 * associated with that page is freed UNLESS this list is passed in as `pinned_version_list`.
 *
 * dbp: Open db.
 * cache: Target cache.
 * pinned_version_list: A list of versions that cannot be freed or NULL.
 *
 * Returns 0 on success and non-0 on failure.
 *
 * PUBLIC: static int __mempv_cache_evict_page
 * PUBLIC:	__P((DB *, MEMPV_CACHE *, MEMPV_CACHE_PAGE_VERSIONS *));
 */
static int __mempv_cache_evict_page(dbp, cache, pinned_version_list)
	DB *dbp;
	MEMPV_CACHE *cache;
	MEMPV_CACHE_PAGE_VERSIONS *pinned_version_list;
{
	MEMPV_CACHE_PAGE_HEADER *to_evict;

	to_evict = listc_rtl(&cache->evict_list);
	if (to_evict == NULL) {
		return 1;
	}

	// Delete this version from the list of versions for its page.
	hash_del(to_evict->cache->versions, to_evict);
	if ((pinned_version_list != to_evict->cache) && (hash_get_num_entries(to_evict->cache->versions) == 0)) {
		// If we emptied the list of versions for a page and we are not about to add a version for the page,
		// then we can delete the list of versions.

		hash_del(cache->pages, to_evict->cache);
		hash_free(to_evict->cache->versions); 
		__os_free(dbp->dbenv, to_evict->cache); 
	}

	__os_free(dbp->dbenv, to_evict); 
	cache->num_cached_pages--;
	
	return 0;
}

/*
 * __mempv_cache_put --
 * Puts *a copy* of the page version given by `bhp` into the cache.
 *
 * dbp: Open db.
 * cache: Target cache.
 * file_id: File id associated with the page.
 * pgno: Page number.
 * bhp: Buffer header for the page.
 * target_lsn: Target LSN of the running snapshot transaction. A transaction can
 * 				use this cached version iff it has the same target LSN.
 *
 * Returns 0 on success and non-0 on failure.
 *
 * PUBLIC: int __mempv_cache_put
 * PUBLIC:	__P((DB *, MEMPV_CACHE *, u_int8_t[DB_FILE_ID_LEN], db_pgno_t, BH *, DB_LSN));
 */
int __mempv_cache_put(dbp, cache, file_id, pgno, bhp, target_lsn)
	DB *dbp;
	MEMPV_CACHE *cache;
	u_int8_t file_id[DB_FILE_ID_LEN];
	db_pgno_t pgno;
	BH *bhp;
	DB_LSN target_lsn;
{
	MEMPV_CACHE_PAGE_VERSIONS *versions;
	MEMPV_CACHE_PAGE_KEY key;
	MEMPV_CACHE_PAGE_HEADER *page_header;
	int ret, allocd_versions, allocd_header;

	versions = NULL;
	page_header = NULL;
	ret = allocd_versions = allocd_header = 0;
	key.pgno = pgno;
	memcpy(key.ufid, file_id, DB_FILE_ID_LEN);

	pthread_rwlock_wrlock(&(cache->lock));

	versions = hash_find(cache->pages, &key);
	if (versions != NULL) {
		// If we already have a list of versions for this page, we can just add this version to that list.
		goto put_version;
	}

	// We don't already have a list of versions for this page. Create one.

	__os_malloc(dbp->dbenv, sizeof(MEMPV_CACHE_PAGE_VERSIONS), &versions); 
	if (versions == NULL) {
		ret = ENOMEM;
		goto err;
	}
	allocd_versions = 1;

	bzero(versions, sizeof(MEMPV_CACHE_PAGE_VERSIONS));
	versions->key = key;
	versions->versions = hash_init_o(offsetof(MEMPV_CACHE_PAGE_HEADER, snapshot_lsn), sizeof(DB_LSN)); 
	if (versions->versions == NULL) {
		ret = ENOMEM;
		goto err;
	}

	ret = hash_add(cache->pages, versions);
	if (ret) {
		goto err;
	}

put_version:
	page_header = hash_find_readonly(versions->versions, &target_lsn);
	if (page_header != NULL) {
		// We already have this exact version in the cache. Do nothing.
		goto done;
	}

	// We need to allocate space for the new page version.

	if(cache->num_cached_pages == dbp->dbenv->attr.mempv_max_cache_entries) {
		if ((ret = __mempv_cache_evict_page(dbp, cache, versions)), ret != 0) {
			logmsg(LOGMSG_ERROR, "%s: Could not evict cache page\n", __func__);
			goto err;
		}
	}

	__os_malloc(dbp->dbenv, sizeof(MEMPV_CACHE_PAGE_HEADER)-sizeof(u_int8_t) + SSZA(BH, buf) + dbp->pgsize, &page_header); 
	if (page_header == NULL) {
		ret = ENOMEM;
		goto err;
	}
	allocd_header = 1;

	memcpy((char*)(page_header->page), bhp, offsetof(BH, buf) + dbp->pgsize);

	page_header->snapshot_lsn = target_lsn;
	page_header->cache = versions;
	listc_abl(&cache->evict_list, page_header);

	ret = hash_add(versions->versions, page_header);
	if (ret) {
		logmsg(LOGMSG_ERROR, "%s: Could not add entry to cache\n", __func__);
		goto err;
	}

	cache->num_cached_pages++;

done:
	pthread_rwlock_unlock(&(cache->lock));
	return ret;
	
err:
	if (allocd_versions) {
		if (hash_find(cache->pages, &key)) {
			hash_del(cache->pages, versions);
		}
		if (versions->versions != NULL) {
			hash_free(versions->versions); 
		}
		__os_free(dbp->dbenv, versions); 
	}

	if (allocd_header) {
		if (!allocd_versions && hash_find(versions->versions, page_header)) {
			hash_del(versions->versions, page_header);	
		}
		listc_maybe_rfl(&cache->evict_list, page_header);
		__os_free(dbp->dbenv, page_header);
	}

	pthread_rwlock_unlock(&(cache->lock));
	return ret;
}

/*
 * __mempv_cache_get --
 * Gets a page version from the cache.
 *
 * dbp: Open db.
 * cache: Target cache.
 * file_id: File id associated with the page.
 * pgno: Page number.
 * target_lsn: Target LSN of the running snapshot transaction.
 * bhp: Buffer header for the page.
 *
 * Returns 0 on a cache hit or MEMPV_CACHE_ENTRY_NOT_FOUND on a cache miss.
 *
 * PUBLIC: int __mempv_cache_get
 * PUBLIC:	__P((DB *, MEMPV_CACHE *, u_int8_t[DB_FILE_ID_LEN], db_pgno_t, DB_LSN, BH *));
 */
int __mempv_cache_get(dbp, cache, file_id, pgno, target_lsn, bhp)
	DB *dbp;
	MEMPV_CACHE *cache;
	u_int8_t file_id[DB_FILE_ID_LEN];
	db_pgno_t pgno;
	DB_LSN target_lsn;
	BH *bhp;
{
	MEMPV_CACHE_PAGE_VERSIONS *versions;
	MEMPV_CACHE_PAGE_KEY key;
	MEMPV_CACHE_PAGE_HEADER *page_header;
	int ret;
	u_int8_t cks;

	versions = NULL;
	page_header = NULL;
	ret = 0;
	key.pgno = pgno;
	memcpy(key.ufid, file_id, DB_FILE_ID_LEN);

	pthread_rwlock_wrlock(&(cache->lock));

	versions = hash_find_readonly(cache->pages, &key);
	if (versions == NULL) {
		ret = MEMPV_CACHE_ENTRY_NOT_FOUND; 
		goto done;
	}

	page_header = hash_find_readonly(versions->versions, &target_lsn);
	if (page_header == NULL) {
		ret = MEMPV_CACHE_ENTRY_NOT_FOUND;
		goto done;
	}

	// Found the page in the cache. Update lru and copy it out.

	listc_rfl(&cache->evict_list, page_header);
	listc_abl(&cache->evict_list, page_header);

	memcpy(bhp, (char*)(page_header->page), offsetof(BH, buf) + dbp->pgsize);

done:
	pthread_rwlock_unlock(&(cache->lock));

	return ret;
}

static int __mempv_cache_page_version_dump(cache_page_version)
	MEMPV_CACHE_PAGE_HEADER *cache_page_version;
{
	printf("\n\t %p target lsn %d:%d\n", cache_page_version, cache_page_version->snapshot_lsn.file, cache_page_version->snapshot_lsn.offset);
	return 0;
}

static int __mempv_cache_page_dump(cache_page)
	MEMPV_CACHE_PAGE_VERSIONS *cache_page;
{
	printf("\n\n\t\tDUMPING PAGE %d ----\n", cache_page->key.pgno);
	hash_for(cache_page->versions, __mempv_cache_page_version_dump, NULL);
	printf("\n\n\t\tFINISHED DUMPING PAGE %d ----\n", cache_page->key.pgno);
	return 0;
}

void __mempv_cache_dump(cache)
	MEMPV_CACHE *cache;
{
	printf("DUMPING PAGE CACHE\n--------------------\n");
	hash_for(cache->pages, __mempv_cache_page_dump, NULL);
	printf("--------------------\nFINISHED DUMPING PAGE CACHE\n");
}

