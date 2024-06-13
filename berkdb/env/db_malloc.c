#include "db_config.h"

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <stdlib.h>
#include <string.h>
#endif

#include "db_int.h"


/* replace berkeley's shalloc code.  we never use shared memory regions,
   so we don't need it */

int __gbl_use_malloc_for_regions = 0;

/* this can only be called BEFORE any environments are created */
void
berkdb_use_malloc_for_regions()
{
	__gbl_use_malloc_for_regions = 1;
}

int
__db_shalloc_size_malloc(size_t len, size_t align)
{
	/* Never align to less than a db_align_t boundary. */
	if (align <= sizeof(db_align_t))
		align = sizeof(db_align_t);

	return ((int)(ALIGN(len, align)));
}

int
__db_shalloc_malloc(void *p, size_t len, size_t align, void *retp)
{
	int bucket;
	void *rp;
	HEAP *h = *(HEAP **)p;
    int rc;

	if (align < sizeof(int))
		align = sizeof(int);

	rc = comdb2_posix_memalign(h->msp, &rp, align, len);
	if (rc != 0)
		return rc;

	h->used += len;
	h->blocks++;

#ifdef _LINUX_SOURCE
	bucket = 31 - __builtin_clz(len);
	if (bucket < 32)
		h->blocksz[bucket]++;
#endif

	*(void **)retp = rp;
	return 0;
}

void
__db_shalloc_free_malloc(void *regionp, void *ptr)
{
	HEAP *h = *(HEAP **)regionp;
	int bucket;
	size_t sz = comdb2_malloc_usable_size(ptr);
	h->used -= sz;
	h->blocks--;
#ifdef _LINUX_SOURCE
	bucket = 31 - __builtin_clz(sz);
	h->blocksz[bucket]--;
#endif
	comdb2_free(ptr);
}

size_t
__db_shsizeof_malloc(void *ptr)
{
	return comdb2_malloc_usable_size(ptr);
}

void
__db_shalloc_dump_malloc(void *p)
{
}
