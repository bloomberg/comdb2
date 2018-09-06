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

void
__db_shalloc_init_malloc(void *area, size_t size, DB_MUTEX *lock)
{
	HEAP *h;

	h = area;
	h->used = 0;
	h->blocks = 0;
	h->lock = lock;
	memset(h->blocksz, 0, sizeof(h->blocksz));
}

/* use berkeley stuff for this routine, doesn't matter */
struct __data {
	size_t len;
};

int
__db_shalloc_size_malloc(size_t len, size_t align)
{
	/* Never allocate less than the size of a struct __data. */
	if (len < sizeof(struct __data))
		len = sizeof(struct __data);

	/* Never align to less than a db_align_t boundary. */
	if (align <= sizeof(db_align_t))
		align = sizeof(db_align_t);

	return ((int)(ALIGN(len, align) + sizeof(struct __data)));
}

int
__db_shalloc_malloc(void *p, size_t len, size_t align, void *retp)
{
	unsigned long long *mem;
	HEAP *h;
	int bucket;

	memcpy(&h, p, sizeof(HEAP *));

	if (align < sizeof(int))
		align = sizeof(int);

	if (h->used + len > h->size)
		return ENOMEM;

	mem = comdb2_malloc(h->msp, len + sizeof(unsigned long long));
	if (mem == NULL)
		return ENOMEM;

	mem[0] = len;

	h->used += len;
	h->blocks++;

#ifdef _LINUX_SOURCE
	bucket = 31 - __builtin_clz(len);
	if (bucket < 32)
		h->blocksz[bucket]++;
#endif

	*(void **)retp = &mem[1];
	return 0;
}

void
__db_shalloc_free_malloc(void *regionp, void *ptr)
{
	unsigned long long *mem = ptr;
	HEAP *h;
	int bucket;

	memcpy(&h, regionp, sizeof(HEAP *));
	h->used -= mem[-1];
	h->blocks--;
#ifdef _LINUX_SOURCE
	bucket = 31 - __builtin_clz(mem[-1]);
	h->blocksz[bucket]--;
#endif

	comdb2_free(&mem[-1]);
}

size_t
__db_shsizeof_malloc(void *ptr)
{
	unsigned long long *mem;

	mem = ptr;
	return mem[-1];
}

void
__db_shalloc_dump_malloc(void *p)
{
}
