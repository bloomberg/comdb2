/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"
#include "list.h"

#ifndef lint
static const char revid[] = "$Id: db_salloc.c,v 11.17 2003/01/08 04:42:01 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#endif

#include "db_int.h"
#include "logmsg.h"

/* Switch to control whether we allocate regions using malloc. */
extern int __gbl_use_malloc_for_regions;
void __db_shalloc_init_malloc(void *area, size_t size, DB_MUTEX *lock);
int __db_shalloc_size_malloc(size_t len, size_t align);
int __db_shalloc_malloc(void *p, size_t len, size_t align, void *retp);
void __db_shalloc_free_malloc(void *regionp, void *ptr);
size_t __db_shsizeof_malloc(void *ptr);
void __db_shalloc_dump_malloc(void *addr, FILE *fp);

/*
 * Implement shared memory region allocation, using simple first-fit algorithm.
 * The model is that we take a "chunk" of shared memory store and begin carving
 * it up into areas, similarly to how malloc works.  We do coalescing on free.
 *
 * The "len" field in the __data struct contains the length of the free region
 * (less the size_t bytes that holds the length).  We use the address provided
 * by the caller to find this length, which allows us to free a chunk without
 * requiring that the caller pass in the length of the chunk they're freeing.
 */
struct __data {
	size_t len;
	SH_LIST_ENTRY (__data) links;
};

SH_LIST_HEAD(__head, __data);

/*
 * __db_shalloc_init --
 *	Initialize the area as one large chunk.
 *
 * PUBLIC: void __db_shalloc_init __P((DB_ENV *, char *, void *, size_t, DB_MUTEX *));
 */
void
__db_shalloc_init(dbenv, description, area, size, lock)
	DB_ENV *dbenv;
	char *description;
	void *area;
	size_t size;
	DB_MUTEX *lock;
{
	struct __data *elp;
	struct __head *hp;
	size_t len;
	char desc[32];

	if (__gbl_use_malloc_for_regions) {
		int ret;
		HEAP *h;

		ret = __os_malloc(dbenv, sizeof(HEAP), &h);
		if (ret)
			return;
		h->mem = area;
		h->size = size;
		h->used = 0;
		len = h->size-sizeof(HEAP*);

		snprintf(desc, sizeof(desc) / sizeof(char),
		    dbenv->is_tmp_tbl ? "berkdb/%s_tmptbl" : "berkdb/%s",
		    description);
		h->msp = comdb2ma_create_with_base((void*) (((intptr_t)h->mem) +
		    sizeof(HEAP*)), len, len, desc, 0);
		if (h->msp == NULL) {
			__os_free(dbenv, h);
			return;
		}

		__os_strdup(dbenv, description, &h->description);
		h->blocks = 0;
		h->lock = lock;
		memset(h->blocksz, 0, sizeof(h->blocksz));
		memcpy(h->mem, &h, sizeof(HEAP*));
		/* 
		 * return the "real" application address. the reason
		 * we do all this is that berkeley doesn't always
		 * create a heap in allocated regions.
		 */
		listc_abl(&dbenv->regions, h);
#if 0
		printf("HEAP: area %p sz %d\n", h->mem, size);
#endif

		return;
	}

	hp = area;
	SH_LIST_INIT(hp);

	elp = (struct __data *)(hp + 1);
	elp->len = size - sizeof(struct __head) - sizeof(elp->len);
	SH_LIST_INSERT_HEAD(hp, elp, links, __data);
}

/*
 * __db_shalloc_size --
 *	Return the space needed for an allocation, including alignment.
 *
 * PUBLIC: int __db_shalloc_size __P((size_t, size_t));
 */
int
__db_shalloc_size(len, align)
	size_t len, align;
{
	if (__gbl_use_malloc_for_regions)
		return __db_shalloc_size_malloc(len, align);

	/* Never allocate less than the size of a struct __data. */
	if (len < sizeof(struct __data))
		len = sizeof(struct __data);

#ifdef DIAGNOSTIC
	/* Add room for a guard byte. */
	++len;
#endif

	/* Never align to less than a db_align_t boundary. */
	if (align <= sizeof(db_align_t))
		align = sizeof(db_align_t);

	return ((int)(ALIGN(len, align) + sizeof(struct __data)));
}

static void
bb_shalloc_hit(uint64_t start_time_us)
{
	uint64_t time_diff = bb_berkdb_fasttime() - start_time_us;
	struct bb_berkdb_thread_stats *stats;

	stats = bb_berkdb_get_thread_stats();
	stats->n_shallocs++;
	stats->shalloc_time_us += time_diff;

	stats = bb_berkdb_get_process_stats();
	stats->n_shallocs++;
	stats->shalloc_time_us += time_diff;
}

static void
bb_shalloc_free_hit(uint64_t start_time_us)
{
	uint64_t time_diff = bb_berkdb_fasttime() - start_time_us;
	struct bb_berkdb_thread_stats *stats;

	stats = bb_berkdb_get_thread_stats();
	stats->n_shalloc_frees++;
	stats->shalloc_free_time_us += time_diff;

	stats = bb_berkdb_get_process_stats();
	stats->n_shalloc_frees++;
	stats->shalloc_free_time_us += time_diff;
}

/*
 * __db_shalloc --
 *	Allocate some space from the shared region.
 *
 * PUBLIC: int __db_shalloc __P((void *, size_t, size_t, void *));
 */
int
__db_shalloc(p, len, align, retp)
	void *p, *retp;
	size_t len, align;
{
	struct __data *elp;
	size_t *sp;
	void *rp;

	uint64_t start_time_us = 0;

	if (gbl_bb_berkdb_enable_shalloc_timing)
		start_time_us = bb_berkdb_fasttime();

	if (__gbl_use_malloc_for_regions) {
		int ret;

		ret = __db_shalloc_malloc(p, len, align, retp);
		if (gbl_bb_berkdb_enable_shalloc_timing)
			bb_shalloc_hit(start_time_us);
		return ret;
	}

	/* Never allocate less than the size of a struct __data. */
	if (len < sizeof(struct __data))
		len = sizeof(struct __data);

#ifdef DIAGNOSTIC
	/* Add room for a guard byte. */
	++len;
#endif

	/* Never align to less than a db_align_t boundary. */
	if (align <= sizeof(db_align_t))
		align = sizeof(db_align_t);

	/* Walk the list, looking for a slot. */
	for (elp = SH_LIST_FIRST((struct __head *)p, __data);
	    elp != NULL; elp = SH_LIST_NEXT(elp, links, __data)) {
		/*
		 * Calculate the value of the returned pointer if we were to
		 * use this chunk.
		 *      + Find the end of the chunk.
		 *      + Subtract the memory the user wants.
		 *      + Find the closest previous correctly-aligned address.
		 */
		rp = (u_int8_t *)elp + sizeof(size_t) + elp->len;
		rp = (u_int8_t *)rp - len;
		rp = (u_int8_t *)((db_alignp_t) rp & ~(align - 1));

		/*
		 * Rp may now point before elp->links, in which case the chunk
		 * was too small, and we have to try again.
		 */
		if ((u_int8_t *)rp < (u_int8_t *)&elp->links)
			continue;

		*(void **)retp = rp;
#ifdef DIAGNOSTIC
		/*
		 * At this point, whether or not we still need to split up a
		 * chunk, retp is the address of the region we are returning,
		 * and (u_int8_t *)elp + sizeof(size_t) + elp->len gives us
		 * the address of the first byte after the end of the chunk.
		 * Make the byte immediately before that the guard byte.
		 */
		*((u_int8_t *)elp + sizeof(size_t) + elp->len - 1) = GUARD_BYTE;
#endif

#define	SHALLOC_FRAGMENT	32
		/*
		 * If there are at least SHALLOC_FRAGMENT additional bytes of
		 * memory, divide the chunk into two chunks.
		 */
		if ((u_int8_t *)rp >=
		    (u_int8_t *)&elp->links + SHALLOC_FRAGMENT) {
			sp = rp;
			*--sp = elp->len -
			    ((u_int8_t *)rp - (u_int8_t *)&elp->links);
			elp->len -= *sp + sizeof(size_t);
			if (gbl_bb_berkdb_enable_shalloc_timing)
				bb_shalloc_hit(start_time_us);
			return (0);
		}

		/*
		 * Otherwise, we return the entire chunk, wasting some amount
		 * of space to keep the list compact.  However, because the
		 * address we're returning to the user may not be the address
		 * of the start of the region for alignment reasons, set the
		 * size_t length fields back to the "real" length field to a
		 * flag value, so that we can find the real length during free.
		 */
#define	ILLEGAL_SIZE	1
		SH_LIST_REMOVE(elp, links, __data);
		for (sp = rp; (u_int8_t *)--sp >= (u_int8_t *)&elp->links;)
			*sp = ILLEGAL_SIZE;
		if (gbl_bb_berkdb_enable_shalloc_timing)
			bb_shalloc_hit(start_time_us);
		return (0);
	}

	if (gbl_bb_berkdb_enable_shalloc_timing)
		bb_shalloc_hit(start_time_us);
	return (ENOMEM);
}

/*
 * __db_shalloc_free --
 *	Free a shared memory allocation.
 *
 * PUBLIC: void __db_shalloc_free __P((void *, void *));
 */
void
__db_shalloc_free(regionp, ptr)
	void *regionp, *ptr;
{
	struct __data *elp, *lastp, *newp;
	struct __head *hp;
	size_t free_size, *sp;
	int merged;

	if (ptr == NULL)
		return;

	uint64_t start_time_us = 0;

	if (gbl_bb_berkdb_enable_shalloc_timing)
		start_time_us = bb_berkdb_fasttime();

	if (__gbl_use_malloc_for_regions) {
		__db_shalloc_free_malloc(regionp, ptr);
		if (gbl_bb_berkdb_enable_shalloc_timing)
			bb_shalloc_free_hit(start_time_us);
		return;
	}

	/*
	 * Step back over flagged length fields to find the beginning of
	 * the object and its real size.
	 */
	for (sp = (size_t *)ptr; sp[-1] == ILLEGAL_SIZE; --sp);
	ptr = sp;

	newp = (struct __data *)((u_int8_t *)ptr - sizeof(size_t));
	free_size = newp->len;

#ifdef DIAGNOSTIC
	/*
	 * The "real size" includes the guard byte;  it's just the last
	 * byte in the chunk, and the caller never knew it existed.
	 *
	 * Check it to make sure it hasn't been stomped.
	 */
	if (*((u_int8_t *)ptr + free_size - 1) != GUARD_BYTE) {
		/*
		 * Eventually, once we push a DB_ENV handle down to these
		 * routines, we should use the standard output channels.
		 */
		logmsg(LOGMSG_FATAL, "Guard byte incorrect during shared memory free.\n");
		abort();
		/* NOTREACHED */
	}

	/* Trash the returned memory (including guard byte). */
	memset(ptr, CLEAR_BYTE, free_size);
#endif

	/*
	 * Walk the list, looking for where this entry goes.
	 *
	 * We keep the free list sorted by address so that coalescing is
	 * trivial.
	 *
	 * XXX
	 * Probably worth profiling this to see how expensive it is.
	 */
	hp = (struct __head *)regionp;
	for (elp = SH_LIST_FIRST(hp, __data), lastp = NULL;
	    elp != NULL &&(void *)elp < (void *)ptr;
	    lastp = elp, elp = SH_LIST_NEXT(elp, links, __data));

	/*
	 * Elp is either NULL (we reached the end of the list), or the slot
	 * after the one that's being returned.  Lastp is either NULL (we're
	 * returning the first element of the list) or the element before the
	 * one being returned.
	 *
	 * Check for coalescing with the next element.
	 */
	merged = 0;
	if ((u_int8_t *)ptr + free_size == (u_int8_t *)elp) {
		newp->len += elp->len + sizeof(size_t);
		SH_LIST_REMOVE(elp, links, __data);
		if (lastp != NULL)
			SH_LIST_INSERT_AFTER(lastp, newp, links, __data);
		else
			SH_LIST_INSERT_HEAD(hp, newp, links, __data);
		merged = 1;
	}

	/* Check for coalescing with the previous element. */
	if (lastp != NULL && (u_int8_t *)lastp +
	    lastp->len + sizeof(size_t) == (u_int8_t *)newp) {
		lastp->len += newp->len + sizeof(size_t);

		/*
		 * If we have already put the new element into the list take
		 * it back off again because it's just been merged with the
		 * previous element.
		 */
		if (merged)
			SH_LIST_REMOVE(newp, links, __data);
		merged = 1;
	}

	if (!merged) {
		if (lastp == NULL)
			SH_LIST_INSERT_HEAD(hp, newp, links, __data);
		else
			SH_LIST_INSERT_AFTER(lastp, newp, links, __data);
	}

	if (gbl_bb_berkdb_enable_shalloc_timing)
		bb_shalloc_free_hit(start_time_us);
}

/*
 * __db_shsizeof --
 *	Return the size of a shalloc'd piece of memory.
 *
 * !!!
 * Note that this is from an internal standpoint -- it includes not only
 * the size of the memory being used, but also the extra alignment bytes
 * in front and, #ifdef DIAGNOSTIC, the guard byte at the end.
 *
 * PUBLIC: size_t __db_shsizeof __P((void *));
 */
size_t
__db_shsizeof(ptr)
	void *ptr;
{
	struct __data *elp;
	size_t *sp;

	if (__gbl_use_malloc_for_regions)
		return __db_shsizeof_malloc(ptr);

	/*
	 * Step back over flagged length fields to find the beginning of
	 * the object and its real size.
	 */
	for (sp = (size_t *) ptr; sp[-1] == ILLEGAL_SIZE; --sp);

	elp = (struct __data *)((u_int8_t *)sp - sizeof(size_t));
	return (elp->len);
}

/*
 * __db_shalloc_dump --
 *
 * PUBLIC: void __db_shalloc_dump __P((void *, FILE *));
 */
void
__db_shalloc_dump(addr, fp)
	void *addr;
	FILE *fp;
{
	struct __data *elp;

	if (__gbl_use_malloc_for_regions) {
		__db_shalloc_dump_malloc(addr, fp);
		return;
	}

	/* Make it easy to call from the debugger. */
	if (fp == NULL)
		fp = stderr;

	logmsgf(LOGMSG_USER, fp, "%s\nMemory free list\n", DB_LINE);
	for (elp = SH_LIST_FIRST((struct __head *)addr, __data);
	    elp != NULL; elp = SH_LIST_NEXT(elp, links, __data))
		logmsgf(LOGMSG_USER, fp, "%#lx: %lu\t", P_TO_ULONG(elp), (u_long) elp->len);
	logmsgf(LOGMSG_USER, fp, "\n");
}

void
__dbenv_heap_dump(dbenv)
	DB_ENV *dbenv;
{
	HEAP *h;

	if (!__gbl_use_malloc_for_regions)
		return;

	LISTC_FOR_EACH(&dbenv->regions, h, lnk) {
		struct mallinfo info;
		int i;

		logmsg(LOGMSG_USER, "addr %p sz %zu used %10d (%6.02f%%) blocks [%s]\n",
		    h->mem, h->size, h->used,
		    100 * ((double)h->used / (double)h->size), h->description);
		if (__db_mutex_lock(dbenv, h->lock)) {
			__db_err(dbenv, "can't lock mutex\n");
			return;
		}
		info = comdb2_mallinfo(h->msp);
        logmsg(LOGMSG_USER, "  %d heap blocks, %zu malloc blocks:\n", h->blocks,
		    info.hblks);
		for (i = 0; i < 32; i++) {
			if (h->blocksz[i])
				logmsg(LOGMSG_USER, "  %d blocks <= %d\n", h->blocksz[i],
				    1 << i);
		}
		__db_mutex_unlock(dbenv, h->lock);
	}
}
