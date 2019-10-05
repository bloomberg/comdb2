/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: mp_fget.c,v 11.81 2003/09/25 02:15:16 sue Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <alloca.h>
#include <limits.h>
#include <sys/types.h>
#include <limits.h>

#include <string.h>
#include <pthread.h>

#ifdef __sun
   /* for PTHREAD_STACK_MIN on Solaris */
#  define __EXTENSIONS__
#endif

#endif

#include "db_int.h"
#include "dbinc/db_shash.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/db_swap.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/txn.h"

#include "logmsg.h"
#include "locks_wrap.h"
#include "comdb2_atomic.h"
#include "thread_stats.h"


struct bdb_state_tag;
typedef struct bdb_state_tag bdb_state_type;

extern int gbl_prefault_udp;
extern __thread int send_prefault_udp;
extern __thread DB *prefault_dbp;

void udp_prefault_all(bdb_state_type * bdb_state, unsigned int fileid,
    unsigned int pgno);
int send_pg_compact_req(bdb_state_type *bdb_state, int32_t fileid,
	uint32_t size, void *data);

/*
 * __memp_fget_pp --
 *	DB_MPOOLFILE->get pre/post processing.
 *
 * PUBLIC: int __memp_fget_pp
 * PUBLIC:     __P((DB_MPOOLFILE *, db_pgno_t *, u_int32_t, void *));
 */
int
__memp_fget_pp(dbmfp, pgnoaddr, flags, addrp)
	DB_MPOOLFILE *dbmfp;
	db_pgno_t *pgnoaddr;
	u_int32_t flags;
	void *addrp;
{
	DB_ENV *dbenv;
	int rep_check, ret;

	dbenv = dbmfp->dbenv;

	PANIC_CHECK(dbenv);
	MPF_ILLEGAL_BEFORE_OPEN(dbmfp, "DB_MPOOLFILE->get");

	/*
	 * Validate arguments.
	 *
	 * !!!
	 * Don't test for DB_MPOOL_CREATE and DB_MPOOL_NEW flags for readonly
	 * files here, and create non-existent pages in readonly files if the
	 * flags are set, later.  The reason is that the hash access method
	 * wants to get empty pages that don't really exist in readonly files.
	 * The only alternative is for hash to write the last "bucket" all the
	 * time, which we don't want to do because one of our big goals in life
	 * is to keep database files small.  It's sleazy as hell, but we catch
	 * any attempt to actually write the file in memp_fput().
	 */
#define	OKFLAGS		(DB_MPOOL_CREATE | DB_MPOOL_LAST | DB_MPOOL_NEW)
	if (flags != 0) {
		if ((ret = __db_fchk(dbenv, "memp_fget", flags, OKFLAGS)) != 0)
			return (ret);

		switch (flags) {
		case DB_MPOOL_CREATE:
		case DB_MPOOL_LAST:
		case DB_MPOOL_NEW:
		case DB_MPOOL_PROBE:
			break;
		default:
			return (__db_ferr(dbenv, "memp_fget", 1));
		}
	}

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__op_rep_enter(dbenv);
	ret = __memp_fget(dbmfp, pgnoaddr, flags, addrp);
	/*
	 * We only decrement the count in op_rep_exit if the operattion fails.
	 * Otherwise the count will be decremeneted when the page
	 * is no longer pinned in memp_fput.
	 */
	if (ret != 0 && rep_check)
		__op_rep_exit(dbenv);
	return (ret);
}

void (*memp_fget_callback) (void) = 0;
void
__berkdb_register_memp_callback(void (*callback) (void))
{
	memp_fget_callback = callback;
}

/*
 * Bloomberg hack - record a hit to memp_fget, with the time taken
 */
static void
bb_memp_hit(uint64_t start_time_us)
{
	uint64_t time_diff = bb_berkdb_fasttime() - start_time_us;
	struct berkdb_thread_stats *stats;

	stats = bb_berkdb_get_thread_stats();
	stats->n_memp_fgets++;
	stats->memp_fget_time_us += time_diff;

	stats = bb_berkdb_get_process_stats();
	stats->n_memp_fgets++;
	stats->memp_fget_time_us += time_diff;
}


/*
 * Do the fallocate.
 */
static int
__memp_fallocate(dbenv, dbmfp, offset, len)
	DB_ENV *dbenv;
	DB_MPOOLFILE *dbmfp;
	db_pgno_t offset;
	db_pgno_t len;
{
	MPOOLFILE *mfp;

	mfp = dbmfp->mfp;
	return __os_fallocate(dbenv,
	    offset * mfp->stat.st_pagesize,
	    len * mfp->stat.st_pagesize, dbmfp->fhp);
}

/*
 * __mp_falloc_len --
 *	    Plan for the fallocate. Called with the mpool cache 0 
 *     region lock held.
 *
 */
static void
__memp_falloc_len(mfp, offset, len)
	MPOOLFILE *mfp;
	db_pgno_t *offset;
	db_pgno_t *len;
{
	if (mfp->alloc_pgno > mfp->last_pgno) {
		*offset = 0;
		*len = 0;
		return;
	}
	/* 
	 * Handle file creation: since alloc_pgno == 0 could either
	 * mean a 0 or 1 page file, we allocate 2 pages and then start from
	 * alloc_pgno = 1;
	 */
	if (mfp->alloc_pgno == 0) {
		*len = 2;
		*offset = 0;
	} else {
		*len = *offset = mfp->alloc_pgno + 1;
	}

	if (*len > mfp->prealloc_max) {
		*len = mfp->prealloc_max;
	}

	/*
	 * DB_MPOOL_CREATE a sparse file, don't do any preallocation and
	 * let the file system handle it.
	 */
	if (*offset + *len < mfp->last_pgno + 1) {
		*offset = *len = 0;
	} else {
		mfp->alloc_pgno = *offset + *len - 1;
	}
}

u_int64_t gbl_memp_pgreads = 0;

/*
 * __memp_fget_internal --
 *	Get a page from the file.
 *      mark whether it needs to do io
 */
static int
__memp_fget_internal(dbmfp, pgnoaddr, flags, addrp, did_io)
	DB_MPOOLFILE *dbmfp;
	db_pgno_t *pgnoaddr;
	u_int32_t flags;
	void *addrp;
	int *did_io;
{
	enum { FIRST_FOUND, FIRST_MISS, SECOND_FOUND, SECOND_MISS } state;
	BH *alloc_bhp, *bhp;
	DB_ENV *dbenv;
	DB_MPOOL *dbmp;
	DB_MPOOL_HASH *hp;
	MPOOL *c_mp, *mp;
	MPOOLFILE *mfp;
	roff_t mf_offset;
	u_int32_t n_cache, st_hsearch, alloc_flags;
	int b_incr, extending, first, ret, is_recovery_page;
	db_pgno_t falloc_off, falloc_len;

	uint64_t start_time_us = 0;

	if (memp_fget_callback)
		memp_fget_callback();

	if (gbl_bb_berkdb_enable_memp_timing) {
		start_time_us = bb_berkdb_fasttime();
	}

	*(void **)addrp = NULL;

	dbenv = dbmfp->dbenv;
	dbmp = dbenv->mp_handle;

	c_mp = NULL;
	mp = dbmp->reginfo[0].primary;
	mfp = dbmfp->mfp;
	mf_offset = R_OFFSET(dbmp->reginfo, mfp);
	alloc_bhp = bhp = NULL;
	hp = NULL;
	b_incr = extending = ret = is_recovery_page = 0;

	switch (flags) {
	case DB_MPOOL_LAST:
		/* Get the last page number in the file. */
		if (flags == DB_MPOOL_LAST) {
			R_LOCK(dbenv, dbmp->reginfo);
			*pgnoaddr = mfp->last_pgno;
			R_UNLOCK(dbenv, dbmp->reginfo);
		}
		break;
	case DB_MPOOL_NEW:
		/*
		 * If always creating a page, skip the first search
		 * of the hash bucket.
		 */
		if (flags == DB_MPOOL_NEW)
			goto alloc;
		break;
	case DB_MPOOL_CREATE:
	case DB_MPOOL_PROBE:
	case DB_MPOOL_RECP:
	case DB_MPOOL_PFGET:
	case DB_MPOOL_NOCACHE:
	default:
		break;
	}

	/*
	 * If mmap'ing the file and the page is not past the end of the file,
	 * just return a pointer.
	 *
	 * The page may be past the end of the file, so check the page number
	 * argument against the original length of the file.  If we previously
	 * returned pages past the original end of the file, last_pgno will
	 * have been updated to match the "new" end of the file, and checking
	 * against it would return pointers past the end of the mmap'd region.
	 *
	 * If another process has opened the file for writing since we mmap'd
	 * it, we will start playing the game by their rules, i.e. everything
	 * goes through the cache.  All pages previously returned will be safe,
	 * as long as the correct locking protocol was observed.
	 *
	 * We don't discard the map because we don't know when all of the
	 * pages will have been discarded from the process' address space.
	 * It would be possible to do so by reference counting the open
	 * pages from the mmap, but it's unclear to me that it's worth it.
	 */
	if (dbmfp->addr != NULL &&
	    F_ISSET(mfp, MP_CAN_MMAP) && *pgnoaddr <= mfp->orig_last_pgno) {
		*(void **)addrp =
		    R_ADDR(dbmfp, *pgnoaddr * mfp->stat.st_pagesize);
		++mfp->stat.st_map;
		if (gbl_bb_berkdb_enable_memp_timing)
			bb_memp_hit(start_time_us);
		return (0);
	}

hb_search:
	/*
	 * Determine the cache and hash bucket where this page lives and get
	 * local pointers to them.  Reset on each pass through this code, the
	 * page number can change.
	 */
	n_cache = NCACHE(mp, mf_offset, *pgnoaddr);
	c_mp = dbmp->reginfo[n_cache].primary;
	hp = R_ADDR(&dbmp->reginfo[n_cache], c_mp->htab);
	hp = &hp[NBUCKET(c_mp, mf_offset, *pgnoaddr)];

	/* Search the hash chain for the page. */
retry:	st_hsearch = 0;
	MUTEX_LOCK(dbenv, &hp->hash_mutex);
	for (bhp = SH_TAILQ_FIRST(&hp->hash_bucket, __bh);
	    bhp != NULL; bhp = SH_TAILQ_NEXT(bhp, hq, __bh)) {
		++st_hsearch;
		if (bhp->pgno != *pgnoaddr || bhp->mf_offset != mf_offset)
			continue;

		/*
		 * Increment the reference count.  We may discard the hash
		 * bucket lock as we evaluate and/or read the buffer, so we
		 * need to ensure it doesn't move and its contents remain
		 * unchanged.
		 */
		if (bhp->ref == UINT16_T_MAX) {
			__db_err(dbenv,
			    "%s: page %lu: reference count overflow",
			    __memp_fn(dbmfp), (u_long)bhp->pgno);
			ret = EINVAL;
			MUTEX_UNLOCK(dbenv, &hp->hash_mutex);
			goto err;
		}
		++bhp->ref;
		b_incr = 1;

		/*
		 * BH_LOCKED --
		 * I/O is in progress or sync is waiting on the buffer to write
		 * it.  Because we've incremented the buffer reference count,
		 * we know the buffer can't move.  Unlock the bucket lock, wait
		 * for the buffer to become available, reacquire the bucket.
		 */
		for (first = 1; F_ISSET(bhp, BH_LOCKED) &&
		    !F_ISSET(dbenv, DB_ENV_NOLOCKING); first = 0) {
			/*
			 * If someone is trying to sync this buffer and the
			 * buffer is hot, they may never get in.  Give up
			 * and try again.
			 */
			if (!first && bhp->ref_sync != 0) {
				--bhp->ref;
				b_incr = 0;
				MUTEX_UNLOCK(dbenv, &hp->hash_mutex);
				__os_yield(dbenv, 1);
				goto retry;
			}

			MUTEX_UNLOCK(dbenv, &hp->hash_mutex);
			/*
			 * Explicitly yield the processor if not the first pass
			 * through this loop -- if we don't, we might run to the
			 * end of our CPU quantum as we will simply be swapping
			 * between the two locks.
			 */
			if (!first)
				__os_yield(dbenv, 1);

			MUTEX_LOCK(dbenv, &bhp->mutex);
			/* Wait for I/O to finish... */
			MUTEX_UNLOCK(dbenv, &bhp->mutex);
			MUTEX_LOCK(dbenv, &hp->hash_mutex);
		}

		/* Layer violation */
		if (ISINTERNAL(bhp->buf))
			++mfp->stat.st_cache_ihit;
		else if (ISLEAF(bhp->buf))
			++mfp->stat.st_cache_lhit;

		++mfp->stat.st_cache_hit;

        if (LF_ISSET(DB_MPOOL_PFGET))
            ++c_mp->stat.st_page_pf_in_late;

		break;
	}

	/*
	 * Update the hash bucket search statistics -- do now because our next
	 * search may be for a different bucket.
	 */
	++c_mp->stat.st_hash_searches;
	if (st_hsearch > c_mp->stat.st_hash_longest)
		c_mp->stat.st_hash_longest = st_hsearch;
	c_mp->stat.st_hash_examined += st_hsearch;

	/*
	 * There are 4 possible paths to this location:
	 *
	 * FIRST_MISS:
	 *	Didn't find the page in the hash bucket on our first pass:
	 *	bhp == NULL, alloc_bhp == NULL
	 *
	 * FIRST_FOUND:
	 *	Found the page in the hash bucket on our first pass:
	 *	bhp != NULL, alloc_bhp == NULL
	 *
	 * SECOND_FOUND:
	 *	Didn't find the page in the hash bucket on the first pass,
	 *	allocated space, and found the page in the hash bucket on
	 *	our second pass:
	 *	bhp != NULL, alloc_bhp != NULL
	 *
	 * SECOND_MISS:
	 *	Didn't find the page in the hash bucket on the first pass,
	 *	allocated space, and didn't find the page in the hash bucket
	 *	on our second pass:
	 *	bhp == NULL, alloc_bhp != NULL
	 */
	state = bhp == NULL ?
	    (alloc_bhp == NULL ? FIRST_MISS : SECOND_MISS) :
	    (alloc_bhp == NULL ? FIRST_FOUND : SECOND_FOUND);
	switch (state) {
	case FIRST_FOUND:
		/* We found the buffer in our first check -- we're done. */
		break;
	case FIRST_MISS:
		/*
		 * We didn't find the buffer in our first check.  Figure out
		 * if the page exists, and allocate structures so we can add
		 * the page to the buffer pool.
		 */
		MUTEX_UNLOCK(dbenv, &hp->hash_mutex);

alloc:		/*
		 * If DB_MPOOL_NEW is set, we have to allocate a page number.
		 * If neither DB_MPOOL_CREATE or DB_MPOOL_CREATE is set, then
		 * it's an error to try and get a page past the end of file.
		 */
		COMPQUIET(n_cache, 0);

		extending = ret = 0;
		R_LOCK(dbenv, dbmp->reginfo);
		switch (flags) {
		case DB_MPOOL_NEW:
			extending = 1;
			if (mfp->maxpgno != 0 && mfp->last_pgno >= mfp->maxpgno) {
				__db_err(dbenv, "%s: file limited to %lu pages",
				    __memp_fn(dbmfp), (u_long) mfp->maxpgno);
				ret = ENOSPC;
			} else {
				*pgnoaddr = mfp->last_pgno + 1;
			}
			break;
			case DB_MPOOL_CREATE:if (mfp->maxpgno != 0 &&
			    *pgnoaddr > mfp->maxpgno) {
				__db_err(dbenv, "%s: file limited to %lu pages",
				    __memp_fn(dbmfp), (u_long) mfp->maxpgno);
				ret = ENOSPC;
			} else {
				extending = *pgnoaddr > mfp->last_pgno;
			}
			break;
		case DB_MPOOL_PROBE:
			ret = *pgnoaddr > mfp->last_pgno ?
			    DB_PAGE_NOTFOUND : DB_FIRST_MISS;
			break;
			/* Trying to restore from the recovery file */
		case DB_MPOOL_RECP:
			is_recovery_page = 1;
			if (*pgnoaddr > mfp->last_pgno) {
				mfp->last_pgno = *pgnoaddr;
				mfp->orig_last_pgno = *pgnoaddr;
				mfp->alloc_pgno = mfp->orig_last_pgno;
			}
		default:
			ret = *pgnoaddr > mfp->last_pgno ? DB_PAGE_NOTFOUND : 0;
			break;
		}
		R_UNLOCK(dbenv, dbmp->reginfo);
		if (ret != 0)
			goto err;

		/*
		 * !!!
		 * In the DB_MPOOL_NEW code path, mf_offset and n_cache have
		 * not yet been initialized.
		 */
		mf_offset = R_OFFSET(dbmp->reginfo, mfp);
		n_cache = NCACHE(mp, mf_offset, *pgnoaddr);
		c_mp = dbmp->reginfo[n_cache].primary;
		alloc_flags = flags == DB_MPOOL_NOCACHE ? DB_MPOOL_LOWPRI : 0;

		/* Allocate a new buffer header and data space. */
		if ((ret = __memp_alloc_flags(dbmp,
			    &dbmp->reginfo[n_cache], mfp, 0, NULL, alloc_flags,
			    &alloc_bhp)) != 0)
			 goto err;

#ifdef DIAGNOSTIC
		if ((db_alignp_t) alloc_bhp->buf & (sizeof(size_t) - 1)) {
			__db_err(dbenv,
			    "DB_MPOOLFILE->get: buffer data is NOT size_t aligned");
			ret = EINVAL;

			goto err;
		}
#endif
		/*
		 * If we are extending the file, we'll need the region lock
		 * again.
		 */
		if (extending)
			R_LOCK(dbenv, dbmp->reginfo);

		/*
		 * DB_MPOOL_NEW does not guarantee you a page unreferenced by
		 * any other thread of control.  (That guarantee is interesting
		 * for DB_MPOOL_NEW, unlike DB_MPOOL_CREATE, because the caller
		 * did not specify the page number, and so, may reasonably not
		 * have any way to lock the page outside of mpool.) Regardless,
		 * if we allocate the page, and some other thread of control
		 * requests the page by number, we will not detect that and the
		 * thread of control that allocated using DB_MPOOL_NEW may not
		 * have a chance to initialize the page.  (Note: we *could*
		 * detect this case if we set a flag in the buffer header which
		 * guaranteed that no gets of the page would succeed until the
		 * reference count went to 0, that is, until the creating page
		 * put the page.)  What we do guarantee is that if two threads
		 * of control are both doing DB_MPOOL_NEW calls, they won't
		 * collide, that is, they won't both get the same page.
		 *
		 * There's a possibility that another thread allocated the page
		 * we were planning to allocate while we were off doing buffer
		 * allocation.  We can do that by making sure the page number
		 * we were going to use is still available.  If it's not, then
		 * we check to see if the next available page number hashes to
		 * the same mpool region as the old one -- if it does, we can
		 * continue, otherwise, we have to start over.
		 */
		if (flags == DB_MPOOL_NEW && *pgnoaddr != mfp->last_pgno + 1) {
			*pgnoaddr = mfp->last_pgno + 1;
			if (n_cache != NCACHE(mp, mf_offset, *pgnoaddr)) {
				/*
				 * flags == DB_MPOOL_NEW, so extending is set
				 * and we're holding the region locked.
				 */
				R_UNLOCK(dbenv, dbmp->reginfo);

				R_LOCK(dbenv, &dbmp->reginfo[n_cache]);
				__db_shalloc_free(
				    dbmp->reginfo[n_cache].addr, alloc_bhp);
				c_mp->stat.st_pages--;
				R_UNLOCK(dbenv, &dbmp->reginfo[n_cache]);

				alloc_bhp = NULL;
				goto alloc;
			}
		}

		/*
		 * We released the region lock, so another thread might have
		 * extended the file.  Update the last_pgno and initialize
		 * the file, as necessary, if we extended the file.
		 */
		if (extending) {
			if (*pgnoaddr > mfp->last_pgno)
				mfp->last_pgno = *pgnoaddr;

			__memp_falloc_len(mfp, &falloc_off, &falloc_len);

			R_UNLOCK(dbenv, dbmp->reginfo);

			if (falloc_len != 0) {
				(void)__memp_fallocate(dbenv, dbmfp, falloc_off,
				    falloc_len);
			}

			if (ret != 0)
				goto err;
		}
		goto hb_search;
	case SECOND_FOUND:
		/*
		 * We allocated buffer space for the requested page, but then
		 * found the page in the buffer cache on our second check.
		 * That's OK -- we can use the page we found in the pool,
		 * unless DB_MPOOL_NEW is set.
		 *
		 * Free the allocated memory, we no longer need it.  Since we
		 * can't acquire the region lock while holding the hash bucket
		 * lock, we have to release the hash bucket and re-acquire it.
		 * That's OK, because we have the buffer pinned down.
		 */
		MUTEX_UNLOCK(dbenv, &hp->hash_mutex);
		R_LOCK(dbenv, &dbmp->reginfo[n_cache]);
		__db_shalloc_free(dbmp->reginfo[n_cache].addr, alloc_bhp);
		c_mp->stat.st_pages--;
		alloc_bhp = NULL;
		R_UNLOCK(dbenv, &dbmp->reginfo[n_cache]);

		/*
		 * We can't use the page we found in the pool if DB_MPOOL_NEW
		 * was set.  (For details, see the above comment beginning
		 * "DB_MPOOL_NEW does not guarantee you a page unreferenced by
		 * any other thread of control".)  If DB_MPOOL_NEW is set, we
		 * release our pin on this particular buffer, and try to get
		 * another one.
		 */
		if (flags == DB_MPOOL_NEW) {
			--bhp->ref;
			b_incr = 0;
			goto alloc;
		}

		/* We can use the page -- get the bucket lock. */
		MUTEX_LOCK(dbenv, &hp->hash_mutex);
		break;
	case SECOND_MISS:
		/*
		 * We allocated buffer space for the requested page, and found
		 * the page still missing on our second pass through the buffer
		 * cache.  Instantiate the page.
		 */
		bhp = alloc_bhp;
		alloc_bhp = NULL;

		/*
		 * Initialize all the BH and hash bucket fields so we can call
		 * __memp_bhfree if an error occurs.
		 *
		 * Append the buffer to the tail of the bucket list and update
		 * the hash bucket's priority.
		 */
		b_incr = 1;

		memset(bhp, 0, sizeof(BH));
		bhp->ref = 1;
		bhp->priority = UINT32_T_MAX;
		bhp->pgno = *pgnoaddr;
		bhp->mf_offset = mf_offset;
		SH_TAILQ_INSERT_TAIL(&hp->hash_bucket, bhp, hq);

		hp->hash_priority =
		    SH_TAILQ_FIRST(&hp->hash_bucket, __bh)->priority;

		/* If we extended the file, make sure the page is never lost. */
		if (extending) {
			ATOMIC_ADD32(hp->hash_page_dirty, 1);
			ATOMIC_ADD32(c_mp->stat.st_page_dirty, 1);
			F_SET(bhp, BH_DIRTY | BH_DIRTY_CREATE);
			if (dbenv->tx_perfect_ckp) {
				/* Set page first-dirty-LSN to not logged */
				DB_LSN not_logged;
				LSN_NOT_LOGGED(not_logged);
				bhp->first_dirty_tx_begin_lsn =
					__txn_get_first_dirty_begin_lsn(not_logged);
			}
		}

		/* 
		 * Don't increment lru_cache for nocache buffers.
		 */
		if (flags == DB_MPOOL_NOCACHE) {
			F_SET(bhp, BH_NOINCR);
		}

		/*
		 * If we created the page, zero it out.  If we didn't create
		 * the page, read from the backing file.
		 *
		 * !!!
		 * DB_MPOOL_NEW doesn't call the pgin function.
		 *
		 * If DB_MPOOL_CREATE is used, then the application's pgin
		 * function has to be able to handle pages of 0's -- if it
		 * uses DB_MPOOL_NEW, it can detect all of its page creates,
		 * and not bother.
		 *
		 * If we're running in diagnostic mode, smash any bytes on the
		 * page that are unknown quantities for the caller.
		 *
		 * Otherwise, read the page into memory, optionally creating it
		 * if DB_MPOOL_CREATE is set.
		 */
		if (extending) {
			if (mfp->clear_len == 0)
				memset(bhp->buf, 0, mfp->stat.st_pagesize);
			else {
				memset(bhp->buf, 0, mfp->clear_len);
#if defined(DIAGNOSTIC) || defined(UMRW)
				memset(bhp->buf + mfp->clear_len, CLEAR_BYTE,
				    mfp->stat.st_pagesize - mfp->clear_len);
#endif
			}

			if (flags == DB_MPOOL_CREATE && mfp->ftype != 0)
				F_SET(bhp, BH_CALLPGIN);

			++mfp->stat.st_page_create;
		} else {

			F_SET(bhp, BH_TRASH);
			++mfp->stat.st_cache_miss;
			if (LF_ISSET(DB_MPOOL_PFGET)) {
				++c_mp->stat.st_page_pf_in;
                
                
				F_SET(bhp, BH_PREFAULT);
			} else {
				F_CLR(bhp, BH_PREFAULT);
			}

			gbl_memp_pgreads++;
			if (did_io != NULL)
				*did_io = 1;
		}

		/* Increment buffer count referenced by MPOOLFILE. */
		MUTEX_LOCK(dbenv, &mfp->mutex);
		++mfp->block_cnt;
		MUTEX_UNLOCK(dbenv, &mfp->mutex);

		/*
		 * Initialize the mutex.  This is the last initialization step,
		 * because it's the only one that can fail, and everything else
		 * must be set up or we can't jump to the err label because it
		 * will call __memp_bhfree.
		 */
		if ((ret = __db_mutex_setup(dbenv,
		    &dbmp->reginfo[n_cache], &bhp->mutex, 0)) != 0)
			goto err;
	}

	DB_ASSERT(bhp->ref != 0);

	/*
	 * If we're the only reference, update buffer and bucket priorities.
	 * We may be about to release the hash bucket lock, and everything
	 * should be correct, first.  (We've already done this if we created
	 * the buffer, so there is no need to do it again.)
	 */

	/* from patch */
	if (state != SECOND_MISS && bhp->ref == 1) {
		bhp->priority = UINT32_T_MAX;
		if (SH_TAILQ_FIRST(&hp->hash_bucket, __bh) !=
		    SH_TAILQ_LAST(&hp->hash_bucket, HashTab)) {
			SH_TAILQ_REMOVE(&hp->hash_bucket, bhp, hq, __bh);
			SH_TAILQ_INSERT_TAIL(&hp->hash_bucket, bhp, hq);
		}
		hp->hash_priority =
		    SH_TAILQ_FIRST(&hp->hash_bucket, __bh)->priority;
	}
#if 0
	if (state != SECOND_MISS && bhp->ref == 1) {
		bhp->priority = UINT32_T_MAX;
		SH_TAILQ_REMOVE(&hp->hash_bucket, bhp, hq, __bh);
		SH_TAILQ_INSERT_TAIL(&hp->hash_bucket, bhp, hq);
		hp->hash_priority =
		    SH_TAILQ_FIRST(&hp->hash_bucket, __bh)->priority;
	}
#endif


	/*
	 * BH_TRASH --
	 * The buffer we found may need to be filled from the disk.
	 *
	 * It's possible for the read function to fail, which means we fail as
	 * well.  Note, the __memp_pgread() function discards and reacquires
	 * the hash lock, so the buffer must be pinned down so that it cannot
	 * move and its contents are unchanged.  Discard the buffer on failure
	 * unless another thread is waiting on our I/O to complete.  It's OK to
	 * leave the buffer around, as the waiting thread will see the BH_TRASH
	 * flag set, and will also attempt to discard it.  If there's a waiter,
	 * we need to decrement our reference count.
	 */


	if (F_ISSET(bhp, BH_TRASH)) {
		if ((ret = __memp_pgread(dbmfp,
				hp, bhp,
			    LF_ISSET(DB_MPOOL_CREATE) ? 1 : 0,
			    is_recovery_page)) != 0)
			 goto err;

		if (state == SECOND_MISS) {
			if (ISINTERNAL(bhp->buf))
				++mfp->stat.st_cache_imiss;
			else if (ISLEAF(bhp->buf))
				++mfp->stat.st_cache_lmiss;
		}
	}

	/*
	 * BH_CALLPGIN --
	 * The buffer was processed for being written to disk, and now has
	 * to be re-converted for use.
	 */
	if (F_ISSET(bhp, BH_CALLPGIN)) {
		if ((ret = __memp_pg(dbmfp, bhp, 1)) != 0)
			goto err;
		F_CLR(bhp, BH_CALLPGIN);
	}

	MUTEX_UNLOCK(dbenv, &hp->hash_mutex);

#ifdef DIAGNOSTIC
	/* Update the file's pinned reference count. */
	R_LOCK(dbenv, dbmp->reginfo);
	++dbmfp->pinref;
	R_UNLOCK(dbenv, dbmp->reginfo);

	/*
	 * We want to switch threads as often as possible, and at awkward
	 * times.  Yield every time we get a new page to ensure contention.
	 */
	if (F_ISSET(dbenv, DB_ENV_YIELDCPU))
		__os_yield(dbenv, 1);
#endif

	*(void **)addrp = bhp->buf;
	if (bhp->fget_count < UINT_MAX)
			bhp->fget_count++;

	if (gbl_bb_berkdb_enable_memp_timing)
		bb_memp_hit(start_time_us);
	return (0);

err:	/*
	 * Discard our reference.  If we're the only reference, discard the
	 * the buffer entirely.  If we held a reference to a buffer, we are
	 * also still holding the hash bucket mutex.
	 */
	if (b_incr) {
		if (bhp->ref == 1)
			(void)__memp_bhfree(dbmp, hp, bhp, 1);
		else {
			--bhp->ref;
			MUTEX_UNLOCK(dbenv, &hp->hash_mutex);
		}
	}

	/* If alloc_bhp is set, free the memory. */
	if (alloc_bhp != NULL) {
		R_LOCK(dbenv, &dbmp->reginfo[n_cache]);
		__db_shalloc_free(dbmp->reginfo[n_cache].addr, alloc_bhp);
		c_mp->stat.st_pages--;
		R_UNLOCK(dbenv, &dbmp->reginfo[n_cache]);
	}

	if (gbl_bb_berkdb_enable_memp_timing)
		bb_memp_hit(start_time_us);
	return (ret);
}

/*
 * __memp_read_recovery_pages --
 *  Pull all of the recovery pages into the bufferpool.  This is done during
 *  recovery.  If any of these pages is malformed, the recovery page logic will
 *  correct it.
 * PUBLIC: int __memp_read_recovery_pages __P((DB_MPOOLFILE *));
 */
int
__memp_read_recovery_pages(dbmfp)
	DB_MPOOLFILE *dbmfp;
{
	DB_ENV *dbenv;
	MPOOLFILE *mfp;
	DB_MPOOL *dbmp;
	DB_MPREG *mpreg;
	DB_PGINFO duminfo = { 0 }, *pginfo;
	PAGE *pagep;
	int ret, free_buf, ftype, i;
	void *fpage;
	size_t nr, pagesize;
	db_pgno_t inpg;

	dbenv = dbmfp->dbenv;
	mfp = dbmfp->mfp;
	pagesize = mfp->stat.st_pagesize;
	dbmp = dbenv->mp_handle;
	free_buf = 0;
	pginfo = &duminfo;
	ftype = mfp->ftype;

	/* Error out here if there is no recovery page file. */
	if (NULL == dbmfp->recp)
		return 0;

	/* Create a temporary pagecache. */
	if (pagesize <= 4096)
		pagep = (PAGE *)alloca(pagesize);
	else {
		if ((ret = __os_malloc(dbenv, pagesize, &pagep)) != 0)
			return (ret);
		free_buf = 1;
	}

	MUTEX_THREAD_LOCK(dbenv, dbmp->mutexp);

	/* Get the page-cookie for data format. */
	for (mpreg = LIST_FIRST(&dbmp->dbregq);
	    mpreg != NULL; mpreg = LIST_NEXT(mpreg, q)) {
		if (ftype != mpreg->ftype)
			continue;
		if (mfp->pgcookie_len > 0) {
			pginfo = (DB_PGINFO *)
			    R_ADDR(dbmp->reginfo, mfp->pgcookie_off);
		}
		break;
	}

	MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);


	/* Scan in each of the recovery pages. */
	for (i = 0; i <= dbenv->mp_recovery_pages; i++) {
		/* Lock out other threads */
		Pthread_mutex_lock(&dbmfp->recp_lk_array[i]);

		/* Read page. */
		if ((ret = __os_io(dbenv, DB_IO_READ,
			    dbmfp->recp, i, pagesize,
			    (u_int8_t *)pagep, &nr)) != 0) {
			Pthread_mutex_unlock(&dbmfp->recp_lk_array[i]);
			break;
		}

		Pthread_mutex_unlock(&dbmfp->recp_lk_array[i]);

		/* Verify length. */
		if (nr < pagesize)
			break;

		/* Check if this is the correct page. */
		if (F_ISSET(pginfo, DB_AM_SWAP)) {
			P_32_COPYSWAP(&PGNO(pagep), &inpg);
		} else {
			inpg = PGNO(pagep);
		}

		/* Read this page into the bufferpool. */
		if (0 == __memp_fget_internal(dbmfp, &inpg,
			DB_MPOOL_RECP, &fpage, NULL))
			 __memp_fput(dbmfp, fpage, 0);
	}

	if (free_buf)
		free(pagep);

	return 0;
}



/* page compact runtines START { */

/* variables */
extern double gbl_pg_compact_thresh;
extern int gbl_pg_compact_latency_ms;
/* Thread local flag to disable page compaction.
   Currently only table rebuild sets it. */
pthread_key_t no_pgcompact;

/* data structures */
struct spg {
	DB_ENV *dbenv;
	int32_t id;
	u_int8_t  ufid[DB_FILE_ID_LEN];
	db_pgno_t pgno;
	double sparseness;
};

static struct {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	pthread_t tid;
	pthread_attr_t attrs;
	struct spg list[16];
	int wait;
} spgs = {
	PTHREAD_MUTEX_INITIALIZER,
	PTHREAD_COND_INITIALIZER
	/* 0 */
};

/* functions */

/*
 * __memp_send_sparse_page_thread --
 *  Pick the least filled page. Send it to master for compaction.
 */
static void*
__memp_send_sparse_page_thread(_)
	void *_;
{
	DB_ENV *dbenv;
	int32_t fileid;
	DBT dbt;
	db_pgno_t pgno;
	bdb_state_type *bdb_state;
	struct spg ent;
	int ii;
	u_int8_t *ufid;

	ii = sizeof(spgs.list) / sizeof(spgs.list[0]) - 1;

	while (1) {
		{
			Pthread_mutex_lock(&spgs.lock);
			while (spgs.list[ii].sparseness == 0) {
				/* no entry, cond wait */
				spgs.wait = 1;
				Pthread_cond_wait(&spgs.cond, &spgs.lock);
			}

			spgs.wait = 0;
			ent = spgs.list[ii];
			memmove(&spgs.list[1], spgs.list, sizeof(struct spg) * ii);
			memset(spgs.list, 0, sizeof(struct spg));
			Pthread_mutex_unlock(&spgs.lock);
		}

		dbenv = ent.dbenv;
		fileid = ent.id;
		ufid = ent.ufid;
		pgno = ent.pgno;
		bdb_state = (bdb_state_type *)dbenv->app_private;

		if (fileid == DB_LOGFILEID_INVALID) {
			if (!__dbreg_ufid_exists(dbenv, ufid, &fileid))
				continue;
		}

		memset(&dbt, 0, sizeof(dbt));
		if (__dbenv_ispgcompactible(dbenv, fileid,
                    pgno, &dbt, gbl_pg_compact_thresh) == 0) {
			send_pg_compact_req(bdb_state, fileid, dbt.size, dbt.data);
			if (gbl_pg_compact_latency_ms > 0)
				usleep(gbl_pg_compact_latency_ms * 1000LL);
		}
		__os_free(dbenv, dbt.data);
	}

	return NULL;
}

/*
 * __memp_add_sparse_page --
 *  Add a sparse page to the fixed-size array. If the array is full,
 *  replace the page with the least fill factor. The lost page, however,
 *  will be found again eventually.
 */
static void
__memp_add_sparse_page(dbenv, id, ufid, pgno, sparseness)
	DB_ENV *dbenv;
	int32_t id;
	u_int8_t *ufid;
	db_pgno_t pgno;
	double sparseness;
{
	int ii, ofs, len;
	struct spg ent;
	if (sparseness < spgs.list[0].sparseness)
		return;

	/* We try-lock the list mutex, since we don't want to block the caller. 
	   If we fail to add the page, leave it. */

	if (pthread_mutex_trylock(&spgs.lock) != 0)
		return;

	len = sizeof(spgs.list) / sizeof(spgs.list[0]);
	ofs = len - 1;
	for (ii = -1; ii != ofs; ++ii)
		if (sparseness < spgs.list[ii + 1].sparseness)
			break;

	if (ii == -1)
		return;

	ent.dbenv = dbenv;
	ent.id = id;
	memcpy(ent.ufid, ufid, DB_FILE_ID_LEN);
	ent.pgno = pgno;
	ent.sparseness = sparseness;

	/* make room and then insert */
	memmove(spgs.list, &spgs.list[1], sizeof(struct spg) * ii);
	spgs.list[ii] = ent;

	if (spgs.wait)
		Pthread_cond_signal(&spgs.cond);

	Pthread_mutex_unlock(&spgs.lock);
}

/*
 * __memp_init_pgcompact_routines --
 *  Initialize data and thread
 */
void
__memp_init_pgcompact_routines(void)
{
	if (gbl_pg_compact_thresh <= 0)
		return;

	Pthread_attr_init(&spgs.attrs);

#ifdef PTHREAD_STACK_MIN
	/* ~128kB stack size */
	Pthread_attr_setstacksize(&spgs.attrs, (PTHREAD_STACK_MIN + 0x20000));
#endif

	if (pthread_attr_setdetachstate(&spgs.attrs, PTHREAD_CREATE_DETACHED) != 0) {
		logmsgperror("pthread_attr_setdetachstate");
		abort();
	}

	if (pthread_create(&spgs.tid, &spgs.attrs, __memp_send_sparse_page_thread, NULL) != 0) {
		logmsgperror("pthread_create");
		abort();
	}

	Pthread_key_create(&no_pgcompact, NULL);
        Pthread_attr_destroy(&spgs.attrs);
}
/* } page compact runtines END */

#include <time.h>

int __slow_memp_fget_ns = 0;

/*
 * __memp_fget --
 *	Get a page from the file.
 *
 * PUBLIC: int __memp_fget
 * PUBLIC:     __P((DB_MPOOLFILE *, db_pgno_t *, u_int32_t, void *));
 */
int
__memp_fget(dbmfp, pgnoaddr, flags, addrp)
	DB_MPOOLFILE *dbmfp;
	db_pgno_t *pgnoaddr;
	u_int32_t flags;
	void *addrp;
{
	int ret;
	struct timespec s, rem;
	int rc;
	int did_io = 0;
	PAGE *h;
	double fullsz, sparseness;

	if (LF_ISSET(DB_MPOOL_PFGET) && !F_ISSET(dbmfp, MP_OPEN_CALLED))
		return (EINVAL);

	if (__slow_memp_fget_ns) {
		s.tv_sec = 0;
		s.tv_nsec = __slow_memp_fget_ns;
		rc = nanosleep(&s, &rem);
		while (rc == -1 && errno == -EINTR) {
			s = rem;
			rc = nanosleep(&s, &rem);
		}
	}

	ret = __memp_fget_internal(dbmfp, pgnoaddr, flags, addrp, &did_io);
	if (ret || !did_io || !prefault_dbp || !prefault_dbp->log_filename)
		goto out;

	h = *(PAGE **)addrp;
	if (TYPE(h) != P_LBTREE) 
		goto out;

	if(send_prefault_udp) {
		udp_prefault_all((bdb_state_type *) dbmfp->dbenv->app_private,
				(u_int32_t)prefault_dbp->log_filename->id,
				(u_int32_t)*pgnoaddr);
	}

	int pgcomp_enabled = (gbl_pg_compact_thresh > 0)
		&& (pthread_getspecific(no_pgcompact) != (void*)1)
		&& LF_ISSET(DB_MPOOL_COMPACT);

	if (pgcomp_enabled) {
		/* Memory pool knows the pages. This is why we choose to
		   initiate page compact requests here. When a fresh page
		   is fetched, fget() examines its free space, add the page
		   to a fixed size array sorted by page free space if the page
		   is considered less full.  A dedicated thread, picks up
		   entries from the list, does a comprehensive page check,
		   and sends compact requests for qulified pages.

		   The 1st check only looks at page fill ratio.
		   We tend to keep 1st check as simple as possible because
		   we don't want to block the caller too long. A complete
		   check is asynchronously done in the dedicated send thread. */

		fullsz = prefault_dbp->pgsize - SIZEOF_PAGE;
		sparseness = P_FREESPACE(prefault_dbp, h) / fullsz;
		if (sparseness >= (1 - gbl_pg_compact_thresh)) {
			/* We have a pre-qualified page that might need to have data relocated
			   to it. Add the page. */
			__memp_add_sparse_page(dbmfp->dbenv,
					prefault_dbp->log_filename->id,
					prefault_dbp->fileid,
					*pgnoaddr, sparseness);
		}
	}
out:

	return ret;
}
