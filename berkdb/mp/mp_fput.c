/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: mp_fput.c,v 11.48 2003/09/30 17:12:00 sue Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#endif

#include "db_int.h"
#include "dbinc/db_shash.h"
#include "dbinc/log.h"
#include "dbinc/db_page.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"

#include <string.h>
#include "comdb2_atomic.h"

extern int gbl_enable_cache_internal_nodes;

static void __memp_reset_lru __P((DB_ENV *, REGINFO *));

/*
 * __memp_fput_pp --
 *	DB_MPOOLFILE->put pre/post processing.
 *
 * PUBLIC: int __memp_fput_pp __P((DB_MPOOLFILE *, void *, u_int32_t));
 */
int
__memp_fput_pp(dbmfp, pgaddr, flags)
	DB_MPOOLFILE *dbmfp;
	void *pgaddr;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	int ret;

	dbenv = dbmfp->dbenv;
	PANIC_CHECK(dbenv);

	ret = __memp_fput(dbmfp, pgaddr, flags);
	if (IS_ENV_REPLICATED(dbenv))
		__op_rep_exit(dbenv);
	return (ret);
}

/*
 * __memp_fput --
 *	DB_MPOOLFILE->put.
 *
 * PUBLIC: int __memp_fput __P((DB_MPOOLFILE *, void *, u_int32_t));
 */
static inline int
__memp_fput_internal(dbmfp, pgaddr, flags, pgorder)
	DB_MPOOLFILE *dbmfp;
	void *pgaddr;
	u_int32_t flags;
	u_int32_t pgorder;
{
	BH *fbhp, *bhp, *prev;
	DB_ENV *dbenv;
	DB_MPOOL *dbmp;
	DB_MPOOL_HASH *hp;
	MPOOL *c_mp;
	u_int32_t n_cache;
	int adjust, ret, incr_count = 1;

	dbenv = dbmfp->dbenv;
	MPF_ILLEGAL_BEFORE_OPEN(dbmfp, "DB_MPOOLFILE->put");

	dbmp = dbenv->mp_handle;
	/* Validate arguments. */
	if (flags) {
		if ((ret = __db_fchk(dbenv, "memp_fput", flags,
		    DB_MPOOL_CLEAN | DB_MPOOL_DIRTY |DB_MPOOL_DISCARD |
		    DB_MPOOL_NOCACHE | DB_MPOOL_PFPUT)) != 0)
			 return (ret);
		if ((ret = __db_fcchk(dbenv, "memp_fput",
		    flags, DB_MPOOL_CLEAN, DB_MPOOL_DIRTY)) != 0)
			 return (ret);

		if (LF_ISSET(DB_MPOOL_DIRTY) && F_ISSET(dbmfp, MP_READONLY)) {
			__db_err(dbenv,
			    "%s: dirty flag set for readonly file page",
			    __memp_fn(dbmfp));
			return (EACCES);
		}
	}


	/*
	 * If we're mapping the file, there's nothing to do.  Because we can
	 * stop mapping the file at any time, we have to check on each buffer
	 * to see if the address we gave the application was part of the map
	 * region.
	 */
	if (dbmfp->addr != NULL && pgaddr >= dbmfp->addr &&
	    (u_int8_t *)pgaddr <= (u_int8_t *)dbmfp->addr + dbmfp->len)
		return (0);

#ifdef DIAGNOSTIC
	{
		int ret;

		/*
		 * Decrement the per-file pinned buffer count (mapped
		 * pages aren't counted).
		 */
		R_LOCK(dbenv, dbmp->reginfo);
		if (dbmfp->pinref == 0) {
			ret = EINVAL;

			__db_err(dbenv,
			    "%s: more pages returned than retrieved",
			    __memp_fn(dbmfp));
		} else {
			ret = 0;
			--dbmfp->pinref;
		}
		R_UNLOCK(dbenv, dbmp->reginfo);
		if (ret != 0)
			return (ret);
	}
#endif

	/* Convert a page address to a buffer header and hash bucket. */
	bhp = (BH *)((u_int8_t *)pgaddr - SSZA(BH, buf));

	if ((flags & DB_MPOOL_DIRTY)&&dbenv->attr.check_zero_lsn_writes &&
	    (dbenv->open_flags & DB_INIT_TXN)) {
		static const char zerobuf[32] = { 0 };
		u_int8_t *data = bhp->buf;

		if (memcmp(data, zerobuf, sizeof(zerobuf)) == 0) {
			__db_err(dbenv, "%s %s: zero LSN for page %u", __func__,
			    __memp_fn(dbmfp), bhp->pgno);
			if (dbenv->attr.abort_zero_lsn_memp_put)
				abort();
		}
	}

	n_cache = NCACHE(dbmp->reginfo[0].primary, bhp->mpf, bhp->pgno);
	c_mp = dbmp->reginfo[n_cache].primary;
	hp = R_ADDR(&dbmp->reginfo[n_cache], c_mp->htab);
	hp = &hp[NBUCKET(c_mp, bhp->mpf, bhp->pgno)];

	MUTEX_LOCK(dbenv, &hp->hash_mutex);

	/* Set/clear the page bits. */
	if (LF_ISSET(DB_MPOOL_CLEAN) &&
	    F_ISSET(bhp, BH_DIRTY) && !F_ISSET(bhp, BH_DIRTY_CREATE)) {
		DB_ASSERT(hp->hash_page_dirty != 0);
		ATOMIC_ADD32(hp->hash_page_dirty, -1);
		ATOMIC_ADD32(c_mp->stat.st_page_dirty, -1);
		F_CLR(bhp, BH_DIRTY);
	}
	if (LF_ISSET(DB_MPOOL_DIRTY) && !F_ISSET(bhp, BH_DIRTY)) {
		ATOMIC_ADD32(hp->hash_page_dirty, 1);
		ATOMIC_ADD32(c_mp->stat.st_page_dirty, 1);
		F_SET(bhp, BH_DIRTY);
		/* Update first_dirty_lsn when flag goes from CLEAN to DIRTY. */
		if (dbenv->tx_perfect_ckp)
			bhp->first_dirty_tx_begin_lsn = __txn_get_first_dirty_begin_lsn(LSN(pgaddr));
	}
	if (LF_ISSET(DB_MPOOL_DISCARD))
		F_SET(bhp, BH_DISCARD);

	/*
	 * Check for a reference count going to zero.  This can happen if the
	 * application returns a page twice.
	 */
	if (bhp->ref == 0) {
		__db_err(dbenv, "%s: page %lu: unpinned page returned",
		    __memp_fn(dbmfp), (u_long)bhp->pgno);
		MUTEX_UNLOCK(dbenv, &hp->hash_mutex);
		return (EINVAL);
	}

	/* Note the activity so allocation won't decide to quit. */
	++c_mp->put_counter;

	/*
	 * If more than one reference to the page or a reference other than a
	 * thread waiting to flush the buffer to disk, we're done.  Ignore the
	 * discard flags (for now) and leave the buffer's priority alone.
	 */
	if (--bhp->ref > 1 || (bhp->ref == 1 && !F_ISSET(bhp, BH_LOCKED))) {
#ifdef REF_SYNC_TEST
		if (F_ISSET(bhp, BH_LOCKED) && bhp->ref_sync) {
			fprintf(stderr,
	    "> I AM NOT DECREMENTING REF_SYNC BUT I AM DECREMENTING REF\n");
			fprintf(stderr, "> bhp->ref_sync = %d\n",
			    bhp->ref_sync);
		}
#endif
		MUTEX_UNLOCK(dbenv, &hp->hash_mutex);
		return (0);
	}

	/* Update priority values. */
	if (F_ISSET(bhp, BH_DISCARD) ||
	    dbmfp->mfp->priority == MPOOL_PRI_VERY_LOW) {
		bhp->priority = 0;
	}
	/*
	 * Only set the priority to 0 if both DB_MPOOL_NOCACHE is set and
	 * the BH_NOINCR flag is set in the buffer header.  If BH_NOINCR
	 * is set, but DB_MPOOL_NOCACHE is clear, then this is being called
	 * as __bam_c_close is closing a freshly created cursor: we'll need
	 * the buffer in a few cycles, so the priority should stay high. 
	 * If DB_MPOOL_NOCACHE is set, but BH_NOINCR is clear, then the 
	 * buffer was already in the bufferpool at memp_fget time.
	 */
	else if (LF_ISSET(DB_MPOOL_NOCACHE) && F_ISSET(bhp, BH_NOINCR)) {
		bhp->priority = 0;
	} else {
		/*
		 * We don't lock the LRU counter or the stat.st_pages field, if
		 * we get garbage (which won't happen on a 32-bit machine), it
		 * only means a buffer has the wrong priority.
		 */
		bhp->priority = c_mp->lru_count;

		adjust = 0;
		if (dbmfp->mfp->priority != 0)
			adjust =
			    (int)c_mp->stat.st_pages / dbmfp->mfp->priority;
		if (F_ISSET(bhp, BH_DIRTY))
			adjust += c_mp->stat.st_pages / MPOOL_PRI_DIRTY;
#if 0
		/* should I bump priority for prefault pages? */
		if (LF_ISSET(DB_MPOOL_PFPUT)) {
			adjust += c_mp->stat.st_pages / 10;
			incr_count = 0;
		}
#endif

		/* Bump priority for non page-order internal nodes. */
		if (!pgorder && gbl_enable_cache_internal_nodes &&
		    TYPE(pgaddr) == P_IBTREE)
			adjust += c_mp->stat.st_pages / MPOOL_PRI_INTERNAL;

		if (adjust > 0) {
			if (UINT32_T_MAX - bhp->priority >= (u_int32_t)adjust)
				bhp->priority += adjust;
		} else if (adjust < 0) {
			if (bhp->priority > (u_int32_t)-adjust)
				bhp->priority += adjust;
		}
	}

	/* 
	 * If this is a no-increment buffer don't increment lru cache.  
	 * This prevents transient buffers from pushing out others.
	 */
	if (F_ISSET(bhp, BH_NOINCR)) {
		incr_count = 0;
	}

	/*
	 * Buffers on hash buckets are sorted by priority -- move the buffer
	 * to the correct position in the list.
	 */
	if ((fbhp =
		SH_TAILQ_FIRST(&hp->hash_bucket, __bh)) ==
	    SH_TAILQ_LAST(&hp->hash_bucket, HashTab))
		goto done;

	if (fbhp == bhp)
		fbhp = SH_TAILQ_NEXT(fbhp, hq, __bh);
	SH_TAILQ_REMOVE(&hp->hash_bucket, bhp, hq, __bh);

	for (prev = NULL; fbhp != NULL;
	    prev = fbhp, fbhp = SH_TAILQ_NEXT(fbhp, hq, __bh))
		if (fbhp->priority > bhp->priority)
			break;
	if (prev == NULL)
		SH_TAILQ_INSERT_HEAD(&hp->hash_bucket, bhp, hq, __bh);
	else
		SH_TAILQ_INSERT_AFTER(&hp->hash_bucket, prev, bhp, hq, __bh);

done:
	/* Reset the hash bucket's priority. */
	hp->hash_priority = SH_TAILQ_FIRST(&hp->hash_bucket, __bh)->priority;

#ifdef DIAGNOSTIC
	__memp_check_order(hp);
#endif

	/*
	 * The sync code has a separate counter for buffers on which it waits.
	 * It reads that value without holding a lock so we update it as the
	 * last thing we do.  Once that value goes to 0, we won't see another
	 * reference to that buffer being returned to the cache until the sync
	 * code has finished, so we're safe as long as we don't let the value
	 * go to 0 before we finish with the buffer.
	 */
	if (F_ISSET(bhp, BH_LOCKED) && bhp->ref_sync != 0)
		--bhp->ref_sync;

	MUTEX_UNLOCK(dbenv, &hp->hash_mutex);

	/*
	 * On every buffer put we update the buffer generation number and check
	 * for wraparound.  We can't borrow the hash-mutex, since there are 
	 * multiple hash-mutexes so check against a range.
	 */
	if (incr_count) {
		if (++c_mp->lru_count >= (UINT32_T_MAX - 1024))
			__memp_reset_lru(dbenv, dbmp->reginfo);
	}

	return (0);
}

int
__memp_fput(dbmfp, pgaddr, flags)
	DB_MPOOLFILE *dbmfp;
	void *pgaddr;
	u_int32_t flags;
{
	return __memp_fput_internal(dbmfp, pgaddr, flags, 0);
}

// PUBLIC: int __memp_fput_pageorder __P((DB_MPOOLFILE *, void *, u_int32_t));
int
__memp_fput_pageorder(dbmfp, pgaddr, flags)
	DB_MPOOLFILE *dbmfp;
	void *pgaddr;
	u_int32_t flags;
{
	return __memp_fput_internal(dbmfp, pgaddr, flags, 1);
}


/*
 * __memp_reset_lru --
 *	Reset the cache LRU counter.
 */
static void
__memp_reset_lru(dbenv, memreg)
	DB_ENV *dbenv;
	REGINFO *memreg;
{
	BH *bhp;
	DB_MPOOL_HASH *hp;
	MPOOL *c_mp;
	int bucket;

	c_mp = memreg->primary;

	/*
	 * Update the counter so all future allocations will start at the
	 * bottom.
	 */
	c_mp->lru_count -= MPOOL_BASE_DECREMENT;

	/* Adjust the priority of every buffer in the system. */
	for (hp = R_ADDR(memreg, c_mp->htab),
	    bucket = 0; bucket < c_mp->htab_buckets; ++hp, ++bucket) {
		/*
		 * Skip empty buckets.
		 *
		 * We can check for empty buckets before locking as we
		 * only care if the pointer is zero or non-zero.
		 */
		if (SH_TAILQ_FIRST(&hp->hash_bucket, __bh) == NULL)
			continue;

		MUTEX_LOCK(dbenv, &hp->hash_mutex);
		for (bhp = SH_TAILQ_FIRST(&hp->hash_bucket, __bh);
		    bhp != NULL; bhp = SH_TAILQ_NEXT(bhp, hq, __bh))
			if (bhp->priority != UINT32_T_MAX &&
			    bhp->priority > MPOOL_BASE_DECREMENT)
				bhp->priority -= MPOOL_BASE_DECREMENT;
		MUTEX_UNLOCK(dbenv, &hp->hash_mutex);
	}
}
