/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: mp_alloc.c,v 11.40 2003/07/03 02:24:34 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_shash.h"
#include "dbinc/mp.h"

#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h>

#include <pthread.h>
#include "ctrace.h"
#include <btree/bt_cache.h>
#include "dbinc/btree.h"

#include "util.h"
#include <cdb2_constants.h>
#include "logmsg.h"
#include "locks_wrap.h"

typedef struct {
	DB_MPOOL_HASH *bucket;
	u_int32_t priority;
} HS;

static void __memp_bad_buffer __P((DB_MPOOL_HASH *));

// PUBLIC: int __memp_dump_bufferpool_info __P((DB_ENV *, FILE *));
int
__memp_dump_bufferpool_info(dbenv, f)
	DB_ENV *dbenv;
	FILE *f;
{
	BH *bhp;
	DB_MPOOL_HASH *dbht, *hp;
	DB_MUTEX *mutexp;
	REGINFO *memreg;
	MPOOL *mp, *c_mp;
	DB_MPOOL *dbmp;
	int count, n_cache;
	u_int64_t bufcnt;

	dbmp = dbenv->mp_handle;
	dbenv = dbmp->dbenv;
	mp = dbmp->reginfo[0].primary;

	bufcnt = 0;

	/* Print format of buffers. */
	logmsgf(LOGMSG_USER, f, "FORMAT = ( MPOOLOFFSET : PAGENUMBER : PRIORITY )\n");

	for (n_cache = 0; n_cache < mp->nreg; n_cache++) {
		memreg = &dbmp->reginfo[n_cache];
		c_mp = dbmp->reginfo[n_cache].primary;
		dbht = R_ADDR(memreg, c_mp->htab);
		logmsgf(LOGMSG_USER, f, "CACHE %d AT %p\n", n_cache, c_mp);

		for (count = 0; count < c_mp->htab_buckets; count++) {
			hp = &dbht[count];


			mutexp = &hp->hash_mutex;
			MUTEX_LOCK(dbenv, mutexp);

			if ((bhp =
				SH_TAILQ_FIRST(&hp->hash_bucket,
				    __bh)) == NULL) {
				/*fprintf(f, " (empty)\n"); */
				MUTEX_UNLOCK(dbenv, mutexp);
				continue;
			}

			logmsgf(LOGMSG_USER, f, "HASH-PTR %p :", hp);

			do {
				bufcnt++;
				logmsgf(LOGMSG_USER, f, " (%d:%d:%d)", (int)bhp->mf_offset,
				    bhp->pgno, bhp->priority);
				bhp = SH_TAILQ_NEXT(bhp, hq, __bh);
			}
			while (bhp);

			logmsgf(LOGMSG_USER, f, "\n");

			MUTEX_UNLOCK(dbenv, mutexp);
		}
		logmsgf(LOGMSG_USER, f, "LRU_COUNT = %d\n", c_mp->lru_count);
		logmsgf(LOGMSG_USER, f, "\n");
	}
	logmsgf(LOGMSG_USER, f, "BUFCNT = %"PRIu64"\n", bufcnt);
	return 0;
}


int __gbl_max_mpalloc_sleeptime = 60;

extern char gbl_dbname[MAX_DBNAME_LENGTH];

/* copy and paste from bdb/info.c - don't want to call back into bdb */
static void dump_page_stats(DB_ENV *dbenv) {
	DB_TXN_STAT *txn_stats;
	DB_TXN_ACTIVE *active;
	int i;
	FILE *out;
	char *fname;
	struct tm tm;
	time_t now = time(NULL);

	fname = comdb2_location("debug", "%s.cachedump", gbl_dbname);
	out = fopen(fname, "a");
	logmsg(LOGMSG_USER, "writing cache stats to %s\n", fname);
	localtime_r(&now, &tm);
	logmsgf(LOGMSG_USER, out, "%02d/%02d/%04d %02d:%02d:%02d\n", tm.tm_mon, tm.tm_mday, tm.tm_year + 1900,
		tm.tm_hour, tm.tm_min, tm.tm_sec);

	DB_MPOOL_STAT *mpool_stats;
	DB_MPOOL_FSTAT **fsp;
   
	dbenv->txn_stat(dbenv, &txn_stats, 0);
   
	logmsgf(LOGMSG_USER, out, "st_last_ckp: %u:%u\n",  txn_stats->st_last_ckp.file, txn_stats->st_last_ckp.offset);
	logmsgf(LOGMSG_USER, out, "st_time_ckp: %lu\n", txn_stats->st_time_ckp);
	logmsgf(LOGMSG_USER, out, "st_last_txnid: %u\n", txn_stats->st_last_txnid);
	logmsgf(LOGMSG_USER, out, "st_maxtxns: %u\n", txn_stats->st_maxtxns);
	logmsgf(LOGMSG_USER, out, "st_nactive: %u\n", txn_stats->st_nactive);
	logmsgf(LOGMSG_USER, out, "st_maxnactive: %u\n", txn_stats->st_maxnactive);
	logmsgf(LOGMSG_USER, out, "st_nbegins: %u\n", txn_stats->st_nbegins);
	logmsgf(LOGMSG_USER, out, "st_naborts: %u\n", txn_stats->st_naborts);
	logmsgf(LOGMSG_USER, out, "st_ncommits: %u\n", txn_stats->st_ncommits);
	logmsgf(LOGMSG_USER, out, "st_nrestores: %u\n", txn_stats->st_nrestores);
	logmsgf(LOGMSG_USER, out, "st_regsize: %u\n", txn_stats->st_regsize);
	logmsgf(LOGMSG_USER, out, "st_region_wait: %u\n", txn_stats->st_region_wait);
	logmsgf(LOGMSG_USER, out, "st_region_nowait: %u\n", txn_stats->st_region_nowait); 
   
	active = txn_stats->st_txnarray;
	for (i = 0; i < txn_stats->st_nactive; i++)
	{
		logmsgf(LOGMSG_USER, out, "active transactions:\n");
		logmsgf(LOGMSG_USER, out, " %d %d %u:%u\n", active->txnid, active->parentid,
			active->lsn.file, active->lsn.offset);
		active++;
	}
   
	free(txn_stats);


	dbenv->memp_dump_region(dbenv, "A", out);
	dbenv->memp_stat(dbenv, &mpool_stats, &fsp, 0);

	logmsgf(LOGMSG_USER, out, "st_gbytes: %"PRId64"\n", mpool_stats->st_gbytes);
	logmsgf(LOGMSG_USER, out, "st_bytes: %"PRId64"\n", mpool_stats->st_bytes);
	logmsgf(LOGMSG_USER, out, "st_ncache: %"PRId64"\n", mpool_stats->st_ncache);
	logmsgf(LOGMSG_USER, out, "st_regsize: %"PRId64"\n", mpool_stats->st_regsize);
	logmsgf(LOGMSG_USER, out, "st_map: %"PRId64"\n", mpool_stats->st_map);
	logmsgf(LOGMSG_USER, out, "st_cache_hit: %"PRId64"\n", mpool_stats->st_cache_hit);
	logmsgf(LOGMSG_USER, out, "st_cache_miss: %"PRId64"\n", mpool_stats->st_cache_miss);
	logmsgf(LOGMSG_USER, out, "st_page_in: %"PRId64"\n", mpool_stats->st_page_in);
	logmsgf(LOGMSG_USER, out, "st_page_out: %"PRId64"\n", mpool_stats->st_page_out);
	logmsgf(LOGMSG_USER, out, "st_ro_merges: %"PRId64"\n", mpool_stats->st_ro_merges);
	logmsgf(LOGMSG_USER, out, "st_rw_merges: %"PRId64"\n", mpool_stats->st_rw_merges);
	logmsgf(LOGMSG_USER, out, "st_ro_evict: %"PRId64"\n", mpool_stats->st_ro_evict);
	logmsgf(LOGMSG_USER, out, "st_rw_evict: %"PRId64"\n", mpool_stats->st_rw_evict);
	logmsgf(LOGMSG_USER, out, "st_ro_levict: %"PRId64"\n", mpool_stats->st_ro_levict);
	logmsgf(LOGMSG_USER, out, "st_rw_levict: %"PRId64"\n", mpool_stats->st_rw_levict);
	logmsgf(LOGMSG_USER, out, "st_pf_evict: %"PRId64"\n", mpool_stats->st_pf_evict);
	logmsgf(LOGMSG_USER, out, "st_rw_evict_skip: %"PRId64"\n", mpool_stats->st_rw_evict_skip);
	logmsgf(LOGMSG_USER, out, "st_page_trickle: %"PRId64"\n", mpool_stats->st_page_trickle);
	logmsgf(LOGMSG_USER, out, "st_pages: %"PRId64"\n", mpool_stats->st_pages);
	logmsgf(LOGMSG_USER, out, "st_page_clean: %"PRId64"\n", mpool_stats->st_page_clean);
	logmsgf(LOGMSG_USER, out, "st_page_dirty: %"PRId32"\n", mpool_stats->st_page_dirty);
	logmsgf(LOGMSG_USER, out, "st_hash_buckets: %"PRId64"\n", mpool_stats->st_hash_buckets);
	logmsgf(LOGMSG_USER, out, "st_hash_searches: %"PRId64"\n", mpool_stats->st_hash_searches);
	logmsgf(LOGMSG_USER, out, "st_hash_longest: %"PRId64"\n", mpool_stats->st_hash_longest);
	logmsgf(LOGMSG_USER, out, "st_hash_examined: %"PRId64"\n", mpool_stats->st_hash_examined);
	logmsgf(LOGMSG_USER, out, "st_hash_nowait: %"PRId64"\n", mpool_stats->st_hash_nowait);
	logmsgf(LOGMSG_USER, out, "st_hash_wait: %"PRId64"\n", mpool_stats->st_hash_wait);
	logmsgf(LOGMSG_USER, out, "st_hash_max_wait: %"PRId64"\n", mpool_stats->st_hash_max_wait);
	logmsgf(LOGMSG_USER, out, "st_hash_region_wait: %"PRId64"\n", mpool_stats->st_region_wait);
	logmsgf(LOGMSG_USER, out, "st_hash_region_nowait: %"PRId64"\n",
		mpool_stats->st_region_nowait);
	logmsgf(LOGMSG_USER, out, "st_alloc: %"PRId64"\n", mpool_stats->st_alloc);
	logmsgf(LOGMSG_USER, out, "st_alloc_buckets: %"PRId64"\n", mpool_stats->st_alloc_buckets);
	logmsgf(LOGMSG_USER, out, "st_alloc_max_buckets: %"PRId64"\n", 
		mpool_stats->st_alloc_max_buckets);
	logmsgf(LOGMSG_USER, out, "st_alloc_pages: %"PRId64"\n", mpool_stats->st_alloc_pages);
	logmsgf(LOGMSG_USER, out, "st_alloc_max_pages: %"PRId64"\n",
		mpool_stats->st_alloc_max_pages);

	for(; fsp != NULL && *fsp != NULL; ++fsp)
	{
		logmsgf(LOGMSG_USER, out, "Pool file [%s]:-\n", (*fsp)->file_name);
		logmsgf(LOGMSG_USER, out, "  st_pagesize   : %zu\n", (*fsp)->st_pagesize);
		logmsgf(LOGMSG_USER, out, "  st_map        : %"PRId64"\n", (*fsp)->st_map);
		logmsgf(LOGMSG_USER, out, "  st_cache_hit  : %"PRId64"\n", (*fsp)->st_cache_hit);
		logmsgf(LOGMSG_USER, out, "  st_cache_miss : %"PRId64"\n", (*fsp)->st_cache_miss);
		logmsgf(LOGMSG_USER, out, "  st_page_create: %"PRId64"\n", (*fsp)->st_page_create);
		logmsgf(LOGMSG_USER, out, "  st_page_in    : %"PRId64"\n", (*fsp)->st_page_in);
		logmsgf(LOGMSG_USER, out, "  st_page_out   : %"PRId64"\n", (*fsp)->st_page_out);
		logmsgf(LOGMSG_USER, out, "  st_ro_merges  : %"PRId64"\n", (*fsp)->st_ro_merges);
		logmsgf(LOGMSG_USER, out, "  st_rw_merges  : %"PRId64"\n", (*fsp)->st_rw_merges);
	}

	free(mpool_stats);

	fflush(out);
	free(fname);
	fclose(out);
}


int gbl_debug_memp_alloc_size = 0;
static pthread_mutex_t dump_once_lk = PTHREAD_MUTEX_INITIALIZER;
/*
 * PUBLIC: int __memp_alloc_flags __P((DB_MPOOL *, REGINFO *,
 * PUBLIC:     MPOOLFILE *, size_t, roff_t *, u_int32_t, void *));
 */
int
__memp_alloc_flags(dbmp, memreg, mfp, len, offsetp, flags, retp)
	DB_MPOOL *dbmp;
	REGINFO *memreg;
	MPOOLFILE *mfp;
	size_t len;
	roff_t *offsetp;
	u_int32_t flags;
	void *retp;
{
	BH *bhp;
	DB_ENV *dbenv;
	DB_MPOOL_HASH *dbht, *hp, *hp_end, *hp_tmp;
	DB_MUTEX *mutexp;
	MPOOL *c_mp;
	MPOOLFILE *bh_mfp;
	size_t freed_space;
	u_int32_t buckets, buffers, high_priority, priority, put_counter;
	u_int32_t total_buckets;
	int aggressive, giveup, ret;
	void *p;
	int sleeptime = 0;

	if (gbl_debug_memp_alloc_size &&
	    ((len > (100 * 1024)) || (mfp && len > mfp->stat.st_pagesize))) {
		logmsg(LOGMSG_USER, "allocating %zu bytes (pgsize %zu)\n", len,
		       (mfp ? mfp->stat.st_pagesize : 0));
	}

	dbenv = dbmp->dbenv;
	c_mp = memreg->primary;
	dbht = R_ADDR(memreg, c_mp->htab);
	hp_end = &dbht[c_mp->htab_buckets];

	buckets = buffers = put_counter = total_buckets = 0;
	aggressive = giveup = 0;
	hp_tmp = NULL;

	c_mp->stat.st_alloc++;

	/*
	 * If we're allocating a buffer, and the one we're discarding is the
	 * same size, we don't want to waste the time to re-integrate it into
	 * the shared memory free list.  If the DB_MPOOLFILE argument isn't
	 * NULL, we'll compare the underlying page sizes of the two buffers
	 * before free-ing and re-allocating buffers.
	 */
	if (mfp != NULL)
		len = (sizeof(BH) - sizeof(u_int8_t)) + mfp->stat.st_pagesize;

	R_LOCK(dbenv, memreg);
	/*
	 * Anything newer than 1/10th of the buffer pool is ignored during
	 * allocation (unless allocation starts failing).
	 */
	high_priority = c_mp->lru_count - c_mp->stat.st_pages / 10;

	/*
	 * Ignore everything but the lowest 10% if this is a low-priority 
	 * allocation (again, unless allocation starts failing).
	 */
	/*
	 * if (LF_ISSET(DB_MPOOL_LOWPRI)) {
	 * high_priority = c_mp->lru_count - (c_mp->stat.st_pages * 9) / 10;
	 * }
	 */
	if (LF_ISSET(DB_MPOOL_LOWPRI)) {
		high_priority =
		    c_mp->lru_count - (c_mp->stat.st_pages * 99) / 100;
	}


	/*
	 * First we try to allocate from free memory.  If that fails, scan the
	 * buffer pool to find buffers with low priorities.  We consider small
	 * sets of hash buckets each time to limit the amount of work needing
	 * to be done.  This approximates LRU, but not very well.  We either
	 * find a buffer of the same size to use, or we will free 3 times what
	 * we need in the hopes it will coalesce into a contiguous chunk of the
	 * right size.  In the latter case we branch back here and try again.
	 */
alloc:	if ((ret = __db_shalloc(memreg->addr, len, MUTEX_ALIGN, &p)) == 0) {
		if (mfp != NULL)
			c_mp->stat.st_pages++;
		R_UNLOCK(dbenv, memreg);

found:		if (offsetp != NULL)
			*offsetp = R_OFFSET(memreg, p);
		*(void **)retp = p;

		/*
		 * Update the search statistics.
		 *
		 * We're not holding the region locked here, these statistics
		 * can't be trusted.
		 */
		total_buckets += buckets;
		if (total_buckets != 0) {
			if (total_buckets > c_mp->stat.st_alloc_max_buckets)
				c_mp->stat.st_alloc_max_buckets = total_buckets;
			c_mp->stat.st_alloc_buckets += total_buckets;
		}
		if (buffers != 0) {
			if (buffers > c_mp->stat.st_alloc_max_pages)
				c_mp->stat.st_alloc_max_pages = buffers;
			c_mp->stat.st_alloc_pages += buffers;
		}
		return (0);
	} else if (giveup || c_mp->stat.st_pages == 0) {
		R_UNLOCK(dbenv, memreg);
		logmsg(LOGMSG_FATAL,
		       "unable to allocate space from the buffer cache\n");
		abort();
	}

	/*
	 * We re-attempt the allocation every time we've freed 3 times what
	 * we need.  Reset our free-space counter.
	 */
	freed_space = 0;
	total_buckets += buckets;
	buckets = 0;

	/*
	 * Walk the hash buckets and find the next two with potentially useful
	 * buffers.  Free the buffer with the lowest priority from the buckets'
	 * chains.
	 */
	for (;;) {
		/* All pages have been freed, make one last try */
		if (c_mp->stat.st_pages == 0)
			goto alloc;

		/* Check for wrap around. */
		hp = &dbht[c_mp->last_checked++];
		if (hp >= hp_end) {
			c_mp->last_checked = 0;
			hp = &dbht[c_mp->last_checked++];
		}

		/*
		 * The failure mode is when there are too many buffers we can't
		 * write or there's not enough memory in the system.  We don't
		 * have a way to know that allocation has no way to succeed.
		 * We fail if there were no pages returned to the cache after
		 * we've been trying for a relatively long time.
		 *
		 * Get aggressive if we've tried to flush the number of hash
		 * buckets as are in the system and have not found any more
		 * space.  Aggressive means:
		 *
		 * a: set a flag to attempt to flush high priority buffers as
		 *    well as other buffers.
		 * b: sync the mpool to force out queue extent pages.  While we
		 *    might not have enough space for what we want and flushing
		 *    is expensive, why not?
		 * c: look at a buffer in every hash bucket rather than choose
		 *    the more preferable of two.
		 * d: start to think about giving up.
		 *
		 * If we get here twice, sleep for a second, hopefully someone
		 * else will run and free up some memory.
		 *
		 * Always try to allocate memory too, in case some other thread
		 * returns its memory to the region.
		 *
		 * !!!
		 * This test ignores pathological cases like no buffers in the
		 * system -- that shouldn't be possible.
		 */
		if (buckets++ == c_mp->htab_buckets) {
			if (freed_space > 0)
				goto alloc;
			R_UNLOCK(dbenv, memreg);

			switch (++aggressive) {
			case 1:
				break;
			case 2:
				put_counter = c_mp->put_counter;
				/* FALLTHROUGH */
			case 3:
			case 4:
			case 5:
			case 6:
				(void)__memp_sync_int(dbenv, NULL, 0,
				    DB_SYNC_ALLOC, NULL, 0, NULL, 0);

				sleeptime++;
				if (__gbl_max_mpalloc_sleeptime &&
				    sleeptime > __gbl_max_mpalloc_sleeptime) {
					Pthread_mutex_lock(&dump_once_lk);
					alarm(10);
					dump_page_stats(dbenv);
					_exit(1);
				}
				(void)__os_sleep(dbenv, 1, 0);
				break;
			default:
				aggressive = 1;
				if (put_counter == c_mp->put_counter)
					giveup = 1;
				break;
			}

			R_LOCK(dbenv, memreg);
			goto alloc;
		}

		/*
		 * Skip empty buckets.
		 *
		 * We can check for empty buckets before locking as we
		 * only care if the pointer is zero or non-zero.
		 */
		if (SH_TAILQ_FIRST(&hp->hash_bucket, __bh) == NULL)
			continue;

		if (!aggressive) {
			/* Skip high priority buckets. */
			if (hp->hash_priority > high_priority)
				continue;

			/*
			 * Find two buckets and select the one with the lowest
			 * priority.  Performance testing shows that looking
			 * at two improves the LRUness and looking at more only
			 * does a little better.
			 */
			if (hp_tmp == NULL) {
				hp_tmp = hp;
				continue;
			}
			if (hp->hash_priority > hp_tmp->hash_priority)
				hp = hp_tmp;
			hp_tmp = NULL;
		}

		/* Remember the priority of the buffer we're looking for. */
		priority = hp->hash_priority;

		/* Unlock the region and lock the hash bucket. */
		R_UNLOCK(dbenv, memreg);
		mutexp = &hp->hash_mutex;
		MUTEX_LOCK(dbenv, mutexp);

#ifdef DIAGNOSTIC
		__memp_check_order(hp);
#endif
		/*
		 * The lowest priority page is first in the bucket, as they are
		 * maintained in sorted order.
		 *
		 * The buffer may have been freed or its priority changed while
		 * we switched from the region lock to the hash lock.  If so,
		 * we have to restart.  We will still take the first buffer on
		 * the bucket's list, though, if it has a low enough priority.
		 */
		if ((bhp =
			SH_TAILQ_FIRST(&hp->hash_bucket,
			    __bh)) == NULL ||bhp->ref != 0 ||
		    bhp->priority > priority)
			goto next_hb;

		GET_BH_GEN(&bhp->buf) = 0;
		buffers++;

		/* Find the associated MPOOLFILE. */
		bh_mfp = R_ADDR(dbmp->reginfo, bhp->mf_offset);

		if (F_ISSET(bhp, BH_PREFAULT)) {
			++c_mp->stat.st_pf_evict;
		}

		/* 
		 * If the page is dirty, pin it and write it. Regardless
		 * of priority, skip dirty pages on the first pass.
		 */
		ret = 0;
		if (F_ISSET(bhp, BH_DIRTY)) {
			if (aggressive == 0) {
				++c_mp->stat.st_rw_evict_skip;
				goto next_hb;
			}

			++bhp->ref;
			ret = __memp_bhwrite(dbmp, hp, bh_mfp, bhp, 0);
			--bhp->ref;
			if (ret == 0) {
				++c_mp->stat.st_rw_evict;
				if(ISLEAF(bhp->buf)) ++c_mp->stat.st_rw_levict;
			}
		} else {
			++c_mp->stat.st_ro_evict;
			if(ISLEAF(bhp->buf)) ++c_mp->stat.st_ro_levict;
		}

		/*
		 * If a write fails for any reason, we can't proceed.
		 *
		 * We released the hash bucket lock while doing I/O, so another
		 * thread may have acquired this buffer and incremented the ref
		 * count after we wrote it, in which case we can't have it.
		 *
		 * If there's a write error and we're having problems finding
		 * something to allocate, avoid selecting this buffer again
		 * by making it the bucket's least-desirable buffer.
		 */
		if (ret != 0 || bhp->ref != 0) {
			if (ret != 0 && aggressive)
				__memp_bad_buffer(hp);
			goto next_hb;
		}

		/*
		 * Check to see if the buffer is the size we're looking for.
		 * If so, we can simply reuse it.  Else, free the buffer and
		 * its space and keep looking.
		 */
		if (mfp != NULL &&
		    mfp->stat.st_pagesize == bh_mfp->stat.st_pagesize) {
			__memp_bhfree(dbmp, hp, bhp, 0);

			p = bhp;
			goto found;
		}

		freed_space += __db_shsizeof(bhp);
		__memp_bhfree(dbmp, hp, bhp, 1);
		if (aggressive > 1)
			aggressive = 1;

		/*
		 * Unlock this hash bucket and re-acquire the region lock. If
		 * we're reaching here as a result of calling memp_bhfree, the
		 * hash bucket lock has already been discarded.
		 */
		if (0) {
next_hb:		MUTEX_UNLOCK(dbenv, mutexp);
		}
		R_LOCK(dbenv, memreg);

		/*
		 * Retry the allocation as soon as we've freed up sufficient
		 * space.  We're likely to have to coalesce of memory to
		 * satisfy the request, don't try until it's likely (possible?)
		 * we'll succeed.
		 */
		if (freed_space >= 3 * len)
			goto alloc;
	}
	/* NOTREACHED */
}


/*
 * __memp_alloc --
 *	Allocate some space from a cache region.
 *
 * PUBLIC: int __memp_alloc __P((DB_MPOOL *,
 * PUBLIC:     REGINFO *, MPOOLFILE *, size_t, roff_t *, void *));
 */
int
__memp_alloc(dbmp, memreg, mfp, len, offsetp, retp)
	DB_MPOOL *dbmp;
	REGINFO *memreg;
	MPOOLFILE *mfp;
	size_t len;
	roff_t *offsetp;
	void *retp;
{
	return __memp_alloc_flags(dbmp, memreg, mfp, len, offsetp, 0, retp);
}


/*
 * __memp_bad_buffer --
 *	Make the first buffer in a hash bucket the least desirable buffer.
 */
static void
__memp_bad_buffer(hp)
	DB_MPOOL_HASH *hp;
{
	BH *bhp;
	u_int32_t priority;

	/* Remove the first buffer from the bucket. */
	bhp = SH_TAILQ_FIRST(&hp->hash_bucket, __bh);
	SH_TAILQ_REMOVE(&hp->hash_bucket, bhp, hq, __bh);

	/*
	 * Find the highest priority buffer in the bucket.  Buffers are
	 * sorted by priority, so it's the last one in the bucket.
	 */
	priority = bhp->priority;
	if (!SH_TAILQ_EMPTY(&hp->hash_bucket))
		priority = SH_TAILQ_LAST(&hp->hash_bucket, HashTab)->priority;

	/*
	 * Set our buffer's priority to be just as bad, and append it to
	 * the bucket.
	 */
	bhp->priority = priority;
	SH_TAILQ_INSERT_TAIL(&hp->hash_bucket, bhp, hq);

	/* Reset the hash bucket's priority. */
	hp->hash_priority = SH_TAILQ_FIRST(&hp->hash_bucket, __bh)->priority;
}

#ifdef DIAGNOSTIC
/*
 * __memp_check_order --
 *	Verify the priority ordering of a hash bucket chain.
 *
 * PUBLIC: #ifdef DIAGNOSTIC
 * PUBLIC: void __memp_check_order __P((DB_MPOOL_HASH *));
 * PUBLIC: #endif
 */
void
__memp_check_order(hp)
	DB_MPOOL_HASH *hp;
{
	BH *bhp;
	u_int32_t priority;

	/*
	 * Assumes the hash bucket is locked.
	 */
	if ((bhp = SH_TAILQ_FIRST(&hp->hash_bucket, __bh)) == NULL)
		return;

	DB_ASSERT(bhp->priority == hp->hash_priority);

	for (priority = bhp->priority;
	    (bhp = SH_TAILQ_NEXT(bhp, hq, __bh)) != NULL;
	    priority = bhp->priority)
		DB_ASSERT(priority <= bhp->priority);
}
#endif
