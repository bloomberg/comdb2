/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
/*
 * Copyright (c) 1990, 1993, 1994, 1995, 1996
 *	Keith Bostic.  All rights reserved.
 */
/*
 * Copyright (c) 1990, 1993, 1994, 1995
 *	The Regents of the University of California.  All rights reserved.
 *
 * This code is derived from software contributed to Berkeley by
 * Mike Olson.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: bt_search.c,v 11.47 2003/06/30 17:19:35 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#include <sys/time.h>

#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/btree.h"
#include "dbinc/lock.h"
#include "dbinc/mp.h"

#include <thread_util.h>
#include <btree/bt_prefix.h>
#include <btree/bt_cache.h>

#include <btree/bt_pf.h>


#include <stdbool.h>
#include <time.h>
#include <util.h>

#include <logmsg.h>
#include <locks_wrap.h>

/*
 * __bam_cmp --
 *	Compare a key to a given record.
 *
 * PUBLIC: int __bam_cmp __P((DB *, const DBT *, PAGE *,
 * PUBLIC:    u_int32_t, int (*)(DB *, const DBT *, const DBT *), int *));
 */
static inline int
__bam_cmp_inline(dbp, dbt, h, indx, func, cmpp, buf)
	DB *dbp;
	const DBT *dbt;
	PAGE *h;
	u_int32_t indx;
	int (*func)__P((DB *, const DBT *, const DBT *));
	int *cmpp;
	uint8_t *buf;
{
	BINTERNAL *bi;
	BKEYDATA *bk;
	BOVERFLOW *bo;
	DBT pg_dbt;

	/*
	 * Returns:
	 *	< 0 if dbt is < page record
	 *	= 0 if dbt is = page record
	 *	> 0 if dbt is > page record
	 *
	 * !!!
	 * We do not clear the pg_dbt DBT even though it's likely to contain
	 * random bits.  That should be okay, because the app's comparison
	 * routine had better not be looking at fields other than data/size.
	 * We don't clear it because we go through this path a lot and it's
	 * expensive.
	 */
	switch (TYPE(h)) {
	case P_LBTREE:
	case P_LDUP:
	case P_LRECNO:
		bk = GET_BKEYDATA(dbp, h, indx);
		if (B_TYPE(bk) == B_OVERFLOW)
			bo = (BOVERFLOW *)bk;
		else {
			bk_decompress(dbp, h, &bk, buf, KEYBUF);
			pg_dbt.app_data = NULL;
			pg_dbt.data = bk->data;
			ASSIGN_ALIGN_DIFF(u_int32_t, pg_dbt.size, db_indx_t,
			    bk->len);
			if (likely(func == __bam_defcmp)) {
				int len;
				len = dbt->size > pg_dbt.size ? pg_dbt.size
				    : dbt->size;
				*cmpp = memcmp(dbt->data, pg_dbt.data, len);
				if (unlikely(*cmpp == 0))
					*cmpp =
					    ((long)dbt->size -
					    (long)pg_dbt.size);
			} else {
				*cmpp = func(dbp, dbt, &pg_dbt);
			}
			return (0);
		}
		break;
	case P_IBTREE:
		/*
		 * The following code guarantees that the left-most key on an
		 * internal page at any place in the tree sorts less than any
		 * user-specified key.  The reason is that if we have reached
		 * this internal page, we know the user key must sort greater
		 * than the key we're storing for this page in any internal
		 * pages at levels above us in the tree.  It then follows that
		 * any user-specified key cannot sort less than the first page
		 * which we reference, and so there's no reason to call the
		 * comparison routine.  While this may save us a comparison
		 * routine call or two, the real reason for this is because
		 * we don't maintain a copy of the smallest key in the tree,
		 * so that we don't have to update all the levels of the tree
		 * should the application store a new smallest key.  And, so,
		 * we may not have a key to compare, which makes doing the
		 * comparison difficult and error prone.
		 */
		if (indx == 0) {
			*cmpp = 1;
			return (0);
		}

		bi = GET_BINTERNAL(dbp, h, indx);
		if (unlikely(B_TYPE(bi) == B_OVERFLOW))
			bo = (BOVERFLOW *)(bi->data);
		else {
			pg_dbt.app_data = NULL;

			pg_dbt.data = bi->data;
			pg_dbt.size = bi->len;
			if (likely(func == __bam_defcmp)) {
				int len;

				len = dbt->size > pg_dbt.size ? pg_dbt.size
				    : dbt->size;
				*cmpp = memcmp(dbt->data, pg_dbt.data, len);
				if (unlikely((*cmpp == 0)))
					*cmpp = ((long)dbt->size -
					    (long)pg_dbt.size);
			} else {
				*cmpp = func(dbp, dbt, &pg_dbt);
			}
			return (0);
		}
		break;
	default:
		return (__db_pgfmt(dbp->dbenv, PGNO(h)));
	}

	/*
	 * Overflow.
	 */
	return (__db_moff(dbp, dbt,
		bo->pgno, bo->tlen, func == __bam_defcmp ? NULL : func, cmpp));
}

/* genid-pgno hashtable - some code stolen from plhash.c */
genid_hash *
genid_hash_init(DB_ENV *dbenv, int szkb)
{
	static const unsigned int prime[] =
	    { 61, 131, 257, 521, 1031, 1511, 2053, 3079, 4099, 6053, 8209,
	      12329, 16411, 24533, 32771, 48871, 65537, 95531, 131101,
	      190507, 262147, 393203, 524309, 786469, 1048583, 2100001,
	      4194319, 0 };

	unsigned int ii;
	genid_hash *h;

	if (szkb <= 0)
		return NULL;

	if (__os_calloc(dbenv, 1, sizeof(genid_hash), &h) != 0) {
		return NULL;
	}

	h->ntbl = szkb;
	h->ntbl = (h->ntbl << 10) / 12;

	ii = 0;
	while (prime[ii] != 0) {
		if (prime[ii + 1] > h->ntbl || prime[ii + 1] == 0) {
			h->ntbl = prime[ii];
			break;
		} else {
			ii++;
		}
	}

	Pthread_mutex_init(&(h->mutex), NULL);

	if (__os_calloc(dbenv, h->ntbl, sizeof(__genid_pgno), &(h->tbl)) != 0) {
		__os_free(dbenv, h);
		return NULL;
	}
	return h;
}

void
genid_hash_resize(DB_ENV *dbenv, genid_hash ** hpp, int szkb)
{
	if (szkb <= 0)
		return;
	if (*hpp != NULL) {
		genid_hash_free(dbenv, *hpp);
		*hpp = NULL;
	}
	*hpp = genid_hash_init(dbenv, szkb);
}

void
genid_hash_free(DB_ENV *dbenv, genid_hash * hp)
{
	if (!hp)
		return;
	__os_free(dbenv, hp->tbl);
	Pthread_mutex_destroy(&(hp->mutex));
	__os_free(dbenv, hp);
}

enum { PRIME = 8388013 };

unsigned int
hash_fixedwidth(const unsigned char *genid)
{
	unsigned hash;
	int jj;

	for (hash = 0, jj = 0; jj < 8; jj++)
		hash = ((hash % PRIME) << 8) + genid[jj];
	return hash;
}

#ifdef _LINUX_SOURCE
int
genidcmp(const void *hash_genid, const void *genid)
{
	genid = (char *)genid + 2;
	return memcmp(hash_genid, genid, HASH_GENID_SIZE);
}
#else
int
genidcmp(const void *hash_genid, const void *genid)
{
	return memcmp(hash_genid, genid, HASH_GENID_SIZE);
}
#endif

#ifdef _LINUX_SOURCE
void
genidcpy(void *dest, const void *src)
{
	src = (char *)src + 2;
	memcpy(dest, src, HASH_GENID_SIZE);
}
#else
void
genidcpy(void *dest, const void *src)
{
	memcpy(dest, src, HASH_GENID_SIZE);
}
#endif

void
genidsetzero(void *g)
{
	memset(g, 0, HASH_GENID_SIZE);
}

void
timeval_add(struct timeval *tvp, struct timeval *uvp, struct timeval *vvp)
{
	vvp->tv_sec = tvp->tv_sec + uvp->tv_sec;
	vvp->tv_usec = tvp->tv_usec + uvp->tv_usec;
	if (vvp->tv_usec >= 1000000) {
		vvp->tv_sec++;
		vvp->tv_usec -= 1000000;
	}
}

/*
 * __bam_search --
 *	Search a btree for a key.
 *
 * PUBLIC: int __bam_search __P((DBC *, db_pgno_t,
 * PUBLIC:     const DBT *, u_int32_t, int, db_recno_t *, int *));
 */
int
__bam_search(dbc, root_pgno, key, flags, stop, recnop, exactp)
	DBC *dbc;
	db_pgno_t root_pgno;
	const DBT *key;
	u_int32_t flags;
	int stop, *exactp;
	db_recno_t *recnop;
{
	BTREE *t;
	BTREE_CURSOR *cp;
	DB *dbp;
	DB_LOCK lock;
	DB_MPOOLFILE *mpf;
	PAGE *h;
	db_indx_t base, i, indx, *inp, lim;
	db_lockmode_t lock_mode;
	db_pgno_t pg;
	db_recno_t recno;
	int adjust, cmp, deloffset, ret, stack;
	int (*func) __P((DB *, const DBT *, const DBT *));
	void *cached_pg = NULL;
	void *bfpool_pg = NULL;
	bool save = false;
	uint16_t gen;
	uint32_t slot;
	unsigned int hh = 0;
	genid_hash *hash = NULL;
	__genid_pgno *hashtbl = NULL;
	int got_pg_from_hash = 0;
	int add_to_hash = 0;
	db_pgno_t hash_pg = 0;
	db_pgno_t pg_copy = 0;

	struct timeval before, after, diff;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;
	t = dbp->bt_internal;
	recno = 0;


	BT_STK_CLR(cp);

	INTERNAL_PTR_CHECK(cp == dbc->internal);

	/*
	 * There are several ways we search a btree tree.  The flags argument
	 * specifies if we're acquiring read or write locks, if we position
	 * to the first or last item in a set of duplicates, if we return
	 * deleted items, and if we are locking pairs of pages.  In addition,
	 * if we're modifying record numbers, we have to lock the entire tree
	 * regardless.  See btree.h for more details.
	 *
	 * If write-locking pages, we need to know whether or not to acquire a
	 * write lock on a page before getting it.  This depends on how deep it
	 * is in tree, which we don't know until we acquire the root page.  So,
	 * if we need to lock the root page we may have to upgrade it later,
	 * because we won't get the correct lock initially.
	 *
	 * Retrieve the root page.
	 */
try_again:

	INTERNAL_PTR_CHECK(cp == dbc->internal);

	pg = root_pgno == PGNO_INVALID ? cp->root : root_pgno;
	stack = LF_ISSET(S_STACK) && F_ISSET(cp, C_RECNUM);
	lock_mode = stack ? DB_LOCK_WRITE : DB_LOCK_READ;

	dbp->pg_hash_stat.n_bt_search++;
	gettimeofday(&before, NULL);

	extern bool gbl_rcache;

	if (gbl_rcache && pg == 1 && bfpool_pg == NULL &&
	    lock_mode == DB_LOCK_READ && LF_ISSET(S_FIND)) {
		save = true;
		if (rcache_find(
		    dbp, &cached_pg, &bfpool_pg, &gen, &slot) == 0) {
			h = cached_pg;
			goto got_pg;
		}
	}

	/*
	  If berkdb page hash is enabled and the search on a datafile
	  does not required a page stack or a parent page, we can use
	  our fixed-size page hash to get to the leaf page quickly
	  with cracking the btree.  If the genid is in the hash, we
	  jump to the corresponding page immediately If no such genid
	  stored in the hash, we start from root page.  We write to
	  the hash with mutex but we tried to read the hash
	  locklessly.  The protocol is: READER: 1
	  Hash-to-correct-entry 2 Verify that the genid is correct 3
	  Grab the page number 4 Verify that the genid is correct
	  (again)

	  WRITER:
	  1 Hash-to-correct-entry
	  2 Write an invalid genid (0)
	  3 Write the pagenumber
	  4 Write the correct genid

	  Page splits (and reverse-splits) will update the page number
	  in the hash automatically. We first get the page from hash
	  and we will verify the page again after we get the lock on
	  that page. If after getting the page lock, the hash page
	  changes, we know that is due to page split (rsplit) and we
	  will start from the new page instead. If we can not see the
	  genid in the hash after getting page lock, we don't know
	  whether the page is still correct. (split could have
	  happened.) Instead, we will start from root page in this
	  case. So we assume the page numbers in the hash are always
	  correct. (If not there is bug.)  Cases are: 1) If the page
	  got from the hash is no longer a leaf page, the page is
	  deleted and we return NOTFOUND 2) If we can not find the key
	  on the page specified by the hash, (since we assume page
	  numbers are always correct in our scheme), key must be
	  deleted and we return NOTFOUND 3) Othervise, key should be
	  found in the page.
	*/
	if (!stack &&
	    !LF_ISSET(S_PARENT) &&
	    F_ISSET(dbp, DB_AM_HASH) &&
	    (hash = dbp->pg_hash) != NULL &&
	    key->size == GENID_SIZE) {
		dbp->pg_hash_stat.n_bt_hash++;
		hashtbl = hash->tbl;
		//1 Hash-to-correct-entry
		hh = hash_fixedwidth((unsigned char *)(key->data)) % 
			(hash->ntbl);
		//2 Verify that the genid is correct
		if (genidcmp(hashtbl[hh].genid, key->data) == 0) {
			//3 Grab the page number
			hash_pg = hashtbl[hh].pgno;
			//4 Verify that the genid is correct (again)
			if (genidcmp(hashtbl[hh].genid, key->data) == 0) {
				pg_copy = pg;
				pg = hash_pg;
				got_pg_from_hash = 1;
			} else {
				dbp->pg_hash_stat.n_bt_hash_miss++;
				add_to_hash = 1;
			}
		} else {
			dbp->pg_hash_stat.n_bt_hash_miss++;
			add_to_hash = 1;
		}
	}
hash_backup:
	if ((ret = __db_lget(dbc, 0, pg, lock_mode, 0, &lock)) != 0) {
		if (got_pg_from_hash) {
			pg = pg_copy;
			add_to_hash = 1;
			got_pg_from_hash = 0;
			goto hash_backup;
		} else {
			return (ret);
		}
	}
	ret = __memp_fget(mpf, &pg, 0, &h);
	if (ret != 0) {
		/* Did not read it, so we can release the lock */
		(void)__LPUT(dbc, lock);
		if (got_pg_from_hash) {
			pg = pg_copy;
			add_to_hash = 1;
			got_pg_from_hash = 0;
			goto hash_backup;
		} else {
			return (ret);
		}
	}

	if (got_pg_from_hash) {
		if ((genidcmp(hashtbl[hh].genid, key->data) != 0) ||
		    ((hash_pg = hashtbl[hh].pgno) != h->pgno)) {
			// hash page changed between searching the hash and getting page lock
			(void)__memp_fput(mpf, h, 0);
			(void)__LPUT(dbc, lock);
			if (genidcmp(hashtbl[hh].genid, key->data) == 0) {
				// use the new hash pg
				pg = hash_pg;
				got_pg_from_hash = 1;
			} else {
				// search from root page
				pg = pg_copy;
				got_pg_from_hash = 0;
				add_to_hash = 1;
			}
			goto hash_backup;
		}
		dbp->pg_hash_stat.n_bt_hash_hit++;
		if (!(TYPE(h) == P_LBTREE || TYPE(h) == P_LDUP)) {
			// remove pg from hash
			Pthread_mutex_lock(&(hash->mutex));
			genidsetzero(hashtbl[hh].genid);
			hashtbl[hh].pgno = 0;
			Pthread_mutex_unlock(&(hash->mutex));
			goto notfound;
		}
	}

	if (save && TYPE(h) == P_IBTREE) {	// WORKS ONLY WHEN ROOT IS INTERNAL
		uint16_t gen = LSN(h).file + LSN(h).offset;

		GET_BH_GEN(h) = gen;
		rcache_save(dbp, h, gen);
	}

	INTERNAL_PTR_CHECK(cp == dbc->internal);

	/*
	 * Decide if we need to save this page; if we do, write lock it.
	 * We deliberately don't lock-couple on this call.  If the tree
	 * is tiny, i.e., one page, and two threads are busily updating
	 * the root page, we're almost guaranteed deadlocks galore, as
	 * each one gets a read lock and then blocks the other's attempt
	 * for a write lock.
	 */
	if (!stack &&
	    ((LF_ISSET(S_PARENT) && (u_int8_t)(stop + 1) >= h->level) ||
		(LF_ISSET(S_WRITE) && h->level == LEAFLEVEL))) {
		(void)__memp_fput(mpf, h, 0);
		(void)__LPUT(dbc, lock);
		INTERNAL_PTR_CHECK(cp == dbc->internal);
		lock_mode = DB_LOCK_WRITE;
		if ((ret = __db_lget(dbc, 0, pg, lock_mode, 0, &lock)) != 0)
			return (ret);
		ret = __memp_fget(mpf, &pg, 0, &h);
		if (ret != 0) {
			/* Did not read it, so we can release the lock */
			(void)__LPUT(dbc, lock);
			return (ret);
		}
		if (!((LF_ISSET(S_PARENT) &&
			    (u_int8_t)(stop + 1) >= h->level) ||
			(LF_ISSET(S_WRITE) && h->level == LEAFLEVEL))) {
			/* Someone else split the root, start over. */
			INTERNAL_PTR_CHECK(cp == dbc->internal);
			(void)__memp_fput(mpf, h, 0);
			(void)__LPUT(dbc, lock);
			INTERNAL_PTR_CHECK(cp == dbc->internal);
			goto try_again;
		}
		stack = 1;
	}

	/* Choose a comparison function. */
got_pg:func = t->bt_compare;

	INTERNAL_PTR_CHECK(cp == dbc->internal);

	for (;;) {
		inp = P_INP(dbp, h);
		/*
		 * Do a binary search on the current page.  If we're searching
		 * a Btree leaf page, we have to walk the indices in groups of
		 * two.  If we're searching an internal page or a off-page dup
		 * page, they're an index per page item.  If we find an exact
		 * match on a leaf page, we're done.
		 */
		adjust = TYPE(h) == P_LBTREE ? P_INDX : O_INDX;
		uint8_t buf[KEYBUF];

		for (base = 0,
		    lim = NUM_ENT(h) / (db_indx_t) adjust; lim != 0;
		    lim >>= 1) {
			indx = base + ((lim >> 1) * adjust);

			if ((ret =
				__bam_cmp_inline(dbp, key, h, indx, func, &cmp,
				    buf)) != 0)
				goto err;
			if (cmp == 0) {
				if (TYPE(h) == P_LBTREE || TYPE(h) == P_LDUP)
					goto found;
				goto next;
			}
			if (cmp > 0) {
				base = indx + adjust;
				--lim;
			}
		}

		/*
		 * No match found.  Base is the smallest index greater than
		 * key and may be zero or a last + O_INDX index.
		 *
		 * If it's a leaf page, return base as the "found" value.
		 * Delete only deletes exact matches.
		 */
		if (TYPE(h) == P_LBTREE || TYPE(h) == P_LDUP) {
			*exactp = 0;

			if (LF_ISSET(S_EXACT))
				goto notfound;

			if (LF_ISSET(S_STK_ONLY)) {
				BT_STK_NUM(dbp->dbenv, cp, h, base, ret);
				__LPUT(dbc, lock);
				(void)__memp_fput(mpf, h, 0);
				return (ret);
			}

			/*
			 * !!!
			 * Possibly returning a deleted record -- DB_SET_RANGE,
			 * DB_KEYFIRST and DB_KEYLAST don't require an exact
			 * match, and we don't want to walk multiple pages here
			 * to find an undeleted record.  This is handled by the
			 * calling routine.
			 */
			BT_STK_ENTER(dbp->dbenv,
			    cp, h, base, lock, lock_mode, ret);
			if (ret != 0)
				goto err;
			return (0);
		}

		/*
		 * If it's not a leaf page, record the internal page (which is
		 * a parent page for the key).  Decrement the base by 1 if it's
		 * non-zero so that if a split later occurs, the inserted page
		 * will be to the right of the saved page.
		 */
		indx = base > 0 ? base - O_INDX : base;

		/*
		 * If we're trying to calculate the record number, sum up
		 * all the record numbers on this page up to the indx point.
		 */
next:		if (recnop != NULL)
			for (i = 0; i < indx; ++i)
				recno += GET_BINTERNAL(dbp, h, i)->nrecs;

		pg = GET_BINTERNAL(dbp, h, indx)->pgno;
#if USE_BTPF
		// ######################## PAGE FAULT  /// Fabio
		trk_descent(dbp, dbc, h, indx);
		// ##################################################
#endif

		if (LF_ISSET(S_STK_ONLY)) {
			if (stop == h->level) {
				INTERNAL_PTR_CHECK(cp == dbc->internal);
				BT_STK_NUM(dbp->dbenv, cp, h, indx, ret);
				__LPUT(dbc, lock);
				(void)__memp_fput(mpf, h, 0);
				INTERNAL_PTR_CHECK(cp == dbc->internal);
				return (ret);
			}
			INTERNAL_PTR_CHECK(cp == dbc->internal);
			BT_STK_NUMPUSH(dbp->dbenv, cp, h, indx, ret);
			(void)__memp_fput(mpf, h, 0);
			INTERNAL_PTR_CHECK(cp == dbc->internal);
			if ((ret = __db_lget(dbc,
				    LCK_COUPLE_ALWAYS, pg, lock_mode, 0,
				    &lock)) != 0) {
				/*
				 * Discard our lock and return on failure.  This
				 * is OK because it only happens when descending
				 * the tree holding read-locks.
				 */
				__LPUT(dbc, lock);
				INTERNAL_PTR_CHECK(cp == dbc->internal);
				return (ret);
			}
			INTERNAL_PTR_CHECK(cp == dbc->internal);
		} else if (stack) {
			/* Return if this is the lowest page wanted. */
			INTERNAL_PTR_CHECK(cp == dbc->internal);
			if (LF_ISSET(S_PARENT) && stop == h->level) {
				INTERNAL_PTR_CHECK(cp == dbc->internal);
				BT_STK_ENTER(dbp->dbenv,
				    cp, h, indx, lock, lock_mode, ret);
				INTERNAL_PTR_CHECK(cp == dbc->internal);
				if (ret != 0)
					goto err;
				INTERNAL_PTR_CHECK(cp == dbc->internal);
				return (0);
			}
			INTERNAL_PTR_CHECK(cp == dbc->internal);
			BT_STK_PUSH(dbp->dbenv,
			    cp, h, indx, lock, lock_mode, ret);
			INTERNAL_PTR_CHECK(cp == dbc->internal);
			if (ret != 0)
				goto err;

			lock_mode = DB_LOCK_WRITE;
			if ((ret =
				__db_lget(dbc, 0, pg, lock_mode, 0,
				    &lock)) != 0)
				goto err;
			INTERNAL_PTR_CHECK(cp == dbc->internal);
		} else {
			/*
			 * Decide if we want to return a reference to the next
			 * page in the return stack.  If so, lock it and never
			 * unlock it.
			 */
			INTERNAL_PTR_CHECK(cp == dbc->internal);
			if ((LF_ISSET(S_PARENT) &&
				(u_int8_t)(stop + 1) >=
				(u_int8_t)(h->level - 1)) ||
			    (h->level - 1) == LEAFLEVEL)
				stack = 1;

			lock_mode = stack &&
			    LF_ISSET(S_WRITE) ? DB_LOCK_WRITE : DB_LOCK_READ;

			if (cached_pg) {
				/* Used rcache to get here. Don't lck couple. */
				if ((ret = __db_lget(dbc, 0, pg, lock_mode, 0,
					    &lock)) != 0)
					goto err;
			} else {
				(void)__memp_fput(mpf, h, 0);
				if ((ret = __db_lget(dbc,
					    LCK_COUPLE_ALWAYS, pg, lock_mode, 0,
					    &lock)) != 0) {
					INTERNAL_PTR_CHECK(cp == dbc->internal);
					/*
					 * If we fail, discard the lock we held.  This
					 * is OK because this only happens when we are
					 * descending the tree holding read-locks.
					 */
					__LPUT(dbc, lock);
					INTERNAL_PTR_CHECK(cp == dbc->internal);
					goto err;
				}
			}
			INTERNAL_PTR_CHECK(cp == dbc->internal);
		}
#if 0
		printf("GETTING %s:%d\n", dbc->dbp->fname, pg);
#endif
		ret = __memp_fget(mpf, &pg, 0, &h);
		if (ret != 0) {
			if (cached_pg) {
				/*
				 * Used rcache and failed getting child
				 * page. Let's retry w/o rcache.
				 */
				cached_pg = NULL;

				rcache_invalidate(slot);
				__LPUT(dbc, lock);
				goto try_again;
			}
			goto err;
		}

		if (cached_pg) {
			/* Used rcache and got child page. Validate rcache. */
			DB_LSN *l1 = &LSN(cached_pg);
			DB_LSN *l2 = &LSN(bfpool_pg);
			cached_pg = NULL;

			if (gen == GET_BH_GEN(bfpool_pg)
			    && memcmp(l1, l2, sizeof(DB_LSN)) == 0 && gen == GET_BH_GEN(bfpool_pg)	//re-check. warm&fuzzy
			    ) {
				;
			} else {
				__memp_fput(mpf, h, 0);
				__LPUT(dbc, lock);
				rcache_invalidate(slot);
				goto try_again;
			}
		}
	}
	/* NOTREACHED */

found:	*exactp = 1;


	INTERNAL_PTR_CHECK(cp == dbc->internal);
	/*
	 * If we got here, we know that we have a Btree leaf or off-page
	 * duplicates page.  If it's a Btree leaf page, we have to handle
	 * on-page duplicates.
	 *
	 * If there are duplicates, go to the first/last one.  This is
	 * safe because we know that we're not going to leave the page,
	 * all duplicate sets that are not on overflow pages exist on a
	 * single leaf page.
	 */
	if (TYPE(h) == P_LBTREE) {
		if (LF_ISSET(S_DUPLAST))
			while (indx < (db_indx_t)(NUM_ENT(h) - P_INDX) &&
			    inp[indx] == inp[indx + P_INDX])
				indx += P_INDX;
		else
			while (indx > 0 &&
			    inp[indx] == inp[indx - P_INDX])
				indx -= P_INDX;
	}

	/*
	 * Now check if we are allowed to return deleted items; if not, then
	 * find the next (or previous) non-deleted duplicate entry.  (We do
	 * not move from the original found key on the basis of the S_DELNO
	 * flag.)
	 */
	DB_ASSERT(recnop == NULL ||LF_ISSET(S_DELNO));

	INTERNAL_PTR_CHECK(cp == dbc->internal);
	if (LF_ISSET(S_DELNO)) {
		INTERNAL_PTR_CHECK(cp == dbc->internal);
		deloffset = TYPE(h) == P_LBTREE ? O_INDX : 0;
		if (LF_ISSET(S_DUPLAST))
			while (B_DISSET(GET_BKEYDATA(dbp, h, indx + deloffset))
			    && indx > 0 && inp[indx] == inp[indx - adjust])
				indx -= adjust;
		else
			while (B_DISSET(GET_BKEYDATA(dbp, h, indx + deloffset))
			    && indx < (db_indx_t)(NUM_ENT(h) - adjust) &&
			    inp[indx] == inp[indx + adjust])
				indx += adjust;

		INTERNAL_PTR_CHECK(cp == dbc->internal);
		/*
		 * If we weren't able to find a non-deleted duplicate, return
		 * DB_NOTFOUND.
		 */
		if (B_DISSET(GET_BKEYDATA(dbp, h, indx + deloffset)))
			goto notfound;

		/*
		 * Increment the record counter to point to the found element.
		 * Ignore any deleted key/data pairs.  There doesn't need to
		 * be any correction for duplicates, as Btree doesn't support
		 * duplicates and record numbers in the same tree.
		 */
		if (recnop != NULL) {
			DB_ASSERT(TYPE(h) == P_LBTREE);

			for (i = 0; i < indx; i += P_INDX)
				if (!B_DISSET(GET_BKEYDATA(dbp, h, i + O_INDX)))
					++recno;

			/* Correct the number for a 0-base. */
			*recnop = recno + 1;
		}
	}

	INTERNAL_PTR_CHECK(cp == dbc->internal);

	if (LF_ISSET(S_STK_ONLY)) {
		INTERNAL_PTR_CHECK(cp == dbc->internal);
		BT_STK_NUM(dbp->dbenv, cp, h, indx, ret);

		__LPUT(dbc, lock);
		(void)__memp_fput(mpf, h, 0);
		INTERNAL_PTR_CHECK(cp == dbc->internal);
	} else {

		INTERNAL_PTR_CHECK(cp == dbc->internal);
		BT_STK_ENTER(dbp->dbenv, cp, h, indx, lock, lock_mode, ret);
		INTERNAL_PTR_CHECK(cp == dbc->internal);
		if (ret != 0)
			goto err;
	}
	INTERNAL_PTR_CHECK(cp == dbc->internal);

#if USE_BTPF
	// ################### PAGE FAULT
	trk_leaf(dbc, h, dbc->internal->indx);

	crsr_jump(dbc);
	// ########################################
#endif

	// only save non-root page
	if (add_to_hash && h->pgno != 1) {
		Pthread_mutex_lock(&(hash->mutex));
		//1 Hash-to-correct-entry
		//2 Write an invalid genid (0)
		genidsetzero(hashtbl[hh].genid);
		//3 Write the pagenumber
		hashtbl[hh].pgno = h->pgno;
		//4 Write the correct genid
		genidcpy(hashtbl[hh].genid, key->data);
		Pthread_mutex_unlock(&(hash->mutex));
	}
	gettimeofday(&after, NULL);

	timeval_diff(&before, &after, &diff);
	timeval_add(&(dbp->pg_hash_stat.t_bt_search),
	    &diff, &(dbp->pg_hash_stat.t_bt_search));

	return (0);

notfound:
	INTERNAL_PTR_CHECK(cp == dbc->internal);
	/* Keep the page locked for serializability. */
	dbc->lastpage = h->pgno;

	(void)__memp_fput(mpf, h, 0);
	(void)__TLPUT(dbc, lock);
	ret = DB_NOTFOUND;

	INTERNAL_PTR_CHECK(cp == dbc->internal);
	gettimeofday(&after, NULL);

	timeval_diff(&before, &after, &diff);
	timeval_add(&(dbp->pg_hash_stat.t_bt_search),
	    &diff, &(dbp->pg_hash_stat.t_bt_search));

err:	BT_STK_POP(cp);
	__bam_stkrel(dbc, 0);
	return (ret);
}

/*
 * __bam_stkrel --
 *	Release all pages currently held in the stack.
 *
 * PUBLIC: int __bam_stkrel __P((DBC *, u_int32_t));
 */
int
__bam_stkrel(dbc, flags)
	DBC *dbc;
	u_int32_t flags;
{
	BTREE_CURSOR *cp;
	DB *dbp;
	DB_MPOOLFILE *mpf;
	EPG *epg;
	int ret, t_ret;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;

	/*
	 * Release inner pages first.
	 *
	 * The caller must be sure that setting STK_NOLOCK will not effect
	 * either serializability or recoverability.
	 */
	for (ret = 0, epg = cp->sp; epg <= cp->csp; ++epg) {
		if (epg->page != NULL) {
			if (LF_ISSET(STK_CLRDBC) && cp->page == epg->page) {
				cp->page = NULL;
				LOCK_INIT(cp->lock);
			}
			if ((t_ret =
			    __memp_fput(mpf, epg->page, 0)) != 0 && ret == 0)
				ret = t_ret;
			/*
			 * XXX
			 * Temporary fix for #3243 -- under certain deadlock
			 * conditions we call here again and re-free the page.
			 * The correct fix is to never release a stack that
			 * doesn't hold items.
			 */
			epg->page = NULL;
		}
		if (LF_ISSET(STK_NOLOCK))
			(void)__LPUT(dbc, epg->lock);
		else
			(void)__TLPUT(dbc, epg->lock);
	}

	/* Clear the stack, all pages have been released. */
	BT_STK_CLR(cp);

	return (ret);
}

/*
 * __bam_stkgrow --
 *	Grow the stack.
 *
 * PUBLIC: int __bam_stkgrow __P((DB_ENV *, BTREE_CURSOR *));
 */
int
__bam_stkgrow(dbenv, cp)
	DB_ENV *dbenv;
	BTREE_CURSOR *cp;
{
	EPG *p;
	size_t entries;
	int ret;

	entries = cp->esp - cp->sp;

	if ((ret = __os_calloc(dbenv, entries * 2, sizeof(EPG), &p)) != 0)
		return (ret);
	memcpy(p, cp->sp, entries * sizeof(EPG));
	if (cp->sp != cp->stack)
		__os_free(dbenv, cp->sp);
	cp->sp = p;
	cp->csp = p + entries;
	cp->esp = p + entries * 2;
	return (0);
}
