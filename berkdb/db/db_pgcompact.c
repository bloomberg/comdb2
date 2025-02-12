/*
   Copyright 2015 Bloomberg Finance L.P.
  
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
   
       http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and 
   limitations under the License.
 */

#include "db_config.h"

#ifndef NO_SYSTEM_INCLUDES
#include <errno.h>
#include <string.h>
#include <unistd.h>
#endif

#include "db_int.h"
#include "dbinc/db_am.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"
#include "dbinc_auto/dbreg_auto.h"
#include "dbinc_auto/dbreg_ext.h"
#include "dbinc_auto/btree_ext.h"

#include "logmsg.h"

/*
 * __db_pgcompact --
 *  Compact page.
 *
 * PUBLIC: int __db_pgcompact __P((DB *, DB_TXN *, DBT *, double, double));
 */
int
__db_pgcompact(dbp, txn, dbt, ff, tgtff)
	DB *dbp;
	DB_TXN *txn;
	DBT *dbt;
	double ff;
	double tgtff;
{
	int ret, t_ret;
	DBC *dbc;
	DB_ENV *dbenv;
	extern int gbl_keycompr;

	dbenv = dbp->dbenv;

	if (F_ISSET(dbp, DB_AM_RECOVER))
		return EINVAL;

	/* We may get here before db is ready. If so, return (this is not an error). */
	if (gbl_keycompr && dbp->compression_flags == 0)
		return EPERM;

	if ((ret = __db_cursor(dbp, txn, &dbc, 0)) != 0) {
		__db_err(dbenv, "__db_cursor: %s", strerror(ret));
		return (ret);
	}

	if (dbc->dbtype == DB_BTREE) {
		/* Safeguard __bam_pgcompact(). I want to keep page compaction routine
		   private for now, so don't make it a function pointer of DB struct.  */
		ret = __bam_pgcompact(dbc, dbt, ff, tgtff);
	} else {
		__db_err(dbenv, "__db_pgcompact: %s",
				"Wrong access method or wrong cursor reference.");
		ret = EINVAL;
	}

	if ((t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}

/*
 * __db_ispgcompactible --
 *	Return whether the given page is compactible
 *
 * PUBLIC: int __db_ispgcompactible __P((DB *, db_pgno_t, DBT *, double));
 */
int
__db_ispgcompactible(dbp, pgno, dbt, ff)
	DB *dbp;
	db_pgno_t pgno;
	DBT *dbt;
    double ff;
{
	int ret, t_ret;
    DBC *dbc;
	DB_ENV *dbenv;

	dbenv = dbp->dbenv;

	if ((ret = __db_cursor(dbp, NULL, &dbc, 0)) != 0) {
		__db_err(dbenv, "__db_cursor: %s", strerror(ret));
		return (ret);
	}

    /* Keep pgcompact() to ourselves only. No function pointer
       added to the DBC struct. */
	if (dbc->dbtype == DB_BTREE) {
		ret = __bam_ispgcompactible(dbc, pgno, dbt, ff);
	} else {
		__db_err(dbenv, "__db_pgcompact: %s",
				"Wrong access method. Expect BTREE.");
		ret = EINVAL;
	}

	if ((t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

static int
pgno_cmp(const void *x, const void *y)
{
	return ((*(db_pgno_t *)x) - (*(db_pgno_t *)y));
}
static int
PAGE_cmp(const void *x, const void *y)
{
	return (PGNO(*(PAGE **)x) - PGNO(*(PAGE **)y));
}
/* PGMV tunables */

/* print additional pgmv information */
int gbl_pgmv_verbose = 0;
/* check pages even if they are still referenced in the log */
int gbl_pgmv_unsafe_db_resize = 0;
/* max number of page swaps within a single txn */
int gbl_pgmv_max_num_pages_swapped_per_txn = 128;
/* only process pages already in the bufferpool */
int gbl_pgmv_only_process_pages_in_bufferpool = 1;
/* handle overflow pages */
int gbl_pgmv_handle_overflow = 1;
int db_is_exiting(void);

/* number of deadlock errors */
int64_t gbl_pgmv_stat_ndeadlocks;
/* number of freelist sorts */
int64_t gbl_pgmv_stat_nflsorts;
/* number of overflow pages read */
int64_t gbl_pgmv_stat_novflreads;
/* number of overflo page swap attempts */
int64_t gbl_pgmv_stat_novflswapattempts;
/* number of overflow page swaps */
int64_t gbl_pgmv_stat_novflswaps;
/* number of pages read, including main and overflow pages */
int64_t gbl_pgmv_stat_npgreads;
/* number of pages skipped */
int64_t gbl_pgmv_stat_npgskips;
/* number of page swap attempts, including main and overflow pages */
int64_t gbl_pgmv_stat_npgswapattempts;
/* number of page swaps, including main and overflow pages */
int64_t gbl_pgmv_stat_npgswaps;
/* number of pages truncated */
int64_t gbl_pgmv_stat_npgtruncates;
/* number of file resizes */
int64_t gbl_pgmv_stat_nresizes;

/*
 * __db_rebuild_freelist --
 *  Shrink a database
 *
 * PUBLIC: int __db_rebuild_freelist __P((DB *, DB_TXN *));
 */
int
__db_rebuild_freelist(dbp, txn)
	DB *dbp;
	DB_TXN *txn;
{
	int ret, t_ret;
	size_t npages, pglistsz, notch, ii;
	int modified;

	DB_ENV *dbenv;
	DBC *dbc;
	DBMETA *meta;
	DB_MPOOLFILE *dbmfp;
	DB_LOCK metalock;
	PAGE *h;
	db_pgno_t pgno, *pglist, maxfreepgno, endpgno, oldlast;
	DB_LSN *pglsnlist;
	DBT pgnos, lsns;

	DB_LOGC *logc = NULL;
	DB_LSN firstlsn;
	DBT firstlog;
	int is_too_young;

	if (gbl_pgmv_verbose) {
		logmsg(LOGMSG_WARN, "%s: %s\n", __func__, dbp->fname);
	}

	dbenv = dbp->dbenv;
	dbmfp = dbp->mpf;

	LOCK_INIT(metalock);

	meta = NULL;
	pgno = PGNO_BASE_MD;
	pglist = NULL;
	pglsnlist = NULL;
	modified = 0;
	memset(&pgnos, 0, sizeof(DBT));
	memset(&lsns, 0, sizeof(DBT));
	npages = 0;
	pglistsz = 16; /* initial size */
	maxfreepgno = PGNO_INVALID;

	if ((ret = __os_malloc(dbenv, sizeof(db_pgno_t) * pglistsz, &pglist)) != 0)
		goto done;
	if ((ret = __os_malloc(dbenv, sizeof(DB_LSN) * pglistsz, &pglsnlist)) != 0)
		goto done;

	if ((ret = __db_cursor(dbp, txn, &dbc, 0)) != 0)
		return (ret);

	if ((ret = __db_lget(dbc, 0, pgno, DB_LOCK_WRITE, DB_LOCK_NOWAIT, &metalock)) != 0)
		goto done;
	if ((ret = PAGEGET(dbc, dbmfp, &pgno, 0, &meta)) != 0) {
		__db_pgerr(dbp, pgno, ret);
		goto done;
	}

	for (pgno = meta->free; pgno != PGNO_INVALID; ++npages) {
		if (db_is_exiting()) {
			logmsg(LOGMSG_WARN, "%s: we're asked to stop\n", __func__);
			ret = -1;
			goto done;
		}
		if (npages == pglistsz) {
			pglistsz <<= 1;
			if ((ret =  __os_realloc(dbenv, sizeof(db_pgno_t) * pglistsz, &pglist)) != 0)
				goto done;
			if ((ret =  __os_realloc(dbenv, sizeof(DB_LSN) * pglistsz, &pglsnlist)) != 0)
				goto done;
		}

		if (pgno > maxfreepgno)
			maxfreepgno = pgno;

		pglist[npages] = pgno;

		if ((ret = PAGEGET(dbc, dbmfp, &pgno, 0, &h)) != 0) {
			__db_pgerr(dbp, pgno, ret);
			goto done;
		}

		pglsnlist[npages] = LSN(h);
		pgno = NEXT_PGNO(h);

		if ((ret = PAGEPUT(dbc, dbmfp, h, 0)) != 0)
			goto done;
	}
	endpgno = pgno;

	if (gbl_pgmv_verbose) {
		logmsg(LOGMSG_WARN, "%s: %zu free pages collected:\n", __func__, npages);
		for (int i = 0; i != npages; ++i) {
			logmsg(LOGMSG_WARN, "%u ", pglist[i]);
			if (i == npages - 1)
				logmsg(LOGMSG_WARN, "\n");
		}
	}

	if (npages == 0) {
		if (gbl_pgmv_verbose)
			logmsg(LOGMSG_WARN, "%s: no free pages. there is nothing for us to do\n", __func__);
		goto done;
	}

	if (gbl_pgmv_verbose && maxfreepgno < meta->last_pgno) {
		logmsg(LOGMSG_WARN, "%s: no free pages at the end of the file. maxfreepgno %u last_pgno %u\n",
				__func__, maxfreepgno, meta->last_pgno);
	}

	/*
	 * Get the first log record. Do not truncate a page if it's LSN is greater than
	 * the first log record. It's okay if the actual first log record in the
	 * system advances after this.
	 */
	memset(&firstlog, 0, sizeof(DBT));
	firstlog.flags = DB_DBT_MALLOC;
	ret = dbenv->log_cursor(dbenv, &logc, 0);
	if (ret) {
		__db_err(dbenv, "%s: log_cursor error %d\n", __func__, ret);
		goto done;
	}
	ret = logc->get(logc, &firstlsn, &firstlog, DB_FIRST);
	if (ret) {
		__db_err(dbenv, "%s: log_c_get(FIRST) error %d\n", __func__, ret);
		goto done;
	}
	__os_free(dbenv, firstlog.data);

	/* Walk the file backwards, and find done where we can safely truncate */
	for (notch = npages, pgno = meta->last_pgno; notch > 0 && pglist[notch - 1] == pgno && pgno != PGNO_INVALID; --notch, --pgno) {
		if (gbl_pgmv_unsafe_db_resize)
			continue;

		if (db_is_exiting()) {
			logmsg(LOGMSG_WARN, "%s: we're asked to stop\n", __func__);
			ret = -1;
			goto done;
		}

		/*
		 * Chech if page LSN is still accessible.
		 * We can't safely truncate the page unless
		 * it's no longer referenced in the log
		 */
		if ((ret = PAGEGET(dbc, dbmfp, &pgno, 0, &h)) != 0) {
			__db_pgerr(dbp, pgno, ret);
			goto done;
		}
		is_too_young = (LSN(h).file >= firstlsn.file);
		if ((ret = PAGEPUT(dbc, dbmfp, h, 0)) != 0)
			goto done;

		if (is_too_young) {
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_WARN, "%s: %u is too young\n", __func__, pgno);
			}
			/* This is the pgno that we can truncate to, at most. No point continuing. */
			break;
		}
	}

	/* pglist[notch] is where in the freelist we can safely truncate. */
	if (gbl_pgmv_verbose) {
		if (notch == npages) {
			logmsg(LOGMSG_WARN, "%s: can't truncate: last free page %u last pg %u\n",
					__func__, pglist[notch - 1], meta->last_pgno);
		} else {
			logmsg(LOGMSG_WARN, "%s: last pgno %u truncation point (array index) %zu pgno %u\n",
					__func__, meta->last_pgno, notch, pglist[notch]);
		}
	}

	/* log the change */
	if (!DBC_LOGGING(dbc)) {
		LSN_NOT_LOGGED(LSN(meta));
	} else {
		/* network order */
		for (ii = 0; ii != npages; ++ii) {
			pglist[ii] = htonl(pglist[ii]);
			pglsnlist[ii].file = htonl(pglsnlist[ii].file);
			pglsnlist[ii].offset = htonl(pglsnlist[ii].offset);
		}

		pgnos.size = npages * sizeof(db_pgno_t);
		pgnos.data = pglist;
		lsns.size = npages * sizeof(DB_LSN);
		lsns.data = pglsnlist;

		/* TODO XXX: if unsafe-resize is disabled, can we reduce LSN's in this log record? */
		ret = __db_rebuild_freelist_log(dbp, txn, &LSN(meta), 0, &LSN(meta), PGNO_BASE_MD, meta->last_pgno, endpgno, &pgnos, &lsns, notch);
		if (ret != 0)
			goto done;

		/* host order */
		for (ii = 0; ii != npages; ++ii) {
			pglist[ii] = htonl(pglist[ii]);
			pglsnlist[ii].file = ntohl(pglsnlist[ii].file);
			pglsnlist[ii].offset = ntohl(pglsnlist[ii].offset);
		}
	}

	qsort(pglist, npages, sizeof(db_pgno_t), pgno_cmp);
	++gbl_pgmv_stat_nflsorts;

	if (gbl_pgmv_verbose) {
		logmsg(LOGMSG_WARN, "%s: freelist after sorting (%zu pages):\n", __func__, npages);
		for (int i = 0; i != npages; ++i) {
			logmsg(LOGMSG_WARN, "%u ", pglist[i]);
		}
		logmsg(LOGMSG_WARN, "\n");
	}

	/* rebuild the freelist, in page order */
	for (ii = 0; ii != notch; ++ii) {
		if (db_is_exiting()) {
			logmsg(LOGMSG_WARN, "%s: we're asked to stop\n", __func__);
			ret = -1;
			goto done;
		}

		pgno = pglist[ii];
		if ((ret = PAGEGET(dbc, dbmfp, &pgno, 0, &h)) != 0) {
			__db_pgerr(dbp, pgno, ret);
			goto done;
		}

		NEXT_PGNO(h) = (ii == notch - 1) ? endpgno : pglist[ii + 1];

		LSN(h) = LSN(meta);
		if ((ret = PAGEPUT(dbc, dbmfp, h, DB_MPOOL_DIRTY)) != 0)
			goto done;
	}

	/* Discard pages to be truncated from buffer pool */
	for (ii = notch; ii != npages; ++ii) {
		if (db_is_exiting()) {
			logmsg(LOGMSG_WARN, "%s: we're asked to stop\n", __func__);
			ret = -1;
			goto done;
		}
		pgno = pglist[ii];
		/*
		 * Probe the page. If it's paged in already, mark the page clean and
		 * discard it we don't want memp_sync to accidentally flush the page
		 * after we truncate, which would create a hole in the file.
		 */
		if ((ret = PAGEGET(dbc, dbmfp, &pgno, DB_MPOOL_PROBE, &h)) != 0)
			continue;
		if ((ret = PAGEPUT(dbc, dbmfp, h, DB_MPOOL_CLEAN | DB_MPOOL_DISCARD)) != 0)
			goto done;
	}

	/*
	 * Re-point the freelist to the smallest free page passed to us.
	 * If all pages in this range can be truncated, instead, point
	 * the freelist to the first free page after this range. It can
	 * be PGNO_INVALID if there is no more free page after this range.
	 */
	meta->free = notch > 0 ? pglist[0] : endpgno;
	modified = 1;

	if (notch < npages) {
		oldlast = meta->last_pgno;

		if (!DBC_LOGGING(dbc)) {
			LSN_NOT_LOGGED(LSN(meta));
		} else {
			ret = __db_resize_log(dbp, txn, &LSN(meta), 0, &LSN(meta), PGNO_BASE_MD, oldlast, pglist[notch] - 1);
			if (ret != 0)
				goto done;
		}

		/* pglist[notch] is where we will truncate. so point last_pgno to the page before this one. */
		meta->last_pgno = pglist[notch] - 1;

		/* also makes bufferpool aware */
		if ((ret = __memp_resize(dbmfp, meta->last_pgno)) != 0) {
			__db_err(dbenv, "%s: __memp_resize(%u) rc %d\n", __func__, meta->last_pgno, ret);
			goto done;
		}

		++gbl_pgmv_stat_nresizes;
		gbl_pgmv_stat_npgtruncates += oldlast - meta->last_pgno;
	}

done:
	if (ret == DB_LOCK_DEADLOCK) {
		/* Keep track of deadlocks. This gives us an idea of how impactful page mover is */
		++gbl_pgmv_stat_ndeadlocks;
	}

	__os_free(dbenv, pglist);
	__os_free(dbenv, pglsnlist);
	if (meta != NULL && (t_ret = PAGEPUT(dbc, dbmfp, meta, modified ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __TLPUT(dbc, metalock)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}

/*
 * __db_rebuild_freelist_pp --
 *	DB->rebuild_freelist pre/post processing.
 *
 * PUBLIC: int __db_rebuild_freelist_pp __P((DB *, DB_TXN *));
 */

int
__db_rebuild_freelist_pp(dbp, txn)
	DB *dbp;
	DB_TXN *txn;
{
	DB_ENV *dbenv;
	int handle_check, ret;

	dbenv = dbp->dbenv;

	PANIC_CHECK(dbenv);

	if (!F_ISSET(dbp, DB_AM_OPEN_CALLED)) {
		ret = __db_mi_open(dbenv, "DB->rebuild_freelist", 0);
		return (ret);
	}

	/* Check for consistent transaction usage. */
	if ((ret = __db_check_txn(dbp, txn, DB_LOCK_INVALIDID, 0)) != 0)
		return (ret);

	handle_check = IS_REPLICATED(dbenv, dbp);
	if (handle_check && (ret = __db_rep_enter(dbp, 1, 0)) != 0)
		return (ret);

	/* Shrink the file. */
	ret = __db_rebuild_freelist(dbp, txn);

	if (handle_check)
		__db_rep_exit(dbenv);
	return (ret);
}

/*
 * __db_pgswap --
 *
 * PUBLIC: int __db_pgswap __P((DB *, DB_TXN *));
 */
int
__db_pgswap(dbp, txn)
	DB *dbp;
	DB_TXN *txn;
{
	int ret, t_ret, ii, stack;
	int pglvl, unused;
	u_int8_t page_type;

	DBC *dbc;
	DB_ENV *dbenv;
	DB_MPOOLFILE *dbmfp;
	db_pgno_t pgno,
			  cpgno,	/* current page number when descending the btree */
			  newpgno,	/* page number of the new page */
			  ppgno,	/* parent page number */
			  prefpgno,	/* the page number referenced in the parent page */
			  logpgno;	/* used to print pgno on error */
	db_indx_t prefindx; /* position of `prefpgno' in the parent page */
	PAGE *h,	/* current page */
		 *ph,	/* its previous page */
		 *nh,	/* its next page */
		 *pp,	/* its parent page */
		 *np;	/* new page */

	DB_LOCK hl,		/* page lock for current page */
			pl,		/* page lock for prev */
			nl,		/* page lock for next */
			newl;	/* page lock for new page */
	int got_hl, got_pl, got_nl, got_newl;

	int num_pages_swapped = 0;
	int max = gbl_pgmv_max_num_pages_swapped_per_txn;
	PAGE **lfp; /* list of pages to be placed on freelist */
	db_pgno_t *lpgnofromfl; /* list of page numbers swapped in from freelist*/

	DB_LSN ret_lsn, /* new lsn */
		   *nhlsn,	/* lsn of next */
		   *phlsn,	/* lsn of prev */
		   *pplsn;	/* lsn of parent */

	BTREE_CURSOR *cp;
	EPG *epg;

	DBT hdr, dta, firstkey;

	++gbl_pgmv_stat_npgswapattempts;
	if (gbl_pgmv_verbose) {
		logmsg(LOGMSG_WARN, "%s: %s\n", __func__, dbp->fname);
	}

	dbenv = dbp->dbenv;
	dbmfp = dbp->mpf;
	dbc = NULL;

	h = ph = nh = pp = np = NULL;
	got_hl = got_pl = got_nl = got_newl = 0;
	stack = 0;

	lfp = NULL;
	lpgnofromfl = NULL;

	if (dbp->type != DB_BTREE) {
		ret = EINVAL;
		goto done;
	}

	if ((ret = __os_calloc(dbenv, max, sizeof(PAGE *), &lfp)) != 0) {
		__db_err(dbenv, "%s: __os_malloc: rc %d", __func__, ret);
		goto done;
	}

	if ((ret = __os_calloc(dbenv, max, sizeof(db_pgno_t), &lpgnofromfl)) != 0) {
		__db_err(dbenv, "%s: __os_malloc: rc %d", __func__, ret);
		goto done;
	}

	if ((ret = __db_cursor(dbp, txn, &dbc, 0)) != 0) {
		__db_err(dbenv, "%s: __db_cursor: rc %d", __func__, ret);
		goto done;
	}
	cp = (BTREE_CURSOR *)dbc->internal;

	/* Walk the file backwards and swap pages with a lower-numbered free page */
	for (__memp_last_pgno(dbmfp, &pgno), stack = 0; pgno >= 1; --pgno, ++gbl_pgmv_stat_npgreads, stack = 0) {
		if (gbl_pgmv_verbose)
			logmsg(LOGMSG_USER, "%s: checking pgno %u\n", __func__, pgno);

		if (db_is_exiting()) {
			logmsg(LOGMSG_WARN, "%s: we're asked to stop\n", __func__);
			ret = -1;
			goto done;
		}

		if (h != NULL) {
			cpgno = PGNO(h);
			ret = PAGEPUT(dbc, dbmfp, h, 0);
			h = NULL;
			if (ret != 0) {
				__db_err(dbenv, "%s: __memp_fput(%u): rc %d", __func__, cpgno, ret);
				goto done;
			}
		}

		if (got_hl) {
			got_hl = 0;
			if ((ret = __LPUT(dbc, hl)) != 0) {
				__db_err(dbenv, "%s: __LPUT(%u): rc %d", __func__, cpgno, ret);
				goto done;
			}
		}

		h = ph = nh = pp = np = NULL;
		nhlsn = phlsn = pplsn = NULL;
		ppgno = prefpgno = PGNO_INVALID;
		prefindx = 0;

		LOCK_INIT(hl);
		LOCK_INIT(pl);
		LOCK_INIT(nl);
		LOCK_INIT(newl);

		got_hl = got_pl = got_nl = got_newl = 0;

		if (bsearch(&pgno, lpgnofromfl, num_pages_swapped, sizeof(db_pgno_t), pgno_cmp) != NULL) {
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_WARN, "%s: pgno %u was just swapped in from freelist, skip it\n", __func__, pgno);
			}
			continue;
		}

		if (num_pages_swapped >= max) {
			if (gbl_pgmv_verbose)
				logmsg(LOGMSG_WARN, "%s: pages to be freed %d max %d\n", __func__, num_pages_swapped, max);
			break;
		}

		if ((ret = __db_lget(dbc, 0, pgno, DB_LOCK_READ, DB_LOCK_NOWAIT, &hl)) != 0) {
			__db_err(dbenv, "%s: __db_lget(%u): rc %d", __func__, pgno, ret);
			goto done;
		}

		got_hl = 1;

		if (gbl_pgmv_only_process_pages_in_bufferpool) {
			ret = PAGEGET(dbc, dbmfp, &pgno, DB_MPOOL_PROBE, &h);
			if (ret == DB_FIRST_MISS || ret == DB_PAGE_NOTFOUND) {
				if (gbl_pgmv_verbose) {
					logmsg(LOGMSG_WARN, "%s: pgno %u not found in bufferpool\n", __func__, pgno);
				}
				ret = 0;
				h = NULL;
				continue;
			}
		} else if ((ret = PAGEGET(dbc, dbmfp, &pgno, 0, &h)) != 0) {
			__db_pgerr(dbp, pgno, ret);
			h = NULL;
			goto done;
		}

		page_type = TYPE(h);
		if (page_type != P_LBTREE && page_type != P_IBTREE) {
			if (page_type != P_INVALID && page_type != P_OVERFLOW) {
				++gbl_pgmv_stat_npgskips;
				logmsg(LOGMSG_WARN, "%s: unsupported page type %d\n", __func__, page_type);
			}
			continue;
		}

		/* Try allocating a page from the freelist, without extending the file */
		if ((ret = __db_new_ex(dbc, page_type, &np, 1)) != 0) {
			__db_err(dbenv, "%s: __db_new: rc %d", __func__, ret);
			goto done;
		}

		if (np == NULL) {
			if (gbl_pgmv_verbose)
				logmsg(LOGMSG_INFO, "%s: free list is empty\n", __func__);
			goto done;
		}

		if (gbl_pgmv_verbose)
			logmsg(LOGMSG_WARN, "%s: use free pgno %u\n", __func__, PGNO(np));

		if (PGNO(np) > pgno) {
			if (gbl_pgmv_verbose)
				logmsg(LOGMSG_WARN, "%s: free page number is greater than this page!\n", __func__);
			/*
			 * The new page unfortunately has a higher page number than our page,
			 * Since we're scanning backwards from the back of the file, the next
			 * page will be even lower-numbered. It makes no sense to continue.
			 */
			ret = __db_free(dbc, np);
			np = NULL;
			goto done;
		}

		/* Grab a wlock on the new page */
		if ((ret = __db_lget(dbc, 0, PGNO(np), DB_LOCK_WRITE, 0, &newl)) != 0) {
			__db_err(dbenv, "%s: __db_lget(%u): rc %d", __func__, PGNO(np), ret);
			goto done;
		}
		got_newl = 1;

		memset(&firstkey, 0, sizeof(DBT));
		pglvl = LEVEL(h);

		/* descend from pgno till we hit a non-internal page */
		while (ret == 0 && ISINTERNAL(h)) {
			cpgno = GET_BINTERNAL(dbp, h, 0)->pgno;
			ret = PAGEPUT(dbc, dbmfp, h, 0);
			h = NULL;
			if (ret != 0) {
				__db_err(dbenv, "%s: __memp_fput(%u): rc %d", __func__, cpgno, ret);
				goto done;
			}

			got_hl = 0;
			if ((ret = __LPUT(dbc, hl)) != 0) {
				__db_err(dbenv, "%s: __LPUT(%u): rc %d", __func__, cpgno, ret);
				goto done;
			}

			if ((ret = __db_lget(dbc, 0, cpgno, DB_LOCK_READ, DB_LOCK_NOWAIT, &hl)) != 0) {
				__db_err(dbenv, "%s: __db_lget(%u): rc %d", __func__, cpgno, ret);
				goto done;
			}
			got_hl = 1;

			if ((ret = PAGEGET(dbc, dbmfp, &cpgno, 0, &h)) != 0) {
				__db_pgerr(dbp, cpgno, ret);
				h = NULL;
				goto done;
			}
		}

		if (ret != 0)
			goto done;
		if (!ISLEAF(h))
			goto done;
		if ((ret = __db_ret(dbp, h, 0, &firstkey, &firstkey.data, &firstkey.ulen)) != 0)
			goto done;
		if ((ret = __bam_search(dbc, PGNO_INVALID, &firstkey, S_WRITE | S_PARENT, pglvl, NULL, &unused)) != 0)
			goto done;
		stack = 1;

		/* Release my reference to this page, for __bam_search() pins the page */
		cpgno = PGNO(h);
		ret = PAGEPUT(dbc, dbmfp, h, 0);
		h = NULL;
		if (ret != 0) {
			__db_err(dbenv, "%s: __memp_fput(%u): rc %d", __func__, cpgno, ret);
			goto done;
		}

		got_hl = 0;
		if ((ret = __LPUT(dbc, hl)) != 0) {
			__db_err(dbenv, "%s: __LPUT(%u): rc %d", __func__, cpgno, ret);
			goto done;
		}

		if (cp->sp != cp->csp) { /* Have a parent page */
			epg = &cp->csp[-1];
			pp = epg->page;
			ppgno = PGNO(pp);
			pplsn = &LSN(pp);
			prefindx = epg->indx;
		}

		h = cp->csp->page;
		/* Now grab prev and next */
		if (PREV_PGNO(h) != PGNO_INVALID) {
			if ((ret = __db_lget(dbc, 0, PREV_PGNO(h), DB_LOCK_WRITE, DB_LOCK_NOWAIT, &pl)) != 0) {
				__db_err(dbenv, "%s: __db_lget(%u): rc %d", __func__, PREV_PGNO(h), ret);
				goto done;
			}
			got_pl = 1;
			if ((ret = PAGEGET(dbc, dbmfp, &PREV_PGNO(h), 0, &ph)) != 0) {
				ret = __db_pgerr(dbp, PREV_PGNO(h), ret);
				ph = NULL;
				goto done;
			}
			phlsn = &LSN(ph);
		}

		if (NEXT_PGNO(h) != PGNO_INVALID) {
			if ((ret = __db_lget(dbc, 0, NEXT_PGNO(h), DB_LOCK_WRITE, DB_LOCK_NOWAIT, &nl)) != 0) {
				__db_err(dbenv, "%s: __db_lget(%u): rc %d", __func__, NEXT_PGNO(h), ret);
				goto done;
			}
			got_nl = 1;
			if ((ret = PAGEGET(dbc, dbmfp, &NEXT_PGNO(h), 0, &nh)) != 0) {
				nh = NULL;
				ret = __db_pgerr(dbp, NEXT_PGNO(h), ret);
				goto done;
			}
			nhlsn = &LSN(nh);
		}

		/* Get old page header and data. */
		memset(&hdr, 0, sizeof(DBT));
		memset(&dta, 0, sizeof(DBT));

		hdr.data = h;
		hdr.size = LOFFSET(dbp, h);
		dta.data = (u_int8_t *)h + HOFFSET(h);
		dta.size = dbp->pgsize - HOFFSET(h);

		if (DBC_LOGGING(dbc)) {
			ret = __db_pg_swap_log(dbp, txn, &ret_lsn, 0,
					PGNO(h), &LSN(h), &hdr, &dta, /* old page */
					NEXT_PGNO(h), nhlsn, /* sibling page, if any */
					PREV_PGNO(h), phlsn, /* sibling page, if any */
					ppgno, pplsn, prefindx, /* parent page, if any */
					PGNO(np), &LSN(np) /* new page to swap with */);
			if (ret != 0)
				goto done;
		} else {
			LSN_NOT_LOGGED(ret_lsn);
		}

		/* update LSN */
		LSN(h) = ret_lsn;
		LSN(np) = ret_lsn;
		if (nh != NULL)
			LSN(nh) = ret_lsn;
		if (ph != NULL)
			LSN(ph) = ret_lsn;
		if (pp != NULL)
			LSN(pp) = ret_lsn;

		if (gbl_pgmv_verbose) {
			logmsg(LOGMSG_WARN, "%s: swapping pgno %u with free page %u\n", __func__, PGNO(h), PGNO(np));
		}

		/* copy content to new page and fix pgno */
		newpgno = PGNO(np);
		memcpy(np, h, dbp->pgsize);
		PGNO(np) = newpgno;

		if ((ret = __memp_fset(dbmfp, np, DB_MPOOL_DIRTY)) != 0) {
			__db_err(dbenv, "%s: __memp_fset(%u): rc %d", __func__, newpgno, ret);
			goto done;
		}

		/*
		 * Empty old page and remove prefix. This ensures that
		 * we call into the non-data version of db_free()
		 */
		HOFFSET(h) = dbp->pgsize;
		NUM_ENT(h) = 0;
		CLR_PREFIX(h);

		/* Place the page on the to-be-freed list, that gets freed after the while loop.
		 * It ensures higher page numbers won't be placed on the front of the list. */
		lfp[num_pages_swapped] = h;
		h = NULL;

		lpgnofromfl[num_pages_swapped] = newpgno;
		++num_pages_swapped;
		qsort(lpgnofromfl, num_pages_swapped, sizeof(db_pgno_t), pgno_cmp);

		/* relink next */
		if (nh != NULL) {
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_WARN, "%s: relinking pgno %u to the right of %u\n", __func__, PGNO(nh), newpgno);
			}
			logpgno = PGNO(nh);
			PREV_PGNO(nh) = newpgno;
			ret = PAGEPUT(dbc, dbmfp, nh, DB_MPOOL_DIRTY);
			nh = NULL;
			if (ret != 0) {
				__db_err(dbenv, "%s: __memp_fput(%u): rc %d", __func__, logpgno, ret);
				goto done;
			}
			got_nl = 0;
			if ((ret = __TLPUT(dbc, nl)) != 0) {
				__db_err(dbenv, "%s: __TLPUT(%u): rc %u", __func__, PGNO(nh), ret);
				goto done;
			}
		}

		/* relink prev */
		if (ph != NULL) {
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_WARN, "%s: relinking pgno %u to the left of %u\n", __func__, PGNO(ph), newpgno);
			}
			logpgno = PGNO(ph);
			NEXT_PGNO(ph) = newpgno;
			ret = PAGEPUT(dbc, dbmfp, ph, DB_MPOOL_DIRTY);
			ph = NULL;
			if (ret != 0) {
				__db_err(dbenv, "%s: __memp_fput(%u): rc %d", __func__, logpgno, ret);
				goto done;
			}
			got_pl = 0;
			if ((ret = __TLPUT(dbc, pl)) != 0) {
				__db_err(dbenv, "%s: __TLPUT(%u): rc %d", __func__, PGNO(ph), ret);
				goto done;
			}
		}

		/* update parent */
		if (cp->sp != cp->csp) {
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_WARN, "%s: update parent %u reference to %u\n", __func__, PGNO(pp), newpgno);
			}
			GET_BINTERNAL(dbp, pp, prefindx)->pgno = newpgno;

			if ((ret = __memp_fset(dbmfp, pp, DB_MPOOL_DIRTY)) != 0) {
				__db_err(dbenv, "%s: __memp_fset(%u): rc %d", __func__, PGNO(pp), ret);
				goto done;
			}
		}

		/*
		 * Old page is placed on the to-be-freed list, swap in the new page.
		 * We still retain the the old page's lock in the cursor stack,
		 * and __bam_stkrel will take care of that lock. We manually release
		 * the new page's lock here.
		 */
		cp->csp->page = np;
		np = NULL;
		got_newl = 0;
		if ((ret = __TLPUT(dbc, newl)) != 0) {
			__db_err(dbenv, "%s: __TLPUT(%u): rc %d", __func__, newpgno, ret);
			goto done;
		}
		if ((ret = __bam_stkrel(dbc, STK_CLRDBC)) != 0) {
			stack = 0;
			__db_err(dbenv, "%s: __bam_stkrel(): rc %d", __func__, ret);
			goto done;
		}

		++gbl_pgmv_stat_npgswaps;
	} /* end of the big for-loop */

done:
	if (ret == 0) {
		/*
		 * The list is most likely sorted in a descending order of pgno,
		 * for we scanned the file backwards. Free pages from the head of
		 * the list (ie from the largest pgno), so that smaller pages
		 * are placed on the front of the freelist.
		 */
		if (gbl_pgmv_verbose) {
			logmsg(LOGMSG_USER, "%s: num pages swapped %u\n", __func__, num_pages_swapped);
		}
		for (ii = 0; ii != num_pages_swapped; ++ii) {
			if ((ret = __db_free(dbc, lfp[ii])) != 0) {
				__db_err(dbenv, "%s: __db_free(%u): rc %d", __func__, PGNO(lfp[ii]), ret);
				break;
			}
		}
	} else {
		/*
		 * We're going to abort this transaction. The pages are still pinned by us,
		 * so make sure that they're released.
		 */
		if (gbl_pgmv_verbose) {
			logmsg(LOGMSG_USER, "%s: num pages that need reverting %u\n", __func__, num_pages_swapped);
		}
		for (ii = 0; ii != num_pages_swapped; ++ii) {
			t_ret = PAGEPUT(dbc, dbmfp, lfp[ii], 0);
			if (t_ret != 0)
				__db_err(dbenv, "%s: __db_free(%u): rc %d", __func__, PGNO(lfp[ii]), ret);
		}
	}
	__os_free(dbenv, lfp);
	__os_free(dbenv, lpgnofromfl);

	if (!stack && h != NULL && (t_ret = PAGEPUT(dbc, dbmfp, h, 0)) != 0 && ret == 0)
		ret = t_ret;
	if (got_hl && (t_ret = __TLPUT(dbc, hl)) != 0 && ret == 0)
		ret = t_ret;
	if (nh != NULL && (t_ret = PAGEPUT(dbc, dbmfp, nh, 0)) != 0 && ret == 0)
		ret = t_ret;
	if (got_nl && (t_ret = __TLPUT(dbc, nl)) != 0 && ret == 0)
		ret = t_ret;
	if (ph != NULL && (t_ret = PAGEPUT(dbc, dbmfp, ph, 0)) != 0 && ret == 0)
		ret = t_ret;
	if (got_pl && (t_ret = __TLPUT(dbc, pl)) != 0 && ret == 0)
		ret = t_ret;
	if (np != NULL && (t_ret = PAGEPUT(dbc, dbmfp, np, 0)) != 0 && ret == 0)
		ret = t_ret;
	if (got_newl && (t_ret = __TLPUT(dbc, newl)) != 0 && ret == 0)
		ret = t_ret;
	if (stack && (t_ret = __bam_stkrel(dbc, STK_CLRDBC)) != 0 && ret == 0)
		ret = t_ret;
	if (dbc != NULL && (t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

/*
 * __db_pgswap_pp --
 *     DB->pgswap pre/post processing.
 *
 * PUBLIC: int __db_pgswap_pp __P((DB *, DB_TXN *));
 */
int
__db_pgswap_pp(dbp, txn)
	DB *dbp;
	DB_TXN *txn;
{
	DB_ENV *dbenv;
	int handle_check, ret;

	dbenv = dbp->dbenv;

	PANIC_CHECK(dbenv);

	if (!F_ISSET(dbp, DB_AM_OPEN_CALLED)) {
		ret = __db_mi_open(dbenv, "DB->pgswap", 0);
		return (ret);
	}

	/* Check for consistent transaction usage. */
	if ((ret = __db_check_txn(dbp, txn, DB_LOCK_INVALIDID, 0)) != 0)
		return (ret);

	handle_check = IS_REPLICATED(dbenv, dbp);
	if (handle_check && (ret = __db_rep_enter(dbp, 1, 0)) != 0)
		return (ret);

	ret = __db_pgswap(dbp, txn);

	if (handle_check)
		__db_rep_exit(dbenv);
	return (ret);
}

/*
 * __db_evict_from_cache --
 *
 * PUBLIC: int __db_evict_from_cache __P((DB *, DB_TXN *));
 */
int
__db_evict_from_cache(dbp, txn)
	DB *dbp;
	DB_TXN *txn;
{
	int ret, t_ret;

	DBC *dbc;
	DB_ENV *dbenv;
	DB_MPOOLFILE *dbmfp;
	db_pgno_t pgno, last_pgno;
	PAGE *h;

	DB_LOCK hl;
	int got_hl;

	dbenv = dbp->dbenv;
	dbmfp = dbp->mpf;
	dbc = NULL;

	pgno = 0;
	h = NULL;
	got_hl = 0;

	if (dbp->type != DB_BTREE) {
		ret = EINVAL;
		goto done;
	}

	if ((ret = __db_cursor(dbp, txn, &dbc, 0)) != 0) {
		__db_err(dbenv, "%s: __db_cursor: rc %d", __func__, ret);
		goto done;
	}

	for (__memp_last_pgno(dbmfp, &last_pgno); pgno <= last_pgno; ++pgno) {
		if ((ret = __db_lget(dbc, 0, pgno, DB_LOCK_WRITE, 0, &hl)) != 0) {
			__db_err(dbenv, "%s: __db_lget(%u): rc %d", __func__, pgno, ret);
			goto done;
		}
		got_hl = 1;

		ret = PAGEGET(dbc, dbmfp, &pgno, DB_MPOOL_PROBE, &h);
		if (ret == DB_PAGE_NOTFOUND || ret == DB_FIRST_MISS) {
			continue;
		}
		ret = PAGEPUT(dbc, dbmfp, h, DB_MPOOL_EVICT);
		h = NULL;
		if (ret != 0) {
			__db_err(dbenv, "%s: __memp_fput(%u, evict): rc %d", __func__, PGNO(h), ret);
			goto done;
		}

		got_hl = 0;
		if ((ret = __LPUT(dbc, hl)) != 0) {
			__db_err(dbenv, "%s: __LPUT(%d): rc %d", __func__, pgno, ret);
			goto done;
		}
	}

done:
	if (h != NULL && (t_ret = PAGEPUT(dbc, dbmfp, h, 0)) != 0 && ret == 0)
		ret = t_ret;
	if (got_hl && (t_ret = __LPUT(dbc, hl)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

/*
 * __db_evict_from_cache_pp --
 *     DB->evict_from_cache pre/post processing.
 *
 * PUBLIC: int __db_evict_from_cache_pp __P((DB *, DB_TXN *));
 */
int
__db_evict_from_cache_pp(dbp, txn)
	DB *dbp;
	DB_TXN *txn;
{
	DB_ENV *dbenv;
	int handle_check, ret;

	dbenv = dbp->dbenv;

	PANIC_CHECK(dbenv);

	if (!F_ISSET(dbp, DB_AM_OPEN_CALLED)) {
		ret = __db_mi_open(dbenv, "DB->evict_from_cache", 0);
		return (ret);
	}

	/* Check for consistent transaction usage. */
	if ((ret = __db_check_txn(dbp, txn, DB_LOCK_INVALIDID, 0)) != 0)
		return (ret);

	handle_check = IS_REPLICATED(dbenv, dbp);
	if (handle_check && (ret = __db_rep_enter(dbp, 1, 0)) != 0)
		return (ret);

	ret = __db_evict_from_cache(dbp, txn);

	if (handle_check)
		__db_rep_exit(dbenv);
	return (ret);
}

/*
 * __db_pgswap_overflow --
 *
 * PUBLIC: int __db_pgswap_overflow __P((DB *, DB_TXN *));
 */
int
__db_pgswap_overflow(dbp, txn)
	DB *dbp;
	DB_TXN *txn;
{
	DB_ENV *dbenv;
	DBC *dbc;
	DB_MPOOLFILE *dbmfp;
	PAGE *h, **lovfl;
	DB_LOCK lock;
	db_pgno_t pgno, logpgno, newpgno;

	BOVERFLOW *bo;
	DBT dta;

	/* for logging */
	db_pgno_t mainpgno;
	DB_LSN *pmainpglsn;
	db_indx_t mainindx;
	DB_LSN ret_lsn, *ovfl_nhlsn, *ovfl_phlsn; 

	db_pgno_t ovfl_pgno, next_ovfl_pgno, prev_ovfl_pgno;
	PAGE *ovfl_h, *ovfl_newh, *ovfl_ph, *ovfl_nh;

	int ret, t_ret, ii, nents;
	int have_lock;
	int max, num_swapped;

	dbc = NULL;
	dbenv = dbp->dbenv;
	dbmfp = dbp->mpf;
	LOCK_INIT(lock);
	h = ovfl_h = ovfl_newh = ovfl_ph = ovfl_nh = NULL;

	have_lock = 0;
	max = gbl_pgmv_max_num_pages_swapped_per_txn;
	num_swapped = 0;
	lovfl = NULL;
	ret = t_ret = 0;

	++gbl_pgmv_stat_novflswapattempts;
	if (gbl_pgmv_verbose) {
		logmsg(LOGMSG_WARN, "%s: %s\n", __func__, dbp->fname);
	}

	if (dbp->type != DB_BTREE) {
		ret = EINVAL;
		goto done;
	}

	if ((ret = __db_cursor(dbp, txn, &dbc, 0)) != 0) {
		__db_err(dbenv, "%s: __db_cursor: rc %d", __func__, ret);
		goto done;
	}

	if ((ret = __os_calloc(dbenv, max, sizeof(PAGE *), &lovfl)) != 0) {
		__db_err(dbenv, "%s: __os_malloc: rc %d", __func__, ret);
		goto done;
	}

	/*
	 * Walk the file, find overflow page references and follow the overflow page chains.
	 * Locking is simpler in this function: we only need to acquire write lock on the
	 * main page. The write lock unfortunately may be longer duration: we need to hold
	 * onto the lock while traversing the overflow page chain. */
	for (__memp_last_pgno(dbmfp, &pgno); pgno >= 1; --pgno, ++gbl_pgmv_stat_npgreads) {
		if (gbl_pgmv_verbose)
			logmsg(LOGMSG_USER, "%s: checking overflow items on pgno %u\n", __func__, pgno);

		if (db_is_exiting() || !gbl_pgmv_handle_overflow) {
			logmsg(LOGMSG_WARN, "%s: we're asked to stop\n", __func__);
			ret = -1;
			goto done;
		}

		if (h != NULL) {
			logpgno = PGNO(h);
			ret = PAGEPUT(dbc, dbmfp, h, 0);
			h = NULL;
			if (ret != 0) {
				__db_err(dbenv, "%s: __memp_fput(%u): rc %d", __func__, logpgno, ret);
				goto done;
			}
		}
		if (have_lock) {
			have_lock = 0;
			if ((ret = __LPUT(dbc, lock)) != 0) {
				__db_err(dbenv, "%s: __LPUT: rc %d", __func__, ret);
				goto done;
			}
		}

		if ((ret = __db_lget(dbc, 0, pgno, DB_LOCK_WRITE, DB_LOCK_NOWAIT, &lock)) != 0) {
			__db_err(dbenv, "%s: __db_lget(%u): rc %d", __func__, pgno, ret);
			goto done;
		}
		have_lock = 1;

		if (gbl_pgmv_only_process_pages_in_bufferpool) {
			ret = PAGEGET(dbc, dbmfp, &pgno, DB_MPOOL_PROBE, &h);
			if (ret == DB_FIRST_MISS || ret == DB_PAGE_NOTFOUND) {
				ret = 0;
				h = NULL;
				continue;
			}
		} else if ((ret = PAGEGET(dbc, dbmfp, &pgno, 0, &h)) != 0) {
			__db_pgerr(dbp, pgno, ret);
			h = NULL;
			goto done;
		}

		if (!ISLEAF(h))
			continue;

		for (ii = 1, nents = NUM_ENT(h); ret == 0 && ii < nents; ii += 2) {
			if (B_TYPE(GET_BKEYDATA(dbp, h, ii)) != B_OVERFLOW)
				continue;
			bo = GET_BOVERFLOW(dbp, h, ii);
			ovfl_pgno = bo->pgno;

			/* walk the overflow chain */
			for (; ovfl_pgno != PGNO_INVALID; ++gbl_pgmv_stat_npgreads, ++gbl_pgmv_stat_novflreads) {
				if (gbl_pgmv_verbose)
					logmsg(LOGMSG_USER, "%s: checking overflow chain pgno %u (main pgno %u)\n", __func__, ovfl_pgno, pgno);

				if (db_is_exiting() || !gbl_pgmv_handle_overflow) {
					logmsg(LOGMSG_WARN, "%s: we're asked to stop\n", __func__);
					ret = -1;
					goto done;
				}

				if (ovfl_h != NULL) {
					logpgno = PGNO(ovfl_h);
					ret = PAGEPUT(dbc, dbmfp, ovfl_h, 0);
					ovfl_h = NULL;
					if (ret != 0) {
						__db_err(dbenv, "%s: __memp_fput(%u): rc %d", __func__, logpgno, ret);
						goto done;
					}
				}

				if (ovfl_nh != NULL) {
					logpgno = PGNO(ovfl_nh);
					ret = PAGEPUT(dbc, dbmfp, ovfl_nh, 0);
					ovfl_nh = NULL;
					if (ret != 0) {
						__db_err(dbenv, "%s: __memp_fput(%u): rc %d", __func__, logpgno, ret);
						goto done;
					}
				}

				if (ovfl_ph != NULL) {
					logpgno = PGNO(ovfl_ph);
					ret = PAGEPUT(dbc, dbmfp, ovfl_ph, 0);
					ovfl_ph = NULL;
					if (ret != 0) {
						__db_err(dbenv, "%s: __memp_fput(%u): rc %d", __func__, logpgno, ret);
						goto done;
					}
				}

				/* these keep track of the reference on the main page of the overflow chain */
				mainpgno = PGNO(h);
				pmainpglsn = &LSN(h);
				mainindx = (db_indx_t)ii;

				/* thse keep track of the overflow chain */
				next_ovfl_pgno = prev_ovfl_pgno = PGNO_INVALID;
				ovfl_h = ovfl_newh = ovfl_ph = ovfl_nh = NULL;
				ovfl_phlsn = ovfl_nhlsn = NULL;
				ZERO_LSN(ret_lsn);

				if (num_swapped >= max)
					goto done;

				if (gbl_pgmv_only_process_pages_in_bufferpool) {
					/*
					 * we are asked to only work with pages in the bufferpool.
					 * check if this page and its siblings are already there.
					 */
					ret = PAGEGET(dbc, dbmfp, &ovfl_pgno, DB_MPOOL_PROBE, &ovfl_h);
					if (ret == DB_FIRST_MISS || ret == DB_PAGE_NOTFOUND) {
						ret = 0;
						ovfl_h = NULL;
						continue;
					}
					ovfl_pgno = next_ovfl_pgno = NEXT_PGNO(ovfl_h);
					if (next_ovfl_pgno != PGNO_INVALID) {
						ret = PAGEGET(dbc, dbmfp, &next_ovfl_pgno, DB_MPOOL_PROBE, &ovfl_nh);
						if (ret == DB_FIRST_MISS || ret == DB_PAGE_NOTFOUND) {
							ret = 0;
							ovfl_nh = NULL;
							continue;
						}
						ovfl_nhlsn = &LSN(ovfl_nh);
					}
					prev_ovfl_pgno = PREV_PGNO(ovfl_h);
					if (prev_ovfl_pgno != PGNO_INVALID) {
						ret = PAGEGET(dbc, dbmfp, &prev_ovfl_pgno, DB_MPOOL_PROBE, &ovfl_ph);
						if (ret == DB_FIRST_MISS || ret == DB_PAGE_NOTFOUND) {
							ret = 0;
							ovfl_ph = NULL;
							continue;
						}
						ovfl_phlsn = &LSN(ovfl_ph);
					}
				} else {
					ret = PAGEGET(dbc, dbmfp, &ovfl_pgno, 0, &ovfl_h);
					if (ret != 0) {
						__db_pgerr(dbp, ovfl_pgno, ret);
						ovfl_h = NULL;
						goto done;
					}
					ovfl_pgno = next_ovfl_pgno = NEXT_PGNO(ovfl_h);
					if (next_ovfl_pgno != PGNO_INVALID) {
						ret = PAGEGET(dbc, dbmfp, &next_ovfl_pgno, 0, &ovfl_nh);
						if (ret != 0) {
							__db_pgerr(dbp, next_ovfl_pgno, ret);
							ovfl_nh = NULL;
							goto done;
						}
						ovfl_nhlsn = &LSN(ovfl_nh);
					}
					prev_ovfl_pgno = PREV_PGNO(ovfl_h);
					if (prev_ovfl_pgno != PGNO_INVALID) {
						ret = PAGEGET(dbc, dbmfp, &prev_ovfl_pgno, 0, &ovfl_ph);
						if (ret != 0) {
							__db_pgerr(dbp, prev_ovfl_pgno, ret);
							ovfl_ph = NULL;
							goto done;
						}
						ovfl_phlsn = &LSN(ovfl_ph);
					}
				}

				if (OV_REF(ovfl_h) > 1) {
					if (gbl_pgmv_verbose)
						logmsg(LOGMSG_INFO, "%s: page %u ref-count %d is greater than 1\n", __func__, PGNO(ovfl_h), OV_REF(ovfl_h));
					continue;
				}

				if ((ret = __db_new_ex(dbc, P_OVERFLOW, &ovfl_newh, 1)) != 0) {
					__db_err(dbenv, "%s: __db_new: rc %d", __func__, ret);
					goto done;
				}

				if (ovfl_newh == NULL) {
					if (gbl_pgmv_verbose)
						logmsg(LOGMSG_INFO, "%s: free list is empty\n", __func__);
					goto done;
				}

				if (PGNO(ovfl_newh) > PGNO(ovfl_h)) {
					if (gbl_pgmv_verbose)
						logmsg(LOGMSG_INFO, "%s: free page number %u is greater than page %u\n", __func__, PGNO(ovfl_newh), PGNO(ovfl_h));
					OV_LEN(ovfl_newh) = 0;
					ret = __db_free(dbc, ovfl_newh);
					ovfl_newh = NULL;
					continue;
				}

				if (gbl_pgmv_verbose)
					logmsg(LOGMSG_WARN, "%s: swapping overflow pgno %u with free page %u\n", __func__, PGNO(ovfl_h), PGNO(ovfl_newh));

				/* log this */
				memset(&dta, 0, sizeof(DBT));
				dta.data = ovfl_h;
				dta.size = P_OVERHEAD(dbp) + OV_LEN(ovfl_h);

				if (DBC_LOGGING(dbc)) {
					ret = __db_pg_swap_overflow_log(dbp, txn, &ret_lsn, 0,
							PGNO(ovfl_h), &LSN(ovfl_h), &dta,
							NEXT_PGNO(ovfl_h), ovfl_nhlsn,
							PREV_PGNO(ovfl_h), ovfl_phlsn,
							mainpgno, pmainpglsn, mainindx,
							PGNO(ovfl_newh), &LSN(ovfl_newh));
					if (ret != 0)
						goto done;
				} else {
					LSN_NOT_LOGGED(ret_lsn);
				}

				LSN(ovfl_h) = ret_lsn;
				LSN(ovfl_newh) = ret_lsn;
				if (ovfl_nh != NULL)
					LSN(ovfl_nh) = ret_lsn;
				if (ovfl_ph != NULL)
					LSN(ovfl_ph) = ret_lsn;
				else
					LSN(h) = ret_lsn;

				/* copy the content of the old page to new page, and unpin the new page */
				newpgno = PGNO(ovfl_newh);
				memcpy(ovfl_newh, dta.data, dta.size);
				/* fix page number and links */
				PGNO(ovfl_newh) = newpgno;
				ret = PAGEPUT(dbc, dbmfp, ovfl_newh, DB_MPOOL_DIRTY);
				ovfl_newh = NULL;
				if (ret != 0) {
					__db_err(dbenv, "%s: __memp_fput(%u): rc %d", __func__, newpgno, ret);
					goto done;
				}

				/* empty old page */
				OV_LEN(ovfl_h) = 0;
				lovfl[num_swapped++] = ovfl_h;
				ovfl_h = NULL;

				/* relink next */
				if (ovfl_nh != NULL) {
					if (gbl_pgmv_verbose)
						logmsg(LOGMSG_WARN, "%s: relinking pgno %u -> %u\n", __func__, newpgno, PGNO(ovfl_nh));
					logpgno = PGNO(ovfl_nh);
					PREV_PGNO(ovfl_nh) = newpgno;
					ret = PAGEPUT(dbc, dbmfp, ovfl_nh, DB_MPOOL_DIRTY);
					ovfl_nh = NULL;
					if (ret != 0) {
						__db_err(dbenv, "%s: __memp_fput(%u): rc %d", __func__, logpgno, ret);
						goto done;
					}
				}

				/* relink prev. if we're the 1st page on the overflow chain, fix up the main page instead */
				if (ovfl_ph == NULL) {
					if (gbl_pgmv_verbose)
						logmsg(LOGMSG_WARN, "%s: modifying ref (%d) on main page %u to %u\n", __func__, mainindx, PGNO(h), newpgno);
					bo->pgno = newpgno;
					if ((ret = __memp_fset(dbmfp, h, DB_MPOOL_DIRTY)) != 0) {
						__db_err(dbenv, "%s: __memp_fset(%u): rc %d", __func__, newpgno, ret);
						goto done;
					}
				} else {
					if (gbl_pgmv_verbose)
						logmsg(LOGMSG_WARN, "%s: relinking pgno %u <- %u\n", __func__, PGNO(ovfl_ph), newpgno);
					logpgno = PGNO(ovfl_ph);
					NEXT_PGNO(ovfl_ph) = newpgno;
					ret = PAGEPUT(dbc, dbmfp, ovfl_ph, DB_MPOOL_DIRTY);
					ovfl_ph = NULL;
					if (ret != 0) {
						__db_err(dbenv, "%s: __memp_fput(%u): rc %d", __func__, logpgno, ret);
						goto done;
					}
				}

				++gbl_pgmv_stat_npgswaps;
				++gbl_pgmv_stat_novflswaps;
			} /* end of walk the overflow chain */
		} /* end of walk the entries on the main page */
	} /* end of walk the btree */
done:
	ii = num_swapped;
	if (ret == 0) {
		if (gbl_pgmv_verbose) 
			logmsg(LOGMSG_USER, "%s: num overflow pages swapped %u\n", __func__, num_swapped);
		/*
		 * Sort the list and free from the back of the list,
		 * so that smaller pages will be placed in the front
		 */
		qsort(lovfl, num_swapped, sizeof(PAGE *), PAGE_cmp);
		for (ii = num_swapped; --ii >= 0;) {
			OV_LEN(lovfl[ii]) = 0;
			if ((ret = __db_free(dbc, lovfl[ii])) != 0) {
				__db_err(dbenv, "%s: __db_free(%u): rc %d", __func__, PGNO(lovfl[ii]), ret);
				break;
			}
		}
	}

	if (ret != 0) {
		if (gbl_pgmv_verbose)
			logmsg(LOGMSG_USER, "%s: num overflow pages that need reverting %u\n", __func__, ii);
		while (--ii >= 0) {
			t_ret = PAGEPUT(dbc, dbmfp, lovfl[ii], 0);
			if (t_ret != 0)
				__db_err(dbenv, "%s: __db_free(%u): rc %d", __func__, PGNO(lovfl[ii]), t_ret);
		}
	}
	__os_free(dbenv, lovfl);

	if (h != NULL && (t_ret = PAGEPUT(dbc, dbmfp, h, 0)) != 0 && ret == 0)
		ret = t_ret;
	if (ovfl_h != NULL && (t_ret = PAGEPUT(dbc, dbmfp, ovfl_h, 0)) != 0 && ret == 0)
		ret = t_ret;
	if (ovfl_nh != NULL && (t_ret = PAGEPUT(dbc, dbmfp, ovfl_nh, 0)) != 0 && ret == 0)
		ret = t_ret;
	if (ovfl_ph != NULL && (t_ret = PAGEPUT(dbc, dbmfp, ovfl_ph, 0)) != 0 && ret == 0)
		ret = t_ret;
	if (dbc != NULL && (t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}

/*
 * __db_pgswap_overflow_pp --
 *     DB->pgswap_overflow pre/post processing.
 *
 * PUBLIC: int __db_pgswap_overflow_pp __P((DB *, DB_TXN *));
 */
int
__db_pgswap_overflow_pp(dbp, txn)
	DB *dbp;
	DB_TXN *txn;
{
	DB_ENV *dbenv;
	int handle_check, ret;

	dbenv = dbp->dbenv;

	PANIC_CHECK(dbenv);

	if (!F_ISSET(dbp, DB_AM_OPEN_CALLED)) {
		ret = __db_mi_open(dbenv, "DB->pgswap_overflow", 0);
		return (ret);
	}

	/* Check for consistent transaction usage. */
	if ((ret = __db_check_txn(dbp, txn, DB_LOCK_INVALIDID, 0)) != 0)
		return (ret);

	handle_check = IS_REPLICATED(dbenv, dbp);
	if (handle_check && (ret = __db_rep_enter(dbp, 1, 0)) != 0)
		return (ret);

	ret = __db_pgswap_overflow(dbp, txn);

	if (handle_check)
		__db_rep_exit(dbenv);
	return (ret);
}
