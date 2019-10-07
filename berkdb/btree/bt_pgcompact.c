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
/* sys */
#include <alloca.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <stdint.h>

#ifdef PGCOMP_DBG
#include <pthread.h>
#endif

#endif

/* common */
#include "db_int.h"
#include "dbinc/db_am.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/log.h"
#include "dbinc/lock.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"

/* precious */
#include "bt_cache.h"
#include "bt_prefix.h"

#include <lz4.h>
#include "thdpool.h"

#if LZ4_VERSION_NUMBER < 10701
#define LZ4_compress_default LZ4_compress_limitedOutput
#endif


#define P_UNKNOWN 0x00
#define P_L2R     0x01	/* Merge scan from left to right */
#define P_R2L     0x02	/* Merge scan from right to left */
#define P_BI      0x04	/* Bi-directional - L2R then R2L */
#define NUM_MAX_RETRIES 5

#ifdef PGCOMP_DBG
enum {
	 REASON_EXIT = 0
	,REASON_OFF
	,REASON_DRYRUN
	,REASON_ROOT
	,REASON_DUP
	,REASON_KDEL
	,REASON_NLEAF
	,REASON_EMPTY
	,REASON_FULL
	,REASON_PARNT
	,REASON_EDGE
	,REASON_NFIT
	,REASON_TURN
	,REASON_LCK
	,REASON_LSN
	,REASON_RETRY
	,REASON_MAX_RETRY
	,REASON_RESHAPE
	,REASON_MAX_PGS
	,REASON_NBACKSCAN
	,REASON_WHOAH
	,REASON_WHOAH_UNCOMP
	,REASON_WHOAH_COMP
	,REASON_WHOAH_PFXREC
};

static const char *reasons[] = {
	 "Exiting..."
	,"Feature turned off"
	,"In dry-run mode"
	,"Page is root node"
	,"Page has off-page dups"
	,"Key is deleted"
	,"Page is internal"
	,"Page is empty"
	,"Page is full"
	,"Parent has too few children"
	,"Reached leftmost/rightmost"
	,"Does not fit"
	,"Turn around"
	,"Would require too many page locks"
	,"Page LSN-s changed"
	,"Doing retry"
	,"Have done maximum retries"
	,"Subtree reshaped during decompression"
	,"Exceeded max number of pages per txn"
	,"Backward scan is disabled"
	,"WHOAH!!!"
	,"WHOAH!!! Merged 2 uncompressed pages"
	,"WHOAH!!! Merged 2 compressed pages"
	,"WHOAH!!! Moved prefix'd records"
};
static int lcl_pgcompact_dryrun = 0;
#define REASON(i) fprintf(stderr, "(!) (0x%x) %s %d: %s() - %s.\n",	\
		pthread_self(), __FILE__, __LINE__, __func__, reasons[i])

#define TRACE(...) fprintf(stderr, __VA_ARGS__)
#else
#define REASON(i)
#define TRACE(...)
#endif

static int
__bam_validate_subtree(dbc, nfb, direct)
	DBC *dbc;
	u_int32_t nfb;
	int *direct;
{
	PAGE *h, *ph;
	db_pgno_t pgno;
	BTREE_CURSOR *cp;
	EPG *epg;

	cp = (BTREE_CURSOR *)dbc->internal;
	h = cp->csp->page;
	pgno = PGNO(h);

	if (TYPE(h) != P_LBTREE) { /* Page is not leaft. Done. */
		REASON(REASON_NLEAF);
		return (1);
	}

	if (NUM_ENT(h) == 0) { /* Page is empty. Done. */
		REASON(REASON_EMPTY);
		return (1);
	}

	if (pgno == cp->root) { /* Page is root page. Done. */
		REASON(REASON_ROOT);
		return (1);
	}
	
	if (P_FREESPACE(dbc->dbp, h) <= nfb) {
		/* Page has a good fill ratio. Done. */
		REASON(REASON_FULL);
		return (1);
	}

	if (cp->sp == cp->csp)
		return (0);

	epg = &cp->csp[-1];
	ph = epg->page;

	if (NUM_ENT(ph) <= 2) {
		/* If the parent only has 2 children left, done.
		   Merging these 2 children would collapse the sibling, and
		   write-lock up to the common ancester to switch the key.
		   Do not want. */
		REASON(REASON_PARNT);
		return (1);
	}

	if (*direct == P_UNKNOWN)
		return (0);

	switch (*direct) {
	case P_L2R:
		/* May overflow so cast operands to signed */
		if ((int)(epg->indx) >= (int)(NUM_ENT(ph) - 2)) {
			REASON(REASON_LCK);
			return (1);
		}
		break;
	case P_BI:
		if ((int)(epg->indx) >= (int)(NUM_ENT(ph) - 2)) {
			/* Change direction because otherwise we would
			   go to a different subtree. Fall thought. */
			*direct = P_R2L;
		} else {
			break;
		}
	case P_R2L:
		if (epg->indx <= 1) {
			REASON(REASON_LCK);
			return (1);
		}
		break;
	}
	return (0);
}

static int
__bam_dup_cur_w_sibling_lock(dbc, pdupc, nh, nnpgno, dplockp, ndplockp, direct)
	DBC *dbc;
	DBC **pdupc;
	PAGE **nh;
	db_pgno_t nnpgno;
	DB_LOCK *dplockp;
	DB_LOCK *ndplockp;
	int direct;
{
	int ret;
	DB_ENV *dbenv;
	BTREE_CURSOR *cp;
	EPG *epg;
	PAGE *ph;

	dbenv = dbc->dbp->dbenv;
	cp = (BTREE_CURSOR *)dbc->internal;
	epg = &cp->csp[-1];
	ph = epg->page;

	/* Duplicate the cursor. */
	if ((ret = __db_c_dup(dbc, pdupc, 0)) != 0)
		goto out;
	cp = (BTREE_CURSOR *)(*pdupc)->internal;

	/* Transfer the lock on the next page. */
	if ((ret = __LPUT(dbc, cp->lock)) != 0)
		goto out;
	cp->lock = *dplockp;
	LOCK_INIT(*dplockp);

	/* Push parent and the next page. */
	BT_STK_PUSH(dbenv, cp, ph,
			(direct == P_R2L) ? (epg->indx - 1) : (epg->indx + 1),
			epg->lock, epg->lock_mode, ret);
	if (ret != 0)
		goto out;
	BT_STK_ENTER(dbenv, cp, *nh, 0, cp->lock, DB_LOCK_WRITE, ret);
	LOCK_INIT(cp->lock);

	if (nnpgno != PGNO_INVALID)
		/* Also lock 1 page further because we need to relink pages. */
		ret = __db_lget(dbc, 0, nnpgno, DB_LOCK_WRITE, 0, ndplockp);
out:
	if (ret != 0) {
		/* If any error, close the cursor and mark nh NULL. */
		if (*nh == cp->csp->page)
			*nh = NULL;
		(void)__bam_stkrel(*pdupc, STK_CLRDBC);
		(void)__db_c_close(*pdupc);
		*pdupc = NULL;
	}
	return (ret);
}

/* Compress page images? Default to true. */
int gbl_compress_page_compact_log = 1;

#define ALLOCA_LZ4_BUFFER() do {					\
	if (gbl_compress_page_compact_log)				\
		lz4dta = alloca(dbp->pgsize - HOFFSET(nh));	\
} while (0)

#define ALLOCA_UNLZ4_BUFFER() do {				\
	hdrdbtdata = argp->hdr.data;				\
	hdrdbtsize = argp->hdr.size;				\
	if (argp->data.size != argp->dtaoriglen) {	\
		unlz4dta = alloca(argp->dtaoriglen);	\
		dtadbtdata = (void *)unlz4dta;			\
		dtadbtsize = argp->dtaoriglen;			\
	} else {									\
		dtadbtdata = argp->data.data;			\
		dtadbtsize = argp->data.size;			\
	}											\
} while (0)

/* Try to compress page image. Return 1 if successfully lz'd. */
static int
__bam_lz4_pg_img(dbp, h, hdrp, dtap, dtabuf)
	DB *dbp;
	PAGE *h;
	DBT *hdrp;
	DBT *dtap;
	u_int8_t *dtabuf;
{
	int ret, ncompr;

	/* Zero DBT-s. */
	memset(hdrp, 0, sizeof(DBT));
	memset(dtap, 0, sizeof(DBT));

	/* Don't compress page header -
	   usually it is uncompressible or has very low compression rate. */
	hdrp->data = h;
	hdrp->size = LOFFSET(dbp, h);

	if (gbl_compress_page_compact_log) {
		ncompr = LZ4_compress_default((const char *)h + HOFFSET(h),
		    (char *)dtabuf, (dbp->pgsize - HOFFSET(h)), (dbp->pgsize - HOFFSET(h) - 1));
		if (ncompr <= 0)
			goto fallback_to_nocompr;

		dtap->data = dtabuf;
		dtap->size = ncompr;
		ret = 1;
	} else {
fallback_to_nocompr:
		dtap->data = (u_int8_t *)h + HOFFSET(h);
		dtap->size = dbp->pgsize - HOFFSET(h);
		ret = 0;
	}

	return (ret);
}

static int
__bam_unlz4_pg_img(argp, dtabuf)
	__bam_pgcompact_args *argp;
	u_int8_t *dtabuf;
{
	int nuncompr;

	if (argp->data.size == argp->dtaoriglen)
		return (0); /* Uncompressed. Not an error. */

	nuncompr = LZ4_decompress_safe(
			(const char *)argp->data.data,
			(char *)dtabuf,
            (int)argp->data.size,
			(int)argp->dtaoriglen);

	return (nuncompr < 0);
}

/* Merge 2 uncompressed pages. */
static int
__bam_merge_uncomp_pages(dbc, h, nh, ph, indx)
	DBC *dbc;
	PAGE *h;
	PAGE *nh;
	PAGE *ph;
    db_indx_t indx;
{
	int ret, direct;
	DB *dbp;
	u_int8_t *sp, *lz4dta = NULL;
	u_int32_t nent, len, ii;
	db_indx_t *ninp, *pinp;
	DBT hdr, dta;

	dbp = dbc->dbp;
	len = dbp->pgsize - HOFFSET(nh);
	direct = (PGNO(nh) == NEXT_PGNO(h)) ? P_L2R : P_R2L;

	if (!DBC_LOGGING(dbc))
		LSN_NOT_LOGGED(LSN(h));
	else {
		ALLOCA_LZ4_BUFFER();
		__bam_lz4_pg_img(dbp, nh, &hdr, &dta, lz4dta);

		if ((ret = __bam_pgcompact_log(dbp,
						dbc->txn, &LSN(h), 0,
						PGNO(h), &LSN(h), PGNO(nh), &LSN(nh),
						&hdr, &dta, len, 0, 0, 0, 0, 0, 0, 1,
						(direct == P_R2L), PGNO(ph), indx)) != 0)
			return (ret);
	}
	LSN(nh) = LSN(h);

	if (direct == P_L2R) { /* append */
		sp = (u_int8_t *)h + HOFFSET(h) - len;
		memcpy(sp, (u_int8_t *)nh + HOFFSET(nh), len);
		pinp = P_INP(dbp, h) + NUM_ENT(h);
		ninp = P_INP(dbp, nh);

		for (ii = 0, nent = NUM_ENT(nh); ii != nent; ++ii, ++pinp, ++ninp)
			*pinp = *ninp - (dbp->pgsize - HOFFSET(h));
	} else { /* prepend */
		sp = (u_int8_t *)h + HOFFSET(h) - len;
		/* Shift existing items on the page to the left. */
		memmove(sp, (u_int8_t *)h + HOFFSET(h), dbp->pgsize - HOFFSET(h));
		/* Shift existing indexes on the page to the right. */
		pinp = P_INP(dbp, h) + NUM_ENT(h) + NUM_ENT(nh);
		ninp = P_INP(dbp, h) + NUM_ENT(h);
		for (ii = 0, nent = NUM_ENT(h); ii != nent; ++ii, --pinp, --ninp)
			*pinp = *ninp - len;

		/* Prepend data. */
		sp = (u_int8_t *)h + dbp->pgsize - len;
		memcpy(sp, (u_int8_t *)nh + HOFFSET(nh), len);
		pinp = P_INP(dbp, h);
		ninp = P_INP(dbp, nh);
		memcpy(pinp, ninp, sizeof(db_indx_t) * NUM_ENT(nh));
	}

	HOFFSET(h) -= len;
	NUM_ENT(h) += nent;

	HOFFSET(nh) = dbp->pgsize;
	NUM_ENT(nh) = 0;

	return (0);
}

/* Merge 2 compressed pages. */
static int
__bam_merge_comp_pages(dbc, h, nh, ph, indx)
	DBC *dbc;
	PAGE *h;
	PAGE *nh;
	PAGE *ph;
    db_indx_t indx;
{
	int ret, direct;
	DB *dbp;
	u_int8_t *sp, *lz4dta = NULL;
	u_int32_t nent, len, ii;
	db_indx_t *ninp, *pinp;
	BKEYDATA *pfxh, *pfxnh;
	DBT hdr, dta, pfxhdbt, pfxnhdbt;

	dbp = dbc->dbp;
	len = *P_PREFIX(dbp, nh) - HOFFSET(nh);
	direct = (PGNO(nh) == NEXT_PGNO(h)) ? P_L2R : P_R2L;

	if (!DBC_LOGGING(dbc))
		LSN_NOT_LOGGED(LSN(h));
	else {
		ALLOCA_LZ4_BUFFER();
		__bam_lz4_pg_img(dbp, nh, &hdr, &dta, lz4dta);

		pfxh = P_PFXENTRY(dbp, h);
		memset(&pfxhdbt, 0, sizeof(DBT));
		ASSIGN_ALIGN_DIFF(u_int32_t, pfxhdbt.size, db_indx_t, pfxh->len);
		pfxhdbt.data = pfxh->data;

		pfxnh = P_PFXENTRY(dbp, nh);
		memset(&pfxnhdbt, 0, sizeof(DBT));
		ASSIGN_ALIGN_DIFF(u_int32_t, pfxnhdbt.size, db_indx_t, pfxnh->len);
		pfxnhdbt.data = pfxnh->data;

		if ((ret = __bam_pgcompact_log(dbp,
						dbc->txn, &LSN(h), 0, PGNO(h),
						&LSN(h), PGNO(nh), &LSN(nh), &hdr,
						&dta, (dbp->pgsize - HOFFSET(nh)), 0, 0,
						&pfxhdbt, pfxh->type, &pfxnhdbt, pfxnh->type,
						1, (direct == P_R2L), PGNO(ph), indx)) != 0)
			return (ret);
	}
	LSN(nh) = LSN(h);

	if (direct == P_L2R) { /* append */
		sp = (u_int8_t *)h + HOFFSET(h) - len;
		memcpy(sp, (u_int8_t *)nh + HOFFSET(nh), len);
		pinp = P_INP(dbp, h) + NUM_ENT(h);
		ninp = P_INP(dbp, nh);

		for (ii = 0, nent = NUM_ENT(nh); ii != nent; ++ii, ++pinp, ++ninp)
			*pinp = *ninp - (*P_PREFIX(dbp, nh) - HOFFSET(h));
	} else { /* prepend */
		sp = (u_int8_t *)h + HOFFSET(h) - len;
		/* Shift existing items on the page to the left. */
		memmove(sp, (u_int8_t *)h + HOFFSET(h), *P_PREFIX(dbp, h) - HOFFSET(h));
		/* Shift existing indexes on the page to the right. */
		pinp = P_INP(dbp, h) + NUM_ENT(h) + NUM_ENT(nh);
		ninp = P_INP(dbp, h) + NUM_ENT(h);
		for (ii = 0, nent = NUM_ENT(h); ii != nent; ++ii, --pinp, --ninp)
			*pinp = *ninp - len;

		/* Prepend data. */
		sp = (u_int8_t *)h + dbp->pgsize - len;
		memcpy(sp, (u_int8_t *)nh + HOFFSET(nh), len);
		pinp = P_INP(dbp, h);
		ninp = P_INP(dbp, nh);
		memcpy(pinp, ninp, sizeof(db_indx_t) * NUM_ENT(nh));
	}

	HOFFSET(h) -= len;
	NUM_ENT(h) += nent;

	HOFFSET(nh) = dbp->pgsize;
	NUM_ENT(nh) = 0;
	/* We do not need __db_free() to restore prefix for us.
	   All information we need during recovery is in `hdr` and `dta`. */
	CLR_PREFIX(nh);

	return (0);
}

static int
__bam_move_comp_entries_log(dbc, pfx, newh, h, nh, ph, indx)
	DBC *dbc;
	pfx_t *pfx;
	PAGE *newh;
	PAGE *h;
	PAGE *nh;
	PAGE *ph;
	db_indx_t indx;
{
	int ret, direct;
	DB *dbp;
	BKEYDATA *pfxh = NULL, *pfxnh = NULL, *newpfx;
	DBT hdr, dta, newpfxdbt, pfxhdbt, pfxnhdbt;
	u_int8_t *lz4dta = NULL;

	dbp = dbc->dbp;
	direct = (PGNO(nh) == NEXT_PGNO(h)) ? P_L2R : P_R2L;

	if (!DBC_LOGGING(dbc))
		LSN_NOT_LOGGED(LSN(h));
	else {
		ALLOCA_LZ4_BUFFER();
		__bam_lz4_pg_img(dbp, nh, &hdr, &dta, lz4dta);

		memset(&newpfxdbt, 0, sizeof(DBT));
		newpfx = alloca(pfx_bkeydata_size(pfx));
		(void)pfx_to_bkeydata(pfx, newpfx);
		ASSIGN_ALIGN_DIFF(u_int32_t, newpfxdbt.size, db_indx_t, newpfx->len);
		newpfxdbt.data = newpfx->data;

		memset(&pfxhdbt, 0, sizeof(DBT));
		if (IS_PREFIX(h)) {
			pfxh = P_PFXENTRY(dbp, h);
			ASSIGN_ALIGN_DIFF(u_int32_t, pfxhdbt.size, db_indx_t, pfxh->len);
			pfxhdbt.data = pfxh->data;
		}

		memset(&pfxnhdbt, 0, sizeof(DBT));
		if (IS_PREFIX(nh)) {
			pfxnh = P_PFXENTRY(dbp, nh);
			ASSIGN_ALIGN_DIFF(u_int32_t, pfxnhdbt.size, db_indx_t, pfxnh->len);
			pfxnhdbt.data = pfxnh->data;
		}

		if ((ret = __bam_pgcompact_log(dbp,
						dbc->txn, &LSN(h), 0, PGNO(h),
						&LSN(h), PGNO(nh), &LSN(nh), &hdr, &dta,
						(dbp->pgsize - HOFFSET(nh)), &newpfxdbt,
						newpfx->type, &pfxhdbt, pfxh->type, &pfxnhdbt,
						pfxnh->type, 0, (direct == P_R2L), PGNO(ph), indx)) != 0)
			return (ret);
	}
	LSN(newh) = LSN(h);

	return (0);
}

/* Move records of h and nh to newh */
static int
__bam_move_comp_entries(dbc, pfx, h, nh, newh)
	DBC *dbc;
	pfx_t *pfx;
	PAGE *h;
	PAGE *nh;
	PAGE *newh;
{
	int rc;
	DB *dbp;
	dbp = dbc->dbp;
	memset(newh, 0, dbp->pgsize);

	if (PGNO(nh) == NEXT_PGNO(h))
		rc = pfx_compress_pages(dbp, newh, h, pfx, 2, h, nh);
	else                                           /* ^^^^^ */
		rc = pfx_compress_pages(dbp, newh, h, pfx, 2, nh, h);
		                                           /* ^^^^^ */

	return rc;
}

/*
 * __bam_swap_parent_keys --
 *  Swap keys at `indx` and (`indx`-1) on internal page `ph`.
 *
 * PUBLIC: int __bam_swap_parent_keys __P((DBC *, PAGE *, db_indx_t));
 */
int
__bam_swap_parent_keys(dbc, ph, indx)
	DBC *dbc;
	PAGE *ph;
	db_indx_t indx;
{
	/* For right-to-left scan. */
	BINTERNAL *l_int, *r_int;
	db_indx_t *l_inp, nlintb, nrintb;
	db_pgno_t pgno;
	DB *dbp;
	void *buf;

	dbp = dbc->dbp;
	l_inp = P_INP(dbp, ph) + (indx - 1);
	l_int = GET_BINTERNAL(dbp, ph, indx - 1);
	r_int = GET_BINTERNAL(dbp, ph, indx);

	/* Swap page numbers. */
	pgno = l_int->pgno;
	l_int->pgno = r_int->pgno;
	r_int->pgno = pgno;

	nlintb = BINTERNAL_SIZE(l_int->len);
	nrintb = BINTERNAL_SIZE(r_int->len);
	buf = alloca(nrintb);
	memcpy(buf, r_int, nrintb);
	memmove(r_int, l_int, nlintb);
	memcpy((u_int8_t *)r_int + nlintb, buf, nrintb);
	*l_inp += (nlintb - nrintb);
	return (0);
}

/* Backward merge scan is disabled by default.
   Backward scan
   1) :) offers slightly better fill factor
   2) :( is slower
   3) :( generates more log
   than forward scan. */
int gbl_disable_backward_scan = 1;

/* Maximum number of pages that can be merged (deleted)
   within a single transaction. Default to unlimited. */
unsigned int gbl_max_num_compact_pages_per_txn = ~(0U);

/*
 * __bam_pgcompact --
 *  Light-weight page compaction.
 *
 * PUBLIC: int __bam_pgcompact __P((DBC *, DBT *, double, double));
 */
int
__bam_pgcompact(dbc, dbt, ff, tgtff)
	DBC *dbc;
	DBT *dbt;
	double ff;
	double tgtff;
{
	extern struct thdpool *gbl_pgcompact_thdpool;
	int ret, t_ret, do_not_care;
	u_int32_t ntb, nub, nfb; //, nsfb;
	int32_t snbl, snbr;
	DB *dbp;
	DBC *dupc;
	DB_ENV *dbenv;
	DB_MPOOLFILE *dbmfp;
	DB_LOCK dplock, ndplock;
	db_pgno_t pgno, ppgno, npgno, cpgno;
	PAGE *h, *nh, *ph, *hcp, *nhcp, *newh;
	BTREE_CURSOR *cp, *dupcp;
	EPG *epg;
	int direct, fit, nretries;
	int local_disable_backward;
	unsigned int local_max_np_per_txn, np_txn;

	/* For compressed pages. */
	uint8_t *pfxbuf;
	pfx_t *pfx;
	DB_LSN lsnsbefore[2], lsnsafter[2];

	dupc = NULL;
	cp = NULL;
	dupcp = NULL;
	direct = P_UNKNOWN;
	fit = BTPFX_FIT_NO;
	nretries = 0;

	dbp = dbc->dbp;
	dbenv = dbp->dbenv;
	dbmfp = dbp->mpf;

	pgno = ppgno = npgno = cpgno = PGNO_INVALID;
	h = nh = ph = NULL;
	LOCK_INIT(dplock);
	LOCK_INIT(ndplock);

	/* for page compression */
	pfx = NULL;
	pfxbuf = NULL;
	hcp = alloca(dbp->pgsize);
	nhcp = alloca(dbp->pgsize);
	newh = alloca(dbp->pgsize);
	np_txn = 0;

	/* Make a local copy to speed up. */
	local_disable_backward = gbl_disable_backward_scan;
	local_max_np_per_txn = gbl_max_num_compact_pages_per_txn;

	TRACE("(!) 0x%x %s %d: %s() begins.\n",
			pthread_self(), __FILE__, __LINE__, __func__);

	if (F_ISSET(dbc, DBC_OPD)) {
		REASON(REASON_DUP);
		__db_err(dbenv, "__bam_pgcompact: %s",
				"OPD compaction not implemented");
		return (EINVAL);
	}

	/* Search down the btree in read mode. The page may have been 
	   changed before we get here. Therefore we need to examine
	   the page one more time. Don't rush to get the write lock. */
	ret = __bam_search(dbc,
			PGNO_INVALID, dbt, S_READ, LEAFLEVEL, NULL, &do_not_care);
	if (ret != 0) {
		if (ret == DB_NOTFOUND) { /* Key deleted. This is okay. Done. */
			REASON(REASON_KDEL);
			goto done;
		}
		goto err;
	}
	cp = (BTREE_CURSOR *)dbc->internal;
	h = cp->csp->page;

	ntb = dbp->pgsize - SIZEOF_PAGE;    /* total */
	nub = ntb * ff;                     /* expected used */
	nfb = ntb - nub;                    /* expected free */
	//nsfb = (u_int32_t)((1 - tgtff) * (double)ntb); /* target free */

	TRACE("(!) 0x%x %s %d: %s() I'm looking at pgno %d file %s.\n",
			pthread_self(), __FILE__, __LINE__, __func__, PGNO(h), dbp->fname);

#ifdef PGCOMP_DBG
	if (lcl_pgcompact_dryrun) {
		REASON(REASON_DRYRUN);
		goto done;
	}
#endif

	if (__bam_validate_subtree(dbc, nfb, &direct) != 0)
		goto done;

	/* We're going to scan siblings for merge. Get a write lock
	   on the page. Release the page first so we don't deadlock */
	if ((ret = __memp_fput(dbmfp, h, 0)) != 0)
		goto err_zero_h;
	if ((ret = __LPUT(dbc, cp->csp->lock)) != 0)
		goto err_zero_h;

	BT_STK_CLR(cp);
	h = NULL;

	/* We need both the page and its parent page. 
       It is okay if the key is deleted before we get here.
       If __bam_search() returns DB_LOCK_DEADLOCK,
       return an error immediately without any retry effort. */
	ret = __bam_search(dbc,
			PGNO_INVALID, dbt, S_PARENT | S_WRITE, LEAFLEVEL, NULL, &do_not_care);
	if (ret != 0) {
		if (ret == DB_NOTFOUND) { /* Key gone. Okay. */
			REASON(REASON_KDEL);
			goto done;
		}
		goto err;
	}

retry:
	if (nretries > NUM_MAX_RETRIES) {
		REASON(REASON_MAX_RETRY);
		goto done;
	}

	h = cp->csp->page;
	pgno = PGNO(h);
	epg = &cp->csp[-1];
	ph = epg->page;
	ppgno = PGNO(ph);

	/* Determine merge direction.
	   We don't want to merge siblings with different parents (?) because
	   we would need to find the common ancester and lock a potentially
	   very big subtree. So if the target page is the leftmost(rightmost)
	   child of its parent, we look up siblings only in a forward(backward)
	   direction. If the target is somewhere in the center, we do this
	   in a bi-directional manner - sweep the right half till
	   1) the page hits target fill ratio, or,
	   2) Parent has only 2 child, or
	   3) we reach the edge child, or
	   4) the next page can't be merged with the target page.
	   In case #1 and #2, we're done. In #3 and #4, we sweep the other half
	   till we meet #1, #2 or #3. Then, we're very done. */
	if (epg->indx == 0) /* leftmost */
		direct = P_L2R;
	else if (epg->indx == NUM_ENT(ph) - 1) /* rightmost */
		direct = P_R2L;
	else /* center */
		direct = P_BI;

	while (__bam_validate_subtree(dbc, nfb, &direct) == 0) {

		if (np_txn++ == local_max_np_per_txn) {
			REASON(REASON_MAX_PGS);
			goto done;
		}

		if (local_disable_backward && direct == P_R2L) {
			REASON(REASON_NBACKSCAN);
			goto done;
		}

		npgno = (direct == P_R2L) ? PREV_PGNO(h) : NEXT_PGNO(h);

		/* Acquire next page in write mode. Note that we already have write lock
		   on next page if we are doing retry. */
		if (nretries == 0) {
			if ((ret = __db_lget(dbc, 0, npgno, DB_LOCK_WRITE, 0, &dplock)) != 0)
				goto err_zero_h;
			nh = NULL;
			if ((ret = __memp_fget(dbmfp, &npgno, 0, &nh)) != 0)
				goto err_zero_h;
		}

		if (NUM_ENT(nh) == 0) {
			REASON(REASON_EMPTY);
			goto not_fit;
		}

		/* do this in advance */
		npgno = (direct == P_R2L) ? PREV_PGNO(nh) : NEXT_PGNO(nh);

		/*
		                   +-------------+
		   eg, ph, ppgno - | Parent page |
		                   +-------------+ .... may or may not connect ....+
		                  /               \                                |
		      dbc, pgno, h                 dupc, nh                        npgno
		              \                        |                          /
		   +-------------------+     +--------------------+     +--------------------+
		   | Page to be merged | <-> | Page being deleted | <-> | Page to be checked |
		   +-------------------+     +--------------------+     +--------------------+
		 */

		/* Cut 1 - Only check if next page fits. If not, done. The downside
		   is that we may never be able to compact pages from the pattern
		   (P1 ff=0.2) -> (P2 ff=0.8) -> (P3 ff=0.2) to
		   (P1 ff=0.6) -> (P2 ff=0.6).
		   We could alternatively relocate a few items from
		   next page to the target page, but it would 1) add extra complexity,
		   2) lock more pages, and 3) increase log size - We would have to
		   relocate and log items one at a time so we know where to stop. */

		/* Copy over */
		if (!IS_PREFIX(h) && !IS_PREFIX(nh)) {
			snbl = dbp->pgsize - P_OVERHEAD(dbp) - P_FREESPACE(dbp, nh);
			snbr = P_FREESPACE(dbp, h) /* - nsfb */;
			if (snbl > snbr)
				goto not_fit;

			if ((ret = __bam_dup_cur_w_sibling_lock(dbc,
							&dupc, &nh, npgno, &dplock, &ndplock, direct)) != 0)
				goto err_zero_h;
			dupcp = (BTREE_CURSOR *)dupc->internal;

			if ((ret = __bam_merge_uncomp_pages(dbc, h, nh, ph, epg->indx)) != 0)
				goto err_zero_h;
			REASON(REASON_WHOAH_UNCOMP);
		} else {
			if (pfxbuf == NULL) {
				pfxbuf = alloca(KEYBUF + 128);
				pfx = (pfx_t *)pfxbuf;
			}
			fit = can_fit(dbp, h, nh, pfx);
			switch (fit) {
			case BTPFX_FIT_NO:
				goto not_fit;
			case BTPFX_FIT_YES_MEMCPY:
				if ((ret = __bam_dup_cur_w_sibling_lock(dbc,
								&dupc, &nh, npgno, &dplock, &ndplock, direct)) != 0)
					goto err_zero_h;
				dupcp = (BTREE_CURSOR *)dupc->internal;

				if ((ret = __bam_merge_comp_pages(dbc, h, nh, ph, epg->indx)) != 0)
					goto err_zero_h;
				REASON(REASON_WHOAH_COMP);
				break;
			case BTPFX_FIT_MAYBE:
				if (thdpool_get_nqueuedworks(gbl_pgcompact_thdpool) > 0)
					/* No attempt if pool is busy. */
					goto not_fit;
				/* fall through */
			case BTPFX_FIT_YES_DECOMP:

				/* We don't want to hold parent while moving records.
				   We also have to release all leaf locks because we can't
				   bottom-up lock the tree when we relock the parent.
				   So here is our mind blown plan -
				   1) Decompress without locks.
				   2) After we're done, re-lock pages and validate LSNs.
				   3) If the subtree has been reorganized, we're done.
					  Elif LSNs have changed and the target still has low ff, retry.
					  Otherwise we're also done. */

				lsnsbefore[0] = LSN(h);
				lsnsbefore[1] = LSN(nh);
				memcpy(hcp, h, dbp->pgsize);
				memcpy(nhcp, nh, dbp->pgsize);
				
				/* Unlock */
				if ((ret = __bam_stkrel(dbc, STK_CLRDBC)) != 0)
					goto err_zero_h;
				if ((ret = __LPUT(dbc, dplock)) != 0)
					goto err_zero_h;
				LOCK_INIT(dplock);
				if ((ret = __memp_fput(dbmfp, nh, 0)) != 0) {
					nh = NULL;
					goto err_zero_h;
				}

				/* Move records */
				if ((ret = __bam_move_comp_entries(dbc, pfx, hcp, nhcp, newh)) != 0) {
					/* Can't fit. Make nh null so we don't fput it twice. */
					nh = NULL;
					goto err_zero_h;
				}

				REASON(REASON_WHOAH_PFXREC);

				/* Relock. */
				h = nh = ph = NULL;
				ret = __bam_search(dbc, PGNO_INVALID, dbt,
						S_PARENT | S_WRITE, LEAFLEVEL, NULL, &do_not_care);
				if (ret != 0) {
					if (ret == DB_NOTFOUND) {
						REASON(REASON_KDEL);
						goto done;
					}
					goto err;
				}

				/* Refresh stuff we care. */
				h = cp->csp->page;
				epg = &cp->csp[-1];
				ph = epg->page;
				ppgno = PGNO(ph);
				if (__bam_validate_subtree(dbc, nfb, &direct) != 0) {
					REASON(REASON_RESHAPE);
					goto done;
				}

				npgno = (direct == P_R2L) ? PREV_PGNO(h) : NEXT_PGNO(h);
				if ((ret = __db_lget(dbc, 0, npgno, DB_LOCK_WRITE,
								0, &dplock)) != 0)
					goto err_zero_h;
				if ((ret = __memp_fget(dbmfp, &npgno, 0, &nh)) != 0)
					goto err_zero_h;

				/* validate page LSNs */
				lsnsafter[0] = LSN(h);
				lsnsafter[1] = LSN(nh);
				if (memcmp(&lsnsbefore, &lsnsafter, sizeof(lsnsbefore)) == 0) {
					/* LSNs remain the same. Best. */
					npgno = (direct == P_R2L) ? PREV_PGNO(nh) : NEXT_PGNO(nh);

					if ((ret = __bam_dup_cur_w_sibling_lock(dbc,
									&dupc, &nh, npgno, &dplock, &ndplock, direct)) != 0)
						goto err_zero_h;
					dupcp = (BTREE_CURSOR *)dupc->internal;

					/* log before touching pages */
					if ((ret = __bam_move_comp_entries_log(dbc,
									pfx, newh, hcp, nhcp, ph, epg->indx)) != 0)
						goto err_zero_h;

					memcpy(h, newh, dbp->pgsize);
					HOFFSET(nh) = dbp->pgsize;
					NUM_ENT(nh) = 0;
					LSN(nh) = LSN(h);
					CLR_PREFIX(nh);
					break;
				} else {
					/* LSNs have changed. Retry. */
					REASON(REASON_RETRY);
					++nretries;
					goto retry;
				}
			}
		}

		/* If the direction is right-to-left, we need to update the parent page
		   before we drop the leaf. We cheat a little bit here by swapping
		   the left and right keys and letting __bam_dpages() take care of
		   the rest. We swap the keys back using the same function when undoing. */
		if (direct == P_R2L)
			__bam_swap_parent_keys(dbc, ph, epg->indx);

		++GET_BH_GEN(h);

		if ((ret = __db_relink(dbc, DB_REM_PAGE, dupcp->csp->page, NULL, 0)) != 0)
			goto err_zero_h;

		cp->sp->page = NULL;
		LOCK_INIT(cp->sp->lock);

		if ((ret = __bam_dpages(dupc, dupcp->sp, 1)) != 0)
			goto err_zero_h;
		nh = NULL;

		REASON(REASON_WHOAH);

		/* Done merging. Release the extra lock. */
		if ((ret = __TLPUT(dbc, ndplock)) != 0)
			goto err_zero_h;
		LOCK_INIT(ndplock);

		/* Reset retry counter. */
		nretries = 0;

		/* Refresh the cursor stack. */
		epg = &cp->csp[-1];
		ph = epg->page;
		if (ph == NULL) {
			if ((ret = __memp_fput(dbmfp, cp->csp->page, DB_MPOOL_DIRTY)) != 0)
				goto err_zero_h;
			/* Clear references in case we fail to fget(). */
			h = cp->csp->page = NULL;
			if ((ret = __memp_fget(dbmfp, &ppgno, 0, &ph)) != 0)
				goto err_zero_h;
			if ((ret = __memp_fget(dbmfp, &pgno, 0, &h)) != 0)
				goto err_zero_h;
			epg->page = ph;
			cp->csp->page = h;
		}

		cpgno = (direct == P_R2L) ? PREV_PGNO(h) : NEXT_PGNO(h);

		if (npgno != cpgno) { /* whoops */
			REASON(REASON_NFIT);
not_fit:
			if (direct != P_BI)
				goto done;

			/* Change direction because pages did not fit. */
			REASON(REASON_NFIT);
			direct = P_R2L;
		}

		/* Before we look at the left side,
		   release the right sibling. */
		if (dupc != NULL) {
			if (nh == dupcp->csp->page)
				nh = NULL;
			if (dupcp->sp->page == cp->sp->page) {
				dupcp->sp->page = NULL;
				LOCK_INIT(dupcp->sp->lock);
			}

			if ((t_ret = __bam_stkrel(dupc, STK_CLRDBC)) != 0 && ret == 0)
				ret = t_ret;
			if ((t_ret = __db_c_close(dupc)) != 0 && ret == 0)
				ret = t_ret;
			dupc = NULL;
			dupcp = NULL;
		}

		if (nh != NULL) {
			if ((ret = __memp_fput(dbmfp, nh, 0)) != 0 && ret == 0)
				ret = t_ret;
			nh = NULL;
		}

		if ((t_ret = __TLPUT(dbc, dplock)) != 0 && ret == 0)
			ret = t_ret;
		LOCK_INIT(dplock);
		if ((t_ret = __TLPUT(dbc, ndplock)) != 0 && ret == 0)
			ret = t_ret;
		LOCK_INIT(ndplock);

		if (ret != 0)
			goto err_zero_h;

		ppgno = PGNO(ph);
		npgno = cpgno = PGNO_INVALID;
	}

err:
	if (0) {
done:	/* Not an error. */
		if (ret != 0)
			ret = 0;
err_zero_h:
		h = NULL;
	}

	REASON(REASON_EXIT);

	if (dupc != NULL) { /* clear duplicate cursor */
		if (nh == dupcp->csp->page)
			nh = NULL;
		if (dupcp->sp->page == cp->sp->page) {
			dupcp->sp->page = NULL;
			LOCK_INIT(dupcp->sp->lock);
		}
		if ((t_ret = __bam_stkrel(dupc, STK_CLRDBC)) != 0 && ret == 0)
			ret = t_ret;
		else if ((t_ret = __db_c_close(dupc)) != 0 && ret == 0)
			ret = t_ret;
	}

    /* cp could be NULL if the 1st call to __bam_search()
       returns an error. */
	if (cp != NULL && h == cp->csp->page)
		h = NULL;
	if ((t_ret = __bam_stkrel(dbc, STK_CLRDBC)) != 0 && ret == 0)
		ret = t_ret;
	if (h != NULL)
		if ((t_ret = __memp_fput(dbmfp, h, 0)) != 0 && ret == 0)
			ret = t_ret;
	if (nh != NULL)
		if ((t_ret = __memp_fput(dbmfp, nh, 0)) != 0 && ret == 0)
			ret = t_ret;
	if ((t_ret = __TLPUT(dbc, dplock)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __TLPUT(dbc, ndplock)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

/*
 * __bam_pgcompact_redo_target --
 *  Move data to the target page.
 *
 * PUBLIC: int __bam_pgcompact_redo_target __P((DBC *, PAGE *, __bam_pgcompact_args *));
 */
int
__bam_pgcompact_redo_target(dbc, h, argp)
	DBC *dbc;
	PAGE *h;
	__bam_pgcompact_args *argp;
{
	int ret;
	DB *dbp;
	u_int8_t *sp, *pfxbuf, *nhbuf, *unlz4dta = NULL;
	PAGE *c, *nh;
	db_indx_t *ninp, *pinp, ii, nent, len;
	pfx_t *pfx;
	void *dtadbtdata, *hdrdbtdata;
	u_int32_t dtadbtsize, hdrdbtsize;

	dbp = dbc->dbp;

	ALLOCA_UNLZ4_BUFFER();
	if ((ret = __bam_unlz4_pg_img(argp, unlz4dta)) != 0) {
		__db_err(dbp->dbenv, "__bam_unlz4_pg_img: %s",
				"Failed to decompress");
		return 1;
	}

	if (argp->copymem) {
		/* memcpy */
		ret = 0;
		DB_ASSERT(argp->prvpgpfx.size == 0 && argp->prvnpgpfx.size == 0
				|| argp->prvpgpfx.size != 0 && argp->prvnpgpfx.size != 0);
		if (argp->prvpgpfx.size == 0) {
			/* both uncompressed */
			if (!argp->rtl) { /* append */
				sp = (u_int8_t *)h + HOFFSET(h) - (db_indx_t)dtadbtsize;
				memcpy(sp, dtadbtdata, dtadbtsize);

				pinp = P_INP(dbp, h) + NUM_ENT(h);
				ninp = P_INP(dbp, hdrdbtdata);
				for (ii = 0, nent = NUM_ENT(hdrdbtdata);
						ii != nent; ++ii, ++pinp, ++ninp)
					*pinp = *ninp - (dbp->pgsize - HOFFSET(h));
			} else { /* prepend */
				sp = (u_int8_t *)h + HOFFSET(h) - (db_indx_t)dtadbtsize;
				/* Shift existing items on the page to the left. */
				memmove(sp, (u_int8_t *)h + HOFFSET(h), dbp->pgsize - HOFFSET(h));
				/* Shift existing indexes on the page to the right. */
				pinp = P_INP(dbp, h) + NUM_ENT(h) + NUM_ENT(hdrdbtdata);
				ninp = P_INP(dbp, h) + NUM_ENT(h);
				for (ii = 0, nent = NUM_ENT(h); ii != nent; ++ii, --pinp, --ninp)
					*pinp = *ninp - (db_indx_t)dtadbtsize;

				/* Prepend data. */
				sp = (u_int8_t *)h + (db_indx_t)(dbp->pgsize - dtadbtsize);
				memcpy(sp, dtadbtdata, dtadbtsize);
				pinp = P_INP(dbp, h);
				ninp = P_INP(dbp, hdrdbtdata);
				memcpy(pinp, ninp, sizeof(db_indx_t) * NUM_ENT(hdrdbtdata));
			}

			HOFFSET(h) -= dtadbtsize;
			NUM_ENT(h) += nent;
		} else {
			/* both compressed */
			if (!argp->rtl) { /* append */
				len = *P_PREFIX(dbp, hdrdbtdata) - HOFFSET(hdrdbtdata);
				sp = (u_int8_t *)h + HOFFSET(h) - len;
				memcpy(sp, dtadbtdata, len);

				pinp = P_INP(dbp, h) + NUM_ENT(h);
				ninp = P_INP(dbp, hdrdbtdata);
				for (ii = 0, nent = NUM_ENT(hdrdbtdata);
						ii != nent; ++ii, ++pinp, ++ninp)
					*pinp = *ninp - (*P_PREFIX(dbp, hdrdbtdata) - HOFFSET(h));
			} else { /* prepend */
				len = *P_PREFIX(dbp, hdrdbtdata) - HOFFSET(hdrdbtdata);
				sp = (u_int8_t *)h + HOFFSET(h) - len;
				/* Shift existing items on the page to the left. */
				memmove(sp, (u_int8_t *)h + HOFFSET(h), *P_PREFIX(dbp, h) - HOFFSET(h));
				/* Shift existing indexes on the page to the right. */
				pinp = P_INP(dbp, h) + NUM_ENT(h) + NUM_ENT(hdrdbtdata);
				ninp = P_INP(dbp, h) + NUM_ENT(h);
				for (ii = 0, nent = NUM_ENT(h); ii != nent; ++ii, --pinp, --ninp)
					*pinp = *ninp - len;

				/* Prepend data. */
				sp = (u_int8_t *)h + dbp->pgsize - len;
				memcpy(sp, dtadbtdata, dtadbtsize);
				pinp = P_INP(dbp, h);
				ninp = P_INP(dbp, hdrdbtdata);
				memcpy(pinp, ninp, sizeof(db_indx_t) * NUM_ENT(hdrdbtdata));
			}

			HOFFSET(h) -= len;
			NUM_ENT(h) += nent;
		}
	} else {
		/* Decompress both and then compress using the new common pfx. */
		c = alloca(dbp->pgsize);
		nhbuf = alloca(dbp->pgsize);
		pfxbuf = alloca(KEYBUF);

		/* Construct a PAGE object so we can call pfx_compress_pages() directly. */
		memset(c, 0, dbp->pgsize);
		memset(nhbuf, 0, dbp->pgsize);
		nh = (PAGE *)nhbuf;
		pfx = (pfx_t *)pfxbuf;
		sp = (u_int8_t *)nh + HOFFSET(hdrdbtdata);

		memcpy(nh, hdrdbtdata, hdrdbtsize);
		memcpy(sp, dtadbtdata, dtadbtsize);

		if ((ret = dbt_to_pfx(&argp->newpfx, argp->ntype, pfx)) != 0)
			goto out;

		if (!argp->rtl) { /* append */
			if ((ret = pfx_compress_pages(dbp, c, h, pfx, 2, h, nh)) != 0)
				goto out;                                 /* ^^^^^ */
		} else {
			if ((ret = pfx_compress_pages(dbp, c, h, pfx, 2, nh, h)) != 0)
				goto out;                                 /* ^^^^^ */
		}

		memcpy(h, c, dbp->pgsize);
	}

out:
	return (ret);
}

/*
 * __bam_pgcompact_undo_target --
 *  Move items off the target page.
 *
 * PUBLIC: int __bam_pgcompact_undo_target __P((DBC *, PAGE *, __bam_pgcompact_args *));
 */
int
__bam_pgcompact_undo_target(dbc, h, argp)
	DBC *dbc;
	PAGE *h;
	__bam_pgcompact_args *argp;
{
	int ret;
	db_indx_t ii, nent, pos, bklen;
	u_int32_t nb;
	DB *dbp;
	PAGE *c;
	BKEYDATA *bk;
	__bam_prefix_args *pfxargp;
	u_int8_t *unlz4dta = NULL;
	void *dtadbtdata, *hdrdbtdata;
	u_int32_t dtadbtsize, hdrdbtsize;

	ret = 0;
	dbp = dbc->dbp;

	ALLOCA_UNLZ4_BUFFER();
	if ((ret = __bam_unlz4_pg_img(argp, unlz4dta)) != 0) {
		__db_err(dbp->dbenv, "__bam_unlz4_pg_img: %s",
				"Failed to decompress");
		return 1;
	}

	if (!argp->rtl) { /* append */
		/* Logically delete items from the end of pagelog. */
		for (ii = 0, nent = NUM_ENT(hdrdbtdata); ii != nent; ++ii) {
			pos = NUM_ENT(h) - 1;
			if (pos >= P_INDX && P_INP(dbp, h)[pos] == P_INP(dbp, h)[pos - P_INDX]) {
				--NUM_ENT(h);
				continue;
			}
			bk = GET_BKEYDATA(dbp, h, pos);
			ASSIGN_ALIGN(db_indx_t, bklen, bk->len);
			nb = BKEYDATA_SIZE(bklen);
			__db_ditem(dbc, h, pos, nb);
		}
	} else {
		/* Logically delete items from the beginning of pagelog. */
		for (pos = NUM_ENT(hdrdbtdata); pos != 0;) {
			--pos;
			if (pos >= P_INDX && P_INP(dbp, h)[pos] == P_INP(dbp, h)[pos - P_INDX]) {
				--NUM_ENT(h);
				continue;
			}
			bk = GET_BKEYDATA(dbp, h, pos);
			ASSIGN_ALIGN(db_indx_t, bklen, bk->len);
			nb = BKEYDATA_SIZE(bklen);
			__db_ditem(dbc, h, pos, nb);
		}
	}

	if (argp->copymem)
		goto out;

	/* Restore key compression on the target page. */
	pfxargp = (__bam_prefix_args *)alloca(sizeof(__bam_prefix_args));
	pfxargp->prvpfx = argp->prvpgpfx;
	pfxargp->ptype = argp->ppgtype;
	c = (PAGE *)alloca(dbp->pgsize);
	memset(c, 0, dbp->pgsize);

	/* This isn't a real compress undo, just borrowing the function. */
	if ((ret = pfx_compress_undo(dbp, h, c, pfxargp)) != 0)
		goto out;
	ret = pfx_apply(dbp, h, c);

out:
	return (ret);
}

/*
 * __bam_pgcompact_redo_victim --
 *  Empty the victim page.
 *
 * PUBLIC: int __bam_pgcompact_redo_victim __P((DBC *, PAGE *, __bam_pgcompact_args *));
 */
int
__bam_pgcompact_redo_victim(dbc, h, argp)
	DBC *dbc;
	PAGE *h;
	__bam_pgcompact_args *argp;
{
	HOFFSET(h) = dbc->dbp->pgsize;
	NUM_ENT(h) = 0;
	return (0);
}

/*
 * __bam_pgcompact_undo_victim --
 *  Move items back to the victim page.
 *
 * PUBLIC: int __bam_pgcompact_undo_victim __P((DBC *, PAGE *, __bam_pgcompact_args *));
 */
int
__bam_pgcompact_undo_victim(dbc, h, argp)
	DBC *dbc;
	PAGE *h;
	__bam_pgcompact_args *argp;
{
	u_int8_t *sp, *unlz4dta = NULL;
	DB *dbp;
	void *dtadbtdata, *hdrdbtdata;
	u_int32_t dtadbtsize, hdrdbtsize;

	dbp = dbc->dbp;

	ALLOCA_UNLZ4_BUFFER();
	if (__bam_unlz4_pg_img(argp, unlz4dta) != 0) {
		__db_err(dbp->dbenv, "__bam_unlz4_pg_img: %s",
				"Failed to decompress");
		return 1;
	}

	/* Copy data over. */
	sp = (u_int8_t *)h + HOFFSET(h) - (db_indx_t)dtadbtsize;
	memcpy(sp, dtadbtdata, dtadbtsize);
	/* Copy header. */
	memcpy(h, hdrdbtdata, hdrdbtsize);

	return (0);
}

/*
 * __bam_ispgcompactible --
 *	Return (0) if the given page is compactible, non-0 otherwise.
 *
 * PUBLIC: int __bam_ispgcompactible __P((DBC *, db_pgno_t, DBT *, double));
 */
int
__bam_ispgcompactible(dbc, pgno, dbt, ff)
	DBC *dbc;
	db_pgno_t pgno;
	DBT *dbt;
	double ff;
{
	int ret, t_ret;
	double pgff;
	DB *dbp;
	DB_MPOOLFILE *dbmfp;
	PAGE *h;
	BTREE_CURSOR *cp;

	/* Check if the page is still capable of being compacted. 
	   There are several cases where the page is considered un-compactible.
	   1) Page compaction is disabled.
	   2) Page is root page.
	   3) The page is not a leaf page (page splited before we get here).
	   4) Page has been emptied before we get here.
	   5) The page has been filled before we get here.

	   If we are any of these cases, we are very done. */

	if (ff <= 0) { /* #1 */
		REASON(REASON_OFF);
		return (1);
	}

	if (pgno == dbc->internal->root) { /* #2 */
		REASON(REASON_ROOT);
		return (1);
	}

	/* Try lock */
	cp = (BTREE_CURSOR *)dbc->internal;
	if ((ret = __db_lget(dbc, 0, pgno, DB_LOCK_READ,
					DB_LOCK_NOWAIT, &cp->lock)) != 0)
		return (ret);

	dbp = dbc->dbp;
	dbmfp = dbp->mpf;

	if ((ret = __memp_fget(dbmfp, &pgno, 0, &h)) != 0) {
		(void)__LPUT(dbc, cp->lock);
		return (ret);
	}

	if (TYPE(h) != P_LBTREE) { /* #3 */
		REASON(REASON_NLEAF);
		goto error_out;
	}

	if (NUM_ENT(h) == 0) { /* #4 */
		REASON(REASON_EMPTY);
		goto error_out;
	}

	pgff = P_FREESPACE(dbp, h) / (double)(dbp->pgsize - SIZEOF_PAGE);
	if (pgff < (1 - ff)) { /* #5 */
		REASON(REASON_FULL);
		goto error_out;
	}

	/* We need the 1st key on the page. */
	memset(dbt, 0, sizeof(DBT));
	ret = __db_ret(dbp,
			h, 0, dbt, &dbt->data, &dbt->ulen);

    if (0) {
error_out:
		if (ret == 0)
			ret = 1;
    }

	if ((t_ret = __memp_fput(dbmfp, h, 0)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __LPUT(dbc, cp->lock)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}
