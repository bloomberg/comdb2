/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: db_dup.c,v 11.36 2003/06/30 17:19:44 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/lock.h"
#include "dbinc/mp.h"
#include "dbinc/db_am.h"
#include "dbinc/db_swap.h"
#include "dbinc/trigger_subscription.h"

#include <stdlib.h>
#include <alloca.h>
#include <comdb2rle.h>
#include <btree/bt_prefix.h>
#include <btree/bt_cache.h>
#include <logmsg.h>
#include <locks_wrap.h>

extern int gbl_keycompr;

/*
 * __db_ditem --
 *	Remove an item from a page.
 *
 * PUBLIC:  int __db_ditem __P((DBC *, PAGE *, u_int32_t, u_int32_t));
 */
int
__db_ditem(dbc, pagep, indx, nbytes)
	DBC *dbc;
	PAGE *pagep;
	u_int32_t indx, nbytes;
{
	++GET_BH_GEN(pagep);
	DB *dbp;
	db_indx_t cnt, *inp, offset;
	int ret;
	u_int8_t *from;

	dbp = dbc->dbp;
	if (DBC_LOGGING(dbc)) {
		DBT ldbt;
		int binternal_swap = 0;
		int overflow_swap = 0;
		uint32_t opcode = DB_REM_DUP;

		BINTERNAL *bi;
		BOVERFLOW *bo;
		BKEYDATA *bk;

		ldbt.data = P_ENTRY(dbp, pagep, indx);
		ldbt.size = nbytes;

		bi = ldbt.data;
		bo = ldbt.data;
		bk = ldbt.data;

		if (LOG_SWAPPED() && ldbt.size >= 12 &&
		    TYPE(pagep) == P_IBTREE) {
			M_16_SWAP(bi->len);
			M_32_SWAP(bi->pgno);
			M_32_SWAP(bi->nrecs);
			binternal_swap = 1;
		} else if (LOG_SWAPPED() && ldbt.size >= 12 &&
		    B_OVERFLOW == B_TYPE(bo)) {
			M_16_SWAP(bo->unused1);
			M_32_SWAP(bo->pgno);
			M_32_SWAP(bo->tlen);
			overflow_swap = 1;
		} else if (IS_PREFIX(pagep) && B_TYPE(bk) == B_KEYDATA) {
			opcode =
			    PUT_PFX_FLG(DB_REM_COMPR, bk_compressed(dbp, pagep,
				bk));
			// log uncompressed-record
			if (bk_decompress(dbp, pagep, &bk, alloca(KEYBUF),
				KEYBUF) != 0) {
				abort();
			}
			ldbt.data = bk;
			db_indx_t bklen;

			ASSIGN_ALIGN(db_indx_t, bklen, bk->len);
			ldbt.size = BKEYDATA_SIZE_FLUFFLESS(bklen);
		}
		ret = __db_addrem_log(dbp, dbc->txn,
		    &LSN(pagep), 0, opcode, PGNO(pagep), (u_int32_t)indx,
		    ldbt.size, &ldbt, NULL, &LSN(pagep));

		if (binternal_swap) {
			M_16_SWAP(bi->len);
			M_32_SWAP(bi->pgno);
			M_32_SWAP(bi->nrecs);
		} else if (overflow_swap) {
			M_16_SWAP(bo->unused1);
			M_32_SWAP(bo->pgno);
			M_32_SWAP(bo->tlen);
		}

		if (ret != 0)
			return (ret);

	} else
		LSN_NOT_LOGGED(LSN(pagep));

	/*
	 * If there's only a single item on the page, we don't have to
	 * work hard.
	 */
	if (NUM_ENT(pagep) == 1) {
		NUM_ENT(pagep) = 0;
		if (IS_PREFIX(pagep))
			HOFFSET(pagep) = *(P_PREFIX(dbp, pagep));
		else
			HOFFSET(pagep) = dbp->pgsize;
		return (0);
	}

	if (IS_PREFIX(pagep)) {
		BKEYDATA *bk = GET_BKEYDATA(dbp, pagep, indx);
		db_indx_t bklen;
		ASSIGN_ALIGN(db_indx_t, bklen, bk->len);
		if (B_TYPE(bk) == B_KEYDATA)
			nbytes = BKEYDATA_SIZE_FLUFFLESS(bklen);
	}

	inp = P_INP(dbp, pagep);
	/*
	 * Pack the remaining key/data items at the end of the page.  Use
	 * memmove(3), the regions may overlap.
	 */
	from = (u_int8_t *)pagep + HOFFSET(pagep);
	DB_ASSERT((int)inp[indx] - HOFFSET(pagep) >= 0);
	memmove(from + nbytes, from, inp[indx] - HOFFSET(pagep));
	HOFFSET(pagep) += nbytes;

	/* Adjust the indices' offsets. */
	offset = inp[indx];
	for (cnt = 0; cnt < NUM_ENT(pagep); ++cnt)
		if (inp[cnt] < offset)
			inp[cnt] += nbytes;

	/* Shift the indices down. */
	--NUM_ENT(pagep);
	if (indx != NUM_ENT(pagep)) {
		memmove(&inp[indx], &inp[indx + 1],
		    sizeof(db_indx_t) * (NUM_ENT(pagep) - indx));
	}
	return (0);
}

/*
 * __db_pitem_opcode --
 *  Put an item on a page.
 *
 * PUBLIC: int __db_pitem_opcode
 * PUBLIC:     __P((DBC *, PAGE *, u_int32_t, u_int32_t, DBT *, DBT *, u_int32_t));
 */
int
__db_pitem_opcode(dbc, pagep, indx, nbytes, hdr, data, opcode)
	DBC *dbc;
	PAGE *pagep;
	u_int32_t indx;
	u_int32_t nbytes;
	DBT *hdr, *data;
	u_int32_t opcode;
{
	++GET_BH_GEN(pagep);
	DBT thdr;
	BKEYDATA bk;
	DB *dbp;
	db_indx_t *inp;
	int ret;
	u_int8_t *p;

	dbp = dbc->dbp;

	/* If there is an active Lua trigger/consumer, wake it up. */
	struct __db_trigger_subscription *t = dbp->trigger_subscription;
	if (t && t->active && (indx & 1)) {
		Pthread_cond_signal(&t->cond);
	}

	/*
	 * Put a single item onto a page.  The logic figuring out where to
	 * insert and whether it fits is handled in the caller.  All we do
	 * here is manage the page shuffling.  We cheat a little bit in that
	 * we don't want to copy the dbt on a normal put twice.  If hdr is
	 * NULL, we create a BKEYDATA structure on the page, otherwise, just
	 * copy the caller's information onto the page.
	 *
	 * This routine is also used to put entries onto the page where the
	 * entry is pre-built, e.g., during recovery.  In this case, the hdr
	 * will point to the entry, and the data argument will be NULL.
	 *
	 * !!!
	 * There's a tremendous potential for off-by-one errors here, since
	 * the passed in header sizes must be adjusted for the structure's
	 * placeholder for the trailing variable-length data field.
	 */
	if (DBC_LOGGING(dbc)) {
		int binternal_swap = 0;
		int overflow_swap = 0;

		/* comdb2 endian changes */
		BOVERFLOW *bo = (hdr ? hdr->data : NULL);
		BINTERNAL *bi = (hdr ? hdr->data : NULL);

		/* endianize the overflow header before writing the log */
		if (hdr != NULL &&LOG_SWAPPED() && hdr->size >= 12 && bo &&
		    B_OVERFLOW == B_TYPE(bo)) {
			M_16_SWAP(bo->unused1);
			M_32_SWAP(bo->pgno);
			M_32_SWAP(bo->tlen);
			overflow_swap = 1;
		}
		/* endianize the b-internal header before writing the log */
		else if (hdr != NULL &&LOG_SWAPPED() && hdr->size >= 12 &&
		    TYPE(pagep) == P_IBTREE) {
			M_16_SWAP(bi->len);
			M_32_SWAP(bi->pgno);
			M_32_SWAP(bi->nrecs);
			binternal_swap = 1;
		}

		ret = __db_addrem_log(dbp, dbc->txn,
		    &LSN(pagep), 0, DB_ADD_DUP, PGNO(pagep),
		    (u_int32_t)indx, nbytes, hdr, data, &LSN(pagep));

		if (binternal_swap) {
			M_16_SWAP(bi->len);
			M_32_SWAP(bi->pgno);
			M_32_SWAP(bi->nrecs);
		} else if (overflow_swap) {
			M_16_SWAP(bo->unused1);
			M_32_SWAP(bo->pgno);
			M_32_SWAP(bo->tlen);
		}

		if (ret != 0)
			return (ret);
	} else {
		LSN_NOT_LOGGED(LSN(pagep));
	}

	DBT tdata;		/* temp-dbt to hold compressed data */
	DBT hdata;		/* temp-dbt to hold hdr data */
	int pfx = 0, rle = 0;
	if (IS_PREFIX(pagep)) {
		int compr;

		if (GET_ADDREM_OPCODE(opcode) == DB_REM_COMPR)
			compr = GET_PFX_FLG(opcode);
		else
			compr = (indx % 2 == 0);

		if (data == NULL) {
			// deconstruct pre-built hdr during recovery
			BKEYDATA *b = hdr->data;
			if (B_TYPE(b) == B_KEYDATA) {
				hdata.data = b->data;
				hdata.size = b->len;
				hdr = NULL;
				data = &hdata;
			}
		}

		if (data) {
			if (compr) {
				tdata.data = alloca(data->size);
				bk_compress(dbp, pagep, data, &tdata);
				pfx = tdata.flags & B_PFX;
				rle = tdata.flags & B_RLE;
				data = &tdata;
			}
			nbytes = BKEYDATA_SIZE_FLUFFLESS(data->size);
		}
	}

	if (hdr == NULL) {
		B_TSET(&bk, B_KEYDATA, 0, pfx, rle);
		bk.len = data == NULL ? 0 : data->size;

		thdr.data = &bk;
		thdr.size = SSZA(BKEYDATA, data);

		hdr = &thdr;
	}

	if (nbytes > P_FREESPACE(dbp, pagep)) {
        logmsg(LOGMSG_ERROR, "%s nbytes:%u freespace:%"PRId64"\n",                                                                         
            __func__, nbytes, (int64_t)P_FREESPACE(dbp, pagep));
		DB_ASSERT(nbytes <= P_FREESPACE(dbp, pagep));
		return (EINVAL);
	}

	inp = P_INP(dbp, pagep);

	/* Adjust the index table, then put the item on the page. */
	if (indx != NUM_ENT(pagep))
		memmove(&inp[indx + 1], &inp[indx],
		    sizeof(db_indx_t) * (NUM_ENT(pagep) - indx));
	HOFFSET(pagep) -= nbytes;
	inp[indx] = HOFFSET(pagep);
	++NUM_ENT(pagep);
	p = P_ENTRY(dbp, pagep, indx);
	memcpy(p, hdr->data, hdr->size);
	if (data != NULL)
		memcpy(p + hdr->size, data->data, data->size);
	return (0);
}

int bdb_relink_pglogs(void *bdb_state, unsigned char *fileid, db_pgno_t pgno,
    db_pgno_t prev_pgno, db_pgno_t next_pgno, DB_LSN lsn);

/*
 * __db_pitem --
 *  Put an item on a page.
 *
 * PUBLIC: int __db_pitem
 * PUBLIC:     __P((DBC *, PAGE *, u_int32_t, u_int32_t, DBT *, DBT *));
 */
int
__db_pitem(dbc, pagep, indx, nbytes, hdr, data)
	DBC *dbc;
	PAGE *pagep;
	u_int32_t indx;
	u_int32_t nbytes;
	DBT *hdr, *data;
{
	return __db_pitem_opcode(dbc, pagep, indx, nbytes, hdr, data, 0);
}

/*
 * __db_relink --
 *	Relink around a deleted page.
 *
 * PUBLIC: int __db_relink __P((DBC *, u_int32_t, PAGE *, PAGE **, int));
 */
int
__db_relink(dbc, add_rem, pagep, new_next, needlock)
	DBC *dbc;
	u_int32_t add_rem;
	PAGE *pagep, **new_next;
	int needlock;
{
	DB *dbp;
	PAGE *np, *pp;
	DB_LOCK npl, ppl;
	DB_LSN *nlsnp, *plsnp, ret_lsn;
	DB_MPOOLFILE *mpf;
	int ret;

	dbp = dbc->dbp;
	np = pp = NULL;
	LOCK_INIT(npl);
	LOCK_INIT(ppl);
	nlsnp = plsnp = NULL;
	mpf = dbp->mpf;
	ret = 0;

	/*
	 * Retrieve and lock the one/two pages.  For a remove, we may need
	 * two pages (the before and after).  For an add, we only need one
	 * because, the split took care of the prev.
	 */
	if (pagep->next_pgno != PGNO_INVALID) {
		if (needlock && (ret = __db_lget(dbc,
		    0, pagep->next_pgno, DB_LOCK_WRITE, 0, &npl)) != 0)
			goto err;
		if ((ret = __memp_fget(mpf, &pagep->next_pgno, 0, &np)) != 0) {
			ret = __db_pgerr(dbp, pagep->next_pgno, ret);
			goto err;
		}
		nlsnp = &np->lsn;
	}
	if (add_rem == DB_REM_PAGE && pagep->prev_pgno != PGNO_INVALID) {
		if (needlock && (ret = __db_lget(dbc,
		    0, pagep->prev_pgno, DB_LOCK_WRITE, 0, &ppl)) != 0)
			goto err;
		if ((ret = __memp_fget(mpf, &pagep->prev_pgno, 0, &pp)) != 0) {
			ret = __db_pgerr(dbp, pagep->prev_pgno, ret);
			goto err;
		}
		plsnp = &pp->lsn;
	}

	/* Log the change. */
	if (DBC_LOGGING(dbc)) {
		if ((ret = __db_relink_log(dbp, dbc->txn, &ret_lsn, 0, add_rem,
				pagep->pgno, &pagep->lsn, pagep->prev_pgno, plsnp,
				pagep->next_pgno, nlsnp)) != 0)
			goto err;

		if (bdb_relink_pglogs(dbp->dbenv->app_private, mpf->fileid,
			pagep->pgno, pagep->prev_pgno, pagep->next_pgno,
			ret_lsn) != 0) {
			logmsg(LOGMSG_FATAL, "%s: failed relink pglogs\n", __func__);
			abort();
		}
	} else
		LSN_NOT_LOGGED(ret_lsn);
	if (np != NULL)
		np->lsn = ret_lsn;
	if (pp != NULL)
		pp->lsn = ret_lsn;
	if (add_rem == DB_REM_PAGE)
		pagep->lsn = ret_lsn;

	/*
	 * Modify and release the two pages.
	 *
	 * !!!
	 * The parameter new_next gets set to the page following the page we
	 * are removing.  If there is no following page, then new_next gets
	 * set to NULL.
	 */
	if (np != NULL) {
		if (add_rem == DB_ADD_PAGE)
			np->prev_pgno = pagep->pgno;
		else
			np->prev_pgno = pagep->prev_pgno;
		if (new_next == NULL) {
			ret = __memp_fput(mpf, np, DB_MPOOL_DIRTY);
		} else {
			*new_next = np;
			ret = __memp_fset(mpf, np, DB_MPOOL_DIRTY);
		}
		if (ret != 0)
			goto err;
		if (needlock)
			(void)__TLPUT(dbc, npl);
	} else if (new_next != NULL)
		*new_next = NULL;

	if (pp != NULL) {
		pp->next_pgno = pagep->next_pgno;
		if ((ret = __memp_fput(mpf, pp, DB_MPOOL_DIRTY)) != 0)
			goto err;
		if (needlock)
			(void)__TLPUT(dbc, ppl);
	}
	return (0);

err:	if (np != NULL)
		(void)__memp_fput(mpf, np, 0);
	if (needlock)
		(void)__TLPUT(dbc, npl);
	if (pp != NULL)
		(void)__memp_fput(mpf, pp, 0);
	if (needlock)
		(void)__TLPUT(dbc, ppl);
	return (ret);
}
