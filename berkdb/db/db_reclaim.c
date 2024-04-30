/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: db_reclaim.c,v 11.37 2003/06/30 17:19:47 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/btree.h"
#include "dbinc/mp.h"

/*
 * __db_traverse_big
 *	Traverse a chain of overflow pages and call the callback routine
 * on each one.  The calling convention for the callback is:
 *	callback(dbp, page, cookie, did_put),
 * where did_put is a return value indicating if the page in question has
 * already been returned to the mpool.
 *
 * PUBLIC: int __db_traverse_big __P((DBC *, DB *,
 * PUBLIC:     db_pgno_t, int (*)(DB *, PAGE *, void *, int *), void *));
 */
int
__db_traverse_big(dbc, dbp, pgno, callback, cookie)
	DBC *dbc;
	DB *dbp;
	db_pgno_t pgno;
	int (*callback) __P((DB *, PAGE *, void *, int *));
	void *cookie;
{
	DB_MPOOLFILE *mpf;
	PAGE *p;
	int did_put, ret;

	mpf = dbp->mpf;

	do {
		did_put = 0;
		if ((ret = PAGEGET(dbc, mpf, &pgno, 0, &p)) != 0)
			return (ret);
		pgno = NEXT_PGNO(p);
		if ((ret = callback(dbp, p, cookie, &did_put)) == 0 &&
		    !did_put)
			ret = PAGEPUT(dbc, mpf, p, 0);
	} while (ret == 0 && pgno != PGNO_INVALID);

	return (ret);
}

/*
 * __db_reclaim_callback
 * This is the callback routine used during a delete of a subdatabase.
 * we are traversing a btree or hash table and trying to free all the
 * pages.  Since they share common code for duplicates and overflow
 * items, we traverse them identically and use this routine to do the
 * actual free.  The reason that this is callback is because hash uses
 * the same traversal code for statistics gathering.
 *
 * PUBLIC: int __db_reclaim_callback __P((DB *, PAGE *, void *, int *));
 */
int
__db_reclaim_callback(dbp, p, cookie, putp)
	DB *dbp;
	PAGE *p;
	void *cookie;
	int *putp;
{
	int ret;

	COMPQUIET(dbp, NULL);

	if ((ret = __db_free(cookie, p)) != 0)
		return (ret);
	*putp = 1;

	return (0);
}

/*
 * __db_truncate_callback
 * This is the callback routine used during a truncate.
 * we are traversing a btree or hash table and trying to free all the
 * pages.
 *
 * PUBLIC: int __db_truncate_callback __P((DB *, PAGE *, void *, int *));
 */
int
__db_truncate_callback(dbp, p, cookie, putp)
	DB *dbp;
	PAGE *p;
	void *cookie;
	int *putp;
{
	DB_MPOOLFILE *mpf;
	db_indx_t indx, len, off, tlen, top;
	db_trunc_param *param;
	u_int8_t *hk, type;
	int ret;

	top = NUM_ENT(p);
	mpf = dbp->mpf;
	param = cookie;
	*putp = 1;

	switch (TYPE(p)) {
	case P_LBTREE:
		/* Skip for off-page duplicates and deleted items. */
		for (indx = 0; indx < top; indx += P_INDX) {
			BKEYDATA *bk = GET_BKEYDATA(dbp, p, indx + O_INDX);
			if (!B_DISSET(bk) && B_TYPE(bk) != B_DUPLICATE)
				++param->count;
		}
		/* FALLTHROUGH */
	case P_IBTREE:
	case P_IRECNO:
	case P_INVALID:
		if (dbp->type != DB_HASH &&
		    ((BTREE *)dbp->bt_internal)->bt_root == PGNO(p)) {
			type = dbp->type == DB_RECNO ? P_LRECNO : P_LBTREE;
			goto reinit;
		}
		break;
	case P_OVERFLOW:
		if (DBC_LOGGING(param->dbc)) {
			if ((ret = __db_ovref_log(dbp, param->dbc->txn,
			    &LSN(p), 0, p->pgno, -1, &LSN(p))) != 0)
				return (ret);
		} else
			LSN_NOT_LOGGED(LSN(p));
		if (--OV_REF(p) != 0)
			*putp = 0;
		break;
	case P_LRECNO:
		for (indx = 0; indx < top; indx += O_INDX) {
			if (!B_DISSET(GET_BKEYDATA(dbp, p, indx)))
				++param->count;
		}

		if (((BTREE *)dbp->bt_internal)->bt_root == PGNO(p)) {
			type = P_LRECNO;
			goto reinit;
		}
		break;
	case P_LDUP:
		/* Correct for deleted items. */
		for (indx = 0; indx < top; indx += O_INDX)
			if (!B_DISSET(GET_BKEYDATA(dbp, p, indx)))
				++param->count;

		break;
	case P_HASH:
		/* Correct for on-page duplicates and deleted items. */
		for (indx = 0; indx < top; indx += P_INDX) {
			switch (*H_PAIRDATA(dbp, p, indx)) {
			case H_OFFDUP:
			case H_OFFPAGE:
				break;
			case H_KEYDATA:
				++param->count;
				break;
			case H_DUPLICATE:
				tlen = LEN_HDATA(dbp, p, 0, indx);
				hk = H_PAIRDATA(dbp, p, indx);
				for (off = 0; off < tlen;
				    off += len + 2 * sizeof (db_indx_t)) {
					++param->count;
					memcpy(&len,
					    HKEYDATA_DATA(hk)
					    + off, sizeof(db_indx_t));
				}
				break;
			default:
				return (__db_pgfmt(dbp->dbenv, p->pgno));
			}
		}
		/* Don't free the head of the bucket. */
		if (PREV_PGNO(p) == PGNO_INVALID) {
			type = P_HASH;

reinit:			*putp = 0;
			if (DBC_LOGGING(param->dbc)) {
				if ((ret = __db_free(param->dbc, p)) != 0)
					return (ret);
				if ((ret = __db_new(param->dbc, type, &p)) != 0)
					return (ret);
			} else
				LSN_NOT_LOGGED(LSN(p));

			P_INIT(p, dbp->pgsize, PGNO(p), PGNO_INVALID,
			    PGNO_INVALID, type == P_HASH ? 0 : 1, type);
		}
		break;
	default:
		return (__db_pgfmt(dbp->dbenv, p->pgno));
	}

	if (*putp == 1) {
		if ((ret = __db_free(param->dbc, p)) != 0)
			return (ret);
	} else {
		if ((ret = __memp_fput(mpf, p, DB_MPOOL_DIRTY)) != 0)
			return (ret);
		*putp = 1;
	}

	return (0);
}
