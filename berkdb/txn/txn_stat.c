/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: txn_stat.c,v 11.22 2003/09/13 19:20:43 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_swap.h"
#include "dbinc/log.h"
#include "dbinc/txn.h"
#include <logmsg.h>

static int __txn_stat __P((DB_ENV *, DB_TXN_STAT **, u_int32_t));

/*
 * __txn_stat_pp --
 *	DB_ENV->txn_stat pre/post processing.
 *
 * PUBLIC: int __txn_stat_pp __P((DB_ENV *, DB_TXN_STAT **, u_int32_t));
 */
int
__txn_stat_pp(dbenv, statp, flags)
	DB_ENV *dbenv;
	DB_TXN_STAT **statp;
	u_int32_t flags;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv, dbenv->tx_handle, "txn_stat", DB_INIT_TXN);

	if ((ret = __db_fchk(dbenv,
		"DB_ENV->txn_stat", flags, DB_STAT_CLEAR)) != 0)
		return (ret);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __txn_stat(dbenv, statp, flags);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __txn_stat --
 *	DB_ENV->txn_stat.
 */
static int
__txn_stat(dbenv, statp, flags)
	DB_ENV *dbenv;
	DB_TXN_STAT **statp;
	u_int32_t flags;
{
	DB_TXNMGR *mgr;
	DB_TXNREGION *region;
	DB_TXN_STAT *stats;
	TXN_DETAIL *txnp;
	DB_LOGC *dbc = NULL;
	DBT dbt = { 0 };
	__txn_ckp_args *ckp = NULL;
	size_t nbytes;
	int32_t type;
	u_int32_t maxtxn, ndx;
	int ret;

	*statp = NULL;
	mgr = dbenv->tx_handle;
	region = mgr->reginfo.primary;

	/*
	 * Allocate for the maximum active transactions -- the DB_TXN_ACTIVE
	 * struct is small and the maximum number of active transactions is
	 * not going to be that large.  Don't have to lock anything to look
	 * at the region's maximum active transactions value, it's read-only
	 * and never changes after the region is created.
	 *
	 * The maximum active transactions isn't a hard limit, so allocate
	 * some extra room, and don't walk off the end.
	 */
	maxtxn = region->maxtxns + (region->maxtxns / 10) + 10;
	nbytes = sizeof(DB_TXN_STAT) + sizeof(DB_TXN_ACTIVE) * maxtxn;
	if ((ret = __os_umalloc(dbenv, nbytes, &stats)) != 0)
		return (ret);
	ret = dbenv->log_cursor(dbenv, &dbc, 0);
	if (ret) {
		logmsg(LOGMSG_ERROR, "log_cursor ret %d\n", ret);
		return (ret);
	}
	dbt.flags = DB_DBT_MALLOC;

	R_LOCK(dbenv, &mgr->reginfo);
	memcpy(stats, &region->stat, sizeof(*stats));
	stats->st_last_txnid = region->last_txnid;
	stats->st_last_ckp = region->last_ckp;
	ZERO_LSN(stats->st_ckp_lsn);
	ret = dbc->get(dbc, &stats->st_last_ckp, &dbt, DB_SET);
	if (!ret) {
		LOGCOPY_32(&type, dbt.data);
		if (type == DB___txn_ckp) {
			ret = __txn_ckp_read(dbenv, dbt.data, &ckp);
			if (ret == 0) {
				stats->st_ckp_lsn = ckp->ckp_lsn;
			}
		}
	}
	if (dbt.data)
		__os_ufree(dbenv, dbt.data);
	if (ckp)
		__os_free(dbenv, ckp);
	if (dbc)
		dbc->close(dbc, 0);

	stats->st_time_ckp = region->time_ckp;
	stats->st_txnarray = (DB_TXN_ACTIVE *)&stats[1];

	for (ndx = 0,
		txnp = SH_TAILQ_FIRST(&region->active_txn, __txn_detail);
		txnp != NULL && ndx < maxtxn;
		txnp = SH_TAILQ_NEXT(txnp, links, __txn_detail), ++ndx) {
		stats->st_txnarray[ndx].txnid = txnp->txnid;
		if (txnp->parent == INVALID_ROFF)
			stats->st_txnarray[ndx].parentid = TXN_INVALID;
		else
			stats->st_txnarray[ndx].parentid =
				((TXN_DETAIL *)R_ADDR(&mgr->reginfo,
				txnp->parent))->txnid;
		stats->st_txnarray[ndx].lsn = txnp->begin_lsn;
		if ((stats->st_txnarray[ndx].xa_status = txnp->xa_status) != 0)
			memcpy(stats->st_txnarray[ndx].xid,
				txnp->xid, DB_XIDDATASIZE);
	}

	stats->st_region_wait = mgr->reginfo.rp->mutex.mutex_set_wait;
	stats->st_region_nowait = mgr->reginfo.rp->mutex.mutex_set_nowait;
	stats->st_regsize = mgr->reginfo.rp->size;
	if (LF_ISSET(DB_STAT_CLEAR)) {
		mgr->reginfo.rp->mutex.mutex_set_wait = 0;
		mgr->reginfo.rp->mutex.mutex_set_nowait = 0;
		memset(&region->stat, 0, sizeof(region->stat));
		region->stat.st_maxtxns = region->maxtxns;
		region->stat.st_maxnactive =
			region->stat.st_nactive = stats->st_nactive;
	}

	R_UNLOCK(dbenv, &mgr->reginfo);

	*statp = stats;
	return (0);
}
