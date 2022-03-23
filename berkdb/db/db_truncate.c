/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2001-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: db_truncate.c,v 11.195 2003/07/02 15:39:51 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/hash.h"
#include "dbinc/qam.h"
#include "dbinc/txn.h"

static int __db_cursor_check __P((DB *));

/*
 * __db_truncate_pp
 *	DB->truncate pre/post processing.
 *
 * PUBLIC: int __db_truncate_pp __P((DB *, DB_TXN *, u_int32_t *, u_int32_t));
 */
int
__db_truncate_pp(dbp, txn, countp, flags)
	DB *dbp;
	DB_TXN *txn;
	u_int32_t *countp, flags;
{
	DB_ENV *dbenv;
	int handle_check, ret, txn_local;

	dbenv = dbp->dbenv;

	PANIC_CHECK(dbenv);

	/* Check for invalid flags. */
	if (F_ISSET(dbp, DB_AM_SECONDARY) && !LF_ISSET(DB_UPDATE_SECONDARY)) {
		__db_err(dbenv,
		    "DBP->truncate forbidden on secondary indices");
		return (EINVAL);
	}
	LF_CLR(DB_UPDATE_SECONDARY);
	if ((ret =
	    __db_fchk(dbenv, "DB->truncate", flags, DB_AUTO_COMMIT)) != 0)
		return (ret);

	/*
	 * Make sure there are no active cursors on this db.  Since we drop
	 * pages we cannot really adjust cursors.
	 */
	if (__db_cursor_check(dbp) != 0) {
		__db_err(dbenv,
		     "DB->truncate not permitted with active cursors");
		return (EINVAL);
	}

	/*
	 * Create local transaction as necessary, check for consistent
	 * transaction usage.
	 */
	txn_local = 0;
	if (IS_AUTO_COMMIT(dbenv, txn, flags)) {
		if ((ret = __db_txn_auto_init(dbenv, &txn)) != 0)
			goto err;
		txn_local = 1;
		LF_CLR(DB_AUTO_COMMIT);
	} else if (txn != NULL && !TXN_ON(dbenv)) {
		ret = __db_not_txn_env(dbenv);
		return (ret);
	}

	handle_check = IS_REPLICATED(dbenv, dbp);
	if (handle_check && (ret = __db_rep_enter(dbp, 1, txn != NULL)) != 0)
		goto err;

	ret = __db_truncate(dbp, txn, countp, flags);

	if (handle_check)
		__db_rep_exit(dbenv);

err:	return (txn_local ? __db_txn_auto_resolve(dbenv, txn, 0, ret) : ret);
}

/*
 * __db_truncate
 *	DB->truncate.
 *
 * PUBLIC: int __db_truncate __P((DB *, DB_TXN *, u_int32_t *, u_int32_t));
 */
int
__db_truncate(dbp, txn, countp, flags)
	DB *dbp;
	DB_TXN *txn;
	u_int32_t *countp, flags;
{
	DB *sdbp;
	DBC *dbc;
	DB_ENV *dbenv;
	u_int32_t scount;
	int ret, t_ret;

	COMPQUIET(flags, 0);

	dbenv = dbp->dbenv;
	dbc = NULL;
	ret = 0;

	DB_TEST_RECOVERY(dbp, DB_TEST_PREDESTROY, ret, NULL);

	/*
	 * Run through all secondaries and truncate them first.  The count
	 * returned is the count of the primary only.  QUEUE uses normal
	 * processing to truncate so it will update the secondaries normally.
	 */
	if (dbp->type != DB_QUEUE && LIST_FIRST(&dbp->s_secondaries) != NULL)
		for (sdbp = __db_s_first(dbp);
		    sdbp != NULL && ret == 0; ret = __db_s_next(&sdbp))
			if ((ret = __db_truncate(sdbp,
			     txn, &scount, DB_UPDATE_SECONDARY)) != 0)
				return (ret);

	/* Acquire a cursor. */
	if ((ret = __db_cursor(dbp, txn, &dbc, 0)) != 0)
		return (ret);

	switch (dbp->type) {
	case DB_BTREE:
	case DB_RECNO:
		ret = __bam_truncate(dbc, countp);
		break;
	case DB_HASH:
		ret = __ham_truncate(dbc, countp);
		break;
	case DB_QUEUE:
		ret = __qam_truncate(dbc, countp);
		break;
	case DB_UNKNOWN:
	default:
		ret = __db_unknown_type(dbenv, "DB->truncate", dbp->type);
		break;
	}

	/* Discard the cursor. */
	if (dbc != NULL && (t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;

	DB_TEST_RECOVERY(dbp, DB_TEST_POSTDESTROY, ret, NULL);

DB_TEST_RECOVERY_LABEL

	return (ret);
}

/*
 * __db_cursor_check --
 *	See if there are any active cursors on this db.
 */
static int
__db_cursor_check(dbp)
	DB *dbp;
{
	DB *ldbp;
	DBC *dbc;
	DB_ENV *dbenv;
	DB_CQ *cq;
	int found;

	dbenv = dbp->dbenv;

	MUTEX_THREAD_LOCK(dbenv, dbenv->dblist_mutexp);
	for (found = 0, ldbp = __dblist_get(dbenv, dbp->adj_fileid);
	    ldbp != NULL && ldbp->adj_fileid == dbp->adj_fileid;
	    ldbp = LIST_NEXT(ldbp, dblistlinks)) {
		for (dbc = __db_lock_aq(dbp, ldbp, &cq);
		    dbc != NULL; dbc = TAILQ_NEXT(dbc, links)) {
			if (IS_INITIALIZED(dbc)) {
				found = 1;
				break;
			}
		}
		__db_unlock_aq(dbp, ldbp, cq);
		if (found == 1)
			break;
	}
	MUTEX_THREAD_UNLOCK(dbenv, dbenv->dblist_mutexp);

	return (found);
}
