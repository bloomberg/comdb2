/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1999-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: hash_meta.c,v 11.24 2003/09/09 16:46:10 ubell Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/hash.h"
#include "dbinc/lock.h"
#include "dbinc/mp.h"

/*
 * Acquire the meta-data page.
 *
 * PUBLIC: int __ham_get_meta __P((DBC *));
 */
int
__ham_get_meta(dbc)
	DBC *dbc;
{
	DB *dbp;
	DB_ENV *dbenv;
	DB_MPOOLFILE *mpf;
	HASH *hashp;
	HASH_CURSOR *hcp;
	int ret;

	dbp = dbc->dbp;
	dbenv = dbp->dbenv;
	mpf = dbp->mpf;
	hashp = dbp->h_internal;
	hcp = (HASH_CURSOR *)dbc->internal;

	if (dbenv != NULL &&
	    STD_LOCKING(dbc) && !F_ISSET(dbc, DBC_RECOVER | DBC_COMPENSATE)) {
		dbc->lock.pgno = hashp->meta_pgno;
		if ((ret = __lock_get(dbenv, dbc->locker,
		    DB_NONBLOCK(dbc) ? DB_LOCK_NOWAIT : 0,
		    &dbc->lock_dbt, DB_LOCK_READ, &hcp->hlock)) != 0)
			return ((ret == DB_LOCK_NOTGRANTED &&
			     !F_ISSET(dbenv, DB_ENV_TIME_NOTGRANTED)) ?
			     DB_LOCK_DEADLOCK : ret);

	}

	if ((ret = __memp_fget(mpf,
	    &hashp->meta_pgno, DB_MPOOL_CREATE, &(hcp->hdr))) != 0 &&
	    LOCK_ISSET(hcp->hlock))
		(void)__lock_put(dbenv, &hcp->hlock);

	return (ret);
}

/*
 * Release the meta-data page.
 *
 * PUBLIC: int __ham_release_meta __P((DBC *));
 */
int
__ham_release_meta(dbc)
	DBC *dbc;
{
	DB_MPOOLFILE *mpf;
	HASH_CURSOR *hcp;

	mpf = dbc->dbp->mpf;
	hcp = (HASH_CURSOR *)dbc->internal;

	if (hcp->hdr)
		(void)__memp_fput(mpf, hcp->hdr,
		    F_ISSET(hcp, H_DIRTY) ? DB_MPOOL_DIRTY : 0);
	hcp->hdr = NULL;
	if (!F_ISSET(dbc, DBC_RECOVER | DBC_COMPENSATE) &&
	    dbc->txn == NULL && LOCK_ISSET(hcp->hlock))
		(void)__lock_put(dbc->dbp->dbenv, &hcp->hlock);
	F_CLR(hcp, H_DIRTY);

	return (0);
}

/*
 * Mark the meta-data page dirty.
 *
 * PUBLIC: int __ham_dirty_meta __P((DBC *));
 */
int
__ham_dirty_meta(dbc)
	DBC *dbc;
{
	DB *dbp;
	DB_ENV *dbenv;
	DB_LOCK _tmp;
	HASH *hashp;
	HASH_CURSOR *hcp;
	int ret;

	dbp = dbc->dbp;
	hashp = dbp->h_internal;
	hcp = (HASH_CURSOR *)dbc->internal;

	ret = 0;
	dbenv = dbp->dbenv;
	if (STD_LOCKING(dbc) && !F_ISSET(dbc, DBC_RECOVER | DBC_COMPENSATE)) {
		dbc->lock.pgno = hashp->meta_pgno;
		if ((ret = __lock_get(dbenv, dbc->locker,
		    DB_NONBLOCK(dbc) ? DB_LOCK_NOWAIT : 0,
		    &dbc->lock_dbt, DB_LOCK_WRITE, &_tmp)) == 0) {
			ret = __lock_put(dbenv, &hcp->hlock);
			hcp->hlock = _tmp;
		}
	}

	if (ret == 0)
		F_SET(hcp, H_DIRTY);
	return ((ret == DB_LOCK_NOTGRANTED &&
	     !F_ISSET(dbenv, DB_ENV_TIME_NOTGRANTED)) ?
	     DB_LOCK_DEADLOCK : ret);
}
