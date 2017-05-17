/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: txn_method.c,v 11.66 2003/06/30 17:20:30 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#ifdef HAVE_RPC
#include <rpc/rpc.h>
#endif

#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/txn.h"

#ifdef HAVE_RPC
#include "dbinc_auto/db_server.h"
#include "dbinc_auto/rpc_client_ext.h"
#endif

#include "logmsg.h"

static int __txn_get_tx_max __P((DB_ENV *, u_int32_t *));
static int __txn_get_tx_timestamp __P((DB_ENV *, time_t *));
static int __txn_set_tx_timestamp __P((DB_ENV *, time_t *));
static void __txn_dump_ltrans __P((DB_ENV *, FILE *, u_int32_t));
static int __txn_set_logical_start __P((DB_ENV *,
	int (*)(DB_ENV *, void *, u_int64_t, DB_LSN *)));
static int __txn_set_logical_commit __P((DB_ENV *,
	int (*)(DB_ENV *, void *, u_int64_t, DB_LSN *)));


/*
 * __txn_dbenv_create --
 *	Transaction specific initialization of the DB_ENV structure.
 *
 * PUBLIC: void __txn_dbenv_create __P((DB_ENV *));
 */
void
__txn_dbenv_create(dbenv)
	DB_ENV *dbenv;
{
	/*
	 * !!!
	 * Our caller has not yet had the opportunity to reset the panic
	 * state or turn off mutex locking, and so we can neither check
	 * the panic state or acquire a mutex in the DB_ENV create path.
	 */

	dbenv->tx_max = DEF_MAX_TXNS;

#ifdef HAVE_RPC
	if (F_ISSET(dbenv, DB_ENV_RPCCLIENT)) {
		dbenv->get_tx_max = __dbcl_get_tx_max;
		dbenv->set_tx_max = __dbcl_set_tx_max;
		dbenv->get_tx_timestamp = __dbcl_get_tx_timestamp;
		dbenv->set_tx_timestamp = __dbcl_set_tx_timestamp;

		dbenv->txn_checkpoint = __dbcl_txn_checkpoint;
		dbenv->txn_recover = __dbcl_txn_recover;
		dbenv->txn_stat = __dbcl_txn_stat;
		dbenv->txn_begin = __dbcl_txn_begin;
	} else
#endif
	{
		dbenv->get_tx_max = __txn_get_tx_max;
		dbenv->set_tx_max = __txn_set_tx_max;
		dbenv->get_tx_timestamp = __txn_get_tx_timestamp;
		dbenv->set_tx_timestamp = __txn_set_tx_timestamp;

		dbenv->txn_checkpoint = __txn_checkpoint_pp;
		dbenv->txn_recover = __txn_recover_pp;
		dbenv->txn_stat = __txn_stat_pp;
		dbenv->txn_begin = __txn_begin_pp;
		dbenv->txn_dump_ltrans =  __txn_dump_ltrans;
		dbenv->lowest_logical_lsn = __txn_ltrans_find_lowest_lsn;
		dbenv->ltran_count = __txn_count_ltrans;
		dbenv->get_ltran_list = __txn_get_ltran_list;
		dbenv->set_logical_start = __txn_set_logical_start;
		dbenv->set_logical_commit = __txn_set_logical_commit;
		dbenv->txn_begin_set_retries = __txn_begin_set_retries_pp;
	}
}

static int
__txn_set_logical_start(dbenv, l_start)
	DB_ENV *dbenv;
	int (*l_start) __P((DB_ENV *, void *state, u_int64_t ltranid,
	DB_LSN *lsn));
{
	PANIC_CHECK(dbenv);
	if (l_start == NULL) {
		__db_err(dbenv,
		    "DB_ENV->set_logical_start no start function specified");
		return (EINVAL);
	}
	dbenv->txn_logical_start = l_start;
	return (0);
}

static int
__txn_set_logical_commit(dbenv, l_commit)
	DB_ENV *dbenv;
	int (*l_commit) __P((DB_ENV *, void *state, u_int64_t ltranid,
	DB_LSN *lsn));
{
	PANIC_CHECK(dbenv);
	if (l_commit == NULL) {
		__db_err(dbenv,
		    "DB_ENV->set_logical_commit no commit function specified");
		return (EINVAL);
	}
	dbenv->txn_logical_commit = l_commit;
	return (0);
}


static int
__txn_get_tx_max(dbenv, tx_maxp)
	DB_ENV *dbenv;
	u_int32_t *tx_maxp;
{
	*tx_maxp = dbenv->tx_max;
	return (0);
}

/*
 * __txn_set_tx_max --
 *	DB_ENV->set_tx_max.
 *
 * PUBLIC: int __txn_set_tx_max __P((DB_ENV *, u_int32_t));
 */
int
__txn_set_tx_max(dbenv, tx_max)
	DB_ENV *dbenv;
	u_int32_t tx_max;
{
	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_tx_max");

	dbenv->tx_max = tx_max;
	return (0);
}

static int
__txn_get_tx_timestamp(dbenv, timestamp)
	DB_ENV *dbenv;
	time_t *timestamp;
{
	*timestamp = dbenv->tx_timestamp;
	return (0);
}

/*
 * __txn_set_tx_timestamp --
 *	Set the transaction recovery timestamp.
 */
static int
__txn_set_tx_timestamp(dbenv, timestamp)
	DB_ENV *dbenv;
	time_t *timestamp;
{
	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_tx_timestamp");

	dbenv->tx_timestamp = *timestamp;
	return (0);
}

static inline void
__txn_print_ltrans(dbenv, lt, f, flags)
	DB_ENV *dbenv;
	LTDESC *lt;
	FILE *f;
	u_int32_t flags;
{
	logmsg(LOGMSG_USER, "LTRANID %016llx ACTIVE-TXN %4d\n", lt->ltranid,
	    lt->active_txn_count);
}

static void
__txn_dump_ltrans(dbenv, f, flags)
	DB_ENV *dbenv;
	FILE *f;
	u_int32_t flags;
{
	LTDESC *lt, *lttemp;

	pthread_mutex_lock(&dbenv->ltrans_active_lk);
	logmsg(LOGMSG_USER, "%4d ACTIVE LOGICAL TRANSACTIONS\n",
	    listc_size(&dbenv->active_ltrans));
	LISTC_FOR_EACH_SAFE(&dbenv->active_ltrans, lt, lttemp, lnk) {
		__txn_print_ltrans(dbenv, lt, f, flags);
	}
	pthread_mutex_unlock(&dbenv->ltrans_active_lk);
}
