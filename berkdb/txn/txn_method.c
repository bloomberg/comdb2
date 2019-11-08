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
#include <stdlib.h>
#include <sys/types.h>
#include <pthread.h>

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
#include <locks_wrap.h>

static int __txn_get_tx_max __P((DB_ENV *, u_int32_t *));
static int __txn_get_tx_timestamp __P((DB_ENV *, time_t *));
static int __txn_set_tx_timestamp __P((DB_ENV *, time_t *));
static void __txn_dump_ltrans __P((DB_ENV *, FILE *, u_int32_t));
static int __txn_set_logical_start __P((DB_ENV *,
	int (*)(DB_ENV *, void *, u_int64_t, DB_LSN *)));
static int __txn_set_logical_commit __P((DB_ENV *,
	int (*)(DB_ENV *, void *, u_int64_t, DB_LSN *)));

int gbl_use_perfect_ckp = 0;
pthread_key_t txn_key;
static pthread_once_t init_txn_key_once = PTHREAD_ONCE_INIT;

/*
 * __txn_init_key --
 * Initialize txn_key which is used by pefect checkpoints.
 */
static inline void
__txn_init_key(void)
{
	Pthread_key_create(&txn_key, NULL);
}

/*
 * __txn_get_first_dirty_begin_lsn --
 *  Return the begin LSN of the thread local DB_TXN object.
 *  If there is no thread local DB_TXN object (out of the context
 *  of any transactions or the work is transfered to another
 *  thread of control), return the default LSN value.
 *
 *  I don't think passing or returning a DB_LSN pointer
 *  is faster than simply using DB_LSN on 64-bit machines:
 *  DB_LSN is only 8 bytes long - same size as a pointer.
 *  Plus the caller does not need to take address of the argument
 *  or dereference the return value.
 *
 * PUBLIC: DB_LSN __txn_get_first_dirty_begin_lsn __P((DB_LSN));
 */
DB_LSN
__txn_get_first_dirty_begin_lsn(dfl)
	DB_LSN dfl;
{
	DB_TXN *thrlcltxn = pthread_getspecific(txn_key);

	if (thrlcltxn == NULL)
		return (dfl);

	if (thrlcltxn->parent != NULL) {
		do {
			thrlcltxn = thrlcltxn->parent;
		} while (thrlcltxn->parent != NULL);

		Pthread_setspecific(txn_key, thrlcltxn);
	}

	return (thrlcltxn->we_start_at_this_lsn);
}

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

	/* If we lazily initialize the key in __txn_begin(), Operations outside
	   the context of any transaction before the 1st call to __txn_begin()
	   may end up accessing an unintialized pthread key.
	   There will be multiple __txn_dbenv_create() calls (e.g., temptables)
	   thus it needs to be pthread_once. */
	if (gbl_use_perfect_ckp)
		pthread_once(&init_txn_key_once, __txn_init_key);
	/* Make a copy in the structure to improve locality. */
	dbenv->tx_perfect_ckp = gbl_use_perfect_ckp;
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
	logmsg(LOGMSG_USER, "LTRANID %016"PRIx64" ACTIVE-TXN %4d\n", lt->ltranid,
	    lt->active_txn_count);
}

static void
__txn_dump_ltrans(dbenv, f, flags)
	DB_ENV *dbenv;
	FILE *f;
	u_int32_t flags;
{
	LTDESC *lt, *lttemp;

	Pthread_mutex_lock(&dbenv->ltrans_active_lk);
	logmsg(LOGMSG_USER, "%4d ACTIVE LOGICAL TRANSACTIONS\n",
	    listc_size(&dbenv->active_ltrans));
	LISTC_FOR_EACH_SAFE(&dbenv->active_ltrans, lt, lttemp, lnk) {
		__txn_print_ltrans(dbenv, lt, f, flags);
	}
	Pthread_mutex_unlock(&dbenv->ltrans_active_lk);
}
