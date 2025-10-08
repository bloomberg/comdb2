/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: lock_method.c,v 11.35 2003/06/30 17:20:15 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#ifdef HAVE_RPC
#include <rpc/rpc.h>
#endif

#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_shash.h"
#include "dbinc/lock.h"

#ifdef HAVE_RPC
#include "dbinc_auto/db_server.h"
#include "dbinc_auto/rpc_client_ext.h"
#endif

static int __lock_get_lk_conflicts __P((DB_ENV *, const u_int8_t **, int *));
static int __lock_set_lk_conflicts __P((DB_ENV *, u_int8_t *, int));
static int __lock_get_lk_detect __P((DB_ENV *, u_int32_t *));
static int __lock_get_lk_max_lockers __P((DB_ENV *, u_int32_t *));
static int __lock_get_lk_max_locks __P((DB_ENV *, u_int32_t *));
static int __lock_get_lk_max_objects __P((DB_ENV *, u_int32_t *));
static int __lock_get_env_timeout __P((DB_ENV *, db_timeout_t *, u_int32_t));

/*
 * __lock_dbenv_create --
 *	Lock specific creation of the DB_ENV structure.
 *
 * PUBLIC: void __lock_dbenv_create __P((DB_ENV *));
 */
void
__lock_dbenv_create(dbenv)
	DB_ENV *dbenv;
{
	/*
	 * !!!
	 * Our caller has not yet had the opportunity to reset the panic
	 * state or turn off mutex locking, and so we can neither check
	 * the panic state or acquire a mutex in the DB_ENV create path.
	 */

	dbenv->lk_max = DB_LOCK_DEFAULT_N;
	dbenv->lk_max_lockers = DB_LOCK_DEFAULT_N;
	dbenv->lk_max_objects = DB_LOCK_DEFAULT_N;

#ifdef	HAVE_RPC
	if (F_ISSET(dbenv, DB_ENV_RPCCLIENT)) {
		dbenv->get_lk_conflicts = __dbcl_get_lk_conflicts;
		dbenv->set_lk_conflicts = __dbcl_set_lk_conflict;
		dbenv->get_lk_detect = __dbcl_get_lk_detect;
		dbenv->set_lk_detect = __dbcl_set_lk_detect;
		dbenv->set_lk_max = __dbcl_set_lk_max;
		dbenv->get_lk_max_lockers = __dbcl_get_lk_max_lockers;
		dbenv->set_lk_max_lockers = __dbcl_set_lk_max_lockers;
		dbenv->get_lk_max_locks = __dbcl_get_lk_max_locks;
		dbenv->set_lk_max_locks = __dbcl_set_lk_max_locks;
		dbenv->get_lk_max_objects = __dbcl_get_lk_max_objects;
		dbenv->set_lk_max_objects = __dbcl_set_lk_max_objects;

		dbenv->lock_detect = __dbcl_lock_detect;
		dbenv->lock_dump_region = NULL;
		dbenv->lock_get = __dbcl_lock_get;
		dbenv->lock_id = __dbcl_lock_id;
		dbenv->lock_id_free = __dbcl_lock_id_free;
		dbenv->lock_put = __dbcl_lock_put;
		dbenv->lock_stat = __dbcl_lock_stat;
		dbenv->lock_vec = __dbcl_lock_vec;
	} else
#endif
	{
		dbenv->get_lk_conflicts = __lock_get_lk_conflicts;
		dbenv->set_lk_conflicts = __lock_set_lk_conflicts;
		dbenv->get_lk_detect = __lock_get_lk_detect;
		dbenv->set_lk_detect = __lock_set_lk_detect;
		dbenv->set_lk_max = __lock_set_lk_max;
		dbenv->get_lk_max_lockers = __lock_get_lk_max_lockers;
		dbenv->set_lk_max_lockers = __lock_set_lk_max_lockers;
		dbenv->get_lk_max_locks = __lock_get_lk_max_locks;
		dbenv->set_lk_max_locks = __lock_set_lk_max_locks;
		dbenv->get_lk_max_objects = __lock_get_lk_max_objects;
		dbenv->set_lk_max_objects = __lock_set_lk_max_objects;
		dbenv->get_timeout = __lock_get_env_timeout;
		dbenv->set_timeout = __lock_set_env_timeout;

		dbenv->lock_detect = __lock_detect_pp;
		dbenv->lock_dump_region = __lock_dump_region;
		dbenv->lock_get = __lock_get_pp;
		dbenv->lock_query = __lock_query_pp;

		dbenv->lock_abort_waiters = __lock_abort_waiters_pp;
		dbenv->lock_count_waiters = __lock_count_waiters_pp;
		dbenv->lock_count_write_waiters = __lock_count_write_waiters_pp;
		dbenv->lock_add_child_locker = __lock_add_child_locker_pp;
		dbenv->lock_id = __lock_id_pp;
		dbenv->lock_id_flags = __lock_id_flags_pp;
		dbenv->lock_id_free = __lock_id_free_pp;
		dbenv->lock_id_has_waiters = __lock_id_has_waiters_pp;
		dbenv->lock_id_set_logical_abort =
		    __lock_id_set_logical_abort_pp;
		dbenv->lock_put = __lock_put_pp;
        dbenv->locker_set_track = __locker_set_track_pp;
		dbenv->locker_set_timestamp = __locker_set_timestamp_pp;
		dbenv->locker_get_timestamp = __locker_get_timestamp_pp;
		dbenv->collect_locks = __lock_collect_pp;
		dbenv->lock_stat = __lock_stat_pp;
		dbenv->lock_locker_lockcount = __lock_locker_lockcount_pp;
		dbenv->lock_locker_pagelockcount =
		    __lock_locker_pagelockcount_pp;
		dbenv->lock_vec = __lock_vec_pp;
		dbenv->lock_to_dbt = __lock_to_dbt_pp;
	}
}

/*
 * __lock_dbenv_close --
 *	Lock specific destruction of the DB_ENV structure.
 *
 * PUBLIC: void __lock_dbenv_close __P((DB_ENV *));
 */
void
__lock_dbenv_close(dbenv)
	DB_ENV *dbenv;
{
	if (dbenv->lk_conflicts != NULL) {
		__os_free(dbenv, dbenv->lk_conflicts);
		dbenv->lk_conflicts = NULL;
	}
}

/*
 * __lock_get_lk_conflicts
 *	Get the conflicts matrix.
 */
static int
__lock_get_lk_conflicts(dbenv, lk_conflictsp, lk_modesp)
	DB_ENV *dbenv;
	const u_int8_t **lk_conflictsp;
	int *lk_modesp;
{
	if (lk_conflictsp != NULL)
		*lk_conflictsp = dbenv->lk_conflicts;
	if (lk_modesp != NULL)
		*lk_modesp = dbenv->lk_modes;
	return (0);
}

/*
 * __lock_set_lk_conflicts
 *	Set the conflicts matrix.
 */
static int
__lock_set_lk_conflicts(dbenv, lk_conflicts, lk_modes)
	DB_ENV *dbenv;
	u_int8_t *lk_conflicts;
	int lk_modes;
{
	int ret;

	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_lk_conflicts");

	if (dbenv->lk_conflicts != NULL) {
		__os_free(dbenv, dbenv->lk_conflicts);
		dbenv->lk_conflicts = NULL;
	}
	if ((ret = __os_malloc(dbenv,
	    lk_modes * lk_modes, &dbenv->lk_conflicts)) != 0)
		return (ret);
	memcpy(dbenv->lk_conflicts, lk_conflicts, lk_modes * lk_modes);
	dbenv->lk_modes = lk_modes;

	return (0);
}

static int
__lock_get_lk_detect(dbenv, lk_detectp)
	DB_ENV *dbenv;
	u_int32_t *lk_detectp;
{
	*lk_detectp = dbenv->lk_detect;
	return (0);
}

/*
 * __lock_set_lk_detect
 *	DB_ENV->set_lk_detect.
 *
 * PUBLIC: int __lock_set_lk_detect __P((DB_ENV *, u_int32_t));
 */
int
__lock_set_lk_detect(dbenv, lk_detect)
	DB_ENV *dbenv;
	u_int32_t lk_detect;
{
	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_lk_detect");

	switch (lk_detect) {
	case DB_LOCK_DEFAULT:
	case DB_LOCK_EXPIRE:
	case DB_LOCK_MAXLOCKS:
	case DB_LOCK_MINLOCKS:
	case DB_LOCK_MINWRITE:
	case DB_LOCK_MAXWRITE:
	case DB_LOCK_OLDEST:
	case DB_LOCK_RANDOM:
	case DB_LOCK_YOUNGEST:
	case DB_LOCK_YOUNGEST_EVER:
		break;
	default:
		__db_err(dbenv,
	    "DB_ENV->set_lk_detect: unknown deadlock detection mode specified");
		return (EINVAL);
	}
	dbenv->lk_detect = lk_detect;
	return (0);
}

/*
 * __lock_set_lk_max
 *	DB_ENV->set_lk_max.
 *
 * PUBLIC: int __lock_set_lk_max __P((DB_ENV *, u_int32_t));
 */
int
__lock_set_lk_max(dbenv, lk_max)
	DB_ENV *dbenv;
	u_int32_t lk_max;
{
	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_lk_max");

	dbenv->lk_max = lk_max;
	dbenv->lk_max_objects = lk_max;
	dbenv->lk_max_lockers = lk_max;
	return (0);
}

static int
__lock_get_lk_max_locks(dbenv, lk_maxp)
	DB_ENV *dbenv;
	u_int32_t *lk_maxp;
{
	*lk_maxp = dbenv->lk_max;
	return (0);
}

/*
 * __lock_set_lk_max_locks
 *	DB_ENV->set_lk_max_locks.
 *
 * PUBLIC: int __lock_set_lk_max_locks __P((DB_ENV *, u_int32_t));
 */
int
__lock_set_lk_max_locks(dbenv, lk_max)
	DB_ENV *dbenv;
	u_int32_t lk_max;
{
	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_lk_max_locks");

	dbenv->lk_max = lk_max;
	return (0);
}

static int
__lock_get_lk_max_lockers(dbenv, lk_maxp)
	DB_ENV *dbenv;
	u_int32_t *lk_maxp;
{
	*lk_maxp = dbenv->lk_max_lockers;
	return (0);
}

/*
 * __lock_set_lk_max_lockers
 *	DB_ENV->set_lk_max_lockers.
 *
 * PUBLIC: int __lock_set_lk_max_lockers __P((DB_ENV *, u_int32_t));
 */
int
__lock_set_lk_max_lockers(dbenv, lk_max)
	DB_ENV *dbenv;
	u_int32_t lk_max;
{
	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_lk_max_lockers");

	dbenv->lk_max_lockers = lk_max;
	return (0);
}

static int
__lock_get_lk_max_objects(dbenv, lk_maxp)
	DB_ENV *dbenv;
	u_int32_t *lk_maxp;
{
	*lk_maxp = dbenv->lk_max_objects;
	return (0);
}

/*
 * __lock_set_lk_max_objects
 *	DB_ENV->set_lk_max_objects.
 *
 * PUBLIC: int __lock_set_lk_max_objects __P((DB_ENV *, u_int32_t));
 */
int
__lock_set_lk_max_objects(dbenv, lk_max)
	DB_ENV *dbenv;
	u_int32_t lk_max;
{
	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_lk_max_objects");

	dbenv->lk_max_objects = lk_max;
	return (0);
}

static int
__lock_get_env_timeout(dbenv, timeoutp, flag)
	DB_ENV *dbenv;
	db_timeout_t *timeoutp;
	u_int32_t flag;
{
	switch (flag) {
	case DB_SET_LOCK_TIMEOUT:
		*timeoutp = dbenv->lk_timeout;
		break;
	case DB_SET_TXN_TIMEOUT:
		*timeoutp = dbenv->tx_timeout;
		break;
	default:
		return (__db_ferr(dbenv, "DB_ENV->get_timeout", 0));
		/* NOTREACHED */
	}

	return (0);
}

/*
 * __lock_set_env_timeout
 *	DB_ENV->set_lock_timeout.
 *
 * PUBLIC: int __lock_set_env_timeout __P((DB_ENV *, db_timeout_t, u_int32_t));
 */
int
__lock_set_env_timeout(dbenv, timeout, flags)
	DB_ENV *dbenv;
	db_timeout_t timeout;
	u_int32_t flags;
{
	DB_LOCKREGION *region;

	region = NULL;
	if (F_ISSET(dbenv, DB_ENV_OPEN_CALLED)) {
		if (!LOCKING_ON(dbenv))
			return (__db_env_config(
			    dbenv, "set_timeout", DB_INIT_LOCK));
		region = ((DB_LOCKTAB *)dbenv->lk_handle)->reginfo.primary;
	}

	switch (flags) {
	case DB_SET_LOCK_TIMEOUT:
		dbenv->lk_timeout = timeout;
		if (region != NULL)
			region->lk_timeout = timeout;
		break;
	case DB_SET_TXN_TIMEOUT:
		dbenv->tx_timeout = timeout;
		if (region != NULL)
			region->tx_timeout = timeout;
		break;
	default:
		return (__db_ferr(dbenv, "DB_ENV->set_timeout", 0));
		/* NOTREACHED */
	}

	return (0);
}
