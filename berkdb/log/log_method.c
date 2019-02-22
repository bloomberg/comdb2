/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1999-2003
 *	Sleepycat Software.  All rights reserved.
 */
#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: log_method.c,v 11.38 2003/06/30 17:20:16 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#ifdef HAVE_RPC
#include <rpc/rpc.h>
#endif

#include <stdlib.h>
#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/log.h"

#ifdef HAVE_RPC
#include "dbinc_auto/db_server.h"
#include "dbinc_auto/rpc_client_ext.h"
#endif

static int __log_get_lg_nsegs __P((DB_ENV *, u_int32_t *));
static int __log_set_lg_nsegs __P((DB_ENV *, u_int32_t));
static int __log_get_lg_bsize __P((DB_ENV *, u_int32_t *));
static int __log_get_lg_dir __P((DB_ENV *, const char **));
static int __log_get_lg_max __P((DB_ENV *, u_int32_t *));
static int __log_get_lg_regionmax __P((DB_ENV *, u_int32_t *));

/*
 * __log_dbenv_create --
 *	Log specific initialization of the DB_ENV structure.
 *
 * PUBLIC: void __log_dbenv_create __P((DB_ENV *));
 */
void
__log_dbenv_create(dbenv)
	DB_ENV *dbenv;
{
	/*
	 * !!!
	 * Our caller has not yet had the opportunity to reset the panic
	 * state or turn off mutex locking, and so we can neither check
	 * the panic state or acquire a mutex in the DB_ENV create path.
	 */

	dbenv->lg_nsegs = LG_NSEGS_DEFAULT;
	dbenv->lg_bsize = LG_BSIZE_DEFAULT;
	dbenv->lg_regionmax = LG_BASE_REGION_SIZE;


#ifdef	HAVE_RPC
	/*
	 * If we have a client, overwrite what we just setup to
	 * point to client functions.
	 */
	if (F_ISSET(dbenv, DB_ENV_RPCCLIENT)) {
		dbenv->get_lg_bsize = __dbcl_get_lg_bsize;
		dbenv->set_lg_bsize = __dbcl_set_lg_bsize;
		dbenv->get_lg_dir = __dbcl_get_lg_dir;
		dbenv->set_lg_dir = __dbcl_set_lg_dir;
		dbenv->get_lg_max = __dbcl_get_lg_max;
		dbenv->set_lg_max = __dbcl_set_lg_max;
		dbenv->get_lg_regionmax = __dbcl_get_lg_regionmax;
		dbenv->set_lg_regionmax = __dbcl_set_lg_regionmax;

		dbenv->log_archive = __dbcl_log_archive;
		dbenv->log_cursor = __dbcl_log_cursor;
		dbenv->log_file = __dbcl_log_file;
		dbenv->log_flush = __dbcl_log_flush;
		dbenv->log_put = __dbcl_log_put;
		dbenv->log_stat = __dbcl_log_stat;
		dbenv->log_get_last_lsn = __dbcl_log_get_last_lsn;
	} else
#endif
	{
		dbenv->get_lg_bsize = __log_get_lg_bsize;
		dbenv->set_lg_bsize = __log_set_lg_bsize;
		dbenv->get_lg_nsegs = __log_get_lg_nsegs;
		dbenv->set_lg_nsegs = __log_set_lg_nsegs;
		dbenv->get_lg_dir = __log_get_lg_dir;
		dbenv->set_lg_dir = __log_set_lg_dir;
		dbenv->get_lg_max = __log_get_lg_max;
		dbenv->set_lg_max = __log_set_lg_max;
		dbenv->get_lg_regionmax = __log_get_lg_regionmax;
		dbenv->set_lg_regionmax = __log_set_lg_regionmax;

		dbenv->log_archive = __log_archive_pp;
		dbenv->log_cursor = __log_cursor_pp;
		dbenv->log_file = __log_file_pp;
		dbenv->log_flush = __log_flush_pp;
		dbenv->log_put = __log_put_pp;
		dbenv->log_stat = __log_stat_pp;
		dbenv->log_get_last_lsn = __log_get_last_lsn_pp;
	}
}

static int
__log_get_lg_nsegs(dbenv, lg_nsegsp)
	DB_ENV *dbenv;
	u_int32_t *lg_nsegsp;
{
	*lg_nsegsp = dbenv->lg_nsegs;
	return (0);
}

static int
__log_set_lg_nsegs(dbenv, lg_nsegs)
	DB_ENV *dbenv;
	u_int32_t lg_nsegs;
{

	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_lg_bsize");

	if (lg_nsegs == 0)
		lg_nsegs = LG_NSEGS_DEFAULT;

	dbenv->lg_nsegs = lg_nsegs;
	return (0);
}

static int
__log_get_lg_bsize(dbenv, lg_bsizep)
	DB_ENV *dbenv;
	u_int32_t *lg_bsizep;
{
	*lg_bsizep = dbenv->lg_bsize;
	return (0);
}

/*
 * __log_set_lg_bsize --
 *	DB_ENV->set_lg_bsize.
 *
 * PUBLIC: int __log_set_lg_bsize __P((DB_ENV *, u_int32_t));
 */
int
__log_set_lg_bsize(dbenv, lg_bsize)
	DB_ENV *dbenv;
	u_int32_t lg_bsize;
{
	u_int32_t lg_max;

	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_lg_bsize");

	if (lg_bsize == 0)
		lg_bsize = LG_BSIZE_DEFAULT;

	/* Let's not be silly. */
	lg_max = dbenv->lg_size == 0 ? LG_MAX_DEFAULT : dbenv->lg_size;
	if (lg_bsize > lg_max / 4) {
		__db_err(dbenv,
		    "log buffer * log segments (%d) must be <= log file size(%d) / 4", lg_bsize, lg_max);
		return (EINVAL);
	}

	dbenv->lg_bsize = lg_bsize;
	return (0);
}

static int
__log_get_lg_max(dbenv, lg_maxp)
	DB_ENV *dbenv;
	u_int32_t *lg_maxp;
{
	LOG *region;

	if (F_ISSET(dbenv, DB_ENV_OPEN_CALLED)) {
		if (!LOGGING_ON(dbenv))
			return (__db_env_config(
			    dbenv, "get_lg_max", DB_INIT_LOG));
		region = ((DB_LOG *)dbenv->lg_handle)->reginfo.primary;

		*lg_maxp = region->log_nsize;
	} else
		*lg_maxp = dbenv->lg_size;

	return (0);
}

/*
 * __log_set_lg_max --
 *	DB_ENV->set_lg_max.
 *
 * PUBLIC: int __log_set_lg_max __P((DB_ENV *, u_int32_t));
 */
int
__log_set_lg_max(dbenv, lg_max)
	DB_ENV *dbenv;
	u_int32_t lg_max;
{
	LOG *region;

	if (lg_max == 0)
		lg_max = LG_MAX_DEFAULT;

	if (F_ISSET(dbenv, DB_ENV_OPEN_CALLED)) {
		if (!LOGGING_ON(dbenv))
			return (__db_env_config(
			    dbenv, "set_lg_max", DB_INIT_LOG));
		region = ((DB_LOG *)dbenv->lg_handle)->reginfo.primary;

					/* Let's not be silly. */
		if (lg_max < region->buffer_size * 4)
			goto err;
		region->log_nsize = lg_max;
	} else {
					/* Let's not be silly. */
		if (lg_max < dbenv->lg_bsize * 4)
			goto err;
		dbenv->lg_size = lg_max;
	}

	return (0);

err:	__db_err(dbenv, "log file size must be >= log buffer size * 4");
	return (EINVAL);
}

static int
__log_get_lg_regionmax(dbenv, lg_regionmaxp)
	DB_ENV *dbenv;
	u_int32_t *lg_regionmaxp;
{
	*lg_regionmaxp = dbenv->lg_regionmax;
	return (0);
}

/*
 * __log_set_lg_regionmax --
 *	DB_ENV->set_lg_regionmax.
 *
 * PUBLIC: int __log_set_lg_regionmax __P((DB_ENV *, u_int32_t));
 */
int
__log_set_lg_regionmax(dbenv, lg_regionmax)
	DB_ENV *dbenv;
	u_int32_t lg_regionmax;
{
	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_lg_regionmax");

					/* Let's not be silly. */
	if (lg_regionmax != 0 && lg_regionmax < LG_BASE_REGION_SIZE) {
		__db_err(dbenv,
		    "log file size must be >= %d", LG_BASE_REGION_SIZE);
		return (EINVAL);
	}

	dbenv->lg_regionmax = lg_regionmax;
	return (0);
}

static int
__log_get_lg_dir(dbenv, dirp)
	DB_ENV *dbenv;
	const char **dirp;
{
	*dirp = dbenv->db_log_dir;
	return (0);
}

/*
 * __log_set_lg_dir --
 *	DB_ENV->set_lg_dir.
 *
 * PUBLIC: int __log_set_lg_dir __P((DB_ENV *, const char *));
 */
int
__log_set_lg_dir(dbenv, dir)
	DB_ENV *dbenv;
	const char *dir;
{
	if (dbenv->db_log_dir != NULL)
		__os_free(dbenv, dbenv->db_log_dir);
	return (__os_strdup(dbenv, dir, &dbenv->db_log_dir));
}
