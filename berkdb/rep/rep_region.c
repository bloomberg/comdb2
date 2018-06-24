/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2001-2003
 *	Sleepycat Software.  All rights reserved.
 */
#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: rep_region.c,v 1.42 2003/09/04 18:06:49 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#if TIME_WITH_SYS_TIME
#include <sys/time.h>
#include <time.h>
#else
#if HAVE_SYS_TIME_H
#include <sys/time.h>
#else
#include <time.h>
#endif
#endif

#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/log.h"
#include "dbinc/db_am.h"

void init_log_repdb_queue(void);

/*
 * __rep_region_init --
 *	Initialize the shared memory state for the replication system.
 *
 * PUBLIC: int __rep_region_init __P((DB_ENV *));
 */
int
__rep_region_init(dbenv)
	DB_ENV *dbenv;
{
	REGENV *renv;
	REGINFO *infop;
	DB_MUTEX *db_mutexp;
	DB_REP *db_rep;
	REP *rep;
	int ret;

	db_rep = dbenv->rep_handle;
	infop = dbenv->reginfo;
	renv = infop->primary;
	ret = 0;

	MUTEX_LOCK(dbenv, &renv->mutex);
	if (renv->rep_off == INVALID_ROFF) {
		/* Must create the region. */
		if ((ret = __db_shalloc(infop->addr,
		    sizeof(REP), MUTEX_ALIGN, &rep)) != 0)
			goto err;
		memset(rep, 0, sizeof(*rep));
		rep->tally_off = INVALID_ROFF;
		rep->v2tally_off = INVALID_ROFF;
		renv->rep_off = R_OFFSET(infop, rep);

		if ((ret = __db_mutex_setup(dbenv, infop, &rep->mutex,
		    MUTEX_NO_RECORD)) != 0)
			goto err;

		/*
		 * We must create a place for the db_mutex separately;
		 * mutexes have to be aligned to MUTEX_ALIGN, and the only way
		 * to guarantee that is to make sure they're at the beginning
		 * of a shalloc'ed chunk.
		 */
		if ((ret = __db_shalloc(infop->addr, sizeof(DB_MUTEX),
		    MUTEX_ALIGN, &db_mutexp)) != 0)
			goto err;
		rep->db_mutex_off = R_OFFSET(infop, db_mutexp);

		/*
		 * Because we have no way to prevent deadlocks and cannot log
		 * changes made to it, we single-thread access to the client
		 * bookkeeping database.  This is suboptimal, but it only gets
		 * accessed when messages arrive out-of-order, so it should
		 * stay small and not be used in a high-performance app.
		 */
		if ((ret = __db_mutex_setup(dbenv, infop, db_mutexp,
		    MUTEX_NO_RECORD)) != 0)
			goto err;

		/* We have the region; fill in the values. */
		rep->eid = db_eid_invalid;
		rep->master_id = db_eid_invalid;
		rep->gen = 0;
        rep->committed_gen = 0;
        memset(&rep->committed_lsn, 0, sizeof(rep->committed_lsn));
		rep->egen = rep->gen + 1;

		/*
		 * Set default values for the min and max log records that we
		 * wait before requesting a missing log record.
		 */
		rep->request_gap = DB_REP_REQUEST_GAP;
		rep->max_gap = DB_REP_MAX_GAP;
		F_SET(rep, REP_F_NOARCHIVE);
        (void)time(&rep->timestamp);
	} else
		rep = R_ADDR(infop, renv->rep_off);
	MUTEX_UNLOCK(dbenv, &renv->mutex);

    init_log_repdb_queue();
	db_rep->rep_mutexp = &rep->mutex;
	db_rep->db_mutexp = R_ADDR(infop, rep->db_mutex_off);
	db_rep->region = rep;
	db_rep->repdbname = NULL;

	db_rep->repdbcnt = 0;

	return (0);

err:	MUTEX_UNLOCK(dbenv, &renv->mutex);
	return (ret);
}

/*
 * __rep_region_destroy --
 *	Destroy any system resources allocated in the replication region.
 *
 * PUBLIC: int __rep_region_destroy __P((DB_ENV *));
 */
int
__rep_region_destroy(dbenv)
	DB_ENV *dbenv;
{
	DB_REP *db_rep;
	int ret, t_ret;

	ret = t_ret = 0;
	db_rep = dbenv->rep_handle;

	if (db_rep != NULL) {
		if (db_rep->rep_mutexp != NULL)
			ret = __db_mutex_destroy(db_rep->rep_mutexp);
		if (db_rep->db_mutexp != NULL)
			t_ret = __db_mutex_destroy(db_rep->db_mutexp);
	}

	return (ret == 0 ? t_ret : ret);
}

/*
 * __rep_dbenv_refresh --
 *	Replication-specific refresh of the DB_ENV structure.
 *
 * PUBLIC: void __rep_dbenv_refresh __P((DB_ENV *));
 */
void
__rep_dbenv_refresh(dbenv)
	DB_ENV *dbenv;
{
	if (REP_ON(dbenv))
		((DB_REP *)dbenv->rep_handle)->region = NULL;
	return;
}

/*
 * __rep_dbenv_close --
 *	Replication-specific destruction of the DB_ENV structure.
 *
 * PUBLIC: int __rep_dbenv_close __P((DB_ENV *));
 */
int
__rep_dbenv_close(dbenv)
	DB_ENV *dbenv;
{
	if (REP_ON(dbenv)) {
		__os_free(dbenv, dbenv->rep_handle);
		dbenv->rep_handle = NULL;
		dbenv->rep_send = NULL;
	}

	return (0);
}

/*
 * __rep_preclose --
 *	If we are a client, shut down our client database and, if we're
 * actually closing the environment, close all databases we've opened
 * while applying messages.
 *
 * PUBLIC: int __rep_preclose __P((DB_ENV *, int));
 */
int
__rep_preclose(dbenv, do_closefiles)
	DB_ENV *dbenv;
	int do_closefiles;
{
	DB *dbp;
	DB_REP *db_rep;
	int ret, t_ret;

	ret = t_ret = 0;

	/* If replication is not initialized, we have nothing to do. */
	if (!REP_ON(dbenv))
		return (0);

	db_rep = dbenv->rep_handle;
	if ((dbp = db_rep->rep_db) != NULL) {
		MUTEX_LOCK(dbenv, db_rep->db_mutexp);
		ret = __db_close(dbp, NULL, DB_NOSYNC);
		db_rep->rep_db = NULL;
		MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);
	}

	if (do_closefiles)
		t_ret = __dbreg_close_files(dbenv);

	return (ret == 0 ? t_ret : ret);
}
