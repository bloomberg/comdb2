/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: mp_trickle.c,v 11.30 2003/09/13 19:20:41 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <stdlib.h>
#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_shash.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"

#include <time.h>

static int __memp_trickle __P((DB_ENV *, int, int *, int));

/*
 * __memp_trickle_pp --
 *	DB_ENV->memp_trickle pre/post processing.
 *
 * PUBLIC: int __memp_trickle_pp __P((DB_ENV *, int, int *, int));
 */
int
__memp_trickle_pp(dbenv, pct, nwrotep, lru)
	DB_ENV *dbenv;
	int pct, *nwrotep, lru;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->mp_handle, "memp_trickle", DB_INIT_MPOOL);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __memp_trickle(dbenv, pct, nwrotep, lru);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __memp_trickle --
 *	DB_ENV->memp_trickle.
 */
static int
__memp_trickle(dbenv, pct, nwrotep, lru)
	DB_ENV *dbenv;
	int pct, *nwrotep, lru;
{
	DB_MPOOL *dbmp;
	MPOOL *c_mp, *mp;
	DB_LSN last_lsn;
	u_int32_t dirty, i, total, dtmp;
	int n, ret, wrote;

	dbmp = dbenv->mp_handle;
	mp = dbmp->reginfo[0].primary;

	if (nwrotep != NULL)
		*nwrotep = 0;

	if (pct < 1 || pct > 100)
		return (EINVAL);

	__log_get_last_lsn(dbenv, &last_lsn);
	if (log_compare(&last_lsn, &mp->trickle_lsn) <= 0)
		return (0);

	/*
	 * If there are sufficient clean buffers, no buffers or no dirty
	 * buffers, we're done.
	 *
	 * XXX
	 * Using hash_page_dirty is our only choice at the moment, but it's not
	 * as correct as we might like in the presence of pools having more
	 * than one page size, as a free 512B buffer isn't the same as a free
	 * 8KB buffer.
	 *
	 * Loop through the caches counting total/dirty buffers.
	 */
	for (ret = 0, i = dirty = total = 0; i < mp->nreg; ++i) {
		c_mp = dbmp->reginfo[i].primary;
		total += c_mp->stat.st_pages;
		__memp_stat_hash(&dbmp->reginfo[i], c_mp, &dtmp);
		dirty += dtmp;
	}

	/*
	 * !!!
	 * Be careful in modifying this calculation, total may be 0.
	 */
	n = ((total * pct) / 100) - (total - dirty);
	if (dirty == 0 || n <= 0)
		goto done;

	if (nwrotep == NULL)
		nwrotep = &wrote;
	if (dbenv->iomap && dbenv->attr.iomap_enabled)
		dbenv->iomap->memptrickle_active = time(NULL);
	/* With perfect checkpoints it is unlikely to ensure the percentage
	   of clean pages. So here we write all modified pages to disk. */
	ret = __memp_sync_int(dbenv, NULL, n,
	    lru ? DB_SYNC_LRU : DB_SYNC_TRICKLE, nwrotep, 1, NULL, 0);
	if (dbenv->iomap && dbenv->attr.iomap_enabled)
		dbenv->iomap->memptrickle_active = 0;

	mp->stat.st_page_trickle += *nwrotep;

done:	memcpy(&mp->trickle_lsn, &last_lsn, sizeof(DB_LSN));

	return (ret);
}
