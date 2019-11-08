/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: lock_region.c,v 11.73 2003/07/23 13:13:12 mjc Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#endif


#include "db_int.h"
#include "dbinc/db_shash.h"
#include "dbinc/lock.h"

#include <logmsg.h>
#include "locks_wrap.h"

static int  __lock_init __P((DB_ENV *, DB_LOCKTAB *));
static size_t
	    __lock_region_size __P((DB_ENV *));

#ifdef HAVE_MUTEX_SYSTEM_RESOURCES
static size_t __lock_region_maint __P((DB_ENV *));
#endif

/*
 * The conflict arrays are set up such that the row is the lock you are
 * holding and the column is the lock that is desired.
 */
#define	DB_LOCK_RIW_N	11
static const u_int8_t db_riw_conflicts[] = {
/*         N   R   W   WT  IW  IR  RIW DR  WW  WA  WD*/
/*   N */  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
/*   R */  0,  0,  1,  0,  1,  0,  1,  0,  1,  1,  1, 
/*   W */  0,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
/*  WT */  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
/*  IW */  0,  1,  1,  0,  0,  0,  0,  1,  1,  1,  1, 
/*  IR */  0,  0,  1,  0,  0,  0,  0,  0,  1,  1,  1,
/* RIW */  0,  1,  1,  0,  0,  0,  0,  1,  1,  1,  1,
/*  DR */  0,  0,  1,  0,  1,  0,  1,  0,  0,  1,  1,
/*  WW */  0,  1,  1,  0,  1,  1,  1,  0,  1,  1,  1,
/*  WA */  0,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
/*  WD */  0,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1
};

/*
 * This conflict array is used for concurrent db access (CDB).  It uses
 * the same locks as the db_riw_conflicts array, but adds an IW mode to
 * be used for write cursors.
 */
#define	DB_LOCK_CDB_N	5
static const u_int8_t db_cdb_conflicts[] = {
	/*          N  R  W  WT IW  */
	/*   N */   0, 0, 0, 0, 0,
	/*   R */   0, 0, 1, 0, 0,
	/*   W */   0, 1, 1, 1, 1,
	/*  WT */   0, 0, 0, 0, 0,
	/*  IW */   0, 0, 1, 0, 1
};

/*
 * __lock_open --
 *	Internal version of lock_open: only called from DB_ENV->open.
 *
 * PUBLIC: int __lock_open __P((DB_ENV *));
 */
int
__lock_open(dbenv)
	DB_ENV *dbenv;
{
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	size_t size;
	int ret;

	/* Create the lock table structure. */
	if ((ret = __os_calloc(dbenv, 1, sizeof(DB_LOCKTAB), &lt)) != 0)
		return (ret);
	lt->dbenv = dbenv;

	/* Join/create the lock region. */
	lt->reginfo.type = REGION_TYPE_LOCK;
	lt->reginfo.id = INVALID_REGION_ID;
	lt->reginfo.mode = dbenv->db_mode;
	lt->reginfo.flags = REGION_JOIN_OK;
	if (F_ISSET(dbenv, DB_ENV_CREATE))
		F_SET(&lt->reginfo, REGION_CREATE_OK);
	size = __lock_region_size(dbenv);
	if ((ret = __db_r_attach(dbenv, &lt->reginfo, size)) != 0)
		goto err;

	/* If we created the region, initialize it. */
	if (F_ISSET(&lt->reginfo, REGION_CREATE))
		if ((ret = __lock_init(dbenv, lt)) != 0)
			goto err;

	/* Set the local addresses. */
	region = lt->reginfo.primary =
	    R_ADDR(&lt->reginfo, lt->reginfo.rp->primary);

	/* Check for incompatible automatic deadlock detection requests. */
	if (dbenv->lk_detect != DB_LOCK_NORUN) {
		if (region->detect != DB_LOCK_NORUN &&
		    dbenv->lk_detect != DB_LOCK_DEFAULT &&
		    region->detect != dbenv->lk_detect) {
			__db_err(dbenv,
		    "lock_open: incompatible deadlock detector mode");
			ret = EINVAL;
			goto err;
		}

		/*
		 * Upgrade if our caller wants automatic detection, and it
		 * was not currently being done, whether or not we created
		 * the region.
		 */
		if (region->detect == DB_LOCK_NORUN)
			region->detect = dbenv->lk_detect;
	}

	/*
	 * A process joining the region may have reset the lock and transaction
	 * timeouts.
	 */
	 if (dbenv->lk_timeout != 0)
		region->lk_timeout = dbenv->lk_timeout;
	 if (dbenv->tx_timeout != 0)
		region->tx_timeout = dbenv->tx_timeout;

	/* Set remaining pointers into region. */
	lt->conflicts = (u_int8_t *)R_ADDR(&lt->reginfo, region->conf_off);

	R_UNLOCK(dbenv, &lt->reginfo);

	dbenv->lk_handle = lt;
	return (0);

err:	if (lt->reginfo.addr != NULL) {
		if (F_ISSET(&lt->reginfo, REGION_CREATE))
			ret = __db_panic(dbenv, ret);
		R_UNLOCK(dbenv, &lt->reginfo);
		(void)__db_r_detach(dbenv, &lt->reginfo, 0);
	}
	__os_free(dbenv, lt);
	return (ret);
}

int
add_to_lock_partition(DB_ENV *dbenv, DB_LOCKTAB *lt,
    int partition, int num, struct __db_lock lp[])
{
	DB_LOCKREGION *region = lt->reginfo.primary;
	int i, ret;

	for (i = 0; i < num; ++i) {
		lp[i].status = DB_LSTAT_FREE;
		lp[i].lpartition = partition;
		lp[i].gen = 0;
		if ((ret = __db_mutex_setup(dbenv, &lt->reginfo, &lp[i].mutex,
		    MUTEX_LOGICAL_LOCK | MUTEX_NO_RLOCK |
		    MUTEX_SELF_BLOCK)) != 0)
			return (ret);

		Pthread_mutex_init(&lp[i].lsns_mtx, NULL);
		SH_LIST_INIT(&lp[i].lsns);
		lp[i].nlsns = 0;
		MUTEX_LOCK(dbenv, &lp[i].mutex);
		SH_TAILQ_INSERT_HEAD(&region->free_locks[partition], &lp[i],
		    links, __db_lock);
	}
	return (0);
}

int init_latches(DB_ENV *, DB_LOCKTAB *);

/*
 * __lock_init --
 *	Initialize the lock region.
 */
static int
__lock_init(dbenv, lt)
	DB_ENV *dbenv;
	DB_LOCKTAB *lt;
{
	const u_int8_t *lk_conflicts;
	struct __db_lock *lp;
	DB_LOCKER *lidp;
	DB_LOCKOBJ *op;
	DB_LOCKREGION *region;
#ifdef HAVE_MUTEX_SYSTEM_RESOURCES
	size_t maint_size;
#endif
	u_int32_t i, j, lk_modes;
	u_int8_t *addr;
	int ret;

	if ((ret = __db_shalloc(lt->reginfo.addr,
	    sizeof(DB_LOCKREGION), 0, &lt->reginfo.primary)) != 0)
		goto mem_err;
	lt->reginfo.rp->primary = R_OFFSET(&lt->reginfo, lt->reginfo.primary);
	region = lt->reginfo.primary;
	memset(region, 0, sizeof(*region));

	/* Select a conflict matrix if none specified. */
	if (dbenv->lk_modes == 0)
		if (CDB_LOCKING(dbenv)) {
			lk_modes = DB_LOCK_CDB_N;
			lk_conflicts = db_cdb_conflicts;
		} else {
			lk_modes = DB_LOCK_RIW_N;
			lk_conflicts = db_riw_conflicts;
		}
	else {
		lk_modes = dbenv->lk_modes;
		lk_conflicts = dbenv->lk_conflicts;
	}

	region->need_dd = 0;

	LOCK_SET_TIME_INVALID(&region->next_timeout);
	region->detect = DB_LOCK_NORUN;
	region->lk_timeout = dbenv->lk_timeout;
	region->tx_timeout = dbenv->tx_timeout;

	unsigned lock_p_size = dbenv->lk_max / gbl_lk_parts;
	unsigned object_p_size = dbenv->lk_max_objects / gbl_lk_parts;
	unsigned locker_p_size = dbenv->lk_max_lockers / gbl_lkr_parts;

	region->object_p_size = object_p_size > gbl_lk_hash ? object_p_size
	    : gbl_lk_hash;
	region->locker_p_size = locker_p_size > gbl_lkr_hash ? locker_p_size
	    : gbl_lkr_hash;
	logmsg(LOGMSG_DEBUG, "Lock & Lock-object partitions:%zu\n"
	    "Locker partitions:%zu\n"
	    "Initial Lockers:%u  Initial Locks:%u  Initial Lock-objects:%u\n"
	    "Lockers/partition:%u  Locks/partition:%u  Lock-objects/partition:%u\n"
	    "Locker hash buckets/partition:%u  Lock-object hash buckets/partition:%u\n",
	    gbl_lk_parts, gbl_lkr_parts, dbenv->lk_max_lockers, dbenv->lk_max,
	    dbenv->lk_max_objects, locker_p_size, lock_p_size, object_p_size,
	    region->locker_p_size, region->object_p_size);

	memset(&region->stat, 0, sizeof(region->stat));
	region->stat.st_id = 0;
	region->stat.st_cur_maxid = DB_LOCK_MAXID;
	region->stat.st_maxlocks = dbenv->lk_max;
	region->stat.st_maxlockers = dbenv->lk_max_lockers;
	region->stat.st_maxobjects = dbenv->lk_max_objects;
	region->stat.st_nmodes = lk_modes;

	/* Allocate room for the conflict matrix and initialize it. */
	if ((ret =
	    __db_shalloc(lt->reginfo.addr, lk_modes * lk_modes, 0,
	    &addr)) != 0) {
		goto mem_err;
	}
	memcpy(addr, lk_conflicts, lk_modes * lk_modes);
	region->conf_off = R_OFFSET(&lt->reginfo, addr);

	/*
	 * Initialize locks onto a free list. Initialize and lock the mutex
	 * so that when we need to block, all we need do is try to acquire
	 * the mutex.
	 */
	if (sizeof(struct __db_lock) < MUTEX_ALIGN) {
		logmsg(LOGMSG_FATAL, "%s:%d %s() EBADASS: bad assumption error\n",
		    __FILE__, __LINE__, __func__);
		abort();
	}

	if ((ret = __db_shalloc(lt->reginfo.addr,
	    sizeof(region->free_lockers[0]) * gbl_lkr_parts, 0,
	    &region->free_lockers)) != 0) {
		goto mem_err;
	}
	if ((ret = __db_shalloc(lt->reginfo.addr,
	    sizeof(region->locker_tab[0]) * gbl_lkr_parts, 0,
	    &region->locker_tab)) != 0) {
		goto mem_err;
	}
	if ((ret = __db_shalloc(lt->reginfo.addr,
	    sizeof(region->locker_tab_mtx[0]) * gbl_lkr_parts, 0,
	    &region->locker_tab_mtx)) != 0) {
		goto mem_err;
	}
	if ((ret = __db_shalloc(lt->reginfo.addr,
	    sizeof(region->free_objs[0]) * gbl_lk_parts, 0,
	    &region->free_objs)) != 0) {
		goto mem_err;
	}
	if ((ret = __db_shalloc(lt->reginfo.addr,
	    sizeof(region->free_locks[0]) * gbl_lk_parts, 0,
	    &region->free_locks)) != 0) {
		goto mem_err;
	}
	if ((ret = __db_shalloc(lt->reginfo.addr,
	    sizeof(region->obj_tab[0]) * gbl_lk_parts, 0,
	    &region->obj_tab) != 0)) {
		goto mem_err;
	}
	if ((ret = __db_shalloc(lt->reginfo.addr,
	    sizeof(region->obj_tab_mtx[0]) * gbl_lk_parts, 0,
	    &region->obj_tab_mtx)) != 0) {
		goto mem_err;
	}
	if ((ret = __db_shalloc(lt->reginfo.addr,
	    sizeof(region->nwlk_scale[0]) * gbl_lk_parts, 0,
	    &region->nwlk_scale)) != 0) {
		goto mem_err;
	}
	if ((ret = __db_shalloc(lt->reginfo.addr,
	    sizeof(region->nwobj_scale[0]) * gbl_lk_parts, 0,
	    &region->nwobj_scale)) != 0) {
		goto mem_err;
	}
	if ((ret = __db_shalloc(lt->reginfo.addr,
	    sizeof(region->nwlkr_scale[0]) * gbl_lkr_parts, 0,
	    &region->nwlkr_scale)) != 0) {
		goto mem_err;
	}

	for (i = 0; i < gbl_lk_parts; ++i) {
		region->nwlk_scale[i] = 0;
		region->nwobj_scale[i] = 0;
		/* Allocate room for the object hash table and initialize it. */
		Pthread_mutex_init(&region->obj_tab_mtx[i].mtx, NULL);
		bzero(region->obj_tab_mtx[i].fluff,
		    sizeof(region->obj_tab_mtx[i].fluff));
		if ((ret = __db_shalloc(lt->reginfo.addr,
		    region->object_p_size * sizeof(ObjTab), 0, &addr)) != 0) {
			goto mem_err;
		}
		ObjTab *tab = (ObjTab *)addr;
		__db_hashinit(tab, region->object_p_size);
		region->obj_tab[i] = tab;

		/* Initialize objects onto a free list.  */
		SH_TAILQ_INIT(&region->free_objs[i]);
		if ((ret = __db_shalloc(lt->reginfo.addr,
		    sizeof(DB_LOCKOBJ) * object_p_size, 0, &op)) != 0)
			goto mem_err;
		for (j = 0; j < object_p_size; ++j, ++op)
			SH_TAILQ_INSERT_HEAD(
			    &region->free_objs[i], op, links, __db_lockobj);

		/* Initialize locks onto a free list.  */
		SH_TAILQ_INIT(&region->free_locks[i]);
		if ((ret = __db_shalloc(lt->reginfo.addr,
		    sizeof(struct __db_lock) * lock_p_size, 0, &lp)) != 0)
			goto mem_err;
		ret = add_to_lock_partition(dbenv, lt, i, lock_p_size, lp);
		if (ret != 0)
			return ret;
	}

	for (i = 0; i < gbl_lkr_parts; ++i) {
		region->nwlkr_scale[i] = 0;
		/* Allocate room for the locker hash table and initialize it. */
		Pthread_mutex_init(&region->locker_tab_mtx[i].mtx, NULL);
		bzero(region->locker_tab_mtx[i].fluff,
		    sizeof(region->locker_tab_mtx[i].fluff));
		if ((ret = __db_shalloc(lt->reginfo.addr,
		    region->locker_p_size * sizeof(LockerTab),
		    0, &addr)) != 0) {
			goto mem_err;
		}
		LockerTab *tab = (LockerTab *)addr;
		__db_hashinit(tab, region->locker_p_size);
		region->locker_tab[i] = tab;

		/* Initialize lockers onto a free list.  */
		SH_TAILQ_INIT(&region->free_lockers[i]);
		if ((ret = __db_shalloc(lt->reginfo.addr,
		    sizeof(DB_LOCKER) * locker_p_size, 0, &lidp)) != 0) {
mem_err:		__db_err(dbenv,
			    "Unable to allocate memory for the lock table");
			return (ret);
		}
		for (j = 0; j < locker_p_size; ++j, ++lidp)
			SH_TAILQ_INSERT_HEAD(
			    &region->free_lockers[i], lidp, links, __db_locker);
	}

#ifdef	HAVE_MUTEX_SYSTEM_RESOURCES
	maint_size = __lock_region_maint(dbenv);
	/* Allocate room for the locker maintenance info and initialize it. */
	if ((ret = __db_shalloc(lt->reginfo.addr,
	    sizeof(REGMAINT) + maint_size, 0, &addr)) != 0)
		goto mem_err;
	__db_maintinit(&lt->reginfo, addr, maint_size);
	region->maint_off = R_OFFSET(&lt->reginfo, addr);
#endif

	SH_TAILQ_INIT(&region->dd_objs);
	SH_TAILQ_INIT(&region->lockers);

	Pthread_mutex_init(&region->dd_mtx.mtx, NULL);
	bzero(region->dd_mtx.fluff, sizeof(region->dd_mtx.fluff));

	Pthread_mutex_init(&region->lockers_mtx.mtx, NULL);
	bzero(region->lockers_mtx.fluff, sizeof(region->lockers_mtx.fluff));

	Pthread_mutex_init(&region->db_lock_lsn_lk, NULL);
	SH_LIST_INIT(&region->db_lock_lsn_head);
	region->db_lock_lsn_step = dbenv->attr.db_lock_lsn_step;

	init_latches(dbenv, lt);

	return (0);
}

/*
 * __lock_dbenv_refresh --
 *	Clean up after the lock system on a close or failed open.  Called
 * only from __dbenv_refresh.  (Formerly called __lock_close.)
 *
 * PUBLIC: int __lock_dbenv_refresh __P((DB_ENV *));
 */
int
__lock_dbenv_refresh(dbenv)
	DB_ENV *dbenv;
{
	DB_LOCKTAB *lt;
	int ret;

	lt = dbenv->lk_handle;

	/* Detach from the region. */
	ret = __db_r_detach(dbenv, &lt->reginfo, 0);

	__os_free(dbenv, lt);

	dbenv->lk_handle = NULL;
	return (ret);
}

/*
 * __lock_region_size --
 *	Return the region size.
 */
static size_t
__lock_region_size(dbenv)
	DB_ENV *dbenv;
{
	size_t retval;

	/*
	 * Figure out how much space we're going to need.  This list should
	 * map one-to-one with the __db_shalloc calls in __lock_init.
	 */
	retval = 0;
	retval += __db_shalloc_size(sizeof(DB_LOCKREGION), 1);
	retval += __db_shalloc_size(dbenv->lk_modes * dbenv->lk_modes, 1);
#ifdef HAVE_MUTEX_SYSTEM_RESOURCES
	retval +=
	    __db_shalloc_size(sizeof(REGMAINT) + __lock_region_maint(dbenv), 1);
#endif

	unsigned lock_p_size = dbenv->lk_max / gbl_lk_parts;
	unsigned lk_hash =
	    gbl_lk_hash > lock_p_size ? gbl_lk_hash : lock_p_size;

	unsigned locker_p_size = dbenv->lk_max_lockers / gbl_lkr_parts;
	unsigned lkr_hash =
	    gbl_lkr_hash > locker_p_size ? gbl_lkr_hash : locker_p_size;

	retval += __db_shalloc_size(lk_hash * gbl_lk_parts * sizeof(ObjTab), 1);
	retval +=
	    __db_shalloc_size(lkr_hash * gbl_lkr_parts * sizeof(LockerTab), 1);

	retval +=
	    __db_shalloc_size(dbenv->lk_max * sizeof(struct __db_lock), 1);
	retval +=
	    __db_shalloc_size(dbenv->lk_max_objects * sizeof(DB_LOCKOBJ), 1);
	retval +=
	    __db_shalloc_size(dbenv->lk_max_lockers * sizeof(DB_LOCKER), 1);

	/*
	 * Include 16 bytes of string space per lock.  DB doesn't use it
	 * because we pre-allocate lock space for DBTs in the structure.
	 */
	//retval += __db_shalloc_size(dbenv->lk_max * 16, sizeof(size_t));

	DB_LOCKREGION *region;

	retval += sizeof(region->nwlk_scale[0] * gbl_lk_parts);
	retval += sizeof(region->nwobj_scale[0] * gbl_lk_parts);
	retval += sizeof(region->nwlkr_scale[0] * gbl_lkr_parts);

	retval += sizeof(region->free_objs[0]) * gbl_lk_parts;
	retval += sizeof(region->free_locks[0]) * gbl_lk_parts;
	retval += sizeof(region->free_lockers[0]) * gbl_lkr_parts;

	retval += sizeof(region->locker_tab[0]) * gbl_lkr_parts;
	retval += sizeof(region->locker_tab_mtx[0]) * gbl_lkr_parts;

	retval += sizeof(region->obj_tab[0]) * gbl_lk_parts;
	retval += sizeof(region->obj_tab_mtx[0]) * gbl_lk_parts;

	/* And we keep getting this wrong, let's be generous. */
	retval += retval / 5;

	return (retval);
}

#ifdef HAVE_MUTEX_SYSTEM_RESOURCES
/*
 * __lock_region_maint --
 *	Return the amount of space needed for region maintenance info.
 */
static size_t
__lock_region_maint(dbenv)
	DB_ENV *dbenv;
{
	size_t s;

	s = sizeof(DB_MUTEX *) * dbenv->lk_max;
	return (s);
}
#endif

/*
 * __lock_region_destroy
 *	Destroy any region maintenance info.
 *
 * PUBLIC: void __lock_region_destroy __P((DB_ENV *, REGINFO *));
 */
void
__lock_region_destroy(dbenv, infop)
	DB_ENV *dbenv;
	REGINFO *infop;
{
	__db_shlocks_destroy(infop, (REGMAINT *)R_ADDR(infop,
	    ((DB_LOCKREGION *)R_ADDR(infop, infop->rp->primary))->maint_off));

	COMPQUIET(dbenv, NULL);
	COMPQUIET(infop, NULL);
}

/*
 * __lock_id_set --
 *	Set the current locker ID and current maximum unused ID (for
 *	testing purposes only).
 *
 * PUBLIC: int __lock_id_set __P((DB_ENV *, u_int32_t, u_int32_t));
 */
int
__lock_id_set(dbenv, cur_id, max_id)
	DB_ENV *dbenv;
	u_int32_t cur_id, max_id;
{
	DB_LOCKTAB *lt;
	DB_LOCKREGION *region;

	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "lock_id_set", DB_INIT_LOCK);

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;
	region->stat.st_id = cur_id;
	region->stat.st_cur_maxid = max_id;

	return (0);
}
