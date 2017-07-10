/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: lock_deadlock.c,v 11.66 2003/11/19 19:59:02 ubell Exp $";
#endif /* not lint */

#include <limits.h>

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#include <stdlib.h>
#include <pthread.h>

#include <string.h>
#include <assert.h>
#endif

#include "db_int.h"
#include "dbinc/db_shash.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/txn.h"
#include <alloca.h>

#include "debug_switches.h"
#include "logmsg.h"

extern int verbose_deadlocks;
extern int gbl_sparse_lockerid_map;
extern int gbl_rowlocks;

void stack_me(char *location);

#define	ISSET_MAP(M, N)	((M)[(N) / 32] & (1 << (N) % 32))

#define	CLEAR_MAP(M, N) {						\
	u_int32_t __i;							\
	for (__i = 0; __i < (N); __i++)					\
		(M)[__i] = 0;						\
}

#define	SET_MAP(M, B)	((M)[(B) / 32] |= (1 << ((B) % 32)))
#define	CLR_MAP(M, B)	((M)[(B) / 32] &= ~(1 << ((B) % 32)))

#define	OR_MAP(D, S, N)	{						\
	u_int32_t __i;							\
	for (__i = 0; __i < (N); __i++)					\
		D[__i] |= S[__i];					\
}
#define	BAD_KILLID	0xffffffff

typedef struct {
	DB_LOCKOBJ *last_obj;
	pthread_t tid;
	roff_t last_lock;
	u_int32_t count;
	u_int32_t id;
	u_int32_t last_locker_id;
	db_pgno_t pgno;
	int killme;
	int saveme;
	int readonly;
	u_int8_t self_wait;
	u_int8_t valid;
	u_int8_t in_abort;
	u_int8_t tracked;
} locker_info;

typedef struct {
	int *alloclist;
	int alloccnt;
	u_int32_t nentries;
	u_int32_t **map;
} sparse_map_t;

static inline void free_sparse_map __P((DB_ENV *, sparse_map_t *));
static inline int copy_sparse_map __P((DB_ENV *, sparse_map_t *,
	sparse_map_t **));
static void (*berkdb_deadlock_callback) (struct berkdb_deadlock_info *) = NULL;

static int __dd_abort __P((DB_ENV *, locker_info *));
static int __dd_build __P((DB_ENV *,
	u_int32_t, u_int32_t **, sparse_map_t **, u_int32_t *, u_int32_t *,
	locker_info **, int));
static int __dd_find __P((DB_ENV *, u_int32_t *, sparse_map_t *, locker_info *,
	u_int32_t, u_int32_t, u_int32_t ***, u_int32_t **, int *));
static int __dd_isolder __P((u_int32_t, u_int32_t, u_int32_t, u_int32_t));
static int __dd_verify __P((locker_info *, u_int32_t *, u_int32_t *,
	u_int32_t *, sparse_map_t *, u_int32_t, u_int32_t, u_int32_t));

#ifdef DIAGNOSTIC
static void __dd_debug __P((DB_ENV *, locker_info *, u_int32_t *,
	sparse_map_t *, u_int32_t, u_int32_t));
#endif

/*
 * sparse_map_cmp --
 *  Compare function for sparse-map qsort.
 */
static int
sparse_map_cmp(a, b)
	const void *a, *b;
{
	const int *s1, *s2;

	s1 = a;
	s2 = b;

	if (*s1 < *s2)
		return -1;
	if (*s1 > *s2)
		return 1;
	return 0;
}

/*
 * free_sparse_map --
 *  Frees all memory associated with a sparse_map.
 */
static inline void
clear_sparse_map(dbenv, sparse_map)
	DB_ENV *dbenv;
	sparse_map_t *sparse_map;
{
	int i, idx;
	u_int32_t *map;

	if (sparse_map && sparse_map->alloclist) {
		for (i = 0; i < sparse_map->alloccnt; i++) {
			idx = sparse_map->alloclist[i];
			map = sparse_map->map[idx];
			assert(map != NULL);
			__os_free (dbenv, map);
			sparse_map->map[idx] = NULL;
		}
		sparse_map->alloccnt = 0;
	}
}

static inline void
free_sparse_map(dbenv, sparse_map)
	DB_ENV *dbenv;
	sparse_map_t *sparse_map;
{
	clear_sparse_map(dbenv, sparse_map);

	if (sparse_map && sparse_map->alloclist)
		__os_free (dbenv, sparse_map->alloclist);

	if (sparse_map && sparse_map->map)
		__os_free (dbenv, sparse_map->map);

	if (sparse_map)
		__os_free (dbenv, sparse_map);
}

/* 
 * allocate_sparse_map --
 *  Allocates memory for a sparse-map.
 */
static inline int
allocate_sparse_map(dbenv, nentries, sparse_map)
	DB_ENV *dbenv;
	u_int32_t nentries;
	sparse_map_t **sparse_map;

{
	int ret;
	sparse_map_t *map;

	if ((ret = __os_calloc(dbenv, 1, sizeof(sparse_map_t), &map))!=0) {
		return (ret);
	}

	if ((ret = __os_calloc(dbenv, sizeof(u_int32_t *),
		    nentries * 32, &map->map))!=0) {
		free_sparse_map(dbenv, map);

		return (ret);
	}

	if ((ret = __os_calloc(dbenv, sizeof(int *),
		    nentries * 32, &map->alloclist))!=0) {
		free_sparse_map(dbenv, map);

		return (ret);
	}

	map->nentries = nentries;
	map->alloccnt = 0;

	*sparse_map = map;

	return 0;
}

/*
 * copy_sparse_map --
 *  Copies everything in the original to the target. 
 */
static inline int
copy_sparse_map(dbenv, sparse_map, sparse_copy)
	DB_ENV *dbenv;
	sparse_map_t *sparse_map;
	sparse_map_t **sparse_copy;

{
	int ret, i, idx;
	sparse_map_t *copy;
	u_int32_t nentries;

	nentries = sparse_map->nentries;

	if ((ret = allocate_sparse_map(dbenv, nentries, &copy))!=0) {
		return (ret);
	}

	for (i = 0; i <sparse_map->alloccnt; i ++) {
		idx = sparse_map->alloclist[i];
		assert(NULL == copy->map[idx] && NULL !=sparse_map->map[idx]);

		if ((ret = __os_calloc(dbenv, sizeof(u_int32_t), nentries,
			    &copy->map[idx]))!=0) {
			free_sparse_map(dbenv, copy);

			return (ret);
		}
		memcpy(copy->map[idx], sparse_map->map[idx],
		    nentries * sizeof(u_int32_t));
		copy->alloclist[i] = idx;
	}
	copy->alloccnt = sparse_map->alloccnt;
	copy->nentries = nentries;

	*sparse_copy = copy;

	return 0;
}

/*
 * dump_regular_bitmap --
 *  For debugging.
 */
static inline void
dump_regular_bitmap(dbenv, idmap, bitmap, nlockers, nalloc, f)
	DB_ENV *dbenv;
	locker_info *idmap;
	u_int32_t *bitmap, nlockers, nalloc;
	FILE *f;
{
	u_int32_t *mymap;
	int i, j;

	logmsg(LOGMSG_USER, "REGULAR BITMAP:\n");

	for (mymap = bitmap, i = 0; i <nlockers; i ++, mymap += nalloc) {
		if (!idmap[i].valid)
			 continue;

		logmsg(LOGMSG_USER, "ID %d -> ", i);

		for (j = 0; j < nlockers; j++) {
			if (ISSET_MAP(mymap, j)) {
				logmsg(LOGMSG_USER, "(%d)", j);
			}
		}
		logmsg(LOGMSG_USER, "\n");
	}
}

/*
 * dump_sparse_bitmap --
 *  For debugging.
 */
static inline void
dump_sparse_bitmap(dbenv, idmap, sparse_map, nlockers, f)
	DB_ENV *dbenv;
	locker_info *idmap;
	u_int32_t nlockers;
	sparse_map_t *sparse_map;
	FILE *f;
{
	u_int32_t *mymap;
	int i, j, idx;

	logmsg(LOGMSG_USER, "SPARSE BITMAP:\n");

	for (i = 0; i <sparse_map->alloccnt; i ++) {
		idx = sparse_map->alloclist[i];
		mymap = sparse_map->map[idx];

		if (!idmap[idx].valid)
			continue;

		logmsg(LOGMSG_USER, "ID %d -> ", idx);

		for (j = 0; j < nlockers; j++) {
			if (ISSET_MAP(mymap, j)) {
				logmsg(LOGMSG_USER, "(%d)", j);
			}
		}
		logmsg(LOGMSG_USER, "\n");
	}
}

/*
 * __lock_detect_pp --
 *	DB_ENV->lock_detect pre/post processing.
 *
 * PUBLIC: int __lock_detect_pp __P((DB_ENV *, u_int32_t, u_int32_t, int *));
 */
int
__lock_detect_pp(dbenv, flags, atype, abortp)
	DB_ENV *dbenv;
	u_int32_t flags, atype;
	int *abortp;
{
	int ret, rep_check;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "DB_ENV->lock_detect", DB_INIT_LOCK);

	/* Validate arguments. */
	if ((ret = __db_fchk(dbenv, "DB_ENV->lock_detect", flags, 0)) != 0)
		return (ret);
	switch (atype) {
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
		    "DB_ENV->lock_detect: unknown deadlock detection mode specified");
		return (EINVAL);
	}

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;

	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __lock_detect(dbenv, atype, abortp);

	if (rep_check)
		__env_rep_exit(dbenv);

	return (ret);
}

static void
show_locker_info(DB_ENV *dbenv, DB_LOCKTAB *lt, DB_LOCKREGION *region,
    locker_info *idmap, int lid)
{
	u_int32_t ndx;
	DB_LOCKER *lockerp;
	int ret;

	LOCKREGION(dbenv, lt);
	lock_lockers(region);

	// Get the locker. 
	LOCKER_INDX(lt, region, idmap[lid].id, ndx);

	if ((ret = __lock_getlocker(lt, idmap[lid].id, ndx, 0,
		    GETLOCKER_KEEP_PART, &lockerp))!=0) {
		logmsg(LOGMSG_USER, "%s:%d__lock_getlocker ret: %d\n",
		    __func__, __LINE__, ret);
	} else if (lockerp != NULL) {
		logmsg(LOGMSG_USER, "lockerid=%lx, killme=%d, tid=%llx \n", idmap[lid].id,
		    idmap[lid].killme, lockerp->tid);
		struct __db_lock *lp =
		    SH_LIST_FIRST(&lockerp->heldby, __db_lock);
		__lock_printlock(lt, lp, 1, stdout);
		unlock_locker_partition(region, lockerp->partition);
	} else
		logmsg(LOGMSG_USER, "lockerid=%lx, killme=%d\n", idmap[lid].id,
		    idmap[lid].killme);

	unlock_lockers(region);
	UNLOCKREGION(dbenv, lt);
}

static inline int
is_valid_policy(int policy)
{
	if (policy > DB_LOCK_NORUN && policy <= DB_LOCK_MINWRITE_EVER)
		return 1;
	return 0;
}

static void
__dd_print_deadlock_cycle(idmap, deadmap, nlockers, victim)
	locker_info *idmap;
	u_int32_t *deadmap;
	u_int32_t nlockers, victim;
{
	int j;

	logmsg(LOGMSG_USER, "DEADLOCK-CYCLE: ");

	for (j = 0; j < nlockers; j++) {

		if (!ISSET_MAP(deadmap, j))
			continue;

		if (j == victim)
			logmsg(LOGMSG_USER, "*");
		logmsg(LOGMSG_USER, "%u(%u) ", idmap[j].id, idmap[j].count);
	}
	logmsg(LOGMSG_USER, "\n");
	fflush(stderr);
}


static void
__dd_print_tracked(idmap, deadmap, nlockers, victim)
	locker_info *idmap;
	u_int32_t *deadmap;
	u_int32_t nlockers, victim;
{
	int j;

	for (j = 0; j < nlockers; j++) {

		if (!ISSET_MAP(deadmap, j))
			continue;

		if (idmap[j].tracked) {
			if (j == victim)
				logmsg(LOGMSG_USER, 
                    "LOCKID %u CHOOSEN AS DEADLOCK VICTIM\n",
				    idmap[j].id);
			else
				logmsg(LOGMSG_USER, 
                    "LOCKID %u PART OF DEADLOCK CYCLE\n",
				    idmap[j].id);
		}
	}
}

uint64_t detect_skip = 0;
uint64_t detect_run = 0;

#define LOCK_DETECT_Q 1

#if LOCK_DETECT_Q
static int __lock_detect_int(DB_ENV *, u_int32_t atype, int *abortp,
    int *retry);
static pthread_mutex_t dlock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t qlock = PTHREAD_MUTEX_INITIALIZER;
static int q = 0;
#endif

/*
 * __lock_detect --
 *	DB_ENV->lock_detect.
 *
 * PUBLIC: int __lock_detect __P((DB_ENV *, u_int32_t, int *));
 */
int
__lock_detect(dbenv, atype, abortp)
	DB_ENV *dbenv;
	u_int32_t atype;
	int *abortp;
{
#if LOCK_DETECT_Q
	int ret;
	int skip;

	/* Run detector if one is not waiting to be run already */
	pthread_mutex_lock(&qlock);
	if (q) {
		skip = 1;
		++detect_skip;
	} else {
		skip = 0;
		q = 1;
	}
	pthread_mutex_unlock(&qlock);
	if (skip) return 0;

	pthread_mutex_lock(&dlock);
	{
		pthread_mutex_lock(&qlock);
		q = 0;
		pthread_mutex_unlock(&qlock);
		int retry = 0;
		ret = __lock_detect_int(dbenv, atype, abortp, &retry);
		if (retry)
			ret = __lock_detect_int(dbenv, atype, abortp, NULL);
	}
	pthread_mutex_unlock(&dlock);
	return ret;
}

static int
__lock_detect_int(dbenv, atype, abortp, can_retry)
	DB_ENV *dbenv;
	u_int32_t atype;
	int *abortp;
	int *can_retry;
{
#else
	int *can_retry = NULL;
#endif
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	DB_TXNMGR *tmgr;
	locker_info *idmap;
	sparse_map_t *sparse_map, *sparse_copymap;
	u_int32_t *bitmap, *copymap, **deadp, *deadwho, **free_me, *free_me_2,
	    *tmpmap;
	u_int32_t i, keeper, killid, limit, nalloc, nlockers, dwhoix;
	u_int32_t lock_max, txn_max;
	extern int gbl_print_deadlock_cycles;
	extern int gbl_deadlock_policy_override;
	int policy_override = gbl_deadlock_policy_override;
	int is_client;
	int ret;

	++detect_run;
	is_client = __rep_is_client(dbenv);

	if (!is_client) {
		/* master */
		if (atype == DB_LOCK_MINWRITE &&
		    dbenv->master_use_minwrite_ever)
			atype = DB_LOCK_MINWRITE_EVER;

		else
			atype = DB_LOCK_MINWRITE;
	} else {
		/* replicant */
		if (atype == DB_LOCK_MINWRITE &&
		    dbenv->replicant_use_minwrite_noread)
			atype = DB_LOCK_MINWRITE_NOREAD;

		else
			atype = DB_LOCK_MINWRITE;
	}

	if (is_valid_policy(policy_override))
		atype = policy_override;

	free_me = NULL;
	free_me_2 = NULL;

	lt = dbenv->lk_handle;
	if (abortp != NULL)
		*abortp = 0;

	/* Check if a detector run is necessary. */
	LOCKREGION(dbenv, lt);

	/* Make a pass only if auto-detect would run. */
	region = lt->reginfo.primary;
	lock_lockers(region);

	keeper = BAD_KILLID;
#if 0
	db_timeval_t now;
	LOCK_SET_TIME_INVALID(&now);

	if (region->need_dd == 0 &&
	    (!LOCK_TIME_ISVALID(&region->next_timeout) ||
		!__lock_expired(dbenv, &now, &region->next_timeout))) {
		UNLOCKREGION(dbenv, lt);
		unlock_lockers(region);

		return (0);
	}
	if (region->need_dd == 0)
		atype = DB_LOCK_EXPIRE;
#endif

	/* Reset need_dd, so we know we've run the detector. */
	region->need_dd = 0;

	/* Build the waits-for bitmap. */
	ret = __dd_build(dbenv, atype, &bitmap, &sparse_map, &nlockers, &nalloc,
	    &idmap, is_client);
	lock_max = region->stat.st_cur_maxid;
	unlock_lockers(region);

	UNLOCKREGION(dbenv, lt);

	/*
	 * We need the cur_maxid from the txn region as well.  In order
	 * to avoid tricky synchronization between the lock and txn
	 * regions, we simply unlock the lock region and then lock the
	 * txn region.  This introduces a small window during which the
	 * transaction system could then wrap.  We're willing to return
	 * the wrong answer for "oldest" or "youngest" in those rare
	 * circumstances.
	 */
	tmgr = dbenv->tx_handle;

	if (tmgr != NULL) {
		R_LOCK(dbenv, &tmgr->reginfo);

		txn_max = ((DB_TXNREGION *)tmgr->reginfo.primary)->cur_maxid;
		R_UNLOCK(dbenv, &tmgr->reginfo);
	} else
		txn_max = TXN_MAXIMUM;
	if (ret !=0 || atype == DB_LOCK_EXPIRE)
		return (ret);

	if (nlockers == 0)
		return (0);
#ifdef DIAGNOSTIC
	if (FLD_ISSET(dbenv->verbose, DB_VERB_WAITSFOR))
		 __dd_debug(dbenv, idmap, bitmap, sparse_map, nlockers, nalloc);
#endif
	/* Now duplicate the bitmaps so we can verify deadlock participants. */
	if (sparse_map) {
		copymap = NULL;
		if ((ret =
			copy_sparse_map(dbenv, sparse_map, &sparse_copymap))!=0)
			 goto err;
	} else {
		sparse_copymap = NULL;
		if ((ret = __os_calloc(dbenv, (size_t)nlockers,
			    sizeof(u_int32_t) * nalloc, &copymap))!=0)
			 goto err;

		memcpy(copymap, bitmap, nlockers * sizeof(u_int32_t) * nalloc);
	}

	if ((ret = __os_calloc(dbenv, sizeof(u_int32_t), nalloc, &tmpmap))!=0)
		 goto err1;

	int found_tracked = 0;

	/* Find a deadlock. */
	if ((ret = __dd_find(dbenv, bitmap, sparse_map, idmap, nlockers, nalloc,
		    &deadp, &deadwho, &found_tracked))!=0)
		 return (ret);

	killid = BAD_KILLID;
	free_me = deadp;
	free_me_2 = deadwho;

	/* dd_find creates an array of bitmaps, each of which describes a deadlock.
	 * A single locker is only allowed to be detected in a single deadlock (the first
	 * detected), so a locker can't be aborted for more than one deadlock. */
	for (dwhoix = 0; *deadp != NULL; deadp++, dwhoix++) {
		if (abortp != NULL)
			++*abortp;
		if (sparse_map) {
			killid = deadwho[dwhoix];
		} else
			killid = (u_int32_t) ((*deadp - bitmap) / nalloc);

		if ((atype == DB_LOCK_DEFAULT || atype == DB_LOCK_RANDOM) &&
				0 == idmap[killid].saveme)
			goto dokill;
		/*
		 * It's conceivable that under XA, the locker could
		 * have gone away.
		 */
		if (killid == BAD_KILLID)
			break;

		/*
		 * Start with the id that we know is deadlocked
		 * and then examine all other set bits and see
		 * if any are a better candidate for abortion
		 * and that they are genuinely part of the
		 * deadlock.  The definition of "best":
		 * OLDEST: smallest id
		 * YOUNGEST: largest id
		 * MAXLOCKS: maximum count
		 * MAXWRITE: maximum write count
		 * MINLOCKS: minimum count
		 * MINWRITE: minimum count
		 */

		if (FLD_ISSET(dbenv->verbose, DB_VERB_DEADLOCK)) {
			logmsg(LOGMSG_USER, "locker involved in deadlock with killid %d\n",
			    killid);
			show_locker_info(dbenv, lt, region, idmap, killid);
		}

		if (idmap[killid].killme) {
			if (FLD_ISSET(dbenv->verbose, DB_VERB_DEADLOCK)) {
				logmsg(LOGMSG_USER, "killid %d has killme set, killing it.\n",
				    killid);
			}
			goto dokill;
		}

		keeper = BAD_KILLID;
		limit = killid;

		for (i = (killid + 1) % nlockers;
		    i !=limit; i = (i +1)%nlockers) {
			if (!ISSET_MAP(*deadp, i) || idmap[i].in_abort)
				 continue;

			if (FLD_ISSET(dbenv->verbose, DB_VERB_DEADLOCK)) {
				logmsg(LOGMSG_USER, "conflicting with other transaction %d\n", i);
				show_locker_info(dbenv, lt, region, idmap, i);
			}
			/* A 'killme' lockerid is participating in a deadlock */
			if (idmap[i].killme) {
				keeper = killid;
				killid = i;

				goto dokill;
			}

			/* Switch out automatically for a non-saveme */
			if (idmap[killid].saveme && !idmap[i].saveme) {
				killid = i;

				continue;
			}

			if (!idmap[killid].saveme && idmap[i].saveme)
				 continue;

			/* killid and i are either either both saveme or not saveme */
			switch (atype) {
			case DB_LOCK_DEFAULT:
			case DB_LOCK_RANDOM:
				if (idmap[i].saveme) {
					keeper = i;

					break;
				}
				killid = i;

				goto dokill;
				break;

			case DB_LOCK_OLDEST:
				if (__dd_isolder(idmap[killid].id,
					idmap[i].id, lock_max, txn_max))
					 continue;
				keeper = i;

				break;
			case DB_LOCK_YOUNGEST:
				if (__dd_isolder(idmap[i].id,
					idmap[killid].id, lock_max, txn_max))
					 continue;
				keeper = i;

				break;
			case DB_LOCK_MAXLOCKS:
			case DB_LOCK_MAXWRITE:
				if (idmap[i].count <idmap[killid].count)
					 continue;
				keeper = i;

				break;

			case DB_LOCK_MINWRITE_EVER:
			case DB_LOCK_MINLOCKS:
			case DB_LOCK_MINWRITE:
			case DB_LOCK_MINWRITE_NOREAD:
				if (idmap[i].count >idmap[killid].count) {
					if (idmap[i].count >100) {
						/*
						 * fprintf(stderr, "sparing %d cause he has %d locks\n",
						 * i, idmap[i].count);
						 */
					}
					continue;
				}
				keeper = i;

				break;

			case DB_LOCK_YOUNGEST_EVER:
				/* if current younger, keep it */
				if (idmap[i].count <idmap[killid].count) {
					continue;
				}
				keeper = i;

				break;

			default:
				killid = BAD_KILLID;
				ret = EINVAL;

				goto dokill;
			}
			if (__dd_verify(idmap, *deadp,
				tmpmap, copymap, sparse_copymap, nlockers,
				nalloc, i))
				 killid = i;
		}

dokill:

		if (killid == BAD_KILLID)
			continue;

		/*
		 * There are cases in which our general algorithm will
		 * fail.  Returning 1 from verify indicates that the
		 * particular locker is not only involved in a deadlock,
		 * but that killing him will allow others to make forward
		 * progress.  Unfortunately, there are cases where we need
		 * to abort someone, but killing them will not necessarily
		 * ensure forward progress (imagine N readers all trying to
		 * acquire a write lock).  In such a scenario, we'll have
		 * gotten all the way through the loop, we will have found
		 * someone to keep (keeper will be valid), but killid will
		 * still be the initial deadlocker.  In this case, if the
		 * initial killid satisfies __dd_verify, kill it, else abort
		 * keeper and indicate that we need to run deadlock detection
		 * again.
		 */

		if (keeper != BAD_KILLID && killid == limit &&
		    __dd_verify(idmap, *deadp,
			tmpmap, copymap, sparse_copymap, nlockers, nalloc,
			killid) == 0) {
			LOCKREGION(dbenv, lt);
			region->need_dd = 1;
			UNLOCKREGION(dbenv, lt);

			killid = keeper;
		}

		if (berkdb_deadlock_callback) {
			struct berkdb_deadlock_info info;
			info.lid = idmap[killid].id;
			berkdb_deadlock_callback (&info);

#ifndef TESTSUITE
			if (debug_switch_stack_on_deadlock())
				stack_me(NULL);
#endif
		}

		if (idmap[killid].count == UINT_MAX && can_retry) {
			*can_retry = 1;
			goto out;
		}

		if (FLD_ISSET(dbenv->verbose, DB_VERB_DEADLOCK)) {
			__db_err(dbenv, "Aborting locker %lx, killme %d",
			    (u_long)idmap[killid].id, idmap[killid].killme);
			show_locker_info(dbenv, lt, region, idmap, killid);
		}

		if (found_tracked) {
			__dd_print_tracked(idmap, *deadp, nlockers, killid);
		}

		if (gbl_print_deadlock_cycles)
			__dd_print_deadlock_cycle(idmap, *deadp, nlockers, killid);

		/* Kill the locker with lockid idmap[killid]. */
		if ((ret = __dd_abort(dbenv, &idmap[killid]))!=0) {
			/*
			 * It's possible that the lock was already aborted;
			 * this isn't necessarily a problem, so do not treat
			 * it as an error.
			 */
			if (ret == DB_ALREADY_ABORTED)
				ret = 0;

			else
				__db_err(dbenv,
				    "warning: unable to abort locker %lx",
				    (u_long)idmap[killid].id);
		} else if (FLD_ISSET(dbenv->verbose, DB_VERB_DEADLOCK))
			 __db_err(dbenv, "Aborted locker %lx",
			    (u_long)idmap[killid].id);
	}
out:__os_free (dbenv, tmpmap);

err1:
	if (copymap != NULL)
		__os_free (dbenv, copymap);

err:
	if (free_me != NULL)
		__os_free (dbenv, free_me);

	if (free_me_2 != NULL)
		__os_free (dbenv, free_me_2);

	if (sparse_map)
		free_sparse_map(dbenv, sparse_map);

	if (sparse_copymap)
		free_sparse_map(dbenv, sparse_copymap);

	return (ret);
}

/*
 * ========================================================================
 * Utilities
 */

#define DD_INVALID_ID	((u_int32_t) -1)

inline static void
__init_lockerid_priority(dbenv, atype, lip, ptr_idarr)
	DB_ENV *dbenv;
	u_int32_t atype;
	DB_LOCKER *lip;
	locker_info *ptr_idarr;
{
	switch (atype) {
	case DB_LOCK_MINLOCKS:
	case DB_LOCK_MAXLOCKS:
		ptr_idarr->count = lip->nlocks;

		break;
	case DB_LOCK_MINWRITE:
	case DB_LOCK_MINWRITE_NOREAD:
		/* bias by the number of times we retried this txn */
		ptr_idarr->count = lip->nretries * dbenv->lk_max;
		ptr_idarr->count +=lip->nwrites;

		break;
	case DB_LOCK_YOUNGEST_EVER:
		ptr_idarr->count = lip->nretries;	/* this is the age in epoch seconds */

		break;
	case DB_LOCK_MINWRITE_EVER:
		ptr_idarr->count = lip->nretries;
		ptr_idarr->count +=lip->nwrites;

		break;
	}
}

inline static void
__adjust_lockerid_priority(dbenv, atype, lip, ptr_idarr)
	DB_ENV *dbenv;
	u_int32_t atype;
	DB_LOCKER *lip;
	locker_info *ptr_idarr;
{
	switch (atype) {
	case DB_LOCK_MINLOCKS:
	case DB_LOCK_MAXLOCKS:
		ptr_idarr->count +=lip->nlocks;

		break;
	case DB_LOCK_MINWRITE:
	case DB_LOCK_MINWRITE_NOREAD:
		/* bias by the number of times we retried this txn */
		ptr_idarr->count +=lip->nretries * dbenv->lk_max;
		ptr_idarr->count +=lip->nwrites;

		break;
	case DB_LOCK_YOUNGEST_EVER:
		if (ptr_idarr->count >lip->nretries)
			ptr_idarr->count = lip->nretries;	/* this is the age in epoch seconds */

		break;
	case DB_LOCK_MINWRITE_EVER:
		ptr_idarr->count +=lip->nretries;
		ptr_idarr->count +=lip->nwrites;

		break;
	}
}

#if TEST_DEADLOCKS
static void
__adjust_lockerid_priority_td(dbenv, atype, lip, dd_id, id_array, increment)
	DB_ENV *dbenv;
	u_int32_t atype;
	DB_LOCKER *lip;
	u_int32_t dd_id;
	locker_info *id_array;
	u_int32_t increment;
{
	if (increment)
		__adjust_lockerid_priority(dbenv, atype, lockerp,
		    &id_array[dd_id]);
	else
		__init_lockerid_priority(dbenv, atype, lip, &id_array[dd_id]);

	{
		int parent_id = -1;
		int parent_dd_id = -1;
		int master_id = -1;
		int master_dd_id = -1;
		DB_LOCKTAB *lt;

		lt = dbenv->lk_handle;

		if (lip->parent_locker != INVALID_ROFF) {
			parent_id =
			    ((DB_LOCKER *)R_ADDR(&lt->reginfo,
				lip->parent_locker))->id;
			parent_dd_id =
			    ((DB_LOCKER *)R_ADDR(&lt->reginfo,
				lip->parent_locker))->dd_id;
		}
		if (lip->master_locker != INVALID_ROFF) {
			master_id =
			    ((DB_LOCKER *)R_ADDR(&lt->reginfo,
				lip->master_locker))->id;
			master_dd_id =
			    ((DB_LOCKER *)R_ADDR(&lt->reginfo,
				lip->master_locker))->dd_id;
		}


		printf
		    ("%d %s:%d lockerid %x got priority %d [dd_id=%d] parent %x[%d] = %d master %x[%d] =%d\n",
		    pthread_self(), __FILE__, __LINE__, lip->id,
		    id_array[dd_id].count, dd_id, parent_id, parent_dd_id,
		    (parent_dd_id != -1) ? id_array[parent_dd_id].count : -1,
		    master_id, master_dd_id,
		    (master_dd_id != -1) ? id_array[master_dd_id].count : -1);
	}
}
#endif


/* we populate object from scratch so don't need previous
 * content; a realloc is more expensive than just free/malloc
 * because it may need to copy content to new area 
 * when ask size is larger than current, allocate 1.5 as much
 * otherwise do nothing 
 */
static inline int __resize_object(DB_ENV *dbenv, void **obj, size_t *obj_size, 
	size_t new_size)
{
	if(new_size < *obj_size)
		return 0;
	new_size = new_size + new_size/2;
	__os_free(dbenv, *obj);
	int ret = __os_malloc (dbenv, new_size, obj);
	if (ret)
		return ret;
	*obj_size = new_size;
	return 0;
}


static int
__dd_build(dbenv, atype, bmp, smap, nlockers, allocp, idmap, is_replicant)
	DB_ENV *dbenv;
	u_int32_t atype, **bmp, *nlockers, *allocp;
	sparse_map_t **smap;
	locker_info **idmap;
	int is_replicant;
{
	struct __db_lock *lp;
	DB_LOCKER *lockerp, *child;
	DB_LOCKOBJ *op, *lo;
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	db_timeval_t now, min_timeout;
	u_int32_t count, dd, *entryp, id, ndx, nentries;
	sparse_map_t *sparse_map = NULL;
	u_int8_t *pptr;
	size_t allocSz;
	int is_first, ret;

	static u_int32_t *dd_bitmap = NULL;
	static size_t dd_bitmap_size = 0;
	static u_int32_t *dd_tmpmap = NULL;
	static size_t dd_tmpmap_size = 0;
	static locker_info *dd_id_array = NULL;
	static size_t dd_id_array_size = 0;


	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;
	LOCK_SET_TIME_INVALID(&now);

	LOCK_SET_TIME_MAX(&min_timeout);
	int expire_only = (atype == DB_LOCK_EXPIRE);


	/*
	 * While we always check for expired timeouts, if we are called
	 * with DB_LOCK_EXPIRE, then we are only checking for timeouts
	 * (i.e., not doing deadlock detection at all).  If we aren't
	 * doing real deadlock detection, then we can skip a significant,
	 * amount of the processing.  In particular we do not build
	 * the conflict array and our caller needs to expect this.
	 */
	if (expire_only) {
		count = 0;
		nentries = 0;
		goto obj_loop;
	}

	/*
	 * We'll check how many lockers there are, add a few more in for
	 * good measure and then allocate all the structures.  Then we'll
	 * verify that we have enough room when we go back in and get the
	 * mutex the second time.
	 */
retry:	count = region->stat.st_nlockers;

	if (count == 0) {
		*nlockers = 0;
		return (0);
	}

	if (FLD_ISSET(dbenv->verbose, DB_VERB_DEADLOCK))
		 __db_err(dbenv, "%lu lockers", (u_long)count);

	count +=20;
	allocSz = (size_t)count * sizeof(locker_info);

	ret = __resize_object(dbenv, (void**) &dd_id_array, 
		&dd_id_array_size, allocSz);
	if(ret) {
		if (sparse_map) 
			free_sparse_map(dbenv, sparse_map);
		return ret;
	}
	memset(dd_id_array, 0, allocSz);


	/*
	 * Now go back in and actually fill in the matrix.
	 */
	if (region->stat.st_nlockers > count) {
		if (sparse_map) {
			free_sparse_map(dbenv, sparse_map);
		}
		goto retry;
	}

	/*
	 * First we go through and assign a deadlock detector id
	 * to each master locker which is in waiting status 
	 */
	id = 0;

	for (DB_LOCKER *lip = SH_TAILQ_FIRST(&region->lockers, __db_locker);
		lip != NULL; lip = SH_TAILQ_NEXT(lip, ulinks, __db_locker)) {
		if (lip->wstatus == 1) {	/*only master lockers can be in waiting status */
			lip->dd_id = id ++;
			locker_info *ptr_idarr = &dd_id_array[lip->dd_id];
			ptr_idarr->id = lip->id;

			ptr_idarr->tid = lip->tid;
			ptr_idarr->killme = F_ISSET(lip, DB_LOCKER_KILLME);
			ptr_idarr->readonly = F_ISSET(lip, DB_LOCKER_READONLY);
			ptr_idarr->saveme =
				F_ISSET(lip, (DB_LOCKER_LOGICAL | DB_LOCKER_IN_LOGICAL_ABORT));
			ptr_idarr->in_abort =
				(F_ISSET(lip, DB_LOCKER_INABORT) != 0);
			ptr_idarr->tracked =
				(F_ISSET(lip, DB_LOCKER_TRACK) != 0);

#if TEST_DEADLOCKS
			__adjust_lockerid_priority_td(dbenv, atype, lip,
				lip->dd_id, dd_id_array, 0);
#else
			__init_lockerid_priority(dbenv, atype, lip, ptr_idarr);
#endif

			if (verbose_deadlocks &&lip->id >DB_LOCK_MAXID)
				logmsg(LOGMSG_USER, "Added lip %p id=%x dd_id=%x count=%u\n",
					lip, lip->id, lip->dd_id, ptr_idarr->count);

		} else {
			lip->dd_id = DD_INVALID_ID;
		}
	}

	count = id;
	nentries = ALIGN(count, 32) / 32;

	if (gbl_sparse_lockerid_map) {
		if ((ret = allocate_sparse_map(dbenv, nentries, &sparse_map))!=0) {
			return (ret);
		}
		dd_bitmap = NULL;
	} else {
		allocSz = (size_t)count * sizeof(u_int32_t) * nentries;
		ret = __resize_object(dbenv, (void**) &dd_bitmap, 
			&dd_bitmap_size, allocSz);
		if (ret)
			return ret;
		memset(dd_bitmap, 0, allocSz);
		sparse_map = NULL;
	}

	allocSz = sizeof(u_int32_t) * nentries;
	ret = __resize_object(dbenv, (void**) &dd_tmpmap, &dd_tmpmap_size, allocSz);
	if(ret) {
		if (sparse_map) 
			free_sparse_map(dbenv, sparse_map);
		return ret;
	}
	memset(dd_tmpmap, 0, allocSz);


	/*
	 * We only need consider objects that have waiters, so we use
	 * the list of objects with waiters (dd_objs) instead of traversing
	 * the entire hash table.  For each object, we traverse the waiters
	 * list and add an entry in the waitsfor matrix for each waiter/holder
	 * combination.
	 */
obj_loop:
	lock_detector(region);
	op = SH_TAILQ_FIRST(&region->dd_objs, __db_lockobj);

	while (op !=NULL) {
		u_int32_t partition = op->partition;
		u_int32_t generation = op->generation;

		if (partition < gbl_lk_parts) {
			unlock_detector(region);
			lock_obj_partition(region, partition);
		} else {
			puts("WHAT IS THIS STATE?");
			abort();
		}
		if (partition != op->partition || generation != op->generation) {
			unlock_obj_partition(region, partition);

			if (sparse_map) {
				clear_sparse_map(dbenv, sparse_map);
			} else {
				memset(dd_bitmap, 0, count *sizeof(u_int32_t) * nentries);
			}
			goto obj_loop;
		}

		if (expire_only)
			goto look_waiters;
		CLEAR_MAP(dd_tmpmap, nentries);

		/*
		 * First we go through and create a bit map that
		 * represents all the holders of this object.
		 */
		int has_master = 0;

		for (lp = SH_TAILQ_FIRST(&op->holders, __db_lock);
			lp !=NULL; lp = SH_TAILQ_NEXT(lp, links, __db_lock)) {
			lockerp = lp->holderp;
			has_master = 0;

			if (lockerp->dd_id == DD_INVALID_ID) {
				if (lockerp->master_locker == INVALID_ROFF)	//master not in waiting status
					continue;
				dd = ((DB_LOCKER *)R_ADDR(&lt->reginfo,
					lockerp->master_locker))->dd_id;
				if (dd == DD_INVALID_ID)	//locker is not in waiting status 
					continue;
				lockerp->dd_id = dd;
#if TEST_DEADLOCKS
				__adjust_lockerid_priority_td(dbenv, atype,
					lockerp, dd, dd_id_array, 1);
#else
				__adjust_lockerid_priority(dbenv, atype,
					lockerp, &dd_id_array[dd]);
#endif
				if (F_ISSET(lockerp, DB_LOCKER_INABORT))
					dd_id_array[dd].in_abort = 1;
				has_master = 1;

			} else
				dd = lockerp->dd_id;
			dd_id_array[dd].valid = 1;

			if (verbose_deadlocks)
				logmsg(LOGMSG_USER, 
					"Marking valid holder after increment lip %p id=%x dd_id=%x count=%u master=%x\n",
						lockerp, lockerp->id, lockerp->dd_id,
						dd_id_array[lockerp->dd_id].count,
						(has_master)? lockerp->master_locker : 0);

			/*
			 * If the holder has already been aborted, then
			 * we should ignore it for now.
			 */
			if (lp->status == DB_LSTAT_HELD)
				SET_MAP(dd_tmpmap, dd);
		}

		/*
		 * Next, for each waiter, we set its row in the matrix
		 * equal to the map of holders we set up above.
		 *
		 * These are already waiting, so no need to acquire
		 * locker-partition mutex - we hold this obj-partition
		 * mutex already.
		 */
look_waiters:
		for (is_first = 1, lp = SH_TAILQ_FIRST(&op->waiters, __db_lock);
			lp !=NULL;
			is_first = 0, lp = SH_TAILQ_NEXT(lp, links, __db_lock)) {
			lockerp = lp->holderp;

			if (lockerp == NULL)
				continue;

			if (lp->status == DB_LSTAT_WAITING) {
				if (__lock_expired(dbenv, &now, &lockerp->lk_expire)) {
					lp->status = DB_LSTAT_EXPIRED;
					MUTEX_UNLOCK(dbenv, &lp->mutex);
					continue;
				}
				if (LOCK_TIME_GREATER(&min_timeout, &lockerp->lk_expire))
					min_timeout = lockerp->lk_expire;

			}

			if (expire_only)
				continue;

			int has_master = 0;

			if (lockerp->dd_id == DD_INVALID_ID) {
				if (lockerp->master_locker == INVALID_ROFF)	//master not in waiting status
					continue;
				dd = ((DB_LOCKER *)R_ADDR(&lt->reginfo,
					lockerp->master_locker))->dd_id;
				if (dd == DD_INVALID_ID)	//locker is not in waiting status 
					continue;
				lockerp->dd_id = dd;
#if TEST_DEADLOCKS
				__adjust_lockerid_priority_td(dbenv, atype,
					lockerp, dd, dd_id_array, 1);
#else
				__adjust_lockerid_priority(dbenv, atype,
					lockerp, &dd_id_array[dd]);
#endif
				has_master = 1;

			} else
				dd = lockerp->dd_id;
			dd_id_array[dd].valid = 1;

			if (verbose_deadlocks)
				logmsg(LOGMSG_USER, "Marking valid waiter after increment lip %p id=%x dd_id=%x count=%u master=%x\n",
					lockerp, lockerp->id, lockerp->dd_id,
					dd_id_array[lockerp->dd_id].count,
					(has_master)? lockerp->master_locker : 0);

			/*
			 * If the transaction is pending abortion, then
			 * ignore it on this iteration.
			 */
			if (lp->status != DB_LSTAT_WAITING) {
				continue;
			}

			if (sparse_map) {
				if (sparse_map->map[dd] == NULL) {
					if ((ret =
						__os_calloc(dbenv, nentries, sizeof(u_int32_t),
							&sparse_map->map[dd]))!=0) {
						free_sparse_map(dbenv, sparse_map);
						unlock_obj_partition(region, partition);
						return (ret);
					}
					sparse_map->alloclist[sparse_map->alloccnt++] = dd;
				}
				entryp = sparse_map->map[dd];
			} else {
				entryp = dd_bitmap + (nentries * dd);
			}
			OR_MAP(entryp, dd_tmpmap, nentries);
			/*
			 * If this is the first waiter on the queue,
			 * then we remove the waitsfor relationship
			 * with oneself.  However, if it's anywhere
			 * else on the queue, then we have to keep
			 * it and we have an automatic deadlock.
			 */
			if (is_first) {
				if (ISSET_MAP(entryp, dd))
					dd_id_array[dd].self_wait = 1;
				CLR_MAP(entryp, dd);
			}
		}
		lock_detector(region);
		op = SH_TAILQ_NEXT(op, dd_links, __db_lockobj);
		unlock_obj_partition(region, partition);
	}
	unlock_detector(region);

	if (LOCK_TIME_ISVALID(&region->next_timeout)) {
		if (LOCK_TIME_ISMAX(&min_timeout))
			LOCK_SET_TIME_INVALID(&region->next_timeout);
		else
			region->next_timeout = min_timeout;
	}

	if (expire_only)
		return (0);

	int fix_pure_readers = 0;
	if (atype == DB_LOCK_MINWRITE_NOREAD || atype == DB_LOCK_MINWRITE_EVER)
		fix_pure_readers = 1;

	/* Now for each locker; record its last lock. */
	u_int32_t lkr_partition = gbl_lkr_parts;
	u_int32_t lpartition = gbl_lk_parts;

	for (id = 0; id <count; id ++) {
		if (!dd_id_array[id].valid)
			 continue;

		/* MINWRITE_NOREAD and MINWRITE_EVER count write-locks, not readlocks */
		if (fix_pure_readers && dd_id_array[id].count == 0 &&
			dd_id_array[id].readonly) {
			/*fprintf(stderr, "setting count to uint_max\n"); */
			dd_id_array[id].count = (UINT_MAX -1);
			if (verbose_deadlocks &&dd_id_array[id].
				id >DB_LOCK_MAXID)
				logmsg(LOGMSG_USER, "Adjusted id=%x dd_id=%x count=%u\n",
					dd_id_array[id].id, id,
					dd_id_array[id].count);
		}

		LOCKER_INDX(lt, region, dd_id_array[id].id, ndx);

		if ((ret = __lock_getlocker(lt, dd_id_array[id].id,
				ndx, 0, GETLOCKER_KEEP_PART, &lockerp))!=0) {
			__db_err(dbenv,
				"No locks for locker %lu",
				(u_long)dd_id_array[id].id);
			continue;
		}
		if (lockerp == NULL) {
			continue;
			/*
			 * } else if (F_ISSET(lockerp, DB_LOCKER_DELETED)) {
			 * unlock_locker_partition(region, lockerp->partition);
			 * continue;
			 */
		}

		/* Rep-thread can't release rowlocks (it'll corrupt the database) */
		if (is_replicant &&
			gbl_rowlocks &&F_ISSET(lockerp, DB_LOCKER_LOGICAL)) {
			dd_id_array[id].count = (UINT_MAX);
		}

		lkr_partition = lockerp->partition;

		if (lkr_partition != lockerp->id %gbl_lkr_parts) {
			puts("HUH!!");
			abort();
		}

		/*
		 * If this is a master transaction, try to
		 * find one of its children's locks first,
		 * as they are probably more recent.
		 */
		child = SH_LIST_FIRST(&lockerp->child_locker, __db_locker);

		if (child !=NULL) {
			do {
again1:			lp = SH_LIST_FIRST(&child->heldby,
					__db_lock);
				if (lp !=NULL) {
					lpartition = lp->lpartition;

					if (lpartition >= gbl_lk_parts) {
						if (SH_LIST_EMPTY(&lockerp->
							child_locker)) {
							goto next_child;
						} else {
							goto again1;
						}
					}

					lock_obj_partition(region, lpartition);

					if (SH_LIST_EMPTY(&lockerp-> child_locker)) {
						unlock_obj_partition(region, lpartition);
						goto next_child;
					}
					if (lp !=SH_LIST_FIRST(&child->heldby, __db_lock)) {
						unlock_obj_partition(region, lpartition);
						goto again1;
					}
					if (lpartition != lp->lpartition) {
						unlock_obj_partition(region, lpartition);
						goto again1;
					}
					if (lp->status == DB_LSTAT_WAITING) {
						dd_id_array[id].last_locker_id = child->id;
						lo = lp->lockobj;

						if (lo->partition !=lp->lpartition) {
							unlock_obj_partition(region, lpartition);
							//Fail fast for now - want to catch it doing this
							abort();
						}
						goto get_lock;
					}
					unlock_obj_partition(region, lpartition);
				}
next_child:			child = SH_LIST_NEXT(child, child_link, __db_locker);
			} while (child !=NULL);
		}

again2:	lp = SH_LIST_FIRST(&lockerp->heldby, __db_lock);

		if (lp !=NULL) {
			lpartition = lp->lpartition;

			if (lpartition >= gbl_lk_parts) {
				if (SH_LIST_EMPTY(&lockerp->heldby)) {
					unlock_locker_partition(region,
						lkr_partition);
					continue;
				} else {
					puts("THIS IS STILL HAPPENING...");
					abort();
				}
			}
			lock_obj_partition(region, lpartition);

			if (SH_LIST_EMPTY(&lockerp->heldby)) {
				goto out;
			}
			if (lp !=SH_LIST_FIRST(&lockerp->heldby, __db_lock)) {
				unlock_obj_partition(region, lpartition);

				goto again2;
			}
			if (lpartition != lp->lpartition) {
				unlock_obj_partition(region, lpartition);

				goto again2;
			}
			lo = lp->lockobj;

			if (lo->partition !=lp->lpartition) {
				unlock_obj_partition(region, lpartition);

				abort();
				goto again2;
			}
			dd_id_array[id].last_locker_id = lockerp->id;
get_lock:	dd_id_array[id].last_lock = R_OFFSET(&lt->reginfo, lp);
			dd_id_array[id].last_obj = lp->lockobj;
			pptr = lo->lockobj.data;

			if (lo->lockobj.size >=sizeof(db_pgno_t))
				memcpy(&dd_id_array[id].pgno, pptr, sizeof(db_pgno_t));
			else
				dd_id_array[id].pgno = 0;
out:		unlock_obj_partition(region, lpartition);
		}
		unlock_locker_partition(region, lkr_partition);
	}

	/*
	 * Pass complete, reset the deadlock detector bit.
	 */
	region->need_dd = 0;

	/*
	 * If this is a sparse-map, sort the alloclist.
	 */
	if (sparse_map) {
		qsort(sparse_map->alloclist, sparse_map->alloccnt, sizeof(int),
			sparse_map_cmp);
	}

	/*
	 * Now we can release everything except the bitmap matrix that we
	 * created.
	 */
	*nlockers = id;

	*idmap = dd_id_array;
	*bmp = dd_bitmap;
	*smap = sparse_map;

	*allocp = nentries;
	return (0);
}

static int
__dd_find(dbenv, bmp, sparse_map, idmap, nlockers, nalloc, deadp, deadwho,
		found_tracked)
	DB_ENV *dbenv;
	u_int32_t *bmp, nlockers, nalloc;
	sparse_map_t *sparse_map;
	locker_info *idmap;
	u_int32_t ***deadp;
	u_int32_t **deadwho;
	int *found_tracked;
{
	u_int32_t i, j, k, *mymap, *tmpmap, endcnt, idx;
	u_int32_t **retp, *whop;
	int ndead, ndeadalloc, ret;

#undef	INITIAL_DEAD_ALLOC
#define	INITIAL_DEAD_ALLOC	8

	ndeadalloc = INITIAL_DEAD_ALLOC;
	ndead = 0;
	whop = NULL;
	*deadwho = NULL;

	if ((ret = __os_malloc (dbenv,
				ndeadalloc * sizeof(u_int32_t *), &retp))!=0)
		 return (ret);

	if (sparse_map &&(ret = __os_malloc (dbenv,
				ndeadalloc * sizeof(u_int32_t), &whop))!=0) {
		__os_free (dbenv, retp);

		return (ret);
	}

	/*
	 * For each locker, OR in the bits from the lockers on which that
	 * locker is waiting.
	 */

	if (sparse_map)
		endcnt = sparse_map->alloccnt;

	else
		endcnt = nlockers;

	for (idx = 0; idx < endcnt; idx++) {
		int is_tracked = 0;

		if (sparse_map) {
			i = sparse_map->alloclist[idx];
			mymap = sparse_map->map[i];
		} else {
			mymap = bmp + (nalloc * idx);
			i = idx;
		}

		if (!idmap[i].valid || idmap[i].in_abort)
			 continue;

		if (idmap[i].tracked)
			is_tracked = 1;

		for (j = 0; j < nlockers; j++) {
			if (!ISSET_MAP(mymap, j))
				continue;

			/* Find the map for this bit. */
			if (sparse_map) {
				tmpmap = sparse_map->map[j];
			} else {
				tmpmap = bmp + (nalloc * j);
			}
			if (tmpmap)
				OR_MAP(mymap, tmpmap, nalloc);

			if (idmap[j].tracked)
				is_tracked = 1;

			if (!ISSET_MAP(mymap, i))
				 continue;

			if (is_tracked)
				*found_tracked = 1;

			/* Make sure we leave room for NULL. */
			if (ndead + 2 >= ndeadalloc) {
				ndeadalloc <<= 1;
				/*
				 * If the alloc fails, then simply return the
				 * deadlocks that we already have.
				 */
				if (__os_realloc(dbenv,
					ndeadalloc * sizeof(u_int32_t *),
					&retp) != 0) {
					retp[ndead] = NULL;

					*deadp = retp;
					*deadwho = whop;
					return (0);
				}
				if (sparse_map &&__os_realloc(dbenv,
					ndeadalloc * sizeof(u_int32_t),
					&whop) != 0) {
					retp[ndead] = NULL;

					*deadp = retp;
					*deadwho = whop;
					return (0);
				}

			}
			if (sparse_map) {
				whop[ndead] = i;
			}
			retp[ndead++] = mymap;

			/* Mark all participants in this deadlock invalid. */
			for (k = 0; k < nlockers; k++)
				if (ISSET_MAP(mymap, k))
					idmap[k].valid = 0;
			break;
		}
	}
	retp[ndead] = NULL;

	*deadp = retp;
	*deadwho = whop;
	return (0);
}

static unsigned long long already_aborted_count = 0;
extern int gbl_already_aborted_trace;

static void
already_aborted_trace(int lineno)
{
	if (gbl_already_aborted_trace) {
		static int lastpr = 0;
		int now;

		if ((now = time (NULL))>lastpr) {
			logmsg(LOGMSG_USER, "Already aborted count is %llu lineno is %d\n",
					already_aborted_count, lineno);
			lastpr = now;
		}
	}
}

static int
__dd_abort(dbenv, info)
	DB_ENV *dbenv;
	locker_info *info;
{
	struct __db_lock *lockp;
	DB_LOCKER *lockerp;
	DB_LOCKOBJ *sh_obj;
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	u_int32_t ndx, partition;
	int ret;

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;

	LOCKREGION(dbenv, lt);
	lock_lockers(region);

	/*
	 * Get the locker.  If its gone or was aborted while
	 * we were detecting return that.
	 */
	LOCKER_INDX(lt, region, info->last_locker_id, ndx);

	if ((ret = __lock_getlocker(lt, info->last_locker_id, ndx, 0,
			GETLOCKER_KEEP_PART, &lockerp))!=0) {
		printf("%s:%d__lock_getlocker ret: %d\n",
			__func__, __LINE__, ret);
		goto out;
	}
	if (lockerp == NULL) {
		ret = DB_ALREADY_ABORTED;

		already_aborted_count++;
		already_aborted_trace(__LINE__);
		goto out;
	}
	if (F_ISSET(lockerp, DB_LOCKER_INABORT)) {
		ret = DB_ALREADY_ABORTED;

		already_aborted_count++;
		already_aborted_trace(__LINE__);

		goto unlock;
	}
	if (SH_LIST_EMPTY(&lockerp->heldby)) {
		region->need_dd = 1;

		already_aborted_count++;
		already_aborted_trace(__LINE__);
		ret = DB_ALREADY_ABORTED;

		goto unlock;
	}

	/*
	 * Find the locker's last lock.
	 * It is possible for this lock to have been freed,
	 * either though a timeout or another detector run.
	 */
	if ((lockp = SH_LIST_FIRST(&lockerp->heldby, __db_lock)) == NULL) {
		already_aborted_count++;
		already_aborted_trace(__LINE__);
		ret = DB_ALREADY_ABORTED;

		goto unlock;
	}
	partition = lockp->lpartition;
	lock_obj_partition(region, partition);

	if (R_OFFSET(&lt->reginfo, lockp) != info->last_lock ||
		lockp->holderp->id !=lockerp->id ||lockp->lockobj !=info->last_obj
		|| lockp->status !=DB_LSTAT_WAITING) {
		already_aborted_count++;
		already_aborted_trace(__LINE__);
		ret = DB_ALREADY_ABORTED;

		goto ounlock;
	}

	/* save the priority to be retrieved by __txn_getpriority before aborting the code */

	if (verbose_deadlocks)
		logmsg(LOGMSG_USER, 
			"%d %s:%d lockerid %x abort with priority %d id=%d dd_id=%d nlocks=%d, npagelocks=%d nwrites=%d master_locker=%d parent_locker=%d\n",
			pthread_self(), __FILE__, __LINE__, lockerp->id,
			info->count, lockerp->id, lockerp->dd_id, lockerp->nlocks,
			lockerp->npagelocks, lockerp->nwrites,
			lockerp->master_locker, lockerp->parent_locker);
	lockerp->nretries = info->count;

	sh_obj = lockp->lockobj;

	/* Abort lock, take it off list, and wake up this lock. */
	//SHOBJECT_LOCK(lt, region, sh_obj, ndx);
	lockp->status = DB_LSTAT_ABORTED;
	SH_TAILQ_REMOVE(&sh_obj->waiters, lockp, links, __db_lock);

	/*
	 * Either the waiters list is now empty, in which case we remove
	 * it from dd_objs, or it is not empty, in which case we need to
	 * do promotion.
	 */
	if (SH_TAILQ_FIRST(&sh_obj->waiters, __db_lock) == NULL) {
		lock_detector(region);
		SH_TAILQ_REMOVE(&region->dd_objs,
		    sh_obj, dd_links, __db_lockobj);
		++sh_obj->generation;
		unlock_detector(region);
	} else {
		int state_changed;
		ret = __lock_promote(lt, sh_obj, &state_changed, 0);
	}
	MUTEX_UNLOCK(dbenv, &lockp->mutex);

	region->stat.st_ndeadlocks++;
ounlock:unlock_obj_partition(region, partition);
unlock:unlock_locker_partition(region, lockerp->partition);
out:unlock_lockers(region);
	UNLOCKREGION(dbenv, lt);

	return (ret);
}


#ifdef DIAGNOSTIC
static void
__dd_debug(dbenv, idmap, bitmap, sparse_map, nlockers, nalloc)
	DB_ENV *dbenv;
	locker_info *idmap;
	sparse_map_t *sparse_map;
	u_int32_t *bitmap, nlockers, nalloc;
{
	u_int32_t i, idx, j, *mymap;
	char *msgbuf;

	__db_err(dbenv, "Waitsfor array\nWaiter:\tWaiting on:");

	/* Allocate space to print 10 bytes per item waited on. */
#undef	MSGBUF_LEN
#define	MSGBUF_LEN ((nlockers + 1) * 10 + 64)
	if (__os_malloc (dbenv, MSGBUF_LEN, &msgbuf) != 0)
		 return;

	if (sparse_map) {
		for (i = 0; i <sparse_map->alloccnt; i ++) {
			idx = sparse_map->alloclist[i];
			assert(idx < sparse_map->nentries);

			if (!idmap[idx].valid)
				continue;
			sprintf(msgbuf,	/* Waiter. */
			    "%lx/%lu:\t", (u_long)idmap[idx].id,
			    (u_long)idmap[idx].pgno);
			mymap = sparse_map->map[idx];

			for (j = 0; j < sparse_map->nentries; j++)
				if (ISSET_MAP(mymap, j))
					sprintf(msgbuf, "%s %lx", msgbuf,
					    (u_long)idmap[j].id);
			(void)sprintf(msgbuf,
			    "%s %lu", msgbuf, (u_long)idmap[idx].last_lock);
			__db_err(dbenv, msgbuf);
		}
	}

	else {
		for (mymap = bitmap, i = 0; i <nlockers; i ++, mymap += nalloc) {
			if (!idmap[i].valid)
				 continue;
			sprintf(msgbuf,	/* Waiter. */
			    "%lx/%lu:\t", (u_long)idmap[i].id,
			    (u_long)idmap[i].pgno);
			for (j = 0; j < nlockers; j++)

				if (ISSET_MAP(mymap, j))
					sprintf(msgbuf, "%s %lx", msgbuf,
					    (u_long)idmap[j].id);
			(void)sprintf(msgbuf,
			    "%s %lu", msgbuf, (u_long)idmap[i].last_lock);
			__db_err(dbenv, msgbuf);
		}

		__os_free (dbenv, msgbuf);
	}
}
#endif

/*
 * Given a bitmap that contains a deadlock, verify that the bit
 * specified in the which parameter indicates a transaction that
 * is actually deadlocked.  Return 1 if really deadlocked, 0 otherwise.
 * deadmap is the array that identified the deadlock.
 * tmpmap is a copy of the initial bitmaps from the dd_build phase
 * origmap is a temporary bit map into which we can OR things
 * nlockers is the number of actual lockers under consideration
 * nalloc is the number of words allocated for the bitmap
 * which is the locker in question
 */
static int
__dd_verify(idmap, deadmap, tmpmap, origmap, orig_sparse, nlockers, nalloc,
    which)
	locker_info *idmap;
	sparse_map_t *orig_sparse;
	u_int32_t *deadmap, *tmpmap, *origmap;
	u_int32_t nlockers, nalloc, which;
{
	u_int32_t *tmap;
	u_int32_t j;
	int count;

	memset(tmpmap, 0, sizeof(u_int32_t) * nalloc);

	/*
	 * In order for "which" to be actively involved in
	 * the deadlock, removing him from the evaluation
	 * must remove the deadlock.  So, we OR together everyone
	 * except which; if all the participants still have their
	 * bits set, then the deadlock persists and which does
	 * not participate.  If the deadlock does not persist
	 * then "which" does participate.
	 */
	count = 0;

	for (j = 0; j < nlockers; j++) {
		if (!ISSET_MAP(deadmap, j) || j == which)
			continue;

		/* Find the map for this bit. */
		if (orig_sparse)
			tmap = orig_sparse->map[j];

		else
			tmap = origmap + (nalloc * j);

		/*
		 * We special case the first waiter who is also a holder, so
		 * we don't automatically call that a deadlock.  However, if
		 * it really is a deadlock, we need the bit set now so that
		 * we treat the first waiter like other waiters.
		 */
		if (idmap[j].self_wait && tmap)
			SET_MAP(tmap, j);

		if (tmap)
			OR_MAP(tmpmap, tmap, nalloc);
		count ++;
	}

	if (count == 1)
		return (1);

	/*
	 * Now check the resulting map and see whether
	 * all participants still have their bit set.
	 */
	for (j = 0; j < nlockers; j++) {
		if (!ISSET_MAP(deadmap, j) || j == which)
			continue;
		if (!ISSET_MAP(tmpmap, j))
			 return (1);
	}
	return (0);
}

/*
 * __dd_isolder --
 *
 * Figure out the relative age of two lockers.  We make all lockers
 * older than all transactions, because that's how it's worked
 * historically (because lockers are lower ids).
 */
static int
__dd_isolder(a, b, lock_max, txn_max)
	u_int32_t	a, b;
	u_int32_t	lock_max, txn_max;
{
	u_int32_t max;

	/* Check for comparing lock-id and txnid. */
	if (a <= DB_LOCK_MAXID && b > DB_LOCK_MAXID)
		return (1);
	if (b <= DB_LOCK_MAXID && a > DB_LOCK_MAXID)
		return (0);

	/* In the same space; figure out which one. */
	max = txn_max;
	if (a <= DB_LOCK_MAXID)
		max = lock_max;

	/*
	 * We can't get a 100% correct ordering, because we don't know
	 * where the current interval started and if there were older
	 * lockers outside the interval.  We do the best we can.
	 */

	/*
	 * Check for a wrapped case with ids above max.
	 */
	if (a > max && b < max)
		return (1);
	if (b > max && a < max)
		return (0);

	return (a < b);
}



/* Snapisol saves us from this without changing anything!  No snapisol session
 * would ever need to hold a rowlock & so won't run this code.  Also, the 
 * replication stream wouldn't need to grab any rowlocks if it could be sure
 * that only snapisol sessions were running. 
 *
 * This is called with the object partition locked.  The new holder will already 
 * be first in the waitlist, so there should be no race.
 *
 * PUBLIC: int __dd_abort_holders __P((DB_ENV *, DB_LOCKOBJ *));
 */
int
__dd_abort_holders(dbenv, sh_obj)
	DB_ENV *dbenv;
	DB_LOCKOBJ *sh_obj;
{
	int ret = 0;
	struct __db_lock *lp, *cntl, *lk;
	locker_info *info = NULL, *infop;
	int infoidx = 0;
	int infomax = 0;
	int free_info = 0;
	int ii;
	DB_LOCKTAB *lt;
	DB_LOCKREGION *region;

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;

	/* Iterate through the locks to grab lockerids */
	for (lp = SH_TAILQ_FIRST(&sh_obj->holders, __db_lock);
	    lp !=NULL; lp = SH_TAILQ_NEXT(lp, links, __db_lock)) {
		DB_LOCKER *lockerp;
		/* Grab the locker */
		lockerp = lp->holderp;

		/* Skip logical holders: these are allowed to block. They won't deadlock. */
		if (F_ISSET(lockerp, DB_LOCKER_LOGICAL)) {
			continue;
		} else {
			F_SET(lockerp, DB_LOCKER_DEADLOCK);
		}

		/* Manage memory */
		if (infoidx >= infomax) {
			int ceiling = 0;

			/* Should never fall into this case twice */
			assert(NULL == info);

			/* Determine maximum possible size */
			for (cntl = lp; cntl != NULL;
			    cntl = SH_TAILQ_NEXT(cntl, links, __db_lock)) {
				ceiling++;
			}

			/* Malloc structure */
			if (ceiling > 8) {
				if ((ret =
					__os_malloc (dbenv,
					    ceiling * sizeof(locker_info),
					    &info))
				    !=0) {
					return (ret);
				}
				free_info = 1;
			}
			/* Alloca structure */
			else {
				info =
				    (locker_info *)alloca(sizeof(locker_info) *
				    ceiling);
			}

			/* Update new maximum */
			infomax = ceiling;
		}

		/* Grab next entry */
		infop = &info[infoidx++];

		/* Grab the locker id */
		infop->last_locker_id = infop->id = lockerp->id;
	}

	/* Unlock the object's partition */
	unlock_obj_partition(region, sh_obj->partition);

	for (ii = 0; ii < infoidx; ii++) {
		DB_LOCKER *lockerp;
		u_int32_t ndx;

		infop = &info[ii];
		LOCKER_INDX(lt, region, infop->id, ndx);

		/* Bad rcode is okay */
		if ((ret = __lock_getlocker(lt, infop->id, ndx, 0,
			    GETLOCKER_KEEP_PART, &lockerp))!=0) {
			continue;
		}

		/* Null (won't have the partition lock) */
		if (lockerp == NULL) {
			continue;
		}

		/* Deleted locker */
		if (F_ISSET(lockerp, DB_LOCKER_DELETED)) {
			unlock_locker_partition(region, lockerp->partition);

			continue;
		}

		/* Sanity */
		if (lockerp->id %gbl_lkr_parts != lockerp->partition) {
			abort();
		}

		/* Skip logical holders: these are allowed to block. They won't deadlock. */
		if (F_ISSET(lockerp, DB_LOCKER_LOGICAL)) {
			unlock_locker_partition(region, lockerp->partition);

			continue;
		} else {
			F_SET(lockerp, DB_LOCKER_DEADLOCK);
		}

		/* Grab the only thing this could possibly be blocked on. */
		lk = SH_LIST_FIRST(&lockerp->heldby, __db_lock);

		/* Continue if this isn't blocked on a lock */
		if (NULL == lk ||lk->status !=DB_LSTAT_WAITING) {
			unlock_locker_partition(region, lockerp->partition);

			continue;
		}

		/* Last lock */
		infop->last_lock = R_OFFSET(&lt->reginfo, lk);

		/* Last obj */
		infop->last_obj = lk->lockobj;

		/* Unlock partition */
		unlock_locker_partition(region, lockerp->partition);

		/* Abort the holder */
		if ((ret = __dd_abort(dbenv, infop))!=0) {
			if (ret == DB_ALREADY_ABORTED) {
				ret = 0;
			} else {
				/* I want this core */
				__db_err(dbenv,
				    "dd_abort_holders: got %d from __dd_abort lockerid %d",
				    ret, infop->id);
				abort();
			}
		}
	}

    /* Free info if I allocated it */
	if (info && free_info) {
		__os_free (dbenv, info);
	}

	return ret;
}

/*
 * We need this so that we can force threads on the replicant to release the 
 * bdb lock.  As with dd_abort_holders, the sh_obj partition is locked when 
 * this is called. The holder will be the replication stream.
 *
 * PUBLIC: int __dd_abort_waiters __P((DB_ENV *, DB_LOCKOBJ *));
 */
int
__dd_abort_waiters(dbenv, sh_obj)
	DB_ENV *dbenv;
	DB_LOCKOBJ *sh_obj;
{
	int ret = 0, ii;
	struct __db_lock *lp, *lk, *cntl;
	locker_info *info = NULL, *infop;
	int infoidx = 0;
	int infomax = 0;
	int free_info = 0;
	DB_LOCKOBJ *lo;
	DB_LOCKTAB *lt;
	DB_LOCKREGION *region;

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;

	/* Iterate through each of the waiters */
	for (lp = SH_TAILQ_FIRST(&sh_obj->waiters, __db_lock);
	    lp !=NULL; lp = SH_TAILQ_NEXT(lp, links, __db_lock)) {
		DB_LOCKER *lockerp;

		/* Grab the locker */
		lockerp = lp->holderp;

		/* Logical waiters: there should only be one, the rep-thread, and it should
		 * be the one which is running this code (so its not waiting).  
		 *
		 * XXX 
		 * This is possible if I'm running the code from something other than the
		 * replication thread (which shouldn't happen), or if maybe I temporarily 
		 * have two replication threads.
		 */
		if (F_ISSET(lockerp, DB_LOCKER_LOGICAL)) {
			__db_err(dbenv,
			    "dd_abort_waiters: logical locker on waitlist?\n");
			abort();
		}

		/* Memory management */
		if (infoidx >= infomax) {
			int ceiling = 0;

			/* Should never fall into this case twice */
			assert(NULL == info);

			for (cntl = lp; cntl != NULL;
			    cntl = SH_TAILQ_NEXT(cntl, links, __db_lock)) {
				ceiling++;
			}

			/* Malloc structure */
			if (ceiling > 8) {
				if ((ret =
					__os_malloc (dbenv,
					    ceiling * sizeof(locker_info),
					    &info))
				    !=0) {
					goto out;
				}
				free_info = 1;
			}
			/* Alloca structure */
			else {
				info =
				    (locker_info *)alloca(sizeof(locker_info) *
				    ceiling);
			}

			/* Update new maximum */
			infomax = ceiling;
		}

		/* Grab next entry */
		infop = &info[infoidx++];

		/* Grab the locker id */
		infop->last_locker_id = infop->id = lockerp->id;
	}

	/* Release obj partition  */
	unlock_obj_partition(region, sh_obj->partition);

	for (ii = 0; ii < infoidx; ii++) {
		DB_LOCKER *lockerp;
		u_int32_t ndx;

		infop = &info[ii];
		LOCKER_INDX(lt, region, infop->id, ndx);

		/* Bad rcode is okay */
		if ((ret = __lock_getlocker(lt, infop->id, ndx, 0,
			    GETLOCKER_KEEP_PART, &lockerp))!=0) {
			continue;
		}

		/* Null (won't have the partition lock) */
		if (lockerp == NULL) {
			continue;
		}

		/* Deleted locker */
		if (F_ISSET(lockerp, DB_LOCKER_DELETED)) {
			unlock_locker_partition(region, lockerp->partition);

			continue;
		}

		/* Sanity */
		if (lockerp->id %gbl_lkr_parts != lockerp->partition) {
			__db_err(dbenv, "dd_abort_waiters: partition error?\n");

			abort();
		}

		/* Grab this lock */
		lk = SH_LIST_FIRST(&lockerp->heldby, __db_lock);

		/* This must be blocked (it can't have been promoted) */
		if (NULL == lk ||lk->status !=DB_LSTAT_WAITING) {
			__db_err(dbenv,
			    "dd_abort_waiters: lock on waitlist changed status?\n");
			abort();
		}

		/* Get the object */
		lo = lk->lockobj;

		/* Should be the same as the sh_obj - I think this can happen 
		 * legitimately, so print a message and continue. */
		if (lo != sh_obj) {
			unlock_locker_partition(region, lockerp->partition);
			logmsg(LOGMSG_ERROR, "dd_abort_waiters: lock-obj mismatch?\n");
			continue;
		}

		/* Grab the last lock */
		infop->last_lock = R_OFFSET(&lt->reginfo, lk);

		/* Grab the last obj */
		infop->last_obj = lk->lockobj;

		/* Unlock this locker partition */
		unlock_locker_partition(region, lockerp->partition);


		/* Abort waiter */
		if ((ret = __dd_abort(dbenv, infop))!=0) {
			if (ret == DB_ALREADY_ABORTED) {
				ret = 0;
			} else {
				/* I want this core */
				__db_err(dbenv,
				    "dd_abort_waiters: got %d from __dd_abort lockerid %d",
				    ret, infop->id);
				abort();
			}
		}
	}

out:
	    /* Free info if I allocated it */
	if (info && free_info) {
		__os_free (dbenv, info);
	}

	return ret;
}


void
berkdb_register_deadlock_callback(void (*callback) (struct berkdb_deadlock_info
	*))
{
	berkdb_deadlock_callback = callback;
}
