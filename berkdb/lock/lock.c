/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: lock.c,v 11.134 2003/11/18 21:30:38 ubell Exp $";
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
#include "dbinc/log.h"

#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

#include "dbinc/db_swap.h"

#ifndef TESTSUITE
#include <thread_util.h>
#include <cheapstack.h>
#include <sys/types.h>
#endif

#include <stackutil.h>
#include "dbinc/txn.h"
#include "logmsg.h"
#include "util.h"
#include "locks_wrap.h"
#include "thread_stats.h"
#include "tohex.h"
#include "txn_properties.h"


#ifdef TRACE_ON_ADDING_LOCKS
// no trace on adding resource the first time
#define PRINTF(res, ...) if (region->res[partition] > 1) printf(__VA_ARGS__)
#else
#define PRINTF(...)
#endif



extern int verbose_deadlocks;
extern int gbl_rowlocks;
extern int gbl_page_latches;
extern int gbl_replicant_latches;
extern int gbl_print_deadlock_cycles;
extern int gbl_lock_conflict_trace;

int gbl_berkdb_track_locks = 0;
unsigned gbl_ddlk = 0;

void comdb2_dump_blocker(unsigned int);
extern void comdb2_cheapstack_sym(FILE *f, char *fmt, ...);

void (*gbl_bb_log_lock_waits_fn) (const void *, size_t sz, int waitms) = NULL;

static int __lock_freelock __P((DB_LOCKTAB *,
	struct __db_lock *, DB_LOCKER *, u_int32_t));
static void __lock_expires __P((DB_ENV *, db_timeval_t *, db_timeout_t));
static void __lock_freelocker
__P((DB_LOCKTAB *, DB_LOCKREGION *, DB_LOCKER *, u_int32_t));
static int __lock_get_internal
__P((DB_LOCKTAB *, u_int32_t, DB_LOCKER *,
	u_int32_t, const DBT *, db_lockmode_t, db_timeout_t, DB_LOCK *));
static int __lock_getobj
__P((DB_LOCKTAB *, const DBT *, u_int32_t, u_int32_t,
	int, DB_LOCKOBJ **));
static int __lock_inherit_locks __P((DB_LOCKTAB *, u_int32_t, u_int32_t));
static int __lock_is_parent __P((DB_LOCKTAB *, u_int32_t, DB_LOCKER *));
static int __lock_put_internal
__P((DB_LOCKTAB *,
	struct __db_lock *, DB_LOCK * lock, u_int32_t, u_int32_t *, u_int32_t));
static int __lock_put_nolock __P((DB_ENV *, DB_LOCK *, u_int32_t *, u_int32_t));
static void __lock_remove_waiter __P((DB_LOCKTAB *,
	DB_LOCKOBJ *, struct __db_lock *, db_status_t));
static int __lock_set_timeout_internal
__P((DB_ENV *,
	u_int32_t, db_timeout_t, u_int32_t));
static int __lock_trade __P((DB_ENV *, DB_LOCK *, u_int32_t, u_int32_t));
static int __lock_sort_cmp __P((const void *, const void *));
static int __locklsn_sort_cmp __P((const void *, const void *));
static int __rowlock_sort_cmp __P((const void *, const void *));
static int __lock_fix_list __P((DB_ENV *, DBT *, u_int32_t, u_int8_t));

static const char __db_lock_err[] = "Lock table is out of available %s";
static const char __db_lock_invalid[] = "%s: Lock is no longer valid";
static const char __db_locker_invalid[] = "Locker is not valid";

int __lock_to_dbt_pp(DB_ENV *, DB_LOCK *, DBT *);

#ifdef DEBUG_LOCKS
extern void bdb_describe_lock_dbt(DB_ENV *dbenv, DBT *dbt, char *out,
    int outlen);
#define LKBUFMAX 10000
#define LKBUFSZ 150
static char lkbuffer[LKBUFMAX][LKBUFSZ];
static int threadid[LKBUFMAX];
static int lbcounter = 0;
static pthread_mutex_t lblk = PTHREAD_MUTEX_INITIALIZER;
#endif

pthread_key_t lockmgr_key;

static int __lock_getlocker_with_prop( DB_LOCKTAB *lt, u_int32_t locker,
    u_int32_t indx, struct txn_properties *prop, u_int32_t flags, DB_LOCKER **retp);

static int __lock_getlocker_int(DB_LOCKTAB *, u_int32_t locker, u_int32_t indx,
    u_int32_t partition, int create, struct txn_properties *prop, DB_LOCKER **retp,
    int *created, int is_logical);

void
berkdb_dump_locks_for_locker(dbenv, id)
	DB_ENV *dbenv;
	u_int32_t id;
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	DB_LOCKER *locker;

	int ndx;
	LOCKER_INDX(lt, region, id, ndx);
	if (__lock_getlocker(lt, id, ndx, GETLOCKER_KEEP_PART, &locker) != 0
	    || locker == NULL)
		return;
	__lock_dump_locker_int(dbenv->lk_handle, locker, stdout, 1);
	unlock_locker_partition(region, locker->partition);
}

/*
 * __lock_id_pp --
 *	DB_ENV->lock_id pre/post processing.
 *
 * PUBLIC: int __lock_id_pp __P((DB_ENV *, u_int32_t *));
 */
int
__lock_id_pp(dbenv, idp)
	DB_ENV *dbenv;
	u_int32_t *idp;
{
	return __lock_id_flags_pp(dbenv, idp, 0);
}

/*
 * __lock_id_flags_pp --
 *	DB_ENV->lock_id_flags pre/post processing.
 *
 * PUBLIC: int __lock_id_flags_pp __P((DB_ENV *, u_int32_t *, u_int32_t));
 */
int
__lock_id_flags_pp(dbenv, idp, flags)
	DB_ENV *dbenv;
	u_int32_t *idp;
	u_int32_t flags;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "DB_ENV->lock_id", DB_INIT_LOCK);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __lock_id_flags(dbenv, idp, flags);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

// PUBLIC: int __lock_id __P((DB_ENV *, u_int32_t *));
int
__lock_id(dbenv, idp)
	DB_ENV *dbenv;
	u_int32_t *idp;
{
	return __lock_id_flags(dbenv, idp, 0);
}


/*
 * __lock_id_flags --
 *	DB_ENV->lock_id_flags.
 *
 * PUBLIC: int  __lock_id_flags __P((DB_ENV *, u_int32_t *, u_int32_t));
 */
int
__lock_id_flags(dbenv, idp, flags)
	DB_ENV *dbenv;
	u_int32_t *idp;
	u_int32_t flags;
{
	DB_LOCKER *lk;
	DB_LOCKTAB *lt;
	DB_LOCKREGION *region;
	u_int32_t *ids, locker_ndx;
	int nids, ret;

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;
	ret = 0;

	/*
	 * Allocate a new lock id.  If we wrap around then we
	 * find the minimum currently in use and make sure we
	 * can stay below that.  This code is similar to code
	 * in __txn_begin_int for recovering txn ids.
	 */
	LOCKREGION(dbenv, lt);
	lock_lockers(region);

	/*
	 * Our current valid range can span the maximum valid value, so check
	 * for it and wrap manually.
	 */
	if (region->stat.st_id == DB_LOCK_MAXID) {
		region->stat.st_id = DB_LOCK_INVALIDID;
	}
	if ((region->stat.st_id + 1) == region->stat.st_cur_maxid) {
		if ((ret = __os_malloc(dbenv,
			sizeof(u_int32_t) * region->stat.st_nlockers, &ids)) != 0) {
			unlock_lockers(region);
			return (ret);
		}
		nids = 0;
		for (lk = SH_TAILQ_FIRST(&region->lockers, __db_locker);
			lk != NULL;
			lk = SH_TAILQ_NEXT(lk, ulinks, __db_locker))
			if (lk->id < TXN_MINIMUM) {
				ids[nids++] = lk->id;
			}
		region->stat.st_id = DB_LOCK_INVALIDID;
		region->stat.st_cur_maxid = DB_LOCK_MAXID;
		if (nids != 0)
			__db_idspace(ids, nids,
				&region->stat.st_id, &region->stat.st_cur_maxid);
		__os_free(dbenv, ids);
	}
	*idp = ++region->stat.st_id;

	/* Allocate a locker for this id. */
	LOCKER_INDX(lt, region, *idp, locker_ndx);
	u_int32_t glflags = GETLOCKER_CREATE | GETLOCKER_DONT_LOCK;
	ret = __lock_getlocker(lt, *idp, locker_ndx, glflags, &lk);

	if (!ret) {
		F_CLR(lk, DB_LOCK_ID_TRACK | DB_LOCKER_READONLY | DB_LOCKER_KILLME);

		if (LF_ISSET(DB_LOCK_ID_LOWPRI) || pthread_getspecific(lockmgr_key))
			F_SET(lk, DB_LOCKER_KILLME);

		if (LF_ISSET(DB_LOCK_ID_READONLY))
			F_SET(lk, DB_LOCKER_READONLY);

		if (LF_ISSET(DB_LOCK_ID_TRACK))
			F_SET(lk, DB_LOCKER_TRACK);
	}

	UNLOCKREGION(dbenv, lt);
	unlock_lockers(region);
	return (ret);
}

int
__lock_id_set_logical_abort(dbenv, id)
	DB_ENV *dbenv;
	u_int32_t id;
{
	DB_LOCKER *sh_locker;
	DB_LOCKTAB *lt;
	DB_LOCKREGION *region;
	u_int32_t locker_ndx;
	int ret;

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;

	LOCKREGION(dbenv, lt);
	LOCKER_INDX(lt, region, id, locker_ndx);
	if ((ret =
	    __lock_getlocker(lt, id, locker_ndx, GETLOCKER_KEEP_PART,
	    &sh_locker)) != 0) {
		return ret;
	}
	if (sh_locker == NULL) {
		return EINVAL;
	}

	F_SET(sh_locker, DB_LOCKER_IN_LOGICAL_ABORT);

	unlock_locker_partition(region, sh_locker->partition);
	UNLOCKREGION(dbenv, lt);
	return (0);
}

// PUBLIC: int __lock_id_set_logical_abort_pp __P((DB_ENV *, u_int32_t));
int
__lock_id_set_logical_abort_pp(dbenv, id)
	DB_ENV *dbenv;
	u_int32_t id;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "DB_ENV->lock_id_set_logical_abort",
	    DB_INIT_LOCK);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);

	ret = __lock_id_set_logical_abort(dbenv, id);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

int
__lock_id_has_waiters(dbenv, id)
	DB_ENV *dbenv;
	u_int32_t id;
{
	DB_LOCKER *sh_locker;
	DB_LOCKTAB *lt;
	DB_LOCKREGION *region;
	u_int32_t locker_ndx;
	int ret;

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;

	LOCKREGION(dbenv, lt);
	LOCKER_INDX(lt, region, id, locker_ndx);
	if ((ret =
		__lock_getlocker(lt, id, locker_ndx, GETLOCKER_KEEP_PART,
		    &sh_locker)) != 0) {
		return ret;
	}
	if (sh_locker == NULL) {
		return EINVAL;
	}

	if (sh_locker->has_waiters)
		ret = 1;
	else
		ret = 0;

	unlock_locker_partition(region, sh_locker->partition);
	UNLOCKREGION(dbenv, lt);
	return (ret);
}


// PUBLIC: int __lock_id_has_waiters_pp __P((DB_ENV *, u_int32_t));
int
__lock_id_has_waiters_pp(dbenv, id)
	DB_ENV *dbenv;
	u_int32_t id;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "DB_ENV->lock_id_has_waiters", DB_INIT_LOCK);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);

	ret = __lock_id_has_waiters(dbenv, id);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __lock_id_free_pp --
 *	DB_ENV->lock_id_free pre/post processing.
 *
 * PUBLIC: int __lock_id_free_pp __P((DB_ENV *, u_int32_t));
 */
int
__lock_id_free_pp(dbenv, id)
	DB_ENV *dbenv;
	u_int32_t id;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "DB_ENV->lock_id_free", DB_INIT_LOCK);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __lock_id_free(dbenv, id);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

static int
__allocate_ilock_latch(dbenv, lnode)
	DB_ENV *dbenv;
	DB_ILOCK_LATCH **lnode;
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	int ret, i;
	Pthread_mutex_lock(&region->ilock_latch_lk);
	if (!region->ilock_latch_head) {
		DB_ILOCK_LATCH *mem;
		if ((ret = __os_calloc(dbenv, sizeof(DB_ILOCK_LATCH),
			    region->ilock_step, &mem)) != 0)
			abort();
		for (i = 0; i < region->ilock_step - 1; i++) {
			Pthread_mutex_init(&mem[i].lsns_mtx, NULL);
			mem[i].next = &mem[i + 1];
		}
		mem[region->ilock_step - 1].next = NULL;
		Pthread_mutex_init(&mem[region->ilock_step - 1].lsns_mtx, NULL);
		region->ilock_latch_head = mem;
	}
	*lnode = region->ilock_latch_head;
	region->ilock_latch_head = region->ilock_latch_head->next;
	Pthread_mutex_unlock(&region->ilock_latch_lk);
	assert(!(*lnode)->count);
	return 0;
}

static int
__allocate_db_lock_lsn(dbenv, lsnp)
	DB_ENV *dbenv;
	struct __db_lock_lsn **lsnp;
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	struct __db_lock_lsn *l;
	int ret, i;

	Pthread_mutex_lock(&region->db_lock_lsn_lk);

	if (!(l = SH_LIST_FIRST(&region->db_lock_lsn_head, __db_lock_lsn))) {
		struct __db_lock_lsn *mem;
		u_int32_t step = region->db_lock_lsn_step;
		if ((ret = __os_calloc(dbenv, sizeof(struct __db_lock_lsn),
			    step, &mem)) != 0)
			abort();
		for (i = step - 1; i > 0; i--)
			SH_LIST_INSERT_HEAD(&region->db_lock_lsn_head, &mem[i],
			    lsn_links, __db_lock_lsn);
		l = &mem[0];
	} else
		SH_LIST_REMOVE(l, lsn_links, __db_lock_lsn);

	Pthread_mutex_unlock(&region->db_lock_lsn_lk);
	(*lsnp) = l;
	return 0;
}

static void
__deallocate_db_lock_lsn(dbenv, lsnp)
	DB_ENV *dbenv;
	struct __db_lock_lsn *lsnp;
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;

	Pthread_mutex_lock(&region->db_lock_lsn_lk);
	SH_LIST_INSERT_HEAD(&region->db_lock_lsn_head, lsnp, lsn_links,
	    __db_lock_lsn);
	Pthread_mutex_unlock(&region->db_lock_lsn_lk);
}

static void
__deallocate_ilock_latch(dbenv, lnode)
	DB_ENV *dbenv;
	DB_ILOCK_LATCH *lnode;
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	struct __db_lock_lsn *lp_lsn, *next_lsn;

	lnode->lock_mode = 0;
	Pthread_mutex_lock(&lnode->lsns_mtx);
	for (lp_lsn = SH_LIST_FIRST(&lnode->lsns, __db_lock_lsn);
	    lp_lsn != NULL; lp_lsn = next_lsn) {
		next_lsn = SH_LIST_NEXT(lp_lsn, lsn_links, __db_lock_lsn);
		SH_LIST_REMOVE(lp_lsn, lsn_links, __db_lock_lsn);
		__deallocate_db_lock_lsn(dbenv, lp_lsn);
	}
	lnode->nlsns = 0;
	Pthread_mutex_unlock(&lnode->lsns_mtx);

	Pthread_mutex_lock(&region->ilock_latch_lk);
	lnode->prev = NULL;
	lnode->next = region->ilock_latch_head;
	region->ilock_latch_head = lnode;
	Pthread_mutex_unlock(&region->ilock_latch_lk);
}

#ifdef DEBUG_LOCKERID_LNODE_LEAK
static uint64_t lockerid_lnode_allocated = 0;
static uint64_t lockerid_lnode_inuse = 0;
#endif

static int
__allocate_lockerid_lnode(dbenv, lidnode)
	DB_ENV *dbenv;
	DB_LOCKERID_LATCH_NODE **lidnode;
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	int ret, i;

	Pthread_mutex_lock(&region->lockerid_node_lk);
	if (!region->lockerid_node_head) {
		DB_LOCKERID_LATCH_NODE *mem;
		if ((ret = __os_calloc(dbenv, sizeof(DB_LOCKERID_LATCH_NODE),
		    region->lockerid_node_step, &mem)) != 0)
			abort();
		for (i = 0; i < region->lockerid_node_step - 1; i++)
			mem[i].next = &mem[i + 1];
		mem[region->lockerid_node_step - 1].next = NULL;
		region->lockerid_node_head = mem;
#ifdef DEBUG_LOCKERID_LNODE_LEAK
		lockerid_lnode_allocated += region->lockerid_node_step;
		static time_t lastpr = 0;
		time_t now;
		if ((now = time(NULL)) > lastpr) {
			fprintf(stderr,
			    "Allocated %llu lockerid_lnodes, %llu in use\n",
			    lockerid_lnode_allocated, lockerid_lnode_inuse);
			lastpr = now;
		}
#endif
	}
	*lidnode = region->lockerid_node_head;
	region->lockerid_node_head = region->lockerid_node_head->next;
#ifdef DEBUG_LOCKERID_LNODE_LEAK
	lockerid_lnode_inuse++;
#endif
	Pthread_mutex_unlock(&region->lockerid_node_lk);
	return 0;
}

static void
__deallocate_lockerid_node(dbenv, lidnode)
	DB_ENV *dbenv;
	DB_LOCKERID_LATCH_NODE *lidnode;
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	Pthread_mutex_lock(&region->lockerid_node_lk);
	lidnode->next = region->lockerid_node_head;
	region->lockerid_node_head = lidnode;
#ifdef DEBUG_LOCKERID_LNODE_LEAK
	lockerid_lnode_inuse--;
#endif
	Pthread_mutex_unlock(&region->lockerid_node_lk);
}

static inline u_int32_t
latchhash(DB_ENV *dbenv, const DBT *obj)
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	DB_LOCK_ILOCK *ilock = (DB_LOCK_ILOCK *)obj->data;
	u_int32_t h = 0x01000193, *f = (u_int32_t *)ilock->fileid;
	h ^= f[0]; h ^= f[1]; h ^= f[2]; h ^= f[3]; h ^= f[4];
	h += ilock->pgno;
	return (h % region->max_latch);
}

static inline u_int32_t
lockeridhash(DB_ENV *dbenv, u_int32_t lockerid)
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	return lockerid % region->max_latch_lockerid;
}

static int
__free_latch_lockerid(dbenv, lockerid)
	DB_ENV *dbenv;
	u_int32_t lockerid;
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	DB_LOCKERID_LATCH_LIST *lockerid_latches = region->lockerid_latches;
	DB_LOCKERID_LATCH_NODE *lidptr;
	int idx;

	if (!gbl_page_latches)
		return 0;

	idx = lockeridhash(dbenv, lockerid);
	Pthread_mutex_lock(&lockerid_latches[idx].lock);

	lidptr = lockerid_latches[idx].head;

	while (lidptr && lidptr->lockerid != lockerid)
		lidptr = lidptr->next;

	if (lidptr) {
		assert(!lidptr->head);
		if (lidptr->next)
			lidptr->next->prev = lidptr->prev;
		if (lidptr->prev)
			lidptr->prev->next = lidptr->next;
		if (lidptr == lockerid_latches[idx].head)
			lockerid_latches[idx].head = lidptr->next;
		if (lidptr->tracked_locklist)
			__os_free(dbenv, lidptr->tracked_locklist);
		lidptr->tracked_locklist = NULL;
		lidptr->maxtrackedlocks = 0;
		lidptr->ntrackedlocks = 0;
		lidptr->has_pglk_lsn = 0;
		lidptr->flags = 0;
		__deallocate_lockerid_node(dbenv, lidptr);
#ifdef VERBOSE_LATCHES
		printf("%s - freeing latch-lockerid %u\n", __func__, lockerid);
#endif



	}
#ifdef VERBOSE_LATCHES
	else {
		printf("%s - couldn't find latch-lockerid %u\n", __func__,
		    lockerid);
	}
#endif

	Pthread_mutex_unlock(&lockerid_latches[idx].lock);

	return 0;
}

/*
 * __lock_id_free --
 *	Free a locker id.
 *
 * PUBLIC: int  __lock_id_free __P((DB_ENV *, u_int32_t));
 */
int
__lock_id_free(dbenv, id)
	DB_ENV *dbenv;
	u_int32_t id;
{
	DB_LOCKER *sh_locker;
	DB_LOCKTAB *lt;
	DB_LOCKREGION *region;
	u_int32_t locker_ndx, partition;
	int ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "DB_ENV->lock_id_free", DB_INIT_LOCK);

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;

	__free_latch_lockerid(dbenv, id);

	LOCKREGION(dbenv, lt);
	lock_lockers(region);
	LOCKER_INDX(lt, region, id, locker_ndx);
	if ((ret =
		__lock_getlocker(lt, id, locker_ndx, GETLOCKER_KEEP_PART,
		    &sh_locker)) != 0) {
		unlock_lockers(region);
		return ret;
	}
	if (sh_locker == NULL) {
		unlock_lockers(region);
		return EINVAL;
	}
	partition = sh_locker->partition;

	if (sh_locker->nlocks != 0) {
		unlock_locker_partition(region, partition);
		unlock_lockers(region);
		logmsg(LOGMSG_ERROR, "locker %x, still has %d locks\n", (int)id,
		    (int)sh_locker->nlocks);
		__db_err(dbenv, "Locker still has locks");
		berkdb_dump_locks_for_locker(dbenv, id);
		return EINVAL;
	}

	__lock_freelocker(lt, region, sh_locker, locker_ndx);

	unlock_locker_partition(region, partition);
	unlock_lockers(region);
	UNLOCKREGION(dbenv, lt);
	return (ret);
}

/*
 * __lock_vec_pp --
 *	DB_ENV->lock_vec pre/post processing.
 *
 * PUBLIC: int __lock_vec_pp __P((DB_ENV *,
 * PUBLIC:     u_int32_t, u_int32_t, DB_LOCKREQ *, int, DB_LOCKREQ **));
 */
int
__lock_vec_pp(dbenv, locker, flags, list, nlist, elistp)
	DB_ENV *dbenv;
	u_int32_t locker, flags;
	int nlist;
	DB_LOCKREQ *list, **elistp;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "DB_ENV->lock_vec", DB_INIT_LOCK);

	/* Validate arguments. */
	if ((ret = __db_fchk(dbenv,
		    "DB_ENV->lock_vec", flags,
		    DB_LOCK_NOWAIT | DB_LOCK_LOGICAL | DB_LOCK_NOPAGELK)) != 0)
		return (ret);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __lock_vec(dbenv, locker, flags, list, nlist, elistp);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/* XXX Macro-ize after I know this works */
static inline int
is_pagelock(lockobj)
	DB_LOCKOBJ *lockobj;
{
	if (lockobj->lockobj.size != sizeof(struct __db_ilock))
		return 0;
	struct __db_ilock *ilock = lockobj->lockobj.data;
	return (ilock->type == DB_PAGE_LOCK);
}

static inline int
is_handlelock(lockobj)
	DB_LOCKOBJ *lockobj;
{
	if (lockobj->lockobj.size != sizeof(struct __db_ilock))
		return 0;
	struct __db_ilock *ilock = lockobj->lockobj.data;
	return (ilock->type == DB_HANDLE_LOCK);
}


int
use_page_latches(dbenv)
	DB_ENV *dbenv;
{
	return (gbl_rowlocks && gbl_page_latches && IS_ENV_REPLICATED(dbenv) &&
	    (!IS_REP_CLIENT(dbenv) || gbl_replicant_latches));
}

int
init_latches(dbenv, lt)
	DB_ENV *dbenv;
	DB_LOCKTAB *lt;
{
	DB_LOCKREGION *region = lt->reginfo.primary;
	u_int32_t i;
	int ret;
	pthread_mutexattr_t attr;

	if (!gbl_page_latches)
		return 0;

	region->max_latch = dbenv->attr.max_latch;
	region->max_latch_lockerid = dbenv->attr.max_latch_lockerid;
	region->lockerid_node_step = dbenv->attr.lockerid_node_step;
	region->ilock_step = dbenv->attr.ilock_step;
	region->blocking_latches = dbenv->attr.blocking_latches;

	pthread_mutexattr_init(&attr);

	if ((ret = __os_calloc(dbenv, region->max_latch, sizeof(DB_LATCH),
		    &(region->latches))) != 0)
		abort();

	if ((ret = __os_calloc(dbenv, region->max_latch_lockerid,
		    sizeof(DB_LOCKERID_LATCH_LIST),
		    &region->lockerid_latches)) != 0)
		abort();

	for (i = 0; i < region->max_latch; i++)
		Pthread_mutex_init(&region->latches[i].lock, &attr);

	for (i = 0; i < region->max_latch_lockerid; i++)
		Pthread_mutex_init(&region->lockerid_latches[i].lock, NULL);

	Pthread_mutex_init(&region->ilock_latch_lk, NULL);
	region->ilock_latch_head = NULL;
	Pthread_mutex_init(&region->lockerid_node_lk, NULL);
	region->lockerid_node_head = NULL;
	return 0;
}

int __get_lockerid_from_lock(DB_ENV *dbenv, u_int32_t locker)
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	if (!lt)
		return 0;
	struct __db_lock *lp = (struct __db_lock *)R_ADDR(&lt->reginfo, locker);
	if (lp && lp->holderp)
		return lp->holderp->id;
	return 0;
}

static inline int
__find_latch_lockerid(DB_ENV *dbenv, u_int32_t locker,
    DB_LOCKERID_LATCH_NODE ** rlid, u_int32_t flags)
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	DB_LOCKERID_LATCH_LIST *lockerid_latches = region->lockerid_latches;
	DB_LOCKERID_LATCH_NODE *lid;
	int idx, ret;

	idx = lockeridhash(dbenv, locker);

	Pthread_mutex_lock(&(lockerid_latches[idx].lock));
	lid = lockerid_latches[idx].head;
	while (lid && lid->lockerid != locker)
		lid = lid->next;

	if (!lid && LF_ISSET(GETLOCKER_CREATE)) {
		if ((ret = __allocate_lockerid_lnode(dbenv, &lid)))
			abort();
		lid->lockerid = locker;
		lid->prev = NULL;
		lid->next = lockerid_latches[idx].head;
		lid->head = NULL;
		if (lockerid_latches[idx].head)
			lockerid_latches[idx].head->prev = lid;
		lockerid_latches[idx].head = lid;
	}
	Pthread_mutex_unlock(&lockerid_latches[idx].lock);
	*rlid = lid;
	return lid ? 0 : -1;
}


static u_int32_t
__count_latches(dbenv, lidptr)
	DB_ENV *dbenv;
	DB_LOCKERID_LATCH_NODE *lidptr;
{
	u_int32_t count = 0;
	DB_LATCH *latch;

	latch = lidptr->head;
	while (latch) {
		count += latch->count;
		latch = latch->next;
	}

#ifdef VERBOSE_LATCH
	printf("%s returns %u for lid %u\n", __func__, count, lidptr->lockerid);
#endif

	return count;
}


static u_int32_t
__count_write_latches(dbenv, lidptr)
	DB_ENV *dbenv;
	DB_LOCKERID_LATCH_NODE *lidptr;
{
	u_int32_t count = 0;
	DB_LATCH *latch;

	latch = lidptr->head;
	while (latch) {
		count += latch->wrcount;
		latch = latch->next;
	}

#ifdef VERBOSE_LATCH
	printf("%s returns %u for lid %u\n", __func__, count, lidptr->lockerid);
#endif

	return count;
}

int bdb_update_txn_pglogs(void *bdb_state, void *pglogs_hashtbl,
    pthread_mutex_t * mutexp, db_pgno_t pgno, unsigned char *fileid,
    DB_LSN lsn);

static int
__latch_update_tracked_writelocks_lsn(DB_ENV *dbenv, DB_TXN *txnp,
    u_int32_t lockerid, DB_LSN lsn)
{
	int ret;
	u_int32_t i;
	DB_LOCKERID_LATCH_NODE *lidptr;
	DB_ILOCK_LATCH *ilatch;
	DB_LOCK_ILOCK *ilock;
	struct __db_lock_lsn *lsnp;
	struct __db_lock_lsn *first_lsnp;

	if ((ret = __find_latch_lockerid(dbenv, lockerid, &lidptr, 0)) != 0)
		abort();

	if (!F_ISSET(lidptr, DB_LOCKER_TRACK_WRITELOCKS)) {
		logmsg(LOGMSG_FATAL, "%s: BUG! Writelocks for lsn[%u][%u] were not tracked\n",
			__func__, lsn.file, lsn.offset);
		abort();
	}

	F_CLR(lidptr, DB_LOCKER_TRACK_WRITELOCKS);
	lidptr->has_pglk_lsn = 1;

	if (!txnp->pglogs_hashtbl)
		DB_ASSERT(F_ISSET(txnp, TXN_COMPENSATE));

	assert(lidptr->ntrackedlocks != 0);
	for (i = 0; i < lidptr->ntrackedlocks; i++) {
		ilatch = lidptr->tracked_locklist[i];
		if (__allocate_db_lock_lsn(dbenv, &lsnp))
			return ENOMEM;
		lsnp->llsn = lsn;
		Pthread_mutex_lock(&ilatch->lsns_mtx);
		first_lsnp = SH_LIST_FIRST(&ilatch->lsns, __db_lock_lsn);
		if (first_lsnp &&
		    log_compare(&first_lsnp->llsn, &lsnp->llsn) == 0) {
			Pthread_mutex_unlock(&ilatch->lsns_mtx);
			__deallocate_db_lock_lsn(dbenv, lsnp);
		} else {
			SH_LIST_INSERT_HEAD(&ilatch->lsns, lsnp, lsn_links,
			    __db_lock_lsn);
			ilatch->nlsns++;
			Pthread_mutex_unlock(&ilatch->lsns_mtx);
			if (txnp->pglogs_hashtbl) {
				ilock = (DB_LOCK_ILOCK *)ilatch->lock;
				bdb_update_txn_pglogs(dbenv->app_private,
				    txnp->pglogs_hashtbl, &txnp->pglogs_mutex,
				    ilock->pgno, ilock->fileid, lsn);
			}
		}
	}
	return 0;
}

static inline int
__latch_clear_tracked_writelocks(DB_ENV *dbenv, u_int32_t lockerid)
{
	int ret;
	DB_LOCKERID_LATCH_NODE *lidptr;

	if ((ret = __find_latch_lockerid(dbenv, lockerid, &lidptr,
	    GETLOCKER_CREATE)) != 0)
		abort();

	F_SET(lidptr, DB_LOCKER_TRACK_WRITELOCKS);
	if (lidptr->tracked_locklist == NULL) {
		int count = dbenv->attr.tracked_locklist_init > 0 ?
		    dbenv->attr.tracked_locklist_init : 10;
		lidptr->maxtrackedlocks = count;
		if ((ret = __os_malloc(dbenv, lidptr->maxtrackedlocks *
		    sizeof(struct __db_ilock_latch *),
		    &(lidptr->tracked_locklist))) != 0)
			return ENOMEM;
	}
	lidptr->ntrackedlocks = 0;
	return 0;
}

static inline int
__put_page_latch_int(dbenv, lock)
	DB_ENV *dbenv;
	DB_LOCK *lock;
{
	DB_ILOCK_LATCH *lnode;
	DB_LATCH *latch;
	int ret;

	lnode = lock->ilock_latch;
	assert(lnode && (lnode->count > 0));
	lnode->count--;

	if (!lnode->count) {
		latch = lnode->latch;
		if (lnode == latch->listhead)
			latch->listhead = lnode->next;
		if (lnode->prev)
			lnode->prev->next = lnode->next;
		if (lnode->next)
			lnode->next->prev = lnode->prev;

		/* If this is the last held for this latch, remove */
		assert(latch->lockerid);

		latch->count--;
		if (IS_WRITELOCK(lnode->lock_mode))
			latch->wrcount--;

		__deallocate_ilock_latch(dbenv, lnode);

		/* We're going to relinquish this latch */
		if (!latch->listhead) {
			DB_LOCKERID_LATCH_NODE *lidptr;

			assert(!latch->count);
			assert(!latch->wrcount);

			if ((ret = __find_latch_lockerid(dbenv, latch->lockerid,
			    &lidptr, 0)) != 0)
				abort();

			if (latch->prev)
				latch->prev->next = latch->next;
			if (latch->next)
				latch->next->prev = latch->prev;
			if (latch == lidptr->head)
				lidptr->head = latch->next;

			latch->prev = latch->next = NULL;
			latch->tid = 0;
			latch->lockerid = 0;
			latch->locker = NULL;
			Pthread_mutex_unlock(&latch->lock);
		}
	}

	LOCK_INIT(*lock);
	return 0;
}

static inline int
__put_page_latch(dbenv, lock)
	DB_ENV *dbenv;
	DB_LOCK *lock;
{
	int ret;
#ifdef VERBOSE_LATCH
	DB_ILOCK_LATCH *lnode = (DB_ILOCK_LATCH *)lock->ilock_latch;
	DB_LATCH *latch = lnode->latch;
	u_int32_t locker = latch->lockerid;
#endif
	ret = __put_page_latch_int(dbenv, lock);
#ifdef VERBOSE_LATCH
	if (!ret) {
		DB_LOCKERID_LATCH_NODE *lid;
		if ((ret = __find_latch_lockerid(dbenv, locker, &lid, 0)) != 0)
			abort();
		uint32_t count = __count_latches(dbenv, lid);
		printf
		    ("locker %u tid %u put page lock %p at latch %p line %d count %u\n",
		    latch->lockerid, pthread_self(), lnode, latch, __LINE__,
		    count);
	}
#endif
	return ret;
}

static inline int
__get_page_latch_int(lt, locker, flags, obj, lock_mode, lock)
	DB_LOCKTAB *lt;
	u_int32_t locker;
	u_int32_t flags;
	const DBT *obj;
	db_lockmode_t lock_mode;
	DB_LOCK *lock;
{
	DB_LOCKREGION *region = lt->reginfo.primary;
	DB_LATCH *latch = NULL;
	DB_LOCKERID_LATCH_NODE *lidptr = NULL;
	DB_ILOCK_LATCH *lnode;
	DB_ENV *dbenv;
	u_int32_t idx, new_latch = 0;
	int ret, pollcnt = 0;

	dbenv = lt->dbenv;

	/* Check if locks have been globally turned off. */
	if (F_ISSET(dbenv, DB_ENV_NOLOCKING))
		return (0);

	if (LF_ISSET(DB_LOCK_SWITCH))
		abort();

	/* Change the lock mode - for upgrade */
	if (LF_ISSET(DB_LOCK_UPGRADE)) {
		assert(LOCK_ISSET(*lock));
		assert(LOCK_ISLATCH(*lock));
		assert(lock->ilock_latch);
		lnode = (DB_ILOCK_LATCH *)lock->ilock_latch;

		/* XXX hashing here for the assert:  remove after we know this works */
		idx = latchhash(dbenv, obj);
		latch = &region->latches[idx];
		assert(lnode->latch == latch);
		assert(latch->lockerid == locker);
		if (!IS_WRITELOCK(lnode->lock_mode))
			latch->wrcount++;
		lnode->lock_mode = lock_mode;
		return (0);
	}

	idx = latchhash(dbenv, obj);
	latch = &region->latches[idx];

	if (((ret = pthread_mutex_trylock(&latch->lock)) != 0) &&
	    latch->lockerid != locker) {
		if (region->blocking_latches || LF_ISSET(DB_LOCK_ONELOCK)) {
			Pthread_mutex_lock(&latch->lock);
        }
		else {
			if (dbenv->attr.latch_timed_mutex) {
				int latch_max_wait = dbenv->attr.latch_max_wait;
				struct timespec timeout;
				struct timeval now;

				gettimeofday(&now, NULL);
				now.tv_usec += latch_max_wait;
				if (now.tv_usec > 1000000) {
					now.tv_usec -= 1000000;
					now.tv_sec += 1;
				}
				timeout.tv_sec = now.tv_sec;
				timeout.tv_nsec = 1000 * now.tv_usec;
				ret =
#ifdef __APPLE__
				    pthread_mutex_trylock(&latch->lock);
#else
				    pthread_mutex_timedlock(&latch->lock,
				    &timeout);
#endif
			} else {
				int latch_max_poll = dbenv->attr.latch_max_poll;
				int latch_poll_us = dbenv->attr.latch_poll_us;
				while (pollcnt++ < latch_max_poll &&
				    ((ret = pthread_mutex_trylock(&latch->
						lock)) != 0))
					usleep(latch_poll_us);
			}

			if (ret) {
#ifdef VERBOSE_LATCH
				printf("locker %u failed to get latch at %p\n",
				    locker, latch);
#endif
				return (LF_ISSET(DB_LOCK_NOWAIT) ?
				    DB_LOCK_NOTGRANTED : DB_LOCK_DEADLOCK);
			}
		}
	}

	if (!latch->lockerid) {
		new_latch = 1;
		latch->lockerid = locker;
		latch->tid = pthread_self();
	} else {
		assert(latch->locker);
		assert(latch->lockerid == locker);
		assert(latch->locker->lockerid == latch->lockerid);
		assert(latch->tid == pthread_self());
	}

	lnode = latch->listhead;
	while (lnode &&
	    memcmp(lnode->lock, obj->data, sizeof(struct __db_ilock)))
		lnode = lnode->next;

	if (!lnode) {
		if ((ret = __allocate_ilock_latch(dbenv, &lnode)))
			abort();

		memcpy(lnode->lock, obj->data, sizeof(struct __db_ilock));

		lnode->prev = NULL;
		lnode->next = latch->listhead;
		lnode->lock_mode = lock_mode;
		lnode->latch = latch;
		if (latch->listhead)
			latch->listhead->prev = lnode;
		latch->listhead = lnode;
		latch->count++;
		if (IS_WRITELOCK(lock_mode))
			latch->wrcount++;
	} else if (IS_WRITELOCK(lock_mode)) {
		assert(lock_mode == DB_LOCK_WRITE);
		if (!IS_WRITELOCK(lnode->lock_mode))
			latch->wrcount++;
		lnode->lock_mode = lock_mode;
	}

	lnode->count++;
	if (new_latch) {
		if ((ret = __find_latch_lockerid(dbenv, locker, &lidptr,
			    GETLOCKER_CREATE)) != 0)
			abort();
#ifdef VERBOSE_LATCH
		uint32_t count = __count_latches(dbenv, lidptr);
		printf("%s found locker %u for latch %p count %u\n", __func__,
		    locker, latch, count);
#endif
		latch->next = lidptr->head;
		latch->prev = NULL;
		if (lidptr->head)
			lidptr->head->prev = latch;
		lidptr->head = latch;
		latch->locker = lidptr;

#ifdef VERBOSE_LATCH
		count = __count_latches(dbenv, lidptr);
		printf("%s count after relink locker %u latch %p is %u\n",
		    __func__, locker, latch, count);
#endif
	}

	lock->off = LATCH_OFFSET;
	lock->ilock_latch = lnode;
	lock->mode = lock_mode;
	lock->ndx = lock->gen = lock->partition = -1;
	lock->owner = -1;

	if (F_ISSET(latch->locker, DB_LOCKER_TRACK_WRITELOCKS) &&
	    IS_WRITELOCK(lock_mode)) {
		lidptr = latch->locker;
		assert(lidptr->maxtrackedlocks);
		if (!lidptr->ntrackedlocks ||
		    lidptr->tracked_locklist[lidptr->ntrackedlocks - 1] !=
		    lnode) {
			if (lidptr->ntrackedlocks + 1 > lidptr->maxtrackedlocks) {
				lidptr->maxtrackedlocks =
				    lidptr->maxtrackedlocks * 2;
				if ((ret =
					__os_realloc(dbenv,
					    sizeof(DB_ILOCK_LATCH) *
					    lidptr->maxtrackedlocks,
					    &lidptr->tracked_locklist)) != 0)
					return ENOMEM;
			}
			lidptr->tracked_locklist[lidptr->ntrackedlocks++] =
			    lnode;
		}
	}

	return (0);
}

static inline int
__get_page_latch(lt, locker, flags, obj, lock_mode, lock)
	DB_LOCKTAB *lt;
	u_int32_t locker;
	u_int32_t flags;
	const DBT *obj;
	db_lockmode_t lock_mode;
	DB_LOCK *lock;
{
	int ret;
	ret = __get_page_latch_int(lt, locker, flags, obj, lock_mode, lock);
#ifdef VERBOSE_LATCH
	if (!ret) {
		DB_LOCKERID_LATCH_NODE *lid;
		DB_ILOCK_LATCH *lnode = (DB_ILOCK_LATCH *)lock->ilock_latch;
		DB_LATCH *latch = lnode->latch;
		if ((ret =
		    __find_latch_lockerid(lt->dbenv, locker, &lid, 0)) != 0)
			abort();

		uint32_t count = __count_latches(lt->dbenv, lid);
		printf
		    ("locker %u tid %u got page lock %p at latch %p count %u\n",
		    locker, pthread_self(), lnode, latch, count);
	} else {
		printf("locker %u failed to get latch, rc=%d\n", locker, ret);
	}
#endif
	return ret;
}

static inline int
is_tablelock(DB_LOCKOBJ *sh_obj)
{
	return (sh_obj->lockobj.size == 32);
}

/*
 * __lock_vec --
 *	DB_ENV->lock_vec.
 *
 *	Vector lock routine.  This function takes a set of operations
 *	and performs them all at once.  In addition, lock_vec provides
 *	functionality for lock inheritance, releasing all locks for a
 *	given locker (used during transaction commit/abort), releasing
 *	all locks on a given object, and generating debugging information.
 *
 * PUBLIC: int __lock_vec __P((DB_ENV *,
 * PUBLIC:     u_int32_t, u_int32_t, DB_LOCKREQ *, int, DB_LOCKREQ **));
 */
int
__lock_vec(dbenv, locker, flags, list, nlist, elistp)
	DB_ENV *dbenv;
	u_int32_t locker, flags;
	int nlist;
	DB_LOCKREQ *list, **elistp;
{
	struct __db_lock *lp, *next_lock;
	DB_LOCKER *sh_locker;
	DB_LOCKOBJ *sh_obj;
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	DB_LOCKERID_LATCH_NODE *latch_lockerid = NULL;
	DB_LATCH *latch, *nextlatch;
	DB_ILOCK_LATCH *lnode, *nextlnode;
	DBT *objlist, *np;
	struct __db_lockobj_lsn *lklsnp;
	u_int32_t lndx, ndx;
	u_int32_t nwrites = 0, nwritelatches = 0, countwl = 0, counttot = 0, ntable = 0;
	u_int32_t partition;
	u_int32_t run_dd;
	int i, ret, rc, upgrade, writes, prepare, has_pglk_lsn = 0;

	/* Check if locks have been globally turned off. */
	if (F_ISSET(dbenv, DB_ENV_NOLOCKING))
		return (0);

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;

	run_dd = 0;
	LOCKREGION(dbenv, (DB_LOCKTAB *)dbenv->lk_handle);
	for (i = 0, ret = 0; i < nlist && ret == 0; i++) {
		switch (list[i].op) {
		case DB_LOCK_GET_TIMEOUT:
			LF_SET(DB_LOCK_SET_TIMEOUT);
		case DB_LOCK_GET:
			ret = __lock_get_internal(dbenv->lk_handle,
			    locker, NULL, flags, list[i].obj,
			    list[i].mode, list[i].timeout, &list[i].lock);
			break;
		case DB_LOCK_INHERIT:
			ret = __lock_inherit_locks(lt, locker, flags);
			break;
		case DB_LOCK_PUT:
			ret = __lock_put_nolock(dbenv,
			    &list[i].lock, &run_dd, flags);
			break;
		case DB_LOCK_PUT_ALL:
		case DB_LOCK_PUT_READ:
		case DB_LOCK_PREPARE:
		case DB_LOCK_UPGRADE_WRITE:
#ifdef VERBOSE_LATCH
			printf("Calling %s for lockerid %u line %d\n",
			    opstring(list[i].op), locker, __LINE__);
#endif

			/*
			 * Get the locker and mark it as deleted.  This
			 * allows us to traverse the locker links without
			 * worrying that someone else is deleting locks out
			 * from under us.  Since the locker may hold no
			 * locks (i.e., you could call abort before you've
			 * done any work), it's perfectly reasonable for there
			 * to be no locker; this is not an error.
			 */

			objlist = list[i].obj;
			if (objlist) {
				objlist->size = 0;
				objlist->data = NULL;
			}

			LOCKER_INDX(lt, region, locker, ndx);
			if ((rc = __lock_getlocker(lt,
				    locker, ndx, GETLOCKER_KEEP_PART,
				    &sh_locker)) != 0 || sh_locker == NULL ||
			    F_ISSET(sh_locker, DB_LOCKER_DELETED)) {
				/*
				 * If ret is set, then we'll generate an
				 * error.  If it's not set, we have nothing
				 * to do.
				 */
				if (sh_locker) {
					unlock_locker_partition(region,
					    sh_locker->partition);
					sh_locker = NULL;
				}
			}

			if (use_page_latches(dbenv))
				__find_latch_lockerid(dbenv, locker,
				    &latch_lockerid, 0);

			if (!latch_lockerid && !sh_locker)
				break;

			upgrade = 0;
			writes = 1;
			prepare = 0;
			if (list[i].op == DB_LOCK_PREPARE) {
				writes = 0;
				prepare = 1;
			}
			else if (list[i].op == DB_LOCK_PUT_READ)
				writes = 0;
			else if (list[i].op == DB_LOCK_UPGRADE_WRITE) {
				assert(!F_ISSET(sh_locker, DB_LOCKER_DIRTY));
				if (F_ISSET(sh_locker, DB_LOCKER_DIRTY))
					upgrade = 1;
				writes = 0;
			}
			objlist = list[i].obj;

			/* Writelocks should only come from sh_locker or
			 * latch_lockerid not both */
			if (latch_lockerid && latch_lockerid->has_pglk_lsn)
				assert(!sh_locker || !sh_locker->has_pglk_lsn);

			has_pglk_lsn = (sh_locker && sh_locker->has_pglk_lsn) ||
			    (latch_lockerid && latch_lockerid->has_pglk_lsn);

			if (latch_lockerid) {
				nwritelatches = nwrites =
				    __count_write_latches(dbenv,
				    latch_lockerid);
				/* XXX could you have write locks on both sides? */
				assert(!sh_locker || !sh_locker->nwrites ||
				    !nwrites);
			}

			if (sh_locker)
				nwrites += sh_locker->nwrites;

			if (objlist != NULL) {
				/*
				 * We know these should be ilocks,
				 * but they could be something else,
				 * so allocate room for the size too.
				 */
				objlist->size = 0;
				objlist->data = NULL;
				objlist->size =
				    nwrites *
				    (has_pglk_lsn ? sizeof(struct
					__db_lockobj_lsn) : sizeof(DBT));

				if (objlist->size) {
					if ((ret =
						__os_malloc(dbenv,
						    objlist->size,
						    &objlist->data)) != 0)
						memset(objlist->data, 0,
						    objlist->size);
				} else {
					objlist->data = NULL;
				}
				if (has_pglk_lsn) {
					lklsnp =
					    (struct __db_lockobj_lsn *)objlist->
					    data;
					np = NULL;
				} else {
					np = (DBT *)objlist->data;
					lklsnp = NULL;
				}
			} else {
				lklsnp = NULL;
				np = NULL;
			}

			if (list[i].op != DB_LOCK_PREPARE && sh_locker)
				F_SET(sh_locker, DB_LOCKER_DELETED);

			/* Now traverse the locks, releasing each one. */
			lp = sh_locker ? SH_LIST_FIRST(&sh_locker->heldby, __db_lock) : NULL;
			for ( ; lp != NULL; lp = next_lock) {
				sh_obj = lp->lockobj;
				next_lock = SH_LIST_NEXT(lp,
				    locker_links, __db_lock);
				if (writes == 1 ||
   					(lp->mode == DB_LOCK_READ &&
						((!prepare) || (!is_tablelock(sh_obj)))) ||
					lp->mode == DB_LOCK_DIRTY) {
					SH_LIST_REMOVE(lp,
					    locker_links, __db_lock);
					sh_obj = lp->lockobj;
					lndx = sh_obj->index;
					partition = sh_obj->partition;
					lock_obj_partition(region, partition);
					/*
					 * We are not letting lock_put_internal
					 * unlink the lock, so we'll have to
					 * update counts here.
					 */
					if (is_pagelock(sh_obj))
						sh_locker->npagelocks--;

					if (is_handlelock(sh_obj))
						sh_locker->nhandlelocks--;

					sh_locker->nlocks--;
					if (IS_WRITELOCK(lp->mode)) {
						sh_locker->nwrites--;
						nwrites--;
					}
					ret = __lock_put_internal(lt, lp,
					    NULL, lndx, &run_dd,
					    DB_LOCK_FREE | DB_LOCK_DOALL);
					unlock_obj_partition(region, partition);
					if (ret != 0)
						break;
					continue;
				}
				if (objlist != NULL) {
					counttot++;
					/* Tablelocks are readlocks, dont include them  */
					if (prepare && lp->mode == DB_LOCK_READ) {
						assert(is_tablelock(lp->lockobj));
						ntable++;
					} else {
						if (has_pglk_lsn) {
							DB_ASSERT((char *)lklsnp <
									(char *)objlist->data +
									objlist->size);
							SH_LIST_INIT(&lklsnp->lsns);
							Pthread_mutex_lock(&lp->
									lsns_mtx);
							lklsnp->nlsns = lp->nlsns;
							SH_LIST_FIRST(&(lklsnp->lsns),
									__db_lock_lsn) =
								SH_LIST_FIRST(&lp->lsns,
										__db_lock_lsn);
							SH_LIST_INIT(&lp->lsns);
							lp->nlsns = 0;
							Pthread_mutex_unlock(&lp->
									lsns_mtx);

							lklsnp->data =
								sh_obj->lockobj.data;
							lklsnp->size =
								sh_obj->lockobj.size;
							lklsnp++;
						} else {
							DB_ASSERT((char *)np <
									(char *)objlist->data +
									objlist->size);
							np->data = sh_obj->lockobj.data;
							np->size = sh_obj->lockobj.size;
							np++;
						}
					}
				}
			}

			if (ret != 0)
				goto up_done;

			/* Same loop but with latches */
			latch = latch_lockerid ? latch_lockerid->head : NULL;
			for ( ; latch ; ) {
				lnode = latch->listhead;
				while (lnode) {
					nextlnode = lnode->next;
					if (writes == 1 ||
					    lnode->lock_mode == DB_LOCK_READ) {
#ifdef VERBOSE_LATCH
						printf
						    ("locker %d tid %u putting page lock %p at latch %p line %d\n",
						    latch->lockerid,
						    pthread_self(), lnode,
						    latch, __LINE__);
#endif
						latch->count--;
						if (IS_WRITELOCK(lnode->
							lock_mode)) {
							latch->wrcount--;
							nwrites--;
							nwritelatches--;
						}
						if (lnode->prev)
							lnode->prev->next =
							    lnode->next;
						if (lnode->next)
							lnode->next->prev =
							    lnode->prev;
						if (lnode == latch->listhead)
							latch->listhead =
							    lnode->next;
						lnode->count = 0;
						__deallocate_ilock_latch(dbenv,
						    lnode);
					} else if (objlist != NULL) {
						countwl++;
						counttot++;
						if (has_pglk_lsn) {
							DB_ASSERT((char *)lklsnp
							    <
							    (char *)objlist->
							    data +
							    objlist->size);
							SH_LIST_INIT(&lklsnp->
							    lsns);
							Pthread_mutex_lock
							    (&lnode->lsns_mtx);
							lklsnp->nlsns =
							    lnode->nlsns;
							SH_LIST_FIRST(&(lklsnp->
								lsns),
							    __db_lock_lsn) =
							    SH_LIST_FIRST
							    (&lnode->lsns,
							    __db_lock_lsn);
							SH_LIST_INIT(&lnode->
							    lsns);
							lnode->nlsns = 0;
							Pthread_mutex_unlock
							    (&lnode->lsns_mtx);
							lklsnp->data =
							    lnode->lock;
							lklsnp->size =
							    sizeof(struct
							    __db_ilock);
							lklsnp++;
						} else {
							DB_ASSERT((char *)np <
							    (char *)objlist->
							    data +
							    objlist->size);
							np->data = lnode->lock;
							np->size =
							    sizeof(struct
							    __db_ilock);
							np++;
						}
					}
					lnode = nextlnode;
				}

				nextlatch = latch->next;
				if (!latch->listhead) {
					if (latch->prev)
						latch->prev->next = latch->next;
					if (latch->next)
						latch->next->prev = latch->prev;
					if (latch == latch_lockerid->head)
						latch_lockerid->head =
						    latch->next;
					latch->prev = latch->next = NULL;
					latch->tid = 0;
					latch->lockerid = 0;
					latch->locker = NULL;
					Pthread_mutex_unlock(&latch->lock);
				}
				latch = nextlatch;
			}

			if (objlist != NULL) {
				assert(counttot - ntable == nwrites);
				assert(countwl == nwritelatches);

				if ((ret = __lock_fix_list(dbenv,
					    objlist, nwrites,
					    has_pglk_lsn)) != 0)
					goto up_done;
			}
			switch (list[i].op) {
			case DB_LOCK_UPGRADE_WRITE:
				if (upgrade != 1)
					goto up_done;
				lp = NULL;
				/* We don't allow dirty reads in comdb2 */
/*
				for (sh_locker && (lp = SH_LIST_FIRST(
				    &sh_locker->heldby, __db_lock));
				    lp != NULL;
				    lp = SH_LIST_NEXT(lp,
					    locker_links, __db_lock)) {
					if (lp->mode != DB_LOCK_WWRITE)
						continue;
					lock.off = R_OFFSET(&lt->reginfo, lp);
					lock.gen = lp->gen;
					F_SET(sh_locker, DB_LOCKER_INABORT);
					unlock_locker_partition(region,
					    sh_locker->partition);
					ret = __lock_get_internal(lt, locker,
					    sh_locker, DB_LOCK_UPGRADE, NULL,
					    DB_LOCK_WRITE, 0, &lock);
					lock_locker_partition(region,
					    sh_locker->partition);
					if (ret != 0)
						break;
				}
*/
up_done:

				/* FALL THROUGH */
			case DB_LOCK_PUT_ALL:
				if (list[i].op == DB_LOCK_PUT_ALL &&
				    latch_lockerid)
					assert(!latch_lockerid->head);

			case DB_LOCK_PUT_READ:
				if (sh_locker)
					F_CLR(sh_locker, DB_LOCKER_DELETED);
				break;
			default:
				break;
			}
			if (sh_locker)
				unlock_locker_partition(region,
				    sh_locker->partition);
			break;
		case DB_LOCK_PUT_OBJ:
			/* Remove all the locks associated with an object. */
			OBJECT_INDX(lt, region, list[i].obj, ndx, partition);
			lock_obj_partition(region, partition);
			if ((ret = __lock_getobj(lt, list[i].obj,
				    ndx, partition, 0, &sh_obj)) != 0 ||
			    sh_obj == NULL) {
				if (ret == 0)
					ret = EINVAL;
				unlock_obj_partition(region, partition);
				break;
			}

			/*
			 * Go through both waiters and holders.  Don't bother
			 * to run promotion, because everyone is getting
			 * released.  The processes waiting will still get
			 * awakened as their waiters are released.
			 */
again1:		for (lp = SH_TAILQ_FIRST(&sh_obj->waiters, __db_lock);
			    ret == 0 && lp != NULL;
			    lp = SH_TAILQ_FIRST(&sh_obj->waiters, __db_lock)) {
				u_int32_t tp = sh_obj->partition;
				u_int32_t tg = sh_obj->generation;
				DB_LOCKER *tl = lp->holderp;
				unlock_obj_partition(region, sh_obj->partition);
				lock_locker_partition(region, tl->partition);
				lock_obj_partition(region, sh_obj->partition);

				if (tp != sh_obj->partition ||
				    tg != sh_obj->generation) {
					unlock_locker_partition(region,
					    tl->partition);
					goto again1;
				}

				ret =
				    __lock_put_internal(lt, lp, NULL, ndx,
				    &run_dd,
				    DB_LOCK_UNLINK | DB_LOCK_NOPROMOTE |
				    DB_LOCK_DOALL);

				unlock_locker_partition(region, tl->partition);
			}

			/*
			 * On the last time around, the object will get
			 * reclaimed by __lock_put_internal, structure the
			 * loop carefully so we do not get bitten.
			 */
again2:		for (lp = SH_TAILQ_FIRST(&sh_obj->holders, __db_lock);
			    ret == 0 && lp != NULL; lp = next_lock) {
				next_lock = SH_TAILQ_NEXT(lp, links, __db_lock);

				u_int32_t tp = sh_obj->partition;
				u_int32_t tg = sh_obj->generation;
				DB_LOCKER *tl = lp->holderp;
				unlock_obj_partition(region, sh_obj->partition);
				lock_locker_partition(region, tl->partition);
				lock_obj_partition(region, sh_obj->partition);

				if (tp != sh_obj->partition ||
				    tg != sh_obj->generation) {
					unlock_locker_partition(region,
					    tl->partition);
					goto again2;
				}

				ret =
				    __lock_put_internal(lt, lp, NULL, ndx,
				    &run_dd,
				    DB_LOCK_UNLINK | DB_LOCK_NOPROMOTE |
				    DB_LOCK_DOALL);

				unlock_locker_partition(region, tl->partition);
			}
			unlock_obj_partition(region, partition);
			break;

		case DB_LOCK_TIMEOUT:
			ret = __lock_set_timeout_internal(dbenv,
			    locker, 0, DB_SET_TXN_NOW);
			break;

		case DB_LOCK_TRADE_COMP:
		case DB_LOCK_TRADE:
			/*
			 * INTERNAL USE ONLY.
			 * Change the holder of the lock described in
			 * list[i].lock to the locker-id specified by
			 * the locker parameter.
			 */
			/*
			 * You had better know what you're doing here.
			 * We are trading locker-id's on a lock to
			 * facilitate file locking on open DB handles.
			 * We do not do any conflict checking on this,
			 * so heaven help you if you use this flag under
			 * any other circumstances.
			 */
			ret = __lock_trade(dbenv, &list[i].lock, locker,
			    (list[i].op == DB_LOCK_TRADE_COMP));
			break;
#ifdef DEBUG
		case DB_LOCK_DUMP:
			/* Find the locker. */
			LOCKER_INDX(lt, region, locker, ndx);
			if ((ret = __lock_getlocker(lt,
				    locker, ndx, GETLOCKER_KEEP_PART,
				    &sh_locker)) != 0 || sh_locker == NULL)
				break;
			if (F_ISSET(sh_locker, DB_LOCKER_DELETED)) {
				unlock_locker_partition(region,
				    sh_locker->partition);
				break;
			}

			for (lp = SH_LIST_FIRST(&sh_locker->heldby, __db_lock);
			    lp != NULL;
			    lp = SH_LIST_NEXT(lp, locker_links, __db_lock)) {
				__lock_printlock(lt, lp, 1, NULL);
			}
			unlock_locker_partition(region, sh_locker->partition);
			break;
#endif
		default:
			__db_err(dbenv,
			    "Invalid lock operation: %d", list[i].op);
			ret = EINVAL;
			break;
		}
	}

	UNLOCKREGION(dbenv, (DB_LOCKTAB *)dbenv->lk_handle);

	/* FIXME TODO XXX should I be checking for need_dd here?? */
	if (run_dd || region->need_dd)
		__lock_detect(dbenv, region->detect, NULL);

	if (ret != 0 && elistp != NULL)
		*elistp = &list[i - 1];

	return (ret);
}

/*
 * __lock_get_pp --
 *	DB_ENV->lock_get pre/post processing.
 *
 * PUBLIC: int __lock_get_pp __P((DB_ENV *,
 * PUBLIC:     u_int32_t, u_int32_t, const DBT *, db_lockmode_t, DB_LOCK *));
 */
int
__lock_get_pp(dbenv, locker, flags, obj, lock_mode, lock)
	DB_ENV *dbenv;
	u_int32_t locker, flags;
	const DBT *obj;
	db_lockmode_t lock_mode;
	DB_LOCK *lock;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "DB_ENV->lock_get", DB_INIT_LOCK);

	/* Validate arguments. */
	if ((ret = __db_fchk(dbenv, "DB_ENV->lock_get", flags,
		    DB_LOCK_NOWAIT | DB_LOCK_UPGRADE | DB_LOCK_SWITCH |
		    DB_LOCK_LOGICAL | DB_LOCK_NOPAGELK | DB_LOCK_ONELOCK)) != 0)
		return (ret);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __lock_get(dbenv, locker, flags, obj, lock_mode, lock);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __lock_get --
 *	DB_ENV->lock_get.
 *
 * PUBLIC: int __lock_get __P((DB_ENV *,
 * PUBLIC:     u_int32_t, u_int32_t, const DBT *, db_lockmode_t, DB_LOCK *));
 */
int
__lock_get(dbenv, locker, flags, obj, lock_mode, lock)
	DB_ENV *dbenv;
	u_int32_t locker, flags;
	const DBT *obj;
	db_lockmode_t lock_mode;
	DB_LOCK *lock;
{
	int ret;

	LOCKREGION(dbenv, (DB_LOCKTAB *)dbenv->lk_handle);
	ret = __lock_get_internal(dbenv->lk_handle,
	    locker, NULL, flags, obj, lock_mode, 0, lock);
	UNLOCKREGION(dbenv, (DB_LOCKTAB *)dbenv->lk_handle);
	return (ret);
}

int gbl_stack_at_lock_get = 0;
int gbl_stack_at_lock_handle = 0;
int gbl_stack_at_write_lock = 0;
int gbl_stack_at_lock_gen_increment = 0;

static void inline
stack_at_gen_increment(struct __db_lock *lockp, DB_LOCK * lock)
{
	lockp->stackid = gbl_stack_at_lock_gen_increment ? stackutil_get_stack_id("getlock") : -1;
}

static void inline
stack_at_get_lock(struct __db_lock *lockp, DB_LOCK * lock, int ishandle, int iswrite)
{
	lockp->stackid = (gbl_stack_at_lock_get ||
					 (gbl_stack_at_lock_handle && ishandle) ||
					 (gbl_stack_at_write_lock && iswrite)) ?
					 stackutil_get_stack_id("getlock") : -1;
}

/* Return 1 if this is a comdb2 rowlock, 0 otherwise. */
static inline int
is_comdb2_rowlock(u_int32_t sz)
{
	/* Size 30 is a rowlock, size 31 is an endlock */
	if (sz == 30 || sz == 31)
		return 1;

	return 0;
}

/*int bdb_lock_desired(void *bdb_state);*/
int rowlocks_bdb_lock_check(void *bdb_state);


/* The replication thread in rowlocks world is forbidden from deadlocking
 * while acquiring a rowlock because only the master can roll back a logical
 * transaction.  The replication thread also (at times) needs to grab the bdb-
 * write lock.  This can cause a deadlock if an sql reader on the replicant 
 * (which holds the bdb-readlock) is blocked on a row that is locked for a 
 * logical transaction, and the replication thread (which holds this rowlock)
 * needs to acquire the bdb-writelock.
 *
 * Since the replication thread can't release it's logical locks, if the bdb
 * lock is desired, we're forced to do the following:
 *
 * 1. Return deadlock all sql threads which are blocked on a rowlock
 * 2. Return deadlock to any sql thread which is about to acquire a rowlock
 *
 * We already set the 'lock_is_desired' flag before acquiring the bdb-writelock.
 * rep_return_deadlock ensures that threads attempting to get a rowlock while
 * the bdb-writelock is desired will immediately return deadlock.  Just before 
 * blocking, bdb_get_writelock calls __lock_abort_lockerid on all logical 
 * txnids.  This will go through the txnid's held locks, and abort anything 
 * that's sitting on the waitlist.  
 *
 * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
 *
 * It turns out that it's not just the bdblock: any lock held by a reader on
 * a replicant can cause a deadlock.  Consider this scenario:
 *
 * 1. The replication thread gathers rowlock (1) using logical lockerid A
 * 2. A READER thread gathers rowlock (2), and blocks on rowlock (1)
 * 3. The replication thread blocks on rowlock (2) under lockerid B
 *
 * A diagram:
 *
 *      REP-LOCKID-B ---> READER(2) ---> REP-LOCKID-A(1)
 *
 * There's no deadlock, and no cycle, but nothing will make any progress.  This
 * impacts only the replicant, which gathers rowlocks strictly in the order of
 * the log-stream, but it won't happen on the master, where every lockerid is 
 * being run in a separate thread.  
 *
 * I'm addressing this via the DB_LOCK_LOGICAL flag.  If the the DB_LOCK_LOGICAL
 * flag is set, the code will attempt to return 'deadlock' to anything which is 
 * holding the lock in question.  Lockerids which are blocked waiting for 
 * another lock will get a deadlock immediately.  Lockerids which are not 
 * blocked on a lock will get a deadlock the next time they attempt to acquire 
 * any lock (until the lockerid is freed).
 */

/* Return 1 if this should throw a deadlock. */
static inline int
rep_return_deadlock(DB_ENV *dbenv, u_int32_t sz)
{
	if (IS_REP_CLIENT(dbenv) &&
	    is_comdb2_rowlock(sz) && rowlocks_bdb_lock_check(dbenv->app_private)
	    ) {
		fprintf(stderr, "%s: thread %p returning deadlock\n", __func__,
		    (void *)pthread_self());
		return 1;
	} else {
		return 0;
	}
}

#define LOCKOBJ_MAX_SIZE 33
struct {
	DBT obj;
	char mem[LOCKOBJ_MAX_SIZE];
} gbl_rep_lockobj;

void berk_init_rep_lockobj() {
	gbl_rep_lockobj.obj.data = gbl_rep_lockobj.mem;
	gbl_rep_lockobj.obj.ulen = LOCKOBJ_MAX_SIZE;
	gbl_rep_lockobj.obj.flags = DB_DBT_USERMEM;
}

void comdb2_dump_blockers(DB_ENV *dbenv)
{
	struct __db_lock *hlp, *lp;
	DB_LOCKTAB *lt;
	DB_LOCKREGION *region;
	DB_LOCKOBJ *obj;
	u_int32_t ndx;
	u_int32_t partition;
	int ret;

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;

	OBJECT_INDX(lt, region, &gbl_rep_lockobj.obj, ndx, partition);

	lock_obj_partition(region, partition);

	ret = __lock_getobj(lt, &gbl_rep_lockobj.obj, ndx, partition, 0, &obj);

	if (ret != 0 || !obj) {
		unlock_obj_partition(region, partition);
		return;
	}

	if (SH_TAILQ_FIRST(&obj->holders, __db_lock)) {
		logmsg(LOGMSG_USER, "SQL statements currently blocking the replication thread:\n");
	}
	for (hlp = SH_TAILQ_FIRST(&obj->holders, __db_lock);
		hlp != NULL; hlp = SH_TAILQ_NEXT(hlp, links, __db_lock))
	{
		comdb2_dump_blocker(hlp->holderp->id);
	}
	unlock_obj_partition(region, partition);
}


#define ADD_TO_HOLDARR(x)                                                      \
	do {                                                                   \
		if (holdix + 1 >= holdsz) {                                    \
			int newsz = (holdsz == 0 ? 10 : holdsz << 1);          \
			if ((ret = __os_realloc(dbenv,                         \
						newsz * sizeof(u_int32_t),     \
						&holdarr)) != 0)               \
				abort();                                       \
			holdsz = newsz;                                        \
		}                                                              \
		holdarr[holdix++] = x;                                         \
	} while (0)

/*
 * __lock_get_internal --
 *
 * All the work for lock_get (and for the GET option of lock_vec) is done
 * inside of lock_get_internal.
 */
static int
__lock_get_internal_int(lt, locker, in_locker, flags, obj, lock_mode, timeout,
    lock)
	DB_LOCKTAB *lt;
	u_int32_t locker;
	DB_LOCKER **in_locker;
	u_int32_t flags;
	const DBT *obj;
	db_lockmode_t lock_mode;
	db_timeout_t timeout;
	DB_LOCK *lock;
{
	extern __thread int disable_random_deadlocks;
	if (disable_random_deadlocks == 0 && 
		unlikely(gbl_ddlk && !LF_ISSET(DB_LOCK_NOWAIT) &&
		rand() % gbl_ddlk == 0)) {
		return DB_LOCK_DEADLOCK;
	}
	u_int32_t partition = gbl_lk_parts, lpartition = gbl_lkr_parts;
	int handlelock = 0, writelock = 0;
	uint64_t x1 = 0, x2;
	struct __db_lock *newl, *lp, *firstlp, *wwrite;
	DB_ENV *dbenv;
	DB_LOCKER *sh_locker;
	DB_LOCKOBJ *sh_obj;
	DB_LOCKREGION *region;
	u_int32_t holder, obj_ndx, ihold, *holdarr = NULL, holdix, holdsz;
	extern int gbl_lock_get_verbose_waiter;
	int verbose_waiter = gbl_lock_get_verbose_waiter;;
	int grant_dirty, no_dd, ret, t_ret;
	extern int gbl_locks_check_waiters;

	/*
	 * We decide what action to take based on what
	 * locks are already held and what locks are
	 * in the wait queue.
	 */
	enum {
		GRANT,		/* Grant the lock. */
		UPGRADE,	/* Upgrade the lock. */
		HEAD,		/* Wait at head of wait queue. */
		SECOND,		/* Wait as the second waiter. */
		TAIL		/* Wait at tail of the wait queue. */
	} action;

	dbenv = lt->dbenv;
	region = lt->reginfo.primary;

	/* Check if locks have been globally turned off. */
	if (unlikely(F_ISSET(dbenv, DB_ENV_NOLOCKING)))
		return (0);

	no_dd = ret = 0;
	newl = NULL;

	/*
	 * If we are not going to reuse this lock, invalidate it
	 * so that if we fail it will not look like a valid lock.
	 */
	if (!LF_ISSET(DB_LOCK_UPGRADE | DB_LOCK_SWITCH))
		LOCK_INIT(*lock);

	/* Check that the lock mode is valid.  */
	if (unlikely((u_int32_t)lock_mode >= region->stat.st_nmodes)) {
		/* DB_LOCK_WRITE_LOGICAL is a fake mode that's changed to DB_LOCK_WRITE under the hood. */
		__db_err(dbenv, "DB_ENV->lock_get: invalid lock mode %lu",
		    (u_long)lock_mode);
		return (EINVAL);
	}

	/* I don't think we get any lock_mode of 0 */
	if (unlikely(DB_LOCK_NG == lock_mode)) {
		/* We shouldn't ever get here. */
		__db_err(dbenv, "DB_ENV->lock_get: invalid lock mode %lu",
		    (u_long)lock_mode);
		abort();
	}

	region->stat.st_nrequests++;

	sh_locker = *in_locker;

	if (sh_locker == NULL) {
		u_int32_t locker_ndx;
		u_int32_t gl_flags =
		    (locker > DB_LOCK_MAXID ? GETLOCKER_CREATE : 0) |
		    (LF_ISSET(DB_LOCK_LOGICAL) ? GETLOCKER_LOGICAL : 0);
		gl_flags |= GETLOCKER_KEEP_PART;
		LOCKER_INDX(lt, region, locker, locker_ndx);
		if ((ret = __lock_getlocker(lt, locker, locker_ndx,
			    gl_flags, &sh_locker)) != 0) {
			goto err;
		}
		if (sh_locker == NULL) {
			__db_err(dbenv, "Locker does not exist");
			abort();
		}
	} else {
		lock_locker_partition(region, sh_locker->partition);
	}
	lpartition = sh_locker->partition;

    extern __thread int track_thread_locks;
    if (track_thread_locks) {
        comdb2_cheapstack_sym(stderr, "lockid %u", locker);
    }

#ifdef DEBUG_LOCKS
	DB_LOCKER *mlockerp = R_ADDR(&lt->reginfo, sh_locker->master_locker);
	logmsg(LOGMSG_ERROR, "%p Get (%c) locker lock %x (m %x)\n",
			(void *)pthread_self(), lock_mode == DB_LOCK_READ? 'R':'W', sh_locker->id,
			mlockerp->id);
	cheap_stack_trace();

	char desc[100];
	DBT dbt = { 0 };
	int idx, size = -1;
	if (obj) {
		dbt.data = obj->data;
		size = dbt.size = obj->size;

		bdb_describe_lock_dbt(dbenv, &dbt, desc, sizeof(desc));
	} else {
		snprintf(desc, sizeof(desc), "NULL OBJ LK");
	}

	Pthread_mutex_lock(&lblk);
	idx = lbcounter;
	lbcounter = (lbcounter + 1) % LKBUFMAX;
	Pthread_mutex_unlock(&lblk);

	threadid[idx] = pthread_self();
	snprintf(lkbuffer[idx], LKBUFSZ, " %p get lid %x %s size=%d ",
			(void *)pthread_self(), locker, desc, size);
	logmsg(LOGMSG_ERROR, "%p __lock_get_internal_int: get lid %x %s size=%d\n",
	       (void *)pthread_self(), locker, desc, size);
#endif

	if (obj == NULL) {
		DB_ASSERT(LOCK_ISSET(*lock));
		lp = (struct __db_lock *)R_ADDR(&lt->reginfo, lock->off);
		sh_obj = lp->lockobj;
		partition = sh_obj->partition;
		lock_obj_partition(region, partition);
	} else {
		/* Run the 'return deadlock' check before grabbing the lock object */
		if (!LF_ISSET(DB_LOCK_LOGICAL) &&
		    rep_return_deadlock(dbenv, obj->size)) {
			ret =
			    (LF_ISSET(DB_LOCK_NOWAIT) ? DB_LOCK_NOTGRANTED :
			    DB_LOCK_DEADLOCK);
			goto err;
		}

		/* Allocate a shared memory new object. */
		OBJECT_INDX(lt, region, obj, lock->ndx, partition);
		lock_obj_partition(region, partition);
		ret = __lock_getobj(lt, obj, lock->ndx, partition, 1, &sh_obj);
		if (ret != 0)
			goto err;
	}
	lock->partition = partition;

	/* Throw deadlock if this is a reader-thread and the bdb lock is desired */
	if (!LF_ISSET(DB_LOCK_LOGICAL) &&
	    rep_return_deadlock(dbenv, sh_obj->lockobj.size)) {
		ret =
		    (LF_ISSET(DB_LOCK_NOWAIT) ? DB_LOCK_NOTGRANTED :
		    DB_LOCK_DEADLOCK);
		goto err;
	}

	/* Throw deadlock if NOPAGELK is set and this lockerid is holding pagelocks */
	if (LF_ISSET(DB_LOCK_NOPAGELK) && sh_locker->npagelocks > 0) {
		ret = (LF_ISSET(DB_LOCK_NOWAIT) ?
		    DB_LOCK_NOTGRANTED : DB_LOCK_DEADLOCK);
		goto err;
	}

	/* Throw deadlock if the deadlock flag is set for this lockerid */
	if (is_comdb2_rowlock(sh_obj->lockobj.size) &&
	    F_ISSET(sh_locker, DB_LOCKER_DEADLOCK)) {
		ret =
		    (LF_ISSET(DB_LOCK_NOWAIT) ? DB_LOCK_NOTGRANTED :
		    DB_LOCK_DEADLOCK);
		goto err;
	}

	/*
	 * Figure out if we can grant this lock or if it should wait.
	 * By default, we can grant the new lock if it does not conflict with
	 * anyone on the holders list OR anyone on the waiters list.
	 * The reason that we don't grant if there's a conflict is that
	 * this can lead to starvation (a writer waiting on a popularly
	 * read item will never be granted).  The downside of this is that
	 * a waiting reader can prevent an upgrade from reader to writer,
	 * which is not uncommon.
	 *
	 * There are two exceptions to the no-conflict rule.  First, if
	 * a lock is held by the requesting locker AND the new lock does
	 * not conflict with any other holders, then we grant the lock.
	 * The most common place this happens is when the holder has a
	 * WRITE lock and a READ lock request comes in for the same locker.
	 * If we do not grant the read lock, then we guarantee deadlock.
	 * Second, dirty readers are granted if at all possible while
	 * avoiding starvation, see below.
	 *
	 * In case of conflict, we put the new lock on the end of the waiters
	 * list, unless we are upgrading or this is a dirty reader in which
	 * case the locker goes at or near the front of the list.
	 */
	ihold = 0;
	grant_dirty = 0;
	holder = 0;
	holdarr = NULL;
	holdix = 0;
	holdsz = 0;
	wwrite = NULL;

	/*
	 * SWITCH is a special case, used by the queue access method
	 * when we want to get an entry which is past the end of the queue.
	 * We have a DB_READ_LOCK and need to switch it to DB_LOCK_WAIT and
	 * join the waiters queue.  This must be done as a single operation
	 * so that another locker cannot get in and fail to wake us up.
	 */
	if (LF_ISSET(DB_LOCK_SWITCH))
		lp = NULL;
	else
		lp = SH_TAILQ_FIRST(&sh_obj->holders, __db_lock);

	firstlp = NULL;
	for (; lp != NULL; lp = SH_TAILQ_NEXT(lp, links, __db_lock)) {
		if (locker == lp->holderp->id) {
			if (lp->mode == lock_mode &&
			    lp->status == DB_LSTAT_HELD) {
				if (LF_ISSET(DB_LOCK_UPGRADE))
					goto upgrade;

				/*
				 * Lock is held, so we can increment the
				 * reference count and return this lock
				 * to the caller.  We do not count reference
				 * increments towards the locks held by
				 * the locker.
				 */
				lp->refcount++;
				lock->off = R_OFFSET(&lt->reginfo, lp);
				lock->gen = lp->gen;
				lock->mode = lp->mode;
				if (is_pagelock(sh_obj) &&
				    IS_WRITELOCK(lock_mode) &&
				    F_ISSET(sh_locker,
					DB_LOCKER_TRACK_WRITELOCKS) &&
				    (sh_locker->ntrackedlocks == 0 ||
					sh_locker->tracked_locklist[sh_locker->
					    ntrackedlocks - 1] != lp)) {
					if (sh_locker->ntrackedlocks + 1 >
					    sh_locker->maxtrackedlocks) {
						struct __db_lock **oldlist =
						    sh_locker->tracked_locklist;
						sh_locker->maxtrackedlocks *= 2;
						if (__os_malloc(dbenv,
							sizeof(struct __db_lock
							    *) *
							sh_locker->
							maxtrackedlocks,
							&(sh_locker->
							    tracked_locklist))
						    != 0) {
							return ENOMEM;
						}
						memcpy(sh_locker->
						    tracked_locklist, oldlist,
						    sizeof(struct __db_lock *) *
						    sh_locker->ntrackedlocks);
						__os_free(dbenv, oldlist);
					}
					sh_locker->tracked_locklist[sh_locker->
					    ntrackedlocks++] = lp;
				}
				goto done;
			} else {
				ihold = 1;
				if (lock_mode == DB_LOCK_WRITE &&
				    lp->mode == DB_LOCK_WWRITE)
					wwrite = lp;
			}
		} else if (__lock_is_parent(lt, lp->holderp->id, sh_locker))
			ihold = 1;

		else if (CONFLICTS(lt, region, lp->mode, lock_mode)) {
			if (gbl_locks_check_waiters) {
				ADD_TO_HOLDARR(lp->holderp->id);
				if (!firstlp)
					firstlp = lp;
				continue;
			}
			break;
		} else if (lp->mode == DB_LOCK_READ ||
		    lp->mode == DB_LOCK_WWRITE) {
			grant_dirty = 1;
			holder = lp->holderp->id;
		}
	}

	if (firstlp)
		lp = firstlp;

	/*
	 * If there are conflicting holders we will have to wait.
	 * An upgrade or dirty reader goes to the head
	 * of the queue, everone else to the back.
	 */
	if (lp != NULL) {
		if (LF_ISSET(DB_LOCK_UPGRADE) ||
		    wwrite != NULL || lock_mode == DB_LOCK_DIRTY ||
		    LF_ISSET(DB_LOCK_LOGICAL))
			action = HEAD;
		else {
			action = TAIL;

			if (gbl_locks_check_waiters) {
				firstlp = lp;
				for (lp =
				    SH_TAILQ_FIRST(&sh_obj->waiters, __db_lock);
				    lp != NULL;
				    lp = SH_TAILQ_NEXT(lp, links, __db_lock)) {
					if (CONFLICTS(lt, region, lp->mode,
						lock_mode) &&
					    locker != lp->holderp->id) {
						ADD_TO_HOLDARR(lp->holderp->id);
					}
				}
				lp = firstlp;
			}
		}
	} else {
		if (LF_ISSET(DB_LOCK_SWITCH))
			action = TAIL;
		else if (LF_ISSET(DB_LOCK_UPGRADE) || wwrite != NULL)
			action = UPGRADE;
		/* Grant logical locks desired by the replicant immediately.  
		 * This flag is only set via the replication stream. */
		else if (ihold)
			action = GRANT;
		else {
			/*
			 * Look for conflicting waiters.
			 */
			firstlp = NULL;
			for (lp = SH_TAILQ_FIRST(&sh_obj->waiters, __db_lock);
			    lp != NULL;
			    lp = SH_TAILQ_NEXT(lp, links, __db_lock)) {
				if (CONFLICTS(lt, region, lp->mode,
					lock_mode) &&
				    locker != lp->holderp->id) {
					/* DB_LOCK_DIRTY goes to head */
					if (gbl_locks_check_waiters &&
					    lock_mode != DB_LOCK_DIRTY) {
						ADD_TO_HOLDARR(lp->holderp->id);
						if (!firstlp) {
							firstlp = lp;
						}
						continue;
					}
					break;
				}
			}

			if (firstlp)
				lp = firstlp;
			/*
			 * If there are no conflicting holders or waiters,
			 * then we grant. Normally when we wait, we
			 * wait at the end (TAIL).  However, the goal of
			 * DIRTY_READ locks to allow forward progress in the
			 * face of updating transactions, so we try to allow
			 * all DIRTY_READ requests to proceed as rapidly
			 * as possible, so long as we can prevent starvation.
			 *
			 * When determining how to queue a DIRTY_READ
			 * request:
			 *
			 *	1. If there is a waiting upgrading writer,
			 *	   then we enqueue the dirty reader BEHIND it
			 *	   (second in the queue).
			 *	2. Else, if the current holders are either
			 *	   READ or WWRITE, we grant
			 *	3. Else queue SECOND i.e., behind the first
			 *	   waiter.
			 *
			 * The end result is that dirty_readers get to run
			 * so long as other lockers are blocked.  Once
			 * there is a locker which is only waiting on
			 * dirty readers then they queue up behind that
			 * locker so that it gets to run.  In general
			 * this locker will be a WRITE which will shortly
			 * get downgraded to a WWRITE, permitting the
			 * DIRTY locks to be granted.
			 */
			if (lp == NULL)
				action = GRANT;
			else if (lock_mode == DB_LOCK_DIRTY && grant_dirty) {
				/*
				 * An upgrade will be at the head of the
				 * queue.
				 */
				lp = SH_TAILQ_FIRST(
						&sh_obj->waiters, __db_lock);
				if (lp->mode == DB_LOCK_WRITE &&
						lp->holderp->id == holder)
					action = SECOND;
				else
					action = GRANT;
			} else if (lock_mode == DB_LOCK_DIRTY)
				action = SECOND;
			else
				action = TAIL;
		}
	}

	switch (action) {
	case HEAD:
	case TAIL:
	case SECOND:
	case GRANT:
		/* Allocate a new lock. */
		if (++region->stat.st_nlocks > region->stat.st_maxnlocks)
			region->stat.st_maxnlocks = region->stat.st_nlocks;

		if ((newl =
			SH_TAILQ_FIRST(&region->free_locks[partition],
			    __db_lock)) == NULL) {
			unsigned num;
			++region->nwlk_scale[partition];
			num = region->object_p_size
			    * region->nwlk_scale[partition];
			PRINTF(nwlk_scale, "add  lk:%d part:%d sc:%d\n",
			    num, partition, region->nwlk_scale[partition]);
			ret = __os_malloc(dbenv,
			    sizeof(struct __db_lock) * num, &newl);
			if (ret != 0) {
				__db_err(dbenv, __db_lock_err, "locks");
				unlock_obj_partition(region, partition);
				unlock_locker_partition(region, lpartition);
				if (holdarr)
					__os_free(dbenv, holdarr);
				return (ENOMEM);
			}
			ret =
			    add_to_lock_partition(dbenv, lt, partition, num,
			    newl);
			if (ret != 0) {
				if (holdarr)
					__os_free(dbenv, holdarr);
				return ret;
			}
			newl =
			    SH_TAILQ_FIRST(&region->free_locks[partition],
			    __db_lock);
		}
		SH_TAILQ_REMOVE(&region->free_locks[partition], newl,
		    links, __db_lock);
		newl->holderp = sh_locker;
		newl->refcount = 1;
		newl->mode = lock_mode;
		newl->lockobj = sh_obj;

		/*
		 * Now, insert the lock onto its locker's list.
		 * If the locker does not currently hold any locks,
		 * there's no reason to run a deadlock
		 * detector, save that information.
		 */
		no_dd = sh_locker->master_locker == INVALID_ROFF &&
		    SH_LIST_FIRST(&sh_locker->child_locker, __db_locker) == NULL
		    && SH_LIST_FIRST(&sh_locker->heldby, __db_lock) == NULL;

		SH_LIST_INSERT_HEAD(&sh_locker->heldby, newl, locker_links,
		    __db_lock);

#ifndef TESTSUITE
		if (gbl_berkdb_track_locks) {
			DBT *objcpy;
			objcpy = malloc(sizeof(DBT));
			*objcpy = *obj;
			objcpy->data = malloc(obj->size);
			memcpy(objcpy->data, obj->data, obj->size);
			newl->dbtobj = objcpy;
			thread_add_resource(RESOURCE_BERKELEY_LOCK, objcpy);
		}
#endif

		break;

	case UPGRADE:
upgrade:
		if (wwrite != NULL) {
			lp = wwrite;
			lp->refcount++;
			lock->off = R_OFFSET(&lt->reginfo, lp);
			lock->gen = lp->gen;
			lock->mode = lock_mode;
		} else
			lp = (struct __db_lock *)R_ADDR(&lt->reginfo,
			    lock->off);
		if (IS_WRITELOCK(lock_mode) && !IS_WRITELOCK(lp->mode))
			sh_locker->nwrites++;
		lp->mode = lock_mode;
		if (is_pagelock(sh_obj) &&
		    IS_WRITELOCK(lock_mode) &&
		    F_ISSET(sh_locker, DB_LOCKER_TRACK_WRITELOCKS) &&
		    (sh_locker->ntrackedlocks == 0 ||
			sh_locker->tracked_locklist[sh_locker->ntrackedlocks -
			    1] != lp)) {
			if (sh_locker->ntrackedlocks + 1 >
			    sh_locker->maxtrackedlocks) {
				struct __db_lock **oldlist =
				    sh_locker->tracked_locklist;
				sh_locker->maxtrackedlocks *= 2;
				if (__os_malloc(dbenv,
					sizeof(struct __db_lock *) *
					sh_locker->maxtrackedlocks,
					&(sh_locker->tracked_locklist)) != 0) {
					return ENOMEM;
				}
				memcpy(sh_locker->tracked_locklist, oldlist,
				    sizeof(struct __db_lock *) *
				    sh_locker->ntrackedlocks);
				__os_free(dbenv, oldlist);
			}
			sh_locker->tracked_locklist[sh_locker->
			    ntrackedlocks++] = lp;
		}
		goto done;
	}

	switch (action) {
	case UPGRADE:
		DB_ASSERT(0);
		break;
	case GRANT:
		newl->status = DB_LSTAT_HELD;
		SH_TAILQ_INSERT_TAIL(&sh_obj->holders, newl, links);
		if (gbl_bb_berkdb_enable_thread_stats) {
			struct berkdb_thread_stats *t;
			struct berkdb_thread_stats *p;
			t = bb_berkdb_get_thread_stats();
			p = bb_berkdb_get_process_stats();
			p->n_locks++;
			t->n_locks++;
		}
		break;
	case HEAD:
	case TAIL:
	case SECOND:
		if (LF_ISSET(DB_LOCK_NOWAIT)) {
			lock->owner = lp->holderp->id;
			lock->mode = lp->mode;
			ret = DB_LOCK_NOTGRANTED;
			region->stat.st_nnowaits++;
			goto err;
		}
		if ((lp = SH_TAILQ_FIRST(&sh_obj->waiters, __db_lock)) == NULL) {
			lock_detector(region);
			SH_TAILQ_INSERT_HEAD(&region->dd_objs,
			    sh_obj, dd_links, __db_lockobj);
			++sh_obj->generation;
			unlock_detector(region);
		}

		extern u_int32_t gbl_rep_lockid;
		if (locker == gbl_rep_lockid) {
			// Copy the lockobj that the replication thread is
			// waiting on. This could later be used to look up
			// the information of the lock holders.
			assert(sh_obj->lockobj.size <= LOCKOBJ_MAX_SIZE);
			memset(gbl_rep_lockobj.obj.data, 0, LOCKOBJ_MAX_SIZE);
			gbl_rep_lockobj.obj.size = sh_obj->lockobj.size;
			memcpy(gbl_rep_lockobj.obj.data, sh_obj->lockobj.data,
				sh_obj->lockobj.size);
		}

		switch (action) {
		case HEAD:
			SH_TAILQ_INSERT_HEAD(&sh_obj->waiters, newl, links,
			    __db_lock);
			break;
		case SECOND:
			SH_TAILQ_INSERT_AFTER(&sh_obj->waiters, lp, newl, links,
			    __db_lock);
			break;
		case TAIL:
			SH_TAILQ_INSERT_TAIL(&sh_obj->waiters, newl, links);
			break;
		default:
			DB_ASSERT(0);
		}

		/*
		 * This is really a blocker for the thread.  It should be
		 * initialized locked, so that when we try to acquire it, we
		 * block.
		 */
		newl->status = DB_LSTAT_WAITING;
		region->stat.st_nconflicts++;

		if (gbl_lock_conflict_trace) {
			static u_int32_t conftime = 0;
			u_int32_t now;

			__os_clock(dbenv, &now, NULL);

			if ((now - conftime) > 1) {
				logmsg(LOGMSG_USER, "st_nconflicts is %"PRId64"\n",
				    region->stat.st_nconflicts);
				conftime = now;
			}
		}

		region->need_dd = 1;

		/*
		 * First check to see if this txn has expired.
		 * If not then see if the lock timeout is past
		 * the expiration of the txn, if it is, use
		 * the txn expiration time.  lk_expire is passed
		 * to avoid an extra call to get the time.
		 */
		if (__lock_expired(dbenv,
			&sh_locker->lk_expire, &sh_locker->tx_expire)) {
			newl->status = DB_LSTAT_EXPIRED;
			sh_locker->lk_expire = sh_locker->tx_expire;

			/* We are done. */
			goto expired;
		}

		/*
		 * If a timeout was specified in this call then it
		 * takes priority.  If a lock timeout has been specified
		 * for this transaction then use that, otherwise use
		 * the global timeout value.
		 */
		if (!LF_ISSET(DB_LOCK_SET_TIMEOUT)) {
			if (F_ISSET(sh_locker, DB_LOCKER_TIMEOUT))
				timeout = sh_locker->lk_timeout;
			else
				timeout = region->lk_timeout;
		}
		if (timeout != 0)
			__lock_expires(dbenv, &sh_locker->lk_expire, timeout);
		else
			LOCK_SET_TIME_INVALID(&sh_locker->lk_expire);

		if (LOCK_TIME_ISVALID(&sh_locker->tx_expire) &&
		    (timeout == 0 || __lock_expired(dbenv,
			    &sh_locker->lk_expire, &sh_locker->tx_expire)))
			sh_locker->lk_expire = sh_locker->tx_expire;
		if (LOCK_TIME_ISVALID(&sh_locker->lk_expire) &&
		    (!LOCK_TIME_ISVALID(&region->next_timeout) ||
			LOCK_TIME_GREATER(&region->next_timeout,
			    &sh_locker->lk_expire)))
			region->next_timeout = sh_locker->lk_expire;

		/* set waiting status for master_locker */
		if (sh_locker->master_locker == INVALID_ROFF)
			sh_locker->wstatus = 1;
		else
			((DB_LOCKER *)R_ADDR(&lt->reginfo,
				sh_locker->master_locker))->wstatus = 1;

		unlock_locker_partition(region, lpartition);

		/* Return 'deadlock' for all holders of this lockobj */
		if (LF_ISSET(DB_LOCK_LOGICAL)) {
			/* Abort holders releases the object lock */
			if (0 != (ret = __dd_abort_holders(dbenv, sh_obj))) {
				abort();
			}
		} else {
			unlock_obj_partition(region, partition);
		}



		/* If we are switching drop the lock we had. */
		if (LF_ISSET(DB_LOCK_SWITCH) &&
		    (ret = __lock_put_nolock(dbenv,
			    lock, &ihold, DB_LOCK_NOWAITERS)) != 0) {
			lock_locker_partition(region, lpartition);
			lock_obj_partition(region, partition);
			__lock_remove_waiter(lt, sh_obj, newl, DB_LSTAT_FREE);
			goto err;
		}

		/* Light a flag in the holder's locker */
		if (holdarr) {
			u_int32_t locker_ndx;
			u_int32_t ii;
			DB_LOCKER *holder_locker;
			for (ii = 0; ii < holdix; ii++) {
				LOCKER_INDX(lt, region, holdarr[ii],
				    locker_ndx);
				if ((ret =
					__lock_getlocker(lt, holdarr[ii],
					    locker_ndx, GETLOCKER_KEEP_PART,
					    &holder_locker)) == 0 &&
				    holder_locker != NULL) {
					if (verbose_waiter)
						logmsg(LOGMSG_USER, 
                               "Set waitflag for lockid %u\n",
						    holdarr[ii]);
					holder_locker->has_waiters = 1;
					unlock_locker_partition(region,
					    holder_locker->partition);
				}
			}
			__os_free(dbenv, holdarr);
			holdarr = NULL;
		}

		/*
		 * We are about to wait; before waiting, see if the deadlock
		 * detector should be run.
		 */
		if (region->detect != DB_LOCK_NORUN && !no_dd)
			__lock_detect(dbenv, region->detect, NULL);

		if (gbl_bb_berkdb_enable_lock_timing) {
			x1 = bb_berkdb_fasttime();
		}
		MUTEX_LOCK(dbenv, &newl->mutex);

		if (gbl_bb_berkdb_enable_thread_stats) {
			struct berkdb_thread_stats *t;
			struct berkdb_thread_stats *p;
			if (gbl_bb_berkdb_enable_lock_timing) {
				x2 = bb_berkdb_fasttime();
			} else {
				x2 = x1;
			}
			t = bb_berkdb_get_thread_stats();
			p = bb_berkdb_get_process_stats();
            uint64_t d = (x2 - x1);
			p->lock_wait_time_us += d;
			if (p->worst_lock_wait_time_us < d)
				p->worst_lock_wait_time_us = d;
			p->n_lock_waits++;
			t->lock_wait_time_us += d;
			if (t->worst_lock_wait_time_us < d)
				t->worst_lock_wait_time_us = d;
			t->n_lock_waits++;
			t->n_locks++;
			p->n_locks++;

			if (gbl_bb_log_lock_waits_fn) {
				/* We had to wait on this lock - call our
				 * callback to record some basic info about
				 * the lock. */
				gbl_bb_log_lock_waits_fn(sh_obj->lockobj.data,
				    sh_obj->lockobj.size, U2M(x2 - x1));
			}
		}

		LOCKREGION(dbenv, (DB_LOCKTAB *)dbenv->lk_handle);
		lock_locker_partition(region, lpartition);
		lock_obj_partition(region, partition);

		/* Turn off lock timeout. */
		if (newl->status != DB_LSTAT_EXPIRED)
			LOCK_SET_TIME_INVALID(&sh_locker->lk_expire);

		if (newl->status != DB_LSTAT_PENDING) {
			switch (newl->status) {
			case DB_LSTAT_ABORTED:
				ret = DB_LOCK_DEADLOCK;
				break;
			case DB_LSTAT_NOTEXIST:
				ret = DB_LOCK_NOTEXIST;
				break;
			case DB_LSTAT_EXPIRED:
expired:			obj_ndx = sh_obj->index;
				if ((ret =
					__lock_put_internal(lt, newl, NULL,
					    obj_ndx, &region->need_dd,
					    DB_LOCK_UNLINK | DB_LOCK_FREE) !=
					0))
					goto err;
				if (LOCK_TIME_EQUAL(&sh_locker->lk_expire,
					&sh_locker->tx_expire)) {
					region->stat.st_ntxntimeouts++;
					unlock_obj_partition(region, partition);
					unlock_locker_partition(region,
					    lpartition);
					if (holdarr)
						__os_free(dbenv, holdarr);
					return (DB_LOCK_NOTGRANTED);
				} else {
					region->stat.st_nlocktimeouts++;
					unlock_obj_partition(region, partition);
					unlock_locker_partition(region,
					    lpartition);
					if (holdarr)
						__os_free(dbenv, holdarr);
					return (DB_LOCK_NOTGRANTED);
				}
			default:
				logmsg(LOGMSG_FATAL, "%s unexpected status:%d\n",
				    __func__, newl->status);
				abort();
			}
			goto err;
		} else if (LF_ISSET(DB_LOCK_UPGRADE)) {
			/*
			 * The lock that was just granted got put on the
			 * holders list.  Since we're upgrading some other
			 * lock, we've got to remove it here.
			 */
			SH_TAILQ_REMOVE(&sh_obj->holders, newl, links,
			    __db_lock);
			goto upgrade;
		} else
			newl->status = DB_LSTAT_HELD;
	}

	lock->off = R_OFFSET(&lt->reginfo, newl);
	lock->gen = newl->gen;
	lock->mode = newl->mode;
	sh_locker->nlocks++;


	/* clear waiting status for master_locker */
	if (sh_locker->master_locker == INVALID_ROFF)
		sh_locker->wstatus = 0;
	else
		((DB_LOCKER *)R_ADDR(&lt->reginfo,
			sh_locker->master_locker))->wstatus = 0;

	if (is_pagelock(sh_obj))
		sh_locker->npagelocks++;
	if ((handlelock = is_handlelock(sh_obj)))
		sh_locker->nhandlelocks++;
	if ((writelock = IS_WRITELOCK(newl->mode)))
		sh_locker->nwrites++;

	stack_at_get_lock(newl, lock, handlelock, writelock);

	if (is_pagelock(sh_obj) && IS_WRITELOCK(lock->mode) &&
	    F_ISSET(sh_locker, DB_LOCKER_TRACK_WRITELOCKS)) {
		if (sh_locker->ntrackedlocks + 1 > sh_locker->maxtrackedlocks) {
			struct __db_lock **oldlist =
			    sh_locker->tracked_locklist;
			sh_locker->maxtrackedlocks *= 2;
			if (__os_malloc(dbenv,
				sizeof(struct __db_lock *) *
				sh_locker->maxtrackedlocks,
				&(sh_locker->tracked_locklist)) != 0) {
				return ENOMEM;
			}
			memcpy(sh_locker->tracked_locklist, oldlist,
			    sizeof(struct __db_lock *) *
			    sh_locker->ntrackedlocks);
			__os_free(dbenv, oldlist);
		}
		sh_locker->tracked_locklist[sh_locker->ntrackedlocks++] = newl;
	}

	*in_locker = sh_locker;

	unlock_obj_partition(region, partition);
	unlock_locker_partition(region, lpartition);
	if (holdarr)
		__os_free(dbenv, holdarr);

	return (0);

done:
	ret = 0;
err:
	if (newl != NULL &&
	    (t_ret = __lock_freelock(lt, newl, sh_locker,
		    DB_LOCK_FREE | DB_LOCK_UNLINK)) != 0 && ret == 0) {
		if (sh_locker && F_ISSET(sh_locker, DB_LOCKER_TRACK))
		    logmsg(LOGMSG_ERROR, "__lock_get_internal_int():%d: ret was %d, t_ret is %d\n", __LINE__, ret, t_ret);
		ret = t_ret;
	}
	if (partition < gbl_lk_parts)
		unlock_obj_partition(region, partition);
	if (lpartition < gbl_lkr_parts)
		unlock_locker_partition(region, lpartition);
	*in_locker = sh_locker;
	if (holdarr)
		__os_free(dbenv, holdarr);

	return (ret);
}

/* Return 1 if this lockid holds this lockobj in this lock_mode, 0 otherwise */
static inline int
__lock_query_internal(lt, locker, obj, lock_mode)
	DB_LOCKTAB *lt;
	u_int32_t locker;
	const DBT *obj;
	db_lockmode_t lock_mode;
{
	int ret = 0;
	u_int32_t partition = gbl_lk_parts, lpartition = gbl_lkr_parts;
	DB_ENV *dbenv = lt->dbenv;
	DB_LOCKREGION *region = lt->reginfo.primary;
	DB_LOCKER *sh_locker;

	if (unlikely(F_ISSET(dbenv, DB_ENV_NOLOCKING)))
		return (0);

	u_int32_t locker_ndx;
	u_int32_t gl_flags = GETLOCKER_KEEP_PART;
	LOCKER_INDX(lt, region, locker, locker_ndx);
	if ((ret = __lock_getlocker(lt, locker, locker_ndx,
					gl_flags, &sh_locker)) != 0 || sh_locker == NULL) {
		logmsg(LOGMSG_DEBUG, "Locker %u does not exist\n", locker);
		goto out;
	}
	lpartition = sh_locker->partition;

	DB_LOCKOBJ *sh_obj;
	u_int32_t object_ndx;
	OBJECT_INDX(lt, region, obj, object_ndx, partition);
	lock_obj_partition(region, partition);
	if ((ret = __lock_getobj(lt, obj, object_ndx, partition, 0, &sh_obj)) != 0
			|| sh_obj == NULL) {
		logmsg(LOGMSG_DEBUG, "Lockobj does not exist\n");
		goto out;
	}

	struct __db_lock *lp;
	for (lp = SH_TAILQ_FIRST(&sh_obj->holders, __db_lock); lp != NULL;
			lp = SH_TAILQ_NEXT(lp, links, __db_lock)) {
		if (locker == lp->holderp->id && lp->mode == lock_mode &&
				lp->status == DB_LSTAT_HELD) {
			ret = 1;
			break;
		}
	}

out:
	if (partition < gbl_lk_parts)
		unlock_obj_partition(region, partition);
	if (lpartition < gbl_lkr_parts)
		unlock_locker_partition(region, lpartition);
	return ret;
}

static int
__lock_query(dbenv, locker, obj, lock_mode)
	DB_ENV *dbenv;
	u_int32_t locker;
	const DBT *obj;
	db_lockmode_t lock_mode;
{
	int ret;
	LOCKREGION(dbenv, (DB_LOCKTAB *)dbenv->lk_handle);
	ret = __lock_query_internal(dbenv->lk_handle, locker, obj, lock_mode);
	UNLOCKREGION(dbenv, (DB_LOCKTAB *)dbenv->lk_handle);
	return (ret);
}

/*
 * __lock_query_pp --
 *	DB_ENV->lock_query pre/post processing.
 *
 * PUBLIC: int __lock_query_pp __P((DB_ENV *,
 * PUBLIC:	 u_int32_t, const DBT *, db_lockmode_t));
 */
int
__lock_query_pp(dbenv, locker, obj, lock_mode)
	DB_ENV *dbenv;
	u_int32_t locker;
	const DBT *obj;
	db_lockmode_t lock_mode;
{
	int rep_check, ret;
	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
		dbenv->lk_handle, "DB_ENV->lock_get", DB_INIT_LOCK);
	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __lock_query(dbenv, locker, obj, lock_mode);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

static inline int
__lock_get_internal(lt, locker, sh_locker, flags, obj, lock_mode, timeout, lock)
	DB_LOCKTAB *lt;
	u_int32_t locker;
	DB_LOCKER *sh_locker;
	u_int32_t flags;
	const DBT *obj;
	db_lockmode_t lock_mode;
	db_timeout_t timeout;
	DB_LOCK *lock;
{
	int rc, use_latch = 0;

	if (use_page_latches(lt->dbenv)) {
		if (obj) {
			if (obj->size == sizeof(DB_LOCK_ILOCK)) {
				DB_LOCK_ILOCK *ilock =
				    (DB_LOCK_ILOCK *)obj->data;
				if (ilock->type == DB_PAGE_LOCK)
					use_latch = 1;
			}
		} else {
			if (LOCK_ISLATCH(*lock))
				use_latch = 1;
		}
	}

	if (use_latch) {
		rc = __get_page_latch(lt, locker, flags, obj, lock_mode, lock);
	} else {
		rc = __lock_get_internal_int(lt, locker, &sh_locker, flags, obj,
		    lock_mode, timeout, lock);
	}

	if (sh_locker && F_ISSET(sh_locker, DB_LOCKER_TRACK)) {
		struct __db_lock *lockp;
		lockp = (struct __db_lock *)R_ADDR(&lt->reginfo, lock->off);
		logmsg(LOGMSG_USER, "LOCKID %u rc %d ", sh_locker->id, rc);
		if (rc == 0) {
			__lock_printlock(lt, lockp, 1, stderr);
		} else
			logmsg(LOGMSG_ERROR, "RC %d FROM GETLOCK, LOCK-SIZE IS %d\n",
			    rc, obj->size);

	}
	return rc;
}


/*
 * __lock_put_pp --
 *	DB_ENV->lock_put pre/post processing.
 *
 * PUBLIC: int  __lock_put_pp __P((DB_ENV *, DB_LOCK *));
 */
int
__lock_put_pp(dbenv, lock)
	DB_ENV *dbenv;
	DB_LOCK *lock;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "DB_LOCK->lock_put", DB_INIT_LOCK);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __lock_put(dbenv, lock);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __lock_put --
 *	DB_ENV->lock_put.
 *
 * PUBLIC: int  __lock_put __P((DB_ENV *, DB_LOCK *));
 */
int
__lock_put(dbenv, lock)
	DB_ENV *dbenv;
	DB_LOCK *lock;
{
	DB_LOCKTAB *lt;

	int ret;
	u_int32_t run_dd = 0;

	lt = dbenv->lk_handle;

	LOCKREGION(dbenv, lt);
	ret = __lock_put_nolock(dbenv, lock, &run_dd, 0);
	UNLOCKREGION(dbenv, lt);

	/*
	 * Only run the lock detector if put told us to AND we are running
	 * in auto-detect mode.  If we are not running in auto-detect, then
	 * a call to lock_detect here will 0 the need_dd bit, but will not
	 * actually abort anything.
	 */
	if (ret == 0 && run_dd)
		__lock_detect(dbenv,
		    ((DB_LOCKREGION *)lt->reginfo.primary)->detect, NULL);

	return (ret);
}

static int
__lock_put_nolock(dbenv, lock, runp, flags)
	DB_ENV *dbenv;
	DB_LOCK *lock;
	u_int32_t *runp;
	u_int32_t flags;
{
	struct __db_lock *lockp;
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	DB_LOCKER *sh_locker;
	int ret;

	/* Check if locks have been globally turned off. */
	if (F_ISSET(dbenv, DB_ENV_NOLOCKING))
		return (0);

	if (LOCK_ISLATCH(*lock)) {
		ret = __put_page_latch(dbenv, lock);
		if (runp)
			*runp = 0;
		return ret;
	}

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;

	lockp = (struct __db_lock *)R_ADDR(&lt->reginfo, lock->off);
	sh_locker = lockp->holderp;

	u_int32_t lpartition = sh_locker->partition;
	u_int32_t partition = lock->partition;

	lock_locker_partition(region, lpartition);
	lock_obj_partition(region, partition);

	LOCK_INIT(*lock);
	if (lock->gen != lockp->gen) {
		__db_err(dbenv, __db_lock_invalid, "DB_LOCK->lock_put");
		abort();
#ifndef TESTSUITE
		cheap_stack_trace();
#endif
		ret = EINVAL;
		goto out;
	}
	ret = __lock_put_internal(lt,
	    lockp, lock, lock->ndx, runp,
	    flags | DB_LOCK_UNLINK | DB_LOCK_FREE);

out:	unlock_obj_partition(region, partition);
	unlock_locker_partition(region, lpartition);
	return (ret);
}

/*
 * __lock_downgrade --
 *
 * Used to downgrade locks.  Currently this is used in two places: 1) by the
 * Concurrent Data Store product to downgrade write locks back to iwrite locks
 * and 2) to downgrade write-handle locks to read-handle locks at the end of
 * an open/create.
 *
 * PUBLIC: int __lock_downgrade __P((DB_ENV *,
 * PUBLIC:     DB_LOCK *, db_lockmode_t, u_int32_t));
 */
int
__lock_downgrade(dbenv, lock, new_mode, flags)
	DB_ENV *dbenv;
	DB_LOCK *lock;
	db_lockmode_t new_mode;
	u_int32_t flags;
{
	struct __db_lock *lockp;
	DB_LOCKER *sh_locker;
	DB_LOCKOBJ *obj;
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	u_int32_t partition;
	int ret;
	int state_changed;

	COMPQUIET(flags, 0);

	PANIC_CHECK(dbenv);
	ret = 0;

	/* Check if locks have been globally turned off. */
	if (F_ISSET(dbenv, DB_ENV_NOLOCKING))
		return (0);

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;
	partition = lock->partition;

	LOCKREGION(dbenv, lt);

	lockp = (struct __db_lock *)R_ADDR(&lt->reginfo, lock->off);
	if (lock->gen != lockp->gen) {
		__db_err(dbenv, __db_lock_invalid, "lock_downgrade");
		ret = EINVAL;
		goto out;
	}

	sh_locker = lockp->holderp;
	if (sh_locker->partition > gbl_lkr_parts)
		goto out;
	lock_locker_partition(region, sh_locker->partition);
	if (IS_WRITELOCK(lockp->mode) && !IS_WRITELOCK(new_mode))
		sh_locker->nwrites--;

	if (new_mode == DB_LOCK_WWRITE)
		F_SET(sh_locker, DB_LOCKER_DIRTY);

	lockp->mode = new_mode;
	lock->mode = new_mode;

	unlock_locker_partition(region, sh_locker->partition);

	/* Get the object associated with this lock. */
	obj = lockp->lockobj;

	assert(partition == obj->partition);

	lock_obj_partition(region, partition);
	__lock_promote(lt, obj, &state_changed, LF_ISSET(DB_LOCK_NOWAITERS));
	unlock_obj_partition(region, partition);

out:	UNLOCKREGION(dbenv, lt);
	return (ret);
}

static int
__lock_put_internal(lt, lockp, lock, obj_ndx, need_dd, flags)
	DB_LOCKTAB *lt;
	struct __db_lock *lockp;
	DB_LOCK *lock;
	u_int32_t obj_ndx;
	u_int32_t *need_dd;
	u_int32_t flags;
{
	DB_LOCKOBJ *sh_obj;
	DB_LOCKREGION *region;
	int ret, state_changed;
	u_int32_t partition;
	DB_ENV *dbenv = lt->dbenv;

	region = lt->reginfo.primary;
	ret = state_changed = 0;

	if (!OBJ_LINKS_VALID(lockp, links)) {
		/*
		 * Someone removed this lock while we were doing a release
		 * by locker id.  We are trying to free this lock, but it's
		 * already been done; all we need to do is return it to the
		 * free list.
		 */
		(void)__lock_freelock(lt, lockp, lockp->holderp, DB_LOCK_FREE);
		return (0);
	}

	if (LF_ISSET(DB_LOCK_DOALL))
		region->stat.st_nreleases += lockp->refcount;
	else
		region->stat.st_nreleases++;

	if (!LF_ISSET(DB_LOCK_DOALL) && lockp->refcount > 1) {
		lockp->refcount--;
		return (0);
	}
	DB_LOCKER *sh_locker = lockp->holderp;

	if (is_pagelock(lockp->lockobj) && IS_WRITELOCK(lockp->mode) &&
			F_ISSET(sh_locker, DB_LOCKER_TRACK_WRITELOCKS)) {
		for (int i = 0; i < sh_locker->ntrackedlocks; i++) {
			if (sh_locker->tracked_locklist[i] == lockp) {
				struct __db_lock *last = sh_locker->tracked_locklist[
					sh_locker->ntrackedlocks - 1];
				sh_locker->tracked_locklist[i] = last;
				sh_locker->ntrackedlocks--;
				i--;
			}
		}
	}

	/* Increment generation number. */
	lockp->gen++;

	stack_at_gen_increment(lockp, lock);

	/* Get the object associated with this lock. */
	sh_obj = lockp->lockobj;
	partition = sh_obj->partition;


#ifdef DEBUG_LOCKS
	DB_LOCKER *mlockerp = R_ADDR(&lt->reginfo, sh_locker->master_locker);
	logmsg(LOGMSG_ERROR, "%p Put locker (%c) lock %x (m %x)\n",
			(void *)pthread_self(), lockp->mode == DB_LOCK_READ? 'R':'W', sh_locker->id,
			mlockerp->id);
	cheap_stack_trace();

	char desc[100];
	int idx;
	DBT dbt = { 0 };

	dbt.data = sh_obj->lockobj.data;
	dbt.size = sh_obj->lockobj.size;

	bdb_describe_lock_dbt(dbenv, &dbt, desc, sizeof(desc));

	Pthread_mutex_lock(&lblk);
	idx = lbcounter;
	lbcounter = (lbcounter + 1) % LKBUFMAX;
	Pthread_mutex_unlock(&lblk);

	threadid[idx] = pthread_self();
	snprintf(lkbuffer[idx], LKBUFSZ, "%p put lid %x %s size=%d",
			(void *)pthread_self(), sh_locker->id, desc, sh_obj->lockobj.size);
	logmsg(LOGMSG_ERROR, "%p __lock_put_internal: put lid %x %s size=%d\n",
			(void *)pthread_self(), sh_locker->id, desc, sh_obj->lockobj.size);
#endif




	/* Remove this lock from its holders/waitlist. */
	if (lockp->status != DB_LSTAT_HELD && lockp->status != DB_LSTAT_PENDING)
		__lock_remove_waiter(lt, sh_obj, lockp, DB_LSTAT_FREE);
	else
		SH_TAILQ_REMOVE(&sh_obj->holders, lockp, links, __db_lock);

	if (LF_ISSET(DB_LOCK_NOPROMOTE))
		state_changed = 0;
	else
		__lock_promote(lt, sh_obj, &state_changed,
		    LF_ISSET(DB_LOCK_REMOVE | DB_LOCK_NOWAITERS));

	/* Check if object should be reclaimed. */
	if (SH_TAILQ_FIRST(&sh_obj->holders, __db_lock) == NULL &&
	    SH_TAILQ_FIRST(&sh_obj->waiters, __db_lock) == NULL) {
		HASHREMOVE_EL(region->obj_tab[partition],
		    obj_ndx, __db_lockobj, links, sh_obj);
		if (sh_obj->lockobj.size > sizeof(sh_obj->objdata)) {
			__os_free(dbenv, sh_obj->lockobj.data);
		}

		SH_TAILQ_INSERT_HEAD(&region->free_objs[partition], sh_obj,
		    links, __db_lockobj);

		region->stat.st_nobjects--;
		state_changed = 1;
		++sh_obj->generation;
	}

	/* Free lock. */
	if (LF_ISSET(DB_LOCK_UNLINK | DB_LOCK_FREE))
		ret = __lock_freelock(lt, lockp, lockp->holderp, flags);

	/*
	 * If we did not promote anyone; we need to run the deadlock
	 * detector again.
	 */
	if (state_changed == 0 || region->need_dd) {
		*need_dd = 1;
	}

	return (ret);
}

/*
 * Utility functions; listed alphabetically.
 */

/*
 * __lock_freelock --
 *	Free a lock.  Unlink it from its locker if necessary.
 *
 */
static int
__lock_freelock(lt, lockp, sh_locker, flags)
	DB_LOCKTAB *lt;
	struct __db_lock *lockp;
	DB_LOCKER *sh_locker;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	DB_LOCKREGION *region;
	int ret;
	struct __db_lock_lsn *lp_lsn, *next_lsn;
	lp_lsn = next_lsn = NULL;

	dbenv = lt->dbenv;
	region = lt->reginfo.primary;
	ret = 0;

	if (LF_ISSET(DB_LOCK_UNLINK)) {
#ifdef LOCKMGRDBG
		if (region->locker_tab_mtx[sh_locker->partition].lock.thd !=
		    pthread_self()) {
			//Fail fast for now - want to catch it doing this
			puts("UNLINKING WITHOUT LOCKING LOCKER");
			abort();
		}
#endif
		SH_LIST_REMOVE(lockp, locker_links, __db_lock);
		if (lockp->status == DB_LSTAT_HELD) {
			DB_LOCKOBJ *sh_obj;
			sh_locker->nlocks--;
			sh_obj = lockp->lockobj;
			if (is_handlelock(sh_obj))
				sh_locker->nhandlelocks--;
			if (is_pagelock(sh_obj))
				sh_locker->npagelocks--;
			if (IS_WRITELOCK(lockp->mode))
				sh_locker->nwrites--;
		}
	}

	if (LF_ISSET(DB_LOCK_FREE)) {
#ifdef LOCKMGRDBG
		if (region->obj_tab_mtx[lockp->lpartition].lock.thd !=
		    pthread_self()) {
			//Fail fast for now - want to catch it doing this
			puts("FREEING WITHOUT LOCKING OBJ");
			abort();
		}
#endif
		lockp->status = DB_LSTAT_FREE;
		Pthread_mutex_lock(&lockp->lsns_mtx);
		for (lp_lsn = SH_LIST_FIRST(&lockp->lsns, __db_lock_lsn);
		    lp_lsn != NULL; lp_lsn = next_lsn) {
			next_lsn =
			    SH_LIST_NEXT(lp_lsn, lsn_links, __db_lock_lsn);
			SH_LIST_REMOVE(lp_lsn, lsn_links, __db_lock_lsn);
			__deallocate_db_lock_lsn(dbenv, lp_lsn);
		}
		lockp->nlsns = 0;
		SH_LIST_INIT(&lockp->lsns);
		Pthread_mutex_unlock(&lockp->lsns_mtx);
		SH_TAILQ_INSERT_HEAD(&region->free_locks[lockp->lpartition],
		    lockp, links, __db_lock);
		region->stat.st_nlocks--;
#ifndef TESTSUITE
		if (gbl_berkdb_track_locks)
			thread_remove_resource(lockp->dbtobj, free);
#endif
	}

	return (ret);
}

/*
 * __lock_addfamilylocker_int
 *	Put a locker entry in for a child transaction.
 *
 * PUBLIC: int __lock_addfamilylocker_int __P((DB_ENV *, u_int32_t, u_int32_t, struct txn_properties *));
 */
int
__lock_addfamilylocker_int(dbenv, pid, id, prop)
	DB_ENV *dbenv;
	u_int32_t pid, id;
    struct txn_properties *prop;
{
	DB_LOCKER *lockerp, *mlockerp;
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	u_int32_t ndx;
	int ret;

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;
	LOCKREGION(dbenv, lt);

	/* get/create the  parent locker info */
	LOCKER_INDX(lt, region, pid, ndx);
	if ((ret = __lock_getlocker_with_prop(dbenv->lk_handle,
		    pid, ndx, prop, GETLOCKER_CREATE, &mlockerp)) != 0)
		goto err;

	/*
	 * We assume that only one thread can manipulate
	 * a single transaction family.
	 * Therefore the master locker cannot go away while
	 * we manipulate it, nor can another child in the
	 * family be created at the same time.
	 */
	LOCKER_INDX(lt, region, id, ndx);
	if ((ret = __lock_getlocker_with_prop(dbenv->lk_handle,
		    id, ndx, prop, GETLOCKER_CREATE, &lockerp)) != 0)
		goto err;

	/* Point to our parent. */
	lockerp->parent_locker = R_OFFSET(&lt->reginfo, mlockerp);

	/* See if this locker is the family master. */
	if (mlockerp->master_locker == INVALID_ROFF)
		lockerp->master_locker = R_OFFSET(&lt->reginfo, mlockerp);
	else {
		lockerp->master_locker = mlockerp->master_locker;
		mlockerp = R_ADDR(&lt->reginfo, mlockerp->master_locker);
	}

#ifdef TEST_DEADLOCKS
	printf("%d %s:%d lockerid %x(%x) has parent %x(%x) and master %x\n",
	    pthread_self(), __FILE__, __LINE__,
	    lockerp->id, id,
	    ((DB_LOCKER *)R_ADDR(&lt->reginfo, lockerp->parent_locker))->id,
	    pid,
	    ((DB_LOCKER *)R_ADDR(&lt->reginfo, lockerp->master_locker))->id);
#endif

	/*
	 * Link the child at the head of the master's list.
	 * The guess is when looking for deadlock that
	 * the most recent child is the one thats blocked.
	 */
	SH_LIST_INSERT_HEAD(&mlockerp->child_locker, lockerp, child_link,
	    __db_locker);

err:
	UNLOCKREGION(dbenv, lt);

	return (ret);
}

int
__lock_addfamilylocker(dbenv, pid, id)
	DB_ENV *dbenv;
	u_int32_t pid, id;
{
    struct txn_properties prop = {0};
	return __lock_addfamilylocker_int(dbenv, pid, id, 0);
}

// PUBLIC: int __lock_add_child_locker_pp __P((DB_ENV *, u_int32_t, u_int32_t));
int
__lock_add_child_locker_pp(dbenv, pid, id)
	DB_ENV *dbenv;
	u_int32_t pid, id;
{
	int rep_check, ret;
	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "DB_ENV->lock_add_child_locker", DB_INIT_LOCK);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __lock_addfamilylocker(dbenv, pid, id);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * COMDB2 MODIFICATION
 *
 * PUBLIC: int __lock_addfamilylocker_with_prop __P((DB_ENV *, u_int32_t, u_int32_t, struct txn_properties *));
 */
int
__lock_addfamilylocker_with_prop(dbenv, pid, id, prop)
	DB_ENV *dbenv;
	u_int32_t pid, id;
    struct txn_properties *prop;
{
	return __lock_addfamilylocker_int(dbenv, pid, id, prop);
}


// PUBLIC: int __lock_locker_set_lowpri __P((DB_ENV *, u_int32_t));
int
__lock_locker_set_lowpri(dbenv, locker)
	DB_ENV *dbenv;
	u_int32_t locker;
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKER *sh_locker;
	DB_LOCKREGION *region;
	u_int32_t indx;
	int ret;

	dbenv = lt->dbenv;
	region = lt->reginfo.primary;

	LOCKREGION(dbenv, lt);
	LOCKER_INDX(lt, region, locker, indx);

	u_int32_t flags = GETLOCKER_CREATE;
	if ((ret = __lock_getlocker(lt,
		    locker, indx, flags, &sh_locker)) != 0 ||
	    sh_locker == NULL) {
		logmsg(LOGMSG_ERROR, "EORROR getting locker %s:%d locker %x \n", __FILE__,
		    __LINE__, locker);
	} else
		F_SET(sh_locker, DB_LOCKER_KILLME);

	UNLOCKREGION(dbenv, lt);
	return (ret);
}




/*
 * __lock_locker_getpriority
 *	Retrieve the priority set for a specific lockerid.
 *
 * This must be called without the locker bucket locked.
 *
 * NOTE: carved after __lock_freefamilylocker;
 *
 * PUBLIC: int __lock_locker_getpriority  __P((DB_LOCKTAB *, u_int32_t, int*));
 */
int
__lock_locker_getpriority(lt, locker, priority)
	DB_LOCKTAB *lt;
	u_int32_t locker;
	int *priority;
{
	DB_ENV *dbenv;
	DB_LOCKER *sh_locker;
	DB_LOCKREGION *region;
	u_int32_t indx;
	int ret;

	if (!priority)
		return 0;
	*priority = 0;

	dbenv = lt->dbenv;
	region = lt->reginfo.primary;

	LOCKREGION(dbenv, lt);
	LOCKER_INDX(lt, region, locker, indx);

	u_int32_t flags = GETLOCKER_KEEP_PART;
	if ((ret = __lock_getlocker(lt,
		    locker, indx, flags, &sh_locker)) != 0 ||
	    sh_locker == NULL)
		goto err;
#if 0
NOTE:	THIS IS USEFUL ONLY FOR CHILD TRANSACTIONS !
	    if (sh_locker->master_locker != INVALID_ROFF) {
		printf("%s:%d %d calling %s for child transaction???\n",
		    __FILE__, __LINE__, locker, __func__);
	}
#endif
	if (verbose_deadlocks)
		logmsg(LOGMSG_USER, "%s:%d locker %x retries %d priority %d\n",
		    __FILE__, __LINE__, locker, sh_locker->num_retries, sh_locker->priority);
	*priority = sh_locker->priority;
	unlock_locker_partition(region, sh_locker->partition);

err:
	UNLOCKREGION(dbenv, lt);
	return (ret);
}


/*
 * __lock_freefamilylocker
 *	Remove a locker from the hash table and its family.
 *
 * This must be called without the locker bucket locked.
 *
 * PUBLIC: int __lock_freefamilylocker  __P((DB_LOCKTAB *, u_int32_t));
 */
int
__lock_freefamilylocker(lt, locker)
	DB_LOCKTAB *lt;
	u_int32_t locker;
{
	DB_ENV *dbenv;
	DB_LOCKER *sh_locker;
	DB_LOCKREGION *region;
	u_int32_t indx, partition;
	int ret;

	dbenv = lt->dbenv;
	region = lt->reginfo.primary;

	__free_latch_lockerid(lt->dbenv, locker);

	lock_lockers(region);
	LOCKREGION(dbenv, lt);
	LOCKER_INDX(lt, region, locker, indx);

	u_int32_t flags = GETLOCKER_KEEP_PART;
	if ((ret = __lock_getlocker(lt,
		    locker, indx, flags, &sh_locker)) != 0 ||
	    sh_locker == NULL) {
		unlock_lockers(region);
		return ret;
	}

	partition = sh_locker->partition;

	if (SH_LIST_FIRST(&sh_locker->heldby, __db_lock) != NULL) {
		logmsg(LOGMSG_USER, "Dumping locks held by locker %u\n", locker);
		__lock_dump_locker_int(dbenv->lk_handle, sh_locker, stderr, 1);
		fflush(stderr);
		unlock_locker_partition(region, partition);
		unlock_lockers(region);
		__db_err(dbenv, "Freeing locker with locks");
#if defined (DEBUG_PREPARE)
        abort();
#endif
		return EINVAL;
	}

	/* If this is part of a family, we must fix up its links. */
	if (sh_locker->master_locker != INVALID_ROFF)
		SH_LIST_REMOVE(sh_locker, child_link, __db_locker);

	__lock_freelocker(lt, region, sh_locker, indx);

	unlock_locker_partition(region, partition);
	unlock_lockers(region);
	UNLOCKREGION(dbenv, lt);
	return (ret);
}

/*
 * __lock_freelocker
 *	common code for deleting a locker.
 *
 * This must be called with the locker bucket locked.
 */
static void
__lock_freelocker(lt, region, sh_locker, indx)
	DB_LOCKTAB *lt;
	DB_LOCKREGION *region;
	DB_LOCKER *sh_locker;
	u_int32_t indx;
{
	u_int32_t partition = sh_locker->partition;
	if (F_ISSET(sh_locker, DB_LOCKER_TRACK))
		logmsg(LOGMSG_USER, "LOCKID %u FREED\n", sh_locker->id);

	sh_locker->has_pglk_lsn = 0;
	sh_locker->ntrackedlocks = 0;
	sh_locker->maxtrackedlocks = 0;
	if (sh_locker->tracked_locklist)
		__os_free(lt->dbenv, sh_locker->tracked_locklist);
	sh_locker->tracked_locklist = NULL;

	HASHREMOVE_EL(region->locker_tab[partition], indx, __db_locker, links,
	    sh_locker);
	SH_TAILQ_INSERT_HEAD(&region->free_lockers[partition], sh_locker, links,
	    __db_locker);
	SH_TAILQ_REMOVE(&region->lockers, sh_locker, ulinks, __db_locker);
	region->stat.st_nlockers--;
}

/*
 * __lock_set_timeout
 *		-- set timeout values in shared memory.
 * This is called from the transaction system.
 * We either set the time that this tranaction expires or the
 * amount of time that a lock for this transaction is permitted
 * to wait.
 *
 * PUBLIC: int __lock_set_timeout __P(( DB_ENV *,
 * PUBLIC:      u_int32_t, db_timeout_t, u_int32_t));
 */
int
__lock_set_timeout(dbenv, locker, timeout, op)
	DB_ENV *dbenv;
	u_int32_t locker;
	db_timeout_t timeout;
	u_int32_t op;
{
	DB_LOCKTAB *lt;
	int ret;

	lt = dbenv->lk_handle;

	LOCKREGION(dbenv, lt);
	ret = __lock_set_timeout_internal(dbenv, locker, timeout, op);
	UNLOCKREGION(dbenv, lt);
	return (ret);
}
/*
 * __lock_set_timeout_internal
 *		-- set timeout values in shared memory.
 * This is the internal version called from the lock system.
 * We either set the time that this tranaction expires or the
 * amount of time that a lock for this transaction is permitted
 * to wait.
 *
 */
static int
__lock_set_timeout_internal(dbenv, locker, timeout, op)
	DB_ENV *dbenv;
	u_int32_t locker;
	db_timeout_t timeout;
	u_int32_t op;
{
	DB_LOCKER *sh_locker;
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	u_int32_t locker_ndx;
	int ret;

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;

	LOCKER_INDX(lt, region, locker, locker_ndx);
	u_int32_t flags = GETLOCKER_CREATE;
	ret = __lock_getlocker(lt, locker, locker_ndx, flags, &sh_locker);

	if (ret != 0)
		return (ret);

	if (op == DB_SET_TXN_TIMEOUT) {
		if (timeout == 0)
			LOCK_SET_TIME_INVALID(&sh_locker->tx_expire);
		else
			__lock_expires(dbenv, &sh_locker->tx_expire, timeout);
	} else if (op == DB_SET_LOCK_TIMEOUT) {
		sh_locker->lk_timeout = timeout;
		F_SET(sh_locker, DB_LOCKER_TIMEOUT);
	} else if (op == DB_SET_TXN_NOW) {
		LOCK_SET_TIME_INVALID(&sh_locker->tx_expire);
		__lock_expires(dbenv, &sh_locker->tx_expire, 0);
		sh_locker->lk_expire = sh_locker->tx_expire;
		if (!LOCK_TIME_ISVALID(&region->next_timeout) ||
		    LOCK_TIME_GREATER(&region->next_timeout,
			&sh_locker->lk_expire))
			region->next_timeout = sh_locker->lk_expire;
	} else
		return (EINVAL);

	return (0);
}

/*
 * __lock_inherit_timeout
 *		-- inherit timeout values from parent locker.
 * This is called from the transaction system.  This will
 * return EINVAL if the parent does not exist or did not
 * have a current txn timeout set.
 *
 * PUBLIC: int __lock_inherit_timeout __P(( DB_ENV *, u_int32_t, u_int32_t));
 */
int
__lock_inherit_timeout(dbenv, parent, locker)
	DB_ENV *dbenv;
	u_int32_t parent, locker;
{
	DB_LOCKER *parent_locker, *sh_locker;
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	u_int32_t locker_ndx;
	int ret;

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;
	ret = 0;
	LOCKREGION(dbenv, lt);

	/* If the parent does not exist, we are done. */
	LOCKER_INDX(lt, region, parent, locker_ndx);
	if ((ret = __lock_getlocker(lt,
		    parent, locker_ndx, 0, &parent_locker)) != 0)
		goto err;

	/*
	 * If the parent is not there yet, thats ok.  If it
	 * does not have any timouts set, then avoid creating
	 * the child locker at this point.
	 */
	if (parent_locker == NULL ||
	    (LOCK_TIME_ISVALID(&parent_locker->tx_expire) &&
		!F_ISSET(parent_locker, DB_LOCKER_TIMEOUT))) {
		ret = EINVAL;
		goto done;
	}

	LOCKER_INDX(lt, region, locker, locker_ndx);
	u_int32_t flags = GETLOCKER_CREATE;
	if ((ret = __lock_getlocker(lt,
		    locker, locker_ndx, flags, &sh_locker)) != 0)
		goto err;

	sh_locker->tx_expire = parent_locker->tx_expire;

	if (F_ISSET(parent_locker, DB_LOCKER_TIMEOUT)) {
		sh_locker->lk_timeout = parent_locker->lk_timeout;
		F_SET(sh_locker, DB_LOCKER_TIMEOUT);
		if (!LOCK_TIME_ISVALID(&parent_locker->tx_expire))
			ret = EINVAL;
	}

done:
err:
	UNLOCKREGION(dbenv, lt);
	return (ret);
}

/*
 * __lock_getlocker_int --
 *	Get a locker in the locker hash table.  The create parameter
 * indicates if the locker should be created if it doesn't exist in
 * the table.
 *
 * This must be called with the locker bucket locked.
 */
static int
__lock_getlocker_int(lt, locker, indx, partition, create, prop, retp,
    created, is_logical)
	DB_LOCKTAB *lt;
	u_int32_t locker, indx, partition;
	int create;
	struct txn_properties *prop;
	DB_LOCKER **retp;
	int *created;
	int is_logical;
{
	DB_ENV *dbenv;
	DB_LOCKER *sh_locker;
	DB_LOCKREGION *region;

	dbenv = lt->dbenv;
	region = lt->reginfo.primary;

	*created = 0;
	HASHLOOKUP(region->locker_tab[partition],
	    indx, __db_locker, links, locker, sh_locker, __lock_locker_cmp);

	/*
	 * If we found the locker, then we can just return it.  If
	 * we didn't find the locker, then we need to create it.
	 */
	if (sh_locker == NULL && create) {
		/* Create new locker and then insert it into hash table. */
		if ((sh_locker = SH_TAILQ_FIRST(&region->free_lockers[partition],
			    __db_locker)) == NULL) {
			unsigned i, num;
			++region->nwlkr_scale[partition];
			num = region->locker_p_size * region->nwlkr_scale[partition];
			PRINTF(nwlkr_scale, "add lkr:%d part:%d sc:%d\n",
			    num, partition, region->nwlkr_scale[partition]);
			int ret = __os_malloc(dbenv, sizeof(DB_LOCKER) * num, &sh_locker);
			if (ret != 0) {
				__db_err(dbenv, __db_lock_err,
				    "locker entries");
				return (ENOMEM);
			}
			sh_locker->has_pglk_lsn = 0;
			sh_locker->ntrackedlocks = 0;
			sh_locker->maxtrackedlocks = 0;
			sh_locker->tracked_locklist = NULL;
			for (i = 0; i < num; ++i, ++sh_locker)
				SH_TAILQ_INSERT_HEAD(&region->
				    free_lockers[partition], sh_locker, links,
				    __db_locker);
			sh_locker =
			    SH_TAILQ_FIRST(&region->free_lockers[partition],
			    __db_locker);
		}
		SH_TAILQ_REMOVE(&region->free_lockers[partition],
		    sh_locker, links, __db_locker);
		sh_locker->id = locker;
		sh_locker->dd_id = 0;
		sh_locker->master_locker = INVALID_ROFF;
		sh_locker->parent_locker = INVALID_ROFF;
		SH_LIST_INIT(&sh_locker->child_locker);
		sh_locker->flags = 0;
		SH_LIST_INIT(&sh_locker->heldby);
		sh_locker->nlocks = 0;
		sh_locker->npagelocks = 0;
		sh_locker->nhandlelocks = 0;
		sh_locker->nwrites = 0;
		sh_locker->has_waiters = 0;
		sh_locker->priority = prop ? prop->priority : 0;
		sh_locker->num_retries = prop ? prop->retries : 0;
		if (prop && prop->flags & DB_LOCK_ID_LOWPRI)
			F_SET(sh_locker, DB_LOCKER_KILLME);
#if TEST_DEADLOCKS
		printf("%p %s:%d lockerid %x setting priority to %d\n",
		    (void*)pthread_self(), __FILE__, __LINE__, sh_locker->id, sh_locker->priority);
#endif
		sh_locker->lk_timeout = 0;
		sh_locker->partition = partition;
		LOCK_SET_TIME_INVALID(&sh_locker->tx_expire);
		LOCK_SET_TIME_INVALID(&sh_locker->lk_expire);

		sh_locker->has_pglk_lsn = 0;
		sh_locker->ntrackedlocks = 0;
		int count = dbenv->attr.tracked_locklist_init > 0 ?
		    dbenv->attr.tracked_locklist_init : 10;
		sh_locker->maxtrackedlocks = count;
		if (__os_malloc(dbenv,
			sizeof(struct __db_lock *) * sh_locker->maxtrackedlocks,
			&(sh_locker->tracked_locklist)) != 0)
			return ENOMEM;
		sh_locker->tracked_locklist[0] = NULL;

		HASHINSERT(region->locker_tab[partition], indx, __db_locker,
		    links, sh_locker);
		*created = 1;
	}

	/* Upgrade this to a logical locker */
	if (sh_locker && is_logical)
		F_SET(sh_locker, DB_LOCKER_LOGICAL);

	/* heuristic: use create as hint that I will OWN this */
	if (sh_locker && create) {
		sh_locker->tid = pthread_self();

		sh_locker->snap_info = NULL;
		if (gbl_print_deadlock_cycles) {
			extern __thread snap_uid_t *osql_snap_info; /* contains cnonce */
			if(osql_snap_info)
				sh_locker->snap_info = osql_snap_info;
		}
	}

	*retp = sh_locker;
	return 0;
}

/* get locker: initialize with the provided retries and priority */
int
__lock_getlocker_with_prop(lt, locker, indx, prop, flags, retp)
	DB_LOCKTAB *lt;
	u_int32_t locker, indx;
	struct txn_properties *prop;
	u_int32_t flags;
	DB_LOCKER **retp;
{
	int created;
	int lk_create = LF_ISSET(GETLOCKER_CREATE);
	int is_logical = LF_ISSET(GETLOCKER_LOGICAL);
	u_int32_t keep_part_lock = LF_ISSET(GETLOCKER_KEEP_PART);
	u_int32_t get_locker_lock = !LF_ISSET(GETLOCKER_DONT_LOCK);

	DB_LOCKREGION *region = lt->reginfo.primary;
	u_int32_t partition = locker % gbl_lkr_parts;

	lock_locker_partition(region, partition);

	int ret = __lock_getlocker_int(lt, locker, indx, partition, lk_create,
                                   prop, retp, &created, is_logical);
    
	if (ret || *retp == NULL || keep_part_lock == 0) 
		unlock_locker_partition(region, partition);

	if(!created)
		return ret;

	if (get_locker_lock) {
		if (keep_part_lock) /* cant hold locker partition when locking lockers*/
			unlock_locker_partition(region, partition);
		lock_lockers(region);
	}
	SH_TAILQ_INSERT_HEAD(&region->lockers, *retp, ulinks, __db_locker);
	if (++region->stat.st_nlockers > region->stat.st_maxnlockers)
		region->stat.st_maxnlockers = region->stat.st_nlockers;
	if (get_locker_lock) {
		unlock_lockers(region);
		if (keep_part_lock) /* caller expects partition locked */
			lock_locker_partition(region, partition);
	}
	return ret;
}

/*
 * __lock_getlocker --
 *	Get a locker in the locker hash table.  The create parameter
 * indicates if the locker should be created if it doesn't exist in
 * the table.
 *
 * PUBLIC: int __lock_getlocker __P((DB_LOCKTAB *, u_int32_t, u_int32_t, u_int32_t, DB_LOCKER **));
 */

int
__lock_getlocker(lt, locker, indx, flags, retp)
	DB_LOCKTAB *lt;
	u_int32_t locker, indx;
	u_int32_t flags;
	DB_LOCKER **retp;
{
    return __lock_getlocker_with_prop(lt, locker, indx, NULL, flags, retp);
}



/*
 * __lock_getobj --
 *	Get an object in the object hash table.  The create parameter
 * indicates if the object should be created if it doesn't exist in
 * the table.
 *
 * This must be called with the object bucket locked.
 */
static int
__lock_getobj(lt, obj, ndx, partition, create, retp)
	DB_LOCKTAB *lt;
	const DBT *obj;
	u_int32_t ndx, partition;
	int create;
	DB_LOCKOBJ **retp;
{
	DB_ENV *dbenv;
	DB_LOCKOBJ *sh_obj;
	DB_LOCKREGION *region;
	int ret;
	void *p;

	dbenv = lt->dbenv;
	region = lt->reginfo.primary;

	/* Look up the object in the hash table. */
	HASHLOOKUP(region->obj_tab[partition],
	    ndx, __db_lockobj, links, obj, sh_obj, __lock_cmp);

	/*
	 * If we found the object, then we can just return it.  If
	 * we didn't find the object, then we need to create it.
	 */
	if (sh_obj == NULL && create) {
		/* Create new object and then insert it into hash table. */
		if ((sh_obj =
			SH_TAILQ_FIRST(&region->free_objs[partition],
			    __db_lockobj)) == NULL) {
			unsigned i, num;
			++region->nwobj_scale[partition];
			num = region->object_p_size
			    * region->nwobj_scale[partition];
			PRINTF(nwobj_scale, "add obj:%d to part:%d sc:%d\n",
			    num, partition, region->nwobj_scale[partition]);
			ret = __os_malloc(dbenv,
			    sizeof(DB_LOCKOBJ) * num, &sh_obj);
			if (ret != 0) {
				__db_err(lt->dbenv, __db_lock_err,
				    "object entries");
				ret = ENOMEM;
				goto err;
			}
			for (i = 0; i < num; ++i, ++sh_obj)
				SH_TAILQ_INSERT_HEAD(&region->
				    free_objs[partition], sh_obj, links,
				    __db_lockobj);
			sh_obj =
			    SH_TAILQ_FIRST(&region->free_objs[partition],
			    __db_lockobj);
		}
		SH_TAILQ_REMOVE(&region->free_objs[partition], sh_obj,
		    links, __db_lockobj);
		if (++region->stat.st_nobjects > region->stat.st_maxnobjects)
			region->stat.st_maxnobjects = region->stat.st_nobjects;
		/*
		 * If we can fit this object in the structure, do so instead
		 * of shalloc-ing space for it.
		 */
		if (obj->size <= sizeof(sh_obj->objdata)) {
			p = sh_obj->objdata;
		} else {
			ret = __os_malloc(dbenv, obj->size, &p);
			if (ret != 0) {
				__db_err(dbenv,
				    "No space for lock object storage");
				SH_TAILQ_INSERT_HEAD(&region->
				    free_objs[partition], sh_obj, links,
				    __db_lockobj);
				goto err;
			}
		}

		memcpy(p, obj->data, obj->size);

		SH_TAILQ_INIT(&sh_obj->waiters);
		SH_TAILQ_INIT(&sh_obj->holders);
		sh_obj->lockobj.size = obj->size;
		sh_obj->lockobj.data = p;
		sh_obj->partition = partition;
		sh_obj->index = ndx;

		HASHINSERT(region->obj_tab[partition], ndx, __db_lockobj, links,
		    sh_obj);
	}

	*retp = sh_obj;
	return (0);

err:	return (ret);
}

/*
 * __lock_is_parent --
 *	Given a locker and a transaction, return 1 if the locker is
 * an ancestor of the designcated transaction.  This is used to determine
 * if we should grant locks that appear to conflict, but don't because
 * the lock is already held by an ancestor.
 */
static int
__lock_is_parent(lt, locker, sh_locker)
	DB_LOCKTAB *lt;
	u_int32_t locker;
	DB_LOCKER *sh_locker;
{
	DB_LOCKER *parent = sh_locker;
	while (parent->parent_locker != INVALID_ROFF) {
		parent = (DB_LOCKER *)
		    R_ADDR(&lt->reginfo, parent->parent_locker);
		if (parent->id == locker)
			return (1);
	}
	return (0);
}

static int
__inherit_latches(DB_ENV *dbenv, u_int32_t locker, u_int32_t plocker,
    u_int32_t flags)
{
	DB_LOCKERID_LATCH_NODE *lid, *plid;
	DB_LATCH *latch, *pvlatch;
	int ret;

	if ((ret = __find_latch_lockerid(dbenv, locker, &lid,
		    GETLOCKER_CREATE)) != 0)
		abort();

	if ((ret = __find_latch_lockerid(dbenv, plocker, &plid,
		    GETLOCKER_CREATE)) != 0)
		abort();

#ifdef VERBOSE_LATCH
	u_int32_t parent_latches = __count_latches(dbenv, plid);
	u_int32_t child_latches = __count_latches(dbenv, lid);
#endif

	for (pvlatch = latch = lid->head; latch; latch = latch->next) {
		latch->lockerid = plocker;
		latch->locker = plid;
		assert(latch->tid == pthread_self());
		pvlatch = latch;
	}

	if (pvlatch) {
		pvlatch->next = plid->head;
		if (plid->head)
			plid->head->prev = pvlatch;
		plid->head = lid->head;
	}
#ifdef VERBOSE_LATCH
	u_int32_t total_latches = __count_latches(dbenv, plid);
	printf("plocker %u tid %u inherits locks from child-locker %u\n",
	    plocker, pthread_self(), locker);
	printf
	    ("parent-write-locks(orig) %u child-write-locks %u new-total %u\n",
	    parent_latches, child_latches, total_latches);
#endif

	lid->head = NULL;

	return (0);
}

/*
 * __lock_inherit_locks --
 * Called on child commit to merge child's locks with parent's.
 */
static int
__lock_inherit_locks(lt, locker, flags)
	DB_LOCKTAB *lt;
	u_int32_t locker;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	DB_LOCKER *sh_locker, *sh_parent;
	DB_LOCKOBJ *obj;
	DB_LOCKREGION *region;
	int ret;
	struct __db_lock *hlp, *lp;
	u_int32_t ndx, partition;
	int state_changed;

	region = lt->reginfo.primary;
	dbenv = lt->dbenv;

	/*
	 * Get the committing locker and mark it as deleted.
	 * This allows us to traverse the locker links without
	 * worrying that someone else is deleting locks out
	 * from under us.  However, if the locker doesn't
	 * exist, that just means that the child holds no
	 * locks, so inheritance is easy!
	 */
	LOCKER_INDX(lt, region, locker, ndx);
	if ((ret = __lock_getlocker(lt,
		    locker, ndx, GETLOCKER_KEEP_PART, &sh_locker)) != 0 ||
	    sh_locker == NULL || F_ISSET(sh_locker, DB_LOCKER_DELETED)) {
		if (sh_locker)
			unlock_locker_partition(region, sh_locker->partition);
		if (ret == 0 && sh_locker != NULL)
			ret = EINVAL;
		__db_err(dbenv, __db_locker_invalid);
		goto err;
	}

	/* Make sure we are a child transaction. */
	if (sh_locker->parent_locker == INVALID_ROFF) {
		__db_err(dbenv, "Not a child transaction");
		ret = EINVAL;
		goto err;
	}
	sh_parent = (DB_LOCKER *)R_ADDR(&lt->reginfo, sh_locker->parent_locker);
	F_SET(sh_locker, DB_LOCKER_DELETED);
	unlock_locker_partition(region, sh_locker->partition);

	/*
	 * Now, lock the parent locker; move locks from
	 * the committing list to the parent's list.
	 */
	lock_locker_partition(region, sh_parent->partition);
	if (F_ISSET(sh_parent, DB_LOCKER_DELETED)) {
		if (ret == 0) {
			unlock_locker_partition(region, sh_parent->partition);
			__db_err(dbenv, "Parent locker is not valid");
			ret = EINVAL;
		}
		goto err;
	}

	/*
	 * In order to make it possible for a parent to have
	 * many, many children who lock the same objects, and
	 * not require an inordinate number of locks, we try
	 * to merge the child's locks with its parent's.
	 */
	for (lp = SH_LIST_FIRST(&sh_locker->heldby, __db_lock);
	    lp != NULL; lp = SH_LIST_FIRST(&sh_locker->heldby, __db_lock)) {
		SH_LIST_REMOVE(lp, locker_links, __db_lock);

		/* See if the parent already has a lock. */
		obj = lp->lockobj;
		partition = obj->partition;

		lock_obj_partition(region, partition);

		for (hlp = SH_TAILQ_FIRST(&obj->holders, __db_lock);
		    hlp != NULL; hlp = SH_TAILQ_NEXT(hlp, links, __db_lock))
			if (hlp->holderp->id == sh_parent->id &&
			    lp->mode == hlp->mode)
				break;

		if (hlp != NULL) {
			/* Parent already holds lock. */
			hlp->refcount += lp->refcount;

			/* Remove lock from object list and free it. */
			DB_ASSERT(lp->status == DB_LSTAT_HELD);
			SH_TAILQ_REMOVE(&obj->holders, lp, links, __db_lock);
			(void)__lock_freelock(lt, lp, sh_locker, DB_LOCK_FREE);
		} else {
			/* Just move lock to parent chains. */
			SH_LIST_INSERT_HEAD(&sh_parent->heldby,
			    lp, locker_links, __db_lock);
			sh_parent->nlocks++;
			if (IS_WRITELOCK(lp->mode))
				sh_parent->nwrites++;
			if (is_pagelock(obj))
				sh_parent->npagelocks++;
			if (is_handlelock(obj))
				sh_parent->nhandlelocks++;
			lp->holderp = sh_parent;
		}

		/*
		 * We may need to promote regardless of whether we simply
		 * moved the lock to the parent or changed the parent's
		 * reference count, because there might be a sibling waiting,
		 * who will now be allowed to make forward progress.
		 */
		__lock_promote(lt, obj, &state_changed,
		    LF_ISSET(DB_LOCK_NOWAITERS));

		unlock_obj_partition(region, partition);
	}

	unlock_locker_partition(region, sh_parent->partition);

	if (use_page_latches(lt->dbenv)) {
		if ((ret = __inherit_latches(lt->dbenv, locker,
			    sh_parent->id, flags)) != 0)
			abort();
	}

err:	return (ret);
}

/*
 * __lock_promote --
 *
 * Look through the waiters and holders lists and decide which (if any)
 * locks can be promoted.   Promote any that are eligible.
 *
 * PUBLIC: int __lock_promote __P((DB_LOCKTAB *, DB_LOCKOBJ *, int *, u_int32_t));
 */
int
__lock_promote(lt, obj, changed, flags)
	DB_LOCKTAB *lt;
	DB_LOCKOBJ *obj;
	int *changed;
	u_int32_t flags;
{
	struct __db_lock *lp_w, *lp_h, *next_waiter;
	DB_LOCKER *sh_locker;
	DB_LOCKREGION *region;
	int had_waiters, state_changed;

	region = lt->reginfo.primary;
	had_waiters = 0;

	/*
	 * We need to do lock promotion.  We also need to determine if we're
	 * going to need to run the deadlock detector again.  If we release
	 * locks, and there are waiters, but no one gets promoted, then we
	 * haven't fundamentally changed the lockmgr state, so we may still
	 * have a deadlock and we have to run again.  However, if there were
	 * no waiters, or we actually promoted someone, then we are OK and we
	 * don't have to run it immediately.
	 *
	 * During promotion, we look for state changes so we can return this
	 * information to the caller.
	 */

	for (lp_w = SH_TAILQ_FIRST(&obj->waiters, __db_lock),
	    state_changed = lp_w == NULL; lp_w != NULL; lp_w = next_waiter) {
		had_waiters = 1;
		next_waiter = SH_TAILQ_NEXT(lp_w, links, __db_lock);

		/* Waiter may have aborted or expired. */
		if (lp_w->status != DB_LSTAT_WAITING)
			continue;
		/* Are we switching locks? */
		if (LF_ISSET(DB_LOCK_NOWAITERS) && lp_w->mode == DB_LOCK_WAIT)
			continue;

		if (LF_ISSET(DB_LOCK_REMOVE)) {
			__lock_remove_waiter(lt, obj, lp_w, DB_LSTAT_NOTEXIST);
			continue;
		}
		for (lp_h = SH_TAILQ_FIRST(&obj->holders, __db_lock);
		    lp_h != NULL;
		    lp_h = SH_TAILQ_NEXT(lp_h, links, __db_lock)) {
			if (lp_h->holderp->id != lp_w->holderp->id &&
			    CONFLICTS(lt, region, lp_h->mode, lp_w->mode)) {
				sh_locker = lp_w->holderp;
				if (!__lock_is_parent(lt,
					lp_h->holderp->id, sh_locker))
					break;
			}
		}
		if (lp_h != NULL)	/* Found a conflict. */
			break;

		/* No conflict, promote the waiting lock. */
		SH_TAILQ_REMOVE(&obj->waiters, lp_w, links, __db_lock);
		lp_w->status = DB_LSTAT_PENDING;
		SH_TAILQ_INSERT_TAIL(&obj->holders, lp_w, links);

		/* Wake up waiter. */
		MUTEX_UNLOCK(lt->dbenv, &lp_w->mutex);
		state_changed = 1;
	}

	/*
	 * If this object had waiters and doesn't any more, then we need
	 * to remove it from the dd_obj list.
	 */
	if (had_waiters && SH_TAILQ_FIRST(&obj->waiters, __db_lock) == NULL) {
		lock_detector(region);
		SH_TAILQ_REMOVE(&region->dd_objs, obj, dd_links, __db_lockobj);
		++obj->generation;
		unlock_detector(region);
	}
	*changed = state_changed;
	return (0);
}

/*
 * __lock_remove_waiter --
 *	Any lock on the waitlist has a process waiting for it.  Therefore,
 * we can't return the lock to the freelist immediately.  Instead, we can
 * remove the lock from the list of waiters, set the status field of the
 * lock, and then let the process waking up return the lock to the
 * free list.
 *
 * This must be called with the Object bucket locked.
 */
static void
__lock_remove_waiter(lt, sh_obj, lockp, status)
	DB_LOCKTAB *lt;
	DB_LOCKOBJ *sh_obj;
	struct __db_lock *lockp;
	db_status_t status;
{
	DB_LOCKREGION *region;
	int do_wakeup;

	region = lt->reginfo.primary;

	do_wakeup = lockp->status == DB_LSTAT_WAITING;

	SH_TAILQ_REMOVE(&sh_obj->waiters, lockp, links, __db_lock);
	lockp->status = status;
	if (SH_TAILQ_FIRST(&sh_obj->waiters, __db_lock) == NULL) {
		lock_detector(region);
		SH_TAILQ_REMOVE(&region->dd_objs,
		    sh_obj, dd_links, __db_lockobj);
		++sh_obj->generation;
		unlock_detector(region);
	}

	/*
	 * Wake whoever is waiting on this lock.
	 */
	if (do_wakeup)
		MUTEX_UNLOCK(lt->dbenv, &lockp->mutex);
}

/*
 * __lock_expires -- set the expire time given the time to live.
 * We assume that if timevalp is set then it contains "now".
 * This avoids repeated system calls to get the time.
 */
static void
__lock_expires(dbenv, timevalp, timeout)
	DB_ENV *dbenv;
	db_timeval_t *timevalp;
	db_timeout_t timeout;
{
	if (!LOCK_TIME_ISVALID(timevalp))
		__os_clock(dbenv, &timevalp->tv_sec, &timevalp->tv_usec);
	if (timeout > 1000000) {
		timevalp->tv_sec += timeout / 1000000;
		timevalp->tv_usec += timeout % 1000000;
	} else
		timevalp->tv_usec += timeout;

	if (timevalp->tv_usec > 1000000) {
		timevalp->tv_sec++;
		timevalp->tv_usec -= 1000000;
	}
}

/*
 * __lock_expired -- determine if a lock has expired.
 *
 * PUBLIC: int __lock_expired __P((DB_ENV *, db_timeval_t *, db_timeval_t *));
 */
int
__lock_expired(dbenv, now, timevalp)
	DB_ENV *dbenv;
	db_timeval_t *now, *timevalp;
{
	if (!LOCK_TIME_ISVALID(timevalp))
		return (0);

	if (!LOCK_TIME_ISVALID(now))
		__os_clock(dbenv, &now->tv_sec, &now->tv_usec);

	return (now->tv_sec > timevalp->tv_sec ||
	    (now->tv_sec == timevalp->tv_sec &&
		now->tv_usec >= timevalp->tv_usec));
}

/* Used for a limbo compensating txn.  Move the entire latch. */
static int
__latch_trade(DB_ENV *dbenv, DB_LATCH * latch, u_int32_t lockerid)
{
	DB_LOCKERID_LATCH_NODE *lidptr;
	int ret;

	if (latch->lockerid != lockerid) {
		if ((ret = __find_latch_lockerid(dbenv, lockerid, &lidptr,
			    GETLOCKER_CREATE)) != 0)
			abort();
		if (latch->prev)
			latch->prev->next = latch->next;
		if (latch->next)
			latch->next->prev = latch->prev;
		if (latch == latch->locker->head)
			latch->locker->head = latch->next;

		latch->locker = lidptr;
		latch->lockerid = lockerid;

		if (lidptr->head)
			lidptr->head->prev = latch;
		latch->next = lidptr->head;
		latch->prev = NULL;
		lidptr->head = latch;
	}
	return (0);
}


int
__latch_trade_comp(DB_ENV *dbenv, const DBT *obj, u_int32_t oldlocker,
    u_int32_t newlocker)
{
	u_int32_t idx;
	DB_LATCH *latch;
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;
	idx = latchhash(dbenv, obj);
	latch = &region->latches[idx];

	assert(latch->lockerid == oldlocker || latch->lockerid == newlocker);

	if (latch->lockerid == oldlocker) {
		return __latch_trade(dbenv, latch, newlocker);
	}

	return (0);
}

/*
 * __lock_trade --
 *
 * Trade locker ids on a lock.  This is used to reassign file locks from
 * a transactional locker id to a long-lived locker id.  This should be
 * called with the region mutex held.
 */
static int
__lock_trade(dbenv, lock, new_locker, create)
	DB_ENV *dbenv;
	DB_LOCK *lock;
	u_int32_t new_locker;
	u_int32_t create;
{
	struct __db_lock *lp;
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	DB_LOCKER *sh_locker;
	DB_LOCKOBJ *sh_obj;
	int ret;
	u_int32_t locker_ndx;
	u_int32_t lpartition = gbl_lkr_parts;

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;

	if (LOCK_ISLATCH(*lock)) {
		DB_ILOCK_LATCH *lnode = lock->ilock_latch;
		return __latch_trade(dbenv, lnode->latch, new_locker);
	}


	/* Make sure that we can get new locker and add this lock to it. */
	LOCKER_INDX(lt, region, new_locker, locker_ndx);
	if ((ret =
		__lock_getlocker(lt, new_locker, locker_ndx,
		    create ? GETLOCKER_CREATE : 0, &sh_locker)) != 0)
		goto out;

	if (sh_locker == NULL) {
		__db_err(dbenv, "Locker does not exist");
#ifndef TESTSUITE
		cheap_stack_trace();
#endif
		ret = EINVAL;
		abort();
		goto out;
	}

	lp = (struct __db_lock *)R_ADDR(&lt->reginfo, lock->off);
	DB_LOCKER *old_locker = lp->holderp;

	lock_locker_partition(region, old_locker->partition);
	lock_obj_partition(region, lock->partition);

	lpartition = old_locker->partition;

	/* If the lock is already released, simply return. */
	if (lp->gen != lock->gen) {
		ret = DB_NOTFOUND;
		goto unlock;
	}

	/* Remove the lock from its current locker. */
	if ((ret = __lock_freelock(lt, lp, lp->holderp, DB_LOCK_UNLINK)) != 0)
		goto out;

	/* Lock new locker's partition */
	if (sh_locker->partition != lpartition) {
		unlock_locker_partition(region, lpartition);
		unlock_obj_partition(region, lock->partition);
		lock_locker_partition(region, sh_locker->partition);
		lock_obj_partition(region, lock->partition);
		lpartition = sh_locker->partition;
		/* Re-doing this check */
		if (lp->gen != lock->gen) {
			ret = DB_NOTFOUND;
			goto unlock;
		}

	}

	/* Add lock to its new locker. */
	SH_LIST_INSERT_HEAD(&sh_locker->heldby, lp, locker_links, __db_lock);
	sh_locker->nlocks++;
	sh_obj = lp->lockobj;
	if (is_pagelock(sh_obj))
		sh_locker->npagelocks++;
	if (is_handlelock(sh_obj))
		sh_locker->nhandlelocks++;
	if (IS_WRITELOCK(lp->mode))
		sh_locker->nwrites++;
	lp->holderp = sh_locker;

unlock:unlock_obj_partition(region, lock->partition);
out:	if (lpartition < gbl_lkr_parts)
		unlock_locker_partition(region, lpartition);
	return ret;
}

/*
 * Lock list routines.
 *	The list is composed of a 32-bit count of locks followed by
 * each lock.  A lock is represented by a 16-bit page-count, a lock
 * object and a page list.  A lock object consists of a 16-bit size
 * and the object itself.   In a pseudo BNF notation, you get:
 *
 * LIST = COUNT32 LOCK*
 * LOCK = COUNT16 LOCKOBJ PAGELIST
 * LOCKOBJ = COUNT16 OBJ
 * PAGELIST = COUNT32*
 *
 * (Recall that X* means "0 or more X's")
 *
 * In most cases, the OBJ is a struct __db_ilock and the page list is
 * a series of (32-bit) page numbers that should get written into the
 * pgno field of the __db_ilock.  So, the actual number of pages locked
 * is the number of items in the PAGELIST plus 1. If this is an application-
 * specific lock, then we cannot interpret obj and the pagelist must
 * be empty.
 *
 * Consider a lock list for: File A, pages 1&2, File B pages 3-5, Applock
 * This would be represented as:
 *	5 1 [fid=A;page=1] 2 2 [fid=B;page=3] 4 5 0 APPLOCK
 *        ------------------ -------------------- ---------
 *         LOCK for file A    LOCK for file B     application-specific lock
 *
 * 
 * A rowlocks list has a different structure: 
 *
 * LIST = COUNT32 ROWLOCK*
 * ROWLOCK = COUNT32 FILEID20 GENIDLIST
 * GENIDLIST = [ MINLOCKGENID* | MAXLOCKGENID* | GENID64* ]
 *
 * The 'MINLOCKGENID' is 0, the 'MAXLOCKGENID' is 1.
 * Don't waste space storing size (they're derivable).
 */

#define MAX_PGNOS	0xffff
#define MAX_GENIDS      0xffffffff

/* 31-byte minmax lock */
struct __db_lock_minmax {
	u_int8_t fileid[DB_FILE_ID_LEN];	/* File id. */
	u_int8_t fluff[10];
	u_int8_t minmax;
};

typedef struct __db_lock_minmax DB_LOCK_MINMAX;

/* 30 byte rowlock */
struct __db_lock_rowlock {
	u_int8_t fileid[DB_FILE_ID_LEN];	/* File id. */
	u_int8_t fluff[2];
	u_int8_t genid[8];
};

typedef struct __db_lock_rowlock DB_LOCK_ROWLOCK;

struct __db_lock_idxlock {
	u_int8_t fileid[DB_FILE_ID_LEN];	/* File id. */
	u_int8_t hash[8];
};

typedef struct __db_lock_idxlock DB_LOCK_IDXLOCK;

/*
 * These macros are bigger than one might expect because the
 * Solaris compiler says that a cast does not return an lvalue,
 * so constructs like *(u_int32_t*)dp = count; generate warnings.
 */

#define PACKED_ROWLOCKS_SIZE(nfid, nlocks) (			\
		(sizeof(u_int32_t)) + (sizeof(u_int32_t)) + 	\
		((nfid) * (DB_FILE_ID_LEN + sizeof(u_int32_t))) + \
		((nlocks) * sizeof(u_int64_t)))

#define RET_SIZE(size, count)  ((size) + 			\
     sizeof(u_int32_t) + (count) * 2 * sizeof(u_int16_t))

#define PUT_NKEYS(dp, nkeys) 	do {	u_int32_t *ip = (u_int32_t *)dp;\
					*ip = nkeys;			\
                    if(LOG_SWAPPED())           \
                        M_32_SWAP(*ip);         \
					dp = (u_int8_t *)dp +		\
					     sizeof(u_int32_t);		\
				} while (0)

#define PUT_COUNT(dp, count) 	do {	u_int32_t *ip = (u_int32_t *)dp;\
					*ip = count;			\
                    if(LOG_SWAPPED())           \
                        M_32_SWAP(*ip);         \
					dp = (u_int8_t *)dp +		\
					     sizeof(u_int32_t);		\
				} while (0)
/* Put genid count */
#define PUT_GCOUNT(dp, count)   do {    u_int32_t *ip = (u_int32_t *)dp;\
					*ip = count;			\
					if(LOG_SWAPPED())           \
					M_32_SWAP(*ip);         \
					dp = (u_int8_t *)dp +		\
					    sizeof(u_int32_t);		\
				} while (0)

#define PUT_PCOUNT(dp, count) 	do {	u_int16_t *ip = (u_int16_t *)dp;\
					*ip = count;			\
                    if(LOG_SWAPPED())           \
                        M_16_SWAP(*ip);         \
					dp = (u_int8_t *)dp +		\
					    sizeof(u_int16_t);		\
				} while (0)
#define PUT_LSNCOUNT(dp, count) 	do {	u_int32_t *ip = (u_int32_t *)dp;\
					*ip = count;			\
                    if(LOG_SWAPPED())           \
                        M_32_SWAP(*ip);         \
					dp = (u_int8_t *)dp +		\
					    sizeof(u_int32_t);		\
				} while (0)
#define PUT_SIZE(dp, size) 	do {	u_int16_t *ip = (u_int16_t *)dp;\
					*ip = size;			\
                    if(LOG_SWAPPED())           \
                        M_16_SWAP(*ip);         \
					dp = (u_int8_t *)dp +		\
					    sizeof(u_int16_t);		\
				} while (0)
#define PUT_PGNO(dp, pgno) 	do {	db_pgno_t *ip = (db_pgno_t *)dp;\
					*ip = pgno;			\
                    if(LOG_SWAPPED())           \
                        M_32_SWAP(*ip);         \
					dp = (u_int8_t *)dp +		\
					    sizeof(db_pgno_t);		\
				} while (0)

#define PUT_FILEID(dp, obj)     do {					\
					memcpy(dp, (obj)->data,		\
						DB_FILE_ID_LEN); 	\
					dp = (u_int8_t *)dp + 		\
						DB_FILE_ID_LEN;		\
				} while (0)

#define PUT_MINMAX(dp, obj)	do {					\
					u_int64_t *gi = (u_int64_t *)dp;\
					DB_LOCK_MINMAX *mm = (DB_LOCK_MINMAX *)((obj)->data);\
					assert(mm->minmax == 0 || mm->minmax == 1); 	\
					if(mm->minmax == 0) *gi = 0;	\
					else *gi = -1;			\
					dp = (u_int8_t *)dp + 		\
						sizeof(u_int64_t);	\
				} while (0)

#define PUT_ROWLOCK(db, obj)	do {					\
					u_int64_t *gi = (u_int64_t *)dp;\
					DB_LOCK_ROWLOCK *rl = (DB_LOCK_ROWLOCK *)((obj)->data);\
					memcpy(gi, &rl->genid, sizeof(u_int64_t)); \
					dp = (u_int8_t *)dp + 		\
						sizeof(u_int64_t);	\
				} while (0)

#define PUT_LOCK(dp, obj)	do {					\
					if((obj)->size == 		\
						sizeof(DB_LOCK_MINMAX)) { \
						PUT_MINMAX(dp, obj);	\
					}				\
					else if((obj)->size == 		\
						sizeof(DB_LOCK_ROWLOCK)) {\
						PUT_ROWLOCK(dp, obj);	\
					}				\
					else {				\
						logmsg(LOGMSG_FATAL, "%s: invalid size for rowlock, %d\n", __func__, (obj)->size);\
						abort(); \
					}\
				} while (0)

#define PUT_LSN(dp, lsn)	do {					\
					memcpy(dp,			\
					    lsn, sizeof(DB_LSN));  \
					dp = (u_int8_t *)dp +		\
						    sizeof(DB_LSN); 	\
				} while (0)

#define COPY_OBJ(dp, obj)	do {					\
					memcpy(dp,			\
					    (obj)->data, (obj)->size);  \
					dp = (u_int8_t *)dp +		\
					     ALIGN((obj)->size,		\
					    sizeof(u_int32_t)); 	\
				} while (0)

#define GET_NKEYS(dp, nkeys)	do {					\
					(nkeys) = *(u_int32_t *) dp;	\
                    if(LOG_SWAPPED())           \
                        M_32_SWAP(nkeys);         \
					dp = (u_int8_t *)dp +		\
					     sizeof(u_int32_t); 	\
				} while (0);

#define GET_COUNT(dp, count)	do {					\
					(count) = *(u_int32_t *) dp;	\
                    if(LOG_SWAPPED())           \
                        M_32_SWAP(count);         \
					dp = (u_int8_t *)dp +		\
					     sizeof(u_int32_t); 	\
				} while (0);
#define GET_PCOUNT(dp, count)	do {					\
					(count) = *(u_int16_t *) dp;	\
                    if(LOG_SWAPPED())           \
                        M_16_SWAP(count);         \
					dp = (u_int8_t *)dp +		\
					     sizeof(u_int16_t); 	\
				} while (0);
#define GET_LSNCOUNT(dp, count)	do {					\
					(count) = *(u_int32_t *) dp;	\
                    if(LOG_SWAPPED())           \
                        M_32_SWAP(count);         \
					dp = (u_int8_t *)dp +		\
					     sizeof(u_int32_t); 	\
				} while (0);
#define GET_GCOUNT(dp, count)	do {					\
					(count) = *(u_int32_t *) dp;	\
                    if(LOG_SWAPPED())           \
                        M_32_SWAP(count);         \
					dp = (u_int8_t *)dp +		\
					     sizeof(u_int32_t); 	\
				} while (0)

#define GET_SIZE(dp, size)	do {					\
					(size) = *(u_int16_t *) dp;	\
                    if(LOG_SWAPPED())           \
                        M_16_SWAP(size);         \
					dp = (u_int8_t *)dp +		\
					     sizeof(u_int16_t); 	\
				} while (0)

#define GET_FILEID(dp, ptr)	do {					\
					memcpy(ptr, dp, DB_FILE_ID_LEN);\
					dp = (u_int8_t *)dp + 		\
						DB_FILE_ID_LEN; \
				} while (0)
#define GET_PGNO(dp, pgno)	do {					\
					(pgno) = *(db_pgno_t *) dp;	\
                    if(LOG_SWAPPED())           \
                        M_32_SWAP(pgno);         \
					dp = (u_int8_t *)dp +		\
					     sizeof(db_pgno_t); 	\
				} while (0)

#define GET_LSN(dp, lsn)	do {					\
					memcpy(lsn,			\
					    dp, sizeof(DB_LSN));  \
					dp = (u_int8_t *)dp +		\
						    sizeof(DB_LSN); 	\
				} while (0)

#define GET_GENID(dp, genid)	do {					\
					u_int8_t *g = (u_int8_t *) &genid; \
					memcpy(g, dp, sizeof(u_int64_t)); \
					dp = (u_int8_t *)dp +		\
					     sizeof(u_int64_t); 	\
				} while (0)


extern int gbl_new_snapisol_logging;
#define MAX_LOCK_COUNT	0xffffffff

static int
__lock_fix_list(dbenv, list_dbt, nlocks, has_pglk_lsn)
	DB_ENV *dbenv;
	DBT *list_dbt;
	u_int32_t nlocks;
	u_int8_t has_pglk_lsn;
{
	DBT *obj_dbt = NULL;
	struct __db_lockobj_lsn *obj_lsn = NULL;
	struct __db_dbt_internal *obj;
	DB_LOCK_ILOCK *lock, *plock;
	u_int32_t i, j, nfid, npgno, size, nlsns;
	int ret;
	u_int8_t *data = NULL, *dp = NULL;
	struct __db_lock_lsn *lsnp;
	struct __db_lock_lsn *next_lsnp;

	void *endptr;
	int oldsize;

	void *baseptr;
	void *ptr;

	/*
	 * if (nlocks > 1)
	 * {
	 * fprintf(stderr, "__lock_fix_list nlocks=%d\n", nlocks);
	 * }
	 */

	size = list_dbt->size;
	if (size == 0)
		return (0);
	if (has_pglk_lsn)
		obj_lsn = (struct __db_lockobj_lsn *)list_dbt->data;
	else
		obj_dbt = (DBT *)list_dbt->data;

	/*
	 * If necessary sort the list of locks so that locks
	 * on the same fileid are together.  We do not sort
	 * 1 or 2 locks because by definition if there are
	 * locks on the same fileid they will be together.
	 * The sort will also move any locks that do not
	 * look like page locks to the end of the list
	 * so we can stop looking for locks we can combine
	 * when we hit one.
	 */
	switch (nlocks) {
	case 1:
		if (has_pglk_lsn)
			obj = (struct __db_dbt_internal *)obj_lsn;
		else
			obj = (struct __db_dbt_internal *)obj_dbt;
		size = sizeof(unsigned long long);
		size += RET_SIZE(obj->size, 1);
		if (gbl_new_snapisol_logging) {
			size += sizeof(u_int32_t);	// n pagelock-lsn pairs
			size += sizeof(u_int32_t);	// flag to indicate new logging format
		}
		if (has_pglk_lsn && gbl_new_snapisol_logging) {
			size += sizeof(u_int32_t);	// nlsn
			size += obj_lsn->nlsns * sizeof(DB_LSN);	// lsn list
		}
		if ((ret = __os_malloc(dbenv, size, &data)) != 0)
			return (ret);

		dp = data;
		if (gbl_new_snapisol_logging) {
			PUT_COUNT(dp, MAX_LOCK_COUNT);	// indicate new logging format
			PUT_NKEYS(dp, has_pglk_lsn ? obj_lsn->nlsns : 0);
		}
		PUT_COUNT(dp, 1);
		PUT_PCOUNT(dp, 0);
		PUT_SIZE(dp, obj->size);
		COPY_OBJ(dp, obj);
		if (has_pglk_lsn) {
			if (gbl_new_snapisol_logging)
				PUT_LSNCOUNT(dp, obj_lsn->nlsns);
			for (lsnp =
			    SH_LIST_FIRST(&(obj_lsn->lsns), __db_lock_lsn);
			    lsnp != NULL; lsnp = next_lsnp) {
				next_lsnp =
				    SH_LIST_NEXT(lsnp, lsn_links,
				    __db_lock_lsn);
#ifdef NEWSI_DEBUG
				if (next_lsnp)
					assert(log_compare(&lsnp->llsn,
						&next_lsnp->llsn) >= 0);
#endif
				if (gbl_new_snapisol_logging)
					PUT_LSN(dp, &(lsnp->llsn));
				SH_LIST_REMOVE(lsnp, lsn_links, __db_lock_lsn);
				__deallocate_db_lock_lsn(dbenv, lsnp);
			}
		}
		bzero(dp, sizeof(unsigned long long));

		/*
		 * fprintf( stderr, "%d %s Put nlocks = 1 npgno = 0 size = %d", 
		 * pthread_self(), __func__, obj->size);
		 * if (obj->size == sizeof(DB_LOCK_ILOCK))
		 * {
		 * int rc;
		 * char *file;
		 * 
		 * rc = __dbreg_get_name(dbenv, (u_int8_t*) ((DB_LOCK_ILOCK*)obj->data)->fileid, &file);
		 * if (rc)
		 * fprintf(stderr,  "%d lock unknown file page %d\n", 
		 * ((DB_LOCK_ILOCK*)obj->data)->type, ((DB_LOCK_ILOCK*)obj->data)->pgno);
		 * else
		 * fprintf(stderr, "%d lock %s page %hd\n", 
		 * ((DB_LOCK_ILOCK*)obj->data)->type, file, ((DB_LOCK_ILOCK*)obj->data)->pgno);
		 * }
		 * else
		 * fprintf( stderr ,"\n");
		 */
		/*cheap_stack_trace(); */

		break;

	default:
		/* Sort so that all locks with same fileid are together. */
		qsort(list_dbt->data, nlocks,
		    has_pglk_lsn ? sizeof(struct __db_lockobj_lsn) :
		    sizeof(DBT),
		    has_pglk_lsn ? __locklsn_sort_cmp : __lock_sort_cmp);
		/* FALL THROUGH */
	case 2:
		nfid = npgno = nlsns = 0;
		i = 0;
		if (has_pglk_lsn) {
			if (obj_lsn->size != sizeof(DB_LOCK_ILOCK))
				goto not_ilock;

			nfid = 1;
			plock = (DB_LOCK_ILOCK *)obj_lsn->data;

			/* We use ulen to keep track of the number of pages. */
			j = 0;
			obj_lsn[0].ulen = 0;
			nlsns = obj_lsn[0].nlsns;
			for (i = 1; i < nlocks; i++) {
				if (obj_lsn[i].size != sizeof(DB_LOCK_ILOCK))
					break;
				lock = (DB_LOCK_ILOCK *)obj_lsn[i].data;
				nlsns += obj_lsn[i].nlsns;
				if (obj_lsn[j].ulen < MAX_PGNOS &&
				    lock->type == plock->type &&
				    memcmp(lock->fileid,
					plock->fileid, DB_FILE_ID_LEN) == 0) {
					obj_lsn[j].ulen++;
					npgno++;
				} else {
					nfid++;
					plock = lock;
					j = i;
					obj_lsn[j].ulen = 0;
				}
			}
		} else {
			if (obj_dbt->size != sizeof(DB_LOCK_ILOCK))
				goto not_ilock;

			nfid = 1;
			plock = (DB_LOCK_ILOCK *)obj_dbt->data;

			/* We use ulen to keep track of the number of pages. */
			j = 0;
			obj_dbt[0].ulen = 0;
			for (i = 1; i < nlocks; i++) {
				if (obj_dbt[i].size != sizeof(DB_LOCK_ILOCK))
					break;
				lock = (DB_LOCK_ILOCK *)obj_dbt[i].data;
				if (obj_dbt[j].ulen < MAX_PGNOS &&
				    lock->type == plock->type &&
				    memcmp(lock->fileid,
					plock->fileid, DB_FILE_ID_LEN) == 0) {
					obj_dbt[j].ulen++;
					npgno++;
				} else {
					nfid++;
					plock = lock;
					j = i;
					obj_dbt[j].ulen = 0;
				}
			}
		}

not_ilock:

		size = nfid * sizeof(DB_LOCK_ILOCK);
		size += npgno * sizeof(db_pgno_t);
		/* Add the number of nonstandard locks and get their size. */
		nfid += nlocks - i;

		oldsize = size;

		baseptr = NULL;
		ptr = baseptr;

		if (has_pglk_lsn) {
			for (; i < nlocks; i++) {
				oldsize += obj_lsn[i].size;
				ptr =
				    (u_int8_t *)ptr + ALIGN(obj_lsn[i].size,
				    sizeof(u_int32_t));
				obj_lsn[i].ulen = 0;
				nlsns += obj_lsn[i].nlsns;
			}
		} else {
			for (; i < nlocks; i++) {
				oldsize += obj_dbt[i].size;
				ptr =
				    (u_int8_t *)ptr + ALIGN(obj_dbt[i].size,
				    sizeof(u_int32_t));
				obj_dbt[i].ulen = 0;
			}
		}


		size += (unsigned char *)ptr - (unsigned char *)baseptr;

		/*fprintf(stderr, "oldsize %d size %d\n", oldsize, size); */

		if (gbl_new_snapisol_logging) {
			size += sizeof(u_int32_t);	// flag to indicate new logging format
			size += sizeof(u_int32_t);	// n pagelock-lsn key
		}
		if (has_pglk_lsn && gbl_new_snapisol_logging) {
			/* allowed space for list of lsns */
			size += nlocks * sizeof(u_int32_t);	//nlsn for each lock
			size += nlsns * sizeof(DB_LSN);	//total number of lsns
		}

		/* allocate space for context */
		size += sizeof(unsigned long long);

		size = RET_SIZE(size, nfid);

		if ((ret = __os_malloc(dbenv, size, &data)) != 0)
			return (ret);

		dp = data;

		endptr = dp + size;

		if (gbl_new_snapisol_logging) {
			if (dp + sizeof(u_int32_t) > (uint8_t *)endptr) {
				logmsg(LOGMSG_USER, "%d: __lock_fix_list about to overrun (PUT_COUNT)\n",
				    __LINE__);
				abort();
			}
			PUT_COUNT(dp, MAX_LOCK_COUNT);
			if (dp + sizeof(u_int32_t) > (uint8_t *)endptr) {
				logmsg(LOGMSG_FATAL, "%d: __lock_fix_list about to overrun (PUT_NKEYS)\n",
				    __LINE__);
				abort();
			}
			PUT_NKEYS(dp, has_pglk_lsn ? nlsns : 0);
		}

		if (dp + sizeof(u_int32_t) > (uint8_t *)endptr) {
            logmsg(LOGMSG_FATAL, "%d: __lock_fix_list about to overrun (PUT_COUNT)\n",
			    __LINE__);
			abort();
		}
		PUT_COUNT(dp, nfid);

		/*
		 * fprintf( stderr, "%d %s nlocks(nfid) = %d)\n",
		 * pthread_self(), __func__, nfid);
		 */

		if (has_pglk_lsn) {
			for (i = 0; i < nlocks; i = j) {

				if (dp + sizeof(u_int16_t) > (uint8_t *)endptr) {
					logmsg(LOGMSG_FATAL, "%d: __lock_fix_list about to overrun (PUT_PCOUNT)\n",
					    __LINE__);
					abort();
				}
				PUT_PCOUNT(dp, obj_lsn[i].ulen);

				if (dp + sizeof(u_int16_t) > (uint8_t *)endptr) {
					logmsg(LOGMSG_FATAL, "%d: __lock_fix_list about to overrun (PUT_SIZE)\n",
					    __LINE__);
					abort();
				}
				PUT_SIZE(dp, obj_lsn[i].size);

				if (dp + obj_lsn[i].size > (uint8_t *)endptr) {
					logmsg(LOGMSG_FATAL, "%d: __lock_fix_list about to overrun (COPY_OBJ)\n",
					    __LINE__);
					abort();
				}
				COPY_OBJ(dp, &obj_lsn[i]);

				if (gbl_new_snapisol_logging) {
					if (dp + sizeof(u_int16_t) >
					    (uint8_t *)endptr) {
						logmsg(LOGMSG_FATAL, "%d: __lock_fix_list about to overrun (PUT_LSNCOUNT)\n",
						    __LINE__);
						abort();
					}
					PUT_LSNCOUNT(dp, obj_lsn[i].nlsns);
				}

				for (lsnp =
				    SH_LIST_FIRST(&(obj_lsn[i].lsns),
					__db_lock_lsn); lsnp != NULL;
				    lsnp = next_lsnp) {
					next_lsnp =
					    SH_LIST_NEXT(lsnp, lsn_links,
					    __db_lock_lsn);
#ifdef NEWSI_DEBUG
					if (next_lsnp)
						assert(log_compare(&lsnp->llsn,
							&next_lsnp->llsn) >= 0);
#endif
					if (gbl_new_snapisol_logging) {
						if (dp + sizeof(DB_LSN) >
						    (uint8_t *)endptr) {
							logmsg(LOGMSG_FATAL, "%d: __lock_fix_list about to overrun (PUT_LSN)\n",
							    __LINE__);
							abort();
						}
						PUT_LSN(dp, &(lsnp->llsn));
					} SH_LIST_REMOVE(lsnp, lsn_links,
					    __db_lock_lsn);
					__deallocate_db_lock_lsn(dbenv, lsnp);
				}
				/*
				 * fprintf( stderr, "%d %d %d\n", 
				 * pthread_self(), obj_lsn[i].ulen, obj_lsn[i].size);
				 * if (obj_lsn[i].size == sizeof(DB_LOCK_ILOCK))
				 * {
				 * int rc;
				 * char *file;
				 * 
				 * rc = __dbreg_get_name(dbenv, (u_int8_t*) ((DB_LOCK_ILOCK*)obj_lsn[i].data)->fileid, &file);
				 * if (rc)
				 * fprintf(stderr, "%d lock unknown file page %d\n", 
				 * ((DB_LOCK_ILOCK*)obj_lsn[i].data)->type, ((DB_LOCK_ILOCK*)obj_lsn[i].data)->pgno);
				 * else
				 * fprintf(stderr, "%d lock %s page %d\n", 
				 * ((DB_LOCK_ILOCK*)obj_lsn[i].data)->type, file, ((DB_LOCK_ILOCK*)obj_lsn[i].data)->pgno);
				 * }
				 */

				lock = (DB_LOCK_ILOCK *)obj_lsn[i].data;
				for (j = i + 1; j <= i + obj_lsn[i].ulen; j++) {
					lock = (DB_LOCK_ILOCK *)obj_lsn[j].data;

					if (dp + sizeof(db_pgno_t) >
					    (uint8_t *)endptr) {
						logmsg(LOGMSG_FATAL,
						    "__lock_fix_list about to overrun (PUT_PGNO)\n");
						abort();
					}
					PUT_PGNO(dp, lock->pgno);

					if (gbl_new_snapisol_logging) {
						if (dp + sizeof(u_int16_t) >
						    (uint8_t *)endptr) {
							logmsg(LOGMSG_FATAL, "%d: __lock_fix_list about to overrun (PUT_LSNCOUNT)\n",
							    __LINE__);
							abort();
						}
						PUT_LSNCOUNT(dp,
						    obj_lsn[j].nlsns);
					}

					for (lsnp =
					    SH_LIST_FIRST(&(obj_lsn[j].lsns),
						__db_lock_lsn); lsnp != NULL;
					    lsnp = next_lsnp) {
						next_lsnp =
						    SH_LIST_NEXT(lsnp,
						    lsn_links, __db_lock_lsn);
#ifdef NEWSI_DEBUG
						if (next_lsnp)
							assert(log_compare
							    (&lsnp->llsn,
								&next_lsnp->
								llsn) >= 0);
#endif
						if (gbl_new_snapisol_logging) {
							if (dp +
							    sizeof(DB_LSN) >
							    (uint8_t *)endptr) {
								logmsg(LOGMSG_FATAL, "%d: __lock_fix_list about to overrun (PUT_LSN)\n",
								    __LINE__);
								abort();
							}
							PUT_LSN(dp,
							    &(lsnp->llsn));
						}
						SH_LIST_REMOVE(lsnp, lsn_links,
						    __db_lock_lsn);
						__deallocate_db_lock_lsn(dbenv,
						    lsnp);
					}
					/* 
					 * fprintf( stderr, "%d pgno =%d\n", pthread_self(), lock->pgno);
					 */
				}
			}
		} else {
			for (i = 0; i < nlocks; i = j) {

				if (dp + sizeof(u_int16_t) > (uint8_t *)endptr) {
					logmsg(LOGMSG_FATAL, "__lock_fix_list about to overrun (PUT_PCOUNT)\n");
					abort();
				}
				PUT_PCOUNT(dp, obj_dbt[i].ulen);

				if (dp + sizeof(u_int16_t) > (uint8_t *)endptr) {
					logmsg(LOGMSG_FATAL, "__lock_fix_list about to overrun (PUT_SIZE)\n");
					abort();
				}
				PUT_SIZE(dp, obj_dbt[i].size);

				if (dp + obj_dbt[i].size > (uint8_t *)endptr) {
                    logmsg(LOGMSG_FATAL, "__lock_fix_list about to overrun (COPY_OBJ)\n");
					abort();
				}
				COPY_OBJ(dp, &obj_dbt[i]);
				/*
				 * fprintf( stderr, "%d %d %d\n", 
				 * pthread_self(), obj_dbt[i].ulen, obj_dbt[i].size);
				 * if (obj_dbt[i].size == sizeof(DB_LOCK_ILOCK))
				 * {
				 * int rc;
				 * char *file;
				 * 
				 * rc = __dbreg_get_name(dbenv, (u_int8_t*) ((DB_LOCK_ILOCK*)obj_dbt[i].data)->fileid, &file);
				 * if (rc)
				 * fprintf(stderr, "%d lock unknown file page %d\n", 
				 * ((DB_LOCK_ILOCK*)obj_dbt[i].data)->type, ((DB_LOCK_ILOCK*)obj_dbt[i].data)->pgno);
				 * else
				 * fprintf(stderr, "%d lock %s page %d\n", 
				 * ((DB_LOCK_ILOCK*)obj_dbt[i].data)->type, file, ((DB_LOCK_ILOCK*)obj_dbt[i].data)->pgno);
				 * }
				 */

				lock = (DB_LOCK_ILOCK *)obj_dbt[i].data;
				for (j = i + 1; j <= i + obj_dbt[i].ulen; j++) {
					lock = (DB_LOCK_ILOCK *)obj_dbt[j].data;

					if (dp + sizeof(db_pgno_t) >
					    (uint8_t *)endptr) {
						logmsg(LOGMSG_FATAL, "__lock_fix_list about to overrun (PUT_PGNO)\n");
						abort();
					}
					PUT_PGNO(dp, lock->pgno);
					/* 
					 * fprintf( stderr, "%d pgno =%d\n", pthread_self(), lock->pgno);
					 */
				}
			}
		}

		if (dp + sizeof(unsigned long long) > (uint8_t *)endptr) {
			logmsg(LOGMSG_FATAL, "%d: __lock_fix_list about to overrun (context)\n",
			    __LINE__);
			abort();
		}
		bzero(dp, sizeof(unsigned long long));
	}

	(void)__os_free(dbenv, list_dbt->data);

	list_dbt->data = data;
	list_dbt->size = size;
	return (0);

}

extern int gbl_disable_rowlocks;

// PUBLIC: int __lock_get_rowlocks_list __P((DB_ENV *, u_int32_t, u_int32_t,
// PUBLIC:     db_lockmode_t, DBT *, DB_LOCK **, u_int32_t *));
int
__lock_get_rowlocks_list(dbenv, locker, flags, lock_mode, list, locklist,
    lockcnt)
	DB_ENV *dbenv;
	u_int32_t locker, flags;
	db_lockmode_t lock_mode;
	DBT *list;
	DB_LOCK **locklist;
	u_int32_t *lockcnt;

{
	u_int32_t i, j, totlocks, ngenids, locker_ndx;
	u_int64_t genid, tmpgenid;
	u_int8_t lockmem[31];
	DB_LOCKREGION *region;
	DB_LOCK_MINMAX *mm = (DB_LOCK_MINMAX *)lockmem;
	DB_LOCK_ROWLOCK *rl = (DB_LOCK_ROWLOCK *)lockmem;
	DB_LOCKER *sh_locker;
	DB_LOCK *ret_lock;
	DB_LOCKTAB *lt;
	int nfid, ret;
	DBT obj_dbt;
	void *dp;

	*lockcnt = 0;
	(*locklist) = NULL;

	if (list->size == 0)
		return (0);

	ret = 0;
	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;
	dp = list->data;

	GET_COUNT(dp, totlocks);
	GET_COUNT(dp, nfid);

	if ((ret = __os_malloc(dbenv, totlocks * sizeof(DB_LOCK),
		    &ret_lock)) != 0) {
		goto err;
	}

	LOCKER_INDX(lt, region, locker, locker_ndx);
	if ((ret = __lock_getlocker(lt, locker, locker_ndx,
		    0, &sh_locker)) != 0) {
		goto err;
	}

	memset(lockmem, 0, sizeof(lockmem));
	obj_dbt.data = lockmem;

	for (i = 0; i < nfid; i++) {
		GET_GCOUNT(dp, ngenids);
		GET_FILEID(dp, &lockmem);

		for (j = 0; j < ngenids; j++) {
			GET_GENID(dp, genid);
			P_64_COPYSWAP(&genid, &tmpgenid);

			/* Minlock */
			if (genid == 0) {
				memset(mm->fluff, 0, sizeof(mm->fluff));
				mm->minmax = 0;
				obj_dbt.size = sizeof(*mm);
			}
			/* Maxlock */
			else if (genid == -1) {
				memset(mm->fluff, 0, sizeof(mm->fluff));
				mm->minmax = 1;
				obj_dbt.size = sizeof(*mm);
			}
			/* Rowlock */
			else {
				memcpy(&rl->genid, &genid, sizeof(rl->genid));
				obj_dbt.size = sizeof(*rl);
			}


			if (!gbl_disable_rowlocks) {
				if ((ret =
				    __lock_get_internal(lt, locker, sh_locker,
					flags, &obj_dbt, lock_mode, 0,
					&ret_lock[*lockcnt])) != 0) {
					goto err;
				}
				(*lockcnt)++;
			}
		}
	}
	assert(*lockcnt == totlocks || gbl_disable_rowlocks);
	*locklist = ret_lock;
err:
	UNLOCKREGION(dbenv, (DB_LOCKTAB *)dbenv->lk_handle);
	return (ret);
}

int bdb_pglogs_key_list_init(void **listp, int n);
int bdb_add_pglogs_key_list(int i, void **listp,
    db_pgno_t pgno, unsigned char *fileid, DB_LSN lsn, DB_LSN commit_lsn);

static char *
ilock_type_str(int type)
{
	switch (type) {
	case DB_HANDLE_LOCK:
		return "HANDLE-LOCK";
		break;
	case DB_RECORD_LOCK:
		return "RECORD-LOCK";
		break;
	case DB_PAGE_LOCK:
		return "PAGE-LOCK";
		break;
	default:
		return "UNKNOWN-LOCK";
		break;
	}
}

#include "cdb2_constants.h"
void form_tablelock_keyname(const char *name, char *keynamebuf, DBT *dbt_out);

static int
__lock_list_parse_pglogs_int(dbenv, locker, flags, lock_mode, list, maxlsn,
    pglogs, keycnt, get_lock, ret_dp, fp)
	DB_ENV *dbenv;
	u_int32_t locker, flags;
	db_lockmode_t lock_mode;
	DBT *list;
	DB_LSN *maxlsn;
	void **pglogs;
	u_int32_t *keycnt;
	int get_lock;
	void **ret_dp;
	FILE *fp;
{
	DBT obj_dbt;
	char tablename[30] = {0};
	DB_LOCK ret_lock;
	DB_LOCK_ILOCK *lock;
	DB_LOCKER *sh_locker;
	DB_LOCKTAB *lt = NULL;
	DB_LOCKREGION *region;
	db_pgno_t save_pgno;
	u_int16_t npgno, size;
	u_int32_t i, nkeys, nlocks, j, keyidx, nlsns;
	DB_LSN llsn;
	int ret;
	void *dp = NULL;

	if (!pglogs)
		return (0);

	if (list->size == 0)
		return (0);

	ret = 0;
	dp = list->data;
	*keycnt = 0;

	GET_COUNT(dp, nlocks);
	if (get_lock)
		assert(nlocks == MAX_LOCK_COUNT);
	else if (nlocks != MAX_LOCK_COUNT)
		return -1;

	GET_NKEYS(dp, nkeys);
	GET_COUNT(dp, nlocks);

	if (nkeys == 0 && get_lock == 0)
		return 0;

	if (get_lock && LF_ISSET(LOCK_GET_LIST_GETLOCK)) {
		lt = dbenv->lk_handle;
		region = lt->reginfo.primary;
		LOCKREGION(dbenv, (DB_LOCKTAB *)dbenv->lk_handle);
		u_int32_t locker_ndx;
		LOCKER_INDX(lt, region, locker, locker_ndx);
		if ((ret = __lock_getlocker(lt, locker, locker_ndx,
			    0, &sh_locker)) != 0) {
			goto err;
		}
	}
	if (LF_ISSET(LOCK_GET_LIST_GETLOCK | LOCK_GET_LIST_PAGELOGS))
		bdb_pglogs_key_list_init(pglogs, nkeys);
	*keycnt = nkeys;
	keyidx = 0;
	for (i = 0; i < nlocks; i++) {
		GET_PCOUNT(dp, npgno);
		GET_SIZE(dp, size);
		if (nlocks == 1 && npgno == 0)
			flags |= DB_LOCK_ONELOCK;
		lock = (DB_LOCK_ILOCK *)dp;
		save_pgno = lock->pgno;
		obj_dbt.data = dp;
		obj_dbt.size = size;
		int first_fid = 1;
		dp = ((u_int8_t *)dp) + ALIGN(size, sizeof(u_int32_t));
		do {
			if (LF_ISSET(LOCK_GET_LIST_GETLOCK)) {
				uint32_t lflags =
					(flags & (~(LOCK_GET_LIST_GETLOCK |
						LOCK_GET_LIST_PRINTLOCK | LOCK_GET_LIST_PREPARE)));
				if (get_lock &&
					(ret =
					__lock_get_internal(lt, locker,
						sh_locker, lflags, &obj_dbt,
						lock_mode, 0, &ret_lock)) != 0) {
					lock->pgno = save_pgno;
					goto err;
				}
				if (get_lock && LF_ISSET(LOCK_GET_LIST_PREPARE) && obj_dbt.size == 
					sizeof(struct __db_ilock) && first_fid) {
					char *filename = NULL;
					if ((__ufid_to_fname(dbenv, &filename, lock->fileid)) != 0) {
						/* Maybe ufid logging not enabled? */
						char fid_str[(DB_FILE_ID_LEN * 2) + 1] = {0};
						fileid_str(lock->fileid, fid_str);
						logmsg(LOGMSG_ERROR, "Error resolving btree from ufid %s\n", fid_str);
					} else {
						char *start = (char *)&filename[4];
						char *end = (char *)&filename[strlen(filename) - 1];
						while (end > start && *end != '\0' && *end != '.') {
							end--;
						}
						end -= 17;
						if (end <= start) {
							logmsg(LOGMSG_ERROR, "%s: unrecognized file format for %s\n", __func__, filename);
							return 0;
						}
						char t[MAXTABLELEN + 1] = {0};
						memcpy(t, start, (end - start));
						char name[32]; /* TABLELOCK_KEY_SIZE */
						DBT lk = {0};
						form_tablelock_keyname(t, name, &lk);
						if ((ret = __lock_get_internal(lt, locker, sh_locker, lflags, &lk,
								DB_LOCK_READ, 0, &ret_lock)) != 0) {
							goto err;
						}
					}
				}
			}
			first_fid = 0;

			GET_LSNCOUNT(dp, nlsns);
			if (LF_ISSET(LOCK_GET_LIST_PRINTLOCK)) {
				switch(obj_dbt.size)  {
					case(sizeof(struct __db_ilock)): 
						if (fp)
							fprintf(fp, "\t\tFILEID: ");
						else
							logmsg(LOGMSG_USER, "\t\tFILEID: ");

						hexdumpfp(fp, lock->fileid, sizeof(lock->fileid));
						if (fp) {
							fprintf(fp, " TYPE: %s", ilock_type_str(lock->type));
							fprintf(fp, " PAGE: %u\n", lock->pgno);
						} else {
							logmsg(LOGMSG_USER, " TYPE: %s", ilock_type_str(lock->type));
							logmsg(LOGMSG_USER, " PAGE: %u\n", lock->pgno);
						}
						if (nlsns) {
							if (fp)
								fprintf(fp, "\t\tPGLOGS: ");
							else
								logmsg(LOGMSG_USER, "\t\tPGLOGS: ");
						}
						break;
					case(32):
						memcpy(tablename,obj_dbt.data, 28);
						if (fp)
							fprintf(fp, "\t\tTABLELOCK: %s", tablename);
						else
							logmsg(LOGMSG_USER, "\t\tTABLELOCK: %s", tablename);
						// Tablenames > 28 chars have crc32 appended 
						uint32_t crc32 = *(uint32_t *)(obj_dbt.data + 28);
						if (crc32 > 0) {
							if (fp)
								fprintf(fp, " CRC32: %u", crc32);
							else
								logmsg(LOGMSG_USER, " CRC32: %u", crc32);
						}
						if (fp)
							fprintf(fp, "\n");
						else
							logmsg(LOGMSG_USER, "\n");
						break;
					default:
						if (fp)
							fprintf(fp, "\t\tUNKNOWN-SIZE-%d: ", obj_dbt.size);
						else
							logmsg(LOGMSG_USER, "\t\tUNKNOWN-SIZE-%d: ", obj_dbt.size);
						hexdumpfp(fp, obj_dbt.data, obj_dbt.size);
						break;
				}
			}
			for (j = 0; j < nlsns; j++) {
				GET_LSN(dp, &llsn);
				if (keyidx >= nkeys) {
					logmsg(LOGMSG_FATAL, "%s:%d keyidx = %d, nkeys = %d\n",
						__func__, __LINE__, keyidx, nkeys);
					abort();
				}
				if (LF_ISSET(LOCK_GET_LIST_GETLOCK |
					LOCK_GET_LIST_PAGELOGS)) {
					bdb_add_pglogs_key_list(keyidx++,
						pglogs, lock->pgno, lock->fileid,
						llsn, *maxlsn);
				}
				if (LF_ISSET(LOCK_GET_LIST_PRINTLOCK)) {
					if (fp)
						fprintf(fp, "%d:%d ", llsn.file, llsn.offset);
					else
						logmsg(LOGMSG_USER, "%d:%d ", llsn.file, llsn.offset);
				}
			}
			if (LF_ISSET(LOCK_GET_LIST_PRINTLOCK)) {
				if (fp)
					fprintf(fp, "\n");
				else
					logmsg(LOGMSG_USER, "\n");
			}
			if (npgno != 0)
				GET_PGNO(dp, lock->pgno);
		} while (npgno-- != 0);
		lock->pgno = save_pgno;
	}

err:
	if (get_lock)
		UNLOCKREGION(dbenv, (DB_LOCKTAB *)dbenv->lk_handle);
	if (ret_dp)
		*ret_dp = dp;
	return (ret);
}

static int
__lock_get_list_int_int(dbenv, locker, flags, lock_mode, list, pcontext, maxlsn,
    pglogs, keycnt, fp)
	DB_ENV *dbenv;
	u_int32_t locker, flags;
	db_lockmode_t lock_mode;
	DBT *list;
	unsigned long long *pcontext;
	DB_LSN *maxlsn;
	void **pglogs;
	u_int32_t *keycnt;
	FILE *fp;
{
	char tablename[30] = {0};
	DBT obj_dbt;
	DB_LOCK ret_lock;
	DB_LOCK_ILOCK *lock;
	DB_LOCKER *sh_locker;
	DB_LOCKTAB *lt = NULL;
	DB_LOCKREGION *region;
	db_pgno_t save_pgno;
	u_int16_t npgno, size;
	u_int32_t i, nkeys = 0, nlocks = 0;
	int ret;
	void *dp;
	int locked_region = 0;

	if (list->size == 0)
		return (0);

	ret = 0;

	if (LF_ISSET(LOCK_GET_LIST_GETLOCK)) {
		lt = dbenv->lk_handle;
		region = lt->reginfo.primary;
	}
	dp = list->data;
	*keycnt = 0;

	if (list->size == sizeof(unsigned long long)) {
		/* special case, no locks, only context
		 * dp points to context */

		logmsg(LOGMSG_ERROR, "%p size is 0, no nocks\n", (void *)pthread_self());

		LOCKREGION(dbenv, (DB_LOCKTAB *)dbenv->lk_handle);
		locked_region = 1;
		goto handle_context;
	}

	nkeys = 0;
	GET_COUNT(dp, nlocks);
	if (nlocks == MAX_LOCK_COUNT) {
		GET_NKEYS(dp, nkeys);
		GET_COUNT(dp, nlocks);
	}

	if (nkeys == 0) {

		if (LF_ISSET(LOCK_GET_LIST_GETLOCK)) {
			LOCKREGION(dbenv, (DB_LOCKTAB *)dbenv->lk_handle);
			locked_region = 1;
			u_int32_t locker_ndx;
			LOCKER_INDX(lt, region, locker, locker_ndx);
			if ((ret = __lock_getlocker(lt, locker, locker_ndx,
				    0, &sh_locker)) != 0) {
				goto err;
			}
		}

		for (i = 0; i < nlocks; i++) {
			GET_PCOUNT(dp, npgno);
			GET_SIZE(dp, size);
			if (nlocks == 1 && npgno == 0)
				flags |= DB_LOCK_ONELOCK;
			lock = (DB_LOCK_ILOCK *)dp;
			save_pgno = lock->pgno;
			obj_dbt.data = dp;
			obj_dbt.size = size;
			int first_fid = 1;
			dp = ((u_int8_t *)dp) + ALIGN(size, sizeof(u_int32_t));
			/* 
			 * Comdb2 early locking in replication does not support 
			 * handle locks in write mode for opened file.  We rely
			 * on table locks instead.
			 *
			 */
			if (size == sizeof(DB_LOCK_ILOCK) && 
					IS_WRITELOCK(lock_mode) &&
					((DB_LOCK_ILOCK*)obj_dbt.data)->type == DB_HANDLE_LOCK) {
				continue;
			}
			do {
				if (LF_ISSET(LOCK_GET_LIST_GETLOCK)) {
					uint32_t lflags =
						(flags & (~(LOCK_GET_LIST_GETLOCK |
							LOCK_GET_LIST_PRINTLOCK | LOCK_GET_LIST_PREPARE)));
					if ((ret =
						__lock_get_internal(lt, locker,
							sh_locker, lflags, &obj_dbt,
							lock_mode, 0,
							&ret_lock)) != 0) {
						lock->pgno = save_pgno;
						goto err;
					}
					if (LF_ISSET(LOCK_GET_LIST_PREPARE) && obj_dbt.size == 
						sizeof(struct __db_ilock) && first_fid) {
						char *filename = NULL;
						if ((__ufid_to_fname(dbenv, &filename, lock->fileid)) != 0) {
							/* Maybe ufid logging not enabled? */
							char fid_str[(DB_FILE_ID_LEN * 2) + 1] = {0};
							fileid_str(lock->fileid, fid_str);
							logmsg(LOGMSG_ERROR, "Error resolving btree from ufid %s\n", fid_str);
						} else {
							char *start = (char *)&filename[4];
							char *end = (char *)&filename[strlen(filename) - 1];
							while (end > start && *end != '\0' && *end != '.') {
								end--;
							}
							end -= 17;
							if (end <= start) {
								logmsg(LOGMSG_ERROR, "%s: unrecognized file format for %s\n", __func__, filename);
								return 0;
							}
							char t[MAXTABLELEN + 1] = {0};
							memcpy(t, start, (end - start));
							char name[32]; /* TABLELOCK_KEY_SIZE */
							DBT lk = {0};
							form_tablelock_keyname(t, name, &lk);
							if ((ret = __lock_get_internal(lt, locker, sh_locker, lflags, &lk,
											DB_LOCK_READ, 0, &ret_lock)) != 0) {
								goto err;
							}
						}
					}
					first_fid = 0;
				}
				if (LF_ISSET(LOCK_GET_LIST_PRINTLOCK)) {
					switch(obj_dbt.size) {
						case(sizeof(struct __db_ilock)): 
							if (fp) 
								fprintf(fp, "\t\tFILEID: ");
							else
								logmsg(LOGMSG_USER, "\t\tFILEID: ");

							hexdumpfp(fp, lock->fileid,
									sizeof(lock->fileid));
							if (fp) {
								fprintf(fp, " TYPE: %s",
										ilock_type_str(lock->type));
								fprintf(fp, " PAGE: %u\n", lock->pgno);
							} else {
								logmsg(LOGMSG_USER, " TYPE: %s",
										ilock_type_str(lock->type));
								logmsg(LOGMSG_USER, " PAGE: %u\n", lock->pgno);
							}
							break;

						case(32):
							memcpy(tablename,obj_dbt.data, 28);
							if (fp)
								fprintf(fp, "\t\tTABLELOCK: %s", tablename);
							else
								logmsg(LOGMSG_USER, "\t\tTABLELOCK: %s", tablename);

							// Tablenames > 28 chars have crc32 appended 
							uint32_t crc32 = *(uint32_t *)(obj_dbt.data + 28);
							if (crc32 > 0) {
								if (fp)
									fprintf(fp, " CRC32: %u", crc32);
								else
									logmsg(LOGMSG_USER, " CRC32: %u", crc32);
							}
							if (fp)
								fprintf(fp, "\n");
							else
								logmsg(LOGMSG_USER, "\n");
							break;

						default:
							if (fp)
								fprintf(fp, "\t\tUNKNOWN-SIZE-%d: ", obj_dbt.size);
							else
								logmsg(LOGMSG_USER, "\t\tUNKNOWN-SIZE-%d: ", obj_dbt.size);
							hexdumpfp(fp, obj_dbt.data, obj_dbt.size);
							break;
					}
				}

				if (npgno != 0)
					GET_PGNO(dp, lock->pgno);
			} while (npgno-- != 0);
			lock->pgno = save_pgno;
		}
	} else {
		ret = __lock_list_parse_pglogs_int(
		    dbenv, locker, flags, lock_mode, list, maxlsn, pglogs,
		    keycnt, 1, &dp, fp);
		if (ret)
			return ret;
	}

handle_context:

	if (pcontext) {
		memcpy(pcontext, dp, sizeof(unsigned long long));
		/*
		 * fprintf( stderr, "%d context %llx\n",
		 * pthread_self(), *pcontext);
		 */
	} else {
		/*
		 * fprintf( stderr, "%d no context\n",
		 * pthread_self());
		 */
	}

err:
	if (locked_region)
		UNLOCKREGION(dbenv, (DB_LOCKTAB *)dbenv->lk_handle);
	return (ret);
}


static int
__lock_get_list_int(dbenv, locker, flags, lock_mode, list, pcontext, maxlsn,
    pglogs, keycnt, fp)
	DB_ENV *dbenv;
	u_int32_t locker, flags;
	db_lockmode_t lock_mode;
	DBT *list;
	unsigned long long *pcontext;
	DB_LSN *maxlsn;
	void **pglogs;
	u_int32_t *keycnt;
    FILE *fp;
{
	int rc;
	extern int gbl_lock_get_list_start;

	gbl_lock_get_list_start = time(NULL);
	rc = __lock_get_list_int_int(dbenv, locker, flags, lock_mode, list,
	    pcontext, maxlsn, pglogs, keycnt, fp);
	gbl_lock_get_list_start = 0;
	return rc;
}

int
lock_list_parse_pglogs(dbenv, list, maxlsn, pglogs, keycnt)
	DB_ENV *dbenv;
	DBT *list;
	DB_LSN *maxlsn;
	void **pglogs;
	u_int32_t *keycnt;
{
	return __lock_list_parse_pglogs_int(dbenv, 0, LOCK_GET_LIST_PAGELOGS, 0,
	    list, maxlsn, pglogs, keycnt, 0, NULL, NULL);
}


/*
 * PUBLIC: int __lock_get_list __P((DB_ENV *, u_int32_t, u_int32_t,
 * PUBLIC:	      db_lockmode_t, DBT *, DB_LSN *, void **, u_int32_t *, FILE *));
 */
int
__lock_get_list(dbenv, locker, flags, lock_mode, list, maxlsn, pglogs, keycnt, fp)
	DB_ENV *dbenv;
	u_int32_t locker, flags;
	db_lockmode_t lock_mode;
	DBT *list;
	DB_LSN *maxlsn;
	void **pglogs;
	u_int32_t *keycnt;
    FILE *fp;
{
	return __lock_get_list_int(dbenv, locker, flags, lock_mode, list, NULL,
	    maxlsn, pglogs, keycnt, fp);
}

/*
 * PUBLIC: int __lock_get_list_context __P((DB_ENV *, u_int32_t, u_int32_t,
 * PUBLIC:	      db_lockmode_t, DBT *, unsigned long long *, DB_LSN*, void **, u_int32_t *));
 */
int
__lock_get_list_context(dbenv, locker, flags, lock_mode, list, pcontext, maxlsn,
    pglogs, keycnt)
	DB_ENV *dbenv;
	u_int32_t locker, flags;
	db_lockmode_t lock_mode;
	DBT *list;
	unsigned long long *pcontext;
	DB_LSN *maxlsn;
	void **pglogs;
	u_int32_t *keycnt;
{
	return __lock_get_list_int(dbenv, locker, flags, lock_mode, list,
	    pcontext, maxlsn, pglogs, keycnt, NULL);
}

static int
__rowlock_sort_cmp(a, b)
	const void *a, *b;
{
	const DBT **d1, **d2;
	char *fi1, *fi2;

	d1 = (const DBT **)a;
	d2 = (const DBT **)b;

	/* Sanity checks */
	switch ((*d1)->size) {
	case 30:
	case 31:
		break;
	default:
		logmsg(LOGMSG_FATAL, "%s: incorrect size for d1: %d",
		    __func__, (*d1)->size);
		abort();
	}

	switch ((*d2)->size) {
	case 30:
	case 31:
		break;
	default:
		logmsg(LOGMSG_FATAL, "%s: incorrect size for d2: %d",
		    __func__, (*d2)->size);
		abort();
	}

	fi1 = (*d1)->data;
	fi2 = (*d2)->data;

	return memcmp(fi1, fi2, DB_FILE_ID_LEN);
}

static int
__lock_sort_cmp(a, b)
	const void *a, *b;
{
	const DBT *d1, *d2;
	DB_LOCK_ILOCK *l1, *l2;

	d1 = a;
	d2 = b;

	/* Force all non-standard locks to sort at end. */
	if (d1->size != sizeof(DB_LOCK_ILOCK)) {
		if (d2->size != sizeof(DB_LOCK_ILOCK))
			return (d1->size - d2->size);
		else
			return (1);
	} else if (d2->size != sizeof(DB_LOCK_ILOCK))
		return (-1);

	l1 = d1->data;
	l2 = d2->data;
	if (l1->type != l2->type)
		return (l1->type - l2->type);
	return (memcmp(l1->fileid, l2->fileid, DB_FILE_ID_LEN));
}

static int
__locklsn_sort_cmp(a, b)
	const void *a, *b;
{
	const struct __db_lockobj_lsn *d1, *d2;
	DB_LOCK_ILOCK *l1, *l2;

	d1 = a;
	d2 = b;

	/* Force all non-standard locks to sort at end. */
	if (d1->size != sizeof(DB_LOCK_ILOCK)) {
		if (d2->size != sizeof(DB_LOCK_ILOCK))
			return (d1->size - d2->size);
		else
			return (1);
	} else if (d2->size != sizeof(DB_LOCK_ILOCK))
		return (-1);

	l1 = d1->data;
	l2 = d2->data;
	if (l1->type != l2->type)
		return (l1->type - l2->type);
	return (memcmp(l1->fileid, l2->fileid, DB_FILE_ID_LEN));
}

static int
__lock_to_dbt_unlocked(dbenv, lock, dbt)
	DB_ENV *dbenv;
	DB_LOCK *lock;
	DBT *dbt;
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKOBJ *sh_obj;
	struct __db_lock *lockp;
	u_int8_t *lockdata;
	int rc = 0;

	lockp = (struct __db_lock *)R_ADDR(&lt->reginfo, lock->off);
	if (lock->gen != lockp->gen) {
		__db_err(dbenv, __db_lock_invalid, "DB_LOCK->lock_put");
		rc = EINVAL;
		goto done;
	}

	sh_obj = lockp->lockobj;
	lockdata = sh_obj->lockobj.data;

	/* Set the dbt size */
	dbt->size = sh_obj->lockobj.size;

	if (dbt->flags & DB_DBT_MALLOC) {
		if ((rc = __os_malloc(dbenv, sh_obj->lockobj.size,
			    &dbt->data)) != 0)
			goto done;
		memcpy(dbt->data, lockdata, sh_obj->lockobj.size);
	} else if (dbt->ulen >= sh_obj->lockobj.size) {
		memcpy(dbt->data, lockdata, sh_obj->lockobj.size);
	} else {
		rc = ENOMEM;
	}
done:
	return rc;
}

/* Write the dbt data associated with a lock to a dbt. */
static int
__lock_to_dbt(dbenv, lock, dbt)
	DB_ENV *dbenv;
	DB_LOCK *lock;
	DBT *dbt;
{
	int rc;
	LOCKREGION(dbenv, dbenv->lk_handle);
	rc = __lock_to_dbt_unlocked(dbenv, lock, dbt);
	UNLOCKREGION(dbenv, dbenv->lk_handle);
	return rc;
}

static int
__lock_abort_waiters(dbenv, locker, flags)
	DB_ENV *dbenv;
	u_int32_t locker;
	u_int32_t flags;
{
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	u_int32_t partition;
	struct __db_lock *lp;
	DB_LOCKER *sh_locker;
	u_int32_t locker_ndx;
	int ret;

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;

	/* Check if locks have been globally turned off. */
	if (F_ISSET(dbenv, DB_ENV_NOLOCKING))
		return (0);

	/* Get the locker. */
	LOCKER_INDX(lt, region, locker, locker_ndx);

	/* Retrieve the locker */
	if ((ret = __lock_getlocker(lt, locker, locker_ndx,
		    GETLOCKER_KEEP_PART, &sh_locker)) != 0) {
		__db_err(dbenv, "Error in lock_getlocker for lid %u", locker);
		goto err;
	}
	partition = sh_locker->partition;

	if (NULL == sh_locker) {
		unlock_locker_partition(region, partition);
		__db_err(dbenv, "Locker does not exist");
		ret = EINVAL;
		abort();
		goto err;
	}

	/* Mark the locker as deleted temporarily so I can unlock the partition */
	F_SET(sh_locker, DB_LOCKER_DELETED);

	/* Unlock the partition */
	unlock_locker_partition(region, partition);

	/* Loop through all locks held by this locker */
	for (lp = SH_LIST_FIRST(&sh_locker->heldby, __db_lock); lp != NULL;
	    lp = SH_LIST_NEXT(lp, locker_links, __db_lock)) {
		DB_LOCKOBJ *lockobj;
		u_int32_t ndx, part;

		/* Get the object associated with this lock */
		lockobj = lp->lockobj;

		/* Get the index & partition */
		ndx = lockobj->index;
		part = lockobj->partition;

		/* Lock the partition */
		lock_obj_partition(region, part);

		/* Abort anything blocked on its rowlocks */
		if (!LF_ISSET(DB_LOCK_ABORT_LOGICAL) || is_comdb2_rowlock(lockobj->lockobj.size)) {
			/* This releases the lockobj */
			if ((ret = __dd_abort_waiters(dbenv, lockobj)) != 0) {
				__db_err(dbenv, "Error aborting waiters\n");
				goto err;
			}
		} else {
			/* Unlock the partition */
			unlock_obj_partition(region, part);
		}
	}

err:
	F_CLR(sh_locker, DB_LOCKER_DELETED);
	return ret;
}


// PUBLIC: int __lock_abort_waiters_pp __P((DB_ENV *, u_int32_t, u_int32_t));
int
__lock_abort_waiters_pp(dbenv, locker, flags)
	DB_ENV *dbenv;
	u_int32_t locker;
	u_int32_t flags;
{
	int ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "DB_ENV->lock_abort_waiters",
	    DB_INIT_LOCK);

	LOCKREGION(dbenv, (DB_LOCKTAB *)dbenv->lk_handle);
	ret = __lock_abort_waiters(dbenv, locker, flags);
	UNLOCKREGION(dbenv, (DB_LOCKTAB *)dbenv->lk_handle);
	return (ret);
}


// PUBLIC: int __lock_to_dbt_pp __P((DB_ENV *, DB_LOCK *, DBT *));
int
__lock_to_dbt_pp(dbenv, lock, dbt)
	DB_ENV *dbenv;
	DB_LOCK *lock;
	DBT *dbt;
{
	int ret;
	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "DB_LOCK->lock_put", DB_INIT_LOCK);
	ret = __lock_to_dbt(dbenv, lock, dbt);
	return ret;
}

// this is heavy-weight
int
__nlocks_for_thread(DB_ENV *dbenv, int *locks, int *lockers)
{
	DB_LOCKTAB *lt;
	DB_LOCKER *sh_locker;
	DB_LOCKREGION *region;
	u_int32_t locker_ndx;
	int ret;
	int nlocks = 0;
	int nlockers = 0;

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;

	LOCKREGION(dbenv, lt);
	lock_lockers(region);
	for (int i = 0; i < gbl_lkr_parts; ++i) {
		lock_locker_partition(region, i);
		for (int j = 0; j < region->locker_p_size; j++) {
			DB_LOCKER *lip;
			for (lip = SH_TAILQ_FIRST(&region->locker_tab[i][j], __db_locker);
					lip != NULL; lip = SH_TAILQ_NEXT(lip, links, __db_locker)) {
				if (lip->tid == pthread_self()) {
					nlockers++;
					// Don't count handle locks toward total
					nlocks += (lip->nlocks - lip->nhandlelocks);
				}
			}
		}
		unlock_locker_partition(region, i);
	}
	unlock_lockers(region);
	UNLOCKREGION(dbenv, lt);
	if (locks)
		(*locks) = nlocks;
	if (lockers)
		(*lockers) = nlockers;
	return 0;
}

int
__nlocks_for_locker(DB_ENV *dbenv, u_int32_t id)
{
	DB_LOCKTAB *lt;
	DB_LOCKER *sh_locker;
	DB_LOCKREGION *region;
	u_int32_t locker_ndx;
	int ret;
	int nlocks;

	lt = dbenv->lk_handle;
	region = lt->reginfo.primary;

	LOCKREGION(dbenv, lt);
	LOCKER_INDX(lt, region, id, locker_ndx);
	if ((ret = __lock_getlocker(lt, id, locker_ndx, 0, &sh_locker)) != 0)
		goto err;
	nlocks = sh_locker->nlocks;
	UNLOCKREGION(dbenv, lt);
	return nlocks;
err:
	UNLOCKREGION(dbenv, lt);
	return -1;
}

static int
__latch_set_parent_has_pglk_lsn(DB_ENV *dbenv, u_int32_t parentid,
	u_int32_t lockerid)
{
	int ret, ndx;
	DB_LOCKERID_LATCH_NODE *lidptr, *plidptr;
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	int has_pglk_lsn;

	if ((ret = __find_latch_lockerid(dbenv, lockerid, &lidptr, 0)) == 0) {
		has_pglk_lsn = lidptr->has_pglk_lsn;
	} else {
		DB_LOCKER *locker;
		LOCKER_INDX(lt, region, lockerid, ndx);
		if (__lock_getlocker(lt, lockerid, ndx, 0, &locker) != 0
			|| locker == NULL) {
			logmsg(LOGMSG_FATAL, "%s: lockid %x not found\n", __func__,
				lockerid);
			abort();
		}
		has_pglk_lsn = locker->has_pglk_lsn;
	}

	if ((ret = __find_latch_lockerid(dbenv, parentid, &plidptr, 0)) == 0) {
		plidptr->has_pglk_lsn = has_pglk_lsn;
	} else {
		DB_LOCKER *parent_locker;
		LOCKER_INDX(lt, region, parentid, ndx);
		if (__lock_getlocker(lt, parentid, ndx, 0,
			&parent_locker) != 0 || parent_locker == NULL) {
			logmsg(LOGMSG_FATAL, "%s: parent-lockid %x not found\n",
				__func__, parentid);
			abort();
		}

		parent_locker->has_pglk_lsn = has_pglk_lsn;
	}

	return 0;
}

int
__lock_set_parent_has_pglk_lsn(DB_ENV *dbenv, u_int32_t parentid,
	u_int32_t lockid)
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	DB_LOCKER *locker;
	DB_LOCKER *parent_locker;
	int ndx;

	if (use_page_latches(dbenv))
		return __latch_set_parent_has_pglk_lsn(dbenv, parentid, lockid);

	LOCKER_INDX(lt, region, lockid, ndx);
	if (__lock_getlocker(lt, lockid, ndx, 0, &locker) != 0
		|| locker == NULL) {
		logmsg(LOGMSG_FATAL, "%s: lockid %x not found\n", __func__, lockid);
		abort();
	}

	LOCKER_INDX(lt, region, parentid, ndx);
	if (__lock_getlocker(lt, parentid, ndx, 0, &parent_locker) != 0
		|| parent_locker == NULL) {
		logmsg(LOGMSG_FATAL, "%s: lockid %x not found\n", __func__,
			parentid);
		abort();
	}

	parent_locker->has_pglk_lsn = locker->has_pglk_lsn;

	return 0;
}

// PUBLIC: int __lock_update_tracked_writelocks_lsn_pp __P((DB_ENV *, DB_TXN *, u_int32_t, DB_LSN));
int
__lock_update_tracked_writelocks_lsn_pp(DB_ENV *dbenv, DB_TXN *txnp,
	u_int32_t lockid, DB_LSN lsn)
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	DB_LOCKER *locker;
	u_int32_t i = 0;
	struct __db_lock *lp;
	DB_LOCKOBJ *lockobj;
	DB_LOCK_ILOCK *ilock;
	struct __db_lock_lsn *lsnp;
	struct __db_lock_lsn *first_lsnp;
	int rc = 0;

	if (!gbl_new_snapisol_logging)
		return 0;

	if (use_page_latches(dbenv))
		return __latch_update_tracked_writelocks_lsn(dbenv, txnp,
			lockid, lsn);

	int ndx;
	LOCKER_INDX(lt, region, lockid, ndx);
	if (__lock_getlocker(lt, lockid, ndx, 0, &locker) != 0
		|| locker == NULL) {
		logmsg(LOGMSG_FATAL, "%s: lockid %x not found\n", __func__, lockid);
		abort();
	}

	if (!F_ISSET(locker, DB_LOCKER_TRACK_WRITELOCKS)) {
		logmsg(LOGMSG_FATAL, "%s: BUG! Writelocks for lsn[%u][%u] were not tracked\n",
			__func__, lsn.file, lsn.offset);
		abort();
	}

	F_CLR(locker, DB_LOCKER_TRACK_WRITELOCKS);
	locker->has_pglk_lsn = 1;

	if (!txnp->pglogs_hashtbl)
		DB_ASSERT(F_ISSET(txnp, TXN_COMPENSATE));

#ifdef NEWSI_DEBUG
	for (i = 0; i < locker->ntrackedlocks; i++) {
		lp = SH_LIST_FIRST(&locker->heldby, __db_lock);
		for ( ; lp != NULL; lp = SH_LIST_NEXT(lp, locker_links, __db_lock)) {
			if (lp == locker->tracked_locklist[i])
				break;
		}
		assert(lp != NULL);
	}
#endif

	assert(locker->ntrackedlocks != 0);
	for (i = 0; i < locker->ntrackedlocks; i++) {
		lp = locker->tracked_locklist[i];
		if (lp->status == DB_LSTAT_HELD) {
			rc = __allocate_db_lock_lsn(dbenv, &lsnp);
			if (rc)
				return ENOMEM;
			lsnp->llsn = lsn;
			Pthread_mutex_lock(&lp->lsns_mtx);
			first_lsnp = SH_LIST_FIRST(&lp->lsns, __db_lock_lsn);
			if (first_lsnp &&
				log_compare(&first_lsnp->llsn, &lsnp->llsn) == 0) {
				Pthread_mutex_unlock(&lp->lsns_mtx);
				__deallocate_db_lock_lsn(dbenv, lsnp);
			} else {
				SH_LIST_INSERT_HEAD(&lp->lsns, lsnp, lsn_links,
					__db_lock_lsn);
				lp->nlsns++;
				Pthread_mutex_unlock(&lp->lsns_mtx);
				if (txnp->pglogs_hashtbl) {
					lockobj = lp->lockobj;
					if (lockobj->lockobj.size ==
						sizeof(DB_LOCK_ILOCK)) {
						ilock = lockobj->lockobj.data;
						bdb_update_txn_pglogs(dbenv->
							app_private,
							txnp->pglogs_hashtbl,
							&txnp->pglogs_mutex,
							ilock->pgno, ilock->fileid,
							lsn);
					}
				}
			}
		}
	}
	return 0;
}

// PUBLIC: int __lock_clear_tracked_writelocks_pp __P((DB_ENV *, u_int32_t));
int
__lock_clear_tracked_writelocks_pp(DB_ENV *dbenv, u_int32_t lockid)
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	DB_LOCKER *locker;
	int rc = 0;
	int ndx;

	if (!gbl_new_snapisol_logging)
		return 0;

	if (use_page_latches(dbenv))
		return __latch_clear_tracked_writelocks(dbenv, lockid);

	LOCKER_INDX(lt, region, lockid, ndx);
	if (__lock_getlocker(lt, lockid, ndx, GETLOCKER_CREATE, &locker) != 0
		|| locker == NULL) {
		logmsg(LOGMSG_FATAL, "%s: lockid %x not found\n", __func__, lockid);
		abort();
	}
	F_SET(locker, DB_LOCKER_TRACK_WRITELOCKS);

	if (!locker->tracked_locklist) {
		int count = dbenv->attr.tracked_locklist_init > 0 ?
			dbenv->attr.tracked_locklist_init : 10;
		locker->maxtrackedlocks = count;
		rc = __os_malloc(dbenv,
			sizeof(struct __db_lock *) & locker->maxtrackedlocks,
			&(locker->tracked_locklist));
		if (rc)
			return ENOMEM;
	}

	locker->ntrackedlocks = 0;
	return 0;
}
