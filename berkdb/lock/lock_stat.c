/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: lock_stat.c,v 11.44 2003/09/13 19:20:36 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#include <string.h>
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

#include <ctype.h>
#endif

#include "db_int.h"
#include "dbinc/db_shash.h"
#include "dbinc/db_page.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/db_am.h"
#include <strings.h>

#ifdef _LINUX_SOURCE
#include <execinfo.h>
#endif
#include "logmsg.h"
#include <locks_wrap.h>

static void __lock_dump_locker __P((DB_LOCKTAB *, DB_LOCKER *, FILE *));
static void __lock_dump_object __P((DB_LOCKTAB *, DB_LOCKOBJ *, FILE *, int));
static void __lock_printheader __P((FILE *));
static int __lock_stat __P((DB_ENV *, DB_LOCK_STAT **, u_int32_t));
static int __lock_collect __P((DB_ENV *, collect_locks_f, void *));

void __lock_dump_locker_int __P((DB_LOCKTAB *, DB_LOCKER *, FILE *, int));
int __lock_printlock_int __P((DB_LOCKTAB *, struct __db_lock *, int, FILE *,
	int));

/*
 * __lock_stat_pp --
 *	DB_ENV->lock_stat pre/post processing.
 *
 * PUBLIC: int __lock_stat_pp __P((DB_ENV *, DB_LOCK_STAT **, u_int32_t));
 */
int
__lock_stat_pp(dbenv, statp, flags)
	DB_ENV *dbenv;
	DB_LOCK_STAT **statp;
	u_int32_t flags;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "DB_ENV->lock_stat", DB_INIT_LOCK);

	if ((ret = __db_fchk(dbenv,
		    "DB_ENV->lock_stat", flags, DB_STAT_CLEAR))!=0)
		 return (ret);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;

	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __lock_stat(dbenv, statp, flags);

	if (rep_check)
		__env_rep_exit(dbenv);

	return (ret);
}

/*
 * __lock_stat --
 *	DB_ENV->lock_stat.
 */
static int
__lock_stat(dbenv, statp, flags)
	DB_ENV *dbenv;
	DB_LOCK_STAT **statp;
	u_int32_t flags;
{
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	DB_LOCK_STAT *stats, tmp;
	int ret;

	*statp = NULL;
	lt = dbenv->lk_handle;

	if ((ret = __os_umalloc(dbenv, sizeof(*stats), &stats)) != 0)
		return (ret);

	/* Copy out the global statistics. */
	R_LOCK(dbenv, &lt->reginfo);

	region = lt->reginfo.primary;
	memcpy(stats, &region->stat, sizeof(*stats));
	stats->st_locktimeout = region->lk_timeout;
	stats->st_txntimeout = region->tx_timeout;

	stats->st_region_wait = lt->reginfo.rp->mutex.mutex_set_wait;
	stats->st_region_nowait = lt->reginfo.rp->mutex.mutex_set_nowait;
	stats->st_regsize = lt->reginfo.rp->size;
	if (LF_ISSET(DB_STAT_CLEAR)) {
		tmp = region->stat;
		memset(&region->stat, 0, sizeof(region->stat));
		lt->reginfo.rp->mutex.mutex_set_wait = 0;
		lt->reginfo.rp->mutex.mutex_set_nowait = 0;

		region->stat.st_id = tmp.st_id;
		region->stat.st_cur_maxid = tmp.st_cur_maxid;
		region->stat.st_maxlocks = tmp.st_maxlocks;
		region->stat.st_maxlockers = tmp.st_maxlockers;
		region->stat.st_maxobjects = tmp.st_maxobjects;
		region->stat.st_nlocks =
		    region->stat.st_maxnlocks = tmp.st_nlocks;
		region->stat.st_nlockers =
		    region->stat.st_maxnlockers = tmp.st_nlockers;
		region->stat.st_nobjects =
		    region->stat.st_maxnobjects = tmp.st_nobjects;
		region->stat.st_nmodes = tmp.st_nmodes;
	}

	R_UNLOCK(dbenv, &lt->reginfo);

	*statp = stats;
	return (0);
}

#define	LOCK_DUMP_CONF		0x001	/* Conflict matrix. */
#define	LOCK_DUMP_LOCKERS	0x002	/* Display lockers. */
#define	LOCK_DUMP_MEM		0x004	/* Display region memory. */
#define	LOCK_DUMP_OBJECTS	0x008	/* Display objects. */
#define	LOCK_DUMP_PARAMS	0x010	/* Display params. */
#define	LOCK_DUMP_A_LOCKER	0x020	/* Dump a single locker. */
#define	LOCK_DUMP_THREAD	0x040	/* Dump locks held by this thread. */
#define	LOCK_DUMP_ALL			/* All */			\
	(LOCK_DUMP_CONF | LOCK_DUMP_LOCKERS | LOCK_DUMP_MEM |		\
	LOCK_DUMP_OBJECTS | LOCK_DUMP_PARAMS)



/* COMDB2 MODIFICATION: take another parameter: flag to ignore handles
  and lockers with no held locks */
/*
 * __lock_dump_region --
 *
 * PUBLIC: int __lock_dump_region_int_int __P((DB_ENV *, const char *, FILE *, int, u_int32_t));
 */
int
__lock_dump_region_int_int(dbenv, area, fp, just_active_locks, lockerid)
	DB_ENV *dbenv;
	const char *area;
	FILE *fp;
	int just_active_locks;
	u_int32_t lockerid;
{
	DB_LOCKER *lip;
	DB_LOCKOBJ *op;
	DB_LOCKREGION *lrp;
	DB_LOCKTAB *lt;
	u_int32_t flags, i, j;
	char buf[64];

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "lock_dump_region", DB_INIT_LOCK);

	/* Make it easy to call from the debugger. */
	if (fp == NULL)
		fp = stderr;

	for (flags = 0; *area != '\0'; ++area)
		switch (*area) {
		case 'A':
			LF_SET(LOCK_DUMP_ALL);
			break;
		case 'c':
			LF_SET(LOCK_DUMP_CONF);
			break;
		case 'l':
			LF_SET(LOCK_DUMP_LOCKERS);
			break;
		case 'm':
			LF_SET(LOCK_DUMP_MEM);
			break;
		case 'o':
			LF_SET(LOCK_DUMP_OBJECTS);
			break;
		case 'p':
			LF_SET(LOCK_DUMP_PARAMS);
			break;
		case 't':
			LF_SET(LOCK_DUMP_THREAD);
		}

	lt = dbenv->lk_handle;
	lrp = lt->reginfo.primary;
	LOCKREGION(dbenv, lt);

	if (LF_ISSET(LOCK_DUMP_PARAMS)) {
		logmsgf(LOGMSG_USER, fp, "%s\nLock region parameters\n", DB_LINE);
		logmsgf(LOGMSG_USER, fp, 
            "%s: %lu, %s: %lu, %s: %lu,\n%s: %lu, %s: %lu, %s: %lu\n",
		    "region partitions", (u_long) gbl_lk_parts,
		    "locker table size", (u_long)lrp->locker_p_size,
		    "object table size", (u_long)lrp->object_p_size,
		    "osynch_off", (u_long)lrp->osynch_off,
		    "lsynch_off", (u_long)lrp->lsynch_off,
		    "need_dd", (u_long)lrp->need_dd);
		if (LOCK_TIME_ISVALID(&lrp->next_timeout)) {
			struct tm mytime;
			strftime(buf, sizeof(buf), "%m-%d-%H:%M:%S",
			    localtime_r((time_t*)&lrp->next_timeout.tv_sec, &mytime));
			logmsgf(LOGMSG_USER, fp, "next_timeout: %s.%lu\n",
			    buf, (u_long)lrp->next_timeout.tv_usec);
		}
	}

	if (LF_ISSET(LOCK_DUMP_CONF)) {
		logmsgf(LOGMSG_USER, fp, "\n%s\nConflict matrix\n", DB_LINE);

		for (i = 0; i < lrp->stat.st_nmodes; i++) {
			for (j = 0; j < lrp->stat.st_nmodes; j++)
				logmsgf(LOGMSG_USER, fp, "%lu\t", (u_long)
				    lt->conflicts[i * lrp->stat.st_nmodes + j]);
			logmsgf(LOGMSG_USER, fp, "\n");
		}
	}


	if (LF_ISSET(LOCK_DUMP_LOCKERS|LOCK_DUMP_THREAD)) {
		int j;
		logmsgf(LOGMSG_USER, fp, "%s\nLocks grouped by lockers\n", DB_LINE);
		__lock_printheader(fp);
		lock_lockers(lrp);

		for (i = 0; i < gbl_lkr_parts; ++i) {
			lock_locker_partition(lrp, i);

			for (j = 0; j < lrp->locker_p_size; j++) {
				for (lip = SH_TAILQ_FIRST(
				    &lrp->locker_tab[i][j], __db_locker);
				    lip != NULL;
				    lip = SH_TAILQ_NEXT(
				    lip, links, __db_locker)) {
					if (!LF_ISSET(LOCK_DUMP_THREAD) ||
					    lip->tid == pthread_self())
						__lock_dump_locker_int(lt,
						    lip, fp, just_active_locks);
				}
			}
			unlock_locker_partition(lrp, i);
		}
		unlock_lockers(lrp);
	}

	if (LF_ISSET(LOCK_DUMP_OBJECTS)) {
		int j;
		logmsgf(LOGMSG_USER, fp, "%s\nLocks grouped by object\n", DB_LINE);
		__lock_printheader(fp);
		lock_lockers(lrp);

		for (i = 0; i < gbl_lk_parts; ++i) {
			lock_obj_partition(lrp, i);

			for (j = 0; j < lrp->object_p_size; j++) {
				for (op =
				    SH_TAILQ_FIRST(&lrp->obj_tab[i][j],
					__db_lockobj); op !=NULL;
				    op = SH_TAILQ_NEXT(op, links, __db_lockobj))
					 __lock_dump_object(lt, op, fp,
					    just_active_locks);
			}
			unlock_obj_partition(lrp, i);
		}
		unlock_lockers(lrp);
	}

	if (LF_ISSET(LOCK_DUMP_MEM))
		__db_shalloc_dump(lt->reginfo.addr, fp);

	UNLOCKREGION(dbenv, lt);

	return (0);
}

int
__lock_dump_region_int(dbenv, area, fp, just_active_locks)
	DB_ENV *dbenv;
	const char *area;
	FILE *fp;
	int just_active_locks;
{
	return __lock_dump_region_int_int(dbenv, area, fp, just_active_locks, 0);
}

int
__lock_dump_region_lockerid(dbenv, area, fp, lockerid)
	DB_ENV *dbenv;
	const char *area;
	FILE *fp;
	u_int32_t lockerid;
{
	return __lock_dump_region_int_int(dbenv, area, fp, 0, lockerid);
}

static int
__dump_lid_latches(dbenv, lid, fp)
	DB_ENV *dbenv;
	DB_LOCKERID_LATCH_NODE *lid;
	FILE *fp;
{
	DB_ILOCK_LATCH *lnode;
	DB_LATCH *latch = NULL;
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;

	for (latch = lid->head; latch; latch = latch->next) {
		u_int8_t *addr = (u_int8_t *) latch;
		u_int32_t latchidx =
		    ((addr -(u_int8_t *) & region->latches[0])/sizeof(*latch));

		for (lnode = latch->listhead; lnode; lnode = lnode->next) {
			char *namep = NULL, *mode = NULL;
			u_int8_t *ptr = lnode->lock;
			db_pgno_t pgno;
			u_int32_t *fidp, type;

			memcpy(&pgno, ptr, sizeof(db_pgno_t));

			fidp = (u_int32_t *) (ptr +sizeof(db_pgno_t));
			type =
			    *(u_int32_t *) (ptr +sizeof(db_pgno_t) +
			    DB_FILE_ID_LEN);

			__dbreg_get_name(dbenv, (u_int8_t *) fidp, &namep);

			switch (lnode->lock_mode) {
			case DB_LOCK_READ:
				mode = "READ";
				break;
			case DB_LOCK_WRITE:
				mode = "WRITE";
				break;
			default:
				break;
			}

			/* I don't want the print-status code to grab any locks,
			 * so this is a little racy.  Only print valid latches 
			 * if type isn't db_page_lock, then we are looking at 
			 * the freelist */
			if (mode &&namep &&type == DB_PAGE_LOCK)
				logmsgf(LOGMSG_USER, fp, "%8x %11u %5u %-6s %8u %s\n",
				    lid->lockerid, latchidx, lnode->count,
				    mode, pgno, namep);
		}
	}
	return 0;
}


static int
__latch_dump_region_int(dbenv, fp)
	DB_ENV *dbenv;
	FILE *fp;
{
	u_int32_t idx;
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	DB_LOCKERID_LATCH_LIST *lockerid_latches = region->lockerid_latches;
	DB_LOCKERID_LATCH_NODE *lid = NULL;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv, lt, "latch_dump_region", DB_INIT_LOCK);

	if (fp == NULL)
		fp = stderr;

	logmsgf(LOGMSG_USER, fp, "%-8s %-11s %-5s %-6s %8s %s\n", "Locker", "Latch", "Count",
	    "Mode", "Page", "Filename");

	for (idx = 0; idx < region->max_latch_lockerid; idx++) {
		Pthread_mutex_lock(&(lockerid_latches[idx].lock));
		lid = lockerid_latches[idx].head;

		while (lid) {
			__dump_lid_latches(dbenv, lid, fp);
			lid = lid->next;
		}
		Pthread_mutex_unlock(&(lockerid_latches[idx].lock));
	}
	return 0;
}

int
__latch_dump_region(dbenv, fp)
	DB_ENV *dbenv;
	FILE *fp;
{
	return __latch_dump_region_int(dbenv, fp);
}

// PUBLIC: int __lock_dump_region __P((DB_ENV *, const char *, FILE *));
int
__lock_dump_region(dbenv, area, fp)
	DB_ENV *dbenv;
	const char *area;
	FILE *fp;
{
	return __lock_dump_region_int(dbenv, area, fp, 0);
}

static int
__lock_locker_lockcount(dbenv, id, nlocks)
	DB_ENV *dbenv;
	u_int32_t id;
	u_int32_t *nlocks;
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	DB_LOCKER *locker;

	int ndx;

	LOCKER_INDX(lt, region, id, ndx)

	if (__lock_getlocker(lt, id, ndx, 0, GETLOCKER_KEEP_PART, &locker) != 0
	    || locker == NULL) {
		return -1;
	}

	*nlocks = locker->nlocks;
	unlock_locker_partition(region, locker->partition);

	return 0;
}

#include <stdlib.h>

/* Go through the lockers held-list & count the pagelocks */
static int
__lock_locker_pagelockcount(dbenv, id, nlocks)
	DB_ENV *dbenv;
	u_int32_t id;
	u_int32_t *nlocks;
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	DB_LOCKER *locker;
	struct __db_lock *lp;
	int cnt = 0;
	int ndx;

	LOCKER_INDX(lt, region, id, ndx)

	if (__lock_getlocker(lt, id, ndx, 0, GETLOCKER_KEEP_PART, &locker) != 0
	    || locker == NULL) {
		return -1;
	}

	for (lp = SH_LIST_FIRST(&locker->heldby, __db_lock); lp !=NULL;
	    lp = SH_LIST_NEXT(lp, locker_links, __db_lock)) {
		DB_LOCKOBJ *lockobj = lp->lockobj;

		if (lockobj->lockobj.size == sizeof(struct __db_ilock)) {
			struct __db_ilock *ilock = lockobj->lockobj.data;

			if (ilock->type == DB_PAGE_LOCK) {
				cnt +=lp->refcount;
			}
		}
	}

	/* Make sure these are in sync */
	assert(cnt == locker->npagelocks);

	unlock_locker_partition(region, locker->partition);
	*nlocks = cnt;

	return 0;
}

// PUBLIC: int __lock_locker_lockcount_pp __P((DB_ENV *, u_int32_t, u_int32_t *));
int
__lock_locker_lockcount_pp(dbenv, id, nlocks)
	DB_ENV *dbenv;
	u_int32_t id;
	u_int32_t *nlocks;
{
	int rep_check, ret;
	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "DB_ENV->lock_locker_lockcount", DB_INIT_LOCK);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;

	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __lock_locker_lockcount(dbenv, id, nlocks);

	if (rep_check)
		__env_rep_exit(dbenv);

	return (ret);
}

// PUBLIC: int __lock_locker_pagelockcount_pp __P((DB_ENV *, u_int32_t, u_int32_t *));
int
__lock_locker_pagelockcount_pp(dbenv, id, nlocks)
	DB_ENV *dbenv;
	u_int32_t id;
	u_int32_t *nlocks;
{
	int rep_check, ret;
	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "DB_ENV->lock_locker_lockcount", DB_INIT_LOCK);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;

	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __lock_locker_pagelockcount(dbenv, id, nlocks);

	if (rep_check)
		__env_rep_exit(dbenv);

	return (ret);
}

static char *mode_to_str(int lpmode)
{
	char *mode = NULL;
	switch (lpmode) {
	case DB_LOCK_DIRTY:
		mode = "DIRTY_READ";
		break;
	case DB_LOCK_IREAD:
		mode = "IREAD";
		break;
	case DB_LOCK_IWR:
		mode = "IWR";
		break;
	case DB_LOCK_IWRITE:
		mode = "IWRITE";
		break;
	case DB_LOCK_NG:
		mode = "NG";
		break;
	case DB_LOCK_READ:
		mode = "READ";
		break;
	case DB_LOCK_WRITE:
		mode = "WRITE";
		break;
	case DB_LOCK_WRITEADD:
		mode = "WRITEADD";
		break;
	case DB_LOCK_WRITEDEL:
		mode = "WRITEDEL";
		break;
	case DB_LOCK_WWRITE:
		mode = "WAS_WRITE";
		break;
	case DB_LOCK_WAIT:
		mode = "WAIT";
		break;
	default:
		mode = "UNKNOWN";
		break;
	}
	return mode;
}

static char *status_to_str(int lpstatus)
{
	char *status;
	switch (lpstatus) {
	case DB_LSTAT_ABORTED:
		status = "ABORT";
		break;
	case DB_LSTAT_ERR:
		status = "ERROR";
		break;
	case DB_LSTAT_FREE:
		status = "FREE";
		break;
	case DB_LSTAT_HELD:
		status = "HELD";
		break;
	case DB_LSTAT_WAITING:
		status = "WAIT";
		break;
	case DB_LSTAT_PENDING:
		status = "PENDING";
		break;
	case DB_LSTAT_EXPIRED:
		status = "EXPIRED";
		break;
	default:
		status = "UNKNOWN";
		break;
	}
	return status;
}

#include "tohex.h"

static int
__collect_lock(DB_LOCKTAB *lt, DB_LOCKER *lip, struct __db_lock *lp,
		collect_locks_f func, void *arg)
{
	DB_LOCKOBJ *lockobj;
	db_pgno_t pgno = 0;
	DB_LSN lsn;
	int64_t page = -1;
	u_int32_t *fidp, type;
	u_int8_t *ptr;
	char minmax = 0;
	char *hexdump = NULL;
	char *namep = NULL;
	char rectype[80]={0};
	unsigned long long genid;
	char fileid[DB_FILE_ID_LEN];
	char tablename[64] = {0};
	const char *mode, *status;

	mode = mode_to_str(lp->mode);
	status = status_to_str(lp->status);

	lockobj = lp->lockobj;
	ptr = lockobj->lockobj.data;

	switch(lockobj->lockobj.size) {
		case(sizeof(struct __db_ilock)):
			memcpy(&pgno, ptr, sizeof(db_pgno_t));
			fidp = (u_int32_t *)(ptr + sizeof(db_pgno_t));
			type = *(u_int32_t *)(ptr + sizeof(db_pgno_t) + DB_FILE_ID_LEN);
			if (__dbreg_get_name(lt->dbenv, (u_int8_t *) fidp, &namep) != 0)
				namep = NULL;
			switch(type) {
				case (DB_PAGE_LOCK):
					snprintf(rectype, sizeof(rectype), "PAGE");
					page = pgno;
					break;
				case (DB_HANDLE_LOCK):
					snprintf(rectype, sizeof(rectype), "HANDLE");
					break;
				default:
					snprintf(rectype, sizeof(rectype), "UNKNOWN");
					break;
			}
			break;

		/* row or keyhash lock*/
		case (30):
			memcpy(fileid, ptr, DB_FILE_ID_LEN);
			memcpy(&genid, ptr + DB_FILE_ID_LEN + sizeof(short),
					sizeof(unsigned long long));
			fidp = (u_int32_t*) fileid;
			if (__dbreg_get_name(lt->dbenv, (u_int8_t *)fidp, &namep) != 0)
				namep = NULL;
			int l=strlen(namep);
			if (l >= 5 && !strncmp(&namep[l-5], "index", 5)) {
				snprintf(rectype, sizeof(rectype), "KEYHASH %llx", genid);
			} else {
				snprintf(rectype, sizeof(rectype), "ROWLOCK %llx", genid);
			}
			break;

		case (31):
			memcpy(fileid, ptr, DB_FILE_ID_LEN);
			memcpy(&minmax, ((char *)ptr)+30, sizeof(char));
			fidp = (u_int32_t *) fileid;
			if (__dbreg_get_name(lt->dbenv, (u_int8_t *) fidp, &namep) != 0)
				namep = NULL;
			snprintf(rectype, sizeof(rectype), "MINMAX %s", minmax == 0 ?
					"MIN" : "MAX");
			break;

		case (32):
			memcpy(tablename, ptr, 28);
			snprintf(rectype, sizeof(rectype), "TABLELOCK %s", tablename);
			namep = tablename;
			break;

		case (20):
			memcpy(fileid, ptr, DB_FILE_ID_LEN);
			fidp = (u_int32_t *) fileid;
			if (__dbreg_get_name(lt->dbenv, (u_int8_t *) fidp, &namep) != 0)
				namep = NULL;
			snprintf(rectype, sizeof(rectype), "STRIPELOCK");
			break;

		/* LSN LOCK .. REALLY?? */
		case (8):
			memcpy(&lsn, ptr, sizeof(DB_LSN));
			snprintf(rectype, sizeof(rectype), "LSN %u:%u", lsn.file,lsn.offset);
			break;

		case (4):
			if (*((int *)ptr) == 1) {
				namep = "ENVLOCK";
				snprintf(rectype, sizeof(rectype), "ENVLOCK");
				break;
			}

		default:
			hexdumpbuf(lockobj->lockobj.data, lockobj->lockobj.size, &hexdump);
			snprintf(rectype, sizeof(rectype), "UNKNOWN-TYPE SIZE %d",
					lockobj->lockobj.size);
			namep = hexdump;
			break;
	}

	if (namep && memcmp(namep, "XXX.", 4) == 0)
		namep += 4;

	(*func)(arg, lip->tid, lip->id, mode, status, namep, page, rectype);
	if (hexdump)
		free(hexdump);
	return 0;
}

static int
__collect_locker(DB_LOCKTAB *lt, DB_LOCKER *lip, collect_locks_f func, void *arg)
{
	struct __db_lock *lp;

	lp = SH_LIST_FIRST(&lip->heldby, __db_lock);
	if (lp !=NULL) {
		for (; lp !=NULL;
			lp = SH_LIST_NEXT(lp, locker_links, __db_lock))
			 __collect_lock(lt, lip, lp, func, arg);
	}
	return 0;
}

static int
__lock_collect(DB_ENV *dbenv, collect_locks_f func, void *arg)
{
	DB_LOCKTAB *lt;
	DB_LOCKER *lip;
	DB_LOCKREGION *lrp;
	int i, j;

	lt = dbenv->lk_handle;
	lrp = lt->reginfo.primary;
	LOCKREGION(dbenv, lt);
	lock_lockers(lrp);

	for (i = 0; i < gbl_lkr_parts; ++i) {
		lock_locker_partition(lrp, i);

		for (j = 0; j < lrp->locker_p_size; j++) {
			for (lip = SH_TAILQ_FIRST(&lrp->locker_tab[i][j], __db_locker);
					lip != NULL; lip = SH_TAILQ_NEXT(lip, links, __db_locker)) {
				__collect_locker(lt, lip, func, arg);
			}
		}
		unlock_locker_partition(lrp, i);
	}

	unlock_lockers(lrp);
	UNLOCKREGION(dbenv, lt);
	return (0);
}


/*
 * __lock_collect_pp --
 *	DB_ENV->lock_collect pre/post processing.
 *
 * PUBLIC: int __lock_collect_pp __P((DB_ENV *, collect_locks_f, void *));
 */
int
__lock_collect_pp(dbenv, func, arg)
	DB_ENV *dbenv;
	collect_locks_f func;
	void *arg;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
		dbenv->lk_handle, "DB_ENV->lock_collect", DB_INIT_LOCK);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;

	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __lock_collect(dbenv, func, arg);

	if (rep_check)
		__env_rep_exit(dbenv);

	return (ret);
}


/*
 * COMDB2 MODIFICATION
 *
 * PUBLIC: void __lock_dump_locker_int __P((DB_LOCKTAB *, DB_LOCKER *, FILE *, int));
 */
void
__lock_dump_locker_int(lt, lip, fp, just_active_locks)
	DB_LOCKTAB *lt;
	DB_LOCKER *lip;
	FILE *fp;
	int just_active_locks;
{
	struct __db_lock *lp;
	time_t s;
	char buf[64];
	int have_interesting_locks;
	int have_waiters = 0;

	if (just_active_locks &&lip->nlocks == 0)
		return;

	if (lip->has_waiters)
		have_waiters = 1;
	have_interesting_locks = 0;
	/* NB: just dumping active locks via this function will not print
	 * lockers which have only one lock in WAIT status -- use LOCK_DUMP_OBJECTS
	 * instead if you want to see those lockers (ex. in case of a deadlock) */
	if (just_active_locks) {
		lp = SH_LIST_FIRST(&lip->heldby, __db_lock);

		if (lp !=NULL) {
			for (; lp !=NULL;
				lp = SH_LIST_NEXT(lp, locker_links, __db_lock)) {
				DB_LOCKOBJ *lockobj;
				db_pgno_t pgno;
				u_int32_t *fidp, type;
				u_int8_t *ptr;
				char *namep;

				lockobj = lp->lockobj;
				ptr = lockobj->lockobj.data;

				if (lockobj->lockobj.size ==
					sizeof(struct __db_ilock)) {
					/* Assume this is a DBT lock. */
					memcpy(&pgno, ptr, sizeof(db_pgno_t));

					fidp =
						(u_int32_t *) (ptr
						+sizeof(db_pgno_t));
					type =
						*(u_int32_t *) (ptr
						+sizeof(db_pgno_t) +
						DB_FILE_ID_LEN);
					if (__dbreg_get_name(lt->dbenv,
						(u_int8_t *) fidp, &namep) != 0)
						namep = NULL;

					if (type == DB_PAGE_LOCK ||
						type == DB_RECORD_LOCK) {
						have_interesting_locks = 1;
						break;
					}
				} else {
					have_interesting_locks = 1;
					break;
				}
			}
		}
	}
	if (!just_active_locks)
		logmsg(LOGMSG_USER, "%8x havelocks %d\n", lip->id, have_interesting_locks);

	if (just_active_locks &&!have_interesting_locks)
		return;

	if (!just_active_locks)
		logmsgf(LOGMSG_USER, fp, "%8lx dd=%2ld locks held %-4d write locks %-4d waiters %s thread %lu (0x%lx)",
			(u_long)lip->id, (long)lip->dd_id, lip->nlocks,
			lip->nwrites, have_waiters ? "Y" : "N", lip->tid, lip->tid);

	logmsgf(LOGMSG_USER, fp, "%s", F_ISSET(lip, DB_LOCKER_DELETED) ? "(D)" : "   ");

	struct tm mytime;
	if (LOCK_TIME_ISVALID(&lip->tx_expire)) {
		s = lip->tx_expire.tv_sec;
		strftime(buf, sizeof(buf), "%m-%d-%H:%M:%S", localtime_r(&s, &mytime));
		logmsgf(LOGMSG_USER, fp,
			"expires %s.%lu", buf, (u_long)lip->tx_expire.tv_usec);
	}
	if (F_ISSET(lip, DB_LOCKER_TIMEOUT))
		logmsgf(LOGMSG_USER, fp, " lk timeout %u", lip->lk_timeout);

	if (LOCK_TIME_ISVALID(&lip->lk_expire)) {
		s = lip->lk_expire.tv_sec;
		strftime(buf, sizeof(buf), "%m-%d-%H:%M:%S", localtime_r(&s, &mytime));
		logmsgf(LOGMSG_USER, fp,
			" lk expires %s.%lu", buf, (u_long)lip->lk_expire.tv_usec);
	}
	logmsgf(LOGMSG_USER, fp, "\n");

	lp = SH_LIST_FIRST(&lip->heldby, __db_lock);

	if (lp !=NULL) {
		for (; lp !=NULL;
			lp = SH_LIST_NEXT(lp, locker_links, __db_lock))
			 __lock_printlock_int(lt, lp, 1, fp, just_active_locks);
		logmsgf(LOGMSG_USER, fp, "\n");
	}
}

static void
__lock_dump_object(lt, op, fp, just_active_locks)
	DB_LOCKTAB *lt;
	DB_LOCKOBJ *op;
	FILE *fp;
	int just_active_locks;
{
	struct __db_lock *lp;
	int printed;

	for (lp =
		SH_TAILQ_FIRST(&op->holders, __db_lock);
		lp !=NULL; lp = SH_TAILQ_NEXT(lp, links, __db_lock))
		printed =
			__lock_printlock_int(lt, lp, 1, fp, just_active_locks);
	for (lp = SH_TAILQ_FIRST(&op->waiters, __db_lock); lp !=NULL;
		lp = SH_TAILQ_NEXT(lp, links, __db_lock))
		printed =
			__lock_printlock_int(lt, lp, 1, fp, just_active_locks);

	/*
	 * if (printed)
	 * fprintf(fp, "\n");
	 */
}

/* XXX It's easier to diff against master & replicant locks if I don't print the lid */
#undef TRACE_DIFF


/*
 * __lock_printheader --
 */
static void
__lock_printheader(fp)
	FILE *fp;
{
#ifdef TRACE_DIFF
	logmsgf(LOGMSG_USER, fp, "%-10s%-4s %-7s %s\n",
	    "Mode",
	    "Count", "Status", "----------------- Object ---------------");
#else
	logmsgf(LOGMSG_USER, fp, "%-14s %-8s %-8s %-10s%-4s %-7s %s\n",
	    "TID", "MRLocker", "Locker", "Mode",
	    "Count", "Status", "----------------- Object ---------------");
#endif
}

/*
 * __lock_printlock --
 *
 * PUBLIC: void __lock_printlock __P((DB_LOCKTAB *,
 * PUBLIC:      struct __db_lock *, int, FILE *));
 */
int
__lock_printlock_int(lt, lp, ispgno, fp, just_active_locks)
	DB_LOCKTAB *lt;
	struct __db_lock *lp;
	int ispgno;
	FILE *fp;
	int just_active_locks;
{
	DB_LOCKOBJ *lockobj;
	db_pgno_t pgno;
	u_int32_t *fidp, type;
	u_int8_t *ptr;
	char *namep;
	const char *mode, *status;

	/* Make it easy to call from the debugger. */
	if (fp == NULL)
		fp = stderr;

    mode = mode_to_str(lp->mode);
    status = status_to_str(lp->status);

    /* peek at type, skip if handle, if asked */ 
    if (just_active_locks) {
        lockobj = lp->lockobj;
	    ptr = lockobj->lockobj.data;
        if (ispgno && lockobj->lockobj.size == sizeof(struct __db_ilock)) {
            /* Assume this is a DBT lock. */
            memcpy(&pgno, ptr, sizeof(db_pgno_t));
            fidp = (u_int32_t *)(ptr + sizeof(db_pgno_t));
            type = *(u_int32_t *)(ptr + sizeof(db_pgno_t) + DB_FILE_ID_LEN);
            if (type != DB_PAGE_LOCK && type != DB_RECORD_LOCK)
                return 0;
        }
    }


#ifdef TRACE_DIFF
	logmsgf(LOGMSG_USER, fp, "%-10s %4lu %-7s ",
	        mode, (u_long)lp->refcount, status);
#else
	DB_LOCKER *mlockerp = R_ADDR(&lt->reginfo, lp->holderp->master_locker);
	logmsgf(LOGMSG_USER, fp, "0x%lx %8x %8x %-10s %4lu %-7s ",
			lp->holderp->tid, mlockerp->id, lp->holderp->id, mode, (u_long)lp->refcount, status);
#endif

	lockobj = lp->lockobj;
	ptr = lockobj->lockobj.data;

	if (ispgno && lockobj->lockobj.size == sizeof(struct __db_ilock)) {
		/* Assume this is a DBT lock. */
		memcpy(&pgno, ptr, sizeof(db_pgno_t));

		fidp = (u_int32_t *) (ptr +sizeof(db_pgno_t));
		type = *(u_int32_t *) (ptr +sizeof(db_pgno_t) + DB_FILE_ID_LEN);
		if (__dbreg_get_name(lt->dbenv, (u_int8_t *) fidp, &namep) != 0)
			namep = NULL;

		if (just_active_locks &&type !=DB_PAGE_LOCK &&
		    type !=DB_RECORD_LOCK)
			return 0;

		if (namep == NULL)
			logmsgf(LOGMSG_USER, fp, "(%lx %lx %lx %lx %lx)",
			    (u_long)fidp[0], (u_long)fidp[1], (u_long)fidp[2],
			    (u_long)fidp[3], (u_long)fidp[4]);
		else
			logmsgf(LOGMSG_USER, fp, "%-25s", namep);
		logmsgf(LOGMSG_USER, fp, "%-7s %7lu\n",
		    type == DB_PAGE_LOCK ? " page" :
		    type == DB_RECORD_LOCK ? " record" : " handle",
		    (u_long)pgno);
	}
	/* see if its a comdb2 row lock */
	else if (lockobj->lockobj.size == 30) {
		char fileid[DB_FILE_ID_LEN];
		extern int gbl_rowlocks;
		unsigned long long genid;
		int is_keyhash = 0;

		memcpy(fileid, ptr, DB_FILE_ID_LEN);
		memcpy(&genid, ptr + DB_FILE_ID_LEN + sizeof(short),
		    sizeof(unsigned long long));

		fidp = (u_int32_t*) fileid;

		if (__dbreg_get_name(lt->dbenv, (u_int8_t *)fidp, &namep) != 0)
			namep = NULL;

		if (namep == NULL)
			logmsgf(LOGMSG_USER, fp, "(%lx %lx %lx %lx %lx)",
			    (u_long)fidp[0], (u_long)fidp[1], (u_long)fidp[2],
			    (u_long)fidp[3], (u_long)fidp[4]);
		else {
			int l=strlen(namep);
			logmsgf(LOGMSG_USER, fp, "%-25s", namep);
			if (gbl_rowlocks && l >= 5 &&
			    !strncmp(&namep[l-5], "index", 5))
				is_keyhash = 1;
		}

		if(is_keyhash)
			logmsgf(LOGMSG_USER, fp, " keyhash %llx\n", genid);
		else
			logmsgf(LOGMSG_USER, fp, " rowlock %llx\n", genid);
	}
	/* see if its a comdb2 minmax lock */
	else if (lockobj->lockobj.size == 31) {
		char fileid[DB_FILE_ID_LEN];
		char minmax = 0;

		memcpy(fileid, ptr, DB_FILE_ID_LEN);

		memcpy(&minmax, ((char *)ptr)+30, sizeof(char));

		fidp = (u_int32_t *) fileid;

		if (__dbreg_get_name(lt->dbenv, (u_int8_t *) fidp, &namep) != 0)
			namep = NULL;

		if (namep == NULL)
			logmsgf(LOGMSG_USER, fp, "(%lx %lx %lx %lx %lx)",
			    (u_long)fidp[0], (u_long)fidp[1], (u_long)fidp[2],
			    (u_long)fidp[3], (u_long)fidp[4]);
		else
			logmsgf(LOGMSG_USER, fp, "%-25s", namep);

		logmsgf(LOGMSG_USER, fp, " minmax %s\n", minmax == 0 ? "min" : "max");
	}

	/* see if its a comdb2 table lock */
	else if (lockobj->lockobj.size == 32) {
		char tablename[64];

		bzero(tablename, 64);

		memcpy(tablename, ptr, 28);

		logmsgf(LOGMSG_USER, fp, " tablelock %s\n", tablename);
	}

	else if (lockobj->lockobj.size == 8) {
		DB_LSN lsn;
		memcpy(&lsn, ptr, sizeof(DB_LSN));
		logmsg(LOGMSG_USER, "lsn %u:%u\n", lsn.file, lsn.offset);
	}

	else if (lockobj->lockobj.size == 20) {
		char fileid[DB_FILE_ID_LEN];
		memcpy(fileid, ptr, DB_FILE_ID_LEN);

		fidp = (u_int32_t *) fileid;

		if (__dbreg_get_name(lt->dbenv, (u_int8_t *) fidp, &namep) != 0)
			namep = NULL;

		if (namep == NULL)
			logmsgf(LOGMSG_USER, fp, "(%lx %lx %lx %lx %lx)",
			    (u_long)fidp[0], (u_long)fidp[1], (u_long)fidp[2],
			    (u_long)fidp[3], (u_long)fidp[4]);
		else
			logmsgf(LOGMSG_USER, fp, "%-25s", namep);

		logmsgf(LOGMSG_USER, fp, " stripelock\n");
	}


	/* else its some other unknown type of lock */
	else {
		logmsgf(LOGMSG_USER, fp, "0x%lx ", (u_long)R_OFFSET(&lt->reginfo, lockobj));
		__db_pr(ptr, lockobj->lockobj.size, fp);
		logmsgf(LOGMSG_USER, fp, "\n");
	}

	return 1;
}

/* COMDB2 MODIFICATION: another parameter to skip handles */
void
__lock_printlock(lt, lp, ispgno, fp)
	DB_LOCKTAB *lt;
	struct __db_lock *lp;
	int ispgno;
	FILE *fp;
{
	__lock_printlock_int(lt, lp, ispgno, fp, 0);
}


/* COMDB2 MODIFICATION */
/*
 * __lock_checklocker --
 *	Retrieve a locker; if it exists, check if there are any locks hold
 * by it (return 1 in this case);  we'll use this to track if a 
 * curtran operation is leaking locks
 *
 * NOTE: we could call something like __get_locker to faster retrieve a lock
 *       this is gonna troll through the whole list, which is more paranoic
 *       until we found the locker (since we intend to call this before 
 *       dropping curtran, locker is guaranteed to exist)
 * 
 * PUBLIC: int __lock_locker_haslocks __P((DB_ENV *, u_int32_t));
 */
int
__lock_locker_haslocks(dbenv, lockerid)
	DB_ENV *dbenv;
	u_int32_t lockerid;
{
	DB_LOCKER *lip;
	DB_LOCKREGION *lrp;
	DB_LOCKTAB *lt;
	u_int32_t i;
	u_int32_t j;
	int done = 0;
	int haslocks = 0;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lk_handle, "lock_dump_region", DB_INIT_LOCK);

	lt = dbenv->lk_handle;
	lrp = lt->reginfo.primary;


	LOCKREGION(dbenv, lt);

	for (i = 0; i <gbl_lkr_parts; ++i) {
		lock_locker_partition(lrp, i);

		for (j = 0; j < lrp->locker_p_size && !done; j++) {
			for (lip =
			    SH_TAILQ_FIRST(&lrp->locker_tab[i][j], __db_locker);
			    lip != NULL &&!done;
			    lip = SH_TAILQ_NEXT(lip, links, __db_locker)) {
				if (lip->id == lockerid) {
					struct __db_lock *lp;
					lp = SH_LIST_FIRST(&lip->heldby,
					    __db_lock);
					if (lp !=NULL) {
						haslocks = 1;
					}
					done = 1;
				}
			}
		}
		unlock_locker_partition(lrp, i);
	}
	UNLOCKREGION(dbenv, lt);

	return haslocks;
}

/* TODO: fix (we dont print anything) or remove
void
berkdb_dump_locks_for_tran(DB_ENV *dbenv, DB_TXN *txn)
{
	DB_LOCKTAB *lt;
	DB_LOCKREGION *lrp;
	DB_LOCKER *lip;
	int done = 0;
	u_int32_t i;
	u_int32_t j;

	int lockerid;
	lt = dbenv->lk_handle;
	lrp = lt->reginfo.primary;

	LOCKREGION(dbenv, lt);
	lockerid = txn->txnid;

	for (i = 0; i <gbl_lk_parts; ++i) {
		lock_locker_partition(lrp, i);

		for (j = 0; j < lrp->locker_p_size && !done; j++) {
			for (lip = SH_TAILQ_FIRST(&lrp->locker_tab[i][j], __db_locker);
			    lip != NULL &&!done;
			    lip = SH_TAILQ_NEXT(lip, links, __db_locker)) {
				if (lip->id == lockerid) {
					struct __db_lock *lp;

					for (lp = SH_LIST_FIRST(&lip->heldby, __db_lock); lp;
					    lp = SH_LIST_NEXT(lp, locker_links, __db_lock)) {
						DB_LOCKOBJ *lockobj;
						lockobj = lp->lockobj;
					}
					done = 1;
				}
			}
		}
		unlock_locker_partition(lrp, i);
	}
	UNLOCKREGION(dbenv, lt);
}
*/

void
berkdb_dump_lockers_summary(DB_ENV *dbenv)
{
	DB_LOCKTAB *lt = dbenv->lk_handle;
	DB_LOCKREGION *region = lt->reginfo.primary;
	DB_LOCKER *l;
	int i = 0;
	lock_lockers(region);
	l = SH_TAILQ_FIRST(&region->lockers, __db_locker);

	while (l) {
		DB_LOCKER *m = NULL;

		if (l->master_locker == INVALID_ROFF)
			m = (DB_LOCKER *)R_ADDR(&lt->reginfo, l->master_locker);
		logmsg(LOGMSG_USER, "%d: locker id:%5u (0x%x) nlocks:%u p:%u m:%u w:%c\n",
		    ++i, l->id, l->id, l->nlocks,
		    (l->parent_locker ==
			INVALID_ROFF ? 0 : ((DB_LOCKER *)R_ADDR(&lt->reginfo,
				l->parent_locker))->id), (m ? m->id : 0),
		    (l->wstatus ? 'y' : (m &&m->wstatus ? 'p' : 'n'))
		    );
		l = SH_TAILQ_NEXT(l, ulinks, __db_locker);
	}
	unlock_lockers(region);
}

// PUBLIC: int __lock_dump_active_locks __P((DB_ENV *, FILE *));
int __lock_dump_active_locks(
		DB_ENV *dbenv,
		FILE *fp)
{
	/* "o" will print all active objects in object order, including 
	 * lockers that have onle one lock in WAIT status */
	return __lock_dump_region_int(dbenv, "o", fp, 1 /*just_active_locks*/);
}
