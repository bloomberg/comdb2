/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 *
 * $Id: lock.h,v 11.47 2003/04/16 18:23:27 ubell Exp $
 */

#ifndef	_DB_LOCK_H_
#define	_DB_LOCK_H_

#include <assert.h>
#include <compile_time_assert.h>

extern size_t gbl_lk_parts;
extern size_t gbl_lkr_parts;
extern size_t gbl_lk_hash;
extern size_t gbl_lkr_hash;

#define	DB_LOCK_DEFAULT_N	1000	/* Default # of locks in region. */

#include <dbinc/maxstackframes.h>

/*
 * The locker id space is divided between the transaction manager and the lock
 * manager.  Lock IDs start at 1 and go to DB_LOCK_MAXID.  Txn IDs start at
 * DB_LOCK_MAXID + 1 and go up to TXN_MAXIMUM.
 */
#define	DB_LOCK_INVALIDID	0
#define DB_LOCK_MAXID_DEFAULT 0x7fffffff
extern int DB_LOCK_MAXID;

/*
 * Out of band value for a lock.  Locks contain an offset into a lock region,
 * so we use an invalid region offset to indicate an invalid or unset lock.
 */
#define	LOCK_INVALID		INVALID_ROFF
#define LATCH_OFFSET		-1
#define LOCK_ISLATCH(lock)	((lock).off == LATCH_OFFSET)
#define	LOCK_ISSET(lock)	((lock).off != LOCK_INVALID)
#define	LOCK_INIT(lock)		((lock).off = LOCK_INVALID)

/*
 * Macro to identify a write lock for the purpose of counting locks
 * for the NUMWRITES option to deadlock detection.
 */
#define	IS_WRITELOCK(m) ( (m) == DB_LOCK_WRITE  || (m) == DB_LOCK_WWRITE  || (m) == DB_LOCK_WRITEADD || (m) == DB_LOCK_WRITEDEL || (m) == DB_LOCK_IWRITE  || (m) == DB_LOCK_IWR )

/*
 * Lock timers.
 */
typedef struct {
	u_int32_t	tv_sec;		/* Seconds. */
	u_int32_t	tv_usec;	/* Microseconds. */
} db_timeval_t;

#define	LOCK_TIME_ISVALID(time)		((time)->tv_sec != 0)
#define	LOCK_SET_TIME_INVALID(time)	((time)->tv_sec = 0)
#define	LOCK_TIME_ISMAX(time)		((time)->tv_sec == UINT32_T_MAX)
#define	LOCK_SET_TIME_MAX(time)		((time)->tv_sec = UINT32_T_MAX)
#define	LOCK_TIME_EQUAL(t1, t2)						\
	((t1)->tv_sec == (t2)->tv_sec && (t1)->tv_usec == (t2)->tv_usec)
#define	LOCK_TIME_GREATER(t1, t2)					\
	((t1)->tv_sec > (t2)->tv_sec ||					\
	((t1)->tv_sec == (t2)->tv_sec && (t1)->tv_usec > (t2)->tv_usec))

#ifdef LOCKMGRDBG
typedef struct
{
	const char	*file;
	const char	*func;
	size_t 		line;
	pthread_t	thd;
} Comdb2LockDebug;

#ifdef  __x86_64
#define FLUFF uint8_t fluff[88]
#else
#define FLUFF uint8_t fluff[1]
#endif

typedef struct
{
	pthread_mutex_t	mtx;
	Comdb2LockDebug	lock;
	Comdb2LockDebug	unlock;
	FLUFF;
} PthreadMutexWithFluff;

#else // no LOCKMGRDBG

#ifdef  __x86_64
#  ifdef __APPLE__
#    define FLUFF uint8_t fluff[128]
#  else
#    define FLUFF uint8_t fluff[152]
#  endif
#else
#define FLUFF uint8_t fluff[1]
#endif

typedef struct
{
	pthread_mutex_t	mtx;
	FLUFF;
} PthreadMutexWithFluff;

#endif // LOCKMGRDBG

#ifdef  __x86_64
BB_COMPILE_TIME_ASSERT(lockfluff_not_192, sizeof(PthreadMutexWithFluff) == 192);
#endif

typedef SH_TAILQ_HEAD(LockerTab, __db_locker) LockerTab;
typedef SH_TAILQ_HEAD(ObjTab, __db_lockobj) ObjTab;

struct __db_latch;
struct __db_lock_lsn;
struct __db_lockerid_latch_node;
struct __db_lockerid_latch_list;

/*
 * DB_LOCKREGION --
 *	The lock shared region.
 */
typedef struct __db_lockregion {
	u_int32_t	need_dd;	/* flag for deadlock detector */
	u_int32_t	detect;		/* run dd on every conflict */
	u_int32_t	dd_gen;		/* generation of deadlock detector ID (dd_id) */
	db_timeval_t	next_timeout;	/* next time to expire a lock */
	SH_TAILQ_HEAD(__dobj, __db_lockobj) dd_objs;	/* objects with waiters */
	SH_TAILQ_HEAD(__lkrs, __db_locker) lockers;	/* list of lockers */
	SH_TAILQ_HEAD(__wlkrs, __db_locker) wlockers;	/* list of lockers in waiting status */
	db_timeout_t	lk_timeout;	/* timeout for locks. */
	db_timeout_t	tx_timeout;	/* timeout for txns. */
	roff_t		conf_off;	/* offset of conflicts array */
	roff_t		osynch_off;	/* offset of the object mutex table */
	roff_t		lsynch_off;	/* offset of the locker mutex table */
	DB_LOCK_STAT	stat;		/* stats about locking. */
#ifdef HAVE_MUTEX_SYSTEM_RESOURCES
	roff_t		maint_off;	/* offset of region maintenance info */
#endif
	/* Not partitioned */
	PthreadMutexWithFluff	dd_mtx;
	PthreadMutexWithFluff	lockers_mtx;

	/* Partitioned on locker id */
	u_int32_t		locker_p_size;	/* size of locker hash table per partition */
	LockerTab		**locker_tab;	/* beginning of locker hash table. */
	PthreadMutexWithFluff	*locker_tab_mtx;
	SH_TAILQ_HEAD(__flocker, __db_locker)	*free_lockers;	/* free locker header */
	uint8_t			*nwlkr_scale;	/* scale num of lockers to be added */

	/* Partitioned on lock */
	u_int32_t		object_p_size;	/* size of object hash table per partition */
	ObjTab			**obj_tab;	/* beginning of object hash table. */
	PthreadMutexWithFluff	*obj_tab_mtx;
	SH_TAILQ_HEAD(__fobj, __db_lockobj)	*free_objs;	/* free obj header */
	uint8_t			*nwobj_scale;	/* scale num of objs to be added */

	SH_TAILQ_HEAD(__flock, __db_lock)	*free_locks;	/* free lock header */
	uint8_t			*nwlk_scale;	/* scale num of locks to be added */

	/* Latches */
	u_int32_t		max_latch;
	u_int32_t		max_latch_lockerid;
	struct __db_latch	*latches;
	struct __db_lockerid_latch_list	*lockerid_latches;
	u_int32_t		ilock_step;
	u_int32_t		lockerid_node_step;
	u_int32_t		db_lock_lsn_step;
	int			blocking_latches;
	pthread_mutex_t		ilock_latch_lk;
	struct __db_ilock_latch	*ilock_latch_head;
	pthread_mutex_t 	lockerid_node_lk;
	struct __db_lockerid_latch_node	*lockerid_node_head;
	pthread_mutex_t 	db_lock_lsn_lk;
	SH_LIST_HEAD(_regionlsns, __db_lock_lsn) db_lock_lsn_head;
} DB_LOCKREGION;

typedef struct __sh_dbt {
	u_int32_t size;
	void 	 *data;
} SH_DBT;

/*
 * Object structures;  these live in the object hash table.
 */
typedef struct __db_lockobj {
	SH_DBT	lockobj;		/* Identifies object locked. */
	SH_TAILQ_ENTRY(__db_lockobj) links;	/* Links for free list or hash list. */
	SH_TAILQ_ENTRY(__db_lockobj) dd_links;	/* Links for dd list. */
	SH_TAILQ_HEAD(__waitl, __db_lock) waiters;	/* List of waiting locks. */
	SH_TAILQ_HEAD(__holdl, __db_lock) holders;	/* List of held locks. */
					/* Declare room in the object to hold
					 * typical DB lock structures so that
					 * we do not have to allocate them from
					 * shalloc at run-time. */
	u_int8_t objdata[32];		/* A little space for well known lock objs
					 * to avoids a few mallocs
					 * u_int8_t objdata[sizeof(struct __db_ilock)]; */
	u_int32_t partition;
	u_int32_t index;
	u_int32_t generation;
} DB_LOCKOBJ;

typedef struct __db_ilock_latch
{
	u_int8_t lock[28];
	db_lockmode_t lock_mode;
	pthread_mutex_t	lsns_mtx;
	SH_LIST_HEAD(_latchlsns, __db_lock_lsn) lsns;	/* logical lsns that hold this lock. */
	u_int32_t nlsns;
	int count;
	struct __db_ilock_latch *next;
	struct __db_ilock_latch *prev;
	struct __db_latch *latch;
} DB_ILOCK_LATCH;

typedef struct __db_lockerid_latch_node DB_LOCKERID_LATCH_NODE;

typedef struct __db_latch {
	pthread_mutex_t lock;		/* Mutex */
	u_int32_t lockerid;		/* Owner lockid for quick lookup */
	DB_LOCKERID_LATCH_NODE *locker; /* Pointer to owner */
	DB_ILOCK_LATCH *listhead;	/* Multiple latches can hash to here */
	u_int32_t count;		/* Number of locks in the locklist */
	u_int32_t wrcount;		/* Number of wrlocks in the locklist */
	pthread_t tid;			/* Owner tid */
	struct __db_latch *next;
	struct __db_latch *prev;
} DB_LATCH;

struct __db_lockerid_latch_node {
	u_int32_t lockerid;
	u_int32_t flags;
	u_int32_t ntrackedlocks;
	u_int32_t maxtrackedlocks;
	int has_pglk_lsn;
	struct __db_ilock_latch **tracked_locklist;
	struct __db_lockerid_latch_node *next;
	struct __db_lockerid_latch_node *prev;
	DB_LATCH *head;
};

typedef struct __db_lockerid_latch_list {
	pthread_mutex_t lock;
	DB_LOCKERID_LATCH_NODE *head;
} DB_LOCKERID_LATCH_LIST;


typedef struct snap_uid_t snap_uid_t;

/*
 * Locker structures; these live in the locker hash table.
 */
typedef struct __db_locker {
	pthread_t tid;
	struct __db_lock **tracked_locklist;
	roff_t master_locker;		/* Locker of master transaction. */
	roff_t parent_locker;		/* Parent of this child. */
	SH_LIST_HEAD(_child, __db_locker) child_locker;	/* List of descendant txns;
						   only used in a "master"
						   txn. */
	SH_LIST_ENTRY(__db_locker) child_link;	/* Links transactions in the family;
					   elements of the child_locker
					   list. */
	SH_TAILQ_ENTRY(__db_locker) links;		/* Links for free and hash list. */
	SH_TAILQ_ENTRY(__db_locker) ulinks;	/* Links in-use list. */
	SH_TAILQ_ENTRY(__db_locker) wlinks;	/* Links in-use list. */
	SH_LIST_HEAD(_held, __db_lock) heldby;	/* Locks held by this locker. */
	db_timeval_t	lk_expire;	/* When current lock expires. */
	db_timeval_t	tx_expire;	/* When this txn expires. */
	db_timeout_t	lk_timeout;	/* How long do we let locks live. */
	snap_uid_t      *snap_info; /* contains cnonce to print deadlock info */
	u_int32_t id;			/* Locker id. */
	u_int32_t dd_id;		/* Deadlock detector id. */
	u_int32_t nlocks;		/* Number of locks held. */
	u_int32_t npagelocks;		/* Number of pagelocks held. */
	u_int32_t nwrites;		/* Number of write locks held. */
	u_int32_t nhandlelocks;		/* Number of handle locks held. */
	u_int32_t priority;		/* number of locks accumulated during txn life */
	u_int32_t num_retries;	/* number of times txn was retried */
	u_int32_t partition;
	u_int32_t ntrackedlocks;
	u_int32_t maxtrackedlocks;
#define	DB_LOCKER_DELETED		0x0001
#define	DB_LOCKER_DIRTY			0x0002
#define	DB_LOCKER_INABORT		0x0004
#define	DB_LOCKER_TIMEOUT		0x0008
#define	DB_LOCKER_KILLME		0x0010
#define DB_LOCKER_LOGICAL   		0x0020
#define DB_LOCKER_DEADLOCK  		0x0040
#define DB_LOCKER_LOWPRI    		0x0080
#define DB_LOCKER_TRACK         	0x0100
#define DB_LOCKER_READONLY      	0x0200
#define DB_LOCKER_IN_LOGICAL_ABORT 	0x0800
#define DB_LOCKER_TRACK_WRITELOCKS      0x8000
	u_int8_t has_waiters;
	u_int32_t flags;
	u_int8_t has_pglk_lsn;
	u_int32_t   dd_gen;		/* generation of the locker's deadlock detector ID */
	u_int32_t   dd_in_wlockers; /* whether the locker is in wlockers list */
} DB_LOCKER;

/*
 * DB_LOCKTAB --
 *	The primary library lock data structure (i.e., the one referenced
 * by the environment, as opposed to the internal one laid out in the region.)
 */
typedef struct __db_locktab {
	DB_ENV		*dbenv;		/* Environment. */
	REGINFO		 reginfo;	/* Region information. */
	u_int8_t	*conflicts;	/* Pointer to conflict matrix. */
} DB_LOCKTAB;

/* Test for conflicts. */
#define	CONFLICTS(T, R, HELD, WANTED) \
	(T)->conflicts[(HELD) * (R)->stat.st_nmodes + (WANTED)]

#define	OBJ_LINKS_VALID(O, L) ((O)->L.tqe_prev != (void *)-1)

struct __db_lockobj_lsn {
	__DB_DBT_INTERNAL

	SH_LIST_HEAD(_lkobj_lsns, __db_lock_lsn) lsns;	/* logical lsns that hold this lock. */
	u_int16_t nlsns;
};

struct __db_lock_lsn {
	SH_LIST_ENTRY(__db_lock_lsn) lsn_links;	/* List of llsns that hold the lock. */
	DB_LSN llsn;
};

struct __db_lock {
	/*
	 * Wait on mutex to wait on lock.  You reference your own mutex with
	 * ID 0 and others reference your mutex with ID 1.
	 */
	DB_MUTEX	mutex;

	//u_int32_t	holder;		/* Who holds this lock. */
	u_int32_t	gen;		/* Generation count. */
	SH_TAILQ_ENTRY(__db_lock) links;	/* Free or holder/waiter list. */
	SH_LIST_ENTRY(__db_lock) locker_links;	/* List of locks held by a locker. */
	u_int32_t	refcount;	/* Reference count the lock. */
	db_lockmode_t	mode;		/* What sort of lock. */
	DB_LOCKOBJ	*lockobj;		/* Ptr to lock object. */
	db_status_t	status;		/* Status of this lock. */
	/* COMDB2 MODIFICATION */
	DB_LOCKER	*holderp;
	DBT		*dbtobj;
	u_int32_t	lpartition;

	pthread_mutex_t	lsns_mtx;
	SH_LIST_HEAD(_lsns, __db_lock_lsn) lsns;	/* logical lsns that hold this lock. */
	u_int32_t nlsns;

    int         stackid;
};

/*
 * Flag values for __lock_put_internal:
 * DB_LOCK_DOALL:     Unlock all references in this lock (instead of only 1).
 * DB_LOCK_FREE:      Free the lock (used in checklocker).
 * DB_LOCK_NOPROMOTE: Don't bother running promotion when releasing locks
 *		      (used by __lock_put_internal).
 * DB_LOCK_UNLINK:    Remove from the locker links (used in checklocker).
 * Make sure that these do not conflict with the interface flags because
 * we pass some of those around (i.e., DB_LOCK_REMOVE).
 */
#define	DB_LOCK_DOALL		0x010000
#define	DB_LOCK_FREE		0x020000
#define	DB_LOCK_NOPROMOTE	0x040000
#define	DB_LOCK_UNLINK		0x080000
#define	DB_LOCK_NOWAITERS	0x100000

/*
 * Macros to get/release different types of mutexes.
 */
#define	OBJECT_INDX(lt, reg, obj, ndx, partition)			\
do {									\
	u_int32_t hash = __lock_ohash(obj);				\
	ndx =  hash % (reg)->object_p_size;				\
	partition = hash % gbl_lk_parts;				\
} while(0)

#define	LOCKER_INDX(lt, reg, locker, ndx)				\
	ndx = (locker) % (reg)->locker_p_size;


#if 0
#define	LOCKREGION(dbenv, lt)  R_LOCK((dbenv), &(lt)->reginfo)
#define	UNLOCKREGION(dbenv, lt)  R_UNLOCK((dbenv), &(lt)->reginfo)
#else
#define	LOCKREGION(dbenv, lt)
#define	UNLOCKREGION(dbenv, lt)
#endif

#ifdef LOCKMGRDBG
#define lock_lockers(region) \
do { \
	Pthread_mutex_lock(&(region)->lockers_mtx.mtx); \
	(region)->lockers_mtx.lock.file = __FILE__; \
	(region)->lockers_mtx.lock.func = __func__; \
	(region)->lockers_mtx.lock.line = __LINE__; \
	(region)->lockers_mtx.lock.thd  = pthread_self(); \
} while (0)

#define lock_obj_partition(region, partition)\
do { \
	assert((partition) < gbl_lk_parts); \
	Pthread_mutex_lock(&(region)->obj_tab_mtx[(partition)].mtx); \
	(region)->obj_tab_mtx[(partition)].lock.file = __FILE__; \
	(region)->obj_tab_mtx[(partition)].lock.func = __func__; \
	(region)->obj_tab_mtx[(partition)].lock.line = __LINE__; \
	(region)->obj_tab_mtx[(partition)].lock.thd  = pthread_self(); \
} while (0)

#define lock_locker_partition(region, partition) \
do { \
	assert((partition) < gbl_lkr_parts); \
	Pthread_mutex_lock(&(region)->locker_tab_mtx[(partition)].mtx); \
	(region)->locker_tab_mtx[(partition)].lock.file = __FILE__; \
	(region)->locker_tab_mtx[(partition)].lock.func = __func__; \
	(region)->locker_tab_mtx[(partition)].lock.line = __LINE__; \
	(region)->locker_tab_mtx[(partition)].lock.thd  = pthread_self(); \
} while (0)

#define lock_detector(region) \
do { \
	Pthread_mutex_lock(&(region)->dd_mtx.mtx); \
	(region)->dd_mtx.lock.file = __FILE__; \
	(region)->dd_mtx.lock.func = __func__; \
	(region)->dd_mtx.lock.line = __LINE__; \
	(region)->dd_mtx.lock.thd  = pthread_self(); \
} while (0)

#define unlock_lockers(region) \
do { \
	(region)->lockers_mtx.unlock.file = __FILE__; \
	(region)->lockers_mtx.unlock.func = __func__; \
	(region)->lockers_mtx.unlock.line = __LINE__; \
	(region)->lockers_mtx.unlock.thd  = pthread_self(); \
	Pthread_mutex_unlock(&(region)->lockers_mtx.mtx); \
} while (0)

#define unlock_obj_partition(region, partition) \
do { \
	assert((partition) < gbl_lk_parts); \
	(region)->obj_tab_mtx[(partition)].unlock.file = __FILE__; \
	(region)->obj_tab_mtx[(partition)].unlock.func = __func__; \
	(region)->obj_tab_mtx[(partition)].unlock.line = __LINE__; \
	(region)->obj_tab_mtx[(partition)].unlock.thd  = pthread_self(); \
	Pthread_mutex_unlock(&region->obj_tab_mtx[partition].mtx); \
} while (0)

#define unlock_locker_partition(region, partition) \
do { \
	assert((partition) < gbl_lkr_parts); \
	(region)->locker_tab_mtx[(partition)].unlock.file = __FILE__; \
	(region)->locker_tab_mtx[(partition)].unlock.func = __func__; \
	(region)->locker_tab_mtx[(partition)].unlock.line = __LINE__; \
	(region)->locker_tab_mtx[(partition)].unlock.thd  = pthread_self(); \
	Pthread_mutex_unlock(&(region)->locker_tab_mtx[(partition)].mtx); \
} while (0)

#define unlock_detector(region) \
do { \
	(region)->dd_mtx.unlock.file = __FILE__; \
	(region)->dd_mtx.unlock.func = __func__; \
	(region)->dd_mtx.unlock.line = __LINE__; \
	(region)->dd_mtx.unlock.thd  = pthread_self(); \
	Pthread_mutex_unlock(&(region)->dd_mtx.mtx); \
} while (0)

#else // no LOCKMGRDBG

#define lock_lockers(region) Pthread_mutex_lock(&(region)->lockers_mtx.mtx)
#define lock_obj_partition(region, partition) Pthread_mutex_lock(&(region)->obj_tab_mtx[(partition)].mtx)
#define lock_locker_partition(region, partition) Pthread_mutex_lock(&(region)->locker_tab_mtx[(partition)].mtx)
#define lock_detector(region) Pthread_mutex_lock(&(region)->dd_mtx.mtx)
#define unlock_lockers(region) Pthread_mutex_unlock(&(region)->lockers_mtx.mtx)
#define unlock_obj_partition(region, partition) Pthread_mutex_unlock(&region->obj_tab_mtx[partition].mtx)
#define unlock_locker_partition(region, partition) Pthread_mutex_unlock(&(region)->locker_tab_mtx[(partition)].mtx)
#define unlock_detector(region) Pthread_mutex_unlock(&(region)->dd_mtx.mtx)

#endif // LOCKMGRDBG



/* Note: Locker and Locker-Partition are two different locks */
#define GETLOCKER_CREATE	0x0001 //If not found, create new locker
#define GETLOCKER_KEEP_PART	0x0002 //Don't unlock locker-partition when done
#define GETLOCKER_DONT_LOCK	0x0004 //Don't lock locker when creating new
#define GETLOCKER_LOGICAL	0x0008 //Make this a logical locker

#define LOCK_GET_LIST_GETLOCK   0x0010
#define LOCK_GET_LIST_PRINTLOCK 0x0020
#define LOCK_GET_LIST_PAGELOGS  0x0040


int add_to_lock_partition(DB_ENV *, DB_LOCKTAB *, int partition, int num, struct __db_lock []);

int lock_list_parse_pglogs(DB_ENV *, DBT *, DB_LSN *, void **, u_int32_t *);

#include "dbinc_auto/lock_ext.h"

#endif /* !_DB_LOCK_H_ */
