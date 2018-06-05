/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2001-2003
 *	Sleepycat Software.  All rights reserved.
 */

#ifndef _REP_H_
#define	_REP_H_

#include "dbinc/rep_types.h"
/* Shared replication structure. */

typedef struct __rep {
	/*
	 * Due to alignment constraints on some architectures (e.g. HP-UX),
	 * DB_MUTEXes must be the first element of shalloced structures,
	 * and as a corollary there can be only one per structure.  Thus,
	 * db_mutex_off points to a mutex in a separately-allocated chunk.
	 */
	DB_MUTEX	mutex;		/* Region lock. */
	roff_t		db_mutex_off;	/* Client database mutex. */
	roff_t		tally_off;	/* Offset of the tally region. */
	roff_t		v2tally_off;	/* Offset of the vote2 tally region. */
	char*		eid;		/* Environment id. */
	char*		master_id;	/* ID of the master site. */
	u_int32_t	egen;		/* Replication election generation. */
	u_int32_t	gen;		/* Replication generation number. */
	u_int32_t	recover_gen;	/* Last generation number in log. */
    u_int32_t   committed_gen;  /* Last generation number written to a commit record */
    DB_LSN      committed_lsn;  /* Last committed-lsn */
	int		asites;		/* Space allocated for sites. */
	int		nsites;		/* Number of sites in group. */
	int		priority;	/* My priority in an election. */
	u_int32_t	gbytes;		/* Limit on data sent in single... */
	u_int32_t	bytes;		/* __rep_process_message call. */


#define	DB_REP_REQUEST_GAP	4
#define	DB_REP_MAX_GAP		128


#if 0
   /* COMDB2 */
#define	DB_REP_REQUEST_GAP	1
#define	DB_REP_MAX_GAP		32
#endif

	u_int32_t	request_gap;	/* # of records to receive before we
					 * request a missing log record. */
	u_int32_t	max_gap;	/* Maximum number of records before
					 * requesting a missing log record. */
	/* Status change information */
	u_int32_t	msg_th;		/* Number of callers in rep_proc_msg. */
	int		start_th;	/* A thread is in rep_start. */
	u_int32_t	handle_cnt;	/* Count of handles in library. */
	u_int32_t	op_cnt;		/* Multi-step operation count.*/
	int		in_recovery;	/* Running recovery now. */
	time_t		timestamp;	/* Recovery timestamp. */

	/* Vote tallying information. */
	int		sites;		/* Sites heard from. */
	char    *winner;		/* Current winner. */
	int		w_priority;	/* Winner priority. */
	u_int32_t	w_gen;		/* Winner generation. */
    u_int32_t   w_committed_gen; /* Winner committed generation */
	DB_LSN		w_lsn;		/* Winner LSN. */
	int		w_tiebreaker;	/* Winner tiebreaking value. */
	int		votes;		/* Number of votes for this site. */

	/* Statistics. */
	DB_REP_STAT	stat;

#define	REP_F_EPHASE1		0x001		/* In phase 1 of election. */
#define	REP_F_EPHASE2		0x002		/* In phase 2 of election. */
#define	REP_F_LOGSONLY		0x004		/* Log only; can't upgrade. */
#define	REP_F_MASTER		0x008		/* Master replica. */
#define	REP_F_MASTERELECT	0x010		/* Master elect */
#define	REP_F_NOARCHIVE		0x020		/* Rep blocks log_archive */
#define	REP_F_READY		0x040		/* Wait for txn_cnt to be 0. */
#define	REP_F_RECOVER		0x080		/* In recovery. */
#define	REP_F_TALLY		0x100		/* Tallied vote before elect. */
#define	REP_F_UPGRADE		0x200		/* Upgradeable replica. */
#define REP_F_WAITSTART     0x400       /* No logputs until start */
#define	REP_ISCLIENT	(REP_F_UPGRADE | REP_F_LOGSONLY)
	u_int32_t	flags;
} REP;

#define	IN_ELECTION(R)		F_ISSET((R), REP_F_EPHASE1 | REP_F_EPHASE2)
#define	IN_ELECTION_TALLY(R) \
	F_ISSET((R), REP_F_EPHASE1 | REP_F_EPHASE2 | REP_F_TALLY)
#define	IN_ELECTION_TALLY_WAITSTART(R) \
	F_ISSET((R), REP_F_EPHASE1 | REP_F_EPHASE2 | REP_F_TALLY | REP_F_WAITSTART)
#define	IS_REP_MASTER(dbenv)						\
	(REP_ON(dbenv) && ((DB_REP *)(dbenv)->rep_handle)->region &&	\
	    F_ISSET(((REP *)((DB_REP *)(dbenv)->rep_handle)->region),	\
	    REP_F_MASTER))

#define	IS_REP_CLIENT(dbenv)						\
	(REP_ON(dbenv) && ((DB_REP *)(dbenv)->rep_handle)->region &&	\
	    F_ISSET(((REP *)((DB_REP *)(dbenv)->rep_handle)->region),	\
	    REP_ISCLIENT))

#define	IS_REP_LOGSONLY(dbenv)						\
	(REP_ON(dbenv) && ((DB_REP *)(dbenv)->rep_handle)->region &&	\
	    F_ISSET(((REP *)((DB_REP *)(dbenv)->rep_handle)->region),	\
	    REP_F_LOGSONLY))

/*
 * Macros to figure out if we need to do replication pre/post-amble
 * processing.
 */
#define	IS_REPLICATED(E, D)						\
	(!F_ISSET((D), DB_AM_RECOVER | DB_AM_REPLICATION) &&		\
	REP_ON(E) && ((DB_REP *)((E)->rep_handle))->region != NULL &&	\
	((DB_REP *)((E)->rep_handle))->region->flags != 0)

#define	IS_ENV_REPLICATED(E) (!IS_RECOVERING(E) && REP_ON(E) &&		\
	((DB_REP *)((E)->rep_handle))->region != NULL &&		\
	((DB_REP *)((E)->rep_handle))->region->flags != 0)

/*
 * Per-process replication structure.
 *
 * There are 2 mutexes used in replication.
 * 1.  rep_mutexp - This protects the fields of the rep region above.
 * 2.  db_mutexp - This protects the bookkeeping database and all
 * of the components that maintain it.  Those components include
 * the following fields in the log region (see log.h):
 *	a. ready_lsn
 *	b. waiting_lsn
 *	c. verify_lsn
 *	d. wait_recs
 *	e. rcvd_recs
 *	f. max_wait_lsn
 * These fields in the log region are NOT protected by the log
 * region lock at all.
 *
 * The lock ordering protocol is that db_mutexp must be acquired
 * first and then either rep_mutexp, or the log region mutex may
 * be acquired if necessary.
 */
struct __db_rep {
	DB_MUTEX	*rep_mutexp;	/* Mutex for rep region */

	DB_MUTEX	*db_mutexp;	/* Mutex for bookkeeping database. */
	char		*repdbname;
	int		repdbcnt;
	DB		*rep_db;	/* Bookkeeping database. */

	REP		*region;	/* In memory structure. */
};

/*
 * Control structure for replication communication infrastructure.
 *
 * Note that the version information should be at the beginning of the
 * structure, so that we can rearrange the rest of it while letting the
 * version checks continue to work.  DB_REPVERSION should be revved any time
 * the rest of the structure changes.
 */
typedef struct __rep_control {
#define	DB_REPVERSION	1
	u_int32_t	rep_version;	/* Replication version number. */
	u_int32_t	log_version;	/* Log version number. */

	DB_LSN		lsn;		/* Log sequence number. */
	u_int32_t	rectype;	/* Message type. */
	u_int32_t	gen;		/* Generation number. */
	u_int32_t	flags;		/* log_put flag value. */
} REP_CONTROL;

/* Election vote information. */
typedef struct __rep_vote {
	u_int32_t	egen;		/* Election generation. */
	int		nsites;		/* Number of sites I've been in
					 * communication with. */
	int		priority;	/* My site's priority. */
	int		tiebreaker;	/* Tie-breaking quasi-random int. */
} REP_VOTE_INFO;

typedef struct __rep_gen_vote {
	u_int32_t	egen;		/* Election generation. */
    u_int32_t   last_write_gen; /* Last generation which was written to the log */
	int		nsites;		/* Number of sites I've been in communication with. */
	int		priority;	/* My site's priority. */
	int		tiebreaker;	/* Tie-breaking quasi-random int. */
} REP_GEN_VOTE_INFO;

typedef struct __rep_vtally {
	u_int32_t	egen;		/* Voter's election generation. */
	char		*eid;		/* Voter's ID. */
} REP_VTALLY;

/*
 * This structure takes care of representing a transaction.
 * It holds all the records, sorted by page number so that
 * we can obtain locks and apply updates in a deadlock free
 * order.
 */
typedef struct __lsn_page {
	DB_LSN		lsn;
	int32_t		fid;
	DB_LOCK_ILOCK	pgdesc;
#define	LSN_PAGE_NOLOCK		0x0001	/* No lock necessary for log rec. */
	u_int32_t	flags;
    char *comment;
} LSN_PAGE;

typedef struct __txn_recs {
	int		npages;
	int		nalloc;
	LSN_PAGE	*array;
	u_int32_t	txnid;
	u_int32_t	lockid;
} TXN_RECS;

/*
 * This is used by the page-prep routines to do the lock_vec call to
 * apply the updates for a single transaction or a collection of
 * transactions.
 */
typedef struct _linfo {
	int		n;
	DB_LOCKREQ	*reqs;
	DBT		*objs;
} linfo_t;

#include "dbinc_auto/rep_ext.h"
#endif	/* !_REP_H_ */
