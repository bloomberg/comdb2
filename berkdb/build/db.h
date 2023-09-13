/*
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 *
 * $Id: db.in,v 11.389 2003/10/01 21:33:58 sue Exp $
 *
 * db.h include file layout:
 *	General.
 *	Database Environment.
 *	Locking subsystem.
 *	Logging subsystem.
 *	Shared buffer cache (mpool) subsystem.
 *	Transaction subsystem.
 *	Access methods.
 *	Access method cursors.
 *	Dbm/Ndbm, Hsearch historic interfaces.
 */

#ifndef _DB_H_
#define	_DB_H_


#include <pthread.h>

#include <list.h>
#include <pool.h>
#include <plhash.h>
#include <dlmalloc.h>
#include <thdpool.h>
#include <mem_berkdb.h>
#include <sys/time.h>
#include <sbuf2.h>
#include <locks_wrap.h>

#ifndef COMDB2AR
#include <mem_override.h>
#endif

#include <netinet/in.h>

#ifndef __NO_SYSTEM_INCLUDES

#ifdef _AIX
#include <unistd.h>
#endif

#ifdef _HP_SOURCE
#define _STRUCT_MALLINFO
#include <strings.h>
#endif

#include <sys/types.h>
/* <sys/types.h> does not include <inttypes.h> on some systems. */
#include <inttypes.h>
#include <stdio.h>

#include "tunables.h"
#include "dbinc/trigger_subscription.h"
#include <dbinc/maxstackframes.h>

#if defined(__cplusplus)
extern "C" {
#endif


#undef __P
#define	__P(protos)	protos

/*
 * Berkeley DB version information.
 */
#define	DB_VERSION_MAJOR	4
#define	DB_VERSION_MINOR	2
#define	DB_VERSION_PATCH	52
#define	DB_VERSION_STRING	"Sleepycat Software: Berkeley DB 4.2.52: (December  3, 2003)"

/*
 * !!!
 * Berkeley DB uses specifically sized types.  If they're not provided by
 * the system, typedef them here.
 *
 * We protect them against multiple inclusion using __BIT_TYPES_DEFINED__,
 * as does BIND and Kerberos, since we don't know for sure what #include
 * files the user is using.
 *
 * !!!
 * We also provide the standard u_int, u_long etc., if they're not provided
 * by the system.
 */


#ifndef        __BIT_TYPES_DEFINED__
#if defined _SUN_SOURCE || defined _HP_SOURCE
typedef unsigned char u_int8_t;
typedef unsigned short u_int16_t;
typedef unsigned int u_int32_t;
typedef unsigned long long u_int64_t;
#endif
#define        __BIT_TYPES_DEFINED__
#endif




/* Basic types that are exported or quasi-exported. */
typedef	u_int32_t	db_pgno_t;	/* Page number type. */
typedef	u_int16_t	db_indx_t;	/* Page offset type. */
#define	DB_MAX_PAGES	0xffffffff	/* >= # of pages in a file */

typedef	u_int32_t	db_recno_t;	/* Record number type. */
#define	DB_MAX_RECORDS	0xffffffff	/* >= # of records in a tree */

typedef u_int32_t	db_timeout_t;	/* Type of a timeout. */

/*
 * Region offsets are currently limited to 32-bits.  I expect that's going
 * to have to be fixed in the not-too-distant future, since we won't want to
 * split 100Gb memory pools into that many different regions.
 */
typedef intptr_t roff_t;

/*
 * Forward structure declarations, so we can declare pointers and
 * applications can get type checking.
 */
struct __db;		typedef struct __db DB;
struct __db_bt_stat;	typedef struct __db_bt_stat DB_BTREE_STAT;
struct __db_cipher;	typedef struct __db_cipher DB_CIPHER;
struct __db_env;	typedef struct __db_env DB_ENV;
struct __db_h_stat;	typedef struct __db_h_stat DB_HASH_STAT;
struct __db_ilock;	typedef struct __db_ilock DB_LOCK_ILOCK;
struct __db_lock_stat;	typedef struct __db_lock_stat DB_LOCK_STAT;
struct __db_lock_u;	typedef struct __db_lock_u DB_LOCK;
struct __db_lockreq;	typedef struct __db_lockreq DB_LOCKREQ;
struct __db_log_cursor_stat; typedef struct __db_log_cursor_stat DB_LOGC_STAT;
struct __db_log_cursor;	typedef struct __db_log_cursor DB_LOGC;
struct __db_log_stat;	typedef struct __db_log_stat DB_LOG_STAT;
struct __db_lsn;	typedef struct __db_lsn DB_LSN;
struct __db_ltran; typedef struct __db_ltran DB_LTRAN;
struct __db_mpool;	typedef struct __db_mpool DB_MPOOL;
struct __db_mpool_fstat;typedef struct __db_mpool_fstat DB_MPOOL_FSTAT;
struct __db_mpool_stat;	typedef struct __db_mpool_stat DB_MPOOL_STAT;
struct __db_mpoolfile;	typedef struct __db_mpoolfile DB_MPOOLFILE;
struct __db_preplist;	typedef struct __db_preplist DB_PREPLIST;
struct __db_qam_stat;	typedef struct __db_qam_stat DB_QUEUE_STAT;
struct __db_rep;	typedef struct __db_rep DB_REP;
struct __db_rep_stat;	typedef struct __db_rep_stat DB_REP_STAT;
struct __db_txn;	typedef struct __db_txn DB_TXN;
struct __db_txn_prepared;   typedef struct __db_txn_prepared DB_TXN_PREPARED;
struct __db_txn_active;	typedef struct __db_txn_active DB_TXN_ACTIVE;
struct __db_txn_stat;	typedef struct __db_txn_stat DB_TXN_STAT;
struct __db_txnmgr;	typedef struct __db_txnmgr DB_TXNMGR;
struct __dbc;		typedef struct __dbc DBC;
struct __dbc_ser;	typedef struct __dbc_ser DBCS;      /* comdb2 addition */
struct __dbc_pause;	typedef struct __dbc_pause DBCPS;   /* comdb2 addition */
struct __dbc_internal;	typedef struct __dbc_internal DBC_INTERNAL;
struct __fh_t;		typedef struct __fh_t DB_FH;
struct __fname;		typedef struct __fname FNAME;
struct __key_range;	typedef struct __key_range DB_KEY_RANGE;
struct __mpoolfile;	typedef struct __mpoolfile MPOOLFILE;
struct __mutex_t;	typedef struct __mutex_t DB_MUTEX;
struct __heap;		typedef struct __heap HEAP;
struct __dbenv_attr;    typedef struct __dbenv_attr DBENV_ATTR;
struct __dbenv_envmap;	typedef struct __dbenv_envmap DBENV_MAP;
struct __lc_cache_entry; typedef struct __lc_cache_entry LC_CACHE_ENTRY;
struct __lc_cache;	typedef struct __lc_cache LC_CACHE;
struct __lsn_collection; typedef struct __lsn_collection LSN_COLLECTION;

struct __ltrans_descriptor; typedef struct __ltrans_descriptor LTDESC;
struct __rowlock_list; typedef struct __rowlock_list RLLIST;
struct __recovery_processor;
struct __recovery_list;
struct __db_trigger_subscription;

struct __utxnid; typedef struct __utxnid UTXNID;
struct __utxnid_track; typedef struct __utxnid_track UTXNID_TRACK;
struct __logfile_txn_list; typedef struct __logfile_txn_list LOGFILE_TXN_LIST;
struct __txn_commit_map; typedef struct __txn_commit_map DB_TXN_COMMIT_MAP;

struct txn_properties;

#include "db_dbt.h"

/* Compiler hints for branch prediction */
#if defined(__GNUC__) || defined(__IBMC__)
#  define likely(x) __builtin_expect(!!(x), 1)
#  define unlikely(x) __builtin_expect(!!(x), 0)
#else
#  define likely(X) (X)
#  define unlikely(X) (X)
#endif

/*
 * Common flags --
 *	Interfaces which use any of these common flags should never have
 *	interface specific flags in this range.
 */
#define	DB_CREATE	      0x0000001	/* Create file as necessary. */
#define	DB_CXX_NO_EXCEPTIONS  0x0000002	/* C++: return error values. */
#define	DB_FORCE	      0x0000004	/* Force (anything). */
#define	DB_NOMMAP	      0x0000008	/* Don't mmap underlying file. */
#define	DB_RDONLY	      0x0000010	/* Read-only (O_RDONLY). */
#define	DB_RECOVER	      0x0000020	/* Run normal recovery. */
#define	DB_THREAD	      0x0000040	/* Applications are threaded. */
#define	DB_TRUNCATE	      0x0000080	/* Discard existing DB (O_TRUNC). */
#define	DB_TXN_NOSYNC	      0x0000100	/* Do not sync log on commit. */
#define	DB_TXN_NOT_DURABLE    0x0000200	/* Do not log changes. */
#define	DB_USE_ENVIRON	      0x0000400	/* Use the environment. */
#define	DB_USE_ENVIRON_ROOT   0x0000800	/* Use the environment if root. */

/*
 * Common flags --
 *	Interfaces which use any of these common flags should never have
 *	interface specific flags in this range.
 *
 * DB_AUTO_COMMIT:
 *	DB_ENV->set_flags, DB->associate, DB->del, DB->put, DB->open,
 *	DB->remove, DB->rename, DB->truncate
 * DB_DIRTY_READ:
 *	DB->cursor, DB->get, DB->join, DB->open, DBcursor->c_get,
 *	DB_ENV->txn_begin
 * DB_NOAUTO_COMMIT
 *	DB->associate, DB->del, DB->put, DB->open,
 *	DB->remove, DB->rename, DB->truncate
 *
 * !!!
 * The DB_DIRTY_READ bit mask can't be changed without also changing the
 * masks for the flags that can be OR'd into DB access method and cursor
 * operation values.
 */
#define	DB_AUTO_COMMIT	      0x01000000	/* Implied transaction. */
#define	DB_DIRTY_READ	      0x02000000	/* Dirty Read. */
#define	DB_NO_AUTO_COMMIT     0x04000000	/* Override env-wide AUTO-COMMIT. */
#define DB_PAGE_ORDER         0x08000000	/* Page order scan. */
#define DB_DISCARD_PAGES      0x10000000	/* Discard page-order pages. */
#define DB_PAUSIBLE           0x20000000	/* Discard page-order pages. */
#define DB_TEMPTABLE          0x40000000	/* btree is for a temp table */
#define DB_OLCOMPACT          0x80000000	/* online compact btree if necessary */

/*
 * Flags private to db_env_create.
 */
#define	DB_RPCCLIENT	      0x0000001	/* An RPC client environment. */

/*
 * Flags private to db_create.
 */
#define	DB_REP_CREATE	      0x0000001	/* Open of an internal rep database. */
#define	DB_XA_CREATE	      0x0000002	/* Open in an XA environment. */
#define DB_INDEX_CREATE       0x0000004 /* Create with index priority. */

/*
 * Flags private to DB_ENV->open.
 *	   Shared flags up to 0x0000800 */
#define	DB_INIT_CDB	      0x0001000	/* Concurrent Access Methods. */
#define	DB_INIT_LOCK	      0x0002000	/* Initialize locking. */
#define	DB_INIT_LOG	      0x0004000	/* Initialize logging. */
#define	DB_INIT_MPOOL	      0x0008000	/* Initialize mpool. */
#define	DB_INIT_REP	      0x0010000	/* Initialize replication. */
#define	DB_INIT_TXN	      0x0020000	/* Initialize transactions. */
#define	DB_JOINENV	      0x0040000	/* Initialize all subsystems present. */
#define	DB_LOCKDOWN	      0x0080000	/* Lock memory into physical core. */
#define	DB_PRIVATE	      0x0100000	/* DB_ENV is process local. */
#define	DB_RECOVER_FATAL      0x0200000	/* Run catastrophic recovery. */
#define	DB_RECOVER_NOCKP      0x0400000	/* Do not write a checkpoint. */
#define	DB_SYSTEM_MEM	      0x0400000	/* Use system-backed memory. */

/*
 * Flags private to DB->open.
 *	   Shared flags up to 0x0000800 */
#define	DB_EXCL		      0x0001000	/* Exclusive open (O_EXCL). */
#define	DB_FCNTL_LOCKING      0x0002000	/* UNDOC: fcntl(2) locking. */
#define	DB_RDWRMASTER	      0x0004000	/* UNDOC: allow subdb master open R/W */
#define	DB_WRITEOPEN	      0x0008000	/* UNDOC: open with write lock. */
#define	DB_DATAFILE	      0x0010000	/* UNDOC: open datafile. */

/*
 * Flags private to DB_ENV->txn_begin.
 *	   Shared flags up to 0x0000800 */
#define DB_TXN_INTERNAL       0x0000001
#define DB_TXN_RECOVERY       0x0000002
#define	DB_TXN_NOWAIT	      0x0001000	/* Do not wait for locks in this TXN. */
#define	DB_TXN_SYNC	      0x0002000	/* Always sync log on commit. */
#define DB_TXN_REP_ACK        0x0010000 /* Rep should send an ACK */
#define DB_TXN_LOGICAL_BEGIN  0x0020000 /* Contains a logical begin */
#define DB_TXN_LOGICAL_COMMIT 0x0040000 /* Contains a logical commit */
#define DB_TXN_DONT_GET_REPO_MTX   0x0080000 /* Get the repo mutex on this commit */
#define DB_TXN_SCHEMA_LOCK         0x0100000 /* Get the schema-lock */
#define DB_TXN_LOGICAL_GEN         0x0200000 /* Contains generation info (txn_regop_rl) */
#define DB_TXN_FOP_NOBLOCK         0x0400000 /* Don't block on fop operations */
#define DB_TXN_DIST_PREPARE        0x0800000 /* Write a prepare record for this txn */
#define DB_TXN_DIST_UPD_SHADOWS    0x1000000 /* Set update-shadows in dist-commit */
/*
 * Flags private to DB_ENV->set_encrypt.
 */
#define	DB_ENCRYPT_AES	      0x0000001	/* AES, assumes SHA1 checksum */

/*
 * Flags private to DB_ENV->set_flags.
 *	   Shared flags up to 0x0000800 */
#define	DB_CDB_ALLDB	      0x0001000	/* Set CDB locking per environment. */
#define	DB_DIRECT_DB	      0x0002000	/* Don't buffer databases in the OS. */
#define	DB_DIRECT_LOG	      0x0004000	/* Don't buffer log files in the OS. */
#define	DB_LOG_AUTOREMOVE     0x0008000	/* Automatically remove log files. */
#define	DB_NOLOCKING	      0x0010000	/* Set locking/mutex behavior. */
#define	DB_NOPANIC	      0x0020000	/* Set panic state per DB_ENV. */
#define	DB_OVERWRITE	      0x0040000	/* Overwrite unlinked region files. */
#define	DB_PANIC_ENVIRONMENT  0x0080000	/* Set panic state per environment. */
#define	DB_REGION_INIT	      0x0100000	/* Page-fault regions on open. */
#define	DB_TIME_NOTGRANTED    0x0200000	/* Return NOTGRANTED on timeout. */
#define	DB_TXN_WRITE_NOSYNC   0x0400000	/* Write, don't sync, on txn commit. */
#define	DB_YIELDCPU	      0x0800000	/* Yield the CPU (a lot). */

#define DB_ROWLOCKS       0x10000000 /* Environment supports rowlocks */
#define DB_OSYNC          0x20000000

/*
 * Flags private to DB->set_feedback's callback.
 */
#define	DB_UPGRADE	      0x0000001	/* Upgrading. */
#define	DB_VERIFY	      0x0000002	/* Verifying. */

/*
 * Flags private to DB_MPOOLFILE->open.
 *	   Shared flags up to 0x0000800 */
#define	DB_DIRECT	      0x0001000	/* Don't buffer the file in the OS. */
#define	DB_EXTENT	      0x0002000	/* UNDOC: dealing with an extent. */
#define	DB_ODDFILESIZE	      0x0004000	/* Truncate file to N * pgsize. */

/*
 * Flags private to DB->set_flags.
 */
#define	DB_CHKSUM	      0x0000001	/* Do checksumming */
#define	DB_DUP		      0x0000002	/* Btree, Hash: duplicate keys. */
#define	DB_DUPSORT	      0x0000004	/* Btree, Hash: duplicate keys. */
#define	DB_ENCRYPT	      0x0000008	/* Btree, Hash: duplicate keys. */
#define	DB_RECNUM	      0x0000010	/* Btree: record numbers. */
#define	DB_RENUMBER	      0x0000020	/* Recno: renumber on insert/delete. */
#define	DB_REVSPLITOFF	   0x0000040	/* Btree: turn off reverse splits. */
#define	DB_SNAPSHOT	      0x0000080	/* Recno: snapshot the input. */

/*
 * Flags private to the DB->stat methods.
 */
#define	DB_STAT_CLEAR	   0x0000001	/* Clear stat after returning values. */
#define DB_STAT_VERIFY     0x0000002    /* check verify_lsn when catching up */
#define DB_STAT_MINIMAL    0x0000004    /* Just get cache hit/miss */

/*
 * Flags private to DB->join.
 */
#define	DB_JOIN_NOSORT	      0x0000001	/* Don't try to optimize join. */

/*
 * Flags private to DB->verify.
 */
#define	DB_AGGRESSIVE	      0x0000001	/* Salvage whatever could be data.*/
#define	DB_NOORDERCHK	      0x0000002	/* Skip sort order/hashing check. */
#define	DB_ORDERCHKONLY	      0x0000004	/* Only perform the order check. */
#define	DB_PR_PAGE	      0x0000008	/* Show page contents (-da). */
#define	DB_PR_RECOVERYTEST    0x0000010	/* Recovery test (-dr). */
#define	DB_PRINTABLE	      0x0000020	/* Use printable format for salvage. */
#define	DB_SALVAGE	      0x0000040	/* Salvage what looks like data. */
#define	DB_IN_ORDER_CHECK   0x0000080	/* We are traversing in order so check intra page for out-of-order keys. */
#define	DB_RECCNTCHK	      0x0000100	/* Perform record number check. */
/*
 * !!!
 * These must not go over 0x8000, or they will collide with the flags
 * used by __bam_vrfy_subtree.
 */

/*
 * Flags private to DB->set_rep_transport's send callback.
 */
#define	DB_REP_NOBUFFER	      0x0000001	/* Do not buffer this message. */
#define	DB_REP_PERMANENT        0x0000002	/* Important--app. may want to flush. */
#define  DB_REP_LOGPROGRESS      0x0000004   /* marks a new log record, not a commit though */
#define DB_REP_FLUSH            0x0000008

/* Don't allow these to overlap with log_put flags */
#define DB_REP_NODROP           0x0001000
#define DB_REP_TRACE           0x0002000 /* Trace this through the net layer */
#define DB_REP_SENDACK          0x0004000

/*******************************************************
 * Locking.
 *******************************************************/
#define	DB_LOCKVERSION	1

#define	DB_FILE_ID_LEN		20	/* Unique file ID length. */

/*
 * Deadlock detector modes; used in the DB_ENV structure to configure the
 * locking subsystem.
 */
#define	DB_LOCK_NORUN		0
#define	DB_LOCK_DEFAULT         1	/* Default policy. */
#define	DB_LOCK_EXPIRE		2	/* Only expire locks, no detection. */
#define	DB_LOCK_MAXLOCKS	3	/* Abort txn with maximum # of locks. */
#define	DB_LOCK_MINLOCKS	4	/* Abort txn with minimum # of locks. */
#define	DB_LOCK_MINWRITE	5	/* Abort txn with minimum writelocks. */
#define	DB_LOCK_OLDEST		6	/* Abort oldest transaction. */
#define	DB_LOCK_RANDOM		7	/* Abort random transaction. */
#define	DB_LOCK_YOUNGEST	8	/* Abort youngest transaction. */
#define	DB_LOCK_MAXWRITE	9	/* Select locker with max writelocks. */
#define	DB_LOCK_MINWRITE_NOREAD	10	/* Select locker with max writelocks. */
#define	DB_LOCK_YOUNGEST_EVER	11	/* Select locker for youngest
					   transaction including old
					   reincarnations. */
#define	DB_LOCK_MINWRITE_EVER	12	/* Select locker with min
					   writelocks including old
					   reincarnations. */
#define	DB_LOCK_MAX		12	/* Max deadlock mode. */

/* Flag values for lock_vec(), lock_get(). */
#define	DB_LOCK_NOWAIT		0x001	/* Don't wait on unavailable lock. */
#define	DB_LOCK_RECORD		0x002	/* Internal: record lock. */
#define	DB_LOCK_REMOVE		0x004	/* Internal: flag object removed. */
#define	DB_LOCK_SET_TIMEOUT	0x008	/* Internal: set lock timeout. */
#define	DB_LOCK_SWITCH		0x010	/* Internal: switch existing lock. */
#define	DB_LOCK_UPGRADE		0x020	/* Internal: upgrade existing lock. */
#define	DB_LOCK_LOGICAL		0x040	/* Force holders of this
					 * lockobj to deadlock. */
#define	DB_LOCK_NOPAGELK	0x080   /* Return deadlock if lockerid
					 * is holding a pagelock */
#define	DB_LOCK_ONELOCK		0x100   /* lockerid will acquire only this
					 * lock */

/* Flag values for lock_id_flags(). */
#define DB_LOCK_ID_LOWPRI   0x001	/* Choose this as a deadlock victim */
#define DB_LOCK_ID_TRACK    0x002	/* Track this lockid */
#define DB_LOCK_ID_READONLY 0x004	/* Mark this as a read-only lockid */

/* Flag values for lock_abort_waiters */
#define DB_LOCK_ABORT_LOGICAL   0x0001 /* Only abort logical waiters */

/*
 * Simple R/W lock modes and for multi-granularity intention locking.
 *
 * !!!
 * These values are NOT random, as they are used as an index into the lock
 * conflicts arrays, i.e., DB_LOCK_IWRITE must be == 3, and DB_LOCK_IREAD
 * must be == 4.
 */
typedef enum {
	DB_LOCK_NG=0,			/* Not granted. */
	DB_LOCK_READ=1,			/* Shared/read. */
	DB_LOCK_WRITE=2,		/* Exclusive/write. */
	DB_LOCK_WAIT=3,			/* Wait for event */
	DB_LOCK_IWRITE=4,		/* Intent exclusive/write. */
	DB_LOCK_IREAD=5,		/* Intent to share/read. */
	DB_LOCK_IWR=6,			/* Intent to read and write. */
	DB_LOCK_DIRTY=7,		/* Dirty Read. */
	DB_LOCK_WWRITE=8,		/* Was Written. */
	DB_LOCK_WRITEADD=9,		/* Exclusive/write - intent to
					 * add a record */
	DB_LOCK_WRITEDEL=10,		/* Exclusive/write - intent to
					 * delete a record */
	DB_LOCK_WRITE_LOGICAL=11	/* Write lock which can't be a
					 * deadlock victim on
					 * replicant */
} db_lockmode_t;

/*
 * Request types.
 */
typedef enum {
	DB_LOCK_DUMP=0,			/* Display held locks. */
	DB_LOCK_GET=1,			/* Get the lock. */
	DB_LOCK_GET_TIMEOUT=2,		/* Get lock with a timeout. */
	DB_LOCK_INHERIT=3,		/* Pass locks to parent. */
	DB_LOCK_PUT=4,			/* Release the lock. */
	DB_LOCK_PUT_ALL=5,		/* Release locker's locks. */
	DB_LOCK_PUT_OBJ=6,		/* Release locker's locks on obj. */
	DB_LOCK_PUT_READ=7,		/* Release locker's read locks. */
	DB_LOCK_TIMEOUT=8,		/* Force a txn to timeout. */
	DB_LOCK_TRADE=9,		/* Trade locker ids on a lock. */
	DB_LOCK_UPGRADE_WRITE=10, /* Upgrade writes for dirty reads. */
	DB_LOCK_TRADE_COMP=11,  /* Trade locks for compensating txn. */
    DB_LOCK_PREPARE=12      /* lock-put-read but retain tablelocks */
} db_lockop_t;

/*
 * Status of a lock.
 */
typedef enum  {
	DB_LSTAT_ABORTED=1,		/* Lock belongs to an aborted txn. */
	DB_LSTAT_ERR=2,			/* Lock is bad. */
	DB_LSTAT_EXPIRED=3,		/* Lock has expired. */
	DB_LSTAT_FREE=4,		/* Lock is unallocated. */
	DB_LSTAT_HELD=5,		/* Lock is currently held. */
	DB_LSTAT_NOTEXIST=6,		/* Object on which lock was waiting
					 * was removed */
	DB_LSTAT_PENDING=7,		/* Lock was waiting and has been
					 * promoted; waiting for the owner
					 * to run and upgrade it to held. */
	DB_LSTAT_WAITING=8		/* Lock is on the wait queue. */
}db_status_t;

/* Lock statistics structure. */
struct __db_lock_stat {
    /* locker ID can't be larger than DB_LOCK_MAXID (2^31 -1),
       so keep st_id and st_cur_maxid int32. */
	u_int32_t st_id;		/* Last allocated locker ID. */
	u_int32_t st_cur_maxid;		/* Current maximum unused ID. */
	u_int64_t st_maxlocks;		/* Maximum number of locks in table. */
	u_int64_t st_maxlockers;	/* Maximum num of lockers in table. */
	u_int32_t st_maxobjects;	/* Maximum num of objects in table. */
	u_int32_t st_nmodes;		/* Number of lock modes. */
	u_int64_t st_nlocks;		/* Current number of locks. */
	u_int64_t st_maxnlocks;		/* Maximum number of locks so far. */
	u_int64_t st_nlockers;		/* Current number of lockers. */
	u_int64_t st_maxnlockers;	/* Maximum number of lockers so far. */
	u_int64_t st_nobjects;		/* Current number of objects. */
	u_int64_t st_maxnobjects;	/* Maximum number of objects so far. */
	u_int64_t st_nconflicts;	/* Number of lock conflicts. */
	u_int64_t st_nrequests;		/* Number of lock gets. */
	u_int64_t st_nreleases;		/* Number of lock puts. */
	u_int64_t st_nnowaits;		/* Number of requests that would have
					   waited, but NOWAIT was set. */
	u_int64_t st_ndeadlocks;	/* Number of lock deadlocks. */
	u_int64_t st_locks_aborted;	/* Number of locks released on deadlocks.*/
	db_timeout_t st_locktimeout;	/* Lock timeout. */
	u_int64_t st_nlocktimeouts;	/* Number of lock timeouts. */
	db_timeout_t st_txntimeout;	/* Transaction timeout. */
	u_int64_t st_ntxntimeouts;	/* Number of transaction timeouts. */
	u_int64_t st_region_wait;	/* Region lock granted after wait. */
	u_int64_t st_region_nowait;	/* Region lock granted without wait. */
	u_int64_t st_regsize;		/* Region size. */
};

/*
 * DB_LOCK_ILOCK --
 *	Internal DB access method lock.
 */
struct __db_ilock {
	db_pgno_t pgno;			/* Page being locked. */
	u_int8_t fileid[DB_FILE_ID_LEN];/* File id. */
#define	DB_HANDLE_LOCK	1
#define	DB_RECORD_LOCK	2
#define	DB_PAGE_LOCK	3
	u_int32_t type;			/* Type of lock. */
};

/*
 * DB_LOCK --
 *	The structure is allocated by the caller and filled in during a
 *	lock_get request (or a lock_vec/DB_LOCK_GET).
 */
struct __db_lock_u {
	roff_t		off;		/* Offset of the lock in the region */

    void        *ilock_latch;     /* Pointer to ilock-latch */

	u_int32_t	ndx;		/* Index of the object referenced by
					 * this lock; used for locking. */
	u_int32_t	gen;		/* Generation number of this lock. */
	db_lockmode_t	mode;		/* mode of this lock. */
	u_int32_t	owner;		/* owner of this lock if not granted as a NOWAIT */
	u_int32_t	partition;
};

/* Lock request structure. */
struct __db_lockreq {
	db_lockop_t	 op;		/* Operation. */
	db_lockmode_t	 mode;		/* Requested mode. */
	db_timeout_t	 timeout;	/* Time to expire lock. */
	DBT		*obj;		/* Object being locked. */
	DB_LOCK		 lock;		/* Lock returned. */
};

/*******************************************************
 * Logging.
 *******************************************************/
#define	DB_LOGVERSION	8		/* Current log version. */
#define	DB_LOGOLDVER	8		/* Oldest log version supported. */
#define	DB_LOGMAGIC	0x040988

/* Flag values for DB_ENV->log_archive(). */
#define	DB_ARCH_ABS	0x001		/* Absolute pathnames. */
#define	DB_ARCH_DATA	0x002		/* Data files. */
#define	DB_ARCH_LOG	0x004		/* Log files. */
#define	DB_ARCH_REMOVE	0x008	/* Remove log files. */

/* Flag values for DB_ENV->log_put(). */
#define	DB_FLUSH		0x001	/* Flush data to disk (public). */
#define	DB_LOG_CHKPNT		0x002	/* Flush supports a checkpoint */
#define	DB_LOG_COMMIT		0x004	/* Flush supports a commit */
#define	DB_LOG_NOCOPY		0x008	/* Don't copy data */
#define	DB_LOG_NOT_DURABLE	0x010	/* Do not log; keep in memory */
#define	DB_LOG_PERM		0x020	/* Flag record with REP_PERMANENT */
#define	DB_LOG_WRNOSYNC		0x040	/* Write, don't sync log_put */
#define	DB_LOG_DONT_LOCK	0x080   /* We already have dbreglk */
#define	DB_LOG_REP_ACK		0x100   /* Send an ACK */
#define	DB_LOG_DONT_INFLATE	0x200   /* Prevent recursive inflates */
#define	DB_LOG_LOGICAL_COMMIT	0x400 /* This contains a logical commit */

#define	DB_REG_HAVE_FQLOCK	0x001   /* We already have fqlock */


/*
 * A DB_LSN has two parts, a fileid which identifies a specific file, and an
 * offset within that file.  The fileid is an unsigned 4-byte quantity that
 * uniquely identifies a file within the log directory -- currently a simple
 * counter inside the log.  The offset is also an unsigned 4-byte value.  The
 * log manager guarantees the offset is never more than 4 bytes by switching
 * to a new log file before the maximum length imposed by an unsigned 4-byte
 * offset is reached.
 */
struct __db_lsn {
	u_int32_t	file;		/* File ID. */
	u_int32_t	offset;		/* File offset. */
};

struct __db_ltran {
	u_int64_t   tranid;
	DB_LSN      begin_lsn;
	DB_LSN      last_lsn;
};

/*
 * Application-specified log record types start at DB_user_BEGIN, and must not
 * equal or exceed DB_debug_FLAG.
 *
 * DB_debug_FLAG is the high-bit of the u_int32_t that specifies a log record
 * type.  If the flag is set, it's a log record that was logged for debugging
 * purposes only, even if it reflects a database change -- the change was part
 * of a non-durable transaction.
 */
#define	DB_user_BEGIN		10000
#define	DB_debug_FLAG		0x80000000

struct __db_log_cursor_stat {
    int incursor_count;
    int ondisk_count;
    int inregion_count;
    unsigned long long incursorus;
    unsigned long long ondiskus;
    unsigned long long inregionus;
    unsigned long long totalus;
    unsigned long long lockwaitus;
};

/*
 * DB_LOGC --
 *	Log cursor.
 */
struct __db_log_cursor {
	DB_ENV	 *dbenv;		/* Enclosing dbenv. */

	DB_FH	 *c_fhp;		/* File handle. */
	DB_LSN	  c_lsn;		/* Cursor: LSN */
	u_int32_t c_len;		/* Cursor: record length */
	u_int32_t c_prev;		/* Cursor: previous record's offset */

	DBT	  c_dbt;		/* Return DBT. */

#define	DB_LOGC_BUF_SIZE	(32 * 1024)
	u_int8_t *bp;			/* Allocated read buffer. */
	u_int32_t bp_size;		/* Read buffer length in bytes. */
	u_int32_t bp_rlen;		/* Read buffer valid data length. */
	DB_LSN	  bp_lsn;		/* Read buffer first byte LSN. */

	u_int32_t bp_maxrec;		/* Max record length in the log file. */

					/* Methods. */
	int (*close) __P((DB_LOGC *, u_int32_t));
	int (*get) __P((DB_LOGC *, DB_LSN *, DBT *, u_int32_t));
    int (*stat) __P((DB_LOGC *, DB_LOGC_STAT **));
    int (*setflags) __P((DB_LOGC*, u_int32_t));

    /* Instrumentation for log stats */
    int incursor_count;
    int ondisk_count;
    int inregion_count;

    u_int64_t incursorus;
    u_int64_t ondiskus;
    u_int64_t inregionus;
    u_int64_t totalus;
    u_int64_t lockwaitus;

#define	DB_LOG_DISK		0x01	/* Log record came from disk. */
#define	DB_LOG_LOCKED		0x02	/* Log region already locked */
#define	DB_LOG_SILENT_ERR	0x04	/* Turn-off error messages. */
#define DB_LOG_NO_PANIC		0x08    /* Don't panic on error. */
#define DB_LOG_CUSTOM_SIZE  0x10    /* This cursor has a custom size */
	u_int32_t flags;
    struct __db_log_cursor *next;
    struct __db_log_cursor *prev;
};

/* Log statistics structure. */
struct __db_log_stat {
	u_int32_t st_magic;		/* Log file magic number. */
	u_int32_t st_version;		/* Log file version number. */
	int st_mode;			/* Log file mode. */
	u_int32_t st_lg_bsize;		/* Log buffer size. */
	u_int32_t st_lg_size;		/* Log file size. */
	u_int32_t st_lg_nsegs;		/* Number of segments. */
	u_int32_t st_lg_segsz;		/* Size of a log-segment. */
	u_int32_t st_w_bytes;		/* Bytes to log. */
	u_int32_t st_w_mbytes;		/* Megabytes to log. */
	u_int32_t st_wc_bytes;		/* Bytes to log since checkpoint. */
	u_int32_t st_wc_mbytes;		/* Megabytes to log since checkpoint. */
	u_int32_t st_wcount;		/* Total writes to the log. */
	u_int32_t st_wcount_fill;	/* Overflow writes to the log. */
	u_int32_t st_scount;		/* Total syncs to the log. */
	u_int32_t st_swrites;		/* Writes forced by log syncs. */
	u_int32_t st_region_wait;	/* Region lock granted after wait. */
	u_int32_t st_region_nowait;	/* Region lock granted without wait. */
	u_int32_t st_cur_file;		/* Current log file number. */
	u_int32_t st_cur_offset;	/* Current log file offset. */
	u_int32_t st_disk_file;		/* Known on disk log file number. */
	u_int32_t st_disk_offset;	/* Known on disk log file offset. */
	u_int32_t st_regsize;		/* Region size. */
	u_int32_t st_maxcommitperflush;	/* Max number of commits in a flush. */
	u_int32_t st_mincommitperflush;	/* Min number of commits in a flush. */
	u_int32_t st_total_wakeups;	/* Total writer td-wakeup. */
	u_int32_t st_false_wakeups;	/* No-write td-wakeup counter. */
	u_int32_t st_max_td_written;	/* Max flushed in a wakeup. */
	u_int32_t st_inline_writes;	/* Inline writes counter. */
	u_int32_t st_in_cursor_get;	/* gets filled by the cursor's cache. */
	u_int32_t st_in_region_get;	/* In-region log_get. */
	u_int32_t st_part_region_get;	/* Partial in-region log_get. */
	u_int32_t st_ondisk_get;	/* On-disk log_get. */
	u_int32_t st_inmem_trav;	/* Mem-log steps for partial reads. */
	u_int32_t st_wrap_copy;		/* Count of wrapped copies. */
};

/*******************************************************
 * Shared buffer cache (mpool).
 *******************************************************/
/* Flag values for DB_MPOOLFILE->get. */
#define	DB_MPOOL_CREATE		0x001	/* Create a page. */
#define	DB_MPOOL_LAST		0x002	/* Return the last page. */
#define	DB_MPOOL_NEW		0x004	/* Create a new page. */
#define	DB_MPOOL_PROBE		0x008	/* Fail at first miss. */
#define	DB_MPOOL_RECP		0x010	/* Restore from a recovery page */
#define	DB_MPOOL_PFGET		0x020	/* Prefault a page */

/* Flag values for DB_MPOOLFILE->put, DB_MPOOLFILE->get. */
#define DB_MPOOL_NOCACHE	0x040	/* Discard low-priority. */

/* Flag values for DB_MPOOLFILE->get. */
#define	DB_MPOOL_COMPACT	0x080   /* Compact a page if necessary */

/* Flag values for DB_MPOOLFILE->put, DB_MPOOLFILE->set. */
#define	DB_MPOOL_CLEAN		0x001	/* Page is not modified. */
#define	DB_MPOOL_DIRTY		0x002	/* Page is modified. */
#define	DB_MPOOL_DISCARD	0x004	/* Don't cache the page. */
#define	DB_MPOOL_PFPUT		0x008	/* page got by prefault */

/* Flag values for DB_MPOOLFILE->alloc. */
#define DB_MPOOL_LOWPRI		0x001   /* Evict low-priority pages. */

/* Flags values for DB_MPOOLFILE->set_flags. */
#define	DB_MPOOL_NOFILE		0x001	/* Never open a backing file. */
#define	DB_MPOOL_UNLINK		0x002	/* Unlink the file on last close. */

/* Priority values for DB_MPOOLFILE->set_priority. */
typedef enum {
	DB_PRIORITY_VERY_LOW=1,
	DB_PRIORITY_LOW=2,
	DB_PRIORITY_DEFAULT=3,
	DB_PRIORITY_HIGH=4,
	DB_PRIORITY_VERY_HIGH=5,
	DB_PRIORITY_INDEX=6
} DB_CACHE_PRIORITY;
/*
 * Mpool priorities from low to high.  Defined in terms of fractions of the
 * buffers in the pool.
 */
typedef enum {
	MPOOL_PRI_VERY_LOW=-1,	/* Dead duck.  Check and set to 0. */
	MPOOL_PRI_LOW=-2,	/* Low. */
	MPOOL_PRI_DEFAULT=0,	/* No adjustment -- special case.*/
	MPOOL_PRI_HIGH=10,	/* With the dirty buffers. */
	MPOOL_PRI_DIRTY=5,	/* Dirty gets a 20% boost. */
	MPOOL_PRI_INDEX=2,   /* Index pages get a 50% boost. */
	MPOOL_PRI_INTERNAL=4,   /* Internal pages get an additional 25% boost. */
	MPOOL_PRI_VERY_HIGH=1,	/* Add number of buffers in pool. */
} MPOOL_PRIORITY;


/* Per-process DB_MPOOLFILE information. */
struct __db_mpoolfile {
	DB_FH	  *fhp;		/* Underlying file handle. */
	DB_FH     *recp;        /* Recovery page file handle. */

/* Protected by mpoolfile mutex */
	struct {								\
		DB_MPOOLFILE *le_next;	/* next element */			\
		DB_MPOOLFILE **le_prev;	/* address of previous next element */	\
	} mpfq;

	/* Lock-array for recovery pages. */
	pthread_mutex_t *recp_lk_array;
	int       rec_idx;      /* Current index for recover pages. */

	/* Recovery page index lock. */
	pthread_mutex_t recp_idx_lk;
	/*
	 * !!!
	 * The ref, pinref and q fields are protected by the region lock.
	 */
	u_int32_t  ref;			/* Reference count. */
	u_int32_t pinref;		/* Pinned block reference count. */

	/*
	 * !!!
	 * Explicit representations of structures from queue.h.
	 * TAILQ_ENTRY(__db_mpoolfile) q;
	 */
	struct {
		struct __db_mpoolfile *tqe_next;
		struct __db_mpoolfile **tqe_prev;
	} q;				/* Linked list of DB_MPOOLFILE's. */

	/*
	 * !!!
	 * The rest of the fields (with the exception of the MP_FLUSH flag)
	 * are not thread-protected, even when they may be modified at any
	 * time by the application.  The reason is the DB_MPOOLFILE handle
	 * is single-threaded from the viewpoint of the application, and so
	 * the only fields needing to be thread-protected are those accessed
	 * by checkpoint or sync threads when using DB_MPOOLFILE structures
	 * to flush buffers from the cache.
	 */
	DB_ENV	       *dbenv;		/* Overlying DB_ENV. */
	MPOOLFILE      *mfp;		/* Underlying MPOOLFILE. */

	u_int32_t	clear_len;	/* Cleared length on created pages. */
	u_int8_t			/* Unique file ID. */
			fileid[DB_FILE_ID_LEN];
	int		ftype;		/* File type. */
	int32_t		lsn_offset;	/* LSN offset in page. */
	u_int32_t	gbytes, bytes;	/* Maximum file size. */
	DBT	       *pgcookie;	/* Byte-string passed to pgin/pgout. */
	MPOOL_PRIORITY		/* Cache priority. */
			priority;

	void	       *addr;		/* Address of mmap'd region. */
	size_t		len;		/* Length of mmap'd region. */

	u_int32_t	config_flags;	/* Flags to DB_MPOOLFILE->set_flags. */

					/* Methods. */
	int (*close) __P((DB_MPOOLFILE *, u_int32_t));
	int (*get) __P((DB_MPOOLFILE *, db_pgno_t *, u_int32_t, void *));
	int (*open)__P((DB_MPOOLFILE *, const char *, u_int32_t, int, size_t));
	int (*put) __P((DB_MPOOLFILE *, void *, u_int32_t));
	int (*set) __P((DB_MPOOLFILE *, void *, u_int32_t));
	int (*get_clear_len) __P((DB_MPOOLFILE *, u_int32_t *));
	int (*set_clear_len) __P((DB_MPOOLFILE *, u_int32_t));
	int (*get_fileid) __P((DB_MPOOLFILE *, u_int8_t *));
	int (*set_fileid) __P((DB_MPOOLFILE *, u_int8_t *));
	int (*get_flags) __P((DB_MPOOLFILE *, u_int32_t *));
	int (*set_flags) __P((DB_MPOOLFILE *, u_int32_t, int));
	int (*get_ftype) __P((DB_MPOOLFILE *, int *));
	int (*set_ftype) __P((DB_MPOOLFILE *, int));
	int (*get_lsn_offset) __P((DB_MPOOLFILE *, int32_t *));
	int (*set_lsn_offset) __P((DB_MPOOLFILE *, int32_t));
	int (*get_maxsize) __P((DB_MPOOLFILE *, u_int32_t *, u_int32_t *));
	int (*set_maxsize) __P((DB_MPOOLFILE *, u_int32_t, u_int32_t));
	int (*get_pgcookie) __P((DB_MPOOLFILE *, DBT *));
	int (*set_pgcookie) __P((DB_MPOOLFILE *, DBT *));
	int (*get_priority) __P((DB_MPOOLFILE *, DB_CACHE_PRIORITY *));
	int (*set_priority) __P((DB_MPOOLFILE *, DB_CACHE_PRIORITY));
	int (*sync) __P((DB_MPOOLFILE *));

	/*
	 * MP_FILEID_SET, MP_OPEN_CALLED and MP_READONLY do not need to be
	 * thread protected because they are initialized before the file is
	 * linked onto the per-process lists, and never modified.
	 *
	 * MP_FLUSH is thread protected becase it is potentially read/set by
	 * multiple threads of control.
	 */
#define	MP_FILEID_SET	0x001		/* Application supplied a file ID. */
#define	MP_FLUSH	0x002		/* Was opened to flush a buffer. */
#define	MP_OPEN_CALLED	0x004		/* File opened. */
#define	MP_READONLY	0x008		/* File is readonly. */
	u_int32_t  flags;
};

/*
 * Mpool statistics structure.
 */
struct __db_mpool_stat {
	u_int64_t st_gbytes;		/* Total cache size: GB. */
	u_int64_t st_bytes;		/* Total cache size: B. */
	u_int64_t st_total_bytes;	/* Total cache size: st_gbytes + st_bytes. */
	u_int64_t st_used_bytes;	/* Total used cache size: B. */
	u_int64_t st_ncache;		/* Number of caches. */
	u_int64_t st_regsize;		/* Cache size. */
	u_int64_t st_map;		/* Pages from mapped files. */
	u_int64_t st_cache_hit;		/* Pages found in the cache. */
	u_int64_t st_cache_miss;	/* Pages not found in the cache. */
	u_int64_t st_cache_ihit;	/* Internal found in the cache. */
	u_int64_t st_cache_imiss;	/* Internal not found in the cache. */
	u_int64_t st_cache_lhit;	/* Leaves found in the cache. */
	u_int64_t st_cache_lmiss;	/* Leaves not found in the cache. */
	u_int64_t st_page_create;	/* Pages created in the cache. */
	u_int64_t st_page_pf_in;	/* Pages read in by prefault */
	u_int64_t st_page_pf_in_late;/* Unaffective prefault requests */
	u_int64_t st_page_in;		/* Pages read in. */
	u_int64_t st_page_out;		/* Pages written out. */
	u_int64_t st_ro_merges;		/* Read merges performed. */
	u_int64_t st_rw_merges;		/* Write merges performed. */
	u_int64_t st_ro_evict;		/* Clean pages forced from the cache. */
	u_int64_t st_rw_evict;		/* Dirty pages forced from the cache. */
	u_int64_t st_ro_levict;		/* Clean leaf pages forced from cache.*/
	u_int64_t st_rw_levict;		/* Dirty leaf pages forced from cache.*/
	u_int64_t st_pf_evict;		/* Prefault pages forced from  cache. */
	u_int64_t st_rw_evict_skip;	/* Dirty pages skipped during evict. */
	u_int64_t st_page_trickle;	/* Pages written by memp_trickle. */
	u_int64_t st_pages;		/* Total number of pages. */
	u_int64_t st_page_clean;	/* Clean pages. */
	uint32_t  st_page_dirty;	/* Dirty pages. */
	u_int64_t st_hash_buckets;	/* Number of hash buckets. */
	u_int64_t st_hash_searches;	/* Total hash chain searches. */
	u_int64_t st_hash_longest;	/* Longest hash chain searched. */
	u_int64_t st_hash_examined;	/* Total hash entries searched. */
	u_int64_t st_hash_nowait;	/* Hash lock granted with nowait. */
	u_int64_t st_hash_wait;		/* Hash lock granted after wait. */
	u_int64_t st_hash_max_wait;	/* Max hash lock granted after wait. */
	u_int64_t st_region_nowait;	/* Region lock granted with nowait. */
	u_int64_t st_region_wait;	/* Region lock granted after wait. */
	u_int64_t st_alloc;		/* Number of page allocations. */
	u_int64_t st_alloc_buckets;	/* Buckets checked during allocation. */
	u_int64_t st_alloc_max_buckets;	/* Max checked during allocation. */
	u_int64_t st_alloc_pages;	/* Pages checked during allocation. */
	u_int64_t st_alloc_max_pages;	/* Max checked during allocation. */
	u_int64_t st_ckp_pages_sync;	/* Number of pages sync'd using perfect ckp. */
	u_int64_t st_ckp_pages_skip;	/* Number of pages skipped using perfect ckp. */
};

/* Mpool file statistics structure. */
struct __db_mpool_fstat {
	char *file_name;		/* File name. */
	size_t st_pagesize;		/* Page size. */
	u_int64_t st_map;		/* Pages from mapped files. */
	u_int64_t st_cache_hit;		/* Pages found in the cache. */
	u_int64_t st_cache_miss;	/* Pages not found in the cache. */
	u_int64_t st_cache_ihit;	/* Internal found in the cache. */
	u_int64_t st_cache_imiss;	/* Internal not found in the cache. */
	u_int64_t st_cache_lhit;	/* Leaves found in the cache. */
	u_int64_t st_cache_lmiss;	/* Leaves not found in the cache. */
	u_int64_t st_page_create;	/* Pages created in the cache. */
	u_int64_t st_page_in;		/* Pages read in. */
	u_int64_t st_page_out;		/* Pages written out. */
	u_int64_t st_ro_merges;		/* Read merges performed. */
	u_int64_t st_rw_merges;		/* Write merges performed. */
};

/*******************************************************
 * Transactions and recovery.
 *******************************************************/
#define	DB_TXNVERSION	1

typedef enum {
	DB_TXN_ABORT=0,			/* Public. */
	DB_TXN_APPLY=1,			/* Public. */
	DB_TXN_BACKWARD_ALLOC=2,	/* Internal. */
	DB_TXN_BACKWARD_ROLL=3,		/* Public. */
	DB_TXN_FORWARD_ROLL=4,		/* Public. */
	DB_TXN_GETPGNOS=5,		/* Internal. */
	DB_TXN_OPENFILES=6,		/* Internal. */
	DB_TXN_POPENFILES=7,		/* Internal. */
	DB_TXN_PRINT=8,			/* Public. */
	DB_TXN_UNUSED1=9,
	DB_TXN_SNAPISOL=10,		/* COMDB2 MODIFICATION */
	DB_TXN_UNUSED2=11,
	DB_TXN_UNUSED3=12,
	DB_TXN_UNUSED4=13,
	DB_TXN_LOGICAL_BACKWARD_ROLL=14, /* Public, COMDB2 MODIFICATION */
	DB_TXN_NOT_IN_RECOVERY=15,
	DB_TXN_GETALLPGNOS=16,		/* Internal. */
	DB_TXN_DIST_ADD_TXNLIST=17  /* Internal. */
} db_recops;

/*
 * BACKWARD_ALLOC is used during the forward pass to pick up any aborted
 * allocations for files that were created during the forward pass.
 * The main difference between _ALLOC and _ROLL is that the entry for
 * the file not exist during the rollforward pass.
 */
#define	DB_UNDO(op)	((op) == DB_TXN_ABORT ||			\
		(op) == DB_TXN_BACKWARD_ROLL || (op) == DB_TXN_BACKWARD_ALLOC)
#define	DB_REDO(op)	((op) == DB_TXN_FORWARD_ROLL || (op) == DB_TXN_APPLY)

struct __db_txn {
	DB_TXNMGR	*mgrp;		/* Pointer to transaction manager. */
	DB_TXN		*parent;	/* Pointer to transaction's parent. */
	DB_LSN		last_lsn;	/* Lsn of last log write. */
	u_int32_t	txnid;		/* Unique transaction id. */
	u_int32_t	tid;		/* Thread id for use in MT XA. */
	u_int64_t   logbytes;
	roff_t		off;		/* Detail structure within region. */
	db_timeout_t	lock_timeout;	/* Timeout for locks for this txn. */
	db_timeout_t	expire;		/* Time this txn expires. */
	void		*txn_list;	/* Undo information for parent. */

	/*
	 * !!!
	 * Explicit representations of structures from queue.h.
	 * TAILQ_ENTRY(__db_txn) links;
	 * TAILQ_ENTRY(__db_txn) xalinks;
	 */
	struct {
		struct __db_txn *tqe_next;
		struct __db_txn **tqe_prev;
	} links;			/* Links transactions off manager. */
	struct {
		struct __db_txn *tqe_next;
		struct __db_txn **tqe_prev;
	} xalinks;			/* Links active XA transactions. */

	/*
	 * !!!
	 * Explicit representations of structures from queue.h.
	 * TAILQ_HEAD(__events, __txn_event) events;
	 */
	struct {
		struct __txn_event *tqh_first;
		struct __txn_event **tqh_last;
	} events;

	/*
	 * !!!
	 * Explicit representations of structures from queue.h.
	 * STAILQ_HEAD(__logrec, __txn_logrec) logs;
	 */
	struct {
		struct __txn_logrec *stqh_first;
		struct __txn_logrec **stqh_last;
	} logs;				/* Links deferred events. */

	/*
	 * !!!
	 * Explicit representations of structures from queue.h.
	 * TAILQ_HEAD(__kids, __db_txn) kids;
	 */
	struct __kids {
		struct __db_txn *tqh_first;
		struct __db_txn **tqh_last;
	} kids;

	/*
	 * !!!
	 * Explicit representations of structures from queue.h.
	 * TAILQ_ENTRY(__db_txn) klinks;
	 */
	struct {
		struct __db_txn *tqe_next;
		struct __db_txn **tqe_prev;
	} klinks;

	/* 
	 * Stores the utxnids of committed kids so that parent can add them
	 * to the commit LSN map when it commits.
	 */
	LISTC_T(UTXNID) committed_kids; 

	/* API-private structure: used by C++ */
	void	*api_internal;

	u_int32_t	cursors;	/* Number of cursors open for txn */

					/* Methods. */
	int	  (*abort) __P((DB_TXN *));
	int	  (*commit) __P((DB_TXN *, u_int32_t));
	int	  (*commit_getlsn) __P((DB_TXN *, u_int32_t, u_int64_t *, DB_LSN *, void *));
	int	  (*commit_rowlocks) __P((DB_TXN *, u_int32_t, u_int64_t,
			  u_int32_t, DB_LSN *,DBT *, DB_LOCK *,
			  u_int32_t, u_int64_t *, DB_LSN *, DB_LSN *, void *));
	int   (*dist_prepare) __P((DB_TXN *, const char *, const char *, const char *, u_int32_t, DBT *, u_int32_t));
	int   (*getlogbytes) __P((DB_TXN *, u_int64_t *));
	int	  (*discard) __P((DB_TXN *, u_int32_t));
	u_int32_t (*id) __P((DB_TXN *));
	int	  (*prepare) __P((DB_TXN *, u_int8_t *));
	int	  (*set_timeout) __P((DB_TXN *, db_timeout_t, u_int32_t));

#define	TXN_CHILDCOMMIT	0x001		/* Transaction that has committed. */
#define	TXN_COMPENSATE	0x002		/* Compensating transaction. */
#define	TXN_DIRTY_READ	0x004		/* Transaction does dirty reads. */
#define	TXN_LOCKTIMEOUT	0x008		/* Transaction has a lock timeout. */
#define	TXN_MALLOC	0x010		/* Structure allocated by TXN system. */
#define	TXN_NOSYNC	0x020		/* Do not sync on prepare and commit. */
#define	TXN_NOWAIT	0x040		/* Do not wait on locks. */
#define	TXN_RESTORED	0x080		/* Transaction has been restored. */
#define	TXN_SYNC	0x100		/* Sync on prepare and commit. */
#define	TXN_RECOVER_LOCK	0x200 /* Transaction holds the recovery lock */
#define TXN_FOP_NOBLOCK		0x400 /* Dont block on fop transactions */
#define TXN_DIST_PREPARED	0x800 /* Dist-txn has written prepare record */
#define TXN_DIST_DISCARD	0x1000 /* Discard a repared dist-txn */
#define TXN_NOPREP	        0x2000 /* Prevent prepares against this txn */
	u_int32_t	flags;

	void	 *app_private;		/* pointer to bdb transaction object */
	DB_LSN   we_start_at_this_lsn;	/* hard to pinpoint the
					 * existing startlsn usage, so
					 * this is a new one */
	void			*pglogs_hashtbl;
	pthread_mutex_t pglogs_mutex;
	u_int64_t utxnid;
	char *dist_txnid;
	char *coordinator_name;
	char *coordinator_tier;
	u_int32_t coordinator_gen;
	DBT blkseq_key;
};

typedef enum {
	DB_DIST_HAVELOCKS   = 0x00000001,
	DB_DIST_COMMITTED   = 0x00000002,
	DB_DIST_ABORTED     = 0x00000004,
	DB_DIST_SCHEMA_LK   = 0x00000008,
	DB_DIST_INFLIGHT    = 0x00000010,
	DB_DIST_RECOVERED   = 0x00000020,
	DB_DIST_UPDSHADOWS  = 0x00000040
} db_dist_state;

struct __db_txn_prepared_child {
	u_int64_t cutxnid;
	DB_TXN_PREPARED *p;
    LINKC_T(struct __db_txn_prepared_child) lnk;
};

typedef LISTC_T(struct __db_txn_prepared_child) txn_children;

struct __db_txn_prepared {
	char *dist_txnid;
	u_int64_t utxnid;
	u_int32_t flags;
	DB_LSN prepare_lsn;
	DB_LSN prev_lsn;
	DB_LSN begin_lsn;
	DBT blkseq_key;
	u_int32_t coordinator_gen;
	DBT coordinator_name;
	DBT coordinator_tier;
	struct __db_txn *txnp;
	void *pglogs;
	u_int32_t keycnt;
	txn_children children;
};

/*
 * Structure used for two phase commit interface.  Berkeley DB support for two
 * phase commit is compatible with the X/open XA interface.
 *
 * The XA #define XIDDATASIZE defines the size of a global transaction ID.  We
 * have our own version here (for name space reasons) which must have the same
 * value.
 */
#define	DB_XIDDATASIZE	128
struct __db_preplist {
	DB_TXN	*txn;
	u_int8_t gid[DB_XIDDATASIZE];
};

/* Transaction statistics structure. */
struct __db_txn_active {
	u_int32_t txnid;		/* Transaction ID */
	u_int32_t parentid;		/* Transaction ID of parent */
	DB_LSN	  lsn;			/* LSN when transaction began */
	u_int32_t xa_status;		/* XA status */
	u_int8_t  xid[DB_XIDDATASIZE];	/* XA global transaction ID */
};

struct __db_txn_stat {
	DB_LSN	  st_last_ckp;		/* lsn of the last checkpoint */
	DB_LSN	  st_ckp_lsn;		/* ckp-lsn of last checkpoint */
	time_t	  st_time_ckp;		/* time of last checkpoint */
	u_int32_t st_last_txnid;	/* last transaction id given out */
	u_int32_t st_maxtxns;		/* maximum txns possible */
	u_int32_t st_naborts;		/* number of aborted transactions */
	u_int32_t st_nbegins;		/* number of begun transactions */
	u_int32_t st_ncommits;		/* number of committed transactions */
	u_int32_t st_nactive;		/* number of active transactions */
	u_int32_t st_nrestores;		/* number of restored transactions
					   after recovery. */
	u_int32_t st_maxnactive;	/* maximum active transactions */
	DB_TXN_ACTIVE *st_txnarray;	/* array of active transactions */
	u_int32_t st_region_wait;	/* Region lock granted after wait. */
	u_int32_t st_region_nowait;	/* Region lock granted without wait. */
	u_int32_t st_regsize;		/* Region size. */
};

/*******************************************************
 * Replication.
 *******************************************************/
/* Special, out-of-band environment IDs. */
extern char *db_eid_broadcast;
extern char *db_eid_invalid;

/* rep_start flags values */
#define	DB_REP_CLIENT		0x001
#define	DB_REP_LOGSONLY		0x002
#define	DB_REP_MASTER		0x004

/* rep truncate flags */
#define DB_REP_TRUNCATE_MASTER 0x001
#define DB_REP_TRUNCATE_ONLINE 0x002

/* Replication statistics. */
struct __db_rep_stat {
	/* !!!
	 * Many replication statistics fields cannot be protected by a mutex
	 * without an unacceptable performance penalty, since most message
	 * processing is done without the need to hold a region-wide lock.
	 * Fields whose comments end with a '+' may be updated without holding
	 * the replication or log mutexes (as appropriate), and thus may be
	 * off somewhat (or, on unreasonable architectures under unlucky
	 * circumstances, garbaged).
	 */
	u_int32_t st_status;		/* Current replication status. */
	DB_LSN st_next_lsn;		/* Next LSN to use or expect. */
	DB_LSN st_waiting_lsn;		/* LSN we're awaiting, if any. */

	u_int32_t st_dupmasters;	/* # of times a duplicate master
					   condition was detected.+ */
	char* st_env_id;			/* Current environment ID. */
	int st_env_priority;		/* Current environment priority. */
	u_int32_t st_gen;		/* Current generation number. */
	u_int32_t st_in_recovery;	/* This site is in client sync-up. */
	u_int32_t st_log_duplicated;	/* Log records received multiply.+ */
	u_int32_t st_log_queued;	/* Log records currently queued.+ */
	u_int32_t st_log_queued_max;	/* Max. log records queued at once.+ */
	u_int32_t st_log_queued_total;	/* Total # of log recs. ever queued.+ */
	u_int32_t st_log_records;	/* Log records received and put.+ */
	u_int32_t st_log_requested;	/* Log recs. missed and requested.+ */
	char *st_master;			/* Env. ID of the current master. */
	u_int32_t st_master_changes;	/* # of times we've switched masters. */
	u_int32_t st_msgs_badgen;	/* Messages with a bad generation #.+ */
	u_int32_t st_msgs_processed;	/* Messages received and processed.+ */
	u_int32_t st_msgs_recover;	/* Messages ignored because this site
					   was a client in recovery.+ */
	u_int32_t st_msgs_send_failures;/* # of failed message sends.+ */
	u_int32_t st_msgs_sent;		/* # of successful message sends.+ */
	u_int32_t st_newsites;		/* # of NEWSITE msgs. received.+ */
	int st_nsites;			/* Current number of sites we will
					   assume during elections. */
	u_int32_t st_nthrottles;	/* # of times we were throttled. */
	u_int32_t st_outdated;		/* # of times we detected and returned
					   an OUTDATED condition.+ */
	u_int32_t st_txns_applied;	/* # of transactions applied.+ */

	/* Elections generally. */
	u_int32_t st_elections;		/* # of elections held.+ */
	u_int32_t st_elections_won;	/* # of elections won by this site.+ */

	/* Statistics about an in-progress election. */
	char* st_election_cur_winner;	/* Current front-runner. */
	u_int32_t st_election_gen;	/* Election generation number. */
	DB_LSN st_election_lsn;		/* Max. LSN of current winner. */
	int st_election_nsites;		/* # of "registered voters". */
	int st_election_priority;	/* Current election priority. */
	int st_election_status;		/* Current election status. */
	int st_election_tiebreaker;	/* Election tiebreaker value. */
	int st_election_votes;		/* Votes received in this round. */

	/* Replication retry stats */
	unsigned long long retry;	/* replication retried for deadlock  */
	int				/* max number of times a replication
					 * has been retried on replicant due to
					 * deadlock */
		max_replication_trans_retries;

	u_int64_t lc_cache_hits;	/* Transaction commit records in cache*/
	u_int64_t lc_cache_misses;	/* Transaction commit records
					 * NOT in cache */
	int lc_cache_size;		/* Current size of lc cache */
	uint32_t durable_gen;
	DB_LSN durable_lsn;
};


/*******************************************************
 * Access methods.
 *******************************************************/
typedef enum {
	DB_BTREE=1,
	DB_HASH=2,
	DB_RECNO=3,
	DB_QUEUE=4,
	DB_UNKNOWN=5,			/* Figure it out on open. */
	DB_TYPE_MAX=6
} DBTYPE;

#define	DB_RENAMEMAGIC	0x030800	/* File has been renamed. */

#define	DB_BTREEVERSION	9		/* Current btree version. */
#define	DB_BTREEOLDVER	8		/* Oldest btree version supported. */
#define	DB_BTREEMAGIC	0x053162

#define	DB_HASHVERSION	8		/* Current hash version. */
#define	DB_HASHOLDVER	7		/* Oldest hash version supported. */
#define	DB_HASHMAGIC	0x061561

#define	DB_QAMVERSION	4		/* Current queue version. */
#define	DB_QAMOLDVER	3		/* Oldest queue version supported. */
#define	DB_QAMMAGIC	0x042253

/*
 * DB access method and cursor operation values.  Each value is an operation
 * code to which additional bit flags are added.
 */
#define	DB_AFTER		 1	/* c_put() */
#define	DB_APPEND		 2	/* put() */
#define	DB_BEFORE		 3	/* c_put() */
#define	DB_CACHED_COUNTS	 4	/* stat() */
#define	DB_CONSUME		 5	/* get() */
#define	DB_CONSUME_WAIT		 6	/* get() */
#define	DB_CURRENT		 7	/* c_get(), c_put(), DB_LOGC->get() */
#define	DB_FAST_STAT		 8	/* stat() */
#define	DB_FIRST		 9	/* c_get(), DB_LOGC->get() */
#define	DB_GET_BOTH		10	/* get(), c_get() */
#define	DB_GET_BOTHC		11	/* c_get() (internal) */
#define	DB_GET_BOTH_RANGE	12	/* get(), c_get() */
#define	DB_GET_RECNO		13	/* c_get() */
#define	DB_JOIN_ITEM		14	/* c_get(); do not do primary lookup */
#define	DB_KEYFIRST		15	/* c_put() */
#define	DB_KEYLAST		16	/* c_put() */
#define	DB_LAST			17	/* c_get(), DB_LOGC->get() */
#define	DB_NEXT			18	/* c_get(), DB_LOGC->get() */
#define	DB_NEXT_DUP		19	/* c_get() */
#define	DB_NEXT_NODUP		20	/* c_get() */
#define	DB_NODUPDATA		21	/* put(), c_put() */
#define	DB_NOOVERWRITE		22	/* put() */
#define	DB_NOSYNC		23	/* close() */
#define	DB_POSITION		24	/* c_dup() */
#define	DB_PREV			25	/* c_get(), DB_LOGC->get() */
#define	DB_PREV_NODUP		26	/* c_get(), DB_LOGC->get() */
#define	DB_RECORDCOUNT		27	/* stat() */
#define	DB_SET			28	/* c_get(), DB_LOGC->get() */
#define	DB_SET_LOCK_TIMEOUT	29	/* set_timout() */
#define	DB_SET_RANGE		30	/* c_get() */
#define	DB_SET_RECNO		31	/* get(), c_get() */
#define	DB_SET_TXN_NOW		32	/* set_timout() (internal) */
#define	DB_SET_TXN_TIMEOUT	33	/* set_timout() */
#define	DB_UPDATE_SECONDARY	34	/* c_get(), c_del() (internal) */
#define	DB_WRITECURSOR		35	/* cursor() */
#define	DB_WRITELOCK		36	/* cursor() (internal) */
#define DB_PREV_VALUE		37	/* prev record without moving cursor */
#define DB_NEXT_VALUE		38	/* next record without moving cursor */

/* This has to change when the max opcode hits 255. */
#define	DB_OPFLAGS_MASK	0x000000ff	/* Mask for operations flags. */

/*
 * Masks for flags that can be OR'd into DB access method and cursor
 * operation values.
 *
 *	DB_DIRTY_READ	0x02000000	   Dirty Read. */
#define	DB_MULTIPLE	0x04000000	/* Return multiple data values. */
#define	DB_MULTIPLE_KEY	0x08000000	/* Return multiple data/key pairs. */
#define	DB_RMW		0x10000000	/* Acquire write flag immediately. */

/*
 * DB (user visible) error return codes.
 *
 * !!!
 * For source compatibility with DB 2.X deadlock return (EAGAIN), use the
 * following:
 *	#include <errno.h>
 *	#define DB_LOCK_DEADLOCK EAGAIN
 *
 * !!!
 * We don't want our error returns to conflict with other packages where
 * possible, so pick a base error value that's hopefully not common.  We
 * document that we own the error name space from -30,800 to -30,999.
 */
/* DB (public) error return codes. */
#define	DB_DONOTINDEX		(-30999)/* "Null" return from 2ndary callbk. */
#define	DB_FILEOPEN		(-30998)/* Rename/remove while file is open. */
#define	DB_KEYEMPTY		(-30997)/* Key/data deleted or never created. */
#define	DB_KEYEXIST		(-30996)/* The key/data pair already exists. */
#define	DB_LOCK_DEADLOCK	(-30995)/* Deadlock. */
#define	DB_LOCK_NOTGRANTED	(-30994)/* Lock unavailable. */
#define	DB_NOSERVER		(-30993)/* Server panic return. */
#define	DB_NOSERVER_HOME	(-30992)/* Bad home sent to server. */
#define	DB_NOSERVER_ID		(-30991)/* Bad ID sent to server. */
#define	DB_NOTFOUND		(-30990)/* Key/data pair not found (EOF). */
#define	DB_OLD_VERSION		(-30989)/* Out-of-date version. */
#define	DB_PAGE_NOTFOUND	(-30988)/* Requested page not found. */
#define	DB_REP_DUPMASTER	(-30987)/* There are two masters. */
#define	DB_REP_HANDLE_DEAD	(-30986)/* Rolled back a commit. */
#define	DB_REP_HOLDELECTION	(-30985)/* Time to hold an election. */
#define	DB_REP_ISPERM		(-30984)/* Cached not written perm written.*/
#define	DB_REP_NEWMASTER	(-30983)/* We have learned of a new master. */
#define	DB_REP_NEWSITE		(-30982)/* New site entered system. */
#define	DB_REP_NOTPERM		(-30981)/* Permanent log record not written. */
#define	DB_REP_OUTDATED		(-30980)/* Site is too far behind master. */
#define	DB_REP_UNAVAIL		(-30979)/* Site cannot currently be reached. */
#define	DB_RUNRECOVERY		(-30978)/* Panic return. */
#define	DB_SECONDARY_BAD	(-30977)/* Secondary index corrupt. */
#define	DB_VERIFY_BAD		(-30976)/* Verify failed; bad format. */
#define	DB_CUR_STALE		(-30975)/* Cursor deserialization failed since 
					 * something had changed. comdb2 add */
#define	DB_HAS_MAJORITY		(-30974)/* have enough votes to become a majority */
#define DB_LOCK_DESIRED		(-31000)/* BDB needs  a long running operation
					 *  out of the library */
#define DB_LOCK_DEADLOCK_CUSTOM	(-31001)/* Deadlock on custom log record.  */

#define  DB_REP_STALEMASTER	(-31002) /* mismatch client/master identities */


/* DB (private) error return codes. */
#define DB_IGNORED			(-30900) /* Ignore logging to this btree */
#define	DB_ALREADY_ABORTED	(-30899)
#define	DB_DELETED		(-30898)/* Recovery file marked deleted. */
#define	DB_LOCK_NOTEXIST	(-30897)/* Object to lock is gone. */
#define	DB_NEEDSPLIT		(-30896)/* Page needs to be split. */
#define	DB_SURPRISE_KID		(-30895)/* Child commit where parent
					   didn't know it was a parent. */
#define	DB_SWAPBYTES		(-30894)/* Database needs byte swapping. */
#define	DB_TIMEOUT		(-30893)/* Timed out waiting for election. */
#define	DB_TXN_CKP		(-30892)/* Encountered ckp record in log. */
#define	DB_VERIFY_FATAL		(-30891)/* DB->verify cannot proceed. */
#define DB_FIRST_MISS		(-30890)/* Return on first-miss in memp_fget. */
#define DB_SEARCH_PGCACHE	(-30889)/* Search the recovery pagecache. */
#define DB_ELECTION_GENCHG  (-30888)

/* genid-pgno hashtable - some code stolen from plhash.c */
#define GENID_SIZE 8
#define HASH_GENID_SIZE 6

typedef struct __genid_pgno 
{
   u_int8_t genid[HASH_GENID_SIZE];
   u_int32_t pgno;
} __genid_pgno;

typedef struct genid_hash
{
   unsigned int ntbl;		/* num entries in this table */
   pthread_mutex_t mutex;
   __genid_pgno *tbl;
} genid_hash;

typedef struct
{
   u_int32_t n_bt_search;
   u_int32_t n_bt_hash;
   u_int32_t n_bt_hash_hit;
   u_int32_t n_bt_hash_miss;
   struct timeval t_bt_search;
} dbp_bthash_stat;

genid_hash *genid_hash_init(DB_ENV *dbenv, int sz);
void genid_hash_resize(DB_ENV *dbenv, genid_hash **hpp, int szkb);
void genid_hash_free(DB_ENV *dbenv, genid_hash *hp);

struct fileid_adj_fileid
{
	u_int8_t fileid[DB_FILE_ID_LEN];
	int adj_fileid;
};

/* Thread-local cursor queue definitions

   +-------------------------------------------+
   | gbl_all_cursors (linkedlist)			  |
   |										   |
   |		+----------------------------+	 |
   |		| Thread N				   |	 |
   |	  +----------------------------+ |	 |
   |	  | Thread (N - 1)			 | |	 |
   |	 //////////////////////////////| |	 |
   |	+----------------------------+/| |	 |
   |	| Thread 2				   |/| |	 |
   |  +----------------------------+ |/| |	 |
   |  | Thread 1				   | |/| |	 |
   |  |----------------------------| |/| |	 |
   |  | DB_CQ_HASH (hashtable)	 | |/| |	 |
   |  |----------------------------| |/| |	 |
   |  | Key  | Value			   | |/| |	 |
   |  |----------------------------| |/| |	 |
   |  | DB * | DB_CQ (linkedlist)  | |/| |	 |
   |  |	  | DBC <-> ... <-> DBC | |/| |	 |
   |  |----------------------------| |/| |	 |
   |  | DB * | DB_CQ (linkedlist)  | |/| |	 |
   |  |	  | DBC <-> ... <-> DBC | |/| |	 |
   |  |----------------------------| |/| |	 |
   |  ~ .......................... ~ |/| |	 |
   |  ~ .......................... ~ |/| |	 |
   |  ~ .......................... ~ |/| |	 |
   |  |----------------------------| |/| |	 |
   |  | DB * | DB_CQ (linkedlist)  | |/|-+	 |
   |  |	  | DBC <-> ... <-> DBC | |/+	   |
   |  |----------------------------| |/		|
   |  | DB * | DB_CQ (linkedlist)  |-+		 |
   |  |	  | DBC <-> ... <-> DBC |		   |
   |  +----------------------------+		   |
   |										   |
   +-------------------------------------------+
 */

/* Cursor queue */
typedef struct __db_cq {
	/* The DB structure the cursors use. It's also the hash key. */
	DB *db;
	/* This points to DB_CQ_HASH.lk */
	pthread_mutex_t *lk;
	/* Active queue */
	struct {
		DBC *tqh_first;
		DBC **tqh_last;
	} aq;
	/* Free queue */
	struct {
		DBC *tqh_first;
		DBC **tqh_last;
	} fq;
} DB_CQ;

/* Thread-local hashtable of cursor queues */
typedef struct __db_cq_hash {
	/* plhash */
	hash_t *h;
	/* Although the structure is mostly single-threaded, cursors may be
	   modified by another thread of control (e.g., a schema change
	   invalidates all free cursors). So guard the structure with a lock. */
	pthread_mutex_t lk;
	struct {
		struct __db_cq_hash *tqe_next;
		struct __db_cq_hash **tqe_prev;
	} links;
} DB_CQ_HASH;

/* List of thread-local hashtables of cursor queues */
typedef struct __db_cq_hash_list {
	DB_CQ_HASH *tqh_first;
	DB_CQ_HASH **tqh_last;
	pthread_mutex_t lk;
} DB_CQ_HASH_LIST;

extern DB_CQ_HASH_LIST gbl_all_cursors;
extern pthread_key_t tlcq_key;

/* Database handle. */
struct __db {
	/*******************************************************
	 * Public: owned by the application.
	 *******************************************************/
	u_int32_t pgsize;		/* Database logical page size. */

					/* Callbacks. */
	int (*db_append_recno) __P((DB *, DBT *, db_recno_t));
	void (*db_feedback) __P((DB *, int, int));
	int (*dup_compare) __P((DB *, const DBT *, const DBT *));

	void	*app_private;		/* Application-private handle. */

	/*******************************************************
	 * Private: owned by DB.
	 *******************************************************/
	DB_ENV	*dbenv;			/* Backing environment. */

	DBTYPE	 type;			/* DB access method type. */

	DB_MPOOLFILE *mpf;		/* Backing buffer pool. */

	DB_MUTEX *mutexp;		/* Synchronization for free threading */
	DB_MUTEX *free_mutexp;		/* Synchronization for free threading */

	char *fname, *dname;		/* File/database passed to DB->open. */
	u_int32_t open_flags;		/* Flags passed to DB->open. */

	u_int8_t fileid[DB_FILE_ID_LEN];/* File's unique ID for locking. */

	u_int32_t adj_fileid;		/* File's unique ID for curs. adj. */

#define	DB_LOGFILEID_INVALID	-1
	FNAME *log_filename;		/* File's naming info for logging. */
	int added_to_ufid;

	db_pgno_t meta_pgno;		/* Meta page number */
	u_int32_t lid;			/* Locker id for handle locking. */
	u_int32_t cur_lid;		/* Current handle lock holder. */
	u_int32_t associate_lid;	/* Locker id for DB->associate call. */
	DB_LOCK	handle_lock;		/* Lock held on this handle. */

	long	 cl_id;			/* RPC: remote client id. */

	time_t	 timestamp;		/* Handle timestamp for replication. */

	/*
	 * Returned data memory for DB->get() and friends.
	 */
	DBT	 my_rskey;		/* Secondary key. */
	DBT	 my_rkey;		/* [Primary] key. */
	DBT	 my_rdata;		/* Data. */

	/*
	 * !!!
	 * Some applications use DB but implement their own locking outside of
	 * DB.  If they're using fcntl(2) locking on the underlying database
	 * file, and we open and close a file descriptor for that file, we will
	 * discard their locks.  The DB_FCNTL_LOCKING flag to DB->open is an
	 * undocumented interface to support this usage which leaves any file
	 * descriptors we open until DB->close.  This will only work with the
	 * DB->open interface and simple caches, e.g., creating a transaction
	 * thread may open/close file descriptors this flag doesn't protect.
	 * Locking with fcntl(2) on a file that you don't own is a very, very
	 * unsafe thing to do.  'Nuff said.
	 */
	DB_FH	*saved_open_fhp;	/* Saved file handle. */

	/*
	 * Linked list of DBP's, linked from the DB_ENV, used to keep track
	 * of all open db handles for cursor adjustment.
	 *
	 * !!!
	 * Explicit representations of structures from queue.h.
	 * LIST_ENTRY(__db) dblistlinks;
	 */
	struct {
		struct __db *le_next;
		struct __db **le_prev;
	} dblistlinks;

	/* 1 if using thread-local cursor queues. */
	int use_tlcq;

	/*
	 * Cursor queues.
	 *
	 * !!!
	 * Explicit representations of structures from queue.h.
	 * TAILQ_HEAD(__cq_fq, __dbc) free_queue;
	 * TAILQ_HEAD(__cq_aq, __dbc) active_queue;
	 * TAILQ_HEAD(__cq_jq, __dbc) join_queue;
	 */
	struct __cq_fq {
		struct __dbc *tqh_first;
		struct __dbc **tqh_last;
	} free_queue;
	struct __cq_aq {
		struct __dbc *tqh_first;
		struct __dbc **tqh_last;
	} active_queue;
	struct __cq_jq {
		struct __dbc *tqh_first;
		struct __dbc **tqh_last;
	} join_queue;

	/*
	 * Secondary index support.
	 *
	 * Linked list of secondary indices -- set in the primary.
	 *
	 * !!!
	 * Explicit representations of structures from queue.h.
	 * LIST_HEAD(s_secondaries, __db);
	 */
	struct {
		struct __db *lh_first;
	} s_secondaries;

	/*
	 * List entries for secondaries, and reference count of how
	 * many threads are updating this secondary (see __db_c_put).
	 *
	 * !!!
	 * Note that these are synchronized by the primary's mutex, but
	 * filled in in the secondaries.
	 *
	 * !!!
	 * Explicit representations of structures from queue.h.
	 * LIST_ENTRY(__db) s_links;
	 */
	struct {
		struct __db *le_next;
		struct __db **le_prev;
	} s_links;
	u_int32_t s_refcnt;

	/* Secondary callback and free functions -- set in the secondary. */
	int	(*s_callback) __P((DB *, const DBT *, const DBT *, DBT *));

	/* Reference to primary -- set in the secondary. */
	DB	*s_primary;

	/* API-private structure: used by DB 1.85, C++, Java, Perl and Tcl */
	void	*api_internal;

	/* Subsystem-private structure. */
	void	*bt_internal;		/* Btree/Recno access method. */
	void	*h_internal;		/* Hash access method. */
	void	*q_internal;		/* Queue access method. */
	void	*xa_internal;		/* XA. */

					/* Methods. */
	int  (*associate) __P((DB *, DB_TXN *, DB *, int (*)(DB *, const DBT *,
		const DBT *, DBT *), u_int32_t));
	int  (*get_fileid) __P((DB *, u_int8_t *fileid));
	int  (*close) __P((DB *, u_int32_t));
	int  (*closetxn) __P((DB *, DB_TXN *, u_int32_t));
	int  (*cursor) __P((DB *, DB_TXN *, DBC **, u_int32_t));
	/* comdb2 addition */
	int  (*cursor_ser) __P((DB *, DB_TXN *, DBCS *, DBC **, u_int32_t));
	int  (*del) __P((DB *, DB_TXN *, DBT *, u_int32_t));
	void (*err) __P((DB *, int, const char *, ...));
	void (*errx) __P((DB *, const char *, ...));
	int  (*fd) __P((DB *, int *));
	int  (*get) __P((DB *, DB_TXN *, DBT *, DBT *, u_int32_t));
	int  (*pget) __P((DB *, DB_TXN *, DBT *, DBT *, DBT *, u_int32_t));
	int  (*get_byteswapped) __P((DB *, int *));
	int  (*get_cachesize) __P((DB *, u_int32_t *, u_int32_t *, int *));
	int  (*get_mp_multiple) __P((DB *, double *));
	int  (*get_dbname) __P((DB *, const char **, const char **));
	int  (*get_encrypt_flags) __P((DB *, u_int32_t *));
	int  (*get_env) __P((DB *, DB_ENV **));
	void (*get_errfile) __P((DB *, FILE **));
	void (*get_errpfx) __P((DB *, const char **));
	int  (*get_flags) __P((DB *, u_int32_t *));
	int  (*get_lorder) __P((DB *, int *));
	int  (*get_open_flags) __P((DB *, u_int32_t *));
	int  (*get_pagesize) __P((DB *, u_int32_t *));
	int  (*get_transactional) __P((DB *, int *));
	int  (*get_type) __P((DB *, DBTYPE *));
	int  (*join) __P((DB *, DBC **, DBC **, u_int32_t));
	int  (*key_range) __P((DB *,
		DB_TXN *, DBT *, DB_KEY_RANGE *, u_int32_t));
	int  (*open) __P((DB *, DB_TXN *,
		const char *, const char *, DBTYPE, u_int32_t, int));
	int  (*put) __P((DB *, DB_TXN *, DBT *, DBT *, u_int32_t));
	int  (*remove) __P((DB *, const char *, const char *, u_int32_t));
	int  (*rename) __P((DB *,
		const char *, const char *, const char *, u_int32_t));
	int  (*truncate) __P((DB *, DB_TXN *, u_int32_t *, u_int32_t));
	int  (*set_append_recno) __P((DB *, int (*)(DB *, DBT *, db_recno_t)));
	int  (*set_alloc) __P((DB *, void *(*)(size_t),
		void *(*)(void *, size_t), void (*)(void *)));
	int  (*set_cachesize) __P((DB *, u_int32_t, u_int32_t, int));
	int  (*set_mp_multiple) __P((DB *, double));
	int  (*set_dup_compare) __P((DB *,
		int (*)(DB *, const DBT *, const DBT *)));
	int  (*set_encrypt) __P((DB *, const char *, u_int32_t));
	void (*set_errcall) __P((DB *, void (*)(const char *, char *)));
	void (*set_errfile) __P((DB *, FILE *));
	void (*set_errpfx) __P((DB *, const char *));
	int  (*set_feedback) __P((DB *, void (*)(DB *, int, int)));
	int  (*set_flags) __P((DB *, u_int32_t));
	int  (*set_lorder) __P((DB *, int));
	int  (*set_pagesize) __P((DB *, u_int32_t));
	int  (*set_paniccall) __P((DB *, void (*)(DB_ENV *, int)));
	int  (*stat) __P((DB *, void *, u_int32_t));
	int  (*sync) __P((DB *, u_int32_t));
	int  (*upgrade) __P((DB *, const char *, u_int32_t));
	int  (*verify) __P((DB *,
		const char *, const char *, FILE *, u_int32_t));

	int  (*get_bt_minkey) __P((DB *, u_int32_t *));
	int  (*set_bt_compare) __P((DB *,
		int (*)(DB *, const DBT *, const DBT *)));
	int  (*set_bt_maxkey) __P((DB *, u_int32_t));
	int  (*set_bt_minkey) __P((DB *, u_int32_t));
	int  (*set_bt_prefix) __P((DB *,
		size_t (*)(DB *, const DBT *, const DBT *)));

	int  (*get_h_ffactor) __P((DB *, u_int32_t *));
	int  (*get_h_nelem) __P((DB *, u_int32_t *));
	int  (*set_h_ffactor) __P((DB *, u_int32_t));
	int  (*set_h_hash) __P((DB *,
		u_int32_t (*)(DB *, const void *, u_int32_t)));
	int  (*set_h_nelem) __P((DB *, u_int32_t));

	int  (*get_re_delim) __P((DB *, int *));
	int  (*get_re_len) __P((DB *, u_int32_t *));
	int  (*get_re_pad) __P((DB *, int *));
	int  (*get_re_source) __P((DB *, const char **));
	int  (*set_re_delim) __P((DB *, int));
	int  (*set_re_len) __P((DB *, u_int32_t));
	int  (*set_re_pad) __P((DB *, int));
	int  (*set_re_source) __P((DB *, const char *));

	int  (*get_q_extentsize) __P((DB *, u_int32_t *));
	int  (*set_q_extentsize) __P((DB *, u_int32_t));

	int  (*db_am_remove) __P((DB *,
		DB_TXN *, const char *, const char *, DB_LSN *));
	int  (*db_am_rename) __P((DB *, DB_TXN *,
		const char *, const char *, const char *));

	/* COMDB2 MODIFICATION
	 * Create a cursor with the same locker id as an existing cursor.
	 * This allows readonly to work without undetectable (by berkeley)
	 * deadlocks.
	 */
	int  (*paired_cursor) __P((DB *, DBC *, DBC **, u_int32_t));
	int  (*paired_cursor_from_txn) __P((DB *, DB_TXN *, DBC **, u_int32_t));
	int  (*paired_cursor_from_lid) __P((DB *, u_int32_t, DBC **, u_int32_t));
	int  (*cursor_nocount) __P((DB *, DB_TXN *, DBC **, u_int32_t));
	int  (*get_numpages) __P((DB *, db_pgno_t *));

	/*
	 * Never called; these are a place to save function pointers
	 * so that we can undo an associate.
	 */
	int  (*stored_get) __P((DB *, DB_TXN *, DBT *, DBT *, u_int32_t));
	int  (*stored_close) __P((DB *, u_int32_t));

#define	DB_OK_BTREE	0x01
#define	DB_OK_HASH	0x02
#define	DB_OK_QUEUE	0x04
#define	DB_OK_RECNO	0x08
	u_int32_t	am_ok;		/* Legal AM choices. */

#define	DB_AM_CHKSUM		0x00000001 /* Checksumming. */
#define	DB_AM_CL_WRITER		0x00000002 /* Allow writes in client replica. */
#define	DB_AM_COMPENSATE	0x00000004 /* Created by compensating txn. */
#define	DB_AM_CREATED		0x00000008 /* Database was created upon open. */
#define	DB_AM_CREATED_MSTR	0x00000010 /* Encompassing file was created. */
#define	DB_AM_DBM_ERROR		0x00000020 /* Error in DBM/NDBM database. */
#define	DB_AM_DELIMITER		0x00000040 /* Variable length delimiter set. */
#define	DB_AM_DIRTY		0x00000080 /* Support Dirty Reads. */
#define	DB_AM_DISCARD		0x00000100 /* Discard any cached pages. */
#define	DB_AM_DUP		0x00000200 /* DB_DUP. */
#define	DB_AM_DUPSORT		0x00000400 /* DB_DUPSORT. */
#define	DB_AM_ENCRYPT		0x00000800 /* Encryption. */
#define	DB_AM_FIXEDLEN		0x00001000 /* Fixed-length records. */
#define	DB_AM_INMEM		0x00002000 /* In-memory; no sync on close. */
#define	DB_AM_IN_RENAME		0x00004000 /* File is being renamed. */
#define	DB_AM_NOT_DURABLE	0x00008000 /* Do not log changes. */
#define	DB_AM_OPEN_CALLED	0x00010000 /* DB->open called. */
#define	DB_AM_PAD		0x00020000 /* Fixed-length record pad. */
#define	DB_AM_PGDEF		0x00040000 /* Page size was defaulted. */
#define	DB_AM_RDONLY		0x00080000 /* Database is readonly. */
#define	DB_AM_RECNUM		0x00100000 /* DB_RECNUM. */
#define	DB_AM_RECOVER		0x00200000 /* DB opened by recovery routine. */
#define	DB_AM_RENUMBER		0x00400000 /* DB_RENUMBER. */
#define	DB_AM_REPLICATION	0x00800000 /* An internal replication file. */
#define	DB_AM_REVSPLITOFF	0x01000000 /* DB_REVSPLITOFF. */
#define	DB_AM_SECONDARY		0x02000000 /* Database is a secondary index. */
#define	DB_AM_SNAPSHOT		0x04000000 /* DB_SNAPSHOT. */
#define	DB_AM_SUBDB		0x08000000 /* Subdatabases supported. */
#define	DB_AM_SWAP		0x10000000 /* Pages need to be byte-swapped. */
#define	DB_AM_TXN		0x20000000 /* Opened in a transaction. */
#define	DB_AM_VERIFYING		0x40000000 /* DB handle is in the verifier. */
#define	DB_AM_HASH		   0x80000000 /* Datafile with genid-pg hash */
	u_int32_t orig_flags;		   /* Flags at  open, for refresh. */
	u_int32_t flags;

	/* DB *peer points to DB with the same fileid that has
	 * genid-pg hash set up in dbenv->dblist. Peer will be used in
	 * recovery to update the page hash
	 */
	DB *peer;

	/* DB **revpeer points back to the recovery dbp.  It allows us
	 * to set it's peer pointer to NULL if the table is closed.
	 */

	DB **revpeer;
	int revpeer_count;

	int is_free;

	genid_hash *pg_hash;

	dbp_bthash_stat pg_hash_stat;

	LINKC_T(DB) adjlnk;
	int inadjlist;

#define DB_PFX_COMP		  0x0000001
#define DB_SFX_COMP		  0x0000002
#define DB_RLE_COMP		  0x0000004
	uint8_t compression_flags;
	void (*set_compression_flags) __P((DB *, uint8_t));
	uint8_t (*get_compression_flags) __P((DB *));
	uint8_t temptable;
	int offset_bias;
	uint8_t olcompact;
	struct __db_trigger_subscription *trigger_subscription;
};

/*
 * Macros for bulk get.  These are only intended for the C API.
 * For C++, use DbMultiple*Iterator.
 */
#define	DB_MULTIPLE_INIT(pointer, dbt)					\
	(pointer = (u_int8_t *)(dbt)->data +				\
		(dbt)->ulen - sizeof(u_int32_t))
#define	DB_MULTIPLE_NEXT(pointer, dbt, retdata, retdlen)		\
	do {								\
		if (*((u_int32_t *)(pointer)) == (u_int32_t)-1) {	\
			retdata = NULL;					\
			pointer = NULL;					\
			break;						\
		}							\
		retdata = (u_int8_t *)					\
			(dbt)->data + *(u_int32_t *)(pointer);		\
		(pointer) = (u_int32_t *)(pointer) - 1;			\
		retdlen = *(u_int32_t *)(pointer);			\
		(pointer) = (u_int32_t *)(pointer) - 1;			\
		if (retdlen == 0 &&					\
			retdata == (u_int8_t *)(dbt)->data)			\
			retdata = NULL;					\
	} while (0)
#define	DB_MULTIPLE_KEY_NEXT(pointer, dbt, retkey, retklen, retdata, retdlen) \
	do {								\
		if (*((u_int32_t *)(pointer)) == (u_int32_t)-1) {	\
			retdata = NULL;					\
			retkey = NULL;					\
			pointer = NULL;					\
			break;						\
		}							\
		retkey = (u_int8_t *)					\
			(dbt)->data + *(u_int32_t *)(pointer);		\
		(pointer) = (u_int32_t *)(pointer) - 1;			\
		retklen = *(u_int32_t *)(pointer);			\
		(pointer) = (u_int32_t *)(pointer) - 1;			\
		retdata = (u_int8_t *)					\
			(dbt)->data + *(u_int32_t *)(pointer);		\
		(pointer) = (u_int32_t *)(pointer) - 1;			\
		retdlen = *(u_int32_t *)(pointer);			\
		(pointer) = (u_int32_t *)(pointer) - 1;			\
	} while (0)

#define	DB_MULTIPLE_RECNO_NEXT(pointer, dbt, recno, retdata, retdlen)   \
	do {								\
		if (*((u_int32_t *)(pointer)) == (u_int32_t)0) {	\
			recno = 0;					\
			retdata = NULL;					\
			pointer = NULL;					\
			break;						\
		}							\
		recno = *(u_int32_t *)(pointer);			\
		(pointer) = (u_int32_t *)(pointer) - 1;			\
		retdata = (u_int8_t *)					\
			(dbt)->data + *(u_int32_t *)(pointer);		\
		(pointer) = (u_int32_t *)(pointer) - 1;			\
		retdlen = *(u_int32_t *)(pointer);			\
		(pointer) = (u_int32_t *)(pointer) - 1;			\
	} while (0)

/*******************************************************
 * Access method cursors.
 *******************************************************/
#define MAXSTACKDEPTH 25
#define SKIPFRAMES 3
struct cursor_track {
	void *stack[MAXSTACKDEPTH];
	unsigned nframes;
	u_int32_t lockerid;
};


struct __dbc {
	DB *dbp;			/* Related DB access method. */
	DB_TXN	 *txn;			/* Associated transaction. */

	/*
	 * Active/free cursor queues.
	 *
	 * !!!
	 * Explicit representations of structures from queue.h.
	 * TAILQ_ENTRY(__dbc) links;
	 */
	struct {
		DBC *tqe_next;
		DBC **tqe_prev;
	} links;

	/*
	 * The DBT *'s below are used by the cursor routines to return
	 * data to the user when DBT flags indicate that DB should manage
	 * the returned memory.  They point at a DBT containing the buffer
	 * and length that will be used, and "belonging" to the handle that
	 * should "own" this memory.  This may be a "my_*" field of this
	 * cursor--the default--or it may be the corresponding field of
	 * another cursor, a DB handle, a join cursor, etc.  In general, it
	 * will be whatever handle the user originally used for the current
	 * DB interface call.
	 */
	DBT	 *rskey;		/* Returned secondary key. */
	DBT	 *rkey;			/* Returned [primary] key. */
	DBT	 *rdata;		/* Returned data. */

	DBT	  my_rskey;		/* Space for returned secondary key. */
	DBT	  my_rkey;		/* Space for returned [primary] key. */
	DBT	  my_rdata;		/* Space for returned data. */

	u_int32_t lid;			/* Default process' locker id. */
	u_int32_t locker;		/* Locker for this operation. */
	u_int32_t origlocker;
	DBT	  lock_dbt;		/* DBT referencing lock. */
	DB_LOCK_ILOCK lock;		/* Object to be locked. */
	DB_LOCK	  mylock;		/* CDB lock held on this cursor. */

	long	  cl_id;		/* Remote client id. */

	DBTYPE	  dbtype;		/* Cursor type. */

	DBC_INTERNAL *internal;		/* Access method private. */


	int (*c_close) __P((DBC *));	/* Methods: public. */
	int (*c_close_nocount) __P((DBC *));	/* Methods: public. */


	/* comdb2 additions */
	int (*c_getpgno) __P((DBC *, db_pgno_t *pgno));
	int (*c_getnextpgno) __P((DBC *, db_pgno_t *nextpgno));
	int (*c_firstleaf) __P((DBC *, db_pgno_t *pgno));
	int (*c_getgenids) __P((DBC *, unsigned long long *genids, int *num,
							 int max  ));
	int (*c_close_ser) __P((DBC *, DBCS *));
	int (*c_pause) __P((DBC *, DBCPS *));
	int (*c_unpause) __P((DBC *, DBCPS *));
	int (*c_get_pagelsn) __P((DBC *, DB_LSN *));
	int (*c_get_pageindex) __P((DBC *, int *, int *));
	int (*c_get_fileid) __P((DBC *, void *));
	int (*c_get_pageinfo) __P((DBC *, int *, int *, DB_LSN *));

	int (*c_count) __P((DBC *, db_recno_t *, u_int32_t));
	int (*c_del) __P((DBC *, u_int32_t));
	int (*c_dup) __P((DBC *, DBC **, u_int32_t));
	int (*c_get) __P((DBC *, DBT *, DBT *, u_int32_t));
	int (*c_get_dup) __P((DBC *, DBC **, DBT *, DBT *, u_int32_t));
	int (*c_pget) __P((DBC *, DBT *, DBT *, DBT *, u_int32_t));
	int (*c_put) __P((DBC *, DBT *, DBT *, u_int32_t));
	int (*c_skip_stat) __P((DBC *, u_int64_t *nxtcnt, u_int64_t *skpcnt));
	int (*c_replace_lockid) __P((DBC *, u_int32_t));

					/* Methods: private. */
	int (*c_am_bulk) __P((DBC *, DBT *, u_int32_t));
	int (*c_am_close) __P((DBC *, db_pgno_t, int *));
	int (*c_am_del) __P((DBC *));
	int (*c_am_destroy) __P((DBC *));
	int (*c_am_get) __P((DBC *, DBT *, DBT *, u_int32_t, db_pgno_t *));
	int (*c_am_put) __P((DBC *, DBT *, DBT *, u_int32_t, db_pgno_t *));
	int (*c_am_writelock) __P((DBC *));

#define	DBC_ACTIVE	 0x0001		/* Cursor in use. */
#define	DBC_COMPENSATE	 0x0002		/* Cursor compensating, don't lock. */
#define	DBC_DIRTY_READ	 0x0004		/* Cursor supports dirty reads. */
#define	DBC_OPD		 0x0008		/* Cursor references off-page dups. */
#define	DBC_RECOVER	 0x0010		/* Recovery cursor; don't log/lock. */
#define	DBC_RMW		 0x0020		/* Acquire write flag in read op. */
#define	DBC_TRANSIENT	 0x0040		/* Cursor is transient. */
#define	DBC_WRITECURSOR	 0x0080		/* Cursor may be used to write (CDB). */
#define	DBC_WRITER	 0x0100		/* Cursor immediately writing (CDB). */
#define	DBC_MULTIPLE	 0x0200		/* Return Multiple data. */
#define	DBC_MULTIPLE_KEY 0x0400		/* Return Multiple keys and data. */
#define	DBC_OWN_LID	 0x0800		/* Free lock id on destroy. */
#define	DBC_PAGE_ORDER	 0x1000		/* Traverse btree in page-order. */
#define	DBC_DISCARD_PAGES 0x2000	/* Fast discard pages after reading. */
#define	DBC_PAUSIBLE	 0x4000		/* Never considered for curadj */
	u_int32_t flags;

	int pp_allocated;   /* the owner of the cursor tracking structure */
	
	pthread_t   tid;	/* tid of the creating thread */

	struct cursor_track stackinfo;

	u_int64_t   nextcount;
	u_int64_t   skipcount;
	
	char*	   pf; // Added by Fabio for prefaulting the index pages
	db_pgno_t   lastpage; // pgno of last move
};
extern pthread_key_t DBG_FREE_CURSOR;

struct __dbg_free_cursor {
	int counter;
}; 

/*******************************************************
 * Paused and serialized cursor. comdb2 addition.
 *******************************************************/
struct __dbc_pause {
	u_int8_t opaque[20];
};
struct __dbc_ser {
	u_int8_t opaque[40];
};


int transfer_cursor_ownership(DBC *cold, DBC *cnew);

/* Key range statistics structure */
struct __key_range {
	double less;
	double equal;
	double greater;
};

/* Btree/Recno statistics structure. */
struct __db_bt_stat {
	u_int32_t bt_magic;		/* Magic number. */
	u_int32_t bt_version;		/* Version number. */
	u_int32_t bt_metaflags;		/* Metadata flags. */
	u_int32_t bt_nkeys;		/* Number of unique keys. */
	u_int32_t bt_ndata;		/* Number of data items. */
	u_int32_t bt_pagesize;		/* Page size. */
	u_int32_t bt_maxkey;		/* Maxkey value. */
	u_int32_t bt_minkey;		/* Minkey value. */
	u_int32_t bt_re_len;		/* Fixed-length record length. */
	u_int32_t bt_re_pad;		/* Fixed-length record pad. */
	u_int32_t bt_levels;		/* Tree levels. */
	u_int32_t bt_int_pg;		/* Internal pages. */
	u_int32_t bt_leaf_pg;		/* Leaf pages. */
	u_int32_t bt_dup_pg;		/* Duplicate pages. */
	u_int32_t bt_over_pg;		/* Overflow pages. */
	u_int32_t bt_free;		/* Pages on the free list. */
	u_int32_t bt_int_pgfree;	/* Bytes free in internal pages. */
	u_int32_t bt_leaf_pgfree;	/* Bytes free in leaf pages. */
	u_int32_t bt_dup_pgfree;	/* Bytes free in duplicate pages. */
	u_int32_t bt_over_pgfree;	/* Bytes free in overflow pages. */
};

/* Hash statistics structure. */
struct __db_h_stat {
	u_int32_t hash_magic;		/* Magic number. */
	u_int32_t hash_version;		/* Version number. */
	u_int32_t hash_metaflags;	/* Metadata flags. */
	u_int32_t hash_nkeys;		/* Number of unique keys. */
	u_int32_t hash_ndata;		/* Number of data items. */
	u_int32_t hash_pagesize;	/* Page size. */
	u_int32_t hash_ffactor;		/* Fill factor specified at create. */
	u_int32_t hash_buckets;		/* Number of hash buckets. */
	u_int32_t hash_free;		/* Pages on the free list. */
	u_int32_t hash_bfree;		/* Bytes free on bucket pages. */
	u_int32_t hash_bigpages;	/* Number of big key/data pages. */
	u_int32_t hash_big_bfree;	/* Bytes free on big item pages. */
	u_int32_t hash_overflows;	/* Number of overflow pages. */
	u_int32_t hash_ovfl_free;	/* Bytes free on ovfl pages. */
	u_int32_t hash_dup;		/* Number of dup pages. */
	u_int32_t hash_dup_free;	/* Bytes free on duplicate pages. */
};

/* Queue statistics structure. */
struct __db_qam_stat {
	u_int32_t qs_magic;		/* Magic number. */
	u_int32_t qs_version;		/* Version number. */
	u_int32_t qs_metaflags;		/* Metadata flags. */
	u_int32_t qs_nkeys;		/* Number of unique keys. */
	u_int32_t qs_ndata;		/* Number of data items. */
	u_int32_t qs_pagesize;		/* Page size. */
	u_int32_t qs_extentsize;	/* Pages per extent. */
	u_int32_t qs_pages;		/* Data pages. */
	u_int32_t qs_re_len;		/* Fixed-length record length. */
	u_int32_t qs_re_pad;		/* Fixed-length record pad. */
	u_int32_t qs_pgfree;		/* Bytes free in data pages. */
	u_int32_t qs_first_recno;	/* First not deleted record. */
	u_int32_t qs_cur_recno;		/* Next available record number. */
};


typedef struct
{
	char *data_dir;
	char *txn_dir;
	char *tmp_dir;
} comdb2_dirs_type;
   

/*******************************************************
 * Environment.
 *******************************************************/
#define	DB_REGION_MAGIC	0x120897	/* Environment magic number. */

/* TODO: use something bdb/attr.h-like so we don't need to edit 17 places
 * to have settable attributes */
struct __dbenv_attr {
	char *iomapfile;
#undef BERK_DEF_ATTR
#define BERK_DEF_ATTR(option, description, type, default_value) \
	int option;
#include "dbinc/db_attr.h"
#undef BERK_DEF_ATTR
};

struct __dbenv_envmap {
	int memptrickle_active;
};

struct lsn_range {
	DB_LSN start;
	DB_LSN end;
	char *fname;
	DB *dbp;
	LINKC_T(struct lsn_range) lnk;
};

typedef LISTC_T(struct lsn_range) lsn_range_list;
struct fileid_track {
	lsn_range_list *ranges;
	int numids;
};

enum {
	MINTRUNCATE_START = 0,
	MINTRUNCATE_SCAN = 1,
	MINTRUNCATE_READY = 2
};

enum {
	MINTRUNCATE_DBREG_START = 1,
	MINTRUNCATE_CHECKPOINT = 2
};

struct mintruncate_entry {
	int type;
	int32_t timestamp;
	DB_LSN lsn;
	DB_LSN ckplsn;
#ifdef MINTRUNCATE_DEBUG
	char *func;
#endif
	LINKC_T(struct mintruncate_entry) lnk;
};

/* hoisted out of rep.h */
struct __lsn_collection {
	int nlsns;
	int nalloc;
	struct logrecord *array;
	int memused;
	int had_serializable_records;
	int filled_from_cache;
	LISTC_T(UTXNID) *child_utxnids;
};

struct __lc_cache_entry {
	u_int32_t txnid;
	u_int64_t utxnid;
	int cacheid;  /* offset in __lc_cache.ent */
	DB_LSN last_seen_lsn;
	LINKC_T(struct __lc_cache_entry) lnk;
	LSN_COLLECTION lc;
};

struct __lc_cache {
	int nent;
	int memused;
	hash_t *txnid_hash;
	struct __lc_cache_entry *ent;
	LISTC_T(struct __lc_cache_entry) lru;
	LISTC_T(struct __lc_cache_entry) avail;
	pthread_mutex_t lk;
};

struct __ufid_to_db_t {
	u_int8_t ufid[DB_FILE_ID_LEN];
	int ignore : 1;
	char *fname;
	DB *dbp;
};

typedef int (*collect_locks_f)(void *args, int64_t threadid, int32_t lockerid,
		const char *mode, const char *status, const char *table,
		int64_t page, const char *rectype, int stackid);

typedef int (*collect_prepared_f)(void *args, char *dist_txnid, uint32_t flags,
		DB_LSN *lsn, DB_LSN *begin_lsn, uint32_t coordinator_gen, char *coordinator_name,
		char *coordinator_tier, uint64_t utxnid);

/* Database Environment handle. */
struct __db_env {
	/*******************************************************
	 * Public: owned by the application.
	 *******************************************************/
	FILE		*db_errfile;	/* Error message file stream. */
	const char	*db_errpfx;	/* Error message prefix. */
					/* Callbacks. */
	void (*db_errcall) __P((const char *, char *));
	void (*db_feedback) __P((DB_ENV *, int, int));
	void (*db_paniccall) __P((DB_ENV *, int));

					/* App-specified alloc functions. */
	void *(*db_malloc) __P((size_t));
	void *(*db_realloc) __P((void *, size_t));
	void (*db_free) __P((void *));

	/* expose logging rep_apply */
	int (*apply_log) __P((DB_ENV *, unsigned int, unsigned int, int64_t,
				void*, int));
	size_t (*get_log_header_size) __P((DB_ENV*)); 
	int (*rep_verify_match) __P((DB_ENV *, unsigned int, unsigned int, int));
	int (*mintruncate_lsn_timestamp) __P((DB_ENV *, int file, DB_LSN *outlsn, int32_t *timestamp));
	int (*dump_mintruncate_list) __P((DB_ENV *));
	int (*clear_mintruncate_list) __P((DB_ENV *));
	int (*build_mintruncate_list) __P((DB_ENV *));
	int (*mintruncate_delete_log) __P((DB_ENV *, int lowfile));

	/*
	 * Currently, the verbose list is a bit field with room for 32
	 * entries.  There's no reason that it needs to be limited, if
	 * there are ever more than 32 entries, convert to a bit array.
	 */
#define	DB_VERB_CHKPOINT	0x0001	/* List checkpoints. */
#define	DB_VERB_DEADLOCK	0x0002	/* Deadlock detection information. */
#define	DB_VERB_RECOVERY	0x0004	/* Recovery information. */
#define	DB_VERB_REPLICATION	0x0008	/* Replication information. */
#define	DB_VERB_WAITSFOR	0x0010	/* Dump waits-for table. */
#define DB_RECV_EXTENSION   ".recovery_pages" /* Recovery pages extention. */
	u_int32_t	 verbose;	/* Verbose output. */

	void		*app_private;	/* Application-private handle. */

	int (*app_dispatch)		/* User-specified recovery dispatch. */
		__P((DB_ENV *, DBT *, DB_LSN *, db_recops));

	/* Locking. */
	u_int8_t	*lk_conflicts;	/* Two dimensional conflict matrix. */
	u_int32_t	 lk_modes;	/* Number of lock modes in table. */
	u_int32_t	 lk_max;	/* Maximum number of locks. */
	u_int32_t	 lk_max_lockers;/* Maximum number of lockers. */
	u_int32_t	 lk_max_objects;/* Maximum number of locked objects. */
	u_int32_t	 lk_detect;	/* Deadlock detect on all conflicts. */
	db_timeout_t	 lk_timeout;	/* Lock timeout period. */

	/* Logging. */
	u_int32_t	 lg_bsize;	/* Buffer size. */
	u_int32_t	 lg_nsegs;	/* Number of log segments. */
	u_int32_t	 lg_size;	/* Log file size. */
	u_int32_t	 lg_regionmax;	/* Region size. */

	/* Memory pool. */
	u_int32_t	 mp_gbytes;	/* Cachesize: GB. */
	u_int32_t	 mp_bytes;	/* Cachesize: Bytes. */
	size_t		 mp_size;	/* DEPRECATED: Cachesize: bytes. */
	int		 mp_ncache;	/* Number of cache regions. */
	size_t		 mp_mmapsize;	/* Maximum file size for mmap. */
	int		 mp_maxwrite;	/* Maximum buffers to write. */
	int				/* Sleep after writing max buffers. */
			 mp_maxwrite_sleep;
	double		 mp_multiple;	/* Multiplier for hash buckets. */

	/* Number of recovery pages for each backing DB_MPOOLFILE. */
	int		 mp_recovery_pages;
	/* Page size for the replication database. */
	int		 rep_db_pagesize;

	/* Replication */
	char*		rep_eid;	/* environment id. */
	int		(*rep_send)	/* Send function. */
			__P((DB_ENV *, const DBT *, const DBT *,
				   const DB_LSN *, char*, int, void *));
	int	 (*rep_ignore) 
			__P((const char *));
	int	 (*txn_logical_start)
			__P((DB_ENV *, void *state, u_int64_t ltranid,
			DB_LSN *lsn));
	int	 (*txn_logical_commit)
			__P((DB_ENV *, void *state, u_int64_t ltranid,
			DB_LSN *lsn));
	DB_LSN last_locked_lsn;
	pthread_mutex_t locked_lsn_lk;

	/* Transactions. */
	u_int32_t	 tx_max;	/* Maximum number of transactions. */
	time_t		 tx_timestamp;	/* Recover to specific timestamp. */
	db_timeout_t	 tx_timeout;	/* Timeout for transactions. */
	int		 tx_perfect_ckp;	/* 1: Use perfect ckp. 0: Don't */

	/*******************************************************
	 * Private: owned by DB.
	 *******************************************************/
					/* User files, paths. */
	char		*db_home;	/* Database home. */
	char		*db_log_dir;	/* Database log file directory. */
	char		*db_tmp_dir;	/* Database tmp file directory. */

	char		   **db_data_dir;	/* Database data file directories. */
	int		 data_cnt;	/* Database data file slots. */
	int		 data_next;	/* Next Database data file slot. */

	int		 db_mode;	/* Default open permissions. */
	u_int32_t	 open_flags;	/* Flags passed to DB_ENV->open. */

	void		*reginfo;	/* REGINFO structure reference. */
	DB_FH		*lockfhp;	/* fcntl(2) locking file handle. */

	int		  (**recover_dtab)	/* Dispatch table for recover funcs. */
				__P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
	size_t		 recover_dtab_size;
					/* Slots in the dispatch table. */

	void		*cl_handle;	/* RPC: remote client handle. */
	long		 cl_id;		/* RPC: remote client env id. */

	int		 db_ref;	/* DB reference count. */

	long		 shm_key;	/* shmget(2) key. */
	u_int32_t	 tas_spins;	/* test-and-set spins. */

	/*
	 * List of open DB handles for this DB_ENV, used for cursor
	 * adjustment.  Must be protected for multi-threaded support.
	 *
	 * !!!
	 * As this structure is allocated in per-process memory, the
	 * mutex may need to be stored elsewhere on architectures unable
	 * to support mutexes in heap memory, e.g. HP/UX 9.
	 *
	 * !!!
	 * Explicit representation of structure in queue.h.
	 * LIST_HEAD(dblist, __db);
	 */
	DB_MUTEX	*dblist_mutexp;	/* Mutex. */
	struct {
		struct __db *lh_first;
	} dblist;

	/*
	 * XA support.
	 *
	 * !!!
	 * Explicit representations of structures from queue.h.
	 * TAILQ_ENTRY(__db_env) links;
	 * TAILQ_HEAD(xa_txn, __db_txn);
	 */
	struct {
		struct __db_env *tqe_next;
		struct __db_env **tqe_prev;
	} links;
	struct __xa_txn {	/* XA Active Transactions. */
		struct __db_txn *tqh_first;
		struct __db_txn **tqh_last;
	} xa_txn;
	int		 xa_rmid;	/* XA Resource Manager ID. */

	/* API-private structure. */
	void		*api1_internal;	/* C++, Perl API private */
	void		*api2_internal;	/* Java API private */

	char		*passwd;	/* Cryptography support. */
	size_t		 passwd_len;
	void		*crypto_handle;	/* Primary handle. */
	DB_MUTEX	*mt_mutexp;	/* Mersenne Twister mutex. */
	int		 mti;		/* Mersenne Twister index. */
	u_long		*mt;		/* Mersenne Twister state vector. */

					/* DB_ENV Methods. */
	int  (*close) __P((DB_ENV *, u_int32_t));
	int  (*dbremove) __P((DB_ENV *,
		DB_TXN *, const char *, const char *, u_int32_t));
	int  (*dbrename) __P((DB_ENV *, DB_TXN *,
		const char *, const char *, const char *, u_int32_t));
	void (*err) __P((const DB_ENV *, int, const char *, ...));
	void (*errx) __P((const DB_ENV *, const char *, ...));
	int  (*get_home) __P((DB_ENV *, const char **));
	int  (*get_open_flags) __P((DB_ENV *, u_int32_t *));
	int  (*open) __P((DB_ENV *, const char *, u_int32_t, int));
	int  (*remove) __P((DB_ENV *, const char *, u_int32_t));
	int  (*set_alloc) __P((DB_ENV *, void *(*)(size_t),
		void *(*)(void *, size_t), void (*)(void *)));
	int  (*set_app_dispatch) __P((DB_ENV *,
		int (*)(DB_ENV *, DBT *, DB_LSN *, db_recops)));
	int  (*get_data_dirs) __P((DB_ENV *, const char ***));
	int  (*set_data_dir) __P((DB_ENV *, const char *));
	int  (*get_encrypt_flags) __P((DB_ENV *, u_int32_t *));
	int  (*set_encrypt) __P((DB_ENV *, const char *, u_int32_t));
	void (*set_errcall) __P((DB_ENV *, void (*)(const char *, char *)));
	void (*get_errfile) __P((DB_ENV *, FILE **));
	void (*set_errfile) __P((DB_ENV *, FILE *));
	void (*get_errpfx) __P((DB_ENV *, const char **));
	void (*set_errpfx) __P((DB_ENV *, const char *));
	int  (*set_feedback) __P((DB_ENV *, void (*)(DB_ENV *, int, int)));
	int  (*get_flags) __P((DB_ENV *, u_int32_t *));
	int  (*set_flags) __P((DB_ENV *, u_int32_t, int));
	int  (*set_paniccall) __P((DB_ENV *, void (*)(DB_ENV *, int)));
	int  (*set_rpc_server) __P((DB_ENV *,
		void *, const char *, long, long, u_int32_t));
	int  (*get_shm_key) __P((DB_ENV *, long *));
	int  (*set_shm_key) __P((DB_ENV *, long));
	int  (*get_tas_spins) __P((DB_ENV *, u_int32_t *));
	int  (*set_tas_spins) __P((DB_ENV *, u_int32_t));
	int  (*get_tmp_dir) __P((DB_ENV *, const char **));
	int  (*set_tmp_dir) __P((DB_ENV *, const char *));
	int  (*get_verbose) __P((DB_ENV *, u_int32_t, int *));
	int  (*set_verbose) __P((DB_ENV *, u_int32_t, int));
	int  (*get_concurrent) __P((DB_ENV *env, int *val));
	int  (*set_lsn_chaining) __P((DB_ENV *, int));
	int  (*get_lsn_chaining) __P((DB_ENV *, int *));

	void *lg_handle;		/* Log handle and methods. */
	int  (*get_lg_bsize) __P((DB_ENV *, u_int32_t *));
	int  (*set_lg_bsize) __P((DB_ENV *, u_int32_t));
	int  (*get_lg_nsegs) __P((DB_ENV *, u_int32_t *));
	int  (*set_lg_nsegs) __P((DB_ENV *, u_int32_t));
	int  (*get_lg_dir) __P((DB_ENV *, const char **));
	int  (*set_lg_dir) __P((DB_ENV *, const char *));
	int  (*get_lg_max) __P((DB_ENV *, u_int32_t *));
	int  (*set_lg_max) __P((DB_ENV *, u_int32_t));
	int  (*get_lg_regionmax) __P((DB_ENV *, u_int32_t *));
	int  (*set_lg_regionmax) __P((DB_ENV *, u_int32_t));
	int  (*log_archive) __P((DB_ENV *, char **[], u_int32_t));
	int  (*log_get_last_lsn) __P((DB_ENV *, DB_LSN *));
	int  (*log_cursor) __P((DB_ENV *, DB_LOGC **, u_int32_t));
	int  (*log_file) __P((DB_ENV *, const DB_LSN *, char *, size_t));
	int  (*log_flush) __P((DB_ENV *, const DB_LSN *));
	int  (*log_put) __P((DB_ENV *, DB_LSN *, const DBT *, u_int32_t));
	int  (*log_stat) __P((DB_ENV *, DB_LOG_STAT **, u_int32_t));

	void *lk_handle;		/* Lock handle and methods. */
	int  (*get_lk_conflicts) __P((DB_ENV *, const u_int8_t **, int *));
	int  (*set_lk_conflicts) __P((DB_ENV *, u_int8_t *, int));
	int  (*get_lk_detect) __P((DB_ENV *, u_int32_t *));
	int  (*set_lk_detect) __P((DB_ENV *, u_int32_t));
	int  (*set_lk_max) __P((DB_ENV *, u_int32_t));
	int  (*get_lk_max_locks) __P((DB_ENV *, u_int32_t *));
	int  (*set_lk_max_locks) __P((DB_ENV *, u_int32_t));
	int  (*get_lk_max_lockers) __P((DB_ENV *, u_int32_t *));
	int  (*set_lk_max_lockers) __P((DB_ENV *, u_int32_t));
	int  (*get_lk_max_objects) __P((DB_ENV *, u_int32_t *));
	int  (*set_lk_max_objects) __P((DB_ENV *, u_int32_t));
	int  (*lock_detect) __P((DB_ENV *, u_int32_t, u_int32_t, int *));
	int  (*lock_dump_region) __P((DB_ENV *, const char *, FILE *));
	int  (*lock_abort_waiters)__P((DB_ENV *, u_int32_t, u_int32_t));
	int  (*lock_get) __P((DB_ENV *,
		u_int32_t, u_int32_t, const DBT *, db_lockmode_t, DB_LOCK *));
	int  (*lock_query) __P((DB_ENV *, u_int32_t, const DBT *, db_lockmode_t));
	int  (*lock_put) __P((DB_ENV *, DB_LOCK *));
	int  (*lock_id) __P((DB_ENV *, u_int32_t *));
	int  (*lock_id_flags) __P((DB_ENV *, u_int32_t *, u_int32_t));
	int  (*lock_id_free) __P((DB_ENV *, u_int32_t));
	int  (*lock_id_has_waiters) __P((DB_ENV *, u_int32_t));
	int  (*lock_id_set_logical_abort) __P((DB_ENV *, u_int32_t));
	int  (*lock_stat) __P((DB_ENV *, DB_LOCK_STAT **, u_int32_t));
	int  (*collect_locks) __P((DB_ENV *, collect_locks_f, void *arg));
	int  (*collect_prepared) __P((DB_ENV *, collect_prepared_f, void *arg));
	int  (*lock_locker_lockcount)
		__P((DB_ENV *, u_int32_t id, u_int32_t *nlocks));
	int  (*lock_locker_pagelockcount)
		__P((DB_ENV *, u_int32_t id, u_int32_t *nlocks));
	int  (*lock_vec) __P((DB_ENV *,
		u_int32_t, u_int32_t, DB_LOCKREQ *, int, DB_LOCKREQ **));
	int  (*lock_to_dbt) __P((DB_ENV *,DB_LOCK *, DBT *));
	int  (*lock_add_child_locker) __P((DB_ENV *, u_int32_t, u_int32_t));
	int  (*lock_update_tracked_writelocks_lsn)
		__P((DB_ENV *, DB_TXN *, u_int32_t, DB_LSN));
	int  (*lock_clear_tracked_writelocks) __P((DB_ENV *, u_int32_t));
	void *mp_handle;		/* Mpool handle and methods. */
	int  (*get_cachesize) __P((DB_ENV *, u_int32_t *, u_int32_t *, int *));
	int  (*set_cachesize) __P((DB_ENV *, u_int32_t, u_int32_t, int));
	int  (*get_mp_mmapsize) __P((DB_ENV *, size_t *));
	int  (*set_mp_mmapsize) __P((DB_ENV *, size_t));
	int  (*get_mp_multiple) __P((DB_ENV *, double *));
	int  (*set_mp_multiple) __P((DB_ENV *, double));
	int  (*get_mp_maxwrite) __P((DB_ENV *, int *, int *));
	int  (*set_mp_maxwrite) __P((DB_ENV *, int, int));
	int  (*get_mp_recovery_pages) __P((DB_ENV *, int *));
	int  (*set_mp_recovery_pages) __P((DB_ENV *, int));
	int  (*memp_dump_region) __P((DB_ENV *, const char *, FILE *));
	int  (*memp_fcreate) __P((DB_ENV *, DB_MPOOLFILE **, u_int32_t));
	int  (*memp_register) __P((DB_ENV *, int,
		int (*)(DB_ENV *, db_pgno_t, void *, DBT *),
		int (*)(DB_ENV *, db_pgno_t, void *, DBT *)));
	int  (*memp_stat) __P((DB_ENV *,
		DB_MPOOL_STAT **, DB_MPOOL_FSTAT ***, u_int32_t));
	int  (*memp_sync) __P((DB_ENV *, DB_LSN *));
	int  (*memp_dump) __P((DB_ENV *, SBUF2 *, u_int64_t maxpages));
	int  (*memp_load) __P((DB_ENV *, SBUF2 *));
	int  (*memp_dump_default) __P((DB_ENV *, u_int32_t));
	int  (*memp_load_default) __P((DB_ENV *));
	int  (*memp_trickle) __P((DB_ENV *, int, int *, int));

	void *rep_handle;		/* Replication handle and methods. */
	int  (*rep_elect) __P((DB_ENV *, int, int, u_int32_t, u_int32_t *, int *, char **));
	int  (*rep_flush) __P((DB_ENV *));
	int  (*rep_process_message) __P((DB_ENV *, DBT *, DBT *,
		char **, DB_LSN *, uint32_t *, int));
	int  (*rep_verify_will_recover) __P((DB_ENV *, DBT *, DBT *));
	int  (*rep_truncate_repdb) __P((DB_ENV *));
	int  (*rep_start) __P((DB_ENV *, DBT *, u_int32_t, u_int32_t));
	int  (*rep_stat) __P((DB_ENV *, DB_REP_STAT **, u_int32_t));
	int  (*rep_deadlocks) __P((DB_ENV *, u_int64_t *));
	int  (*set_logical_start) __P((DB_ENV *, int (*) (DB_ENV *, void *,
		u_int64_t, DB_LSN *)));
	int  (*set_logical_commit) __P((DB_ENV *, int (*) (DB_ENV *, void *,
		u_int64_t, DB_LSN *)));
	int  (*get_rep_limit) __P((DB_ENV *, u_int32_t *, u_int32_t *));
	int  (*set_rep_limit) __P((DB_ENV *, u_int32_t, u_int32_t));
	void  (*get_rep_gen) __P((DB_ENV *, u_int32_t *));
	void  (*get_rep_log_gen) __P((DB_ENV *, u_int32_t *));
	int  (*get_last_locked) __P((DB_ENV *, DB_LSN *));
	int  (*set_rep_request) __P((DB_ENV *, u_int32_t, u_int32_t));
	int  (*set_rep_transport) __P((DB_ENV *, char*,
		int (*) (DB_ENV *, const DBT *, const DBT *, const DB_LSN *,
		char*, int, void *)));
	int  (*set_rep_db_pagesize) __P((DB_ENV *, int));
	int  (*get_rep_db_pagesize) __P((DB_ENV *, int *));
	int  (*set_rep_ignore) __P((DB_ENV *, int (*func)(const char *filename)));
	void *tx_handle;		/* Txn handle and methods. */
	int  (*get_tx_max) __P((DB_ENV *, u_int32_t *));
	int  (*set_tx_max) __P((DB_ENV *, u_int32_t));
	int  (*get_tx_timestamp) __P((DB_ENV *, time_t *));
	int  (*set_tx_timestamp) __P((DB_ENV *, time_t *));
	int  (*debug_log) __P((DB_ENV *, DB_TXN *, const DBT *op, const DBT *key, const DBT *data));
	int  (*txn_begin) __P((DB_ENV *, DB_TXN *, DB_TXN **, u_int32_t));
	int  (*txn_assert_notran) __P((DB_ENV *));
	int  (*txn_checkpoint) __P((DB_ENV *, u_int32_t, u_int32_t, u_int32_t));
	int  (*txn_recover) __P((DB_ENV *,
		DB_PREPLIST *, long, long *, u_int32_t));
	int  (*txn_stat) __P((DB_ENV *, DB_TXN_STAT **, u_int32_t));
	int  (*txn_commit_recovered) __P((DB_ENV *, const char *));
	int  (*txn_abort_recovered) __P((DB_ENV *, const char *));
	int  (*txn_discard_recovered) __P((DB_ENV *, const char *));
	int  (*txn_discard_all_recovered) __P((DB_ENV *));
	int  (*txn_upgrade_all_prepared) __P((DB_ENV *));
	int  (*txn_abort_prepared_waiters) __P((DB_ENV *));
	int  (*get_timeout) __P((DB_ENV *, db_timeout_t *, u_int32_t));
	int  (*set_timeout) __P((DB_ENV *, db_timeout_t, u_int32_t));
	int  (*set_bulk_stops_on_page) __P((DB_ENV*, int));
	int  (*memp_dump_bufferpool_info) __P((DB_ENV *, FILE *));
	int  (*get_rep_master) __P((DB_ENV *, char **, u_int32_t *, u_int32_t *));
	int  (*get_rep_eid) __P((DB_ENV *, char **));
	void (*txn_dump_ltrans) __P((DB_ENV *, FILE *, u_int32_t));
	int  (*lowest_logical_lsn) __P((DB_ENV *, DB_LSN *));
	int  (*ltran_count) __P((DB_ENV *, u_int32_t *));
	int  (*get_ltran_list) __P((DB_ENV *, DB_LTRAN **, u_int32_t *));

#define	DB_TEST_ELECTINIT	 1	/* after __rep_elect_init */
#define	DB_TEST_POSTDESTROY	 2	/* after destroy op */
#define	DB_TEST_POSTLOG		 3	/* after logging all pages */
#define	DB_TEST_POSTLOGMETA	 4	/* after logging meta in btree */
#define	DB_TEST_POSTOPEN	 5	/* after __os_open */
#define	DB_TEST_POSTSYNC	 6	/* after syncing the log */
#define	DB_TEST_PREDESTROY	 7	/* before destroy op */
#define	DB_TEST_PREOPEN		 8	/* before __os_open */
#define	DB_TEST_SUBDB_LOCKS	 9	/* subdb locking tests */
	int		 test_abort;	/* Abort value for testing. */
	int		 test_copy;	/* Copy value for testing. */

#define	DB_ENV_AUTO_COMMIT	0x0000001 /* DB_AUTO_COMMIT. */
#define	DB_ENV_CDB		0x0000002 /* DB_INIT_CDB. */
#define	DB_ENV_CDB_ALLDB	0x0000004 /* CDB environment wide locking. */
#define	DB_ENV_CREATE		0x0000008 /* DB_CREATE set. */
#define	DB_ENV_DBLOCAL		0x0000010 /* DB_ENV allocated for private DB. */
#define	DB_ENV_DIRECT_DB	0x0000020 /* DB_DIRECT_DB set. */
#define	DB_ENV_DIRECT_LOG	0x0000040 /* DB_DIRECT_LOG set. */
#define	DB_ENV_FATAL		0x0000080 /* Doing fatal recovery in env. */
#define	DB_ENV_LOCKDOWN		0x0000100 /* DB_LOCKDOWN set. */
#define	DB_ENV_LOG_AUTOREMOVE   0x0000200 /* DB_LOG_AUTOREMOVE set. */
#define	DB_ENV_NOLOCKING	0x0000400 /* DB_NOLOCKING set. */
#define	DB_ENV_NOMMAP		0x0000800 /* DB_NOMMAP set. */
#define	DB_ENV_NOPANIC		0x0001000 /* Okay if panic set. */
#define	DB_ENV_OPEN_CALLED	0x0002000 /* DB_ENV->open called. */
#define	DB_ENV_OVERWRITE	0x0004000 /* DB_OVERWRITE set. */
#define	DB_ENV_PRIVATE		0x0008000 /* DB_PRIVATE set. */
#define	DB_ENV_REGION_INIT	0x0010000 /* DB_REGION_INIT set. */
#define	DB_ENV_RPCCLIENT	0x0020000 /* DB_RPCCLIENT set. */
#define	DB_ENV_RPCCLIENT_GIVEN	0x0040000 /* User-supplied RPC client struct */
#define	DB_ENV_SYSTEM_MEM	0x0080000 /* DB_SYSTEM_MEM set. */
#define	DB_ENV_THREAD		0x0100000 /* DB_THREAD set. */
#define	DB_ENV_TIME_NOTGRANTED	0x0200000 /* DB_TIME_NOTGRANTED set. */
#define	DB_ENV_TXN_NOSYNC	0x0400000 /* DB_TXN_NOSYNC set. */
#define	DB_ENV_TXN_NOT_DURABLE	0x0800000 /* DB_TXN_NOT_DURABLE set. */
#define	DB_ENV_TXN_WRITE_NOSYNC	0x1000000 /* DB_TXN_WRITE_NOSYNC set. */
#define	DB_ENV_YIELDCPU		0x2000000 /* DB_YIELDCPU set. */
#define	DB_ENV_ROWLOCKS		0x4000000 /* DB_ROWLOCKS set. */
#define	DB_ENV_OSYNC		0x8000000 /* DB_OSYNC */
	u_int32_t flags;
	u_int32_t close_flags;

	LISTC_T(DB) *dbs;
	int maxdb;
	hash_t *fidhash;

	LISTC_T(HEAP) regions;
	int bulk_stops_on_page;

	void (*lsn_undone_callback)(DB_ENV *env, DB_LSN*);

	int  (*txn_begin_with_prop)
		__P((DB_ENV *, DB_TXN *, DB_TXN **, u_int32_t, struct txn_properties*));

	int  (*set_num_recovery_processor_threads)
		__P((DB_ENV *env, int nthreads));
	int  (*set_num_recovery_worker_threads)
		__P((DB_ENV *env, int nthreads));
	void  (*set_recovery_memsize) __P((DB_ENV *env, int sz));
	int  (*get_recovery_memsize) __P((DB_ENV *env));
	int (*get_page_extent_size) __P((DB_ENV *env));
	void (*set_page_extent_size) __P((DB_ENV *env, u_int32_t npages));
	int (*setattr) __P((DB_ENV *env, char *attr, char *val, int ival));
	int (*getattr) __P((DB_ENV *env, char *attr, char **val, int *ival));
	int (*dumpattrs) __P((DB_ENV *env, FILE *out));

	DB_FH *checkpoint;

	/* ltran structs for ltran->lid map & parallel rep coordination */
	hash_t *ltrans_hash;
	LISTC_T(LTDESC) active_ltrans;
	LISTC_T(LTDESC) inactive_ltrans;
	pthread_mutex_t ltrans_hash_lk;
	pthread_mutex_t ltrans_inactive_lk;
	pthread_mutex_t ltrans_active_lk;

	/* ufid to dbp hash */
	hash_t *ufid_to_db_hash;
	pthread_mutex_t ufid_to_db_lk;

	/* prepared transactions */
	hash_t *prepared_txn_hash;
	hash_t *prepared_utxnid_hash;
	hash_t *prepared_children;
	pthread_mutex_t prepared_txn_lk;

	/* Parallel recovery.  These are only valid on replicants. */
	DB_LSN prev_commit_lsn;
	int num_recovery_processor_threads;
	int num_recovery_worker_threads;
	struct thdpool *recovery_processors;
	struct thdpool *recovery_workers;

	LISTC_T(struct __recovery_processor) inflight_transactions;
	LISTC_T(struct __recovery_processor) inactive_transactions;
	pthread_mutex_t recover_lk;
	pthread_cond_t recover_cond;
	int recovery_memsize;  /* Use up to this much memory for log records */
	pthread_mutex_t ser_lk;
	pthread_cond_t ser_cond;
	int ser_count;
	int lsn_chain;

	/* overrides for minwrite deadlock */
	int (*set_deadlock_override) __P((DB_ENV *, u_int32_t));
	int master_use_minwrite_ever;
	int replicant_use_minwrite_noread;
	int page_extent_size;

	comdb2_dirs_type comdb2_dirs;
	int  (*set_comdb2_dirs) __P((DB_ENV *, char *, char *, char *));

	DBENV_ATTR attr;

	DB_FH* iomap_fd;
	DBENV_MAP *iomap;

	/* These fields are for changes to recovery code. */ 
	struct fileid_track fileid_track;
	pthread_mutex_t mintruncate_lk;
	int mintruncate_state;
	DB_LSN mintruncate_first;
	LISTC_T(struct mintruncate_entry) mintruncate;
	DB_LSN last_mintruncate_dbreg_start;
	DB_LSN last_mintruncate_ckp;
	DB_LSN last_mintruncate_ckplsn;
	db_recops recovery_pass;
	pthread_rwlock_t dbreglk;
	pthread_rwlock_t recoverlk;
	DB_LSN recovery_start_lsn;
	int (*get_recovery_lsn) __P((DB_ENV*, DB_LSN*));
	int (*set_recovery_lsn) __P((DB_ENV*, DB_LSN*));

	void (*get_rep_verify_lsn) __P((DB_ENV*, DB_LSN*, DB_LSN*));

	time_t newest_rep_verify_tran_time;
	DB_LSN rep_verify_start_lsn;
	DB_LSN rep_verify_current_lsn;

	int (**pgnos_dtab)	/* Dispatch table for recover funcs. */
		__P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
	size_t		 pgnos_dtab_size;

	LC_CACHE lc_cache;

	int is_tmp_tbl;
	int  (*set_is_tmp_tbl) __P((DB_ENV *, int));

	int use_sys_malloc;
	int  (*set_use_sys_malloc) __P((DB_ENV *, int));

	/* keep a copy here to enhance locality */
	unsigned int bmaszthresh;
	comdb2bma bma;
	int  (*blobmem_yield) __P((DB_ENV *));

	/* Stable LSN: must be acked by majority of cluster. */
	DB_LSN durable_lsn;
	uint32_t durable_generation;
    uint32_t rep_gen;

	void (*set_durable_lsn) __P((DB_ENV *, DB_LSN *, uint32_t));
	void (*get_durable_lsn) __P((DB_ENV *, DB_LSN *, uint32_t *));
    int (*replicant_generation) __P((DB_ENV *, uint32_t *));
	int (*set_check_standalone) __P((DB_ENV *, int (*)(DB_ENV *)));
	int (*check_standalone)(DB_ENV *);
	int (*set_truncate_sc_callback) __P((DB_ENV *, int (*)(DB_ENV *, DB_LSN *lsn)));
	int (*truncate_sc_callback)(DB_ENV *, DB_LSN *lsn);
	int (*set_rep_truncate_callback) __P((DB_ENV *, int (*)(DB_ENV *, DB_LSN *lsn, uint32_t flags)));
	int (*rep_truncate_callback)(DB_ENV *, DB_LSN *lsn, uint32_t flags);
	void (*rep_set_gen)(DB_ENV *, uint32_t gen);
	int (*set_rep_recovery_cleanup) __P((DB_ENV *, int (*)(DB_ENV *, DB_LSN *lsn, int is_master)));
	int (*rep_recovery_cleanup)(DB_ENV *, DB_LSN *lsn, int is_master);
	int (*wrlock_recovery_lock)(DB_ENV *, const char *func, int line);
    int (*wrlock_recovery_blocked)(DB_ENV *);
	int (*lock_recovery_lock)(DB_ENV *, const char *func, int line);
	int (*unlock_recovery_lock)(DB_ENV *, const char *func, int line);
	/* Trigger/consumer signalling support */
	int(*trigger_subscribe) __P((DB_ENV *, const char *, pthread_cond_t **,
					 pthread_mutex_t **, const uint8_t **));
	int(*trigger_unsubscribe) __P((DB_ENV *, const char *));
	int(*trigger_open) __P((DB_ENV *, const char *));
	int(*trigger_close) __P((DB_ENV *, const char *));
	int(*trigger_ispaused) __P((DB_ENV *, const char *));
	int(*trigger_pause) __P((DB_ENV *, const char *));
	int(*trigger_unpause) __P((DB_ENV *, const char *));

	int (*pgin[DB_TYPE_MAX]) __P((DB_ENV *, db_pgno_t, void *, DBT *));
	int (*pgout[DB_TYPE_MAX]) __P((DB_ENV *, db_pgno_t, void *, DBT *));

	pthread_mutex_t utxnid_lock;
	u_int64_t next_utxnid;

	DB_TXN_COMMIT_MAP* txmap;
};

struct __utxnid {
	u_int64_t utxnid;
	LINKC_T(struct __utxnid) lnk;
};

struct __logfile_txn_list {
	u_int32_t file_num;
	LISTC_T(struct __utxnid) commit_utxnids;
};

struct __utxnid_track {
	u_int64_t utxnid;
	DB_LSN commit_lsn;
};

struct __txn_commit_map {
	pthread_mutex_t txmap_mutexp;
	hash_t *transactions;
	hash_t *logfile_lists;
};

#ifndef DB_DBM_HSEARCH
#define	DB_DBM_HSEARCH	0		/* No historic interfaces by default. */
#endif
#if DB_DBM_HSEARCH != 0
/*******************************************************
 * Dbm/Ndbm historic interfaces.
 *******************************************************/
typedef struct __db DBM;

#define	DBM_INSERT	0		/* Flags to dbm_store(). */
#define	DBM_REPLACE	1

/*
 * The DB support for ndbm(3) always appends this suffix to the
 * file name to avoid overwriting the user's original database.
 */
#define	DBM_SUFFIX	".db"

#if defined(_XPG4_2)
typedef struct {
	char *dptr;
	size_t dsize;
} datum;
#else
typedef struct {
	char *dptr;
	int dsize;
} datum;
#endif

/*
 * Translate NDBM calls into DB calls so that DB doesn't step on the
 * application's name space.
 */
#define	dbm_clearerr(a)		__db_ndbm_clearerr(a)
#define	dbm_close(a)		__db_ndbm_close(a)
#define	dbm_delete(a, b)	__db_ndbm_delete(a, b)
#define	dbm_dirfno(a)		__db_ndbm_dirfno(a)
#define	dbm_error(a)		__db_ndbm_error(a)
#define	dbm_fetch(a, b)		__db_ndbm_fetch(a, b)
#define	dbm_firstkey(a)		__db_ndbm_firstkey(a)
#define	dbm_nextkey(a)		__db_ndbm_nextkey(a)
#define	dbm_open(a, b, c)	__db_ndbm_open(a, b, c)
#define	dbm_pagfno(a)		__db_ndbm_pagfno(a)
#define	dbm_rdonly(a)		__db_ndbm_rdonly(a)
#define	dbm_store(a, b, c, d) \
	__db_ndbm_store(a, b, c, d)

/*
 * Translate DBM calls into DB calls so that DB doesn't step on the
 * application's name space.
 *
 * The global variables dbrdonly, dirf and pagf were not retained when 4BSD
 * replaced the dbm interface with ndbm, and are not supported here.
 */
#define	dbminit(a)	__db_dbm_init(a)
#define	dbmclose	__db_dbm_close
#if !defined(__cplusplus)
#define	delete(a)	__db_dbm_delete(a)
#endif
#define	fetch(a)	__db_dbm_fetch(a)
#define	firstkey	__db_dbm_firstkey
#define	nextkey(a)	__db_dbm_nextkey(a)
#define	store(a, b)	__db_dbm_store(a, b)

/*******************************************************
 * Hsearch historic interface.
 *******************************************************/
typedef enum {
	FIND, ENTER
} ACTION;

typedef struct entry {
	char *key;
	char *data;
} ENTRY;

#define	hcreate(a)	__db_hcreate(a)
#define	hdestroy	__db_hdestroy
#define	hsearch(a, b)	__db_hsearch(a, b)

#endif /* DB_DBM_HSEARCH */

#if defined(__cplusplus)
}
#endif
#endif /* !_DB_H_ */

/* DO NOT EDIT: automatically built by dist/s_rpc. */
#define	DB_RPC_SERVERPROG ((unsigned long)(351457))
#define	DB_RPC_SERVERVERS ((unsigned long)(4002))

/* DO NOT EDIT: automatically built by dist/s_include. */
#ifndef	_DB_EXT_PROT_IN_
#define	_DB_EXT_PROT_IN_

#if defined(__cplusplus)
extern "C" {
#endif

int db_create __P((DB **, DB_ENV *, u_int32_t));
char *db_strerror __P((int));
int db_env_create __P((DB_ENV **, u_int32_t));
char *db_version __P((int *, int *, int *));
int log_compare __P((const DB_LSN *, const DB_LSN *));
int db_env_set_func_close __P((int (*)(int)));
int db_env_set_func_dirfree __P((void (*)(char **, int)));
int db_env_set_func_dirlist __P((int (*)(const char *, char ***, int *)));
int db_env_set_func_exists __P((int (*)(const char *, int *)));
int db_env_set_func_free __P((void (*)(void *)));
int db_env_set_func_fsync __P((int (*)(int)));
int db_env_set_func_ioinfo __P((int (*)(const char *, int, u_int32_t *, u_int32_t *, u_int32_t *)));
int db_env_set_func_malloc __P((void *(*)(size_t)));
int db_env_set_func_map __P((int (*)(char *, size_t, int, int, void **)));
int db_env_set_func_open __P((int (*)(const char *, int, ...)));
int db_env_set_func_read __P((ssize_t (*)(int, void *, size_t)));
int db_env_set_func_realloc __P((void *(*)(void *, size_t)));
int db_env_set_func_rename __P((int (*)(const char *, const char *)));
int db_env_set_func_seek __P((int (*)(int, size_t, db_pgno_t, u_int32_t, int, int)));
int db_env_set_func_sleep __P((int (*)(u_long, u_long)));
int db_env_set_func_unlink __P((int (*)(const char *)));
int db_env_set_func_unmap __P((int (*)(void *, size_t)));
int db_env_set_func_write __P((ssize_t (*)(int, const void *, size_t)));
int db_env_set_func_yield __P((int (*)(void)));
#if DB_DBM_HSEARCH != 0
int	 __db_ndbm_clearerr __P((DBM *));
void	 __db_ndbm_close __P((DBM *));
int	 __db_ndbm_delete __P((DBM *, datum));
int	 __db_ndbm_dirfno __P((DBM *));
int	 __db_ndbm_error __P((DBM *));
datum __db_ndbm_fetch __P((DBM *, datum));
datum __db_ndbm_firstkey __P((DBM *));
datum __db_ndbm_nextkey __P((DBM *));
DBM	*__db_ndbm_open __P((const char *, int, int));
int	 __db_ndbm_pagfno __P((DBM *));
int	 __db_ndbm_rdonly __P((DBM *));
int	 __db_ndbm_store __P((DBM *, datum, datum, int));
int	 __db_dbm_close __P((void));
int	 __db_dbm_dbrdonly __P((void));
int	 __db_dbm_delete __P((datum));
int	 __db_dbm_dirf __P((void));
datum __db_dbm_fetch __P((datum));
datum __db_dbm_firstkey __P((void));
int	 __db_dbm_init __P((char *));
datum __db_dbm_nextkey __P((datum));
int	 __db_dbm_pagf __P((void));
int	 __db_dbm_store __P((datum, datum));
#endif
#if DB_DBM_HSEARCH != 0
int __db_hcreate __P((size_t));
ENTRY *__db_hsearch __P((ENTRY, ACTION));
void __db_hdestroy __P((void));
#endif


/*
 * Helper macros for microsecond-granularity event logging.
 * Multiplication usually takes fewer CPU cycles than division. Therefore
 * when comparing a usec and a msec, it is preferable to use:
 * usec <comparison operator> M2U(msec)
 */
#ifndef U2M
#define U2M(usec) (int)((usec) / 1000)
#endif
#ifndef M2U
#define M2U(msec) ((msec) * 1000ULL)
#endif
uint64_t bb_berkdb_fasttime(void);
struct berkdb_thread_stats *bb_berkdb_get_thread_stats(void);
struct berkdb_thread_stats *bb_berkdb_get_process_stats(void);
void bb_berkdb_thread_stats_init(void);
void bb_berkdb_thread_stats_reset(void);

extern int gbl_bb_berkdb_enable_thread_stats;
extern int gbl_bb_berkdb_enable_lock_timing;
extern int gbl_bb_berkdb_enable_memp_timing;
extern int gbl_bb_berkdb_enable_memp_pg_timing;
extern int gbl_bb_berkdb_enable_shalloc_timing;

struct berkdb_deadlock_info {
	u_int32_t lid;
};

struct __heap {
	void *mem;
	char *description;
	size_t size;
	int used;
	int blocks;
	int blocksz[32];
	DB_MUTEX *lock;
	comdb2ma msp;
	LINKC_T(HEAP) lnk;

};

void berkdb_register_deadlock_callback(void (*callback) (struct berkdb_deadlock_info *lkinfo));

/* control deadlock policy at runtime */
void berkdb_set_max_rep_retries(int max);
int berkdb_get_max_rep_retries();

/* COMDB2 MODIFICATION */
int berkdb_is_recovering(DB_ENV *dbenv);

#define TIMEIT(x)			   \
do {							\
	int start, end, diff;	   \
	start = comdb2_time_epochms(); \
	x						   \
	end = comdb2_time_epochms();\
	diff = end - start;		 \
	if (diff > 100)			 \
		printf(">> %d %dms\n", __LINE__, diff); \
} while(0)

#define TIMEITX(x, y)		   \
do {							\
	int start, end, diff;	   \
	start = comdb2_time_epochms(); \
	x						   \
	end = comdb2_time_epochms();\
	diff = end - start;		 \
	if (diff > 100) {		   \
		printf(">> %d %dms\n", __LINE__, diff); \
		y					   \
	}						   \
} while(0)

struct __db_checkpoint {
	u_int8_t chksum[20];

	DB_LSN lsn;

	/* if expanding this structure, shrink this.
	 * sizeof(__db_checkpoint) should be 512 */
	u_int8_t pad[482];
};

struct logrecord {
	DB_LSN lsn;
	DBT rec;
};

struct __recovery_record {
	DBT logdbt;	/* log record to apply */
	DB_LSN lsn;	/* LSN of log record to apply */
	int fileid;
	LINKC_T(struct __recovery_record) lnk;
};

struct __recovery_queue {
	/* processor making the request */
	struct __recovery_processor *processor;
	int fileid;
	int used;
	LISTC_T(struct __recovery_record) records;
	LINKC_T(struct __recovery_queue) lnk;
};

struct __recovery_processor {
	DB_LSN commit_lsn;
	DB_LSN prev_commit_lsn;
	DB_ENV *dbenv;
	pthread_mutex_t lk;
	pthread_cond_t  wait;
	int num_busy_workers;
	int num_fileids;
	int has_logical_commit;
	int has_schema_lock;
	u_int32_t child;
	unsigned long long context;
	u_int32_t lockid;
	struct __recovery_queue **recovery_queues;
	void *txninfo;
	LSN_COLLECTION lc;
	pool_t *recpool;
	LTDESC *ltrans;
	LINKC_T(struct __recovery_processor) lnk;
	comdb2ma msp;
	int mspsize;
};

struct __rowlock_list {
	DB_LSN lsn;
	u_int32_t lockcnt;
	DB_LOCK *locks;
	LINKC_T(struct __rowlock_list) lnk;
};

#ifdef LTRANS_DEBUG
struct track_stack_info {
	void *stack[100];
	u_int32_t nframes;
	u_int32_t time;
	pthread_t tid;
	int ret;
};
#endif

/* Map logical tranids to locker_ids */
struct __ltrans_descriptor {
	u_int64_t ltranid;
	u_int32_t active_txn_count;
	u_int32_t lockcnt;
	DB_LSN begin_lsn;
	DB_LSN last_lsn;
	pthread_mutex_t lk;
	pthread_cond_t wait;
#define	TXN_LTRANS_WASCOMMITTED	0x00000002
	u_int32_t flags;
	LISTC_T (struct __rowlock_list) locklist;
	LINKC_T(LTDESC) lnk;
#ifdef LTRANS_DEBUG
	struct track_stack_info allocate_info;
	struct track_stack_info add_locklist_info;
#endif
};

int __checkpoint_open(DB_ENV *dbenv, const char *db_home);
int __page_cache_set_path(DB_ENV *dbenv, const char *db_home);
int __checkpoint_get(DB_ENV *dbenv, DB_LSN *lsnout);
int __checkpoint_get_recovery_lsn(DB_ENV *dbenv, DB_LSN *lsnout);
int __checkpoint_save(DB_ENV *dbenv, DB_LSN *lsn, int in_recovery);
int __checkpoint_ok_to_delete_log(DB_ENV *dbenv, int logfile);
int berkdb_verify_lsn_written_to_disk(DB_ENV *dbenv, DB_LSN *lsn,
	int check_checkpoint);

int ufid_for_recovery_record(DB_ENV *env, DB_LSN *lsn,
	int rectype, u_int8_t *ufid, DBT *dbt, int utxnid_logged);

int __rep_get_master(DB_ENV *dbenv, char **master, u_int32_t *gen, u_int32_t *egen);
int __rep_get_eid(DB_ENV *dbenv,char **eid);

unsigned int __berkdb_count_freepages(int fd);
void __berkdb_count_freeepages_abort(void);

int get_committed_lsns(DB_ENV *dbenv, DB_LSN **lsns, int *n_lsns,
	int epoch, int file, int offset);

int get_lsn_context_from_timestamp(DB_ENV *dbenv, int32_t timestamp,
	DB_LSN *ret_lsn, unsigned long long *ret_context);
int get_context_from_lsn(DB_ENV *dbenv, DB_LSN lsn,
	unsigned long long *ret_context);

void __log_txn_lsn(DB_ENV *, DB_LSN *, u_int32_t *, u_int32_t *);

int __recover_logfile_pglogs(DB_ENV *, void *);
int normalize_rectype(u_int32_t* rectype);

//#################################### THREAD POOL FOR LOADING PAGES ASYNCHRNOUSLY (WELL NO CALLBACK YET.....) 

struct string_ref;
int thdpool_enqueue(struct thdpool *pool, thdpool_work_fn work_fn,
	void *work, int queue_override, struct string_ref *persistent_info,
	uint32_t flags);


typedef struct {
	DB_MPOOLFILE *mpf;
	db_pgno_t pgno;
} touch_pg;

int enqueue_touch_page(DB_MPOOLFILE *mpf, db_pgno_t pgno);
void touch_page(DB_MPOOLFILE *mpf, db_pgno_t pgno);

//#############################################
#if defined(__cplusplus)
}
#endif
#endif				/* !_DB_EXT_PROT_IN_ */

#endif
