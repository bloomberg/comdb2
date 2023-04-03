/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 *
 * $Id: db_am.h,v 11.70 2003/06/30 17:19:50 bostic Exp $
 */
#ifndef _DB_AM_H_
#define	_DB_AM_H_

/*
 * IS_AUTO_COMMIT --
 *	Test for local auto-commit flag or global flag with no local DbTxn
 *	handle.
 */
#define	IS_AUTO_COMMIT(dbenv, txn, flags)				\
	(LF_ISSET(DB_AUTO_COMMIT) ||					\
	    ((txn) == NULL && F_ISSET((dbenv), DB_ENV_AUTO_COMMIT) &&	\
	    !LF_ISSET(DB_NO_AUTO_COMMIT)))

/* DB recovery operation codes. */
#define	DB_ADD_DUP	1
#define	DB_REM_DUP	2
#define	DB_ADD_BIG	3
#define	DB_REM_BIG	4
#define	DB_ADD_PAGE	5
#define	DB_REM_PAGE	6
#define	DB_REM_COMPR	7 /* For REM on a keycompr page */

/* u_int32_t opcode; */
#define GET_ADDREM_OPCODE(x) ((x) & 0x00ffffff)
#define GET_PFX_FLG(opcode) (((opcode) & 0xff000000) >> 24)
#define PUT_PFX_FLG(opcode, flg) ((opcode) | ((flg) << 24))
#define IS_REM_OPCODE(x) (GET_ADDREM_OPCODE(x) == DB_REM_DUP || GET_ADDREM_OPCODE(x) == DB_REM_COMPR)

#if defined (DEBUG_ABORT_ON_RECOVERY_FAILURE)
#define CHECK_ABORT do { __log_flush(dbenv, NULL); abort(); } while(0);
#else
#define CHECK_ABORT 
#endif
/*
 * Standard initialization and shutdown macros for all recovery functions.
 */
#define	REC_INTRO(func, inc_count) do {					\
	extern int __log_flush(DB_ENV *dbenv, const DB_LSN *); \
	argp = NULL;							\
	dbc = NULL;							\
	file_dbp = NULL;						\
	mpf = NULL;							\
	if ((ret = func(dbenv, dbtp->data, &argp)) != 0) {		\
		CHECK_ABORT \
		goto out;						\
	}								   \
	if (argp->type > 3000 || (argp->type > 1000 && argp->type < 2000)) { \
		if ((ret = __ufid_to_db(dbenv, argp->txnid, &file_dbp, \
						argp->ufid_fileid, lsnp)) != 0) { \
			if (ret	== DB_DELETED || ret == DB_IGNORED) { \
				ret = 0; \
				goto done; \
			} \
			__ufid_dump(dbenv); \
			fflush(stderr); \
			CHECK_ABORT				 \
			goto out;						\
		} \
	} else { \
		if ((ret = __dbreg_id_to_db(dbenv, argp->txnid,			\
			&file_dbp, argp->fileid, inc_count, lsnp, 0)) != 0) { 		\
			if (ret	== DB_DELETED) {\
				ret = 0;\
				goto done;\
			}\
			CHECK_ABORT				 \
			goto out;\
		} \
	} \
	if ((ret = __db_cursor(file_dbp, NULL, &dbc, 0)) != 0) {	\
        CHECK_ABORT                     \
		goto out;						\
	}										   \
	F_SET(dbc, DBC_RECOVER);					\
	mpf = file_dbp->mpf;						\
} while (0)


extern void __bb_dbreg_print_dblist_stdout(DB_ENV *dbenv);


	 /* panic the db if we are told to process an op related to a
	  * deleted file, and we are not in the middle of recovery */

int __log_flush(DB_ENV *dbenv, const DB_LSN *);
#define	REC_INTRO_PANIC(func, inc_count) do {				\
	argp = NULL;							\
	dbc = NULL;							\
	file_dbp = NULL;						\
	mpf = NULL;							\
	if ((ret = func(dbenv, dbtp->data, &argp)) != 0) {		\
		__log_flush(dbenv, NULL); 				\
	}								\
	if ((argp->type > 1000 && argp->type < 2000) || (argp->type > 3000)) {					\
		ret = __ufid_to_db(dbenv, argp->txnid, &file_dbp,	\
			argp->ufid_fileid, lsnp);			\
	}								\
	else {								\
		ret = __dbreg_id_to_db(dbenv, argp->txnid,		\
			&file_dbp, argp->fileid, inc_count, lsnp, 0);	\
	} 								\
	if (ret) { 							\
		if (ret == DB_IGNORED || (ret == DB_DELETED && IS_RECOVERING(dbenv))) {	\
			ret = 0;					\
			goto done;					\
		}							\
		__bb_dbreg_print_dblist_stdout(dbenv);			\
		__ufid_dump(dbenv);					\
		__log_flush(dbenv, NULL);				\
		__db_panic(dbenv, DB_RUNRECOVERY);			\
		abort();						\
	}								\
	if ((ret = __db_cursor(file_dbp, NULL, &dbc, 0)) != 0) {	\
		__log_flush(dbenv, NULL);				\
		abort();						\
	}								\
	F_SET(dbc, DBC_RECOVER);					\
	mpf = file_dbp->mpf;						\
} while (0)

#define	REC_CLOSE {							\
	int __t_ret;							\
	if (argp != NULL)						\
		__os_free(dbenv, argp);					\
	if (dbc != NULL &&						\
	    (__t_ret = __db_c_close(dbc)) != 0 && ret == 0)		\
		ret = __t_ret;						\
	}								\
    if (ret != 0) {                         \
        CHECK_ABORT                         \
    } \
	return (ret)

/*
 * No-op versions of the same macros.
 */
#define	REC_NOOP_INTRO(func) do {					\
	argp = NULL;							\
	if ((ret = func(dbenv, dbtp->data, &argp)) != 0)		\
		return (ret);						\
} while (0)
#define	REC_NOOP_CLOSE							\
	if (argp != NULL)						\
		__os_free(dbenv, argp);					\
	return (ret)

/*
 * Standard debugging macro for all recovery functions.
 */
#ifdef DEBUG_RECOVER
#define	REC_PRINT(func)							\
	(void)func(dbenv, dbtp, lsnp, op, info);
#else
#define	REC_PRINT(func)
#endif

/*
 * Actions to __db_lget
 */
#define	LCK_ALWAYS		1	/* Lock even for off page dup cursors */
#define	LCK_COUPLE		2	/* Lock Couple */
#define	LCK_COUPLE_ALWAYS	3	/* Lock Couple even in txn. */
#define	LCK_DOWNGRADE		4	/* Downgrade the lock. (internal) */
#define	LCK_ROLLBACK		5	/* Lock even if in rollback */

/*
 * If doing transactions we have to hold the locks associated with a data item
 * from a page for the entire transaction.  However, we don't have to hold the
 * locks associated with walking the tree.  Distinguish between the two so that
 * we don't tie up the internal pages of the tree longer than necessary.
 */
#define	__LPUT(dbc, lock)						\
	(LOCK_ISSET(lock) ?  __lock_put((dbc)->dbp->dbenv, &(lock)) : 0)

/*
 * __TLPUT -- transactional lock put
 *	If the lock is valid then
 *	   If we are not in a transaction put the lock.
 *	   Else if the cursor is doing dirty reads and this was a read then
 *		put the lock.
 *	   Else if the db is supporting dirty reads and this is a write then
 *		downgrade it.
 *	Else do nothing.
 */
#define	__TLPUT(dbc, lock)						\
	(LOCK_ISSET(lock) ? __db_lput(dbc, &(lock)) : 0)

typedef struct {
	DBC *dbc;
	u_int32_t count;
} db_trunc_param;

#include "dbinc/db_dispatch.h"
#include "dbinc_auto/db_auto.h"
#include "dbinc_auto/crdel_auto.h"
#include "dbinc_auto/db_ext.h"
#endif /* !_DB_AM_H_ */
