/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1997-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: dbreg_util.c,v 11.39 2003/11/10 17:42:34 sue Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/db_shash.h"
#include "dbinc/mp.h"
#include <poll.h>
#include "dbinc_auto/mp_ext.h"
#include "dbinc/log.h"
#include "dbinc/txn.h"
#include "printformats.h"
#include "logmsg.h"
#include "comdb2_atomic.h"

static int __dbreg_check_master __P((DB_ENV *, u_int8_t *, char *));

/*
 * __dbreg_add_dbentry --
 *	Adds a DB entry to the dbreg DB entry table.
 *
 * PUBLIC: int __dbreg_add_dbentry __P((DB_ENV *, DB_LOG *, DB *, int32_t));
 */
int
__dbreg_add_dbentry(dbenv, dblp, dbp, ndx)
	DB_ENV *dbenv;
	DB_LOG *dblp;
	DB *dbp;
	int32_t ndx;
{
	int32_t i;
	int ret;

	ret = 0;

	MUTEX_THREAD_LOCK(dbenv, dblp->mutexp);

	/*
	 * Check if we need to grow the table.  Note, ndx is 0-based (the
	 * index into the DB entry table) an dbentry_cnt is 1-based, the
	 * number of available slots.
	 */
	if (dblp->dbentry_cnt <= ndx) {
		if ((ret = __os_realloc(dbenv,
		    (ndx + DB_GROW_SIZE) * sizeof(DB_ENTRY),
		    &dblp->dbentry)) != 0)
			goto err;

		/* Initialize the new entries. */
		for (i = dblp->dbentry_cnt; i < ndx + DB_GROW_SIZE; i++) {
			dblp->dbentry[i].dbp = NULL;
			dblp->dbentry[i].deleted = 0;
			dblp->dbentry[i].pfcnt = 0;
		}
		dblp->dbentry_cnt = i;
	}

	DB_ASSERT(dblp->dbentry[ndx].dbp == NULL);
	dblp->dbentry[ndx].deleted = dbp == NULL;

	dblp->dbentry[ndx].dbp = dbp;

err:	MUTEX_THREAD_UNLOCK(dbenv, dblp->mutexp);
	return (ret);
}

/*
 * __dbreg_rem_dbentry
 *	Remove an entry from the DB entry table.
 *
 * PUBLIC: void __dbreg_rem_dbentry __P((DB_LOG *, int32_t));
 */
void
__dbreg_rem_dbentry(dblp, ndx)
	DB_LOG *dblp;
	int32_t ndx;
{
	uint32_t count = 0;

	MUTEX_THREAD_LOCK(dblp->dbenv, dblp->mutexp);
	if (dblp->dbentry_cnt > ndx) {
		dblp->dbentry[ndx].dbp = NULL;

		while (dblp->dbentry[ndx].pfcnt > 0) {
			poll(NULL, 0, 10);

			if (0 == ++count % 100) {
				logmsg(LOGMSG_ERROR, "%s waiting for prefault for dbreg entry %d to complete\n",
						__func__, ndx);
			}
		}
		dblp->dbentry[ndx].deleted = 0;
	}

	MUTEX_THREAD_UNLOCK(dblp->dbenv, dblp->mutexp);
}

/*
 * __ufid_sanity_check --
 *	Abort if ufid and ufid_chk dont match
 *
 * PUBLIC: void __ufid_sanity_check __P((DB_ENV *, FNAME *));
 */
void
__ufid_sanity_check(dbenv, fnp)
	DB_ENV *dbenv;
	FNAME *fnp;
{
	if (memcmp(fnp->ufid, fnp->ufid_chk, DB_FILE_ID_LEN)) {
		logmsg(LOGMSG_FATAL, "%s: critical error: ufid has changed\n",
                __func__);
		abort();
	}
}


/*
 * __dbreg_open_files --
 *	Put a DBREG_CHKPNT log record for each open database.
 *
 * PUBLIC: int __dbreg_open_files __P((DB_ENV *));
 */
static int
__dbreg_open_files_int(dbenv, flags)
	DB_ENV *dbenv;
	u_int32_t flags;
{
	DB_LOG *dblp;
	DB_LSN r_unused;
	DBT *dbtp, fid_dbt, t;
	FNAME *fnp;
	LOG *lp;
	int ret;
	int oflags = 0;

	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;

	ret = 0;

	if (!LF_ISSET(DB_REG_HAVE_FQLOCK))
		MUTEX_LOCK(dbenv, &lp->fq_mutex);

	for (fnp = SH_TAILQ_FIRST(&lp->fq, __fname);
	    fnp != NULL; fnp = SH_TAILQ_NEXT(fnp, q, __fname)) {

		if (fnp->name_off == INVALID_ROFF)
			dbtp = NULL;

		else {
			memset(&t, 0, sizeof(t));
			t.data = R_ADDR(&dblp->reginfo, fnp->name_off);
			t.size = (u_int32_t)strlen(t.data) + 1;
			dbtp = &t;
		}
		memset(&fid_dbt, 0, sizeof(fid_dbt));

		__ufid_sanity_check(dbenv, fnp);

		fid_dbt.data = fnp->ufid;
		fid_dbt.size = DB_FILE_ID_LEN;

		oflags = DB_LOG_DONT_LOCK;
		if (!fnp->is_durable)
			oflags |= DB_LOG_NOT_DURABLE;
		/*
		 * Output DBREG_CHKPNT records which will be processed during
		 * the OPENFILES pass of recovery.  At the end of recovery we
		 * want to output the files that were open so a future recovery
		 * run will have the correct files open during a backward pass.
		 * For this we output DBREG_RCLOSE records so the files will be
		 * closed on the forward pass.
		 */
		if ((ret = __dbreg_register_log(dbenv,
			    NULL, &r_unused, oflags,
			    F_ISSET(dblp,
				DBLOG_RECOVER) ? DBREG_RCLOSE : DBREG_CHKPNT,
			    dbtp, &fid_dbt, fnp->id, fnp->s_type,
			    fnp->meta_pgno, TXN_INVALID)) != 0)
			 break;
	}

	if (!LF_ISSET(DB_REG_HAVE_FQLOCK))
		MUTEX_UNLOCK(dbenv, &lp->fq_mutex);

	return (ret);
}


int
__dbreg_open_files(dbenv)
	DB_ENV *dbenv;
{
	return __dbreg_open_files_int(dbenv, 0);
}

// PUBLIC: int __dbreg_open_files_checkpoint __P((DB_ENV *));
int
__dbreg_open_files_checkpoint(dbenv)
	DB_ENV *dbenv;
{
	return __dbreg_open_files_int(dbenv, DB_REG_HAVE_FQLOCK);
}




/*
 * __dbreg_recovery_pages --
 *	Iterate through the files - call 'recover_pages' on each.
 *
 * PUBLIC: int __dbreg_recovery_pages __P((DB_ENV *));
 */
int
__dbreg_recovery_pages(dbenv)
	DB_ENV *dbenv;
{
	DB_LOG *dblp;
	DB *dbp;
	int32_t i;

	COMPQUIET(dbp, NULL);

	dblp = dbenv->lg_handle;

	MUTEX_THREAD_LOCK(dbenv, dblp->mutexp);
	for (i = 0; i < dblp->dbentry_cnt; i++) {
		if ((dbp = dblp->dbentry[i].dbp) != NULL) {
			__memp_read_recovery_pages(dbp->mpf);
		}
	}
	MUTEX_THREAD_UNLOCK(dbenv, dblp->mutexp);
	return 0;
}

/*
 * __dbreg_close_files --
 *	Remove the id's of open files and actually close those
 *	files that were opened by the recovery daemon.  We sync the
 *	file, unless its mpf pointer has been NULLed by a db_remove or
 *	db_rename.  We may not have flushed the log_register record that
 *	closes the file.
 *
 * PUBLIC: int __dbreg_close_files __P((DB_ENV *));
 */
int
__dbreg_close_files(dbenv)
	DB_ENV *dbenv;
{
	DB_LOG *dblp;
	DB *dbp;
	int ret, t_ret;
	int32_t i;

	/* If we haven't initialized logging, we have nothing to do. */
	if (!LOGGING_ON(dbenv))
		return (0);

	dblp = dbenv->lg_handle;
	ret = 0;
	MUTEX_THREAD_LOCK(dbenv, dblp->mutexp);
	for (i = 0; i < dblp->dbentry_cnt; i++) {
		/*
		 * We only want to close dbps that recovery opened.  Any
		 * dbps that weren't opened by recovery but show up here
		 * are about to be unconditionally removed from the table.
		 * Before doing so, we need to revoke their log fileids
		 * so that we don't end up leaving around FNAME entries
		 * for dbps that shouldn't have them.
		 */
		if ((dbp = dblp->dbentry[i].dbp) != NULL) {
			/*
			 * It's unsafe to call DB->close or revoke_id 
			 * while holding the thread lock, because
			 * we'll call __dbreg_rem_dbentry and grab it again.
			 *
			 * Just drop it.  Since dbreg ids go monotonically
			 * upward, concurrent opens should be safe, and the
			 * user should have no business closing files while
			 * we're in this loop anyway--we're in the process of
			 * making all outstanding dbps invalid.
			 */
			MUTEX_THREAD_UNLOCK(dbenv, dblp->mutexp);
			if (F_ISSET(dbp, DB_AM_RECOVER))
				t_ret = __db_close(dbp,
				    NULL, dbp->mpf == NULL ? DB_NOSYNC : 0);
			else
				t_ret =
				    __dbreg_revoke_id(dbp, 0,
				    DB_LOGFILEID_INVALID);
			if (ret == 0)
				ret = t_ret;
			MUTEX_THREAD_LOCK(dbenv, dblp->mutexp);
		}

		dblp->dbentry[i].deleted = 0;
		dblp->dbentry[i].dbp = NULL;
	}
	MUTEX_THREAD_UNLOCK(dbenv, dblp->mutexp);
	return (ret);
}


// PUBLIC: int __dbreg_id_to_db __P(( DB_ENV *, DB_TXN *, DB **, int32_t, int, DB_LSN *, int));
int
__dbreg_id_to_db(dbenv, txn, dbpp, ndx, inc, lsnp, in_recovery_verify)
	DB_ENV *dbenv;
	DB_TXN *txn;
	DB **dbpp;
	int32_t ndx;
	int inc;
	DB_LSN *lsnp;
	int in_recovery_verify;
{
	int rc;

	rc = __dbreg_id_to_db_int(dbenv, txn, dbpp, ndx, inc, 1, lsnp,
	    in_recovery_verify);

	if (rc == ENOENT) {
		/*fprintf(stderr, "__dbreg_id_to_db %d ENOENT\n", ndx); */
      /*__db_panic(dbenv, rc);*/
	}

	if (rc == DB_DELETED) {
		/*fprintf(stderr, "__dbreg_id_to_db %d DB_DELETED\n", ndx); */
      /*__db_panic(dbenv, rc);*/
	}

	return rc;
}

/*
 * PUBLIC: int __dbreg_id_to_db_in_recovery __P((DB_ENV *, int, DB_TXN *, DB_LSN *, DB **));
 * Return a DB corresponding to dbreg id - special case for when we're in recovery
 * and dbreg ids may refer to values that aren't the most recent dbreg id for a given db.
 */
int
__dbreg_id_to_db_in_recovery(DB_ENV *dbenv, int ndx, DB_TXN *txn, DB_LSN *lsnp,
    DB **dbpp)
{
	struct lsn_range *r;
	struct fileid_track *ft = &dbenv->fileid_track;
	int ret = 0;

	if (ndx < 0 || ndx >= ft->numids) {
		ret = EINVAL;

		goto err;
	}

	r = ft->ranges[ndx].bot;
	while (r) {
		if (log_compare(&r->start, lsnp) <= 0 &&
		    ((log_compare(&r->end, lsnp) > 0) || IS_ZERO_LSN(r->end)))
			break;
		r = r->lnk.prev;
	}
	if (r) {
		if (r->dbp == NULL) {
			ret = db_create(&r->dbp, dbenv, 0);
			if (ret)
				goto err;
			F_SET(r->dbp, DB_AM_RECOVER);
			ret =
			    __db_open(r->dbp, txn, r->fname, NULL, DB_UNKNOWN,
			    DB_ODDFILESIZE, __db_omode("rw----"), 0);
			/* If we fail to open it with ENOENT, we'll return ENOENT, which is expected if the file
			 * is no longer there. Any other errors are errors, and get passed through. */
			if (ret) {
				logmsg(LOGMSG_ERROR, "in recovery, at %u:%u open %s rc %d\n",
				    lsnp->file, lsnp->offset, r->fname, ret);
				logmsg(LOGMSG_ERROR, "range was " PR_LSN " to " PR_LSN "\n",
				    PARM_LSN(r->start), PARM_LSN(r->end));
				if (ret == ENOENT) {
					if (!dbenv->attr.dbreg_errors_fatal)
						ret = DB_DELETED;
				}
				__db_close(r->dbp, txn, 0);
				r->dbp = NULL;

				goto err;
			}
		}
		*dbpp = r->dbp;
		goto err;
	} else {
		__db_err(dbenv, "No range defined for fileid %d lsn %u:%u", ndx,
		    lsnp->file, lsnp->offset);
		ret = EINVAL;

		goto err;
	}

err:
	return ret;
}


static int
__dbreg_id_to_db_int_int(dbenv, txn, dbpp, ndx, inc, tryopen, lsnp,
    in_recovery_verify, prefault_refcount)
	DB_ENV *dbenv;
	DB_TXN *txn;
	DB **dbpp;
	int32_t ndx;
	int inc, tryopen;
	DB_LSN *lsnp;
	int in_recovery_verify;
	int prefault_refcount;
{
	DB_LOG *dblp;
	FNAME *fname;
	int ret;
	char *name;

	ret = 0;
	dblp = dbenv->lg_handle;
	COMPQUIET(inc, 0);

	MUTEX_THREAD_LOCK(dbenv, dblp->mutexp);

	/*
	 * For a backwards roll, we may be undoing events that refer
	 * to files which have been opened, but with different
	 * fileids, because they got recycled.  In that case, use the
	 * right file, depending on which LSN we're processing.  This
	 * needs 2 changes: LSN needs to be specified for this routine
	 * (it's always available since we're processing log records
	 * to begin with), and we need the whole fileid_tracker
	 * kludge.  We don't need a fancy data structure here since
	 * we're processing things in order.
	 *
	 * TODO: keep track of current list element for each fileid -
	 * no reason to rescan the list each time.  It's likely to be
	 * a rather short scan, in practice.
	 */
	if (prefault_refcount == 0 && lsnp &&
	    dbenv->attr.apprec_track_lsn_ranges &&
	    (dbenv->recovery_pass == DB_TXN_BACKWARD_ROLL ||
		in_recovery_verify)) {
		ret = __dbreg_id_to_db_in_recovery(dbenv, ndx, txn, lsnp, dbpp);
		if (ret)
			__db_err(dbenv,
"No range defined for fileid %d lsn "PR_LSN", ret %d\n",
			    ndx, PARM_LSNP(lsnp), ret);
		goto err;
	}

	if (ndx < 0) {
		ret = ENOENT;

		goto err;
	}

	/*
	 * Under XA, a process different than the one issuing DB operations
	 * may abort a transaction.  In this case, the "recovery" routines
	 * are run by a process that does not necessarily have the file open,
	 * so we we must open the file explicitly.
	 */
	if (ndx >= dblp->dbentry_cnt ||
	    (!dblp->dbentry[ndx].deleted && dblp->dbentry[ndx].dbp == NULL)) {
		if (!tryopen || F_ISSET(dblp, DBLOG_RECOVER)) {
			ret = ENOENT;

			goto err;
		}

		/*
		 * __dbreg_id_to_fname acquires the region's fq_mutex,
		 * which we can't safely acquire while we hold the thread lock.
		 * We no longer need it anyway--the dbentry table didn't
		 * have what we needed.
		 */
		MUTEX_THREAD_UNLOCK(dbenv, dblp->mutexp);

		if (__dbreg_id_to_fname(dblp, ndx, 0, &fname) != 0)
			/*
			 * With transactional opens, we may actually have
			 * closed this file in the transaction in which
			 * case this will fail too.  Then it's up to the
			 * caller to reopen the file.
			 */
			return (ENOENT);

		/*
		 * Note that we're relying on fname not to change, even
		 * though we released the mutex that protects it (fq_mutex)
		 * inside __dbreg_id_to_fname.  This should be a safe
		 * assumption, because the other process that has the file
		 * open shouldn't be closing it while we're trying to abort.
		 */
		name = R_ADDR(&dblp->reginfo, fname->name_off);

		/*
		 * At this point, we are not holding the thread lock, so exit
		 * directly instead of going through the exit code at the
		 * bottom.  If the __dbreg_do_open succeeded, then we don't need
		 * to do any of the remaining error checking at the end of this
		 * routine.
		 * XXX I am sending a NULL txnlist and 0 txnid which may be
		 * completely broken ;(
		 */

		__ufid_sanity_check(dbenv, fname);

		if ((ret = __dbreg_do_open(dbenv, txn, dblp,
		    fname->ufid, name, fname->s_type,
		    ndx, fname->meta_pgno, NULL, 0)) != 0)
			return (ret);

		*dbpp = dblp->dbentry[ndx].dbp;
		return (0);
	}

	/*
	 * Return DB_DELETED if the file has been deleted (it's not an error).
	 */
	if (dblp->dbentry[ndx].deleted) {
		ret = DB_DELETED;
		goto err;
	}

	/* It's an error if we don't have a corresponding writeable DB. */
	if ((*dbpp = dblp->dbentry[ndx].dbp) == NULL) {
		ret = ENOENT;

		goto err;
	}

	/* Increment the prefault refcount for this */
	if (prefault_refcount) {
		ATOMIC_ADD32(dblp->dbentry[ndx].pfcnt, 1);
	}

err:	MUTEX_THREAD_UNLOCK(dbenv, dblp->mutexp);
	return (ret);
}

// PUBLIC: void __dbreg_prefault_complete __P((DB_ENV *, int32_t));
void
__dbreg_prefault_complete(dbenv, ndx)
	DB_ENV *dbenv;
	int32_t ndx;
{
	DB_LOG *dblp;

	dblp = dbenv->lg_handle;

	DB_ASSERT(ndx < dblp->dbentry_cnt);
	DB_ASSERT(dblp->dbentry[ndx].pfcnt > 0);

	ATOMIC_ADD32(dblp->dbentry[ndx].pfcnt, -1);
}

// PUBLIC: int __dbreg_id_to_db_prefault __P((DB_ENV *, DB_TXN *, DB **, int32_t, int));
int
__dbreg_id_to_db_prefault(dbenv, txn, dbpp, ndx, inc)
	DB_ENV *dbenv;
	DB_TXN *txn;
	DB **dbpp;
	int32_t ndx;
	int inc;
{
	return __dbreg_id_to_db_int_int(dbenv, txn, dbpp, ndx, inc, 0, NULL, 0,
	    1);
}


/*
 * __dbreg_id_to_db_int --
 *	Return the DB corresponding to the specified dbreg id.  The internal
 * version takes a final parameter that indicates whether we should attempt
 * to open the file if no mapping is found.  During recovery, the recovery
 * routines all want to try to open the file (and this is called from
 * __dbreg_id_to_db), however, if we have a multi-process environment where
 * some processes may not have the files open (e.g., XA), then we also get
 * called from __dbreg_assign_id and it's OK if there is no mapping.
 *
 * PUBLIC: int __dbreg_id_to_db_int __P((DB_ENV *,
 * PUBLIC:     DB_TXN *, DB **, int32_t, int, int, DB_LSN *, int));
 */
int
__dbreg_id_to_db_int(dbenv, txn, dbpp, ndx, inc, tryopen, lsnp,
    in_recovery_verify)
	DB_ENV *dbenv;
	DB_TXN *txn;
	DB **dbpp;
	int32_t ndx;
	int inc, tryopen;
	DB_LSN *lsnp;
	int in_recovery_verify;
{
	return __dbreg_id_to_db_int_int(dbenv, txn, dbpp, ndx, inc, tryopen,
	    lsnp, in_recovery_verify, 0);
}


/*
 * __dbreg_id_to_fname --
 *	Traverse the shared-memory region looking for the entry that
 *	matches the passed dbreg id.  Returns 0 on success; -1 on error.
 *
 * PUBLIC: int __dbreg_id_to_fname __P((DB_LOG *, int32_t, int, FNAME **));
 */
int
__dbreg_id_to_fname(dblp, lid, have_lock, fnamep)
	DB_LOG *dblp;
	int32_t lid;
	int have_lock;
	FNAME **fnamep;
{
	DB_ENV *dbenv;
	FNAME *fnp;
	LOG *lp;
	int ret;

	dbenv = dblp->dbenv;
	lp = dblp->reginfo.primary;

	ret = -1;

	if (!have_lock)
		MUTEX_LOCK(dbenv, &lp->fq_mutex);
	for (fnp = SH_TAILQ_FIRST(&lp->fq, __fname);
	    fnp != NULL; fnp = SH_TAILQ_NEXT(fnp, q, __fname)) {
		if (fnp->id == lid) {
			*fnamep = fnp;
			ret = 0;
			break;
		}
	}
	if (!have_lock)
		MUTEX_UNLOCK(dbenv, &lp->fq_mutex);

	return (ret);
}
/*
 * __dbreg_fid_to_fname --
 *	Traverse the shared-memory region looking for the entry that
 *	matches the passed file unique id.  Returns 0 on success; -1 on error.
 *
 * PUBLIC: int __dbreg_fid_to_fname __P((DB_LOG *, u_int8_t *, int, FNAME **));
 */
int
__dbreg_fid_to_fname(dblp, fid, have_lock, fnamep)
	DB_LOG *dblp;
	u_int8_t *fid;
	int have_lock;
	FNAME **fnamep;
{
	DB_ENV *dbenv;
	FNAME *fnp;
	LOG *lp;
	int ret;

	dbenv = dblp->dbenv;
	lp = dblp->reginfo.primary;

	ret = -1;

	if (!have_lock)
		MUTEX_LOCK(dbenv, &lp->fq_mutex);
	for (fnp = SH_TAILQ_FIRST(&lp->fq, __fname);
		fnp != NULL; fnp = SH_TAILQ_NEXT(fnp, q, __fname)) {

		__ufid_sanity_check(dbenv, fnp);

		if (memcmp(fnp->ufid, fid, DB_FILE_ID_LEN) == 0) {
			*fnamep = fnp;
			ret = 0;
			break;
		}
	}
	if (!have_lock)
		MUTEX_UNLOCK(dbenv, &lp->fq_mutex);

	if (ret == -1) {
		/*fprintf(stderr, "__dbreg_fid_to_fname for %lld returns -1\n", *fid); */
	}

	return (ret);
}

/*
 * __dbreg_get_name
 *
 * Interface to get name of registered files.  This is mainly diagnostic
 * and the name passed could be transient unless there is something
 * ensuring that the file cannot be closed.
 *
 * PUBLIC: int __dbreg_get_name __P((DB_ENV *, u_int8_t *, char **));
 */
int
__dbreg_get_name(dbenv, fid, namep)
	DB_ENV *dbenv;
	u_int8_t *fid;
	char **namep;
{
	DB_LOG *dblp;
	FNAME *fname;

	dblp = dbenv->lg_handle;

	if (dblp != NULL && __dbreg_fid_to_fname(dblp, fid, 0, &fname) == 0) {
		*namep = R_ADDR(&dblp->reginfo, fname->name_off);
		return (0);
	}

	return (-1);
}

/*
 * __dbreg_do_open --
 *	Open files referenced in the log.  This is the part of the open that
 * is not protected by the thread mutex.
 * PUBLIC: int __dbreg_do_open __P((DB_ENV *, DB_TXN *, DB_LOG *, u_int8_t *,
 * PUBLIC:     char *, DBTYPE, int32_t, db_pgno_t, void *, u_int32_t));
 */
int
__dbreg_do_open(dbenv,
    txn, lp, uid, name, ftype, ndx, meta_pgno, info, id)
	DB_ENV *dbenv;
	DB_TXN *txn;
	DB_LOG *lp;
	u_int8_t *uid;
	char *name;
	DBTYPE ftype;
	int32_t ndx;
	db_pgno_t meta_pgno;
	void *info;
	u_int32_t id;
{
	DB *dbp;
	int ret;
	u_int32_t cstat;

	if ((ret = db_create(&dbp, lp->dbenv, 0)) != 0)
		return (ret);

	/*
	 * We can open files under a number of different scenarios.
	 * First, we can open a file during a normal txn_abort, if that file
	 * was opened and closed during the transaction (as is the master
	 * database of a sub-database).
	 * Second, we might be aborting a transaction in XA and not have
	 * it open in the process that is actually doing the abort.
	 * Third, we might be in recovery.
	 * In case 3, there is no locking, so there is no issue.
	 * In cases 1 and 2, we are guaranteed to already hold any locks
	 * that we need, since we're still in the same transaction, so by
	 * setting DB_AM_RECOVER, we guarantee that we don't log and that
	 * we don't try to acquire locks on behalf of a different locker id.
	 */
	F_SET(dbp, DB_AM_RECOVER);
	if (meta_pgno != PGNO_BASE_MD) {
		memcpy(dbp->fileid, uid, DB_FILE_ID_LEN);
		dbp->meta_pgno = meta_pgno;
	}
	if ((ret = __db_open(dbp, txn, name, NULL,
	    ftype, DB_ODDFILESIZE, __db_omode("rw----"), meta_pgno)) == 0) {

		/*
		 * Verify that we are opening the same file that we were
		 * referring to when we wrote this log record.
		 */
		if ((meta_pgno != PGNO_BASE_MD &&
		    __dbreg_check_master(dbenv, uid, name) != 0) ||
		    memcmp(uid, dbp->fileid, DB_FILE_ID_LEN) != 0)
			cstat = TXN_IGNORE;
		else
			cstat = TXN_EXPECTED;

		/* Assign the specific dbreg id to this dbp. */
		ret = __dbreg_assign_id(dbp, ndx);
		if (ret != 0)
			goto err;

		/*
		 * If we successfully opened this file, then we need to
		 * convey that information to the txnlist so that we
		 * know how to handle the subtransaction that created
		 * the file system object.
		 */
		if (id != TXN_INVALID) {
			if ((ret = __db_txnlist_update(dbenv,
			    info, id, cstat, NULL)) == TXN_NOTFOUND)
				ret = __db_txnlist_add(dbenv,
				    info, id, cstat, NULL);
			else if (ret > 0)
				ret = 0;
		}
err:
		if (cstat == TXN_IGNORE)
			goto not_right;
		return (ret);
	} else if (ret == ENOENT) {


		/* Record that the open failed in the txnlist. */
		if (id != TXN_INVALID && (ret = __db_txnlist_update(dbenv,
		    info, id, TXN_UNEXPECTED, NULL)) == TXN_NOTFOUND)
			ret = __db_txnlist_add(dbenv,
			    info, id, TXN_UNEXPECTED, NULL);
	}
not_right:
	(void)__db_close(dbp, NULL, 0);
	/* Add this file as deleted. */

   /*fprintf(stderr, "__dbreg_do_open: adding ndx %d as deleted\n", ndx);*/

	(void)__dbreg_add_dbentry(dbenv, lp, NULL, ndx);
	return (ret);
}

static int
__dbreg_check_master(dbenv, uid, name)
	DB_ENV *dbenv;
	u_int8_t *uid;
	char *name;
{
	DB *dbp;
	int ret;

	ret = 0;
	if ((ret = db_create(&dbp, dbenv, 0)) != 0)
		return (ret);
	F_SET(dbp, DB_AM_RECOVER);
	ret = __db_open(dbp,
	    NULL, name, NULL, DB_BTREE, 0, __db_omode("rw----"), PGNO_BASE_MD);

	if (ret == 0 && memcmp(uid, dbp->fileid, DB_FILE_ID_LEN) != 0)
		ret = EINVAL;

	(void)__db_close(dbp, NULL, 0);
	return (ret);
}

/*
 * __dbreg_lazy_id --
 *	When a replication client gets upgraded to being a replication master,
 * it may have database handles open that have not been assigned an ID, but
 * which have become legal to use for logging.
 *
 *	This function lazily allocates a new ID for such a function, in a
 * new transaction created for the purpose.  We need to do this in a new
 * transaction because we definitely wish to commit the dbreg_register, but
 * at this point we have no way of knowing whether the log record that incited
 * us to call this will be part of a committed transaction.
 *
 * PUBLIC: int __dbreg_lazy_id __P((DB *));
 */
int
__dbreg_lazy_id(dbp)
	DB *dbp;
{
	DB_ENV *dbenv;
	DB_LOG *dblp;
	DB_TXN *txn;
	FNAME *fnp;
	LOG *lp;
	int32_t id;
	int ret;

	dbenv = dbp->dbenv;

	DB_ASSERT(IS_REP_MASTER(dbenv));

	dbenv = dbp->dbenv;
	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;
	fnp = dbp->log_filename;

	/* The fq_mutex protects the FNAME list and id management. */
	MUTEX_LOCK(dbenv, &lp->fq_mutex);
	if (fnp->id != DB_LOGFILEID_INVALID) {
		MUTEX_UNLOCK(dbenv, &lp->fq_mutex);
		return (0);
	}
	id = DB_LOGFILEID_INVALID;
	if ((ret = __txn_begin(dbenv, NULL, &txn, 0)) != 0)
		goto err;

	if ((ret = __dbreg_get_id(dbp, txn, &id)) != 0) {
		(void)__txn_abort(txn);
		goto err;
	}

	if ((ret = __txn_commit(txn, DB_TXN_NOSYNC)) != 0)
		goto err;

	/*
	 * All DB related logging routines check the id value *without*
	 * holding the fq_mutex to know whether we need to call
	 * dbreg_lazy_id to begin with.  We must set the ID after a
	 * *successful* commit so that there is no possibility of a second
	 * modification call finding a valid ID in the dbp before the
	 * dbreg_register and commit records are in the log.
	 * If there was an error, then we call __dbreg_revoke_id to
	 * remove the entry from the lists. 
	 */
	fnp->id = id;
err:
	if (ret != 0 && id != DB_LOGFILEID_INVALID)
		(void)__dbreg_revoke_id(dbp, 1, id);
	MUTEX_UNLOCK(dbenv, &lp->fq_mutex);
	return (ret);
}

/*
 * __dbreg_push_id and __dbreg_pop_id --
 *	Dbreg ids from closed files are kept on a stack in shared memory
 * for recycling.  (We want to reuse them as much as possible because each
 * process keeps open files in an array by ID.)  Push them to the stack and
 * pop them from it, managing memory as appropriate.
 *
 * The stack is protected by the fq_mutex, and in both functions we assume
 * that this is already locked.
 *
 * PUBLIC: int __dbreg_push_id __P((DB_ENV *, int32_t));
 * PUBLIC: int __dbreg_pop_id __P((DB_ENV *, int32_t *));
 */
int
__dbreg_push_id(dbenv, id)
	DB_ENV *dbenv;
	int32_t id;
{
	DB_LOG *dblp;
	LOG *lp;
	int32_t *stack, *newstack;
	int ret;

	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;

	if (lp->free_fid_stack != INVALID_ROFF)
		stack = R_ADDR(&dblp->reginfo, lp->free_fid_stack);
	else
		stack = NULL;

	/* Check if we have room on the stack. */
	if (lp->free_fids_alloced <= lp->free_fids + 1) {
		R_LOCK(dbenv, &dblp->reginfo);
		if ((ret = __os_malloc(dbenv,
		    (lp->free_fids_alloced + 20) * sizeof(u_int32_t),
		    &newstack)) != 0) {
			R_UNLOCK(dbenv, &dblp->reginfo);
			return (ret);
		}

		memcpy(newstack, stack,
		    lp->free_fids_alloced * sizeof(u_int32_t));
		lp->free_fid_stack = R_OFFSET(&dblp->reginfo, newstack);
		lp->free_fids_alloced += 20;

		if (stack != NULL)
			__os_free(dbenv, stack);

		stack = newstack;
		R_UNLOCK(dbenv, &dblp->reginfo);
	}

	DB_ASSERT(stack != NULL);
	stack[lp->free_fids++] = id;
	return (0);
}

int
__dbreg_pop_id(dbenv, id)
	DB_ENV *dbenv;
	int32_t *id;
{
	DB_LOG *dblp;
	LOG *lp;
	int32_t *stack;

	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;

	/* Do we have anything to pop? */
	if (lp->free_fid_stack != INVALID_ROFF && lp->free_fids > 0) {
		stack = R_ADDR(&dblp->reginfo, lp->free_fid_stack);
		*id = stack[--lp->free_fids];
	} else
		*id = DB_LOGFILEID_INVALID;

	return (0);
}

/*
 * __dbreg_pluck_id --
 *	Remove a particular dbreg id from the stack of free ids.  This is
 * used when we open a file, as in recovery, with a specific ID that might
 * be on the stack.
 *
 * Returns success whether or not the particular id was found, and like
 * push and pop, assumes that the fq_mutex is locked.
 *
 * PUBLIC: int __dbreg_pluck_id __P((DB_ENV *, int32_t));
 */
int
__dbreg_pluck_id(dbenv, id)
	DB_ENV *dbenv;
	int32_t id;
{
	DB_LOG *dblp;
	LOG *lp;
	int32_t *stack;
	int i;

	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;

	/* Do we have anything to look at? */
	if (lp->free_fid_stack != INVALID_ROFF) {
		stack = R_ADDR(&dblp->reginfo, lp->free_fid_stack);
		for (i = 0; i < lp->free_fids; i++)
			if (id == stack[i]) {
				/*
				 * Found it.  Overwrite it with the top
				 * id (which may harmlessly be itself),
				 * and shorten the stack by one.
				 */
				stack[i] = stack[lp->free_fids - 1];
				lp->free_fids--;
				return (0);
			}
	}

	return (0);
}

/*
 * __bb_dbreg_print_all_dblist --
 *	Display all entries in dblist.
 *
 * PUBLIC: void __bb_dbreg_print_all_dblist __P((DB_ENV *, void(*)(void *, const char *, ...), void *));
 */
void
__bb_dbreg_print_all_dblist(dbenv, prncallback, userptr)
	DB_ENV *dbenv;
	void (*prncallback)(void *userptr, const char *fmt, ...);
	void *userptr;
{
    DB *ldbp;
	FNAME *fnp;
	int del, first;
	char *name;

    MUTEX_THREAD_LOCK(dbenv, dbenv->dblist_mutexp);
    DB_MPOOL *dbmp = dbenv->mp_handle;
	int longest_len = 0;

	for (ldbp = LIST_FIRST(&dbenv->dblist);
			ldbp != NULL; ldbp = LIST_NEXT(ldbp, dblistlinks)) {
		name = (char*)R_ADDR(dbmp->reginfo, ldbp->mpf->mfp->path_off); 
		int len = strlen(name);
		if (longest_len < len)
			longest_len = len;
	}

	prncallback(userptr, "__bb_dbreg_print_dblist ----------\n");
	for (first = 1, ldbp = LIST_FIRST(&dbenv->dblist);
			ldbp != NULL; ldbp = LIST_NEXT(ldbp, dblistlinks)) {

		if (first) {
			first = 0;
			prncallback(userptr, "%5s %-*s%-8s%-10s%-10s%s\n",
					"ID", longest_len + 1, "Name", "Type", "Pgno", "Txnid", "DBP-Info");
		}
		fnp = ldbp->log_filename;
		if (fnp->name_off == INVALID_ROFF)
			name = "";
		else
			name = (char*)R_ADDR(dbmp->reginfo, ldbp->mpf->mfp->path_off); 

		prncallback(userptr,
		    "%5ld %-*s %-8s%-10lu%-10lx%s %d %lx %lx\n",
		    (long)fnp->id, longest_len, name, __db_dbtype_to_string(ldbp->type),
		    (u_long)ldbp->meta_pgno, (u_long)fnp->create_txnid,
		    ldbp == NULL ? "No DBP" : "DBP", NULL, P_TO_ULONG(ldbp),
		    (u_long)(ldbp == NULL ? 0 : ldbp->flags));
    }

    MUTEX_THREAD_UNLOCK(dbenv, dbenv->dblist_mutexp);
	prncallback(userptr, "__bb_dbreg_print_dblist ^^^^^^^^^^\n");
}

/*
 * __bb_dbreg_print_dblist --
 *	Display the list of files.  Hacked for Bloomberg.
 *	Don't use tabs, they don't look nice after they go through
 *	console.  Also option to redirect to ctrace file via callback.
 *
 * PUBLIC: void __bb_dbreg_print_dblist __P((DB_ENV *, void(*)(void *, const char *, ...), void *));
 */
void
__bb_dbreg_print_dblist(dbenv, prncallback, userptr)
	DB_ENV *dbenv;
	void (*prncallback)(void *userptr, const char *fmt, ...);
	void *userptr;
{
	DB *dbp;
	DB_LOG *dblp;
	FNAME *fnp;
	LOG *lp;
	int del, first;
	char *name;
	int longest_len = 0;

	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;
	for (first = 1, fnp = SH_TAILQ_FIRST(&lp->fq, __fname);
	    fnp != NULL; fnp = SH_TAILQ_NEXT(fnp, q, __fname)) {
		if (fnp->name_off == INVALID_ROFF)
            continue;
        name = R_ADDR(&dblp->reginfo, fnp->name_off);
		int len = strlen(name);
		if (longest_len < len)
			longest_len = len;
    }

	prncallback(userptr, "__bb_dbreg_print_dblist ----------\n");
	MUTEX_LOCK(dbenv, &lp->fq_mutex);

	for (first = 1, fnp = SH_TAILQ_FIRST(&lp->fq, __fname);
	    fnp != NULL; fnp = SH_TAILQ_NEXT(fnp, q, __fname)) {
		if (first) {
			first = 0;
			prncallback(userptr, "%5s %-*s%-8s%-10s%-10s%s\n",
					"ID", longest_len + 1, "Name", "Type", "Pgno", "Txnid", "DBP-Info");
		}
		if (fnp->name_off == INVALID_ROFF)
			name = "";
		else
			name = R_ADDR(&dblp->reginfo, fnp->name_off);

		dbp = fnp->id >= dblp->dbentry_cnt ? NULL :
		    dblp->dbentry[fnp->id].dbp;
		del = fnp->id >= dblp->dbentry_cnt ? 0 :
		    dblp->dbentry[fnp->id].deleted;
		prncallback(userptr,
		    "%5ld %-*s %-8s%-10lu%-10lx%s %d %lx %lx\n",
		    (long)fnp->id, longest_len, name, __db_dbtype_to_string(fnp->s_type),
		    (u_long)fnp->meta_pgno, (u_long)fnp->create_txnid,
		    dbp == NULL ? "No DBP" : "DBP", del, P_TO_ULONG(dbp),
		    (u_long)(dbp == NULL ? 0 : dbp->flags));
	}

	MUTEX_UNLOCK(dbenv, &lp->fq_mutex);
	prncallback(userptr, "__bb_dbreg_print_dblist ^^^^^^^^^^\n");
}


// PUBLIC: void __bb_dbreg_print_dblist_stdout __P((DB_ENV *));
void
__bb_dbreg_print_dblist_stdout(dbenv)
	DB_ENV *dbenv;
{
	DB *dbp;
	DB_LOG *dblp;
	FNAME *fnp;
	LOG *lp;
	int del, first;
	char *name;

	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;

	logmsg(LOGMSG_USER, "__bb_dbreg_print_dblist ----------\n");
	MUTEX_LOCK(dbenv, &lp->fq_mutex);

	for (first = 1, fnp = SH_TAILQ_FIRST(&lp->fq, __fname);
	    fnp != NULL; fnp = SH_TAILQ_NEXT(fnp, q, __fname)) {
		if (first) {
			first = 0;
			logmsg(LOGMSG_USER, "  %-5s%-32s%-8s%-10s%-10s%s\n",
			    "ID", "Name", "Type", "Pgno", "Txnid", "DBP-Info");
		}
		if (fnp->name_off == INVALID_ROFF)
			name = "";
		else
			name = R_ADDR(&dblp->reginfo, fnp->name_off);

		dbp = fnp->id >= dblp->dbentry_cnt ? NULL :
		    dblp->dbentry[fnp->id].dbp;
		del = fnp->id >= dblp->dbentry_cnt ? 0 :
		    dblp->dbentry[fnp->id].deleted;
		logmsg(LOGMSG_USER, "  %-5ld%-32s %-8s%-10lu%-10lx%s %d %lx %lx\n",
		    (long)fnp->id, name,
		    __db_dbtype_to_string(fnp->s_type),
		    (u_long)fnp->meta_pgno, (u_long)fnp->create_txnid,
		    dbp == NULL ? "No DBP" : "DBP", del, P_TO_ULONG(dbp),
		    (u_long)(dbp == NULL ? 0 : dbp->flags));
	}

	MUTEX_UNLOCK(dbenv, &lp->fq_mutex);
	logmsg(LOGMSG_USER, "__bb_dbreg_print_dblist ^^^^^^^^^^\n");
}

/*
 * PUBLIC: int __dbreg_ufid_exists __P((DB_ENV *, const u_int8_t *, int *));
 *  Return whether the unique file id exists in dbreg. Also return logging file id
 *  in the output pointer if found.
 */
int
__dbreg_ufid_exists(DB_ENV *dbenv, const u_int8_t *ufid, int *id)
{
	FNAME *fnp;
	DB_LOG *dblp;
	LOG *lp;
	int found = 0;

	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;

	MUTEX_LOCK(dbenv, &lp->fq_mutex);
	for (fnp = SH_TAILQ_FIRST(&lp->fq, __fname); fnp; fnp = SH_TAILQ_NEXT(fnp, q, __fname)) {
		if (memcmp(ufid, fnp->ufid, DB_FILE_ID_LEN) == 0) {
			found = 1;
			if (id != NULL)
				*id = fnp->id;
		}
	}
	MUTEX_UNLOCK(dbenv, &lp->fq_mutex);
	return found;
}

int
__dbreg_exists(DB_ENV *dbenv, const char *find_name)
{
	FNAME *fnp;
	DB_LOG *dblp;
	LOG *lp;
	char *name;
	int found = 0;

	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;

	MUTEX_LOCK(dbenv, &lp->fq_mutex);
	for (fnp = SH_TAILQ_FIRST(&lp->fq, __fname); fnp;
	    fnp = SH_TAILQ_NEXT(fnp, q, __fname)) {
		if (fnp->name_off == INVALID_ROFF)
			name = "";
		else
			name = R_ADDR(&dblp->reginfo, fnp->name_off);

		if (strcmp(name, find_name) == 0) {
			found = 1;
			break;
		}
	}
	MUTEX_UNLOCK(dbenv, &lp->fq_mutex);

	return found;
}
