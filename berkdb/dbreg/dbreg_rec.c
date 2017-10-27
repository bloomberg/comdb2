/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
/*
 * Copyright (c) 1995, 1996
 *	The President and Fellows of Harvard University.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: dbreg_rec.c,v 11.120 2003/10/27 15:54:31 sue Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <string.h>
#include <stddef.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/db_am.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"
#include "dbinc/qam.h"

#include "printformats.h"

static int __dbreg_open_file __P((DB_ENV *,
    DB_TXN *, __dbreg_register_args *, void *));

int
__dbreg_register_print(DB_ENV *dbenv, DBT *dbtp, DB_LSN *lsnp, db_recops notused2, void *notused3);

static char* opstr(db_recops op) {
    switch (op) {
        case DB_TXN_ABORT:  return "DB_TXN_ABORT";
        case DB_TXN_APPLY: return "DB_TXN_APPLY";
        case DB_TXN_BACKWARD_ALLOC: return "DB_TXN_BACKWARD_ALLOC";
        case DB_TXN_BACKWARD_ROLL: return "DB_TXN_BACKWARD_ROLL";
        case DB_TXN_FORWARD_ROLL: return "DB_TXN_FORWARD_ROLL";
        case DB_TXN_GETPGNOS: return "DB_TXN_GETPGNOS";
        case DB_TXN_OPENFILES: return "DB_TXN_OPENFILES";
        case DB_TXN_POPENFILES: return "DB_TXN_POPENFILES";
        case DB_TXN_PRINT: return "DB_TXN_PRINT";
        case DB_TXN_SNAPISOL: return "DB_TXN_SNAPISOL";
        case DB_TXN_LOGICAL_BACKWARD_ROLL: return "DB_TXN_LOGICAL_BACKWARD_ROLL";
        default: return "???";
    }
}

/*
 * PUBLIC: int __dbreg_register_recover
 * PUBLIC:     __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__dbreg_register_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	DB_ENTRY *dbe;
	DB_LOG *dblp;
	DB *dbp;
	__dbreg_register_args *argp;
	int do_close, do_open, do_rem, ret, t_ret;
    DB_LSN dbreg_lsn = *lsnp;
    struct lsn_range *r = NULL;

	dblp = dbenv->lg_handle;
	dbp = NULL;

#ifdef DEBUG_RECOVER
	REC_PRINT(__dbreg_register_print);
#endif
	do_open = do_close = 0;
	if ((ret = __dbreg_register_read(dbenv, dbtp->data, &argp)) != 0)
		goto out;

	/* we're in a forward recovery pass opening files - */
	if ((op == DB_TXN_OPENFILES || op == DB_TXN_POPENFILES) &&
	    dbenv->attr.apprec_track_lsn_ranges) {
		struct fileid_track *ft;
		int i;
		ft = &dbenv->fileid_track;
		if (argp->fileid >= ft->numids) {
			__os_realloc(dbenv, (argp->fileid+1) * 
				     sizeof(lsn_range_list), &ft->ranges);
			for (i = ft->numids; i < argp->fileid+1; i++) {
				listc_init(&ft->ranges[i], 
					   offsetof(struct lsn_range, lnk));
			}
			ft->numids = argp->fileid+1;
		}

		/* Do we already have a record for this range?  Same
		 * filename for the same id?)  If so, extend the
		 * range rather than creating an additional record. */
		struct lsn_range *prev =  ft->ranges[argp->fileid].bot;
		if (prev && dbenv->attr.consolidate_dbreg_ranges &&
		    strlen(prev->fname)+1 == argp->name.size &&
		    strcmp(prev->fname, argp->name.data) == 0) {
			/* do nothing */
		} else {
			__os_malloc(dbenv, sizeof(struct lsn_range), &r);
			/* end is not defined until we get to the
			 * next record */
			ZERO_LSN(r->end);
			r->start.file = lsnp->file;
			r->start.offset = lsnp->offset;
			__os_malloc(dbenv, argp->name.size+1, &r->fname);
			memcpy(r->fname, argp->name.data, argp->name.size);
			r->fname[argp->name.size] = 0;
			r->dbp = NULL; /* we're about to open the db below */

			/* if this isn't the first range for this
			 * fileid, close the previous range */
			if (ft->ranges[argp->fileid].bot != NULL) {
				/* the range ends here */
				prev->end.file = lsnp->file;
				prev->end.offset = lsnp->offset;
			}
			listc_abl(&ft->ranges[argp->fileid], r);
		}
	}

	switch (argp->opcode) {
	case DBREG_OPEN:
		if ((DB_REDO(op) ||
		    op == DB_TXN_OPENFILES || op == DB_TXN_POPENFILES))
			do_open = 1;
		else
			do_close = 1;
		break;

	case DBREG_CLOSE:
		if (DB_UNDO(op))
			do_open = 1;
		else
			do_close = 1;
		break;
	case DBREG_RCLOSE:
		/*
		 * DBREG_RCLOSE was generated by recover because a file was
		 * left open.  The POPENFILES pass, which is run to open
		 * files to abort prepared transactions, may not include the
		 * open for this file so we open it here.  Note that a normal
		 * CLOSE is not legal before the prepared transaction is
		 * committed or aborted.
		 */
		if (DB_UNDO(op) || op == DB_TXN_POPENFILES)
			do_open = 1;
		else
			do_close = 1;
		break;

	case DBREG_CHKPNT:
		if (DB_UNDO(op) || op == DB_TXN_APPLY ||
		    op == DB_TXN_OPENFILES || op == DB_TXN_POPENFILES)
			do_open = 1;
		break;
	}

	if (do_open) {
		/*
		 * We must open the db even if the meta page is not
		 * yet written as we may be creating subdatabase.
		 */
		if (op == DB_TXN_OPENFILES && argp->opcode != DBREG_CHKPNT)
			F_SET(dblp, DBLOG_FORCE_OPEN);

		/*
		 * During an abort or an open pass to recover prepared txns,
		 * we need to make sure that we use the same locker id on the
		 * open.  We pass the txnid along to ensure this.
		 */
		ret = __dbreg_open_file(dbenv,
		    op == DB_TXN_ABORT || op == DB_TXN_POPENFILES ?
		    argp->txnid : NULL, argp, info);

		if (ret != 0) {
			/*
			 fprintf(stderr,
			     "__dbreg_open_file got %d for id %d\n",
			     ret, argp->fileid);
			 */
		}


		if (ret == ENOENT || ret == EINVAL) {

			/*
			 * If this is an OPEN while rolling forward, it's
			 * possible that the file was recreated since last
			 * time we got here.  In that case, we've got deleted
			 * set and probably shouldn't, so we need to check
			 * for that case and possibly retry.
			 */
			if (op == DB_TXN_FORWARD_ROLL &&
			    argp->txnid != 0 &&
			    dblp->dbentry[argp->fileid].deleted) {
				dblp->dbentry[argp->fileid].deleted = 0;
				ret =
				    __dbreg_open_file(dbenv, NULL, argp, info);
			}
			/*
			 * We treat ENOENT as OK since it's possible that
			 * the file was renamed or deleted.
			 * All other errors, we return.
			 */
			if (ret == ENOENT)
				ret = 0;
		}
		F_CLR(dblp, DBLOG_FORCE_OPEN);
	}

	if (do_close) {
		/*
		 * If we are undoing an open, or redoing a close,
		 * then we need to close the file.
		 *
		 * If the file is deleted, then we can just ignore this close.
		 * Otherwise, we should usually have a valid dbp we should
		 * close or whose reference count should be decremented.
		 * However, if we shut down without closing a file, we may, in
		 * fact, not have the file open, and that's OK.
		 */
		do_rem = 0;
		MUTEX_THREAD_LOCK(dbenv, dblp->mutexp);
		if (argp->fileid < dblp->dbentry_cnt) {
			/*
			 * Typically, closes should match an open which means
			 * that if this is a close, there should be a valid
			 * entry in the dbentry table when we get here,
			 * however there are exceptions.  1. If this is an
			 * OPENFILES pass, then we may have started from
			 * a log file other than the first, and the
			 * corresponding open appears in an earlier file.
			 * 2. If we are undoing an open on an abort or
			 * recovery, it's possible that we failed after
			 * the log record, but before we actually entered
			 * a handle here.
			 * 3. If we aborted an open, then we wrote a non-txnal
			 * RCLOSE into the log.  During the forward pass, the
			 * file won't be open, and that's OK.
			 */
			dbe = &dblp->dbentry[argp->fileid];
			if (dbe->dbp == NULL && !dbe->deleted) {
				/* No valid entry here. */
#if 0
				if (DB_REDO(op) || argp->opcode == DBREG_CHKPNT) {
					__db_err(dbenv,
					    "Improper file close at %lu/%lu",
					    (u_long)lsnp->file,
					    (u_long)lsnp->offset);
					ret = EINVAL;
				}
#endif
				MUTEX_THREAD_UNLOCK(dbenv, dblp->mutexp);
				goto done;
			}

			/* We have either an open entry or a deleted entry. */
			if ((dbp = dbe->dbp) != NULL) {
				MUTEX_THREAD_UNLOCK(dbenv, dblp->mutexp);
				(void)__dbreg_revoke_id(dbp, 0,
				    DB_LOGFILEID_INVALID);

				/*
				 * If we're a replication client, it's
				 * possible to get here with a dbp that
				 * the user opened, but which we later
				 * assigned a fileid to.  Be sure that
				 * we only close dbps that we opened in
				 * the recovery code;  they should have
				 * DB_AM_RECOVER set.
				 *
				 * The only exception is if we're aborting
				 * in a normal environment;  then we might
				 * get here with a non-AM_RECOVER database.
				 */
				if (F_ISSET(dbp, DB_AM_RECOVER) ||
				    op == DB_TXN_ABORT)
					do_rem = 1;
			} else if (dbe->deleted) {
				MUTEX_THREAD_UNLOCK(dbenv, dblp->mutexp);
				__dbreg_rem_dbentry(dblp, argp->fileid);
			}
		} else
			MUTEX_THREAD_UNLOCK(dbenv, dblp->mutexp);
		if (do_rem) {
			/*
			 * During recovery, all files are closed.  On an abort,
			 * we only close the file if we opened it during the
			 * abort (DB_AM_RECOVER set), otherwise we simply do
			 * a __db_refresh.  For the close case, if remove or
			 * rename has closed the file, don't request a sync,
			 * because the NULL mpf would be a problem.
			 */
			if (dbp != NULL) {
				/*
				 * If we are undoing a create we'd better
				 * discard any buffers from the memory pool.
				 * We identify creates because the argp->id
				 * field contains the transaction containing
				 * the file create; if that id is invalid, we
				 * are not creating.
				 */

				if (argp->id != TXN_INVALID)
					F_SET(dbp, DB_AM_DISCARD);

				if (op == DB_TXN_ABORT &&
				    !F_ISSET(dbp, DB_AM_RECOVER))
					t_ret = __db_refresh(dbp,
					    NULL, DB_NOSYNC, NULL);
				else {
					if (op == DB_TXN_APPLY)
						__db_sync(dbp);
					t_ret =
					    __db_close(dbp, NULL, DB_NOSYNC);
				}
				if (t_ret != 0 && ret == 0)
					ret = t_ret;
			}
		}
	}
done:
	if (ret == 0)
		*lsnp = argp->prev_lsn;
out:	if (argp != NULL)
		__os_free(dbenv, argp);


	return (ret);
}

/*
 * __dbreg_open_file --
 *	Called during log_register recovery.  Make sure that we have an
 *	entry in the dbentry table for this ndx.  Returns 0 on success,
 *	non-zero on error.
 */
static int
__dbreg_open_file(dbenv, txn, argp, info)
	DB_ENV *dbenv;
	DB_TXN *txn;
	__dbreg_register_args *argp;
	void *info;
{
	DB_ENTRY *dbe;
	DB_LOG *lp;
	DB *dbp;
	u_int32_t id;
	int ret;


	lp = (DB_LOG *)dbenv->lg_handle;
	/*
	 * We never re-open temporary files.  Temp files are only
	 * useful during aborts in which case the dbp was entered
	 * when the file was registered.  During recovery, we treat
	 * temp files as properly deleted files, allowing the open to
	 * fail and not reporting any errors when recovery fails to
	 * get a valid dbp from __dbreg_id_to_db.
	 */
	if (argp->name.size == 0) {
		(void)__dbreg_add_dbentry(dbenv, lp, NULL, argp->fileid);
		return (ENOENT);
	}

	/*
	 * When we're opening, we have to check that the name we are opening
	 * is what we expect.  If it's not, then we close the old file and
	 * open the new one.
	 */
	MUTEX_THREAD_LOCK(dbenv, lp->mutexp);
	if (argp->fileid < lp->dbentry_cnt)
		dbe = &lp->dbentry[argp->fileid];
	else
		dbe = NULL;

	if (dbe != NULL) {
		if ((dbp = dbe->dbp) != NULL) {
			if (dbp->meta_pgno != argp->meta_pgno ||
			    memcmp(dbp->fileid,
				argp->uid.data, DB_FILE_ID_LEN) != 0) {
				MUTEX_THREAD_UNLOCK(dbenv, lp->mutexp);
				(void)__dbreg_revoke_id(dbp, 0,
				    DB_LOGFILEID_INVALID);
				if (F_ISSET(dbp, DB_AM_RECOVER))
					__db_close(dbp, NULL, DB_NOSYNC);

				goto reopen;
			}

			/*
			 * We should only get here if we already have the
			 * dbp from an openfiles pass, in which case, what's
			 * here had better be the same dbp.
			 */
			DB_ASSERT(dbe->dbp == dbp);
			MUTEX_THREAD_UNLOCK(dbenv, lp->mutexp);

			/*
			 * This is a successful open.  We need to record that
			 * in the txnlist so that we know how to handle the
			 * subtransaction that created the file system object.
			 */
			if (argp->id != TXN_INVALID &&
			    __db_txnlist_update(dbenv, info,
				argp->id, TXN_EXPECTED, NULL) == TXN_NOTFOUND)
				(void)__db_txnlist_add(dbenv,
				    info, argp->id, TXN_EXPECTED, NULL);
			return (0);
		}
	}

	MUTEX_THREAD_UNLOCK(dbenv, lp->mutexp);

	/*
	 * We are about to pass a recovery txn pointer into the main library.
	 * We need to make sure that any accessed fields are set appropriately.
	 */
reopen:if (txn != NULL) {
		id = txn->txnid;
		memset(txn, 0, sizeof(DB_TXN));
		txn->txnid = id;
		txn->mgrp = dbenv->tx_handle;
	}

	ret = __dbreg_do_open(dbenv, txn, lp, argp->uid.data, argp->name.data,
	    argp->ftype, argp->fileid, argp->meta_pgno, info, argp->id);

	return ret;
}
