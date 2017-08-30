/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: db_rec.c,v 11.48 2003/08/27 03:54:18 ubell Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/hash.h"
#include "dbinc/db_swap.h"
#include "logmsg.h"

static int __db_pg_free_recover_int __P((DB_ENV *,
	__db_pg_freedata_args *, DB *, DB_LSN *, DB_MPOOLFILE *, db_recops,
	int));

#include <stdlib.h>

extern int gbl_check_page_in_recovery;

int
__db_addrem_verify_fileid(dbenv, dbp, lsnp, prevlsn, fileid)
	DB_ENV *dbenv;
	DB *dbp;
	DB_LSN *lsnp;
	DB_LSN *prevlsn;
	int32_t fileid;
{
	int ret = 0;
	int32_t type;
	DB_LOGC *logc = NULL;
	DBT log = { 0 };
	__db_debug_args *debug = NULL;
	int32_t op;

	if (prevlsn->file == 0) {
#if 0
		fprintf(stderr, "no previous log record for adderem at %u:%u\n",
		    lsnp->file, lsnp->offset);
#endif
		ret = 0;
		goto done;
	}

	ret = dbenv->log_cursor(dbenv, &logc, 0);
	if (ret)
		goto done;
	log.flags = DB_DBT_MALLOC;

	ret = logc->get(logc, prevlsn, &log, DB_SET);

	if (ret) {
		/* first log? nothing to read */
		if (ret == DB_NOTFOUND)
			ret = 0;
		goto done;
	}

	LOGCOPY_32(&type, log.data);
	if (type != DB___db_debug) {
#if 0
		fprintf(stderr,
		    "addrem, no previous debug record at %u:%u, prev is %u:%u\n",
		    lsnp->file, lsnp->offset, prevlsn->file, prevlsn->offset);
#endif
		ret = 0;
		goto done;
	}

	ret = __db_debug_read(dbenv, log.data, &debug);
	if (ret)
		goto done;

	if (debug->op.size != sizeof(int32_t) || debug->key.data == NULL) {
		logmsg(LOGMSG_ERROR, 
            "addrem, prev was debug, but not in format I expected, at %u:%u prev %u:%u\n",
		    lsnp->file, lsnp->offset, prevlsn->file, prevlsn->offset);
		goto done;
	}

	LOGCOPY_32(&op, debug->op.data);
	if (op != 1) {
		logmsg(LOGMSG_ERROR, 
            "addrem, prev was debug, but I don't understand op %d at %u:%u prev %u:%u\n",
		    op, lsnp->file, lsnp->offset, prevlsn->file,
		    prevlsn->offset);
		goto done;
	}
	if (strcmp(dbp->fname, debug->key.data)) {
		logmsg(LOGMSG_ERROR, 
            "mismatch @%u:%u: fileid %d log record %s, dbp %s\n",
		    lsnp->file, lsnp->offset, fileid, debug->key.data,
		    dbp->fname);
		ret = EINVAL;

		goto done;
	}
	// printf("%u:%u debug %s, log %s\n", prevlsn->file, prevlsn->offset, debug->key.data, dbp->fname);

done:
	if (logc)
		logc->close(logc, 0);
	if (log.data)
		free(log.data);
	if (debug)
		free(debug);
	return ret;
}


/*
 * PUBLIC: int __db_addrem_recover
 * PUBLIC:    __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
 *
 * This log message is generated whenever we add or remove a duplicate
 * to/from a duplicate page.  On recover, we just do the opposite.
 */
int
__db_addrem_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__db_addrem_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	PAGE *pagep;
	u_int32_t change;
	int cmp_n, cmp_p, ret;
    int check_page = gbl_check_page_in_recovery;

	pagep = NULL;
	COMPQUIET(info, NULL);
	REC_PRINT(__db_addrem_print);
	REC_INTRO(__db_addrem_read, 1);

	if (dbenv->attr.debug_addrem_dbregs) {
		ret =
		    __db_addrem_verify_fileid(dbenv, file_dbp, lsnp,
		    &argp->prev_lsn, argp->fileid);
		if (ret)
			goto out;
	}

	if ((ret = __memp_fget(mpf, &argp->pgno, 0, &pagep)) != 0) {
		if (DB_UNDO(op)) {
			/*
			 * We are undoing and the page doesn't exist.  That
			 * is equivalent to having a pagelsn of 0, so we
			 * would not have to undo anything.  In this case,
			 * don't bother creating a page.
			 */
			goto done;
		} else
			if ((ret = __memp_fget(mpf,
			    &argp->pgno, DB_MPOOL_CREATE, &pagep)) != 0)
				goto out;
	}

    if (check_page) {
        __dir_pg( mpf, argp->pgno, (u_int8_t *)pagep, 0);
        __dir_pg( mpf, argp->pgno, (u_int8_t *)pagep, 1);
    }

	cmp_n = log_compare(lsnp, &LSN(pagep));
	cmp_p = log_compare(&LSN(pagep), &argp->pagelsn);
	CHECK_LSN(op, cmp_p, &LSN(pagep), &argp->pagelsn, lsnp, argp->fileid,
	    argp->pgno);
	change = 0;
	if ((cmp_p == 0 && DB_REDO(op) && argp->opcode == DB_ADD_DUP) ||
	    (cmp_n == 0 && DB_UNDO(op) && IS_REM_OPCODE(argp->opcode))) {
		BINTERNAL *bi = argp->hdr.data;
		BOVERFLOW *bo = argp->hdr.data;

		/* comdb2 endian changes */
		/* if this is a b-internal page, swap the header read from the logfile */
		if (LOG_SWAPPED() && argp->hdr.size >= 12 &&
		    TYPE(pagep) == P_IBTREE) {
			M_16_SWAP(bi->len);
			M_32_SWAP(bi->pgno);
			M_32_SWAP(bi->nrecs);
		}
		/* if this is an overflow page, swap the header read from the logfile */
		else if (LOG_SWAPPED() && argp->hdr.size >= 12 &&
		    B_OVERFLOW == B_TYPE(bo)) {
			M_16_SWAP(bo->unused1);
			M_32_SWAP(bo->pgno);
			M_32_SWAP(bo->tlen);
		}
		/* Need to redo an add, or undo a delete. */
		if ((ret =
			__db_pitem_opcode(dbc, pagep, argp->indx, argp->nbytes,
			    argp->hdr.size == 0 ? NULL : &argp->hdr,
			    argp->dbt.size == 0 ? NULL : &argp->dbt,
			    argp->opcode)) != 0)
			 goto out;
		change = DB_MPOOL_DIRTY;
	} else if ((cmp_n == 0 && DB_UNDO(op) && argp->opcode == DB_ADD_DUP) ||
	    (cmp_p == 0 && DB_REDO(op) && IS_REM_OPCODE(argp->opcode))) {
		/* Need to undo an add, or redo a delete. */
		if ((ret = __db_ditem(dbc,
			    pagep, argp->indx, argp->nbytes)) != 0)
			goto out;
		change = DB_MPOOL_DIRTY;
	}

	if (change) {
		if (DB_REDO(op))
			LSN(pagep) = *lsnp;
		else
			LSN(pagep) = argp->pagelsn;
	}
    
    if (check_page) {
        __dir_pg( mpf, argp->pgno, (u_int8_t *)pagep, 0);
        __dir_pg( mpf, argp->pgno, (u_int8_t *)pagep, 1);
    }

	if ((ret = __memp_fput(mpf, pagep, change)) != 0)
		goto out;
	pagep = NULL;

done:	*lsnp = argp->prev_lsn;
	ret = 0;

out:	if (pagep != NULL)
		(void)__memp_fput(mpf, pagep, 0);
	REC_CLOSE;
}

/*
 * PUBLIC: int __db_big_recover
 * PUBLIC:     __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_big_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__db_big_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	PAGE *pagep;
	u_int32_t change;
	int cmp_n, cmp_p, ret;

	pagep = NULL;
	COMPQUIET(info, NULL);
	REC_PRINT(__db_big_print);
	REC_INTRO(__db_big_read, 1);

	if ((ret = __memp_fget(mpf, &argp->pgno, 0, &pagep)) != 0) {
		if (DB_UNDO(op)) {
			/*
			 * We are undoing and the page doesn't exist.  That
			 * is equivalent to having a pagelsn of 0, so we
			 * would not have to undo anything.  In this case,
			 * don't bother creating a page.
			 */
			ret = 0;
			goto ppage;
		} else
			if ((ret = __memp_fget(mpf,
			    &argp->pgno, DB_MPOOL_CREATE, &pagep)) != 0)
				goto out;
	}

	/*
	 * There are three pages we need to check.  The one on which we are
	 * adding data, the previous one whose next_pointer may have
	 * been updated, and the next one whose prev_pointer may have
	 * been updated.
	 */
	cmp_n = log_compare(lsnp, &LSN(pagep));
	cmp_p = log_compare(&LSN(pagep), &argp->pagelsn);
	CHECK_LSN(op, cmp_p, &LSN(pagep), &argp->pagelsn, lsnp, argp->fileid,
	    argp->pgno);
	change = 0;
	if ((cmp_p == 0 && DB_REDO(op) && argp->opcode == DB_ADD_BIG) ||
	    (cmp_n == 0 && DB_UNDO(op) && argp->opcode == DB_REM_BIG)) {
		/* We are either redo-ing an add, or undoing a delete. */
		P_INIT(pagep, file_dbp->pgsize, argp->pgno, argp->prev_pgno,
			argp->next_pgno, 0, P_OVERFLOW);
		OV_LEN(pagep) = argp->dbt.size;
		OV_REF(pagep) = 1;
		memcpy((u_int8_t *)pagep + P_OVERHEAD(file_dbp), argp->dbt.data,
		    argp->dbt.size);
		PREV_PGNO(pagep) = argp->prev_pgno;
		change = DB_MPOOL_DIRTY;
	} else if ((cmp_n == 0 && DB_UNDO(op) && argp->opcode == DB_ADD_BIG) ||
	    (cmp_p == 0 && DB_REDO(op) && argp->opcode == DB_REM_BIG)) {
		/*
		 * We are either undo-ing an add or redo-ing a delete.
		 * The page is about to be reclaimed in either case, so
		 * there really isn't anything to do here.
		 */
		change = DB_MPOOL_DIRTY;
	}
	if (change)
		LSN(pagep) = DB_REDO(op) ? *lsnp : argp->pagelsn;

	if ((ret = __memp_fput(mpf, pagep, change)) != 0)
		goto out;
	pagep = NULL;

	/*
	 * We only delete a whole chain of overflow.
	 * Each page is handled individually
	 */
	if (argp->opcode == DB_REM_BIG)
		goto done;

	/* Now check the previous page. */
ppage:	if (argp->prev_pgno != PGNO_INVALID) {
		change = 0;
		if ((ret =
		    __memp_fget(mpf, &argp->prev_pgno, 0, &pagep)) != 0) {
			if (DB_UNDO(op)) {
				/*
				 * We are undoing and the page doesn't exist.
				 * That is equivalent to having a pagelsn of 0,
				 * so we would not have to undo anything.  In
				 * this case, don't bother creating a page.
				 */
				*lsnp = argp->prev_lsn;
				ret = 0;
				goto npage;
			} else
				if ((ret = __memp_fget(mpf, &argp->prev_pgno,
				    DB_MPOOL_CREATE, &pagep)) != 0)
					goto out;
		}

		cmp_n = log_compare(lsnp, &LSN(pagep));
		cmp_p = log_compare(&LSN(pagep), &argp->prevlsn);
		CHECK_LSN(op, cmp_p, &LSN(pagep), &argp->prevlsn, lsnp,
		    argp->fileid, argp->pgno);

		if (cmp_p == 0 && DB_REDO(op) && argp->opcode == DB_ADD_BIG) {
			/* Redo add, undo delete. */
			NEXT_PGNO(pagep) = argp->pgno;
			change = DB_MPOOL_DIRTY;
		} else if (cmp_n == 0 &&
		    DB_UNDO(op) && argp->opcode == DB_ADD_BIG) {
			/* Redo delete, undo add. */
			NEXT_PGNO(pagep) = argp->next_pgno;
			change = DB_MPOOL_DIRTY;
		}
		if (change)
			LSN(pagep) = DB_REDO(op) ? *lsnp : argp->prevlsn;
		if ((ret = __memp_fput(mpf, pagep, change)) != 0)
			goto out;
	}
	pagep = NULL;

	/* Now check the next page.  Can only be set on a delete. */
npage:	if (argp->next_pgno != PGNO_INVALID) {
		change = 0;
		if ((ret =
		    __memp_fget(mpf, &argp->next_pgno, 0, &pagep)) != 0) {
			if (DB_UNDO(op)) {
				/*
				 * We are undoing and the page doesn't exist.
				 * That is equivalent to having a pagelsn of 0,
				 * so we would not have to undo anything.  In
				 * this case, don't bother creating a page.
				 */
				goto done;
			} else
				if ((ret = __memp_fget(mpf, &argp->next_pgno,
				    DB_MPOOL_CREATE, &pagep)) != 0)
					goto out;
		}

		cmp_n = log_compare(lsnp, &LSN(pagep));
		cmp_p = log_compare(&LSN(pagep), &argp->nextlsn);
		CHECK_LSN(op, cmp_p, &LSN(pagep), &argp->nextlsn, lsnp,
		    argp->fileid, argp->pgno);
		if (cmp_p == 0 && DB_REDO(op)) {
			PREV_PGNO(pagep) = PGNO_INVALID;
			change = DB_MPOOL_DIRTY;
		} else if (cmp_n == 0 && DB_UNDO(op)) {
			PREV_PGNO(pagep) = argp->pgno;
			change = DB_MPOOL_DIRTY;
		}
		if (change)
			LSN(pagep) = DB_REDO(op) ? *lsnp : argp->nextlsn;
		if ((ret = __memp_fput(mpf, pagep, change)) != 0)
			goto out;
	}
	pagep = NULL;

done:	*lsnp = argp->prev_lsn;
	ret = 0;

out:	if (pagep != NULL)
		(void)__memp_fput(mpf, pagep, 0);
	REC_CLOSE;
}

/*
 * __db_ovref_recover --
 *	Recovery function for __db_ovref().
 *
 * PUBLIC: int __db_ovref_recover
 * PUBLIC:     __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_ovref_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__db_ovref_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	PAGE *pagep;
	int cmp, modified, ret;

	pagep = NULL;
	COMPQUIET(info, NULL);
	REC_PRINT(__db_ovref_print);
	REC_INTRO(__db_ovref_read, 1);

	if ((ret = __memp_fget(mpf, &argp->pgno, 0, &pagep)) != 0) {
		if (DB_UNDO(op))
			goto done;
		ret = __db_pgerr(file_dbp, argp->pgno, ret);
		goto out;
	}

	modified = 0;
	cmp = log_compare(&LSN(pagep), &argp->lsn);
	CHECK_LSN(op, cmp, &LSN(pagep), &argp->lsn, lsnp, argp->fileid,
	    argp->pgno);
	if (cmp == 0 && DB_REDO(op)) {
		/* Need to redo update described. */
		OV_REF(pagep) += argp->adjust;

		pagep->lsn = *lsnp;
		modified = 1;
	} else if (log_compare(lsnp, &LSN(pagep)) == 0 && DB_UNDO(op)) {
		/* Need to undo update described. */
		OV_REF(pagep) -= argp->adjust;

		pagep->lsn = argp->lsn;
		modified = 1;
	}
	if ((ret = __memp_fput(mpf, pagep, modified ? DB_MPOOL_DIRTY : 0)) != 0)
		goto out;
	pagep = NULL;

done:	*lsnp = argp->prev_lsn;
	ret = 0;

out:	if (pagep != NULL)
		(void)__memp_fput(mpf, pagep, 0);
	REC_CLOSE;
}

int bdb_relink_pglogs(void *bdb_state, unsigned char *fileid, db_pgno_t pgno,
    db_pgno_t prev_pgno, db_pgno_t next_pgno, DB_LSN lsn);
/*
 * __db_relink_recover --
 *	Recovery function for relink.
 *
 * PUBLIC: int __db_relink_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_relink_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__db_relink_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	PAGE *pagep;
	int cmp_n, cmp_p, modified, ret;

	pagep = NULL;
	COMPQUIET(info, NULL);
	REC_PRINT(__db_relink_print);
	REC_INTRO(__db_relink_read, 1);

	if (mpf && bdb_relink_pglogs(dbenv->app_private, mpf->fileid,
	    argp->pgno, argp->prev, argp->next, *lsnp) != 0) {
		logmsg(LOGMSG_FATAL, "%s: fail relink pglogs\n", __func__);
		abort();
	}

	/*
	 * There are up to three pages we need to check -- the page, and the
	 * previous and next pages, if they existed.  For a page add operation,
	 * the current page is the result of a split and is being recovered
	 * elsewhere, so all we need do is recover the next page.
	 */
	if ((ret = __memp_fget(mpf, &argp->pgno, 0, &pagep)) != 0) {
		if (DB_REDO(op)) {
			ret = __db_pgerr(file_dbp, argp->pgno, ret);
			goto out;
		}
		goto next2;
	}
	modified = 0;
	if (argp->opcode == DB_ADD_PAGE)
		goto next1;

	cmp_p = log_compare(&LSN(pagep), &argp->lsn);
	CHECK_LSN(op, cmp_p, &LSN(pagep), &argp->lsn, lsnp, argp->fileid,
	    argp->pgno);
	if (cmp_p == 0 && DB_REDO(op)) {
		/* Redo the relink. */
		pagep->lsn = *lsnp;
		modified = 1;
	} else if (log_compare(lsnp, &LSN(pagep)) == 0 && DB_UNDO(op)) {
		/* Undo the relink. */
		pagep->next_pgno = argp->next;
		pagep->prev_pgno = argp->prev;

		pagep->lsn = argp->lsn;
		modified = 1;
	}
next1:	if ((ret = __memp_fput(mpf, pagep, modified ? DB_MPOOL_DIRTY : 0)) != 0)
		goto out;
	pagep = NULL;

next2:	if ((ret = __memp_fget(mpf, &argp->next, 0, &pagep)) != 0) {
		if (DB_REDO(op)) {
			ret = __db_pgerr(file_dbp, argp->next, ret);
			goto out;
		}
		goto prev;
	}
	modified = 0;
	cmp_n = log_compare(lsnp, &LSN(pagep));
	cmp_p = log_compare(&LSN(pagep), &argp->lsn_next);
	CHECK_LSN(op, cmp_p, &LSN(pagep), &argp->lsn_next, lsnp, argp->fileid,
	    argp->pgno);
	if ((argp->opcode == DB_REM_PAGE && cmp_p == 0 && DB_REDO(op)) ||
	    (argp->opcode == DB_ADD_PAGE && cmp_n == 0 && DB_UNDO(op))) {
		/* Redo the remove or undo the add. */
		pagep->prev_pgno = argp->prev;

		modified = 1;
	} else if ((argp->opcode == DB_REM_PAGE && cmp_n == 0 && DB_UNDO(op)) ||
	    (argp->opcode == DB_ADD_PAGE && cmp_p == 0 && DB_REDO(op))) {
		/* Undo the remove or redo the add. */
		pagep->prev_pgno = argp->pgno;

		modified = 1;
	}
	if (modified == 1) {
		if (DB_UNDO(op))
			pagep->lsn = argp->lsn_next;
		else
			pagep->lsn = *lsnp;
	}
	if ((ret = __memp_fput(mpf, pagep, modified ? DB_MPOOL_DIRTY : 0)) != 0)
		goto out;
	pagep = NULL;
	if (argp->opcode == DB_ADD_PAGE)
		goto done;

prev:	if ((ret = __memp_fget(mpf, &argp->prev, 0, &pagep)) != 0) {
		if (DB_REDO(op)) {
			ret = __db_pgerr(file_dbp, argp->prev, ret);
			goto out;
		}
		goto done;
	}
	modified = 0;
	cmp_p = log_compare(&LSN(pagep), &argp->lsn_prev);
	CHECK_LSN(op, cmp_p, &LSN(pagep), &argp->lsn_prev, lsnp, argp->fileid,
	    argp->pgno);
	if (cmp_p == 0 && DB_REDO(op)) {
		/* Redo the relink. */
		pagep->next_pgno = argp->next;

		modified = 1;
	} else if (log_compare(lsnp, &LSN(pagep)) == 0 && DB_UNDO(op)) {
		/* Undo the relink. */
		pagep->next_pgno = argp->pgno;

		modified = 1;
	}
	if (modified == 1) {
		if (DB_UNDO(op))
			pagep->lsn = argp->lsn_prev;
		else
			pagep->lsn = *lsnp;
	}
	if ((ret = __memp_fput(mpf, pagep, modified ? DB_MPOOL_DIRTY : 0)) != 0)
		goto out;
	pagep = NULL;

done:	*lsnp = argp->prev_lsn;
	ret = 0;

out:	if (pagep != NULL)
		(void)__memp_fput(mpf, pagep, 0);
	REC_CLOSE;
}

/*
 * __db_debug_recover --
 *	Recovery function for debug.
 *
 * PUBLIC: int __db_debug_recover __P((DB_ENV *,
 * PUBLIC:     DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_debug_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__db_debug_args *argp;
	int ret;

	COMPQUIET(dbenv, NULL);

	COMPQUIET(op, DB_TXN_ABORT);
	COMPQUIET(info, NULL);

	REC_PRINT(__db_debug_print);
	REC_NOOP_INTRO(__db_debug_read);

	*lsnp = argp->prev_lsn;
	ret = 0;

	REC_NOOP_CLOSE;
}

/*
 * __db_noop_recover --
 *	Recovery function for noop.
 *
 * PUBLIC: int __db_noop_recover __P((DB_ENV *,
 * PUBLIC:      DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_noop_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__db_noop_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	PAGE *pagep;
	u_int32_t change;
	int cmp_n, cmp_p, ret;

	pagep = NULL;
	COMPQUIET(info, NULL);
	REC_PRINT(__db_noop_print);
	REC_INTRO(__db_noop_read, 0);

	if ((ret = __memp_fget(mpf, &argp->pgno, 0, &pagep)) != 0)
		goto out;

	cmp_n = log_compare(lsnp, &LSN(pagep));
	cmp_p = log_compare(&LSN(pagep), &argp->prevlsn);
	CHECK_LSN(op, cmp_p, &LSN(pagep), &argp->prevlsn, lsnp, argp->fileid,
	    argp->pgno);
	change = 0;
	if (cmp_p == 0 && DB_REDO(op)) {
		LSN(pagep) = *lsnp;
		change = DB_MPOOL_DIRTY;
	} else if (cmp_n == 0 && DB_UNDO(op)) {
		LSN(pagep) = argp->prevlsn;
		change = DB_MPOOL_DIRTY;
	}
	ret = __memp_fput(mpf, pagep, change);
	pagep = NULL;

done:	*lsnp = argp->prev_lsn;
out:	if (pagep != NULL)
		(void)__memp_fput(mpf, pagep, 0);
	REC_CLOSE;
}

/*
 * __db_pg_alloc_recover --
 *	Recovery function for pg_alloc.
 *
 * PUBLIC: int __db_pg_alloc_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_pg_alloc_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__db_pg_alloc_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DBMETA *meta;
	DB_MPOOLFILE *mpf;
	PAGE *pagep;
	db_pgno_t pgno;
	int cmp_n, cmp_p, created, level, modified, ret;

	meta = NULL;
	pagep = NULL;
	REC_PRINT(__db_pg_alloc_print);
	REC_INTRO(__db_pg_alloc_read, 0);

	/*
	 * Fix up the allocated page.  If we're redoing the operation, we have
	 * to get the page (creating it if it doesn't exist), and update its
	 * LSN.  If we're undoing the operation, we have to reset the page's
	 * LSN and put it on the free list.
	 *
	 * Fix up the metadata page.  If we're redoing the operation, we have
	 * to get the metadata page and update its LSN and its free pointer.
	 * If we're undoing the operation and the page was ever created, we put
	 * it on the freelist.
	 */
	pgno = PGNO_BASE_MD;
	if ((ret = __memp_fget(mpf, &pgno, 0, &meta)) != 0) {
		/* The metadata page must always exist on redo. */
		if (DB_REDO(op)) {
			ret = __db_pgerr(file_dbp, pgno, ret);
			goto out;
		} else
			goto done;
	}
	created = modified = 0;
	if ((ret = __memp_fget(mpf, &argp->pgno, 0, &pagep)) != 0) {
		/*
		 * We have to be able to identify if a page was newly
		 * created so we can recover it properly.  We cannot simply
		 * look for an empty header, because hash uses a pgin
		 * function that will set the header.  Instead, we explicitly
		 * try for the page without CREATE and if that fails, then
		 * create it.
		 */
		if ((ret = __memp_fget(
		    mpf, &argp->pgno, DB_MPOOL_CREATE, &pagep)) != 0) {
			if (ret == ENOSPC)
				goto do_meta;
			ret = __db_pgerr(file_dbp, argp->pgno, ret);
			goto out;
		}
		created = modified = 1;
	}

	/* Fix up the allocated page. */
	cmp_n = log_compare(lsnp, &LSN(pagep));
	cmp_p = log_compare(&LSN(pagep), &argp->page_lsn);

	/*
	 * If an inital allocation is aborted and then reallocated
	 * during an archival restore the log record will have
	 * an LSN for the page but the page will be empty.
	 */
	if (IS_ZERO_LSN(LSN(pagep)))
		cmp_p = 0;
	CHECK_LSN(op, cmp_p, &LSN(pagep), &argp->page_lsn, lsnp, argp->fileid,
	    argp->pgno);
	/*
	 * If we we rolled back this allocation previously during an
	 * archive restore, the page may have INIT_LSN from the limbo list.
	 * Another special case we have to handle is if we ended up with a
	 * page of all 0's which can happen if we abort between allocating a
	 * page in mpool and initializing it.  In that case, even if we're
	 * undoing, we need to re-initialize the page.
	 */
	if (DB_REDO(op) &&
	    (cmp_p == 0 ||
	    (IS_ZERO_LSN(argp->page_lsn) && IS_INIT_LSN(LSN(pagep))))) {
		/* Need to redo update described. */
		switch (argp->ptype) {
		case P_LBTREE:
		case P_LRECNO:
		case P_LDUP:
			level = LEAFLEVEL;
			break;
		default:
			level = 0;
			break;
		}
		P_INIT(pagep, file_dbp->pgsize,
		    argp->pgno, PGNO_INVALID, PGNO_INVALID, level, argp->ptype);

		pagep->lsn = *lsnp;
		modified = 1;
	} else if (DB_UNDO(op) && (cmp_n == 0 || created)) {
		/*
		 * This is where we handle the case of a 0'd page (pagep->pgno
		 * is equal to PGNO_INVALID).
		 * Undo the allocation, reinitialize the page and
		 * link its next pointer to the free list.
		 */
		P_INIT(pagep, file_dbp->pgsize,
		    argp->pgno, PGNO_INVALID, argp->next, 0, P_INVALID);

		pagep->lsn = argp->page_lsn;
		modified = 1;
	}

	/*
	 * If the page was newly created, put it on the limbo list.
	 */
	if (IS_ZERO_LSN(LSN(pagep)) &&
	    IS_ZERO_LSN(argp->page_lsn) && DB_UNDO(op)) {
		/* Put the page in limbo.*/
		if ((ret = __db_add_limbo(dbenv,
		    info, argp->fileid, argp->pgno, 1)) != 0)
			goto out;
	}

	if ((ret = __memp_fput(mpf, pagep, modified ? DB_MPOOL_DIRTY : 0)) != 0)
		goto out;
	pagep = NULL;

do_meta:
	/* Fix up the metadata page. */
	modified = 0;
	cmp_n = log_compare(lsnp, &LSN(meta));
	cmp_p = log_compare(&LSN(meta), &argp->meta_lsn);
	/*CHECK_LSN(op, cmp_p, &LSN(meta), &argp->meta_lsn); */
	if (cmp_p == 0 && DB_REDO(op)) {
		/* Need to redo update described. */
		LSN(meta) = *lsnp;
		meta->free = argp->next;
		modified = 1;
	} else if (cmp_n == 0 && DB_UNDO(op)) {
		/* Need to undo update described. */
		LSN(meta) = argp->meta_lsn;

		/*
		 * If the page has a zero LSN then its newly created
		 * and will go into limbo rather than directly on the
		 * free list.
		 */
		if (!IS_ZERO_LSN(argp->page_lsn))
			meta->free = argp->pgno;
		modified = 1;
	}

	/*
	 * Make sure that meta->last_pgno always reflects the largest page
	 * that we've ever allocated.
	 */
	if (argp->pgno > meta->last_pgno) {
		meta->last_pgno = argp->pgno;
		modified = 1;
	}

	if ((ret = __memp_fput(mpf, meta, modified ? DB_MPOOL_DIRTY : 0)) != 0)
		goto out;
	meta = NULL;

done:	*lsnp = argp->prev_lsn;
	ret = 0;

out:	if (pagep != NULL)
		(void)__memp_fput(mpf, pagep, 0);
	if (meta != NULL)
		(void)__memp_fput(mpf, meta, 0);
	if (ret == ENOENT && op == DB_TXN_BACKWARD_ALLOC)
		ret = 0;
	REC_CLOSE;
}

#include <poll.h>
int gbl_poll_in_pg_free_recover;

/*
 * __db_pg_free_recover_int --
 */
static int
__db_pg_free_recover_int(dbenv, argp, file_dbp, lsnp, mpf, op, data)
	DB_ENV *dbenv;
	__db_pg_freedata_args *argp;
	DB *file_dbp;
	DB_LSN *lsnp;
	DB_MPOOLFILE *mpf;
	db_recops op;
	int data;
{
	DBMETA *meta;
	DB_LSN copy_lsn;
	PAGE *pagep;
	db_pgno_t pgno;
	int cmp_n, cmp_p, modified, ret;

	meta = NULL;
	pagep = NULL;

        if (gbl_poll_in_pg_free_recover) poll(0, 0, 100);

        /*
         * Fix up the freed page.  If we're redoing the operation we get the
         * page and explicitly discard its contents, then update its LSN.  If
         * we're undoing the operation, we get the page and restore its header.
         * Create the page if necessary, we may be freeing an aborted
         * create.
         */
        if ((ret = __memp_fget(mpf, &argp->pgno, DB_MPOOL_CREATE, &pagep)) != 0)
		goto out;
	modified = 0;
	(void)__ua_memcpy(&copy_lsn, &LSN(argp->header.data), sizeof(DB_LSN));
	cmp_n = log_compare(lsnp, &LSN(pagep));
	cmp_p = log_compare(&LSN(pagep), &copy_lsn);
	CHECK_LSN(op, cmp_p, &LSN(pagep), &copy_lsn, lsnp, argp->fileid,
	    argp->pgno);
	if (DB_REDO(op) && (cmp_p == 0 || (IS_ZERO_LSN(copy_lsn) &&
		    log_compare(&LSN(pagep), &argp->meta_lsn) <= 0))) {
		/* Need to redo update described. */
		P_INIT(pagep, file_dbp->pgsize,
		    argp->pgno, PGNO_INVALID, argp->next, 0, P_INVALID);
		pagep->lsn = *lsnp;

		modified = 1;
	} else if (cmp_n == 0 && DB_UNDO(op)) {
		/* Need to undo update described. */
		memcpy(pagep, argp->header.data, argp->header.size);
		if (data)
			memcpy((u_int8_t*)pagep + pagep->hf_offset,
			     argp->data.data, argp->data.size);

		modified = 1;
	}
	if ((ret = __memp_fput(mpf, pagep, modified ? DB_MPOOL_DIRTY : 0)) != 0)
		goto out;
	pagep = NULL;

	/*
	 * Fix up the metadata page.  If we're redoing or undoing the operation
	 * we get the page and update its LSN and free pointer.
	 */
	pgno = PGNO_BASE_MD;
	if ((ret = __memp_fget(mpf, &pgno, 0, &meta)) != 0) {
		/* The metadata page must always exist. */
		ret = __db_pgerr(file_dbp, pgno, ret);
		goto out;
	}

	modified = 0;
	cmp_n = log_compare(lsnp, &LSN(meta));
	cmp_p = log_compare(&LSN(meta), &argp->meta_lsn);
	/*CHECK_LSN(op, cmp_p, &LSN(meta), &argp->meta_lsn); */
	if (cmp_p == 0 && DB_REDO(op)) {
		/* Need to redo the deallocation. */
		meta->free = argp->pgno;
		/*
		 * If this was a compensating transaction and
		 * we are a replica, then we never executed the
		 * original allocation which incremented meta->free.
		 */
		if (meta->last_pgno < meta->free)
			meta->last_pgno = meta->free;
		LSN(meta) = *lsnp;
		modified = 1;
	} else if (cmp_n == 0 && DB_UNDO(op)) {
		/* Need to undo the deallocation. */
		meta->free = argp->next;
		LSN(meta) = argp->meta_lsn;
		modified = 1;
	}
	if ((ret = __memp_fput(mpf, meta, modified ? DB_MPOOL_DIRTY : 0)) != 0)
		goto out;
	meta = NULL;

	*lsnp = argp->prev_lsn;
	ret = 0;

out:	if (pagep != NULL)
		(void)__memp_fput(mpf, pagep, 0);
	if (meta != NULL)
		(void)__memp_fput(mpf, meta, 0);

	return (ret);
}

/*
 * __db_pg_free_recover --
 *	Recovery function for pg_free.
 *
 * PUBLIC: int __db_pg_free_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_pg_free_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	__db_pg_free_args *argp;
	int ret;

	COMPQUIET(info, NULL);
	REC_PRINT(__db_pg_free_print);
	REC_INTRO(__db_pg_free_read, 1);

	ret = __db_pg_free_recover_int(dbenv,
	     (__db_pg_freedata_args *)argp, file_dbp, lsnp, mpf, op, 0);

done:
out:
	REC_CLOSE;

}

/*
 * __db_pg_new_recover --
 *	A new page from the file was put on the free list.
 * This record is only generated during a LIMBO_COMPENSATE.
 *
 * PUBLIC: int __db_pg_new_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_pg_new_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	__db_pg_free_args *argp;
	int ret;

	REC_PRINT(__db_pg_free_print);
	REC_INTRO(__db_pg_free_read, 1);
	COMPQUIET(op, 0);

	if ((ret =
	    __db_add_limbo(dbenv, info, argp->fileid, argp->pgno, 1)) == 0)
		*lsnp = argp->prev_lsn;

done:
out:
	REC_CLOSE;

}

/*
 * __db_pg_freedata_recover --
 *	Recovery function for pg_freedata.
 *
 * PUBLIC: int __db_pg_freedata_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_pg_freedata_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	__db_pg_freedata_args *argp;
	int ret;

	COMPQUIET(info, NULL);
	REC_PRINT(__db_pg_freedata_print);
	REC_INTRO(__db_pg_freedata_read, 1);

	ret = __db_pg_free_recover_int(dbenv, argp, file_dbp, lsnp, mpf, op, 1);

done:
out:
	REC_CLOSE;

}

/*
 * __db_cksum_recover --
 *	Recovery function for checksum failure log record.
 *
 * PUBLIC: int __db_cksum_recover __P((DB_ENV *,
 * PUBLIC:      DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_cksum_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__db_cksum_args *argp;

	int ret;

	COMPQUIET(info, NULL);
	COMPQUIET(lsnp, NULL);
	COMPQUIET(op, DB_TXN_ABORT);

	REC_PRINT(__db_cksum_print);

	if ((ret = __db_cksum_read(dbenv, dbtp->data, &argp)) != 0)
		return (ret);

	/*
	 * We had a checksum failure -- the only option is to run catastrophic
	 * recovery.
	 */
	if (F_ISSET(dbenv, DB_ENV_FATAL))
		ret = 0;
	else {
		__db_err(dbenv,
		    "Checksum failure requires catastrophic recovery");
		ret = __db_panic(dbenv, DB_RUNRECOVERY);
	}

	__os_free(dbenv, argp);
	return (ret);
}
/*
 * __db_pg_prepare_recover --
 *	Recovery function for pg_prepare.
 *
 * PUBLIC: int __db_pg_prepare_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_pg_prepare_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__db_pg_prepare_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	PAGE *pagep;
	int ret, t_ret;

	REC_PRINT(__db_pg_prepare_print);
	REC_INTRO(__db_pg_prepare_read, 1);

	mpf = file_dbp->mpf;

	/*
	 * If this made it into the limbo list a prepare time then
	 * it was a new free page allocated by an aborted subtransaction.
	 * Only that subtransaction could have toched the page.
	 * All other pages in the free list at this point are
	 * either of the same nature or were put there by this subtransactions
	 * other subtransactions that followed this one.  If
	 * they were put there by this subtransaction the log records
	 * of the following allocations will reflect that.
	 * Note that only one transaction could have had the
	 * metapage locked at the point of the crash.
	 * All this is to say that we can P_INIT this page without
	 * loosing other pages on the free list because they
	 * will be linked in by records earlier in the log for
	 * this transaction which we will roll back.
	 */
	if (op == DB_TXN_ABORT) {
		if ((ret = __memp_fget(
		    mpf, &argp->pgno, DB_MPOOL_CREATE, &pagep)) != 0)
			goto out;
		P_INIT(pagep, file_dbp->pgsize,
		    argp->pgno, PGNO_INVALID, PGNO_INVALID, 0, P_INVALID);
		ZERO_LSN(pagep->lsn);
		ret = __db_add_limbo(dbenv, info, argp->fileid, argp->pgno, 1);
		if ((t_ret =
		    __memp_fput(mpf, pagep, DB_MPOOL_DIRTY)) != 0 && ret == 0)
			ret = t_ret;
	}

done:	if (ret == 0)
		*lsnp = argp->prev_lsn;
out:	REC_CLOSE;
}

/* return size of a db file */
long long
__db_filesz(dbp)
	DB *dbp;
{
	db_pgno_t pgno = PGNO_BASE_MD;
	int rc;
	DBMETA *meta;
	off_t sz;

	rc = __memp_fget(dbp->mpf, &pgno, 0, &meta);
	if (rc)
		return rc;
	sz = meta->last_pgno * meta->pagesize;
	__memp_fput(dbp->mpf, meta, 0);
	return sz;
}

/*
** FIXME TODO XXX
** HI HI HI -- pg_prealloc AUTHOR.
** NEED TO FILL IN THE BLANKS BELOW.
** SIMPLY COPIED FROM GENERATED TEMPLATE
** SO tsk CAN BE BUILDed.
*/

/*
 * __db_pg_prealloc_recover --
 *	Recovery function for pg_prealloc.
 *
 * PUBLIC: int __db_pg_prealloc_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_pg_prealloc_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__db_pg_prealloc_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	PAGE *pagep;
	int cmp_n, cmp_p, modified, ret;

	REC_PRINT(__db_pg_prealloc_print);
	REC_INTRO(__db_pg_prealloc_read, 1);

	if ((ret = mpf->get(mpf, &argp->pgno, 0, &pagep)) != 0)
		if (DB_REDO(op)) {
			if ((ret = mpf->get(mpf,
			    &argp->pgno, DB_MPOOL_CREATE, &pagep)) != 0)
				goto out;
		} else {
			*lsnp = argp->prev_lsn;
			ret = 0;
			goto out;
		}

	modified = 0;
	cmp_n = log_compare(lsnp, &LSN(pagep));

	/*
	 * Use this when there is something like "pagelsn" in the argp
	 * structure.  Sometimes, you might need to compare meta-data
	 * lsn's instead.
	 *
	 * cmp_p = log_compare(&LSN(pagep), argp->pagelsn);
	 */
	if (cmp_p == 0 && DB_REDO(op)) {
		/* Need to redo update described. */
		modified = 1;
	} else if (cmp_n == 0 && !DB_REDO(op)) {
		/* Need to undo update described. */
		modified = 1;
	}
	if (ret = mpf->put(mpf, pagep, modified ? DB_MPOOL_DIRTY : 0))
		goto out;

	*lsnp = argp->prev_lsn;
	ret = 0;

done:
out:	REC_CLOSE;
}
