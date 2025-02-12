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
#include "dbinc/txn.h"
#include "logmsg.h"

static int __db_pg_free_recover_int __P((DB_ENV *,
	__db_pg_freedata_args *, DB *, DB_LSN *, DB_MPOOLFILE *, db_recops,
	int));

#include <stdlib.h>

extern int gbl_check_page_in_recovery;

/*
 * __db_addrem_undo_add_redo_del --
 * Undo add or redo del
 *
 * public: int __db_addrem_undo_add_redo_del
 * public:    __p((DBC *, __db_addrem_args *, PAGE *, db_recops, DB_LSN *));
 */
 int 
 __db_addrem_undo_add_redo_del(dbc, argp, pagep, op, lsnp)
 	DBC *dbc;
	__db_addrem_args *argp;
	PAGE *pagep;
	db_recops op;
	DB_LSN *lsnp;
{
	int ret = 0;

	if ((ret = __db_ditem(dbc, pagep, argp->indx, argp->nbytes)) != 0) {
		goto out;
	}

	if (DB_UNDO(op)) {
		LSN(pagep) = argp->pagelsn;
	} else {
		LSN(pagep) = *lsnp;
	}

out:
	return ret;
}

/*
 * __db_addrem_redo_add_undo_del --
 * Redo add or undo del
 *
 * public: int __db_addrem_redo_add_undo_del
 * public:    __p((DBC *, __db_addrem_args *, PAGE *, db_recops op, DB_LSN *lsnp));
 */
int 
__db_addrem_redo_add_undo_del(dbc, argp, pagep, op, lsnp)
 	DBC *dbc;
	__db_addrem_args *argp;
	PAGE *pagep;
	db_recops op;
	DB_LSN *lsnp;
{
	int ret;

	ret = 0;
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
	if ((ret = __db_pitem_opcode(dbc, pagep, argp->indx, argp->nbytes,
			argp->hdr.size == 0 ? NULL : &argp->hdr,
			argp->dbt.size == 0 ? NULL : &argp->dbt,
			argp->opcode)) != 0) {
		goto out;
	}
	
	if (DB_UNDO(op)) {
		LSN(pagep) = argp->pagelsn;
	} else {
		LSN(pagep) = *lsnp;
	}

out:
	return ret;
}

int
__db_addrem_verify_fileid(dbenv, dbp, lsnp, prevlsn, fileid)
	DB_ENV *dbenv;
	DB *dbp;
	DB_LSN *lsnp;
	DB_LSN *prevlsn;
	int32_t fileid;
{
	int ret = 0;
	u_int32_t type;
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
	normalize_rectype(&type);
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
		    lsnp->file, lsnp->offset, fileid, (char *)debug->key.data,
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
#if defined (UFID_HASH_DEBUG)
			logmsg(LOGMSG_USER, "t-%p %s ignoring failed memp_fget because undo\n",
					(void *)pthread_self(),__func__);
#endif
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
#if defined (UFID_HASH_DEBUG)
	logmsg(LOGMSG_USER, "td-%p %s (%s) lsn [%d:%d] pagelsn [%d:%d] op %d\n",
			(void *)pthread_self(), __func__, DB_REDO(op) ? "REDO" : "UNDO",
			lsnp->file, lsnp->offset, LSN(pagep).file, LSN(pagep).offset, op);
#endif

	if ((cmp_p == 0 && DB_REDO(op) && argp->opcode == DB_ADD_DUP) ||
	    (cmp_n == 0 && DB_UNDO(op) && IS_REM_OPCODE(argp->opcode))) {
		if ((ret = __db_addrem_redo_add_undo_del(dbc, argp, pagep, op, lsnp)) != 0) {
			goto out;
		}
		change = DB_MPOOL_DIRTY;
	} else if ((cmp_n == 0 && DB_UNDO(op) && argp->opcode == DB_ADD_DUP) ||
	    (cmp_p == 0 && DB_REDO(op) && IS_REM_OPCODE(argp->opcode))) {
		/* Need to undo an add, or redo a delete. */
		if ((ret = __db_addrem_undo_add_redo_del(dbc, argp, pagep, op, lsnp)) != 0) {
			goto out;
		}
		change = DB_MPOOL_DIRTY;
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
 * __db_big_undo_add_redo_del --
 * Undo big add or redo big del on the page with the data.
 *
 * PUBLIC: void __db_big_undo_add_redo_del
 * PUBLIC:    __P((PAGE *, __db_big_args *, db_recops, DB_LSN *));
 */
void __db_big_undo_add_redo_del(pagep, argp, op, lsnp)
	PAGE *pagep;
	__db_big_args *argp;
	db_recops op;
	DB_LSN *lsnp;
{
	LSN(pagep) = DB_REDO(op) ? *lsnp : argp->pagelsn;
}

/*
 * __db_big_redo_add_undo_del --
 * Redo big add or undo big del on the page with the data.
 * 
 * PUBLIC: void __db_big_redo_add_undo_del
 * PUBLIC:    __P((DB *, PAGE *, __db_big_args *, db_recops, DB_LSN *));
 */
void __db_big_redo_add_undo_del(file_dbp, pagep, argp, op, lsnp)
	DB *file_dbp;
	PAGE *pagep;
	__db_big_args *argp;
	db_recops op;
	DB_LSN * lsnp;
{
	P_INIT(pagep, file_dbp->pgsize, argp->pgno, argp->prev_pgno,
		argp->next_pgno, 0, P_OVERFLOW);
	OV_LEN(pagep) = argp->dbt.size;
	OV_REF(pagep) = 1;
	memcpy((u_int8_t *)pagep + P_OVERHEAD(file_dbp), argp->dbt.data,
		argp->dbt.size);
	PREV_PGNO(pagep) = argp->prev_pgno;
	LSN(pagep) = DB_REDO(op) ? *lsnp : argp->pagelsn;
}

/*
 * __db_big_prev_redo_add_undo_del --
 * Redo big add or undo big del on prev page.
 *
 * PUBLIC: void __db_big_prev_redo_add_undo_del
 * PUBLIC:    __P((PAGE *, __db_big_args *, db_recops, DB_LSN *));
 */
void __db_big_prev_redo_add_undo_del(pagep, argp, op, lsnp)
	PAGE *pagep;
	__db_big_args *argp;
	db_recops op;
	DB_LSN *lsnp;
{
	NEXT_PGNO(pagep) = argp->pgno;
	LSN(pagep) = DB_REDO(op) ? *lsnp : argp->prevlsn;
}

/*
 * __db_big_prev_redo_del_undo_add --
 * Redo big del or undo big add on prev page.
 *
 * PUBLIC: void __db_big_prev_redo_del_undo_add
 * PUBLIC:    __P((PAGE *, __db_big_args *, db_recops, DB_LSN *));
 */
void __db_big_prev_redo_del_undo_add(pagep, argp, op, lsnp)
	PAGE *pagep;
	__db_big_args *argp;
	db_recops op;
	DB_LSN *lsnp;
{
	NEXT_PGNO(pagep) = argp->next_pgno;
	LSN(pagep) = DB_REDO(op) ? *lsnp : argp->prevlsn;
}

/*
 * __db_big_next_redo --
 * Redo big on next page.
 *
 * PUBLIC: void __db_big_next_redo
 * PUBLIC:    __P((PAGE *, __db_big_args *, DB_LSN *));
 */
void __db_big_next_redo(pagep, argp, lsnp)
	PAGE *pagep;
	__db_big_args *argp;
	DB_LSN *lsnp;
{
	PREV_PGNO(pagep) = PGNO_INVALID;
	LSN(pagep) = *lsnp;
}

/*
 * __db_big_next_undo --
 * Undo big on next page.
 *
 * PUBLIC: void __db_big_next_undo
 * PUBLIC:    __P((PAGE *, __db_big_args *));
 */
void __db_big_next_undo(pagep, argp)
	PAGE *pagep;
	__db_big_args *argp;
{
	PREV_PGNO(pagep) = argp->pgno;
	LSN(pagep) = argp->nextlsn;
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
		__db_big_redo_add_undo_del(file_dbp, pagep, argp, op, lsnp);
		change = DB_MPOOL_DIRTY;
	} else if ((cmp_n == 0 && DB_UNDO(op) && argp->opcode == DB_ADD_BIG) ||
	    (cmp_p == 0 && DB_REDO(op) && argp->opcode == DB_REM_BIG)) {
		/*
		 * We are either undo-ing an add or redo-ing a delete.
		 * The page is about to be reclaimed in either case, so
		 * there really isn't anything to do here.
		 */
		__db_big_undo_add_redo_del(pagep, argp, op, lsnp);
		change = DB_MPOOL_DIRTY;
	}

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
			__db_big_prev_redo_add_undo_del(pagep, argp, op, lsnp);
			change = DB_MPOOL_DIRTY;
		} else if (cmp_n == 0 &&
		    DB_UNDO(op) && argp->opcode == DB_ADD_BIG) {
			/* Redo delete, undo add. */
			__db_big_prev_redo_del_undo_add(pagep, argp, op, lsnp);
			change = DB_MPOOL_DIRTY;
		}
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
			__db_big_next_redo(pagep, argp, lsnp);
			change = DB_MPOOL_DIRTY;
		} else if (cmp_n == 0 && DB_UNDO(op)) {
			__db_big_next_undo(pagep, argp);
			change = DB_MPOOL_DIRTY;
		}
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
 * __db_ovref_redo --
 * Redo ovref.
 *
 * PUBLIC: void __db_ovref_redo
 * PUBLIC:    __P((PAGE *, __db_ovref_args *, DB_LSN *));
 */
void __db_ovref_redo(PAGE *pagep, __db_ovref_args *argp, DB_LSN *lsnp)
{
	OV_REF(pagep) += argp->adjust;

	pagep->lsn = *lsnp;
}

/*
 * __db_ovref_undo --
 * Undo ovref.
 *
 * PUBLIC: void __db_ovref_undo
 * PUBLIC:    __P((PAGE *, __db_ovref_args *));
 */
void __db_ovref_undo(PAGE *pagep, __db_ovref_args *argp)
{
	OV_REF(pagep) -= argp->adjust;

	pagep->lsn = argp->lsn;
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
		__db_ovref_redo(pagep, argp, lsnp);
		modified = 1;
	} else if (log_compare(lsnp, &LSN(pagep)) == 0 && DB_UNDO(op)) {
		/* Need to undo update described. */
		__db_ovref_undo(pagep, argp);
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
 * __db_relink_next_add_undo_rem_redo --
 * Undo relink add or undo relink rem on next page.
 *
 * PUBLIC: void __db_relink_next_add_undo_rem_redo
 * PUBLIC:    __P((PAGE *, __db_relink_args *, db_recops, DB_LSN *));
 */
void __db_relink_next_add_undo_rem_redo(PAGE *pagep, __db_relink_args *argp, db_recops op, DB_LSN *lsnp) {
	pagep->prev_pgno = argp->prev;
	pagep->lsn = DB_REDO(op) ? *lsnp : argp->lsn_next;
}

/*
 * __db_relink_next_add_redo_rem_undo --
 * Redo relink add or undo relink rem on next page.
 *
 * PUBLIC: void __db_relink_next_add_redo_rem_undo
 * PUBLIC:    __P((PAGE *, __db_relink_args *, db_recops, DB_LSN *));
 */
void __db_relink_next_add_redo_rem_undo(PAGE *pagep, __db_relink_args *argp, db_recops op, DB_LSN *lsnp) {
	pagep->prev_pgno = argp->pgno;
	pagep->lsn = DB_REDO(op) ? *lsnp : argp->lsn_next;
}

/*
 * __db_relink_target_rem_undo --
 * Undo relink rem on relinked page.
 *
 * PUBLIC: void __db_relink_target_rem_undo
 * PUBLIC:    __P((PAGE *, __db_relink_args *));
 */
void __db_relink_target_rem_undo(PAGE *pagep, __db_relink_args *argp) {
	pagep->next_pgno = argp->next;
	pagep->prev_pgno = argp->prev;
	pagep->lsn = argp->lsn;
}

/*
 * __db_relink_target_rem_redo --
 * Redo relink rem on relinked page. 
 *
 * PUBLIC: void __db_relink_target_rem_redo
 * PUBLIC:    __P((PAGE *, DB_LSN *));
 */
void __db_relink_target_rem_redo(PAGE *pagep, DB_LSN *lsnp) {
	pagep->lsn = *lsnp;
}

/*
 * __db_relink_prev_rem_undo --
 * Undo relink rem on prev page.
 *
 * PUBLIC: void __db_relink_prev_rem_undo
 * PUBLIC:    __P((PAGE *, __db_relink_args *));
 */
void __db_relink_prev_rem_undo(PAGE *pagep, __db_relink_args *argp) {
	pagep->next_pgno = argp->pgno;
	pagep->lsn = argp->lsn_prev;
}

/*
 * __db_relink_prev_rem_redo --
 * Redo relink rem on prev page.
 *
 * PUBLIC: void __db_relink_prev_rem_redo
 * PUBLIC:    __P((PAGE *, __db_relink_args *, DB_LSN *));
 */
void __db_relink_prev_rem_redo(PAGE *pagep, __db_relink_args *argp, DB_LSN *lsnp) {
	pagep->next_pgno = argp->next;
	pagep->lsn = *lsnp;
}
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
		__db_relink_target_rem_redo(pagep, lsnp);
		modified = 1;
	} else if (log_compare(lsnp, &LSN(pagep)) == 0 && DB_UNDO(op)) {
		/* Undo the relink. */
		__db_relink_target_rem_undo(pagep, argp);
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
		__db_relink_next_add_undo_rem_redo(pagep, argp, op, lsnp);

		modified = 1;
	} else if ((argp->opcode == DB_REM_PAGE && cmp_n == 0 && DB_UNDO(op)) ||
	    (argp->opcode == DB_ADD_PAGE && cmp_p == 0 && DB_REDO(op))) {
		/* Undo the remove or redo the add. */
		__db_relink_next_add_redo_rem_undo(pagep, argp, op, lsnp);

		modified = 1;
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
		__db_relink_prev_rem_redo(pagep, argp, lsnp);

		modified = 1;
	} else if (log_compare(lsnp, &LSN(pagep)) == 0 && DB_UNDO(op)) {
		/* Undo the relink. */
		__db_relink_prev_rem_undo(pagep, argp);

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
 * __db_noop_redo --
 * Redo noop.
 *
 * PUBLIC: void __db_noop_redo 
 * PUBLIC:     __P((PAGE *, DB_LSN *));
 */
void __db_noop_redo(PAGE *pagep, DB_LSN *lsnp)
{
	LSN(pagep) = *lsnp;
}

/*
 * __db_noop_undo --
 * Undo noop.
 *
 * PUBLIC: void __db_noop_undo
 * PUBLIC:     __P((PAGE *, __db_noop_args *));
 */
void __db_noop_undo(PAGE *pagep, __db_noop_args *argp)
{
	LSN(pagep) = argp->prevlsn;
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
		__db_noop_redo(pagep, lsnp);
		change = DB_MPOOL_DIRTY;
	} else if (cmp_n == 0 && DB_UNDO(op)) {
		__db_noop_undo(pagep, argp);
		change = DB_MPOOL_DIRTY;
	}
	ret = __memp_fput(mpf, pagep, change);
	pagep = NULL;

done:	*lsnp = argp->prev_lsn;
out:	if (pagep != NULL)
		(void)__memp_fput(mpf, pagep, 0);
	REC_CLOSE;
}

void comdb2_cheapstack_sym(FILE *f, char *fmt, ...);

/*
 * __db_pg_alloc_target_undo --
 * Undo pg alloc on allocated page
 *
 * PUBLIC: void __db_pg_alloc_target_undo
 * PUBLIC:    __P((DB *, PAGE *, __db_pg_alloc_args *));
 */
void __db_pg_alloc_target_undo(DB *file_dbp, PAGE *pagep, __db_pg_alloc_args *argp) {
		/*
		 * This is where we handle the case of a 0'd page (pagep->pgno
		 * is equal to PGNO_INVALID).
		 * Undo the allocation, reinitialize the page and
		 * link its next pointer to the free list.
		 */
	P_INIT(pagep, file_dbp->pgsize,
		argp->pgno, PGNO_INVALID, argp->next, 0, P_INVALID);

	pagep->lsn = argp->page_lsn;
}

/*
 * __db_pg_alloc_target_redo --
 * Redo pg alloc on allocated page.
 *
 * PUBLIC: void __db_pg_alloc_target_redo
 * PUBLIC:    __P((DB *, PAGE *, __db_pg_alloc_args *, DB_LSN *));
 */
void __db_pg_alloc_target_redo(DB *file_dbp, PAGE *pagep, __db_pg_alloc_args *argp, DB_LSN *lsnp) {
	int level;

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
}

/*
 * __db_pg_alloc_meta_redo --
 * Redo updates made by pg alloc to meta page.
 *
 * PUBLIC: void __db___db_pg_alloc_meta_redo
 * PUBLIC:    __P((DBMETA *, __db_pg_alloc_args *, DB_LSN *));
 */
void __db_pg_alloc_meta_redo(DBMETA *meta, __db_pg_alloc_args *argp, DB_LSN *lsnp)
{
	LSN(meta) = *lsnp;
	meta->free = argp->next;
}

/*
 * __db_pg_alloc_meta_undo --
 * Undo updates made by pg alloc to meta page.
 *
 * PUBLIC: void __db___db_pg_alloc_meta_undo
 * PUBLIC:    __P((DB *, DBMETA *, __db_pg_alloc_args *));
 */
void __db_pg_alloc_meta_undo(DB *file_dbp, DBMETA *meta, __db_pg_alloc_args *argp) {
	LSN(meta) = argp->meta_lsn;

	/*
	 * If the page has a zero LSN then its newly created
	 * and will go into limbo rather than directly on the
	 * free list.
	 */
	if (!IS_ZERO_LSN(argp->page_lsn))
		meta->free = argp->pgno;
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
	int cmp_n, cmp_p, created, modified, ret;

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

#if defined (UFID_HASH_DEBUG)
	logmsg(LOGMSG_USER, "%s [%d:%d] %s dbp %p pagelsn [%d:%d]\n", __func__,
			lsnp->file, lsnp->offset, DB_REDO(op) ? "REDO" : "UNDO",
			file_dbp, pagep ? LSN(pagep).file : -1,
			pagep ? LSN(pagep).offset : -1);
#endif

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
		__db_pg_alloc_target_redo(file_dbp, pagep, argp, lsnp);
		modified = 1;
	} else if (DB_UNDO(op) && (cmp_n == 0 || created || (IS_ZERO_LSN(argp->page_lsn) && IS_ZERO_LSN(pagep->lsn)))) {
		__db_pg_alloc_target_undo(file_dbp, pagep, argp);
		modified = 1;
	}
#if defined (UFID_HASH_DEBUG)
	logmsg(LOGMSG_USER, "%s [%d:%d] %s dbp %p set pagelsn to [%d:%d]\n",
			__func__, lsnp->file, lsnp->offset, DB_REDO(op) ? "REDO" : "UNDO",
			file_dbp, LSN(pagep).file, LSN(pagep).offset);
#endif

	/*
	 * If the page was newly created, put it on the limbo list.
	 */
	if (IS_ZERO_LSN(LSN(pagep)) &&
		IS_ZERO_LSN(argp->page_lsn) && DB_UNDO(op)) {
		int found_prepared = __txn_is_prepared(dbenv, argp->txnid->utxnid);
		int is_prepared = (op == DB_TXN_BACKWARD_ROLL && found_prepared);

#if defined (DEBUG_PREPARE)
		comdb2_cheapstack_sym(stderr, "op is %d", op);
		logmsg(LOGMSG_USER, "is_prepared=%d, op=%d, txnid=%x, found-prepared=%d\n",
			is_prepared, op, argp->txnid->txnid, found_prepared);
#endif
		if (is_prepared) {
			logmsg(LOGMSG_INFO, "%s ignoring unresolved prepared txn %u\n", __func__,
                argp->txnid->txnid);
		} else if ((argp->type > 1000 && argp->type < 2000) || (argp->type > 3000)) {
			ret = __db_add_limbo_fid(dbenv, info, argp->ufid_fileid,
					argp->pgno, 1);
		} else {
			ret = __db_add_limbo(dbenv,
					info, argp->fileid, argp->pgno, 1);
		}

		if (ret != 0)
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
		__db_pg_alloc_meta_redo(meta, argp, lsnp);
		modified = 1;
	} else if (cmp_n == 0 && DB_UNDO(op)) {
		/* Need to undo update described. */
		__db_pg_alloc_meta_undo(file_dbp, meta, argp);
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
 * __db_pg_free_undo --
 * Undo pg free on freed page.
 *
 * PUBLIC: void __db_pg_free_undo
 * PUBLIC:    __P((PAGE *, __db_pg_freedata_args *, int));
 */
void
__db_pg_free_undo(PAGE *pagep, __db_pg_freedata_args *argp, int data)
{
	memcpy(pagep, argp->header.data, argp->header.size);
	if (data)
		memcpy((u_int8_t*)pagep + pagep->hf_offset,
			 argp->data.data, argp->data.size);
}

/*
 * __db_pg_free_redo --
 * Redo pg free on freed page.
 *
 * PUBLIC: void __db_pg_free_redo
 * PUBLIC:    __P((DB *, PAGE *, __db_pg_freedata_args *, DB_LSN *));
 */
void
__db_pg_free_redo(DB *file_dbp, PAGE *pagep, __db_pg_freedata_args *argp, DB_LSN *lsnp)
{
	P_INIT(pagep, file_dbp->pgsize,
		argp->pgno, PGNO_INVALID, argp->next, 0, P_INVALID);
	pagep->lsn = *lsnp;
}

/*
 * __db_pg_free_meta_undo --
 * Undo updates made by pg free to meta page.
 *
 * PUBLIC: void __db_pg_free_meta_undo
 * PUBLIC:    __P((DBMETA *, __db_pg_freedata_args *));
 */
void 
__db_pg_free_meta_undo(DBMETA *meta, __db_pg_freedata_args *argp)
{
	meta->free = argp->next;
	LSN(meta) = argp->meta_lsn;
}

/*
 * __db_pg_free_meta_redo --
 * Redo updates made by pg free to meta page.
 *
 * PUBLIC: void __db_pg_free_meta_redo
 * PUBLIC:    __P((DBMETA *, __db_pg_freedata_args *, DB_LSN *));
 */
void 
__db_pg_free_meta_redo(DBMETA *meta, __db_pg_freedata_args *argp, DB_LSN *lsnp)
{
	meta->free = argp->pgno;
	/*
	 * If this was a compensating transaction and
	 * we are a replica, then we never executed the
	 * original allocation which incremented meta->free.
	 */
	if (meta->last_pgno < meta->free)
		meta->last_pgno = meta->free;
	LSN(meta) = *lsnp;
}

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
	DB_REP *db_rep;
	REP *rep;
	int cmp_n, cmp_p, modified, ret;

	meta = NULL;
	pagep = NULL;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	if (gbl_poll_in_pg_free_recover && !rep->in_recovery) poll(0, 0, 100);

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
		__db_pg_free_redo(file_dbp, pagep, argp, lsnp);

		modified = 1;
	} else if (cmp_n == 0 && DB_UNDO(op)) {
		/* Need to undo update described. */
		__db_pg_free_undo(pagep, argp, data);

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
		__db_pg_free_meta_redo(meta, argp, lsnp);
		modified = 1;
	} else if (cmp_n == 0 && DB_UNDO(op)) {
		/* Need to undo the deallocation. */
		__db_pg_free_meta_undo(meta, argp);
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

	if ((argp->type > 1000 && argp->type < 2000) || (argp->type > 3000)) {
		if ((ret = __db_add_limbo_fid(dbenv, info, argp->ufid_fileid,
						argp->pgno, 1)) == 0)
			*lsnp = argp->prev_lsn;
	} else {
		if ((ret = __db_add_limbo(dbenv, info, argp->fileid, argp->pgno,
						1)) == 0)
			*lsnp = argp->prev_lsn;
	}

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
 * __db_pg_prepare_undo --
 * Undo pg prepare
 *
 * PUBLIC: int __db_pg_prepare_undo
 * PUBLIC:   __P((DB_ENV *, DB *, PAGE *, __db_pg_prepare_args *, int, void *));
 */
int __db_pg_prepare_undo(DB_ENV *dbenv, DB *file_dbp, PAGE *pagep, __db_pg_prepare_args *argp, int update_limbo, void *info)
{
	int ret;

	ret = 0;

	P_INIT(pagep, file_dbp->pgsize,
	    argp->pgno, PGNO_INVALID, PGNO_INVALID, 0, P_INVALID);
	ZERO_LSN(pagep->lsn);

	if (update_limbo) {
		if ((argp->type > 1000 && argp->type < 2000) || (argp->type > 3000)) {
			ret = __db_add_limbo_fid(dbenv, info, argp->ufid_fileid, argp->pgno, 1);
		} else {
			ret = __db_add_limbo(dbenv, info, argp->fileid, argp->pgno, 1);
		}
	}
	return ret;
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
		ret = __db_pg_prepare_undo(dbenv, file_dbp, pagep, argp, 1, info);
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
	int cmp_n, cmp_p = 0, modified, ret;

	REC_PRINT(__db_pg_prealloc_print);
	REC_INTRO(__db_pg_prealloc_read, 1);

	if ((ret = mpf->get(mpf, &argp->pgno, 0, &pagep)) != 0) {
		if (DB_REDO(op)) {
			if ((ret = mpf->get(mpf,
			    &argp->pgno, DB_MPOOL_CREATE, &pagep)) != 0)
				goto out;
		} else {
			*lsnp = argp->prev_lsn;
			ret = 0;
			goto out;
		}
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
	if ((ret = mpf->put(mpf, pagep, modified ? DB_MPOOL_DIRTY : 0)) != 0)
		goto out;

	*lsnp = argp->prev_lsn;
	ret = 0;

done:
out:	REC_CLOSE;
}

extern int gbl_pgmv_verbose;

typedef struct page_db_lsn {
	db_pgno_t pgno;
	DB_LSN *lsnp;
} PAGE_DB_LSN;

static int
pgno_cmp(const void *x, const void *y)
{
	return ((*(db_pgno_t *)x) - (*(db_pgno_t *)y));
}

/*
 * __db_rebuild_freelist_recover --
 *	Recovery function for truncate_freelist.
 *
 * PUBLIC: int __db_rebuild_freelist_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_rebuild_freelist_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__db_rebuild_freelist_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB *dbp;
	DBMETA *meta;
	DB_MPOOLFILE *mpf;
	PAGE *pagep;
	db_pgno_t pgno, *pglist, next_pgno;
	DB_LSN *pglsnlist;
	PAGE_DB_LSN *sorted;
	int cmp_n, cmp_p, cmp_pc, modified, ret;
	int check_page = gbl_check_page_in_recovery;
	size_t npages, ii, notch;

	meta = NULL;

	REC_PRINT(__db_rebuild_freelist_print);
	REC_INTRO(__db_rebuild_freelist_read, 0);

	dbp = dbc->dbp;

	if ((ret = __memp_fget(mpf, &argp->meta_pgno, 0, &meta)) != 0) {
		/* The metadata page must always exist on redo. */
		if (DB_REDO(op)) {
			ret = __db_pgerr(file_dbp, argp->meta_pgno, ret);
			goto out;
		} else {
			goto done;
		}
	}

	if (check_page) {
		__dir_pg(mpf, argp->meta_pgno, (u_int8_t *)meta, 0);
		__dir_pg(mpf, argp->meta_pgno, (u_int8_t *)meta, 1);
	}

	modified = 0;
	cmp_n = log_compare(lsnp, &LSN(meta));
	cmp_p = log_compare(&LSN(meta), &argp->meta_lsn);
	CHECK_LSN(op, cmp_p, &LSN(meta), &argp->meta_lsn, lsnp, argp->fileid, argp->meta_pgno);

	npages = argp->fl.size / sizeof(db_pgno_t);

	pglist = argp->fl.data;
	pglsnlist = argp->fllsn.data;
	sorted = NULL;

	notch = argp->notch;

	/* convert page lsn to host order */
	for (ii = 0; ii != npages; ++ii) {
		pglist[ii] = ntohl(pglist[ii]);
		pglsnlist[ii].file = ntohl(pglsnlist[ii].file);
		pglsnlist[ii].offset = ntohl(pglsnlist[ii].offset);
	}

	if (DB_REDO(op)) {
		if ((ret = __os_malloc(dbenv, sizeof(PAGE_DB_LSN) * npages, &sorted)) != 0)
			goto out;
		for (ii = 0; ii != npages; ++ii) {
			sorted[ii].pgno = pglist[ii];
			sorted[ii].lsnp = &pglsnlist[ii];
		}
		qsort(sorted, npages, sizeof(PAGE_DB_LSN), pgno_cmp);

		if (gbl_pgmv_verbose) {
			/* sorted[notch] is where in the freelist we can safely truncate. */
			if (notch == npages) {
				logmsg(LOGMSG_WARN, "can't truncate: last free page %u last pg %u\n", sorted[notch - 1].pgno, argp->last_pgno);
			} else {
				logmsg(LOGMSG_WARN, "last pgno %u truncation point (array index) %zu pgno %u\n", argp->last_pgno, notch, sorted[notch].pgno);
			}
		}

		/* rebuild the freelist, in page order */
		for (ii = 0; ii != notch; ++ii) {
			pgno = sorted[ii].pgno;
			if ((ret = __memp_fget(mpf, &pgno, 0, &pagep)) != 0) {
				__db_pgerr(dbp, pgno, ret);
				goto out;
			}

			cmp_pc = log_compare(&LSN(pagep), sorted[ii].lsnp);
			CHECK_LSN(op, cmp_pc, &LSN(pagep), sorted[ii].lsnp, lsnp, argp->fileid, pgno);
			if (cmp_pc == 0) {
				NEXT_PGNO(pagep) = (ii == notch - 1) ? argp->end_pgno : sorted[ii + 1].pgno;
				LSN(pagep) = *lsnp;
				if ((ret = __memp_fput(mpf, pagep, DB_MPOOL_DIRTY)) != 0)
					goto out;
			} else if ((ret = __memp_fput(mpf, pagep, 0)) != 0) {
				goto out;
			}
		}

		/* Discard pages to be truncated from buffer pool */
		for (ii = notch; ii != npages; ++ii) {
			pgno = sorted[ii].pgno;
			if ((ret = __memp_fget(mpf, &pgno, DB_MPOOL_PROBE, &pagep)) != 0)
				continue;
			/*
			 * mark the page clean and discard it from bufferpool.
			 * we don't want memp_sync to accidentally flush the page
			 * after we truncate, that would create a hole in the file.
			 */
			if ((ret = __memp_fput(mpf, pagep, DB_MPOOL_CLEAN | DB_MPOOL_DISCARD)) != 0)
				goto out;
		}

		if (cmp_p == 0) {
			/*
			 * Point the freelist to the smallest free page passed to us.
			 * If all pages in this range can be truncated, instead, point
			 * the freelist to the first free page after this range. It can
			 * be PGNO_INVALID if there is no more free page after this range.
			 */
			meta->free = notch > 0 ? sorted[0].pgno : argp->end_pgno;
			modified = 1;
			LSN(meta) = *lsnp;
		}
	} else if (DB_UNDO(op)) {
		/* undo freelist */
		for (ii = 0; ii != npages; ++ii) {
			pgno = pglist[ii];
			next_pgno = (ii == npages - 1) ? argp->end_pgno : pglist[ii + 1];
			if ((ret = __memp_fget(mpf, &pgno, 0, &pagep)) != 0)
				goto out;

			/*
			 * if we need to undo this free page, or the page is recreated from
			 * undoing a resize log record (in which case the page lsn will be zero),
			 * fix the free page's next reference, and set its page lsn correctly.
			 */
			if (log_compare(lsnp, &LSN(pagep)) == 0 || IS_ZERO_LSN(LSN(pagep))) {
				P_INIT(pagep, dbp->pgsize, pgno, PGNO_INVALID, next_pgno, 0, P_INVALID);
				LSN(pagep) = pglsnlist[ii];
				if ((ret = __memp_fput(mpf, pagep, DB_MPOOL_DIRTY)) != 0)
					goto out;
			} else if ((ret = __memp_fput(mpf, pagep, 0)) != 0) {
				goto out;
			}
		}

		/* undo metapage */
		if (cmp_n == 0) {
			meta->free = pglist[0];
			LSN(meta) = argp->meta_lsn;
			modified = 1;
		}
	}

	if (check_page) {
		__dir_pg(mpf, argp->meta_pgno, (u_int8_t *)meta, 0);
		__dir_pg(mpf, argp->meta_pgno, (u_int8_t *)meta, 1);
	}

	if ((ret = __memp_fput(mpf, meta, modified ? DB_MPOOL_DIRTY : 0)) != 0)
		goto out;
	meta = NULL;

done:	*lsnp = argp->prev_lsn;
	ret = 0;

out:	
	__os_free(dbenv, sorted);
	if (meta != NULL)
		(void)__memp_fput(mpf, meta, 0);
	REC_CLOSE;
}

/*
 * __db_pg_swap_recover --
 *	Recovery function for pg_swap.
 *
 * PUBLIC: int __db_pg_swap_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_pg_swap_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__db_pg_swap_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;

	PAGE *pagep,		/* old page */
	     *pp,			/* parent page */
	     *newp,			/* new page */
	     *nextp,		/* next page */
	     *prevp;		/* previous page */
	int hmodified,		/* is old page modified */
	    ppmodified, 	/* is parent page modified */
	    newmodified,	/* is new page modified */
	    nhmodified,		/* is next page modified */
	    phmodified;		/* is previous page modified */

	int cmp_n, cmp_p;
	int ret, t_ret;
	int check_page = gbl_check_page_in_recovery;

	pagep = pp = newp = nextp = prevp = NULL;
	hmodified = ppmodified = newmodified = nhmodified = phmodified = 0;

	REC_PRINT(__db_pg_swap_print);
	REC_INTRO(__db_pg_swap_read, 0);

	if ((ret = __memp_fget(mpf, &argp->pgno, 0, &pagep)) != 0) {
		ret = __db_pgerr(file_dbp, argp->pgno, ret);
		pagep = NULL;
		goto out;
	}

	if (check_page) {
		__dir_pg(mpf, argp->pgno, (u_int8_t *)pagep, 0);
		__dir_pg(mpf, argp->pgno, (u_int8_t *)pagep, 1);
    }

	if (gbl_pgmv_verbose) {
		logmsg(LOGMSG_WARN, "replacing pgno %u with newp %u before lsn %d:%d, "
				"page lsn %d:%d, after lsn %d:%d\n",
				argp->pgno, argp->new_pgno, argp->lsn.file, argp->lsn.offset,
				LSN(pagep).file, LSN(pagep).offset, lsnp->file, lsnp->offset);
	}

	if (DB_REDO(op)) {

		if (gbl_pgmv_verbose) {
			logmsg(LOGMSG_WARN, "%s: starting redo %u\n", __func__, argp->pgno);
		}

		/*
		 * redo in the following order:
		 *	- zap old page
		 *	- copy content to new page
		 *	- relink sibling
		 *	- update reference in parent page
		 */

		/* redo old page */
		cmp_p = log_compare(&LSN(pagep), &argp->lsn);
		CHECK_LSN(op, cmp_p, &LSN(pagep), &argp->lsn, lsnp, argp->fileid, argp->pgno);
		if (cmp_p == 0) {
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_WARN, "%s: redo old page %u: emptying this page\n",
						__func__, argp->pgno);
			}
			HOFFSET(pagep) = file_dbp->pgsize;
			NUM_ENT(pagep) = 0;
			CLR_PREFIX(pagep);
			LSN(pagep) = *lsnp;
			hmodified = 1;
		}

		/* redo new page */
		if ((ret = __memp_fget(mpf, &argp->new_pgno, 0, &newp)) != 0) {
			ret = __db_pgerr(file_dbp, argp->new_pgno, ret);
			newp = NULL;
			goto out;
		}
		cmp_p = log_compare(&LSN(newp), &argp->new_pglsn);
		CHECK_LSN(op, cmp_p, &LSN(newp), &argp->new_pglsn, lsnp, argp->fileid, argp->new_pgno);
		if (cmp_p == 0) {
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_WARN, "%s: redo new page %u: moving content to this page\n",
						__func__, argp->new_pgno);
			}
			memcpy(newp, argp->hdr.data, argp->hdr.size);
			memcpy((u_int8_t *)newp + HOFFSET(newp), argp->data.data, argp->data.size);
			PGNO(newp) = argp->new_pgno;
			LSN(newp) = *lsnp;
			newmodified = 1;
		}

		/* redo next page */
		if (argp->next_pgno != PGNO_INVALID) {
			if ((ret = __memp_fget(mpf, &argp->next_pgno, 0, &nextp)) != 0) {
				ret = __db_pgerr(file_dbp, argp->next_pgno, ret);
				nextp = NULL;
				goto out;
			}
			cmp_p = log_compare(&LSN(nextp), &argp->next_pglsn);
			CHECK_LSN(op, cmp_p, &LSN(nextp), &argp->next_pglsn, lsnp, argp->fileid, argp->next_pgno);
			if (cmp_p == 0) {
				if (gbl_pgmv_verbose) {
					logmsg(LOGMSG_WARN, "%s: relink %u<-%u\n", __func__, argp->new_pgno, argp->next_pgno);
				}
				PREV_PGNO(nextp) = argp->new_pgno;
				LSN(nextp) = *lsnp;
				nhmodified = 1;
			}
		}

		/* redo previous page */
		if (argp->prev_pgno != PGNO_INVALID) {
			if ((ret = __memp_fget(mpf, &argp->prev_pgno, 0, &prevp)) != 0) {
				ret = __db_pgerr(file_dbp, argp->prev_pgno, ret);
				prevp = NULL;
				goto out;
			}
			cmp_p = log_compare(&LSN(prevp), &argp->prev_pglsn);
			CHECK_LSN(op, cmp_p, &LSN(prevp), &argp->prev_pglsn, lsnp, argp->fileid, argp->prev_pgno);
			if (cmp_p == 0) {
				if (gbl_pgmv_verbose) {
					logmsg(LOGMSG_WARN, "%s: relink %u->%u\n", __func__, argp->prev_pgno, argp->new_pgno);
				}
				NEXT_PGNO(prevp) = argp->new_pgno;
				LSN(prevp) = *lsnp;
				phmodified = 1;
			}
		}

		/* redo parent page */
		if (argp->parent_pgno != PGNO_INVALID) {
			if ((ret = __memp_fget(mpf, &argp->parent_pgno, 0, &pp)) != 0) {
				ret = __db_pgerr(file_dbp, argp->parent_pgno, ret);
				pp = NULL;
				goto out;
			}
			cmp_p = log_compare(&LSN(pp), &argp->parent_pglsn);
			CHECK_LSN(op, cmp_p, &LSN(pp), &argp->parent_pglsn, lsnp, argp->fileid, argp->parent_pgno);
			if (cmp_p == 0) {
				if (gbl_pgmv_verbose) {
					logmsg(LOGMSG_WARN, "%s: redo parent %u reference to %u\n", __func__, argp->parent_pgno, argp->new_pgno);
				}
				GET_BINTERNAL(dbc->dbp, pp, argp->pref_indx)->pgno = argp->new_pgno;
				LSN(pp) = *lsnp;
				ppmodified = 1;
			}
		}
	} else if (DB_UNDO(op)) {
		if (gbl_pgmv_verbose) {
			logmsg(LOGMSG_WARN, "%s: starting undo %u\n", __func__, argp->pgno);
		}

		/*
		 * undo in the following order:
		 *	- undo reference in parent page
		 *	- relink sibling to old page
		 *	- copy content back to old page
		 */

		/* undo parent */
		if (argp->parent_pgno != PGNO_INVALID) {
			if ((ret = __memp_fget(mpf, &argp->parent_pgno, 0, &pp)) != 0) {
				ret = __db_pgerr(file_dbp, argp->parent_pgno, ret);
				pp = NULL;
				goto out;
			}
			cmp_n = log_compare(lsnp, &LSN(pp));
			if (cmp_n == 0) {
				if (gbl_pgmv_verbose) {
					logmsg(LOGMSG_WARN, "%s: undo parent %u reference to %u\n", __func__, argp->parent_pgno, argp->pgno);
				}
				GET_BINTERNAL(dbc->dbp, pp, argp->pref_indx)->pgno = argp->pgno;
				LSN(pp) = argp->parent_pglsn;
				ppmodified = 1;
			}
		}

		/* undo previous */
		if (argp->prev_pgno != PGNO_INVALID) {
			if ((ret = __memp_fget(mpf, &argp->prev_pgno, 0, &prevp)) != 0) {
				ret = __db_pgerr(file_dbp, argp->prev_pgno, ret);
				prevp = NULL;
				goto out;
			}
			cmp_n = log_compare(lsnp, &LSN(prevp));
			if (cmp_n == 0) {
				if (gbl_pgmv_verbose) {
					logmsg(LOGMSG_USER, "%s: relink %u->%u\n", __func__, argp->prev_pgno, argp->pgno);
				}
				NEXT_PGNO(prevp) = argp->pgno;
				LSN(prevp) = argp->prev_pglsn;
				phmodified = 1;
			}
		}

		/* undo next */
		if (argp->next_pgno != PGNO_INVALID) {
			if ((ret = __memp_fget(mpf, &argp->next_pgno, 0, &nextp)) != 0) {
				ret = __db_pgerr(file_dbp, argp->next_pgno, ret);
				nextp = NULL;
				goto out;
			}
			cmp_n = log_compare(lsnp, &LSN(nextp));
			if (cmp_n == 0) {
				if (gbl_pgmv_verbose) {
					logmsg(LOGMSG_USER, "%s: relink %u<-%u\n", __func__, argp->pgno, argp->next_pgno);
				}
				PREV_PGNO(nextp) = argp->pgno;
				LSN(nextp) = argp->next_pglsn;
				nhmodified = 1;
			}
		}


		/* undo new page */
		if ((ret = __memp_fget(mpf, &argp->new_pgno, 0, &newp)) != 0) {
			ret = __db_pgerr(file_dbp, argp->new_pgno, ret);
			newp = NULL;
			goto out;
		}
		cmp_n = log_compare(lsnp, &LSN(newp));
		if (cmp_n == 0) {
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_USER, "%s: undo new page %u: emptying this page\n", __func__, argp->new_pgno);
			}
			HOFFSET(newp) = file_dbp->pgsize;
			NUM_ENT(newp) = 0;
			CLR_PREFIX(newp);
			LSN(newp) = argp->new_pglsn;
			newmodified = 1;
		}

		/* undo old */
		cmp_n = log_compare(lsnp, &LSN(pagep));
		if (cmp_n == 0) {
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_USER, "%s: undo old page %u: moving content back to this page\n",
						__func__, argp->pgno);
			}
			memcpy(pagep, argp->hdr.data, argp->hdr.size);
			memcpy((u_int8_t *)pagep + HOFFSET(pagep), argp->data.data, argp->data.size);
			DB_ASSERT(PGNO(pagep) == argp->pgno);
			LSN(pagep) = argp->lsn;
			hmodified = 1;
		}
	}

	if (check_page) {
		__dir_pg(mpf, argp->pgno, (u_int8_t *)pagep, 0);
		__dir_pg(mpf, argp->pgno, (u_int8_t *)pagep, 1);
	}

	if ((ret = __memp_fput(mpf, pagep, hmodified ? DB_MPOOL_DIRTY : 0)) != 0)
		goto out;
	pagep = NULL;

done:	*lsnp = argp->prev_lsn;
	ret = 0;
out:	/* release all pages */
	if (pagep != NULL && (t_ret = __memp_fput(mpf, pagep, hmodified ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if (pp != NULL && (t_ret = __memp_fput(mpf, pp, ppmodified ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if (newp != NULL && (t_ret = __memp_fput(mpf, newp, newmodified ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if (nextp != NULL && (t_ret = __memp_fput(mpf, nextp, nhmodified ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if (prevp != NULL && (t_ret = __memp_fput(mpf, prevp, phmodified ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	REC_CLOSE;
}

/*
 * __db_resize_recover --
 *	Recovery function for truncate_freelist.
 *
 * PUBLIC: int __db_resize_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_resize_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__db_resize_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DBMETA *meta;
	DB_MPOOLFILE *mpf;
	PAGE *h;

	int cmp_n, cmp_p, modified, ret;
	int check_page = gbl_check_page_in_recovery;

	db_pgno_t pgno, next_pgno;

	h = NULL;
	meta = NULL;
	modified = 0;

	REC_PRINT(__db_resize_print);
	REC_INTRO(__db_resize_read, 0);

	/* metapage must exist */
	if ((ret = __memp_fget(mpf, &argp->meta_pgno, 0, &meta)) != 0) {
        meta = NULL;
		ret = __db_pgerr(file_dbp, argp->meta_pgno, ret);
		goto out;
	}

	if (check_page) {
		__dir_pg(mpf, argp->meta_pgno, (u_int8_t *)meta, 0);
		__dir_pg(mpf, argp->meta_pgno, (u_int8_t *)meta, 1);
	}

	cmp_n = log_compare(lsnp, &LSN(meta));
	cmp_p = log_compare(&LSN(meta), &argp->meta_lsn);

	DB_ASSERT(argp->newlast <= argp->oldlast);

	if (cmp_p == 0 && DB_REDO(op)) {
		for (pgno = argp->newlast + 1; pgno <= argp->oldlast; ++pgno) {
			/* If we don't find this page in bufferpool, this is fine. */
			if ((ret = __memp_fget(mpf, &pgno, DB_MPOOL_PROBE, &h)) != 0)
				continue;
			/* If we find the page in bufferpool, mark clean and discard.
			 * We don't want memp_sync to write the page after we truncate,
			 * that may extend the file by mistake */
			ret = __memp_fput(mpf, h, DB_MPOOL_CLEAN | DB_MPOOL_DISCARD);
			h = NULL;
			if (ret != 0)
				goto out;
		}
		/* fixing last pgno on metapage */
		meta->last_pgno = argp->newlast;
		/* notify bufferpool */
		__memp_resize(mpf, argp->newlast);
		LSN(meta) = *lsnp;
		modified = 1;
	} else if (cmp_n == 0 && DB_UNDO(op)) {
		/* notify bufferpool */
		__memp_resize(mpf, argp->oldlast);
		/* fixing last pgno on metapage */
		meta->last_pgno = argp->oldlast;
		for (pgno = argp->newlast + 1; pgno <= argp->oldlast; ++pgno) {
			/* If we can't retrieve this page in bufferpool, this is NOT fine. */
			if ((ret = __memp_fget(mpf, &pgno, DB_MPOOL_CREATE, &h)) != 0) {
				ret = __db_pgerr(file_dbp, pgno, ret);
				goto out;
			}
			/*
			 * reinit the page, we'll fix the page lsn and next-page reference
			 * in rebuild_freelist_recover().
			 */
			next_pgno = (pgno == argp->oldlast) ? PGNO_INVALID : (pgno + 1);
			P_INIT(h, file_dbp->pgsize, pgno, PGNO_INVALID, next_pgno, 0, P_INVALID);
			ret = __memp_fput(mpf, h, DB_MPOOL_DIRTY);
			h = NULL;
			if (ret != 0)
				goto out;
		}
		LSN(meta) = *lsnp;
		modified = 1;
	}
done:	*lsnp = argp->prev_lsn;
	ret = 0;

out:
	if (meta != NULL)
		(void)__memp_fput(mpf, meta, modified ? DB_MPOOL_DIRTY : 0);
	if (h != NULL)
		(void)__memp_fput(mpf, h, 0);
	REC_CLOSE;
}


/*
 * __db_pg_swap_overflow_recover --
 *	Recovery function for truncate_freelist.
 *
 * PUBLIC: int __db_pg_swap_overflow_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_pg_swap_overflow_recover(dbenv, dbtp, lsnp, op, info)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__db_pg_swap_overflow_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;

	PAGE *oh, *poh, *noh, *h, *newoh;
	BOVERFLOW *bo;

	int check_page = gbl_check_page_in_recovery;
	int cmp_n, cmp_p;
	int ret, t_ret;
	int ohmodified, pohmodified, nohmodified, hmodified, newohmodified;

	oh = poh = noh = h = newoh = NULL;
	ohmodified = pohmodified = nohmodified = hmodified = newohmodified = 0;

	REC_PRINT(__db_pg_swap_overflow_print);
	REC_INTRO(__db_pg_swap_overflow_read, 0);

	if ((ret = __memp_fget(mpf, &argp->pgno, 0, &oh)) != 0) {
		ret = __db_pgerr(file_dbp, argp->pgno, ret);
		oh = NULL;
		goto out;
	}

	if (argp->next_pgno != PGNO_INVALID && (ret = __memp_fget(mpf, &argp->next_pgno, 0, &noh)) != 0) {
		ret = __db_pgerr(file_dbp, argp->next_pgno, ret);
		noh = NULL;
		goto out;
	}

	if (argp->prev_pgno != PGNO_INVALID && (ret = __memp_fget(mpf, &argp->prev_pgno, 0, &poh)) != 0) {
		ret = __db_pgerr(file_dbp, argp->prev_pgno, ret);
		poh = NULL;
		goto out;
	}

	if (argp->main_pgno != PGNO_INVALID && (ret = __memp_fget(mpf, &argp->main_pgno, 0, &h)) != 0) {
		ret = __db_pgerr(file_dbp, argp->main_pgno, ret);
		h = NULL;
		goto out;
	}

	if ((ret = __memp_fget(mpf, &argp->new_pgno, 0, &newoh)) != 0) {
		ret = __db_pgerr(file_dbp, argp->new_pgno, ret);
		newoh = NULL;
		goto out;
	}

	if (check_page) {
		__dir_pg(mpf, argp->pgno, (u_int8_t *)oh, 0);
		__dir_pg(mpf, argp->pgno, (u_int8_t *)oh, 1);
		__dir_pg(mpf, argp->pgno, (u_int8_t *)newoh, 0);
		__dir_pg(mpf, argp->pgno, (u_int8_t *)newoh, 1);
		if (noh != NULL) {
			__dir_pg(mpf, argp->pgno, (u_int8_t *)noh, 0);
			__dir_pg(mpf, argp->pgno, (u_int8_t *)noh, 1);
		}
		if (poh != NULL) {
			__dir_pg(mpf, argp->pgno, (u_int8_t *)poh, 0);
			__dir_pg(mpf, argp->pgno, (u_int8_t *)poh, 1);
		}
		if (h != NULL) {
			__dir_pg(mpf, argp->pgno, (u_int8_t *)h, 0);
			__dir_pg(mpf, argp->pgno, (u_int8_t *)h, 1);
		}
	}

	if (gbl_pgmv_verbose)
		logmsg(LOGMSG_WARN, "replacing overflow pgno %u with newp %u before lsn %d:%d, "
				"page lsn %d:%d, after lsn %d:%d\n",
				argp->pgno, argp->new_pgno, argp->lsn.file, argp->lsn.offset,
				LSN(oh).file, LSN(oh).offset, lsnp->file, lsnp->offset);

	if (DB_REDO(op)) {
		if (gbl_pgmv_verbose)
			logmsg(LOGMSG_WARN, "%s: starting undo %u\n", __func__, argp->pgno);

		cmp_p = log_compare(&LSN(oh), &argp->lsn);
		CHECK_LSN(op, cmp_p, &LSN(oh), &argp->lsn, lsnp, argp->fileid, argp->pgno);
		if (cmp_p == 0) {
			if (gbl_pgmv_verbose)
				logmsg(LOGMSG_WARN, "%s: redo old page %u: emptying this page\n", __func__, argp->pgno);
			OV_LEN(oh) = 0;
			LSN(oh) = *lsnp;
			ohmodified = 1;
		}

		cmp_p = log_compare(&LSN(newoh), &argp->new_pglsn);
		CHECK_LSN(op, cmp_p, &LSN(newoh), &argp->new_pglsn, lsnp, argp->fileid, argp->new_pgno);
		if (cmp_p == 0) {
			if (gbl_pgmv_verbose)
				logmsg(LOGMSG_WARN, "%s: redo new page %u\n", __func__, argp->new_pgno);
			memcpy(newoh, argp->data.data, argp->data.size);
			PGNO(newoh) = argp->new_pgno;
			LSN(newoh) = *lsnp;
			ohmodified = 1;
		}
		
		if (noh != NULL) {
			cmp_p = log_compare(&LSN(noh), &argp->next_pglsn);
			CHECK_LSN(op, cmp_p, &LSN(noh), &argp->next_pglsn, lsnp, argp->fileid, argp->next_pgno);
			if (cmp_p == 0) {
				if (gbl_pgmv_verbose)
					logmsg(LOGMSG_WARN, "%s: relink %u<-%u\n", __func__, argp->new_pgno, argp->next_pgno);
				PREV_PGNO(noh) = argp->new_pgno;
				LSN(noh) = *lsnp;
				nohmodified = 1;
			}
		}

		if (poh != NULL) {
			cmp_p = log_compare(&LSN(poh), &argp->prev_pglsn);
			CHECK_LSN(op, cmp_p, &LSN(poh), &argp->prev_pglsn, lsnp, argp->fileid, argp->prev_pgno);
			if (cmp_p == 0) {
				if (gbl_pgmv_verbose)
					logmsg(LOGMSG_WARN, "%s: relink %u->%u\n", __func__, argp->prev_pgno, argp->new_pgno);
				NEXT_PGNO(poh) = argp->new_pgno;
				LSN(poh) = *lsnp;
				pohmodified = 1;
			}
		} else if (h != NULL) {
			cmp_p = log_compare(&LSN(h), &argp->main_pglsn);
			CHECK_LSN(op, cmp_p, &LSN(h), &argp->main_pglsn, lsnp, argp->fileid, argp->main_pgno);
			if (cmp_p == 0) {
				if (gbl_pgmv_verbose)
					logmsg(LOGMSG_WARN, "%s: redo main page %u ovfl page to %u\n", __func__, argp->main_pgno, argp->new_pgno);
				bo = GET_BOVERFLOW(file_dbp, h, argp->main_indx);
				bo->pgno = argp->new_pgno;
				LSN(h) = *lsnp;
				hmodified = 1;
			}
		}
	} else if (DB_UNDO(op)) {
		if (gbl_pgmv_verbose)
			logmsg(LOGMSG_WARN, "%s: starting undo %u\n", __func__, argp->pgno);
		if (h != NULL) {
			cmp_n = log_compare(lsnp, &LSN(h));
			if (cmp_n == 0) {
				if (gbl_pgmv_verbose)
					logmsg(LOGMSG_WARN, "%s: undo main page %u ovfl page to %u\n", __func__, argp->main_pgno, argp->pgno);
				bo = GET_BOVERFLOW(file_dbp, h, argp->main_indx);
				bo->pgno = argp->pgno;
				LSN(h) = argp->main_pglsn;
				hmodified = 1;
			}
		}

		if (poh != NULL) {
			cmp_n = log_compare(lsnp, &LSN(poh));
			if (cmp_n == 0) {
				if (gbl_pgmv_verbose)
					logmsg(LOGMSG_WARN, "%s: relink %u->%u\n", __func__, argp->prev_pgno, argp->pgno);
				NEXT_PGNO(poh) = argp->pgno;
				LSN(poh) = argp->prev_pglsn;
				pohmodified = 1;
			}
		}

		if (noh != NULL) {
			cmp_n = log_compare(lsnp, &LSN(noh));
			if (cmp_n == 0) {
				if (gbl_pgmv_verbose)
					logmsg(LOGMSG_WARN, "%s: relink %u<-%u\n", __func__, argp->pgno, argp->next_pgno);
				PREV_PGNO(noh) = argp->pgno;
				LSN(noh) = argp->next_pglsn;
				nohmodified = 1;
			}
		}

		cmp_n = log_compare(lsnp, &LSN(newoh));
		if (cmp_n == 0) {
			if (gbl_pgmv_verbose)
				logmsg(LOGMSG_WARN, "%s: undo new page %u\n", __func__, argp->new_pgno);
			OV_LEN(newoh) = 0;
			LSN(newoh) = argp->new_pglsn;
			newohmodified = 1;
		}

		cmp_n = log_compare(lsnp, &LSN(oh));
		if (cmp_n == 0) {
			if (gbl_pgmv_verbose)
				logmsg(LOGMSG_WARN, "%s: undo old page %u\n", __func__, argp->pgno);
			memcpy(oh, argp->data.data, argp->data.size);
			DB_ASSERT(PGNO(oh) == argp->pgno);
			LSN(oh) = argp->lsn;
			ohmodified = 1;
		}
	}

	if (check_page) {
		__dir_pg(mpf, argp->pgno, (u_int8_t *)oh, 0);
		__dir_pg(mpf, argp->pgno, (u_int8_t *)oh, 1);
		__dir_pg(mpf, argp->pgno, (u_int8_t *)newoh, 0);
		__dir_pg(mpf, argp->pgno, (u_int8_t *)newoh, 1);
		if (noh != NULL) {
			__dir_pg(mpf, argp->pgno, (u_int8_t *)noh, 0);
			__dir_pg(mpf, argp->pgno, (u_int8_t *)noh, 1);
		}
		if (poh != NULL) {
			__dir_pg(mpf, argp->pgno, (u_int8_t *)poh, 0);
			__dir_pg(mpf, argp->pgno, (u_int8_t *)poh, 1);
		}
		if (h != NULL) {
			__dir_pg(mpf, argp->pgno, (u_int8_t *)h, 0);
			__dir_pg(mpf, argp->pgno, (u_int8_t *)h, 1);
		}
	}
	
done:
	*lsnp = argp->prev_lsn;
	ret  = 0;
out:
	if (oh != NULL && (t_ret = __memp_fput(mpf, oh, ohmodified ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if (noh != NULL && (t_ret = __memp_fput(mpf, noh, nohmodified ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if (poh != NULL && (t_ret = __memp_fput(mpf, poh, pohmodified ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if (h != NULL && (t_ret = __memp_fput(mpf, h, hmodified ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if (newoh != NULL && (t_ret = __memp_fput(mpf, newoh, newohmodified ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	REC_CLOSE;
}
