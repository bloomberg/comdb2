#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: bt_rec.c,v 11.64 2003/09/13 18:48:58 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/btree.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "bt_prefix.h"

#include <stdlib.h>
#include <logmsg.h>
#include <locks_wrap.h>

#define	IS_BTREE_PAGE(pagep)						\
	(TYPE(pagep) == P_IBTREE ||					\
	 TYPE(pagep) == P_LBTREE || TYPE(pagep) == P_LDUP)

extern int __bam_repl_undo(DB *, DB_ENV *, DBC *, BKEYDATA *bk, PAGE *, __bam_repl_args *);
extern int __bam_prefix_undo(DB_ENV *, DBC *, PAGE *, __bam_prefix_args *);
extern void __bam_cdel_undo(DB *, PAGE *, __bam_cdel_args *, int);

extern void __bam_cadjust_undo(DB *file_dbp, PAGE *pagep, __bam_cadjust_args *argp);

extern int __bam_adj_undo(DBC *dbc, PAGE *pagep, __bam_adj_args *argp);

extern void __bam_rsplit_target_undo(PAGE *pagep, __bam_rsplit_args *argp);
extern int __bam_rsplit_root_undo(DB *file_dbp, DBC *dbc, PAGE *pagep, __bam_rsplit_args *argp);

extern void __bam_split_target_undo(PAGE *pagep, __bam_split_args *argp);
extern void __bam_split_rootsplit_left_undo(PAGE *pagep, __bam_split_args *argp);
extern void __bam_split_right_undo(PAGE *pagep, __bam_split_args *argp);
extern void __bam_split_next_undo(PAGE *pagep, __bam_split_args *argp);

/* 
 * __bam_split_snap_recover --
 *	Recovery function for split.
 *
 * PUBLIC: int __bam_split_snap_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, PAGE *));
 */
int
__bam_split_snap_recover(dbenv, dbtp, lsnp, op, pagep)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	PAGE *pagep;
{
	__bam_split_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	int ret, rootsplit;
	db_pgno_t pgno_in;

	ret = 0;
	REC_INTRO_PANIC(__bam_split_read, 1);

	pgno_in = PGNO(pagep);
	rootsplit = argp->root_pgno != PGNO_INVALID;

	if (rootsplit) {
		if (pgno_in == argp->root_pgno) {
			__bam_split_target_undo(pagep, argp);
		} else if (pgno_in == argp->left) {
			__bam_split_rootsplit_left_undo(pagep, argp);
		} else if (pgno_in == argp->right) {
			__bam_split_right_undo(pagep, argp);
		} else {
			logmsg(LOGMSG_ERROR, "%s:[%d:%d] Page %d is not a valid recovery target\n", __func__, lsnp->file, lsnp->offset, pgno_in);
			ret = 1;
		}
	} else {
		if (pgno_in == argp->left) {
			__bam_split_target_undo(pagep, argp);
		} else if (pgno_in == argp->npgno) {
			__bam_split_next_undo(pagep, argp);
		} else if (pgno_in == argp->right) {
			__bam_split_right_undo(pagep, argp);
		} else {
			logmsg(LOGMSG_ERROR, "%s:[%d:%d] Page %d is not a valid recovery target\n", __func__, lsnp->file, lsnp->offset, pgno_in);
			ret = 1;
		}
	}

done:
	REC_CLOSE;
}

/* 
 * __bam_rsplit_snap_recover --
 *	Recovery function for a reverse split.
 *
 * PUBLIC: int __bam_rsplit_snap_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, PAGE *));
 */
int
__bam_rsplit_snap_recover(dbenv, dbtp, lsnp, op, pagep)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	PAGE *pagep;
{
	__bam_rsplit_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	int ret;
	db_pgno_t pgno_in;

	ret = 0;
	REC_INTRO_PANIC(__bam_rsplit_read, 1);

	pgno_in = PGNO(pagep);

	if (pgno_in == argp->pgno) {
		__bam_rsplit_target_undo(pagep, argp);
	} else if(pgno_in == argp->root_pgno) {
		if ((ret  = __bam_rsplit_root_undo(file_dbp, dbc, pagep, argp)) != 0) {
			goto out;
		}
	} else {
		logmsg(LOGMSG_ERROR, "%s:[%d:%d] Page %d is not a valid recovery target\n", __func__, lsnp->file, lsnp->offset, pgno_in);
		ret = 1;
	}

out:
done:
	REC_CLOSE;
}

/*
 * __bam_adj_snap_recover --
 *	Recovery function for adj.
 *
 * PUBLIC: int __bam_adj_snap_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, PAGE *));
 */
int
__bam_adj_snap_recover(dbenv, dbtp, lsnp, op, pagep)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	PAGE *pagep;
{
	__bam_adj_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	int ret;

	ret = 0;
	REC_INTRO_PANIC(__bam_adj_read, 1);

	__bam_adj_undo(dbc, pagep, argp);

done:
out:
	REC_CLOSE;
}

/*
 * __bam_cadjust_snap_recover --
 *	Recovery function for the adjust of a count change in an internal
 *	page.
 *
 * PUBLIC: int __bam_cadjust_snap_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, PAGE *));
 */
int
__bam_cadjust_snap_recover(dbenv, dbtp, lsnp, op, pagep)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	PAGE *pagep;
{
	__bam_cadjust_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	int ret;

	ret = 0;
	REC_INTRO_PANIC(__bam_cadjust_read, 1);

	__bam_cadjust_undo(file_dbp, pagep, argp);

done:
	REC_CLOSE;
}


/*
 * __bam_cdel_snap_recover --
 *	Recovery function for the intent-to-delete of a cursor record.
 *
 * PUBLIC: int __bam_cdel_snap_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, PAGE *));
 */
int
__bam_cdel_snap_recover(dbenv, dbtp, lsnp, op, pagep)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	PAGE *pagep;
{
	__bam_cdel_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	u_int32_t indx;
	int cmp_n, cmp_p, modified, ret;

	ret = 0;
	REC_INTRO_PANIC(__bam_cdel_read, 1);

	__bam_cdel_undo(file_dbp, pagep, argp, 0 /* Snapshot rollback should not affect other cursors */);

done:
	REC_CLOSE;
}

/*
 * __bam_repl_snap_recover --
 *	Recovery function for page item replacement.
 *
 * PUBLIC: int __bam_repl_snap_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, PAGE *));
 */
int
__bam_repl_snap_recover(dbenv, dbtp, lsnp, op, pagep)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	PAGE *pagep;
{
	__bam_repl_args *argp;
	BKEYDATA *bk;
	DB *file_dbp;
	DBC *dbc;
	DBT dbt;
	DB_MPOOLFILE *mpf;
	int ret;
	u_int8_t *p;

	ret = 0;
	p = NULL;
	REC_INTRO_PANIC(__bam_repl_read, 1);

	bk = GET_BKEYDATA(file_dbp, pagep, argp->indx);

	ret = __bam_repl_undo(file_dbp, dbenv, dbc, bk, pagep, argp);

done:
	REC_CLOSE;
}


/*
 * __bam_prefix_snap_recover --
 *	Recovery function for prefix.
 *
 * PUBLIC: int __bam_prefix_snap_recover
 * PUBLIC:   __P((DB_ENV *, DBT *, DB_LSN *, db_recops, PAGE *));
 */
int
__bam_prefix_snap_recover(dbenv, dbtp, lsnp, op, pagep)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	PAGE *pagep;
{
	__bam_prefix_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	PAGE *c = NULL;
	int cmp_n, cmp_p, ret;

	ret = 0;
	REC_INTRO(__bam_prefix_read, 1);

	ret = __bam_prefix_undo(dbenv, dbc, pagep, argp);

out:
done:
	REC_CLOSE;
}
