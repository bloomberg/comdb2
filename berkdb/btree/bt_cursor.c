/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: bt_cursor.c,v 11.169 2003/11/19 18:41:06 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

/* for ntohs */
#include <netinet/in.h>

#include <string.h>
#include <strings.h>
#endif
#include <walkback.h>
#include <pthread.h>

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/btree.h"
#include "dbinc/lock.h"
#include "dbinc/mp.h"

#include <btree/bt_prefix.h>
#include <btree/bt_pf.h>
#include <stdlib.h>

#include "debug_switches.h"
#include "logmsg.h"



int __bam_bulk __P((DBC *, DBT *, u_int32_t));
static int __bam_c_close __P((DBC *, db_pgno_t, int *));
static int __bam_c_del __P((DBC *));
static int __bam_c_destroy __P((DBC *));
static int __bam_c_first __P((DBC *));
static int __bam_c_get __P((DBC *, DBT *, DBT *, u_int32_t, db_pgno_t *));
static int __bam_c_getstack __P((DBC *));
static int __bam_c_last __P((DBC *));
static int __bam_c_next __P((DBC *, int, int));
static int __bam_c_physdel __P((DBC *));
static int __bam_c_prev __P((DBC *));
static int __bam_c_put __P((DBC *, DBT *, DBT *, u_int32_t, db_pgno_t *));
static int __bam_c_search __P((DBC *,
	db_pgno_t, const DBT *, u_int32_t, int *));
static int __bam_c_writelock __P((DBC *));
static int __bam_getboth_finddatum __P((DBC *, DBT *, u_int32_t));
static int __bam_getbothc __P((DBC *, DBT *));
static int __bam_get_prev __P((DBC *));
static int __bam_isopd __P((DBC *, db_pgno_t *));


int __bam_c_getpgno __P((DBC *, db_pgno_t *));
int __bam_c_getnextpgno __P((DBC *, db_pgno_t *));
int __bam_c_getgenids __P((DBC *, unsigned long long *, int *, int));



#ifndef TESTSUITE
extern int gbl_disable_deadlock_trace;
extern int gbl_enable_pageorder_trace;
extern int gbl_disable_overflow_page_trace;
#endif

int gbl_skip_cget_in_db_put = 1;
__thread DB *prefault_dbp = NULL;

/*
 * Acquire a new page/lock.  If we hold a page/lock, discard the page, and
 * lock-couple the lock.
 *
 * !!!
 * We have to handle both where we have a lock to lock-couple and where we
 * don't -- we don't duplicate locks when we duplicate cursors if we are
 * running in a transaction environment as there's no point if locks are
 * never discarded.  This means that the cursor may or may not hold a lock.
 * In the case where we are decending the tree we always want to
 * unlock the held interior page so we use ACQUIRE_COUPLE.
 */

#undef  ACQUIRE_TRACE
#define	ACQUIRE_TRACE(dbc, mode, lpgno, lock, fpgno, pagep, ret, trace) {		\
	DB_MPOOLFILE *__mpf = (dbc)->dbp->mpf;				\
    int __flags = F_ISSET(dbc, DBC_DISCARD_PAGES)?DB_MPOOL_NOCACHE:0; \
	if ((pagep) != NULL) {						\
		ret = __memp_fput(__mpf, pagep, __flags);			\
        if(trace && ret) {                                  \
            fprintf(stderr,"ACQUIRE MEMP_FPUT PG %d RETURNS %d\n", PGNO(pagep), ret);                                 \
        }    \
		pagep = NULL;						\
	} else								\
		ret = 0;						\
	if ((ret) == 0 && STD_LOCKING(dbc)){				\
		ret = __db_lget(dbc, LCK_COUPLE, lpgno, mode, 0, &(lock));\
        if(trace && ret) {                                  \
            /* acquire_fprintf(stderr,"ACQUIRE DB_LGET LPGNO %d RETURNS %d\n", lpgno, ret); */                             \
        }    \
    }                                   \
	if ((ret) == 0){							\
        if (!F_ISSET(dbc, DBC_RMW | DBC_WRITECURSOR | DBC_WRITER) && (dbc)->dbp->olcompact)	\
            __flags |= DB_MPOOL_COMPACT;			\
		ret = __memp_fget(__mpf, &(fpgno), __flags, &(pagep));	\
        if(trace && ret) {                                  \
            fprintf(stderr,"ACQUIRE MEMP_FGET FPGNO %d RETURNS %d\n", fpgno, ret);                                  \
        }    \
    } \
}


#undef	ACQUIRE
#define	ACQUIRE(dbc, mode, lpgno, lock, fpgno, pagep, ret) {		\
    ACQUIRE_TRACE(dbc, mode, lpgno, lock, fpgno, pagep, ret, 0); \
}

#undef	ACQUIRE_NOCOUPLE
#define	ACQUIRE_NOCOUPLE(dbc, mode, lpgno, lock, fpgno, pagep, discard, ret) {		\
	DB_MPOOLFILE *__mpf = (dbc)->dbp->mpf;				\
    int __flags = (F_ISSET(dbc, DBC_DISCARD_PAGES)|discard)?DB_MPOOL_NOCACHE:0;    \
	if ((pagep) != NULL) {						\
		ret = __memp_fput_pageorder(__mpf, pagep, __flags);			\
		pagep = NULL;						\
	} else								\
		ret = 0;						\
	if ((ret) == 0 && STD_LOCKING(dbc)) {				\
        __LPUT( dbc, lock ); \
		ret = __db_lget(dbc, 0, lpgno, mode, 0, &(lock)); \
    }                                       \
	if ((ret) == 0)	{						\
        if (!F_ISSET(dbc, DBC_RMW | DBC_WRITECURSOR | DBC_WRITER) && (dbc)->dbp->olcompact)	\
            __flags |= DB_MPOOL_COMPACT;			\
		ret = __memp_fget(__mpf, &(fpgno), __flags, &(pagep));	\
    } \
}

#undef	ACQUIRE_COUPLE
#define	ACQUIRE_COUPLE(dbc, mode, lpgno, lock, fpgno, pagep, ret) {	\
	DB_MPOOLFILE *__mpf = (dbc)->dbp->mpf;				\
    int __flags = F_ISSET(dbc, DBC_DISCARD_PAGES)?DB_MPOOL_NOCACHE:0; \
	if ((pagep) != NULL) {						\
		ret = __memp_fput(__mpf, pagep, __flags);			\
		pagep = NULL;						\
	} else								\
		ret = 0;						\
	if ((ret) == 0 && STD_LOCKING(dbc))				\
		ret = __db_lget(dbc,					\
		    LCK_COUPLE_ALWAYS, lpgno, mode, 0, &(lock));	\
	if ((ret) == 0) {							\
        if (!F_ISSET(dbc, DBC_RMW | DBC_WRITECURSOR | DBC_WRITER) && (dbc)->dbp->olcompact)	\
            __flags |= DB_MPOOL_COMPACT;			\
		ret = __memp_fget(__mpf, &(fpgno), __flags, &(pagep));	\
    }   \
}

/* Acquire a new page/lock for a cursor. */
#undef	ACQUIRE_CUR_TRACE
#define	ACQUIRE_CUR_TRACE(dbc, mode, p, ret, trace) {				\
	BTREE_CURSOR *__cp = (BTREE_CURSOR *)(dbc)->internal;		\
 	if (p != __cp->pgno)						\
 		__cp->pgno = PGNO_INVALID;				\
	ACQUIRE_TRACE(dbc, mode, p, __cp->lock, p, __cp->page, ret, trace);		\
	if ((ret) == 0) {						\
		__cp->pgno = p;						\
		__cp->lock_mode = (mode);				\
	}								\
}



/* Acquire a new page/lock for a cursor. */
#undef	ACQUIRE_CUR
#define	ACQUIRE_CUR(dbc, mode, p, ret) {				\
    ACQUIRE_CUR_TRACE(dbc, mode, p, ret, 0); \
}

/* 
 * Acquire a new page/lock for a cursor without lock coupling.
 * This is used for page-order tablescans so that we don't violate
 * the locking protocol.
 */
#undef	ACQUIRE_CUR_NOCOUPLE
#define	ACQUIRE_CUR_NOCOUPLE(dbc, mode, p, discard, ret) {				\
	BTREE_CURSOR *__cp = (BTREE_CURSOR *)(dbc)->internal;		\
 	if (p != __cp->pgno)						\
 		__cp->pgno = PGNO_INVALID;				\
	ACQUIRE_NOCOUPLE(dbc, mode, p, __cp->lock, p, __cp->page, discard, ret);		\
	if ((ret) == 0) {						\
		__cp->pgno = p;						\
		__cp->lock_mode = (mode);				\
	}								\
}



/*
 * Acquire a new page/lock for a cursor and release the previous.
 * This is typically used when decending a tree and we do not
 * want to hold the interior nodes locked.
 */
#undef	ACQUIRE_CUR_COUPLE
#define	ACQUIRE_CUR_COUPLE(dbc, mode, p, ret) {				\
	BTREE_CURSOR *__cp = (BTREE_CURSOR *)(dbc)->internal;		\
 	if (p != __cp->pgno)						\
 		__cp->pgno = PGNO_INVALID;				\
	ACQUIRE_COUPLE(dbc, mode, p, __cp->lock, p, __cp->page, ret);	\
	if ((ret) == 0) {						\
		__cp->pgno = p;						\
		__cp->lock_mode = (mode);				\
	}								\
}

/*
 * Acquire a write lock if we don't already have one.
 *
 * !!!
 * See ACQUIRE macro on why we handle cursors that don't have locks.
 */
#undef	ACQUIRE_WRITE_LOCK
#define	ACQUIRE_WRITE_LOCK(dbc, ret) {					\
	BTREE_CURSOR *__cp = (BTREE_CURSOR *)(dbc)->internal;		\
	ret = 0;							\
	if (STD_LOCKING(dbc) &&						\
	    __cp->lock_mode != DB_LOCK_WRITE &&				\
	    ((ret) = __db_lget(dbc,					\
	    LOCK_ISSET(__cp->lock) ? LCK_COUPLE : 0,			\
	    __cp->pgno, DB_LOCK_WRITE, 0, &__cp->lock)) == 0)		\
		__cp->lock_mode = DB_LOCK_WRITE;			\
}

/* Discard the current page/lock for a cursor. */
#undef	DISCARD_CUR
#define	DISCARD_CUR(dbc, ret) {						\
	BTREE_CURSOR *__cp = (BTREE_CURSOR *)(dbc)->internal;		\
	DB_MPOOLFILE *__mpf = (dbc)->dbp->mpf;				\
	int __t_ret;							\
	if ((__cp->page) != NULL) {					\
		ret = __memp_fput(__mpf, __cp->page, 0);		\
		__cp->page = NULL;					\
	} else								\
		ret = 0;						\
	__t_ret = __TLPUT((dbc), __cp->lock);				\
	if (__t_ret != 0 && (ret) == 0)					\
		ret = __t_ret;						\
	if ((ret) == 0 && !LOCK_ISSET(__cp->lock))			\
		__cp->lock_mode = DB_LOCK_NG;				\
}

/* If on-page item is a deleted record. */
#undef	IS_DELETED
#define	IS_DELETED(dbp, page, indx)					\
	B_DISSET(GET_BKEYDATA(dbp, page,				\
	    (indx) + (TYPE(page) == P_LBTREE ? O_INDX : 0)))
#undef	IS_CUR_DELETED
#define	IS_CUR_DELETED(dbc)						\
	IS_DELETED((dbc)->dbp, (dbc)->internal->page, (dbc)->internal->indx)

/*
 * Test to see if two cursors could point to duplicates of the same key.
 * In the case of off-page duplicates they are they same, as the cursors
 * will be in the same off-page duplicate tree.  In the case of on-page
 * duplicates, the key index offsets must be the same.  For the last test,
 * as the original cursor may not have a valid page pointer, we use the
 * current cursor's.
 */
#undef	IS_DUPLICATE
#define	IS_DUPLICATE(dbc, i1, i2)					\
	    (P_INP((dbc)->dbp,((PAGE *)(dbc)->internal->page))[i1] ==	\
	     P_INP((dbc)->dbp,((PAGE *)(dbc)->internal->page))[i2])
#undef	IS_CUR_DUPLICATE
#define	IS_CUR_DUPLICATE(dbc, orig_pgno, orig_indx)			\
	(F_ISSET(dbc, DBC_OPD) ||					\
	    (orig_pgno == (dbc)->internal->pgno &&			\
	    IS_DUPLICATE(dbc, (dbc)->internal->indx, orig_indx)))

#ifndef TESTSUITE
#include <ctrace.h>
#endif

/* cheapstack rippoff, thanks Mike */
#define MAXSTKDEPTH     100
#define SKIPFRMS        4

static char
prhexnib(unsigned char nib)
{
	char map[] = "0123456789ABCDEF";

	return map[nib & 0x0F];
}

/* 
 * print val as hex into str[]
 * str must be large enough to hold val in hex + '\0' 
 */
static char *
prhexval(char str[], void *val, int nbytes)
{
	int nnib = 0;
	int i;

	for (i = 0; i < nbytes; ++i) {
		unsigned char byte = ((unsigned char *)val)[i];

		str[nnib++] = prhexnib(byte >> 4);	/* hi nibble */
		str[nnib++] = prhexnib(byte);	/* lo nibble */
	}
	str[nnib++] = '\0';

	return str;
}

#ifndef TESTSUITE

void
stack_me(char *location)
{

	void *stack[MAXSTKDEPTH];
	char msg[MAXSTKDEPTH * 9];
	unsigned nframes;
	int rc;
	int i;

	bzero(msg, sizeof(msg));

	rc = stack_pc_getlist(NULL, stack, MAXSTKDEPTH, &nframes);

	if (rc) {
		ctrace("Can't get stack trace\n");
		return;
	}
	/*ctrace("[%s] start cheap_stack:\n", location); */
	for (i = SKIPFRMS; i < nframes; i++) {
		msg[(i - SKIPFRMS) * 9] = ' ';
		prhexval(&msg[(i - SKIPFRMS) * 9 + 1], &stack[i], 4);
	}
	msg[(i - SKIPFRMS) * 9] = '\n';
	ctrace("%s", msg);
}

#endif

int
comdb2__db_c_close(dbc)
	DBC *dbc;
{
	int rc = __db_c_close(dbc);

#if 0
	ctrace("CC %d close %d\n", pthread_self(), dbc->locker);
#endif
	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("c_close deadlock");
#endif
	}

	return rc;
}

int
comdb2__db_c_close_nocount(dbc)
	DBC *dbc;
{
	int rc = __db_c_close_nocount(dbc);

#if 0
	ctrace("CCN %d close %d\n", pthread_self(), dbc->locker);
#endif
	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("c_close_nocount deadlock");
#endif
	}

	return rc;
}

int
comdb2__db_c_get_pagelsn(dbc, lsn)
	DBC *dbc;
	DB_LSN *lsn;
{
	return __db_c_get_pagelsn(dbc, lsn);
}

int
comdb2__db_c_get_pageinfo(dbc, page, index, lsn)
	DBC *dbc;
	int *page;
	int *index;
	DB_LSN *lsn;
{
	int ret;

	if ((ret = __db_c_get_pageindex(dbc, page, index)) != 0)
		goto err;

	if ((ret = __db_c_get_pagelsn(dbc, lsn)) != 0)
		goto err;

err:
	if (ret) {
		ZERO_LSN(*lsn);
		*page = *index = -1;
	}
	return (ret);
}

int
comdb2__db_c_get_pageindex(dbc, page, index)
	DBC *dbc;
	int *page;
	int *index;
{
	return __db_c_get_pageindex(dbc, page, index);
}

int
comdb2__db_c_get_fileid(dbc, pfileid)
	DBC *dbc;
	void *pfileid;
{
	if (dbc->dbtype != DB_BTREE) {
		logmsg(LOGMSG_ERROR, "__db_c_get_fileid only for btree\n");
		return -1;
	}
	memcpy(pfileid, dbc->dbp->fileid, DB_FILE_ID_LEN);
	return 0;
}


int
comdb2__db_c_close_ser(dbc, dbcs)
	DBC *dbc;
	DBCS *dbcs;
{
	int rc = __db_c_close_ser(dbc, dbcs);

#if 0
	ctrace("CCN %d close %d\n", pthread_self(), dbc->locker);
#endif
	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("c_close_nocount deadlock");
#endif
	}

	return rc;
}




int
comdb2__db_c_getpgno(dbc, pgno)
	DBC *dbc;
	db_pgno_t *pgno;
{
	int rc = __db_c_getpgno(dbc, pgno);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("c_getpgno deadlock");
#endif
	}

	return rc;
}

int
comdb2__db_c_getnextpgno(dbc, pgno)
	DBC *dbc;
	db_pgno_t *pgno;
{
	int rc = __db_c_getnextpgno(dbc, pgno);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("c_getnextpgno deadlock");
#endif
	}

	return rc;
}

int
comdb2__db_c_getgenids(dbc, genids, num, max)
	DBC *dbc;
	unsigned long long *genids;
	int *num;
	int max;
{
	int rc = __bam_c_getgenids(dbc, genids, num, max);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("__bam_c_getgenids");
#endif
	}

	return rc;
}


int
comdb2__db_c_pause(dbc, dbcps)
	DBC *dbc;
	DBCPS *dbcps;
{
	int rc = __db_c_pause(dbc, dbcps);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("c_pause deadlock");
#endif
	}

	return rc;
}

int
comdb2__db_c_firstleaf(dbc, pgno)
	DBC *dbc;
	db_pgno_t *pgno;
{
	int rc = __db_c_firstleaf(dbc, pgno);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("c_firstleaf deadlock");
#endif
	}

	return rc;
}

int
comdb2__db_c_unpause(dbc, dbcps)
	DBC *dbc;
	DBCPS *dbcps;
{
	int rc = __db_c_unpause(dbc, dbcps);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("c_unpause deadlock");
#endif
	}

	return rc;
}

static int
comdb2__db_c_count_pp(dbc, recnop, flags)
	DBC *dbc;
	db_recno_t *recnop;
	u_int32_t flags;
{
	int rc = __db_c_count_pp(dbc, recnop, flags);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("c_count deadlock");
#endif
	}

	return rc;
}

static int
comdb2__db_c_del_pp(dbc, flags)
	DBC *dbc;
	u_int32_t flags;
{
	int rc = __db_c_del_pp(dbc, flags);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("c_del deadlock");
#endif
	}

	return rc;
}

static int
comdb2__db_c_dup_pp(dbc, dbcp, flags)
	DBC *dbc, **dbcp;
	u_int32_t flags;
{
	int rc = __db_c_dup_pp(dbc, dbcp, flags);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("c_dup deadlock");
#endif
	}

	return rc;
}

static int
comdb2__db_c_get_dup_pp(dbc, dbcp, key, data, flags)
	DBC *dbc, **dbcp;
	DBT *key, *data;
	u_int32_t flags;
{
	int rc = __db_c_get_dup_pp(dbc, dbcp, key, data, flags);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("c_get deadlock");
#endif
	}

	return rc;
}

static int
comdb2__db_c_get_pp(dbc, key, data, flags)
	DBC *dbc;
	DBT *key, *data;
	u_int32_t flags;
{
	int rc = __db_c_get_pp(dbc, key, data, flags);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("c_get deadlock");
#endif
	}

	return rc;
}

static int
comdb2__db_c_pget_pp(dbc, skey, pkey, data, flags)
	DBC *dbc;
	DBT *skey, *pkey, *data;
	u_int32_t flags;
{
	int rc = __db_c_pget_pp(dbc, skey, pkey, data, flags);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("c_pget deadlock");
#endif
	}

	return rc;
}

static int
comdb2__db_c_put_pp(dbc, key, data, flags)
	DBC *dbc;
	DBT *key, *data;
	u_int32_t flags;
{
	int rc = __db_c_put_pp(dbc, key, data, flags);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("c_put deadlock");
#endif
	}

	return rc;
}

static int
comdb2__db_c_skip_stat(dbc, nxtcnt, skpcnt)
	DBC *dbc;
	u_int64_t *nxtcnt;
	u_int64_t *skpcnt;
{
	*nxtcnt = dbc->nextcount;
	*skpcnt = dbc->skipcount;
	return 0;
}

static int
comdb2__bam_bulk(dbc, data, flags)
	DBC *dbc;
	DBT *data;
	u_int32_t flags;
{
	int rc = __bam_bulk(dbc, data, flags);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("bam_bulk deadlock");
#endif
	}

	return rc;
}

static int
comdb2__bam_c_close(dbc, root_pgno, rmroot)
	DBC *dbc;
	db_pgno_t root_pgno;
	int *rmroot;
{
	int rc = __bam_c_close(dbc, root_pgno, rmroot);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("bam_c_close deadlock");
#endif
	}

	return rc;
}

static int
comdb2__bam_c_del(dbc)
	DBC *dbc;
{
	int rc = __bam_c_del(dbc);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("bam_c_del deadlock");
#endif
	}

	return rc;
}

static int
comdb2__bam_c_destroy(dbc)
	DBC *dbc;
{
	int rc = __bam_c_destroy(dbc);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("bam_c_destroy deadlock");
#endif
	}

	return rc;
}


static int
comdb2__bam_c_get(dbc, key, data, flags, pgnop)
	DBC *dbc;
	DBT *key, *data;
	u_int32_t flags;
	db_pgno_t *pgnop;
{
	int rc = __bam_c_get(dbc, key, data, flags, pgnop);

#ifndef TESTSUITE
	if (rc == DB_LOCK_DEADLOCK) {
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("bam_c_get deadlock");
	}
#endif

	return rc;
}

static int
comdb2__bam_c_put(dbc, key, data, flags, pgnop)
	DBC *dbc;
	DBT *key, *data;
	u_int32_t flags;
	db_pgno_t *pgnop;
{
	int rc = __bam_c_put(dbc, key, data, flags, pgnop);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("bam_c_put deadlock");
#endif
	}

	return rc;
}

static int
comdb2__bam_c_writelock(dbc)
	DBC *dbc;
{
	int rc = __bam_c_writelock(dbc);

	if (rc == DB_LOCK_DEADLOCK) {
#ifndef TESTSUITE
		if (debug_switch_verbose_cursor_deadlocks() &&
		    !gbl_disable_deadlock_trace)
			stack_me("bam_c_writelock deadlock");
#endif
	}

	return rc;
}
/*
 * __bam_pgorder_prev --
 * Last-page - 1 
 */

static inline int
__bam_pgorder_prev(dbc, pgno)
	DBC *dbc;
	db_pgno_t pgno;
{
	static int lastpr = 0;

	if (gbl_enable_pageorder_trace && time(NULL) > lastpr + 1) {
		logmsg(LOGMSG_USER, "Berkdb cursor %p page-order prev to page %d\n",
		    dbc, pgno - 1);
		lastpr = time(NULL);
	}
	return pgno - 1;
}



/*
 * __bam_pgorder_next --
 * Last-page + 1 
 */

static inline int
__bam_pgorder_next(dbc, pgno)
	DBC *dbc;
	db_pgno_t pgno;
{
	static int lastpr=0;
	if (gbl_enable_pageorder_trace && time(NULL) > lastpr + 1) {
		logmsg(LOGMSG_USER, "Berkdb cursor %p page-order next to page"
			"%d, skip=%lu next=%lu\n",
			dbc, pgno + 1, dbc->skipcount, dbc->nextcount);
		lastpr=time(NULL);
	}
	dbc->nextcount++;
	return pgno + 1;
}

/*
 * __bam_c_init --
 *	Initialize the access private portion of a cursor
 *
 * PUBLIC: int __bam_c_init __P((DBC *, DBTYPE));
 */
int
__bam_c_init(dbc, dbtype)
	DBC *dbc;
	DBTYPE dbtype;
{
	DB_ENV *dbenv;
	BTREE_CURSOR *cp;
	int ret;

	dbenv = dbc->dbp->dbenv;

	/* Allocate/initialize the internal structure. */
	if (dbc->internal == NULL &&(ret =
	    __os_calloc(dbenv, 1, sizeof(BTREE_CURSOR), &dbc->internal)) != 0)
		return (ret);

	cp = (BTREE_CURSOR *)dbc->internal;

	/* Initialize methods. */
	dbc->c_close = comdb2__db_c_close;
	dbc->c_close_nocount = comdb2__db_c_close_nocount;

	dbc->c_get_pagelsn = comdb2__db_c_get_pagelsn;
	dbc->c_get_pageindex = comdb2__db_c_get_pageindex;
	dbc->c_get_fileid = comdb2__db_c_get_fileid;
	dbc->c_get_pageinfo = comdb2__db_c_get_pageinfo;

	dbc->c_close_ser = comdb2__db_c_close_ser;
	dbc->c_firstleaf = comdb2__db_c_firstleaf;
	dbc->c_pause = comdb2__db_c_pause;
	dbc->c_unpause = comdb2__db_c_unpause;
	dbc->c_count = comdb2__db_c_count_pp;
	dbc->c_del = comdb2__db_c_del_pp;
	dbc->c_dup = comdb2__db_c_dup_pp;
	dbc->c_get = comdb2__db_c_get_pp;
	dbc->c_get_dup = comdb2__db_c_get_dup_pp;
	dbc->c_pget = comdb2__db_c_pget_pp;
	dbc->c_put = comdb2__db_c_put_pp;
	dbc->c_skip_stat = comdb2__db_c_skip_stat;
	if (dbtype == DB_BTREE) {
		dbc->c_am_bulk = comdb2__bam_bulk;
		dbc->c_am_close = comdb2__bam_c_close;
		dbc->c_am_del = comdb2__bam_c_del;
		dbc->c_am_destroy = comdb2__bam_c_destroy;
		dbc->c_am_get = comdb2__bam_c_get;
		dbc->c_am_put = comdb2__bam_c_put;
		dbc->c_am_writelock = comdb2__bam_c_writelock;
	} else {
		dbc->c_am_bulk = __bam_bulk;
		dbc->c_am_close = __bam_c_close;
		dbc->c_am_del = __ram_c_del;
		dbc->c_am_destroy = __bam_c_destroy;
		dbc->c_am_get = __ram_c_get;
		dbc->c_am_put = __ram_c_put;
		dbc->c_am_writelock = __bam_c_writelock;
	}
	dbc->pf = (char *)btpf_init();
	return (0);
}

static inline void
__bam_zero_counts(dbc)
	DBC *dbc;
{
	dbc->nextcount = dbc->skipcount = 0;
}

/*
 * __bam_c_refresh --
 *	Set things up properly for cursor re-use.
 *
 * PUBLIC: int __bam_c_refresh __P((DBC *));
 */
int
__bam_c_refresh(dbc)
	DBC *dbc;
{
	BTREE *t;
	BTREE_CURSOR *cp;
	DB *dbp;
	int ret;

	/* TODO Fabio change this to line to a reset
	btpf_free((btpf**)&dbc->pf);
	dbc->pf = (char*) btpf_init();
	*/

	dbp = dbc->dbp;
	t = dbp->bt_internal;
	cp = (BTREE_CURSOR *)dbc->internal;
	cp->pagelsn.file = 0;
	cp->pagelsn.offset = 0;

	__bam_zero_counts(dbc);

	/*
	 * If our caller set the root page number, it's because the root was
	 * known.  This is always the case for off page dup cursors.  Else,
	 * pull it out of our internal information.
	 */
	if (cp->root == PGNO_INVALID)
		cp->root = t->bt_root;

	LOCK_INIT(cp->lock);
	cp->lock_mode = DB_LOCK_NG;

	if (cp->sp == NULL) {
		cp->sp = cp->stack;
		cp->esp = cp->stack + sizeof(cp->stack) / sizeof(cp->stack[0]);
	}

	cp->csp = cp->sp;

	/*
	 * The btree leaf page data structures require that two key/data pairs
	 * (or four items) fit on a page, but other than that there's no fixed
	 * requirement.  The btree off-page duplicates only require two items,
	 * to be exact, but requiring four for them as well seems reasonable.
	 *
	 * Recno uses the btree bt_ovflsize value -- it's close enough.
	 */
	cp->ovflsize = B_MINKEY_TO_OVFLSIZE(
	    dbp,  F_ISSET(dbc, DBC_OPD) ? 2 : t->bt_minkey, dbp->pgsize);

	cp->recno = RECNO_OOB;
	cp->order = INVALID_ORDER;
	cp->flags = 0;

	/* Initialize for record numbers. */
	if (F_ISSET(dbc, DBC_OPD) ||
	    dbc->dbtype == DB_RECNO || F_ISSET(dbp, DB_AM_RECNUM)) {
		F_SET(cp, C_RECNUM);

		/*
		 * All btrees that support record numbers, optionally standard
		 * recno trees, and all off-page duplicate recno trees have
		 * mutable record numbers.
		 */
		if ((F_ISSET(dbc, DBC_OPD) && dbc->dbtype == DB_RECNO) ||
		    F_ISSET(dbp, DB_AM_RECNUM | DB_AM_RENUMBER))
			F_SET(cp, C_RENUMBER);
	}

	return (0);
}

/*
 * PUBLIC: int __bam_c_getpgno __P((DBC *, db_pgno_t *));
 */
int
__bam_c_getpgno(dbc, pgno)
	DBC *dbc;
	db_pgno_t *pgno;
{
	BTREE_CURSOR *cp;
	PAGE *page;

	cp = (BTREE_CURSOR *)dbc->internal;
	page = cp->page;

	*pgno = page->pgno;

	return 0;
}

/*
 * PUBLIC: int __bam_c_getnextpgno __P((DBC *, db_pgno_t *));
 */
int
__bam_c_getnextpgno(dbc, pgno)
	DBC *dbc;
	db_pgno_t *pgno;
{
	BTREE_CURSOR *cp;
	PAGE *page;

	cp = (BTREE_CURSOR *)dbc->internal;
	page = cp->page;

	*pgno = page->next_pgno;

	return 0;
}

/*
 * PUBLIC: int __bam_c_getgenids __P((DBC *, unsigned long long *, int *, int));
 */
int
__bam_c_getgenids(dbc, genids, num, max)
	DBC *dbc;
	unsigned long long *genids;
	int *num;
	int max;
{
	PAGE *page;
	BKEYDATA *data;
	BTREE_CURSOR *cp;
	db_indx_t *entries;
	int i;

	*num = 0;
	cp = (BTREE_CURSOR *)dbc->internal;
	page = cp->page;

	for (i = 1; i < ntohs(page->entries); i += 2) {
		db_indx_t *entries;

		entries = ((db_indx_t *)((u_int8_t *)(page)));
		data = (BKEYDATA *)(((u_int8_t *)page) + ntohs(entries[i]));

		/* skip deleted */
		if (B_DISSET(data))
			continue;

		/* weak sanity check - see if its the right size */
		if (data->len == sizeof(unsigned long long))
			if (*num < max) {
				memcpy(genids + (*num), data->data,
				    sizeof(unsigned long long));
				*num = *num + 1;
			}
	}

	return 0;
}

/*
 * __bam_c_firstleaf --
 *  Return the first leaf found by the previous DB_SET_RANGE.
 *
 * PUBLIC: int __bam_c_firstleaf __P((DBC *, db_pgno_t *));
 */
int
__bam_c_firstleaf(dbc, pgno)
	DBC *dbc;
	db_pgno_t *pgno;
{
	BTREE_CURSOR *cp = (BTREE_CURSOR *)dbc->internal;

	*pgno = cp->firstleaf;
	return 0;
}

/*
 * __bam_c_pause --
 *	Pause a btree cursor.
 *	comdb2 addition
 *
 * PUBLIC: int __bam_c_pause __P((DBC *, DBCPS *));
 */
int
__bam_c_pause(dbc, dbcps)
	DBC *dbc;
	DBCPS *dbcps;
{
	BTREE_CURSOR *cp;
	BTREE_CURSOR_PAUSE *dbcpsi;

	cp = (BTREE_CURSOR *)dbc->internal;
	int ret;

	cp = (BTREE_CURSOR *)dbc->internal;
	dbcpsi = (BTREE_CURSOR_PAUSE *) dbcps;

	/* XXX this is for debugging */
	if (cp->paused) {
		logmsg(LOGMSG_FATAL, "Pausing an already paused cursor?\n");
		abort();
	}

	/* if we don't have the page, grab it */
	if (!cp->page) {
		DB_ASSERT(0 == cp->pgno);
		db_lockmode_t mode =
		    (DB_LOCK_NG ==
		    cp->lock_mode ? DB_LOCK_READ : cp->lock_mode);
		ACQUIRE_CUR(dbc, mode, cp->pgno, ret);
		if (ret != 0)
			return ret;
	}
	dbcpsi->lsn = LSN(cp->page);

	/* We can't write in pausing txns */
	DB_ASSERT(!F_ISSET(cp, C_DELETED));

	/* Store the page & index */
	dbcpsi->pgno = cp->pgno;
	dbcpsi->indx = cp->indx;

	/* Store the lock_mode */
	dbcpsi->lock_mode = cp->lock_mode;

	/* Set to invalid before discarding the lock */
	cp->pgno = PGNO_INVALID;
	cp->indx = 0;
	cp->paused = 1;

	/* Put the page back and release it's lock.  This sets ret. */
	DISCARD_CUR(dbc, ret);

	/* This can't be set now */
	DB_ASSERT(!LOCK_ISSET(cp->lock));

	/* I want to see this while I'm debugging */
	if (ret)
		abort();

	return (0);
}

/*
 * __bam_c_unpause --
 *	Unpause a btree cursor.
 *	comdb2 addition
 *
 * PUBLIC: int __bam_c_unpause __P((DBCPS *, DBC *));
 */
int
__bam_c_unpause(dbcps, dbc)
	DBCPS *dbcps;
	DBC *dbc;
{
	BTREE_CURSOR_PAUSE *dbcpsi;
	BTREE_CURSOR *cp;
	int is_stale;
	int ret;

	dbcpsi = (BTREE_CURSOR_PAUSE *)dbcps;
	cp = (BTREE_CURSOR *)dbc->internal;

	/* This is for debugging */
	if (cp->paused == 0) {
		logmsg(LOGMSG_FATAL, "Unpausing a non-paused cursor?\n");
		abort();
	}

	cp->paused = 0;

	/* Get the page and it's lock.  This macro sets the pgno in the cursor */
	ACQUIRE_CUR_TRACE(dbc, dbcpsi->lock_mode, dbcpsi->pgno, ret, 1);
	if (ret != 0) {
		if (ret == DB_PAGE_NOTFOUND)
			return (DB_CUR_STALE);

		return (ret);
	}

	/* Set correct index */
	cp->indx = dbcpsi->indx;

	is_stale = LSN(cp->page).file != dbcpsi->lsn.file
	    || LSN(cp->page).offset != dbcpsi->lsn.offset;

	if (is_stale)
		return (DB_CUR_STALE);

	return (0);
}

/*
 * PUBLIC: int __bam_c_get_pageindex __P((DBC *, int *, int *));
 */
int
__bam_c_get_pageindex(dbc, page, index)
	DBC *dbc;
	int *page;
	int *index;
{
	BTREE_CURSOR *cp;

	cp = (BTREE_CURSOR *)dbc->internal;

	if (cp->pgno == PGNO_INVALID)
		return EINVAL;

	if (page)
		*page = cp->pgno;
	if (index)
		*index = cp->indx;
	return 0;
}

/*
 * PUBLIC: int __bam_c_get_pagelsn __P((DBC *, DB_LSN *));
 */
int
__bam_c_get_pagelsn(dbc, lsn)
	DBC *dbc;
	DB_LSN *lsn;
{
	BTREE_CURSOR *cp;

	cp = (BTREE_CURSOR *)dbc->internal;

	if (cp->pagelsn.file == 0)
		return EINVAL;

	*lsn = cp->pagelsn;

	return 0;
}

/*
 * __bam_c_ser --
 *	Serialize a btree cursor.
 *	comdb2 addition
 *
 * PUBLIC: int __bam_c_ser __P((DBC *, DBCS *));
 */
int
__bam_c_ser(dbc, dbcs)
	DBC *dbc;
	DBCS *dbcs;
{
	BTREE_CURSOR *cp;
	BTREE_CURSOR_SER *dbcsi;
	int ret;

	cp = (BTREE_CURSOR *)dbc->internal;
	dbcsi = (BTREE_CURSOR_SER *) dbcs;

	/* if we don't have the page, grab it */
	if (!cp->page) {
		ACQUIRE_CUR(dbc, cp->lock_mode, cp->pgno, ret);
		if (ret != 0)
			return ret;
	}
	dbcsi->lsn = LSN(cp->page);

	dbcsi->ovflsize = cp->ovflsize;
	dbcsi->recno = cp->recno;
	dbcsi->flags = cp->flags;

	return (0);
}

/*
 * __bam_c_deser --
 *	Deserialize a btree cursor.
 *	comdb2 addition
 *
 * PUBLIC: int __bam_c_deser __P((DBCS *, DBC *));
 */
int
__bam_c_deser(dbcs, dbc)
	DBCS *dbcs;
	DBC *dbc;
{
	BTREE_CURSOR_SER *dbcsi;
	BTREE_CURSOR *cp;
	int ret;

	dbcsi = (BTREE_CURSOR_SER *)dbcs;
	cp = (BTREE_CURSOR *)dbc->internal;

	cp->ovflsize = dbcsi->ovflsize;
	cp->recno = dbcsi->recno;
	cp->flags = dbcsi->flags;

	ACQUIRE_CUR(dbc, cp->lock_mode, cp->pgno, ret);
	if (ret != 0) {
		if (ret == DB_PAGE_NOTFOUND)
			return (DB_CUR_STALE);

		return (ret);
	}

	if (LSN(cp->page).file != dbcsi->lsn.file
	    || LSN(cp->page).offset != dbcsi->lsn.offset)
		return (DB_CUR_STALE);

	return (0);
}

/*
 * __bam_c_close --
 *	Close down the cursor.
 */
static int
__bam_c_close(dbc, root_pgno, rmroot)
	DBC *dbc;
	db_pgno_t root_pgno;
	int *rmroot;
{
	BTREE_CURSOR *cp, *cp_opd, *cp_c;
	DB *dbp;
	DBC *dbc_opd, *dbc_c;
	DB_MPOOLFILE *mpf;
	DB_ENV *dbenv;
	PAGE *h;
	int cdb_lock, ret, t_ret;

	dbp = dbc->dbp;
	dbenv = dbp->dbenv;
	mpf = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;
	cp_opd = (dbc_opd = cp->opd) == NULL ?
	    NULL : (BTREE_CURSOR *)dbc_opd->internal;
	cdb_lock = ret = 0;
	prefault_dbp = dbp;

	/*
	 * There are 3 ways this function is called:
	 *
	 * 1. Closing a primary cursor: we get called with a pointer to a
	 *    primary cursor that has a NULL opd field.  This happens when
	 *    closing a btree/recno database cursor without an associated
	 *    off-page duplicate tree.
	 *
	 * 2. Closing a primary and an off-page duplicate cursor stack: we
	 *    get called with a pointer to the primary cursor which has a
	 *    non-NULL opd field.  This happens when closing a btree cursor
	 *    into database with an associated off-page btree/recno duplicate
	 *    tree. (It can't be a primary recno database, recno databases
	 *    don't support duplicates.)
	 *
	 * 3. Closing an off-page duplicate cursor stack: we get called with
	 *    a pointer to the off-page duplicate cursor.  This happens when
	 *    closing a non-btree database that has an associated off-page
	 *    btree/recno duplicate tree or for a btree database when the
	 *    opd tree is not empty (root_pgno == PGNO_INVALID).
	 *
	 * If either the primary or off-page duplicate cursor deleted a btree
	 * key/data pair, check to see if the item is still referenced by a
	 * different cursor.  If it is, confirm that cursor's delete flag is
	 * set and leave it to that cursor to do the delete.
	 *
	 * NB: The test for == 0 below is correct.  Our caller already removed
	 * our cursor argument from the active queue, we won't find it when we
	 * search the queue in __bam_ca_delete().
	 * NB: It can't be true that both the primary and off-page duplicate
	 * cursors have deleted a btree key/data pair.  Either the primary
	 * cursor may have deleted an item and there's no off-page duplicate
	 * cursor, or there's an off-page duplicate cursor and it may have
	 * deleted an item.
	 *
	 * Primary recno databases aren't an issue here.  Recno keys are either
	 * deleted immediately or never deleted, and do not have to be handled
	 * here.
	 *
	 * Off-page duplicate recno databases are an issue here, cases #2 and
	 * #3 above can both be off-page recno databases.  The problem is the
	 * same as the final problem for off-page duplicate btree databases.
	 * If we no longer need the off-page duplicate tree, we want to remove
	 * it.  For off-page duplicate btrees, we are done with the tree when
	 * we delete the last item it contains, i.e., there can be no further
	 * references to it when it's empty.  For off-page duplicate recnos,
	 * we remove items from the tree as the application calls the remove
	 * function, so we are done with the tree when we close the last cursor
	 * that references it.
	 *
	 * We optionally take the root page number from our caller.  If the
	 * primary database is a btree, we can get it ourselves because dbc
	 * is the primary cursor.  If the primary database is not a btree,
	 * the problem is that we may be dealing with a stack of pages.  The
	 * cursor we're using to do the delete points at the bottom of that
	 * stack and we need the top of the stack.
	 */
	if (F_ISSET(cp, C_DELETED)) {
		if (cp->paused) {
			logmsg(LOGMSG_FATAL, 
                   "Deleted flag set for paused cursor?\n");
			abort();
		}
		dbc_c = dbc;
		switch (dbc->dbtype) {
		case DB_BTREE:				/* Case #1, #3. */
			if (__bam_ca_delete_from_close(dbp, cp->pgno, cp->indx,
				1) == 0)
				goto lock;
			/* If we're here, there's another open cursor */
			goto done;
		case DB_RECNO:
			if (!F_ISSET(dbc, DBC_OPD))	/* Case #1. */
				goto done;
			/* Case #3. */
			if (__ram_ca_delete(dbp, cp->root) == 0)
				goto lock;
			goto done;
		case DB_HASH:
		case DB_QUEUE:
		case DB_UNKNOWN:
		default:
			return (__db_unknown_type(dbp->dbenv,
				"__bam_c_close", dbc->dbtype));
		}
	}

	/* Reset paused flag */
	cp->paused = 0;

	if (dbc_opd == NULL)
		goto done;

	if (F_ISSET(cp_opd, C_DELETED)) {		/* Case #2. */
		/*
		 * We will not have been provided a root page number.  Acquire
		 * one from the primary database.
		 */
		if ((ret = __memp_fget(mpf, &cp->pgno, 0, &h)) != 0)
			goto err;
		root_pgno = GET_BOVERFLOW(dbp, h, cp->indx + O_INDX)->pgno;
		if ((ret = __memp_fput(mpf, h, 0)) != 0)
			goto err;

		dbc_c = dbc_opd;
		switch (dbc_opd->dbtype) {
		case DB_BTREE:
			if (__bam_ca_delete(
			    dbp, cp_opd->pgno, cp_opd->indx, 1) == 0)
				goto lock;
			goto done;
		case DB_RECNO:
			if (__ram_ca_delete(dbp, cp_opd->root) == 0)
				goto lock;
			goto done;
		case DB_HASH:
		case DB_QUEUE:
		case DB_UNKNOWN:
		default:
			return (__db_unknown_type(dbp->dbenv,
				"__bam_c_close", dbc->dbtype));
		}
	}
	goto done;

lock:	cp_c = (BTREE_CURSOR *)dbc_c->internal;

	/*
	 * If this is CDB, upgrade the lock if necessary.  While we acquired
	 * the write lock to logically delete the record, we released it when
	 * we returned from that call, and so may not be holding a write lock
	 * at the moment.
	 */
	if (CDB_LOCKING(dbp->dbenv)) {
		if (F_ISSET(dbc, DBC_WRITECURSOR)) {
			if ((ret = __lock_get(dbp->dbenv,
			    dbc->locker, DB_LOCK_UPGRADE, &dbc->lock_dbt,
			    DB_LOCK_WRITE, &dbc->mylock)) != 0)
				goto err;
			cdb_lock = 1;
		}
		if ((ret = __memp_fget(mpf, &cp_c->pgno, 0, &cp_c->page)) != 0)
			goto err;
		goto delete;
	}

	/*
	 * The variable dbc_c has been initialized to reference the cursor in
	 * which we're going to do the delete.  Initialize the cursor's page
	 * and lock structures as necessary.
	 *
	 * First, we may not need to acquire any locks.  If we're in case #3,
	 * that is, the primary database isn't a btree database, our caller
	 * is responsible for acquiring any necessary locks before calling us.
	 */
	if (F_ISSET(dbc, DBC_OPD)) {
		if ((ret = __memp_fget(mpf, &cp_c->pgno, 0, &cp_c->page)) != 0)
			goto err;
		goto delete;
	}

	/*
	 * Otherwise, acquire a write lock.  If the cursor that did the initial
	 * logical deletion (and which had a write lock) is not the same as the
	 * cursor doing the physical deletion (which may have only ever had a
	 * read lock on the item), we need to upgrade.  The confusion comes as
	 * follows:
	 *
	 *	C1	created, acquires item read lock
	 *	C2	dup C1, create C2, also has item read lock.
	 *	C1	acquire write lock, delete item
	 *	C1	close
	 *	C2	close, needs a write lock to physically delete item.
	 *
	 * If we're in a TXN, we know that C2 will be able to acquire the write
	 * lock, because no locker other than the one shared by C1 and C2 can
	 * acquire a write lock -- the original write lock C1 acquire was never
	 * discarded.
	 *
	 * If we're not in a TXN, it's nastier.  Other cursors might acquire
	 * read locks on the item after C1 closed, discarding its write lock,
	 * and such locks would prevent C2 from acquiring a read lock.  That's
	 * OK, though, we'll simply wait until we can acquire a read lock, or
	 * we'll deadlock.  (Which better not happen, since we're not in a TXN.)
	 *
	 * Lock the primary database page, regardless of whether we're deleting
	 * an item on a primary database page or an off-page duplicates page.
	 */
	ACQUIRE(dbc, DB_LOCK_WRITE,
	    cp->pgno, cp_c->lock, cp_c->pgno, cp_c->page, ret);
	if (ret != 0)
		goto err;

delete:	/*
	 * If the delete occurred in a btree, delete the on-page physical item
	 * referenced by the cursor.
	 */
	if (dbc_c->dbtype == DB_BTREE && (ret = __bam_c_physdel(dbc_c)) != 0)
		goto err;

	/*
	 * If we're not working in an off-page duplicate tree, then we're
	 * done.
	 */
	if (!F_ISSET(dbc_c, DBC_OPD) || root_pgno == PGNO_INVALID)
		goto done;

	/*
	 * We may have just deleted the last element in the off-page duplicate
	 * tree, and closed the last cursor in the tree.  For an off-page btree
	 * there are no other cursors in the tree by definition, if the tree is
	 * empty.  For an off-page recno we know we have closed the last cursor
	 * in the tree because the __ram_ca_delete call above returned 0 only
	 * in that case.  So, if the off-page duplicate tree is empty at this
	 * point, we want to remove it.
	 */
	if ((ret = __memp_fget(mpf, &root_pgno, 0, &h)) != 0)
		goto err;
	if (NUM_ENT(h) == 0) {
		if ((ret = __db_free(dbc, h)) != 0)
			goto err;
	} else {
		if ((ret = __memp_fput(mpf, h, 0)) != 0)
			goto err;
		goto done;
	}

	/*
	 * When removing the tree, we have to do one of two things.  If this is
	 * case #2, that is, the primary tree is a btree, delete the key that's
	 * associated with the tree from the btree leaf page.  We know we are
	 * the only reference to it and we already have the correct lock.  We
	 * detect this case because the cursor that was passed to us references
	 * an off-page duplicate cursor.
	 *
	 * If this is case #3, that is, the primary tree isn't a btree, pass
	 * the information back to our caller, it's their job to do cleanup on
	 * the primary page.
	 */
	if (dbc_opd != NULL) {
		if ((ret = __memp_fget(mpf, &cp->pgno, 0, &cp->page)) != 0)
			goto err;
		if ((ret = __bam_c_physdel(dbc)) != 0)
			goto err;
	} else
		*rmroot = 1;
err:
done:	/*
	 * Discard the page references and locks, and confirm that the stack
	 * has been emptied.
	 */
	if (dbc_opd != NULL) {
		DISCARD_CUR(dbc_opd, t_ret);
		if (t_ret != 0 && ret == 0)
			ret = t_ret;
	}
	DISCARD_CUR(dbc, t_ret);
	if (t_ret != 0 && ret == 0)
		ret = t_ret;


#if USE_BTPF
	btpf_rst((btpf *) dbc->pf);
#endif
	/* Downgrade any CDB lock we acquired. */
	if (cdb_lock)
		(void)__lock_downgrade(
		    dbp->dbenv, &dbc->mylock, DB_LOCK_IWRITE, 0);
	prefault_dbp = NULL;

	return (ret);
}

/*
 * __bam_c_destroy --
 *	Close a single cursor -- internal version.
 */
static int
__bam_c_destroy(dbc)
	DBC *dbc;
{
	BTREE_CURSOR *cp;

	cp = (BTREE_CURSOR *)dbc->internal;

	/* Discard the structures. */
	if (cp->sp != cp->stack)
		__os_free(dbc->dbp->dbenv, cp->sp);

	__os_free(dbc->dbp->dbenv, cp);
	btpf_free((btpf **) & dbc->pf);
	return (0);
}

/*
 * __bam_c_count --
 *	Return a count of on and off-page duplicates.
 *
 * PUBLIC: int __bam_c_count __P((DBC *, db_recno_t *));
 */
int
__bam_c_count(dbc, recnop)
	DBC *dbc;
	db_recno_t *recnop;
{
	BTREE_CURSOR *cp;
	DB *dbp;
	DB_MPOOLFILE *mpf;
	db_indx_t indx, top;
	db_recno_t recno;
	int ret;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;

	/*
	 * Called with the top-level cursor that may reference an off-page
	 * duplicates tree.  We don't have to acquire any new locks, we have
	 * to have a read lock to even get here.
	 */
	if (cp->opd == NULL) {
		/*
		 * On-page duplicates, get the page and count.
		 */
		if ((ret = __memp_fget(mpf, &cp->pgno, 0, &cp->page)) != 0)
			return (ret);

		/*
		 * Move back to the beginning of the set of duplicates and
		 * then count forward.
		 */
		for (indx = cp->indx;; indx -= P_INDX)
			if (indx == 0 ||
			    !IS_DUPLICATE(dbc, indx, indx - P_INDX))
				break;
		for (recno = 0,
		    top = NUM_ENT(cp->page) - P_INDX;; indx += P_INDX) {
			if (!IS_DELETED(dbp, cp->page, indx))
				++recno;
			if (indx == top ||
			    !IS_DUPLICATE(dbc, indx, indx + P_INDX))
				break;
		}
	} else {
		/*
		 * Off-page duplicates tree, get the root page of the off-page
		 * duplicate tree.
		 */
		if ((ret = __memp_fget(
		    mpf, &cp->opd->internal->root, 0, &cp->page)) != 0)
			return (ret);

		/*
		 * If the page is an internal page use the page's count as it's
		 * up-to-date and reflects the status of cursors in the tree.
		 * If the page is a leaf page for unsorted duplicates, use the
		 * page's count as cursors don't mark items deleted on the page
		 * and wait, cursor delete items immediately.
		 * If the page is a leaf page for sorted duplicates, there may
		 * be cursors on the page marking deleted items -- count.
		 */
		if (TYPE(cp->page) == P_LDUP)
			for (recno = 0, indx = 0,
			    top = NUM_ENT(cp->page) - O_INDX;; indx += O_INDX) {
				if (!IS_DELETED(dbp, cp->page, indx))
					++recno;
				if (indx == top)
					break;
		} else
			recno = RE_NREC(cp->page);
	}

	*recnop = recno;

	ret = __memp_fput(mpf, cp->page, 0);
	cp->page = NULL;

	return (ret);
}

/*
 * __bam_c_del --
 *	Delete using a cursor.
 */
static int
__bam_c_del(dbc)
	DBC *dbc;
{
	BTREE_CURSOR *cp;
	DB *dbp;
	DB_MPOOLFILE *mpf;
	int ret, t_ret;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;
	ret = 0;

	/* If the item was already deleted, return failure. */
	if (F_ISSET(cp, C_DELETED))
		return (DB_KEYEMPTY);

	prefault_dbp = dbp;

	/*
	 * This code is always called with a page lock but no page.
	 */
	DB_ASSERT(cp->page == NULL);

	/*
	 * We don't physically delete the record until the cursor moves, so
	 * we have to have a long-lived write lock on the page instead of a
	 * a long-lived read lock.  Note, we have to have a read lock to even
	 * get here.
	 *
	 * If we're maintaining record numbers, we lock the entire tree, else
	 * we lock the single page.
	 */
	if (F_ISSET(cp, C_RECNUM)) {
		if ((ret = __bam_c_getstack(dbc)) != 0)
			goto err;
		cp->page = cp->csp->page;
	} else {
		ACQUIRE_CUR(dbc, DB_LOCK_WRITE, cp->pgno, ret);
		if (ret != 0)
			goto err;
	}

	/* Log the change. */
	if (DBC_LOGGING(dbc)) {
		if ((ret = __bam_cdel_log(dbp, dbc->txn, &LSN(cp->page), 0,
		    PGNO(cp->page), &LSN(cp->page), cp->indx)) != 0)
			goto err;
	} else
		LSN_NOT_LOGGED(LSN(cp->page));

	/* Set the intent-to-delete flag on the page. */
	if (TYPE(cp->page) == P_LBTREE)
		B_DSET(GET_BKEYDATA(dbp, cp->page, cp->indx + O_INDX));
	else
		B_DSET(GET_BKEYDATA(dbp, cp->page, cp->indx));

	/* Mark the page dirty. */
	ret = __memp_fset(mpf, cp->page, DB_MPOOL_DIRTY);

err:	/*
	 * If we've been successful so far and the tree has record numbers,
	 * adjust the record counts.  Either way, release acquired page(s).
	 */
	if (F_ISSET(cp, C_RECNUM)) {
		if (ret == 0)
			ret = __bam_adjust(dbc, -1);
		(void)__bam_stkrel(dbc, 0);
	} else
		if (cp->page != NULL &&
		    (t_ret = __memp_fput(mpf, cp->page, 0)) != 0 && ret == 0)
			ret = t_ret;

	cp->page = NULL;

	/* Update the cursors last, after all chance of failure is past. */
	if (ret == 0)
		(void)__bam_ca_delete(dbp, cp->pgno, cp->indx, 1);
	prefault_dbp = NULL;

	return (ret);
}

/*
 * __bam_c_dup --
 *	Duplicate a btree cursor, such that the new one holds appropriate
 *	locks for the position of the original.
 *
 * PUBLIC: int __bam_c_dup __P((DBC *, DBC *));
 */
int
__bam_c_dup(orig_dbc, new_dbc)
	DBC *orig_dbc, *new_dbc;
{
	BTREE_CURSOR *orig, *new;
	int ret;

	orig = (BTREE_CURSOR *)orig_dbc->internal;
	new = (BTREE_CURSOR *)new_dbc->internal;
#if USE_BTPF
	btpf_copy_dbc(orig_dbc, new_dbc);
#endif
	/*
	 * If we're holding a lock we need to acquire a copy of it, unless
	 * we're in a transaction.  We don't need to copy any lock we're
	 * holding inside a transaction because all the locks are retained
	 * until the transaction commits or aborts.
	 */
	if (LOCK_ISSET(orig->lock) && orig_dbc->txn == NULL) {
		if ((ret = __db_lget(new_dbc,
		    0, new->pgno, new->lock_mode, 0, &new->lock)) != 0)
			return (ret);
	}
	new->ovflsize = orig->ovflsize;
	new->recno = orig->recno;
	new->flags = orig->flags;

	return (0);
}

/*
 * __bam_c_get --
 *	Get using a cursor (btree).
 */
static int
__bam_c_get(dbc, key, data, flags, pgnop)
	DBC *dbc;
	DBT *key, *data;
	u_int32_t flags;
	db_pgno_t *pgnop;
{
	BTREE_CURSOR *cp;
	DB *dbp;
	DB_MPOOLFILE *mpf;
	db_pgno_t orig_pgno;
	db_indx_t orig_indx;
	int exact, newopd, ret;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;
	orig_pgno = cp->pgno;
	orig_indx = cp->indx;
	prefault_dbp = dbp;

	newopd = 0;
	switch (flags) {
	case DB_CURRENT:
		/* It's not possible to return a deleted record. */
		if (F_ISSET(cp, C_DELETED)) {
			ret = DB_KEYEMPTY;
			goto err;
		}

		/*
		 * Acquire the current page.  We have at least a read-lock
		 * already.  The caller may have set DB_RMW asking for a
		 * write lock, but upgrading to a write lock has no better
		 * chance of succeeding now instead of later, so don't try.
		 */
		if ((ret = __memp_fget(mpf, &cp->pgno, 0, &cp->page)) != 0)
			goto err;
		break;
	case DB_FIRST:
		newopd = 1;
		if ((ret = __bam_c_first(dbc)) != 0)
			goto err;
		break;
	case DB_GET_BOTH:
	case DB_GET_BOTH_RANGE:
		/*
		 * There are two ways to get here based on DBcursor->c_get
		 * with the DB_GET_BOTH/DB_GET_BOTH_RANGE flags set:
		 *
		 * 1. Searching a sorted off-page duplicate tree: do a tree
		 * search.
		 *
		 * 2. Searching btree: do a tree search.  If it returns a
		 * reference to off-page duplicate tree, return immediately
		 * and let our caller deal with it.  If the search doesn't
		 * return a reference to off-page duplicate tree, continue
		 * with an on-page search.
		 */
		if (F_ISSET(dbc, DBC_OPD)) {
			if ((ret = __bam_c_search(
			    dbc, PGNO_INVALID, data, flags, &exact)) != 0)
				goto err;
			if (flags == DB_GET_BOTH) {
				if (!exact) {
					ret = DB_NOTFOUND;
					goto err;
				}
				break;
			}

			/*
			 * We didn't require an exact match, so the search may
			 * may have returned an entry past the end of the page,
			 * or we may be referencing a deleted record.  If so,
			 * move to the next entry.
			 */
			if ((cp->indx == NUM_ENT(cp->page) ||
			    IS_CUR_DELETED(dbc)) &&
			    (ret = __bam_c_next(dbc, 1, 0)) != 0)
				goto err;
		} else {
			if ((ret = __bam_c_search(
			    dbc, PGNO_INVALID, key, flags, &exact)) != 0)
				return (ret);
			if (!exact) {
				ret = DB_NOTFOUND;
				goto err;
			}

			if (pgnop != NULL && __bam_isopd(dbc, pgnop)) {
				newopd = 1;
				break;
			}
			if ((ret =
			    __bam_getboth_finddatum(dbc, data, flags)) != 0)
				goto err;
		}
		break;
	case DB_GET_BOTHC:
		if ((ret = __bam_getbothc(dbc, data)) != 0)
			goto err;
		break;
	case DB_LAST:
		newopd = 1;
		if ((ret = __bam_c_last(dbc)) != 0)
			goto err;
		break;
	case DB_NEXT:
		newopd = 1;
		if (cp->pgno == PGNO_INVALID) {
			if ((ret = __bam_c_first(dbc)) != 0)
				goto err;
		} else
			if ((ret = __bam_c_next(dbc, 1, 0)) != 0)
				goto err;
		break;
	case DB_NEXT_DUP:
		if ((ret = __bam_c_next(dbc, 1, 0)) != 0)
			goto err;
		if (!IS_CUR_DUPLICATE(dbc, orig_pgno, orig_indx)) {
			ret = DB_NOTFOUND;
			goto err;
		}
		break;
	case DB_NEXT_NODUP:
		newopd = 1;
		if (cp->pgno == PGNO_INVALID) {
			if ((ret = __bam_c_first(dbc)) != 0)
				goto err;
		} else
			do {
				if ((ret = __bam_c_next(dbc, 1, 0)) != 0)
					goto err;
			} while (IS_CUR_DUPLICATE(dbc, orig_pgno, orig_indx));
		break;
	case DB_PREV:
		newopd = 1;
		if (cp->pgno == PGNO_INVALID) {
			if ((ret = __bam_c_last(dbc)) != 0)
				goto err;
		} else if ((ret = __bam_c_prev(dbc)) != 0)
			goto err;
		break;
	case DB_PREV_NODUP:
		newopd = 1;
		if (cp->pgno == PGNO_INVALID) {
			if ((ret = __bam_c_last(dbc)) != 0)
				goto err;
		} else
			do {
				if ((ret = __bam_c_prev(dbc)) != 0)
					goto err;
			} while (IS_CUR_DUPLICATE(dbc, orig_pgno, orig_indx));
		break;
		case DB_SET:case DB_SET_RECNO:newopd = 1;

		if ((ret = __bam_c_search(dbc,
			    PGNO_INVALID, key, flags, &exact)) != 0)
			goto err;
		break;
	case DB_SET_RANGE:
		newopd = 1;

		INTERNAL_PTR_CHECK(cp == dbc->internal);

		if ((ret = __bam_c_search(dbc,
			    PGNO_INVALID, key, flags, &exact)) != 0)
			goto err;

		INTERNAL_PTR_CHECK(cp == dbc->internal);

		/* Collect firstleaf */
		cp->firstleaf = cp->pgno;

		/*
		 * As we didn't require an exact match, the search function
		 * may have returned an entry past the end of the page.  Or,
		 * we may be referencing a deleted record.  If so, move to
		 * the next entry.
		 *
		 * I've seen the below IS_CUR_DELETED crash because
		 * dbc->internal->page is set to NULL.  When this happened,
		 * the cp pointer seemed valid and intact.
		 */
		if (cp->indx == NUM_ENT(cp->page) || IS_CUR_DELETED(dbc))
			if ((ret = __bam_c_next(dbc, 0, 0)) != 0)
				goto err;
		break;
	default:
		ret = __db_unknown_flag(dbp->dbenv, "__bam_c_get", flags);
		goto err;
	}

	if (cp->page == NULL) {
		cp->pagelsn.file = 0;
		cp->pagelsn.offset = 0;
	} else
		cp->pagelsn = LSN(cp->page);

	/*
	 * We may have moved to an off-page duplicate tree.  Return that
	 * information to our caller.
	 */
	if (newopd && pgnop != NULL)
		(void)__bam_isopd(dbc, pgnop);

	/*
	 * Don't return the key, it was passed to us (this is true even if the
	 * application defines a compare function returning equality for more
	 * than one key value, since in that case which actual value we store
	 * in the database is undefined -- and particularly true in the case of
	 * duplicates where we only store one key value).
	 */
	if (flags == DB_GET_BOTH ||
	    flags == DB_GET_BOTH_RANGE || flags == DB_SET)
		F_SET(key, DB_DBT_ISSET);

err:	/*
	 * Regardless of whether we were successful or not, if the cursor
	 * moved, clear the delete flag, DBcursor->c_get never references
	 * a deleted key, if it moved at all.
	 */
	if (F_ISSET(cp, C_DELETED) &&
	    (cp->pgno != orig_pgno || cp->indx != orig_indx))
		F_CLR(cp, C_DELETED);
	prefault_dbp = NULL;

	return (ret);
}


#if 0
static int
__bam_c_position(dbc, key, data, flags, dbcps, pgnop)
	DBC *dbc;
	DBT *key, *data;
	u_int32_t flags;
	DBCPS *dbcps;
	db_pgno_t *pgnop;
{
	int ret;
	BTREE_CURSOR *cp;
	BTREE_CURSOR_PAUSE *dbcpsi;

	dbcpsi = (BTREE_CURSOR_PAUSE *) dbcps;
	cp = (BTREE_CURSOR *)dbc->internal;

	/* Set cursor page number and index */
	cp->pgno = dbcpsi->pgno;
	cp->indx = dbcpsi->indx;

	/* Get current */
	ret = __bam_c_get(dbc, key, data, DB_CURRENT, pgnop);



}
#endif

static int
__bam_get_prev(dbc)
	DBC *dbc;
{
	BTREE_CURSOR *cp;
	DBT key, data;
	db_pgno_t pgno;
	int ret;

	if ((ret = __bam_c_prev(dbc)) != 0)
		return (ret);

	if (__bam_isopd(dbc, &pgno)) {
		cp = (BTREE_CURSOR *)dbc->internal;
		if ((ret = __db_c_newopd(dbc, pgno, cp->opd, &cp->opd)) != 0)
			return (ret);
		if ((ret = cp->opd->c_am_get(cp->opd,
		    &key, &data, DB_LAST, NULL)) != 0)
			return (ret);
	}

	return (0);
}

/*
 * __bam_bulk -- Return bulk data from a btree.
 */
int
__bam_bulk(dbc, data, flags)
	DBC *dbc;
	DBT *data;
	u_int32_t flags;
{
	BKEYDATA *bk;
	BOVERFLOW *bo;
	BTREE_CURSOR *cp;
	PAGE *pg;
	db_indx_t *inp, indx, pg_keyoff;
	int32_t *endp, key_off, *offp, *saveoffp;
	u_int8_t *dbuf, *dp, *np;
	u_int32_t key_size, pagesize, size, space;
	int adj, is_key, need_pg, next_key, no_dup, rec_key, ret;

	ret = 0;
	key_off = 0;
	size = 0;
	pagesize = dbc->dbp->pgsize;
	cp = (BTREE_CURSOR *)dbc->internal;

	/*
	 * dp tracks the beginging of the page in the buffer.
	 * np is the next place to copy things into the buffer.
	 * dbuf always stays at the beging of the buffer.
	 */
	dbuf = data->data;
	np = dp = dbuf;

	/* Keep track of space that is left.  There is a termination entry */
	space = data->ulen;
	space -= sizeof(*offp);

	/* Build the offset/size table from the end up. */
	endp = (int32_t *)((u_int8_t *)dbuf + data->ulen);
	endp--;
	offp = endp;

	key_size = 0;

	/*
	 * Distinguish between BTREE and RECNO.
	 * There are no keys in RECNO.  If MULTIPLE_KEY is specified
	 * then we return the record numbers.
	 * is_key indicates that multiple btree keys are returned.
	 * rec_key is set if we are returning record numbers.
	 * next_key is set if we are going after the next key rather than dup.
	 */
	if (dbc->dbtype == DB_BTREE) {
		is_key = LF_ISSET(DB_MULTIPLE_KEY) ? 1: 0;
		rec_key = 0;
		next_key = is_key && LF_ISSET(DB_OPFLAGS_MASK) != DB_NEXT_DUP;
		adj = 2;
	} else {
		is_key = 0;
		rec_key = LF_ISSET(DB_MULTIPLE_KEY) ? 1 : 0;
		next_key = LF_ISSET(DB_OPFLAGS_MASK) != DB_NEXT_DUP;
		adj = 1;
	}
	no_dup = LF_ISSET(DB_OPFLAGS_MASK) == DB_NEXT_NODUP;

next_pg:
	indx = cp->indx;
	pg = cp->page;
	inp = P_INP(dbc->dbp, pg);
	/* The current page is not yet in the buffer. */
	need_pg = 1;

	/*
	 * Keep track of the offset of the current key on the page.
	 * If we are returning keys, set it to 0 first so we force
	 * the copy of the key to the buffer.
	 */
	pg_keyoff = 0;
	if (is_key == 0)
		pg_keyoff = inp[indx];

	do {
		if (IS_DELETED(dbc->dbp, pg, indx)) {
			if (dbc->dbtype != DB_RECNO)
				continue;

			cp->recno++;
			/*
			 * If we are not returning recnos then we
			 * need to fill in every slot so the user
			 * can calculate the record numbers.
			 */
			if (rec_key != 0)
				continue;

			space -= 2 * sizeof(*offp);
			/* Check if space as underflowed. */
			if (space > data->ulen)
				goto back_up;

			/* Just mark the empty recno slots. */
			*offp-- = 0;
			*offp-- = 0;
			continue;
		}

		/*
		 * Check to see if we have a new key.
		 * If so, then see if we need to put the
		 * key on the page.  If its already there
		 * then we just point to it.
		 */
		if (is_key && pg_keyoff != inp[indx]) {
			bk = GET_BKEYDATA(dbc->dbp, pg, indx);
			if (B_TYPE(bk) == B_OVERFLOW) {
				bo = (BOVERFLOW *)bk;
				size = key_size = bo->tlen;
				if (key_size > space)
					goto get_key_space;
				if ((ret = __bam_bulk_overflow(dbc,
				    bo->tlen, bo->pgno, np)) != 0)
					return (ret);
				space -= key_size;
				key_off = (int32_t)(np - dbuf);
				np += key_size;
			} else {
				if (need_pg) {
					dp = np;
					size = pagesize - HOFFSET(pg);
					if (space < size) {
get_key_space:
						/* Nothing added, then error. */
						if (offp == endp) {
							data->size =
							    ALIGN(size +
							    pagesize, 1024);
							return (ENOMEM);
						}
						/*
						 * We need to back up to the
						 * last record put into the
						 * buffer so that it is
						 * CURRENT.
						 */
						if (indx != 0)
							indx -= P_INDX;
						else {
							if ((ret =
							    __bam_get_prev(
							    dbc)) != 0)
								return (ret);
							indx = cp->indx;
							pg = cp->page;
						}
						break;
					}
					/*
					 * Move the data part of the page
					 * to the buffer.
					 */
					if (IS_PREFIX(pg))
						return pfx_bulk_page(dbc, np,
						    offp, space);
					memcpy(dp, (u_int8_t *)pg + HOFFSET(pg),
					    size);
					cp->pagelsn = LSN(pg);
					need_pg = 0;
					space -= size;
					np += size;
				}
				key_size = bk->len;
				key_off = (int32_t)((inp[indx] - HOFFSET(pg))
				    + (dp - dbuf) + SSZA(BKEYDATA, data));
				pg_keyoff = inp[indx];
			}
		}

		/*
		 * Reserve space for the pointers and sizes.
		 * Either key/data pair or just for a data item.
		 */
		space -= (is_key ? 4 : 2) * sizeof(*offp);
		if (rec_key)
			space -= sizeof(*offp);

		/* Check to see if space has underflowed. */
		if (space > data->ulen)
			goto back_up;

		/*
		 * Determine if the next record is in the
		 * buffer already or if it needs to be copied in.
		 * If we have an off page dup, then copy as many
		 * as will fit into the buffer.
		 */
		bk = GET_BKEYDATA(dbc->dbp, pg, indx + adj - 1);
		if (B_TYPE(bk) == B_DUPLICATE) {
			bo = (BOVERFLOW *)bk;
			if (is_key) {
				*offp-- = (int32_t)key_off;
				*offp-- = (int32_t)key_size;
			}
			/*
			 * We pass the offset of the current key.
			 * On return we check to see if offp has
			 * moved to see if any data fit.
			 */
			saveoffp = offp;
			if ((ret = __bam_bulk_duplicates(dbc, bo->pgno,
			    dbuf, is_key ? offp + P_INDX : NULL,
			    &offp, &np, &space, no_dup)) != 0) {
				if (ret == ENOMEM) {
					size = space;
					space = 0;
					/* If nothing was added, then error. */
					if (offp == saveoffp) {
						offp += 2;
						goto back_up;
					}
					goto get_space;
				}
				return (ret);
			}
		} else if (B_TYPE(bk) == B_OVERFLOW) {
			bo = (BOVERFLOW *)bk;
			size = bo->tlen;
			if (size > space)
				goto back_up;
			if ((ret =
			    __bam_bulk_overflow(dbc,
				bo->tlen, bo->pgno, np)) != 0)
				return (ret);
			space -= size;
			if (is_key) {
				*offp-- = (int32_t)key_off;
				*offp-- = (int32_t)key_size;
			} else if (rec_key)
				*offp-- = (int32_t)cp->recno;
			*offp-- = (int32_t)(np - dbuf);
			np += size;
			*offp-- = (int32_t)size;
		} else {
			if (need_pg) {
				dp = np;
				size = pagesize - HOFFSET(pg);
				if (space < size) {
back_up:
					/*
					 * Back up the index so that the
					 * last record in the buffer is CURRENT
					 */
					if (indx >= adj)
						indx -= adj;
					else {
						if ((ret =
						    __bam_get_prev(dbc)) != 0 &&
						    ret != DB_NOTFOUND)
							return (ret);
						indx = cp->indx;
						pg = cp->page;
					}
					if (dbc->dbtype == DB_RECNO)
						cp->recno--;
get_space:
					/*
					 * See if we put anything in the
					 * buffer or if we are doing a DBP->get
					 * did we get all of the data.
					 */
					if (offp >=
					    (is_key ? &endp[-1] : endp) ||
					    F_ISSET(dbc, DBC_TRANSIENT)) {
						data->size = ALIGN(size +
						    data->ulen - space, 1024);
						return (ENOMEM);
					}
					break;
				}
				memcpy(dp, (u_int8_t *)pg + HOFFSET(pg), size);
				cp->pagelsn = LSN(pg);
				need_pg = 0;
				space -= size;
				np += size;
			}
			/*
			 * Add the offsets and sizes to the end of the buffer.
			 * First add the key info then the data info.
			 */
			if (is_key) {
				*offp-- = (int32_t)key_off;
				*offp-- = (int32_t)key_size;
			} else if (rec_key)
				*offp-- = (int32_t)cp->recno;
			*offp-- = (int32_t)((inp[indx + adj - 1] - HOFFSET(pg))
			    + (dp - dbuf) + SSZA(BKEYDATA, data));
			*offp-- = bk->len;
		}
		if (dbc->dbtype == DB_RECNO)
			cp->recno++;
		else if (no_dup) {
			while (indx + adj < NUM_ENT(pg) &&
			    pg_keyoff == inp[indx + adj])
				indx += adj;
		}
	/*
	 * Stop when we either run off the page or we
	 * move to the next key and we are not returning mulitple keys.
	 */
	} while ((indx += adj) < NUM_ENT(pg) &&
	    (next_key || pg_keyoff == inp[indx]));

	/* If we are off the page then try to the next page. */
	if (ret == 0 && next_key && indx >= NUM_ENT(pg)) {
		cp->indx = indx;
		if (dbc->dbp->dbenv->bulk_stops_on_page)
			goto done;
		ret = __bam_c_next(dbc, 0, 1);
		if (ret == 0)
			goto next_pg;
		if (ret != DB_NOTFOUND)
			return (ret);
	}

	/*
	 * If we did a DBP->get we must error if we did not return
	 * all the data for the current key because there is
	 * no way to know if we did not get it all, nor any
	 * interface to fetch the balance.
	 */
done:
	if (ret == 0 && indx < pg->entries &&
	    F_ISSET(dbc, DBC_TRANSIENT) && pg_keyoff == inp[indx]) {
		data->size = (data->ulen - space) + size;
		return (ENOMEM);
	}
	/*
	 * Must leave the index pointing at the last record fetched.
	 * If we are not fetching keys, we may have stepped to the
	 * next key.
	 */
	if (ret == ENOMEM || next_key || pg_keyoff == inp[indx])
		cp->indx = indx;
	else
		cp->indx = indx - P_INDX;

	if (rec_key == 1)
		*offp = RECNO_OOB;
	else
		*offp = -1;
	return (0);
}

/*
 * __bam_bulk_overflow --
 *	Dump overflow record into the buffer.
 *	The space requirements have already been checked.
 * PUBLIC: int __bam_bulk_overflow
 * PUBLIC:    __P((DBC *, u_int32_t, db_pgno_t, u_int8_t *));
 */
int
__bam_bulk_overflow(dbc, len, pgno, dp)
	DBC *dbc;
	u_int32_t len;
	db_pgno_t pgno;
	u_int8_t *dp;
{
	DBT dbt;

	memset(&dbt, 0, sizeof(dbt));
	F_SET(&dbt, DB_DBT_USERMEM);
	dbt.ulen = len;
	dbt.data = (void *)dp;
	return (__db_goff(dbc->dbp, &dbt, len, pgno, NULL, NULL));
}

/*
 * __bam_bulk_duplicates --
 *	Put as many off page duplicates as will fit into the buffer.
 * This routine will adjust the cursor to reflect the position in
 * the overflow tree.
 * PUBLIC: int __bam_bulk_duplicates __P((DBC *,
 * PUBLIC:       db_pgno_t, u_int8_t *, int32_t *,
 * PUBLIC:	 int32_t **, u_int8_t **, u_int32_t *, int));
 */
int
__bam_bulk_duplicates(dbc, pgno, dbuf, keyoff, offpp, dpp, spacep, no_dup)
	DBC *dbc;
	db_pgno_t pgno;
	u_int8_t *dbuf;
	int32_t *keyoff, **offpp;
	u_int8_t **dpp;
	u_int32_t *spacep;
	int no_dup;
{
	DB *dbp;
	BKEYDATA *bk;
	BOVERFLOW *bo;
	BTREE_CURSOR *cp;
	DBC *opd;
	DBT key, data;
	PAGE *pg;
	db_indx_t indx, *inp;
	int32_t *offp;
	u_int32_t pagesize, size, space;
	u_int8_t *dp, *np;
	int first, need_pg, ret, t_ret;

	ret = 0;

	dbp = dbc->dbp;
	cp = (BTREE_CURSOR *)dbc->internal;
	opd = cp->opd;

	if (opd == NULL) {
		if ((ret = __db_c_newopd(dbc, pgno, NULL, &opd)) != 0)
			return (ret);
		cp->opd = opd;
		if ((ret = opd->c_am_get(opd,
		    &key, &data, DB_FIRST, NULL)) != 0)
			goto close_opd;
	}

	pagesize = opd->dbp->pgsize;
	cp = (BTREE_CURSOR *)opd->internal;
	space = *spacep;
	/* Get current offset slot. */
	offp = *offpp;

	/*
	 * np is the next place to put data.
	 * dp is the begining of the current page in the buffer.
	 */
	np = dp = *dpp;
	first = 1;
	indx = cp->indx;

	do {
		/* Fetch the current record.  No initial move. */
		if ((ret = __bam_c_next(opd, 0, 0)) != 0)
			break;
		pg = cp->page;
		indx = cp->indx;
		inp = P_INP(dbp, pg);
		/* We need to copy the page to the buffer. */
		need_pg = 1;

		do {
			if (IS_DELETED(dbp, pg, indx))
				goto contin;
			bk = GET_BKEYDATA(dbp, pg, indx);
			space -= 2 * sizeof(*offp);
			/* Allocate space for key if needed. */
			if (first == 0 && keyoff != NULL)
				space -= 2 * sizeof(*offp);

			/* Did space underflow? */
			if (space > *spacep) {
				ret = ENOMEM;
				if (first == 1) {
					/* Get the absolute value. */
					space = -(int32_t)space;
					space = *spacep + space;
					if (need_pg)
						space += pagesize - HOFFSET(pg);
				}
				break;
			}
			if (B_TYPE(bk) == B_OVERFLOW) {
				bo = (BOVERFLOW *)bk;
				size = bo->tlen;
				if (size > space) {
					ret = ENOMEM;
					space = *spacep + size;
					break;
				}
				if (first == 0 && keyoff != NULL) {
					*offp-- = keyoff[0];
					*offp-- = keyoff[-1];
				}
				if ((ret = __bam_bulk_overflow(dbc,
				    bo->tlen, bo->pgno, np)) != 0)
					return (ret);
				space -= size;
				*offp-- = (int32_t)(np - dbuf);
				np += size;
			} else {
				if (need_pg) {
					dp = np;
					size = pagesize - HOFFSET(pg);
					if (space < size) {
						ret = ENOMEM;
						/* Return space required. */
						space = *spacep + size;
						break;
					}
					memcpy(dp,
					    (u_int8_t *)pg + HOFFSET(pg), size);
					need_pg = 0;
					space -= size;
					np += size;
				}
				if (first == 0 && keyoff != NULL) {
					*offp-- = keyoff[0];
					*offp-- = keyoff[-1];
				}
				size = bk->len;
				*offp-- = (int32_t)((inp[indx] - HOFFSET(pg))
				    + (dp - dbuf) + SSZA(BKEYDATA, data));
			}
			*offp-- = (int32_t)size;
			first = 0;
			if (no_dup)
				break;
contin:
			indx++;
			if (opd->dbtype == DB_RECNO)
				cp->recno++;
		} while (indx < NUM_ENT(pg));
		if (no_dup)
			break;
		cp->indx = indx;

	} while (ret == 0);

	/* Return the updated information. */
	*spacep = space;
	*offpp = offp;
	*dpp = np;

	/*
	 * If we ran out of space back up the pointer.
	 * If we did not return any dups or reached the end, close the opd.
	 */
	if (ret == ENOMEM) {
		if (opd->dbtype == DB_RECNO) {
			if (--cp->recno == 0)
				goto close_opd;
		} else if (indx != 0)
			cp->indx--;
		else {
			t_ret = __bam_c_prev(opd);
			if (t_ret == DB_NOTFOUND)
				goto close_opd;
			if (t_ret != 0)
				ret = t_ret;
		}
	} else if (keyoff == NULL && ret == DB_NOTFOUND) {
		cp->indx--;
		if (opd->dbtype == DB_RECNO)
			--cp->recno;
	} else if (indx == 0 || ret == DB_NOTFOUND) {
close_opd:
		if (ret == DB_NOTFOUND)
			ret = 0;
		if ((t_ret = __db_c_close(opd)) != 0 && ret == 0)
			ret = t_ret;
		((BTREE_CURSOR *)dbc->internal)->opd = NULL;
	}
	if (ret == DB_NOTFOUND)
		ret = 0;

	return (ret);
}

/*
 * __bam_getbothc --
 *	Search for a matching data item on a join.
 */
static int
__bam_getbothc(dbc, data)
	DBC *dbc;
	DBT *data;
{
	BTREE_CURSOR *cp;
	DB *dbp;
	DB_MPOOLFILE *mpf;
	int cmp, exact, ret;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;

	/*
	 * Acquire the current page.  We have at least a read-lock
	 * already.  The caller may have set DB_RMW asking for a
	 * write lock, but upgrading to a write lock has no better
	 * chance of succeeding now instead of later, so don't try.
	 */
	if ((ret = __memp_fget(mpf, &cp->pgno, 0, &cp->page)) != 0)
		return (ret);

	/*
	 * An off-page duplicate cursor.  Search the remaining duplicates
	 * for one which matches (do a normal btree search, then verify
	 * that the retrieved record is greater than the original one).
	 */
	if (F_ISSET(dbc, DBC_OPD)) {
		/*
		 * Check to make sure the desired item comes strictly after
		 * the current position;  if it doesn't, return DB_NOTFOUND.
		 */
		if ((ret = __bam_cmp(dbp, data, cp->page, cp->indx,
		    dbp->dup_compare == NULL ? __bam_defcmp : dbp->dup_compare,
		    &cmp)) != 0)
			return (ret);

		if (cmp <= 0)
			return (DB_NOTFOUND);

		/* Discard the current page, we're going to do a full search. */
		if ((ret = __memp_fput(mpf, cp->page, 0)) != 0)
			return (ret);
		cp->page = NULL;

		return (__bam_c_search(dbc,
		    PGNO_INVALID, data, DB_GET_BOTH, &exact));
	}

	/*
	 * We're doing a DBC->c_get(DB_GET_BOTHC) and we're already searching
	 * a set of on-page duplicates (either sorted or unsorted).  Continue
	 * a linear search from after the current position.
	 *
	 * (Note that we could have just finished a "set" of one duplicate,
	 * i.e. not a duplicate at all, but the following check will always
	 * return DB_NOTFOUND in this case, which is the desired behavior.)
	 */
	if (cp->indx + P_INDX >= NUM_ENT(cp->page) ||
	    !IS_DUPLICATE(dbc, cp->indx, cp->indx + P_INDX))
		return (DB_NOTFOUND);
	cp->indx += P_INDX;

	return (__bam_getboth_finddatum(dbc, data, DB_GET_BOTH));
}

/*
 * __bam_getboth_finddatum --
 *	Find a matching on-page data item.
 */
static int
__bam_getboth_finddatum(dbc, data, flags)
	DBC *dbc;
	DBT *data;
	u_int32_t flags;
{
	BTREE_CURSOR *cp;
	DB *dbp;
	db_indx_t base, lim, top;
	int cmp, ret;

	COMPQUIET(cmp, 0);

	dbp = dbc->dbp;
	cp = (BTREE_CURSOR *)dbc->internal;

	/*
	 * Called (sometimes indirectly) from DBC->get to search on-page data
	 * item(s) for a matching value.  If the original flag was DB_GET_BOTH
	 * or DB_GET_BOTH_RANGE, the cursor is set to the first undeleted data
	 * item for the key.  If the original flag was DB_GET_BOTHC, the cursor
	 * argument is set to the first data item we can potentially return.
	 * In both cases, there may or may not be additional duplicate data
	 * items to search.
	 *
	 * If the duplicates are not sorted, do a linear search.
	 */
	if (dbp->dup_compare == NULL) {
		for (;; cp->indx += P_INDX) {
			if (!IS_CUR_DELETED(dbc) &&
			    (ret = __bam_cmp(dbp, data, cp->page,
			    cp->indx + O_INDX, __bam_defcmp, &cmp)) != 0)
				return (ret);
			if (cmp == 0)
				return (0);

			if (cp->indx + P_INDX >= NUM_ENT(cp->page) ||
			    !IS_DUPLICATE(dbc, cp->indx, cp->indx + P_INDX))
				break;
		}
		return (DB_NOTFOUND);
	}

	/*
	 * If the duplicates are sorted, do a binary search.  The reason for
	 * this is that large pages and small key/data pairs result in large
	 * numbers of on-page duplicates before they get pushed off-page.
	 *
	 * Find the top and bottom of the duplicate set.  Binary search
	 * requires at least two items, don't loop if there's only one.
	 */
	for (base = top = cp->indx; top < NUM_ENT(cp->page); top += P_INDX)
		if (!IS_DUPLICATE(dbc, cp->indx, top))
			break;
	if (base == (top - P_INDX)) {
		if  ((ret = __bam_cmp(dbp, data,
		    cp->page, cp->indx + O_INDX, dbp->dup_compare, &cmp)) != 0)
			return (ret);
		return (cmp == 0 ||
		    (cmp < 0 && flags == DB_GET_BOTH_RANGE) ? 0 : DB_NOTFOUND);
	}

	for (lim = (top - base) / (db_indx_t)P_INDX; lim != 0; lim >>= 1) {
		cp->indx = base + ((lim >> 1) * P_INDX);
		if ((ret = __bam_cmp(dbp, data, cp->page,
		    cp->indx + O_INDX, dbp->dup_compare, &cmp)) != 0)
			return (ret);
		if (cmp == 0) {
			/*
			 * XXX
			 * No duplicate duplicates in sorted duplicate sets,
			 * so there can be only one.
			 */
			if (!IS_CUR_DELETED(dbc))
				return (0);
			break;
		}
		if (cmp > 0) {
			base = cp->indx + P_INDX;
			--lim;
		}
	}

	/* No match found; if we're looking for an exact match, we're done. */
	if (flags == DB_GET_BOTH)
		return (DB_NOTFOUND);

	/*
	 * Base is the smallest index greater than the data item, may be zero
	 * or a last + O_INDX index, and may be deleted.  Find an undeleted
	 * item.
	 */
	cp->indx = base;
	while (cp->indx < top && IS_CUR_DELETED(dbc))
		cp->indx += P_INDX;
	return (cp->indx < top ? 0 : DB_NOTFOUND);
}

/*
 * __bam_c_put --
 *	Put using a cursor.
 */
static int
__bam_c_put(dbc, key, data, flags, pgnop)
	DBC *dbc;
	DBT *key, *data;
	u_int32_t flags;
	db_pgno_t *pgnop;
{
	BTREE_CURSOR *cp;
	DB *dbp;
	DBT dbt;
	DB_MPOOLFILE *mpf;
	db_pgno_t root_pgno;
	u_int32_t iiop;
	int cmp, exact, own, ret, stack;
	void *arg;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;
	root_pgno = cp->root;
	prefault_dbp = dbp;

split:	ret = stack = 0;
	switch (flags) {
	case DB_AFTER:
	case DB_BEFORE:
	case DB_CURRENT:
		iiop = flags;
		own = 1;

		/*
		 * If the Btree has record numbers (and we're not replacing an
		 * existing record), we need a complete stack so that we can
		 * adjust the record counts.  The check for flags == DB_CURRENT
		 * is superfluous but left in for clarity.  (If C_RECNUM is set
		 * we know that flags must be DB_CURRENT, as DB_AFTER/DB_BEFORE
		 * are illegal in a Btree unless it's configured for duplicates
		 * and you cannot configure a Btree for both record renumbering
		 * and duplicates.)
		 */
		if (flags == DB_CURRENT &&
		    F_ISSET(cp, C_RECNUM) && F_ISSET(cp, C_DELETED)) {
			if ((ret = __bam_c_getstack(dbc)) != 0)
				goto err;
			/*
			 * Initialize the cursor from the stack.  Don't take
			 * the page number or page index, they should already
			 * be set.
			 */
			cp->page = cp->csp->page;
			cp->lock = cp->csp->lock;
			cp->lock_mode = cp->csp->lock_mode;

			stack = 1;
			break;
		}

		/* Acquire the current page with a write lock. */
		ACQUIRE_WRITE_LOCK(dbc, ret);
		if (ret != 0)
			goto err;
		if ((ret = __memp_fget(mpf, &cp->pgno, 0, &cp->page)) != 0)
			goto err;
		break;
	case DB_KEYFIRST:
	case DB_KEYLAST:
	case DB_NODUPDATA:
		own = 0;
		/*
		 * Searching off-page, sorted duplicate tree: do a tree search
		 * for the correct item; __bam_c_search returns the smallest
		 * slot greater than the key, use it.
		 *
		 * See comment below regarding where we can start the search.
		 */
		if (F_ISSET(dbc, DBC_OPD)) {
			if ((ret = __bam_c_search(dbc,
			    F_ISSET(cp, C_RECNUM) ? cp->root : root_pgno,
			    data, flags, &exact)) != 0)
				goto err;
			stack = 1;

			/* Disallow "sorted" duplicate duplicates. */
			if (exact) {
				if (IS_DELETED(dbp, cp->page, cp->indx)) {
					iiop = DB_CURRENT;
					break;
				}
				ret = __db_duperr(dbp, flags);
				goto err;
			}
			iiop = DB_BEFORE;
			break;
		}

		/*
		 * Searching a btree.
		 *
		 * If we've done a split, we can start the search from the
		 * parent of the split page, which __bam_split returned
		 * for us in root_pgno, unless we're in a Btree with record
		 * numbering.  In that case, we'll need the true root page
		 * in order to adjust the record count.
		 */
		if ((ret = __bam_c_search(dbc,
		    F_ISSET(cp, C_RECNUM) ? cp->root : root_pgno, key,
		    flags == DB_KEYFIRST || dbp->dup_compare != NULL ?
		    DB_KEYFIRST : DB_KEYLAST, &exact)) != 0)
			goto err;
		stack = 1;

		if(gbl_skip_cget_in_db_put) {
		    /* AZ: if we found exact and it was DB_NODUPDATA then
		    * we could have a dup problem and we should return DB_KEYEXIST
		    */
		    if(flags == DB_NODUPDATA && exact)
		        return DB_KEYEXIST;
		}

		/*
		 * If we don't have an exact match, __bam_c_search returned
		 * the smallest slot greater than the key, use it.
		 */
		if (!exact) {
			iiop = DB_KEYFIRST;
			break;
		}

		/*
		 * If duplicates aren't supported, replace the current item.
		 * (If implementing the DB->put function, our caller already
		 * checked the DB_NOOVERWRITE flag.)
		 */
		if (!F_ISSET(dbp, DB_AM_DUP)) {
			iiop = DB_CURRENT;
			break;
		}

		/*
		 * If we find a matching entry, it may be an off-page duplicate
		 * tree.  Return the page number to our caller, we need a new
		 * cursor.
		 */
		if (pgnop != NULL && __bam_isopd(dbc, pgnop))
			goto done;

		/* If the duplicates aren't sorted, move to the right slot. */
		if (dbp->dup_compare == NULL) {
			if (flags == DB_KEYFIRST)
				iiop = DB_BEFORE;
			else
				for (;; cp->indx += P_INDX)
					if (cp->indx + P_INDX >=
					    NUM_ENT(cp->page) ||
					    !IS_DUPLICATE(dbc, cp->indx,
						cp->indx + P_INDX)) {
						iiop = DB_AFTER;
						break;
					}
			break;
		}

		/*
		 * We know that we're looking at the first of a set of sorted
		 * on-page duplicates.  Walk the list to find the right slot.
		 */
		for (;; cp->indx += P_INDX) {
			if ((ret = __bam_cmp(dbp, data, cp->page,
			    cp->indx + O_INDX, dbp->dup_compare, &cmp)) != 0)
				goto err;
			if (cmp < 0) {
				iiop = DB_BEFORE;
				break;
			}

			/* Disallow "sorted" duplicate duplicates. */
			if (cmp == 0) {
				if (IS_DELETED(dbp, cp->page, cp->indx)) {
					iiop = DB_CURRENT;
					break;
				}
				ret = __db_duperr(dbp, flags);
				goto err;
			}

			if (cp->indx + P_INDX >= NUM_ENT(cp->page) ||
			    P_INP(dbp, ((PAGE *)cp->page))[cp->indx] !=
			    P_INP(dbp, ((PAGE *)cp->page))[cp->indx + P_INDX]) {
				iiop = DB_AFTER;
				break;
			}
		}
		break;
	default:
		ret = __db_unknown_flag(dbp->dbenv, "__bam_c_put", flags);
		goto err;
	}

	switch (ret = __bam_iitem(dbc, key, data, iiop, 0)) {
	case 0:
		break;
	case DB_NEEDSPLIT:
		/*
		 * To split, we need a key for the page.  Either use the key
		 * argument or get a copy of the key from the page.
		 */
		if (flags == DB_AFTER ||
		    flags == DB_BEFORE || flags == DB_CURRENT) {
			memset(&dbt, 0, sizeof(DBT));
			if ((ret = __db_ret(dbp, cp->page, 0, &dbt,
			    &dbc->my_rkey.data, &dbc->my_rkey.ulen)) != 0)
				goto err;
			arg = &dbt;
		} else
			arg = F_ISSET(dbc, DBC_OPD) ? data : key;

		/*
		 * SR [#6059]
		 * If we do not own a lock on the page anymore then
		 * clear the cursor so we don't point at it.
		 * Even if we call __bam_stkrel above we still
		 * may have entered the routine with the cursor
		 * posistioned to a particular record.  This
		 * is in the case where C_RECNUM is set.
		 */

		/* Invalidate the cursor before releasing the pagelock */
		if (own == 0) {
			cp->pgno = PGNO_INVALID;
			cp->indx = 0;
		}

		/*
		 * Discard any locks and pinned pages (the locks are discarded
		 * even if we're running with transactions, as they lock pages
		 * that we're sorry we ever acquired).  If stack is set and the
		 * cursor entries are valid, they point to the same entries as
		 * the stack, don't free them twice.
		 */
		if (stack)
			ret = __bam_stkrel(dbc, STK_CLRDBC | STK_NOLOCK);
		else
			DISCARD_CUR(dbc, ret);
		if (ret != 0)
			goto err;

		/* Split the tree. */
		if ((ret = __bam_split(dbc, arg, &root_pgno)) != 0)
			return (ret);

		goto split;
	default:
		goto err;
	}

err:
done:	/*
	 * Discard any pages pinned in the tree and their locks, except for
	 * the leaf page.  Note, the leaf page participated in any stack we
	 * acquired, and so we have to adjust the stack as necessary.  If
	 * there was only a single page on the stack, we don't have to free
	 * further stack pages.
	 */
	if (stack && BT_STK_POP(cp) != NULL)
		(void)__bam_stkrel(dbc, 0);

	/*
	 * Regardless of whether we were successful or not, clear the delete
	 * flag.  If we're successful, we either moved the cursor or the item
	 * is no longer deleted.  If we're not successful, then we're just a
	 * copy, no need to have the flag set.
	 *
	 * We may have instantiated off-page duplicate cursors during the put,
	 * so clear the deleted bit from the off-page duplicate cursor as well.
	 */
	F_CLR(cp, C_DELETED);
	if (cp->opd != NULL) {
		cp = (BTREE_CURSOR *)cp->opd->internal;
		F_CLR(cp, C_DELETED);
	}
	prefault_dbp = NULL;

	return (ret);
}

/*
 * __bam_c_rget --
 *	Return the record number for a cursor.
 *
 * PUBLIC: int __bam_c_rget __P((DBC *, DBT *));
 */
int
__bam_c_rget(dbc, data)
	DBC *dbc;
	DBT *data;
{
	BTREE_CURSOR *cp;
	DB *dbp;
	DBT dbt;
	DB_MPOOLFILE *mpf;
	db_recno_t recno;
	int exact, ret, t_ret;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;

	/*
	 * Get the page with the current item on it.
	 * Get a copy of the key.
	 * Release the page, making sure we don't release it twice.
	 */
	if ((ret = __memp_fget(mpf, &cp->pgno, 0, &cp->page)) != 0)
		return (ret);
	memset(&dbt, 0, sizeof(DBT));
	if ((ret = __db_ret(dbp, cp->page,
	    cp->indx, &dbt, &dbc->my_rkey.data, &dbc->my_rkey.ulen)) != 0)
		goto err;
	ret = __memp_fput(mpf, cp->page, 0);
	cp->page = NULL;
	if (ret != 0)
		return (ret);

	if ((ret = __bam_search(dbc, PGNO_INVALID, &dbt,
	    F_ISSET(dbc, DBC_RMW) ? S_FIND_WR : S_FIND,
	    1, &recno, &exact)) != 0)
		goto err;

	ret = __db_retcopy(dbp->dbenv, data,
	    &recno, sizeof(recno), &dbc->rdata->data, &dbc->rdata->ulen);

	/* Release the stack. */
err:	if ((t_ret = __bam_stkrel(dbc, 0)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}

/*
 * __bam_c_writelock --
 *	Upgrade the cursor to a write lock.
 */
static int
__bam_c_writelock(dbc)
	DBC *dbc;
{
	BTREE_CURSOR *cp;
	int ret;

	cp = (BTREE_CURSOR *)dbc->internal;

	if (cp->lock_mode == DB_LOCK_WRITE)
		return (0);

	/*
	 * When writing to an off-page duplicate tree, we need to have the
	 * appropriate page in the primary tree locked.  The general DBC
	 * code calls us first with the primary cursor so we can acquire the
	 * appropriate lock.
	 */
	ACQUIRE_WRITE_LOCK(dbc, ret);
	return (ret);
}

static void
__pageorder_scan_warning(const char *func)
{
	static int lastpr = 0;

	if (!gbl_disable_overflow_page_trace && time(NULL) > lastpr + 1) {
		logmsg(LOGMSG_WARN, "%s: WARNING: overflow pg in pageorder scan.\n",
		    func);
		lastpr = time(NULL);
	}
}

/*
 * __bam_c_first --
 *	Return the first record.
 */
static int
__bam_c_first(dbc)
	DBC *dbc;
{
	BTREE_CURSOR *cp;
	db_pgno_t pgno;
	int ret;

	cp = (BTREE_CURSOR *)dbc->internal;
	ret = 0;


	__bam_zero_counts(dbc);

	/* Switch to pgorder mode if specified in the cursor. */
	if (F_ISSET(dbc, DBC_PAGE_ORDER)) {

		DB_ASSERT(!F_ISSET(dbc, DBC_RMW));

		/* Scan from page 0 until we hit a leaf. */
		for (pgno = 0;;) {

			pgno = __bam_pgorder_next(dbc, pgno);
			ACQUIRE_CUR_NOCOUPLE(dbc, DB_LOCK_READ, pgno, 0, ret);

			if (ret != 0) {
				if (ret == DB_PAGE_NOTFOUND) {
					return DB_NOTFOUND;
				}
				return ret;
			}

			if (TYPE(cp->page) == P_OVERFLOW) {
				__pageorder_scan_warning(__func__);
			}

			if (ISLEAF(cp->page))
				break;

			dbc->skipcount++;
		}
	}

	else {

		/* Walk down the left-hand side of the tree. */
		for (pgno = cp->root;;) {

			ACQUIRE_CUR_COUPLE(dbc, DB_LOCK_READ, pgno, ret);
			if (ret != 0)
				return (ret);

			/* If we find a leaf page, we're done. */
			if (ISLEAF(cp->page))
				break;
#if USE_BTPF
			trk_descent(dbc->dbp, dbc, cp->page, 0);
#endif
			pgno = GET_BINTERNAL(dbc->dbp, cp->page, 0)->pgno;

		}
#if USE_BTPF
		trk_leaf(dbc, cp->page, 0);
		crsr_jump(dbc);
#endif
	}

	/* If we want a write lock instead of a read lock, get it now. */
	if (F_ISSET(dbc, DBC_RMW)) {
		ACQUIRE_WRITE_LOCK(dbc, ret);
		if (ret != 0)
			return (ret);
	}

	cp->indx = 0;

	/* If on an empty page or a deleted record, move to the next one. */
	if (NUM_ENT(cp->page) == 0 || IS_CUR_DELETED(dbc))
		if ((ret = __bam_c_next(dbc, 0, 0)) != 0)
			return (ret);

	return (0);
}

/*
 * __bam_c_last --
 *	Return the last record.
 */
static int
__bam_c_last(dbc)
	DBC *dbc;
{
	BTREE_CURSOR *cp;
	db_pgno_t pgno;
	int ret;

	cp = (BTREE_CURSOR *)dbc->internal;
	ret = 0;
	db_indx_t n_ent = 0;

	/* Assert that this isn't a page-order cursor. */
	DB_ASSERT(!F_ISSET(dbc, DBC_PAGE_ORDER));

	/* Walk down the right-hand side of the tree. */
	for (pgno = cp->root;;) {
		ACQUIRE_CUR_COUPLE(dbc, DB_LOCK_READ, pgno, ret);
		if (ret != 0)
			return (ret);


		/* If we find a leaf page, we're done. */
		if (ISLEAF(cp->page))
			break;
		n_ent = NUM_ENT(cp->page) == 0 ? 0 : NUM_ENT(cp->page) - O_INDX;
#if USE_BTPF
		trk_descent(dbc->dbp, dbc, cp->page, n_ent);
#endif
		pgno = GET_BINTERNAL(dbc->dbp, cp->page,
		    NUM_ENT(cp->page) - O_INDX)->pgno;

	}
#if USE_BTPF
	trk_leaf(dbc, cp->page, NUM_ENT(cp->page));
	crsr_jump(dbc);
#endif
	/* If we want a write lock instead of a read lock, get it now. */
	if (F_ISSET(dbc, DBC_RMW)) {
		ACQUIRE_WRITE_LOCK(dbc, ret);
		if (ret != 0)
			return (ret);
	}

	cp->indx = NUM_ENT(cp->page) == 0 ? 0 :
	    NUM_ENT(cp->page) - (TYPE(cp->page) == P_LBTREE ? P_INDX : O_INDX);

	/* If on an empty page or a deleted record, move to the previous one. */
	if (NUM_ENT(cp->page) == 0 || IS_CUR_DELETED(dbc))
		if ((ret = __bam_c_prev(dbc)) != 0)
			return (ret);

	return (0);
}

/*
 * __bam_c_next --
 *	Move to the next record.
 */
static int
__bam_c_next(dbc, initial_move, deleted_okay)
	DBC *dbc;
	int initial_move, deleted_okay;
{
	BTREE_CURSOR *cp;
	db_indx_t adjust;
	db_lockmode_t lock_mode;
	db_pgno_t pgno;
	int discard, ret;
	int op_pf = 0;

	cp = (BTREE_CURSOR *)dbc->internal;
	ret = discard = 0;

	/*
	 * We're either moving through a page of duplicates or a btree leaf
	 * page.
	 *
	 * !!!
	 * This code handles empty pages and pages with only deleted entries.
	 */
	if (F_ISSET(dbc, DBC_OPD)) {
		adjust = O_INDX;
		lock_mode = DB_LOCK_NG;
	} else {
		adjust = dbc->dbtype == DB_BTREE ? P_INDX : O_INDX;
		lock_mode =
		    F_ISSET(dbc, DBC_RMW) ? DB_LOCK_WRITE : DB_LOCK_READ;
	}

	if (cp->page == NULL) {
		if (F_ISSET(dbc, DBC_PAGE_ORDER)) {
			ACQUIRE_CUR_NOCOUPLE(dbc, lock_mode, cp->pgno, discard,
			    ret);

			if (0 == ret && TYPE(cp->page) == P_OVERFLOW) {
				__pageorder_scan_warning(__func__);
			}

			if (0 == ret && NUM_ENT(cp->page) == 0) {
				discard = 1;
			}
		} else {
			ACQUIRE_CUR(dbc, lock_mode, cp->pgno, ret);
		}
		if (ret != 0)
			return (ret);
	}

	if (initial_move)
		cp->indx += adjust;
	for (;;) {
		/*
		 * If at the end of the page, move to a subsequent page.
		 *
		 * !!!
		 * Check for >= NUM_ENT.  If the original search landed us on
		 * NUM_ENT, we may have incremented indx before the test.
		 */

		if (cp->indx >= NUM_ENT(cp->page)) {

			if (F_ISSET(dbc, DBC_PAGE_ORDER)) {
				/*
				 * If the user asked to discard pages,
				 * ACQUIRE_CUR_NOCOUPLE will request
				 * that __memp_fput return the page to
				 * the buffer pool at the beginning of
				 * the hash chain and with a priority
				 * of 0, and it will request that
				 * __memp_fget evict lower priority
				 * buffers on the first pass through
				 * the buffer pool.
				 *
				 * It's worth noting that the sequence
				 * of calls in a table-scan will fget
				 * and fput each page twice:
				 *
				 * 1. fget in ACQUIRE_CUR_NOCOUPLE
				 * below.
				 *
				 * 2. fput in cursor cleanup
				 * for NEXT (db_c_get->db_c_cleanup)
				 *
				 * 3. fget in ACQUIRE_CUR_NOCOUPLE
				 * above this loop.
				 *
				 * 4. fput in ACQUIRE_CUR_NOCOUPLE
				 * below.
				 *
				 * In order to reduce the chances of
				 * this being paged out while this is
				 * being used, only the second fput
				 * twiddles the priority to 0.
				 *
				 * Page order tablescan works
				 * "correctly" when we encounter
				 * sparse files: the page-in function
				 * treats a zero page as a hash page
				 * with 0 entries. The discard
				 * variable lowers the priority any
				 * pages which don't have data.
				 */
				do {
					pgno =
					    __bam_pgorder_next(dbc, cp->pgno);
					ACQUIRE_CUR_NOCOUPLE(dbc, lock_mode,
					    pgno, discard, ret);
					if (0 == ret &&
					    TYPE(cp->page) == P_OVERFLOW) {
						__pageorder_scan_warning
						    (__func__);
					}
					if (0 == ret &&
					    NUM_ENT(cp->page) == 0) {
						discard = 1;
					} else {
						discard = 0;
					}

					if (0 == ret &&
					    TYPE(cp->page) != P_LBTREE) {
						dbc->skipcount++;
					} else {
						break;
					}
				}
				while (1);

				if (ret != 0) {
					if (ret == DB_PAGE_NOTFOUND)
						return (DB_NOTFOUND);
					return (ret);
				}
			} else {
				pgno = NEXT_PGNO(cp->page);
				if (PGNO_INVALID == pgno)
					return (DB_NOTFOUND);
#if USE_BTPF
				// ####################### PRE FAULT
				crsr_pf_nxt(dbc);
				op_pf = 1;
				// ##################################
#endif
				ACQUIRE_CUR(dbc, lock_mode, pgno, ret);
				if (ret != 0)
					return (ret);
			}
			cp->indx = 0;

			continue;
		}
		if (!deleted_okay && IS_CUR_DELETED(dbc)) {
			cp->indx += adjust;
			continue;
		}
		break;
	}
#if USE_BTPF
	if (!op_pf)
		crsr_nxt(dbc);
#endif
	return (0);
}

/*
 * __bam_c_prev --
 *	Move to the previous record.
 */
static int
__bam_c_prev(dbc)
	DBC *dbc;
{
	BTREE_CURSOR *cp;
	db_indx_t adjust;
	db_lockmode_t lock_mode;
	db_pgno_t pgno;
	int discard, ret;

	cp = (BTREE_CURSOR *)dbc->internal;
	ret = discard = 0;
	int op_pf = 0;

	/*
	 * We're either moving through a page of duplicates or a btree leaf
	 * page.
	 *
	 * !!!
	 * This code handles empty pages and pages with only deleted entries.
	 */
	if (F_ISSET(dbc, DBC_OPD)) {
		adjust = O_INDX;
		lock_mode = DB_LOCK_NG;
	} else {
		adjust = dbc->dbtype == DB_BTREE ? P_INDX : O_INDX;
		lock_mode =
		    F_ISSET(dbc, DBC_RMW) ? DB_LOCK_WRITE : DB_LOCK_READ;
	}
	if (cp->page == NULL) {
		if (F_ISSET(dbc, DBC_PAGE_ORDER)) {
			ACQUIRE_CUR_NOCOUPLE(dbc, lock_mode, cp->pgno, discard,
			    ret);

			if (0 == ret && TYPE(cp->page) == P_OVERFLOW) {
				__pageorder_scan_warning(__func__);
			}

			if (0 == ret && NUM_ENT(cp->page) == 0) {
				discard = 1;
			}
		} else {
			ACQUIRE_CUR(dbc, lock_mode, cp->pgno, ret);
		}
		if (ret != 0)
			return (ret);
	}

	for (;;) {
		/* If at the beginning of the page, move to a previous one. */
		if (cp->indx == 0) {

			crsr_pf_prv(dbc);
			/* See comments in __bam_c_next. */
			if (F_ISSET(dbc, DBC_PAGE_ORDER)) {
				do {
					pgno =
					    __bam_pgorder_prev(dbc, cp->pgno);

					if (pgno == PGNO_INVALID) {
						return (DB_NOTFOUND);
					}

					ACQUIRE_CUR_NOCOUPLE(dbc, lock_mode,
					    pgno, discard, ret);
					if (0 == ret &&
					    TYPE(cp->page) == P_OVERFLOW) {
						__pageorder_scan_warning
						    (__func__);
					}
					if (0 == ret && NUM_ENT(cp->page) == 0) {
						discard = 1;
					} else {
						discard = 0;
					}
				}
				while (ret == 0 && TYPE(cp->page) != P_LBTREE);

				if (ret != 0) {
					if (ret == DB_PAGE_NOTFOUND)
						return (DB_NOTFOUND);
					return (ret);
				}
			} else {
				if ((pgno =
					PREV_PGNO(cp->page)) == PGNO_INVALID)
					return (DB_NOTFOUND);
#if USE_BTPF
				crsr_pf_prv(dbc);
				op_pf = 1;
#endif
				ACQUIRE_CUR(dbc, lock_mode, pgno, ret);
				if (ret != 0)
					return (ret);
			}

			if ((cp->indx = NUM_ENT(cp->page)) == 0)
				continue;
		}

		/* Ignore deleted records. */
		cp->indx -= adjust;
		if (IS_CUR_DELETED(dbc))
			continue;

		break;
	}
#if USE_BTPF
	if (!op_pf)
		crsr_prv(dbc);
#endif
	return (0);
}

/*
 * __bam_c_search --
 *	Move to a specified record.
 */
static int
__bam_c_search(dbc, root_pgno, key, flags, exactp)
	DBC *dbc;
	db_pgno_t root_pgno;
	const DBT *key;
	u_int32_t flags;
	int *exactp;
{
	BTREE *t;
	BTREE_CURSOR *cp;
	DB *dbp;
	PAGE *h;
	db_indx_t indx, *inp;
	db_pgno_t bt_lpgno;
	db_recno_t recno;
	u_int32_t sflags;
	int cmp, ret;
	int oldret;

	dbp = dbc->dbp;
	cp = (BTREE_CURSOR *)dbc->internal;
	t = dbp->bt_internal;
	ret = 0;

	/* Probe zaps counts */
	__bam_zero_counts(dbc);

	/*
	 * Find an entry in the database.  Discard any lock we currently hold,
	 * we're going to search the tree.
	 */
	DISCARD_CUR(dbc, ret);
	if (ret != 0)
		return (ret);

	INTERNAL_PTR_CHECK(cp == dbc->internal);

	switch (flags) {
	case DB_SET_RECNO:
		if ((ret = __ram_getno(dbc, key, &recno, 0)) != 0)
			return (ret);
		sflags = (F_ISSET(dbc, DBC_RMW) ? S_FIND_WR : S_FIND) | S_EXACT;
		if ((ret = __bam_rsearch(dbc, &recno, sflags, 1, exactp)) != 0)
			return (ret);
		break;
	case DB_SET:
	case DB_GET_BOTH:
		sflags = (F_ISSET(dbc, DBC_RMW) ? S_FIND_WR : S_FIND) | S_EXACT;
		goto search;
	case DB_GET_BOTH_RANGE:
		sflags = (F_ISSET(dbc, DBC_RMW) ? S_FIND_WR : S_FIND);
		goto search;
	case DB_SET_RANGE:
		sflags =
		    (F_ISSET(dbc, DBC_RMW) ? S_WRITE : S_READ) | S_DUPFIRST;
		goto search;
	case DB_KEYFIRST:
		sflags = S_KEYFIRST;
		goto fast_search;
	case DB_KEYLAST:
	case DB_NODUPDATA:
		sflags = S_KEYLAST;
fast_search:	/*
		 * If the application has a history of inserting into the first
		 * or last pages of the database, we check those pages first to
		 * avoid doing a full search.
		 *
		 * If the tree has record numbers, we need a complete stack so
		 * that we can adjust the record counts, so fast_search isn't
		 * possible.
		 */
		if (F_ISSET(cp, C_RECNUM))
			goto search;

		/*
		 * !!!
		 * We do not mutex protect the t->bt_lpgno field, which means
		 * that it can only be used in an advisory manner.  If we find
		 * page we can use, great.  If we don't, we don't care, we do
		 * it the slow way instead.  Regardless, copy it into a local
		 * variable, otherwise we might acquire a lock for a page and
		 * then read a different page because it changed underfoot.
		 */
		bt_lpgno = t->bt_lpgno;

		/*
		 * If the tree has no history of insertion, do it the slow way.
		 */
		if (bt_lpgno == PGNO_INVALID)
			goto search;

		/* Lock and retrieve the page on which we last inserted. */
		h = NULL;
		ACQUIRE_CUR(dbc, DB_LOCK_WRITE, bt_lpgno, ret);
		if (ret != 0)
			goto fast_miss;

		h = cp->page;
		inp = P_INP(dbp, h);
		/*
		 * It's okay if the page type isn't right or it's empty, it
		 * just means that the world changed.
		 */
		if (TYPE(h) != P_LBTREE || NUM_ENT(h) == 0)
			goto fast_miss;

		/*
		 * What we do here is test to see if we're at the beginning or
		 * end of the tree and if the new item sorts before/after the
		 * first/last page entry.  We don't try and catch inserts into
		 * the middle of the tree (although we could, as long as there
		 * were two keys on the page and we saved both the index and
		 * the page number of the last insert).
		 */
		if (h->next_pgno == PGNO_INVALID) {
			indx = NUM_ENT(h) - P_INDX;
			if ((ret = __bam_cmp(dbp,
			    key, h, indx, t->bt_compare, &cmp)) != 0)
				return (ret);

			if (cmp < 0)
				goto try_begin;
			if (cmp > 0) {
				indx += P_INDX;
				goto fast_hit;
			}

			/*
			 * Found a duplicate.  If doing DB_KEYLAST, we're at
			 * the correct position, otherwise, move to the first
			 * of the duplicates.  If we're looking at off-page
			 * duplicates, duplicate duplicates aren't permitted,
			 * so we're done.
			 */
			if (flags == DB_KEYLAST)
				goto fast_hit;
			for (;
			    indx > 0 && inp[indx - P_INDX] == inp[indx];
			    indx -= P_INDX);
			goto fast_hit;
		}
try_begin:	if (h->prev_pgno == PGNO_INVALID) {
			indx = 0;
			if ((ret = __bam_cmp(dbp,
			    key, h, indx, t->bt_compare, &cmp)) != 0)
				return (ret);

			if (cmp > 0)
				goto fast_miss;
			if (cmp < 0)
				goto fast_hit;

			/*
			 * Found a duplicate.  If doing DB_KEYFIRST, we're at
			 * the correct position, otherwise, move to the last
			 * of the duplicates.  If we're looking at off-page
			 * duplicates, duplicate duplicates aren't permitted,
			 * so we're done.
			 */
			if (flags == DB_KEYFIRST)
				goto fast_hit;
			for (;
			    indx < (db_indx_t)(NUM_ENT(h) - P_INDX) &&
			    inp[indx] == inp[indx + P_INDX];
			    indx += P_INDX);
			goto fast_hit;
		}
		goto fast_miss;

fast_hit:	/* Set the exact match flag, we may have found a duplicate. */
		*exactp = cmp == 0;

		/*
		 * Insert the entry in the stack.  (Our caller is likely to
		 * call __bam_stkrel() after our return.)
		 */
		BT_STK_CLR(cp);
		BT_STK_ENTER(dbp->dbenv,
		    cp, h, indx, cp->lock, cp->lock_mode, ret);
		if (ret != 0)
			return (ret);
		break;

fast_miss:	/*
		 * This was not the right page, so we do not need to retain
		 * the lock even in the presence of transactions.
		 */
		oldret = ret;
		DISCARD_CUR(dbc, ret);
		cp->pgno = PGNO_INVALID;
		(void)__LPUT(dbc, cp->lock);
		if (ret != 0)
			return (ret);
		if (oldret == DB_LOCK_DEADLOCK) 
		    return oldret;

search:		if ((ret = __bam_search(dbc, root_pgno,
		     key, sflags, 1, NULL, exactp)) != 0)
			return (ret);

		INTERNAL_PTR_CHECK(cp == dbc->internal);
		break;
	default:
		return (__db_unknown_flag(dbp->dbenv, "__bam_c_search", flags));
	}

	/* Make sure this hasn't changed */
	INTERNAL_PTR_CHECK(cp == dbc->internal);

	/* Initialize the cursor from the stack. */
	cp->page = cp->csp->page;
	cp->pgno = cp->csp->page->pgno;
	cp->indx = cp->csp->indx;
	cp->lock = cp->csp->lock;
	cp->lock_mode = cp->csp->lock_mode;

	/*
	 * If we inserted a key into the first or last slot of the tree,
	 * remember where it was so we can do it more quickly next time.
	 * If there are duplicates and we are inserting into the last slot,
	 * the cursor will point _to_ the last item, not after it, which
	 * is why we subtract P_INDX below.
	 */
	if (TYPE(cp->page) == P_LBTREE &&
	    (flags == DB_KEYFIRST || flags == DB_KEYLAST))
		t->bt_lpgno =
		    (NEXT_PGNO(cp->page) == PGNO_INVALID &&
		    cp->indx >= NUM_ENT(cp->page) - P_INDX) ||
		    (PREV_PGNO(cp->page) == PGNO_INVALID &&
		    cp->indx == 0) ? cp->pgno : PGNO_INVALID;
	return (0);
}

/*
 * __bam_c_physdel --
 *	Physically remove an item from the page.
 */
static int
__bam_c_physdel(dbc)
	DBC *dbc;
{
	BTREE_CURSOR *cp;
	DB *dbp;
	DBT key;
	DB_LOCK lock;
	DB_MPOOLFILE *mpf;
	PAGE *h;
	db_pgno_t pgno;
	int delete_page, empty_page, exact, level, ret;

	dbp = dbc->dbp;
	memset(&key, 0, sizeof(DBT));
	mpf = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;
	delete_page = empty_page = ret = 0;

	/* If the page is going to be emptied, consider deleting it. */
	delete_page = empty_page =
	    NUM_ENT(cp->page) == (TYPE(cp->page) == P_LBTREE ? 2 : 1);

	/*
	 * Check if the application turned off reverse splits.  Applications
	 * can't turn off reverse splits in off-page duplicate trees, that
	 * space will never be reused unless the exact same key is specified.
	 */
	if (delete_page &&
	    !F_ISSET(dbc, DBC_OPD) && F_ISSET(dbp, DB_AM_REVSPLITOFF))
		delete_page = 0;

	/*
	 * We never delete the last leaf page.  (Not really true -- we delete
	 * the last leaf page of off-page duplicate trees, but that's handled
	 * by our caller, not down here.)
	 */
	if (delete_page && cp->pgno == cp->root)
		delete_page = 0;

	/*
	 * To delete a leaf page other than an empty root page, we need a
	 * copy of a key from the page.  Use the 0th page index since it's
	 * the last key the page held.
	 *
	 * !!!
	 * Note that because __bam_c_physdel is always called from a cursor
	 * close, it should be safe to use the cursor's own "my_rkey" memory
	 * to temporarily hold this key.  We shouldn't own any returned-data
	 * memory of interest--if we do, we're in trouble anyway.
	 */
	if (delete_page)
		if ((ret = __db_ret(dbp, cp->page,
		    0, &key, &dbc->my_rkey.data, &dbc->my_rkey.ulen)) != 0)
			return (ret);

	/*
	 * Delete the items.  If page isn't empty, we adjust the cursors.
	 *
	 * !!!
	 * The following operations to delete a page may deadlock.  The easy
	 * scenario is if we're deleting an item because we're closing cursors
	 * because we've already deadlocked and want to call txn->abort.  If
	 * we fail due to deadlock, we'll leave a locked, possibly empty page
	 * in the tree, which won't be empty long because we'll undo the delete
	 * when we undo the transaction's modifications.
	 *
	 * !!!
	 * Delete the key item first, otherwise the on-page duplicate checks
	 * in __bam_ditem() won't work!
	 */
	if (TYPE(cp->page) == P_LBTREE) {
		if ((ret = __bam_ditem(dbc, cp->page, cp->indx)) != 0)
			return (ret);
		if (!empty_page)
			if ((ret = __bam_ca_di(dbc,
			    PGNO(cp->page), cp->indx, -1)) != 0)
				return (ret);
	}
	if ((ret = __bam_ditem(dbc, cp->page, cp->indx)) != 0)
		return (ret);
	if (!empty_page)
		if ((ret = __bam_ca_di(dbc, PGNO(cp->page), cp->indx, -1)) != 0)
			return (ret);

	/* If we're not going to try and delete the page, we're done. */
	if (!delete_page)
		return (0);

	/*
	 * Call __bam_search to reacquire the empty leaf page, but this time
	 * get both the leaf page and its parent, locked.  Jump back up the
	 * tree, until we have the top pair of pages that we want to delete.
	 * Once we have the top page that we want to delete locked, lock the
	 * underlying pages and check to make sure they're still empty.  If
	 * they are, delete them.
	 */
	for (level = LEAFLEVEL;; ++level) {
		/* Acquire a page and its parent, locked. */
		if ((ret = __bam_search(dbc, PGNO_INVALID,
		    &key, S_WRPAIR, level, NULL, &exact)) != 0)
			return (ret);

		/*
		 * If we reach the root or the parent page isn't going to be
		 * empty when we delete one record, stop.
		 */
		h = cp->csp[-1].page;
		if (h->pgno == cp->root || NUM_ENT(h) != 1)
			break;

		/* Discard the stack, retaining no locks. */
		(void)__bam_stkrel(dbc, STK_NOLOCK);
	}

	/*
	 * Move the stack pointer one after the last entry, we may be about
	 * to push more items onto the page stack.
	 */
	++cp->csp;

	/*
	 * cp->csp[-2].page is now the parent page, which we may or may not be
	 * going to delete, and cp->csp[-1].page is the first page we know we
	 * are going to delete.  Walk down the chain of pages, acquiring pages
	 * until we've acquired a leaf page.  Generally, this shouldn't happen;
	 * we should only see a single internal page with one item and a single
	 * leaf page with no items.  The scenario where we could see something
	 * else is if reverse splits were turned off for awhile and then turned
	 * back on.  That could result in all sorts of strangeness, e.g., empty
	 * pages in the tree, trees that looked like linked lists, and so on.
	 *
	 * !!!
	 * Sheer paranoia: if we find any pages that aren't going to be emptied
	 * by the delete, someone else added an item while we were walking the
	 * tree, and we discontinue the delete.  Shouldn't be possible, but we
	 * check regardless.
	 */
	for (h = cp->csp[-1].page;;) {
		if (ISLEAF(h)) {
			if (NUM_ENT(h) != 0)
				break;
			break;
		} else if (NUM_ENT(h) != 1)
			break;

		/*
		 * Get the next page, write lock it and push it onto the stack.
		 * We know it's index 0, because it can only have one element.
		 */
		switch (TYPE(h)) {
		case P_IBTREE:
			pgno = GET_BINTERNAL(dbp, h, 0)->pgno;
			break;
		case P_IRECNO:
			pgno = GET_RINTERNAL(dbp, h, 0)->pgno;
			break;
		default:
			return (__db_pgfmt(dbp->dbenv, PGNO(h)));
		}

		if ((ret =
		    __db_lget(dbc, 0, pgno, DB_LOCK_WRITE, 0, &lock)) != 0)
			break;
		if ((ret = __memp_fget(mpf, &pgno, 0, &h)) != 0)
			break;
		BT_STK_PUSH(dbp->dbenv, cp, h, 0, lock, DB_LOCK_WRITE, ret);
		if (ret != 0)
			break;
	}

	/* Adjust the cursor stack to reference the last page on the stack. */
	BT_STK_POP(cp);

	/*
	 * If everything worked, delete the stack, otherwise, release the
	 * stack and page locks without further damage.
	 */
	if (ret == 0)
		ret = __bam_dpages(dbc, cp->sp, 0);
	else
		(void)__bam_stkrel(dbc, 0);

	return (ret);
}

/*
 * __bam_c_getstack --
 *	Acquire a full stack for a cursor.
 */
static int
__bam_c_getstack(dbc)
	DBC *dbc;
{
	BTREE_CURSOR *cp;
	DB *dbp;
	DBT dbt;
	DB_MPOOLFILE *mpf;
	PAGE *h;
	int exact, ret, t_ret;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;

	/*
	 * Get the page with the current item on it.  The caller of this
	 * routine has to already hold a read lock on the page, so there
	 * is no additional lock to acquire.
	 */
	if ((ret = __memp_fget(mpf, &cp->pgno, 0, &h)) != 0)
		return (ret);

	/* Get a copy of a key from the page. */
	memset(&dbt, 0, sizeof(DBT));
	if ((ret = __db_ret(dbp,
	    h, 0, &dbt, &dbc->my_rkey.data, &dbc->my_rkey.ulen)) != 0)
		goto err;

	/* Get a write-locked stack for the page. */
	exact = 0;
	ret = __bam_search(dbc, PGNO_INVALID,
	    &dbt, S_KEYFIRST, 1, NULL, &exact);

err:	/* Discard the key and the page. */
	if ((t_ret = __memp_fput(mpf, h, 0)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}

/*
 * __bam_isopd --
 *	Return if the cursor references an off-page duplicate tree via its
 *	page number.
 */
static int
__bam_isopd(dbc, pgnop)
	DBC *dbc;
	db_pgno_t *pgnop;
{
	BOVERFLOW *bo;

	if (TYPE(dbc->internal->page) != P_LBTREE)
		return (0);

	bo = GET_BOVERFLOW(dbc->dbp,
	    dbc->internal->page, dbc->internal->indx + O_INDX);
	if (B_TYPE(bo) == B_DUPLICATE) {
		*pgnop = bo->pgno;
		return (1);
	}
	return (0);
}


/* COMDB2 MODIFICATION */
/* 
 * return the page locked by a cursor 
 *
 */
db_pgno_t
__bam_get_dbc_page(dbc)
	DBC *dbc;
{
	BTREE_CURSOR *cp = (BTREE_CURSOR *)dbc->internal;

	return cp->pgno;
}
