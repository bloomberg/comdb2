/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char copyright[] =
	"Copyright (c) 1996-2003\nSleepycat Software Inc.  All rights reserved.\n";
static const char revid[] =
	"$Id: env_recover.c,v 11.112 2003/09/13 18:46:20 bostic Exp $";
#endif

#include <arpa/inet.h>

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#if TIME_WITH_SYS_TIME
#include <sys/time.h>
#include <time.h>
#else
#if HAVE_SYS_TIME_H
#include <sys/time.h>
#else
#include <time.h>
#endif
#endif

#include <string.h>
#endif
#include <stdlib.h>
#include <unistd.h>

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/log.h"
#include "dbinc/txn.h"
#include "dbinc/mp.h"
#include "dbinc/db_am.h"
#include "dbinc/db_swap.h"
#include "dbinc_auto/db_auto.h"
#include <locks_wrap.h>



#include <printformats.h>

#include "dbinc/btree.h"
#include "dbinc/lock.h"

#include "list.h"
#include "logmsg.h"

#ifndef TESTSUITE
void bdb_get_writelock(void *bdb_state,
	const char *idstr, const char *funcname, int line);
void bdb_rellock(void *bdb_state, const char *funcname, int line);
int bdb_is_open(void *bdb_state);

extern int gbl_is_physical_replicant;
int gbl_apprec_gen;

#define BDB_WRITELOCK(idstr)	bdb_get_writelock(bdb_state, (idstr), __func__, __LINE__)
#define BDB_RELLOCK()		   bdb_rellock(bdb_state, __func__, __LINE__)

#else

#define BDB_WRITELOCK(x)
#define BDB_RELLOCK()

#endif

#include "dbinc/hmac.h"
#include "dbinc_auto/hmac_ext.h"

#include "printformats.h"

static int __log_earliest __P((DB_ENV *, DB_LOGC *, int32_t *, DB_LSN *));
static double __lsn_diff __P((DB_LSN *, DB_LSN *, DB_LSN *, u_int32_t, int));
static int __log_find_latest_checkpoint_before_lsn(DB_ENV *dbenv,
	DB_LOGC *logc, DB_LSN *max_lsn, DB_LSN *start_lsn);
static int __log_find_latest_checkpoint_before_lsn_try_harder(DB_ENV *dbenv,
	DB_LOGC *logc, DB_LSN *max_lsn, DB_LSN *foundlsn);
int gbl_ufid_dbreg_test = 0;
int gbl_ufid_log = 0;

/* Get the recovery LSN. */
int
__checkpoint_get_recovery_lsn(DB_ENV *dbenv, DB_LSN *lsnout)
{
	int rc;
	DB_LSN lsn;
	DB_LOGC *dbc = NULL;
	DBT dbt = { 0 };
	u_int32_t type;
	__txn_ckp_args *ckp = NULL;

	ZERO_LSN(*lsnout);

	rc = __checkpoint_get(dbenv, &lsn);
	if (rc) {
		logmsg(LOGMSG_ERROR, "__checkpoint_get rc %d\n", rc);
		goto err;
	}

	rc = dbenv->log_cursor(dbenv, &dbc, 0);
	if (rc) {
		logmsg(LOGMSG_ERROR, "log_cursor rc %d\n", rc);
		goto err;
	}

	dbt.flags = DB_DBT_MALLOC;

	rc = dbc->get(dbc, &lsn, &dbt, DB_SET);

	if (rc) {
		logmsg(LOGMSG_ERROR, "dbc->get rc %d\n", rc);
		goto err;
	}

	LOGCOPY_32(&type, dbt.data);
	normalize_rectype(&type);
	if (type != DB___txn_ckp) {
		logmsg(LOGMSG_ERROR, "checkpoint record unexpeted type %d\n", type);
		goto err;
	}

	rc = __txn_ckp_read(dbenv, dbt.data, &ckp);
	if (rc) {
		logmsg(LOGMSG_ERROR, "txn_ckp_read rc %d\n", rc);
		goto err;
	}

	*lsnout = ckp->ckp_lsn;

err:
	if (dbt.data)
		__os_ufree(dbenv, dbt.data);
	if (ckp)
		__os_free(dbenv, ckp);
	if (dbc)
		dbc->close(dbc, 0);

	return rc;
}

/* Get the checkpoint LSN. */
int
__checkpoint_get(DB_ENV *dbenv, DB_LSN *lsnout)
{
	struct __db_checkpoint ckpt = {{0}};
	int rc;

	/*
	 * we can't use __os_io here because that calls
	 * __checkpoint_verify, which calls __checkpoint_get, which
	 * calls __os_io which calls __checkpoint_verify ...
	 */

	rc = pread(dbenv->checkpoint->fd, &ckpt, 512, 0);
	if (rc != 512) {
		__db_err(dbenv, "can't read checkpoint record rc %d %s\n",
			errno, strerror(errno));
		return EINVAL;
	}

	if (lsnout)
		LOGCOPY_FROMLSN(lsnout, &ckpt.lsn);

	if (__db_check_chksum_no_crypto(dbenv, NULL, ckpt.chksum, &ckpt.lsn,
		sizeof(struct __db_checkpoint) -
		offsetof(struct __db_checkpoint, lsn), 0)) {
		__db_err(dbenv, "checkpoint checksum verification failed");
		__db_panic(dbenv, EINVAL);
	}

	return 0;
}

/* Note:  this places additional restrictions on berkeley.  File numbers passed
 * to this routine must come from files returned by dbenv->log_archive */
int
__checkpoint_ok_to_delete_log(DB_ENV *dbenv, int logfile)
{
	DB_LSN lsn;

	/* Don't delete a log file if the checkpoint record is in it or in an earlier log. */
	__checkpoint_get(dbenv, &lsn);
	if (logfile >= lsn.file)
		return 0;

	/* Don't delete a log file if the recovery lsn is in it or in an earlier log. */
	__checkpoint_get_recovery_lsn(dbenv, &lsn);
	if (logfile >= lsn.file)
		return 0;

	return 1;
}

void
berkdb_set_recovery(DB_ENV *dbenv)
{
	logmsg(LOGMSG_DEBUG, "setting recovery\n");

	F_SET((DB_LOG *)dbenv->lg_handle, DBLOG_RECOVER);
}

void
__fileid_track_init(dbenv)
	DB_ENV *dbenv;
{
	struct fileid_track *ft = &dbenv->fileid_track;
	ft->ranges = NULL;

	ft->numids = 0;
}

static int
__fileid_track_free(dbenv)
	DB_ENV *dbenv;
{
	int i;
	int ret;
	struct fileid_track *ft = &dbenv->fileid_track;

	for (i = 0; i < ft->numids; i++) {
		struct lsn_range *r, *next;

		r = listc_rtl(&ft->ranges[i]);
		while (r) {
			if (r->dbp) {
				ret = __db_close(r->dbp, NULL, 0);

				if (ret) {
					logmsg(LOGMSG_ERROR, "__db_close %s rc %d\n",
						r->fname, ret);
				}
			}
			__os_free(dbenv, r->fname);
			next = listc_rtl(&ft->ranges[i]);
			__os_free(dbenv, r);
			r = next;
		}
	}
	__os_free(dbenv, ft->ranges);
	ft->ranges = NULL;

	ft->numids = 0;

	return 0;
}

void
__fileid_track_dump(dbenv)
	DB_ENV *dbenv;
{
	int i;
	struct fileid_track *ft;

	ft = &dbenv->fileid_track;

	for (i = 0; i < ft->numids; i++) {
		if (ft->ranges[i].top) {
			struct lsn_range *r;

			logmsg(LOGMSG_USER, "%d:\n", i);
			r = ft->ranges[i].top;
			while (r) {
				logmsg(LOGMSG_USER, "  %u:%u -> %u:%u %s\n", r->start.file,
					r->start.offset, r->end.file, r->end.offset,
					r->fname);
				r = r->lnk.next;
			}
		}
	}
}

/*
 * PUBLIC: int __db_find_recovery_start_if_enabled __P((DB_ENV*, DB_LSN*));
 *
 **/
int
__db_find_recovery_start_if_enabled(dbenv, outlsn)
	DB_ENV *dbenv;
	DB_LSN *outlsn;
{
	if (!dbenv->attr.start_recovery_at_dbregs) {
		ZERO_LSN((*outlsn));
		return 0;
	}
	return __db_find_recovery_start(dbenv, outlsn);
}

/* Walk forward in the log until the first debug record */
static int
__db_find_earliest_recover_point_after_file(dbenv, outlsn, file)
	DB_ENV *dbenv;
	DB_LSN *outlsn;
	int file;
{
	int ret = 0;
	int flags = DB_FIRST;
	DB_LOGC *logc = NULL;
	u_int32_t type;
	DB_LSN lsn, start_lsn = {0};
	__db_debug_args *debug_args = NULL;
	DBT rec = { 0 };

	rec.flags = DB_DBT_REALLOC;

	if ((ret = __log_cursor(dbenv, &logc)) != 0)
		goto err;

	if (file > 0) {
		lsn.file = file;
		lsn.offset = 28;
		if (0 == __log_c_get(logc, &lsn, &rec, DB_SET))
			flags = DB_SET;
	}

	for (ret = __log_c_get(logc, &lsn, &rec, flags);
			ret == 0; ret = __log_c_get(logc, &lsn, &rec, DB_NEXT)) {
		LOGCOPY_32(&type, rec.data);
		normalize_rectype(&type);
		if (type == DB___db_debug && lsn.file >= file) {
			int optype = 0;
			if((ret = __db_debug_read(dbenv, rec.data, &debug_args)) != 0)
				goto err;
			LOGCOPY_32(&optype, debug_args->op.data);
			__os_free(dbenv, debug_args);
			if (optype == 2) {
				start_lsn = lsn;
				logmsg(LOGMSG_INFO, "%s: fullrecovery starting at lsn %u:%u\n",
						__func__, lsn.file, lsn.offset);
				break;
			}
		}
	}

err:
	if (rec.data) {
		__os_free(dbenv, rec.data);
		rec.data = NULL;
	}

	if (logc)
		__log_c_close(logc);

	(*outlsn) = start_lsn;
	return ret;
}

/*
 * PUBLIC: int __db_find_recovery_start __P((DB_ENV*, DB_LSN*));
 *
 **/
static int
__db_find_recovery_start_int(dbenv, outlsn, max_lsn)
	DB_ENV *dbenv;
	DB_LSN *outlsn;
	DB_LSN *max_lsn;
{
	int ret = 0;
#if defined FIND_RECOVERY_START_TRACE
	DB_LSN prev_lsn = { 0 };
#endif
	DB_LSN lsn, checkpoint_lsn, ckp_lsn;
	DB_LOGC *logc = NULL;
	DBT rec = { 0 };
	__txn_ckp_args *ckp_args = NULL;
	u_int32_t rectype;
	__db_debug_args *debug_args = NULL;
	int optype = 0;

	ZERO_LSN(*outlsn);

	rec.flags = DB_DBT_REALLOC;

	/* Find a checkpoint - from the checkpoint file, if we can, or from
	 * the log if we can't (eg: after a copy) */
	if ((ret = __checkpoint_get(dbenv, &lsn)) != 0 &&
		(ret = __txn_getckp(dbenv, &lsn)) != 0) {
		ret = DB_NOTFOUND;
		goto err;
	}
	if ((ret = __log_cursor(dbenv, &logc)) != 0)
		goto err;

#if defined FIND_RECOVERY_START_TRACE
	prev_lsn = lsn;
#endif
	do {
		if (ckp_args) {
			free(ckp_args);
			ckp_args = NULL;
		}
		if ((ret = __log_c_get(logc, &lsn, &rec, DB_SET)) != 0 ||
			(ret = __txn_ckp_read(dbenv, rec.data, &ckp_args)) != 0) {
			goto err;
		}
#if defined FIND_RECOVERY_START_TRACE
		if (max_lsn) {
			fprintf(stderr,
				"%s: max_lsn search: found [%d][%d] searching for the first "
				"checkpoint below [%d][%d]\n", __func__,
				ckp_args->last_ckp.file, ckp_args->last_ckp.offset,
				max_lsn->file, max_lsn->offset);
		}
		prev_lsn = lsn;
#endif
		lsn = ckp_args->last_ckp;
	}
	while (max_lsn != NULL &&
		log_compare(&ckp_args->ckp_lsn, max_lsn) >= 0);

	ckp_lsn = ckp_args->ckp_lsn;
	do {
		if (ckp_args) {
			free(ckp_args);
			ckp_args = NULL;
		}
		if ((ret = __log_c_get(logc, &lsn, &rec, DB_SET)) != 0 ||
			(ret = __txn_ckp_read(dbenv, rec.data, &ckp_args)) != 0)
			goto err;
#if defined FIND_RECOVERY_START_TRACE
		fprintf(stderr,
			"%s: ckp_lsn search: found [%d][%d] searching for the first "
			"checkpoint below [%d][%d]\n", __func__,
			ckp_args->last_ckp.file, ckp_args->last_ckp.offset,
			ckp_lsn.file, ckp_lsn.offset);
		prev_lsn = lsn;
#endif
		lsn = ckp_args->last_ckp;
	}
	while (log_compare(&ckp_args->last_ckp, &ckp_lsn) >= 0);

	checkpoint_lsn = ckp_args->ckp_lsn;
#if defined FIND_RECOVERY_START_TRACE
	fprintf(stderr, "%s: using lsn [%d][%d]\n", __func__,
		checkpoint_lsn.file, checkpoint_lsn.offset);
#endif

	ret = __log_c_get(logc, &lsn, &rec, DB_SET);
	if (ret)
		goto err;

	LOGCOPY_32(&rectype, rec.data);
	normalize_rectype(&rectype);
	if (rectype == DB___db_debug) {
		ret = __db_debug_read(dbenv, rec.data, &debug_args);
		if (ret)
			goto err;
		LOGCOPY_32(&optype, debug_args->op.data);
	}
	while (rectype != DB___db_debug || optype != 2) {
		if (log_compare(&lsn, &checkpoint_lsn) &&
			rectype != DB___dbreg_register &&
			dbenv->attr.warn_nondbreg_records) {
			__db_err(dbenv, "non-register record type %d at %u:%u "
				"before checkpoint at %u:%u",
				rectype, lsn.file, lsn.offset,
				checkpoint_lsn.file, checkpoint_lsn.offset);
		}
#if defined FIND_RECOVERY_START_TRACE
		prev_lsn = lsn;
#endif
		ret = __log_c_get(logc, &lsn, &rec, DB_PREV);
		if (ret)
			break;
		LOGCOPY_32(&rectype, rec.data);
		normalize_rectype(&rectype);
		optype = -1;
		if (rectype == DB___db_debug) {
			__os_free(dbenv, debug_args);
			debug_args = NULL;
			ret = __db_debug_read(dbenv, rec.data, &debug_args);
			LOGCOPY_32(&optype, debug_args->op.data);
		}
	}
	if (ret == DB_NOTFOUND) {
		/* This may be ok, if it's a new database, and hasn't
		 * yet written a checkpoint, or during rollout, where
		 * old log-deletion code is still running on some
		 * nodes. Just print a warning we can look for to see
		 * if this ever happens. It shouldn't - if it does we
		 * have problems. */
		__db_err(dbenv,
			"no recovery start record found prior to checkpoint");
		if (!dbenv->attr.dbreg_errors_fatal) {
			/* go old way for now - use the LSN at the
			 * last trusted checkpoint */
			ret = __checkpoint_get_recovery_lsn(dbenv, &lsn);
			if (ret)
				goto err;
		}
	}

	if (ret == 0)
		*outlsn = lsn;
err:
#if defined FIND_RECOVERY_START_TRACE
	if (ret != 0) {
		fprintf(stderr,
			"%s: ret=%d for lsn [%d][%d] prev-lsn [%d][%d]\n",
			__func__, ret, lsn.file, lsn.offset, prev_lsn.file,
			prev_lsn.offset);
		if (max_lsn)
			fprintf(stderr, "%s: max_lsn is [%d][%d]\n", __func__,
				max_lsn->file, max_lsn->offset);
	}
#endif
   if (rec.data) {
	  __os_free(dbenv, rec.data);
	  rec.data = NULL;
   }
	if (logc)
		__log_c_close(logc);
	if (ckp_args)
		__os_free(dbenv, ckp_args);
	if (debug_args)
		__os_free(dbenv, debug_args);
	return ret;
}

int
__db_find_recovery_start(dbenv, outlsn)
	DB_ENV *dbenv;
	DB_LSN *outlsn;
{
	return __db_find_recovery_start_int(dbenv, outlsn, NULL);
}

static char *mt_string(int mt)
{
	switch(mt) {
		case MINTRUNCATE_START:
			return "START";
		case MINTRUNCATE_SCAN:
			return "SCAN";
		case MINTRUNCATE_READY:
			return "READY";
		default:
			return "??INVALID??";
	}
}

int
__dbenv_clear_mintruncate_list(dbenv)
	DB_ENV *dbenv;
{
	struct mintruncate_entry *mt;
	Pthread_mutex_lock(&dbenv->mintruncate_lk);
	while ((mt = listc_rtl(&dbenv->mintruncate)) != NULL)
		free(mt);
	dbenv->mintruncate_state = MINTRUNCATE_START;
	ZERO_LSN(dbenv->last_mintruncate_dbreg_start);
	ZERO_LSN(dbenv->last_mintruncate_ckplsn);
	Pthread_mutex_unlock(&dbenv->mintruncate_lk);
	return 0;
}

int
__dbenv_dump_mintruncate_list(dbenv)
	DB_ENV *dbenv;
{
	struct mintruncate_entry *mt;
	Pthread_mutex_lock(&dbenv->mintruncate_lk);
	/* Find first suitable dbreg-start */
	logmsg(LOGMSG_USER, "Mintruncate-state is %s\n", 
			mt_string(dbenv->mintruncate_state));
	for (mt = LISTC_BOT(&dbenv->mintruncate); mt != NULL ;
				mt = mt->lnk.prev) {
		logmsg(LOGMSG_USER, "%s @ [%d:%d] timestamp %d ckplsn [%d:%d] "
#ifdef MINTRUNCATE_DEBUG
				"added by %s"
#endif
				"\n",
				mt->type == MINTRUNCATE_DBREG_START ? "dbreg" : "chkpt",
				mt->lsn.file, mt->lsn.offset, mt->timestamp, mt->ckplsn.file,
				mt->ckplsn.offset
#ifdef MINTRUNCATE_DEBUG
				,mt->func
#endif
				);
	}
	Pthread_mutex_unlock(&dbenv->mintruncate_lk);
	return 0;
}

int
__dbenv_mintruncate_lsn_timestamp(dbenv, lowfile, outlsn, outtime)
	DB_ENV *dbenv;
	int lowfile;
	DB_LSN *outlsn;
	int32_t *outtime;
{
	struct mintruncate_entry *mt;
	__txn_ckp_args *ckp_args = NULL;
	DBT rec = { 0 };
	DB_LOGC *logc = NULL;
	DB_LSN lowlsn = {0};
	int ret;

	if ((ret = __txn_getckp(dbenv, outlsn)) != 0 ||
			(ret = __log_cursor(dbenv, &logc)) != 0)
		goto err;

	rec.flags = DB_DBT_REALLOC;
	if ((ret = __log_c_get(logc, outlsn, &rec, DB_SET)) != 0 ||
			(ret = __txn_ckp_read(dbenv, rec.data, &ckp_args)) != 0) {
		goto err;
	}

	logc->close(logc, 0);

	*outtime = ckp_args->timestamp;
	free(rec.data);
	free(ckp_args);

	Pthread_mutex_lock(&dbenv->mintruncate_lk);

	if (!IS_ZERO_LSN(dbenv->mintruncate_first) && lowfile >
			dbenv->mintruncate_first.file)
		dbenv->mintruncate_state = MINTRUNCATE_READY;

	/* Delete old entries */
	while ((mt = LISTC_BOT(&dbenv->mintruncate)) != NULL &&
			mt->lsn.file <= lowfile) {
		mt = listc_rbl(&dbenv->mintruncate);
		free(mt);
	}

	/* Find first DBREG (we've deleted all below lowfile) */
	for (mt = LISTC_BOT(&dbenv->mintruncate) ;
			mt && (mt->type != MINTRUNCATE_DBREG_START) ;
			mt = mt->lnk.prev)
		;

	if (mt)
		lowlsn = mt->lsn;

	/* Find first checkpoint with ckplsn > than that */
	while (mt && (mt->type != MINTRUNCATE_CHECKPOINT || 
			 log_compare(&mt->ckplsn, &lowlsn) < 0))
		mt = mt->lnk.prev;

	/* If we found it, that's our minimum truncate point */
	if (mt) {
		ret = 0;
		*outlsn = mt->lsn;
		*outtime = mt->timestamp;
	}

	Pthread_mutex_unlock(&dbenv->mintruncate_lk);
	
err:
	return ret;
}

/* Must be holding mintruncate_lk while calling this */
void __dbenv_reset_mintruncate_vars(dbenv)
	DB_ENV *dbenv;
{
	int found_dbreg=0, found_ckp=0;
	struct mintruncate_entry *mt;
	ZERO_LSN(dbenv->last_mintruncate_dbreg_start);
	ZERO_LSN(dbenv->last_mintruncate_ckplsn);
	for (mt = LISTC_TOP(&dbenv->mintruncate); mt; mt = mt->lnk.next) {
		if (!found_dbreg && mt->type == MINTRUNCATE_DBREG_START) {
			dbenv->last_mintruncate_dbreg_start = mt->lsn;
			found_dbreg = 1;
		}

		if (!found_ckp && mt->type == MINTRUNCATE_CHECKPOINT) {
			dbenv->last_mintruncate_ckplsn = mt->ckplsn;
			found_ckp = 1;
		}

		if (found_dbreg && found_ckp)
			break;
	}
}

#ifdef MINTRUNCATE_DEBUG
void verify_list(dbenv)
	DB_ENV *dbenv;
{
	struct mintruncate_entry *mt;
	DB_LSN last = {0};
	for (mt = LISTC_BOT(&dbenv->mintruncate) ; mt ; mt = mt->lnk.prev) {
		if (log_compare(&last, &mt->lsn) >= 0)
			abort();
		last = mt->lsn;
	}
}
#endif

int __dbenv_build_mintruncate_list(dbenv)
	DB_ENV *dbenv;
{
	struct mintruncate_entry *mt, *newmt, *prev_mt;

	u_int32_t type;
	int optype = 0;
	__txn_ckp_args *ckp_args;
	__db_debug_args *debug_args;
	DBT rec = {0};
	DB_LSN lsn, last_ckp_lsn = {0}, last_dbreg_start = {0}, last_add;
	DB_LOGC *logc = NULL;
	int ret = 0, caught_up = 0;

	if (dbenv->mintruncate_state == MINTRUNCATE_READY) {
		logmsg(LOGMSG_WARN, "%s: no need to build map\n", __func__);
		return 0;
	}

	dbenv->mintruncate_state = MINTRUNCATE_SCAN;

	if ((ret = __log_cursor(dbenv, &logc)) != 0)
		abort();

	rec.flags = DB_DBT_REALLOC;
	for (ret = __log_c_get(logc, &lsn, &rec, DB_FIRST);
			ret == 0 && caught_up == 0;
			ret = __log_c_get(logc, &lsn, &rec, DB_NEXT)) {

		LOGCOPY_32(&type, rec.data);
		normalize_rectype(&type);
		if (type == DB___db_debug) {
			if ((ret = __db_debug_read(dbenv, rec.data, &debug_args))!=0)
				abort();
			LOGCOPY_32(&optype, debug_args->op.data);
			__os_free(dbenv, debug_args);
			Pthread_mutex_lock(&dbenv->mintruncate_lk);

			/* Only add if we've switched files */
			if (optype == 2 && (last_dbreg_start.file < lsn.file)) {

				/* Normal log traffic can be adding to the other end: find
				 * correct place to insert */

				for (prev_mt = NULL, mt = LISTC_BOT(&dbenv->mintruncate) ;
						mt && log_compare(&mt->lsn, &lsn) < 0;
						prev_mt = mt, mt = mt->lnk.prev)
					;
				if (!mt || log_compare(&mt->lsn, &lsn) > 0) {
					newmt = malloc(sizeof(*newmt));
#ifdef MINTRUNCATE_DEBUG
					newmt->func = __func__;
#endif
					newmt->type = MINTRUNCATE_DBREG_START;
					newmt->timestamp = 0;
					last_dbreg_start = newmt->lsn = lsn;
					ZERO_LSN(newmt->ckplsn);
					if (!mt) {
						listc_atl(&dbenv->mintruncate, newmt);
					} else {
						listc_add_after(&dbenv->mintruncate, newmt, mt);
					}
#ifdef MINTRUNCATE_DEBUG
					verify_list(dbenv);
#endif
				} else if (mt) {
					assert(log_compare(&mt->lsn, &lsn) == 0);
					assert(mt->type == MINTRUNCATE_DBREG_START);
					caught_up = 1;
				}
			}

			Pthread_mutex_unlock(&dbenv->mintruncate_lk);
		}

		if (type == DB___txn_ckp) {
			if ((ret = __txn_ckp_read(dbenv, rec.data, &ckp_args)) != 0)
				abort();

			if (ckp_args->ckp_lsn.file > last_ckp_lsn.file) {
				Pthread_mutex_lock(&dbenv->mintruncate_lk);
				for (prev_mt = NULL, mt = LISTC_BOT(&dbenv->mintruncate) ;
						mt && log_compare(&mt->lsn, &lsn) < 0;
						prev_mt = mt, mt = mt->lnk.prev)
					;

				if (!mt || log_compare(&mt->lsn, &lsn) > 0) {
					newmt = malloc(sizeof(*newmt));
#ifdef MINTRUNCATE_DEBUG
					newmt->func = __func__;
#endif
					newmt->type = MINTRUNCATE_CHECKPOINT;
					newmt->timestamp = ckp_args->timestamp;
					newmt->lsn = lsn;
					last_ckp_lsn = newmt->ckplsn = ckp_args->ckp_lsn;
					if (!mt) { 
						listc_atl(&dbenv->mintruncate, newmt);
					} else {
						listc_add_after(&dbenv->mintruncate, newmt, mt);
					}
#ifdef MINTRUNCATE_DEBUG
					verify_list(dbenv);
#endif
				} else if (mt) {
					assert(log_compare(&mt->lsn, &lsn) == 0);
					assert(mt->type == MINTRUNCATE_CHECKPOINT);
					caught_up = 1;
				}
				Pthread_mutex_unlock(&dbenv->mintruncate_lk);
			}
			__os_free(dbenv, ckp_args);
		}
	}

	Pthread_mutex_lock(&dbenv->mintruncate_lk);
	__dbenv_reset_mintruncate_vars(dbenv);
	dbenv->mintruncate_state = MINTRUNCATE_READY;
	Pthread_mutex_unlock(&dbenv->mintruncate_lk);

err:
	if (logc)
		__log_c_close(logc);

	if (rec.data)
		free(rec.data);

	return ret;
}

int __rep_check_applied_lsns(DB_ENV * dbenv, LSN_COLLECTION * lc,
	int inrecovery);

static int
full_recovery_check(DB_ENV *dbenv, DB_LSN *max_lsn)
{
	DB_LOGC *logc = NULL;
	int ret;
	DBT logrec = { 0 };
	DB_LSN lsn, cpy;
	u_int32_t type;
	LSN_COLLECTION lc = { 0 };
	int ignore;
	int maxnlsns = 0;
	DB_LSN first, last;

	ZERO_LSN(first);
	ZERO_LSN(last);

	logrec.flags = DB_DBT_REALLOC;

	ret = __log_cursor(dbenv, &logc);
	for (ret = __log_c_get(logc, &lsn, &logrec, DB_FIRST);
		ret == 0 && (max_lsn == NULL || log_compare(&lsn, max_lsn) <= 0);
		ret = __log_c_get(logc, &lsn, &logrec, DB_NEXT)) {
		last = lsn;
		LOGCOPY_32(&type, logrec.data);
		normalize_rectype(&type);
		if (IS_ZERO_LSN(first))
			first = lsn;
		if (type == DB___txn_regop || type == DB___txn_regop_gen ||
			type == DB___txn_dist_commit || type == DB___txn_regop_rowlocks) {
			cpy = lsn;
			ret =
				__rep_collect_txn(dbenv, &cpy, &lc, &ignore, NULL);
			if (lc.nlsns > maxnlsns)
				maxnlsns = lc.nlsns;
			if (ret) {
				/*
				 * It's entirely possible that we
				 * can't collect records - transaction
				 * could be earlier than a checkpoint
				 * and its logs can be deleted - just
				 * warn
				 */
				logmsg(LOGMSG_ERROR, 
				"can't collect records for transaction at "
					PR_LSN "\n", PARM_LSN(lsn));
				ret = 0;
				continue;
			}
			/* If we managed to collect the logs we better be able to verify the transaction. */
			ret = __rep_check_applied_lsns(dbenv, &lc, 1);
			if (ret) {
				logmsg(LOGMSG_ERROR, 
				"verification failed for txn at " PR_LSN
					"\n", PARM_LSN(last));
				if (ret) {
					ret =
						__rep_check_applied_lsns(dbenv, &lc,
						1);
					break;
				}
			}
		}
	}
	/* if we ran off the end of the log, we're done */
	if (ret == DB_NOTFOUND)
		ret = 0;
	for (int i = 0; i < maxnlsns; i++)
		free(lc.array[i].rec.data);
	free(lc.array);
	__log_c_close(logc);
	logmsg(LOGMSG_INFO, "ran full_recovery_check from " PR_LSN " to " PR_LSN
		", ret %d\n", PARM_LSN(first), PARM_LSN(last), ret);
	return ret;
}

/*
  Log the progress of database recovery process.
*/
void log_recovery_progress(int stage, int progress)
{
	static int last_stage = -1;
	static int last_reported = -1;
	const int step = 10;

	/* End-of-stage marker */
	if (last_stage == stage && progress == -1) {
		logmsg(LOGMSG_WARN, " .. done\n");
		return;
	}

	/* Begin-of-stage marker */
	if (stage > last_stage) {
		logmsg(LOGMSG_WARN, "Recovery pass #%d\n", stage);
		last_stage = stage;
		last_reported = -1;
		return;
	}

	if ((progress > last_reported) && ((progress % step) == 0)) {
		last_reported = progress;
		logmsg(LOGMSG_WARN, " %d%%", progress);
	}
}




/*
 * __db_apprec --
 *	Perform recovery.  If max_lsn is non-NULL, then we are trying
 * to synchronize this system up with another system that has a max
 * LSN of max_lsn, so we need to roll back sufficiently far for that
 * to work.  See __log_backup for details.
 *
 * PUBLIC: int __db_apprec __P((DB_ENV *, DB_LSN *, DB_LSN *, u_int32_t,
 * PUBLIC:	u_int32_t));
 */
int
__db_apprec(dbenv, max_lsn, trunclsn, update, flags)
	DB_ENV *dbenv;
	DB_LSN *max_lsn, *trunclsn;
	u_int32_t update, flags;
{
	DBT data;
	DB_LOGC *logc;
	DB_LSN ckp_lsn, first_lsn, last_lsn, lowlsn, lsn, stop_lsn;
	DB_REP *db_rep;
	DB_TXNREGION *region;
	REP *rep;
	__txn_ckp_args *ckp_args;
	time_t now, tlow;
	int32_t log_size, low;
	double nfiles;
	int have_rec, progress, ret, t_ret;
	int (**dtab) __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
	u_int32_t hi_txn, txnid;
	char *p, *pass, t1[60], t2[60];
	void *txninfo;
	DB_LSN logged_checkpoint_lsn;
	int start_recovery_at_dbregs;

	COMPQUIET(nfiles, (double)0);

	logc = NULL;
	ckp_args = NULL;
	dtab = NULL;

	hi_txn = TXN_MAXIMUM;
	txninfo = NULL;

	if (trunclsn)
		(*trunclsn) = (*max_lsn);

	pass = "initial";

	/*
	 * XXX
	 * Get the log size.  No locking required because we're single-threaded
	 * during recovery.
	 */
	log_size =
		((LOG *)(((DB_LOG *)dbenv->lg_handle)->reginfo.primary))->log_size;

	/*
	 * If we need to, update the env handle timestamp.  The timestamp
	 * field can be updated here without acquiring the rep mutex
	 * because recovery is single-threaded, even in the case of
	 * replication.
	 */
	if (update && (db_rep = dbenv->rep_handle) != NULL &&
		(rep = db_rep->region) != NULL)
		(void)time(&rep->timestamp);

	/* Set in-recovery flags. */
	F_SET((DB_LOG *)dbenv->lg_handle, DBLOG_RECOVER);
	region = ((DB_TXNMGR *)dbenv->tx_handle)->reginfo.primary;
	F_SET(region, TXN_IN_RECOVERY);

	/* Allocate a cursor for the log. */
	if ((ret = __log_cursor(dbenv, &logc)) != 0)
		goto err;

	/*
	 * If the user is specifying recovery to a particular point in time
	 * or to a particular LSN, find the point to start recovery from.
	 */
	ZERO_LSN(lowlsn);
	if (max_lsn != NULL) {
		if ((ret = __log_backup(dbenv, logc, max_lsn, &lowlsn)) != 0)
			goto err;
	} else if (dbenv->tx_timestamp != 0) {
		if ((ret = __log_earliest(dbenv, logc, &low, &lowlsn)) != 0)
			goto err;
		if ((int32_t)dbenv->tx_timestamp < low) {
			char my_buf[30];
			(void)snprintf(t1, sizeof(t1),
				"%s", ctime_r(&dbenv->tx_timestamp, my_buf));
			if ((p = strchr(t1, '\n')) != NULL)
				*p = '\0';
			tlow = (time_t)low;
			(void)snprintf(t2, sizeof(t2), "%s", ctime_r(&tlow, my_buf));
			if ((p = strchr(t2, '\n')) != NULL)
				*p = '\0';
			__db_err(dbenv,
			"Invalid recovery timestamp %s; earliest time is %s",
				t1, t2);
			ret = EINVAL;
			goto err;
		}
	}

        ++gbl_apprec_gen;

	/*
	 * Recovery is done in three passes:
	 * Pass #0:
	 *	We need to find the position from which we will open files.
	 *	We need to open files beginning with the earlier of the
	 *	most recent checkpoint LSN and a checkpoint LSN before the
	 *	recovery timestamp, if specified.  We need to be before the
	 *	most recent checkpoint LSN because we are going to collect
	 *	information about which transactions were begun before we
	 *	start rolling forward.  Those that were should never be undone
	 *	because queue cannot use LSNs to determine what operations can
	 *	safely be aborted and it cannot rollback operations in
	 *	transactions for which there may be records not processed
	 *	during recovery.  We need to consider earlier points in time
	 *	in case we are recovering to a particular timestamp.
	 *
	 * Pass #1:
	 *	Read forward through the log from the position found in pass 0
	 *	opening and closing files, and recording transactions for which
	 *	we've seen their first record (the transaction's prev_lsn is
	 *	0,0).  At the end of this pass, we know all transactions for
	 *	which we've seen begins and we have the "current" set of files
	 *	open.
	 *
	 * Pass #1.5:
	 *  If recovery page logging is enabled, bring each recovery
	 *  page into the bufferpool.  If there are any errors or cksum
	 *  mismatches, the pages will be fixed and written back to the
	 *  data files.
	 *
	 * Pass #2:
	 *	Read backward through the log undoing any uncompleted TXNs.
	 *	There are four cases:
	 *		1.  If doing catastrophic recovery, we read to the
	 *		beginning of the log
	 *		2.  If we are doing normal reovery, then we have to roll
	 *		back to the most recent checkpoint LSN.
	 *		3.  If we are recovering to a point in time, then we have
	 *		to roll back to the checkpoint whose ckp_lsn is earlier
	 *		than the specified time.  __log_earliest will figure
	 *		this out for us.
	 *		4.	If we are recovering back to a particular LSN, then
	 *		we have to roll back to the checkpoint whose ckp_lsn
	 *		is earlier than the max_lsn.  __log_backup will figure
	 *		that out for us.
	 *	In case 2, "uncompleted TXNs" include all those who commited
	 *	after the user's specified timestamp.
	 *
	 * Pass #3:
	 *	Read forward through the log from the LSN found in pass #2,
	 *	redoing any committed TXNs (which commited after any user-
	 *	specified rollback point).  During this pass, checkpoint
	 *	file information is ignored, and file openings and closings
	 *	are redone.
	 *
	 * ckp_lsn   -- lsn of the last checkpoint or the first in the log.
	 * first_lsn -- the lsn where the forward passes begin.
	 * last_lsn  -- the last lsn in the log, used for feedback
	 * lowlsn	-- the lsn we are rolling back to, if we are recovering
	 *		to a point in time.
	 * lsn	   -- temporary use lsn.
	 * stop_lsn  -- the point at which forward roll should stop
	 */

	/*
	 * Find out the last lsn, so that we can estimate how far along we
	 * are in recovery.  This will help us determine how much log there
	 * is between the first LSN that we're going to be working with and
	 * the last one.  We assume that each of the three phases takes the
	 * same amount of time (a false assumption) and then use the %-age
	 * of the amount of log traversed to figure out how much of the
	 * pass we've accomplished.
	 *
	 * If we can't find any log records, we're kind of done.
	 */
#ifdef UMRW
	ZERO_LSN(last_lsn);
#endif
	memset(&data, 0, sizeof(data));
	if ((ret = __log_c_get(logc, &last_lsn, &data, DB_LAST)) != 0) {
		if (ret == DB_NOTFOUND)
			ret = 0;
		else
			__db_err(dbenv, "Last log record not found");
		goto err;
	}

	do {
		/* txnid is after rectype, which is a u_int32. */
		LOGCOPY_32(&txnid, (u_int8_t *)data.data + sizeof(u_int32_t));
		if (txnid != 0)
			break;
	} while ((ret = __log_c_get(logc, &lsn, &data, DB_PREV)) == 0);

	/*
	 * There are no transactions, so there is nothing to do unless
	 * we're recovering to an LSN.  If we are, we need to proceed since
	 * we'll still need to do a vtruncate based on information we haven't
	 * yet collected.
	 */
	if (ret == DB_NOTFOUND)
		ret = 0;
	else if (ret != 0)
		goto err;

	hi_txn = txnid;

	/*
	 * Pass #0
	 * Find the LSN from which we begin OPENFILES.
	 *
	 * If this is a catastrophic recovery, or if no checkpoint exists
	 * in the log, the LSN is the first LSN in the log.
	 *
	 * Otherwise, it is the minimum of (1) the LSN in the last checkpoint
	 * and (2) the LSN in the checkpoint before any specified recovery
	 * timestamp or max_lsn.
	 */
	/*
	 * Get the first LSN in the log; it's an initial default
	 * even if this is not a catastrophic recovery.
	 */
	log_recovery_progress(0, -1);

	if ((ret = __log_c_get(logc, &ckp_lsn, &data, DB_FIRST)) != 0) {
		if (ret == DB_NOTFOUND)
			ret = 0;
		else
			__db_err(dbenv, "First log record not found");
		goto err;
	}
	first_lsn = ckp_lsn;
	have_rec = 1;

	start_recovery_at_dbregs = dbenv->attr.start_recovery_at_dbregs;
	if (!LF_ISSET(DB_RECOVER_FATAL)) {
		ret = 0;
		/* if we saved a checkpoint in the checkpoint file,
		 * use that instead */
		if (start_recovery_at_dbregs) {
			ret = __db_find_recovery_start_int(dbenv, &first_lsn, max_lsn);
			if (ret) {
				logmsg(LOGMSG_INFO, "__db_find_recovery_start rc %d\n", ret);
				/* We read this above as the first LSN */
				first_lsn = ckp_lsn;
				if (dbenv->attr.dbreg_errors_fatal)
					goto err;
			} else {
				have_rec = 0;
			}

			if (ret && dbenv->attr.dbreg_errors_fatal)
				goto err;
		}

		if (!start_recovery_at_dbregs || ret != 0) {
			if ((ret = __checkpoint_get(dbenv, &logged_checkpoint_lsn) == 0)) {
				logmsg(LOGMSG_DEBUG, "loaded %u:%u\n",
					logged_checkpoint_lsn.file,
					logged_checkpoint_lsn.offset);
				have_rec = 0;
				first_lsn = logged_checkpoint_lsn;
			} else {
				__db_err(dbenv,
					"can't get recovery lsn rc %d", ret);
				goto err;
			}

			if ((ret = __log_c_get(logc, &logged_checkpoint_lsn,
				&data, DB_SET)) != 0) {
				__db_err(dbenv,
	"can't read checkpoint lsn %u:%u, falling back to full recovery.",
					logged_checkpoint_lsn.file,
					logged_checkpoint_lsn.offset);
				/* We read this above as the first LSN */
				first_lsn = ckp_lsn;
			} else {
				if ((ret = __txn_ckp_read(dbenv, data.data,
					&ckp_args)) != 0) {
					__db_err(dbenv,
					  "invalid checkpoint record at %u:%u",
					  (unsigned)logged_checkpoint_lsn.file,
					  (unsigned)logged_checkpoint_lsn.offset);
					goto err;
				}
				first_lsn = ckp_args->ckp_lsn;
				// Get a starting next_utxnid that's higher than the checkpoint.
				// This will be updated as transactions are processed during 
				// forward roll.
				u_int32_t rectype;
				LOGCOPY_32(&rectype, data.data);
				if (normalize_rectype(&rectype)) {
					Pthread_mutex_lock(&dbenv->utxnid_lock);
					dbenv->next_utxnid = ckp_args->max_utxnid+1;
					Pthread_mutex_unlock(&dbenv->utxnid_lock);
				}
				have_rec = 0;
				logmsg(LOGMSG_DEBUG, "checkpoint %u:%u points to last lsn %u:%u\n",
					logged_checkpoint_lsn.file,
					logged_checkpoint_lsn.offset,
					first_lsn.file, first_lsn.offset);
			}
		}

		/*
		 * If LSN (2) exists, use it if it's before LSN (1).
		 * (If LSN (1) doesn't exist, first_lsn is the
		 * beginning of the log, so will "win" this check.)
		 *
		 * XXX
		 * In the recovery-to-a-timestamp case, lowlsn is chosen by
		 * __log_earliest, and is the checkpoint LSN of the
		 * *earliest* checkpoint in the unreclaimed log.  I
		 * (krinsky) believe that we could optimize this by looking
		 * instead for the LSN of the *latest* checkpoint before
		 * the timestamp of interest, but I'm not sure that this
		 * is worth doing right now.  (We have to look for lowlsn
		 * and low anyway, to make sure the requested timestamp is
		 * somewhere in the logs we have, and all that's required
		 * is that we pick *some* checkpoint after the beginning of
		 * the logs and before the timestamp.
		 */
		if ((dbenv->tx_timestamp != 0 || max_lsn != NULL) &&
			log_compare(&lowlsn, &first_lsn) < 0) {
			DB_ASSERT(have_rec == 0);
			first_lsn = lowlsn;
		}
	} else {
		/* If fatal recovery, and we were told where to start,
		 * start there. */
		if (!IS_ZERO_LSN(dbenv->recovery_start_lsn)) {
			first_lsn = dbenv->recovery_start_lsn;
		} else if (start_recovery_at_dbregs) {
			ret = __db_find_earliest_recover_point_after_file(dbenv,
					&first_lsn, 0);
			if (ret) {
				__db_err(dbenv,
						"__db_find_recovery_start_int rc %d\n", ret);
				goto err;
			}
		} 
	}
	/* Reset the start LSN so subsequent recoveries don't use it. */
	ZERO_LSN(dbenv->recovery_start_lsn);

	/* Get the record at first_lsn if we don't have it already. */
	if (!have_rec &&
		(ret = __log_c_get(logc, &first_lsn, &data, DB_SET)) != 0) {
		__db_err(dbenv, "Checkpoint LSN record [%ld][%ld] not found",
			(u_long)first_lsn.file, (u_long)first_lsn.offset);
		goto err;
	}

	if (last_lsn.file == first_lsn.file)
		nfiles = (double)
			(last_lsn.offset - first_lsn.offset) / log_size;
	else
		nfiles = (double)(last_lsn.file - first_lsn.file) +
			(double)(log_size - first_lsn.offset +
			last_lsn.offset) / log_size;
	/* We are going to divide by nfiles; make sure it isn't 0. */
	if (nfiles == 0)
		nfiles = (double)0.001;

	/* Find a low txnid. */
	ret = 0;
	if (hi_txn != 0)
		do {
			/* txnid is after rectype, which is a u_int32. */
			LOGCOPY_32(&txnid,
				(u_int8_t *)data.data + sizeof(u_int32_t));

			if (txnid != 0)
				break;
		} while ((ret = __log_c_get(logc, &lsn, &data, DB_NEXT)) == 0);

	/*
	 * There are no transactions and we're not recovering to an LSN (see
	 * above), so there is nothing to do.
	 */
	if (ret == DB_NOTFOUND)
		ret = 0;

	/* Reset to the first lsn. */
	if (ret != 0 ||
		(ret = __log_c_get(logc, &first_lsn, &data, DB_SET)) != 0)
		goto err;

	/* Initialize the transaction list. */
	if ((ret =
		__db_txnlist_init(dbenv, txnid, hi_txn, max_lsn,
			&txninfo)) != 0)
		goto err;

	__fileid_track_free(dbenv);

	log_recovery_progress(0, -1);
	dbenv->recovery_pass = DB_TXN_OPENFILES;

	/*
	 * Pass #1
	 * Run forward through the log starting at the first relevant lsn.
	 */
	log_recovery_progress(1, -1);
	logmsg(LOGMSG_WARN, "running forward pass from %u:%u -> %u:%u\n",
		first_lsn.file, first_lsn.offset, last_lsn.file, last_lsn.offset);
	if ((ret = __env_openfiles(dbenv, logc,
			txninfo, &data, &first_lsn, &last_lsn, nfiles, 1)) != 0)
		goto err;

	/* If there were no transactions, then we can bail out early. */
	if (hi_txn == 0 && max_lsn == NULL)
		goto done;

	/*
	 * Pass #1.5
	 * If recovery-page logging is enabled, bring each of the recovery-pages
	 * into the bufferpool.
	 */
	if (dbenv->mp_recovery_pages > 0) {
		__dbreg_recovery_pages(dbenv);
	}

	log_recovery_progress(1, -1);
	dbenv->recovery_pass = DB_TXN_BACKWARD_ROLL;

	/*
	 * Pass #2.
	 *
	 * We used first_lsn to tell us how far back we need to recover,
	 * use it here.
	 */
	log_recovery_progress(2, -1);

#if 0
	printf("lsn ranges at end of pass #1\n");
	__fileid_track_dump(dbenv);
#endif

	if (FLD_ISSET(dbenv->verbose, DB_VERB_RECOVERY))
		__db_err(dbenv, "Recovery starting from [%lu][%lu]",
			(u_long)first_lsn.file, (u_long)first_lsn.offset);

	pass = "backward";
	ret = __log_c_get(logc, &lsn, &data, DB_LAST);
	if (ret)
		goto err;
	logmsg(LOGMSG_WARN, "running backward pass from %u:%u <- %u:%u\n",
		first_lsn.file, first_lsn.offset, lsn.file, lsn.offset);
	for (; ret == 0 && log_compare(&lsn, &first_lsn) >= 0;
		ret = __log_c_get(logc, &lsn, &data, DB_PREV)) {
#if 0
		progress = 34 + (int)(33 * (__lsn_diff(&first_lsn,
#else
		progress = (int)(100 * (__lsn_diff(&first_lsn,
#endif
			&last_lsn, &lsn, log_size, 0) / nfiles));
		log_recovery_progress(2, progress);

		if (dbenv->db_feedback != NULL) {
			dbenv->db_feedback(dbenv, DB_RECOVER, progress);
		}

		ret = __db_dispatch(dbenv, dbenv->recover_dtab,
			dbenv->recover_dtab_size, &data, &lsn,
			DB_TXN_BACKWARD_ROLL, txninfo);
		if (ret != 0) {
			if (ret != DB_TXN_CKP)
				goto msgerr;
			else
				ret = 0;
		}
		if (dbenv->lsn_undone_callback)
			dbenv->lsn_undone_callback(dbenv, &lsn);
	}

	if (ret != 0 && ret != DB_NOTFOUND)
		goto err;

	log_recovery_progress(2, -1);

	/*
	 * Pass #3.  If we are recovering to a timestamp or to an LSN,
	 * we need to make sure that we don't roll-forward beyond that
	 * point because there may be non-transactional operations (e.g.,
	 * closes that would fail).  The last_lsn variable is used for
	 * feedback calculations, but use it to set an initial stopping
	 * point for the forward pass, and then reset appropriately to
	 * derive a real stop_lsn that tells how far the forward pass
	 * should go.
	 */
	log_recovery_progress(3, -1);
	pass = "forward";
	dbenv->recovery_pass = DB_TXN_FORWARD_ROLL;
	stop_lsn = last_lsn;
	if (max_lsn != NULL ||dbenv->tx_timestamp != 0)
		stop_lsn = ((DB_TXNHEAD *)txninfo)->maxlsn;

	logmsg(LOGMSG_WARN, "running forward pass from %u:%u -> %u:%u\n",
		lsn.file, lsn.offset, stop_lsn.file, stop_lsn.offset);
	for (ret = __log_c_get(logc, &lsn, &data, DB_NEXT);
		ret == 0; ret = __log_c_get(logc, &lsn, &data, DB_NEXT)) {
		/*
		 * If we are recovering to a timestamp or an LSN,
		 * we need to make sure that we don't try to roll
		 * forward beyond the soon-to-be end of log.
		 */


		if (log_compare(&lsn, &stop_lsn) > 0)
			break;
#if 0
		progress = 67 + (int)(33 * (__lsn_diff(&first_lsn,
#else
		progress = (int)(100 * (__lsn_diff(&first_lsn,
#endif
			&last_lsn, &lsn, log_size, 1) / nfiles));
		log_recovery_progress(3, progress);

		if (dbenv->db_feedback != NULL) {
			dbenv->db_feedback(dbenv, DB_RECOVER, progress);
		}

		ret = __db_dispatch(dbenv, dbenv->recover_dtab,
			dbenv->recover_dtab_size, &data, &lsn,
			DB_TXN_FORWARD_ROLL, txninfo);
		if (ret != 0) {
			if (ret != DB_TXN_CKP)
				goto msgerr;
			else
				ret = 0;
		}

	}

	if (ret != 0 && ret != DB_NOTFOUND)
		goto err;
	dbenv->recovery_pass = DB_TXN_NOT_IN_RECOVERY;

	/*
	 * Process any pages that were on the limbo list and move them to
	 * the free list.  Do this before checkpointing the database.
	 */
	if ((ret = __db_do_the_limbo(dbenv, NULL, NULL, txninfo,
			dbenv->tx_timestamp !=
			0 ? LIMBO_TIMESTAMP : LIMBO_RECOVER)) != 0)
		 goto err;

	if (max_lsn == NULL)
		region->last_txnid = ((DB_TXNHEAD *)txninfo)->maxid;

	if (dbenv->tx_timestamp != 0) {
		/* We are going to truncate, so we'd best close the cursor. */
		if (logc != NULL &&(ret = __log_c_close(logc)) != 0)
			goto err;
		logc = NULL;

		/* Flush everything to disk, we are losing the log. */
		if ((ret = __memp_sync(dbenv, NULL)) != 0)
			 goto err;

		region->last_ckp = ((DB_TXNHEAD *)txninfo)->ckplsn;
		__log_vtruncate(dbenv, &((DB_TXNHEAD *)txninfo)->maxlsn,
			&((DB_TXNHEAD *)txninfo)->ckplsn, trunclsn);
		/*
		 * Generate logging compensation records.
		 * If we crash during/after vtruncate we may have
		 * pages missing from the free list since they
		 * if we roll things further back from here.
		 * These pages are only known in memory at this pont.
		 */
		 if ((ret = __db_do_the_limbo(dbenv,
			 NULL, NULL, txninfo, LIMBO_COMPENSATE)) != 0)
			goto err;
	}

	/* Take a checkpoint here to force any dirty data pages to disk. */
	if (gbl_is_physical_replicant || LF_ISSET(DB_RECOVER_NOCKP)) {
		if (MPOOL_ON(dbenv) && (ret = __memp_sync_restartable(dbenv,
						NULL, 0, 0)) != 0) {
			logmsg(LOGMSG_ERROR, "memp_sync returned %d\n", ret);
			goto err;
		}
	} else if ((ret = __txn_checkpoint(dbenv, 0, 0, DB_FORCE)) != 0)
		goto err;


	/* Close all the db files that are open. */
	if ((ret = __dbreg_close_files(dbenv)) != 0)
		goto err;

done:
	if (max_lsn != NULL || gbl_is_physical_replicant) {

			/*
			 * When I truncate, I am running replicated recovery.
			 * I am synced all the way to the last checkpoint, so
			 * dropping the all the checkpoints in the truncation
			 * log phase does not make me bad
			 */
		if (max_lsn != NULL) {

			DB_LSN last_valid_checkpoint = { 0 };

			/*
			 * We can't truncate before we write a valid
			 * checkpoint into the checkpoint file.  We don't know
			 * what that is.  Find it.  By this point we already
			 * did recovery, so any checkpoint LSN < max_lsn is
			 * valid.
			 */
			if ((ret =
				__log_find_latest_checkpoint_before_lsn(dbenv, logc,
					max_lsn, &last_valid_checkpoint)) != 0) {
				__db_err(dbenv,
					"can't find last logged checkpoint max_lsn %u:%u",
					max_lsn->file, max_lsn->offset);
				ret =
					__log_find_latest_checkpoint_before_lsn_try_harder
					(dbenv, logc, max_lsn, &last_valid_checkpoint);
				if (ret == 0)
					logmsg(LOGMSG_DEBUG, "tried harder and found %u:%u\n",
						last_valid_checkpoint.file,
						last_valid_checkpoint.offset);
				else {
					logmsg(LOGMSG_DEBUG, "tried harder and still failed rc; I'll be good unless I crash %d\n",
						ret);
					/* Here we are gonna write a checkpoint FILE containing 0:0 as recovery lsn, claiming all it 
					 * is good at this point */
					/*goto err; */
				}
			}

			/* Save the checkpoint. */
			if ((ret =
				__checkpoint_save(dbenv, &last_valid_checkpoint,
					1)) != 0) {
				__db_err(dbenv, "can't save checkpoint %u:%u",
					last_valid_checkpoint.file,
					last_valid_checkpoint.offset);
				goto err;
			}

			logmsg(LOGMSG_WARN, "TRUNCATING to %u:%u checkpoint lsn is %u:%u\n",
				max_lsn->file, max_lsn->offset,
				last_valid_checkpoint.file, last_valid_checkpoint.offset);

			region->last_ckp = ((DB_TXNHEAD *) txninfo)->ckplsn;
			logmsg(LOGMSG_DEBUG, "%s:%d last_ckp is %u:%u\n", __FILE__, __LINE__,
				region->last_ckp.file, region->last_ckp.offset);

			if (IS_ZERO_LSN(region->last_ckp)) {
				/* I still don't understand how this ends up as 0:0 */
				logmsg(LOGMSG_DEBUG, "last_ckp zero lsn? Let's try %u:%u\n",
					last_valid_checkpoint.file,
					last_valid_checkpoint.offset);
				region->last_ckp = last_valid_checkpoint;

			}

			/* We are going to truncate, so we'd best close the cursor. */
			if (logc != NULL && (ret = __log_c_close(logc)) != 0)
				goto err;

			__log_vtruncate(dbenv, max_lsn, &region->last_ckp, trunclsn);

			logmsg(LOGMSG_WARN, "TRUNCATED TO is %u:%u \n", trunclsn->file,
				trunclsn->offset);
		}


		/*
		 * Now we need to open files that should be open in order for
		 * client processing to continue.  However, since we've
		 * truncated the log, we need to recompute from where the
		 * openfiles pass should begin.
		 */
		if ((ret = __log_cursor(dbenv, &logc)) != 0)
			goto err;
		if ((ret = __log_c_get(logc, &first_lsn, &data, DB_FIRST)) != 0) {
			if (ret == DB_NOTFOUND)
				ret = 0;
			else
				__db_err(dbenv, "First log record not found");
			goto err;
		}
		if ((ret = __txn_getckp(dbenv, &first_lsn)) == 0 &&
			(ret = __log_c_get(logc, &first_lsn, &data, DB_SET)) == 0) {
			/* We have a recent checkpoint.  This is LSN (1). */
			if ((ret = __txn_ckp_read(dbenv,
					data.data, &ckp_args)) != 0) {
				__db_err(dbenv,
					"Invalid checkpoint record at [%ld][%ld]",
					(u_long) first_lsn.file,
					(u_long) first_lsn.offset);
				goto err;
			}
			first_lsn = ckp_args->ckp_lsn;
		}
		if ((ret = __log_c_get(logc, &first_lsn, &data, DB_SET)) != 0)
			goto err;
		if ((ret = __env_openfiles(dbenv, logc,
				txninfo, &data, &first_lsn, NULL, nfiles, 1)) != 0)
			goto err;
	} else if (region->stat.st_nrestores == 0) {
		/*
		 * If there are no prepared transactions that need resolution,
		 * we need to reset the transaction ID space and log this fact.
		 */
		assert(!gbl_is_physical_replicant);

		if ((ret = __txn_reset(dbenv)) != 0)
			goto err;
	}


	if (FLD_ISSET(dbenv->verbose, DB_VERB_RECOVERY)) {
		(void)time(&now);
		char my_buf[30];
		__db_err(dbenv, "Recovery complete at %.24s", ctime_r(&now, my_buf));
		__db_err(dbenv, "%s %lx %s [%lu][%lu]",
			"Maximum transaction ID",
			(u_long) (txninfo == NULL ?
			TXN_MINIMUM : ((DB_TXNHEAD *) txninfo)->maxid),
			"Recovery checkpoint",
			(u_long) region->last_ckp.file,
			(u_long) region->last_ckp.offset);
	}
	log_recovery_progress(3, -1);

	if (0) {
msgerr:	__db_err(dbenv,
			"Recovery function for LSN %lu %lu failed on %s pass",
			(u_long) lsn.file, (u_long) lsn.offset, pass);
#if defined (DEBUG_ABORT_ON_RECOVERY_FAILURE)
		__log_flush(dbenv, NULL);
		abort();
#endif
	}

err:	if (logc != NULL && (t_ret = __log_c_close(logc)) != 0 && ret == 0)
		ret = t_ret;

	if (txninfo != NULL)
		__db_txnlist_end(dbenv, txninfo);

	if (dtab != NULL)
		__os_free(dbenv, dtab);

	if (ckp_args != NULL)
		__os_free(dbenv, ckp_args);

	/* if recovery went ok, do an extra verify pass */
	if (ret == 0 && dbenv->attr.recovery_verify) {
		ret = full_recovery_check(dbenv, max_lsn);
		if (!dbenv->attr.recovery_verify_fatal)
			ret = 0;
	}

	dbenv->tx_timestamp = 0;

	/* Clear in-recovery flags. */
	F_CLR((DB_LOG *) dbenv->lg_handle, DBLOG_RECOVER);

	/*   BDB_RELLOCK(); */

	F_CLR(region, TXN_IN_RECOVERY);
	dbenv->recovery_pass = DB_TXN_NOT_IN_RECOVERY;

	return (ret);
}

/*
 * Figure out how many logfiles we have processed.  If we are moving
 * forward (is_forward != 0), then we're computing current - low.  If
 * we are moving backward, we are computing high - current.  max is
 * the number of bytes per logfile.
 */
static double
__lsn_diff(low, high, current, max, is_forward)
	DB_LSN *low, *high, *current;
	u_int32_t max;
	int is_forward;
{
	double nf;

	/*
	 * There are three cases in each direction.  If you are in the
	 * same file, then all you need worry about is the difference in
	 * offsets.  If you are in different files, then either your offsets
	 * put you either more or less than the integral difference in the
	 * number of files -- we need to handle both of these.
	 */
	if (is_forward) {
		if (current->file == low->file)
			nf = (double)(current->offset - low->offset) / max;
		else if (current->offset < low->offset)
			nf = (double)(current->file - low->file - 1) +
				(double)(max - low->offset + current->offset) / max;
		else
			nf = (double)(current->file - low->file) +
				(double)(current->offset - low->offset) / max;
	} else {
		if (current->file == high->file)
			nf = (double)(high->offset - current->offset) / max;
		else if (current->offset > high->offset)
			nf = (double)(high->file - current->file - 1) + (double)
				(max - current->offset + high->offset) / max;
		else
			nf = (double)(high->file - current->file) +
				(double)(high->offset - current->offset) / max;
	}
	return (nf);
}


/* This is identical to __log_backup, except it finds the lsn of the
 * checkpoint (ie: of the checkpoint record), not the LSN IN the checkpoint
 * record.  If I had like 17 more braincells, I'd have __log_backup use
 * this routine, but I'd rather not break things. */
static int
__log_find_latest_checkpoint_before_lsn(DB_ENV * dbenv, DB_LOGC * logc,
	DB_LSN * max_lsn, DB_LSN * start_lsn)
{
	DB_LSN lsn;
	DBT data;
	__txn_ckp_args *ckp_args = NULL;
	int ret;

	memset(&data, 0, sizeof(data));
	ckp_args = NULL;

	/*
	 * Follow checkpoints through the log until we find one with
	 * a ckp_lsn less than max_lsn.
	 */
	if ((ret = __txn_getckp(dbenv, &lsn)) != 0)
		return ret;

	if (IS_ZERO_LSN(lsn))
		logmsg(LOGMSG_WARN, "last_ckp lsn is 0:0\n");

	while ((ret = __log_c_get(logc, &lsn, &data, DB_SET)) == 0) {
		if ((ret = __txn_ckp_read(dbenv, data.data, &ckp_args)) != 0)
			return (ret);
		if (log_compare(&lsn, max_lsn) < 0) {
			*start_lsn = lsn;
			__os_free(dbenv, ckp_args);
			return 0;
		}

		lsn = ckp_args->last_ckp;
		if (IS_ZERO_LSN(lsn))
			break;
		__os_free(dbenv, ckp_args);
		ckp_args = NULL;
	}

	if (ckp_args != NULL)
		__os_free(dbenv, ckp_args);
	if (ret == 0)
		ret = DB_NOTFOUND;
	return ret;
}

/* This is functionaly identical to __log_find_latest_checkpoint_before_lsn, but
 * instead of traversing checkpoints in order, traverses the entire log.  This
 * may be needed if for some reason there's no checkpoint in the log, or if the 
 * checkpoint chain is broken (ie: last_ckp points to 0:0) */
static int
__log_find_latest_checkpoint_before_lsn_try_harder(DB_ENV * dbenv,
	DB_LOGC * logc, DB_LSN * max_lsn, DB_LSN * foundlsn)
{
	DB_LSN lsn;
	DBT data;
	int ret;
	u_int32_t type;

	memset(&data, 0, sizeof(data));
	data.flags = DB_DBT_REALLOC;

	ret = __log_c_get(logc, &lsn, &data, DB_LAST);
	if (ret) {
		logmsg(LOGMSG_ERROR, "Can't find last log record rc %d\n", ret);
		return ret;
	}

	do {
		if (data.size >= sizeof(u_int32_t)) {
			LOGCOPY_32(&type, data.data);
			normalize_rectype(&type);
			if (type == DB___txn_ckp) {
				if (log_compare(&lsn, max_lsn) < 0) {
					*foundlsn = lsn;
					free(data.data);
					return 0;
				}
			}
		}
	} while ((ret = __log_c_get(logc, &lsn, &data, DB_PREV)) == 0);

	return ret;
}

void
__test_last_checkpoint(DB_ENV * dbenv, int file, int offset)
{
	DB_LSN lsn;
	DB_LSN outlsn;
	DB_LOGC *logc;
	int rc;


	rc = dbenv->log_cursor(dbenv, &logc, 0);
	if (rc) {
		logmsg(LOGMSG_ERROR, "can't get cursor\n");
		return;
	}

	__txn_getckp(dbenv, &lsn);

	logmsg(LOGMSG_USER, "start: %u:%u\n", lsn.file, lsn.offset);
	do {
		rc = __log_find_latest_checkpoint_before_lsn(dbenv, logc, &lsn,
			&outlsn);
		logmsg(LOGMSG_USER, "next: %u:%u\n", outlsn.file, outlsn.offset);
		lsn = outlsn;
	} while (rc == 0);

	logc->close(logc, 0);
}

/*
 * __log_backup --
 * PUBLIC: int __log_backup __P((DB_ENV *, DB_LOGC *, DB_LSN *, DB_LSN *));
 *
 * This is used to find the earliest log record to process when a client
 * is trying to sync up with a master whose max LSN is less than this
 * client's max lsn; we want to roll back everything after that
 *
 * Find the latest checkpoint whose ckp_lsn is less than the max lsn.
 */
int
__log_backup(dbenv, logc, max_lsn, start_lsn)
	DB_ENV *dbenv;
	DB_LOGC *logc;
	DB_LSN *max_lsn, *start_lsn;
{
	DB_LSN lsn;
	DBT data;
	__txn_ckp_args *ckp_args;
	int ret;

	memset(&data, 0, sizeof(data));
	ckp_args = NULL;

	/*
	 * Follow checkpoints through the log until we find one with
	 * a ckp_lsn less than max_lsn.
	 */
	if ((ret = __txn_getckp(dbenv, &lsn)) != 0)
		goto err;
	while ((ret = __log_c_get(logc, &lsn, &data, DB_SET)) == 0) {
		if ((ret = __txn_ckp_read(dbenv, data.data, &ckp_args)) != 0)
			return (ret);
		if (log_compare(&ckp_args->ckp_lsn, max_lsn) <= 0) {
			*start_lsn = ckp_args->ckp_lsn;
			break;
		}

		lsn = ckp_args->last_ckp;
		if (IS_ZERO_LSN(lsn))
			break;
		__os_free(dbenv, ckp_args);
		ckp_args = NULL;
	}

	if (ckp_args != NULL)
		__os_free(dbenv, ckp_args);
err:	if (IS_ZERO_LSN(*start_lsn) && (ret == 0 || ret == DB_NOTFOUND))
		ret = __log_c_get(logc, start_lsn, &data, DB_FIRST);
	return (ret);
}

/*
 * __log_earliest --
 *
 * Return the earliest recovery point for the log files present.  The
 * earliest recovery time is the time stamp of the first checkpoint record
 * whose checkpoint LSN is greater than the first LSN we process.
 */
static int
__log_earliest(dbenv, logc, lowtime, lowlsn)
	DB_ENV *dbenv;
	DB_LOGC *logc;
	int32_t *lowtime;
	DB_LSN *lowlsn;
{
	DB_LSN first_lsn, lsn;
	DBT data;
	__txn_ckp_args *ckpargs;
	u_int32_t rectype;
	int cmp, ret;

	memset(&data, 0, sizeof(data));
	/*
	 * Read forward through the log looking for the first checkpoint
	 * record whose ckp_lsn is greater than first_lsn.
	 */

	for (ret = __log_c_get(logc, &first_lsn, &data, DB_FIRST);
		ret == 0; ret = __log_c_get(logc, &lsn, &data, DB_NEXT)) {
		LOGCOPY_32(&rectype, data.data);
		normalize_rectype(&rectype);
		if (rectype != DB___txn_ckp)
			continue;
		if ((ret = __txn_ckp_read(dbenv, data.data, &ckpargs)) == 0) {
			cmp = log_compare(&ckpargs->ckp_lsn, &first_lsn);
			*lowlsn = ckpargs->ckp_lsn;
			*lowtime = ckpargs->timestamp;

			__os_free(dbenv, ckpargs);
			if (cmp >= 0)
				break;
		}
	}

	return (ret);
}

/*
 * __env_openfiles --
 * Perform the pass of recovery that opens files.  This is used
 * both during regular recovery and an initial call to txn_recover (since
 * we need files open in order to abort prepared, but not yet committed
 * transactions).
 *
 * See the comments in db_apprec for a detailed description of the
 * various recovery passes.
 *
 * If we are not doing feedback processing (i.e., we are doing txn_recover
 * processing and in_recovery is zero), then last_lsn can be NULL.
 *
 * PUBLIC: int __env_openfiles __P((DB_ENV *, DB_LOGC *,
 * PUBLIC:	 void *, DBT *, DB_LSN *, DB_LSN *, double, int));
 */
int
__env_openfiles(dbenv, logc, txninfo,
	data, open_lsn, last_lsn, nfiles, in_recovery)
	DB_ENV *dbenv;
	DB_LOGC *logc;
	void *txninfo;
	DBT *data;
	DB_LSN *open_lsn, *last_lsn;
	int in_recovery;
	double nfiles;
{
	DB_LSN lsn;
	u_int32_t log_size;
	int progress, ret;
   DB_LSN last_good_lsn = {0};

	/*
	 * XXX
	 * Get the log size.  No locking required because we're single-threaded
	 * during recovery.
	 */
	log_size =
		((LOG *) (((DB_LOG *) dbenv->lg_handle)->reginfo.primary))->
		log_size;

	logmsg(LOGMSG_INFO, "%s: open files from lsn %u:%u\n", __func__, open_lsn->file,
		open_lsn->offset);
	lsn = *open_lsn;

	for (;;) {
		if (in_recovery) {
			DB_ASSERT(last_lsn != NULL);
#if 0
			progress = (int)(33 * (__lsn_diff(open_lsn,
#else
			progress = (int)(100 * (__lsn_diff(open_lsn,
#endif
				last_lsn, &lsn, log_size, 1) / nfiles));
			log_recovery_progress(1, progress);

			if (dbenv->db_feedback != NULL) {
				dbenv->db_feedback(dbenv, DB_RECOVER, progress);
			}
		}
		ret = __db_dispatch(dbenv,
			dbenv->recover_dtab, dbenv->recover_dtab_size, data, &lsn,
			in_recovery ? DB_TXN_OPENFILES : DB_TXN_POPENFILES,
			txninfo);
		if (ret != 0 && ret != DB_TXN_CKP) {
			__db_err(dbenv,
				"Recovery function for LSN %lu %lu failed, ret=%d",
				(u_long) lsn.file, (u_long) lsn.offset, ret);
#if defined (DEBUG_ABORT_ON_RECOVERY_FAILURE)
			__log_flush(dbenv, NULL);
			abort();
#endif
			break;
		}
		if ((ret = __log_c_get(logc, &lsn, data, DB_NEXT)) != 0) {
			if (ret == DB_NOTFOUND) {
			/* if we fail to get this lsn, and this is NOT the last
			record, it can be a corrupted record in the middle, abort! */
			DB_LSN cmp_lsn;
			if (last_lsn == NULL) {
			   /* get here the know tail of the log */
			   ret = __log_c_get(logc, &cmp_lsn, data, DB_LAST);
			   if (ret)
				  abort();  
			} else {
			   cmp_lsn = *last_lsn;
			}
			if (last_good_lsn.file != cmp_lsn.file || last_good_lsn.offset != cmp_lsn.offset) {
			   __db_err(dbenv,
					 "Recovery open file failed in the middle lsn %d.%d\n",
					 last_good_lsn.file, last_good_lsn.offset);
			   abort();
			}

				ret = 0;
		 }
		 break;
		} else  {
		 last_good_lsn = lsn;
	  }
	}

	return (ret);
}

void bdb_set_gbl_recoverable_lsn(void *lsn, int32_t timestamp);
int bdb_update_logfile_pglogs(void *bdb_state, void *pglogs, unsigned int nkeys,
	DB_LSN logical_commit_lsn, hash_t *fileid_tbl);
int bdb_update_ltran_pglogs_hash(void *bdb_state, void *pglogs,
	unsigned int nkeys, unsigned long long logical_tranid,
	int is_logical_commit, DB_LSN logical_commit_lsn, hash_t *fileid_tbl);
int transfer_ltran_pglogs_to_gbl(void *bdb_state,
	unsigned long long logical_tranid, DB_LSN logical_commit_lsn);
int bdb_relink_logfile_pglogs(void *bdb_state, unsigned char *fileid,
	db_pgno_t pgno, db_pgno_t prev_pgno, db_pgno_t next_pgno, DB_LSN lsn,
	hash_t *fileid_tbl);
int bdb_update_timestamp_lsn(void *bdb_state, int32_t timestamp, DB_LSN lsn,
	unsigned long long context);
int bdb_checkpoint_list_push(DB_LSN lsn, DB_LSN ckp_lsn, int32_t timestamp);
extern DB_LSN bdb_latest_commit_lsn;
extern pthread_mutex_t bdb_asof_current_lsn_mutex;

/*
 * __recover_logfile_pglogs
 *
 * recover the global pglogs structure from the log file
 *
 */
#define GOTOERR do{ lineno=__LINE__; goto err; } while(0);

extern int bdb_push_pglogs_commit_recovery(void *in_bdb_state, DB_LSN commit_lsn,
	uint32_t gen, unsigned long long ltranid, int push);

int
__recover_logfile_pglogs(dbenv, fileid_tbl)
	DB_ENV *dbenv;
   void *fileid_tbl;
{
	DB_LOGC *logc;
	DB_LOGC *logc_prep;
	DB_LSN first_lsn, lsn, preplsn;
	DBT data;
	int ret;
	int lineno = 0;
	int got_recoverable_lsn = 0;
	int not_newsi_log_format = 0;
	void *keylist = NULL;
	u_int32_t keycnt = 0;
	DB *file_dbp;
	DB_MPOOLFILE *mpf;
	u_int32_t rectype;

	__txn_ckp_args *ckp_args = NULL;
	__txn_regop_args *txn_args = NULL;
	__txn_regop_gen_args *txn_gen_args = NULL;
	__txn_dist_commit_args *txn_dist_args = NULL;
	__txn_dist_prepare_args *txn_prep_args = NULL;
	__txn_regop_rowlocks_args *txn_rl_args = NULL;
	__bam_split_args *split_args = NULL;
	__bam_rsplit_args *rsplit_args = NULL;
	__db_relink_args *relink_args = NULL;
	void *free_ptr = NULL;
	void *free_ptr2 = NULL;

	logc = NULL;
    logc_prep = NULL;
	memset(&data, 0, sizeof(data));

	/* Allocate a cursor for the log. */
	if ((ret = __log_cursor(dbenv, &logc)) != 0)
		return ret;

	for (ret = __log_c_get(logc, &first_lsn, &data, DB_FIRST), lsn =
		first_lsn; ret == 0;
		ret = __log_c_get(logc, &lsn, &data, DB_NEXT)) {
		LOGCOPY_32(&rectype, data.data);
		normalize_rectype(&rectype);
		switch (rectype) {
		case DB___txn_ckp:
			if ((ret =
				__txn_ckp_read(dbenv, data.data,
					&ckp_args)) != 0) {
				GOTOERR;
		 }
		 free_ptr = ckp_args;

			ret =
				bdb_checkpoint_list_push(lsn, ckp_args->ckp_lsn,
				ckp_args->timestamp);
			if (ret) {
				logmsg(LOGMSG_ERROR, 
				  "%s: failed to push to checkpoint list, ret %d\n",
					__func__, ret);
		   	GOTOERR;
		 }

			if (!got_recoverable_lsn) {
				ret =
					log_compare(&ckp_args->ckp_lsn, &first_lsn);
				if (ret >= 0) {
					bdb_set_gbl_recoverable_lsn(&lsn,
						ckp_args->timestamp);
					got_recoverable_lsn = 1;
					logmsg(LOGMSG_WARN, "set gbl_recoverable_lsn as [%d][%d]\n",
						lsn.file, lsn.offset);
				}
			}

			break;
		case DB___txn_regop_gen:
			if ((ret =
				__txn_regop_gen_read(dbenv, data.data,
					&txn_gen_args)) != 0) {
				GOTOERR;
		 }
		 bdb_push_pglogs_commit_recovery(dbenv->app_private, lsn, 
			   txn_gen_args->generation, 0, 0);
			free_ptr = txn_gen_args;

			ret =
				bdb_update_timestamp_lsn(dbenv->app_private,
				txn_gen_args->timestamp, lsn,
				txn_gen_args->context);
			if (ret) {
				GOTOERR;
		 }
		   ret = lock_list_parse_pglogs(dbenv, &txn_gen_args->locks, &lsn, 
			   &keylist, &keycnt);
			if (ret) {
				not_newsi_log_format = 1;
				break;
			}
			ret =
				bdb_update_logfile_pglogs(dbenv->app_private,
				keylist, keycnt, lsn, fileid_tbl);
			if (ret) {
				GOTOERR;
		 }
		 break;
		case DB___txn_dist_commit:
			if ((ret = __txn_dist_commit_read(dbenv, data.data,
				&txn_dist_args)) != 0) {
				GOTOERR;
			}
			bdb_push_pglogs_commit_recovery(dbenv->app_private, lsn, txn_dist_args->generation, 0, 0);
			ret = bdb_update_timestamp_lsn(dbenv->app_private,
				txn_dist_args->timestamp, lsn, txn_dist_args->context);
			if (ret) {
				GOTOERR;
			}

			if (logc_prep == NULL) {
				if ((ret = __log_cursor(dbenv, &logc_prep)) != 0) {
					return ret;
				}
			}
			preplsn = txn_dist_args->prev_lsn;

			free(txn_dist_args);
			txn_dist_args = NULL;

			ret = __log_c_get(logc_prep, &preplsn, &data, DB_SET);
			if (ret != 0) {
				GOTOERR;
			}

			LOGCOPY_32(&rectype, data.data);
			normalize_rectype(&rectype);
			assert(rectype == DB___txn_dist_prepare);

			if ((ret = 
				__txn_dist_prepare_read(dbenv, data.data, &txn_prep_args)) != 0) {
				GOTOERR;
			}

			ret = lock_list_parse_pglogs(dbenv, &txn_prep_args->locks, &lsn, &keylist, &keycnt);
			if (ret) {
				not_newsi_log_format = 1;
				break;
			}

			ret = bdb_update_logfile_pglogs(dbenv->app_private, keylist, keycnt, lsn, fileid_tbl);
			if (ret) {
				GOTOERR;
			}
			break;
		case DB___txn_regop:
			if ((ret =
				__txn_regop_read(dbenv, data.data,
					&txn_args)) != 0) {
				GOTOERR;
		 }
		 bdb_push_pglogs_commit_recovery(dbenv->app_private, lsn, 0, 0, 0);
			free_ptr = txn_args;

			ret =
				bdb_update_timestamp_lsn(dbenv->app_private,
				txn_args->timestamp, lsn,
				__txn_regop_read_context(txn_args));
			if (ret) {
				GOTOERR;
		 }
		   ret = lock_list_parse_pglogs(dbenv,
					&txn_args->locks, &lsn, &keylist, &keycnt);
			if (ret) {
				not_newsi_log_format = 1;
				break;
			}
			ret =
				bdb_update_logfile_pglogs(dbenv->app_private,
				keylist, keycnt, lsn, fileid_tbl);
			if (ret) {
				GOTOERR;
		 }
		 break;
		case DB___txn_regop_rowlocks:
			if ((ret =
				__txn_regop_rowlocks_read(dbenv, data.data,
					&txn_rl_args)) != 0) {
				GOTOERR;
		 }
		 bdb_push_pglogs_commit_recovery(dbenv->app_private, lsn, 
			   txn_rl_args->generation, 0, 0);
			free_ptr = txn_rl_args;

			ret = bdb_update_timestamp_lsn(dbenv->app_private,
				txn_rl_args->timestamp, lsn, txn_rl_args->context);
			if (ret) {
				GOTOERR;
		 }
		   ret = lock_list_parse_pglogs(dbenv,
					&txn_rl_args->locks, &lsn, &keylist,
					&keycnt);
			if (ret) {
				logmsg(LOGMSG_ERROR, 
				"%s line %d: couldn't parse pagelogs for regop_rowlocks at lsn %d:%d\n",
					__func__, __LINE__, lsn.file, lsn.offset);
				not_newsi_log_format = 1;
				break;
			}

			ret =
				bdb_update_ltran_pglogs_hash(dbenv->app_private,
				keylist, keycnt, txn_rl_args->ltranid,
				(txn_rl_args->lflags & DB_TXN_LOGICAL_COMMIT), lsn, fileid_tbl);
			if (ret) {
				GOTOERR;
		 }
 		  if (txn_rl_args->lflags & DB_TXN_LOGICAL_COMMIT) {
					ret = transfer_ltran_pglogs_to_gbl(dbenv->
						app_private, txn_rl_args->ltranid,
						lsn);
					if (ret) {
						GOTOERR;
			   }
		 }

						break;
		case DB___bam_split:
					if ((ret =
						__bam_split_read(dbenv,
							data.data,
							&split_args)) != 0) {
						GOTOERR;
			   }
			   free_ptr = split_args;

					if ((ret =
						__dbreg_id_to_db(dbenv,
							split_args->txnid,
							&file_dbp,
							split_args->fileid, 1, &lsn,
							0)) != 0) {
						// Assume this was later fastinit'd
						break;
					}

					mpf = file_dbp->mpf;

					ret =
						bdb_relink_logfile_pglogs(dbenv->
						app_private, mpf->fileid,
						split_args->left, split_args->right,
						PGNO_INVALID, lsn, fileid_tbl);
					if (ret) { 
						GOTOERR;
			   }
			   break;
		case DB___bam_rsplit:
					if ((ret =
						__bam_rsplit_read(dbenv,
							data.data,
							&rsplit_args)) != 0) {
						GOTOERR;
			   }
			   free_ptr = rsplit_args;

					if ((ret =
						__dbreg_id_to_db(dbenv,
							rsplit_args->txnid,
							&file_dbp,
							rsplit_args->fileid, 1,
							&lsn, 0)) != 0)
						break;

					mpf = file_dbp->mpf;

					ret =
						bdb_relink_logfile_pglogs(dbenv->
						app_private, mpf->fileid,
						rsplit_args->pgno,
						rsplit_args->root_pgno,
						PGNO_INVALID, lsn, fileid_tbl);
					if (ret) {
						GOTOERR;
			   }
			   break;
		case DB___db_relink:
					if ((ret =
						__db_relink_read(dbenv,
							data.data,
							&relink_args)) != 0) {
						GOTOERR;
			   }
			   free_ptr = relink_args;

					if ((ret =
						__dbreg_id_to_db(dbenv,
							relink_args->txnid,
							&file_dbp,
							relink_args->fileid, 1,
							&lsn, 0)) != 0)
						break;
					mpf = file_dbp->mpf;

					ret =
						bdb_relink_logfile_pglogs(dbenv->
						app_private, mpf->fileid,
						relink_args->pgno,
						relink_args->prev,
						relink_args->next, lsn, fileid_tbl);
					if (ret) {
						GOTOERR;
			   }
			   break;
		default:
					free_ptr = NULL;
					break;
				}
			if (free_ptr) {
				__os_free(dbenv, free_ptr);
				free_ptr = NULL;
			}
			if (free_ptr2) {
				__os_free(dbenv, free_ptr2);
				free_ptr2 = NULL;
			}
			if (keylist)
				__os_free(dbenv, keylist);
			keylist = NULL;
			keycnt = 0;
		}

		if (not_newsi_log_format) {
			logmsg(LOGMSG_ERROR, 
			   "NEWSI recovery error: lack of pglogs info on some commit records\n");
			ret = -1;
		} else {
			ret = 0;
		}

err:
		if (lineno)
			logmsg(LOGMSG_ERROR, "%s error from %d\n", __func__, lineno);

		if (free_ptr) {
			__os_free(dbenv, free_ptr);
			free_ptr = NULL;
		}

		if (keylist)
			__os_free(dbenv, keylist);
		keylist = NULL;
		keycnt = 0;

		if (logc) {
			logc->close(logc, 0);
		}

		if (logc_prep) {
			logc_prep->close(logc_prep, 0);
		}

		return ret;
}

/*
 * __env_find_verify_recover_start --
 *  We need to be able to recover to the 1st matchable commit record
 *  preceding the last sync LSN. The function returns the recovery
 *  starting point in `lsnp'.
 *
 * PUBLIC: int __env_find_verify_recover_start __P((DB_ENV *, DB_LSN *));
 */
int
__env_find_verify_recover_start(dbenv, lsnp)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
{
	int ret;
	u_int32_t rectype;
	DB_LSN txnlsn, s_lsn;
	__txn_ckp_args *ckp_args;
	__db_debug_args *debug_args;
	DBT rec = {0};
	DB_LOGC *logc = NULL;
	int optype = 0;

	rec.flags = DB_DBT_REALLOC;

	/* Step 1: Find the 1st matchable commit record
			   lower than the last sync LSN. */
	if ((ret = __log_sync_lsn(dbenv, &s_lsn)) != 0)
		goto err;
	if ((ret = __log_cursor(dbenv, &logc)) != 0)
		goto err;
	if ((ret = __log_c_get(logc, lsnp, &rec, DB_LAST)) != 0)
		goto err;

	do {
		LOGCOPY_32(&rectype, rec.data);
		normalize_rectype(&rectype);
	} while ((!matchable_log_type(rectype) || log_compare(lsnp, &s_lsn) >= 0) &&
			 (ret = __log_c_get(logc, lsnp, &rec, DB_PREV)) == 0);

	if (ret != 0)
		goto err;

	/* Step 2: find the checkpoint LSN of the checkpoint
			   preceding the commit record. */
	if ((ret = __txn_getckp(dbenv, &txnlsn)) != 0)
		goto err;

	while ((ret = __log_c_get(logc, &txnlsn, &rec, DB_SET)) == 0) {
		if ((ret = __txn_ckp_read(dbenv, rec.data, &ckp_args)) != 0)
			return (ret);
		if (log_compare(&txnlsn, lsnp) < 0) {
			*lsnp = ckp_args->ckp_lsn;
			txnlsn = ckp_args->last_ckp;
			__os_free(dbenv, ckp_args);
			break;
		}

		txnlsn = ckp_args->last_ckp;
		if (IS_ZERO_LSN(txnlsn))
			goto err;
		__os_free(dbenv, ckp_args);
	}

	/* Step 3: find the checkpoint preceding the ckp LSN from Step 2. */
	while ((ret = __log_c_get(logc, &txnlsn, &rec, DB_SET)) == 0) {
		if ((ret = __txn_ckp_read(dbenv, rec.data, &ckp_args)) != 0)
			return (ret);
		if (log_compare(&txnlsn, lsnp) < 0) {
			*lsnp = txnlsn;
			__os_free(dbenv, ckp_args);
			break;
		}

		txnlsn = ckp_args->last_ckp;
		if (IS_ZERO_LSN(txnlsn))
			goto err;
		__os_free(dbenv, ckp_args);
	}

	/* Step 4: find the start of DBREG records of the ckp from Step 3. */
	do {
		LOGCOPY_32(&rectype, rec.data);
		normalize_rectype(&rectype);
		optype = -1;
		if (rectype == DB___db_debug) {
			ret = __db_debug_read(dbenv, rec.data, &debug_args);
			if (ret)
				goto err;
			LOGCOPY_32(&optype, debug_args->op.data);
			__os_free(dbenv, debug_args);
		}
	} while ((rectype != DB___db_debug || optype != 2) &&
			(ret = __log_c_get(logc, lsnp, &rec, DB_PREV)) == 0);

err:
	if (logc)
		(void)__log_c_close(logc);
	__os_ufree(dbenv, rec.data);
	return (ret);
}
