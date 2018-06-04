/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: log_get.c,v 11.98 2003/09/13 19:20:38 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <string.h>
#endif

#include <assert.h>
#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/hmac.h"
#include "dbinc/log.h"
#include "dbinc/db_swap.h"
#include "dbinc/hash.h"
#include <epochlib.h>

typedef enum { L_ALREADY, L_ACQUIRED, L_NONE } RLOCK;

/* Need to acquire this for log_get. */
extern pthread_mutex_t log_write_lk;

static int __log_c_close_pp __P((DB_LOGC *, u_int32_t));
static int __log_c_get_pp __P((DB_LOGC *, DB_LSN *, DBT *, u_int32_t));
static int __log_c_stat_pp __P((DB_LOGC *, DB_LOGC_STAT **));
static int __log_c_get_int __P((DB_LOGC *, DB_LSN *, DBT *, u_int32_t));
static int __log_c_hdrchk __P((DB_LOGC *, DB_LSN *, HDR *, int *));
static int __log_c_incursor __P((DB_LOGC *, DB_LSN *, HDR *, u_int8_t **));
static int __log_c_inregion __P((DB_LOGC *,
	       DB_LSN *, RLOCK *, DB_LSN *, HDR *, u_int8_t **));
static int __log_c_io __P((DB_LOGC *,
	       u_int32_t, u_int32_t, void *, size_t *, int *));
static int __log_c_ondisk __P((DB_LOGC *,
	       DB_LSN *, DB_LSN *, int, HDR *, u_int8_t **, int *));
static int __log_c_set_maxrec __P((DB_LOGC *, char *));
static int __log_c_shortread __P((DB_LOGC *, DB_LSN *, int));

/*
 * __log_cursor_pp --
 *	DB_ENV->log_cursor
 *
 * PUBLIC: int __log_cursor_pp __P((DB_ENV *, DB_LOGC **, u_int32_t));
 */
int
__log_cursor_pp(dbenv, logcp, flags)
	DB_ENV *dbenv;
	DB_LOGC **logcp;
	u_int32_t flags;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lg_handle, "DB_ENV->log_cursor", DB_INIT_LOG);

	/* Validate arguments. */
	if ((ret = __db_fchk(dbenv, "DB_ENV->log_cursor", flags, 0)) != 0)
		return (ret);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __log_cursor(dbenv, logcp);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}


static pthread_mutex_t curlk = PTHREAD_MUTEX_INITIALIZER;
static DB_LOGC *curhd = NULL;

static inline int __log_cursor_cache(dbenv, logcp)
    DB_ENV *dbenv;
    DB_LOGC **logcp;
{
	DB_LOGC *logc = NULL;

    if (!dbenv->attr.log_cursor_cache)
        return -1;

    pthread_mutex_lock(&curlk);
    if (curhd) 
    {
        logc = curhd;
        curhd = curhd->next;
        if (curhd) curhd->prev = NULL;
        logc->next = logc->prev = NULL;
    }
    pthread_mutex_unlock(&curlk);
    *logcp = logc;
    return logc ? 0 : -1;
}

/*
 * __log_cursor_complete --
 *	Create a log cursor.
 *
 * PUBLIC: int __log_cursor_complete __P((DB_ENV *, DB_LOGC **, int bpsize, int maxrec));
 */
int
__log_cursor_complete(dbenv, logcp, bpsize, maxrec)
	DB_ENV *dbenv;
	DB_LOGC **logcp;
	int bpsize;
	int maxrec;
{
	DB_LOGC *logc;
	int ret;

	*logcp = NULL;

    if (bpsize || maxrec || (ret = __log_cursor_cache(dbenv, &logc)) != 0) {

        /* Allocate memory for the cursor. */
        if ((ret = __os_calloc(dbenv, 1, sizeof(DB_LOGC), &logc)) != 0)
            return (ret);

        logc->bp_size = bpsize ? bpsize : DB_LOGC_BUF_SIZE;
        /*
         * Set this to something positive.
         */
        logc->bp_maxrec = maxrec ? maxrec : MEGABYTE;
        if ((ret = __os_malloc(dbenv, logc->bp_size, &logc->bp)) != 0) {
            __os_free(dbenv, logc);
            return (ret);
        }

        logc->dbenv = dbenv;
        logc->close = __log_c_close_pp;
        logc->get = __log_c_get_pp;
        logc->stat = __log_c_stat_pp;
    }
    logc->incursor_count = 0;
    logc->ondisk_count = 0;
    logc->inregion_count = 0;
    logc->incursorus = 0;
    logc->ondiskus = 0;
    logc->inregionus = 0;
    logc->totalus = 0;
    logc->lockwaitus = 0;

    if (bpsize || maxrec)
        F_SET(logc, DB_LOG_CUSTOM_SIZE);

	*logcp = logc;
	return (0);
}

/*
 * __log_cursor --
 *	Create a log cursor.
 *
 * PUBLIC: int __log_cursor __P((DB_ENV *, DB_LOGC **));
 */
int
__log_cursor(dbenv, logcp)
	DB_ENV *dbenv;
	DB_LOGC **logcp;
{
    return __log_cursor_complete(dbenv, logcp, 0, 0);
}

/*
 * __log_c_close_pp --
 *	DB_LOGC->close pre/post processing.
 */
static int
__log_c_close_pp(logc, flags)
	DB_LOGC *logc;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	int rep_check, ret;

	dbenv = logc->dbenv;

	PANIC_CHECK(dbenv);
	if ((ret = __db_fchk(dbenv, "DB_LOGC->close", flags, 0)) != 0)
		return (ret);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __log_c_close(logc);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __log_c_close --
 *	DB_LOGC->close.
 *
 * PUBLIC: int __log_c_close __P((DB_LOGC *));
 */
int
__log_c_close(logc)
	DB_LOGC *logc;
{
	DB_ENV *dbenv;

	dbenv = logc->dbenv;

    if (dbenv->attr.log_cursor_cache && !F_ISSET(logc, DB_LOG_CUSTOM_SIZE))
    {
        ZERO_LSN(logc->c_lsn);
        logc->c_len = 0;
        logc->c_prev = 0;
        if (logc->c_dbt.data != NULL)
            __os_free(dbenv, logc->c_dbt.data);
        memset(&logc->c_dbt, 0, sizeof(DBT));
        ZERO_LSN(logc->bp_lsn);
        logc->bp_maxrec = 0;
        logc->bp_rlen = 0;
        if (logc->c_fhp != NULL) {
            (void)__os_closehandle(dbenv, logc->c_fhp);
            logc->c_fhp = NULL;
        }
        pthread_mutex_lock(&curlk);
        logc->prev = NULL;
        logc->next = curhd;
        if (curhd) curhd->prev = logc;
        curhd = logc;
        pthread_mutex_unlock(&curlk);
        return (0);
    }

	if (logc->c_fhp != NULL) {
		(void)__os_closehandle(dbenv, logc->c_fhp);
		logc->c_fhp = NULL;
	}

	if (logc->c_dbt.data != NULL)
		__os_free(dbenv, logc->c_dbt.data);

	__os_free(dbenv, logc->bp);
	__os_free(dbenv, logc);

	return (0);
}

/*
 * __log_c_stat_pp
 *  DB_LOGC->stat pre/post processing.
 *
 */
static int
__log_c_stat_pp(logc, stats)
	DB_LOGC *logc;
	DB_LOGC_STAT **stats;
{
	DB_ENV *dbenv;
	DB_LOGC_STAT *st;
	int ret;

	dbenv = logc->dbenv;
	PANIC_CHECK(dbenv);

	if ((ret = __os_umalloc(dbenv, sizeof(DB_LOGC_STAT), &st)) != 0)
		return (ret);

	st->incursor_count = logc->incursor_count;
	st->inregion_count = logc->inregion_count;
	st->ondisk_count = logc->ondisk_count;
	st->incursorus = logc->incursorus;
	st->inregionus = logc->inregionus;
	st->ondiskus = logc->ondiskus;
	st->totalus = logc->totalus;
	st->lockwaitus = logc->lockwaitus;
	*stats = st;

	return 0;
}

/*
 * __log_c_get_pp --
 *	DB_LOGC->get pre/post processing.
 */
static int
__log_c_get_pp(logc, alsn, dbt, flags)
	DB_LOGC *logc;
	DB_LSN *alsn;
	DBT *dbt;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	int rep_check, ret;

	dbenv = logc->dbenv;

	PANIC_CHECK(dbenv);

	/* Validate arguments. */
	switch (flags) {
	case DB_CURRENT:
	case DB_FIRST:
	case DB_LAST:
	case DB_NEXT:
	case DB_PREV:
		break;
	case DB_SET:
		if (IS_ZERO_LSN(*alsn)) {
			__db_err(dbenv, "DB_LOGC->get: invalid LSN: %lu/%lu",
			    (u_long)alsn->file, (u_long)alsn->offset);
			return (EINVAL);
		}
		break;
	default:
		return (__db_ferr(dbenv, "DB_LOGC->get", 1));
	}

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __log_c_get(logc, alsn, dbt, flags);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}


/*
 * __log_persistswap --
 *	Swap the bytes in a log file persistent header from machines with
 *	different endianness.
 *
 * PUBLIC: void __log_persistswap __P((LOGP *));
 */
void
__log_persistswap(persist)
	LOGP *persist;
{
	M_32_SWAP(persist->magic);
	M_32_SWAP(persist->version);
	M_32_SWAP(persist->log_size);
	M_32_SWAP(persist->mode);
}

static int
__log_c_get_timed(logc, alsn, dbt, flags)
	DB_LOGC *logc;
	DB_LSN *alsn;
	DBT *dbt;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	DB_LSN saved_lsn;
	LOGP *persist;
	int ret;

	dbenv = logc->dbenv;

	/*
	 * On error, we take care not to overwrite the caller's LSN.  This
	 * is because callers looking for the end of the log loop using the
	 * DB_NEXT flag, and expect to take the last successful lsn out of
	 * the passed-in structure after DB_LOGC->get fails with DB_NOTFOUND.
	 *
	 * !!!
	 * This line is often flagged an uninitialized memory read during a
	 * Purify or similar tool run, as the application didn't initialize
	 * *alsn.  If the application isn't setting the DB_SET flag, there is
	 * no reason it should have initialized *alsn, but we can't know that
	 * and we want to make sure we never overwrite whatever the application
	 * put in there.
	 */
	saved_lsn = *alsn;

	/*
	 * If we get one of the log's header records as a result of doing a
	 * DB_FIRST, DB_NEXT, DB_LAST or DB_PREV, repeat the operation, log
	 * file header records aren't useful to applications.
	 */
	if ((ret = __log_c_get_int(logc, alsn, dbt, flags)) != 0) {
		*alsn = saved_lsn;
		return (ret);
	}
	if (alsn->offset == 0 && (flags == DB_FIRST ||
	    flags == DB_NEXT || flags == DB_LAST || flags == DB_PREV)) {
		switch (flags) {
		case DB_FIRST:
			flags = DB_NEXT;
			break;
		case DB_LAST:
			flags = DB_PREV;
			break;
		}

		persist = (LOGP *)dbt->data;
		if (LOG_SWAPPED())
			__log_persistswap(persist);
		if (F_ISSET(dbt, DB_DBT_MALLOC)) {
			__os_ufree(dbenv, dbt->data);
			dbt->data = NULL;
		}
		if ((ret = __log_c_get_int(logc, alsn, dbt, flags)) != 0) {
			*alsn = saved_lsn;
			return (ret);
		}
	}

	return (0);
}

/*
 * __log_c_get --
 *	DB_LOGC->get.
 *
 * PUBLIC: int __log_c_get __P((DB_LOGC *, DB_LSN *, DBT *, u_int32_t));
 */
int
__log_c_get(logc, alsn, dbt, flags)
	DB_LOGC *logc;
	DB_LSN *alsn;
	DBT *dbt;
	u_int32_t flags;
{
    u_int64_t start;
    int rc;

    start = comdb2_time_epochus();
    rc = __log_c_get_timed(logc, alsn, dbt, flags);
    logc->totalus += (comdb2_time_epochus() - start);
    return rc;
}


/*
 * __log_c_get_int --
 *	Get a log record; internal version.
 */
static int
__log_c_get_int(logc, alsn, dbt, flags)
	DB_LOGC *logc;
	DB_LSN *alsn;
	DBT *dbt;
	u_int32_t flags;
{
	DB_CIPHER *db_cipher;
	DB_ENV *dbenv;
	DB_LOG *dblp;
	DB_LSN last_lsn, nlsn;
	HDR hdr;
	LOG *lp;
	RLOCK rlock;
	logfile_validity status;
	u_int32_t cnt;
	u_int8_t *rp;
	int eof, is_hmac, ret, st, tot;

	dbenv = logc->dbenv;
	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;
	is_hmac = 0;

	/*
	 * We don't acquire the log region lock until we need it, and we
	 * release it as soon as we're done.
	 */
	rlock = F_ISSET(logc, DB_LOG_LOCKED) ? L_ALREADY : L_NONE;

	nlsn = logc->c_lsn;
	switch (flags) {
	case DB_NEXT:				/* Next log record. */
		if (!IS_ZERO_LSN(nlsn)) {
			/* Increment the cursor by the cursor record size. */
			nlsn.offset += logc->c_len;
			break;
		}
		flags = DB_FIRST;
		/* FALLTHROUGH */
	case DB_FIRST:				/* First log record. */
		/* Find the first log file. */
		if ((ret = __log_find(dblp, 1, &cnt, &status)) != 0)
			goto err;

		/*
		 * DB_LV_INCOMPLETE:
		 *	Theoretically, the log file we want could be created
		 *	but not yet written, the "first" log record must be
		 *	in the log buffer.
		 * DB_LV_NORMAL:
		 * DB_LV_OLD_READABLE:
		 *	We found a log file we can read.
		 * DB_LV_NONEXISTENT:
		 *	No log files exist, the "first" log record must be in
		 *	the log buffer.
		 * DB_LV_OLD_UNREADABLE:
		 *	No readable log files exist, we're at the cross-over
		 *	point between two versions.  The "first" log record
		 *	must be in the log buffer.
		 */
		switch (status) {
		case DB_LV_INCOMPLETE:
			DB_ASSERT(lp->lsn.file == cnt);
			/* FALLTHROUGH */
		case DB_LV_NORMAL:
		case DB_LV_OLD_READABLE:
			nlsn.file = cnt;
			break;
		case DB_LV_NONEXISTENT:
			nlsn.file = 1;
			DB_ASSERT(lp->lsn.file == nlsn.file);
			break;
		case DB_LV_OLD_UNREADABLE:
			nlsn.file = cnt + 1;
			DB_ASSERT(lp->lsn.file == nlsn.file);
			break;
		}
		nlsn.offset = 0;
		break;
	case DB_CURRENT:			/* Current log record. */
		break;
	case DB_PREV:				/* Previous log record. */
		if (!IS_ZERO_LSN(nlsn)) {
			/* If at start-of-file, move to the previous file. */
			if (nlsn.offset == 0) {
				if (nlsn.file == 1 || __log_valid(dblp,
				    nlsn.file - 1, 0, NULL, 0, &status) != 0) {
					ret = DB_NOTFOUND;
					goto err;
				}

				if (status != DB_LV_NORMAL &&
				    status != DB_LV_OLD_READABLE) {
					ret = DB_NOTFOUND;
					goto err;
				}

				--nlsn.file;
			}
			nlsn.offset = logc->c_prev;
			break;
		}
		/* FALLTHROUGH */
	case DB_LAST:				/* Last log record. */
		if (rlock == L_NONE) {
			rlock = L_ACQUIRED;
			st = comdb2_time_epochus();
			R_LOCK(dbenv, &dblp->reginfo);
			tot = (comdb2_time_epochus() - st);
			logc->lockwaitus += (tot > 0 ? tot : 0);
		}
		nlsn.file = lp->lsn.file;
		nlsn.offset = lp->lsn.offset - lp->len;
		break;
	case DB_SET:				/* Set log record. */
		nlsn = *alsn;
		break;
	}

	if (0) {				/* Move to the next file. */
next_file:	++nlsn.file;
		nlsn.offset = 0;
	}

	/*
	 * The above switch statement should have set nlsn to the lsn of
	 * the requested record.
	 */

	if (CRYPTO_ON(dbenv)) {
		hdr.size = HDR_CRYPTO_SZ;
		is_hmac = 1;
	} else {
		hdr.size = HDR_NORMAL_SZ;
		is_hmac = 0;
	}
	/* Check to see if the record is in the cursor's buffer. */
	if ((ret = __log_c_incursor(logc, &nlsn, &hdr, &rp)) != 0) {
		/*
		fprintf(stderr, "__log_c_incursor error: %d for lsn=%d:%d \n",
		    ret, nlsn.file, nlsn.offset);
		 */
		goto err;
	}
	if (rp != NULL) {
		goto cksum;
	}

	/*
	 * Look to see if we're moving backward in the log with the last record
	 * coming from the disk -- it means the record can't be in the region's
	 * buffer.  Else, check the region's buffer.
	 *
	 * If the record isn't in the region's buffer, we're going to have to
	 * read the record from disk.  We want to make a point of not reading
	 * past the end of the logical log (after recovery, there may be data
	 * after the end of the logical log, not to mention the log file may
	 * have been pre-allocated).  So, zero out last_lsn, and initialize it
	 * inside __log_c_inregion -- if it's still zero when we check it in
	 * __log_c_ondisk, that's OK, it just means the logical end of the log
	 * isn't an issue for this request.
	 */
	ZERO_LSN(last_lsn);
	if (!F_ISSET(logc, DB_LOG_DISK) || log_compare(&nlsn, &logc->c_lsn) > 0) {
		F_CLR(logc, DB_LOG_DISK);

		if ((ret = __log_c_inregion(logc,
			    &nlsn, &rlock, &last_lsn, &hdr, &rp)) != 0) {
			/*
			fprintf(stderr,
		"log_c_inregion error: %d for lsn=%d:%d logc->c_lsn=%d:%d\n",
			    ret, nlsn.file, nlsn.offset, logc->c_lsn.file,
			    logc->c_lsn.offset);
			 */
			goto err;
		}
		if (rp != NULL) {
			goto cksum;
		}
	}

	/*
	 * We have to read from an on-disk file to retrieve the record.
	 * If we ever can't retrieve the record at offset 0, we're done,
	 * return EOF/DB_NOTFOUND.
	 *
	 * Discard the region lock if we're still holding it, the on-disk
	 * reading routines don't need it.
	 */
	if (rlock == L_ACQUIRED) {
		rlock = L_NONE;
		R_UNLOCK(dbenv, &dblp->reginfo);
	}
	if ((ret =
		__log_c_ondisk(logc, &nlsn, &last_lsn, flags, &hdr, &rp,
		    &eof)) != 0) {
		if (ret == EIO) {
			/* mismatching log record offsets more
			 * probably than disk ioe error
			fprintf(stderr, "log_c_ondisk error: ret=%d\n", ret);
			 */
			ret = DB_NOTFOUND;
		}
		goto err;
	}

	if (eof == 1) {
		/*
		 * Only DB_NEXT automatically moves to the next file, and
		 * it only happens once.
		 */
		if (flags != DB_NEXT || nlsn.offset == 0)
			return (DB_NOTFOUND);
		goto next_file;
	}
	F_SET(logc, DB_LOG_DISK);

cksum:	/*
	 * Discard the region lock if we're still holding it.  (The path to
	 * get here is that we acquired the lock because of the caller's
	 * flag argument, but we found the record in the cursor's buffer.
	 * Improbable, but it's easy to avoid.
	 */
	if (rlock == L_ACQUIRED) {
		rlock = L_NONE;
		R_UNLOCK(dbenv, &dblp->reginfo);
	}

	/*
	 * Checksum: there are two types of errors -- a configuration error
	 * or a checksum mismatch.  The former is always bad.  The latter is
	 * OK if we're searching for the end of the log, and very, very bad
	 * if we're reading random log records.
	 */
	db_cipher = dbenv->crypto_handle;
	if ((ret = __db_check_chksum(dbenv, db_cipher,
		    hdr.chksum, rp + hdr.size, hdr.len - hdr.size,
		    is_hmac)) != 0) {
		if (F_ISSET(logc, DB_LOG_SILENT_ERR)) {
			if (ret == 0 || ret == -1)
				ret = EIO;
		} else if (ret == -1) {
			__db_err(dbenv,
		    "DB_LOGC->get: log record LSN %lu/%lu: checksum mismatch",
			    (u_long) nlsn.file, (u_long) nlsn.offset);
			if (!F_ISSET(logc, DB_LOG_NO_PANIC)) {
				__db_err(dbenv,
		    "DB_LOGC->get: catastrophic recovery may be required");
				ret = __db_panic(dbenv, DB_RUNRECOVERY);
			} else
				ret = DB_NOTFOUND;
		}
		goto err;
	}

	/*
	 * If we got a 0-length record, that means we're in the midst of
	 * some bytes that got 0'd as the result of a vtruncate.  We're
	 * going to have to retry.
	 */
	if (hdr.len == 0) {
		switch (flags) {
		case DB_FIRST:
		case DB_NEXT:
			/* Zero'd records always indicate the end of a file. */
			goto next_file;

		case DB_LAST:
		case DB_PREV:
			/*
			 * We should never get here.  If we recover a log
			 * file with 0's at the end, we'll treat the 0'd
			 * headers as the end of log and ignore them.  If
			 * we're reading backwards from another file, then
			 * the first record in that new file should have its
			 * prev field set correctly.
			 */
			 __db_err(dbenv,
		"Encountered zero length records while traversing backwards");
			 DB_ASSERT(0);
		case DB_SET:
		default:
			/* Return the 0-length record. */
			break;
		}
	}

	/* Copy the record into the user's DBT. */
	if ((ret = __db_retcopy(dbenv, dbt, rp + hdr.size,
	    (u_int32_t)(hdr.len - hdr.size),
	    &logc->c_dbt.data, &logc->c_dbt.ulen)) != 0)
		goto err;

	if (CRYPTO_ON(dbenv)) {
		if ((ret = db_cipher->decrypt(dbenv, db_cipher->data,
		    hdr.iv, dbt->data, hdr.len - hdr.size)) != 0) {
			ret = EAGAIN;
			goto err;
		}
		/*
		 * Return the original log record size to the user,
		 * even though we've allocated more than that, possibly.
		 * The log record is decrypted in the user dbt, not in
		 * the buffer, so we must do this here after decryption,
		 * not adjust the len passed to the __db_retcopy call.
		 */
#if 0
		if (dbt->size != hdr.orig_size)
			printf(
	    "%s:%d %s  dbt->size:%u hdr.len:%u hdr.size:%u hdr.orig_size:%u\n",
			    __FILE__, __LINE__, __func__, dbt->size, hdr.len,
			    hdr.size, hdr.orig_size);
#endif
		dbt->size = hdr.orig_size;
	}

	/* Update the cursor and the returned LSN. */
	*alsn = nlsn;
	logc->c_lsn = nlsn;
	logc->c_len = hdr.len;
	logc->c_prev = hdr.prev;

err:	if (rlock == L_ACQUIRED)
		R_UNLOCK(dbenv, &dblp->reginfo);

	return (ret);
}

/*
 * __log_hdrswap --
 *	Swap the bytes in a log header from machines with different endianness.
 *
 * PUBLIC: void __log_hdrswap __P((HDR *, int));
 */
void
__log_hdrswap(hdr, is_hmac)
	HDR *hdr;
	int is_hmac;
{
	M_32_SWAP(hdr->prev);
	M_32_SWAP(hdr->len);
	if (is_hmac) {
		M_32_SWAP(hdr->orig_size);
		P_32_SWAP(hdr->chksum);
	} else {
		P_32_SWAP(hdr->chksum);
	}
}





/*
 * __log_c_incursor --
 *	Check to see if the requested record is in the cursor's buffer.
 */
static int
__log_c_incursor_int(logc, lsn, hdr, pp)
	DB_LOGC *logc;
	DB_LSN *lsn;
	HDR *hdr;
	u_int8_t **pp;
{
	u_int8_t *p;
	int eof;
	DB_ENV *dbenv;
	LOG *lp;

	dbenv = logc->dbenv;

	*pp = NULL;

	/*
	 * Test to see if the requested LSN could be part of the cursor's
	 * buffer.
	 *
	 * The record must be part of the same file as the cursor's buffer.
	 * The record must start at a byte offset equal to or greater than
	 * the cursor buffer.
	 * The record must not start at a byte offset after the cursor
	 * buffer's end.
	 */
	if (logc->bp_lsn.file != lsn->file)
		return (0);
	if (logc->bp_lsn.offset > lsn->offset)
		return (0);
	if (logc->bp_lsn.offset + logc->bp_rlen <= lsn->offset + hdr->size)
		return (0);

	/*
	 * Read the record's header and check if the record is entirely held
	 * in the buffer.  If the record is not entirely held, get it again.
	 * (The only advantage in having part of the record locally is that
	 * we might avoid a system call because we already have the HDR in
	 * memory.)
	 *
	 * If the header check fails for any reason, it must be because the
	 * LSN is bogus.  Fail hard.
	 */
	p = logc->bp + (lsn->offset - logc->bp_lsn.offset);
	memcpy(hdr, p, hdr->size);
	if (LOG_SWAPPED())
		__log_hdrswap(hdr, CRYPTO_ON(dbenv));
	if (__log_c_hdrchk(logc, lsn, hdr, &eof))
		return (DB_NOTFOUND);
	if (eof || logc->bp_lsn.offset + logc->bp_rlen < lsn->offset + hdr->len)
		return (0);

	*pp = p;		/* Success. */

	/* Get to stats. */
	lp = ((DB_LOG *)dbenv->lg_handle)->reginfo.primary;

	/* Increment stat for incursor get. */
	lp->stat.st_in_cursor_get++;

	return (0);
}

static int
__log_c_incursor(logc, lsn, hdr, pp)
	DB_LOGC *logc;
	DB_LSN *lsn;
	HDR *hdr;
	u_int8_t **pp;
{
	int rc;
	int64_t start;
	start = comdb2_time_epochus();
	rc = __log_c_incursor_int(logc, lsn, hdr, pp);
	logc->incursorus += (comdb2_time_epochus() - start);

	if (rc == 0 && pp != NULL) 
		logc->incursor_count++;

	return rc;
}


/*
 * segmented_copy --
 *  Utility function to accomodate wrapped log.
 *
 */
static int
segmented_copy(dblp, lp, to_p, buf_offset, cp_sz)
	DB_LOG *dblp;
	LOG *lp;
	u_int8_t *to_p;
	int32_t buf_offset;
	uint32_t cp_sz;
{
	u_int8_t *p;

	/* Sanity- this length should be less than the buffer size. */
	assert(buf_offset < lp->buffer_size);

	/* Sanity check number two. */
	assert(cp_sz <= lp->buffer_size);

	/* Set pointer to the correct offset. */
	p = dblp->bufp + buf_offset;

	/* Handle the wrap-case. */
	if (buf_offset + cp_sz > lp->buffer_size) {
		/* Copy from offset to the end of the buffer. */
		memcpy(to_p, p, lp->buffer_size - buf_offset);

		/* Update the copy amount. */
		cp_sz -= lp->buffer_size - buf_offset;

		/* Update the write-offset. */
		to_p += lp->buffer_size - buf_offset;

		/* Resume copy at the beginning of the buffer. */
		memcpy(to_p, dblp->bufp, cp_sz);

		/* Increment wrap-copy count. */
		lp->stat.st_wrap_copy++;
	}
	/* Normal case. */
	else {
		memcpy(to_p, p, cp_sz);
	}
	return 0;
}

/*
 * __log_c_inregion --
 *	Check to see if the requested record is in the region's buffer.
 */
static int
__log_c_inregion_int(logc, lsn, rlockp, last_lsn, hdr, pp)
	DB_LOGC *logc;
	DB_LSN *lsn, *last_lsn;
	RLOCK *rlockp;
	HDR *hdr;
	u_int8_t **pp;
{
	DB_ENV *dbenv;
	DB_LOG *dblp;
	DB_LSN *seg_lsn_array;
	DB_LSN *seg_start_lsn_array;
	DB_LSN f_lsn;
	DB_LSN a_lsn;
	LOG *lp;
	size_t len, nr;
	u_int32_t b_disk, b_region;
	u_int32_t w_off;
	u_int32_t st_off;
	u_int32_t cp_sz;
	u_int32_t b_remain;
	u_int32_t inmemlen = 0;
	int32_t buf_offset;
	u_int32_t curseg, oldseg;
	int ret, st, tot;
	u_int8_t *p;

	dbenv = logc->dbenv;
	dblp = dbenv->lg_handle;
	lp = ((DB_LOG *)logc->dbenv->lg_handle)->reginfo.primary;

	ret = 0;
	*pp = NULL;

	/* If we haven't yet acquired the log region lock, do so. */
	if (*rlockp == L_NONE) {
		*rlockp = L_ACQUIRED;
		st = comdb2_time_epochus();
		R_LOCK(dbenv, &dblp->reginfo);
		tot = comdb2_time_epochus() - st;
		logc->lockwaitus += (tot > 0 ? tot : 0);
	}

	/*
	 * The routines to read from disk must avoid reading past the logical
	 * end of the log, so pass that information back to it.
	 *
	 * Since they're reading directly from the disk, they must also avoid
	 * reading past the offset we've written out.  If the log was
	 * truncated, it's possible that there are zeroes or garbage on
	 * disk after this offset, and the logical end of the log can
	 * come later than this point if the log buffer isn't empty.
	 */
	*last_lsn = lp->lsn;

	/*
	 * last_lsn is filled here, but used in __log_c_ondisk as a safeguard:
	 * it's used to ensure that we don't attempt to read past the end of
	 * the part of the logfile which is ondisk.  I'm purposely not grabbing
	 * log_write_lk here because it shouldn't matter.
	 */
	if (last_lsn->offset > lp->w_off)
		last_lsn->offset = lp->w_off;

	/*
	 * Test to see if the requested LSN could be part of the region's
	 * buffer.
	 *
	 * During recovery, we read the log files getting the information to
	 * initialize the region.  In that case, the region's lsn field will
	 * not yet have been filled in, use only the disk.
	 *
	 * The record must not start at a byte offset after the region buffer's
	 * end, since that means the request is for a record after the end of
	 * the log.  Do this test even if the region's buffer is empty -- after
	 * recovery, the log files may continue past the declared end-of-log,
	 * and the disk reading routine will incorrectly attempt to read the
	 * remainder of the log.
	 *
	 * Otherwise, test to see if the region's buffer actually has what we
	 * want:
	 *
	 * The buffer must have some useful content.
	 * The record must be in the same file as the region's buffer and must
	 * start at a byte offset equal to or greater than the region's buffer.
	 */
	if (IS_ZERO_LSN(lp->lsn))
		return (0);
	if (lsn->file > lp->lsn.file ||
	    (lsn->file == lp->lsn.file && lsn->offset >= lp->lsn.offset))
		return (DB_NOTFOUND);
	if (__inmemory_buf_empty(lp))
		return (0);

	if (lp->num_segments > 1) {
		/* Need this to find the oldest in-memory lsn. */
		seg_lsn_array = R_ADDR(&dblp->reginfo, lp->segment_lsns_off);

		/* Need this for a compare. */
		seg_start_lsn_array = 
			R_ADDR(&dblp->reginfo, lp->segment_start_lsns_off);

		/* Get writer's current segment. */
		curseg = (lp->b_off / lp->segment_size);

		/* If we haven't wrapped yet, the oldest segment is 0. */
		if (IS_ZERO_LSN( seg_lsn_array[ lp->num_segments - 1 ])) {
			oldseg = 0;
		}
		/* Otherwise, the oldest segment is the current segment + 1. */
		else {
			/* Instrument: make sure segments are in
			 * increasing lsn order */
			oldseg = ( curseg + 1 ) % lp->num_segments;  

		}

		/* Copy actual lsn value. */
		a_lsn = seg_lsn_array[ oldseg ];

		/* Copy start lsn value. */
		f_lsn = seg_start_lsn_array[ oldseg ];
	} else {
		f_lsn = lp->f_lsn;
	}

	if (lsn->file < f_lsn.file || lsn->offset < f_lsn.offset)
		return (0);

	/*
	 * The current contents of the cursor's buffer will be useless for a
	 * future call -- trash it rather than try and make it look correct.
	 */
	ZERO_LSN(logc->bp_lsn);

	/*
	 * If the requested LSN is greater than the region buffer's first
	 * byte, we know the entire record is in the buffer on a good LSN.
	 *
	 * If we're given a bad LSN, the "entire" record might
	 * not be in our buffer in order to fail at the chksum.
	 * __log_c_hdrchk made sure our dest buffer fits, via
	 * bp_maxrec, but we also need to make sure we don't run off
	 * the end of this buffer, the src.
	 *
	 * If the header check fails for any reason, it must be because the
	 * LSN is bogus.  Fail hard.
	 */
	if (lsn->offset > f_lsn.offset) {
		if (lp->num_segments > 1) {
			/*
			 * For the segmented case, we have both the
			 * precise and the start lsn values.  Use the
			 * precise value to calculate the offset from
			 * the beginning of the oldest segment.
			 */
			buf_offset = lsn->offset - a_lsn.offset;

			/*
			 * Sanity: this length should be less than the
			 * buffer size.
			 */
			assert(buf_offset < lp->buffer_size);

			/* This is an offset from the oldest segment. */
			buf_offset += oldseg * lp->segment_size;

			/* Find the start in circular buffer. */
			buf_offset %= lp->buffer_size;

			/* Copy from the segmented buffer. */
			segmented_copy(dblp, lp, (u_int8_t *)hdr, buf_offset, hdr->size);

			/* Calculate the total size of the in-memory buffer. */
			if (buf_offset > lp->b_off) {
				b_remain =
				    (lp->buffer_size - buf_offset) + lp->b_off;
			} else {
				b_remain = lp->b_off - buf_offset;
			}
		} else {
			w_off = lp->w_off;
			p = dblp->bufp + (lsn->offset - w_off);
			memcpy(hdr, p, hdr->size);
		}
		if (LOG_SWAPPED())
			__log_hdrswap(hdr, CRYPTO_ON(dbenv));
		if (__log_c_hdrchk(logc, lsn, hdr, NULL))
			 return (DB_NOTFOUND);

		if (lp->num_segments > 1) {
			if (hdr->len > b_remain)
				return (DB_NOTFOUND);
		} else {
			if (lsn->offset + hdr->len > w_off + lp->buffer_size)
				return (DB_NOTFOUND);
		}
		if (logc->bp_size <= hdr->len) {
			len = ALIGN(hdr->len * 2, 128);
			if ((ret =
				__os_realloc(logc->dbenv, len, &logc->bp)) != 0)
				return (ret);
			logc->bp_size = (u_int32_t)len;
		}

		/* Segmented buffer case. */
		if (lp->num_segments > 1) {
			segmented_copy(dblp, lp, logc->bp, buf_offset,
			    hdr->len);
		} else {
			memcpy(logc->bp, p, hdr->len);
		}
		*pp = logc->bp;

		/* Increment a log-stats counter. */
		lp->stat.st_in_region_get++;

		return (0);
	}

	/*
	 * There's a partial record, that is, the requested record starts
	 * in a log file and finishes in the region buffer.  We have to
	 * find out how many bytes of the record are in the region buffer
	 * so we can copy them out into the cursor buffer.  First, check
	 * to see if the requested record is the only record in the region
	 * buffer, in which case we should copy the entire region buffer.
	 *
	 * Else, walk back through the region's buffer to find the first LSN
	 * after the record that crosses the buffer boundary -- we can detect
	 * that LSN, because its "prev" field will reference the record we
	 * want.  The bytes we need to copy from the region buffer are the
	 * bytes up to the record we find.  The bytes we'll need to allocate
	 * to hold the log record are the bytes between the two offsets.
	 */
	if (lp->num_segments > 1) {
		/* Number of on-disk bytes. */
		b_disk = a_lsn.offset - lsn->offset;

		/* Calculate the length of the in-memory buffer. */
		if (oldseg > curseg) {
			inmemlen =
			    lp->buffer_size - (oldseg * lp->segment_size);
			inmemlen += lp->b_off;
		} else {
			inmemlen = lp->b_off - (oldseg * lp->segment_size);
		}

		/*
		 * The 'current' lsn starts on disk, and ends in
		 * memory.  This is the simple case: just copy the
		 * entire thing.
		 */
		if (inmemlen <= lp->len) {
			b_region = inmemlen;
		} else {
			/* Find the header for the last lsn. */
			buf_offset = (lp->b_off - lp->len + lp->buffer_size) %
			    lp->buffer_size;

			/* Walk backwards. */
			while (1) {
				/* Copy from circular buffer. */
				segmented_copy(dblp, lp, (u_int8_t *)hdr, buf_offset,
				    hdr->size);

				/* Swap the header. */
				if (LOG_SWAPPED())
					__log_hdrswap(hdr, CRYPTO_ON(dbenv));

				/* Check if this is what we want. */
				if (hdr->prev == lsn->offset) {
					/* Start offset. */
					st_off = oldseg * lp->segment_size;

					/* If it's greater than the
					 * offset, get the
					 * wraped-len.
					 */
					if (st_off > buf_offset) {
						b_region =
						    lp->buffer_size - st_off +
						    buf_offset;
					} else {
						b_region = buf_offset - st_off;
					}
					break;
				}

				/* Calculate new offset. */
				buf_offset = hdr->prev - a_lsn.offset;

				/* Sanity: this length should be less than the buffer size. */
				assert(buf_offset < lp->buffer_size);

				/* This is an offset from the oldest segment. */
				buf_offset += oldseg * lp->segment_size;

				/* Find the start in circular buffer. */
				buf_offset %= lp->buffer_size;

				/* Increment in-memory traversal stat. */
				lp->stat.st_inmem_trav++;
			}
		}
	} else {
		b_disk = lp->w_off - lsn->offset;
		/*
		 * The last lsn is larger than the in-memory buffer.
		 * This has to be the record that we're asking for.
		 * Set b_region to the size of it's in-memory portion.
		 */
		if (lp->b_off <= lp->len)
			b_region = (u_int32_t)lp->b_off;
		/*
		 * Walk backwards until the last in-memory's 'prev' is
		 * equal to what we're looking for.
		 */
		else {
			for (p = dblp->bufp + (lp->b_off - lp->len);;) {
				memcpy(hdr, p, hdr->size);
				if (LOG_SWAPPED())
					__log_hdrswap(hdr, CRYPTO_ON(dbenv));
				if (hdr->prev == lsn->offset) {
					b_region = (u_int32_t)(p - dblp->bufp);
					break;
				}
				p = dblp->bufp + (hdr->prev - lp->w_off);
			}
		}
	}

	/*
	 * If we don't have enough room for the record, we have to allocate
	 * space.  We have to do it while holding the region lock, which is
	 * truly annoying, but there's no way around it.  This call is why
	 * we allocate cursor buffer space when allocating the cursor instead
	 * of waiting.
	 */
	if (logc->bp_size <= b_region + b_disk) {
		len = ALIGN((b_region + b_disk) * 2, 128);
		if ((ret = __os_realloc(logc->dbenv, len, &logc->bp)) != 0)
			return (ret);
		logc->bp_size = (u_int32_t)len;
	}

	if (lp->num_segments > 1) {
		/* Pointer into the cursor-buffer. */
		p = (logc->bp + logc->bp_size) - b_region;

		/* Offset to the oldest in-memory segment. */
		buf_offset = oldseg * lp->segment_size;

		/* Segmented-copy. */
		segmented_copy(dblp, lp, p, buf_offset, b_region);
	} else {
		/*
		 * Copy the in-memory region's bytes to the end of the
		 * cursor's buffer.
		 */
		p = (logc->bp + logc->bp_size) - b_region;
		memcpy(p, dblp->bufp, b_region);
	}

	/* Release the region lock. */
	if (*rlockp == L_ACQUIRED) {
		*rlockp = L_NONE;
		R_UNLOCK(dbenv, &dblp->reginfo);
	}

	/*
	 * Read the rest of the information from disk.  Neither short reads
	 * or EOF are acceptable, the bytes we want had better be there.
	 */
	if (b_disk != 0) {
		p -= b_disk;
		nr = b_disk;
		if ((ret = __log_c_io(
		    logc, lsn->file, lsn->offset, p, &nr, NULL)) != 0)
			return (ret);
		if (nr < b_disk)
			return (__log_c_shortread(logc, lsn, 0));
	}

	/* Copy the header information into the caller's structure. */
	memcpy(hdr, p, hdr->size);
	if (LOG_SWAPPED())
		__log_hdrswap(hdr, CRYPTO_ON(dbenv));

	*pp = p;

	/* Increment stats for partial get. */
	lp->stat.st_part_region_get++;

	return (0);
}


static int
__log_c_inregion(logc, lsn, rlockp, last_lsn, hdr, pp)
	DB_LOGC *logc;
	DB_LSN *lsn, *last_lsn;
	RLOCK *rlockp;
	HDR *hdr;
	u_int8_t **pp;
{
	int rc;
	int64_t start;
	start = comdb2_time_epochus();
	rc = __log_c_inregion_int(logc, lsn, rlockp, last_lsn, hdr, pp);
	logc->inregionus += (comdb2_time_epochus() - start);
	if (rc == 0 && pp != NULL) {
		logc->inregion_count++;
	}
	return rc;
}



/*
 * __log_c_ondisk --
 *	Read a record off disk.
 */
static int
__log_c_ondisk_int(logc, lsn, last_lsn, flags, hdr, pp, eofp)
	DB_LOGC *logc;
	DB_LSN *lsn, *last_lsn;
	int flags, *eofp;
	HDR *hdr;
	u_int8_t **pp;
{
	DB_ENV *dbenv;
	LOG *lp;
	size_t len, nr;
	u_int32_t offset;
	int ret;

	dbenv = logc->dbenv;
	*eofp = 0;

	nr = hdr->size;
	if ((ret =
	    __log_c_io(logc, lsn->file, lsn->offset, hdr, &nr, eofp)) != 0)
		return (ret);
	if (*eofp)
		return (0);

	if (LOG_SWAPPED())
		__log_hdrswap(hdr, CRYPTO_ON(dbenv));

	/* If we read 0 bytes, assume we've hit EOF. */
	if (nr == 0) {
		*eofp = 1;
		return (0);
	}

	/* Check the HDR. */
	if ((ret = __log_c_hdrchk(logc, lsn, hdr, eofp)) != 0)
		return (ret);

	if (*eofp)
		return (0);

	/* Otherwise, we should have gotten the bytes we wanted. */
	if (nr < hdr->size)
		return (__log_c_shortread(logc, lsn, 1));

	/*
	 * Regardless of how we return, the previous contents of the cursor's
	 * buffer are useless -- trash it.
	 */
	ZERO_LSN(logc->bp_lsn);

	/*
	 * Otherwise, we now (finally!) know how big the record is.  (Maybe
	 * we should have just stuck the length of the record into the LSN!?)
	 * Make sure we have enough space.
	 */
	if (logc->bp_size <= hdr->len) {
		len = ALIGN(hdr->len * 2, 128);
		if ((ret = __os_realloc(dbenv, len, &logc->bp)) != 0)
			return (ret);
		logc->bp_size = (u_int32_t)len;
	}

	/*
	 * If we're moving forward in the log file, read this record in at the
	 * beginning of the buffer.  Otherwise, read this record in at the end
	 * of the buffer, making sure we don't try and read before the start
	 * of the file.  (We prefer positioning at the end because transaction
	 * aborts use DB_SET to move backward through the log and we might get
	 * lucky.)
	 *
	 * Read a buffer's worth, without reading past the logical EOF.  The
	 * last_lsn may be a zero LSN, but that's OK, the test works anyway.
	 */

	/*
	 * If this is a first or next, make this lsn the first in the log-
	 * cursor's cache.
	 */
	if (flags == DB_FIRST || flags == DB_NEXT)
		offset = lsn->offset;

	/*
	 * If the entire log including the requested lsn will fit in my cache,
	 * copy it.
	 */
	else if (lsn->offset + hdr->len < logc->bp_size)
		offset = 0;

	/*
	 * Set the offset such that the requested lsn will be the last entry in
	 * the cursor-cache.
	 */
	else
		offset = (lsn->offset + hdr->len) - logc->bp_size;

	nr = logc->bp_size;

	/*
	 * The reason why last_lsn is passed along: we do not want to read
	 * past it.
	 */
	if (lsn->file == last_lsn->file && offset + nr >= last_lsn->offset)
		nr = last_lsn->offset - offset;

	if ((ret =
	    __log_c_io(logc, lsn->file, offset, logc->bp, &nr, eofp)) != 0)
		return (ret);

	/*
	 * We should have at least gotten the bytes up-to-and-including the
	 * record we're reading.
	 */
	if (nr < (lsn->offset + hdr->len) - offset)
		return (__log_c_shortread(logc, lsn, 1));

	/* Set up the return information. */
	logc->bp_rlen = (u_int32_t)nr;
	logc->bp_lsn.file = lsn->file;
	logc->bp_lsn.offset = offset;

	*pp = logc->bp + (lsn->offset - offset);

	/* Get to stats. */
	lp = ((DB_LOG *)dbenv->lg_handle)->reginfo.primary;

	/* Increment stats for ondisk. */
	lp->stat.st_ondisk_get++;

	return (0);
}


static int
__log_c_ondisk(logc, lsn, last_lsn, flags, hdr, pp, eofp)
	DB_LOGC *logc;
	DB_LSN *lsn, *last_lsn;
	int flags, *eofp;
	HDR *hdr;
	u_int8_t **pp;
{
	int rc;
	int64_t start;
	start = comdb2_time_epochus();
	rc = __log_c_ondisk_int(logc, lsn, last_lsn, flags, hdr, pp, eofp);
	logc->ondiskus += (comdb2_time_epochus() - start);
	if (rc == 0 && pp != NULL) {
		logc->ondisk_count++;
	}
	return rc;
}


/*
 * __log_c_hdrchk --
 *
 * Check for corrupted HDRs before we use them to allocate memory or find
 * records.
 *
 * If the log files were pre-allocated, a zero-filled HDR structure is the
 * logical file end.  However, we can see buffers filled with 0's during
 * recovery, too (because multiple log buffers were written asynchronously,
 * and one made it to disk before a different one that logically precedes
 * it in the log file.
 *
 * XXX
 * I think there's a potential pre-allocation recovery flaw here -- if we
 * fail to write a buffer at the end of a log file (by scheduling its
 * write asynchronously, and it never making it to disk), then succeed in
 * writing a log file block to a subsequent log file, I don't think we will
 * detect that the buffer of 0's should have marked the end of the log files
 * during recovery.  I think we may need to always write some garbage after
 * each block write if we pre-allocate log files.  (At the moment, we do not
 * pre-allocate, so this isn't currently an issue.)
 *
 * Check for impossibly large records.  The malloc should fail later, but we
 * have customers that run mallocs that treat all allocation failures as fatal
 * errors.
 *
 * Note that none of this is necessarily something awful happening.  We let
 * the application hand us any LSN they want, and it could be a pointer into
 * the middle of a log record, there's no way to tell.
 */
static int
__log_c_hdrchk(logc, lsn, hdr, eofp)
	DB_LOGC *logc;
	DB_LSN *lsn;
	HDR *hdr;
	int *eofp;
{
	DB_ENV *dbenv;
	int ret;

	dbenv = logc->dbenv;

	/*
	 * Check EOF before we do any other processing.
	 */
	if (eofp != NULL) {
		if (hdr->prev == 0 && hdr->chksum[0] == 0 && hdr->len == 0) {
			*eofp = 1;
			return (0);
		}
		*eofp = 0;
	}

	/*
	 * Sanity check the log record's size.
	 * We must check it after "virtual" EOF above.
	 */
	if (hdr->len <= hdr->size)
		goto err;

	/*
	 * If the cursor's max-record value isn't yet set, it means we aren't
	 * reading these records from a log file and no check is necessary.
	 */
	if (logc->bp_maxrec != 0 && hdr->len > logc->bp_maxrec) {
		/*
		 * If we fail the check, there's the pathological case that
		 * we're reading the last file, it's growing, and our initial
		 * check information was wrong.  Get it again, to be sure.
		 */
		if ((ret = __log_c_set_maxrec(logc, NULL)) != 0) {
			__db_err(dbenv, "DB_LOGC->get: %s", db_strerror(ret));
			return (ret);
		}
		if (logc->bp_maxrec != 0 && hdr->len > logc->bp_maxrec)
			goto err;
	}
	return (0);

err:	if (!F_ISSET(logc, DB_LOG_SILENT_ERR))
		__db_err(dbenv,
	    "DB_LOGC->get: LSN %lu/%lu: hdr=%p invalid log record header",
		    (u_long)lsn->file, (u_long)lsn->offset, hdr);
	return (EIO);
}

/*
 * __log_c_io --
 *	Read records from a log file.
 */
static int
__log_c_io(logc, fnum, offset, p, nrp, eofp)
	DB_LOGC *logc;
	u_int32_t fnum, offset;
	void *p;
	size_t *nrp;
	int *eofp;
{
	DB_ENV *dbenv;
	DB_LOG *dblp;
	int ret;
	char *np;

	dbenv = logc->dbenv;
	dblp = dbenv->lg_handle;

	/*
	 * If we've switched files, discard the current file handle and acquire
	 * a new one.
	 */
	if (logc->c_fhp != NULL && logc->bp_lsn.file != fnum) {
		ret = __os_closehandle(dbenv, logc->c_fhp);
		logc->c_fhp = NULL;
		if (ret != 0)
			return (ret);
	}
	if (logc->c_fhp == NULL) {
		if ((ret = __log_name(dblp, fnum,
		    &np, &logc->c_fhp, DB_OSO_RDONLY | DB_OSO_SEQ)) != 0) {
			/*
			 * If we're allowed to return EOF, assume that's the
			 * problem, set the EOF status flag and return 0.
			 */
			if (eofp != NULL) {
				*eofp = 1;
				ret = 0;
			} else if (!F_ISSET(logc, DB_LOG_SILENT_ERR))
				__db_err(dbenv, "DB_LOGC->get: %s: %s",
				    np, db_strerror(ret));
			__os_free(dbenv, np);
			return (ret);
		}

		if ((ret = __log_c_set_maxrec(logc, np)) != 0) {
			__db_err(dbenv,
			    "DB_LOGC->get: %s: %s", np, db_strerror(ret));
			__os_free(dbenv, np);
			return (ret);
		}
		__os_free(dbenv, np);
	}

	/* Seek to the record's offset. */
	if ((ret = __os_seek(dbenv,
	    logc->c_fhp, 0, 0, offset, 0, DB_OS_SEEK_SET)) != 0) {
		if (!F_ISSET(logc, DB_LOG_SILENT_ERR))
			__db_err(dbenv,
			    "DB_LOGC->get: LSN: %lu/%lu: seek: %s",
			    (u_long)fnum, (u_long)offset, db_strerror(ret));
		return (ret);
	}
	/* Read the data. */
	if ((ret = __os_read(dbenv, logc->c_fhp, p, *nrp, nrp)) != 0) {
		if (!F_ISSET(logc, DB_LOG_SILENT_ERR))
			__db_err(dbenv,
			    "DB_LOGC->get: LSN: %lu/%lu: read: %s",
			    (u_long)fnum, (u_long)offset, db_strerror(ret));
		return (ret);
	}
	return (0);
}

/*
 * __log_c_shortread --
 *	Read was short -- return a consistent error message and error.
 */
static int
__log_c_shortread(logc, lsn, check_silent)
	DB_LOGC *logc;
	DB_LSN *lsn;
	int check_silent;
{
	if (!check_silent || !F_ISSET(logc, DB_LOG_SILENT_ERR))
		__db_err(logc->dbenv, "DB_LOGC->get: LSN: %lu/%lu: short read",
		    (u_long)lsn->file, (u_long)lsn->offset);
	return (EIO);
}

/*
 * __log_c_set_maxrec --
 *	Bound the maximum log record size in a log file.
 */
static int
__log_c_set_maxrec(logc, np)
	DB_LOGC *logc;
	char *np;
{
	DB_ENV *dbenv;
	DB_LOG *dblp;
	LOG *lp;
	u_int32_t mbytes, bytes;
	int ret;

	dbenv = logc->dbenv;
	dblp = dbenv->lg_handle;

	/*
	 * We don't want to try and allocate huge chunks of memory because
	 * applications with error-checking malloc's often consider that a
	 * hard failure.  If we're about to look at a corrupted record with
	 * a bizarre size, we need to know before trying to allocate space
	 * to hold it.  We could read the persistent data at the beginning
	 * of the file but that's hard -- we may have to decrypt it, checksum
	 * it and so on.  Stat the file instead.
	 */
	if (logc->c_fhp != NULL) {
		if ((ret = __os_ioinfo(dbenv, np, logc->c_fhp,
		    &mbytes, &bytes, NULL)) != 0)
			return (ret);
		if (logc->bp_maxrec < (mbytes * MEGABYTE + bytes))
			logc->bp_maxrec = mbytes * MEGABYTE + bytes;
	}

	/*
	 * If reading from the log file currently being written, we could get
	 * an incorrect size, that is, if the cursor was opened on the file
	 * when it had only a few hundred bytes, and then the cursor used to
	 * move forward in the file, after more log records were written, the
	 * original stat value would be wrong.  Use the maximum of the current
	 * log file size and the size of the buffer -- that should represent
	 * the max of any log record currently in the file.
	 *
	 * The log buffer size is set when the environment is opened and never
	 * changed, we don't need a lock on it.
	 */
	lp = dblp->reginfo.primary;
	if (logc->bp_maxrec < lp->buffer_size)
		logc->bp_maxrec = lp->buffer_size;

	return (0);
}
