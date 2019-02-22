/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: db_err.c,v 11.100 2003/10/07 18:55:38 mjc Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <stdlib.h>
#include <string.h>
#include <unistd.h>				/* Declare STDERR_FILENO. */
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/db_shash.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/txn.h"
#include "logmsg.h"

/*
 * __db_fchk --
 *	General flags checking routine.
 *
 * PUBLIC: int __db_fchk __P((DB_ENV *, const char *, u_int32_t, u_int32_t));
 */
int
__db_fchk(dbenv, name, flags, ok_flags)
	DB_ENV *dbenv;
	const char *name;
	u_int32_t flags, ok_flags;
{
	return (LF_ISSET(~ok_flags) ? __db_ferr(dbenv, name, 0) : 0);
}

/*
 * __db_fcchk --
 *	General combination flags checking routine.
 *
 * PUBLIC: int __db_fcchk
 * PUBLIC:    __P((DB_ENV *, const char *, u_int32_t, u_int32_t, u_int32_t));
 */
int
__db_fcchk(dbenv, name, flags, flag1, flag2)
	DB_ENV *dbenv;
	const char *name;
	u_int32_t flags, flag1, flag2;
{
	return (LF_ISSET(flag1) &&
	    LF_ISSET(flag2) ? __db_ferr(dbenv, name, 1) : 0);
}

/*
 * __db_ferr --
 *	Common flag errors.
 *
 * PUBLIC: int __db_ferr __P((const DB_ENV *, const char *, int));
 */
int
__db_ferr(dbenv, name, iscombo)
	const DB_ENV *dbenv;
	const char *name;
	int iscombo;
{
	__db_err(dbenv, "illegal flag %sspecified to %s",
	    iscombo ? "combination " : "", name);
	return (EINVAL);
}

/*
 * __db_fnl --
 *	Common flag-needs-locking message.
 *
 * PUBLIC: int __db_fnl __P((const DB_ENV *, const char *));
 */
int
__db_fnl(dbenv, name)
	const DB_ENV *dbenv;
	const char *name;
{
	__db_err(dbenv,
	    "%s: the DB_DIRTY_READ and DB_RMW flags require locking", name);
	return (EINVAL);
}

/*
 * __db_pgerr --
 *	Error when unable to retrieve a specified page.
 *
 * PUBLIC: int __db_pgerr __P((DB *, db_pgno_t, int));
 */
int
__db_pgerr(dbp, pgno, errval)
	DB *dbp;
	db_pgno_t pgno;
	int errval;
{
	/*
	 * Three things are certain:
	 * Death, taxes, and lost data.
	 * Guess which has occurred.
	 */
	__db_err(dbp->dbenv,
	    "unable to create/retrieve page %lu", (u_long)pgno);
	return (__db_panic(dbp->dbenv, errval));
}

/*
 * __db_pgfmt --
 *	Error when a page has the wrong format.
 *
 * PUBLIC: int __db_pgfmt __P((DB_ENV *, db_pgno_t));
 */
int
__db_pgfmt(dbenv, pgno)
	DB_ENV *dbenv;
	db_pgno_t pgno;
{
	__db_err(dbenv, "page %lu: illegal page type or format", (u_long)pgno);
	return (__db_panic(dbenv, EINVAL));
}

#ifdef DIAGNOSTIC
/*
 * __db_assert --
 *	Error when an assertion fails.  Only checked if #DIAGNOSTIC defined.
 *
 * PUBLIC: #ifdef DIAGNOSTIC
 * PUBLIC: void __db_assert __P((const char *, const char *, int));
 * PUBLIC: #endif
 */
void
__db_assert(failedexpr, file, line)
	const char *failedexpr, *file;
	int line;
{
	(void)logmsg(LOGMSG_FATAL, 
        "__db_assert: \"%s\" failed: file \"%s\", line %d\n",
	    failedexpr, file, line);
	(void)fflush(stderr);

	/* We want a stack trace of how this could possibly happen. */
	abort();

	/* NOTREACHED */
}
#endif

/*
 * __db_panic_msg --
 *	Just report that someone else paniced.
 *
 * PUBLIC: int __db_panic_msg __P((DB_ENV *));
 */
int
__db_panic_msg(dbenv)
	DB_ENV *dbenv;
{
	__db_err(dbenv, "PANIC: fatal region error detected; run recovery");

	if (dbenv->db_paniccall != NULL)
		dbenv->db_paniccall(dbenv, DB_RUNRECOVERY);

	return (DB_RUNRECOVERY);
}

/*
 * __db_panic --
 *	Lock out the tree due to unrecoverable error.
 *
 * PUBLIC: int __db_panic __P((DB_ENV *, int));
 */
int
__db_panic(dbenv, errval)
	DB_ENV *dbenv;
	int errval;
{
    dbenv->log_flush(dbenv, NULL);
	if (dbenv != NULL) {
		PANIC_SET(dbenv, 1);

		__db_err(dbenv, "PANIC: %s", db_strerror(errval));

		if (dbenv->db_paniccall != NULL)
			dbenv->db_paniccall(dbenv, errval);
	}

#if defined(DIAGNOSTIC) && !defined(CONFIG_TEST)
	/*
	 * We want a stack trace of how this could possibly happen.
	 *
	 * Don't drop core if it's the test suite -- it's reasonable for the
	 * test suite to check to make sure that DB_RUNRECOVERY is returned
	 * under certain conditions.
	 */
	abort();
#endif

	/*
	 * Chaos reigns within.
	 * Reflect, repent, and reboot.
	 * Order shall return.
	 */
	return (DB_RUNRECOVERY);
}

/*
 * db_strerror --
 *	ANSI C strerror(3) for DB.
 *
 * EXTERN: char *db_strerror __P((int));
 */
char *
db_strerror(error)
	int error;
{
	char *p;

	if (error == 0)
		return ("Successful return: 0");
	if (error > 0) {
		if ((p = strerror(error)) != NULL)
			return (p);
		goto unknown_err;
	}

	/*
	 * !!!
	 * The Tcl API requires that some of these return strings be compared
	 * against strings stored in application scripts.  So, any of these
	 * errors that do not invariably result in a Tcl exception may not be
	 * altered.
	 */
	switch (error) {
	case DB_DONOTINDEX:
		return ("DB_DONOTINDEX: Secondary index callback returns null");
	case DB_FILEOPEN:
		return ("DB_FILEOPEN: Rename or remove while file is open.");
	case DB_KEYEMPTY:
		return ("DB_KEYEMPTY: Non-existent key/data pair");
	case DB_KEYEXIST:
		return ("DB_KEYEXIST: Key/data pair already exists");
	case DB_LOCK_DEADLOCK:
		return
		    ("DB_LOCK_DEADLOCK: Locker killed to resolve a deadlock");
	case DB_LOCK_NOTGRANTED:
		return ("DB_LOCK_NOTGRANTED: Lock not granted");
	case DB_NOSERVER:
		return ("DB_NOSERVER: Fatal error, no RPC server");
	case DB_NOSERVER_HOME:
		return ("DB_NOSERVER_HOME: Home unrecognized at server");
	case DB_NOSERVER_ID:
		return ("DB_NOSERVER_ID: Identifier unrecognized at server");
	case DB_NOTFOUND:
		return ("DB_NOTFOUND: No matching key/data pair found");
	case DB_OLD_VERSION:
		return ("DB_OLDVERSION: Database requires a version upgrade");
	case DB_PAGE_NOTFOUND:
		return ("DB_PAGE_NOTFOUND: Requested page not found");
	case DB_REP_DUPMASTER:
		return ("DB_REP_DUPMASTER: A second master site appeared");
	case DB_REP_HANDLE_DEAD:
		return ("DB_REP_HANDLE_DEAD: Handle is no longer valid.");
	case DB_REP_HOLDELECTION:
		return ("DB_REP_HOLDELECTION: Need to hold an election");
	case DB_REP_ISPERM:
		return ("DB_REP_ISPERM: Permanent record written");
	case DB_REP_NEWMASTER:
		return ("DB_REP_NEWMASTER: A new master has declared itself");
	case DB_REP_NEWSITE:
		return ("DB_REP_NEWSITE: A new site has entered the system");
	case DB_REP_NOTPERM:
		return ("DB_REP_NOTPERM: Permanent log record not written.");
	case DB_REP_OUTDATED:
		return
		    ("DB_REP_OUTDATED: Insufficient logs on master to recover");
	case DB_REP_UNAVAIL:
		return ("DB_REP_UNAVAIL: Unable to elect a master");
	case DB_RUNRECOVERY:
		return ("DB_RUNRECOVERY: Fatal error, run database recovery");
	case DB_SECONDARY_BAD:
		return
	    ("DB_SECONDARY_BAD: Secondary index inconsistent with primary");
	case DB_VERIFY_BAD:
		return ("DB_VERIFY_BAD: Database verification failed");
    case DB_CUR_STALE:
		return ("DB_CUR_STALE: Something has changed since serialization");
	default:
		break;
	}

unknown_err: {
		/*
		 * !!!
		 * Room for a 64-bit number + slop.  This buffer is only used
		 * if we're given an unknown error, which should never happen.
		 * Note, however, we're no longer thread-safe if it does.
		 */
		static char ebuf[40];

		(void)snprintf(ebuf, sizeof(ebuf), "Unknown error: %d", error);
		return (ebuf);
	}
}

/*
 * __db_err --
 *	Standard DB error routine.  The same as errx, except we don't write
 *	to stderr if no output mechanism was specified.
 *
 * PUBLIC: void __db_err __P((const DB_ENV *, const char *, ...))
 * PUBLIC:    __attribute__ ((__format__ (__printf__, 2, 3)));
 */
void
#ifdef STDC_HEADERS
__db_err(const DB_ENV *dbenv, const char *fmt, ...)
#else
__db_err(dbenv, fmt, va_alist)
	const DB_ENV *dbenv;
	const char *fmt;
	va_dcl
#endif
{
	DB_REAL_ERR(dbenv, 0, 0, 0, fmt);
}

#define	OVERFLOW_ERROR	"internal buffer overflow, process aborted\n"
#ifndef	STDERR_FILENO
#define	STDERR_FILENO	2
#endif

/*
 * __db_errcall --
 *	Do the error message work for callback functions.
 *
 * PUBLIC: void __db_errcall
 * PUBLIC:          __P((const DB_ENV *, int, int, const char *, va_list));
 */
void
__db_errcall(dbenv, error, error_set, fmt, ap)
	const DB_ENV *dbenv;
	int error, error_set;
	const char *fmt;
	va_list ap;
{
	char *p;
	char errbuf[2048];	/* !!!: END OF THE STACK DON'T TRUST SPRINTF. */

	p = errbuf;
	if (fmt != NULL)
		p += vsnprintf(errbuf, sizeof(errbuf), fmt, ap);
	if (error_set)
		p += snprintf(p,
		    sizeof(errbuf) - (size_t)(p - errbuf), ": %s",
		    db_strerror(error));
#ifndef HAVE_VSNPRINTF
	/*
	 * !!!
	 * We're potentially manipulating strings handed us by the application,
	 * and on systems without a real snprintf() the sprintf() calls could
	 * have overflowed the buffer.  We can't do anything about it now, but
	 * we don't want to return control to the application, we might have
	 * overwritten the stack with a Trojan horse.  We're not trying to do
	 * anything recoverable here because systems without snprintf support
	 * are pretty rare anymore.
	 */
	if ((size_t)(p - errbuf) > sizeof(errbuf)) {
		write(
		    STDERR_FILENO, OVERFLOW_ERROR, sizeof(OVERFLOW_ERROR) - 1);
		abort();
		/* NOTREACHED */
	}
#endif

	dbenv->db_errcall(dbenv->db_errpfx, errbuf);
}

/*
 * __db_errfile --
 *	Do the error message work for FILE *s.
 *
 * PUBLIC: void __db_errfile
 * PUBLIC:          __P((const DB_ENV *, int, int, const char *, va_list));
 */
void
__db_errfile(dbenv, error, error_set, fmt, ap)
	const DB_ENV *dbenv;
	int error, error_set;
	const char *fmt;
	va_list ap;
{
	FILE *fp;

	fp = dbenv == NULL ||
	    dbenv->db_errfile == NULL ? stderr : dbenv->db_errfile;

	if (dbenv != NULL && dbenv->db_errpfx != NULL)
		(void)logmsgf(LOGMSG_USER, fp, "%s: ", dbenv->db_errpfx);
	if (fmt != NULL) {
		(void)logmsgvf(LOGMSG_USER, fp, fmt, ap);
		if (error_set)
			(void)logmsgf(LOGMSG_USER, fp, ": ");
	}
	if (error_set)
		(void)logmsgf(LOGMSG_USER, fp, "%s", db_strerror(error));
	(void)logmsgf(LOGMSG_USER, fp, "\n");
	(void)fflush(fp);
}

static char buf[256] = {0};

// PUBLIC: void __db_errbuf __P((const DB_ENV *, int, int, const char *, va_list));
void
__db_errbuf(dbenv, error, error_set, fmt, ap)
	const DB_ENV *dbenv;
	int error, error_set;
	const char *fmt;
	va_list ap;
{
    int off = 0;
    if(fmt != NULL) {
        off += vsnprintf(&buf[off], sizeof(buf) - off, fmt, ap);
        if (error_set)
            off += snprintf(&buf[off], sizeof(buf) - off, ": ");
    }
    if (error_set)
        off += snprintf(&buf[off], sizeof(buf) - off, "%s", db_strerror(error));

    snprintf(&buf[off], sizeof(buf) - off, "\n");
}

/*
 * __db_logmsg --
 *	Write information into the DB log.
 *
 * PUBLIC: void __db_logmsg __P((const DB_ENV *,
 * PUBLIC:     DB_TXN *, const char *, u_int32_t, const char *, ...))
 * PUBLIC:    __attribute__ ((__format__ (__printf__, 5, 6)));
 */
void
#ifdef STDC_HEADERS
__db_logmsg(const DB_ENV *dbenv,
    DB_TXN *txnid, const char *opname, u_int32_t flags, const char *fmt, ...)
#else
__db_logmsg(dbenv, txnid, opname, flags, fmt, va_alist)
	const DB_ENV *dbenv;
	DB_TXN *txnid;
	const char *opname, *fmt;
	u_int32_t flags;
	va_dcl
#endif
{
	DBT opdbt, msgdbt;
	DB_LSN lsn;
	va_list ap;
	char __logbuf[2048];	/* !!!: END OF THE STACK DON'T TRUST SPRINTF. */

	if (!LOGGING_ON(dbenv))
		return;

#ifdef STDC_HEADERS
	va_start(ap, fmt);
#else
	va_start(ap);
#endif
	memset(&opdbt, 0, sizeof(opdbt));
	opdbt.data = (void *)opname;
	opdbt.size = (u_int32_t)(strlen(opname) + 1);

	memset(&msgdbt, 0, sizeof(msgdbt));
	msgdbt.data = __logbuf;
	msgdbt.size = (u_int32_t)vsnprintf(__logbuf, sizeof(__logbuf), fmt, ap);

#ifndef HAVE_VSNPRINTF
	/*
	 * !!!
	 * We're potentially manipulating strings handed us by the application,
	 * and on systems without a real snprintf() the sprintf() calls could
	 * have overflowed the buffer.  We can't do anything about it now, but
	 * we don't want to return control to the application, we might have
	 * overwritten the stack with a Trojan horse.  We're not trying to do
	 * anything recoverable here because systems without snprintf support
	 * are pretty rare anymore.
	 */
	if (msgdbt.size > sizeof(__logbuf)) {
		write(
		    STDERR_FILENO, OVERFLOW_ERROR, sizeof(OVERFLOW_ERROR) - 1);
		abort();
		/* NOTREACHED */
	}
#endif

	/*
	 * XXX
	 * Explicitly discard the const.  Otherwise, we have to const DB_ENV
	 * references throughout the logging subsystem.
	 */
	(void)__db_debug_log(
	    (DB_ENV *)dbenv, txnid, &lsn, flags, &opdbt, -1, &msgdbt, NULL, 0);

	va_end(ap);
}

/*
 * __db_unknown_flag -- report internal error
 *
 * PUBLIC: int __db_unknown_flag __P((DB_ENV *, char *, u_int32_t));
 */
int
__db_unknown_flag(dbenv, routine, flag)
	DB_ENV *dbenv;
	char *routine;
	u_int32_t flag;
{
	__db_err(dbenv, "%s: Unknown flag: 0x%x", routine, (u_int)flag);
	DB_ASSERT(0);
	return (EINVAL);
}

/*
 * __db_unknown_type -- report internal error
 *
 * PUBLIC: int __db_unknown_type __P((DB_ENV *, char *, DBTYPE));
 */
int
__db_unknown_type(dbenv, routine, type)
	DB_ENV *dbenv;
	char *routine;
	DBTYPE type;
{
	__db_err(dbenv, "%s: Unknown db type: 0x%x", routine, (u_int)type);
	DB_ASSERT(0);
	return (EINVAL);
}

/*
 * __db_check_txn --
 *	Check for common transaction errors.
 *
 * PUBLIC: int __db_check_txn __P((DB *, DB_TXN *, u_int32_t, int));
 */
int
__db_check_txn(dbp, txn, assoc_lid, read_op)
	DB *dbp;
	DB_TXN *txn;
	u_int32_t assoc_lid;
	int read_op;
{
	DB_ENV *dbenv;

	dbenv = dbp->dbenv;

	/*
	 * If we are in recovery or aborting a transaction, then we
	 * don't need to enforce the rules about dbp's not allowing
	 * transactional operations in non-transactional dbps and
	 * vica-versa.  This happens all the time as the dbp during
	 * an abort may be transactional, but we undo operations
	 * outside a transaction since we're aborting.
	 */
	if (IS_RECOVERING(dbenv) || F_ISSET(dbp, DB_AM_RECOVER))
		return (0);

	/*
	 * Check for common transaction errors:
	 *	Failure to pass a transaction handle to a DB operation
	 *	Failure to configure the DB handle in a proper environment
	 *	Operation on a handle whose open commit hasn't completed.
	 *
	 * Read operations don't require a txn even if we've used one before
	 * with this handle, although if they do have a txn, we'd better be
	 * prepared for it.
	 */
	if (txn == NULL) {
		if (!read_op && F_ISSET(dbp, DB_AM_TXN)) {
			__db_err(dbenv,
    "DB handle previously used in transaction, missing transaction handle");
            abort();
			return (EINVAL);
		}

		if (dbp->cur_lid >= TXN_MINIMUM)
			goto open_err;
	} else {
		if (dbp->cur_lid >= TXN_MINIMUM && dbp->cur_lid != txn->txnid)
			goto open_err;

		if (!TXN_ON(dbenv))
			 return (__db_not_txn_env(dbenv));

		if (!F_ISSET(dbp, DB_AM_TXN)) {
			__db_err(dbenv,
    "Transaction specified for a DB handle opened outside a transaction");
			return (EINVAL);
		}
	}

	/*
	 * If dbp->associate_lid is not DB_LOCK_INVALIDID, that means we're in
	 * the middle of a DB->associate with DB_CREATE (i.e., a secondary index
	 * creation).
	 *
	 * In addition to the usual transaction rules, we need to lock out
	 * non-transactional updates that aren't part of the associate (and
	 * thus are using some other locker ID).
	 *
	 * Transactional updates should simply block;  from the time we
	 * decide to build the secondary until commit, we'll hold a write
	 * lock on all of its pages, so it should be safe to attempt to update
	 * the secondary in another transaction (presumably by updating the
	 * primary).
	 */
	if (!read_op && dbp->associate_lid != DB_LOCK_INVALIDID &&
	    txn != NULL && dbp->associate_lid != assoc_lid) {
		__db_err(dbenv,
	    "Operation forbidden while secondary index is being created");
		return (EINVAL);
	}

	return (0);
open_err:
	__db_err(dbenv,
	    "Transaction that opened the DB handle is still active");
	return (EINVAL);
}

/*
 * __db_not_txn_env --
 *	DB handle must be in an environment that supports transactions.
 *
 * PUBLIC: int __db_not_txn_env __P((DB_ENV *));
 */
int
__db_not_txn_env(dbenv)
	DB_ENV *dbenv;
{
	__db_err(dbenv, "DB environment not configured for transactions");
	return (EINVAL);
}

/*
 * __db_rec_toobig --
 *	Fixed record length exceeded error message.
 *
 * PUBLIC: int __db_rec_toobig __P((DB_ENV *, u_int32_t, u_int32_t));
 */
int
__db_rec_toobig(dbenv, data_len, fixed_rec_len)
	DB_ENV *dbenv;
	u_int32_t data_len, fixed_rec_len;
{
	__db_err(dbenv, "%s: length of %lu larger than database's value of %lu",
	    "Record length error", (u_long)data_len, (u_long)fixed_rec_len);
	return (EINVAL);
}

/*
 * __db_rec_repl --
 *	Fixed record replacement length error message.
 *
 * PUBLIC: int __db_rec_repl __P((DB_ENV *, u_int32_t, u_int32_t));
 */
int
__db_rec_repl(dbenv, data_size, data_dlen)
	DB_ENV *dbenv;
	u_int32_t data_size, data_dlen;
{
	__db_err(dbenv,
	    "%s: replacement length %lu differs from replaced length %lu",
	    "Record length error", (u_long)data_size, (u_long)data_dlen);
	return (EINVAL);
}
