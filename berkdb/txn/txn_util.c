/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2001-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: txn_util.c,v 11.25 2003/12/03 14:33:07 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/lock.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"
#include "dbinc/db_am.h"
#include <logmsg.h>

typedef struct __txn_allocedpage TXN_ALLOCEDPAGE;
struct __txn_allocedpage {
	TAILQ_ENTRY(__txn_allocedpage) links;
	DB *dbp;
	db_pgno_t pgno;
};

typedef struct __txn_freedpage TXN_FREEDPAGE;
struct __txn_freedpage {
	TAILQ_ENTRY(__txn_freedpage) links;
	DB *dbp;
	db_pgno_t pgno;
};

typedef struct __txn_event TXN_EVENT;
struct __txn_event {
	TXN_EVENT_T op;
	TAILQ_ENTRY(__txn_event) links;
	union {
		struct {
			/* Delayed close. */
			DB *dbp;
		} c;
		struct {
			/* Delayed remove. */
			char *name;
			u_int8_t *fileid;
		} r;
		struct {
			/* Lock event. */
			DB_LOCK lock;
			u_int32_t locker;
			DB *dbp;
		} t;
	} u;
};

/*
 * __txn_closeevent --
 *
 * Creates a close event that can be added to the [so-called] commit list, so
 * that we can redo a failed DB handle close once we've aborted the transaction.
 *
 * PUBLIC: int __txn_closeevent __P((DB_ENV *, DB_TXN *, DB *));
 */
int
__txn_closeevent(dbenv, txn, dbp)
	DB_ENV *dbenv;
	DB_TXN *txn;
	DB *dbp;
{
	int ret;
	TXN_EVENT *e;

	e = NULL;
	if ((ret = __os_calloc(dbenv, 1, sizeof(TXN_EVENT), &e)) != 0)
		return (ret);

	e->u.c.dbp = dbp;
	e->op = TXN_CLOSE;
	TAILQ_INSERT_TAIL(&txn->events, e, links);

	return (0);
}

/*
 * __txn_track_alloced_page --
 * Adds page to list of allocated pages for a transaction.  Will be returned
 * to the freelist in a separate transaction on abort.
 *
 * PUBLIC: int __txn_track_alloced_page __P((DB_ENV *, DB_TXN *, DB *, db_pgno_t));
 */

int
__txn_track_alloced_page(dbenv, txn, dbp, pgno)
	DB_ENV *dbenv;
	DB_TXN *txn;
	DB *dbp;
	db_pgno_t pgno;
{
	int ret;
	TXN_ALLOCEDPAGE *p;
	if ((ret = __os_calloc(dbenv, 1, sizeof(*p), &p)) != 0)
		return ret;

	p->dbp = dbp;
	p->pgno = pgno;
	TAILQ_INSERT_TAIL(&txn->alloced_pages, p, links);
	return (0);
}

/*
 * __txn_track_freed_page --
 * Adds page to list of freed pages for a transaction.  Actual freeing
 * is deferred until the transaction commits.
 *
 * PUBLIC: int __txn_track_freed_page __P((DB_ENV *, DB_TXN *, DB *, db_pgno_t));
 */

int
__txn_track_freed_page(dbenv, txn, dbp, pgno)
	DB_ENV *dbenv;
	DB_TXN *txn;
	DB *dbp;
	db_pgno_t pgno;
{
	int ret;
	TXN_FREEDPAGE *p;
	if ((ret = __os_calloc(dbenv, 1, sizeof(TXN_FREEDPAGE), &p)) != 0)
		return ret;

	p->dbp = dbp;
	p->pgno = pgno;
	TAILQ_INSERT_TAIL(&txn->freed_pages, p, links);
	return (0);
}

/*
 * __txn_concat_page_lists --
 * Concatenate a child transaction's pagelists into the parent
 *
 * PUBLIC: int __txn_concat_page_lists __P((DB_ENV *,
 * PUBLIC:	   DB_TXN *));
 */
int
__txn_concat_page_lists(dbenv, txn)
	DB_ENV *dbenv;
	DB_TXN *txn;
{
	assert(txn->parent);
	if (TAILQ_FIRST(&txn->alloced_pages)) {
		TAILQ_CONCAT(&txn->alloced_pages,
				&txn->parent->alloced_pages, links);
		txn->parent->alloced_pages = txn->alloced_pages;
	}
	TAILQ_INIT(&txn->alloced_pages);

	if (TAILQ_FIRST(&txn->freed_pages)) {
		TAILQ_CONCAT(&txn->freed_pages,
				&txn->parent->freed_pages, links);
		txn->parent->freed_pages = txn->freed_pages;
	}
	TAILQ_INIT(&txn->freed_pages);
	return 0;
}

/*
 * __txn_reclaim_alloced_pages --
 * Reclaim pages that were allocated during this aborted transaction.
 *
 * PUBLIC: int __txn_reclaim_alloced_pages __P((DB_ENV *,
 * PUBLIC:	   DB_TXN *));
 */
int
__txn_reclaim_alloced_pages(dbenv, txn)
	DB_ENV *dbenv;
	DB_TXN *txn;
{
	TXN_ALLOCEDPAGE *p;
	DB_TXN *txnp = NULL;
	DBC *dbc = NULL;
	int ret;
	DB *dbp, *last_dbp = NULL;
	PAGE *h;
	db_pgno_t pgno;

	while ((p = TAILQ_FIRST(&txn->alloced_pages)) != NULL) {
		TAILQ_REMOVE(&txn->alloced_pages, p, links);
		if (!txnp && (ret = dbenv->txn_begin(dbenv, NULL, &txnp, 0)) != 0) {
			logmsg(LOGMSG_FATAL, "%s cannot begin a transaction, ret=%d\n",
					__func__, ret);
			abort();
		}
		dbp = p->dbp;
		pgno = p->pgno;
		__os_free(dbenv, p);

		if (dbc && last_dbp != dbp) {
			if ((ret = __db_c_close(dbc)) != 0) {
				logmsg(LOGMSG_FATAL, "%s failed to close cursor, ret=%d\n",
						__func__, ret);
				abort();
			}
			dbc = NULL;
		}

		last_dbp = dbp;

		if (dbc == NULL && (ret = __db_cursor(dbp, txnp, &dbc, 0)) != 0) {
			logmsg(LOGMSG_FATAL, "%s failed to aquire cursor, ret=%d\n",
					__func__, ret);
			abort();
		}

		if ((ret = __memp_fget(dbp->mpf, &pgno, 0, &h)) != 0) {
			logmsg(LOGMSG_FATAL, "%s failed to aquire page, ret=%d\n",
					__func__, ret);
			abort();
		}

		if ((ret = __db_dofree(dbc, h)) != 0) {
			logmsg(LOGMSG_FATAL, "%s failed to free page, ret=%d\n",
					__func__, ret);
			abort();
		}
	}

	if (dbc && (ret = __db_c_close(dbc)) != 0) {
		logmsg(LOGMSG_FATAL, "%s failed to close last cursor, ret=%d\n",
				__func__, ret);
		abort();
	}

	if (txnp && (ret = txnp->commit(txnp,0)) != 0) {
		logmsg(LOGMSG_FATAL, "%s failed to commit freed pages, ret=%d\n",
				__func__, ret);
		abort();
	}

	TAILQ_INIT(&txn->alloced_pages);
	return 0;
}

/*
 * __txn_clear_alloced_page_list --
 *
 * Free memory for allocated pages
 *
 * PUBLIC: int __txn_clear_alloced_page_list __P((DB_ENV *,
 * PUBLIC:	   DB_TXN *));
 */
int
__txn_clear_alloced_page_list(dbenv, txn)
	DB_ENV *dbenv;
	DB_TXN *txn;
{
	TXN_ALLOCEDPAGE *ap, *next_ap;
	for ((ap = TAILQ_FIRST(&txn->alloced_pages)); ap != NULL; ap = next_ap) {
		next_ap = TAILQ_NEXT(ap, links);
		TAILQ_REMOVE(&txn->alloced_pages, ap, links);
		__os_free(dbenv, ap);
	}
	TAILQ_INIT(&txn->alloced_pages);
	return 0;
}

/*
 * __txn_clear_free_page_list --
 *
 * Free memory for allocated pages
 *
 * PUBLIC: int __txn_clear_free_page_list __P((DB_ENV *,
 * PUBLIC:	   DB_TXN *));
 */
int
__txn_clear_free_page_list(dbenv, txn)
	DB_ENV *dbenv;
	DB_TXN *txn;
{
	TXN_FREEDPAGE *fp, *next_fp;
	for ((fp  = TAILQ_FIRST(&txn->freed_pages)); fp != NULL; fp = next_fp) {
		next_fp = TAILQ_NEXT(fp, links);
		TAILQ_REMOVE(&txn->freed_pages, fp, links);
		__os_free(dbenv, fp);
	}
	TAILQ_INIT(&txn->freed_pages);
	return 0;
}

/*
 * __txn_freepages --
 *
 * Free pages that were released during this committed transaction.
 *
 * PUBLIC: int __txn_freepages __P((DB_ENV *,
 * PUBLIC:	   DB_TXN *));
 */
int
__txn_freepages(dbenv, txn)
	DB_ENV *dbenv;
	DB_TXN *txn;
{
	TXN_FREEDPAGE *p, *next_p;
	int ret;
	PAGE *h;
	db_pgno_t pgno;
	DB *dbp, *last_dbp = NULL;
	DBC *dbc = NULL;
	DB_TXN *txnp = NULL;

	for ((p = TAILQ_FIRST(&txn->freed_pages)); p != NULL; p = next_p) {
		next_p = TAILQ_NEXT(p, links);
		TAILQ_REMOVE(&txn->freed_pages, p, links);
		if (!txnp && (ret = dbenv->txn_begin(dbenv, NULL, &txnp, 0)) != 0) {
			logmsg(LOGMSG_FATAL, "%s cannot begin a transaction, ret=%d\n",
					__func__, ret);
			abort();
		}
		dbp = p->dbp;
		pgno = p->pgno;
		__os_free(dbenv, p);

		if (dbc && last_dbp != dbp) {
			if ((ret = __db_c_close(dbc)) != 0) {
				logmsg(LOGMSG_FATAL, "%s failed to close cursor, ret=%d\n",
						__func__, ret);
				abort();
			}
			dbc = NULL;
		}

		last_dbp = dbp;

		if (dbc == NULL && (ret = __db_cursor(dbp, txnp, &dbc, 0)) != 0) {
			logmsg(LOGMSG_FATAL, "%s failed to aquire cursor, ret=%d\n",
					__func__, ret);
			abort();
		}

		if ((ret = __memp_fget(dbp->mpf, &pgno, 0, &h)) != 0) {
			logmsg(LOGMSG_FATAL, "%s failed to aquire page, ret=%d\n",
					__func__, ret);
			abort();
		}

		if ((ret = __db_dofree(dbc, h)) != 0) {
			logmsg(LOGMSG_FATAL, "%s failed to free page, ret=%d\n",
					__func__, ret);
			abort();
		}

		if ((ret = __memp_fput(dbp->mpf, h, 0)) != 0) {
			logmsg(LOGMSG_FATAL, "%s failed to release page, ret=%d\n",
					__func__, ret);
			abort();
		}
	}

	if (dbc && (ret = __db_c_close(dbc)) != 0) {
		logmsg(LOGMSG_FATAL, "%s failed to close last cursor, ret=%d\n",
				__func__, ret);
		abort();
	}

	if (txnp && (ret = txnp->commit(txnp, 0)) != 0) {
		logmsg(LOGMSG_FATAL, "%s failed to commit freed pages, ret=%d\n",
				__func__, ret);
		abort();
	}

	TAILQ_INIT(&txn->freed_pages);
	return 0;
}

/*
 * __txn_remevent --
 *
 * Creates a remove event that can be added to the commit list.
 *
 * PUBLIC: int __txn_remevent __P((DB_ENV *,
 * PUBLIC:       DB_TXN *, const char *, u_int8_t*));
 */
int
__txn_remevent(dbenv, txn, name, fileid)
	DB_ENV *dbenv;
	DB_TXN *txn;
	const char *name;
	u_int8_t *fileid;
{
	int ret;
	TXN_EVENT *e;

	e = NULL;
	if ((ret = __os_calloc(dbenv, 1, sizeof(TXN_EVENT), &e)) != 0)
		return (ret);

	if ((ret = __os_strdup(dbenv, name, &e->u.r.name)) != 0)
		goto err;

	if (fileid != NULL) {
		if ((ret = __os_calloc(dbenv,
		    1, DB_FILE_ID_LEN, &e->u.r.fileid)) != 0)
			return (ret);
		memcpy(e->u.r.fileid, fileid, DB_FILE_ID_LEN);
	}

	e->op = TXN_REMOVE;
	TAILQ_INSERT_TAIL(&txn->events, e, links);

	return (0);

err:	if (e != NULL)
		__os_free(dbenv, e);

	return (ret);
}
/*
 * __txn_remrem --
 *	Remove a remove event because the remove has be superceeded,
 * by a create of the same name, for example.
 *
 * PUBLIC: void __txn_remrem __P((DB_ENV *, DB_TXN *, const char *));
 */
void
__txn_remrem(dbenv, txn, name)
	DB_ENV *dbenv;
	DB_TXN *txn;
	const char *name;
{
	TXN_EVENT *e, *next_e;

	for (e = TAILQ_FIRST(&txn->events); e != NULL; e = next_e) {
		next_e = TAILQ_NEXT(e, links);
		if (e->op != TXN_REMOVE || strcmp(name, e->u.r.name) != 0)
			continue;
		TAILQ_REMOVE(&txn->events, e, links);
		__os_free(dbenv, e->u.r.name);
		if (e->u.r.fileid != NULL)
			__os_free(dbenv, e->u.r.fileid);
		__os_free(dbenv, e);
	}

	return;
}

/*
 * __txn_lockevent --
 *
 * Add a lockevent to the commit-queue.  The lock event indicates a locker
 * trade.
 *
 * PUBLIC: int __txn_lockevent __P((DB_ENV *,
 * PUBLIC:     DB_TXN *, DB *, DB_LOCK *, u_int32_t));
 */
int
__txn_lockevent(dbenv, txn, dbp, lock, locker)
	DB_ENV *dbenv;
	DB_TXN *txn;
	DB *dbp;
	DB_LOCK *lock;
	u_int32_t locker;
{
	int ret;
	TXN_EVENT *e;

	if (!LOCKING_ON(dbenv))
		return (0);

	e = NULL;
	if ((ret = __os_calloc(dbenv, 1, sizeof(TXN_EVENT), &e)) != 0)
		return (ret);

	e->u.t.locker = locker;
	e->u.t.lock = *lock;
	e->u.t.dbp = dbp;
	e->op = TXN_TRADE;
	TAILQ_INSERT_TAIL(&txn->events, e, links);

	return (0);
}

/*
 * __txn_remlock --
 *	Remove a lock event because the locker is going away.  We can remove
 * by lock (using offset) or by locker_id (or by both).
 *
 * PUBLIC: void __txn_remlock __P((DB_ENV *, DB_TXN *, DB_LOCK *, u_int32_t));
 */
void
__txn_remlock(dbenv, txn, lock, locker)
	DB_ENV *dbenv;
	DB_TXN *txn;
	DB_LOCK *lock;
	u_int32_t locker;
{
	TXN_EVENT *e, *next_e;

	for (e = TAILQ_FIRST(&txn->events); e != NULL; e = next_e) {
		next_e = TAILQ_NEXT(e, links);
		if ((e->op != TXN_TRADE && e->op != TXN_TRADED) ||
		    (e->u.t.lock.off != lock->off && e->u.t.locker != locker))
			continue;
		TAILQ_REMOVE(&txn->events, e, links);
		__os_free(dbenv, e);
	}

	return;
}

/*
 * __txn_doevents --
 * Process the list of events associated with a transaction.  On commit,
 * apply the events; on abort, just toss the entries.
 *
 * PUBLIC: int __txn_doevents __P((DB_ENV *, DB_TXN *, int, int));
 */
#define	DO_TRADE do {							\
	memset(&req, 0, sizeof(req));					\
	req.lock = e->u.t.lock;						\
	req.op = DB_LOCK_TRADE;						\
	t_ret = __lock_vec(dbenv, e->u.t.locker, 0, &req, 1, NULL);	\
	if (t_ret == 0)							\
		e->u.t.dbp->cur_lid = e->u.t.locker;			\
	else if (t_ret == DB_NOTFOUND)					\
		t_ret = 0;						\
	if (t_ret != 0 && ret == 0)					\
		ret = t_ret;						\
	e->op = TXN_TRADED;						\
} while (0)

int
__txn_doevents(dbenv, txn, opcode, preprocess)
	DB_ENV *dbenv;
	DB_TXN *txn;
	int opcode, preprocess;
{
	DB_LOCKREQ req;
	TXN_EVENT *e;
	int ret, t_ret;

	ret = 0;

	/*
	 * This phase only gets called if we have a phase where we
	 * release read locks.  Since not all paths will call this
	 * phase, we have to check for it below as well.  So, when
	 * we do the trade, we update the opcode of the entry so that
	 * we don't try the trade again.
	 */
	if (preprocess) {
		for (e = TAILQ_FIRST(&txn->events);
		    e != NULL; e = TAILQ_NEXT(e, links)) {
			if (e->op != TXN_TRADE)
				continue;
			DO_TRADE;
		}
		return (ret);
	}

	/*
	 * Prepare should only cause a preprocess, since the transaction
	 * isn't over.
	 */
	DB_ASSERT(opcode != TXN_PREPARE);
	while ((e = TAILQ_FIRST(&txn->events)) != NULL) {
		TAILQ_REMOVE(&txn->events, e, links);
		/*
		 * Most deferred events should only happen on
		 * commits, not aborts or prepares.  The one exception
		 * is a close which gets done on commit and abort, but
		 * not prepare. If we're not doing operations, then we
		 * can just go free resources.
		 */
		if (opcode == TXN_ABORT && e->op != TXN_CLOSE)
			goto dofree;
		switch (e->op) {
		case TXN_CLOSE:
			/* If we didn't abort this txn, we screwed up badly. */
			DB_ASSERT(opcode == TXN_ABORT);
			if ((t_ret =
			    __db_close(e->u.c.dbp, NULL, 0)) != 0 && ret == 0)
				ret = t_ret;
			break;
		case TXN_REMOVE:
			if (e->u.r.fileid != NULL) {
				if ((t_ret = __memp_nameop(dbenv,
				    e->u.r.fileid,
				    NULL, e->u.r.name, NULL)) != 0 && ret == 0)
					ret = t_ret;
			} else if ((t_ret =
			    __os_unlink(dbenv, e->u.r.name)) != 0 && ret == 0)
				ret = t_ret;
			break;
		case TXN_TRADE:
			DO_TRADE;
			/* Fall through */
		case TXN_TRADED:
			/* Downgrade the lock. */
			if ((t_ret = __lock_downgrade(dbenv,
			    &e->u.t.lock, DB_LOCK_READ, 0)) != 0 && ret == 0)
				ret = t_ret;
			break;
		default:
			/* This had better never happen. */
			DB_ASSERT(0);
		}
dofree:
		/* Free resources here. */
		switch (e->op) {
		case TXN_REMOVE:
			if (e->u.r.fileid != NULL)
				__os_free(dbenv, e->u.r.fileid);
			__os_free(dbenv, e->u.r.name);
			break;

		default:
			break;
		}
		__os_free(dbenv, e);
	}

	return (ret);
}
