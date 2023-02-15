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
#include "logmsg.h"
#include "dbinc/log.h"
#include "dbinc/db_swap.h"
#include "schema_lk.h"

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

static inline void __free_prepared_txn(DB_ENV *dbenv, DB_TXN_PREPARED *p)
{
	assert(p->is_prepared == 0);
	if (p->blkseq_key.data != NULL)
		__os_free(dbenv, p->blkseq_key.data);
	if (p->coordinator_name.data != NULL)
		__os_free(dbenv, p->coordinator_name.data);
	if (p->coordinator_tier.data != NULL)
		__os_free(dbenv, p->coordinator_tier.data);
	/* This better not have locks or schema-lock */
	if (p->txnp)
		__txn_discard(p->txnp, 0);
	if (p->pglogs)
		free(p->pglogs);
	__os_free(dbenv, p);
}

/*
 * __txn_recover_prepared --
 *
 * Collect prepared transactions during recovery's forward-pass (openfiles).
 *
 * PUBLIC: int __txn_recover_prepared __P((DB_ENV *,
 * PUBLIC:     u_int64_t, DB_LSN *, DBT *, u_int32_t, DBT *, DBT *));
 */
int __txn_recover_prepared(dbenv, dist_txnid, prep_lsn, blkseq_key, coordinator_gen,
		coordinator_name, coordinator_tier)
	DB_ENV *dbenv;
	u_int64_t dist_txnid;
	DB_LSN *prep_lsn;
	DBT *blkseq_key;
	u_int32_t coordinator_gen;
	DBT *coordinator_name;
	DBT *coordinator_tier;
{
	int ret;
	DB_TXN_PREPARED *p, *fnd;

	if ((ret = __os_calloc(dbenv, 1, sizeof(DB_TXN_PREPARED), &p)) != 0) {
		return (ret);
	}

	p->dist_txnid = dist_txnid;
	p->prepare_lsn = *prep_lsn;

	if ((ret = __os_calloc(dbenv, 1, blkseq_key->size, &p->blkseq_key.data)) != 0) {
		__free_prepared_txn(dbenv, p);
		__db_err(dbenv, "failed malloc");
		return ret;
	}

	memcpy(p->blkseq_key.data, blkseq_key->data, blkseq_key->size);
	p->blkseq_key.size = blkseq_key->size;

	p->coordinator_gen = coordinator_gen;

	if ((ret = __os_calloc(dbenv, 1, coordinator_name->size, &p->coordinator_name.data)) != 0) {
		__free_prepared_txn(dbenv, p);
		__db_err(dbenv, "failed malloc");
		return ret;
	}

	memcpy(p->coordinator_name.data, coordinator_name->data, coordinator_name->size);
	p->coordinator_name.size = coordinator_name->size;

	if ((ret = __os_calloc(dbenv, 1, coordinator_tier->size, &p->coordinator_tier.data)) != 0) {
		__free_prepared_txn(dbenv, p);
		__db_err(dbenv, "failed malloc");
		return ret;
	}

	memcpy(p->coordinator_tier.data, coordinator_tier->data, coordinator_tier->size);
	p->coordinator_tier.size = coordinator_tier->size;

	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((fnd = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		logmsg(LOGMSG_FATAL, "Recovery found multiple prepared records with dist-txnid %"PRIu64"\n",
			dist_txnid);
		abort();
	}
	hash_add(dbenv->prepared_txn_hash, p);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	return 0;
}

static int __free_prepared(void *obj, void *arg)
{
	DB_ENV *dbenv = (DB_ENV *)arg;
	DB_TXN_PREPARED *p = (DB_TXN_PREPARED *)obj;
	__free_prepared_txn(dbenv, p);
	return 0;
}

/*
 * __txn_clear_all_prepared --
 *
 * Clear all prepared transactions
 *
 * PUBLIC: int __txn_recover_prepared __P((DB_ENV *));
 */
int __txn_clear_all_prepared(dbenv)
	DB_ENV *dbenv;
{
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	hash_for(dbenv->prepared_txn_hash, __free_prepared, dbenv);
	hash_clear(dbenv->prepared_txn_hash);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	return 0;
}

/* 
 * __txn_clear_prepared --
 *
 * Remove a prepared transaction, optionally update it's blkseq 
 *
 * PUBLIC: int __txn_clear_prepared __P((DB_ENV *, u_int64_t, int ));
 */
int __txn_clear_prepared(dbenv, dist_txnid, update_blkseq)
	DB_ENV *dbenv;
	u_int64_t dist_txnid;
	int update_blkseq;
{
	DB_TXN_PREPARED *p;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		hash_del(dbenv->prepared_txn_hash, p);
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);

	if (p == NULL) {
		logmsg(LOGMSG_INFO, "%s unable to locate txnid %"PRIu64"\n", __func__, dist_txnid);
		return -1;
	}

	/* TODO */
	if (update_blkseq) {
	}
	__free_prepared_txn(dbenv, p);

	return 0;
}

static int __upgrade_prepared_txn(DB_ENV *dbenv, DB_LOGC *logc, DB_TXN_PREPARED *p)
{
	DBT dbt = { 0 };
	__txn_dist_prepare_args *prepare = NULL;
	u_int32_t type;
	dbt.flags = DB_DBT_REALLOC;
	DB_TXN *txnp = NULL;
	int ret;

	assert(p->is_prepared == 0);

	if ((ret = __log_c_get(logc, &p->prepare_lsn, &dbt, DB_SET)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error %d retrieving prepare record at %d:%d\n",
			__func__, ret, p->prepare_lsn.file, p->prepare_lsn.offset);
		abort();
	}

	LOGCOPY_32(&type, dbt.data);
	assert(type == DB___txn_dist_prepare);

	if ((ret = __txn_dist_prepare_read(dbenv, dbt.data, &prepare)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error %d unpacking prepare record at %d:%d\n",
			__func__, ret, p->prepare_lsn.file, p->prepare_lsn.offset);
		abort();
	}

	/* TODO: I guess I need to force it to use the txnid listed in the log so
	 * that it's not re-allocated to a different txn.  Maybe straightforward? */
	if ((ret = __txn_begin_with_prop(dbenv, NULL, &txnp, 0, NULL)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error %d creating txn at %d:%d\n",
			__func__, ret, p->prepare_lsn.file, p->prepare_lsn.offset);
		abort();
	}

	/* TODO: consider collecting & storing lc-colleciton in prepared-txn */
	if (prepare->lflags & DB_TXN_SCHEMA_LOCK) {
		wrlock_schema_lk(); p->have_schema_lock = 1;
	}

	/* Acquire locks/pglogs from prepare record */
	if ((ret = __lock_get_list(dbenv, txnp->txnid, LOCK_GET_LIST_GETLOCK,
		DB_LOCK_WRITE, &prepare->locks, &p->prepare_lsn, &p->pglogs,
		&p->keycnt, stdout)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error %d acquiring locks for prepare at %d:%d\n",
			__func__, ret, p->prepare_lsn.file, p->prepare_lsn.offset);
		abort();
	}

	/* Set the 'apply-forward' flag.. on second thought i dont need it */
	//F_SET(txnp, TXN_DIST_REC_PREPARED);
	p->txnp = txnp;
	p->is_prepared = 1;

	__os_free(dbenv, prepare);
	__os_free(dbenv, dbt.data);

	return 0;
}

static int __upgrade_prepared(void *obj, void *arg)
{
	DB_LOGC *logc = (DB_LOGC *)arg;
	DB_TXN_PREPARED *p = (DB_TXN_PREPARED *)obj;
	__upgrade_prepared_txn(logc->dbenv, logc, p);
	return 0;
}

/* 
 * __txn_upgrade_all_prepared --
 *
 * Create berkley txns for unresolved but prepared transactions we found
 * during recovery.  Acquire locks for these transactions.
 *
 * PUBLIC: int __txn_upgrade_all_prepared __P((DB_ENV *));
 */
int __txn_upgrade_all_prepared(dbenv)
	DB_ENV *dbenv;
{
	DB_LOGC *logc = NULL;
	int ret;
	if ((ret = __log_cursor(dbenv, &logc)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error getting log-cursor, %d\n", __func__, ret);
		abort();
	}
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	hash_for(dbenv->prepared_txn_hash, __upgrade_prepared, logc);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	logc->close(logc, 0);
	return 0;
}

/* 
 * __txn_abort_prepared --
 *
 * Write a dist_abort record for this prepared txnid.  
 *
 * PUBLIC: int __txn_abort_prepared __P((DB_ENV *, u_int64_t));
 */
int __txn_abort_prepared(dbenv, dist_txnid)
	DB_ENV *dbenv;
	u_int64_t dist_txnid;
{
	return 0;
}

int __txn_commit_prepared(dbenv, dist_txnid)
	DB_ENV *dbenv;
	u_int64_t dist_txnid;
{
	DB_TXN_PREPARED *p;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		if (!p->is_prepared) {
			logmsg(LOGMSG_INFO, "%s unable to commit unprepared dist-txn %"PRIu64"\n", __func__, dist_txnid);
			p = NULL;
		} else {
			hash_del(dbenv->prepared_txn_hash, p);
		}
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	if (p == NULL) {
		logmsg(LOGMSG_INFO, "%s unable to locate txnid %"PRIu64"\n", __func__, dist_txnid);
		return -1;
	}
	/* Collect txn */
	/* Then 'roll-forward' */
	/* Then 'commit' */

	return 0;
}
