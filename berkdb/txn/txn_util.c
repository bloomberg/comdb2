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
#include "txn_properties.h"
#include "epochlib.h"

void comdb2_cheapstack_sym(FILE *f, char *fmt, ...);

extern int set_commit_context_prepared(unsigned long long context);

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
 * PUBLIC:	   DB_TXN *, const char *, u_int8_t*));
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
 * PUBLIC:	 DB_TXN *, DB *, DB_LOCK *, u_int32_t));
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
	assert(!F_ISSET(p, DB_DIST_HAVELOCKS));
	if (p->blkseq_key.data != NULL)
		__os_free(dbenv, p->blkseq_key.data);
	if (p->coordinator_name.data != NULL)
		__os_free(dbenv, p->coordinator_name.data);
	if (p->coordinator_tier.data != NULL)
		__os_free(dbenv, p->coordinator_tier.data);
	if (p->txnp)
		__txn_discard(p->txnp, 0);
	if (p->pglogs)
		free(p->pglogs);
	if (p->children)
		free(p->children);
	__os_free(dbenv, p);
}

/*
 * __txn_is_dist_committed --
 *
 * Add dist_txn to prepared-txn hash as a committed txn.  This is needed 
 * to accommodate a commit from a different master.
 *
 * PUBLIC: int __txn_is_dist_committed __P((DB_ENV *, u_int64_t dist_txnid));
 */
int __txn_is_dist_committed(dbenv, dist_txnid)
	DB_ENV *dbenv;
	u_int64_t dist_txnid;
{
	int ret = 0;
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_TXN_PREPARED *p;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) && F_ISSET(p, DB_DIST_COMMITTED))
		ret = 1;
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	return ret;
}

/*
 * __txn_recover_dist_commit --
 *
 * Add dist_txn to prepared-txn hash as a committed txn.  This is needed 
 * to accommodate a commit from a different master.
 *
 * PUBLIC: int __txn_recover_dist_commit __P((DB_ENV *, u_int64_t dist_txnid));
 */
int __txn_recover_dist_commit(dbenv, dist_txnid)
	DB_ENV *dbenv;
	u_int64_t dist_txnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	int ret;
	DB_TXN_PREPARED *p;

	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) == NULL) {

		if ((ret = __os_calloc(dbenv, 1, sizeof(DB_TXN_PREPARED), &p)) != 0) {
			return (ret);
		}
		p->dist_txnid = dist_txnid;
		hash_add(dbenv->prepared_txn_hash, p);
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	F_SET(p, DB_DIST_COMMITTED);
#if defined (DEBUG_PREPARE)
	logmsg(LOGMSG_USER, "%s adding committed prepare dist-txnid %"PRIu64" to list\n",
		__func__, dist_txnid);
#endif
	return 0;
}

/*
 * __txn_recover_dist_abort --
 *
 * Add dist_txn to prepared-txn hash as an aborted txn.  
 * TODO: add the blkseq key.
 *
 * PUBLIC: int __txn_recover_dist_abort __P((DB_ENV *, u_int64_t dist_txnid));
 */
int __txn_recover_dist_abort(dbenv, dist_txnid)
	DB_ENV *dbenv;
	u_int64_t dist_txnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	int ret;
	DB_TXN_PREPARED *p, *fnd;

	if ((ret = __os_calloc(dbenv, 1, sizeof(DB_TXN_PREPARED), &p)) != 0) {
		return (ret);
	}

	p->dist_txnid = dist_txnid;
	F_SET(p, DB_DIST_ABORTED);
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((fnd = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		logmsg(LOGMSG_FATAL, "Recovery found multiple prepared records with dist-txnid %"PRIu64"\n",
			dist_txnid);
		abort();
	}
	hash_add(dbenv->prepared_txn_hash, p);
#if defined (DEBUG_PREPARE)
	logmsg(LOGMSG_USER, "%s adding aborted prepare dist-txnid %"PRIu64" to list\n",
		__func__, dist_txnid);
#endif
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	return 0;
}

int __txn_master_prepared(dbenv, dist_txnid, prep_lsn, begin_lsn, blkseq_key, coordinator_gen,
		coordinator_name, coordinator_tier)
	DB_ENV *dbenv;
	u_int64_t dist_txnid;
	DB_LSN *prep_lsn;
	DB_LSN *begin_lsn;
	DBT *blkseq_key;
	u_int32_t coordinator_gen;
	DBT *coordinator_name;
	DBT *coordinator_tier;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	int ret;
	DB_TXN_PREPARED *p, *fnd;

	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		logmsg(LOGMSG_FATAL, "master-prepared txn on dist-list, dist-txnid %"PRIu64"\n",
				dist_txnid);
		abort();
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);

	if ((ret = __os_calloc(dbenv, 1, sizeof(DB_TXN_PREPARED), &p)) != 0) {
		return (ret);
	}

	p->dist_txnid = dist_txnid;
	p->prepare_lsn = *prep_lsn;
	p->begin_lsn = *begin_lsn;

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
	F_SET(p, DB_DIST_INFLIGHT);

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

/*
 * __txn_recover_abort_prepared --
 *
 * Remove an aborted transaction from the distributed transaction list.
 * This is run on the master when a prepared transaction aborts.
 *
 * PUBLIC: int __txn_recover_abort_prepared __P((DB_ENV *,
 * PUBLIC:	  u_int64_t, DB_LSN *, DBT *, u_int32_t, DBT *, DBT *));
 */

int __txn_recover_abort_prepared(dbenv, dist_txnid, prep_lsn, blkseq_key, coordinator_gen,
		coordinator_name, coordinator_tier)
	DB_ENV *dbenv;
	u_int64_t dist_txnid;
	DB_LSN *prep_lsn;
	DBT *blkseq_key;
	u_int32_t coordinator_gen;
	DBT *coordinator_name;
	DBT *coordinator_tier;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	int ret;
	DB_TXN_PREPARED *p, *fnd;

	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) == NULL) {
		logmsg(LOGMSG_FATAL, "%s cannot find prepared transaction %"PRIu64"\n", 
			__func__, dist_txnid);
		abort();
	}
	hash_del(dbenv->prepared_txn_hash, p);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	if (F_ISSET(p, DB_DIST_RECOVERED) || p->txnp != NULL) {
		 logmsg(LOGMSG_FATAL, "%s aborting a recovered transaction %"PRIu64"\n", 
			__func__, dist_txnid);
		abort();
	}

	__free_prepared_txn(dbenv, p);
	return 0;
}

/*
 * __txn_recover_prepared --
 *
 * Collect prepared transactions during recovery's backward-pass.  We should
 * only run this for aborted and unresolved dist-txns.
 *
 * PUBLIC: int __txn_recover_prepared __P((DB_ENV *, DB_TXN *txnid,
 * PUBLIC:	  u_int64_t, DB_LSN *, DB_LSN *, DBT *, u_int32_t, DBT *, DBT *));
 */
int __txn_recover_prepared(dbenv, txnid, dist_txnid, prep_lsn, begin_lsn, blkseq_key,
		coordinator_gen, coordinator_name, coordinator_tier)
	DB_ENV *dbenv;
	DB_TXN *txnid;
	u_int64_t dist_txnid;
	DB_LSN *prep_lsn;
	DB_LSN *begin_lsn;
	DBT *blkseq_key;
	u_int32_t coordinator_gen;
	DBT *coordinator_name;
	DBT *coordinator_tier;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	int ret;
	DB_TXN_PREPARED *p, *fnd;

	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		/* Should only find this here during backwards roll, and it should have
		 * the DIST_ABORTED flag lit */
		if(!F_ISSET(p, DB_DIST_ABORTED)) {
			logmsg(LOGMSG_FATAL, "Non-aborted prepared record, dist-txnid %"PRIu64"\n",
				dist_txnid);
			abort();
		}
		hash_del(dbenv->prepared_txn_hash, p);
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	if (p) {
		/* 'aborted-from-recovery': we assert that ABORTED flag is lit above:
		   !!! TODO !!! need to write dist-abort to blkseq  */
#if defined (DEBUG_PREPARE)
		logmsg(LOGMSG_USER, "Removing aborted prepare %"PRIu64" from recovered txn list\n", dist_txnid);
#endif
		__free_prepared_txn(dbenv, p);
		return (0);
	}

	if ((ret = __os_calloc(dbenv, 1, sizeof(DB_TXN_PREPARED), &p)) != 0) {
		return (ret);
	}

	p->dist_txnid = dist_txnid;
	p->prepare_lsn = *prep_lsn;
	p->begin_lsn = *begin_lsn;
	p->txnid = txnid->txnid;

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
	F_SET(p, DB_DIST_RECOVERED);

	hash_add(dbenv->prepared_txn_hash, p);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	logmsg(LOGMSG_DEBUG, "%s added unresolved prepared txn %"PRIu64"\n", __func__, dist_txnid);
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
 * PUBLIC: int __txn_clear_all_prepared __P((DB_ENV *));
 */
int __txn_clear_all_prepared(dbenv)
	DB_ENV *dbenv;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	hash_for(dbenv->prepared_txn_hash, __free_prepared, dbenv);
	hash_clear(dbenv->prepared_txn_hash);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	return 0;
}

/* 
 * __txn_clear_prepared --
 *
 * Remove a prepared transaction, optionally update it's blkseq.  Called
 * When we recover a dist_abort record.
 *
 * PUBLIC: int __txn_clear_prepared __P((DB_ENV *, u_int64_t, int));
 */
int __txn_clear_prepared(dbenv, dist_txnid, update_blkseq)
	DB_ENV *dbenv;
	u_int64_t dist_txnid;
	int update_blkseq;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_TXN_PREPARED *p;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		hash_del(dbenv->prepared_txn_hash, p);
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);

	if (p == NULL) {
#if defined (DEBUG_PREPARE)
		logmsg(LOGMSG_USER, "%s unable to locate prepared txnid %"PRIu64"\n", __func__, dist_txnid);
#endif
		return -1;
	}

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

	assert(!F_ISSET(p, DB_DIST_HAVELOCKS) && !F_ISSET(p, DB_DIST_ABORTED));

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

	/* Use the txnid in the log */
	struct txn_properties prop = {.prepared_txnid = prepare->txnid->txnid,
								  .begin_lsn = prepare->begin_lsn };
	if ((ret = __txn_begin_with_prop(dbenv, NULL, &txnp, 0, &prop)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error %d creating txn at %d:%d\n",
			__func__, ret, p->prepare_lsn.file, p->prepare_lsn.offset);
		abort();
	}

	/* Set the prepared flag & begin_lsn */
	F_SET(txnp, TXN_DIST_PREPARED);

	assert(txnp->txnid == prepare->txnid->txnid);

	/* TODO: consider collecting & storing lc-colleciton in prepared-txn */
	if (prepare->lflags & DB_TXN_SCHEMA_LOCK) {
		wrlock_schema_lk(); 
		F_SET(p, DB_DIST_SCHEMA_LK);
	}

	/* Acquire locks/pglogs from prepare record */
	//u_int32_t flags = (LOCK_GET_LIST_GETLOCK | LOCK_GET_LIST_PRINTLOCK | LOCK_GET_LIST_PREPARE);
	u_int32_t flags = (LOCK_GET_LIST_GETLOCK | LOCK_GET_LIST_PRINTLOCK);

	if ((ret = __lock_get_list(dbenv, txnp->txnid, flags, DB_LOCK_WRITE, &prepare->locks,
		&p->prepare_lsn, &p->pglogs, &p->keycnt, stdout)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error %d acquiring locks for prepare at %d:%d\n",
			__func__, ret, p->prepare_lsn.file, p->prepare_lsn.offset);
		abort();
	}

	/* Make sure new master doesn't allocate prepared genids */
	set_commit_context_prepared(prepare->genid);

	p->txnp = txnp;
	p->prev_lsn = prepare->prev_lsn;
	txnp->last_lsn = p->prepare_lsn;
	F_SET(p, DB_DIST_HAVELOCKS);

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

static int __downgrade_prepared(void *obj, void *arg)
{
	DB_ENV *dbenv = (DB_ENV *)arg;
	DB_TXN_PREPARED *p = (DB_TXN_PREPARED *)obj;
	DB_LOCKREQ request = {.op = DB_LOCK_PUT_ALL};
	int ret;

	if (!F_ISSET(p, DB_DIST_HAVELOCKS))
		return 0;

	if ((ret = __lock_vec(dbenv, p->txnp->txnid, 0, &request, 1, NULL)) != 0) {
		logmsg(LOGMSG_FATAL, "Error releasing locks dist-txn %"PRIu64", LSN %d:%d\n", p->dist_txnid,
		p->prepare_lsn.file, p->prepare_lsn.offset);
		abort();
	}
	F_CLR(p, DB_DIST_HAVELOCKS);
	if (F_ISSET(p, DB_DIST_SCHEMA_LK)) {
		unlock_schema_lk();
		F_CLR(p, DB_DIST_SCHEMA_LK);
	}
	return 0;
}

void __txn_prune_resolved_prepared(dbenv)
	DB_ENV *dbenv;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	unsigned int bkt;
	void *ent;
	DB_TXN_PREPARED *prev = NULL;
	DB_TXN_PREPARED *p = hash_first(dbenv->prepared_txn_hash, &ent, &bkt);
	while (p || prev) {
		if (prev && F_ISSET(prev, DB_DIST_ABORTED|DB_DIST_COMMITTED)) {
			hash_del(dbenv->prepared_txn_hash, prev);
			__free_prepared_txn(dbenv, prev);
		}
		prev = p;
		if (p) p = hash_next(dbenv->prepared_txn_hash, &ent, &bkt);
	}
}

/* 
 * __txn_downgrade_all_prepared --
 *
 * Release locks for all prepared txns.
 *
 * PUBLIC: int __txn_downgrade_all_prepared __P((DB_ENV *));
 */
int __txn_downgrade_all_prepared(dbenv)
	DB_ENV *dbenv;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	__txn_prune_resolved_prepared(dbenv);
	hash_for(dbenv->prepared_txn_hash, __downgrade_prepared, dbenv);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	return 0;
}

static int __downgrade_and_free_prepared(void *obj, void *arg)
{
	__downgrade_prepared(obj, arg);
	__free_prepared(obj, arg);
	return 0;
}


/*
 * __txn_downgrade_and_free_all_prepared --
 *
 * Downgrade and clear all prepared transactions
 *
 * PUBLIC: int __txn_downgrade_and_free_all_prepared __P((DB_ENV *));
 */
int __txn_downgrade_and_free_all_prepared(dbenv)
	DB_ENV *dbenv;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	hash_for(dbenv->prepared_txn_hash, __downgrade_and_free_prepared, dbenv);
	hash_clear(dbenv->prepared_txn_hash);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
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
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_LOGC *logc = NULL;
	int ret;
	if ((ret = __log_cursor(dbenv, &logc)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error getting log-cursor, %d\n", __func__, ret);
		abort();
	}
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	__txn_prune_resolved_prepared(dbenv);
	hash_for(dbenv->prepared_txn_hash, __upgrade_prepared, logc);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	logc->close(logc, 0);
	return 0;
}

/*
 * __txn_rep_abort_recovered --
 * Replicant processing for dist-abort record
 *
 * PUBLIC: int __txn_rep_abort_recovered __P((DB_ENV *, u_int64_t));
 */
int __txn_rep_abort_recovered(dbenv, dist_txnid)
	DB_ENV *dbenv;
	u_int64_t dist_txnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_TXN_PREPARED *p;
	int ret;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) == NULL) {
		logmsg(LOGMSG_FATAL, "%s unable to find dist-txn %"PRIu64" in recovered txnlist\n", 
			__func__, dist_txnid);
		abort();
	}
	hash_del(dbenv->prepared_txn_hash, p);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);

	if (F_ISSET(p, DB_DIST_HAVELOCKS) || p->txnp != NULL) {
		logmsg(LOGMSG_FATAL, "%s recovered-txn holding locks, %"PRIu64"\n", 
				__func__, dist_txnid);
		abort();
	}
	__free_prepared_txn(dbenv, p);
	return 0;
}

static int __find_txn(void *obj, void *arg)
{
	u_int32_t *txnid = (u_int32_t *)arg;
	DB_TXN_PREPARED *p = (DB_TXN_PREPARED *)obj;
	if (p->txnid == *txnid) {
#if defined (DEBUG_PREPARE)
		logmsg(LOGMSG_USER, "%s found prepared %u returning 1\n", __func__, p->txnid);
#endif
		return 1;
	}
	for (int i = 0; i < p->numchildren; i++) {
		if (p->children[i] == *txnid) {
#if defined (DEBUG_PREPARE)
			logmsg(LOGMSG_USER, "%s found prepared child %u returning 1\n", __func__, p->txnid);
#endif
			return 1;
		}
	}
	return 0;
}

int __txn_is_prepared(dbenv, txnid)
	DB_ENV *dbenv;
	u_int32_t txnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	int found;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	found = hash_for(dbenv->prepared_txn_hash, __find_txn, &txnid);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	return found;
}

int __txn_abort_recovered(dbenv, dist_txnid)
	DB_ENV *dbenv;
	u_int64_t dist_txnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_TXN_PREPARED *p;
	int ret;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		if (!F_ISSET(p, DB_DIST_HAVELOCKS) || p->txnp == NULL) {
			logmsg(LOGMSG_INFO, "%s unable to abort unprepared dist-txn %"PRIu64"\n", __func__, dist_txnid);
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

	if ((ret = __txn_dist_abort_log(dbenv, p->txnp, &p->txnp->last_lsn, 0, TXN_COMMIT, p->dist_txnid)) != 0) {
		logmsg(LOGMSG_FATAL, "Error writing dist-abort for txn %"PRIu64", LSN %d:%d\n", p->dist_txnid,
			p->prev_lsn.file, p->prev_lsn.offset);
		abort();
	}

	DB_LOCKREQ request = {.op = DB_LOCK_PUT_ALL};

	if ((ret = __lock_vec(dbenv, p->txnp->txnid, 0, &request, 1, NULL)) != 0) {
		logmsg(LOGMSG_FATAL, "Error releasing locks dist-txn %"PRIu64", LSN %d:%d\n", p->dist_txnid,
		p->prepare_lsn.file, p->prepare_lsn.offset);
		abort();
	}

	F_CLR(p, DB_DIST_HAVELOCKS);
	if (F_ISSET(p, DB_DIST_SCHEMA_LK)) {
		unlock_schema_lk();
		F_CLR(p, DB_DIST_SCHEMA_LK);
	}
	__free_prepared_txn(dbenv, p);
	return 0;
}

extern int bdb_transfer_pglogs_to_queues(void *bdb_state, void *pglogs,
		unsigned int nkeys, int is_logical_commit,
		unsigned long long logical_tranid, DB_LSN logical_commit_lsn, uint32_t gen,
		int32_t timestamp, unsigned long long context);

extern int __rep_lsn_cmp __P((const void *, const void *));

int __txn_rep_discard_recovered(dbenv, dist_txnid)
	DB_ENV *dbenv;
	u_int64_t dist_txnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_TXN_PREPARED *p;
	int ret;

	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) == NULL) {
		logmsg(LOGMSG_FATAL, "%s unable to find dist-txn %"PRIu64" in recovered txnlist\n", 
			__func__, dist_txnid);
		abort();
	}
	hash_del(dbenv->prepared_txn_hash, p);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);

	if (F_ISSET(p, DB_DIST_HAVELOCKS) || p->txnp != NULL) {
		logmsg(LOGMSG_FATAL, "%s recovered txnsaction holding locks, %"PRIu64"\n", 
			__func__, dist_txnid);
		abort();
	}
	__free_prepared_txn(dbenv, p);
	return 0;
}

static int __lowest_prepared_lsn(void *obj, void *arg)
{
	DB_TXN_PREPARED *p = (DB_TXN_PREPARED *)obj;
	DB_LSN *lsn = (DB_LSN *)arg;
	if (IS_ZERO_LSN(*lsn) || log_compare(&p->begin_lsn, lsn) < 0) {
		*lsn = p->begin_lsn;
	}
	return 0;
}

int __txn_lowest_prepared_lsn(dbenv, lsn)
	DB_ENV *dbenv;
	DB_LSN *lsn;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_LSN local_lsn = {0};
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	hash_for(dbenv->prepared_txn_hash, __lowest_prepared_lsn, &local_lsn);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	*lsn = local_lsn;
#if defined (DEBUG_PREPARE)
	logmsg(LOGMSG_USER, "%s returns [%d:%d]\n", __func__, lsn->file, lsn->offset);
#endif
	return 0;
}

struct parent_child {
	DB_ENV *dbenv;
	u_int32_t ptxnid;
	u_int32_t ctxnid;
};

static void add_child(DB_ENV *dbenv, DB_TXN_PREPARED *p, u_int32_t ctxnid)
{
	int ret = 0;
	for (int i = 0; i < p->numchildren; i++) {
		if (p->children[i] == ctxnid) {
#if defined (DEBUG_PREPARE)
			logmsg(LOGMSG_USER, "%s prepared child %u already in child-list\n", __func__, ctxnid);
#endif
			return;
		}
	}
	if (p->numchildren + 1 > p->childrensz) {
		p->childrensz = p->childrensz ? p->childrensz * 2 : 16;
		if ((ret = __os_realloc(dbenv, p->childrensz * sizeof(u_int32_t),
			&p->children)) != 0) {
			logmsg(LOGMSG_FATAL, "Error realloc'ing children array, %d\n", ret);
			abort();
		}
	}
#if defined (DEBUG_PREPARE)
	logmsg(LOGMSG_USER, "%s adding prepared child %u to child-list for %"PRIu64"\n",
		__func__, ctxnid, p->dist_txnid);
#endif
	p->children[p->numchildren++] = ctxnid;
}

static int __add_prepared_child(void *obj, void *arg)
{
	struct parent_child *pc = (struct parent_child *)arg;
	DB_TXN_PREPARED *p = (DB_TXN_PREPARED *)obj;
	if (p->txnid == pc->ptxnid && !F_ISSET(p, DB_DIST_ABORTED)) {
#if defined (DEBUG_PREPARE)
		comdb2_cheapstack_sym(stderr, "add_prepared_child");
#endif
		add_child(pc->dbenv, p, pc->ctxnid);
		return 1;
	}
	return 0;
}

int __txn_add_prepared_child(dbenv, ctxnid, ptxnid)
	DB_ENV *dbenv;
	u_int32_t ctxnid;
	u_int32_t ptxnid;
{
	struct parent_child pc = { .dbenv = dbenv, .ptxnid = ptxnid, .ctxnid = ctxnid };
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	hash_for(dbenv->prepared_txn_hash, __add_prepared_child, &pc);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	return 0;
}

int __txn_discard_recovered(dbenv, dist_txnid)
	DB_ENV *dbenv;
	u_int64_t dist_txnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_TXN_PREPARED *p;
	int ret;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		hash_del(dbenv->prepared_txn_hash, p);
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	if (p == NULL) {
		logmsg(LOGMSG_INFO, "%s unable to locate txnid %"PRIu64"\n", __func__, dist_txnid);
		return -1;
	}

	if (F_ISSET(p, DB_DIST_HAVELOCKS)) {
		DB_LOCKREQ request = {.op = DB_LOCK_PUT_ALL};

		if ((ret = __lock_vec(dbenv, p->txnp->txnid, 0, &request, 1, NULL)) != 0) {
			logmsg(LOGMSG_FATAL, "Error releasing locks dist-txn %"PRIu64", LSN %d:%d\n", p->dist_txnid,
					p->prepare_lsn.file, p->prepare_lsn.offset);
			abort();
		}

		F_CLR(p, DB_DIST_HAVELOCKS);
		if (F_ISSET(p, DB_DIST_SCHEMA_LK)) {
			unlock_schema_lk();
			F_CLR(p, DB_DIST_SCHEMA_LK);
		}
	}

	__free_prepared_txn(dbenv, p);
	return 0;
}

int __txn_commit_recovered(dbenv, dist_txnid)
	DB_ENV *dbenv;
	u_int64_t dist_txnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_TXN_PREPARED *p;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		if (!F_ISSET(p, DB_DIST_HAVELOCKS)) {
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

	/* Write the commit record now to get commit-context */
	u_int64_t context = 0;
	int32_t timestamp = comdb2_time_epoch();
	u_int32_t gen = 0;
	u_int32_t lflags = DB_LOG_COMMIT | DB_LOG_PERM;
	DB_LSN lsn_out;
	DB_REP *db_rep = dbenv->rep_handle;
	REP *rep = db_rep->region;
	int ret;

	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	gen = rep->gen;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

	if ((ret = __txn_dist_commit_log(dbenv, p->txnp, &lsn_out, &context, lflags, TXN_COMMIT,
			p->dist_txnid, gen, timestamp, NULL)) != 0) {
		logmsg(LOGMSG_FATAL, "Error writing dist-commit for txn %"PRIu64", LSN %d:%d\n", p->dist_txnid,
			p->prev_lsn.file, p->prev_lsn.offset);
		abort();
	}

	assert(context != 0);

	bdb_transfer_pglogs_to_queues(dbenv->app_private, p->pglogs, p->keycnt, 0,
		0, lsn_out, gen, timestamp, context);

	/* Collect txn, roll-forward, call txn_commit */
	LSN_COLLECTION lc = {0};
	DBT data_dbt = {0};
	DB_LOGC *logc = NULL;
	int had_serializable_records = 0;
	uint32_t rectype = 0;
	void *txninfo = NULL;

	if ((ret = __rep_collect_txn(dbenv, &p->prepare_lsn, &lc, &had_serializable_records, NULL)) != 0) {
		logmsg(LOGMSG_FATAL, "Error collecting dist-txn %"PRIu64", LSN %d:%d\n", p->dist_txnid,
		p->prepare_lsn.file, p->prepare_lsn.offset);
		abort();
	}

	qsort(lc.array, lc.nlsns, sizeof(struct logrecord), __rep_lsn_cmp);

	if ((ret = __db_txnlist_init(dbenv, 0, 0, NULL, &txninfo)) != 0) {
		logmsg(LOGMSG_FATAL, "Error initing txnlist, %d\n", ret);
		abort();
	}

	if ((ret = __log_cursor(dbenv, &logc)) != 0) {
		logmsg(LOGMSG_FATAL, "Error getting log-cursor, %d\n", ret);
		abort();
	}

	F_SET(&data_dbt, DB_DBT_REALLOC);

	for (int i = 0; i < lc.nlsns; i++) {
		DBT lcin_dbt = { 0 };
		uint32_t rectype = 0;
		DB_LSN lsn = lc.array[i].lsn, *lsnp = &lsn;

		if ((ret = __log_c_get(logc, lsnp, &data_dbt, DB_SET)) != 0) {
			logmsg(LOGMSG_FATAL, "Error getting %d:%d for prepared txn, %d\n",
				lsnp->file, lsnp->offset, ret);
			abort();
		}
		assert(data_dbt.size >= sizeof(uint32_t));
		LOGCOPY_32(&rectype, data_dbt.data);
		data_dbt.app_data = &context;

		if ((ret = __db_dispatch(dbenv, dbenv->recover_dtab, dbenv->recover_dtab_size,
				&data_dbt, lsnp, DB_TXN_APPLY, txninfo)) != 0) {
			logmsg(LOGMSG_FATAL, "Error applying %d:%d for prepared txn, %d\n",
				lsnp->file, lsnp->offset, ret);
			abort();
		}
	}

	DB_LOCKREQ request = {.op = DB_LOCK_PUT_ALL};

	if ((ret = __lock_vec(dbenv, p->txnp->txnid, 0, &request, 1, NULL)) != 0) {
		logmsg(LOGMSG_FATAL, "Error releasing locks dist-txn %"PRIu64", LSN %d:%d\n", p->dist_txnid,
		p->prepare_lsn.file, p->prepare_lsn.offset);
		abort();
	}
	F_CLR(p, DB_DIST_HAVELOCKS);

	if (F_ISSET(p, DB_DIST_SCHEMA_LK)) {
		unlock_schema_lk();
		F_CLR(p, DB_DIST_SCHEMA_LK);
	}

	__free_prepared_txn(dbenv, p);
	return 0;
}
