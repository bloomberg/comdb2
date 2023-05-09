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

/*
 * __txn_commit_map_init --
 * 	Initialize commit LSN map.
 *
 * PUBLIC: int __txn_commit_map_init
 * PUBLIC:     __P((DB_ENV *));
 */
int __txn_commit_map_init(dbenv) 
	DB_ENV *dbenv;
{
	int ret;
	DB_TXN_COMMIT_MAP *txmap;

	ret = __os_calloc(dbenv, 1, sizeof(DB_TXN_COMMIT_MAP), &txmap);

	if (ret) {
		goto err;
	}

	txmap->transactions = hash_init_o(offsetof(UTXNID_TRACK, utxnid), sizeof(u_int64_t));
	txmap->logfile_lists = hash_init_o(offsetof(LOGFILE_TXN_LIST, file_num), sizeof(u_int32_t));

	if (txmap->transactions == NULL || txmap->logfile_lists == NULL) {
		ret = ENOMEM;
		goto err;
	}

	Pthread_mutex_init(&txmap->txmap_mutexp, NULL);
	dbenv->txmap = txmap;

	return 0;
err:
	logmsg(LOGMSG_ERROR, "Failed to initialize commit LSN map\n");
	return ret;
}

static int free_logfile_list_elt(obj, arg)
        void *obj;
        void *arg;
{
        LOGFILE_TXN_LIST *llist;
        UTXNID *elt, *tmp_elt;
        DB_ENV *dbenv;

        llist = (LOGFILE_TXN_LIST *) obj;
        dbenv = (DB_ENV *) arg;

        LISTC_FOR_EACH_SAFE(&llist->commit_utxnids, elt, tmp_elt, lnk)
        {
                __os_free(dbenv, elt);
        }
	__os_free(dbenv, llist);
        return 0;
}

static int free_transactions(obj, arg)
        void *obj;
        void *arg;
{
        UTXNID_TRACK *elt;
        DB_ENV *dbenv;

        elt = (UTXNID_TRACK *) obj;
        dbenv = (DB_ENV *) arg;

        __os_free(dbenv, elt);
        return 0;
}

/*
 * __commit_map_destroy --
 *      Destroy commit LSN map.
 *
 * PUBLIC: int __commit_map_destroy
 * PUBLIC:     __P((DB_ENV *));
 */
int __txn_commit_map_destroy(dbenv)
        DB_ENV *dbenv;
{
        if (dbenv->txmap) {
                hash_for(dbenv->txmap->transactions, &free_transactions, (void *) dbenv);
                hash_clear(dbenv->txmap->transactions);
                hash_free(dbenv->txmap->transactions);

                hash_for(dbenv->txmap->logfile_lists, &free_logfile_list_elt, (void *) dbenv);
                hash_clear(dbenv->txmap->logfile_lists);
                hash_free(dbenv->txmap->logfile_lists);

                Pthread_mutex_destroy(&dbenv->txmap->txmap_mutexp);
                __os_free(dbenv, dbenv->txmap);
        }

        return 0;
}

/*
 * __txn_commit_map_remove_nolock --
 *  Remove a transaction from the commit LSN map without locking.
 *
 * PUBLIC: static int __txn_commit_map_remove_nolock
 * PUBLIC:     __P((DB_ENV *, u_int64_t));
 */
static int __txn_commit_map_remove_nolock(dbenv, utxnid)
	DB_ENV *dbenv;
	u_int64_t utxnid;
{
	DB_TXN_COMMIT_MAP *txmap;
	UTXNID_TRACK *txn;
	int ret;

	txmap = dbenv->txmap;
	ret = 0;

	txn = hash_find(txmap->transactions, &utxnid);

	if (txn) {
		hash_del(txmap->transactions, txn);
		__os_free(dbenv, txn); 
	} else {
		ret = 1;
	}

	return ret;
}

/*
 * __txn_commit_map_delete_logfile_txns --
 *  Remove all transactions that committed in a specific logfile 
 *  from the commit LSN map.	
 *
 * PUBLIC: int __txn_commit_map_delete_logfile_txns
 * PUBLIC:     __P((DB_ENV *, u_int32_t));
 */
int __txn_commit_map_delete_logfile_txns(dbenv, del_log) 
	DB_ENV *dbenv;
	u_int32_t del_log;
{
	DB_TXN_COMMIT_MAP *txmap;
	LOGFILE_TXN_LIST *to_delete;
	UTXNID *elt, *tmpp;
	int ret;

	txmap = dbenv->txmap;
	to_delete = hash_find(txmap->logfile_lists, &del_log);
	ret = 0;

	Pthread_mutex_lock(&txmap->txmap_mutexp);

	if (to_delete) {
		LISTC_FOR_EACH_SAFE(&to_delete->commit_utxnids, elt, tmpp, lnk)
		{
			__txn_commit_map_remove_nolock(dbenv, elt->utxnid);
			__os_free(dbenv, elt);
		}

		hash_del(txmap->logfile_lists, &del_log);
		__os_free(dbenv, to_delete);
	} else {
		ret = 1;
	}

	Pthread_mutex_unlock(&txmap->txmap_mutexp);
	return ret;
}
 
/*
 * __txn_commit_map_get --
 *  Get the commit LSN of a transaction.
 *
 * PUBLIC: int __txn_commit_map_get
 * PUBLIC:     __P((DB_ENV *, u_int64_t, DB_LSN*));
 */
int __txn_commit_map_get(dbenv, utxnid, commit_lsn) 
	DB_ENV *dbenv;
	u_int64_t utxnid;
	DB_LSN *commit_lsn;
{
	DB_TXN_COMMIT_MAP *txmap;
	UTXNID_TRACK *txn;
	int ret;
   
	txmap = dbenv->txmap;
	ret = 0;

	Pthread_mutex_lock(&txmap->txmap_mutexp);
	txn = hash_find(txmap->transactions, &utxnid);

	if (txn == NULL) {
		ret = DB_NOTFOUND;
	} else {
		*commit_lsn = txn->commit_lsn;
	}

	Pthread_mutex_unlock(&txmap->txmap_mutexp);
	return ret;
}

/*
 * __txn_commit_map_add --
 *  Store the commit LSN of a transaction.
 *
 * PUBLIC: int __txn_commit_map_add
 * PUBLIC:     __P((DB_ENV *, u_int64_t, DB_LSN));
 */
int __txn_commit_map_add(dbenv, utxnid, commit_lsn) 
	DB_ENV *dbenv;
	u_int64_t utxnid;
	DB_LSN commit_lsn;
{
	DB_TXN_COMMIT_MAP *txmap;
	LOGFILE_TXN_LIST *to_delete;
	UTXNID_TRACK *txn;
	UTXNID* elt;
	int ret, alloc_txn, alloc_delete_list;

	txmap = dbenv->txmap;
	alloc_txn = 0;
	alloc_delete_list = 0;
	ret = 0;

	/* Don't add transactions that commit at the zero LSN */
	if (IS_ZERO_LSN(commit_lsn)) {
		return ret;
	}

	Pthread_mutex_lock(&txmap->txmap_mutexp);

	txn = hash_find(txmap->transactions, &utxnid);

	if (txn != NULL) { 
		/* Don't add transactions that already exist in the map */
		Pthread_mutex_unlock(&txmap->txmap_mutexp);
		return ret;
	}

	to_delete = hash_find(txmap->logfile_lists, &commit_lsn.file);

	if (!to_delete) {
		if ((ret = __os_malloc(dbenv, sizeof(LOGFILE_TXN_LIST), &to_delete) != 0)) {
			ret = ENOMEM;
			goto err;
		} else {
			alloc_delete_list = 1;
		}
	}

        if ((ret = __os_malloc(dbenv, sizeof(UTXNID_TRACK), &txn)) != 0) {
                ret = ENOMEM;
                goto err;
        }
        alloc_txn = 1;

        if ((ret = __os_malloc(dbenv, sizeof(UTXNID), &elt)) != 0) {
                ret = ENOMEM;
                goto err;
        }

        if (alloc_delete_list) {
                to_delete->file_num = commit_lsn.file;
                listc_init(&to_delete->commit_utxnids, offsetof(UTXNID, lnk));
                hash_add(txmap->logfile_lists, to_delete);
        }

        txn->utxnid = utxnid;
        txn->commit_lsn = commit_lsn;
        hash_add(txmap->transactions, txn);

        elt->utxnid = utxnid;
        listc_atl(&to_delete->commit_utxnids, elt);

        Pthread_mutex_unlock(&txmap->txmap_mutexp);
        return ret;
err:
        Pthread_mutex_unlock(&txmap->txmap_mutexp);
        if (alloc_delete_list) {
                __os_free(dbenv, to_delete);
        }
        if (alloc_txn) {
                __os_free(dbenv, txn);
        }
        return ret;
}

