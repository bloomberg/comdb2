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

/* TODO: callback funcs rather than layer-violation */
int osql_blkseq_register_cnonce(void *cnonce, int len);
int osql_blkseq_unregister_cnonce(void *cnonce, int len);
int dist_txn_abort_write_blkseq(void *bdb_state, void *bskey, int bskeylen);

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

	LSN_NOT_LOGGED(txmap->highest_checkpoint_lsn);
	LSN_NOT_LOGGED(txmap->highest_commit_lsn);
	txmap->smallest_logfile = -1;
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
 * __txn_commit_map_get_highest_checkpoint_lsn --
 *  Get the highest checkpoint lsn
 *  from the commit LSN map. If `lock` neq 0 then will acquire lock over data access.
 *
 * PUBLIC: int __txn_commit_map_get_highest_checkpoint_lsn
 * PUBLIC:     __P((DB_ENV *, DB_LSN *, int));
 */
int __txn_commit_map_get_highest_checkpoint_lsn(dbenv, highest_checkpoint_lsn, lock)
	DB_ENV *dbenv;
	DB_LSN *highest_checkpoint_lsn;
	int lock;
{
	DB_TXN_COMMIT_MAP *txmap;

	txmap = dbenv->txmap;

	if (lock) { Pthread_mutex_lock(&txmap->txmap_mutexp); }

	*highest_checkpoint_lsn = txmap->highest_checkpoint_lsn;

	if (lock) { Pthread_mutex_unlock(&txmap->txmap_mutexp); }
	return 0;
}

/*
 * __txn_commit_map_get_highest_commit_lsn --
 *  Get the highest commit lsn
 *  from the commit LSN map. If `lock` neq 0 then will acquire lock over data access.
 *
 * PUBLIC: int __txn_commit_map_get_highest_commit_lsn
 * PUBLIC:     __P((DB_ENV *, DB_LSN *, int));
 */
int __txn_commit_map_get_highest_commit_lsn(dbenv, highest_commit_lsn, lock)
	DB_ENV *dbenv;
	DB_LSN *highest_commit_lsn;
	int lock;
{
	DB_TXN_COMMIT_MAP *txmap;

	txmap = dbenv->txmap;

	if (lock) { Pthread_mutex_lock(&txmap->txmap_mutexp); }

	*highest_commit_lsn = txmap->highest_commit_lsn;

	if (lock) { Pthread_mutex_unlock(&txmap->txmap_mutexp); }
	return 0;
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
	LOGFILE_TXN_LIST *to_delete, *successor;
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

		if (log_compare(&txmap->highest_commit_lsn, &to_delete->highest_commit_lsn) == 0) {
			// We are deleting the highest logfile represented in our map. 
			// Find the next highest logfile to set highest_commit_lsn.
			for (int prev_log=del_log-1; prev_log >= 0; --prev_log) {
				successor = hash_find(txmap->logfile_lists, &prev_log);
				if (successor) {
					txmap->highest_commit_lsn = successor->highest_commit_lsn;
					break;
				} else if (prev_log == 0) {
					// If we didn't find a logfile between the highest one and the first one,
					// then our map no longer contains entries.
					ZERO_LSN(txmap->highest_commit_lsn);
				}
			}
		}

		if (del_log == txmap->smallest_logfile) {
			// We are deleting the smallest logfile. Find the next smallest.
			do {
				txmap->smallest_logfile++;
				successor = hash_find(txmap->logfile_lists, &txmap->smallest_logfile);
			} while (!successor);
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
 * __txn_commit_map_add_nolock --
 *  Store the commit LSN of a transaction.
 *
 * PUBLIC: int __txn_commit_map_add_nolock
 * PUBLIC:     __P((DB_ENV *, u_int64_t, DB_LSN));
 */
int __txn_commit_map_add_nolock(dbenv, utxnid, commit_lsn) 
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

	txn = hash_find(txmap->transactions, &utxnid);

	if (txn != NULL) { 
		/* Don't add transactions that already exist in the map */
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

            if (commit_lsn.file < txmap->smallest_logfile || txmap->smallest_logfile == -1) {
                txmap->smallest_logfile = commit_lsn.file;
            }
        }

        if (log_compare(&txmap->highest_commit_lsn, &commit_lsn) <= 0) {
            txmap->highest_commit_lsn = commit_lsn;
        }

        txn->utxnid = utxnid;
        txn->commit_lsn = commit_lsn;
        hash_add(txmap->transactions, txn);

        elt->utxnid = utxnid;
        listc_atl(&to_delete->commit_utxnids, elt);
		
		return ret;
err:
	if (alloc_delete_list) {
		__os_free(dbenv, to_delete);
	}
	if (alloc_txn) {
		__os_free(dbenv, txn);
	}
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
	int ret;

	ret = 0;

	Pthread_mutex_lock(&dbenv->txmap->txmap_mutexp);

	ret = __txn_commit_map_add_nolock(dbenv, utxnid, commit_lsn);

	Pthread_mutex_unlock(&dbenv->txmap->txmap_mutexp);
	return ret;
}

static inline void __free_prepared_children(DB_ENV *dbenv, DB_TXN_PREPARED *p)
{
	struct __db_txn_prepared_child *c;
	while ((c = listc_rtl(&p->children)) != NULL) {
		hash_del(dbenv->prepared_children, c);
		__os_free(dbenv, c);
	}
}

static inline void __free_prepared_txn(DB_ENV *dbenv, DB_TXN_PREPARED *p)
{
	assert(!F_ISSET(p, DB_DIST_HAVELOCKS));
	if (p->dist_txnid != NULL)
		__os_free(dbenv, p->dist_txnid);
	if (p->blkseq_key.data != NULL)
		__os_free(dbenv, p->blkseq_key.data);
	if (p->coordinator_name.data != NULL)
		__os_free(dbenv, p->coordinator_name.data);
	if (p->coordinator_tier.data != NULL)
		__os_free(dbenv, p->coordinator_tier.data);
	if (p->txnp)
		__txn_free_recovered(p->txnp);
	if (p->pglogs)
		free(p->pglogs);
	__os_free(dbenv, p);
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
	normalize_rectype(&type);
	assert(type == DB___txn_dist_prepare);

	if ((ret = __txn_dist_prepare_read(dbenv, dbt.data, &prepare)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error %d unpacking prepare record at %d:%d\n",
			__func__, ret, p->prepare_lsn.file, p->prepare_lsn.offset);
		abort();
	}

	/* Use the txnid in the log */
	struct txn_properties prop = {.prepared_txnid = prepare->txnid->txnid,
								  .prepared_utxnid = prepare->txnid->utxnid,
								  .begin_lsn = prepare->begin_lsn };

	if ((ret = __txn_begin_with_prop(dbenv, NULL, &txnp, 0, &prop)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error %d creating txn at %d:%d\n",
			__func__, ret, p->prepare_lsn.file, p->prepare_lsn.offset);
		abort();
	}

	/* Set the prepared flag & begin_lsn */
	F_SET(txnp, TXN_DIST_PREPARED);

	assert(txnp->txnid == prepare->txnid->txnid);
	assert(txnp->utxnid == prepare->txnid->utxnid);

    /* Halt replays in the block-processor */
    osql_blkseq_register_cnonce(p->blkseq_key.data, p->blkseq_key.size);

	if (prepare->lflags & DB_TXN_SCHEMA_LOCK) {
		wrlock_schema_lk(); 
		F_SET(p, DB_DIST_SCHEMA_LK);
	}

	if (prepare->lflags & DB_TXN_DIST_UPD_SHADOWS) {
		F_SET(p, DB_DIST_UPDSHADOWS);
	}

	/* Acquire locks/pglogs from prepare record */
	u_int32_t flags = (LOCK_GET_LIST_GETLOCK | LOCK_GET_LIST_PREPARE);

	if ((ret = __lock_get_list(dbenv, txnp->txnid, flags, DB_LOCK_WRITE, &prepare->locks,
		&p->prepare_lsn, &p->pglogs, &p->keycnt, stdout)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error %d acquiring locks for prepare at %d:%d\n",
			__func__, ret, p->prepare_lsn.file, p->prepare_lsn.offset);
		abort();
	}

	/* Make sure new master doesn't allocate prepared genids.  
	 * TODO: this function produces alot of noise if genid is already higher: 
	 * make a quiet version */
	set_commit_context_prepared(prepare->genid);

	p->txnp = txnp;
	p->prev_lsn = prepare->prev_lsn;
	txnp->last_lsn = p->prepare_lsn;
	F_SET(p, DB_DIST_HAVELOCKS);
	logmsg(LOGMSG_INFO, "Upgraded prepared txn %s\n", p->dist_txnid);

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

static int __downgrade_prepared_int(DB_ENV *dbenv, DB_TXN_PREPARED *p)
{
	DB_LOCKREQ request = {.op = DB_LOCK_PUT_ALL};
	int ret;

	if (!F_ISSET(p, DB_DIST_HAVELOCKS))
		return 0;

    /* Unregister cnonce */
    osql_blkseq_unregister_cnonce(p->blkseq_key.data, p->blkseq_key.size);

	if ((ret = __lock_vec(dbenv, p->txnp->txnid, 0, &request, 1, NULL)) != 0) {
		logmsg(LOGMSG_FATAL, "Error releasing locks dist-txn %s, LSN %d:%d\n", p->dist_txnid,
		p->prepare_lsn.file, p->prepare_lsn.offset);
		abort();
	}

	__txn_free_recovered(p->txnp);
	p->txnp = NULL;

	F_CLR(p, DB_DIST_HAVELOCKS);
	if (F_ISSET(p, DB_DIST_SCHEMA_LK)) {
		unlock_schema_lk();
		F_CLR(p, DB_DIST_SCHEMA_LK);
	}

	return 0;
}

static int __downgrade_prepared(void *obj, void *arg)
{
	DB_ENV *dbenv = (DB_ENV *)arg;
	DB_TXN_PREPARED *p = (DB_TXN_PREPARED *)obj;
	return __downgrade_prepared_int(dbenv, p);
}

/*
 * __txn_is_dist_committed --
 *
 * Add dist_txn to prepared-txn hash as a committed txn.  As a prepared 
 * transaction may have been committed by a different master, this requires
 * special handling in recovery (see the DB_TXN_DIST_ADD_TXNLIST case).
 *
 * PUBLIC: int __txn_is_dist_committed __P((DB_ENV *, const char *dist_txnid));
 */
int __txn_is_dist_committed(dbenv, dist_txnid)
	DB_ENV *dbenv;
	const char *dist_txnid;
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
 * We allocate a dist_txnid structure and in dist_abort as a place holders so that
 * recovery can discriminate between resolved and unresolved prepares.  We mark these
 * with either the DIST_COMMITTED or DIST_ABORTED flag.  After recovery all resolved
 * prepares will pruned.
 *
 * PUBLIC: int __txn_recover_dist_commit __P((DB_ENV *, const char *dist_txnid));
 */
int __txn_recover_dist_commit(dbenv, dist_txnid)
	DB_ENV *dbenv;
	const char *dist_txnid;
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
		if ((ret = __os_calloc(dbenv, 1, strlen(dist_txnid) + 1, &p->dist_txnid)) != 0) {
			__os_free(dbenv, p);
			return (ret);
		}
		memcpy(p->dist_txnid, dist_txnid, strlen(dist_txnid));
		hash_add(dbenv->prepared_txn_hash, p);
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	F_SET(p, DB_DIST_COMMITTED);
#if defined (DEBUG_PREPARE)
	logmsg(LOGMSG_USER, "%s adding committed prepare dist-txnid %s to list\n",
		__func__, dist_txnid);
#endif
	return 0;
}

/*
 * __txn_recover_dist_abort --
 *
 * Add dist_txn to prepared-txn hash as an aborted txn.  This is similar to the 
 * dist-committed function above.  This will be removed when we undo the prepare
 * record, or when the prepared-txn list is pruned.  This is used only in 
 * recovery.  It ignores blkseq, because that is added from bdb_recover_blkseq 
 * in a separate pass.
 *
 * PUBLIC: int __txn_recover_dist_abort __P((DB_ENV *, const char *dist_txnid));
 */
int __txn_recover_dist_abort(dbenv, dist_txnid)
	DB_ENV *dbenv;
	const char *dist_txnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	int ret;
	DB_TXN_PREPARED *p, *fnd;

	if ((ret = __os_calloc(dbenv, 1, sizeof(DB_TXN_PREPARED), &p)) != 0) {
		return (ret);
	}
	if ((ret = __os_calloc(dbenv, 1, strlen(dist_txnid) + 1, &p->dist_txnid)) != 0) {
		__os_free(dbenv, p);
		return (ret);
	}

	memcpy(p->dist_txnid, dist_txnid, strlen(dist_txnid));
	F_SET(p, DB_DIST_ABORTED);
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((fnd = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		logmsg(LOGMSG_FATAL, "Recovery found multiple prepared records with dist-txnid %s\n",
			dist_txnid);
		abort();
	}
	hash_add(dbenv->prepared_txn_hash, p);
#if defined (DEBUG_PREPARE)
	logmsg(LOGMSG_USER, "%s adding aborted prepare dist-txnid %s to list\n",
		__func__, dist_txnid);
#endif
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	return 0;
}

/*
 * __txn_master_prepared --
 *
 * Master has prepared a transaction and written a prepare record.  Add this 
 * prepare to the prepared transaction list.
 *
 * PUBLIC: int __txn_master_prepared __P((DB_ENV *,
 * PUBLIC:	  const char *, DB_LSN *, DBT *, u_int32_t, DBT *, DBT *));
 */
int __txn_master_prepared(dbenv, dist_txnid, prep_lsn, begin_lsn, blkseq_key, coordinator_gen,
		coordinator_name, coordinator_tier)
	DB_ENV *dbenv;
	const char *dist_txnid;
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
		logmsg(LOGMSG_FATAL, "master-prepared txn on dist-list, dist-txnid %s\n",
				dist_txnid);
		abort();
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);

	if ((ret = __os_calloc(dbenv, 1, sizeof(DB_TXN_PREPARED), &p)) != 0) {
		return (ret);
	}
	if ((ret = __os_calloc(dbenv, 1, strlen(dist_txnid) + 1, &p->dist_txnid)) != 0) {
		__os_free(dbenv, p);
		return (ret);
	}

	memcpy(p->dist_txnid, dist_txnid, strlen(dist_txnid));
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
		logmsg(LOGMSG_FATAL, "Recovery found multiple prepared records with dist-txnid %s\n",
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
 * This is db_dispatch with the DB_TXN_ABORT flag- it's called both for a normal 
 * txn->abort, and from txn_dbenv_refresh.  This later could abort a recovered
 * prepare.
 *
 * PUBLIC: int __txn_recover_abort_prepared __P((DB_ENV *,
 * PUBLIC:	  u_int64_t, DB_LSN *, DBT *, u_int32_t, DBT *, DBT *));
 */
int __txn_recover_abort_prepared(dbenv, dist_txnid, prep_lsn, blkseq_key, coordinator_gen,
		coordinator_name, coordinator_tier)
	DB_ENV *dbenv;
	const char *dist_txnid;
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
		logmsg(LOGMSG_FATAL, "%s cannot find prepared transaction %s\n", 
			__func__, dist_txnid);
		abort();
	}
	hash_del(dbenv->prepared_txn_hash, p);
	hash_del(dbenv->prepared_utxnid_hash, p);
	__free_prepared_children(dbenv, p);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);

	if (F_ISSET(p, DB_DIST_RECOVERED) && F_ISSET(p, DB_DIST_HAVELOCKS)) {
		__downgrade_prepared_int(dbenv, p);
	}

	__free_prepared_txn(dbenv, p);
	return 0;
}

/*
 * __txn_recover_prepared --
 *
 * Collect prepared transactions during recovery's backward-pass.  We should
 * only run this for aborted and unresolved dist-txns.  There are two cases
 * that will be handled in this function: 
 *
 * First, if this transaction has a dist-commit record, then this should
 * already be in the prepared-txn hash with the DB_DIST_COMMIT flag lit.  In
 * fact it's added to the hash specifically to differentiate between a 
 * committed-dist-txnid and an unresolved dist-txns.
 *
 * Second, if this transaction is not in the prepared-txn hash, then this is
 * an unresolved prepare.  If this node is elected master, it will need to
 * find out the fate of this prepare from the coordinator.
 *
 * dist-abort code should never run this function- we treat this as a normal
 * abort, except that we update the transaction's blkseq record.  This is 
 * handled in bdb_recover_blkseq.
 *
 * PUBLIC: int __txn_recover_prepared __P((DB_ENV *, DB_TXN *txnid,
 * PUBLIC:	  u_int64_t, DB_LSN *, DB_LSN *, DBT *, u_int32_t, DBT *, DBT *));
 */
int __txn_recover_prepared(dbenv, txnid, dist_txnid, prep_lsn, begin_lsn, blkseq_key,
		coordinator_gen, coordinator_name, coordinator_tier)
	DB_ENV *dbenv;
	DB_TXN *txnid;
	const char *dist_txnid;
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
			logmsg(LOGMSG_FATAL, "Non-aborted prepared record, dist-txnid %s\n",
				dist_txnid);
			abort();
		}
		hash_del(dbenv->prepared_txn_hash, p);
		hash_del(dbenv->prepared_utxnid_hash, p);
		__free_prepared_children(dbenv, p);
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	if (p) {
		/* 'aborted-from-recovery': we assert that ABORTED flag is lit above:
		   !!! TODO !!! need to write dist-abort to blkseq  */
#if defined (DEBUG_PREPARE)
		logmsg(LOGMSG_USER, "Removing aborted prepare %s from recovered txn list\n", dist_txnid);
#endif
		__free_prepared_txn(dbenv, p);
		return (0);
	}

	if ((ret = __os_calloc(dbenv, 1, sizeof(DB_TXN_PREPARED), &p)) != 0) {
		return (ret);
	}
	listc_init(&p->children, offsetof(struct __db_txn_prepared_child, lnk));
	if ((ret = __os_calloc(dbenv, 1, strlen(dist_txnid) + 1, &p->dist_txnid)) != 0) {
		__os_free(dbenv, p);
		return (ret);
	}

	memcpy(p->dist_txnid, dist_txnid, strlen(dist_txnid));;
	p->prepare_lsn = *prep_lsn;
	p->begin_lsn = *begin_lsn;
	p->utxnid = txnid->utxnid;

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
		logmsg(LOGMSG_FATAL, "Recovery found multiple prepared records with dist-txnid %s\n",
			dist_txnid);
		abort();
	}
	F_SET(p, DB_DIST_RECOVERED);

	hash_add(dbenv->prepared_txn_hash, p);
	hash_add(dbenv->prepared_utxnid_hash, p);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	logmsg(LOGMSG_DEBUG, "%s added unresolved prepared txn %s\n", __func__, dist_txnid);
	return 0;
}

static int __free_prepared(void *obj, void *arg)
{
	DB_ENV *dbenv = (DB_ENV *)arg;
	DB_TXN_PREPARED *p = (DB_TXN_PREPARED *)obj;
	__free_prepared_txn(dbenv, p);
	return 0;
}

static int __hash_free_child(void *obj, void *arg)
{
	DB_ENV *dbenv = (DB_ENV *)arg;
    struct __db_txn_prepared_child *c = (struct __db_txn_prepared_child *)obj;
    listc_rfl(&c->p->children, c);
	__os_free(dbenv, obj);
	return 0;
}

/*
 * __txn_clear_all_prepared --
 *
 * Clear all prepared transactions.
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
	hash_clear(dbenv->prepared_utxnid_hash);
	hash_for(dbenv->prepared_children, __hash_free_child, dbenv);
	hash_clear(dbenv->prepared_children);
	hash_for(dbenv->prepared_txn_hash, __free_prepared, dbenv);
	hash_clear(dbenv->prepared_txn_hash);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	return 0;
}

/*
 * __txn_prune_resolved_prepared --
 *
 * Purges all resolved prepares from the txnlist.
 *
 * PUBLIC: int __txn_prune_resolved_prepared __P((DB_ENV *));
 */
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
			hash_del(dbenv->prepared_utxnid_hash, prev);
			__free_prepared_txn(dbenv, prev);
		}
		prev = p;
		if (p) p = hash_next(dbenv->prepared_txn_hash, &ent, &bkt);
	}
}

/* 
 * __txn_downgrade_all_prepared --
 *
 * Release locks for all prepared txns, but maintain entries in the
 * dist-txn list.  This is called when role-changing to replicant.
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
	hash_for(dbenv->prepared_children, __hash_free_child, dbenv);
	hash_clear(dbenv->prepared_children);
	__txn_prune_resolved_prepared(dbenv);
	hash_for(dbenv->prepared_txn_hash, __downgrade_prepared, dbenv);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	return 0;
}

/* 
 * __txn_upgrade_all_prepared --
 *
 * Create berkley txns for unresolved but prepared transactions we found
 * during recovery.  Acquire locks for these transactions.  Update the 
 * transaction id-space and write a txn-recycle record
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
	hash_for(dbenv->prepared_children, __hash_free_child, dbenv);
	hash_clear(dbenv->prepared_children);
	__txn_prune_resolved_prepared(dbenv);
	hash_for(dbenv->prepared_txn_hash, __upgrade_prepared, logc);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	logc->close(logc, 0);
	__txn_recycle_after_upgrade_prepared(dbenv);
	return 0;
}

int __txn_is_prepared(dbenv, utxnid)
	DB_ENV *dbenv;
	u_int64_t utxnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_TXN_PREPARED *p;
	struct __db_txn_prepared_child *c;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_utxnid_hash, &utxnid)) != NULL) {
		Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
#if defined (DEBUG_PREPARE)
		logmsg(LOGMSG_USER, "%s: found prepared utxnid %"PRIx64
			", dist-txn %s\n", __func__, utxnid, p->dist_txnid);
#endif
		return 1;
	}
	if ((c = hash_find(dbenv->prepared_children, &utxnid)) != NULL) {
		Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
#if defined (DEBUG_PREPARE)
		logmsg(LOGMSG_USER, "%s: found child prepared utxnid %"PRIx64
			", dist-txn %s\n", __func__, utxnid, c->p->dist_txnid);
#endif
		return 1;
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	return 0;
}

extern int __txn_undo(DB_TXN *txnp);
/* 
 * __txn_abort_recovered --
 *
 * Abort a prepared transaction that was recovered by this master.
 * 
 * PUBLIC: int __txn_abort_recovered __P((DB_ENV *, const char *));
 */
int __txn_abort_recovered(dbenv, dist_txnid)
	DB_ENV *dbenv;
	const char *dist_txnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_TXN_PREPARED *p;
	int ret;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		if (!F_ISSET(p, DB_DIST_HAVELOCKS) || p->txnp == NULL) {
			logmsg(LOGMSG_INFO, "%s unable to abort unprepared dist-txn %s\n", __func__, dist_txnid);
			p = NULL;
		} else {
			hash_del(dbenv->prepared_txn_hash, p);
			hash_del(dbenv->prepared_utxnid_hash, p);
			__free_prepared_children(dbenv, p);
		}
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	if (p == NULL) {
		logmsg(LOGMSG_INFO, "%s unable to locate txnid %s\n", __func__, dist_txnid);
		return -1;
	}

	DBT dtxnid = {0};
	dtxnid.data = p->dist_txnid;
	dtxnid.size = strlen(p->dist_txnid);

	DB_REP *db_rep = dbenv->rep_handle;
	REP *rep = db_rep->region;

	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	u_int32_t gen = rep->gen;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

	u_int64_t timestamp = comdb2_time_epoch();

	if ((ret = __txn_dist_abort_log(dbenv, p->txnp, &p->txnp->last_lsn, 0, gen, timestamp, &dtxnid)) != 0) {
		logmsg(LOGMSG_FATAL, "Error writing dist-abort for txn %s, LSN %d:%d\n", p->dist_txnid,
			p->prev_lsn.file, p->prev_lsn.offset);
		abort();
	}

    /* Write blkseq record for aborted prepare */
    dist_txn_abort_write_blkseq(NULL, p->blkseq_key.data, p->blkseq_key.size);

    /* Unregister cnonce */
    osql_blkseq_unregister_cnonce(p->blkseq_key.data, p->blkseq_key.size);

	/* Avoid surprise children */
	F_SET(p->txnp, TXN_CHILDCOMMIT);

	/* skip prepare-record */
	p->txnp->last_lsn = p->prev_lsn;

	/* Undo to recover pg-allocations */
	if ((ret = __txn_undo(p->txnp)) != 0) {
		logmsg(LOGMSG_FATAL, "%s: failed to abort prepared txn %s: %d\n", __func__,
			p->dist_txnid, ret);
		abort(); 
	}

	DB_LOCKREQ request = {.op = DB_LOCK_PUT_ALL};

	if ((ret = __lock_vec(dbenv, p->txnp->txnid, 0, &request, 1, NULL)) != 0) {
		logmsg(LOGMSG_FATAL, "Error releasing locks dist-txn %s, LSN %d:%d\n", p->dist_txnid,
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

/* TODO: make these callback-functions */
extern void bdb_osql_trn_repo_lock();
extern void bdb_osql_trn_repo_unlock();
extern int update_shadows_beforecommit(void *bdb_state, DB_LSN *last_logical_lsn,
		unsigned long long *commit_genid, int is_master);
extern int bdb_update_pglogs_commitlsn(void *bdb_state, void *pglogs, unsigned int nkeys,
		DB_LSN commit_lsn);
extern int bdb_transfer_pglogs_to_queues(void *bdb_state, void *pglogs,
		unsigned int nkeys, int is_logical_commit,
		unsigned long long logical_tranid, DB_LSN logical_commit_lsn, uint32_t gen,
		int32_t timestamp, unsigned long long context);

extern int __rep_lsn_cmp __P((const void *, const void *));

static int __lowest_prepared_lsn(void *obj, void *arg)
{
	DB_TXN_PREPARED *p = (DB_TXN_PREPARED *)obj;
	DB_LSN *lsn = (DB_LSN *)arg;
	if (F_ISSET(p, DB_DIST_ABORTED | DB_DIST_COMMITTED)) {
		return 0;
	}
	if (IS_ZERO_LSN(*lsn) || log_compare(&p->begin_lsn, lsn) < 0) {
		*lsn = p->begin_lsn;
	}
	return 0;
}

/* 
 * __txn_lowest_prepared_lsn --
 *
 * A new master will issue a checkpoint before acquiring locks & txn objects for
 * unresolved prepared transactions.  These are still active transactions, so 
 * recovery should begin at or prior to the oldest begin-lsn.
 *
 * PUBLIC: int __txn_lowest_prepared_lsn __P((DB_ENV *, DB_LSN *));
 */
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

static void add_child(DB_ENV *dbenv, DB_TXN_PREPARED *p, u_int64_t cutxnid)
{
    int ret;
	struct __db_txn_prepared_child *c = hash_find(dbenv->prepared_children, &cutxnid);
	if (c != NULL) {
#if defined (DEBUG_PREPARE)
		logmsg(LOGMSG_INFO, "%s: re-adding child %"PRIx64" with for %s\n", __func__,
			cutxnid, p->dist_txnid);
#endif
		return;
	}
	if ((ret = __os_calloc(dbenv, 1, sizeof(*c), &c)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error allocating memory %d\n", __func__, ret);
		return;
	}
	c->cutxnid = cutxnid;
	c->p = p;
	listc_abl(&p->children, c);
	hash_add(dbenv->prepared_children, c);
#if defined (DEBUG_PREPARE)
	logmsg(LOGMSG_INFO, "%s: added child %"PRIx64" with parent %"PRIx64", %s\n",
			__func__, cutxnid, p->utxnid, p->dist_txnid);
#endif
}

/* 
 * __txn_add_prepared_child --
 *
 * Unresolved-prepares are rolled-back, but they can't reclaim allocated pages,
 * as this could eventually commit.  This function records child-tranids of 
 * unresolved prepares in the 'prepared_children' hash so that pg_alloc_recover
 * won't add the allocated pages to limbo.  We will add them to the freelist if
 * the prepared-txn aborts.
 *
 * PUBLIC: int __txn_add_prepared_child __P((DB_ENV *, u_int64_t, u_int64_t));
 */
int __txn_add_prepared_child(dbenv, cutxnid, putxnid)
	DB_ENV *dbenv;
	u_int64_t cutxnid;
	u_int64_t putxnid;
{
	DB_TXN_PREPARED *p;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_utxnid_hash, &putxnid)) != NULL) {
		if (F_ISSET(p, DB_DIST_COMMITTED)) {
			logmsg(LOGMSG_USER, "%s backward rolling a committed txn?\n",
				__func__);
			abort();
		}

		if (!F_ISSET(p, DB_DIST_ABORTED)) {
			logmsg(LOGMSG_INFO, "%s: adding child-txn %"PRIx64" for prepared dist-txn %s\n",
				__func__, cutxnid, p->dist_txnid);
			add_child(dbenv, p, cutxnid);
		}
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	return 0;
}

static int __discard(dbenv, p)
	DB_ENV *dbenv;
	DB_TXN_PREPARED *p;
{
	int ret = 0;
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	if (F_ISSET(p, DB_DIST_HAVELOCKS)) {
		assert(p->txnp != NULL);
		DB_LOCKREQ request = {.op = DB_LOCK_PUT_ALL};

		if ((ret = __lock_vec(dbenv, p->txnp->txnid, 0, &request, 1, NULL)) != 0) {
			logmsg(LOGMSG_FATAL, "Error releasing locks dist-txn %s, LSN %d:%d\n", p->dist_txnid,
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

/* 
 * __txn_discard_all_recovered --
 *
 * This master is about to exit.  Discard all recovered prepared transactions.
 *
 * PUBLIC: int __txn_discard_all_recovered __P((DB_ENV *));
 */
int __txn_discard_all_recovered(dbenv)
	DB_ENV *dbenv;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	unsigned int bkt;
	void *ent;
	DB_TXN_PREPARED *prev = NULL;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	DB_TXN_PREPARED *p = hash_first(dbenv->prepared_txn_hash, &ent, &bkt);
	while (p || prev) {
		if (prev && F_ISSET(prev, DB_DIST_HAVELOCKS)) {
			__discard(dbenv, prev);
		}
		prev = p;
		if (p) p = hash_next(dbenv->prepared_txn_hash, &ent, &bkt);
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	return 0;
}

/* 
 * __txn_discard_recovered --
 *
 * This master has prepared, and then resolved a prepared transaction. Remove 
 * it from the prepared-txn list.
 *
 * PUBLIC: int __txn_discard_recovered __P((DB_ENV *, const char *));
 */
int __txn_discard_recovered(dbenv, dist_txnid)
	DB_ENV *dbenv;
	const char *dist_txnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_TXN_PREPARED *p;
	int ret;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		hash_del(dbenv->prepared_txn_hash, p);
		hash_del(dbenv->prepared_utxnid_hash, p);
		__free_prepared_children(dbenv, p);
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	if (p == NULL) {
		logmsg(LOGMSG_INFO, "%s unable to locate txnid %s\n", __func__, dist_txnid);
		return -1;
	}

	return __discard(dbenv, p);
}

/* 
 * __rep_commit_dist_prepared --
 *
 * Replication saw a dist-commit for this prepared txn. Remove it from the
 * prepared-txn list.
 *
 * PUBLIC: int __rep_commit_dist_prepared __P((DB_ENV *, const char *));
 */
int __rep_commit_dist_prepared(dbenv, dist_txnid)
	DB_ENV *dbenv;
	const char *dist_txnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_TXN_PREPARED *p;
	int ret;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		hash_del(dbenv->prepared_txn_hash, p);
		hash_del(dbenv->prepared_utxnid_hash, p);
		__free_prepared_children(dbenv, p);
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	if (p == NULL) {
		logmsg(LOGMSG_INFO, "%s unable to locate txnid %s\n", __func__, dist_txnid);
#if defined (DEBUG_PREPARE)
		abort();
#else
		return -1;
#endif
	}

	assert(!F_ISSET(p, DB_DIST_HAVELOCKS));
	__free_prepared_txn(dbenv, p);
	return 0;
}

/* 
 * __rep_abort_dist_prepared --
 *
 * Replication saw a dist-abort for this prepared txn.  Write the blkseq
 * and remove it from the prepared-txn list.
 *
 * PUBLIC: int __rep_abort_dist_prepared __P((DB_ENV *, const char *, ));
 */
int __rep_abort_dist_prepared(dbenv, dist_txnid)
	DB_ENV *dbenv;
	const char *dist_txnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_TXN_PREPARED *p;
	int ret;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		hash_del(dbenv->prepared_txn_hash, p);
		hash_del(dbenv->prepared_utxnid_hash, p);
		__free_prepared_children(dbenv, p);
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	if (p == NULL) {
		logmsg(LOGMSG_INFO, "%s unable to locate txnid %s\n", __func__, dist_txnid);
#if defined (DEBUG_PREPARE)
		abort();
#else
		return -1;
#endif
	}

    dist_txn_abort_write_blkseq(NULL, p->blkseq_key.data, p->blkseq_key.size);
	assert(!F_ISSET(p, DB_DIST_HAVELOCKS));
	__free_prepared_txn(dbenv, p);
	return 0;
}

static int __abort_waiters(void *obj, void *arg)
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_TXN_PREPARED *p = (DB_TXN_PREPARED *)obj;
	DB_ENV *dbenv = (DB_ENV *)arg;
	if (F_ISSET(p, DB_DIST_HAVELOCKS)) {
#if defined (DEBUG_PREPARE)
		logmsg(LOGMSG_USER, "%s abort waiters on %s\n", __func__, p->dist_txnid);
#endif
		dbenv->lock_abort_waiters(dbenv, p->txnp->txnid, 0);
	}
	return 0;
}

/* 
 * __txn_abort_prepared_waiters --
 *
 * To downgrade, a master must force all non-prepared transactions to abort,
 * and then subsequently unroll prepared transactions.  This routine returns
 * deadlock to any transaction which is blocked on a prepared transaction.
 *
 * PUBLIC: int __txn_abort_prepared_waiters __P((DB_ENV *));
 */
int __txn_abort_prepared_waiters(dbenv)
	DB_ENV *dbenv;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	hash_for(dbenv->prepared_txn_hash, __abort_waiters, dbenv);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	return 0;
}

/* 
 * __txn_commit_recovered --
 *
 * Commits a prepared transaction that was recovered by this master.
 *
 * PUBLIC: int __txn_commit_recovered __P((DB_ENV *, const char *));
 */
int __txn_commit_recovered(dbenv, dist_txnid)
	DB_ENV *dbenv;
	const char *dist_txnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_TXN_PREPARED *p;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) != NULL) {
		if (!F_ISSET(p, DB_DIST_HAVELOCKS)) {
			logmsg(LOGMSG_INFO, "%s unable to commit unprepared dist-txn %s\n", __func__, dist_txnid);
			p = NULL;
		} else {
			hash_del(dbenv->prepared_txn_hash, p);
			hash_del(dbenv->prepared_utxnid_hash, p);
			__free_prepared_children(dbenv, p);
		}
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	if (p == NULL) {
		logmsg(LOGMSG_INFO, "%s unable to locate txnid %s\n", __func__, dist_txnid);
		return -1;
	}


	/* Write the commit record now to get commit-context */
	u_int64_t context = 0;
	u_int64_t timestamp = comdb2_time_epoch();
	u_int32_t gen = 0;
	u_int32_t lflags = DB_LOG_COMMIT | DB_LOG_PERM;
	DB_LSN lsn_out;
	DB_REP *db_rep = dbenv->rep_handle;
	REP *rep = db_rep->region;
	int ret;

	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	gen = rep->gen;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

	if (F_ISSET(p, DB_DIST_UPDSHADOWS)) {
		bdb_osql_trn_repo_lock();
	}

	DBT dtxnid = {0};
	dtxnid.data = p->dist_txnid;
	dtxnid.size = strlen(p->dist_txnid);

	if ((ret = __txn_dist_commit_log(dbenv, p->txnp, &lsn_out, &context, lflags, &dtxnid, 
		gen, timestamp, NULL)) != 0) {
		logmsg(LOGMSG_FATAL, "Error writing dist-commit for txn %s, LSN %d:%d\n", p->dist_txnid,
			p->prev_lsn.file, p->prev_lsn.offset);
		abort();
	}

	assert(context != 0);

	/* We use the (incorrect) 'prepare' lsn collecting pglogs above */
	bdb_update_pglogs_commitlsn(dbenv->app_private, p->pglogs, p->keycnt, lsn_out);
	bdb_transfer_pglogs_to_queues(dbenv->app_private, p->pglogs, p->keycnt, 0,
		0, lsn_out, gen, timestamp, context);

	if (F_ISSET(p, DB_DIST_UPDSHADOWS)) {
#if defined (DEBUG_PREPARE)
		comdb2_cheapstack_sym(stderr, "update_shadows_beforecommit", __func__);
#endif
		if ((ret = update_shadows_beforecommit(dbenv->app_private, &p->prev_lsn, NULL, 1)) !=0 ) {
			logmsg(LOGMSG_FATAL, "Error %d updating shadows before commit for txn %s, LSN %d:%d\n",
				ret, p->dist_txnid, p->prepare_lsn.file, p->prepare_lsn.offset);
			abort();
		}
		bdb_osql_trn_repo_unlock();
	}

	/* Collect txn, roll-forward, call txn_commit */
	LSN_COLLECTION lc = {0};
	DBT data_dbt = {0};
	DB_LOGC *logc = NULL;
	int had_serializable_records = 0;
	uint32_t rectype = 0;
	void *txninfo = NULL;

	if ((ret = __rep_collect_txn(dbenv, &p->prepare_lsn, &lc, &had_serializable_records, NULL)) != 0) {
		logmsg(LOGMSG_FATAL, "Error collecting dist-txn %s, LSN %d:%d\n", p->dist_txnid,
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

    /* Unregister cnonce */
    osql_blkseq_unregister_cnonce(p->blkseq_key.data, p->blkseq_key.size);

	DB_LOCKREQ request = {.op = DB_LOCK_PUT_ALL};

	if ((ret = __lock_vec(dbenv, p->txnp->txnid, 0, &request, 1, NULL)) != 0) {
		logmsg(LOGMSG_FATAL, "Error releasing locks dist-txn %s, LSN %d:%d\n", p->dist_txnid,
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
