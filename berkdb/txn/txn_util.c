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
extern int gbl_utxnid_log;
extern int gbl_snapshot_isolation;

/*
 * __txn_commit_map_enabled --
 *	The commit-LSN map exists only to serve snapshot isolation, and is only
 *	correct if utxnids are being logged.  When snapshot isolation is turned
 *	off we skip populating it entirely: one entry per committed transaction
 *	is retained until the log file holding the commit is deleted, which is
 *	the dominant memory cost on a node that never runs a snapshot query.
 *
 * PUBLIC: int __txn_commit_map_enabled __P((void));
 */
int __txn_commit_map_enabled()
{
	return gbl_utxnid_log && gbl_snapshot_isolation;
}

/*
 * Count of times we were asked to remove a utxnid that wasn't present in the
 * commit-LSN map.  This is an expected condition (the map is a windowed cache;
 * see __txn_commit_map_remove_nolock), so it is not fatal -- but a spike here
 * outside of physrep rewinds / recover-to-lsn can indicate a real map-population
 * bug, so it is surfaced as an observable counter.
 */
int64_t gbl_commit_map_remove_miss = 0;

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
	 * release read locks.	Since not all paths will call this
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
 * Everything in the commit LSN map comes from one allocator, which shows up as
 * "commit_map" in memstat.  It is shared by every map in the process (the
 * selftest builds private ones), so it locks.
 */
static comdb2ma commit_map_ma;
static pthread_once_t commit_map_ma_once = PTHREAD_ONCE_INIT;

static void commit_map_ma_init(void)
{
	commit_map_ma = comdb2ma_create(0, 0, "commit_map", COMDB2MA_MT_SAFE);
}

static void *commit_map_malloc(size_t size)
{
	return comdb2_malloc(commit_map_ma, size);
}

static void *commit_map_calloc(size_t n, size_t size)
{
	return comdb2_calloc(commit_map_ma, n, size);
}

static void commit_map_free(void *ptr)
{
	comdb2_free(ptr);
}

static hash_t *commit_map_hash_init(int keyoff, int keylen)
{
	return hash_setalloc_init(commit_map_malloc, commit_map_free, keyoff, keylen);
}

/*
 * Transactions are kept in slabs, a chain of them per logfile, so that a
 * transaction needs no allocation of its own and a purge frees a few large
 * blocks.  A transaction removed on its own (recovery undoing its commit) only
 * has its utxnid cleared; its slot goes with the rest of the logfile.  A
 * logfile's first slab is small and each one after it twice the size of the last, so a quiet
 * database does not pay for large slabs.
 */
#define COMMIT_MAP_SLAB_MIN 256
#define COMMIT_MAP_SLAB_MAX 16384

struct __utxnid_slab {
	struct __utxnid_slab *next;
	u_int32_t ntracks;
	u_int32_t nused;
	UTXNID_TRACK tracks[];
};

/*
 * The utxnid hash is chained through the transactions themselves.  It doubles
 * once it holds more than one transaction per bucket, and shrinks once it is
 * down to fewer than one per eight, so that a purge gives back the buckets
 * too.  A resize relinks every transaction into the new table at once, walking
 * the slabs.
 */
#define COMMIT_MAP_MIN_BITS 10

static inline u_int64_t __txn_commit_map_bucket(u_int64_t utxnid, u_int32_t bits)
{
	return (utxnid * 0x9E3779B97F4A7C15ULL) >> (64 - bits);
}

/* Returns the link that points to the transaction, or NULL */
static UTXNID_TRACK **__txn_commit_map_findp(DB_TXN_COMMIT_MAP *txmap, u_int64_t utxnid)
{
	UTXNID_TRACK **pp;

	for (pp = &txmap->buckets[__txn_commit_map_bucket(utxnid, txmap->bits)]; *pp; pp = &(*pp)->next) {
		if ((*pp)->utxnid == utxnid) {
			return pp;
		}
	}
	return NULL;
}

static void __txn_commit_map_link(DB_TXN_COMMIT_MAP *txmap, UTXNID_TRACK *txn)
{
	const u_int64_t b = __txn_commit_map_bucket(txn->utxnid, txmap->bits);

	txn->next = txmap->buckets[b];
	txmap->buckets[b] = txn;
}

static int __txn_commit_map_relink(void *obj, void *arg)
{
	LOGFILE_TXN_LIST * const list = (LOGFILE_TXN_LIST *) obj;
	DB_TXN_COMMIT_MAP * const txmap = arg;
	struct __utxnid_slab *slab;
	u_int32_t i;

	for (slab = list->slabs; slab; slab = slab->next) {
		for (i = 0; i < slab->nused; i++) {
			if (slab->tracks[i].utxnid != 0) {
				__txn_commit_map_link(txmap, &slab->tracks[i]);
			}
		}
	}
	return 0;
}

/* Grow or shrink the hash if it has drifted out of range */
static void __txn_commit_map_maybe_resize(DB_TXN_COMMIT_MAP *txmap)
{
	const u_int64_t nbuckets = (u_int64_t) 1 << txmap->bits;
	const u_int64_t nentries = txmap->nentries;
	UTXNID_TRACK **buckets;
	u_int32_t bits;

	if (nentries > nbuckets) {
		bits = txmap->bits + 1;
	} else if (txmap->bits > COMMIT_MAP_MIN_BITS && nentries < nbuckets / 8) {
		for (bits = COMMIT_MAP_MIN_BITS; ((u_int64_t) 1 << bits) < 2 * nentries; bits++)
			;
	} else {
		return;
	}
	/* if this fails, keep using the table we have */
	if ((buckets = commit_map_calloc((size_t) 1 << bits, sizeof(*buckets))) == NULL) {
		return;
	}
	commit_map_free(txmap->buckets);
	txmap->buckets = buckets;
	txmap->bits = bits;
	txmap->nresizes++;
	hash_for(txmap->logfile_lists, __txn_commit_map_relink, txmap);
}


static UTXNID_TRACK *__txn_commit_map_slot(DB_TXN_COMMIT_MAP *txmap, LOGFILE_TXN_LIST *list)
{
	struct __utxnid_slab *slab = list->slabs;

	if (slab == NULL || slab->nused == slab->ntracks) {
		u_int32_t n = COMMIT_MAP_SLAB_MIN;
		if (slab) {
			n = slab->ntracks < COMMIT_MAP_SLAB_MAX / 2 ? slab->ntracks * 2 : COMMIT_MAP_SLAB_MAX;
		}
		if ((slab = commit_map_malloc(offsetof(struct __utxnid_slab, tracks) + n * sizeof(UTXNID_TRACK))) == NULL) {
			return NULL;
		}
		slab->ntracks = n;
		slab->nused = 0;
		slab->next = list->slabs;
		list->slabs = slab;
		txmap->nslabs++;
		txmap->nslots += n;
	}
	return &slab->tracks[slab->nused++];
}

/*
 * __txn_commit_map_init --
 *	Initialize commit LSN map.
 *
 * PUBLIC: int __txn_commit_map_init
 * PUBLIC:	   __P((DB_ENV *));
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

	pthread_once(&commit_map_ma_once, commit_map_ma_init);
	if (commit_map_ma == NULL) {
		ret = ENOMEM;
		goto err;
	}

	txmap->bits = COMMIT_MAP_MIN_BITS;
	txmap->buckets = commit_map_calloc((size_t) 1 << COMMIT_MAP_MIN_BITS, sizeof(UTXNID_TRACK *));
	txmap->logfile_lists = commit_map_hash_init(offsetof(LOGFILE_TXN_LIST, file_num), sizeof(u_int32_t));

	if (txmap->buckets == NULL || txmap->logfile_lists == NULL) {
		ret = ENOMEM;
		goto err;
	}

	txmap->smallest_logfile = -1;
	txmap->highest_logfile = -1;
	Pthread_mutex_init(&txmap->txmap_mutexp, NULL);
	dbenv->txmap = txmap;

	return 0;
err:
	logmsg(LOGMSG_ERROR, "Failed to initialize commit LSN map\n");
	return ret;
}

static void __txn_commit_map_free_slabs(DB_TXN_COMMIT_MAP *txmap, LOGFILE_TXN_LIST *list)
{
	struct __utxnid_slab *slab, *next;

	for (slab = list->slabs; slab; slab = next) {
		next = slab->next;
		txmap->nslabs--;
		txmap->nslots -= slab->ntracks;
		commit_map_free(slab);
	}
	list->slabs = NULL;
}

static int free_logfile_list_elt(obj, arg)
		void *obj;
		void *arg;
{
	LOGFILE_TXN_LIST * const to_delete = (LOGFILE_TXN_LIST * const) obj;

	__txn_commit_map_free_slabs(((DB_ENV *) arg)->txmap, to_delete);
	commit_map_free(to_delete);
	return 0;
}

/*
 * __commit_map_destroy --
 *		Destroy commit LSN map.
 *
 * PUBLIC: int __commit_map_destroy
 * PUBLIC:	   __P((DB_ENV *));
 */
int __txn_commit_map_destroy(dbenv)
		DB_ENV *dbenv;
{
		if (dbenv->txmap) {
				hash_for(dbenv->txmap->logfile_lists, &free_logfile_list_elt, (void *) dbenv);
				hash_clear(dbenv->txmap->logfile_lists);
				hash_free(dbenv->txmap->logfile_lists);
				commit_map_free(dbenv->txmap->buckets);

				Pthread_mutex_destroy(&dbenv->txmap->txmap_mutexp);
				__os_free(dbenv, dbenv->txmap);
		}

		return 0;
}


/*
 * __txn_commit_map_delete_logfile_list --
 *	Remove a logfile's list from the map, along with every transaction still
 *	on it.
 *
 * PUBLIC: static int __txn_commit_map_delete_logfile_list
 * PUBLIC:	   __P((DB_ENV *, LOGFILE_TXN_LIST * const));
 */
static void __txn_commit_map_delete_logfile_list(DB_ENV *dbenv, LOGFILE_TXN_LIST * const to_delete) {
	DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;
	const u_int32_t del_log = to_delete->file_num;
	const int i_am_highest_logfile = del_log == txmap->highest_logfile;
	const int i_am_smallest_logfile = del_log == txmap->smallest_logfile;
	struct __utxnid_slab *slab;
	u_int32_t i;

	if (i_am_highest_logfile && i_am_smallest_logfile)
	{
		logmsg(LOGMSG_WARN, "%s: Deleting the only logfile (%"PRIu32") in txmap\n", __func__, del_log);

		txmap->highest_logfile = -1;
		txmap->smallest_logfile = -1;
	} else if (i_am_highest_logfile)
	{
		logmsg(LOGMSG_WARN, "%s: Deleting the highest logfile (%"PRIu32") in txmap\n", __func__, del_log);

		LOGFILE_TXN_LIST *successor = NULL;
		while ((successor == NULL) && (--txmap->highest_logfile >= txmap->smallest_logfile)) {
			successor = hash_find(txmap->logfile_lists, &txmap->highest_logfile);
		}
		assert(successor);
	} else if (i_am_smallest_logfile)
	{
		LOGFILE_TXN_LIST *successor = NULL;
		while ((successor == NULL) && (++txmap->smallest_logfile <= txmap->highest_logfile)) {
			successor = hash_find(txmap->logfile_lists, &txmap->smallest_logfile);
		}
		assert(successor);
	}

	for (slab = to_delete->slabs; slab; slab = slab->next) {
		for (i = 0; i < slab->nused; i++) {
			UTXNID_TRACK * const txn = &slab->tracks[i];
			UTXNID_TRACK **pp;

			if (txn->utxnid == 0) {
				continue;
			}
			if (dbenv->attr.commit_map_debug) {
				logmsg(LOGMSG_DEBUG, "%s: Deleting utxnid %"PRIu64"\n",
						__func__, txn->utxnid);
			}
			pp = __txn_commit_map_findp(txmap, txn->utxnid);
			if (pp == NULL || *pp != txn) {
				/* the slab is about to be freed: carrying on would leave the hash pointing into it */
				logmsg(LOGMSG_FATAL, "%s: utxnid %"PRIu64" of logfile %"PRIu32" is not in the hash\n",
						__func__, txn->utxnid, del_log);
				abort();
			}
			*pp = txn->next;
			txmap->nentries--;
		}
	}

	__txn_commit_map_free_slabs(txmap, to_delete);
	hash_del(txmap->logfile_lists, to_delete);
	commit_map_free(to_delete);
}

/*
 * __txn_commit_map_remove_nolock --
 *	Remove a transaction from the commit LSN map without locking.
 *
 * PUBLIC: static int __txn_commit_map_remove_nolock
 * PUBLIC:	   __P((DB_ENV *, u_int64_t));
 */
static int __txn_commit_map_remove_nolock(dbenv, utxnid)
	DB_ENV *dbenv;
	u_int64_t utxnid;
{
	if (dbenv->attr.commit_map_debug) {
		logmsg(LOGMSG_DEBUG, "%s: Deleting utxnid %"PRIu64"\n",
				__func__, utxnid);
	}

	if (utxnid == 0) {
		if (dbenv->attr.commit_map_debug) {
			logmsg(LOGMSG_DEBUG, "%s: Not deleting txn with utxnid 0\n", __func__);
		}
		return 0;
	}

	int ret = 0;
	DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;

	UTXNID_TRACK ** const pp = __txn_commit_map_findp(txmap, utxnid);
	if (!pp) {
		/*
		 * The commit-LSN map is a windowed, in-memory cache: it is pruned
		 * as log files are deleted, is rebuilt only from the last
		 * checkpoint forward on restart, and (on physical replicants) need
		 * not span a range that is being rewound / truncated.  A missing
		 * entry is therefore an expected condition, not corruption.  Report
		 * it as DB_NOTFOUND and let the caller decide -- recovery's backward
		 * pass treats it as a no-op so it can continue.
		 */
		++gbl_commit_map_remove_miss;
		/*
		 * On a large physrep rewind every committed txn in the undo window
		 * can miss, so rate-limit the log to at most one line per second.
		 * gbl_commit_map_remove_miss (see 'bdb clminfo') is the durable,
		 * always-on signal. Safe: this runs under txmap_mutexp.
		 */
		static int lastpr = 0;
		const int now = comdb2_time_epoch();
		if (now != lastpr) {
			logmsg(LOGMSG_WARN, "%s: transaction %"PRIu64" not in commit map "
					"(nothing to remove); total misses %"PRId64"\n",
					__func__, utxnid, gbl_commit_map_remove_miss);
			lastpr = now;
		}
		ret = DB_NOTFOUND;
		goto err;
	}

	UTXNID_TRACK * const txn = *pp;
	LOGFILE_TXN_LIST * const logfile_list = hash_find(txmap->logfile_lists, &(txn->commit_lsn.file));
	if (!logfile_list) {
		logmsg(LOGMSG_ERROR, "%s: Could not find logfile list for file %d\n", __func__, txn->commit_lsn.file);
		ret = 1;
		goto err;
	}

	*pp = txn->next;
	txn->utxnid = 0;
	txmap->nentries--;
	if (--logfile_list->nlive == 0) {
		__txn_commit_map_delete_logfile_list(dbenv, logfile_list);
	}

	__txn_commit_map_maybe_resize(txmap);

err:
	return ret;
}

/*
 * __txn_commit_map_remove --
 *	Removes a transaction from the commit LSN map
 *
 * PUBLIC: int __txn_commit_map_remove
 * PUBLIC:	   __P((DB_ENV *, u_int64_t));
 */
int __txn_commit_map_remove(dbenv, utxnid)
	DB_ENV *dbenv;
	u_int64_t utxnid;
{
	DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;

	Pthread_mutex_lock(&txmap->txmap_mutexp);
	int ret = __txn_commit_map_remove_nolock(dbenv, utxnid);
	Pthread_mutex_unlock(&txmap->txmap_mutexp);

	return ret;
}

/*
 * __txn_commit_map_print_info --
 *	Prints the internal state of the commit LSN map.
 */
void __txn_commit_map_print_info(DB_ENV *dbenv, loglvl lvl, int should_lock) {
	DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;

	if (should_lock) { Pthread_mutex_lock(&txmap->txmap_mutexp); }

	logmsg(lvl, "Highest logfile: %"PRId64"; "
					"Smallest logfile: %"PRId64"; "
					"Remove misses: %"PRId64"; "
					"Entries: %"PRId64"; "
					"Peak entries: %"PRId64"; "
					"Buckets: %"PRIu64"; "
					"Resizes: %"PRId64"; "
					"Bucket bytes: %"PRIu64"; "
					"Slabs: %"PRId64"; "
					"Slots: %"PRId64"\n",
					txmap->highest_logfile,
					txmap->smallest_logfile,
					gbl_commit_map_remove_miss,
					txmap->nentries,
					txmap->peak_entries,
					(u_int64_t) 1 << txmap->bits,
					txmap->nresizes,
					((u_int64_t) 1 << txmap->bits) * sizeof(UTXNID_TRACK *),
					txmap->nslabs,
					txmap->nslots);

	if (should_lock) { Pthread_mutex_unlock(&txmap->txmap_mutexp); }
}

/*
 * __txn_commit_map_delete_logfile_txns --
 *	Remove all transactions that committed in a specific logfile
 *	from the commit LSN map.
 *
 * PUBLIC: int __txn_commit_map_delete_logfile_txns
 * PUBLIC:	   __P((DB_ENV *, u_int32_t));
 */
int __txn_commit_map_delete_logfile_txns(dbenv, del_log)
	DB_ENV *dbenv;
	u_int32_t del_log;
{
	DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;
	int ret = 0;

	Pthread_mutex_lock(&txmap->txmap_mutexp);

	if (dbenv->attr.commit_map_debug) {
		logmsg(LOGMSG_DEBUG, "%s: Deleting log %"PRIu32"\n",
				__func__, del_log);
		__txn_commit_map_print_info(dbenv, LOGMSG_DEBUG, 0);
	}

	LOGFILE_TXN_LIST * const to_delete = hash_find(txmap->logfile_lists, &del_log);
	if (!to_delete) {
		ret = 1;
		goto err;
	}

	__txn_commit_map_delete_logfile_list(dbenv, to_delete);
	__txn_commit_map_maybe_resize(txmap);
err:
	Pthread_mutex_unlock(&txmap->txmap_mutexp);

	return ret;
}

/*
 * __txn_commit_map_get --
 *	Get the commit LSN of a transaction.
 *
 * PUBLIC: int __txn_commit_map_get
 * PUBLIC:	   __P((DB_ENV *, u_int64_t, DB_LSN*));
 */
int __txn_commit_map_get(dbenv, utxnid, commit_lsn)
	DB_ENV *dbenv;
	u_int64_t utxnid;
	DB_LSN *commit_lsn;
{
	DB_TXN_COMMIT_MAP *txmap;
	UTXNID_TRACK **pp;
	int ret;

	txmap = dbenv->txmap;
	ret = 0;

	Pthread_mutex_lock(&txmap->txmap_mutexp);
	pp = __txn_commit_map_findp(txmap, utxnid);

	if (pp == NULL) {
		ret = DB_NOTFOUND;
	} else {
		*commit_lsn = (*pp)->commit_lsn;
	}

	Pthread_mutex_unlock(&txmap->txmap_mutexp);
	return ret;
}

/*
 * __txn_commit_map_add_nolock --
 *	Store the commit LSN of a transaction.
 *
 * PUBLIC: int __txn_commit_map_add_nolock
 * PUBLIC:	   __P((DB_ENV *, u_int64_t, DB_LSN));
 */
int __txn_commit_map_add_nolock(dbenv, utxnid, commit_lsn)
	DB_ENV *dbenv;
	u_int64_t utxnid;
	DB_LSN commit_lsn;
{
	DB_TXN_COMMIT_MAP *txmap;
	LOGFILE_TXN_LIST *to_delete;
	UTXNID_TRACK *txn;
	int commit_map_debug, new_list;

	txmap = dbenv->txmap;
	commit_map_debug = dbenv->attr.commit_map_debug;

	if (commit_map_debug) {
		logmsg(LOGMSG_DEBUG, "Trying to add utxnid %"PRIu64" commit lsn %"PRIu32":%"PRIu32" to the map\n", utxnid, commit_lsn.file, commit_lsn.offset);
	}

	if ((utxnid == 0) || IS_ZERO_LSN(commit_lsn)) {
		if (commit_map_debug) {
			logmsg(LOGMSG_DEBUG, "Transaction is not eligible to be added to the map.\n");
		}
		return 0;
	}

	if (__txn_commit_map_findp(txmap, utxnid) != NULL) {
		if (commit_map_debug) {
			logmsg(LOGMSG_DEBUG, "Transaction already exists in the map. Not adding it again\n");
		}
		return 0;
	}

	to_delete = hash_find(txmap->logfile_lists, &commit_lsn.file);
	new_list = (to_delete == NULL);

	if (new_list) {
		if ((to_delete = commit_map_malloc(sizeof(LOGFILE_TXN_LIST))) == NULL) {
			return ENOMEM;
		}
		to_delete->file_num = commit_lsn.file;
		to_delete->nlive = 0;
		to_delete->slabs = NULL;
	}

	if ((txn = __txn_commit_map_slot(txmap, to_delete)) == NULL) {
		if (new_list) {
			commit_map_free(to_delete);
		}
		return ENOMEM;
	}

	if (new_list) {
		if (hash_add(txmap->logfile_lists, to_delete) != 0) {
			__txn_commit_map_free_slabs(txmap, to_delete);
			commit_map_free(to_delete);
			return ENOMEM;
		}
		if (commit_lsn.file < txmap->smallest_logfile || txmap->smallest_logfile == -1) {
			txmap->smallest_logfile = commit_lsn.file;
		}
	}

	if (commit_lsn.file > txmap->highest_logfile) { txmap->highest_logfile = commit_lsn.file; }

	txn->utxnid = utxnid;
	txn->commit_lsn = commit_lsn;
	__txn_commit_map_link(txmap, txn);
	to_delete->nlive++;
	if (++txmap->nentries > txmap->peak_entries) {
		txmap->peak_entries = txmap->nentries;
	}

	__txn_commit_map_maybe_resize(txmap);

	return 0;
}

/*
 * __txn_commit_map_add --
 *	Store the commit LSN of a transaction.
 *
 * PUBLIC: int __txn_commit_map_add
 * PUBLIC:	   __P((DB_ENV *, u_int64_t, DB_LSN));
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

struct __txn_commit_map_foreach {
	int (*fn)(UTXNID_TRACK *, void *);
	void *arg;
};

static int __txn_commit_map_foreach_list(void *obj, void *arg)
{
	LOGFILE_TXN_LIST * const list = (LOGFILE_TXN_LIST *) obj;
	struct __txn_commit_map_foreach * const fa = arg;
	struct __utxnid_slab *slab;
	u_int32_t i;
	int rc;

	for (slab = list->slabs; slab; slab = slab->next) {
		for (i = 0; i < slab->nused; i++) {
			if (slab->tracks[i].utxnid != 0 && (rc = fa->fn(&slab->tracks[i], fa->arg)) != 0) {
				return rc;
			}
		}
	}
	return 0;
}

/*
 * __txn_commit_map_foreach_nolock --
 *	Call fn on every transaction in the map until it returns non-zero.
 *	Caller holds txmap_mutexp.
 *
 * PUBLIC: int __txn_commit_map_foreach_nolock
 * PUBLIC:	   __P((DB_ENV *, int (*)(UTXNID_TRACK *, void *), void *));
 */
int __txn_commit_map_foreach_nolock(DB_ENV *dbenv, int (*fn)(UTXNID_TRACK *, void *), void *arg)
{
	struct __txn_commit_map_foreach fa = {fn, arg};
	return hash_for(dbenv->txmap->logfile_lists, __txn_commit_map_foreach_list, &fa);
}

/*
 * __txn_commit_map_nentries_nolock --
 *	Number of transactions in the map.  Caller holds txmap_mutexp.
 *
 * PUBLIC: int64_t __txn_commit_map_nentries_nolock __P((DB_ENV *));
 */
int64_t __txn_commit_map_nentries_nolock(DB_ENV *dbenv)
{
	return dbenv->txmap->nentries;
}

#ifdef COMDB2_TEST
struct __txn_commit_map_check {
	DB_TXN_COMMIT_MAP *txmap;
	int64_t ntxns;
	int nlists;
	int err;
};

static int __txn_commit_map_check_list(void *obj, void *arg)
{
	LOGFILE_TXN_LIST * const list = (LOGFILE_TXN_LIST *) obj;
	struct __txn_commit_map_check * const chk = arg;
	struct __utxnid_slab *slab;
	u_int32_t i, live = 0;

	chk->nlists++;
	if (list->nlive == 0 || list->slabs == NULL ||
	    list->file_num < chk->txmap->smallest_logfile ||
	    list->file_num > chk->txmap->highest_logfile) {
		chk->err = __LINE__;
	}
	for (slab = list->slabs; slab; slab = slab->next) {
		if (slab->nused > slab->ntracks) {
			chk->err = __LINE__;
		}
		for (i = 0; i < slab->nused; i++) {
			UTXNID_TRACK * const txn = &slab->tracks[i];
			UTXNID_TRACK **pp;
			if (txn->utxnid == 0) {
				continue;
			}
			live++;
			pp = __txn_commit_map_findp(chk->txmap, txn->utxnid);
			if (pp == NULL || *pp != txn || txn->commit_lsn.file != list->file_num) {
				chk->err = __LINE__;
			}
		}
	}
	if (live != list->nlive) {
		chk->err = __LINE__;
	}
	chk->ntxns += live;
	return 0;
}

/*
 * Every transaction has to be in a slab of the logfile it committed in and
 * chained in the bucket its utxnid hashes to, and nowhere else.
 */
static int __txn_commit_map_check(DB_TXN_COMMIT_MAP *txmap, int64_t ntxns, int nlists)
{
	struct __txn_commit_map_check chk = {txmap, 0, 0, 0};
	u_int32_t smallest = txmap->smallest_logfile, highest = txmap->highest_logfile;
	int64_t chained = 0;
	u_int64_t b;

	hash_for(txmap->logfile_lists, __txn_commit_map_check_list, &chk);
	if (chk.err) {
		return chk.err;
	}
	for (b = 0; b < ((u_int64_t) 1 << txmap->bits); b++) {
		UTXNID_TRACK *txn;
		for (txn = txmap->buckets[b]; txn; txn = txn->next) {
			if (txn->utxnid == 0 || __txn_commit_map_bucket(txn->utxnid, txmap->bits) != b) {
				return __LINE__;
			}
			chained++;
		}
	}
	if (chk.ntxns != ntxns || txmap->nentries != ntxns || chained != ntxns || chk.nlists != nlists ||
	    hash_get_num_entries(txmap->logfile_lists) != nlists) {
		return __LINE__;
	}
	if (nlists == 0 && (txmap->smallest_logfile != -1 || txmap->highest_logfile != -1)) {
		return __LINE__;
	}
	if (nlists > 0 && (!hash_find(txmap->logfile_lists, &smallest) ||
	    !hash_find(txmap->logfile_lists, &highest))) {
		return __LINE__;
	}
	return 0;
}

struct __txn_commit_map_collect {
	u_int64_t *ids;
	int64_t n;
	int64_t cap;
};

static int __txn_commit_map_collect_one(UTXNID_TRACK *txn, void *arg)
{
	struct __txn_commit_map_collect * const c = arg;

	if (c->n == c->cap) {
		return 1;
	}
	c->ids[c->n++] = txn->utxnid;
	return 0;
}

static int __txn_commit_map_cmp_utxnid(const void *a, const void *b)
{
	const u_int64_t x = *(const u_int64_t *) a, y = *(const u_int64_t *) b;
	return x < y ? -1 : x > y;
}

/* foreach has to return every transaction in the map, each exactly once */
static int __txn_commit_map_check_foreach(DB_ENV *dbenv)
{
	DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;
	struct __txn_commit_map_collect c = {NULL, 0, txmap->nentries + 1};
	int64_t i;
	int err = 0;

	if ((c.ids = malloc(sizeof(u_int64_t) * c.cap)) == NULL) {
		return __LINE__;
	}
	if (__txn_commit_map_foreach_nolock(dbenv, __txn_commit_map_collect_one, &c) != 0 ||
	    c.n != txmap->nentries || c.n != __txn_commit_map_nentries_nolock(dbenv)) {
		err = __LINE__;
	}
	qsort(c.ids, c.n, sizeof(u_int64_t), __txn_commit_map_cmp_utxnid);
	for (i = 1; !err && i < c.n; i++) {
		if (c.ids[i] == c.ids[i - 1]) {
			err = __LINE__;
		}
	}
	for (i = 0; !err && i < c.n; i++) {
		if (__txn_commit_map_findp(txmap, c.ids[i]) == NULL) {
			err = __LINE__;
		}
	}
	free(c.ids);
	return err;
}

static int __txn_commit_map_has(DB_ENV *dbenv, u_int64_t utxnid, u_int32_t file)
{
	DB_LSN lsn;
	return __txn_commit_map_get(dbenv, utxnid, &lsn) == 0 && lsn.file == file;
}

#define CLM_SELFTEST(cond)                                                        \
	do {                                                                      \
		if (!(cond)) {                                                    \
			logmsg(LOGMSG_ERROR, "%s: line %d: %s\n", __func__,       \
			    __LINE__, #cond);                                     \
			ret = 1;                                                  \
			goto done;                                                \
		}                                                                 \
	} while (0)

/*
 * __txn_commit_map_selftest --
 *	Exercise a private commit LSN map: whole-logfile purges, removal of the
 *	head, middle, tail and only transaction of a logfile, and the hash
 *	growing and shrinking under it.  The live map is not touched.
 *
 * PUBLIC: int __txn_commit_map_selftest __P((void));
 */
int __txn_commit_map_selftest(void)
{
	DB_ENV *dbenv;
	DB_TXN_COMMIT_MAP *txmap;
	DB_LSN lsn = {0};
	u_int64_t u;
	u_int64_t base, nxt;
	u_int32_t f;
	int ret, i, k;

	if ((dbenv = calloc(1, sizeof(DB_ENV))) == NULL) {
		return ENOMEM;
	}
	dbenv->use_sys_malloc = 1;
	if ((ret = __txn_commit_map_init(dbenv)) != 0) {
		free(dbenv);
		return ret;
	}
	txmap = dbenv->txmap;

	/* Several transactions in one logfile, purged together */
	lsn.file = 50;
	for (u = 1; u <= 3; u++) {
		lsn.offset = u;
		CLM_SELFTEST(__txn_commit_map_add(dbenv, u, lsn) == 0);
	}
	CLM_SELFTEST(__txn_commit_map_check(txmap, 3, 1) == 0);
	for (u = 1; u <= 3; u++) {
		CLM_SELFTEST(__txn_commit_map_has(dbenv, u, 50));
	}
	CLM_SELFTEST(__txn_commit_map_delete_logfile_txns(dbenv, 50) == 0);
	for (u = 1; u <= 3; u++) {
		CLM_SELFTEST(!__txn_commit_map_has(dbenv, u, 50));
	}
	CLM_SELFTEST(__txn_commit_map_check(txmap, 0, 0) == 0);

	/* Purging a logfile leaves the other logfiles alone */
	lsn.file = 50;
	CLM_SELFTEST(__txn_commit_map_add(dbenv, 11, lsn) == 0);
	CLM_SELFTEST(__txn_commit_map_add(dbenv, 12, lsn) == 0);
	lsn.file = 51;
	CLM_SELFTEST(__txn_commit_map_add(dbenv, 13, lsn) == 0);
	lsn.file = 52;
	CLM_SELFTEST(__txn_commit_map_add(dbenv, 14, lsn) == 0);
	CLM_SELFTEST(__txn_commit_map_check(txmap, 4, 3) == 0);
	CLM_SELFTEST(__txn_commit_map_delete_logfile_txns(dbenv, 50) == 0);
	CLM_SELFTEST(!__txn_commit_map_has(dbenv, 11, 50) && !__txn_commit_map_has(dbenv, 12, 50));
	CLM_SELFTEST(__txn_commit_map_has(dbenv, 13, 51) && __txn_commit_map_has(dbenv, 14, 52));
	CLM_SELFTEST(__txn_commit_map_check(txmap, 2, 2) == 0);
	CLM_SELFTEST(__txn_commit_map_delete_logfile_txns(dbenv, 51) == 0);
	CLM_SELFTEST(__txn_commit_map_has(dbenv, 14, 52));
	CLM_SELFTEST(__txn_commit_map_check(txmap, 1, 1) == 0);
	CLM_SELFTEST(__txn_commit_map_delete_logfile_txns(dbenv, 52) == 0);
	CLM_SELFTEST(__txn_commit_map_delete_logfile_txns(dbenv, 52) != 0);
	CLM_SELFTEST(__txn_commit_map_check(txmap, 0, 0) == 0);

	/* Remove the middle, head, tail and then the only transaction of a logfile */
	lsn.file = 60;
	for (u = 21; u <= 25; u++) {
		CLM_SELFTEST(__txn_commit_map_add(dbenv, u, lsn) == 0);
	}
	CLM_SELFTEST(__txn_commit_map_remove(dbenv, 23) == 0);
	CLM_SELFTEST(!__txn_commit_map_has(dbenv, 23, 60) && __txn_commit_map_has(dbenv, 22, 60) &&
	    __txn_commit_map_has(dbenv, 24, 60));
	CLM_SELFTEST(__txn_commit_map_check(txmap, 4, 1) == 0);
	CLM_SELFTEST(__txn_commit_map_remove(dbenv, 21) == 0);
	CLM_SELFTEST(__txn_commit_map_check(txmap, 3, 1) == 0);
	CLM_SELFTEST(__txn_commit_map_remove(dbenv, 25) == 0);
	CLM_SELFTEST(__txn_commit_map_check(txmap, 2, 1) == 0);
	CLM_SELFTEST(__txn_commit_map_has(dbenv, 22, 60) && __txn_commit_map_has(dbenv, 24, 60));
	CLM_SELFTEST(__txn_commit_map_remove(dbenv, 22) == 0);
	CLM_SELFTEST(__txn_commit_map_check(txmap, 1, 1) == 0);
	CLM_SELFTEST(__txn_commit_map_remove(dbenv, 24) == 0);
	CLM_SELFTEST(__txn_commit_map_check(txmap, 0, 0) == 0);

	/* Duplicates, utxnid 0 and zero LSNs are not added */
	lsn.file = 70;
	CLM_SELFTEST(__txn_commit_map_add(dbenv, 31, lsn) == 0);
	CLM_SELFTEST(__txn_commit_map_add(dbenv, 31, lsn) == 0);
	CLM_SELFTEST(__txn_commit_map_add(dbenv, 0, lsn) == 0);
	ZERO_LSN(lsn);
	CLM_SELFTEST(__txn_commit_map_add(dbenv, 32, lsn) == 0);
	CLM_SELFTEST(__txn_commit_map_check(txmap, 1, 1) == 0);
	CLM_SELFTEST(__txn_commit_map_delete_logfile_txns(dbenv, 70) == 0);
	CLM_SELFTEST(__txn_commit_map_check(txmap, 0, 0) == 0);

	/* Many logfiles: purge one in the middle, then interleave removals and purges */
	for (f = 100; f < 120; f++) {
		lsn.file = f;
		for (k = 0; k < 100; k++) {
			lsn.offset = k + 1;
			CLM_SELFTEST(__txn_commit_map_add(dbenv, 1000 + f * 100 + k, lsn) == 0);
		}
	}
	CLM_SELFTEST(__txn_commit_map_check(txmap, 2000, 20) == 0);
	CLM_SELFTEST(__txn_commit_map_delete_logfile_txns(dbenv, 110) == 0);
	CLM_SELFTEST(!__txn_commit_map_has(dbenv, 1000 + 110 * 100, 110));
	CLM_SELFTEST(__txn_commit_map_has(dbenv, 1000 + 109 * 100 + 99, 109));
	CLM_SELFTEST(__txn_commit_map_has(dbenv, 1000 + 111 * 100, 111));
	CLM_SELFTEST(__txn_commit_map_check(txmap, 1900, 19) == 0);
	for (f = 100; f < 120; f++) {
		for (k = 1; k < 100 && f != 110; k += 2) {
			CLM_SELFTEST(__txn_commit_map_remove(dbenv, 1000 + f * 100 + k) == 0);
		}
	}
	CLM_SELFTEST(__txn_commit_map_check(txmap, 950, 19) == 0);
	/* the lowest and highest logfiles go first, to move both ends */
	CLM_SELFTEST(__txn_commit_map_delete_logfile_txns(dbenv, 100) == 0);
	CLM_SELFTEST(__txn_commit_map_delete_logfile_txns(dbenv, 119) == 0);
	CLM_SELFTEST(txmap->smallest_logfile == 101 && txmap->highest_logfile == 118);
	CLM_SELFTEST(__txn_commit_map_check(txmap, 850, 17) == 0);
	for (f = 101; f < 119; f++) {
		CLM_SELFTEST(f == 110 || __txn_commit_map_delete_logfile_txns(dbenv, f) == 0);
	}
	CLM_SELFTEST(__txn_commit_map_check(txmap, 0, 0) == 0);

	/* Repeated cycles of adds and removals, then purge what is left */
	for (i = 0; i < 1000; i++) {
		lsn.file = 200 + i % 5;
		for (k = 0; k < 20; k++) {
			lsn.offset = k + 1;
			CLM_SELFTEST(__txn_commit_map_add(dbenv, 100000 + i * 20 + k, lsn) == 0);
		}
		for (k = 0; k < 5; k++) {
			CLM_SELFTEST(__txn_commit_map_remove(dbenv, 100000 + i * 20 + k) == 0);
			CLM_SELFTEST(__txn_commit_map_remove(dbenv, 100000 + i * 20 + 19 - k) == 0);
		}
	}
	CLM_SELFTEST(__txn_commit_map_check(txmap, 10000, 5) == 0);
	for (f = 200; f < 205; f++) {
		CLM_SELFTEST(__txn_commit_map_delete_logfile_txns(dbenv, f) == 0);
	}
	CLM_SELFTEST(__txn_commit_map_check(txmap, 0, 0) == 0);

	/*
	 * The hash grows with the map, so every transaction has to be found
	 * after each resize; once most of the map is purged it shrinks back to
	 * fit what is left, and removals one at a time shrink it further.
	 */
	i = txmap->nresizes;
	for (f = 300; f < 310; f++) {
		lsn.file = f;
		for (k = 0; k < 10000; k++) {
			lsn.offset = k + 1;
			CLM_SELFTEST(__txn_commit_map_add(dbenv, 200000 + (f - 300) * 10000 + k, lsn) == 0);
		}
		CLM_SELFTEST(__txn_commit_map_check(txmap, (f - 300 + 1) * 10000, f - 300 + 1) == 0);
	}
	CLM_SELFTEST(txmap->nresizes > i && txmap->nentries <= ((int64_t) 1 << txmap->bits));
	CLM_SELFTEST(__txn_commit_map_check_foreach(dbenv) == 0);
	CLM_SELFTEST(__txn_commit_map_remove(dbenv, 200000 + 5 * 10000 + 17) == 0);
	CLM_SELFTEST(__txn_commit_map_add(dbenv, 200000 + 5 * 10000 + 17, lsn) == 0);
	CLM_SELFTEST(__txn_commit_map_has(dbenv, 200000 + 5 * 10000 + 17, 309));
	for (f = 300; f < 309; f++) {
		CLM_SELFTEST(__txn_commit_map_delete_logfile_txns(dbenv, f) == 0);
	}
	/* 10001 left: shrunk to the smallest table with at least two buckets per transaction */
	CLM_SELFTEST((1 << txmap->bits) >= 2 * 10001 && (1 << (txmap->bits - 1)) < 2 * 10001);
	CLM_SELFTEST(__txn_commit_map_check(txmap, 10001, 1) == 0);
	for (k = 0; k < 10000; k++) {
		CLM_SELFTEST(__txn_commit_map_has(dbenv, 200000 + 9 * 10000 + k, 309));
	}
	/* below 4096 of 32768 buckets, removals shrink it to 8192 */
	for (k = 0; k < 6000; k++) {
		CLM_SELFTEST(__txn_commit_map_remove(dbenv, 290000 + k) == 0);
	}
	CLM_SELFTEST((1 << txmap->bits) == 8192);
	CLM_SELFTEST(__txn_commit_map_check(txmap, 4001, 1) == 0 && __txn_commit_map_check_foreach(dbenv) == 0);
	CLM_SELFTEST(!__txn_commit_map_has(dbenv, 290000, 309) && __txn_commit_map_has(dbenv, 296000, 309));
	CLM_SELFTEST(__txn_commit_map_delete_logfile_txns(dbenv, 309) == 0);
	CLM_SELFTEST(__txn_commit_map_check(txmap, 0, 0) == 0 && txmap->bits == COMMIT_MAP_MIN_BITS);

	/*
	 * Recovery removing transactions one at a time, followed by new commits
	 * in the same logfile: the removed slots stay in the slabs, skipped by
	 * lookups, foreach and resizes, until the logfile is purged.
	 */
	lsn.file = 500;
	base = 1000000;
	for (k = 0; k < 10000; k++) {
		lsn.offset = k + 1;
		CLM_SELFTEST(__txn_commit_map_add(dbenv, base + k, lsn) == 0);
	}
	/* the first 1000 stay throughout and keep the logfile's list alive */
	nxt = base + 1000;
	for (i = 0; i < 5; i++) {
		for (k = 0; k < 9000; k++) {
			CLM_SELFTEST(__txn_commit_map_remove(dbenv, nxt + k) == 0);
		}
		nxt += 9000;
		for (k = 0; k < 9000; k++) {
			lsn.offset = 10000 + i * 9000 + k + 1;
			CLM_SELFTEST(__txn_commit_map_add(dbenv, nxt + k, lsn) == 0);
		}
		CLM_SELFTEST(__txn_commit_map_check(txmap, 10000, 1) == 0);
	}
	CLM_SELFTEST(__txn_commit_map_check(txmap, 10000, 1) == 0);
	CLM_SELFTEST(__txn_commit_map_check_foreach(dbenv) == 0);
	CLM_SELFTEST(__txn_commit_map_has(dbenv, base, 500) && __txn_commit_map_has(dbenv, nxt + 8999, 500));
	CLM_SELFTEST(__txn_commit_map_delete_logfile_txns(dbenv, 500) == 0);
	CLM_SELFTEST(__txn_commit_map_check(txmap, 0, 0) == 0 && txmap->nslots == 0 && txmap->nslabs == 0);

done:
	__txn_commit_map_destroy(dbenv);
	free(dbenv);
	return ret;
}
#endif

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
	assert(type == DB___txn_dist_prepare || type == DB___txn_dist_prepare_endianize);

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

	if (type == DB___txn_dist_prepare_endianize) {
		flags |= LOCK_GET_LIST_ENDIANIZE;
	}

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

struct __collect_disttxn_node
{
	char *coordinator_name;
	char *coordinator_tier;
	char *dist_txnid;
};

struct __collect_disttxns
{
	DB_ENV *dbenv;
	struct __collect_disttxn_node *disttxns;
	int count;
};

static int __collect_prepared(void *obj, void *arg)
{
	DB_TXN_PREPARED *p = (DB_TXN_PREPARED *)obj;
	struct __collect_disttxns *collect = (struct __collect_disttxns *)arg;
	int ret;
	if (F_ISSET(p, DB_DIST_HAVELOCKS)) {

		/* Collect coordinator name */
		if ((ret = __os_calloc(collect->dbenv, 1, p->coordinator_name.size + 1,
			&collect->disttxns[collect->count].coordinator_name)) != 0) {
			logmsg(LOGMSG_FATAL, "%s error allocating memory %d\n", __func__, ret);
			abort();
		}
		memcpy(collect->disttxns[collect->count].coordinator_name, p->coordinator_name.data,
			p->coordinator_name.size);

		/* Collect coordinator tier */
		if ((ret = __os_calloc(collect->dbenv, 1, p->coordinator_tier.size + 1,
			&collect->disttxns[collect->count].coordinator_tier)) != 0) {
			logmsg(LOGMSG_FATAL, "%s error allocating memory %d\n", __func__, ret);
			abort();
		}
		memcpy(collect->disttxns[collect->count].coordinator_tier, p->coordinator_tier.data,
			p->coordinator_tier.size);

		/* Collect dist_txnid */
		if ((ret = __os_strdup(collect->dbenv, p->dist_txnid, &collect->disttxns[collect->count].dist_txnid)) != 0) {
			logmsg(LOGMSG_FATAL, "%s error allocating memory %d\n", __func__, ret);
			abort();
		}
		collect->count++;
	}
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
 * recovery can discriminate between resolved and unresolved prepares.	We mark these
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
 * Master has prepared a transaction and written a prepare record.	Add this 
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

void __txn_set_prepared_discard(dbenv, dist_txnid)
	DB_ENV *dbenv;
	const char *dist_txnid;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	DB_TXN_PREPARED *p;

	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) == NULL) {
		logmsg(LOGMSG_FATAL, "%s cannot find prepared transaction %s\n", 
			__func__, dist_txnid);
		abort();
	}
	F_SET(p, DB_DIST_DISCARDED);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
}

/*
 * __txn_recover_abort_prepared --
 *
 * Remove an aborted transaction from the distributed transaction list.
 * This is db_dispatch with the DB_TXN_ABORT flag- it's called both for a normal 
 * txn->abort, and from txn_dbenv_refresh.	This later could abort a recovered
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
	DB_TXN_PREPARED *p;

	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	if ((p = hash_find(dbenv->prepared_txn_hash, &dist_txnid)) == NULL) {
		logmsg(LOGMSG_FATAL, "%s cannot find prepared transaction %s\n", 
			__func__, dist_txnid);
		abort();
	}
	if (F_ISSET(p, DB_DIST_DISCARDED)) {
		F_CLR(p, DB_DIST_DISCARDED);
		Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
		logmsg(LOGMSG_INFO, "%s retaining discarded transaction %s on downgrade\n", __func__, dist_txnid);
		return 0;
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
 * only run this for aborted and unresolved dist-txns.	There are two cases
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
		   !!! TODO !!! need to write dist-abort to blkseq	*/
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
#if defined (DEBUG_PREPARE)
	logmsg(LOGMSG_USER, "%s added prepared txn %s\n", __func__, dist_txnid);
#endif
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
 * __txn_recover_all_prepared --
 *
 * Contact coordinator for every recovered prepare through the callback 
 * function.
 *
 * PUBLIC: int __txn_recover_all_prepared __P((DB_ENV *));
 */
int __txn_recover_all_prepared(dbenv)
	DB_ENV *dbenv;
{
#if defined (DEBUG_PREPARE)
	comdb2_cheapstack_sym(stderr, "%s", __func__);
#endif
	if (dbenv->recover_prepared_callback == NULL) {
		logmsg(LOGMSG_ERROR, "%s recover_prepared_callback is not set\n", __func__);
		return -1;
	}

	int alloc, ret;
	struct __collect_disttxns collect = {0};
	collect.dbenv = dbenv;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	hash_info(dbenv->prepared_txn_hash, NULL, NULL, NULL, NULL, &alloc, NULL, NULL);
	if ((ret = __os_malloc(dbenv, sizeof(struct __collect_disttxn_node) * alloc, &collect.disttxns)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error allocating memory %d\n", __func__, ret);
		abort();
	}
	hash_for(dbenv->prepared_txn_hash, __collect_prepared, &collect);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	for (int i = 0; i < collect.count; i++) {
		dbenv->recover_prepared_callback(collect.disttxns[i].dist_txnid, collect.disttxns[i].coordinator_name,
			collect.disttxns[i].coordinator_tier);
		__os_free(dbenv, collect.disttxns[i].dist_txnid);
		__os_free(dbenv, collect.disttxns[i].coordinator_name);
		__os_free(dbenv, collect.disttxns[i].coordinator_tier);
	}
	__os_free(dbenv, collect.disttxns);
	return 0;
}

/*
 * __txn_collect_ddl_prepared --
 *
 * Collect DDL prepared transactions (those needing schema lock) that are
 * unresolved. These must be resolved before upgrade to avoid deadlock with
 * the schema lock held during initialization.
 *
 * PUBLIC: int __txn_collect_ddl_prepared __P((DB_ENV *, char ***, char ***, char ***, int *));
 */

struct __collect_ddl_disttxn_node {
	char *coordinator_name;
	char *coordinator_tier;
	char *dist_txnid;
};

struct __collect_ddl_disttxns {
	DB_ENV *dbenv;
	struct __collect_ddl_disttxn_node *disttxns;
	int count;
	int alloc;
};

static int __collect_ddl_prepared_cb(void *obj, void *arg)
{
	DB_TXN_PREPARED *p = (DB_TXN_PREPARED *)obj;
	struct __collect_ddl_disttxns *collect = (struct __collect_ddl_disttxns *)arg;
	int ret;

	if (!F_ISSET(p, DB_DIST_SCHEMA_LK))
		return 0;
	if (F_ISSET(p, DB_DIST_HAVELOCKS | DB_DIST_COMMITTED | DB_DIST_ABORTED))
		return 0;
	if (collect->count >= collect->alloc) {
		logmsg(LOGMSG_ERROR, "%s: exceeded allocated count %d\n", __func__, collect->alloc);
		return -1;
	}

	if ((ret = __os_calloc(collect->dbenv, 1, p->coordinator_name.size + 1,
		&collect->disttxns[collect->count].coordinator_name)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error allocating memory %d\n", __func__, ret);
		abort();
	}
	memcpy(collect->disttxns[collect->count].coordinator_name,
		p->coordinator_name.data, p->coordinator_name.size);

	if ((ret = __os_calloc(collect->dbenv, 1, p->coordinator_tier.size + 1,
		&collect->disttxns[collect->count].coordinator_tier)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error allocating memory %d\n", __func__, ret);
		abort();
	}
	memcpy(collect->disttxns[collect->count].coordinator_tier,
		p->coordinator_tier.data, p->coordinator_tier.size);

	if ((ret = __os_strdup(collect->dbenv, p->dist_txnid,
		&collect->disttxns[collect->count].dist_txnid)) != 0) {
		logmsg(LOGMSG_FATAL, "%s error allocating memory %d\n", __func__, ret);
		abort();
	}
	collect->count++;
	return 0;
}

int __txn_collect_ddl_prepared(dbenv, dist_txnids, coordinator_names,
	coordinator_tiers, count)
	DB_ENV *dbenv;
	char ***dist_txnids;
	char ***coordinator_names;
	char ***coordinator_tiers;
	int *count;
{
	int alloc;
	struct __collect_ddl_disttxns collect = {0};
	collect.dbenv = dbenv;

	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	hash_info(dbenv->prepared_txn_hash, NULL, NULL, NULL, NULL, &alloc, NULL, NULL);
	if (alloc == 0) {
		Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
		*count = 0;
		return 0;
	}

	int ret;
	if ((ret = __os_malloc(dbenv, sizeof(struct __collect_ddl_disttxn_node) * alloc,
		&collect.disttxns)) != 0) {
		Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
		return ret;
	}
	collect.alloc = alloc;
	hash_for(dbenv->prepared_txn_hash, __collect_ddl_prepared_cb, &collect);
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);

	*count = collect.count;
	if (collect.count == 0) {
		__os_free(dbenv, collect.disttxns);
		*dist_txnids = NULL;
		*coordinator_names = NULL;
		*coordinator_tiers = NULL;
		return 0;
	}

	*dist_txnids = NULL;
	*coordinator_names = NULL;
	*coordinator_tiers = NULL;

	if ((ret = __os_malloc(dbenv, sizeof(char *) * collect.count, dist_txnids)) != 0)
		goto err;
	if ((ret = __os_malloc(dbenv, sizeof(char *) * collect.count, coordinator_names)) != 0)
		goto err;
	if ((ret = __os_malloc(dbenv, sizeof(char *) * collect.count, coordinator_tiers)) != 0)
		goto err;

	for (int i = 0; i < collect.count; i++) {
		(*dist_txnids)[i] = collect.disttxns[i].dist_txnid;
		(*coordinator_names)[i] = collect.disttxns[i].coordinator_name;
		(*coordinator_tiers)[i] = collect.disttxns[i].coordinator_tier;
	}
	__os_free(dbenv, collect.disttxns);
	return 0;

err:
	for (int i = 0; i < collect.count; i++) {
		__os_free(dbenv, collect.disttxns[i].dist_txnid);
		__os_free(dbenv, collect.disttxns[i].coordinator_name);
		__os_free(dbenv, collect.disttxns[i].coordinator_tier);
	}
	__os_free(dbenv, collect.disttxns);
	if (*dist_txnids) __os_free(dbenv, *dist_txnids);
	if (*coordinator_names) __os_free(dbenv, *coordinator_names);
	if (*coordinator_tiers) __os_free(dbenv, *coordinator_tiers);
	*dist_txnids = NULL;
	*coordinator_names = NULL;
	*coordinator_tiers = NULL;
	*count = 0;
	return ret;
}

/*
 * __txn_mark_prepared_resolved --
 *
 * Mark a prepared transaction as committed or aborted without requiring
 * DB_DIST_HAVELOCKS. Used during startup to resolve DDL transactions before
 * upgrade. __txn_prune_resolved_prepared will remove them before upgrade.
 *
 * PUBLIC: int __txn_mark_prepared_resolved __P((DB_ENV *, const char *, int));
 */
int __txn_mark_prepared_resolved(dbenv, dist_txnid, committed)
	DB_ENV *dbenv;
	const char *dist_txnid;
	int committed;
{
	DB_TXN_PREPARED *p;
	Pthread_mutex_lock(&dbenv->prepared_txn_lk);
	p = hash_find(dbenv->prepared_txn_hash, &dist_txnid);
	if (p != NULL) {
		if (committed)
			F_SET(p, DB_DIST_COMMITTED);
		else
			F_SET(p, DB_DIST_ABORTED);
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	if (p == NULL) {
		logmsg(LOGMSG_ERROR, "%s unable to locate txnid %s\n", __func__, dist_txnid);
		return -1;
	}
	logmsg(LOGMSG_INFO, "%s marked DDL prepared txn %s as %s\n", __func__,
		dist_txnid, committed ? "committed" : "aborted");
	return 0;
}

/*
 * __txn_upgrade_all_prepared --
 *
 * Create berkley txns for unresolved but prepared transactions we found
 * during recovery.  Acquire locks for these transactions.	Update the
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
			logmsg(LOGMSG_DEBUG, "%s unable to abort unprepared dist-txn %s\n", __func__, dist_txnid);
			p = NULL;
		} else {
			hash_del(dbenv->prepared_txn_hash, p);
			hash_del(dbenv->prepared_utxnid_hash, p);
			__free_prepared_children(dbenv, p);
		}
	}
	Pthread_mutex_unlock(&dbenv->prepared_txn_lk);
	if (p == NULL) {
		logmsg(LOGMSG_DEBUG, "%s unable to locate txnid %s\n", __func__, dist_txnid);
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
	dist_txn_abort_write_blkseq(dbenv->app_private, p->blkseq_key.data, p->blkseq_key.size);

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
 * won't add the allocated pages to limbo.	We will add them to the freelist if
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
 * Replication saw a dist-abort for this prepared txn.	Write the blkseq
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

	dist_txn_abort_write_blkseq(dbenv->app_private, p->blkseq_key.data, p->blkseq_key.size);
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
 * and then subsequently unroll prepared transactions.	This routine returns
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

extern void lc_free(DB_ENV *dbenv, struct __recovery_processor *rp, LSN_COLLECTION * lc);

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
	void *txninfo = NULL;

	if ((ret = __os_malloc(dbenv, sizeof(LISTC_T(UTXNID)), &lc.child_utxnids)) != 0) {
		logmsg(LOGMSG_FATAL, "Error allocating memory for child-utxnids\n");
		abort();
	}
	listc_init(lc.child_utxnids, offsetof(UTXNID, lnk));

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

	/* Update commit-lsn map */
	Pthread_mutex_lock(&dbenv->txmap->txmap_mutexp);
	if (__txn_commit_map_enabled()) {

		if ((ret = __txn_commit_map_add_nolock(dbenv, p->utxnid, lsn_out)) != 0) {
			logmsg(LOGMSG_FATAL, "Error adding commit-lsn map for txn %"PRIu64"\n", p->utxnid);
			abort();
		}

		if (lc.child_utxnids != NULL) {
			UTXNID *elt;
			LISTC_FOR_EACH(lc.child_utxnids, elt, lnk) {
				if ((ret = __txn_commit_map_add_nolock(dbenv, elt->utxnid, lsn_out)) != 0) {
					logmsg(LOGMSG_FATAL, "Error adding commit-lsn map for txn %"PRIu64"\n", p->utxnid);
					abort();
				}
			}
		}
	}
	Pthread_mutex_unlock(&dbenv->txmap->txmap_mutexp);

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
	lc_free(dbenv, NULL, &lc);
	return 0;
}

void invalidate_modsnap_txns_starting_at_lsn_geq_cutoff_lsn(DB_ENV *dbenv, DB_LSN cutoff_lsn) {
	MODSNAP_TXN *txn;

	pthread_mutex_lock(&dbenv->outstanding_modsnap_lock);
	LISTC_FOR_EACH(&dbenv->outstanding_modsnaps, txn, lnk) {
		if (log_compare(&cutoff_lsn, &txn->modsnap_start_lsn) <= 0) {
			txn->is_allowed_to_open_cursors = 0;
		}
	}
	pthread_mutex_unlock(&dbenv->outstanding_modsnap_lock);
}

