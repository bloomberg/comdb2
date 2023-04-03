#include <db.h>
#include <db_int.h>
#include "dbinc_auto/os_ext.h"
#include "dbinc_auto/txn_auto.h"
#include "dbinc/db_swap.h"
#include "printformats.h"

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "list.h"
#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc_auto/dbreg_auto.h"
#include "dbinc_auto/fileops_auto.h"
#include "dbinc_auto/qam_auto.h"
#include "dbinc/txn.h"
#include "dbinc_auto/txn_ext.h"
#include "dbinc_auto/txn_auto.h"
#include "dbinc_auto/db_auto.h"
#include "dbinc_auto/btree_auto.h"
#include <assert.h>
#include "logmsg.h"
#include "locks_wrap.h"


/* TODO:
   [X] 1.  Nested transactions
   [ ] 2.  Overall memory limit/per transaction limit above which we stop?
   [ ] 3.  Use mspace (also gets us #2 for free)?
*/

int normalize_rectype(u_int32_t * rectype);

// PUBLIC: int __lc_cache_init __P((DB_ENV *, int));
int
__lc_cache_init(DB_ENV *dbenv, int reinit)
{
	int ret;

	if (!reinit) {
		Pthread_mutex_init(&dbenv->lc_cache.lk, NULL);
	} 

	Pthread_mutex_lock(&dbenv->lc_cache.lk);

	LC_CACHE *lcc;

	lcc = &dbenv->lc_cache;
	lcc->nent = 0;
	if ((ret =
		__os_calloc(dbenv, dbenv->attr.cache_lc_max,
		    sizeof(LC_CACHE_ENTRY), &lcc->ent)) != 0)
		goto err;
	listc_init(&lcc->lru, offsetof(LC_CACHE_ENTRY, lnk));
	listc_init(&lcc->avail, offsetof(LC_CACHE_ENTRY, lnk));
	lcc->txnid_hash = hash_init(sizeof(u_int32_t));
	for (int i = 0; i < dbenv->attr.cache_lc_max; i++) {
		lcc->ent[i].cacheid = i;
		lcc->ent[i].lc.memused = 0;
		listc_abl(&lcc->avail, &lcc->ent[i]);
	}
	lcc->nent = dbenv->attr.cache_lc_max;
	lcc->memused = 0;

	ret = 0;
err:
	Pthread_mutex_unlock(&dbenv->lc_cache.lk);
	return ret;
}

// PUBLIC: int __lc_cache_destroy __P((DB_ENV *));
int
__lc_cache_destroy(DB_ENV *dbenv)
{
	LC_CACHE *lcc;

	lcc = &dbenv->lc_cache;

	__os_free(dbenv, lcc->ent);
	if (lcc->txnid_hash)
		hash_free(lcc->txnid_hash);
	Pthread_mutex_destroy(&lcc->lk);

	return 0;
}

static void
free_lsn_collection(DB_ENV *dbenv, LSN_COLLECTION * lc)
{
	for (int i = 0; i < lc->nlsns; i++) {
		if (lc->array[i].rec.data) {
			__os_free(dbenv, lc->array[i].rec.data);
			lc->array[i].rec.data = NULL;
		}
	}
	__os_free(dbenv, lc->array);
	lc->array = 0;
	lc->nalloc = 0;
	lc->nlsns = 0;
}

static void
free_ent(DB_ENV *dbenv, LC_CACHE_ENTRY * e)
{
	if (e && e->txnid) {
#ifndef NDEBUG
		LC_CACHE_ENTRY *fnd;

		/* XXX remove this if you see it */
		assert((fnd =
			hash_find(dbenv->lc_cache.txnid_hash, &e->txnid)) == e);
#endif
		hash_del(dbenv->lc_cache.txnid_hash, e);
		free_lsn_collection(dbenv, &e->lc);
		e->txnid = 0;
		e->utxnid = 0;
		e->lc.had_serializable_records = 0;
		listc_abl(&dbenv->lc_cache.avail, e);
		dbenv->lc_cache.memused -= e->lc.memused;
		e->lc.memused = 0;
	}
}

static int
append_lsn_collection(DB_ENV *dbenv, LSN_COLLECTION * dest,
    LSN_COLLECTION * src)
{
	int ret;

	/* may need to resize the destination */
	if (dest->nalloc <= (src->nlsns + dest->nlsns)) {
		if ((ret =
			__os_realloc(dbenv,
			    (src->nlsns +
				dest->nlsns) * sizeof(struct logrecord),
			    &dest->array) != 0))
			goto err;
		dest->nalloc = src->nlsns + dest->nlsns;
	}
	for (int i = 0; i < src->nlsns; i++)
		dest->array[dest->nlsns + i] = src->array[i];
	dest->nlsns += src->nlsns;

	dest->had_serializable_records |= src->had_serializable_records;

	/* We'll free src shortly, which will adjust the accounting information - oversell it until
	 * that happens. */
	dest->memused += src->memused;
	dbenv->lc_cache.memused += src->memused;

	return 0;

err:
	return ret;
}

/* add a log record to an existing collection */
static int
lsn_collection_add(DB_ENV *dbenv, LSN_COLLECTION * lc, DB_LSN lsn, DBT *dbt)
{
	int ret;
	int nalloc;

	if (lc->nlsns >= lc->nalloc) {
		nalloc = lc->nalloc == 0 ? 20 : lc->nalloc * 2;
		if ((ret =
		    __os_realloc(dbenv, nalloc * sizeof(struct logrecord),
			&lc->array)) != 0)
			goto err;
		lc->nalloc = nalloc;
		for (int i = lc->nlsns; i < lc->nalloc; i++)
			lc->array[i].rec.flags = DB_DBT_MALLOC;
	}
	lc->array[lc->nlsns].lsn = lsn;
	lc->array[lc->nlsns].rec.size = dbt->size;
	if ((ret = __os_malloc(dbenv, dbt->size, &lc->array[lc->nlsns].rec.data)) != 0)
		goto err;
	memcpy(lc->array[lc->nlsns].rec.data, dbt->data,
	    lc->array[lc->nlsns].rec.size);
	lc->nlsns++;
	lc->memused += dbt->size;
	dbenv->lc_cache.memused += dbt->size;
	return 0;

err:
	return ret;
}

static int
lc_dump_cache(DB_ENV *dbenv, int needlock)
{

	if (needlock)
		Pthread_mutex_lock(&dbenv->lc_cache.lk);

	LC_CACHE_ENTRY *e;

	logmsg(LOGMSG_USER, "Total used: %d\n", dbenv->lc_cache.memused);
	for (int ent = 0; ent < dbenv->lc_cache.nent; ent++) {
		e = &dbenv->lc_cache.ent[ent];
		if (e->txnid) {
			logmsg(LOGMSG_USER, "%x ", e->txnid);
			logmsg(LOGMSG_USER, "mem %d ", e->lc.memused);
			for (int i = 0; i < e->lc.nlsns; i++) {
				logmsg(LOGMSG_USER, PR_LSN " (%d) ",
				    PARM_LSN(e->lc.array[i].lsn),
				    e->lc.array[i].rec.size);
			}
			logmsg(LOGMSG_USER, "\n");
		}
	}

	if (needlock)
		Pthread_mutex_unlock(&dbenv->lc_cache.lk);

	return 0;
}

// PUBLIC: int __lc_cache_feed __P((DB_ENV *, DB_LSN, DBT));
int
__lc_cache_feed(DB_ENV *dbenv, DB_LSN lsn, DBT dbt)
{
	LC_CACHE_ENTRY *e;
	int ret;

	u_int32_t type;
	u_int32_t txnid;
	u_int64_t utxnid = 0;
	DB_LSN prevlsn;
	uint8_t *logrec;

	Pthread_mutex_lock(&dbenv->lc_cache.lk);

	logrec = dbt.data;
	if (dbt.size <
	    sizeof(u_int32_t) /*type */ +sizeof(u_int32_t) /*txnid */
	    +sizeof(DB_LSN) /*prevlsn */ ) {
		/* don't know what this is, but it can't possibly contain enough information - not expected */
		/* TODO: this really shouldn't happen, buf if it does - find matching transaction in cache,
		 * invalidate? */
		if (dbt.size != 0) {
			__db_err(dbenv, "suspicious record size %d\n",
			    dbt.size);
			abort();
		}
		goto done;
	}
	LOGCOPY_32(&type, logrec);
	logrec += sizeof(u_int32_t);
	LOGCOPY_32(&txnid, logrec);
	logrec += sizeof(u_int32_t);
	LOGCOPY_TOLSN(&prevlsn, logrec);
	logrec += sizeof(DB_LSN);

	if (normalize_rectype(&type)) {
		LOGCOPY_64(&utxnid, logrec);
		logrec += sizeof(u_int64_t);
	}

	/* dump our current state */
	if (dbenv->attr.cache_lc_debug) {
		logmsg(LOGMSG_USER, ">> got txnid %x lsn " PR_LSN " prevlsn " PR_LSN
		    " utxnid \%"PRIx64" type %u sz %d\n", txnid, PARM_LSN(lsn), PARM_LSN(prevlsn),
		    utxnid, type, dbt.size);
	}

	/* Don't process txnid 0 transactions. */
	if (txnid == 0) {
		if (dbenv->attr.cache_lc_debug)
			logmsg(LOGMSG_USER, ">> got txnid 0 at lsn " PR_LSN "\n",
			    PARM_LSN(lsn));
		ret = 0;
		goto done;
	}

	if (type == DB___txn_regop || type == DB___txn_regop_gen ||
	    type == DB___txn_regop_rowlocks) {
		/* Transaction processing starts at the log record preceding the commit.
		 * That's the record that __rep_apply_txn will call __lc_cache_find with.
		 * Don't add the commit record to the list. */
		ret = 0;
		goto done;
	}

	/* Did the tunable change and we now want a different max size? */
	if (dbenv->attr.cache_lc_max != dbenv->lc_cache.nent) {
		/* TODO: test all this - looks suspicious. */
		if (dbenv->attr.cache_lc_debug)
			logmsg(LOGMSG_USER, ">> resizing cache\n");
		/* clear lru and avail entries, throw them away since this cache is 
		 * just an optimization */
		do {
			e = listc_rtl(&dbenv->lc_cache.lru);
			if (e) {
				free_ent(dbenv, e);
			}
		} while (e);
		do {
			e = listc_rtl(&dbenv->lc_cache.avail);
			/* Don't free - we free things before they make it on the avail list. */
		} while (e);

		__os_free(dbenv, dbenv->lc_cache.ent);
		/* recreate */
		__lc_cache_init(dbenv, 1);
		e = NULL;
	}

	/* find matching transaction */
	if ((e = hash_find(dbenv->lc_cache.txnid_hash, &txnid)) != NULL) {
		if (dbenv->attr.cache_lc_debug)
			logmsg(LOGMSG_USER, ">> txnid %x matched, txn prevlsn " PR_LSN
			    " cache prevlsn " PR_LSN "\n", txnid,
			    PARM_LSN(prevlsn), PARM_LSN(e->last_seen_lsn));
		/* Found a transaction that we already have a record for.  If it's in sequence,
		 * save the record.  If it's not, we can't cache this anymore. */
		if (log_compare(&e->last_seen_lsn, &prevlsn) != 0) {
			if (dbenv->attr.cache_lc_debug ||
			    dbenv->attr.cache_lc_trace_evictions)
				logmsg(LOGMSG_USER, ">> txnid %x got lsn " PR_LSN
				    " but expected " PR_LSN "\n", txnid,
				    PARM_LSN(lsn), PARM_LSN(e->last_seen_lsn));
			/* Sanity check - look at the collections last lsn */
			assert(0 ==
			    log_compare(&e->lc.array[e->lc.nlsns - 1].lsn,
				&e->last_seen_lsn));

			listc_rfl(&dbenv->lc_cache.lru, e);
			free_ent(dbenv, e);
			ret = 0;
			goto err;
		} else {
			/* Don't append __txn_child - but we still need to add the child
			 * transaction's log, which happens below. */
			if (type != DB___txn_child) {
				/* all is well, append to lsn collection */
				if ((dbenv->attr.cache_lc_memlimit &&
					dbenv->lc_cache.memused + dbt.size >
					dbenv->attr.cache_lc_memlimit) ||
				    (dbenv->attr.cache_lc_memlimit_tran &&
					e->lc.memused + dbt.size >
					dbenv->attr.cache_lc_memlimit_tran)) {
					listc_rfl(&dbenv->lc_cache.lru, e);
					free_ent(dbenv, e);
					if (dbenv->attr.cache_lc_debug ||
					    dbenv->attr.
					    cache_lc_trace_evictions) {
						logmsg(LOGMSG_USER, 
                            "total lc cache %d (limit %d) tran %d  (limit %d) next %d\n",
						    dbenv->lc_cache.memused,
						    dbenv->attr.
						    cache_lc_memlimit,
						    e->lc.memused,
						    dbenv->attr.
						    cache_lc_memlimit_tran,
						    dbt.size);
					}
					ret = 0;
					goto err;
				}

				ret =
				    lsn_collection_add(dbenv, &e->lc, lsn,
				    &dbt);
				if (dbenv->attr.cache_lc_debug)
					logmsg(LOGMSG_USER, ">> txnid %x got lsn " PR_LSN
					    ", appending to cache ret %d\n",
					    txnid, PARM_LSN(lsn), ret);
				if (ret)
					goto err;
			}
			e->last_seen_lsn = lsn;
		}
		/* move to tail of lru */
		listc_rfl(&dbenv->lc_cache.lru, e);
		listc_abl(&dbenv->lc_cache.lru, e);
		__rep_classify_type(type, &e->lc.had_serializable_records);
	}
	if (e == NULL) {
		if ((dbenv->attr.cache_lc_memlimit &&
			dbenv->lc_cache.memused + dbt.size >
			dbenv->attr.cache_lc_memlimit) ||
		    (dbenv->attr.cache_lc_memlimit_tran &&
			dbt.size > dbenv->attr.cache_lc_memlimit_tran)) {
			if (dbenv->attr.cache_lc_debug) {
				logmsg(LOGMSG_USER, 
                    "total lc cache %d (limit %d) tran 0 (limit %d) next %d\n",
				    dbenv->lc_cache.memused,
				    dbenv->attr.cache_lc_memlimit,
				    dbenv->attr.cache_lc_memlimit_tran,
				    dbt.size);
			}
			ret = 0;
			goto err;
		}

		if (dbenv->attr.cache_lc_debug)
			logmsg(LOGMSG_USER, ">> didn't find txnid %x\n", txnid);
		/* We didn't find it.  If it's a first record for a transaction, add to cache */
		if (IS_ZERO_LSN(prevlsn)) {
			if (dbenv->attr.cache_lc_debug)
				logmsg(LOGMSG_USER, ">> txnid %x first lsn of transaction\n",
				    txnid);
			e = listc_rtl(&dbenv->lc_cache.avail);
			if (e == NULL) {
				if (dbenv->attr.cache_lc_debug)
					logmsg(LOGMSG_USER, ">> didn't found item on available list\n");
				/* No room, let's remove an existing entry. */
				if (dbenv->attr.cache_lc_debug ||
				    dbenv->attr.cache_lc_trace_evictions)
					logmsg(LOGMSG_USER, ">> LRUing out data for txnid %x\n",
					    txnid);
				e = listc_rtl(&dbenv->lc_cache.lru);
				free_ent(dbenv, e);
				e = listc_rtl(&dbenv->lc_cache.avail);
			}
			if (e == NULL) {
				/* Still no room? Well, THAT shouldn't happen... */
				__db_err(dbenv,
				    "no items available after lru?\n");
				__db_panic(dbenv, EINVAL);
			}
			e->txnid = txnid;
			e->utxnid = utxnid;
			e->last_seen_lsn = lsn;

			if (type != DB___txn_child) {
				ret =
				    lsn_collection_add(dbenv, &e->lc, lsn,
				    &dbt);

				if (ret) {
					if (dbenv->attr.cache_lc_debug)
						logmsg(LOGMSG_USER, ">> txnid %x new transaction added to cache ret %d\n",
						    txnid, ret);

					/* don't leak if we can't add to collection - not likely to happen */
					free_ent(dbenv, e);
					listc_abl(&dbenv->lc_cache.avail, e);
					ret = 0;
					goto err;
				}
				__rep_classify_type(type,
				    &e->lc.had_serializable_records);

				if (dbenv->attr.cache_lc_debug)
					logmsg(LOGMSG_USER, ">> txnid %x new transaction added to cache ret %d\n",
					    txnid, ret);
			}
			listc_abl(&dbenv->lc_cache.lru, e);
			hash_add(dbenv->lc_cache.txnid_hash, e);
		} else {
			if (dbenv->attr.cache_lc_debug ||
			    dbenv->attr.cache_lc_trace_misses)
				logmsg(LOGMSG_USER, ">> txnid %x not found and lsn " PR_LSN
				    " not first, not adding\n", txnid,
				    PARM_LSN(prevlsn));
			ret = 0;
			goto err;
		}
	}

	/* If this is a commit of a child transaction, we have a bit more work to do -
	 * we want to move all the child's records into the parent's collection, and get
	 * rid of the child. */
	if (type == DB___txn_child) {
		u_int32_t child_txnid;
		DB_LSN c_lsn;

		LOGCOPY_32(&child_txnid, logrec);
		logrec += sizeof(u_int32_t);
		LOGCOPY_TOLSN(&c_lsn, logrec);
		logrec += sizeof(DB_LSN);

		if (dbenv->attr.cache_lc_debug)
			logmsg(LOGMSG_USER, "child commit for parent %x at child c_lsn "
			    PR_LSN "\n", txnid, PARM_LSN(c_lsn));

		/* See if we have the child entry - if not, throw away the parent, it's
		 * useless */
		LC_CACHE_ENTRY *ce;
		int found_child = 0;

		for (int child = 0; child < dbenv->lc_cache.nent; child++) {
			ce = &dbenv->lc_cache.ent[child];
			/* if we have the child, and it's last LSN is what we expect, we're done */
			if (ce->txnid == child_txnid &&
			    log_compare(&ce->last_seen_lsn, &c_lsn) == 0) {
				/* Gotcha, kid. */
				found_child = 1;

				if (dbenv->attr.cache_lc_debug)
					logmsg(LOGMSG_USER, "found child txn %x\n",
					    ce->txnid);

				if (dbenv->attr.cache_lc_memlimit &&
				    e->lc.memused + ce->lc.memused >
				    dbenv->attr.cache_lc_memlimit) {
					listc_rfl(&dbenv->lc_cache.lru, e);
					free_ent(dbenv, e);
					listc_rfl(&dbenv->lc_cache.lru, ce);
					free_ent(dbenv, ce);
					ret = 0;
					goto err;
				}

				/* We don't want to reallocate any of those log records - just move over
				 * the LSN_COLLECTION pointers. */
				if ((ret =
					append_lsn_collection(dbenv, &e->lc,
					    &ce->lc)) != 0) {
					if (dbenv->attr.cache_lc_debug)
						logmsg(LOGMSG_USER, "couldn't append child txn %x to parent %x\n",
						    ce->txnid, txnid);

					/* get rid of child and parent in this unlikely case */
					listc_rfl(&dbenv->lc_cache.lru, e);
					free_ent(dbenv, e);
					listc_rfl(&dbenv->lc_cache.lru, ce);
					free_ent(dbenv, ce);
					ret = 0;
					goto err;
				}

				/* We're done with the child. Zap the child's nlsns so free_ent doesn't free it
				 * since we now have pointers to it inside the parent. */
				ce->lc.nlsns = 0;
				listc_rfl(&dbenv->lc_cache.lru, ce);
				free_ent(dbenv, ce);
				break;
			}
		}
		if (found_child == 0) {
			if (dbenv->attr.cache_lc_debug ||
			    dbenv->attr.cache_lc_trace_evictions)
				logmsg(LOGMSG_USER, "didn't find child, removed parent\n");

			/* If we didn't find the child, we can't continue caching the parent, get rid of it. */
			listc_rfl(&dbenv->lc_cache.lru, e);
			free_ent(dbenv, e);
			ret = 0;
			goto err;
		}
	}


	if (dbenv->lc_cache.lru.count + dbenv->lc_cache.avail.count !=
	    dbenv->lc_cache.nent) {
		__db_err(dbenv,
		    "dbenv->lc_cache.lru.count %d + dbev->lc_cache.avail.count %d != dbenv->lc_cache->nent %d",
		    dbenv->lc_cache.lru.count, dbenv->lc_cache.avail.count,
		    dbenv->lc_cache.nent);
		ret = EINVAL;

		goto err;
	}

	if (dbenv->attr.cache_lc_debug)
		lc_dump_cache(dbenv, 0);

done:
	ret = 0;
err:
	Pthread_mutex_unlock(&dbenv->lc_cache.lk);
	return ret;
}

/*
 * This is a destructive get: it removes the item from cache, and freeing the associated
 * LSN_COLLECTION becomes the responsibility of the caller.  The caller should treat this call
 * as __rep_collect_txn - in other words should qsort the resulting records.
 * PUBLIC: int __lc_cache_get __P((DB_ENV *, DB_LSN *, LSN_COLLECTION *, u_int32_t));
 */
int
__lc_cache_get(DB_ENV *dbenv, DB_LSN *lsnp, LSN_COLLECTION * lcout,
    u_int32_t txnid)
{
	LC_CACHE_ENTRY *e;

	Pthread_mutex_lock(&dbenv->lc_cache.lk);

	if (dbenv->attr.cache_lc_debug)
		logmsg(LOGMSG_USER, "looking for " PR_LSN "\n", PARM_LSNP(lsnp));
	// lc_dump_cache(dbenv);
	// XXX Use hash find here */
	if (txnid && (e = hash_find(dbenv->lc_cache.txnid_hash, &txnid)) != NULL) {
		if (log_compare(&e->last_seen_lsn, lsnp) == 0) {
			if (dbenv->attr.cache_lc_debug)
				logmsg(LOGMSG_USER, "matched " PR_LSN " txn %x\n",
				    PARM_LSNP(lsnp), e->txnid);

			*lcout = e->lc;
			/* lcout now has a copy of lc which must be freed by the caller. */
			e->lc.nlsns = 0;
			e->lc.nalloc = 0;
			e->lc.array = NULL;

			e->lc.had_serializable_records = 0;
			e->txnid = 0;
			e->utxnid = 0;
			// printf("%d %d\n", dbenv->lc_cache.memused, e->lc.memused);
			dbenv->lc_cache.memused -= e->lc.memused;
			e->lc.memused = 0;

			ZERO_LSN(e->last_seen_lsn);
			listc_rfl(&dbenv->lc_cache.lru, e);
			listc_abl(&dbenv->lc_cache.avail, e);


			ZERO_LSN(*lsnp);

			Pthread_mutex_unlock(&dbenv->lc_cache.lk);
			return 0;
		} else {
			if (dbenv->attr.cache_lc_debug ||
			    dbenv->attr.cache_lc_trace_misses)
				logmsg(LOGMSG_USER, "Found txnid %x but wanted " PR_LSN
				    " (got " PR_LSN ")\n", txnid,
				    PARM_LSN(e->last_seen_lsn),
				    PARM_LSNP(lsnp));
		}
	}
	if (dbenv->attr.cache_lc_debug || dbenv->attr.cache_lc_trace_misses)
		logmsg(LOGMSG_USER, "didn't find txnid %x, " PR_LSN "\n", txnid,
		    PARM_LSNP(lsnp));

	Pthread_mutex_unlock(&dbenv->lc_cache.lk);
	return DB_NOTFOUND;
}

void
__lc_cache_stat(DB_ENV *dbenv)
{
	lc_dump_cache(dbenv, 1);
}
