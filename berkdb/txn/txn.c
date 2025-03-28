/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
/*
 * Copyright (c) 1995, 1996
 *	The President and Fellows of Harvard University.  All rights reserved.
 *
 * This code is derived from software contributed to Berkeley by
 * Margo Seltzer.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "db_config.h"
#include <pthread.h>

#ifndef lint
static const char revid[] = "$Id: txn.c,v 11.219 2003/12/03 14:33:06 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <unistd.h>


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

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/hmac.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/hash.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"
#include "dbinc_auto/txn_ext.h"

#include "printformats.h"
#include "dbinc/db_swap.h"
#include "logmsg.h"
#include "sys_wrap.h"
#include "txn_properties.h"

#ifndef TESTSUITE

#include <thread_util.h>
#include <bdbglue.h>
extern unsigned long long get_commit_context(const void *, uint32_t generation);
int comdb2_time_epoch(void);
void ctrace(char *format, ...);

int __txn_commit_map_add_nolock(DB_ENV *, u_int64_t, DB_LSN);

extern int gbl_is_physical_replicant;
extern int gbl_fullrecovery;
extern int gbl_recovery_gen;
extern int gbl_reproduce_ckp_bug;
int gbl_recovery_ckp = 1;
extern int get_commit_lsn_map_switch_value();

#else

#define BDB_WRITELOCK(x)
#define BDB_RELLOCK()

#endif

#if defined (UFID_HASH_DEBUG)
void comdb2_cheapstack_sym(FILE *f, char *fmt, ...);
#endif

static int __txn_begin_int_with_prop(DB_TXN *txn, struct txn_properties *prop,
	DB_LSN *we_start_at_this_lsn, u_int32_t flags);

extern int __lock_locker_getpriority(DB_LOCKTAB *lt, u_int32_t locker,
	int *priority);
extern int __rep_check_applied_lsns(DB_ENV *dbenv, LSN_COLLECTION * lc,
	int inrecovery);
extern int dumptxn(DB_ENV *, DB_LSN *);
extern void fsnapf(FILE * fil, const void *buf, int len);

/* Track transactions open by a single td (should remain small) */
#define TXN_TD_MAX 10
static __thread int txncnt = 0;
static __thread DB_TXN *td_txn[TXN_TD_MAX];

#define	SET_LOG_FLAGS_ROWLOCKS(dbenv, txnp, ltranflags, lflags) \
	do {								\
		lflags = DB_LOG_COMMIT;			\
		if (ltranflags & DB_TXN_LOGICAL_COMMIT) \
			lflags |= DB_LOG_PERM; \
		if (F_ISSET(txnp, TXN_SYNC))				\
			lflags |= DB_FLUSH;				\
		else if (!F_ISSET(txnp, TXN_NOSYNC) &&			\
			!F_ISSET(dbenv, DB_ENV_TXN_NOSYNC)) {		\
			if (F_ISSET(dbenv, DB_ENV_TXN_WRITE_NOSYNC))	\
				lflags |= DB_LOG_WRNOSYNC;		\
			else						\
				lflags |= DB_FLUSH;			\
		}							\
		if (LF_ISSET(DB_TXN_REP_ACK))	   \
			lflags |= DB_LOG_REP_ACK; \
	} while (0)


#define	SET_LOG_FLAGS(dbenv, txnp, lflags)				\
	do {								\
		lflags = DB_LOG_COMMIT | DB_LOG_PERM;			\
		if (F_ISSET(txnp, TXN_SYNC))				\
			lflags |= DB_FLUSH;				\
		else if (!F_ISSET(txnp, TXN_NOSYNC) &&			\
			!F_ISSET(dbenv, DB_ENV_TXN_NOSYNC)) {		\
			if (F_ISSET(dbenv, DB_ENV_TXN_WRITE_NOSYNC))	\
				lflags |= DB_LOG_WRNOSYNC;		\
			else						\
				lflags |= DB_FLUSH;			\
		}							\
		if (LF_ISSET(DB_TXN_REP_ACK))	   \
			lflags |= DB_LOG_REP_ACK; \
	} while (0)

/*
 * __txn_isvalid enumerated types.  We cannot simply use the transaction
 * statuses, because different statuses need to be handled differently
 * depending on the caller.
 */
typedef enum {
	TXN_OP_ABORT,
	TXN_OP_COMMIT,
	TXN_OP_DISCARD,
	TXN_OP_PREPARE
} txnop_t;

static int __txn_abort_pp __P((DB_TXN *));
static int __txn_begin_int __P((DB_TXN *, u_int32_t));
static int __txn_commit_pp __P((DB_TXN *, u_int32_t));
static int __txn_logbytes_pp __P((DB_TXN *, u_int64_t *));
static int __txn_set_discard __P((DB_TXN *));
static int __txn_commit_getlsn_pp __P((DB_TXN *, u_int32_t, u_int64_t *, DB_LSN *, void *));
static int __txn_commit_rl_pp __P((DB_TXN *, u_int32_t, u_int64_t, u_int32_t,
	DB_LSN *, DBT *, DB_LOCK *, u_int32_t, u_int64_t *, DB_LSN *, DB_LSN *, void *));
static int __txn_dist_prepare_pp __P((DB_TXN *, const char *, const char *, const char*, u_int32_t, DBT *, DB_LSN *, u_int32_t));
static int __txn_discard_pp __P((DB_TXN *, u_int32_t));
static int __txn_end __P((DB_TXN *, int));
static int __txn_isvalid __P((const DB_TXN *, TXN_DETAIL **, txnop_t));
int __txn_undo __P((DB_TXN *));
static int __txn_dispatch_undo __P((DB_ENV *,
	DB_TXN *, DBT *, DB_LSN *, void *));



/*
 * __txn_begin_pp --
 *	DB_ENV->txn_begin pre/post processing.
 *
 * PUBLIC: int __txn_begin_pp __P((DB_ENV *, DB_TXN *, DB_TXN **, u_int32_t));
 */
int
__txn_begin_pp_int(dbenv, parent, txnpp, flags, prop)
	DB_ENV *dbenv;
	DB_TXN *parent, **txnpp;
	u_int32_t flags;
	struct txn_properties *prop;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv, dbenv->tx_handle, "txn_begin", DB_INIT_TXN);

	if ((ret = __db_fchk(dbenv,
		"txn_begin", flags,
		DB_DIRTY_READ | DB_TXN_NOWAIT |
		DB_TXN_NOSYNC | DB_TXN_SYNC | DB_TXN_RECOVERY |
		DB_TXN_INTERNAL)) != 0)
		return (ret);
	if ((ret = __db_fcchk(dbenv,
		"txn_begin", flags, DB_TXN_NOSYNC, DB_TXN_SYNC)) != 0)
		return (ret);

	if (parent == NULL) {
		rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
		if (rep_check)
			__op_rep_enter(dbenv);
	} else
		rep_check = 0;
	ret = __txn_begin_with_prop(dbenv, parent, txnpp, flags, prop);
	/*
	 * We only decrement the count if the operation fails.
	 * Otherwise the count will be decremented when the
	 * txn is resolved by txn_commit, txn_abort, etc.
	 */
	if (ret != 0 && rep_check)
		__op_rep_exit(dbenv);

	return (ret);
}

int
__txn_begin_pp(dbenv, parent, txnpp, flags)
	DB_ENV *dbenv;
	DB_TXN *parent, **txnpp;
	u_int32_t flags;
{
	return __txn_begin_pp_int(dbenv, parent, txnpp, flags, 0);
}

/* Nightmare-recovery test might unleak a txn from a different td */
int gbl_abort_on_unfound_txn = 1;

static void remove_td_txn(DB_TXN *txn)
{
	int found = 0;
	for (int i = 0; i < txncnt; i++) {
		if (td_txn[i] == txn) {
			found = 1;
			td_txn[i] = td_txn[--txncnt];
			break;
		}
	}
	if (!found && gbl_abort_on_unfound_txn) {
		logmsg(LOGMSG_FATAL, "%s unable to locate td txn %p\n", __func__,
				txn);
		abort();
	}
}

 /*
  *
  * PUBLIC: int __txn_begin_with_prop_pp __P((DB_ENV *, DB_TXN *, DB_TXN **, u_int32_t, struct txn_properties *));
  */
int
__txn_begin_with_prop_pp(dbenv, parent, txnpp, flags, prop)
	DB_ENV *dbenv;
	DB_TXN *parent, **txnpp;
	u_int32_t flags;
	struct txn_properties *prop;
{
	return __txn_begin_pp_int(dbenv, parent, txnpp, flags, prop);
}

int bdb_txn_pglogs_init(void *bdb_state, void **pglogs_hashtbl,
	pthread_mutex_t * mutexp);
int bdb_txn_pglogs_close(void *bdb_state, void **pglogs_hashtbl,
	pthread_mutex_t * mutexp);

/*
 * __txn_begin --
 *	DB_ENV->txn_begin.
 *
 * This is a wrapper to the actual begin process.  Normal transaction begin
 * allocates a DB_TXN structure for the caller, while XA transaction begin
 * does not.  Other than that, both call into common __txn_begin_int code.
 *
 * Internally, we use TXN_DETAIL structures, but the DB_TXN structure
 * provides access to the transaction ID and the offset in the transaction
 * region of the TXN_DETAIL structure.
 *
 * PUBLIC: int __txn_begin __P((DB_ENV *, DB_TXN *, DB_TXN **, u_int32_t, struct txn_properties *));
 */
int
__txn_begin_main(dbenv, parent, txnpp, flags, prop)
	DB_ENV *dbenv;
	DB_TXN *parent, **txnpp;
	u_int32_t flags;
	struct txn_properties *prop;
{
	DB_LOCKREGION *region;
	DB_TXN *txn;
	int ret;
	DB_LSN we_start_at_this_lsn;

	/*
	 * if (prop && prop->retries)
	 * fprintf(stderr, "txn_begin_main retries %d\n", prop->retries);
	 */

	*txnpp = NULL;

	if ((ret = __os_calloc(dbenv, 1, sizeof(DB_TXN), &txn)) != 0)
		return (ret);

	txn->mgrp = dbenv->tx_handle;
	txn->parent = parent;
	listc_init(&txn->committed_kids, offsetof(UTXNID, lnk));	
	TAILQ_INIT(&txn->kids);
	TAILQ_INIT(&txn->events);
	STAILQ_INIT(&txn->logs);
	txn->flags = TXN_MALLOC;
	if (LF_ISSET(DB_DIRTY_READ))
		F_SET(txn, TXN_DIRTY_READ);
	if (LF_ISSET(DB_TXN_NOSYNC))
		F_SET(txn, TXN_NOSYNC);
	if (LF_ISSET(DB_TXN_SYNC))
		F_SET(txn, TXN_SYNC);
	if (LF_ISSET(DB_TXN_NOWAIT))
		F_SET(txn, TXN_NOWAIT);
	if (prop && prop->flags & DB_TXN_FOP_NOBLOCK) 
		F_SET(txn, TXN_FOP_NOBLOCK);

	if ((ret =
		__txn_begin_int_with_prop(txn, prop,
			&we_start_at_this_lsn, flags)) != 0)
		goto err;

	if (!parent) {
		txn->we_start_at_this_lsn = we_start_at_this_lsn;
	}

	if (parent != NULL)
		TAILQ_INSERT_HEAD(&parent->kids, txn, klinks);

	if (LOCKING_ON(dbenv)) {
		region = ((DB_LOCKTAB *)dbenv->lk_handle)->reginfo.primary;
		if (parent != NULL) {
			ret = __lock_inherit_timeout(dbenv,
				parent->txnid, txn->txnid);
			/* No parent locker set yet. */
			if (ret == EINVAL) {
				parent = NULL;

				ret = 0;
			}
			if (ret != 0)
				goto err;
		}

		/*
		 * Parent is NULL if we have no parent
		 * or it has no timeouts set.
		 */
		if (parent == NULL && region->tx_timeout != 0)
			if ((ret = __lock_set_timeout(dbenv, txn->txnid,
				region->tx_timeout, DB_SET_TXN_TIMEOUT)) != 0)
				goto err;
	}

	ret = bdb_txn_pglogs_init(dbenv->app_private, &txn->pglogs_hashtbl,
		&txn->pglogs_mutex);
	if (ret)
		goto err;

	*txnpp = txn;

	if (LOGGING_ON(dbenv) && dbenv->tx_perfect_ckp) {
		/* We set the pthread key to the DB_TXN so that
		   we can trace back to the parent DB_TXN to get its
		   begin LSN. Do not rush to get the begin LSN as
		   it may not be needed (eg, readonly txn's). */
		if (pthread_getspecific(txn_key) == NULL)
			Pthread_setspecific(txn_key, (void *)txn);
	}
	return (0);

err:
	__os_free(dbenv, txn);
	return (ret);
}

int
__txn_begin(dbenv, parent, txnpp, flags)
	DB_ENV *dbenv;
	DB_TXN *parent, **txnpp;
	u_int32_t flags;
{
	return __txn_begin_main(dbenv, parent, txnpp, flags, NULL);
}

int
__txn_begin_with_prop(dbenv, parent, txnpp, flags, prop)
	DB_ENV *dbenv;
	DB_TXN *parent, **txnpp;
	u_int32_t flags;
	struct txn_properties *prop;
{
	return __txn_begin_main(dbenv, parent, txnpp, flags, prop);
}

/*
 * __txn_xa_begin --
 *	XA version of txn_begin.
 *
 * PUBLIC: int __txn_xa_begin __P((DB_ENV *, DB_TXN *));
 */
int
__txn_xa_begin(dbenv, txn)
	DB_ENV *dbenv;
	DB_TXN *txn;
{
	PANIC_CHECK(dbenv);

	/*
	 * We need to initialize the transaction structure, but we must
	 * be careful not to smash the links.  We manually intialize the
	 * structure.
	 */
	txn->mgrp = dbenv->tx_handle;
	TAILQ_INIT(&txn->kids);
	TAILQ_INIT(&txn->events);
	STAILQ_INIT(&txn->logs);
	txn->parent = NULL;
	ZERO_LSN(txn->last_lsn);
	txn->txnid = TXN_INVALID;
	txn->utxnid = TXN_INVALID;
	txn->tid = 0;
	txn->cursors = 0;
	memset(&txn->lock_timeout, 0, sizeof(db_timeout_t));
	memset(&txn->expire, 0, sizeof(db_timeout_t));

	return (__txn_begin_int(txn, 0));
}

/*
 * __txn_compensate_begin
 *	Begin an compensation transaction.  This is a special interface
 * that is used only for transactions that must be started to compensate
 * for actions during an abort.  Currently only used for allocations.
 *
 * PUBLIC: int __txn_compensate_begin __P((DB_ENV *, DB_TXN **txnp));
 */
int
__txn_compensate_begin(dbenv, txnpp)
	DB_ENV *dbenv;
	DB_TXN **txnpp;
{
	DB_TXN *txn;
	int ret;

	PANIC_CHECK(dbenv);

	if ((ret = __os_calloc(dbenv, 1, sizeof(DB_TXN), &txn)) != 0)
		return (ret);

	txn->mgrp = dbenv->tx_handle;
	TAILQ_INIT(&txn->kids);
	TAILQ_INIT(&txn->events);
	STAILQ_INIT(&txn->logs);
	txn->flags = TXN_COMPENSATE | TXN_MALLOC;

	*txnpp = txn;
	return (__txn_begin_int(txn, DB_TXN_INTERNAL));
}

/* Abort if this thread has an open transaction */
static void __txn_assert_notran(dbenv)
	DB_ENV *dbenv;
{
	if (txncnt > 0) {
		logmsg(LOGMSG_FATAL, "%s td has open txns\n", __func__);
		abort();
	}
}

/*
 * __txn_assert_notran_pp --
 *	Abort if this thread has an open transaction.
 */
int __txn_assert_notran_pp(dbenv)
	DB_ENV *dbenv;
{
	int rep_check;
	PANIC_CHECK(dbenv);
	if (IS_REP_CLIENT(dbenv))
		return (0);
	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	__txn_assert_notran(dbenv);
	if (rep_check)
		__env_rep_exit(dbenv);
	return 0;
}

int
__txn_recycle_after_upgrade_prepared(dbenv)
	DB_ENV *dbenv;
{
	u_int32_t *ids = NULL;
	int nids;
	DB_LSN null_lsn;
	TXN_DETAIL *td;
	DB_TXNMGR *mgr = dbenv->tx_handle;
	DB_TXNREGION *region = mgr->reginfo.primary;
	int ret;
	if ((ret = __os_malloc(dbenv,
		sizeof(u_int32_t) * (region->maxtxns + 1), &ids)) != 0)
		goto err;

	R_LOCK(dbenv, &mgr->reginfo);
	nids = 0;
	for (td = SH_TAILQ_FIRST(&region->active_txn, __txn_detail);
		td != NULL;
		td = SH_TAILQ_NEXT(td, links, __txn_detail)) {
		ids[nids++] = td->txnid;
	}
	region->last_txnid = TXN_MINIMUM - 1;
	region->cur_maxid = TXN_MAXIMUM;

	if (nids != 0) {
		__db_idspace(ids, nids,
			&region->last_txnid, &region->cur_maxid);
	}

	if (DBENV_LOGGING(dbenv) && (ret = __txn_recycle_log(dbenv, NULL, &null_lsn,
			0, region->last_txnid, region->cur_maxid)) != 0) {
		 goto err;
	}

err:
	R_UNLOCK(dbenv, &mgr->reginfo);
	if (ids != NULL) {
		__os_free(dbenv, ids);
	}
	return ret;
}

/*
 * __txn_begin_int --
 *	Normal DB version of txn_begin.
 */
static int
__txn_begin_int_int(txn, prop, we_start_at_this_lsn, flags)
	DB_TXN *txn;
	struct txn_properties *prop;
	DB_LSN *we_start_at_this_lsn;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	DB_LSN begin_lsn, null_lsn;
	DB_TXNMGR *mgr;
	DB_TXNREGION *region;
	TXN_DETAIL *td;
	size_t off;
	u_int32_t id, *ids;
	int nids, ret;
	int internal = LF_ISSET(DB_TXN_INTERNAL);
	int recovery = LF_ISSET(DB_TXN_RECOVERY);
	uint64_t utxnid = 0;
	u_int32_t prepared_txnid = prop ? prop->prepared_txnid : 0;
	u_int64_t prepared_utxnid = prop ? prop->prepared_utxnid : 0;

	/*
	 * if (prop && prop->retries)
	 * fprintf(stderr, "begin_int_int with retries %d\n", prop->retries);
	 */

	mgr = txn->mgrp;
	dbenv = mgr->dbenv;

	/* Fail big for now.  I want to sniff this out */
	if (!dbenv) {
		abort();
	}

	region = mgr->reginfo.primary;

	if (!prepared_utxnid) {
		Pthread_mutex_lock(&dbenv->utxnid_lock);
		utxnid = ++dbenv->next_utxnid;
		Pthread_mutex_unlock(&dbenv->utxnid_lock);
	}

	/*
	 * We do not have to write begin records (and if we do not, then we
	 * need never write records for read-only transactions).  However,
	 * we do need to find the current LSN so that we can store it in the
	 * transaction structure, so we can know where to take checkpoints.
	 *
	 * XXX
	 * We should set this value when we write the first log record, not
	 * here.
	 */
	if (DBENV_LOGGING(dbenv)) {
		if (prop && prop->begin_lsn.file) {
			begin_lsn = prop->begin_lsn;
		} else {
			__log_txn_lsn(dbenv, &begin_lsn, NULL, NULL);
		}
	} else
		ZERO_LSN(begin_lsn);

	if (we_start_at_this_lsn)
		*we_start_at_this_lsn = begin_lsn;

	if (!recovery) {
		dbenv->lock_recovery_lock(dbenv, __func__, __LINE__);
	}

	R_LOCK(dbenv, &mgr->reginfo);
	if (!F_ISSET(txn, TXN_COMPENSATE) && F_ISSET(region, TXN_IN_RECOVERY)) {
		__db_err(dbenv, "operation not permitted during recovery");
		ret = EINVAL;
		goto err;
	}

	/* Make sure that we aren't still recovering prepared transactions. */
	if (!internal && region->stat.st_nrestores != 0) {
		__db_err(dbenv,
	"recovery of prepared but not yet committed transactions is incomplete");
		ret = EINVAL;
		goto err;
	}

	/*
	 * Allocate a new transaction id. Our current valid range can span
	 * the maximum valid value, so check for it and wrap manually.
	 */
	if (region->last_txnid == TXN_MAXIMUM &&
		region->cur_maxid != TXN_MAXIMUM)
		region->last_txnid = TXN_MINIMUM - 1;

	/* Loop here for prepared_txnid's here as a sanity check */
	if (prepared_txnid || ((region->last_txnid + 1) == region->cur_maxid)) {
		if ((ret = __os_malloc(dbenv,
			sizeof(u_int32_t) * (region->maxtxns + 1), &ids)) != 0)
			goto err;
		nids = 0;
		for (td = SH_TAILQ_FIRST(&region->active_txn, __txn_detail);
			td != NULL;
			td = SH_TAILQ_NEXT(td, links, __txn_detail)) {
			ids[nids++] = td->txnid;
			if (prepared_txnid == td->txnid) {
				logmsg(LOGMSG_FATAL, "%s prepared txnid %"PRIu32" already in txnlist\n",
					__func__, prepared_txnid); 
				abort();
			}
		}
		/* Write recycle for non-prepared case.  We emit a single recycle after upgrading
		   all prepared records in txn_recycle_after_upgrade_prepared */
		if ((region->last_txnid + 1) == region->cur_maxid) {
			region->last_txnid = TXN_MINIMUM - 1;
			region->cur_maxid = TXN_MAXIMUM;
			if (nids != 0)
				__db_idspace(ids, nids,
					&region->last_txnid, &region->cur_maxid);
			__os_free(dbenv, ids);
			/* Don't emit log until after we upgrade all prepared txns */
			if (DBENV_LOGGING(dbenv) &&
				(ret = __txn_recycle_log(dbenv, NULL,
					&null_lsn, 0, region->last_txnid,
					region->cur_maxid)) != 0)
				 goto err;
		}
	}

	/* Allocate a new transaction detail structure. */
#ifdef TESTSUITE
	td = calloc(1, sizeof(TXN_DETAIL));
#else
	if ((ret =
		__db_shalloc(mgr->reginfo.addr, sizeof(TXN_DETAIL), 0,
			&td)) != 0) {
		__db_err(dbenv,
			"Unable to allocate memory for transaction detail");
		goto err;
	}
#endif

	/* Place transaction on active transaction list. */
	SH_TAILQ_INSERT_HEAD(&region->active_txn, td, links, __txn_detail);

	id = prepared_txnid ? prepared_txnid : ++region->last_txnid;

	++region->stat.st_nbegins;
	if (++region->stat.st_nactive > region->stat.st_maxnactive)
		region->stat.st_maxnactive = region->stat.st_nactive;

	td->txnid = id;
	ZERO_LSN(td->last_lsn);
	td->begin_lsn = begin_lsn;
	if (txn->parent != NULL)
		td->parent = txn->parent->off;
	else
		td->parent = INVALID_ROFF;
	td->status = TXN_RUNNING;
	td->flags = 0;
	td->xa_status = 0;
	td->tid = pthread_self();

	off = R_OFFSET(&mgr->reginfo, td);
	R_UNLOCK(dbenv, &mgr->reginfo);

	ZERO_LSN(txn->last_lsn);
	txn->txnid = id;
	txn->off = off;

	txn->abort = __txn_abort_pp;
	txn->commit = __txn_commit_pp;
	txn->getlogbytes = __txn_logbytes_pp;
    txn->set_discard = __txn_set_discard;
	txn->commit_getlsn = __txn_commit_getlsn_pp;
	txn->commit_rowlocks = __txn_commit_rl_pp;
	txn->dist_prepare = __txn_dist_prepare_pp;
	txn->discard = __txn_discard_pp;
	txn->id = __txn_id;
	txn->prepare = __txn_prepare;
	txn->set_timeout = __txn_set_timeout;

	if (!recovery)
		F_SET(txn, TXN_RECOVER_LOCK);

	/*
	 * If this is a transaction family, we must link the child to the
	 * maximal grandparent in the lock table for deadlock detection.
	 */
	if (txn->parent != NULL && LOCKING_ON(dbenv))
		if ((ret = __lock_addfamilylocker_with_prop(dbenv,
				txn->parent->txnid, txn->txnid, prop)) != 0)
			return (ret);

	/* TODO : this isn't valid when we recover prepared txns */
	if (txncnt >= TXN_TD_MAX) {
		logmsg(LOGMSG_FATAL, "%s insane number of open txns for this td\n",
				__func__);
		__log_flush(dbenv, NULL);
		abort();
	}
	if (prepared_utxnid)
		txn->utxnid = prepared_utxnid;
	else
		txn->utxnid = utxnid;

	td_txn[txncnt++] = txn;

	if (F_ISSET(txn, TXN_MALLOC)) {
		MUTEX_THREAD_LOCK(dbenv, mgr->mutexp);
		TAILQ_INSERT_TAIL(&mgr->txn_chain, txn, links);
		MUTEX_THREAD_UNLOCK(dbenv, mgr->mutexp);
	}

	return (0);

err:
	R_UNLOCK(dbenv, &mgr->reginfo);
	if (!recovery) {
		dbenv->unlock_recovery_lock(dbenv, __func__, __LINE__);
	}
	return (ret);
}


static int
__txn_begin_int(txn, flags)
	DB_TXN *txn;
	u_int32_t flags;
{
	return __txn_begin_int_int(txn, NULL, NULL, flags);
}

static int
__txn_begin_int_with_prop(txn, prop, we_start_at_this_lsn, flags)
	DB_TXN *txn;
	struct txn_properties *prop;
	DB_LSN *we_start_at_this_lsn;
	u_int32_t flags;
{
	return __txn_begin_int_int(txn, prop, we_start_at_this_lsn, flags);
}

int

__txn_regop_log_commit(DB_ENV *dbenv, DB_TXN *txnid, DB_LSN *ret_lsnp,
	u_int64_t * ret_contextp, u_int32_t flags, u_int32_t opcode,
	int32_t timestamp, const DBT *locks, void *usr_ptr);

int

__txn_regop_gen_log(DB_ENV *dbenv, u_int32_t rectype, DB_TXN *txnid, DB_LSN *ret_lsnp,
	u_int64_t * ret_contextp, u_int32_t flags, u_int32_t opcode,
	u_int32_t generation, u_int64_t timestamp, const DBT *locks, void *usr_ptr);


int

__txn_regop_rowlocks_log(DB_ENV *dbenv, u_int32_t rectype, DB_TXN *txnid, DB_LSN *ret_lsnp,
	u_int64_t *ret_contextp, u_int32_t flags, u_int32_t opcode,
	u_int64_t ltranid, DB_LSN *begin_lsn, DB_LSN *last_commit_lsn,
	u_int64_t timestamp, u_int32_t lflags, u_int32_t generation,
	const DBT *locks, const DBT *rowlocks, void *usr_ptr);

static int
__txn_check_applied_lsns(DB_ENV *dbenv, DB_TXN *txnp)
{
	LSN_COLLECTION lc = { 0 };
	DB_LSN lsn;
	int unused;
	int ret;

	/* __rep_collect_txn overrides the input LSN, so make a copy and use that */
	lsn = txnp->last_lsn;
	ret = __rep_collect_txn(dbenv, &lsn, &lc, &unused, NULL);

	if (ret) {
		/* this should probably also be fatal? */
		__db_err(dbenv,
			"Unable to collect log records for txn %x, lsn " PR_LSN
			"\n", txnp->txnid, PARM_LSN(lsn));
	} else {
		/* collected log records successfully - now check that they've been applied */
		ret = __rep_check_applied_lsns(dbenv, &lc, 0);
	}

	free(lc.array);
	return ret;
}

#ifdef LTRANS_DEBUG
#include <walkback.h>

static int
__txn_track_stack_info(sinfo)
	struct track_stack_info *sinfo;
{
	int ret;

	ret = stack_pc_getlist(NULL, sinfo->stack, 100, &sinfo->nframes);

	sinfo->tid = pthread_self();
	sinfo->time = time(NULL);

	sinfo->ret = ret;
	return ret;
}
#endif

int
__txn_count_ltrans(dbenv, count)
	DB_ENV *dbenv;
	u_int32_t *count;
{
	Pthread_mutex_lock(&dbenv->ltrans_active_lk);
	*count = listc_size(&dbenv->active_ltrans);
	Pthread_mutex_unlock(&dbenv->ltrans_active_lk);
	return 0;
}

/* This is used for checkpoint */
int
__txn_ltrans_find_lowest_lsn(dbenv, lsnp)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
{
	LTDESC *lt, *lttemp;

	ZERO_LSN(*lsnp);
	Pthread_mutex_lock(&dbenv->ltrans_active_lk);
	LISTC_FOR_EACH_SAFE(&dbenv->active_ltrans, lt, lttemp, lnk) {
		if (IS_ZERO_LSN(*lsnp) || log_compare(&lt->begin_lsn, lsnp) < 0)
			(*lsnp) = lt->begin_lsn;
	}
	Pthread_mutex_unlock(&dbenv->ltrans_active_lk);

	return (0);
}

int
__txn_allocate_ltrans(dbenv, ltranid, begin_lsn, rlt)
	DB_ENV *dbenv;
	unsigned long long ltranid;
	DB_LSN *begin_lsn;
	LTDESC **rlt;
{
	LTDESC *lt = NULL;
	int ret = 0;

	Pthread_mutex_lock(&dbenv->ltrans_inactive_lk);
	lt = listc_rtl(&dbenv->inactive_ltrans);
	Pthread_mutex_unlock(&dbenv->ltrans_inactive_lk);

	if (!lt) {
		if ((ret = __os_calloc(dbenv, 1, sizeof(LTDESC), &lt)) != 0)
			goto err;
		Pthread_mutex_init(&lt->lk, NULL);
		Pthread_cond_init(&lt->wait, NULL);
	}

	lt->ltranid = ltranid;
	lt->lockcnt = 0;
	lt->flags = 0;
	lt->begin_lsn = *begin_lsn;

	Pthread_mutex_lock(&dbenv->ltrans_hash_lk);
#ifdef LTRANS_DEBUG
	LTDESC *hflt;
	assert(!(hflt = hash_find(dbenv->ltrans_hash, lt)));
#endif
	hash_add(dbenv->ltrans_hash, lt);
	Pthread_mutex_unlock(&dbenv->ltrans_hash_lk);

	Pthread_mutex_lock(&dbenv->ltrans_active_lk);
	listc_abl(&dbenv->active_ltrans, lt);
	Pthread_mutex_unlock(&dbenv->ltrans_active_lk);

#ifdef LTRANS_DEBUG
	__txn_track_stack_info(&lt->allocate_info);
#endif

err:
	(*rlt) = lt;

	return (0);
}

int
__txn_get_ltran_list(dbenv, rlist, rcount)
	DB_ENV *dbenv;
	DB_LTRAN **rlist;
	u_int32_t *rcount;
{
	int ret, idx, count;
	LTDESC *lt, *lttemp;
	DB_LTRAN *list = NULL;

	ret = idx = 0;

	Pthread_mutex_lock(&dbenv->ltrans_active_lk);
	count = listc_size(&dbenv->active_ltrans);
	if (count == 0)
		goto done;

	if ((ret = __os_malloc(dbenv, count * sizeof(DB_LTRAN), &list)) != 0)
		goto err;

	LISTC_FOR_EACH_SAFE(&dbenv->active_ltrans, lt, lttemp, lnk) {
		list[idx].tranid = lt->ltranid;
		list[idx].begin_lsn = lt->begin_lsn;
		list[idx].last_lsn = lt->last_lsn;
		idx++;
	}

done:
	*rlist = list;
	*rcount = count;

err:
	Pthread_mutex_unlock(&dbenv->ltrans_active_lk);
	return (ret);
}


int
__txn_find_ltrans(dbenv, ltranid, rlt)
	DB_ENV *dbenv;
	unsigned long long ltranid;
	LTDESC **rlt;
{
	LTDESC *lt = NULL;

	Pthread_mutex_lock(&dbenv->ltrans_hash_lk);
	lt = hash_find(dbenv->ltrans_hash, &ltranid);
	Pthread_mutex_unlock(&dbenv->ltrans_hash_lk);
	(*rlt) = lt;
	return (lt) ? 0 : -1;
}

/* XXX 3 mutexes to allocate or deallocate? */
void
__txn_deallocate_ltrans(dbenv, lt)
	DB_ENV *dbenv;
	LTDESC *lt;
{
	assert(lt->active_txn_count == 0);
	lt->flags = 0;

	Pthread_mutex_lock(&dbenv->ltrans_hash_lk);
#ifdef LTRANS_DEBUG
	LTDESC *hflt;
	assert(hflt = hash_find(dbenv->ltrans_hash, lt));
#endif
	hash_del(dbenv->ltrans_hash, lt);
	Pthread_mutex_unlock(&dbenv->ltrans_hash_lk);

	Pthread_mutex_lock(&dbenv->ltrans_active_lk);
	listc_rfl(&dbenv->active_ltrans, lt);
	Pthread_mutex_unlock(&dbenv->ltrans_active_lk);

	Pthread_mutex_lock(&dbenv->ltrans_inactive_lk);
	listc_abl(&dbenv->inactive_ltrans, lt);
	Pthread_mutex_unlock(&dbenv->ltrans_inactive_lk);
}
extern int gbl_new_snapisol;
extern int gbl_new_snapisol_logging;
int bdb_transfer_txn_pglogs(void *bdb_state, void *pglogs_hashtbl, 
	pthread_mutex_t *mutexp, DB_LSN commit_lsn, uint32_t flags,
	unsigned long long logical_tranid, int32_t timestamp,
	unsigned long long context);
int __lock_set_parent_has_pglk_lsn(DB_ENV *dbenv, u_int32_t parentid, u_int32_t lockid);

/* This prevents dbreg logs from being logged between the LOCK_PUT_READ and
 * the commit record */
extern pthread_rwlock_t gbl_dbreg_log_lock;
#if defined DEBUG_STACK_AT_TXN_LOG
void comdb2_cheapstack_sym(FILE *f, char *fmt, ...);
#endif

extern int gbl_fullrecovery;
int gbl_endianize_locklist = 1;

/*
 * __txn_commit --
 *	Commit a transaction.
 *
 * PUBLIC: int __txn_commit __P((DB_TXN *, u_int32_t));
 */
static int
__txn_commit_int(txnp, flags, ltranid, llid, last_commit_lsn, rlocks, inlks,
	nrlocks, logbytes, begin_lsn, lsn_out, usr_ptr)
	DB_TXN *txnp;
	u_int32_t flags;
	u_int64_t ltranid;
	u_int32_t llid;
	DB_LSN *last_commit_lsn;
	DBT *rlocks;
	DB_LOCK *inlks;
	u_int32_t nrlocks;
	u_int64_t *logbytes;
	DB_LSN *begin_lsn;
	DB_LSN *lsn_out;
	void *usr_ptr;
{
	DBT list_dbt;
	DBT list_dbt_rl;
	DB_ENV *dbenv;
	DB_REP *db_rep;
	REP *rep;
	DB_LOCKREQ request;
	DB_TXN *kid;
	LTDESC *lt = NULL;
	TXN_DETAIL *td = NULL, *ptd = NULL;
	UTXNID *utxnid_track;
	u_int32_t lflags, ltranflags = 0;
	int32_t timestamp;
	uint32_t gen;
	u_int64_t context = 0;
	int ret, t_ret, elect_highest_committed_gen, commit_lsn_map;
	int endianize_locklist = gbl_endianize_locklist;

	dbenv = txnp->mgrp->dbenv;
	commit_lsn_map = get_commit_lsn_map_switch_value();

	PANIC_CHECK(dbenv);

	if ((ret = __txn_isvalid(txnp, &td, TXN_OP_COMMIT)) != 0) {
		return (ret);
	}

	/*
	 * We clear flags that are incorrect, ignoring any flag errors, and
	 * default to synchronous operations.  By definition, transaction
	 * handles are dead when we return, and this error should never
	 * happen, but we don't want to fail in the field 'cause the app is
	 * specifying the wrong flag for some reason.
	 */
	if (__db_fchk(dbenv,
		"DB_TXN->commit", flags,
		DB_TXN_LOGICAL_BEGIN | DB_TXN_LOGICAL_COMMIT | DB_TXN_NOSYNC |
		DB_TXN_SYNC | DB_TXN_REP_ACK | DB_TXN_DONT_GET_REPO_MTX |
		DB_TXN_SCHEMA_LOCK | DB_TXN_LOGICAL_GEN | DB_TXN_DIST_PREPARE |
		DB_TXN_DIST_UPD_SHADOWS) != 0)
		flags = DB_TXN_SYNC;
	if (__db_fcchk(dbenv,
		"DB_TXN->commit", flags, DB_TXN_NOSYNC, DB_TXN_SYNC) != 0)
		flags = DB_TXN_SYNC;
	if (LF_ISSET(DB_TXN_NOSYNC)) {
		F_CLR(txnp, TXN_SYNC);
		F_SET(txnp, TXN_NOSYNC);
	}
	if (LF_ISSET(DB_TXN_SYNC)) {
		F_CLR(txnp, TXN_NOSYNC);
		F_SET(txnp, TXN_SYNC);
	}
	if (dbenv->attr.sync_standalone && dbenv->check_standalone &&
			dbenv->check_standalone(dbenv)) {
		LF_CLR(DB_TXN_NOSYNC);
		LF_SET(DB_TXN_SYNC);
		F_CLR(txnp, TXN_NOSYNC);
		F_SET(txnp, TXN_SYNC);
	}

	int is_prepare = LF_ISSET(DB_TXN_DIST_PREPARE);
	int commit_prepared = F_ISSET(txnp, TXN_DIST_PREPARED);

	if (is_prepare) {
		if (commit_prepared) {
			__db_err(dbenv,
					"transaction is already prepared");
			return EINVAL;
		}
	}

	/*
	 * This was written to run on replicants, thus the rep_* calls
	 * - it should still work on the master, though it's a strange
	 * layer violation.
	 */
	if (dbenv->attr.check_applied_lsns) {
		int c_ret;

		c_ret = __txn_check_applied_lsns(dbenv, txnp);
		if (c_ret && dbenv->attr.check_applied_lsns_fatal)
			return __db_panic(dbenv, c_ret);
	}

    /* don't let full recovery write a (higher) generation: it will force this newly-recovered node to be master on the next election */
	elect_highest_committed_gen = (dbenv->attr.elect_highest_committed_gen && !gbl_fullrecovery);
	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	/*
	 * Commit any unresolved children.  If anyone fails to commit,
	 * then try to abort the rest of the kids and then abort the parent.
	 * Abort should never fail; if it does, we bail out immediately.
	 * Skip this check for prepared transactions.
	 */
	if (!commit_prepared) {
		while ((kid = TAILQ_FIRST(&txnp->kids)) != NULL)
			if ((ret = __txn_commit(kid, flags)) != 0)
				while ((kid = TAILQ_FIRST(&txnp->kids)) != NULL)
					if ((t_ret = __txn_abort(kid)) != 0)
						return (__db_panic(dbenv, t_ret));
	}

	/*
	 * If there are any log records, write a log record and sync the log,
	 * else do no log writes.  If the commit is for a child transaction,
	 * we do not need to commit the child synchronously since it may still
	 * abort (if its parent aborts), and otherwise its parent or ultimate
	 * ancestor will write synchronously.
	 */
#ifndef NDEBUG
	int iszero = 0;
	if (IS_ZERO_LSN(txnp->last_lsn)) {
		/* Put a breakpoint here */
		iszero = 1;
		(void)iszero;
	}
#endif

	extern int gbl_dumptxn_at_commit;

	if (gbl_dumptxn_at_commit && !is_prepare)
		dumptxn(dbenv, &txnp->last_lsn);

	if (DBENV_LOGGING(dbenv) && (!IS_ZERO_LSN(txnp->last_lsn) ||
		STAILQ_FIRST(&txnp->logs) != NULL)) {
		if (txnp->parent == NULL) {
			/*
			 * We are about to free all the read locks for this
			 * transaction below.  Some of those locks might be
			 * handle locks which should not be freed, because
			 * they will be freed when the handle is closed. Check
			 * the events and preprocess any trades now so we don't
			 * release the locks below.
			 */
			if ((ret =
				__txn_doevents(dbenv, txnp, TXN_PREPARE,
					1)) != 0) {
				goto err;
			}

			memset(&request, 0, sizeof(request));
			memset(&list_dbt_rl, 0, sizeof(list_dbt_rl));

			if (!is_prepare)
				Pthread_rwlock_rdlock(&gbl_dbreg_log_lock);

			if (LOCKING_ON(dbenv)) {
				u_int32_t flags = endianize_locklist ? DB_LOCK_ENDIANIZE : 0;
				request.op = is_prepare ? DB_LOCK_PREPARE : DB_LOCK_PUT_READ;
				if (IS_REP_MASTER(dbenv) &&
					!IS_ZERO_LSN(txnp->last_lsn)) {
					memset(&list_dbt, 0, sizeof(list_dbt));
					request.obj = &list_dbt;
				}
				ret = __lock_vec(dbenv,
					txnp->txnid, flags, &request, 1, NULL);

				assert(ret == 0);
			}

			if (ret == 0 && !IS_ZERO_LSN(txnp->last_lsn)) {
				if(ltranid != 0) {
					int ignore = 0;
					ltranflags = (flags & 
							  (DB_TXN_LOGICAL_BEGIN |
							   DB_TXN_LOGICAL_COMMIT |
							   DB_TXN_SCHEMA_LOCK));

					SET_LOG_FLAGS_ROWLOCKS(dbenv,
								   txnp,
								   ltranflags,
								   lflags);

					if ((ltranflags &
						 DB_TXN_LOGICAL_BEGIN) &&
						(ltranflags &
						 DB_TXN_LOGICAL_COMMIT))
						ignore = 1;

					if (!ignore &&
						(ltranflags &
						 DB_TXN_LOGICAL_BEGIN)) {
						if ((ret = 
							 __txn_allocate_ltrans(
								 dbenv, 
								 ltranid,
								 begin_lsn,
								 &lt)) != 0) {
							if (!is_prepare)
								Pthread_rwlock_unlock(&gbl_dbreg_log_lock);
							goto err;
						}
					}

					else if (!ignore && (ret =
						__txn_find_ltrans(dbenv,
							ltranid, &lt)) != 0) {
						logmsg(LOGMSG_FATAL, "Couldn't find ltrans?");
						abort();
						if (!is_prepare)
							Pthread_rwlock_unlock(&gbl_dbreg_log_lock);
						goto err;
					}

					assert(lsn_out);

					timestamp = comdb2_time_epoch();

					if (elect_highest_committed_gen) {
						MUTEX_LOCK(dbenv,
							db_rep->rep_mutexp);
						gen = rep->gen;
						MUTEX_UNLOCK(dbenv,
							db_rep->rep_mutexp);
						ltranflags |= DB_TXN_LOGICAL_GEN;
					} else
						gen = 0;

					if (commit_prepared) {

						DBT dist_txnid = {0};
						dist_txnid.data = txnp->dist_txnid;
						dist_txnid.size = strlen(txnp->dist_txnid);

						ret = __txn_dist_commit_log(dbenv,
									txnp, lsn_out, &context, lflags,
									&dist_txnid, gen, timestamp, usr_ptr);
						if ((ret = __txn_discard_recovered(dbenv, txnp->dist_txnid)) != 0) {
							abort();
						}
					} else {
						u_int32_t rectype = endianize_locklist ? 
							DB___txn_regop_rowlocks_endianize :
							DB___txn_regop_rowlocks;
						ret =
							__txn_regop_rowlocks_log(dbenv, rectype,
									txnp, lsn_out, &context, lflags,
									TXN_COMMIT, ltranid, begin_lsn,
									last_commit_lsn, timestamp,
									ltranflags, gen, request.obj,
									&list_dbt_rl, usr_ptr);
					}
#if defined DEBUG_STACK_AT_TXN_LOG
					comdb2_cheapstack_sym(stderr, "COMMIT-RL TXNID %x LSN [%d:%d]",
							txnp->txnid, lsn_out->file, lsn_out->offset);
#endif

					if (elect_highest_committed_gen) {
						MUTEX_LOCK(dbenv,
							db_rep->rep_mutexp);
						rep->committed_gen = gen;
						rep->committed_lsn = txnp->last_lsn;
						MUTEX_UNLOCK(dbenv,
							db_rep->rep_mutexp);
					}

					txnp->last_lsn = *lsn_out;

					if (!ignore && ret == 0) {
						lt->last_lsn = txnp->last_lsn;

						if (ltranflags &
							DB_TXN_LOGICAL_COMMIT) {
							__txn_deallocate_ltrans(
								dbenv, lt);
						}
					}
				} else {
					SET_LOG_FLAGS(dbenv, txnp, lflags);
					timestamp = comdb2_time_epoch();

					if (is_prepare) {
						MUTEX_LOCK(dbenv,
							db_rep->rep_mutexp);
						gen = rep->gen;
						MUTEX_UNLOCK(dbenv,
							db_rep->rep_mutexp);
						DBT coordinator = {0}, tier = {0}, dist_txnid = {0};
						coordinator.data = txnp->coordinator_name;
						coordinator.size = strlen(txnp->coordinator_name);
						tier.data = txnp->coordinator_tier;
						tier.size = strlen(txnp->coordinator_tier);
						dist_txnid.data = txnp->dist_txnid;
						dist_txnid.size = strlen(txnp->dist_txnid);
						SET_LOG_FLAGS_ROWLOCKS(dbenv, txnp, ltranflags, lflags);
						ltranflags |= LF_ISSET(DB_TXN_DIST_UPD_SHADOWS);

						TXN_DETAIL *tp = (TXN_DETAIL *)R_ADDR(&txnp->mgrp->reginfo, txnp->off);
						u_int64_t genid = get_commit_context(NULL, 0);
                        u_int32_t rectype = endianize_locklist ?
                            DB___txn_dist_prepare_endianize :
                            DB___txn_dist_prepare;

						ret = __txn_dist_prepare_log(dbenv, rectype, txnp, &txnp->last_lsn, lflags,
							gen, &tp->begin_lsn, &dist_txnid, genid, ltranflags,
							txnp->coordinator_gen, &coordinator, &tier, &txnp->blkseq_key, request.obj);
						F_SET(txnp, TXN_DIST_PREPARED);
						if ((ret = __txn_master_prepared(dbenv, txnp->dist_txnid, &txnp->last_lsn,
							&tp->begin_lsn, &txnp->blkseq_key, txnp->coordinator_gen, &coordinator, &tier))!=0) {
							abort();
						}

						/* Treat prepares like commits in elections */
						if (elect_highest_committed_gen) {
							MUTEX_LOCK(dbenv,
									db_rep->rep_mutexp);
							rep->committed_gen = gen;
							rep->committed_lsn = txnp->last_lsn;
							MUTEX_UNLOCK(dbenv,
									db_rep->rep_mutexp);
						}

					} else if (elect_highest_committed_gen) {

						MUTEX_LOCK(dbenv,
							db_rep->rep_mutexp);
						gen = rep->gen;
						MUTEX_UNLOCK(dbenv,
							db_rep->rep_mutexp);

						if (commit_prepared) {
							DBT dist_txnid = {0};
							dist_txnid.data = txnp->dist_txnid;
							dist_txnid.size = strlen(txnp->dist_txnid);

							ret = __txn_dist_commit_log(dbenv, txnp, 
									&txnp->last_lsn, &context, lflags,
									&dist_txnid, gen, timestamp, usr_ptr);
							/* XXX better name for this ? */
							if ((ret = __txn_discard_recovered(dbenv, txnp->dist_txnid)) != 0) {
								abort();
							}
						} else {
							if (txnp->wrote_regop_gen) {
								logmsg(LOGMSG_FATAL, "%s writing multiple commits for same txn?\n", __func__);
								abort();
							}
							u_int32_t rectype = endianize_locklist ? 
								DB___txn_regop_gen_endianize :
								DB___txn_regop_gen;
							ret =
								__txn_regop_gen_log(dbenv, rectype,
										txnp, &txnp->last_lsn,
										&context, lflags,
										TXN_COMMIT, gen, timestamp,
										request.obj, usr_ptr);
							txnp->wrote_regop_gen = 1;
						}
#if defined DEBUG_STACK_AT_TXN_LOG
						comdb2_cheapstack_sym(stderr, "TXN-COMMIT-GEN TXNID %x LSN [%d:%d]",
								txnp->txnid,txnp->last_lsn.file,txnp->last_lsn.offset);
#endif
						MUTEX_LOCK(dbenv,
							db_rep->rep_mutexp);
						rep->committed_gen = gen;
						rep->committed_lsn = txnp->last_lsn;
						MUTEX_UNLOCK(dbenv,
							db_rep->rep_mutexp);
					} else {
						if (commit_prepared) {
							DBT dist_txnid = {0};
							dist_txnid.data = txnp->dist_txnid;
							dist_txnid.size = strlen(txnp->dist_txnid);
							MUTEX_LOCK(dbenv,
									db_rep->rep_mutexp);
							gen = rep->gen;
							MUTEX_UNLOCK(dbenv,
									db_rep->rep_mutexp);

							ret = __txn_dist_commit_log(dbenv, txnp, 
									&txnp->last_lsn, &context, lflags,
									&dist_txnid, gen, timestamp, usr_ptr);
							/* XXX better name for this ? */
							if ((ret = __txn_discard_recovered(dbenv, txnp->dist_txnid)) != 0) {
								abort();
							}


						} else {
							ret =
								__txn_regop_log_commit
								(dbenv, txnp,
								 &txnp->last_lsn, &context,
								 lflags, TXN_COMMIT,
								 timestamp, request.obj,
								 usr_ptr);
						}
#if defined DEBUG_STACK_AT_TXN_LOG
						comdb2_cheapstack_sym(stderr, "TXN-COMMIT TXNID %x LSN [%d:%d]",
							txnp->txnid,txnp->last_lsn.file,txnp->last_lsn.offset);
#endif
					}

					if (lsn_out) {
						lsn_out->file = 
							txnp->last_lsn.file;
						lsn_out->offset =
							txnp->last_lsn.offset;
					}
				}
				if (!is_prepare)
					Pthread_rwlock_unlock(&gbl_dbreg_log_lock);

				if (gbl_new_snapisol && !is_prepare) {
					if (!txnp->pglogs_hashtbl) {
						DB_ASSERT(
							F_ISSET(
							txnp. TXN_COMPENSATE));
					} else {
						bdb_transfer_txn_pglogs(
							dbenv->app_private,
							txnp->pglogs_hashtbl,
							&txnp->pglogs_mutex,
							txnp->last_lsn,
							(flags & 
							 (DB_TXN_LOGICAL_COMMIT |
							  DB_TXN_DONT_GET_REPO_MTX
								 )),
							ltranid, timestamp,
							context);
					}
				}
			}

			if (list_dbt_rl.data != NULL)
				__os_free(dbenv, list_dbt_rl.data);

			if (request.obj != NULL && request.obj->data != NULL)
				__os_free(dbenv, request.obj->data);
			if (ret != 0) {
				goto err;
			}
		} else {
			/* Log the commit in the parent! */
			timestamp = comdb2_time_epoch();
			if (!IS_ZERO_LSN(txnp->last_lsn)) {
				if ((ret = __txn_child_log(dbenv,
								txnp->parent, &txnp->parent->last_lsn,
								0, txnp->txnid, txnp->utxnid, &txnp->last_lsn)) != 0) {
					goto err;
				}
#if defined DEBUG_STACK_AT_TXN_LOG
				comdb2_cheapstack_sym(stderr, "CHILD-COMMIT TXNID %x LSN [%d:%d]",
						txnp->txnid,txnp->last_lsn.file,txnp->last_lsn.offset);
#endif
			}

			ptd = (TXN_DETAIL *)R_ADDR(&txnp->mgrp->reginfo, txnp->parent->off);

			R_LOCK(dbenv, &txnp->mgrp->reginfo);
			if (log_compare(&td->begin_lsn, &ptd->begin_lsn) < 0) {
				logmsg(LOGMSG_INFO, "Reset parent begin-lsn from [%d:%d] to "
						"[%d:%d]\n", ptd->begin_lsn.file,
						ptd->begin_lsn.offset, td->begin_lsn.file,
						td->begin_lsn.offset);
				ptd->begin_lsn = td->begin_lsn;
			}
			R_UNLOCK(dbenv, &txnp->mgrp->reginfo);

			if (__lock_set_parent_has_pglk_lsn(dbenv,
				txnp->parent->txnid, txnp->txnid) != 0)
				abort();

			if (gbl_new_snapisol) {	 
				if (!txnp->pglogs_hashtbl) {		
					DB_ASSERT(F_ISSET(txnp. 
							  TXN_COMPENSATE));
				} else {
					uint32_t fl = 
						(flags & DB_TXN_DONT_GET_REPO_MTX) |
						DB_TXN_LOGICAL_COMMIT;
					bdb_transfer_txn_pglogs(
						dbenv->app_private,
						txnp->pglogs_hashtbl,	   
						&txnp->pglogs_mutex,
						txnp->parent->last_lsn,
						fl, 0, timestamp, 0);
				}	  
			}


			if (lsn_out) {
				/* its a child txn - */
				/* XXX why are we waiting on this?  bug? */
				lsn_out->file = 0;
				lsn_out->offset = 0;
			}

			if (STAILQ_FIRST(&txnp->logs) != NULL) {
				/*
				 * Put the child first so we back it out first.
				 * All records are undone in reverse order.
				 */
				STAILQ_CONCAT(&txnp->logs, &txnp->parent->logs);
				txnp->parent->logs = txnp->logs;
				STAILQ_INIT(&txnp->logs);
			}

			F_SET(txnp->parent, TXN_CHILDCOMMIT);
			if (commit_lsn_map) {
				if ((ret = __os_malloc(dbenv, sizeof(UTXNID), &utxnid_track)) != 0) {
					goto err;
				}

				utxnid_track->utxnid = txnp->utxnid;
				listc_atl(&txnp->parent->committed_kids, utxnid_track);
			}
		}
	}

	/*
	 * Process any aborted pages from our children.
	 * We delay putting pages on the free list that are newly
	 * allocated and then aborted so that we can undo other
	 * allocations, if necessary, without worrying about
	 * these pages which were not on the free list before.
	 */
	if (txnp->txn_list != NULL) {
		t_ret = __db_do_the_limbo(dbenv,
			  NULL, txnp, txnp->txn_list, LIMBO_NORMAL);
		__db_txnlist_end(dbenv, txnp->txn_list);
		txnp->txn_list = NULL;
		if (t_ret != 0 && ret == 0)
			ret = t_ret;
	}

	if (ret != 0) {
		goto err;
	}

	if (F_ISSET(txnp, TXN_RECOVER_LOCK)) {
		dbenv->unlock_recovery_lock(dbenv, __func__, __LINE__);
		F_CLR(txnp, TXN_RECOVER_LOCK);
	}
	
	if (is_prepare) {
		return 0;
	}

	Pthread_mutex_lock(&dbenv->txmap->txmap_mutexp);

	if (commit_lsn_map && !txnp->parent) {
		ret = __txn_commit_map_add_nolock(dbenv, txnp->utxnid, txnp->last_lsn);
		if (ret != 0) {
			Pthread_mutex_unlock(&dbenv->txmap->txmap_mutexp);
			goto err;
		}

		/* No grandchildren in comdb2, so this is sufficient. */
		LISTC_FOR_EACH(&txnp->committed_kids, utxnid_track, lnk) {
			ret = __txn_commit_map_add_nolock(dbenv, utxnid_track->utxnid, txnp->last_lsn);
			if (ret != 0) {
				Pthread_mutex_unlock(&dbenv->txmap->txmap_mutexp);
				goto err;
			}
		}
	}

	Pthread_mutex_unlock(&dbenv->txmap->txmap_mutexp);

	remove_td_txn(txnp);
	if (logbytes) {
		(*logbytes) = txnp->logbytes;
	}

	/* This is OK because __txn_end can only fail with a panic. */
	return (__txn_end(txnp, 1));

err:	/*
	 * If we are prepared, then we "must" be able to commit.  We
	 * panic here because even though the coordinator might be
	 * able to retry it is not clear it would know to do that.
	 * Otherwise  we'll try to abort.  If that is successful,
	 * then we return whatever was in ret (i.e., the reason we failed).
	 * If the abort was unsuccessful, then abort probably returned
	 * DB_RUNRECOVERY and we need to propagate that up.
	 */
	if (td->status == TXN_PREPARED)
		return (__db_panic(dbenv, ret));

	if (logbytes) {
		(*logbytes) = txnp->logbytes;
	}

	if ((t_ret = __txn_abort(txnp)) != 0) {
		ret = t_ret;
	}
	return (ret);
}

int
__txn_commit(txnp, flags)
	DB_TXN *txnp;
	u_int32_t flags;
{
	return __txn_commit_int(txnp, flags, 0, 0, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL);
}


/*
 * __txn_commit_pp --
 *	Interface routine to TXN->commit.
 */
static int
__txn_commit_pp(txnp, flags)
	DB_TXN *txnp;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	int not_child, ret;

	dbenv = txnp->mgrp->dbenv;
	not_child = txnp->parent == NULL;
	ret =
		__txn_commit_int(txnp, flags, 0, 0, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL);
	if (not_child && IS_ENV_REPLICATED(dbenv))
		__op_rep_exit(dbenv);
	return (ret);
}

/*
 * __txn_commit_txnlen --
 *  Commit a transaction, and return the number of logbytes written.
 */
static int
__txn_commit_txnlen_pp(txnp, logbytes, flags)
	DB_TXN *txnp;
	u_int64_t *logbytes;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	int not_child, ret;

	dbenv = txnp->mgrp->dbenv;
	not_child = txnp->parent == NULL;
	ret =
		__txn_commit_int(txnp, flags, 0, 0, NULL, NULL, NULL, 0, logbytes, NULL, NULL, NULL);
	if (not_child && IS_ENV_REPLICATED(dbenv))
		__op_rep_exit(dbenv);
	return (ret);
}

/*
 * __txn_commit_getlsn__pp --
 *	Interface routine to TXN->commit.
 */
static int
__txn_commit_getlsn_pp(txnp, flags, logbytes, lsn_out, usr_ptr)
	DB_TXN *txnp;
	u_int32_t flags;
	u_int64_t *logbytes;
	DB_LSN *lsn_out;
	void *usr_ptr;
{
	DB_ENV *dbenv;
	int not_child, ret;
	u_int64_t ltranid = 0;

	dbenv = txnp->mgrp->dbenv;
	not_child = txnp->parent == NULL;
	ret =
		__txn_commit_int(txnp, flags, ltranid, 0, NULL, NULL, NULL, 0, logbytes, NULL,
		lsn_out, usr_ptr);
	if (not_child && IS_ENV_REPLICATED(dbenv))
		__op_rep_exit(dbenv);

	return (ret);
}

static int
__txn_logbytes_pp(txnp, logbytes)
	DB_TXN *txnp;
	u_int64_t *logbytes;
{
	(*logbytes) = txnp->logbytes;
	return 0;
}

static int
__txn_commit_rl_pp(txnp, flags, ltranid, llid, last_commit_lsn, rlocks,
	lks, nrlocks, logbytes, begin_lsn, lsn_out, usr_ptr)
	DB_TXN *txnp;
	u_int32_t flags;
	u_int64_t ltranid;
	u_int32_t llid;
	DB_LSN *last_commit_lsn;
	DBT *rlocks;
	DB_LOCK *lks;
	u_int32_t nrlocks;
	u_int64_t *logbytes;
	DB_LSN *begin_lsn;
	DB_LSN *lsn_out;
	void *usr_ptr;
{
	DB_ENV *dbenv;
	int not_child, ret;

	dbenv = txnp->mgrp->dbenv;
	not_child = txnp->parent == NULL;

	ret = __txn_commit_int(txnp, flags, ltranid, llid, last_commit_lsn,
		rlocks, lks, nrlocks, logbytes, begin_lsn, lsn_out, usr_ptr);
	if (not_child && IS_ENV_REPLICATED(dbenv))
		__op_rep_exit(dbenv);

	return (ret);
}




/*
 * __txn_abort_pp --
 *	Interface routine to TXN->abort.
 */
static int
__txn_abort_pp(txnp)
	DB_TXN *txnp;
{
	DB_ENV *dbenv;
	int not_child, ret;

	dbenv = txnp->mgrp->dbenv;
	not_child = txnp->parent == NULL;
	ret = __txn_abort(txnp);
	if (not_child && IS_ENV_REPLICATED(dbenv))
		__op_rep_exit(dbenv);
	return (ret);
}

/*
 * __txn_free_recovered --
 *	Abort a transaction.
 *
 * PUBLIC: int __txn_free_recovered __P((DB_TXN *));
 */
int
__txn_free_recovered(txnp)
	DB_TXN *txnp;
{
	TXN_DETAIL *td;
	int ret;
	DB_ENV *dbenv = txnp->mgrp->dbenv;
	if ((ret = __txn_isvalid(txnp, &td, TXN_OP_ABORT)) != 0) {
		abort();
	}

	if (F_ISSET(txnp, TXN_RECOVER_LOCK)) {
		dbenv->unlock_recovery_lock(dbenv, __func__, __LINE__);
		F_CLR(txnp, TXN_RECOVER_LOCK);
	}

	F_SET(txnp, TXN_DIST_DISCARD);

	return (__txn_end(txnp, 0));
}

static int
__txn_set_discard(txnp)
	DB_TXN *txnp;
{
	if (!F_ISSET(txnp, TXN_DIST_PREPARED)) {
		logmsg(LOGMSG_FATAL, "%s set discard flag for unprepared transaction\n", __func__);
		abort();
	}
	F_SET(txnp, TXN_DIST_DISCARD);
	__txn_set_prepared_discard(txnp->mgrp->dbenv, txnp->dist_txnid);
	return 0;
}


/*
 * __txn_abort --
 *	Abort a transaction.
 *
 * PUBLIC: int __txn_abort __P((DB_TXN *));
 */
int
__txn_abort(txnp)
	DB_TXN *txnp;
{
	DB_ENV *dbenv;
	DB_LOCKREQ request;
	DB_TXN *kid;
	TXN_DETAIL *td = NULL;
	u_int32_t lflags;
	int flags = 0;
	int ret;

	dbenv = txnp->mgrp->dbenv;

	PANIC_CHECK(dbenv);

	if (dbenv->attr.check_applied_lsns) {
		int c_ret;

		c_ret = __txn_check_applied_lsns(dbenv, txnp);
		if (c_ret && dbenv->attr.check_applied_lsns_fatal)
			return __db_panic(dbenv, c_ret);
	}

	/* Ensure that abort always fails fatally. */
	if ((ret = __txn_isvalid(txnp, &td, TXN_OP_ABORT)) != 0)
		return (__db_panic(dbenv, ret));

	/*
	 * Try to abort any unresolved children.
	 *
	 * Abort either succeeds or panics the region.  As soon as we
	 * see any failure, we just get out of here and return the panic
	 * up.
	 */
	while ((kid = TAILQ_FIRST(&txnp->kids)) != NULL) {
		if (F_ISSET(txnp, TXN_DIST_DISCARD))
			F_SET(kid, TXN_DIST_DISCARD);
		if ((ret = __txn_abort(kid)) != 0)
			return (ret);
	}

	if (LOCKING_ON(dbenv)) {
		/*
		 * We are about to free all the read locks for this transaction
		 * below.  Some of those locks might be handle locks which
		 * should not be freed, because they will be freed when the
		 * handle is closed.  Check the events and preprocess any
		 * trades now so that we don't release the locks below.
		 */
		if ((ret = __txn_doevents(dbenv, txnp, TXN_ABORT, 1)) != 0)
			return (__db_panic(dbenv, ret));

		/* Turn off timeouts. */
		if ((ret = __lock_set_timeout(dbenv,
			txnp->txnid, 0, DB_SET_TXN_TIMEOUT)) != 0)
			return (__db_panic(dbenv, ret));

		if ((ret = __lock_set_timeout(dbenv,
			txnp->txnid, 0, DB_SET_LOCK_TIMEOUT)) != 0)
			return (__db_panic(dbenv, ret));

		request.op = DB_LOCK_UPGRADE_WRITE;
		request.obj = NULL;
		if ((ret = __lock_vec(
			dbenv, txnp->txnid, 0, &request, 1, NULL)) != 0)
			return (__db_panic(dbenv, ret));
	}
	/*
	 * __txn_undo will reclaim allocated pages.  If this is a
	 * dist_prepared txn, write the abort record now, as the 
	 * compensating txn's locks will overlap the prepare's
	 */
	if (DBENV_LOGGING(dbenv) && F_ISSET(txnp, TXN_DIST_PREPARED)) {
		if (!F_ISSET(txnp, TXN_DIST_DISCARD)) {

			DBT dist_txnid = {0};
			dist_txnid.data = txnp->dist_txnid;
			dist_txnid.size = strlen(txnp->dist_txnid);

			DB_REP *db_rep = dbenv->rep_handle;
			REP *rep = db_rep->region;

			MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
			u_int32_t gen = rep->gen;
			MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
			u_int64_t timestamp = comdb2_time_epoch();

			if ((ret = __txn_dist_abort_log(dbenv, txnp, &txnp->last_lsn, 0,
							gen, timestamp, &dist_txnid) != 0))
				return (__db_panic(dbenv, ret));
		}
	}

	if ((ret = __txn_undo(txnp)) != 0)
		return (__db_panic(dbenv, ret));

	/*
	 * Normally, we do not need to log aborts.  However, if we
	 * are a distributed transaction (i.e., we have a prepare),
	 * then we log the abort so we know that this transaction
	 * was actually completed.
	 */
	SET_LOG_FLAGS(dbenv, txnp, lflags);

	if (DBENV_LOGGING(dbenv) && td->status == TXN_PREPARED &&
		(ret = __txn_regop_log(dbenv, txnp, &txnp->last_lsn, NULL,
			lflags, TXN_ABORT, (int32_t)comdb2_time_epoch(), NULL)) != 0)
		 return (__db_panic(dbenv, ret));

	if (F_ISSET(txnp, TXN_RECOVER_LOCK)) {
		dbenv->unlock_recovery_lock(dbenv, __func__, __LINE__);
		F_CLR(txnp, TXN_RECOVER_LOCK);
	}

	remove_td_txn(txnp);

	/* __txn_end always panics if it errors, so pass the return along. */
	return (__txn_end(txnp, 0));
}

/*
 * __txn_discard_pp --
 *	Interface routine to TXN->discard.
 */
static int
__txn_discard_pp(txnp, flags)
	DB_TXN *txnp;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	int not_child, ret;

	dbenv = txnp->mgrp->dbenv;
	not_child = txnp->parent == NULL;
	ret = __txn_discard(txnp, flags);
	if (not_child && IS_ENV_REPLICATED(dbenv))
		__op_rep_exit(dbenv);
	return (ret);
}

/*
 * __txn_discard --
 *	Free the per-process resources associated with this txn handle.
 *
 * PUBLIC: int __txn_discard __P((DB_TXN *, u_int32_t flags));
 */
int
__txn_discard(txnp, flags)
	DB_TXN *txnp;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	DB_TXN *freep;
	TXN_DETAIL *td;
	int ret;

	COMPQUIET(flags, 0);

	dbenv = txnp->mgrp->dbenv;
	freep = NULL;

	PANIC_CHECK(dbenv);

	if ((ret = __txn_isvalid(txnp, &td, TXN_OP_DISCARD)) != 0)
		return (ret);

	/* Should be no children. */
	DB_ASSERT(TAILQ_FIRST(&txnp->kids) == NULL);

	/* Free the space. */
	MUTEX_THREAD_LOCK(dbenv, txnp->mgrp->mutexp);
	txnp->mgrp->n_discards++;
	if (F_ISSET(txnp, TXN_MALLOC)) {
		TAILQ_REMOVE(&txnp->mgrp->txn_chain, txnp, links);
		freep = txnp;
	}
	MUTEX_THREAD_UNLOCK(dbenv, txnp->mgrp->mutexp);
	if (freep != NULL) {
		if (freep->pglogs_hashtbl)
			bdb_txn_pglogs_close(dbenv->app_private,
				&freep->pglogs_hashtbl,
				&freep->pglogs_mutex);
		__os_free(dbenv, freep);
	}

	if (dbenv->tx_perfect_ckp)
		Pthread_setspecific(txn_key, NULL);

	return (0);
}

static int
__txn_dist_prepare_pp(txnp, dist_txnid, coordinator_name, coordinator_tier, coordinator_gen, blkseq_key, commit_lsn, lflags)
	DB_TXN *txnp;
	const char *dist_txnid;
	const char *coordinator_name;
	const char *coordinator_tier;
	u_int32_t coordinator_gen;
	DBT *blkseq_key;
	DB_LSN *commit_lsn;
	u_int32_t lflags;
{
	DB_ENV *dbenv = txnp->mgrp->dbenv;
	int ret;

	if (txnp->parent) {
		__db_err(dbenv,
	"dist_prepare is not supported in child transactions");
		return EINVAL;
	}
	if (txnp->dist_txnid != 0) {
		__db_err(dbenv,
	"transaction is already prepared");
		return EINVAL;
	}
	if (F_ISSET(txnp, TXN_NOPREP)) {
		__db_err(dbenv,
	"transaction with non-utxnid logs cannot be prepared");
		return EINVAL;
	}
	if ((ret = __os_calloc(dbenv, 1, strlen(coordinator_name) + 1, &txnp->coordinator_name)) != 0) {
		__db_err(dbenv, "failed malloc");
		return ret;
	}
	memcpy(txnp->coordinator_name, coordinator_name, strlen(coordinator_name));
	if ((ret = __os_calloc(dbenv, 1, strlen(coordinator_tier) + 1, &txnp->coordinator_tier)) != 0) {
		__db_err(dbenv, "failed malloc");
		return ret;
	}
	memcpy(txnp->coordinator_tier, coordinator_tier, strlen(coordinator_tier));
	if ((ret = __os_calloc(dbenv, 1, strlen(dist_txnid) + 1, &txnp->dist_txnid)) != 0) {
		__db_err(dbenv, "failed malloc");
		return ret;
	}
	memcpy(txnp->dist_txnid, dist_txnid, strlen(dist_txnid));

	txnp->coordinator_gen = coordinator_gen;

	if ((ret = __os_calloc(dbenv, 1, blkseq_key->size, &txnp->blkseq_key.data)) != 0) {
		__db_err(dbenv, "failed malloc");
		return ret;
	}
	txnp->blkseq_key.size = blkseq_key->size;
	memcpy(txnp->blkseq_key.data, blkseq_key->data, blkseq_key->size);

	lflags |= DB_TXN_DIST_PREPARE;

	ret =
		__txn_commit_int(txnp, lflags, 0, 0, NULL, NULL, NULL, 0, NULL, NULL, commit_lsn, NULL);
	return (ret);
}

/*
 * __txn_prepare --
 *	Flush the log so a future commit is guaranteed to succeed.
 *
 * PUBLIC: int __txn_prepare __P((DB_TXN *, u_int8_t *));
 */
int
__txn_prepare(txnp, gid)
	DB_TXN *txnp;
	u_int8_t *gid;
{
	DBT list_dbt, xid;
	DB_ENV *dbenv;
	DB_LOCKREQ request;
	DB_TXN *kid;
	TXN_DETAIL *td = NULL;
	u_int32_t lflags;
	int ret;

	dbenv = txnp->mgrp->dbenv;

	PANIC_CHECK(dbenv);

	if ((ret = __txn_isvalid(txnp, &td, TXN_OP_PREPARE)) != 0)
		return (ret);

	/* Commit any unresolved children. */
	while ((kid = TAILQ_FIRST(&txnp->kids)) != NULL)
		if ((ret = __txn_commit(kid, DB_TXN_NOSYNC)) != 0)
			return (ret);

	if (txnp->txn_list != NULL  &&
		(ret = __db_do_the_limbo(dbenv,
		NULL, txnp, txnp->txn_list, LIMBO_PREPARE)) != 0)
		return (ret);
	/*
	 * In XA, the global transaction ID in the txn_detail structure is
	 * already set; in a non-XA environment, we must set it here.  XA
	 * requires that the transaction be either ENDED or SUSPENDED when
	 * prepare is called, so we know that if the xa_status isn't in one
	 * of those states, then we are calling prepare directly and we need
	 * to fill in the td->xid.
	 */
	if ((ret = __txn_doevents(dbenv, txnp, TXN_PREPARE, 1)) != 0)
		return (ret);
	memset(&request, 0, sizeof(request));
	if (LOCKING_ON(dbenv)) {
		request.op = DB_LOCK_PUT_READ;
		if (IS_REP_MASTER(dbenv) &&
			IS_ZERO_LSN(txnp->last_lsn)) {
			memset(&list_dbt, 0, sizeof(list_dbt));
			request.obj = &list_dbt;
		}
		if ((ret = __lock_vec(dbenv,
			txnp->txnid, 0, &request, 1, NULL)) != 0)
			return (ret);

	}
	if (DBENV_LOGGING(dbenv)) {
		memset(&xid, 0, sizeof(xid));
		if (td->xa_status != TXN_XA_ENDED &&
			td->xa_status != TXN_XA_SUSPENDED)
			/* Regular prepare; fill in the gid. */
			memcpy(td->xid, gid, sizeof(td->xid));

		xid.size = sizeof(td->xid);
		xid.data = td->xid;

		lflags = DB_LOG_COMMIT | DB_LOG_PERM | DB_FLUSH;
		if ((ret = __txn_xa_regop_log(dbenv, txnp, &txnp->last_lsn,
			lflags, TXN_PREPARE, &xid, td->format, td->gtrid, td->bqual,
			&td->begin_lsn, request.obj)) != 0) {
			__db_err(dbenv, "DB_TXN->prepare: log_write failed %s",
				db_strerror(ret));
		}
		if (request.obj != NULL && request.obj->data != NULL)
			__os_free(dbenv, request.obj->data);
		if (ret != 0)
			return (ret);
		
	}

	MUTEX_THREAD_LOCK(dbenv, txnp->mgrp->mutexp);
	td->status = TXN_PREPARED;
	MUTEX_THREAD_UNLOCK(dbenv, txnp->mgrp->mutexp);
	return (0);
}

/*
 * __txn_id --
 *	Return the transaction ID.
 *
 * PUBLIC: u_int32_t __txn_id __P((DB_TXN *));
 */
u_int32_t
__txn_id(txnp)
	DB_TXN *txnp;
{
	return (txnp->txnid);
}

/*
 * __txn_set_timeout --
 *	DB_ENV->set_txn_timeout.
 *
 * PUBLIC: int  __txn_set_timeout __P((DB_TXN *, db_timeout_t, u_int32_t));
 */
int
__txn_set_timeout(txnp, timeout, op)
	DB_TXN *txnp;
	db_timeout_t timeout;
	u_int32_t op;
{
	if (op != DB_SET_TXN_TIMEOUT &&  op != DB_SET_LOCK_TIMEOUT)
		return (__db_ferr(txnp->mgrp->dbenv, "DB_TXN->set_timeout", 0));

	return (__lock_set_timeout(
		txnp->mgrp->dbenv, txnp->txnid, timeout, op));
}

/*
 * __txn_isvalid --
 *	Return 0 if the txnp is reasonable, otherwise panic.
 */
static int
__txn_isvalid(txnp, tdp, op)
	const DB_TXN *txnp;
	TXN_DETAIL **tdp;
	txnop_t op;
{
	DB_TXNMGR *mgrp;
	DB_TXNREGION *region;
	TXN_DETAIL *tp;

	mgrp = txnp->mgrp;
	region = mgrp->reginfo.primary;

	/* Check for recovery. */
	if (!F_ISSET(txnp, TXN_COMPENSATE) &&
		F_ISSET(region, TXN_IN_RECOVERY)) {
		__db_err(mgrp->dbenv,
			"operation not permitted during recovery");
		goto err;
	}

	/* Check for live cursors. */
	if (txnp->cursors != 0) {
		__db_err(mgrp->dbenv, "transaction has active cursors");
		goto err;
	}

	/* Check transaction's state. */
	tp = (TXN_DETAIL *)R_ADDR(&mgrp->reginfo, txnp->off);
	if (tdp != NULL)
		*tdp = tp;

	/* Handle any operation specific checks. */
	switch (op) {
	case TXN_OP_DISCARD:
		/*
		 * Since we're just tossing the per-process space; there are
		 * a lot of problems with the transaction that we can tolerate.
		 */

		/* Transaction is already been reused. */
		if (txnp->txnid != tp->txnid)
			return (0);

		/*
		 * What we've got had better be either a prepared or
		 * restored transaction.
		 */
		if (tp->status != TXN_PREPARED &&
			!F_ISSET(tp, TXN_DTL_RESTORED) && !F_ISSET(txnp, TXN_DIST_PREPARED)) {
			__db_err(mgrp->dbenv, "not a restored transaction");
			return (__db_panic(mgrp->dbenv, EINVAL));
		}

		return (0);
	case TXN_OP_PREPARE:
		if (txnp->parent != NULL) {
			/*
			 * This is not fatal, because you could imagine an
			 * application that simply prepares everybody because
			 * it doesn't distinguish between children and parents.
			 * I'm not arguing this is good, but I could imagine
			 * someone doing it.
			 */
			__db_err(mgrp->dbenv,
				"Prepare disallowed on child transactions");
			return (EINVAL);
		}
		break;
	case TXN_OP_ABORT:
	case TXN_OP_COMMIT:
	default:
		break;
	}

	switch (tp->status) {
	case TXN_PREPARED:
		if (op == TXN_OP_PREPARE) {
			__db_err(mgrp->dbenv, "transaction already prepared");
			/*
			 * Txn_prepare doesn't blow away the user handle, so
			 * in this case, give the user the opportunity to
			 * abort or commit.
			 */
			return (EINVAL);
		}
		break;
	case TXN_RUNNING:
		break;
	case TXN_ABORTED:
	case TXN_COMMITTED:
	default:
		__db_err(mgrp->dbenv, "transaction already %s",
			tp->status == TXN_COMMITTED ? "committed" : "aborted");
		goto err;
	}

	return (0);

err:	/*
	 * If there's a serious problem with the transaction, panic.  TXN
	 * handles are dead by definition when we return, and if you use
	 * a cursor you forgot to close, we have no idea what will happen.
	 */
	return (__db_panic(mgrp->dbenv, EINVAL));
}

/*
 * __txn_end --
 *	Internal transaction end routine.
 */
static int
__txn_end(txnp, is_commit)
	DB_TXN *txnp;
	int is_commit;
{
	DB_ENV *dbenv;
	DB_LOCKREQ request;
	DB_TXNLOGREC *lr;
	DB_TXNMGR *mgr;
	DB_TXNREGION *region;
	TXN_DETAIL *tp;
	UTXNID *utxnid_track, *tmpp;
	int do_closefiles, ret;

	mgr = txnp->mgrp;
	dbenv = mgr->dbenv;
	region = mgr->reginfo.primary;
	do_closefiles = 0;

	/* Process commit events. */
	if ((ret = __txn_doevents(dbenv,
		txnp, is_commit ? TXN_COMMIT : TXN_ABORT, 0)) != 0)
		return (__db_panic(dbenv, ret));

	/*
	 * Release the locks.
	 *
	 * __txn_end cannot return an simple error, we MUST return
	 * success/failure from commit or abort, ignoring any internal
	 * errors.  So, we panic if something goes wrong.  We can't
	 * deadlock here because we're not acquiring any new locks,
	 * so DB_LOCK_DEADLOCK is just as fatal as any other error.
	 */
	if (LOCKING_ON(dbenv)) {
		request.op = txnp->parent == NULL ||
			is_commit == 0 ? DB_LOCK_PUT_ALL : DB_LOCK_INHERIT;
		request.obj = NULL;
		if ((ret = __lock_vec(dbenv,
			txnp->txnid, 0, &request, 1, NULL)) != 0)
			return (__db_panic(dbenv, ret));
	}

	/* End the transaction. */
	R_LOCK(dbenv, &mgr->reginfo);

	tp = (TXN_DETAIL *)R_ADDR(&mgr->reginfo, txnp->off);
	SH_TAILQ_REMOVE(&region->active_txn, tp, links, __txn_detail);
	if (F_ISSET(tp, TXN_DTL_RESTORED)) {
		region->stat.st_nrestores--;
		do_closefiles = region->stat.st_nrestores == 0;
	}
#ifdef TESTSUITE
	free(tp);
#else
	__db_shalloc_free(mgr->reginfo.addr, tp);
#endif

	if (is_commit)
		region->stat.st_ncommits++;
	else
		region->stat.st_naborts++;
	--region->stat.st_nactive;

	R_UNLOCK(dbenv, &mgr->reginfo);

	/*
	 * The transaction cannot get more locks, remove its locker info,
	 * if any.
	 */
	if (LOCKING_ON(dbenv) && (ret =
		__lock_freefamilylocker(dbenv->lk_handle, txnp->txnid)) != 0)
		return (__db_panic(dbenv, ret));
	if (txnp->parent != NULL)
		TAILQ_REMOVE(&txnp->parent->kids, txnp, klinks);
	
	LISTC_FOR_EACH_SAFE(&txnp->committed_kids, utxnid_track, tmpp, lnk) {
		__os_free(dbenv, utxnid_track);
	}

	/* Free the space. */
	while ((lr = STAILQ_FIRST(&txnp->logs)) != NULL) {
		STAILQ_REMOVE(&txnp->logs, lr, __txn_logrec, links);
		__os_free(dbenv, lr);
	}

	if (txnp->coordinator_name) {
		__os_free(dbenv, txnp->coordinator_name);
		__os_free(dbenv, txnp->coordinator_tier);
		__os_free(dbenv, txnp->dist_txnid);
		__os_free(dbenv, txnp->blkseq_key.data);
	}

	if (F_ISSET(txnp, TXN_MALLOC)) {
		MUTEX_THREAD_LOCK(dbenv, mgr->mutexp);
		TAILQ_REMOVE(&mgr->txn_chain, txnp, links);
		MUTEX_THREAD_UNLOCK(dbenv, mgr->mutexp);

		if (txnp->pglogs_hashtbl)
			bdb_txn_pglogs_close(dbenv->app_private,
				&txnp->pglogs_hashtbl,
				&txnp->pglogs_mutex);

		__os_free(dbenv, txnp);
	}

	if (do_closefiles) {
		F_SET((DB_LOG *)dbenv->lg_handle, DBLOG_RECOVER);
		(void)__dbreg_close_files(dbenv);
		F_CLR((DB_LOG *)dbenv->lg_handle, DBLOG_RECOVER);

		mgr->n_discards = 0;
		(void)__txn_checkpoint(dbenv, 0, 0, DB_FORCE);
	}

	if (dbenv->tx_perfect_ckp)
		Pthread_setspecific(txn_key, NULL);

	return (0);
}

/*
 * __txn_getpriority --
 * Returns the priority used for this transaction assigned during
 * deadlock detection.
 *	
 * PUBLIC: int __txn_getpriority __P((DB_TXN *, int*));
 */
int
__txn_getpriority(txnp, priority)
	DB_TXN *txnp;
	int *priority;
{
	DB_ENV *dbenv;
	int ret;

	if (!priority)
		return 0;

	*priority = 0;

	dbenv = txnp->mgrp->dbenv;

	if (LOCKING_ON(dbenv) && (ret =
		__lock_locker_getpriority(dbenv->lk_handle, txnp->txnid,
			priority)) != 0)
		return (__db_panic(dbenv, ret));

#ifdef TEST_DEADLOCKS
	printf("%d %s:%d lockerid %x has priority %d\n",
		pthread_self(), __FILE__, __LINE__, txnp->txnid, *priority);
#endif

	return (0);
}

static int
__txn_dispatch_undo(dbenv, txnp, rdbt, key_lsn, txnlist)
	DB_ENV *dbenv;
	DB_TXN *txnp;
	DBT *rdbt;
	DB_LSN *key_lsn;
	void *txnlist;
{
	int ret;

#if defined (UFID_HASH_DEBUG)
	comdb2_cheapstack_sym(stderr, "%s undoing [%d:%d]", __func__,
			key_lsn->file, key_lsn->offset);
#endif
	ret = __db_dispatch(dbenv, dbenv->recover_dtab,
		dbenv->recover_dtab_size, rdbt, key_lsn, DB_TXN_ABORT, txnlist);
	if (F_ISSET(txnp, TXN_CHILDCOMMIT))
		(void)__db_txnlist_lsnadd(dbenv,
			txnlist, key_lsn, 0);
	if (ret == DB_SURPRISE_KID) {
		if ((ret = __db_txnlist_lsninit(
			dbenv, txnlist, key_lsn)) == 0)
			F_SET(txnp, TXN_CHILDCOMMIT);
	}

	return (ret);
}

/*
 * __txn_undo --
 *	Undo the transaction with id txnid.
 */
int
__txn_undo(txnp)
	DB_TXN *txnp;
{
	DBT rdbt;
	DB_ENV *dbenv;
	DB_LOGC *logc;
	DB_LSN key_lsn;
	DB_TXN *ptxn;
	DB_TXNLOGREC *lr;
	DB_TXNMGR *mgr;
	int ret, t_ret;
	void *txnlist;

	mgr = txnp->mgrp;
	dbenv = mgr->dbenv;
	logc = NULL;
	txnlist = NULL;

	if (!DBENV_LOGGING(dbenv))
		return (0);

	/*
	 * This is the simplest way to code this, but if the mallocs during
	 * recovery turn out to be a performance issue, we can do the
	 * allocation here and use DB_DBT_USERMEM.
	 */
	memset(&rdbt, 0, sizeof(rdbt));

	/*
	 * Allocate a txnlist for children and aborted page allocs.
	 * We need to associate the list with the maximal parent
	 * so that aborted pages are recovered when that transaction
	 * is commited or aborted.
	 */
	for (ptxn = txnp->parent; ptxn != NULL && ptxn->parent != NULL;)
		ptxn = ptxn->parent;

	if (ptxn != NULL && ptxn->txn_list != NULL)
		txnlist = ptxn->txn_list;
	else if (txnp->txn_list != NULL)
		txnlist = txnp->txn_list;
	else if ((ret = __db_txnlist_init(dbenv, 0, 0, NULL, &txnlist)) != 0)
		return (ret);
	else if (ptxn != NULL)
		ptxn->txn_list = txnlist;

	if (F_ISSET(txnp, TXN_CHILDCOMMIT) &&
		(ret = __db_txnlist_lsninit(dbenv, txnlist, &txnp->last_lsn)) != 0)
		return (ret);

	/*
	 * Take log records from the linked list stored in the transaction,
	 * then from the log.
	 */
	for (lr = STAILQ_FIRST(&txnp->logs);
		lr != NULL; lr = STAILQ_NEXT(lr, links)) {
		rdbt.data = lr->data;
		rdbt.size = 0;
		LSN_NOT_LOGGED(key_lsn);
		ret =
			__txn_dispatch_undo(dbenv, txnp, &rdbt, &key_lsn, txnlist);
		if (ret != 0) {
			__db_err(dbenv,
				"DB_TXN->abort: In-memory log undo failed: %s",
				db_strerror(ret));
			goto err;
		}
	}

	key_lsn = txnp->last_lsn;

	if (!IS_ZERO_LSN(key_lsn) &&
		 (ret = __log_cursor(dbenv, &logc)) != 0)
		goto err;

	while (!IS_ZERO_LSN(key_lsn)) {
		/*
		 * The dispatch routine returns the lsn of the record
		 * before the current one in the key_lsn argument.
		 */
		if ((ret = __log_c_get(logc, &key_lsn, &rdbt, DB_SET)) == 0) {
			if ((ret = __txn_dispatch_undo(dbenv,
					txnp, &rdbt, &key_lsn, txnlist)) != 0) {
				__db_err(dbenv,
					"DB_TXN->abort: txn-undo failed for LSN: %lu %lu: %s",
					(u_long) key_lsn.file,
					(u_long) key_lsn.offset, db_strerror(ret));
				goto err;
			}
		}

		if (ret != 0) {
			__db_err(dbenv,
		"DB_TXN->abort: undo failure in log_c_get for LSN: %lu %lu: %s",
				(u_long) key_lsn.file, (u_long) key_lsn.offset,
				db_strerror(ret));
			goto err;
		}
	}

	/* Dont recover pages for prepare & discarded txns */
	if (!F_ISSET(txnp, TXN_DIST_DISCARD)) {
		ret = __db_do_the_limbo(dbenv, ptxn, txnp, txnlist, LIMBO_NORMAL);
	}

err:	if (logc != NULL && (t_ret = __log_c_close(logc)) != 0 && ret == 0)
		ret = t_ret;

	if (ptxn == NULL && txnlist != NULL)
		__db_txnlist_end(dbenv, txnlist);
	return (ret);
}

/*
 * __txn_set_recover_prepared_callback --
 *  DB_ENV->set_recover_prepared_callback function
 *
 * PUBLIC: int __txn_set_recover_prepared_callback
 * PUBLIC:   __P((DB_ENV *, void (*)(const char *, const char *, const char *)));
 */
int
__txn_set_recover_prepared_callback(dbenv, func)
	DB_ENV *dbenv;
	void (*func)(const char *, const char *, const char *);
{
	dbenv->recover_prepared_callback = func;
	return 0;
}

/*
 * __txn_commit_recovered_pp --
 *	DB_ENV->txn_commit_recovered pre/post processing.
 *
 * PUBLIC: int __txn_commit_recovered_pp
 * PUBLIC:	 __P((DB_ENV *, u_int64_t));
 */
 int
 __txn_commit_recovered_pp(dbenv, dist_txnid)
	DB_ENV *dbenv;
	const char *dist_txnid;
{
	int rep_check, ret;

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (IS_REP_CLIENT(dbenv))
		return (0);
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __txn_commit_recovered(dbenv, dist_txnid);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __txn_abort_recovered_pp --
 *	DB_ENV->txn_abort_recovered pre/post processing.
 *
 * PUBLIC: int __txn_abort_recovered_pp
 * PUBLIC:	 __P((DB_ENV *, const char *));
 */
int
 __txn_abort_recovered_pp(dbenv, dist_txnid)
	DB_ENV *dbenv;
	const char *dist_txnid;
{
	int rep_check, ret;

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (IS_REP_CLIENT(dbenv))
		return (0);
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __txn_abort_recovered(dbenv, dist_txnid);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __txn_discard_recovered_pp --
 *	DB_ENV->txn_dist_discard pre/post processing.
 *
 * PUBLIC: int __txn_discard_recovered_pp
 * PUBLIC:	 __P((DB_ENV *, const char *));
 */
int
 __txn_discard_recovered_pp(dbenv, dist_txnid)
	DB_ENV *dbenv;
	const char *dist_txnid;
{
	int rep_check, ret;

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (IS_REP_CLIENT(dbenv))
		return (0);
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __txn_discard_recovered(dbenv, dist_txnid);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __txn_discard_all_recovered_pp --
 *	DB_ENV->txn_dist_discard_all pre/post processing.
 *
 * PUBLIC: int __txn_discard_all_recovered_pp
 * PUBLIC:	 __P((DB_ENV *, const char *));
 */
int
 __txn_discard_all_recovered_pp(dbenv)
	DB_ENV *dbenv;
{
	int rep_check, ret;

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (IS_REP_CLIENT(dbenv))
		return (0);
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __txn_discard_all_recovered(dbenv);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __txn_checkpoint_pp --
 *	DB_ENV->txn_checkpoint pre/post processing.
 *
 * PUBLIC: int __txn_checkpoint_pp
 * PUBLIC:	 __P((DB_ENV *, u_int32_t, u_int32_t, u_int32_t));
 */
int
__txn_checkpoint_pp(dbenv, kbytes, minutes, flags)
	DB_ENV *dbenv;
	u_int32_t kbytes, minutes, flags;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
		dbenv->tx_handle, "txn_checkpoint", DB_INIT_TXN);

	/*
	 * On a replication client, all transactions are read-only; therefore,
	 * a checkpoint is a null-op.
	 *
	 * We permit txn_checkpoint, instead of just rendering it illegal,
	 * so that an application can just let a checkpoint thread continue
	 * to operate as it gets promoted or demoted between being a
	 * master and a client.
	 */
	if (IS_REP_CLIENT(dbenv))
		return (0);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __txn_checkpoint(dbenv, kbytes, minutes, flags);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

int still_running(DB_ENV *dbenv, u_int32_t *txnarray, int count)
{
	int runcount = 0;
	DB_TXNMGR *mgr;
	TXN_DETAIL *txnp;
	DB_TXNREGION *region;
	mgr = dbenv->tx_handle;
	region = mgr->reginfo.primary;
	R_LOCK(dbenv, &mgr->reginfo);
	for (txnp = SH_TAILQ_FIRST(&region->active_txn, __txn_detail);
			txnp != NULL && runcount == 0;
		txnp = SH_TAILQ_NEXT(txnp, links, __txn_detail)) {
		for (int j = 0; j < count; j++) {
			if (txnarray[j] == txnp->txnid) {
				runcount++;
			}
		}
	}
	R_UNLOCK(dbenv, &mgr->reginfo);
	return runcount;
}

void collect_txnids(DB_ENV *dbenv, u_int32_t *txnarray, int max, int *count)
{
	int idx = 0;
	DB_TXNMGR *mgr;
	TXN_DETAIL *txnp;
	DB_TXNREGION *region;
	mgr = dbenv->tx_handle;
	region = mgr->reginfo.primary;
	R_LOCK(dbenv, &mgr->reginfo);
	for (txnp = SH_TAILQ_FIRST(&region->active_txn, __txn_detail);
			txnp != NULL;
		txnp = SH_TAILQ_NEXT(txnp, links, __txn_detail)) {
		if (idx < max) {
			txnarray[idx++] = txnp->txnid;
		}
	}
	R_UNLOCK(dbenv, &mgr->reginfo);
	(*count) = idx;
}

int bdb_checkpoint_list_push(DB_LSN lsn, DB_LSN ckp_lsn, int32_t timestamp, int push_top);

/* Configure txn_checkpoint() to sleep this much time before memp_sync() */
int gbl_ckp_sleep_before_sync = 0;

/* Don't actually write a checkpoint */
int gbl_disable_ckp = 0;

/*
 * __txn_checkpoint --
 *	DB_ENV->txn_checkpoint.
 *
 * PUBLIC: int __txn_checkpoint
 * PUBLIC:	__P((DB_ENV *, u_int32_t, u_int32_t, u_int32_t));
 */
int
__txn_checkpoint(dbenv, kbytes, minutes, flags)
	DB_ENV *dbenv;
	u_int32_t kbytes, minutes, flags;
{
	struct mintruncate_entry *newmt;
	DB_LSN ckp_lsn, last_ckp, ltrans_ckp_lsn, ckp_lsn_sav, prepared;
	DB_TXNMGR *mgr;
	DB_TXNREGION *region;
	TXN_DETAIL *txnp;
	DB_LOG *dblp;
	LOG *lp;
	time_t last_ckp_time, now;
	int32_t timestamp;
	u_int32_t bytes, gen, mbytes;
	int ret;

	ret = gen = 0;

	/*
	 * A client will only call through here during recovery,
	 * so just sync the Mpool and go home.
	 */
	if (IS_REP_CLIENT(dbenv)) {
		if (MPOOL_ON(dbenv) && (ret = __memp_sync(dbenv, NULL)) != 0) {
			__db_err(dbenv,
			"txn_checkpoint: failed to flush the buffer cache %s",
				db_strerror(ret));
			return (ret);
		} else
			return (0);
	}

	mgr = dbenv->tx_handle;
	region = mgr->reginfo.primary;

	if (gbl_disable_ckp)
		return 0;

	/*
	 * The checkpoint LSN is an LSN such that all transactions begun before
	 * it are complete.  Our first guess (corrected below based on the list
	 * of active transactions) is the last-written LSN.
	 */
	__log_txn_lsn(dbenv, &ckp_lsn, &mbytes, &bytes);

	if (!LF_ISSET(DB_FORCE)) {
		/* Don't checkpoint a quiescent database. */
		if (bytes == 0 && mbytes == 0)
			return (0);

		/*
		 * If either kbytes or minutes is non-zero, then only take the
		 * checkpoint if more than "minutes" minutes have passed or if
		 * more than "kbytes" of log data have been written since the
		 * last checkpoint.
		 */
		if (kbytes != 0 &&
			mbytes * 1024 + bytes / 1024 >= (u_int32_t)kbytes)
			goto do_ckp;

		if (minutes != 0) {
			(void)time(&now);

			R_LOCK(dbenv, &mgr->reginfo);
			last_ckp_time = region->time_ckp;
			R_UNLOCK(dbenv, &mgr->reginfo);

			if (now - last_ckp_time >= (time_t)(minutes * 60))
				goto do_ckp;
		}

		/*
		 * If we checked time and data and didn't go to checkpoint,
		 * we're done.
		 */
		if (minutes != 0 || kbytes != 0)
			return (0);
	}

do_ckp:	
	dbenv->lock_recovery_lock(dbenv, __func__, __LINE__);

	/* Retrieve lsn again after locking */
	__log_txn_lsn(dbenv, &ckp_lsn, &mbytes, &bytes);

	/*
	 * Find the oldest active transaction and figure out its "begin" LSN.
	 * This is the lowest LSN we can checkpoint, since any record written
	 * after it may be involved in a transaction and may therefore need
	 * to be undone in the case of an abort.
	 */
	R_LOCK(dbenv, &mgr->reginfo);
	for (txnp = SH_TAILQ_FIRST(&region->active_txn, __txn_detail);
		txnp != NULL;
		txnp = SH_TAILQ_NEXT(txnp, links, __txn_detail))
		if (!IS_ZERO_LSN(txnp->begin_lsn) &&
			log_compare(&txnp->begin_lsn, &ckp_lsn) < 0)
			ckp_lsn = txnp->begin_lsn;

	__txn_ltrans_find_lowest_lsn(dbenv, &ltrans_ckp_lsn);
	R_UNLOCK(dbenv, &mgr->reginfo);

	if (!IS_ZERO_LSN(ltrans_ckp_lsn) &&
		log_compare(&ltrans_ckp_lsn, &ckp_lsn) < 0)
		ckp_lsn = ltrans_ckp_lsn;

	__txn_lowest_prepared_lsn(dbenv, &prepared);
	if (!IS_ZERO_LSN(prepared) &&
		log_compare(&prepared, &ckp_lsn) < 0) {
		ckp_lsn = prepared;
	}
	if (unlikely(gbl_ckp_sleep_before_sync > 0))
		usleep(gbl_ckp_sleep_before_sync * 1000LL);

	ckp_lsn_sav = ckp_lsn;
	/* If flag is DB_FORCE, don't run perfect checkpoints. */
	if (MPOOL_ON(dbenv) &&
			(ret = __memp_sync_restartable(dbenv,
				   (LF_ISSET(DB_FORCE) ? NULL : &ckp_lsn), 0, 0)) != 0) {
		__db_err(dbenv,
			"txn_checkpoint: failed to flush the buffer cache %s",
			db_strerror(ret));
		dbenv->unlock_recovery_lock(dbenv, __func__, __LINE__);
		return (ret);
	}

	if (log_compare(&ckp_lsn_sav, &ckp_lsn)<0)
		ckp_lsn = ckp_lsn_sav;

	/*
	 * Because we can't be a replication client here, and because
	 * recovery (somewhat unusually) calls txn_checkpoint and expects
	 * it to write a log message, LOGGING_ON is the correct macro here.
	 */
	if (LOGGING_ON(dbenv) && !gbl_is_physical_replicant &&
			!LF_ISSET(DB_RECOVER_NOCKP)) {
		DB_LSN debuglsn;
		DBT op = { 0 };
		int debugtype;

		R_LOCK(dbenv, &mgr->reginfo);
		last_ckp = region->last_ckp;
		R_UNLOCK(dbenv, &mgr->reginfo);

		/*
		 * `ckp_lsn' may go backwards. Here is an example:
		 *
		 * Suppose we have a large enough buffer pool, and memp_trickle
		 * is disabled or the threshold is low enough to allow all dirty
		 * pages stay in the buffer pool.
		 *
		 * 1) TXN A begins with LSN A.
		 * 2) TXN B begins with an older LSN B.
		 * 3) TXN A reads pages, and has not modified any pages yet.
		 * 4) TXN B modifies pages.
		 * 5) Checkpoint takes place and ckp_lsn is adjusted to LSN B.
		 *    the last checkpoint LSN becomes LSN B.
		 * 6) TXN A modifies pages.
		 * 7) Checkpoint takes place and ckp_lsn is adjusted to LSN A.
		 *    LSN A is younger than the last checkpoint LSN which is LSN B.
		 *
		 * If ckp_lsn goes backwards, we skip the checkpoint record.
		 * It should be fine because we have a separate checkpoint file
		 * which tells us where the checkpoints are in the log files.
		 * However, it does break the assumption that every successful
		 * __txn_checkpoint() writes a checkpoint in the log.
		 */
		if (dbenv->tx_perfect_ckp && log_compare(&ckp_lsn, &last_ckp) <= 0) {
			dbenv->unlock_recovery_lock(dbenv, __func__, __LINE__);
			return (0);
		}

		if (REP_ON(dbenv)) {
			__rep_get_gen(dbenv, &gen);
			if (gbl_fullrecovery && gen > gbl_recovery_gen) {
				if (gbl_reproduce_ckp_bug) {
					logmsg(LOGMSG_USER, "Reproducing ckp-bug by writing larger-generation ckp: %d vs %d\n", gbl_recovery_gen, gen);

				} else {
					logmsg(LOGMSG_FATAL, "txn_checkpoint: writing larger gen-checkpoint in full-recovery mode, recovery_gen=%u, gen=%u\n",
							gbl_recovery_gen, gen);
					abort();
				}
			}
		}

		/* Get the fq-lock now to preserve our locking order */
		dblp = dbenv->lg_handle;
		lp = dblp->reginfo.primary;
		MUTEX_LOCK(dbenv, &lp->fq_mutex);

		/* Put out a special debug record.  Recovery will look for it
		 * to know where to start. */
		Pthread_rwlock_wrlock(&dbenv->dbreglk);
		op.data = &debugtype;
		op.size = sizeof(int);
		debugtype = htonl(2);
		ret =
			__db_debug_log(dbenv, NULL, &debuglsn, DB_LOG_DONT_LOCK,
			&op, -1, NULL, NULL, 0);
		if (ret) {
			Pthread_rwlock_unlock(&dbenv->dbreglk);
			MUTEX_UNLOCK(dbenv, &lp->fq_mutex);
			dbenv->unlock_recovery_lock(dbenv, __func__, __LINE__);
			return ret;
		}

		Pthread_mutex_lock(&dbenv->mintruncate_lk);
		if (dbenv->last_mintruncate_dbreg_start.file < debuglsn.file) {
			newmt = malloc(sizeof(*newmt));
#ifdef MINTRUNCATE_DEBUG
			newmt->func = __func__;
#endif
			newmt->type = MINTRUNCATE_DBREG_START;
			newmt->timestamp = 0;
			newmt->lsn = dbenv->last_mintruncate_dbreg_start = debuglsn;
			ZERO_LSN(newmt->ckplsn);
			listc_atl(&dbenv->mintruncate, newmt);
		}
		Pthread_mutex_unlock(&dbenv->mintruncate_lk);

		/*
		 * Put out records for the open files before we log
		 * the checkpoint.  The records are certain to be at
		 * or after ckp_lsn, but before the checkpoint record
		 * itself, so they're sure to be included if we start
		 * recovery from the ckp_lsn contained in this
		 * checkpoint.
		 */
		ckp_lsn_sav = ckp_lsn;
		timestamp = (int32_t)time(NULL);

		u_int64_t max_utxnid;
		Pthread_mutex_lock(&dbenv->utxnid_lock);
		max_utxnid = dbenv->next_utxnid;
		Pthread_mutex_unlock(&dbenv->utxnid_lock);

		u_int32_t rectype = ((gbl_fullrecovery || LF_ISSET(DB_RECOVERY_CKP)) &&
			gbl_recovery_ckp) ? DB___txn_ckp_recovery : DB___txn_ckp;
		if ((ret = __dbreg_open_files_checkpoint(dbenv)) != 0 ||
			(ret = __txn_ckp_log(dbenv, NULL, rectype, &ckp_lsn,
				DB_FLUSH |DB_LOG_PERM |DB_LOG_CHKPNT |
				DB_LOG_DONT_LOCK, &ckp_lsn, &last_ckp, timestamp,
				gen, max_utxnid)) != 0) {
			__db_err(dbenv,
				"txn_checkpoint: log failed at LSN [%ld %ld] %s",
				(long)ckp_lsn.file, (long)ckp_lsn.offset,
				db_strerror(ret));
			Pthread_rwlock_unlock(&dbenv->dbreglk);
			MUTEX_UNLOCK(dbenv, &lp->fq_mutex);
			dbenv->unlock_recovery_lock(dbenv, __func__, __LINE__);
			return (ret);
		}
		Pthread_rwlock_unlock(&dbenv->dbreglk);
		MUTEX_UNLOCK(dbenv, &lp->fq_mutex);

		Pthread_mutex_lock(&dbenv->mintruncate_lk);
		if (ckp_lsn_sav.file > dbenv->last_mintruncate_ckplsn.file) {
			newmt = malloc(sizeof(*newmt));
#ifdef MINTRUNCATE_DEBUG
			newmt->func = __func__;
#endif
			newmt->type = MINTRUNCATE_CHECKPOINT;
			newmt->timestamp = timestamp;
			newmt->lsn = ckp_lsn;
			newmt->ckplsn = ckp_lsn_sav;
			listc_atl(&dbenv->mintruncate, newmt);
			if (dbenv->mintruncate_first.file == 0)
				dbenv->mintruncate_first = ckp_lsn;
			dbenv->last_mintruncate_ckplsn = ckp_lsn_sav;
		}
		Pthread_mutex_unlock(&dbenv->mintruncate_lk);

		ret = bdb_checkpoint_list_push(ckp_lsn, ckp_lsn_sav, timestamp, 0);
		if (ret) {
			logmsg(LOGMSG_ERROR, 
				"%s: failed to push to checkpoint list, ret %d\n",
				__func__, ret);
			dbenv->unlock_recovery_lock(dbenv, __func__, __LINE__);
			return ret;
		}

		ret = __log_flush_pp(dbenv, NULL);
		if (ret == 0)
			__txn_updateckp(dbenv, &ckp_lsn);	/* this is the output lsn from txn_ckp_log */
	}
	dbenv->unlock_recovery_lock(dbenv, __func__, __LINE__);
	return (ret);
}

/*
 * __txn_getckp --
 *	Get the LSN of the last transaction checkpoint.
 *
 * PUBLIC: int __txn_getckp __P((DB_ENV *, DB_LSN *));
 */
int
__txn_getckp(dbenv, lsnp)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
{
	DB_LSN lsn;
	DB_TXNMGR *mgr;
	DB_TXNREGION *region;

	mgr = dbenv->tx_handle;
	region = mgr->reginfo.primary;

	R_LOCK(dbenv, &mgr->reginfo);
	lsn = region->last_ckp;
	R_UNLOCK(dbenv, &mgr->reginfo);

	if (IS_ZERO_LSN(lsn))
		return (DB_NOTFOUND);

	*lsnp = lsn;
	return (0);
}

/*
 * __txn_activekids --
 *	Return if this transaction has any active children.
 *
 * PUBLIC: int __txn_activekids __P((DB_ENV *, u_int32_t, DB_TXN *));
 */
int
__txn_activekids(dbenv, rectype, txnp)
	DB_ENV *dbenv;
	u_int32_t rectype;
	DB_TXN *txnp;
{
	/*
	 * On a child commit, we know that there are children (i.e., the
	 * commiting child at the least.  In that case, skip this check.
	 */
	normalize_rectype(&rectype);
	if (F_ISSET(txnp, TXN_COMPENSATE) || rectype == DB___txn_child) {
		return (0);
	}

	if (TAILQ_FIRST(&txnp->kids) != NULL) {
		__db_err(dbenv, "Child transaction is active");
		return (EPERM);
	}
	return (0);
}

/*
 * __txn_force_abort --
 *	Force an abort record into the log if the commit record
 *	failed to get to disk.
 *
 * PUBLIC: int __txn_force_abort __P((DB_ENV *, u_int8_t *));
 */
int
__txn_force_abort(dbenv, buffer)
	DB_ENV *dbenv;
	u_int8_t *buffer;
{
	DB_CIPHER *db_cipher;
	HDR *hdr;
	u_int32_t hdrlen, offset, opcode, rec_len, sum_len;
	u_int8_t *bp, *key, chksum[DB_MAC_KEY];
	size_t hdrsize;
	int ret;

	db_cipher = dbenv->crypto_handle;

	/*
	 * This routine depends on the layout of HDR and the __txn_regop
	 * __txn_xa_regop records in txn.src.  We are passed the beginning
	 * of the commit record in the log buffer and overwrite the
	 * commit with an abort and recalculate the checksum.
	 */
	hdrsize = CRYPTO_ON(dbenv) ? HDR_CRYPTO_SZ : HDR_NORMAL_SZ;

	hdr = (HDR *)buffer;
	memcpy(&hdrlen, buffer + SSZ(HDR, len), sizeof(hdr->len));
	rec_len = hdrlen - hdrsize;

	u_int32_t rectype = 0;
	LOGCOPY_32(&rectype, buffer + hdrsize);
	int utxnid_logged = normalize_rectype(&rectype);

	offset = sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) + (utxnid_logged ? sizeof(u_int64_t) : 0);
	if (CRYPTO_ON(dbenv)) {
		key = db_cipher->mac_key;
		sum_len = DB_MAC_KEY;
		if ((ret = db_cipher->decrypt(dbenv, db_cipher->data,
			&hdr->iv[0], buffer + hdrsize, rec_len)) != 0)
			return (__db_panic(dbenv, ret));
	} else {
		key = NULL;
		sum_len = sizeof(u_int32_t);
	}
	bp = buffer + hdrsize + offset;
	opcode = TXN_ABORT;
	memcpy(bp, &opcode, sizeof(opcode));

	if (CRYPTO_ON(dbenv) &&
		(ret = db_cipher->encrypt(dbenv,
		db_cipher->data, &hdr->iv[0], buffer + hdrsize, rec_len)) != 0)
		return (__db_panic(dbenv, ret));

	__db_chksum(buffer + hdrsize, rec_len, key, chksum);
	memcpy(buffer + SSZ(HDR, chksum), &chksum, sum_len);

	return (0);
}

/*
 * __txn_preclose
 *	Before we can close an environment, we need to check if we
 * were in the midst of taking care of restored transactions.  If
 * so, then we need to close the files that we opened.
 *
 * PUBLIC: int __txn_preclose __P((DB_ENV *));
 */
int
__txn_preclose(dbenv)
	DB_ENV *dbenv;
{
	DB_TXNMGR *mgr;
	DB_TXNREGION *region;
	int do_closefiles, ret;

	mgr = (DB_TXNMGR *)dbenv->tx_handle;
	region = mgr->reginfo.primary;
	do_closefiles = 0;

	R_LOCK(dbenv, &mgr->reginfo);
	if (region != NULL &&
		region->stat.st_nrestores
		<= mgr->n_discards && mgr->n_discards != 0)
		do_closefiles = 1;
	R_UNLOCK(dbenv, &mgr->reginfo);
	if (do_closefiles) {
		/*
		 * Set the DBLOG_RECOVER flag while closing these
		 * files so they do not create additional log records
		 * that will confuse future recoveries.
		 */
		F_SET((DB_LOG *)dbenv->lg_handle, DBLOG_RECOVER);
		ret = __dbreg_close_files(dbenv);
		F_CLR((DB_LOG *)dbenv->lg_handle, DBLOG_RECOVER);
	} else
		ret = 0;

	return (ret);
}

/*
 * __txn_reset --
 *	Reset the last txnid to its minimum value, and log the reset.
 *
 * PUBLIC: int __txn_reset __P((DB_ENV *));
 */
int
__txn_reset(dbenv)
	DB_ENV *dbenv;
{
	DB_LSN scrap;
	DB_TXNREGION *region;

	/* physical replicants cannot log the reset */
	if (gbl_is_physical_replicant)
		return 0;

	region = ((DB_TXNMGR *)dbenv->tx_handle)->reginfo.primary;
	region->last_txnid = TXN_MINIMUM;

	DB_ASSERT(LOGGING_ON(dbenv));
	return (__txn_recycle_log(dbenv,
		NULL, &scrap, 0, TXN_MINIMUM, TXN_MAXIMUM));
}

/*
 * __txn_updateckp --
 *	Update the last_ckp field in the transaction region.  This happens
 * at the end of a normal checkpoint and also when a replication client
 * receives a checkpoint record.
 *
 * PUBLIC: void __txn_updateckp __P((DB_ENV *, DB_LSN *));
 */
void
__txn_updateckp(dbenv, lsnp)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
{
	DB_TXNMGR *mgr;
	DB_TXNREGION *region;

	mgr = dbenv->tx_handle;
	region = mgr->reginfo.primary;

	/* We need to write the checkpoint first.  Otherwise 
	 * log deletion code can get in and delete the log the
	 * checkpoint is in, and we'll be non-recoverable if we
	 * crash at that point.  This isn't hypothetical. */
	__checkpoint_save(dbenv, lsnp, 0);

	/*
	 * We want to make sure last_ckp only moves forward;  since
	 * we drop locks above and in log_put, it's possible
	 * for two calls to __txn_ckp_log to finish in a different
	 * order from how they were called.
	 */
	R_LOCK(dbenv, &mgr->reginfo);
	if (log_compare(&region->last_ckp, lsnp) < 0) {
		region->last_ckp = *lsnp;
		(void)time(&region->time_ckp);
		ctrace("DBENV->region last_ckp = %d:%d\n", lsnp->file,
			lsnp->offset);
	}
	if (IS_ZERO_LSN(region->last_ckp))
		logmsg(LOGMSG_INFO, "%s:%d last_ckp is 0:0\n", __FILE__, __LINE__);
	R_UNLOCK(dbenv, &mgr->reginfo);
}

int
cmp_by_lsn(const void *pp1, const void *pp2)
{
	const struct logrecord *p1 = (struct logrecord *)pp1;
	const struct logrecord *p2 = (struct logrecord *)pp2;

	return log_compare(&p1->lsn, &p2->lsn);
}

int
dumptxn(DB_ENV * dbenv, DB_LSN * lsnpp)
{
	int ret;
	int had_serializable_records;
	DB_LOGC *logc = NULL;
	DBT dbt = { 0 };
	LSN_COLLECTION lc = { 0 };
	__db_addrem_args *a = NULL;
	DB_LSN lsnv = *lsnpp;
	DB_LSN *lsnp = &lsnv;

	dbt.flags |= DB_DBT_REALLOC;
	ret =
		__rep_collect_txn(dbenv, lsnp, &lc, &had_serializable_records,
		NULL);
	if (ret) {
		__db_err(dbenv, "can't find logs for txn at " PR_LSN "\n",
			PARM_LSNP(lsnp));
		goto done;
	}
	// qsort(lc.array, lc.nlsns, sizeof(struct logrecord), cmp_by_lsn);

	ret = __log_cursor(dbenv, &logc);
	if (ret) {
		__db_err(dbenv, "can't get log cursor");
		goto done;
	}

	logmsg(LOGMSG_USER, "{ %d log records\n", lc.nlsns);
	for (int i = lc.nlsns - 1; i >= 0; i--) {
		u_int32_t type;

		if (a) {
			free(a);
			a = NULL;
		}
		ret = __log_c_get(logc, &lc.array[i].lsn, &dbt, DB_SET);
		if (ret) {
			__db_err(dbenv,
				"can't fetch log record at " PR_LSN " ret %d\n",
				PARM_LSN(lc.array[i].lsn), ret);
			goto done;
		}
		LOGCOPY_32(&type, dbt.data);
		normalize_rectype(&type);
		if (type == DB___db_addrem) {
			DB *db;
			char *name;

			ret = __db_addrem_read_int(dbenv, dbt.data, 0, &a);
			if (ret) {
				__db_err(dbenv, "can't decode addrem record\n");
				goto done;
			}

			ret =
				__dbreg_id_to_db(dbenv, NULL, &db, a->fileid, 0,
				&lc.array[i].lsn, 0);
			if (ret == DB_DELETED)
				name = "deleted";
			else if (ret)
				name = "??????";
			else
				name = db->fname;

			if (a->opcode == DB_ADD_DUP) {
				logmsg(LOGMSG_USER, "addrem: " PR_LSN " %s ",
					PARM_LSN(lc.array[i].lsn), name);
				fsnapf(stdout, a->dbt.data, a->dbt.size);
			}
		} else if (type == 10019) {
			logmsg(LOGMSG_USER, "blkseq: " PR_LSN "\n",
				PARM_LSN(lc.array[i].lsn));
		}
	}
	logmsg(LOGMSG_USER, "} commit at " PR_LSN "\n", PARM_LSNP(lsnpp));

done:
	if (logc)
		__log_c_close(logc);
	if (dbt.data)
		free(dbt.data);
	if (a)
		free(a);
	return ret;
}
