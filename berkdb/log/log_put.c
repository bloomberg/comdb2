/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: log_put.c,v 11.145 2003/09/13 19:20:39 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#include <fcntl.h> /* for sync_file_range */

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

#include <stdio.h>
#include <string.h>
#endif

#include <assert.h>
#include <stdlib.h>

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/hmac.h"
#include "dbinc/log.h"
#include "dbinc/db_swap.h"
#include "dbinc/txn.h"
#include <pthread.h>

#include <alloca.h>
#include <netinet/in.h>

#include "logmsg.h"
#include <locks_wrap.h>
#include <poll.h>

extern unsigned long long get_commit_context(const void *, uint32_t generation);
extern int bdb_update_startlwm_berk(void *statearg, unsigned long long ltranid,
    DB_LSN *firstlsn);
extern int bdb_commitdelay(void *arg);
extern int bdb_push_pglogs_commit(void *in_bdb_state, DB_LSN commit_lsn, 
	uint32_t generation, unsigned long long ltranid, int push);


static int __log_encrypt_record __P((DB_ENV *, DBT *, HDR *, u_int32_t));
static int __log_file __P((DB_ENV *, const DB_LSN *, char *, size_t));
static int __log_fill __P((DB_LOG *, DB_LSN *, void *, u_int32_t));
static int __log_fill_segments __P((DB_LOG *, DB_LSN *, DB_LSN *, void *,
	u_int32_t));
static int __log_flush_commit __P((DB_ENV *, const DB_LSN *, u_int32_t));
static int __log_newfh __P((DB_LOG *));
static int __log_put_next __P((DB_ENV *,
	DB_LSN *, u_int64_t *, DBT *, const DBT *, HDR *, DB_LSN *, int,
	u_int8_t *key, u_int32_t));
static int __log_putr __P((DB_LOG *, DB_LSN *, const DBT *, u_int32_t, HDR *));
static int __log_write __P((DB_LOG *, void *, u_int32_t));
static void __log_sync_range __P((DB_LOG *, off_t));

pthread_mutex_t log_write_lk = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t log_write_cond = PTHREAD_COND_INITIALIZER;
static pthread_once_t log_write_once = PTHREAD_ONCE_INIT;
static pthread_t log_write_td = 0;
static int log_write_td_should_stop = 0;
static DB_LOG *log_write_dblp = NULL;

int __db_debug_log(DB_ENV *, DB_TXN *, DB_LSN *, u_int32_t, const DBT *,
    int32_t, const DBT *, const DBT *, u_int32_t);

extern int gbl_inflate_log;
pthread_cond_t gbl_logput_cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t gbl_logput_lk = PTHREAD_MUTEX_INITIALIZER;
int gbl_num_logput_listeners = 0;

/* TODO: Delete once finished with testing on local reps */
extern int gbl_is_physical_replicant;
int gbl_abort_on_illegal_log_put = 0;

extern int gbl_wal_osync;

extern char *gbl_physrep_source_dbname;

/*
 * __log_put_pp --
 *	DB_ENV->log_put pre/post processing.
 *
 * PUBLIC: int __log_put_pp __P((DB_ENV *, DB_LSN *, const DBT *, u_int32_t));
 */
int
__log_put_pp(dbenv, lsnp, udbt, flags)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	const DBT *udbt;
	u_int32_t flags;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lg_handle, "DB_ENV->log_put", DB_INIT_LOG);

	/* Validate arguments: check for allowed flags. */
	if ((ret = __db_fchk(dbenv, "DB_ENV->log_put", flags,
	    DB_LOG_CHKPNT | DB_LOG_COMMIT |
	    DB_FLUSH | DB_LOG_NOCOPY | DB_LOG_PERM | DB_LOG_WRNOSYNC)) != 0)
		return (ret);

	/* DB_LOG_WRNOSYNC and DB_FLUSH are mutually exclusive. */
	if (LF_ISSET(DB_LOG_WRNOSYNC) && LF_ISSET(DB_FLUSH))
		return (__db_ferr(dbenv, "DB_ENV->log_put", 1));

	/* Replication clients should never write log records. */
	if (IS_REP_CLIENT(dbenv)) {
		__db_err(dbenv,
		    "DB_ENV->log_put is illegal on replication clients");
		if (gbl_abort_on_illegal_log_put)
			abort();
		return (EINVAL);
	}

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __log_put(dbenv, lsnp, udbt, flags);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

int gbl_commit_delay_trace = 0;

static inline int is_commit_record(int rectype) {
    switch(rectype) {
        /* regop regop_gen regop_rowlocks */
        case (DB___txn_regop): 
        case (DB___txn_regop_gen):
        case (DB___txn_dist_commit):
        case (DB___txn_regop_rowlocks):
            return 1;
            break;
        default:
            return 0;
            break;
    }
}
 
static int
__log_put_int_int(dbenv, lsnp, contextp, udbt, flags, off_context, usr_ptr)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	u_int64_t *contextp;
	const DBT *udbt;
	u_int32_t flags;
	int off_context;
	void *usr_ptr;
{
	DB_CIPHER *db_cipher;
	DBT *dbt, t;
	DB_LOG *dblp;
	DB_LSN lsn, old_lsn;
	HDR hdr;
	LOG *lp;
	int lock_held, need_free, ret;
	u_int8_t *key = NULL;
	u_int32_t rectype = 0;
	int delay;

	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;
	db_cipher = dbenv->crypto_handle;
	dbt = &t;
	t = *udbt;
	u_int8_t *pp;
    int adjsize = 0;
	int utxnid_logged = 0;

	lock_held = need_free = 0;
	flags &= (~(DB_LOG_DONT_LOCK | DB_LOG_DONT_INFLATE));

	{
		pp = udbt->data;
		LOGCOPY_32(&rectype, pp);
		utxnid_logged = normalize_rectype(&rectype);
	}

    /* prevent local replicant from generating logs */
    if (gbl_is_physical_replicant)
    {
        logmsg(LOGMSG_FATAL, "%s line %d invalid logput for physical "
               "replicant\n", __func__, __LINE__);
        abort();
    }

	if (!IS_REP_MASTER(dbenv) && !(dblp->flags & DBLOG_RECOVER)) {

		if (dbenv->attr.warn_on_replicant_log_write)
			logmsg(LOGMSG_USER, "not master and not in recovery - I shouldn't be writing logs!!!\n");
		if (dbenv->attr.abort_on_replicant_log_write)
			abort();
	}

	/*
	 * If we are coming from the logging code, we use an internal flag,
	 * DB_LOG_NOCOPY, because we know we can overwrite/encrypt the log
	 * record in place.  Otherwise, if a user called log_put then we
	 * must copy it to new memory so that we know we can write it.
	 *
	 * We also must copy it to new memory if we are a replication master
	 * so that we retain an unencrypted copy of the log record to send
	 * to clients.
	 */
	if (!LF_ISSET(DB_LOG_NOCOPY) || IS_REP_MASTER(dbenv)) {
		if (CRYPTO_ON(dbenv)) {
            adjsize = db_cipher->adj_size(udbt->size);
			t.size += adjsize;
        }

		if (t.size > 4096) {
			if ((ret = __os_calloc(dbenv, 1, t.size, &t.data)) != 0)
				goto err;
			need_free = 1;
		} else
			t.data = alloca(t.size);

		memcpy(t.data, udbt->data, udbt->size);

        if (adjsize) {
            assert(adjsize < 16);
            uint8_t *pad = (uint8_t*) t.data + (t.size - adjsize);
            for (int i = 0; i < adjsize; i++)
                pad[i] = adjsize;
        }
	}
	unsigned long long ltranid = 0;
	if (10006 == rectype) {
		/* Find the logical tranid.  Offset should be (rectype + txn_num + last_lsn + txn_unum) */
		ltranid = *(unsigned long long *)(&pp[4 + 4 + 8 + (utxnid_logged ? 8 : 0)]);
	}

    /* try to do this before grabbing the region lock */
    if (!(off_context >= 0 && IS_REP_MASTER(dbenv))) {
        if ((ret = __log_encrypt_record(dbenv, dbt, &hdr, udbt->size)) != 0)
            goto err;
        if (CRYPTO_ON(dbenv))
            key = db_cipher->mac_key;
        else
            key = NULL;
        __db_chksum(dbt->data, dbt->size, key, hdr.chksum);
    } else {
        if (CRYPTO_ON(dbenv)) {
            hdr.size = HDR_CRYPTO_SZ;
            hdr.orig_size = udbt->size;
        } else {
            hdr.size = HDR_NORMAL_SZ;
        }
    }


	R_LOCK(dbenv, &dblp->reginfo);
	lock_held = 1;

	ZERO_LSN(old_lsn);

	if ((ret =
		__log_put_next(dbenv, lsnp, contextp, dbt, udbt, &hdr, &old_lsn,
		    off_context, key, flags)) != 0)
		goto panic_check;

	lsn = *lsnp;

	/*if (DB_llog_ltran_start == rectype) */
	if (10006 == rectype) {
		bdb_update_startlwm_berk(dbenv->app_private, ltranid, &lsn);
	}

	if (IS_REP_MASTER(dbenv)) {

		/*
		 * Replication masters need to drop the lock to send
		 * messages, but we want to drop and reacquire it a minimal
		 * number of times.
		 */
		R_UNLOCK(dbenv, &dblp->reginfo);
		lock_held = 0;
		/*
		 * If we are not a rep application, but are sharing a
		 * master rep env, we should not be writing log records.
		 */
		if (dbenv->rep_send == NULL) {
			__db_err(dbenv, "%s %s",
			    "Non-replication DB_ENV handle attempting",
			    "to modify a replicated environment");
			ret = EINVAL;
			goto err;
		}

		/*
		 * If we changed files and we're in a replicated
		 * environment, we need to inform our clients now that
		 * we've dropped the region lock.
		 *
		 * Note that a failed NEWFILE send is a dropped message
		 * that our client can handle, so we can ignore it.  It's
		 * possible that the record we already put is a commit, so
		 * we don't just want to return failure.
		 */
		if (!IS_ZERO_LSN(old_lsn))
			(void)__rep_send_message(dbenv,
			    db_eid_broadcast, REP_NEWFILE, &old_lsn, NULL, 0,
			    usr_ptr);

		/*
		 * Then send the log record itself on to our clients.
		 *
		 * If the send fails and we're a commit or checkpoint,
		 * there's nothing we can do;  the record's in the log.
		 * Flush it, even if we're running with TXN_NOSYNC, on the
		 * grounds that it should be in durable form somewhere.
		 */
		/*
		 * !!!
		 * In the crypto case, we MUST send the udbt, not the
		 * now-encrypted dbt.  Clients have no way to decrypt
		 * without the header.
		 */
		/* COMDB2 MODIFICATION
		 *
		 * We want to be able to throttle log propagation to
		 * avoid filling the net queue; this will allow signal
		 * messages and catching up log replies to be
		 * transferred even though the database is under heavy
		 * load.  Problem is, in berkdb_send_rtn both regular
		 * log messages and catching up log replies are coming
		 * as REP_LOG In __log_push we replace REP_LOG with
		 * REP_LOG_LOGPUT so we know that this must be
		 * throttled; we revert to REP_LOG in the same routine
		 * before sending the message
		 */
		if ((__rep_send_message(dbenv,
			    db_eid_broadcast, REP_LOG_LOGPUT, &lsn, udbt, flags,
			    usr_ptr) != 0) && LF_ISSET(DB_LOG_PERM))
			 LF_SET(DB_FLUSH);
	}

	/*
	 * If needed, do a flush.  Note that failures at this point
	 * are only permissible if we know we haven't written a commit
	 * record;  __log_flush_commit is responsible for enforcing this.
	 *
	 * If a flush is not needed, see if WRITE_NOSYNC was set and we
	 * need to write out the log buffer.
	 */
	if (LF_ISSET(DB_FLUSH | DB_LOG_WRNOSYNC)) {
		if (!lock_held) {
			R_LOCK(dbenv, &dblp->reginfo);
			lock_held = 1;
		}
		if ((ret = __log_flush_commit(dbenv, &lsn, flags)) != 0)
			goto panic_check;
	}

	*lsnp = lsn;

	/*
	 * If flushed a checkpoint record, reset the "bytes since the last
	 * checkpoint" counters.
	 */
	if (LF_ISSET(DB_LOG_CHKPNT))
		lp->stat.st_wc_bytes = lp->stat.st_wc_mbytes = 0;

	if (0) {
panic_check:	/*
		 * Writing log records cannot fail if we're a replication
		 * master.  The reason is that once we send the record to
		 * replication clients, the transaction can no longer
		 * abort, otherwise the master would be out of sync with
		 * the rest of the replication group.  Panic the system.
		 */
		if (ret != 0 && IS_REP_MASTER(dbenv))
			ret = __db_panic(dbenv, ret);
	}
err:
	if (lock_held)
		R_UNLOCK(dbenv, &dblp->reginfo);
	if (need_free)
		__os_free(dbenv, dbt->data);

	if (gbl_num_logput_listeners > 0) {
		Pthread_mutex_lock(&gbl_logput_lk);
		if (gbl_num_logput_listeners > 0)
			Pthread_cond_broadcast(&gbl_logput_cond);
		Pthread_mutex_unlock(&gbl_logput_lk);
	}

	if (IS_REP_MASTER(dbenv) && is_commit_record(rectype) && 
			(delay = bdb_commitdelay(dbenv->app_private))) {
		static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
		static unsigned long long count=0;
		static int lastpr = 0;
		int now;

		/* Don't lock out anything else */
		Pthread_mutex_lock(&lk);
		poll(NULL, 0, delay);
		Pthread_mutex_unlock(&lk);
		count++;
		if (gbl_commit_delay_trace && (now = time(NULL))-lastpr) {
			logmsg(LOGMSG_USER, "%s line %d commit-delayed for %d ms, %llu "
					"total-delays\n", __func__, __LINE__, delay, count);
			lastpr = now;
		}
	}

	/*
	 * If auto-remove is set and we switched files, remove unnecessary
	 * log files.
	 */
	if (ret == 0 &&
	    F_ISSET(dbenv, DB_ENV_LOG_AUTOREMOVE) && !IS_ZERO_LSN(old_lsn))
		__log_autoremove(dbenv);

	return (ret);
}

static int
__log_put_int(dbenv, lsnp, contextp, udbt, flags, off_context, usr_ptr)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	u_int64_t *contextp;
	const DBT *udbt;
	u_int32_t flags;
	int off_context;
	void *usr_ptr;
{
	if (gbl_inflate_log && !LF_ISSET(DB_LOG_DONT_INFLATE)) {
		static int inflate_carry = 0;
		int target, *op;
		int need_free = 0, ret, nflags = DB_LOG_DONT_INFLATE;
		DB_LSN lsn;
		DBT d;
		char *data;

		target =
		    ((udbt->size * gbl_inflate_log) / 100) + inflate_carry - 36;

		if (target < 4) {
			inflate_carry += ((udbt->size * gbl_inflate_log) / 100);
			goto logput;
		}

		if (target > 4096) {
			if ((ret = __os_malloc(dbenv, target, &data)) != 0) {
				goto logput;
			}
			need_free = 1;
		} else {
			data = alloca(target);
		}

		memset(&d, 0, sizeof(d));
		d.data = (void *)data;
		d.size = (u_int32_t)target;

		op = (int *)data;
		*op = htonl(3);

		if (LF_ISSET(DB_LOG_DONT_LOCK))
			nflags |= DB_LOG_DONT_LOCK;

		if ((ret = __db_debug_log((DB_ENV *)dbenv, NULL, &lsn,
			    nflags, &d, -1, NULL, NULL, 0)) != 0) {
			if (need_free)
				__os_free(dbenv, data);
			goto logput;
		}
		inflate_carry = 0;
	}
logput:
	return __log_put_int_int(dbenv, lsnp, contextp, udbt, flags,
	    off_context, usr_ptr);
}


/*
 * __log_put --
 *	DB_ENV->log_put.
 *
 * PUBLIC: int __log_put __P((DB_ENV *, DB_LSN *, const DBT *, u_int32_t));
 */
int
__log_put(dbenv, lsnp, udbt, flags)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	const DBT *udbt;
	u_int32_t flags;
{
	int ret;

	if (!(flags & DB_LOG_DONT_LOCK))
		Pthread_rwlock_rdlock(&dbenv->dbreglk);
	ret = __log_put_int(dbenv, lsnp, NULL, udbt, flags, -1, NULL);

	if (!(flags & DB_LOG_DONT_LOCK))
		Pthread_rwlock_unlock(&dbenv->dbreglk);
	return ret;
}

/*
 * __log_put --
 *	DB_ENV->log_put.
 *
 * PUBLIC: int __log_put_commit_context __P((DB_ENV *, DB_LSN *, u_int64_t *, const DBT *, u_int32_t, int, void *));
 */
int
__log_put_commit_context(dbenv, lsnp, contextp, udbt, flags, off_context,
    usr_ptr)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	u_int64_t *contextp;
	const DBT *udbt;
	u_int32_t flags;
	int off_context;
	void *usr_ptr;
{
	int ret;

	if (!(flags & DB_LOG_DONT_LOCK))
		Pthread_rwlock_rdlock(&dbenv->dbreglk);
	ret =
	    __log_put_int(dbenv, lsnp, contextp, udbt, flags, off_context,
	    usr_ptr);
	if (!(flags & DB_LOG_DONT_LOCK))
		Pthread_rwlock_unlock(&dbenv->dbreglk);
	return ret;
}

/* Return the number of non-written bytes.  Racy, but it doesn't matter. */
static inline int
__count_non_written_bytes(LOG *lp)
{
	u_int32_t l_off, bytes = 0;

	if (lp->num_segments > 1) {
		l_off = lp->l_off;

		if (l_off > lp->b_off) {
			bytes += lp->buffer_size - l_off;
			l_off = 0;
		}

		if (l_off < lp->b_off) {
			bytes += lp->b_off - l_off;
		}

		return bytes;
	} else {
		return lp->b_off;
	}
}

/*
 * __log_txn_lsn --
 *
 * PUBLIC: void __log_txn_lsn
 * PUBLIC:     __P((DB_ENV *, DB_LSN *, u_int32_t *, u_int32_t *));
 */
void
__log_txn_lsn(dbenv, lsnp, mbytesp, bytesp)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	u_int32_t *mbytesp, *bytesp;
{
	DB_LOG *dblp;
	LOG *lp;

	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;

	R_LOCK(dbenv, &dblp->reginfo);

	/*
	 * We are trying to get the LSN of the last entry in the log.  We use
	 * this in two places: 1) DB_ENV->txn_checkpoint uses it as a first
	 * value when trying to compute an LSN such that all transactions begun
	 * before it are complete.   2) DB_ENV->txn_begin uses it as the
	 * begin_lsn.
	 *
	 * Typically, it's easy to get the last written LSN, you simply look
	 * at the current log pointer and back up the number of bytes of the
	 * last log record.  However, if the last thing we did was write the
	 * log header of a new log file, then, this doesn't work, so we return
	 * the first log record that will be written in this new file.
	 */
	*lsnp = lp->lsn;
	if (lp->lsn.offset > lp->len)
		lsnp->offset -= lp->len;

	/*
	 * Since we're holding the log region lock, return the bytes put into
	 * the log since the last checkpoint, transaction checkpoint needs it.
	 *
	 * We add the current buffer offset so as to count bytes that have not
	 * yet been written, but are sitting in the log buffer.
	 */

	/*
	 * Count_non_written bytes is racy with the threaded write,
	 * but it doesn't matter: it's only used to determine whether
	 * there have been enough log- writes to allow a checkpoint.
	 */
	if (mbytesp != NULL) {
		*mbytesp = lp->stat.st_wc_mbytes;
		*bytesp =
		    (u_int32_t)(lp->stat.st_wc_bytes +
		    __count_non_written_bytes(lp));
	}

	R_UNLOCK(dbenv, &dblp->reginfo);
}

/*
 * __log_put_next --
 *	Put the given record as the next in the log, wherever that may
 * turn out to be.
 */
static int
__log_put_next(dbenv, lsn, context, dbt, udbt, hdr, old_lsnp, off_context, key, flags)
	DB_ENV *dbenv;
	DB_LSN *lsn;
	u_int64_t *context;
	DBT *dbt;
	const DBT *udbt;
	HDR *hdr;
	DB_LSN *old_lsnp;
	int off_context;
	u_int8_t *key;
	u_int32_t flags;
{
	DB_LOG *dblp;
	DB_LSN old_lsn;
	LOG *lp;
	u_int8_t *pp;
	DB_CIPHER *db_cipher;
	int newfile, ret;
	u_int32_t rectype;
	uint32_t generation;
 	unsigned long long ctx;

	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;
	db_cipher = dbenv->crypto_handle;

	/*
	 * Save a copy of lp->lsn before we might decide to switch log
	 * files and change it.  If we do switch log files, and we're
	 * doing replication, we'll need to tell our clients about the
	 * switch, and they need to receive a NEWFILE message
	 * with this "would-be" LSN in order to know they're not
	 * missing any log records.
	 */
	old_lsn = lp->lsn;
	newfile = 0;
	generation = 0;

	/*
	 * If this information won't fit in the file, or if we're a
	 * replication client environment and have been told to do so,
	 * swap files.
	 */
	if (lp->lsn.offset == 0 ||
		lp->lsn.offset + hdr->size + dbt->size > lp->log_size) {
		if (hdr->size + sizeof(LOGP) + dbt->size > lp->log_size) {
			__db_err(dbenv,
		"DB_ENV->log_put: record larger than maximum file size (%lu > %lu)",
				(u_long)hdr->size + sizeof(LOGP) + dbt->size,
				(u_long)lp->log_size);
			return (EINVAL);
		}

		if ((ret = __log_newfile(dblp, NULL)) != 0)
			return (ret);

		/*
		 * Flag that we switched files, in case we're a master
		 * and need to send this information to our clients.
		 * We postpone doing the actual send until we can
		 * safely release the log region lock and are doing so
		 * anyway.
		 */
		newfile = 1;
	}

	/*
	 * The offset into the log file at this point is the LSN where
	 * we're about to put this record, and is the LSN the caller wants.
	 */
	*lsn = lp->lsn;

	/* If we switched log files, let our caller know where. */
	if (newfile)
		*old_lsnp = old_lsn;

	pp = udbt->data;
	LOGCOPY_32(&rectype, pp);
	int utxnid_logged = normalize_rectype(&rectype);

	/* we have the log lsn value, can get context */
	if (off_context >= 0) {
		unsigned long long ltid = 0, *ltranid = &ltid;
		int pushlog = 1;

		assert(rectype == DB___txn_regop || rectype == DB___txn_regop_gen ||
				rectype == DB___txn_regop_rowlocks || rectype == DB___txn_dist_commit);

		if (rectype == DB___txn_regop_rowlocks)
		{
			/* rectype(4)+txn_num(4)+db_lsn(8)+txn_unum(8)+opcode(4)+LTRANID(8)+begin_lsn(8)+last_commit_lsn(8)+context(8)+timestamp(8)+lflags(4)+GENERATION(4) */
			ltranid = (unsigned long long *)(&pp[4 + 4 + 8 + (utxnid_logged ? 8 : 0) + 4]);
			LOGCOPY_32( &generation, &pp[4 + 4 + 8 + (utxnid_logged ? 8 : 0) + 4 + 8 + 8 + 8 + 8 + 8 + 4] );
			pushlog = (flags & DB_LOG_LOGICAL_COMMIT);
		}

		if (rectype == DB___txn_regop_gen)
		{
			/* rectype(4)+txn_num(4)+db_lsn(8)+txn_unum(8)+opcode(4)+GENERATION(4) */
			LOGCOPY_32( &generation, &pp[ 4 + 4 + 8 + (utxnid_logged ? 8 : 0) + 4] );
		}

		if (rectype == DB___txn_dist_commit)
		{
			/* rectype(4)+txn_num(4)+db_lsn(8)+GENERATION(4)*/
			LOGCOPY_32( &generation, &pp[ 4 + 4 + 8 + 4] );
		}

		bdb_push_pglogs_commit(dbenv->app_private, *lsn, generation, *ltranid, pushlog);

		ctx = get_commit_context(lsn, generation);

		if (context)
			*context = ctx;

		if (dbt->size < sizeof(unsigned long long))
			abort();

		memcpy((char *)dbt->data + off_context, &ctx,
			sizeof(unsigned long long));
		memcpy((char *)udbt->data + off_context, &ctx,
			sizeof(unsigned long long));

		if ((ret = __log_encrypt_record(dbenv, dbt, hdr, udbt->size)) != 0) {
			return ret;
		}
		if (CRYPTO_ON(dbenv))
			key = db_cipher->mac_key;
		else
			key = NULL;

		__db_chksum((unsigned char *)dbt->data, dbt->size, key,
				hdr->chksum);
	}

	/* Actually put the record. */
	return (__log_putr(dblp, lsn, dbt, lp->lsn.offset - lp->len, hdr));
}

/*
 * Return 1 if the in-memory buffer is empty, 0 if it is not.
 *
 * PUBLIC: int __inmemory_buf_empty __P((LOG *));
 */
int
__inmemory_buf_empty(LOG *lp)
{
	/*
	 * This will be called while holding the region lock, so b_off
	 * cannot be incremented.  We don't need to hold log_write_lk
	 * to check for equality because l_off cannot progress beyond
	 * b_off.
	 */
	if (lp->num_segments > 1) {
		return (lp->b_off == lp->l_off);
	} else {
		return (lp->b_off == 0);
	}
}

/*
 * Write everything up to the current segment to disk.  Called while holding
 * log_write_lk.  Caller must also hold the region lock if 'write_all' is lit.
 */
static int
__write_inmemory_buffer_lk(dblp, bytes_written, write_all)
	DB_LOG *dblp;
	u_int32_t *bytes_written;
	int write_all;
{
	LOG *lp;
	DB_ENV *dbenv;
	DB_LSN *seg_lsn_array;
	DB_LSN *seg_start_lsn_array;
	u_int32_t curseg, nxwseg, begseg;
	int ret;

	lp = dblp->reginfo.primary;

	/* Zero out bytes written. */
	if (bytes_written)
		*bytes_written = 0;

	/* Get writer's current segment. */
	curseg = (lp->b_off / lp->segment_size);

	/* Get the next-to-be-written segment. */
	nxwseg = (lp->l_off / lp->segment_size);

	/* Sanity check that l_off falls on a segment boundry. */
	assert(0 == lp->l_off % lp->segment_size);

	/* Write to the buffer end. */
	if (nxwseg > curseg) {
		/* Actually do the write. */
		if ((ret = __log_write(dblp, dblp->bufp + lp->l_off,
			    lp->buffer_size - lp->l_off)) != 0) {
			dbenv = dblp->dbenv;
			__db_err(dbenv,
			    "DB_ENV->log_write_td: error writing to log");
			ret = __db_panic(dbenv, ret);
			return -1;
		}

		/* Wrote from the nxwseg to the end. */
		if (bytes_written)
			*bytes_written += lp->buffer_size - lp->l_off;

		/* New nxwseg will be segment 0. */
		nxwseg = 0;

		/* New l_off is the first byte in the buffer. */
		lp->l_off = 0;
	}

	/* Write to the begining of the current segment. */
	if (nxwseg < curseg || (write_all && nxwseg == curseg)) {
		if (write_all) {
			/* Write if there's anything to write. */
			if (lp->b_off != lp->l_off)
				ret = __log_write(dblp, dblp->bufp + lp->l_off,
				    lp->b_off - lp->l_off);

			/* Update bytes_written. */
			if (bytes_written)
				*bytes_written += lp->b_off - lp->l_off;

			/* Reset both l_off and b_off. */
			lp->l_off = 0;
			lp->b_off = 0;

			/* Retrieve the precise segment lsn array. */
			seg_lsn_array =
			    R_ADDR(&dblp->reginfo, lp->segment_lsns_off);

			/* Retrieve the start segment lsn array. */
			seg_start_lsn_array = R_ADDR(&dblp->reginfo,
			    lp->segment_start_lsns_off);

			/* Shift the last written lsn. */
			if (curseg != 0) {
				seg_start_lsn_array[0] =
				    seg_start_lsn_array[curseg];
				seg_lsn_array[0] = seg_lsn_array[curseg];
			}

			/* Zero out the last LSN. */
			ZERO_LSN(seg_lsn_array[lp->num_segments - 1]);
		} else {
			/* Find the offset of the current segment. */
			begseg = (curseg * lp->segment_size);

			/* Write up to that point. */
			ret =
			    __log_write(dblp, dblp->bufp + lp->l_off,
			    begseg - lp->l_off);

			/* Update bytes written. */
			if (bytes_written)
				*bytes_written += begseg - lp->l_off;

			/* Update the last-written offset. */
			lp->l_off = begseg;
		}

		/* Panic if my write failed. */
		if (ret != 0) {
			dbenv = dblp->dbenv;
			__db_err(dbenv,
			    "DB_ENV->log_write_td: error writing to log");
			ret = __db_panic(dbenv, ret);
			return -1;
		}
	}
	return 0;
}

int
__write_inmemory_buffer(dblp, write_all)
	DB_LOG *dblp;
	int write_all;
{
	LOG *lp;
	int ret;

	lp = dblp->reginfo.primary;

	if (lp->num_segments > 1) {
		Pthread_mutex_lock(&log_write_lk);
		ret = __write_inmemory_buffer_lk(dblp, NULL, write_all);

		Pthread_mutex_unlock(&log_write_lk);
	} else {
		if ((ret = __log_write(dblp, dblp->bufp, (u_int32_t)lp->b_off)) == 0) {
			if (!gbl_wal_osync)
				ret = __os_fsync(dblp->dbenv, dblp->lfhp);
		}
	}
	return ret;
}


/*
 * __log_flush_commit --
 *	Flush a record.
 */
static int
__log_flush_commit(dbenv, lsnp, flags)
	DB_ENV *dbenv;
	const DB_LSN *lsnp;
	u_int32_t flags;
{
	DB_LOG *dblp;
	DB_LSN flush_lsn;
	LOG *lp;
	int ret;

	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;
	flush_lsn = *lsnp;

	ret = 0;

	/*
	 * DB_FLUSH:
	 *	Flush a record for which the DB_FLUSH flag to log_put was set.
	 *
	 * DB_LOG_WRNOSYNC:
	 *	If there's anything in the current log buffer, write it out.
	 */
	if (LF_ISSET(DB_FLUSH))
		ret = __log_flush_int(dblp, &flush_lsn, 1);
	else if (!__inmemory_buf_empty(lp)) {
		if ((ret = __write_inmemory_buffer(dblp, 1)) == 0)
			lp->b_off = 0;
	}

	/*
	 * If a flush supporting a transaction commit fails, we must abort the
	 * transaction.  (If we aren't doing a commit, return the failure; if
	 * if the commit we care about made it to disk successfully, we just
	 * ignore the failure, because there's no way to undo the commit.)
	 */
	if (ret == 0 || !LF_ISSET(DB_LOG_COMMIT))
		return (ret);

	if (flush_lsn.file != lp->lsn.file || flush_lsn.offset < lp->w_off)
		return (0);

	/*
	 * Else, make sure that the commit record does not get out after we
	 * abort the transaction.  Do this by overwriting the commit record
	 * in the buffer.  (Note that other commits in this buffer will wait
	 * wait until a sucessful write happens, we do not wake them.)  We
	 * point at the right part of the buffer and write an abort record
	 * over the commit.  We must then try and flush the buffer again,
	 * since the interesting part of the buffer may have actually made
	 * it out to disk before there was a failure, we can't know for sure.
	 */
	if (__txn_force_abort(dbenv,
	    dblp->bufp + flush_lsn.offset - lp->w_off) == 0)
		(void)__log_flush_int(dblp, &flush_lsn, 0);

	return (ret);
}

extern int wait_for_running_transactions(DB_ENV *);

/*
 * __log_newfile --
 *	Initialize and switch to a new log file.  (Note that this is
 * called both when no log yet exists and when we fill a log file.)
 *
 * PUBLIC: int __log_newfile __P((DB_LOG *, DB_LSN *));
 */
int
__log_newfile(dblp, lsnp)
	DB_LOG *dblp;
	DB_LSN *lsnp;
{
	DB_CIPHER *db_cipher;
	DB_ENV *dbenv;
	DB_LSN lsn;
	DBT t;
	HDR hdr;
	LOG *lp;
	int need_free, ret;
	u_int32_t lastoff;
	size_t tsize;
	u_int8_t *tmp;

	dbenv = dblp->dbenv;

	lp = dblp->reginfo.primary;

	/* If we're not at the beginning of a file already, start a new one. */
	if (lp->lsn.offset != 0) {
		/*
		 * NOTE: caller has the region lock (see also below comments)
		 * Flush the log so this file is out and can be closed.  We
		 * cannot release the region lock here because we need to
		 * protect the end of the file while we switch.  In
		 * particular, a thread with a smaller record than ours
		 * could detect that there is space in the log. Even
		 * blocking that event by declaring the file full would
		 * require all threads to wait here so that the lsn.file
		 * can be moved ahead after the flush completes.  This
		 * probably can be changed if we had an lsn for the
		 * previous file and one for the curent, but it does not
		 * seem like this would get much more throughput, if any.
		 */
		if ((ret = __log_flush_int(dblp, NULL, 0)) != 0)
			return (ret);

		DB_ASSERT(lp->b_off == 0);
		/*
		 * Save the last known offset from the previous file, we'll
		 * need it to initialize the persistent header information.
		 */
		lastoff = lp->lsn.offset;

		/* Point the current LSN to the new file. */
		++lp->lsn.file;
		lp->lsn.offset = 0;

		/* Reset the file write offset. */
		lp->w_off = 0;
	} else
		lastoff = 0;

	/*
	 * Insert persistent information as the first record in every file.
	 * Note that the previous length is wrong for the very first record
	 * of the log, but that's okay, we check for it during retrieval.
	 */
	DB_ASSERT(__inmemory_buf_empty(lp));

	memset(&t, 0, sizeof(t));
	memset(&hdr, 0, sizeof(HDR));

	need_free = 0;
	tsize = sizeof(LOGP);
	db_cipher = dbenv->crypto_handle;
	if (CRYPTO_ON(dbenv))
		tsize += db_cipher->adj_size(tsize);
	if ((ret = __os_calloc(dbenv, 1, tsize, &tmp)) != 0)
		return (ret);
	need_free = 1;

	lp->persist.log_size = lp->log_size = lp->log_nsize;
	memcpy(tmp, &lp->persist, sizeof(LOGP));

	t.data = tmp;
	t.size = (u_int32_t)tsize;

	if (LOG_SWAPPED())
		__log_persistswap((LOGP *)tmp);




	if ((ret =
		__log_encrypt_record(dbenv, &t, &hdr, (u_int32_t)tsize)) != 0)
		goto err;
	__db_chksum(t.data, t.size,
	    (CRYPTO_ON(dbenv)) ? db_cipher->mac_key : NULL, hdr.chksum);
	lsn = lp->lsn;
	if ((ret = __log_putr(dblp, &lsn,
		    &t, lastoff == 0 ? 0 : lastoff - lp->len, &hdr)) != 0)
		goto err;

	/* Update the LSN information returned to the caller. */
	if (lsnp != NULL)
		*lsnp = lp->lsn;

err:
	if (need_free)
		__os_free(dbenv, tmp);
	return (ret);
}

/*
 * __log_putr --
 *	Actually put a record into the log.
 */
static int
__log_putr(dblp, lsn, dbt, prev, h)
	DB_LOG *dblp;
	DB_LSN *lsn;
	const DBT *dbt;
	u_int32_t prev;
	HDR *h;
{
	DB_CIPHER *db_cipher;
	DB_ENV *dbenv;
	LOG *lp;
	DB_LSN tmplsn;
	HDR tmp, *hdr;
	int ret;
	size_t nr;

	dbenv = dblp->dbenv;
	lp = dblp->reginfo.primary;

	/*
	 * If we weren't given a header, use a local one.
	 */
	db_cipher = dbenv->crypto_handle;
	if (h == NULL) {
		hdr = &tmp;
		memset(hdr, 0, sizeof(HDR));
		if (CRYPTO_ON(dbenv))
			hdr->size = HDR_CRYPTO_SZ;
		else
			hdr->size = HDR_NORMAL_SZ;
	} else
		hdr = h;

	/* Panic if we fail. */

	/*
	 * Initialize the header.  If we just switched files, lsn.offset will
	 * be 0, and what we really want is the offset of the previous record
	 * in the previous file.  Fortunately, prev holds the value we want.
	 */
	hdr->prev = prev;
	hdr->len = (u_int32_t)hdr->size + dbt->size;

	/*
	 * If we were passed in a nonzero checksum, our caller calculated
	 * the checksum before acquiring the log mutex, as an optimization.
	 *
	 * If our caller calculated a real checksum of 0, we'll needlessly
	 * recalculate it.  C'est la vie;  there's no out-of-bounds value
	 * here.
	 */
	if (hdr->chksum[0] == 0)
		__db_chksum(dbt->data, dbt->size,
		    (CRYPTO_ON(dbenv)) ? db_cipher->mac_key : NULL,
		    hdr->chksum);

	nr = hdr->size;
	if (LOG_SWAPPED())
		__log_hdrswap(hdr, CRYPTO_ON(dbenv));

	/* Run segmented __log_fill if enabled. */
	if (lp->num_segments > 1) {
		tmplsn = *lsn;
		ret = __log_fill_segments(dblp, lsn, &tmplsn, hdr, nr);
		assert(tmplsn.offset == lsn->offset + nr);
	} else {
		ret = __log_fill(dblp, lsn, hdr, (u_int32_t)nr);
	}

	if (ret != 0)
		goto err;

	if (lp->num_segments > 1) {
		/* Sanity check. */
		ret =
		    __log_fill_segments(dblp, lsn, &tmplsn, dbt->data,
		    dbt->size);
		assert(tmplsn.offset == lsn->offset + nr + dbt->size);
	} else {
		ret = __log_fill(dblp, lsn, dbt->data, dbt->size);
	}

	if (ret != 0)
		goto err;

	lp->len = (u_int32_t)(nr + dbt->size);
	lp->lsn.offset += (u_int32_t)(nr + dbt->size);
	return (0);

err:
	/* 
	 * Panic if __log_fill returned non-0. 
	 */
	__db_err(dbenv, "Error writing the log-buffer");
	__db_panic(dbenv, ret);
	return (ret);
}

/*
 * __log_flush_pp --
 *	DB_ENV->log_flush pre/post processing.
 *
 * PUBLIC: int __log_flush_pp __P((DB_ENV *, const DB_LSN *));
 */
int
__log_flush_pp(dbenv, lsn)
	DB_ENV *dbenv;
	const DB_LSN *lsn;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lg_handle, "DB_ENV->log_flush", DB_INIT_LOG);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __log_flush(dbenv, lsn);
	if (rep_check)
		__env_rep_exit(dbenv);

	return (ret);
}

/*
 * __log_flush --
 *	DB_ENV->log_flush
 *
 * PUBLIC: int __log_flush __P((DB_ENV *, const DB_LSN *));
 */
int
__log_flush(dbenv, lsn)
	DB_ENV *dbenv;
	const DB_LSN *lsn;
{
	DB_LOG *dblp;
	int ret;

	dblp = dbenv->lg_handle;
	R_LOCK(dbenv, &dblp->reginfo);
	ret = __log_flush_int(dblp, lsn, 1);
	R_UNLOCK(dbenv, &dblp->reginfo);
	return (ret);
}

/* Called while we are holding the region lock. */
static inline DB_LSN
__log_lwr_lsn(dblp)
	DB_LOG *dblp;
{
	u_int32_t lwrseg, curseg;
	LOG *lp;
	DB_LSN *seg_start_lsn_array;

	lp = dblp->reginfo.primary;
	if (lp->num_segments > 1) {
		/*
		 * This uses the 'start' lsn array rather than the
		 * actual LSN array.
		 */
		seg_start_lsn_array =
		    R_ADDR(&dblp->reginfo, lp->segment_start_lsns_off);

		/*
		 * Acquire both the current segment and the last-write
		 * segment.
		 */
		curseg = (lp->b_off / lp->segment_size);
		lwrseg = (lp->l_off / lp->segment_size);

		/*
		 * This is an optimization: we are holding the region
		 * lock.  The last write segment cannot progress
		 * beyond the beginning of the current segment.  If
		 * these are the same, we can trust this offset
		 * without grabbing log_write_lk.
		 */
		if (curseg != lwrseg) {
			Pthread_mutex_lock(&log_write_lk);
			lwrseg = (lp->l_off / lp->segment_size);
			Pthread_mutex_unlock(&log_write_lk);
		}

		/* The seg_start_lsn_array is protected by the region lock. */
		return (seg_start_lsn_array[lwrseg]);
	} else {
		return (lp->f_lsn);
	}
}

/*
 * __log_flush_int --
 *	Write all records less than or equal to the specified LSN; internal
 *	version.
 *
 * PUBLIC: int __log_flush_int __P((DB_LOG *, const DB_LSN *, int));
 */
int
__log_flush_int(dblp, lsnp, release)
	DB_LOG *dblp;
	const DB_LSN *lsnp;
	int release;
{
	struct __db_commit *commit, *tcommit;

	/* COMDB2 MODIFICATION */
	/* The lp->commit list is getting corrupted on linux- try to detect this. */
	struct __db_commit *ck_corrupt = NULL;
	DB_ENV *dbenv;
	DB_LSN flush_lsn, f_lsn, s_lsn;
	DB_MUTEX *flush_mutexp;
	LOG *lp;
	u_int32_t ncommit, w_off, listcnt;
	int do_flush, first, ret, wrote_inmem;

	dbenv = dblp->dbenv;
	lp = dblp->reginfo.primary;
	flush_mutexp = R_ADDR(&dblp->reginfo, lp->flush_mutex_off);
	ncommit = 0;
	ret = 0;

	/*
	 * If no LSN specified, flush the entire log by setting the flush LSN
	 * to the last LSN written in the log.  Otherwise, check that the LSN
	 * isn't a non-existent record for the log.
	 */
	if (lsnp == NULL) {
		flush_lsn.file = lp->lsn.file;
		flush_lsn.offset = lp->lsn.offset - lp->len;
	} else if (lsnp->file > lp->lsn.file ||
	    (lsnp->file == lp->lsn.file &&
	    lsnp->offset > lp->lsn.offset - lp->len)) {
		__db_err(dbenv,
    "DB_ENV->log_flush: LSN of %lu/%lu past current end-of-log of %lu/%lu",
		    (u_long)lsnp->file, (u_long)lsnp->offset,
		    (u_long)lp->lsn.file, (u_long)lp->lsn.offset);
		__db_err(dbenv, "%s %s %s",
		    "Database environment corrupt; the wrong log files may",
		    "have been removed or incompatible database files imported",
		    "from another environment");
        /* Abort immediately */
        abort();
	} else {
		/*
		 * See if we need to wait.  s_lsn is not locked so some
		 * care is needed.  The sync point can only move forward.
		 * The lsnp->file cannot be greater than the s_lsn.file.
		 * If the file we want is in the past we are done.
		 * If the file numbers are the same check the offset.
		 * This all assumes we can read an integer in one
		 * state or the other, not in transition.
		 */
		if (lp->s_lsn.file > lsnp->file)
			return (0);

		if (lp->s_lsn.file == lsnp->file &&
		    lp->s_lsn.offset > lsnp->offset)
			return (0);

		flush_lsn = *lsnp;
	}

	/*
	 * Comdb2 modification: skip this logic if we are a replicant.
	 */
#ifndef TESTSUITE
	/*
	 * if (IS_REP_CLIENT(dbenv)) {
	 * release = 0;
	 * }
	 */
#endif

	/*
	 * If a flush is in progress and we're allowed to do so, drop
	 * the region lock and block waiting for the next flush.
	 */
	if (release && lp->in_flush != 0) {
		if ((commit =
			SH_TAILQ_FIRST(&lp->free_commits,
			    __db_commit)) == NULL) {
			if ((ret =
				__os_malloc(dbenv,
				    sizeof(struct __db_commit),
				    &commit)) != 0) {
				goto flush;
			}
			memset(commit, 0, sizeof(*commit));
			if ((ret = __db_mutex_setup(dbenv, &dblp->reginfo,
				    &commit->mutex,
				    MUTEX_SELF_BLOCK |MUTEX_NO_RLOCK)) != 0) {
				__os_free(dbenv, commit);
				return (ret);
			}
			MUTEX_LOCK(dbenv, &commit->mutex);
			/*
			 * COMDB2 MODIFICATION
			 * The nextpointer for this is getting corrupted on linux.  I'm 
			 * fencing the SH_TAILQ_ENTRY value to try to detect this.
			 */
			commit->fence1 = 0x12345678;
			commit->fence2 = 0x87654321;
		} else {
			SH_TAILQ_REMOVE(&lp->free_commits, commit, links,
			    __db_commit);
		}

		/* check fence */
		if (0x12345678 != commit->fence1 ||
		    0x87654321 != commit->fence2) {
			__db_err(dbenv,
			    "DB_ENV->log_flush fence value corruption");
			ret = __db_panic(dbenv, ret);
			return ret;
		}

		lp->ncommit++;

		/*
		 * Flushes may be requested out of LSN order;  be
		 * sure we only move lp->t_lsn forward.
		 */
		if (log_compare(&lp->t_lsn, &flush_lsn) < 0)
			lp->t_lsn = flush_lsn;

		commit->lsn = flush_lsn;
		SH_TAILQ_INSERT_HEAD(
		    &lp->commits, commit, links, __db_commit);
		R_UNLOCK(dbenv, &dblp->reginfo);
		/* Wait here for the in-progress flush to finish. */
		MUTEX_LOCK(dbenv, &commit->mutex);
		R_LOCK(dbenv, &dblp->reginfo);

		lp->ncommit--;
		/*
		 * Grab the flag before freeing the struct to see if
		 * we need to flush the log to commit.  If so,
		 * use the maximal lsn for any committing thread.
		 */
		do_flush = F_ISSET(commit, DB_COMMIT_FLUSH);
		F_CLR(commit, DB_COMMIT_FLUSH);
		SH_TAILQ_INSERT_HEAD(
		    &lp->free_commits, commit, links, __db_commit);
		if (do_flush) {
			lp->in_flush--;
			flush_lsn = lp->t_lsn;
		} else
			return (0);
	}

	/*
	 * Protect flushing with its own mutex so we can release
	 * the region lock except during file switches.
	 */
flush:	MUTEX_LOCK(dbenv, flush_mutexp);

	/*
	 * If the LSN is less than or equal to the last-sync'd LSN, we're done.
	 * Note, the last-sync LSN saved in s_lsn is the LSN of the first byte
	 * after the byte we absolutely know was written to disk, so the test
	 * is <, not <=.
	 */
	if (flush_lsn.file < lp->s_lsn.file ||
	    (flush_lsn.file == lp->s_lsn.file &&
	    flush_lsn.offset < lp->s_lsn.offset)) {
		MUTEX_UNLOCK(dbenv, flush_mutexp);
		goto done;
	}

	/*
	 * We may need to write the current buffer.  We have to write the
	 * current buffer if the flush LSN is greater than or equal to the
	 * buffer's starting LSN.
	 */
	f_lsn = __log_lwr_lsn( dblp );
	wrote_inmem = 0;
	if (!__inmemory_buf_empty(lp) && log_compare(&flush_lsn, &f_lsn) >= 0){
		ret = __write_inmemory_buffer( dblp, 1 );
		wrote_inmem = 1;

		if (ret != 0) {
			MUTEX_UNLOCK(dbenv, flush_mutexp);
			goto done;
		}

		/* 
		 * Setting b_off to 0 is redundant for the
		 * multi-segment case, but required for the
		 * single-segment case.
		 */
		lp->b_off = 0;

		/* Increment sync-write count */
		++lp->stat.st_swrites;
	}

    /* Has to be segmented switch file case: the last in-buffer write occurs at
     * the end of a segment.  Shift lsn & start-lsn down to segment 0.  This
     * prevents the segmented log-buffer from containing data from multiple logs
     * in the new-file case (which always calls flush).  */
    if (!wrote_inmem && lp->b_off && !release)
    {
        u_int32_t curseg;
        DB_LSN *seg_lsn_array;
        DB_LSN *seg_start_lsn_array;

        assert(lp->num_segments > 1 && lp->b_off == lp->l_off);
        curseg = (lp->b_off / lp->segment_size);
        seg_lsn_array = R_ADDR(&dblp->reginfo, lp->segment_lsns_off);
        seg_start_lsn_array = R_ADDR(&dblp->reginfo, lp->segment_start_lsns_off);
        seg_start_lsn_array[0] = seg_start_lsn_array[curseg];
        seg_lsn_array[0] = seg_lsn_array[curseg];
        ZERO_LSN( seg_lsn_array[lp->num_segments - 1] );
        lp->l_off = 0; lp->b_off = 0;
    }

	/*
	 * It's possible that this thread may never have written to this log
	 * file.  Acquire a file descriptor if we don't already have one.
	 */
	if (dblp->lfhp == NULL || dblp->lfname != lp->lsn.file)
		if ((ret = __log_newfh(dblp)) != 0) {
			MUTEX_UNLOCK(dbenv, flush_mutexp);
			goto done;
		}

	/*
	 * We are going to flush, release the region.
	 * First get the current state of the buffer since
	 * another write may come in, but we may not flush it.
	 */
	w_off = lp->w_off;
	f_lsn = lp->f_lsn;

	s_lsn = __log_lwr_lsn(dblp);
	lp->in_flush++;
	if (release)
		R_UNLOCK(dbenv, &dblp->reginfo);

	/* Sync all writes to disk. */
	if ((ret = __os_fsync(dbenv, dblp->lfhp)) != 0) {
		MUTEX_UNLOCK(dbenv, flush_mutexp);
		if (release)
			R_LOCK(dbenv, &dblp->reginfo);
		ret = __db_panic(dbenv, ret);
		return (ret);
	}

	/*
	 * Set the last-synced LSN.
	 * This value must be set to the LSN past the last complete
	 * record that has been flushed.  This is at least the first
	 * lsn, f_lsn.  If the buffer is empty, b_off == 0, then
	 * we can move up to write point since the first lsn is not
	 * set for the new buffer.
	 */
	lp->s_lsn = s_lsn;

	/*
	 * The s_lsn optimization doesn't work for the segmented buffer case
	 * as we could have written a non-fsync'd log record after the above
	 * fsync.
	 */
	if (1 == lp->num_segments && 0 == lp->b_off)
		lp->s_lsn.offset = w_off;

	MUTEX_UNLOCK(dbenv, flush_mutexp);
	if (release)
		R_LOCK(dbenv, &dblp->reginfo);

	lp->in_flush--;
	++lp->stat.st_scount;

	/*
	 * How many flush calls (usually commits) did this call actually sync?
	 * At least one, if it got here.
	 */
	ncommit = 1;
done:	listcnt = 0;
	if (lp->ncommit != 0) {
		first = 1;
		SH_TAILQ_FOREACH_SAFE(commit, &lp->commits, links, __db_commit,
		    tcommit) {
			/* 
			 * COMDB2 MODIFICATION
			 * try to detect waiter-list corruption
			 */
			int corrupt = 0;

			/* increment number of list elements */
			listcnt++;

			/* check if corrupt */
			if (listcnt > lp->ncommit) {
			__db_err(dbenv,
	    "DB_ENV->log_flush processed more than ncommit waiters?");
				corrupt = 1;
			}

			/* check commit record */
			if (ck_corrupt == commit) {
				__db_err(dbenv,
	    "DB_ENV->log_flush cycle in commit-waiter list");
				corrupt = 1;
			}

			/* set to current value */
			ck_corrupt = commit;

			/* check fence1 */
			if (0x12345678 != commit->fence1) {
				__db_err(dbenv,
	    "DB_ENV->log_flush fence1 value is corrupt");
				corrupt = 1;
			}

			/* check fence2 */
			if (0x87654321 != commit->fence2) {
				__db_err(dbenv,
	    "DB_ENV->log_flush fence2 value is corrupt");
				corrupt = 1;
			}

			/* panic if corrupt */
			if (corrupt) {
				__db_err(dbenv,
	    "DB_ENV->log_flush corruption detected in commit-waiter list");
				ret = __db_panic(dbenv, ret);
				return ret;
			}

			if (log_compare(&lp->s_lsn, &commit->lsn) > 0) {
				MUTEX_UNLOCK(dbenv, &commit->mutex);
				SH_TAILQ_REMOVE(&lp->commits, commit, links,
				    __db_commit);
				ncommit++;
			} else if (first == 1) {
				F_SET(commit, DB_COMMIT_FLUSH);
				MUTEX_UNLOCK(dbenv, &commit->mutex);
				SH_TAILQ_REMOVE(&lp->commits, commit, links,
				    __db_commit);
				/*
				 * This thread will wake and flush.
				 * If another thread commits and flushes
				 * first we will waste a trip trough the
				 * mutex.
				 */
				lp->in_flush++;
				first = 0;
			}
		}
	}
	if (lp->stat.st_maxcommitperflush < ncommit)
		lp->stat.st_maxcommitperflush = ncommit;
	if (lp->stat.st_mincommitperflush > ncommit ||
	    lp->stat.st_mincommitperflush == 0)
		lp->stat.st_mincommitperflush = ncommit;

	return (ret);
}

/* Wakeup when signaled and write the log-buffer to disk. */
static void *
__log_write_td(arg)
	void *arg;
{
	DB_LOG *dblp;
	LOG *lp;
	uint32_t bytes_written;

	dblp = (DB_LOG *)arg;
	lp = dblp->reginfo.primary;

	Pthread_mutex_lock(&log_write_lk);
	do {
		/* Block until there's more to write. */
		Pthread_cond_wait(&log_write_cond, &log_write_lk);

		/* Write whatever segments I can. */
		__write_inmemory_buffer_lk(dblp, &bytes_written, 0);

		/* Nothing written. */
		if (0 == bytes_written) {
			++lp->stat.st_false_wakeups;
		}

		/* Update max-bytes written counter. */
		if (bytes_written > lp->stat.st_max_td_written) {
			lp->stat.st_max_td_written = bytes_written;
		}

		/* Update total wakeups counter. */
		++lp->stat.st_total_wakeups;
	}
	while (!log_write_td_should_stop);

	Pthread_mutex_unlock(&log_write_lk);

	return NULL;
}


static void
__log_write_segments_init(void)
{
	int ret;
	if ((ret = pthread_create(&log_write_td, NULL, __log_write_td,
			   log_write_dblp)) != 0) {
		DB_ENV *dbenv = log_write_dblp->dbenv;
		__db_err(dbenv,
		    "DB_ENV->log_write_segments_init: error creating pthread");
		ret = __db_panic(dbenv, ret);
	}
}

/*
 * __log_fill_segments --
 * Multi-segmented version of __log_fill.
 */
static int
__log_fill_segments(dblp, startlsn, lsn, addr, len)
	DB_LOG *dblp;
	DB_LSN *startlsn;
	DB_LSN *lsn;
	void *addr;
	u_int32_t len;
{
	LOG *lp;
	DB_LSN *seg_lsn_array, *seg_start_lsn_array;
	u_int32_t curseg, segoff, nbufs;
	size_t copyamt, segspace, nxtseg;
	int ret;

	log_write_dblp = dblp;
	pthread_once(&log_write_once, __log_write_segments_init);

	lp = dblp->reginfo.primary;

	seg_lsn_array = R_ADDR(&dblp->reginfo, lp->segment_lsns_off);
	seg_start_lsn_array =
	    R_ADDR(&dblp->reginfo, lp->segment_start_lsns_off);

	while (len > 0) {

		/* Update f_lsn. */
		if (lp->b_off == 0)
			lp->f_lsn = *startlsn;

		/* Get my current segment. */
		curseg = (lp->b_off / lp->segment_size);

		/* Get the offset from the beginning of the segment. */
		segoff = (lp->b_off % lp->segment_size);

		/*
		 * If this is the beginning of a new segment, update
		 * the segment lsn array with the lsn of the first
		 * byte of this segment.  With segmented logfiles, the
		 * writer thread updates lp->f_lsn.
		 */
		if (0 == segoff) {
			/* The actual offset. */
			seg_lsn_array[curseg] = *lsn;

			/* The start of this log-record. */
			seg_start_lsn_array[curseg] = *startlsn;
		}

		/* 
		 * Write everything if this exceeds the size of the
		 * in-memory buffer.
		 */
		if (0 == segoff && len >= lp->buffer_size) {
			/* 
			 * Write in buffer-sized intervals.  Defer as
			 * much as possible for the log_write_td to
			 * handle.
			 */
			nbufs = len / lp->buffer_size;

			Pthread_mutex_lock(&log_write_lk);

			/* Flush the in-memory buffer. */
			__write_inmemory_buffer_lk(dblp, NULL, 0);

			/* Write this log. */
			if ((ret =
				__log_write(dblp, addr,
				    nbufs * lp->buffer_size)) != 0) {
				Pthread_mutex_unlock(&log_write_lk);
				return (ret);
			}

			/* Adjust current buffer. */
			addr = (u_int8_t *)addr + (nbufs * lp->buffer_size);

			/* Adjust current len. */
			len -= (nbufs * lp->buffer_size);

			/* Increment wcount. */
			++lp->stat.st_wcount_fill;

			/* Reset both l_off and b_off. */
			lp->l_off = 0;
			lp->b_off = 0;

			/* I no longer neeed the write lock. */
			Pthread_mutex_unlock(&log_write_lk);

			/* Zero out the last LSN. */
			ZERO_LSN(seg_lsn_array[lp->num_segments - 1]);

			/* Adjust lsn offset. */
			lsn->offset += (nbufs * lp->buffer_size);

			/* Set the precise lsn array. */
			seg_lsn_array[0] = *lsn;

			/* Set the start lsn array. */
			seg_start_lsn_array[0] = *startlsn;

			continue;
		}

		/* Space left in the current segment. */
		segspace = lp->segment_size - segoff;

		/* Determine the amount to copy. */
		copyamt = (segspace > len) ? len : segspace;

		/* Copy into the log-buffer. */
		memcpy(dblp->bufp + lp->b_off, addr, copyamt);

		/* Increment addr. */
		addr = (u_int8_t *)addr + copyamt;

		/* Increment lsn offset. */
		lsn->offset += copyamt;

		/* Decrement len. */
		len -= (u_int32_t)copyamt;

		/* Move to the next segment. */
		if (segspace == copyamt) {
			Pthread_cond_signal(&log_write_cond);

			nxtseg = (lp->b_off + copyamt) % lp->buffer_size;

			/* Can't proceed until this is written to disk. */
			if (lp->l_off == nxtseg) {
				Pthread_mutex_lock(&log_write_lk);

				/* Writer thread didn't flush: write inline. */
				if (lp->l_off == nxtseg) {
					__write_inmemory_buffer_lk(dblp, NULL,
					    0);
					++lp->stat.st_inline_writes;
				}

				Pthread_mutex_unlock(&log_write_lk);
			}

			assert(lp->l_off != nxtseg);
		}

		/* Increment b_off. */
		lp->b_off = (lp->b_off + copyamt) % lp->buffer_size;
	}
	return (0);
}

/*
 * __log_fill --
 *	Write information into the log.
 */
static int
__log_fill(dblp, lsn, addr, len)
	DB_LOG *dblp;
	DB_LSN *lsn;
	void *addr;
	u_int32_t len;
{
	LOG *lp;
	u_int32_t bsize, nrec;
	size_t nw, remain;
	int ret;
	off_t s_off;

	lp = dblp->reginfo.primary;

	bsize = lp->buffer_size;

	while (len > 0) {			/* Copy out the data. */
		/*
		 * If we're beginning a new buffer, note the user LSN to which
		 * the first byte of the buffer belongs.  We have to know this
		 * when flushing the buffer so that we know if the in-memory
		 * buffer needs to be flushed.
		 */
		if (lp->b_off == 0)
			lp->f_lsn = *lsn;

		/*
		 * If we're on a buffer boundary and the data is big enough,
		 * copy as many records as we can directly from the data.
		 */
		if (lp->b_off == 0 && len >= bsize) {
			nrec = len / bsize;
			s_off = lp->w_off;
			if ((ret = __log_write(dblp, addr, nrec * bsize)) != 0)
				return (ret);
			/* hint the file system to start writing out the file to disk. it's okay
			   to not wait for the write-out to complete, for log_put() does not guarantee
			   the log record will be sync'd to disk (log_flush() does). */
			__log_sync_range(dblp, s_off);
			addr = (u_int8_t *)addr + nrec * bsize;
			len -= nrec * bsize;
			++lp->stat.st_wcount_fill;
			continue;
		}

		/* Figure out how many bytes we can copy this time. */
		remain = bsize - lp->b_off;
		nw = remain > len ? len : remain;
		memcpy(dblp->bufp + lp->b_off, addr, nw);
		addr = (u_int8_t *)addr + nw;
		len -= (u_int32_t)nw;
		lp->b_off += nw;

		/* If we fill the buffer, flush it. */
		if (lp->b_off == bsize) {
			s_off = lp->w_off;
			if ((ret = __log_write(dblp, dblp->bufp, bsize)) != 0)
				return (ret);
			/* hint the file system to start writing out the file to disk. it's okay
			   to not wait for the write-out to complete, for log_put() does not guarantee
			   the log record will be sync'd to disk (log_flush() does). */
			__log_sync_range(dblp, s_off);
			lp->b_off = 0;
			++lp->stat.st_wcount_fill;
		}
	}
	return (0);
}

extern int gbl_wal_osync;
static void
__log_sync_range(dblp, off)
	DB_LOG *dblp;
	off_t off;
{
	/* Linux only: hint the OS that we're about to fsync the log file. */
#ifdef __linux__
	DB_ENV *dbenv;
	LOG *lp;
	dbenv = dblp->dbenv;
	lp = dblp->reginfo.primary;

	if (!gbl_wal_osync)
		sync_file_range(dblp->lfhp->fd, off, 0, SYNC_FILE_RANGE_WRITE);
#endif
}

/*
 * __log_write --
 *	Write the log buffer to disk.
 */
static int
__log_write(dblp, addr, len)
	DB_LOG *dblp;
	void *addr;
	u_int32_t len;
{
	DB_ENV *dbenv;
	LOG *lp;
	size_t nw;
	int ret;

	dbenv = dblp->dbenv;
	lp = dblp->reginfo.primary;

	/*
	 * If we haven't opened the log file yet or the current one
	 * has changed, acquire a new log file.
	 */
	if (dblp->lfhp == NULL || dblp->lfname != lp->lsn.file)
		if ((ret = __log_newfh(dblp)) != 0)
			return (ret);

	/*
	 * Seek to the offset in the file (someone may have written it
	 * since we last did).
	 */
	if ((ret = __os_seek(dbenv,
	    dblp->lfhp, 0, 0, lp->w_off, 0, DB_OS_SEEK_SET)) != 0 ||
	    (ret = __os_write(dbenv, dblp->lfhp, addr, len, &nw)) != 0)
		return (ret);

	/* Reset the buffer offset and update the seek offset. */
	lp->w_off += len;

	/* Update written statistics. */
	if ((lp->stat.st_w_bytes += len) >= MEGABYTE) {
		int mb = (lp->stat.st_w_bytes / MEGABYTE);

		lp->stat.st_w_mbytes += mb;
		lp->stat.st_w_bytes -= (mb * MEGABYTE);
		assert(lp->stat.st_w_bytes < MEGABYTE);
	}
	if ((lp->stat.st_wc_bytes += len) >= MEGABYTE) {
		int mb = (lp->stat.st_wc_bytes / MEGABYTE);

		lp->stat.st_wc_mbytes += mb;
		lp->stat.st_wc_bytes -= (mb * MEGABYTE);
		assert(lp->stat.st_wc_bytes < MEGABYTE);
	}
	++lp->stat.st_wcount;

	return (0);
}

/*
 * __log_file_pp --
 *	DB_ENV->log_file pre/post processing.
 *
 * PUBLIC: int __log_file_pp __P((DB_ENV *, const DB_LSN *, char *, size_t));
 */
int
__log_file_pp(dbenv, lsn, namep, len)
	DB_ENV *dbenv;
	const DB_LSN *lsn;
	char *namep;
	size_t len;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->lg_handle, "DB_ENV->log_file", DB_INIT_LOG);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __log_file(dbenv, lsn, namep, len);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __log_file --
 *	DB_ENV->log_file.
 */
static int
__log_file(dbenv, lsn, namep, len)
	DB_ENV *dbenv;
	const DB_LSN *lsn;
	char *namep;
	size_t len;
{
	DB_LOG *dblp;
	int ret;
	char *name;

	dblp = dbenv->lg_handle;
	R_LOCK(dbenv, &dblp->reginfo);
	ret = __log_name(dblp, lsn->file, &name, NULL, 0);
	R_UNLOCK(dbenv, &dblp->reginfo);
	if (ret != 0)
		return (ret);

	/* Check to make sure there's enough room and copy the name. */
	if (len < strlen(name) + 1) {
		*namep = '\0';
		__db_err(dbenv, "DB_ENV->log_file: name buffer is too short");
		return (EINVAL);
	}
	(void)strcpy(namep, name);
	__os_free(dbenv, name);

	return (0);
}

/*
 * __log_newfh --
 *	Acquire a file handle for the current log file.
 */
static int
__log_newfh(dblp)
	DB_LOG *dblp;
{
	DB_ENV *dbenv;
	LOG *lp;
	u_int32_t flags;
	int ret;
	logfile_validity status;

	dbenv = dblp->dbenv;
	lp = dblp->reginfo.primary;

	/* Close any previous file descriptor. */
	if (dblp->lfhp != NULL) {
		(void)__os_closehandle(dbenv, dblp->lfhp);
		dblp->lfhp = NULL;
	}

	/*
	 * Adding DB_OSO_LOG to the flags may add additional platform-specific
	 * optimizations.  On WinNT, the logfile is preallocated, which may
	 * have a time penalty at startup, but have better overall throughput.
	 * We are not certain that this works reliably, so enable at your own
	 * risk.
	 */
	flags = DB_OSO_CREATE |DB_OSO_SEQ |
	    (F_ISSET(dbenv, DB_ENV_DIRECT_LOG) ? DB_OSO_DIRECT : 0);
	if (F_ISSET(dbenv, DB_ENV_OSYNC))
		flags |= DB_OSO_OSYNC;

	LF_SET(DB_OSO_LOG);

	/* Get the path of the new file and open it. */
	dblp->lfname = lp->lsn.file;
	if ((ret = __log_valid(dblp, dblp->lfname, 0, &dblp->lfhp,
	    flags, &status)) != 0)
		__db_err(dbenv,
		    "DB_ENV->log_put: %d: %s", lp->lsn.file, db_strerror(ret));
	else if (status != DB_LV_NORMAL && status != DB_LV_INCOMPLETE)
		ret = DB_NOTFOUND;

	return (ret);
}

/*
 * __log_name --
 *	Return the log name for a particular file, and optionally open it.
 *
 * PUBLIC: int __log_name __P((DB_LOG *,
 * PUBLIC:     u_int32_t, char **, DB_FH **, u_int32_t));
 */
int
__log_name(dblp, filenumber, namep, fhpp, flags)
	DB_LOG *dblp;
	u_int32_t filenumber, flags;
	char **namep;
	DB_FH **fhpp;
{
	DB_ENV *dbenv;
	LOG *lp;
	int ret;
	char *oname;
	char old[sizeof(LFPREFIX) + 5 + 20], new[sizeof(LFPREFIX) + 10 + 20];

    flags |= DB_OSO_LOG;

	dbenv = dblp->dbenv;
	lp = dblp->reginfo.primary;

	/*
	 * !!!
	 * The semantics of this routine are bizarre.
	 *
	 * The reason for all of this is that we need a place where we can
	 * intercept requests for log files, and, if appropriate, check for
	 * both the old-style and new-style log file names.  The trick is
	 * that all callers of this routine that are opening the log file
	 * read-only want to use an old-style file name if they can't find
	 * a match using a new-style name.  The only down-side is that some
	 * callers may check for the old-style when they really don't need
	 * to, but that shouldn't mess up anything, and we only check for
	 * the old-style name when we've already failed to find a new-style
	 * one.
	 *
	 * Create a new-style file name, and if we're not going to open the
	 * file, return regardless.
	 */
	(void)snprintf(new, sizeof(new), LFNAME, filenumber);
	if ((ret = __db_appname(dbenv,
	    DB_APP_LOG, new, 0, NULL, namep)) != 0 || fhpp == NULL)
		return (ret);

	/* Open the new-style file -- if we succeed, we're done. */
	if ((ret = __os_open_extend(dbenv,
	    *namep, lp->log_size, 0, flags, lp->persist.mode, fhpp)) == 0)
	{
		return (0);
	}

	/*
	 * The open failed... if the DB_RDONLY flag isn't set, we're done,
	 * the caller isn't interested in old-style files.
	 */
	if (!LF_ISSET(DB_OSO_RDONLY)) {
		__db_err(dbenv,
		    "%s: log file open failed: %s", *namep, db_strerror(ret));
		return (__db_panic(dbenv, ret));
	}

	/* Create an old-style file name. */
	(void)snprintf(old, sizeof(old), LFNAME_V1, filenumber);
	if ((ret = __db_appname(dbenv, DB_APP_LOG, old, 0, NULL, &oname)) != 0)
		goto err;

	/*
	 * Open the old-style file -- if we succeed, we're done.  Free the
	 * space allocated for the new-style name and return the old-style
	 * name to the caller.
	 */
	if ((ret =
	    __os_open(dbenv, oname, flags, lp->persist.mode, fhpp)) == 0) {
		__os_free(dbenv, *namep);
		*namep = oname;
		return (0);
	}

	/*
	 * Couldn't find either style of name -- return the new-style name
	 * for the caller's error message.  If it's an old-style name that's
	 * actually missing we're going to confuse the user with the error
	 * message, but that implies that not only were we looking for an
	 * old-style name, but we expected it to exist and we weren't just
	 * looking for any log file.  That's not a likely error.
	 */
err:	__os_free(dbenv, oname);
	return (ret);
}

/*
 * __log_rep_put --
 *	Short-circuit way for replication clients to put records into the
 * log.  Replication clients' logs need to be laid out exactly their masters'
 * are, so we let replication take responsibility for when the log gets
 * flushed, when log switches files, etc.  This is just a thin PUBLIC wrapper
 * for __log_putr with a slightly prettier interface.
 *
 * Note that the db_rep->db_mutexp should be held when this is called.
 * Note that we acquire the log region lock while holding db_mutexp.
 *
 * PUBLIC: int __log_rep_put __P((DB_ENV *, DB_LSN *, const DBT *));
 */
int
__log_rep_put(dbenv, lsnp, rec)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	const DBT *rec;
{
	DB_CIPHER *db_cipher;
	DB_LOG *dblp;
	HDR hdr;
	DBT *dbt, t;
	LOG *lp;
	int need_free, ret;

	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;

	R_LOCK(dbenv, &dblp->reginfo);
	memset(&hdr, 0, sizeof(HDR));
	t = *rec;
	dbt = &t;
	need_free = 0;
	db_cipher = (DB_CIPHER *)dbenv->crypto_handle;
	if (CRYPTO_ON(dbenv))
		t.size += db_cipher->adj_size(rec->size);

	if (t.size > 4096) {
		if ((ret = __os_calloc(dbenv, 1, t.size, &t.data)) != 0)
			goto err;
		need_free = 1;
	} else
		t.data = alloca(t.size);

	memcpy(t.data, rec->data, rec->size);

	if ((ret = __log_encrypt_record(dbenv, dbt, &hdr, rec->size)) != 0)
		goto err;
	__db_chksum(t.data, t.size,
	    (CRYPTO_ON(dbenv)) ? db_cipher->mac_key : NULL, hdr.chksum);

	DB_ASSERT(log_compare(lsnp, &lp->lsn) == 0);
	ret = __log_putr(dblp, lsnp, dbt, lp->lsn.offset - lp->len, &hdr);

        /* Physical replication:

           In a 'physical replication cluster', it is the leader node's job to
           read changes from the source, apply them locally and also transmit
           the changes to other nodes in the cluster. The following code
           enables the replication of logs to other nodes in the cluster.
        */
	if (IS_REP_MASTER(dbenv)) {
                assert(gbl_physrep_source_dbname != NULL);
		/*
		 * If we are not a rep application, but are sharing a
		 * master rep env, we should not be writing log records.
		 */
		if (dbenv->rep_send == NULL) {
			__db_err(dbenv, "%s %s",
			    "Non-replication DB_ENV handle attempting",
			    "to modify a replicated environment");
			ret = EINVAL;
			goto err;
		}

                /* There is no need to send REP_NEWFILE here (unlike done in
                 * __log_put_int_int()) as the physrep worker thread running
                 * on this (master) node automatically detects and sends
                 * REP_NEWFILE on log file change.
                 */

		/*
		 * Then send the log record itself on to our clients.
		 *
		 * If the send fails and we're a commit or checkpoint,
		 * there's nothing we can do;  the record's in the log.
		 * Flush it, even if we're running with TXN_NOSYNC, on the
		 * grounds that it should be in durable form somewhere.
		 */
		/*
		 * !!!
		 * In the crypto case, we MUST send the udbt, not the
		 * now-encrypted dbt.  Clients have no way to decrypt
		 * without the header.
		 */
		/* COMDB2 MODIFICATION
		 *
		 * We want to be able to throttle log propagation to
		 * avoid filling the net queue; this will allow signal
		 * messages and catching up log replies to be
		 * transferred even though the database is under heavy
		 * load.  Problem is, in berkdb_send_rtn both regular
		 * log messages and catching up log replies are coming
		 * as REP_LOG In __log_push we replace REP_LOG with
		 * REP_LOG_LOGPUT so we know that this must be
		 * throttled; we revert to REP_LOG in the same routine
		 * before sending the message
		 */
                DB_LSN lsn;
                lsn = *lsnp;

                int flags = 0;
                u_int32_t rectype = 0;

                LOGCOPY_32(&rectype, rec->data);
                normalize_rectype(&rectype);
                if (is_commit_record(rectype)) {
                    flags = DB_REP_FLUSH|DB_LOG_PERM;
                }

		if ((__rep_send_message(dbenv,
			    db_eid_broadcast, REP_LOG, &lsn, rec, flags,
			    NULL) != 0) && LF_ISSET(DB_LOG_PERM))
			 LF_SET(DB_FLUSH);
	}

err:
	/*
	 * !!! Assume caller holds db_rep->db_mutex to modify ready_lsn.
	 */
	lp->ready_lsn = lp->lsn;
/*
        fprintf(stderr, "Setting readylsn file %s line %d to %d:%d\n", __FILE__, 
                __LINE__, lp->ready_lsn.file, lp->ready_lsn.offset);
*/
	R_UNLOCK(dbenv, &dblp->reginfo);
	if (need_free)
		__os_free(dbenv, t.data);
	return (ret);
}

static int
__log_encrypt_record(dbenv, dbt, hdr, orig)
	DB_ENV *dbenv;
	DBT *dbt;
	HDR *hdr;
	u_int32_t orig;
{
	DB_CIPHER *db_cipher;
	int ret;

	if (CRYPTO_ON(dbenv)) {
		db_cipher = (DB_CIPHER *)dbenv->crypto_handle;
		hdr->size = HDR_CRYPTO_SZ;
		hdr->orig_size = orig;
		if ((ret = db_cipher->encrypt(dbenv, db_cipher->data,
		    hdr->iv, dbt->data, dbt->size)) != 0)
			return (ret);
	} else {
		hdr->size = HDR_NORMAL_SZ;
	}
	return (0);
}
