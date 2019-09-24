/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
#ifndef lint
static const char revid[] = "$Id: mp_sync.c,v 11.80 2003/09/13 19:20:41 bostic Exp $";
#endif /* not lint */

#include "db_config.h"
#include "db_int.h"
#include "dbinc/db_shash.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/db_swap.h"
#include "dbinc/hmac.h"
#include "dbinc_auto/hmac_ext.h"
#include "dbinc/txn.h"

#include <sys/types.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <poll.h>
#include <limits.h>

#include "thdpool.h"
#include <ctrace.h>
#include <pool.h>
#include <logmsg.h>
#include <locks_wrap.h>

typedef struct {
	DB_MPOOL_HASH *track_hp;	/* Hash bucket. */

	roff_t	  track_off;		/* Page file offset. */
	db_pgno_t track_pgno;		/* Page number. */
	u_int32_t track_prio;		/* Priority. */
	DB_LSN    track_tx_begin_lsn;	/* first dirty txn begin LSN. */
} BH_TRACK;

static int __bhcmp __P((const void *, const void *));
static int __bhlru __P((const void *, const void *));
static int __memp_close_flush_files __P((DB_ENV *, DB_MPOOL *));
static int __memp_sync_files __P((DB_ENV *, DB_MPOOL *));

extern void *gbl_bdb_state;
void bdb_get_writelock(void *bdb_state,
    const char *idstr, const char *funcname, int line);
void bdb_rellock(void *bdb_state, const char *funcname, int line);
void bdb_get_readlock(void *bdb_state,
    const char *idstr, const char *funcname, int line);
int bdb_the_lock_desired(void);

#define BDB_WRITELOCK(idstr)    bdb_get_writelock(gbl_bdb_state, (idstr), __func__, __LINE__)
#define BDB_READLOCK(idstr)     bdb_get_readlock(gbl_bdb_state, (idstr), __func__, __LINE__)
#define BDB_RELLOCK()           bdb_rellock(gbl_bdb_state, __func__, __LINE__)
int bdb_the_lock_designed(void);

/*
 * __memp_sync_pp --
 *	DB_ENV->memp_sync pre/post processing.
 *
 * PUBLIC: int __memp_sync_pp __P((DB_ENV *, DB_LSN *));
 */
int
__memp_sync_pp(dbenv, lsnp)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
	    dbenv->mp_handle, "memp_sync", DB_INIT_MPOOL);

	/*
	 * If no LSN is provided, flush the entire cache (reasonable usage
	 * even if there's no log subsystem configured).
	 */
	if (lsnp != NULL)
		ENV_REQUIRES_CONFIG(dbenv,
		    dbenv->lg_handle, "memp_sync", DB_INIT_LOG);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	BDB_READLOCK("__memp_sync");
	if (rep_check)
		__env_rep_enter(dbenv);
	do {
		ret = __memp_sync_restartable(dbenv, lsnp, 1, 1);
		if (ret == DB_LOCK_DESIRED) {
			if (rep_check)
				__env_rep_exit(dbenv);
			BDB_RELLOCK();
			__os_sleep(dbenv, 1, 0);
			BDB_READLOCK("__memp_sync");
			if (rep_check)
				__env_rep_enter(dbenv);
		}
	} while (ret == DB_LOCK_DESIRED);
	if (rep_check)
		__env_rep_exit(dbenv);
	BDB_RELLOCK();
	return (ret);
}

static pthread_mutex_t mempsync_lk;
static pthread_cond_t mempsync_wait;
static pthread_once_t mempsync_once = PTHREAD_ONCE_INIT;
static pthread_t mempsync_tid = 0;
static int mempsync_thread_running = 0;
static int mempsync_thread_should_stop = 0;
static DB_LSN mempsync_lsn;

int __os_io(DB_ENV *dbenv, int op, DB_FH * fhp, db_pgno_t pgno, size_t pagesize,
    u_int8_t *buf, size_t * niop);

int gbl_checkpoint_paranoid_verify = 0;

int
__checkpoint_verify(DB_ENV *dbenv)
{
	DB_LSN lsn;
	int rc;

	if (!gbl_checkpoint_paranoid_verify)
		return 0;

	if (!dbenv->checkpoint)
		return 0;

	/* don't do this when we are exiting */
	if (!dbenv->lg_handle)
		return 0;

	if (IS_RECOVERING(dbenv))
		return 0;

	if ((rc = __checkpoint_get(dbenv, &lsn)) != 0) {
		__db_err(dbenv, "can't get checkpoint");
		__db_panic(dbenv, EINVAL);
	}
	/*
	 * This happens when we're in rep_start in initcomdb2 and have
	 * not yet written a checkpoint record (because we don't yet
	 * have logs)
	 */
	if (lsn.file == 0 && lsn.offset == 0)
		return 0;
	if ((rc = berkdb_verify_lsn_written_to_disk(dbenv, &lsn, 1)) != 0) {
		__db_err(dbenv, "can't verify checkpoint");
		__db_panic(dbenv, EINVAL);
	}
	return 0;
}

int
__checkpoint_save(DB_ENV *dbenv, DB_LSN *lsn, int in_recovery)
{
	struct __db_checkpoint ckpt = {{0}};
	int rc;
	size_t niop = 0;

	logmsg(LOGMSG_DEBUG, "ckpt lsn: %u:%u\n", lsn->file, lsn->offset);

	LOGCOPY_TOLSN(&ckpt.lsn, lsn);

	/* 2 or more.  Better learn to live with 1!
	 * We need to keep 2 checkpoints, since the master may truncate a replicant's log to a
	 * point before the last checkpoint.  So before we write the current checkpoint, get the
	 * last one, and remember it. */

	if ((rc = __checkpoint_get(dbenv, NULL))) {
		__db_err(dbenv,
		    "__checkpoint_save, couldn't fetch last checkpoint rc %d\n",
		    rc);
		return EINVAL;
	}

	__db_chksum_no_crypto((u_int8_t *)&ckpt.lsn,
	    sizeof(struct __db_checkpoint) - offsetof(struct __db_checkpoint,
		lsn), ckpt.chksum);

	if ((lsn->file != 0 && lsn->offset != 0) && berkdb_verify_lsn_written_to_disk(dbenv, lsn, !in_recovery	/* If we're in recovery, don't verify the checkpoint.  It's correct
														 * because otherwise we wouldn't be able to recover. */ )) {
		__db_err(dbenv,
		    "in __checkpoint_save, but couldn't read %u:%u\n",
		    lsn->file, lsn->offset);
		abort();
	}

	rc = __os_io(dbenv, DB_IO_WRITE, dbenv->checkpoint, 0, 512,
	    (u_int8_t *)&ckpt, &niop);
	if (rc) {
		/* This isn't critical - just means it'll take longer to recover. */
		__db_err(dbenv, "can't write checkpoint record rc %d %s\n", rc,
		    strerror(rc));
		return EINVAL;
	}

	return 0;
}

/* TODO: violating many many rules here.  This code should move into bdb. */
extern void bdb_set_key(void *);
void bdb_thread_event(void *bdb_state, int event);

enum {
	BDBTHR_EVENT_DONE_RDONLY = 0,
	BDBTHR_EVENT_START_RDONLY = 1,
	BDBTHR_EVENT_DONE_RDWR = 2,
	BDBTHR_EVENT_START_RDWR = 3
};

void *
mempsync_thd(void *p)
{
	int rc;
	DB_LOGC *logc;
	void *bdb_state;
	DBT data_dbt;
	DB_LSN lsn, sync_lsn;
	DB_ENV *dbenv = p;
	int rep_check = 0;

	/* slightly kludgy, but necessary since this code gets
	 * called in recovery */
	bdb_state = dbenv->app_private;
	bdb_set_key(bdb_state);
	bdb_thread_event(bdb_state, BDBTHR_EVENT_START_RDONLY);

	rep_check = IS_ENV_REPLICATED(dbenv);

	mempsync_thread_running = 1;
	while (!mempsync_thread_should_stop) {
		Pthread_mutex_lock(&mempsync_lk);
		Pthread_cond_wait(&mempsync_wait, &mempsync_lk);
		if (mempsync_thread_should_stop) {
			Pthread_mutex_unlock(&mempsync_lk);
			break;
		}
		/* latch the lsn */
		Pthread_mutex_unlock(&mempsync_lk);
		BDB_READLOCK("mempsync_thd");
		Pthread_mutex_lock(&mempsync_lk);
		lsn = mempsync_lsn;
		sync_lsn = lsn;
		Pthread_mutex_unlock(&mempsync_lk);
		BDB_RELLOCK();

		/*
		 * When we do parallel recovery, there are "commit" records
		 * written before a transaction has committed.  So before
		 * doing a checkpoint, make sure the LSN we've been handed
		 * is older than the newest commited transaction.
		 */
		while (__rep_inflight_txns_older_than_lsn(dbenv, &lsn)) {
			logmsg(LOGMSG_WARN, "have transactions in flight older than %u:%u, deferring checkpoint\n",
			    lsn.file, lsn.offset);
			__os_sleep(dbenv, 1, 0);
		}

		BDB_READLOCK("mempsync_thd");
		if (rep_check)
			__env_rep_enter(dbenv);
		do {
			/* We are replicant. So use whatever LSN master tells us
			   and do not attempt to change it. */
			rc = __memp_sync_restartable(dbenv, &lsn, 1, 1);
			if (rep_check)
				__env_rep_exit(dbenv);
			BDB_RELLOCK();
			if (rc == DB_LOCK_DESIRED) {
				__os_sleep(dbenv, 1, 0);
				BDB_READLOCK("mempsync_thd");
				if (rep_check)
					__env_rep_enter(dbenv);
			}
		} while (rc == DB_LOCK_DESIRED);
		if (rc == 0) {
#if 0
			DB_LSN ckp;
			DB_LOG_STAT *st;

			rc = __txn_getckp(dbenv, &ckp);
			if (rc == 0)
				__log_stat_pp(dbenv, &st, 0);
			printf
			    ("wanted sync to %u:%u mempsync wrote up to %u:%u, checkpoint at %u:%u, log at %u:%u, disk at %u:%u\n",
			    sync_lsn.file, sync_lsn.offset, lsn.file,
			    lsn.offset, ckp.file, ckp.offset, st->st_cur_file,
			    st->st_cur_offset, st->st_disk_file,
			    st->st_disk_offset);
			free(st);
#endif
			/* For the reference:  sync_lsn is the lsn of the txn_ckp in the log,
			 * as provided by mempsync_sync_out_of_band(); 
			 * Not sure if it safe to sync beyond ckp->ckp_lsn all the way to ckp lsn though */
            BDB_READLOCK("mempsync_thd_ckp");
            rc = __log_flush_pp(dbenv, NULL);
            if ((rc = __log_cursor(dbenv, &logc)) != 0)
                goto err;

            memset(&data_dbt, 0, sizeof(data_dbt));
            data_dbt.flags = DB_DBT_REALLOC;

            if ((rc = __log_c_get(logc, &sync_lsn, &data_dbt, DB_SET)) == 0) {
                __txn_updateckp(dbenv, &sync_lsn);
                __os_free(dbenv, data_dbt.data);
            }
            __log_c_close(logc);
            BDB_RELLOCK();

		} else {
err:
			logmsg(LOGMSG_ERROR, "__memp_sync rc %d\n", rc);
		}
	}

	bdb_thread_event(bdb_state, BDBTHR_EVENT_DONE_RDONLY);

	mempsync_thread_running = 0;

	return NULL;
}

/* kludge - should have one per environment instead of global */
static DB_ENV *gblenv;
void
mempsync_out_of_band_init(void)
{
	int rc;
	Pthread_mutex_init(&mempsync_lk, NULL);
	Pthread_cond_init(&mempsync_wait, NULL);

	rc = pthread_create(&mempsync_tid, NULL, mempsync_thd, gblenv);
	if (rc) {
		logmsg(LOGMSG_FATAL, "pthread_create rc %d %s\n", rc, strerror(rc));
		abort();
	}
}

// PUBLIC: int __memp_sync_out_of_band __P((DB_ENV *, DB_LSN *));
int
__memp_sync_out_of_band(DB_ENV *dbenv, DB_LSN *lsn)
{
	gblenv = dbenv;
	pthread_once(&mempsync_once, mempsync_out_of_band_init);

	Pthread_mutex_lock(&mempsync_lk);
	mempsync_lsn = *lsn;
	Pthread_cond_signal(&mempsync_wait);
	Pthread_mutex_unlock(&mempsync_lk);

	return 0;
}

/*
 * __memp_sync_restartable --
 *  If lsnp is NUL or perfect checkpoints is disabled,
 *  the function guarantees that every dirty page whose LSN
 *  is less than *lsnp is written to disk, upon success.
 *  Otherwise, the function uses *lsnp (or as a hint,
 *  if `fixed' is false) to determine the checkpoint LSN
 *  which requires the least amount of I/O. Hence it is not
 *  guaranteed every dirty page with LSN < *lsnp is written to disk.
 *
 * PUBLIC: int __memp_sync_restartable
 * PUBLIC:     __P((DB_ENV *, DB_LSN *, int, int));
 */
int
__memp_sync_restartable(dbenv, lsnp, restartable, fixed)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	int restartable;
	int fixed;
{
	DB_MPOOL *dbmp;
	MPOOL *mp;
	int ret;

	dbmp = dbenv->mp_handle;
	mp = dbmp->reginfo[0].primary;

	/* If we've flushed to the requested LSN, return that information. */
	if (lsnp != NULL) {
		R_LOCK(dbenv, dbmp->reginfo);
		if (log_compare(lsnp, &mp->lsn) <= 0) {
			*lsnp = mp->lsn;

			R_UNLOCK(dbenv, dbmp->reginfo);
			return (0);
		}
		R_UNLOCK(dbenv, dbmp->reginfo);
	}

	if ((ret =
	    __memp_sync_int(dbenv, NULL, 0, DB_SYNC_CACHE, NULL,
	    restartable, (dbenv->tx_perfect_ckp ? lsnp : NULL), fixed)) != 0)
		 return (ret);

	if (lsnp != NULL) {
		R_LOCK(dbenv, dbmp->reginfo);
		if (log_compare(lsnp, &mp->lsn) > 0)
			mp->lsn = *lsnp;
		R_UNLOCK(dbenv, dbmp->reginfo);
	}

	return (0);
}


/*
 * __memp_sync --
 *	DB_ENV->memp_sync.
 *
 * PUBLIC: int __memp_sync __P((DB_ENV *, DB_LSN *));
 */
int
__memp_sync(dbenv, lsnp)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
{
	/* Implementation note:
	   To avoid confusion, we keep the original behavior of memp_sync():
	   Flush everything in MPool so that we know for certain that
	   dirty pages with LSN less than lsnp are written to disk. */
	return __memp_sync_restartable(dbenv, lsnp, 0, 1);
}

/*
 * __memp_fsync_pp --
 *	DB_MPOOLFILE->sync pre/post processing.
 *
 * PUBLIC: int __memp_fsync_pp __P((DB_MPOOLFILE *));
 */
int
__memp_fsync_pp(dbmfp)
	DB_MPOOLFILE *dbmfp;
{
	DB_ENV *dbenv;
	int rep_check, ret;

	dbenv = dbmfp->dbenv;

	PANIC_CHECK(dbenv);
	MPF_ILLEGAL_BEFORE_OPEN(dbmfp, "DB_MPOOLFILE->sync");

	if ((rep_check = IS_ENV_REPLICATED(dbenv)) != 0)
		__env_rep_enter(dbenv);
	ret = __memp_fsync(dbmfp);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __memp_fsync --
 *	DB_MPOOLFILE->sync.
 *
 * PUBLIC: int __memp_fsync __P((DB_MPOOLFILE *));
 */
int
__memp_fsync(dbmfp)
	DB_MPOOLFILE *dbmfp;
{
	/*
	 * If this handle doesn't have a file descriptor that's open for
	 * writing, or if the file is a temporary, there's no reason to
	 * proceed further.
	 */
	if (F_ISSET(dbmfp, MP_READONLY))
		return (0);

	if (F_ISSET(dbmfp->mfp, MP_TEMP))
		return (0);

	return (__memp_sync_int(dbmfp->dbenv,
			                dbmfp, 0, DB_SYNC_FILE, NULL, 0, NULL, 0));
}

/*
 * __mp_xxx_fh --
 *	Return a file descriptor for DB 1.85 compatibility locking.
 *
 * PUBLIC: int __mp_xxx_fh __P((DB_MPOOLFILE *, DB_FH **));
 */
int
__mp_xxx_fh(dbmfp, fhp)
	DB_MPOOLFILE *dbmfp;
	DB_FH **fhp;
{
	/*
	 * This is a truly spectacular layering violation, intended ONLY to
	 * support compatibility for the DB 1.85 DB->fd call.
	 *
	 * Sync the database file to disk, creating the file as necessary.
	 *
	 * We skip the MP_READONLY and MP_TEMP tests done by memp_fsync(3).
	 * The MP_READONLY test isn't interesting because we will either
	 * already have a file descriptor (we opened the database file for
	 * reading) or we aren't readonly (we created the database which
	 * requires write privileges).  The MP_TEMP test isn't interesting
	 * because we want to write to the backing file regardless so that
	 * we get a file descriptor to return.
	 */
	if ((*fhp = dbmfp->fhp) != NULL)
		return (0);

	return (__memp_sync_int(dbmfp->dbenv,
			                dbmfp, 0, DB_SYNC_FILE, NULL, 0, NULL, 0));
}

static pthread_once_t trickle_threads_once = PTHREAD_ONCE_INIT;

struct trickler {
	/* These are set and never modified */
	DB_ENV *dbenv;
	DB_MPOOL *dbmp;
	db_sync_op op;
	int restartable;
	int sgio;

	int nwaits;		/* only updated by one thread */

	/* These variables are protected by lk */
	int total_pages;
	int done_pages;
	int written_pages;
	int ret;
	pthread_mutex_t lk;
	pthread_cond_t wait;
};

struct writable_range {
	BH_TRACK *bharray;
	BH **bhparray;
	DB_MPOOL_HASH **hparray;
	size_t len;

	struct trickler *t;
};

static struct thdpool *trickle_thdpool;
static pool_t *pgpool;
pthread_mutex_t pgpool_lk;


static void
trickle_do_work(struct thdpool *thdpool, void *work, void *thddata, int thd_op)
{
	struct writable_range *range;
	DB_ENV *dbenv;
	db_sync_op op;
	BH *bhp = NULL;
	BH_TRACK *bharray;
	BH **bhparray;
	DB_MPOOL *dbmp;
	DB_MPOOL_HASH *hp;
	DB_MPOOL_HASH **hparray;
	DB_MUTEX *mutexp;
	MPOOLFILE *mfp;
	int ar_cnt, hb_lock, i, j, pass, remaining, ret;
	int wait_cnt, write_cnt, wrote;
	int sgio, gathered, delay_write;
	db_pgno_t off_gather;

	ret = 0;

	range = (struct writable_range *)work;
	dbenv = range->t->dbenv;
	dbmp = range->t->dbmp;
	op = range->t->op;
	bharray = range->bharray;
	bhparray = range->bhparray;
	hparray = range->hparray;
	ar_cnt = range->len;

	sgio = range->t->sgio;
	wrote = gathered = delay_write = 0;
	off_gather = 0;

	/*
	 * Walk the array, writing buffers.  When we write a buffer, we NULL
	 * out its hash bucket pointer so we don't process a slot more than
	 * once.
	 */
	for (i = pass = write_cnt = 0, remaining = ar_cnt; remaining > 0; ++i) {
		/*
		 * If we have buffers locked and ready to write, and we
		 * can't gain anything by delaying writing this bhp,
		 * write out the gather queue immediately. 
		 */
		if (gathered > 0 &&
		    (bharray[off_gather].track_off != bharray[i].track_off ||
		     bharray[off_gather].track_pgno + gathered !=
		     bharray[i].track_pgno)){
			mfp = R_ADDR(dbmp->reginfo,
			    bhparray[off_gather]->mf_offset);

			if (op == DB_SYNC_REMOVABLE_QEXTENT) {
				mfp = NULL;
			}

			if ((ret = __memp_bhwrite_multi(dbmp,
			    &hparray[off_gather],
			    mfp, &bhparray[off_gather], gathered, 1)) == 0)
				wrote += gathered;
			else if (op == DB_SYNC_CACHE || op == DB_SYNC_TRICKLE ||
			    op == DB_SYNC_LRU)
				__db_err(dbenv, "%s: unable to flush page: %lu",
				     __memp_fns(dbmp, mfp), (u_long) bhp->pgno);
			else
				ret = 0;
			
			for (j = off_gather; j < off_gather + gathered; ++j) {
				if (F_ISSET(bhparray[j], BH_LOCKED)) {
				    F_CLR(bhparray[j], BH_LOCKED);
				    MUTEX_UNLOCK(dbenv, &bhparray[j]->mutex);
				}

				/*
				 * Reset the ref_sync count regardless of our
				 * success, we're done with this buffer for
				 * now.
				 */
				bhparray[j]->ref_sync = 0;

				/* Discard our reference and unlock the bucket*/
				--bhparray[j]->ref;
				MUTEX_UNLOCK(dbenv, &hparray[j]->hash_mutex);
			}

			/* 
			 * Once here, we're done with the current gather
			 * queue. Reset. 
			 */
			gathered = 0;

		}

		if (i >= ar_cnt) {
			i = 0;
			++pass;
			sgio = 0;
			(void)__os_sleep(dbenv, 1, 0);
		}
		if ((hp = bharray[i].track_hp) == NULL)
			continue;

		/* Lock the hash bucket and find the buffer. */
		mutexp = &hp->hash_mutex;
		MUTEX_LOCK(dbenv, mutexp);
		for (bhp = SH_TAILQ_FIRST(&hp->hash_bucket, __bh);
		    bhp != NULL; bhp = SH_TAILQ_NEXT(bhp, hq, __bh))
			if (bhp->pgno == bharray[i].track_pgno &&
			    bhp->mf_offset == bharray[i].track_off)
				break;

		/*
		 * If we can't find the buffer we're done, somebody else had
		 * to have written it.
		 *
		 * If the buffer isn't pinned or dirty, we're done, there's
		 * no work needed.
		 */
		if (bhp == NULL || (bhp->ref == 0 && !F_ISSET(bhp, BH_DIRTY))) {
			MUTEX_UNLOCK(dbenv, mutexp);
			--remaining;
			bharray[i].track_hp = NULL;
			continue;
		}

		/*
		 * If the buffer is locked by another thread, ignore it, we'll
		 * come back to it.
		 *
		 * If the buffer is pinned and it's only the first or second
		 * time we have looked at it, ignore it, we'll come back to
		 * it.
		 *
		 * In either case, skip the buffer if we're not required to
		 * write it.
		 */
		if (F_ISSET(bhp, BH_LOCKED) || (bhp->ref != 0 && pass < 2)) {
			MUTEX_UNLOCK(dbenv, mutexp);
			if (op != DB_SYNC_CACHE && op != DB_SYNC_FILE) {
				--remaining;
				bharray[i].track_hp = NULL;
			}
			continue;
		}

		/*
		 * The buffer is either pinned or dirty.
		 *
		 * Set the sync wait-for count, used to count down outstanding
		 * references to this buffer as they are returned to the cache.
		 */
		bhp->ref_sync = bhp->ref;

		/* Pin the buffer into memory and lock it. */
		++bhp->ref;
		F_SET(bhp, BH_LOCKED);
		MUTEX_LOCK(dbenv, &bhp->mutex);

		/*
		 * Unlock the hash bucket and wait for the wait-for count to
		 * go to 0.   No new thread can acquire the buffer because we
		 * have it locked.
		 *
		 * If a thread attempts to re-pin a page, the wait-for count
		 * will never go to 0 (the thread spins on our buffer lock,
		 * while we spin on the thread's ref count).  Give up if we
		 * don't get the buffer in 3 seconds, we can try again later.
		 *
		 * If, when the wait-for count goes to 0, the buffer is found
		 * to be dirty, write it.
		 */
		MUTEX_UNLOCK(dbenv, mutexp);

#ifdef REF_SYNC_TEST
		while (bhp->ref_sync != 0) {
			fprintf(stderr,
		    "... bhp->ref_sync is %d (waiting for it to go to 0)\n",
			    bhp->ref_sync);
			(void)__os_sleep(dbenv, 1, 0);
		}
#else
		for (wait_cnt = 1;
		    bhp->ref_sync != 0 && wait_cnt < 4; ++wait_cnt)
			(void)__os_sleep(dbenv, 1, 0);
#endif

		MUTEX_LOCK(dbenv, mutexp);
		hb_lock = 1;

		/*
		 * If the ref_sync count has gone to 0, we're going to be done
		 * with this buffer no matter what happens.
		 */
		if (bhp->ref_sync == 0) {
			--remaining;
			bharray[i].track_hp = NULL;
		}

		/*
		 * If the ref_sync count has gone to 0 and the buffer is still
		 * dirty, we write it.  We only try to write the buffer once.
		 * Any process checkpointing or trickle-flushing the pool
		 * must be able to write any underlying file -- if the write
		 * fails, error out.  It would be very strange if file sync
		 * failed to write, but we don't care if it happens.
		 */
		if (bhp->ref_sync == 0 && F_ISSET(bhp, BH_DIRTY)) {
			hb_lock = 0;
			MUTEX_UNLOCK(dbenv, mutexp);

			/*
			 * If the following buffer is in sequence, we can
			 * delay this write and write out both buffers as
			 * one I/O.
			 */
			if (sgio && i < ar_cnt - 1 &&
			    bharray[i + 1].track_off == bhp->mf_offset &&
			    bharray[i + 1].track_pgno == bhp->pgno + 1) {
				bhparray[i] = bhp;
				hparray[i] = hp;

				/* If there is no gather queue, start one. */
				if (gathered == 0) {
					off_gather = i;
					gathered = 1;

					/*
					 * We're still holding the buffer lock
					 * and a reference to this buffer,
					 * but not the hb_lock.
					 */
					continue;
				}
				/* 
				 * Check to see if this buffer is part
				 * of the current queue. If so, add it.
				 */
				if (bharray[off_gather].track_off ==
				    bhp->mf_offset &&
				    bharray[off_gather].track_pgno + gathered
				    == bhp->pgno) {

					/*
					 * Ensure that this is the only
					 * dirty page in the bucket. If
					 * there is more than one, than
					 * we run the risk of taking 
					 * the hb_lock twice.
					 */
					if (hp->hash_page_dirty == 1) {
						++gathered;
						continue;
					}
				}
			}

			if (gathered > 0) {
				/* 
				 * Check if this is the last buffer in
				 * the queue.
				 */
				if (bharray[off_gather].track_off ==
				    bhp->mf_offset &&
				    bharray[off_gather].track_pgno + gathered
				    == bhp->pgno && 
					hp->hash_page_dirty == 1) {
					bhparray[i] = bhp;
					hparray[i] = hp;
					++gathered;
				} else {
					/* Delay write to next iteration. */
					delay_write = 1;
				}

				mfp = R_ADDR(dbmp->reginfo,
				    bhparray[off_gather]->mf_offset);
			} else {
				/* One buffer gather queue. */
				bhparray[i] = bhp;
				hparray[i] = hp;
				off_gather = i;
				gathered = 1;

				mfp = R_ADDR(dbmp->reginfo, bhp->mf_offset);
			}

			/* 
			 * If this is a gonner queue extent, call bhwrite with
			 * mfp == NULL.
			 */
			if (op == DB_SYNC_REMOVABLE_QEXTENT) {
				mfp = NULL;
			}

			if ((ret =
				__memp_bhwrite_multi(dbmp,
				    &hparray[off_gather],
				    mfp,
				    &bhparray[off_gather], gathered, 1)) == 0)
				wrote += gathered;
			else if (op == DB_SYNC_CACHE || op == DB_SYNC_TRICKLE
			    || op == DB_SYNC_LRU)
				__db_err(dbenv, "%s: unable to flush page: %lu",
				    __memp_fns(dbmp, mfp), (u_long) bhp->pgno);
			else
				ret = 0;

			/*
			 * Ensure that the buffers in the gather queue
			 * are unlocked.  We're going to have the hash bucket
			 * lock for all the buckets in the gather queue, as
			 * __memp_bhwriten_multi -> __memp_pgwrite_multi will
			 * have swapped the buffer lock for the hash lock.
			 */
			for (j = off_gather; j < off_gather + gathered; ++j) {
				/* 
				 * Use the generic code following the loop for
				 * this bhp. 
				 */
				if (bhp == bhparray[j])
					break;

				if (F_ISSET(bhparray[j], BH_LOCKED)) {
					F_CLR(bhparray[j], BH_LOCKED);
					MUTEX_UNLOCK(dbenv,
					    &bhparray[j]->mutex);
				}

				/*
				 * Reset the ref_sync count regardless of our
				 * success, we're done with this buffer for
				 * now.
				 */
				bhparray[j]->ref_sync = 0;


				/* Discard our reference and unlock
				 * the bucket. */
				--bhparray[j]->ref;
				MUTEX_UNLOCK(dbenv, &hparray[j]->hash_mutex);
			}

			/* 
			 * Once here, we're done with the current gather
			 * queue. Reset. 
			 */
			gathered = 0;

			if (range->t->restartable && bdb_the_lock_desired()) {
				ret = DB_LOCK_DESIRED;

				__db_err(dbenv, "%s: lock desired - page: %lu",
				    __memp_fns(dbmp, mfp), (u_long) bhp->pgno);
			}
		}

		/*
		 * Avoid saturating the disk, sleep once we've done
		 * some number of writes.
		 */
		if (dbenv->mp_maxwrite != 0 &&
		    ++write_cnt >= dbenv->mp_maxwrite) {
			write_cnt = 0;
			(void)__os_sleep(dbenv, 0, dbenv->mp_maxwrite_sleep);
		}

		/*
		 * If this buffer is getting written out on the next
		 * iteration, we need to keep it locked. Otherwise, if
		 * ref_sync count never went to 0, the buffer was written
		 * by another thread, or the write failed, we still have
		 * the buffer locked and it must be unlocked.
		 *
		 * We may or may not currently hold the hash bucket mutex.  If
		 * the __memp_bhwrite -> __memp_pgwrite call was successful,
		 * then __memp_pgwrite will have swapped the buffer lock for
		 * the hash lock.  All other call paths will leave us without
		 * the hash bucket lock.
		 *
		 * The order of mutexes above was to acquire the buffer lock
		 * while holding the hash bucket lock.  Don't deadlock here,
		 * release the buffer lock and then acquire the hash bucket
		 * lock.
		 */
		if (delay_write) {
			bhparray[i] = bhp;
			hparray[i] = hp;

			off_gather = i;
			gathered = 1;
			delay_write = 0;
		} else {
			if (F_ISSET(bhp, BH_LOCKED)) {
				F_CLR(bhp, BH_LOCKED);
				MUTEX_UNLOCK(dbenv, &bhp->mutex);

				if (!hb_lock)
					MUTEX_LOCK(dbenv, mutexp);
			}

			/*
			 * Reset the ref_sync count regardless of our
			 * success, we're done with this buffer for
			 * now.
			 */
			bhp->ref_sync = 0;

			/* Discard our reference and unlock the bucket. */
			--bhp->ref;
			MUTEX_UNLOCK(dbenv, mutexp);
		}

		if (ret != 0)
			break;
	}

	/* 
	 * If we exited the loop and there was still something in the 
	 * gather queue. Write it out now. Also unlock and unpin all
	 * buffers in the gather queue.
	 */
	if (ret == 0 && gathered > 0) {
		mfp = R_ADDR(dbmp->reginfo, bhparray[off_gather]->mf_offset);

		if (op == DB_SYNC_REMOVABLE_QEXTENT) {
			mfp = NULL;
		}

		if ((ret = __memp_bhwrite_multi(dbmp,
		    &hparray[off_gather],
		    mfp, &bhparray[off_gather], gathered, 1)) == 0)
			wrote += gathered;
		else if (op == DB_SYNC_CACHE || op == DB_SYNC_TRICKLE ||
		    op == DB_SYNC_LRU)
			__db_err(dbenv, "%s: unable to flush page: %lu",
			    __memp_fns(dbmp, mfp), (u_long) bhp->pgno);
		else
			ret = 0;
	}

	for (j = off_gather; j < off_gather + gathered; ++j) {
		if (F_ISSET(bhparray[j], BH_LOCKED)) {
			F_CLR(bhparray[j], BH_LOCKED);
			MUTEX_UNLOCK(dbenv, &bhparray[j]->mutex);
		}

		/*
		 * Reset the ref_sync count regardless of our
		 * success, we're done with this buffer for
		 * now.
		 */
		bhparray[j]->ref_sync = 0;

		/* Discard our reference and unlock the bucket. */
		--bhparray[j]->ref;
		MUTEX_UNLOCK(dbenv, &hparray[j]->hash_mutex);
	}

	Pthread_mutex_lock(&range->t->lk);
	range->t->written_pages += wrote;
	range->t->done_pages += ar_cnt;
	range->t->ret = ret;
	Pthread_cond_signal(&range->t->wait);
	Pthread_mutex_unlock(&range->t->lk);

	Pthread_mutex_lock(&pgpool_lk);
	pool_relablk(pgpool, range);
	Pthread_mutex_unlock(&pgpool_lk);
}

static struct thdpool *loadcache_thdpool;
int gbl_load_cache_threads = 8;
int gbl_load_cache_max_pages = 0;
int gbl_dump_cache_max_pages = 0;
int gbl_max_pages_per_cache_thread = 8192;

void
init_trickle_threads(void)
{
	trickle_thdpool = thdpool_create("memptrickle", 0);
	thdpool_set_linger(trickle_thdpool, 10);
	thdpool_set_minthds(trickle_thdpool, 1);
	thdpool_set_maxthds(trickle_thdpool, 4);
	thdpool_set_maxqueue(trickle_thdpool, 8000);
	thdpool_set_longwaitms(trickle_thdpool, 30000);
	Pthread_mutex_init(&pgpool_lk, NULL);

	pgpool =
		pool_setalloc_init(sizeof(struct writable_range), 0, malloc, free);

	loadcache_thdpool = thdpool_create("loadcache", 0);
	thdpool_set_linger(loadcache_thdpool, 10);
	thdpool_set_minthds(loadcache_thdpool, 0);
	thdpool_set_maxthds(loadcache_thdpool, gbl_load_cache_threads);
	thdpool_set_maxqueue(loadcache_thdpool, 0);
	thdpool_set_maxqueueoverride(loadcache_thdpool, 0);
	thdpool_set_wait(loadcache_thdpool, 0);
}

int gbl_parallel_memptrickle = 1;

extern int comdb2_time_epochms();
void thdpool_process_message(struct thdpool *pool, char *line, int lline,
	int st);

void
berkdb_iopool_process_message(char *line, int lline, int st)
{
	pthread_once(&trickle_threads_once, init_trickle_threads);
	thdpool_process_message(trickle_thdpool, line, lline, st);
}

static int memp_sync_alarm_ms = 500;

void
berk_memp_sync_alarm_ms(int x)
{
	logmsg(LOGMSG_DEBUG, "__berkdb_sync_alarm_ms = %d\n", x);
	memp_sync_alarm_ms = x;
}

/*
 * __memp_sync_int --
 *	Mpool sync internal function.
 *
 * PUBLIC: int __memp_sync_int
 * PUBLIC:     __P((DB_ENV *, DB_MPOOLFILE *, int, db_sync_op, int *, int,
 * PUBLIC:          DB_LSN *, int));
 */
int
__memp_sync_int(dbenv, dbmfp, trickle_max, op, wrotep, restartable,
                ckp_lsnp, fixed)
	DB_ENV *dbenv;
	DB_MPOOLFILE *dbmfp;
	int trickle_max, *wrotep;
	db_sync_op op;
	int restartable;
	DB_LSN *ckp_lsnp;
	int fixed;
{
	BH *bhp;
	BH_TRACK *bharray;
	BH **bhparray;
	DB_MPOOL *dbmp;
	DB_MPOOL_HASH *hp;
	DB_MPOOL_HASH **hparray;
	MPOOL *c_mp = NULL, *mp;
	MPOOLFILE *mfp;
	u_int32_t n_cache;
	int ar_cnt, ar_max, i, j, ret, t_ret;
	int wrote;
	int do_parallel;
	struct trickler *pt;
	struct writable_range *range;
	int start, end;
	int memp_sync_files_time = 0;
	DB_LSN oldest_first_dirty_tx_begin_lsn;
	int accum_sync, accum_skip;
	BH_TRACK swap;

	/*
	 *  Perfect checkpoints: If the first dirty LSN is to the right
	 *  of the checkpoint LSN, we don't need to sync the page.
	 *
	 *  The perfect checkpoints algorithm has 3 steps:
	 *
	 *  1) ckp_lsnp is obtained from active TXN list by our caller.
	 *     Use it as our 1st guess of the oldest begin LSN.
	 *
	 *  2) Walk buffer bool. Add pages whose first dirty LSN is to
	 *     the *LEFT* of the checkpoint LSN to bharray. If we find
	 *     a page whose first dirty LSN is to the *RIGHT* of the
	 *     checkpoint LSN, set the oldest begin LSN to the page's
	 *     first dirty LSN. Keep in mind that pages added to bharray
	 *     may not need written because we don't know the oldest
	 *     begin LSN for sure, yet. However the value will converge
	 *     to the real begin LSN as more pages are examined.
	 *
	 *  3) Now we have the real oldest begin LSN. Filter out
	 *     pages whose first dirty LSN is to the right of
	 *     the real oldest LSN.
	 */

	/* Perfect checkpoints step 1: first guess. */
	if (ckp_lsnp != NULL)
		oldest_first_dirty_tx_begin_lsn = *ckp_lsnp;
	else
		MAX_LSN(oldest_first_dirty_tx_begin_lsn);

	accum_sync = accum_skip = 0;
	dbmp = dbenv->mp_handle;
	mp = dbmp->reginfo[0].primary;
	wrote = 0;

	do_parallel = gbl_parallel_memptrickle;

	start = comdb2_time_epochms();

	pthread_once(&trickle_threads_once, init_trickle_threads);

	if (op == DB_SYNC_REMOVABLE_QEXTENT)
		DB_ASSERT(dbmfp != NULL);

	/* Assume one dirty page per bucket. */
	ar_max = mp->nreg * mp->htab_buckets;
	if ((ret =
		__os_malloc(dbenv, ar_max * sizeof(BH_TRACK), &bharray)) != 0)
		return (ret);
	if ((ret =
	     __os_malloc(dbenv, sizeof(struct trickler), &pt)) != 0) {
		__os_free(dbenv, bharray);
		return (ret);
	}
	bhparray = NULL;
	hparray = NULL;

	/*
	 * Walk each cache's list of buffers and mark all dirty buffers to be
	 * written and all pinned buffers to be potentially written, depending
	 * on our flags.
	 */
	for (ar_cnt = 0, n_cache = 0; n_cache < mp->nreg; ++n_cache) {
		c_mp = dbmp->reginfo[n_cache].primary;

		hp = R_ADDR(&dbmp->reginfo[n_cache], c_mp->htab);
		for (i = 0; i < c_mp->htab_buckets; i++, hp++) {
			/*
			 * We can check for empty buckets before locking as we
			 * only care if the pointer is zero or non-zero.  We
			 * can ignore empty buckets because we only need write
			 * buffers that were dirty before we started.
			 */
			if (SH_TAILQ_FIRST(&hp->hash_bucket, __bh) == NULL)
				continue;

			MUTEX_LOCK(dbenv, &hp->hash_mutex);
			for (bhp = SH_TAILQ_FIRST(&hp->hash_bucket, __bh);
			    bhp != NULL; bhp = SH_TAILQ_NEXT(bhp, hq, __bh)) {
				/* Always ignore unreferenced, clean pages. */
				if (bhp->ref == 0 && !F_ISSET(bhp, BH_DIRTY))
					continue;

				/*
				 * Checkpoints have to wait on all pinned pages,
				 * as pages may be marked dirty when returned to
				 * the cache.
				 *
				 * File syncs only wait on pages both pinned and
				 * dirty.  (We don't care if pages are marked
				 * dirty when returned to the cache, that means
				 * there's another writing thread and flushing
				 * the cache for this handle is meaningless.)
				 */
				if (op == DB_SYNC_FILE &&
				    !F_ISSET(bhp, BH_DIRTY))
					continue;

				mfp = R_ADDR(dbmp->reginfo, bhp->mf_offset);

				/*
				 * Ignore temporary files -- this means you
				 * can't even flush temporary files by handle.
				 * (Checkpoint doesn't require temporary files
				 * be flushed and the underlying buffer write
				 * write routine may not be able to write it
				 * anyway.)
				 */
				if (F_ISSET(mfp, MP_TEMP))
					continue;

				/*
				 * If we're flushing a specific file, see if
				 * this page is from that file.
				 */
				if (dbmfp != NULL && mfp != dbmfp->mfp)
					continue;

				/*
				 * Ignore files that aren't involved in DB's
				 * transactional operations during checkpoints.
				 */
				if (dbmfp == NULL && mfp->lsn_off == -1)
					continue;

				/* Perfect checkpoints step 2: compare and update. */
				if (ckp_lsnp != NULL) {
					/*
					 * If log_compare returns 0, it means that the page was
					 * first marked dirty by the TXN whose begin LSN is
					 * oldest_first_dirty_tx_begin_lsn. We know that
					 * the LSN is in the log, therefore the page can be
					 * safely skipped.
					 */
					if (log_compare(&bhp->first_dirty_tx_begin_lsn,
					                &oldest_first_dirty_tx_begin_lsn) >= 0) {
						++accum_skip;
						continue;
					}
					++accum_sync;

					/* If we get not logged or zero LSN, ignore. */
					if (!fixed && !IS_ZERO_LSN(bhp->first_dirty_tx_begin_lsn))
						oldest_first_dirty_tx_begin_lsn = bhp->first_dirty_tx_begin_lsn;
				}

				/* Track the buffer, we want it. */
				bharray[ar_cnt].track_hp = hp;
				bharray[ar_cnt].track_pgno = bhp->pgno;
				bharray[ar_cnt].track_off = bhp->mf_offset;
				bharray[ar_cnt].track_prio = bhp->priority;
				bharray[ar_cnt].track_tx_begin_lsn = bhp->first_dirty_tx_begin_lsn;
				ar_cnt++;

				/*
				 * If we run out of space, double and continue.
				 * Don't stop at trickle_max, we want to sort
				 * as large a sample set as possible in order
				 * to minimize disk seeks.
				 */
				if (ar_cnt >= ar_max) {
					if ((ret = __os_realloc(dbenv,
						    (ar_max * 2) *
						    sizeof(BH_TRACK),
						    &bharray)) != 0)
						break;
					ar_max *= 2;
				}
			}
			MUTEX_UNLOCK(dbenv, &hp->hash_mutex);

			if (ret != 0)
				goto err;
		}
	}

	/* Perfect checkpoints step 3: inplace filtration. */
	if (ckp_lsnp != NULL) {
		if (!fixed) {
			*ckp_lsnp = oldest_first_dirty_tx_begin_lsn;
			for (i = 0, j = ar_cnt; i < j;) {
				if (log_compare(&bharray[i].track_tx_begin_lsn,
				                &oldest_first_dirty_tx_begin_lsn) < 0)
					++i;
				else {
					/* Swap i & j */
					--j;
					swap = bharray[i];
					bharray[i] = bharray[j];
					bharray[j] = swap;
					/* Adjust stats */
					--accum_sync;
					++accum_skip;
				}
			}
			ar_cnt = j;
		}
		c_mp->stat.st_ckp_pages_skip += accum_skip;
		c_mp->stat.st_ckp_pages_sync += accum_sync;
	}

	/* If there no buffers to write, we're done. */
	if (ar_cnt == 0)
		goto done;

	/* Allocate bhparray and hparray only after we know the exact value of ar_cnt.
	   This way we are able to save memory and reduce allocation calls. */
	if ((ret = __os_malloc(dbenv, ar_cnt * sizeof(BH *), &bhparray)) != 0)
		goto err;
	if ((ret = __os_malloc(dbenv,
	                       ar_cnt * sizeof(DB_MPOOL_HASH *), &hparray)) != 0)
		goto err;

	/*
	 * If writing in LRU, do so. Otherwise, write the buffers in
	 * file/page order, trying to reduce seeks by the filesystem
	 * and, when pages are smaller than filesystem block sizes,
	 * reduce the actual number of writes.
	 */
	if (op == DB_SYNC_LRU)
		qsort(bharray, ar_cnt, sizeof(BH_TRACK), __bhlru);
	else if (ar_cnt > 1)
		qsort(bharray, ar_cnt, sizeof(BH_TRACK), __bhcmp);

	/*
	 * If we're trickling buffers, only write enough to reach the correct
	 * percentage.
	 */
	if ((op == DB_SYNC_TRICKLE || op == DB_SYNC_LRU)
	    && ar_cnt > trickle_max)
		ar_cnt = trickle_max;

	/*
	 * Write the LRU pages in file/page order, only sorting as many
	 * as ar_cnt.
	 */
	if (op == DB_SYNC_LRU)
		qsort(bharray, ar_cnt, sizeof(BH_TRACK), __bhcmp);

	/*
	 * Flush the log.  We have to ensure the log records reflecting the
	 * changes on the database pages we're writing have already made it
	 * to disk.  We still have to check the log each time we write a page
	 * (because pages we are about to write may be modified after we have
	 * flushed the log), but in general this will at least avoid any I/O
	 * on the log's part.
	 */
	if (LOGGING_ON(dbenv) && (ret = __log_flush(dbenv, NULL)) != 0)
		 goto err;


	/*
	 * Initialize for the call to trickle_do_work.
	 */
	pt->dbenv = dbenv;
	pt->dbmp = dbmp;
	pt->op = op;
	pt->restartable = restartable;
	pt->sgio = dbenv->attr.sgio_enabled;
			
	pt->total_pages = pt->done_pages = pt->written_pages = 0;
	pt->ret = pt->nwaits = 0;
	Pthread_mutex_init(&pt->lk, NULL);
	Pthread_cond_init(&pt->wait, NULL);

	/*
	 * Flush each file by passing it to a thread. This serializes writes
	 * to a file, which may help throughput and performance.
	 */
	if (do_parallel &&
	    (op == DB_SYNC_TRICKLE || op == DB_SYNC_LRU ||
		op == DB_SYNC_CACHE)) {

		for (i = 1, j = 0; i < ar_cnt; ++i) {
			if (bharray[j].track_off != bharray[i].track_off) {
				Pthread_mutex_lock(&pgpool_lk);
				range = pool_getablk(pgpool);
				Pthread_mutex_unlock(&pgpool_lk);

				range->bharray = &bharray[j];
				range->bhparray = &bhparray[j];
				range->hparray = &hparray[j];
				range->len = (size_t) i - j;
				range->t = pt;

				/* 
				 * lame, should block instead, thdpool
				 *  can't do that yet 
				 */
				t_ret = 1;
				Pthread_mutex_lock(&pt->lk);
				while(pt->ret == 0 &&
				      t_ret != 0) {
					Pthread_mutex_unlock(&pt->lk);
					
					t_ret = thdpool_enqueue(trickle_thdpool,
					    trickle_do_work, range, 0, NULL, 0,
					    PRIORITY_T_DEFAULT);
					if (t_ret) {
						pt->nwaits++;
						poll(NULL, 0, 10);
					}
					Pthread_mutex_lock(&pt->lk);
				}

				/*
				 * pt->lk is still locked
				 */
				if (t_ret == 0) {
					pt->total_pages += i - j;
				}
				Pthread_mutex_unlock(&pt->lk);
					

				j = i;
			}
		}

		if (pt->ret == 0) {
			Pthread_mutex_lock(&pgpool_lk);
			range = pool_getablk(pgpool);
			Pthread_mutex_unlock(&pgpool_lk);

			range->bharray = &bharray[j];
			range->bhparray = &bhparray[j];
			range->hparray = &hparray[j];
			range->len = (size_t) i - j;
			range->t = pt;

			/* 
			 * lame, should block instead, thdpool
			 *  can't do that yet 
			 */
			t_ret = 1;
			Pthread_mutex_lock(&pt->lk);
			while(pt->ret == 0 && t_ret != 0) {
				Pthread_mutex_unlock(&pt->lk);

				t_ret = thdpool_enqueue(trickle_thdpool,
				    trickle_do_work, range, 0, NULL, 0,
				    PRIORITY_T_DEFAULT);
				if (t_ret) {
					pt->nwaits++;
					poll(NULL, 0, 10);
				}
			
				Pthread_mutex_lock(&pt->lk);
			}

			/*
			 * pt->lk is still locked
			 */
			if (t_ret == 0) {
				pt->total_pages += i - j;
			}
			Pthread_mutex_unlock(&pt->lk);
		}

		/* wait for writers to finish */
		Pthread_mutex_lock(&pt->lk);
		while (pt->done_pages < pt->total_pages) {
			Pthread_cond_wait(&pt->wait, &pt->lk);
		}
		wrote = pt->written_pages;
		ret = pt->ret;
		Pthread_mutex_unlock(&pt->lk);
	} else {
		Pthread_mutex_lock(&pgpool_lk);
		range = pool_getablk(pgpool);
		Pthread_mutex_unlock(&pgpool_lk);

		range->bharray = bharray;
		range->bhparray = bhparray;
		range->hparray = hparray;
		range->len = ar_cnt;
		range->t = pt;
		
		trickle_do_work(NULL, range, NULL, 0);

		wrote = pt->written_pages;
		ret = pt->ret;
	}

	Pthread_mutex_destroy(&pt->lk);
	Pthread_cond_destroy(&pt->wait);
done:
        /*
         * If doing a checkpoint or flushing a file for the application, we
         * have to force the pages to disk.  We don't do this as we go along
         * because we want to give the OS as much time as possible to lazily
         * flush, and because we have to flush files that might not even have
         * had dirty buffers in the cache, so we have to walk the files list.
         */
        if (ret == 0 && (op == DB_SYNC_CACHE || op == DB_SYNC_FILE)) {
            if (dbmfp == NULL) {
                int start, end;
                start = comdb2_time_epochms();
                ret = __memp_sync_files(dbenv, dbmp);
                end = comdb2_time_epochms();
                memp_sync_files_time = end - start;
            }
            else
                ret = __os_fsync(dbenv, dbmfp->fhp);
        }

	/* If we've opened files to flush pages, close them. */
	if ((t_ret = __memp_close_flush_files(dbenv, dbmp)) != 0 && ret == 0)
		ret = t_ret;

err:	__os_free(dbenv, bharray);
	__os_free(dbenv, bhparray);
	__os_free(dbenv, hparray);
	__os_free(dbenv, pt);

	if (wrotep != NULL)
		*wrotep = wrote;

	end = comdb2_time_epochms();

	if (wrote && ((end - start) > memp_sync_alarm_ms))
		ctrace("memp_sync %d pages %d ms (memp_sync_files %d ms)\n",
		    wrote, end - start, memp_sync_files_time);

	return (ret);
}

/*
 * __memp_sync_files --
 *	Sync all the files in the environment, open or not.
 */
static
int __memp_sync_files(dbenv, dbmp)
	DB_ENV *dbenv;
	DB_MPOOL *dbmp;
{
	DB_MPOOLFILE *dbmfp;
	MPOOL *mp;
	MPOOLFILE *mfp;
	int final_ret, ret, had_files, didnt_have_files;
	u_int32_t flags;

	flags = dbenv->close_flags;
	had_files = didnt_have_files = final_ret = 0;
	mp = dbmp->reginfo[0].primary;

	/* if all dbs are opened O_SYNC or O_DIRECT, there's no reason to 
	 * call fsync on them, so skip that step */

	if (dbenv->attr.skip_sync_if_direct &&
	    ((dbenv->flags & DB_ENV_DIRECT_DB) ||
		(dbenv->flags & DB_ENV_OSYNC)))
		return 0;

	R_LOCK(dbenv, dbmp->reginfo);

	if (dbenv->attr.flush_scan_dbs_first) {
		/*
		 * Go through all the databases first.  We'll likely
		 * have open files for those.  If we have a file
		 * handle for a database, flush it and mark it
		 * flushed.
		 */
		MUTEX_THREAD_LOCK(dbenv, dbmp->mutexp);
		for (dbmfp = TAILQ_FIRST(&dbmp->dbmfq); dbmfp != NULL;
		    dbmfp = TAILQ_NEXT(dbmfp, q)) {
			mfp = dbmfp->mfp;
			if (mfp == NULL ||F_ISSET(dbmfp, MP_READONLY) ||
			    !mfp->file_written || mfp->deadfile ||
			    F_ISSET(mfp, MP_TEMP))
				continue;

			if (!LF_ISSET(DB_NOSYNC)) {
				ret = __os_fsync(dbenv, dbmfp->fhp);
				if (ret != 0) {
					__db_err(dbenv,
					    "%s: unable to flush: %s",
					    (char *)R_ADDR(dbmp->reginfo,
						mfp->path_off),
					    db_strerror(ret));
					if (final_ret == 0)
						final_ret = ret;
				}
			}
			mfp->flushed = 1;
			had_files++;
		}
		MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);
		/*
		 * Now go through all the file handles.  If we didn't
		 * flush it above, we know we don't have an open file
		 * for it, so create one and flush it.
		 */
		for (mfp = SH_TAILQ_FIRST(&mp->mpfq, __mpoolfile);
		    mfp != NULL; mfp = SH_TAILQ_NEXT(mfp, q, __mpoolfile)) {
			if (!mfp->file_written ||
			    mfp->deadfile || F_ISSET(mfp, MP_TEMP) ||
			    mfp->flushed) {
				mfp->flushed = 0;
				continue;
			}
			mfp->flushed = 0;

			ret = __memp_mf_sync(dbmp, mfp);
			if (ret != 0) {
				__db_err(dbenv, "%s: unable to flush: %s",
				    (char *)R_ADDR(dbmp->reginfo,
					mfp->path_off), db_strerror(ret));
				if (final_ret == 0)
					final_ret = ret;
			}
			didnt_have_files++;
		}
	} else {
		for (mfp = SH_TAILQ_FIRST(&mp->mpfq, __mpoolfile);
		    mfp != NULL; mfp = SH_TAILQ_NEXT(mfp, q, __mpoolfile)) {
			if (!mfp->file_written ||
			    mfp->deadfile || F_ISSET(mfp, MP_TEMP))
				continue;

			/*
			 * Look for an already open, writeable handle
			 * (fsync doesn't work on read-only Windows
			 * handles).
			 */
			ret = 0;
			MUTEX_THREAD_LOCK(dbenv, dbmp->mutexp);
			for (dbmfp = TAILQ_FIRST(&dbmp->dbmfq);
			    dbmfp != NULL; dbmfp = TAILQ_NEXT(dbmfp, q)) {
				if (dbmfp->mfp != mfp ||
				    F_ISSET(dbmfp, MP_READONLY))
					continue;

				if (!LF_ISSET(DB_NOSYNC))
					ret = __os_fsync(dbenv, dbmfp->fhp);

				break;
			}
			MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);

			/* If we don't find one, open one. */
			if (dbmfp == NULL)
				ret = __memp_mf_sync(dbmp, mfp);
			if (ret != 0) {
				__db_err(dbenv, "%s: unable to flush: %s",
				    (char *)R_ADDR(dbmp->reginfo,
					mfp->path_off), db_strerror(ret));
				if (final_ret == 0)
					final_ret = ret;
			}
		}
	}

	R_UNLOCK(dbenv, dbmp->reginfo);

	return (final_ret);
}

/*
 * __memp_close_flush_files --
 *	Close files opened only to flush buffers.
 */
static int
__memp_close_flush_files(dbenv, dbmp)
	DB_ENV *dbenv;
	DB_MPOOL *dbmp;
{
	DB_MPOOLFILE *dbmfp;
	int ret;

	/*
	 * The routine exists because we must close files opened by sync to
	 * flush buffers.  There are two cases: first, extent files have to
	 * be closed so they may be removed when empty.  Second, regular
	 * files have to be closed so we don't run out of descriptors (for
	 * example, and application partitioning its data into databases
	 * based on timestamps, so there's a continually increasing set of
	 * files).
	 *
	 * We mark files opened in the __memp_bhwrite() function with the
	 * MP_FLUSH flag.  Here we walk through our file descriptor list,
	 * and, if a file was opened by __memp_bhwrite(), we close it.
	 */
retry:	MUTEX_THREAD_LOCK(dbenv, dbmp->mutexp);
	for (dbmfp = TAILQ_FIRST(&dbmp->dbmfq);
	    dbmfp != NULL; dbmfp = TAILQ_NEXT(dbmfp, q))
		if (F_ISSET(dbmfp, MP_FLUSH)) {
			F_CLR(dbmfp, MP_FLUSH);
			MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);
			if ((ret = __memp_fclose(dbmfp, 0)) != 0)
				return (ret);
			goto retry;
		}
	MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);

	return (0);
}

#include <sbuf2.h>
#include <unistd.h>
#include <fcntl.h>

/*
 * __memp_load_pp --
 *	DB_ENV->memp_load pre/post processing.
 *
 * PUBLIC: int __memp_load_pp __P((DB_ENV *, SBUF2 *));
 */
int
__memp_load_pp(dbenv, s)
	DB_ENV *dbenv;
	SBUF2 *s;
{
	int rep_check, ret;
	u_int32_t lines;
    u_int64_t pagecount;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
		dbenv->mp_handle, "memp_load", DB_INIT_MPOOL);
	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __memp_load(dbenv, s, &pagecount, &lines);
	if (rep_check)
		__env_rep_exit(dbenv);
	logmsg(LOGMSG_INFO, "%s loaded %"PRIu64" pages\n", __func__, pagecount);
	return (ret);
}

/*
 * __memp_dump_pp --
 *	DB_ENV->memp_dump pre/post processing.
 *
 * PUBLIC: int __memp_dump_pp __P((DB_ENV *, SBUF2 *, u_int64_t));
 */
int
__memp_dump_pp(dbenv, s, max_pages)
	DB_ENV *dbenv;
	SBUF2 *s;
	u_int64_t max_pages;
{
	int rep_check, ret;
	u_int64_t pagecount;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
		dbenv->mp_handle, "memp_dump", DB_INIT_MPOOL);
	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __memp_dump(dbenv, s, max_pages, &pagecount);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __memp_load_default_pp --
 *	DB_ENV->memp_load_default pre/post processing.
 *
 * PUBLIC: int __memp_load_default_pp __P((DB_ENV *));
 */
int
__memp_load_default_pp(dbenv)
	DB_ENV *dbenv;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
		dbenv->mp_handle, "memp_load_default", DB_INIT_MPOOL);
	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __memp_load_default(dbenv);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __memp_dump_default_pp --
 *	DB_ENV->memp_dump_default pre/post processing.
 *
 * PUBLIC: int __memp_dump_default_pp __P((DB_ENV *, u_int32_t));
 */
int
__memp_dump_default_pp(dbenv, force)
	DB_ENV *dbenv;
    u_int32_t force;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv,
		dbenv->mp_handle, "memp_dump_default", DB_INIT_MPOOL);
	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __memp_dump_default(dbenv, force);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

static int getcpage(SBUF2 *s, char *cpage, int cpagesz, int *endofline)
{
	(*endofline) = 0;
	for (int i = 0; i < cpagesz; i++) {
		char c = sbuf2getc(s);
		if (c <= 0 || c == '\n')
			(*endofline) = 1;
		if (c <= 0 || c == '\n' || c == ' ') {
			cpage[i] = '\0';
			return i;
		}
		cpage[i] = c;
	}
	return -1;
}

typedef struct fileid_page_list {
	u_int8_t fileid[DB_FILE_ID_LEN];
	db_pgno_t *pages;
	u_int64_t cnt;
	u_int64_t alloced;
} fileid_page_list_t;

#define PAGELIST_INIT 1024
#define PAGEARRAY_INIT 4096

typedef struct page_fget_count {
	fileid_page_list_t *fileid_page_list;
	db_pgno_t page;
	u_int32_t fget_count;
} page_fget_count_t;

typedef struct sorted_page_list {
	u_int64_t cnt;
	u_int64_t alloced;
	page_fget_count_t *pagearray;
} sorted_page_list_t;

static inline int
pgcmp(const void *p1, const void *p2)
{
	db_pgno_t *page1 = (db_pgno_t *)p1;
	db_pgno_t *page2 = (db_pgno_t *)p2;
	if (*page1 == *page2) {
		return 0;
	} else if (*page1 < *page2) {
		return -1;
	} else {
		return 1;
	}
	abort();
}

static inline int
pgrefcmp(const void *p1, const void *p2)
{
	page_fget_count_t *page1 = (page_fget_count_t *)p1;
	page_fget_count_t *page2 = (page_fget_count_t *)p2;
	if (page1->fget_count == page2->fget_count) {
		return 0;
	} else if (page1->fget_count < page2->fget_count) {
		return 1;
	} else {
		return -1;
	}
	abort();
}

typedef struct fileid_load_info
{
	DB_ENV *dbenv;
	struct thdpool *load_thdpool;
} fileid_load_info_t;

typedef struct fileid_page_env
{
	DB_ENV *dbenv;
	fileid_page_list_t *pagelist;
	pthread_mutex_t *lk;
	pthread_cond_t *cd;
	int *active_threads;
} fileid_page_env_t;

void touch_page(DB_MPOOLFILE *mpf, db_pgno_t pgno);

static void
load_fileids(struct thdpool *thdpool, void *work, void *thddata, int thd_op)
{
	fileid_page_env_t *fileid_env = (fileid_page_env_t *)work;
	fileid_page_list_t *pagelist = fileid_env->pagelist;

	DB_ENV *dbenv;
	DB_MPOOL *dbmp;
	DB_MPOOLFILE *dbmfp;

	dbenv = fileid_env->dbenv;
	dbmp = dbenv->mp_handle;
	dbmfp = NULL;

	MUTEX_THREAD_LOCK(dbenv, dbmp->mutexp);
	for (dbmfp = TAILQ_FIRST(&dbmp->dbmfq); dbmfp != NULL;
			dbmfp = TAILQ_NEXT(dbmfp, q)) {
		if (memcmp(dbmfp->fileid, pagelist->fileid, DB_FILE_ID_LEN) == 0)
			break;
	}
	MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);

	if (dbmfp) {
		for(int pages = 0 ; pages < pagelist->cnt; pages++) {
			touch_page(dbmfp, pagelist->pages[pages]);
		}
	} 

	Pthread_mutex_lock(fileid_env->lk);
	(*fileid_env->active_threads)--;
	Pthread_cond_signal(fileid_env->cd);
	Pthread_mutex_unlock(fileid_env->lk);

	__os_free(dbenv, pagelist->pages);
	__os_free(dbenv, pagelist);
	__os_free(dbenv, fileid_env);
}

struct sbuf_env {
	DB_ENV *dbenv;
	SBUF2 *s;
};

static int
output_fileid_page(void *obj, void *arg)
{
	struct sbuf_env *sbenv = (struct sbuf_env *)arg;
	u_int8_t *p;
	DB_ENV *dbenv = sbenv->dbenv;
	SBUF2 *s = sbenv->s;
	fileid_page_list_t *pagelist = (fileid_page_list_t *)obj;
	qsort(pagelist->pages, pagelist->cnt, sizeof(db_pgno_t), pgcmp);
	p = pagelist->fileid;

	if (pagelist->cnt == 0)
		return 0;
	for (int j = 0; j < DB_FILE_ID_LEN; ++j, ++p) {
		sbuf2printf(s, "%2.2x", (u_int)*p);
	}
	for (u_int64_t pages = 0; pages < pagelist->cnt; pages++) {
		sbuf2printf(s, " %"PRIu32, pagelist->pages[pages]);
	}
	sbuf2printf(s, "\n");
	return 0;
}

static int
output_fileid_page_hash(DB_ENV *dbenv, hash_t *hash, SBUF2 *s)
{
	struct sbuf_env sbenv = { .dbenv = dbenv, .s = s };
	hash_for(hash, output_fileid_page, &sbenv);
	return 0;
}

static inline int add_page_to_fileid_list(DB_ENV *dbenv, fileid_page_list_t *pagelist,
		db_pgno_t pg)
{
	int ret;
	if (pagelist->cnt == pagelist->alloced) {
		u_int64_t alloccount = (pagelist->alloced == 0) ? PAGELIST_INIT :
				(pagelist->alloced * 2);
		if ((ret = __os_realloc(dbenv, alloccount * sizeof(db_pgno_t),
						&pagelist->pages)) != 0) {
			return ret;
		}
		pagelist->alloced = alloccount;
	}
	pagelist->pages[pagelist->cnt++] = pg;
	return 0;
}

static inline int add_page_to_sorted_page_list(DB_ENV *dbenv, sorted_page_list_t
		*pagearray, fileid_page_list_t *pagelist, db_pgno_t pg, u_int32_t fget_count)
{
	int ret;
	if (pagearray->cnt == pagearray->alloced) {
		u_int64_t alloccount = (pagearray->alloced == 0) ? PAGEARRAY_INIT :
				(pagearray->alloced * 2);
		if ((ret = __os_realloc(dbenv, alloccount * sizeof(page_fget_count_t),
						&pagearray->pagearray)) != 0) {
			return ret;
		}
		pagearray->alloced = alloccount;
	}
	pagearray->pagearray[pagearray->cnt].fileid_page_list = pagelist;
	pagearray->pagearray[pagearray->cnt].page = pg;
	pagearray->pagearray[pagearray->cnt].fget_count = fget_count;
	pagearray->cnt++;
	return 0;
}

static inline int
add_fileid_page(DB_ENV *dbenv, hash_t *hash, sorted_page_list_t *pagearray,
		u_int8_t *fileid, db_pgno_t pg, u_int32_t fget_count)
{
	int ret;
	fileid_page_list_t *pagelist = hash_find(hash, fileid);
	if (!pagelist) {
		if ((ret = __os_calloc(dbenv, 1, sizeof(*pagelist), &pagelist)) != 0) {
			return ret;
		}
		memcpy(pagelist->fileid, fileid, DB_FILE_ID_LEN);
		hash_add(hash, pagelist);
	}
	return add_page_to_sorted_page_list(dbenv, pagearray, pagelist, pg,
			fget_count);
}

static int free_fileid_page(void *obj, void *arg)
{
	fileid_page_list_t *f = (fileid_page_list_t *)obj;
	DB_ENV *dbenv = (DB_ENV *)arg;
	__os_free(dbenv, f->pages);
	__os_free(dbenv, f);
	return 0;
}

static void
destroy_fileid_page_hash(DB_ENV *dbenv, hash_t *hash)
{
	hash_for(hash, free_fileid_page, dbenv);
	hash_clear(hash);
	hash_free(hash);
}

static void
load_fileids_thdpool(fileid_page_env_t *fileid_env)
{
	int ret;
	Pthread_mutex_lock(fileid_env->lk);
	(*fileid_env->active_threads)++;
	if ((ret = thdpool_enqueue(loadcache_thdpool, load_fileids,
                                   fileid_env, 0, NULL, 0,
                                   PRIORITY_T_DEFAULT)) != 0) {
		Pthread_mutex_unlock(fileid_env->lk);
		load_fileids(NULL, fileid_env, NULL, 0);
	} else {
		Pthread_mutex_unlock(fileid_env->lk);
	}
}

/*
 * __memp_load --
 *	Load bufferpool fileids and pages to a file
 *
 * PUBLIC: int __memp_load
 * PUBLIC:	 __P((DB_ENV *, SBUF2 *, u_int64_t *, u_int32_t *));
 */
int
__memp_load(dbenv, s, pagecount, lines)
	DB_ENV *dbenv;
	SBUF2 *s;
	u_int64_t *pagecount;
	u_int32_t *lines;
{
	DB_MPOOL *dbmp;
	DB_MPOOLFILE *dbmfp;
	fileid_page_env_t *fileid_env = NULL;
 	u_int32_t lineno = 0, start, end;
	int ret = 0, endofline = 0, max_pages = gbl_load_cache_max_pages;
	u_int8_t *pr;
	u_int8_t fileid[DB_FILE_ID_LEN] = {0};
	char cfileid[DB_FILE_ID_LEN*2], cpage[64];
	char *p, *sp, c;
	db_pgno_t pg;
	unsigned int hx;
	void *addrp = NULL;
	dbmp = dbenv->mp_handle;
	dbmfp = NULL;
	pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
	pthread_cond_t cd = PTHREAD_COND_INITIALIZER;
	int active_threads = 0;
	(*lines) = (*pagecount) = 0;


	MUTEX_THREAD_LOCK(dbenv, dbmp->mutexp);
	dbmfp = TAILQ_FIRST(&dbmp->dbmfq);
	MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);
	if (dbmfp == NULL) {
#if PAGELIST_DEBUG
		logmsg(LOGMSG_USER, "%s mp_handle has no mpoolfiles\n",
				__func__);
#endif
		return -1;
	}

	start = time(NULL);
	while ((!max_pages || (*pagecount) < max_pages) && (ret =
				sbuf2fread(cfileid, sizeof(cfileid), 1, s)) == 1) {
		lineno++;

		p = cfileid;
		for (int j = 0; j < DB_FILE_ID_LEN; ++j, p+=2) {
			sscanf(p, "%2x", &hx);
			fileid[j] = hx;
		}

		if ((c = sbuf2getc(s)) != ' ') {
#if PAGELIST_DEBUG
			logmsg(LOGMSG_USER, "%s line %u, invalid format\n",
					__func__, lineno);
#endif
			sbuf2nextline(s);
			continue;
		}

		while ((!max_pages || (*pagecount) < max_pages) && getcpage(s, cpage,
					sizeof(cpage), &endofline) > 0) {
			if ((sscanf(cpage, "%"PRIu32, &hx)) <= 0) {
				logmsg(LOGMSG_DEBUG, "%s bad page format on line %u "
						"cpage %s\n", __func__, lineno, cpage);
				break;
			}
			pg = hx;

			if (fileid_env == NULL) { 
				if ((ret = __os_malloc(dbenv, sizeof(*fileid_env), 
								&fileid_env)) != 0) {
					logmsg(LOGMSG_ERROR, "%s line %d error alocating memory, %d\n",
							__func__, __LINE__, ret);
					goto done;
				}

				if ((ret = __os_calloc(dbenv, 1, sizeof(fileid_page_list_t),
								&fileid_env->pagelist)) != 0) {
					__os_free(dbenv, fileid_env);
					fileid_env = NULL;
					logmsg(LOGMSG_ERROR, "%s line %d error alocating memory, %d\n",
							__func__, __LINE__, ret);
					goto done;
				}

				fileid_env->dbenv = dbenv;
				fileid_env->lk = &lk;
				fileid_env->cd = &cd;
				fileid_env->active_threads = &active_threads;
				memcpy(fileid_env->pagelist->fileid, fileid, DB_FILE_ID_LEN);
			}

			if ((ret = add_page_to_fileid_list(dbenv, fileid_env->pagelist,
							pg)) != 0) {
				__os_free(dbenv, fileid_env->pagelist);
				__os_free(dbenv, fileid_env);
				fileid_env = NULL;
				logmsg(LOGMSG_ERROR, "%s line %d error alocating memory, %d\n",
						__func__, __LINE__, ret);
				goto done;
			}

			if (fileid_env->pagelist->cnt >= gbl_max_pages_per_cache_thread) {
				load_fileids_thdpool(fileid_env);
				fileid_env = NULL;
			}

			(*pagecount)++;

			if (endofline)
				break;
		}
		if (fileid_env) {
			load_fileids_thdpool(fileid_env);
			fileid_env = NULL;
		}
		if (!endofline)
			sbuf2nextline(s);
	}
	if (fileid_env) {
		load_fileids_thdpool(fileid_env);
		fileid_env = NULL;
	}
done:
	Pthread_mutex_lock(&lk);
	while(active_threads > 0) {
		Pthread_cond_wait(&cd, &lk);
	}
	Pthread_mutex_unlock(&lk);
	end = time(NULL);

	logmsg(LOGMSG_DEBUG, "Loaded %"PRIu64" bufferpool pages in %u seconds\n",
			*pagecount, (end - start));
	(*lines) = lineno;
	return ret;
}

/*
 * __memp_dump --
 *	Write bufferpool fileids and pages to a file
 *
 * PUBLIC: int __memp_dump
 * PUBLIC:	 __P((DB_ENV *, SBUF2 *, u_int64_t, u_int64_t *));
 */
int
__memp_dump(dbenv, s, max_pages, pagecount)
	DB_ENV *dbenv;
	SBUF2 *s;
	u_int64_t max_pages;
	u_int64_t *pagecount;
{
	BH *bhp;
	BH_TRACK *bharray;
	BH **bhparray;
	DB_MPOOL *dbmp;
	DB_MPOOL_HASH *hp;
	DB_MPOOL_HASH **hparray;
	MPOOL *c_mp = NULL, *mp;
	MPOOLFILE *mfp;
	u_int32_t n_cache;
	u_int64_t dump_pages;
	int i, j, ret, t_ret, first = 1;
	u_int8_t *fileid, *p, *pp, last_fileid[DB_FILE_ID_LEN] = {0};
	hash_t *fileid_pages = NULL;
	sorted_page_list_t pagearray = {0};

	dbmp = dbenv->mp_handle;
	mp = dbmp->reginfo[0].primary;

	(*pagecount) = 0;
	fileid_pages = hash_init(DB_FILE_ID_LEN);
	for (n_cache = 0; n_cache < mp->nreg; ++n_cache) {
		c_mp = dbmp->reginfo[n_cache].primary;

		hp = R_ADDR(&dbmp->reginfo[n_cache], c_mp->htab);
		for (i = 0; i < c_mp->htab_buckets; i++, hp++) {

			if (SH_TAILQ_FIRST(&hp->hash_bucket, __bh) == NULL)
				continue;

			MUTEX_LOCK(dbenv, &hp->hash_mutex);
			for (bhp = SH_TAILQ_FIRST(&hp->hash_bucket, __bh);
				bhp != NULL; bhp = SH_TAILQ_NEXT(bhp, hq, __bh)) {

				mfp = R_ADDR(dbmp->reginfo, bhp->mf_offset);

				if (F_ISSET(mfp, MP_TEMP) || mfp->lsn_off == -1)
					continue;

				mfp = R_ADDR(dbmp->reginfo, bhp->mf_offset);
				fileid = R_ADDR(dbmp->reginfo, mfp->fileid_off);

				if ((ret = add_fileid_page(dbenv, fileid_pages, &pagearray,
								fileid, bhp->pgno, bhp->fget_count)) != 0) {
					MUTEX_UNLOCK(dbenv, &hp->hash_mutex);
					destroy_fileid_page_hash(dbenv, fileid_pages);
					__os_free(dbenv, pagearray.pagearray);
					logmsg(LOGMSG_ERROR, "%s error adding fileid page to hash "
							"%d\n", __func__, ret);
					return ret;
				}
			}
			MUTEX_UNLOCK(dbenv, &hp->hash_mutex);
		}
	}

	if (!max_pages)
		dump_pages = pagearray.cnt;
	else
		dump_pages = (max_pages < pagearray.cnt) ? max_pages : pagearray.cnt;

	/* Sort so we output the most used pages if we only want a subset */
	if (dump_pages != pagearray.cnt)
		qsort(pagearray.pagearray, pagearray.cnt, sizeof(page_fget_count_t),
				pgrefcmp);

	u_int32_t lastfget = UINT_MAX;
	for (u_int64_t i = 0; i < dump_pages; i++) {
		page_fget_count_t *page_fget = &pagearray.pagearray[i];
		assert((page_fget->fget_count <= lastfget) ||
				(dump_pages == pagearray.cnt));
		lastfget = page_fget->fget_count;
		add_page_to_fileid_list(dbenv, page_fget->fileid_page_list, page_fget->page);
		(*pagecount)++;
	}
	__os_free(dbenv, pagearray.pagearray);
	output_fileid_page_hash(dbenv, fileid_pages, s);
	destroy_fileid_page_hash(dbenv, fileid_pages);

	return 0;
}

static pthread_mutex_t page_flush_lk = PTHREAD_MUTEX_INITIALIZER;

#define PAGELIST "pagelist"
#define PAGELISTTEMP "pagelist.tmp"

char *bdb_trans(const char infile[], char outfile[]);

extern u_int64_t gbl_memp_pgreads;
static u_int64_t memp_pagecount = 0;

/*
 * __memp_load_default --
 *	Load bufferpool cache to log directory
 *
 * PUBLIC: int __memp_load_default
 * PUBLIC:	 __P((DB_ENV *));
 */
int
__memp_load_default(dbenv)
	DB_ENV *dbenv;
{
	char path[PATH_MAX], pathbuf[PATH_MAX], *rpath;
	u_int32_t lines;
	u_int64_t cnt;
	int fd, ret;
	SBUF2 *s;

	Pthread_mutex_lock(&page_flush_lk);

	snprintf(path, sizeof(path), "%s/%s", dbenv->db_home, PAGELIST);
	rpath = bdb_trans(path, pathbuf);
#if PAGELIST_DEBUG
	logmsg(LOGMSG_USER, "%s line %d opening %s\n", __func__, __LINE__,
			rpath);
#endif
	if ((fd = open(rpath, O_RDONLY, 0666)) < 0 ||
			(s = sbuf2open(fd, 0)) == NULL) {
#if PAGELIST_DEBUG
		logmsg(LOGMSG_ERROR, "%s line %d error opening %s, %d\n", __func__,
				__LINE__, rpath, errno);
#endif
		if (fd >= 0)
			close(fd);
		ret = -1;
		goto done;
	}

	if ((ret = __memp_load(dbenv, s, &cnt, &lines)) != 0) {
#if PAGELIST_DEBUG
		logmsg(LOGMSG_USER, "%s failed load page cache: bufferpool not open\n",
				__func__);
#endif
	} else {
#if PAGELIST_DEBUG
		logmsg(LOGMSG_USER, "%s loaded cache %"PRIu64" pages processed %u "
				"lines, ret=%d\n", __func__, cnt, lines, ret);
#endif
	}
	sbuf2close(s);

done:
	memp_pagecount = cnt;
	Pthread_mutex_unlock(&page_flush_lk);
	gbl_memp_pgreads = 0;
	return ret;
}

int gbl_memp_dump_cache_threshold = 20;

/*
 * __memp_dump_default --
 *	Dump bufferpool pagelist to log directory
 *
 * PUBLIC: int __memp_dump_default
 * PUBLIC:	 __P((DB_ENV *, u_int32_t));
 */
int
__memp_dump_default(dbenv, force)
	DB_ENV *dbenv;
	u_int32_t force;
{
	static int count = 0;
	int fd, ret = 0;
	u_int32_t lines;
	u_int64_t cnt;
	int thresh = gbl_memp_dump_cache_threshold;
	char path[PATH_MAX], pathbuf[PATH_MAX], *rpath;
	char tmppath[PATH_MAX], tmppathbuf[PATH_MAX], *rtmppath;
#if PAGELIST_DEBUG
	char rnpath[PATH_MAX], rnpathbuf[PATH_MAX], *rrnpath;
#endif
	SBUF2 *s;

	if (!force && thresh > 0) {
		u_int64_t target = ((memp_pagecount * thresh) / 100);
#if PAGELIST_DEBUG
		logmsg(LOGMSG_USER, "%s memp_pagecount is %"PRIu64
				" and thresh is %d, target is %"PRIu64", pgreads is %"
				PRIu64"\n", __func__, memp_pagecount, thresh, target,
				gbl_memp_pgreads);
#endif
		if (gbl_memp_pgreads < target) {
			return 0;
		}
	}
	Pthread_mutex_lock(&page_flush_lk);

	snprintf(path, sizeof(path), "%s/%s", dbenv->db_home, PAGELIST);
	rpath = bdb_trans(path, pathbuf);
	snprintf(tmppath, sizeof(tmppath), "%s/%s", dbenv->db_home,
			PAGELISTTEMP);
	rtmppath = bdb_trans(tmppath, tmppathbuf);
	if ((fd = open(rtmppath, O_WRONLY | O_TRUNC | O_CREAT, 0666)) < 0 ||
			(s = sbuf2open(fd, 0)) == NULL) {
#if PAGELIST_DEBUG
		logmsg(LOGMSG_ERROR, "%s line %d error opening %s, %d\n", __func__,
				__LINE__, rtmppath, errno);
#endif
		if (fd >= 0)
			close(fd);
		ret = -1;
		goto done;
	}
	__memp_dump(dbenv, s, gbl_dump_cache_max_pages, &cnt);
	sbuf2close(s);

#if PAGELIST_DEBUG
	snprintf(rnpath, sizeof(rnpath), "%s/%s.%d", dbenv->db_home, PAGELIST,
			count++);
	rrnpath = bdb_trans(rnpath, rnpathbuf);
	rename(rpath, rrnpath);
	logmsg(LOGMSG_USER, "%s dumped cache %s %"PRIu64" pages, rename returns "
			"%d\n", __func__, rpath, cnt, ret);
#endif
	ret = rename(rtmppath, rpath);

done:
	memp_pagecount = cnt;
	gbl_memp_pgreads = 0;
	Pthread_mutex_unlock(&page_flush_lk);
	return ret;
}



static int
__bhcmp(p1, p2)
	const void *p1, *p2;
{
	BH_TRACK *bhp1, *bhp2;

	bhp1 = (BH_TRACK *)p1;
	bhp2 = (BH_TRACK *)p2;

	/* Sort by file (shared memory pool offset). */
	if (bhp1->track_off < bhp2->track_off)
		return (-1);
	if (bhp1->track_off > bhp2->track_off)
		return (1);

	/*
	 * !!!
	 * Defend against badly written quicksort code calling the comparison
	 * function with two identical pointers (e.g., WATCOM C++ (Power++)).
	 */
	if (bhp1->track_pgno < bhp2->track_pgno)
		return (-1);
	if (bhp1->track_pgno > bhp2->track_pgno)
		return (1);
	return (0);
}

static int
__bhlru(p1, p2)
	const void *p1, *p2;
{
	BH_TRACK *bhp1, *bhp2;

	bhp1 = (BH_TRACK *)p1;
	bhp2 = (BH_TRACK *)p2;

	/* Sort by priority.  */
	if (bhp1->track_prio < bhp2->track_prio)
		return (-1);
	if (bhp1->track_prio > bhp2->track_prio)
		return (1);

	return (0);
}
