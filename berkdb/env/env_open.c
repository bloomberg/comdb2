/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"
#include "dbinc/db_swap.h"
#include <pthread.h>

#ifndef lint
static const char revid[] = "$Id: env_open.c,v 11.144 2003/09/13 18:39:34 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#endif
#include <unistd.h>
#include <limits.h>

#include <plhash.h>
#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/btree.h"
#include "dbinc/hash.h"
#include "dbinc/fop.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/qam.h"
#include "dbinc/txn.h"

#include "logmsg.h"

static int __db_tmp_open __P((DB_ENV *, u_int32_t, char *, DB_FH **));
static int __dbenv_config __P((DB_ENV *, const char *, u_int32_t));
static int __dbenv_refresh __P((DB_ENV *, u_int32_t, int));
static int __dbenv_remove_int __P((DB_ENV *, const char *, u_int32_t));

/*
 * db_version --
 *	Return version information.
 *
 * EXTERN: char *db_version __P((int *, int *, int *));
 */
char *
db_version(majverp, minverp, patchp)
	int *majverp, *minverp, *patchp;
{
	if (majverp != NULL)
		*majverp = DB_VERSION_MAJOR;
	if (minverp != NULL)
		*minverp = DB_VERSION_MINOR;
	if (patchp != NULL)
		*patchp = DB_VERSION_PATCH;
	return ((char *)DB_VERSION_STRING);
}

/*
 * __dbenv_open --
 *	DB_ENV->open.
 *
 * PUBLIC: int __dbenv_open __P((DB_ENV *, const char *, u_int32_t, int));
 */
int
__dbenv_open(dbenv, db_home, flags, mode)
	DB_ENV *dbenv;
	const char *db_home;
	u_int32_t flags;
	int mode;
{
	DB_MPOOL *dbmp;
	u_int32_t init_flags, orig_flags;
	int rep_check, ret;

	orig_flags = dbenv->flags;
	rep_check = 0;

#undef	OKFLAGS
#define	OKFLAGS								\
	(DB_CREATE | DB_INIT_CDB | DB_INIT_LOCK | DB_INIT_LOG |		\
	DB_INIT_MPOOL | DB_INIT_REP | DB_INIT_TXN | DB_JOINENV |	\
	DB_LOCKDOWN | DB_PRIVATE | DB_RECOVER | DB_RECOVER_FATAL |	\
	DB_SYSTEM_MEM |	DB_THREAD | DB_USE_ENVIRON | DB_USE_ENVIRON_ROOT | \
    DB_ROWLOCKS )
#undef	OKFLAGS_CDB
#define	OKFLAGS_CDB							\
	(DB_CREATE | DB_INIT_CDB | DB_INIT_MPOOL | DB_LOCKDOWN |	\
	DB_PRIVATE | DB_SYSTEM_MEM | DB_THREAD |			\
	DB_USE_ENVIRON | DB_USE_ENVIRON_ROOT)

	/*
	 * Flags saved in the init_flags field of the environment, representing
	 * flags to DB_ENV->set_flags and DB_ENV->open that need to be set.
	 */
#define	DB_INITENV_CDB		0x0001	/* DB_INIT_CDB */
#define	DB_INITENV_CDB_ALLDB	0x0002	/* DB_INIT_CDB_ALLDB */
#define	DB_INITENV_LOCK		0x0004	/* DB_INIT_LOCK */
#define	DB_INITENV_LOG		0x0008	/* DB_INIT_LOG */
#define	DB_INITENV_MPOOL	0x0010	/* DB_INIT_MPOOL */
#define	DB_INITENV_REP		0x0020	/* DB_INIT_REP */
#define	DB_INITENV_TXN		0x0040	/* DB_INIT_TXN */

	if ((ret = __db_fchk(dbenv, "DB_ENV->open", flags, OKFLAGS)) != 0)
		return (ret);
	if (LF_ISSET(DB_INIT_CDB) &&
	    (ret = __db_fchk(dbenv, "DB_ENV->open", flags, OKFLAGS_CDB)) != 0)
		return (ret);
	if ((ret = __db_fcchk(dbenv,
	    "DB_ENV->open", flags, DB_PRIVATE, DB_SYSTEM_MEM)) != 0)
		return (ret);
	if ((ret = __db_fcchk(dbenv,
	    "DB_ENV->open", flags, DB_RECOVER, DB_RECOVER_FATAL)) != 0)
		return (ret);
	if ((ret = __db_fcchk(dbenv, "DB_ENV->open", flags, DB_JOINENV,
	    DB_CREATE | DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL |
	    DB_INIT_REP | DB_INIT_TXN |
	    DB_PRIVATE | DB_RECOVER | DB_RECOVER_FATAL)) != 0)
		return (ret);
	if (LF_ISSET(DB_INIT_REP) && !LF_ISSET(DB_INIT_TXN)) {
		__db_err(dbenv, "Replication must be used with transactions");
		return (EINVAL);
	}
	if (LF_ISSET(DB_INIT_REP) && !LF_ISSET(DB_INIT_LOCK)) {
		__db_err(dbenv, "Replication must be used with locking");
		return (EINVAL);
	}
	if (F_ISSET(dbenv, DB_ENV_TXN_NOT_DURABLE) && !LF_ISSET(DB_INIT_TXN)) {
		__db_err(dbenv,
		    "Setting non-durability only valid with transactions");
		return (EINVAL);
	}

	/*
	 * Currently we support one kind of mutex that is intra-process only,
	 * POSIX 1003.1 pthreads, because a variety of systems don't support
	 * the full pthreads API, and our only alternative is test-and-set.
	 */
#ifdef HAVE_MUTEX_THREAD_ONLY
	if (!LF_ISSET(DB_PRIVATE)) {
		/*
		__db_err(dbenv,
    "Berkeley DB library configured to support only DB_PRIVATE environments");
		*/
		return (EINVAL);
	}
#endif

#if 0
	/* Initialize the DB_ENV structure. */
	if ((ret = __dbenv_config(dbenv, db_home, flags)) != 0)
		goto err;

	/* Convert the DB_ENV->open flags to internal flags. */
	if (LF_ISSET(DB_CREATE))
		F_SET(dbenv, DB_ENV_CREATE);
	if (LF_ISSET(DB_LOCKDOWN))
		F_SET(dbenv, DB_ENV_LOCKDOWN);
	if (LF_ISSET(DB_PRIVATE))
		F_SET(dbenv, DB_ENV_PRIVATE);
	if (LF_ISSET(DB_RECOVER_FATAL))
		F_SET(dbenv, DB_ENV_FATAL);
	if (LF_ISSET(DB_SYSTEM_MEM))
		F_SET(dbenv, DB_ENV_SYSTEM_MEM);
	if (LF_ISSET(DB_THREAD))
		F_SET(dbenv, DB_ENV_THREAD);
	if (LF_ISSET(DB_DIRECT_DB))
		F_SET(dbenv, DB_ENV_DIRECT_DB);

	/* Default permissions are read-write for both owner and group. */
	dbenv->db_mode = mode == 0 ? __db_omode("rwrw--") : mode;
#endif

	/*
	 * If we're doing recovery, destroy the environment so that we create
	 * all the regions from scratch.  I'd like to reuse already created
	 * regions, but that's hard.  We would have to create the environment
	 * region from scratch, at least, as we have no way of knowing if its
	 * linked lists are corrupted.
	 *
	 * I suppose we could set flags while modifying those links, but that
	 * is going to be difficult to get right.  The major concern I have
	 * is if the application stomps the environment with a rogue pointer.
	 * We have no way of detecting that, and we could be forced into a
	 * situation where we start up and then crash, repeatedly.
	 *
	 * Note that we do not check any flags like DB_PRIVATE before calling
	 * remove.  We don't care if the current environment was private or
	 * not, we just want to nail any files that are left-over for whatever
	 * reason, from whatever session.
	 */
	if (LF_ISSET(DB_RECOVER | DB_RECOVER_FATAL))
		if ((ret = __dbenv_remove_int(dbenv, db_home, DB_FORCE)) != 0 ||
		    (ret = __dbenv_refresh(dbenv, orig_flags, 0)) != 0)
			return (ret);
#if 1
	/* Initialize the DB_ENV structure. */
	if ((ret = __dbenv_config(dbenv, db_home, flags)) != 0)
		goto err;

	/* Convert the DB_ENV->open flags to internal flags. */
	if (LF_ISSET(DB_CREATE))
		 F_SET(dbenv, DB_ENV_CREATE);

	if (LF_ISSET(DB_LOCKDOWN))
		F_SET(dbenv, DB_ENV_LOCKDOWN);
	if (LF_ISSET(DB_PRIVATE))
		F_SET(dbenv, DB_ENV_PRIVATE);

	if (LF_ISSET(DB_RECOVER_FATAL))
		F_SET(dbenv, DB_ENV_FATAL);
	if (LF_ISSET(DB_SYSTEM_MEM))
		F_SET(dbenv, DB_ENV_SYSTEM_MEM);
	if (LF_ISSET(DB_THREAD))
		F_SET(dbenv, DB_ENV_THREAD);

	if (LF_ISSET(DB_ROWLOCKS))
		F_SET(dbenv, DB_ENV_ROWLOCKS);

	/* Default permissions are read-write for both owner and group. */
	dbenv->db_mode = mode == 0 ? __db_omode("rwrw--") : mode;
#endif

	/*
	 * Create/join the environment.  We pass in the flags that
	 * will be of interest to an environment joining later;  if
	 * we're not the ones to do the create, we
	 * pull out whatever has been stored.
	 */
	init_flags = 0;
	init_flags |= (LF_ISSET(DB_INIT_CDB) ? DB_INITENV_CDB : 0);
	init_flags |= (LF_ISSET(DB_INIT_LOCK) ? DB_INITENV_LOCK : 0);
	init_flags |= (LF_ISSET(DB_INIT_LOG) ? DB_INITENV_LOG : 0);
	init_flags |= (LF_ISSET(DB_INIT_MPOOL) ? DB_INITENV_MPOOL : 0);
	init_flags |= (LF_ISSET(DB_INIT_REP) ? DB_INITENV_REP : 0);
	init_flags |= (LF_ISSET(DB_INIT_TXN) ? DB_INITENV_TXN : 0);
	init_flags |=
	    (F_ISSET(dbenv, DB_ENV_CDB_ALLDB) ? DB_INITENV_CDB_ALLDB : 0);

	if ((ret = __db_e_attach(dbenv, &init_flags)) != 0)
		goto err;

	/*
	 * __db_e_attach will return the saved init_flags field, which
	 * contains the DB_INIT_* flags used when we were created.
	 */
	if (LF_ISSET(DB_JOINENV)) {
		LF_CLR(DB_JOINENV);

		LF_SET((init_flags & DB_INITENV_CDB) ? DB_INIT_CDB : 0);
		LF_SET((init_flags & DB_INITENV_LOCK) ? DB_INIT_LOCK : 0);
		LF_SET((init_flags & DB_INITENV_LOG) ? DB_INIT_LOG : 0);
		LF_SET((init_flags & DB_INITENV_MPOOL) ? DB_INIT_MPOOL : 0);
		LF_SET((init_flags & DB_INITENV_REP) ? DB_INIT_REP : 0);
		LF_SET((init_flags & DB_INITENV_TXN) ? DB_INIT_TXN : 0);

		if (LF_ISSET(DB_INITENV_CDB_ALLDB) &&
		    (ret = __dbenv_set_flags(dbenv, DB_CDB_ALLDB, 1)) != 0)
			goto err;
	}

	/* Initialize for CDB product. */
	if (LF_ISSET(DB_INIT_CDB)) {
		LF_SET(DB_INIT_LOCK);
		F_SET(dbenv, DB_ENV_CDB);
	}
	if (LF_ISSET(DB_RECOVER |
	     DB_RECOVER_FATAL) && !LF_ISSET(DB_INIT_TXN)) {
		__db_err(dbenv,
    "DB_RECOVER and DB_RECOVER_FATAL require DB_TXN_INIT in DB_ENV->open");
		ret = EINVAL;
		goto err;
	}
	/* Save the flags passed to DB_ENV->open. */
	dbenv->open_flags = flags;

    ret = pthread_rwlock_init(&dbenv->dbreglk, NULL);
    if (ret) {
        __db_err(dbenv, "Can't create dbreglk lock %d %s", ret, strerror(ret));
        goto err;
    }

	/*
	 * Initialize the subsystems.
	 *
	 * Initialize the replication area first, so that we can lock out this
	 * call if we're currently running recovery for replication.
	 */
	if (LF_ISSET(DB_INIT_REP)) {
		if ((ret = __rep_open(dbenv)) != 0)
			goto err;
	}


	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);

	if (LF_ISSET(DB_INIT_MPOOL))
		if ((ret = __memp_open(dbenv)) != 0)
			goto err;
	/*
	 * Initialize the ciphering area prior to any running of recovery so
	 * that we can initialize the keys, etc. before recovery.
	 *
	 * !!!
	 * This must be after the mpool init, but before the log initialization
	 * because log_open may attempt to run log_recover during its open.
	 */
	if ((ret = __crypto_region_init(dbenv)) != 0)
		goto err;

	/*
	 * Transactions imply logging but do not imply locking.  While almost
	 * all applications want both locking and logging, it would not be
	 * unreasonable for a single threaded process to want transactions for
	 * atomicity guarantees, but not necessarily need concurrency.
	 */
	if (LF_ISSET(DB_INIT_LOG | DB_INIT_TXN))
		if ((ret = __log_open(dbenv)) != 0)
			goto err;
	if (LF_ISSET(DB_INIT_LOCK))
		if ((ret = __lock_open(dbenv)) != 0)
			goto err;

	/* Init this part before txn's */
	if (LF_ISSET(DB_INIT_REP)) {
		dbenv->ltrans_hash = hash_init(sizeof(u_int64_t));
		pthread_mutex_init(&dbenv->ltrans_hash_lk, NULL);
		listc_init(&dbenv->active_ltrans,
		    offsetof(struct __ltrans_descriptor, lnk));
		listc_init(&dbenv->inactive_ltrans,
		    offsetof(struct __ltrans_descriptor, lnk));
		pthread_mutex_init(&dbenv->ltrans_inactive_lk, NULL);
		pthread_mutex_init(&dbenv->ltrans_active_lk, NULL);
		pthread_mutex_init(&dbenv->locked_lsn_lk, NULL);
	}

	if (LF_ISSET(DB_INIT_TXN)) {
		if ((ret = __txn_open(dbenv)) != 0)
			goto err;

		/*
		 * If the application is running with transactions, initialize
		 * the function tables.
		 */
		if ((ret = __bam_init_recover(dbenv, &dbenv->recover_dtab,
		    &dbenv->recover_dtab_size)) != 0)
			goto err;
		if ((ret = __crdel_init_recover(dbenv, &dbenv->recover_dtab,
		    &dbenv->recover_dtab_size)) != 0)
			goto err;
		if ((ret = __db_init_recover(dbenv, &dbenv->recover_dtab,
		    &dbenv->recover_dtab_size)) != 0)
			goto err;
		if ((ret = __dbreg_init_recover(dbenv, &dbenv->recover_dtab,
		    &dbenv->recover_dtab_size)) != 0)
			goto err;
		if ((ret = __fop_init_recover(dbenv, &dbenv->recover_dtab,
		    &dbenv->recover_dtab_size)) != 0)
			goto err;
		if ((ret = __ham_init_recover(dbenv, &dbenv->recover_dtab,
		    &dbenv->recover_dtab_size)) != 0)
			goto err;
		if ((ret = __qam_init_recover(dbenv, &dbenv->recover_dtab,
		    &dbenv->recover_dtab_size)) != 0)
			goto err;
		if ((ret = __txn_init_recover(dbenv, &dbenv->recover_dtab,
		    &dbenv->recover_dtab_size)) != 0)
			goto err;

        /* Also the getallpgnos dtab */
		if ((ret = __bam_init_getallpgnos(dbenv, &dbenv->pgnos_dtab,
		    &dbenv->pgnos_dtab_size)) != 0)
			goto err;
		if ((ret = __crdel_init_getallpgnos(dbenv, &dbenv->pgnos_dtab,
		    &dbenv->pgnos_dtab_size)) != 0)
			goto err;
		if ((ret = __db_init_getallpgnos(dbenv, &dbenv->pgnos_dtab,
		    &dbenv->pgnos_dtab_size)) != 0)
			goto err;
		if ((ret = __dbreg_init_getallpgnos(dbenv, &dbenv->pgnos_dtab,
		    &dbenv->pgnos_dtab_size)) != 0)
			goto err;
		if ((ret = __fop_init_getallpgnos(dbenv, &dbenv->pgnos_dtab,
		    &dbenv->pgnos_dtab_size)) != 0)
			goto err;
		if ((ret = __ham_init_getallpgnos(dbenv, &dbenv->pgnos_dtab,
		    &dbenv->pgnos_dtab_size)) != 0)
			goto err;
		if ((ret = __qam_init_getallpgnos(dbenv, &dbenv->pgnos_dtab,
		    &dbenv->pgnos_dtab_size)) != 0)
			goto err;
		if ((ret = __txn_init_getallpgnos(dbenv, &dbenv->pgnos_dtab,
		    &dbenv->pgnos_dtab_size)) != 0)
			goto err;

		/* Open (or create) the checkpoint file, place handle in dbenv. */
		if ((ret = __checkpoint_open(dbenv, db_home)) != 0)
			goto err;

		/* Perform recovery for any previous run. */
		if (LF_ISSET(DB_RECOVER | DB_RECOVER_FATAL)) {
			extern int gbl_recovery_lsn_file, gbl_recovery_lsn_offset, 
				   gbl_recovery_timestamp;
			DB_LSN maxlsn={0}, lsn;

			if (gbl_recovery_lsn_file != 0) {
				maxlsn.file = gbl_recovery_lsn_file;
				maxlsn.offset = gbl_recovery_lsn_offset;
				ret = __db_apprec(dbenv, &maxlsn, &maxlsn, 1, 
						LF_ISSET(DB_RECOVER | DB_RECOVER_FATAL));
			}
			else if (gbl_recovery_timestamp != 0) {
				DB_LOGC *logc;
				__txn_regop_args *regop;
				__txn_regop_gen_args *regopgen;
				__txn_regop_rowlocks_args *regoprowlocks;
				u_int32_t rectype;
				int32_t timestamp=0;
				DBT data;

				if ((ret = __log_cursor(dbenv, &logc)) != 0)
					goto err;

				for (ret = __log_c_get(logc, &lsn, &data, DB_LAST);
						ret == 0; ret = __log_c_get(logc, &lsn, 
							&data, DB_PREV)) {
					LOGCOPY_32(&rectype, data.data);
					switch (rectype)
					{
						case (DB___txn_regop):
							if ((ret = __txn_regop_read(dbenv, data.data, 
											&regop))!=0)
								goto err;
							timestamp = regop->timestamp;
							__os_free(dbenv, regop);
							
							if (timestamp <= gbl_recovery_timestamp) {
								maxlsn.file = lsn.file;
								maxlsn.offset = lsn.offset;
								goto foundlsn;
							}
							break;
						case (DB___txn_regop_gen):
							if ((ret = __txn_regop_gen_read(dbenv, data.data, 
											&regopgen))!=0)
								goto err;
							timestamp = regopgen->timestamp;
							__os_free(dbenv, regopgen);
							
							if (timestamp <= gbl_recovery_timestamp) {
								maxlsn.file = lsn.file;
								maxlsn.offset = lsn.offset;
								goto foundlsn;
							}
							break;
						case (DB___txn_regop_rowlocks):
							if ((ret = __txn_regop_rowlocks_read(dbenv, 
											data.data, &regoprowlocks)) != 0)
								goto err;
							timestamp = regoprowlocks->timestamp;
							__os_free(dbenv, regoprowlocks);

							if (timestamp <= gbl_recovery_timestamp) {
								maxlsn.file = lsn.file;
								maxlsn.offset = lsn.offset;
								goto foundlsn;
							}
							break;
					}
				}
foundlsn:
				if ((ret = __log_c_close(logc)) != 0)
					goto err;
				
				if (maxlsn.file == 0) {
					logmsg(LOGMSG_FATAL, "Invalid recovery timestamp, oldest is %u at lsn %d:%d\n",
							timestamp, lsn.file, lsn.offset);
					exit(1);
				}
				else {
					ret = __db_apprec(dbenv, &maxlsn, &maxlsn, 1, 
							LF_ISSET(DB_RECOVER | DB_RECOVER_FATAL));
				}


			}
			else {
				ret = __db_apprec(dbenv, NULL, NULL, 1,
						LF_ISSET(DB_RECOVER | DB_RECOVER_FATAL));
			}
			if (ret != 0) 
				goto err;
		}
	}

	/*
	 * Initialize the DB list, and its mutex as necessary.  If the env
	 * handle isn't free-threaded we don't need a mutex because there
	 * will never be more than a single DB handle on the list.  If the
	 * mpool wasn't initialized, then we can't ever open a DB handle.
	 *
	 * We also need to initialize the MT mutex as necessary, so do them
	 * both.  If we error, __dbenv_refresh() will clean up.
	 *
	 * !!!
	 * This must come after the __memp_open call above because if we are
	 * recording mutexes for system resources, we will do it in the mpool
	 * region for environments and db handles.  So, the mpool region must
	 * already be initialized.
	 */
	{
		int i;
		DB *db;

		for (i = 1; i < dbenv->maxdb; i++) {
			db = listc_rtl(&dbenv->dbs[i]);
			while (db) {
				db->inadjlist = 0;
				db = listc_rtl(&dbenv->dbs[i]);
			}
		}
	}
	LIST_INIT(&dbenv->dblist);
	if (F_ISSET(dbenv, DB_ENV_THREAD) && LF_ISSET(DB_INIT_MPOOL)) {
		dbmp = dbenv->mp_handle;
		if ((ret =
			__db_mutex_setup(dbenv, dbmp->reginfo,
			    &dbenv->dblist_mutexp,
			    MUTEX_ALLOC |MUTEX_THREAD)) != 0)
			 goto err;
		if ((ret =
			__db_mutex_setup(dbenv, dbmp->reginfo,
			    &dbenv->mt_mutexp, MUTEX_ALLOC |MUTEX_THREAD)) != 0)
			 goto err;
	}

	/*
	 * If we've created the regions, are running with transactions, and did
	 * not just run recovery, we need to log the fact that the transaction
	 * IDs got reset.
	 *
	 * If we ran recovery, there may be prepared-but-not-yet-committed
	 * transactions that need to be resolved.  Recovery resets the minimum
	 * transaction ID and logs the reset if that's appropriate, so we
	 * don't need to do anything here in the recover case.
	 */
	if (TXN_ON(dbenv) &&
	    F_ISSET((REGINFO *)dbenv->reginfo, REGION_CREATE) &&
	    !LF_ISSET(DB_RECOVER | DB_RECOVER_FATAL) &&
	    (ret = __txn_reset(dbenv)) != 0)
		goto err;

	if (dbenv->num_recovery_processor_threads > 0 &&
	    dbenv->num_recovery_worker_threads == 0) {
		__db_err(dbenv,
		    "Have recovery processor threads, but not worker threads");
		ret = EINVAL;

		goto err;
	}

	/* Parallel recovery stuff */
	if (LF_ISSET(DB_INIT_REP)) {
		dbenv->recovery_processors =
		    thdpool_create("recovery_processors", 0);
		thdpool_set_maxthds(dbenv->recovery_processors,
		    dbenv->num_recovery_processor_threads);
		thdpool_set_linger(dbenv->recovery_processors, 30);
		thdpool_set_maxqueue(dbenv->recovery_processors, 0);
		thdpool_set_wait(dbenv->recovery_processors, 1);
		dbenv->recovery_workers = thdpool_create("recovery_workers", 0);
		thdpool_set_maxthds(dbenv->recovery_workers,
		    dbenv->num_recovery_worker_threads);
		thdpool_set_linger(dbenv->recovery_workers, 30);
		thdpool_set_maxqueue(dbenv->recovery_workers, 8000);
		pthread_mutex_init(&dbenv->recover_lk, NULL);
		pthread_cond_init(&dbenv->recover_cond, NULL);
		pthread_rwlock_init(&dbenv->ser_lk, NULL);
		listc_init(&dbenv->inflight_transactions,
		    offsetof(struct __recovery_processor, lnk));
		listc_init(&dbenv->inactive_transactions,
		    offsetof(struct __recovery_processor, lnk));
		dbenv->recovery_memsize = 512 * 1024;	/* 512k by default */
	}

	if (dbenv->attr.iomapfile) {
		DBENV_MAP map;
		size_t nwp;

		map.memptrickle_active = 0;
		dbenv->iomap = NULL;

		ret =
		    __os_open(dbenv, dbenv->attr.iomapfile,
		    DB_OSO_CREATE |DB_OSO_TRUNC, 0666, &dbenv->iomap_fd);
		if (ret)
			__db_err(dbenv,
			    "can't open/create iomap file %s (not fatal)\n",
			    dbenv->attr.iomapfile);
		if (ret == 0) {
			ret =
			    __os_write(dbenv, dbenv->iomap_fd, &map,
			    sizeof(DBENV_MAP), &nwp);
			if (ret)
				__db_err(dbenv,
				    "can't init iomap file %s (not fatal)\n",
				    dbenv->attr.iomapfile);
			if (ret == 0) {
				ret =
				    __os_mapfile(dbenv, NULL, dbenv->iomap_fd,
				    sizeof(DBENV_MAP), 0,
				    (void **)&dbenv->iomap);
				if (ret)
					__db_err(dbenv,
					    "can't mmap iomap file %s (not fatal)\n",
					    dbenv->attr.iomapfile);
				if (ret != 0)
					dbenv->iomap = NULL;
			}
		}
		if (dbenv->iomap)
			dbenv->iomap->memptrickle_active = 0;
	}

	if (rep_check)
		__env_rep_exit(dbenv);

	if ((ret = __lc_cache_init(dbenv, 0)) != 0)
		goto err;

	dbenv->verbose |= DB_VERB_REPLICATION;

	return (0);

err:				/* If we fail after creating the regions, remove them. */
	if (dbenv->reginfo != NULL &&F_ISSET((REGINFO *)dbenv->reginfo,
		REGION_CREATE)) {
		ret = __db_panic(dbenv, ret);

		/* Refresh the DB_ENV so we can use it to call remove. */
		(void)__dbenv_refresh(dbenv, orig_flags, rep_check);
		(void)__dbenv_remove_int(dbenv, db_home, DB_FORCE);
		(void)__dbenv_refresh(dbenv, orig_flags, 0);
	} else
		(void)__dbenv_refresh(dbenv, orig_flags, rep_check);

	return (ret);
}

/*
 * __dbenv_remove --
 *	DB_ENV->remove.
 *
 * PUBLIC: int __dbenv_remove __P((DB_ENV *, const char *, u_int32_t));
 */
int
__dbenv_remove(dbenv, db_home, flags)
	DB_ENV *dbenv;
	const char *db_home;
	u_int32_t flags;
{
	int ret, t_ret;

#undef	OKFLAGS
#define	OKFLAGS								\
	(DB_FORCE | DB_USE_ENVIRON | DB_USE_ENVIRON_ROOT)

	/* Validate arguments. */
	if ((ret = __db_fchk(dbenv, "DB_ENV->remove", flags, OKFLAGS)) != 0)
		return (ret);

	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->remove");

	ret = __dbenv_remove_int(dbenv, db_home, flags);

	if ((t_ret = __dbenv_close(dbenv, 0)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}

/*
 * __dbenv_remove_int --
 *	Discard an environment, internal version.
 */
static int
__dbenv_remove_int(dbenv, db_home, flags)
	DB_ENV *dbenv;
	const char *db_home;
	u_int32_t flags;
{
	int ret;

	/* Initialize the DB_ENV structure. */
	if ((ret = __dbenv_config(dbenv, db_home, flags)) != 0)
		return (ret);

	/* Remove the environment. */
	return (__db_e_remove(dbenv, flags));
}

/*
 * __dbenv_config --
 *	Minor initialization of the DB_ENV structure, read the DB_CONFIG file.
 */
static int
__dbenv_config(dbenv, db_home, flags)
	DB_ENV *dbenv;
	const char *db_home;
	u_int32_t flags;
{
	FILE *fp;
	int ret;
	char *p, buf[256];

	/*
	 * Set the database home.  Do this before calling __db_appname,
	 * it uses the home directory.
	 */
	if ((ret = __db_home(dbenv, db_home, flags)) != 0)
		return (ret);

	/*
	 * If no temporary directory path was specified in the config file,
	 * choose one.
	 */
	if (dbenv->db_tmp_dir == NULL && (ret = __os_tmpdir(dbenv, flags)) != 0)
		return (ret);

	/* Flag that the DB_ENV structure has been initialized. */
	F_SET(dbenv, DB_ENV_OPEN_CALLED);

	return (0);
}

/*
 * __dbenv_close_pp --
 *	DB_ENV->close pre/post processor.
 *
 * PUBLIC: int __dbenv_close_pp __P((DB_ENV *, u_int32_t));
 */
int
__dbenv_close_pp(dbenv, flags)
	DB_ENV *dbenv;
	u_int32_t flags;
{
	int rep_check, ret, t_ret;

	ret = 0;

	PANIC_CHECK(dbenv);

	/*
	 * Validate arguments, but as a DB_ENV handle destructor, we can't
	 * fail.
	 */
	if (flags != 0 &&
	    (t_ret = __db_ferr(dbenv, "DB_ENV->close", 0)) != 0 && ret == 0)
		ret = t_ret;

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);

	if ((t_ret = __dbenv_close(dbenv, rep_check)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}

/*
 * __dbenv_close --
 *	DB_ENV->close.
 *
 * PUBLIC: int __dbenv_close __P((DB_ENV *, int));
 */
int
__dbenv_close(dbenv, rep_check)
	DB_ENV *dbenv;
	int rep_check;
{
	int ret, t_ret;
	char **p;

	ret = 0;

	/*
	 * Before checking the reference count, we have to see if we were in
	 * the middle of restoring transactions and need to close the open
	 * files.
	 */
	if (TXN_ON(dbenv) && (t_ret = __txn_preclose(dbenv)) != 0 && ret == 0)
		ret = t_ret;

	if (REP_ON(dbenv) &&
	    (t_ret = __rep_preclose(dbenv, 1)) != 0 && ret == 0)
		ret = t_ret;

	if (dbenv->db_ref != 0) {
		__db_err(dbenv,
		    "Database handles open during environment close");
		if (ret == 0)
			ret = EINVAL;
	}

	/*
	 * Detach from the regions and undo the allocations done by
	 * DB_ENV->open.
	 */
	if ((t_ret = __dbenv_refresh(dbenv, 0, rep_check)) != 0 && ret == 0)
		ret = t_ret;

	/* Do per-subsystem destruction. */
	__lock_dbenv_close(dbenv);

	if ((t_ret = __rep_dbenv_close(dbenv)) != 0 && ret == 0)
		ret = t_ret;

#ifdef HAVE_CRYPTO
	/*
	 * Crypto comes last, because higher level close functions needs
	 * cryptography.
	 */
	if ((t_ret = __crypto_dbenv_close(dbenv)) != 0 && ret == 0)
		ret = t_ret;
#endif

	/* Release any string-based configuration parameters we've copied. */
	if (dbenv->db_log_dir != NULL)
		__os_free(dbenv, dbenv->db_log_dir);
	if (dbenv->db_tmp_dir != NULL)
		__os_free(dbenv, dbenv->db_tmp_dir);
	if (dbenv->db_data_dir != NULL) {
		for (p = dbenv->db_data_dir; *p != NULL; ++p)
			__os_free(dbenv, *p);
		__os_free(dbenv, dbenv->db_data_dir);
	}

	if (dbenv->comdb2_dirs.data_dir != NULL)
		__os_free(dbenv, dbenv->comdb2_dirs.data_dir);
	if (dbenv->comdb2_dirs.txn_dir != NULL)
		__os_free(dbenv, dbenv->comdb2_dirs.txn_dir);
	if (dbenv->comdb2_dirs.tmp_dir != NULL)
		__os_free(dbenv, dbenv->comdb2_dirs.tmp_dir);

	if (dbenv->ltrans_hash != NULL) {
        hash_clear(dbenv->ltrans_hash);
        hash_free(dbenv->ltrans_hash);
    }

	/* Release DB list */
	__os_free(dbenv, dbenv->dbs);

	/* Release lc_cache */
	__lc_cache_destroy(dbenv);

	/* Discard the structure. */
	memset(dbenv, CLEAR_BYTE, sizeof(DB_ENV));
	__os_free(NULL, dbenv);


	return (ret);
}

/*
 * __dbenv_refresh --
 *	Refresh the DB_ENV structure, releasing resources allocated by
 * DB_ENV->open, and returning it to the state it was in just before
 * open was called.  (Note that this means that any state set by
 * pre-open configuration functions must be preserved.)
 */
static int
__dbenv_refresh(dbenv, orig_flags, rep_check)
	DB_ENV *dbenv;
	u_int32_t orig_flags;
	int rep_check;
{
	DB_MPOOL *dbmp;
	int ret, t_ret;

	ret = 0;

	/*
	 * Close subsystems, in the reverse order they were opened (txn
	 * must be first, it may want to discard locks and flush the log).
	 *
	 * !!!
	 * Note that these functions, like all of __dbenv_refresh, only undo
	 * the effects of __dbenv_open.  Functions that undo work done by
	 * db_env_create or by a configurator function should go in
	 * __dbenv_close.
	 */
	if (TXN_ON(dbenv) &&
	    (t_ret = __txn_dbenv_refresh(dbenv)) != 0 && ret == 0)
		ret = t_ret;

	if (LOGGING_ON(dbenv) &&
	    (t_ret = __log_dbenv_refresh(dbenv)) != 0 && ret == 0)
		ret = t_ret;

	/*
	 * Locking should come after logging, because closing log results
	 * in files closing which may require locks being released.
	 */
	if (LOCKING_ON(dbenv) &&
	    (t_ret = __lock_dbenv_refresh(dbenv)) != 0 && ret == 0)
		ret = t_ret;

	/*
	 * Discard DB list and its mutex.
	 * Discard the MT mutex.
	 *
	 * !!!
	 * This must be done before we close the mpool region because we
	 * may have allocated the DB handle mutex in the mpool region.
	 * It must be done *after* we close the log region, though, because
	 * we close databases and try to acquire the mutex when we close
	 * log file handles.  Ick.
	 */
	LIST_INIT(&dbenv->dblist);
	if (dbenv->dblist_mutexp != NULL) {
		dbmp = dbenv->mp_handle;
		__db_mutex_free(dbenv, dbmp->reginfo, dbenv->dblist_mutexp);
	}
	if (dbenv->mt_mutexp != NULL) {
		dbmp = dbenv->mp_handle;
		__db_mutex_free(dbenv, dbmp->reginfo, dbenv->mt_mutexp);
	}
	if (dbenv->mt != NULL) {
		__os_free(dbenv, dbenv->mt);
		dbenv->mt = NULL;
	}

	if (MPOOL_ON(dbenv)) {
		/*
		 * If it's a private environment, flush the contents to disk.
		 * Recovery would have put everything back together, but it's
		 * faster and cleaner to flush instead.
		 */
		{
			if (F_ISSET(dbenv, DB_ENV_PRIVATE) &&
			    (t_ret = __memp_sync(dbenv, NULL)) != 0 && ret == 0)
				ret = t_ret;

			if ((t_ret = __memp_dbenv_refresh(dbenv)) != 0 &&
			    ret == 0)
				ret = t_ret;
		}
	}

skip:


	/*
	 * If we're included in a shared replication handle count, this
	 * is our last chance to decrement that count.
	 *
	 * !!!
	 * We can't afford to do anything dangerous after we decrement the
	 * handle count, of course, as replication may be proceeding with
	 * client recovery.  However, since we're discarding the regions
	 * as soon as we drop the handle count, there's little opportunity
	 * to do harm.
	 */
	if (rep_check)
		__env_rep_exit(dbenv);

	/* Detach from the region. */
	/*
	 * Must come after we call __env_rep_exit above.
	 */
	__rep_dbenv_refresh(dbenv);

	if (dbenv->reginfo != NULL) {
		if ((t_ret = __db_e_detach(dbenv, 0)) != 0 && ret == 0)
			ret = t_ret;
		/*
		 * !!!
		 * Don't free dbenv->reginfo or set the reference to NULL,
		 * that was done by __db_e_detach().
		 */
	}

	/* Undo changes and allocations done by __dbenv_open. */
	if (dbenv->db_home != NULL) {
		__os_free(dbenv, dbenv->db_home);
		dbenv->db_home = NULL;
	}

	dbenv->open_flags = 0;
	dbenv->db_mode = 0;

	if (dbenv->recover_dtab != NULL) {
		__os_free(dbenv, dbenv->recover_dtab);
		dbenv->recover_dtab = NULL;
		dbenv->recover_dtab_size = 0;
	}
	if (dbenv->pgnos_dtab != NULL) {
		__os_free(dbenv, dbenv->pgnos_dtab);
		dbenv->pgnos_dtab = NULL;
		dbenv->pgnos_dtab_size = 0;
	}

	dbenv->flags = orig_flags;

	if (dbenv->checkpoint) {
		__os_closehandle(dbenv, dbenv->checkpoint);
		dbenv->checkpoint = NULL;
	}

	return (ret);
}

#define	DB_ADDSTR(add) {						\
	if ((add) != NULL) {						\
		/* If leading slash, start over. */			\
		if (__os_abspath(add)) {				\
			p = str;					\
			slash = 0;					\
		}							\
		/* Append to the current string. */			\
		len = strlen(add);					\
		if (slash)						\
			*p++ = PATH_SEPARATOR[0];			\
		memcpy(p, add, len);					\
		p += len;						\
		slash = strchr(PATH_SEPARATOR, p[-1]) == NULL;		\
	}								\
}

/*
 * __dbenv_get_open_flags
 *	Retrieve the flags passed to DB_ENV->open.
 *
 * PUBLIC: int __dbenv_get_open_flags __P((DB_ENV *, u_int32_t *));
 */
int
__dbenv_get_open_flags(dbenv, flagsp)
	DB_ENV *dbenv;
	u_int32_t *flagsp;
{
	ENV_ILLEGAL_BEFORE_OPEN(dbenv, "DB_ENV->get_open_flags");

	*flagsp = dbenv->open_flags;
	return (0);
}

/*
 * __db_appname --
 *	Given an optional DB environment, directory and file name and type
 *	of call, build a path based on the DB_ENV->open rules, and return
 *	it in allocated space.
 *
 * PUBLIC: int __db_appname __P((DB_ENV *, APPNAME,
 * PUBLIC:    const char *, u_int32_t, DB_FH **, char **));
 */
int
__db_appname(dbenv, appname, file, tmp_oflags, fhpp, namep)
	DB_ENV *dbenv;
	APPNAME appname;
	const char *file;
	u_int32_t tmp_oflags;
	DB_FH **fhpp;
	char **namep;
{
	size_t len, str_len;
	int data_entry, ret, slash, tmp_create;
	const char *a, *b;
	char *p, *str;

	a = b = NULL;
	data_entry = -1;
	tmp_create = 0;

	/*
	 * We don't return a name when creating temporary files, just a file
	 * handle.  Default to an error now.
	 */
	if (fhpp != NULL)
		*fhpp = NULL;
	if (namep != NULL)
		*namep = NULL;

	/*
	 * Absolute path names are never modified.  If the file is an absolute
	 * path, we're done.
	 */
	if (file != NULL && __os_abspath(file))
		return (__os_strdup(dbenv, file, namep));

	/* Everything else is relative to the environment home. */
	if (dbenv != NULL)
		a = dbenv->db_home;

retry:	/*
	 * DB_APP_NONE:
	 *      DB_HOME/file
	 * DB_APP_DATA:
	 *      DB_HOME/DB_DATA_DIR/file
	 * DB_APP_LOG:
	 *      DB_HOME/DB_LOG_DIR/file
	 * DB_APP_TMP:
	 *      DB_HOME/DB_TMP_DIR/<create>
	 */
	switch (appname) {
	case DB_APP_NONE:
		break;
	case DB_APP_DATA:
		if (dbenv != NULL && dbenv->db_data_dir != NULL &&
		    (b = dbenv->db_data_dir[++data_entry]) == NULL) {
			data_entry = -1;
			b = dbenv->db_data_dir[0];
		}
		break;
	case DB_APP_LOG:
		if (dbenv != NULL)
			b = dbenv->db_log_dir;
		break;
	case DB_APP_TMP:
		if (dbenv != NULL)
			b = dbenv->db_tmp_dir;
		tmp_create = 1;
		break;
	}

	len =
	    (a == NULL ? 0 : strlen(a) + 1) +
	    (b == NULL ? 0 : strlen(b) + 1) +
	    (file == NULL ? 0 : strlen(file) + 1);

	/*
	 * Allocate space to hold the current path information, as well as any
	 * temporary space that we're going to need to create a temporary file
	 * name.
	 */
#define	DB_TRAIL	"/_temp_XXXXXXXXXX.db"
	str_len = len + sizeof(DB_TRAIL) + 10 + strlen(dbenv->db_tmp_dir);
	if ((ret = __os_malloc(dbenv, str_len, &str)) != 0)
		return (ret);

	slash = 0;
	p = str;
	DB_ADDSTR(a);
	DB_ADDSTR(b);
	DB_ADDSTR(file);
	*p = '\0';

	/*
	 * If we're opening a data file, see if it exists.  If it does,
	 * return it, otherwise, try and find another one to open.
	 */
#if 0

	if (__os_exists(str, NULL) != 0 && data_entry != -1) {
		__os_free(dbenv, str);
		b = NULL;
		goto retry;
	}
#endif

	/* Create the file if so requested. */
	if (tmp_create &&
	    (ret = __db_tmp_open(dbenv, tmp_oflags, str, fhpp)) != 0) {
		__os_free(dbenv, str);
		return (ret);
	}

	if (namep == NULL)
		__os_free(dbenv, str);
	else
		*namep = str;
	return (0);
}

/*
 * __db_home --
 *	Find the database home.
 *
 * PUBLIC:	int __db_home __P((DB_ENV *, const char *, u_int32_t));
 */
int
__db_home(dbenv, db_home, flags)
	DB_ENV *dbenv;
	const char *db_home;
	u_int32_t flags;
{
	const char *p;

	/*
	 * Use db_home by default, this allows utilities to reasonably
	 * override the environment either explicitly or by using a -h
	 * option.  Otherwise, use the environment if it's permitted
	 * and initialized.
	 */
	if ((p = db_home) == NULL &&
	    (LF_ISSET(DB_USE_ENVIRON) ||
	    (LF_ISSET(DB_USE_ENVIRON_ROOT) && __os_isroot())) &&
	    (p = getenv("DB_HOME")) != NULL && p[0] == '\0') {
		__db_err(dbenv, "illegal DB_HOME environment variable");
		return (EINVAL);
	}

	return (p == NULL ? 0 : __os_strdup(dbenv, p, &dbenv->db_home));
}

#define	__DB_OVFL(v, max)						\
	if (v > max) {							\
		__v = v;						\
		__max = max;						\
		goto toobig;						\
	}


static pthread_mutex_t tmp_open_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * __db_tmp_open --
 *	Create a temporary file.
 */
static int
__db_tmp_open(dbenv, tmp_oflags, path, fhpp)
	DB_ENV *dbenv;
	u_int32_t tmp_oflags;
	char *path;
	DB_FH **fhpp;
{
	u_int32_t id;
	int mode, isdir, ret;
	const char *p;
	char *trv;
	static pthread_mutex_t mutex_copy_before;
	static pthread_mutex_t mutex_copy_after;

	int x;
	static int num;

	memcpy(&mutex_copy_before, &tmp_open_lock, sizeof(mutex_copy_before));
	pthread_mutex_lock(&tmp_open_lock);
	num++;
	x = num;
	pthread_mutex_unlock(&tmp_open_lock);
	memcpy(&mutex_copy_after, &tmp_open_lock, sizeof(mutex_copy_after));

	/*   
	 * x = __sync_fetch_and_add(&num, 1);
	 */

	sprintf(path, "%s/_temp_%d.db", dbenv->db_tmp_dir, x);


	/* Set up open flags and mode. */
	mode = __db_omode("rw----");

	if ((ret = __os_open(dbenv, path,
		    tmp_oflags | DB_OSO_CREATE |DB_OSO_EXCL |DB_OSO_TEMP,
		    mode, fhpp)) == 0)
		return (0);


	__db_err(dbenv, "tmp_open: %s: %s", path, db_strerror(ret));
	return (ret);
}


/* NOTE: wrong layer, shouldn't expose os flags here */
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>

#include "dbinc/hmac.h"
#include "dbinc_auto/hmac_ext.h"

extern char *bdb_trans(const char infile[], char outfile[]);
extern int ___os_openhandle(DB_ENV *dbenv, const char *name, int flags,
    int mode, DB_FH ** fhpp);
extern int __os_closehandle(DB_ENV *dbenv, DB_FH * fhp);

int
__checkpoint_open(DB_ENV *dbenv, const char *db_home)
{
	int ret = 0;
	char buf[PATH_MAX];
	char fname[PATH_MAX];
	const char *pbuf;
	struct __db_checkpoint ckpt = { 0 };
	int niop = 0;
	DB_LSN lsn;
	size_t sz;

	snprintf(fname, sizeof(fname), "%s/checkpoint", db_home);
	pbuf = bdb_trans(fname, buf);

	ret =
	    ___os_openhandle(dbenv, pbuf, O_SYNC | O_RDWR, 0666,
	    &dbenv->checkpoint);
	if (ret == ENOENT) {
		DB_TXNMGR *mgr;
		DB_TXNREGION *region;

		mgr = dbenv->tx_handle;
		region = mgr->reginfo.primary;

		if (dbenv->checkpoint)
			__os_closehandle(dbenv, dbenv->checkpoint);
		ret =
		    ___os_openhandle(dbenv, pbuf,
		    O_SYNC | O_RDWR | O_CREAT | O_EXCL, 0666,
		    &dbenv->checkpoint);
		if (ret) {
			__db_err(dbenv, "__checkpoint_open create new rc %d\n",
			    ret);
			goto err;
		}

		/* LSN of last checkpoint. */
		R_LOCK(dbenv, &mgr->reginfo);
		lsn = region->last_ckp;
		R_UNLOCK(dbenv, &mgr->reginfo);

		LOGCOPY_TOLSN(&ckpt.lsn, &lsn);

		/* Write checkpoint. All other writes to the checkpoint file use __checkpoint_save. */
		sz = sizeof(struct __db_checkpoint) -
		    offsetof(struct __db_checkpoint, lsn);
		__db_chksum_no_crypto((u_int8_t *)&ckpt.lsn, sz, ckpt.chksum);

		/* use pwrite instead of __os_io until __checkpoint_open is done */
		ret =
		    pwrite(dbenv->checkpoint->fd, &ckpt,
		    sizeof(struct __db_checkpoint), 0);
		if (ret != 512)
			ret = EINVAL;

		else
			ret = 0;
		if (ret) {
			__db_err(dbenv, "__checkpoint_open log_cursor rc %d\n",
			    ret);
			goto err;
		}
	} else if (ret)
		goto err;

	/* verify checksum */
	ret = pread(dbenv->checkpoint->fd, &ckpt, 512, 0);
	if (ret != 512)
		ret = EINVAL;

	else
		ret = 0;
	if ((ret =
		__db_check_chksum_no_crypto(dbenv, NULL, ckpt.chksum, &ckpt.lsn,
		    sizeof(struct __db_checkpoint) -
		    offsetof(struct __db_checkpoint, lsn), 0)) != 0) {
		__db_err(dbenv, "checkpoint checksum verification failed");
		goto err;
	}

err:
	return ret;
}
