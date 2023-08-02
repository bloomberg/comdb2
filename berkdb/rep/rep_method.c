/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2001-2003
 *	Sleepycat Software.  All rights reserved.
 */
#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: rep_method.c,v 1.134 2003/11/13 15:41:51 sue Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#ifdef HAVE_RPC
#include <rpc/rpc.h>
#endif

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#endif

#include "db_int.h"
#include "dbinc/db_swap.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/log.h"
#include "dbinc/txn.h"
#include <comdb2_atomic.h>

#ifdef HAVE_RPC
#include "dbinc_auto/db_server.h"
#include "dbinc_auto/rpc_client_ext.h"
#endif

#include <epochlib.h>
#include "logmsg.h"
#include <locks_wrap.h>

int gbl_rep_method_max_sleep_cnt = 0;

static int __rep_abort_prepared __P((DB_ENV *));
static int __rep_bt_cmp __P((DB *, const DBT *, const DBT *));
static int __rep_client_dbinit __P((DB_ENV *, int));
static int __rep_elect __P((DB_ENV *, int, int, u_int32_t, u_int32_t *, int *, char **));
static int __rep_elect_init
__P((DB_ENV *, DB_LSN *, int, int, int *, u_int32_t *));
static int __rep_flush __P((DB_ENV *));
static int __rep_restore_prepared __P((DB_ENV *));
static int __rep_get_limit __P((DB_ENV *, u_int32_t *, u_int32_t *));
static int __rep_set_limit __P((DB_ENV *, u_int32_t, u_int32_t));
int __rep_set_request __P((DB_ENV *, u_int32_t, u_int32_t));
static int __rep_set_rep_transport __P((DB_ENV *, char *,
	int (*)(DB_ENV *, const DBT *, const DBT *, const DB_LSN *,
		char *, int, void *)));
static int __rep_set_check_standalone __P((DB_ENV *, int (*)(DB_ENV *)));
static int __rep_set_truncate_sc_callback __P((DB_ENV *, int (*)(DB_ENV *, DB_LSN *)));
static int __rep_set_rep_truncate_callback __P((DB_ENV *, int (*)(DB_ENV *, DB_LSN *, uint32_t is_master)));
static int __rep_set_rep_recovery_cleanup __P((DB_ENV *, int (*)(DB_ENV *, DB_LSN *, int is_master)));
static int __rep_lock_recovery_lock __P((DB_ENV *, const char *func, int line));
static int __rep_wrlock_recovery_lock __P((DB_ENV *, const char *func, int line));
static int __rep_unlock_recovery_lock __P((DB_ENV *, const char *func, int line));
static int __rep_wrlock_recovery_blocked __P((DB_ENV *));
static int __rep_set_rep_db_pagesize __P((DB_ENV *, int));
static int __rep_get_rep_db_pagesize __P((DB_ENV *, int *));
static int __rep_set_ignore __P((DB_ENV *, int (*func)(const char *filename)));
static int __rep_start __P((DB_ENV *, DBT *, u_int32_t, u_int32_t));
static int __rep_stat __P((DB_ENV *, DB_REP_STAT **, u_int32_t));
static int __rep_deadlocks __P((DB_ENV *, u_int64_t *));
static int __rep_wait __P((DB_ENV *, u_int32_t, char **, u_int32_t *, u_int32_t, u_int32_t));

extern int gbl_is_physical_replicant;
extern int gbl_inmem_repdb;

#ifndef TESTSUITE
void bdb_get_writelock(void *bdb_state,
	const char *idstr, const char *funcname, int line);
void bdb_rellock(void *bdb_state, const char *funcname, int line);
int bdb_is_open(void *bdb_state);

#define BDB_WRITELOCK(idstr)	bdb_get_writelock(bdb_state, (idstr), __func__, __LINE__)
#define BDB_RELLOCK()		   bdb_rellock(bdb_state, __func__, __LINE__)

#else

#define BDB_WRITELOCK(x)
#define BDB_RELLOCK()

#endif

/*
 * __rep_dbenv_create --
 *	Replication-specific initialization of the DB_ENV structure.
 *
 * PUBLIC: int __rep_dbenv_create __P((DB_ENV *));
 */
int
__rep_dbenv_create(dbenv)
	DB_ENV *dbenv;
{
#ifdef HAVE_RPC
	if (F_ISSET(dbenv, DB_ENV_RPCCLIENT)) {
		dbenv->rep_elect = __dbcl_rep_elect;
		dbenv->rep_flush = __dbcl_rep_flush;
		dbenv->rep_process_message = __dbcl_rep_process_message;
		dbenv->rep_start = __dbcl_rep_start;
		dbenv->rep_stat = __dbcl_rep_stat;
		dbenv->get_rep_limit = __dbcl_rep_get_limit;
		dbenv->set_rep_limit = __dbcl_rep_set_limit;
		dbenv->set_rep_request = __dbcl_rep_set_request;
		dbenv->set_rep_transport = __dbcl_rep_set_rep_transport;

	} else
#endif
	{
		dbenv->rep_elect = __rep_elect;
		dbenv->rep_flush = __rep_flush;
		dbenv->rep_process_message = __rep_process_message;
		dbenv->rep_verify_will_recover = __rep_verify_will_recover;
		dbenv->rep_start = __rep_start;
		dbenv->rep_stat = __rep_stat;
		dbenv->rep_deadlocks = __rep_deadlocks;
		dbenv->get_rep_gen = __rep_get_gen;
		dbenv->get_rep_log_gen = __rep_get_log_gen;
		dbenv->get_last_locked = __rep_get_last_locked;
		dbenv->get_rep_limit = __rep_get_limit;
		dbenv->set_rep_limit = __rep_set_limit;
		dbenv->set_rep_request = __rep_set_request;
		dbenv->set_rep_transport = __rep_set_rep_transport;
		dbenv->set_rep_ignore = __rep_set_ignore;
		dbenv->set_truncate_sc_callback = __rep_set_truncate_sc_callback;
		dbenv->set_rep_truncate_callback = __rep_set_rep_truncate_callback;
		dbenv->set_rep_recovery_cleanup = __rep_set_rep_recovery_cleanup;
		dbenv->rep_set_gen = __rep_set_gen_pp;
		dbenv->wrlock_recovery_lock = __rep_wrlock_recovery_lock;
        dbenv->wrlock_recovery_blocked = __rep_wrlock_recovery_blocked;
		dbenv->lock_recovery_lock = __rep_lock_recovery_lock;
		dbenv->unlock_recovery_lock = __rep_unlock_recovery_lock;
		dbenv->set_check_standalone = __rep_set_check_standalone;
		dbenv->set_rep_db_pagesize = __rep_set_rep_db_pagesize;
		dbenv->get_rep_db_pagesize = __rep_get_rep_db_pagesize;
		dbenv->rep_truncate_repdb = __rep_truncate_repdb;
	}

	return (0);
}

/*
 * __rep_open --
 *	Replication-specific initialization of the DB_ENV structure.
 *
 * PUBLIC: int __rep_open __P((DB_ENV *));
 */
int
__rep_open(dbenv)
	DB_ENV *dbenv;
{
	DB_REP *db_rep;
	int ret;

	if ((ret = __os_calloc(dbenv, 1, sizeof(DB_REP), &db_rep)) != 0)
		return (ret);
	dbenv->rep_handle = db_rep;
	ret = __rep_region_init(dbenv);
	return (ret);
}

extern pthread_mutex_t rep_candidate_lock;

/*
 * __rep_start --
 *	Become a master or client, and start sending messages to participate
 * in the replication environment.  Must be called after the environment
 * is open.
 *
 * We must protect rep_start, which may change the world, with the rest
 * of the DB library.  Each API interface will count itself as it enters
 * the library.  Rep_start checks the following:
 *
 * rep->msg_th - this is the count of threads currently in rep_process_message
 * rep->start_th - this is set if a thread is in rep_start.
 * rep->handle_cnt - number of threads actively using a dbp in library.
 * rep->txn_cnt - number of active txns.
 * REP_F_READY - Replication flag that indicates that we wish to run
 * recovery, and want to prohibit new transactions from entering and cause
 * existing ones to return immediately (with a DB_LOCK_DEADLOCK error).
 *
 * There is also the rep->timestamp which is updated whenever significant
 * events (i.e., new masters, log rollback, etc).  Upon creation, a handle
 * is associated with the current timestamp.  Each time a handle enters the
 * library it must check if the handle timestamp is the same as the one
 * stored in the replication region.  This prevents the use of handles on
 * clients that reference non-existent files whose creation was backed out
 * during a synchronizing recovery.
 */
static int
__rep_start(dbenv, dbt, gen, flags)
	DB_ENV *dbenv;
	DBT *dbt;
	u_int32_t gen;
	u_int32_t flags;
{
	DB_LOG *dblp;
	DB_LSN lsn;
	DB_REP *db_rep;
	REP *rep;
	u_int32_t repflags;
	int announce, init_db, redo_prepared, ret, role_chg;
	int sleep_cnt, t_ret, upgrade_prepared = 0;
	void *bdb_state = dbenv->app_private;

	PANIC_CHECK(dbenv);
	ENV_ILLEGAL_BEFORE_OPEN(dbenv, "DB_ENV->rep_start");
	ENV_REQUIRES_CONFIG(dbenv, dbenv->rep_handle, "rep_start", DB_INIT_REP);

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	if ((ret = __db_fchk(dbenv, "DB_ENV->rep_start", flags,
		DB_REP_CLIENT | DB_REP_LOGSONLY | DB_REP_MASTER)) != 0)
		return (ret);

	/* Exactly one of CLIENT and MASTER must be specified. */
	if ((ret = __db_fcchk(dbenv,
		"DB_ENV->rep_start", flags, DB_REP_CLIENT, DB_REP_MASTER)) != 0)
		return (ret);
	if (!LF_ISSET(DB_REP_CLIENT | DB_REP_MASTER | DB_REP_LOGSONLY)) {
		__db_err(dbenv,
	"DB_ENV->rep_start: replication mode must be specified");
		return (EINVAL);
	}

	/* Masters can't be logs-only. */
	if ((ret = __db_fcchk(dbenv,
		"DB_ENV->rep_start", flags, DB_REP_LOGSONLY, DB_REP_MASTER)) != 0)
		return (ret);

	/* We need a transport function. */
	if (dbenv->rep_send == NULL) {
		__db_err(dbenv,
	"DB_ENV->set_rep_transport must be called before DB_ENV->rep_start");
		return (EINVAL);
	}

	BDB_WRITELOCK("berk");

	/*
	 * If we are about to become (or stay) a master.  Let's flush the log
	 * to close any potential holes that might happen when upgrading from
	 * client to master status.
	 */
	if (LF_ISSET(DB_REP_MASTER) && (ret = __log_flush(dbenv, NULL)) != 0) {
		BDB_RELLOCK();
		return (ret);
	}

	Pthread_mutex_lock(&rep_candidate_lock);
	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	/*
	 * We only need one thread to start-up replication, so if
	 * there is another thread in rep_start, we'll let it finish
	 * its work and have this thread simply return.
	 */
	if (rep->start_th != 0) {
		/*
		 * There is already someone in rep_start.  Return.
		 */
#ifdef DIAGNOSTIC
		if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
			__db_err(dbenv, "Thread already in rep_start");
#endif
		goto err;
	} else
		rep->start_th = 1;

	role_chg = (!F_ISSET(rep, REP_F_MASTER) && LF_ISSET(DB_REP_MASTER)) ||
		(!F_ISSET(rep, REP_F_UPGRADE) && LF_ISSET(DB_REP_CLIENT));

	/*
	 * Wait for any active txns or mpool ops to complete, and
	 * prevent any new ones from occurring, only if we're
	 * changing roles.  If we are not changing roles, then we
	 * only need to coordinate with msg_th.
	 */
	if (role_chg) {
	}
	else {
		pid_t pid;
		char cmd[32];

		for (sleep_cnt = 0; rep->msg_th != 0;) {
			if (++sleep_cnt % 60 == 0)
				__db_err(dbenv,
					"DB_ENV->rep_start waiting %d minutes for replication message thread",
					sleep_cnt / 60);

			if (sleep_cnt > gbl_rep_method_max_sleep_cnt &&
				gbl_rep_method_max_sleep_cnt > 0) {
				logmsg(LOGMSG_FATAL, "%s:%d:%s: Exiting after waiting too long for replication message thread.\n",
					__FILE__, __LINE__, __func__);
                void pstack_self(void);
                pstack_self();
				abort();
			}
			MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
			Pthread_mutex_unlock(&rep_candidate_lock);
			(void)__os_sleep(dbenv, 1, 0);
			Pthread_mutex_lock(&rep_candidate_lock);
			MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		}
	}

	if (rep->eid == db_eid_invalid)
		rep->eid = dbenv->rep_eid;

	if (LF_ISSET(DB_REP_MASTER)) {
		if (role_chg) {
			/*
			 * If we're upgrading from having been a client,
			 * preclose, so that we close our temporary database.
			 *
			 * Do not close files that we may have opened while
			 * doing a rep_apply;  they'll get closed when we
			 * finally close the environment, but for now, leave
			 * them open, as we don't want to recycle their
			 * fileids, and we may need the handles again if
			 * we become a client and the original master
			 * that opened them becomes a master again.
			 */
			if (!gbl_is_physical_replicant && ((ret = __rep_preclose(dbenv, 1)) != 0))
				goto errunlock;
		}

		redo_prepared = 0;
		if (!F_ISSET(rep, REP_F_MASTER)) {
			/* Master is not yet set. */
			if (role_chg) {
				if (rep->w_gen > rep->recover_gen) {
					++rep->w_gen;
					__rep_set_gen(dbenv, __func__, __LINE__, rep->w_gen);
				} else if (rep->gen > rep->recover_gen) {
					__rep_set_gen(dbenv, __func__, __LINE__, rep->gen + 1);
				} else {
					__rep_set_gen(dbenv, __func__, __LINE__, 
							rep->recover_gen + 1);
				}
				/*
				 * There could have been any number of failed
				 * elections, so jump the gen if we need to now.
				 */
				if (gen != 0) {
					logmsg(LOGMSG_DEBUG, "%s line %d setting gen to arg %d "
							"current egen is %d\n", __func__, __LINE__, gen, 
							rep->egen);
					__rep_set_gen(dbenv, __func__, __LINE__, gen);
				} else if (rep->egen > rep->gen) {
					logmsg(LOGMSG_DEBUG, "%s line %d setting gen to rep->egen "
							"%d\n", __func__, __LINE__, rep->egen);
					__rep_set_gen(dbenv, __func__, __LINE__, rep->egen);
				}

				redo_prepared = 1;
			} else if (rep->gen == 0) {
				logmsg(LOGMSG_DEBUG, "%s line %d setting gen to recover_gen + 1 "
						"%d\n", __func__, __LINE__, rep->recover_gen + 1);
				__rep_set_gen(dbenv, __func__, __LINE__, rep->recover_gen + 1);
			}
			if (F_ISSET(rep, REP_F_MASTERELECT)) {
				__rep_elect_done(dbenv, rep, 0, __func__, __LINE__);
				F_CLR(rep, REP_F_MASTERELECT);
			}
			if (rep->egen <= rep->gen)
				__rep_set_egen(dbenv, __func__, __LINE__, rep->gen + 1);

			logmsg(LOGMSG_DEBUG, "%s line %d upgrading to gen %d egen %d\n",
					__func__, __LINE__, rep->gen, rep->egen);
			__rep_set_log_gen(dbenv, __func__, __LINE__, rep->gen);
		}
		F_CLR(rep, REP_F_WAITSTART);
		Pthread_mutex_unlock(&rep_candidate_lock);
		rep->master_id = rep->eid;
		/*
		 * Note, setting flags below implicitly clears out
		 * REP_F_NOARCHIVE, REP_F_INIT and REP_F_READY.
		 */
		rep->flags = REP_F_MASTER;
		rep->start_th = 0;
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		dblp = (DB_LOG *)dbenv->lg_handle;
		R_LOCK(dbenv, &dblp->reginfo);
		lsn = ((LOG *)dblp->reginfo.primary)->lsn;
		R_UNLOCK(dbenv, &dblp->reginfo);

		/*
		 * Send the NEWMASTER message first so that clients know
		 * subsequent messages are coming from the right master.
		 * We need to perform all actions below no master what
		 * regarding errors.
		 */
		logmsg(LOGMSG_DEBUG, "%s line %d sending REP_NEWMASTER\n", 
				__func__, __LINE__);
		(void)__rep_send_message(dbenv,
			db_eid_broadcast, REP_NEWMASTER, &lsn, NULL, 0, NULL);
		ret = 0;
		if (role_chg) {
			ret = __txn_reset(dbenv);
			MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
			F_CLR(rep, REP_F_READY);
			rep->in_recovery = 0;
			MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		}
		/*
		 * Take a transaction checkpoint so that our new generation
		 * number get written to the log.
		 */
		if (!gbl_is_physical_replicant)
		{
			if ((t_ret = __txn_checkpoint(dbenv, 0, 0, DB_FORCE)) != 0 &&
					ret == 0)
				ret = t_ret;
			if (redo_prepared) {
				upgrade_prepared = 1;
				if ((t_ret = __rep_restore_prepared(dbenv)) != 0 && ret == 0)
					ret = t_ret;
			}
		}
	} else {
		init_db = 0;
		announce = role_chg || rep->master_id == db_eid_invalid;

		/*
		 * If we're changing roles from master to client or if
		 * we never were any role at all, we need to init the db.
		 */
		if (role_chg || !F_ISSET(rep, REP_F_UPGRADE)) {
			rep->master_id = db_eid_invalid;
			init_db = 1;
		}
		/* Zero out everything except recovery and tally flags. */
		repflags = F_ISSET(rep, REP_F_NOARCHIVE |
		    REP_F_READY | REP_F_RECOVER | REP_F_TALLY);
		if (LF_ISSET(DB_REP_LOGSONLY))
			FLD_SET(repflags, REP_F_LOGSONLY);
		else
			FLD_SET(repflags, REP_F_UPGRADE);

		rep->flags = repflags;
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		Pthread_mutex_unlock(&rep_candidate_lock);

		/*
		 * Abort any prepared transactions that were restored
		 * by recovery.  We won't be able to create any txns of
		 * our own until they're resolved, but we can't resolve
		 * them ourselves;  the master has to.  If any get
		 * resolved as commits, we'll redo them when commit
		 * records come in.  Aborts will simply be ignored.
		 */
		if ((ret = __rep_abort_prepared(dbenv)) != 0)
			goto errlock;

		if ((ret = __txn_downgrade_all_prepared(dbenv)) != 0)
			goto errlock;

		if ((ret = __rep_client_dbinit(dbenv, init_db)) != 0)
			goto errlock;
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		rep->start_th = 0;
		if (role_chg) {
			F_CLR(rep, REP_F_READY);
			rep->in_recovery = 0;
		}
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

		/*
		 * If this client created a newly replicated environment,
		 * then announce the existence of this client.  The master
		 * should respond with a message that will tell this client
		 * the current generation number and the current LSN.  This
		 * will allow the client to either perform recovery or
		 * simply join in.
		 */
		if (announce)
			(void)__rep_send_message(dbenv,
			    db_eid_broadcast, REP_NEWCLIENT, NULL, dbt, 0,
			    NULL);
	}

	if (0) {
		/*
		 * We have separate labels for errors.  If we're returning an
		 * error before we've set start_th, we use 'err'.  If
		 * we are erroring while holding the rep_mutex, then we use
		 * 'errunlock' label.  If we're erroring without holding the rep
		 * mutex we must use 'errlock'.
		 */
errlock:
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
errunlock:
		rep->start_th = 0;
		if (role_chg) {
			F_CLR(rep, REP_F_READY);
			rep->in_recovery = 0;
		}
err:		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
	}
	extern int backend_opened(void);
	if (backend_opened() && upgrade_prepared && 
		(t_ret = __txn_upgrade_all_prepared(dbenv)) != 0 && ret == 0)
		ret = t_ret;

	BDB_RELLOCK();
	return (ret);
}

/*
 * __rep_client_dbinit --
 *
 * Initialize the LSN database on the client side.  This is called from the
 * client initialization code.  The startup flag value indicates if
 * this is the first thread/process starting up and therefore should create
 * the LSN database.  This routine must be called once by each process acting
 * as a client.
 */
static int
__rep_client_dbinit(dbenv, startup)
	DB_ENV *dbenv;
	int startup;
{
	DB_REP *db_rep;
	DB *dbp;
	int ret, t_ret, i, dircnt;
	char *dir, **namesp, *p, *repdbname = NULL;
	u_int32_t flags;

	PANIC_CHECK(dbenv);
	db_rep = dbenv->rep_handle;
	dbp = NULL;

#define	REPDBBASE	"__db.rep.db"

	MUTEX_LOCK(dbenv, db_rep->db_mutexp);

	/* Check if this has already been called on this environment. */
	if (db_rep->rep_db != NULL) {
		MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);
		return (0);
	}

	if (startup) {
		/* Go through the txn directory & remove any files with 
		 * the __db.rep.db signature */

		if ((ret =
			__os_malloc(dbenv,
			    strlen(dbenv->comdb2_dirs.txn_dir) + 4, &dir)) != 0)
			goto err;

		sprintf(dir, "%s/", dbenv->comdb2_dirs.txn_dir);

		ret = __os_dirlist(dbenv, dir, &namesp, &dircnt);
		__os_free(dbenv, dir);

		if (ret != 0)
			goto err;

		for (i = 0; i < dircnt; i++) {
			if ((p = strrchr(namesp[i], '/')) != NULL)
				p++;
			else
				p = &namesp[i][0];

			if (strncmp(p, REPDBBASE, strlen(REPDBBASE)) == 0) {

				if ((ret =
					db_create(&dbp, dbenv,
					    DB_REP_CREATE)) != 0)
					goto err;

				(void)__db_remove(dbp, NULL, p, NULL, DB_FORCE);

			}
		}
		__os_dirfree(dbenv, namesp, dircnt);
		namesp = NULL;
	}

	if(gbl_inmem_repdb)
		goto done;

	if ((ret = db_create(&dbp, dbenv, DB_REP_CREATE)) != 0)
		goto err;
	if ((ret = __bam_set_bt_compare(dbp, __rep_bt_cmp)) != 0)
		goto err;

	/* Allow writes to this database on a client. */
	F_SET(dbp, DB_AM_CL_WRITER);

	flags = DB_NO_AUTO_COMMIT |
	    (startup ? DB_CREATE : 0)|
	    (F_ISSET(dbenv, DB_ENV_THREAD) ? DB_THREAD : 0);

	/* Set the pagesize. */
	if (dbenv->rep_db_pagesize > 0) {
		if ((ret = dbp->set_pagesize(dbp, dbenv->rep_db_pagesize)) != 0)
			goto err;
	}

	/* Create a name */
	if ((ret = __os_malloc(dbenv, strlen(REPDBBASE) + 32, &repdbname)) != 0)
		goto err;

	sprintf(repdbname, "%s.%ld.%d", REPDBBASE, time(NULL),
	    db_rep->repdbcnt++);

	if ((ret = __db_open(dbp, NULL,
		    repdbname, NULL, DB_BTREE, flags, 0, PGNO_BASE_MD)) != 0)
		 goto err;

	db_rep->repdbname = repdbname;
	db_rep->rep_db = dbp;

	if (0) {
err:		if (dbp != NULL &&
		    (t_ret = __db_close(dbp, NULL, DB_NOSYNC)) != 0 &&
		    ret == 0)
			ret = t_ret;
		db_rep->rep_db = NULL;

		if (repdbname != NULL)
			__os_free(dbenv, repdbname);
	}

done:
	MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);

	return (ret);
}

/*
 * __rep_bt_cmp --
 *
 * Comparison function for the LSN table.  We use the entire control
 * structure as a key (for simplicity, so we don't have to merge the
 * other fields in the control with the data field), but really only
 * care about the LSNs.
 */
static int
__rep_bt_cmp(dbp, dbt1, dbt2)
	DB *dbp;
	const DBT *dbt1, *dbt2;
{
	DB_LSN lsn1, lsn2;
	REP_CONTROL *rp1, *rp2;

	COMPQUIET(dbp, NULL);

	rp1 = dbt1->data;
	rp2 = dbt2->data;

	__ua_memcpy(&lsn1, &rp1->lsn, sizeof(DB_LSN));
	__ua_memcpy(&lsn2, &rp2->lsn, sizeof(DB_LSN));

	if (lsn1.file > lsn2.file)
		return (1);

	if (lsn1.file < lsn2.file)
		return (-1);

	if (lsn1.offset > lsn2.offset)
		return (1);

	if (lsn1.offset < lsn2.offset)
		return (-1);

	return (0);
}

/*
 * __rep_abort_prepared --
 *	Abort any prepared transactions that recovery restored.
 *
 *	This is used by clients that have just run recovery, since
 * they cannot/should not call txn_recover and handle prepared transactions
 * themselves.
 */
static int
__rep_abort_prepared(dbenv)
	DB_ENV *dbenv;
{
#define	PREPLISTSIZE	50
	DB_PREPLIST prep[PREPLISTSIZE], *p;
	DB_TXNMGR *mgr;
	DB_TXNREGION *region;
	int do_aborts, ret;
	long count, i;
	u_int32_t op;

	mgr = dbenv->tx_handle;
	region = mgr->reginfo.primary;

	do_aborts = 0;
	R_LOCK(dbenv, &mgr->reginfo);
	if (region->stat.st_nrestores != 0)
		do_aborts = 1;
	R_UNLOCK(dbenv, &mgr->reginfo);

	if (do_aborts) {
		op = DB_FIRST;
		do {
			if ((ret = __txn_recover(dbenv,
			    prep, PREPLISTSIZE, &count, op)) != 0)
				return (ret);
			for (i = 0; i < count; i++) {
				p = &prep[i];
				if ((ret = __txn_abort(p->txn)) != 0)
					return (ret);
			}
			op = DB_NEXT;
		} while (count == PREPLISTSIZE);
	}

	return (0);
}

/*
 * __rep_restore_prepared --
 *	Restore to a prepared state any prepared but not yet committed
 * transactions.
 *
 *	This performs, in effect, a "mini-recovery";  it is called from
 * __rep_start by newly upgraded masters.  There may be transactions that an
 * old master prepared but did not resolve, which we need to restore to an
 * active state.
 */
static int
__rep_restore_prepared(dbenv)
	DB_ENV *dbenv;
{
	return 0;
}

static int
__rep_get_limit(dbenv, gbytesp, bytesp)
	DB_ENV *dbenv;
	u_int32_t *gbytesp, *bytesp;
{
	DB_REP *db_rep;
	REP *rep;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv, dbenv->rep_handle, "rep_get_limit",
	    DB_INIT_REP);

	if (!REP_ON(dbenv)) {
		__db_err(dbenv,
    "DB_ENV->get_rep_limit: database environment not properly initialized");
		return (__db_panic(dbenv, EINVAL));
	}
	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	if (gbytesp != NULL)
		*gbytesp = rep->gbytes;
	if (bytesp != NULL)
		*bytesp = rep->bytes;

	return (0);
}

/*
 * __rep_set_limit --
 *	Set a limit on the amount of data that will be sent during a single
 * invocation of __rep_process_message.
 */
static int
__rep_set_limit(dbenv, gbytes, bytes)
	DB_ENV *dbenv;
	u_int32_t gbytes, bytes;
{
	DB_REP *db_rep;
	REP *rep;

	PANIC_CHECK(dbenv);
	ENV_ILLEGAL_BEFORE_OPEN(dbenv, "DB_ENV->rep_set_limit");
	ENV_REQUIRES_CONFIG(dbenv, dbenv->rep_handle, "rep_set_limit",
	    DB_INIT_REP);

	if (!REP_ON(dbenv)) {
		__db_err(dbenv,
		    "DB_ENV->set_rep_limit: database environment not properly initialized");
		return (__db_panic(dbenv, EINVAL));
	}
	db_rep = dbenv->rep_handle;
	rep = db_rep->region;
	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	if (bytes > GIGABYTE) {
		gbytes += bytes / GIGABYTE;
		bytes = bytes % GIGABYTE;
	}
	rep->gbytes = gbytes;
	rep->bytes = bytes;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

	return (0);
}

/*
 * __rep_set_request --
 *	Set the minimum and maximum number of log records that we wait
 * before retransmitting.
 * UNDOCUMENTED.
 */
int
__rep_set_request(dbenv, min, max)
	DB_ENV *dbenv;
	u_int32_t min, max;
{
	LOG *lp;
	DB_LOG *dblp;
	DB_REP *db_rep;
	REP *rep;

	PANIC_CHECK(dbenv);
	ENV_ILLEGAL_BEFORE_OPEN(dbenv, "DB_ENV->rep_set_request");
	ENV_REQUIRES_CONFIG(dbenv, dbenv->rep_handle, "rep_set_request",
	    DB_INIT_REP);

	if (!REP_ON(dbenv)) {
		__db_err(dbenv,
    "DB_ENV->set_rep_request: database environment not properly initialized");
		return (__db_panic(dbenv, EINVAL));
	}
	db_rep = dbenv->rep_handle;
	rep = db_rep->region;
	/*
	 * Note we acquire the rep_mutexp or the db_mutexp as needed.
	 */
	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	rep->request_gap = min;
	rep->max_gap = max;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

	MUTEX_LOCK(dbenv, db_rep->db_mutexp);
	dblp = dbenv->lg_handle;
	if (dblp != NULL && (lp = dblp->reginfo.primary) != NULL) {
		lp->wait_recs = 0;
		lp->rcvd_recs = 0;
	}
	MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);

	return (0);
}

/*
 * __rep_get_rep_db_pagesize
 * Get the pagesize for the replication database.
 */
static int
__rep_get_rep_db_pagesize(dbenv, pagesize)
	DB_ENV *dbenv;
	int *pagesize;
{
	PANIC_CHECK(dbenv);
	*pagesize = dbenv->rep_db_pagesize;
	return 0;
}

/*
 * __rep_set_rep_db_pagesize
 * Set the pagesize for the replication database.
 */
static int
__rep_set_rep_db_pagesize(dbenv, pagesize)
	DB_ENV *dbenv;
	int pagesize;
{
	PANIC_CHECK(dbenv);
	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_rep_db_pagesize");

	if (pagesize < 4096) {
		__db_err(dbenv,
		    "DB_ENV->set_rep_db_pagesize: "
		    "pagesize must be >= 4096.");
		return EINVAL;
	}

	dbenv->rep_db_pagesize = pagesize;
	return 0;
}

static int
__rep_set_check_standalone(dbenv, f_check_standalone)
	DB_ENV *dbenv;
	int (*f_check_standalone) __P((DB_ENV *));
{
	PANIC_CHECK(dbenv);
	if (f_check_standalone == NULL) {
		__db_err(dbenv, "DB_ENV->set_check_standalone: no function specified");
		return (EINVAL);
	}
	dbenv->check_standalone = f_check_standalone;
	return (0);
}

static int
__rep_set_rep_recovery_cleanup(dbenv, rep_recovery_cleanup)
	DB_ENV *dbenv;
	int (*rep_recovery_cleanup) __P((DB_ENV *, DB_LSN *lsn, int is_master));
{
	PANIC_CHECK(dbenv);
	if (rep_recovery_cleanup == NULL) {
		__db_err(dbenv, "DB_ENV->set_rep_recovery_cleanup: no function specified");
		return (EINVAL);
	}
	dbenv->rep_recovery_cleanup = rep_recovery_cleanup;
	return (0);
}

static int
__rep_set_rep_truncate_callback(dbenv, rep_truncate_callback)
	DB_ENV *dbenv;
	int (*rep_truncate_callback) __P((DB_ENV *, DB_LSN *lsn, uint32_t flags));
{
	PANIC_CHECK(dbenv);
	if (rep_truncate_callback == NULL) {
		__db_err(dbenv, "DB_ENV->set_rep_truncate_sc_callback: no function specified");
		return (EINVAL);
	}
	dbenv->rep_truncate_callback = rep_truncate_callback;
	return (0);
}

#if DEBUG_RECOVERY_LOCK
void comdb2_cheapstack_sym(FILE *f, char *fmt, ...);
#endif

static int recoverlk_blocked = 0;

static int
__rep_wrlock_recovery_lock(dbenv, func, line)
	DB_ENV *dbenv;
	const char *func;
	int line;
{
    recoverlk_blocked = 1;
	Pthread_rwlock_wrlock(&dbenv->recoverlk);
    recoverlk_blocked = 0;
#if DEBUG_RECOVERY_LOCK
	logmsg(LOGMSG_USER, "%s line %d WRITE-LOCK recoverlk, readers=%d\n", func, line,
        dbenv->recoverlk.__data.__readers);
    comdb2_cheapstack_sym(stderr, "%s:%d", func, line);
#endif
	return 0;
}

static int
__rep_wrlock_recovery_blocked(dbenv)
    DB_ENV *dbenv;
{
    return recoverlk_blocked;
}

#if DEBUG_RECOVERY_LOCK
pthread_mutex_t prlk = PTHREAD_MUTEX_INITIALIZER;
#endif

static int
__rep_lock_recovery_lock(dbenv, func, line)
	DB_ENV *dbenv;
	const char *func;
	int line;
{
	Pthread_rwlock_rdlock(&dbenv->recoverlk);
#if DEBUG_RECOVERY_LOCK
    Pthread_mutex_lock(&prlk);
	logmsg(LOGMSG_USER, "%s line %d READ-LOCK recoverlk, readers=%d\n", func, line,
        dbenv->recoverlk.__data.__readers);
    comdb2_cheapstack_sym(stderr, "%s:%d", func, line);
    Pthread_mutex_unlock(&prlk);
#endif
	return 0;
}

static int
__rep_unlock_recovery_lock(dbenv, func, line)
	DB_ENV *dbenv;
	const char *func;
	int line;
{
#if DEBUG_RECOVERY_LOCK
    Pthread_mutex_lock(&prlk);
	logmsg(LOGMSG_USER, "%s line %d UNLOCK recoverlk, readers=%d\n", func, line,
        dbenv->recoverlk.__data.__readers);
    comdb2_cheapstack_sym(stderr, "%s:%d", func, line);
    Pthread_mutex_unlock(&prlk);
#endif
	Pthread_rwlock_unlock(&dbenv->recoverlk);
	return 0;
}

static int
__rep_set_truncate_sc_callback(dbenv, truncate_sc_callback)
	DB_ENV *dbenv;
	int (*truncate_sc_callback) __P((DB_ENV *, DB_LSN *lsn));
{
	PANIC_CHECK(dbenv);
	if (truncate_sc_callback == NULL) {
		__db_err(dbenv, "DB_ENV->truncate_sc_callback: no function specified");
		return (EINVAL);
	}
	dbenv->truncate_sc_callback = truncate_sc_callback;
	return (0);
}

/*
 * __rep_set_ignore --
 *  Register function which tells replication to ignore certain fileids 
 */
static int
__rep_set_ignore(dbenv, f_ignore)
	DB_ENV *dbenv;
	int (*f_ignore) __P((const char *));
{
	PANIC_CHECK(dbenv);
	if (f_ignore == NULL) {
		__db_err(dbenv,
			"DB_ENV->set_rep_ignore_fileid_func: no send function specified");
		return (EINVAL);
	}
	dbenv->rep_ignore = f_ignore;
	return (0);
}

/*
 * __rep_set_transport --
 *	Set the transport function for replication.
 */
static int
__rep_set_rep_transport(dbenv, eid, f_send)
	DB_ENV *dbenv;
	char *eid;
	int (*f_send) __P((DB_ENV *, const DBT *, const DBT *, const DB_LSN *,
	char *, int, void *user_ptr));
{
	PANIC_CHECK(dbenv);

	if (f_send == NULL) {
		__db_err(dbenv,
		    "DB_ENV->set_rep_transport: no send function specified");
		return (EINVAL);
	}
	dbenv->rep_send = f_send;
	dbenv->rep_eid = eid;
	return (0);
}

extern pthread_mutex_t rep_queue_lock;
extern void send_master_req(DB_ENV *dbenv, const char *func, int line);

static int
__retrieve_logged_generation_commitlsn(dbenv, lsn, gen)
	DB_ENV *dbenv;
	DB_LSN *lsn;
	u_int32_t *gen;
{
	DBT rec;
	DB_LOGC *logc;
	DB_REP *db_rep;
	DB_LSN lastlsn, curlsn;
	REP *rep;
	u_int32_t rectype;
	int ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv, dbenv->rep_handle,
			"retrieve_logged_generation_commitlsn", DB_INIT_REP);

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	*lsn = rep->committed_lsn;
	*gen = rep->committed_gen;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

	/* Return immediately */
	if (lsn->file != 0)
		return 0;

	if ((ret = __log_cursor(dbenv, &logc)) != 0)
		return (ret);

	memset(&rec, 0, sizeof(rec));
	memset(lsn, 0, sizeof(*lsn));

	if ((ret = __log_c_get(logc, &lastlsn, &rec, DB_LAST)) != 0)
		goto err;

	curlsn = lastlsn;

	LOGCOPY_32(&rectype, rec.data);
	normalize_rectype(&rectype);

	while (ret == 0 && rectype != DB___txn_regop_gen && rectype !=
			DB___txn_regop_rowlocks && rectype != DB___txn_dist_commit &&
			rectype != DB___txn_dist_prepare && rectype != DB___txn_regop) {
		if ((ret = __log_c_get(logc, &curlsn, &rec, DB_PREV)) == 0) {
			LOGCOPY_32(&rectype, rec.data);
			normalize_rectype(&rectype);
		}
	}

	if (rectype == DB___txn_regop_gen) {
		__txn_regop_gen_args *txn_gen_args = NULL;
		if ((ret = __txn_regop_gen_read(dbenv, rec.data,
						&txn_gen_args)) != 0)
			goto err;
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		rep->committed_lsn = *lsn = curlsn;
		rep->committed_gen = *gen = txn_gen_args->generation;
		if (rep->gen < rep->committed_gen) {
			__rep_set_gen(dbenv, __func__, __LINE__, rep->committed_gen);
			__rep_set_log_gen(dbenv, __func__, __LINE__, rep->gen);
		}
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		__os_free(dbenv, txn_gen_args);
	} else if (rectype == DB___txn_dist_prepare) {
		__txn_dist_prepare_args *txn_dist_prepare_args = NULL;
		if ((ret = __txn_dist_prepare_read(dbenv, rec.data,
						&txn_dist_prepare_args)) != 0)
			goto err;
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		rep->committed_lsn = *lsn = curlsn;
		rep->committed_gen = *gen = txn_dist_prepare_args->generation;
		if (rep->gen < rep->committed_gen)
			__rep_set_gen(dbenv, __func__, __LINE__, rep->committed_gen);
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		__os_free(dbenv, txn_dist_prepare_args);
	} else if (rectype == DB___txn_regop_rowlocks) {
		__txn_regop_rowlocks_args *txn_rl_args = NULL;
		if ((ret = __txn_regop_rowlocks_read(dbenv, rec.data,
						&txn_rl_args)) != 0)
			goto err;
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		rep->committed_lsn = *lsn = curlsn;
		rep->committed_gen = *gen = txn_rl_args->generation;
		if (rep->gen < rep->committed_gen) {
			__rep_set_gen(dbenv, __func__, __LINE__, rep->committed_gen);
			__rep_set_log_gen(dbenv, __func__, __LINE__, rep->gen);
		}
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		__os_free(dbenv, txn_rl_args);
	} else if (rectype == DB___txn_regop) {
		logmsg(LOGMSG_ERROR, "%s returning -1 on regop-record / "
				"recent-upgrade.\n", __func__);
		ret = -1;
	} else if (rectype == DB___txn_dist_commit) {
        __txn_dist_commit_args *txn_dist_commit_args = NULL;
        if ((ret = __txn_dist_commit_read(dbenv, rec.data,
                    &txn_dist_commit_args)) != 0)
            goto err;
        MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
        rep->committed_lsn = *lsn = curlsn;
        rep->committed_gen = *gen = txn_dist_commit_args->generation;
        if (rep->gen < rep->committed_gen)
            __rep_set_gen(dbenv, __func__, __LINE__, rep->committed_gen);
        MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
        __os_free(dbenv, txn_dist_commit_args);
	}

err:
	__log_c_close(logc);
	return ret;
}

/*
 * __rep_elect --
 *	Called after master failure to hold/participate in an election for
 *	a new master.
 */
static int
__rep_elect(dbenv, nsites, priority, timeout, newgen, already_master, eidp)
	DB_ENV *dbenv;
	int nsites, priority;
	u_int32_t timeout;
	u_int32_t *newgen;
    int *already_master;
	char **eidp;
{
	DB_LOG *dblp;
	DB_LSN lsn = {0};
	DB_REP *db_rep;
	REP *rep;
	int done, in_progress, ret, tiebreaker, use_committed_gen, send_vote2;
	u_int32_t egen, committed_gen = 0, orig_tally, pid, sec, usec;
	char *send_vote;

	*already_master = 0;
	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv, dbenv->rep_handle, "rep_elect", DB_INIT_REP);

	*newgen = 0;

	/* Error checking. */
	if (nsites <= 0) {
		__db_err(dbenv,
			"DB_ENV->rep_elect: nsites must be greater than 0");
		return (EINVAL);
	}
	if (priority < 0) {
		__db_err(dbenv,
			"DB_ENV->rep_elect: priority may not be negative");
		return (EINVAL);
	}

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;
	dblp = dbenv->lg_handle;

	/* This sets 'use_committed_gen' and feature-tests simultaneously */
	if ((use_committed_gen = dbenv->attr.elect_highest_committed_gen))
		__retrieve_logged_generation_commitlsn(dbenv, &lsn, &committed_gen);

	if (lsn.file == 0)
	{
		R_LOCK(dbenv, &dblp->reginfo);
		lsn = ((LOG *)dblp->reginfo.primary)->lsn;
		R_UNLOCK(dbenv, &dblp->reginfo);
	}

	orig_tally = 0;
	if ((ret = __rep_elect_init(dbenv,
		&lsn, nsites, priority, &in_progress, &orig_tally)) != 0) {
		if (ret == DB_REP_NEWMASTER) {
			ret = 0;
			*already_master = 1;
			*eidp = dbenv->rep_eid;
		}
		goto err;
	}
	/*
	 * If another thread is in the middle of an election we
	 * just quietly return and not interfere.
	 */
	if (in_progress) {
		*eidp = dbenv->rep_eid;
		logmsg(LOGMSG_DEBUG, "%s line %d returning %d master %s egen is %d\n",
				__func__, __LINE__, ret, *eidp, *newgen);
		return (0);
	}
#if 0
	fprintf(stderr, "%s:%d broadcasting REP_MASTER_REQ\n",
		__FILE__, __LINE__);
#endif
	send_master_req(dbenv, __func__, __LINE__);
	ret = __rep_wait(dbenv, timeout / 4, eidp, newgen, 0, REP_F_EPHASE1);
	switch (ret) {
	case 0:
		/* Check if we found a master. */
		if (*eidp != db_eid_invalid) {
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
				__db_err(dbenv, "Found master %d", *eidp);
#endif
			logmsg(LOGMSG_DEBUG, "%s line %d returning %d master %s egen is %d\n",
					__func__, __LINE__, ret, *eidp, *newgen);
			return (0);
		}
		/*
		 * If we didn't find a master, continue
		 * the election.
		 */
		break;
	case DB_TIMEOUT:
#ifdef DIAGNOSTIC
		if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
			__db_err(dbenv, "Did not find master.  Sending vote1");
#endif
		break;
	default:
		goto err;
	}
	/* Generate a randomized tiebreaker value. */
restart:
	__os_id(&pid);
	if ((ret = __os_clock(dbenv, &sec, &usec)) != 0)
		return (ret);
	tiebreaker = pid ^ sec ^ usec ^ (u_int) rand() ^ P_TO_UINT32(&pid);

	Pthread_mutex_lock(&rep_candidate_lock);
	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);

	/* WAITSTART pushes us past point of no-return */
	F_SET(rep, REP_F_EPHASE1 | REP_F_WAITSTART | REP_F_NOARCHIVE);
	F_CLR(rep, REP_F_TALLY);

	/* Grab lsn again now that we are a candidate.  The candidate-lock prevents
	 * Our log from being applied until we have resolved this election.  Just
	 * use 'current' lsn for the non-committed-gen case as it's broken anyway.*/

	if (use_committed_gen)
		lsn = rep->committed_lsn;

	static uint32_t last_egen = 0;
	if(last_egen && last_egen >= rep->egen)
		abort();
	last_egen = rep->egen;

	/* Tally our own vote */
	if (__rep_tally(dbenv, rep, rep->eid, &rep->sites, rep->egen,
		rep->tally_off, __func__, __LINE__) != 0)
		goto lockdone;
	__rep_cmp_vote(dbenv, rep, &rep->eid, rep->egen, &lsn, priority, 
			rep->gen, rep->committed_gen, tiebreaker);

#ifdef DIAGNOSTIC
	if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
		__db_err(dbenv, "Beginning an election");
#endif

	/* Now send vote */
	send_vote = db_eid_invalid;
	egen = rep->egen;
	committed_gen = rep->committed_gen;
	send_vote2 = (rep->sites >= rep->nsites && rep->w_priority != 0);
	logmsg(LOGMSG_DEBUG, "%s line %d send_vote2 is %d, rep->sites is %d, rep->nsites is %d\n",
			__func__, __LINE__, send_vote2, rep->sites, rep->nsites);

	/* If we have all vote1, change to PHASE2 immediately */
	if (send_vote2) {
		F_SET(rep, REP_F_EPHASE2);
		F_CLR(rep, REP_F_EPHASE1);
		if (rep->winner == rep->eid) {
			(void)__rep_tally(dbenv, rep, rep->eid,
					&rep->votes, egen, rep->v2tally_off, __func__, __LINE__);
		}
	}

	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
	Pthread_mutex_unlock(&rep_candidate_lock);

	if (use_committed_gen) {
		logmsg(LOGMSG_DEBUG, "%s line %d broadcasting REP_GEN_VOTE1 to all with committed-gen=%d gen=%d egen=%d\n",
			__func__, __LINE__, committed_gen, rep->gen, egen);
		__rep_send_gen_vote(dbenv, &lsn, nsites, priority, tiebreaker,
			egen, committed_gen, db_eid_broadcast, REP_GEN_VOTE1);
	} else {
		logmsg(LOGMSG_DEBUG, "%s line %d broadcasting REP_VOTE1 to all (committed-gen=0) gen=%d egen=%d\n",
			__func__, __LINE__, rep->gen, egen);
		__rep_send_vote(dbenv, &lsn, nsites, priority, tiebreaker, egen,
			db_eid_broadcast, REP_VOTE1);
	}

	ret = __rep_wait(dbenv, timeout, eidp, newgen, egen, REP_F_EPHASE1);
	switch (ret) {
		case 0:
			/* Check if election complete or phase complete. */
			if (*eidp != db_eid_invalid) {
#ifdef DIAGNOSTIC
				if (FLD_ISSET(dbenv->verbose,
					DB_VERB_REPLICATION))
					__db_err(dbenv,
						"Ended election phase 1 %d", ret);
#endif
				logmsg(LOGMSG_DEBUG, "%s line %d returning %d master %s egen is %d\n",
						__func__, __LINE__, ret, *eidp, *newgen);
				return (0);
			}
			goto phase2;
		case DB_ELECTION_GENCHG:
		case DB_TIMEOUT:
			break;
		default:
			goto err;
	}
	/*
	 * If we got here, we haven't heard from everyone, but we've
	 * run out of time, so it's time to decide if we have enough
	 * votes to pick a winner and if so, to send out a vote to
	 * the winner.
	 */
	Pthread_mutex_lock(&rep_candidate_lock);
	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	/*
	 * If our egen changed while we were waiting.  We need to
	 * essentially reinitialize our election.
	 */
	if (egen != rep->egen) {
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		Pthread_mutex_unlock(&rep_candidate_lock);
#ifdef DIAGNOSTIC
		if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
			__db_err(dbenv, "Egen changed from %lu to %lu",
				(u_long)egen, (u_long)rep->egen);
#endif
		goto restart;
	}

	*newgen = egen;

	if (rep->sites > rep->nsites / 2) {

		/* We think we've seen enough to cast a vote. */
		send_vote = rep->winner;
		/*
		 * See if we won.  This will make sure we
		 * don't count ourselves twice if we're racing
		 * with incoming votes.
		 */
		if (rep->winner == rep->eid) {
			(void)__rep_tally(dbenv, rep, rep->eid, &rep->votes,
				egen, rep->v2tally_off, __func__, __LINE__);
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
				__db_err(dbenv,
					"Counted my vote %d", rep->votes);
#endif
		}
		F_SET(rep, REP_F_EPHASE2);
		F_CLR(rep, REP_F_EPHASE1);
	}
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
	Pthread_mutex_unlock(&rep_candidate_lock);
	if (send_vote == db_eid_invalid) {
		/* We do not have enough votes to elect. */
#ifdef DIAGNOSTIC
		if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
			__db_err(dbenv,
				"Not enough votes to elect: received %d of %d",
				rep->sites, rep->nsites);
#endif
		ret = DB_REP_UNAVAIL;
		goto err;

	} else {
		/*
		 * We have seen enough vote1's.  Now we need to wait
		 * for all the vote2's.
		 */
		if (send_vote != rep->eid) {
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION) &&
				send_vote != rep->eid)
				__db_err(dbenv, "Sending vote");
#endif
			if (use_committed_gen) {
				logmsg(LOGMSG_DEBUG, "%s line %d sending REP_GEN_VOTE2 to %s "
						"with committed-gen=%d gen=%d egen=%d\n", __func__, __LINE__,
						send_vote, committed_gen, rep->gen, egen);
				__rep_send_gen_vote(dbenv, NULL, 0, 0, 0, egen,
					committed_gen, send_vote, REP_GEN_VOTE2);
			} else {
				logmsg(LOGMSG_DEBUG, "%s line %d sending REP_VOTE2 to %s "
						"(committed-gen=0) gen=%d egen=%d\n", __func__, __LINE__,
						send_vote, rep->gen, egen);
				__rep_send_vote(dbenv, NULL, 0, 0, 0, egen,
					send_vote, REP_VOTE2);
			}
		}
phase2:
		ret = __rep_wait(dbenv, timeout, eidp, newgen, 0, REP_F_EPHASE2);
#ifdef DIAGNOSTIC
				if (FLD_ISSET(dbenv->verbose,
					DB_VERB_REPLICATION))
					__db_err(dbenv,
						"Ended election phase 2 %d", ret);
#endif
		switch (ret) {
			case 0:
				logmsg(LOGMSG_DEBUG, "%s line %d returning %d master %s egen is %d\n",
						__func__, __LINE__, ret, *eidp, *newgen);
				return (0);
			case DB_TIMEOUT:
				ret = DB_REP_UNAVAIL;
				break;
			default:
				goto err;
		}
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		done = rep->votes > rep->nsites / 2;
#ifdef DIAGNOSTIC
		if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
			__db_err(dbenv,
				"After phase 2: done %d, votes %d, nsites %d",
				done, rep->votes, rep->nsites);
#endif
		if (send_vote == rep->eid && done) {
			if (nsites == 1)
				__rep_elect_master(dbenv, rep, eidp);
			logmsg(LOGMSG_DEBUG, "%s line %d elected master %s current-egen "
					"%d\n", __func__, __LINE__, rep->eid, rep->egen);
			ret = 0;
			goto lockdone;
		}
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
	}

err:	
	Pthread_mutex_lock(&rep_candidate_lock);
	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
lockdone:
	/*
	 * If we get here because of a non-election error, then we
	 * did not tally our vote.  The only non-election error is
	 * from elect_init where we were unable to grow_sites.  In
	 * that case we do not want to discard all known election info.
	 */
	logmsg(LOGMSG_DEBUG, "%s line %d ret is %d\n", __func__, __LINE__, ret);
	if (ret == 0 || ret == DB_REP_UNAVAIL) {
		__rep_elect_done(dbenv, rep, 0, __func__, __LINE__);
	} else if (orig_tally) {
		F_SET(rep, orig_tally);
	}

	Pthread_mutex_unlock(&rep_candidate_lock);
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
	logmsg(LOGMSG_DEBUG, "%s line %d returning %d master %s egen is %d\n",
			__func__, __LINE__, ret, *eidp, *newgen);
	return (ret);
}

/*
 * __rep_elect_init
 *	Initialize an election.  Sets beginp non-zero if the election is
 * already in progress; makes it 0 otherwise.
 */
static int
__rep_elect_init(dbenv, lsnp, nsites, priority, beginp, otally)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	int nsites, priority;
	int *beginp;
	u_int32_t *otally;
{
	DB_REP *db_rep;
	REP *rep;
	int ret;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	ret = 0;

	/* We may miscount, as we don't hold the replication mutex here. */
	rep->stat.st_elections++;

	/* If we are already a master; simply broadcast that fact and return. */
	if (F_ISSET(rep, REP_F_MASTER)) {
        logmsg(LOGMSG_DEBUG, "%s line %d sending REP_NEWMASTER\n", 
                __func__, __LINE__);
		(void)__rep_send_message(dbenv,
		    db_eid_broadcast, REP_NEWMASTER, lsnp, NULL, 0, NULL);
		rep->stat.st_elections_won++;
		return (DB_REP_NEWMASTER);
	}

	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	if (otally != NULL)
		*otally = F_ISSET(rep, REP_F_TALLY);
	*beginp = IN_ELECTION(rep);
	if (!*beginp) {
		/*
		 * Make sure that we always initialize all the election fields
		 * before putting ourselves in an election state.  That means
		 * issuing calls that can fail (allocation) before setting all
		 * the variables.
		 */
		if (nsites > rep->asites &&
		    (ret = __rep_grow_sites(dbenv, nsites)) != 0)
			goto err;
		DB_ENV_TEST_RECOVERY(dbenv, DB_TEST_ELECTINIT, ret, NULL);
		rep->nsites = nsites;
		rep->priority = priority;
		rep->master_id = db_eid_invalid;
	}
DB_TEST_RECOVERY_LABEL
err:	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
	return (ret);
}

/*
 * __rep_elect_master
 *	Set up for new master from election.  Must be called with
 *	the db_rep->rep_mutex held.
 *
 * PUBLIC: void __rep_elect_master __P((DB_ENV *, REP *, char **));
 */
void
__rep_elect_master(dbenv, rep, eidp)
	DB_ENV *dbenv;
	REP *rep;
	char **eidp;
{
	rep->master_id = rep->eid;
	F_SET(rep, REP_F_MASTERELECT);
	if (eidp != NULL)
		*eidp = rep->master_id;
	rep->stat.st_elections_won++;
#ifdef DIAGNOSTIC
	if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
		__db_err(dbenv,
    "Got enough votes to win; election done; winner is %d, gen %lu",
		    rep->master_id, (u_long)rep->gen);
#else
	COMPQUIET(dbenv, NULL);
#endif
}

extern pthread_mutex_t gbl_rep_egen_lk;
extern pthread_cond_t gbl_rep_egen_cd;

static int
__rep_wait(dbenv, timeout, eidp, outegen, inegen, flags)
	DB_ENV *dbenv;
	u_int32_t timeout;
	char **eidp;
    u_int32_t *outegen;
    u_int32_t inegen;
	u_int32_t flags;
{
	DB_REP *db_rep;
	REP *rep;
	int done;
    int genchange;
    int rc;
	u_int32_t sleeptime;
    struct timespec tm;

	done = 0;
    genchange = 0;
	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	/*
	 * The user specifies an overall timeout function, but checking
	 * is cheap and the timeout may be a generous upper bound.
	 * Sleep repeatedly for the smaller of .5s and timeout/10.
	 */
	while (timeout > 0) {
        sleeptime = (timeout > 5000000) ? (500000 * 1000) : (timeout * 1000) / 10;
        if (sleeptime == 0)
            sleeptime = 1000;

        clock_gettime(CLOCK_REALTIME, &tm);
        tm.tv_nsec += sleeptime;
        while (tm.tv_nsec >= 1000000000) {
            tm.tv_nsec -= 1000000000;
            tm.tv_sec += 1;
        }
        Pthread_mutex_lock(&gbl_rep_egen_lk);
        rc = pthread_cond_timedwait(&gbl_rep_egen_cd, &gbl_rep_egen_lk, &tm);
        if (rc && rc != ETIMEDOUT) 
            logmsg(LOGMSG_ERROR, "Err rc=%d from pthread_cond_timedwait\n", rc);

        *outegen = rep->egen;
        Pthread_mutex_unlock(&gbl_rep_egen_lk);
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		done = !F_ISSET(rep, flags) && rep->master_id != db_eid_invalid;

		*eidp = rep->master_id;
        /* Not sure if this is right */
        if (inegen && *outegen != inegen)
            genchange = 1;
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

		if (done) {
			return (0);
        }

        if (genchange)
            return (DB_ELECTION_GENCHG);

		if (timeout > sleeptime)
			timeout -= sleeptime;
		else
			timeout = 0;
	}
	return (DB_TIMEOUT);
}

/*
 * __rep_flush --
 *	Re-push the last log record to all clients, in case they've lost
 * messages and don't know it.
 */
static int
__rep_flush(dbenv)
	DB_ENV *dbenv;
{
	DBT rec;
	DB_LOGC *logc;
	DB_LSN lsn;
	int ret, t_ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv, dbenv->rep_handle, "rep_flush", DB_INIT_REP);

	if ((ret = __log_cursor(dbenv, &logc)) != 0)
		return (ret);

	memset(&rec, 0, sizeof(rec));
	memset(&lsn, 0, sizeof(lsn));

	if ((ret = __log_c_get(logc, &lsn, &rec, DB_LAST)) != 0)
		goto err;

	/* treat the end of the log as perm */
	(void)__rep_send_message(dbenv,
	    db_eid_broadcast, REP_LOG, &lsn, &rec, DB_LOG_PERM, NULL);

err:	if ((t_ret = __log_c_close(logc)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

int
__rep_get_eid(dbenv, eid_out)
	DB_ENV *dbenv;
	char **eid_out;
{
	DB_REP *db_rep;
	REP *rep;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv, dbenv->rep_handle, "rep_stat", DB_INIT_REP);

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	*eid_out = rep->eid;

	return 0;
}

/*
 * __rep_stat --
 *	Fetch replication statistics.
 */
int
__rep_get_master(dbenv, master_out, gen, egen)
	DB_ENV *dbenv;
	char **master_out;
	u_int32_t *gen;
	u_int32_t *egen;
{
	char *master = db_eid_invalid;
	DB_REP *db_rep;
	REP *rep;

	u_int32_t repflags;
	int dolock;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv, dbenv->rep_handle, "rep_stat", DB_INIT_REP);

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	repflags = rep->flags;
	if (FLD_ISSET(repflags, REP_F_RECOVER))
		dolock = 0;
	else {
		dolock = 1;
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	}

	master = rep->master_id;

	if (gen)
		*gen = rep->gen;

	if (egen)
		*egen = rep->egen;

	if (dolock) {
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
	}

	*master_out = master;
	return 0;
}

extern pthread_mutex_t gbl_durable_lsn_lk;

static int
__rep_deadlocks(dbenv, deadlocks)
    DB_ENV *dbenv;
    u_int64_t *deadlocks;
{
	DB_REP *db_rep = dbenv->rep_handle;
	REP *rep = db_rep->region;
    *deadlocks = ATOMIC_LOAD64(rep->stat.retry);
    return 0;
}

/*
 * __rep_stat --
 *	Fetch replication statistics.
 */
static int
__rep_stat(dbenv, statp, flags)
	DB_ENV *dbenv;
	DB_REP_STAT **statp;
	u_int32_t flags;
{
	DB_LOG *dblp;
	DB_REP *db_rep;
	DB_REP_STAT *stats;
	LOG *lp;
	REP *rep;
	u_int32_t queued, repflags;
	int dolock, ret;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv, dbenv->rep_handle, "rep_stat", DB_INIT_REP);

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;
	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;

	*statp = NULL;
	if ((ret = __db_fchk(dbenv,
	    "DB_ENV->rep_stat", flags, DB_STAT_CLEAR)) != 0)
		return (ret);

	/* Allocate a stat struct to return to the user. */
	if ((ret = __os_umalloc(dbenv, sizeof(DB_REP_STAT), &stats)) != 0)
		return (ret);

	/*
	 * Read without holding the lock.  If we are in client
	 * recovery, we copy just the stats struct so we won't
	 * block.  We only copy out those stats that don't
	 * require acquiring any mutex.
	 */
	repflags = rep->flags;
	if (FLD_ISSET(repflags, REP_F_RECOVER))
		dolock = 0;
	else {
		dolock = 1;
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	}
	memcpy(stats, &rep->stat, sizeof(*stats));

	/* Copy out election stats. */
	if (IN_ELECTION_TALLY(rep)) {
		if (F_ISSET(rep, REP_F_EPHASE1))
			stats->st_election_status = 1;
		else if (F_ISSET(rep, REP_F_EPHASE2))
			stats->st_election_status = 2;

		stats->st_election_nsites = rep->sites;
		stats->st_election_cur_winner = rep->winner;
		stats->st_election_priority = rep->w_priority;
		stats->st_election_gen = rep->w_gen;
		stats->st_election_lsn = rep->w_lsn;
		stats->st_election_votes = rep->votes;
		stats->st_election_tiebreaker = rep->w_tiebreaker;
	}

	/* Copy out other info that's protected by the rep mutex. */
	stats->st_env_id = rep->eid;
	stats->st_env_priority = rep->priority;
	stats->st_nsites = rep->nsites;
	stats->st_master = rep->master_id;
	stats->st_gen = rep->gen;

	if (F_ISSET(rep, REP_F_MASTER))
		stats->st_status = DB_REP_MASTER;
	else if (F_ISSET(rep, REP_F_LOGSONLY))
		stats->st_status = DB_REP_LOGSONLY;
	else if (F_ISSET(rep, REP_F_UPGRADE))
		stats->st_status = DB_REP_CLIENT;
	else
		stats->st_status = 0;

	if (LF_ISSET(DB_STAT_CLEAR)) {
		queued = rep->stat.st_log_queued;
		memset(&rep->stat, 0, sizeof(rep->stat));
		rep->stat.st_log_queued = rep->stat.st_log_queued_total =
		    rep->stat.st_log_queued_max = queued;
	}

	if (dolock) {
		stats->st_in_recovery = 0;
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		MUTEX_LOCK(dbenv, db_rep->db_mutexp);
	} else
		stats->st_in_recovery = 1;

	/*
	 * Log-related replication info is stored in the log system and
	 * protected by the log region lock.
	 */
	if (F_ISSET(rep, REP_ISCLIENT)) {
		stats->st_next_lsn = lp->ready_lsn;
		stats->st_waiting_lsn = lp->waiting_lsn;
	} else {
		if (F_ISSET(rep, REP_F_MASTER))
			stats->st_next_lsn = lp->lsn;
		else
			ZERO_LSN(stats->st_next_lsn);
		ZERO_LSN(stats->st_waiting_lsn);
	}
	if (dolock)
		MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);

	stats->lc_cache_hits = rep->stat.lc_cache_hits;
	stats->lc_cache_misses = rep->stat.lc_cache_misses;
	stats->lc_cache_size = dbenv->lc_cache.memused;
	Pthread_mutex_lock(&gbl_durable_lsn_lk);
	stats->durable_lsn = dbenv->durable_lsn;
	stats->durable_gen = dbenv->durable_generation;
	Pthread_mutex_unlock(&gbl_durable_lsn_lk);

	*statp = stats;
	return (0);
}

/* COMDB2 MODIFICATION */
int
berkdb_is_recovering(DB_ENV *dbenv)
{

	return IS_RECOVERING(dbenv);
}
