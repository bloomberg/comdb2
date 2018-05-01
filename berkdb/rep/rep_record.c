/*- * See the file LICENSE for redistribution information.

 *
 * Copyright (c) 2001-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"
#include "dbinc/db_swap.h"

#ifndef lint
static const char revid[] =
    "$Id: rep_record.c,v 1.193 2003/11/14 05:32:31 ubell Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <pthread.h>
#include <unistd.h>
#endif

#include <stdlib.h>
#include <dlmalloc.h>
#include <alloca.h>

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/db_shash.h"
#include "dbinc/db_am.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"
#include <limits.h>

#include "dbinc/hmac.h"
#include <ctrace.h>
#include <sys/poll.h>

#include "dbinc_auto/fileops_auto.h"
#include "dbinc_auto/qam_auto.h"
#include "dbinc_auto/txn_auto.h"

#include "printformats.h"

#include <list.h>
#include <bbhrtime.h>
#include <epochlib.h>
#include "schema_lk.h"
#include "logmsg.h"
#include <errno.h>


#ifndef TESTSUITE
int bdb_am_i_coherent(void *bdb_state);
void bdb_thread_event(void *bdb_state, int event);
void bdb_get_writelock(void *bdb_state,
    const char *idstr, const char *funcname, int line);
void bdb_rellock(void *bdb_state, const char *funcname, int line);
int bdb_is_open(void *bdb_state);

extern int gbl_rep_printlock;
extern int gbl_dispatch_rowlocks_bench;
extern int gbl_rowlocks_bench_logical_rectype;
extern int gbl_rowlocks;
extern int gbl_optimize_truncate_repdb;
extern int gbl_early;
extern int gbl_reallyearly;
extern int gbl_rep_process_txn_time;
int gbl_rep_badgen_trace;

void hexdump(unsigned char *key, int keylen);
extern void fsnapf(FILE *, void *, int);
static int reset_recovery_processor(struct __recovery_processor *rp);

#define BDB_WRITELOCK(idstr)    bdb_get_writelock(bdb_state, (idstr), __func__, __LINE__)
#define BDB_RELLOCK()           bdb_rellock(bdb_state, __func__, __LINE__)

#else

#define BDB_WRITELOCK(x)
#define BDB_RELLOCK()

#endif


extern int bdb_purge_logical_transactions(void *statearg, DB_LSN *trunclsn);
extern int set_commit_context(unsigned long long context, uint32_t *generation,
    void *plsn, void *args, unsigned int rectype);
extern int gbl_berkdb_verify_skip_skipables;

static int __rep_apply __P((DB_ENV *, REP_CONTROL *, DBT *, DB_LSN *,
	uint32_t *));
static int __rep_dorecovery __P((DB_ENV *, DB_LSN *, DB_LSN *));
static int __rep_lsn_cmp __P((const void *, const void *));
static int __rep_newfile __P((DB_ENV *, REP_CONTROL *, DB_LSN *));
static int __rep_verify_match __P((DB_ENV *, REP_CONTROL *, time_t));

int64_t gbl_rep_trans_parallel = 0, gbl_rep_trans_serial =
    0, gbl_rep_trans_deadlocked = 0, gbl_rep_trans_inline =
    0, gbl_rep_rowlocks_multifile = 0;

static inline int wait_for_running_transactions(DB_ENV *dbenv);

#define	IS_SIMPLE(R)	((R) != DB___txn_regop && (R) != DB___txn_xa_regop && \
    (R) != DB___txn_regop_rowlocks && (R) != DB___txn_regop_gen && (R) != \
    DB___txn_ckp && (R) != DB___dbreg_register)

int gbl_rep_process_msg_print_rc;

#define PRINT_RETURN(retrc, fromline)										\
	do {																	\
		static uint32_t lastpr = 0;											\
		static uint32_t count = 0;											\
		uint32_t now;														\
		count++;															\
		if (gbl_rep_process_msg_print_rc && ((now = time(NULL)) - lastpr)) {\
			logmsg(LOGMSG_ERROR,											\
				"td %u %s line %d from line %d returning %d, count=%u\n",	\
				(uint32_t)pthread_self(), __func__, __LINE__, fromline,		\
				retrc, count);												\
		}																	\
		return retrc;														\
	} while (0);

/* Used to consistently designate which messages ought to be received where. */

#ifdef DIAGNOSTIC
#define MASTER_ONLY(rep, rp)											 	\
	do {																 	\
		if (!F_ISSET(rep, REP_F_MASTER)) {									\
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION)) {			\
				__db_err(dbenv, "Master record received on client");		\
				__rep_print_message(dbenv, *eidp, rp, "rep_process_message");\
			}															 	\
			ret = DB_REP_STALEMASTER;										\
			fromline = __LINE__;										 	\
			goto errlock;												 	\
		}																	\
	} while (0)

#define CLIENT_ONLY(rep, rp)												\
	do {																	\
		if (!F_ISSET(rep, REP_ISCLIENT)) {									\
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION)) {			\
				__db_err(dbenv, "Client record received on master");		\
				__rep_print_message(dbenv, *eidp, rp, "rep_process_message");\
			}																\
			(void)__rep_send_message(dbenv, db_eid_broadcast, REP_DUPMASTER,\
									 NULL, NULL, 0, NULL);					\
			ret = DB_REP_DUPMASTER;											\
			fromline = __LINE__;											\
			goto errlock;													\
																			\
		}																	\
	} while (0)

#define MASTER_CHECK(dbenv, eid, rep)										\
	do {																	\
		if (rep->master_id == db_eid_invalid) {								\
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))				\
				__db_err(dbenv, "Received record from %s, master is INVALID",\
						 eid);												\
			ret = 0;														\
			(void)__rep_send_message(dbenv, db_eid_broadcast, REP_MASTER_REQ,\
									 NULL, NULL, 0, NULL);					\
			fromline = __LINE__;											\
			goto errlock;													\
		}																	\
		if (eid != rep->master_id) {										\
			char ip1[16], ip2[16];											\
			__db_err(dbenv,													\
					 "Received master record from %d (%s), master is %d (%s)",\
					 eid inet_ntoa_r(eid, ip1), rep->master_id,				\
					 inet_ntoa_r(rep->master_id, ip2));						\
			ret = DB_REP_STALEMASTER;										\
			fromline = __LINE__;											\
			goto errlock;													\
		}																	\
	} while (0)
#else
#define MASTER_ONLY(rep, rp)												\
	do {																	\
		if (!F_ISSET(rep, REP_F_MASTER)) {									\
			__db_err(dbenv,													\
					 "Received master-only request on client, master is %s",\
					 rep->master_id);										\
			ret = DB_REP_STALEMASTER;										\
			fromline = __LINE__;											\
			goto errlock;													\
		}																	\
	} while (0)

#define CLIENT_ONLY(rep, rp)												\
	do {																	\
		if (!F_ISSET(rep, REP_ISCLIENT)) {									\
			(void)__rep_send_message(dbenv, db_eid_broadcast, REP_DUPMASTER,\
									 NULL, NULL, 0, NULL);					\
			ret = DB_REP_DUPMASTER;											\
			fromline = __LINE__;											\
			goto errlock;													\
		}																	\
	} while (0)

#define MASTER_CHECK(dbenv, eid, rep)										\
	do {																	\
		if (rep->master_id == db_eid_invalid) {								\
			ret = 0;														\
			(void)__rep_send_message(dbenv, db_eid_broadcast, REP_MASTER_REQ,\
									 NULL, NULL, 0, NULL);					\
			fromline = __LINE__;											\
			goto errlock;													\
		}																	\
		if (eid != rep->master_id) {										\
			__db_err(dbenv, "Received master record from %s, master is %s",	\
					 eid, rep->master_id);									\
			ret = DB_REP_STALEMASTER;										\
			fromline = __LINE__;											\
			goto errlock;													\
		}																	\
	} while (0)
#endif

#define	ANYSITE(rep)


/*
 * __rep_vote_info_swap
 *  Swap the bytes in a rep vote_info message from machines with
 *  different endianness
 *
 * PUBLIC: void __rep_vote_info_swap __P((REP_VOTE_INFO *vi));
 */
void
__rep_vote_info_swap(vi)
	REP_VOTE_INFO *vi;
{
	M_32_SWAP(vi->egen);
	M_32_SWAP(vi->nsites);
	M_32_SWAP(vi->priority);
	M_32_SWAP(vi->tiebreaker);
}

/*
 * __rep_gen_vote_info_swap
 *  Swap the bytes in a rep vote_info message from machines with
 *  different endianness
 *
 * PUBLIC: void __rep_gen_vote_info_swap __P((REP_GEN_VOTE_INFO *vi));
 */

void
__rep_gen_vote_info_swap(vi)
	REP_GEN_VOTE_INFO *vi;
{
	M_32_SWAP(vi->egen);
	M_32_SWAP(vi->last_write_gen);
	M_32_SWAP(vi->nsites);
	M_32_SWAP(vi->priority);
	M_32_SWAP(vi->tiebreaker);
}


/*
 * __rep_control_swap
 *  Swap the bytes in a rep control message header from machines with
 *  different endianness
 *
 * PUBLIC: void __rep_control_swap __P((REP_CONTROL *rp));
 */
void
__rep_control_swap(rp)
	REP_CONTROL *rp;
{
	DB_LSN tmplsn;

	M_32_SWAP(rp->rep_version);
	M_32_SWAP(rp->log_version);
	LOGCOPY_FROMLSN(&tmplsn, &rp->lsn);
	memcpy(&rp->lsn, &tmplsn, sizeof(tmplsn));
	M_32_SWAP(rp->rectype);
	M_32_SWAP(rp->gen);
	M_32_SWAP(rp->flags);
}

int gbl_verify_rep_log_records = 0;

static void
lc_free(DB_ENV *dbenv, struct __recovery_processor *rp, LSN_COLLECTION * lc)
{
	for (int i = 0; i < lc->nlsns; i++) {
		if (lc->array[i].rec.data &&
		    lc->array[i].rec.flags == DB_DBT_USERMEM) {
			comdb2_free(lc->array[i].rec.data);
			lc->array[i].rec.data = NULL;
			lc->array[i].rec.flags = 0;
		} else if (lc->array[i].rec.data) {
			__os_free(dbenv, lc->array[i].rec.data);
			lc->array[i].rec.data = NULL;
			lc->array[i].rec.flags = 0;
		}
	}
	if (lc->nalloc)
		__os_free(dbenv, lc->array);
	lc->array = NULL;
	lc->nlsns = 0;
	lc->nalloc = 0;
	lc->memused = 0;
	lc->nalloc = 0;
}

/*
 * matchable_log_type --
 *
 * PUBLIC: int matchable_log_type __P((int));
 */
int
matchable_log_type(int rectype)
{
	extern int gbl_only_match_commit_records;
	int ret;
	if (gbl_only_match_commit_records) {
		ret = (rectype == DB___txn_regop ||
		    rectype == DB___txn_regop_gen ||
		    rectype == DB___txn_regop_rowlocks);
	} else {
		switch (rectype) {
		case DB___txn_recycle:
		case DB___txn_ckp:
		case DB___dbreg_register:
		case DB___db_debug:
			ret = 0;
			break;

		default:
			ret = 1;
			break;
		}
	}
	return ret;
}

int gbl_rep_verify_will_recover_trace = 0;
int gbl_rep_verify_always_grab_writelock = 0;

/*
 * __rep_verify_will_recover --
 *
 * This routine returns non-zero if a subsequent call to __rep_process_message
 * will run recovery.
 *
 * PUBLIC: int __rep_verify_will_recover __P((DB_ENV *, DBT *, DBT *));
 */

int
__rep_verify_will_recover(dbenv, control, rec)
	DB_ENV *dbenv;
	DBT *control, *rec;
{
	DB_LOG *dblp;
	DBT mylog;
	REP_CONTROL *rp;
	DB_LSN lsn;
	LOG *lp;
	DB_LOGC *logc;
	int will_recover = 0;
	int ret;
	u_int32_t rectype;
	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;
	rp = (REP_CONTROL *)control->data;

	if (gbl_rep_verify_always_grab_writelock)
		return 1;

	if (LOG_SWAPPED())
		__rep_control_swap(rp);

	if (IS_ZERO_LSN(lp->verify_lsn))
		goto done;

	if ((ret = __log_cursor(dbenv, &logc)) != 0)
		goto done;

	memset(&mylog, 0, sizeof(mylog));

	if ((ret = __log_c_get(logc, &rp->lsn, &mylog, DB_SET)) != 0) {
		will_recover = 1;
		goto close_cursor;
    }

	if (mylog.size == rec->size &&
			memcmp(mylog.data, rec->data, rec->size) == 0)
		will_recover = 1;

	LOGCOPY_32(&rectype, mylog.data);

	if ((will_recover == 1 && !matchable_log_type(rectype)) &&
			((ret = __log_c_get(logc, &lsn, &mylog, DB_PREV)) == 0)){
		will_recover = 0;
	}

close_cursor:
	__log_c_close(logc);

done:
	if (LOG_SWAPPED())
		__rep_control_swap(rp);

	if (gbl_rep_verify_will_recover_trace)
		logmsg(LOGMSG_ERROR, "%s is returning %d\n", __func__, will_recover);

	return will_recover;
}


    extern int gbl_verbose_master_req;


/*
 * __rep_process_message --
 *
 * This routine takes an incoming message and processes it.
 *
 * control: contains the control fields from the record
 * rec: contains the actual record
 * eidp: contains the machine id of the sender of the message;
 *	in the case of a DB_NEWMASTER message, returns the eid
 *	of the new master.
 * ret_lsnp: On DB_REP_ISPERM and DB_REP_NOTPERM returns, contains the
 *	lsn of the maximum permanent or current not permanent log record
 *	(respectively).
 *
 * PUBLIC: int __rep_process_message __P((DB_ENV *, DBT *, DBT *, char**,
 * PUBLIC:     DB_LSN *, uint32_t *));
 */

int
__rep_process_message(dbenv, control, rec, eidp, ret_lsnp, commit_gen)
	DB_ENV *dbenv;
	DBT *control, *rec;
	char **eidp;
	DB_LSN *ret_lsnp;
	uint32_t *commit_gen;
{
	int fromline;
	DB_LOG *dblp;
	DB_LOGC *logc;
	DB_LSN endlsn, lsn, oldfilelsn, tmplsn;
	DB_REP *db_rep;
	DBT *d, data_dbt, mylog;
	LOG *lp;
	REP *rep;
	REP_CONTROL *rp;
	REP_VOTE_INFO *vi;
	REP_GEN_VOTE_INFO *vig;
	u_int32_t bytes, egen, committed_gen, flags, gen, gbytes, rectype, type;
	int check_limit, cmp, done, do_req;
	int match, old, recovering, ret, t_ret;
	time_t savetime;
#if defined INSTRUMENT_REP_APPLY
	static unsigned long long rpm_count = 0;
	static int rpm_pr = 0;
	int rpm_now;
#endif
    static time_t verify_req_print = 0;
    static unsigned long long verify_req_count = 0;
    time_t now;


	u_int32_t vi_last_write_gen, vi_egen;
	int vi_nsites, vi_priority, vi_tiebreaker;

	char *master;

	PANIC_CHECK(dbenv);
	ENV_REQUIRES_CONFIG(dbenv, dbenv->rep_handle, "rep_process_message",
	    DB_INIT_REP);

	/* Control argument must be non-Null. */
	if (control == NULL || control->size == 0) {
		__db_err(dbenv,
		    "DB_ENV->rep_process_message: control argument must be specified");
		return (EINVAL);
	}

	if (!IS_REP_MASTER(dbenv) && !IS_REP_CLIENT(dbenv)) {
		__db_err(dbenv,
		    "Environment not configured as replication master or client");
		return (EINVAL);
	}

	ret = 0;
	db_rep = dbenv->rep_handle;
	rep = db_rep->region;
	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;
	rp = (REP_CONTROL *)control->data;

	if (LOG_SWAPPED())
		__rep_control_swap(rp);

	if (gbl_verbose_master_req) {
		switch (rp->rectype) {
			case REP_MASTER_REQ:
				logmsg(LOGMSG_USER, "%s processing REP_MASTER_REQ\n",
					__func__);
				break;
			case REP_NEWMASTER:
				logmsg(LOGMSG_USER, "%s processing REP_NEWMASTER\n", __func__);
				break;
				default: 
					break;
		}
	}

#if defined INSTRUMENT_REP_APPLY
	rpm_count++;
	rpm_now = time(NULL);
	if (rpm_pr - rpm_now > 0) {
		logmsg(LOGMSG_INFO, "Got %llu rep_process_messages, now=%d\n",
		    rpm_count, rpm_now);
		rpm_pr = rpm_now;
	}
#endif
	/*
	 * Acquire the replication lock.
	 */
	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	if (rep->start_th != 0) {
		/*
		 * If we're racing with a thread in rep_start, then
		 * just ignore the message and return.
		 */
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		if (F_ISSET(rp, DB_LOG_PERM)) {
			if (ret_lsnp != NULL) {
				*ret_lsnp = rp->lsn;
			}
			PRINT_RETURN (DB_REP_NOTPERM, __LINE__);
		} else
			PRINT_RETURN (0, __LINE__);
	}
	if (rep->in_recovery != 0) {
		/*
		 * If we're racing with a thread in __db_apprec,
		 * just ignore the message and return.
		 */
		rep->stat.st_msgs_recover++;
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		PRINT_RETURN (0, __LINE__);
	}
	rep->msg_th++;
	gen = rep->gen;
	recovering = rep->in_recovery ||
	    F_ISSET(rep, REP_F_READY | REP_F_RECOVER);
	savetime = rep->timestamp;

	rep->stat.st_msgs_processed++;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

#ifdef DIAGNOSTIC
	if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
		__rep_print_message(dbenv, *eidp, rp, "rep_process_message");
#endif

	/* Complain if we see an improper version number. */
	if (rp->rep_version != DB_REPVERSION) {
		__db_err(dbenv,
		    "unexpected replication message version %lu, expected %d",
		    (u_long)rp->rep_version, DB_REPVERSION);
		ret = EINVAL;
		fromline = __LINE__;
		goto errlock;
	}
	if (rp->log_version != DB_LOGVERSION) {
		__db_err(dbenv,
		    "unexpected log record version %lu, expected %d",
		    (u_long)rp->log_version, DB_LOGVERSION);
		ret = EINVAL;
		fromline = __LINE__;
		goto errlock;
	}

	/*
	 * Check for generation number matching.  Ignore any old messages
	 * except requests that are indicative of a new client that needs
	 * to get in sync.
	 */
	if (rp->gen < gen && rp->rectype != REP_ALIVE_REQ &&
		rp->rectype != REP_NEWCLIENT && rp->rectype != REP_MASTER_REQ) {
		/*
		 * We don't hold the rep mutex, and could miscount if we race.
		 */
		rep->stat.st_msgs_badgen++;

		static u_int32_t lastpr = 0;
		u_int32_t now;
		if (gbl_rep_badgen_trace && ((now = time(NULL)) - lastpr)) {
			logmsg(LOGMSG_ERROR, "Ignoring rp->gen %u from %s mygen is %u, "
				"rectype=%u cnt %u\n", rp->gen, *eidp, gen, rp->rectype, 
				rep->stat.st_msgs_badgen);
			lastpr = now;
		}

		fromline = __LINE__;
		goto errlock;
	}

	if (rp->gen > gen) {
		/*
		 * If I am a master and am out of date with a lower generation
		 * number, I am in bad shape and should downgrade.
		 */
			static u_int32_t lastpr = 0;
			u_int32_t now;
			if (gbl_rep_badgen_trace && ((now = time(NULL)) - lastpr)) {
				logmsg(LOGMSG_ERROR, "rp->gen %u from %s is larger than "
					"mygen %u, rectype=%u\n", rp->gen, *eidp, gen, rp->rectype);
				lastpr = now;
			}

		if (F_ISSET(rep, REP_F_MASTER)) {
			rep->stat.st_dupmasters++;
			ret = DB_REP_DUPMASTER;
			if (rp->rectype != REP_DUPMASTER)
				(void)__rep_send_message(dbenv,
					db_eid_broadcast, REP_DUPMASTER,
					NULL, NULL, 0, NULL);
			fromline = __LINE__;
			goto errlock;
		}

		/*
		 * I am a client and am out of date.  If this is an election,
		 * or a response from the first site I contacted, then I can
		 * accept the generation number and participate in future
		 * elections and communication. Otherwise, I need to hear about
		 * a new master and sync up.
		 */
		if (rp->rectype == REP_ALIVE ||
		    rp->rectype == REP_VOTE1 || rp->rectype == REP_VOTE2 ||
		    rp->rectype == REP_GEN_VOTE1 ||
		    rp->rectype == REP_GEN_VOTE2) {
			MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
				__db_err(dbenv, "Updating gen from %lu to %lu",
				    (u_long)gen, (u_long)rp->gen);
#endif
			gen = rep->gen = rp->gen;
			if (rep->egen <= gen)
				rep->egen = rep->gen + 1;
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
				__db_err(dbenv, "Updating egen to %lu",
				    (u_long)rep->egen);
#endif
			MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		} else if (rp->rectype != REP_NEWMASTER) {

			if (gbl_verbose_master_req) {
				logmsg(LOGMSG_USER, "%s line %d sending REP_MASTER_REQ\n",
						__func__, __LINE__);
			}
			(void)__rep_send_message(dbenv,
			    db_eid_broadcast, REP_MASTER_REQ, NULL, NULL, 0,
			    NULL);
			fromline = __LINE__;
			goto errlock;
		}

		/*
		 * If you get here, then you're a client and either you're
		 * in an election or you have a NEWMASTER or an ALIVE message
		 * whose processing will do the right thing below.
		 */

	}

	/*
	 * We need to check if we're in recovery and if we are
	 * then we need to ignore any messages except VERIFY*, VOTE*,
	 * NEW* and ALIVE_REQ.
	 */
	if (recovering) {
		switch (rp->rectype) {
		case REP_VERIFY:
			MUTEX_LOCK(dbenv, db_rep->db_mutexp);
			cmp = log_compare(&lp->verify_lsn, &rp->lsn);
			MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);
			if (cmp != 0) {
#if 0
				fprintf(stderr,
				    "Skipping ver %d:%d got %d:%d\n",
				    lp->verify_lsn.file, lp->verify_lsn.offset,
				    rp->lsn.file, rp->lsn.offset);
#endif
				goto skip;
			}
			break;
		case REP_ALIVE:
		case REP_ALIVE_REQ:
		case REP_DUPMASTER:
		case REP_NEWCLIENT:
		case REP_NEWMASTER:
		case REP_NEWSITE:
		case REP_VERIFY_FAIL:
		case REP_VOTE1:
		case REP_VOTE2:
		case REP_GEN_VOTE1:
		case REP_GEN_VOTE2:
			break;
		default:
skip:				/*
				 * We don't hold the rep mutex, and could
				 * miscount if we race.
				 */
			rep->stat.st_msgs_recover++;

			/* Check for need to retransmit. */
			MUTEX_LOCK(dbenv, db_rep->db_mutexp);
			MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
			do_req = ++lp->rcvd_recs >= lp->wait_recs;
			MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
			if (do_req) {

				lp->wait_recs *= 2;

				if (lp->wait_recs > rep->max_gap)
					lp->wait_recs = rep->max_gap;
				lp->rcvd_recs = 0;
				lsn = lp->verify_lsn;
			}
			MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);
			if (do_req) {
				/*
				 * Don't respond to a MASTER_REQ with
				 * a MASTER_REQ.
				 */
				if (rep->master_id == db_eid_invalid &&
				    rp->rectype != REP_MASTER_REQ) {
                    static time_t master_req_print = 0;
                    static unsigned long long master_req_count = 0;

                    master_req_count++;
                    if ((now = time(NULL)) > master_req_print) {
                        logmsg(LOGMSG_INFO, "%s line %d: recovery requesting_master count=%llu\n", 
                                __func__, __LINE__, master_req_count);
                        master_req_print = now;
                    }

					if (gbl_verbose_master_req) {
						logmsg(LOGMSG_USER, "%s line %d sending REP_MASTER_REQ\n",
								__func__, __LINE__);
					}

					(void)__rep_send_message(dbenv,
					    db_eid_broadcast,
					    REP_MASTER_REQ,
					    NULL, NULL, 0, NULL);
				} else if (*eidp == rep->master_id) {
                    verify_req_count++;
                    if ((now = time(NULL)) > verify_req_print) {
                        logmsg(LOGMSG_INFO, "%s line %d: recovery sending verify_req count=%llu lsn [%d][%d]\n", 
                                __func__, __LINE__, verify_req_count, lsn.file, lsn.offset);
                        verify_req_print = now;
                    }
#if 0
					fprintf(stderr,
					    "%s:%d Requesting REP_VERIFY_REQ %d:%d\n",
					    __FILE__, __LINE__, lsn.file,
					    lsn.offset);
#endif

					(void)__rep_send_message(dbenv, *eidp,
					    REP_VERIFY_REQ,
					    &lsn, NULL, 0, NULL);
				}
			}
			fromline = __LINE__;
			goto errlock;
		}
	}

	switch (rp->rectype) {
	case REP_ALIVE:
		ANYSITE(rep);
		egen = *(u_int32_t *)rec->data;
		if (LOG_SWAPPED())
			M_32_SWAP(egen);
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
#ifdef DIAGNOSTIC
		if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
			__db_err(dbenv, "Received ALIVE egen of %lu, mine %lu",
			    (u_long)egen, (u_long)rep->egen);
#endif
		if (egen > rep->egen)
			rep->egen = egen;
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		break;
	case REP_ALIVE_REQ:
		ANYSITE(rep);
		dblp = dbenv->lg_handle;
		R_LOCK(dbenv, &dblp->reginfo);
		lsn = ((LOG *)dblp->reginfo.primary)->lsn;
		R_UNLOCK(dbenv, &dblp->reginfo);
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		egen = rep->egen;
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		if (LOG_SWAPPED())
			M_32_SWAP(egen);
		data_dbt.data = &egen;
		data_dbt.size = sizeof(egen);
		(void)__rep_send_message(dbenv,
		    *eidp, REP_ALIVE, &lsn, &data_dbt, 0, NULL);
		fromline = __LINE__;
		goto errlock;
	case REP_DUPMASTER:
		if (F_ISSET(rep, REP_F_MASTER))
			ret = DB_REP_DUPMASTER;
		fromline = __LINE__;
		goto errlock;
	case REP_ALL_REQ:
		MASTER_ONLY(rep, rp);
		gbytes = bytes = 0;
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		gbytes = rep->gbytes;
		bytes = rep->bytes;
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		check_limit = gbytes != 0 || bytes != 0;
		fromline = __LINE__;
		if ((ret = __log_cursor(dbenv, &logc)) != 0)
			goto errlock;
		/* A confused replicant can send a request
		 * for an invalid log record, and cause the master
		 * to panic.  Don't let that happen. */
		F_SET(logc, DB_LOG_NO_PANIC);
		memset(&data_dbt, 0, sizeof(data_dbt));
		oldfilelsn = lsn = rp->lsn;
		type = REP_LOG;
		flags = IS_ZERO_LSN(rp->lsn) ||
		    IS_INIT_LSN(rp->lsn) ? DB_FIRST : DB_SET;
		for (ret = __log_c_get(logc, &lsn, &data_dbt, flags);
		    ret == 0 && type == REP_LOG;
		    ret = __log_c_get(logc, &lsn, &data_dbt, DB_NEXT)) {
			/*
			 * When a log file changes, we'll have a real log
			 * record with some lsn [n][m], and we'll also want
			 * to send a NEWFILE message with lsn [n-1][MAX].
			 */
			if (lsn.file != oldfilelsn.file)
				(void)__rep_send_message(dbenv,
				    *eidp, REP_NEWFILE, &oldfilelsn, NULL,
				    DB_REP_NOBUFFER, NULL);
			if (check_limit) {
				/*
				 * data_dbt.size is only the size of the log
				 * record;  it doesn't count the size of the
				 * control structure. Factor that in as well
				 * so we're not off by a lot if our log records
				 * are small.
				 */
				while (bytes <
				    data_dbt.size + sizeof(REP_CONTROL)) {
					if (gbytes > 0) {
						bytes += GIGABYTE;
						--gbytes;
						continue;
					}
					/*
					 * We don't hold the rep mutex,
					 * and may miscount.
					 */
					rep->stat.st_nthrottles++;
					type = REP_LOG_MORE;
					goto send;
				}
				bytes -= (data_dbt.size + sizeof(REP_CONTROL));
			}

send:			if (__rep_send_message(dbenv,
				*eidp, type, &lsn, &data_dbt, DB_REP_NOBUFFER,
				NULL) != 0)
				break;

			/*
			 * If we are about to change files, then we'll need the
			 * last LSN in the previous file.  Save it here.
			 */
			oldfilelsn = lsn;
			oldfilelsn.offset += logc->c_len;
		}

		if (ret == DB_NOTFOUND)
			ret = 0;
		if ((t_ret = __log_c_close(logc)) != 0 && ret == 0)
			ret = t_ret;
		fromline = __LINE__;
		goto errlock;
#ifdef NOTYET
	case REP_FILE:		/* TODO */
		CLIENT_ONLY(rep, rp);
		MASTER_CHECK(dbenv, *eidp, rep);
		break;
	case REP_FILE_REQ:
		MASTER_ONLY(rep, rp);
		ret = __rep_send_file(dbenv, rec, *eidp);
		fromline = __LINE__;
		goto errlock;
#endif
	case REP_LOG:
	case REP_LOG_MORE:
		CLIENT_ONLY(rep, rp);
		MASTER_CHECK(dbenv, *eidp, rep);
		if (!IN_ELECTION_TALLY(rep)) {
			fromline = __LINE__;
			if ((ret = __rep_apply(dbenv, rp, rec, ret_lsnp,
								   commit_gen)) != 0)
				goto errlock;
		} else {

			if (gbl_verbose_master_req) {
				logmsg(LOGMSG_USER, "%s line %d sending REP_MASTER_REQ\n",
						__func__, __LINE__);
			}

			(void)__rep_send_message(dbenv, db_eid_broadcast,
				 	REP_MASTER_REQ, NULL, NULL, 0, NULL);
			fromline = __LINE__;
			goto errlock;
		}

		if (rp->rectype == REP_LOG_MORE) {
			MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
			master = rep->master_id;
			MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
			R_LOCK(dbenv, &dblp->reginfo);
			lsn = lp->lsn;
			R_UNLOCK(dbenv, &dblp->reginfo);
			/*
			 * If the master_id is invalid, this means that since
			 * the last record was sent, somebody declared an
			 * election and we may not have a master to request
			 * things of.
			 *
			 * This is not an error;  when we find a new master,
			 * we'll re-negotiate where the end of the log is and
			 * try to bring ourselves up to date again anyway.
			 */
			if (master == db_eid_invalid)
				ret = 0;
			else if (__rep_send_message(dbenv,
				master, REP_ALL_REQ, &lsn, NULL, 0, NULL) != 0)
				break;
		}
		fromline = __LINE__;
		goto errlock;
	case REP_LOG_REQ:
		/* endianize the rec->data lsn */
		MASTER_ONLY(rep, rp);
		if (rec != NULL && rec->size != 0) {
			memcpy(&tmplsn, rec->data, sizeof(tmplsn));
			LOGCOPY_FROMLSN(rec->data, &tmplsn);
		}
#ifdef DIAGNOSTIC
		if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION) &&
		    rec != NULL && rec->size != 0) {
			__db_err(dbenv,
			    "[%lu][%lu]: LOG_REQ max lsn: [%lu][%lu]",
			    (u_long)rp->lsn.file, (u_long)rp->lsn.offset,
			    (u_long)((DB_LSN *)rec->data)->file,
			    (u_long)((DB_LSN *)rec->data)->offset);
		}
#endif
		/*
		 * There are three different cases here.
		 * 1. We asked for a particular LSN and got it.
		 * 2. We asked for an LSN and it's not found because it is
		 *      beyond the end of a log file and we need a NEWFILE msg.
		 *      and then the record that was requested.
		 * 3. We asked for an LSN and it simply doesn't exist, but
		 *    doesn't meet any of those other criteria, in which case
		 *    it's an error (that should never happen).
		 * If we have a valid LSN and the request has a data_dbt with
		 * it, then we need to send all records up to the LSN in the
		 * data dbt.
		 */
		lsn = rp->lsn;
		fromline = __LINE__;
		if ((ret = __log_cursor(dbenv, &logc)) != 0)
			goto errlock;
		F_SET(logc, DB_LOG_NO_PANIC);
		memset(&data_dbt, 0, sizeof(data_dbt));
		ret = __log_c_get(logc, &rp->lsn, &data_dbt, DB_SET);

        int resp_rc;
		if (ret == 0) {
			resp_rc = __rep_send_message(dbenv,
			    *eidp, REP_LOG, &rp->lsn, &data_dbt,
			    DB_REP_NOBUFFER
			    /* we this to be flushed since the node is behind */
			    , NULL);
        } else if (ret == DB_NOTFOUND) {
			R_LOCK(dbenv, &dblp->reginfo);
			endlsn = lp->lsn;
			R_UNLOCK(dbenv, &dblp->reginfo);
			if (endlsn.file > lsn.file) {
				/*
				 * Case 2:
				 * Need to find the LSN of the last record in
				 * file lsn.file so that we can send it with
				 * the NEWFILE call.  In order to do that, we
				 * need to try to get {lsn.file + 1, 0} and
				 * then backup.
				 */
				endlsn.file = lsn.file + 1;
				endlsn.offset = 0;
				if ((ret = __log_c_get(logc,
					    &endlsn, &data_dbt, DB_SET)) != 0 ||
				    (ret = __log_c_get(logc,
					    &endlsn, &data_dbt,
					    DB_PREV)) != 0) {
					if (FLD_ISSET(dbenv->verbose,
						DB_VERB_REPLICATION))
						__db_err(dbenv,
						    "Unable to get prev of [%lu][%lu]",
						    (u_long)lsn.file,
						    (u_long)lsn.offset);
					ret = DB_REP_OUTDATED;
					/* Tell the replicant he's outdated. */
					resp_rc = __rep_send_message(dbenv, *eidp,
					    REP_VERIFY_FAIL, &lsn, NULL, 0,
					    NULL);
				} else {
					endlsn.offset += logc->c_len;
					resp_rc = __rep_send_message(dbenv, *eidp,
					    REP_NEWFILE, &endlsn, NULL, 0,
					    NULL);
				}
			} else {
				/* Case 3 */
				DB_ASSERT(0);
				__db_err(dbenv,
				    "REP_LOG_REQ Request for LSN [%lu][%lu] fails",
				    (u_long)lsn.file, (u_long)lsn.offset);
				ret = EINVAL;
			}
		}

		/*
		 * XXX
		 * Note that we are not observing the limits here that
		 * we observe on ALL_REQs.  If we think that we need to,
		 * then we need to figure out how to convey back to the
		 * client the max_lsn with the LOG_MORE message and I
		 * can't quite figure out how to do that.
		 */
		while (ret == 0 && rec != NULL && rec->size != 0) {
			if ((ret =
				__log_c_get(logc, &lsn, &data_dbt,
				    DB_NEXT)) != 0) {
				if (ret == DB_NOTFOUND)
					ret = 0;
				break;;
			}
			if (log_compare(&lsn, (DB_LSN *)rec->data) >= 0)
				break;
			if (__rep_send_message(dbenv,
				*eidp, REP_LOG, &lsn, &data_dbt,
				DB_REP_NOBUFFER, NULL) != 0)
				break;
		}

		if ((t_ret = __log_c_close(logc)) != 0 && ret == 0)
			ret = t_ret;
		fromline = __LINE__;
		goto errlock;
	case REP_NEWSITE:
		/* We don't hold the rep mutex, and may miscount. */
		rep->stat.st_newsites++;

		/* This is a rebroadcast; simply tell the application. */
		if (F_ISSET(rep, REP_F_MASTER)) {
			dblp = dbenv->lg_handle;
			lp = dblp->reginfo.primary;
			R_LOCK(dbenv, &dblp->reginfo);
			lsn = lp->lsn;
			R_UNLOCK(dbenv, &dblp->reginfo);
            logmsg(LOGMSG_USER, "%s line %d sending REP_NEWMASTER\n", 
                    __func__, __LINE__);
			(void)__rep_send_message(dbenv,
			    *eidp, REP_NEWMASTER, &lsn, NULL, 0, NULL);
		}
		ret = DB_REP_NEWSITE;
		fromline = __LINE__;
		goto errlock;
	case REP_NEWCLIENT:
		/*
		 * This message was received and should have resulted in the
		 * application entering the machine ID in its machine table.
		 * We respond to this with an ALIVE to send relevant information
		 * to the new client (if we are a master, we'll send a
		 * NEWMASTER, so we only need to send the ALIVE if we're a
		 * client).  But first, broadcast the new client's record to
		 * all the clients.
		 */

		(void)__rep_send_message(dbenv,
		    db_eid_broadcast, REP_NEWSITE, &rp->lsn, rec, 0, NULL);

		ret = DB_REP_NEWSITE;

		if (F_ISSET(rep, REP_F_UPGRADE)) {
			MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
			egen = rep->egen;
			MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
			if (LOG_SWAPPED())
				M_32_SWAP(egen);
			data_dbt.data = &egen;
			data_dbt.size = sizeof(egen);

			(void)__rep_send_message(dbenv,
			    *eidp, REP_ALIVE, &rp->lsn, &data_dbt, 0, NULL);
			fromline = __LINE__;
			goto errlock;
		}
		/* FALLTHROUGH */
	case REP_MASTER_REQ:
		if (F_ISSET(rep, REP_F_MASTER)) {
			R_LOCK(dbenv, &dblp->reginfo);
			lsn = lp->lsn;
			R_UNLOCK(dbenv, &dblp->reginfo);
                        logmsg(LOGMSG_USER, "%s line %d sending REP_NEWMASTER: "
                                            "gen=%u egen=%d\n",
                               __func__, __LINE__, rep->gen, rep->egen);
                        (void)__rep_send_message(dbenv, db_eid_broadcast,
                                                 REP_NEWMASTER, &lsn, NULL, 0,
                                                 NULL);
                }
                /*
		 * Otherwise, clients just ignore it.
		 */
		fromline = __LINE__;
		goto errlock;
	case REP_NEWFILE:
		CLIENT_ONLY(rep, rp);
		MASTER_CHECK(dbenv, *eidp, rep);
		ret = __rep_apply(dbenv, rp, rec, ret_lsnp, commit_gen);
		fromline = __LINE__;
		goto errlock;
	case REP_NEWMASTER:
		ANYSITE(rep);
		if (F_ISSET(rep, REP_F_MASTER) && *eidp != dbenv->rep_eid) {
			/* We don't hold the rep mutex, and may miscount. */
			rep->stat.st_dupmasters++;
			ret = DB_REP_DUPMASTER;
			(void)__rep_send_message(dbenv,
			    db_eid_broadcast, REP_DUPMASTER, NULL, NULL, 0,
			    NULL);
				fromline = __LINE__;
				goto errlock;
		}
        logmsg(LOGMSG_USER, "Received NEW MASTER from %s\n", *eidp);
		ret = __rep_new_master(dbenv, rp, *eidp);
		fromline = __LINE__;
		goto errlock;
	case REP_PAGE:		/* TODO */
		CLIENT_ONLY(rep, rp);
		MASTER_CHECK(dbenv, *eidp, rep);
		break;
	case REP_PAGE_REQ:	/* TODO */
		MASTER_ONLY(rep, rp);
		break;
	case REP_PLIST:	/* TODO */
		CLIENT_ONLY(rep, rp);
		MASTER_CHECK(dbenv, *eidp, rep);
		break;
	case REP_PLIST_REQ:	/* TODO */
		MASTER_ONLY(rep, rp);
		break;
	case REP_VERIFY:
		CLIENT_ONLY(rep, rp);
		MASTER_CHECK(dbenv, *eidp, rep);
		DB_ASSERT((F_ISSET(rep, REP_F_RECOVER) &&
			!IS_ZERO_LSN(lp->verify_lsn)) ||
		    (!F_ISSET(rep, REP_F_RECOVER) &&
			IS_ZERO_LSN(lp->verify_lsn)));
		fromline = __LINE__;
		if (IS_ZERO_LSN(lp->verify_lsn))
			goto errlock;

		/*
		 * fprintf(stderr, "Client got rep_verify response for lsn %d:%d.\n", 
		 * rp->lsn.file, rp->lsn.offset);
		 */
		fromline = __LINE__;
		if ((ret = __log_cursor(dbenv, &logc)) != 0)
			goto errlock;
		memset(&mylog, 0, sizeof(mylog));
		if ((ret = __log_c_get(logc, &rp->lsn, &mylog, DB_SET)) != 0)
			goto rep_verify_err;
		match = 0;

		LOGCOPY_32(&rectype, mylog.data);
		if (rectype == DB___txn_regop) {
			/* If it's a commit, copy the timestamp - if we're about to unroll too
			 * far, we want to notice and not do it. */
			uint32_t timestamp;
			time_t t;
			char start_time[30], my_time[30];
			__txn_regop_args a;

			/* I feel slightly bad hardcoding the timestamp offset, but I'd rather not call 
			 * __txn_regop_read_int per record just to perform this check, and the log format 
			 * is not likely to change without notice. */
			LOGCOPY_32(&timestamp,
			    (uint8_t *)mylog.data + sizeof(a.type) +
			    sizeof(a.txnid->txnid) + sizeof(DB_LSN) +
			    sizeof(u_int32_t));
			t = timestamp;
			if (dbenv->newest_rep_verify_tran_time == 0) {
				char start_time[30];
				dbenv->newest_rep_verify_tran_time = t;
				dbenv->rep_verify_start_lsn = rp->lsn;
			}

			if (dbenv->newest_rep_verify_tran_time &&
			    dbenv->attr.max_backout_seconds &&
			    (dbenv->newest_rep_verify_tran_time - timestamp >
				dbenv->attr.max_backout_seconds)) {
				ctime_r(&dbenv->newest_rep_verify_tran_time,
				    start_time);
				ctime_r(&t, my_time);
				__db_err(dbenv,
				    "Rolled back too far at %u:%u:\n   started at %s   now at %s",
				    rp->lsn.file, rp->lsn.offset, start_time,
				    my_time);
				ret = EINVAL;
				goto rep_verify_err;
			}
			if (dbenv->newest_rep_verify_tran_time &&
			    dbenv->attr.max_backout_logs &&
			    ((dbenv->rep_verify_start_lsn.file - rp->lsn.file) >
				dbenv->attr.max_backout_logs)) {
				__db_err(dbenv,
				    "Rolled back too far at %u:%u, started at %s\n",
				    rp->lsn.file, rp->lsn.offset, start_time);
				ret = EINVAL;
				goto rep_verify_err;
			}
		}
		dbenv->rep_verify_current_lsn = rp->lsn;

		if (mylog.size == rec->size &&
		    memcmp(mylog.data, rec->data, rec->size) == 0) {
			match = 1;
			dbenv->newest_rep_verify_tran_time = 0;
			ZERO_LSN(dbenv->rep_verify_start_lsn);
			ZERO_LSN(dbenv->rep_verify_current_lsn);
		}

		/*
		 * Skip over any records recovery can write.
		 */
		if ((match == 0 || !matchable_log_type(rectype)) &&
		    (ret = __log_c_get(logc, &lsn, &mylog, DB_PREV)) == 0) {
			match = 0;

			if (gbl_berkdb_verify_skip_skipables) {
				LOGCOPY_32(&rectype, mylog.data);
				while (!matchable_log_type(rectype) && (ret =
					__log_c_get(logc, &lsn, &mylog,
					    DB_PREV)) == 0) {
					LOGCOPY_32(&rectype, mylog.data);
				}

				if (ret == DB_NOTFOUND) {
					goto notfound;
				}
			}

			MUTEX_LOCK(dbenv, db_rep->db_mutexp);
			lp->verify_lsn = lsn;
			lp->rcvd_recs = 0;
			lp->wait_recs = rep->request_gap;
			MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);

            verify_req_count++;
            if ((now = time(NULL)) > verify_req_print) {
                logmsg(LOGMSG_INFO, "%s line %d: recovery sending verify_req count=%llu lsn [%d][%d]\n", 
                        __func__, __LINE__, verify_req_count, lsn.file, lsn.offset);
                verify_req_print = now;
            }

			(void)__rep_send_message(dbenv,
			    *eidp, REP_VERIFY_REQ, &lsn, NULL, 0, NULL);

		} else if (ret == DB_NOTFOUND) {
notfound:
			if (gbl_berkdb_verify_skip_skipables) {
				__db_err(dbenv,
				    "Log contains only skippable records, chance of diverging logs\n");
				ret = __log_c_get(logc, &lsn, &mylog, DB_FIRST);

				if (ret == 0) {
					MUTEX_LOCK(dbenv, db_rep->db_mutexp);
					lp->verify_lsn = lsn;
					lp->rcvd_recs = 0;
					lp->wait_recs = rep->request_gap;
					MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);

					match = 0;

					/*
					 * gbl_berkdb_verify_skip_skipables = 0;
					 */

					/*
					 * fprintf(stderr, "Client file %s line %d sending verify req for lsn %d:%d\n",
					 * __FILE__, __LINE__, lsn.file, lsn.offset);
					 */

                    verify_req_count++;
                    if ((now = time(NULL)) > verify_req_print) {
                        logmsg(LOGMSG_INFO, "%s line %d: recovery sending verify_req count=%llu lsn [%d][%d]\n", 
                                __func__, __LINE__, verify_req_count, lsn.file, lsn.offset);
                        verify_req_print = now;
                    }

					(void)__rep_send_message(dbenv,
					    *eidp, REP_VERIFY_REQ, &lsn, NULL,
					    0, NULL);
				} else
					abort();

				goto rep_verify_err;
			}
			/*
			 * If we've truly matched on the first record,
			 * verify to that.
			 */
#if 0
			if (match) {
				__db_err(dbenv,
				    "Match but log contains only skippable records, chance of diverging logs\n");
                // This is never correct
				goto verify;
			}
#endif

			/* We've either run out of records because
			 * logs have been removed or we've rolled back
			 * all the way to the beginning.  In both cases
			 * we to return DB_REP_OUTDATED; in the latter
			 * we don't think these sites were every part of
			 * the same environment and we'll say so.
			 */
			ret = DB_REP_OUTDATED;
			if (rp->lsn.file != 1)
				__db_err(dbenv,
				    "Too few log files to sync with master");
			else
				__db_err(dbenv,
				    "Client was never part of master's environment");
		}
		if (match == 1) {
            static time_t verify_match_print = 0;
            static unsigned long long verify_match_count = 0;

verify:		
            verify_match_count++;
            if ((now = time(NULL)) > verify_match_print) {
                logmsg(LOGMSG_INFO, "%s line %d: got rep_verify_match count=%llu for lsn [%d][%d]\n",
                       __func__, __LINE__, verify_match_count, rp->lsn.file, rp->lsn.offset);
                verify_match_print = now;
            }

            ret = __rep_verify_match(dbenv, rp, savetime);
        }

rep_verify_err:if ((t_ret = __log_c_close(logc)) != 0 &&
		    ret == 0)
			ret = t_ret;
		fromline = __LINE__;
		goto errlock;
	case REP_VERIFY_FAIL:
		rep->stat.st_outdated++;
		ret = DB_REP_OUTDATED;
		fromline = __LINE__;
		goto errlock;
	case REP_VERIFY_REQ:
		MASTER_ONLY(rep, rp);
		type = REP_VERIFY;

		/*
		 * fprintf(stderr, "Master got rep_verify request for lsn %d:%d.\n",
		 * rp->lsn.file, rp->lsn.offset);
		 */

		fromline = __LINE__;
		if ((ret = __log_cursor(dbenv, &logc)) != 0)
			goto errlock;
		d = &data_dbt;
		memset(d, 0, sizeof(data_dbt));
		F_SET(logc, DB_LOG_SILENT_ERR);
		ret = __log_c_get(logc, &rp->lsn, d, DB_SET);
		/*
		 * If the LSN was invalid, then we might get a not
		 * found, we might get an EIO, we could get anything.
		 * If we get a DB_NOTFOUND, then there is a chance that
		 * the LSN comes before the first file present in which
		 * case we need to return a fail so that the client can return
		 * a DB_OUTDATED.
		 */
		if (ret == DB_NOTFOUND &&
		    __log_is_outdated(dbenv, rp->lsn.file, &old) == 0 &&
		    old != 0)
			type = REP_VERIFY_FAIL;

		if (ret != 0)
			d = NULL;

        static time_t send_verify_req_print = 0;
        static unsigned long long send_verify_req_count = 0;
        send_verify_req_count++;
        if ((now = time(NULL)) > send_verify_req_print) {
            logmsg(LOGMSG_INFO, "%s line %d: master sending %s count=%llu req-lsn [%d][%d]\n", 
                    __func__, __LINE__, type == REP_VERIFY ? "REP_VERIFY" : "REP_VERIFY_FAIL", 
                    send_verify_req_count, rp->lsn.file, rp->lsn.offset);
            send_verify_req_print = now;
        }
		(void)__rep_send_message(dbenv, *eidp, type, &rp->lsn, d, 0,
		    NULL);
		ret = __log_c_close(logc);
		fromline = __LINE__;
		goto errlock;
	case REP_VOTE1:
	case REP_GEN_VOTE1:
		if (F_ISSET(rep, REP_F_MASTER)) {
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
				__db_err(dbenv, "Master received vote");
#endif
			R_LOCK(dbenv, &dblp->reginfo);
			lsn = lp->lsn;
			R_UNLOCK(dbenv, &dblp->reginfo);
            logmsg(LOGMSG_USER, "%s line %d sending REP_NEWMASTER\n", 
                    __func__, __LINE__);
			(void)__rep_send_message(dbenv,
			    *eidp, REP_NEWMASTER, &lsn, NULL, 0, NULL);
			fromline = __LINE__;
			goto errlock;
		}

		if (rp->rectype == REP_VOTE1) {
			vi = (REP_VOTE_INFO *) rec->data;
			if (LOG_SWAPPED())
				__rep_vote_info_swap(vi);
			vi_egen = vi->egen;
			vi_last_write_gen = 0;
			vi_nsites = vi->nsites;
			vi_priority = vi->priority;
			vi_tiebreaker = vi->tiebreaker;
			logmsg(LOGMSG_USER, "%s line %d processed REP_VOTE1 from %s "
					"(Setting write-gen to 0)\n", 
					__func__, __LINE__, *eidp);
		} else {
			vig = (REP_GEN_VOTE_INFO *) rec->data;
			if (LOG_SWAPPED())
				__rep_gen_vote_info_swap(vig);
			vi_egen = vig->egen;
			vi_last_write_gen = vig->last_write_gen;
			vi_nsites = vig->nsites;
			vi_priority = vig->priority;
			vi_tiebreaker = vig->tiebreaker;
			logmsg(LOGMSG_USER, "%s line %d processed REP_GEN_VOTE1 from %s "
					"(Setting write-gen to %d)\n",
					__func__, __LINE__, *eidp, vig->last_write_gen);
		}


		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);

		/*
		 * If we get a vote from a later election gen, we
		 * clear everything from the current one, and we'll
		 * start over by tallying it.
		 */
		if (vi_egen < rep->egen) {
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
				__db_err(dbenv,
					"Received old vote %lu, egen %lu, ignoring vote1",
					(u_long) vi_egen, (u_long) rep->egen);
#endif
			goto errunlock;
		}
		if (vi_egen > rep->egen) {
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
				__db_err(dbenv,
					"Received VOTE1 from egen %lu, my egen %lu; reset",
					(u_long) vi_egen, (u_long) rep->egen);
#endif
			__rep_elect_done(dbenv, rep);
			rep->egen = vi_egen;
		}
		if (!IN_ELECTION(rep))
			F_SET(rep, REP_F_TALLY);

		/* Check if this site knows about more sites than we do. */
		if (vi_nsites > rep->nsites)
			rep->nsites = vi_nsites;

		/*
		 * We are keeping the vote, let's see if that changes our
		 * count of the number of sites.
		 */
		if (rep->sites + 1 > rep->nsites)
			rep->nsites = rep->sites + 1;
		if (rep->nsites > rep->asites &&
			(ret = __rep_grow_sites(dbenv, rep->nsites)) != 0) {
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
				__db_err(dbenv,
					"Grow sites returned error %d", ret);
#endif
			goto errunlock;
		}

		/*
		 * Ignore vote1's if we're in phase 2.
		 */
		if (F_ISSET(rep, REP_F_EPHASE2)) {
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
				__db_err(dbenv, "In phase 2, ignoring vote1");
#endif
			goto errunlock;
		}

		/*
		 * Record this vote.  If we get back non-zero, we
		 * ignore the vote.
		 */
		if ((ret = __rep_tally(dbenv, rep, *eidp, &rep->sites,
			    vi_egen, rep->tally_off)) != 0) {
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
				__db_err(dbenv, "Tally returned %d, sites %d",
				    ret, rep->sites);
#endif
			ret = 0;
			goto errunlock;
		}
#ifdef DIAGNOSTIC
		if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION)) {
			__db_err(dbenv,
			    "Incoming vote: (eid)%s (pri)%d (gen)%lu (egen)%lu [%lu,%lu]",
			    *eidp, vi_priority,
			    (u_long) rp->gen, (u_long) vi_egen,
			    (u_long) rp->lsn.file, (u_long) rp->lsn.offset);
			if (rep->sites > 1)
				__db_err(dbenv,
				    "Existing vote: (eid)%s (pri)%d (gen)%lu (sites)%d [%lu,%lu]",
				    rep->winner, rep->w_priority,
				    (u_long)rep->w_gen, rep->sites,
				    (u_long)rep->w_lsn.file,
				    (u_long)rep->w_lsn.offset);
		}
#endif

		__rep_cmp_vote(dbenv, rep, eidp, vi_egen, &rp->lsn, vi_priority,
		    rp->gen, vi_last_write_gen, vi_tiebreaker);
		/*
		 * If you get a vote and you're not in an election, we've
		 * already recorded this vote.  But that is all we need
		 * to do.
		 */
		if (!IN_ELECTION(rep)) {
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
				__db_err(dbenv,
				    "Not in election, but received vote1 0x%x",
				    rep->flags);
#endif
			ret = DB_REP_HOLDELECTION;
			goto errunlock;
		}

		master = rep->winner;
		lsn = rep->w_lsn;
		/*
		 * We need to check sites == nsites, not more than half
		 * like we do in __rep_elect and the VOTE2 code below.  The
		 * reason is that we want to process all the incoming votes
		 * and not short-circuit once we reach more than half.  The
		 * real winner's vote may be in the last half.
		 */
		done = rep->sites >= rep->nsites && rep->w_priority != 0;
		if (done) {
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION)) {
				__db_err(dbenv, "Phase1 election done");
				__db_err(dbenv, "Voting for %s%s",
					master, master == rep->eid ? "(self)" : "");
			}
#endif
			egen = rep->egen;
			committed_gen = rep->committed_gen;
			F_SET(rep, REP_F_EPHASE2);
			F_CLR(rep, REP_F_EPHASE1);
			if (master == rep->eid) {
				(void)__rep_tally(dbenv, rep, rep->eid,
					&rep->votes, egen, rep->v2tally_off);
				goto errunlock;
			}
			MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

			/* Vote for someone else. */
			if (dbenv->attr.elect_highest_committed_gen) {
				logmsg(LOGMSG_USER, "%s line %d sending REP_GEN_VOTE2 from %s "
						"with committed-gen=%d\n",
						__func__, __LINE__, *eidp, committed_gen);
				__rep_send_gen_vote(dbenv, NULL, 0, 0, 0, egen,
					committed_gen, master, REP_VOTE2);
			} else {
				logmsg(LOGMSG_USER, "%s line %d sending REP_VOTE2 from %s "
						"(committed-gen=0)\n",
						__func__, __LINE__, *eidp);
				__rep_send_vote(dbenv, NULL, 0, 0, 0, egen,
					master, REP_VOTE2);
			}
		} else
			MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

		/* Election is still going on. */
		break;
	case REP_VOTE2:
	case REP_GEN_VOTE2:
#ifdef DIAGNOSTIC
		if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
			__db_err(dbenv, "We received a vote%s",
			F_ISSET(rep, REP_F_MASTER) ? " (master)" : "");
#endif
		if (F_ISSET(rep, REP_F_MASTER)) {
			R_LOCK(dbenv, &dblp->reginfo);
			lsn = lp->lsn;
			R_UNLOCK(dbenv, &dblp->reginfo);
			rep->stat.st_elections_won++;
			logmsg(LOGMSG_USER, "%s line %d sending REP_NEWMASTER\n", 
					__func__, __LINE__);
			(void)__rep_send_message(dbenv,
				*eidp, REP_NEWMASTER, &lsn, NULL, 0, NULL);
			fromline = __LINE__;
			goto errlock;
		}

		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);

		/* If we have priority 0, we should never get a vote. */
		DB_ASSERT(rep->priority != 0);

		/*
		 * We might be the last to the party and we haven't had
		 * time to tally all the vote1's, but others have and
		 * decided we're the winner.  So, if we're in the process
		 * of tallying sites, keep the vote so that when our
		 * election thread catches up we'll have the votes we
		 * already received.
		 */
		if (rp->rectype == REP_VOTE2) {
			vi = (REP_VOTE_INFO *) rec->data;
			if (LOG_SWAPPED())
				__rep_vote_info_swap(vi);
			vi_egen = vi->egen;
			vi_last_write_gen = 0;
			vi_nsites = vi->nsites;
			vi_priority = vi->priority;
			vi_tiebreaker = vi->tiebreaker;
		} else {
			vig = (REP_GEN_VOTE_INFO *) rec->data;
			if (LOG_SWAPPED())
				__rep_gen_vote_info_swap(vig);
			vi_egen = vig->egen;
			vi_last_write_gen = vig->last_write_gen;
			vi_nsites = vig->nsites;
			vi_priority = vig->priority;
			vi_tiebreaker = vig->tiebreaker;
		}

		if (!IN_ELECTION_TALLY(rep) && vi_egen >= rep->egen) {
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
				__db_err(dbenv,
				    "Not in election gen %lu, at %lu, got vote",
				    (u_long) vi_egen, (u_long) rep->egen);
#endif
			ret = DB_REP_HOLDELECTION;
			goto errunlock;
		}

		/*
		 * Record this vote.  In a VOTE2, the only valid entry
		 * in the REP_VOTE_INFO is the election generation.
		 *
		 * There are several things which can go wrong that we
		 * need to account for:
		 * 1. If we receive a latent VOTE2 from an earlier election,
		 * we want to ignore it.
		 * 2. If we receive a VOTE2 from a site from which we never
		 * received a VOTE1, we want to ignore it.
		 * 3. If we have received a duplicate VOTE2 from this election
		 * from the same site we want to ignore it.
		 * 4. If this is from the current election and someone is
		 * really voting for us, then we finally get to record it.
		 */
		/*
		 * __rep_cmp_vote2 checks for cases 1 and 2.
		 */
		if ((ret = __rep_cmp_vote2(dbenv, rep, *eidp, vi_egen)) != 0) {
			ret = 0;
			goto errunlock;
		}
		/*
		 * __rep_tally takes care of cases 3 and 4.
		 */
		if ((ret = __rep_tally(dbenv, rep, *eidp, &rep->votes,
			    vi_egen, rep->v2tally_off)) != 0) {
			ret = 0;
			goto errunlock;
		}
		done = rep->votes > rep->nsites / 2;
#ifdef DIAGNOSTIC
		if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
			__db_err(dbenv, "Counted vote %d", rep->votes);
#endif
		if (done) {
			__rep_elect_master(dbenv, rep, eidp);
			ret = DB_REP_NEWMASTER;
			goto errunlock;
		} else
			MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		break;

	case REP_PGDUMP_REQ:{
			extern void __pgdump_reprec(DB_ENV *dbenv, DBT *dbt);
			logmsg(LOGMSG_USER, "pgdump request from %s\n", *eidp);
			__pgdump_reprec(dbenv, rec);
			break;
		}

	default:
		__db_err(dbenv,
			"DB_ENV->rep_process_message: unknown replication message: type %lu",
			(u_long)rp->rectype);
		ret = EINVAL;
		fromline = __LINE__;
		goto errlock;
	}

	/*
	 * If we already hold rep_mutexp then we goto 'errunlock'
	 * Otherwise we goto 'errlock' to acquire it before we
	 * decrement our message thread count.
	 */
errlock:
	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
errunlock:
	rep->msg_th--;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
	PRINT_RETURN(ret, fromline);
}

/* Disabled by default, can enable in lrl */
int max_replication_trans_retries = INT_MAX;

void
berkdb_set_max_rep_retries(int max)
{
	max_replication_trans_retries = max;
}

int
berkdb_get_max_rep_retries()
{
	return max_replication_trans_retries;
}

static inline int
logical_rectype(int rectype)
{
	if (rectype <= 10000)
		return 0;
	if (rectype == 10002)
		return 0;
	if (rectype == 10019)
		return 0;
	if (rectype == 10020)
		return 0;
	if (rectype == 10021)
		return 0;
	return 1;
}

static inline int
dispatch_rectype(int rectype)
{
	/* commit_log_bench */
	if (rectype == 10021)
		return 0;

	/* rowlocks_log_bench */
	if (rectype == 10020)
		return gbl_dispatch_rowlocks_bench;

	return 1;
}

void comdb2_early_ack(DB_ENV *, DB_LSN, uint32_t generation);



// PUBLIC: void __rep_classify_type __P((u_int32_t, int *));
void
__rep_classify_type(u_int32_t type, int *had_serializable_records)
{
	extern int gbl_allow_parallel_rep_on_pagesplit;
	extern int gbl_allow_parallel_rep_on_prefix;

	if (had_serializable_records && (type == DB___dbreg_register ||
		type == DB___fop_create ||
		type == DB___fop_remove ||
		type == DB___fop_write ||
		type == DB___fop_rename ||
		type == DB___fop_file_remove ||
		(!gbl_allow_parallel_rep_on_pagesplit &&
		    (type == DB___db_pg_alloc ||
			type == DB___db_pg_free || type == DB___db_pg_freedata)
		) ||
		type == DB___qam_incfirst ||
		type == DB___qam_mvptr ||
		type == DB___qam_del ||
		type == DB___qam_add ||
		type == DB___qam_delext ||
		(!gbl_allow_parallel_rep_on_prefix && (type == DB___bam_prefix)
		) || type == 10002))	/* scdone should be serialized */
		*had_serializable_records = 1;
}

#define ERR do { from = __LINE__; goto err; } while(0)
int
__rep_check_applied_lsns(dbenv, lc, in_recovery_verify)
	DB_ENV *dbenv;
	LSN_COLLECTION *lc;
	int in_recovery_verify;
{
	DB_LOGC *logc = NULL;
	DBT logrec = { 0 };
	TXN_RECS t = { 0 };
	DB *db;
	int ret = 0;
	PAGE *p;
	DB_MPOOLFILE *mpf;
	int from = 0;
	int ignore;
	u_int32_t type;
	DB_LSN lsn;

	ret = __log_cursor(dbenv, &logc);
	if (ret)
		ERR;

	logrec.flags = DB_DBT_REALLOC;
	for (int i = 0; i < lc->nlsns; i++) {
		lsn = lc->array[i].lsn;

		/* get record type and LSN */
		if ((ret = __log_c_get(logc, &lsn, &logrec, DB_SET)) != 0)
			ERR;

		LOGCOPY_32(&type, logrec.data);

		t.npages = 0;

		if (in_recovery_verify && type == DB___dbreg_register)
			return 0;

		/* see what pages this record references */
		if ((ret =
			__db_dispatch(dbenv, dbenv->pgnos_dtab,
			    dbenv->pgnos_dtab_size, &logrec, &lsn,
			    DB_TXN_GETALLPGNOS, &t)) != 0) {
			__db_err(dbenv, "can't discover pgnos for " PR_LSN,
			    PARM_LSN(lsn));
			__db_dispatch(dbenv, dbenv->pgnos_dtab,
			    dbenv->pgnos_dtab_size, &logrec, &lsn,
			    DB_TXN_GETALLPGNOS, &t);
			ERR;
		}

		/* check that the page LSN for those pages is up to the LSN 
		 * being processed - if it's not, we failed to apply a record */
		for (int i = 0; i < t.npages; i++) {
			int cmp;
			db_pgno_t pgno;
			db = NULL;

			/* find the database it refers to */
			if ((ret =
				__dbreg_id_to_db(dbenv, NULL, &db,
				    t.array[i].fid, 0, &lsn,
				    in_recovery_verify) != 0)) {
				__db_err(dbenv,
				    "failed to find db for dbreg id %d\n",
				    t.array[i].fid);
				ERR;
			}

			/* Don't understand how queues work here yet, so skip this test. */
			if (db->type != DB_BTREE) {
				if (dbenv->attr.check_applied_lsns_debug)
					logmsg(LOGMSG_USER, "check " PR_LSN
						" not a btree, skipping\n", PARM_LSN(t.array[i].lsn));
				continue;
			}

			mpf = db->mpf;
			pgno = t.array[i].pgdesc.pgno;


			/* Exceptions for the checks below: */

			/* Btree page splits for non-root pages have root_pgno set to 0 - it's not ABOUT
			 * page 0, so don't check it.  npgno == 0 means a split of the last page - don't
			 * check page 0 for that case either. */
			if (type == DB___bam_split && pgno == 0 &&
			    (strcmp(t.array[i].comment, "root_pgno") == 0 ||
				strcmp(t.array[i].comment, "npgno") == 0)) {
				if (dbenv->attr.check_applied_lsns_debug)
					logmsg(LOGMSG_USER, "check " PR_LSN
					    " fid %d pgno %u (%s), skip\n",
					    PARM_LSN(lsn), t.array[i].fid, pgno,
					    t.array[i].comment);

				continue;
			}
			/* Page alloc and free have a next page logged, but the next page isn't modified
			 * as part of recovery . */
			else if ((type == DB___db_pg_alloc ||
				type == DB___db_pg_free ||
				type == DB___db_pg_freedata) &&
			    strcmp(t.array[i].comment, "next") == 0) {

				if (dbenv->attr.check_applied_lsns_debug)
					logmsg(LOGMSG_USER, "check " PR_LSN
					    " fid %d pgno %u (%s), skip\n",
					    PARM_LSN(lsn), t.array[i].fid, pgno,
					    t.array[i].comment);

				continue;
			}
			/* Next and previous pages on a big put may be 0 (first and last pages) */
			else if (type == DB___db_big &&
			    ((strcmp(t.array[i].comment, "next_pgno") == 0 ||
				    strcmp(t.array[i].comment,
					"prev_pgno") == 0)) && pgno == 0) {
				if (dbenv->attr.check_applied_lsns_debug)
					logmsg(LOGMSG_USER, "check " PR_LSN
					    " fid %d pgno %u (%s), skip\n",
					    PARM_LSN(lsn), t.array[i].fid, pgno,
					    t.array[i].comment);

				continue;
			}

			/* For overflow page deletes, db will log __db_pg_free before __db_big? Sounds strange, but
			 * who am I to judge.  Don't treat this as an error. */
			else if (type == DB___db_big) {
				int opcode;
				/*
				 * typedef struct ___db_big_args {
				 * u_int32_t type;
				 * DB_TXN *txnid;
				 * DB_LSN prev_lsn;
				 * u_int32_t    opcode;
				 * ...
				 * }
				 */
				LOGCOPY_32(&opcode,
				    (u_int8_t *)logrec.data +
				    sizeof(u_int32_t) /*type */ +
				    sizeof(u_int32_t) /*txn */ +sizeof(DB_LSN)
				    /*prevlsn */ );
				if (opcode == DB_REM_BIG &&
				    strcmp(t.array[i].comment,
					"prev_pgno") == 0) {
					if (dbenv->attr.
					    check_applied_lsns_debug)
						logmsg(LOGMSG_USER, "check " PR_LSN
						    " fid %d pgno %u (%s), skip\n",
						    PARM_LSN(lsn),
						    t.array[i].fid, pgno,
						    t.array[i].comment);
					continue;
				}
			}

			/* dbreg uses pgno for verifying if it's opening the right file - it doesn't
			 * get updated as part of recovery for dbreg - skip the check */
			else if (type == DB___dbreg_register &&
			    strcmp(t.array[i].comment, "meta_pgno") == 0) {
				if (dbenv->attr.check_applied_lsns_debug)
					logmsg(LOGMSG_USER, "check " PR_LSN
					    " fid %d pgno %u (%s), skip\n",
					    PARM_LSN(lsn), t.array[i].fid, pgno,
					    t.array[i].comment);
				continue;
			}



			if ((ret = __memp_fget(mpf, &pgno, 0, &p)) != 0) {
				__db_err(dbenv,
				    "failed to read pgno %u for %s dbreg id %d for field %s ret %d\n",
				    pgno, db->fname, t.array[i].fid,
				    t.array[i].comment, ret);
				ERR;
			}

			if (!IS_NOT_LOGGED_LSN(p->lsn)) {
				cmp = log_compare(&p->lsn, &lsn);

				if (cmp < 0) {
					__db_err(dbenv,
					    "at " PR_LSN
					    " type %d file %s pgno %d (%s) page lsn "
					    PR_LSN " cmp %d", PARM_LSN(lsn),
					    type, db->fname,
					    t.array[i].pgdesc.pgno,
					    t.array[i].comment,
					    PARM_LSN(p->lsn), cmp);

#if 0
					extern void __pgdump(DB_ENV *dbenv,
					    int32_t _fileid, db_pgno_t _pgno);
					extern void __pgdumpall(DB_ENV *dbenv,
					    int32_t _fileid, db_pgno_t _pgno);
					__pgdump(dbenv, t.array[i].fid, pgno);
					__pgdumpall(dbenv, t.array[i].fid,
					    pgno);
#endif
					if (dbenv->attr.
					    check_applied_lsns_fatal) {
						ret = EINVAL;
						ERR;
					}
				} else if (dbenv->attr.check_applied_lsns_debug) {
					logmsg(LOGMSG_USER, "check " PR_LSN
					    " fid %d pgno %u (%s) page lsn "
					    PR_LSN "\n", PARM_LSN(lsn),
					    t.array[i].fid, pgno,
					    t.array[i].comment,
					    PARM_LSN(p->lsn));
				}

			}

			ret = __memp_fput(mpf, p, 0);
			if (ret)
				ERR;
		}
	}
err:
	if (ret)
		__db_err(dbenv, "ret %d line %d at " PR_LSN, ret, from,
		    PARM_LSN(lsn));
	if (logc)
		__log_c_close(logc);
	if (logrec.data)
		free(logrec.data);
	if (t.array)
		__os_free(dbenv, t.array);

	return ret;
}
#undef ERR

int bdb_checkpoint_list_push(DB_LSN lsn, DB_LSN ckp_lsn, int32_t timestamp);

int bdb_the_lock_desired(void);

/*
 * __rep_apply --
 *
 * Handle incoming log records on a client, applying when possible and
 * entering into the bookkeeping table otherwise.  This is the guts of
 * the routine that handles the state machine that describes how we
 * process and manage incoming log records.
 */
static int
__rep_apply_int(dbenv, rp, rec, ret_lsnp, commit_gen)
	DB_ENV *dbenv;
	REP_CONTROL *rp;
	DBT *rec;
	DB_LSN *ret_lsnp;
	uint32_t *commit_gen;
{
	__dbreg_register_args dbreg_args;
	__txn_ckp_args *ckp_args = NULL;
	static int count_in_func = 0;
	void *bdb_state;
	u_int8_t *pp;
	DB_REP *db_rep;
	DBT control_dbt, key_dbt, lsn_dbt;
	DBT max_lsn_dbt, *max_lsn_dbtp, nextrec_dbt, rec_dbt;
	DB *dbp;
	DBC *dbc;
	DB_LOG *dblp;
	DB_LSN ckp_lsn, max_lsn, next_lsn, tmp_lsn;
	LOG *lp;
	REP *rep;
	REP_CONTROL *grp;
	u_int32_t rectype, txnid;
	int cmp, do_req, gap, ret, t_ret;
	int num_retries;
	int disabled_minwrite_noread = 0;
	char *eid;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;
	/*dbp = db_rep->rep_db; */
	dbc = NULL;
	ret = gap = 0;
	memset(&control_dbt, 0, sizeof(control_dbt));
	memset(&rec_dbt, 0, sizeof(rec_dbt));
	max_lsn_dbtp = NULL;
	bzero(&max_lsn, sizeof(max_lsn));

	if (gbl_verify_rep_log_records && rec->size >= HDR_NORMAL_SZ)
		LOGCOPY_32(&rectype, rec->data);

	if (gbl_verify_rep_log_records && IS_SIMPLE(rectype) &&
	    rec->size >= HDR_NORMAL_SZ) {
		/* We just got a log record.  before it sees the light of disk or
		 * the log buffer, make sure it's not corrupt. */
		HDR *h;

		if (rec->size < HDR_NORMAL_SZ) {
			logmsg(LOGMSG_USER, 
                    "unexpected log record sz %d expected >= %d\n",
                    rec->size, HDR_NORMAL_SZ);
		} else {
			HDR hcpy;
			int len;
			u_int32_t cksum;
			h = rec->data;
			if (LOG_SWAPPED()) {
				memcpy(&hcpy, h, HDR_NORMAL_SZ);
				__log_hdrswap(&hcpy, CRYPTO_ON(dbenv));
				len = hcpy.len;
				memcpy(&cksum, hcpy.chksum, sizeof(u_int32_t));
			} else {
				len = h->len;
				memcpy(&cksum, h->chksum, 0);
			}

			if (len > HDR_NORMAL_SZ &&
			    __db_check_chksum(dbenv, NULL, (uint8_t *)&cksum,
				(uint8_t *)h + HDR_NORMAL_SZ,
				len - HDR_NORMAL_SZ, 0)) {
				__db_err(dbenv, "checksum error at %u:%u\n",
				    rp->lsn.file, rp->lsn.offset);
				return EINVAL;
			}
		}
	}

	/*
	 * If this is a log record and it's the next one in line, simply
	 * write it to the log.  If it's a "normal" log record, i.e., not
	 * a COMMIT or CHECKPOINT or something that needs immediate processing,
	 * just return.  If it's a COMMIT, CHECKPOINT, LOG_REGISTER, PREPARE
	 * (i.e., not SIMPLE), handle it now.  If it's a NEWFILE record,
	 * then we have to be prepared to deal with a logfile change.
	 */
	dblp = dbenv->lg_handle;
	MUTEX_LOCK(dbenv, db_rep->db_mutexp);
	dbp = db_rep->rep_db;
	count_in_func++;
	assert(count_in_func == 1);
	lp = dblp->reginfo.primary;
	cmp = log_compare(&rp->lsn, &lp->ready_lsn);

	/*
	 * fprintf(stderr, "Rep log file %s line %d for %d:%d ready_lsn is %d:%d cmp=%d\n", 
	 * __FILE__, __LINE__, rp->lsn.file, rp->lsn.offset, lp->ready_lsn.file, 
	 * lp->ready_lsn.offset, cmp);
	 */

	/*
	 * This is written to assume that you don't end up with a lot of
	 * records after a hole.  That is, it optimizes for the case where
	 * there is only a record or two after a hole.  If you have a lot
	 * of records after a hole, what you'd really want to do is write
	 * all of them and then process all the commits, checkpoints, etc.
	 * together.  That is more complicated processing that we can add
	 * later if necessary.
	 *
	 * That said, I really don't want to do db operations holding the
	 * log mutex, so the synchronization here is tricky.
	 */
	if (cmp == 0) {
		/* We got the log record that we are expecting. */
		if (rp->rectype == REP_NEWFILE) {

			/* this will flush in-memory buffer, but we don't have the region lock for it;
			 * get it here */
			R_LOCK(dbenv, &dblp->reginfo);
			ret = __rep_newfile(dbenv, rp, &lp->ready_lsn);
			R_UNLOCK(dbenv, &dblp->reginfo);

			/*
			 * fprintf(stderr, "Set ready_lsn file %s line %d to %d:%d\n", 
			 * __FILE__, __LINE__, lp->ready_lsn.file, 
			 * lp->ready_lsn.offset);
			 */
			/* Make this evaluate to a simple rectype. */
			rectype = 0;
		} else {
			if (F_ISSET(rp, DB_LOG_PERM)) {
				gap = 1;
				max_lsn = rp->lsn;
			}

			LOGCOPY_32(&rectype, rec->data);

			/* 
			 * If the rectype is DB___txn_ckp and out-of-band checkpoints are
			 * not enabled, make sure to do a memp_sync here, before writing
			 * the checkpoint record to this replicant's logfile.
			 */

			ret = __log_rep_put(dbenv, &rp->lsn, rec);
			if (ret == 0)
				/*
				 * We may miscount if we race, since we
				 * don't currently hold the rep mutex.
				 */
				rep->stat.st_log_records++;

			if (dbenv->attr.cache_lc)
				__lc_cache_feed(dbenv, rp->lsn, *rec);
		}

		/*
		 * If we get the record we are expecting, reset
		 * the count of records we've received and are applying
		 * towards the request interval.
		 */
		lp->rcvd_recs = 0;

		/* 
		 * This should never happen.  It means we've progressed beyond
		 * a hole that we're trying to fill without consuming it 
		 * from repdb.
		 */
		DB_ASSERT(IS_ZERO_LSN(lp->waiting_lsn) ||
		    log_compare(&lp->ready_lsn, &lp->waiting_lsn) <= 0);

		while (ret == 0 && IS_SIMPLE(rectype) &&
		    log_compare(&lp->ready_lsn, &lp->waiting_lsn) == 0) {
			/*
			 * We just filled in a gap in the log record stream.
			 * Write subsequent records to the log.
			 */
gap_check:		max_lsn_dbtp = NULL;
			lp->wait_recs = 0;
			lp->rcvd_recs = 0;
			ZERO_LSN(lp->max_wait_lsn);
			if (dbc == NULL &&
			    (ret = __db_cursor(dbp, NULL, &dbc, 0)) != 0) {
				abort();
				goto err;
			}

			/* The DBTs need to persist through another call. */
			F_SET(&control_dbt, DB_DBT_REALLOC);
			F_SET(&rec_dbt, DB_DBT_REALLOC);
			if ((ret = __db_c_get(dbc,
				    &control_dbt, &rec_dbt,
				    DB_RMW | DB_FIRST)) != 0) {
				abort();
				goto err;
			}

			rp = (REP_CONTROL *)control_dbt.data;
			rec = &rec_dbt;
			LOGCOPY_32(&rectype, rec->data);

			if (rp->rectype != REP_NEWFILE) {

				/* 
				 * If the rectype is DB___txn_ckp and out-of-band checkpoints are
				 * not enabled, make sure to do a memp_sync here, before writing
				 * the checkpoint record to this replicant's logfile.
				 */

				ret = __log_rep_put(dbenv, &rp->lsn, rec);
				/*
				 * We may miscount if we race, since we
				 * don't currently hold the rep mutex.
				 */
				if (ret == 0)
					rep->stat.st_log_records++;

				if (dbenv->attr.cache_lc)
					__lc_cache_feed(dbenv, rp->lsn, *rec);

			} else {
				/* this will flush in-memory buffer, but we don't have the region lock for it;
				 * get it here */

				/* make sure no transactions are in flight on the replicant - do this outside the region lock.
				 * we can get away with this because this code path is single-threaded outside of applying transactions, 
				 * and it serializes running transactions */
				wait_for_running_transactions(dbenv);

				/* now create the new file while holding the region lock */
				R_LOCK(dbenv, &dblp->reginfo);
				ret = __rep_newfile(dbenv, rp, &lp->ready_lsn);
				R_UNLOCK(dbenv, &dblp->reginfo);
				rectype = 0;
			}
			if ((ret = __db_c_del(dbc, 0)) != 0) {
				abort();
				goto err;
			}

			/*
			 * If we just processed a permanent log record, make
			 * sure that we note that we've done so and that we
			 * save its LSN.
			 */
			if (F_ISSET(rp, DB_LOG_PERM)) {
				gap = 1;
				max_lsn = rp->lsn;
			}
			/*
			 * We may miscount, as we don't hold the rep
			 * mutex.
			 */
			--rep->stat.st_log_queued;

			/*
			 * Update waiting_lsn.  We need to move it
			 * forward to the LSN of the next record
			 * in the queue.
			 *
			 * If the next item in the database is a log
			 * record--the common case--we're not
			 * interested in its contents, just in its LSN.
			 * Optimize by doing a partial get of the data item.
			 */
			memset(&nextrec_dbt, 0, sizeof(nextrec_dbt));
			F_SET(&nextrec_dbt, DB_DBT_PARTIAL);
			nextrec_dbt.ulen = nextrec_dbt.dlen = 0;

			memset(&lsn_dbt, 0, sizeof(lsn_dbt));
			ret = __db_c_get(dbc, &lsn_dbt, &nextrec_dbt, DB_NEXT);
			if (ret != DB_NOTFOUND && ret != 0) {
				abort();
				goto err;
			}

			if (ret == DB_NOTFOUND) {
				ZERO_LSN(lp->waiting_lsn);
				/*
				 * Whether or not the current record is
				 * simple, there's no next one, and
				 * therefore we haven't got anything
				 * else to do right now.  Break out.
				 */
				break;
			}
			grp = (REP_CONTROL *)lsn_dbt.data;
			lp->waiting_lsn = grp->lsn;

			/*
			 * If the current rectype is simple, we're done with it,
			 * and we should check and see whether the next record
			 * queued is the next one we're ready for.  This is
			 * just the loop condition, so we continue.
			 *
			 * If this record isn't simple, then we need to
			 * process it before continuing.
			 */
			if (!IS_SIMPLE(rectype))
				break;
		}

		/*
		 * Check if we're at a gap in the table and if so, whether we
		 * need to ask for any records.
		 */
		do_req = 0;

		if (!IS_ZERO_LSN(lp->waiting_lsn) &&
		    log_compare(&lp->ready_lsn, &lp->waiting_lsn) != 0) {
			/*
			 * We got a record and processed it, but we may
			 * still be waiting for more records.
			 */
			next_lsn = lp->ready_lsn;
			do_req = ++lp->rcvd_recs >= lp->wait_recs;

			do_req = 1;

			if (do_req) {
				lp->wait_recs = rep->request_gap;
				lp->rcvd_recs = 0;
				if (log_compare(&rp->lsn,
					&lp->max_wait_lsn) == 0) {
					/*
					 * This single record was requested
					 * so ask for the rest of the gap.
					 */
					lp->max_wait_lsn = lp->waiting_lsn;
					memset(&max_lsn_dbt,
					    0, sizeof(max_lsn_dbt));
					max_lsn_dbt.data = &lp->waiting_lsn;
					max_lsn_dbt.size =
					    sizeof(lp->waiting_lsn);
					max_lsn_dbtp = &max_lsn_dbt;
				}
			}
		} else {
			lp->wait_recs = 0;
			ZERO_LSN(lp->max_wait_lsn);
		}

		if (dbc != NULL)
			if ((ret = __db_c_close(dbc)) != 0) {
				abort();
				goto err;
			}
		dbc = NULL;

		if (do_req) {
			MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
			eid = db_rep->region->master_id;
			MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
			if (eid != db_eid_invalid) {
				rep->stat.st_log_requested++;

				/* 
				 * next_lsn is endianized by rep_send_message.  Endianize 
				 * the payload lsn if we're requesting a range.
				 */

				if (max_lsn_dbtp) {
					LOGCOPY_TOLSN(&tmp_lsn,
					    max_lsn_dbtp->data);
					max_lsn_dbtp->data = &tmp_lsn;
				}

				/*
				 * fprintf(stderr, "Requesting file %s line %d lsn %d:%d\n", 
				 * __FILE__, __LINE__, next_lsn.file, next_lsn.offset);
				 */
				(void)__rep_send_message(dbenv, eid,
				    REP_LOG_REQ, &next_lsn, max_lsn_dbtp, 0,
				    NULL);
			}
		}
	} else if (cmp > 0) {
		/*
		 * The LSN is higher than the one we were waiting for.
		 * This record isn't in sequence; add it to the temporary
		 * database, update waiting_lsn if necessary, and perform
		 * calculations to determine if we should issue requests
		 * for new records.
		 */
		memset(&key_dbt, 0, sizeof(key_dbt));
		key_dbt.data = rp;
		key_dbt.size = sizeof(*rp);
		R_LOCK(dbenv, &dblp->reginfo);
		next_lsn = lp->lsn;
		R_UNLOCK(dbenv, &dblp->reginfo);
		do_req = 0;
		if (lp->wait_recs == 0) {
			/*
			 * This is a new gap. Initialize the number of
			 * records that we should wait before requesting
			 * that it be resent.  We grab the limits out of
			 * the rep without the mutex.
			 */
			lp->wait_recs = rep->request_gap;
			lp->rcvd_recs = 0;
			ZERO_LSN(lp->max_wait_lsn);
		}

		if (++lp->rcvd_recs >= lp->wait_recs) {
			/*
			 * If we've waited long enough, request the record
			 * (or set of records) and double the wait interval.
			 */
			do_req = 1;
			lp->rcvd_recs = 0;

			lp->wait_recs *= 2;

			if (lp->wait_recs > rep->max_gap)
				lp->wait_recs = rep->max_gap;

			/*
			 * If we've never requested this record, then request
			 * everything between it and the first record we have.
			 * If we have requested this record, then only request
			 * this record, not the entire gap.
			 */
			if (IS_ZERO_LSN(lp->max_wait_lsn)) {
				lp->max_wait_lsn = lp->waiting_lsn;
				memset(&max_lsn_dbt, 0, sizeof(max_lsn_dbt));
				max_lsn_dbt.data = &lp->waiting_lsn;
				max_lsn_dbt.size = sizeof(lp->waiting_lsn);
				max_lsn_dbtp = &max_lsn_dbt;
			} else {
				max_lsn_dbtp = NULL;
				lp->max_wait_lsn = next_lsn;
			}
		}
#if defined INSTRUMENT_REP_APPLY
		static unsigned long long putcnt = 0;
		static int lastpr = 0;
		int now = time(NULL);

		putcnt++;
		if (now - lastpr > 0) {
			logmsg(LOGMSG_INFO, "Put %llu lsn %d:%d into repdb,  max_wait_lsn is %d:%d, waiting_lsn is %d:%d, ready_lsn is %d:%d\n",
			    putcnt, rp->lsn.file, rp->lsn.offset,
			    lp->max_wait_lsn.file, lp->max_wait_lsn.offset,
			    lp->waiting_lsn.file, lp->waiting_lsn.offset,
			    lp->ready_lsn.file, lp->ready_lsn.offset);
			lastpr = now;
		}
#endif
		ret = __db_put(dbp, NULL, &key_dbt, rec, 0);
		if (ret != 0)
			abort();

		rep->stat.st_log_queued++;
		rep->stat.st_log_queued_total++;
		if (rep->stat.st_log_queued_max < rep->stat.st_log_queued)
			rep->stat.st_log_queued_max = rep->stat.st_log_queued;

		if (ret != 0)
			goto done;

		if (IS_ZERO_LSN(lp->waiting_lsn) ||
		    log_compare(&rp->lsn, &lp->waiting_lsn) < 0) {
			lp->waiting_lsn = rp->lsn;
		}

		if (do_req) {
			/* Request the LSN we are still waiting for. */
			MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
			eid = db_rep->region->master_id;

			/*
			 * If the master_id is invalid, this means that since
			 * the last record was sent, somebody declared an
			 * election and we may not have a master to request
			 * things of.
			 *
			 * This is not an error;  when we find a new master,
			 * we'll re-negotiate where the end of the log is and
			 * try to to bring ourselves up to date again anyway.
			 */
			if (eid != db_eid_invalid) {
				rep->stat.st_log_requested++;
				MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

				if (max_lsn_dbtp) {
					LOGCOPY_TOLSN(&tmp_lsn,
					    max_lsn_dbtp->data);
					max_lsn_dbtp->data = &tmp_lsn;
				}

				/*
				 * fprintf(stderr, "Requesting file %s line %d lsn %d:%d\n", 
				 * __FILE__, __LINE__, next_lsn.file, next_lsn.offset);
				 */
				(void)__rep_send_message(dbenv, eid,
				    REP_LOG_REQ, &next_lsn, max_lsn_dbtp, 0,
				    NULL);
			} else {
				MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

				if (gbl_verbose_master_req) {
					logmsg(LOGMSG_USER, "%s line %d sending REP_MASTER_REQ\n",
							__func__, __LINE__);
				}

				(void)__rep_send_message(dbenv,
					db_eid_broadcast, REP_MASTER_REQ,
				    NULL, NULL, 0, NULL);
			}
		}

		/*
		 * If this is permanent; let the caller know that we have
		 * not yet written it to disk, but we've accepted it.
		 */
		if (ret == 0 && F_ISSET(rp, DB_LOG_PERM)) {
			if (commit_gen != NULL)
				*commit_gen = 0;
			if (ret_lsnp != NULL)
				*ret_lsnp = rp->lsn;
			ret = DB_REP_NOTPERM;

		}
		goto done;
	} else {
		/*
		 * We may miscount if we race, since we
		 * don't currently hold the rep mutex.
		 */
		rep->stat.st_log_duplicated++;
		goto done;
	}
	if (ret != 0 || cmp < 0 || (cmp == 0 && IS_SIMPLE(rectype)))
		goto done;


	/*
	 * If we got here, then we've got a log record in rp and rec that
	 * we need to process.
	 */
	switch (rectype) {
	case DB___dbreg_register:
		/*
		 * DB opens occur in the context of a transaction, so we can
		 * simply handle them when we process the transaction.  Closes,
		 * however, are not transaction-protected, so we have to
		 * handle them here.
		 *
		 * Note that it should be unsafe for the master to do a close
		 * of a file that was opened in an active transaction, so we
		 * should be guaranteed to get the ordering right.
		 */
		LOGCOPY_32(&txnid, (u_int8_t *) rec->data +
		    ((u_int8_t *) & dbreg_args.txnid -
			(u_int8_t *) & dbreg_args));
		if (txnid == TXN_INVALID && !F_ISSET(rep, REP_F_LOGSONLY)) {
			/* Serialization point: dbreg id are kept in memory & can change here */
			if (dbenv->num_recovery_processor_threads &&
			    dbenv->num_recovery_worker_threads) {
				wait_for_running_transactions(dbenv);
			}

			ret = __db_dispatch(dbenv, dbenv->recover_dtab,
			    dbenv->recover_dtab_size, rec, &rp->lsn,
			    DB_TXN_APPLY, NULL);
		}
		break;
	case DB___txn_ckp:
#if 0
		LOGCOPY_TOLSN(&ckp_lsn, (u_int8_t *) rec->data +
		    ((u_int8_t *) & ckp_args.ckp_lsn -
			(u_int8_t *) & ckp_args));
#endif

		if ((ret = __txn_ckp_read(dbenv, rec->data, &ckp_args)) != 0)
			goto err;

		ret =
		    bdb_checkpoint_list_push(rp->lsn, ckp_args->ckp_lsn,
		    ckp_args->timestamp);
		if (ret) {
			logmsg(LOGMSG_ERROR, "%s: failed to push to checkpoint list, ret %d\n",
			    __func__, ret);
			goto err;
		}
		__os_free(dbenv, ckp_args);

		__memp_sync_out_of_band(dbenv, &rp->lsn);
		break;
	case DB___txn_regop_rowlocks:
	case DB___txn_regop:
	case DB___txn_regop_gen:
		;
		extern int dumptxn(DB_ENV * dbenv, DB_LSN * lsnpp);
		extern int gbl_dumptxn_at_commit;

		if (gbl_dumptxn_at_commit)
			dumptxn(dbenv, &rp->lsn);
		if (!F_ISSET(rep, REP_F_LOGSONLY)) {
			num_retries = 0;
			LTDESC *ltrans = NULL;

			do {
				extern int gbl_slow_rep_process_txn_maxms;
				extern int gbl_slow_rep_process_txn_freq;

				if (gbl_slow_rep_process_txn_maxms) {
					if (!gbl_slow_rep_process_txn_freq ||
					    !(rand() %
						gbl_slow_rep_process_txn_freq))
					{
						poll(0, 0,
						    rand() %
						    gbl_slow_rep_process_txn_maxms);
					}
				}

				/*
				 * If an application is doing app-specific
				 * recovery and acquires locks while applying
				 * a transaction, it can deadlock.  Any other
				 * locks held by this thread should have been
				 * discarded in the __rep_process_txn error
				 * path, so if we simply retry, we should
				 * eventually succeed.
				 */

				if (gbl_reallyearly &&
				    /* don't ack 0:0, which happens for out-of-sequence commits */
				    !(max_lsn.file == 0 && max_lsn.offset == 0)
				    ) {
					comdb2_early_ack(dbenv, max_lsn,
					    rep->committed_gen);
				}

				if (dbenv->num_recovery_processor_threads &&
				    dbenv->num_recovery_worker_threads)
					ret =
					    __rep_process_txn_concurrent(dbenv,
					    rp, rec, &ltrans, rp->lsn, max_lsn,
					    commit_gen, dbenv->prev_commit_lsn);
				else
					ret =
					    __rep_process_txn(dbenv, rp, rec,
					    &ltrans, max_lsn, commit_gen);

				/* Always release locks in order.  This is probably too conservative. */
				if (0 == ret)
					dbenv->prev_commit_lsn = max_lsn;

				if (ret == DB_LOCK_DEADLOCK) {
					rep->stat.retry++;
					num_retries++;
					if (num_retries > rep->stat.max_replication_trans_retries)
                        rep->stat.max_replication_trans_retries = num_retries;
					if (max_replication_trans_retries < INT_MAX &&
					    num_retries >= max_replication_trans_retries &&
					    dbenv->replicant_use_minwrite_noread) {
						logmsg(LOGMSG_WARN, 
                            "Transaction retried %d times, temporarily disabling DB_LOCK_MINWRITE_NOREAD mode\n",
						    num_retries);
						dbenv->replicant_use_minwrite_noread = 0;
						disabled_minwrite_noread = 1;
					}
				} else {
					if (disabled_minwrite_noread)
						dbenv->replicant_use_minwrite_noread = 1;

					if (ret == DB_LOCK_DEADLOCK_CUSTOM) {
						ret = DB_LOCK_DEADLOCK;
						goto err;
					}
				}

				/* Latch the commit LSN - it'll be used by __rep_process_txn */
				/* TODO: if we fail on a custom log record - get us out of here,
				 * we need to release the bdb lock before we try again, because the
				 * deadlock may be a rep_handle_dead and we need to give the watcher
				 * thread a chance to fix it. */
			} while (ret == DB_LOCK_DEADLOCK);
		}
		/* Now flush the log unless we're running TXN_NOSYNC. */
		if (ret == 0 && !F_ISSET(dbenv, DB_ENV_TXN_NOSYNC))
			ret = __log_flush(dbenv, NULL);
		if (ret != 0) {
			__db_err(dbenv, "Error processing txn [%lu][%lu]",
			    (u_long)rp->lsn.file, (u_long)rp->lsn.offset);
			__db_panic(dbenv, ret);
		}
		break;
	case DB___txn_xa_regop:
		ret = __log_flush(dbenv, NULL);
		break;
	default:
		goto err;
	}

	/* Check if we need to go back into the table. */
	if (ret == 0) {
		if (log_compare(&lp->ready_lsn, &lp->waiting_lsn) == 0)
			goto gap_check;
	}


done:
err:	if (dbc != NULL && (t_ret = __db_c_close(dbc)) != 0 && ret == 0) {
		if (t_ret)
			abort();
		ret = t_ret;
	}
	count_in_func--;
	MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);

	if (ret == 0 && F_ISSET(dbenv, DB_ENV_LOG_AUTOREMOVE) &&
	    rp->rectype == REP_NEWFILE)
		__log_autoremove(dbenv);
	if (control_dbt.data != NULL)
		__os_ufree(dbenv, control_dbt.data);
	if (rec_dbt.data != NULL)
		__os_ufree(dbenv, rec_dbt.data);
	if (ret == 0 && gap) {
		if (ret_lsnp != NULL)
			*ret_lsnp = max_lsn;
		ret = DB_REP_ISPERM;
	}
	/* here we have received an inline non simple record; we still 
	 * have to report it to the bdb caller so that lsn is updated
	 * correctly 
	 * if (ret == 0 && cmp == 0 && !IS_SIMPLE(rectype)) {
	 * if (ret_lsnp != NULL)
	 * {
	 * *ret_lsnp = rp->lsn;
	 * ret = DB_REP_ISPERM;
	 * }
	 * }
	 */
#ifdef DIAGNOSTIC
	if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION)) {
		if (ret == DB_REP_ISPERM)
			__db_err(dbenv, "Returning ISPERM [%lu][%lu]",
			    (u_long)ret_lsnp->file, (u_long)ret_lsnp->offset);
		else if (ret == DB_REP_NOTPERM)
			__db_err(dbenv, "Returning NOTPERM [%lu][%lu]",
			    (u_long)ret_lsnp->file, (u_long)ret_lsnp->offset);
		else if (ret != 0)
			__db_err(dbenv, "Returning %d [%lu][%lu]", ret,
			    (u_long)ret_lsnp->file, (u_long)ret_lsnp->offset);
	}
#endif
	return (ret);
}

int gbl_time_rep_apply = 0;

static int
__rep_apply(dbenv, rp, rec, ret_lsnp, commit_gen)
	DB_ENV *dbenv;
	REP_CONTROL *rp;
	DBT *rec;
	DB_LSN *ret_lsnp;
	uint32_t *commit_gen;
{
    static unsigned long long rep_apply_count = 0;
    static unsigned long long rep_apply_usc = 0;
	static int lastpr = 0;
    long long usecs;
    int rc, now;
    bbtime_t start = {0}, end = {0};

    getbbtime(&start);
    rc = __rep_apply_int(dbenv, rp, rec, ret_lsnp, commit_gen);
    getbbtime(&end);
    usecs = diff_bbtime(&end, &start);
    rep_apply_count++;
    rep_apply_usc += usecs;
    if (gbl_time_rep_apply && (now = time(NULL)) > lastpr) {
			logmsg(LOGMSG_USER,
                "%s took %llu usecs, tot-usec=%llu cnt=%llu avg-usec=%llu\n",
			    __func__, usecs, rep_apply_usc,
			    rep_apply_count, rep_apply_usc / rep_apply_count);
			lastpr = now;
    }
    return rc;
}


u_int32_t gbl_rep_lockid;

static void
worker_thd(struct thdpool *pool, void *work, void *thddata, int op)
{
	struct __recovery_processor *rp;
	struct __recovery_queue *rq;
	struct __recovery_record *rr;
	int rc;
	DB_ENV *dbenv;
	DB_LOGC *logc = NULL;
	DBT tmpdbt;
	db_recops redo;
	u_int32_t rectype;
	u_int32_t txnid;
	int recnum = 0, count;
	LISTC_T(struct recovery_record) q;

	listc_init(&q, offsetof(struct __recovery_record, lnk));

	/* TODO: pass empty dbt if a transaction gets large - worker 
	 * should then get a cursor and read the record itself */

	rq = (struct __recovery_queue *)work;
	rp = rq->processor;
	dbenv = rq->processor->dbenv;

	count = rq->records.count;

	rr = listc_rtl(&rq->records);

	while (rr) {
		recnum++;
		if (rr->logdbt.data == NULL) {
			if (logc == NULL) {
				if (__log_cursor(dbenv, &logc)) {
					__db_err(dbenv,
					    "worker can't get log cursor while processing %u:%u\n",
					    rr->lsn.file, rr->lsn.offset);
					abort();
				}
				bzero(&tmpdbt, sizeof(DBT));
				tmpdbt.flags = DB_DBT_REALLOC;
			}
			if ((rc = __log_c_get(logc, &rr->lsn, &tmpdbt, DB_SET))) {
				__db_err(dbenv, "worker can't get lsn %u:%u\n",
				    rr->lsn.file, rr->lsn.offset);
				abort();
			}
			LOGCOPY_32(&rectype, tmpdbt.data);
			tmpdbt.app_data = &rp->context;

			/* Map the txnid to the context */
			if (dispatch_rectype(rectype)) {
				rc = __db_dispatch(dbenv, dbenv->recover_dtab,
				    dbenv->recover_dtab_size, &tmpdbt, &rr->lsn,
				    DB_TXN_APPLY, rq->processor->txninfo);
			} else
				rc = 0;
		} else {

			LOGCOPY_32(&rectype, rr->logdbt.data);

			rr->logdbt.app_data = &rp->context;
			if (dispatch_rectype(rectype)) {
				rc = __db_dispatch(dbenv, dbenv->recover_dtab,
				    dbenv->recover_dtab_size, &rr->logdbt,
				    &rr->lsn, DB_TXN_APPLY,
				    rq->processor->txninfo);
			} else
				rc = 0;
		}

		/* TODO: what do I do on an error? */
		if (rc) {
			__db_err(dbenv, "transaction failed at %lu:%lu rc=%d",
			    (u_long)rr->lsn.file, (u_long)rr->lsn.offset, rc);
			/* and now? */
			abort();
		}

		/* mempool? */
		listc_abl(&q, rr);

		rr = listc_rtl(&rq->records);
	}

	if (logc) {
		if (tmpdbt.data)
			free(tmpdbt.data);
		if ((rc = __log_c_close(logc))) {
			__db_err(dbenv, "__log_c_close rc %d\n", rc);
			abort();
		}
	}

	pthread_mutex_lock(&rq->processor->lk);
	rr = listc_rtl(&q);
	while (rr) {
		pool_relablk(rp->recpool, rr);
		rr = listc_rtl(&q);
	}
	rq->processor->num_busy_workers--;

	/* Signal if not running inline */
	if (pool) {
		pthread_cond_signal(&rq->processor->wait);
	}
	pthread_mutex_unlock(&rq->processor->lk);
}

/* note: must be called under the dbenv->recover_lk lock */
void
in_order_commit_check(DB_LSN *lsn)
{
	static DB_LSN last_lsn = { 0 };

	if (log_compare(&last_lsn, lsn) != -1) {
		if (lsn->file != 0 && lsn->offset != 0) {
			logmsg(LOGMSG_ERROR, "out of order commit?  last was %u:%u, now %u:%u\n",
			    last_lsn.file, last_lsn.offset, lsn->file,
			    lsn->offset);
			/*
			 * extern void bdb_dump_active_locks(void*p, FILE*);
			 * bdb_dump_active_locks(NULL, stdout); */
		}
	}
	if (lsn->file != 0 && lsn->offset != 0)
		last_lsn = *lsn;
}

/* Put start & commit worker thread 0 */
static inline int
logical_start_commit(int rectype)
{
	switch (rectype) {
	case 10005:		/* commit (follows fstblk) */
	case 10006:		/* commit (follows fstblk) */
		return 1;
		break;
	default:
		return 0;
		break;
	}
}

/* Return 1 if this will grab a rowlock */
static inline int
logical_record_file_affinity(int rectype)
{
	switch (rectype) {
	case 10007:		/* comprec    */
	case 10013:		/* add_dta_lk */
	case 10014:		/* add_ix_lk  */
	case 10015:		/* del_dta_lk */
	case 10016:		/* del_ix_lk  */
	case 10017:		/* upd_dta_lk */
	case 10018:		/* upd_ix_lk  */
		return 1;
		break;
	default:
		return 0;
		break;
	}
}

#include <stdlib.h>

int gbl_processor_thd_poll;

static void
processor_thd(struct thdpool *pool, void *work, void *thddata, int op)
{
	struct __recovery_processor *rp;
	struct __recovery_queue *rq;
	struct __recovery_record *rr;
	DBT data_dbt, lock_prev_lsn_dbt;
	DB_LOCK prev_lsn_lk;
	int i;
	int inline_worker;
	int polltm;
	DB_LOGC *logc = NULL;
	DB_LOCKREQ req, *lvp;
	DB_ENV *dbenv;
	int ret, t_ret, last_fileid = -1;
	DB_LSN *lsnp;
	int j;
	LISTC_T(struct __recovery_queue) queues;

	DB_REP *db_rep;
	REP *rep;

	rp = (struct __recovery_processor *)work;
	listc_init(&queues, offsetof(struct __recovery_queue, lnk));
	dbenv = rp->dbenv;
	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	/*  rep_process_message adds to the inflight-txn list while holding the bdblock
     *  We are executing here, so the inflight-txn list-size is greater than 0
     *  get_writelock code blocks after attaining the writelock until the in-flight
     *  txn list is 0
     *  ... so we should NOT need to get the bdb readlock here */
	void *bdb_state = dbenv->app_private;

    bdb_thread_event(bdb_state, 3 /* start rdwr */);

	/* Sleep here if the user has asked us to & if we are coherent */
	if ((polltm = gbl_processor_thd_poll) > 0 && 
			bdb_am_i_coherent(dbenv->app_private)) {
		int lsize;
		pthread_mutex_lock(&dbenv->recover_lk);
		lsize = listc_size(&dbenv->inflight_transactions);
		pthread_mutex_unlock(&dbenv->recover_lk);
		logmsg(LOGMSG_ERROR, "Polling for %d in processor_thd, there are %d "
				"processor thds outstanding\n", polltm, lsize);
		poll(0, 0, polltm);
	}

	/* sanity check */
#if 0
	if (rp) {
		DBT data;
		DB_LOGC *logc = NULL;

		bzero(&data, sizeof(DBT));

		if ((ret = __log_cursor(dbenv, &logc)) != 0) {
			printf("can't get a log cursor\n");
			abort();
		}

		data.flags = DB_DBT_REALLOC;
		if (data.data) {
			free(data.data);
			data.data = NULL;
		}
		for (i = 0; i < rp->lc.nlsns; i++) {
			int rc;
			if ((rc =
				__log_c_get(logc, &rp->lc.array[i].lsn, &data,
				    DB_SET))) {
				__db_err(dbenv, "reget %u:%u rc %d\n",
				    rp->lc.array[i].lsn.file,
				    rp->lc.array[i].lsn.offset, rc);
				abort();
			}
			if (data.size != rp->lc.array[i].rec.size) {
				__db_err(dbenv,
				    "size mismatch cached %u real %u ",
				    rp->lc.array[i].rec.size, data.size);
				abort();
			} else {
				if (memcmp(data.data, rp->lc.array[i].rec.data,
					rp->lc.array[i].rec.size)) {
					printf("mismatch");
					abort();
				}
			}
		}
		if (data.data)
			free(data.data);

		__log_c_close(logc);
	}
#endif

	bzero(&data_dbt, sizeof(DBT));

	if ((ret = __log_cursor(dbenv, &logc)) != 0)
		goto err;

	/* First, bucket records per queue. */
	data_dbt.flags = DB_DBT_REALLOC;

	for (i = 0; i < rp->lc.nlsns; i++) {
		int fileid;
		u_int32_t rectype;

		lsnp = &rp->lc.array[i].lsn;

		if (rp->lc.array[i].rec.data == NULL) {
			assert(!rp->lc.filled_from_cache);
			if ((ret =
				__log_c_get(logc, lsnp, &data_dbt,
				    DB_SET)) != 0) {
				__db_err(dbenv,
				    "failed to read the log at [%lu][%lu]",
				    (u_long)lsnp->file, (u_long)lsnp->offset);
				goto err;
			}
			LOGCOPY_32(&rectype, data_dbt.data);
			fileid =
			    (int)file_id_for_recovery_record(dbenv, NULL,
			    rectype, &data_dbt);
		} else {
			LOGCOPY_32(&rectype, rp->lc.array[i].rec.data);
			fileid =
			    (int)file_id_for_recovery_record(dbenv, NULL,
			    rectype, &rp->lc.array[i].rec);
		}

		if (fileid >= 0) {
			last_fileid = fileid;
		}
		/* Logical follows physical: they should have the same fileid */
		if (fileid == -1 && logical_record_file_affinity(rectype)) {
			fileid = last_fileid;
		}

		/* If there is no fileid, or if this is a start or commit put in fileid 0  */
		if (-1 == fileid || logical_start_commit(rectype)) {
			fileid = 0;
		}

		if (fileid >= rp->num_fileids) {
			rp->recovery_queues =
			    realloc(rp->recovery_queues,
			    (fileid + 1) * sizeof(struct __recovery_queue *));
			for (j = rp->num_fileids; j <= fileid; j++) {
				rp->recovery_queues[j] = NULL;
			}
			rp->num_fileids = fileid + 1;
		}
		if (rp->recovery_queues[fileid] == NULL) {
			rp->recovery_queues[fileid] =
			    malloc(sizeof(struct __recovery_queue));
			rp->recovery_queues[fileid]->fileid = fileid;
			rp->recovery_queues[fileid]->processor = rp;
			rp->recovery_queues[fileid]->used = 0;
			listc_init(&rp->recovery_queues[fileid]->records,
			    offsetof(struct __recovery_record, lnk));
		}
		if (!rp->recovery_queues[fileid]->used) {
			rp->recovery_queues[fileid]->used = 1;
			rp->num_busy_workers++;
			listc_abl(&queues, rp->recovery_queues[fileid]);
		}

		rr = pool_getablk(rp->recpool);
		if (rp->lc.array[i].rec.data)
			rr->logdbt = rp->lc.array[i].rec;
		else
			rr->logdbt.data = NULL;
		rr->lsn = *lsnp;
		rr->fileid = fileid;

		listc_abl(&rp->recovery_queues[fileid]->records, rr);
	}

	if ((dbenv->flags & DB_ENV_ROWLOCKS) && listc_size(&queues) > 1) {
		gbl_rep_rowlocks_multifile++;
	}

	/* Handle inline. */
	if (listc_size(&queues) <= 1) {
		inline_worker = 1;
		rq = listc_rtl(&queues);

		while (rq) {
			rq->used = 0;
			gbl_rep_trans_inline++;
			worker_thd(NULL, rq, NULL, -1);
			rq = listc_rtl(&queues);
		}
	}
	/* Assign to workers */
	else {
		inline_worker = 0;
		rq = listc_rtl(&queues);

		while (rq) {
			if (rp->recovery_queues[rq->fileid] == NULL) {
				logmsg(LOGMSG_FATAL, "NO QUEUE at fileid %d???\n",
				    rq->fileid);
				abort();
			}
			rq->used = 0;
			thdpool_enqueue(dbenv->recovery_workers, worker_thd, rq,
			    0, NULL);
			rq = listc_rtl(&queues);
		}
	}

	/* Wait for worker threads to finish */
	if (!inline_worker) {
		pthread_mutex_lock(&rp->lk);
		int lastpr = 0, pollus =
		    dbenv->attr.recovery_processor_poll_interval_us;
		if (pollus <= 0)
			pollus = 1000;

		while (rp->num_busy_workers) {
			int rc;
			struct timespec ts;

			clock_gettime(CLOCK_REALTIME, &ts);
			if (!lastpr)
				lastpr = ts.tv_sec + 1;

			/* This should stay small:  All workers could finish before the cond_timedwait. */
			ts.tv_nsec += (1000 * pollus);
			if (ts.tv_nsec > 1000000000) {
				ts.tv_nsec %= 1000000000;
				ts.tv_sec++;
			}

			rc = pthread_cond_timedwait(&rp->wait, &rp->lk, &ts);
			if (rp->num_busy_workers && rc == ETIMEDOUT &&
			    ts.tv_sec > lastpr) {
				logmsg(LOGMSG_WARN, "waiting for %d workers\n",
				    rp->num_busy_workers);
				lastpr = ts.tv_sec;
			}
		}
		pthread_mutex_unlock(&rp->lk);
	}


#if 0
	{
		/* debug: assert to make sure we still hold the lock on our commit lsn */
		u_int32_t lid;
		DBT lockname = { 0 };
		DB_LOCK lk;

		ret = __lock_id(dbenv, &lid);
		if (ret)
			goto err;
		lockname.data = &rp->commit_lsn;
		lockname.size = sizeof(DB_LSN);

		ret =
		    dbenv->lock_get(dbenv, lid, DB_LOCK_NOWAIT, &lockname,
		    DB_LOCK_WRITE, &lk);
		if (ret != DB_LOCK_NOTGRANTED) {
			fprintf(stderr, "lock_get rc %d\n", ret);
			exit(1);
		}

		ret = __lock_id_free_pp(dbenv, lid);
		if (ret)
			goto err;
	}
#endif

	/* TODO: when we're convinced that lsn_chain is overkill, nix it */
	if (dbenv->lsn_chain) {
		bzero(&lock_prev_lsn_dbt, sizeof(DBT));
		lock_prev_lsn_dbt.data = &rp->prev_commit_lsn;
		lock_prev_lsn_dbt.size = sizeof(DB_LSN);
		ret =
		    dbenv->lock_get(dbenv, rp->lockid, 0, &lock_prev_lsn_dbt,
		    DB_LOCK_WRITE, &prev_lsn_lk);
		if (ret)
			goto err;
	}

	if (rp->ltrans) {
		int deallocate = 0;

		pthread_mutex_lock(&rp->ltrans->lk);

		rp->ltrans->active_txn_count--;
		if (rp->has_logical_commit)
			F_SET(rp->ltrans, TXN_LTRANS_WASCOMMITTED);

		if (!rp->ltrans->active_txn_count &&
		    F_ISSET(rp->ltrans, TXN_LTRANS_WASCOMMITTED))
			deallocate = 1;

		pthread_mutex_unlock(&rp->ltrans->lk);

		if (rp->has_schema_lock) {
			unlock_schema_lk();
			rp->has_schema_lock = 0;
		}

		if (deallocate) {
			if (NULL == dbenv->txn_logical_commit ||
			    (t_ret =
				dbenv->txn_logical_commit(dbenv,
				    dbenv->app_private, rp->ltrans->ltranid,
				    &rp->commit_lsn)) != 0) {
				logmsg(LOGMSG_ERROR, "%s: txn_logical_commit error, %d\n",
				    __func__, ret);
				ret = t_ret;
			}

			__txn_deallocate_ltrans(dbenv, rp->ltrans);
		}

		rp->ltrans = NULL;
	}


	/* cleanup - similar to __rep_process_txn */
err:
	if (ret == 0) {
		rep->stat.st_txns_applied++;
		if (dbenv->attr.check_applied_lsns) {
			__rep_check_applied_lsns(dbenv, &rp->lc, 0);
		}
	}

	if (data_dbt.data)
		free(data_dbt.data);

	if (logc != NULL && (t_ret = __log_c_close(logc)) != 0 && ret == 0)
		ret = t_ret;

	if (rp->has_schema_lock) {
		unlock_schema_lk();
		rp->has_schema_lock = 0;
	}

	ret = reset_recovery_processor(rp);

	/* TODO: How do I signal error?  What errors can there be? */
	pthread_mutex_lock(&dbenv->recover_lk);
	listc_rfl(&dbenv->inflight_transactions, rp);
	listc_abl(&dbenv->inactive_transactions, rp);
	if (listc_size(&dbenv->inflight_transactions) == 0)
		pthread_cond_broadcast(&dbenv->recover_cond);
	pthread_mutex_unlock(&dbenv->recover_lk);

	if (!dbenv->lsn_chain) {
		pthread_rwlock_unlock(&dbenv->ser_lk);
	}

    bdb_thread_event(bdb_state, 2 /* done rdwr */);
}

static void
debug_dump_lsns(DB_LSN commit_lsn, LSN_COLLECTION * lc, int ret)
{
	char *lsns;
	char *s;
	int malloced = 0;
	int bytes_left;
	int i;

	bytes_left = lc->nlsns * 22 + 1;
	if (lc->nlsns > 100) {
		lsns = malloc(lc->nlsns * 22 + 1);
		malloced = 1;
	} else
		lsns = alloca(lc->nlsns * 22 + 1);

	/* dump LSNs into buffer */
	for (i = 0, s = lsns; i < lc->nlsns; i++) {
		int bytes;
		bytes =
		    snprintf(s, bytes_left, "%u:%u ", lc->array[i].lsn.file,
		    lc->array[i].lsn.offset);
		if (bytes <= 0) {
			/* shouldn't happen. */
			s[0] = 0;
			break;
		}
		s += bytes;
		bytes_left -= bytes;
	}

	ctrace("commit rc %d %u:%u lsns: %s\n", ret, commit_lsn.file,
	    commit_lsn.offset, lsns);

	if (malloced)
		free(lsns);
}

extern int gbl_replicant_gather_rowlocks;
int bdb_transfer_pglogs_to_queues(void *bdb_state, void *pglogs,
	unsigned int nkeys, int is_logical_commit,
	unsigned long long logical_tranid, DB_LSN logical_commit_lsn, uint32_t gen,
	int32_t timestamp, unsigned long long context);

/*
 * __rep_process_txn --
 *
 * This is the routine that actually gets a transaction ready for
 * processing.
 *
 */
static inline int
__rep_process_txn_int(dbenv, rctl, rec, ltrans, maxlsn, commit_gen, lockid, rp,
    lcin)
	DB_ENV *dbenv;
	REP_CONTROL *rctl;
	DBT *rec;
	LTDESC **ltrans;
	DB_LSN maxlsn;
	uint32_t *commit_gen;
	u_int32_t lockid;
	struct __recovery_processor *rp;
	LSN_COLLECTION *lcin;
{
	DBT data_dbt, *lock_dbt = NULL, *rowlock_dbt = NULL;
	LTDESC *lt = NULL;
	LSN_COLLECTION deflc, lc;
	DB_LOCKREQ req, *lvp;
	DB_LOGC *logc;
	DB_LSN prev_lsn, *lsnp;
	DB_REP *db_rep;
	REP *rep;
	__txn_regop_args *txn_args = NULL;
	__txn_regop_gen_args *txn_gen_args = NULL;
	__txn_regop_rowlocks_args *txn_rl_args = NULL;
	void *args = NULL;
	int32_t timestamp = 0;
	__txn_xa_regop_args *prep_args;
	u_int32_t rectype;
	int i, ret, t_ret;
	int got_txns = 0, free_lc = 0;
	u_int32_t txnid;
	void *txninfo;
	unsigned long long context = 0;
	int had_serializable_records = 0;
	int get_locks_and_ack = 1;
	void *pglogs = NULL;
	u_int32_t keycnt = 0;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	logc = NULL;
	txninfo = NULL;
	memset(&data_dbt, 0, sizeof(data_dbt));
	if (F_ISSET(dbenv, DB_ENV_THREAD))
		F_SET(&data_dbt, DB_DBT_REALLOC);

	/*
	 * There are two phases:  First, we have to traverse
	 * backwards through the log records gathering the list
	 * of all LSNs in the transaction.  Once we have this information,
	 * we can loop through and then apply it.
	 */

	/*
	 * We may be passed a prepare (if we're restoring a prepare
	 * on upgrade) instead of a commit (the common case).
	 * Check which and behave appropriately.
	 */
	LOGCOPY_32(&rectype, rec->data);
	memset(&lc, 0, sizeof(lc));

	if (rectype == DB___txn_regop_rowlocks) {

		int dontlock = 0;
		u_int32_t lockcnt;

		if ((ret =
			__txn_regop_rowlocks_read(dbenv, rec->data,
			    &txn_rl_args)) != 0)
			return (ret);
		if (txn_rl_args->opcode != TXN_COMMIT) {
			__os_free(dbenv, txn_rl_args);
			return (0);
		}
		args = txn_rl_args;
		rowlock_dbt = &txn_rl_args->rowlocks;
		context = txn_rl_args->context;
		txnid = txn_rl_args->txnid->txnid;

		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		(*commit_gen) = rep->committed_gen = txn_rl_args->generation;
        rep->committed_lsn = rctl->lsn;
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

		/* if this is not-null, then it's already locked */
		if (*ltrans) {
			lt = (*ltrans);
			dontlock = 1;
		}

		else if (txn_rl_args->lflags & DB_TXN_LOGICAL_BEGIN) {
			if ((ret =
				__txn_allocate_ltrans(dbenv,
				    txn_rl_args->ltranid,
				    &txn_rl_args->begin_lsn, &lt)) != 0)
				goto err1;

			if (NULL == dbenv->txn_logical_start ||
			    (ret =
				dbenv->txn_logical_start(dbenv,
				    dbenv->app_private, txn_rl_args->ltranid,
				    &rctl->lsn)) != 0) {
				logmsg(LOGMSG_ERROR, "%s: txn_logical_start error, %d\n",
				    __func__, ret);
				goto err1;
			}
		}

		else {
			if ((ret =
				__txn_find_ltrans(dbenv, txn_rl_args->ltranid,
				    &lt)) != 0) {
				abort();
			}
		}

		if (!lt) {
			abort();
		}

		lt->last_lsn = rctl->lsn;

		*ltrans = lt;

		if (!dontlock) {
			if (txn_rl_args->lflags & DB_TXN_SCHEMA_LOCK) {
				wrlock_schema_lk();
			}
		}

		prev_lsn = txn_rl_args->prev_lsn;
		lock_dbt = &txn_rl_args->locks;
	} else if (rectype == DB___txn_regop) {
		/*
		 * We're the end of a transaction.  Make sure this is
		 * really a commit and not an abort!
		 */
		if ((ret = __txn_regop_read(dbenv, rec->data, &txn_args)) != 0)
			return (ret);
		if (txn_args->opcode != TXN_COMMIT) {
			__os_free(dbenv, txn_args);
			return (0);
		}
		args = txn_args;
		context = __txn_regop_read_context(txn_args);
		txnid = txn_args->txnid->txnid;
		prev_lsn = txn_args->prev_lsn;
		lock_dbt = &txn_args->locks;
		(*commit_gen) = 0;
	} else if (rectype == DB___txn_regop_gen) {
		/*
		 * We're the end of a transaction.  Make sure this is
		 * really a commit and not an abort!
		 */
		if ((ret =
			__txn_regop_gen_read(dbenv, rec->data,
			    &txn_gen_args)) != 0)
			return (ret);
		if (txn_gen_args->opcode != TXN_COMMIT) {
			__os_free(dbenv, txn_gen_args);
			return (0);
		}
		args = txn_gen_args;
		context = txn_gen_args->context;
		txnid = txn_gen_args->txnid->txnid;
		prev_lsn = txn_gen_args->prev_lsn;
		lock_dbt = &txn_gen_args->locks;
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		(*commit_gen) = rep->committed_gen = txn_gen_args->generation;
        rep->committed_lsn = rctl->lsn;
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
	} else {
		/* We're a prepare. */
		DB_ASSERT(rectype == DB___txn_xa_regop);

		if ((ret =
			__txn_xa_regop_read(dbenv, rec->data, &prep_args)) != 0)
			return (ret);
		prev_lsn = prep_args->prev_lsn;
		lock_dbt = &prep_args->locks;
	}

	if (lockid) {
		get_locks_and_ack = 0;
	} else {
		/* Get locks. */
		if ((ret = __lock_id(dbenv, &lockid)) != 0)
			goto err1;
		get_locks_and_ack = 1;
	}

	gbl_rep_lockid = lockid;

	if (get_locks_and_ack) {
		if (!context) {
			uint32_t flags =
			    LOCK_GET_LIST_GETLOCK | (gbl_rep_printlock ?
			    LOCK_GET_LIST_PRINTLOCK : 0);
			ret =
			    __lock_get_list_context(dbenv, lockid, flags,
			    DB_LOCK_WRITE, lock_dbt, &context, &(rctl->lsn),
			    &pglogs, &keycnt);
			if (ret != 0)
				goto err;

			if (ret == 0 && context) {
				set_commit_context(context, commit_gen,
				    &(rctl->lsn), args, rectype);
			}
		} else {
			uint32_t flags =
			    LOCK_GET_LIST_GETLOCK | (gbl_rep_printlock ?
			    LOCK_GET_LIST_PRINTLOCK : 0);
			ret =
			    __lock_get_list(dbenv, lockid, flags, DB_LOCK_WRITE,
			    lock_dbt, &(rctl->lsn), &pglogs, &keycnt, stdout);
			if (ret != 0)
				goto err;

			if (ret == 0 && context) {
				set_commit_context(context, commit_gen,
				    &(rctl->lsn), args, rectype);
			}
		}

		if (ret != 0)
			goto err;

		if ((gbl_early) && (!gbl_reallyearly) &&
		    !(maxlsn.file == 0 && maxlsn.offset == 0) &&
		    (!txn_rl_args ||
			((txn_rl_args->lflags & DB_TXN_LOGICAL_COMMIT) &&
			    !(txn_rl_args->lflags & DB_TXN_SCHEMA_LOCK)) ||
			F_ISSET(rctl, DB_LOG_REP_ACK))
		    ) {
			/* got all the locks.  ack back early */
			comdb2_early_ack(dbenv, maxlsn, *commit_gen);
		}

		if (txn_rl_args)
			timestamp = txn_rl_args->timestamp;
		else if (txn_gen_args)
			timestamp = txn_gen_args->timestamp;
		else if (txn_args)
			timestamp = txn_args->timestamp;

		ret =
			bdb_transfer_pglogs_to_queues(dbenv->app_private, pglogs,
			keycnt, (!txn_rl_args ||
			(txn_rl_args->lflags & DB_TXN_LOGICAL_COMMIT)),
			(txn_rl_args) ? txn_rl_args->ltranid : 0, rctl->lsn,
			*commit_gen, timestamp, context);
		if (ret)
			goto err;
	}

	if ((ret = __log_cursor(dbenv, &logc)) != 0)
		goto err;

	/* Phase 1.  Get a list of the LSNs in this transaction, and sort it. */
	if (lcin)
		lc = *lcin;
	else {
		/* Phase 1.  Get a list of the LSNs in this transaction, and sort it. */
		if ((ret = __rep_collect_txn_txnid(dbenv, &prev_lsn, &lc,
			    &had_serializable_records, NULL, txnid)) != 0)
			goto err;
		lcin = &lc;
		/* here's the bug!!!! ! */
		qsort(lc.array, lc.nlsns, sizeof(struct logrecord),
		    __rep_lsn_cmp);
		got_txns = 1;
	}


	if (txn_rl_args) {
		int cmp;
		if (txn_rl_args->lflags & DB_TXN_LOGICAL_BEGIN) {
			assert((cmp =
				log_compare(&txn_rl_args->begin_lsn,
				    &lc.array[0].lsn)) <= 0);
		} else {
			assert(!IS_ZERO_LSN(lt->begin_lsn));
		}
	}

	/*
	 * The set of records for a transaction may include dbreg_register
	 * records.  Create a txnlist so that they can keep track of file
	 * state between records.
	 */
	if ((ret = __db_txnlist_init(dbenv, 0, 0, NULL, &txninfo)) != 0)
		goto err;

	/* Phase 2: Apply updates. */
	for (i = 0; i < lc.nlsns; i++) {
		DBT lcin_dbt = { 0 };
		uint32_t rectype = 0;
		int needed_to_get_record_from_log;
		DB_LSN lsn;
		lsn = lc.array[i].lsn;
		lsnp = &lsn;

		if (!lc.array[i].rec.data) {
			assert(!lc.filled_from_cache);
			if ((ret =
				__log_c_get(logc, lsnp, &data_dbt,
				    DB_SET)) != 0) {
				__db_err(dbenv,
				    "failed to read the log at [%lu][%lu]",
				    (u_long)lsnp->file, (u_long)lsnp->offset);
				goto err;
			}
			rectype = 0;
			assert(data_dbt.size >= sizeof(uint32_t));
			LOGCOPY_32(&rectype, data_dbt.data);
			data_dbt.app_data = &context;
			needed_to_get_record_from_log = 1;
		} else {
			/* If this record came here from lc_cache, we have the data, and
			 * there's no need to get it again. */
			lcin_dbt = lc.array[i].rec;
			lcin_dbt.app_data = &context;
			rectype = 0;
			assert(lcin_dbt.size >= sizeof(uint32_t));
			LOGCOPY_32(&rectype, lcin_dbt.data);
			needed_to_get_record_from_log = 0;
		}

		if (dispatch_rectype(rectype)) {
			if ((ret = __db_dispatch(dbenv, dbenv->recover_dtab,
				    dbenv->recover_dtab_size,
				    needed_to_get_record_from_log ? &data_dbt :
				    &lcin_dbt, lsnp, DB_TXN_APPLY,
				    txninfo)) != 0) {
				if (ret != DB_LOCK_DEADLOCK)
					__db_err(dbenv,
					    "transaction failed at [%lu][%lu]",
					    (u_long)lsnp->file,
					    (u_long)lsnp->offset);
				if (ret == DB_LOCK_DEADLOCK && rectype >= 10000)
					ret = DB_LOCK_DEADLOCK_CUSTOM;
				goto err;
			}
		} else
			ret = 0;
	}

err:

	memset(&req, 0, sizeof(req));

	req.op = DB_LOCK_PUT_ALL;
	if ((t_ret =
		__lock_vec(dbenv, lockid, 0, &req, 1, &lvp)) != 0 && ret == 0)
		ret = t_ret;

	/*
	 * pthread_mutex_lock(&dbenv->recover_lk);
	 * in_order_commit_check(&maxlsn);
	 * pthread_mutex_unlock(&dbenv->recover_lk);
	 */

	/* Do the lsn sanity check before we release locks. */
	if (ret == 0) {
		if (dbenv->attr.check_applied_lsns) {
			__rep_check_applied_lsns(dbenv, &lc, 0);
		}
	}

	if ((t_ret = __lock_id_free(dbenv, lockid)) != 0 && ret == 0)
		ret = t_ret;

	if (ret == 0 && txn_rl_args &&
	    txn_rl_args->lflags & DB_TXN_LOGICAL_COMMIT) {
		__txn_deallocate_ltrans(dbenv, lt);

		if (NULL == dbenv->txn_logical_commit ||
		    (t_ret =
			dbenv->txn_logical_commit(dbenv, dbenv->app_private,
			    txn_rl_args->ltranid, &rctl->lsn)) != 0) {
			logmsg(LOGMSG_ERROR, "%s: txn_logical_commit error, %d\n",
			    __func__, ret);
			ret = t_ret;
		}
	}

	if (ret == 0 && txn_rl_args && txn_rl_args->lflags & DB_TXN_SCHEMA_LOCK) {
		unlock_schema_lk();
	}

err1:

	if (rectype == DB___txn_regop)
		__os_free(dbenv, txn_args);
	else if (rectype == DB___txn_regop_gen)
		__os_free(dbenv, txn_gen_args);
	else if (rectype == DB___txn_regop_rowlocks)
		__os_free(dbenv, txn_rl_args);
	else
		__os_free(dbenv, prep_args);

	if (logc != NULL && (t_ret = __log_c_close(logc)) != 0 && ret == 0)
		ret = t_ret;

	if (txninfo != NULL)
		__db_txnlist_end(dbenv, txninfo);

	if (F_ISSET(&data_dbt, DB_DBT_REALLOC) && data_dbt.data != NULL)
		__os_ufree(dbenv, data_dbt.data);

	if (ret == 0) {
		/*
		 * We don't hold the rep mutex, and could miscount if we race.
		 */
		rep->stat.st_txns_applied++;
	}

	if (dbenv->attr.log_applied_lsns)
		debug_dump_lsns(maxlsn, &lc, ret);

	/* Free any log records that came along from __rep_collect_txn. But don't free it
	 * if it's the transaction processor's log collection, since it'll clean it up
	 * on its own. */
	if (lcin && (rp == NULL || (lcin != &rp->lc)))
		lc_free(dbenv, rp, lcin);

	return (ret);
}

static unsigned long long rep_process_txn_usc = 0;
static unsigned long long rep_process_txn_cnt = 0;


// PUBLIC: int __rep_process_txn __P((DB_ENV *, REP_CONTROL *, DBT *, LTDESC **, DB_LSN, uint32_t *));
int
__rep_process_txn(dbenv, rctl, rec, ltrans, maxlsn, commit_gen)
	DB_ENV *dbenv;
	REP_CONTROL *rctl;
	DBT *rec;
	LTDESC **ltrans;
	DB_LSN maxlsn;
	uint32_t *commit_gen;
{
	static int lastpr = 0;
	int now;
	if (!gbl_rep_process_txn_time) {
		return __rep_process_txn_int(dbenv, rctl, rec, ltrans, maxlsn,
		    commit_gen, 0, NULL, NULL);
	} else {
		int rc;
		long long usecs;
		bbtime_t start = { 0 }, end = {
		0};

		rep_process_txn_cnt++;
		getbbtime(&start);
		rc = __rep_process_txn_int(dbenv, rctl, rec, ltrans, maxlsn,
		    commit_gen, 0, NULL, NULL);
		getbbtime(&end);
		usecs = diff_bbtime(&end, &start);
		rep_process_txn_usc += usecs;

		if ((now = time(NULL)) > lastpr) {
			logmsg(LOGMSG_ERROR, 
                "%s took %llu usecs, tot-usec=%llu cnt=%llu avg-usec=%llu\n",
			    __func__, usecs, rep_process_txn_usc,
			    rep_process_txn_cnt,
			    rep_process_txn_usc / rep_process_txn_cnt);
			lastpr = now;
		}

		return rc;
	}
}

static inline int
wait_for_lsn_chain_lk(dbenv)
	DB_ENV *dbenv;
{
	u_int32_t lockid = 0;
	DB_LOCK lsnlock;
	DBT lockname;
	DB_LOCKREQ req, *lvp;
	DB_LSN waitlsn;
	int ret;

	pthread_mutex_lock(&dbenv->recover_lk);
	waitlsn = dbenv->prev_commit_lsn;
	pthread_mutex_unlock(&dbenv->recover_lk);

	ret = __lock_id(dbenv, &lockid);
	if (ret != 0)
		goto done;
	bzero(&lockname, sizeof(DBT));
	lockname.data = &waitlsn;
	lockname.size = sizeof(DB_LSN);
#if 0
	printf("Waiting for %u:%u (parallel %d serial %d)\n",
	    dbenv->prev_commit_lsn.file, dbenv->prev_commit_lsn.offset,
	    gbl_rep_trans_parallel, gbl_rep_trans_serial);
#endif
	ret =
	    dbenv->lock_get(dbenv, lockid, 0, &lockname, DB_LOCK_WRITE,
	    &lsnlock);

done:
	if (lockid) {
		int t_ret;
		memset(&req, 0, sizeof(req));
		req.op = DB_LOCK_PUT_ALL;
		t_ret = __lock_vec(dbenv, lockid, 0, &req, 1, &lvp);
		if (t_ret)
			ret = t_ret;
		t_ret = __lock_id_free(dbenv, lockid);
		if (t_ret)
			ret = t_ret;
	}

	return ret;
}

static int inline
wait_for_running_transactions(dbenv)
	DB_ENV *dbenv;
{

	/* The lsn chain version waits on the most recent lsn lock */
	if (dbenv->lsn_chain) {
		return wait_for_lsn_chain_lk(dbenv);
	} else {
		/* Grab the writelock */
		pthread_rwlock_wrlock(&dbenv->ser_lk);

		/* Release immediately: no one else is running */
		pthread_rwlock_unlock(&dbenv->ser_lk);

		return 0;
	}
}

void
berkdb_dumptrans(DB_ENV *dbenv)
{
	struct __recovery_processor *rp;

	pthread_mutex_lock(&dbenv->recover_lk);
	LISTC_FOR_EACH(&dbenv->inflight_transactions, rp, lnk) {
		logmsg(LOGMSG_USER, 
            "  lockid %x   commit_lsn %u:%u  prev_commit_lsn %u:%u  splitn %d  nlsns %d  sz %d\n",
		    rp->lockid, rp->commit_lsn.file, rp->commit_lsn.offset,
		    rp->prev_commit_lsn.file, rp->prev_commit_lsn.offset,
		    rp->num_fileids, rp->lc.nlsns, rp->lc.memused);
	}
	pthread_mutex_unlock(&dbenv->recover_lk);
}

static int
reset_recovery_processor(rp)
	struct __recovery_processor *rp;
{
	DB_ENV *dbenv;
	DB_LOCKREQ req;
	int ret = 0, t_ret;
	int i;

	dbenv = rp->dbenv;
	if (rp->lockid != DB_LOCK_INVALIDID) {
		memset(&req, 0, sizeof(req));
		req.op = DB_LOCK_PUT_ALL;
		if ((t_ret =
			__lock_vec(dbenv, rp->lockid, 0, &req, 1, NULL)) != 0 &&
		    ret == 0)
			ret = t_ret;

		if ((t_ret = __lock_id_free(dbenv, rp->lockid)) != 0 &&
		    ret == 0)
			ret = t_ret;

		rp->lockid = DB_LOCK_INVALIDID;
	}

	if (rp->txninfo != NULL) {
		__db_txnlist_end(dbenv, rp->txninfo);
		rp->txninfo = NULL;
	}

	lc_free(dbenv, rp, &rp->lc);

	return ret;
}

extern int gbl_force_serial_on_writelock;

static inline int
__rep_process_txn_concurrent_int(dbenv, rctl, rec, ltrans, ctrllsn, maxlsn,
    commit_gen, prev_commit_lsn)
	DB_ENV *dbenv;
	REP_CONTROL *rctl;
	DBT *rec;
	LTDESC **ltrans;
	DB_LSN ctrllsn;
	DB_LSN maxlsn;
	uint32_t *commit_gen;
	DB_LSN prev_commit_lsn;
{
	DBT data_dbt, *lock_dbt, *rowlock_dbt, lsn_lock_dbt;
	int32_t timestamp;
	DB_LOCKREQ req, *lvp;
	DB_LOGC *logc;
	DB_LSN prev_lsn, *lsnp;
	DB_REP *db_rep;
	DB_LOCK lsnlock;
	REP *rep;
	LSN_COLLECTION lc;
	u_int32_t txnid;
	int cmp;
	unsigned long long context = 0;
	LTDESC *lt = NULL;
	__txn_regop_args *txn_args = NULL;
	__txn_regop_gen_args *txn_gen_args = NULL;
	__txn_regop_rowlocks_args *txn_rl_args = NULL;
	void *args = NULL;
	__txn_xa_regop_args *prep_args = NULL;
	u_int32_t lockid = DB_LOCK_INVALIDID, rectype;
	int i, ret, t_ret, throwdeadlock = 0;
	void *txninfo;
	struct __recovery_processor *rp;
	int had_serializable_records = 0;
	void *pglogs = NULL;
	u_int32_t keycnt = 0;
	int got_schema_lk = 0;

	pthread_mutex_lock(&dbenv->recover_lk);
	rp = listc_rtl(&dbenv->inactive_transactions);
	pthread_mutex_unlock(&dbenv->recover_lk);
	/* If we can't reuse a processor, create one.  Should probably be
	 * in its own routine.  */
	if (rp == NULL) {
		rp = calloc(1, sizeof(struct __recovery_processor));
		pthread_mutex_init(&rp->lk, NULL);
		pthread_cond_init(&rp->wait, NULL);
		memset(&rp->lc, 0, sizeof(rp->lc));
		rp->recovery_queues = NULL;
		rp->recpool =
		    pool_setalloc_init(sizeof(struct __recovery_record), 0,
		    malloc, free);
		rp->mspsize = dbenv->recovery_memsize;
		rp->msp =
		    comdb2ma_create(rp->mspsize, rp->mspsize,
		    "berkdb/rep/rec_proc", 0);
		rp->num_fileids = 0;
		rp->lockid = DB_LOCK_INVALIDID;
	}
	/* See if recovery_memsize was changed - recreate the processor's mspace if so */
	if (rp->mspsize != dbenv->recovery_memsize) {
		comdb2ma msp;
		int sz;

		sz = dbenv->recovery_memsize;

		logmsg(LOGMSG_INFO, "Resizing mspace: %d -> %d\n", rp->mspsize, sz);
		msp = comdb2ma_create(sz, sz, "berkdb/rep/rec_proc", 0);
		if (msp == NULL) {
			logmsg(LOGMSG_INFO, "Can't resize mspace: can't create mspace of %d bytes\n",
			    sz);
			/* this caps the size for __rep_collect_txn but doesn't destroy the mspace */
			rp->mspsize = sz;
			goto bad_resize;
		}

		comdb2ma_destroy(rp->msp);
		rp->mspsize = sz;
		rp->msp = msp;

bad_resize:	;
	}

	gbl_rep_lockid = 0;

	/* Get locks. */
	if ((ret = __lock_id(dbenv, &lockid)) != 0) {
#if defined ABORT_ON_CONCURRENT_ERROR
		abort();
#else
		goto err;
#endif
	}


	/* setup transaction processsor */
	rp->commit_lsn = ctrllsn;
	rp->prev_commit_lsn = prev_commit_lsn;
	rp->num_busy_workers = 0;
	rp->dbenv = dbenv;
	rp->lc.nlsns = 0;
	rp->lc.memused = 0;
	rp->txninfo = NULL;
	rp->context = 0;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	logc = NULL;
	txninfo = NULL;
	memset(&data_dbt, 0, sizeof(data_dbt));
	if (F_ISSET(dbenv, DB_ENV_THREAD))
		F_SET(&data_dbt, DB_DBT_REALLOC);

	/*
	 * There are two phases:  First, we have to traverse
	 * backwards through the log records gathering the list
	 * of all LSNs in the transaction.  Once we have this information,
	 * we can loop through and then apply it.
	 */

	/*
	 * We may be passed a prepare (if we're restoring a prepare
	 * on upgrade) instead of a commit (the common case).
	 * Check which and behave appropriately.
	 */
	LOGCOPY_32(&rectype, rec->data);
	if (rectype == DB___txn_regop_rowlocks) {
		int dontlock = 0;
		u_int32_t lockcnt = 0;
		if ((ret =
			__txn_regop_rowlocks_read(dbenv, rec->data,
			    &txn_rl_args)) != 0)
			return (ret);
		if (txn_rl_args->opcode != TXN_COMMIT) {
			__os_free(dbenv, txn_rl_args);
			return (0);
		}

		args = txn_rl_args;

		txnid = txn_rl_args->txnid->txnid;
		rowlock_dbt = &txn_rl_args->rowlocks;
		rp->context = txn_rl_args->context;
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		(*commit_gen) = rep->committed_gen = txn_rl_args->generation;
        rep->committed_lsn = rctl->lsn;
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

		/* If this is passed in, then this returned deadlock acquiring pagelocks.
		 * We already have the rowlocks, so don't lock them again. */
		if (*ltrans) {
			lt = (*ltrans);
			dontlock = 1;
		}

		else if (txn_rl_args->lflags & DB_TXN_LOGICAL_BEGIN) {
			if ((ret =
				__txn_allocate_ltrans(dbenv,
				    txn_rl_args->ltranid,
				    &txn_rl_args->begin_lsn, &lt)) != 0)
				goto err;

			if (NULL == dbenv->txn_logical_start ||
			    (ret =
				dbenv->txn_logical_start(dbenv,
				    dbenv->app_private, txn_rl_args->ltranid,
				    &rctl->lsn)) != 0) {
				logmsg(LOGMSG_ERROR, 
                        "%s: error calling txn_logical_start, ret=%d\n",
                        __FILE__, ret);
				goto err;
			}
		}

		else {
			if ((ret =
				__txn_find_ltrans(dbenv, txn_rl_args->ltranid,
				    &lt)) != 0) {
				abort();
			}
		}

		if (!lt) {
			logmsg(LOGMSG_FATAL, "%s: no ltrans, aborting\n", __func__);
			abort();
		}

		lt->last_lsn = rctl->lsn;
		*ltrans = rp->ltrans = lt;

		if (txn_rl_args->lflags & DB_TXN_SCHEMA_LOCK) {
			if (!dontlock) {
				wrlock_schema_lk();
			}
			got_schema_lk = 1;
		}
		prev_lsn = txn_rl_args->prev_lsn;
		lock_dbt = &txn_rl_args->locks;
	} else if (rectype == DB___txn_regop) {
		/*
		 * We're the end of a transaction.  Make sure this is
		 * really a commit and not an abort!
		 */
		if ((ret = __txn_regop_read(dbenv, rec->data, &txn_args)) != 0)
			return (ret);
		if (txn_args->opcode != TXN_COMMIT) {
			__os_free(dbenv, txn_args);
			return (0);
		}

		args = txn_args;
		rp->context = __txn_regop_read_context(txn_args);
		(*commit_gen) = 0;

		txnid = txn_args->txnid->txnid;
		rp->ltrans = NULL;

		prev_lsn = txn_args->prev_lsn;
		lock_dbt = &txn_args->locks;
	} else if (rectype == DB___txn_regop_gen) {
		/*
		 * We're the end of a transaction.  Make sure this is
		 * really a commit and not an abort!
		 */
		if ((ret =
			__txn_regop_gen_read(dbenv, rec->data,
			    &txn_gen_args)) != 0)
			return (ret);
		if (txn_gen_args->opcode != TXN_COMMIT) {
			__os_free(dbenv, txn_gen_args);
			return (0);
		}

		args = txn_gen_args;
		rp->context = txn_gen_args->context;

		txnid = txn_gen_args->txnid->txnid;
		rp->ltrans = NULL;

		prev_lsn = txn_gen_args->prev_lsn;
		lock_dbt = &txn_gen_args->locks;

		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		(*commit_gen) = rep->committed_gen = txn_gen_args->generation;
        rep->committed_lsn = rctl->lsn;
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
	} else {
		/* We're a prepare. */
		DB_ASSERT(rectype == DB___txn_xa_regop);

		if ((ret =
			__txn_xa_regop_read(dbenv, rec->data, &prep_args)) != 0)
			return (ret);
		prev_lsn = prep_args->prev_lsn;
		lock_dbt = &prep_args->locks;
	}

	/* XXX new logic: collect the locks & commit context, and then send the ack */

	if (!rp->context) {
		uint32_t flags =
		    LOCK_GET_LIST_GETLOCK | (gbl_rep_printlock ?
		    LOCK_GET_LIST_PRINTLOCK : 0);
		ret =
		    __lock_get_list_context(dbenv, lockid, flags, DB_LOCK_WRITE,
		    lock_dbt, &rp->context, &(rctl->lsn), &pglogs, &keycnt);
		if (ret != 0)
			goto err;

		if (ret == 0 && rp->context && !throwdeadlock) {
			set_commit_context(rp->context, commit_gen,
			    &(rctl->lsn), args, rectype);
		}
	} else {
		uint32_t flags =
		    LOCK_GET_LIST_GETLOCK | (gbl_rep_printlock ?
		    LOCK_GET_LIST_PRINTLOCK : 0);
		ret =
		    __lock_get_list(dbenv, lockid, flags, DB_LOCK_WRITE,
		    lock_dbt, &(rctl->lsn), &pglogs, &keycnt, stdout);
		if (ret != 0)
			goto err;

		if (ret == 0 && rp->context) {
			set_commit_context(rp->context, commit_gen,
			    &(rctl->lsn), args, rectype);
		}
	}

	if (throwdeadlock)
		ret = DB_LOCK_DEADLOCK;

	if (ret != 0) {
		if (ret == DB_LOCK_DEADLOCK)
			gbl_rep_trans_deadlocked++;
		goto err;
	}

	if ((gbl_early) && (!gbl_reallyearly) &&
	    !(maxlsn.file == 0 && maxlsn.offset == 0) &&
	    (!txn_rl_args || ((txn_rl_args->lflags & DB_TXN_LOGICAL_COMMIT) &&
		    !(txn_rl_args->lflags & DB_TXN_SCHEMA_LOCK)) ||
		F_ISSET(rctl, DB_LOG_REP_ACK))
	    ) {
		/* got all the locks.  ack back early */
		comdb2_early_ack(dbenv, maxlsn, *commit_gen);
	}

	if (txn_rl_args)
		timestamp = txn_rl_args->timestamp;
	else if (txn_gen_args)
		timestamp = txn_gen_args->timestamp;
	else if (txn_args)
		timestamp = txn_args->timestamp;

	ret = bdb_transfer_pglogs_to_queues(dbenv->app_private, pglogs, keycnt,
		(!txn_rl_args || (txn_rl_args->lflags & DB_TXN_LOGICAL_COMMIT)),
		(txn_rl_args) ? txn_rl_args->ltranid : 0,
		rctl->lsn, *commit_gen, timestamp, rp->context);
	if (pglogs)
		__os_free(dbenv, pglogs);
	if (ret)
		goto err;

	/* Before we do anything else - see if this transaction contains any fileops.
	 * If it does, transactions after it in the log may depend on these fileops,
	 * and we need to run this transaction inline first */

	/* Phase 1.  Get a list of the LSNs in this transaction, and sort it. */
	/* Had serializable records means the transaction has a record type that requires
	 * this transaction to be processed serially. */
	had_serializable_records = 0;
	if ((ret = __rep_collect_txn_txnid(dbenv, &prev_lsn, &rp->lc,
		    &had_serializable_records, rp, txnid)) != 0) {
#if defined ABORT_ON_CONCURRENT_ERROR
		abort();
#else
		goto err;
#endif
	}

	qsort(rp->lc.array, rp->lc.nlsns, sizeof(struct logrecord),
	    __rep_lsn_cmp);

	if (txn_rl_args) {
		if (txn_rl_args->lflags & DB_TXN_LOGICAL_BEGIN) {
			assert((cmp =
				log_compare(&txn_rl_args->begin_lsn,
				    &rp->lc.array[0].lsn)) <= 0);
		} else {
			assert(!IS_ZERO_LSN(lt->begin_lsn));
		}
	}

	/* If we had any log records in this transaction that may affect the next transaction, 
	 * process this transaction inline */

	/* Force anything which gets the schema lock to be serial.  The problem is that the 
	 * pthread_rwlock_wrlock API is too nanny-esque: we rely on the processor thread 
	 * (a different thread) to unlock the schema lock if we've gotten it.  If the next 
	 * regop also wants us to grab the schema-lock, we'd like this thread to block on 
	 * itself until the processor thread unlocks it.  Instead, the pthread_rwlock_wrlock 
	 * api can choose to return 'DEADLOCK'- apparently checking the tid of holding thread  
	 * against the tid of the thread which wants the lock.  FTR, this is dumb.
	 *
	 * The solution: we grab the schema-lock rarely: just serialize for those cases.
	 * */
	int desired = 0;
	if (had_serializable_records || got_schema_lk ||
			(desired = (gbl_force_serial_on_writelock &&
			 bdb_the_lock_desired()))) {

		if (txn_args)
			__os_free(dbenv, txn_args);
		if (txn_gen_args)
			__os_free(dbenv, txn_gen_args);
		if (txn_rl_args)
			__os_free(dbenv, txn_rl_args);

		ret = wait_for_running_transactions(dbenv);
		if (ret) {
			logmsg(LOGMSG_ERROR, "wait err %d\n", ret);
#if defined ABORT_ON_CONCURRENT_ERROR
			abort();
#else
			goto err;
#endif
		}

		gbl_rep_trans_serial++;

		ret =
		    __rep_process_txn_int(dbenv, rctl, rec, ltrans, maxlsn,
		    commit_gen, lockid, rp, &rp->lc);

		reset_recovery_processor(rp);
		pthread_mutex_lock(&dbenv->recover_lk);
		listc_abl(&dbenv->inactive_transactions, rp);
		pthread_mutex_unlock(&dbenv->recover_lk);

		return ret;
	}
	gbl_rep_trans_parallel++;

	rp->lockid = lockid;

	if (dbenv->attr.debug_deadlock_replicant_percent) {
		int r = rand() % 100;
		if (r <= dbenv->attr.debug_deadlock_replicant_percent) {
			throwdeadlock = 1;
		}
	}

	/*
	 * The set of records for a transaction may include dbreg_register
	 * records.  Create a txnlist so that they can keep track of file
	 * state between records.
	 */
	if ((ret = __db_txnlist_init(dbenv, 0, 0, NULL, &txninfo)) != 0) {
#if defined ABORT_ON_CONCURRENT_ERROR
		abort();
#else
		goto err;
#endif
	}

	if (logc != NULL && (t_ret = __log_c_close(logc)) != 0 && ret == 0)
		ret = t_ret;
	logc = NULL;

	/* Phase 2: Apply updates. */

	if (dbenv->lsn_chain) {
		/* Grab a lock on the commit LSN */
		lsn_lock_dbt.data = &maxlsn;
		lsn_lock_dbt.size = sizeof(DB_LSN);
		/*printf("before dispatch, lock %u:%u\n", maxlsn.file, maxlsn.offset); */
		ret =
		    dbenv->lock_get(dbenv, lockid, 0, &lsn_lock_dbt,
		    DB_LOCK_WRITE, &lsnlock);
		if (ret)
			goto err;
	} else {
		pthread_rwlock_rdlock(&dbenv->ser_lk);
	}

	/* Dispatch to a processor thread. */
	rp->txninfo = txninfo;
	if (data_dbt.data) {
		free(data_dbt.data);
		data_dbt.data = NULL;
	}
	rp->commit_lsn = ctrllsn;
	rp->has_logical_commit = 0;
	rp->has_schema_lock = 0;
	if (rp->ltrans) {
		pthread_mutex_lock(&rp->ltrans->lk);
		rp->ltrans->active_txn_count++;
		pthread_mutex_unlock(&rp->ltrans->lk);
		if (txn_rl_args->lflags & DB_TXN_LOGICAL_COMMIT)
			rp->has_logical_commit = 1;
		if (txn_rl_args->lflags & DB_TXN_SCHEMA_LOCK)
			rp->has_schema_lock = 1;
	}

	pthread_mutex_lock(&dbenv->recover_lk);
	listc_abl(&dbenv->inflight_transactions, rp);
	pthread_mutex_unlock(&dbenv->recover_lk);

	thdpool_enqueue(dbenv->recovery_processors, processor_thd, rp, 0, NULL);

	if (txn_args)
		__os_free(dbenv, txn_args);
	if (txn_gen_args)
		__os_free(dbenv, txn_gen_args);
	if (txn_rl_args)
		__os_free(dbenv, txn_rl_args);

	txn_args = NULL;
	txn_gen_args = NULL;
	txn_rl_args = NULL;

	/* If we got this far, we let the processor do cleanup */
	return 0;

err:
	if (lockid != DB_LOCK_INVALIDID)
		rp->lockid = lockid;
	if (rectype == DB___txn_regop && txn_args)
		__os_free(dbenv, txn_args);
	if (rectype == DB___txn_regop_gen && txn_gen_args)
		__os_free(dbenv, txn_gen_args);
	else if (rectype == DB___txn_regop_rowlocks && txn_rl_args)
		__os_free(dbenv, txn_rl_args);
	else
		__os_free(dbenv, prep_args);

	if (data_dbt.data) {
		if (F_ISSET(&data_dbt, DB_DBT_REALLOC)) {
			__os_ufree(dbenv, data_dbt.data);
		} else {
			free(data_dbt.data);
		}
		data_dbt.data = NULL;
	}

	reset_recovery_processor(rp);

	pthread_mutex_lock(&dbenv->recover_lk);
	listc_abl(&dbenv->inactive_transactions, rp);
	pthread_mutex_unlock(&dbenv->recover_lk);

	if (logc != NULL && (t_ret = __log_c_close(logc)) != 0 && ret == 0)
		ret = t_ret;

	if (txninfo != NULL)
		__db_txnlist_end(dbenv, txninfo);

	if (ret == 0)
		/*
		 * We don't hold the rep mutex, and could miscount if we race.
		 */
		rep->stat.st_txns_applied++;

	if (dbenv->attr.log_applied_lsns)
		debug_dump_lsns(ctrllsn, &rp->lc, ret);

	return (ret);
}

// PUBLIC: int __rep_process_txn_concurrent __P((DB_ENV *, REP_CONTROL *, DBT *, LTDESC **,
// PUBLIC:     DB_LSN, DB_LSN, uint32_t *, DB_LSN));
int
__rep_process_txn_concurrent(dbenv, rctl, rec, ltrans, ctrllsn, maxlsn,
    commit_gen, prev_commit_lsn)
	DB_ENV *dbenv;
	REP_CONTROL *rctl;
	DBT *rec;
	LTDESC **ltrans;
	DB_LSN ctrllsn;
	DB_LSN maxlsn;
	uint32_t *commit_gen;
	DB_LSN prev_commit_lsn;
{
	static int lastpr = 0;
	int now;
	if (!gbl_rep_process_txn_time) {
		return __rep_process_txn_concurrent_int(dbenv, rctl, rec,
		    ltrans, ctrllsn, maxlsn, commit_gen, prev_commit_lsn);
	} else {
		int rc;
		long long usecs;
		bbtime_t start = { 0 }, end = {
		0};

		rep_process_txn_cnt++;
		getbbtime(&start);
		rc = __rep_process_txn_concurrent_int(dbenv, rctl, rec, ltrans,
		    ctrllsn, maxlsn, commit_gen, prev_commit_lsn);
		getbbtime(&end);
		usecs = diff_bbtime(&end, &start);
		rep_process_txn_usc += usecs;

		if ((now = time(NULL)) > lastpr) {
			logmsg(LOGMSG_INFO, 
                "%s took %llu usecs, tot-usec=%llu cnt=%llu avg-usec=%llu\n",
			    __func__, usecs, rep_process_txn_usc,
			    rep_process_txn_cnt,
			    rep_process_txn_usc / rep_process_txn_cnt);
			lastpr = now;
		}
		return rc;
	}
}



// PUBLIC: int __rep_collect_txn_from_log __P((DB_ENV *, DB_LSN *, LSN_COLLECTION *, int *, struct __recovery_processor *));
int
__rep_collect_txn_from_log(dbenv, lsnp, lc, had_serializable_records, rp)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	LSN_COLLECTION *lc;
	int *had_serializable_records;
	struct __recovery_processor *rp;
{
	__txn_child_args *argp;
	DB_LOGC *logc;
	DB_LSN c_lsn;
	DBT data;
	u_int32_t rectype;
	int nalloc, ret, t_ret;
	int i;
	int switched_to_realloc = 0;
	int recnum = 0;


	memset(&data, 0, sizeof(data));

#if 0
	if (rp && rp->lc.nlsns != 0) {
		printf("expected nlsns == 0\n");
		abort();
	}
#endif

	if (rp)
		F_SET(&data, DB_DBT_USERMEM);
	else
		F_SET(&data, DB_DBT_REALLOC);

	if ((ret = __log_cursor(dbenv, &logc)) != 0)
		return (ret);

	while (!IS_ZERO_LSN(*lsnp)) {
		recnum++;
		ret = __log_c_get(logc, lsnp, &data, DB_SET);
		if (rp) {
			if (ret == ENOMEM && rp &&
			    (data.flags & DB_DBT_USERMEM)) {
				data.data = comdb2_malloc(rp->msp, data.size);
				data.ulen = data.size;
				if (data.data == NULL) {
					switched_to_realloc = 1;
					data.flags = DB_DBT_REALLOC;
				}
				ret = __log_c_get(logc, lsnp, &data, DB_SET);
			}
		}
		if (ret) {
			logmsg(LOGMSG_ERROR, "__rep_collect_txn lsn %u:%u rc %d\n",
			    lsnp->file, lsnp->offset, ret);
			goto err;
		}
		LOGCOPY_32(&rectype, data.data);
		if (rectype == DB___txn_child) {
			if ((ret = __txn_child_read(dbenv,
				    data.data, &argp)) != 0)
				goto err;
			c_lsn = argp->c_lsn;
			*lsnp = argp->prev_lsn;
			__os_free(dbenv, argp);
			ret =
			    __rep_collect_txn_from_log(dbenv, &c_lsn, lc,
			    had_serializable_records, rp);
		} else {

			__rep_classify_type(rectype, had_serializable_records);
			if (lc->nalloc < lc->nlsns + 1) {
				int i;
				nalloc = lc->nalloc == 0 ? 20 : lc->nalloc * 2;
				if ((ret = __os_realloc(dbenv,
					    nalloc * sizeof(struct logrecord),
					    &lc->array)) != 0)
					goto err;
				for (i = lc->nalloc; i < nalloc; i++) {
					bzero(&lc->array[i].rec, sizeof(DBT));
				}
				lc->nalloc = nalloc;
			}
			lc->array[lc->nlsns].lsn = *lsnp;

			/* note: if we don't have a recovery processor, we shouldn't be collecting log
			 * record payloads here at all - just skip it */
			if (rp) {
				if (switched_to_realloc ||
				    ((lc->memused + data.size) >
					dbenv->recovery_memsize)) {
					if (!switched_to_realloc) {
						data.flags = DB_DBT_REALLOC;
						switched_to_realloc = 1;
					}
					lc->array[lc->nlsns].rec.data = NULL;
				} else {
					lc->array[lc->nlsns].rec = data;
					lc->memused += data.size;
				}
			}
			lc->nlsns++;

			/*
			 * Explicitly copy the previous lsn.  The record
			 * starts with a u_int32_t record type, a u_int32_t
			 * txn id, and then the DB_LSN (prev_lsn) that we
			 * want.  We copy explicitly because we have no idea
			 * what kind of record this is.
			 */
			LOGCOPY_TOLSN(lsnp, (u_int8_t *)data.data +
			    sizeof(u_int32_t) + sizeof(u_int32_t));
		}

		/* If we are still allocating our own memory for log records,
		 * make sure berkeley doesn't free it */
		if (rp && (data.flags & DB_DBT_USERMEM) && !switched_to_realloc) {
			data.ulen = data.size = 0;
			data.data = NULL;
		}

		if (ret != 0)
			goto err;
	}

	/* sanity check */
#if 0
	if (rp) {
		data.flags = DB_DBT_REALLOC;
		if (data.data) {
			free(data.data);
			data.data = NULL;
		}
		for (i = 0; i < lc->nlsns; i++) {
			int rc;
			if ((rc =
				__log_c_get(logc, &lc->array[i].lsn, &data,
				    DB_SET))) {
				__db_err(dbenv, "reget %u:%u rc %d\n",
				    lc->array[i].lsn.file,
				    lc->array[i].lsn.offset, rc);
				abort();
			}
			if (data.size != lc->array[i].rec.size) {
				__db_err(dbenv,
				    "size mismatch cached %u real %u ",
				    lc->array[i].rec.size, data.size);
				abort();
			} else {
				if (memcmp(data.data, lc->array[i].rec.data,
					lc->array[i].rec.size)) {
					printf("mismatch");
				}
			}
		}
	}
	printf("__rep_collect_txn: passed sanity check\n");
#endif
	if (ret != 0)
		__db_err(dbenv, "collect failed at: [%lu][%lu]",
		    (u_long)lsnp->file, (u_long)lsnp->offset);

err:	if ((t_ret = __log_c_close(logc)) != 0 && ret == 0)
		ret = t_ret;
	if (data.data != NULL && !(data.flags & DB_DBT_USERMEM))
		__os_ufree(dbenv, data.data);
	return (ret);

}


// PUBLIC: int __rep_collect_txn_txnid __P((DB_ENV *, DB_LSN *, LSN_COLLECTION *, int *, struct __recovery_processor *, u_int32_t));
int
__rep_collect_txn_txnid(dbenv, lsnp, lc, had_serializable_records, rp, txnid)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	LSN_COLLECTION *lc;
	int *had_serializable_records;
	struct __recovery_processor *rp;
	u_int32_t txnid;
{
	int ret;
	DB_LSN savedlsn = *lsnp;
	DB_REP *db_rep;
	REP *rep;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	lc->filled_from_cache = 0;

	if (dbenv->attr.cache_lc && txnid) {
		ret = __lc_cache_get(dbenv, lsnp, lc, txnid);
		/* TODO: had_serializable/had_logical/had_commit - store in lc? */

		if (ret == 0) {
			if (had_serializable_records)
				*had_serializable_records =
				    lc->had_serializable_records;

			lc->filled_from_cache = 1;

			if (dbenv->attr.cache_lc_check) {
				int checkret;
				int hs = 0;
				int bad_compare = 0;
				DBT check_dbt = { 0 };
				DB_LOGC *check_logc = NULL;
				LSN_COLLECTION checklc = { 0 };

				qsort(lc->array, lc->nlsns,
				    sizeof(struct logrecord), __rep_lsn_cmp);

				check_dbt.flags = DB_DBT_MALLOC;

				DB_LSN lsn = savedlsn;
				checkret =
				    __rep_collect_txn_from_log(dbenv, &lsn,
				    &checklc, &hs, NULL);
				if (checkret) {
					__db_err(dbenv,
					    "couldn't collect " PR_LSN
					    " from log but got from cache?\n",
					    PARM_LSN(savedlsn));
					ret = EINVAL;
					goto check_failed;
				}

				qsort(checklc.array, checklc.nlsns,
				    sizeof(struct logrecord), __rep_lsn_cmp);

				ret = __log_cursor(dbenv, &check_logc);
				if (ret) {
					__db_err(dbenv,
					    "can't get log cursor\n");
					goto check_failed;
				}

				if (had_serializable_records &&
				    hs != *had_serializable_records) {
					__db_err(dbenv,
					    "'had serializable' mismatch: log %d != cache %d \n",
					    hs, *had_serializable_records);
					ret = EINVAL;
					goto check_failed;
				}
				if (log_compare(&lsn, lsnp) != 0) {
					__db_err(dbenv,
					    "lsn mismatch: log " PR_LSN
					    " != cache " PR_LSN "\n",
					    PARM_LSN(lsn), PARM_LSNP(lsnp));
					ret = EINVAL;
					goto check_failed;
				}
				if (checklc.nlsns != lc->nlsns) {
					__db_err(dbenv,
					    "nlsns mismatch: log %d != cache %d\n",
					    checklc.nlsns, lc->nlsns);
					bad_compare = 1;
					goto bad_compare;
				}
				for (int i = 0; i < checklc.nlsns; i++) {
					ret =
					    __log_c_get(check_logc,
					    &checklc.array[i].lsn, &check_dbt,
					    DB_SET);
					if (ret)
						goto check_failed;
					checklc.array[i].rec = check_dbt;
					if (log_compare(&checklc.array[i].lsn,
						&lc->array[i].lsn) != 0) {
						bad_compare = 1;
						break;
					}
					if (checklc.array[i].rec.size !=
					    lc->array[i].rec.size) {
						__db_err(dbenv,
						    "at " PR_LSN
						    " size mismatch log %d != cache %d\n",
						    PARM_LSN(lc->array[i].lsn),
						    checklc.array[i].rec.size,
						    lc->array[i].rec.size);
						bad_compare = 1;
						break;
					}
					if (memcmp(checklc.array[i].rec.data,
						lc->array[i].rec.data,
						lc->array[i].rec.size) != 0) {
						__db_err(dbenv,
						    "at " PR_LSN
						    " data mismatch, log:\n",
						    PARM_LSN(checklc.array[i].
							lsn));
						fsnapf(stderr,
						    checklc.array[i].rec.data,
						    checklc.array[i].rec.size);
						__db_err(dbenv, "cache:\n");
						fsnapf(stderr,
						    lc->array[i].rec.data,
						    lc->array[i].rec.size);
					}
				}
bad_compare:
				if (bad_compare) {
					// printf("lsn mismatch:\n");
					__db_err(dbenv, "log:\n");
					for (int i = 0; i < checklc.nlsns; i++) {
						__db_err(dbenv, PR_LSN " ",
						    PARM_LSN(checklc.array[i].
							lsn));
					}
					__db_err(dbenv, "\n");
					__db_err(dbenv, "cache:\n");
					for (int i = 0; i < lc->nlsns; i++) {
						__db_err(dbenv, PR_LSN " ",
						    PARM_LSN(lc->array[i].lsn));
					}
					__db_err(dbenv, "\n");
					ret = EINVAL;
					goto check_failed;
				}

check_failed:
				if (check_logc)
					__log_c_close(check_logc);
				lc_free(dbenv, NULL, &checklc);

				if (ret)
					return ret;

			}
			// printf("collect "PR_LSN" hit\n", PARM_LSN(savedlsn));
			rep->stat.lc_cache_hits++;
			return 0;

		}
	}
	// printf("collect "PR_LSN" miss\n", PARM_LSN(savedlsn));
	rep->stat.lc_cache_misses++;

	return __rep_collect_txn_from_log(dbenv, lsnp, lc,
	    had_serializable_records, rp);
}

// PUBLIC: int __rep_collect_txn __P((DB_ENV *, DB_LSN *, LSN_COLLECTION *, int *, struct __recovery_processor *));
int
__rep_collect_txn(dbenv, lsnp, lc, had_serializable_records, rp)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	LSN_COLLECTION *lc;
	int *had_serializable_records;
	struct __recovery_processor *rp;
{
	return __rep_collect_txn_txnid(dbenv, lsnp, lc,
	    had_serializable_records, rp, 0);
}

/*
 * __rep_lsn_cmp --
 *	qsort-type-compatible wrapper for log_compare.
 */
static int
__rep_lsn_cmp(lsn1, lsn2)
	const void *lsn1, *lsn2;
{

	return (log_compare((DB_LSN *)lsn1, (DB_LSN *)lsn2));
}

/*
 * __rep_newfile --
 *	NEWFILE messages have the LSN of the last record in the previous
 * log file.  When applying a NEWFILE message, make sure we haven't already
 * swapped files.
 */
static int
__rep_newfile(dbenv, rc, lsnp)
	DB_ENV *dbenv;
	REP_CONTROL *rc;
	DB_LSN *lsnp;
{
	DB_LOG *dblp;
	LOG *lp;

	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;

	if (rc->lsn.file + 1 > lp->lsn.file)
		return (__log_newfile(dblp, lsnp));
	else {
		/* We've already applied this NEWFILE.  Just ignore it. */
		*lsnp = lp->lsn;
		return (0);
	}
}

/*
 * __rep_tally --
 * PUBLIC: int __rep_tally __P((DB_ENV *, REP *, char *, int *,
 * PUBLIC:    u_int32_t, u_int32_t));
 *
 * Handle incoming vote1 message on a client.  Called with the db_rep
 * mutex held.  This function will return 0 if we successfully tally
 * the vote and non-zero if the vote is ignored.  This will record
 * both VOTE1 and VOTE2 records, depending on which region offset the
 * caller passed in.
 */
int
__rep_tally(dbenv, rep, eid, countp, egen, vtoff)
	DB_ENV *dbenv;
	REP *rep;
	char *eid;
	int *countp;
	u_int32_t egen, vtoff;
{
	REP_VTALLY *tally, *vtp;
	int i;

#ifndef DIAGNOSTIC
	COMPQUIET(rep, NULL);
#endif

	tally = R_ADDR((REGINFO *)dbenv->reginfo, vtoff);
	i = 0;
	vtp = &tally[i];
	while (i < *countp) {
		/*
		 * Ignore votes from earlier elections (i.e. we've heard
		 * from this site in this election, but its vote from an
		 * earlier election got delayed and we received it now).
		 * However, if we happened to hear from an earlier vote
		 * and we recorded it and we're now hearing from a later
		 * election we want to keep the updated one.  Note that
		 * updating the entry will not increase the count.
		 * Also ignore votes that are duplicates.
		 */
		if (vtp->eid == eid) {
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
				__db_err(dbenv,
				    "Tally found[%d] (%s, %lu), this vote (%s, %lu)",
				    i, vtp->eid, (u_long)vtp->egen,
				    eid, (u_long)egen);
#endif
			if (vtp->egen >= egen)
				return (1);
			else {
				vtp->egen = egen;
				return (0);
			}
		}
		i++;
		vtp = &tally[i];
	}
	/*
	 * If we get here, we have a new voter we haven't
	 * seen before.  Tally this vote.
	 */
#ifdef DIAGNOSTIC
	if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION)) {
		if (vtoff == rep->tally_off)
			__db_err(dbenv, "Tallying VOTE1[%d] (%s, %lu)",
			    i, eid, (u_long)egen);
		else
			__db_err(dbenv, "Tallying VOTE2[%d] (%s, %lu)",
			    i, eid, (u_long)egen);
	}
#endif
	vtp->eid = eid;
	vtp->egen = egen;
	(*countp)++;
	return (0);
}

/*
 * __rep_cmp_vote --
 * PUBLIC: void __rep_cmp_vote __P((DB_ENV *, REP *, char**, u_int32_t, DB_LSN *,
 * PUBLIC:     int, u_int32_t, u_int32_t, int));
 *
 * Compare incoming vote1 message on a client.  Called with the db_rep
 * mutex held.
 */
void
__rep_cmp_vote(dbenv, rep, eidp, egen, lsnp, priority, gen, committed_gen, tiebreaker)
	DB_ENV *dbenv;
	REP *rep;
	char **eidp;
    u_int32_t egen;
	DB_LSN *lsnp;
	int priority;
	u_int32_t gen, committed_gen;
	int tiebreaker;
{
	int cmp;

    // if highest_committed_gen logic is enabled we should compare the lsn of the
    // highest commit-record 
    if (dbenv->attr.elect_highest_committed_gen) {
        if ((cmp = committed_gen - rep->w_committed_gen) == 0) {
            cmp = log_compare(lsnp, &rep->w_lsn);
        }
    }
    else {
		cmp = log_compare(lsnp, &rep->w_lsn);
    }
	/*
	 * If we've seen more than one, compare us to the best so far.
	 * If we're the first, make ourselves the winner to start.
	 */
	if (rep->sites > 1 && priority != 0) {
		/*
		 * LSN is primary determinant. Then priority if LSNs
		 * are equal, then tiebreaker if both are equal.
		 */
		if (cmp > 0 ||
		    (cmp == 0 && (priority > rep->w_priority ||
			    (priority == rep->w_priority &&
				(tiebreaker > rep->w_tiebreaker))))) {
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
				__db_err(dbenv, "Accepting new vote");
#endif
			// XXX put this trace on a switch
			logmsg(LOGMSG_USER, 
                "%s egen=%u gen=%u switching to new vote of %s from %s because "
			    "committed_gen=%u(vs %u) lsn=[%d][%d](vs [%d][%d]) priority=%d(vs %d) tiebreaker=%u (vs %u)\n",
			    __func__, egen, gen, *eidp, rep->winner, committed_gen,
			    rep->w_committed_gen, lsnp->file, lsnp->offset,
			    rep->w_lsn.file, rep->w_lsn.offset, priority,
			    rep->w_priority, tiebreaker, rep->w_tiebreaker);

			rep->winner = *eidp;
			rep->w_priority = priority;
			rep->w_committed_gen = committed_gen;
			rep->w_lsn = *lsnp;
			rep->w_gen = gen;
			rep->w_tiebreaker = tiebreaker;
		} else {
			// XXX put this trace on a switch
			logmsg(LOGMSG_USER, 
                "%s egen=%u gen=%u keeping current vote of %s not %s because "
			    "committed_gen=%u(vs %u) lsn=[%d][%d](vs [%d][%d]) priority=%d(vs %d) tiebreaker=%u (vs %u)\n",
			    __func__, egen, gen, rep->winner, *eidp,
			    rep->w_committed_gen, committed_gen,
			    rep->w_lsn.file, rep->w_lsn.offset, lsnp->file,
			    lsnp->offset, rep->w_priority, priority, 
                rep->w_tiebreaker, tiebreaker);
		}


	} else if (rep->sites == 1) {
		if (priority != 0) {
			/* Make ourselves the winner to start. */
			rep->winner = *eidp;
			rep->w_priority = priority;
			rep->w_committed_gen = committed_gen;
			rep->w_gen = gen;
			rep->w_lsn = *lsnp;
			rep->w_tiebreaker = tiebreaker;
            logmsg(LOGMSG_USER,
                    "%s egen=%u gen=%u first vote %s committed_gen=%u lsn=[%d][%d] priority=%d tiebreaker=%u\n",
                    __func__, egen, gen, rep->winner, committed_gen, lsnp->file, lsnp->offset, priority, tiebreaker);
		} else {
            logmsg(LOGMSG_USER,
                    "%s egen=%u first vote is db_eid_invalid because priority is 0\n", __func__, egen);
			rep->winner = db_eid_invalid;
			rep->w_priority = 0;
			rep->w_committed_gen = 0;
			rep->w_gen = 0;
			ZERO_LSN(rep->w_lsn);
			rep->w_tiebreaker = 0;
		}
	}
	return;
}

/*
 * __rep_cmp_vote2 --
 * PUBLIC: int __rep_cmp_vote2 __P((DB_ENV *, REP *, char*, u_int32_t));
 *
 * Compare incoming vote2 message with vote1's we've recorded.  Called
 * with the db_rep mutex held.  We return 0 if the VOTE2 is from a
 * site we've heard from and it is from this election.  Otherwise we return 1.
 */
int
__rep_cmp_vote2(dbenv, rep, eid, egen)
	DB_ENV *dbenv;
	REP *rep;
	char *eid;
	u_int32_t egen;
{
	int i;
	REP_VTALLY *tally, *vtp;

	tally = R_ADDR((REGINFO *)dbenv->reginfo, rep->tally_off);
	i = 0;
	vtp = &tally[i];
	for (i = 0; i < rep->sites; i++) {
		vtp = &tally[i];
		if (vtp->eid == eid && vtp->egen == egen) {
            logmsg(LOGMSG_USER,
                    "%s egen=%u found matching vote1 from %s\n", __func__, 
                    egen, eid);
#ifdef DIAGNOSTIC
			if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
				__db_err(dbenv,
				    "Found matching vote1 (%d, %lu), at %d of %d",
				    eid, (u_long)egen, i, rep->sites);
#endif
			return (0);
		}
	}
#ifdef DIAGNOSTIC
	if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
		__db_err(dbenv, "Did not find vote1 for eid %d, egen %lu",
		    eid, (u_long)egen);
#endif
    logmsg(LOGMSG_USER,
            "%s egen=%u got vote2 but no matching vote1 from %s\n", __func__, 
            egen, eid);

	return (1);
}

static int
__rep_dorecovery(dbenv, lsnp, trunclsnp)
	DB_ENV *dbenv;
	DB_LSN *lsnp, *trunclsnp;
{
	DB_LSN lsn;
	DBT mylog;
	DB_LOGC *logc;
	int ret, t_ret, undo;
	u_int32_t rectype;
	__txn_regop_args *txnrec;
	__txn_regop_gen_args *txngenrec;
	__txn_regop_rowlocks_args *txnrlrec;

	/* Figure out if we are backing out any commited transactions. */
	if ((ret = __log_cursor(dbenv, &logc)) != 0)
		return (ret);

	if (dbenv->recovery_start_callback)
		dbenv->recovery_start_callback(dbenv);

	memset(&mylog, 0, sizeof(mylog));
	undo = 0;
	while (undo == 0 &&
	    (ret = __log_c_get(logc, &lsn, &mylog, DB_PREV)) == 0 &&
	    log_compare(&lsn, lsnp) > 0) {
		LOGCOPY_32(&rectype, mylog.data);
		if (rectype == DB___txn_regop_rowlocks) {
			if ((ret =
				__txn_regop_rowlocks_read(dbenv, mylog.data,
				    &txnrlrec)) != 0)
				goto err;
			if (txnrlrec->opcode != TXN_ABORT) {
				undo = 1;
			}
			__os_free(dbenv, txnrlrec);
		}
		if (rectype == DB___txn_regop_gen) {
			if ((ret =
				__txn_regop_gen_read(dbenv, mylog.data,
				    &txngenrec)) != 0)
				goto err;
			if (txngenrec->opcode != TXN_ABORT) {
				undo = 1;
			}
			__os_free(dbenv, txngenrec);
		}
		if (rectype == DB___txn_regop) {
			if ((ret =
				__txn_regop_read(dbenv, mylog.data,
				    &txnrec)) != 0)
				goto err;
			if (txnrec->opcode != TXN_ABORT) {
				undo = 1;
			}
			__os_free(dbenv, txnrec);
		}
	}

	ret = __db_apprec(dbenv, lsnp, trunclsnp, undo, 0);

err:	if ((t_ret = __log_c_close(logc)) != 0 && ret == 0)
		ret = t_ret;

	if (dbenv->recovery_done_callback)
		dbenv->recovery_done_callback(dbenv);

	return (ret);
}

extern int gbl_extended_sql_debug_trace;

int
get_committed_lsns(dbenv, inlsns, n_lsns, epoch, file, offset)
	DB_ENV *dbenv;
	DB_LSN **inlsns;
	int *n_lsns;
	int epoch;
	int file;
	int offset;
{
	DB_LOGC *logc;
	DBT mylog;
	DB_LSN lsn;
	u_int32_t rectype;
	int ret, t_ret;
	int curlim = 0;
	void *txninfo;
	__txn_regop_args *txn_args;
	__txn_regop_gen_args *txn_gen_args;
	__txn_regop_rowlocks_args *txn_rl_args;
	int done = 0;
	DB_LSN *lsns = NULL, *newlsns;

	*n_lsns = 0;

#if 0
	txninfo = NULL;
	if ((ret = __db_txnlist_init(dbenv, 0, 0, NULL, &txninfo)) != 0)
		return ret;
#endif

	*inlsns = NULL;

	if ((ret = __log_cursor(dbenv, &logc)) != 0)
		return ret;

	bzero(&lsn, sizeof(lsn));
	bzero(&mylog, sizeof(mylog));

	ret = __log_c_get(logc, &lsn, &mylog, DB_LAST);
	if (ret) {
		logmsg(LOGMSG_ERROR, "%s:%d, %u:%u failed to get last log entry, ret=%d\n",
		    __FILE__, __LINE__, lsn.file, lsn.offset, ret);
		goto err;
	}

        while (!done &&
               (lsn.file > file || (lsn.file == file && lsn.offset > offset))) {
            LOGCOPY_32(&rectype, mylog.data);
            switch (rectype) {
            case DB___txn_regop_rowlocks: {
                if ((ret = __txn_regop_rowlocks_read(dbenv, mylog.data,
                                                     &txn_rl_args)) != 0) {
                    if (gbl_extended_sql_debug_trace) {
                        logmsg(LOGMSG_USER,
                               "td %u %s line %d lsn %d:%d "
                               "txn_regop_rowlocks_read returns %d\n",
                               (uint32_t)pthread_self(), __func__, __LINE__,
                               lsn.file, lsn.offset, ret);
                    }
                    return (ret);
                }

                if (txn_rl_args->timestamp < epoch) {
                    __os_free(dbenv, txn_rl_args);
                    if (gbl_extended_sql_debug_trace) {
                        logmsg(LOGMSG_USER, "td %u %s line %d lsn %d:%d "
                                            "break-loop because timestamp "
                                            "(%lu) < epoch (%d)\n",
                               (uint32_t)pthread_self(), __func__, __LINE__,
                               lsn.file, lsn.offset, txn_rl_args->timestamp,
                               epoch);
                    }
                    done = 1;
                    break;
                }

                if (txn_rl_args->opcode == TXN_COMMIT &&
                    txn_rl_args->lflags & DB_TXN_LOGICAL_COMMIT) {
                    if (*n_lsns + 1 >= curlim) {
                        curlim = (!curlim) ? 1000 : 2 * curlim;
                        if (!(newlsns = (DB_LSN *)realloc(
                                  lsns, curlim * sizeof(DB_LSN)))) {
                            logmsg(LOGMSG_ERROR, "%s:%d Too complex "
                                                 "snapshot (realloc "
                                                 "failure at trns %d)\n",
                                   __FILE__, __LINE__, *n_lsns);
                            ret = ENOMEM;
                            if (lsns) free(lsns);
                            lsns = NULL;
                            __os_free(dbenv, txn_rl_args);
                            goto err;
                        }
                        lsns = newlsns;
                    }

                    if (gbl_extended_sql_debug_trace) {
                        logmsg(LOGMSG_USER, "td %u %s line %d lsn %d:%d "
                                            "adding prev-lsn %d:%d at "
                                            "index %d\n",
                               (uint32_t)pthread_self(), __func__, __LINE__,
                               lsn.file, lsn.offset, txn_rl_args->prev_lsn.file,
                               txn_rl_args->prev_lsn.offset, *n_lsns);
                    }

                    lsns[*n_lsns] = txn_rl_args->prev_lsn;
                    *n_lsns += 1;
                }

                __os_free(dbenv, txn_rl_args);
            }

            break;

            case DB___txn_regop_gen: {
                if ((ret = __txn_regop_gen_read(dbenv, mylog.data,
                                                &txn_gen_args)) != 0) {
                    if (gbl_extended_sql_debug_trace) {
                        fprintf(stderr, "td %u %s line %d lsn %d:%d"
                                        "txn_regop_gen_read returns %d\n",
                                (uint32_t)pthread_self(), __func__, __LINE__,
                                lsn.file, lsn.offset, ret);
                    }
                    return (ret);
                }

                if (txn_gen_args->timestamp < epoch) {
#if 0
					fprintf(stderr,
					    "%s:%d stopped at epoch %u < %u\n",
					    __FILE__, __LINE__,
					    txn_gen_args->timestamp, epoch);
#endif
                        if (gbl_extended_sql_debug_trace) {
                            logmsg(LOGMSG_USER, "td %lu %s line %d lsn %d:%d "
                                                "break-loop because timestamp "
                                                "(%ld) < epoch (%d)\n",
                                   pthread_self(), __func__, __LINE__,
                                   lsn.file, lsn.offset,
                                   txn_gen_args->timestamp, epoch);
                        }
                        __os_free(dbenv, txn_gen_args);
                        done = 1;
                        break;
                    }

                    if (txn_gen_args->opcode == TXN_COMMIT) {
#if 0
					ret = __db_txnlist_add(dbenv,
					    txninfo, txn_gen_args.txnid->txnid,
					    TXN_COMMIT, NULL);
#endif
					if (*n_lsns + 1 >= curlim) {
						curlim =
						    (!curlim) ? 1000 : 2 *
						    curlim;
						if (!(newlsns =
							(DB_LSN *) realloc(lsns,
							    curlim *
							    sizeof(DB_LSN)))) {
							logmsg(LOGMSG_ERROR, 
                                "%s:%d Too complex snapshot (realloc failure at trns %d)\n",
							    __FILE__, __LINE__,
							    *n_lsns);
                                                        ret = ENOMEM;
                                                        if (lsns) free(lsns);
                                                        lsns = NULL;
                                                        __os_free(dbenv,
                                                                  txn_gen_args);
                                                        goto err;
                                                }
                                                lsns = newlsns;
                                        }

                                        if (gbl_extended_sql_debug_trace) {
                                            logmsg(
                                                LOGMSG_USER,
                                                "td %u %s line %d lsn %d:%d "
                                                "adding prev-lsn %d:%d at "
                                                "index %d\n",
                                                (uint32_t)pthread_self(),
                                                __func__, __LINE__, lsn.file,
                                                lsn.offset,
                                                txn_gen_args->prev_lsn.file,
                                                txn_gen_args->prev_lsn.offset,
                                                *n_lsns);
                                        }

                                        lsns[*n_lsns] = txn_gen_args->prev_lsn;
                                        *n_lsns += 1;
                    }
                    __os_free(dbenv, txn_gen_args);
                } break;

                case DB___txn_regop: {
                    if ((ret = __txn_regop_read(dbenv, mylog.data,
                                                &txn_args)) != 0) {
                        if (gbl_extended_sql_debug_trace) {
                            logmsg(LOGMSG_USER, "td %u %s line %d lsn %d:%d"
                                                "txn_regop_read returns %d\n",
                                   (uint32_t)pthread_self(), __func__, __LINE__,
                                   lsn.file, lsn.offset, ret);
                        }
                        return (ret);
                    }

                    if (txn_args->timestamp < epoch) {
#if 0
					fprintf(stderr,
					    "%s:%d stopped at epoch %u < %u\n",
					    __FILE__, __LINE__,
					    txn_args->timestamp, epoch);
#endif
                        if (gbl_extended_sql_debug_trace) {
                            logmsg(LOGMSG_USER, "td %u %s line %d lsn %d:%d "
                                                "break-loop because timestamp "
                                                "(%d) < epoch (%d)\n",
                                   (uint32_t)pthread_self(), __func__, __LINE__,
                                   lsn.file, lsn.offset, txn_args->timestamp,
                                   epoch);
                        }
                        __os_free(dbenv, txn_args);
                        done = 1;
                        break;
                    }

                    if (txn_args->opcode == TXN_COMMIT) {
#if 0
					ret = __db_txnlist_add(dbenv,
					    txninfo, txn_args.txnid->txnid,
					    TXN_COMMIT, NULL);
#endif
					if (*n_lsns + 1 >= curlim) {
						curlim =
						    (!curlim) ? 1000 : 2 *
						    curlim;
                                                if (!(newlsns =
                                                          (DB_LSN *)realloc(
                                                              lsns,
                                                              curlim *
                                                                  sizeof(
                                                                      DB_LSN)))) {
                                                    logmsg(LOGMSG_ERROR,
                                                           "%s:%d Too complex "
                                                           "snapshot (realloc "
                                                           "failure at trns "
                                                           "%d)\n",
                                                           __FILE__, __LINE__,
                                                           *n_lsns);
                                                    ret = ENOMEM;
                                                    if (lsns) free(lsns);
                                                    lsns = NULL;
                                                    __os_free(dbenv, txn_args);
                                                    goto err;
                                                }
                                                lsns = newlsns;
					}

                                        if (gbl_extended_sql_debug_trace) {
                                            logmsg(LOGMSG_USER,
                                                   "td %u %s line %d lsn %d:%d "
                                                   "adding prev-lsn %d:%d at "
                                                   "index %d\n",
                                                   (uint32_t)pthread_self(),
                                                   __func__, __LINE__, lsn.file,
                                                   lsn.offset,
                                                   txn_args->prev_lsn.file,
                                                   txn_args->prev_lsn.offset,
                                                   *n_lsns);
                                        }

                                        lsns[*n_lsns] = txn_args->prev_lsn;
                                        *n_lsns += 1;
                    }
                    __os_free(dbenv, txn_args);
                } break;

#if 0
		default:
			fprintf(stderr, "%s:%d Processing record type %d\n",
			    __FILE__, __LINE__);
#endif
		}
                if ((ret = __log_c_get(logc, &lsn, &mylog, DB_PREV)) != 0)
                    done = 1;
        }

        if (ret == DB_NOTFOUND) ret = 0;

err:
    if ((t_ret = __log_c_close(logc)) != 0 && ret == 0) {
        ret = t_ret;
        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER, "td %u %s line %d log_c_close error: %d\n",
                   (uint32_t)pthread_self(), __func__, __LINE__, ret);
        }
    }

    if (!ret) *inlsns = lsns;

    return ret;
}

void bdb_checkpoint_list_get_ckp_before_timestamp(int32_t timestamp,
    DB_LSN *lsnout);

int
get_lsn_context_from_timestamp(dbenv, timestamp, ret_lsn, ret_context)
	DB_ENV *dbenv;
	int32_t timestamp;
	DB_LSN *ret_lsn;
	unsigned long long *ret_context;
{
	int rc = 0;
	DB_LSN lsn;
	DB_LOGC *logc;
	DBT logdta;
	u_int32_t rectype;
	int got_lsn = 0;;

	__txn_regop_args *txn_args = NULL;
	__txn_regop_gen_args *txn_gen_args = NULL;
	__txn_regop_rowlocks_args *txn_rl_args = NULL;

	ret_lsn->file = 0;
	ret_lsn->offset = 1;
	if (ret_context)
		*ret_context = 0;

	lsn.file = 0;
	lsn.offset = 1;
	bdb_checkpoint_list_get_ckp_before_timestamp(timestamp, &lsn);
	*ret_lsn = lsn;

	if ((rc = __log_cursor(dbenv, &logc)) != 0) {
		logmsg(LOGMSG_ERROR, "%s:%d failed to get log cursor, rc %d\n",
		    __func__, __LINE__, rc);
		return -1;
	}
	bzero(&logdta, sizeof(logdta));
	logdta.flags = DB_DBT_REALLOC;
	if (lsn.file != 0 && lsn.offset != 1) {
		rc = logc->get(logc, &lsn, &logdta, DB_SET);
		if (rc) {
			logmsg(LOGMSG_ERROR, 
                "%s:%d failed to get log at [%u][%u], rc %d\n",
			    __func__, __LINE__, lsn.file, lsn.offset, rc);
			__log_c_close(logc);
			return -1;
		}
	} else {
		rc = logc->get(logc, &lsn, &logdta, DB_FIRST);
		if (rc) {
			logmsg(LOGMSG_ERROR, 
                "%s:%d failed to get first rc %d\n",
			    __func__, __LINE__, rc);
			__log_c_close(logc);
			return -1;
		}
	}

	for (rc = 0; rc == 0; rc = logc->get(logc, &lsn, &logdta, DB_NEXT)) {
		LOGCOPY_32(&rectype, logdta.data);
		if (rectype == DB___txn_regop) {
			if ((rc =
				__txn_regop_read(dbenv, logdta.data,
				    &txn_args)) != 0)
				goto err;
			if (txn_args->timestamp <= timestamp) {
				*ret_lsn = lsn;
				if (ret_context)
					*ret_context =
					    __txn_regop_read_context(txn_args);
				got_lsn = 1;
			}
			if (txn_args->timestamp > timestamp) {
				if (logdta.data) {
					__os_free(dbenv, logdta.data);
					logdta.data = NULL;
				}
				__os_free(dbenv, txn_args);
				__log_c_close(logc);
				return 0;
			}
			__os_free(dbenv, txn_args);
			txn_args = NULL;
		}

		if (rectype == DB___txn_regop_gen) {
			if ((rc =
				__txn_regop_gen_read(dbenv, logdta.data,
				    &txn_gen_args)) != 0)
				goto err;
			if (txn_gen_args->timestamp <= timestamp) {
				*ret_lsn = lsn;
				if (ret_context)
					*ret_context = txn_gen_args->context;
				got_lsn = 1;
			}
			if (txn_gen_args->timestamp > timestamp) {
				if (logdta.data) {
					__os_free(dbenv, logdta.data);
					logdta.data = NULL;
				}
				__os_free(dbenv, txn_gen_args);
				__log_c_close(logc);
				return 0;
			}
			__os_free(dbenv, txn_gen_args);
			txn_gen_args = NULL;
		}


		else if (rectype == DB___txn_regop_rowlocks) {
			if ((rc =
				__txn_regop_rowlocks_read(dbenv, logdta.data,
				    &txn_rl_args)) != 0)
				goto err;
			if (txn_rl_args->timestamp <= timestamp) {
				*ret_lsn = lsn;
				if (ret_context)
					*ret_context = txn_rl_args->context;
				got_lsn = 1;
			}
			if (txn_rl_args->timestamp > timestamp) {
				if (logdta.data) {
					__os_free(dbenv, logdta.data);
					logdta.data = NULL;
				}
				__os_free(dbenv, txn_rl_args);
				__log_c_close(logc);
				return 0;
			}
			__os_free(dbenv, txn_rl_args);
			txn_rl_args = NULL;
		}
		if (logdta.data) {
			__os_free(dbenv, logdta.data);
			logdta.data = NULL;
		}
	}
	assert(rc == DB_NOTFOUND);

err:
	if (logdta.data) {
		__os_free(dbenv, logdta.data);
		logdta.data = NULL;
	}
	__log_c_close(logc);

	return 0;
}

int
get_context_from_lsn(dbenv, lsn, ret_context)
	DB_ENV *dbenv;
	DB_LSN lsn;
	unsigned long long *ret_context;
{
	int rc = 0;
	DB_LOGC *logc;
	DBT logdta = {0};
	u_int32_t rectype;

	__txn_regop_args *txn_args = NULL;
	__txn_regop_gen_args *txn_gen_args = NULL;
	__txn_regop_rowlocks_args *txn_rl_args = NULL;

	*ret_context = 0;

	if ((rc = __log_cursor(dbenv, &logc)) != 0) {
		logmsg(LOGMSG_ERROR, 
            "%s:%d failed to get log cursor, rc %d\n",
		    __func__, __LINE__, rc);
		return -1;
	}
    logdta.flags = DB_DBT_REALLOC;
	rc = logc->get(logc, &lsn, &logdta, DB_SET);
	if (rc) {
		logmsg(LOGMSG_ERROR, 
            "%s:%d failed to get log at [%u][%u], rc %d\n",
		    __func__, __LINE__, lsn.file, lsn.offset, rc);
		__log_c_close(logc);
		return -1;
	}

    LOGCOPY_32(&rectype, logdta.data);
    while (rectype != DB___txn_regop && rectype != DB___txn_regop_gen && 
            rectype != DB___txn_regop_rowlocks) {
        if ((rc = logc->get(logc, &lsn, &logdta, DB_PREV)) != 0) {
            logmsg(LOGMSG_ERROR, "%s:%d failed find log on prev, rc %d\n",
                    __func__, __LINE__, rc);
            if (logdta.data) {
                __os_free(dbenv, logdta.data);
                logdta.data = NULL;
            }
            __log_c_close(logc);
            return -1;
        }
        LOGCOPY_32(&rectype, logdta.data);
    }

	assert(rectype == DB___txn_regop || rectype == DB___txn_regop_gen ||
	    rectype == DB___txn_regop_rowlocks);
	if (rectype == DB___txn_regop) {
		if ((rc = __txn_regop_read(dbenv, logdta.data, &txn_args)) != 0)
			goto err;
		*ret_context = __txn_regop_read_context(txn_args);
		if (logdta.data) {
			__os_free(dbenv, logdta.data);
			logdta.data = NULL;
		}
		__os_free(dbenv, txn_args);
		__log_c_close(logc);
		return 0;
	} else if (rectype == DB___txn_regop_gen) {
		if ((rc =
			__txn_regop_gen_read(dbenv, logdta.data,
			    &txn_gen_args)) != 0)
			goto err;
		*ret_context = txn_gen_args->context;
		if (logdta.data) {
			__os_free(dbenv, logdta.data);
			logdta.data = NULL;
		}
		__os_free(dbenv, txn_gen_args);
		__log_c_close(logc);
		return 0;
	} else if (rectype == DB___txn_regop_rowlocks) {
		if ((rc =
			__txn_regop_rowlocks_read(dbenv, logdta.data,
			    &txn_rl_args)) != 0)
			goto err;
		*ret_context = txn_rl_args->context;
		if (logdta.data) {
			__os_free(dbenv, logdta.data);
			logdta.data = NULL;
		}
		__os_free(dbenv, txn_rl_args);
		__log_c_close(logc);
		return 0;
	}
err:
	if (logdta.data) {
		__os_free(dbenv, logdta.data);
		logdta.data = NULL;
	}
	__log_c_close(logc);

	return -1;
}

unsigned long long
get_current_context(dbenv)
	DB_ENV *dbenv;
{
	unsigned long long context = 0;
	int rc = 0;
	DB_LSN lsn;
	DB_LOGC *logc;
	DBT logdta;
	u_int32_t rectype;

	__txn_regop_args *txn_args = NULL;
	__txn_regop_gen_args *txn_gen_args = NULL;
	__txn_regop_rowlocks_args *txn_rl_args = NULL;

	__log_txn_lsn(dbenv, &lsn, NULL, NULL);

	if ((rc = __log_cursor(dbenv, &logc)) != 0) {
		logmsg(LOGMSG_ERROR, "%s:%d failed to get log cursor, rc %d\n",
		    __func__, __LINE__, rc);
		return 0;
	}
	bzero(&logdta, sizeof(logdta));
	logdta.flags = DB_DBT_REALLOC;
	rc = logc->get(logc, &lsn, &logdta, DB_SET);
	if (rc) {
		logmsg(LOGMSG_ERROR, "%s:%d failed to get log at [%u][%u], rc %d\n",
		    __func__, __LINE__, lsn.file, lsn.offset, rc);
		__log_c_close(logc);
		return 0;
	}
	LOGCOPY_32(&rectype, logdta.data);

	while (rectype != DB___txn_regop || rectype != DB___txn_regop_gen ||
	    rectype != DB___txn_regop_rowlocks) {
again:
		if (logdta.data) {
			__os_free(dbenv, logdta.data);
			logdta.data = NULL;
		}
		rc = logc->get(logc, &lsn, &logdta, DB_PREV);
		if (rc) {
			logmsg(LOGMSG_ERROR, 
                "%s:%d failed to get log at [%u][%u], rc %d\n",
			    __func__, __LINE__, lsn.file, lsn.offset, rc);
			__log_c_close(logc);
			return 0;
		}
		LOGCOPY_32(&rectype, logdta.data);
	}

	if (rectype == DB___txn_regop) {
		if ((rc = __txn_regop_read(dbenv, logdta.data, &txn_args)) != 0)
			abort();
		if (txn_args->opcode != TXN_COMMIT) {
			__os_free(dbenv, txn_args);
			goto again;
		}
		context = __txn_regop_read_context(txn_args);
		__os_free(dbenv, txn_args);
		__log_c_close(logc);
		return context;
	}
	else if (rectype == DB___txn_regop_gen) {
		if ((rc =
			__txn_regop_gen_read(dbenv, logdta.data,
			    &txn_gen_args)) != 0) {
			abort();
		}
		if (txn_gen_args->opcode != TXN_COMMIT) {
			__os_free(dbenv, txn_gen_args);
			goto again;
		}
		context = txn_gen_args->context;
		__os_free(dbenv, txn_gen_args);
		__log_c_close(logc);
		return context;
	}
	else {
		assert(rectype == DB___txn_regop_rowlocks);
		if ((rc =
			__txn_regop_rowlocks_read(dbenv, logdta.data,
			    &txn_rl_args)) != 0)
			abort();
		if (txn_rl_args->opcode != TXN_COMMIT) {
			__os_free(dbenv, txn_rl_args);
			goto again;
		}
		context = txn_rl_args->context;
		__os_free(dbenv, txn_rl_args);
		__log_c_close(logc);
		return context;
	}

	return 0;
}

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

typedef struct del_repdb_args {
	DB_ENV *dbenv;
	DB *oldrepdb;
	char *oldrepdbname;
} del_repdb_args_t;

static void *
del_thd(void *arg)
{
	del_repdb_args_t *delr = (del_repdb_args_t *)arg;
	DB_ENV *dbenv;
	DB *dbp;
	char *repdbname;
	int ret;

	dbenv = delr->dbenv;

	if ((ret = __os_malloc(dbenv, strlen(dbenv->comdb2_dirs.txn_dir) +
		    strlen(delr->oldrepdbname) + 16, &repdbname)) != 0)
		abort();

	sprintf(repdbname, "%s/%s", dbenv->comdb2_dirs.txn_dir,
	    delr->oldrepdbname);

	F_CLR(delr->oldrepdb, DB_AM_RECOVER);

	/* First close it */
	if ((ret = __db_close(delr->oldrepdb, NULL, DB_NOSYNC)) != 0) {
		logmsg(LOGMSG_ERROR, "Error on db_close of %s, ret=%d\n",
		    delr->oldrepdbname, ret);
		goto err;
	}

	if ((ret = db_create(&dbp, dbenv, DB_REP_CREATE)) != 0) {
		logmsg(LOGMSG_ERROR, "Error closing %s, ret=%d\n",
		    delr->oldrepdbname, ret);
		goto err;
	}

	if ((ret =
		__db_remove(dbp, NULL, delr->oldrepdbname, NULL,
		    DB_NOSYNC)) != 0) {
		logmsg(LOGMSG_ERROR, "Couldn't db_remove %s ret=%d\n",
		    delr->oldrepdbname, ret);
		goto err;
	}

err:
	__os_free(dbenv, repdbname);
	__os_free(dbenv, delr->oldrepdbname);
	__os_free(dbenv, delr);

	return NULL;
}

static int
__truncate_repdb(dbenv)
	DB_ENV *dbenv;
{
	DB_REP *db_rep;
	REP *rep;
	u_int32_t unused;
	int ret = 0;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	if (!F_ISSET(rep, REP_ISCLIENT) || !db_rep->rep_db)
		return DB_NOTFOUND;

	MUTEX_LOCK(dbenv, db_rep->db_mutexp);
	F_SET(db_rep->rep_db, DB_AM_RECOVER);
	MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);

	if (!gbl_optimize_truncate_repdb) {
		ret = __db_truncate(db_rep->rep_db, NULL, &unused, 0);
	} else {
#define	REPDBBASE	"__db.rep.db"
		DB *dbp = NULL;
		int ret;
		u_int32_t flags;
		char *repdbname;
		del_repdb_args_t *delr;
		int rc;
		pthread_t tid;
		pthread_attr_t attr;

		logmsg(LOGMSG_INFO, "truncating %s\n", db_rep->repdbname);

		MUTEX_LOCK(dbenv, db_rep->db_mutexp);

		if ((ret = db_create(&dbp, dbenv, DB_REP_CREATE)) != 0) {
			abort();
			goto err;
		}

		if ((ret = __bam_set_bt_compare(dbp, __rep_bt_cmp)) != 0) {
			abort();
			goto err;
		}

		/* Allow writes to this database on a client. */
		F_SET(dbp, DB_AM_CL_WRITER);

		flags = DB_NO_AUTO_COMMIT |
		    DB_CREATE | (F_ISSET(dbenv, DB_ENV_THREAD) ? DB_THREAD : 0);

		/* Set the pagesize. */
		if (dbenv->rep_db_pagesize > 0) {
			if ((ret =
				dbp->set_pagesize(dbp,
				    dbenv->rep_db_pagesize))) {
				abort();
				goto err;
			}
		}

		if ((ret =
			__os_malloc(dbenv, strlen(REPDBBASE) + 32,
			    &repdbname)) != 0) {
			abort();
			goto err;
		}

		sprintf(repdbname, "%s.%ld.%d", REPDBBASE, time(NULL),
		    db_rep->repdbcnt++);

		if ((ret = __db_open(dbp, NULL,
			    repdbname, NULL, DB_BTREE, flags, 0,
			    PGNO_BASE_MD)) != 0) {
			abort();
			goto err;
		}

		/* Install new repdb */
		F_SET(dbp, DB_AM_RECOVER);

		if ((ret = __os_malloc(dbenv, sizeof(*delr), &delr)) != 0) {
			abort();
			goto err;
		}

		delr->dbenv = dbenv;
		delr->oldrepdb = db_rep->rep_db;
		delr->oldrepdbname = db_rep->repdbname;

		db_rep->repdbname = repdbname;
		db_rep->rep_db = dbp;

		pthread_attr_init(&attr);
		pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

		rc = pthread_create(&tid, &attr, del_thd, delr);
		if (rc != 0) {
			logmsg(LOGMSG_FATAL, "couldnt create del_thd\n");
			exit(1);
		}

		MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);
	}

err:

	MUTEX_LOCK(dbenv, db_rep->db_mutexp);
	F_CLR(db_rep->rep_db, DB_AM_RECOVER);
	MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);

	return ret;
}

// PUBLIC: int __rep_truncate_repdb __P((DB_ENV *));
int
__rep_truncate_repdb(dbenv)
	DB_ENV *dbenv;
{
	int ret;
	PANIC_CHECK(dbenv);
	ret = __truncate_repdb(dbenv);
	return (ret);
}



/*
 * __rep_verify_match --
 *	We have just received a matching log record during verification.
 * Figure out if we're going to need to run recovery. If so, wait until
 * everything else has exited the library.  If not, set up the world
 * correctly and move forward.
 */
static int
__rep_verify_match(dbenv, rp, savetime)
	DB_ENV *dbenv;
	REP_CONTROL *rp;
	time_t savetime;
{
	DB_LOG *dblp;
	DB_LSN ckplsn, trunclsn, prevlsn, purge_lsn;
	DB_REP *db_rep;
	LOG *lp;
	REP *rep;
	int done, ret, wait_cnt;
	u_int32_t unused;
	extern int gbl_passed_repverify;
	char *master;

	void *bdb_state = dbenv->app_private;

	dblp = dbenv->lg_handle;
	db_rep = dbenv->rep_handle;
	rep = db_rep->region;
	lp = dblp->reginfo.primary;
	ret = 0;

	/*
	 * Check if the savetime is different than our current time stamp.
	 * If it is, then we're racing with another thread trying to recover
	 * and we lost.  We must give up.
	 */

	MUTEX_LOCK(dbenv, db_rep->db_mutexp);
	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	done = savetime != rep->timestamp;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
	if (done) {
		MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);
		return (0);
	}

	ZERO_LSN(lp->verify_lsn);

	/* Check if we our log is already up to date. */
	R_LOCK(dbenv, &dblp->reginfo);

	prevlsn = lp->lsn;

	done = rp->lsn.file == lp->lsn.file &&
	    rp->lsn.offset + lp->len == lp->lsn.offset;
	if (done && dbenv->attr.always_run_recovery) {
		ctrace("Wasn't going to run recovery, but running anyway\n");
		done = 0;
	}
	if (done) {
		purge_lsn = lp->ready_lsn = lp->lsn;
		/*
		 * fprintf(stderr, "Set ready_lsn file %s line %d to %d:%d\n", 
		 * __FILE__, __LINE__, lp->ready_lsn.file, 
		 * lp->ready_lsn.offset);
		 */
		ZERO_LSN(lp->waiting_lsn);
	}
	R_UNLOCK(dbenv, &dblp->reginfo);
	if (done) {
		ctrace("%s matched current log [%d:%d] no truncate\n",
		    __func__, lp->lsn.file, lp->lsn.offset);
		goto finish;	/* Yes, holding the mutex. */
	}
	MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);

	/* We sniffed out rep_verify in rep.c, & grabbed the writelock there. */

	/* Parallel rep threads could still be working- wait for them to complete
	 * before grabbing the rep_mutex. */
	wait_for_running_transactions(dbenv);

	/*
	 * Make sure the world hasn't changed while we tried to get
	 * the lock.  If it hasn't then it's time for us to kick all
	 * operations out of DB and run recovery.
	 */
	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	if (F_ISSET(rep, REP_F_READY) || rep->in_recovery != 0) {
		rep->stat.st_msgs_recover++;
		goto errunlock;
	}

	/* Phase 1: set REP_F_READY and wait for op_cnt to go to 0. */
	/* This doesn't do anything anymore, but we should have gotten the 
	 * bdb write mutex in the calling code */
	F_SET(rep, REP_F_READY);
	for (wait_cnt = 0; rep->op_cnt != 0;) {
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		__os_sleep(dbenv, 1, 0);
#ifdef DIAGNOSTIC
		if (++wait_cnt % 60 == 0)
			__db_err(dbenv,
			    "Waiting for txn_cnt to run replication recovery for %d minutes",
			    wait_cnt / 60);
#endif
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	}



	/*
	 * Phase 2: set in_recovery and wait for handle count to go
	 * to 0 and for the number of threads in __rep_process_message
	 * to go to 1 (us).
	 */
	rep->in_recovery = 1;
	for (wait_cnt = 0; rep->handle_cnt != 0 || rep->msg_th > 1;) {
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		__os_sleep(dbenv, 1, 0);
#ifdef DIAGNOSTIC
		if (++wait_cnt % 60 == 0)
			__db_err(dbenv,
			    "Waiting for handle/thread count to run replication recovery for %d minutes",
			    wait_cnt / 60);
#endif
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	}

	/* OK, everyone is out, we can now run recovery. */
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

	if ((ret = __rep_dorecovery(dbenv, &rp->lsn, &trunclsn)) != 0) {
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		rep->in_recovery = 0;
		F_CLR(rep, REP_F_READY);
		goto errunlock;
	}

	ctrace("%s truncated log from [%d:%d] to [%d:%d]\n",
	    __func__, prevlsn.file, prevlsn.offset, trunclsn.file,
	    trunclsn.offset);

	/*
	 * The log has been truncated (either directly by us or by __db_apprec)
	 * We want to make sure we're waiting for the LSN at the new end-of-log,
	 * not some later point.
	 */
	MUTEX_LOCK(dbenv, db_rep->db_mutexp);
	purge_lsn = lp->ready_lsn = trunclsn;
	/*
	 * fprintf(stderr, "Set readylsn file %s line %d to %d:%d\n", __FILE__, 
	 * __LINE__, lp->ready_lsn.file, lp->ready_lsn.offset);
	 */
finish:ZERO_LSN(lp->waiting_lsn);
	lp->wait_recs = 0;
	lp->rcvd_recs = 0;
	ZERO_LSN(lp->verify_lsn);

	/*
	 * Discard any log records we have queued;  we're about to re-request
	 * them, and can't trust the ones in the queue.  We need to set the
	 * DB_AM_RECOVER bit in this handle, so that the operation doesn't
	 * deadlock.
	 */

	F_SET(db_rep->rep_db, DB_AM_RECOVER);
	MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);

	if ((ret = __truncate_repdb(dbenv)) != 0) {
		abort();
		goto err;
	}

	MUTEX_LOCK(dbenv, db_rep->db_mutexp);
	F_CLR(db_rep->rep_db, DB_AM_RECOVER);

	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	rep->stat.st_log_queued = 0;
	rep->in_recovery = 0;
	F_CLR(rep, REP_F_NOARCHIVE | REP_F_READY | REP_F_RECOVER);


	if (ret != 0)
		goto errunlock2;

	/*
	 * If the master_id is invalid, this means that since
	 * the last record was sent, somebody declared an
	 * election and we may not have a master to request
	 * things of.
	 *
	 * This is not an error;  when we find a new master,
	 * we'll re-negotiate where the end of the log is and
	 * try to bring ourselves up to date again anyway.
	 *
	 * !!!
	 * We cannot assert the election flags though because
	 * somebody may have declared an election and then
	 * got an error, thus clearing the election flags
	 * but we still have an invalid master_id.
	 */
	master = rep->master_id;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
	if (master == db_eid_invalid) {
		MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);
		ret = 0;
	} else {
		/*
		 * We're making an ALL_REQ.  But now that we've
		 * cleared the flags, we're likely receiving new
		 * log records from the master, resulting in a gap
		 * immediately.  So to avoid multiple data streams,
		 * set the wait_recs value high now to give the master
		 * a chance to start sending us these records before
		 * the gap code re-requests the same gap.  Wait_recs
		 * will get reset once we start receiving these
		 * records.
		 */
		lp->wait_recs = rep->max_gap;
		MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);
		(void)__rep_send_message(dbenv,
		    master, REP_ALL_REQ, &rp->lsn, NULL, 0, NULL);
	}
	if (0) {
errunlock2:	MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);
errunlock:	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

	}

	/* Purge any in-flight logical transactions.  We hold the bdb writelock. */
	bdb_purge_logical_transactions(dbenv->app_private, &purge_lsn);

	/* passed verify */
	gbl_passed_repverify = 1;

err:

	return (ret);
}

int __rep_block_on_inflight_transactions(DB_ENV *dbenv)
{
	struct timespec ts;
	int rc;

	pthread_mutex_lock(&dbenv->recover_lk);
	while (listc_size(&dbenv->inflight_transactions) > 0) {
		clock_gettime(CLOCK_REALTIME, &ts);
		ts.tv_sec++;
		pthread_cond_timedwait(&dbenv->recover_cond, &dbenv->recover_lk, &ts);
		if (listc_size(&dbenv->inflight_transactions) > 0) {
			logmsg(LOGMSG_ERROR, "%s: waiting for %d processor threads "
					"to exit\n", __func__,
					listc_size(&dbenv->inflight_transactions));
		}
	}
	pthread_mutex_unlock(&dbenv->recover_lk);
	return 0;
}


// PUBLIC: int __rep_inflight_txns_older_than_lsn __P((DB_ENV *, DB_LSN *));
int
__rep_inflight_txns_older_than_lsn(DB_ENV *dbenv, DB_LSN *lsn)
{
	struct __recovery_processor *rp;
	pthread_mutex_lock(&dbenv->recover_lk);
	LISTC_FOR_EACH(&dbenv->inflight_transactions, rp, lnk) {
		if (log_compare(&rp->commit_lsn, lsn) < 0) {
			pthread_mutex_unlock(&dbenv->recover_lk);
			/*printf("inflight transaction %u:%u checkpoint lsn %u:%u\n", 
			 * rp->commit_lsn.file, rp->commit_lsn.offset,
			 * lsn->file, lsn->offset); */
			return 1;
		}
	}
	pthread_mutex_unlock(&dbenv->recover_lk);
	return 0;
}

/* Not crazy about leaving this here.  This is used in bdb and berkdb.  It's
 * initialized in db, early in main.  It doesn't really belong in any one place. */
char *db_eid_broadcast = NULL;
char *db_eid_invalid = NULL;
