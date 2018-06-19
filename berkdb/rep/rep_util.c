/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2001-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"
#include "dbinc/db_swap.h"
#include "logmsg.h"
#include <epochlib.h>

#ifndef lint
static const char revid[] = "$Id: rep_util.c,v 1.103 2003/11/14 05:32:32 ubell Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <stdlib.h>
#include <string.h>
#include <time.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/btree.h"
#include "dbinc/fop.h"
#include "dbinc/hash.h"
#include "dbinc/log.h"
#include "dbinc/lock.h"
#include "dbinc/qam.h"
#include "dbinc/txn.h"

#ifndef TESTSUITE
#include <ctrace.h>

#include "util.h"

extern pthread_mutex_t rep_candidate_lock;
extern int gbl_passed_repverify;
struct bdb_state_tag;
void bdb_set_rep_handle_dead(struct bdb_state_tag *);
#endif

int gbl_verbose_master_req = 0;

/*
 * rep_util.c:
 *	Miscellaneous replication-related utility functions, including
 *	those called by other subsystems.
 */

#ifdef REP_DIAGNOSTIC
static void __rep_print_logmsg __P((DB_ENV *, const DBT *, DB_LSN *));
#endif

/*
 * __rep_check_alloc --
 *	Make sure the array of TXN_REC entries is of at least size n.
 *	(This function is called by the __*_getpgnos() functions in
 *	*.src.)
 *
 * PUBLIC: int __rep_check_alloc __P((DB_ENV *, TXN_RECS *, int));
 */
int
__rep_check_alloc(dbenv, r, n)
	DB_ENV *dbenv;
	TXN_RECS *r;
	int n;
{
	int nalloc, ret;

	while (r->nalloc < r->npages + n) {
		nalloc = r->nalloc == 0 ? 20 : r->nalloc * 2;

		if ((ret = __os_realloc(dbenv, nalloc * sizeof(LSN_PAGE),
		    &r->array)) != 0)
			return (ret);

		r->nalloc = nalloc;
	}

	return (0);
}

extern int gbl_verbose_fills;

static inline int is_logput(int type) {
    switch (type) {
        case REP_LOG:
        case REP_LOG_LOGPUT:
        case REP_LOG_FILL:
        case REP_LOG_MORE:
            return 1;
        default:
            return 0;
    }
}

/*
 * __rep_send_message --
 *	This is a wrapper for sending a message.  It takes care of constructing
 * the REP_CONTROL structure and calling the user's specified send function.
 *
 * PUBLIC: int __rep_send_message __P((DB_ENV *, char*,
 * PUBLIC:     u_int32_t, DB_LSN *, const DBT *, u_int32_t,
 * PUBLIC:     void *usr_ptr));
 */
int
__rep_send_message(dbenv, eid, rtype, lsnp, dbtp, flags, usr_ptr)
	DB_ENV *dbenv;
	char *eid;
	u_int32_t rtype;
	DB_LSN *lsnp;
	const DBT *dbtp;
	u_int32_t flags;
	void *usr_ptr;
{
	DB_REP *db_rep;
	REP *rep;
	DBT cdbt, scrap_dbt;
	REP_CONTROL cntrl;
	int ret;
	u_int32_t myflags, rectype = 0;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	if (gbl_verbose_master_req) {
		switch (rtype) {
			case REP_MASTER_REQ:
				logmsg(LOGMSG_USER, "%s sending REP_MASTER_REQ to %s\n",
					__func__, eid);
				break;
			case REP_NEWMASTER:
				logmsg(LOGMSG_USER, "%s sending REP_NEWMASTER to %s\n",
					__func__, eid);
				break;
			default: 
				break;
		}
	}

	/* Set up control structure. */
	memset(&cntrl, 0, sizeof(cntrl));
	if (lsnp == NULL)
		ZERO_LSN(cntrl.lsn);
	else
		cntrl.lsn = *lsnp;
	cntrl.rectype = rtype;
	cntrl.flags = flags;
	cntrl.rep_version = DB_REPVERSION;
	cntrl.log_version = DB_LOGVERSION;
	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	cntrl.gen = rep->gen;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);


	memset(&cdbt, 0, sizeof(cdbt));
	cdbt.data = &cntrl;
	cdbt.size = sizeof(cntrl);

	/* Don't assume the send function will be tolerant of NULL records. */
	if (dbtp == NULL) {
		memset(&scrap_dbt, 0, sizeof(DBT));
		dbtp = &scrap_dbt;
	}

	/*
	 * I'm seeing commits without DB_LOG_PERM set on the
	 * replicant.  Find out why. 
	 * Mystery solved: these are being sent as a result of a client doing a 
	 * REQ_ALL request 
	 */
#if 0
	if (dbtp->size >= sizeof(rectype))
		LOGCOPY_32(&rectype, dbtp->data);

	if ((rtype == REP_LOG || rtype == REP_LOG_LOGPUT) &&
	    (rectype == DB___txn_regop)) {
		assert(LF_ISSET(DB_LOG_PERM));
	}
#endif

#ifdef DIAGNOSTIC
	if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
		__rep_print_message(dbenv, eid, &cntrl, "rep_send_message");
#endif
#ifdef REP_DIAGNOSTIC
	if (rtype == REP_LOG || rtype == REP_LOG_LOGPUT)
		__rep_print_logmsg(dbenv, dbtp, lsnp);
#endif
	/*
	 * There are three types of records: commit and checkpoint records
	 * that affect database durability, regular log records that might
	 * be buffered on the master before being transmitted, and control
	 * messages which don't require the guarantees of permanency, but
	 * should not be buffered.
	 */
	myflags = 0;
	if (LF_ISSET(DB_LOG_PERM)) {
		myflags = DB_REP_PERMANENT;
	} else if (is_logput(rtype)) {
		myflags = DB_REP_LOGPROGRESS;
        myflags |= (flags & (DB_REP_NOBUFFER|DB_REP_NODROP));
	} else if (!is_logput(rtype)) {
		myflags = DB_REP_NOBUFFER;
	} else {
		/*
		 * Check if this is a log record we just read that
		 * may need a DB_LOG_PERM.  This is of type REP_LOG,
		 * so we know that dbtp is a log record.
		 */
		memcpy(&rectype, dbtp->data, sizeof(rectype));
		if (rectype == DB___txn_regop || rectype == DB___txn_regop_gen
		    || rectype == DB___txn_ckp ||
		    rectype == DB___txn_regop_rowlocks)
			F_SET(&cntrl, DB_LOG_PERM);
	}

	if (LF_ISSET(DB_LOG_REP_ACK)) {
		myflags |= DB_REP_FLUSH;
	}

	/*
	 * We set the LSN above to something valid.  Give the master the
	 * actual LSN so that they can coordinate with permanent records from
	 * the client if they want to.
	 */

	if (LOG_SWAPPED())
		__rep_control_swap(&cntrl);

    if (LF_ISSET(DB_REP_TRACE)) {
        logmsg(LOGMSG_USER, "%s line %d tracing for rtype %d\n", __func__, 
                __LINE__, rtype);
        myflags |= DB_REP_TRACE;
    }

	ret = dbenv->rep_send(dbenv, &cdbt, dbtp, &cntrl.lsn, eid, myflags,
	    usr_ptr);

    if (LF_ISSET(DB_REP_TRACE)) {
        logmsg(LOGMSG_USER, "%s line %d rep_send returns %d\n", __func__, 
                __LINE__, ret);
    }

	/* Do we need to swap back? */
	if (LOG_SWAPPED())
		__rep_control_swap(&cntrl);

	/*
	 * We don't hold the rep lock, so this could miscount if we race.
	 * I don't think it's worth grabbing the mutex for that bit of
	 * extra accuracy.
	 */
	if (ret == 0)
		rep->stat.st_msgs_sent++;
	else
		rep->stat.st_msgs_send_failures++;

#ifdef DIAGNOSTIC
	if (ret != 0 && FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
		__db_err(dbenv, "rep_send_function returned: %d", ret);
#endif
	return (ret);
}

#ifdef REP_DIAGNOSTIC

/*
 * __rep_print_logmsg --
 *	This is a debugging routine for printing out log records that
 * we are about to transmit to a client.
 */

static void
__rep_print_logmsg(dbenv, logdbt, lsnp)
	DB_ENV *dbenv;
	const DBT *logdbt;
	DB_LSN *lsnp;
{
	/* Static structures to hold the printing functions. */
	static int (**ptab) __P((DB_ENV *,
		DBT *, DB_LSN *, db_recops, void *)) = NULL;
	size_t ptabsize = 0;

	if (ptabsize == 0) {
		/* Initialize the table. */
		(void)__bam_init_print(dbenv, &ptab, &ptabsize);
		(void)__crdel_init_print(dbenv, &ptab, &ptabsize);
		(void)__db_init_print(dbenv, &ptab, &ptabsize);
		(void)__dbreg_init_print(dbenv, &ptab, &ptabsize);
		(void)__fop_init_print(dbenv, &ptab, &ptabsize);
		(void)__ham_init_print(dbenv, &ptab, &ptabsize);
		(void)__qam_init_print(dbenv, &ptab, &ptabsize);
		(void)__txn_init_print(dbenv, &ptab, &ptabsize);
	}

	(void)__db_dispatch(dbenv,
	    ptab, ptabsize, (DBT *)logdbt, lsnp, DB_TXN_PRINT, NULL);
}

#endif
/*
 * __rep_set_gen --
 *  Called as a utility function to see places where an instance's 
 * replication generation can be changed.
 *
 * PUBLIC: void __rep_set_gen __P((DB_ENV *, const char *func, int line, int gen));
 */
void
__rep_set_gen(dbenv, func, line, gen)
    DB_ENV *dbenv;
    const char *func;
    int line;
    int gen;
{
	DB_REP *db_rep;
	REP *rep;
	db_rep = dbenv->rep_handle;
	rep = db_rep->region;
    logmsg(LOGMSG_DEBUG, "%s line %d setting rep->gen to %d\n", func, line, gen);
    rep->gen = gen;
}

/*
 * __rep_new_master --
 *	Called after a master election to sync back up with a new master.
 * It's possible that we already know of this new master in which case
 * we don't need to do anything.
 *
 * This is written assuming that this message came from the master; we
 * need to enforce that in __rep_process_record, but right now, we have
 * no way to identify the master.
 *
 * PUBLIC: int __rep_new_master __P((DB_ENV *, REP_CONTROL *, char *));
 */

int gbl_abort_on_incorrect_upgrade;
extern int last_fill;
extern int gbl_decoupled_logputs;

int
__rep_new_master(dbenv, cntrl, eid)
	DB_ENV *dbenv;
	REP_CONTROL *cntrl;
	char *eid;
{
	DB_LOG *dblp;
	DB_LOGC *logc;
	DB_LSN last_lsn, lsn;
	DB_REP *db_rep;
	DBT dbt;
	LOG *lp;
	REP *rep;
	int change, ret, t_ret;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;
	ret = 0;
    pthread_mutex_lock(&rep_candidate_lock);
	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);

        /* This should never happen: we are calling new-master against a
           network message with a lower generation.  I believe this is the
           election bug that I've been tracking down: this node's generation
           can change from when we initially checked it at the top of
           process_message. */
        logmsg(LOGMSG_DEBUG, "%s: my-gen=%u ctl-gen=%u rep-master=%s new=%s\n",
               __func__, rep->gen, cntrl->gen, rep->master_id, eid);
        if (rep->gen > cntrl->gen) {
            logmsg(LOGMSG_INFO,
                   "%s: rep-gen (%u) > cntrl->gen (%u): ignoring upgrade\n",
                   __func__, rep->gen, cntrl->gen);

            if (gbl_abort_on_incorrect_upgrade) abort();

            MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
            pthread_mutex_unlock(&rep_candidate_lock);
            rep->stat.st_msgs_badgen++;
            return 0;
        }

        if (cntrl->gen < rep->gen)
            abort();

        __rep_elect_done(dbenv, rep, 0);
        change = rep->gen < cntrl->gen || rep->master_id != eid;
        if (change) {
#ifdef DIAGNOSTIC
            if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
                __db_err(dbenv, "Updating gen from %lu to %lu from master %d",
                         (u_long)rep->gen, (u_long)cntrl->gen, eid);
#endif
        __rep_set_gen(dbenv, __func__, __LINE__, cntrl->gen);
		if (rep->egen <= rep->gen)
			rep->egen = rep->gen + 1;
#ifdef DIAGNOSTIC
		if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
			__db_err(dbenv,
			    "Updating egen to %lu", (u_long)rep->egen);
#endif
		rep->master_id = eid;
		rep->stat.st_master_changes++;
		F_SET(rep, REP_F_NOARCHIVE | REP_F_RECOVER);
	}
	F_CLR(rep, REP_F_WAITSTART);
	pthread_mutex_unlock(&rep_candidate_lock);
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

	dblp = dbenv->lg_handle;
	lp = dblp->reginfo.primary;
	R_LOCK(dbenv, &dblp->reginfo);
	last_lsn = lsn = lp->lsn;
	if (last_lsn.offset > sizeof(LOGP))
		last_lsn.offset -= lp->len;
	R_UNLOCK(dbenv, &dblp->reginfo);

	if (!change) {
		/*
		 * If there wasn't a change, we might still have some
		 * catching up or verification to do.
		 */
		ret = 0;
		if (F_ISSET(rep, REP_F_RECOVER)) {
			MUTEX_LOCK(dbenv, db_rep->db_mutexp);
			lsn = lp->verify_lsn;
			MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);
			if (!IS_ZERO_LSN(lsn)) {
#if 0
				fprintf(stderr,
				    "%s:%d Requesting REP_VERIFY_REQ %d:%d\n",
				    __FILE__, __LINE__, last_lsn.file,
				    last_lsn.offset);
#endif
				(void)__rep_send_message(dbenv, eid,
				    REP_VERIFY_REQ, &last_lsn, NULL, 0, NULL);
			}
		} else {
            /* Let the apply-thread make this request */
			if (log_compare(&lsn, &cntrl->lsn) < 0 && !gbl_decoupled_logputs) {
				if (__rep_send_message(dbenv, eid, REP_ALL_REQ, &lsn, 
                            NULL, DB_REP_NODROP|DB_REP_NOBUFFER, NULL) == 0) {
                    if (gbl_verbose_fills) {
                        logmsg(LOGMSG_USER, "%s line %d sending REP_ALL_REQ "
                                "for %d:%d\n", __func__, __LINE__, lsn.file,
                                lsn.offset);
                    }
                    last_fill = comdb2_time_epochms();
                } else if (gbl_verbose_fills) {
                    logmsg(LOGMSG_USER, "%s line %d failed REP_ALL_REQ for "
                            "%d:%d\n", __func__, __LINE__, lsn.file, 
                            lsn.offset);
                }
            }
			MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
			F_CLR(rep, REP_F_NOARCHIVE);
			MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		}
		return (ret);
	}

	/*
	 * If the master changed, we need to start the process of
	 * figuring out what our last valid log record is.  However,
	 * if both the master and we agree that the max LSN is 0,0,
	 * then there is no recovery to be done.  If we are at 0 and
	 * the master is not, then we just need to request all the log
	 * records from the master.
	 */
	if (IS_INIT_LSN(lsn) || IS_ZERO_LSN(lsn)) {
empty:		MUTEX_LOCK(dbenv, db_rep->db_mutexp);
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		F_CLR(rep, REP_F_NOARCHIVE | REP_F_READY | REP_F_RECOVER);
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

		if (!IS_INIT_LSN(cntrl->lsn)) {
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
			if (__rep_send_message(dbenv, rep->master_id,
			    REP_ALL_REQ, &lsn, NULL, DB_REP_NODROP, NULL) == 0) {
                last_fill = comdb2_time_epochms();
            } 
		} else
			MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);

		return (DB_REP_NEWMASTER);
	} else if (last_lsn.offset <= sizeof(LOGP)) {
		/*
		 * We have just changed log files and need to set lastlsn
		 * to the last record in the previous log files.
		 */
		if ((ret = __log_cursor(dbenv, &logc)) != 0)
			return (ret);
		memset(&dbt, 0, sizeof(dbt));
		ret = __log_c_get(logc, &last_lsn, &dbt, DB_LAST);
		if ((t_ret = __log_c_close(logc)) != 0 && ret == 0)
			ret = t_ret;
		if (ret == DB_NOTFOUND)
			goto empty;
		if (ret != 0) {
			/*
			 * We failed here and if we set recover above,
			 * we'd better clear it, because we haven't
			 * set the verify LSN
			 */
			if (change) {
				MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
				F_CLR(rep, REP_F_RECOVER);
				MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
			}
			return (ret);
		}
	}

	MUTEX_LOCK(dbenv, db_rep->db_mutexp);
	lp->verify_lsn = last_lsn;
	lp->rcvd_recs = 0;
	lp->wait_recs = rep->request_gap;
	MUTEX_UNLOCK(dbenv, db_rep->db_mutexp);
	{
#if 0
		fprintf(stderr, "%s:%d Requesting REP_VERIFY_REQ %d:%d\n",
		    __FILE__, __LINE__, last_lsn.file, last_lsn.offset);
#endif
		/* mark the node not available */
		gbl_passed_repverify = 0;

		dbenv->newest_rep_verify_tran_time = 0;
		(void)__rep_send_message(dbenv,
		    eid, REP_VERIFY_REQ, &last_lsn, NULL, 0, NULL);
	}

	return (DB_REP_NEWMASTER);
}

/*
 * __rep_is_client
 *	Used by other subsystems to figure out if this is a replication
 * client site.
 *
 * PUBLIC: int __rep_is_client __P((DB_ENV *));
 */
int
__rep_is_client(dbenv)
	DB_ENV *dbenv;
{
	DB_REP *db_rep;
	REP *rep;
	int ret;

	if (!REP_ON(dbenv))
		return (0);
	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	ret = F_ISSET(rep, REP_F_UPGRADE | REP_F_LOGSONLY);
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
	return (ret);
}

/*
 * __rep_noarchive
 *	Used by log_archive to determine if it is okay to remove
 * log files.
 *
 * PUBLIC: int __rep_noarchive __P((DB_ENV *));
 */
int
__rep_noarchive(dbenv)
	DB_ENV *dbenv;
{
	DB_REP *db_rep;
	REP *rep;

	if (!REP_ON(dbenv))
		return (0);
	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	return (F_ISSET(rep, REP_F_NOARCHIVE));
}

/*
 * __rep_send_vote
 *	Send this site's vote for the election.
 *
 * PUBLIC: void __rep_send_vote __P((DB_ENV *, DB_LSN *, int, int, int,
 * PUBLIC:    u_int32_t, char *, u_int32_t));
 */
void
__rep_send_vote(dbenv, lsnp, nsites, pri, tiebreaker, egen, eid, vtype)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	char *eid;
	int nsites, pri, tiebreaker;
	u_int32_t egen, vtype;
{
	DBT vote_dbt;
	REP_VOTE_INFO vi;

	memset(&vi, 0, sizeof(vi));

	vi.egen = egen;
	vi.priority = pri;
	vi.nsites = nsites;
	vi.tiebreaker = tiebreaker;

	if (LOG_SWAPPED())
		__rep_vote_info_swap(&vi);

	memset(&vote_dbt, 0, sizeof(vote_dbt));
	vote_dbt.data = &vi;
	vote_dbt.size = sizeof(vi);

	(void)__rep_send_message(dbenv, eid, vtype, lsnp, &vote_dbt, 0, NULL);
}


/*
 * __rep_send_gen_vote
 *	Send this site's vote for the election.
 *
 * PUBLIC: void __rep_send_gen_vote __P((DB_ENV *, DB_LSN *, int, int, int,
 * PUBLIC:    u_int32_t, u_int32_t, char *, u_int32_t));
 */

void
__rep_send_gen_vote(dbenv, lsnp, nsites, pri, tiebreaker, egen, committed_gen,
    eid, vtype)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	char *eid;
	int nsites, pri, tiebreaker;
	u_int32_t egen, committed_gen, vtype;
{
	DBT vote_dbt;
	REP_GEN_VOTE_INFO vi;

	memset(&vi, 0, sizeof(vi));

	vi.egen = egen;
	vi.priority = pri;
	vi.nsites = nsites;
	vi.tiebreaker = tiebreaker;
	vi.last_write_gen = committed_gen;

	if (LOG_SWAPPED())
		__rep_gen_vote_info_swap(&vi);

	memset(&vote_dbt, 0, sizeof(vote_dbt));
	vote_dbt.data = &vi;
	vote_dbt.size = sizeof(vi);

	(void)__rep_send_message(dbenv, eid, vtype, lsnp, &vote_dbt, 0, NULL);
}

pthread_mutex_t gbl_rep_egen_lk = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t gbl_rep_egen_cd = PTHREAD_COND_INITIALIZER;

/*
 * __rep_elect_done
 *	Clear all election information for this site.  Assumes the
 *	caller hold rep_mutex.
 *
 * PUBLIC: void __rep_elect_done __P((DB_ENV *, REP *, int egen));
 */
void
__rep_elect_done(dbenv, rep, egen)
	DB_ENV *dbenv;
	REP *rep;
    int egen;
{
	int inelect;

#ifndef DIAGNOSTIC
	COMPQUIET(dbenv, NULL);
#endif

	inelect = IN_ELECTION_TALLY(rep);
	F_CLR(rep, REP_F_EPHASE1 | REP_F_EPHASE2 | REP_F_TALLY);
	rep->sites = 0;
	rep->votes = 0;
	if (inelect) {
        pthread_mutex_lock(&gbl_rep_egen_lk);
        if (egen)
            rep->egen = egen;
        else
            rep->egen++;
        pthread_cond_broadcast(&gbl_rep_egen_cd);
        pthread_mutex_unlock(&gbl_rep_egen_lk);
    }
#ifdef DIAGNOSTIC
	if (FLD_ISSET(dbenv->verbose, DB_VERB_REPLICATION))
		__db_err(dbenv, "Election done; egen %lu", (u_long)rep->egen);
#endif
}

/*
 * __rep_grow_sites --
 *	Called to allocate more space in the election tally information.
 * Called with the rep mutex held.  We need to call the region mutex, so
 * we need to make sure that we *never* acquire those mutexes in the
 * opposite order.
 *
 * PUBLIC: int __rep_grow_sites __P((DB_ENV *dbenv, int nsites));
 */
int
__rep_grow_sites(dbenv, nsites)
	DB_ENV *dbenv;
	int nsites;
{
	REGENV *renv;
	REGINFO *infop;
	REP *rep;
	int nalloc, ret, *tally;

	rep = ((DB_REP *)dbenv->rep_handle)->region;

	/*
	 * Allocate either twice the current allocation or nsites,
	 * whichever is more.
	 */

	nalloc = 2 * rep->asites;
	if (nalloc < nsites)
		nalloc = nsites;

	infop = dbenv->reginfo;
	renv = infop->primary;
	MUTEX_LOCK(dbenv, &renv->mutex);
	/*
	 * We allocate 2 tally regions, one for tallying VOTE1's and
	 * one for VOTE2's.  Always grow them in tandem, because if we
	 * get more VOTE1's we'll always expect more VOTE2's then too.
	 */
	if ((ret = __db_shalloc(infop->addr,
		    nalloc * sizeof(REP_VTALLY), sizeof(REP_VTALLY),
		    &tally)) == 0) {
		if (rep->tally_off != INVALID_ROFF)
			__db_shalloc_free(infop->addr,
			    R_ADDR(infop, rep->tally_off));
		rep->tally_off = R_OFFSET(infop, tally);
		if ((ret = __db_shalloc(infop->addr,
			    nalloc * sizeof(REP_VTALLY), sizeof(REP_VTALLY),
			    &tally)) == 0) {
			/* Success */
			if (rep->v2tally_off != INVALID_ROFF)
				__db_shalloc_free(infop->addr,
				    R_ADDR(infop, rep->v2tally_off));
			rep->v2tally_off = R_OFFSET(infop, tally);
			rep->asites = nalloc;
			rep->nsites = nsites;
		} else {
			/*
			 * We were unable to allocate both.  So, we must
			 * free the first one and reinitialize.  If
			 * v2tally_off is valid, it is from an old
			 * allocation and we are clearing it all out due
			 * to the error.
			 */
			if (rep->v2tally_off != INVALID_ROFF)
				__db_shalloc_free(infop->addr,
				    R_ADDR(infop, rep->v2tally_off));
			__db_shalloc_free(infop->addr,
			    R_ADDR(infop, rep->tally_off));
			rep->v2tally_off = rep->tally_off = INVALID_ROFF;
			rep->asites = 0;
			rep->nsites = 0;
		}
	}
	MUTEX_UNLOCK(dbenv, &renv->mutex);
	return (ret);
}

/*
 * __env_rep_enter --
 *
 *	Check if we are in the middle of replication initialization and/or
 * recovery, and if so, disallow operations.  If operations are allowed,
 * increment handle-counts, so that we do not start recovery while we
 * are operating in the library.
 *
 * PUBLIC: void __env_rep_enter __P((DB_ENV *));
 */
void
__env_rep_enter(dbenv)
	DB_ENV *dbenv;
{
}

/*
 * __env_rep_exit --
 *
 *	Decrement handle count upon routine exit.
 *
 * PUBLIC: void __env_rep_exit __P((DB_ENV *));
 */
void
__env_rep_exit(dbenv)
	DB_ENV *dbenv;
{
}

/*
 * __db_rep_enter --
 *	Called in replicated environments to keep track of in-use handles
 * and prevent any concurrent operation during recovery.  If checkgen is
 * non-zero, then we verify that the dbp has the same handle as the env.
 * If return_now is non-zero, we'll return DB_DEADLOCK immediately, else we'll
 * sleep before returning DB_DEADLOCK.
 *
 * PUBLIC: int __db_rep_enter __P((DB *, int, int));
 */
int
__db_rep_enter(dbp, checkgen, return_now)
	DB *dbp;
	int checkgen, return_now;
{
	return (0);
}

/*
 * __db_rep_exit --
 *	Decrement handle counts.
 *
 * PUBLIC: void __db_rep_exit __P((DB_ENV *));
 */
void
__db_rep_exit(dbenv)
	DB_ENV *dbenv;
{
}

/*
 * __op_rep_enter --
 *
 *	Check if we are in the middle of replication initialization and/or
 * recovery, and if so, disallow new multi-step operations, such as
 * transaction and memp gets.  If operations are allowed,
 * increment the op_cnt, so that we do not start recovery while we have
 * active operations.
 *
 * PUBLIC: void __op_rep_enter __P((DB_ENV *));
 */
void
__op_rep_enter(dbenv)
	DB_ENV *dbenv;
{
	DB_REP *db_rep;
	REP *rep;
	int cnt;

	/* Check if locks have been globally turned off. */
	if (F_ISSET(dbenv, DB_ENV_NOLOCKING))
		return;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	for (cnt = 0; F_ISSET(rep, REP_F_READY);) {
		MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
		(void)__os_sleep(dbenv, 5, 0);
		MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
		if (++cnt % 60 == 0)
			__db_err(dbenv,
	"__op_rep_enter waiting %d minutes for op count to drain",
			    cnt / 60);
	}
	rep->op_cnt++;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
}

/*
 * __op_rep_exit --
 *
 *	Decrement op count upon transaction commit/abort/discard or
 *  	memp_fput.
 *
 * PUBLIC: void __op_rep_exit __P((DB_ENV *));
 */
void
__op_rep_exit(dbenv)
	DB_ENV *dbenv;
{
	DB_REP *db_rep;
	REP *rep;

	/* Check if locks have been globally turned off. */
	if (F_ISSET(dbenv, DB_ENV_NOLOCKING))
		return;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	DB_ASSERT(rep->op_cnt > 0);
	rep->op_cnt--;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
}

/*
 * __rep_set_last_locked --
 *
 *	Get the last "locked" lsn
 *
 * PUBLIC: int __rep_set_last_locked __P((DB_ENV *, DB_LSN *));
 */
int 
__rep_set_last_locked(dbenv, last_locked_lsn)
    DB_ENV *dbenv;
    DB_LSN *last_locked_lsn;
{
    pthread_mutex_lock(&dbenv->locked_lsn_lk);
    if (last_locked_lsn->file <= 0) 
        abort();
    dbenv->last_locked_lsn = *last_locked_lsn;
    pthread_mutex_unlock(&dbenv->locked_lsn_lk);
    return 0;
}

/*
 * __rep_get_last_locked --
 *
 *	Get the last "locked" lsn
 *
 * PUBLIC: int __rep_get_last_locked __P((DB_ENV *, DB_LSN *));
 */
int
__rep_get_last_locked(dbenv, last_locked_lsn)
	DB_ENV *dbenv;
    DB_LSN *last_locked_lsn;
{
    pthread_mutex_lock(&dbenv->locked_lsn_lk);
    *last_locked_lsn = dbenv->last_locked_lsn;
    pthread_mutex_unlock(&dbenv->locked_lsn_lk);
    return 0;
}

/*
 * __rep_get_gen --
 *
 *	Get the generation number from a replicated environment.
 *
 * PUBLIC: void __rep_get_gen __P((DB_ENV *, u_int32_t *));
 */
void
__rep_get_gen(dbenv, genp)
	DB_ENV *dbenv;
	u_int32_t *genp;
{
	DB_REP *db_rep;
	REP *rep;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	if (rep->recover_gen > rep->gen)
		*genp = rep->recover_gen;
	else
		*genp = rep->gen;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
}

#ifdef NOTYET
static int __rep_send_file __P((DB_ENV *, DBT *, u_int32_t));

/*
 * __rep_send_file --
 *	Send an entire file, one block at a time.
 */
static int
__rep_send_file(dbenv, rec, eid)
	DB_ENV *dbenv;
	DBT *rec;
	u_int32_t eid;
{
	DB *dbp;
	DB_LOCK lk;
	DB_MPOOLFILE *mpf;
	DBC *dbc;
	DBT rec_dbt;
	PAGE *pagep;
	db_pgno_t last_pgno, pgno;
	int ret, t_ret;

	dbp = NULL;
	dbc = NULL;
	pagep = NULL;
	mpf = NULL;

	LOCK_INIT(lk);

	if ((ret = db_create(&dbp, dbenv, 0)) != 0)
		goto err;

	/* 
	 * TODO maybe - rec->data is a character string (nothing to endianize..)
	 */
	if ((ret =
		__db_open(dbp, rec->data, NULL, DB_UNKNOWN, 0, 0,
		    PGNO_BASE_MD)) != 0)
		 goto err;

	if ((ret = __db_cursor(dbp, NULL, &dbc, 0)) != 0)
		 goto err;

	/*
	 * Force last_pgno to some value that will let us read the meta-dat
	 * page in the following loop.
	 */
	memset(&rec_dbt, 0, sizeof(rec_dbt));
	last_pgno = 1;
	for (pgno = 0; pgno <= last_pgno; pgno++) {
		if ((ret = __db_lget(dbc, 0, pgno, DB_LOCK_READ, 0, &lk)) != 0)
			goto err;

		if ((ret = __memp_fget(mpf, &pgno, 0, &pagep)) != 0)
			goto err;

		if (pgno == 0)
			last_pgno = ((DBMETA *)pagep)->last_pgno;

		rec_dbt.data = pagep;
		rec_dbt.size = dbp->pgsize;
		if (__rep_send_message(dbenv, eid,
			REP_FILE, NULL, &rec_dbt, pgno == last_pgno) != 0)
			break;

		ret = __memp_fput(mpf, pagep, 0);
		pagep = NULL;

		if (ret != 0)
			goto err;
		ret = __LPUT(dbc, lk);
		LOCK_INIT(lk);
		if (ret != 0)
			goto err;
	}

err:	if (LOCK_ISSET(lk) && (t_ret = __LPUT(dbc, lk)) != 0 && ret == 0)
		ret = t_ret;
	if (dbc != NULL && (t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	if (pagep != NULL &&
	    (t_ret = __memp_fput(mpf, pagep, 0)) != 0 && ret == 0)
		ret = t_ret;
	if (dbp != NULL && (t_ret = __db_close(dbp, NULL, 0)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}
#endif

#ifdef DIAGNOSTIC
/*
 * PUBLIC: void __rep_print_message __P((DB_ENV *, char*, REP_CONTROL *, char *));
 */
void
__rep_print_message(dbenv, eid, rp, str)
	DB_ENV *dbenv;
	char *eid;
	REP_CONTROL *rp;
	char *str;
{
	char *type;
	switch (rp->rectype) {
	case REP_ALIVE:
		type = "alive";
		break;
	case REP_ALIVE_REQ:
		type = "alive_req";
		break;
	case REP_ALL_REQ:
		type = "all_req";
		break;
	case REP_DUPMASTER:
		type = "dupmaster";
		break;
	case REP_FILE:
		type = "file";
		break;
	case REP_FILE_REQ:
		type = "file_req";
		break;
	case REP_LOG:
		type = "log";
		break;
	case REP_LOG_MORE:
		type = "log_more";
		break;
	case REP_LOG_REQ:
		type = "log_req";
		break;
	case REP_MASTER_REQ:
		type = "master_req";
		break;
	case REP_NEWCLIENT:
		type = "newclient";
		break;
	case REP_NEWFILE:
		type = "newfile";
		break;
	case REP_NEWMASTER:
		type = "newmaster";
		break;
	case REP_NEWSITE:
		type = "newsite";
		break;
	case REP_PAGE:
		type = "page";
		break;
	case REP_PAGE_REQ:
		type = "page_req";
		break;
	case REP_PLIST:
		type = "plist";
		break;
	case REP_PLIST_REQ:
		type = "plist_req";
		break;
	case REP_VERIFY:
		type = "verify";
		break;
	case REP_VERIFY_FAIL:
		type = "verify_fail";
		break;
	case REP_VERIFY_REQ:
		type = "verify_req";
		break;
	case REP_VOTE1:
		type = "vote1";
		break;
	case REP_VOTE2:
		type = "vote2";
		break;
	case REP_GEN_VOTE1:
		type = "gen_vote1";
		break;
	case REP_GEN_VOTE2:
		type = "gen_vote2";
		break;
	default:
		type = "NOTYPE";
		break;
	}
	__db_err(dbenv,
	    "%s %s: gen = %lu eid %s, type %s (0x%x), LSN [%lu][%lu]",
	    dbenv->db_home, str, (u_long) rp->gen, eid, type, rp->rectype,
	    (u_long) rp->lsn.file, (u_long) rp->lsn.offset);
}
#endif
