/*
 * Deterministic election tally regression tests.
 *
 * The debug_rep_elect_test tunable runs a scenario at startup, while a
 * replicant with no reachable peers (a node configured in a cluster with
 * hosts that never come up) waits for a master.  The test plays the peers
 * itself by feeding forged VOTE1s to __rep_process_message.  It runs a real
 * __rep_elect on another thread and parks that thread at
 * REP_ELECT_TEST_VOTE1_SENT to control the interleaving.  After each step it
 * checks the election tally invariants.
 *
 * Scenarios:
 *
 * tally_full: peers fill the vote1 tally to its allocated size while we are
 * only tallying (REP_F_TALLY), then our own election tallies our vote.
 *
 * stale_egen: the sequence from a production crash.  Our election starts and
 * tallies its own vote at egen N.  A peer reporting more sites grows the
 * tally, a vote carrying a higher gen moves egen to N+1 without resetting
 * the election, and further votes complete phase 1 of N+1.  Then our
 * election thread wakes up and notices its egen changed.
 *
 * The hooks in __rep_elect are only active while a test runs.
 */

#include "db_config.h"

#include <sys/types.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "db_int.h"
#include "dbinc/db_swap.h"
#include "dbinc/log.h"
#include "logmsg.h"
#include "intern_strings.h"
#include <sys_wrap.h>

int gbl_rep_elect_test_hooks;	/* __rep_elect reports to us */

static pthread_mutex_t test_lk = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t test_cd = PTHREAD_COND_INITIALIZER;
static int park_armed;		/* park the election thread at VOTE1_SENT */
static int parked;
static int release_seq;
static int vote1_sent;		/* times the election thread sent a vote1 */
static int phase_overlap;	/* PHASE1 set while PHASE2 was set */
static int joined_phase2;
static int elect_done;

#define	TEST_ELECT_TIMEOUT	2000000	/* microseconds */
#define	TEST_WAIT_SECS		30
#define	MAX_TEST_PEERS		64

/*
 * __rep_elect_test_point --
 *	Called from __rep_elect while a test is running.
 *
 * PUBLIC: void __rep_elect_test_point __P((DB_ENV *, int));
 */
void
__rep_elect_test_point(dbenv, point)
	DB_ENV *dbenv;
	int point;
{
	REP *rep;
	int seq;

	rep = ((DB_REP *)dbenv->rep_handle)->region;

	Pthread_mutex_lock(&test_lk);
	switch (point) {
	case REP_ELECT_TEST_PHASE1_SET:
		/* Caller holds rep_mutexp: never block here. */
		if (F_ISSET(rep, REP_F_EPHASE2))
			phase_overlap++;
		break;
	case REP_ELECT_TEST_VOTE1_SENT:
		vote1_sent++;
		if (park_armed) {
			seq = release_seq;
			parked = 1;
			Pthread_cond_broadcast(&test_cd);
			while (park_armed && seq == release_seq)
				Pthread_cond_wait(&test_cd, &test_lk);
			parked = 0;
		}
		break;
	case REP_ELECT_TEST_JOIN_PHASE2:
		joined_phase2++;
		break;
	}
	Pthread_cond_broadcast(&test_cd);
	Pthread_mutex_unlock(&test_lk);
}

struct elect_args {
	DB_ENV *dbenv;
	int nsites;
	int priority;
	int ret;
};

static void *
elect_thd(arg)
	void *arg;
{
	struct elect_args *a;
	u_int32_t newgen;
	int already_master;
	char *master;

	a = arg;
	a->ret = a->dbenv->rep_elect(a->dbenv, a->nsites, a->priority,
	    TEST_ELECT_TIMEOUT, &newgen, &already_master, &master);

	Pthread_mutex_lock(&test_lk);
	elect_done = 1;
	Pthread_cond_broadcast(&test_cd);
	Pthread_mutex_unlock(&test_lk);
	return (NULL);
}

/*
 * Wait until the election thread returns, is parked (want_parked), or has
 * sent another vote1 or joined phase 2 since the given counts.
 */
static int
wait_elect(want_parked, v1, j2)
	int want_parked, v1, j2;
{
	struct timespec ts;
	int met, rc;

	clock_gettime(CLOCK_REALTIME, &ts);
	ts.tv_sec += TEST_WAIT_SECS;
	Pthread_mutex_lock(&test_lk);
	for (;;) {
		met = elect_done || (want_parked && parked) ||
		    (!want_parked && (vote1_sent > v1 || joined_phase2 > j2));
		if (met)
			break;
		rc = pthread_cond_timedwait(&test_cd, &test_lk, &ts);
		if (rc == ETIMEDOUT)
			break;
	}
	Pthread_mutex_unlock(&test_lk);
	return (met ? 0 : ETIMEDOUT);
}

static void
release_elect(keep_armed)
	int keep_armed;
{
	Pthread_mutex_lock(&test_lk);
	park_armed = keep_armed;
	release_seq++;
	Pthread_cond_broadcast(&test_cd);
	Pthread_mutex_unlock(&test_lk);
}

static int
known_voter(eid, eids, neids)
	char *eid, **eids;
	int neids;
{
	int i;

	for (i = 0; i < neids; i++)
		if (eids[i] == eid)
			return (1);
	return (0);
}

/* Every counted tally entry must be a distinct voter of this test. */
static int
check_tally(dbenv, name, off, count, eids, neids)
	DB_ENV *dbenv;
	const char *name;
	roff_t off;
	int count;
	char **eids;
	int neids;
{
	REP_VTALLY *tally;
	int bad, i, j;

	if (count == 0)
		return (0);
	if (off == INVALID_ROFF) {
		logmsg(LOGMSG_USER, "rep_elect_test: FAIL: %s count %d "
		    "without a tally\n", name, count);
		return (1);
	}
	tally = R_ADDR((REGINFO *)dbenv->reginfo, off);
	bad = 0;
	for (i = 0; i < count; i++) {
		if (!known_voter(tally[i].eid, eids, neids)) {
			logmsg(LOGMSG_USER, "rep_elect_test: FAIL: %s tally[%d] "
			    "holds an unknown voter %p\n", name, i,
			    (void *)tally[i].eid);
			bad = 1;
			continue;
		}
		for (j = 0; j < i; j++)
			if (tally[j].eid == tally[i].eid) {
				logmsg(LOGMSG_USER, "rep_elect_test: FAIL: %s "
				    "counts %s twice\n", name, tally[i].eid);
				bad = 1;
			}
	}
	return (bad);
}

static int
check_state(dbenv, when, eids, neids)
	DB_ENV *dbenv;
	const char *when;
	char **eids;
	int neids;
{
	DB_REP *db_rep;
	REP *rep;
	int bad;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;
	bad = 0;

	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	logmsg(LOGMSG_USER, "rep_elect_test: %s: gen %u egen %u sites %d "
	    "votes %d nsites %d asites %d flags 0x%x%s%s%s\n", when, rep->gen,
	    rep->egen, rep->sites, rep->votes, rep->nsites, rep->asites,
	    rep->flags, F_ISSET(rep, REP_F_EPHASE1) ? " PHASE1" : "",
	    F_ISSET(rep, REP_F_EPHASE2) ? " PHASE2" : "",
	    F_ISSET(rep, REP_F_TALLY) ? " TALLY" : "");
	if (rep->sites > rep->asites) {
		logmsg(LOGMSG_USER, "rep_elect_test: FAIL: vote1 tally "
		    "overflow: sites %d > asites %d\n", rep->sites, rep->asites);
		bad = 1;
	}
	if (rep->votes > rep->asites) {
		logmsg(LOGMSG_USER, "rep_elect_test: FAIL: vote2 tally "
		    "overflow: votes %d > asites %d\n", rep->votes, rep->asites);
		bad = 1;
	}
	if (F_ISSET(rep, REP_F_EPHASE1) && F_ISSET(rep, REP_F_EPHASE2)) {
		logmsg(LOGMSG_USER,
		    "rep_elect_test: FAIL: PHASE1 and PHASE2 both set\n");
		bad = 1;
	}
	bad |= check_tally(dbenv, "vote1", rep->tally_off,
	    rep->sites < rep->asites ? rep->sites : rep->asites, eids, neids);
	bad |= check_tally(dbenv, "vote2", rep->v2tally_off,
	    rep->votes < rep->asites ? rep->votes : rep->asites, eids, neids);
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
	return (bad);
}

/* Feed __rep_process_message a vote1 from a forged peer. */
static int
inject_vote1(dbenv, from, gen, egen, nsites)
	DB_ENV *dbenv;
	char *from;
	u_int32_t gen, egen;
	int nsites;
{
	DB_LOG *dblp;
	DBT control, rec;
	DB_LSN lsn, ret_lsn;
	REP_CONTROL rp;
	REP_VOTE_INFO vi;
	REP_GEN_VOTE_INFO vig;
	u_int32_t commit_gen, newgen;
	char *eid, *newmaster;
	int ret;

	dblp = dbenv->lg_handle;
	R_LOCK(dbenv, &dblp->reginfo);
	lsn = ((LOG *)dblp->reginfo.primary)->lsn;
	R_UNLOCK(dbenv, &dblp->reginfo);

	memset(&rp, 0, sizeof(rp));
	rp.rep_version = DB_REPVERSION;
	rp.log_version = DB_LOGVERSION;
	rp.lsn = lsn;
	rp.gen = gen;

	memset(&control, 0, sizeof(control));
	control.data = &rp;
	control.size = sizeof(rp);
	memset(&rec, 0, sizeof(rec));

	/* Lower priority than ours, so that we are the winner. */
	if (dbenv->attr.elect_highest_committed_gen) {
		memset(&vig, 0, sizeof(vig));
		vig.egen = egen;
		vig.nsites = nsites;
		vig.priority = 1;
		if (LOG_SWAPPED())
			__rep_gen_vote_info_swap(&vig);
		rp.rectype = REP_GEN_VOTE1;
		rec.data = &vig;
		rec.size = sizeof(vig);
	} else {
		memset(&vi, 0, sizeof(vi));
		vi.egen = egen;
		vi.nsites = nsites;
		vi.priority = 1;
		if (LOG_SWAPPED())
			__rep_vote_info_swap(&vi);
		rp.rectype = REP_VOTE1;
		rec.data = &vi;
		rec.size = sizeof(vi);
	}
	if (LOG_SWAPPED())
		__rep_control_swap(&rp);

	eid = from;
	ret = dbenv->rep_process_message(dbenv, &control, &rec, &eid, &ret_lsn,
	    &commit_gen, &newgen, &newmaster, 1);
	logmsg(LOGMSG_USER, "rep_elect_test: vote1 from %s gen %u egen %u "
	    "nsites %d returned %d\n", from, gen, egen, nsites, ret);
	return (ret);
}

static void
rep_state(dbenv, gen, egen, sites, asites)
	DB_ENV *dbenv;
	u_int32_t *gen, *egen;
	int *sites, *asites;
{
	DB_REP *db_rep;
	REP *rep;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;
	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	if (gen != NULL)
		*gen = rep->gen;
	if (egen != NULL)
		*egen = rep->egen;
	if (sites != NULL)
		*sites = rep->sites;
	if (asites != NULL)
		*asites = rep->asites;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
}

static int
precheck(dbenv)
	DB_ENV *dbenv;
{
	DB_REP *db_rep;
	REP *rep;
	int ok;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;
	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	ok = !F_ISSET(rep, REP_F_MASTER) && rep->master_id == db_eid_invalid &&
	    !IN_ELECTION_TALLY(rep) && rep->sites == 0 && rep->votes == 0;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);
	if (!ok)
		logmsg(LOGMSG_USER, "rep_elect_test: needs a replicant with no "
		    "master, no peers and no election in progress\n");
	return (ok ? 0 : EINVAL);
}

static void
make_peers(scenario, eids, npeers)
	const char *scenario;
	char **eids;
	int npeers;
{
	char name[64];
	int i;

	for (i = 1; i <= npeers; i++) {
		snprintf(name, sizeof(name), "rep-elect-test-%s-%d", scenario, i);
		eids[i] = intern(name);
	}
}

static int
scenario_tally_full(dbenv, eids, neidsp, ep)
	DB_ENV *dbenv;
	char **eids;
	int *neidsp;
	struct elect_args *ep;
{
	u_int32_t gen, egen;
	int asites, bad, i, n, sites;

	rep_state(dbenv, &gen, &egen, NULL, &asites);
	n = asites ? asites : 3;
	if (n + 1 > MAX_TEST_PEERS) {
		logmsg(LOGMSG_USER, "rep_elect_test: asites %d too large\n", n);
		return (EINVAL);
	}
	make_peers("tally_full", eids, n);
	*neidsp = n + 1;

	/* We aren't in an election yet: these are only tallied. */
	for (i = 1; i <= n; i++)
		inject_vote1(dbenv, eids[i], gen, egen, n);
	rep_state(dbenv, NULL, NULL, &sites, &asites);
	if (sites != n || asites != n) {
		logmsg(LOGMSG_USER, "rep_elect_test: setup failed: sites %d "
		    "asites %d, wanted %d\n", sites, asites, n);
		return (EINVAL);
	}
	bad = check_state(dbenv, "peers filled the tally", eids, *neidsp);

	park_armed = 1;
	ep->nsites = n;
	return (bad);
}

static int
scenario_stale_egen(dbenv, eids, neidsp, ep, phasep)
	DB_ENV *dbenv;
	char **eids;
	int *neidsp;
	struct elect_args *ep;
	int phasep;
{
	u_int32_t gen, egen, newgen, newegen;
	int asites, bad, grow, i;

	if (phasep == 0) {
		/* Start our election with as small a tally as possible. */
		rep_state(dbenv, NULL, NULL, NULL, &asites);
		park_armed = 1;
		ep->nsites = asites > 2 ? asites : 2;
		return (0);
	}

	/* Our election is parked after tallying and sending its vote1. */
	rep_state(dbenv, &gen, &egen, NULL, &asites);
	grow = 2 * asites;
	if (grow > MAX_TEST_PEERS) {
		logmsg(LOGMSG_USER, "rep_elect_test: asites %d too large\n",
		    asites);
		return (EINVAL);
	}
	make_peers("stale_egen", eids, grow - 1);
	*neidsp = grow;
	bad = check_state(dbenv, "our election sent its vote1", eids, *neidsp);

	/* A peer that knows more sites makes us grow the tally. */
	inject_vote1(dbenv, eids[1], gen, egen, grow);
	bad |= check_state(dbenv, "after growing the tally", eids, *neidsp);

	/* A peer with a higher gen moves egen without ending the election. */
	newgen = (gen > egen ? gen : egen) + 1;
	newegen = newgen + 1;
	inject_vote1(dbenv, eids[2], newgen, newegen, grow);
	rep_state(dbenv, NULL, &egen, NULL, NULL);
	if (egen != newegen) {
		logmsg(LOGMSG_USER, "rep_elect_test: setup failed: egen %u, "
		    "wanted %u\n", egen, newegen);
		return (EINVAL);
	}

	/* The remaining peers complete phase 1 of the new egen. */
	for (i = 3; i < grow; i++)
		inject_vote1(dbenv, eids[i], newgen, newegen, grow);
	bad |= check_state(dbenv, "new egen finished phase 1", eids, *neidsp);
	return (bad);
}

/*
 * __rep_elect_test --
 *	Run an election test scenario.  Returns 0 if it passed.
 *
 * PUBLIC: int __rep_elect_test __P((DB_ENV *, const char *));
 */
int
__rep_elect_test(dbenv, scenario)
	DB_ENV *dbenv;
	const char *scenario;
{
	DB_REP *db_rep;
	REP *rep;
	struct elect_args args;
	pthread_t tid;
	char *eids[MAX_TEST_PEERS];
	u_int32_t egen, newegen;
	int bad, neids, phase2, rc, stale, v1, j2;

	stale =strcmp(scenario, "stale_egen") == 0;
	if (!stale && strcmp(scenario, "tally_full") != 0) {
		logmsg(LOGMSG_USER, "rep_elect_test: unknown scenario '%s', "
		    "expected tally_full or stale_egen\n", scenario);
		return (EINVAL);
	}
	if (precheck(dbenv) != 0)
		return (EINVAL);

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;
	eids[0] = rep->eid;
	neids = 1;

	Pthread_mutex_lock(&test_lk);
	park_armed = parked = vote1_sent = phase_overlap = 0;
	joined_phase2 = elect_done = 0;
	Pthread_mutex_unlock(&test_lk);
	gbl_rep_elect_test_hooks = 1;

	memset(&args, 0, sizeof(args));
	args.dbenv = dbenv;
	args.priority = 100;
	rc = stale ? scenario_stale_egen(dbenv, eids, &neids, &args, 0) :
	    scenario_tally_full(dbenv, eids, &neids, &args);
	if (rc == EINVAL) {
		bad = 1;
		goto out;
	}
	bad = rc;

	Pthread_create(&tid, NULL, elect_thd, &args);
	if (wait_elect(1, 0, 0) != 0 || elect_done) {
		logmsg(LOGMSG_USER, "rep_elect_test: FAIL: election did not "
		    "reach its vote1 (ret %d)\n", args.ret);
		bad = 1;
		goto join;
	}
	if (!stale) {
		bad |= check_state(dbenv, "our election tallied its vote",
		    eids, neids);
		goto join;
	}

	rc = scenario_stale_egen(dbenv, eids, &neids, &args, 1);
	if (rc == EINVAL) {
		bad = 1;
		goto join;
	}
	bad |= rc;
	MUTEX_LOCK(dbenv, db_rep->rep_mutexp);
	phase2 = F_ISSET(rep, REP_F_EPHASE2) != 0;
	newegen = rep->egen;
	MUTEX_UNLOCK(dbenv, db_rep->rep_mutexp);

	/* Wake the stale election thread and see what it does. */
	Pthread_mutex_lock(&test_lk);
	v1 = vote1_sent;
	j2 = joined_phase2;
	Pthread_mutex_unlock(&test_lk);
	release_elect(1);
	if (wait_elect(0, v1, j2) != 0) {
		logmsg(LOGMSG_USER, "rep_elect_test: FAIL: stale election "
		    "thread did not react to the new egen\n");
		bad = 1;
		goto join;
	}
	bad |= check_state(dbenv, "stale election thread woke up", eids,
	    neids);
	Pthread_mutex_lock(&test_lk);
	if (phase2 && vote1_sent > v1) {
		logmsg(LOGMSG_USER, "rep_elect_test: FAIL: stale election "
		    "thread restarted phase 1 of egen %u after it reached "
		    "phase 2\n", newegen);
		bad = 1;
	}
	Pthread_mutex_unlock(&test_lk);
	rep_state(dbenv, NULL, &egen, NULL, NULL);
	if (egen < newegen) {
		logmsg(LOGMSG_USER, "rep_elect_test: FAIL: egen went back "
		    "from %u to %u\n", newegen, egen);
		bad = 1;
	}

join:	release_elect(0);
	if (wait_elect(0, INT_MAX, INT_MAX) != 0) {
		logmsg(LOGMSG_USER, "rep_elect_test: FAIL: election thread "
		    "did not return\n");
		bad = 1;
		pthread_detach(tid);
		goto out;
	}
	Pthread_join(tid, NULL);
	logmsg(LOGMSG_USER, "rep_elect_test: election returned %d\n",
	    args.ret);
	bad |= check_state(dbenv, "election returned", eids, neids);
	if (phase_overlap) {
		logmsg(LOGMSG_USER, "rep_elect_test: FAIL: election set PHASE1 "
		    "while PHASE2 was set\n");
		bad = 1;
	}

out:	gbl_rep_elect_test_hooks = 0;
	release_elect(0);
	logmsg(LOGMSG_USER, "rep_elect_test: %s %s\n", scenario,
	    bad ? "FAILED" : "PASSED");
	return (bad ? 1 : 0);
}
