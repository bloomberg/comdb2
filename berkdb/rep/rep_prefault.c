/*
 * Out-of-band page prefault for the replication apply path: parse a record the
 * apply loop has not reached yet and warm its pages on a thread pool.  All best
 * effort -- on any failure the apply loop just pays the fault itself.
 */

#include "db_config.h"
#include "dbinc/db_swap.h"

#ifndef NO_SYSTEM_INCLUDES
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <unistd.h>
#include <pthread.h>
#endif

#include <db.h>
#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/db_shash.h"
#include "dbinc/db_am.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"
#include "dbinc/rep.h"

#include "dbinc_auto/fileops_auto.h"
#include "dbinc_auto/qam_auto.h"

#include "thdpool.h"
#include "logmsg.h"
#include "comdb2_atomic.h"

/* Set while this thread is applying replication, so the buffer pool can
 * attribute its hits and misses to the apply path. */
__thread int berkdb_applying_rep = 0;

extern int db_is_exiting(void);

int gbl_rep_prefault = 1;
int gbl_rep_prefault_lookahead = 16;
/* Sizes the pool; the controller steers it too.  Kept in step with the pool
 * by __rep_prefault_sync_threads.  Threads linger 10s, so idle is free. */
int gbl_rep_prefault_threads = 32;

/* Adaptive control: one global pages-in-flight budget, split across the open
 * windows.  64 reproduces the serial and parallel optima from the sweep. */
int gbl_rep_prefault_adaptive = 1;
int gbl_rep_prefault_budget = 64;
int gbl_rep_prefault_adapt_trace = 0;

#define REP_PF_WINDOW_MIN	4	/* per-queue clamp when budget > 0 */
#define REP_PF_WINDOW_MAX	256
#define REP_PF_BUDGET_MIN	8
#define REP_PF_BUDGET_MAX	1024
#define REP_PF_THREADS_MIN	8
#define REP_PF_THREADS_MAX	64
#define REP_PF_PROBE_BUDGET	16	/* budget while probing a dead regime */
#define REP_PF_PROBE_EPOCHS	30	/* epochs between probes */
#define REP_PF_MIN_ACTIVITY	100	/* requests/epoch below which we hold */
#define REP_PF_VERIFY_MAX	1000000	/* cap on "verify N" */

/* The live budget the controller steers.  We adopt the tunable when it moves
 * and never write it back, so an operator's setting survives. */
static int rep_pf_budget = 64;
static int rep_pf_budget_seed = 64;

/* Last value we pushed at the pool, so an operator change to the tunable is
 * distinguishable from our own. */
static int rep_pf_threads_seed = 32;

/* Windows currently open across every applying transaction. */
static int32_t rep_pf_open_windows = 0;

/* Controller decision counters, dumped by the message trap. */
static int64_t rep_pf_adapt_grow = 0;
static int64_t rep_pf_adapt_shrink = 0;
static int64_t rep_pf_adapt_threads = 0;
static int64_t rep_pf_adapt_useless = 0;
static int64_t rep_pf_adapt_probes = 0;

/* Counters, dumped by the "repprefault" message trap.  Atomic: bumped from
 * every pool and apply thread, and the controller steers off them. */
int64_t gbl_rep_prefault_records = 0;	/* records parsed */
int64_t gbl_rep_prefault_pages = 0;	/* pages handed to the pool */
int64_t gbl_rep_prefault_dedup = 0;	/* pages skipped as duplicates */
int64_t gbl_rep_prefault_filtered = 0;	/* sentinel pgnos skipped */
int64_t gbl_rep_prefault_enq_fail = 0;	/* pool queue full */
int64_t gbl_rep_prefault_parse_fail = 0;/* record would not parse */
int64_t gbl_rep_prefault_stashed = 0;	/* records saved for the apply loop */

/* How long __dbreg_rem_dbentry waited on prefault pins.  It spins holding
 * dblp->mutexp, so this is time no dbreg entry can be added or removed. */
int64_t gbl_rep_prefault_dbreg_waits = 0;	/* removals that had to wait */
int64_t gbl_rep_prefault_dbreg_wait_us = 0;	/* total time waited */
int64_t gbl_rep_prefault_dbreg_wait_max_us = 0;	/* worst single wait */

/* Worker outcomes.  touched + the drops must account for every page the
 * pool accepted; a large drop count means the pages we asked for never got
 * warmed and the effectiveness counters are measuring almost nothing. */
int64_t gbl_rep_prefault_touched = 0;		/* fget actually warmed a page */
int64_t gbl_rep_prefault_no_mpopen = 0;		/* mpf never had open called */
int64_t gbl_rep_prefault_notfound = 0;		/* page is past end of file */
int64_t gbl_rep_prefault_fget_err = 0;		/* any other fget failure */
int gbl_rep_prefault_last_err = 0;		/* rc of the last such failure */
int64_t gbl_rep_prefault_drop_dbreg = 0;	/* dbreg id would not resolve */
int64_t gbl_rep_prefault_drop_ufid = 0;		/* ufid would not resolve */
int64_t gbl_rep_prefault_drop_nobtree = 0;	/* resolved, not a btree */
int64_t gbl_rep_prefault_ufid_why[UFID_PF_NREASON];

static struct thdpool *rep_prefault_thdpool = NULL;

/*
 * __rep_prefault_window --
 *	Per-transaction lookahead.  Static mode returns the tunable; adaptive
 *	splits the budget across the open windows plus the one about to open.
 *
 * PUBLIC: int __rep_prefault_window __P((void));
 */
int
__rep_prefault_window()
{
	int k, n;

	if (!gbl_rep_prefault)
		return (0);
	if (!gbl_rep_prefault_adaptive)
		return (gbl_rep_prefault_lookahead);
	if ((k = rep_pf_budget) <= 0)
		return (0);
	n = ATOMIC_LOAD32(rep_pf_open_windows);
	if (n > 0)
		k /= (n + 1);
	if (k < REP_PF_WINDOW_MIN)
		k = REP_PF_WINDOW_MIN;
	if (k > REP_PF_WINDOW_MAX)
		k = REP_PF_WINDOW_MAX;
	return (k);
}

/*
 * Window accounting, one open per sliding window (per recovery queue in the
 * parallel path, per transaction in the serial path).
 *
 * PUBLIC: void __rep_prefault_window_open __P((void));
 * PUBLIC: void __rep_prefault_window_close __P((void));
 */
void
__rep_prefault_window_open()
{
	ATOMIC_ADD32(rep_pf_open_windows, 1);
}

void
__rep_prefault_window_close()
{
	ATOMIC_ADD32(rep_pf_open_windows, -1);
}

/*
 * __rep_prefault_dbreg_wait --
 *	Record that __dbreg_rem_dbentry blocked for usecs waiting on prefault
 *	pins.  Reported by the message trap; see the counters above.
 *
 * PUBLIC: void __rep_prefault_dbreg_wait __P((int64_t));
 */
void
__rep_prefault_dbreg_wait(usecs)
	int64_t usecs;
{
	int64_t cur;

	if (usecs <= 0)
		return;
	ATOMIC_ADD64(gbl_rep_prefault_dbreg_waits, 1);
	ATOMIC_ADD64(gbl_rep_prefault_dbreg_wait_us, usecs);
	/* Racy max: two removals finishing together can lose one update, which
	 * only ever understates the peak.  Not worth a lock on this path. */
	cur = ATOMIC_LOAD64(gbl_rep_prefault_dbreg_wait_max_us);
	if (usecs > cur)
		gbl_rep_prefault_dbreg_wait_max_us = usecs;
}

/*
 * __rep_prefault_sync_threads --
 *	Reconcile the three writers of the pool's thread count: the tunable,
 *	the controller, and "repprefault maxt N".  Returns the live value.
 */
static int
__rep_prefault_sync_threads()
{
	int live;

	if (rep_prefault_thdpool == NULL)
		return (gbl_rep_prefault_threads);

	live = thdpool_get_maxthds(rep_prefault_thdpool);

	/* Operator moved our tunable: that wins over whatever the pool says. */
	if (gbl_rep_prefault_threads != rep_pf_threads_seed) {
		live = gbl_rep_prefault_threads;
		if (live < 1)
			live = 1;
		if (live > REP_PF_THREADS_MAX)
			live = REP_PF_THREADS_MAX;
		thdpool_set_maxthds(rep_prefault_thdpool, live);
	}

	rep_pf_threads_seed = gbl_rep_prefault_threads = live;
	return (live);
}

/*
 * __rep_prefault_set_threads --
 *	Resize the pool from the controller, keeping the tunable in step.
 */
static void
__rep_prefault_set_threads(n)
	int n;
{
	if (n < REP_PF_THREADS_MIN)
		n = REP_PF_THREADS_MIN;
	if (n > REP_PF_THREADS_MAX)
		n = REP_PF_THREADS_MAX;
	thdpool_set_maxthds(rep_prefault_thdpool, n);
	rep_pf_threads_seed = gbl_rep_prefault_threads = n;
}

/*
 * __rep_prefault_adapt_thd --
 *	One-second control epochs over counter deltas: hold when idle, adopt
 *	tunable changes, probe, size the pool, then steer the budget.
 */
static void *
__rep_prefault_adapt_thd(arg)
	void *arg;
{
	int64_t l_ready = 0, l_infl = 0, l_evict = 0, l_late = 0, l_touch = 0;
	int64_t d_ready, d_infl, d_evict, d_late, d_touch, used;
	int probe_countdown = REP_PF_PROBE_EPOCHS;
	int budget, step, depth, threads;

	COMPQUIET(arg, NULL);

	for (;;) {
		sleep(1);

		if (db_is_exiting())
			break;

		/* Unconditional: this is how rep_prefault_threads reaches the
		 * pool at all, and it has to work with adaptive control off. */
		threads = __rep_prefault_sync_threads();

		if (!gbl_rep_prefault || !gbl_rep_prefault_adaptive ||
		    rep_prefault_thdpool == NULL) {
			/* Stay current so re-enabling starts from clean deltas. */
			l_ready = ATOMIC_LOAD64(gbl_rep_pf_ready_ct);
			l_infl = ATOMIC_LOAD64(gbl_rep_pf_inflight_ct);
			l_evict = ATOMIC_LOAD64(gbl_rep_pf_evict_unused_ct);
			l_late = ATOMIC_LOAD64(gbl_rep_pf_late_ct);
			l_touch = ATOMIC_LOAD64(gbl_rep_prefault_touched);
			continue;
		}

		d_ready = ATOMIC_LOAD64(gbl_rep_pf_ready_ct) - l_ready;
		d_infl = ATOMIC_LOAD64(gbl_rep_pf_inflight_ct) - l_infl;
		d_evict = ATOMIC_LOAD64(gbl_rep_pf_evict_unused_ct) - l_evict;
		d_late = ATOMIC_LOAD64(gbl_rep_pf_late_ct) - l_late;
		d_touch = ATOMIC_LOAD64(gbl_rep_prefault_touched) - l_touch;
		l_ready += d_ready;
		l_infl += d_infl;
		l_evict += d_evict;
		l_late += d_late;
		l_touch += d_touch;

		/* Adopt an operator change to the budget tunable, including one
		 * made while we had steered ourselves to zero. */
		if (gbl_rep_prefault_budget != rep_pf_budget_seed) {
			rep_pf_budget_seed = gbl_rep_prefault_budget;
			rep_pf_budget = gbl_rep_prefault_budget;
			probe_countdown = REP_PF_PROBE_EPOCHS;
			if (gbl_rep_prefault_adapt_trace)
				logmsg(LOGMSG_USER,
				    "rep_prefault adapt: adopting budget %d "
				    "from tunable\n", rep_pf_budget);
		}

		budget = rep_pf_budget;

		/* Turned off by the useless rule: hold, and periodically
		 * re-open a small window to see if the workload changed. */
		if (budget <= 0) {
			if (--probe_countdown <= 0) {
				rep_pf_budget = REP_PF_PROBE_BUDGET;
				probe_countdown = REP_PF_PROBE_EPOCHS;
				rep_pf_adapt_probes++;
				if (gbl_rep_prefault_adapt_trace)
					logmsg(LOGMSG_USER,
					    "rep_prefault adapt: probe\n");
			}
			continue;
		}

		/* Not enough traffic to read anything from the deltas. */
		if (d_touch < REP_PF_MIN_ACTIVITY)
			continue;

		/* Requests execute but almost never help: cache-resident or
		 * insert-heavy workload.  Stop paying for the parses. */
		used = d_ready + d_infl;
		if (used * 20 < d_touch) {
			rep_pf_budget = 0;
			probe_countdown = REP_PF_PROBE_EPOCHS;
			rep_pf_adapt_useless++;
			if (gbl_rep_prefault_adapt_trace)
				logmsg(LOGMSG_USER,
				    "rep_prefault adapt: off (used %" PRId64
				    " of %" PRId64 ")\n", used, d_touch);
			continue;
		}

		/* Stale requests: the pool cannot drain what the windows
		 * enqueue, so pages get applied before their prefault runs.
		 * More window would only deepen the queue; add drain. */
		depth = thdpool_get_queue_depth(rep_prefault_thdpool);
		if (d_late * 4 > d_touch && depth > 2 * threads &&
		    threads < REP_PF_THREADS_MAX) {
			__rep_prefault_set_threads(threads + 8);
			rep_pf_adapt_threads++;
			if (gbl_rep_prefault_adapt_trace)
				logmsg(LOGMSG_USER,
				    "rep_prefault adapt: threads -> %d\n",
				    gbl_rep_prefault_threads);
			continue;
		}

		/* Give them back once the queue drains, so the next burst does
		 * not start with the whole fleet before we know it needs it. */
		if (depth == 0 && d_late * 20 < d_touch &&
		    threads > REP_PF_THREADS_MIN) {
			__rep_prefault_set_threads(threads - 8);
			rep_pf_adapt_threads++;
			if (gbl_rep_prefault_adapt_trace)
				logmsg(LOGMSG_USER,
				    "rep_prefault adapt: threads -> %d\n",
				    gbl_rep_prefault_threads);
			continue;
		}

		/* Steer toward evict-unused at a quarter to a half of inflight,
		 * not parity: an unused evict wasted a read and still costs a
		 * full miss later, while an inflight hit only waited out the
		 * tail of one.  The measured optimum sits at ~0.35.  Steps
		 * shrink with the budget or the controller orbits the target
		 * instead of settling. */
		step = budget / 8;
		if (step < 2)
			step = 2;
		if (d_infl > 4 * d_evict + 50) {
			budget += step;
			if (budget > REP_PF_BUDGET_MAX)
				budget = REP_PF_BUDGET_MAX;
			if (budget != rep_pf_budget) {
				rep_pf_budget = budget;
				rep_pf_adapt_grow++;
				if (gbl_rep_prefault_adapt_trace)
					logmsg(LOGMSG_USER,
					    "rep_prefault adapt: budget -> %d\n",
					    budget);
			}
		} else if (2 * d_evict > d_infl + 50) {
			budget -= step;
			if (budget < REP_PF_BUDGET_MIN)
				budget = REP_PF_BUDGET_MIN;
			if (budget != rep_pf_budget) {
				rep_pf_budget = budget;
				rep_pf_adapt_shrink++;
				if (gbl_rep_prefault_adapt_trace)
					logmsg(LOGMSG_USER,
					    "rep_prefault adapt: budget -> %d\n",
					    budget);
			}
		}
	}
	return (NULL);
}

struct rep_pf_work {
	DB_ENV *dbenv;
	int32_t fid;			/* dbreg id, or -1 when ufid-logged */
	u_int8_t ufid[DB_FILE_ID_LEN];	/* valid when fid < 0 */
	db_pgno_t pgno;
};

/*
 * __rep_prefault_do_work --
 *	Pool worker: resolve the file and warm one page.  Resolving here rather
 *	than on the apply thread keeps a hash lookup off that thread.  Both
 *	paths pin the dbreg entry, which __dbreg_rem_dbentry waits on.
 */
static void
__rep_prefault_do_work(pool, work, thddata, op)
	struct thdpool *pool;
	void *work;
	void *thddata;
	int op;
{
	struct rep_pf_work *w;
	DB_ENV *dbenv;
	DB *dbp;
	int32_t ndx;

	w = (struct rep_pf_work *)work;

	/* THD_FREE is the pool draining us; exiting means dbenv may be gone.
	 * free() below is berkdb's, so needs no dbenv -- unlike __os_free. */
	if (op != THD_RUN || db_is_exiting())
		goto done;

	dbenv = w->dbenv;
	dbp = NULL;
	ndx = -1;

	if (w->fid >= 0) {
		if (__dbreg_id_to_db_prefault(dbenv, NULL, &dbp, w->fid, 1) != 0) {
			ATOMIC_ADD64(gbl_rep_prefault_drop_dbreg, 1);
			goto done;
		}
		ndx = w->fid;
	} else {
		int why = UFID_PF_OK;

		if (__ufid_find_db_prefault(dbenv, &dbp, w->ufid, &ndx,
		    &why) != 0) {
			ATOMIC_ADD64(gbl_rep_prefault_drop_ufid, 1);
			if (why >= 0 && why < UFID_PF_NREASON)
				ATOMIC_ADD64(gbl_rep_prefault_ufid_why[why], 1);
			goto done;
		}
	}

	/* Only btree: queue/hash pgno fields don't map to pool pages the same
	 * way, and the workload we care about is btree. */
	if (dbp == NULL || dbp->type != DB_BTREE || dbp->mpf == NULL) {
		ATOMIC_ADD64(gbl_rep_prefault_drop_nobtree, 1);
		goto complete;
	}

	/*
	 * Inline rather than touch_page() so the return code is visible:
	 * __memp_fget rejects a PFGET on an unopened handle before it reaches
	 * any statistic, so a failure here is otherwise completely silent.
	 */
	if (!F_ISSET(dbp->mpf, MP_OPEN_CALLED)) {
		ATOMIC_ADD64(gbl_rep_prefault_no_mpopen, 1);
		goto complete;
	}
	{
		PAGE *pagep = NULL;
		int rc = __memp_fget(dbp->mpf, &w->pgno, DB_MPOOL_PFGET, &pagep);

		if (rc == 0) {
			ATOMIC_ADD64(gbl_rep_prefault_touched, 1);
			(void)__memp_fput(dbp->mpf, pagep, DB_MPOOL_PFPUT);
		} else if (rc == DB_PAGE_NOTFOUND) {
			ATOMIC_ADD64(gbl_rep_prefault_notfound, 1);
		} else {
			ATOMIC_ADD64(gbl_rep_prefault_fget_err, 1);
			gbl_rep_prefault_last_err = rc;
		}
	}

complete:

	if (ndx >= 0)
		__dbreg_prefault_complete(dbenv, ndx);

done:
	free(w);
}

/*
 * __rep_prefault_init --
 *	Create the prefault pool.  Called from __env_open under DB_INIT_REP.
 *
 * PUBLIC: void __rep_prefault_init __P((DB_ENV *));
 */
void
__rep_prefault_init(dbenv)
	DB_ENV *dbenv;
{
	COMPQUIET(dbenv, NULL);

	if (rep_prefault_thdpool != NULL)
		return;

	/* Own pool: gbl_udppfault_thdpool is already shared with bt_pf.c and
	 * capped at 8.  maxqueue is the backpressure -- when it fills we drop
	 * the prefault rather than make the apply thread wait. */
	rep_prefault_thdpool = thdpool_create("repprefaultpool", 0);
	if (rep_prefault_thdpool == NULL) {
		logmsg(LOGMSG_ERROR,
		    "%s: failed to create prefault pool, disabling\n", __func__);
		gbl_rep_prefault = 0;
		return;
	}
	if (gbl_rep_prefault_threads < 1)
		gbl_rep_prefault_threads = 1;
	if (gbl_rep_prefault_threads > REP_PF_THREADS_MAX)
		gbl_rep_prefault_threads = REP_PF_THREADS_MAX;
	rep_pf_threads_seed = gbl_rep_prefault_threads;

	/* Seed the live budget from the tunable, so an lrl setting is what the
	 * controller starts steering from. */
	rep_pf_budget = rep_pf_budget_seed = gbl_rep_prefault_budget;

	thdpool_set_minthds(rep_prefault_thdpool, 0);
	thdpool_set_maxthds(rep_prefault_thdpool, gbl_rep_prefault_threads);
	thdpool_set_maxqueue(rep_prefault_thdpool, 1000);
	thdpool_set_maxqueueoverride(rep_prefault_thdpool, 0);
	thdpool_set_linger(rep_prefault_thdpool, 10);
	thdpool_set_longwaitms(rep_prefault_thdpool, 10000);
	thdpool_set_wait(rep_prefault_thdpool, 0);

	{
		pthread_t tid;

		if (pthread_create(&tid, NULL, __rep_prefault_adapt_thd,
		    NULL) != 0) {
			logmsg(LOGMSG_ERROR,
			    "%s: no adapt thread, adaptive control off\n",
			    __func__);
			gbl_rep_prefault_adaptive = 0;
		} else
			pthread_detach(tid);
	}
}

/*
 * __rep_prefault_process_message --
 *	"repprefault" message trap: pool controls plus our counters.
 *
 * PUBLIC: void __rep_prefault_process_message __P((char *, int, int));
 */
void
__rep_prefault_process_message(line, lline, st)
	char *line;
	int lline;
	int st;
{
	if (rep_prefault_thdpool == NULL) {
		logmsg(LOGMSG_USER, "rep prefault pool is not running\n");
		return;
	}

	logmsg(LOGMSG_USER, "rep prefault: %s, lookahead %d\n",
	    gbl_rep_prefault ? "enabled" : "disabled",
	    gbl_rep_prefault_lookahead);
	logmsg(LOGMSG_USER,
	    "adaptive: %s, budget %d (tunable %d), windows %d, threads %d\n",
	    gbl_rep_prefault_adaptive ? "on" : "off", rep_pf_budget,
	    gbl_rep_prefault_budget, ATOMIC_LOAD32(rep_pf_open_windows),
	    thdpool_get_maxthds(rep_prefault_thdpool));
	logmsg(LOGMSG_USER,
	    "  adapt grow/shrink/threads/off/probe: %" PRId64 "/%" PRId64
	    "/%" PRId64 "/%" PRId64 "/%" PRId64 "\n",
	    rep_pf_adapt_grow, rep_pf_adapt_shrink, rep_pf_adapt_threads,
	    rep_pf_adapt_useless, rep_pf_adapt_probes);
	logmsg(LOGMSG_USER, "  records parsed   : %" PRId64 "\n",
	    gbl_rep_prefault_records);
	logmsg(LOGMSG_USER, "  records stashed  : %" PRId64 "\n",
	    gbl_rep_prefault_stashed);
	logmsg(LOGMSG_USER, "  pages enqueued   : %" PRId64 "\n",
	    gbl_rep_prefault_pages);
	logmsg(LOGMSG_USER, "  dedup skips      : %" PRId64 "\n",
	    gbl_rep_prefault_dedup);
	logmsg(LOGMSG_USER, "  sentinel skips   : %" PRId64 "\n",
	    gbl_rep_prefault_filtered);
	logmsg(LOGMSG_USER, "  enqueue failures : %" PRId64 "\n",
	    gbl_rep_prefault_enq_fail);
	logmsg(LOGMSG_USER, "  parse failures   : %" PRId64 "\n",
	    gbl_rep_prefault_parse_fail);
	logmsg(LOGMSG_USER, "  pages touched    : %" PRId64 "\n",
	    gbl_rep_prefault_touched);
	logmsg(LOGMSG_USER, "  drop no dbreg    : %" PRId64 "\n",
	    gbl_rep_prefault_drop_dbreg);
	logmsg(LOGMSG_USER, "  drop no ufid     : %" PRId64 "\n",
	    gbl_rep_prefault_drop_ufid);
	logmsg(LOGMSG_USER, "  drop not btree   : %" PRId64 "\n",
	    gbl_rep_prefault_drop_nobtree);
	logmsg(LOGMSG_USER, "  drop no mp_open  : %" PRId64 "\n",
	    gbl_rep_prefault_no_mpopen);
	logmsg(LOGMSG_USER, "  drop page absent : %" PRId64 "\n",
	    gbl_rep_prefault_notfound);
	logmsg(LOGMSG_USER, "  drop fget error  : %" PRId64 " (last rc %d)\n",
	    gbl_rep_prefault_fget_err, gbl_rep_prefault_last_err);
	logmsg(LOGMSG_USER, "    ufid missing   : %" PRId64 "\n",
	    gbl_rep_prefault_ufid_why[UFID_PF_NO_UFID]);
	logmsg(LOGMSG_USER, "    ufid no dbp    : %" PRId64 "\n",
	    gbl_rep_prefault_ufid_why[UFID_PF_NO_DBP]);
	logmsg(LOGMSG_USER, "    ufid no fname  : %" PRId64 "\n",
	    gbl_rep_prefault_ufid_why[UFID_PF_NO_FNAME]);
	logmsg(LOGMSG_USER, "    ufid bad ndx   : %" PRId64 "\n",
	    gbl_rep_prefault_ufid_why[UFID_PF_BAD_NDX]);
	logmsg(LOGMSG_USER, "    ufid ndx mismat: %" PRId64 "\n",
	    gbl_rep_prefault_ufid_why[UFID_PF_NDX_MISMATCH]);
	logmsg(LOGMSG_USER,
	    "  dbreg rem waits  : %" PRId64 " (total %" PRId64 " us, max %"
	    PRId64 " us)\n", gbl_rep_prefault_dbreg_waits,
	    gbl_rep_prefault_dbreg_wait_us, gbl_rep_prefault_dbreg_wait_max_us);

	/* "repprefault maxt N" lands here; pick the change up rather than let
	 * the controller overwrite it from a stale tunable next epoch. */
	thdpool_process_message(rep_prefault_thdpool, line, lline, st);
	rep_pf_threads_seed = gbl_rep_prefault_threads =
	    thdpool_get_maxthds(rep_prefault_thdpool);
}

/*
 * __rep_prefault_ctx_init --
 *	Prepare per-transaction scratch state.
 *
 * PUBLIC: void __rep_prefault_ctx_init __P((REP_PREFAULT_CTX *));
 */
void
__rep_prefault_ctx_init(ctx)
	REP_PREFAULT_CTX *ctx;
{
	memset(ctx, 0, sizeof(*ctx));
}

/*
 * __rep_prefault_ctx_destroy --
 *	Release per-transaction scratch state.
 *
 * PUBLIC: void __rep_prefault_ctx_destroy __P((DB_ENV *, REP_PREFAULT_CTX *));
 */
void
__rep_prefault_ctx_destroy(dbenv, ctx)
	DB_ENV *dbenv;
	REP_PREFAULT_CTX *ctx;
{
	if (ctx->recs.array != NULL) {
		__os_free(dbenv, ctx->recs.array);
		ctx->recs.array = NULL;
		ctx->recs.nalloc = 0;
		ctx->recs.npages = 0;
	}
}

/*
 * __rep_prefault_seen --
 *	Approximate "already asked for this page" test; records either way.
 *	A collision costs a missed prefault, never a wrong one.
 */
static int
__rep_prefault_seen(ctx, fid, ufid, pgno)
	REP_PREFAULT_CTX *ctx;
	int32_t fid;
	u_int8_t *ufid;
	db_pgno_t pgno;
{
	u_int32_t h, idx, slot;
	int i;

	/* FNV-1a over the file identity, then mixed with the page number. */
	h = 2166136261u;
	if (fid >= 0) {
		u_int32_t f = (u_int32_t)fid;
		for (i = 0; i < 4; i++) {
			h ^= (f >> (i * 8)) & 0xff;
			h *= 16777619u;
		}
	} else {
		for (i = 0; i < DB_FILE_ID_LEN; i++) {
			h ^= ufid[i];
			h *= 16777619u;
		}
	}
	for (i = 0; i < 4; i++) {
		h ^= (pgno >> (i * 8)) & 0xff;
		h *= 16777619u;
	}
	if (h == 0)			/* 0 marks an empty slot */
		h = 1;

	idx = h & (REP_PREFAULT_DEDUP_SLOTS - 1);
	for (i = 0; i < 4; i++) {
		slot = (idx + i) & (REP_PREFAULT_DEDUP_SLOTS - 1);
		if (ctx->dedup[slot] == h)
			return (1);
		if (ctx->dedup[slot] == 0) {
			ctx->dedup[slot] = h;
			return (0);
		}
	}
	/* All probes occupied -- evict the first and take its place. */
	ctx->dedup[idx] = h;
	return (0);
}

/*
 * __rep_prefault_rectype --
 *	Base record type for a raw log record.  normalize_rectype only strips
 *	the utxnid bias; ufid-logged records -- the default -- also carry +1000,
 *	which __db_dispatch strips internally but callers must strip themselves.
 */
static u_int32_t
__rep_prefault_rectype(rec)
	DBT *rec;
{
	u_int32_t type;

	LOGCOPY_32(&type, rec->data);
	(void)normalize_rectype(&type);
	if (type > 1000 && type < 10000)
		type -= 1000;
	return (type);
}

/*
 * __rep_prefault_skip --
 *	GETALLPGNOS emits one entry per db_pgno_t field, and some of those hold
 *	a sentinel rather than a page.  Same exceptions __rep_check_applied_lsns
 *	makes.
 */
static int
__rep_prefault_skip(type, pgno, comment)
	u_int32_t type;
	db_pgno_t pgno;
	char *comment;
{
	if (pgno == PGNO_INVALID)
		return (1);

	if (comment == NULL)
		return (0);

	/* A non-root split logs root_pgno 0; a split of the last page logs
	 * npgno 0.  Neither is really about page 0. */
	if (type == DB___bam_split && pgno == 0 &&
	    (strcmp(comment, "root_pgno") == 0 || strcmp(comment, "npgno") == 0))
		return (1);

	/* Page alloc/free log a next page that recovery does not modify. */
	if ((type == DB___db_pg_alloc || type == DB___db_pg_free ||
	    type == DB___db_pg_freedata) && strcmp(comment, "next") == 0)
		return (1);

	/* First/last page of an overflow chain logs a 0 neighbour. */
	if (type == DB___db_big && pgno == 0 &&
	    (strcmp(comment, "next_pgno") == 0 ||
	    strcmp(comment, "prev_pgno") == 0))
		return (1);

	return (0);
}

/*
 * __rep_prefault_record --
 *	Parse one log record and hand its pages to the prefault pool.
 *
 *	Must not modify rec: callers pass buffers that are applied afterwards.
 *	That is why getallpgnos parses with do_pgswp 0 -- do_pgswp 1 byte-swaps
 *	page images in place, and apply would then swap them a second time.
 *
 * PUBLIC: void __rep_prefault_record __P((DB_ENV *, DBT *, DB_LSN *,
 * PUBLIC:     REP_PREFAULT_CTX *));
 */
void
__rep_prefault_record(dbenv, rec, lsnp, ctx)
	DB_ENV *dbenv;
	DBT *rec;
	DB_LSN *lsnp;
	REP_PREFAULT_CTX *ctx;
{
	TXN_RECS *t;
	struct rep_pf_work *w;
	u_int32_t type;
	int i;

	if (!gbl_rep_prefault || rep_prefault_thdpool == NULL)
		return;
	if (rec == NULL || rec->data == NULL || rec->size < sizeof(u_int32_t))
		return;
	if (dbenv->pgnos_dtab == NULL || dbenv->pgnos_dtab_size == 0)
		return;

	type = __rep_prefault_rectype(rec);

	t = &ctx->recs;
	t->npages = 0;

	if (__db_dispatch(dbenv, dbenv->pgnos_dtab, dbenv->pgnos_dtab_size,
	    rec, lsnp, DB_TXN_GETALLPGNOS, t) != 0) {
		/* Nothing to do but skip the record.  The dispatch may have
		 * partially filled the array, so reset it. */
		t->npages = 0;
		ATOMIC_ADD64(gbl_rep_prefault_parse_fail, 1);
		return;
	}

	ATOMIC_ADD64(gbl_rep_prefault_records, 1);

	for (i = 0; i < t->npages; i++) {
		LSN_PAGE *lp = &t->array[i];
		db_pgno_t pgno = lp->pgdesc.pgno;

		if (__rep_prefault_skip(type, pgno, lp->comment)) {
			ATOMIC_ADD64(gbl_rep_prefault_filtered, 1);
			continue;
		}

		/* Under ufid logging (the default) fid is -1 and the generator
		 * puts the ufid in pgdesc.fileid.  All-zero means the record
		 * carried no usable file identity. */
		if (lp->fid < 0) {
			static const u_int8_t zero_ufid[DB_FILE_ID_LEN] = { 0 };

			if (memcmp(lp->pgdesc.fileid, zero_ufid,
			    DB_FILE_ID_LEN) == 0)
				continue;
		}

		if (__rep_prefault_seen(ctx, lp->fid, lp->pgdesc.fileid, pgno)) {
			ATOMIC_ADD64(gbl_rep_prefault_dedup, 1);
			continue;
		}

		/* Plain malloc/free, paired with the free in the worker; see
		 * the note there for why this one does not take a dbenv. */
		if ((w = malloc(sizeof(*w))) == NULL)
			continue;
		w->dbenv = dbenv;
		w->fid = lp->fid;
		w->pgno = pgno;
		memcpy(w->ufid, lp->pgdesc.fileid, DB_FILE_ID_LEN);

		if (thdpool_enqueue(rep_prefault_thdpool,
		    __rep_prefault_do_work, w, 0, NULL, 0) != 0) {
			ATOMIC_ADD64(gbl_rep_prefault_enq_fail, 1);
			free(w);
			continue;
		}
		ATOMIC_ADD64(gbl_rep_prefault_pages, 1);
	}

	t->npages = 0;
}

/*
 * __rep_prefault_verify --
 *	Re-run the apply-path extraction over the tail of the log and report
 *	what it found.  Lets a node that is not applying replication check that
 *	real records yield resolvable pages.  With touch != 0 it drives the real
 *	__rep_prefault_record instead, so the pool and worker run too.
 *
 * PUBLIC: void __rep_prefault_verify __P((DB_ENV *, int, int));
 */
void
__rep_prefault_verify(dbenv, nrecs, touch)
	DB_ENV *dbenv;
	int nrecs;
	int touch;
{
	static const u_int8_t zero_ufid[DB_FILE_ID_LEN] = { 0 };
	DB_LOGC *logc = NULL;
	DBT rec = { 0 };
	DB_LSN lsn;
	TXN_RECS t = { 0 };
	REP_PREFAULT_CTX ctx;
	u_int32_t type, flags;
	int ret, i, n = 0;
	int64_t n_withpages = 0, n_parsefail = 0, n_pages = 0, n_skip = 0;
	int64_t n_dbreg = 0, n_ufid = 0, n_noid = 0;
	int64_t n_resolved = 0, n_unresolved = 0;

	/* Walks the log synchronously on the message-trap thread, so cap it. */
	if (nrecs <= 0)
		nrecs = 1000;
	if (nrecs > REP_PF_VERIFY_MAX) {
		logmsg(LOGMSG_USER,
		    "rep prefault verify: clamping %d records to %d\n", nrecs,
		    REP_PF_VERIFY_MAX);
		nrecs = REP_PF_VERIFY_MAX;
	}

	if (dbenv->pgnos_dtab == NULL || dbenv->pgnos_dtab_size == 0) {
		logmsg(LOGMSG_USER,
		    "rep prefault verify: pgnos dispatch table is not set up\n");
		return;
	}

	if ((ret = __log_cursor(dbenv, &logc)) != 0) {
		logmsg(LOGMSG_USER, "rep prefault verify: __log_cursor rc %d\n",
		    ret);
		return;
	}
	F_SET(&rec, DB_DBT_REALLOC);
	if (touch)
		__rep_prefault_ctx_init(&ctx);

	for (flags = DB_LAST; n < nrecs; flags = DB_PREV) {
		if (__log_c_get(logc, &lsn, &rec, flags) != 0)
			break;
		n++;

		if (rec.size < sizeof(u_int32_t))
			continue;

		if (touch) {
			/* Drive the real thing: parse, dedup, enqueue. */
			__rep_prefault_record(dbenv, &rec, &lsn, &ctx);
			continue;
		}

		type = __rep_prefault_rectype(&rec);

		t.npages = 0;
		if (__db_dispatch(dbenv, dbenv->pgnos_dtab,
		    dbenv->pgnos_dtab_size, &rec, &lsn,
		    DB_TXN_GETALLPGNOS, &t) != 0) {
			t.npages = 0;
			n_parsefail++;
			continue;
		}
		if (t.npages > 0)
			n_withpages++;

		for (i = 0; i < t.npages; i++) {
			LSN_PAGE *lp = &t.array[i];
			DB *dbp = NULL;
			int32_t ndx = -1;

			n_pages++;

			if (__rep_prefault_skip(type, lp->pgdesc.pgno,
			    lp->comment)) {
				n_skip++;
				continue;
			}

			if (lp->fid >= 0) {
				n_dbreg++;
				if (__dbreg_id_to_db_prefault(dbenv, NULL, &dbp,
				    lp->fid, 1) == 0) {
					n_resolved++;
					__dbreg_prefault_complete(dbenv, lp->fid);
				} else
					n_unresolved++;
			} else if (memcmp(lp->pgdesc.fileid, zero_ufid,
			    DB_FILE_ID_LEN) != 0) {
				n_ufid++;
				if (__ufid_find_db_prefault(dbenv, &dbp,
				    lp->pgdesc.fileid, &ndx, NULL) == 0) {
					n_resolved++;
					if (ndx >= 0)
						__dbreg_prefault_complete(dbenv,
						    ndx);
				} else
					n_unresolved++;
			} else
				n_noid++;
		}
	}

	logmsg(LOGMSG_USER, "rep prefault verify: walked %d log records\n", n);
	if (touch) {
		logmsg(LOGMSG_USER,
		    "  drove the prefault pool; see 'repprefault' for counters\n");
		__rep_prefault_ctx_destroy(dbenv, &ctx);
		if (rec.data != NULL)
			__os_ufree(dbenv, rec.data);
		(void)__log_c_close(logc);
		return;
	}
	logmsg(LOGMSG_USER, "  records with pages : %" PRId64 "\n", n_withpages);
	logmsg(LOGMSG_USER, "  records unparsed   : %" PRId64 "\n", n_parsefail);
	logmsg(LOGMSG_USER, "  pages reported     : %" PRId64 "\n", n_pages);
	logmsg(LOGMSG_USER, "  pages skipped      : %" PRId64 "\n", n_skip);
	logmsg(LOGMSG_USER, "  pages by dbreg id  : %" PRId64 "\n", n_dbreg);
	logmsg(LOGMSG_USER, "  pages by ufid      : %" PRId64 "\n", n_ufid);
	logmsg(LOGMSG_USER, "  pages no file id   : %" PRId64 "\n", n_noid);
	logmsg(LOGMSG_USER, "  pages resolved     : %" PRId64 "\n", n_resolved);
	logmsg(LOGMSG_USER, "  pages unresolved   : %" PRId64 "\n", n_unresolved);

	if (t.array != NULL)
		__os_free(dbenv, t.array);
	if (rec.data != NULL)
		__os_ufree(dbenv, rec.data);
	(void)__log_c_close(logc);
}
