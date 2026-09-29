/*
 * Asynchronous distributed commit.  See db/seqnum_wait.h for the rationale.
 *
 * This is a non-blocking re-expression of bdb_wait_for_seqnum_from_all_int():
 * instead of one block-processor thread sleeping on the seqnum condvar per
 * outstanding commit, one thread walks a list of outstanding commits and polls
 * each node with bdb_wait_for_seqnum_from_node_poll() -- the same per-node
 * checks as the inline wait, without sleeping.
 * All of the policy (durability accounting, demoting stragglers) is shared
 * with the inline path via helpers exported from bdb/rep.c.
 */

#include <poll.h>
#include <string.h>

#include "seqnum_wait.h"

#include "comdb2.h"
#include "comdb2_atomic.h"
#include "sys_wrap.h"
#include "logmsg.h"
#include "osqlblkseq.h"
#include "pool.h"
#include "gettimeofday_ms.h"
#include "reqlog.h"
#include "debug_switches.h"
#include "bdb_int.h"

void destroy_ireq(struct dbenv *dbenv, struct ireq *iq);
extern int gbl_async_dist_commit_max_outstanding_trans;
extern int gbl_2pc;
extern int gbl_replicant_retry_on_not_durable;
extern int gbl_ignore_final_non_durable_retry;
extern int gbl_debug_force_non_durable;
extern int gbl_debug_disttxn_trace;
extern int64_t gbl_distributed_commit_count;
extern int64_t gbl_not_durable_commit_count;
extern long long n_commit_time;

enum seqnum_wait_state {
    SEQNUM_WAIT_INIT,
    SEQNUM_WAIT_FIRST_ACK,
    SEQNUM_WAIT_GOT_FIRST_ACK,
    SEQNUM_WAIT_DONE,
    SEQNUM_WAIT_COMMIT,
    SEQNUM_WAIT_REPLY,
};

/* One commit waiting for replicants to ack: how far along the wait is, and
 * who to tell when it finishes. */
struct seqnum_wait {
    LINKC_T(struct seqnum_wait) lsn_lnk;
    LINKC_T(struct seqnum_wait) absolute_ts_lnk;

    enum seqnum_wait_state cur_state;
    bdb_state_type *bdb_state;
    seqnum_type seqnum;

    struct interned_string *nodelist[REPMAX];
    struct interned_string *connlist[REPMAX];
    int total_connected;
    int numnodes;
    int numwait;
    int numskip;
    int numfailed;
    int num_successfully_acked;
    int durable_lsns;
    int force_non_durable;
    int fake_incoherent;
    int catchup_window;

    int waitms;
    int cur_node;     /* index of the node we wait on now (-1: start a pass) */
    int node_begin;   /* when we started waiting on nodelist[cur_node] */
    int enqueue_time; /* for the rep-time stats, as the inline wait counts them */
    int timeoutms;    /* ditto: first-ack time plus the budget for the rest */
    int start_time;   /* when we started waiting for the first ack */
    int end_time;     /* when the first ack arrived (or we gave up on it) */
    int we_used;      /* end_time - start_time */
    int next_ts;      /* absolute ms timestamp when this item wants attention */

    struct interned_string *base_node;
    int outrc;

    /* The request itself: the block processor hands it over (with its
     * request logger) instead of recycling it, so the reply, the queue work,
     * the request log and closing the osql session all happen after the acks,
     * as they did when the block processor waited. */
    struct ireq *iq;
    int block_rc; /* toblock()'s rc: did the request itself succeed */
};

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    LISTC_T(struct seqnum_wait) lsn_list;         /* every outstanding commit */
    LISTC_T(struct seqnum_wait) absolute_ts_list; /* ordered by next_ts */
} seqnum_wait_queue;

static seqnum_wait_queue *work_queue = NULL;
static pool_t *seqnum_wait_queue_pool = NULL;
static pthread_mutex_t seqnum_wait_queue_pool_lk = PTHREAD_MUTEX_INITIALIZER;

static void *queue_processor(void *);

static struct seqnum_wait *allocate_seqnum_wait(void)
{
    struct seqnum_wait *s;
    Pthread_mutex_lock(&seqnum_wait_queue_pool_lk);
    s = pool_getablk(seqnum_wait_queue_pool);
    Pthread_mutex_unlock(&seqnum_wait_queue_pool_lk);
    return s;
}

static void deallocate_seqnum_wait(struct seqnum_wait *item)
{
    Pthread_mutex_lock(&seqnum_wait_queue_pool_lk);
    pool_relablk(seqnum_wait_queue_pool, item);
    Pthread_mutex_unlock(&seqnum_wait_queue_pool_lk);
}

int seqnum_wait_gbl_mem_init(void)
{
    pthread_t tid;
    pthread_attr_t attr;

    work_queue = calloc(1, sizeof(seqnum_wait_queue));
    if (work_queue == NULL)
        return -1;

    listc_init(&work_queue->lsn_list, offsetof(struct seqnum_wait, lsn_lnk));
    listc_init(&work_queue->absolute_ts_list, offsetof(struct seqnum_wait, absolute_ts_lnk));
    Pthread_mutex_init(&work_queue->mutex, NULL);
    Pthread_cond_init(&work_queue->cond, NULL);

    seqnum_wait_queue_pool = pool_setalloc_init(sizeof(struct seqnum_wait), 0, malloc, free);
    if (seqnum_wait_queue_pool == NULL) {
        free(work_queue);
        work_queue = NULL;
        return -1;
    }

    Pthread_attr_init(&attr);
    Pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    Pthread_create(&tid, &attr, queue_processor, NULL);
    Pthread_attr_destroy(&attr);
    return 0;
}

/* work_queue->mutex held */
static void add_to_absolute_ts_list(struct seqnum_wait *item)
{
    struct seqnum_wait *pos, *tmp;
    LISTC_FOR_EACH_SAFE(&work_queue->absolute_ts_list, pos, tmp, absolute_ts_lnk)
    {
        if (item->next_ts <= pos->next_ts) {
            listc_add_before(&work_queue->absolute_ts_list, item, pos);
            return;
        }
    }
    listc_abl(&work_queue->absolute_ts_list, item);
}

static void reschedule(struct seqnum_wait *item, int next_ts)
{
    Pthread_mutex_lock(&work_queue->mutex);
    item->next_ts = next_ts;
    listc_rfl(&work_queue->absolute_ts_list, item);
    add_to_absolute_ts_list(item);
    Pthread_mutex_unlock(&work_queue->mutex);
}

/* When to look at an item again if no ack wakes us first (an ack always
 * does).  As the inline wait: every seqnum_wait_interval if that is over 50ms,
 * otherwise not until the deadline. */
static int next_poll(bdb_state_type *bdb_state, int deadline)
{
    int interval = bdb_state->attr->seqnum_wait_interval;
    int next = comdb2_time_epochms() + interval;
    return (interval <= 50 || next > deadline) ? deadline : next;
}

void seqnum_wait_track(bdb_state_type *bdb_state, db_seqnum_type *commit_seqnum)
{
    struct interned_string *connlist[REPMAX];
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;
    if (!bdb_state->attr->track_replication_times)
        return;

    int n = net_get_all_commissioned_nodes_interned(bdb_state->repinfo->netinfo, connlist);
    bdb_track_commit_replication(bdb_state, (seqnum_type *)commit_seqnum, connlist, n);
}

/* items reserved by seqnum_wait_prepare() but not started yet; they count
 * against the cap.  Under work_queue->mutex. */
static int num_reserved = 0;

struct seqnum_wait *seqnum_wait_prepare(bdb_state_type *bdb_state, db_seqnum_type *seqnum, struct ireq *iq,
                                        int block_rc)
{
    struct seqnum_wait *swait;

    if (work_queue == NULL)
        return NULL;

    Pthread_mutex_lock(&work_queue->mutex);
    if (listc_size(&work_queue->lsn_list) + num_reserved >= gbl_async_dist_commit_max_outstanding_trans) {
        Pthread_mutex_unlock(&work_queue->mutex);
        return NULL;
    }
    num_reserved++;
    ATOMIC_ADD32(gbl_seqnum_wait_outstanding, 1);
    Pthread_mutex_unlock(&work_queue->mutex);

    swait = allocate_seqnum_wait();
    if (swait == NULL) {
        Pthread_mutex_lock(&work_queue->mutex);
        num_reserved--;
        ATOMIC_ADD32(gbl_seqnum_wait_outstanding, -1);
        Pthread_mutex_unlock(&work_queue->mutex);
        return NULL;
    }

    memset(swait, 0, sizeof(*swait));
    swait->cur_state = SEQNUM_WAIT_INIT;
    swait->bdb_state = bdb_state->parent ? bdb_state->parent : bdb_state;
    memcpy(&swait->seqnum, seqnum, sizeof(swait->seqnum));
    swait->next_ts = swait->end_time = swait->start_time = swait->enqueue_time = comdb2_time_epochms();
    swait->timeoutms = -1; /* what the inline wait leaves when it exits early */
    swait->iq = iq;
    swait->block_rc = block_rc;
    return swait;
}

void seqnum_wait_start(struct seqnum_wait *swait)
{
    Pthread_mutex_lock(&work_queue->mutex);
    num_reserved--;
    listc_abl(&work_queue->lsn_list, swait);
    add_to_absolute_ts_list(swait);
    Pthread_cond_signal(&work_queue->cond);
    Pthread_mutex_unlock(&work_queue->mutex);

    /* The waiter may be parked; there is work now.  Bump new_lsns -- the guard
     * it re-checks just before parking -- and broadcast, both under the lock
     * that owns new_lsns, so the wakeup cannot be lost in the check/park gap. */
    Pthread_mutex_lock(&new_lsns_lk);
    new_lsns++;
    Pthread_cond_broadcast(&new_lsns_cond);
    Pthread_mutex_unlock(&new_lsns_lk);
}

/* On clean exit, after the block processors have stopped and bdb is exiting
 * (so the waiter stops waiting on replicants): give the requests handed to
 * the waiter a chance to be replied to and finished. */
void seqnum_wait_drain(void)
{
    int start = comdb2_time_epochms();
    int left;

    if (work_queue == NULL)
        return;

    while (1) {
        Pthread_mutex_lock(&work_queue->mutex);
        left = listc_size(&work_queue->lsn_list) + num_reserved;
        Pthread_mutex_unlock(&work_queue->mutex);
        if (left == 0)
            return;
        if (comdb2_time_epochms() - start > 10000) {
            logmsg(LOGMSG_WARN, "%s: giving up with %d commits still waiting for replicants\n", __func__, left);
            return;
        }
        poll(NULL, 0, 10);
    }
}

void seqnum_wait_done(struct ireq *iq, int waitms, int rc)
{
    struct dbenv *dbenv = iq->dbenv;

    /* toblock() skips these when the wait comes after it */
    if (iq->timeoutms > dbenv->max_timeout_ms)
        dbenv->max_timeout_ms = iq->timeoutms;
    dbenv->total_timeouts_ms += iq->timeoutms;
    if (iq->reptimems > dbenv->max_reptime_ms)
        dbenv->max_reptime_ms = iq->reptimems;
    dbenv->total_reptime_ms += iq->reptimems;
    /* from the request's start, as the inline wait counts it; the request
     * kept its logger, so this includes the time spent handing it over */
    ATOMIC_ADD64(n_commit_time, (long long)reqlog_current_us(iq->reqlogger));

    /* trans_commit_int()'s traces for the wait */
    if (gbl_debug_disttxn_trace)
        logmsg(LOGMSG_USER, "%s wait-for-seqnum took %d ms rc %d\n", __func__, waitms, rc);
    if (gbl_extended_sql_debug_trace && IQ_HAS_SNAPINFO_KEY(iq))
        logmsg(LOGMSG_USER, "%.*s %s line %d: wait_for_seqnum [%d][%d] returns %d\n", IQ_SNAPINFO(iq)->keylen,
               IQ_SNAPINFO(iq)->key, __func__, __LINE__, iq->commit_file, iq->commit_offset, rc);
}

static int refresh_nodelist(struct seqnum_wait *item)
{
    item->numnodes = item->numskip = item->numwait = 0;
    item->total_connected = net_get_all_commissioned_nodes_interned(item->bdb_state->repinfo->netinfo, item->connlist);
    if (item->total_connected == 0)
        return 0;

    for (int i = 0; i < item->total_connected; i++) {
        int wait = 0;
        /* is_incoherent_complete returns 0 for COHERENT & INCOHERENT_WAIT */
        if (!is_incoherent_complete(item->bdb_state, item->connlist[i], &wait)) {
            item->nodelist[item->numnodes++] = item->connlist[i];
            if (wait)
                item->numwait++;
        } else {
            item->numskip++;
        }
    }
    return item->numnodes;
}

/* The inline wait's early exits.  The bdb write lock is wanted: a role change
 * or an env reopen/recovery is stopping the world, so we can no longer prove
 * this commit is durable here.  Or we are no longer master, or not the one
 * that made this commit: the inline wait, which starts right after the
 * commit, has no such gap, but the waiter may get to a commit after a role
 * change has come and gone.  Abandon the wait
 * and mirror the inline exits' commit accounting; like them, skip
 * bdb_wait_for_seqnum_finish() but still hold for the lease. */
static int stop_early(struct seqnum_wait *item)
{
    const char *why;
    if (bdb_lock_desired(item->bdb_state))
        why = "lock-is-desired";
    else if (!bdb_amimaster(item->bdb_state))
        why = "we are no longer master";
    else if (bdb_get_rep_gen(item->bdb_state) != item->seqnum.generation)
        why = "the generation changed";
    else
        return 0;

    logmsg(LOGMSG_ERROR, "%s line %d early exit because %s\n", __func__, __LINE__, why);
    ATOMIC_ADD64(gbl_distributed_commit_count, 1);
    ATOMIC_ADD64(gbl_not_durable_commit_count, 1);
    item->outrc = item->durable_lsns ? BDBERR_NOT_DURABLE : -1;
    item->cur_state = SEQNUM_WAIT_COMMIT;
    return 1;
}

static void process_work_item(struct seqnum_wait *item)
{
    bdb_state_type *bdb_state = item->bdb_state;

    switch (item->cur_state) {
    case SEQNUM_WAIT_INIT: {
        /* As the inline wait decides them.  trans_commit_int() refuses to
         * hand off when any of these tunables is set; re-read them anyway, as
         * they can come on between the hand-off and here. */
        int is_final = item->iq->sorese ? item->iq->sorese->is_final : 0;
        int non_durable_retry =
            gbl_replicant_retry_on_not_durable && (!is_final || !gbl_ignore_final_non_durable_retry);
        item->force_non_durable = non_durable_retry && gbl_debug_force_non_durable;
        item->durable_lsns = (bdb_state->attr->durable_lsns || non_durable_retry || gbl_2pc);
        item->fake_incoherent = debug_switch_all_incoherent() && (rand() % 2);
        item->catchup_window = bdb_state->attr->catchup_window;
        item->start_time = comdb2_time_epochms();

        /* the inline wait counts one expected udp ack per node it waits on;
         * count them once here, not on every poll */
        if (gbl_udp && refresh_nodelist(item) > 0) {
            for (int i = 0; i < item->numnodes; i++) {
                struct hostinfo *h = retrieve_hostinfo(item->nodelist[i]);
                Pthread_mutex_lock(&bdb_state->seqnum_info->lock);
                h->expected_udp_count++;
                Pthread_mutex_unlock(&bdb_state->seqnum_info->lock);
            }
        }
        item->cur_node = -1; /* start a pass */
        item->cur_state = SEQNUM_WAIT_FIRST_ACK;
    }
        /* fall through */

    case SEQNUM_WAIT_FIRST_ACK:
        if (stop_early(item))
            goto lease;

        if (bdb_state->exiting) {
            logmsg(LOGMSG_WARN, "exiting, not waiting for initial replication of <%d:%d>\n", item->seqnum.lsn.file,
                   item->seqnum.lsn.offset);
            /* the durability accounting needs the nodes, as inline has them */
            refresh_nodelist(item);
            item->cur_state = SEQNUM_WAIT_DONE;
            goto done_wait;
        }

        /* As the inline wait: passes over the nodes, each node getting up to
         * 1s to ack before we look at the next, until one acks.  The first to
         * ack in that order, not the fastest, sets how long the rest get. */
        for (;;) {
            if (item->cur_node < 0) {
                if (refresh_nodelist(item) == 0) {
                    item->cur_state = SEQNUM_WAIT_DONE;
                    goto done_wait;
                }
                item->cur_node = 0;
                item->node_begin = comdb2_time_epochms();
            }
            if (item->cur_node >= item->numnodes) {
                /* a whole pass and nobody acked: another if we still have
                 * time.  Like the inline do/while, we always make one. */
                if (comdb2_time_epochms() - item->start_time >= bdb_state->attr->rep_timeout_maxms)
                    break;
                item->cur_node = -1;
                continue;
            }

            int i = item->cur_node;
            int rc =
                bdb_wait_for_seqnum_from_node_poll(bdb_state, &item->seqnum, item->nodelist[i], item->fake_incoherent);
            int now = comdb2_time_epochms();
            if (rc == -999) {
                if (now - item->node_begin < 1000) {
                    reschedule(item, next_poll(bdb_state, item->node_begin + 1000));
                    return;
                }
                /* its 1s is up: it stays on the list, look at the next */
                item->cur_node++;
                item->node_begin = now;
                continue;
            }
            if (rc != 0) {
                /* drop it, as wait_for_seqnum_remove_node() does inline, so
                 * the loop for the rest does not demote it */
                item->nodelist[i] = item->nodelist[--item->numnodes];
                item->node_begin = now;
                if (item->numnodes == 0) {
                    item->cur_state = SEQNUM_WAIT_DONE;
                    goto done_wait;
                }
                continue;
            }

            item->base_node = item->nodelist[i];
            item->num_successfully_acked++;
            item->end_time = comdb2_time_epochms();
            item->we_used = item->end_time - item->start_time;

            /* make up a number for how long to wait for the rest based on
             * how long this node took */
            item->waitms = (item->we_used * bdb_state->attr->rep_timeout_lag) / 100;
            if (item->waitms < bdb_state->attr->rep_timeout_minms)
                item->waitms = bdb_state->attr->rep_timeout_minms;

            item->cur_node = 0;
            item->node_begin = item->end_time;
            item->timeoutms = item->we_used + item->waitms;
            item->cur_state = SEQNUM_WAIT_GOT_FIRST_ACK;
            goto got_first_ack;
        }

        /* we blew through rep_timeout_maxms without a single ack */
        logmsg(LOGMSG_WARN, "timed out waiting for initial replication of <%d:%d>\n", item->seqnum.lsn.file,
               item->seqnum.lsn.offset);
        item->end_time = comdb2_time_epochms();
        item->we_used = item->end_time - item->start_time;
        item->waitms = bdb_state->attr->rep_timeout_minms;
        item->cur_node = 0;
        item->node_begin = item->end_time;
        item->timeoutms = item->we_used + item->waitms;
        item->cur_state = SEQNUM_WAIT_GOT_FIRST_ACK;
        /* fall through */

    case SEQNUM_WAIT_GOT_FIRST_ACK:
    got_first_ack:
        if (stop_early(item))
            goto lease;

        /* Wait for the rest one node at a time, as the inline path does: each
         * node gets what is left of waitms, but never less than
         * rep_timeout_minms, counted from when we reach it. */
        for (; item->cur_node < item->numnodes && !bdb_state->exiting; item->cur_node++) {
            struct interned_string *node = item->nodelist[item->cur_node];
            if (node == item->base_node)
                continue;
            if (item->waitms < bdb_state->attr->rep_timeout_minms)
                item->waitms = bdb_state->attr->rep_timeout_minms;

            /* 1 (don't wait) if another commit gave up on it since we built
             * the list; unlike the inline path this also covers SLOW */
            int rc = is_incoherent_complete(bdb_state, node, NULL)
                         ? 1
                         : bdb_wait_for_seqnum_from_node_poll(bdb_state, &item->seqnum, node, item->fake_incoherent);
            int now = comdb2_time_epochms();
            if (rc == -999 && now - item->node_begin < item->waitms) {
                reschedule(item, next_poll(bdb_state, item->node_begin + item->waitms));
                return;
            }

            if (rc == 0) {
                item->num_successfully_acked++;
            } else if (rc == -999) {
                logmsg(LOGMSG_WARN, "replication timeout to node %s (%d ms), base node was %s with %d ms\n", node->str,
                       item->waitms, item->base_node ? item->base_node->str : "(none)", item->we_used);
                item->numfailed++;
            }
            /* demote on a timeout, a newer generation or a disconnect, but not
             * when it is catching up (1) or was demoted as rtcpu'd (-2) */
            if (rc != 0 && rc != 1 && rc != -2)
                bdb_wait_for_seqnum_mark_incoherent(bdb_state, &item->seqnum, node, item->catchup_window);

            /* take away the time this node used */
            item->waitms -= now - item->node_begin;
            item->node_begin = now;
        }
        item->cur_state = SEQNUM_WAIT_DONE;
        /* fall through */

    case SEQNUM_WAIT_DONE:
    done_wait:
        /* inline checks after every node it waits on; a lock can become
         * wanted after our last check, and the check below would block on it */
        if (!bdb_state->exiting && stop_early(item))
            goto lease;
        item->outrc = bdb_wait_for_seqnum_finish(bdb_state, &item->seqnum, item->numfailed, item->numskip,
                                                 item->numwait, item->num_successfully_acked, item->total_connected,
                                                 item->durable_lsns, item->force_non_durable);
        item->cur_state = SEQNUM_WAIT_COMMIT;
        /* fall through */

    case SEQNUM_WAIT_COMMIT:
    lease:
        if (bdb_attr_get(bdb_state->attr, BDB_ATTR_COHERENCY_LEASE)) {
            /* Somebody just went incoherent: hold this commit back, exactly as
             * the inline path does at the end of trans_wait_for_seqnum_int.
             * Re-checked on every entry: every wake looks at every item,
             * whatever its next_ts. */
            uint64_t now = gettimeofday_ms(), next_commit = next_commit_timestamp();
            if (next_commit > now) {
                reschedule(item, comdb2_time_epochms() + (int)(next_commit - now));
                return;
            }
        }

        if (item->outrc != 0)
            logmsg(LOGMSG_ERROR, "*WARNING* bdb_wait_seqnum:error syncing all nodes rc %d\n", item->outrc);

        item->cur_state = SEQNUM_WAIT_REPLY;
        break;

    default: /* SEQNUM_WAIT_REPLY: start_reply() takes it off the lists */
        break;
    }
}

/* The wait is over: reply to the request and finish it, then free the item. */
static void reply(struct seqnum_wait *item)
{
    /* Reply with toblock()'s rc, as handle_ireq() does; the ack wait only
     * overrides it when the commit came back non-durable, as toblock()
     * decides for the inline wait. */
    struct ireq *iq = item->iq;
    int rc = item->block_rc;
    if (item->outrc == BDBERR_NOT_DURABLE && durable_change_rcode(iq))
        rc = ERR_NOT_DURABLE;

    /* stats first, as toblock() does before the reply inline */
    iq->timeoutms = item->timeoutms;
    int waitms = comdb2_time_epochms() - item->enqueue_time;
    iq->reptimems += waitms;
    seqnum_wait_done(iq, waitms, item->outrc);

    /* A retry of this request stalls until it leaves the in-flight
     * blkseq table; toblock() left it there for us, so that the retry
     * waits for the acks, as it did inline. */
    osql_blkseq_unregister(iq);
    sorese_send_rc(iq, rc);

    /* the rest of handle_ireq(): queue work, request log (closes the osql
     * session); then what the block processor would have done: recycle
     * the request.  The logger was detached from its thread for us. */
    struct reqlogger *logger = iq->reqlogger;
    handle_ireq_finish(iq, rc);
    reqlog_free(logger);
    destroy_ireq(thedb, iq);
    deallocate_seqnum_wait(item);
}

/* Take a finished item off the waiter's lists and reply. */
static void start_reply(struct seqnum_wait *item)
{
    Pthread_mutex_lock(&work_queue->mutex);
    listc_rfl(&work_queue->absolute_ts_list, item);
    listc_rfl(&work_queue->lsn_list, item);
    ATOMIC_ADD32(gbl_seqnum_wait_outstanding, -1);
    Pthread_mutex_unlock(&work_queue->mutex);

    reply(item);
}

/* Look at every item, replying to the finished ones.  No more than the cap
 * (a handful), and each look is a poll that does not block: cheaper than
 * working out which ones an ack, a disconnect or exit could have moved. */
static void process_all(void)
{
    struct seqnum_wait *item, *next;

    Pthread_mutex_lock(&work_queue->mutex);
    item = LISTC_TOP(&work_queue->lsn_list);
    Pthread_mutex_unlock(&work_queue->mutex);

    while (item != NULL) {
        Pthread_mutex_lock(&work_queue->mutex);
        next = item->lsn_lnk.next;
        Pthread_mutex_unlock(&work_queue->mutex);

        process_work_item(item);

        if (item->cur_state == SEQNUM_WAIT_REPLY)
            start_reply(item);
        item = next;
    }
}

/* The seqnum-wait thread: look at every commit, then sleep until something
 * changes (an ack, a disconnect, exit, a new commit) or the earliest deadline
 * expires, whichever comes first. */
static void *queue_processor(void *arg)
{
    struct seqnum_wait *item;
    struct timespec waittime;
    uint64_t local_new_lsns;

    /* Deliberately not thrman_register()ed: this thread never exits, and
     * begin_clean_exit() waits for every registered generic thread.  Clean
     * exit waits for its requests with seqnum_wait_drain() instead. */
    thread_started("seqnum waiter");
    /* bdb_wait_for_seqnum_finish() takes BDB_READLOCK, which requires this
     * thread to have a bdb lock slot. */
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START);

    Pthread_mutex_lock(&new_lsns_lk);
    local_new_lsns = new_lsns;
    Pthread_mutex_unlock(&new_lsns_lk);

    while (1) {
        Pthread_mutex_lock(&work_queue->mutex);
        while (listc_size(&work_queue->lsn_list) == 0)
            Pthread_cond_wait(&work_queue->cond, &work_queue->mutex);
        Pthread_mutex_unlock(&work_queue->mutex);

        process_all();

        Pthread_mutex_lock(&work_queue->mutex);
        item = LISTC_TOP(&work_queue->absolute_ts_list);
        Pthread_mutex_unlock(&work_queue->mutex);

        if (item == NULL || comdb2_time_epochms() >= item->next_ts)
            continue;

        /* Park on new_lsns_cond, guarded by the lock that owns new_lsns, so a
         * bump+broadcast cannot land between the check and the sleep.  A
         * timed wait still bounds us by the earliest deadline. */
        Pthread_mutex_lock(&new_lsns_lk);
        if (local_new_lsns == new_lsns) {
            setup_waittime(&waittime, item->next_ts - comdb2_time_epochms());
            pthread_cond_timedwait(&new_lsns_cond, &new_lsns_lk, &waittime);
        }
        local_new_lsns = new_lsns;
        Pthread_mutex_unlock(&new_lsns_lk);
    }
    return NULL;
}
