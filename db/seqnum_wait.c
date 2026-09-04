/*
 * Asynchronous distributed commit.  See db/seqnum_wait.h for the rationale.
 *
 * This is a non-blocking re-expression of bdb_wait_for_seqnum_from_all_int():
 * instead of one block-processor thread sleeping on the seqnum condvar per
 * outstanding commit, one thread walks a list of outstanding commits and polls
 * each node's acked seqnum with bdb_wait_for_seqnum_from_node_nowait_int().
 * All of the policy (durability accounting, demoting stragglers) is shared
 * with the inline path via helpers exported from bdb/rep.c.
 */

#include <poll.h>
#include <string.h>

#include "seqnum_wait.h"

#include "comdb2.h"
#include "sys_wrap.h"
#include "logmsg.h"
#include "osqlcomm.h"
#include "osqlsession.h"
#include "pool.h"
#include "gettimeofday_ms.h"

/* from bdb/rep.c */
extern pthread_mutex_t max_lsn_so_far_lk;
extern DB_LSN max_lsn_so_far;
extern uint64_t new_lsns;
extern int last_slow_node_check_time;
extern pthread_mutex_t slow_node_check_lk;
int bdb_wait_for_seqnum_from_node_nowait_int(bdb_state_type *bdb_state, seqnum_type *seqnum,
                                             struct interned_string *host);
int is_incoherent_complete(bdb_state_type *bdb_state, struct interned_string *host, int *incohwait);
int bdb_track_replication_time(bdb_state_type *bdb_state, seqnum_type *seqnum, struct interned_string *host);
void bdb_slow_replicant_check(bdb_state_type *bdb_state, seqnum_type *seqnum);
void bdb_wait_for_seqnum_mark_incoherent(bdb_state_type *bdb_state, seqnum_type *seqnum, struct interned_string *host,
                                         int catchup_window);
int bdb_wait_for_seqnum_finish(bdb_state_type *bdb_state, seqnum_type *seqnum, int numfailed, int numskip, int numwait,
                               int num_successfully_acked, int total_commissioned, int durable_lsns,
                               int force_non_durable);

extern int gbl_async_dist_commit;
extern int gbl_async_dist_commit_max_outstanding_trans;
extern int gbl_2pc;
extern int gbl_replicant_retry_on_not_durable;

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
    if (seqnum_wait_queue_pool == NULL)
        return -1;

    Pthread_attr_init(&attr);
    Pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    Pthread_create(&tid, &attr, queue_processor, NULL);
    Pthread_attr_destroy(&attr);
    return 0;
}

/* work_queue->mutex held */
static void add_to_lsn_list(struct seqnum_wait *item)
{
    struct seqnum_wait *pos, *tmp;
    LISTC_FOR_EACH_SAFE(&work_queue->lsn_list, pos, tmp, lsn_lnk)
    {
        if (log_compare(&item->seqnum.lsn, &pos->seqnum.lsn) < 0) {
            listc_add_before(&work_queue->lsn_list, item, pos);
            return;
        }
    }
    listc_abl(&work_queue->lsn_list, item);
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

int add_to_seqnum_wait_queue(bdb_state_type *bdb_state, seqnum_type *seqnum, struct ireq *iq)
{
    struct seqnum_wait *swait;

    if (work_queue == NULL)
        return 0;

    Pthread_mutex_lock(&work_queue->mutex);
    if (listc_size(&work_queue->lsn_list) >= gbl_async_dist_commit_max_outstanding_trans) {
        Pthread_mutex_unlock(&work_queue->mutex);
        return 0;
    }
    Pthread_mutex_unlock(&work_queue->mutex);

    swait = allocate_seqnum_wait();
    if (swait == NULL)
        return 0;

    memset(swait, 0, sizeof(*swait));
    swait->cur_state = SEQNUM_WAIT_INIT;
    swait->bdb_state = bdb_state->parent ? bdb_state->parent : bdb_state;
    swait->seqnum = *seqnum;
    swait->next_ts = swait->end_time = swait->start_time = comdb2_time_epochms();

    swait->target = iq->sorese->target;
    swait->rqid = iq->sorese->rqid;
    comdb2uuidcpy(swait->uuid, iq->sorese->uuid);
    swait->snap = iq->sorese->snap_info;
    swait->nops = iq->sorese->nops;
    swait->rcout = iq->sorese->rcout;
    swait->errstat = iq->errstat;

    Pthread_mutex_lock(&work_queue->mutex);
    if (listc_size(&work_queue->lsn_list) >= gbl_async_dist_commit_max_outstanding_trans) {
        Pthread_mutex_unlock(&work_queue->mutex);
        deallocate_seqnum_wait(swait);
        return 0;
    }
    add_to_lsn_list(swait);
    add_to_absolute_ts_list(swait);
    Pthread_cond_signal(&work_queue->cond);
    Pthread_mutex_unlock(&work_queue->mutex);

    /* The waiter may be parked on the seqnum condvar; there is work now. */
    Pthread_mutex_lock(&swait->bdb_state->seqnum_info->lock);
    Pthread_cond_broadcast(&swait->bdb_state->seqnum_info->cond);
    Pthread_mutex_unlock(&swait->bdb_state->seqnum_info->lock);
    return 1;
}

/* work_queue->mutex held by caller for list surgery */
static void free_work_item(struct seqnum_wait *item)
{
    Pthread_mutex_lock(&work_queue->mutex);
    listc_rfl(&work_queue->absolute_ts_list, item);
    listc_rfl(&work_queue->lsn_list, item);
    Pthread_mutex_unlock(&work_queue->mutex);
    deallocate_seqnum_wait(item);
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

static void set_lock_desired_rc(struct seqnum_wait *item)
{
    item->outrc = item->durable_lsns ? BDBERR_NOT_DURABLE : -1;
    item->cur_state = SEQNUM_WAIT_COMMIT;
}

static void process_work_item(struct seqnum_wait *item)
{
    bdb_state_type *bdb_state = item->bdb_state;

    switch (item->cur_state) {
    case SEQNUM_WAIT_INIT:
        item->durable_lsns = (bdb_state->attr->durable_lsns || gbl_replicant_retry_on_not_durable || gbl_2pc);
        item->catchup_window = bdb_state->attr->catchup_window;
        item->start_time = comdb2_time_epochms();

        if (bdb_state->attr->track_replication_times) {
            int do_slow_node_check = 0;

            item->total_connected =
                net_get_all_commissioned_nodes_interned(bdb_state->repinfo->netinfo, item->connlist);

            Pthread_mutex_lock(&bdb_state->seqnum_info->lock);
            for (int i = 0; i < item->total_connected; i++)
                bdb_track_replication_time(bdb_state, &item->seqnum, item->connlist[i]);
            Pthread_mutex_unlock(&bdb_state->seqnum_info->lock);

            /* once a second, see if we have any slow replicants */
            Pthread_mutex_lock(&slow_node_check_lk);
            if (comdb2_time_epochms() - last_slow_node_check_time > 1000) {
                last_slow_node_check_time = comdb2_time_epochms();
                do_slow_node_check = 1;
            }
            Pthread_mutex_unlock(&slow_node_check_lk);

            if (do_slow_node_check &&
                (bdb_state->attr->warn_slow_replicants || bdb_state->attr->make_slow_replicants_incoherent)) {
                bdb_slow_replicant_check(bdb_state, &item->seqnum);
            }
        }
        item->cur_state = SEQNUM_WAIT_FIRST_ACK;
        /* fall through */

    case SEQNUM_WAIT_FIRST_ACK:
        if (bdb_lock_desired(bdb_state)) {
            set_lock_desired_rc(item);
            goto commit;
        }

        if ((comdb2_time_epochms() - item->start_time) < bdb_state->attr->rep_timeout_maxms) {
            if (refresh_nodelist(item) == 0) {
                item->cur_state = SEQNUM_WAIT_DONE;
                goto done_wait;
            }

            for (int i = 0; i < item->numnodes; i++) {
                if (bdb_wait_for_seqnum_from_node_nowait_int(bdb_state, &item->seqnum, item->nodelist[i]) != 0)
                    continue;

                item->base_node = item->nodelist[i];
                item->num_successfully_acked++;
                item->end_time = comdb2_time_epochms();
                item->we_used = item->end_time - item->start_time;

                /* make up a number for how long to wait for the rest based on
                 * how long the fastest node took */
                item->waitms = (item->we_used * bdb_state->attr->rep_timeout_lag) / 100;
                if (item->waitms < bdb_state->attr->rep_timeout_minms)
                    item->waitms = bdb_state->attr->rep_timeout_minms;

                item->cur_state = SEQNUM_WAIT_GOT_FIRST_ACK;
                goto got_first_ack;
            }

            /* nobody has caught up yet and we still have time -- look again
             * when a new ack lands or in 1ms, whichever is sooner. */
            reschedule(item, comdb2_time_epochms() + 1);
            return;
        }

        /* we blew through rep_timeout_maxms without a single ack */
        logmsg(LOGMSG_WARN, "timed out waiting for initial replication of <%d:%d>\n", item->seqnum.lsn.file,
               item->seqnum.lsn.offset);
        item->end_time = comdb2_time_epochms();
        item->we_used = item->end_time - item->start_time;
        item->waitms = bdb_state->attr->rep_timeout_minms;
        item->cur_state = SEQNUM_WAIT_GOT_FIRST_ACK;
        /* fall through */

    case SEQNUM_WAIT_GOT_FIRST_ACK:
    got_first_ack:
        if (bdb_lock_desired(bdb_state)) {
            set_lock_desired_rc(item);
            goto commit;
        }

        item->numfailed = 0;
        for (int i = 0; i < item->numnodes; i++) {
            if (item->nodelist[i] == item->base_node)
                continue;
            if (bdb_wait_for_seqnum_from_node_nowait_int(bdb_state, &item->seqnum, item->nodelist[i]) == 0)
                continue;
            item->numfailed++;
            break; /* one straggler is enough -- we have to wait anyway */
        }

        if (item->numfailed == 0) {
            /* everyone is caught up */
            item->num_successfully_acked = item->numnodes;
            item->cur_state = SEQNUM_WAIT_DONE;
            goto done_wait;
        }

        if ((comdb2_time_epochms() - item->end_time) < item->waitms) {
            reschedule(item, comdb2_time_epochms() + 1);
            return;
        }

        /* out of patience: final sweep, demote whoever is still behind */
        item->numfailed = 0;
        item->num_successfully_acked = item->base_node ? 1 : 0;
        for (int i = 0; i < item->numnodes; i++) {
            if (item->nodelist[i] == item->base_node)
                continue;
            if (bdb_wait_for_seqnum_from_node_nowait_int(bdb_state, &item->seqnum, item->nodelist[i]) == 0) {
                item->num_successfully_acked++;
                continue;
            }
            logmsg(LOGMSG_WARN, "replication timeout to node %s (%d ms), base node was %s with %d ms\n",
                   item->nodelist[i]->str, item->waitms, item->base_node ? item->base_node->str : "(none)",
                   item->we_used);
            item->numfailed++;
            bdb_wait_for_seqnum_mark_incoherent(bdb_state, &item->seqnum, item->nodelist[i], item->catchup_window);
        }
        item->cur_state = SEQNUM_WAIT_DONE;
        /* fall through */

    case SEQNUM_WAIT_DONE:
    done_wait:
        item->outrc =
            bdb_wait_for_seqnum_finish(bdb_state, &item->seqnum, item->numfailed, item->numskip, item->numwait,
                                       item->num_successfully_acked, item->total_connected, item->durable_lsns, 0);
        item->cur_state = SEQNUM_WAIT_COMMIT;

        if (bdb_attr_get(bdb_state->attr, BDB_ATTR_COHERENCY_LEASE)) {
            /* Somebody just went incoherent: hold this commit back, exactly as
             * the inline path does at the end of trans_wait_for_seqnum_int. */
            uint64_t now = gettimeofday_ms(), next_commit = next_commit_timestamp();
            if (next_commit > now) {
                reschedule(item, comdb2_time_epochms() + (int)(next_commit - now));
                return;
            }
        }
        /* fall through */

    case SEQNUM_WAIT_COMMIT:
    commit: {
        int sorese_rc = item->outrc;

        if (item->outrc && (!item->rcout || item->outrc == ERR_NOT_DURABLE))
            item->rcout = item->outrc;

        if (item->outrc == 0 && item->rcout == 0 && item->errstat.errval == COMDB2_SCHEMACHANGE_OK) {
            /* pretend an error happened to get errstat shipped to replicant */
            sorese_rc = 1;
        } else {
            item->errstat.errval = item->rcout;
        }

        osql_comm_signal_sqlthr_rc(&item->target, item->rqid, item->uuid, item->nops, &item->errstat, item->snap,
                                   sorese_rc);
        item->cur_state = SEQNUM_WAIT_FREE;
        break;
    }

    case SEQNUM_WAIT_FREE:
        break;
    }
}

/* Walk `list' processing every item that is ready, freeing finished ones.
 * `ready' decides whether we should still be looking at this item. */
static void drain(int ts_order)
{
    struct seqnum_wait *item, *next;

    Pthread_mutex_lock(&work_queue->mutex);
    item = ts_order ? LISTC_TOP(&work_queue->absolute_ts_list) : LISTC_TOP(&work_queue->lsn_list);
    Pthread_mutex_unlock(&work_queue->mutex);

    while (item != NULL) {
        if (ts_order) {
            if (item->next_ts > comdb2_time_epochms())
                break;
        } else {
            int past_max;
            Pthread_mutex_lock(&max_lsn_so_far_lk);
            past_max = log_compare(&item->seqnum.lsn, &max_lsn_so_far) > 0;
            Pthread_mutex_unlock(&max_lsn_so_far_lk);
            /* the list is LSN-ordered: nothing further can have been acked */
            if (past_max)
                break;
        }

        process_work_item(item);

        Pthread_mutex_lock(&work_queue->mutex);
        next = ts_order ? item->absolute_ts_lnk.next : item->lsn_lnk.next;
        Pthread_mutex_unlock(&work_queue->mutex);

        if (item->cur_state == SEQNUM_WAIT_FREE)
            free_work_item(item);
        item = next;
    }
}

static void *queue_processor(void *arg)
{
    struct seqnum_wait *item;
    struct timespec waittime;
    int wait_rc = 0;
    uint64_t local_new_lsns;

    /* Deliberately not thrman_register()ed: this thread never exits, and
     * begin_clean_exit() waits for every registered generic thread. */
    thread_started("seqnum waiter");
    /* bdb_wait_for_seqnum_finish() takes BDB_READLOCK, which requires this
     * thread to have a bdb lock slot. */
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START);

    Pthread_mutex_lock(&max_lsn_so_far_lk);
    local_new_lsns = new_lsns;
    Pthread_mutex_unlock(&max_lsn_so_far_lk);

    while (1) {
        Pthread_mutex_lock(&work_queue->mutex);
        while (listc_size(&work_queue->lsn_list) == 0)
            Pthread_cond_wait(&work_queue->cond, &work_queue->mutex);
        Pthread_mutex_unlock(&work_queue->mutex);

        drain(wait_rc == ETIMEDOUT);

        Pthread_mutex_lock(&work_queue->mutex);
        item = LISTC_TOP(&work_queue->absolute_ts_list);
        Pthread_mutex_unlock(&work_queue->mutex);

        if (item == NULL) {
            wait_rc = 0;
            continue;
        }

        if (comdb2_time_epochms() >= item->next_ts) {
            wait_rc = ETIMEDOUT;
            continue;
        }

        /* Park on the seqnum condvar so that any incoming ack wakes us. */
        Pthread_mutex_lock(&item->bdb_state->seqnum_info->lock);
        Pthread_mutex_lock(&max_lsn_so_far_lk);
        if (local_new_lsns != new_lsns) {
            /* acks landed while we were working -- don't sleep on them */
            local_new_lsns = new_lsns;
            Pthread_mutex_unlock(&max_lsn_so_far_lk);
            Pthread_mutex_unlock(&item->bdb_state->seqnum_info->lock);
            wait_rc = 0;
            continue;
        }
        Pthread_mutex_unlock(&max_lsn_so_far_lk);

        setup_waittime(&waittime, item->next_ts - comdb2_time_epochms());
        wait_rc =
            pthread_cond_timedwait(&item->bdb_state->seqnum_info->cond, &item->bdb_state->seqnum_info->lock, &waittime);
        Pthread_mutex_unlock(&item->bdb_state->seqnum_info->lock);

        Pthread_mutex_lock(&max_lsn_so_far_lk);
        local_new_lsns = new_lsns;
        Pthread_mutex_unlock(&max_lsn_so_far_lk);
    }
    return NULL;
}
