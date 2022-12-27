#include<async_wait.h>
#include<osqlblkseq.h>
#include<osqlblockproc.h>
#include<translistener.h>
#include<osqlcomm.h>
#include<socket_interfaces.h>
#include "gettimeofday_ms.h"
#include<lockmacros.h>
#include "comdb2.h"
#include "bdb_api.h"
#include <mem.h>
#include<errstat.h>
#include "reqlog.h"
#include "reqlog_int.h"

async_wait_queue *work_queue = NULL;

static pool_t *async_wait_queue_pool = NULL;
static pthread_mutex_t async_wait_queue_pool_lk;
static volatile sig_atomic_t async_wait_thread_running = 0;
static volatile sig_atomic_t async_wait_thread_should_stop = 0;

extern pthread_mutex_t max_lsn_so_far_lk;
extern DB_LSN max_lsn_so_far;
extern uint64_t coherency_commit_timestamp;
extern struct dbenv *thedb;
extern pool_t *p_reqs;
extern pthread_mutex_t lock;
extern int last_slow_node_check_time;
extern pthread_mutex_t slow_node_check_lk;
extern pool_t *p_reqs;
extern hash_t *seqnum_ts_hash;
struct seqnum_hash_entry;
uint64_t max_time_before_first_access = 0;

extern int gbl_async_dist_commit_max_outstanding_trans;
extern int gbl_async_dist_commit_verbose;
extern int gbl_async_dist_commit_track_seqnum_times;
extern int gbl_replicant_retry_on_not_durable;

void finish_handling_ireq(struct ireq *iq, int rc);
void cleanup_ireq(struct ireq *iq);
void *get_bdb_handle_ireq(struct ireq *iq, int auxdb);
extern pthread_mutex_t seqnum_hash_lk;
void *queue_processor(void *);  
int nodeix(const char *node);
uint64_t get_ent_ts(struct seqnum_hash_entry* entry);


static struct async_wait_node *allocate_async_wait_node(void){
    struct async_wait_node *s;
    Pthread_mutex_lock(&async_wait_queue_pool_lk);
    s = pool_getablk(async_wait_queue_pool);
    Pthread_mutex_unlock(&async_wait_queue_pool_lk);
    return s;
}

static int deallocate_async_wait_node(async_wait_node *item){
    int rc = 0;
    Pthread_mutex_lock(&async_wait_queue_pool_lk);
    rc = pool_relablk(async_wait_queue_pool, item);
    Pthread_mutex_unlock(&async_wait_queue_pool_lk); 
    return rc;
}

void set_stop_async_wait_thread() {
    async_wait_thread_should_stop = 1;
    if (async_wait_thread_running) {
        Pthread_cond_signal(&(work_queue->cond));
    }
}

int async_wait_init(){
    pthread_t dummy_tid;
    pthread_attr_t attr;

    work_queue = (async_wait_queue *)malloc(sizeof(async_wait_queue));
    if(work_queue == NULL){
        return -1;
    }
    work_queue->next_commit_timestamp = 0;
    listc_init(&work_queue->lsn_list, offsetof(async_wait_node, lsn_lnk));
    listc_init(&work_queue->absolute_ts_list, offsetof(async_wait_node, absolute_ts_lnk));
    Pthread_mutex_init(&(work_queue->mutex), NULL);
    Pthread_cond_init(&(work_queue->cond), NULL);
    if (gbl_async_dist_commit_track_seqnum_times) {
        seqnum_ts_hash = hash_init(sizeof(DB_LSN));
    }
    pthread_attr_init(&attr);
    Pthread_mutex_init(&async_wait_queue_pool_lk,NULL);
    Pthread_create(&dummy_tid, &attr, queue_processor, NULL);
    async_wait_queue_pool = pool_setalloc_init(sizeof(async_wait_node), 0, malloc, free);
    return 0;
}


int ts_compare(uint64_t ts1, uint64_t ts2){
    if(ts1 < ts2) return -1;
    else if(ts1 > ts2) return 1;
    else return 0;
}

/* Utility method to print the work_queue
assumes work_queue->mutex lock is held */
void print_lists(){
    async_wait_node *cur = NULL;
    logmsg(LOGMSG_USER,"locking at %d: \n",__LINE__);
    LISTC_FOR_EACH(&work_queue->lsn_list,cur,lsn_lnk)
    {
        logmsg(LOGMSG_USER,"{LSN: %d|%d}",cur->seqnum.lsn.file,cur->seqnum.lsn.offset);
        logmsg(LOGMSG_USER,"->"); 
    }
    logmsg(LOGMSG_USER,"\n");
    cur = NULL;
    LISTC_FOR_EACH(&work_queue->absolute_ts_list,cur, absolute_ts_lnk)
    {
        logmsg(LOGMSG_USER,"{TS: %d|%d|%ld}",cur->seqnum.lsn.file,cur->seqnum.lsn.offset,cur->next_ts);
        logmsg(LOGMSG_USER,"->"); 
    }
    logmsg(LOGMSG_USER,"\n");
    logmsg(LOGMSG_USER,"unlocking at %d: \n",__LINE__);
}

/* Assumes that we already have lock on work_queue->mutex */
void add_to_lsn_list(async_wait_node *item){
    async_wait_node *add_before_lsn = NULL;
    async_wait_node *temp = NULL;
    LISTC_FOR_EACH_SAFE(&work_queue->lsn_list,add_before_lsn,temp,lsn_lnk)
    {
        if(log_compare(&(item->seqnum.lsn), &(add_before_lsn->seqnum.lsn)) < 0){
            if (gbl_async_dist_commit_verbose) {
                logmsg(LOGMSG_DEBUG, "thread %p Adding item %d:%d to lsn_list before %d:%d\n",(void *)pthread_self(), item->seqnum.lsn.file, item->seqnum.lsn.offset, add_before_lsn->seqnum.lsn.file, add_before_lsn->seqnum.lsn.offset);
            }
            listc_add_before(&(work_queue->lsn_list),item,add_before_lsn);
            return;
        }
    }

    if (gbl_async_dist_commit_verbose) {
        logmsg(LOGMSG_DEBUG, "thread %p Adding item %d:%d to end of lsn_list\n", (void *)pthread_self(),item->seqnum.lsn.file, item->seqnum.lsn.offset);
    }
    listc_abl(&(work_queue->lsn_list), item);
}

/* Assumes that we already have lock on work_queue->mutex */
void add_to_absolute_ts_list(async_wait_node *item){
    async_wait_node *add_before_ts = NULL;
    async_wait_node *temp = NULL;
    if (gbl_async_dist_commit_verbose) {
        logmsg(LOGMSG_DEBUG, "Adding item %d:%d to absolute_ts_list with next time stamp %ld\n", item->seqnum.lsn.file, item->seqnum.lsn.offset, item->next_ts);
    }
    LISTC_FOR_EACH_SAFE(&work_queue->absolute_ts_list,add_before_ts,temp,absolute_ts_lnk)
    {
        if(item->next_ts < add_before_ts->next_ts){
            listc_add_before(&(work_queue->absolute_ts_list),item,add_before_ts);
            return;
        }
    }

    listc_abl(&(work_queue->absolute_ts_list), item);
}


int add_to_async_wait_queue(struct ireq *iq, int rc){
    char out[37];
    if (gbl_async_dist_commit_verbose) {
        logmsg(LOGMSG_USER,"trying to add work item linked to sorese uuid %s\n", comdb2uuidstr(iq->sorese->uuid,out));
    }

    if (async_wait_thread_should_stop) {
        logmsg(LOGMSG_USER, "async wait thread was asked to exit. not enqueing work item\n");
        return 0;
    }
    async_wait_node *swait = allocate_async_wait_node();
    if(swait==NULL){
        /* Could not allocate memory
           return -1 here to wait inline */
        return 0; 
    }
    /* copy necessary iq fields */
    swait->iq = iq;
    swait->cur_state = INIT;
    if (iq->usedb) {
        swait->bdb_state = iq->usedb->handle;
    } else {
        swait->bdb_state = thedb->bdb_env;
    }
    /* take ownership of osql sess */
    memcpy(&swait->seqnum, (seqnum_type *)iq->commit_seqnum, sizeof(seqnum_type));
    memcpy(&swait->errstat, &iq->errstat, sizeof(errstat_t));
    swait->numfailed = 0;
    swait->num_incoh = 0;
    swait->we_used = 0;
    swait->base_node = NULL;
    swait->num_successfully_acked = 0;
    swait->next_ts = swait->end_time = swait->start_time = comdb2_time_epochms();
    swait->absolute_ts_lnk.prev = NULL;
    swait->absolute_ts_lnk.next = NULL;
    swait->outrc = rc;
    swait->lsn_lnk.next = NULL;
    swait->lsn_lnk.prev = NULL;
    swait->track_once = 1;
    swait->durable_lsns = 0;
    Pthread_mutex_lock(&(work_queue->mutex));
    if(listc_size(&work_queue->lsn_list) > gbl_async_dist_commit_max_outstanding_trans){
        Pthread_mutex_unlock(&work_queue->mutex);
        if (gbl_async_dist_commit_verbose) {
            logmsg(LOGMSG_USER,"could not enqueue for async dist commit as seqnum wait queue at max capacity."
                    "waiting in line for distributed commit\n");
        }
        deallocate_async_wait_node(swait);
        return 0;
    }
    swait->enqueue_time = comdb2_time_epochms();
    add_to_lsn_list(swait);
    add_to_absolute_ts_list(swait);
    if (gbl_async_dist_commit_verbose) {
        logmsg(LOGMSG_USER,"Added work item linked to sorese uuid %s\n", comdb2uuidstr(iq->sorese->uuid,out));
    }
    // swait->logger->iq = swait->iq; 
    Pthread_cond_signal(&(work_queue->cond));
    Pthread_mutex_unlock(&(work_queue->mutex));
    /* Signal seqnum_cond as the worker might be waiting on this condition.. 
       We don't want the worker to wait needlessley i.e while there is still work to be done on the queue */
    Pthread_mutex_lock(&(swait->bdb_state->seqnum_info->lock));
    Pthread_cond_broadcast(&(swait->bdb_state->seqnum_info->cond));
    Pthread_mutex_unlock(&(swait->bdb_state->seqnum_info->lock));
    return 1;
}


/* assumes work_queue->mutex lock held */
void remove_from_async_wait_queue(async_wait_node *item){
    listc_rfl(&work_queue->absolute_ts_list, item);
    listc_rfl(&work_queue->lsn_list, item);

}

// removes the work item from the two lists, and returns memory back to mempool
int free_work_item(async_wait_node *item){
    int rc = 0;
    Pthread_mutex_lock(&(work_queue->mutex));
    remove_from_async_wait_queue(item);
    Pthread_mutex_unlock(&(work_queue->mutex));
    time_metric_add(thedb->async_wait_queue_time, comdb2_time_epochms() - item->enqueue_time);
    time_metric_add(thedb->async_wait_init_time, item->wait_init_end_time - item->wait_init_start_time);
    time_metric_add(thedb->async_wait_wait_for_first_ack_time, item->wait_wait_for_first_ack_end_time - item->wait_wait_for_first_ack_start_time);
    time_metric_add(thedb->async_wait_wait_for_all_ack_time, item->wait_wait_for_all_ack_end_time - item->wait_wait_for_all_ack_start_time);
    time_metric_add(thedb->async_wait_wait_finish_time, item->wait_wait_finish_end_time - item->wait_wait_finish_start_time);
    time_metric_add(thedb->async_wait_wait_next_commit_timestamp_time, item->wait_wait_next_commit_timestamp_end_time - item->wait_wait_next_commit_timestamp_start_time);
    time_metric_add(thedb->async_wait_wait_notify_time, item->wait_wait_notify_end_time - item->wait_wait_notify_start_time);
    rc = deallocate_async_wait_node(item);
    return rc;
}

static void print_time_diff(async_wait_node *item) {
    struct seqnum_hash_entry *ent;

    Pthread_mutex_lock(&seqnum_hash_lk);
    ent = hash_find(seqnum_ts_hash, &item->seqnum.lsn);
    if (ent != NULL) {
        logmsg(LOGMSG_USER, " node with seqnum %d:%d, got_new_seqnum at: %ld, being considered for retirement at : %d\n",item->seqnum.lsn.file, item->seqnum.lsn.offset, get_ent_ts(ent), comdb2_time_epochms());
    }
    Pthread_mutex_unlock(&seqnum_hash_lk);
}

void process_work_item(async_wait_node *item){
    int lock_desired = 0;
    char out[37];
    uint64_t diff = 0;
    switch(item->cur_state){
        case INIT:
            diff = comdb2_time_epochms() - item->enqueue_time;
            if (diff > max_time_before_first_access) {
                logmsg(LOGMSG_USER, " diff (%ld) greater than max_time_before_first_access (%ld) for lsn %d:%d\n", diff, max_time_before_first_access,item->seqnum.lsn.file, item->seqnum.lsn.offset);
                max_time_before_first_access = diff;
            }
            if (gbl_async_dist_commit_track_seqnum_times) {
                print_time_diff(item);
            }
            time_metric_add(thedb->async_wait_time_before_first_access, diff);
            item->wait_init_start_time = comdb2_time_epochms();
            /* short ciruit if we are waiting on lsn 0:0  */
            if ((item->seqnum.lsn.file == 0) && (item->seqnum.lsn.offset == 0)) {
                item->outrc = 0;
                item->cur_state = WAIT_NOTIFY;
                item->wait_init_end_time = comdb2_time_epochms();
                goto wait_notify_label;
            }
            /* if we were passed a child, find parent*/
            if (item->bdb_state->parent) {
                item->bdb_state = item->bdb_state->parent;
            }

            if (item->bdb_state->attr->durable_lsns || gbl_replicant_retry_on_not_durable) {
                item->durable_lsns = 1;
                if (gbl_async_dist_commit_verbose) {
                    logmsg(LOGMSG_USER, "turning on durable lsns for %d:%d\n", item->seqnum.lsn.file, item->seqnum.lsn.offset);
                }
            }

            if (gbl_async_dist_commit_verbose) {
                logmsg(LOGMSG_USER, "%s waiting for %d:%d\n", __func__, item->seqnum.lsn.file, item->seqnum.lsn.offset);
            }
            item->start_time = comdb2_time_epochms();
            item->num_commissioned = net_get_all_commissioned_nodes(item->bdb_state->repinfo->netinfo, item->connlist); 

            if (item->num_commissioned == 0){
                /* no replicants to wait for! */
                if (gbl_async_dist_commit_verbose) {
                    logmsg(LOGMSG_USER, "found %d nodes commissioned. Done waiting!\n", item->num_commissioned);
                }
                item->cur_state = WAIT_FINISH;
                item->wait_init_end_time = comdb2_time_epochms();
                goto wait_finish_label;
            }

            if (item->track_once && item->bdb_state->attr->track_replication_times) {
                item->track_once = 0;
                int do_slow_node_check = 0;
                Pthread_mutex_lock(&(item->bdb_state->seqnum_info->lock));
                for (int i = 0; i < item->num_commissioned; i++)
                    bdb_track_replication_time(item->bdb_state, &item->seqnum, item->connlist[i]);
                Pthread_mutex_unlock(&(item->bdb_state->seqnum_info->lock));

                /* once a second, see if we have any slow replicants */
                int now = comdb2_time_epochms();
                Pthread_mutex_lock(&slow_node_check_lk);
                if (now - last_slow_node_check_time > 1000) {
                    if (item->bdb_state->attr->track_replication_times) {
                        last_slow_node_check_time = now;
                        do_slow_node_check = 1;
                    }
                }
                Pthread_mutex_unlock(&slow_node_check_lk);

                /* do the slow replicant check - only if we need to ... */
                if (do_slow_node_check &&
                        item->bdb_state->attr->track_replication_times &&
                        (item->bdb_state->attr->warn_slow_replicants ||
                         item->bdb_state->attr->make_slow_replicants_incoherent)) {
                    if (gbl_async_dist_commit_verbose) {
                        logmsg(LOGMSG_USER, "doing slow node check at time %d\n", now);
                    }
                    bdb_slow_replicant_check(item->bdb_state, &item->seqnum);
                }
            }
            item->wait_init_end_time = comdb2_time_epochms();
        case WAIT_FOR_FIRST_ACK:
            if (item->cur_state != WAIT_FOR_FIRST_ACK) {
                item->cur_state = WAIT_FOR_FIRST_ACK;
                item->wait_wait_for_first_ack_start_time = comdb2_time_epochms();
            }
            if (((comdb2_time_epochms() - item->start_time) < item->bdb_state->attr->rep_timeout_maxms) && !(lock_desired = bdb_lock_desired(item->bdb_state))) {
                item->numnodes = 0;
                item->numskip = 0;
                item->numwait = 0;
                item->num_commissioned = net_get_all_commissioned_nodes(item->bdb_state->repinfo->netinfo, item->connlist); 

                if (item->num_commissioned == 0){
                    /* no replicants to wait for! */
                    if (gbl_async_dist_commit_verbose) {
                        logmsg(LOGMSG_USER, "found %d nodes commissioned. Done waiting!\n", item->num_commissioned);
                    }
                    item->cur_state = WAIT_FINISH;
                    item->wait_wait_for_first_ack_end_time = comdb2_time_epochms();
                    goto wait_finish_label;
                }
                for (int i = 0; i < item->num_commissioned; i++) {
                    int wait = 0;
                    /* is_incoherent returns 0 for COHERENT & INCOHERENT_WAIT */
                    if (!(is_incoherent_complete(item->bdb_state, item->connlist[i], &wait))) {
                        item->nodelist[item->numnodes] = item->connlist[i];
                        item->numnodes++;
                        if (wait)
                            item->numwait++;
                    } else {
                        item->numskip++;
                        item->num_incoh++;
                    }
                }

                if (item->numnodes == 0) {
                    item->cur_state = WAIT_FINISH;
                    item->wait_wait_for_first_ack_end_time = comdb2_time_epochms();
                    goto wait_finish_label;
                }
                if (gbl_async_dist_commit_verbose) {
                    logmsg(LOGMSG_USER, "item->numnodes: %d, item->numskip: %d, item->numwait: %d, item->num_incoh: %d\n",
                            item->numnodes, item->numskip, item->numwait, item->num_incoh);	
                }
                for (int i = 0; i < item->numnodes;i++) {
                    if (item->bdb_state->rep_trace){
                        logmsg(LOGMSG_USER,
                                "waiting for initial NEWSEQ from node %s of >= <%d:%d>\n",
                                item->nodelist[i], item->seqnum.lsn.file, item->seqnum.lsn.offset);
                    }
                    int rc = bdb_wait_for_seqnum_from_node_int(item->bdb_state, &item->seqnum, item->nodelist[i], 1000, __LINE__, 0 /* Fake incoherent */);
                    if (bdb_lock_desired(item->bdb_state)) {
                        logmsg(LOGMSG_ERROR,
                               "%s line %d early exit because lock-is-desired\n",
                               __func__, __LINE__);
                        if(item->durable_lsns){
                            item->outrc = BDBERR_NOT_DURABLE;
                        } else {
                            item->outrc = -1;
                        }
                        item->cur_state = WAIT_NOTIFY;
                        item->wait_wait_for_first_ack_end_time = comdb2_time_epochms();
                        goto wait_notify_label;
                    }

                    if (wait_for_seqnum_remove_node(item->bdb_state, rc)) {
                        if (gbl_async_dist_commit_verbose) {
                            logmsg(LOGMSG_USER, "removing node %d from nodelist\n", i);
                        }
                        item->nodelist[i] = item->nodelist[item->numnodes - 1];
                        item->numnodes--;
                        if (item->numnodes <= 0) {
                            item->wait_wait_for_first_ack_end_time = comdb2_time_epochms();
                            goto wait_finish_label;
                        }
                        i--;
                        assert(rc != 0);
                    }

                    if(rc == 0){
                        item->base_node = item->nodelist[i];
                        item->num_successfully_acked++;

                        item->end_time = comdb2_time_epochms();
                        item->we_used = item->end_time - item->start_time;
                        item->waitms = (item->we_used * item->bdb_state->attr->rep_timeout_lag) / 100;

                        if (item->waitms < item->bdb_state->attr->rep_timeout_minms){
                            /* If the first node responded really fast, we don't want to impose too harsh a timeout on the remaining nodes */
                            item->waitms = item->bdb_state->attr->rep_timeout_minms;
                        }
                        if (item->bdb_state->rep_trace) {
                            logmsg(LOGMSG_USER, "fastest node to <%d:%d> was %dms, will wait "
                                    "another %dms for remainder\n",
                                    item->seqnum.lsn.file, item->seqnum.lsn.offset, item->we_used, item->waitms);
                        }

                        /* WE got first ack.. move to next state */
                        item->cur_state = WAIT_FOR_ALL_ACK;
                        item->wait_wait_for_first_ack_end_time = comdb2_time_epochms();
                        goto got_first_ack_label;
                    }
                }
                /* none of the replicants have caught up yet, AND we are still within rep_timeout_maxms. 
                   continue to wait for first ack */
                Pthread_mutex_lock(&(work_queue->mutex));
                /* we want to revisit after timeout. If we get seqnums in the meanwhile
                   we'll recheck anyway. */
                int now = comdb2_time_epochms();
                item->next_ts = now +(item->bdb_state->attr->rep_timeout_maxms - (now - item->start_time)); 
                listc_rfl(&work_queue->absolute_ts_list, item);
                add_to_absolute_ts_list(item);
                Pthread_mutex_unlock(&(work_queue->mutex));
                break;
            } else {
                item->end_time = comdb2_time_epochms(); 
                item->we_used = item->end_time - item->start_time;
                item->waitms = item->bdb_state->attr->rep_timeout_minms - item->bdb_state->attr->rep_timeout_maxms;
                if (item->waitms < 0) {
                    item->waitms = 0;
                }
                if(!lock_desired)
                    logmsg(LOGMSG_WARN, "timed out waiting for initial replication of <%d:%d>\n",
                           item->seqnum.lsn.file, item->seqnum.lsn.offset);
                else
                    logmsg(LOGMSG_WARN,
                           "lock desired, not waiting for initial replication of <%d:%d>\n",
                           item->seqnum.lsn.file, item->seqnum.lsn.offset);
            } 
            item->wait_wait_for_first_ack_end_time = comdb2_time_epochms();
        case WAIT_FOR_ALL_ACK:
got_first_ack_label:
            item->cur_state = WAIT_FOR_ALL_ACK;
            item->wait_wait_for_all_ack_start_time = comdb2_time_epochms();
            item->numfailed = 0;
            // int acked = 0;
            int begin_time = 0, end_time = 0;
            item->start_time = comdb2_time_epochms();
            for(int i=0;i<item->numnodes;i++){
                if (item->nodelist[i] == item->base_node) {
                    continue;
                }

                /* we always want to wait atleast rep_timout_minms */
                if (item->waitms < item->bdb_state->attr->rep_timeout_minms) {
                    item->waitms = item->bdb_state->attr->rep_timeout_minms;
                }
                if (item->bdb_state->rep_trace) {
                    logmsg(LOGMSG_DEBUG,
                            "checking for NEWSEQ from node %s of >= <%d:%d> timeout %d\n",
                            item->nodelist[i], item->seqnum.lsn.file, item->seqnum.lsn.offset, item->waitms);
                }

                begin_time = comdb2_time_epochms();
                int rc = bdb_wait_for_seqnum_from_node_int(item->bdb_state, &item->seqnum, item->nodelist[i],item->waitms, __LINE__, 0 /* fake incoherent */);
                if (bdb_lock_desired(item->bdb_state)) {
                    logmsg(LOGMSG_USER,
                            "%s line %d early exit because lock-is-desired\n", __func__,
                            __LINE__);
                    if (item->durable_lsns) {
                        item->outrc = BDBERR_NOT_DURABLE;
                    } else {
                        logmsg(LOGMSG_USER, "lock desired .. setting outrc to -1 from %s:%d\n", __func__, __LINE__);
                        item->outrc = -1;
                    }
                    item->cur_state = WAIT_NOTIFY;
                    item->wait_wait_for_all_ack_end_time = comdb2_time_epochms();
                    goto wait_notify_label;
                }
                if (rc == -999) {
                    if (gbl_async_dist_commit_verbose) {
                        logmsg(LOGMSG_USER, "node %s hasn't caught up yet, base node "
                                "was %s",
                                item->nodelist[i],item->base_node);
                    }
                    item->numfailed++;
                } else if (rc == 0) {
                    item->num_successfully_acked++;
                } else if (rc == 1) {
                    rc = 0;
                }
                end_time = comdb2_time_epochms();
                item->waitms -= (end_time - begin_time);
                DB_LSN nodelsn = {0};
                uint32_t nodegen = 0;
                /* we may need to make node INCOHERENT.
                   NOTE: rc of -2 is for rtcpu. bdb_wait_for_seqnum_from_node_int handles that case */
                if (rc != 0 && rc != -2) {
                    int node_ix = nodeix(item->nodelist[i]);
                    /* Extract seqnum */
                    Pthread_mutex_lock(&(item->bdb_state->seqnum_info->lock));
                    nodegen = item->bdb_state->seqnum_info->seqnums[node_ix].generation;
                    nodelsn = item->bdb_state->seqnum_info->seqnums[node_ix].lsn;
                    Pthread_mutex_unlock(&(item->bdb_state->seqnum_info->lock));
                    if (gbl_async_dist_commit_verbose) {
                        logmsg(LOGMSG_USER,"item->nodegen: %d, item->nodelsn: %d:%d \n",nodegen, nodelsn.file, nodelsn.offset);
                    }
                    Pthread_mutex_lock(&(item->bdb_state->coherent_state_lock));
                    if (nodegen <= item->seqnum.generation && log_compare(&(item->seqnum.lsn), &nodelsn) >= 0) {
                        if (item->bdb_state->coherent_state[node_ix] == STATE_COHERENT) {
                            defer_commits(item->bdb_state, item->nodelist[i], __func__);
                        }
                        /* if catchup on commit then change to INCOHERENT_WAIT */
                        if (item->bdb_state->attr->catchup_on_commit && item->bdb_state->attr->catchup_window) { 
                            DB_LSN *masterlsn = &(item->bdb_state->seqnum_info->seqnums[nodeix(item->bdb_state->repinfo->master_host)].lsn);
                            int cntbytes = subtract_lsn(item->bdb_state, masterlsn, &nodelsn);
                            set_coherent_state(item->bdb_state, item->nodelist[i], (cntbytes < item->bdb_state->attr->catchup_window)?STATE_INCOHERENT_WAIT: STATE_INCOHERENT,__func__, __LINE__);

                        } else {
                            set_coherent_state(item->bdb_state, item->nodelist[i], STATE_INCOHERENT,__func__, __LINE__);
                        }
                        /* change next_commit_timestamp for the work queue, if new value of coherency_commit_timestamp is larger,
                           than current value of coherency_commit_timestamp */
                        work_queue->next_commit_timestamp = (work_queue->next_commit_timestamp < coherency_commit_timestamp)?coherency_commit_timestamp:work_queue->next_commit_timestamp;
                        if (gbl_async_dist_commit_verbose) {
                            logmsg(LOGMSG_USER," work_queue->next_commit_timestamp: %ld, coherency_commit_timestamp: %ld\n", work_queue->next_commit_timestamp, coherency_commit_timestamp);
                        }
                        item->bdb_state->last_downgrade_time[nodeix(item->nodelist[i])] = gettimeofday_ms();
                        item->bdb_state->repinfo->skipsinceepoch = comdb2_time_epoch();
                    }
                    Pthread_mutex_unlock(&(item->bdb_state->coherent_state_lock));
                }
            }
            item->wait_wait_for_all_ack_end_time = comdb2_time_epochms();
        case WAIT_FINISH:
wait_finish_label:
            item->cur_state = WAIT_FINISH;
            item->wait_wait_finish_start_time = comdb2_time_epochms();
            item->outrc = 0;
            if (!item->numfailed && !item->numskip && !item->numwait &&
                    item->bdb_state->attr->remove_commitdelay_on_coherent_cluster &&
                    item->bdb_state->attr->commitdelay) {
                logmsg(LOGMSG_USER, "Cluster is in sync, removing commitdelay\n");
                item->bdb_state->attr->commitdelay = 0;
            }


            if(item->durable_lsns){
                item->outrc = is_txn_durable(item->bdb_state, item->num_commissioned, 
                                             item->num_successfully_acked, &(item->seqnum));
            }
            item->wait_wait_finish_end_time = comdb2_time_epochms();
        case WAIT_NEXT_COMMIT_TIMESTAMP:
            if (item->cur_state != WAIT_NEXT_COMMIT_TIMESTAMP) {
                item->wait_wait_next_commit_timestamp_start_time = comdb2_time_epochms();
                item->cur_state = WAIT_NEXT_COMMIT_TIMESTAMP;
            }
            if (bdb_attr_get(item->iq->dbenv->bdb_attr, BDB_ATTR_COHERENCY_LEASE)) {
                uint64_t now = gettimeofday_ms(), next_commit = next_commit_timestamp();
                if (next_commit > now) {
                    /* Not safe to commit yet */
                    if (gbl_async_dist_commit_verbose) {
                        logmsg(LOGMSG_USER, "now: %ld, next_commit_timestamp: %ld." 
                                            "we are before next_commit_timestamp."
                                            "going to wait\n", now, work_queue->next_commit_timestamp);
                    }
                    Pthread_mutex_lock(&(work_queue->mutex));
                    item->next_ts = comdb2_time_epochms() + (next_commit - now);
                    //item->cur_state = WAIT_NOTIFY;
                    listc_rfl(&work_queue->absolute_ts_list, item);
                    add_to_absolute_ts_list(item);
                    Pthread_mutex_unlock(&(work_queue->mutex));
                    break;
                }
            }
            item->wait_wait_next_commit_timestamp_end_time = comdb2_time_epochms();
        case WAIT_NOTIFY:
wait_notify_label:
            item->cur_state = WAIT_NOTIFY;
            item->wait_wait_notify_start_time = comdb2_time_epochms();
            if (item->outrc == BDBERR_NOT_DURABLE) {
                item->outrc = ERR_NOT_DURABLE;
            }

            /* send result back */
            if (item->outrc && (!item->iq->sorese->rcout || item->outrc == ERR_NOT_DURABLE))
                item->iq->sorese->rcout = item->outrc;

            int sorese_rc = item->outrc;
            if (item->outrc == 0 && item->iq->sorese->rcout == 0 &&
                    item->errstat.errval == COMDB2_SCHEMACHANGE_OK) {
                /* pretend error happend to get errstat shipped to replicant */
                sorese_rc = 1;
            } else {
                item->errstat.errval = item->iq->sorese->rcout;
            }

            if (item->iq->sorese->rqid == 0)
                abort();
            if (gbl_async_dist_commit_verbose) {
                logmsg(LOGMSG_USER, "Calling osql_comm_signal_sqlthr_rc from %s:%d with sorese_rc: %d\n", __func__, __LINE__, sorese_rc);
            }
            osql_comm_signal_sqlthr_rc(&item->iq->sorese->target, item->iq->sorese->rqid, item->iq->sorese->uuid, 
                                       item->iq->sorese->nops,&(item->errstat), item->iq->sorese->snap_info, sorese_rc);
            item->iq->timings.req_sentrc = osql_log_time();
            if (gbl_async_dist_commit_verbose) {
                logmsg(LOGMSG_USER, "Done processing work item linked to sorese uuid : %s\n", comdb2uuidstr(item->iq->sorese->uuid,out));
            }
            finish_handling_ireq(item->iq, item->outrc);
            reqlog_free(item->iq->reqlogger);
            LOCK(&lock) 
            {
                if(item->iq->commit_seqnum)
                    free(item->iq->commit_seqnum);
                cleanup_ireq(item->iq);
            }
            UNLOCK(&lock);
            item->wait_wait_notify_end_time = comdb2_time_epochms();
            item->cur_state = DONE; 
            break;
        default:
            return;

    } /* End of Switch */
}

void *queue_processor(void *arg){
    async_wait_node *item = NULL;
    async_wait_node *next_item = NULL;
    struct timespec waittime;
    int wait_rc = 0;
    extern uint64_t new_lsns;
    extern bdb_state_type *gbl_bdb_state;
    int local_new_lsns = 0;
    int cur_time = 0;
    DB_LSN local_max_lsn_so_far = { .file = 0, .offset = 0};
    if (gbl_async_dist_commit_verbose) {
        logmsg(LOGMSG_DEBUG,"Starting async_wait worker thread\n");
    }

    bdb_thread_event(gbl_bdb_state, BDBTHR_EVENT_START_RDONLY);

    Pthread_mutex_lock(&gbl_bdb_state->seqnum_info->lock);
    local_new_lsns = new_lsns;
    memcpy(&local_max_lsn_so_far, &max_lsn_so_far, sizeof(DB_LSN));
    Pthread_mutex_unlock(&gbl_bdb_state->seqnum_info->lock);

    if (gbl_async_dist_commit_verbose) {
        logmsg(LOGMSG_USER,"local_new_lsns:%d\n", local_new_lsns);
    }

    async_wait_thread_running = 1;
    while(!async_wait_thread_should_stop){
        Pthread_mutex_lock(&(work_queue->mutex));
        while(!async_wait_thread_should_stop && listc_size(&(work_queue->lsn_list))==0){
            if (gbl_async_dist_commit_verbose) {
                logmsg(LOGMSG_USER,"LSN list is empty.... Waiting for work item\n");
            }
            Pthread_cond_wait(&(work_queue->cond),&(work_queue->mutex));
        }
        Pthread_mutex_unlock(&(work_queue->mutex));
        if (async_wait_thread_should_stop) {
            break;
        }

        /* iterate through LSN_LIST*/
        if (wait_rc==0) {
            if (gbl_async_dist_commit_verbose) {
                logmsg(LOGMSG_USER, "going through lsn list\n");
            }
            Pthread_mutex_lock(&gbl_bdb_state->seqnum_info->lock);
            local_new_lsns = new_lsns;
            memcpy(&local_max_lsn_so_far, &max_lsn_so_far, sizeof(DB_LSN));
            Pthread_mutex_unlock(&gbl_bdb_state->seqnum_info->lock);
            Pthread_mutex_lock(&(work_queue->mutex));
            item = LISTC_TOP(&(work_queue->lsn_list));
            Pthread_mutex_unlock(&(work_queue->mutex));
            while (item!=NULL) { 
                if(log_compare(&item->seqnum.lsn, &local_max_lsn_so_far) > 0){
                    /* We don't need to go down the lsn list any more as the remaining work_items are for Larger LSNs */
                    if (gbl_async_dist_commit_verbose) {
                        logmsg(LOGMSG_USER, "Current item LSN %d:%d larger than max_so_far %d:%d. Stop processing LSN list\n",item->seqnum.lsn.file, item->seqnum.lsn.offset, local_max_lsn_so_far.file, local_max_lsn_so_far.offset);
                    }
                    wait_rc = 1; /* go over absolute_ts list in next iteration */
                    break;
                }
                if (gbl_async_dist_commit_verbose) {
                    logmsg(LOGMSG_USER, "Processing work item with seqnum %d:%d, in state: %d\n",item->seqnum.lsn.file, item->seqnum.lsn.offset,item->cur_state);
                }
                process_work_item(item);
                if (item->cur_state == DONE) {
                    if (gbl_async_dist_commit_verbose) {
                        logmsg(LOGMSG_USER, "Done processing work item with seqnum %d:%d... Freeing it\n",item->seqnum.lsn.file, item->seqnum.lsn.offset);
                    }
                    Pthread_mutex_lock(&(work_queue->mutex));
                    next_item = item->lsn_lnk.next;
                    remove_from_async_wait_queue(item);
                    Pthread_mutex_unlock(&(work_queue->mutex));
                    deallocate_async_wait_node(item);
                    item = next_item;
                } else {
                    Pthread_mutex_lock(&(work_queue->mutex));
                    item = item->lsn_lnk.next;
                    Pthread_mutex_unlock(&(work_queue->mutex));
                }
            }
        } else {
            if (gbl_async_dist_commit_verbose) {
                logmsg(LOGMSG_USER,"Timed out on conditional wait.. Going over absolute ts list\n");
            }
            Pthread_mutex_lock(&(work_queue->mutex));
            item = LISTC_TOP(&(work_queue->absolute_ts_list));
            Pthread_mutex_unlock(&(work_queue->mutex));
            cur_time = comdb2_time_epochms();
            while(item!=NULL && (item->next_ts <= cur_time)){
                if (gbl_async_dist_commit_verbose) {
                    logmsg(LOGMSG_USER, "Processing work item with seqnum  %d:%d, in state: %d\n",item->seqnum.lsn.file, item->seqnum.lsn.offset,item->cur_state);
                }
                process_work_item(item);
                if(item->cur_state == DONE){
                    if (gbl_async_dist_commit_verbose) {
                        logmsg(LOGMSG_USER, "Done processing work item with seqnum %d:%d... Freeing it\n",item->seqnum.lsn.file, item->seqnum.lsn.offset);
                    }
                    Pthread_mutex_lock(&(work_queue->mutex));
                    next_item = item->absolute_ts_lnk.next;
                    remove_from_async_wait_queue(item);
                    Pthread_mutex_unlock(&(work_queue->mutex));
                    deallocate_async_wait_node(item);
                    item = next_item;
                }
                else{
                    Pthread_mutex_lock(&(work_queue->mutex));
                    item = item->absolute_ts_lnk.next;
                    Pthread_mutex_unlock(&(work_queue->mutex));
                }
            }
            if (item!=NULL) {
                /* Before going into a timed wait check if we've got new lsns in the meanwhile.
                   In this case, we shouldn't go into a timed wait... we should rather go over the lsn list */
                if(local_new_lsns != new_lsns){
                    if (gbl_async_dist_commit_verbose) {
                        logmsg(LOGMSG_USER,"Not going into timed wait as we got new lsns\n");
                    }
                    wait_rc = 0; /* since it's not ETIMEDOUT, we go through LSN list in next iteration */
                    continue;
                }
                setup_waittime(&waittime, item->next_ts - comdb2_time_epochms());
                if (gbl_async_dist_commit_verbose) {
                    logmsg(LOGMSG_USER, "Current time(%d) before earliest time (%ld) a work item has to be checked. going into timedcondwait\n",comdb2_time_epochms(),item->next_ts);
                }
                Pthread_mutex_lock(&(item->bdb_state->seqnum_info->lock));
                wait_rc = pthread_cond_timedwait(&(item->bdb_state->seqnum_info->cond),
                        &(item->bdb_state->seqnum_info->lock), &waittime); 
                Pthread_mutex_unlock(&(item->bdb_state->seqnum_info->lock));
            } else {
                if (gbl_async_dist_commit_verbose) {
                    logmsg(LOGMSG_USER, "List is empty... nothing to do\n");
                }
                wait_rc = 0;
            }
        }
    }
    async_wait_thread_running = 0;
    return NULL;
}

void async_wait_cleanup(){
    struct async_wait_node *item = NULL;

    //FREE THE WORK QUEUE
    Pthread_mutex_lock(&work_queue->mutex);
    LISTC_FOR_EACH(&work_queue->absolute_ts_list,item,absolute_ts_lnk)
    {
        remove_from_async_wait_queue(item);
        deallocate_async_wait_node(item);
    }

    //listc_free(&work_queue->absolute_ts_list);
    //listc_free(&work_queue->lsn_list);
    Pthread_mutex_unlock(&work_queue->mutex);
    Pthread_mutex_destroy(&work_queue->mutex);
    Pthread_cond_destroy(&work_queue->cond);
    if(work_queue!=NULL)
        free(work_queue);
    // FREE THE MEM POOL
    Pthread_mutex_lock(&async_wait_queue_pool_lk);
    pool_free(async_wait_queue_pool);
    Pthread_mutex_unlock(&async_wait_queue_pool_lk);
}
