#include<seqnum_wait.h>
#include<osqlblkseq.h>
#include<osqlblockproc.h>
#include<translistener.h>
#include<osqlcomm.h>
#include<socket_interfaces.h>
#include<gettimeofday_ms.h>
#include<lockmacros.h>
#include "comdb2.h"
#include <mem.h>
#include<errstat.h>

/* 
 * WORK QUEUE 
 * work_queue -> A queue abstraction encapsulating lists and locks used
 * seqnum_wait_queue_pool -> A mempool from which work items are allocated
 * seqnum_wait_queue_pool_lk -> Lock guarding the mempool. We need a lock because more than one block processor thread could try to enqueue a work item
 * */
seqnum_wait_queue *work_queue = NULL;
static pool_t *seqnum_wait_queue_pool = NULL;
static pthread_mutex_t seqnum_wait_queue_pool_lk;
extern pthread_mutex_t max_lsn_so_far_lk;
extern DB_LSN max_lsn_so_far;
extern uint64_t coherency_commit_timestamp;
extern struct dbenv *thedb;
extern pool_t *p_reqs;
extern int last_slow_node_check_time;
void *queue_processor(void *);  
extern int gbl_async_dist_commit_max_outstanding_trans;

int bdb_track_replication_time(bdb_state_type *bdb_state, seqnum_type *seqnum, const char *host);

void bdb_slow_replicant_check(bdb_state_type *bdb_state,
                                     seqnum_type *seqnum);
int bdb_wait_for_seqnum_from_node_nowait_int(bdb_state_type *bdb_state,
                                                    seqnum_type *seqnum,
                                                    const char *host);

int is_incoherent_complete(bdb_state_type *bdb_state,
                                         const char *host, int *incohwait);
void defer_commits(bdb_state_type *bdb_state, const char *host,
                                 const char *func);

int nodeix(const char *node);
void set_coherent_state(bdb_state_type *bdb_state,
                                      const char *hostname, int state,
                                      const char *func, int line);

static struct seqnum_wait *allocate_seqnum_wait(void){
    struct seqnum_wait *s;
    Pthread_mutex_lock(&seqnum_wait_queue_pool_lk);
    s = pool_getablk(seqnum_wait_queue_pool);
    Pthread_mutex_unlock(&seqnum_wait_queue_pool_lk);
    return s;
}

static int deallocate_seqnum_wait(struct seqnum_wait *item){
    int rc = 0;
    Pthread_mutex_lock(&seqnum_wait_queue_pool_lk);
    rc = pool_relablk(seqnum_wait_queue_pool, item);
    Pthread_mutex_unlock(&seqnum_wait_queue_pool_lk); 
    return rc;
}

int seqnum_wait_gbl_mem_init(){
    work_queue = (seqnum_wait_queue *)malloc(sizeof(seqnum_wait_queue));
    if(work_queue == NULL){
        return -1;
    }
    work_queue->next_commit_timestamp = 0;
    listc_init(&work_queue->lsn_list, offsetof(struct seqnum_wait, lsn_lnk));
    listc_init(&work_queue->absolute_ts_list, offsetof(struct seqnum_wait, absolute_ts_lnk));
    Pthread_mutex_init(&(work_queue->mutex), NULL);
    Pthread_cond_init(&(work_queue->cond), NULL);
    
    pthread_t dummy_tid;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    // Init mem pool lock
    Pthread_mutex_init(&seqnum_wait_queue_pool_lk,NULL);
    // Create worker thread 
    Pthread_create(&dummy_tid, &attr, queue_processor, NULL);
    // Allocate the mem pool
    seqnum_wait_queue_pool = pool_setalloc_init(sizeof(struct seqnum_wait), 0, malloc, free);
    return 0;
}

int ts_compare(int ts1, int ts2){
    if(ts1 < ts2) return -1;
    else if(ts1 > ts2) return 1;
    else return 0;
}
//Utility method to print the work_queue
//assumes work_queue->mutex lock is held
void print_lists(){
    struct seqnum_wait *cur = NULL;
    //printf("locking at %d: \n",__LINE__);
    //pthread_mutex_lock(&(work_queue->mutex));
    LISTC_FOR_EACH(&work_queue->lsn_list,cur,lsn_lnk)
    {
        printf("{LSN: %d|%d}",cur->seqnum.lsn.file,cur->seqnum.lsn.offset);
        printf("->"); 
    }
    printf("\n");
    cur = NULL;
    LISTC_FOR_EACH(&work_queue->absolute_ts_list,cur, absolute_ts_lnk)
    {
        printf("{TS: %d|%d|%d}",cur->seqnum.lsn.file,cur->seqnum.lsn.offset,cur->next_ts);
        printf("->"); 
    }
    printf("\n");
    //printf("unlocking at %d: \n",__LINE__);
    //pthread_mutex_unlock(&(work_queue->mutex));
}

// Assumes that we already have lock on work_queue->mutex
void add_to_lsn_list(struct seqnum_wait *item){
    struct seqnum_wait *add_before_lsn = NULL;
    struct seqnum_wait *temp = NULL;
    //logmsg(LOGMSG_DEBUG, "+++Adding item %d:%d to lsn_list\n", item->seqnum.lsn.file, item->seqnum.lsn.offset);
    LISTC_FOR_EACH_SAFE(&work_queue->lsn_list,add_before_lsn,temp,lsn_lnk)
    {
        if(log_compare(&(item->seqnum.lsn), &(add_before_lsn->seqnum.lsn)) < 0){
            listc_add_before(&(work_queue->lsn_list),item,add_before_lsn);
            return;
        }
    }
    
        // The new LSN is the highest yet... Adding to end of lsn list
    listc_abl(&(work_queue->lsn_list), item);
}

// Assumes that we already have lock on work_queue->mutex
void add_to_absolute_ts_list(struct seqnum_wait *item){
    struct seqnum_wait *add_before_ts = NULL;
    struct seqnum_wait *temp = NULL;
    //logmsg(LOGMSG_DEBUG, "+++Adding item %d:%d to absolute_ts_list with next time stamp %d\n", item->seqnum.lsn.file, item->seqnum.lsn.offset, item->next_ts);
    LISTC_FOR_EACH_SAFE(&work_queue->absolute_ts_list,add_before_ts,temp,absolute_ts_lnk)
    {
        if(item->next_ts <= add_before_ts->next_ts){
           listc_add_before(&(work_queue->absolute_ts_list),item,add_before_ts);
           return;
        }
    }

    // updated next timestamp for this item is the highest yet... Adding to end of absolute_ts_list
    listc_abl(&(work_queue->absolute_ts_list), item);
}


int add_to_seqnum_wait_queue(bdb_state_type* bdb_state, seqnum_type *seqnum,struct dbenv *dbenv,sorese_info_t *sorese, errstat_t *errstat,int rc){
    struct seqnum_wait *swait = allocate_seqnum_wait();
    if(swait==NULL){
        // Could not allocate memory...  return 0 here to relapse to waiting inline
        return 0; 
    }
    swait->cur_state = INIT;
    swait->bdb_state = bdb_state;
    swait->dbenv = dbenv;
    memcpy(&swait->seqnum, seqnum, sizeof(seqnum_type));
    swait->do_slow_node_check = 0;
    swait->numfailed = 0;
    swait->num_incoh = 0;
    swait->we_used = 0;
    swait->base_node = NULL;
    swait->num_successfully_acked = 0;
    swait->lock_desired = 0;
    swait->next_ts = swait->end_time = swait->start_time = comdb2_time_epochms();
    swait->absolute_ts_lnk.prev = NULL;
    swait->absolute_ts_lnk.next = NULL;
    swait->outrc = rc;
    swait->lsn_lnk.next = NULL;
    swait->lsn_lnk.prev = NULL;
    memcpy(&swait->sorese,sorese, sizeof(sorese_info_t));
    memcpy(&swait->errstat,errstat, sizeof(errstat_t));
    Pthread_mutex_lock(&(work_queue->mutex));
    if(listc_size(&work_queue->lsn_list) >= gbl_async_dist_commit_max_outstanding_trans){
        Pthread_mutex_unlock(&work_queue->mutex);
        //logmsg(LOGMSG_USER,"could not enqueue for async dist commit as seqnum wait queue at max capacity... waiting in line for distributed commit\n");
        deallocate_seqnum_wait(swait);
        return 0;
    }

    // Add to the lsn list in increasing order of LSN 
    add_to_lsn_list(swait);
    // Add to absolute_ts_list in increasing order of next_ts
    add_to_absolute_ts_list(swait);
    Pthread_cond_signal(&(work_queue->cond));
    Pthread_mutex_unlock(&(work_queue->mutex));
    // Signal seqnum_cond as the worker might be waiting on this condition.. 
    // We don't want the worker to wait needlessley i.e while there is still work to be done on the queue
    Pthread_mutex_lock(&(swait->bdb_state->seqnum_info->lock));
    Pthread_cond_broadcast(&(swait->bdb_state->seqnum_info->cond));
    Pthread_mutex_unlock(&(swait->bdb_state->seqnum_info->lock));
    return 1;
}
// assumes work_queue->mutex lock held
void remove_from_seqnum_wait_queue(struct seqnum_wait *item){
    listc_rfl(&work_queue->absolute_ts_list, item);
    listc_rfl(&work_queue->lsn_list, item);

}

// removes the work item from the two lists, and returns memory back to mempool
int free_work_item(struct seqnum_wait *item){
    int rc = 0;
    // First remove item from the two lists;
    Pthread_mutex_lock(&(work_queue->mutex));
    remove_from_seqnum_wait_queue(item);
    Pthread_mutex_unlock(&(work_queue->mutex));

    // Return memory to mempool
    rc = deallocate_seqnum_wait(item);
    return rc;
}

void process_work_item(struct seqnum_wait *item){
    extern pthread_mutex_t slow_node_check_lk;
    switch(item->cur_state){
        case INIT:
            /* if we were passed a child, find his parent*/
            if(item->bdb_state->parent)
            item->bdb_state = item->bdb_state->parent;

            /* short circuit if we are waiting on lsn 0:0  
            if((item->seqnum.lsn.file == 0) && (item->seqnum.lsn.offset == 0))
            {
                // Do stuff corresponding to rc=0 in bdb_wait_for_seqnum_from_all_int
                item->outrc = 0;
                item->cur_state = COMMIT;
                goto commit_label;
            }*/
            //logmsg(LOGMSG_DEBUG, "+++%s waiting for %s\n", __func__, lsn_to_str(item->str, &(item->seqnum.lsn)));
            item->start_time = comdb2_time_epochms();
            if(item->bdb_state->attr->durable_lsns){
                item->total_connected = net_get_sanctioned_replicants(item->bdb_state->repinfo->netinfo, REPMAX, item->connlist);
            } else {
                item->total_connected = net_get_all_commissioned_nodes(item->bdb_state->repinfo->netinfo, item->connlist); 
            }
            if (item->total_connected == 0){
                /* nobody is there to wait for! */
                item->cur_state = DONE_WAIT;
                goto done_wait_label;
            }

            if (item->bdb_state->attr->track_replication_times) {
                Pthread_mutex_lock(&(item->bdb_state->seqnum_info->lock));
                for (int i = 0; i < item->total_connected; i++)
                    bdb_track_replication_time(item->bdb_state, &item->seqnum, item->connlist[i]);
                Pthread_mutex_unlock(&(item->bdb_state->seqnum_info->lock));

                /* once a second, see if we have any slow replicants */
                item->now = comdb2_time_epochms();
                Pthread_mutex_lock(&slow_node_check_lk);
                if (item->now - last_slow_node_check_time > 1000) {
                    if (item->bdb_state->attr->track_replication_times) {
                        last_slow_node_check_time = item->now;
                        item->do_slow_node_check = 1;
                    }
                }
                Pthread_mutex_unlock(&slow_node_check_lk);

                /* do the slow replicant check - only if we need to ... */
                if (item->do_slow_node_check &&
                    item->bdb_state->attr->track_replication_times &&
                    (item->bdb_state->attr->warn_slow_replicants ||
                     item->bdb_state->attr->make_slow_replicants_incoherent)) {
                    bdb_slow_replicant_check(item->bdb_state, &item->seqnum);
                }
            }
            // We're done with INIT state.. Move to FIRST_ACK and fall through this case.
            item->cur_state = FIRST_ACK;
        case FIRST_ACK:
            item->cur_state = FIRST_ACK;
            if(((comdb2_time_epochms() - item->start_time) < item->bdb_state->attr->rep_timeout_maxms) &&
                    !(item->lock_desired = bdb_lock_desired(item->bdb_state))){
                item->numnodes = 0;
                item->numskip = 0;
                item->numwait = 0;

                if(item->bdb_state->attr->durable_lsns){
                    item->total_connected = net_get_sanctioned_replicants(item->bdb_state->repinfo->netinfo, REPMAX, item->connlist);
                } else {
                    item->total_connected = net_get_all_commissioned_nodes(item->bdb_state->repinfo->netinfo, item->connlist); 
                }
                if (item->total_connected == 0){
                    /* nobody is there to wait for! */
                    item->cur_state = DONE_WAIT;
                    goto done_wait_label;
                }
                for (int i = 0; i < item->total_connected; i++) {
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
                    item->cur_state = DONE_WAIT;
                    goto done_wait_label;
                }
                
                for (int i = 0; i < item->numnodes; i++) {
                    if (item->bdb_state->rep_trace){
                        /*logmsg(LOGMSG_DEBUG,
                               "+++checking for initial NEWSEQ from node %s of >= <%s>\n",
                               item->nodelist[i], lsn_to_str(item->str, &(item->seqnum.lsn)));    */
                    }
                    int rc = bdb_wait_for_seqnum_from_node_nowait_int(item->bdb_state, &item->seqnum, item->nodelist[i]);
                    if(rc == 0){
                        item->base_node = item->nodelist[i];
                        item->num_successfully_acked++;
                        item->end_time = comdb2_time_epochms();
                        item->we_used = item->end_time - item->start_time;
                        item->waitms = (item->we_used * item->bdb_state->attr->rep_timeout_lag) / 100;
                        if (item->waitms < item->bdb_state->attr->rep_timeout_minms){
                            // If the first node responded really fast, we don't want to impose too harsh a timeout on the remaining nodes
                            item->waitms = item->bdb_state->attr->rep_timeout_minms;
                        }
                        if (item->bdb_state->rep_trace)
                            /*logmsg(LOGMSG_DEBUG, "+++fastest node to <%s> was %dms, will wait "
                                            "another %dms for remainder\n",
                                    lsn_to_str(item->str, &(item->seqnum.lsn)), item->we_used, item->waitms);*/
                        // WE got first ack.. move to next state.. 
                        item->cur_state = GOT_FIRST_ACK;
                        goto got_first_ack_label;
                    }
                }
                // If we get here, then none of the replicants have caught up yet, AND we are still within rep_timeout_maxms 
                // Let's wait for one second and check again.
                Pthread_mutex_lock(&(work_queue->mutex));
                item->next_ts = comdb2_time_epochms();
                listc_rfl(&work_queue->absolute_ts_list, item);
                add_to_absolute_ts_list(item);
                Pthread_mutex_unlock(&(work_queue->mutex));
                // We break out of the switch as we are done processing this item (for now)
                break;
              }
           else{
               // Either we timed out, or someone else is asking for bdb lock i.e master swing
               if(item->lock_desired){
                   //logmsg(LOGMSG_DEBUG,"+++lock desired, not waiting for initial replication of <%s>\n", lsn_to_str(item->str, &(item->seqnum.lsn)));
                   // Not gonna wait for anymore acks...Set rcode and go to DONE_WAIT;
                   if(item->bdb_state->attr->durable_lsns){
                       item->outrc = BDBERR_NOT_DURABLE;
                   }
                   else{
                       item->outrc = -1;
                   }
                   item->cur_state = COMMIT;
                   goto commit_label;
               }
               // we timed out i.e exceeded bdb->attr->rep_timeout_maxms
               /*logmsg(LOGMSG_DEBUG, "+++timed out waiting for initial replication of <%s>\n",
                       lsn_to_str(item->str, &(item->seqnum.lsn)));*/
               item->end_time = comdb2_time_epochms(); 
               item->we_used = item->end_time - item->start_time;
               item->waitms = 0; // We've already exceeded max timeout... Not gonna wait anymore
               item->cur_state = GOT_FIRST_ACK;
           } 
        case GOT_FIRST_ACK: got_first_ack_label:
           // First check if someone else wants bdb_lock a.k.a master swing
           if(bdb_lock_desired(item->bdb_state)){
               //logmsg(LOGMSG_DEBUG,"+++%s line %d early exit because lock-is-desired\n",__func__,__LINE__);
               if(item->bdb_state->attr->durable_lsns){
                   item->outrc = BDBERR_NOT_DURABLE;
               }
               else{
                   item->outrc = -1;
               }
               item->cur_state = COMMIT;
               goto commit_label;
           }
           // Either we've received first ack or we've timed out
            item->numfailed = 0;
            int acked = 0;
            for(int i=0;i<item->numnodes;i++){
                if(item->nodelist[i] == item->base_node)
                    continue;      
                if (item->bdb_state->rep_trace){
                    /*logmsg(LOGMSG_DEBUG,
                           "+++checking for NEWSEQ from node %s of >= <%s> timeout %d\n",
                           item->nodelist[i], lsn_to_str(item->str, &(item->seqnum.lsn)), item->waitms);*/
                }
                int rc = bdb_wait_for_seqnum_from_node_nowait_int(item->bdb_state, &item->seqnum, item->nodelist[i]);
                if (bdb_lock_desired(item->bdb_state)) {
                    /*logmsg(LOGMSG_DEBUG,
                           "+++%s line %d early exit because lock-is-desired\n", __func__,
                           __LINE__);*/
                    if(item->bdb_state->attr->durable_lsns){
                       item->outrc = BDBERR_NOT_DURABLE;
                    }
                    else{
                       item->outrc = -1;
                    }
                    item->cur_state = COMMIT;
                    goto commit_label;
                }
                if (rc == -999){
                    /*logmsg(LOGMSG_DEBUG, "+++node %s hasn't caught up yet, base node "
                                    "was %s",
                            item->nodelist[i],item->base_node);*/
                    item->numfailed++;
                    // If even one replicant hasn't caught up, we come out of the loop and wait
                    // No point in checking the others as we will have to wait anyways
                    break;
                }
                else if (rc == 0){
                    acked++;
                }
            }
            if(item->numfailed == 0){
                // Awesome! Everyone's caught up. 
                item->num_successfully_acked += acked; 
                item->cur_state = DONE_WAIT;
                goto done_wait_label;
            }
            else{
                //If we are still within waitms timeout, we still have hope! 
                //Modify position of item appropriately in absolute_ts_list
                if((comdb2_time_epochms() - item->end_time) < item->waitms){
                    // Change position of current work item in absolute_ts_list based on new_ts (the next absolute timestamp that this node has to be worked on again 
                    Pthread_mutex_lock(&(work_queue->mutex));
                    item->next_ts = item->end_time + item->waitms ;
                    listc_rfl(&work_queue->absolute_ts_list, item);
                    add_to_absolute_ts_list(item);
                    Pthread_mutex_unlock(&(work_queue->mutex));
                    // We break out and handle next iten in the work queue
                    break;
                }
                else{
                    // We have timed out with numfailed !=0
                    // Go through list of connected nodes one last time to find how many acked or how many failed.
                    // Mark those that failed as incoherent
                    item->numfailed = 0;
                    for(int i=0;i<item->numnodes;i++){
                        if(item->nodelist[i] == item->base_node)
                            continue;      
                        if (item->bdb_state->rep_trace){
                            /*logmsg(LOGMSG_DEBUG,
                                   "+++checking for NEWSEQ from node %s of >= <%s> timeout %d\n",
                                   item->nodelist[i], lsn_to_str(item->str, &(item->seqnum.lsn)), item->waitms);*/
                        }
                        int rc = bdb_wait_for_seqnum_from_node_nowait_int(item->bdb_state, &item->seqnum, item->nodelist[i]);
                        if (bdb_lock_desired(item->bdb_state)) {
                            /*logmsg(LOGMSG_DEBUG,
                                   "+++%s line %d early exit because lock-is-desired\n", __func__,
                                   __LINE__);*/
                            if(item->bdb_state->attr->durable_lsns){
                               item->outrc = BDBERR_NOT_DURABLE;
                            }
                            else{
                               item->outrc = -1;
                            }
                            item->cur_state = COMMIT;
                            goto commit_label;
                        }
                        if (rc == -999){
                            /*logmsg(LOGMSG_DEBUG, "+++node %s hasn't caught up yet, base node "
                                            "was %s",
                                    item->nodelist[i],item->base_node);*/
                            item->numfailed++;
                            // Extract seqnum
                            Pthread_mutex_lock(&(item->bdb_state->seqnum_info->lock));
                            item->nodegen = item->bdb_state->seqnum_info->seqnums[nodeix(item->nodelist[i])].generation;
                            item->nodelsn = item->bdb_state->seqnum_info->seqnums[nodeix(item->nodelist[i])].lsn;
                            Pthread_mutex_unlock(&(item->bdb_state->seqnum_info->lock));
                            // We now mark the node incoherent
                            Pthread_mutex_lock(&(item->bdb_state->coherent_state_lock));
                            if(item->bdb_state->coherent_state[nodeix(item->nodelist[i])] == STATE_COHERENT){
                                defer_commits(item->bdb_state, item->nodelist[i], __func__);
                                if(item->bdb_state->attr->catchup_on_commit && item->bdb_state->attr->catchup_window){
                                    item->masterlsn = &(item->bdb_state->seqnum_info->seqnums[nodeix(item->bdb_state->repinfo->master_host)].lsn);
                                    item->cntbytes = subtract_lsn(item->bdb_state, item->masterlsn, &(item->nodelsn));
                                    set_coherent_state(item->bdb_state, item->nodelist[i], (item->cntbytes < item->bdb_state->attr->catchup_window)?STATE_INCOHERENT_WAIT: STATE_INCOHERENT,__func__, __LINE__);
   
                                }
                                else{
                                    set_coherent_state(item->bdb_state, item->nodelist[i], STATE_INCOHERENT,__func__, __LINE__);
                                }
                                // change next_commit_timestamp for the work queue, if new value of coherency_commit_timestamp is larger,
                                //  than current value of coherency_commit_timestamp 
                                work_queue->next_commit_timestamp = (work_queue->next_commit_timestamp < coherency_commit_timestamp)?coherency_commit_timestamp:work_queue->next_commit_timestamp;
                                item->bdb_state->last_downgrade_time[nodeix(item->nodelist[i])] = gettimeofday_ms();
                                item->bdb_state->repinfo->skipsinceepoch = comdb2_time_epoch();
                            }
                            Pthread_mutex_unlock(&(item->bdb_state->coherent_state_lock));
                            
                        }
                        else if (rc == 0){
                            item->num_successfully_acked++;
                        }
                    }
                    item->cur_state = DONE_WAIT;
                    goto done_wait_label;
                }
            }
        case DONE_WAIT: done_wait_label:
            if (!item->numfailed && !item->numskip && !item->numwait &&
                item->bdb_state->attr->remove_commitdelay_on_coherent_cluster &&
                item->bdb_state->attr->commitdelay) {
                /*logmsg(LOGMSG_DEBUG, "+++Cluster is in sync, removing commitdelay\n");*/
                item->bdb_state->attr->commitdelay = 0;
            }

            /*if (item->numfailed) {
                item->outrc = -1;
            }*/

            int now = comdb2_time_epochms();
            Pthread_mutex_lock(&(work_queue->mutex));
            if(now > work_queue->next_commit_timestamp){
                Pthread_mutex_unlock(&(work_queue->mutex));
                // We're safe to commit and return control to client
                item->cur_state = COMMIT;
                goto commit_label;
            }
            else{
                item->cur_state = COMMIT;
                item->next_ts = work_queue->next_commit_timestamp;
                listc_rfl(&work_queue->absolute_ts_list, item);
                add_to_absolute_ts_list(item);
                Pthread_mutex_unlock(&(work_queue->mutex));
                break;
            }
        case COMMIT: commit_label:
                if (item->outrc && (!item->sorese.rcout || item->outrc == ERR_NOT_DURABLE))
                    item->sorese.rcout = item->outrc;

                int sorese_rc = item->outrc;
                if (item->outrc == 0 && item->sorese.rcout == 0 &&
                    item->errstat.errval == COMDB2_SCHEMACHANGE_OK) {
                    // pretend error happend to get errstat shipped to replicant
                    sorese_rc = 1;
                } else {
                    item->errstat.errval = item->sorese.rcout;
                }

                if (item->sorese.rqid == 0)
                    abort();
                /*logmsg(LOGMSG_USER, "Calling osql_comm_signal_sqlthr_rc from %s:%d with sorese_rc: %d\n", __func__, __LINE__, sorese_rc);*/
                osql_comm_signal_sqlthr_rc(&item->sorese, &item->errstat, sorese_rc);
            item->cur_state = FREE; 
            break;
    }// End of Switch
}

void *queue_processor(void *arg){
    struct seqnum_wait *item = NULL;
    struct seqnum_wait *next_item = NULL;
    struct timespec waittime;
    int wait_rc = 0;
    int now = 0;
    extern uint64_t new_lsns;
    extern bdb_state_type *gbl_bdb_state;
    int local_new_lsns = 0;
    Pthread_mutex_lock(&gbl_bdb_state->seqnum_info->lock);
    local_new_lsns = new_lsns;
    //logmsg(LOGMSG_USER,"+++captured value of local_new_lsns:%d\n", local_new_lsns);
    Pthread_mutex_unlock(&gbl_bdb_state->seqnum_info->lock);

    //logmsg(LOGMSG_DEBUG,"+++Starting seqnum_wait worker thread\n");
    while(1){
        Pthread_mutex_lock(&(work_queue->mutex));
        while(listc_size(&(work_queue->lsn_list))==0){
            //logmsg(LOGMSG_DEBUG,"+++LSN list is empty.... Waiting for work item\n");
            Pthread_cond_wait(&(work_queue->cond),&(work_queue->mutex));
        }
        Pthread_mutex_unlock(&(work_queue->mutex));
        //logmsg(LOGMSG_DEBUG,"+++Finally! got a work item\n");

        // if timed_wait below resulted in a timeout, we need to go over absolute_ts_list
        if(wait_rc == ETIMEDOUT){
            //logmsg(LOGMSG_DEBUG,"+++Timed out on conditional wait.. Going over absolute ts list\n");
            Pthread_mutex_lock(&(work_queue->mutex));
            item = LISTC_TOP(&(work_queue->absolute_ts_list));
            Pthread_mutex_unlock(&(work_queue->mutex));
            now = comdb2_time_epochms();
            while(item!=NULL && (item->next_ts <= now)){
                if(item->cur_state == FREE){
                    //logmsg(LOGMSG_DEBUG, "+++Done processing work item: %d:%d... Freeing it\n",item->seqnum.lsn.file, item->seqnum.lsn.offset);
                    Pthread_mutex_lock(&(work_queue->mutex));
                    next_item = item->lsn_lnk.next;
                    Pthread_mutex_unlock(&(work_queue->mutex));
                    free_work_item(item);
                    item = next_item;
                    continue;
                }
                //logmsg(LOGMSG_DEBUG, "+++Processing work item: %d:%d, in state: %d\n",item->seqnum.lsn.file, item->seqnum.lsn.offset,item->cur_state);
                process_work_item(item);
                Pthread_mutex_lock(&(work_queue->mutex));
                item = item->absolute_ts_lnk.next;
                Pthread_mutex_unlock(&(work_queue->mutex));
            }
        }
        else{
            // wait_rc == 0, which means either this is the first run of infinite while loop , or..
            // the timed_wait below was signalled...i.e... we got new seqnum -> we iterate over lsn_list upto max_lsn_seen 
            //logmsg(LOGMSG_DEBUG, "+++Either got new seqnum or going through lsn list for the first time\n");
            Pthread_mutex_lock(&(work_queue->mutex));
            item = LISTC_TOP(&(work_queue->lsn_list));
            Pthread_mutex_unlock(&(work_queue->mutex));
            while(item!=NULL){
                Pthread_mutex_lock(&max_lsn_so_far_lk);
                if(log_compare(&item->seqnum.lsn, &max_lsn_so_far) > 0){
                    // We don't need to go down the lsn list any more as the remaining work_items are for Larger LSNs
                    //logmsg(LOGMSG_USER, "Current item LSN larger than max_so_far.. Quit processing LSN list\n");
                    Pthread_mutex_unlock(&max_lsn_so_far_lk);
                    break;
                }
                Pthread_mutex_unlock(&max_lsn_so_far_lk);
                if(item->cur_state == FREE){
                    //logmsg(LOGMSG_DEBUG, "+++Done processing work item: %d:%d... Freeing it\n",item->seqnum.lsn.file, item->seqnum.lsn.offset);
                    Pthread_mutex_lock(&(work_queue->mutex));
                    next_item = item->lsn_lnk.next;
                    Pthread_mutex_unlock(&(work_queue->mutex));
                    free_work_item(item);
                    item = next_item;
                    continue;
                }
                //logmsg(LOGMSG_DEBUG, "+++Processing work item: %d:%d, in state: %d\n",item->seqnum.lsn.file, item->seqnum.lsn.offset,item->cur_state);
                process_work_item(item);
                Pthread_mutex_lock(&(work_queue->mutex));
                item = item->lsn_lnk.next;
                Pthread_mutex_unlock(&(work_queue->mutex));
            }
        }
        // Check first item on absolute_ts_list 
        Pthread_mutex_lock(&(work_queue->mutex));
        item = LISTC_TOP(&(work_queue->absolute_ts_list));
        Pthread_mutex_unlock(&(work_queue->mutex));
        if(item!=NULL){
            int now = comdb2_time_epochms();
            if(now <  item->next_ts){
                // we are early, wait till the earliest time that a node has to be checked
                // Or, till we get signalled by got_new_seqnum_from_node
                Pthread_mutex_lock(&(item->bdb_state->seqnum_info->lock));
                //Before going into a timed wait check if we've got new lsns in the meanwhile.. In this case, we shouldn't go into a timed wait... we should rather go over the lsn list
                if(local_new_lsns != new_lsns){
                    //We got new lsns in the meanwhile... Go over LSN list.
                    //logmsg(LOGMSG_USER,"Not going into timed wait as we got new lsns\n");
                    local_new_lsns = new_lsns;
                    Pthread_mutex_unlock(&item->bdb_state->seqnum_info->lock);
                    wait_rc = 0;
                    continue;
                }
                setup_waittime(&waittime, item->next_ts-now);
                //logmsg(LOGMSG_DEBUG, "+++Current time :%d before earliest time a work item has to be checked:%d ... going into timedcondwait\n",now,item->next_ts);
                wait_rc = pthread_cond_timedwait(&(item->bdb_state->seqnum_info->cond),
                                            &(item->bdb_state->seqnum_info->lock), &waittime); 
                local_new_lsns = new_lsns;
               // if(wait_rc !=   ETIMEDOUT){
                    Pthread_mutex_unlock(&(item->bdb_state->seqnum_info->lock));
            }
            else{
                // We are at(or have crossed) the smallest next_ts in absolute_ts_list
                wait_rc = ETIMEDOUT;
            }
        }
        else{
            //Nothing on the lists... reset wait_rc
            wait_rc = 0;
        }
    }
}

void seqnum_wait_cleanup(){
    struct seqnum_wait *item = NULL;

    //FREE THE WORK QUEUE
    Pthread_mutex_lock(&work_queue->mutex);
    LISTC_FOR_EACH(&work_queue->absolute_ts_list,item,absolute_ts_lnk)
    {
        free(listc_rfl(&work_queue->absolute_ts_list, item));
    }

    LISTC_FOR_EACH(&work_queue->lsn_list,item,lsn_lnk)
    {
        free(listc_rfl(&work_queue->lsn_list, item));
    }
    //listc_free(&work_queue->absolute_ts_list);
    //listc_free(&work_queue->lsn_list);
    Pthread_mutex_unlock(&work_queue->mutex);
    Pthread_mutex_destroy(&work_queue->mutex);
    Pthread_cond_destroy(&work_queue->cond);
    if(work_queue!=NULL)
        free(work_queue);
    // FREE THE MEM POOL
    Pthread_mutex_lock(&seqnum_wait_queue_pool_lk);
    pool_free(seqnum_wait_queue_pool);
    Pthread_mutex_unlock(&seqnum_wait_queue_pool_lk);
    Pthread_mutex_destroy(&seqnum_wait_queue_pool_lk);
}
