/*
   Copyright 2019 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */


#include <pthread.h>
#include <sys/time.h>
#include "locks_wrap.h"
#ifdef TIMING_ACCOUNTING

#include "plhash.h"

static hash_t *htimes = NULL;
unsigned long long totaltime;
unsigned long long totalcount;

const char *CHR_IXADDK = "CHR_IXADDK";
const char *CHR_DATADD = "CHR_DATADD";
const char *CHR_TMPSVOP = "CHR_TMPSVOP";


typedef struct {
    const char *name;
    unsigned long long utime;
    unsigned long long worstutime;
    unsigned long long count;
} name_time_pair_t;

__thread int already_timing;

// return timediff in microseconds (us) from tv passed in
int chrono_stop(struct timeval *tv)
{
    struct timeval tmp;
    gettimeofday(&tmp, NULL);
    int sec_part = (tmp.tv_sec - tv->tv_sec)*1000000;
    int usec_part = (tmp.tv_usec - tv->tv_usec);

    return sec_part + usec_part;
}

//hash add happens at startup so no need for locks later
static void add_to_hash(const char *name)
{
    if (!htimes)
        abort();
    name_time_pair_t *ptr;
    if ((ptr = hash_find_readonly(htimes, &name)) == 0) {
        ptr = malloc(sizeof(name_time_pair_t));
        ptr->name = strdup(name);
        ptr->utime = 0;
        ptr->worstutime = 0;
        ptr->count = 0;
        hash_add(htimes, ptr);
    }
}
// add time accounting to appropriate slot
void accumulate_time(const char *name, unsigned int us)
{
    //printf("%s adding %s\n", __func__, name);
    name_time_pair_t *ptr;
    if ((ptr = hash_find_readonly(htimes, &name)) == 0) {
        return; // not in hash
    }

    // try without atomics first, we don't need precise
    ptr->utime += us;
    ptr->count++;
    if (ptr->worstutime < us) ptr->worstutime = us;
}

void reset_time_accounting(const char *name)
{
    if (!htimes) {
        return;
    }
    name_time_pair_t *ptr;
    if ((ptr = hash_find_readonly(htimes, &name)) != 0) {
        ptr->utime = 0;
        ptr->worstutime = 0;
        ptr->count = 0;
    }
}
static int reset_time(void *obj, void *unused)
{
    name_time_pair_t *ptr = obj;
    ptr->utime = 0;
    ptr->worstutime = 0;
    ptr->count = 0;
    return 0;

}
void reset_all_time_accounting()
{
    if (!htimes) {
        return;
    }
    hash_for(htimes, reset_time, NULL);
}


void print_time_accounting(const char *name)
{
    if (!htimes) {
        return;
    }
    name_time_pair_t *ptr;
    if ((ptr = hash_find_readonly(htimes, &name)) != 0) {
        logmsg(LOGMSG_USER, "Timing information for %s: %lluus\n", ptr->name, ptr->utime);
    }
}

static int print_name_time_pair(void *obj, void *unused)
{
    name_time_pair_t *ptr = obj;
    logmsg(LOGMSG_USER, "name=%s time=%lluus count=%llu worst=%lluus, avg=%lfus\n", ptr->name, ptr->utime, ptr->count, ptr->worstutime, (double)ptr->utime/ptr->count);
    totaltime += ptr->utime;
    totalcount += ptr->count;
    return 0;

}

void print_all_time_accounting()
{
    if (!htimes) {
        logmsg(LOGMSG_USER, "No timing information is available\n");
        return;
    }
    totaltime = 0;
    totalcount = 0;
    logmsg(LOGMSG_USER, "Timing information:\n");
    hash_for(htimes, print_name_time_pair, NULL);
    logmsg(LOGMSG_USER, "totaltime %llu, totalcount %llu\n", totaltime, totalcount);
}

static int free_name_time_pair(void *obj, void *unused)
{
    free(obj);
    return 0;
}

void cleanup_time_accounting()
{
    if (!htimes) {
        return;
    }
    hash_for(htimes, free_name_time_pair, NULL);
    hash_clear(htimes);
    hash_free(htimes);
    htimes = NULL;
}

void time_accounting_init()
{
    htimes = hash_init_strptr(offsetof(name_time_pair_t, name));
add_to_hash("&apply_lk");
add_to_hash("&appsock_conn_lk");
add_to_hash("&args->mutex");
add_to_hash("&asof_cursor_pool_lk");
add_to_hash("&bdb_asof_current_lsn_mutex");
add_to_hash("&bdb_gbl_ltran_pglogs_mutex");
add_to_hash("&bdb_gbl_recoverable_lsn_mutex");
add_to_hash("&bdb_gbl_timestamp_lsn_mutex");
add_to_hash("&(bdb_state->bdblock_debug_lock)");
add_to_hash("&bdb_state->blkseq_lk[stripe]");
add_to_hash("&(bdb_state->children_lock)");
add_to_hash("&(bdb_state->coherent_state_lock)");
add_to_hash("&bdb_state->coherent_state_lock");
add_to_hash("&bdb_state->durable_lsn_lk");
add_to_hash("&(bdb_state->exit_lock)");
add_to_hash("&(bdb_state->gblcontext_lock)");
add_to_hash("&(bdb_state->id_lock)");
add_to_hash("&bdb_state->last_dta_lk");
add_to_hash("&(bdb_state->master_lease_lk)");
add_to_hash("&(bdb_state->numthreads_lock)");
add_to_hash("&bdb_state->pending_broadcast_lock");
add_to_hash("&(bdb_state->repinfo->appseqnum_lock)");
add_to_hash("&(bdb_state->repinfo->elect_mutex)");
add_to_hash("&bdb_state->repinfo->receive_lock");
add_to_hash("&bdb_state->sc_redo_lk");
add_to_hash("&(bdb_state->seed_lock)");
add_to_hash("&(bdb_state->seqnum_info->lock)");
add_to_hash("&bdb_state->seqnum_info->lock");
add_to_hash("&(bdb_state->temp_list_lock)");
add_to_hash("&bdb_state->temp_list_lock");
add_to_hash("&bdb_state->thread_lock_info_list_mutex");
add_to_hash("&bdb_state->translist_lk");
add_to_hash("&blklk");
add_to_hash("&blobmutex");
add_to_hash("btcursor->tmptable->lk");
add_to_hash("&ckp_lst_mtx");
add_to_hash("&clients_relink_req_lk");
add_to_hash("&clients_update_req_lk");
add_to_hash("&(*clnt)->dtran_mtx");
add_to_hash("&clnt->dtran_mtx");
add_to_hash("&clnt_lk");
add_to_hash("&clntlru_mtx");
add_to_hash("&clnt->state_lk");
add_to_hash("&clnt->wait_mutex");
add_to_hash("&clnt->write_lock");
add_to_hash("&commit_stat_lk");
add_to_hash("&common->lock");
add_to_hash("&comp_thd_mutex");
add_to_hash("&conn->mtx");
add_to_hash("&conns->conns[child_num].mtx");
add_to_hash("&conns->conns[conns->row_src].mtx");
add_to_hash("&conns->conns[i].mtx");
add_to_hash("&consumer->mutex");
add_to_hash("&consumer_sqlthds_mutex");
add_to_hash("&csc2_subsystem_mtx");
add_to_hash("&curlk");
add_to_hash("&cur->shadow_tran->pglogs_mutex");
add_to_hash("cur->tmptable->lk");
add_to_hash("&data->s->livesc_mtx");
add_to_hash("&dbenv->lc_cache.lk");
add_to_hash("&dbenv->locked_lsn_lk");
add_to_hash("&dbenv->log_delete_counter_mutex");
add_to_hash("&dbenv->long_trn_mtx");
add_to_hash("&dbenv->ltrans_active_lk");
add_to_hash("&dbenv->ltrans_hash_lk");
add_to_hash("&dbenv->ltrans_inactive_lk");
add_to_hash("&dbenv->mintruncate_lk");
add_to_hash("&(dbenv->prefault_helper.mutex)");
add_to_hash("&(dbenv->prefault_helper.threads[i].mutex)");
add_to_hash("&(dbenv->prefaultiopool.mutex)");
add_to_hash("&dbenv->recover_lk");
add_to_hash("&dbenv->ser_lk");
add_to_hash("&dbmfp->recp_idx_lk");
add_to_hash("&dbmfp->recp_lk_array[i]");
add_to_hash("&dbmfp->recp_lk_array[idx]");
add_to_hash("&dbqueuedb_admin_lk");
add_to_hash("&delay_lock");
add_to_hash("&del_queue_lk");
add_to_hash("&dlock");
add_to_hash("&dohsql_stats_mtx");
add_to_hash("&dump_once_lk");
add_to_hash("&entry->mtx");
add_to_hash("&e->pglogs_lk");
add_to_hash("&e->relinks_lk");
add_to_hash("&eventlog_lk");
add_to_hash("&exiting_lock");
add_to_hash("&fastseedlk");
add_to_hash("&fdb->dbcon_mtx");
add_to_hash("&fdb_dbname_hash_lock");
add_to_hash("&fdb->users_mtx");
add_to_hash("fileid_env->lk");
add_to_hash("&fileid_pglogs_queue_pool_lk");
add_to_hash("&fstdump.lock");
add_to_hash("&gbl_durable_lsn_lk");
add_to_hash("&gbl_fingerprint_hash_mu");
add_to_hash("&gbl_logput_lk");
add_to_hash("&gbl_rep_egen_lk");
add_to_hash("&gbl_sc_lock");
add_to_hash("&gbl_sql_lock");
add_to_hash("&gbl_tunables->mu");
add_to_hash("&gbl_watchdog_kill_mutex");
add_to_hash("&global_dt_mutex");
add_to_hash("&(hash->mutex)");
add_to_hash("&h->lock");
add_to_hash("&hmtx");
add_to_hash("&(host_node_ptr->enquelk)");
add_to_hash("&(host_node_ptr->lock)");
add_to_hash("&(host_node_ptr->throttle_lock)");
add_to_hash("&(host_node_ptr->timestamp_lock)");
add_to_hash("&(host_node_ptr->wait_mutex)");
add_to_hash("&(host_node_ptr->write_lock)");
add_to_hash("&(host_ptr->throttle_lock)");
add_to_hash("&ilatch->lsns_mtx");
add_to_hash("&iq->dbenv->long_trn_mtx");
add_to_hash("&(iq->dbenv->prefault_helper.mutex)");
add_to_hash("&(iq->dbenv->prefault_helper.threads[i].mutex)");
add_to_hash("&(iq.sc->mtx)");
add_to_hash("&kludgelk");
add_to_hash("&latch->lock");
add_to_hash("&lblk");
add_to_hash("&lcmlk");
add_to_hash("&l_entry->pglogs_lk");
add_to_hash("&l_entry->relinks_lk");
add_to_hash("&lk");
add_to_hash("&lk_desired_lock");
add_to_hash("&lnode->lsns_mtx");
add_to_hash("&lock");
add_to_hash("&(lockerid_latches[idx].lock)");
add_to_hash("&lockerid_latches[idx].lock");
add_to_hash("&lockp->lsns_mtx");
add_to_hash("&logdelete_lk");
add_to_hash("&logfile_pglogs_repo_mutex");
add_to_hash("&log_repo->clients_mtx");
add_to_hash("&log_write_lk");
add_to_hash("&lp->lsns_mtx");
add_to_hash("&lstlk");
add_to_hash("&ltran_ent->pglogs_mutex");
add_to_hash("&ltran_pglogs_key_pool_lk");
add_to_hash("&lua_debug_mutex");
add_to_hash("&mach_mtx");
add_to_hash("&master_mem_mtx");
add_to_hash("&mempsync_lk");
add_to_hash("&me.mutex");
add_to_hash("&msgs_mtx");
add_to_hash("&mtx");
add_to_hash("mu");
add_to_hash("&mutex");
add_to_hash("mutexp");
add_to_hash("&mutexp->mutex");
add_to_hash("&(netinfo_ptr->connlk)");
add_to_hash("&(netinfo_ptr->sanclk)");
add_to_hash("&(netinfo_ptr->seqlock)");
add_to_hash("&(netinfo_ptr->watchlk)");
add_to_hash("&nets_list_lk");
add_to_hash("&n->lock");
add_to_hash("&of_list_mtx");
add_to_hash("&ongoing_alter_mtx");
add_to_hash("&op->data_mutex");
add_to_hash("&open_dbs_mtx");
add_to_hash("&open_serial_lock");
add_to_hash("&op->evict_mutex");
add_to_hash("&osqlpf_mutex");
add_to_hash("&out->mutex");
add_to_hash("&owner_mtx");
add_to_hash("&page_flush_lk");
add_to_hash("parent->emit_mutex");
add_to_hash("&parent->temp_list_lock");
add_to_hash("&parent->translist_lk");
add_to_hash("&pglogs_commit_list_pool_lk");
add_to_hash("&pglogs_key_pool_lk");
add_to_hash("&pglogs_logical_key_pool_lk");
add_to_hash("&pglogs_lsn_commit_list_pool_lk");
add_to_hash("&pglogs_lsn_list_pool_lk");
add_to_hash("&pglogs_queue_cursor_pool_lk");
add_to_hash("&pglogs_queue_key_pool_lk");
add_to_hash("&pglogs_queue_lk");
add_to_hash("&pglogs_relink_key_pool_lk");
add_to_hash("&pglogs_relink_list_pool_lk");
add_to_hash("&pgpool_lk");
add_to_hash("&pool_list_lk");
add_to_hash("&pool->mutex");
add_to_hash("&pr_lk");
add_to_hash("&(p_slock->req_lock)");
add_to_hash("&p_slock->req_lock");
add_to_hash("&pthr_mutex");
add_to_hash("&pt->lk");
add_to_hash("&(ptr->enquelk)");
add_to_hash("&(ptr->timestamp_lock)");
add_to_hash("&qlock");
add_to_hash("q->lock");
add_to_hash("&range->t->lk");
add_to_hash("&region->db_lock_lsn_lk");
add_to_hash("&region->ilock_latch_lk");
add_to_hash("&region->lockerid_node_lk");
add_to_hash("&rep_candidate_lock");
add_to_hash("&rep_queue_lock");
add_to_hash("&rp->lk");
add_to_hash("&rp->ltrans->lk");
add_to_hash("&rq->mtx");
add_to_hash("&rq->processor->lk");
add_to_hash("&rules_mutex");
add_to_hash("&sc_async_mtx");
add_to_hash("&sched->mtx");
add_to_hash("&schema_change_in_progress_mutex");
add_to_hash("&schema_change_sbuf2_lock");
add_to_hash("&sc->mtx");
add_to_hash("&sc_resuming_mtx");
add_to_hash("&servbyname_lk");
add_to_hash("&sess->clients_mtx");
add_to_hash("&sess->completed_lock");
add_to_hash("&sess->mtx");
add_to_hash("&s->livesc_mtx");
add_to_hash("&slow_node_check_lk");
add_to_hash("&s->mtx");
add_to_hash("&s->mtxStart");
add_to_hash("&sockpool_lk");
add_to_hash("&spgs.lock");
add_to_hash("&sql_log_lk");
add_to_hash("&stats->lk");
add_to_hash("&st->rawtotals.lk");
add_to_hash("&subnet_mtx");
add_to_hash("&subscription_lk");
add_to_hash("&table_thd_mutex");
add_to_hash("&taglock");
add_to_hash("&tbl->ents_mtx");
add_to_hash("&testguard");
add_to_hash("&thd->lk");
add_to_hash("&thd->lua_thread_mutex");
add_to_hash("&theosql->cancelall_mtx");
add_to_hash("&timerlk");
add_to_hash("&t->lk");
add_to_hash("&t->lock");
add_to_hash("&tmp_open_lock");
add_to_hash("&trace_lock");
add_to_hash("&tran_clnt->dtran_mtx");
add_to_hash("&tran->store_mtx");
add_to_hash("&trighash_lk");
add_to_hash("&trn->log_mtx");
add_to_hash("&trn_repo_mtx");
add_to_hash("uprec->lk");
add_to_hash("&verifylk");
add_to_hash("&vote2_lock");
add_to_hash("&work->common->lock");
add_to_hash("&conns->conns[x].mtx");
add_to_hash("&(ack_state->netinfo->lock)");
add_to_hash("&center->cursors_rwlock");
add_to_hash("&checkboard->rwlock");
add_to_hash("&clientstats_lk");
add_to_hash("&commit_lock");
add_to_hash("&crons.rwlock");
add_to_hash("&db->consumer_lk");
add_to_hash("&dbenv->dbreglk");
add_to_hash("&dbenv->recoverlk");
add_to_hash("&fdb->h_rwlock");
add_to_hash("&fdbs.arr_lock");
add_to_hash("&gbl_block_qconsume_lock");
add_to_hash("&gbl_dbreg_log_lock");
add_to_hash("&iq->usedb->sc_live_lk");
add_to_hash("lock_handle->bdb_lock");
add_to_hash("&log_repo->tail_lock");
add_to_hash("&min_trunc_lk");
add_to_hash("&(netinfo->lock)");
add_to_hash("&(netinfo_ptr->lock)");
add_to_hash("&proxy_config_lk");
add_to_hash("&qcur->queue->queue_lk");
add_to_hash("&queue->queue_lk");
add_to_hash("&schema_lk");
add_to_hash("&stat->hshlck");
add_to_hash("&taglock");
add_to_hash("&thedb_lock");
add_to_hash("&theosql->hshlck");
add_to_hash("&usedb->sc_live_lk");
add_to_hash("&views_lk");
add_to_hash("&peer_lk");

add_to_hash("&g_mutex");
add_to_hash("&(region)->dd_mtx.mtx");
add_to_hash("&(region)->lockers_mtx.mtx");
add_to_hash("&(region)->locker_tab_mtx[(holder_locker->partition)].mtx");
add_to_hash("&(region)->locker_tab_mtx[(lkr_partition)].mtx");
add_to_hash("&(region)->locker_tab_mtx[(lockerp->partition)].mtx");
add_to_hash("&(region)->locker_tab_mtx[(lpartition)].mtx");
add_to_hash("&(region)->locker_tab_mtx[(old_locker->partition)].mtx");
add_to_hash("&(region)->locker_tab_mtx[(partition)].mtx");
add_to_hash("&(region)->locker_tab_mtx[(sh_locker->partition)].mtx");
add_to_hash("&(region)->locker_tab_mtx[(sh_parent->partition)].mtx");
add_to_hash("&(region)->obj_tab_mtx[(lock->partition)].mtx");
add_to_hash("&region->obj_tab_mtx[lock->partition].mtx");
add_to_hash("&(region)->obj_tab_mtx[(lpartition)].mtx");
add_to_hash("&region->obj_tab_mtx[lpartition].mtx");
add_to_hash("&(region)->obj_tab_mtx[(partition)].mtx");
add_to_hash("&region->obj_tab_mtx[partition].mtx");
add_to_hash("&splk");
add_to_hash("&stop_thds_time_lk");
}


#else

#ifndef NDEBUG


#include "comdb2_atomic.h"
#include "time_accounting.h"

const char *CHR_NAMES[] = {"ix_addk", "dat_add", "temp_table_saveop"};

uint64_t gbl_chron_times[CHR_MAX];

// add time accounting to appropriate slot
void accumulate_time(int el, unsigned int us)
{
    ATOMIC_ADD64(gbl_chron_times[el], us);
}

void reset_time_accounting(int el)
{
    XCHANGE64(gbl_chron_times[el], 0);
}

void print_time_accounting(int el)
{
    logmsg(LOGMSG_USER, "Timing information for %s: %"PRIu64"us\n", CHR_NAMES[el],
           gbl_chron_times[el]);
}

void print_all_time_accounting()
{
    extern int gbl_create_mode;
    if (gbl_create_mode) return;
    logmsg(LOGMSG_USER, "Timing information:\n");
    for (int i = 0; i < CHR_MAX; i++) {
        logmsg(LOGMSG_USER, "%s: %"PRIu64"us\n", CHR_NAMES[i], ATOMIC_LOAD64(gbl_chron_times[i]));
    }
}

#endif

#endif
