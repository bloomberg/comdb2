/*
   Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

/* the helper threads that work behind the scenes */

#include <pthread.h>
#include <sys/poll.h>
#include <unistd.h>

#include "ctrace.h"
#include "bdb_int.h"
#include "locks.h"
#include "comdb2_atomic.h"
#include "memory_sync.h"
#include "autoanalyze.h"
#include "logmsg.h"
#include "thrman.h"
#include <locks_wrap.h>


int db_is_exiting(void);
int send_myseqnum_to_master_udp(bdb_state_type *bdb_state);
void *rep_catchup_add_thread(void *arg);

extern pthread_attr_t gbl_pthread_attr_detached;
extern int gbl_is_physical_replicant;

void udp_backup(int dummyfd, short what, void *arg)
{
    bdb_state_type *bdb_state = arg;
    repinfo_type *repinfo = bdb_state->repinfo;
    if (!gbl_udp) return;
    if (repinfo->master_host == repinfo->myhost) return;
    send_myseqnum_to_master(bdb_state, 1);
}

void auto_analyze(int dummyfd, short what, void *arg)
{
    bdb_state_type *bdb_state = arg;
    repinfo_type *repinfo = bdb_state->repinfo;
    if (!bdb_state->attr->autoanalyze) return;
    if (repinfo->master_host != repinfo->myhost) return;
    pthread_t t;
    Pthread_create(&t, &gbl_pthread_attr_detached, auto_analyze_main, NULL);
}

/* this thread serves two purposes:
 * 1. on replicants it sends acks via tcp in case
 *    of too many dropped udp pakcets
 * 2. on master it runs autoanalyze
 */
void *udpbackup_and_autoanalyze_thd(void *arg)
{
    unsigned pollms = 500;
    unsigned count = 0;
    thrman_register(THRTYPE_GENERIC);
    thread_started("udpbackup_and_autoanalyze");

    bdb_state_type *bdb_state = arg;
    while (!db_is_exiting()) {
        ++count;
        udp_backup(-1, 0, bdb_state);
        if ((gbl_is_physical_replicant == 0) /* No point running analyze on physical replicants */ &&
            (count % ((bdb_state->attr->chk_aa_time * 1001) / pollms) == 0)) {
            auto_analyze(-1, 0, bdb_state);
        }
        poll(NULL, 0, pollms);
    }
    return NULL;
}

/* try to with atomic compare-and-exchange to set thread_running to 1
 * if CAS is successful, we are the only (first) such thread and returns 1
 * if CAS is UNsuccessful, another thread is already running and we return 0
 */
static inline int try_set(int *thread_running)
{
    int zero = 0;
    return CAS32(*thread_running, zero, 1);
}

void *memp_trickle_thread(void *arg)
{
    unsigned int time;
    bdb_state_type *bdb_state;
    static int memp_trickle_thread_running = 0;
    int nwrote;
    int rc;

    if (try_set(&memp_trickle_thread_running) == 0)
        return NULL;

    bdb_state = (bdb_state_type *)arg;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    while (!bdb_state->after_llmeta_init_done)
        sleep(1);

    thrman_register(THRTYPE_GENERIC);
    thread_started("bdb memptrickle");

    bdb_thread_event(bdb_state, 1);

    while (!bdb_state->passed_dbenv_open)
        sleep(1);

    while (!db_is_exiting()) {
        BDB_READLOCK("memp_trickle_thread");

        /* time is in usecs, memptricklemsecs is in msecs */
        time = bdb_state->attr->memptricklemsecs * 1000;

    again:
        rc = bdb_state->dbenv->memp_trickle(
            bdb_state->dbenv, bdb_state->attr->memptricklepercent, &nwrote, 1);
        if (rc == DB_LOCK_DESIRED) {
            BDB_RELLOCK();
            sleep(1);
            BDB_READLOCK("memp_trickle_thread");
            goto again;
        } else if (rc == 0) {
            if (nwrote != 0) {
                goto again;
            }
        }

        BDB_RELLOCK();

        if (db_is_exiting())
            break;
        usleep(time);
    }

    bdb_thread_event(bdb_state, 0);
    logmsg(LOGMSG_DEBUG, "memp_trickle_thread: exiting\n");
    return NULL;
}

void *deadlockdetect_thread(void *arg)
{
    bdb_state_type *bdb_state;
    int aborted;

    bdb_state = (bdb_state_type *)arg;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    while (!bdb_state->after_llmeta_init_done)
        sleep(1);

    thread_started("bdb deadlockdetect");

    bdb_thread_event(bdb_state, 1);

    while (1) {
        int rc;
        int policy;

        BDB_READLOCK("deadlockdetect thread");

        policy = DB_LOCK_MINWRITE;

        if (bdb_state->attr->deadlock_most_writes)
            policy = DB_LOCK_MAXWRITE;
        if (bdb_state->attr->deadlock_youngest_ever)
            policy = DB_LOCK_YOUNGEST_EVER;

        if (db_is_exiting()) {
            logmsg(LOGMSG_DEBUG, "deadlockdetect_thread: exiting\n");

            BDB_RELLOCK();
            bdb_thread_event(bdb_state, 0);
            pthread_exit(NULL);
        }

        aborted = 0;

        rc = bdb_state->dbenv->lock_detect(bdb_state->dbenv, 0, policy,
                                           &aborted);

        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "lock_detect rc %d\n", rc);
            exit(1);
        }

        BDB_RELLOCK();

        if (aborted != 0) /* loop hard if aborted */
            poll(0, 0, bdb_state->attr->deadlockdetectms);
        else
            sleep(1);
    }
}

void *master_lease_thread(void *arg)
{
    int pollms, renew, lease_time;
    bdb_state_type *bdb_state = (bdb_state_type *)arg;
    repinfo_type *repinfo = bdb_state->repinfo;
    static int master_lease_thread_running = 0;

    if (try_set(&master_lease_thread_running) == 0)
        return NULL;

    bdb_state->master_lease_thread = pthread_self();

    assert(!bdb_state->parent);
    thrman_register(THRTYPE_GENERIC);
    thread_started("bdb master lease");
    bdb_thread_event(bdb_state, BDBTHR_EVENT_START_RDWR);
    logmsg(LOGMSG_DEBUG, "%s starting\n", __func__);

    while (!db_is_exiting() && (lease_time = bdb_state->attr->master_lease) != 0) {
        if (repinfo->master_host != repinfo->myhost) {
            send_myseqnum_to_master_udp(bdb_state);
        }

        pollms = ((renew = bdb_state->attr->master_lease_renew_interval) &&
                  renew < lease_time)
                     ? renew
                     : (lease_time / 3);
        poll(0, 0, pollms);
    }

    logmsg(LOGMSG_DEBUG, "%s exiting\n", __func__);
    bdb_thread_event(bdb_state, BDBTHR_EVENT_DONE_RDWR);

    bdb_state->master_lease_thread = 0;
    master_lease_thread_running = 0;
    return NULL;
}

void *coherency_lease_thread(void *arg)
{
    int pollms, renew, lease_time, inc_wait, add_interval;
    static time_t last_add_record = 0;
    bdb_state_type *bdb_state = (bdb_state_type *)arg;
    repinfo_type *repinfo = bdb_state->repinfo;
    pthread_t tid;
    static int coherency_thread_running = 0;

    if (try_set(&coherency_thread_running) == 0)
        return NULL;

    bdb_state->coherency_lease_thread = pthread_self();

    assert(!bdb_state->parent);
    thrman_register(THRTYPE_GENERIC);
    thread_started("bdb coherency lease");
    bdb_thread_event(bdb_state, BDBTHR_EVENT_START_RDWR);
    logmsg(LOGMSG_DEBUG, "%s starting\n", __func__);

    while (!db_is_exiting() && (lease_time = bdb_state->attr->coherency_lease)) {
        inc_wait = 0;
        uint32_t current_gen, durable_gen;
        DB_LSN durable_lsn;
        BDB_READLOCK(__func__);
        if (db_is_exiting()) {
            BDB_RELLOCK();
            break;
        }
        if (repinfo->master_host == repinfo->myhost) {
            send_coherency_leases(bdb_state, lease_time, &inc_wait);

            if (bdb_state->attr->durable_lsns) {
                /* See if master has written a durable LSN */
                bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &current_gen);
                bdb_state->dbenv->get_durable_lsn(bdb_state->dbenv,
                                                  &durable_lsn, &durable_gen);

                /* Insert a record if it hasn't */
                if (durable_gen != current_gen) {
                    inc_wait = 1;
                }
            }
        }
        if (inc_wait && (add_interval = bdb_state->attr->add_record_interval)) {
            time_t now = time(NULL);
            if ((now - last_add_record) >= add_interval) {
                pthread_create(&tid, &gbl_pthread_attr_detached,
                               rep_catchup_add_thread, bdb_state);
                last_add_record = now;
            }
        }
        BDB_RELLOCK();
        pollms = ((renew = bdb_state->attr->lease_renew_interval) &&
                  renew < lease_time)
                     ? renew
                     : (lease_time / 3);

        if (db_is_exiting())
            break;
        poll(0, 0, pollms);
    }

    logmsg(LOGMSG_DEBUG, "%s exiting\n", __func__);
    bdb_thread_event(bdb_state, BDBTHR_EVENT_DONE_RDWR);

    bdb_state->coherency_lease_thread = 0;
    coherency_thread_running = 0;
    return NULL;
}

void *logdelete_thread(void *arg)
{
    bdb_state_type *bdb_state = (bdb_state_type *)arg;
    if (bdb_state->parent) bdb_state = bdb_state->parent;

    while (!bdb_state->after_llmeta_init_done)
        sleep(1);

    thrman_register(THRTYPE_GENERIC);
    thread_started("bdb logdelete");

    bdb_thread_event(bdb_state, 1);
    time_t last_run_time = 0;

    while (!db_is_exiting()) {
        time_t now = time(NULL);
        int run_interval = bdb_state->attr->logdelete_run_interval;
        run_interval = (run_interval <= 0 ? 30 : run_interval);

        if ((now - last_run_time) >= run_interval) {
            delete_log_files(bdb_state);
            last_run_time = now;
        }
        sleep(1);
    }

    logmsg(LOGMSG_DEBUG, "logdelete_thread: exiting\n");
    bdb_thread_event(bdb_state, 0);
    return NULL;
}

extern int gbl_rowlocks;
extern unsigned long long osql_log_time(void);

int64_t gbl_last_checkpoint_ms;
int64_t gbl_total_checkpoint_ms;
int gbl_checkpoint_count;
int gbl_cache_flush_interval = 30;
int backend_opened(void);

void *checkpoint_thread(void *arg)
{
    int rc, now;
    int checkpointtime;
    int checkpointtimepoll;
    int checkpointrand;
    int loaded_cache = 0, last_cache_dump = 0;
    bdb_state_type *bdb_state;
    int start, end;
    int total_sleep_msec;
    unsigned long long end_sleep_time_msec;
    unsigned long long crt_time_msec;
    DB_LSN logfile;
    DB_LSN crtlogfile;
    int broken;
    static int checkpoint_thd_running = 0;

    if (try_set(&checkpoint_thd_running) == 0)
        return NULL;

    thrman_register(THRTYPE_GENERIC);
    thread_started("bdb checkpoint");

    bdb_state = (bdb_state_type *)arg;
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    while (!bdb_state->after_llmeta_init_done)
        sleep(1);

    bdb_thread_event(bdb_state, 1);

    while (!db_is_exiting()) {
        BDB_READLOCK("checkpoint_thread");
        checkpointtime = bdb_state->attr->checkpointtime;
        checkpointrand = bdb_state->attr->checkpointrand;
        checkpointtimepoll = bdb_state->attr->checkpointtimepoll;

        broken = bdb_state->dbenv->log_get_last_lsn(bdb_state->dbenv, &logfile);
        if (broken) {
            logmsg(LOGMSG_ERROR, "%s failed in log_get_last_lsn rc=%d\n", __func__,
                    broken);
        }

        if (gbl_rowlocks && !backend_opened()) {
            BDB_RELLOCK();
            sleep(1);
            continue;
        }

        /* Record the start time of the checkpoint operation.  If the checkpoint
         * hangs (this has happened) then another thread will use this to raise
         * an alarm. */
        start = comdb2_time_epochms();
        bdb_state->checkpoint_start_time = comdb2_time_epoch();
        MEMORY_SYNC;

        rc = ll_checkpoint(bdb_state, 0);

        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "checkpoint failed rc %d\n", rc);
        }

        /* This is spawned before we open tables- don't repopulate the
         * cache until the backend has opened */
        if ((gbl_cache_flush_interval > 0) &&
            ((now = time(NULL)) - last_cache_dump) > gbl_cache_flush_interval) {
            if (!loaded_cache) {
                bdb_state->dbenv->memp_load_default(bdb_state->dbenv);
                loaded_cache = 1;
            } else {
                bdb_state->dbenv->memp_dump_default(bdb_state->dbenv, 0);
                last_cache_dump = now;
            }
        }

        end = comdb2_time_epochms();
        bdb_state->checkpoint_start_time = 0;
        MEMORY_SYNC;
        ctrace("checkpoint (scheduled) took %d ms\n", end - start);
        gbl_last_checkpoint_ms = (end - start);
        gbl_total_checkpoint_ms += gbl_last_checkpoint_ms;
        gbl_checkpoint_count++;

        BDB_RELLOCK();

        total_sleep_msec = 1000 * (checkpointtime + (rand() % checkpointrand));

        if (broken) {
            int ss = total_sleep_msec / 1000;
            for (int i = 0; i < ss && !db_is_exiting(); i++)
                sleep(1);
        } else {
            if (checkpointtimepoll > total_sleep_msec) {
                checkpointtimepoll = total_sleep_msec;
            }
            end_sleep_time_msec = osql_log_time() + total_sleep_msec;
            crt_time_msec = 0;

            do {
                if (checkpointtimepoll > end_sleep_time_msec - crt_time_msec) {
                    checkpointtimepoll = end_sleep_time_msec - crt_time_msec;
                }

                poll(0, 0, checkpointtimepoll);
                if (db_is_exiting())
                    break;

                BDB_READLOCK("checkpoint_thread2");
                broken = bdb_state->dbenv->log_get_last_lsn(bdb_state->dbenv,
                                                            &crtlogfile);
                BDB_RELLOCK();

                if (!broken) {
                    /* if we jumped to a new log file, trickle a checkpoint */
                    if (logfile.file != crtlogfile.file)
                        break;
                }

                crt_time_msec = osql_log_time();
            } while (crt_time_msec < end_sleep_time_msec);
        }
    }

    logmsg(LOGMSG_DEBUG, "checkpoint_thread: exiting\n");
    bdb_thread_event(bdb_state, 0);
    return NULL;
}

int bdb_get_checkpoint_time(bdb_state_type *bdb_state)
{
    int start;
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;
    start = bdb_state->checkpoint_start_time;
    if (start != 0)
        start = comdb2_time_epoch() - start;
    return start;
}

void *lwm_printer_thd(void *p)
{
    int rc;
    bdb_state_type *bdb_state = p;
    int numtrans;
    int bdberr;
    DB_LSN lsn;
    DB_LSN *active_lsns = NULL;
    int trannum;

    for (;;) {
        bdb_get_lsn_lwm(bdb_state, &lsn);
        logmsg(LOGMSG_DEBUG, "lsn %d:%d\n", lsn.file, lsn.offset);
        rc = bdb_get_active_logical_transaction_lsns(bdb_state, &active_lsns,
                                                     &numtrans, &bdberr, NULL);
        if (rc)
            logmsg(LOGMSG_ERROR, "unexpected error getting lsn list: rc %d bdberr %d\n", rc,
                   bdberr);
        for (trannum = 0; trannum < numtrans; trannum++)
            logmsg(LOGMSG_DEBUG, "%u:%u ", active_lsns[trannum].file,
                   active_lsns[trannum].offset);
        if (numtrans > 0)
            logmsg(LOGMSG_DEBUG, "\n");
        if (active_lsns)
            free(active_lsns);
        poll(0, 0, 50);
    }
}
