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

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/poll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stddef.h>
#include <str0.h>

#include <build/db.h>
#include <epochlib.h>

#include <ctrace.h>

#include <net.h>
#include "bdb_int.h"
#include "locks.h"

#include <memory_sync.h>
#include <plbitlib.h> /* for bset/btst */
#include <autoanalyze.h>
#include <logmsg.h>

int db_is_stopped(void);

void *udp_backup(void *arg)
{
    unsigned pollms = 500; // factor of 1000
    bdb_state_type *bdb_state = arg;
    repinfo_type *repinfo = bdb_state->repinfo;
    while (!db_is_stopped()) {
        if (repinfo->master_host != repinfo->myhost && gbl_udp) {
            send_myseqnum_to_master(bdb_state, 1);
        }
        poll(NULL, 0, pollms);
    }
    return NULL;
}

extern pthread_attr_t gbl_pthread_attr_detached;

/* this thread serves two purposes:
 * 1. on replicants it sends acks via tcp in case
 *    of too many dropped udp pakcets
 * 2. on master it runs autoanalyze
 */
void *udpbackup_and_autoanalyze_thd(void *arg)
{
    unsigned pollms = 500;
    unsigned count = 0;
    bdb_state_type *bdb_state = arg;
    repinfo_type *repinfo = bdb_state->repinfo;
    while (!db_is_stopped()) {
        ++count;
        if (repinfo->master_host != repinfo->myhost) { // not master
            if (gbl_udp)
                send_myseqnum_to_master(bdb_state, 1);
        } else if (bdb_state->attr->autoanalyze &&
                   count % ((bdb_state->attr->chk_aa_time * 1000) / pollms) ==
                       0) { // do this is on master if autoanalyze on
            pthread_t autoanalyze;
            pthread_create(&autoanalyze, &gbl_pthread_attr_detached,
                           auto_analyze_main, NULL);
        }

        poll(NULL, 0, pollms);
    }
    return NULL;
}

void *memp_trickle_thread(void *arg)
{
    unsigned int time;
    bdb_state_type *bdb_state;
    int nwrote;
    int rc;

    bdb_state = (bdb_state_type *)arg;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    while (!bdb_state->after_llmeta_init_done)
        sleep(1);

    thread_started("bdb memptrickle");

    bdb_thread_event(bdb_state, 1);

    while (!bdb_state->passed_dbenv_open)
        sleep(1);

    while (!db_is_stopped()) {
        int t1, t2;

        BDB_READLOCK("memp_trickle_thread");

        /* time is in usecs, memptricklemsecs is in msecs */
        time = bdb_state->attr->memptricklemsecs * 1000;

    again:
        rc = bdb_state->dbenv->memp_trickle(
            bdb_state->dbenv, bdb_state->attr->memptricklepercent, &nwrote, 1);
        if (rc == 0) {
            if (rc == DB_LOCK_DESIRED) {
                BDB_RELLOCK();
                sleep(1);
                BDB_READLOCK("memp_trickle_thread");
            }
            if (nwrote != 0) {
                goto again;
            }
        }

        BDB_RELLOCK();

        if (db_is_stopped())
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
        int time_now;
        int policy;

        BDB_READLOCK("deadlockdetect thread");

        policy = DB_LOCK_MINWRITE;

        if (bdb_state->attr->deadlock_most_writes)
            policy = DB_LOCK_MAXWRITE;
        if (bdb_state->attr->deadlock_youngest_ever)
            policy = DB_LOCK_YOUNGEST_EVER;

        if (db_is_stopped()) {
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
    static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
    static int have_master_lease_thread = 0;
    bdb_state_type *bdb_state = (bdb_state_type *)arg;
    repinfo_type *repinfo = bdb_state->repinfo;

    pthread_mutex_lock(&lk);
    if (have_master_lease_thread) {
        pthread_mutex_unlock(&lk);
        return NULL;
    } else {
        have_master_lease_thread = 1;
        bdb_state->master_lease_thread = pthread_self();
        pthread_mutex_unlock(&lk);
    }

    assert(!bdb_state->parent);
    thread_started("bdb master lease");
    bdb_thread_event(bdb_state, BDBTHR_EVENT_START_RDWR);
    logmsg(LOGMSG_DEBUG, "%s starting\n", __func__);

    while (!db_is_stopped() &&
           (lease_time = bdb_state->attr->master_lease) != 0) {
        if (repinfo->master_host != repinfo->myhost) {
            int send_myseqnum_to_master_udp(bdb_state_type * bdb_state);
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
    pthread_mutex_lock(&lk);
    have_master_lease_thread = 0;
    bdb_state->master_lease_thread = 0;
    pthread_mutex_unlock(&lk);
    return NULL;
}

void *coherency_lease_thread(void *arg)
{
    int pollms, renew, lease_time, inc_wait, add_interval;
    static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
    static int have_coherency_thread = 0;
    static time_t last_add_record = 0;
    time_t now;
    bdb_state_type *bdb_state = (bdb_state_type *)arg;
    repinfo_type *repinfo = bdb_state->repinfo;
    pthread_t tid;

    pthread_mutex_lock(&lk);
    if (have_coherency_thread) {
        pthread_mutex_unlock(&lk);
        return NULL;
    } else {
        have_coherency_thread = 1;
        bdb_state->coherency_lease_thread = pthread_self();
        pthread_mutex_unlock(&lk);
    }
    assert(!bdb_state->parent);
    thread_started("bdb coherency lease");
    bdb_thread_event(bdb_state, BDBTHR_EVENT_START_RDWR);
    logmsg(LOGMSG_DEBUG, "%s starting\n", __func__);

    while (!db_is_stopped() &&
           (lease_time = bdb_state->attr->coherency_lease)) {
        inc_wait = 0;
        uint32_t current_gen, durable_gen;
        DB_LSN durable_lsn;
        BDB_READLOCK(__func__);
        if (db_is_stopped()) {
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
        now = time(NULL);
        if (inc_wait && (add_interval = bdb_state->attr->add_record_interval)) {
            if ((now - last_add_record) >= add_interval) {
                void *rep_catchup_add_thread(void *arg);
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

        if (db_is_stopped())
            break;
        poll(0, 0, pollms);
    }

    logmsg(LOGMSG_DEBUG, "%s exiting\n", __func__);
    bdb_thread_event(bdb_state, BDBTHR_EVENT_DONE_RDWR);
    pthread_mutex_lock(&lk);
    have_coherency_thread = 0;
    bdb_state->coherency_lease_thread = 0;
    pthread_mutex_unlock(&lk);
    return NULL;
}

void *logdelete_thread(void *arg)
{
    bdb_state_type *bdb_state = (bdb_state_type *)arg;
    if (bdb_state->parent) bdb_state = bdb_state->parent;

    while (!bdb_state->after_llmeta_init_done)
        sleep(1);

    thread_started("bdb logdelete");

    bdb_thread_event(bdb_state, 1);
    time_t last_run_time = 0;

    while (!db_is_stopped()) {
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
extern int db_is_stopped();

void *checkpoint_thread(void *arg)
{
    int rc;
    int checkpointtime;
    int checkpointtimepoll;
    int checkpointrand;
    bdb_state_type *bdb_state;
    int start, end;
    int total_sleep_msec;
    unsigned long long end_sleep_time_msec;
    unsigned long long crt_time_msec;
    DB_LSN logfile;
    DB_LSN crtlogfile;
    int broken;

    thread_started("bdb checkpoint");

    bdb_state = (bdb_state_type *)arg;

    bdb_state = (bdb_state_type *)arg;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    while (!bdb_state->after_llmeta_init_done)
        sleep(1);

    bdb_thread_event(bdb_state, 1);

    while (!db_is_stopped()) {
        BDB_READLOCK("checkpoint_thread");
        checkpointtime = bdb_state->attr->checkpointtime;
        checkpointrand = bdb_state->attr->checkpointrand;
        checkpointtimepoll = bdb_state->attr->checkpointtimepoll;

        broken = bdb_state->dbenv->log_get_last_lsn(bdb_state->dbenv, &logfile);
        if (broken) {
            logmsg(LOGMSG_ERROR, "%s failed in log_get_last_lsn rc=%d\n", __func__,
                    broken);
        }

        /* can't call checkpoint until llmeta is open if we are using rowlocks
         */
        if (gbl_rowlocks && !bdb_state->after_llmeta_init_done) {
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
        end = comdb2_time_epochms();
        bdb_state->checkpoint_start_time = 0;
        MEMORY_SYNC;
        ctrace("checkpoint (scheduled) took %d ms\n", end - start);

        BDB_RELLOCK();

        total_sleep_msec = 1000 * (checkpointtime + (rand() % checkpointrand));

        if (broken) {
            sleep(total_sleep_msec / 1000);
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
