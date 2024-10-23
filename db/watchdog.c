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

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <sockpool.h>
#include "lockmacros.h"
#include "comdb2.h"
#include "sql.h"
#include "fdb_fend.h"
#include "views.h"
#include "logmsg.h"
#include "reqlog.h"
#include "sc_global.h"
#include <comdb2_atomic.h>

int gbl_client_queued_slow_seconds = 0;
int gbl_client_running_slow_seconds = 0;
int gbl_client_abort_on_slow = 0;

extern int gbl_watcher_thread_ran;

static void *watchdog_thread(void *arg);

static void *dummy_thread(void *arg) { return NULL; }

static int gbl_watchdog_kill_time;
static pthread_t gbl_watchdog_kill_tid;
static pthread_mutex_t gbl_watchdog_kill_mutex = PTHREAD_MUTEX_INITIALIZER;

static int gbl_nowatch = 1; /* start off disabled */
static int gbl_watchdog_time; /* last timestamp when things were ok */

static pthread_attr_t gbl_pthread_joinable_attr;

extern pthread_attr_t gbl_pthread_attr;

void watchdog_set_alarm(int seconds)
{
    Pthread_mutex_lock(&gbl_watchdog_kill_mutex);

    /* if theres already an alarm, leave it alone */
    if (gbl_watchdog_kill_time) {
        Pthread_mutex_unlock(&gbl_watchdog_kill_mutex);
        return;
    }

    gbl_watchdog_kill_time = comdb2_time_epoch() + seconds;
    gbl_watchdog_kill_tid = pthread_self();

    Pthread_mutex_unlock(&gbl_watchdog_kill_mutex);
}

void watchdog_cancel_alarm(void)
{
    Pthread_mutex_lock(&gbl_watchdog_kill_mutex);

    /* if no alarm is set, its an error */
    if (!gbl_watchdog_kill_time) {
        Pthread_mutex_unlock(&gbl_watchdog_kill_mutex);
        return;
    }

    /* if the currently set alarm isnt ours, leave it alone */
    if (gbl_watchdog_kill_tid != pthread_self()) {
        Pthread_mutex_unlock(&gbl_watchdog_kill_mutex);
        return;
    }

    /* it's ours, disarm it */
    gbl_watchdog_kill_tid = 0;
    gbl_watchdog_kill_time = 0;

    Pthread_mutex_unlock(&gbl_watchdog_kill_mutex);
}

int gbl_epoch_time; /* db has been up gbl_epoch_time - gbl_starttime seconds */

static void watchdogsql(void)
{
    struct sqlclntstate clnt;
    start_internal_sql_clnt(&clnt);
    clnt.dbtran.mode = TRANLEVEL_SOSQL;
    clnt.admin = 1;
    clnt.skip_eventlog = 1;
    run_internal_sql_clnt(&clnt, "select 1");
    end_internal_sql_clnt(&clnt);
}

static void *watchdog_thread(void *arg)
{
    comdb2_name_thread(__func__);
    void *ptr;
    pthread_t dummy_tid;
    int rc;
    int fd;
    int its_bad;          /* per iteration, fast track */
    int its_bad_slow = 0; /* per counter, slow track */
    int coherent = 0;

    int counter = 0;
    char curlsn[64];
    uint64_t lastlsnbytes = 0, curlsnbytes;
    char master_curlsn[64];
    uint64_t master_lastlsnbytes = 0, master_curlsnbytes;
    int sockpool_timeout;

    Pthread_attr_init(&gbl_pthread_joinable_attr);
    Pthread_attr_setstacksize(&gbl_pthread_joinable_attr, DEFAULT_THD_STACKSZ);
    Pthread_attr_setdetachstate(&gbl_pthread_joinable_attr, PTHREAD_CREATE_JOINABLE);

    thrman_register(THRTYPE_GENERIC);
    thread_started("watchdog");

    while (!gbl_ready)
        sleep(1);

    int test_io_time = 0;
    int test_sql_time = 0;
    while (!db_is_exiting()) {
        gbl_epoch_time = comdb2_time_epoch();

        if (!gbl_nowatch) {
            int stop_thds_time;

            its_bad = 0;

            if (gbl_watchdog_kill_time) {
                if (comdb2_time_epoch() >= gbl_watchdog_kill_time) {
                    logmsg(LOGMSG_WARN, "gbl_watchdog_kill_time set\n");
                    its_bad = 1;
                }
            }

            /* try to malloc something */
            ptr = calloc(1, 128 * 1024);
            if (!ptr) {
                logmsg(LOGMSG_WARN, "watchdog: Can't allocate memory\n");
                its_bad = 1;
            }
            free(ptr);

            /* try to get a file descriptor */
            fd = open("/", O_RDONLY);
            if (fd == -1) {
                logmsg(LOGMSG_WARN, "watchdog: Can't open file\n");
                its_bad = 1;
            }

            rc = Close(fd);
            if (rc) {
                logmsg(LOGMSG_WARN, "watchdog: Can't close file\n");
                its_bad = 1;
            }

            /* simple test to check a few fdb */
            fdb_sanity_check();

            /* find out if we're trying to stop threads */
            LOCK(&stop_thds_time_lk) { stop_thds_time = gbl_stop_thds_time; }
            UNLOCK(&stop_thds_time_lk);
            if (stop_thds_time) {
                int diff_sec;

                diff_sec = comdb2_time_epoch() - stop_thds_time;
                if (diff_sec > gbl_stop_thds_time_threshold) {
                    logmsg(LOGMSG_WARN, 
                            "watchdog: Trying to stop threads for %d seconds\n",
                            diff_sec);
                    thd_dump();
                    its_bad = 1;
                }
            }
            /* can't run a sql query if the sql threads are being stopped */
            else {

                /* try to run an sql query
                 * sqlglue code will fail if the db is set up for async
                 * replication
                 * and we read from a non-master, so no point trying. */

                backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDONLY);
                coherent = bdb_am_i_coherent(thedb->bdb_env);
                backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDONLY);

/* i dont know why this seems to fail sometimes with -4.
   disabling for now. */
#if 0             
             if ((thedb->rep_sync != REP_SYNC_NONE
                         || thedb->master == gbl_myhostname)
                     && coherent)
             {
                 rc = sqltest(thedb->envname);
                 if (rc != 0)
                 {
                     fprintf(stderr, "watchdog: Can't run a sql query\n");
                     its_bad = 1;
                 }
             }
#endif

                /* if i am incoherent, check to see if we make progress against
                   master
                   if this is not the case, it means I am deadlock
                   we run this for each 10 iterations of watchdog
                 */
                if (counter % 10 == 0) {
                    char *master = thedb->master;
                    /* testing slow event time */
                    its_bad_slow = 0;

                    /* try to create a thread */
                    rc = pthread_create(&dummy_tid, &gbl_pthread_joinable_attr,
                                        dummy_thread, thedb);
                    if (rc) {
                        logmsg(LOGMSG_WARN, "watchdog: Can't create thread\n");
                        its_bad_slow = its_bad = 1;
                    } else {
                        rc = pthread_join(dummy_tid, NULL);
                        if (rc) {
                            logmsg(LOGMSG_WARN,
                                   "watchdog: Can't join thread\n");
                            its_bad_slow = its_bad = 1;
                        }
                    }

                    if (!coherent && master > 0 && master != gbl_myhostname) {
                        bdb_get_cur_lsn_str(thedb->bdb_env, &curlsnbytes,
                                            curlsn, sizeof(curlsn));
                        bdb_get_cur_lsn_str_node(
                            thedb->bdb_env, &master_curlsnbytes, master_curlsn,
                            sizeof(curlsn), thedb->master);
                        if (!lastlsnbytes) {
                            lastlsnbytes = curlsnbytes;
                            master_lastlsnbytes = master_curlsnbytes;
                        }
                        /* time for deadlock test;
                           for now we ignore master progress */
                        else if (lastlsnbytes == curlsnbytes &&
                                 /* earth did not moved in the meantime */
                                 master_curlsnbytes > curlsnbytes &&
                                 master_lastlsnbytes > curlsnbytes) {
                            /* we were behind last run, we are still
                               behind and we did not move: DEADLOCK */

                            logmsg(LOGMSG_WARN,
                                   "watchdog: DATABASE MAKES NO PROGRESS; "
                                   "DEADLOCK ALERT %s %s!\n",
                                   curlsn, master_curlsn);
                            its_bad = 1;
                            its_bad_slow = 1;
                        }
                    }
                }
            }

            /* test netinfo lock */
            {
                const char *hostlist[REPMAX];
                net_get_all_nodes_connected(thedb->handle_sibling, hostlist);
            }

            /* See if we can grab the berkeley log region lock.  If we block on
               it, something is seriously wrong.  We can do
               this by trying to flush a log at a point where it's almost
               certainly already flushed, like 1:0 - the underlying code needs
               to check the lsn under the region lock, but then has nothing else
               to do. */
            bdb_flush_up_to_lsn(thedb->bdb_env, 1, 0);

            if (gbl_epoch_time - test_io_time >
                bdb_attr_get(thedb->bdb_attr, BDB_ATTR_TEST_IO_TIME)) {
                if (bdb_watchdog_test_io(thedb->bdb_env)) {
                    logmsg(LOGMSG_FATAL,
                           "%s:bdb_watchdog_test_io failed - aborting\n",
                           __func__);
                    abort();
                }
                test_io_time = gbl_epoch_time;
            }

            int check_sql_time = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_TEST_SQL_TIME);
            if (check_sql_time && ((gbl_epoch_time - test_sql_time) > check_sql_time)) {
                watchdogsql();
                test_sql_time = gbl_epoch_time;
            }

            /* if nothing was bad, update the timestamp */
            if (!its_bad && !its_bad_slow) {
                int now = comdb2_time_epoch();
                time_metric_add(thedb->watchdog_time, now - gbl_watchdog_time);
                gbl_watchdog_time = now;
            }
        }

        sockpool_timeout =
            bdb_attr_get(thedb->bdb_attr, BDB_ATTR_TIMEOUT_SERVER_SOCKPOOL);
        if (!sockpool_timeout)
            sockpool_timeout = 10;

        if ((counter % sockpool_timeout) == (sockpool_timeout - 1)) {
            /* roughly every 5 minute */
            socket_pool_timeout();
        }
        
        int one = 1;
        if (CAS32(gbl_trigger_timepart, one, 0)) {
            if (thedb->master == gbl_myhostname) {
                rc = views_cron_restart(thedb->timepart_views);
                if (rc) {
                    logmsg(LOGMSG_WARN, "Failed to restart timepartitions rc=%d!\n",
                            rc);
                }
            }
        }

        if ((gbl_client_queued_slow_seconds > 0) ||
            (gbl_client_running_slow_seconds > 0)) {
            struct connection_info *conn_infos = NULL;
            int conn_count = 0;
            if ((rc = gather_connection_info(&conn_infos, &conn_count)) == 0) {
                int slow_count = 0;
                int conn_time_now = comdb2_time_epochms();
                for (int cid = 0; cid < conn_count; cid++) {
                    struct connection_info *conn_info = &conn_infos[cid];
                    const char *zState = NULL;
                    int slow_seconds = 0;
                    switch (conn_info->state_int) {
                        case CONNECTION_QUEUED:
                            zState = "QUEUED";
                            slow_seconds = gbl_client_queued_slow_seconds;
                            break;
                        case CONNECTION_RUNNING:
                            zState = "RUNNING";
                            slow_seconds = gbl_client_running_slow_seconds;
                            break;
                    }
                    if ((zState != NULL) && (slow_seconds > 0)) {
                        int state_time = conn_info->time_in_state_int;
                        int diff_seconds = (conn_time_now - state_time) / 1000;
                        if ((diff_seconds < 0) || (diff_seconds > slow_seconds)) {
                            logmsg((diff_seconds > 0) && gbl_client_abort_on_slow ?
                                           LOGMSG_FATAL : LOGMSG_ERROR,
                                   "%s: client #%lld has been in state %s for "
                                   "%d seconds (>%d): connect_time %0.2f "
                                   "seconds, raw_time_in_state %d, host {%s}, "
                                   "pid %lld, sql {%s}\n", __func__,
                                   (long long int)conn_info->connection_id,
                                   zState, diff_seconds, slow_seconds,
                                   difftime(conn_info->connect_time_int, (time_t)0),
                                   conn_info->time_in_state_int, conn_info->host,
                                   (long long int)conn_info->pid, conn_info->sql);
                             /* NOTE: Do not count negative seconds here... */
                             if (diff_seconds > 0) slow_count++;
                        }
                    }
                }
                free_connection_info(conn_infos, conn_count);
                if (slow_count > 0) {
                    bdb_dump_threads_and_maybe_abort(
                        thedb->bdb_env, 0, gbl_client_abort_on_slow);
                }
            } else {
                logmsg(LOGMSG_ERROR, "%s: gather_connection_info rc=%d\n",
                       __func__, rc);
            }
        }

        reqlog_log_all_longreqs();

        sc_alter_latency(counter);

        /* we use counter to downsample the run events for lower frequence
           tasks, like deadlock detector */
        counter++;

        sleep(1);
    }
    return NULL;
}

void watchdog_disable(void)
{
    logmsg(LOGMSG_INFO, "watchdog_disable called\n");
    gbl_nowatch = 1;
}

void watchdog_enable(void)
{
    logmsg(LOGMSG_INFO, "watchdog_enable called\n");
    gbl_watchdog_time = comdb2_time_epoch();
    gbl_nowatch = 0;
}

void comdb2_die(int abort)
{
    bdb_dump_threads_and_maybe_abort(thedb->bdb_env, 1, abort);
    _exit(1);
}

static void *watchdog_watcher_thread(void *arg)
{
    comdb2_name_thread(__func__);
    extern int gbl_watchdog_watch_threshold;
    int failed_once = 0;

    while (!db_is_exiting()) {
        int ss = 10; /* sleep for these many seconds */
        for (int i = 0; i < ss && !db_is_exiting(); i++) {
            sleep(1);
            void check_timers(void);
            check_timers();
        }

        if (gbl_nowatch || db_is_exiting())
            continue;

        int tmstmp = comdb2_time_epoch();
        if (tmstmp - gbl_watchdog_time > gbl_watchdog_watch_threshold) {
            /*
              In order to handle situations where the watchdog is assumed
              dead once the system suspends, and wakes back up after more
              than gbl_watchdog_time secs, we need to wait for an additional
              cycle before making a decision to abort.
            */
            if (failed_once > 0) {
                logmsg(LOGMSG_FATAL, "watchdog thread stuck for more than %d seconds - expected time was %d seconds, exiting\n",
                        gbl_watchdog_watch_threshold,
                        (int) time_metric_average(thedb->watchdog_time));
                comdb2_die(1);
            }
            failed_once++;
            continue;
        }
        /*
          Reset the flag to protect against abort being triggered for
          non-successive failures.
        */
        failed_once = 0;

        /* I also wanna watch the bdb watcher, since that one
           gets stuck every time there is berkdb lockdown */
        if (tmstmp - gbl_watcher_thread_ran > gbl_watchdog_watch_threshold) {
            logmsg(LOGMSG_FATAL, "rep watcher thread stuck for more than %d seconds, exiting\n",
                   gbl_watchdog_watch_threshold);
            comdb2_die(1);
        }
    }
    return NULL;
}

void create_watchdog_thread(struct dbenv *dbenv)
{
    int rc;
    pthread_attr_t attr;

    Pthread_attr_init(&attr);
    Pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    /* HP needs more stack space to call AttachCurrentThread:
       http://docs.hp.com/en/JDKJRE60RN/jdk_rnotes_6.0.01.html#CreateJavaVM
       The docs refer to a different version of java and a different machine,
       but it still seems to hold true for 1.4 on our hardware.

       DEFAULT_THD_STACKSZ is 512k on HP */
    Pthread_attr_setstacksize(&attr, DEFAULT_THD_STACKSZ);

    rc = pthread_create(&dbenv->watchdog_tid, &attr, watchdog_thread, thedb);
    if (rc)
        logmsg(LOGMSG_ERROR, "Warning: can't start watchdog_thread thread: rc %d err %s\n",
               rc, strerror(rc));

    rc = pthread_create(&dbenv->watchdog_watcher_tid, &gbl_pthread_attr,
                        watchdog_watcher_thread, thedb);
    if (rc)
        logmsg(LOGMSG_ERROR, "Warning: can't start watchdog_watcher_thread thread:"
               " rc %d err %s\n",
               rc, strerror(rc));

    Pthread_attr_destroy(&attr);
}
