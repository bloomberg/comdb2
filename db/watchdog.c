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
#include <sys/resource.h>
#include <sys/utsname.h>

#include <alloca.h>
#include <ctype.h>
#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <time.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <netdb.h>

#include <epochlib.h>
#include "marshal.h"
#include <segstr.h>
#include <lockmacro.h>

#include <list.h>

#include <str0.h>
#include <rtcpu.h>
#include <ctrace.h>

#include <memory_sync.h>

#include <net.h>
#include <bdb_api.h>
#include <sbuf2.h>
#include <quantize.h>
#include <sockpool.h>

#include "comdb2.h"
#include "sql.h"

#include "comdb2_trn_intrl.h"
#include "history.h"
#include "tag.h"
#include "types.h"
#include "timer.h"
#include <plhash.h>
#include <dynschemaload.h>
#include "translistener.h"
#include "util.h"
#include "verify.h"
#include "switches.h"
#include "sqloffload.h"
#include "osqlblockproc.h"
#include "fdb_fend.h"

#include <sqliteInt.h>

#include "thdpool.h"
#include "memdebug.h"
#include "bdb_access.h"
#include "views.h"
#include <logmsg.h>

extern int gbl_watcher_thread_ran;

static void *watchdog_thread(void *arg);

static void *dummy_thread(void *arg) { return NULL; }

static int gbl_watchdog_kill_time;
static pthread_t gbl_watchdog_kill_tid;
static pthread_mutex_t gbl_watchdog_kill_mutex;

static int gbl_nowatch = 1; /* start off disabled */
static int gbl_watchdog_time;

static pthread_attr_t gbl_pthread_joinable_attr;

extern pthread_attr_t gbl_pthread_attr;

void watchdog_set_alarm(int seconds)
{
    pthread_mutex_lock(&gbl_watchdog_kill_mutex);

    /* if theres already an alarm, leave it alone */
    if (gbl_watchdog_kill_time) {
        pthread_mutex_unlock(&gbl_watchdog_kill_mutex);
        return;
    }

    gbl_watchdog_kill_time = comdb2_time_epoch() + seconds;
    gbl_watchdog_kill_tid = pthread_self();

    pthread_mutex_unlock(&gbl_watchdog_kill_mutex);
}

void watchdog_cancel_alarm(void)
{
    pthread_mutex_lock(&gbl_watchdog_kill_mutex);

    /* if no alarm is set, its an error */
    if (!gbl_watchdog_kill_time) {
        pthread_mutex_unlock(&gbl_watchdog_kill_mutex);
        return;
    }

    /* if the currently set alarm isnt ours, leave it alone */
    if (gbl_watchdog_kill_tid != pthread_self()) {
        pthread_mutex_unlock(&gbl_watchdog_kill_mutex);
        return;
    }

    /* it's ours, disarm it */
    gbl_watchdog_kill_tid = 0;
    gbl_watchdog_kill_time = 0;

    pthread_mutex_unlock(&gbl_watchdog_kill_mutex);
}

int gbl_epoch_time;

static void *watchdog_thread(void *arg)
{
    void *ptr;
    pthread_t dummy_tid;
    int rc;
    int fd;
    int its_bad;          /* per iteration, fast track */
    int its_bad_slow = 0; /* per counter, slow track */
    int coherent = 0;

    int counter = 0;
    char lastlsn[63] = "", curlsn[64];
    uint64_t lastlsnbytes = 0, curlsnbytes;
    char master_lastlsn[63] = "", master_curlsn[64];
    uint64_t master_lastlsnbytes = 0, master_curlsnbytes;
    char *master;
    int sockpool_timeout;

    rc = pthread_mutex_init(&gbl_watchdog_kill_mutex, NULL);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "pthread_mutex_init gbl_watchdog_kill_mutex failed\n");
        exit(1);
    }

    pthread_attr_init(&gbl_pthread_joinable_attr);
    pthread_attr_setstacksize(&gbl_pthread_joinable_attr, DEFAULT_THD_STACKSZ);
    pthread_attr_setdetachstate(&gbl_pthread_joinable_attr,
                                PTHREAD_CREATE_JOINABLE);

    while (!gbl_ready) {
        sleep(10);
    }

    while (!thedb->exiting) {
        sleep(1);

        gbl_epoch_time = comdb2_time_epoch();

        if (!gbl_nowatch && !thedb->exiting) {
            int stop_thds_time;

            its_bad = 0;

            if (gbl_watchdog_kill_time) {
                if (comdb2_time_epoch() >= gbl_watchdog_kill_time) {
                    logmsg(LOGMSG_WARN, "gbl_watchdog_kill_time set\n");
                    its_bad = 1;
                }
            }

            /* try to malloc something */
            ptr = malloc(128 * 1024);
            if (!ptr) {
                logmsg(LOGMSG_WARN, "watchdog: Can't malloc\n");
                its_bad = 1;
            }

            free(ptr);

            /* try to create a thread */
            rc = pthread_create(&dummy_tid, &gbl_pthread_joinable_attr,
                                dummy_thread, thedb);
            if (rc) {
                logmsg(LOGMSG_WARN, "watchdog: Can't create thread\n");
                its_bad = 1;
            } else {
                rc = pthread_join(dummy_tid, NULL);
                if (rc) {
                    logmsg(LOGMSG_WARN, "watchdog: Can't join thread\n");
                    its_bad = 1;
                }
            }

            /* try to get a file descriptor */
            fd = open("/", O_RDONLY);
            if (fd == -1) {
                logmsg(LOGMSG_WARN, "watchdog: Can't open file\n");
                its_bad = 1;
            }

            rc = close(fd);
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
                         || thedb->master == gbl_mynode)
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
                master = thedb->master;
                if (counter % 10 == 0) {
                    /* testing slow event time */
                    its_bad_slow = 0;

                    if (!coherent && master > 0 && master != gbl_mynode) {
                        bdb_get_cur_lsn_str(thedb->bdb_env, &curlsnbytes,
                                            curlsn, sizeof(curlsn));
                        bdb_get_cur_lsn_str_node(
                            thedb->bdb_env, &master_curlsnbytes, master_curlsn,
                            sizeof(curlsn), thedb->master);
                        if (!lastlsnbytes) {
                            lastlsnbytes = curlsnbytes;
                            master_lastlsnbytes = master_curlsnbytes;
                        } else {

                            /* time for deadlock test;
                               for now we ignore master progress
                             */
                            if (lastlsnbytes == curlsnbytes) {
                                /* earth did not moved in the meantime */
                                if (master_curlsnbytes > curlsnbytes &&
                                    master_lastlsnbytes > curlsnbytes) {
                                    /* we were behind last run, we are still
                                       behind
                                       and we did not move: DEADLOCK */

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
                }
            }

            /* test netinfo lock */
            {
                int count;
                const char *hostlist[REPMAX];
                count = net_get_all_nodes_connected(thedb->handle_sibling,
                                                    hostlist);
            }

            /* See if we can grab the berkeley log region lock.  If we block on
               it, something is seriously wrong.  We can do
               this by trying to flush a log at a point where it's almost
               certainly already flushed, like 1:0 - the underlying code needs
               to check the lsn under the region lock, but then has nothing else
               to do. */
            bdb_flush_up_to_lsn(thedb->bdb_env, 1, 0);

            if (bdb_watchdog_test_io(thedb->bdb_env))
                its_bad = 1;

            /* if nothing was bad, update the timestamp */
            if (!its_bad && !its_bad_slow) {
                gbl_watchdog_time = comdb2_time_epoch();
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
        
        if (gbl_trigger_timepart) {
            gbl_trigger_timepart = 0;
            if(thedb->master == gbl_mynode) {
                rc = views_cron_restart(thedb->timepart_views);
                if (rc) {
                    logmsg(LOGMSG_WARN, "Failed to restart timepartitions rc=%d!\n",
                            rc);
                }
            }
        }

        /* we use counter to downsample the run events for lower frequence
           tasks,
           like deadlock detector */
        counter++;
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

void lock_info_lockers(FILE *out, bdb_state_type *bdb_state);

void comdb2_die(int aborat)
{
    pid_t pid;
    char pstack_cmd[128];
    int rc;
    pthread_t tid;

    /* we have 60 seconds to "print useful stuff" */
    alarm(60);

    logmsg(LOGMSG_FATAL, "Getting ready to die, printing useful debug info.\n");

    lock_info_lockers(stderr, thedb->bdb_env);

    /* print some useful stuff */

    thd_dump();

    pid = getpid();
    if (snprintf(pstack_cmd, sizeof(pstack_cmd), "pstack %d", (int)pid) >=
        sizeof(pstack_cmd)) {
        logmsg(LOGMSG_WARN, "pstack cmd too long for buffer\n");
    } else {
        int dum = system(pstack_cmd);
    }

    if (aborat)
        abort();
    else
        _exit(1);
}

static void *watchdog_watcher_thread(void *arg)
{
    extern int gbl_watchdog_watch_threshold;
    int failed_once = 0;

    while (!thedb->exiting) {
        sleep(10);
        if (gbl_nowatch || thedb->exiting)
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
                logmsg(LOGMSG_FATAL, "watchdog thread stuck, exiting\n");
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
        if (tmstmp - gbl_watcher_thread_ran > 60) {
            logmsg(LOGMSG_FATAL, "rep watcher thread stuck, exiting\n");
            comdb2_die(1);
        }
    }
    return NULL;
}

void create_watchdog_thread(struct dbenv *dbenv)
{
    int rc;
    pthread_attr_t attr;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    /* HP needs more stack space to call AttachCurrentThread:
       http://docs.hp.com/en/JDKJRE60RN/jdk_rnotes_6.0.01.html#CreateJavaVM
       The docs refer to a different version of java and a different machine,
       but it still seems to hold true for 1.4 on our hardware.

       DEFAULT_THD_STACKSZ is 512k on HP */
    pthread_attr_setstacksize(&attr, DEFAULT_THD_STACKSZ);

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

    pthread_attr_destroy(&attr);
}
