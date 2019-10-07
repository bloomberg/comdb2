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

/*
 * This module is concerned with ondisk queues, managing subscribers
 * and pumping data to them as it becomes available.
 */

#include <alloca.h>
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stddef.h>
#include <time.h>
#include <pthread.h>
#include <poll.h>
#include <time.h>
#include <pthread.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <inttypes.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>

#include <epochlib.h>
#include <bb_stdint.h>
#include <str0.h>
#include <memory_sync.h>
#include <rtcpu.h>
#include <segstring.h>

#include <bdb_api.h>
#include <sbuf2.h>
#include <bb_oscompat.h>
#include <tcputil.h>
#include "dbdest.h"
#include "comdb2.h"
#include <portmuxapi.h>

#include "util.h"

#include <net_types.h>
#include <cdb2_constants.h>
#include <trigger.h>
#include <intern_strings.h>
#include "logmsg.h"

static void coalesce(struct dbenv *dbenv);
static int wake_all_consumers_all_queues(struct dbenv *dbenv, int force);
static void wake_up_consumer(struct consumer *consumer, int force);
static int wake_all_consumers(struct dbtable *db, int force);
static void consumer_destroy(struct consumer *consumer);
static int add_consumer_int(struct dbtable *db, int consumern,
                                    const char *method, int noremove,
                                    int checkonly);
static int add_consumer(struct dbtable *db, int consumern, const char *method,
                         int noremove);
static int add_consumer_int(struct dbtable *db, int consumern,
                                    const char *method, int noremove,
                                    int checkonly);


int set_consumer_options(struct consumer *consumer, const char *opts);

/* a queue consumer */
struct consumer
{
    struct consumer_base base;   /* must be first */

    struct dbtable *db;  /* chain back to db */
    int consumern;

    volatile int active;     /* nonzero if thread is running */

    /* keep some statistics */
    unsigned int n_consumed;

    /* *** for javasp delivery *** */
    char procedure_name[MAXCUSTOPNAME];

    /* Used to implement a sleep/wakeup system for the consumer thread that can
     * be interrupted to make it exit instantly.
     * waiting_for_data is set if we're sleeping in the very outer loop i.e.
     * we are waiting for data to be available.  Normally we don't want to
     * wake up consumers that are sleeping between fstsnd retries just
     * because there's more data on the queue. */
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int need_to_wake;
    int waiting_for_data;

    int please_stop;
    int stopped;

    /* The following data is used to report about the inactivity in the
     * consumption of events from the queue.
     */
    time_t inactive_since; /* timestamp since when events were not consumed
                              from the queue. */
    time_t last_checked;   /* timestamp when activity on the event queue was
                              last checked (done every sec). */
    unsigned long long event_genid; /* genid of the event in the front of the
                                       queue waiting to be consumed when last
                                       checked. */
};


static void coalesce(struct dbenv *dbenv)
{
    /* First, go through and wake up any blocked consumer threads so they can
     * begin their clean exit path. */
    wake_all_consumers_all_queues(dbenv, 1);
}

static int wake_all_consumers_all_queues(struct dbenv *dbenv, int force)
{
    int ii;
    for (ii = 0; ii < dbenv->num_qdbs; ii++) {
        if (dbenv->qdbs[ii]->dbtype == DBTYPE_QUEUEDB)
        {
            struct dbtable *db = dbenv->qdbs[ii];
            wake_all_consumers(db, force);
        }
    }

    /* Return that we didn't handle this event type to let other handlers
     * have a chance to run.  This means we'll wake up some consumers 
     * unnecessarily, but that's ok. */
    return -1;
}

/* Wake up the consumer thread if it is blocked on the condition
 * variable - otherwise no effect. */
static void wake_up_consumer(struct consumer *consumer, int force)
{
    Pthread_mutex_lock(&consumer->mutex);
    if (force || consumer->waiting_for_data) {
        consumer->need_to_wake = 1;
        Pthread_cond_broadcast(&consumer->cond);
    }
    Pthread_mutex_unlock(&consumer->mutex);
}

static int wake_all_consumers(struct dbtable *db, int force)
{
    int consumern;

    if (db->dbtype != DBTYPE_QUEUEDB)
        return -1;

    Pthread_rwlock_rdlock(&db->consumer_lk);
    if (db->dbtype == DBTYPE_QUEUE || db->dbtype == DBTYPE_QUEUEDB) {
        for (consumern = 0; consumern < MAXCONSUMERS; consumern++) {
            struct consumer *consumer = db->consumers[consumern];
            if (consumer && consumer->active)
                wake_up_consumer(consumer, force);
        }
    }
    Pthread_rwlock_unlock(&db->consumer_lk);
    return 0;
}

static void consumer_destroy(struct consumer *consumer)
{
    Pthread_cond_destroy(&consumer->cond);
    free(consumer);
}

/* Add a consumer to a queue.
 *
 * The format is consumertype,option,option,...
 *
 * e.g. fstsnd:localhost:n100,no_rtcpu
 * */
static int add_consumer_int(struct dbtable *db, int consumern,
                                    const char *method, int noremove,
                                    int checkonly)
{
    const char *opts;
    int rc = 0;

    if (!checkonly && db->dbtype == DBTYPE_QUEUEDB)
        Pthread_rwlock_wrlock(&db->consumer_lk);

    if (checkonly) {
        if (strncmp(method, "lua:", 4) != 0 &&
            strncmp(method, "dynlua:", 7) != 0) {
            logmsg(LOGMSG_ERROR, "Unsupported method: %s\n", method);
            rc = -1;
            goto done;
        }
    }

    if (!checkonly && db && (db->dbtype != DBTYPE_QUEUEDB)) {
        logmsg(LOGMSG_ERROR, "%s: %s is not a queue\n", __func__,
               db->tablename);
        rc = -1;
        goto done;
    }

    if (!checkonly && (consumern < 0 || consumern >= MAXCONSUMERS)) {
        logmsg(LOGMSG_ERROR,
               "%s: %s consumer number %d out of range\n",
               __func__, db->tablename, consumern);
        rc = -1;
        goto done;
    }

    if (!checkonly && db && consumern >= 0 && db->consumers[consumern]) {
        if (noremove) {
            logmsg(
                LOGMSG_ERROR,
                "%s: %s consumer number %d in use already\n",
                __func__, db->tablename, consumern);
            rc = -1;
            goto done;
        } else {
            consumer_destroy(db->consumers[consumern]);
            db->consumers[consumern] = NULL;
        }
    }

    struct consumer *consumer = calloc(1, sizeof(struct consumer));
    if (!consumer) {
        logmsg(LOGMSG_ERROR, "%s: malloc failed for consumer\n", __func__);
        rc = -1;
        goto done;
    }
    consumer->db = db;
    consumer->consumern = consumern;
    /* Find options and pull them out separately */
    opts = strchr(method, ',');
    if (opts) {
        int length = opts - method;
        char *tmp;
        opts++;
        tmp = alloca(length + 1);
        memcpy(tmp, method, length);
        tmp[length] = '\0';
        method = tmp;
    } else {
        opts = "";
    }

    if (strncmp(method, "lua:", 4) == 0) {
        consumer->base.type = CONSUMER_TYPE_LUA;
        strncpy(consumer->procedure_name, method + 4,
                sizeof(consumer->procedure_name));
    } else if (strncmp(method, "dynlua:", 7) == 0) {
        consumer->base.type = CONSUMER_TYPE_DYNLUA;
        strncpy(consumer->procedure_name, method + 7,
                sizeof(consumer->procedure_name));
    } else if (strcmp(method, "remove") == 0 && !noremove) {
        free(consumer);
        consumer = NULL;
    } else {
        logmsg(LOGMSG_ERROR, "%s: %s consumer number %d has "
                             "unknown delivery method '%s'\n",
               __func__, db->tablename, consumern, method);
        free(consumer);
        rc = -1;
        goto done;
    }

    /* might have been a "remove" command, so consumer may be NULL */
    if (consumer) {
        /* Allow unrecognised options */
        set_consumer_options(consumer, opts);

        Pthread_mutex_init(&consumer->mutex, NULL);
        Pthread_cond_init(&consumer->cond, NULL);
    }

    if (!checkonly && db)
        db->consumers[consumern] = consumer;
    if (checkonly) {
        consumer_destroy(consumer);
    }
    rc = 0;

done:
    if (!checkonly && db->dbtype == DBTYPE_QUEUEDB)
        Pthread_rwlock_unlock(&db->consumer_lk);
    return rc;
}

static int add_consumer(struct dbtable *db, int consumern, const char *method,
                         int noremove)
{
    return add_consumer_int(db, consumern, method, noremove, 0);
}

/* Does all the steps of adding the consumer, minus the adding. */
static int check_consumer(const char *method)
{
    return add_consumer_int(NULL, -1, method, 0, 1);
}

static int set_consumern_options(struct dbtable *db, int consumern,
                                  const char *opts)
{
    struct consumer *consumer;
    if (consumern < 0 || consumern >= MAXCONSUMERS)
        consumer = NULL;
    else
        consumer = db->consumers[consumern];
    if (consumer)
        return set_consumer_options(consumer, opts);
    else {
        logmsg(LOGMSG_ERROR, "Bad consumer number %d for queue %s\n", consumern,
               db->tablename);
        return -1;
    }
}

int set_consumer_options(struct consumer *consumer, const char *opts)
{
    static const char *delims = ",";
    char *lasts;
    char *copy;
    char *tok;
    int len = strlen(opts);
    int nerrors = 0;

    copy = alloca(len + 1);
    memcpy(copy, opts, len + 1);

    tok = strtok_r(copy, delims, &lasts);
    while (tok) {
        logmsg(LOGMSG_USER, "Queue %s consumer %d option '%s'\n",
               consumer->db->tablename, consumer->consumern, tok);

        tok = strtok_r(NULL, delims, &lasts);
    }

    return nerrors;
}

/* Returns the genid of the event in the front of the queue. */
static unsigned long long dbqueue_get_front_genid(struct dbtable *table,
                                                  int consumer)
{
    unsigned long long genid;
    int rc;
    struct ireq iq;
    void *fnddta;
    size_t fnddtalen;
    size_t fnddtaoff;
    const uint8_t *open;
    pthread_mutex_t *mu;
    pthread_cond_t *cond;

    init_fake_ireq(table->dbenv, &iq);
    iq.usedb = table;
    genid = 0;

    rc = bdb_trigger_subscribe(table->handle, &cond, &mu, &open);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR,
               "dbq_get_front_genid: bdb_trigger_subscribe "
               "failed (rc: %d)\n",
               rc);
        return 0;
    }
    Pthread_mutex_lock(mu);

    if (*open != 1) {
        goto skip;
    }

    rc = dbq_get(&iq, consumer, NULL, (void **)&fnddta, &fnddtalen, &fnddtaoff,
                 NULL, NULL);
    if (rc == 0) {
        genid = dbq_item_genid(fnddta);
    } else if (rc != IX_NOTFND) {
        logmsg(LOGMSG_ERROR,
               "dbq_get_front_genid: dbq_item_genid failed "
               "(rc: %d)\n",
               rc);
    }

skip:
    rc = bdb_trigger_unsubscribe(table->handle);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR,
               "dbq_get_front_genid: bdb_trigger_unsubscribe "
               "failed (rc: %d)\n",
               rc);
    }
    Pthread_mutex_unlock(mu);

    return genid;
}

static void dbqueue_check_inactivity(struct consumer *consumer)
{
    time_t current_time = time(NULL);
    time_t diff = current_time - consumer->last_checked;
    time_t intervals = 60; /* in seconds */

    if (diff && diff >= intervals) {
        unsigned long long genid;

        genid = dbqueue_get_front_genid(consumer->db, consumer->consumern);

        if (!genid)
            return;

        if (genid == consumer->event_genid) {
            logmsg(LOGMSG_USER,
                   "%s: no events were consumed since last %ld secs.\n",
                   consumer->procedure_name,
                   current_time - consumer->inactive_since);
        } else {
            consumer->event_genid = genid;
            consumer->inactive_since = current_time;
        }
        consumer->last_checked = current_time;
    }
}

static pthread_mutex_t dbqueuedb_admin_lk = PTHREAD_MUTEX_INITIALIZER;
static int dbqueuedb_admin_running = 0;

/* This gets called once a second from purge_old_blkseq_thread().
 * If we have become master we make sure that we have threads in place
 * for each consumer. */
static void admin(struct dbenv *dbenv, int type)
{
    int iammaster = (dbenv->master == gbl_mynode) ? 1 : 0;

    Pthread_mutex_lock(&dbqueuedb_admin_lk);
    if (dbqueuedb_admin_running) {
        Pthread_mutex_unlock(&dbqueuedb_admin_lk);
        return;
    }
    dbqueuedb_admin_running = 1;
    Pthread_mutex_unlock(&dbqueuedb_admin_lk);

    /* If we are master then make sure all the queues are running */
    if (iammaster && !dbenv->stopped) {
        for (int ii = 0; ii < dbenv->num_qdbs; ii++) {
            if (dbenv->qdbs[ii] == NULL)
                continue;
            if (dbenv->qdbs[ii]->dbtype == DBTYPE_QUEUEDB) {
                struct dbtable *db = dbenv->qdbs[ii];
                Pthread_rwlock_rdlock(&db->consumer_lk);
                for (int consumern = 0; consumern < MAXCONSUMERS; consumern++) {
                    struct consumer *consumer = db->consumers[consumern];
                    if (!consumer)
                        continue;
                    if (consumer->base.type != type)
                        continue;
                    switch (consumer->base.type) {
                    case CONSUMER_TYPE_LUA:
                        dbqueue_check_inactivity(consumer);

                        if (!trigger_registered(consumer->procedure_name)) {
                            char *name = consumer->procedure_name;
                            char *host =
                                net_get_osql_node(thedb->handle_sibling);
                            if (host == NULL) {
                                trigger_start(name);
                            } else {
                                void *net = thedb->handle_sibling;
                                net_send_message(net, host, NET_TRIGGER_START,
                                                 name, strlen(name) + 1, 0, 0);
                            }
                        }
                        break;
                    case CONSUMER_TYPE_DYNLUA:
                        dbqueue_check_inactivity(consumer);
                        break;
                    }
                }
                Pthread_rwlock_unlock(&db->consumer_lk);
            }
        }
    }

    Pthread_mutex_lock(&dbqueuedb_admin_lk);
    dbqueuedb_admin_running = 0;
    Pthread_mutex_unlock(&dbqueuedb_admin_lk);
}

static int stat_callback(int consumern, size_t length,
                                 unsigned int epoch, void *userptr)
{
    struct consumer_stat *stats = userptr;

    if (consumern < 0 || consumern >= MAXCONSUMERS) {
        logmsg(LOGMSG_USER, "%s: consumern=%d length=%u epoch=%u\n",
                __func__, consumern, (unsigned)length, epoch);
    } else {
        if (!stats[consumern].has_stuff) {
            stats[consumern].first_item_length = length;
            stats[consumern].epoch = epoch;
        }
        stats[consumern].has_stuff = 1;
        stats[consumern].depth++;
    }

    return BDB_QUEUE_WALK_CONTINUE;
}

static void stat_thread_int(struct dbtable *db, int fullstat, int walk_queue)
{
    if (db->dbtype != DBTYPE_QUEUE && db->dbtype != DBTYPE_QUEUEDB)
        logmsg(LOGMSG_ERROR, "'%s' is not a queue\n", db->tablename);
    else {
        int ii;
        struct ireq iq;
        struct consumer_stat stats[MAXCONSUMERS] = {{0}};
        int flags = 0;
        const struct bdb_queue_stats *bdbstats;

        bdbstats = bdb_queue_get_stats(db->handle);

        init_fake_ireq(db->dbenv, &iq);
        iq.usedb = db;

        logmsg(LOGMSG_USER, "(scanning queue '%s' for stats, please wait...)\n",
               db->tablename);
        if (!walk_queue)
            flags = BDB_QUEUE_WALK_FIRST_ONLY;
        if (fullstat)
            flags |= BDB_QUEUE_WALK_KNOWN_CONSUMERS_ONLY;
        dbq_walk(&iq, flags, stat_callback, stats);

        logmsg(LOGMSG_USER, "queue '%s':-\n", db->tablename);
        logmsg(LOGMSG_USER, "  geese added     %u\n", db->num_goose_adds);
        logmsg(LOGMSG_USER, "  geese consumed  %u\n", db->num_goose_consumes);
        logmsg(LOGMSG_USER, "  bdb get bdbstats   %u log %u phys\n",
               bdbstats->n_logical_gets, bdbstats->n_physical_gets);
        logmsg(LOGMSG_USER, "  bdb deadlocks   add %u get %u con %u\n",
               bdbstats->n_add_deadlocks, bdbstats->n_get_deadlocks,
               bdbstats->n_consume_deadlocks);
        logmsg(LOGMSG_USER, "  bdb get not founds %u\n", bdbstats->n_get_not_founds);
        logmsg(LOGMSG_USER, "  bdb con bdbstats   new [con %u, abt %u, gee %u], old %u\n",
               bdbstats->n_new_way_frags_consumed,
               bdbstats->n_new_way_frags_aborted,
               bdbstats->n_new_way_geese_consumed,
               bdbstats->n_old_way_frags_consumed);

        if (db->dbtype == DBTYPE_QUEUEDB)
            Pthread_rwlock_rdlock(&db->consumer_lk);
        for (ii = 0; ii < MAXCONSUMERS; ii++) {
            struct consumer *consumer = db->consumers[ii];

            if (!fullstat && !consumer && !stats[ii].has_stuff)
                continue;

            if (consumer) {
                logmsg(LOGMSG_USER, "  [consumer %02d]: ", ii);
                switch (consumer->base.type) {
                default:
                    logmsg(LOGMSG_USER, "unknown consumer type %d?!\n", consumer->base.type);
                    break;
                }
                logmsg(LOGMSG_USER, "    %u messages consumed\n", consumer->n_consumed);
                if (walk_queue)
                    logmsg(LOGMSG_USER, "    %d items on queue\n", stats[ii].depth);
            } else
                logmsg(LOGMSG_USER, "  [consumer %02d]: none\n", ii);

            if (stats[ii].has_stuff) {
                unsigned int now = comdb2_time_epoch();
                unsigned int age = now - stats[ii].epoch;
                struct tm ctime;
                time_t cepoch = (time_t)stats[ii].epoch;
                char buf[32]; /* must be at least 26 chars */
                unsigned int hr, mn, sc;

                localtime_r(&cepoch, &ctime);
                asctime_r(&ctime, buf);
                hr = age / (60 * 60);
                age -= hr * 60 * 60;
                mn = age / 60;
                sc = age % 60;

                logmsg(
                    LOGMSG_USER,
                    "    head item length %u age %u:%02u:%02u created %ld %s",
                    (unsigned)stats[ii].first_item_length, hr, mn, sc,
                    stats[ii].epoch, buf);
            } else if (consumer)
                logmsg(LOGMSG_USER, "    empty\n");
        }
        if (db->dbtype == DBTYPE_QUEUEDB)
            Pthread_rwlock_unlock(&db->consumer_lk);

        logmsg(LOGMSG_USER, "-----\n");
    }
}

struct statthrargs {
    struct dbtable *db;
    int fullstat;
    int walk_queue;
};

static void *stat_thread(void *argsptr)
{
    struct statthrargs *args = argsptr;
    thread_started("dbque stat");
    backend_thread_event(args->db->dbenv, COMDB2_THR_EVENT_START_RDONLY);
    stat_thread_int(args->db, args->fullstat, args->walk_queue);
    backend_thread_event(args->db->dbenv, COMDB2_THR_EVENT_DONE_RDONLY);
    free(args);
    return NULL;
}

static void queue_stat(struct dbtable *db, int full, int walk_queue, int blocking)
{
    int rc;
    pthread_t tid;
    pthread_attr_t attr;
    struct statthrargs *args;

    args = calloc(1, sizeof(struct statthrargs));
    if (!args) {
        logmsg(LOGMSG_ERROR, "%s: calloc failed\n", __func__);
        return;
    }
    args->db = db;
    args->fullstat = full;
    args->walk_queue = walk_queue;

    if (blocking) {
        Pthread_attr_init(&attr);
        Pthread_attr_setstacksize(&attr, DEFAULT_THD_STACKSZ);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    }

    rc = pthread_create(&tid, blocking ? &attr : &gbl_pthread_attr_detached,
                        stat_thread, args);

    if (blocking) {
        Pthread_attr_destroy(&attr);
    }

    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: pthread_create failed rc=%d %s\n", __func__, rc,
                strerror(rc));
        free(args);
    }

    if (blocking) {
        void *status;
        rc = pthread_join(tid, &status);
        if (rc != 0)
            logmsg(LOGMSG_ERROR, "%s: pthread_join %d %s\n", __func__, rc,
                    strerror(rc));
    }
}

/* This is not a totally thread safe solution, but it makes flushed abortable */
static int flush_thread_active = 0;

extern int 
queue_consume(struct ireq *iq, const void *fnd, int consumern);

static void queue_flush(struct dbtable *db, int consumern)
{
    struct ireq iq;
    int nflush = 0;

    init_fake_ireq(db->dbenv, &iq);
    iq.usedb = db;

    logmsg(LOGMSG_INFO, "Beginning flush for queue '%s' consumer %d\n",
           db->tablename, consumern);

    if (db->dbenv->master != gbl_mynode) {
        logmsg(LOGMSG_WARN, "... but I am not the master node, so I do nothing.\n");
        return;
    }

    while (1) {
        void *item;
        int rc;

        if (!flush_thread_active) {
            logmsg(LOGMSG_WARN, "Terminating flush operation prematurely\n");
            return;
        }

        rc = dbq_get(&iq, consumern, NULL, (void **)&item, NULL, NULL, NULL,
                     NULL);

        if (rc != 0) {
            if (rc != IX_NOTFND)
                logmsg(LOGMSG_ERROR, "Terminating with dbq_get rcode %d\n", rc);
            break;
        }

        rc = queue_consume(&iq, item, consumern);
        free(item);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "Terminating after consume rcode %d\n", rc);
            break;
        }
        if (++nflush % 100 == 0) {
            logmsg(LOGMSG_INFO, "... flushed %d items\n", nflush);
        }
    }

    logmsg(LOGMSG_INFO,
           "Done flush for queue '%s' consumer %d, flushed %d items\n",
           db->tablename, consumern, nflush);
}

struct flush_thd_data {
    struct dbtable *db;
    int consumern;
};

static void *flush_thd(void *argsptr)
{
    struct flush_thd_data *args = argsptr;
    thread_started("dbque flush");
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);
    queue_flush(args->db, args->consumern);
    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
    if (flush_thread_active)
        flush_thread_active = 0;
    free(args);
    return NULL;
}

void flush_in_thread(struct dbtable *db, int consumern)
{
    pthread_attr_t attr;
    struct flush_thd_data *args;
    int rc;
    pthread_t tid;

    if (flush_thread_active) {
        logmsg(LOGMSG_ERROR, "Flush operation already in progress\n");
        return;
    }

    Pthread_attr_init(&attr);
    Pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    Pthread_attr_setstacksize(&attr, DEFAULT_THD_STACKSZ);

    args = calloc(1, sizeof(struct flush_thd_data));
    if (!args) {
        Pthread_attr_destroy(&attr);
        logmsg(LOGMSG_ERROR, "%s: out of memory\n", __func__);
        return;
    }
    args->db = db;
    args->consumern = consumern;

    flush_thread_active = 1;
    rc = pthread_create(&tid, &attr, flush_thd, args);
    if (rc != 0) {
        flush_thread_active = 0;
        logmsg(LOGMSG_ERROR, "%s: pthread_create error %d %s\n",
                __func__, rc, strerror(rc));
        free(args);
    }
    Pthread_attr_destroy(&attr);
}

void flush_abort(void)
{
    if (flush_thread_active) {
        logmsg(LOGMSG_INFO, "Signalling flush abort\n");
        flush_thread_active = 0;
    } else {
        logmsg(LOGMSG_ERROR, "No flush thread is active\n");
    }
}

static void stop_consumer(struct consumer *consumer)
{
    // These get offloaded to replicants, not run locally.
    // Let them get error from missing queue
    if (consumer->base.type == CONSUMER_TYPE_DYNLUA ||
        consumer->base.type == CONSUMER_TYPE_LUA) {
        return;
    }

    // TODO CONSUMER_TYPE_LUA,
    Pthread_mutex_lock(&consumer->mutex);
    consumer->please_stop = 1;
    Pthread_cond_signal(&consumer->cond);
    Pthread_mutex_unlock(&consumer->mutex);

    Pthread_mutex_lock(&consumer->mutex);
    if (consumer->stopped) {
        Pthread_mutex_unlock(&consumer->mutex);
        return;
    }
    while (!consumer->stopped) {
        Pthread_cond_wait(&consumer->cond, &consumer->mutex);
        Pthread_mutex_unlock(&consumer->mutex);
    }
}

/* doesn't actually start, admin will do that, but
 * mark "no longer stopped" so admin can do it's thing */
static void restart_consumer(struct consumer *consumer)
{
    Pthread_mutex_lock(&consumer->mutex);
    consumer->stopped = 0;
    Pthread_mutex_unlock(&consumer->mutex);
}

int stop_consumers(struct dbtable *db)
{
    if (db->dbtype != DBTYPE_QUEUEDB)
        return -1;
    Pthread_rwlock_rdlock(&db->consumer_lk);
    for (int i = 0; i < MAXCONSUMERS; i++) {
        if (db->consumers[i])
            stop_consumer(db->consumers[i]);
    }
    Pthread_rwlock_unlock(&db->consumer_lk);
    return 0;
}

int restart_consumers(struct dbtable *db)
{
    if (db->dbtype != DBTYPE_QUEUEDB)
        return -1;
    Pthread_rwlock_rdlock(&db->consumer_lk);
    for (int i = 0; i < MAXCONSUMERS; i++) {
        if (db->consumers[i])
            restart_consumer(db->consumers[i]);
    }
    Pthread_rwlock_unlock(&db->consumer_lk);

    return 0;
}

static enum consumer_t consumer_type(struct consumer *c)
{
    return c->base.type;
}

static int handles_method(const char *method) {
    if (strncmp(method, "remove", 6) == 0 ||
            strncmp(method, "lua:4" , 4) == 0 ||
            strncmp(method, "dynlua:", 7) == 0) {
        return 1;
    }
    return 0;
}

static int get_name(struct dbtable *db, char **spname) {
    struct consumer *consumer = db->consumers[0];
    if (db->dbtype != DBTYPE_QUEUEDB)
        return -1;
    if (consumer)
        *spname = strdup(consumer->procedure_name);
    return 0;
}

static int get_stats(struct dbtable *db, struct consumer_stat *st) {
    struct ireq iq;
    if (db->dbtype != DBTYPE_QUEUEDB)
        return -1;
    init_fake_ireq(db->dbenv, &iq);
    iq.usedb = db;
    int rc = dbq_walk(&iq, 0, stat_callback, st);
    if (rc)
        return rc;
    return 0;
}

comdb2_queue_consumer_t dbqueuedb_plugin_lua = {
    .type = CONSUMER_TYPE_LUA,
    .add_consumer = add_consumer,
    .admin = admin,
    .check_consumer = check_consumer,
    .consumer_type = consumer_type,
    .coalesce = coalesce,
    .restart_consumers = restart_consumers,
    .stop_consumers = stop_consumers,
    .wake_all_consumers = wake_all_consumers,
    .wake_all_consumers_all_queues = wake_all_consumers_all_queues,
    .handles_method = handles_method,
    .get_name = get_name,
    .get_stats = get_stats
};

comdb2_queue_consumer_t dbqueuedb_plugin_dynlua = {
    .type = CONSUMER_TYPE_DYNLUA,
    .add_consumer = add_consumer,
    .admin = admin,
    .check_consumer = check_consumer,
    .consumer_type = consumer_type,
    .coalesce = coalesce,
    .restart_consumers = restart_consumers,
    .stop_consumers = stop_consumers,
    .wake_all_consumers = wake_all_consumers,
    .wake_all_consumers_all_queues = wake_all_consumers_all_queues,
    .handles_method = handles_method,
    .get_name = get_name,
    .get_stats = get_stats
};


#include "plugin.h"
