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
#include <machpthread.h>
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
#include "machine.h"

#include <net_types.h>
#include <cdb2_constants.h>
#include <dbqueue.h>
#include <trigger.h>
#include <intern_strings.h>
#include "logmsg.h"

enum {
    XMIT_FLUSH = -9,
    XMIT_NOCONSUME = -8,
    XMIT_STOPPED = -7,
    XMIT_RTCPUOFF = -6,
    XMIT_INTERNAL = -5,
    XMIT_NOTMASTER = -4,
    XMIT_NOACK = -3,
    XMIT_TOOLONG = -2,
    XMIT_BADCOMM = -1,
    XMIT_OK = 0
};

struct dbq_key {
    bbuint32_t keywords[4];
};

struct dbq_msgbuf {
    /* prccom header */
    bbint16_t prccom[4];

    /* Eight characters - CDB2_MSG */
    char comdb2[8];

    /* Total size of this buffer in bytes including prccom header */
    bbuint32_t buflen;

    /* Sending database name */
    char dbname[8];

    /* Name of queue that this message came from. */
    char queue_name[32];

    /* Should be sent as all zeroes for now. */
    bbuint32_t reserved[8];

    /* How many messages are encoded in this buffer. */
    bbuint32_t num_messages;

    /* The messages. */
    char msgs[1];
};

struct dbq_msg {
    bbuint32_t length;

    bbuint32_t frag_offset;
    bbuint32_t frag_length;

    bbuint32_t reserved;

    struct dbq_key key;

    char data[1];
};

int gbl_queue_debug = 0;
int gbl_goose_throttle = 60;
unsigned gbl_goose_add_rate = 0;      /* we should no longer need to goose
                                         the queue because bdblib now uses
                                         the queue get and consume opration as
                                         part of its regular head delete code. */
unsigned gbl_goose_consume_rate = 60; /* check for a goose every minute */

/* I've seen cases where threads don't wake up which I
   can't explain yet.  A check every 5 seconds doesn't
   sound very expensive. This is hopefully temporary. */
int gbl_queue_sleeptime = 5; /* how many seconds to sleep for between
                                polling intervals - 0 is no dumb
                                polling but wake up on demand */

int gbl_consumer_rtcpu_check = 1; /* don't send to rtcpu'd nodes.
                                     occasionally it is useful to disable this
                                     for a whee while. */
int gbl_node1rtcpuable = 0;       /* no rtcpu check for node 1 */

int gbl_reset_queue_cursor = 1;

extern int getlclbfpoolwidthbigsnd(void);

static void *dbqueue_consume_thread(void *arg);
int consume(struct ireq *iq, const void *fnd, struct consumer *consumer,
            int consumern);
static int dispatch_item(struct ireq *iq, struct consumer *consumer,
                         const struct dbq_cursor *cursor, void *item);
static int dispatch_flush(struct ireq *iq, struct consumer *consumer);
static int dispatch_fstsnd(struct ireq *iq, struct consumer *consumer,
                           const struct dbq_cursor *cursor, void *item);
static int flush_fstsnd(struct ireq *iq, struct consumer *consumer);
static void condbgf(struct consumer *consumer, const char *format, ...);
static void goose_queue(struct dbtable *db);

static void condbgf(struct consumer *consumer, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    logmsg(LOGMSG_USER, "[DBGconsumer:%s:%d]::", consumer->db->dbname,
            consumer->consumern);
    logmsgv(LOGMSG_USER, format, args);
    va_end(args);
}

void dbqueue_coalesce(struct dbenv *dbenv)
{
    /* First, go through and wake up any blocked consumer threads so they can
     * begin their clean exit path. */
    dbqueue_wake_all_consumers_all_queues(dbenv, 1);
}

void dbqueue_wake_all_consumers_all_queues(struct dbenv *dbenv, int force)
{
    int ii;
    for (ii = 0; ii < dbenv->num_qdbs; ii++) {
        if (dbenv->qdbs[ii]->dbtype == DBTYPE_QUEUE ||
            dbenv->qdbs[ii]->dbtype == DBTYPE_QUEUEDB) {
            struct dbtable *db = dbenv->qdbs[ii];
            dbqueue_wake_all_consumers(db, force);
        }
    }
}

/* Wake up the consumer thread if it is blocked on the condition
 * variable - otherwise no effect. */
static void dbqueue_wake_up_consumer(struct consumer *consumer, int force)
{
    int rc;
    rc = pthread_mutex_lock(&consumer->mutex);
    if (rc != 0)
        logmsg(LOGMSG_ERROR, "dbqueue_wake_up_consumer: "
                        "pthread_mutex_lock %d %s\n",
                rc, strerror(rc));
    else {
        if (force || consumer->waiting_for_data) {
            consumer->need_to_wake = 1;
            rc = pthread_cond_broadcast(&consumer->cond);
            if (rc != 0)
                logmsg(LOGMSG_ERROR, "dbqueue_wake_up_consumer: "
                                "pthread_cond_broadcast %d %s\n",
                        rc, strerror(rc));
        }
        rc = pthread_mutex_unlock(&consumer->mutex);
        if (rc != 0)
            logmsg(LOGMSG_ERROR, "dbqueue_wake_up_consumer: "
                            "pthread_mutex_unlock %d %s\n",
                    rc, strerror(rc));
    }
}

void dbqueue_wake_all_consumers(struct dbtable *db, int force)
{
    int consumern;
    if (db->dbtype == DBTYPE_QUEUEDB)
        pthread_rwlock_rdlock(&db->consumer_lk);
    if (db->dbtype == DBTYPE_QUEUE || db->dbtype == DBTYPE_QUEUEDB) {
        for (consumern = 0; consumern < MAXCONSUMERS; consumern++) {
            struct consumer *consumer = db->consumers[consumern];
            if (consumer && consumer->active)
                dbqueue_wake_up_consumer(consumer, force);
        }
    }
    if (db->dbtype == DBTYPE_QUEUEDB)
        pthread_rwlock_unlock(&db->consumer_lk);
}

/* Send a consumer to sleep for a while.  It can be woken up using
 * dbqueue_wake_up_consumer().  for_data indicates if we are sleeping
 * while we wait for new data. */
static void consumer_sleep(struct consumer *consumer, int seconds, int for_data)
{
    int rc;
    struct timespec ts;
    if (seconds) {
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += seconds;
    }
    rc = pthread_mutex_lock(&consumer->mutex);
    if (rc != 0)
        logmsg(LOGMSG_ERROR, "consumer_sleep: pthread_mutex_lock %d %s\n", rc,
                strerror(rc));
    else {
        consumer->waiting_for_data = for_data;
        if (consumer->need_to_wake) {
            /* No sleep; we've been told to wake up and do stuff. */
            consumer->need_to_wake = 0;
        } else if (seconds) {
            rc = pthread_cond_timedwait(&consumer->cond, &consumer->mutex, &ts);
            if (rc != 0 && rc != ETIMEDOUT)
                logmsg(LOGMSG_ERROR, "consumer_sleep: "
                                "pthread_cond_timedwait %d %s\n",
                        rc, strerror(rc));
        } else {
            rc = pthread_cond_wait(&consumer->cond, &consumer->mutex);
            if (rc != 0)
                logmsg(LOGMSG_ERROR, "consumer_sleep: "
                                "pthread_cond_wait %d %s\n",
                        rc, strerror(rc));
        }
        consumer->waiting_for_data = 0;
        rc = pthread_mutex_unlock(&consumer->mutex);
        if (rc != 0)
            logmsg(LOGMSG_ERROR, "consumer_sleep: pthread_mutex_unlock %d %s\n", rc,
                    strerror(rc));
    }
}

static int configure_consumer_with_bbhost_tag(struct consumer *consumer,
                                              char *tag)
{
    int fd;
    SBUF2 *f;
    char line[2048];
    char *tok;
    int nalloc = 0;
    char *endp;

    char *bbcpu;
    bbcpu = comdb2_location("config", "bbcpu.lst");
    fd = open(bbcpu, O_RDONLY);
    free(bbcpu);
    if (fd == -1) {
        logmsg(LOGMSG_ERROR, "open bbcpu.lst rc %d %s\n", errno, strerror(errno));
        return -1;
    }
    f = sbuf2open(fd, 0);
    if (f == NULL) {
        close(fd);
        logmsg(LOGMSG_ERROR, "can't create sbuf for file\n");
        return -1;
    }

    while (sbuf2gets(line, sizeof(line), f) > 0) {
        char *host;
        int node;

        tok = strtok_r(line, " \t\n", &endp);
        if (tok == NULL)
            continue;
        host = tok;
        tok = strtok_r(NULL, " \t\n", &endp);
        if (tok == NULL)
            continue;
        node = atoi(tok);
        if (node == 0)
            continue;

        tok = strtok_r(NULL, " \t\n", &endp);
        while (tok) {
            if (strcmp(tok, tag) == 0) {
                /* add tag */
                if (consumer->ndesthosts >= nalloc) {
                    nalloc = nalloc * 2 + 16;
                    consumer->desthosts =
                        realloc(consumer->desthosts, nalloc * sizeof(char *));
                    if (consumer->desthosts == NULL) {
                        logmsg(LOGMSG_ERROR, "out of memory allocating node list "
                                        "for consumer\n");
                        return -1;
                    }
                }
                consumer->desthosts[consumer->ndesthosts++] = strdup(host);
            }
            tok = strtok_r(NULL, " \t\n", &endp);
        }
    }
    if (consumer->ndesthosts > 0)
        consumer->desthosts =
            realloc(consumer->desthosts, consumer->ndesthosts * sizeof(char *));
    sbuf2close(f);
    return 0;
}

static void consumer_randomize_destination_list(struct consumer *consumer)
{
    char **newlist;
    if (consumer->ndesthosts == 0)
        return;

    newlist = malloc(consumer->ndesthosts * sizeof(char *));
    for (int i = 0; i < consumer->ndesthosts; i++) {
        int r;
        r = rand() % (consumer->ndesthosts - i);
        newlist[i] = consumer->desthosts[r];
        consumer->desthosts[r] =
            consumer->desthosts[consumer->ndesthosts - i - 1];
    }
    free(consumer->desthosts);
    consumer->desthosts = newlist;
}

void consumer_destroy(struct consumer *consumer)
{
    pthread_cond_destroy(&consumer->cond);
    if (consumer->ndesthosts) {
        for (int i = 0; i < consumer->ndesthosts; i++)
            free(consumer->desthosts[i]);
        free(consumer->desthosts);
        free(consumer->bbhost_tag);
    }
    free(consumer->rmtmach);
    free(consumer);
}

/* Add a consumer to a queue.
 *
 * The format is consumertype,option,option,...
 *
 * e.g. fstsnd:localhost:n100,no_rtcpu
 * */
int static dbqueue_add_consumer_int(struct dbtable *db, int consumern,
                                    const char *method, int noremove,
                                    int checkonly)
{
    struct consumer *consumer;
    const char *opts;
    int rc = 0;

    if (!checkonly && db->dbtype == DBTYPE_QUEUEDB)
        pthread_rwlock_wrlock(&db->consumer_lk);

    if (checkonly) {
        if (strncmp(method, "fstsnd:", 7) != 0 &&
            strncmp(method, "lua:", 4) != 0 &&
            strncmp(method, "dynlua:", 7) != 0) {
            logmsg(LOGMSG_ERROR, "Unsupported method: %s\n", method);
            rc = -1;
            goto done;
        }
    }

    if (!checkonly && db &&
        (db->dbtype != DBTYPE_QUEUE && db->dbtype != DBTYPE_QUEUEDB)) {
        logmsg(LOGMSG_ERROR, "dbqueue_add_consumer: %s is not a queue\n",
                db->dbname);
        rc = -1;
        goto done;
    }

    if (!checkonly && consumern < 0 || consumern >= MAXCONSUMERS) {
        logmsg(LOGMSG_ERROR, 
                "dbqueue_add_consumer: %s consumer number %d out of range\n",
                db->dbname, consumern);
        rc = -1;
        goto done;
    }

    if (!checkonly && db && consumern >= 0 && db->consumers[consumern]) {
        if (noremove) {
            logmsg(LOGMSG_ERROR, 
                    "dbqueue_add_consumer: %s consumer number %d in use already\n",
                    db->dbname, consumern);
            rc = -1;
            goto done;
        } else {
            consumer_destroy(db->consumers[consumern]);
            db->consumers[consumern] = NULL;
        }
    }

    consumer = malloc(sizeof(struct consumer));
    if (!consumer) {
        logmsg(LOGMSG_ERROR, "%s: malloc failed for consumer\n", __func__);
        rc = -1;
        goto done;
    }
    bzero(consumer, sizeof(struct consumer));
    consumer->db = db;
    consumer->consumern = consumern;
    consumer->rtcpu_mode = DFLT_RTCPU;
    consumer->ndesthosts = 0;
    consumer->desthosts = NULL;
    consumer->current_dest = 0;
    consumer->randomize_list = 0;
    consumer->bbhost_tag = NULL;
    consumer->listener_fd = -1;
    consumer->first = 1;
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
        consumer->type = CONSUMER_TYPE_LUA;
        strncpy(consumer->procedure_name, method + 4,
                sizeof(consumer->procedure_name));
    } else if (strncmp(method, "dynlua:", 7) == 0) {
        consumer->type = CONSUMER_TYPE_DYNLUA;
        strncpy(consumer->procedure_name, method + 7,
                sizeof(consumer->procedure_name));
    } else if (strncmp(method, "fstsnd:", 7) == 0) {
        char buf[128];
        char *machstr;
        char *dbnumstr;

        strncpy0(buf, method + 7, sizeof(buf));
        dbnumstr = strchr(buf, ':');
        if (!dbnumstr) {
            logmsg(LOGMSG_ERROR, "dbqueue_add_consumer: %s consumer number %d has "
                            "unknown delivery method '%s'\n",
                    db ? db->dbname : "???", consumern, method);
            free(consumer);
            rc = -1;
            goto done;
        }
        *dbnumstr = '\0';
        dbnumstr++;
        machstr = buf;

        consumer->type = CONSUMER_TYPE_FSTSND;
        consumer->dbnum = strtol(dbnumstr, NULL, 0);

        /* Is user specifies multi, defer the host check until later. */
        if (strncmp(machstr, "bbhost@", 7) == 0) {
            char *hosttag = machstr + 7;
            char *endhosttag;
            consumer->rmtmach = NULL;
            configure_consumer_with_bbhost_tag(consumer, hosttag);
            if (consumer->ndesthosts == 0) {
                logmsg(LOGMSG_ERROR, "%s:%d consumer %d: no nodes found for '%s'\n",
                        __FILE__, __LINE__, consumern, hosttag);
                free(consumer);
                rc = -1;
                goto done;
            }
            consumer->bbhost_tag = strdup(hosttag);
        } else {
            if (strcmp(machstr, "multi") != 0 &&
                bb_gethostbyname(machstr) == NULL) {
                logmsg(LOGMSG_ERROR, "%s:%d consumer %d: unknown host %s\n",
                        __FILE__, __LINE__, consumern, machstr);
                consumer_destroy(consumer);
                return -1;
            }
            consumer->rmtmach = intern(machstr);
        }

        if (consumer->dbnum <= 0) {
            logmsg(LOGMSG_ERROR, "dbqueue_add_consumer: %s consumer number %d has "
                            "unknown delivery method '%s' - invalid remote "
                            "database number\n",
                    db->dbname, consumern, method);
            free(consumer);
            rc = -1;
            goto done;
        }

        if (consumer->rmtmach == NULL && consumer->ndesthosts == 0) {
            logmsg(LOGMSG_ERROR, "dbqueue_add_consumer: %s consumer number %d has "
                            "unknown delivery method '%s' - invalid remote "
                            "machine name or number '%s'\n",
                    db->dbname, consumern, method, machstr);
            free(consumer);
            rc = -1;
            goto done;
        }
    } else if (strncmp(method, "javasp:", 7) == 0) {
        consumer->type = CONSUMER_TYPE_JAVASP;
        strncpy(consumer->procedure_name, method + 7,
                sizeof(consumer->procedure_name));
    } else if (strcmp(method, "api") == 0) {
        consumer->type = CONSUMER_TYPE_API;
    } else if (strcmp(method, "remove") == 0 && !noremove) {
        free(consumer);
        consumer = NULL;
    } else {
        logmsg(LOGMSG_ERROR, "dbqueue_add_consumer: %s consumer number %d has "
                        "unknown delivery method '%s'\n",
                db->dbname, consumern, method);
        free(consumer);
        rc = -1;
        goto done;
    }

    /* might have been a "remove" command, so consumer may be NULL */
    if (consumer) {
        /* Allow unrecognised options */
        dbqueue_set_consumer_options(consumer, opts);

        pthread_mutex_init(&consumer->mutex, NULL);
        pthread_cond_init(&consumer->cond, NULL);

        if (consumer->randomize_list)
            consumer_randomize_destination_list(consumer);
    }

    if (!checkonly && db)
        db->consumers[consumern] = consumer;
    if (checkonly) {
        consumer_destroy(consumer);
    }
    rc = 0;

done:
    if (!checkonly && db->dbtype == DBTYPE_QUEUEDB)
        pthread_rwlock_unlock(&db->consumer_lk);
    return rc;
}

int dbqueue_add_consumer(struct dbtable *db, int consumern, const char *method,
                         int noremove)
{
    return dbqueue_add_consumer_int(db, consumern, method, noremove, 0);
}

/* Does all the steps of adding the consumer, minus the adding. */
int dbqueue_check_consumer(const char *method)
{
    return dbqueue_add_consumer_int(NULL, -1, method, 0, 1);
}

int dbqueue_set_consumern_options(struct dbtable *db, int consumern,
                                  const char *opts)
{
    struct consumer *consumer;
    if (consumern < 0 || consumern >= MAXCONSUMERS)
        consumer = NULL;
    else
        consumer = db->consumers[consumern];
    if (consumer)
        return dbqueue_set_consumer_options(consumer, opts);
    else {
        logmsg(LOGMSG_ERROR, "Bad consumer number %d for queue %s\n", consumern,
                db->dbname);
        return -1;
    }
}

int dbqueue_set_consumer_options(struct consumer *consumer, const char *opts)
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
        logmsg(LOGMSG_USER, "Queue %s consumer %d option '%s'\n", consumer->db->dbname,
               consumer->consumern, tok);
        if (strcmp(tok, "no_rtcpu") == 0)
            consumer->rtcpu_mode = NO_RTCPU;
        else if (strcmp(tok, "use_rtcpu") == 0)
            consumer->rtcpu_mode = USE_RTCPU;
        else if (strcmp(tok, "dflt_rtcpu") == 0)
            consumer->rtcpu_mode = DFLT_RTCPU;
        else if (strcmp(tok, "randomize_list") == 0)
            consumer->randomize_list = 1;
        else {
            logmsg(LOGMSG_USER, "Bad option for queue %s consumer %d: '%s'\n",
                    consumer->db->dbname, consumer->consumern, tok);
            nerrors++;
        }

        tok = strtok_r(NULL, delims, &lasts);
    }

    return nerrors;
}

/* This gets called once a second from purge_old_blkseq_thread().
 * If we have become master we make sure that we have threads in place
 * for each consumer. */
void dbqueue_admin(struct dbenv *dbenv)
{
    int iammaster = (dbenv->master == machine()) ? 1 : 0;

    pthread_mutex_lock(&dbenv->dbqueue_admin_lk);
    if (dbenv->dbqueue_admin_running) {
        pthread_mutex_unlock(&dbenv->dbqueue_admin_lk);
        return;
    }
    dbenv->dbqueue_admin_running = 1;
    pthread_mutex_unlock(&dbenv->dbqueue_admin_lk);

    /* if we are master then make sure all the queues are running */
    if (iammaster && !dbenv->stopped && !dbenv->exiting) {
        for (int ii = 0; ii < dbenv->num_qdbs; ii++) {
            if (dbenv->qdbs[ii] == NULL)
                continue;
            if (dbenv->qdbs[ii]->dbtype == DBTYPE_QUEUE ||
                dbenv->qdbs[ii]->dbtype == DBTYPE_QUEUEDB) {
                struct dbtable *db = dbenv->qdbs[ii];
                if (db->dbtype == DBTYPE_QUEUEDB)
                    pthread_rwlock_rdlock(&db->consumer_lk);
                for (int consumern = 0; consumern < MAXCONSUMERS; consumern++) {
                    struct consumer *consumer = db->consumers[consumern];
                    if (!consumer)
                        continue;
                    switch (consumer->type) {
                    case CONSUMER_TYPE_FSTSND:
                    case CONSUMER_TYPE_JAVASP:
                        if (consumer && !consumer->active &&
                            !consumer->stopped) {
                            consumer->need_to_wake = 0;
                            consumer->active = 1;
                            MEMORY_SYNC;
                            pthread_t tid;
                            int rc = pthread_create(
                                &tid, &gbl_pthread_attr_detached,
                                dbqueue_consume_thread, consumer);
                            if (rc < 0) {
                                consumer->active = 0;
                                MEMORY_SYNC;
                                logmsg(LOGMSG_USER, "%s: cannot create %s consumer %d:%d %s\n",
                                    __func__, db->dbname, consumern, errno,
                                    strerror(errno));
                            }
                        }
                        break;
                    case CONSUMER_TYPE_LUA:
                        if (!trigger_registered(consumer->procedure_name)) {
                            char *name = consumer->procedure_name;
                            char *host;
                            host = net_get_osql_node(thedb->handle_sibling);
                            if (host == NULL && thedb->nsiblings == 1) {
                                trigger_start(name); // standalone
                            } else {
                                void *net = thedb->handle_sibling;
                                net_send_message(net, host, NET_TRIGGER_START,
                                                 name, strlen(name) + 1, 0, 0);
                            }
                        }
                        break;
                    }
                }
                if (db->dbtype == DBTYPE_QUEUEDB)
                    pthread_rwlock_unlock(&db->consumer_lk);

                if (dbenv->qdbs[ii]->dbtype == DBTYPE_QUEUE) {
                    /* Periodically check for a goose at the queue head and
                     * remove it.  Don't goose btree-based queues. */
                    if (gbl_goose_consume_rate &&
                        ++(db->goose_consume_cnt) >= gbl_goose_consume_rate) {
                        db->goose_consume_cnt = 0;
                        goose_queue(db);
                    }

                    /* periodically add a goose to the end of the queue. */
                    if (gbl_goose_add_rate &&
                        ++(db->goose_add_cnt) >= gbl_goose_add_rate) {
                        db->goose_add_cnt = 0;
                        dbqueue_goose(db, 0);
                    }
                }
            }
        }
    }

    pthread_mutex_lock(&dbenv->dbqueue_admin_lk);
    dbenv->dbqueue_admin_running = 0;
    pthread_mutex_unlock(&dbenv->dbqueue_admin_lk);
}

/* Prevent silliness by consuming the head of the queue.
 * The "goose" concept is that we add dummy records specifically to be
 * consumed in this way, thus ensuring that the queue head advances and we
 * don't end up with millions of empty queue extents. */
static void goose_queue(struct dbtable *db)
{
    tran_type *trans;
    struct ireq iq;
    int rc, retries, debug = 0;
    int num_goosed = 0;

    int iammaster = (db->dbenv->master == machine()) ? 1 : 0;

    if (!iammaster || db->dbenv->stopped || db->dbenv->exiting)
        return;

    if (gbl_queue_debug > 0) {
        debug = 1;
        gbl_queue_debug--;
    }

    init_fake_ireq(db->dbenv, &iq);
    iq.usedb = db;

again:

    /* First do a non transactional read to see if the head of the queue is a
     * goose - if it isn't then don't go any further. */
    rc = dbq_check_goose(&iq, NULL);
    if (debug)
        logmsg(LOGMSG_USER, "[GOOSE:%s]::dbq_check_goose rc=%d\n", db->dbname, rc);
    if (rc != 0)
        return;

    for (retries = 0; retries < gbl_maxretries; retries++) {
        tran_type *trans;
        int rc;
        int startms, diffms;

        /* If we have to retry always sleep so we don't win against real
         * database threads. */
        if (retries > 0)
            poll(0, 0, (rand() % 250 + 100));

        rc = trans_start(&iq, NULL, &trans);
        if (rc != 0) {
            if (debug)
                logmsg(LOGMSG_USER, "[GOOSE:%s]::trans_start rc=%d\n", db->dbname, rc);
            return;
        }

        /* Check again for goose under transaction. */
        rc = dbq_check_goose(&iq, trans);
        if (debug)
            logmsg(LOGMSG_USER, "[GOOSE:%s]::dbq_check_goose (trans) rc=%d\n", db->dbname,
                   rc);
        if (rc != 0) {
            trans_abort(&iq, trans);
            if (rc == RC_INTERNAL_RETRY)
                continue;
            else
                return;
        }

        /* We're pretty sure that there's a goose in the queue.  Consume the
         * head.  If it turns out that it wasn't a goose then we MUST roll back
         * because otherwise we've consumed something we shouldn't have! */
        {
            int gotlk = 0;

            if (gbl_exclusive_blockop_qconsume) {
                pthread_rwlock_wrlock(&gbl_block_qconsume_lock);
                gotlk = 1;
            }

            rc = dbq_consume_goose(&iq, trans);
            if (debug)
                logmsg(LOGMSG_USER, "[GOOSE:%s]::dbq_consume_goose rc=%d\n", db->dbname, rc);
            if (rc != 0) {
                trans_abort(&iq, trans);
                if (gotlk)
                    pthread_rwlock_unlock(&gbl_block_qconsume_lock);
                if (rc == RC_INTERNAL_RETRY)
                    continue;
                else
                    return;
            }

            rc = trans_commit(&iq, trans, 0);
            if (debug)
                logmsg(LOGMSG_USER, "[GOOSE:%s]::trans_commit rc=%d\n", db->dbname, rc);
            if (rc != RC_INTERNAL_RETRY) {
                /* If we consumed a goose then go again, but make sure we don't
                 * end up running hot on CPU just consuming geese.  If that
                 * happens
                 * the real question should be, why is the queue so full of
                 * geese?
                 */
                db->num_goose_consumes++;
                if (++num_goosed < gbl_goose_throttle) {
                    if (gotlk)
                        pthread_rwlock_unlock(&gbl_block_qconsume_lock);
                    goto again;
                }
                logmsg(LOGMSG_DEBUG, "goose_queue: consumed %d geese in one go\n",
                       num_goosed);

                if (gotlk)
                    pthread_rwlock_unlock(&gbl_block_qconsume_lock);

                return;
            }

            if (gotlk)
                pthread_rwlock_unlock(&gbl_block_qconsume_lock);
        }
    }

    logmsg(LOGMSG_WARN, "goose_queue: too much contention %d\n", retries);
}

/* Add a goose record to the given queue */
void dbqueue_goose(struct dbtable *db, int force)
{
    struct ireq iq;
    int rc, retries, debug = 0;
    int gotlk = 0;

    int iammaster = (db->dbenv->master == machine()) ? 1 : 0;

    if (!iammaster || db->dbenv->stopped || db->dbenv->exiting)
        return;

    if (gbl_queue_debug > 0) {
        debug = 1;
        gbl_queue_debug--;
    }

    init_fake_ireq(db->dbenv, &iq);
    iq.usedb = db;

    /* First do a non transactional read to see if the head of the queue is a
     * goose - if it is then don't add another, we don't want a queue that is
     * full of geese. */
    if (!force) {
        rc = dbq_check_goose(&iq, NULL);
        if (debug)
            logmsg(LOGMSG_USER, "[GOOSE:%s]::dbq_check_goose rc=%d\n", db->dbname, rc);
        if (rc == 0)
            return;
    }

    for (retries = 0; retries < gbl_maxretries; retries++) {
        tran_type *trans;
        int rc;
        int startms, diffms;

        /* If we have to retry always sleep so we don't win against real
         * database threads. */
        if (retries > 0)
            poll(0, 0, (rand() % 250 + 100));

        if (gbl_exclusive_blockop_qconsume) {
            pthread_rwlock_wrlock(&gbl_block_qconsume_lock);
            gotlk = 1;
        }

        rc = trans_start(&iq, NULL, &trans);
        if (rc != 0) {
            if (debug)
                logmsg(LOGMSG_USER, "[ADDGOOSE:%s]::trans_start rc=%d\n", db->dbname, rc);
            if (gotlk)
                pthread_rwlock_unlock(&gbl_block_qconsume_lock);
            return;
        }

        rc = dbq_add_goose(&iq, trans);
        if (debug)
            logmsg(LOGMSG_USER, "[ADDGOOSE:%s]:: dbq_add_goose rc=%d\n", db->dbname, rc);
        if (rc != 0) {
            trans_abort(&iq, trans);
            if (gotlk)
                pthread_rwlock_unlock(&gbl_block_qconsume_lock);
            if (rc == RC_INTERNAL_RETRY)
                continue;
            else
                return;
        }

        rc = trans_commit(&iq, trans, 0);
        if (debug)
            logmsg(LOGMSG_USER, "[ADDGOOSE:%s]:: trans_commit rc=%d\n", db->dbname, rc);
        if (rc != RC_INTERNAL_RETRY) {
            if (gotlk)
                pthread_rwlock_unlock(&gbl_block_qconsume_lock);
            if (rc == 0)
                db->num_goose_adds++;
            return;
        }
        if (gotlk)
            pthread_rwlock_unlock(&gbl_block_qconsume_lock);
    }

    logmsg(LOGMSG_WARN, "dbqueue_goose: too much contention %d\n", retries);
}

struct consumer_stat {
    int has_stuff;
    size_t first_item_length;
    time_t epoch;
    int depth;
};

static int dbqueue_stat_callback(int consumern, size_t length,
                                 unsigned int epoch, void *userptr)
{
    struct consumer_stat *stats = userptr;

    if (consumern < 0 || consumern >= MAXCONSUMERS) {
        logmsg(LOGMSG_USER, "dbqueue_stat_callback: consumern=%d length=%u epoch=%u\n",
                consumern, (unsigned)length, epoch);
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

static void dbqueue_stat_thread_int(struct dbtable *db, int fullstat, int walk_queue)
{
    if (db->dbtype != DBTYPE_QUEUE && db->dbtype != DBTYPE_QUEUEDB)
        logmsg(LOGMSG_ERROR, "'%s' is not a queue\n", db->dbname);
    else {
        int ii, rc;
        struct ireq iq;
        struct consumer_stat stats[MAXCONSUMERS];
        int flags = 0;
        const struct bdb_queue_stats *bdbstats;

        bdbstats = bdb_queue_get_stats(db->handle);

        init_fake_ireq(db->dbenv, &iq);
        iq.usedb = db;

        bzero(stats, sizeof(stats));
        logmsg(LOGMSG_USER, "(scanning queue '%s' for stats, please wait...)\n", db->dbname);
        if (!walk_queue)
            flags = BDB_QUEUE_WALK_FIRST_ONLY;
        if (fullstat)
            flags |= BDB_QUEUE_WALK_KNOWN_CONSUMERS_ONLY;
        rc = dbq_walk(&iq, flags, dbqueue_stat_callback, stats);

        logmsg(LOGMSG_USER, "queue '%s':-\n", db->dbname);
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
            pthread_rwlock_rdlock(&db->consumer_lk);
        for (ii = 0; ii < MAXCONSUMERS; ii++) {
            struct consumer *consumer = db->consumers[ii];

            if (!fullstat && !consumer && !stats[ii].has_stuff)
                continue;

            if (consumer) {
                logmsg(LOGMSG_USER, "  [consumer %02d]: ", ii);
                switch (consumer->type) {
                case CONSUMER_TYPE_API:
                    logmsg(LOGMSG_USER, "api\n");
                    break;

                case CONSUMER_TYPE_FSTSND:
                    if (consumer->ndesthosts) {
                        logmsg(LOGMSG_USER, "fstsnd to hosts %s [", consumer->bbhost_tag);
                        for (int i = 0; i < consumer->ndesthosts; i++) {
                            logmsg(LOGMSG_USER, "%s", consumer->desthosts[i]);
                            if (i == consumer->current_dest)
                                logmsg(LOGMSG_USER, "*");
                            if (i != consumer->ndesthosts - 1)
                                logmsg(LOGMSG_USER, " ");
                        }
                        logmsg(LOGMSG_USER, "] dbnum %d\n", consumer->dbnum);
                    } else {
                        logmsg(LOGMSG_USER, "fstsnd to host %s dbnum %d\n",
                               consumer->rmtmach, consumer->dbnum);
                    }

                    if (consumer->n_good_fstsnds)
                        logmsg(LOGMSG_USER, "    %u good fstsnds, avg time %u ms\n",
                               consumer->n_good_fstsnds,
                               consumer->ms_good_fstsnds /
                                   consumer->n_good_fstsnds);
                    else
                        logmsg(LOGMSG_USER, "    %u good fstsnds\n", 0);

                    logmsg(LOGMSG_USER, "    %u bad fstsnds, %u bad rtcpu fstsnds, %u "
                           "rejected fstsnds\n",
                           consumer->n_bad_fstsnds,
                           consumer->n_bad_rtcpu_fstsnds,
                           consumer->n_rejected_fstsnds);
                    break;

                case CONSUMER_TYPE_JAVASP:
                    logmsg(LOGMSG_USER, "stored procedure %s\n", consumer->procedure_name);
                    logmsg(LOGMSG_USER, "    %u commits, %u aborts, %u retries\n",
                           consumer->n_commits, consumer->n_aborts,
                           consumer->n_retries);
                    break;

                default:
                    logmsg(LOGMSG_USER, "unknown consumer type %d?!\n", consumer->type);
                    break;
                }
                logmsg(LOGMSG_USER, "    %u messages consumed\n", consumer->n_consumed);
                switch (consumer->rtcpu_mode) {
                case NO_RTCPU:
                    logmsg(LOGMSG_USER, "    rtcpu override: no rtcpu checks\n");
                    break;
                case USE_RTCPU:
                    logmsg(LOGMSG_USER, "    rtcpu override: using rtcpu checks\n");
                    break;
                }
                if (walk_queue)
                    logmsg(LOGMSG_USER, "    %d items on queue\n", stats[ii].depth);
            } else
                logmsg(LOGMSG_USER, "  [consumer %02d]: none\n", ii);

            if (stats[ii].has_stuff) {
                unsigned int now = time_epoch();
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

                logmsg(LOGMSG_USER, "    head item length %u age %u:%02u:%02u created %u %s",
                       (unsigned)stats[ii].first_item_length, hr, mn, sc,
                       stats[ii].epoch, buf);
            } else if (consumer)
                logmsg(LOGMSG_USER, "    empty\n");
        }
        if (db->dbtype == DBTYPE_QUEUEDB)
            pthread_rwlock_unlock(&db->consumer_lk);

        logmsg(LOGMSG_USER, "-----\n");
    }
}

struct statthrargs {
    struct dbtable *db;
    int fullstat;
    int walk_queue;
};

static void *dbqueue_stat_thread(void *argsptr)
{
    struct statthrargs *args = argsptr;
    struct thr_handle *thr_self = thrman_register(THRTYPE_QSTAT);
    thread_started("dbque stat");
    backend_thread_event(args->db->dbenv, COMDB2_THR_EVENT_START_RDONLY);
    dbqueue_stat_thread_int(args->db, args->fullstat, args->walk_queue);
    backend_thread_event(args->db->dbenv, COMDB2_THR_EVENT_DONE_RDONLY);
    free(args);
    return NULL;
}

void dbqueue_stat(struct dbtable *db, int full, int walk_queue, int blocking)
{
    int rc;
    pthread_t tid;
    pthread_attr_t attr;
    struct statthrargs *args;

    args = calloc(1, sizeof(struct statthrargs));
    if (!args) {
        logmsg(LOGMSG_ERROR, "dbqueue_stat: calloc failed\n");
        return;
    }
    args->db = db;
    args->fullstat = full;
    args->walk_queue = walk_queue;

    if (blocking) {
        pthread_attr_init(&attr);
        pthread_attr_setstacksize(&attr, DEFAULT_THD_STACKSZ);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    }

    rc = pthread_create(&tid, blocking ? &attr : &gbl_pthread_attr_detached,
                        dbqueue_stat_thread, args);

    if (blocking) {
        pthread_attr_destroy(&attr);
    }

    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "dbqueue_stat: pthread_create failed rc=%d %s\n", rc,
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

static void dbqueue_flush(struct dbtable *db, int consumern)
{
    struct ireq iq;
    int nflush = 0;

    init_fake_ireq(db->dbenv, &iq);
    iq.usedb = db;

   logmsg(LOGMSG_INFO, "Beginning flush for queue '%s' consumer %d\n", db->dbname,
           consumern);

    if (db->dbenv->master != machine()) {
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

        rc = consume(&iq, item, NULL, consumern);
        free(item);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "Terminating after consume rcode %d\n", rc);
            break;
        }
        if (++nflush % 100 == 0) {
            logmsg(LOGMSG_INFO, "... flushed %d items\n", nflush);
        }
    }

    logmsg(LOGMSG_INFO, "Done flush for queue '%s' consumer %d, flushed %d items\n",
           db->dbname, consumern, nflush);
}

struct flush_thd_data {
    struct dbtable *db;
    int consumern;
};

static void *dbqueue_flush_thd(void *argsptr)
{
    struct flush_thd_data *args = argsptr;
    thread_started("dbque flush");
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);
    dbqueue_flush(args->db, args->consumern);
    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
    if (flush_thread_active)
        flush_thread_active = 0;
    free(args);
    return NULL;
}

void dbqueue_flush_in_thread(struct dbtable *db, int consumern)
{
    pthread_attr_t attr;
    struct flush_thd_data *args;
    int rc;
    pthread_t tid;

    if (flush_thread_active) {
        logmsg(LOGMSG_ERROR, "Flush operation already in progress\n");
        return;
    }

    rc = pthread_attr_init(&attr);
    if (rc) {
        logmsgperror("dbqueue_flush_in_thread:pthread_attr_init");
        return;
    }
    PTHD_ATTR_SETDETACHED(attr, rc);
    if (rc) {
        logmsgperror("dbqueue_flush_in_thread:pthread_attr_setdetached");
        pthread_attr_destroy(&attr);
        return;
    }
    pthread_attr_setstacksize(&attr, DEFAULT_THD_STACKSZ);

    args = malloc(sizeof(struct flush_thd_data));
    if (!args) {
        pthread_attr_destroy(&attr);
        logmsg(LOGMSG_ERROR, "dbqueue_flush_in_thread: out of memory\n");
        return;
    }
    bzero(args, sizeof(struct flush_thd_data));
    args->db = db;
    args->consumern = consumern;

    flush_thread_active = 1;
    rc = pthread_create(&tid, &attr, dbqueue_flush_thd, args);
    if (rc != 0) {
        flush_thread_active = 0;
        logmsg(LOGMSG_ERROR, "dbqueue_flush_in_thread: pthread_create error %d %s\n",
                rc, strerror(rc));
        free(args);
    }
    rc = pthread_attr_destroy(&attr);
    if (rc) {
        logmsgperror("dbqueue_flush_in_thread:pthread_attr_destroy");
        return;
    }
}

void dbqueue_flush_abort(void)
{
    if (flush_thread_active) {
        logmsg(LOGMSG_INFO, "Signalling flush abort\n");
        flush_thread_active = 0;
    } else {
        logmsg(LOGMSG_ERROR, "No flush thread is active\n");
    }
}

/* Check that it is ok to send to the consumer's machine. */
static int check_consumer_destination(struct consumer *consumer)
{
    if (consumer->type == CONSUMER_TYPE_LUA ||
        consumer->type == CONSUMER_TYPE_DYNLUA) {
        return 1;
    }

    char *dest = consumer->rmtmach;
    if (consumer->ndesthosts)
        dest = consumer->desthosts[consumer->current_dest];

    /* See if we don't do a rtcpu check */
    if (!gbl_consumer_rtcpu_check)
        return 1;

    if (consumer->rtcpu_mode != USE_RTCPU) {
        if (consumer->rtcpu_mode == NO_RTCPU)
            return 1;

        /* by default, no rtcpu checks for local destinations */
        if (dest == NULL || dest == gbl_mynode)
            return 1;
    }

    if (machine_is_up(dest) != 1) {
        return 0;
    }

    /* Destination is ok */
    return 1;
}

static void stop_consumer(struct consumer *consumer)
{
    // These get offloaded to replicants, not run locally.
    // Let them get error from missing queue
    if (consumer->type == CONSUMER_TYPE_DYNLUA ||
        consumer->type == CONSUMER_TYPE_LUA) {
        return;
    }

    // TODO CONSUMER_TYPE_LUA,
    pthread_mutex_lock(&consumer->mutex);
    consumer->please_stop = 1;
    pthread_cond_signal(&consumer->cond);
    pthread_mutex_unlock(&consumer->mutex);

    pthread_mutex_lock(&consumer->mutex);
    if (consumer->stopped) {
        pthread_mutex_unlock(&consumer->mutex);
        return;
    }
    while (!consumer->stopped) {
        pthread_cond_wait(&consumer->cond, &consumer->mutex);
        pthread_mutex_unlock(&consumer->mutex);
    }
}

/* doesn't actually start, dbqueue_admin will do that, but
 * mark "no longer stopped" so dbqueue_admin can do it's thing */
static void restart_consumer(struct consumer *consumer)
{
    pthread_mutex_lock(&consumer->mutex);
    consumer->stopped = 0;
    pthread_mutex_unlock(&consumer->mutex);
}

void dbqueue_stop_consumers(struct dbtable *db)
{
    if (db->dbtype == DBTYPE_QUEUEDB)
        pthread_rwlock_rdlock(&db->consumer_lk);
    for (int i = 0; i < MAXCONSUMERS; i++) {
        if (db->consumers[i])
            stop_consumer(db->consumers[i]);
    }
    if (db->dbtype == DBTYPE_QUEUEDB)
        pthread_rwlock_unlock(&db->consumer_lk);
}

void dbqueue_restart_consumers(struct dbtable *db)
{
    if (db->dbtype == DBTYPE_QUEUEDB)
        pthread_rwlock_rdlock(&db->consumer_lk);
    for (int i = 0; i < MAXCONSUMERS; i++) {
        if (db->consumers[i])
            restart_consumer(db->consumers[i]);
    }
    if (db->dbtype == DBTYPE_QUEUEDB)
        pthread_rwlock_unlock(&db->consumer_lk);
}

static void *dbqueue_consume_thread(void *arg)
{
    struct consumer *consumer = arg;
    struct dbtable *db = consumer->db;
    struct dbenv *dbenv = db->dbenv;
    int iammaster = (dbenv->master == machine()) ? 1 : 0;
    struct ireq iq;
    struct dbq_cursor last;
    int ii;
    struct thr_handle *thr_self;

    thread_started("dbqueue consume");

    thr_self = thrman_register(THRTYPE_CONSUMER);

    /* don't allow broadcasts to prod, that would be bad */
    if (consumer->rmtmach && !allow_broadcast_to_remote(consumer->rmtmach)) {
        if (!consumer->complained) {
            logmsg(LOGMSG_WARN, "consumer %d for queue '%s' WILL NOT START - "
                            "I will not broadcast to %s machine %s!\n",
                    consumer->consumern, db->dbname,
                    get_mach_class_str(consumer->rmtmach), consumer->rmtmach);
            consumer->complained = 1;
        }
        consumer->active = 0;
        MEMORY_SYNC;
        return NULL;
    }

    consumer->complained = 0;

    logmsg(LOGMSG_INFO, "consumer %d starting for queue '%s'\n", consumer->consumern,
           db->dbname);

    pthread_mutex_lock(&consumer->mutex);
    consumer->stopped = 0;
    pthread_mutex_unlock(&consumer->mutex);

    backend_thread_event(dbenv, COMDB2_THR_EVENT_START_RDWR);

    init_fake_ireq(dbenv, &iq);
    iq.usedb = db;
    bzero(&last, sizeof(last));
    bzero(consumer->items, sizeof(consumer->items));
    consumer->numitems = 0;

    while (!dbenv->stopped && !dbenv->exiting && dbenv->master == machine() &&
           !consumer->please_stop) {
        int rc;
        char *dta = NULL;
        size_t dtalen = 0, dtaoff = 0;
        unsigned int epoch;
        void *fnd = NULL;
        struct dbq_cursor next;
        int diffms, startms;

        /* back off if lock desired */
        if (bdb_the_lock_desired()) {
            logmsg(LOGMSG_WARN, "dbqueue_consume_thread sleeping as lock desired\n");
            sleep(10);
            continue;
        }

        /* No locking; it's only debug trace control */
        if (gbl_queue_debug > 0) {
            gbl_queue_debug--;
            consumer->debug = 1;
        } else
            consumer->debug = 0;

        /* If destination machine is routed off then don't try.  If dest machine
         * is irrelevant for this consumer type then it should be marked
         * as zero. */
        if (!check_consumer_destination(consumer)) {
            if (consumer->debug)
                condbgf(consumer, "%s is rtcpu'd off\n", consumer->rmtmach);

            if (consumer->ndesthosts) {
                int start = consumer->current_dest;
                for (consumer->current_dest =
                         (consumer->current_dest + 1) % consumer->ndesthosts;
                     consumer->current_dest != start;
                     consumer->current_dest =
                         (consumer->current_dest + 1) % consumer->ndesthosts) {

                    if (check_consumer_destination(consumer))
                        break;
                }

                if (consumer->current_dest == start) {
                    /* No suitable nodes to send to.  Replicate the kludgery
                     * below. */
                    if (gbl_replicate_local)
                        consumer_sleep(consumer, 5, 0);
                    else {
                        consumer_sleep(consumer, 60, 0);
                    }
                }
            } else {

                /* sleep for a minute */
                if (gbl_replicate_local)
                    /* this is a kludge.  if there a local replicant, it can
                       clear
                       a table (and write a record to broadcast that change)
                       while
                       consumers are down. Another modification to the db
                       will accomplish the same thing.  Sleep for 5 seconds
                       instead
                       of 60 to minimize lag time.  This is crappy and needs to
                       change
                       but it'll do for now. */
                    consumer_sleep(consumer, 5, 0);
                else
                    consumer_sleep(consumer, 60, 0);
            }
            continue;
        }

        /* Read the next thing from the queue */
        bdb_reset_thread_stats();
        thrman_where(thr_self, "dbq_get");
        startms = time_epochms();
        rc = dbq_get(&iq, consumer->consumern, &last, (void **)&dta, &dtalen,
                     &dtaoff, &next, &epoch);
        diffms = time_epochms() - startms;
        thrman_where(thr_self, NULL);

        if (diffms > LONG_REQMS) {
            const struct bdb_thread_stats *t = bdb_get_thread_stats();
            logmsg(LOGMSG_WARN,
                   "LONG REQUEST %-8d msec  consumer thread queue '%s' "
                   "consumer %d dbq_get\n",
                   diffms, consumer->db->dbname, consumer->consumern);
            if (t->n_lock_waits > 0) {
                logmsg(LOGMSG_WARN, "  %u lock waits took %u ms (%u ms/wait)\n",
                       t->n_lock_waits, U2M(t->lock_wait_time_us),
                       U2M(t->lock_wait_time_us / t->n_lock_waits));
            }
            if (t->n_preads > 0) {
                logmsg(LOGMSG_WARN,
                       "  %u preads took %u ms total of %u bytes\n",
                       t->n_preads, U2M(t->pread_time_us), t->pread_bytes);
            }
            if (t->n_pwrites > 0) {
                logmsg(LOGMSG_WARN,
                       "  %u pwrites took %u ms total of %u bytes\n",
                       t->n_pwrites, U2M(t->pwrite_time_us), t->pwrite_bytes);
            }
            if (t->n_memp_fgets > 0) {
                logmsg(LOGMSG_WARN, "  %u __memp_fget calls took %u ms\n",
                       t->n_memp_fgets, U2M(t->memp_fget_time_us));
            }
            if (t->n_shallocs > 0 || t->n_shalloc_frees > 0) {
                logmsg(LOGMSG_WARN,
                       "  %u shallocs took %u ms, %u shalloc_frees "
                       "took %u ms\n",
                       t->n_shallocs, U2M(t->shalloc_time_us),
                       t->n_shalloc_frees, U2M(t->shalloc_free_time_us));
            }
        }
        if (rc == 0) {
            unsigned long long genid;

            /* Make sure that the found item isn't on the waiting for
             * replication list.  If it is, block until it isn't.  This
             * prevents us from broadcasting about a transaction that
             * hasn't yet replicated to all nodes. */
            genid = dbq_item_genid(dta);
            wait_for_genid_repl(genid);

            /* Send this item off for dispatch.  This will either succeed or it
             * will keep trying until we are not master and need to stop.
             * Responsibility for the memory is passed on. */
            thrman_where(thr_self, "dispatch_item");
            rc = dispatch_item(&iq, consumer, &next, dta);
            thrman_where(thr_self, NULL);
            if (rc == 0) {
                memcpy(&last, &next, sizeof(last));

                /* If we have no items outstanding (i.e. no partially formed
                 * fstsnd buffers waiting to be sent or anything like that)
                 * then we can safely reset our read cursor.  This ensures that
                 * we read from the front of the queue and so do not miss
                 * anything that might have a lower seq # than what we just
                 * read but had not yet been committed.
                 * This will hurt performance if there are many unconsumed
                 * items for other consumers at the front of the queue but
                 * you know what?  Just don't let that happen. */
                if (gbl_reset_queue_cursor && consumer->numitems == 0) {
                    bzero(&last, sizeof(last));
                }
            } else {
                /* If we hit any kind of an error then we must reset our
                 * search cursor so that we retry with the correct items. */
                bzero(&last, sizeof(last));

                /* Don't go into a hot loop if bigsnd is down etc.
                 * If dest machine is rtcpu'd then sleep for much longer than
                 * otherwise. */
                if (consumer->type == CONSUMER_TYPE_FSTSND &&
                    consumer->rmtmach != NULL &&
                    !machine_is_up(consumer->rmtmach) != 1)
                    consumer_sleep(consumer, 60, 0);
                else
                    consumer_sleep(consumer, 1, 0);
            }
        } else {
            /* There is nothing more.  Flush the dispatcher. */
            if (dispatch_flush(&iq, consumer) != 0) {
                /* Something went wrong trying to flush, so we go back to
                 * the beginning. */
                bzero(&last, sizeof(last));
            }

            /* Sleep until woken up with new data. */
            consumer_sleep(consumer, gbl_queue_sleeptime, 1);
        }
    }

    backend_thread_event(dbenv, COMDB2_THR_EVENT_DONE_RDWR);

    /* free any items that we couldn't send. */
    for (ii = 0; ii < consumer->numitems; ii++) {
        if (consumer->items[ii])
            free(consumer->items[ii]);
    }
    bzero(consumer->items, sizeof(consumer->items));
    consumer->numitems = 0;

    logmsg(LOGMSG_INFO, "consumer %d exiting for queue '%s'\n", consumer->consumern,
           db->dbname);
    consumer->active = 0;
    MEMORY_SYNC;

    pthread_mutex_lock(&consumer->mutex);
    if (consumer->please_stop) {
        consumer->please_stop = 0;
        consumer->stopped = 1;
    }
    pthread_cond_signal(&consumer->cond);
    pthread_mutex_unlock(&consumer->mutex);

    return NULL;
}

int consume(struct ireq *iq, const void *fnd, struct consumer *consumer,
            int consumern)
{
    const int sleeptime = 1;
    int gotlk = 0;

    if (consumer)
        consumern = consumer->consumern;

    /* Outer loop - long sleep between retries */
    while (1) {
        int retries;

        /* Inner loop - short delay between retries */
        for (retries = 0; retries < gbl_maxretries; retries++) {
            tran_type *trans;
            int rc;
            int startms, diffms;

            if (retries > 10)
                poll(0, 0, (rand() % 25 + 1));

            if (gbl_exclusive_blockop_qconsume) {
                pthread_rwlock_wrlock(&gbl_block_qconsume_lock);
                gotlk = 1;
            }

            rc = trans_start(iq, NULL, &trans);
            if (rc != 0) {
                if (consumer && consumer->debug)
                    condbgf(consumer, "trans_start rc=%d\n", rc);
                if (gotlk)
                    pthread_rwlock_unlock(&gbl_block_qconsume_lock);
                return -1;
            }

            rc = dbq_consume(iq, trans, consumern, fnd);
            if (consumer && consumer->debug)
                condbgf(consumer, "dbq_consume rc=%d\n", rc);
            if (rc != 0) {
                trans_abort(iq, trans);
                if (gotlk)
                    pthread_rwlock_unlock(&gbl_block_qconsume_lock);
                if (rc == RC_INTERNAL_RETRY)
                    continue;
                else if (rc == IX_NOTFND)
                    return 0;
                else
                    break;
            }

            rc = trans_commit(iq, trans, 0);
            if (gotlk)
                pthread_rwlock_unlock(&gbl_block_qconsume_lock);
            if (consumer && consumer->debug)
                condbgf(consumer, "trans_commit rc=%d\n", rc);
            if (rc == 0)
                return 0;
            else if (rc == RC_INTERNAL_RETRY)
                continue;
            else if (rc == ERR_NOMASTER)
                return -1;
            else
                break;
        }

        logmsg(LOGMSG_WARN, "difficulty consuming key from queue '%s' consumer %d\n",
               iq->usedb->dbname, consumern);
        if (!consumer)
            sleep(sleeptime);
        else {
            struct dbenv *dbenv = consumer->db->dbenv;
            consumer_sleep(consumer, sleeptime, 0);
            if (dbenv->stopped || dbenv->exiting || dbenv->master != machine())
                return -1;
        }
    }
}

static int dispatch_item(struct ireq *iq, struct consumer *consumer,
                         const struct dbq_cursor *cursor, void *item)
{
    switch (consumer->type) {
    case CONSUMER_TYPE_FSTSND:
        return dispatch_fstsnd(iq, consumer, cursor, item);
    default:
        return -1;
    }
}

static int dispatch_flush(struct ireq *iq, struct consumer *consumer)
{
    switch (consumer->type) {
    case CONSUMER_TYPE_FSTSND:
        return flush_fstsnd(iq, consumer);

    case CONSUMER_TYPE_JAVASP:
        /* no-op for javasp; everything is done in dispatch */
        return 0;

    default:
        return 0;
    }
}

static int dispatch_fstsnd(struct ireq *iq, struct consumer *consumer,
                           const struct dbq_cursor *cursor, void *item)
{
    struct dbq_msgbuf *buf = NULL;
    struct dbq_msg *msghdr = NULL;

    size_t frag_len = 0, frag_off = 0;
    size_t msglen;
    size_t tmplen;
    int tmpmsg;

    const char *dta;
    size_t dtalen;
    size_t dtaoff;

    dbq_get_item_info(item, &dtaoff, &dtalen);
    dta = item;
    dta += dtaoff;

    /* If there isn't enough room for another message then flush our buffer. */
    if (consumer->numitems >= MAXSENDQUEUE) {
        if (consumer->debug)
            condbgf(consumer, "dispatch_fstsnd: "
                              "consumer->numitems=%d, flushing\n",
                    consumer->numitems);
        if (flush_fstsnd(iq, consumer) != 0) {
            free(item);
            return XMIT_FLUSH;
        }
    }

    buf = (struct dbq_msgbuf *)consumer->buf;

beginning:
    if (consumer->first) {
        consumer->first = 0;
        consumer->spaceleft = getlclbfpoolwidthbigsnd() & ~3;
        /* Populate header */
        bzero(buf, offsetof(struct dbq_msgbuf, msgs));
        strncpy(buf->comdb2, "CDB2_MSG", sizeof(buf->comdb2));
        strncpy(buf->queue_name, consumer->db->dbname, sizeof(buf->queue_name));
        strncpy(buf->dbname, consumer->db->dbenv->envname, sizeof(buf->dbname));
        /* XXX endian */
        buf->buflen = htonl(offsetof(struct dbq_msgbuf, msgs));
        /* XXX endian */
        buf->num_messages = 0;

        if (consumer->debug)
            condbgf(consumer, "dispatch_fstsnd: prepared new buffer\n");

        consumer->spaceleft -= offsetof(struct dbq_msgbuf, msgs);
    }

    /* If there is no room for another message fragment then flush and go
     * back to the beginning. */
    if (consumer->spaceleft < offsetof(struct dbq_msg, data) + 16) {
        if (consumer->debug)
            condbgf(consumer, "dispatch_fstsnd: "
                              "consumer->spaceleft=%u, flushing\n",
                    consumer->spaceleft);
        if (flush_fstsnd(iq, consumer) != 0) {
            free(item);
            return XMIT_FLUSH;
        }
        goto beginning;
    }

    frag_len = consumer->spaceleft - offsetof(struct dbq_msg, data);
    if (frag_len > dtalen - frag_off)
        frag_len = dtalen - frag_off;

    /* Put as much as we can in the buffer. */
    msghdr = (struct dbq_msg *)(((char *)buf) + ntohl(buf->buflen));
    /* XXX endian */
    bzero(msghdr, offsetof(struct dbq_msg, data));
    msghdr->length = htonl(dtalen);
    msghdr->frag_offset = htonl(frag_off);
    msghdr->frag_length = htonl(frag_len);
    /* XXX not sure */
    memcpy(&msghdr->key, cursor, sizeof(msghdr->key));
    memcpy(msghdr->data, dta + frag_off, frag_len);

    /* 32 bit align messages */
    msglen = offsetof(struct dbq_msg, data) + frag_len;
    if (msglen & 3)
        msglen += (4 - (msglen & 3));

    consumer->spaceleft -= msglen;

    /* XXX endian */

    tmplen = ntohl(buf->buflen);
    tmplen += msglen;
    buf->buflen = htonl(tmplen);
    tmpmsg = ntohl(buf->num_messages) + 1;
    buf->num_messages = ntohl(tmpmsg);

    frag_off += frag_len;

    if (consumer->debug)
        condbgf(consumer, "dispatch_fstsnd: packed fragment "
                          "length=%u frag_offset=%u frag_length=%u "
                          "num_messages=%u buflen=%u\n",
                ntohl(msghdr->length), ntohl(msghdr->frag_offset),
                ntohl(msghdr->frag_length), ntohl(buf->num_messages),
                ntohl(buf->buflen));

    /* If there is more to send then flush and continue. */
    if (frag_off < dtalen) {
        if (consumer->debug)
            condbgf(consumer, "dispatch_fstsnd: frag_off=%u dtalen=%u "
                              "flushing this fragment\n",
                    frag_off, dtalen);
        if (flush_fstsnd(iq, consumer) != 0) {
            free(item);
            return XMIT_FLUSH;
        }
        goto beginning;
    }

    /* All done, add this item to our list of memory pointers to free at the
     * next ack. */
    consumer->items[consumer->numitems++] = item;
    return 0;
}

enum { DB_NAME_LEN = 32 };

enum { REQ_HELLO = 1, REQ_TRIGGER = 2 };

struct req {
    int opcode;
    int parm;
    int followlen;
};

struct rsp {
    int rc;
    int followlen;
};

struct hello {
    char fromdb[DB_NAME_LEN];
    int todb;
};

int consumer_connect_host(struct consumer *consumer, char *host)
{
    int fd;
    int rc;
    struct hello hello = {0};
    struct req req = {0};
    struct iovec iov[2];

    fd = portmux_connect_to(host, "comdb2", "triggers", "0", 2000);
    if (fd < 0) {
        logmsg(LOGMSG_ERROR, "consumer_connect_host queue %s consumer %d host %s rc %d %d %s\n",
            consumer->db->dbname, consumer->consumern, host, fd, errno,
            strerror(errno));
        return -1;
    }

    /* Identify ourselves. */
    req.opcode = htonl(REQ_HELLO);
    req.parm = htonl(0);
    req.followlen = htonl(sizeof(struct hello));
    strcpy(hello.fromdb, consumer->db->dbname);
    hello.todb = htonl(consumer->dbnum);
    iov[0].iov_base = &req;
    iov[0].iov_len = sizeof(struct req);
    iov[1].iov_base = &hello;
    iov[1].iov_len = sizeof(struct hello);

    rc = tcpwritemsgv(fd, 2, iov, 2000);
    if (rc <= 0) {
        close(fd);
        return -1;
    }

    consumer->listener_fd = fd;
    return 0;
}

int consumer_connect(struct consumer *consumer)
{
    int rc;

    if (strcmp(consumer->rmtmach, "multi") == 0) {
        if (consumer->ndesthosts == 0) {
            return -1;
        } else {
            int current = consumer->current_dest;
            int start = current;
            /* Try all destinations until we get one that works */
            rc = consumer_connect_host(consumer, consumer->desthosts[current]);
            while (rc &&
                   (current = ((current + 1) % consumer->ndesthosts)) !=
                       start) {
                rc = consumer_connect_host(consumer,
                                           consumer->desthosts[current]);
                consumer->current_dest = current;
            }
        }
    } else
        rc = consumer_connect_host(consumer, consumer->rmtmach);
    return rc;
}

static char *consumer_current_dest(struct consumer *consumer)
{
    if (strcmp(consumer->rmtmach, "multi") == 0) {
        return consumer->desthosts[consumer->current_dest];
    } else
        return consumer->rmtmach;
}

static int consumer_send(struct consumer *consumer, int len)
{
    struct req req;
    struct rsp rsp;
    struct iovec iov[2];
    int rc;

    req.opcode = htonl(REQ_TRIGGER);
    req.parm = htonl(0);
    req.followlen = htonl(len);
    iov[0].iov_base = &req;
    iov[0].iov_len = sizeof(struct req);
    iov[1].iov_base = consumer->buf;
    iov[1].iov_len = len;

    /* TODO:NOENV: timeout? */
    rc = tcpwritemsgv(consumer->listener_fd, 2, iov, 0);
    /* 996 is bigsnd error for "couldn't send" - simulate that */
    if (rc <= 0)
        return 996;

    rc = tcpreadmsg(consumer->listener_fd, &rsp, sizeof(struct rsp), 0);
    if (rc <= 0)
        return 996;

    rsp.followlen = ntohl(rsp.followlen);
    rsp.rc = ntohl(rsp.rc);

    if (rsp.followlen) {
        if (rsp.followlen < 0 || rsp.followlen > getlclbfpoolwidthbigsnd()) {
            logmsg(LOGMSG_ERROR, "queue %s consumer %d (%s) suspicious response length %d\n",
                    consumer->db->dbname, consumer->consumern,
                    consumer_current_dest(consumer), rsp.followlen);
            return 996;
        }
        rc = tcpreadmsg(consumer->listener_fd, consumer->buf, rsp.followlen, 0);
        if (rc <= 0) {
            logmsg(LOGMSG_ERROR, "queue %s consumer %d (%s) read response rc %d\n",
                    consumer->db->dbname, consumer->consumern,
                    consumer_current_dest(consumer), rc);
            return 996;
        }
    }

    return rsp.rc;
}

static int flush_fstsnd(struct ireq *iq, struct consumer *consumer)
{
    int rc = 0;
    int ii;

    if (consumer->listener_fd == -1) {
        rc = consumer_connect(consumer);
        if (rc)
            return -1;
    }

    if (consumer->db->dbenv->stopped || consumer->db->dbenv->exiting) {
        if (consumer->debug)
            condbgf(consumer, "flush_fstsnd: XMIT_STOPPED\n");
        rc = XMIT_STOPPED;
    } else if (consumer->db->dbenv->master != machine()) {
        if (consumer->debug)
            condbgf(consumer, "flush_fstsnd: XMIT_NOTMASTER\n");
        rc = XMIT_NOTMASTER;
    }

    if (rc == 0 && !consumer->first) {
        struct dbq_msgbuf *buf = NULL;
        buf = (struct dbq_msgbuf *)consumer->buf;
        int ii;
        unsigned int before_ms, after_ms;

        before_ms = time_epochms();

        /* TODO:NOENV remote policy? */

        rc = consumer_send(consumer, ntohl(buf->buflen));
        consumer->first = 1;

        after_ms = time_epochms();

        if (consumer->debug)
            condbgf(consumer, "flush_fstsnd: fstsnd sndbak rc=%d\n", rc);

        /* We want a 0 to acknowledge receipt. */
        if (rc == 996) {
            consumer->n_bad_fstsnds++;
            rc = XMIT_NOACK;
            goto flushed_buffer;
        }

        /* Good fstsnd */
        consumer->n_good_fstsnds++;
        if (after_ms > before_ms)
            consumer->ms_good_fstsnds += (after_ms - before_ms);

        /* Receipt was acknowledged, so consume all these items. */
        if (consumer->debug)
            condbgf(consumer, "flush_fstsnd: %d items to consume\n",
                    consumer->numitems);
        for (ii = 0; ii < consumer->numitems; ii++) {
            rc =
                consume(iq, consumer->items[ii], consumer, consumer->consumern);
            if (rc != 0) {
                rc = XMIT_NOCONSUME;
                goto flushed_buffer;
            }
            consumer->n_consumed++;
        }
    }

flushed_buffer:
    for (ii = 0; ii < consumer->numitems; ii++)
        if (consumer->items[ii])
            free(consumer->items[ii]);
    consumer->numitems = 0;
    return rc;
}

enum consumer_t consumer_type(struct consumer *c) { return c->type; }
