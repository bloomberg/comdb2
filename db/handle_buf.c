/*
   Copyright 2015, 2017, Bloomberg Finance L.P.

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

/* comdb index front end */

#include "limit_fortify.h"
#include <stdio.h>
#include <pthread.h>
#include <stddef.h>
#include <errno.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <unistd.h>

#include <epochlib.h>
#include <list.h>
#include <lockmacro.h>
#include <machpthread.h>
#include <pool.h>
#include <time.h>

#include "debug_switches.h"

#include "comdb2.h"
#include "block_internal.h"
#include "util.h"
#include "translistener.h"
#include "socket_interfaces.h"
#include "osqlsession.h"
#include "sql.h"
#include "osqlblockproc.h"
#include "intern_strings.h"
#include "logmsg.h"

#ifdef MONITOR_STACK
#include "comdb2_pthread_create.h"
#endif

void (*comdb2_ipc_sndbak)(int *, int) = 0;

enum THD_EV { THD_EV_END = 0, THD_EV_START = 1 };

/* request pool & queue */

static pool_t *p_reqs; /* request pool */

struct dbq_entry_t {
    LINKC_T(struct dbq_entry_t) qlnk;
    LINKC_T(struct dbq_entry_t) rqlnk;
    void *obj;
};

static pool_t *pq_reqs;  /* queue entry pool */

pool_t *p_bufs;   /* buffer pool for socket requests */
pool_t *p_slocks; /* pool of socket locks*/

LISTC_T(struct dbq_entry_t) q_reqs;         /* all queued requests */
static LISTC_T(struct dbq_entry_t) rq_reqs; /* queue of read requests */

/* thread pool */

/* thread associated with this request */
struct thd {
    pthread_t tid;
    pthread_cond_t wakeup;
    struct ireq *iq;
    LINKC_T(struct thd) lnk;
};

static pool_t *p_thds;
static LISTC_T(struct thd) idle; /*idle thread.*/
static LISTC_T(struct thd) busy; /*busy thread.*/

static int write_thd_count = 0;

static int is_req_write(int opcode);

int handle_buf_main(
    struct dbenv *dbenv, struct ireq *iq, SBUF2 *sb, const uint8_t *p_buf,
    const uint8_t *p_buf_end, int debug, char *frommach, int frompid,
    char *fromtask, sorese_info_t *sorese, int qtype,
    void *data_hndl, // handle to data that can be used according to request
                     // type
    int luxref, unsigned long long rqid);

static pthread_mutex_t lock;
pthread_mutex_t buf_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_attr_t attr;

#ifdef MONITOR_STACK
static size_t stack_sz;
static comdb2ma stack_alloc;
#endif

static pthread_cond_t coalesce_wakeup;
static unsigned coalesce_waiters = 0;
static unsigned coalesce_reqthd_waiters = 0;

/* stats */

enum { MAXSTAT = 200 };

static int nreqs;
static int nthdcreates;
static int nwaits;
static int nqfulls;
static int nerrs;
static int nretire;
static int bkt_thd[MAXSTAT];
static int bkt_queue[MAXSTAT];

static int iothreads = 0, waitthreads = 0;

void test_the_lock(void)
{
    LOCK(&lock) {}
    UNLOCK(&lock)
}

static void thd_io_start(void)
{
    LOCK(&lock) { iothreads++; }
    UNLOCK(&lock);
}

static void thd_io_complete(void)
{
    LOCK(&lock) { iothreads--; }
    UNLOCK(&lock);
}

int thd_init(void)
{
    int rc;
    rc = pthread_mutex_init(&lock, 0);
    if (rc) {
        perror_errnum("thd_init:pthread_mutex_init", rc);
        return -1;
    }
    rc = pthread_attr_init(&attr);
    if (rc) {
        perror_errnum("thd_init:pthread_attr_init", rc);
        return -1;
    }
    PTHD_ATTR_SETDETACHED(attr, rc);
    if (rc) {
        perror_errnum("thd_init:pthread_attr_setdetached", rc);
        return -1;
    }
    rc = pthread_cond_init(&coalesce_wakeup, NULL);
    if (rc) {
        perror_errnum("thd_init:pthread_cond_init", rc);
        return -1;
    }
    p_thds = pool_setalloc_init(sizeof(struct thd), 0, malloc, free);
    if (p_thds == 0) {
        logmsg(LOGMSG_ERROR, "thd_init:failed thd pool init");
        return -1;
    }
    p_reqs = pool_setalloc_init(sizeof(struct ireq), 0, malloc, free);
    if (p_reqs == 0) {
        logmsg(LOGMSG_ERROR, "thd_init:failed req pool init");
        return -1;
    }
    p_bufs = pool_setalloc_init(MAX_BUFFER_SIZE, 64, malloc, free);
    if (p_bufs == 0) {
        logmsg(LOGMSG_ERROR, "thd_init:failed buf pool init");
        return -1;
    }
    p_slocks = pool_setalloc_init(sizeof(struct buf_lock_t), 64, malloc, free);
    if (p_slocks == 0) {
        logmsg(LOGMSG_ERROR, "thd_init:failed sock lock pool init");
        return -1;
    }
    pq_reqs = pool_setalloc_init(sizeof(struct dbq_entry_t), 64, malloc, free);
    if (pq_reqs == 0) {
        logmsg(LOGMSG_ERROR, "thd_init:failed queue req pool init");
        return -1;
    }

    listc_init(&q_reqs, offsetof(struct dbq_entry_t, qlnk));
    listc_init(&rq_reqs, offsetof(struct dbq_entry_t, rqlnk));

#ifdef MONITOR_STACK
    stack_sz = 4096 * 1024;
    stack_alloc = comdb2ma_create_with_scope(0, 0, "stack", "tag", 1);
    if (stack_alloc == NULL) {
        logmsg(LOGMSG_ERROR, "thd_init: failed to initialize thread subsystem\n");
        return 1;
    }
#endif

    listc_init(&idle, offsetof(struct thd, lnk));
    listc_init(&busy, offsetof(struct thd, lnk));
    bdb_set_io_control(thd_io_start, thd_io_complete);
    logmsg(LOGMSG_INFO, "thd_init: thread subsystem initialized\n");
    rc = pthread_attr_setstacksize(&attr, 4096 * 1024);
    return 0;
}

void thd_stats(void)
{
    int ii, jj;
    logmsg(LOGMSG_USER, "num reqs              %d\n", nreqs);
    logmsg(LOGMSG_USER, "num waits             %d\n", nwaits);
    logmsg(LOGMSG_USER, "num items on queue    %d\n", q_reqs.count);
    logmsg(LOGMSG_USER, "num reads on queue   %d\n", rq_reqs.count);
    logmsg(LOGMSG_USER, "num threads wrt busy/busy/idle %d/%d/%d\n", write_thd_count,
           busy.count, idle.count);
    logmsg(LOGMSG_USER, "num threads in i/o    %d\n", iothreads);
    logmsg(LOGMSG_USER, "num threads waiting   %d\n", waitthreads);
    logmsg(LOGMSG_USER, "---\n");
    logmsg(LOGMSG_USER, "config:MAXTHREADS     %d\n", gbl_maxthreads);
    logmsg(LOGMSG_USER, "config:MAXWRTTHREADS  %d\n", gbl_maxwthreads);
    logmsg(LOGMSG_USER, "gbl_maxwthreadpenalty %d\n", gbl_maxwthreadpenalty);
    logmsg(LOGMSG_USER, "penaltyincpercent     %d\n", gbl_penaltyincpercent);
    logmsg(LOGMSG_USER, "config:MAXQUEUE       %d\n", gbl_maxqueue);
    logmsg(LOGMSG_USER, "---\n");
    logmsg(LOGMSG_USER, "num queue fulls       %d\n", nqfulls);
    logmsg(LOGMSG_USER, "num errors            %d\n", nerrs);
    logmsg(LOGMSG_USER, "num thread creates    %d\n", nthdcreates);
    logmsg(LOGMSG_USER, "num retires           %d\n", nretire);
    logmsg(LOGMSG_USER, "---\n");
    logmsg(LOGMSG_USER, "#threads:count\n");
    for (jj = 0, ii = 0; ii < MAXSTAT; ii++) {
        if (ii < 4 || bkt_thd[ii] > 0) {
            logmsg(LOGMSG_USER, " %-3d:%-8d", ii + 1, bkt_thd[ii]);
            jj++;
            if ((jj % 4) == 0)
                logmsg(LOGMSG_USER, "\n");
        }
    }
    if ((jj % 4) != 0)
        logmsg(LOGMSG_USER, "\n");
    logmsg(LOGMSG_USER, "#queue:count\n");
    for (jj = 0, ii = 0; ii < MAXSTAT; ii++) {
        if (ii < 4 || bkt_queue[ii] > 0) {
            logmsg(LOGMSG_USER, " %-3d:%-8d", ii + 1, bkt_queue[ii]);
            jj++;
            if ((jj % 4) == 0)
                logmsg(LOGMSG_USER, "\n");
        }
    }
    if ((jj % 4) != 0)
        logmsg(LOGMSG_USER, "\n");
}

void thd_dbinfo2_stats(struct db_info2_stats *stats)
{
    int ii;
    uint32_t queue_sum = 0, queue_mean = 0;
    stats->thr_max = gbl_maxthreads;
    stats->thr_maxwr = gbl_maxwthreads;
    stats->thr_cur = nthdcreates - nretire;
    stats->q_max_conf = gbl_maxqueue;
    for (ii = 0; ii < MAXSTAT; ii++) {
        if (bkt_queue[ii] > 0) {
            stats->q_max_reached = ii + 1;
            queue_mean += bkt_queue[ii] * stats->q_max_reached;
            queue_sum += bkt_queue[ii];
        }
    }
    if (stats->q_max_reached > 0) {
        double f = (double)queue_mean / (double)queue_sum;
        f += 0.5;
        stats->q_mean_reached = (uint32_t)f;
    } else
        stats->q_mean_reached = 0;
}

static void thd_coalesce_check_ll(void)
{
    if (coalesce_waiters && busy.count <= coalesce_reqthd_waiters &&
        q_reqs.count == 0) {
        int rc;
        rc = pthread_cond_broadcast(&coalesce_wakeup);
        if (rc != 0)
            logmsg(LOGMSG_ERROR, "%s:pthread_cond_broadcast: %d %s\n", __func__, rc,
                    strerror(rc));
    }
}

static void thd_dump_nolock(void)
{
    struct thd *thd;
    uint64_t nowus;
    int opc, cnt = 0;
    nowus = comdb2_time_epochus();

    {
        for (thd = busy.top; thd; thd = thd->lnk.next) {
            cnt++;
            opc = thd->iq->opcode;
            logmsg(LOGMSG_USER, "busy  tid %lu  time %5d ms  %-6s (%-3d) "
                                "%-20s where %s %s\n",
                   thd->tid, U2M(nowus - thd->iq->nowus), req2a(opc), opc,
                   getorigin(thd->iq), thd->iq->where, thd->iq->gluewhere);
        }

        for (thd = idle.top; thd; thd = thd->lnk.next) {
            cnt++;
            logmsg(LOGMSG_USER, "idle  tid %lu \n", thd->tid);
        }
    }

    if (cnt == 0)
        logmsg(LOGMSG_USER, "no active threads\n");
}

void thd_coalesce(struct dbenv *dbenv)
{
    LOCK(&lock)
    {
        struct thd *thd;
        int am_req_thd = 0;
        int num_wait;

        /* fstsnd based fastinit can lead to us waiting for ourself.. check if
         * this is one of the request threads and if so that's one less
         * thread to wait for. */
        LISTC_FOR_EACH(&busy, thd, lnk)
        {
            if (thd->tid == pthread_self()) {
                am_req_thd = 1;
                break;
            }
        }
        coalesce_waiters++;
        coalesce_reqthd_waiters += am_req_thd;
        while (busy.count > coalesce_reqthd_waiters || q_reqs.count > 0) {
            int rc;
            struct timespec ts;

            ++num_wait;
            logmsg(LOGMSG_USER, "waiting for threads %d/%d/%d num queue %d\n",
                   write_thd_count, busy.count, idle.count, q_reqs.count);
            if (num_wait > 5)
                thd_dump_nolock();
            rc = clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;
            rc = pthread_cond_timedwait(&coalesce_wakeup, &lock, &ts);
            if (rc != 0 && rc != ETIMEDOUT)
                logmsg(LOGMSG_ERROR, "%s:pthread_cond_timedwait: %d %s\n", __func__,
                        rc, strerror(rc));
        }
        coalesce_waiters--;
        coalesce_reqthd_waiters -= am_req_thd;
    }
    UNLOCK(&lock);
}

void thd_dump(void)
{
    struct thd *thd;
    uint64_t nowus;
    int cnt = 0;
    nowus = comdb2_time_epochus();
    LOCK(&lock)
    {
        for (thd = busy.top; thd; thd = thd->lnk.next) {
            cnt++;
            logmsg(LOGMSG_USER,
                   "busy  tid %lu  time %5d ms  %-6s (%-3d) %-20s where %s "
                   "%s\n",
                   thd->tid, U2M(nowus - thd->iq->nowus),
                   req2a(thd->iq->opcode), thd->iq->opcode, getorigin(thd->iq),
                   thd->iq->where, thd->iq->gluewhere);
        }

        for (thd = idle.top; thd; thd = thd->lnk.next) {
            cnt++;
            logmsg(LOGMSG_USER, "idle  tid %lu \n", thd->tid);
        }
    }
    UNLOCK(&lock);
    if (cnt == 0)
        logmsg(LOGMSG_USER, "no active threads\n");
}

uint8_t *get_bigbuf()
{
    uint8_t *p_buf = NULL;
    LOCK(&buf_lock) { p_buf = pool_getablk(p_bufs); }
    UNLOCK(&buf_lock);
    return p_buf;
}

int free_bigbuf_nosignal(uint8_t *p_buf)
{
    LOCK(&buf_lock) { pool_relablk(p_bufs, p_buf); }
    UNLOCK(&buf_lock);
    return 0;
}

int free_bigbuf(uint8_t *p_buf, struct buf_lock_t *p_slock)
{
    if (p_slock == NULL)
        return 0;
    p_slock->reply_state = REPLY_STATE_DONE;
    LOCK(&buf_lock) { pool_relablk(p_bufs, p_buf); }
    UNLOCK(&buf_lock);
    pthread_cond_signal(&(p_slock->wait_cond));
    return 0;
}

int signal_buflock(struct buf_lock_t *p_slock)
{
    p_slock->reply_state = REPLY_STATE_DONE;
    pthread_cond_signal(&(p_slock->wait_cond));
    return 0;
}

/* request handler */
static void *thd_req(void *vthd)
{
    struct thd *thd = (struct thd *)vthd;
    struct dbenv *dbenv;
    struct timespec ts;
    pthread_cond_t *hldcnd;
    int rc;
    int iamwriter = 0;
    struct thread_info *thdinfo;
    struct thr_handle *thr_self;
    struct reqlogger *logger;
    int numwriterthreads;

    thread_started("request");

#ifdef PER_THREAD_MALLOC
    thread_type_key = "tag";
#endif
    thr_self = thrman_register(THRTYPE_REQ);
    logger = thrman_get_reqlogger(thr_self);

    dbenv = thd->iq->dbenv;
    backend_thread_event(dbenv, COMDB2_THR_EVENT_START_RDWR);

    /* thdinfo is assigned to thread specific variable unique_tag_key which
     * will automatically free it when the thread exits. */
    thdinfo = malloc(sizeof(struct thread_info));
    if (thdinfo == NULL) {
        logmsg(LOGMSG_FATAL, "**aborting due malloc failure thd %lu\n",
               pthread_self());
        abort();
    }
    thdinfo->uniquetag = 0;
    thdinfo->ct_id_key = 0LL;
    thdinfo->ct_add_table = NULL;
    thdinfo->ct_del_table = NULL;
    thdinfo->ct_add_index = NULL;

    thdinfo->ct_add_table =
        (void *)create_constraint_table(&thdinfo->ct_id_key);
    if (thdinfo->ct_add_table == NULL) {
        logmsg(LOGMSG_FATAL,
               "**aborting: cannot allocate constraint add table thd "
               "%lu\n",
               pthread_self());
        abort();
    }
    thdinfo->ct_del_table =
        (void *)create_constraint_table(&thdinfo->ct_id_key);
    if (thdinfo->ct_del_table == NULL) {
        logmsg(LOGMSG_FATAL,
               "**aborting: cannot allocate constraint delete table "
               "thd %lu\n",
               pthread_self());
        abort();
    }
    thdinfo->ct_add_index =
        (void *)create_constraint_index_table(&thdinfo->ct_id_key);
    if (thdinfo->ct_add_index == NULL) {
        logmsg(LOGMSG_FATAL,
               "**aborting: cannot allocate constraint add index table "
               "thd %lu\n",
               pthread_self());
        abort();
    }
    pthread_setspecific(unique_tag_key, thdinfo);

    /*printf("started handler %ld thd %p thd->id %ld\n", pthread_self(), thd,
     * thd->tid);*/
    do {
        if (thd->tid != pthread_self()) /*sanity check*/
        {
            logmsg(LOGMSG_FATAL, "**aborting due thd_req mismatch thd id %ld (my "
                            "thd %ld)\n",
                    thd->tid, pthread_self());
            abort();
        }
        thd->iq->startus = comdb2_time_epochus();
        thd->iq->where = "executing";
        /*PROCESS REQUEST*/
        thd->iq->reqlogger = logger;
        iamwriter = is_req_write(thd->iq->opcode) ? 1 : 0;
        dbenv = thd->iq->dbenv;
        thrman_where(thr_self, req2a(thd->iq->opcode));
        thrman_origin(thr_self, getorigin(thd->iq));
        user_request_begin(REQUEST_TYPE_QTRAP, FLAG_REQUEST_TRACK_EVERYTHING);
        handle_ireq(thd->iq);
        if (debug_this_request(gbl_debug_until) ||
            (gbl_who > 0 && !gbl_sdebug)) {
            struct per_request_stats *st;
            st = user_request_get_stats();
            if (st)
                logmsg(LOGMSG_USER, "nreads %d (%lld bytes) nwrites %d (%lld bytes) nfsyncs "
                       "%d nmempgets %d\n",
                       st->nreads, st->readbytes, st->nwrites, st->writebytes,
                       st->nfsyncs, st->mempgets);
        }
        thread_util_donework();
        thrman_origin(thr_self, NULL);
        thrman_where(thr_self, "idle");
        thd->iq->where = "done executing";

        // before acquiring next request, yield
        comdb2bma_yield_all();

        /*NEXT REQUEST*/
        LOCK(&lock)
        {
            struct dbq_entry_t *nxtrq = NULL;
            int newrqwriter = 0;

            if (iamwriter) {
                write_thd_count--;
            }

            if (thd->iq->usedb && thd->iq->ixused >= 0 &&
                thd->iq->ixused < thd->iq->usedb->nix &&
                thd->iq->usedb->ixuse) {
                thd->iq->usedb->ixuse[thd->iq->ixused] += thd->iq->ixstepcnt;
            }
            thd->iq->ixused = -1;
            thd->iq->ixstepcnt = 0;

            if (thd->iq->dbglog_file) {
                sbuf2close(thd->iq->dbglog_file);
                thd->iq->dbglog_file = NULL;
            }
            if (thd->iq->nwrites) {
                free(thd->iq->nwrites);
                thd->iq->nwrites = NULL;
            }
            if (thd->iq->vfy_genid_hash) {
                hash_free(thd->iq->vfy_genid_hash);
                thd->iq->vfy_genid_hash = NULL;
            }
            if (thd->iq->vfy_genid_pool) {
                pool_free(thd->iq->vfy_genid_pool);
                thd->iq->vfy_genid_pool = NULL;
            }
            if (thd->iq->sorese.osqllog) {
                sbuf2close(thd->iq->sorese.osqllog);
                thd->iq->sorese.osqllog = NULL;
            }
            thd->iq->vfy_genid_track = 0;
#if 0
            fprintf(stderr, "%s:%d: THD=%d relablk iq=%p\n", __func__, __LINE__, pthread_self(), thd->iq);
#endif
            pool_relablk(p_reqs, thd->iq); /* this request is done, so release
                                            * resource. */
            /* get next item off hqueue */
            nxtrq = (struct dbq_entry_t *)listc_rtl(&q_reqs);
            thd->iq = 0;
            if (nxtrq != 0) {
                thd->iq = nxtrq->obj;
                newrqwriter = is_req_write(thd->iq->opcode) ? 1 : 0;

                numwriterthreads = gbl_maxwthreads - gbl_maxwthreadpenalty;
                if (numwriterthreads < 1)
                    numwriterthreads = 1;

                if (newrqwriter &&
                    (write_thd_count - iothreads) >= numwriterthreads) {
                    /* dont process next request as it goes over
                       the write limit..put it back on queue and grab
                       next read */
                    listc_atl(&q_reqs, nxtrq);
                    nxtrq = (struct dbq_entry_t *)listc_rtl(&rq_reqs);
                    if (nxtrq != NULL) {
                        listc_rfl(&q_reqs, nxtrq);
                        /* release the memory block of the link */
                        thd->iq = nxtrq->obj;
                        pool_relablk(pq_reqs, nxtrq);
                        newrqwriter = 0;
                    } else {
                        thd->iq = 0;
                    }
                } else {
                    if (!newrqwriter) {
                        /*get rid of new request from read queue */
                        listc_rfl(&rq_reqs, nxtrq);
                    }
                    /* release the memory block of the link */
                    pool_relablk(pq_reqs, nxtrq);
                }
                if (newrqwriter && thd->iq != 0) {
                    write_thd_count++;
                }
            }
            if (thd->iq == 0) {
                /*wait for something to do, or go away after a while */
                listc_rfl(&busy, thd);
                thd_coalesce_check_ll();

                listc_atl(&idle, thd);

                rc = clock_gettime(CLOCK_REALTIME, &ts);
                if (rc != 0) {
                    logmsg(LOGMSG_ERROR, "thd_req:clock_gettime bad rc %d:%s\n", rc,
                            strerror(errno));
                    memset(&ts, 0, sizeof(ts)); /*force failure later*/
                }

                ts.tv_sec += gbl_thd_linger;
                rc = 0;
                do {
                    /*waitft thread will deposit a request in thd->iq*/
                    rc = pthread_cond_timedwait(&thd->wakeup, &lock, &ts);
                } while (thd->iq == 0 && rc == 0);
                if (rc != 0 && rc != ETIMEDOUT) {
                    logmsg(LOGMSG_ERROR, "thd_req:pthread_cond_timedwait "
                                    "failed:%s\n",
                            strerror(rc));
                    /* error'd out, so i still have lock: errLOCK(&lock);*/
                }
                if (thd->iq == 0) /*nothing to do. this thread retires.*/
                {
                    nretire++;
                    listc_rfl(&idle, thd);
                    pthread_cond_destroy(&thd->wakeup);
                    thd->tid =
                        -2; /*returned. this is just for info & debugging*/
                    pool_relablk(p_thds, thd); /*release this struct*/
                    /**/
                    retUNLOCK(&lock);
                    /**/
                    /*printf("ending handler %ld\n", pthread_self());*/
                    delete_constraint_table(thdinfo->ct_add_table);
                    delete_constraint_table(thdinfo->ct_del_table);
                    delete_constraint_table(thdinfo->ct_add_index);
                    backend_thread_event(dbenv, COMDB2_THR_EVENT_DONE_RDWR);
                    return 0;
                }
            }
            thd_coalesce_check_ll();
        }
        UNLOCK(&lock);

        /* Should not be done under lock - might be expensive */
        truncate_constraint_table(thdinfo->ct_add_table);
        truncate_constraint_table(thdinfo->ct_del_table);
        truncate_constraint_table(thdinfo->ct_add_index);
    } while (1);
}

/* sndbak error code &  return resources.*/
static int reterr(intptr_t curswap, struct thd *thd, struct ireq *iq, int rc)
/* 040307dh: 64bits */
{
    int is_legacy_fstsnd = 1;
    if (thd || iq) {
        LOCK(&lock)
        {
            if (thd) {
                if (thd->iq) {
                    int iamwriter = 0;
                    iamwriter = is_req_write(thd->iq->opcode) ? 1 : 0;
                    listc_rfl(&busy, thd); /*this means busy*/
                    thd_coalesce_check_ll();
                    if (iamwriter) {
                        write_thd_count--;
                    }
                }
                thd->iq = 0;
                thd->tid = -1;
                pool_relablk(p_thds, thd);
            }
            if (iq) {
                if (iq->is_fromsocket) {
                    if (iq->is_socketrequest) {
                        sndbak_open_socket(iq->sb, NULL, 0, ERR_INTERNAL);
                    } else {
                        sndbak_socket(iq->sb, NULL, 0, ERR_INTERNAL);
                    }
                    is_legacy_fstsnd = 0;
                } else if (iq->is_sorese) {
                    if (iq->sorese.osqllog) {
                        sbuf2close(iq->sorese.osqllog);
                        iq->sorese.osqllog = NULL;
                    }
                    is_legacy_fstsnd = 0;
                }
                pool_relablk(p_reqs, iq);
            }
        }
        UNLOCK(&lock);
    }
    if (comdb2_ipc_sndbak) {
        /* curswap is just a pointer to the buffer */
        int *ibuf = (int *)curswap;
        ibuf += 2;
        comdb2_ipc_sndbak(ibuf, ERR_INTERNAL);
    }
    if (rc == ERR_INTERNAL) /*qfull hits this code too, so differentiate*/
        nerrs++;
    return rc;
}

static int reterr_withfree(struct ireq *iq, int rc)
{
    if (iq->is_fromsocket || iq->sorese.type) {
        if (iq->is_fromsocket) {
            /* process socket end request */
            if (iq->is_socketrequest) {
                sndbak_open_socket(iq->sb, NULL, 0, rc);
                free_bigbuf(iq->p_buf_out_start, iq->request_data);
                iq->request_data = iq->p_buf_out_start = NULL;
            } else {
                sndbak_socket(iq->sb, NULL, 0, rc);
            }
        } else {
            /* we don't do this anymore for sorese requests */
            abort();
        }
        if (iq->p_buf_out_start) {
            free(iq->p_buf_out_start);
        }
        iq->p_buf_out_end = iq->p_buf_out_start = iq->p_buf_out = NULL;
        iq->p_buf_in_end = iq->p_buf_in = NULL;

        LOCK(&lock)
        {
#if 0
           fprintf(stderr, "%s:%d: THD=%d relablk iq=%p\n", __func__, __LINE__, pthread_self(), iq);
#endif
            pool_relablk(p_reqs, iq);
        }
        UNLOCK(&lock);

        return 0;
    } else {
        return reterr(iq->curswap, NULL, iq, rc);
    }
}

int handle_buf_block_offload(struct dbenv *dbenv, uint8_t *p_buf,
                             const uint8_t *p_buf_end, int debug,
                             char *frommach, unsigned long long rqid)
{
    int length = p_buf_end - p_buf;
    uint8_t *p_bigbuf = get_bigbuf();
    memcpy(p_bigbuf, p_buf, length);
    int rc = handle_buf_main(dbenv, NULL, NULL, p_bigbuf, p_bigbuf + length,
                             debug, frommach, 0, NULL, NULL, REQ_SOCKREQUEST,
                             NULL, 0, rqid);

    return rc;
}

int handle_buf_sorese(struct dbenv *dbenv, struct ireq *iq, int debug)
{
    int rc = 0;

    rc = handle_buf_main(dbenv, iq, NULL, NULL, NULL, debug, 0, 0, NULL, NULL,
                         REQ_OFFLOAD, NULL, 0, 0);

    return rc;
}

int handle_socket_long_transaction(struct dbenv *dbenv, SBUF2 *sb,
                                   uint8_t *p_buf, const uint8_t *p_buf_end,
                                   int debug, char *frommach, int frompid,
                                   char *fromtask)
{
    return handle_buf_main(dbenv, NULL, sb, p_buf, p_buf_end, debug, frommach,
                           frompid, fromtask, NULL, REQ_SOCKET, NULL, 0, 0);
}

void cleanup_lock_buffer(struct buf_lock_t *lock_buffer)
{
    if (lock_buffer == NULL)
        return;

    /* sbuf2 is owned by the appsock. Don't close it here. */

    pthread_cond_destroy(&lock_buffer->wait_cond);
    pthread_mutex_destroy(&lock_buffer->req_lock);

    LOCK(&buf_lock)
    {
        if (lock_buffer->bigbuf != NULL)
            pool_relablk(p_bufs, lock_buffer->bigbuf);
        pool_relablk(p_slocks, lock_buffer);
    }
    UNLOCK(&buf_lock);
}

/* handle a buffer from waitft */
int handle_buf(struct dbenv *dbenv, uint8_t *p_buf, const uint8_t *p_buf_end,
               int debug, char *frommach) /* 040307dh: 64bits */
{
    return handle_buf_main(dbenv, NULL, NULL, p_buf, p_buf_end, debug, frommach,
                           0, NULL, NULL, REQ_WAITFT, NULL, 0, 0);
}

int handled_queue;

int q_reqs_len(void) { return q_reqs.count; }

static int init_ireq(struct dbenv *dbenv, struct ireq *iq, SBUF2 *sb,
                     uint8_t *p_buf, const uint8_t *p_buf_end, int debug,
                     char *frommach, int frompid, char *fromtask, int qtype,
                     void *data_hndl, int luxref, unsigned long long rqid,
                     void *p_sinfo, intptr_t curswap)
{
    struct req_hdr hdr;
    uint64_t nowus;
    int rc, num, ndispatch, iamwriter = 0;
    struct thd *thd;
    int numwriterthreads;

    nowus = comdb2_time_epochus();

    if (iq == 0) {
        errUNLOCK(&lock);
        logmsg(LOGMSG_ERROR, "handle_buf:failed allocate req\n");
        return reterr(curswap, /*thd*/ 0, /*iq*/ 0, ERR_INTERNAL);
    }

    /* set up request */
    const size_t len = sizeof(*iq) - offsetof(struct ireq, region3);
    bzero(&iq->region3, len);

    iq->corigin[0] = '\0';
    iq->debug_buf[0] = '\0';
    iq->tzname[0] = '\0';
    iq->sqlhistory[0] = '\0';

    iq->where = "setup";
    iq->frommach = frommach ? intern(frommach) : NULL;
    iq->frompid = frompid;
    iq->gluewhere = "-";
    iq->debug = debug_this_request(gbl_debug_until) || (debug && gbl_debug);
    iq->debug_now = iq->nowus = nowus;
    iq->dbenv = dbenv;
    iq->rqid = rqid;

    iq->p_buf_orig =
        p_buf; /* need this for optimized fast fail (skip blkstate) */
    iq->p_buf_in = p_buf;
    iq->p_buf_in_end = p_buf_end;
    iq->p_buf_out = p_buf + REQ_HDR_LEN;
    iq->p_buf_out_start = p_buf;
    iq->p_buf_out_end = p_buf_end - RESERVED_SZ;

    /* IPC stuff */
    iq->p_sinfo = p_sinfo;
    iq->curswap = curswap;

    if (!(iq->p_buf_in = req_hdr_get(&hdr, iq->p_buf_in, iq->p_buf_in_end))) {
        errUNLOCK(&lock);
        logmsg(LOGMSG_ERROR, "handle_buf:failed to unpack req header\n");
        return reterr(curswap, /*thd*/ 0, iq, ERR_BADREQ);
    }

    iq->opcode = hdr.opcode;

    if (qtype == REQ_PQREQUEST) {
        iq->is_fake = 1;
        iq->is_dumpresponse = 1;
        iq->request_data = data_hndl;
    }

    iq->sb = NULL;

    if (qtype == REQ_SOCKET || qtype == REQ_SOCKREQUEST) {
        iq->sb = sb;
        iq->is_fromsocket = 1;
        iq->is_socketrequest = (qtype == REQ_SOCKREQUEST) ? 1 : 0;
    }

    if (iq->is_socketrequest) {
        iq->request_data = data_hndl;
    }

    iq->__limits.maxcost = gbl_querylimits_maxcost;
    iq->__limits.tablescans_ok = gbl_querylimits_tablescans_ok;
    iq->__limits.temptables_ok = gbl_querylimits_temptables_ok;

    iq->__limits.maxcost_warn = gbl_querylimits_maxcost_warn;
    iq->__limits.tablescans_warn = gbl_querylimits_tablescans_warn;
    iq->__limits.temptables_warn = gbl_querylimits_temptables_warn;

    iq->cost = 0;
    iq->sorese.osqllog = NULL;
    iq->luxref = luxref;

#if 0
    Pulled this one out of init_req, it only adds to the confusion
    This code was only triggered by create_ireq,i.e. create_sorese_ireq, called by sorese_rcvreq
    when a new sorese request is received. 

    if(qtype==REQ_OFFLOAD) {
    }
#endif

    if (iq->is_fromsocket) {
        if (iq->frommach == gbl_mynode)
            snprintf(iq->corigin, sizeof(iq->corigin), "SLCL  %.8s PID %6.6d",
                     fromtask, frompid);
        else
            snprintf(iq->corigin, sizeof(iq->corigin), "SRMT# %s PID %6.6d",
                     iq->frommach, frompid);
    }

    if (luxref < 0 || luxref >= dbenv->num_dbs) {
        errUNLOCK(&lock);
        logmsg(LOGMSG_ERROR, "handle_buf:luxref out of range %d max %d\n",
               luxref, dbenv->num_dbs);
        return reterr(curswap, /*thd*/ 0, iq, ERR_REJECTED);
    }

    iq->origdb = dbenv->dbs[luxref]; /*lux is one based*/
    iq->usedb = iq->origdb;
    if (thedb->stopped) {
        errUNLOCK(&lock);
        return reterr(curswap, NULL, iq, ERR_REJECTED);
    }

    if (gbl_debug_verify_tran)
        iq->transflags |= TRAN_VERIFY;
    if (iq->frommach == NULL)
        iq->frommach = intern(gbl_mynode);

    return 0;
}

int handle_buf_main2(struct dbenv *dbenv, struct ireq *iq, SBUF2 *sb,
                     const uint8_t *p_buf, const uint8_t *p_buf_end, int debug,
                     char *frommach, int frompid, char *fromtask,
                     sorese_info_t *sorese, int qtype, void *data_hndl,
                     int luxref, unsigned long long rqid, void *p_sinfo,
                     intptr_t curswap)
{
    int rc, nowms, num, ndispatch, iamwriter = 0;
    struct thd *thd;
    int numwriterthreads;
    struct dbq_entry_t *newent = NULL;

    net_delay(frommach);

    ndispatch = 0;
    nreqs++;

    if (gbl_who > 0) {
        --gbl_who;
        debug = 1;
    }

    /* allocate a request for later dispatch to available thread */

    pthread_mutex_lock(&lock);
    if (iq == NULL) {
        iq = (struct ireq *)pool_getablk(p_reqs);
#if 0
        fprintf(stderr, "%s:%d: THD=%d getablk iq=%p\n", __func__, __LINE__, pthread_self(), iq);
#endif

        rc = init_ireq(dbenv, iq, sb, (uint8_t *)p_buf, p_buf_end, debug,
                       frommach, frompid, fromtask, qtype, data_hndl, luxref,
                       rqid, p_sinfo, curswap);
        if (rc) {
            logmsg(LOGMSG_ERROR, "handle_buf:failed to unpack req header\n");
            return rc;
        }
    } else {
#if 0
       fprintf(stderr, "%s:%d: THD=%d delivered iq=%p\n", __func__, __LINE__, pthread_self(), iq);
#endif
    }

    {
        ++handled_queue;

        /*count queue*/
        num = q_reqs.count;
        if (num >= MAXSTAT)
            num = MAXSTAT - 1;
        bkt_queue[num]++;

        /*while ((idle.top || busy.count < gbl_maxthreads)
         * && (iq = queue_next(q_reqs)))*/
        newent = (struct dbq_entry_t *)pool_getablk(pq_reqs);
        if (newent == NULL) {
            errUNLOCK(&lock);
            logmsg(LOGMSG_ERROR, "handle_buf:failed to alloc new queue entry, rc %d\n", rc);
            return reterr(curswap, /*thd*/ 0, iq, ERR_REJECTED);
        }
        newent->obj = (void *)iq;
        iamwriter = is_req_write(iq->opcode) ? 1 : 0;
        if (!iamwriter) {
            (void)listc_abl(&rq_reqs, newent);
        }

        /*add to global queue*/
        (void)listc_abl(&q_reqs, newent);
        /* dispatch work ...*/
        iq->where = "enqueued";

        while (busy.count - iothreads < gbl_maxthreads) {
            struct dbq_entry_t *nextrq = NULL;
            nextrq = (struct dbq_entry_t *)listc_rtl(&q_reqs);
            if (nextrq == NULL)
                break;
            iq = nextrq->obj;
            iamwriter = is_req_write(iq->opcode) ? 1 : 0;

            numwriterthreads = gbl_maxwthreads - gbl_maxwthreadpenalty;
            if (numwriterthreads < 1)
                numwriterthreads = 1;

            if (iamwriter &&
                (write_thd_count - iothreads) >= numwriterthreads) {
                /* i am invalid writer, check the read queue instead */
                listc_atl(&q_reqs, nextrq);

                nextrq = (struct dbq_entry_t *)listc_rtl(&rq_reqs);
                if (nextrq == NULL)
                    break;
                iq = nextrq->obj;
                /* remove from global list, and release link block of reader*/
                listc_rfl(&q_reqs, nextrq);
                pool_relablk(pq_reqs, nextrq);
                if (!iq)
                    /* this should never be hit */
                    break;
                /* make sure to mark the reader request accordingly */
                iamwriter = 0;
            } else {
                /* i am reader or valid writer */
                if (!iamwriter) {
                    /* remove reader from read queue */
                    listc_rfl(&rq_reqs, nextrq);
                }
                /* release link block */
                pool_relablk(pq_reqs, nextrq);
                if (!iq) {
                    /* this should never be hit */
                    abort();
                    break;
                }
            }
            if ((thd = listc_rtl(&idle)) != NULL) /*try to find an idle thread*/
            {
#if 0
                printf("%s:%d: thdpool FOUND THD=%d -> newTHD=%d iq=%p\n", __func__, __LINE__, pthread_self(), thd->tid, iq);
#endif
                thd->iq = iq;
                iq->where = "dispatched";
                num = busy.count;
                listc_abl(&busy, thd);
                if (iamwriter) {
                    write_thd_count++;
                }
                if (num >= MAXSTAT)
                    num = MAXSTAT - 1;
                bkt_thd[num]++; /*count threads*/
                pthread_cond_signal(&thd->wakeup);
                ndispatch++;
            } else /*i can create one..*/
            {
                thd = (struct thd *)pool_getzblk(p_thds);
                if (thd == 0) {
                    rc = errno;
                    errUNLOCK(&lock);
                    logmsg(LOGMSG_ERROR, "handle_buf:failed calloc thread:%s\n",
                            strerror(errno));
                    return reterr(curswap, /*thd*/ 0, iq, ERR_INTERNAL);
                }
                /*add holder for this one being born...*/
                num = busy.count;
                listc_abl(&busy, thd);
                if (iamwriter) {
                    write_thd_count++;
                }
                thd->iq = iq;
                /*                fprintf(stderr, "added3 %8.8x\n",thd);*/
                iq->where = "dispatched new";
                rc = pthread_cond_init(&thd->wakeup, 0);
                if (rc != 0) {
                    errUNLOCK(&lock);
                    perror_errnum("handle_buf:failed pthread_cond_init", rc);
                    return reterr(curswap, thd, iq, ERR_INTERNAL);
                }
                nthdcreates++;
#ifdef MONITOR_STACK
                rc = comdb2_pthread_create(&thd->tid, &attr, thd_req,
                                           (void *)thd, stack_alloc, stack_sz);
#else
                rc = pthread_create(&thd->tid, &attr, thd_req, (void *)thd);
#endif

#if 0
                printf("%s:%d: thdpool CREATE THD=%d -> newTHD=%d iq=%p\n", __func__, __LINE__, pthread_self(), thd->tid, iq);
#endif
                if (rc != 0) {
                    errUNLOCK(&lock);
                    perror_errnum("handle_buf:failed pthread_thread_start", rc);
                    /* This tends to happen when we're out of memory.  Rather
                     * than limp onwards, we should just exit here.  Hand off
                     * masterness if possible. */
                    if (debug_exit_on_pthread_create_error()) {
                        bdb_transfermaster(thedb->dbs[0]->handle);
                        logmsg(LOGMSG_FATAL, 
                                "%s:Exiting due to thread create errors\n",
                                __func__);
                        exit(1);
                    }
                    return reterr(curswap, thd, iq, ERR_INTERNAL);
                }
                /* added thread to thread pool.*/
                if (num >= MAXSTAT)
                    num = MAXSTAT - 1;
                bkt_thd[num]++; /*count threads*/
                ndispatch++;
            }
            comdb2bma_transfer_priority(blobmem, thd->tid);
        }

        /* drain queue if too full */
        rc = q_reqs.count;
        if (qtype != REQ_OFFLOAD && rc > gbl_maxqueue) {
            struct dbq_entry_t *nextrq = NULL;
            logmsg(LOGMSG_ERROR,
                   "THD=%lu handle_buf:rejecting requests queue too full %d "
                   "(max %d)\n",
                   pthread_self(), rc, gbl_maxqueue);

            comdb2bma_yield_all();
            /* Dequeue the request I just queued. */
            nextrq = (struct dbq_entry_t *)listc_rbl(&q_reqs);
            if (nextrq && nextrq == newent) {
                iq = nextrq->obj;
                iamwriter = is_req_write(iq->opcode) ? 1 : 0;
                if (!iamwriter) {
                    listc_rfl(&rq_reqs, nextrq);
                }
                pool_relablk(pq_reqs, nextrq);
                pthread_mutex_unlock(&lock);
                nqfulls++;
                reterr_withfree(iq, ERR_REJECTED);
            } else {
                /* THIS can happen since the queue might be already full,
                   with requests we keep, and this could be a successfully
                   dispatched request (which is not at the head of the list
                   anymore).
                   If it is not me, stay in queue */
                listc_abl(&q_reqs, nextrq);

                iq = nextrq->obj;
#if 0
               fprintf(stderr, "SKIP DISCARDING iq=%p\n", iq);
#endif

                /* paranoia; this cannot be read */
                iamwriter = is_req_write(iq->opcode) ? 1 : 0;
                if (!iamwriter) {
                    /* this should not be a read, unless code changed; reads are
                    not kept in the queue above the limit */
                    abort();
                }

                pthread_mutex_unlock(&lock);
            }
        } else {
            pthread_mutex_unlock(&lock);
        }
    }

    if (ndispatch == 0)
        nwaits++;
    return 0;
}

int handle_buf_main(struct dbenv *dbenv, struct ireq *iq, SBUF2 *sb,
                    const uint8_t *p_buf, const uint8_t *p_buf_end, int debug,
                    char *frommach, int frompid, char *fromtask,
                    sorese_info_t *sorese, int qtype, void *data_hndl,
                    int luxref, unsigned long long rqid)
{
    return handle_buf_main2(dbenv, iq, sb, p_buf, p_buf_end, debug, frommach,
                            frompid, fromtask, sorese, qtype, data_hndl, luxref,
                            rqid, 0, 0);
}
struct ireq *create_sorese_ireq(struct dbenv *dbenv, SBUF2 *sb, uint8_t *p_buf,
                                const uint8_t *p_buf_end, int debug,
                                char *frommach, sorese_info_t *sorese)
{
    int rc;
    struct ireq *iq;

    LOCK(&lock)
    {
        iq = (struct ireq *)pool_getablk(p_reqs);
#if 0
        fprintf(stderr, "%s:%d: THD=%d getablk iq=%p\n", __func__, __LINE__, pthread_self(), iq);
#endif
        if (iq == NULL) {
            logmsg(LOGMSG_ERROR, "can't allocate ireq\n");
            errUNLOCK(&lock);
        }
        rc = init_ireq(dbenv, iq, sb, p_buf, p_buf_end, debug, frommach, 0,
                       NULL, REQ_OFFLOAD, NULL, 0, 0, 0, 0);
        if (rc)
            /* init_ireq unlocks on error */
            return NULL;

        iq->sorese = *sorese;
        iq->is_sorese = 1;

#if 0
        printf("Mapping sorese %llu\n", osql_log_time());
#endif
        /* this creates the socksql/recom/serial local log (temp table) */
        snprintf(iq->corigin, sizeof(iq->corigin), "SORESE# %15s %s RQST %llx",
                 iq->sorese.host, osql_sorese_type_to_str(iq->sorese.type),
                 iq->sorese.rqid);

        /* enable logging, if any */
        if (gbl_enable_osql_logging) {
            int ffile = 0;
            char filename[128];
            static unsigned long long fcounter = 0;

            snprintf(filename, sizeof(filename), "osql_%llu.log", fcounter++);

            ffile = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0666);
            if (ffile == -1) {
                logmsg(LOGMSG_ERROR, "Failed to open osql log file %s\n", filename);
            } else {
                iq->sorese.osqllog = sbuf2open(ffile, 0);
                if (!iq->sorese.osqllog) {
                    close(ffile);
                }
            }
        }
    }
    UNLOCK(&lock);

    return iq;
}

void destroy_ireq(struct dbenv *dbenv, struct ireq *iq)
{
    LOCK(&lock) { pool_relablk(p_reqs, iq); }
    UNLOCK(&lock);
}

static int is_req_write(int opcode)
{
    if (opcode == OP_FWD_LBLOCK || opcode == OP_BLOCK ||
        opcode == OP_LONGBLOCK || opcode == OP_FWD_BLOCK ||
        opcode == OP_CLEARTABLE || opcode == OP_TRAN_FINALIZE ||
        opcode == OP_TRAN_COMMIT || opcode == OP_TRAN_ABORT)
        return 1;
    return 0;
}
