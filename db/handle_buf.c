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
#include <pool.h>
#include <time.h>

#include "debug_switches.h"
#include "lockmacros.h"
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
#include "transactionstate_systable.h"
#include <poll.h>

#ifdef MONITOR_STACK
#include "comdb2_pthread_create.h"
#endif

extern int free_it(void *obj, void *arg);
extern void destroy_hash(hash_t *h, int (*free_func)(void *, void *));

pthread_key_t thd_info_key;

void (*comdb2_ipc_sndbak)(int *, int) = 0;

enum THD_EV { THD_EV_END = 0, THD_EV_START = 1 };

/* request pool & queue */

pool_t *p_bufs;   /* buffer pool for socket requests */
pool_t *p_slocks; /* pool of socket locks*/

/* thread pool */

/* thread associated with this request */
struct thd {
    struct ireq *iq;
    LINKC_T(struct thd) lnk;

    // extensions to allow calling thd_req inline
    int do_inline;
    struct thr_handle *thr_self;
};

static int is_req_write(int opcode);

pthread_mutex_t buf_lock = PTHREAD_MUTEX_INITIALIZER;

#ifdef MONITOR_STACK
static size_t stack_sz;
static comdb2ma stack_alloc;
#endif

/* stats */
static int nerrs;

static void handle_buf_thd_start(struct thdpool *pool, void *thddata);
static void handle_buf_thd_stop(struct thdpool *pool, void *thddata);
static struct thdpool *create_handle_buf_thdpool(const char *name, int nthds);

void thd_cleanup()
{
    thdpool_destroy(&gbl_handle_buf_write_thdpool, 0);
    thdpool_destroy(&gbl_handle_buf_read_thdpool, 0);
    thdpool_destroy(&gbl_handle_buf_queue_thdpool, 0);
}

int thd_init(void)
{
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
    gbl_handle_buf_write_thdpool = create_handle_buf_thdpool("handle_buf_write", gbl_maxwthreads);
    gbl_handle_buf_read_thdpool = create_handle_buf_thdpool("handle_buf_read", gbl_maxthreads);
    gbl_handle_buf_queue_thdpool = create_handle_buf_thdpool("handle_buf_queue", gbl_maxwthreads);
    Pthread_key_create(&thd_info_key, free);
    logmsg(LOGMSG_INFO, "thd_init: thread subsystem initialized\n");
    return 0;
}

#define THDPOOLS_COUNTER(c)                             \
    (thdpool_get_ ## c (gbl_handle_buf_read_thdpool) +  \
     thdpool_get_ ## c (gbl_handle_buf_write_thdpool) + \
     thdpool_get_ ## c (gbl_handle_buf_queue_thdpool))

#define THDPOOLS_WRITER_COUNTER(c)                      \
    (thdpool_get_ ## c (gbl_handle_buf_write_thdpool) + \
     thdpool_get_ ## c (gbl_handle_buf_queue_thdpool))

void thd_stats(void)
{
    logmsg(LOGMSG_USER, "num reqs              %d\n", THDPOOLS_COUNTER(passed) + THDPOOLS_COUNTER(enqueued));
    logmsg(LOGMSG_USER, "num waits             %d\n", THDPOOLS_COUNTER(enqueued));
    logmsg(LOGMSG_USER, "num items on queue    %d\n", THDPOOLS_COUNTER(queue_depth));
    logmsg(LOGMSG_USER, "num reads on queue    %d\n", thdpool_get_queue_depth(gbl_handle_buf_read_thdpool));
    logmsg(LOGMSG_USER, "num threads wrt busy/busy/idle %d/%d/%d\n", THDPOOLS_WRITER_COUNTER(nbusythds),
           thdpool_get_nbusythds(gbl_handle_buf_read_thdpool), THDPOOLS_COUNTER(nfreethds));
    logmsg(LOGMSG_USER, "---\n");
    logmsg(LOGMSG_USER, "config:MAXTHREADS     %d\n", gbl_maxthreads);
    logmsg(LOGMSG_USER, "config:MAXWRTTHREADS  %d\n", gbl_maxwthreads);
    logmsg(LOGMSG_USER, "gbl_maxwthreadpenalty %d\n", gbl_maxwthreadpenalty);
    logmsg(LOGMSG_USER, "penaltyincpercent     %d\n", gbl_penaltyincpercent);
    logmsg(LOGMSG_USER, "config:MAXQUEUE       %d\n", gbl_maxqueue);
    logmsg(LOGMSG_USER, "---\n");
    logmsg(LOGMSG_USER, "num queue fulls       %d\n", THDPOOLS_COUNTER(failed_dispatches));
    logmsg(LOGMSG_USER, "num errors            %d\n", nerrs);
    logmsg(LOGMSG_USER, "num thread creates    %d\n", THDPOOLS_COUNTER(creates));
    logmsg(LOGMSG_USER, "num retires           %d\n", THDPOOLS_COUNTER(exits));
    logmsg(LOGMSG_USER, "---\n");
    logmsg(LOGMSG_USER, "Use <send handle_buf_[read|write|queue] stat> to view busy thread info\n");
}

void thd_dbinfo2_stats(struct db_info2_stats *stats)
{
    stats->thr_max = gbl_maxthreads;
    stats->thr_maxwr = gbl_maxwthreads;
    stats->thr_cur = THDPOOLS_COUNTER(nthds);
    stats->q_max_conf = gbl_maxqueue;
    stats->q_max_reached = THDPOOLS_COUNTER(enqueued);
    stats->q_mean_reached = 0;
}

int thd_queue_depth(void)
{
    return THDPOOLS_COUNTER(queue_depth);
}

void thd_coalesce(struct dbenv *dbenv)
{
    thdpool_stop(gbl_handle_buf_read_thdpool);
    thdpool_stop(gbl_handle_buf_write_thdpool);
    thdpool_stop(gbl_handle_buf_queue_thdpool);
}

void dump_a_thd(struct thdpool *pool, pthread_t tid, int idle, void *thddata, void *user)
{
    int *pcnt = (int *)user;
    struct thd *thd = thddata;
    struct ireq *iq;
    uint64_t nowus;

    ++(*pcnt);
    nowus = comdb2_time_epochus();

    if (idle)
        logmsg(LOGMSG_USER, "idle  tid %p \n", (void *)tid);
    else {
        iq = thd->iq;
        logmsg(LOGMSG_USER, "busy  tid %p  time %5d ms  %-6s (%-3d) %-20s where %s %s\n",
               (void *)tid, U2M(nowus - iq->nowus), req2a(iq->opcode), iq->opcode,
               getorigin(iq), iq->where, iq->gluewhere);
    }
}

void thd_dump(void)
{
    int cnt = 0;

    thdpool_for_each_thd(gbl_handle_buf_write_thdpool, dump_a_thd, &cnt);
    thdpool_for_each_thd(gbl_handle_buf_queue_thdpool, dump_a_thd, &cnt);
    thdpool_for_each_thd(gbl_handle_buf_read_thdpool, dump_a_thd, &cnt);

    if (cnt == 0)
        logmsg(LOGMSG_USER, "no active threads\n");
}

struct thds_info {
    struct thd_info *tinfo;
    int max;
    int n;
};

void collect_a_thd(struct thdpool *pool, pthread_t tid, int idle, void *thddata, void *user)
{
    struct thd_info *tinfo;
    struct thd *thd = (struct thd*)thddata;
    struct ireq *iq = thd->iq;
    struct thds_info *thds_info = (struct thds_info *)user;
    uint64_t nowus;
    nowus = comdb2_time_epochus();

    if (thds_info->n == thds_info->max)
        return;

    tinfo = &thds_info->tinfo[thds_info->n];
    if (idle) {
        tinfo->state = strdup("idle");
        tinfo->isIdle = 1;
    } else {
        tinfo->state = strdup("busy");
        tinfo->time = U2M(nowus - iq->nowus);
        tinfo->machine = strdup(iq->frommach);
        tinfo->opcode = strdup(iq->where);
        tinfo->function = strdup(iq->gluewhere);
        tinfo->isIdle = 0;
    }
    ++thds_info->n;
}

int get_thd_info(thd_info **data, int *npoints)
{
    struct thds_info thds_info = {0};
    thds_info.max = THDPOOLS_COUNTER(nthds);
    thds_info.tinfo = malloc(thds_info.max * sizeof(thd_info));

    thdpool_for_each_thd(gbl_handle_buf_write_thdpool, collect_a_thd, &thds_info);

    *npoints = thds_info.n;
    *data = thds_info.tinfo;

    return 0;
}

void free_thd_info(thd_info *data, int npoints) {
    thd_info *tinfo = data;
    for (int i=0; i<npoints; ++i) {
        if (!tinfo->isIdle) {
            free(tinfo->machine);
            free(tinfo->opcode);
            free(tinfo->function);
        }
        free(tinfo->state);
        ++tinfo;
    }
    free(data);
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
    Pthread_cond_signal(&(p_slock->wait_cond));
    return 0;
}

int signal_buflock(struct buf_lock_t *p_slock)
{
    p_slock->reply_state = REPLY_STATE_DONE;
    Pthread_cond_signal(&(p_slock->wait_cond));
    return 0;
}

/* request handler */
void thd_req(void *vthd)
{
    comdb2_name_thread(__func__);
    struct thd *thd = (struct thd *)vthd;
    struct thread_info *thdinfo = pthread_getspecific(thd_info_key);
    struct thr_handle *thr_self;
    struct reqlogger *logger;
    struct ireq *iq;

    thr_self = thd->thr_self;
    logger = thrman_get_reqlogger(thr_self);
    iq = thd->iq;

    iq->startus = comdb2_time_epochus();
    iq->where = "executing";
    /*PROCESS REQUEST*/
    iq->reqlogger = logger;
    thrman_where(thr_self, req2a(iq->opcode));
    thrman_origin(thr_self, getorigin(iq));
    user_request_begin(REQUEST_TYPE_QTRAP, FLAG_REQUEST_TRACK_EVERYTHING);
    handle_ireq(iq);
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
    iq->where = "done executing";

    if (thd->do_inline) {
        free(iq);
        return;
    }

    /* before acquiring next request, yield */
    comdb2bma_yield_all();

    /* clean up and stats */ 
    if (iq->usedb && iq->ixused >= 0 &&
            iq->ixused < iq->usedb->nix &&
            iq->usedb->ixuse) {
        iq->usedb->ixuse[iq->ixused] += iq->ixstepcnt;
    }
    iq->ixused = -1;
    iq->ixstepcnt = 0;

    if (iq->dbglog_file) {
        sbuf2close(iq->dbglog_file);
        iq->dbglog_file = NULL;
    }
    if (iq->nwrites) {
        free(iq->nwrites);
        iq->nwrites = NULL;
    }
    if (iq->vfy_genid_hash) {
        hash_free(iq->vfy_genid_hash);
        iq->vfy_genid_hash = NULL;
    }
    if (iq->vfy_genid_pool) {
        pool_free(iq->vfy_genid_pool);
        iq->vfy_genid_pool = NULL;
    }
    iq->vfy_genid_track = 0;
    if (iq->vfy_idx_hash) {
        destroy_hash(iq->vfy_idx_hash, free_it);
        iq->vfy_idx_hash = NULL;
    }
    iq->vfy_idx_track = 0;
    iq->dup_key_insert = 0;
    free(iq);
    thd->iq = 0;

    // TODO: reuse
    /* Should not be done under lock - might be expensive */
    truncate_constraint_table(thdinfo->ct_add_table);
    truncate_constraint_table(thdinfo->ct_del_table);
    truncate_constraint_table(thdinfo->ct_add_index);
    hash_clear(thdinfo->ct_add_table_genid_hash);
    if (thdinfo->ct_add_table_genid_pool) {
        pool_clear(thdinfo->ct_add_table_genid_pool);
    }
    truncate_defered_index_tbl();
}

void thd_req_inline(struct ireq *iq) {
    struct thd inlinerq = {0};
    // TODO: reuse the constraint tables, etc
    inlinerq.do_inline = 1;
    inlinerq.iq = iq;

    handle_buf_thd_start(NULL, (void *)&inlinerq);
    thd_req(&inlinerq);
    handle_buf_thd_stop(NULL, (void *)&inlinerq);
}

/* sndbak error code &  return resources.*/
static int reterr(intptr_t curswap, struct thd *thd, struct ireq *iq, int rc)
/* 040307dh: 64bits */
{
    if (thd)
        thd->iq = 0;
    if (iq && iq->is_fromsocket) {
        if (iq->is_socketrequest) {
            sndbak_open_socket(iq->sb, NULL, 0, ERR_INTERNAL);
        } else {
            sndbak_socket(iq->sb, NULL, 0, ERR_INTERNAL);
            iq->sb = NULL;
        }
        free(iq);
    }
    if (iq && iq->ipc_sndbak) {
        iq->ipc_sndbak(iq, rc, iq->p_buf_out_end - iq->p_buf_out_start);
    }
    else if (comdb2_ipc_sndbak && curswap) {
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
    if (iq->is_fromsocket || iq->sorese) {
        if (iq->is_fromsocket) {
            /* process socket end request */
            if (iq->is_socketrequest) {
                sndbak_open_socket(iq->sb, NULL, 0, rc);
                free_bigbuf(iq->p_buf_out_start, iq->request_data);
                iq->request_data = iq->p_buf_out_start = NULL;
            } else {
                sndbak_socket(iq->sb, NULL, 0, rc);
                iq->sb = NULL;
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

        free(iq);
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
    int rc = handle_buf_main(dbenv, NULL, p_bigbuf, p_bigbuf + length, debug,
                             frommach, 0, NULL, NULL, REQ_SOCKREQUEST, NULL, 0,
                             rqid, NULL);

    return rc;
}

int handle_socket_long_transaction(struct dbenv *dbenv, SBUF2 *sb,
                                   uint8_t *p_buf, const uint8_t *p_buf_end,
                                   int debug, char *frommach, int frompid,
                                   char *fromtask)
{
    return handle_buf_main(dbenv, sb, p_buf, p_buf_end, debug, frommach,
                           frompid, fromtask, NULL, REQ_SOCKET, NULL, 0, 0, NULL);
}

void cleanup_lock_buffer(struct buf_lock_t *lock_buffer)
{
    if (lock_buffer == NULL)
        return;

    /* sbuf2 is owned by the appsock. Don't close it here. */

    Pthread_cond_destroy(&lock_buffer->wait_cond);
    Pthread_mutex_destroy(&lock_buffer->req_lock);

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
    return handle_buf_main(dbenv, NULL, p_buf, p_buf_end, debug, frommach, 0,
                           NULL, NULL, REQ_WAITFT, NULL, 0, 0, NULL);
}

int q_reqs_len(void) { return THDPOOLS_COUNTER(queue_depth); }

static int init_ireq_legacy(struct dbenv *dbenv, struct ireq *iq, SBUF2 *sb,
                            uint8_t *p_buf, const uint8_t *p_buf_end, int debug,
                            char *frommach, int frompid, char *fromtask,
                            osql_sess_t *sorese, int qtype, void *data_hndl,
                            int luxref, unsigned long long rqid, void *p_sinfo,
                            intptr_t curswap, int comdbg_flags)
{
    struct req_hdr hdr;
    uint64_t nowus;

    nowus = comdb2_time_epochus();

    /* set up request */
    const size_t len = sizeof(*iq) - offsetof(struct ireq, region3);
    bzero(&iq->region3, len);

    iq->corigin[0] = '\0';
    iq->debug_buf[0] = '\0';
    iq->tzname[0] = '\0';

    iq->where = "setup";
    iq->frommach = frommach ? intern(frommach) : NULL;
    iq->frompid = frompid;
    iq->gluewhere = "-";
    iq->debug = debug_this_request(gbl_debug_until) || (debug && gbl_debug);
    iq->debug_now = iq->nowus = nowus;
    iq->dbenv = dbenv;
    iq->fwd_tag_rqid = rqid;

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
    iq->comdbg_flags = comdbg_flags;

    /* HERE: unpack and get the proper lux - req_hdr_get_and_fixup_lux */
    if (!(iq->p_buf_in = req_hdr_get(&hdr, iq->p_buf_in, iq->p_buf_in_end, iq->comdbg_flags))) {
        logmsg(LOGMSG_ERROR, "%s:failed to unpack req header\n", __func__);
        return ERR_BADREQ;
    }

    iq->opcode = hdr.opcode;

    if (qtype == REQ_PQREQUEST) {
        iq->is_fake = 1;
        iq->is_dumpresponse = 1;
        iq->request_data = data_hndl;
    }

    if (qtype == REQ_SOCKET || qtype == REQ_SOCKREQUEST) {
        iq->sb = sb;
        iq->is_fromsocket = 1;
        iq->is_socketrequest = (qtype == REQ_SOCKREQUEST) ? 1 : 0;
    } else {
        iq->sb = NULL;
    }

    if (iq->is_socketrequest || qtype == REQ_SQLLEGACY) {
        iq->request_data = data_hndl;
    }

    iq->__limits.maxcost = gbl_querylimits_maxcost;
    iq->__limits.tablescans_ok = gbl_querylimits_tablescans_ok;
    iq->__limits.temptables_ok = gbl_querylimits_temptables_ok;

    iq->__limits.maxcost_warn = gbl_querylimits_maxcost_warn;
    iq->__limits.tablescans_warn = gbl_querylimits_tablescans_warn;
    iq->__limits.temptables_warn = gbl_querylimits_temptables_warn;

    iq->cost = 0;
    iq->luxref = luxref;

    if (iq->is_fromsocket) {
        if (iq->frommach == gbl_myhostname)
            snprintf(iq->corigin, sizeof(iq->corigin), "SLCL  %.8s PID %6.6d",
                     fromtask ? fromtask : "null", frompid);
        else
            snprintf(iq->corigin, sizeof(iq->corigin), "SRMT# %s PID %6.6d",
                     iq->frommach ? iq->frommach : "null", frompid);
    } else if (sorese) {
        iq->sorese = sorese;
        snprintf(iq->corigin, sizeof(iq->corigin), "SORESE# %15s %s RQST %llx",
                 iq->sorese->target.host,
                 osql_sorese_type_to_str(iq->sorese->type), iq->sorese->rqid);
        iq->timings.req_received = osql_log_time();
        /* cache these things so we don't change too much code */
        iq->tranddl = iq->sorese->scs.count;
        iq->tptlock = iq->sorese->is_tptlock;
        iq->sorese->iq = iq;
        if (!iq->debug) {
            if (gbl_who > 0) {
                gbl_who--;
                iq->debug = 1;
            }
        }
    }

    if (dbenv->num_dbs > 0 && (luxref < 0 || luxref >= dbenv->num_dbs)) {
        logmsg(LOGMSG_ERROR, "%s:luxref out of range %d max %d\n", __func__,
               luxref, dbenv->num_dbs);
        return ERR_REJECTED;
    }

    iq->origdb = dbenv->dbs[luxref]; /*lux is one based*/
    if (dbenv->num_dbs == 0 || iq->origdb == NULL)
        iq->origdb = &thedb->static_table;
    iq->usedb = iq->origdb;

    if (iq->frommach == NULL)
        iq->frommach = gbl_myhostname;

    return 0;
}

int gbl_handle_buf_add_latency_ms = 0;

struct thdpool *gbl_handle_buf_write_thdpool;
struct thdpool *gbl_handle_buf_read_thdpool;
struct thdpool *gbl_handle_buf_queue_thdpool;

static void handle_buf_work_pp(struct thdpool *pool, void *work, void *thddata, int op)
{
    struct ireq *iq = work;
    struct thd *thd = thddata;

    switch (op) {
    case THD_RUN:
        thd->iq = iq;
        thd_req(thd);
        break;
    default:
        break;
    }
}

static void handle_buf_thd_start(struct thdpool *pool, void *thddata)
{
    struct thd *thd = thddata;
    struct thread_info *thdinfo;

    if (pool == NULL) {
        thd->thr_self = thrman_self();
    } else {
        thd->thr_self = thrman_register(THRTYPE_REQ);
    }

    // This was already called in the thread that's calling this code if we're called
    // inline.  If we're called as a start routine of a new thread, we need to call it
    // ourselves.
    if (pool != NULL)
        backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);

    /* thdinfo is assigned to thread specific variable thd_info_key which
     * will automatically free it when the thread exits. */
    thdinfo = malloc(sizeof(struct thread_info));
    if (thdinfo == NULL) {
        logmsg(LOGMSG_FATAL, "**aborting due malloc failure thd %p\n", (void *)pthread_self());
        abort();
    }
    thdinfo->uniquetag = 0;
    thdinfo->ct_id_key = 0LL;

    thdinfo->ct_add_table = create_constraint_table();
    if (thdinfo->ct_add_table == NULL) {
        logmsg(LOGMSG_FATAL,
                "**aborting: cannot allocate constraint add table thd %p\n",
                (void *)pthread_self());
        abort();
    }
    thdinfo->ct_del_table = create_constraint_table();
    if (thdinfo->ct_del_table == NULL) {
        logmsg(LOGMSG_FATAL,
                "**aborting: cannot allocate constraint delete table thd %p\n",
                (void *)pthread_self());
        abort();
    }
    thdinfo->ct_add_index = create_constraint_index_table();
    if (thdinfo->ct_add_index == NULL) {
        logmsg(LOGMSG_FATAL,
                "**aborting: cannot allocate constraint add index table thd %p\n",
                (void *)pthread_self());
        abort();
    }
    thdinfo->ct_add_table_genid_hash = hash_init(sizeof(unsigned long long));
    thdinfo->ct_add_table_genid_pool = pool_setalloc_init(sizeof(unsigned long long), 0, malloc, free);

    /* Initialize the sql statement cache */
    thdinfo->stmt_cache = stmt_cache_new(NULL);
    if (thdinfo->stmt_cache == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d failed to create sql statement cache\n",
                __func__, __LINE__);
    }

    Pthread_setspecific(thd_info_key, thdinfo);
}

static void handle_buf_thd_stop(struct thdpool *pool, void *thddata)
{
    struct thread_info *thdinfo = pthread_getspecific(thd_info_key);

    delete_constraint_table(thdinfo->ct_add_table);
    delete_constraint_table(thdinfo->ct_del_table);
    delete_constraint_table(thdinfo->ct_add_index);
    hash_free(thdinfo->ct_add_table_genid_hash);
    if (thdinfo->ct_add_table_genid_pool) {
        pool_free(thdinfo->ct_add_table_genid_pool);
    }
    delete_defered_index_tbl();

    if (pool != NULL)
        backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
}

static void handle_buf_thd_dequeue(struct thdpool *pool, struct workitem *item, int timeout)
{
    if (gbl_handle_buf_add_latency_ms > 0)
        poll(0, 0, rand() % gbl_handle_buf_add_latency_ms);

    time_metric_add(thedb->handle_buf_queue_time, comdb2_time_epochms() - item->queue_time_ms);
    int nqueued = THDPOOLS_COUNTER(queue_depth) + thdpool_get_queue_depth(get_default_sql_pool(0));
    time_metric_add(thedb->queue_depth, nqueued);
}

static struct thdpool *create_handle_buf_thdpool(const char *name, int nthds)
{
    struct thdpool *pool = thdpool_create(name, sizeof(struct thd));
    if (pool != NULL) {
        thdpool_set_stack_size(pool, 4 * 1024 * 1024);
        thdpool_set_init_fn(pool, handle_buf_thd_start);
        thdpool_set_delt_fn(pool, handle_buf_thd_stop);
        thdpool_set_dque_fn(pool, handle_buf_thd_dequeue);
        thdpool_set_minthds(pool, 0);
        thdpool_set_maxthds(pool, nthds);
        thdpool_set_linger(pool, gbl_thd_linger);
        thdpool_set_maxqueue(pool, gbl_maxqueue);
    }
    return pool;
}

int gbl_queue_use_dedicated_writers = 1;
int gbl_queue_max_dedicated_writers = 16;
void handle_buf_set_queue_thdpool_maxthds(int max)
{
    if (max > thdpool_get_maxthds(gbl_handle_buf_queue_thdpool) && max <= gbl_queue_max_dedicated_writers)
        thdpool_set_maxthds(gbl_handle_buf_queue_thdpool, max);
}

int handle_buf_main2(struct dbenv *dbenv, SBUF2 *sb, const uint8_t *p_buf,
                     const uint8_t *p_buf_end, int debug, char *frommach,
                     int frompid, char *fromtask, osql_sess_t *sorese,
                     int qtype, void *data_hndl, int luxref,
                     unsigned long long rqid, void *p_sinfo, intptr_t curswap,
                     int comdbg_flags, void (*iq_setup_func)(struct ireq*, void *setup_data),
                     void *setup_data, int doinline, void* authdata)
{
    struct thdpool *pool = NULL;
    struct ireq *iq = NULL;
    int rc;

    if (db_is_exiting()) {
        return reterr(curswap, 0, NULL, ERR_REJECTED);
    }

    if (qtype != REQ_OFFLOAD && gbl_server_admin_mode)
        return reterr(curswap, 0, NULL, ERR_REJECTED);

    net_delay(frommach);

    if (gbl_who > 0) {
        --gbl_who;
        debug = 1;
    }

    iq = malloc(sizeof(struct ireq));
    if (!iq) {
        logmsg(LOGMSG_ERROR, "handle_buf:failed allocate req\n");
        return reterr(curswap, 0, iq, ERR_INTERNAL);
    }

    rc = init_ireq_legacy(dbenv, iq, sb, (uint8_t *)p_buf, p_buf_end, debug,
            frommach, frompid, fromtask, sorese, qtype,
            data_hndl, luxref, rqid, p_sinfo, curswap, comdbg_flags);
    if (rc) {
        logmsg(LOGMSG_ERROR, "handle_buf:failed to unpack req header\n");
        return reterr(curswap, /*thd*/ 0, iq, rc);
    }
    iq->sorese = sorese;
    if (iq_setup_func)
        iq_setup_func(iq, setup_data);

    if (iq->comdbg_flags == -1)
        iq->comdbg_flags = 0;

    if (p_buf && p_buf[7] == OP_FWD_BLOCK_LE)
        iq->comdbg_flags |= COMDBG_FLAG_FROM_LE;
    iq->authdata = authdata;

    if (doinline) {
        thd_req_inline(iq);
        return 0;
    }

    if (!is_req_write(iq->opcode)) {
        pool = gbl_handle_buf_read_thdpool;
    } else if (gbl_queue_use_dedicated_writers && (sorese != NULL) && sorese->is_qconsume_only == 1) {
        pool = gbl_handle_buf_queue_thdpool;
    } else {
        pool = gbl_handle_buf_write_thdpool;
        int n = gbl_maxwthreads - gbl_maxwthreadpenalty;
        thdpool_set_maxthds(pool, n);
    }
    rc = thdpool_enqueue(pool, handle_buf_work_pp, iq, 1, NULL, (qtype == REQ_OFFLOAD) ? THDPOOL_FORCE_QUEUE : 0);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "handle_buf:rejecting requests queue too full %d (max %d)\n",
               thdpool_get_queue_depth(pool), thdpool_get_maxqueue(pool));
        reterr_withfree(iq, ERR_REJECTED);
    }
    return 0;
}

int handle_buf_main(struct dbenv *dbenv, SBUF2 *sb, const uint8_t *p_buf,
                    const uint8_t *p_buf_end, int debug, char *frommach,
                    int frompid, char *fromtask, osql_sess_t *sorese, int qtype,
                    void *data_hndl, int luxref, unsigned long long rqid, 
                    void (*iq_setup_func)(struct ireq *, void *setup_data))
{
    return handle_buf_main2(dbenv, sb, p_buf, p_buf_end, debug, frommach,
                            frompid, fromtask, sorese, qtype, data_hndl, luxref,
                            rqid, 0, 0, 0, iq_setup_func, NULL, 0, NULL);
}

static int is_req_write(int opcode)
{
    if (opcode == OP_FWD_LBLOCK || opcode == OP_BLOCK ||
        opcode == OP_LONGBLOCK || opcode == OP_FWD_BLOCK ||
        opcode == OP_CLEARTABLE || opcode == OP_TRAN_FINALIZE ||
        opcode == OP_TRAN_COMMIT || opcode == OP_TRAN_ABORT ||
        opcode == OP_FWD_BLOCK_LE)
        return 1;
    return 0;
}
