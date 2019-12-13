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

#include <strings.h>
#include <poll.h>
#include <util.h>
#include <ctrace.h>
#include <netinet/in.h>
#include "osqlsession.h"
#include "osqlcomm.h"
#include "osqlblockproc.h"
#include "osqlrepository.h"
#include "osqlcheckboard.h"
#include "comdb2uuid.h"
#include <net_types.h>

#include "debug_switches.h"
#include "comdb2_atomic.h"

#include <uuid/uuid.h>
#include "str0.h"

static void _destroy_session(osql_sess_t **prq, int phase);

/**
 * Saves the current sql in a buffer for our use. Reusing the buffer
 * that was passed into the calling function is not always good, for
 * example, it may be clobbered by slt_pck().
 */
static void save_sql(struct ireq *iq, osql_sess_t *sess, const char *sql,
                     int sqlen)
{
    /* hippity hop job to save tho' strings */
    assert(iq->sqlhistory_len >= 0);
    if (iq->sqlhistory_ptr == NULL) {
        iq->sqlhistory_ptr = &iq->sqlhistory[0];
        iq->sqlhistory_len = 0;
    }
    if (iq->sqlhistory_len + sqlen > sizeof(iq->sqlhistory)) {
        /* need more SPACE... (I can't breath...) */
        if (iq->sqlhistory_ptr == &iq->sqlhistory[0]) {
            iq->sqlhistory_ptr = malloc(iq->sqlhistory_len + sqlen);
            if (!iq->sqlhistory_ptr)
                exit(1);
            memcpy(iq->sqlhistory_ptr, iq->sqlhistory, iq->sqlhistory_len);
        } else {
            iq->sqlhistory_ptr =
                realloc(iq->sqlhistory_ptr, iq->sqlhistory_len + sqlen);
            if (!iq->sqlhistory_ptr)
                exit(1);
        }
    }
    memcpy(iq->sqlhistory_ptr + iq->sqlhistory_len, sql, sqlen);
    sess->sql = iq->sqlhistory_ptr + iq->sqlhistory_len;
    iq->sqlhistory_len += sqlen;
}

/**
 * Terminates an in-use osql session (for which we could potentially
 * receive message from sql thread).
 * Returns 0 if success
 *
 * NOTE: it is possible to inline clean a request on master bounce,
 * which starts by unlinking the session first, and freeing bplog afterwards
 */
int osql_close_session(osql_sess_t **psess, int is_linked, const char *func,
                       const char *callfunc, int line)
{

    osql_sess_t *sess = *psess;
    int rc = 0;

    if (is_linked) {
        /* unlink the request so no more messages are received */
        rc = osql_repository_rem(sess, 1, func, callfunc, line);
    }
#if 0
   if(sess->stat.logsb) {
      uuidstr_t us;
      sbuf2printf(sess->stat.logsb, "%llu %s Close\n", sess->rqid, comdb2uuidstr(sess->uuid, us));
      sbuf2close(sess->stat.logsb);
      sess->stat.logsb = NULL;
   }
#endif

    /*
       wait for all receivers to go away (in current implem, this is only 1, the
       reader_thread
       since we removed the hash entry, no new messages are added
     */
    if (!rc) {
        while (ATOMIC_LOAD32(sess->clients) > 0) {
            poll(NULL, 0, 10);
        }

        _destroy_session(psess, 0);
    }

    return rc;
}

static int free_selectv_genids(void *obj, void *arg)
{
    free(obj);
    return 0;
}

static void _destroy_session(osql_sess_t **prq, int phase)
{
    osql_sess_t *rq = *prq;

    switch (phase) {
    case 0:
        if (rq->selectv_writelock_on_update) {
            hash_for(rq->selectv_genids, free_selectv_genids, NULL);
            hash_clear(rq->selectv_genids);
            hash_free(rq->selectv_genids);
        }
    case 1:
        Pthread_cond_destroy(&rq->cond);
    case 2:
        Pthread_mutex_destroy(&rq->mtx);
    case 3:
        Pthread_mutex_destroy(&rq->completed_lock);
    case 4:
        free(rq);
    }

    *prq = NULL;
}

/**
 * Get the request id, aka rqid
 *
 */
inline unsigned long long osql_sess_getrqid(osql_sess_t *sess)
{
    return sess->rqid;
}

/**
 * Register client
 * Prevent temporary the session destruction
 *
 */
int osql_sess_addclient(osql_sess_t *sess)
{
#if 0
   uuidstr_t us;
   comdb2uuidstr(sess->uuid, us);
   fprintf(stderr, "\t\tADDCLNT p_sees=%p rqid=[%llx %s] sess->completed=%llx thread=%d clients=%d p_sess->iq=%p\n",
         sess, sess->rqid, us, sess->completed, pthread_self(), sess->clients+1, sess->iq);
#endif

    ATOMIC_ADD32(sess->clients, 1);

    return 0;
}

/**
 * Register client
 * Prevent temporary the session destruction
 *
 */
int osql_sess_remclient(osql_sess_t *sess)
{
#if 0
   uuidstr_t us;
   comdb2uuidstr(sess->uuid, us);
   fprintf(stderr, "\t\tREMCLNT p_sees=%p rqid=%llx uuid=%s sess->completed=%llx thread=%d clients=%d p_sess->iq=%p\n",
         sess, sess->rqid, us, sess->completed, pthread_self(), sess->clients-1, sess->iq);
#endif

    int loc_clients = ATOMIC_ADD32(sess->clients, -1);

    if (loc_clients < 0) {
        uuidstr_t us;
        fprintf(stderr,
                "%s: BUG ALERT, session %llu %s freed one too many times\n",
                __func__, sess->rqid, comdb2uuidstr(sess->uuid, us));
    }

    return 0;
}

/**
 * Print summary session
 *
 */
int osql_sess_getcrtinfo(void *obj, void *arg)
{

    osql_sess_t *sess = (osql_sess_t *)obj;
    uuidstr_t us;

    printf("   %llx %s %s %s\n", sess->rqid, comdb2uuidstr(sess->uuid, us),
           sess->host ? "REMOTE" : "LOCAL",
           sess->host ? sess->host : "localhost");

    return 0;
}


/**
 * Mark session duration and reported result.
 *
 */
int osql_sess_set_complete(unsigned long long rqid, uuid_t uuid,
                           osql_sess_t *sess, struct errstat *xerr)
{

    Pthread_mutex_lock(&sess->completed_lock);

    if (sess->rqid != rqid || comdb2uuidcmp(uuid, sess->uuid)) {
        Pthread_mutex_unlock(&sess->completed_lock);
        return 0;
    }

    if (sess->completed != 0) {
        Pthread_mutex_unlock(&sess->completed_lock);
        return 0;
    }

    sess->endus = comdb2_time_epochus();

    if (xerr) {
        uint8_t *p_buf = (uint8_t *)xerr;
        uint8_t *p_buf_end = (p_buf + sizeof(struct errstat));
        osqlcomm_errstat_type_get(&sess->xerr, p_buf, p_buf_end);
    } else {
        bzero(&sess->xerr, sizeof(sess->xerr));
    }

    sess->completed = rqid;
    comdb2uuidcpy(sess->completed_uuid, uuid);
    Pthread_mutex_unlock(&sess->completed_lock);

    return 0;
}

/**
 * Returns "true" if code is
 *   - SQLITE_DEADLOCK
 *   - SQLITE_TOOEARLY
 */
static inline int is_session_repeatable(int code)
{
    if (code ==
        SQLITE_DEADLOCK) /* sql thread deadlocked with replication thread */
        return 1;
    if (code == OSQL_TOOEARLY) /* node is going down/not accepting requests at
                                  this time */
        return 1;
    if (code == OSQL_FAILDISPATCH) /* node handles too many requests */
        return 1;
#if 0
   We might want to retry in this case, though I am not
   sure it is a good thing at this time
   if(code == OSQL_NOOSQLTHR)
      return 1;
#endif
    return 0;
}

/**
 * Returns
 * - total time (tottm)
 * - last roundtrip time (rtt)
 * - retries (rtrs)
 *
 */
void osql_sess_getsummary(osql_sess_t *sess, int *tottm, int *rtrs)
{
    *tottm = U2M(sess->endus - sess->startus);
    *rtrs = sess->iq ? sess->iq->retries : 0;
}

/**
 * Log query to the reqlog
 */
void osql_sess_reqlogquery(osql_sess_t *sess, struct reqlogger *reqlog)
{
    uuidstr_t us;
    char rqid[25];
    if (sess->rqid == OSQL_RQID_USE_UUID) {
        comdb2uuidstr(sess->uuid, us);
    } else
        snprintf(rqid, sizeof(rqid), "%llx", sess->rqid);

    reqlog_logf(reqlog, REQL_INFO,
                "rqid %s node %s time %" PRId64 "ms rtrs %d queuetime=%" PRId64
                "ms \"%s\"\n",
                sess->rqid == OSQL_RQID_USE_UUID ? us : rqid, sess->host ? sess->host : "",
                U2M(sess->endus - sess->startus), reqlog_get_retries(reqlog),
                U2M(reqlog_get_queue_time(reqlog)),
                sess->sql ? sess->sql : "()");
}

/**
 * Returns associated blockproc transaction
 * Only used for saveop, so return NULL if it's completed or terminated.
 */
void *osql_sess_getbptran(osql_sess_t *sess)
{
    void *bsql = NULL;

    Pthread_mutex_lock(&sess->mtx);
    if (sess->iq && !sess->completed && !sess->terminate) {
        bsql = sess->iq->blocksql_tran;
    }
    Pthread_mutex_unlock(&sess->mtx);
    return bsql;
}

/**
 * Handles a new op received for session "rqid"
 * It saves the packet in the local bplog
 * Return 0 if success
 * Set found if the session is found or not
 *
 */
int osql_sess_rcvop(unsigned long long rqid, uuid_t uuid, int type, void *data,
                    int datalen, int *found)
{
    int rc = 0;
    int is_msg_done = 0;
    struct errstat *perr;

    /* NOTE: before retrieving a session, we have to figure out if this is a
       sorese completion and lock the repository until the session is dispatched
       This prevents the race against signal_rtoff forcefully cleanup */
    is_msg_done = osql_comm_is_done(type, data, datalen,
                                    rqid == OSQL_RQID_USE_UUID, &perr, NULL);

    /* get the session */
    osql_sess_t *sess = osql_repository_get(rqid, uuid, is_msg_done);
    if (!sess) {
        /* in the current implementation we tolerate redundant ops from session
         * that have been already terminated--discard the packet in that case */
        uuidstr_t us;
        comdb2uuidstr(uuid, us);
        logmsg(LOGMSG_ERROR,
               "discarding packet for %llx %s, session not found\n", rqid, us);
        *found = 0;

        /* we used to free data here, but some callers like net_osql_rpl_tail()
         * (and probably all others expect to manage it themselves */
        return 0;
    }

    if (is_msg_done && perr && htonl(perr->errval) == SQLITE_ABORT &&
        !bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DISABLE_SELECTVONLY_TRAN_NOP)) {
        /* release the session */
        if ((rc = osql_repository_put(sess, is_msg_done)) != 0) {
            logmsg(LOGMSG_ERROR, "%s: osql_repository_put rc =%d\n", __func__,
                   rc);
        }

        /* sqlite aborted the transaction, skip all the work here
           master not needed */
        rc = sql_cancelled_transaction(sess->iq);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed cancelling transaction! rc %d\n",
                   __func__, rc);
        }

        /* done here */
        return rc;
    }

    *found = 1;

    Pthread_mutex_lock(&sess->completed_lock);
    /* ignore new coming osql packages */
    if (sess->completed || sess->dispatched || sess->terminate) {
        uuidstr_t us;
        Pthread_mutex_unlock(&sess->completed_lock);
        if ((rc = osql_repository_put(sess, is_msg_done)) != 0) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d osql_repository_put failed with rc %d\n", __func__,
                   __LINE__, rc);
        }
        comdb2uuidstr(uuid, us);
        logmsg(LOGMSG_INFO,
               "%s: rqid=%llx, uuid=%s is already done, ignoring packages\n",
               __func__, rqid, us);
        return 0;
    }
    Pthread_mutex_unlock(&sess->completed_lock);

    /* save op */
    int rc_out = osql_bplog_saveop(sess, data, datalen, rqid, uuid, type);

    /* if rc_out, sess is FREED! */
    if (!rc_out) {
        /* Must increment seq under completed_lock */
        Pthread_mutex_lock(&sess->completed_lock);
        if (sess->rqid == rqid || (rqid == OSQL_RQID_USE_UUID &&
                                   comdb2uuidcmp(sess->uuid, uuid) == 0)) {
            sess->seq++;
            sess->last_row = time(NULL);
        }

        Pthread_mutex_unlock(&sess->completed_lock);
    }

    /* release the session */
    if ((rc = osql_repository_put(sess, is_msg_done)) != 0) {
        logmsg(LOGMSG_ERROR, "%s: osql_repository_put rc =%d\n", __func__, rc);
    }

    if (rc_out)
        return rc_out;

    if (rc || rc_out) { /* something is wrong with the session, terminate it*/
        sess->terminate = OSQL_TERMINATE;
    }

    return rc_out ? rc_out : rc;
}

/**
 * Mark the session terminated if the node "arg"
 * machine the provided session "obj",
 * If "*arg: is 0, "obj" is marked terminated anyway
 *
 */
int osql_session_testterminate(void *obj, void *arg)
{
    osql_sess_t *sess = (osql_sess_t *)obj;
    char *node = arg;
    int need_clean = 0;

    if (node && sess->host != node) {
        /* if this is for a different node, ignore */
        return 0;
    }

    Pthread_mutex_lock(&sess->mtx);
    Pthread_mutex_lock(&sess->completed_lock);

    sess->terminate = OSQL_TERMINATE;
    need_clean = sess->dispatched;

    Pthread_mutex_unlock(&sess->completed_lock);
    Pthread_mutex_unlock(&sess->mtx);

    if (!need_clean) {
        /* there is a block processor thread working on this */
        return 0;
    }

    /* step 1) make sure no reader thread finds the session again */
    int rc = osql_repository_rem(sess, 0, __func__, NULL,
                                 0); /* already have exclusive lock */
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to remove session from repository rc=%d\n", __func__,
               rc);
    }

    Pthread_mutex_lock(&sess->mtx);
    Pthread_mutex_lock(&sess->completed_lock);

    /* step 2) wait for current reader threads to go away */
    while (ATOMIC_LOAD32(sess->clients) > 0 && need_clean) {
        Pthread_mutex_unlock(&sess->completed_lock);
        Pthread_mutex_unlock(&sess->mtx);

        poll(NULL, 0, 10);

        /* the reader thread might just dispatch this!
           need to check again the condition */
        Pthread_mutex_lock(&sess->mtx);
        Pthread_mutex_lock(&sess->completed_lock);

        need_clean = sess->dispatched;
    }
    Pthread_mutex_unlock(&sess->completed_lock);
    Pthread_mutex_unlock(&sess->mtx);

    /* reader thread dispatched the session before returning control */
    if (!need_clean)
        return 0;

    /* NOTE: at this point there will be no other bplog updates coming from
       this sorese session; the session might still be worked on; if that is the
       case the session is marked already complete, since this is done by reader
       thread which bumps up clients! */

    /* step 3) check if this is complete; if it is, it will/is being
       dispatched if not complete, we need to clear it right now */

    osql_bplog_free(sess->iq, 0, __func__, NULL, 0);

    return 0;
}

typedef struct {
    char *tablename;
    unsigned long long genid;
    int tableversion;
    bool get_writelock;
} selectv_genid_t;

int gbl_selectv_writelock_on_update = 1;

/**
 * Creates an sock osql session
 * Runs on master node when an initial sorese message is received
 * Returns created object if success, NULL otherwise
 *
 */
osql_sess_t *osql_sess_create(const char *sql, int sqlen, char *tzname,
                              int type, unsigned long long rqid, uuid_t uuid,
                              bool is_reorder_on)
{
    osql_sess_t *sess = NULL;

#ifdef TEST_QSQL_REQ
    uuidstr_t us;
    logmsg(LOGMSG_INFO, "%s: Opening request %llu %s\n", __func__, rqid,
           comdb2uuidstr(uuid, us));
#endif

    /* alloc object */
    sess = (osql_sess_t *)calloc(sizeof(*sess), 1);
    if (!sess) {
        logmsg(LOGMSG_ERROR, "%s:unable to allocate %zu bytes\n", __func__,
               sizeof(*sess));
        return NULL;
    }
#if DEBUG_REORDER
    uuidstr_t us;
    comdb2uuidstr(uuid, us);
    logmsg(LOGMSG_DEBUG, "%s:processing sql=%s sess=%p, uuid=%s\n", __func__,
           sql, sess, us);
#endif

    /* init sync fields */
    Pthread_mutex_init(&sess->completed_lock, NULL);
    Pthread_mutex_init(&sess->mtx, NULL);
    Pthread_cond_init(&sess->cond, NULL);

    sess->rqid = rqid;
    comdb2uuidcpy(sess->uuid, uuid);
    /*save_sql(iq, sess, sql, sqlen);*/
    sess->type = type;
    sess->startus = comdb2_time_epochus();
    sess->is_reorder_on = is_reorder_on;
    sess->selectv_writelock_on_update = gbl_selectv_writelock_on_update;
    if (sess->selectv_writelock_on_update)
        sess->selectv_genids =
            hash_init(offsetof(selectv_genid_t, get_writelock));
    if (tzname)
        strncpy0(sess->tzname, tzname, sizeof(sess->tzname));

    sess->clients = 1;

    return sess;
}

int osql_cache_selectv(int type, osql_sess_t *sess, unsigned long long rqid,
                       char *rpl)
{
    char *p_buf;
    int rc = -1;
    selectv_genid_t *sgenid, fnd = {0};
    enum {
        OSQLCOMM_UUID_RPL_TYPE_LEN = 4 + 4 + 16,
        OSQLCOMM_RPL_TYPE_LEN = 4 + 4 + 8
    };
    switch (type) {
    case OSQL_UPDATE:
    case OSQL_DELETE:
    case OSQL_UPDREC:
    case OSQL_DELREC:
        p_buf = rpl + (rqid == OSQL_RQID_USE_UUID ? OSQLCOMM_UUID_RPL_TYPE_LEN
                                                  : OSQLCOMM_RPL_TYPE_LEN);
        buf_no_net_get(&fnd.genid, sizeof(fnd.genid), p_buf,
                       p_buf + sizeof(fnd.genid));
        assert(sess->tablename);
        fnd.tablename = sess->tablename;
        fnd.tableversion = sess->tableversion;
        if ((sgenid = hash_find(sess->selectv_genids, &fnd)) != NULL)
            sgenid->get_writelock = 1;
        rc = 0;
        break;
    case OSQL_RECGENID:
        p_buf = rpl + (rqid == OSQL_RQID_USE_UUID ? OSQLCOMM_UUID_RPL_TYPE_LEN
                                                  : OSQLCOMM_RPL_TYPE_LEN);
        buf_no_net_get(&fnd.genid, sizeof(fnd.genid), p_buf,
                       p_buf + sizeof(fnd.genid));
        assert(sess->tablename);
        fnd.tablename = sess->tablename;
        if (hash_find(sess->selectv_genids, &fnd) == NULL) {
            sgenid = (selectv_genid_t *)calloc(sizeof(*sgenid), 1);
            sgenid->genid = fnd.genid;
            sgenid->tablename = sess->tablename;
            sgenid->tableversion = sess->tableversion;
            sgenid->get_writelock = 0;
            hash_add(sess->selectv_genids, sgenid);
        }
        rc = 0;
        break;
    }
    return rc;
}

typedef struct {
    int (*wr_sv)(void *, const char *tablename, int tableversion,
                 unsigned long long genid);
    void *arg;
} sv_hf_args;

int process_selectv(void *obj, void *arg)
{
    sv_hf_args *hf_args = (sv_hf_args *)arg;
    selectv_genid_t *sgenid = (selectv_genid_t *)obj;
    if (sgenid->get_writelock) {
        return (*hf_args->wr_sv)(hf_args->arg, sgenid->tablename,
                                 sgenid->tableversion, sgenid->genid);
    }
    return 0;
}

int osql_process_selectv(osql_sess_t *sess,
                         int (*wr_sv)(void *arg, const char *tablename,
                                      int tableversion,
                                      unsigned long long genid),
                         void *wr_selv_arg)
{
    sv_hf_args hf_args = {.wr_sv = wr_sv, .arg = wr_selv_arg};
    if (!sess->selectv_writelock_on_update)
        return 0;
    return hash_for(sess->selectv_genids, process_selectv, &hf_args);
}

int osql_sess_type(osql_sess_t *sess)
{
    return sess->type;
}

int osql_sess_queryid(osql_sess_t *sess)
{
    return sess->queryid;
}

// get sess->uuid into uuid as destination
void osql_sess_getuuid(osql_sess_t *sess, uuid_t uuid)
{
    comdb2uuidcpy(uuid, sess->uuid);
}

inline int osql_session_set_ireq(osql_sess_t *sess, struct ireq *iq)
{
    sess->iq = iq;
    return 0;
}

inline struct ireq *osql_session_get_ireq(osql_sess_t *sess)
{
    return sess->iq;
}

/**
 * Force a session to end
 * Call with sess->mtx and sess->completed_lock held
 */
static int osql_sess_set_terminate(osql_sess_t *sess)
{
    int rc = 0;
    sess->terminate = OSQL_TERMINATE;
    rc = osql_repository_rem(sess, 0, __func__, NULL,
                             __LINE__); /* already have exclusive lock */
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to remove session from repository rc=%d\n", __func__,
               rc);
        return rc;
    }

    assert(!sess->completed && !sess->dispatched);
    /* no one will work on this; need to clear it */
    osql_bplog_free(sess->iq, 0, __func__, NULL, __LINE__);
    return rc;
}

/**
 * Terminate a session if the session is not yet completed/dispatched
 * Return 0 if session is successfully terminated,
 *        -1 for errors,
 *        1 otherwise (if session was already processed)
 */
int osql_sess_try_terminate(osql_sess_t *sess)
{
    int rc;

    Pthread_mutex_lock(&sess->mtx);
    Pthread_mutex_lock(&sess->completed_lock);

    if ( sess->completed | sess->dispatched) {
        Pthread_mutex_unlock(&sess->completed_lock);
        Pthread_mutex_unlock(&sess->mtx);
        return 1;
    }

    rc = osql_sess_set_terminate(sess);
    if (rc) {
        abort();
    }

    return 0;
}


int handle_buf_sorese(osql_sess_t *sess)
{
    int debug;
    int rc = 0;

    debug = debug_this_request(gbl_debug_until);
    if (gbl_who > 0 && gbl_debug) {
        debug = 1;
    }

    Pthread_mutex_lock(&sess->mtx);
    Pthread_mutex_lock(&sess->completed_lock);

    if (sess->dispatched || sess->terminate) {
        Pthread_mutex_unlock(&sess->completed_lock);
        Pthread_mutex_unlock(&sess->mtx);
        return 0;
    }

    sess->dispatched = 1;

    rc = handle_buf_main(thedb, sess->iq, NULL, NULL, NULL, debug, 0, 0, NULL, NULL,
                         REQ_OFFLOAD, NULL, 0, 0);

    Pthread_mutex_unlock(&sess->completed_lock);
    Pthread_mutex_unlock(&sess->mtx);

    return rc;
}

