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
#include "intern_strings.h"

#include <uuid/uuid.h>
#include "str0.h"
#include "reqlog.h"

struct sess_impl {
    int clients; /* number of clients; prevents freeing rq while
                    reader_thread gets a new packet for it */

    pthread_mutex_t completed_lock;

    bool dispatched : 1; /* Set when session is dispatched to handle_buf */
    bool terminate : 1;  /* Set when this session is about to be terminated */

    uint8_t *buf; /* toblock request buffer */
};

static void _destroy_session(osql_sess_t **psess);

/**
 * Terminates an in-use osql session (for which we could potentially
 * receive message from sql thread).
 * Returns 0 if success
 *
 * This function will remove from osql_repository_rem() if is_linked is set
 * then wait till there are no more clients using this sess then destroy obj
 *
 * NOTE:
 * - it is possible to inline clean a request on master bounce,
 *   which starts by unlinking the session first, and freeing bplog afterwards
 *
 * - if caller has already removed sess from osql repository, they should
 *   call this function with is_linked = 0
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

    while (ATOMIC_LOAD32(sess->clients) > 0) {
        poll(NULL, 0, 10);
    }
    _destroy_session(psess, 0);

    return 0;
}

static void _destroy_session(osql_sess_t **psess)
{
    osql_sess_t *sess = *psess;

    if (sess->impl->buf)
        free(sess->impl->buf);
    Pthread_cond_destroy(&sess->cond);
    Pthread_mutex_destroy(&sess->mtx);
    Pthread_mutex_destroy(&sess->impl->completed_lock);
    free(sess);

    *psess = NULL;
}

/**
 * Mark that the reader thread is working on this session
 *
 * Return error if session is dispatched; it is silently 
 * ignored in implementations when redundant packets can
 * arrive
 */
int osql_sess_addclient(osql_sess_t *sess)
{
    if (sess->impl->dispatched)
        return -1;
    ATOMIC_ADD32(sess->impl->clients, 1);
    return 0;
}

/**
 * The reader_thread is done with updating the bplog
 * CALLING THIS UNDER REPOSITORY LOCK
 * Since no termination can race (because of the lock), we can
 * check if the session is terminated.  If the session is terminated
 * the terminating thread skipped the session (since reader was working
 * on it).  The reader thread needs to free this session
 * If the session is not terminated, it can be dispatched if all bplog
 * was received, so we lit the flag (this will prevent any terminating 
 * thread from touching it).
 *
 */
int osql_sess_remclient(osql_sess_t *sess, int bplog_complete)
{
    int loc_clients = ATOMIC_ADD32(sess->impl->clients, -1);

    if (loc_clients < 0) {
        abort(); // remove this in future
        uuidstr_t us;
        logmsg(LOGMSG_ERROR,
               "%s: BUG ALERT, session %llu %s freed one too many times\n",
               __func__, sess->rqid, comdb2uuidstr(sess->uuid, us));
    }

    if (sess->terminate) {
        return 1;
    }
  
    if (bplog_complete)
        sess->dispatched = true;

    return 0;
}

/**
 * Return malloc-ed string:
 * sess_type rqid uuid local/remote host
 *
 */
char* osql_sess_info(osql_sess_t * sess)
{
    uuidstr_t us;
    char *ret = malloc(OSQL_SESS_INFO_LEN);

    if (ret) {
        snprintf(ret, OSQL_SESS_INFO_LEN, "%s, %llx %s %s%s", 
                osql_sorese_type_to_str(sess->type),
                sess->rqid, comdb2uuidstr(sess->uuid, us),
                sess->host ? "REMOTE " : "LOCAL ",
                sess->host ? sess->host : "");
    }
    return ret;
}

/**
 * Log query to the reqlog
 */
void osql_sess_reqlogquery(osql_sess_t *sess, struct reqlogger *reqlog)
{
    char *info = osql_sess_info(sess);
    reqlog_logf(reqlog, REQL_INFO,
                "%s time %" PRId64 "ms queuetime=%" PRId64
                "ms \"%s\"\n",
                (info)?info:"unknown",
                U2M(sess->endus - sess->startus),
                U2M(reqlog_get_queue_time(reqlog)),
                sess->sql ? sess->sql : "()");
    if(info)
        free(info);
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
    sess_impl_t *impl = NULL;
    int rc = 0;
    int is_msg_done = 0;
    struct errstat *perr;

    is_msg_done = osql_comm_is_done(NULL, type, data, datalen,
                                    rqid == OSQL_RQID_USE_UUID, &perr);

    /* get the session; dispatched sessions are ignored */
    osql_sess_t *sess = osql_repository_get(, uuid);
    if (!sess) {
        /* in the current implementation we tolerate redundant ops from session
         * that have been already terminated--discard the packet in that case */
        *found = 0;
        return 0;
    }
    impl = sess->impl;

    /* we have received an OSQL_XERR; replicant is wants to abort the transaction;
       we are simply discarding this session */
    if (is_msg_done && perr) {
        /* release the session */
        if ((rc = osql_repository_put(sess, is_msg_done)) != 0) {
            logmsg(LOGMSG_ERROR, "%s: osql_repository_put rc =%d\n", __func__,
                   rc);
        }

        /* sqlite aborted the transaction, skip all the work here
           master not needed */
        sql_cancelled_transaction(sess->iq);

        /* done here */
        return perr->errval;
    }

    *found = 1;

    /* save op */
    int irc = osql_bplog_saveop(sess, sess->tran, data, datalen, type);

    /* release the session */
    if ((rc = osql_repository_put(sess)) < 0) {
        logmsg(LOGMSG_ERROR, "%s: osql_repository_put rc =%d\n", __func__, rc);
    } else if (rc == 1) {
        /* session was marked terminated already */
        TODO: if not dispatched, need to clear it
    }

    if (irc) {
        osql_sess_try_terminate(sess);
        return irc;
    }

    return 0;
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
    int rc;

    if (node && sess->host != node) {
        /* if this is for a different node, ignore */
        return 0;
    }

    rc = osql_sess_try_terminate(sess);
    if (rc < 0)
        return rc;

    if (rc == 1) {
        /* session is not dispatched */
        
    }

    return 0;
}

/**
 * Creates an sock osql session
 * Runs on master node when an initial sorese message is received
 * Returns created object if success, NULL otherwise
 *
 */
osql_sess_t *osql_sess_create(const char *sql, int sqlen, char *tzname,
                              int type, unsigned long long rqid, uuid_t uuid,
                              const char *host, uint8_t *buf,
                              bool is_reorder_on)
{
    osql_sess_t *sess = NULL;
    sess_impl_t *impl;

#ifdef TEST_QSQL_REQ
    uuidstr_t us;
    logmsg(LOGMSG_INFO, "%s: Opening request %llu %s\n", __func__, rqid,
           comdb2uuidstr(uuid, us));
#endif

    /* alloc object */
    sess = (osql_sess_t *)calloc(sizeof(osql_sess_t) + sizeof(sess_impl_t), 1);
    if (!sess) {
        logmsg(LOGMSG_ERROR, "%s:unable to allocate %zu bytes\n", __func__,
               sizeof(*sess));
        return NULL;
    }
    sess->impl = impl = (sess_impl_t *)(sess + 1);
#if DEBUG_REORDER
    uuidstr_t us;
    comdb2uuidstr(uuid, us);
    logmsg(LOGMSG_DEBUG, "%s:processing sql=%s sess=%p, uuid=%s\n", __func__,
           sql, sess, us);
#endif

    /* init sync fields */
    Pthread_mutex_init(&impl->completed_lock, NULL);
    Pthread_mutex_init(&sess->mtx, NULL);
    Pthread_cond_init(&sess->cond, NULL);

    sess->rqid = rqid;
    comdb2uuidcpy(sess->uuid, uuid);
    sess->type = type;
    sess->host = host ? intern(host) : NULL;
    sess->startus = comdb2_time_epochus();
    sess->is_reorder_on = is_reorder_on;
    if (tzname)
        strncpy0(sess->tzname, tzname, sizeof(sess->tzname));

    sess->impl->clients = 1;
    sess->impl->buf = buf;

    /* create bplog so we can collect ops from sql thread */
    sess->tran = osql_bplog_create(sess->is_reorder_on);
    if (!sess->tran) {
        logmsg(LOGMSG_ERROR, "%s Unable to create new bplog\n", __func__);
        _destroy_session(sess);
        sess = NULL;
    }

    return sess;
}

int osql_sess_queryid(osql_sess_t *sess)
{
    return sess->queryid;
}

/**
 * Terminate a session if the session is not yet completed/dispatched
 * we come here from osql_repository_add() if already in osql hash map
 * which can happen in case there is an early replay
 * Return 0 if session is successfully terminated,
 *        -1 for errors,
 *        1 otherwise (if session was already processed)
 */
int osql_sess_try_terminate(osql_sess_t *sess)
{
    sess_impl_t *impl = sess->impl;
    int rc;

    Pthread_mutex_lock(&sess->mtx);
    Pthread_mutex_lock(&impl->completed_lock);
    
    if (impl->dispatched) {
        Pthread_mutex_unlock(&impl->completed_lock);
        Pthread_mutex_unlock(&sess->mtx);
        return 1;
    }

    sess->terminate = true;


    if (!reader_thread) {
        /* no one will work on this; need to clear it */
        osql_close_session(sess, 0, __func__, NULL, __LINE__);
    }

    return (ATOMIC_LOAD32(sess->clients) <= 0)?0:;
}

int handle_buf_sorese(osql_sess_t *sess)
{
    sess_impl_t *impl = sess->impl;
    int debug;
    int rc = 0;

    debug = debug_this_request(gbl_debug_until);
    if (gbl_who > 0 && gbl_debug) {
        debug = 1;
    }

    Pthread_mutex_lock(&sess->mtx);
    Pthread_mutex_lock(&impl->completed_lock);

    if (impl->dispatched || impl->terminate) {
        Pthread_mutex_unlock(&impl->completed_lock);
        Pthread_mutex_unlock(&sess->mtx);
        return 0;
    }

    sess->endus = comdb2_time_epochus();
    impl->dispatched = true;
    bzero(&sess->xerr, sizeof(sess->xerr));
    rc = handle_buf_main(thedb, sess->iq, NULL, NULL, NULL, debug, 0, 0, NULL,
                         NULL, REQ_OFFLOAD, NULL, 0, 0);

    Pthread_mutex_unlock(&impl->completed_lock);
    Pthread_mutex_unlock(&sess->mtx);

    return rc;
} 
