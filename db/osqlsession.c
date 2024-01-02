/*
   Copyright 2020 Bloomberg Finance L.P.

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
#include "osqlsqlnet.h"

struct sess_impl {
    int clients; /* number of threads using the session */

    unsigned dispatched : 1; /* Set when session is dispatched to handle_buf */
    unsigned terminate : 1;  /* Set when this session is about to be terminated */
    unsigned socket : 1;     /* Set if request comes over socket instead of net */
    unsigned embedded_sql : 1; /* Set if sql is part of session malloc object */

    pthread_mutex_t mtx; /* dispatched/terminate/clients protection */
};

static void _destroy_session(osql_sess_t **psess);
static int handle_buf_sorese(osql_sess_t *psess);
static osql_sess_t *_osql_sess_create(osql_sess_t *sess, char *tzname, int type,
                                      unsigned long long rqid, uuid_t uuid,
                                      const char *host, int is_reorder_on);

/**
 * Creates an sock osql session
 * Runs on master host when an initial sorese message is received
 * Returns created object if success, NULL otherwise
 *
 */
osql_sess_t *osql_sess_create(const char *sql, int sqlen, char *tzname,
                              int type, unsigned long long rqid, uuid_t uuid,
                              const char *host, int is_reorder_on)
{
    osql_sess_t *sess = NULL;

    /* alloc object */
    sess = (osql_sess_t *)calloc(
        sizeof(osql_sess_t) + sizeof(sess_impl_t) + sqlen + 1, 1);
    if (!sess) {
        logmsg(LOGMSG_ERROR, "%s:unable to allocate %zu bytes\n", __func__,
               sizeof(*sess));
        return NULL;
    }
    sess->impl = (sess_impl_t *)(sess + 1);
    sess->sql = (char *)(sess->impl + 1);
    strncpy0((char *)sess->sql, sql, sqlen + 1);
    sess->impl->embedded_sql = 1;

    return _osql_sess_create(sess, tzname, type, rqid, uuid, host,
                             is_reorder_on);
}

/**
 * Same as osql_sess_create, but sql is already allocated
 *
 */
osql_sess_t *osql_sess_create_socket(const char *sql, char *tzname, int type,
                                     unsigned long long rqid, uuid_t uuid,
                                     const char *host, int is_reorder_on)
{
    osql_sess_t *sess = NULL;

    /* alloc object */
    sess = (osql_sess_t *)calloc(sizeof(osql_sess_t) + sizeof(sess_impl_t), 1);
    if (!sess) {
        logmsg(LOGMSG_ERROR, "%s:unable to allocate %zu bytes\n", __func__,
               sizeof(*sess));
        return NULL;
    }
    sess->impl = (sess_impl_t *)(sess + 1);
    sess->sql = sql;
    sess->impl->embedded_sql = 0;
    sess->impl->socket = 1;

    return _osql_sess_create(sess, tzname, type, rqid, uuid, host,
                             is_reorder_on);
}


static inline int is_sess_from_sockbplog(osql_sess_t *sess)
{
    return !!sess->impl->socket;
}


/**
 * Terminates an in-use osql session (for which we could potentially
 * receive message from sql thread).
 * Returns 0 if success
 *
 * This function will remove from osql_repository_rem() if is_linked is set
 * and if did not come over sockbplog, then wait till there are no more clients
 * using this sess and only then destroy obj
 *
 * NOTE:
 * - it is possible to inline clean a request on master bounce,
 *   which starts by unlinking the session first, and freeing bplog afterwards
 *
 * - if caller has already removed sess from osql repository, they should
 *   call this function with is_linked = 0; THIS IS DONE IF THE CALLER OWNS
 *   THE REPO LOCK ALREADY, TO PREVENT REPO_REM FROM LOCKING
 *
 */
int osql_sess_close(osql_sess_t **psess, int is_linked)
{
    osql_sess_t *sess = *psess;

    if (is_linked && !is_sess_from_sockbplog(sess)) {
        /* unlink the request so no more messages are received */
        int rc = osql_repository_rem(sess);
        if (rc) {
            logmsg(LOGMSG_USER,
                   "%s: %p failed to delete from repository, already cleanedup ex. osql_sess_try_terminate\n",
                   __func__, sess);
            return rc;
        }
    }

    while (ATOMIC_LOAD32(sess->impl->clients) > 0) {
        poll(NULL, 0, 10);
    }

    if (sess->tran)
        osql_bplog_close(&sess->tran);

    _destroy_session(psess);

    return 0;
}

static void _destroy_session(osql_sess_t **psess)
{
    osql_sess_t *sess = *psess;

    if (sess->snap_info) {
        free(sess->snap_info);
    }

    Pthread_mutex_destroy(&sess->impl->mtx);
    if (!sess->impl->embedded_sql)
        free((char *)sess->sql);
#ifndef NDEBUG
      memset(sess, 0xdb, sizeof(osql_sess_t) + sizeof(sess_impl_t));
#endif
    free(sess); // sess->impl is freed thru this free

    *psess = NULL;
}

/**
 * Mark that the reader thread is working on this session
 *
 * Return error if session is dispatched; it is silently
 * ignored in implementations when redundant packets can
 * arrive
 */
int osql_sess_addclient(osql_sess_t *psess)
{
    sess_impl_t *sess = psess->impl;
    int rc = 0;

    Pthread_mutex_lock(&sess->mtx);
    if (sess->dispatched) {
        rc = -1;
    } else
        sess->clients += 1;
    Pthread_mutex_unlock(&sess->mtx);

    return rc;
}

/**
 * The reader_thread is done with updating the bplog
 * Return:
 *   0 if no emergency action needs to be taken
 *   1 if the session is marked terminated / caller might have to free
 *     the session
 *
 * NOTE: CALLING THIS UNDER REPOSITORY LOCK
 *
 * Since no termination can race (because of the lock), we can
 * check if the session is terminated.  If the session is terminated
 * the terminating thread skipped the session (since reader was working
 * on it).  The reader thread needs to free this session
 * If the session is not terminated, it can be dispatched if all bplog
 * was received, so we lit the flag (this will prevent any terminating
 * thread from touching it).
 *
 */
int osql_sess_remclient(osql_sess_t *psess)
{
    sess_impl_t *sess = psess->impl;
    int rc = 0;

    Pthread_mutex_lock(&sess->mtx);
    assert(sess->clients > 0);
    sess->clients -= 1;
    if (sess->terminate) {
        rc = 1;
    }
    Pthread_mutex_unlock(&sess->mtx);

    return rc;
}

/**
 * Return malloc-ed string:
 * sess_type rqid uuid local/remote host
 *
 */
char *osql_sess_info(osql_sess_t *sess)
{
    uuidstr_t us;
    char *ret = malloc(OSQL_SESS_INFO_LEN);

    if (ret) {
        snprintf(ret, OSQL_SESS_INFO_LEN, "%s, %llx %s %s%s",
                 osql_sorese_type_to_str(sess->type), sess->rqid,
                 comdb2uuidstr(sess->uuid, us),
                 sess->target.host == gbl_myhostname ? "REMOTE " : "LOCAL ",
                 sess->target.host);
    }
    return ret;
}

/**
 * Log query to the reqlog
 */
void osql_sess_reqlogquery(osql_sess_t *sess, struct reqlogger *reqlog)
{
    char *info = osql_sess_info(sess);
    reqlog_logf(
        reqlog, REQL_INFO,
        "%s time %dms queuetime=%dms \"%s\"\n",
        (info) ? info : "unknown", U2M(sess->sess_endus - sess->sess_startus),
        U2M(reqlog_get_queue_time(reqlog)), sess->sql ? sess->sql : "()");
    if (info)
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
    int rc = 0;
    int is_msg_done = 0;
    struct errstat *perr = NULL;

    /* get the session; dispatched sessions are ignored */
    osql_sess_t *sess = osql_repository_get(rqid, uuid);
    if (!sess) {
        /* in the current implementation we tolerate redundant ops from session
         * that have been already terminated--discard the packet in that case */
        *found = 0;
        return 0;
    }

    is_msg_done =
        osql_comm_is_done(sess, type, data, datalen, rqid == OSQL_RQID_USE_UUID,
                          &perr, NULL) != 0;

    /* we have received an OSQL_XERR; replicant wants to abort the transaction;
       discard the session and be done */
    if (is_msg_done && perr) {
        if (debug_switch_test_sync_osql_cancel()) {
            /* We need a scenario where requests arrive faster than they're
               cancelled. This usually means the master is a lot busier than its
               replicants. It's hard to archieve in our docker environment, for
               all instances share resources of the physical host. Let's just
               add an artificial sleep here. */
            poll(NULL, 0, 1000);
        }
        goto failed_stream;
    }

    *found = 1;

    /* save op */
    rc = osql_bplog_saveop(sess, sess->tran, data, datalen, type);
    if (rc) {
        /* failed to save into bplog; discard and be done */
        goto failed_stream;
    }

    /* release the session */
    if (!is_msg_done) {
        rc = osql_repository_put(sess);
        if (rc == 1) {
            /* session was marked terminated and not finished*/
            osql_sess_close(&sess, 1);
        }
        return 0;
    }

    /* IT WAS A DONE MESSAGE
       HERE IS THE DISPATCH */
    return handle_buf_sorese(sess);

failed_stream:
    if (is_msg_done && perr)
        osql_comm_signal_sqlthr_rc(&sess->target, rqid, uuid, 0, &sess->xerr, NULL, 0);

    /* release the session */
    osql_repository_put(sess);

    logmsg(LOGMSG_DEBUG, "%s: cancelled transaction\n", __func__);
    osql_sess_close(&sess, 1);

    return rc;
}

extern int gbl_sockbplog_debug;

/**
 * Same as osql_sess_rcvop, for socket protocol
 *
 */
int osql_sess_rcvop_socket(osql_sess_t *sess, int type, void *data, int datalen,
                           int *is_msg_done)
{
    int rc = 0;
    struct errstat *perr = NULL;

    *is_msg_done =
        osql_comm_is_done(sess, type, data, datalen, 1, &perr, NULL) != 0;

    /* we have received an OSQL_XERR; replicant wants to abort the transaction;
       discard the session and be done */
    if (*is_msg_done && perr) {
        if (debug_switch_test_sync_osql_cancel())
            poll(NULL, 0, 1000);
        osql_comm_signal_sqlthr_rc(&sess->target, sess->rqid, sess->uuid, 0, &sess->xerr, NULL, 0);
        sess->is_cancelled = 1;
        return 0;
    }

    /* save op */
    rc = osql_bplog_saveop(sess, sess->tran, data, datalen, type);
    if (rc) {
        /* failed to save into bplog; discard and be done */
        return rc;
    }

    /* release the session */
    if (!*is_msg_done) {
        return 0;
    }

    if (gbl_sockbplog_debug)
        logmsg(LOGMSG_ERROR, "%p Dispatching transaction\n", (void *)pthread_self());
    /* IT WAS A DONE MESSAGE
       HERE IS THE DISPATCH */
    return handle_buf_sorese(sess);
}

int osql_sess_queryid(osql_sess_t *sess)
{
    return sess->queryid;
}

/**
 * Terminate a session if the session is not yet completed/dispatched
 * Return
 *    0 if session can be terminated by caller
 *    1 otherwise (if session was already dispatched)
 *
 * NOTE: this should be called under osql repository lock
 */
int osql_sess_try_terminate(osql_sess_t *psess, const char *host)
{
    sess_impl_t *sess = psess->impl;
    int keep_sess = 0;
    uuidstr_t us;

    if (host && host != psess->target.host)
        return 1;

    Pthread_mutex_lock(&sess->mtx);

    if (sess->dispatched) {
        keep_sess = 1;
        goto done;
    }

    sess->terminate = 1;

    /* NOTE: if there is at least a client, it will check the status
    before taking decrementing the client, and if "terminate" is lit
    it will free the session safely; otherwise, we have to free the
    session here, since there is no-one to free it afterwards */
    keep_sess = (sess->clients > 0);

done:
    logmsg(LOGMSG_INFO, "%s: rqid=%llx, uuid=%s %s\n", __func__, psess->rqid,
           comdb2uuidstr(psess->uuid, us),
           keep_sess ? "was already dispatched" : "can be cancelled");
    Pthread_mutex_unlock(&sess->mtx);

    return keep_sess;
}

/**
 * Dispatch the query
 * At this point the session has one client (this running reader_thread)
 * and it is unlocked; it cannot be closed.
 * Any thread trying to close this session will wait for client counter to
 * go to zero. Here we are locking the session and mark it dispatched,
 * before decrementing the client counter.  This way, any thread trying to close
 * it will get out of the busy loop, but skip the session because it is marked
 * dispatched
 *
 * Note:
 *   If the sesssion dispatch fails (queue full?), we need to send back retry
 *   error code to the source replicant
 *
 */
static int handle_buf_sorese(osql_sess_t *psess)
{
    sess_impl_t *sess = psess->impl;
    int debug;
    int rc = 0;
    uint8_t *p_buf = NULL;
    const uint8_t *p_buf_end = NULL;

    debug = debug_this_request(gbl_debug_until);
    if (gbl_who > 0 && gbl_debug) {
        debug = 1;
    }

    Pthread_mutex_lock(&sess->mtx);
    /* NOTE: the session here has one client at least, so it will not be
    close; it might be terminanted but we allow to dispatch */
    sess->dispatched = 1;
    psess->sess_endus = comdb2_time_epochus();
    bzero(&psess->xerr, sizeof(psess->xerr));
    Pthread_mutex_unlock(&sess->mtx);

    /* session marked dispatched, any potential closer waiting on the client
    counter to go to zero will skip it;  we need to decrement client counter
    here so that block processor can close the session */

    osql_repository_put(psess);

    /* create the buffer now */
    /* construct a block transaction */
    if (osql_bplog_build_sorese_req(&p_buf, &p_buf_end, psess->sql,
                                    strlen(psess->sql) + 1, psess->tzname,
                                    psess->type, psess->rqid, psess->uuid)) {
        logmsg(LOGMSG_ERROR, "bug in code %s:%d", __func__, __LINE__);
        return rc;
    }

    rc = handle_buf_main(thedb, NULL, p_buf, p_buf_end, debug,
                         (char *)psess->target.host, 0, NULL, psess,
                         REQ_OFFLOAD, NULL, 0, 0);

    if (rc) {
        signal_replicant_error(&psess->target, psess->rqid, psess->uuid,
                               ERR_NOMASTER, "failed tp dispatch, queue full");
        osql_sess_close(&psess, 1);
    }
    return rc;
}

static osql_sess_t *_osql_sess_create(osql_sess_t *sess, char *tzname, int type,
                                      unsigned long long rqid, uuid_t uuid,
                                      const char *host, int is_reorder_on)
{
#ifdef TEST_QSQL_REQ
    uuidstr_t us;
    logmsg(LOGMSG_INFO, "%s: Opening request %llu %s\n", __func__, rqid,
           comdb2uuidstr(uuid, us));
#endif

#if DEBUG_REORDER
    uuidstr_t us;
    comdb2uuidstr(uuid, us);
    logmsg(LOGMSG_DEBUG, "%s:processing sql=%s sess=%p, uuid=%s\n", __func__,
           sql, sess, us);
#endif

    /* init sync fields */
    Pthread_mutex_init(&sess->impl->mtx, NULL);

    sess->rqid = rqid;
    comdb2uuidcpy(sess->uuid, uuid);
    sess->type = type;
    sess->target.host = intern(host);
    sess->sess_startus = comdb2_time_epochus();
    // hi! when using bit-fields make sure assigned value is not out of range
    sess->is_reorder_on = !!is_reorder_on; // Convert non-zero -> 1
    if (tzname)
        strncpy0(sess->tzname, tzname, sizeof(sess->tzname));

    sess->impl->clients = 1;
    /* defaults to net */
    init_bplog_net(&sess->target);

    /* create bplog so we can collect ops from sql thread */
    sess->tran = osql_bplog_create(sess->rqid == OSQL_RQID_USE_UUID,
                                   sess->is_reorder_on);
    if (!sess->tran) {
        logmsg(LOGMSG_ERROR, "%s Unable to create new bplog\n", __func__);
        _destroy_session(&sess);
    }

    return sess;
}
