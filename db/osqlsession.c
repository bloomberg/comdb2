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

#include <uuid/uuid.h>

int gbl_osql_sess_max_retries =
    10; /* set how many times we are willing to retry a session */

static int osql_poke_replicant(osql_sess_t *sess);
static void _destroy_session(osql_sess_t **prq, int phase);
static int clear_messages(osql_sess_t *sess);

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
int osql_close_session(struct ireq *iq, osql_sess_t **psess, int is_linked, const char *func, const char *callfunc, int line)
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
        pthread_mutex_lock(&sess->clients_mtx);
        while (sess->clients > 0) {
            pthread_mutex_unlock(&sess->clients_mtx);
            poll(NULL, 0, 10);
            pthread_mutex_lock(&sess->clients_mtx);
        }
        pthread_mutex_unlock(&sess->clients_mtx);

        _destroy_session(psess, 0);
    }

    return rc;
}

static void _destroy_session(osql_sess_t **prq, int phase)
{

    osql_sess_t *rq = *prq;
    uuidstr_t us;

    free_blob_buffers(rq->blobs, MAXBLOBS);
    switch (phase) {
    case 0:
#ifdef TEST_OSQL
        fprintf(stderr, "[%llu %s] FREEING QUEUE\n", rq->rqid,
                comdb2uuidstr(rq->uuid, us));
#endif
        if (rq->req)
            free(rq->req);

        /* queue might not be empty; be nice and free its objects */
        {
            int cleared = clear_messages(rq);
            if (cleared && debug_switch_osql_verbose_clear())
                fprintf(stderr, "%llu %s cleared %d messages\n", rq->rqid,
                        comdb2uuidstr(rq->uuid, us), cleared);
        }

        queue_free(rq->que);
    case 1:
        pthread_cond_destroy(&rq->cond);
    case 2:
        pthread_mutex_destroy(&rq->mtx);
    case 3:
        pthread_mutex_destroy(&rq->clients_mtx);
    case 4:
        pthread_mutex_destroy(&rq->completed_lock);
    case 5:
        free(rq);
    }

    *prq = NULL;
}

static int clear_messages(osql_sess_t *sess)
{

    char *tmp = NULL;
    int cnt = 0;

    while ((tmp = queue_next(sess->que)) != NULL) {
        free(tmp);
        cnt++;
    }

    return cnt;
}

/**
 * Get the cached sql request
 *
 */
osql_req_t *osql_session_getreq(osql_sess_t *sess)
{

    return (osql_req_t *)sess->req;
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
    int rc = 0;

    if ((rc = pthread_mutex_lock(&sess->clients_mtx)) != 0) {
        fprintf(stderr, "%s: pthread_mutex_lock failed rc = %d\n", __func__,
                rc);
        abort();
        return -1;
    }
#if 0
   uuidstr_t us;
   comdb2uuidstr(sess->uuid, us);
   fprintf(stderr, "\t\tADDCLNT p_sees=%p rqid=[%llx %s] sess->completed=%llx thread=%d clients=%d p_sess->iq=%p\n",
         sess, sess->rqid, us, sess->completed, pthread_self(), sess->clients+1, sess->iq);
#endif

    sess->clients++;

    if ((rc = pthread_mutex_unlock(&sess->clients_mtx)) != 0) {
        fprintf(stderr, "%s: pthread_mutex_unlock failed rc = %d\n", __func__,
                rc);
        return -1;
    }

    return 0;
}

/**
 * Register client
 * Prevent temporary the session destruction
 *
 */
int osql_sess_remclient(osql_sess_t *sess)
{

    int rc = 0;

    if ((rc = pthread_mutex_lock(&sess->clients_mtx)) != 0) {
        fprintf(stderr, "%s: pthread_mutex_lock failed rc = %d\n", __func__,
                rc);
        // this is happening for me
        abort();
        return -1;
    }

#if 0
   uuidstr_t us;
   comdb2uuidstr(sess->uuid, us);
   fprintf(stderr, "\t\tREMCLNT p_sees=%p rqid=%llx uuid=%s sess->completed=%llx thread=%d clients=%d p_sess->iq=%p\n",
         sess, sess->rqid, us, sess->completed, pthread_self(), sess->clients-1, sess->iq);
#endif

    sess->clients--;

    if (sess->clients < 0) {
        uuidstr_t us;
        fprintf(stderr,
                "%s: BUG ALERT, session %llu %s freed one too many times\n",
                __func__, sess->rqid, comdb2uuidstr(sess->uuid, us));
    }

    if ((rc = pthread_mutex_unlock(&sess->clients_mtx)) != 0) {
        fprintf(stderr, "%s: pthread_mutex_unlock failed rc = %d\n", __func__,
                rc);
        return -1;
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
           (sess->offhost) ? "REMOTE" : "LOCAL",
           sess->offhost ? sess->offhost : "localhost");

    return 0;
}

/**
 * Registers the destination for osql session "sess"
 *
 */
void osql_sess_bindreq(osql_sess_t *sess, char *host) { sess->offhost = host; }

/**
 * Mark session duration and reported result.
 *
 */
int osql_sess_set_complete(unsigned long long rqid, uuid_t uuid,
                           osql_sess_t *sess, struct errstat *xerr)
{

    pthread_mutex_lock(&sess->completed_lock);

    if (sess->rqid != rqid || comdb2uuidcmp(uuid, sess->uuid)) {
        pthread_mutex_unlock(&sess->completed_lock);
        return 0;
    }

    if (sess->completed != 0) {
        pthread_mutex_unlock(&sess->completed_lock);
        return 0;
    }

    sess->end = time(NULL);

    if (xerr) {
        uint8_t *p_buf = (uint8_t *)xerr;
        uint8_t *p_buf_end = (p_buf + sizeof(struct errstat));
        osqlcomm_errstat_type_get(&sess->xerr, p_buf, p_buf_end);
    } else {
        bzero(&sess->xerr, sizeof(sess->xerr));
    }

    sess->completed = rqid;
    comdb2uuidcpy(sess->completed_uuid, uuid);
    if (sess->terminate != OSQL_TERMINATE) {
        osql_bplog_session_is_done(sess->iq);
    }
    pthread_mutex_unlock(&sess->completed_lock);

    return 0;
}

/**
 * Returns "true" if code is
 *   - SQLITE_DEADLOCK
 *   - SQLITE_TOOEARLY
 */
static int is_session_repeatable(int code)
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
 * Checks if a session is complete;
 * Returns:
 * - SESS_DONE_OK, if the session completed successfully
 * - SESS_DONE_ERROR_REPEATABLE, if the session is completed
 *   but finished with an error that allows repeating the request
 * - SESS_DONE_ERROR, if the session completed with an unrecoverable error
 * - SESS_PENDING, otherwise
 *
 * xerr is set to point to session errstat so that blockproc can retrieve
 * individual session error, if any.
 *
 *
 */
int osql_sess_test_complete(osql_sess_t *sess, struct errstat **xerr)
{
    int rc = SESS_PENDING;

    pthread_mutex_lock(&sess->completed_lock);

    if (sess->completed) {

        /* Lost the race against the retry code.  Just retry again. */
        if (sess->completed != sess->rqid ||
            comdb2uuidcmp(sess->completed_uuid, sess->uuid) != 0) {
            rc = SESS_DONE_ERROR_REPEATABLE;
        }

        else if (sess->xerr.errval) {
            int errval;
            *xerr = &sess->xerr;

            errval = sess->xerr.errval;

            rc = is_session_repeatable(errval) ? SESS_DONE_ERROR_REPEATABLE
                                               : SESS_DONE_ERROR;
        } else {
#if 0
         uuidstr_t us;
         printf("Recv DONE rqid=%llu %s tmp=%llu\n", sess->rqid, comdb2uuidstr(sess->uuid, us), osql_log_time());
#endif
            *xerr = NULL;
            rc = SESS_DONE_OK;
        }
    } else {
        if (sess->terminate)
            rc = SESS_DONE_ERROR_REPEATABLE;
    }

    pthread_mutex_unlock(&sess->completed_lock);

    return rc;
}

/**
 * Check if there was a delay in receiving rows from
 * replicant, and if so, poke the sql session to detect
 * if this is still in progress
 *
 */
int osql_sess_test_slow(blocksql_tran_t *tran, osql_sess_t *sess)
{
    int rc = 0;
    time_t crttime = time(NULL);

    /* check if too much time has passed and poke the request otherwise */
    if (crttime - sess->last_row > gbl_osql_blockproc_timeout_sec) {
        rc = osql_poke_replicant(sess);
        if (rc) {
            uuidstr_t us;
            fprintf(stderr,
                    "%s: session %llx %s lost its offloading node on %s\n",
                    __func__, (unsigned long long)sess->rqid,
                    comdb2uuidstr(sess->uuid, us), sess->offhost);
            sess->terminate = OSQL_TERMINATE;

            /* wake up the block processor */
            rc = osql_bplog_signal(tran);

            if (bdb_lock_desired(thedb->bdb_env))
                return ERR_NOMASTER;

            return rc;
        }
    }

    return 0;
}

/**
 * Returns
 * - total time (tottm)
 * - last roundtrip time (rtt)
 * - retries (rtrs)
 *
 */
void osql_sess_getsummary(osql_sess_t *sess, int *tottm, int *rtt, int *rtrs)
{
    *tottm = sess->end - sess->initstart;
    *rtt = sess->end - sess->start;
    *rtrs = sess->retries;
}

/**
 * Log query to the reqlog
 */
void osql_sess_reqlogquery(osql_sess_t *sess, struct reqlogger *reqlog)
{
    reqlog_logf(reqlog, REQL_INFO, "rqid %llx node %s sec %u rtrs %u \"%s\"\n",
                sess->rqid, sess->offhost, sess->end - sess->initstart,
                sess->retries, (sess->sql) ? sess->sql : "()");
}

/**
 * Returns associated blockproc transaction
 * Only used for saveop, so return NULL if it's completed or terminated.
 */
void *osql_sess_getbptran(osql_sess_t *sess)
{
    void *bsql = NULL;

    pthread_mutex_lock(&sess->mtx);
    if (sess->iq && !sess->completed && !sess->terminate) {
        bsql = sess->iq->blocksql_tran;
    }
    pthread_mutex_unlock(&sess->mtx);
    return bsql;
}

int osql_sess_lock(osql_sess_t *sess) { return pthread_mutex_lock(&sess->mtx); }

int osql_sess_unlock(osql_sess_t *sess)
{
    return pthread_mutex_unlock(&sess->mtx);
}

int osql_sess_is_terminated(osql_sess_t *sess) { return sess->terminate; }

void osql_sess_set_dispatched(osql_sess_t *sess, int dispatched)
{
    sess->dispatched = dispatched;
}

int osql_sess_dispatched(osql_sess_t *sess) { return sess->dispatched; }

int osql_sess_lock_complete(osql_sess_t *sess)
{
    return pthread_mutex_lock(&sess->completed_lock);
}

int osql_sess_unlock_complete(osql_sess_t *sess)
{
    return pthread_mutex_unlock(&sess->completed_lock);
}

/**
 * Handles a new op received for session "rqid"
 * It saves the packet in the local bplog
 * Return 0 if success
 * Set found if the session is found or not
 *
 */
int osql_sess_rcvop(unsigned long long rqid, uuid_t uuid, void *data,
                    int datalen, int *found)
{
    osql_sess_t *sess = NULL;
    int rc = 0;
    int rc_out = 0;
    int is_sorese = 0;
    int is_msg_done = 0;
    struct errstat *perr;

    /* NOTE: before retrieving a session, we have to figure out if this is a
       sorese completion and lock the repository until the session is dispatched
       This prevents the race against signal_rtoff forcefully cleanup */
    is_msg_done =
        osql_comm_is_done(data, datalen, rqid == OSQL_RQID_USE_UUID, &perr, NULL);

    /* get the session */
    sess = osql_repository_get(rqid, uuid, is_msg_done);

    if (sess) {
        is_sorese = osql_session_is_sorese(sess);

        if (is_msg_done && perr && htonl(perr->errval) == SQLITE_ABORT &&
            !bdb_attr_get(thedb->bdb_attr,
                          BDB_ATTR_DISABLE_SELECTVONLY_TRAN_NOP)) {
            /* release the session */
            if ((rc = osql_repository_put(sess, is_msg_done)) != 0) {
                fprintf(stderr, "%s: rc =%d\n", __func__, rc);
            }

            /* sqlite aborted the transaction, skip all the work here
               master not needed */
            rc = sql_cancelled_transaction(sess->iq);
            if (rc) {
                fprintf(stderr, "%s: failed cancelling transaction! rc %d\n",
                        __func__, rc);
            }

            /* done here */
            return rc;
        }

        *found = 1;

        pthread_mutex_lock(&sess->completed_lock);
        /* ignore new coming osql packages */
        if (sess->completed || sess->dispatched || sess->terminate) {
            uuidstr_t us;
            pthread_mutex_unlock(&sess->completed_lock);
            if ((rc = osql_repository_put(sess, is_msg_done)) != 0) {
                logmsg(LOGMSG_ERROR,
                       "%s:%d osql_repository_put failed with rc %d\n",
                       __func__, __LINE__, rc);
            }
            comdb2uuidstr(uuid, us);
            logmsg(
                LOGMSG_INFO,
                "%s: rqid=%llx, uuid=%s is already done, ignoring packages\n",
                __func__, rqid, us);
            return 0;
        }
        pthread_mutex_unlock(&sess->completed_lock);

        /* save op */
        rc_out = osql_bplog_saveop(sess, data, datalen, rqid, uuid, sess->seq,
                                   sess->offhost);

        /* if rc_out, sess is FREED! */
        if (!rc_out) {
            /* Must increment seq under completed_lock */
            pthread_mutex_lock(&sess->completed_lock);
            uuidstr_t sessuuid, requuid;

            if (sess->rqid == rqid || (rqid == OSQL_RQID_USE_UUID &&
                                       comdb2uuidcmp(sess->uuid, uuid) == 0)) {
                sess->seq++;
                sess->last_row = time(NULL);
            }

            pthread_mutex_unlock(&sess->completed_lock);
        }

        /* release the session */
        if ((rc = osql_repository_put(sess, is_msg_done)) != 0) {
            fprintf(stderr, "%s: rc =%d\n", __func__, rc);
        }

        if (is_sorese && rc_out) {
            /*exit here*/
            return rc_out;
        }

    } else {
        uuidstr_t us;
        comdb2uuidstr(uuid, us);
        printf("discarding packet for %llx %s, session not found\n", rqid, us);

        /* in the current implementation we tolerate
           redundant ops from session that have been
           terminated in the meantime;
           we simply discard the packet in such a case
         */
        *found = 0;

        /* we used to free data here, but some callers like net_osql_rpl_tail()
         * (and probably all others expect to manage it themselves */

        rc = 0; /* redundant */
    }

    if (rc || rc_out) {
        /* something is wrong with the session, terminate it*/
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

    char *node = arg;
    osql_sess_t *sess = (osql_sess_t *)obj;
    int rc = 0;
    int need_clean = 0;
    int completed = 0;

    if (!node || sess->offhost == node) {

        if ((rc = pthread_mutex_lock(&sess->mtx)) != 0) {
            fprintf(stderr, "pthread_mutex_lock: error code %d\n", rc);
            return rc;
        }

        pthread_mutex_lock(&sess->completed_lock);
        sess->terminate = OSQL_TERMINATE;
        if (!sess->completed) {
            if (sess->iq)
                osql_bplog_session_is_done(sess->iq);

            /* NOTE: here we have to do a bit more;
               if this is a sorese transaction, transaction
               has not received done yet, chances are
               transaction will never finish, so we need
               to abort it here otherwise we leak it (including
               the temp table*/
            need_clean = osql_session_is_sorese(sess);
        }
        pthread_mutex_unlock(&sess->completed_lock);

        /* this request might be waiting to be dispatched */
        if (sess->iq)
            osql_bplog_signal(sess->iq->blocksql_tran);

        /* wake up the block processor waiting for this request */

        if ((rc = pthread_mutex_unlock(&sess->mtx)) != 0) {
            fprintf(stderr, "pthread_mutex_unlock: error code %d\n", rc);
            return rc;
        }
    }

    if (need_clean) {
        /* step 1) make sure no reader thread finds the session again */
        rc = osql_repository_rem(sess, 0, __func__, NULL, 0); /* already have exclusive lock */
        if (rc) {
            fprintf(stderr,
                    "%s: failed to remove session from repository rc=%d\n",
                    __func__, rc);
        }

        /* step 2) wait for current reader threads to go away */
        pthread_mutex_lock(&sess->clients_mtx);
        while (sess->clients > 0) {
            pthread_mutex_unlock(&sess->clients_mtx);
            poll(NULL, 0, 10);
            pthread_mutex_lock(&sess->clients_mtx);
        }
        pthread_mutex_unlock(&sess->clients_mtx);
        /* NOTE: at this point there will be no other bplog updates coming from
           this
           sorese session; the session might still be worked on; if that is the
           case
           the session is marked already complete, since this is done by reader
           thread
           which bumps up clients! */

        /* step 3) check if this is complete; if it is, it will/is being
           dispatched
                   if not complete, we need to clear it right now */
        pthread_mutex_lock(&sess->completed_lock);
        completed = sess->completed | sess->dispatched;
        pthread_mutex_unlock(&sess->completed_lock);

        if (!completed) {
#if 0
         fprintf(stderr, "%s: calling bplog_free to release rqid=%llx sess=%p iq=%p\n",
            __func__, sess->rqid, sess, sess->iq);
#endif

            /* no one will work on this; need to clear it */
            rc = osql_bplog_free(sess->iq, 0, __func__, NULL, 0);
            /* NOTE: sess is clear here! */
            if (rc) {
                fprintf(stderr, "%s: error in bplog_free rc=%d\n", __func__,
                        rc);
            }
        }
    }
    return 0;
}

static int osql_poke_replicant(osql_sess_t *sess)
{

    int rc = 0;
    uuidstr_t us;

    ctrace("Poking %s from %s for rqid %llx %s\n", sess->offhost, gbl_mynode,
           sess->rqid, comdb2uuidstr(sess->uuid, us));

    if (sess->offhost) {

        rc = osql_comm_send_poke(sess->offhost, sess->rqid, sess->uuid,
                                 NET_OSQL_POKE);

        return rc;
    }

    /* checkup local listings */
    if ((rc = osql_chkboard_sqlsession_exists(sess->rqid, sess->uuid, 1)) ==
        0) {

        if (!sess->xerr.errval) {
            /* ideally this should never happen, i.e.
               a local request should be either dispatch
               successfully or reported as failure, not disappear
               JIC, here we mark it MIA
             */
            uuidstr_t us;
            sess->xerr.errval = OSQL_NOOSQLTHR;
            snprintf(sess->xerr.errstr, sizeof(sess->xerr.errstr),
                     "Missing sql session %llx %s in local mode", sess->rqid,
                     comdb2uuidstr(sess->uuid, us));
            rc = -1;
        }

        /* Decrement throttle for retry */
        osql_bplog_session_is_done(sess->iq);

    } else if (rc == 1) {
        rc = 0; /* session exists */
    }

    return rc;
}

/**
 * Registers the destination for osql session "sess"
 *
 */
void osql_sess_setnode(osql_sess_t *sess, char *host) { sess->offhost = host; }

/**
 * Get the cached sql request
 *
 */
osql_req_t *osql_sess_getreq(osql_sess_t *sess) { return sess->req; }

/**
 * Creates an sock osql session and add it to the repository
 * Returns created object if success, NULL otherwise
 *
 */
osql_sess_t *osql_sess_create_sock(const char *sql, int sqlen, char *tzname,
                                   int type, unsigned long long rqid,
                                   uuid_t uuid, char *fromhost, struct ireq *iq,
                                   int *replaced)
{

    osql_sess_t *sess = NULL;
    int rc = 0;
    uuidstr_t us;

#ifdef TEST_QSQL_REQ
    fprintf(stdout, "%s: Opening request %llu %s\n", __func__, rqid,
            comdb2uuidstr(uuid, us));
#endif

    /* alloc object */
    sess = (osql_sess_t *)calloc(sizeof(*sess), 1);
    if (!sess) {
        fprintf(stderr, "%s:unable to allocate %zu bytes\n", __func__,
                sizeof(*sess));
        return NULL;
    }

    /* init sync fields */
    rc = pthread_mutex_init(&sess->clients_mtx, NULL);
    if (rc) {
        _destroy_session(&sess, 4);
        return NULL;
    }
    rc = pthread_mutex_init(&sess->mtx, NULL);
    if (rc) {
        _destroy_session(&sess, 3);
        return NULL;
    }
    rc = pthread_cond_init(&sess->cond, NULL);
    if (rc) {
        _destroy_session(&sess, 2);
        return NULL;
    }

    /* init queue of messages */
    sess->que = queue_new();
    if (!sess->que) {
        _destroy_session(&sess, 1);
        return NULL;
    }

    sess->rqid = rqid;
    comdb2uuidcpy(sess->uuid, uuid);
    sess->req = NULL;
    sess->reqlen = 0;
    save_sql(iq, sess, sql, sqlen);
    sess->type = type;
    sess->offhost = fromhost;
    sess->start = sess->initstart = time(NULL);

    if (tzname)
        strncpy(sess->tzname, tzname, sizeof(sess->tzname));

    sess->iq = iq;
    sess->clients = 1;

    /* how about we start the bplog before making this available to the world?
     */
    rc = osql_bplog_start(iq, sess);
    if (rc)
        goto late_error;

    rc = osql_repository_add(sess, replaced);
    if (rc || *replaced)
        goto late_error;

    sess->last_row = time(NULL);

    return sess;

late_error:
    /* notification of failure to sql thread is handled by caller */
    _destroy_session(&sess, 0);
    return NULL;
}

char *osql_sess_tag(osql_sess_t *sess) { return sess->tag; }

void *osql_sess_tagbuf(osql_sess_t *sess) { return sess->tagbuf; }

int osql_sess_tagbuf_len(osql_sess_t *sess) { return sess->tagbuflen; }

void osql_sess_set_reqlen(osql_sess_t *sess, int len) { sess->reqlen = len; }

void osql_sess_get_blob_info(osql_sess_t *sess, blob_buffer_t **blobs,
                             int *nblobs)
{
    *blobs = sess->blobs;
    *nblobs = sess->numblobs;
}

int osql_sess_reqlen(osql_sess_t *sess) { return sess->reqlen; }

int osql_sess_type(osql_sess_t *sess) { return sess->type; }

int osql_sess_queryid(osql_sess_t *sess) { return sess->queryid; }

void osql_sess_getuuid(osql_sess_t *sess, uuid_t uuid)
{
    comdb2uuidcpy(uuid, sess->uuid);
}

#if 0
/**
 * Needed for socksql and bro-s, which creates sessions before
 * iq->bplogs.
 * If we fail to dispatch to a blockprocession thread, we need this function
 * to clear the session from repository and free that leaked memory
 *
 * NOTE: this is basically called from net:reader_thread callback
 * so if there are any rows coming for the session, they will not
 * be read from the socket buffers until this ends 
 *
 */
void osql_sess_clear_on_error(struct ireq *iq, unsigned long long rqid, uuid_t uuid) {

   osql_sess_t    *sess = NULL;
   int            rc = 0;
   
   /* get the session */
   sess = osql_repository_get(rqid, uuid, 0);

   if(sess)
   {
      if(rc=osql_repository_put(sess, 0))
      {
         fprintf(stderr, "%s: rc =%d\n", 
               __func__, rc);
      }

      if(rc=osql_close_session(iq, &sess, 1))
      {
         fprintf(stderr, "%s: rc =%d\n", 
               __func__, rc);
      }
   }
}
#endif

inline int osql_session_is_sorese(osql_sess_t *sess)
{
    return (sess->type == OSQL_RECOM_REQ || sess->type == OSQL_SOCK_REQ_COST ||
            sess->type == OSQL_SOCK_REQ || sess->type == OSQL_SNAPISOL_REQ ||
            sess->type == OSQL_SERIAL_REQ);
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
    if (sess->iq) {
        osql_bplog_session_is_done(sess->iq);
        osql_bplog_signal(sess->iq->blocksql_tran);
    }
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
    rc = osql_bplog_free(sess->iq, 0, __func__, NULL, __LINE__);
    /* NOTE: sess is clear here! */
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: error in bplog_free rc=%d\n", __func__, rc);
    }

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
    int completed = 0;
    if (rc = osql_sess_lock(sess)) {
        logmsg(LOGMSG_ERROR, "%s:%d osql_sess_lock rc %d\n", __func__, __LINE__,
               rc);
        return -1;
    }
    if (rc = osql_sess_lock_complete(sess)) {
        logmsg(LOGMSG_ERROR, "%s:%d osql_sess_lock_complete rc %d\n", __func__,
               __LINE__, rc);
        osql_sess_unlock(sess);
        return -1;
    }
    completed = sess->completed | sess->dispatched;
    if (rc = osql_sess_unlock_complete(sess)) {
        logmsg(LOGMSG_ERROR, "%s:%d osql_sess_unlock_complete rc %d\n",
               __func__, __LINE__, rc);
        osql_sess_unlock(sess);
        return -1;
    }
    if (rc = osql_sess_unlock(sess)) {
        logmsg(LOGMSG_ERROR, "%s:%d osql_sess_unlock rc %d\n", __func__,
               __LINE__, rc);
        return -1;
    }
    if (completed) {
        /* request is being processed and this is a replay */
        return 1;
    } else {
        rc = osql_sess_set_terminate(sess);
        if (rc) {
            abort();
        }
    }
    return 0;
}
