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

/**
 *
 * Osql Interface with Block Processor
 *
 * Each block processor used for osql keeps a local log of operations (bplog)
 * of the current transaction. The operations are sent by the sqlthread.
 *
 * In blocksql mode, the bp opens an osql session for each query part of the
 *current
 * transaction.
 * In socksql/recom/snapisol/serial mode, the bp has only one session.
 *
 * Block processor waits until all pending sessions are completed.  If any of
 *the sessions
 * completes with an error that could be masked (like deadlocks), the block
 *processor
 * will re-issue it.
 *
 * If all the sessions complete successfully, the bp scans through the log and
 *applies all the
 * changes by calling record.c functions.
 *
 * At the end, the return code is provided to the client (in the case of
 *blocksql)
 * or the remote socksql/recom/snapisol/serial thread.
 *
 */
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <limits.h>
#include <strings.h>
#include <poll.h>
#include <str0.h>
#include <epochlib.h>
#include <unistd.h>
#include <plhash.h>
#include <assert.h>
#include <semaphore.h>

#include "comdb2.h"
#include "osqlblockproc.h"
#include "block_internal.h"
#include "osqlsession.h"
#include "osqlcomm.h"
#include "sqloffload.h"
#include "osqlrepository.h"
#include "comdb2uuid.h"
#include "bpfunc.h"

#include "logmsg.h"


int g_osql_blocksql_parallel_max = 5;
extern int gbl_blocksql_grace;

typedef struct blocksql_info {
    osql_sess_t *sess; /* pointer to the osql session */

    LINKC_T(struct blocksql_info)
        p_reqs; /* pending osql sessions linked in same transaction */
    LINKC_T(struct blocksql_info)
        c_reqs; /* completed osql sessions linked in same transaction */
} blocksql_info_t;

struct blocksql_tran {
    /* NOTE: we keep only block processor thread operating on this, so we don't
     * need a lock */
    LISTC_T(blocksql_info_t)
        pending; /* list of complete sessions for this block sql */
    LISTC_T(blocksql_info_t)
        complete; /* list of complete sessions for this block sql */

    pthread_mutex_t store_mtx; /* mutex for db access - those are non-env dbs */
    struct temp_table *db;     /* temp table that keeps the list of ops for all
                                  current sessions */

    pthread_mutex_t mtx; /* mutex and cond for notifying when any session
                           has completed */
    pthread_cond_t cond;
    int dowait; /* mark this when session completes to avoid loosing signal */

    int num; /* count how many sessions were started for this tran */

    sem_t throttle; /* parallel sessions in blocksql */
    int delayed;

    int rows;

    struct dbtable *last_db;
};

typedef struct oplog_key {
    unsigned long long rqid;
    uuid_t uuid;
    unsigned long long seq;
} oplog_key_t;

static int apply_changes(struct ireq *iq, blocksql_tran_t *tran, void *iq_tran,
                         int *nops, struct block_err *err, SBUF2 *logsb);
static int osql_bplog_wait(blocksql_tran_t *tran);
static int req2blockop(int reqtype);
static int osql_bplog_loginfo(struct ireq *iq, osql_sess_t *sess);

/**
 * The bplog key-compare function - required because memcmp changes
 * the order of temp_table_next on little-endian machines.
 */
static int osql_bplog_key_cmp(void *usermem, int key1len, const void *key1,
                              int key2len, const void *key2)
{
    assert(sizeof(oplog_key_t) == key1len);
    assert(sizeof(oplog_key_t) == key2len);

#ifdef _SUN_SOURCE
    oplog_key_t t1, t2;
    memcpy(&t1, key1, key1len);
    memcpy(&t2, key2, key2len);
    oplog_key_t *k1 = &t1;
    oplog_key_t *k2 = &t2;
#else
    oplog_key_t *k1 = (oplog_key_t *)key1;
    oplog_key_t *k2 = (oplog_key_t *)key2;
#endif
    int cmp;

    if (k1->rqid < k2->rqid) {
        return -1;
    }

    if (k1->rqid > k2->rqid) {
        return 1;
    }

    cmp = comdb2uuidcmp(k1->uuid, k2->uuid);
    if (cmp)
        return cmp;

    if (k1->seq < k2->seq) {
        return -1;
    }

    if (k1->seq > k2->seq) {
        return 1;
    }

    return 0;
}

/**
 * Adds the current session to the bplog pending session list.
 * If there is no bplog created, it creates one.
 * Returns 0 if success.
 *
 */
int osql_bplog_start(struct ireq *iq, osql_sess_t *sess)
{

    blocksql_tran_t *tran = NULL;
    blocksql_info_t *info = NULL;
    int bdberr = 0;
    int rc;

    /*
       this stuff is LOCKLESS, since we have only block
       processor calling this

       if we need to have like "send ... thr" print these
       info, we'll need a lock around alloc/dealloc
     */

    info = (blocksql_info_t *)calloc(1, sizeof(blocksql_info_t));
    if (!info) {
        logmsg(LOGMSG_ERROR, "%s: error allocating %d bytes\n", __func__,
                sizeof(blocksql_info_t));
        return -1;
    }

    if (iq->blocksql_tran)
        abort();

    tran = calloc(sizeof(blocksql_tran_t), 1);
    if (!tran) {
        logmsg(LOGMSG_ERROR, "%s: error allocating %d bytes\n", __func__,
               sizeof(blocksql_tran_t));
        free(info);
        return -1;
    }

    pthread_mutex_init(&tran->store_mtx, NULL);
    pthread_mutex_init(&tran->mtx, NULL);
    pthread_cond_init(&tran->cond, NULL);

    rc = sem_init(&tran->throttle, 0, g_osql_blocksql_parallel_max);
    if (rc == -1) {
        logmsgperror("sem_init");
    }

    iq->blocksql_tran = tran; /* now blockproc knows about it */

    /* init the lists and the temporary table and cursor */
    listc_init(&tran->pending, offsetof(blocksql_info_t, p_reqs));
    listc_init(&tran->complete, offsetof(blocksql_info_t, c_reqs));

    tran->db = bdb_temp_table_create(thedb->bdb_env, &bdberr);
    if (!tran->db || bdberr) {
        logmsg(LOGMSG_ERROR, "%s: failed to create temp table bdberr=%d\n",
               __func__, bdberr);
        free(tran);
        free(info);
        return -1;
    }

    bdb_temp_table_set_cmp_func(tran->db, osql_bplog_key_cmp);

    tran->dowait = 1;

    iq->timings.req_received = osql_log_time();
    /*printf("Set req_received=%llu\n", iq->timings.req_received);*/

    tran->num++;

    info->sess = sess;
    listc_abl(&tran->pending, info);

    return 0;
}

/**
 *
 *
 */
int osql_bplog_finish_sql(struct ireq *iq, struct block_err *err)
{
    blocksql_tran_t *tran = (blocksql_tran_t *)iq->blocksql_tran;
    int error = 0;
    blocksql_info_t *info = NULL, *temp = NULL;
    struct errstat generr = {0}, *xerr;
    int rc = 0, irc = 0;
    int stop_time = 0;

    /* wait for pending sessions to finish;
       if any request has failed because of deadlock, repeat
     */
    while (tran->pending.top && !error) {

        /* go through the list of pending requests and if there are any
            that have not received responses over a certain window, poke them */
        /* if we find any complete lists, move them from pending to complete */
        /* if the session finished with deadlock on replicant, or failed
           session,
           resubmit it */
        LISTC_FOR_EACH_SAFE(&tran->pending, info, temp, p_reqs)
        {

            rc = osql_sess_test_complete(info->sess, &xerr);
            switch (rc) {
            case SESS_DONE_OK:
                listc_rfl(&tran->pending, info);
                listc_abl(&tran->complete, info);
                break;

            case SESS_DONE_ERROR_REPEATABLE:

                /* generate a new id for this session */
                if (iq->sorese.type) {
                    /* this is socksql, recom, snapisol or serial; no retry here
                     */
                    generr.errval = ERR_INTERNAL;
                    strncpy(generr.errstr, "master cancelled transaction",
                            sizeof(generr.errstr));
                    xerr = &generr;
                    error = 1;
                    break;
                }
                break;

            case SESS_DONE_ERROR:

                /* TOOBIG magic */
                if ((xerr->errval == ERR_TRAN_TOO_BIG || xerr->errval == 4)) {
                    irc = osql_bplog_loginfo(iq, info->sess);
                    if (irc) {
                        logmsg(LOGMSG_ERROR, "%s: failed to log the bplog rc=%d\n",
                                __func__, irc);
                    }
                }

                error = 1;
                break;

            case SESS_PENDING:
                rc = osql_sess_test_slow(tran, info->sess);
                if (rc)
                    return rc;
                break;
            }
            if (error)
                break;
        }

        /* please stop !!! */
        if (thedb->stopped || thedb->exiting) {
            if (stop_time == 0) {
                stop_time = time_epoch();
            } else {
                if (stop_time + gbl_blocksql_grace /*seconds grace time*/ <=
                    time_epoch()) {
                    logmsg(LOGMSG_ERROR, 
                            "blocksql session closing early, db stopped\n");
                    return ERR_NOMASTER;
                }
            }
        }

        if (!tran->pending.top)
            break;

        if (bdb_lock_desired(thedb->bdb_env)) {
            logmsg(LOGMSG_ERROR, "%d %s:%d blocksql session closing early\n",
                    pthread_self(), __FILE__, __LINE__);
            err->blockop_num = 0;
            err->errcode = ERR_NOMASTER;
            err->ixnum = 0;
            return ERR_NOMASTER;
        }

        /* this ensure we poke frequently enough */
        if (!error && tran->pending.top) {
#if 0
         printf("Going to wait tmp=%llu\n", osql_log_time());
#endif

            rc = osql_bplog_wait(tran);
            if (rc)
                return rc;
        } else
            break; /* all completed */
    }              /* done with all requests, or unrepeatable error */

    iq->timings.req_alldone = osql_log_time();

    if (error) {
        iq->errstat.errval = xerr->errval;
        errstat_cat_str(&iq->errstat, errstat_get_str(xerr));
        return xerr->errval;
    }

    return 0;
}

/**
 * Wait for all pending osql sessions of this transaction to
 * finish
 * Once all finished ok, we apply all the changes
 */
int osql_bplog_commit(struct ireq *iq, void *iq_trans, int *nops,
                      struct block_err *err)
{
    blocksql_tran_t *tran = (blocksql_tran_t *)iq->blocksql_tran;
    int rc = 0;

    /* apply changes */
    rc = apply_changes(iq, tran, iq_trans, nops, err, iq->sorese.osqllog);

    iq->timings.req_applied = osql_log_time();

    return rc;
}

char *osql_sorese_type_to_str(int stype)
{
    char *rtn = "UNKNOWN";

    switch (stype) {
    case 0:
        rtn = "BLOCKSQL";
        break;
    case OSQL_SOCK_REQ:
        rtn = "SOCKSQL";
        break;
    case OSQL_RECOM_REQ:
        rtn = "RECOM";
        break;
    case OSQL_SNAPISOL_REQ:
        rtn = "SNAPISOL";
        break;
    case OSQL_SERIAL_REQ:
        rtn = "SERIAL";
        break;
    }

    return rtn;
}

/**
 * Prints summary for the current osql bp transaction
 *
 */
char *osql_get_tran_summary(struct ireq *iq)
{

    int i = 0;
    char *ret = NULL;
    char *nametype;

    /* format
       (BLOCKSQL OPS:[4] MIN:[8]msec MAX:[8]msec)
     */
    if (iq->blocksql_tran) {
        blocksql_tran_t *tran = (blocksql_tran_t *)iq->blocksql_tran;
        blocksql_info_t *info = NULL;

        int sz = 128;
        int min_rtt = INT_MAX;
        int max_rtt = 0;
        int min_tottm = INT_MAX;
        int max_tottm = 0;
        int min_rtrs = 0;
        int max_rtrs = 0;

        ret = (char *)malloc(sz);
        if (!ret) {
            logmsg(LOGMSG_ERROR, "%s: failed to allocated %d bytes\n", __func__, sz);
            return NULL;
        }

        LISTC_FOR_EACH(&tran->complete, info, c_reqs)
        {
            int crt_tottm = 0;
            int crt_rtt = 0;
            int crt_rtrs = 0;

            osql_sess_getsummary(info->sess, &crt_tottm, &crt_rtt, &crt_rtrs);

            min_tottm = (min_tottm < crt_tottm) ? min_tottm : crt_tottm;
            max_tottm = (max_tottm > crt_tottm) ? max_tottm : crt_tottm;
            min_rtt = (min_rtt < crt_rtt) ? min_rtt : crt_rtt;
            max_rtt = (max_rtt > crt_rtt) ? max_rtt : crt_rtt;
            min_rtrs = (min_rtrs < crt_rtrs) ? min_rtrs : crt_rtrs;
            max_rtrs = (max_rtrs > crt_rtrs) ? max_rtrs : crt_rtrs;
        }

        nametype = osql_sorese_type_to_str(iq->sorese.type);

        snprintf(ret, sz, "%s num=%u tot=[%u %u] rtt=[%u %u] rtrs=[%u %u]",
                 nametype, tran->num, min_tottm, max_tottm, min_rtt, max_rtt,
                 min_rtrs, max_rtrs);
        ret[sz - 1] = '\0';
    }

    return ret;
}

/**
 * Free all the sessions and free the bplog
 * HACKY: since we want to catch and report long requests in block
 * process, call this function only after reqlog_end_request is called
 * (sltdbt.c)
 */
static pthread_mutex_t kludgelk = PTHREAD_MUTEX_INITIALIZER;
int osql_bplog_free(struct ireq *iq, int are_sessions_linked, const char *func, const char *callfunc, int line)
{
    int val = 0;
    int rc = 0;
    int bdberr = 0;
    blocksql_info_t *info = NULL, *tmp = NULL;
    pthread_mutex_lock(&kludgelk);
    blocksql_tran_t *tran = (blocksql_tran_t *)iq->blocksql_tran;
    iq->blocksql_tran = NULL;
    pthread_mutex_unlock(&kludgelk);

    if (tran) {

        /* null this here, since the clients of sessions could still use
           sess->iq->blockproc to update the transaction bplog
           osql_close_session ensures there are no further clients
           of sess before removing it from the lookup hash
         */

        /* remove the sessions from repository and free them */
        LISTC_FOR_EACH_SAFE(&tran->pending, info, tmp, p_reqs)
        {
            /* this can be the case for failed transactions */
            osql_close_session(iq, &info->sess, are_sessions_linked, func, callfunc, line);
        }


        LISTC_FOR_EACH_SAFE(&tran->pending, info, tmp, p_reqs)
        {
            listc_rfl(&tran->pending, info);
            free(info);
        }

        LISTC_FOR_EACH_SAFE(&tran->complete, info, tmp, c_reqs)
        {
            osql_close_session(iq, &info->sess, are_sessions_linked, func, callfunc, line);
        }

        LISTC_FOR_EACH_SAFE(&tran->complete, info, tmp, c_reqs)
        {
            listc_rfl(&tran->complete, info);
            free(info);
        }


        /* destroy transaction */
        pthread_cond_destroy(&tran->cond);
        pthread_mutex_destroy(&tran->mtx);
        pthread_mutex_destroy(&tran->store_mtx);

        /* this will clean up any hung block procs */
        sem_getvalue(&tran->throttle, &val);
        while (val < g_osql_blocksql_parallel_max) {
            sem_post(&tran->throttle);
            poll(NULL, 0, 10);
            sem_getvalue(&tran->throttle, &val);
        }
        sem_destroy(&tran->throttle);

        rc = bdb_temp_table_close(thedb->bdb_env, tran->db, &bdberr);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s: failed close table rc=%d bdberr=%d\n",
                    __func__, rc, bdberr);
        } else {
            tran->db = NULL;
        }

        free(tran);

        /* free the space for sql strings */
        if (iq->sqlhistory_ptr && iq->sqlhistory_ptr != &iq->sqlhistory[0]) {
            free(iq->sqlhistory_ptr);
            iq->sqlhistory_ptr = NULL;
        }
        iq->sqlhistory_len = 0;
    }
    return 0;
}

const char *osql_reqtype_str(int type)
{
    switch (type) {
    case OSQL_RPLINV:
        return "rplinv";
    case OSQL_DONE:
        return "done";
    case OSQL_USEDB:
        return "usedb";
    case OSQL_DELREC:
        return "delrec";
    case OSQL_INSREC:
        return "insrec";
    case OSQL_CLRTBL:
        return "clrtbl";
    case OSQL_QBLOB:
        return "qblob";
    case OSQL_UPDREC:
        return "updrec";
    case OSQL_XERR:
        return "xerr";
    case OSQL_UPDCOLS:
        return "updcols";
    case OSQL_DONE_STATS:
        return "done_stats";
    case OSQL_DBGLOG:
        return "dbglog";
    case OSQL_RECGENID:
        return "recgenid";
    case OSQL_UPDSTAT:
        return "updstat";
    case OSQL_EXISTS:
        return "exists";
    case OSQL_INSERT:
        return "insert";
    case OSQL_DELETE:
        return "delete";
    case OSQL_UPDATE:
        return "update";
    case OSQL_SCHEMACHANGE: return "schemachange";
    default:
        return "???";
    }
}

/**
 * Inserts the op in the iq oplog
 * If sql processing is local, this is called by sqlthread
 * If sql processing is remote, this is called by reader_thread for the offload
 *node
 * Returns 0 if success
 *
 */
int osql_bplog_saveop(osql_sess_t *sess, char *rpl, int rplen,
                      unsigned long long rqid, uuid_t uuid,
                      unsigned long long seq, char *host)
{
    blocksql_tran_t *tran = (blocksql_tran_t *)osql_sess_getbptran(sess);
    struct ireq *iq;
    int rc = 0, rc_op = 0;
    oplog_key_t key;
    int rpl_len = 0;
    struct errstat *xerr;
    int bdberr;
    int debug = 0;
    uuidstr_t us;

    int type = 0;
    buf_get(&type, sizeof(type), rpl, rpl + rplen);
    if (type == OSQL_SCHEMACHANGE) sess->iq->tranddl = 1;

#if 0
    printf("Saving done bplog rqid=%llx type=%d (%s) tmp=%llu seq=%d\n",
           rqid, type, osql_reqtype_str(type), osql_log_time(), seq);
#endif

    key.rqid = rqid;
    key.seq = seq;
    comdb2uuidcpy(key.uuid, uuid);

    if (!tran || !tran->db) {
        /* something has gone wrong dispatching the socksql request, ignoring
           when the bplog creation failed, the caller is responsible for
           notifying the source that the session has gone awry
         */
        return 0;
    }

    /* add the op into the temporary table */
    if ((rc = pthread_mutex_lock(&tran->store_mtx))) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock: error code %d\n", rc);
        return rc;
    }

    if (seq == 0 && osql_session_is_sorese(sess)) {
        /* lets make sure that the temp table is empty since commit retries will
         * use same rqid*/
        if (tran->rows > 0) {
            sorese_info_t sorese_info = {0};
            struct errstat generr = {0};

            logmsg(LOGMSG_ERROR, "%s Broken transaction, received first seq row but "
                            "session already has rows?\n",
                    __func__);

            sorese_info.rqid = rqid;
            sorese_info.host = host;
            sorese_info.type = -1; /* I don't need it */

            generr.errval = RC_INTERNAL_RETRY;
            strncpy(generr.errstr,
                    "malformed transaction, received duplicated first row",
                    sizeof(generr.errstr));

            if ((rc = pthread_mutex_unlock(&tran->store_mtx))) {
                logmsg(LOGMSG_ERROR, "pthread_mutex_unlock: error code %d\n", rc);
            }

            rc = osql_comm_signal_sqlthr_rc(&sorese_info, &generr,
                                            RC_INTERNAL_RETRY);
            if (rc) {
                logmsg(LOGMSG_ERROR, "Failed to signal replicant rc=%d\n", rc);
            }

            return -1;
        }
    }

#if 0 
   printf("%s: rqid=%llx Saving op type=%d\n", __func__, key.rqid, ntohl(*((int*)rpl)));
#endif

    rc_op = bdb_temp_table_put(thedb->bdb_env, tran->db, &key, sizeof(key), rpl,
                               rplen, NULL, &bdberr);
    if (rc_op) {
        logmsg(LOGMSG_ERROR, 
            "%s: fail to put oplog rqid=%llx (%lld) seq=%llu rc=%d bdberr=%d\n",
            __func__, key.rqid, key.rqid, key.seq, rc, bdberr);
    } else if (gbl_osqlpfault_threads) {
        osql_page_prefault(rpl, rplen, &(tran->last_db),
                           &(osql_session_get_ireq(sess)->osql_step_ix), rqid,
                           uuid, seq);
    }

    tran->rows++;

    if ((rc = pthread_mutex_unlock(&tran->store_mtx))) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_unlock: error code %d\n", rc);
        return rc;
    }

    if (!rc_op && osql_comm_is_done(rpl, rplen, rqid == OSQL_RQID_USE_UUID, &xerr,
                                    osql_session_get_ireq(sess))) {

        osql_sess_set_complete(rqid, uuid, sess, xerr);

        /* if we received a too early, check the coherency and mark blackout the
         * node */
        if (xerr && xerr->errval == OSQL_TOOEARLY) {
            osql_comm_blkout_node(host);
        }

        if ((rc = osql_bplog_signal(tran))) {
            return rc;
        }

        debug = debug_this_request(gbl_debug_until);
        if (gbl_who > 0 && gbl_debug) {
            debug = 1;
        }

        if (osql_session_is_sorese(sess)) {

            osql_sess_lock(sess);
            osql_sess_lock_complete(sess);
            if (!osql_sess_dispatched(sess) && !osql_sess_is_terminated(sess)) {
                iq = osql_session_get_ireq(sess);
                osql_session_set_ireq(sess, NULL);
                osql_sess_set_dispatched(sess, 1);
                rc = handle_buf_sorese(thedb, iq, debug);
            }
            osql_sess_unlock_complete(sess);
            osql_sess_unlock(sess);

            return rc;
        }

        return 0;
    }

    return rc_op;
}

/**
 * Wakeup the block processor waiting for a completed session
 *
 */
int osql_bplog_signal(blocksql_tran_t *tran)
{

    int rc = 0;

    /* signal block processor that one done event arrived */
    if ((rc = pthread_mutex_lock(&tran->mtx))) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock: error code %d\n", rc);
        return rc;
    }

    tran->dowait = 0;
#if 0
   printf("Signalling tmp=%llu\n", osql_log_time());
#endif

    if ((rc = pthread_cond_signal(&tran->cond))) {
        logmsg(LOGMSG_ERROR, "pthread_cond_signal: error code %d\n", rc);
    }

    if ((rc = pthread_mutex_unlock(&tran->mtx))) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock: error code %d\n", rc);
        return rc;
    }

    return rc;
}

/**
 * Construct a blockprocessor transaction buffer containing
 * a sock sql /recom /snapisol /serial transaction
 *
 * Note: sqlqret is the location where we saved the query
 *       This will allow let us point directly to the buffer
 *
 */
static int wordoff(int byteoff) { return (byteoff + 3) / 4; }
int osql_bplog_build_sorese_req(uint8_t *p_buf_start,
                                const uint8_t **pp_buf_end, const char *sqlq,
                                int sqlqlen, const char *tzname, int reqtype,
                                char **sqlqret, int *sqlqlenret,
                                unsigned long long rqid, uuid_t uuid)
{
    struct dbtable *db;

    struct req_hdr req_hdr;
    struct block_req req;
    struct packedreq_hdr op_hdr;
    struct packedreq_usekl usekl;
    struct packedreq_tzset tzset;
    struct packedreq_sql sql;
    struct packedreq_seq seq;
    struct packedreq_seq2 seq2;
    int *p_rqid = (int *)&rqid;
    int seed;

    uint8_t *p_buf;

    uint8_t *p_buf_req_start;
    const uint8_t *p_buf_req_end;

    uint8_t *p_buf_op_hdr_start;
    const uint8_t *p_buf_op_hdr_end;

    p_buf = p_buf_start;

    /*
     * req hdr (prccom hdr)
     */

    /* build prccom header */
    bzero(&req_hdr, sizeof(req_hdr));
    req_hdr.opcode = OP_BLOCK;

    /* pack prccom header */
    if (!(p_buf = req_hdr_put(&req_hdr, p_buf, *pp_buf_end)))
        return -1;

    /*
     * req
     */

    /* save room in the buffer for req, we need to write it last after we know
     * the proper overall offset */
    if (BLOCK_REQ_LEN > (*pp_buf_end - p_buf))
        return -1;
    p_buf_req_start = p_buf;
    p_buf += BLOCK_REQ_LEN;
    p_buf_req_end = p_buf;

    req.num_reqs = 0; /* will be incremented as we go */

    /*
     * usekl
     */

    /* save room for usekl op header */
    if (PACKEDREQ_HDR_LEN > (*pp_buf_end - p_buf))
        return -1;
    p_buf_op_hdr_start = p_buf;
    p_buf += PACKEDREQ_HDR_LEN;
    p_buf_op_hdr_end = p_buf;

    /* provide db[0], it doesn't matter anyway */
    db = thedb->dbs[0];
    usekl.dbnum = db->dbnum;
    usekl.taglen = strlen(db->dbname) + 1 /*NUL byte*/;

    /* pack usekl */
    if (!(p_buf = packedreq_usekl_put(&usekl, p_buf, *pp_buf_end)))
        return -1;
    if (!(p_buf = buf_no_net_put(db->dbname, usekl.taglen, p_buf, *pp_buf_end)))
        return -1;

    /* build usekl op hdr */
    ++req.num_reqs;
    op_hdr.opcode = BLOCK2_USE;
    op_hdr.nxt = one_based_word_offset_from_ptr(p_buf_start, p_buf);

    /* pad to the next word */
    if (!(p_buf = buf_zero_put(
              ptr_from_one_based_word_offset(p_buf_start, op_hdr.nxt) - p_buf,
              p_buf, *pp_buf_end)))
        return -1;

    /* pack usekl op hdr in the space we saved */
    if (packedreq_hdr_put(&op_hdr, p_buf_op_hdr_start, p_buf_op_hdr_end) !=
        p_buf_op_hdr_end)
        return -1;

    /*
     * tzset
     */

    /* save room for tzset op header */
    if (PACKEDREQ_HDR_LEN > (*pp_buf_end - p_buf))
        return -1;
    p_buf_op_hdr_start = p_buf;
    p_buf += PACKEDREQ_HDR_LEN;
    p_buf_op_hdr_end = p_buf;

    /* build tzset */
    tzset.tznamelen = strlen(tzname) + 1 /*NUL byte*/;

    /* pack tzset */
    if (!(p_buf = packedreq_tzset_put(&tzset, p_buf, *pp_buf_end)))
        return -1;
    if (!(p_buf = buf_no_net_put(tzname, tzset.tznamelen, p_buf, *pp_buf_end)))
        return -1;

    /* build tzset op hdr */
    ++req.num_reqs;
    op_hdr.opcode = BLOCK2_TZ;
    op_hdr.nxt = one_based_word_offset_from_ptr(p_buf_start, p_buf);

    /* pad to the next word */
    if (!(p_buf = buf_zero_put(
              ptr_from_one_based_word_offset(p_buf_start, op_hdr.nxt) - p_buf,
              p_buf, *pp_buf_end)))
        return -1;

    /* pack usekl op hdr in the space we saved */
    if (packedreq_hdr_put(&op_hdr, p_buf_op_hdr_start, p_buf_op_hdr_end) !=
        p_buf_op_hdr_end)
        return -1;

    /*
     * sql
     */

    /* save room for sql op header */
    if (PACKEDREQ_HDR_LEN > (*pp_buf_end - p_buf))
        return -1;
    p_buf_op_hdr_start = p_buf;
    p_buf += PACKEDREQ_HDR_LEN;
    p_buf_op_hdr_end = p_buf;

    /* bulid sql */
    sql.sqlqlen = sqlqlen;
    /* include all sql if requested for longreq logging */
    if (sqlqlen > 256 && !gbl_enable_osql_longreq_logging)
        sql.sqlqlen = 256;
    /* must make sure the packed sqlq is NUL terminated later on */

    /* pack sql */
    uint8_t *sqlbuf = p_buf;
    if (!(p_buf = packedreq_sql_put(&sql, p_buf, *pp_buf_end)))
        return -1;
    *sqlqret =
        (char *)p_buf; /* save location of the sql string in the buffer */
    *sqlqlenret = sql.sqlqlen;
    if (!(p_buf = buf_no_net_put(sqlq, sql.sqlqlen, p_buf, *pp_buf_end)))
        return -1;

    /* build sql op hdr */
    ++req.num_reqs;
    op_hdr.opcode = req2blockop(reqtype);
    op_hdr.nxt = one_based_word_offset_from_ptr(p_buf_start, p_buf);

    /* TODO NUL terminate sql here if it was truncated earlier */
    /* pad to the next word */
    if (!(p_buf = buf_zero_put(
              ptr_from_one_based_word_offset(p_buf_start, op_hdr.nxt) - p_buf,
              p_buf, *pp_buf_end)))
        return -1;

    /* pack sql op hdr in the space we saved */
    if (packedreq_hdr_put(&op_hdr, p_buf_op_hdr_start, p_buf_op_hdr_end) !=
        p_buf_op_hdr_end)
        return -1;

    /*
     * blkseq
     */
    p_buf_op_hdr_start = p_buf;
    p_buf += PACKEDREQ_HDR_LEN;
    if (rqid == OSQL_RQID_USE_UUID) {
        if (PACKEDREQ_SEQ2_LEN > (*pp_buf_end - p_buf))
            return -1;
    } else {
        if (PACKEDREQ_SEQ_LEN > (*pp_buf_end - p_buf))
            return -1;
    }
    p_buf_op_hdr_end = p_buf;

    ++req.num_reqs;
    if (rqid == OSQL_RQID_USE_UUID) {
        comdb2uuidcpy(seq2.seq, uuid);

        if (!(p_buf = packedreq_seq2_put(&seq2, p_buf, *pp_buf_end)))
            return -1;

        op_hdr.opcode = BLOCK2_SEQV2;
    } else {
        seq.seq1 = p_rqid[0];
        seq.seq2 = p_rqid[1];
        seed = seq.seq1 ^ seq.seq2;
        seq.seq3 = rand_r((unsigned int *)&seed);

        if (!(p_buf = packedreq_seq_put(&seq, p_buf, *pp_buf_end)))
            return -1;

        op_hdr.opcode = BLOCK_SEQ;
    }

    /* blkseq header */
    op_hdr.nxt = one_based_word_offset_from_ptr(p_buf_start, p_buf);

    /* pad to the next word */
    if (!(p_buf = buf_zero_put(
              ptr_from_one_based_word_offset(p_buf_start, op_hdr.nxt) - p_buf,
              p_buf, *pp_buf_end)))
        return -1;

    /* pack seq op hdr in the space we saved */
    if (packedreq_hdr_put(&op_hdr, p_buf_op_hdr_start, p_buf_op_hdr_end) !=
        p_buf_op_hdr_end)
        return -1;

    /*
     * req
     */

    /* build req */
    req.flags = BLKF_ERRSTAT; /* we want error stat */
    req.offset = op_hdr.nxt;  /* overall offset = next offset of last op */

    /* pack req in the space we saved at the start */
    if (block_req_put(&req, p_buf_req_start, p_buf_req_end) != p_buf_req_end)
        return -1;

    *pp_buf_end = p_buf;
    return 0;
}

/**
 * Signal blockprocessor that one has completed
 * For now this is used only for
 *
 */
int osql_bplog_session_is_done(struct ireq *iq)
{
    blocksql_tran_t *tran = iq->blocksql_tran;
    int rc;

    if (!tran) {
        return -1;
    }

    rc = sem_post(&tran->throttle);
    if (rc) {
        logmsgperror("sem_post");
    }
    return rc;
}

/**
 * Set parallelism threshold
 *
 */
void osql_bplog_setlimit(int limit) { g_osql_blocksql_parallel_max = limit; }

/************************* INTERNALS
 * ***************************************************/

static int process_this_session(
    struct ireq *iq, void *iq_tran, osql_sess_t *sess, int *bdberr, int *nops,
    struct block_err *err, SBUF2 *logsb, struct temp_cursor *dbc,
    int (*func)(struct ireq *, unsigned long long, uuid_t, void *, char *, int,
                int *, int **, blob_buffer_t blobs[MAXBLOBS], int,
                struct block_err *, int *, SBUF2 *))
{

    blocksql_tran_t *tran = (blocksql_tran_t *)iq->blocksql_tran;
    unsigned long long rqid = osql_sess_getrqid(sess);
    oplog_key_t *key = NULL;
    oplog_key_t key_next, key_crt;
    char *data = NULL;
    int datalen = 0;
    int countops = 0;
    int lastrcv = 0;
    int rc = 0, rc_out = 0;
    int *updCols = NULL;

    /* session info */
    blob_buffer_t blobs[MAXBLOBS] = {0};
    int step = 0;
    int receivedrows = 0;
    int flags = 0;
    uuid_t uuid;
    uuidstr_t us;

    iq->queryid = osql_sess_queryid(sess);

    key = (oplog_key_t *)malloc(sizeof(oplog_key_t));
    if (!key) {
        logmsg(LOGMSG_ERROR, "%s: unable to allocated %d bytes\n", __func__,
                sizeof(oplog_key_t));
        return -1;
    }

    key->rqid = rqid;
    key->seq = 0;
    osql_sess_getuuid(sess, key->uuid);
    osql_sess_getuuid(sess, uuid);
    key_next = key_crt = *key;

    if (key->rqid != OSQL_RQID_USE_UUID)
        reqlog_set_rqid(iq->reqlogger, &key->rqid, sizeof(unsigned long long));
    else
        reqlog_set_rqid(iq->reqlogger, uuid, sizeof(uuid));
    reqlog_set_event(iq->reqlogger, "txn");

    /* go through each record */
    rc = bdb_temp_table_find_exact(thedb->bdb_env, dbc, key, sizeof(*key),
                                   bdberr);
    if (rc && rc != IX_EMPTY && rc != IX_NOTFND) {
        logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_first failed rc=%d bdberr=%d\n",
                __func__, rc, *bdberr);
        return rc;
    }

    if (rc == IX_NOTFND) {
        comdb2uuidstr(uuid, us);
        logmsg(LOGMSG_ERROR, "%s: session %llx %s has no update rows?\n", __func__,
                rqid, us);
    }

    while (!rc && !rc_out) {

        data = bdb_temp_table_data(dbc);
        datalen = bdb_temp_table_datasize(dbc);

        if (bdb_lock_desired(thedb->bdb_env)) {
            logmsg(LOGMSG_ERROR, "%d %s:%d blocksql session closing early\n",
                    pthread_self(), __FILE__, __LINE__);
            err->blockop_num = 0;
            err->errcode = ERR_NOMASTER;
            err->ixnum = 0;
            return ERR_NOMASTER /*OSQL_FAILDISPATCH*/;
        }

        if (iq->osql_step_ix)
            gbl_osqlpf_step[*(iq->osql_step_ix)].step = key_next.seq << 7;

        lastrcv = receivedrows;

        /* this locks pages */
        rc_out = func(iq, rqid, uuid, iq_tran, data, datalen, &flags, &updCols,
                      blobs, step, err, &receivedrows, logsb);

        if (rc_out != 0 && rc_out != OSQL_RC_DONE) {
            /* error processing, can be a verify error or deadlock */
            break;
        }

        if (lastrcv != receivedrows && is_rowlocks_transaction(iq_tran)) {
            rowlocks_check_commit_physical(thedb->bdb_env, iq_tran, ++countops);
        }

        key_crt = key_next; /* save previous key */

        rc = bdb_temp_table_next(thedb->bdb_env, dbc, bdberr);
        if (!rc) {
            /* are we still on the same rqid */
            key_next = *(oplog_key_t *)bdb_temp_table_key(dbc);
            if (key_next.rqid != key_crt.rqid) {
                /* done with the current transaction*/
                break;
            }
            if (key_crt.rqid == OSQL_RQID_USE_UUID) {
                if (comdb2uuidcmp(key_crt.uuid, key_next.uuid)) {
                    /* done with the current transaction*/
                    break;
                }
            } else {
                if (key_next.rqid != key_crt.rqid) {
                    /* done with the current transaction*/
                    break;
                }
            }

            /* check correct sequence; this is an attempt to
               catch dropped packets - not that we would do that purposely */
            if (key_next.seq != key_crt.seq + 1) {
                uuidstr_t us;
                comdb2uuidstr(uuid, us);
                logmsg(LOGMSG_ERROR, "%s: session %llu %s has missing packets [jump "
                                "%llu to %llu], aborting\n",
                        __func__, rqid, us, key_crt.seq, key_next.seq);
                rc = OSQL_SKIPSEQ;
            }
        }
        step++;
    }

    /* if for some reason the session has not completed correctly,
       this will free the eventually allocated buffers */
    free_blob_buffers(blobs, MAXBLOBS);

    if (updCols)
        free(updCols);

    if (rc == 0 || rc == IX_PASTEOF || rc == IX_EMPTY) {
        rc = 0;
    } else {
        logmsg(LOGMSG_ERROR, "%s:%d bdb_temp_table_next failed rc=%d bdberr=%d\n",
                __func__, __LINE__, rc, *bdberr);
        rc_out = ERR_INTERNAL;
        /* fall-through */
    }

    if (rc_out == OSQL_RC_DONE) {
        *nops += receivedrows;
        rc_out = 0;
    }

    return rc_out;
}

/**
 * Log the strings for each completed blocksql request for the
 * reqlog
 */
int osql_bplog_reqlog_queries(struct ireq *iq)
{
    blocksql_tran_t *tran = (blocksql_tran_t *)iq->blocksql_tran;
    blocksql_info_t *info = NULL;

    /* go through the complete list and apply all the changes */
    LISTC_FOR_EACH(&tran->complete, info, c_reqs)
    {
        osql_sess_reqlogquery(info->sess, iq->reqlogger);
    }

    return 0;
}

static int apply_changes(struct ireq *iq, blocksql_tran_t *tran, void *iq_tran,
                         int *nops, struct block_err *err, SBUF2 *logsb)
{

    blocksql_info_t *info = NULL;
    int rc = 0;
    int out_rc = 0;
    int bdberr = 0;
    struct temp_cursor *dbc = NULL;
    bpfunc_lstnode_t *cur_bpfunc = NULL;

    /* lock the table (it should get no more access anway) */
    if ((rc = pthread_mutex_lock(&tran->store_mtx))) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock: error code %d\n", rc);
        return rc;
    }

    *nops = 0;

    /* if we've already had a few verify-failures, add extended checking now */
    if (!iq->vfy_genid_track &&
        iq->sorese.verify_retries >= gbl_osql_verify_ext_chk) {
        iq->vfy_genid_track = 1;
        iq->vfy_genid_hash = hash_init(sizeof(unsigned long long));
        iq->vfy_genid_pool =
            pool_setalloc_init(sizeof(unsigned long long), 0, malloc, free);
    }

    /* clear everything- we are under store_mtx */
    if (iq->vfy_genid_track) {
        hash_clear(iq->vfy_genid_hash);
        pool_clear(iq->vfy_genid_pool);
    }

    /* create a cursor */
    dbc = bdb_temp_table_cursor(thedb->bdb_env, tran->db, NULL, &bdberr);
    if (!dbc || bdberr) {
        if (pthread_mutex_unlock(&tran->store_mtx)) {
            logmsg(LOGMSG_ERROR, "pthread_mutex_unlock: error code %d\n", rc);
        }
        logmsg(LOGMSG_ERROR, "%s: failed to create cursor bdberr = %d\n", __func__,
                bdberr);
        return ERR_INTERNAL;
    }

    listc_init(&iq->bpfunc_lst, offsetof(bpfunc_lstnode_t, linkct));

    /* go through the complete list and apply all the changes */
    LISTC_FOR_EACH(&tran->complete, info, c_reqs)
    {

        /* TODO: add an extended error structure to be passed back to the client
         */
        out_rc = process_this_session(iq, iq_tran, info->sess, &bdberr, nops,
                                      err, logsb, dbc, osql_process_packet);

        if (out_rc)
            break;
    }
#if 0
    /* we will apply these outside a transaction */
    if (out_rc) {
        while ((cur_bpfunc = listc_rtl(&iq->bpfunc_lst))) {
            assert(cur_bpfunc->func->fail != NULL);
            cur_bpfunc->func->fail(iq_tran, cur_bpfunc->func, NULL);
            free_bpfunc(cur_bpfunc->func);
        }
    } else {
        while ((cur_bpfunc = listc_rtl(&iq->bpfunc_lst))) {
            assert(cur_bpfunc->func->success != NULL);
            cur_bpfunc->func->success(iq_tran, cur_bpfunc->func, NULL);
            free_bpfunc(cur_bpfunc->func);
        }
    }
#endif

    if (rc = pthread_mutex_unlock(&tran->store_mtx)) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_unlock: error code %d\n", rc);
        return rc;
    }

    /* close the cursor */
    rc = bdb_temp_table_close_cursor(thedb->bdb_env, dbc, &bdberr);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: failed close cursor rc=%d bdberr=%d\n", __func__,
                rc, bdberr);
    }

    return out_rc;
}

static int osql_bplog_wait(blocksql_tran_t *tran)
{

    struct timespec tm_s;
    int rc = 0;

    /* wait for more ops */
    clock_gettime(CLOCK_REALTIME, &tm_s);
    tm_s.tv_sec += gbl_osql_blockproc_timeout_sec;

    /* signal block processor that one done event arrived */
    if ((rc = pthread_mutex_lock(&tran->mtx))) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock: error code %d\n", rc);
        return rc;
    }

    if (tran->dowait) {
#if 0
      printf("Waitng tmp=%llu\n", osql_log_time());
#endif
        rc = pthread_cond_timedwait(&tran->cond, &tran->mtx, &tm_s);
        if (rc && rc != ETIMEDOUT) {
            logmsg(LOGMSG_ERROR, "pthread_cond_wait: error code %d\n", rc);
            return rc;
        }

        tran->dowait = 0;
    } else {
        tran->dowait = 1;
    }

    if ((rc = pthread_mutex_unlock(&tran->mtx))) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock: error code %d\n", rc);
        return rc;
    }

    return rc;
}

static int req2blockop(int reqtype)
{
    switch (reqtype) {
    case OSQL_BLOCK_REQ:
        return BLOCK2_SQL;

    case OSQL_SOCK_REQ:
        return BLOCK2_SOCK_SQL;

    case OSQL_RECOM_REQ:
        return BLOCK2_RECOM;

    case OSQL_SNAPISOL_REQ:
        return BLOCK2_SNAPISOL;

    case OSQL_SERIAL_REQ:
        return BLOCK2_SERIAL;
    }

    logmsg(LOGMSG_ERROR, "%s: unknown request type %d\n", __func__, reqtype);
    {
        int once = 0;
        if (!once) {
            cheap_stack_trace();
            once = 1;
        }
    }

    return OSQL_REQINV;
}

void osql_bplog_time_done(struct ireq *iq)
{
    blocksql_tran_t *tran = (blocksql_tran_t *)iq->blocksql_tran;
    osql_bp_timings_t *tms = &iq->timings;
    blocksql_info_t *info = NULL;
    int rc = 0;
    int bdberr = 0;
    char msg[4096];
    unsigned long long rqid;
    int tottm = 0;
    int rtt = 0;
    int rtrs = 0;
    int len;

    if (!gbl_time_osql)
        return;

    if (tran) {

        if (tms->req_sentrc == 0)
            tms->req_sentrc = tms->req_applied;

        snprintf0(
            msg, sizeof(msg),
            "Total %llu (sql=%llu upd=%llu repl=%llu signal=%llu retries=%u) [",
            tms->req_finished - tms->req_received, /* total time */
            tms->req_alldone -
                tms->req_received, /* time to get sql processing done */
            tms->req_applied - tms->req_alldone,    /* time to apply updates */
            tms->req_sentrc - tms->replication_end, /* time to sent rc back to
                                                       sql (non-relevant for
                                                       blocksql*/
            tms->replication_end -
                tms->replication_start, /* time to replicate */
            tms->retries - 1);          /* how many time bplog was retried */
        len = strlen(msg);

        LISTC_FOR_EACH(&tran->pending, info, p_reqs)
        {
            /* these are failed */
            osql_sess_getsummary(info->sess, &tottm, &rtt, &rtrs);
            snprintf0(msg + len, sizeof(msg) - len,
                      " F(rqid=%llu time=%u lastrtt=%u retries=%u)",
                      osql_sess_getrqid(info->sess), tottm, rtt, rtrs);
            len = strlen(msg);
        }

        LISTC_FOR_EACH(&tran->complete, info, c_reqs)
        {
            /* these have finished */
            osql_sess_getsummary(info->sess, &tottm, &rtt, &rtrs);
            snprintf0(msg + len, sizeof(msg) - len,
                      " C(rqid=%llu time=%u lastrtt=%u retries=%u)",
                      osql_sess_getrqid(info->sess), tottm, rtt, rtrs);
            len = strlen(msg);
        }
    }
    logmsg(LOGMSG_USER, "%s]\n", msg);
}

static int osql_bplog_loginfo(struct ireq *iq, osql_sess_t *sess)
{
    blocksql_tran_t *tran = (blocksql_tran_t *)iq->blocksql_tran;
    struct temp_cursor *dbc = NULL;
    blob_buffer_t blobs[MAXBLOBS];
    SBUF2 *logsb = NULL;
    int bdberr = 0;
    int nops = 0;
    struct block_err err;
    int rc = 0, outrc = 0;
    char filename[PATH_MAX];
    int counter = 0;
    int fd = 0;
    pthread_mutex_t bplog_ctr_mtx = PTHREAD_MUTEX_INITIALIZER;
    static int bplog_ctr = 0; /* 256 files only */
    unsigned long long rqid;
    uuid_t uuid;

    rqid = osql_sess_getrqid(sess);
    osql_sess_getuuid(sess, uuid);

    pthread_mutex_lock(&bplog_ctr_mtx);
    counter = (bplog_ctr++) % 256;
    pthread_mutex_unlock(&bplog_ctr_mtx);

    /* open the sbuf2 */
    snprintf(filename, sizeof(filename), "%s/%s_toobig.log.%d", thedb->basedir,
             thedb->envname, counter);

    fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) {
        logmsg(LOGMSG_ERROR, "%s: failed to open %s\n", __func__, filename);
        return -1;
    }

    logsb = sbuf2open(fd, 0);
    if (!logsb) {
        logmsg(LOGMSG_ERROR, "%s sbuf2open failed\n", __func__);
        close(fd);
        return -1;
    }

    /* create a cursor */
    dbc = bdb_temp_table_cursor(thedb->bdb_env, tran->db, NULL, &bdberr);
    if (!dbc || bdberr) {
        logmsg(LOGMSG_ERROR, "%s: failed to create cursor bdberr = %d\n", __func__,
                bdberr);
        return ERR_INTERNAL;
    }

    outrc = process_this_session(iq, NULL, sess, &bdberr, &nops, &err, logsb,
                                 dbc, osql_log_packet);

    /* close the cursor */
    rc = bdb_temp_table_close_cursor(thedb->bdb_env, dbc, &bdberr);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: failed close cursor rc=%d bdberr=%d\n", __func__,
                rc, bdberr);
    }

    sbuf2close(logsb);

    return outrc;
}

int osql_throttle_session(struct ireq *iq)
{
    blocksql_tran_t *tran = iq->blocksql_tran;
    int rc;

    if (!tran) {
        return -1;
    }

    rc = sem_wait(&tran->throttle);
    if (rc) {
        logmsgperror("sem_wait");
    }
    return rc;
}

void osql_set_delayed(struct ireq *iq)
{
    if (iq) {
        blocksql_tran_t *t = (blocksql_tran_t *)iq->blocksql_tran;
        if (t)
            t->delayed = 1;
    }
}

int osql_get_delayed(struct ireq *iq)
{
    if (iq) {
        blocksql_tran_t *t = (blocksql_tran_t *)iq->blocksql_tran;
        if (t)
            return t->delayed;
    }
    return 1;
}

/**
 * Throw bplog to /dev/null, sql does not need this
 *
 */
int sql_cancelled_transaction(struct ireq *iq)
{
    int rc;

    logmsg(LOGMSG_DEBUG, "%s: cancelled transaction\n", __func__);
    rc = osql_bplog_free(iq, 1, __func__, NULL, 0);

    if (iq->p_buf_orig) {
        /* nothing changed this siince init_ireq */
        free(iq->p_buf_orig);
        iq->p_buf_orig = NULL;
    }

    destroy_ireq(thedb, iq);

    return rc;
}
