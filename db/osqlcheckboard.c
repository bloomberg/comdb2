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

#include <stdlib.h>
#include <pthread.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#include <plhash.h>
#include <sys/time.h>
#include <ctrace.h>
#include "comdb2.h"
#include "osqlcheckboard.h"
#include "osqlsqlthr.h"
#include "osqlcomm.h"
#include "bdb_api.h"
#include "errstat.h"
#include "sql.h"
#include "osqlrepository.h"
#include "bdb_api.h"
#include "comdb2uuid.h"
#include "net_types.h"
#include "logmsg.h"
#include "comdb2_atomic.h"

/* delete this after comdb2_api.h changes makes it through */
#define SQLHERR_MASTER_QUEUE_FULL -108
#define SQLHERR_MASTER_TIMEOUT -109

typedef struct osql_checkboard {
    hash_t * rqs; /* sql threads processing a blocksql are registered here */
    hash_t *rqsuuid;         /* like above, but register by uuid */
    pthread_mutex_t mtx; /* protect all the requests */

} osql_checkboard_t;

static osql_checkboard_t *checkboard = NULL;

/* will get rdlock on checkboard->mtx if parameter lock is set
 * if caller already has mtx, call this func with lock = false
 *
 * NB: If calling with lock == false, make sure you don't dereference 
 * pointer returned because it may be freed from under you.
 */
static inline osql_sqlthr_t *osql_chkboard_fetch_entry(unsigned long long rqid,
                                                       uuid_t uuid, bool lock)
{
    osql_sqlthr_t *entry = NULL;

    if (lock)
        Pthread_mutex_lock(&checkboard->mtx);

    if (rqid == OSQL_RQID_USE_UUID)
        entry = hash_find_readonly(checkboard->rqsuuid, uuid);
    else
        entry = hash_find_readonly(checkboard->rqs, &rqid);

    if (lock)
        Pthread_mutex_unlock(&checkboard->mtx);
    return entry;
}

int osql_checkboard_init(void)
{
    osql_checkboard_t *tmp = NULL;

    /* create repository if none */
    if (checkboard)
        return 0;

    tmp = (osql_checkboard_t *)calloc(1, sizeof(osql_checkboard_t));
    if (!tmp) {
        logmsg(LOGMSG_ERROR, "%s: calloc error\n", __func__);
        abort();
    }

    tmp->rqs = hash_init(sizeof(unsigned long long));
    if (!tmp->rqs) {
        free(tmp);
        logmsg(LOGMSG_ERROR, "%s: error init hash\n", __func__);
        return -1;
    }
    tmp->rqsuuid = hash_init_o(offsetof(osql_sqlthr_t, uuid), sizeof(uuid_t));

    Pthread_mutex_init(&tmp->mtx, NULL);
    checkboard = tmp;

    return 0;
}

/**
 * Destroy the checkboard
 * No more blocksql/socksql/recom/snapisol/serial threads can be created
 * after this.
 *
 */
void osql_checkboard_destroy(void) { /* TODO*/ }

/* insert entry into checkerboard */
static inline int insert_into_checkerboard(osql_checkboard_t *checkboard,
                                           osql_sqlthr_t *entry)
{
    int rc = 0;
    Pthread_mutex_lock(&checkboard->mtx);

    if (entry->rqid == OSQL_RQID_USE_UUID)
        rc = hash_add(checkboard->rqsuuid, entry);
    else
        rc = hash_add(checkboard->rqs, entry);

    Pthread_mutex_unlock(&checkboard->mtx);
    return rc;
}

/* delete entry from checkerboard */
static inline osql_sqlthr_t * delete_from_checkerboard(osql_checkboard_t *checkboard, osqlstate_t *osql)
{
    Pthread_mutex_lock(&checkboard->mtx);

    osql_sqlthr_t *entry = osql_chkboard_fetch_entry(osql->rqid, osql->uuid, false);
    if (!entry) {
        goto done;
    }
    if (osql->rqid == OSQL_RQID_USE_UUID) {
        int rc = hash_del(checkboard->rqsuuid, entry);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s: unable to delete record %llx, rc=%d\n",
                   __func__, entry->rqid, rc);
    } else {
        int rc = hash_del(checkboard->rqs, entry);
        if (rc) {
            uuidstr_t us;
            logmsg(LOGMSG_ERROR, "%s: unable to delete record %llx %s, rc=%d\n",
                   __func__, entry->rqid, comdb2uuidstr(osql->uuid, us),
                   rc);
        }
    }
    /*reset rqid */
    osql->rqid = 0;
done:
    Pthread_mutex_unlock(&checkboard->mtx);
    return entry;
}

/**
 * Register an osql thread with the checkboard
 * This allows block processor to query the status
 * of its sql peer
 *
 */
int osql_register_sqlthr(struct sqlclntstate *clnt, int type)
{
    osql_sqlthr_t *entry = (osql_sqlthr_t *)calloc(sizeof(osql_sqlthr_t), 1);
    int retry = 0;
    uuidstr_t us;

    if (!entry) {
        logmsg(LOGMSG_ERROR, "%s: unable to allocate %zu bytes\n", __func__,
               sizeof(unsigned long long));
        return -1;
    }

    entry->rqid = clnt->osql.rqid;
    comdb2uuidcpy(entry->uuid, clnt->osql.uuid);
    entry->master = clnt->osql.host;
    entry->type = type;
    entry->last_checked = entry->last_updated =
        comdb2_time_epochms(); /* initialize these to insert time */
    entry->clnt = clnt;
    entry->register_time = osql_log_time();

#ifdef DEBUG
    if (gbl_debug_sql_opcodes) {
        logmsg(LOGMSG_USER, "Registered %llx %s %s %d\n", entry->rqid,
               comdb2uuidstr(entry->uuid, us), entry->master, entry->type);
    }
#endif

    /* making sure we're adding the correct master */
    if (entry->master != thedb->master) {
        while ((entry->master = clnt->osql.host = thedb->master) == 0 &&
               retry < 60) {
            poll(NULL, 0, 500);
            retry++;
        }
        if (retry >= 60) {
            logmsg(LOGMSG_ERROR, "No master, failed to register request\n");
            free(entry);
            return -1;
        }
    }

    if (clnt->osql.host == gbl_mynode) {
        clnt->osql.host = 0;
    }

    if (entry->master == 0)
        entry->master = gbl_mynode;

    Pthread_mutex_init(&entry->c_mtx, NULL);
    Pthread_cond_init(&entry->cond, NULL);
    Pthread_mutex_init(&entry->cleanup_mtx, NULL);
    Pthread_cond_init(&entry->cleanup_cond, NULL);

    int rc = insert_into_checkerboard(checkboard, entry);

    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: error adding record %llx %s rc=%d\n",
               __func__, entry->rqid, comdb2uuidstr(entry->uuid, us), rc);
        free(entry);
    }

    if (gbl_enable_osql_logging && !clnt->osql.logsb) {
        int fd = 0;
        char filename[256];

        snprintf(filename, sizeof(filename), "m_%s_%u_%llu_%s.log",
                 clnt->osql.host, type, clnt->osql.rqid,
                 comdb2uuidstr(clnt->osql.uuid, us));
        fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if (!fd) {
            logmsg(LOGMSG_ERROR, "Error opening log file %s\n", filename);
        } else {
            clnt->osql.logsb = sbuf2open(fd, 0);
            if (!clnt->osql.logsb) {
                logmsg(LOGMSG_ERROR, "Error opening sbuf2 for file %s, fd %d\n",
                        filename, fd);
                close(fd);
            }
        }
    }

    return rc;
}


/**
 * Unregister an osql thread from the checkboard
 * No further signalling for this thread is possible
 *
 */
int osql_unregister_sqlthr(struct sqlclntstate *clnt)
{
    int rc = 0;

    if (clnt->osql.rqid == 0)
        return 0;

    osql_sqlthr_t *entry = delete_from_checkerboard(checkboard, &clnt->osql);
    if (!entry) {
        uuidstr_t us;
        logmsg(LOGMSG_ERROR, "%s: error unable to find record %llx %s\n",
               __func__, clnt->osql.rqid, comdb2uuidstr(clnt->osql.uuid, us));
        return 0;
    }

    int cnt = 0;
    /* wait till not in use to free; TODO: Can i reuse entry->c_mtx instead? */
    Pthread_mutex_lock(&entry->cleanup_mtx);
    while (ATOMIC_LOAD32(entry->in_use) > 0) {
        if (cnt > 10) // 10 seconds
            abort();

        struct timespec timeout;
        struct timeval tp;
        gettimeofday(&tp, NULL);
        timeout.tv_sec = tp.tv_sec + 1;
        timeout.tv_nsec = tp.tv_usec * 1000;

        int rc = pthread_cond_timedwait(&entry->cleanup_cond,
                                        &entry->cleanup_mtx, &timeout);
        if (rc == ETIMEDOUT) {
            cnt++;
        }
    }
    Pthread_mutex_unlock(&entry->cleanup_mtx);

    if (clnt->osql.logsb) {
        sbuf2close(clnt->osql.logsb);
        clnt->osql.logsb = NULL;
    }

#ifdef DEBUG
    if (gbl_debug_sql_opcodes) {
        uuidstr_t us;
        logmsg(LOGMSG_USER, "UNRegistered %llx %s %s %d\n", entry->rqid,
               comdb2uuidstr(clnt->osql.uuid, us), entry->master, entry->type);
    }
#endif

    /* free sql thread registration entry */
    Pthread_cond_destroy(&entry->cond);
    Pthread_mutex_destroy(&entry->c_mtx);
    Pthread_mutex_destroy(&entry->cleanup_mtx);
    Pthread_cond_destroy(&entry->cleanup_cond);
    free(entry);

    return rc;
}

/*
 * Checks the checkboard for sql session "rqid"
 * Returns true or false depending on whether sesssion exists
 */
bool osql_chkboard_sqlsession_exists(unsigned long long rqid, uuid_t uuid)
{
    if (!checkboard)
        return 0;

    return (osql_chkboard_fetch_entry(rqid, uuid, true) != NULL);
}

int osql_chkboard_sqlsession_rc(unsigned long long rqid, uuid_t uuid, int nops,
                                void *data, struct errstat *errstat)
{
    if (!checkboard)
        return 0;

    Pthread_mutex_lock(&checkboard->mtx);

    osql_sqlthr_t *entry = osql_chkboard_fetch_entry(rqid, uuid, false);
    if (!entry) {
        Pthread_mutex_unlock(&checkboard->mtx);
        /* This happens naturally for example
           if the client drops the connection while block processor
           is sending back the result
           Keep this in trace log

        fprintf(stderr, "%s: received result for missing session %llu\n",
              __func__, rqid);
         */
        uuidstr_t us;
        ctrace("%s: received result for missing session %llu %s\n", __func__,
               rqid, comdb2uuidstr(uuid, us));
        return -1;
    }

    ATOMIC_ADD32(entry->in_use, 1);  /* inc in use count so it won't be freed */
    Pthread_mutex_unlock(&checkboard->mtx);

    Pthread_mutex_lock(&entry->c_mtx);

    if (errstat)
        entry->err = *errstat;
    else
        bzero(&entry->err, sizeof(entry->err));

    entry->done = 1; /* mem sync? */
    entry->nops = nops;

    if (entry->type == OSQL_SNAP_UID_REQ && data != NULL) {
        snap_uid_t *snap_info = (snap_uid_t *)data;
        if (snap_info->rqtype == OSQL_NET_SNAP_FOUND_UID) {
            entry->clnt->is_retry = 1;
            entry->clnt->effects = snap_info->effects;
        } else if (snap_info->rqtype == OSQL_NET_SNAP_NOT_FOUND_UID) {
            entry->clnt->is_retry = 0;
        } else {
            entry->clnt->is_retry = -1;
        }
    }

    Pthread_cond_signal(&entry->cond);
    Pthread_mutex_unlock(&entry->c_mtx);
    ATOMIC_ADD32(entry->in_use, -1); /* no need for mtx because atomic */
    Pthread_cond_signal(&entry->cleanup_cond);

    return 0;
}

static inline void signal_master_change(osql_sqlthr_t *entry, char *host,
                                        const char *line)
{
    uuidstr_t us;
    if (gbl_master_swing_osql_verbose)
        logmsg(LOGMSG_INFO, "%s signaling entry new master %s %llx %s\n", line,
               host, entry->rqid, comdb2uuidstr(entry->uuid, us));
    entry->master_changed = 1;
    Pthread_cond_signal(&entry->cond);
}

int osql_checkboard_master_changed(void *obj, void *arg)
{
    osql_sqlthr_t *entry = obj;
    Pthread_mutex_lock(&entry->c_mtx);
    if (entry->master != arg && !(entry->master == 0 && gbl_mynode == arg)) {
        signal_master_change(entry, arg, __func__);
    }
    Pthread_mutex_unlock(&entry->c_mtx);
    return 0;
}

void osql_checkboard_for_each(void *arg, int (*func)(void *, void *))
{
    if (!checkboard)
        return;

    Pthread_mutex_lock(&checkboard->mtx);

    hash_for(checkboard->rqs, func, arg);
    hash_for(checkboard->rqsuuid, func, arg);

    Pthread_mutex_unlock(&checkboard->mtx);
}

static int osql_checkboard_check_request_down_node(void *obj, void *arg)
{
    osql_sqlthr_t *entry = obj;
    Pthread_mutex_lock(&entry->c_mtx);
    if (entry->master == arg) {
        signal_master_change(entry, arg, __func__);
    }
    Pthread_mutex_unlock(&entry->c_mtx);
    return 0;
}

void osql_checkboard_check_down_nodes(char *host)
{
    osql_checkboard_for_each(host, osql_checkboard_check_request_down_node);
}

/* NB: this is a helper function and waits for response from master
 * until max_wait count is reached.
 * This function is ment to be called with mutex entry->c_mtx in locked state
 * and returns with that same mutex locked if rc is 0
 * but unlocked if rc is nonzero
 */
static int wait_till_max_wait_or_timeout(osql_sqlthr_t *entry, int max_wait,
                                         struct errstat *xerr, int *cnt)
{
    int rc = 0;
    uuidstr_t us;
    /* several conditions cause us to break out */
    while (entry->done != 1 && !entry->master_changed &&
           ((max_wait > 0 && (*cnt) < max_wait) || max_wait < 0)) {

        /* prepare to wait for a second */
        struct timespec tm_s;
        clock_gettime(CLOCK_REALTIME, &tm_s);
        tm_s.tv_sec++;

        Pthread_mutex_unlock(&entry->c_mtx);

        int tm_recov_deadlk = comdb2_time_epochms();
        /* this call could wait for a bdb read lock; in the meantime,
           someone might try to signal us */
        if (osql_comm_check_bdb_lock(__func__, __LINE__)) {
            logmsg(LOGMSG_ERROR, "sosql: timed-out on bdb_lock_desired\n");
            rc = ERR_READONLY;
            break;
        }
        tm_recov_deadlk = comdb2_time_epochms() - tm_recov_deadlk;

        Pthread_mutex_lock(&entry->c_mtx);

        if (entry->done == 1 || entry->master_changed)
            break;

        int lrc = 0;
        if ((lrc = pthread_cond_timedwait(&entry->cond, &entry->c_mtx, &tm_s))) {
            if (ETIMEDOUT == lrc) {
                /* normal timeout .. */
            } else {
                logmsg(LOGMSG_ERROR, "pthread_cond_timedwait: error code %d\n",
                       lrc);
                rc = -7;
                break;
            }
        }

        (*cnt)++;

        /* we got back the mutex, are we there yet ? */
        if (entry->done == 1 || entry->master_changed ||
            (max_wait > 0 && (*cnt) >= max_wait))
            break;

        int poke_timeout =
            bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SOSQL_POKE_TIMEOUT_SEC) *
            1000;
        int poke_freq =
            bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SOSQL_POKE_FREQ_SEC) * 1000;

        /* is it the time to check the master? have we already done so? */
        int now = comdb2_time_epochms();

        if ((poke_timeout > 0) &&
            (entry->last_updated + poke_timeout + tm_recov_deadlk < now)) {
            /* timeout the request */
            logmsg(LOGMSG_ERROR,
                   "Master %s failed to acknowledge session %llu %s\n",
                   entry->master, entry->rqid, comdb2uuidstr(entry->uuid, us));
            entry->done = 1;
            xerr->errval = entry->err.errval = SQLHERR_MASTER_TIMEOUT;
            snprintf(entry->err.errstr, sizeof(entry->err.errstr),
                     "master %s lost transaction %llu", entry->master,
                     entry->rqid);
            break;
        }

        if ((poke_freq > 0) && (entry->last_checked + poke_freq <= now)) {
            entry->last_checked = now;

            /* try poke again */
            if (entry->master == 0 || entry->master == gbl_mynode) {
                /* local checkup */
                bool found =
                    osql_repository_session_exists(entry->rqid, entry->uuid);
                if (!found) {
                    logmsg(LOGMSG_ERROR,
                           "Local SORESE failed to find local "
                           "transaction %llu %s\n",
                           entry->rqid, comdb2uuidstr(entry->uuid, us));
                    entry->done = 1;
                    xerr->errval = entry->err.errval = SQLHERR_MASTER_TIMEOUT;
                    snprintf(entry->err.errstr, sizeof(entry->err.errstr),
                             "Local transaction failed, unable to locate "
                             "entry id=%llu",
                             entry->rqid);
                    break;
                }
                entry->last_updated = now;
                continue;
            }

            int lrc = osql_comm_send_poke(entry->master, entry->rqid,
                                          entry->uuid, NET_OSQL_MASTER_CHECK);
            if (lrc) {
                logmsg(LOGMSG_ERROR, "Failed to send master check lrc=%d\n",
                       lrc);
                entry->done = 1;
                xerr->errval = entry->err.errval = SQLHERR_MASTER_TIMEOUT;
                snprintf(entry->err.errstr, sizeof(entry->err.errstr),
                         "failed comm with master %s entry id=%llu %s",
                         entry->master, entry->rqid,
                         comdb2uuidstr(entry->uuid, us));
                break;
            }
        }
    }
    return rc;
}

/**
 * Wait for the session to complete
 * Upon return, sqlclntstate's errstat is set
 *
 */
int osql_chkboard_wait_commitrc(unsigned long long rqid, uuid_t uuid,
                                int max_wait, struct errstat *xerr)
{
    int cnt = 0;
    uuidstr_t us;
    int rc =0;

    if (!checkboard)
        return 0;

    Pthread_mutex_lock(&checkboard->mtx);
    osql_sqlthr_t *entry = osql_chkboard_fetch_entry(rqid, uuid, false);

    if (!entry) {
        Pthread_mutex_unlock(&checkboard->mtx);
        logmsg(LOGMSG_ERROR,
               "%s: received result for missing session %llu\n", __func__,
               rqid);
        return -2;
    }
    ATOMIC_ADD32(entry->in_use, 1);  /* inc in use count so it won't be freed */
    Pthread_mutex_unlock(&checkboard->mtx);


    while (!entry->done) {
        Pthread_mutex_lock(&entry->c_mtx);
        /* reset these time parameters */
        entry->last_checked = entry->last_updated = comdb2_time_epochms();

        // wait_till_max_wait_or_timeout() expects to be called with c_mtx locked
        rc = wait_till_max_wait_or_timeout(entry, max_wait, xerr, &cnt);
        if (rc) // dont unlock entry->c_mtx, on nonzero rc it was already unlocked
            break;

        int master_changed = entry->master_changed; /* fetch value under lock */

        Pthread_mutex_unlock(&entry->c_mtx);

        if (max_wait > 0 && cnt >= max_wait) {
            logmsg(LOGMSG_ERROR,
                   "%s: timed-out waiting for master %s "
                   "to commit id=%llu %s\n",
                   __func__, entry->master, entry->rqid,
                   comdb2uuidstr(entry->uuid, us));
            rc = -6;
            break;
        }

        if (master_changed) { /* retry at higher level */
            xerr->errval = ERR_NOMASTER;
            if (gbl_master_swing_osql_verbose) {
                uuidstr_t us;
                logmsg(LOGMSG_ERROR, "%s: [%llx][%s] master changed\n",
                       __func__, entry->rqid, comdb2uuidstr(entry->uuid, us));
            }
            break;
        }
    }

    *xerr = entry->err; // will deep copy err structure
    ATOMIC_ADD32(entry->in_use, -1); /* no need for mtx because atomic */
    Pthread_cond_signal(&entry->cleanup_cond);

    if (xerr->errval)
        logmsg(LOGMSG_DEBUG, "%s: done xerr->errval=%d\n", __func__,
               xerr->errval);
    return rc;
}

/**
 * Update status of the pending sorese transaction, to support poking
 *
 */
int osql_checkboard_update_status(unsigned long long rqid, uuid_t uuid,
                                  int status, int timestamp)
{
    uuidstr_t us;

    if (!checkboard)
        return 0;

    Pthread_mutex_lock(&checkboard->mtx);

    osql_sqlthr_t *entry = osql_chkboard_fetch_entry(rqid, uuid, false);
    if (!entry) {
        Pthread_mutex_unlock(&checkboard->mtx);
        ctrace("%s: SORESE received exists for missing session %llu %s\n",
               __func__, rqid, comdb2uuidstr(uuid, us));
        return -1;
    }

    ATOMIC_ADD32(entry->in_use, 1);  /* inc in use count so it won't be freed */
    Pthread_mutex_unlock(&checkboard->mtx);

    Pthread_mutex_lock(&entry->c_mtx);

    entry->status = status;
    entry->timestamp = timestamp;
    entry->last_updated = comdb2_time_epochms();

    Pthread_mutex_unlock(&entry->c_mtx);
    ATOMIC_ADD32(entry->in_use, -1); /* no need for mtx because atomic */
    Pthread_cond_signal(&entry->cleanup_cond);

    return 0;
}

/**
 * Reset fields when a session is retried
 * we're interested in things like master_changed
 *
 */
int osql_reuse_sqlthr(struct sqlclntstate *clnt, char *master)
{
    if (clnt->osql.rqid == 0)
        return 0;

    Pthread_mutex_lock(&checkboard->mtx);

    osql_sqlthr_t *entry =
        osql_chkboard_fetch_entry(clnt->osql.rqid, clnt->osql.uuid, false);
    if (!entry) {
        Pthread_mutex_unlock(&checkboard->mtx);
        uuidstr_t us;
        logmsg(LOGMSG_ERROR, "%s: error unable to find record %llx %s\n", __func__,
                clnt->osql.rqid, comdb2uuidstr(clnt->osql.uuid, us));
        return -1;
    }

    ATOMIC_ADD32(entry->in_use, 1);  /* inc in use count so it won't be freed */
    Pthread_mutex_unlock(&checkboard->mtx);

    Pthread_mutex_lock(&entry->c_mtx);
    entry->last_checked = entry->last_updated =
        comdb2_time_epochms(); /* reset these time */
    entry->done = 0;
    entry->master_changed = 0;
    entry->master =
        master ? master : gbl_mynode; /* master changed, store it here */
    bzero(&entry->err, sizeof(entry->err));
    Pthread_mutex_unlock(&entry->c_mtx);

    ATOMIC_ADD32(entry->in_use, -1); /* no need for mtx because atomic */
    Pthread_cond_signal(&entry->cleanup_cond);
    return 0;
}

/**
 * Retrieve the sqlclntstate for a certain rqid
 *
 *
 * NOTE: the returned clnt structure is dtran_mtx LOCKED!!!
 *
 */
int osql_chkboard_get_clnt_int(hash_t *h, void *k, struct sqlclntstate **clnt)
{
    int rc = 0;

    if (!checkboard)
        return 0;

    Pthread_mutex_lock(&checkboard->mtx);

    osql_sqlthr_t *entry = hash_find_readonly(h, k);
    if (!entry) {
        /* This happens naturally for example
           if the client drops the connection while block processor
           is sending back the result
           Keep this in trace log

           fprintf(stderr, "%s: received result for missing session %llu\n",
           __func__, rqid);

           No trc.c filling trace

        ctrace( "%s: received result for missing session %llu\n",
              __func__, rqid);
         */

        *clnt = NULL;
        rc = -1;
    } else {
        *clnt = entry->clnt;

        /* Need to lock this guy out
           This prevents them from getting freed upon commit
           before a new cursor is recorded */
        Pthread_mutex_lock(&(*clnt)->dtran_mtx);
    }

    Pthread_mutex_unlock(&checkboard->mtx);

    return rc;
}

int osql_chkboard_get_clnt(unsigned long long rqid, struct sqlclntstate **clnt)
{
    int rc;
    rc = osql_chkboard_get_clnt_int(checkboard->rqs, &rqid, clnt);
    if (*clnt == NULL) {
        ctrace("%s: received result for missing session %llu\n", __func__,
               rqid);
    }
    return rc;
}

int osql_chkboard_get_clnt_uuid(uuid_t uuid, struct sqlclntstate **clnt)
{
    int rc;
    rc = osql_chkboard_get_clnt_int(checkboard->rqsuuid, uuid, clnt);
    if (*clnt == NULL) {
        uuidstr_t us;
        ctrace("%s: received result for missing session %s\n", __func__,
               comdb2uuidstr(uuid, us));
    }
    return rc;
}
