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
#include <net_types.h>
#include <logmsg.h>

/* delete this after comdb2_api.h changes makes it through */
#define SQLHERR_MASTER_QUEUE_FULL -108
#define SQLHERR_MASTER_TIMEOUT -109

typedef struct osql_checkboard {

    hash_t *
        rqs; /* all the sql thread processing a blocksql are registered here */
    hash_t *rqsuuid;         /* like above, but register by uuid */
    pthread_rwlock_t rwlock; /* protect all the requests */

} osql_checkboard_t;

struct osql_sqlthr {
    unsigned long long rqid; /* osql rq id */
    uuid_t uuid;             /* request id, take 2 */
    char *master;            /* who was the master I was talking to */
    int done;            /* result of socksql, recom, snapisol and serial master
                            transactions*/
    struct errstat err;  /* valid if done = 1 */
    int type;            /* type of the request, enum OSQL_REQ_TYPE */
    pthread_mutex_t mtx; /* mutex and cond for commitrc sync */
    pthread_cond_t cond;
    int master_changed; /* set if we detect that node we were waiting for was
                           disconnected */
    int nops;

    int status;       /* poking support; status at the last check */
    int timestamp;    /* poking support: timestamp at the last check */
    int last_updated; /* poking support: when was the last time I got info, 0 is
                         never */
    int last_checked; /* poking support: when was the loast poke sent */
    struct sqlclntstate *clnt; /* cache clnt */
};

static osql_checkboard_t *checkboard = NULL;

int osql_checkboard_init(void)
{

    osql_checkboard_t *tmp = NULL;

    /* create repository if none */
    if (!checkboard) {

        tmp = (osql_checkboard_t *)calloc(1, sizeof(osql_checkboard_t));

        tmp->rqs = hash_init(sizeof(unsigned long long));
        if (!tmp->rqs) {
            free(tmp);
            logmsg(LOGMSG_ERROR, "%s: error init hash\n", __func__);
            return -1;
        }
        tmp->rqsuuid =
            hash_init_o(offsetof(osql_sqlthr_t, uuid), sizeof(uuid_t));

        if (pthread_rwlock_init(&tmp->rwlock, NULL)) {
            logmsg(LOGMSG_ERROR, "%s: error init pthread_rwlock_t\n", __func__);
            hash_free(tmp->rqs);
            free(tmp);
            return -1;
        }

        checkboard = tmp;
    }

    return 0;
}

/**
 * Destroy the checkboard
 * No more blocksql/socksql/recom/snapisol/serial threads can be created
 * after this.
 *
 */
void osql_checkboard_destroy(void) { /* TODO*/ }

/**
 * Register an osql thread with the checkboard
 * This allows block processor to query the status
 * of its sql peer
 *
 */
int _osql_register_sqlthr(struct sqlclntstate *clnt, int type, int is_remote)
{

    osql_sqlthr_t *entry = (osql_sqlthr_t *)calloc(sizeof(osql_sqlthr_t), 1);
    int rc = 0, rc2 = 0;
    int retry = 0;
    uuidstr_t us;

    if (!entry) {
        logmsg(LOGMSG_ERROR, "%s: unable to allocate %d bytes\n", __func__,
                sizeof(unsigned long long));
        return -1;
    }

    entry->rqid = clnt->osql.rqid;
    comdb2uuidcpy(entry->uuid, clnt->osql.uuid);
    entry->master = clnt->osql.host;
    entry->type = type;
    entry->last_checked = entry->last_updated =
        time_epoch(); /* initialize these to insert time */
    entry->clnt = clnt;

#ifdef DEBUG
    if (gbl_debug_sql_opcodes) {
        logmsg(LOGMSG_USER, "Registered %llx %s %d %d\n", entry->rqid,
                comdb2uuidstr(entry->uuid, us), entry->master, entry->type);
    }
#endif

    rc = pthread_mutex_init(&entry->mtx, NULL);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: error init mutex, rc=%d\n", __func__, rc);
        free(entry);
        return -1;
    }
    rc = pthread_cond_init(&entry->cond, NULL);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: error init cond, rc=%d\n", __func__, rc);
        pthread_mutex_destroy(&entry->mtx);
        free(entry);
        return -1;
    }

retry:

    /* insert entry */
    if ((rc = pthread_rwlock_wrlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_wrlock: error code %d\n", rc);
        return -1;
    }

    /* making sure we're adding the correct master */
    if (entry->master != thedb->master) {
        entry->master = clnt->osql.host = thedb->master;
        if (!entry->master) {
            if ((rc = pthread_rwlock_unlock(&checkboard->rwlock))) {
                logmsg(LOGMSG_ERROR, "pthread_rwlock_unlock: error code %d\n", rc);
                return -1;
            }
            if (retry < 60) /* 60*500 = 30 seconds */
            {
                poll(NULL, 0, 500);
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "No master, failed to register request\n");
            return -1;
        }
    }

    if (entry->master == gbl_mynode) {
        entry->master = clnt->osql.host = NULL; /* myself */
    }

    if (entry->rqid == OSQL_RQID_USE_UUID)
        rc2 = hash_add(checkboard->rqsuuid, entry);
    else
        rc2 = hash_add(checkboard->rqs, entry);

    if (rc2) {
        logmsg(LOGMSG_ERROR, "%s: error adding record %llx %s rc=%d\n", __func__,
                entry->rqid, comdb2uuidstr(entry->uuid, us), rc);
    }

    clnt->osql.sess_blocksock = entry;

    if ((rc = pthread_rwlock_unlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_unlock: error code %d\n", rc);
        return -1;
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

    return rc2;
}

/**
 * Register an osql thread with the checkboard
 * This allows block processor to query the status
 * of its sql peer
 *
 */
int osql_register_sqlthr(struct sqlclntstate *clnt, int type)
{
    return _osql_register_sqlthr(clnt, type, 0);
}

/**
 * Register a remote transaction, part of a distributed transaction
 *
 */
int osql_register_remtran(struct sqlclntstate *clnt, int type, char *tid)
{
    return _osql_register_sqlthr(clnt, type, 1);
}

/**
 * Unregister an osql thread from the checkboard
 * No further signalling for this thread is possible
 *
 */
int osql_unregister_sqlthr(struct sqlclntstate *clnt)
{
    osql_sqlthr_t *entry = NULL;
    int rc = 0, rc2 = 0;

    if (clnt->osql.rqid == 0)
        return 0;

    if ((rc = pthread_rwlock_wrlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_wrlock: error code %d\n", rc);
        return -1;
    }

    /* non-thread-safe hash_find() ok because we have the write (exclusive)
     * lock */
    if (clnt->osql.rqid == OSQL_RQID_USE_UUID)
        entry = hash_find(checkboard->rqsuuid, clnt->osql.uuid);
    else
        entry = hash_find(checkboard->rqs, &clnt->osql.rqid);
    if (!(entry)) {
        uuidstr_t us;
        logmsg(LOGMSG_ERROR, "%s: error unable to find record %llx %s\n", __func__,
                clnt->osql.rqid, comdb2uuidstr(clnt->osql.uuid, us));
    } else {

        if (clnt->osql.rqid == OSQL_RQID_USE_UUID) {
            rc2 = hash_del(checkboard->rqsuuid, entry);
            if (rc2)
                logmsg(LOGMSG_ERROR, "%s: unable to delete record %llx, rc=%d\n",
                        __func__, entry->rqid, rc2);
        } else {
            rc2 = hash_del(checkboard->rqs, entry);
            if (rc2) {
                uuidstr_t us;
                logmsg(LOGMSG_ERROR, "%s: unable to delete record %llx %s, rc=%d\n",
                        __func__, entry->rqid,
                        comdb2uuidstr(clnt->osql.uuid, us), rc2);
            }
        }

        clnt->osql.sess_blocksock = NULL;

        if (clnt->osql.logsb) {
            sbuf2close(clnt->osql.logsb);
            clnt->osql.logsb = NULL;
        }

#ifdef DEBUG
        if (gbl_debug_sql_opcodes) {
            uuidstr_t us;
            logmsg(LOGMSG_USER, "UNRegistered %llx %s %d %d\n", entry->rqid,
                    comdb2uuidstr(clnt->osql.uuid, us), entry->master,
                    entry->type);
        }
#endif

        /* free sql thread registration entry */
        pthread_cond_destroy(&entry->cond);
        pthread_mutex_destroy(&entry->mtx);
        free(entry);

        /*reset rqid */
        clnt->osql.rqid = 0;
    }

    if ((rc = pthread_rwlock_unlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_unlock: error code %d\n", rc);
        return -1;
    }

    return rc2;
}

/**
 * Checks the checkboard for sql session "rqid"
 * Returns:
 * - 1 is the session exists
 * - 0 if no session
 * - <0 if error
 *
 */
int osql_chkboard_sqlsession_exists(unsigned long long rqid, uuid_t uuid,
                                    int lock)
{

    osql_sqlthr_t *entry = NULL;
    int rc = 0, rc2;

    if (!checkboard)
        return 0;

    if (lock && (rc = pthread_rwlock_rdlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_wrlock: error code %d\n", rc);
        return -1;
    }

    if (rqid == OSQL_RQID_USE_UUID)
        rc2 = (entry = hash_find_readonly(checkboard->rqsuuid, uuid)) != 0;
    else
        rc2 = (entry = hash_find_readonly(checkboard->rqs, &rqid)) != 0;

    if (lock && (rc = pthread_rwlock_unlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_unlock: error code %d\n", rc);
        return -2;
    }

    return rc2;
}

int osql_chkboard_sqlsession_rc(unsigned long long rqid, uuid_t uuid, int nops,
                                void *data, struct errstat *errstat)
{

    osql_sqlthr_t *entry = NULL;
    int rc = 0, rc2;
    uuidstr_t us;

    if (!checkboard)
        return 0;

    if ((rc = pthread_rwlock_rdlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_wrlock: error code %d\n", rc);
        return -1;
    }

    if (rqid == OSQL_RQID_USE_UUID)
        entry = hash_find_readonly(checkboard->rqsuuid, uuid);
    else
        entry = hash_find_readonly(checkboard->rqs, &rqid);
    if (!entry) {
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
        rc2 = -1;

    } else {

        if (errstat)
            entry->err = *errstat;
        else
            bzero(&entry->err, sizeof(entry->err));

        if ((rc = pthread_mutex_lock(&entry->mtx))) {
            logmsg(LOGMSG_ERROR, "pthread_mutex_lock: error code %d\n", rc);
            return rc;
        }

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

        if ((rc = pthread_cond_signal(&entry->cond))) {
            logmsg(LOGMSG_ERROR, "pthread_cond_signal: error code %d\n", rc);
        }

        if ((rc = pthread_mutex_unlock(&entry->mtx))) {
            logmsg(LOGMSG_ERROR, "pthread_mutex_unlock: error code %d\n", rc);
            return rc;
        }

        rc2 = 0;
    }

    if ((rc = pthread_rwlock_unlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_unlock: error code %d\n", rc);
        return -2;
    }

    return rc2;
}

int osql_checkboard_check_request_down_node(void *obj, void *arg)
{
    char *host;
    osql_sqlthr_t *rq;
    uuidstr_t us;
    rq = (osql_sqlthr_t *)obj;
    host = (char *)arg;
    if (rq->master == host) {
        logmsg(LOGMSG_INFO, "signaling rq %llx %s\n", rq->rqid, comdb2uuidstr(rq->uuid, us));
        pthread_mutex_lock(&rq->mtx);
        rq->master_changed = 1;
        pthread_cond_signal(&rq->cond);
        pthread_mutex_unlock(&rq->mtx);
    }
    return 0;
}

void osql_checkboard_check_down_nodes(char *host)
{
    int rc;

    if (!checkboard)
        return;

    if ((rc = pthread_rwlock_rdlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_wrlock: error code %d\n", rc);
        return;
    }

    hash_for(checkboard->rqs, osql_checkboard_check_request_down_node, host);
    hash_for(checkboard->rqsuuid, osql_checkboard_check_request_down_node,
             host);

    if ((rc = pthread_rwlock_unlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_unlock: error code %d\n", rc);
        return;
    }
}

int osql_checkboard_check_request_master_changed(void *obj, void *arg)
{
    char *host = (char *)arg;
    uuidstr_t us;
    osql_sqlthr_t *rq;
    rq = (osql_sqlthr_t *)obj;
    if (rq->master != host) {
        if (gbl_master_swing_osql_verbose)
           logmsg(LOGMSG_ERROR, "signaling rq new master %llx %s\n", rq->rqid,
                   comdb2uuidstr(rq->uuid, us));
        pthread_mutex_lock(&rq->mtx);
        rq->master_changed = 1;
        pthread_cond_signal(&rq->cond);
        pthread_mutex_unlock(&rq->mtx);
    }
    return 0;
}

void osql_checkboard_check_master_changed(char *host)
{
    int rc;

    if (!checkboard)
        return;

    if ((rc = pthread_rwlock_rdlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_wrlock: error code %d\n", rc);
        return;
    }

    hash_for(checkboard->rqs, osql_checkboard_check_request_master_changed,
             host);
    hash_for(checkboard->rqsuuid, osql_checkboard_check_request_master_changed,
             host);

    if ((rc = pthread_rwlock_unlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_unlock: error code %d\n", rc);
        return;
    }
}

int osql_chkboard_longwait_commitrc(unsigned long long rqid, uuid_t uuid,
                                    struct errstat *xerr)
{
    return osql_chkboard_timedwait_commitrc(rqid, uuid, -1, xerr);
}

int osql_chkboard_wait_commitrc(unsigned long long rqid, uuid_t uuid,
                                struct errstat *xerr)
{
    return osql_chkboard_timedwait_commitrc(
        rqid, uuid,
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SOSQL_MAX_COMMIT_WAIT_SEC),
        xerr);
}

/**
 * Wait for the session to complete
 * Upon return, sqlclntstate's errstat is set
 *
 */
inline int osql_chkboard_timedwait_commitrc(unsigned long long rqid,
                                            uuid_t uuid, int max_wait,
                                            struct errstat *xerr)
{
    struct timespec tm_s;
    osql_sqlthr_t *entry = NULL;
    int rc = 0;
    int done = 0;
    int cnt = 0;
    int now;
    int poke_timeout;
    int poke_freq;
    uuidstr_t us;

    if (!checkboard)
        return 0;

    while (!done) {

        if ((rc = pthread_rwlock_rdlock(&checkboard->rwlock))) {
            logmsg(LOGMSG_ERROR, "pthread_rwlock_wrlock: error code %d\n", rc);
            return -1;
        }

        if (rqid == OSQL_RQID_USE_UUID)
            entry = hash_find_readonly(checkboard->rqsuuid, uuid);
        else
            entry = hash_find_readonly(checkboard->rqs, &rqid);
        if (!entry) {

            logmsg(LOGMSG_ERROR, "%s: received result for missing session %llu\n",
                    __func__, rqid);
            rc = -2;

            if ((rc = pthread_rwlock_unlock(&checkboard->rwlock))) {
                logmsg(LOGMSG_ERROR, "pthread_rwlock_unlock: error code %d\n", rc);
                return -3;
            }

            break;

        } else {

            if (entry->done == 1) {
                /* we are done */
                *xerr = entry->err;
                done = 1;
                if ((rc = pthread_rwlock_unlock(&checkboard->rwlock))) {
                    logmsg(LOGMSG_ERROR, "pthread_rwlock_unlock: error code %d\n",
                            rc);
                    return -3;
                }
                break;
            }
        }

        if ((rc = pthread_rwlock_unlock(&checkboard->rwlock))) {
            logmsg(LOGMSG_ERROR, "pthread_rwlock_unlock: error code %d\n", rc);
            return -4;
        }

        /* maximum wait-time */
        // max_wait = bdb_attr_get(thedb->bdb_attr,
        // BDB_ATTR_SOSQL_MAX_COMMIT_WAIT_SEC);

        if ((rc = pthread_mutex_lock(&entry->mtx))) {
            logmsg(LOGMSG_ERROR, "pthread_mutex_lock: error code %d\n", rc);
            return -5;
        }

        /* several conditions cause us to break out */
        while (entry->done != 1 && !entry->master_changed &&
               ((max_wait > 0 && cnt < max_wait) || max_wait < 0)) {

            /* wait for a second */
            clock_gettime(CLOCK_REALTIME, &tm_s);
            tm_s.tv_sec++;

            if ((rc = pthread_mutex_unlock(&entry->mtx))) {
                logmsg(LOGMSG_ERROR, "pthread_mutex_lock: error code %d\n", rc);
                return -5;
            }

            /* this call could wait for a bdb read lock; in the meantime,
               someone might try to signal us */
            if (osql_comm_check_bdb_lock()) {
                rc = pthread_mutex_unlock(&entry->mtx);
                if (rc)
                    logmsg(LOGMSG_ERROR, "pthread_mutex_unlock: error code %d\n",
                            rc);
                logmsg(LOGMSG_ERROR, "sosql: timed-out on bdb_lock_desired\n");

                return ERR_READONLY;
            }

            if ((rc = pthread_mutex_lock(&entry->mtx))) {
                logmsg(LOGMSG_ERROR, "pthread_mutex_lock: error code %d\n", rc);
                return -5;
            }

            if (entry->done == 1 || entry->master_changed)
                break;

            if ((rc = pthread_cond_timedwait(&entry->cond, &entry->mtx,
                                             &tm_s))) {
                if (ETIMEDOUT == rc) {
                    /* normal timeout .. */
                } else {
                    logmsg(LOGMSG_ERROR, "pthread_cond_timedwait: error code %d\n",
                            rc);
                    return -7;
                }
            }

            cnt++;
            // printf("cnt %d/%d done %d master_changed %d pokebit %d\n", cnt,
            // max_wait, entry->done, entry->master_changed);

            /* we got back the mutex, are we there yet ? */
            if (entry->done == 1 || entry->master_changed ||
                (max_wait > 0 && cnt >= max_wait))
                break;

            poke_timeout =
                bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SOSQL_POKE_TIMEOUT_SEC);
            poke_freq =
                bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SOSQL_POKE_FREQ_SEC);

            /* is it the time to check the master? have we already done so? */
            now = time_epoch();

            if ((poke_timeout > 0) &&
                (entry->last_updated + poke_timeout < now)) {
                /* timeout the request */
                logmsg(LOGMSG_ERROR, 
                        "Master %s failed to acknowledge session %llu %s\n",
                        entry->master, entry->rqid, comdb2uuidstr(entry->uuid, us));
                entry->done = 1;
                xerr->errval = entry->err.errval = SQLHERR_MASTER_TIMEOUT;
                snprintf(entry->err.errstr, sizeof(entry->err.errstr),
                         "master %s lost transaction %llu", entry->master,
                         entry->rqid);
                rc = 0;
                break;
            }

            if ((poke_freq > 0) && (entry->last_checked + poke_freq < now)) {
                /* try poke again */
                if (entry->master == 0 || entry->master == gbl_mynode) {
                    /* local checkup */
                    rc = osql_repository_session_exists(entry->rqid,
                                                        entry->uuid);
                    if (!rc) {
                        logmsg(LOGMSG_ERROR, "Local SORESE failed to find local "
                                        "transaction %llu %s\n",
                                entry->rqid, comdb2uuidstr(entry->uuid, us));
                        entry->done = 1;
                        xerr->errval = entry->err.errval =
                            SQLHERR_MASTER_TIMEOUT;
                        snprintf(entry->err.errstr, sizeof(entry->err.errstr),
                                 "Local transaction failed, unable to locate "
                                 "entry id=%llu",
                                 entry->rqid);
                        rc = 0; /* fail normally */
                        break;
                    } else {
                        entry->last_updated = now;
                    }
                } else {
                    rc =
                        osql_comm_send_poke(entry->master, entry->rqid,
                                            entry->uuid, NET_OSQL_MASTER_CHECK);
                    if (rc) {
                        logmsg(LOGMSG_ERROR, "Failed to send master check rc=%d\n",
                                rc);
                        entry->done = 1;
                        xerr->errval = entry->err.errval =
                            SQLHERR_MASTER_TIMEOUT;
                        snprintf(entry->err.errstr, sizeof(entry->err.errstr),
                                 "failed comm with master %s entry id=%llu %s",
                                 entry->master, entry->rqid,
                                 comdb2uuidstr(entry->uuid, us));
                        rc = 0; /* fail normally */
                        break;
                    }
                }

                entry->last_checked = now;
            }
        }

        if ((max_wait > 0 && cnt >= max_wait)) {
            rc = pthread_mutex_unlock(&entry->mtx);
            if (rc)
                logmsg(LOGMSG_ERROR, "pthread_mutex_unlock: error code %d\n", rc);
            fprintf(stderr,
                    "sosql: timed-out waiting for commit from master\n");
            return -6;
        }

        if (entry->master_changed) {
            rc = pthread_mutex_unlock(&entry->mtx);
            if (rc)
                logmsg(LOGMSG_ERROR, "pthread_mutex_unlock: error code %d\n", rc);
            rc = 0; /* retry at higher level */
            xerr->errval = ERR_NOMASTER;
            if (gbl_master_swing_osql_verbose)
                logmsg(LOGMSG_ERROR, "sosql: master changed %d\n", pthread_self());
            goto done;
        }

        if ((rc = pthread_mutex_unlock(&entry->mtx))) {
            logmsg(LOGMSG_ERROR, "pthread_mutex_unlock: error code %d\n", rc);
            return -6;
        }

    } /* done */

done:

    return rc;
}

/**
 * Update status of the pending sorese transaction, to support poking
 *
 */
int osql_checkboard_update_status(unsigned long long rqid, uuid_t uuid,
                                  int status, int timestamp)
{
    osql_sqlthr_t *entry = NULL;
    int rc, rc2;
    uuidstr_t us;

    if (!checkboard)
        return 0;

    if ((rc = pthread_rwlock_rdlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_wrlock: error code %d\n", rc);
        return -1;
    }

    if (rqid == OSQL_RQID_USE_UUID)
        entry = hash_find_readonly(checkboard->rqsuuid, uuid);
    else
        entry = hash_find_readonly(checkboard->rqs, &rqid);
    if (!entry) {
        ctrace("%s: SORESE received exists for missing session %llu %s\n",
               __func__, rqid, comdb2uuidstr(uuid, us));
        rc2 = -1;

    } else {

        if (rc = pthread_mutex_lock(&entry->mtx)) {
            logmsg(LOGMSG_ERROR, "pthread_mutex_lock: error code %d\n", rc);
            return rc;
        }

        entry->status = status;
        entry->timestamp = timestamp;
        entry->last_updated = time_epoch();

        if (rc = pthread_mutex_unlock(&entry->mtx)) {
            logmsg(LOGMSG_ERROR, "pthread_mutex_unlock: error code %d\n", rc);
            return rc;
        }

        rc2 = 0;
    }

    if ((rc = pthread_rwlock_unlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_unlock: error code %d\n", rc);
        return -1;
    }

    return rc2;
}

/**
 * Reset fields when a session is retried
 * we're interested in things like master_changed
 *
 */
int osql_reuse_sqlthr(struct sqlclntstate *clnt)
{
    osql_sqlthr_t *entry = NULL;
    int rc = 0, rc2 = 0;

    if (clnt->osql.rqid == 0)
        return 0;

    if ((rc = pthread_rwlock_wrlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_wrlock_rdlock: error code %d\n", rc);
        return -1;
    }

    if (clnt->osql.rqid == OSQL_RQID_USE_UUID) {
        entry = hash_find_readonly(checkboard->rqsuuid, clnt->osql.uuid);
    } else {
        entry = hash_find_readonly(checkboard->rqs, &clnt->osql.rqid);
    }

    if (!entry) {
        uuidstr_t us;
        logmsg(LOGMSG_ERROR, "%s: error unable to find record %llx %s\n", __func__,
                clnt->osql.rqid, comdb2uuidstr(clnt->osql.uuid, us));
        rc2 = -1;
    } else {
        pthread_mutex_lock(&entry->mtx);
        entry->done = 0;
        entry->master_changed = 0;
        entry->master = thedb->master; /* master changed, store it here */
        bzero(&entry->err, sizeof(entry->err));
        pthread_mutex_unlock(&entry->mtx);
    }

    if ((rc = pthread_rwlock_unlock(&checkboard->rwlock))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_unlock: error code %d\n", rc);
        return -1;
    }

    return rc2;
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
    osql_sqlthr_t *entry = NULL;
    int rc = 0, rc2;

    if (!checkboard)
        return 0;

    if (rc = pthread_rwlock_rdlock(&checkboard->rwlock)) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_wrlock: error code %d\n", rc);
        return -1;
    }

    entry = hash_find_readonly(h, k);
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

        rc2 = -1;

    } else {

        *clnt = entry->clnt;

        /* Need to lock this guy out
           This prevents them from getting freed upon commit
           before a new cursor is recorded */
        pthread_mutex_lock(&(*clnt)->dtran_mtx);

        rc2 = 0;
    }

    if (rc = pthread_rwlock_unlock(&checkboard->rwlock)) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_unlock: error code %d\n", rc);
        return -2;
    }

    return rc2;
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
