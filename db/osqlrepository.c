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
 * This defines the osql repository
 * It is maintain on the master and contains
 * all pending blocksql/socksql/recom/snapisol/serial sessions
 */
#include <poll.h>

#include "osqlrepository.h"
#include "osqlsession.h"
#include "osqlcomm.h"
#include "comdb2uuid.h"
#include <alloca.h>
#include <logmsg.h>

struct osql_repository {

    hash_t *rqs; /* hash of outstanding requests */
    hash_t *rqsuuid;
    pthread_rwlock_t hshlck; /* protect the hash */

    int cancelall; /* set this if we want to prevent new blocksqls */
    pthread_mutex_t cancelall_mtx; /* cancelall mutex */

    struct dbenv *dbenv; /* dbenv */
};

static osql_repository_t *theosql = NULL;

/**
 * Init repository
 * Returns 0 if success
 */
int osql_repository_init(void)
{

    osql_repository_t *tmp = NULL;

    tmp = (osql_repository_t *)calloc(sizeof(osql_repository_t), 1);
    if (!tmp) {
        logmsg(LOGMSG_ERROR, "%s: cannot allocate %d bytes\n", __func__,
                sizeof(osql_repository_t));
        return -1;
    }

    if (pthread_mutex_init(&tmp->cancelall_mtx, NULL)) {
        logmsg(LOGMSG_ERROR, "%s: unable to initialize the mutex\n", __func__);
        free(tmp);
        return -1;
    }

    if (pthread_rwlock_init(&tmp->hshlck, NULL)) {
        logmsg(LOGMSG_ERROR, "%s: unable to initialize the rwlock\n", __func__);
        pthread_mutex_destroy(&tmp->cancelall_mtx);
        free(tmp);
        return -1;
    }

    /* init the client hash */
    tmp->rqs = hash_init(sizeof(unsigned long long)); /* indexed after rqid */
    tmp->rqsuuid = hash_init_o(offsetof(osql_sess_t, uuid), sizeof(uuid_t));

    if (!tmp->rqs) {
        logmsg(LOGMSG_ERROR, "%s: unable to create hash\n", __func__);
        pthread_mutex_destroy(&tmp->cancelall_mtx);
        pthread_rwlock_destroy(&tmp->hshlck);
        free(tmp);
        return -1;
    }

    theosql = tmp;

    return 0;
}

/**
 * Destroy repository
 * Returns 0 if success
 */
void osql_repository_destroy(void)
{

    osql_repository_t *tmp = theosql;

    if (!tmp)
        return;

    logmsg(LOGMSG_INFO, "%s: destroying the repository for %p\n", __func__, theosql);

    theosql = NULL;

    pthread_mutex_destroy(&tmp->cancelall_mtx);
    pthread_rwlock_destroy(&tmp->hshlck);
    if (tmp->rqs)
        hash_free(tmp->rqs);
    if (tmp->rqsuuid)
        hash_free(tmp->rqsuuid);
    free(tmp);
}

static int theosql_pthread_rwlock_unlock(pthread_rwlock_t *lk)
{
    return pthread_rwlock_unlock(lk);
}

#define MAX_UUID_LIST 1000000
static uuid_t add_uuid_list[MAX_UUID_LIST];
static unsigned long long add_uuid_order[MAX_UUID_LIST];
static unsigned long long total_ordering = 0;
static int add_current_uuid = 0;

static char hex(unsigned char a)
{
    if (a < 10)
        return '0' + a;
    return 'a' + (a - 10);
}

/* Return a hex string */
static char *tohex(char *output, char *key, int keylen)
{
    int i = 0;
    char byte[3];

    output[0] = '\0';
    byte[2] = '\0';

    for (i = 0; i < keylen; i++) {
        snprintf(byte, sizeof(byte), "%c%c", hex(((unsigned char)key[i]) / 16),
                 hex(((unsigned char)key[i]) % 16));
        strcat(output, byte);
    }

    return output;
}

/**
 * Adds an osql session to the repository
 * Returns 0 on success
 */
int osql_repository_add(osql_sess_t *sess)
{
    osql_sess_t *sess_chk, *sess_chk2;
    uuid_t uuid;
    unsigned long long rqid;
    int rc = 0;
    int poll_msec = 100;
    int total_time_msec = 30 * 1000;
    int crt_time_msec = 0;

retry:

    /* insert it into the hash table */
    if ((rc = pthread_rwlock_wrlock(&theosql->hshlck))) {
        logmsg(LOGMSG_ERROR, "%s:pthread_rwlock_wrlock error code %d\n", __func__,
                rc);
        return -1;
    }

    /*
    Must check the cancelled flag under hshlck:

    schema-change code sets the cancelall flag which is intended to short-
    circuit incoming requests in block2_sql code.  It then grabs hshlck, &
    sets the "terminate" flag for all existing osql sessions.  The race occurs
    if the block2_sql flag is set after a session checks this in osql, and
    the terminate flag is set before that session is added.
    */

    if (osql_repository_cancelled()) {
        logmsg(LOGMSG_WARN, "%s: osql session cancelled due to schema change\n",
                __func__);
        theosql_pthread_rwlock_unlock(&theosql->hshlck);
        return RC_INTERNAL_RETRY; /* POSITIVE */
    }

    /* how about we check if this session is added again due to an early replay
     */
    rqid = osql_sess_getrqid(sess);
    osql_sess_getuuid(sess, uuid);
    if (rqid == OSQL_RQID_USE_UUID)
        sess_chk = hash_find_readonly(theosql->rqsuuid, &uuid);
    else {
        sess_chk = hash_find_readonly(theosql->rqs, &rqid);
    }
    if (sess_chk) {
        theosql_pthread_rwlock_unlock(&theosql->hshlck);
        char *p = (char *)alloca(64);
        p = tohex(p, uuid, 16);

        logmsg(LOGMSG_ERROR, "%s: trying to add another session with the same "
                "rqid, rqid=%llx uuid=%s retry=%d, waited %u msec\n",
                __func__, sess->rqid, p, crt_time_msec / poll_msec, crt_time_msec);

        if (crt_time_msec + poll_msec < total_time_msec) {
            crt_time_msec += poll_msec;
            poll(NULL, 0, poll_msec);
            goto retry;
        }

        logmsg(LOGMSG_WARN, "%s: timed out waiting for session with same rqid to complete, rqid=%llx uuid=%s\n", 
                __func__, sess->rqid, p);

        return RC_INTERNAL_RETRY;
        //abort();
    }

    if (sess->rqid == OSQL_RQID_USE_UUID)
        rc = hash_add(theosql->rqsuuid, sess);
    else
        rc = hash_add(theosql->rqs, sess);

    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: Unable to hash the new request\n", __func__);
        theosql_pthread_rwlock_unlock(&theosql->hshlck);
        return -2;
    }

    memcpy(add_uuid_list[add_current_uuid], sess->uuid, sizeof(add_uuid_list[0]));
    add_uuid_order[add_current_uuid] = total_ordering++;
    add_current_uuid = ((add_current_uuid + 1) % MAX_UUID_LIST);

    if ((rc = theosql_pthread_rwlock_unlock(&theosql->hshlck))) {
        logmsg(LOGMSG_ERROR, "%s: pthread_rwlock_unlock error code %d\n", __func__,
                rc);
        return -3;
    }

    return 0;
}


/**
 * Remove an osql session from the repository
 * return 0 on success
 */
int osql_repository_rem(osql_sess_t *sess, int lock, const char *func, const char *callfunc, int line)
{

    int rc = 0;

    if (lock) {
        if (rc = pthread_rwlock_wrlock(&theosql->hshlck)) {
            logmsg(LOGMSG_ERROR, "%s:pthread_rwlock_wrlock error code %d\n",
                    __func__, rc);
            return -1;
        }
    }

    if (sess->rqid == OSQL_RQID_USE_UUID) {
        rc = hash_del(theosql->rqsuuid, sess);
    } else {
        rc = hash_del(theosql->rqs, sess);
    }

    static uuid_t uuid_list[MAX_UUID_LIST];
    static const char *uuid_func[MAX_UUID_LIST];
    static const char *uuid_callfunc[MAX_UUID_LIST];
    static int uuid_callfunc_line[MAX_UUID_LIST];
    static unsigned long long uuid_order[MAX_UUID_LIST];
    static int current_uuid = 0;
    int found_uuid = 0;

    if (rc) {
        char *p = alloca(64);
        p = (char *)tohex(p, sess->uuid, 16);
        logmsg(LOGMSG_ERROR, "%s: Unable to hash the new request\n", __func__); 
        for (int i=0; i<MAX_UUID_LIST;i++) {

            if (!memcmp(sess->uuid, add_uuid_list[i], sizeof(add_uuid_list[0]))) {
                logmsg(LOGMSG_ERROR, "uuid %s was added to the repository (index %d) total_order=%llu\n", p, i, 
                        add_uuid_order[i]);
                found_uuid++;
            }

            if (!memcmp(sess->uuid, uuid_list[i], sizeof(uuid_list[0]))) {
                logmsg(LOGMSG_ERROR, "uuid %s was removed from repository %s (index %d) callfunc %s line %d total_order=%llu\n", 
                        p, uuid_func[i], i, uuid_callfunc[i]==NULL?"(none)":uuid_callfunc[i], uuid_callfunc_line[i], 
                        uuid_order[i]);
                found_uuid++;
            }
        }

        if (!found_uuid) {
            logmsg(LOGMSG_ERROR, "%s unable to find previous uuid %s\n", __func__, p);
        }
        else {
            logmsg(LOGMSG_ERROR, "%s found %s %d times in tracking array\n", __func__, 
                    p, found_uuid);
        }

        if (lock) {
            theosql_pthread_rwlock_unlock(&theosql->hshlck);
        }

        abort();
    }
    else {
        memcpy(uuid_list[current_uuid], sess->uuid, sizeof(uuid_list[0]));
        uuid_func[current_uuid] = func;
        uuid_callfunc[current_uuid] = callfunc;
        uuid_callfunc_line[current_uuid] = line;
        uuid_order[current_uuid] = total_ordering++;
        current_uuid = ((current_uuid + 1) % MAX_UUID_LIST);
    }

    if (lock) {
        if ((rc = theosql_pthread_rwlock_unlock(&theosql->hshlck))) {
            logmsg(LOGMSG_ERROR, "%s: pthread_rwlock_unlock error code %d\n",
                    __func__, rc);
            return -3;
        }
    }

    return 0;
}

int pthread_rwlock_rdlock_check(pthread_rwlock_t *lk, const char *func, uint32_t line) 
{
    return pthread_rwlock_rdlock(lk);
}

/**
 * Retrieves a session based on rqid
 * Increments the users to prevent premature
 * deletion
 */
osql_sess_t *osql_repository_get(unsigned long long rqid, uuid_t uuid,
                                 int keep_repository_lock)
{

    osql_sess_t *sess = NULL;
    int rc = 0;

    if ((rc = pthread_rwlock_rdlock_check(&theosql->hshlck, __func__, __LINE__))) {
        logmsg(LOGMSG_ERROR, "%s:pthread_rwlock_rdlock error code %d\n", __func__,
                rc);
        return NULL;
    }

    if (rqid == OSQL_RQID_USE_UUID)
        /* hash_find can modify the ordering of the hash chains, so is not
         * threadsafe; use hash_find_readonly() */
        sess = hash_find_readonly(theosql->rqsuuid, uuid);
    else
        sess = hash_find_readonly(theosql->rqs, &rqid);

    /* register the new receiver; osql_close_req will wait for to finish storing
     * the message */
    if (sess) {
        osql_sess_addclient(sess);
    }

    if (!(sess && keep_repository_lock)) {
        if ((rc = theosql_pthread_rwlock_unlock(&theosql->hshlck))) {
            logmsg(LOGMSG_ERROR, "%s:pthread_rwlock_unlock error code %d\n",
                    __func__, rc);
            return NULL;
        }
    }

    return sess;
}

/**
 * Decrements the number of users
 * Returns 0 if success
 */
int osql_repository_put(osql_sess_t *sess, int release_repository_lock)
{
    int ret = 0;
    int rc = 0;

    ret = osql_sess_remclient(sess);

    if (release_repository_lock) {
        if (rc = theosql_pthread_rwlock_unlock(&theosql->hshlck)) {
            logmsg(LOGMSG_ERROR, "%s:pthread_rwlock_unlock error code %d\n",
                    __func__, rc);
            return -1;
        }
    }

    return ret;
}

/**
 * Enable/disable osql sessions
 *
 */
void osql_set_cancelall(int disable)
{

    if (theosql) {
        pthread_mutex_lock(&theosql->cancelall_mtx);
        theosql->cancelall = disable;
        pthread_mutex_unlock(&theosql->cancelall_mtx);

        if (disable)
            osql_repository_cancelall();
    }
}

/**
 * Print info about pending osql sessions
 *
 */
int osql_repository_printcrtsessions(void)
{

    osql_repository_t *stat = theosql;
    osql_sess_t *rq = NULL;
    int rc = 0;

    if (!stat) {
        if (gbl_ready) {
            logmsg(LOGMSG_ERROR, "%s: called w/ NULL stat\n", __func__);
            return -1;
        }

        return 0;
    }

    if ((rc = pthread_rwlock_rdlock_check(&stat->hshlck, __func__, __LINE__))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_rdlock: error code %d\n", rc);
        return -1;
    }

    logmsg(LOGMSG_USER, "Begin osql session info:\n");
    if ((rc = hash_for(stat->rqs, osql_sess_getcrtinfo, NULL))) {
        logmsg(LOGMSG_USER, "hash_for failed with rc = %d\n", rc);
        pthread_rwlock_unlock(&stat->hshlck);
        return -1;
    }
    logmsg(LOGMSG_USER, "Done osql info.\n");

    if ((rc = pthread_rwlock_unlock(&stat->hshlck))) {
        logmsg(LOGMSG_ERROR, "pthread_rwlock_unlock: error code %d\n", rc);
        return -1;
    }

    return 0;
}

/**
 * Disable temporarily replicant "node"
 * It will receive no more offloading requests
 * until a blackout window will expire
 * It is used mainly with blocksql
 *
 */
int osql_repository_blkout_node(char *host)
{

    osql_repository_t *stat = theosql;
    int outrc = 0;
    int rc = 0;

    if ((rc = pthread_rwlock_wrlock(&theosql->hshlck))) {
        logmsg(LOGMSG_ERROR, "%s: pthread_mutex_lock error %d\n", __func__, rc);
        return -1;
    }

    outrc = osql_comm_blkout_node(host);

    if ((rc = theosql_pthread_rwlock_unlock(&theosql->hshlck))) {
        logmsg(LOGMSG_ERROR, "%s: pthread_mutex_unlock error %d\n", __func__, rc);
        return -1;
    }

    return outrc;
}

/**
 * Go through all the sessions executing on node
 * "node" and mark them "terminate", which cancel
 * them.
 * Used when a node is down.
 * If "node" is 0, all sessions are terminated.
 *
 */
int osql_repository_terminatenode(char *host)
{

    osql_repository_t *stat = theosql;
    int rc = 0;

    if (!stat) {
        if (gbl_ready) {
            logmsg(LOGMSG_ERROR, "%s: called w/ NULL stat\n", __func__);
            return -1;
        }

        return 0;
    }

    /* insert it into the hash table */
    if ((rc = pthread_rwlock_wrlock(&theosql->hshlck))) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock: error code %d\n", rc);
        return -1;
    }

    if ((rc = hash_for(theosql->rqs, osql_session_testterminate, host))) {
        logmsg(LOGMSG_ERROR, "hash_for failed with rc = %d\n", rc);
        pthread_rwlock_unlock(&theosql->hshlck);
        return -1;
    }
    if ((rc = hash_for(theosql->rqsuuid, osql_session_testterminate, host))) {
        logmsg(LOGMSG_ERROR, "hash_for failed with rc = %d\n", rc);
        pthread_rwlock_unlock(&theosql->hshlck);
        return -1;
    }

    /* blackout node
       this will prevent also active osql threads from sending requests in vain
    */
    if ((rc = osql_comm_blkout_node(host))) {
        logmsg(LOGMSG_ERROR, "fail to blackout node %s\n", host);
        theosql_pthread_rwlock_unlock(&theosql->hshlck);
        return -1;
    }

    if ((rc = theosql_pthread_rwlock_unlock(&theosql->hshlck))) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_unlock: error code %d\n", rc);
        return -1;
    }

    return 0;
}

/**
 * Cancel all pending osql block processor
 * transactions
 *
 */
int osql_repository_cancelall(void) { return osql_repository_terminatenode(0); }

/**
 * Returns true if all requests are being
 * cancelled (this is usually done because
 * of a schema change)
 *
 */
int osql_repository_cancelled(void)
{

    int cancelall = 0;

    /* Becomes null when the db is exiting. */
    if (!theosql)
        return 1;
    pthread_mutex_lock(&theosql->cancelall_mtx);
    if (theosql->cancelall)
        cancelall = 1;
    pthread_mutex_unlock(&theosql->cancelall_mtx);

    return cancelall;
}

/**
 * Returns 1 if the session exists
 * used by socksql poking
 *
 */
int osql_repository_session_exists(unsigned long long rqid, uuid_t uuid)
{
    osql_sess_t *sess = NULL;
    int rc = 0;
    int out_rc = 0;

    if ((rc = pthread_rwlock_rdlock_check(&theosql->hshlck, __func__, __LINE__))) {
        logmsg(LOGMSG_ERROR, "%s:pthread_rwlock_rdlock error code %d\n", __func__,
                rc);
        return 0;
    }

    if (rqid == OSQL_RQID_USE_UUID)
        /* hash_find can modify the ordering of the hash chains, so is not
         * threadsafe; use hash_find_readonly() */
        sess = hash_find_readonly(theosql->rqsuuid, uuid);
    else
        sess = hash_find_readonly(theosql->rqs, &rqid);

    /* register the new receiver; osql_close_req will wait for to finish storing
     * the message */
    out_rc = !!sess;

    if ((rc = theosql_pthread_rwlock_unlock(&theosql->hshlck))) {
        logmsg(LOGMSG_ERROR, "%s:pthread_rwlock_unlock error code %d\n", __func__,
                rc);
        return 0;
    }

    return out_rc;
}
