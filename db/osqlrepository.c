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
#include "logmsg.h"
#include "locks_wrap.h"
#include "tohex.h"
#include "comdb2_atomic.h"

struct osql_repository {

    hash_t *rqs; /* hash of outstanding requests */
    hash_t *rqsuuid;
    pthread_rwlock_t hshlck; /* protect the hash */

    int cancelall; /* set this if we want to prevent new blocksqls */

    struct dbenv *dbenv; /* dbenv */
};

static osql_repository_t *theosql_obj = NULL;

static inline osql_repository_t *get_theosql(void)
{
    return theosql_obj;
}

/**
 * Init repository
 * Returns 0 if success
 */
int osql_repository_init(void)
{

    osql_repository_t *tmp = NULL;

    tmp = (osql_repository_t *)calloc(sizeof(osql_repository_t), 1);
    if (!tmp) {
        logmsg(LOGMSG_ERROR, "%s: cannot allocate %zu bytes\n", __func__,
               sizeof(osql_repository_t));
        return -1;
    }

    Pthread_rwlock_init(&tmp->hshlck, NULL);

    /* init the client hash */
    tmp->rqs = hash_init(sizeof(unsigned long long)); /* indexed after rqid */
    tmp->rqsuuid = hash_init_o(offsetof(osql_sess_t, uuid), sizeof(uuid_t));

    if (!tmp->rqs) {
        logmsg(LOGMSG_ERROR, "%s: unable to create hash\n", __func__);
        Pthread_rwlock_destroy(&tmp->hshlck);
        free(tmp);
        return -1;
    }

    theosql_obj = tmp;

    return 0;
}

/**
 * Destroy repository
 * Returns 0 if success
 */
void osql_repository_destroy(void)
{
    theosql_obj = NULL;
}

#ifdef TRACK_OSQL_SESSIONS
#define MAX_UUID_LIST 1000000
static uuid_t add_uuid_list[MAX_UUID_LIST];
static unsigned long long add_uuid_order[MAX_UUID_LIST];
static unsigned long long total_ordering = 0;
static int add_current_uuid = 0;
#endif

static char hex(unsigned char a)
{
    if (a < 10)
        return '0' + a;
    return 'a' + (a - 10);
}


/**
 * Adds an osql session to the repository
 * Returns 0 on success
 */
int osql_repository_add(osql_sess_t *sess, int *replaced)
{
    osql_sess_t *sess_chk;
    uuid_t uuid;
    unsigned long long rqid;
    int rc = 0;
    osql_repository_t *theosql = get_theosql();
    if (theosql == NULL) {
        return -1;
    }

    /* insert it into the hash table */
    Pthread_rwlock_wrlock(&theosql->hshlck);

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
        Pthread_rwlock_unlock(&theosql->hshlck);
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
        char *p = (char *)alloca(64);
        p = util_tohex(p, (char *)uuid, 16);

        logmsg(LOGMSG_ERROR,
               "%s: trying to add another session with the same rqid, "
               "rqid=%llx uuid=%s\n",
               __func__, sess->rqid, p);
        /* dont run the new replace logic for tag and osql retry */
        if (replaced == NULL)
            abort();

        rc = osql_sess_try_terminate(sess_chk);
        if (rc < 0) {
            logmsg(LOGMSG_ERROR, "%s:%d osql_sess_try_terminate rc %d\n",
                   __func__, __LINE__, rc);
            Pthread_rwlock_unlock(&theosql->hshlck);
            return -1;
        }
        if (rc == 0) {
            /* old request was terminated successfully, let's add the new one */
            logmsg(LOGMSG_INFO,
                   "%s: cancelled old request for rqid=%llx, uuid=%s\n",
                   __func__, sess->rqid, p);
        } else {
            /* old request was already processed, ignore new ones */
            Pthread_rwlock_unlock(&theosql->hshlck);
            *replaced = 1;
            logmsg(LOGMSG_INFO,
                   "%s: rqid=%llx, uuid=%s was completed/dispatched\n",
                   __func__, sess->rqid, p);
            return 0;
        }
    }

    if (sess->rqid == OSQL_RQID_USE_UUID)
        rc = hash_add(theosql->rqsuuid, sess);
    else
        rc = hash_add(theosql->rqs, sess);

    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: Unable to hash_add the new request\n",
               __func__);
        rc = -2;
    }
#ifdef TRACK_OSQL_SESSIONS
    else {
        memcpy(add_uuid_list[add_current_uuid], sess->uuid,
               sizeof(add_uuid_list[0]));
        add_uuid_order[add_current_uuid] = total_ordering++;
        add_current_uuid = ((add_current_uuid + 1) % MAX_UUID_LIST);
    }
#endif

    Pthread_rwlock_unlock(&theosql->hshlck);

    return rc;
}

int gbl_abort_on_missing_osql_session = 0;

/**
 * Remove an osql session from the repository
 * return 0 on success
 */
int osql_repository_rem(osql_sess_t *sess, int lock, const char *func, const char *callfunc, int line)
{
    osql_repository_t *theosql = get_theosql();
    if (theosql == NULL) {
        return -1;
    }
    int rc = 0;

    if (lock) {
        Pthread_rwlock_wrlock(&theosql->hshlck);
    }

    if (sess->rqid == OSQL_RQID_USE_UUID) {
        rc = hash_del(theosql->rqsuuid, sess);
    } else {
        rc = hash_del(theosql->rqs, sess);
    }

#ifdef TRACK_OSQL_SESSIONS
    static uuid_t uuid_list[MAX_UUID_LIST];
    static const char *uuid_func[MAX_UUID_LIST];
    static const char *uuid_callfunc[MAX_UUID_LIST];
    static int uuid_callfunc_line[MAX_UUID_LIST];
    static unsigned long long uuid_order[MAX_UUID_LIST];
    static int current_uuid = 0;
    int found_uuid = 0;
#endif

    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: Unable to hash_del the new request\n",
               __func__);
#ifdef TRACK_OSQL_SESSIONS
        char *p = alloca(64);
        p = (char *)util_tohex(p, (char *)sess->uuid, 16);
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
#endif

        if (lock) {
            Pthread_rwlock_unlock(&theosql->hshlck);
            lock = 0;
        }

        /* This can happen legitimately on master swing */
        if (gbl_abort_on_missing_osql_session)
            abort();
    }
#ifdef TRACK_OSQL_SESSIONS
    else {
        memcpy(uuid_list[current_uuid], sess->uuid, sizeof(uuid_list[0]));
        uuid_func[current_uuid] = func;
        uuid_callfunc[current_uuid] = callfunc;
        uuid_callfunc_line[current_uuid] = line;
        uuid_order[current_uuid] = total_ordering++;
        current_uuid = ((current_uuid + 1) % MAX_UUID_LIST);
    }
#endif

    if (lock) {
        Pthread_rwlock_unlock(&theosql->hshlck);
    }

    return 0;
}

/**
 * Retrieves a session based on rqid
 * Increments the users to prevent premature
 * deletion
 */
osql_sess_t *osql_repository_get(unsigned long long rqid, uuid_t uuid,
                                 int keep_repository_lock)
{
    osql_repository_t *theosql = get_theosql();
    if (theosql == NULL) {
        return NULL;
    }
    osql_sess_t *sess = NULL;
    Pthread_rwlock_rdlock(&theosql->hshlck);

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

    /* NB: if session was not found we unlock */
    if (!(sess && keep_repository_lock)) {
        Pthread_rwlock_unlock(&theosql->hshlck);
    }

    return sess;
}

/**
 * Decrements the number of users
 * Returns 0 if success
 */
int osql_repository_put(osql_sess_t *sess, int release_repository_lock)
{
    osql_repository_t *theosql = get_theosql();
    if (theosql == NULL) {
        return -1;
    }
    int ret = osql_sess_remclient(sess);

    if (release_repository_lock) {
        Pthread_rwlock_unlock(&theosql->hshlck);
    }

    return ret;
}

/**
 * Enable/disable osql sessions
 *
 */
void osql_set_cancelall(int disable)
{
    osql_repository_t *theosql = get_theosql();
    if (!theosql)
        return;

    XCHANGE32(theosql->cancelall, disable);
    if (disable)
        osql_repository_cancelall();
}

/**
 * Print info about pending osql sessions
 *
 */
int osql_repository_printcrtsessions(void)
{
    osql_repository_t *stat = get_theosql();
    int rc = 0;
    int maxops = 0;

    if (!stat) {
        if (gbl_ready) {
            logmsg(LOGMSG_ERROR, "%s: called w/ NULL stat\n", __func__);
            return -1;
        }

        return 0;
    }

    maxops = get_osql_maxtransfer();
    logmsg(LOGMSG_USER, "Maximum transaction size: %d bplog entries\n", maxops);

    Pthread_rwlock_rdlock(&stat->hshlck);

    logmsg(LOGMSG_USER, "Begin osql session info:\n");
    if ((rc = hash_for(stat->rqs, osql_sess_getcrtinfo, NULL))) {
        logmsg(LOGMSG_USER, "hash_for failed with rc = %d\n", rc);
        rc = -1;
    } else
        logmsg(LOGMSG_USER, "Done osql info.\n");

    Pthread_rwlock_unlock(&stat->hshlck);

    return rc;
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
    osql_repository_t *theosql = get_theosql();
    if (theosql == NULL) {
        return -1;
    }
    int outrc = 0;
    Pthread_rwlock_wrlock(&theosql->hshlck);

    outrc = osql_comm_blkout_node(host);

    Pthread_rwlock_unlock(&theosql->hshlck);

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
    osql_repository_t *stat = get_theosql();
    int rc = 0;
    if (!stat) {
        if (gbl_ready) {
            logmsg(LOGMSG_ERROR, "%s: called w/ NULL stat\n", __func__);
            return -1;
        }

        return 0;
    }

    osql_repository_t *theosql = stat;

    /* insert it into the hash table */
    Pthread_rwlock_wrlock(&theosql->hshlck);

    if ((rc = hash_for(theosql->rqs, osql_session_testterminate, host))) {
        logmsg(LOGMSG_ERROR, "hash_for failed with rc = %d\n", rc);
        Pthread_rwlock_unlock(&theosql->hshlck);
        return -1;
    }
    if ((rc = hash_for(theosql->rqsuuid, osql_session_testterminate, host))) {
        logmsg(LOGMSG_ERROR, "hash_for failed with rc = %d\n", rc);
        Pthread_rwlock_unlock(&theosql->hshlck);
        return -1;
    }

    /* blackout node
       this will prevent also active osql threads from sending requests in vain
    */
    if ((rc = osql_comm_blkout_node(host))) {
        logmsg(LOGMSG_ERROR, "fail to blackout node %s\n", host);
        Pthread_rwlock_unlock(&theosql->hshlck);
        return -1;
    }

    Pthread_rwlock_unlock(&theosql->hshlck);

    return 0;
}

/**
 * Cancel all pending osql block processor
 * transactions
 *
 */
inline int osql_repository_cancelall(void)
{ 
    return osql_repository_terminatenode(0);
}

/**
 * Returns true if all requests are being
 * cancelled (this is usually done because
 * of a schema change)
 *
 */
int osql_repository_cancelled(void)
{
    osql_repository_t *theosql = get_theosql();
    /* Becomes null when the db is exiting. */
    if (!theosql)
        return 1;

    int cancelall = ATOMIC_LOAD32(theosql->cancelall);
    return cancelall;
}

/**
 * Returns 1 if the session exists
 * used by socksql poking
 *
 */
bool osql_repository_session_exists(unsigned long long rqid, uuid_t uuid)
{
    osql_repository_t *theosql = get_theosql();
    if (theosql == NULL) {
        return 0;
    }

    osql_sess_t *sess = NULL;
    int out_rc = 0;

    Pthread_rwlock_rdlock(&theosql->hshlck);

    if (rqid == OSQL_RQID_USE_UUID)
        /* hash_find can modify the ordering of the hash chains, so is not
         * threadsafe; use hash_find_readonly() */
        sess = hash_find_readonly(theosql->rqsuuid, uuid);
    else
        sess = hash_find_readonly(theosql->rqs, &rqid);

    /* register the new receiver; osql_close_req will wait for to finish storing
     * the message */
    out_rc = !!sess;

    Pthread_rwlock_unlock(&theosql->hshlck);

    return out_rc;
}

void osql_repository_for_each(void *arg, int (*func)(void *, void *))
{
    osql_repository_t *theosql = get_theosql();
    if (!theosql)
        return;

    Pthread_rwlock_rdlock(&theosql->hshlck);

    hash_for(theosql->rqs, func, arg);
    hash_for(theosql->rqsuuid, func, arg);

    Pthread_rwlock_unlock(&theosql->hshlck);
}
