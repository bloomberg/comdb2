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

typedef struct osql_repository {
    hash_t *rqs; /* hash of outstanding requests */
    hash_t *rqsuuid;
    pthread_mutex_t hshlck; /* protect the hash */
    struct dbenv *dbenv;    /* dbenv */

} osql_repository_t;

static osql_repository_t *theosql = NULL;

/**
 * Init repository
 * Returns 0 if success
 */
int osql_repository_init(void)
{
    osql_repository_t *tmp = NULL;

    tmp = (osql_repository_t *)calloc(sizeof(osql_repository_t), 1);
    if (!tmp)
        goto error;

    Pthread_mutex_init(&tmp->hshlck, NULL);
    tmp->rqs = hash_init(sizeof(unsigned long long)); /* indexed after rqid */
    tmp->rqsuuid = hash_init_o(offsetof(osql_sess_t, uuid), sizeof(uuid_t));

    if (!tmp->rqs || !tmp->rqsuuid) 
        goto error;

    theosql = tmp;
    return 0;

error:
    /* no cleanup, this is a server exit */
    logmsg(LOGMSG_ERROR, "Failed to initialize repository\n");
    return -1;
}

/**
 * Destroy repository
 * Returns 0 if success
 */
void osql_repository_destroy(void)
{
    /* TODO: review this for clean exit */
    theosql = NULL;
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

/* there is no lock protection here */
static osql_sess_t * _get_sess(unsigned long long rqid, uuid_t uuid)
{
    osql_sess_t *sess = NULL;

    if (rqid == OSQL_RQID_USE_UUID)
        sess = hash_find_readonly(theosql->rqsuuid, &uuid);
    else {
        sess = hash_find_readonly(theosql->rqs, &rqid);
    }
    
    return sess;
}

/**
 * Adds an osql session to the repository
 * Returns 0 on success
 */
int osql_repository_add(osql_sess_t *sess, int *replaced)
{
    osql_sess_t *sess_chk;
    int rc = 0;

    if (!theosql)
        return -1;

    /* insert it into the hash table */
    Pthread_mutex_lock(&theosql->hshlck);

    /* how about we check if this session is added again due to an early replay
     */
    sess_chk = _get_sess(sess->rqid, sess->uuid); 
    if (sess_chk) {
        char *p = (char *)alloca(64);
        p = util_tohex(p, (char *)sess->uuid, 16);

        logmsg(LOGMSG_ERROR,
               "%s: trying to add another session with the same rqid, "
               "rqid=%llx uuid=%s\n",
               __func__, sess->rqid, p);

        rc = osql_sess_try_terminate(sess_chk);
        if (rc < 0) {
            logmsg(LOGMSG_ERROR, "%s:%d osql_sess_try_terminate rc %d\n",
                   __func__, __LINE__, rc);
            Pthread_mutex_unlock(&theosql->hshlck);
            return -1;
        }
        if (rc) {
            /* old request was already processed, ignore new ones */
            Pthread_mutex_unlock(&theosql->hshlck);
            *replaced = 1;
            logmsg(LOGMSG_INFO,
                   "%s: rqid=%llx, uuid=%s was completed/dispatched\n",
                   __func__, sess->rqid, p);
            return 0;
        }
        /* old request was terminated successfully, let's add the new one */
        logmsg(LOGMSG_INFO,
               "%s: cancelled old request for rqid=%llx, uuid=%s\n", __func__,
               sess->rqid, p);
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

    Pthread_mutex_unlock(&theosql->hshlck);

    return rc;
}

int gbl_abort_on_missing_osql_session = 0;

/**
 * Remove an osql session from the repository
 * return 0 on success
 */
int osql_repository_rem(osql_sess_t *sess, const int lock, const char *func,
                        const char *callfunc, int line)
{
    int rc = 0;

    if (!theosql)
        return -1;

    if (lock) {
        Pthread_rwlock_wrlock(&theosql->hshlck);
    }

    if (sess->rqid == OSQL_RQID_USE_UUID) {
        rc = hash_del(theosql->rqsuuid, sess);
    } else {
        rc = hash_del(theosql->rqs, sess);
    }

    if (lock)
        Pthread_mutex_unlock(&theosql->hshlck);

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
            logmsg(LOGMSG_ERROR, "%s found %s %d times in tracking array\n",
                   __func__, p, found_uuid);
        }
#endif

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

    return 0;
}

/**
 * Retrieves a session based on rqid/uuid
 * Increments the users to prevent premature
 * deletion
 * NOTE: if the session is dispatched, addclient
 * fails and this return NULL
 */
osql_sess_t *osql_repository_get(unsigned long long rqid, uuid_t uuid)
{
    osql_sess_t *sess = NULL;

    if (!theosql)
        return NULL;

    Pthread_rwlock_rdlock(&theosql->hshlck);

    sess = _get_sess(rqid, uuid);

    /* register the new receiver; osql_close_req will wait for to finish storing
     * the message */
    if (sess) {
        if(osql_sess_addclient(sess)) {
            /* session dispatched, ignore */
            sess = NULL;
        }
    }

    Pthread_rwlock_unlock(&theosql->hshlck);

    return sess;
}

/**
 * The reader thread is done with the session
 * 
 * Returns 0 if success
 */
int osql_repository_put(osql_sess_t *sess, int bplog_complete)
{ 
    int rc;

    if (!theosql)
        return -1;

    Pthread_rwlock_rdlock(&theosql->hshlck);

    rc = osql_sess_remclient(sess, bplog_complete);
    
    Pthread_rwlock_unlock(&theosql->hshlck);

    if (rc == 1) {
        /* session is marked terminated, release it */
        osql_repository_rem(sess, 1, __func__, NULL, __LINE__);
    }

    return rc;
}

static int _getcrtinfo(void *obj, void *arg)
{
    char *str = osql_sess_info((osql_sess_t *)obj);

    logmsg(LOGMSG_USER, "   %s\n", str?str:"unknown");

    if (str)
        free(str);

    return 0;
}

/**
 * Print info about pending osql sessions
 *
 */
int osql_repository_printcrtsessions(void)
{
    int rc = 0;
    int maxops = 0;

    if (!theosql)
        return -1;

    maxops = get_osql_maxtransfer();
    logmsg(LOGMSG_USER, "Maximum transaction size: %d bplog entries\n", maxops);

    Pthread_rwlock_rdlock(&theosql->hshlck);

    logmsg(LOGMSG_USER, "Begin osql session info:\n");
    if ((rc = hash_for(theosql->rqs, _getcrtinfo, NULL))) {
        logmsg(LOGMSG_USER, "hash_for failed with rc = %d\n", rc);
        rc = -1;
    } else
        logmsg(LOGMSG_USER, "Done osql info.\n");

    Pthread_rwlock_unlock(&theosql->hshlck);

    return rc;
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
    if (!theosql)
        return -1;

    int rc = 0;

    /* insert it into the hash table */
    Pthread_mutex_lock(&theosql->hshlck);

    if ((rc = hash_for(theosql->rqs, osql_session_testterminate, host))) {
        logmsg(LOGMSG_ERROR, "hash_for failed with rc = %d\n", rc);
        Pthread_mutex_unlock(&theosql->hshlck);
        return -1;
    }
    if ((rc = hash_for(theosql->rqsuuid, osql_session_testterminate, host))) {
        logmsg(LOGMSG_ERROR, "hash_for failed with rc = %d\n", rc);
        Pthread_mutex_unlock(&theosql->hshlck);
        return -1;
    }

    Pthread_mutex_unlock(&theosql->hshlck);

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
 * Returns 1 if the session exists
 * used by socksql poking
 *
 */
bool osql_repository_session_exists(unsigned long long rqid, uuid_t uuid)
{
    if (!theosql)
        return false;

    osql_sess_t *sess = NULL;
    int out_rc = 0;

    Pthread_mutex_lock(&theosql->hshlck);

    sess = _get_sess(rqid, uuid);

    /* register the new receiver; osql_close_req will wait for to finish storing
     * the message */
    out_rc = !!sess;

    Pthread_mutex_unlock(&theosql->hshlck);

    return out_rc;
}

void osql_repository_for_each(void *arg, int (*func)(void *, void *))
{
    if (!theosql)
        return;

    Pthread_mutex_lock(&theosql->hshlck);

    hash_for(theosql->rqs, func, arg);
    hash_for(theosql->rqsuuid, func, arg);

    Pthread_mutex_unlock(&theosql->hshlck);
}
