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
#include "sys_wrap.h"
#include "tohex.h"
#include "comdb2_atomic.h"

typedef struct osql_repository {
    hash_t *rqs; /* hash of outstanding requests */
    hash_t *rqsuuid;
    pthread_mutex_t hshlck; /* protect the hash */
    struct dbenv *dbenv;    /* dbenv */

} osql_repository_t;

static osql_repository_t *theosql = NULL;

static int osql_repository_rem_unlocked(osql_sess_t *sess);

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

static char hex(unsigned char a)
{
    if (a < 10)
        return '0' + a;
    return 'a' + (a - 10);
}

/* this function should be called with theosql->hshlck lock held */
static osql_sess_t *_get_sess(uuid_t uuid)
{
    return hash_find_readonly(theosql->rqsuuid, uuid);
}

/**
 * Adds an osql session to the repository
 * Returns:
 *   0 on success,
 *   -1 generic error
 *   -2 old session with same rqid already running
 */
int osql_repository_add(osql_sess_t *sess)
{
    osql_sess_t *sess_chk;
    int rc = 0;

    if (!theosql)
        return -1;

    /* insert it into the hash table */
    Pthread_mutex_lock(&theosql->hshlck);

    /* check if this session is added again due to an early replay */
    sess_chk = _get_sess(sess->uuid);
    if (sess_chk) {
        uuidstr_t us;

        logmsg(LOGMSG_ERROR,
               "%s: trying to add another session with the same rqid, rqid=%llx uuid=%s\n",
               __func__, sess->rqid, comdb2uuidstr(sess->uuid, us));

        int keep = osql_sess_try_terminate(sess_chk, NULL);
        if (!keep) {
            osql_repository_rem_unlocked(sess_chk);
            osql_sess_close(&sess_chk, 0); /* false -- don't remove from repository again */
        } else {
            Pthread_mutex_unlock(&theosql->hshlck);
            return -2;
        }
    }

    if (sess->rqid == OSQL_RQID_USE_UUID)
        rc = hash_add(theosql->rqsuuid, sess);
    else
        rc = hash_add(theosql->rqs, sess);

    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: Unable to hash_add the new request\n",
               __func__);
        rc = -1;
    }

    Pthread_mutex_unlock(&theosql->hshlck);

    return rc;
}

static int osql_repository_rem_unlocked(osql_sess_t *sess)
{
    int rc = 0;
    if (sess->rqid == OSQL_RQID_USE_UUID) {
        rc = hash_del(theosql->rqsuuid, sess);
    } else {
        rc = hash_del(theosql->rqs, sess);
    }
    if (rc) {
        logmsg(LOGMSG_DEBUG, "%s: Unable to hash_del, session %p (not found)\n",
               __func__, sess);
    }
    return rc;
}

/**
 * Remove an osql session from the repository
 * return 0 on success
 */
int osql_repository_rem(osql_sess_t *sess)
{
    if (!theosql)
        return 1;

    Pthread_mutex_lock(&theosql->hshlck);
    int rc = osql_repository_rem_unlocked(sess);
    Pthread_mutex_unlock(&theosql->hshlck);
    return rc;
}

/**
 * Retrieves a session based on rqid/uuid
 * Increments the users to prevent premature deletion
 *
 * NOTE: if the session is dispatched, addclient * return NULL
 */
osql_sess_t *osql_repository_get(uuid_t uuid)
{
    osql_sess_t *sess = NULL;

    if (!theosql)
        return NULL;

    Pthread_mutex_lock(&theosql->hshlck);
    sess = _get_sess(uuid);
    if (sess) {
        if (osql_sess_addclient(sess)) {
            /* session dispatched, ignore */
            sess = NULL;
        }
    }
    Pthread_mutex_unlock(&theosql->hshlck);

    return sess;
}

/**
 * The reader thread is done with the session
 *
 * Returns
 *   0 if success
 *   1 if session is marked terminated
 */
int osql_repository_put(osql_sess_t *sess)
{
    int rc;
    Pthread_mutex_lock(&theosql->hshlck);
    rc = osql_sess_remclient(sess);
    Pthread_mutex_unlock(&theosql->hshlck);
    return rc;
}

static int _getcrtinfo(void *obj, void *arg)
{
    char *str = osql_sess_info((osql_sess_t *)obj);

    logmsg(LOGMSG_USER, "   %s\n", str ? str : "unknown");

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

    Pthread_mutex_lock(&theosql->hshlck);

    logmsg(LOGMSG_USER, "Begin osql session info (rqs):\n");
    if ((rc = hash_for(theosql->rqs, _getcrtinfo, NULL))) {
        logmsg(LOGMSG_USER, "hash_for failed with rc = %d\n", rc);
        rc = -1;
    } else
        logmsg(LOGMSG_USER, "Done osql info (rqs).\n");
    logmsg(LOGMSG_USER, "Begin osql session info (uuids):\n");

    if ((rc = hash_for(theosql->rqsuuid, _getcrtinfo, NULL))) {
        logmsg(LOGMSG_USER, "hash_for failed with rc = %d\n", rc);
        rc = -1;
    } else
        logmsg(LOGMSG_USER, "Done osql info(uuids).\n");

    Pthread_mutex_unlock(&theosql->hshlck);

    return rc;
}

/**
 * Filter sessions that needs to be considered for
 * termination (matching a machine name, if any)
 *
 */
static int osql_session_testterminate(void *obj, void *arg)
{
    osql_sess_t *sess = (osql_sess_t *)obj;
    char *host = arg;

    int keep = osql_sess_try_terminate(sess, host);
    if (!keep) {
        osql_repository_rem_unlocked(sess);
        osql_sess_close(&sess, 0); /* false -- don't remove from repository again */
    }
    return 0;
}

/**
 * Go through all the sessions executing on host
 * "host" and mark them "terminate", which cancel
 * them.
 * Used when a host is down.
 * If "host" is NULL, all sessions are terminated.
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
int osql_repository_cancelall(void)
{ 
    return osql_repository_terminatenode(0);
}

/**
 * Returns 1 if the session exists
 * used by socksql poking
 *
 */
int osql_repository_session_exists(unsigned long long rqid, uuid_t uuid,
                                   int *rows_affected)
{
    if (!theosql)
        return 0;

    osql_sess_t *sess = NULL;
    int exists = 0;

    if (rows_affected)
        *rows_affected = -1;

    Pthread_mutex_lock(&theosql->hshlck);

    sess = _get_sess(uuid);
    if (sess) {
        exists = 1;
        if (rows_affected) {
            snap_uid_t *snap_info = sess->snap_info;
            /*
             * If replicant pokes before master processes OSQL_DONE_SNAP,
             * the session won't have a snap_info. Protect us from the race
             * condition by check for NULL here.
             */
            if (snap_info)
                *rows_affected = snap_info->effects.num_inserted + snap_info->effects.num_updated +
                                 snap_info->effects.num_deleted;
        }
    }
    Pthread_mutex_unlock(&theosql->hshlck);

    return exists;
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
