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

#ifndef _OSQL_REPOSITORY_H_
#define _OSQL_REPOSITORY_H_

#include "osqlsession.h"
#include "comdb2uuid.h"

/**
 * Adds an osql session to the repository
 * Returns:
 *   0 on success,
 *   -1 generic error
 *   -2 old session with same rqid already running
 *
 */
int osql_repository_add(osql_sess_t *sess);

/**
 * Removes an osql session from the repository
 * Returns 0 on success
 */
int osql_repository_rem(osql_sess_t *sess);

/**
 * Retrieves a session based on rqid
 * Increments the users to prevent premature deletion
 *
 */
osql_sess_t *osql_repository_get(uuid_t uuid);

/**
 * Decrements the number of users
 * Returns 0 if success
 */
int osql_repository_put(osql_sess_t *sess);

/**
 * Init repository
 * Returns 0 if success
 */
int osql_repository_init(void);

/**
 * Destroy repository
 * Returns 0 if success
 */
void osql_repository_destroy(void);

/**
 * Go through all the sessions executing on node
 * "node" and mark them "terminate", which cancel
 * them.
 * Used when a node is down.
 * If "node" is 0, all sessions are terminated.
 *
 */
int osql_repository_terminatenode(char *host);

/**
 * Print info about pending osql sessions
 *
 */
int osql_repository_printcrtsessions(void);

/**
 * Cancel all pending osql block processor
 * transactions
 *
 */
int osql_repository_cancelall(void);

/**
 * Returns 1 if the session exists
 * used by socksql poking
 *
 */
int osql_repository_session_exists(unsigned long long rqid, uuid_t uuid,
                                   int *rows_affected);

void osql_repository_for_each(void *arg, int (*func)(void *, void *));

#endif
