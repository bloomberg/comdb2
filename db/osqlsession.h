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

#ifndef _OSQLSESSION_H_
#define _OSQLSESSION_H_

#include "comdb2.h"
#include "errstat.h"
#include "comdb2uuid.h"
#include "sqloffload.h"

typedef struct osql_req osql_req_t;
typedef struct osql_uuid_req osql_uuid_req_t;

/**
 * Creates an sock osql session and add it to the repository
 * Returns created object if success, NULL otherwise
 *
 */
osql_sess_t *osql_sess_create(const char *sql, int sqlen, char *tzname,
                              int type, unsigned long long rqid, uuid_t uuid,
                              const char *host, bool is_reorder_on);

/**
 * Terminates an in-use osql session (for which we could potentially
 * receive message from sql thread).
 * Returns 0 if success
 *
 * NOTE: it is possible to inline clean a request on master bounce,
 * which starts by unlinking the session first, and freeing bplog afterwards
 */
int osql_sess_close(osql_sess_t **sess, bool is_linked);

/**
 * Register client
 * Prevent temporary the session destruction
 * Returns -1 if the session is dispatched
 *
 */
int osql_sess_addclient(osql_sess_t *sess);

/**
 * Unregister client
 *
 */
int osql_sess_remclient(osql_sess_t *sess);

/**
 * Log query to the reqlog
 */
void osql_sess_reqlogquery(osql_sess_t *sess, struct reqlogger *reqlog);

/**
 * Session information
 * Return malloc-ed string:
 * sess_type rqid uuid local/remote host
 *
 */
#define OSQL_SESS_INFO_LEN 256
char *osql_sess_info(osql_sess_t *sess);

/**
 * Handles a new op received for session "rqid"
 * It saves the packet in the local bplog
 * Return 0 if success
 * Set found if the session is found or not
 *
 */
int osql_sess_rcvop(unsigned long long rqid, uuid_t uuid, int type, void *data,
                    int datalen, int *found);

int osql_sess_queryid(osql_sess_t *sess);

/**
 * Terminate a session if the session is not yet completed/dispatched
 * and the node matches the session source;
 * Return
 *    0 if session can be terminated by caller
 *    1 otherwise (if session was already dispatched, node mismatch)
 *
 * NOTE: this should be called under osql repository lock
 */
int osql_sess_try_terminate(osql_sess_t *psess, const char *node);

#endif
