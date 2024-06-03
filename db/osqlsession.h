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
                              const char *host, int is_reorder_on);
/**
 * Same as osql_sess_create, but sql is already a malloced cstr
 *
 */
osql_sess_t *osql_sess_create_socket(const char *sql, char *tzname, int type,
                                     unsigned long long rqid, uuid_t uuid,
                                     const char *host, int is_reorder_on);

/**
 * Terminates an in-use osql session (for which we could potentially
 * receive message from sql thread).
 * Returns 0 if success
 *
 * NOTE: it is possible to inline clean a request on master bounce,
 * which starts by unlinking the session first, and freeing bplog afterwards
 */
int osql_sess_close(osql_sess_t **sess, int is_linked);

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
 * Handles a new op received for session "uuid"
 * It saves the packet in the local bplog
 * Return 0 if success
 * Set found if the session is found or not
 *
 */
int osql_sess_rcvop(uuid_t uuid, int type, void *data, int datalen, int *found);

/**
 * Same as osql_sess_rcvop, for socket protocol
 *
 */
int osql_sess_rcvop_socket(osql_sess_t *sess, int type, void *data, int datalen,
                           int *is_msg_done);

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

/**
 * Save a schema change object inside session
 *
 */
int osql_sess_save_sc(osql_sess_t *sess, char *rpl, int rplen);

/**
 * Save the list of schema changes serialized into llmeta
 *
 */
int osql_sess_save_sc_list(osql_sess_t *sess);

/**
 * Remove sc list from llmeta
 *
 */
int osql_delete_sc_list(uuid_t uuid, tran_type *trans);

/**
 * Coordinator has asked this participant to prepare it's osql schedule
 */
int osql_prepare(const char *dist_txnid, const char *coordinator_dbname, const char *coordinator_tier,
                 const char *coordinator_master);

/**
 * Coordinator has asked this participant to discard it's osql schedule
 */
int osql_discard(const char *dist_txnid);

#endif
