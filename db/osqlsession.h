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
 * Terminates an in-use osql session (for which we could potentially
 * receive message from sql thread).
 * It calls osql_remove_session.
 * Returns 0 if success
 *
 * NOTE: it is possible to inline clean a request on master bounce,
 * which starts by unlinking the session first, and freeing bplog afterwards
 */
int osql_close_session(osql_sess_t **sess, int is_linked, const char *func,
                       const char *callfunc, int line);

/**
 * Register client
 * Prevent temporary the session destruction
 *
 */
void osql_sess_addclient(osql_sess_t *sess);

/**
 * Unregister client
 *
 */
void osql_sess_remclient(osql_sess_t *sess);

/**
 * Log query to the reqlog
 */
void osql_sess_reqlogquery(osql_sess_t *sess, struct reqlogger *reqlog);

/**
 * Print summary session
 *
 */
int osql_sess_getcrtinfo(void *obj, void *arg);

/**
 * Returns associated blockproc transaction
 *
 */
void *osql_sess_getbptran(osql_sess_t *sess);

/**
 * Handles a new op received for session "rqid"
 * It saves the packet in the local bplog
 * Return 0 if success
 * Set found if the session is found or not
 *
 */
int osql_sess_rcvop(unsigned long long rqid, uuid_t uuid, int type, void *data,
                    int datalen, int *found);

/**
 * If the node "arg" machine the provided session
 * "obj", mark the session terminated
 * If "*arg: is 0, "obj" is marked terminated anyway
 *
 */
int osql_session_testterminate(void *obj, void *arg);

/**
 * Creates an sock osql session and add it to the repository
 * Returns created object if success, NULL otherwise
 *
 */
osql_sess_t *osql_sess_create(const char *sql, int sqlen, char *tzname,
                              int type, unsigned long long rqid, uuid_t uuid,
                              const char *host, bool is_reorder_on);

/**
 * Returns
 * - total time in ms (tottm)
 * - retries (rtrs)
 *
 */
void osql_sess_getsummary(osql_sess_t *sess, int *tottm, int *rtrs);

int osql_sess_queryid(osql_sess_t *sess);

/**
 * Needed for socksql and bro-s, which creates sessions before
 * iq->bplogs.
 * If we fail to dispatch to a blockprocession thread, we need this function
 * to clear the session from repository and free that leaked memory
 *
 */
void osql_sess_clear_on_error(struct ireq *iq, unsigned long long rqid,
                              uuid_t uuid);

int osql_cache_selectv(int type, osql_sess_t *sess, unsigned long long,
                       char *rpl);
int osql_process_selectv(osql_sess_t *sess,
                         int (*wr_sv)(void *arg, const char *tablename,
                                      int tableversion,
                                      unsigned long long genid),
                         void *wr_arg);

/**
 * Terminate a session if the session is not yet completed/dispatched
 * Return 0 if session is successfully terminated,
 *        -1 for errors,
 *        1 otherwise (if session was already processed)
 */
int osql_sess_try_terminate(osql_sess_t *sess);
#endif
