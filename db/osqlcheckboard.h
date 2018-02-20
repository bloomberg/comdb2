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

#ifndef _OSQL_CHECKBOARD_H_
#define _OSQL_CHECKBOARD_H_

#include "comdb2uuid.h"

/**
 *
 * Sql threads handling blocksql/socksql/recom/snapisol/serial sessions
 * register with the checkboard
 * Checkboard maintains the list of active sessions
 * and their status
 * Master can check the "checkboard" for existing sessions
 * and send signals to control their progress (for example,
 * terminate session)
 */

struct errstat;
struct sqlclntstate;

struct osql_sqlthr;
typedef struct osql_sqlthr osql_sqlthr_t;

/**
 * Initializes the checkboard
 * Returns 0 if success
 *
 */
int osql_checkboard_init(void);

/**
 * Destroy the checkboard
 * No more blocksql/socksql/recom/snapisol/serial threads can be created
 * after this.
 *
 */
void osql_checkboard_destroy(void);

/**
 * Checks the checkboard for sql session "rqid"
 * Returns:
 * - 1 is the session exists
 * - 0 if no session
 * - <0 if error
 *
 */
int osql_chkboard_sqlsession_exists(unsigned long long rqid, uuid_t uuid,
                                    int lock);

/**
 * Register an osql thread with the checkboard
 * This allows block processor to query the status
 * of its sql peer
 *
 */
int osql_register_sqlthr(struct sqlclntstate *clnt, int type);

/**
 * Unregister an osql thread from the checkboard
 * No further signalling for this thread is possible
 *
 */
int osql_unregister_sqlthr(struct sqlclntstate *clnt);

/**
 * Called when block processor sends a result back (contained in errstat),
 * Marks sql session complete
 * A null errstat means no error.
 *
 */
int osql_chkboard_sqlsession_rc(unsigned long long rqid, uuid_t uuid, int nops,
                                void *data, struct errstat *errstat);

/**
 * Wait the default time for the session to complete
 * Upon return, sqlclntstate's errstat is set
 *
 */
int osql_chkboard_wait_commitrc(unsigned long long rqid, uuid_t uuid,
                                int max_wait, struct errstat *xerr);

/**
* Update status of the pending sorese transaction, to support poking
 *
 */
int osql_checkboard_update_status(unsigned long long rqid, uuid_t uuid,
                                  int status, int timestamp);
/**
 * Reset fields when a session is retried
 * we're interested in things like master_changed
 *
 */
int osql_reuse_sqlthr(struct sqlclntstate *clnt, char *master);

/**
 * Retrieve the sqlclntstate for a certain rqid
 *
 */
int osql_chkboard_get_clnt(unsigned long long rqid, struct sqlclntstate **clnt);
int osql_chkboard_get_clnt_uuid(uuid_t uuid, struct sqlclntstate **clnt);

#endif
