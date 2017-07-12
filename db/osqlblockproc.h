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

#ifndef _OSQL_BLOCKPROC_H_
#define _OSQL_BLOCKPROC_H_

/**
 *
 * Osql Interface with Block Processor
 *
 * Each block processor used for osql keeps a local log of operations (bplog)
 * of the current transaction. The operations are sent by the sqlthread.
 *
 * In blocksql mode, the bp opens an osql session for each query part of the
 * current transaction.
 * In socksql/recom/snapisol/serial mode, the bp has only one session.
 *
 * Block processor waits until all pending sessions are completed.  If any of
 * the sessions completes with an error that could be masked (like deadlocks),
 * the block processor will re-issue it.
 *
 * If all the sessions complete successfully, the bp scans through the log and
 * applies all the changes by calling record.c functions.
 *
 * At the end, the return code is provided to the client (in the case of
 * blocksql) or the remote socksql/recom/snapisol/serial thread.
 *
 */

#include "comdb2.h"
#include "osqlsession.h"
#include "block_internal.h"
#include "comdb2uuid.h"

struct bplog_func // UNDONE FABIO
    {
    int code;
    void *arg;
};

typedef struct bplog_func bplog_func_t;

/**
 * debug global flags
 */
extern int gbl_time_osql;

/**
 * Adds the current session to the bplog pending session list.
 * If there is no bplog created, it creates one.
 * Returns 0 if success.
 *
 */
int osql_bplog_start(struct ireq *iq, osql_sess_t *sess);

/**
 * Wait for all pending osql sessions of this transaction to
 * finish
 */
int osql_bplog_finish_sql(struct ireq *iq, struct block_err *err);

/**
 * Apply all the bplog updates
 */
int osql_bplog_commit(struct ireq *iq, void *iq_trans, int *nops,
                      struct block_err *err);

/**
 * Free all the sessions and free the bplog
 * HACKY: since we want to catch and report long requests in block
 * process, call this function only after reqlog_end_request is called
 * (sltdbt.c)
 */
int osql_bplog_free(struct ireq *iq, int are_sessions_linked, const char *func, const char *callfunc, int line);

/**
 * Prints summary for the current osql bp transaction
 * It uses the specified "printfn" function to dump the information
 * to a reqlog engine.
 *
 */
char *osql_get_tran_summary(struct ireq *iq);

/**
 * Inserts the op in the iq oplog
 * If sql processing is local, this is called by sqlthread
 * If sql processing is remote, this is called by reader_thread for the
 * offload node
 * Returns 0 if success
 *
 */
int osql_bplog_saveop(osql_sess_t *sess, char *rpl, int rplen,
                      unsigned long long rqid, uuid_t uuid,
                      unsigned long long seq, char *host);

/**
 * Wakeup the block processor waiting for a completed session
 *
 */
int osql_bplog_signal(blocksql_tran_t *tran);

/**
 * Construct a blockprocessor transaction buffer containing
 * a sock sql /recom  / snapisol / serial transaction
 *
 * Note: sqlqret is the location where we saved the query
 *       This will allow let us point directly to the buffer
 *
 */
int osql_bplog_build_sorese_req(uint8_t *p_buf_start,
                                const uint8_t **pp_buf_end, const char *sqlq,
                                int sqlqlen, const char *tzname, int reqtype,
                                char **sqlqret, int *sqlqlenret,
                                unsigned long long rqid, uuid_t uuid);

/**
 * Wait if there are more then "limit" pending sessions
 *
 */
int osql_bplog_session_starthrottle(struct ireq *iq);

/**
 * Signal blockprocessor that one has completed
 * For now this is used only for
 *
 */
int osql_bplog_session_is_done(struct ireq *iq);

/**
 * Set parallelism threshold
 *
 */
void osql_bplog_setlimit(int limit);

/**
 * Log the strings for each completed blocksql request for the
 * reqlog
 */
int osql_bplog_reqlog_queries(struct ireq *iq);

/**
 * Debugging support
 * Prints all the timings recorded for this bplog
 *
 */
void osql_bplog_time_done(struct ireq *iq);

int osql_get_delayed(struct ireq *);

int osql_throttle_session(struct ireq *);
void osql_set_delayed(struct ireq *iq);

/**
 * Throw bplog to /dev/null, sql does not need this
 *
 */
int sql_cancelled_transaction(struct ireq *iq);

#endif
