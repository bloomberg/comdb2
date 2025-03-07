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
blocksql_tran_t *osql_bplog_create(int is_uuid, int is_reorder);

/**
 * Apply all the bplog updates
 */
int osql_bplog_commit(struct ireq *iq, void *iq_trans, int *nops,
                      struct block_err *err);

/**
 * Free the bplog
 */
void osql_bplog_close(blocksql_tran_t **ptran);

/**
 * Inserts the op in the iq oplog
 * If sql processing is local, this is called by sqlthread
 * If sql processing is remote, this is called by reader_thread for the
 * offload node
 * Returns 0 if success
 *
 */
int osql_bplog_saveop(osql_sess_t *sess, blocksql_tran_t *tran, char *rpl,
                      int rplen, int type);

/**
 * Construct a blockprocessor transaction buffer containing
 * a sock sql /recom  / snapisol / serial transaction
 *
 * Note: sqlqret is the location where we saved the query
 *       This will allow let us point directly to the buffer
 *
 */
int osql_bplog_build_sorese_req(uint8_t **p_buf_start,
                                const uint8_t **pp_buf_end, const char *sqlq,
                                int sqlqlen, const char *tzname, int reqtype,
                                unsigned long long rqid, uuid_t uuid);

/**
 * Set proper blkseq from session to iq
 * NOTE: We don't need to create buffers _SEQ, _SEQV2 for it
 *
 */
void osql_bplog_set_blkseq(osql_sess_t *sess, struct ireq *iq);

/**
 * Debugging support
 * Prints all the timings recorded for this bplog
 *
 */
void osql_bplog_time_done(osql_bp_timings_t *tms);

/* Returns the tablename of the last usedb opcode */
const char *osql_last_usedb_tablename(osql_sess_t *sess);
#endif
