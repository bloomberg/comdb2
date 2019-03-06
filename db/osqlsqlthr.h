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

#ifndef _OSQL_SQLTHR_H_
#define _OSQL_SQLTHR_H_

#include "bpfunc.pb-c.h"

/**
 *
 *  Interface with sqlglue.c
 *
 *  Each sqlthread executing an osql request has associated an osqlstate_t
 *  structure.  The structure is registered in a checkboard during the
 *  osql request lifetime.
 *
 *  Sqlthreads will call insrec/updrec/delrec functions.
 *   - For blocksql and socksql modes, each call results in a net transfer
 *     to the master.
 *   - For recom/snapisol/erial mode, each call will update the local shadow
 *tables (which
 *     will be transferred to the master once the transaction is committed).
 *
 */

struct BtCursor;

/* Upsert flags that attach with OSQL_INSERT. */
enum osql_rec_flags {
    OSQL_FORCE_VERIFY = 1 << 0,
    OSQL_IGNORE_FAILURE = 1 << 1,
    OSQL_ITEM_REORDERED = 1 << 2,
};

struct schema_change_type; // TODO fix there is a cyclicinlclude
                           /**
                            *
                            * Process a sqlite delete row request
                            * BtCursor points to the record to be deleted.
                            * Returns SQLITE_OK if successful.
                            *
                            */
int osql_delrec(struct BtCursor *pCur, struct sql_thread *thd);

/**
 * Process a sqlite insert row request
 * Row is provided by (pData, nData, blobs, maxblobs)
 * Returns SQLITE_OK if successful.
 *
 */
int osql_insrec(struct BtCursor *pCur, struct sql_thread *thd, char *pData,
                int nData, blob_buffer_t *blobs, int maxblobs, int flags);

/**
 * Process a sqlite insert row request
 * New row is provided by (pData, nData, blobs, maxblobs)
 * BtCursor points to the old row.
 * Returns SQLITE_OK if successful.
 *
 */
int osql_updrec(struct BtCursor *pCur, struct sql_thread *thd, char *pData,
                int nData, int *updCols, blob_buffer_t *blobs, int maxblobs,
                int flags);

/**
 * Process a sqlite clear table request
 * Currently not implemented due to non-transactional vs slowness unresolved
 * issues.
 *
 */
int osql_cleartable(struct sql_thread *thd, char *dbname);

/**
 * Set maximum osql transaction size
 *
 */
void set_osql_maxtransfer(int limit);

/**
 * Set the maximum time spent throttling
 *
 */
void set_osql_maxthrottle_sec(int limit);

/**
 * Get the maximum time spent throttling
 *
 */
int get_osql_maxthrottle_sec(void);

/**
 * Get maximum osql transaction size
 *
 */
int get_osql_maxtransfer(void);

/**
 * For serial transaction
 * send cursor ranges before commit
 * for read-set validation
 */
int osql_serial_send_readset(struct sqlclntstate *clnt, int nettype);

/**
 * Called when all rows are retrieved
 * Informs block process that the sql processing is over
 * and it can start processing bloplog
 *
 */
int osql_block_commit(struct sql_thread *thd);

/**
 * Start a sosql session, which
 * creates a blockprocessor peer
 * if keep_rqid, this is a retry and we want to
 * keep the same rqid
 *
 */
int osql_sock_start(struct sqlclntstate *clnt, int type, int keep_rqid);

/**
 * Start a sosql session if not already started
 */
int osql_sock_start_deferred(struct sqlclntstate *clnt);

/**
 * Terminates a sosql session
 * Block processor is informed that all the rows are sent
 * It waits for the block processor to report back the return code
 * Returns the result of block processor commit
 *
 */
int osql_sock_commit(struct sqlclntstate *clnt, int type);

/**
 * Terminates a sosql session
 * It notifies the block processor to abort the request
 *
 */
int osql_sock_abort(struct sqlclntstate *clnt, int type);

/**
 * busy bee,
 * comment me
 */
int osql_query_dbglog(struct sql_thread *thd, int queryid);

/**
 * Record a genid with the current transaction so we can
 * verify its existence upon commit
 *
 */
int osql_record_genid(struct BtCursor *pCur, struct sql_thread *thd,
                      unsigned long long genid);

/**
 * Validate Read access to database pointed by cursor pCur
 *
 */
int access_control_check_sql_read(struct BtCursor *pCur,
                                  struct sql_thread *thd);

/**
 * Update an sqlite_stat1 record as part of an analyze
 *
 */

int osql_updstat(struct BtCursor *pCur, struct sql_thread *thd, char *pData,
                 int nData, int nStat);


/**
 * Restart a broken socksql connection by opening
 * a new blockproc on the provided master and
 * sending the cache rows to resume the current.
 * If keep_session is set, the same rqid is used for the replay
 *
 */
int osql_sock_restart(struct sqlclntstate *clnt, int maxretries,
                      int keep_session);

/**
*
* Process a schema change request
* Returns SQLITE_OK if successful.
*
*/
int osql_schemachange_logic(struct schema_change_type *, struct sql_thread *,
                            int usedb);

/**
 *
 * Process a bpfunc request
 * Returns SQLITE_OK if successful.
 *
 */
int osql_bpfunc_logic(struct sql_thread *thd, BpfuncArg *arg);

int osql_dbq_consume_logic(struct sqlclntstate *, const char *spname, genid_t);
int osql_dbq_consume(struct sqlclntstate *, const char *spname, genid_t);

#endif
