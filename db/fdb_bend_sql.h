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

#ifndef _FDB_BEND_SQL_H_
#define _FDB_BEND_SQL_H_

#include <fdb_comm.h>

/**
 * Handle a remote sql request
 *
 */
int fdb_appsock_work(const char *cid, struct sqlclntstate *clnt, int version,
                     enum run_sql_flags flags, char *sql, int sqllen,
                     char *trim_key, int trim_keylen, SBUF2 *sb);

/**
 * Send back a streamed row with return code (marks also eos)
 *
 */
int fdb_svc_sql_row(SBUF2 *sb, char *cid, char *row, int rowlen, int rc);

/**
 * For requests where we want to avoid a dedicated genid lookup socket, this
 * masks every index as covered index
 *
 */
int fdb_svc_alter_schema(struct sqlclntstate *clnt, sqlite3_stmt *stmt,
                         UnpackedRecord *upr);

/**
  * Open an sql "cursor", join the transaction tid
  *
  */
int fdb_svc_cursor_open_sql(char *tid, char *cid, int rootpage, int version,
                            int flags, struct sqlclntstate **clnt);

/**
 * Start a transaction
 *
 */
int fdb_svc_trans_begin(char *tid, enum transaction_level lvl, int flags,
                        int seq, struct sql_thread *thd,
                        struct sqlclntstate **pclnt);

/**
 * Commit a transaction
 *
 */
int fdb_svc_trans_commit(char *tid, enum transaction_level lvl,
                         struct sqlclntstate *clnt, int seq);

/**
 * Rollback a transaction
 *
 */
int fdb_svc_trans_rollback(char *tid, enum transaction_level lvl,
                           struct sqlclntstate *clnt, int seq);

/**
 * Insert a sqlite row in the local transaction
 *
 */
int fdb_svc_cursor_insert(struct sqlclntstate *clnt, char *tblname,
                          int rootpage, int version, unsigned long long genid,
                          char *data, int datalen, int seq);

/**
 * Delete a sqlite row in the local transaction
 *
 */
int fdb_svc_cursor_delete(struct sqlclntstate *clnt, char *tblname,
                          int rootpage, int version, unsigned long long genid,
                          int seq);

/**
 * Update a sqlite row in the local transaction
 *
 */
int fdb_svc_cursor_update(struct sqlclntstate *clnt, char *tblname,
                          int rootpage, int version,
                          unsigned long long oldgenid, unsigned long long genid,
                          char *data, int datalen, int seq);

/**
 * Return the sqlclntstate storing the shared transaction, if any
 *
 */
struct sqlclntstate *fdb_svc_trans_get(char *tid);

#endif

