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

#ifndef __BDB_OSQL_CUR_H_
#define __BDB_OSQL_CUR_H_

#include "bdb_api.h"

struct bdb_osql_trn;

/**
 * Frees all shadow files associated with
 * transaction "tran"
 *
 */
int bdb_tran_free_shadows(bdb_state_type *bdb_state, tran_type *tran);

/**
 * Check if a real genid is marked deleted.
 *
 */
int bdb_tran_deltbl_isdeleted(bdb_cursor_ifn_t *cur, unsigned long long genid,
                              int ignore_limit, int *bdberr);

/**
 * Check if a delete was already done; this is not related to
 * transaction mode, and serve only the purpose to dedup
 * deletes in transactions that naturally occur in one pass deletes
 *
 */
int bdb_tran_deltbl_isdeleted_dedup(bdb_cursor_ifn_t *pcur_ifn,
                                    unsigned long long genid, int ignore_limit,
                                    int *bdberr);

/**
 * Mark the provided genid deleted
 * data, datalen are defined by upper layers (sqlglue).
 *
 */
int bdb_tran_deltbl_setdeleted(bdb_cursor_ifn_t *cur, unsigned long long genid,
                               void *data, int datalen, int *bdberr);

/**
 * Returns the first deleted genid of a transaction if one exists
 * and a cursor for retrieving the next records.
 * Returns NULL if no deleted rows.
 *
 *
 */
struct temp_cursor *bdb_tran_deltbl_first(bdb_state_type *bdb_state,
                                          tran_type *shadow_tran, int dbnum,
                                          unsigned long long *genid,
                                          char **data, int *datalen,
                                          int *bdberr);

/**
 * Returns the next deleted genid of a transaction
 * if one exists and IX_OK;
 * Returns IX_PASTEOF if done.  The cursor is destroyed
 * immediately when returning IX_PASTEOF.
 *
 */
int bdb_tran_deltbl_next(bdb_state_type *bdb_state, tran_type *shadow_tran,
                         struct temp_cursor *cur, unsigned long long *genid,
                         char **data, int *datalen, int *bdberr);

/**
 *  There are 4 step during a bdbcursor move
 *  1) If current real row consumed, move real berkdb
 *  2) Update shadows
 *  3) If current shadow row consumed, move shadow berkdb
 *    - we need to reposition for relative moves
 *  4) Merge real and shadow
 *
 *  This function is handling the step 2
 *  - get first log
 *  - if there is no log (first == NULL) return
 *  - process each log from first to last (this is the last seen when first log
 *is
 *    retrieved
 *  - if this is the last log, reset log for transactions
 *  If any shadow row is added/deleted, mark dirty
 */
enum log_ops;
int bdb_osql_update_shadows(bdb_cursor_ifn_t *cur, struct bdb_osql_trn *trn,
                            int *dirty, enum log_ops log_op, int *bdberr);

/**
 * Check if a shadow is backfilled
 *
 */
int bdb_osql_shadow_is_bkfilled(bdb_cursor_ifn_t *cur, int *bdberr);

/**
 *  Set shadow backfilled
 *
 */
int bdb_osql_shadow_set_bkfilled(bdb_cursor_ifn_t *cur, int *bdberr);

/**
 * Retrieves the last log processed by this transaction
 *
 */
struct bdb_osql_log *bdb_osql_shadow_get_lastlog(bdb_cursor_ifn_t *cur,
                                                 int *bdberr);

/**
 * Stores the last log processed by this cur (which updates
 * the shadow btree status
 *
 */
int bdb_osql_shadow_set_lastlog(bdb_cursor_ifn_t *cur, struct bdb_osql_log *log,
                                int *bdberr);

/**
 * Clear any cached pointers to existing transactions
 * Set the shadow transaction to a reset cursor
 *
 */
int bdb_osql_cursor_reset(bdb_state_type *bdb_state,
                          bdb_cursor_ifn_t *pcur_ifn);
void bdb_osql_cursor_set(bdb_cursor_ifn_t *pcur_ifn, tran_type *shadow_tran);

#endif
