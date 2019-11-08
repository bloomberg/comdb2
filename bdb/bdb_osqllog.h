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

/**
 *  Snapisol/Serial sql support: the bdb osql log repository interface
 *
 *  This interface is used by replication thread/commit path
 *  to store undo records for add/deletes to be processed by
 *  the snapshot/serializable sql sessions
 *
 */
#ifndef _BDB_OSQL_LOG_H_
#define _BDB_OSQL_LOG_H_

#include <list.h>

#include <build/db.h>
#include "bdb_int.h"
#include "llog_auto.h"

struct bdb_state_tag;
struct bdb_osql_trn;

struct bdb_osql_log_impl;

typedef struct bdb_osql_log {
    u_int32_t txnid;
    struct bdb_osql_log_impl *impl;
    LINKC_T(struct bdb_osql_log) lnk;
} bdb_osql_log_t;

/**
 * Data log optimization
 * Keep just a pointer inside the shadow btree to the log entry to avoid
 * multiple copies and shrink shadow btree size.
 *
 * Our scheme: the 'flag' variable here will overlap with both the odh.flag
 * byte and the types.c flag byte.  Since we'll never see -1 in either of
 * those, use flag == -1 to signify that this is a delayed/optimized record.
 */
typedef struct bdb_osql_log_dta_ptr {
    char flag;  /* mark entry as not row */
    DB_LSN lsn; /* log lsn */
} bdb_osql_log_dta_ptr_t;

/**
 * Replication stream records which are added to the virtual-stripe table
 * require a record 'version'.  This is our way of determining whether the
 * record has already been read for a particular scan.  These can also be
 * optimized.
 */
typedef struct bdb_osql_log_addc_ptr {
    char flag;
    DB_LSN lsn;
} bdb_osql_log_addc_ptr_t;

/**
 * Initialize bdb_osql log repository
 *
 */
int bdb_osql_log_repo_init(int *bdberr);

/**
 * Clear the bdb osql log repository
 *
 */
int bdb_osql_log_repo_destroy(int *bdberr);

/**
 *  Returns a serial queue transaction
 *
 */
bdb_osql_log_t *bdb_osql_log_begin(int trak, int *bdberr);

/**
 *  Add an undo records to the log for:
 *  - DEL_DTA
 *  - ADD_DTA
 *  - DEL_IX
 *  - ADD_IX
 *
 *  Dta can be a blob or data
 *  Called from inside berkdb log processing callback
 *  or transaction commit (the latter for in local mode)
 *  This will be processed later by sql snapisol/serial
 *  session threads
 *
 */
int bdb_osql_log_deldta(bdb_osql_log_t *log, DB_LSN *lsn,
                        llog_undo_del_dta_args *del_dta, int *bdberr);
int bdb_osql_log_adddta(bdb_osql_log_t *log, DB_LSN *lsn,
                        llog_undo_add_dta_args *add_dta, int *bdberr);
int bdb_osql_log_delix(bdb_osql_log_t *log, DB_LSN *lsn,
                       llog_undo_del_ix_args *del_dta, int *bdberr);
int bdb_osql_log_addix(bdb_osql_log_t *log, DB_LSN *lsn,
                       llog_undo_add_ix_args *add_dta, int *bdberr);
int bdb_osql_log_upddta(bdb_osql_log_t *log, DB_LSN *lsn,
                        llog_undo_upd_dta_args *upd_dta, int *bdberr);
int bdb_osql_log_updix(bdb_osql_log_t *log, DB_LSN *lsn,
                       llog_undo_upd_ix_args *upd_ix, int *bdberr);
/**
 *  Remove a bdb_osql transaction
 *  All the undo records are freed
 *
 */
int bdb_osql_log_destroy(bdb_osql_log_t *log);

/**
 *  Register this many clients;
 *  This is done once, per log creation, by replication log
 *
 */
int bdb_osql_log_register_clients(bdb_osql_log_t *log, int clients,
                                  int *bdberr);

/**
 *  Get info about the pointed log
 *
 */
bdb_osql_log_t *bdb_osql_log_first(bdb_osql_log_t *log,
                                   unsigned long long *genid);

/**
 *  Get info about the next transaction in the log
 * The return, if not null, is the following log;
 * The argument is the return of a previous
 * bdb_osql_log_first or bdb_osql_log_next
 *
 */
bdb_osql_log_t *bdb_osql_log_next(bdb_osql_log_t *log,
                                  unsigned long long *genid);

/**
 * Returns the oldest genid updated by "log"
 *
 */
unsigned long long bdb_osql_log_get_genid(bdb_osql_log_t *log);

/**
 *  Check the log and clean any completely processed logs
 *
 */
int bdb_osql_log_clean_logs(int *bdberr);

/**
 * Insert a log in the repository
 * (NOTE: logs are first created and populated, before being
 *  inserted in the list)
 * During insert, the clients are checked and updated if needed
 */
int bdb_osql_log_insert(bdb_state_type *bdb_state, bdb_osql_log_t *log,
                        int *bdberr, int is_master);

/**
 * Thread-safe return for the next pointer
 * This is called by sql threads
 * (trn->last_copy is used as hint here, we might lock
 * when we don't have to, but only for the last log)
 * we need this to avoid logs are not skipped for all
 * snapshot/serializable sessions
 *
 */
bdb_osql_log_t *bdb_osql_log_next_synced(bdb_osql_log_t *log, int *bdberr);

/**
 * Apply this log to this transaction
 * If any record of the log is applied, sets "dirty"
 *
 */
int bdb_osql_log_apply_log(bdb_cursor_impl_t *cur, DB_LOGC *logcur,
                           bdb_osql_log_t *log, struct bdb_osql_trn *trn,
                           int *dirty, log_ops_t log_op, int trak,
                           int *bdberr);

/**
 * The record associated with this blob is being read from the data-table, but
 * is NULL in both the blob-data and the blob-shadow files.  Add a NULL blob to
 * the blob-shadow table so that we don't have to search both.
 */
int bdb_osql_set_null_blob_in_shadows(bdb_cursor_impl_t *cur,
                                      struct bdb_osql_trn *trn,
                                      unsigned long long genid, int dbnum,
                                      int blobno, int *bdberr);

/**
 * Retrieve each log record for a provided lsn and create a osqllog
 * to be used for either backfilling or updating the snapshot of a
 * snapshot/serializable transaction
 *
 */
bdb_osql_log_t *parse_log_for_shadows(bdb_state_type *bdb_state, DB_LOGC *cur,
                                      DB_LSN *last_logical_lsn, int is_bkfill,
                                      int *bdberr);

/**
 * Same as old parse_log_for_shadows
 * Decided to create a new one since it diverts considerably from the old one
 *
 */
bdb_osql_log_t *parse_log_for_snapisol(bdb_state_type *bdb_state, DB_LOGC *cur,
                                       DB_LSN *last_logical_lsn, int is_bkfill,
                                       int *bdberr);

/**
 * update shadows with the given lsn of a logical operation
 *
 */
int bdb_osql_update_shadows_with_pglogs(bdb_cursor_impl_t *cur, DB_LSN lsn,
                                        struct bdb_osql_trn *trn, int *dirty,
                                        int trak, int *bdberr);

/**
 * Sync-ed return of last log from log_repo
 *
 */
struct bdb_osql_log *bdb_osql_log_last(int *bdberr);

/**
 * Retrieve a "row" from the log, using the shadow dta pointer
 *
 */
int bdb_osql_log_get_optim_data(bdb_state_type *bdb_state, DB_LSN *lsn,
                                void **row, int *rowlen, int *bdberr);

/**
 * Retrieve a "row", as above, but leave room for the addcur header.
 *
 */
int bdb_osql_log_get_optim_data_addcur(bdb_state_type *bdb_state, DB_LSN *lsn,
                                       void **row, int *rowlen, int *bdberr);
/**
 * Check if a buffer is a log dta pointer
 */
int bdb_osql_log_is_optim_data(void *dta);

/**
 * Determine if a log is applyable for this
 * transaction.
 * NOTE: for inplace updates, new records
 * look old, so the check is actually more
 * conservative (so working, albeit not perfect)
 *
 */
int bdb_osql_log_undo_required(tran_type *tran, bdb_osql_log_t *log);

/**
 *  Unregister logs file from first to last ;
 *
 */
int bdb_osql_log_unregister(tran_type *tran, bdb_osql_log_t *firstlog,
                            bdb_osql_log_t *lastlog, int trak);

/* Modeled after generate_series */
typedef struct bdb_llog_cursor bdb_llog_cursor;
struct bdb_llog_cursor {
    DB_LSN curLsn; /* Current LSN */
    DB_LSN minLsn; /* Minimum LSN */
    DB_LSN maxLsn; /* Maximum LSN */
    int hitLast;
    int openCursor;
    int subop;
    DB_LOGC *logc; /* Log Cursor */
    int getflags;
    DBT data;
    bdb_osql_log_t *log;
};

void bdb_llog_cursor_reset(bdb_llog_cursor *pCur);
int bdb_llog_cursor_open(bdb_llog_cursor *pCur);
void bdb_llog_cursor_close(bdb_llog_cursor *pCur);
int bdb_llog_cursor_first(bdb_llog_cursor *pCur);
int bdb_llog_cursor_next(bdb_llog_cursor *pCur);
int bdb_llog_cursor_find(bdb_llog_cursor *pCur, DB_LSN *lsn);
void bdb_llog_cursor_cleanup(bdb_llog_cursor *pCur);
#endif
