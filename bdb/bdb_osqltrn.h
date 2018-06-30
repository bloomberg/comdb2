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

#ifndef __BDB_OSQL_TRN_H_
#define __BDB_OSQL_TRN_H_

struct tran_tag;
struct bdb_osql_log;

struct bdb_osql_trn;
typedef struct bdb_osql_trn bdb_osql_trn_t;

/**
 * Create the snapshot/serializable transaction repository
 *
 */
int bdb_osql_trn_repo_init(int *bdberr);

/**
 * Destroy the snapshot/serializable transaction repository
 *
 */
int bdb_osql_trn_repo_destroy(int *bdberr);

/**
 * lock/unlock the snapshot/serializable transaction repository
 *
 */
void bdb_verify_repo_lock();
int bdb_osql_trn_repo_lock();
int bdb_osql_trn_repo_unlock();

/**
 *  Register a shadow transaction with the repository
 *  Called upon transaction begin/start
 *
 */
bdb_osql_trn_t *bdb_osql_trn_register(bdb_state_type *bdb_state,
                                      struct tran_tag *shadow_tran, int trak,
                                      int *bdberr, int epoch, int file,
                                      int offset, int is_ha_retry);

/**
 *  Unregister a shadow transaction with the repository
 *  Called upon transaction commit/rollback
 *
 */
int bdb_osql_trn_unregister(bdb_osql_trn_t *trn, int *bdberr);

/**
 * Check all the shadow transactions and register them
 * with the log if any are viable (oldest_genid<birth_genid)
 * If there are no clients for this transaction, set empty
 *
 */
int bdb_osql_trn_check_clients(struct bdb_osql_log *log, int *empty,
                               int lock_repo, int *bdberr);

/**
 * Check all the shadow transactions and register them
 * with the fileid + pgno + lsn key
 *
 */
struct page_logical_lsn_key;

/**
 * Check all the shadow transactions and
 * see if deleting log file will affect active begin-as-of transaction
 */
int bdb_osql_trn_asof_ok_to_delete_log(int filenum);

/**
 * set global recoverabel lsn
 */
void bdb_set_gbl_recoverable_lsn(void *lsn, int32_t timestamp);

/**
 * Do a quick count of all the clients - don't bother parsing
 * the log if there are none.
 */
int bdb_osql_trn_count_clients(int *count, int lock_repo, int *bdberr);

/**
 * Returns the first log
 *
 */
struct bdb_osql_log *bdb_osql_trn_first_log(bdb_osql_trn_t *trn, int *bdberr);

/**
 * Returns the next log
 *
 */
struct bdb_osql_log *bdb_osql_trn_next_log(bdb_osql_trn_t *trn,
                                           struct bdb_osql_log *log,
                                           int *bdberr);

/**
 * Atomically set/reset the log for this transaction
 * This function syncs with replication thread to ensure
 * no logs are skipped
 * It is called only after the transaction "last_copy" log
 * was processed
 *
 */
int bdb_osql_trn_reset_log(bdb_osql_trn_t *trn, int *bdberr);

/**
 * Returns the shadow_tran
 *
 */
struct tran_tag *bdb_osql_trn_get_shadow_tran(bdb_osql_trn_t *trn);

/**
 * Enable/disable tracking for trn repo
 *
 */
void bdb_osql_trn_trak(int flag);

/**
 * Mark "aborted" all snapisol transactions that point to "log"
 * as first argument
 * (Upon updating the shadows, each transaction will actually abort)
 *
 */
int bdb_osql_trn_cancel_clients(struct bdb_osql_log *log, int lock_repo,
                                int *bdberr);

int bdb_oldest_active_lsn(bdb_state_type *bdb_state, void *plsn);
/**
 * Returns 1 if the transaction was cancelled due to resource limitations
 *
 */
int bdb_osql_trn_is_cancelled(bdb_osql_trn_t *trn);

/**
 * Returns the lowest lsn that is required for an active snapisol/serializable
 * transaction;
 * No log containing this or following is deletable (unless we hit the log
 * resource limit and start cancelling transactions)
 */
int bdb_osql_trn_get_lwm(bdb_state_type *bdb_state, void *lsn);

#endif
