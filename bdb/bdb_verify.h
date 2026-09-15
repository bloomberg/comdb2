/*
   Copyright 2015-2020 Bloomberg Finance L.P.

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

#ifndef __bdb_verify_h
#define __bdb_verify_h

#include <pthread.h>

#include "verify.h"

struct bdb_state_type;
typedef struct thdpool thdpool;

/* A record the data scan found with no entry in one of its indexes.  Collected
 * during the scan and repaired afterwards, when nothing holds page locks. */
typedef struct {
    unsigned long long genid;
    int ix;
} verify_missing_key_t;

/* Past this many we stop collecting and say so - a table that far gone wants a
 * rebuild, not a few hundred thousand single-key transactions. */
#define VERIFY_MAX_MISSING_KEYS 100000

/* Room for what a repair attempt has to say about one key.  Kept well under
 * LINE_MAX so it still fits once the caller prefixes the genid and index. */
#define VERIFY_FIX_MSG_LEN 256

typedef enum {
    PROCESS_SEQUENTIAL,
    PROCESS_DATA,
    PROCESS_KEY,
    PROCESS_BLOB
} processing_type;

// common data for all verify threads
typedef struct {
    bdb_state_type *bdb_state;
    struct dbtable *db_table;
    const char *tablename;
    int (*partial_datacopy_callback)(const struct dbtable *tbl, const int pd_ix, const void *inbuf, void *outbuf);
    int (*formkey_callback)(const struct dbtable *tbl, void *dta, void *blob_parm,
                            int ix, void *keyout, int *keysz);
    int (*get_blob_sizes_callback)(const struct dbtable *tbl, void *dta, int blobs[16],
                                   int bloboffs[16], int *nblobs);
    int (*vtag_callback)(void *parm, void *dta, int *dtasz, uint8_t ver);
    int (*add_blob_buffer_callback)(void *parm, void *dta, int dtasz, int blobno);
    void (*free_blob_buffer_callback)(void *parm);
    unsigned long long (*verify_indexes_callback)(void *parm, void *dta, void *blob_parm);
    /* Re-add the index entry for genid in index ix.  Re-reads and re-checks
     * everything under its own transaction, so a missing key that was only a
     * concurrent writer mid-update is left alone.  Returns 0 if a key was
     * added, 1 if no repair was needed, -1 on error; fills msg either way. */
    int (*add_missing_key_callback)(const struct dbtable *tbl, int ix, unsigned long long genid, char *msg,
                                    size_t msglen);
    char *header; // header string for printing for prog rep in default mode
    uint64_t items_processed;             // atomic inc: for progres report
    uint64_t saved_progress;              // previous progress counter
    uint64_t records_processed;           // progress report in default mode
    int nrecs_progress;                   // progress done in this time window
    unsigned int last_connection_check;   // last reported time in ms
    int progress_report_seconds;          // freq of report in seconds
    int progress_report_counter;          // counter used to print progress
    int attempt_fix;
    int fix_missing_keys;    // add index entries records should have had
    int target_ix;           // only this index, VERIFY_ALL_IXNUM for all
    unsigned int keys_fixed; // index entries added back
    // missing keys seen by the data scan, guarded by missing_keys_lk
    pthread_mutex_t missing_keys_lk;
    verify_missing_key_t *missing_keys;
    unsigned int nmissing_keys;
    unsigned int missing_keys_alloc;
    unsigned int missing_keys_dropped; // seen past VERIFY_MAX_MISSING_KEYS
    unsigned int threads_spawned;
    unsigned int threads_completed; // atomic inc
    verify_mode_t verify_mode;
    uint8_t client_dropped_connection;
    uint8_t lock_desired;
    uint8_t verify_status; // 0 success, 1 failure
    verify_peer_check_func *peer_check;
    verify_response_func *verify_response;
    void *arg;
} verify_common_t;

// verify per thread processing info
typedef struct td_processing_info {
    verify_common_t *common_params;
    processing_type type;
    int8_t blobno;
    int8_t dtastripe;
    int8_t index;
} td_processing_info_t;

void bdb_verify_enqueue(td_processing_info_t *, thdpool *);
void verify_record_missing_key(verify_common_t *, unsigned long long genid, int ix);

#endif
