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

#ifndef __bdb_verify_h
#define __bdb_verify_h

#include "verify.h"

struct SBUF2;
struct bdb_state_type;
typedef struct thdpool thdpool;

typedef enum { PROCESS_DATA, PROCESS_KEY, PROCESS_BLOB } processing_type;

// common data for all verify threads
typedef struct {
    SBUF2 *sb;
    bdb_state_type *bdb_state;
    dbtable *db_table;
    int (*formkey_callback)(const dbtable *tbl, void *dta, void *blob_parm,
                            int ix, void *keyout, int *keysz);
    int (*get_blob_sizes_callback)(const dbtable *tbl, void *dta, int blobs[16],
                                   int bloboffs[16], int *nblobs);
    int (*vtag_callback)(void *parm, void *dta, int *dtasz, uint8_t ver);
    int (*add_blob_buffer_callback)(void *parm, void *dta, int dtasz,
                                    int blobno);
    void (*free_blob_buffer_callback)(void *parm);
    unsigned long long (*verify_indexes_callback)(void *parm, void *dta,
                                                  void *blob_parm);
    int (*lua_callback)(void *, const char *);
    void *lua_params;
    char *header; // header string for printing for prog rep in default mode
    unsigned long long items_processed;   // atomic inc: for progres report
    unsigned long long records_processed; // progress report in default mode
    unsigned long long saved_progress;    // previous progress counter
    int nrecs_progress;                   // progress done in this time window
    int last_reported;                    // last reported time
    int progress_report_seconds;
    int attempt_fix;
    unsigned short threads_spawned;
    unsigned short threads_completed; // atomic inc
    verify_mode_t verify_mode;
    uint8_t client_dropped_connection;
    uint8_t verify_status; // 0 success, 1 failure
} verify_common_t;

// verify per thread processing info
typedef struct td_processing_info {
    verify_common_t *common_params;
    processing_type type;
    int8_t blobno;
    int8_t dtastripe;
    int8_t index;
} td_processing_info_t;

int bdb_verify(verify_common_t *par);
int bdb_verify_enqueue(td_processing_info_t *info, thdpool *verify_thdpool);
int bdb_dropped_connection(SBUF2 *sb);

#endif
