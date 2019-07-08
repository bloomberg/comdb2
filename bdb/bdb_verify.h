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

struct SBUF2;
struct bdb_state_type;
typedef struct dbtable dbtable;

typedef enum {PROCESS_DATA, PROCESS_KEY, PROCESS_BLOB} processing_type;
typedef struct processing_info {
    processing_type type;
    int8_t blobno;
    int8_t dtastripe;
    int8_t index;
} processing_info;

typedef struct {
    SBUF2 *sb;
    bdb_state_type *bdb_state;
    dbtable *db_table;
    int (*formkey_callback)(const dbtable *tbl, void *dta, void *blob_parm, int ix,
                            void *keyout, int *keysz);
    int (*get_blob_sizes_callback)(const dbtable *tbl, void *dta, int blobs[16],
                                   int bloboffs[16], int *nblobs);
    int (*vtag_callback)(void *parm, void *dta, int *dtasz, uint8_t ver);
    int (*add_blob_buffer_callback)(void *parm, void *dta, int dtasz,
                                    int blobno);
    void (*free_blob_buffer_callback)(void *parm);
    unsigned long long (*verify_indexes_callback)(void *parm, void *dta,
                                                  void *blob_parm);
    void *callback_parm;
    int (*lua_callback)(void *, const char *);
    void *lua_params;
    processing_info *info;
    uint8_t *verify_status; //0 success, 1 failure
    int progress_report_seconds;
    int attempt_fix;
    uint8_t parallel_verify;
    uint8_t client_dropped_connection;
} verify_td_params;


int bdb_verify(verify_td_params *par);


#endif
