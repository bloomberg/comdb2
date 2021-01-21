/*
   Copyright 2021, Bloomberg Finance L.P.

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

#ifndef PROGRESS_TRACKER_H
#define PROGRESS_TRACKER_H

#include <stdint.h>
#include <string.h>
#include "plhash.h"
#include "list.h"
#include "cdb2api.h"

typedef struct bdb_state_tag bdb_state_type;

extern hash_t *gbl_progress_tracker;
extern pthread_mutex_t progress_tracker_mu;

enum progress_op_type {
    PROGRESS_OP_CREATE_TABLE,
    PROGRESS_OP_ALTER_TABLE,
    PROGRESS_OP_VERIFY_TABLE,
    PROGRESS_OP_ANALYZE_ALL,
    PROGRESS_OP_ANALYZE_TABLE,
    PROGRESS_OP_TYPE_MAX,
};

enum progress_status {
    PROGRESS_STARTED,
    PROGRESS_RUNNING,
    PROGRESS_PAUSED,
    PROGRESS_WAITING,
    PROGRESS_ABORTED,
    PROGRESS_FAILED,
    PROGRESS_COMPLETED,
};

enum progress_stage {
    /* CREATE TABLE */
    PROGRESS_CREATE_TABLE = 0,
    PROGRESS_CREATE_TABLE_MAX,

    /* ALTER TABLE */
    PROGRESS_ALTER_TABLE = 0,
    PROGRESS_ALTER_TABLE_CONVERT_RECORDS,
    PROGRESS_ALTER_TABLE_UPGRADE_RECORDS,
    PROGRESS_ALTER_TABLE_LOGICAL_REDO,
    PROGRESS_ALTER_TABLE_MAX,

    /* VERIFY */
    PROGRESS_VERIFY_TABLE = 0,
    PROGRESS_VERIFY_PROCESS_SEQUENTIAL,
    PROGRESS_VERIFY_PROCESS_DATA_STRIPE,
    PROGRESS_VERIFY_PROCESS_KEY,
    PROGRESS_VERIFY_PROCESS_BLOB,
    PROGRESS_VERIFY_TABLE_MAX,

    /* ANALYZE */
    PROGRESS_ANALYZE_TABLE = 0,
    PROGRESS_ANALYZE_TABLE_SAMPLING_INDICES,
    PROGRESS_ANALYZE_TABLE_SAMPLING_INDEX,
    PROGRESS_ANALYZE_TABLE_SQLITE_ANALYZE,
    PROGRESS_ANALYZE_TABLE_ANALYZING_RECORDS,
    PROGRESS_ANALYZE_TABLE_UPDATING_STATS,
    PROGRESS_ANALYZE_TABLE_MAX,
};

struct progress_tracker {
    char seed[20];
    int type;
    char *name;
    LISTC_T(struct progress_thread) threads;
};

typedef struct progress_entry {
    int thread_id;
    int thread_sub_id;
    const char *type;
    char *name;
    char *sub_name;
    char *seed;
    const char *stage;
    const char *status;
    cdb2_client_datetime_t start_time;
    cdb2_client_datetime_t end_time;
    uint64_t processed_records;
    uint64_t total_records;
    int rate_per_sec;
    int remaining_time_sec;
} progress_entry_t;

int init_progress_tracker();
int deinit_progress_tracker();

const char *get_progress_type_str(int type);
const char *get_progress_stage_str(int op, int stage);
const char *get_progress_status_str(int status);
int get_progress_max_stage(int op);

int progress_tracking_start(int type, uint64_t seed, const char *name);
int progress_tracking_end(uint64_t seed);
int progress_tracking_worker_start(uint64_t seed, int stage);
int progress_tracking_worker_end(uint64_t seed, int status);

void *progress_tracking_update(uint64_t seed, int stage, int status,
                               const char *name);
void *progress_tracking_get_last_attribute(uint64_t seed);
void progress_tracking_update_total_records(void *, uint64_t count);
void progress_tracking_update_processed_records(void *, uint64_t count);
void progress_tracking_compute_total_records(uint64_t seed,
                                             bdb_state_type *state, int ixnum,
                                             int dtastripe);
void progress_tracking_update_stats(void *);
int progress_tracker_copy_data(void **data, int *npoints);

#endif /* PROGRESS_TRACKER_H */
