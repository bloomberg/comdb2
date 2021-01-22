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

#include <stdlib.h>
#include <pthread.h>
#include "progress_tracker.h"
#include "tohex.h"
#include "lockmacros.h"
#include "types.h"
#include "sqlite3.h"
#include "bdb_int.h"

hash_t *gbl_progress_tracker;
pthread_mutex_t progress_tracker_mu = PTHREAD_MUTEX_INITIALIZER;

unsigned long long get_epochms(void);

struct progress_attribute {
    int stage;
    int status;
    char *sub_name;

    char *errmsg;

    uint64_t start_time_ms;
    uint64_t end_time_ms;
    uint64_t remaining_time_ms;

    uint64_t total_records;
    uint64_t processed_records;
    float rate_per_ms;

    LINKC_T(struct progress_attribute) lnk;
};

struct work {
    bdb_state_type *state;
    int ixnum;
    int dtastripe;
    uint64_t *count;
};

struct progress_thread {
    pthread_t thread_id;
    long int thread_sub_id;
    int sql_thd_id;
    char *sql_thd_cnonce;

    pthread_t counter_thd;
    struct work work;
    int wait_for_counter_thd;

    LINKC_T(struct progress_thread) lnk;

    LISTC_T(struct progress_attribute) attribs;
};

int init_progress_tracker()
{
    gbl_progress_tracker =
        hash_init_str(offsetof(struct progress_tracker, seed));
    return 0;
}

int deinit_progress_tracker()
{
    return 0;
}

static struct progress_tracker *new_progress_tracker(int type, uint64_t seed,
                                                     const char *name)
{
    struct progress_tracker *tracker =
        calloc(sizeof(struct progress_tracker), 1);
    if (!tracker) {
        return NULL;
    }

    sprintf(tracker->seed, "%0#16" PRIx64, seed);
    tracker->type = type;
    tracker->name = strdup(name);

    listc_init(&tracker->threads, offsetof(struct progress_thread, lnk));

    Pthread_mutex_lock(&progress_tracker_mu);
    hash_add(gbl_progress_tracker, tracker);
    Pthread_mutex_unlock(&progress_tracker_mu);

    return tracker;
}

static struct progress_tracker *get_tracker(uint64_t seed)
{
    char seed_str[20];
    struct progress_tracker *tracker;

    sprintf(seed_str, "%0#16" PRIx64, seed);
    Pthread_mutex_lock(&progress_tracker_mu);
    tracker = hash_find_readonly(gbl_progress_tracker, &seed_str);
    Pthread_mutex_unlock(&progress_tracker_mu);

    return tracker;
}

static struct progress_thread *
new_progress_thread(struct progress_tracker *tracker)
{
    struct progress_thread *new_thread;
    struct progress_thread *thread;
    struct progress_thread *last = NULL;

    new_thread = calloc(sizeof(struct progress_thread), 1);

    if (!new_thread) {
        return NULL;
    }

    new_thread->thread_id = pthread_self();
    new_thread->thread_sub_id = 0;
    listc_init(&new_thread->attribs, offsetof(struct progress_attribute, lnk));

    Pthread_mutex_lock(&progress_tracker_mu);
    LISTC_FOR_EACH(&tracker->threads, thread, lnk)
    {
        if (thread->thread_id == new_thread->thread_id) {
            last = thread;
        }
    }
    listc_abl(&tracker->threads, new_thread);
    if (last)
        new_thread->thread_sub_id = last->thread_sub_id + 1;
    Pthread_mutex_unlock(&progress_tracker_mu);

    return new_thread;
}

static struct progress_attribute *
append_progress_attribute(struct progress_thread *thd, int stage, int status,
                          const char *sub_name)
{
    struct progress_attribute *attrib =
        calloc(sizeof(struct progress_attribute), 1);
    if (!attrib) {
        return 0;
    }

    attrib->stage = stage;
    attrib->status = status;
    attrib->start_time_ms = get_epochms();
    if (sub_name) {
        attrib->sub_name = strdup(sub_name);
    }

    Pthread_mutex_lock(&progress_tracker_mu);
    listc_abl(&thd->attribs, attrib);
    Pthread_mutex_unlock(&progress_tracker_mu);

    return attrib;
}

const char *get_progress_type_str(int type)
{
    switch (type) {
    case PROGRESS_OP_CREATE_TABLE:
        return "create table";
    case PROGRESS_OP_ALTER_TABLE:
        return "alter table";
    case PROGRESS_OP_VERIFY_TABLE:
        return "verify";
    case PROGRESS_OP_ANALYZE_ALL:
        return "analyze all";
    case PROGRESS_OP_ANALYZE_TABLE:
        return "analyze";
    default:
        break;
    }
    return "unknown";
}

const char *get_progress_status_str(int status)
{
    switch (status) {
    case PROGRESS_STARTED:
        return "started";
    case PROGRESS_RUNNING:
        return "running";
    case PROGRESS_PAUSED:
        return "paused";
    case PROGRESS_WAITING:
        return "waiting";
    case PROGRESS_ABORTED:
        return "aborted";
    case PROGRESS_FAILED:
        return "failed";
    case PROGRESS_COMPLETED:
        return "done";
    default:
        break;
    }
    return "unknown";
};

const char *get_progress_stage_str(int op, int stage)
{
    switch (op) {
    case PROGRESS_OP_CREATE_TABLE:
        switch (stage) {
        case PROGRESS_CREATE_TABLE:
            return "create table";
        }
    case PROGRESS_OP_ALTER_TABLE:
        switch (stage) {
        case PROGRESS_ALTER_TABLE:
            return "alter table";
        case PROGRESS_ALTER_TABLE_CONVERT_RECORDS:
            return "converting records";
        case PROGRESS_ALTER_TABLE_UPGRADE_RECORDS:
            return "upgrading records";
        case PROGRESS_ALTER_TABLE_LOGICAL_REDO:
            return "redoing changes";
        }
    case PROGRESS_OP_VERIFY_TABLE:
        switch (stage) {
        case PROGRESS_VERIFY_TABLE:
            return "verify table";
        case PROGRESS_VERIFY_PROCESS_SEQUENTIAL:
            return "sequential processing";
        case PROGRESS_VERIFY_PROCESS_DATA_STRIPE:
            return "processing data stripe";
        case PROGRESS_VERIFY_PROCESS_KEY:
            return "processing index";
        case PROGRESS_VERIFY_PROCESS_BLOB:
            return "processing blob";
        }
    case PROGRESS_OP_ANALYZE_ALL:
        abort();
    case PROGRESS_OP_ANALYZE_TABLE:
        switch (stage) {
        case PROGRESS_ANALYZE_TABLE:
            return "analyze table";
        case PROGRESS_ANALYZE_TABLE_SAMPLING_INDICES:
            return "sampling indices";
        case PROGRESS_ANALYZE_TABLE_SAMPLING_INDEX:
            return "sampling index";
        case PROGRESS_ANALYZE_TABLE_SQLITE_ANALYZE:
            return "sqlite analyze";
        case PROGRESS_ANALYZE_TABLE_ANALYZING_RECORDS:
            return "submit analyzesqlite";
        case PROGRESS_ANALYZE_TABLE_UPDATING_STATS:
            return "updating stats";
        }
    default:
        break;
    }

    return "unknown";
}

int get_progress_max_stage(int op)
{
    switch (op) {
    case PROGRESS_OP_CREATE_TABLE:
        return PROGRESS_CREATE_TABLE_MAX;
    case PROGRESS_OP_ALTER_TABLE:
        return PROGRESS_ALTER_TABLE_MAX;
    case PROGRESS_OP_VERIFY_TABLE:
        return PROGRESS_VERIFY_TABLE_MAX;
    case PROGRESS_OP_ANALYZE_ALL:
        abort();
    case PROGRESS_OP_ANALYZE_TABLE:
        return PROGRESS_ANALYZE_TABLE_MAX;
    default:
        break;
    }
    abort();
    return -1;
}

int progress_tracking_start(int type, uint64_t seed, const char *name)
{
    struct progress_tracker *tracker;
    struct progress_thread *thread;

    tracker = get_tracker(seed);
    if (tracker) {
        return 0;
    }

    tracker = new_progress_tracker(type, seed, name);
    thread = new_progress_thread(tracker);
    int stage;
    switch (type) {
    case PROGRESS_OP_CREATE_TABLE:
        stage = PROGRESS_CREATE_TABLE;
        break;
    case PROGRESS_OP_ALTER_TABLE:
        stage = PROGRESS_ALTER_TABLE;
        break;
    case PROGRESS_OP_VERIFY_TABLE:
        stage = PROGRESS_VERIFY_TABLE;
        break;
    case PROGRESS_OP_ANALYZE_TABLE:
        stage = PROGRESS_ANALYZE_TABLE;
        break;
    default:
        abort();
    }

    append_progress_attribute(thread, stage, PROGRESS_STARTED, NULL);
    return 0;
}

int progress_tracking_end(uint64_t seed)
{
    struct progress_tracker *tracker;
    struct progress_thread *thread;
    struct progress_thread *first = NULL;
    struct progress_thread *last = NULL;
    struct progress_attribute *attrib;
    pthread_t thread_id;

    tracker = get_tracker(seed);
    if (!tracker) {
        logmsg(LOGMSG_ERROR, "%s:%d progress tracker object missing\n",
               __func__, __LINE__);
        return -1;
    }

    thread_id = pthread_self();

    LISTC_FOR_EACH(&tracker->threads, thread, lnk)
    {
        if (!first)
            first = thread;

        if (thread->thread_id == thread_id) {
            last = thread;
        }
    }

    if (last) {
        thread = last;
    } else {
        logmsg(LOGMSG_ERROR,
               "%s:%d progress tracker object for the thread is missing\n",
               __func__, __LINE__);
        thread = first;
    }

    /* Update the end time for the last attribute in the list. */
    struct progress_attribute *last_attrib = NULL;
    LISTC_FOR_EACH(&thread->attribs, attrib, lnk)
    {
        last_attrib = attrib;
    }

    if (last_attrib) {
        /* Status */
        last_attrib->status = PROGRESS_COMPLETED;

        /* End time */
        last_attrib->end_time_ms = get_epochms();
    }

    return 0;
}

int progress_tracking_worker_start(uint64_t seed, int stage)
{
    struct progress_tracker *tracker;
    struct progress_thread *thread;

    tracker = get_tracker(seed);
    if (!tracker) {
        logmsg(LOGMSG_ERROR, "%s:%d progress tracker object missing\n",
               __func__, __LINE__);
        return -1;
    }

    thread = new_progress_thread(tracker);
    append_progress_attribute(thread, stage, PROGRESS_STARTED, NULL);
    return 0;
}

int progress_tracking_worker_end(uint64_t seed, int status)
{
    struct progress_thread *thread;
    struct progress_thread *last = NULL;
    struct progress_attribute *attrib;
    struct progress_tracker *tracker;
    pthread_t thread_id;

    tracker = get_tracker(seed);
    if (!tracker) {
        logmsg(LOGMSG_ERROR, "%s:%d progress tracker object missing\n",
               __func__, __LINE__);
        return -1;
    }

    thread_id = pthread_self();

    LISTC_FOR_EACH(&tracker->threads, thread, lnk)
    {
        if (thread->thread_id == thread_id) {
            last = thread;
        }
    }

    if (last) {
        thread = last;
    } else {
        logmsg(LOGMSG_ERROR,
               "%s:%d progress tracker object for the thread is missing\n",
               __func__, __LINE__);
        return -1;
    }

    /* Update the last attribute in the list. */
    struct progress_attribute *last_attrib = NULL;
    LISTC_FOR_EACH(&thread->attribs, attrib, lnk)
    {
        last_attrib = attrib;
    }

    if (last_attrib) {
        /* Status */
        last_attrib->status = status;

        /* End time */
        last_attrib->end_time_ms = get_epochms();
    }

    /* Wait for counter thread to complete. */
    if (status == PROGRESS_COMPLETED && thread->wait_for_counter_thd) {
        pthread_join(thread->counter_thd, NULL);
        thread->wait_for_counter_thd = 0;
    }

    return 0;
}

/* Returns the last updated attribute */
void *progress_tracking_update(uint64_t seed, int stage, int status,
                               const char *name)
{
    struct progress_thread *thread;
    struct progress_thread *last = NULL;
    struct progress_attribute *attrib, *last_updated_attrib = NULL;
    struct progress_tracker *tracker;
    pthread_t thread_id;

    tracker = get_tracker(seed);
    if (!tracker) {
        logmsg(LOGMSG_ERROR, "%s:%d progress tracker object missing\n",
               __func__, __LINE__);
        return 0;
    }

    thread_id = pthread_self();

    LISTC_FOR_EACH(&tracker->threads, thread, lnk)
    {
        if (thread->thread_id == thread_id) {
            last = thread;
        }
    }

    if (last) {
        thread = last;
    } else {
        logmsg(LOGMSG_ERROR,
               "%s:%d progress tracker object for the thread is missing\n",
               __func__, __LINE__);
        return 0;
    }

    /* Update the last attribute in the list. */
    struct progress_attribute *last_attrib = NULL;
    LISTC_FOR_EACH(&thread->attribs, attrib, lnk)
    {
        last_attrib = attrib;
    }

    if (last_attrib) {
        /* Add a new attribute (entry) only if we are entering a different
         * stage. */
        if (last_attrib->stage != stage ||
            last_attrib->status == PROGRESS_COMPLETED) {
            if (!name) {
                name = last_attrib->sub_name;
            }
            last_updated_attrib =
                append_progress_attribute(thread, stage, status, name);

            /* Now that we have added a new entry, let's also update the status
             * and end time for last stage. */
            last_attrib->status = PROGRESS_COMPLETED;
            last_attrib->end_time_ms = get_epochms();
        } else { /* Update the last stage. */
            /* Status */
            last_attrib->status = status;

            /* Sub-name */
            if (name) {
                free(last_attrib->sub_name);
                last_attrib->sub_name = strdup(name);
            }

            /* End time */
            if (status == PROGRESS_COMPLETED) {
                last_attrib->end_time_ms = get_epochms();
            }

            last_updated_attrib = last_attrib;
        }
    }

    /* Wait for counter thread to complete. */
    if (status == PROGRESS_COMPLETED && thread->wait_for_counter_thd) {
        pthread_join(thread->counter_thd, NULL);
        thread->wait_for_counter_thd = 0;
    }

    return last_updated_attrib;
}

void *progress_tracking_get_last_attribute(uint64_t seed)
{
    struct progress_thread *thread;
    struct progress_thread *last = NULL;
    struct progress_tracker *tracker;
    pthread_t thread_id;

    tracker = get_tracker(seed);
    if (!tracker) {
        logmsg(LOGMSG_ERROR, "%s:%d progress tracker object missing\n",
               __func__, __LINE__);
        return 0;
    }

    thread_id = pthread_self();

    LISTC_FOR_EACH(&tracker->threads, thread, lnk)
    {
        if (thread->thread_id == thread_id) {
            last = thread;
        }
    }

    if (last) {
        thread = last;
    } else {
        logmsg(LOGMSG_ERROR,
               "%s:%d progress tracker object for the thread is missing\n",
               __func__, __LINE__);
        return 0;
    }

    /* Update the last attribute in the list. */
    struct progress_attribute *attrib, *last_attrib = NULL;
    LISTC_FOR_EACH(&thread->attribs, attrib, lnk)
    {
        last_attrib = attrib;
    }

    return last_attrib;
}

static void *record_counter_thd(void *arg)
{
    int rc;
    struct work *work;

    work = (struct work *)arg;
    rc = bdb_direct_count(work->state, work->ixnum, work->dtastripe,
                          (int64_t *)work->count);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s:%d failed to get row count for ixnum:%d data stripe: %d\n",
               __func__, __LINE__, work->ixnum, work->dtastripe);
    }
    return NULL;
}

void progress_tracking_compute_total_records(uint64_t seed,
                                             bdb_state_type *state, int ixnum,
                                             int dtastripe)
{
    int rc;
    struct progress_thread *thread;
    struct progress_thread *last = NULL;
    struct progress_tracker *tracker;
    pthread_t thread_id;

    tracker = get_tracker(seed);
    if (!tracker) {
        logmsg(LOGMSG_ERROR, "%s:%d progress tracker object missing\n",
               __func__, __LINE__);
        return;
    }

    thread_id = pthread_self();

    LISTC_FOR_EACH(&tracker->threads, thread, lnk)
    {
        if (thread->thread_id == thread_id) {
            last = thread;
        }
    }

    if (last) {
        thread = last;
    } else {
        logmsg(LOGMSG_ERROR,
               "%s:%d progress tracker object for the thread is missing\n",
               __func__, __LINE__);
        return;
    }

    /* Update the last attribute in the list. */
    struct progress_attribute *attrib, *last_attrib = NULL;
    LISTC_FOR_EACH(&thread->attribs, attrib, lnk)
    {
        last_attrib = attrib;
    }

    thread->work.state = state;
    thread->work.ixnum = ixnum;
    thread->work.dtastripe = dtastripe;
    if (last_attrib) /* Safety */
        thread->work.count = &last_attrib->total_records;

    rc = pthread_create(&thread->counter_thd, NULL, record_counter_thd,
                        (void *)&thread->work);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d pthread_create() failed (rc: %d)\n",
               __func__, __LINE__, rc);
    } else {
        thread->wait_for_counter_thd = 1;
    }
    return;
}

void progress_tracking_update_total_records(void *attrib, uint64_t count)
{
    if (!attrib)
        return;
    ((struct progress_attribute *)attrib)->total_records = count;
}

void progress_tracking_update_processed_records(void *attrib, uint64_t count)
{
    if (!attrib)
        return;
    ((struct progress_attribute *)attrib)->processed_records = count;
}

void progress_tracking_update_stats(void *a)
{
    struct progress_attribute *attrib = a;
    uint64_t current_time_ms = get_epochms();

    /* Calculate rate of processing */
    attrib->rate_per_ms = (float)(attrib->processed_records) /
                          (current_time_ms - attrib->start_time_ms);

    if (attrib->processed_records == attrib->total_records) {
        attrib->remaining_time_ms = 0;
    } else if (attrib->total_records == 0) {
        attrib->remaining_time_ms = 0;
    } else if (attrib->processed_records > 0 && attrib->rate_per_ms > 0.0) {
        /* Calculate expected time of completion */
        attrib->remaining_time_ms =
            (attrib->total_records - attrib->processed_records) /
            attrib->rate_per_ms;
    }
}

int progress_tracker_copy_data(void **data, int *npoints)
{
    struct progress_tracker *tracker;
    struct progress_thread *thread;
    struct progress_attribute *attrib;
    void *ent;
    unsigned int bkt;
    int count = 0;

    *npoints = 0;
    *data = NULL;

    Pthread_mutex_lock(&progress_tracker_mu);
    for (tracker = (struct progress_tracker *)hash_first(gbl_progress_tracker,
                                                         &ent, &bkt);
         tracker; tracker = (struct progress_tracker *)hash_next(
                      gbl_progress_tracker, &ent, &bkt)) {
        LISTC_FOR_EACH(&tracker->threads, thread, lnk)
        {
            count += listc_size(&thread->attribs);
        }
    }

    if (count == 0) {
        Pthread_mutex_unlock(&progress_tracker_mu);
        return 0;
    }

    progress_entry_t *pProgress = calloc(count, sizeof(progress_entry_t));
    if (!pProgress) {
        Pthread_mutex_unlock(&progress_tracker_mu);
        return SQLITE_NOMEM;
    }

    count = 0;
    for (tracker = (struct progress_tracker *)hash_first(gbl_progress_tracker,
                                                         &ent, &bkt);
         tracker; tracker = (struct progress_tracker *)hash_next(
                      gbl_progress_tracker, &ent, &bkt)) {
        LISTC_FOR_EACH(&tracker->threads, thread, lnk)
        {
            LISTC_FOR_EACH(&thread->attribs, attrib, lnk)
            {
                progress_tracking_update_stats(attrib);
                pProgress[count].thread_id = (int)thread->thread_id;
                pProgress[count].thread_sub_id = thread->thread_sub_id;
                pProgress[count].type = get_progress_type_str(tracker->type);
                pProgress[count].name = strdup(tracker->name);
                if (attrib->sub_name) {
                    pProgress[count].sub_name = strdup(attrib->sub_name);
                }
                pProgress[count].seed = strdup(tracker->seed);
                pProgress[count].stage =
                    get_progress_stage_str(tracker->type, attrib->stage);
                pProgress[count].status =
                    get_progress_status_str(attrib->status);
                if (attrib->start_time_ms != 0) {
                    dttz_t start_time = (dttz_t){
                        .dttz_sec = attrib->start_time_ms / 1000,
                        .dttz_frac = attrib->start_time_ms -
                                     (attrib->start_time_ms / 1000 * 1000),
                        .dttz_prec = DTTZ_PREC_MSEC};
                    dttz_to_client_datetime(&start_time, "UTC",
                                            (cdb2_client_datetime_t *)&(
                                                pProgress[count].start_time));
                }
                if (attrib->end_time_ms != 0) {
                    dttz_t end_time = (dttz_t){
                        .dttz_sec = attrib->end_time_ms / 1000,
                        .dttz_frac = attrib->end_time_ms -
                                     (attrib->end_time_ms / 1000 * 1000),
                        .dttz_prec = DTTZ_PREC_MSEC};
                    dttz_to_client_datetime(
                        &end_time, "UTC",
                        (cdb2_client_datetime_t *)&(pProgress[count].end_time));
                }
                pProgress[count].processed_records = attrib->processed_records;
                pProgress[count].total_records = attrib->total_records;
                pProgress[count].rate_per_sec = attrib->rate_per_ms * 1000;
                pProgress[count].remaining_time_sec =
                    attrib->remaining_time_ms / 1000;
                ++count;
            }
        }
    }
    Pthread_mutex_unlock(&progress_tracker_mu);

    *data = pProgress;
    *npoints = count;

    return 0;
}
