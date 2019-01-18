/*
   Copyright 2019 Bloomberg Finance L.P.

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

#ifndef __CRON_H_
#define __CRON_H_


#include  "comdb2uuid.h"

/**
 * Primitive cron job that monitors a ordered list of epoch marked events,
 * each having a callback function associated
 * Sleep until next event is to be triggered
 *
 */

/* opaque schedule */
typedef struct cron_sched cron_sched_t;
typedef struct cron_event cron_event_t;


/**
 * Define the multiple types of schedulers 
 *
 */
enum cron_type {
    CRON_TIMEPART = 0
    ,CRON_LOGICAL = 1
};

struct sched_if {
    char *name;
    enum cron_type type;
    int default_sleep_idle;
    /* check if it is time to execute event "event" */
    int (*is_exec_time)(cron_sched_t *sched, cron_event_t *event);
    /* sleep until a signal or next event is due */
    int (*wait_next_event)(cron_sched_t *sched, cron_event_t *event);
    /* describe the scheduler */
    char* (*describe)(cron_sched_t *sched);
    /* describe the event function */
    char* (*event_describe)(cron_sched_t *sched, cron_event_t *event);
    int impl_sz;
    void *intf;
};
typedef struct sched_if sched_if_t;


/* this is the callback prototype for each event */
typedef void *(*FCRON)(uuid_t source_id, void *arg1, void *arg2, void *arg3, 
                       struct errstat *err);

/**
 * Add a new event to a scheduler, and create a scheduler if needed.
 * NOTE: to create a scheduler, "sched"== NULL, and "intf"!=NULL
 * to add to an existing scheduler, "sched"!=NULL and "intf"==NULL
 *
 */
cron_sched_t *cron_add_event(cron_sched_t *sched, int epoch,
                             FCRON func, void *arg1, void *arg2, void *arg3,
                             uuid_t *source_id, struct errstat *err, sched_if_t *intf);

/**
 * Signal looping worker, maybe db is exiting
 *
 */
void cron_signal_worker(cron_sched_t *sched);

/**
 * Clear queue of events
 *
 */
void cron_clear_queue(cron_sched_t *sched);

/**
 * Lock/unlock scheduler so I can look at events
 *
 * NOTE: locking waits for the running to complete
 *
 */
void cron_lock(cron_sched_t *sched);
void cron_unlock(cron_sched_t *sched);

/**
 * Creates a snapshot of the events of a single scheduler, used 
 * to create individual comdb2_.. systables displaying events
 * for specific schedulers
 * 
 */
int cron_systable_sched_events_collect(cron_sched_t *sched, void **data, int *nrecords);

#endif

