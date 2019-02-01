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

#include "comdb2.h"
#include "comdb2uuid.h"
#include "cron_systable.h"

/**
 * Primitive cron job that monitors a ordered list of epoch marked events,
 * each having a callback function associated
 * Sleep until next event is to be triggered
 *
 */

enum {
    CRON_NOERR,
    CRON_ERR_CREATE = -1,
    CRON_ERR_MALLOC = -2,
    CRON_ERR_BUG = -3,
    CRON_ERR_EXIST = -4
};

/* opaque schedule */
typedef struct cron_sched cron_sched_t;

/**
 * Define the multiple types of schedulers
 *
 */
enum cron_type { CRON_TIMEPART = 0, CRON_LOGICAL = 1 };

/* this is the callback prototype for each event */
typedef void *(*FCRON)(uuid_t source_id, void *arg1, void *arg2, void *arg3,
                       struct errstat *err);

struct cron_event {
    int epoch;  /* when this event should run */
    FCRON func; /* what function should run */
    void *
        arg1; /* arguments 1-3 for "func"; note: 2-3 are just for convenience */
    void *arg2;
    void *arg3;
    uuid_t source_id; /* source id, if any, used to map events to sources */
    LINKC_T(struct cron_event) lnk;
};
typedef struct cron_event cron_event_t;

struct sched_if {
    cron_sched_t *sched;
    enum cron_type type;
    char *name;
    int default_sleep_idle;
    /* check if it is time to execute event "event" */
    int (*is_exec_time)(struct sched_if *impl, cron_event_t *event);
    /* sleep until a signal or next event is due */
    int (*wait_next_event)(struct sched_if *impl, cron_event_t *event);
    /* describe the scheduler */
    char *(*describe)(struct sched_if *impl);
    /* describe the event function */
    char *(*event_describe)(struct sched_if *impl, cron_event_t *event);
    void *state;
};
typedef struct sched_if sched_if_t;

/**
 * Add a new event to a scheduler, and create a scheduler if needed.
 * NOTE: to create a scheduler, "sched"== NULL, and "intf"!=NULL
 * to add to an existing scheduler, "sched"!=NULL and "intf"==NULL
 *
 */
cron_sched_t *cron_add_event(cron_sched_t *sched, const char *name, int epoch,
                             FCRON func, void *arg1, void *arg2, void *arg3,
                             uuid_t *source_id, struct errstat *err,
                             sched_if_t *intf);

/**
 * Initialize crons system
 */
void init_cron(void);
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
int cron_timedwait(cron_sched_t *sched, struct timespec *ts);

/**
 * Creates a snapshot of the events of a single scheduler, used
 * to create individual comdb2_.. systables displaying events
 * for specific schedulers
 *
 */
int cron_systable_sched_events_collect(cron_sched_t *sched,
                                       systable_cron_events_t **parr,
                                       int *nrecords, int *pnsize);

/**
 * Create a time scheduler implementation
 *
 */
void time_cron_create(sched_if_t *impl, char *(*describe)(sched_if_t *),
                      char *(*event_describe)(sched_if_t *, cron_event_t *));

/**
 * Return specific scheduler implementation
 *
 */
sched_if_t *cron_impl(cron_sched_t *sched);

 /**
 * Signal all linked schedulers
 */
void cron_signal_all(void);

/**
 * Clear all the queues events 
 *
 */
void cron_clear_queue_all(void);

/**
 * Returns a scheduler with name "name", if any
 * NOTE: scheduler is not locked
 *
 */
cron_sched_t* cron_sched_byname(const char *name);

#endif
