#ifndef _VIEWS_CRON_H_
#define _VIEWS_CRON_H_

#include  "comdb2uuid.h"

/**
 * Primitive cron job that monitors a ordered list of epoch marked events,
 * each having a callback function associated
 * Sleep until next event is to be triggered
 *
 */

/* opaque schedule */
typedef struct cron_sched cron_sched_t;

/* this is the callback prototype */
typedef void *(*FCRON)(uuid_t source_id, void *arg1, void *arg2, void *arg3, 
                       struct errstat *err);

/**
 * Add a new event to a scheduler.
 * Create a scheduler if none exists yet
 *
 */
cron_sched_t *cron_add_event(cron_sched_t *sched, const char *name, int epoch,
                             FCRON func, void *arg1, void *arg2, void *arg3,
                             uuid_t *source_id, struct errstat *err);

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
 * Queue size
 */
int cron_num_events(cron_sched_t *sched);

/**
 * Event details for the 'i'-th event
 * Returns 1 if event exist
 */
int cron_event_details(cron_sched_t *sched, int idx, FCRON *func, int *epoch,
                       void **arg1, void **arg2, void **arg3, uuid_t *sid);

#endif
