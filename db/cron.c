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

#include "cron.h"
#include "cron_systable.h"

/**
 * Primitive cron job that monitors a ordered list of epoch marked events,
 * each having a callback function associated
 * Sleep until next event is to be triggered
 *
 */

struct cron_sched {
    pthread_t tid; /* pthread id of the thread owning this cron structure */
    pthread_cond_t cond; /* locking and signaling */
    pthread_mutex_t mtx;
    int running; /* marked under mtx lock, signaling a event processing if set
                  */
    LISTC_T(struct cron_event) events; /* list of events */
    LINKC_T(struct cron_sched) lnk;    /* link the cron schedulers */
    sched_if_t impl;
};

typedef struct cron_scheds {
    LISTC_T(struct cron_sched) scheds; /* all global schedulers */
    pthread_rwlock_t rwlock;
} cron_scheds_t;

static cron_scheds_t crons;
pthread_mutex_t _crons_mtx = PTHREAD_MUTEX_INITIALIZER;

static void *_cron_runner(void *arg);
static int _queue_event(cron_sched_t *sched, int epoch, FCRON func, void *arg1,
                        void *arg2, void *arg3, uuid_t *source_id,
                        struct errstat *err);

void init_cron(void)
{
    listc_init(&crons.scheds, offsetof(struct cron_sched, lnk));
    Pthread_rwlock_init(&crons.rwlock, NULL);
}

/**
 * Add a new event to a scheduler.
 * Create a scheduler if none exists yet
 *
 */
cron_sched_t *cron_add_event(cron_sched_t *sched, const char *name, int epoch,
                             FCRON func, void *arg1, void *arg2, void *arg3,
                             uuid_t *source_id, struct errstat *err,
                             sched_if_t *impl)
{
    int created;
    int rc;

    logmsg(LOGMSG_DEBUG, "%s %s\n", (sched) ? "ADD_EVENT" : "ADD_SCHEDULER",
           (sched) ? sched->impl.name : name);

    created = 0;
    if (!sched) {
        sched = (cron_sched_t *)calloc(1, sizeof(cron_sched_t));
        if (!sched) {
            goto oom;
        }
        created = 1;

        Pthread_mutex_init(&sched->mtx, NULL);
        Pthread_cond_init(&sched->cond, NULL);
        listc_init(&sched->events, offsetof(struct cron_event, lnk));
        if (!impl) {
            /* default to a time based cron */
            time_cron_create(&sched->impl, NULL, NULL);
        } else {
            sched->impl = *impl;
        }
        if (name && !sched->impl.name) {
            sched->impl.name = strdup(name);
            if (!sched->impl.name) {
                goto oom;
            }
        }

        sched->impl.sched = sched;

        Pthread_rwlock_wrlock(&crons.rwlock);
        listc_abl(&crons.scheds, sched);
        Pthread_rwlock_unlock(&crons.rwlock);

        rc = pthread_create(&sched->tid, &gbl_pthread_attr_detached,
                            _cron_runner, sched);
        if (rc) {
            errstat_set_rc(err, CRON_ERR_CREATE);
            errstat_set_strf(err, "pthread_create rc=%d", rc);
            goto error;
        }
    }

    Pthread_mutex_lock(&sched->mtx);
    rc = _queue_event(sched, epoch, func, arg1, arg2, arg3, source_id, err);
    Pthread_mutex_unlock(&sched->mtx);

    if (rc == CRON_NOERR) {
        return sched;
    }

error:

    if (sched && created) {
        if (sched->impl.state)
            free(sched->impl.state);
        if (sched->impl.name)
            free(sched->impl.name);
        Pthread_cond_destroy(&sched->cond);
        Pthread_mutex_destroy(&sched->mtx);
        free(sched);
        sched = NULL;
    }
    return NULL;

oom:
    errstat_set_rc(err, CRON_ERR_MALLOC);
    errstat_set_strf(err, "%s", "Cron OOM");
    goto error;
}

static void _set_event(cron_event_t *event, int epoch, FCRON func, void *arg1,
                       void *arg2, void *arg3, cron_sched_t *sched)
{
    event->epoch = epoch;
    event->func = func;
    event->arg1 = arg1;
    event->arg2 = arg2;
    event->arg3 = arg3;
    event->schedif = &sched->impl;
}

static void _insert_ordered_event(cron_sched_t *sched, cron_event_t *event)
{
    LISTC_T(struct cron_event) tmpsched;
    cron_event_t *crt;

    listc_init(&tmpsched, offsetof(struct cron_event, lnk));

    /* add all older records that preceed current event */
    while ((crt = sched->events.top) != NULL) {
        if (crt->epoch <= event->epoch) {
            listc_rfl(&sched->events, crt);
            listc_atl(&tmpsched, crt); /* ! reversed order */
        } else {
            break;
        }
    }

    /* NOTE: make sure the event is unique, i.e. only one set
       of parameters */
    if (!sched->events.top ||
        (memcmp(event, sched->events.top, sizeof(*event)) != 0)) {
        /* add new event, anything left in sched queue follows new event */
        listc_atl(&sched->events, event);
    }

    /* add all older events */
    while ((crt = tmpsched.top) != NULL) {
        listc_rfl(&tmpsched, crt);
        listc_atl(&sched->events, crt); /* now order is proper again */
    }

    /* THIS MUST BE CALLED UNDER sched->mtx, guaranteed if called from callback
     */
    if (sched->events.top == event) {
        /* new event at the top of the list,
           notify cron to pick up the event */
        Pthread_cond_signal(&sched->cond);
    }
}

static int _queue_event(cron_sched_t *sched, int epoch, FCRON func, void *arg1,
                        void *arg2, void *arg3, uuid_t *source_id,
                        struct errstat *err)
{
    cron_event_t *event;

    if (!sched) {
        errstat_set_strf(err, "Scheduler not initialized!");
        return err->errval = CRON_ERR_BUG;
    }

    event = (cron_event_t *)calloc(1, sizeof(cron_event_t));
    if (!event) {
        errstat_set_strf(err, "%s", "Cron OOM");
        return err->errval = CRON_ERR_MALLOC;
    }

    /* A new event is born */
    _set_event(event, epoch, func, arg1, arg2, arg3, sched);

    if (source_id) {
        comdb2uuidcpy(event->source_id, *source_id);
    } else {
        comdb2uuid_clear(event->source_id);
    }

    _insert_ordered_event(sched, event);

    return err->errval = CRON_NOERR;
}

static void _destroy_event(cron_sched_t *sched, cron_event_t *event)
{
    listc_rfl(&sched->events, event);
    if (event->arg1)
        free(event->arg1);
    if (event->arg2)
        free(event->arg2);
    if (event->arg3)
        free(event->arg3);
    free(event);
}

/**
 * Regular wake up and run job
 * Function is meant to be used with pthread_create
 * and run a thread.  Callbacks have to create & destroy
 * per thread resources, if any
 *
 */
static void *_cron_runner(void *arg)
{
    cron_sched_t *sched = (cron_sched_t *)arg;
    cron_event_t *event;
    int rc;
    struct errstat xerr;
    int locked;

    if (!sched) {
        logmsg(LOGMSG_ERROR, "%s: NULL schedule!\n", __func__);
        return NULL;
    }

    locked = 0;
    while (!gbl_exit && !db_is_stopped()) {
        Pthread_mutex_lock(&sched->mtx);
        locked = 1;

        while ((event = sched->events.top) != NULL) {
            if (sched->impl.is_exec_time(&sched->impl, event)) {
                bzero(&xerr, sizeof(xerr));

                /* lets do it */
                /* NOTE: we don't need to keep the scheduler lock here!
                   The event pointer should be stable, but the list can change
                   in the
                   meantime.  This prevents callbacks that acquire resources
                   locks from
                   deadlocking with other resource threads that try to schedule
                   an event
                   We mark the cron job as working, to exclude access to top
                   event to any other thread !!!
                   */
                sched->running = 1;
                Pthread_mutex_unlock(&sched->mtx);
                event->func(event, &xerr);
                Pthread_mutex_lock(&sched->mtx);
                sched->running = 0;

                if (xerr.errval)
                    logmsg(LOGMSG_ERROR,
                           "Schedule %s error event %d rc=%d errstr=%s\n",
                           (sched->impl.name) ? sched->impl.name : "(noname)",
                           event->epoch, xerr.errval, xerr.errstr);
                _destroy_event(sched, event);
            } else {
                break;
            }
        }

        rc = sched->impl.wait_next_event(&sched->impl, event);
        if (rc && rc != ETIMEDOUT) {
            logmsg(LOGMSG_ERROR, "%s: bad pthread_cond_timedwait rc=%d\n",
                   __func__, rc);
            break;
        }

        Pthread_mutex_unlock(&sched->mtx);
        locked = 0;
    }

    if (locked) {
        Pthread_mutex_unlock(&sched->mtx);
    }

    logmsg(LOGMSG_DEBUG, "Exiting cron job for %s\n", sched->impl.name);
    return NULL;
}

/**
 * Signal looping worker, maybe db is exiting
 *
 */
void cron_signal_worker(cron_sched_t *sched)
{
    Pthread_mutex_lock(&sched->mtx);

    Pthread_cond_signal(&sched->cond);

    Pthread_mutex_unlock(&sched->mtx);
}

/**
 * Update the next event arguments for a specific event
 * NOTE:
 * Since this can race with an actual exection, the function
 * wait if it tries to update the first to run event while the
 * event is under processing!
 * Events not at the top of the list are safe to update since
 * the cron thread cannot access the queue until this is done
 *
 */
int cron_update_event(cron_sched_t *sched, int epoch, FCRON func, void *arg1,
                      void *arg2, void *arg3, uuid_t source_id,
                      struct errstat *err)
{
    cron_event_t *event = NULL, *tmp = NULL;
    int found = 0;

    cron_lock(sched);

    LISTC_FOR_EACH_SAFE(&sched->events, event, tmp, lnk)
    {
        if (comdb2uuidcmp(event->source_id, source_id) == 0) {

            /* we can process this */
            _set_event(event, epoch, func, arg1, arg2, arg3, sched);

            /* remote the event, and reinsert it in the new position */
            listc_rfl(&sched->events, event);
            _insert_ordered_event(sched, event);
            found = 1;
        }
    }

    cron_unlock(sched);

    return (found) ? CRON_NOERR : CRON_ERR_EXIST;
}

/**
 * Clear queue of events
 *
 */
void cron_clear_queue(cron_sched_t *sched)
{
    cron_event_t *event;

    cron_lock(sched);

    /* mop up */
    while ((event = sched->events.top))
        _destroy_event(sched, event);

    cron_unlock(sched);
}

/* Note for running event, if any, to finish */
void cron_lock(cron_sched_t *sched)
{
    struct timespec now;

    Pthread_mutex_lock(&sched->mtx);

    while (sched->running) {
        clock_gettime(CLOCK_REALTIME, &now);
        now.tv_sec += 1;
        pthread_cond_timedwait(&sched->cond, &sched->mtx, &now);
    }
}

void cron_unlock(cron_sched_t *sched)
{
    Pthread_mutex_unlock(&sched->mtx);
}

int cron_timedwait(cron_sched_t *sched, struct timespec *ts)
{
    return pthread_cond_timedwait(&sched->cond, &sched->mtx, ts);
}

const char *cron_type_to_name(enum cron_type type)
{
    switch (type) {
    case CRON_TIMEPART:
        return "WALLTIME";
    case CRON_LOGICAL:
        return "LOGICAL";
    default:
        return "UNKNOWN";
    }
}

/**
 * Return specific scheduler implementation
 *
 */
sched_if_t *cron_impl(cron_sched_t *sched)
{
    return &sched->impl;
}

/***************** CRON SYSTABLE IMPLEMENTATION *******************/

int cron_systable_schedulers_collect(void **data, int *nrecords)
{
    systable_cron_scheds_t *arr = NULL, *temparr;
    cron_sched_t *sched;
    int narr = 0;
    int nsize = 0;
    int rc = 0;

    Pthread_rwlock_rdlock(&crons.rwlock);
    LISTC_FOR_EACH(&crons.scheds, sched, lnk)
    {
        if (narr >= nsize) {
            nsize += 10;
            temparr = realloc(arr, sizeof(systable_cron_scheds_t) * nsize);
            if (!temparr) {
                logmsg(LOGMSG_ERROR, "%s OOM %zu!\n", __func__,
                       sizeof(systable_cron_scheds_t) * nsize);
                cron_systable_schedulers_free(arr, narr);
                arr = NULL;
                narr = 0;
                rc = -1;
                goto done;
            }
            arr = temparr;
        }
        cron_lock(sched);
        arr[narr].name = strdup(sched->impl.name);
        arr[narr].type = strdup(cron_type_to_name(sched->impl.type));
        arr[narr].running = sched->running;
        arr[narr].nevents = sched->events.count;
        arr[narr].description = sched->impl.describe
                                    ? sched->impl.describe(&sched->impl)
                                    : strdup("");
        narr++;
        cron_unlock(sched);
    }
done:
    Pthread_rwlock_unlock(&crons.rwlock);
    *data = arr;
    *nrecords = narr;
    return rc;
}

void cron_systable_schedulers_free(void *arr, int nrecords)
{
    systable_cron_scheds_t *parr = (systable_cron_scheds_t *)arr;
    int i;

    for (i = 0; i < nrecords; i++) {
        if (parr[i].name)
            free(parr[i].name);
        if (parr[i].type)
            free(parr[i].type);
        if (parr[i].description)
            free(parr[i].description);
    }
    free(arr);
}

int cron_systable_sched_events_collect(cron_sched_t *sched,
                                       systable_cron_events_t **parr,
                                       int *nrecords, int *pnsize)
{
    systable_cron_events_t *arr = *parr, *temparr;
    int narr = *nrecords;
    cron_event_t *event;
    int nsize = *pnsize;
    int rc = 0;
    uuidstr_t us;

    cron_lock(sched);
    LISTC_FOR_EACH(&sched->events, event, lnk)
    {
        if ((narr + sched->events.count) >= nsize) {
            nsize += sched->events.count;
            temparr = realloc(arr, sizeof(systable_cron_events_t) * nsize);
            if (!temparr) {
                logmsg(LOGMSG_ERROR, "%s OOM %zu!\n", __func__,
                       sizeof(systable_cron_events_t) * nsize);
                cron_systable_events_free(arr, narr);
                nsize = narr = 0;
                arr = NULL;
                rc = -1;
                goto done;
            }
            arr = temparr;
        }
        arr[narr].name = sched->impl.event_describe
                             ? sched->impl.event_describe(&sched->impl, event)
                             : strdup("");
        arr[narr].type = strdup(sched->impl.name);
        arr[narr].epoch = event->epoch;
        arr[narr].arg1 = event->arg1 ? strdup(event->arg1) : NULL;
        arr[narr].arg2 = event->arg2 ? strdup(event->arg2) : NULL;
        arr[narr].arg3 = event->arg3 ? strdup(event->arg3) : NULL;
        arr[narr].sourceid = strdup(comdb2uuidstr(event->source_id, us));
        narr++;
    }
done:
    cron_unlock(sched);

    *pnsize = nsize;
    *parr = arr;
    *nrecords = narr;
    return rc;
}

int cron_systable_events_collect(void **data, int *nrecords)
{
    systable_cron_events_t *arr = NULL;
    cron_sched_t *sched;
    int narr = 0;
    int nsize = 0;
    int rc = 0;

    Pthread_rwlock_rdlock(&crons.rwlock);
    LISTC_FOR_EACH(&crons.scheds, sched, lnk)
    {
        rc = cron_systable_sched_events_collect(sched, &arr, &narr, &nsize);
        if (rc)
            break;
    }
    Pthread_rwlock_unlock(&crons.rwlock);
    *data = arr;
    *nrecords = narr;
    return rc;
}

void cron_systable_events_free(void *arr, int nrecords)
{
    systable_cron_events_t *parr = (systable_cron_events_t *)arr;
    int i;

    for (i = 0; i < nrecords; i++) {
        if (parr[i].name)
            free(parr[i].name);
        if (parr[i].type)
            free(parr[i].type);
        if (parr[i].arg1)
            free(parr[i].arg1);
        if (parr[i].arg2)
            free(parr[i].arg2);
        if (parr[i].arg3)
            free(parr[i].arg3);
        if (parr[i].sourceid)
            free(parr[i].sourceid);
    }
    free(arr);
}

/*************** Default time scheduler implementation ******************/

static int time_is_exec_time(sched_if_t *impl, cron_event_t *event)
{
    struct timespec now;
    assert(event);
    clock_gettime(CLOCK_REALTIME, &now);
    return event->epoch <= now.tv_sec;
}

static int time_wait_next_event(sched_if_t *impl, cron_event_t *event)
{
    cron_sched_t *sched = impl->sched;
    struct timespec ts;
    if (event) {
        ts.tv_sec = event->epoch;
    } else {
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += sched->impl.default_sleep_idle;
    }
    ts.tv_nsec = 0;

    return pthread_cond_timedwait(&sched->cond, &sched->mtx, &ts);
}

/**
 * Create a timepart scheduler implementation
 *
 */
void time_cron_create(sched_if_t *intf, char *(*describe)(sched_if_t *),
                      char *(*event_describe)(sched_if_t *, cron_event_t *))
{
    intf->type = CRON_TIMEPART;
    intf->default_sleep_idle =
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_CRON_IDLE_SECS);
    intf->is_exec_time = time_is_exec_time;
    intf->wait_next_event = time_wait_next_event;
    intf->describe = describe;
    intf->event_describe = event_describe;
    intf->state = NULL;
}

/**
 * Returns a scheduler with name "name", if any
 * NOTE: scheduler is not locked
 *
 */
cron_sched_t *cron_sched_byname(const char *name)
{
    cron_sched_t *sched = NULL;

    Pthread_rwlock_rdlock(&crons.rwlock);
    LISTC_FOR_EACH(&crons.scheds, sched, lnk)
    {
        if (!strncasecmp(sched->impl.name, name, strlen(name) + 1)) {
            break;
        }
    }
    Pthread_rwlock_unlock(&crons.rwlock);

    return sched;
}

/**
 * Signal all linked schedulers
 */
void cron_signal_all(void)
{
    cron_sched_t *sched = NULL;

    Pthread_rwlock_rdlock(&crons.rwlock);
    LISTC_FOR_EACH(&crons.scheds, sched, lnk)
    {
        cron_signal_worker(sched);
    }
    Pthread_rwlock_unlock(&crons.rwlock);
}

/**
 * Clear all the queues events
 *
 */
void cron_clear_queue_all(void)
{
    cron_sched_t *sched = NULL;

    Pthread_rwlock_rdlock(&crons.rwlock);
    LISTC_FOR_EACH(&crons.scheds, sched, lnk)
    {
        cron_clear_queue(sched);
    }
    Pthread_rwlock_unlock(&crons.rwlock);
}
