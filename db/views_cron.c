#include <poll.h>
#include "comdb2.h"
#include "views.h"
#include "views_cron.h"

#include "logmsg.h"

/**
 * Primitive cron job that monitors a ordered list of epoch marked events,
 * each having a callback function associated
 * Sleep until next event is to be triggered
 *
 */
struct cron_event {
    int epoch; /* when this event should run */
    FCRON func; /* what function should run */
    void *arg1; /* arguments 1-3 for "func"; note: 2-3 are just for convenience */
    void *arg2;
    void *arg3;
    uuid_t source_id; /* source id, if any, used to map events to sources */
    LINKC_T(struct cron_event) lnk;
};
typedef struct cron_event cron_event_t;

struct cron_sched {
    pthread_t tid; /* pthread id of the thread owning this cron structure */
    char *name; /* name of the cron job */
    pthread_cond_t cond; /* locking and signaling */
    pthread_mutex_t mtx;
    int running; /* marked under mtx lock, signaling a event processing if set */
    LISTC_T(struct cron_event) events; /* list of events */
};

static void *_cron_runner(void *arg);
static int _queue_event(cron_sched_t *sched, int epoch, FCRON func, void *arg1,
                        void *arg2, void *arg3, uuid_t * source_id, 
                        struct errstat *err);

/**
 * Add a new event to a scheduler.
 * Create a scheduler if none exists yet
 *
 */
cron_sched_t *cron_add_event(cron_sched_t *sched, const char *name, int epoch,
                             FCRON func, void *arg1, void *arg2, void *arg3,
                             uuid_t *source_id, struct errstat *err)
{
    int created;
    int rc;

    created = 0;
    if (!sched) {
        sched = (cron_sched_t *)calloc(1, sizeof(cron_sched_t));
        if (!sched) {
            goto oom;
        }
        created = 1;

        pthread_mutex_init(&sched->mtx, NULL);
        pthread_cond_init(&sched->cond, NULL);
        listc_init(&sched->events, offsetof(struct cron_event, lnk));
        if (name) {
            sched->name = strdup(name);
            if (!sched->name) {
                goto oom;
            }
        }

        rc = pthread_create(&sched->tid, &gbl_pthread_attr_detached,
                            _cron_runner, sched);
        if (rc) {
            errstat_set_rc(err, VIEW_ERR_CREATE);
            errstat_set_strf(err, "pthread_create rc=%d", rc);
            goto error;
        }
    }

    pthread_mutex_lock(&sched->mtx);

    rc = _queue_event(sched, epoch, func, arg1, arg2, arg3, source_id, err);
    if (rc != VIEW_NOERR) {
        pthread_mutex_unlock(&sched->mtx);
        goto error;
    }

    pthread_mutex_unlock(&sched->mtx);

    return sched;

error:

    if (sched && created) {
        if (sched->name)
            free(sched->name);
        pthread_cond_destroy(&sched->cond);
        pthread_mutex_destroy(&sched->mtx);
        free(sched);
        sched = NULL;
    }
    return NULL;

oom:
    errstat_set_rc(err, VIEW_ERR_MALLOC);
    errstat_set_strf(err, "%s", "Cron OOM");
    goto error;
}

static void _set_event(cron_event_t *event, int epoch, FCRON func, void *arg1,
        void *arg2, void *arg3)
{
    event->epoch = epoch;
    event->func = func;
    event->arg1 = arg1;
    event->arg2 = arg2;
    event->arg3 = arg3;
}

static void _insert_ordered_event(cron_sched_t * sched, cron_event_t *event)
{
    LISTC_T(struct cron_event) tmpsched;
    cron_event_t *crt;

    listc_init(&tmpsched, offsetof(struct cron_event, lnk));

    /* add all older records that preceed current event */
    while (crt = sched->events.top) {
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
    while (crt = tmpsched.top) {
        listc_rfl(&tmpsched, crt);
        listc_atl(&sched->events, crt); /* now order is proper again */
    }

    /* THIS MUST BE CALLED UNDER sched->mtx, guaranteed if called from callback
     */
    if (sched->events.top == event) {
        /* new event at the top of the list,
           notify cron to pick up the event */
        pthread_cond_signal(&sched->cond);
    }
}

static int _queue_event(cron_sched_t *sched, int epoch, FCRON func, void *arg1,
        void *arg2, void *arg3, uuid_t * source_id, 
        struct errstat *err)
{
    cron_event_t *event;

    if (!sched) {
        errstat_set_strf(err, "Folloup called instead of regular event add");
        return err->errval = VIEW_ERR_BUG;
    }

    event = (cron_event_t *)calloc(1, sizeof(cron_event_t));
    if (!event) {
        errstat_set_strf(err, "%s", "Cron OOM");
        return err->errval = VIEW_ERR_MALLOC;
    }

    /* A new event is born */
    _set_event(event, epoch, func, arg1, arg2, arg3);

    if(source_id) {
        comdb2uuidcpy(event->source_id, *source_id);
    } else {
        comdb2uuid_clear(event->source_id);
    }

    _insert_ordered_event(sched, event);

    return err->errval = VIEW_NOERR;
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
#define DEFAULT_SLEEP_IDLE_SCHEDULE 30

    cron_sched_t *sched = (cron_sched_t *)arg;
    cron_event_t *event;
    int secs_until_next_event;
    struct timespec ts, now;
    int rc;
    struct errstat xerr;
    int locked;

    if (!sched) {
        logmsg(LOGMSG_ERROR, "%s: NULL schedule!\n", __func__);
        return NULL;
    }

    locked = 0;
    while (!gbl_exit) {
        secs_until_next_event = DEFAULT_SLEEP_IDLE_SCHEDULE;

        pthread_mutex_lock(&sched->mtx);
        locked = 1;

        clock_gettime(CLOCK_REALTIME, &now);
        while (event = sched->events.top) {
            /* refresh now, since callback can take a long time!*/
            clock_gettime(CLOCK_REALTIME, &now);

            if (event->epoch <= now.tv_sec) {
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
                pthread_mutex_unlock(&sched->mtx);
                event->func(event->source_id, event->arg1, event->arg2, event->arg3,
                            &xerr);
                pthread_mutex_lock(&sched->mtx);
                sched->running = 0;     

                if (xerr.errval) {
                    logmsg(LOGMSG_ERROR, 
                            "Schedule %s error event %d rc=%d errstr=%s\n",
                            (sched->name) ? sched->name : "(noname)",
                            event->epoch, xerr.errval, xerr.errstr);
                }
                listc_rfl(&sched->events, event);
                if (event->arg1)
                    free(event->arg1);
                if (event->arg2)
                    free(event->arg2);
                if (event->arg3)
                    free(event->arg3);
                free(event);
            } else {
                break;
            }
        }

        if (event) {
            ts.tv_sec = event->epoch;
        } else {
            ts.tv_sec = now.tv_sec + DEFAULT_SLEEP_IDLE_SCHEDULE;
        }
        ts.tv_nsec = 0;

        rc = pthread_cond_timedwait(&sched->cond, &sched->mtx, &ts);
        if (rc && rc != ETIMEDOUT) {
            logmsg(LOGMSG_ERROR, "%s: bad pthread_cond_timedwait rc=%d\n", __func__,
                    rc);

            break;
        }

        pthread_mutex_unlock(&sched->mtx);
        locked = 0;
    }

    if (locked) {
        pthread_mutex_unlock(&sched->mtx);
    }

    logmsg(LOGMSG_DEBUG, "Exiting cron job for %s\n", sched->name);
    return NULL;
}

/**
 * Signal looping worker, maybe db is exiting
 *
 */
void cron_signal_worker(cron_sched_t *sched)
{
    pthread_mutex_lock(&sched->mtx);

    pthread_cond_signal(&sched->cond);

    pthread_mutex_unlock(&sched->mtx);
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
int cron_update_event(cron_sched_t *sched, int epoch, FCRON func, 
    void *arg1, void *arg2, void *arg3, uuid_t source_id, struct errstat *err)
{
    cron_event_t *event = NULL, *tmp = NULL;
    int found = 0;

retry:
    pthread_mutex_lock(&sched->mtx);

    LISTC_FOR_EACH_SAFE(&sched->events, event, tmp, lnk) {
        if(comdb2uuidcmp(event->source_id, source_id) == 0) {
            /* is this event running ? */
            if(event == sched->events.top && sched->running) {
                pthread_mutex_unlock(&sched->mtx);
                poll(NULL, 0, 10);
                goto retry;
            }
            /* we can process this */
            _set_event(event, epoch, func, arg1, arg2, arg3);

            /* remote the event, and reinsert it in the new position */
            listc_rfl(&sched->events, event);
            _insert_ordered_event(sched, event);
            found = 1;
        }
    }
    pthread_mutex_unlock(&sched->mtx);

    return (found)?VIEW_NOERR:VIEW_ERR_EXIST;
}
