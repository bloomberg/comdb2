#include <poll.h>
#include "comdb2.h"
#include "views.h"
#include "views_cron.h"

#include "logmsg.h"

static int timepart_is_exec_time(cron_sched_t *sched, cron_event_t *event);
static int timepart_wait_next_event(cron_sched_t *sched, cron_event_t *event);

/**
 * Start a views cron thread
 *
 */
cron_sched_t* timepart_cron_start(cron_sched_t *sched)
{
    struct errstat xerr = {0};
    sched_if_t  timepart_cron_implem;
    
    timepart_cron_implem.name = "timepart_cron";
    timepart_cron.type = CRON_TIMEPART;
    timepart_cron.default_sleep_idle = bdb_attr_get(thedb->bdb_attr,
                BDB_ATTR_CRON_IDLE_SECS);
    timepart_cron.is_exec_time = timepart_is_exec_time;
    timepart_cron.wait_next_event = timepart_wait_next_event;


    if(!sched)
        sched = cron_add_event(NULL, "timepart_cron", INT_MIN,
                timepart_cron_kickoff, NULL, NULL, NULL,
                NULL, &xerr, &itf);

    return sched;
}



static int timepart_is_exec_time(cron_sched_t *sched, cron_event_t *event)
{
    assert(event);

    if(!ltime) {
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);

        return event->epoch <= now.tv_sec;
    }
    return event->epoch <= ltime;
}


static int timepart_wait_next_event(cron_sched_t *sched, cron_event_t *event)
{
    struct timespec ts;
    if (event) {
        ts.tv_sec = event->epoch;
    } else {
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += sched->ifs->default_sleep_idle;
    }
    ts.tv_nsec = 0;

    return pthread_cond_timedwait(&sched->cond, &sched->mtx, &ts);
}
