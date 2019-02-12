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

#include "views.h"
#include "logical_cron.h"

typedef struct logical_state {
    unsigned long long clock;
} logical_state_t;

static int logical_is_exec_time(sched_if_t *impl, cron_event_t *event)
{
    cron_sched_t *sched = impl->sched;
    logical_state_t *state = (logical_state_t *)impl->state;
    int rc = 0;
    assert(event);
    assert(state);

    cron_lock(sched);
    rc = event->epoch <= state->clock;
    cron_unlock(sched);

    return rc;
}

static int logical_wait_next_event(sched_if_t *impl, cron_event_t *event)
{
    cron_sched_t *sched = impl->sched;
    struct timespec ts;

    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += impl->default_sleep_idle;
    ts.tv_nsec = 0;

    return cron_timedwait(sched, &ts);
    /*return pthread_cond_timedwait(&sched->cond, &sched->mtx, &ts);*/
}

/* assert: read lock on crons.rwlock */
void logical_cron_incr(sched_if_t *impl)
{
    cron_sched_t *sched = impl->sched;
    logical_state_t *state = (logical_state_t *)&impl->state;
    cron_lock(sched);
    state->clock++;
    cron_unlock(sched);
}

/* assert: read lock on crons.rwlock */
void logical_cron_set(sched_if_t *impl, unsigned long long val)
{
    cron_sched_t *sched = impl->sched;
    logical_state_t *state = (logical_state_t *)&impl->state;
    cron_lock(sched);
    state->clock = val;
    cron_unlock(sched);
}

static int
logical_cron_create(sched_if_t *intf, char *(*describe)(sched_if_t *),
                    char *(*event_describe)(sched_if_t *, cron_event_t *))
{
    intf->type = CRON_LOGICAL;
    intf->default_sleep_idle =
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_CRON_LOGICAL_IDLE_SECS);
    intf->is_exec_time = logical_is_exec_time;
    intf->wait_next_event = logical_wait_next_event;
    intf->describe = describe;
    intf->event_describe = event_describe;
    intf->state = calloc(1, sizeof(logical_state_t));

    return (!intf->state) ? -1 : 0;
}

static void *_logical_cron_test_callback(uuid_t source_id, void *arg1,
                                         void *arg2, void *arg3,
                                         struct errstat *err)
{
    cron_sched_t *sched = *(cron_sched_t **)arg3;
    sched_if_t *impl = cron_impl(sched);
    uuidstr_t us;
    int rc;
    int *nrolls = (int *)arg2;

    logmsg(LOGMSG_USER, "Test callback %s %s %s %d\n", impl->name,
           comdb2uuidstr(source_id, us), (char *)arg1, *(int *)arg2);

    (*nrolls)++;
    rc =
        (cron_add_event(sched, NULL, *nrolls, _logical_cron_test_callback, arg1,
                        arg2, arg3, (uuid_t *)&source_id, err, NULL) == NULL)
            ? err->errval
            : VIEW_NOERR;
    if (rc) {
        logmsg(LOGMSG_USER, "Logical test unit failed iteration %d rc %d\n",
               *nrolls, rc);
    }

    return NULL;
}

int logical_cron_unit_test(FILE *out, const char *name)
{
    cron_sched_t *sched;
    sched_if_t *impl;
    uuid_t source_id;
    struct errstat err = {0};
    int nrolls = 1;
    sched_if_t intf = {0};

    if (logical_cron_create(&intf, NULL /*logical_describe*/,
                            NULL /*logical_event_describe*/)) {
        logmsg(LOGMSG_ERROR, "%s Malloc error!\n", __func__);
        return -1;
    }

    comdb2uuid(source_id);

    /* create a logical schedule */
    sched = cron_add_event(NULL, name, 1, _logical_cron_test_callback,
                           "testing", &nrolls, &sched, &source_id, &err, &intf);
    if (!sched) {
        logmsg(LOGMSG_USER, "failed to create logical scheduler!\n");
        return -1;
    }

    impl = cron_impl(sched);

    /* push output and rollout, a few times */
    logical_cron_incr(impl);

    return 0;
}
